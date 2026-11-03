/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Handlers for the ScaleUpFabricManager RPCs.
//!
//! All ScaleUp RPCs now take a caller-supplied `NodeInfo` (an ephemeral
//! switch identity) rather than resolving a node from the rack inventory.
//! The handlers build an ephemeral `Switch` per request using host-endpoint
//! credentials, call the relevant NVUE / SSH / NMX flow, and return a
//! structured status.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use futures::future::join_all;
use librms::protos::rack_manager as rm;
use tokio_util::sync::CancellationToken;

use super::server::RackManagerServiceImpl;
use crate::api::grpc::conversions::{flatten_node_info, proto_node_type_to_domain};
use crate::domain::node::{NodeKind, NodeType};
use crate::domain::rack::NodeConfig;
use crate::libnmxc::{
    Endpoint, NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY, Nmxc, NmxcClientPool, NmxcError,
    NmxcTlsConfig, find_static_config_value,
};
use crate::nodes::nvos::NvBridge;
use crate::nodes::switch_gb200_nvidia::mtls::MtlsUnsetStatus;
use crate::nodes::switch_mtls::SwitchMtlsService;
use crate::nodes::{NodeInstance, SwitchScaleUpManagement};
use crate::utilities::error::{Result, RmsError};
use crate::utilities::special_purpose_ip::{PrivateUse, is_special_purpose_ip};
use crate::utilities::url::grpc_target_uri;

mod handlers_v2;
mod sdn_reset;

pub(super) use handlers_v2::normalize_v2_node_set;

// ── NMX controller helpers ────────────────────────────────────────────

const NMX_TOPOLOGY_UNKNOWN: &str = "unknown";
const NMX_POST_VALIDATION_SOFT_RESET_ATTEMPTS: usize = 2;
const NMX_CONTROLLER_APP_NAME: &str = "nmx-controller";
const NMX_CONTROLLER_READY_POLL_ATTEMPTS: usize = 6;
const NMX_CONTROL_PLANE_READY_STATES: &[&str] = &[
    "CONTROL_PLANE_STATE_CONFIGURED",
    "CONTROL_PLANE_STATE_UNCONFIGURED",
];

#[cfg(test)]
const NMX_CONTROLLER_READY_POLL_DELAY: Duration = Duration::ZERO;

#[cfg(not(test))]
const NMX_CONTROLLER_READY_POLL_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NmxSetStaticConfigResult {
    AlreadyConfigured,
    SetSucceeded,
    Failed,
}

impl NmxSetStaticConfigResult {
    /// Returns true if the result is anything other than `Failed`.
    fn is_success(self) -> bool {
        !matches!(self, Self::Failed)
    }

    fn response_message(self) -> &'static str {
        match self {
            Self::AlreadyConfigured => "NMX topology already configured",
            Self::SetSucceeded => "configuration complete",
            Self::Failed => "failed to set NMX topology",
        }
    }
}

fn nmx_config_response_message(
    result: NmxSetStaticConfigResult,
    soft_reset_attempted: bool,
) -> &'static str {
    if soft_reset_attempted && !result.is_success() {
        return "NMX topology validation failed after two soft resets; hard reset may be required";
    }

    result.response_message()
}

/// Request-scoped switch and its validated connection identity.
pub(crate) struct EphemeralSwitch {
    /// Switch client built from the caller-supplied `NodeInfo`.
    switch: Box<dyn SwitchScaleUpManagement>,

    /// Validated switch-facing target address from `host_endpoint`.
    target_ip: IpAddr,

    /// TLS server name from `host_endpoint`, falling back to `target_ip`.
    tls_server_name: String,
}

impl EphemeralSwitch {
    /// Borrows the underlying switch client (e.g. for a cold-reboot NVOS
    /// readiness probe).
    pub(crate) fn switch(&self) -> &dyn SwitchScaleUpManagement {
        self.switch.as_ref()
    }
}

pub(super) fn nmx_client_tls_with_endpoint_authority(
    mut tls: NmxcTlsConfig,
    endpoint_tls_server_name: &str,
) -> NmxcTlsConfig {
    if tls.authority.is_none() {
        tls.authority = Some(endpoint_tls_server_name.trim().to_owned());
    }

    tls
}

/// Restores insecure NMX-C manager mode when it is not already disabled.
///
/// Disabled clusters skip the readiness wait because the unset operation
/// tolerates that state and V2 retries it after enabling the selected switch.
/// An enabled cluster waits with
/// `SwitchGb200Nvidia::require_cluster_app_manager_action_ready` so RMS
/// does not SSH-restore encryption while the app reports unready. Certificate
/// bind is an NVUE `@update`; this restore is `nv action restore` plus
/// `nv config apply`. The wait is fail-closed, not a proven shared
/// configCallback. A ready app (`status: ok`) is enough; `nmxc-conn` down
/// does not block.
pub(super) async fn disable_insecure_nmx_controller_mtls(
    switch: &dyn SwitchScaleUpManagement,
    cancel: &CancellationToken,
) -> Result<()> {
    let Some(switch) = switch.as_switch_gb200() else {
        return Err(RmsError::invalid_argument(
            "NMX-C mTLS unset requires a GB200/GB300 switch",
        ));
    };

    let manager_mtls_disabled = matches!(
        switch
            .get_cluster_apps_status(NMX_CONTROLLER_APP_NAME)
            .await,
        Ok(status)
            if status
                .get("manager")
                .and_then(|manager| manager.get("encryption"))
                .and_then(|value| value.as_str())
                == Some("disabled")
    );

    if manager_mtls_disabled {
        tracing::info!(
            node = %switch.id(),
            "NMX-C manager mTLS already disabled; skipping restore action"
        );

        return Ok(());
    }

    let cluster_disabled = matches!(
        switch.get_cluster_state().await,
        Ok(cluster) if cluster.get("state").and_then(serde_json::Value::as_str) == Some("disabled")
    );

    // Disabled clusters reject manager actions. Skip their readiness wait;
    // the unset path tolerates that rejection, and V2 retries after enabling.
    // An enabled cluster reuses the certificate-bind fail-closed wait so RMS
    // does not SSH-restore while the app reports unready. NVUE bind is known
    // to fail in that state; SSH restore is a different transport and is
    // gated the same way until a mid-start restore is observed on hardware.
    if !cluster_disabled {
        switch
            .require_cluster_app_manager_action_ready("nmx-controller")
            .await?;
    }

    let report = switch
        .unset_mtls_services(&[SwitchMtlsService::ScaleUpFabricManager], cancel)
        .await
        .map_err(RmsError::from)?;

    if report.cancelled {
        let restored = report
            .services
            .iter()
            .any(|outcome| outcome.status == MtlsUnsetStatus::Cleared);
        let message = if restored {
            "NMX-C mTLS unset was cancelled after the restore reached the switch and before it was committed"
        } else {
            "NMX-C mTLS unset was cancelled before it changed the switch"
        };
        return Err(RmsError::cancelled(message));
    }

    Ok(())
}

/// Best-effort compensating teardown after a failed
/// `ConfigureScaleUpFabricManager`.
///
/// Once Step 2 has run, the switch's `nmx-controller` management gRPC is open
/// to external clients. If a later step fails and the handler returns
/// `Failure`, leaving that open would put the switch in a *more exposed* state
/// than the caller — who is told the configure failed — expects. Disabling
/// external gRPC again makes a failed configure fail closed.
///
/// Scope and caveats:
/// - Only the Step 2 exposure is undone. The Step 1 cluster-enabled state is
///   intentionally left in place: it is the less sensitive, frequently desired
///   durable state, and tearing down the whole cluster on a late-stage failure
///   is more disruptive than the residual it would remove.
/// - This is best-effort. The failure that triggers rollback is often the
///   switch being unreachable, in which case this teardown fails too; the error
///   is logged (not propagated) so the caller still sees the original failure.
/// - `resp.grpc_enabled` is set to `false` only when the disable actually
///   succeeds. A failed teardown leaves the flag unchanged so the response
///   never reports the port as closed when it may still be open.
async fn rollback_external_grpc(
    switch: &dyn SwitchScaleUpManagement,
    node_id: &str,
    resp: &mut rm::ConfigureScaleUpFabricManagerResponse,
) {
    match switch
        .enable_grpc_for_external_clients("nmx-controller", false)
        .await
    {
        Ok(_) => {
            resp.grpc_enabled = false;
            tracing::info!(
                node = %node_id,
                "rolled back external-client gRPC after failed ScaleUpFabric configure"
            )
        }
        Err(e) => tracing::warn!(
            node = %node_id,
            error = %e.message,
            "failed to roll back external-client gRPC after failed ScaleUpFabric configure; \
             switch may remain exposed until reconfigured or explicitly disabled"
        ),
    }
}

fn validate_switch_target_host(host: &str) -> Result<IpAddr> {
    let ip = host.parse::<IpAddr>().map_err(|_| {
        RmsError::invalid_argument(format!(
            "switch target host must be a valid IP address: {host}"
        ))
    })?;

    if is_disallowed_switch_target_ip(ip) {
        return Err(RmsError::invalid_argument(format!(
            "switch target host is not allowed: {ip}"
        )));
    }

    Ok(ip)
}

fn is_disallowed_switch_target_ip(ip: IpAddr) -> bool {
    // In-rack switches use RFC 1918 / unique-local space; every other IANA
    // special-purpose block is still rejected.
    is_special_purpose_ip(ip, PrivateUse::Allowed)
}

/// Helper function to get a specific value by key from the NMX controller's current static config.
/// Returns `Some(value)` when the key is present, or `None` when the RPC fails or the key is
/// absent from the response.
async fn get_nmx_static_config_value(
    client: &mut dyn Nmxc,
    config_file_name: &str,
    key: &str,
    gateway_id: &str,
) -> Option<String> {
    match client
        .get_static_config(config_file_name, key, gateway_id)
        .await
    {
        Ok(response) => {
            let value = find_static_config_value(&response, config_file_name, key);

            match &value {
                Some(v) => tracing::info!(
                    config_file_name,
                    key,
                    value = %v,
                    "NMX GetStaticConfig: key read"
                ),
                None => tracing::warn!(
                    config_file_name,
                    key,
                    "NMX GetStaticConfig: key missing from response"
                ),
            };

            value
        }
        Err(e) => {
            tracing::warn!(
                config_file_name,
                key,
                error = %e,
                "NMX GetStaticConfig: RPC failed"
            );

            None
        }
    }
}

/// Does the actual work of setting the NMX topology value in the static config.
/// Makes no changes if the desired topology is already configured.
/// Validates the final topology matches the desired topology after setting.
async fn call_nmx_set_static_config(
    client: &mut dyn Nmxc,
    topology: &str,
    gateway_id: &str,
) -> NmxSetStaticConfigResult {
    // Reject "unknown" early to avoid treating a missing/unreadable topology as a successful set.
    if topology == NMX_TOPOLOGY_UNKNOWN {
        tracing::error!(topology, "invalid topology value");
        return NmxSetStaticConfigResult::Failed;
    }

    let precheck_value =
        get_nmx_static_config_value(client, NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY, gateway_id)
            .await;

    let mut current_topology_str = String::from(NMX_TOPOLOGY_UNKNOWN);

    match precheck_value {
        Some(value) if value == topology => {
            // Short-circuit — already configured with the desired topology.
            tracing::info!(
                config_file_name = NMX_C_FM_CONFIG_FILE,
                key = NMX_C_TOPOLOGY_KEY,
                current_topology = value,
                desired_topology = topology,
                "NMX topology already configured, skipping"
            );
            return NmxSetStaticConfigResult::AlreadyConfigured;
        }
        Some(value) => {
            tracing::info!(
                config_file_name = NMX_C_FM_CONFIG_FILE,
                key = NMX_C_TOPOLOGY_KEY,
                current_topology = value,
                desired_topology = topology,
                "NMX topology differs; updating"
            );
            current_topology_str = value;
        }
        None => {
            tracing::warn!(
                config_file_name = NMX_C_FM_CONFIG_FILE,
                key = NMX_C_TOPOLOGY_KEY,
                desired_topology = topology,
                "NMX SetStaticConfig: could not read current topology; proceeding with update"
            );
        }
    }

    // Use the NMX client to set the topology directly.
    let set_returned_err = match client
        .set_static_config(
            NMX_C_FM_CONFIG_FILE,
            NMX_C_TOPOLOGY_KEY,
            topology,
            gateway_id,
        )
        .await
    {
        Ok(_) => {
            tracing::info!(
                config_file_name = NMX_C_FM_CONFIG_FILE,
                key = NMX_C_TOPOLOGY_KEY,
                gateway_id,
                current_topology = current_topology_str,
                desired_topology = topology,
                "NMX SetStaticConfig succeeded"
            );
            false
        }
        Err(e) => {
            tracing::warn!(
                config_file_name = NMX_C_FM_CONFIG_FILE,
                key = NMX_C_TOPOLOGY_KEY,
                gateway_id,
                current_topology = current_topology_str,
                desired_topology = topology,
                error = %e,
                "NMX SetStaticConfig failed"
            );
            true
        }
    };

    // After setting, get the current topology again to verify it was set correctly.
    let old_topology = current_topology_str;
    let postcheck_value =
        get_nmx_static_config_value(client, NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY, gateway_id)
            .await;

    let postcheck_unavailable = postcheck_value.is_none();
    let final_topology = postcheck_value.unwrap_or_else(|| String::from(NMX_TOPOLOGY_UNKNOWN));

    // Validate the final topology matches the desired topology.
    // Even if the set returned an error, if the get shows us that the topology
    // matches our desired value, we consider it a success.
    if final_topology == topology {
        tracing::info!(
            config_file_name = NMX_C_FM_CONFIG_FILE,
            key = NMX_C_TOPOLOGY_KEY,
            gateway_id,
            set_returned_err,
            old_topology,
            desired_topology = topology,
            current_topology = final_topology,
            "final topology matches desired topology"
        );
        return NmxSetStaticConfigResult::SetSucceeded;
    }

    // final topology != desired topology
    tracing::error!(
        config_file_name = NMX_C_FM_CONFIG_FILE,
        key = NMX_C_TOPOLOGY_KEY,
        gateway_id,
        set_returned_err,
        postcheck_unavailable,
        old_topology,
        desired_topology = topology,
        current_topology = final_topology,
        "final topology does not match desired topology",
    );

    NmxSetStaticConfigResult::Failed
}

/// Reconciles one non-topology static-config entry and verifies its readback.
///
/// A successful readback is authoritative when `SetStaticConfig` reports an
/// error after applying the requested value.
async fn set_nmx_static_config(
    client: &mut dyn Nmxc,
    config_file_name: &str,
    key: &str,
    desired_value: &str,
    gateway_id: &str,
) -> NmxSetStaticConfigResult {
    let previous_value =
        get_nmx_static_config_value(client, config_file_name, key, gateway_id).await;

    if previous_value.as_deref() == Some(desired_value) {
        tracing::info!(
            config_file_name,
            key,
            desired_value,
            "NMX static config already configured, skipping"
        );

        return NmxSetStaticConfigResult::AlreadyConfigured;
    }

    if let Err(error) = client
        .set_static_config(config_file_name, key, desired_value, gateway_id)
        .await
    {
        tracing::warn!(
            config_file_name,
            key,
            desired_value,
            error = %error,
            "NMX SetStaticConfig failed; checking readback"
        );
    }

    let final_value = get_nmx_static_config_value(client, config_file_name, key, gateway_id).await;

    if final_value.as_deref() == Some(desired_value) {
        NmxSetStaticConfigResult::SetSucceeded
    } else {
        tracing::error!(
            config_file_name,
            key,
            desired_value,
            current_value = final_value.as_deref().unwrap_or("<unavailable>"),
            "final static config does not match desired value"
        );

        NmxSetStaticConfigResult::Failed
    }
}

/// Retries a failed post-`SetStaticConfig` topology configuration.
///
/// The caller enters this path only after a `SetStaticConfig` attempt failed to
/// produce confirmed topology state. Recovery restarts NMX-C and re-runs the
/// idempotent config helper, which verifies current state, sets the topology
/// when needed, and post-validates.
async fn recover_nmx_topology_after_static_config_failure(
    switch: &dyn SwitchScaleUpManagement,
    client: &mut dyn Nmxc,
    switch_target: &str,
    topology: &str,
    gateway_id: &str,
) -> NmxSetStaticConfigResult {
    const WAIT_TIME: Duration = Duration::from_secs(2);

    for attempt in 1..=NMX_POST_VALIDATION_SOFT_RESET_ATTEMPTS {
        tracing::info!(topology, wait_time = ?WAIT_TIME, "waiting for the next soft-reset attempt");
        tokio::time::sleep(WAIT_TIME).await;

        tracing::warn!(
            attempt,
            max_attempts = NMX_POST_VALIDATION_SOFT_RESET_ATTEMPTS,
            topology,
            "soft resetting nmx-controller after topology configuration failure"
        );

        if let Err(e) = switch.restart_cluster_app(NMX_CONTROLLER_APP_NAME).await {
            tracing::warn!(
                attempt,
                error = %e.message,
                "nmx-controller soft reset failed"
            );

            continue;
        }

        if let Err(e) = wait_for_nmx_controller_ready_for_config(switch, switch_target).await {
            tracing::warn!(
                attempt,
                error = %e.message,
                "nmx-controller did not become ready after soft reset"
            );

            continue;
        }

        // The controller restarted, so re-establish the NMX session before
        // retrying the idempotent configuration flow.
        if let Err(e) = client.hello(gateway_id).await {
            tracing::warn!(attempt, error = %e, "NMX Hello failed after soft reset");
            continue;
        }

        match call_nmx_set_static_config(client, topology, gateway_id).await {
            result @ (NmxSetStaticConfigResult::AlreadyConfigured
            | NmxSetStaticConfigResult::SetSucceeded) => {
                tracing::info!(
                    attempt,
                    topology,
                    result = ?result,
                    "NMX topology configuration succeeded after soft reset"
                );

                return result;
            }
            result => tracing::warn!(
                attempt,
                topology,
                result = ?result,
                "NMX topology configuration still not confirmed after soft reset"
            ),
        }
    }

    NmxSetStaticConfigResult::Failed
}

/// Sets topology and extra static config sequentially on one NMX-C client.
///
/// A restarted controller is re-verified before this helper proceeds to extra
/// keys, so callers never use a client before topology is confirmed. Extra
/// entries retain request order, and a failure leaves earlier confirmed writes
/// applied. The result reports `SetSucceeded` when topology or any extra entry
/// changed, and `AlreadyConfigured` only when the complete batch already matched.
///
/// Confirmed writes to `fm_config`, including topology, require an activation
/// restart after all entries succeed. Unchanged entries do not require a restart.
async fn configure_nmx_fabric_static_config(
    switch: &dyn SwitchScaleUpManagement,
    client: &mut dyn Nmxc,
    switch_target: &str,
    topology: &str,
    extra_static_configs: &[rm::ScaleUpFabricStaticConfig],
    gateway_id: &str,
) -> std::result::Result<(NmxSetStaticConfigResult, bool), String> {
    // Topology gates the batch: extra keys are not attempted until its value is
    // confirmed, including any soft-reset recovery.
    let config_result = call_nmx_set_static_config(client, topology, gateway_id).await;
    let soft_reset_attempted = !config_result.is_success();

    let config_result = if soft_reset_attempted {
        recover_nmx_topology_after_static_config_failure(
            switch,
            client,
            switch_target,
            topology,
            gateway_id,
        )
        .await
    } else {
        config_result
    };

    if !config_result.is_success() {
        return Err(nmx_config_response_message(config_result, soft_reset_attempted).to_owned());
    }

    let mut extra_config_updated = false;
    // Readback confirms storage; restart NMX-C to activate a topology write.
    let mut fm_config_reconciled = config_result == NmxSetStaticConfigResult::SetSucceeded;

    // Each entry is read back before advancing. A mismatch stops later writes;
    // earlier confirmed entries remain applied.
    for entry in extra_static_configs {
        let result = set_nmx_static_config(
            client,
            &entry.config_file_name,
            &entry.key,
            &entry.value,
            gateway_id,
        )
        .await;

        if !result.is_success() {
            return Err(format!(
                "failed to set NMX Controller static config {}/{}",
                entry.config_file_name, entry.key
            ));
        }

        let set_succeeded = result == NmxSetStaticConfigResult::SetSucceeded;
        extra_config_updated |= set_succeeded;
        fm_config_reconciled |= set_succeeded && entry.config_file_name == NMX_C_FM_CONFIG_FILE;
    }

    if fm_config_reconciled {
        switch
            .restart_cluster_app(NMX_CONTROLLER_APP_NAME)
            .await
            .map_err(|error| {
                format!(
                    "failed to restart NMX Controller on switch target '{switch_target}': {}",
                    error.message
                )
            })?;

        tracing::info!(
            switch_target,
            config_file_name = NMX_C_FM_CONFIG_FILE,
            "restarted NMX Controller after static configuration reconciliation"
        );
    }

    let config_result = if extra_config_updated {
        NmxSetStaticConfigResult::SetSucceeded
    } else {
        config_result
    };

    Ok((config_result, soft_reset_attempted))
}

fn nmx_controller_app_ready_for_config(app_status: &serde_json::Value) -> bool {
    let status = app_status.get("status").and_then(serde_json::Value::as_str);
    let control_plane_state = ["additional-info", "addition-info"]
        .iter()
        .find_map(|field| app_status.get(*field).and_then(serde_json::Value::as_str));

    status == Some("ok")
        || control_plane_state.is_some_and(|state| NMX_CONTROL_PLANE_READY_STATES.contains(&state))
}

fn nmx_controller_grpc_ready_for_config(grpc_status: &serde_json::Value) -> bool {
    grpc_status
        .get("summary")
        .and_then(|summary| summary.get("ready_for_external_clients"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// Completes NMX-C Hello, optionally waiting through transient startup errors.
///
/// Only `Unavailable` is treated as a controller-startup signal. Other status
/// codes fail immediately.
async fn hello_nmx_controller(
    client: &mut dyn Nmxc,
    gateway_id: &str,
    retry_unavailable: bool,
) -> std::result::Result<(), String> {
    if !retry_unavailable {
        return client
            .hello(gateway_id)
            .await
            .map(|_| ())
            .map_err(|_| "NMX Controller Hello failed".to_owned());
    }

    let mut last_error = String::new();

    for attempt in 0..=NMX_CONTROLLER_READY_POLL_ATTEMPTS {
        match client.hello(gateway_id).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                if !matches!(
                    &error,
                    NmxcError::Status(status) if status.code() == tonic::Code::Unavailable
                ) {
                    return Err(format!("NMX Controller Hello failed: {error}"));
                }

                last_error = error.to_string();

                tracing::warn!(
                    attempt = attempt + 1,
                    max_attempts = NMX_CONTROLLER_READY_POLL_ATTEMPTS + 1,
                    error = %error,
                    "NMX Controller Hello is not ready"
                );
            }
        }

        if attempt < NMX_CONTROLLER_READY_POLL_ATTEMPTS {
            tokio::time::sleep(NMX_CONTROLLER_READY_POLL_DELAY).await;
        }
    }

    Err(format!("NMX Controller Hello failed: {last_error}"))
}

pub(super) async fn wait_for_nmx_controller_ready_for_config(
    switch: &dyn SwitchScaleUpManagement,
    switch_target: &str,
) -> Result<()> {
    let mut last_status = "nmx-controller readiness was not observed".to_owned();

    for poll in 0..=NMX_CONTROLLER_READY_POLL_ATTEMPTS {
        match switch
            .get_cluster_apps_status(NMX_CONTROLLER_APP_NAME)
            .await
        {
            Ok(app_status) if nmx_controller_app_ready_for_config(&app_status) => {
                match switch
                    .check_grpc_status(NMX_CONTROLLER_APP_NAME, switch_target)
                    .await
                {
                    Ok(grpc_status) if nmx_controller_grpc_ready_for_config(&grpc_status) => {
                        return Ok(());
                    }
                    Ok(_) => {
                        last_status = "nmx-controller gRPC endpoint is not ready".to_owned();
                    }
                    Err(e) => {
                        last_status =
                            format!("failed to check nmx-controller gRPC status: {}", e.message);
                    }
                }
            }
            Ok(_) => {
                last_status = "nmx-controller app status is not ready".to_owned();
            }
            Err(e) => {
                last_status = format!("failed to read nmx-controller app status: {}", e.message);
            }
        }

        if poll < NMX_CONTROLLER_READY_POLL_ATTEMPTS {
            tokio::time::sleep(NMX_CONTROLLER_READY_POLL_DELAY).await;
        }
    }

    Err(RmsError::internal(last_status))
}

// ── Ephemeral switch construction ─────────────────────────────────────

/// Build an ephemeral Switch from a `NodeInfo` using host-endpoint
/// credentials and validate the switch-facing target address.
fn build_ephemeral_switch(device: &rm::NodeInfo, insecure_bmc: bool) -> Result<EphemeralSwitch> {
    if device.node_id.is_empty() {
        return Err(RmsError::invalid_argument("device node_id is required"));
    }
    let node_type = proto_node_type_to_domain(device.r#type.unwrap_or_default())
        .filter(|node_type| node_type.kind() == NodeKind::Switch)
        .ok_or_else(|| RmsError::invalid_argument("device node type must be a switch"))?;

    let flat = flatten_node_info(device)?;

    if flat.creds_for_node_type(node_type).is_none() {
        return Err(RmsError::invalid_argument(format!(
            "device {} missing host credentials",
            device.node_id
        )));
    }

    // ScaleUpFabric is a direct host/NVUE workflow. Require host_endpoint for
    // the actual switch call; keep bmc_endpoint optional because this RPC does
    // not perform Redfish power.
    let bmc_endpoint = flat.optional_bmc_endpoint(insecure_bmc)?;
    let host_endpoint = flat.switch_host_management_endpoint()?;
    let tls_server_name = host_endpoint
        .endpoint
        .host_name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(&host_endpoint.endpoint.ip_address)
        .to_owned();

    let target_ip = validate_switch_target_host(&host_endpoint.endpoint.ip_address)?;

    tracing::debug!(
        node = %device.node_id,
        target_ip = %target_ip,
        "validated switch management IP for ScaleUpFabric"
    );

    let config = NodeConfig {
        id: device.node_id.clone(),
        node_type,
        bmc_endpoint,
        host_endpoint: Some(host_endpoint),
        expected_inventory: None,
        recovery: None,
        runtime_config: None,
    };
    let rack_id = device.rack_id.as_str();
    let switch = NodeInstance::create_switch_scale_up(&config, rack_id).map_err(|e| e.message);
    match switch {
        Ok(s) => Ok(EphemeralSwitch {
            switch: s,
            target_ip,
            tls_server_name,
        }),
        Err(msg) => Err(RmsError::internal(msg)),
    }
}

impl RackManagerServiceImpl {
    pub(crate) async fn build_ephemeral_switch(
        &self,
        device: &rm::NodeInfo,
        request_domain: Option<&str>,
    ) -> Result<EphemeralSwitch> {
        let switch = build_ephemeral_switch(device, self.insecure_bmc)?;

        self.initialize_nvue_client_with_options(
            switch.switch.nvue_client(),
            request_domain,
            Some(&switch.tls_server_name),
            false,
        )
        .await?;

        Ok(switch)
    }

    /// Connects to NMX Controller using switch TLS roots and completes `Hello`.
    ///
    /// The request domain selects client credentials and server-name validation;
    /// `Hello` confirms the authenticated endpoint accepts RMS requests.
    /// Callers may retry `Unavailable` while a newly enabled controller starts.
    async fn connect_nmx_controller(
        &self,
        switch_target: &str,
        switch_tls_server_name: &str,
        request_domain: Option<&str>,
        retry_unavailable_hello: bool,
    ) -> std::result::Result<Box<dyn Nmxc>, String> {
        let use_tls = !self.switch_tls_roots.insecure_switch;

        let nmx_tls = if use_tls {
            let nmx_server_name = self
                .switch_tls_roots
                .tls_server_name_for_endpoint(switch_tls_server_name, request_domain)?;

            let tls = nmx_client_tls_with_endpoint_authority(
                self.switch_tls_roots.resolve_client_tls(request_domain)?,
                &nmx_server_name,
            );

            Some(tls)
        } else {
            None
        };

        let endpoint = Endpoint::new(grpc_target_uri(switch_target, 9370, use_tls))
            .map_err(|error| format!("invalid NMX Controller target address: {error}"))?;

        let pool = NmxcClientPool::builder()
            .build()
            .map_err(|error| format!("failed to create NMX Controller client pool: {error}"))?;

        let mut client = pool
            .create_client(endpoint, nmx_tls.as_ref())
            .await
            .map_err(|error| format!("failed to connect to NMX Controller: {error}"))?;

        hello_nmx_controller(
            client.as_mut(),
            &self.nmx_gateway_id,
            retry_unavailable_hello,
        )
        .await?;

        Ok(client)
    }

    /// Configures one switch and applies all requested NMX-C static config.
    ///
    /// Topology and extra keys share the same authenticated NMX-C client. The
    /// client is scoped to this configuration attempt and dropped afterward.
    ///
    /// Insecure-switch mode restores NMX-C manager encryption after the
    /// readiness poll and before the first plaintext Hello.
    async fn configure_scale_up_fabric_manager_on_switch(
        &self,
        node_id: &str,
        ephemeral: &EphemeralSwitch,
        topology: &str,
        extra_static_configs: &[rm::ScaleUpFabricStaticConfig],
        request_domain: Option<&str>,
    ) -> rm::ConfigureScaleUpFabricManagerResponse {
        let switch = ephemeral.switch.as_ref();
        let switch_target = ephemeral.target_ip.to_string();

        let mut response = rm::ConfigureScaleUpFabricManagerResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            topology_used: String::new(),
            scale_up_fabric_state_enabled: false,
            grpc_enabled: false,
        };

        if let Err(error) = switch.set_cluster_state(true).await {
            tracing::warn!(node = node_id, error = %error.message, "set_cluster_state failed");
            response.message = format!("failed to enable cluster: {}", error.message);
            return response;
        }

        response.scale_up_fabric_state_enabled = true;

        if let Err(error) = switch
            .enable_grpc_for_external_clients(NMX_CONTROLLER_APP_NAME, true)
            .await
        {
            tracing::warn!(node = node_id, error = %error.message, "enable_grpc_for_external_clients failed");
            response.message = format!("failed to enable gRPC: {}", error.message);
            return response;
        }

        // The external-client gRPC port is open from here on. Record the
        // exposure immediately so every downstream failure path reports it
        // accurately: rollback clears this flag only on a successful teardown,
        // so if rollback fails (e.g. an unreachable switch) grpc_enabled stays
        // true instead of the initial false that would misreport the port as
        // closed while it is actually still open.
        response.grpc_enabled = true;

        // Cluster enablement can return before the app and external endpoint
        // are ready, so use the bounded readiness poll before opening NMX-C.
        if let Err(error) = wait_for_nmx_controller_ready_for_config(switch, &switch_target).await {
            tracing::warn!(
                node = node_id,
                switch_target,
                error = %error.message,
                "NMX Controller not ready for configuration"
            );

            response.message = "gRPC not ready for external clients".into();
            rollback_external_grpc(switch, node_id, &mut response).await;
            return response;
        }

        if self.switch_tls_roots.insecure_switch {
            // Restore manager encryption only after the controller is ready,
            // then connect without client mTLS.
            //
            // The V1 RPC reaching here is synchronous with no job behind it,
            // so there is no token to cancel against; the unset runs to
            // completion.
            if let Err(error) =
                disable_insecure_nmx_controller_mtls(switch, &CancellationToken::new()).await
            {
                tracing::warn!(node = node_id, error = %error.message, "failed to disable NMX-C mTLS");
                response.message = format!("failed to disable NMX-C mTLS: {}", error.message);
                rollback_external_grpc(switch, node_id, &mut response).await;
                return response;
            }
        }

        let mut client = match self
            .connect_nmx_controller(
                &switch_target,
                &ephemeral.tls_server_name,
                request_domain,
                true,
            )
            .await
        {
            Ok(client) => client,
            Err(message) => {
                tracing::error!(node = node_id, error = %message, "failed to connect to NMX Controller");
                response.message = message;
                rollback_external_grpc(switch, node_id, &mut response).await;
                return response;
            }
        };

        match configure_nmx_fabric_static_config(
            switch,
            client.as_mut(),
            &switch_target,
            topology,
            extra_static_configs,
            &self.nmx_gateway_id,
        )
        .await
        {
            Ok((result, soft_reset_attempted)) => {
                tracing::info!(
                    node = node_id,
                    topology,
                    result = ?result,
                    "NMX static config set succeeded"
                );

                response.status = rm::ReturnCode::Success.into();
                response.message = nmx_config_response_message(result, soft_reset_attempted).into();
            }

            Err(message) => {
                tracing::error!(node = node_id, topology, error = %message, "failed to set NMX static config");
                response.message = message;
                rollback_external_grpc(switch, node_id, &mut response).await;
            }
        }

        response.topology_used = topology.to_owned();
        response
    }
}

/// NVOS rejects cluster disable while cluster-node servers remain configured,
/// so a VR NVL72 switch clears the node IPs RackManagerV2 may have configured
/// before its cluster is disabled.
async fn set_switch_scale_up_fabric_state(
    switch: &dyn SwitchScaleUpManagement,
    enabled: bool,
) -> Result<()> {
    if !enabled
        && switch.node_type() == NodeType::SwitchVrnvl72Nvidia
        && let Some(switch) = switch.as_switch_gb200()
    {
        let capabilities = NvBridge::discover(switch).await?;

        return switch
            .clear_node_ips_and_disable_cluster(capabilities)
            .await;
    }

    switch.set_cluster_state(enabled).await
}

async fn batch_set_scale_up_fabric_state_for_device(
    service: &RackManagerServiceImpl,
    device: rm::NodeInfo,
    enabled: bool,
) -> rm::NodeOperationResult {
    let node_id = device.node_id.clone();
    let rack_id = device.rack_id.clone();
    tracing::info!(
        node = %node_id,
        rack = %rack_id,
        enabled,
        "setting scale-up fabric state"
    );

    let ephemeral = match service.build_ephemeral_switch(&device, None).await {
        Ok(ephemeral) => ephemeral,
        Err(e) => {
            let error_message = e.message;
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                error = %error_message,
                "failed to build switch for scale-up fabric state update"
            );
            return rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message,
            };
        }
    };

    let switch = ephemeral.switch;

    let _admission = match service
        .reserve_switch_mutation(switch.nvue_client(), &device)
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            return rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message: error.message,
            };
        }
    };

    match set_switch_scale_up_fabric_state(switch.as_ref(), enabled).await {
        Ok(()) => {
            tracing::info!(
                node = %node_id,
                rack = %rack_id,
                enabled,
                "scale-up fabric state update completed"
            );
            rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Success.into(),
                error_message: String::new(),
            }
        }
        Err(e) => {
            let error_message = e.message;
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                enabled,
                error = %error_message,
                "scale-up fabric state update failed"
            );
            rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message,
            }
        }
    }
}

// ── ScaleUpFabricManager RPC handlers ─────────────────────────────────

impl RackManagerServiceImpl {
    /// Complete ScaleUpFabricManager configuration on a single switch:
    /// enable cluster, enable gRPC for external clients, verify readiness,
    /// then issue NMX Hello + SetStaticConfig(topology) via an NMX gRPC channel.
    pub(crate) async fn handle_configure_scale_up_fabric_manager(
        &self,
        req: tonic::Request<rm::ConfigureScaleUpFabricManagerRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::ConfigureScaleUpFabricManagerResponse>,
        tonic::Status,
    > {
        let r = req.into_inner();

        let mut resp = rm::ConfigureScaleUpFabricManagerResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            topology_used: String::new(),
            scale_up_fabric_state_enabled: false,
            grpc_enabled: false,
        };

        if r.topology_type.is_empty() {
            resp.message = "topology_type is required".into();
            return Ok(tonic::Response::new(resp));
        }

        let device = match r.node.as_ref() {
            Some(d) => d,
            None => {
                resp.message = "device is required".into();
                return Ok(tonic::Response::new(resp));
            }
        };

        let request_domain = r.domain.as_deref();

        let ephemeral = match self.build_ephemeral_switch(device, request_domain).await {
            Ok(ephemeral) => ephemeral,
            Err(e) => {
                tracing::warn!(node = %device.node_id, error = %e.message, "failed to build ephemeral switch");
                resp.message = e.message;
                return Ok(tonic::Response::new(resp));
            }
        };

        let _admission = match self
            .reserve_switch_mutation(ephemeral.switch().nvue_client(), device)
            .await
        {
            Ok(reservation) => reservation,
            Err(error) => {
                resp.message = error.message;
                return Ok(tonic::Response::new(resp));
            }
        };

        resp = self
            .configure_scale_up_fabric_manager_on_switch(
                &device.node_id,
                &ephemeral,
                &r.topology_type,
                &[],
                request_domain,
            )
            .await;

        Ok(tonic::Response::new(resp))
    }

    /// Concurrently enable or disable ScaleUpFabric cluster state across
    /// caller-supplied switches, then persist the applied config.
    pub(crate) async fn handle_batch_set_scale_up_fabric_state(
        &self,
        req: tonic::Request<rm::BatchSetScaleUpFabricStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchSetScaleUpFabricStateResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let enabled = r.enabled;
        let devices: Vec<rm::NodeInfo> = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        let total_nodes = devices.len() as u32;
        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::new(),
            job_id: String::new(),
            stats: None,
        };

        if devices.is_empty() {
            batch.message = "nodes is required and must contain at least one device".into();
            return Ok(tonic::Response::new(
                batch_set_scale_up_fabric_state_response(batch, total_nodes, 0, 0),
            ));
        }

        tracing::info!(
            enabled,
            device_count = devices.len(),
            "set scale-up fabric state request received"
        );

        let node_results = join_all(
            devices
                .into_iter()
                .map(|device| batch_set_scale_up_fabric_state_for_device(self, device, enabled)),
        )
        .await;

        let successful_nodes = node_results
            .iter()
            .filter(|result| result.status == rm::ReturnCode::Success as i32)
            .count() as u32;
        let failed_nodes = total_nodes - successful_nodes;
        batch.node_results = node_results;

        batch.status = if failed_nodes == 0 {
            rm::ReturnCode::Success.into()
        } else {
            rm::ReturnCode::Failure.into()
        };
        let state = if enabled { "enabled" } else { "disabled" };
        batch.message = if failed_nodes == 0 {
            format!("cluster state {state} on all switches")
        } else {
            format!("cluster state {state} failed on one or more switches")
        };

        Ok(tonic::Response::new(
            batch_set_scale_up_fabric_state_response(
                batch,
                total_nodes,
                successful_nodes,
                failed_nodes,
            ),
        ))
    }

    /// Query cluster-health / fabric-manager status on a caller-supplied
    /// set of switches. Returns a per-`node_id` map with each switch's
    /// `nmx-controller` cluster-apps status (or an error message).
    pub(crate) async fn handle_batch_get_scale_up_fabric_service_status(
        &self,
        req: tonic::Request<rm::BatchGetScaleUpFabricServiceStatusRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::BatchGetScaleUpFabricServiceStatusResponse>,
        tonic::Status,
    > {
        let r = req.into_inner();
        let mut resp = rm::BatchGetScaleUpFabricServiceStatusResponse {
            status: rm::ReturnCode::Failure.into(),
            service_statuses: HashMap::new(),
            stats: None,
        };

        let devices: Vec<rm::NodeInfo> = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        let total_nodes = devices.len() as u32;
        if devices.is_empty() {
            resp.service_statuses = HashMap::new();
            resp.stats = Some(rm::NodeOperationStats {
                total_nodes: 0,
                successful_nodes: 0,
                failed_nodes: 0,
            });
            return Ok(tonic::Response::new(resp));
        }

        for device in devices {
            let node_id = device.node_id.clone();
            let entry = match self.build_ephemeral_switch(&device, None).await {
                Ok(ephemeral) => {
                    let sw = ephemeral.switch;
                    match sw.get_cluster_apps_status("nmx-controller").await {
                        Ok(json) => rm::ScaleUpFabricServiceStatusEntry {
                            status_json: json.to_string(),
                            error_message: String::new(),
                        },
                        Err(e) => rm::ScaleUpFabricServiceStatusEntry {
                            status_json: String::new(),
                            error_message: e.message,
                        },
                    }
                }
                Err(e) => rm::ScaleUpFabricServiceStatusEntry {
                    status_json: String::new(),
                    error_message: e.message,
                },
            };
            resp.service_statuses.insert(node_id, entry);
        }

        resp.status = rm::ReturnCode::Success.into();
        let failed_nodes = resp
            .service_statuses
            .values()
            .filter(|entry| !entry.error_message.is_empty())
            .count() as u32;
        resp.stats = Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes: total_nodes.saturating_sub(failed_nodes),
            failed_nodes,
        });
        Ok(tonic::Response::new(resp))
    }

    /// Return the cluster (ScaleUpFabric) state for a single caller-supplied
    /// switch.
    pub(crate) async fn handle_get_scale_up_fabric_state(
        &self,
        req: tonic::Request<rm::GetScaleUpFabricStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetScaleUpFabricStateResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut resp = rm::GetScaleUpFabricStateResponse {
            status: rm::ReturnCode::Failure.into(),
            state_json: String::new(),
            error_message: String::new(),
        };

        let device = match r.node.as_ref() {
            Some(d) => d,
            None => {
                resp.error_message = "device is required".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let sw = match self.build_ephemeral_switch(device, None).await {
            Ok(ephemeral) => ephemeral.switch,
            Err(e) => {
                resp.error_message = e.message;
                return Ok(tonic::Response::new(resp));
            }
        };

        match sw.get_cluster_state().await {
            Ok(json) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.state_json = json.to_string();
            }
            Err(e) => {
                tracing::warn!(node = %device.node_id, error = %e.message, "get_cluster_state failed");
                resp.error_message = e.message;
            }
        }
        Ok(tonic::Response::new(resp))
    }

    /// Enable or disable the FabricTelemetryInterface (gnmi service) on a
    /// caller-supplied switch.
    pub(crate) async fn handle_set_scale_up_fabric_telemetry_interface_state(
        &self,
        req: tonic::Request<rm::SetScaleUpFabricTelemetryInterfaceStateRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::SetScaleUpFabricTelemetryInterfaceStateResponse>,
        tonic::Status,
    > {
        let r = req.into_inner();
        let mut resp = rm::SetScaleUpFabricTelemetryInterfaceStateResponse {
            status: rm::ReturnCode::Failure.into(),
            result_json: String::new(),
            error_message: String::new(),
        };

        let device = match r.node.as_ref() {
            Some(d) => d,
            None => {
                resp.error_message = "device is required".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let sw = match self.build_ephemeral_switch(device, None).await {
            Ok(ephemeral) => ephemeral.switch,
            Err(e) => {
                resp.error_message = e.message;
                return Ok(tonic::Response::new(resp));
            }
        };

        let _admission = match self.reserve_switch_mutation(sw.nvue_client(), device).await {
            Ok(reservation) => reservation,
            Err(error) => {
                resp.error_message = error.message;
                return Ok(tonic::Response::new(resp));
            }
        };

        match sw.gnmi_service(r.enable).await {
            Ok(json) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.result_json = json.to_string();
            }
            Err(e) => {
                tracing::warn!(node = %device.node_id, error = %e.message, "gnmi_service failed");
                resp.error_message = e.message;
            }
        }
        Ok(tonic::Response::new(resp))
    }
}

fn batch_set_scale_up_fabric_state_response(
    mut batch: rm::NodeBatchResponse,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) -> rm::BatchSetScaleUpFabricStateResponse {
    batch.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });

    rm::BatchSetScaleUpFabricStateResponse {
        response: Some(batch),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use serde_json::Value;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
    use crate::domain::node::{Node, NodeType};
    use crate::libnmxc::test_support::FakeNmxc;
    use crate::libnmxc::{NmxcError, nmxc_model};
    use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;
    use crate::orchestrator::job_tracker::JobTracker;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use crate::utilities::error::ErrorCode;

    #[derive(Default)]
    struct FakeScaleUpSwitch {
        restart_calls: AtomicUsize,
        restart_results: Mutex<VecDeque<Result<()>>>,
        app_status_calls: AtomicUsize,
        grpc_status_calls: AtomicUsize,
        app_status_results: Mutex<VecDeque<Result<Value>>>,
        grpc_status_results: Mutex<VecDeque<Result<Value>>>,
    }

    impl Node for FakeScaleUpSwitch {
        fn id(&self) -> &str {
            "sw-01"
        }

        fn rack_id(&self) -> &str {
            "rack-01"
        }

        fn node_type(&self) -> NodeType {
            NodeType::SwitchGb200Nvidia
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    #[async_trait::async_trait]
    impl SwitchScaleUpManagement for FakeScaleUpSwitch {
        fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
            None
        }

        async fn set_cluster_state(&self, _enabled: bool) -> Result<()> {
            Err(RmsError::internal("unexpected set_cluster_state call"))
        }

        async fn get_cluster_state(&self) -> Result<Value> {
            Err(RmsError::internal("unexpected get_cluster_state call"))
        }

        async fn nvue_hello(&self) -> Result<()> {
            Err(RmsError::internal("unexpected nvue_hello call"))
        }

        async fn get_cluster_apps_status(&self, _app_name: &str) -> Result<Value> {
            self.app_status_calls.fetch_add(1, Ordering::SeqCst);

            self.app_status_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(nmx_controller_ready_app_status()))
        }

        async fn enable_grpc_for_external_clients(
            &self,
            _app_name: &str,
            _enabled: bool,
        ) -> Result<Value> {
            Err(RmsError::internal(
                "unexpected enable_grpc_for_external_clients call",
            ))
        }

        async fn gnmi_service(&self, _enabled: bool) -> Result<Value> {
            Err(RmsError::internal("unexpected gnmi_service call"))
        }

        async fn check_grpc_status(&self, _app_name: &str, _target_switch: &str) -> Result<Value> {
            self.grpc_status_calls.fetch_add(1, Ordering::SeqCst);

            self.grpc_status_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(nmx_controller_ready_grpc_status()))
        }

        async fn restart_cluster_app(&self, app_name: &str) -> Result<()> {
            assert_eq!(app_name, NMX_CONTROLLER_APP_NAME);
            self.restart_calls.fetch_add(1, Ordering::SeqCst);

            self.restart_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(()))
        }

        async fn reset_sdn_factory_default(&self) -> Result<String> {
            Err(RmsError::internal(
                "unexpected reset_sdn_factory_default call",
            ))
        }
    }

    fn nmx_controller_ready_app_status() -> Value {
        serde_json::json!({
            "status": "ok",
            "addition-info": "CONTROL_PLANE_STATE_CONFIGURED",
            "manager": {
                "state": "enabled",
            },
        })
    }

    fn nmx_controller_waiting_app_status() -> Value {
        serde_json::json!({
            "status": "starting",
            "addition-info": "CONTROL_PLANE_STATE_STARTING",
            "manager": {
                "state": "enabled",
            },
        })
    }

    fn nmx_controller_ready_grpc_status() -> Value {
        serde_json::json!({
            "summary": {
                "ready_for_external_clients": true,
            },
        })
    }

    fn nmx_controller_waiting_grpc_status() -> Value {
        serde_json::json!({
            "summary": {
                "ready_for_external_clients": false,
            },
        })
    }

    /// Records `enable_grpc_for_external_clients` calls so the compensating
    /// teardown can be asserted, with a configurable result so the best-effort
    /// error-swallow path is exercised too. All other trait methods are
    /// unexpected: rollback must touch external gRPC only.
    struct RollbackRecordingSwitch {
        grpc_calls: Mutex<Vec<(String, bool)>>,
        fail_disable: bool,
    }

    impl RollbackRecordingSwitch {
        fn new(fail_disable: bool) -> Self {
            Self {
                grpc_calls: Mutex::new(Vec::new()),
                fail_disable,
            }
        }
    }

    impl Node for RollbackRecordingSwitch {
        fn id(&self) -> &str {
            "sw-01"
        }

        fn rack_id(&self) -> &str {
            "rack-01"
        }

        fn node_type(&self) -> NodeType {
            NodeType::SwitchGb200Nvidia
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    #[async_trait::async_trait]
    impl SwitchScaleUpManagement for RollbackRecordingSwitch {
        fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
            None
        }

        async fn set_cluster_state(&self, _enabled: bool) -> Result<()> {
            Err(RmsError::internal(
                "rollback must not touch cluster state (set_cluster_state)",
            ))
        }

        async fn get_cluster_state(&self) -> Result<Value> {
            Err(RmsError::internal("unexpected get_cluster_state call"))
        }

        async fn nvue_hello(&self) -> Result<()> {
            Err(RmsError::internal("unexpected nvue_hello call"))
        }

        async fn get_cluster_apps_status(&self, _app_name: &str) -> Result<Value> {
            Err(RmsError::internal(
                "unexpected get_cluster_apps_status call",
            ))
        }

        async fn enable_grpc_for_external_clients(
            &self,
            app_name: &str,
            enabled: bool,
        ) -> Result<Value> {
            self.grpc_calls
                .lock()
                .unwrap()
                .push((app_name.to_owned(), enabled));

            if self.fail_disable {
                Err(RmsError::internal("switch unreachable"))
            } else {
                Ok(serde_json::json!({}))
            }
        }

        async fn gnmi_service(&self, _enabled: bool) -> Result<Value> {
            Err(RmsError::internal("unexpected gnmi_service call"))
        }

        async fn check_grpc_status(&self, _app_name: &str, _target_switch: &str) -> Result<Value> {
            Err(RmsError::internal("unexpected check_grpc_status call"))
        }

        async fn restart_cluster_app(&self, _app_name: &str) -> Result<()> {
            Err(RmsError::internal("unexpected restart_cluster_app call"))
        }

        async fn reset_sdn_factory_default(&self) -> Result<String> {
            Err(RmsError::internal(
                "unexpected reset_sdn_factory_default call",
            ))
        }
    }

    fn failed_configure_response() -> rm::ConfigureScaleUpFabricManagerResponse {
        rm::ConfigureScaleUpFabricManagerResponse {
            status: rm::ReturnCode::Failure.into(),
            message: "gRPC not ready for external clients".into(),
            topology_used: String::new(),
            scale_up_fabric_state_enabled: true,
            grpc_enabled: true,
        }
    }

    #[tokio::test]
    async fn rollback_external_grpc_disables_external_clients_and_clears_flag() {
        let switch = RollbackRecordingSwitch::new(false);
        let mut resp = failed_configure_response();

        rollback_external_grpc(&switch, "sw-01", &mut resp).await;

        assert!(
            !resp.grpc_enabled,
            "rollback must clear grpc_enabled so the response reflects the disabled state"
        );
        assert_eq!(
            *switch.grpc_calls.lock().unwrap(),
            vec![("nmx-controller".to_owned(), false)],
            "rollback must disable external-client gRPC for nmx-controller exactly once"
        );
        // Scoped teardown: cluster state is intentionally left enabled. The
        // fake's set_cluster_state errors if called, so reaching this assert
        // proves rollback never attempted a cluster teardown.
        assert!(resp.scale_up_fabric_state_enabled);
    }

    #[tokio::test]
    async fn rollback_external_grpc_swallows_switch_errors() {
        let switch = RollbackRecordingSwitch::new(true);
        let mut resp = failed_configure_response();

        // Best-effort: an unreachable switch (the common trigger for rollback)
        // must not panic or propagate, so the caller still surfaces the original
        // failure.
        rollback_external_grpc(&switch, "sw-01", &mut resp).await;

        // Fail closed *and* report honestly: because the disable failed, the
        // switch may still be exposed, so grpc_enabled must NOT be cleared. Only
        // a successful teardown flips it to false; otherwise the response would
        // claim the port is closed when it may still be open.
        assert!(
            resp.grpc_enabled,
            "a failed rollback must leave grpc_enabled=true so the caller is not \
             told the port is closed when it may still be open"
        );
        assert_eq!(
            *switch.grpc_calls.lock().unwrap(),
            vec![("nmx-controller".to_owned(), false)],
            "rollback must still attempt the disable even when it will fail"
        );
    }

    #[test]
    fn validate_switch_target_host_accepts_regular_ip_addresses() {
        assert_eq!(
            validate_switch_target_host("10.0.0.11").unwrap(),
            "10.0.0.11".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            validate_switch_target_host("fd00::1").unwrap(),
            "fd00::1".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn validate_switch_target_host_rejects_non_ip_hosts() {
        let err = validate_switch_target_host("switch.example.com").unwrap_err();
        assert!(err.message.contains("valid IP address"));
    }

    #[test]
    fn validate_switch_target_host_rejects_unsafe_addresses() {
        for host in [
            "127.0.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "169.254.1.10",
            "::1",
            "::",
            "ff02::1",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(
                validate_switch_target_host(host).is_err(),
                "{host} should be rejected"
            );
        }
    }

    fn switch_node_info(host_ip_address: &str, host_name: Option<&str>) -> rm::NodeInfo {
        switch_node_info_with_type(host_ip_address, host_name, rm::NodeType::SwitchGb200Nvidia)
    }

    async fn submit_sdn_reset(
        service: &RackManagerServiceImpl,
        nodes: Vec<rm::NodeInfo>,
    ) -> rm::BatchResetSwitchSdnFactoryDefaultResponse {
        service
            .handle_batch_reset_switch_sdn_factory_default(tonic::Request::new(
                rm::BatchResetSwitchSdnFactoryDefaultRequest {
                    nodes: Some(rm::NodeSet { nodes }),
                    domain: Some("rack-a".into()),
                },
            ))
            .await
            .unwrap()
            .into_inner()
    }

    fn test_service(job_tracker: Arc<JobTracker>) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker,
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: std::path::PathBuf::from("firmware"),
            artifact_require_sha256: false,
            insecure_bmc: true,
            artifact_download_policy:
                crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
            switch_tls_roots: SwitchTlsRoots {
                insecure_switch: true,
                ..SwitchTlsRoots::default()
            },
            sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
            workflow_timings: crate::config::WorkflowTimings::default(),
            recovery_profiles: crate::config::RecoveryProfiles::default(),
            fabric_layout_catalog: std::sync::Arc::new(
                crate::config::fabric_layout::FabricLayoutCatalog::default(),
            ),
            hardware_profiles: Arc::new(
                crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
            ),
            fabric_operations: std::sync::Arc::new(Default::default()),
        }
    }

    fn switch_node_info_with_type(
        host_ip_address: &str,
        host_name: Option<&str>,
        node_type: rm::NodeType,
    ) -> rm::NodeInfo {
        rm::NodeInfo {
            node_id: "sw-01".into(),
            rack_id: "rack-01".into(),
            r#type: Some(node_type as i32),
            bmc_endpoint: None,
            host_endpoint: Some(rm::Endpoint {
                interface: Some(rm::NetworkInterface {
                    ip_address: host_ip_address.into(),
                    mac_address: String::new(),
                    host_name: host_name.map(str::to_owned),
                }),
                port: 443,
                credentials: Some(rm::Credentials {
                    auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                        username: "admin".into(),
                        password: "password".into(),
                    })),
                }),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn build_ephemeral_switch_accepts_regular_host_endpoint() {
        let ephemeral = build_ephemeral_switch(
            &switch_node_info("10.0.0.11", Some(" switch.example.com ")),
            true,
        )
        .unwrap();

        assert_eq!(ephemeral.target_ip, "10.0.0.11".parse::<IpAddr>().unwrap());
        assert_eq!(ephemeral.tls_server_name, "switch.example.com");
    }

    #[test]
    fn build_ephemeral_switch_accepts_legacy_endpoint_without_host_name() {
        let ephemeral = build_ephemeral_switch(&switch_node_info("10.0.0.11", None), true).unwrap();

        assert_eq!(ephemeral.target_ip, "10.0.0.11".parse::<IpAddr>().unwrap());
        assert_eq!(ephemeral.tls_server_name, "10.0.0.11");
    }

    #[test]
    fn build_ephemeral_switch_accepts_gb300_switch_type_and_nvue() {
        let ephemeral = build_ephemeral_switch(
            &switch_node_info_with_type(
                "10.0.0.11",
                Some("switch.example.com"),
                rm::NodeType::SwitchGb300Nvidia,
            ),
            true,
        )
        .unwrap();

        assert_eq!(ephemeral.switch.node_type(), NodeType::SwitchGb300Nvidia);
        assert!(ephemeral.switch.nvue_client().is_some());
    }

    #[test]
    fn build_ephemeral_switch_rejects_unsafe_host_endpoint() {
        let Err(err) = build_ephemeral_switch(
            &switch_node_info("127.0.0.1", Some("switch.example.com")),
            true,
        ) else {
            panic!("expected unsafe host endpoint to be rejected");
        };

        assert!(err.message.contains("switch target host is not allowed"));
    }

    #[test]
    fn build_ephemeral_switch_rejects_cgnat_host_endpoint() {
        let Err(err) = build_ephemeral_switch(
            &switch_node_info("100.64.0.1", Some("switch.example.com")),
            true,
        ) else {
            panic!("expected CGNAT host endpoint to be rejected");
        };

        assert!(err.message.contains("switch target host is not allowed"));
    }

    #[tokio::test]
    async fn batch_reset_switch_sdn_factory_default_rejects_invalid_targets() {
        let service = test_service(Arc::new(JobTracker::new()));
        let mut node = switch_node_info("10.0.0.11", Some("switch.example.com"));
        node.rack_id = " ".into();

        let response = submit_sdn_reset(&service, vec![node]).await;
        let batch = response.response.unwrap();

        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert_eq!(batch.node_results.len(), 1);

        assert_eq!(
            batch.node_results[0].error_message,
            "device rack_id is required"
        );

        let mut first = switch_node_info("127.0.0.1", Some("switch.example.com"));
        first.node_id = "switch-01".into();
        let second = first.clone();

        let response = submit_sdn_reset(&service, vec![first, second]).await;
        let batch = response.response.unwrap();

        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert_eq!(batch.node_results.len(), 2);

        assert!(
            batch
                .node_results
                .iter()
                .any(|result| result.error_message.contains("duplicate target"))
        );
    }

    #[tokio::test]
    async fn disable_insecure_nmx_controller_mtls_skips_readiness_when_cluster_disabled()
    -> Result<()> {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "manager": {
                    "encryption": "mtls",
                },
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "disabled",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed_commands = Arc::clone(&commands);

        let switch =
            SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(move |command| {
                observed_commands.lock().unwrap().push(command.to_owned());

                Ok(String::new())
            });

        disable_insecure_nmx_controller_mtls(&switch, &CancellationToken::new()).await?;

        assert_eq!(
            *commands.lock().unwrap(),
            vec![
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv config apply --assume-yes",
                "nv config save",
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn disable_insecure_nmx_controller_mtls_skips_restore_when_already_disabled() -> Result<()>
    {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "manager": {
                    "encryption": "disabled",
                },
            })))
            .expect(1)
            .mount(&server)
            .await;

        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed_commands = Arc::clone(&commands);

        let switch =
            SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(move |command| {
                observed_commands.lock().unwrap().push(command.to_owned());

                Ok(String::new())
            });

        disable_insecure_nmx_controller_mtls(&switch, &CancellationToken::new()).await?;

        assert!(commands.lock().unwrap().is_empty());

        Ok(())
    }

    /// If the cluster is enabled and the app stays unready for the full retry
    /// budget, RMS must not run `nv action restore … manager encryption`.
    /// Certificate bind uses NVUE `@update`; this restore is SSH. The wait is
    /// fail-closed, not a proven shared configCallback.
    #[tokio::test]
    async fn disable_insecure_nmx_controller_mtls_does_not_restore_encryption_when_the_app_stays_unready()
     {
        let server = MockServer::start().await;
        let starting = serde_json::json!({
            "status": "not ok",
            "additional-info": "CONTROL_PLANE_STATE_UNDEFINED",
            "manager": {
                "state": "enabled",
                "encryption": "mtls",
            },
        });
        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(starting))
            .named("starting GET /nvue_v1/cluster/apps/nmx-controller")
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "enabled",
            })))
            .named("enabled GET /nvue_v1/cluster")
            .mount(&server)
            .await;

        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed_commands = Arc::clone(&commands);
        let switch =
            SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(move |command| {
                observed_commands.lock().unwrap().push(command.to_owned());
                Ok(String::new())
            });

        let error = disable_insecure_nmx_controller_mtls(&switch, &CancellationToken::new())
            .await
            .expect_err("unready app must fail closed before encryption restore");
        assert_eq!(
            error.message,
            "cluster app nmx-controller did not become ready for manager configuration"
        );
        assert!(
            commands.lock().unwrap().is_empty(),
            "must not SSH-restore manager encryption while the app is unready; got {:?}",
            commands.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn disable_insecure_nmx_controller_mtls_restores_when_app_is_ok_and_nmxc_conn_is_down()
    -> Result<()> {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "manager": {
                    "state": "enabled",
                    "encryption": "mtls",
                },
            })))
            .named("ready GET /nvue_v1/cluster/apps/nmx-controller while nmxc-conn is down")
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "enabled",
                "nmxc-conn": {"state": "down"},
            })))
            .named("enabled cluster GET while ready app keeps nmxc-conn down")
            .mount(&server)
            .await;

        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed_commands = Arc::clone(&commands);
        let switch =
            SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(move |command| {
                observed_commands.lock().unwrap().push(command.to_owned());
                Ok(String::new())
            });

        disable_insecure_nmx_controller_mtls(&switch, &CancellationToken::new()).await?;

        assert_eq!(
            *commands.lock().unwrap(),
            vec![
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv config apply --assume-yes",
                "nv config save",
            ]
        );

        Ok(())
    }

    /// A cancel after the restore command has reached the switch must not
    /// look like a completed disable: the change is live and uncommitted.
    #[tokio::test]
    async fn disable_insecure_nmx_controller_mtls_reports_cancellation_after_restore() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "manager": { "encryption": "mtls" },
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "disabled",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let cancel = CancellationToken::new();
        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed_commands = Arc::clone(&commands);
        let trigger = cancel.clone();
        let switch =
            SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(move |command| {
                observed_commands.lock().unwrap().push(command.to_owned());
                trigger.cancel();
                Ok(String::new())
            });

        let error = disable_insecure_nmx_controller_mtls(&switch, &cancel)
            .await
            .expect_err("a cancelled restore must not be reported as success");

        assert_eq!(error.code, ErrorCode::Cancelled);
        assert_eq!(
            error.message,
            "NMX-C mTLS unset was cancelled after the restore reached the switch and before it was committed"
        );
        assert_eq!(
            *commands.lock().unwrap(),
            vec!["nv action restore cluster apps nmx-controller manager encryption"]
        );
    }

    #[tokio::test]
    async fn disable_insecure_nmx_controller_mtls_reports_cancellation_before_any_command() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "manager": { "encryption": "mtls" },
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "disabled",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed_commands = Arc::clone(&commands);
        let switch =
            SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(move |command| {
                observed_commands.lock().unwrap().push(command.to_owned());
                Ok(String::new())
            });
        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = disable_insecure_nmx_controller_mtls(&switch, &cancel)
            .await
            .expect_err("cancelling before the unset starts must not be reported as success");

        assert_eq!(error.code, ErrorCode::Cancelled);
        assert_eq!(
            error.message,
            "NMX-C mTLS unset was cancelled before it changed the switch"
        );
        assert!(commands.lock().unwrap().is_empty());
    }

    #[test]
    fn nmx_client_tls_prefers_existing_authority_then_endpoint_host() {
        for (authority, expected) in [
            (None, "switch.example.com"),
            (Some("rack.example.com".to_owned()), "rack.example.com"),
        ] {
            let tls = nmx_client_tls_with_endpoint_authority(
                NmxcTlsConfig {
                    authority,
                    ..NmxcTlsConfig::default()
                },
                " switch.example.com ",
            );

            assert_eq!(tls.authority.as_deref(), Some(expected));
        }
    }

    #[test]
    fn failed_static_config_result_is_not_success() {
        assert!(!NmxSetStaticConfigResult::Failed.is_success());
    }

    #[test]
    fn successful_static_config_results_are_success() {
        for result in [
            NmxSetStaticConfigResult::AlreadyConfigured,
            NmxSetStaticConfigResult::SetSucceeded,
        ] {
            assert!(result.is_success(), "{result:?} should be successful");
        }
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_skips_already_configured_topology() {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([Ok(FakeNmxc::static_config_response(
                NMX_C_FM_CONFIG_FILE,
                NMX_C_TOPOLOGY_KEY,
                "topology-a",
            ))]),
            ..FakeNmxc::default()
        };

        let result = call_nmx_set_static_config(
            &mut client,
            "topology-a",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::AlreadyConfigured);
        assert!(client.set_calls.is_empty());
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_sets_changed_topology() {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-b",
                )),
            ]),
            set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
            ..FakeNmxc::default()
        };

        let result = call_nmx_set_static_config(
            &mut client,
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::SetSucceeded);
        assert_eq!(client.set_calls.len(), 1);
        assert_eq!(client.set_calls[0].2, "topology-b");
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_accepts_postcheck_match_after_set_error() {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::empty_static_config_response()),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-b",
                )),
            ]),
            set_results: VecDeque::from([Err(NmxcError::NmxReturnCode {
                return_code: nmxc_model::StReturnCode::NmxStGenericError as i32,
                operation: "SetStaticConfig",
            })]),
            ..FakeNmxc::default()
        };

        let result = call_nmx_set_static_config(
            &mut client,
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::SetSucceeded);
        assert_eq!(client.set_calls.len(), 1);
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_reports_failed_set_without_postcheck_match() {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::empty_static_config_response()),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
            ]),
            set_results: VecDeque::from([Err(NmxcError::NmxReturnCode {
                return_code: nmxc_model::StReturnCode::NmxStGenericError as i32,
                operation: "SetStaticConfig",
            })]),
            ..FakeNmxc::default()
        };

        let result = call_nmx_set_static_config(
            &mut client,
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::Failed);
        assert!(!result.is_success());
        assert_eq!(client.set_calls.len(), 1);
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_rejects_unknown_topology() {
        let mut client = FakeNmxc::default();

        let result = call_nmx_set_static_config(
            &mut client,
            NMX_TOPOLOGY_UNKNOWN,
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::Failed);
        assert!(client.set_calls.is_empty());
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_reports_post_validation_mismatch() {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
            ]),
            set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
            ..FakeNmxc::default()
        };

        let result = call_nmx_set_static_config(
            &mut client,
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::Failed);
        assert!(!result.is_success());
        assert_eq!(client.set_calls.len(), 1);
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_reports_post_validation_rpc_failure() {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Err(NmxcError::NmxReturnCode {
                    return_code: nmxc_model::StReturnCode::NmxStGenericError as i32,
                    operation: "GetStaticConfig",
                }),
            ]),
            set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
            ..FakeNmxc::default()
        };

        let result = call_nmx_set_static_config(
            &mut client,
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::Failed);
        assert!(!result.is_success());
        assert_eq!(client.set_calls.len(), 1);
    }

    #[tokio::test]
    async fn static_config_recovery_soft_resets_until_topology_is_observed() {
        let switch = FakeScaleUpSwitch::default();

        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-b",
                )),
            ]),
            set_results: VecDeque::from([
                Ok(FakeNmxc::success_return_code()),
                Ok(FakeNmxc::success_return_code()),
            ]),
            ..FakeNmxc::default()
        };

        let result = recover_nmx_topology_after_static_config_failure(
            &switch,
            &mut client,
            "10.0.0.11",
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::SetSucceeded);
        assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 2);
        assert_eq!(switch.app_status_calls.load(Ordering::SeqCst), 2);
        assert_eq!(switch.grpc_status_calls.load(Ordering::SeqCst), 2);
        assert_eq!(client.set_calls.len(), 2);
    }

    #[tokio::test]
    async fn static_config_recovery_offers_hard_reset_after_two_failures() {
        let switch = FakeScaleUpSwitch::default();

        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::empty_static_config_response()),
                Ok(FakeNmxc::empty_static_config_response()),
                Ok(FakeNmxc::empty_static_config_response()),
                Ok(FakeNmxc::empty_static_config_response()),
            ]),
            set_results: VecDeque::from([
                Ok(FakeNmxc::success_return_code()),
                Ok(FakeNmxc::success_return_code()),
            ]),
            ..FakeNmxc::default()
        };

        let result = recover_nmx_topology_after_static_config_failure(
            &switch,
            &mut client,
            "10.0.0.11",
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::Failed);
        assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 2);
        assert_eq!(switch.app_status_calls.load(Ordering::SeqCst), 2);
        assert_eq!(switch.grpc_status_calls.load(Ordering::SeqCst), 2);
        assert_eq!(client.set_calls.len(), 2);
        assert!(nmx_config_response_message(result, true).contains("hard reset"));
    }

    #[tokio::test]
    async fn static_config_recovery_waits_for_app_and_grpc_ready_before_retrying_config() {
        let switch = FakeScaleUpSwitch {
            app_status_results: Mutex::new(VecDeque::from([
                Ok(nmx_controller_waiting_app_status()),
                Ok(nmx_controller_ready_app_status()),
                Ok(nmx_controller_ready_app_status()),
            ])),
            grpc_status_results: Mutex::new(VecDeque::from([
                Ok(nmx_controller_waiting_grpc_status()),
                Ok(nmx_controller_ready_grpc_status()),
            ])),
            ..FakeScaleUpSwitch::default()
        };

        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::empty_static_config_response()),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-b",
                )),
            ]),
            set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
            ..FakeNmxc::default()
        };

        let result = recover_nmx_topology_after_static_config_failure(
            &switch,
            &mut client,
            "10.0.0.11",
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::SetSucceeded);
        assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 1);
        assert_eq!(switch.app_status_calls.load(Ordering::SeqCst), 3);
        assert_eq!(switch.grpc_status_calls.load(Ordering::SeqCst), 2);
        assert_eq!(client.set_calls.len(), 1);
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_succeeds_when_precheck_rpc_fails() {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Err(NmxcError::NmxReturnCode {
                    return_code: nmxc_model::StReturnCode::NmxStGenericError as i32,
                    operation: "GetStaticConfig",
                }),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-b",
                )),
            ]),
            set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
            ..FakeNmxc::default()
        };

        let result = call_nmx_set_static_config(
            &mut client,
            "topology-b",
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::SetSucceeded);
        assert_eq!(client.set_calls.len(), 1);
    }

    #[tokio::test]
    async fn fabric_static_config_restarts_after_confirmed_topology_write() {
        for precheck_unavailable in [false, true] {
            for set_returns_error in [false, true] {
                let switch = FakeScaleUpSwitch::default();
                let gateway_id = crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID;

                let precheck = if precheck_unavailable {
                    Err(NmxcError::invalid_response("precheck unavailable"))
                } else {
                    Ok(FakeNmxc::static_config_response(
                        NMX_C_FM_CONFIG_FILE,
                        NMX_C_TOPOLOGY_KEY,
                        "topology-a",
                    ))
                };

                let set_result = if set_returns_error {
                    Err(NmxcError::invalid_response("set response unavailable"))
                } else {
                    Ok(FakeNmxc::success_return_code())
                };

                let mut client = FakeNmxc {
                    get_static_config_results: VecDeque::from([
                        precheck,
                        Ok(FakeNmxc::static_config_response(
                            NMX_C_FM_CONFIG_FILE,
                            NMX_C_TOPOLOGY_KEY,
                            "topology-b",
                        )),
                        Ok(FakeNmxc::static_config_response(
                            NMX_C_FM_CONFIG_FILE,
                            NMX_C_TOPOLOGY_KEY,
                            "topology-b",
                        )),
                    ]),
                    set_results: VecDeque::from([set_result]),
                    ..FakeNmxc::default()
                };

                let first = configure_nmx_fabric_static_config(
                    &switch,
                    &mut client,
                    "10.0.0.11",
                    "topology-b",
                    &[],
                    gateway_id,
                )
                .await
                .expect("confirmed topology write should activate");

                assert_eq!(first, (NmxSetStaticConfigResult::SetSucceeded, false));
                assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 1);
                assert_eq!(client.set_calls.len(), 1);

                let retry = configure_nmx_fabric_static_config(
                    &switch,
                    &mut client,
                    "10.0.0.11",
                    "topology-b",
                    &[],
                    gateway_id,
                )
                .await
                .expect("matching topology should remain configured");

                assert_eq!(retry, (NmxSetStaticConfigResult::AlreadyConfigured, false));
                assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 1);
                assert_eq!(client.set_calls.len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn fabric_static_config_restarts_only_for_changed_fm_values() {
        for (file, old_topology, old_log_level, expected_writes, expected_restarts) in [
            (NMX_C_FM_CONFIG_FILE, "topology-a", "3", 2, 1),
            (NMX_C_FM_CONFIG_FILE, "topology-a", "4", 1, 1),
            (NMX_C_FM_CONFIG_FILE, "topology-b", "3", 1, 1),
            (NMX_C_FM_CONFIG_FILE, "topology-b", "4", 0, 0),
            (
                crate::libnmxc::NMX_C_SM_CONFIG_FILE,
                "topology-b",
                "3",
                1,
                0,
            ),
        ] {
            let switch = FakeScaleUpSwitch::default();

            let key = if file == NMX_C_FM_CONFIG_FILE {
                "LOG_LEVEL"
            } else {
                "log_flags"
            };

            let extra = rm::ScaleUpFabricStaticConfig {
                config_file_name: file.into(),
                key: key.into(),
                value: "4".into(),
            };

            let mut responses = VecDeque::from([Ok(FakeNmxc::static_config_response(
                NMX_C_FM_CONFIG_FILE,
                NMX_C_TOPOLOGY_KEY,
                old_topology,
            ))]);

            if old_topology != "topology-b" {
                responses.push_back(Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-b",
                )));
            }

            responses.push_back(Ok(FakeNmxc::static_config_response(
                file,
                key,
                old_log_level,
            )));

            if old_log_level != "4" {
                responses.push_back(Ok(FakeNmxc::static_config_response(file, key, "4")));
            }

            let mut client = FakeNmxc {
                get_static_config_results: responses,
                set_results: (0..expected_writes)
                    .map(|_| Ok(FakeNmxc::success_return_code()))
                    .collect(),
                ..FakeNmxc::default()
            };

            let result = configure_nmx_fabric_static_config(
                &switch,
                &mut client,
                "10.0.0.11",
                "topology-b",
                &[extra],
                crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
            )
            .await
            .expect("matching readbacks should complete the batch");

            assert_eq!(
                switch.restart_calls.load(Ordering::SeqCst),
                expected_restarts
            );

            assert_eq!(client.set_calls.len(), expected_writes);
            assert!(client.get_static_config_results.is_empty());

            assert_eq!(
                result.0 == NmxSetStaticConfigResult::AlreadyConfigured,
                expected_writes == 0
            );
        }
    }

    #[tokio::test]
    async fn fabric_static_config_activates_topology_written_after_recovery_restart() {
        let switch = FakeScaleUpSwitch::default();

        let mut client = FakeNmxc {
            get_static_config_results: ["topology-a", "topology-a", "topology-a", "topology-b"]
                .into_iter()
                .map(|value| {
                    Ok(FakeNmxc::static_config_response(
                        NMX_C_FM_CONFIG_FILE,
                        NMX_C_TOPOLOGY_KEY,
                        value,
                    ))
                })
                .collect(),
            set_results: VecDeque::from([
                Ok(FakeNmxc::success_return_code()),
                Ok(FakeNmxc::success_return_code()),
            ]),
            ..FakeNmxc::default()
        };

        let result = configure_nmx_fabric_static_config(
            &switch,
            &mut client,
            "10.0.0.11",
            "topology-b",
            &[],
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await
        .expect("topology written after recovery should activate");

        assert_eq!(result, (NmxSetStaticConfigResult::SetSucceeded, true));
        assert_eq!(client.set_calls.len(), 2);
        assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fabric_static_config_reports_topology_activation_restart_failure() {
        let switch = FakeScaleUpSwitch {
            restart_results: Mutex::new(VecDeque::from([Err(RmsError::internal(
                "restart failed",
            ))])),
            ..FakeScaleUpSwitch::default()
        };

        let mut client = FakeNmxc {
            get_static_config_results: ["topology-a", "topology-b"]
                .into_iter()
                .map(|value| {
                    Ok(FakeNmxc::static_config_response(
                        NMX_C_FM_CONFIG_FILE,
                        NMX_C_TOPOLOGY_KEY,
                        value,
                    ))
                })
                .collect(),
            set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
            ..FakeNmxc::default()
        };

        let result = configure_nmx_fabric_static_config(
            &switch,
            &mut client,
            "10.0.0.11",
            "topology-b",
            &[],
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await;

        let error = result.expect_err("failed activation must fail the request");

        assert!(error.contains("failed to restart NMX Controller"));
        assert!(error.contains("10.0.0.11"));
        assert!(error.contains("restart failed"));
        assert_eq!(client.set_calls.len(), 1);
        assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fabric_static_config_applies_topology_and_extra_keys_on_one_client() {
        let switch = FakeScaleUpSwitch::default();

        let extra = rm::ScaleUpFabricStaticConfig {
            config_file_name: crate::libnmxc::NMX_C_SM_CONFIG_FILE.into(),
            key: "ROUTING_MODE".into(),
            value: "new".into(),
        };

        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-b",
                )),
                Ok(FakeNmxc::static_config_response(
                    crate::libnmxc::NMX_C_SM_CONFIG_FILE,
                    "ROUTING_MODE",
                    "old",
                )),
                Ok(FakeNmxc::static_config_response(
                    crate::libnmxc::NMX_C_SM_CONFIG_FILE,
                    "ROUTING_MODE",
                    "new",
                )),
            ]),
            set_results: VecDeque::from([
                Ok(FakeNmxc::success_return_code()),
                Ok(FakeNmxc::success_return_code()),
            ]),
            ..FakeNmxc::default()
        };

        let (topology_result, soft_reset_attempted) = configure_nmx_fabric_static_config(
            &switch,
            &mut client,
            "10.0.0.11",
            "topology-b",
            &[extra],
            crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID,
        )
        .await
        .expect("topology and extra static config should converge");

        assert_eq!(topology_result, NmxSetStaticConfigResult::SetSucceeded);
        assert!(!soft_reset_attempted);
        assert_eq!(client.set_calls.len(), 2);
        assert_eq!(client.set_calls[0].0, NMX_C_FM_CONFIG_FILE);
        assert_eq!(client.set_calls[0].1, NMX_C_TOPOLOGY_KEY);
        assert_eq!(client.set_calls[1].0, crate::libnmxc::NMX_C_SM_CONFIG_FILE);
        assert_eq!(client.set_calls[1].1, "ROUTING_MODE");
        assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fabric_static_config_applies_extra_keys_after_topology_soft_reset()
    -> std::result::Result<(), String> {
        let switch = FakeScaleUpSwitch::default();
        let gateway_id = crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID;

        let extra = rm::ScaleUpFabricStaticConfig {
            config_file_name: crate::libnmxc::NMX_C_SM_CONFIG_FILE.into(),
            key: "ROUTING_MODE".into(),
            value: "new".into(),
        };

        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-a",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_FM_CONFIG_FILE,
                    NMX_C_TOPOLOGY_KEY,
                    "topology-b",
                )),
                Ok(FakeNmxc::static_config_response(
                    crate::libnmxc::NMX_C_SM_CONFIG_FILE,
                    "ROUTING_MODE",
                    "old",
                )),
                Ok(FakeNmxc::static_config_response(
                    crate::libnmxc::NMX_C_SM_CONFIG_FILE,
                    "ROUTING_MODE",
                    "new",
                )),
            ]),
            set_results: VecDeque::from([
                Ok(FakeNmxc::success_return_code()),
                Ok(FakeNmxc::success_return_code()),
            ]),
            ..FakeNmxc::default()
        };

        let (config_result, soft_reset_attempted) = configure_nmx_fabric_static_config(
            &switch,
            &mut client,
            "10.0.0.11",
            "topology-b",
            &[extra],
            gateway_id,
        )
        .await?;

        assert_eq!(config_result, NmxSetStaticConfigResult::SetSucceeded);
        assert!(soft_reset_attempted);

        assert_eq!(
            nmx_config_response_message(config_result, soft_reset_attempted),
            "configuration complete"
        );

        assert_eq!(switch.restart_calls.load(Ordering::SeqCst), 1);
        assert_eq!(client.hello_calls, vec![gateway_id.to_owned()]);
        assert_eq!(client.set_calls.len(), 2);
        assert_eq!(client.set_calls[0].0, NMX_C_FM_CONFIG_FILE);
        assert_eq!(client.set_calls[0].1, NMX_C_TOPOLOGY_KEY);
        assert_eq!(client.set_calls[1].0, crate::libnmxc::NMX_C_SM_CONFIG_FILE);
        assert_eq!(client.set_calls[1].1, "ROUTING_MODE");

        Ok(())
    }
}
