/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

//! Handlers for the ScaleUpFabricManager RPCs.
//!
//! All ScaleUp RPCs now take a caller-supplied `NodeInfo` (an ephemeral
//! switch identity) rather than resolving a node from the rack inventory.
//! The handlers build an ephemeral `Switch` per request using host-endpoint
//! credentials, call the relevant NVUE / SSH / NMX flow, and return a
//! structured status.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use futures::future::join_all;

use crate::utilities::url::grpc_target_uri;

use crate::api::grpc::conversions::{flatten_node_info, proto_node_type_to_domain};
use crate::domain::node::NodeKind;
use crate::domain::rack::NodeConfig;
use crate::libnmxc::{
    Endpoint, NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY, Nmxc, NmxcClientPool, NmxcTlsConfig,
    find_static_config_value, gateway_id_from_env,
};
use crate::nodes::switch_mtls::SwitchMtlsService;
use crate::nodes::{NodeInstance, SwitchScaleUpManagement};
use crate::utilities::error::{Result, RmsError};
use librms::protos::rack_manager as rm;

use super::server::RackManagerServiceImpl;

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
struct EphemeralSwitch {
    /// Switch client built from the caller-supplied `NodeInfo`.
    switch: Box<dyn SwitchScaleUpManagement>,

    /// Validated switch-facing target address from `host_endpoint`.
    target_ip: IpAddr,

    /// TLS server name from `host_endpoint`, falling back to `target_ip`.
    tls_server_name: String,
}

impl EphemeralSwitch {
    fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
        self.switch
            .as_switch_gb200()
            .and_then(|switch| switch.optional_nvue_client())
    }
}

fn grpc_port_for_app(app_name: &str) -> i32 {
    match app_name {
        "nmx-telemetry" => 9352,
        _ => 9370,
    }
}

fn nmx_client_tls_with_endpoint_authority(
    mut tls: NmxcTlsConfig,
    endpoint_tls_server_name: &str,
) -> NmxcTlsConfig {
    if tls.authority.is_none() {
        tls.authority = Some(endpoint_tls_server_name.trim().to_owned());
    }

    tls
}

async fn disable_insecure_nmx_controller_mtls(switch: &dyn SwitchScaleUpManagement) -> Result<()> {
    let Some(switch) = switch.as_switch_gb200() else {
        return Err(RmsError::invalid_argument(
            "NMX-C mTLS unset requires a GB200/GB300 switch",
        ));
    };

    switch
        .ensure_cluster_app_manager_action_ready("nmx-controller")
        .await?;

    switch
        .unset_mtls_services(&[SwitchMtlsService::ScaleUpFabricManager])
        .await
        .map(|_| ())
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
    match ip {
        IpAddr::V4(ip) => is_disallowed_switch_target_ipv4(ip),
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_disallowed_switch_target_ipv4(mapped);
            }
            is_disallowed_switch_target_ipv6(ip)
        }
    }
}

fn is_disallowed_switch_target_ipv4(ip: Ipv4Addr) -> bool {
    ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() || ip.is_link_local()
}

fn is_disallowed_switch_target_ipv6(ip: Ipv6Addr) -> bool {
    let [first, ..] = ip.segments();
    ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() || (first & 0xffc0) == 0xfe80
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
) -> NmxSetStaticConfigResult {
    // Reject "unknown" early to avoid treating a missing/unreadable topology as a successful set.
    if topology == NMX_TOPOLOGY_UNKNOWN {
        tracing::error!(topology, "invalid topology value");
        return NmxSetStaticConfigResult::Failed;
    }

    let gateway_id = gateway_id_from_env();

    let precheck_value = get_nmx_static_config_value(
        client,
        NMX_C_FM_CONFIG_FILE,
        NMX_C_TOPOLOGY_KEY,
        &gateway_id,
    )
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
            &gateway_id,
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
    let postcheck_value = get_nmx_static_config_value(
        client,
        NMX_C_FM_CONFIG_FILE,
        NMX_C_TOPOLOGY_KEY,
        &gateway_id,
    )
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
) -> NmxSetStaticConfigResult {
    const WAIT_TIME: Duration = Duration::from_secs(2);

    let gateway_id = gateway_id_from_env();

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
        if let Err(e) = client.hello(&gateway_id).await {
            tracing::warn!(attempt, error = %e, "NMX Hello failed after soft reset");
            continue;
        }

        match call_nmx_set_static_config(client, topology).await {
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

async fn wait_for_nmx_controller_ready_for_config(
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
fn build_ephemeral_switch(device: &rm::NodeInfo) -> Result<EphemeralSwitch> {
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
    let bmc_endpoint = flat.optional_bmc_endpoint()?;
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
    async fn build_ephemeral_switch(
        &self,
        device: &rm::NodeInfo,
        request_domain: Option<&str>,
    ) -> Result<EphemeralSwitch> {
        let switch = build_ephemeral_switch(device)?;

        self.initialize_nvue_client_with_options(
            switch.nvue_client(),
            request_domain,
            Some(&switch.tls_server_name),
            false,
        )
        .await?;

        Ok(switch)
    }
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
    match switch.set_cluster_state(enabled).await {
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

        let EphemeralSwitch {
            switch: sw,
            target_ip: switch_target_ip,
            tls_server_name: switch_tls_server_name,
        } = ephemeral;

        // Step 1: Enable cluster
        if let Err(e) = sw.set_cluster_state(true).await {
            tracing::warn!(node = %device.node_id, error = %e.message, "set_cluster_state failed");
            resp.message = format!("failed to enable cluster: {}", e.message);
            return Ok(tonic::Response::new(resp));
        }
        resp.scale_up_fabric_state_enabled = true;

        // Step 2: Enable gRPC for external clients
        if let Err(e) = sw
            .enable_grpc_for_external_clients("nmx-controller", true)
            .await
        {
            tracing::warn!(node = %device.node_id, error = %e.message, "enable_grpc_for_external_clients failed");
            resp.message = format!("failed to enable gRPC: {}", e.message);
            return Ok(tonic::Response::new(resp));
        }

        if self.switch_tls_roots.insecure_switch {
            // A prior insecure certificate cleanup can no-op while the cluster
            // is disabled. Once the cluster is enabled, restore the NMX-C
            // manager encryption mode before RMS connects without client mTLS.
            if let Err(e) = disable_insecure_nmx_controller_mtls(sw.as_ref()).await {
                tracing::warn!(node = %device.node_id, error = %e.message, "failed to disable NMX-C mTLS");
                resp.message = format!("failed to disable NMX-C mTLS: {}", e.message);
                return Ok(tonic::Response::new(resp));
            }
        }

        // Step 3: Verify gRPC status
        let switch_target = switch_target_ip.to_string();

        let status_check = sw.check_grpc_status("nmx-controller", &switch_target).await;
        let ready = status_check
            .as_ref()
            .ok()
            .and_then(|s| s.get("summary"))
            .and_then(|s| s.get("ready_for_external_clients"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !ready {
            tracing::warn!(node = %device.node_id, switch_target = %switch_target, "gRPC not ready for external clients");
            resp.message = "gRPC not ready for external clients".into();
            return Ok(tonic::Response::new(resp));
        }

        resp.grpc_enabled = true;

        // Step 4: NMX gRPC (Hello + SetStaticConfig with topology_type from request)
        let nmx_port = grpc_port_for_app("nmx-controller");

        let nmx_tls = if self.switch_tls_roots.insecure_switch {
            // --insecure-switch disables NMX-C/gNMI client mTLS as well as
            // NVUE client mTLS. NMX-C uses plaintext HTTP in this mode because
            // disabling the switch manager mTLS mode exposes the non-TLS gRPC
            // endpoint.
            None
        } else {
            let nmx_server_name = match self
                .switch_tls_roots
                .tls_server_name_for_endpoint(&switch_tls_server_name, request_domain)
            {
                Ok(server_name) => server_name,
                Err(message) => {
                    resp.message = message;
                    return Ok(tonic::Response::new(resp));
                }
            };

            match self.switch_tls_roots.resolve_client_tls(request_domain) {
                Ok(tls) => Some(nmx_client_tls_with_endpoint_authority(
                    tls,
                    &nmx_server_name,
                )),
                Err(message) => {
                    resp.message = message;
                    return Ok(tonic::Response::new(resp));
                }
            }
        };

        let nmx_target = grpc_target_uri(
            &switch_target,
            nmx_port as u16,
            !self.switch_tls_roots.insecure_switch,
        );

        let endpoint = match Endpoint::new(&nmx_target) {
            Ok(endpoint) => endpoint,
            Err(e) => {
                tracing::warn!(node = %device.node_id, error = %e, "invalid NMX target address");
                resp.message = format!("invalid NMX target address: {e}");
                return Ok(tonic::Response::new(resp));
            }
        };
        let pool = match NmxcClientPool::builder().build() {
            Ok(pool) => pool,
            Err(e) => {
                tracing::error!(node = %device.node_id, error = %e, "failed to create NMX client pool");
                resp.message = format!("failed to create NMX client pool: {e}");
                return Ok(tonic::Response::new(resp));
            }
        };

        let mut client = match pool.create_client(endpoint, nmx_tls.as_ref()).await {
            Ok(client) => client,
            Err(e) => {
                tracing::error!(node = %device.node_id, error = %e, "failed to connect to NMX controller");
                resp.message = format!("failed to connect to NMX controller: {e}");
                return Ok(tonic::Response::new(resp));
            }
        };

        // Try gRPC Hello to verify the connection is working.
        if let Err(e) = client.hello(&gateway_id_from_env()).await {
            tracing::error!(error = %e, node = %device.node_id, switch_target = %switch_target, nmx_port, "NMX Hello: RPC failed");
            resp.message = "NMX Hello failed".into();
            return Ok(tonic::Response::new(resp));
        }

        // Try to set the topology value in the static config.
        let config_result = call_nmx_set_static_config(client.as_mut(), &r.topology_type).await;

        // Check if a soft-reset can help.
        let soft_reset_attempted = !config_result.is_success();

        let config_result = if soft_reset_attempted {
            recover_nmx_topology_after_static_config_failure(
                sw.as_ref(),
                client.as_mut(),
                &switch_target,
                &r.topology_type,
            )
            .await
        } else {
            config_result
        };

        if config_result.is_success() {
            tracing::info!(node = %device.node_id, topology = %r.topology_type, result = ?config_result, "NMX topology set succeeded");
            resp.status = rm::ReturnCode::Success.into();
        } else {
            tracing::error!(node = %device.node_id, topology = %r.topology_type, result = ?config_result, "failed to set NMX topology");
            resp.status = rm::ReturnCode::Failure.into();
        }

        resp.message = nmx_config_response_message(config_result, soft_reset_attempted).into();
        resp.topology_used = r.topology_type;

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

    use super::*;
    use crate::domain::node::{Node, NodeType};
    use crate::libnmxc::{NmxcError, nmxc_model, test_support::FakeNmxc};
    use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;

    use serde_json::Value;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Default)]
    struct FakeScaleUpSwitch {
        restart_calls: AtomicUsize,
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
        async fn set_cluster_state(&self, _enabled: bool) -> Result<()> {
            Err(RmsError::internal("unexpected set_cluster_state call"))
        }

        async fn get_cluster_state(&self) -> Result<Value> {
            Err(RmsError::internal("unexpected get_cluster_state call"))
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
            Ok(())
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
                dangerously_accept_invalid_certs: false,
            }),
        }
    }

    #[test]
    fn build_ephemeral_switch_accepts_regular_host_endpoint() {
        let ephemeral =
            build_ephemeral_switch(&switch_node_info("10.0.0.11", Some(" switch.example.com ")))
                .unwrap();

        assert_eq!(ephemeral.target_ip, "10.0.0.11".parse::<IpAddr>().unwrap());
        assert_eq!(ephemeral.tls_server_name, "switch.example.com");
    }

    #[test]
    fn build_ephemeral_switch_accepts_legacy_endpoint_without_host_name() {
        let ephemeral = build_ephemeral_switch(&switch_node_info("10.0.0.11", None)).unwrap();

        assert_eq!(ephemeral.target_ip, "10.0.0.11".parse::<IpAddr>().unwrap());
        assert_eq!(ephemeral.tls_server_name, "10.0.0.11");
    }

    #[test]
    fn build_ephemeral_switch_accepts_gb300_switch_type() {
        let ephemeral = build_ephemeral_switch(&switch_node_info_with_type(
            "10.0.0.11",
            Some("switch.example.com"),
            rm::NodeType::SwitchGb300Nvidia,
        ))
        .unwrap();

        assert_eq!(ephemeral.switch.node_type(), NodeType::SwitchGb300Nvidia);
    }

    #[test]
    fn build_ephemeral_switch_rejects_unsafe_host_endpoint() {
        let Err(err) =
            build_ephemeral_switch(&switch_node_info("127.0.0.1", Some("switch.example.com")))
        else {
            panic!("expected unsafe host endpoint to be rejected");
        };

        assert!(err.message.contains("switch target host is not allowed"));
    }

    #[tokio::test]
    async fn disable_insecure_nmx_controller_mtls_restores_manager_encryption() -> Result<()> {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "enabled",
                "nmxc-conn": {"state": "up"},
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "additional-info": "CONTROL_PLANE_STATE_CONFIGURED",
                "manager": {"state": "enabled"},
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

        disable_insecure_nmx_controller_mtls(&switch).await?;

        let commands = commands.lock().unwrap();

        assert_eq!(
            *commands,
            vec![
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv config apply --assume-yes",
                "nv config save",
            ]
        );

        Ok(())
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

        let result = call_nmx_set_static_config(&mut client, "topology-a").await;

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

        let result = call_nmx_set_static_config(&mut client, "topology-b").await;

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

        let result = call_nmx_set_static_config(&mut client, "topology-b").await;

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

        let result = call_nmx_set_static_config(&mut client, "topology-b").await;

        assert_eq!(result, NmxSetStaticConfigResult::Failed);
        assert!(!result.is_success());
        assert_eq!(client.set_calls.len(), 1);
    }

    #[tokio::test]
    async fn call_nmx_set_static_config_rejects_unknown_topology() {
        let mut client = FakeNmxc::default();

        let result = call_nmx_set_static_config(&mut client, NMX_TOPOLOGY_UNKNOWN).await;

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

        let result = call_nmx_set_static_config(&mut client, "topology-b").await;

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

        let result = call_nmx_set_static_config(&mut client, "topology-b").await;

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

        let result = call_nmx_set_static_config(&mut client, "topology-b").await;

        assert_eq!(result, NmxSetStaticConfigResult::SetSucceeded);
        assert_eq!(client.set_calls.len(), 1);
    }
}
