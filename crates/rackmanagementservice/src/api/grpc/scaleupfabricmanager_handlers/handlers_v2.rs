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

//! Declarative scale-up fabric manager V2 reconciliation and status RPC handlers.
//!
//! Requests use caller-supplied switch identities and credentials. The handlers
//! reconcile only drifted state, then verify the complete observed fabric.

mod optional_configs;

use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;

use crate::api::grpc::node_type_resolver::{
    NodeTypeResolutionError, normalize_node_info, resolve_node_info,
};
use crate::domain::node::NodeType;
use crate::libnmxc::{
    Endpoint, NMX_C_FM_CONFIG_FILE, NMX_C_SM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY, Nmxc, NmxcClientPool,
    NmxcError, static_config_file_values,
};
use crate::nodes::SwitchScaleUpManagement;
use crate::nodes::switch_gb200_nvidia::config;
use crate::orchestrator::job_lifecycle::{JobError, JobFailure};
use crate::orchestrator::job_tracker::{JobType, RmsJobHandle};
use crate::utilities::url::grpc_target_uri;

use futures::future::join_all;
use librms::protos::rack_manager as rm;
use librms::protos::rack_manager_v2 as rm_v2;
use nvue_client::cluster::{
    ClusterNodeServerAddresses, InterfaceType, NMX_CONTROLLER_APP_NAME, NmxControlPlaneState,
};
use nvue_client::{Client as NvueClient, DEFAULT_TIMEOUT as NVUE_DEFAULT_TIMEOUT};

use super::super::server::RackManagerServiceImpl;
use super::{
    EphemeralSwitch, NMX_CONTROLLER_READY_POLL_ATTEMPTS, NMX_CONTROLLER_READY_POLL_DELAY,
    NMX_TOPOLOGY_UNKNOWN, NmxSetStaticConfigResult, build_ephemeral_switch,
    get_nmx_static_config_value,
};

// ── NMX controller helpers ────────────────────────────────────────────

const NMX_CONTROLLER_CONFIGURED_TIMEOUT: Duration =
    Duration::from_secs(config::NMX_CONTROLLER_CONFIGURED_TIMEOUT_SECONDS);

const NMX_CONTROLLER_CONFIGURED_POLL_DELAY: Duration =
    Duration::from_secs(config::DEFAULT_POLL_INTERVAL_SECONDS);

/// Normalizes V2 nodes while preserving scoped descriptor-backed FM config.
///
/// Unsupported descriptors return their node-type resolution error unchanged.
pub(crate) fn normalize_v2_node_set(
    node_set: &mut Option<rm::NodeSet>,
) -> std::result::Result<(), NodeTypeResolutionError> {
    if let Some(node_set) = node_set {
        for node in &mut node_set.nodes {
            // Generic normalization treats the descriptor as node-type metadata
            // and may replace or remove it.
            let optional_configs =
                optional_configs::OptionalNodeConfigs::take_from(node.node_descriptor.as_mut());

            let result = normalize_node_info(node).map(|_| ());

            // Restore before propagating the result so job handoff retains the
            // config and failed normalization does not consume request data.
            optional_configs.restore_to(&mut node.node_descriptor);
            result?;
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct NmxControllerConvergenceFailure {
    message: String,
    soft_reset_eligible: bool,
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

struct NodeIpSnapshot {
    prepared_switch: EphemeralSwitch,
    node_ips: ClusterNodeServerAddresses,
}

struct SwitchSnapshot {
    node: rm::NodeInfo,
    enabled: bool,
    node_ip: Option<NodeIpSnapshot>,
}

/// Captures node IP state and retains the authenticated switch for mutation and
/// rollback.
async fn prepare_node_ip_snapshot(
    service: &RackManagerServiceImpl,
    node: &rm::NodeInfo,
    request_domain: Option<&str>,
) -> std::result::Result<NodeIpSnapshot, String> {
    let mut normalized = node.clone();

    normalize_node_info(&mut normalized)
        .map_err(|error| format!("failed to resolve switch '{}': {error}", node.node_id))?;

    let prepared_switch = service
        .build_ephemeral_switch(&normalized, request_domain)
        .await
        .map_err(|error| {
            format!(
                "failed to prepare switch '{}': {}",
                node.node_id, error.message
            )
        })?;

    let switch = prepared_switch.switch.as_switch_gb200().ok_or_else(|| {
        format!(
            "node IP target '{}' did not resolve to an NVIDIA switch",
            node.node_id
        )
    })?;

    let node_ips = switch
        .get_node_ips(InterfaceType::Primary)
        .await
        .map_err(|error| {
            format!(
                "failed to read node IP addresses from '{}': {}",
                node.node_id, error.message
            )
        })?;

    Ok(NodeIpSnapshot {
        prepared_switch,
        node_ips,
    })
}

fn get_node_ips(nodes: &[rm::NodeInfo]) -> std::result::Result<ClusterNodeServerAddresses, String> {
    let mut node_ips = ClusterNodeServerAddresses::new();

    for node in nodes {
        let mut normalized = node.clone();

        let node_type = normalize_node_info(&mut normalized)
            .map_err(|error| format!("failed to resolve node '{}': {error}", node.node_id))?;

        if node_type != NodeType::SwitchVrnvl72Nvidia {
            return Err(format!(
                "node IP configuration does not support node '{}'",
                node.node_id
            ));
        }

        let switch = build_ephemeral_switch(&normalized).map_err(|error| {
            format!(
                "failed to resolve node IP for '{}': {}",
                node.node_id, error.message
            )
        })?;

        node_ips.insert(switch.target_ip);
    }

    Ok(node_ips)
}

fn get_node_ips_for_request(
    node_type: NodeType,
    nodes: &[rm::NodeInfo],
) -> std::result::Result<Option<ClusterNodeServerAddresses>, String> {
    // Scan every node before dispatching by selected type so a mixed topology
    // cannot bypass node IP compatibility validation.
    let mut requires_node_ips = false;
    let mut incompatible_node_id = None;

    for node in nodes {
        let node_type = resolve_node_info(node)
            .map_err(|error| format!("failed to resolve node '{}': {error}", node.node_id))?;

        if node_type == NodeType::SwitchVrnvl72Nvidia {
            requires_node_ips = true;
        } else if incompatible_node_id.is_none() {
            incompatible_node_id = Some(node.node_id.as_str());
        }
    }

    if requires_node_ips && let Some(node_id) = incompatible_node_id {
        return Err(format!(
            "node IP configuration does not support node '{node_id}'"
        ));
    }

    if node_type == NodeType::SwitchVrnvl72Nvidia {
        get_node_ips(nodes).map(Some)
    } else {
        Ok(None)
    }
}

/// Waits for configured state and soft-resets one observed unhealthy controller.
///
/// Static-config readback only proves that NMX-C stored the requested values.
/// The controller can continue through transitional states afterward. This poll
/// treats only `CONTROL_PLANE_STATE_CONFIGURED` as success. After a full
/// convergence window with a reported non-configured state, it restarts NMX
/// Controller once and grants a fresh full window. NVUE-only failures do not
/// prove that the controller is unhealthy and therefore do not trigger restart.
async fn wait_for_nmx_controller_configured(
    switch: &dyn SwitchScaleUpManagement,
    nvue: &NvueClient,
    node_id: &str,
) -> std::result::Result<(), String> {
    wait_for_nmx_controller_configured_with_recovery(
        nvue,
        node_id,
        NMX_CONTROLLER_CONFIGURED_TIMEOUT,
        NMX_CONTROLLER_CONFIGURED_POLL_DELAY,
        || async {
            switch
                .restart_cluster_app(NMX_CONTROLLER_APP_NAME)
                .await
                .map_err(|error| error.message)
        },
    )
    .await
}

/// Runs convergence and one eligible restart with caller-supplied timing.
async fn wait_for_nmx_controller_configured_with_recovery<Restart, RestartFuture>(
    nvue: &NvueClient,
    node_id: &str,
    timeout: Duration,
    poll_delay: Duration,
    restart: Restart,
) -> std::result::Result<(), String>
where
    Restart: FnOnce() -> RestartFuture,
    RestartFuture: Future<Output = std::result::Result<(), String>>,
{
    let initial_failure =
        match wait_for_nmx_controller_configured_with_timing(nvue, node_id, timeout, poll_delay)
            .await
        {
            Ok(()) => return Ok(()),
            Err(failure) if !failure.soft_reset_eligible => return Err(failure.message),
            Err(failure) => failure,
        };

    tracing::warn!(
        node = node_id,
        timeout_secs = timeout.as_secs(),
        "soft resetting nmx-controller after control-plane convergence timeout"
    );

    restart().await.map_err(|error| {
        format!(
            "failed to soft reset nmx-controller after convergence timeout: {error}; initial \
             failure: {}",
            initial_failure.message
        )
    })?;

    wait_for_nmx_controller_configured_with_timing(nvue, node_id, timeout, poll_delay)
        .await
        .map_err(|failure| {
            format!(
                "nmx-controller did not converge after soft reset: {}; initial failure: {}",
                failure.message, initial_failure.message
            )
        })
}

/// Runs the configured-state poll with caller-supplied timing.
///
/// Injectable timing keeps transition, retry, and timeout tests deterministic
/// without changing production retry policy.
async fn wait_for_nmx_controller_configured_with_timing(
    nvue: &NvueClient,
    node_id: &str,
    timeout: Duration,
    poll_delay: Duration,
) -> std::result::Result<(), NmxControllerConvergenceFailure> {
    let started_at = tokio::time::Instant::now();
    let deadline = started_at + timeout;
    let expected_state = NmxControlPlaneState::Configured.as_str();

    let mut poll = 0_u32;
    let mut soft_reset_eligible = false;

    let last_observation = loop {
        let observation = match nvue
            .get_cluster_app(NMX_CONTROLLER_APP_NAME, NVUE_DEFAULT_TIMEOUT)
            .await
        {
            Ok(app_status) if app_status.is_control_plane_configured() => {
                tracing::info!(
                    node = node_id,
                    poll,
                    elapsed_secs = started_at.elapsed().as_secs(),
                    control_plane_state = expected_state,
                    "nmx-controller control plane configured"
                );

                return Ok(());
            }
            Ok(app_status) => {
                let reported_control_plane_state = app_status
                    .addition_info
                    .as_deref()
                    .map(str::trim)
                    .filter(|state| !state.is_empty());

                soft_reset_eligible |= reported_control_plane_state.is_some();

                let control_plane_state = reported_control_plane_state.unwrap_or("<missing>");

                let status = app_status.status.as_deref().unwrap_or("<missing>");

                let reason = app_status.reason.as_deref().unwrap_or("<missing>");

                tracing::debug!(
                    node = node_id,
                    poll,
                    control_plane_state,
                    status,
                    reason,
                    expected_state,
                    "waiting for nmx-controller control-plane convergence"
                );

                format!(
                    "control-plane state '{control_plane_state}', status '{status}', reason '{reason}'"
                )
            }
            Err(error) => {
                tracing::warn!(
                    node = node_id,
                    poll,
                    error = %error,
                    expected_state,
                    "failed to read nmx-controller control-plane state"
                );

                format!("failed to read nmx-controller state: {error}")
            }
        };

        let now = tokio::time::Instant::now();

        if now >= deadline {
            break observation;
        }

        poll += 1;
        tokio::time::sleep(poll_delay.min(deadline.saturating_duration_since(now))).await;
    };

    Err(NmxControllerConvergenceFailure {
        message: format!(
            "nmx-controller did not reach {expected_state} within {} seconds; last observation: \
             {last_observation}",
            timeout.as_secs()
        ),
        soft_reset_eligible,
    })
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

/// Sets one switch's cluster state with TLS resolved from the request domain.
///
/// This cannot call the legacy batch helper because that helper has no
/// request-domain parameter and would resolve switch TLS using process defaults.
async fn set_scale_up_fabric_state_for_device(
    service: &RackManagerServiceImpl,
    device: rm::NodeInfo,
    enabled: bool,
    request_domain: Option<&str>,
) -> rm::NodeOperationResult {
    let node_id = device.node_id.clone();

    let switch = match service
        .build_ephemeral_switch(&device, request_domain)
        .await
    {
        Ok(ephemeral) => ephemeral.switch,
        Err(error) => {
            return rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message: error.message,
            };
        }
    };

    match switch.set_cluster_state(enabled).await {
        Ok(()) => rm::NodeOperationResult {
            node_id,
            status: rm::ReturnCode::Success.into(),
            error_message: String::new(),
        },
        Err(error) => rm::NodeOperationResult {
            node_id,
            status: rm::ReturnCode::Failure.into(),
            error_message: error.message,
        },
    }
}

/// Reads topology and all requested static-config files from NMX Controller.
///
/// The fabric-manager and system-manager files are always included. The topology
/// key is returned separately; remaining entries are sorted for stable status
/// responses and job results.
async fn read_nmx_fabric_config(
    client: &mut dyn Nmxc,
    extra_config_files: &[&str],
    gateway_id: &str,
) -> std::result::Result<(String, Vec<rm::ScaleUpFabricStaticConfig>), String> {
    let mut config_file_names = vec![NMX_C_FM_CONFIG_FILE, NMX_C_SM_CONFIG_FILE];

    for &config_file_name in extra_config_files {
        if !config_file_names.contains(&config_file_name) {
            config_file_names.push(config_file_name);
        }
    }

    let response = client
        .get_static_config_files(&config_file_names, gateway_id)
        .await
        .map_err(|error| format!("failed to read NMX Controller static config: {error}"))?;

    let values = static_config_file_values(&response);

    let topology_type = values
        .iter()
        .find(|value| {
            value.config_file_name == NMX_C_FM_CONFIG_FILE && value.key == NMX_C_TOPOLOGY_KEY
        })
        .map(|value| value.value.clone())
        .ok_or_else(|| "NMX Controller topology is missing from static config".to_string())?;

    let mut extra_static_configs: Vec<rm::ScaleUpFabricStaticConfig> = values
        .into_iter()
        .filter(|value| {
            value.config_file_name != NMX_C_FM_CONFIG_FILE || value.key != NMX_C_TOPOLOGY_KEY
        })
        .map(|value| rm::ScaleUpFabricStaticConfig {
            config_file_name: value.config_file_name,
            key: value.key,
            value: value.value,
        })
        .collect();

    extra_static_configs.sort_unstable_by(|left, right| {
        (&left.config_file_name, &left.key).cmp(&(&right.config_file_name, &right.key))
    });

    Ok((topology_type, extra_static_configs))
}

/// Normalizes and validates desired fabric configuration before any mutation.
///
/// Topology owns the fabric-manager topology key, so callers cannot also supply
/// it as an extra static-config entry. Each remaining file/key pair is unique.
fn validate_scale_up_fabric_config(
    mut config: rm_v2::ScaleUpFabricConfig,
) -> std::result::Result<rm_v2::ScaleUpFabricConfig, tonic::Status> {
    let topology_type = config.topology_type.trim().to_owned();

    if topology_type.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "config.topology_type is required",
        ));
    }

    if topology_type == NMX_TOPOLOGY_UNKNOWN {
        return Err(tonic::Status::invalid_argument(
            "config.topology_type must identify a configured topology",
        ));
    }

    config.topology_type = topology_type;

    let mut keys = HashSet::with_capacity(config.extra_static_configs.len());

    for entry in &mut config.extra_static_configs {
        entry.config_file_name = entry.config_file_name.trim().to_owned();
        entry.key = entry.key.trim().to_owned();
        entry.value = entry.value.trim().to_owned();

        if entry.config_file_name.is_empty() || entry.key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "extra static config file names and keys must be non-empty",
            ));
        }

        if entry.config_file_name == NMX_C_FM_CONFIG_FILE && entry.key == NMX_C_TOPOLOGY_KEY {
            return Err(tonic::Status::invalid_argument(format!(
                "extra static config {NMX_C_FM_CONFIG_FILE}/{NMX_C_TOPOLOGY_KEY} conflicts with config.topology_type"
            )));
        }

        let key = (entry.config_file_name.clone(), entry.key.clone());

        if !keys.insert(key) {
            return Err(tonic::Status::invalid_argument(format!(
                "duplicate extra static config {}/{}",
                entry.config_file_name, entry.key
            )));
        }
    }

    Ok(config)
}

/// Adds descriptor-backed FM config from the selected primary, then validates it.
fn validate_primary_scale_up_fabric_config(
    primary: &mut rm::NodeInfo,
    mut config: rm_v2::ScaleUpFabricConfig,
) -> std::result::Result<rm_v2::ScaleUpFabricConfig, tonic::Status> {
    let optional_configs =
        optional_configs::OptionalNodeConfigs::take_from(primary.node_descriptor.as_mut());

    config
        .extra_static_configs
        .extend(optional_configs.into_static_configs());

    validate_scale_up_fabric_config(config)
}

/// Resolves one observed primary and rejects fabric state with multiple enabled
/// switches because no authoritative NMX Controller config source exists in that
/// state.
fn observed_primary_index<'a>(
    switches: impl IntoIterator<Item = &'a rm::ScaleUpFabricSwitchStatus>,
) -> std::result::Result<Option<usize>, String> {
    let enabled: Vec<(usize, &str)> = switches
        .into_iter()
        .enumerate()
        .filter(|(_, entry)| entry.enabled)
        .map(|(index, entry)| (index, entry.node_id.as_str()))
        .collect();

    match enabled.as_slice() {
        [] => Ok(None),
        [(index, _)] => Ok(Some(*index)),
        _ => Err(format!(
            "multiple switches report scale-up fabric enabled: {}",
            enabled
                .iter()
                .map(|(_, node_id)| *node_id)
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// Verifies that final fabric status matches the resolved primary and every
/// requested desired-state value before the reconciliation job completes.
fn validate_reconciled_scale_up_fabric_status(
    status: &rm::ScaleUpFabricStatus,
    primary_node_id: &str,
    config: &rm_v2::ScaleUpFabricConfig,
) -> std::result::Result<(), String> {
    if let Some(entry) = status
        .switches
        .iter()
        .find(|entry| !entry.error_message.is_empty())
    {
        return Err(format!(
            "failed to verify switch '{}': {}",
            entry.node_id, entry.error_message
        ));
    }

    if status.topology_type != config.topology_type {
        return Err(format!(
            "NMX Controller topology readback mismatch: expected '{}', observed '{}'",
            config.topology_type, status.topology_type
        ));
    }

    let primary = status
        .switches
        .iter()
        .find(|entry| entry.node_id == primary_node_id)
        .ok_or_else(|| format!("primary switch '{primary_node_id}' is missing from readback"))?;

    if !primary.enabled {
        return Err(format!("primary switch '{primary_node_id}' is not enabled"));
    }

    if let Some(entry) = status
        .switches
        .iter()
        .find(|entry| entry.node_id != primary_node_id && entry.enabled)
    {
        return Err(format!(
            "non-primary switch '{}' remains enabled",
            entry.node_id
        ));
    }

    for desired in &config.extra_static_configs {
        let observed = status.extra_static_configs.iter().find(|observed| {
            observed.config_file_name == desired.config_file_name && observed.key == desired.key
        });

        if observed.map(|entry| entry.value.as_str()) != Some(desired.value.as_str()) {
            return Err(format!(
                "NMX Controller static config readback mismatch for {}/{}",
                desired.config_file_name, desired.key
            ));
        }
    }

    Ok(())
}

// ── Declarative NVLink fabric RPC handlers ───────────────────────────

impl RackManagerServiceImpl {
    /// Validates every switch identity and its request-scoped TLS configuration
    /// before a reconciliation job can mutate fabric state.
    async fn validate_scale_up_fabric_nodes(
        &self,
        nodes: &[rm::NodeInfo],
        request_domain: Option<&str>,
    ) -> std::result::Result<(), tonic::Status> {
        let mut node_ids = HashSet::with_capacity(nodes.len());

        for node in nodes {
            if !node_ids.insert(node.node_id.as_str()) {
                return Err(tonic::Status::invalid_argument(format!(
                    "duplicate switch node_id '{}'",
                    node.node_id
                )));
            }
        }

        let validations = join_all(
            nodes
                .iter()
                .map(|node| self.build_ephemeral_switch(node, request_domain)),
        )
        .await;

        let mut node_ips = HashSet::with_capacity(nodes.len());

        for (node, validation) in nodes.iter().zip(validations) {
            let switch = validation.map_err(|error| {
                tonic::Status::invalid_argument(format!(
                    "invalid switch '{}': {}",
                    node.node_id, error.message
                ))
            })?;

            if !node_ips.insert(switch.target_ip) {
                return Err(tonic::Status::invalid_argument(format!(
                    "nodes contains duplicate host endpoint IP address {}",
                    switch.target_ip
                )));
            }
        }

        Ok(())
    }

    /// Reads and logs the current NVOS version without affecting reconciliation.
    async fn log_nvos_version(&self, node: &rm::NodeInfo, request_domain: Option<&str>) {
        let prepared_switch = match self.build_ephemeral_switch(node, request_domain).await {
            Ok(prepared_switch) => prepared_switch,
            Err(error) => {
                tracing::warn!(
                    node = %node.node_id,
                    error = %error.message,
                    "failed to prepare switch for NVOS version read"
                );

                return;
            }
        };

        let Some(switch) = prepared_switch.switch.as_switch_gb200() else {
            tracing::warn!(
                node = %node.node_id,
                "switch does not support NVOS version read"
            );

            return;
        };

        match switch.get_normalized_system_image_state().await {
            Ok(state) if !state.current_build_id.is_empty() => {
                tracing::info!(
                    node = %node.node_id,
                    nvos_version = %state.current_build_id,
                    "read switch NVOS version"
                );
            }
            Ok(_) => {
                tracing::warn!(
                    node = %node.node_id,
                    "switch did not report an NVOS version"
                );
            }
            Err(error) => {
                tracing::warn!(
                    node = %node.node_id,
                    error = %error.message,
                    "failed to read switch NVOS version"
                );
            }
        }
    }

    /// Selects the requested primary, or deterministically selects the lowest tray.
    ///
    /// Reading every tray prevents a transient failure from silently changing the
    /// selected primary during a later reconciliation request.
    async fn select_scale_up_fabric_primary(
        &self,
        nodes: &[rm::NodeInfo],
        primary_node_id: Option<&str>,
        request_domain: Option<&str>,
    ) -> std::result::Result<rm::NodeInfo, tonic::Status> {
        if let Some(id) = primary_node_id.filter(|id| !id.is_empty()) {
            return nodes
                .iter()
                .find(|node| node.node_id == id)
                .cloned()
                .ok_or_else(|| {
                    tonic::Status::invalid_argument(format!(
                        "primary_switch_node_id '{id}' is not present in nodes"
                    ))
                });
        }

        if nodes.len() == 1 {
            return Ok(nodes[0].clone());
        }

        // Node ID provides a stable tie-break when two switches report one tray.
        let primary = self
            .lowest_tray_index_node(nodes, request_domain)
            .await
            .map_err(tonic::Status::failed_precondition)?;

        tracing::info!(primary = %primary.node_id, "selected primary by lowest tray index");

        Ok(primary)
    }

    /// Returns the switch at the lowest rack position after every switch reports
    /// a tray index using the request's TLS domain.
    async fn lowest_tray_index_node(
        &self,
        nodes: &[rm::NodeInfo],
        request_domain: Option<&str>,
    ) -> std::result::Result<rm::NodeInfo, String> {
        let tray_reads = join_all(nodes.iter().map(|node| async move {
            let ephemeral = self
                .build_ephemeral_switch(node, request_domain)
                .await
                .map_err(|error| {
                    format!(
                        "switch '{}': failed to prepare: {}",
                        node.node_id, error.message
                    )
                })?;

            let switch = ephemeral.switch.as_switch_gb200().ok_or_else(|| {
                format!("switch '{}': chassis location is unavailable", node.node_id)
            })?;

            let location = switch.get_chassis_location_info().await.map_err(|error| {
                format!(
                    "switch '{}': failed to read tray index: {}",
                    node.node_id, error.message
                )
            })?;

            let tray_index = location
                .get("switch_info")
                .and_then(|value| value.get("tray_index"))
                .ok_or_else(|| format!("switch '{}': tray index is missing", node.node_id))?;

            let tray_index = if let Some(value) = tray_index.as_u64() {
                u32::try_from(value)
                    .map_err(|_| format!("switch '{}': tray index is invalid", node.node_id))?
            } else if let Some(value) = tray_index.as_str() {
                value
                    .parse::<u32>()
                    .map_err(|_| format!("switch '{}': tray index is invalid", node.node_id))?
            } else {
                return Err(format!("switch '{}': tray index is invalid", node.node_id));
            };

            Ok((tray_index, node))
        }))
        .await;

        let positions: Vec<(u32, &rm::NodeInfo)> = tray_reads
            .into_iter()
            .collect::<std::result::Result<_, _>>()?;

        positions
            .into_iter()
            .min_by(|(left_tray, left), (right_tray, right)| {
                (left_tray, &left.node_id).cmp(&(right_tray, &right.node_id))
            })
            .map(|(_, node)| node.clone())
            .ok_or_else(|| "nodes is empty".to_string())
    }

    /// Runs V2 in mutation-safe order: snapshot, select, disable non-primary
    /// switches, enforce primary security, reconcile, then verify. Failures
    /// after mutation restore the snapshotted switch state.
    async fn run_configure_scale_up_fabric_manager_v2_job(
        &self,
        job: RmsJobHandle,
        nodes: Vec<rm::NodeInfo>,
        primary_node_id: Option<String>,
        config: rm_v2::ScaleUpFabricConfig,
        request_domain: Option<String>,
    ) {
        let request_domain = request_domain.as_deref();

        job.progress("Snapshotting scale-up fabric switch state");

        let snapshots = join_all(nodes.iter().map(|node| async {
            let ((status, _), ()) = tokio::join!(
                self.read_switch(node, request_domain),
                self.log_nvos_version(node, request_domain),
            );

            if status.error_message.is_empty() {
                Ok(SwitchSnapshot {
                    node: node.clone(),
                    enabled: status.enabled,
                    node_ip: None,
                })
            } else {
                Err(format!("{}: {}", node.node_id, status.error_message))
            }
        }))
        .await
        .into_iter()
        .collect::<std::result::Result<Vec<_>, String>>();

        let snapshots = match snapshots {
            Ok(snapshots) => snapshots,
            Err(message) => {
                job.fail(JobFailure::new(
                    JobError::Other,
                    format!("failed to snapshot switch state: {message}"),
                ));

                return;
            }
        };

        job.progress("Selecting scale-up fabric primary switch");

        let mut primary = match self
            .select_scale_up_fabric_primary(&nodes, primary_node_id.as_deref(), request_domain)
            .await
        {
            Ok(primary) => primary,
            Err(error) => {
                job.fail(JobFailure::new(
                    JobError::Other,
                    format!("failed to select primary switch: {}", error.message()),
                ));

                return;
            }
        };

        // Only the selected primary contributes descriptor-backed static config.
        // Merge it before switch mutation so duplicate keys fail without
        // changing switch state.
        let config = match validate_primary_scale_up_fabric_config(&mut primary, config) {
            Ok(config) => config,
            Err(error) => {
                job.fail(JobFailure::new(
                    JobError::InvalidArgument,
                    format!(
                        "invalid scale-up fabric configuration for primary switch '{}': {}",
                        primary.node_id,
                        error.message()
                    ),
                ));

                return;
            }
        };

        let node_type = match normalize_node_info(&mut primary) {
            Ok(node_type) => node_type,
            Err(error) => {
                job.fail(JobFailure::new(
                    JobError::Other,
                    format!(
                        "failed to resolve primary switch '{}': {error}",
                        primary.node_id
                    ),
                ));

                return;
            }
        };

        let desired_node_ips = match get_node_ips_for_request(node_type, &nodes) {
            Ok(node_ips) => node_ips,
            Err(message) => {
                job.fail(JobFailure::new(JobError::Other, message));

                return;
            }
        };

        let snapshots = if desired_node_ips.is_some() {
            job.progress("Preparing node IP configuration");

            // Enabled switches may retain addresses that must be cleared before
            // disable. The selected switch also needs a snapshot for rollback.
            let snapshots = join_all(snapshots.into_iter().map(|mut snapshot| async {
                if snapshot.enabled || snapshot.node.node_id == primary.node_id {
                    snapshot.node_ip =
                        Some(prepare_node_ip_snapshot(self, &snapshot.node, request_domain).await?);
                }

                Ok(snapshot)
            }))
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, String>>();

            match snapshots {
                Ok(snapshots) => snapshots,
                Err(message) => {
                    job.fail(JobFailure::new(JobError::Other, message));

                    return;
                }
            }
        } else {
            snapshots
        };

        let node_ip_update = match desired_node_ips.as_ref() {
            Some(node_ips) => {
                let Some(snapshot) = snapshots
                    .iter()
                    .find(|snapshot| snapshot.node.node_id == primary.node_id)
                    .and_then(|snapshot| snapshot.node_ip.as_ref())
                else {
                    job.fail(JobFailure::new(
                        JobError::Internal,
                        "selected switch node IP snapshot is missing",
                    ));

                    return;
                };

                Some((snapshot, node_ips))
            }
            None => None,
        };

        let result: std::result::Result<String, JobFailure> = async {
            // Switches with node IP state need an ordered clear-and-disable.
            // Remaining non-selected switches use the normal disable path.
            if node_ip_update.is_some() {
                job.progress("Clearing node IP addresses from non-primary switches");

                for snapshot in snapshots
                    .iter()
                    .filter(|snapshot| snapshot.enabled && snapshot.node.node_id != primary.node_id)
                {
                    let switch = snapshot
                        .node_ip
                        .as_ref()
                        .ok_or_else(|| {
                            JobFailure::new(
                                JobError::Internal,
                                format!(
                                    "node IP snapshot is missing for switch '{}'",
                                    snapshot.node.node_id
                                ),
                            )
                        })?
                        .prepared_switch
                        .switch
                        .as_switch_gb200()
                        .ok_or_else(|| {
                            JobFailure::new(
                                JobError::Internal,
                                "node IP target did not resolve to an NVIDIA switch",
                            )
                        })?;

                    switch
                        .clear_node_ips_and_disable_cluster()
                        .await
                        .map_err(|error| {
                            JobFailure::new(
                                JobError::Other,
                                format!(
                                    "failed to clear node IP addresses and disable switch '{}': {}",
                                    snapshot.node.node_id, error.message
                                ),
                            )
                        })?;
                }
            }

            job.progress("Disabling scale-up fabric on non-primary switches");

            let disable_results = join_all(
                snapshots
                    .iter()
                    .filter(|snapshot| {
                        snapshot.node.node_id != primary.node_id
                            && (node_ip_update.is_none() || !snapshot.enabled)
                    })
                    .map(|snapshot| {
                        set_scale_up_fabric_state_for_device(
                            self,
                            snapshot.node.clone(),
                            false,
                            request_domain,
                        )
                    }),
            )
            .await;

            if let Some(result) = disable_results
                .into_iter()
                .find(|result| result.status != rm::ReturnCode::Success as i32)
            {
                return Err(JobFailure::new(
                    JobError::Other,
                    format!(
                        "failed to disable scale-up fabric on switch '{}': {}",
                        result.node_id, result.error_message
                    ),
                ));
            }

            if let Some((snapshot, node_ips)) = &node_ip_update {
                // This path only runs for switch types requiring node IP
                // configuration. NVOS rejects that configuration while the
                // cluster is disabled, so enable the selected switch first.
                job.progress("Enabling selected switch for node IP configuration");

                snapshot
                    .prepared_switch
                    .switch
                    .set_cluster_state(true)
                    .await
                    .map_err(|error| {
                        JobFailure::new(
                            JobError::Other,
                            format!(
                                "failed to enable switch '{}' for node IP configuration: {}",
                                primary.node_id, error.message
                            ),
                        )
                    })?;

                job.progress("Configuring node IP addresses through NVUE");

                let switch = snapshot
                    .prepared_switch
                    .switch
                    .as_switch_gb200()
                    .ok_or_else(|| {
                        JobFailure::new(
                            JobError::Internal,
                            "node IP target did not resolve to an NVIDIA switch",
                        )
                    })?;

                switch
                    .reconcile_node_ips(InterfaceType::Primary, node_ips)
                    .await
                    .map_err(|error| {
                        JobFailure::new(
                            JobError::Other,
                            format!(
                                "failed to configure node IP addresses on '{}': {}",
                                primary.node_id, error.message
                            ),
                        )
                    })?;
            }

            job.progress("Enforcing NMX Controller security on the primary switch");

            let prepared_switch_storage;

            let prepared_switch = if let Some((snapshot, _)) = &node_ip_update {
                &snapshot.prepared_switch
            } else {
                prepared_switch_storage = self
                    .build_ephemeral_switch(&primary, request_domain)
                    .await
                    .map_err(|error| {
                        JobFailure::new(
                            JobError::Other,
                            format!(
                                "failed to prepare primary switch '{}': {}",
                                primary.node_id, error.message
                            ),
                        )
                    })?;

                &prepared_switch_storage
            };

            let switch_target = prepared_switch.target_ip.to_string();

            self.enforce_primary_nmx_controller_security(
                prepared_switch.switch.as_ref(),
                &switch_target,
                &prepared_switch.tls_server_name,
                request_domain,
                job.cancellation_token(),
            )
            .await
            .map_err(|error| {
                JobFailure::new(
                    JobError::Other,
                    format!(
                        "failed to enforce NMX Controller security on primary switch '{}': {}",
                        primary.node_id, error.message
                    ),
                )
            })?;

            job.progress("Configuring NMX Controller and waiting for control-plane convergence");

            // Enabling for node IP configuration can expose gRPC before NMX-C
            // accepts Hello. Other switch paths preserve the single attempt.
            let retry_unavailable_hello = node_ip_update.is_some();

            self.reconcile_scale_up_fabric_primary(
                &primary,
                &config,
                request_domain,
                retry_unavailable_hello,
            )
            .await
            .map_err(|message| JobFailure::new(JobError::Other, message))?;

            job.progress("Reading back fabric status");

            let config_file_names = config
                .extra_static_configs
                .iter()
                .map(|entry| entry.config_file_name.as_str())
                .collect::<Vec<_>>();

            let status = self
                .read_scale_up_fabric_status(&nodes, request_domain, &config_file_names)
                .await
                .map_err(|(message, partial_status)| {
                    let failure = JobFailure::new(JobError::Other, message);

                    match serde_json::to_string(&partial_status) {
                        Ok(result_json) => failure.with_result_json(result_json),
                        Err(error) => {
                            tracing::warn!(
                                error = %error,
                                "failed to serialize partial scale-up fabric status"
                            );

                            failure
                        }
                    }
                })?;

            validate_reconciled_scale_up_fabric_status(&status, &primary.node_id, &config)
                .map_err(|message| JobFailure::new(JobError::Other, message))?;

            // Re-read node IPs after NMX-C reconciliation so later drift cannot
            // pass the job's final checks.
            if let Some((snapshot, expected_node_ips)) = &node_ip_update {
                job.progress("Verifying node IP addresses");

                let switch = snapshot
                    .prepared_switch
                    .switch
                    .as_switch_gb200()
                    .ok_or_else(|| {
                        JobFailure::new(
                            JobError::Internal,
                            "node IP target did not resolve to an NVIDIA switch",
                        )
                    })?;

                let observed_node_ips =
                    switch
                        .get_node_ips(InterfaceType::Primary)
                        .await
                        .map_err(|error| {
                            JobFailure::new(
                                JobError::Other,
                                format!(
                                    "failed to verify node IP addresses on '{}': {}",
                                    primary.node_id, error.message
                                ),
                            )
                        })?;

                if observed_node_ips != **expected_node_ips {
                    return Err(JobFailure::new(
                        JobError::Other,
                        format!(
                            "node IP addresses on '{}' did not match: expected \
                             {expected_node_ips:?}, observed {observed_node_ips:?}",
                            primary.node_id
                        ),
                    ));
                }
            }

            serde_json::to_string(&status).map_err(|error| {
                JobFailure::new(
                    JobError::Internal,
                    format!("failed to serialize scale-up fabric status: {error}"),
                )
            })
        }
        .await;

        match result {
            Ok(result_json) => job.complete("Completed", result_json),
            Err(mut failure) => {
                job.progress("Restoring prior scale-up fabric switch state");

                let mut rollback_errors = Vec::new();

                // Restore node IPs while affected switches are enabled, then
                // restore the original enabled state across the full request.
                rollback_errors.extend(
                    join_all(
                        snapshots
                            .iter()
                            .filter(|snapshot| snapshot.node_ip.is_some())
                            .map(|snapshot| async {
                                let mut errors = Vec::new();

                                let node_ip = snapshot
                                    .node_ip
                                    .as_ref()
                                    .expect("filtered node IP snapshot");

                                let Some(switch) = node_ip.prepared_switch.switch.as_switch_gb200()
                                else {
                                    errors.push(format!(
                                        "{} node IP addresses: target did not resolve to an NVIDIA \
                                         switch",
                                        snapshot.node.node_id
                                    ));

                                    return errors;
                                };

                                if snapshot.enabled
                                    && snapshot.node.node_id != primary.node_id
                                    && let Err(error) = switch.update_cluster_config(true).await
                                {
                                    errors.push(format!(
                                        "{} cluster state: {}",
                                        snapshot.node.node_id, error.message
                                    ));
                                }

                                if let Err(error) = switch
                                    .reconcile_node_ips(InterfaceType::Primary, &node_ip.node_ips)
                                    .await
                                {
                                    errors.push(format!(
                                        "{} node IP addresses: {}",
                                        snapshot.node.node_id, error.message
                                    ));
                                }

                                errors
                            }),
                    )
                    .await
                    .into_iter()
                    .flatten(),
                );

                rollback_errors.extend(
                    join_all(snapshots.iter().map(|snapshot| async {
                        let result = set_scale_up_fabric_state_for_device(
                            self,
                            snapshot.node.clone(),
                            snapshot.enabled,
                            request_domain,
                        )
                        .await;

                        (result.status != rm::ReturnCode::Success as i32)
                            .then(|| format!("{}: {}", snapshot.node.node_id, result.error_message))
                    }))
                    .await
                    .into_iter()
                    .flatten(),
                );

                if !rollback_errors.is_empty() {
                    failure.message.push_str(&format!(
                        "; switch state rollback incomplete: {}",
                        rollback_errors.join("; ")
                    ));
                }

                job.fail(failure);
            }
        };
    }

    /// Builds a failed response, optionally retaining partial fabric status.
    fn get_scale_up_fabric_status_failure(
        message: &str,
        fabric_status: Option<rm::ScaleUpFabricStatus>,
    ) -> rm::GetScaleUpFabricStatusResponse {
        rm::GetScaleUpFabricStatusResponse {
            status: rm::ReturnCode::Failure.into(),
            fabric_status,
            error_message: message.to_string(),
        }
    }

    /// Declaratively reconcile the NVLink scale-up fabric across the supplied
    /// switches. Validates the request and starts an async job that drives the
    /// fabric toward the desired configuration. The job completes only after
    /// NMX Controller reports `CONTROL_PLANE_STATE_CONFIGURED`, keeping the
    /// potentially long convergence wait outside the initiating RPC. Callers
    /// must serialize requests. Returns a job ID for `GetJobStatus`.
    pub(crate) async fn handle_configure_scale_up_fabric_manager_v2(
        &self,
        req: tonic::Request<rm_v2::ConfigureScaleUpFabricManagerRequest>,
    ) -> std::result::Result<
        tonic::Response<rm_v2::ConfigureScaleUpFabricManagerResponse>,
        tonic::Status,
    > {
        let r = req.into_inner();
        let nodes: Vec<rm::NodeInfo> = r.nodes.map(|ns| ns.nodes).unwrap_or_default();

        if nodes.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "nodes is required and must contain at least one switch",
            ));
        }

        let config = r
            .config
            .ok_or_else(|| tonic::Status::invalid_argument("config is required"))?;

        let config = validate_scale_up_fabric_config(config)?;
        let request_domain = r.domain.as_deref();

        self.validate_scale_up_fabric_nodes(&nodes, request_domain)
            .await?;

        // Job records require a node at admission. The job repeats selection
        // after state snapshotting, immediately before any mutation.
        let job_primary = self
            .select_scale_up_fabric_primary(
                &nodes,
                r.primary_switch_node_id.as_deref(),
                request_domain,
            )
            .await?;

        let pending = self
            .job_tracker
            .create_job(
                &job_primary.rack_id,
                &job_primary.node_id,
                JobType::ConfigureScaleUpFabricManagerV2,
            )
            .map_err(|_| {
                tonic::Status::internal("failed to create scale-up fabric reconciliation job")
            })?;

        let job_id = pending.id().to_string();

        let service = self.clone();
        let request_domain = r.domain.clone();
        self.job_tracker
            .spawn_job(pending, move |job: RmsJobHandle| async move {
                service
                    .run_configure_scale_up_fabric_manager_v2_job(
                        job,
                        nodes,
                        r.primary_switch_node_id,
                        config,
                        request_domain,
                    )
                    .await;
            })
            .detach();

        Ok(tonic::Response::new(
            rm_v2::ConfigureScaleUpFabricManagerResponse { job_id },
        ))
    }

    /// Synchronously inspect the fabric and return the observed configuration
    /// (topology and NMX Controller static config) read from enabled primary.
    ///
    /// Per-switch inspection errors are returned in each switch status. A
    /// response fails only when no switch can be inspected or a single
    /// authoritative NMX Controller source cannot be identified. This RPC does
    /// not change switch configuration.
    pub(crate) async fn handle_get_scale_up_fabric_status(
        &self,
        req: tonic::Request<rm::GetScaleUpFabricStatusRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetScaleUpFabricStatusResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let nodes: Vec<rm::NodeInfo> = r.nodes.map(|ns| ns.nodes).unwrap_or_default();

        if nodes.is_empty() {
            return Ok(tonic::Response::new(
                Self::get_scale_up_fabric_status_failure(
                    "nodes is required and must contain at least one switch",
                    None,
                ),
            ));
        }

        // Primary comes from observed switch state, so callers need no primary hint.
        let status = self
            .read_scale_up_fabric_status(&nodes, r.domain.as_deref(), &[])
            .await;

        let status = match status {
            Ok(status) => status,
            Err((message, partial_status)) => {
                return Ok(tonic::Response::new(
                    Self::get_scale_up_fabric_status_failure(&message, Some(partial_status)),
                ));
            }
        };

        if status
            .switches
            .iter()
            .all(|switch| !switch.error_message.is_empty())
        {
            return Ok(tonic::Response::new(
                Self::get_scale_up_fabric_status_failure(
                    "failed to inspect any requested switch",
                    Some(status),
                ),
            ));
        }

        Ok(tonic::Response::new(rm::GetScaleUpFabricStatusResponse {
            status: rm::ReturnCode::Success.into(),
            fabric_status: Some(status),
            error_message: String::new(),
        }))
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

            let mut tls = self.switch_tls_roots.resolve_client_tls(request_domain)?;

            if tls.authority.is_none() {
                tls.authority = Some(nmx_server_name);
            }

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

    /// Reports whether a switch's scale-up fabric cluster state is enabled.
    ///
    /// NVUE exposes both `enabled` and `start` for an active cluster.
    async fn cluster_state_is_enabled(
        sw: &dyn SwitchScaleUpManagement,
    ) -> std::result::Result<bool, String> {
        let state = sw.get_cluster_state().await.map_err(|e| e.message)?;

        Ok(matches!(
            state.get("state").and_then(|value| value.as_str()),
            Some("enabled" | "start")
        ))
    }

    /// Reports whether NMX Controller already holds all desired configuration.
    ///
    /// An unreadable config is treated as non-matching so reconciliation can
    /// recover it through the legacy primary configuration path.
    async fn nmx_config_matches(
        client: &mut dyn Nmxc,
        config: &rm_v2::ScaleUpFabricConfig,
        gateway_id: &str,
    ) -> bool {
        let config_file_names = config
            .extra_static_configs
            .iter()
            .map(|entry| entry.config_file_name.as_str())
            .collect::<Vec<_>>();

        let Ok((topology_type, extra_static_configs)) =
            read_nmx_fabric_config(client, &config_file_names, gateway_id).await
        else {
            return false;
        };

        topology_type == config.topology_type
            && config
                .extra_static_configs
                .iter()
                .all(|entry| extra_static_configs.contains(entry))
    }

    /// Reconciles primary NMX Controller state and requested extra static config.
    ///
    /// Existing primary setup owns cluster enablement, external gRPC, topology,
    /// and soft-reset recovery. This handler reuses it, then applies only extra
    /// keys. If RMS reconciles an `fm_config` value that was not confirmed
    /// already configured, it restarts NMX Controller before verifying
    /// convergence.
    async fn reconcile_scale_up_fabric_primary(
        &self,
        primary: &rm::NodeInfo,
        config: &rm_v2::ScaleUpFabricConfig,
        request_domain: Option<&str>,
        retry_unavailable_hello: bool,
    ) -> std::result::Result<(), String> {
        let EphemeralSwitch {
            switch,
            target_ip,
            tls_server_name,
        } = self
            .build_ephemeral_switch(primary, request_domain)
            .await
            .map_err(|error| error.message)?;

        let nvue = switch.nvue_client().ok_or_else(|| {
            format!(
                "NVUE client is unavailable for switch '{}'",
                primary.node_id
            )
        })?;

        let switch_target = target_ip.to_string();

        let primary_enabled = Self::cluster_state_is_enabled(switch.as_ref())
            .await
            .unwrap_or(false);

        if primary_enabled
            && let Ok(mut client) = self
                .connect_nmx_controller(
                    &switch_target,
                    &tls_server_name,
                    request_domain,
                    retry_unavailable_hello,
                )
                .await
            && Self::nmx_config_matches(client.as_mut(), config, &self.nmx_gateway_id).await
        {
            tracing::info!(
                node = %primary.node_id,
                "NVLink fabric already matches requested state"
            );

            return wait_for_nmx_controller_configured(
                switch.as_ref(),
                nvue.as_ref(),
                &primary.node_id,
            )
            .await;
        }

        let response = self
            .handle_configure_scale_up_fabric_manager(tonic::Request::new(
                rm::ConfigureScaleUpFabricManagerRequest {
                    node: Some(primary.clone()),
                    topology_type: config.topology_type.clone(),
                    domain: request_domain.map(ToOwned::to_owned),
                },
            ))
            .await
            .map_err(|error| error.message().to_owned())?
            .into_inner();

        if response.status != rm::ReturnCode::Success as i32 {
            return Err(format!(
                "failed to configure primary switch '{}': {}",
                primary.node_id, response.message
            ));
        }

        if config.extra_static_configs.is_empty() {
            return wait_for_nmx_controller_configured(
                switch.as_ref(),
                nvue.as_ref(),
                &primary.node_id,
            )
            .await;
        }

        let mut client = self
            .connect_nmx_controller(
                &switch_target,
                &tls_server_name,
                request_domain,
                retry_unavailable_hello,
            )
            .await?;

        let mut fm_config_reconciled = false;

        for entry in &config.extra_static_configs {
            let result = set_nmx_static_config(
                client.as_mut(),
                &entry.config_file_name,
                &entry.key,
                &entry.value,
                &self.nmx_gateway_id,
            )
            .await;

            if !result.is_success() {
                return Err(format!(
                    "failed to set NMX Controller static config {}/{}",
                    entry.config_file_name, entry.key
                ));
            }

            fm_config_reconciled |= entry.config_file_name == NMX_C_FM_CONFIG_FILE
                && matches!(result, NmxSetStaticConfigResult::SetSucceeded);
        }

        drop(client);

        if fm_config_reconciled {
            switch
                .restart_cluster_app(NMX_CONTROLLER_APP_NAME)
                .await
                .map_err(|error| {
                    format!(
                        "failed to restart NMX Controller on primary switch '{}': {}",
                        primary.node_id, error.message
                    )
                })?;
        }

        wait_for_nmx_controller_configured(switch.as_ref(), nvue.as_ref(), &primary.node_id).await
    }

    /// Reads switch state concurrently, then reads config from enabled primary.
    ///
    /// Exactly one enabled switch is required before NMX Controller config can
    /// be reported; zero enabled switches return empty configuration and multiple
    /// enabled switches are reported as drift. Failures retain completed switch
    /// reads in a partial status.
    async fn read_scale_up_fabric_status(
        &self,
        nodes: &[rm::NodeInfo],
        request_domain: Option<&str>,
        extra_config_files: &[&str],
    ) -> std::result::Result<rm::ScaleUpFabricStatus, (String, rm::ScaleUpFabricStatus)> {
        // Read all switches concurrently to keep this synchronous RPC fast.
        let reads = join_all(
            nodes
                .iter()
                .map(|node| self.read_switch(node, request_domain)),
        )
        .await;

        let (switches, connections): (Vec<_>, Vec<_>) = reads.into_iter().unzip();

        let partial_status = || rm::ScaleUpFabricStatus {
            switches: switches.clone(),
            ..Default::default()
        };

        // One enabled switch is the authoritative NMX Controller source. Multiple enabled
        // switches are drift and cannot be represented as one observed primary.
        let observed_idx = observed_primary_index(switches.iter())
            .map_err(|message| (message, partial_status()))?;

        // Read topology and static config from observed primary (empty when none enabled).
        let primary_connection =
            observed_idx.and_then(|idx| connections.get(idx).cloned().flatten());

        let (topology_type, extra_static_configs) =
            if let Some((switch_target, tls_server_name)) = primary_connection {
                let mut client = self
                    .connect_nmx_controller(&switch_target, &tls_server_name, request_domain, false)
                    .await
                    .map_err(|message| (message, partial_status()))?;

                read_nmx_fabric_config(client.as_mut(), extra_config_files, &self.nmx_gateway_id)
                    .await
                    .map_err(|message| (message, partial_status()))?
            } else {
                (String::new(), Vec::new())
            };

        Ok(rm::ScaleUpFabricStatus {
            topology_type,
            extra_static_configs,
            switches,
        })
    }

    /// Reads one switch's observed fabric state and fabric manager app status.
    ///
    /// Cluster-state and app-status queries run concurrently. Inspection errors
    /// remain in `error_message` so other requested switches can still be read.
    /// The target address and TLS name allow the selected primary to be contacted
    /// for its NMX Controller configuration.
    async fn read_switch(
        &self,
        node: &rm::NodeInfo,
        request_domain: Option<&str>,
    ) -> (rm::ScaleUpFabricSwitchStatus, Option<(String, String)>) {
        let mut entry = rm::ScaleUpFabricSwitchStatus {
            node_id: node.node_id.clone(),
            enabled: false,
            fabric_manager_status: String::new(),
            error_message: String::new(),
        };

        let EphemeralSwitch {
            switch: sw,
            target_ip,
            tls_server_name,
        } = match self.build_ephemeral_switch(node, request_domain).await {
            Ok(ephemeral) => ephemeral,
            Err(e) => {
                entry.error_message = e.message;
                return (entry, None);
            }
        };

        let connection = Some((target_ip.to_string(), tls_server_name));

        // Query cluster state and fabric manager app status concurrently.
        let (enabled_result, apps_result) = tokio::join!(
            Self::cluster_state_is_enabled(sw.as_ref()),
            sw.get_cluster_apps_status(NMX_CONTROLLER_APP_NAME),
        );

        match enabled_result {
            Ok(enabled) => entry.enabled = enabled,
            Err(message) => {
                entry.error_message = message;
                return (entry, connection);
            }
        }

        // Best-effort raw fabric manager app status reported by the switch.
        match apps_result {
            Ok(apps) => {
                if let Some(app_status) = apps.get("status").and_then(|value| value.as_str()) {
                    entry.fabric_manager_status = app_status.to_string();
                }
            }
            Err(e) => {
                tracing::debug!(node = %entry.node_id, error = %e.message, "nmx-controller status read failed");
            }
        }

        (entry, connection)
    }
}

#[cfg(test)]
mod reconciliation_tests;

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use nvue_client::{
        ClientConfig as NvueClientConfig, ClientCredentials as NvueClientCredentials,
        ClientEndpoint as NvueClientEndpoint, SharedClient as SharedNvueClient,
    };
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::libnmxc::test_support::FakeNmxc;

    #[tokio::test]
    async fn hello_retries_transient_controller_startup_failure() {
        let mut client = FakeNmxc {
            hello_results: VecDeque::from([
                Err(crate::libnmxc::NmxcError::Status(
                    tonic::Status::unavailable("REST server is not initialized"),
                )),
                Ok(FakeNmxc::hello_response()),
            ]),
            ..FakeNmxc::default()
        };

        hello_nmx_controller(&mut client, "test-gateway", true)
            .await
            .expect("transient Hello failure should be retried");

        assert_eq!(client.hello_calls, 2);
    }

    #[tokio::test]
    async fn hello_does_not_retry_permanent_failure() {
        let mut client = FakeNmxc {
            hello_results: VecDeque::from([
                Err(crate::libnmxc::NmxcError::Status(
                    tonic::Status::unauthenticated("invalid credentials"),
                )),
                Ok(FakeNmxc::hello_response()),
            ]),
            ..FakeNmxc::default()
        };

        let error = hello_nmx_controller(&mut client, "test-gateway", true)
            .await
            .expect_err("permanent Hello failure should not be retried");

        assert!(error.contains("invalid credentials"));
        assert_eq!(client.hello_calls, 1);
    }

    #[tokio::test]
    async fn hello_preserves_single_attempt_when_retry_is_disabled() {
        let mut client = FakeNmxc {
            hello_results: VecDeque::from([
                Err(crate::libnmxc::NmxcError::invalid_response(
                    "REST server is not initialized",
                )),
                Ok(FakeNmxc::hello_response()),
            ]),
            ..FakeNmxc::default()
        };

        let error = hello_nmx_controller(&mut client, "test-gateway", false)
            .await
            .expect_err("Hello should not retry when retry is disabled");

        assert_eq!(error, "NMX Controller Hello failed");
        assert_eq!(client.hello_calls, 1);
    }

    fn node_with_ip(node_id: &str, address: &str, descriptor_only: bool) -> rm::NodeInfo {
        rm::NodeInfo {
            node_id: node_id.to_owned(),
            rack_id: "rack-01".to_owned(),
            r#type: (!descriptor_only).then_some(rm::NodeType::SwitchVrnvl72Nvidia as i32),
            node_descriptor: descriptor_only.then(|| rm::NodeDescriptor {
                attributes: HashMap::from([
                    ("role".to_owned(), "switch".to_owned()),
                    ("vendor".to_owned(), "nvidia".to_owned()),
                    ("product_family".to_owned(), "vrnvl72".to_owned()),
                ]),
            }),
            host_endpoint: Some(rm::Endpoint {
                interface: Some(rm::NetworkInterface {
                    ip_address: address.to_owned(),
                    mac_address: String::new(),
                    host_name: None,
                }),
                port: 443,
                credentials: Some(rm::Credentials {
                    auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                        username: "admin".to_owned(),
                        password: "password".to_owned(),
                    })),
                }),
            }),
            ..Default::default()
        }
    }

    fn test_nvue_client(server: &MockServer) -> SharedNvueClient {
        NvueClient::new(NvueClientConfig {
            endpoint: NvueClientEndpoint::http(
                server.address().ip().to_string(),
                server.address().port(),
            ),
            credentials: NvueClientCredentials::new("admin", "password"),
            dangerously_accept_invalid_certs: false,
        })
        .expect("test NVUE client should be valid")
    }

    #[test]
    fn configured_wait_production_timeout_is_three_minutes() {
        assert_eq!(
            NMX_CONTROLLER_CONFIGURED_TIMEOUT,
            Duration::from_secs(3 * 60)
        );
    }

    #[test]
    fn get_node_ips_uses_legacy_and_descriptor_host_endpoints()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = node_with_ip("sw-01", "192.0.2.101", false);
        let second = node_with_ip("sw-02", "192.0.2.102", true);

        let node_ips = get_node_ips(&[first, second]).map_err(std::io::Error::other)?;

        assert_eq!(node_ips.len(), 2);
        assert!(node_ips.contains(&"192.0.2.101".parse()?));
        assert!(node_ips.contains(&"192.0.2.102".parse()?));

        Ok(())
    }

    #[test]
    fn mixed_node_types_are_rejected_regardless_of_selected_type() {
        let mut incompatible = node_with_ip("sw-01", "192.0.2.101", false);
        incompatible.r#type = Some(rm::NodeType::SwitchGb200Nvidia as i32);
        let supported = node_with_ip("sw-02", "192.0.2.102", false);

        let error =
            get_node_ips_for_request(NodeType::SwitchGb200Nvidia, &[incompatible, supported])
                .unwrap_err();

        assert_eq!(error, "node IP configuration does not support node 'sw-01'");
    }

    #[tokio::test]
    async fn configured_wait_follows_transitional_control_plane_states() {
        let server = MockServer::start().await;
        let app_path = "/nvue_v1/cluster/apps/nmx-controller";

        for state in [
            "CONTROL_PLANE_STATE_UNCONFIGURED",
            "CONTROL_PLANE_STATE_OFFLINE",
            "CONTROL_PLANE_STATE_STANDBY",
        ] {
            Mock::given(method("GET"))
                .and(path(app_path))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "status": "not ok",
                    "addition-info": state,
                })))
                .up_to_n_times(1)
                .expect(1)
                .mount(&server)
                .await;
        }

        Mock::given(method("GET"))
            .and(path(app_path))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "additional-info": "CONTROL_PLANE_STATE_CONFIGURED",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let nvue = test_nvue_client(&server);

        wait_for_nmx_controller_configured_with_timing(
            &nvue,
            "sw-01",
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .await
        .expect("configured state should complete the convergence wait");
    }

    #[tokio::test]
    async fn configured_wait_retries_transient_nvue_read_failure() {
        let server = MockServer::start().await;
        let app_path = "/nvue_v1/cluster/apps/nmx-controller";

        Mock::given(method("GET"))
            .and(path(app_path))
            .respond_with(ResponseTemplate::new(503).set_body_string("temporary NVUE failure"))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(app_path))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "addition-info": "CONTROL_PLANE_STATE_CONFIGURED",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let nvue = test_nvue_client(&server);

        wait_for_nmx_controller_configured_with_timing(
            &nvue,
            "sw-01",
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .await
        .expect("transient NVUE failure should remain retryable");
    }

    #[tokio::test]
    async fn configured_wait_soft_resets_once_after_unhealthy_timeout() {
        let server = MockServer::start().await;
        let app_path = "/nvue_v1/cluster/apps/nmx-controller";

        Mock::given(method("GET"))
            .and(path(app_path))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "not ok",
                "reason": "NMXC: ERROR",
                "addition-info": "CONTROL_PLANE_STATE_STANDBY",
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(app_path))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "addition-info": "CONTROL_PLANE_STATE_CONFIGURED",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let nvue = test_nvue_client(&server);
        let restart_calls = AtomicUsize::new(0);

        wait_for_nmx_controller_configured_with_recovery(
            &nvue,
            "sw-01",
            Duration::ZERO,
            Duration::ZERO,
            || async {
                restart_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .expect("configured state after soft reset should complete convergence");

        assert_eq!(restart_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn configured_wait_does_not_soft_reset_after_nvue_only_timeout() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(503).set_body_string("NVUE unavailable"))
            .expect(1)
            .mount(&server)
            .await;

        let nvue = test_nvue_client(&server);
        let restart_calls = AtomicUsize::new(0);

        let error = wait_for_nmx_controller_configured_with_recovery(
            &nvue,
            "sw-01",
            Duration::ZERO,
            Duration::ZERO,
            || async {
                restart_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .expect_err("NVUE-only timeout must fail without soft reset");

        assert!(error.contains("HTTP GET /nvue_v1/cluster/apps/nmx-controller returned 503"));
        assert_eq!(restart_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn configured_wait_timeout_reports_last_observed_state() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "reason": "GFM: UNCONFIGURED",
                "addition-info": "CONTROL_PLANE_STATE_UNCONFIGURED",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let nvue = test_nvue_client(&server);

        let error = wait_for_nmx_controller_configured_with_timing(
            &nvue,
            "sw-01",
            Duration::ZERO,
            Duration::ZERO,
        )
        .await
        .expect_err("unconfigured state must not complete the convergence wait");

        assert!(error.message.contains("CONTROL_PLANE_STATE_CONFIGURED"));
        assert!(error.message.contains("CONTROL_PLANE_STATE_UNCONFIGURED"));
        assert!(error.message.contains("GFM: UNCONFIGURED"));
        assert!(error.soft_reset_eligible);
    }

    #[tokio::test]
    async fn configured_wait_timeout_reports_last_nvue_error() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(503).set_body_string("NVUE unavailable"))
            .expect(1)
            .mount(&server)
            .await;

        let nvue = test_nvue_client(&server);

        let error = wait_for_nmx_controller_configured_with_timing(
            &nvue,
            "sw-01",
            Duration::ZERO,
            Duration::ZERO,
        )
        .await
        .expect_err("persistent NVUE failure must fail the convergence wait");

        assert!(
            error
                .message
                .contains("HTTP GET /nvue_v1/cluster/apps/nmx-controller returned 503")
        );

        assert!(!error.soft_reset_eligible);
    }

    #[test]
    fn config_validation_normalizes_requested_keys() {
        let config = rm_v2::ScaleUpFabricConfig {
            topology_type: " topology-a ".into(),
            extra_static_configs: vec![rm::ScaleUpFabricStaticConfig {
                config_file_name: " sm_config.cfg ".into(),
                key: " ROUTING_MODE ".into(),
                value: " value ".into(),
            }],
        };

        let validated =
            validate_scale_up_fabric_config(config).expect("configuration should be valid");

        assert_eq!(validated.topology_type, "topology-a");

        assert_eq!(
            validated.extra_static_configs[0].config_file_name,
            "sm_config.cfg"
        );

        assert_eq!(validated.extra_static_configs[0].key, "ROUTING_MODE");
        assert_eq!(validated.extra_static_configs[0].value, "value");
    }

    #[test]
    fn status_failure_preserves_switch_errors() {
        let status = rm::ScaleUpFabricStatus {
            switches: vec![rm::ScaleUpFabricSwitchStatus {
                node_id: "sw-03".into(),
                error_message: "TLS handshake failed".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let response = RackManagerServiceImpl::get_scale_up_fabric_status_failure(
            "failed to inspect any requested switch",
            Some(status),
        );

        assert_eq!(
            response.fabric_status.unwrap().switches[0].error_message,
            "TLS handshake failed"
        );
    }

    #[test]
    fn config_validation_rejects_duplicate_extra_keys() {
        let entry = rm::ScaleUpFabricStaticConfig {
            config_file_name: NMX_C_SM_CONFIG_FILE.into(),
            key: "ROUTING_MODE".into(),
            value: "value".into(),
        };

        let config = rm_v2::ScaleUpFabricConfig {
            topology_type: "topology-a".into(),
            extra_static_configs: vec![entry.clone(), entry],
        };

        let error = validate_scale_up_fabric_config(config)
            .expect_err("duplicate static config should be rejected");

        assert!(error.message().contains("duplicate extra static config"));
    }

    #[test]
    fn config_validation_rejects_explicit_primary_descriptor_key() {
        let mut primary = rm::NodeInfo {
            node_descriptor: Some(rm::NodeDescriptor {
                attributes: [("fm_config:KEY_A".to_owned(), "1".to_owned())].into(),
            }),
            ..Default::default()
        };

        let config = rm_v2::ScaleUpFabricConfig {
            topology_type: "topology-a".into(),
            extra_static_configs: vec![rm::ScaleUpFabricStaticConfig {
                config_file_name: NMX_C_FM_CONFIG_FILE.into(),
                key: "KEY_A".into(),
                value: "0".into(),
            }],
        };

        let error = validate_primary_scale_up_fabric_config(&mut primary, config)
            .expect_err("descriptor and explicit static config must not conflict");

        assert!(error.message().contains("duplicate extra static config"));
    }

    #[test]
    fn v2_normalization_preserves_and_maps_scoped_primary_configs() {
        let mut nodes = Some(rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
                node_descriptor: Some(rm::NodeDescriptor {
                    attributes: [
                        ("fm_config:KEY_B".to_owned(), " second ".to_owned()),
                        ("fm_config:KEY_A".to_owned(), " first ".to_owned()),
                        ("unrecognized_metadata".to_owned(), "ignored".to_owned()),
                    ]
                    .into(),
                }),
                ..Default::default()
            }],
        });

        normalize_v2_node_set(&mut nodes).expect("scoped config should survive normalization");

        let mut primary = nodes
            .expect("normalized node set should remain present")
            .nodes
            .remove(0);

        assert_eq!(
            primary
                .node_descriptor
                .as_ref()
                .and_then(|descriptor| descriptor.attributes.get("fm_config:KEY_A"))
                .map(String::as_str),
            Some(" first ")
        );

        assert!(
            !primary
                .node_descriptor
                .as_ref()
                .is_some_and(|descriptor| descriptor
                    .attributes
                    .contains_key("unrecognized_metadata"))
        );

        let config = validate_primary_scale_up_fabric_config(
            &mut primary,
            rm_v2::ScaleUpFabricConfig {
                topology_type: "topology-a".into(),
                extra_static_configs: Vec::new(),
            },
        )
        .expect("scoped primary config should map to static config");

        assert_eq!(
            config.extra_static_configs,
            vec![
                rm::ScaleUpFabricStaticConfig {
                    config_file_name: NMX_C_FM_CONFIG_FILE.into(),
                    key: "KEY_A".into(),
                    value: "first".into(),
                },
                rm::ScaleUpFabricStaticConfig {
                    config_file_name: NMX_C_FM_CONFIG_FILE.into(),
                    key: "KEY_B".into(),
                    value: "second".into(),
                },
            ]
        );
    }

    #[test]
    fn config_validation_rejects_empty_descriptor_fm_config_key() {
        let mut primary = rm::NodeInfo {
            node_descriptor: Some(rm::NodeDescriptor {
                attributes: [("fm_config:".to_owned(), "value".to_owned())].into(),
            }),
            ..Default::default()
        };

        let error = validate_primary_scale_up_fabric_config(
            &mut primary,
            rm_v2::ScaleUpFabricConfig {
                topology_type: "topology-a".into(),
                extra_static_configs: Vec::new(),
            },
        )
        .expect_err("empty descriptor-backed keys must be rejected");

        assert!(
            error
                .message()
                .contains("extra static config file names and keys must be non-empty")
        );
    }

    #[test]
    fn observed_primary_rejects_multiple_enabled_switches() {
        let switches = vec![
            rm::ScaleUpFabricSwitchStatus {
                node_id: "sw-01".into(),
                enabled: true,
                ..Default::default()
            },
            rm::ScaleUpFabricSwitchStatus {
                node_id: "sw-02".into(),
                enabled: true,
                ..Default::default()
            },
        ];

        let error =
            observed_primary_index(&switches).expect_err("multiple enabled switches are invalid");

        assert!(error.contains("sw-01"));
        assert!(error.contains("sw-02"));
    }

    #[test]
    fn reconciled_status_requires_only_the_selected_switch_enabled() {
        let config = rm_v2::ScaleUpFabricConfig {
            topology_type: "topology-a".into(),
            extra_static_configs: vec![rm::ScaleUpFabricStaticConfig {
                config_file_name: NMX_C_SM_CONFIG_FILE.into(),
                key: "ROUTING_MODE".into(),
                value: "expected".into(),
            }],
        };

        let mut status = rm::ScaleUpFabricStatus {
            topology_type: config.topology_type.clone(),
            extra_static_configs: config.extra_static_configs.clone(),
            switches: vec![
                rm::ScaleUpFabricSwitchStatus {
                    node_id: "sw-01".into(),
                    enabled: true,
                    ..Default::default()
                },
                rm::ScaleUpFabricSwitchStatus {
                    node_id: "sw-02".into(),
                    enabled: false,
                    ..Default::default()
                },
            ],
        };

        validate_reconciled_scale_up_fabric_status(&status, "sw-01", &config)
            .expect("selected primary should validate");

        status.switches[1].enabled = true;

        let error = validate_reconciled_scale_up_fabric_status(&status, "sw-01", &config)
            .expect_err("a second enabled switch should fail validation");

        assert!(error.contains("non-primary switch"));
    }

    #[test]
    fn reconciled_status_rejects_unreadable_switch() {
        let config = rm_v2::ScaleUpFabricConfig {
            topology_type: "topology-a".into(),
            extra_static_configs: Vec::new(),
        };
        let status = rm::ScaleUpFabricStatus {
            topology_type: config.topology_type.clone(),
            switches: vec![
                rm::ScaleUpFabricSwitchStatus {
                    node_id: "sw-01".into(),
                    enabled: true,
                    ..Default::default()
                },
                rm::ScaleUpFabricSwitchStatus {
                    node_id: "sw-02".into(),
                    error_message: "NVUE timeout".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let error = validate_reconciled_scale_up_fabric_status(&status, "sw-01", &config)
            .expect_err("unreadable switch should prevent job completion");

        assert!(error.contains("sw-02"));
        assert!(error.contains("NVUE timeout"));
    }

    #[tokio::test]
    async fn full_config_read_includes_requested_files_and_sorts_entries() {
        let response = FakeNmxc::static_config_files_response(&[
            (
                NMX_C_FM_CONFIG_FILE,
                &format!("{NMX_C_TOPOLOGY_KEY}=topology-a\nzeta=2\n"),
            ),
            (NMX_C_SM_CONFIG_FILE, "alpha 1\n"),
            ("custom_config", "beta=3\n"),
        ]);

        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([Ok(response)]),
            ..FakeNmxc::default()
        };

        let (topology, entries) =
            read_nmx_fabric_config(&mut client, &["custom_config"], "test-gateway")
                .await
                .expect("full config read should succeed");

        assert_eq!(topology, "topology-a");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, "beta");
        assert_eq!(entries[1].key, "zeta");
        assert_eq!(entries[2].key, "alpha");

        assert_eq!(
            client.get_static_config_file_calls,
            vec![vec![
                NMX_C_FM_CONFIG_FILE.to_owned(),
                NMX_C_SM_CONFIG_FILE.to_owned(),
                "custom_config".to_owned(),
            ]]
        );
    }

    #[tokio::test]
    async fn static_config_update_supports_non_topology_entries() {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(FakeNmxc::static_config_response(
                    NMX_C_SM_CONFIG_FILE,
                    "ROUTING_MODE",
                    "old",
                )),
                Ok(FakeNmxc::static_config_response(
                    NMX_C_SM_CONFIG_FILE,
                    "ROUTING_MODE",
                    "new",
                )),
            ]),
            set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
            ..FakeNmxc::default()
        };

        let result = set_nmx_static_config(
            &mut client,
            NMX_C_SM_CONFIG_FILE,
            "ROUTING_MODE",
            "new",
            "test-gateway",
        )
        .await;

        assert_eq!(result, NmxSetStaticConfigResult::SetSucceeded);
        assert_eq!(client.set_calls[0].2, "new");
        assert_eq!(client.set_calls[0].3, "test-gateway");
    }
}
