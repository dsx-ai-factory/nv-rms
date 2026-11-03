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

mod recovery;
#[cfg(test)]
mod test;

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::api::grpc::firmware_task_util::terminal_firmware_task_error;
use crate::domain::node::*;
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::transport::redfish_client::{
    NvidiaMnnvlinkTopology, RedfishClient, ResetType as RedfishResetType,
    redfish_client_from_endpoint_config,
};
use crate::utilities::error::{Result, RmsError};
use recovery::{RecoveryDevice, RecoveryPlan, RecoveryRoster, components_in_recovery_state};

/// Concrete node for NVIDIA reference GB200 compute BMCs (Redfish).
///
/// The concrete implementation remains responsible for protocol behavior.
/// An optional hardware profile supplies validated resource IDs, timeouts, and
/// NVFWUPD selection through [`NodeConfig::runtime_config`].
///
/// Power:     nv-redfish typed ComputerSystem/Manager reads and reset actions
/// Firmware:  nv-redfish UpdateService multipart update
/// Activate:  NVFWUPD workflow activation
pub struct NvidiaGb200Compute {
    id: String,
    rack_id: String,
    node_type: NodeType,
    runtime_config: RedfishNvfwupdComputeConfig,
    has_hardware_profile: bool,
    bmc_endpoint: EndpointConfig,
    host_endpoint: Option<EndpointConfig>,
    host_mac_addresses: Vec<String>,
    host_ip_addresses: Vec<String>,
    expected_inventory: Option<ExpectedInventoryPolicy>,
    /// The parsed recovery profile, when one is bound. `None` leaves
    /// recovery detection off for this node.
    recovery: Option<RecoveryRoster>,
    redfish: RedfishClient,

    // Serializes full node operations across shared clients. GB300 wrappers
    // use this same lock for direct work and delegate without reacquiring it.
    pub(super) op_lock: tokio::sync::Mutex<()>,
}

/// How long the recovery push waits for its BMC task to reach a terminal
/// state. Recovery pushes are budgeted in single-digit minutes.
const RECOVERY_PUSH_TASK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
const RECOVERY_PUSH_TASK_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
/// Target of every recovery push. The prescribed flash command is the same
/// for the regular and the recovery bundle: ForceUpdate plus this chassis
/// as the single target. An empty component would drop Targets
/// from the request, and ForceUpdate suppresses nvfwupd's own default, so
/// the push would go out untargeted.
const RECOVERY_PUSH_TARGET: &str = "/redfish/v1/Chassis/HGX_Chassis_0";

/// How an HMC update starts: through nvfwupd's gates (expected inventory,
/// write protect disabled, background copy settled), or past them. The
/// recovery push skips them because a tray in recovery cannot pass the gates
/// it is about to repair.
enum UpdateGates {
    Checked {
        expected_inventory: Option<Vec<String>>,
    },
    Skipped,
}

/// Built-in settings used when no hardware profile is attached to a node.
pub(crate) fn default_compute_config(node_type: NodeType) -> RedfishNvfwupdComputeConfig {
    RedfishNvfwupdComputeConfig {
        nvfwupd_server_profile: node_type.nvfwupd_server_profile(),
        computer_system_id: std::sync::Arc::from("System_0"),
        bmc_manager_id: std::sync::Arc::from("BMC_0"),
        hmc_manager_id: std::sync::Arc::from("HGX_BMC_0"),
        device_info_timeout: COMPUTE_DEVICE_INFO_FETCH_TIMEOUT,
        redfish_connect_timeout: std::time::Duration::from_secs(10),
        redfish_request_timeout: std::time::Duration::from_secs(30),
        redfish_upload_timeout: std::time::Duration::from_secs(600),
        power_paths: crate::domain::compute_power::ComputePowerPaths::default(),
        firmware_timing: crate::domain::compute_timing::ComputeFirmwareTiming::default(),
    }
}

impl NvidiaGb200Compute {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        rack_id: String,
        host: String,
        port: u16,
        username: &str,
        password: &str,
        mac_address: String,
        host_mac_addresses: Vec<String>,
        host_ip_addresses: Vec<String>,
        dangerously_accept_invalid_certs: bool,
    ) -> Result<Self> {
        let bmc_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: host,
                mac_address,
                port,
                host_name: None,
            },
            Some(EndpointCredentials::new(username, password)),
            dangerously_accept_invalid_certs,
        );

        Ok(Self {
            id,
            rack_id,
            node_type: NodeType::ComputeGb200Nvidia,
            runtime_config: default_compute_config(NodeType::ComputeGb200Nvidia),
            has_hardware_profile: false,
            redfish: redfish_client_from_endpoint_config(&bmc_endpoint)?,
            bmc_endpoint,
            host_endpoint: None,
            host_mac_addresses,
            host_ip_addresses,
            expected_inventory: None,
            recovery: None,
            op_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Shared constructor: the GB300 NVIDIA, Wiwynn GB200 and VR NVL72 nodes
    /// build their inner value through this function, and the GB300 Lenovo and
    /// Supermicro nodes reach it through the GB300 node, each with its own
    /// `config.node_type`. Anything here that interprets config for the GB200
    /// NVIDIA type specifically must be admitted by node type upstream
    /// (`NodeInstance::from_config`), or the wrappers inherit it silently.
    /// The recovery roster is the first such field.
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type.kind() != NodeKind::Compute {
            return Err(RmsError::invalid_argument(format!(
                "NVIDIA GB200 compute implementation does not support {} nodes",
                config.node_type.kind()
            )));
        }
        let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
            return Err(RmsError::invalid_argument(
                "bmc_endpoint is required for compute nodes",
            ));
        };
        let runtime_config = match config.runtime_config.as_ref() {
            Some(NodeRuntimeConfig::RedfishNvfwupdCompute(runtime_config)) => {
                runtime_config.clone()
            }
            None => default_compute_config(config.node_type),
        };

        Ok(Self {
            id: config.id.clone(),
            rack_id: rack_id.to_owned(),
            node_type: config.node_type,
            redfish:
                crate::transport::redfish_client::redfish_client_from_endpoint_config_with_timeouts(
                    bmc_endpoint,
                    runtime_config.redfish_connect_timeout,
                    runtime_config.redfish_request_timeout,
                )?,
            runtime_config,
            has_hardware_profile: config.runtime_config.is_some(),
            bmc_endpoint: bmc_endpoint.clone(),
            host_endpoint: config.host_endpoint.clone(),
            host_mac_addresses: config
                .host_endpoint
                .as_ref()
                .map(|endpoint| endpoint.endpoint.mac_address.clone())
                .filter(|mac| !mac.is_empty())
                .into_iter()
                .collect(),
            host_ip_addresses: config
                .host_endpoint
                .as_ref()
                .map(|endpoint| endpoint.endpoint.ip_address.clone())
                .into_iter()
                .collect(),
            expected_inventory: config.expected_inventory.clone(),
            recovery: config
                .recovery
                .as_ref()
                .map(RecoveryRoster::parse)
                .transpose()?,
            op_lock: tokio::sync::Mutex::new(()),
        })
    }

    fn expected_flint_device_counts(&self) -> nvfwupd::workflow::FlintExpectedDeviceCounts {
        self.expected_inventory
            .as_ref()
            .map(|policy| policy.flint_device_counts.as_ref().clone())
            .unwrap_or_default()
    }

    // ── MNNVLink topology (tray device info from BMC) ──

    // Returns normalized `{chassis_sn, slot_number, tray_index}` extracted from
    // `.Oem.Nvidia.MNNVLinkTopology` on a ComputerSystem Processor resource.
    pub async fn get_mnnvlink_topology(&self) -> Result<NvidiaMnnvlinkTopology> {
        let _op_guard = self.op_lock.lock().await;
        tracing::info!(node = %self.id, "get_mnnvlink_topology");

        // Bound only the Redfish I/O (the op-lock is already held). This makes a
        // slow/stuck BMC an error instead of stalling an inventory fan-out,
        // while keeping the lock wait itself unbounded so ordinary contention
        // with another node operation queues rather than timing out.
        let timeout = self.runtime_config.device_info_timeout;
        tokio::time::timeout(timeout, self.redfish.nvidia_mnnvlink_topology())
            .await
            .map_err(|_elapsed| {
                RmsError::internal(format!(
                    "mnnvlink topology read exceeded {}s deadline",
                    timeout.as_secs()
                ))
            })?
            .map_err(RmsError::from)
    }

    pub(crate) fn firmware_workflow(&self) -> nvfwupd::workflow_api::WorkflowContext {
        let context = nvfwupd::workflow_api::WorkflowContext::new();
        if self.has_hardware_profile {
            context
                .with_compute_timing(self.runtime_config.firmware_timing.into())
                .with_compute_power_paths((&self.runtime_config.power_paths).into())
        } else {
            // Unmigrated platforms retain the driver's current code defaults.
            context
        }
    }

    /// Run vendor preparation and upload while holding the same operation lock.
    /// Preparation owns cancellation around its I/O so a persistent setting
    /// write can finish before the caller attempts rollback.
    pub(super) async fn update_firmware_with_preparation(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
        preparation: impl Future<Output = Result<()>>,
    ) -> Result<FirmwareUpdateOutcome> {
        if flint_family_for_target(target).is_some() {
            return Err(RmsError::failed_precondition(
                "CX7, CX8, and BF3_NIC targets must use the grouped GB200 in-band firmware workflow",
            ));
        }
        let _op_guard = self.op_lock.lock().await;
        if options
            .cancellation
            .as_ref()
            .is_some_and(|token| token.is_cancelled())
        {
            return Err(RmsError::cancelled("firmware preparation cancelled"));
        }
        preparation.await?;

        tracing::info!(
            node = %self.id,
            component = %target.component,
            file = %target.firmware_file,
            "update_firmware via NVFWUPD"
        );
        let request = nvfwupd_adapter::to_update_request(target, force_update, options);
        let expected_inventory = self
            .expected_inventory_policy()
            .map(|policy| policy.ap_names.as_ref().to_vec());
        self.start_update(request, UpdateGates::Checked { expected_inventory })
            .await
    }

    // ── Private helpers ──

    pub(crate) fn nvfwupd_target_config_for(
        &self,
        node_type: NodeType,
    ) -> nvfwupd::workflow::TargetConfig {
        nvfwupd_adapter::to_target_config_with_credentials(
            node_type,
            &self.bmc_endpoint.endpoint.ip_address,
            self.bmc_endpoint.endpoint.port,
            self.bmc_endpoint.credentials.as_ref(),
            !self.bmc_endpoint.dangerously_accept_invalid_certs,
        )
    }

    pub(crate) fn nvfwupd_target_config(&self) -> nvfwupd::workflow::TargetConfig {
        let mut target = self.nvfwupd_target_config_for(self.node_type);
        target.server_type =
            nvfwupd_adapter::to_server_type(self.runtime_config.nvfwupd_server_profile);
        target
    }

    fn flint_host_target_config(&self) -> Result<nvfwupd::workflow::HostTargetConfig> {
        self.flint_host_target_config_with_home(std::env::var_os("HOME").map(PathBuf::from))
    }

    fn flint_host_target_config_with_home(
        &self,
        home: Option<PathBuf>,
    ) -> Result<nvfwupd::workflow::HostTargetConfig> {
        let bmc_credentials = self.bmc_endpoint.credentials.as_ref().ok_or_else(|| {
            RmsError::failed_precondition(
                "BMC username/password credentials are required to activate GB200 in-band firmware updates",
            )
        })?;
        let (bmc_username, _) = bmc_credentials.user_pass().ok_or_else(|| {
            RmsError::failed_precondition(
                "BMC username/password credentials are required to activate GB200 in-band firmware updates",
            )
        })?;
        // `user_pass()` already rejects an empty username/password, so the only
        // extra guard needed is a whitespace-only username. The password is
        // intentionally not trimmed or space-checked: leading/trailing spaces
        // can be a legitimate part of a password.
        if bmc_username.trim().is_empty() {
            return Err(RmsError::failed_precondition(
                "BMC username/password credentials are required to activate GB200 in-band firmware updates",
            ));
        }
        self.inband_host_target_config_with_home(home)
    }

    pub(super) fn inband_host_target_config(&self) -> Result<nvfwupd::workflow::HostTargetConfig> {
        self.inband_host_target_config_with_home(std::env::var_os("HOME").map(PathBuf::from))
    }

    fn inband_host_target_config_with_home(
        &self,
        home: Option<PathBuf>,
    ) -> Result<nvfwupd::workflow::HostTargetConfig> {
        let host = self.host_endpoint.as_ref().ok_or_else(|| {
            RmsError::failed_precondition("host_endpoint is required for in-band firmware updates")
        })?;
        let host_credentials = host.credentials.as_ref().ok_or_else(|| {
            RmsError::failed_precondition(
                "host_endpoint username/password credentials are required for in-band firmware updates",
            )
        })?;
        let (host_username, host_password) = host_credentials.user_pass().ok_or_else(|| {
            RmsError::failed_precondition(
                "host_endpoint username/password credentials are required for in-band firmware updates",
            )
        })?;
        // As above, `user_pass()` already rejects an empty username/password;
        // only guard against a whitespace-only username. The password is left
        // untouched because spaces can be a legitimate part of a password.
        if host_username.trim().is_empty() {
            return Err(RmsError::failed_precondition(
                "host_endpoint username/password credentials are required for in-band firmware updates",
            ));
        }
        let ssh_known_hosts = home
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "SSH host-key verification requires HOME or an explicit known_hosts path",
                )
            })?
            .join(".ssh/known_hosts")
            .into_os_string()
            .into_string()
            .map_err(|_| {
                RmsError::failed_precondition(
                    "SSH known_hosts path must contain valid UTF-8 characters",
                )
            })?;

        Ok(nvfwupd::workflow::HostTargetConfig {
            ip: host.endpoint.ip_address.clone(),
            username: host_username.to_owned(),
            password: host_password.to_owned(),
            port: Some(host.endpoint.port),
            ssh_known_hosts: Some(ssh_known_hosts),
            // Match the NVFWUPD CLI default: learn the first key in the RMS
            // process user's known_hosts file, then reject changed keys.
            ssh_host_key_mode: nvfwupd::workflow::SshHostKeyMode::TrustOnFirstUse,
        })
    }

    /// Start an HMC update through nvfwupd. The caller holds the op lock.
    async fn start_update(
        &self,
        request: nvfwupd::workflow::FirmwareUpdateRequest,
        gates: UpdateGates,
    ) -> Result<FirmwareUpdateOutcome> {
        let outcome = match gates {
            UpdateGates::Checked { expected_inventory } => {
                self.firmware_workflow()
                    .update_firmware_with_expected_inventory(
                        self.nvfwupd_target_config(),
                        request,
                        expected_inventory,
                    )
                    .await
            }
            UpdateGates::Skipped => {
                self.firmware_workflow()
                    .update_firmware_skipping_preconditions(self.nvfwupd_target_config(), request)
                    .await
            }
        }
        .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_outcome(outcome))
    }

    /// One task status read without the op lock, for callers that hold it.
    async fn task_status(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let status = self
            .firmware_workflow()
            .get_task_status(self.nvfwupd_target_config(), task_id)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::to_task_status(status))
    }

    /// Waits for a recovery push task to reach a terminal state, tolerating
    /// status-read failures until `RECOVERY_PUSH_TASK_TIMEOUT`, or until
    /// `cancel` fires. Recovery keeps its own loop: the job layer's poll loop
    /// takes the op lock the caller of this method holds across the whole push.
    async fn wait_for_recovery_task(
        &self,
        task_id: &str,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let deadline = std::time::Instant::now() + RECOVERY_PUSH_TASK_TIMEOUT;
        loop {
            let failure = match self.task_status(task_id).await {
                Ok(status) if status.completed => {
                    match terminal_firmware_task_error("recovery push task", task_id, &status) {
                        // A push the BMC finished is done, even when a cancel
                        // landed in this read: the flow's next safe point
                        // reports the cancel.
                        None => {
                            if !status.status.eq_ignore_ascii_case("OK") {
                                tracing::warn!(
                                    node = %self.id,
                                    task_id = %task_id,
                                    state = %status.state,
                                    status = %status.status,
                                    message = %status.message,
                                    "recovery push task completed with a non-OK status"
                                );
                            }
                            return Ok(());
                        }
                        Some(error) => Some(error),
                    }
                }
                Ok(_) => None,
                // A transient status-read failure must not abort a recovery the
                // BMC is still running: log it and keep polling. A persistent
                // failure ends at the deadline below.
                Err(error) => {
                    tracing::warn!(
                        node = %self.id,
                        task_id = %task_id,
                        error = %error.message,
                        "recovery push task status read failed; retrying"
                    );
                    None
                }
            };

            // The read is network time. A cancel that landed in it is why the
            // job stops, so it wins over a failed task and over the deadline.
            if cancel.is_cancelled() {
                return Err(RmsError::cancelled(match &failure {
                    Some(error) => format!(
                        "job cancelled; recovery push task {task_id} reported a failure: {}",
                        error.message
                    ),
                    None => format!(
                        "recovery push task {task_id} left to finish on the BMC: job cancelled"
                    ),
                }));
            }
            if let Some(error) = failure {
                return Err(error);
            }
            if std::time::Instant::now() >= deadline {
                return Err(RmsError::timeout(format!(
                    "recovery push task {} did not reach a terminal state within {}s",
                    task_id,
                    RECOVERY_PUSH_TASK_TIMEOUT.as_secs()
                )));
            }

            // Abandoning the wait leaves the BMC to finish the push on its
            // own, and no power cycle follows a cancel, so the tray is never
            // cycled with the push still running. The op lock is released
            // with the push task still running, as after the deadline below.
            if cancel
                .run_until_cancelled(tokio::time::sleep(RECOVERY_PUSH_TASK_POLL_INTERVAL))
                .await
                .is_none()
            {
                return Err(RmsError::cancelled(format!(
                    "recovery push task {task_id} left to finish on the BMC: job cancelled"
                )));
            }
        }
    }
}

#[async_trait]
impl Node for NvidiaGb200Compute {
    fn id(&self) -> &str {
        &self.id
    }

    fn rack_id(&self) -> &str {
        &self.rack_id
    }

    fn node_type(&self) -> NodeType {
        self.node_type
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = HashMap::from([
            ("type".to_owned(), self.node_type.as_str().to_owned()),
            (
                "host".to_owned(),
                self.bmc_endpoint.endpoint.ip_address.clone(),
            ),
            (
                "port".to_owned(),
                self.bmc_endpoint.endpoint.port.to_string(),
            ),
            (
                "macAddress".to_owned(),
                self.bmc_endpoint.endpoint.mac_address.clone(),
            ),
        ]);
        for (i, mac) in self.host_mac_addresses.iter().enumerate() {
            info.insert(format!("hostMac_{i}"), mac.clone());
        }
        for (i, ip) in self.host_ip_addresses.iter().enumerate() {
            info.insert(format!("hostIp_{i}"), ip.clone());
        }
        info
    }

    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        self.expected_inventory.as_ref()
    }

    fn recovery(&self) -> Option<&dyn Recoverable> {
        self.recovery.is_some().then_some(self)
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        let _op_guard = self.op_lock.lock().await;
        tracing::debug!(node = %self.id, "get_power_state");

        Ok(self
            .redfish
            .computer_system_power_state_with_path(
                &self.runtime_config.computer_system_id,
                self.runtime_config.power_paths.system_state_path.as_deref(),
            )
            .await?
            .into())
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;

        let (manager_id, reset_path) = match target {
            PowerTargetType::System => {
                tracing::info!(node = %self.id, ?op, "set_power_state via nv-redfish ComputerSystem.Reset");

                self.redfish
                    .reset_computer_system_with_paths(
                        &self.runtime_config.computer_system_id,
                        self.runtime_config.power_paths.system_state_path.as_deref(),
                        self.runtime_config.power_paths.system_reset_path.as_deref(),
                        nv_redfish_reset_type(op),
                    )
                    .await?;

                return Ok(());
            }
            PowerTargetType::BMC => (
                self.runtime_config.bmc_manager_id.as_ref(),
                self.runtime_config.power_paths.bmc_reset_path.as_deref(),
            ),
            PowerTargetType::HMC => (
                self.runtime_config.hmc_manager_id.as_ref(),
                self.runtime_config.power_paths.hmc_reset_path.as_deref(),
            ),
        };

        tracing::info!(node = %self.id, ?op, manager_id, "set_power_state via nv-redfish Manager.Reset");
        self.redfish
            .reset_manager_with_path(manager_id, reset_path, nv_redfish_reset_type(op))
            .await?;

        Ok(())
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let _op_guard = self.op_lock.lock().await;
        tracing::debug!(node = %self.id, "get_firmware_inventory");
        let inventory = self
            .firmware_workflow()
            .get_firmware_inventory(self.nvfwupd_target_config())
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(inventory
            .into_iter()
            .map(nvfwupd_adapter::to_firmware_info)
            .collect())
    }

    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        let _op_guard = self.op_lock.lock().await;
        let inband_targets = flint_targets(targets)?;
        if !inband_targets.is_empty() {
            if inband_targets
                .iter()
                .map(|target| target.firmware_files.len())
                .sum::<usize>()
                != targets.len()
            {
                return Err(RmsError::invalid_argument(
                    "out-of-band and in-band firmware targets cannot be verified in one request",
                ));
            }
            tracing::info!(node = %self.id, "verify_firmware_package_versions via host Flint");
            let summary = nvfwupd::workflow_api::verify_flint_firmware_versions(
                self.flint_host_target_config()?,
                nvfwupd::workflow::FlintFirmwareVersionCheckRequest {
                    targets: inband_targets,
                    cancellation: None,
                    expected_device_counts: self.expected_flint_device_counts(),
                },
            )
            .await
            .map_err(nvfwupd_adapter::map_error)?;
            return Ok(nvfwupd_adapter::map_version_check_summary(summary));
        }

        tracing::info!(node = %self.id, "verify_firmware_package_versions via NVFWUPD");
        let targets = nvfwupd_adapter::to_version_check_targets(targets);
        let summary = self
            .firmware_workflow()
            .verify_firmware_target_versions_after_activation_with_expected_inventory(
                self.nvfwupd_target_config(),
                targets,
                self.expected_inventory
                    .as_ref()
                    .map(|policy| policy.ap_names.as_ref().to_vec()),
            )
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_version_check_summary(summary))
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        self.update_firmware_with_preparation(target, force_update, options, async { Ok(()) })
            .await
    }

    async fn update_firmware_group(
        &self,
        targets: &[FirmwareTarget],
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let inband_targets = flint_targets(targets)?;
        if inband_targets.is_empty() {
            let [target] = targets else {
                return Err(RmsError::invalid_argument(
                    "out-of-band firmware update groups must contain one target",
                ));
            };
            return self.update_firmware(target, force_update, options).await;
        }
        if inband_targets
            .iter()
            .map(|target| target.firmware_files.len())
            .sum::<usize>()
            != targets.len()
        {
            return Err(RmsError::invalid_argument(
                "out-of-band and in-band firmware targets cannot be updated in one request",
            ));
        }

        let _op_guard = self.op_lock.lock().await;
        tracing::info!(
            node = %self.id,
            target_count = targets.len(),
            "update_firmware_group via host Flint"
        );
        let host_target = self.flint_host_target_config()?;
        let workflow = self.firmware_workflow();
        let boot_summary = workflow
            .ensure_gb200_host_os_ready(
                host_target.clone(),
                self.nvfwupd_target_config(),
                options.cancellation.clone(),
            )
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        tracing::info!(
            node = %self.id,
            details = %boot_summary.details,
            "GB200 host OS is ready for the Flint workflow"
        );
        let outcome = workflow
            .update_gb200_flint_firmware(
                host_target,
                self.nvfwupd_target_config(),
                nvfwupd::workflow::FlintFirmwareUpdateRequest {
                    targets: inband_targets,
                    force_update,
                    timeout_secs: None,
                    cancellation: options.cancellation,
                    expected_device_counts: self.expected_flint_device_counts(),
                },
            )
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_outcome(outcome))
    }

    /// Compute firmware upload:
    ///
    /// 1. **Multipart upload** — POST the firmware file to the BMC UpdateService
    /// 2. Returns a Redfish Task ID for polling progress via `poll_firmware_task`
    async fn start_firmware_upload(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
    ) -> Result<String> {
        if target.resolution.as_ref().is_some_and(|resolved| {
            resolved.update_parameters.is_some() || resolved.oem_parameters.is_some()
        }) {
            return match self
                .update_firmware(target, force_update, FirmwareUpdateOptions::default())
                .await?
            {
                FirmwareUpdateOutcome::Started(handle) => Ok(handle.task_id),
                FirmwareUpdateOutcome::Completed(summary) => Err(RmsError::internal(format!(
                    "compute firmware update completed without task id: {}",
                    summary.message
                ))),
                FirmwareUpdateOutcome::Skipped { reason } => Err(RmsError::already_exists(reason)),
            };
        }
        let _op_guard = self.op_lock.lock().await;

        tracing::info!(
            node = %self.id,
            component = %target.component,
            file = %target.firmware_file,
            "start_firmware_upload"
        );

        let destination = nvfwupd_adapter::firmware_target_selector(target);
        let targets = if destination.is_empty() {
            Vec::new()
        } else {
            vec![destination.to_owned()]
        };

        tracing::info!(node = %self.id, "uploading firmware");
        let task_id = self
            .redfish
            .multipart_update_firmware_from_path_with_timeout(
                &target.firmware_file,
                targets,
                force_update,
                self.runtime_config.redfish_upload_timeout,
            )
            .await?;

        tracing::info!(node = %self.id, task_id, "firmware upload task created");
        Ok(task_id)
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let _op_guard = self.op_lock.lock().await;
        tracing::debug!(node = %self.id, task_id, "poll_firmware_task via NVFWUPD");
        self.task_status(task_id).await
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        let _op_guard = self.op_lock.lock().await;
        tracing::info!(node = %self.id, "activate_firmware via NVFWUPD");
        let wait_for_host_os = request.mode == FirmwareActivationMode::FullGb200Compute
            && self.host_endpoint.is_some();
        let cancellation = request.cancellation.clone();
        let request = nvfwupd_adapter::to_activation_request(request);
        let mut summary = self
            .firmware_workflow()
            .activate_firmware(self.nvfwupd_target_config(), request)
            .await
            .map_err(nvfwupd_adapter::map_error)?;

        if wait_for_host_os {
            let host_summary = self
                .firmware_workflow()
                .ensure_gb200_host_os_ready(
                    self.flint_host_target_config()?,
                    self.nvfwupd_target_config(),
                    cancellation,
                )
                .await
                .map_err(nvfwupd_adapter::map_error)?;
            if let Some(details) = summary.details.as_object_mut() {
                details.insert("post_activation_host_os".to_owned(), host_summary.details);
            } else {
                summary.details = serde_json::json!({
                    "activation": summary.details,
                    "post_activation_host_os": host_summary.details,
                });
            }
            summary.message = format!("{}; {}", summary.message, host_summary.message);
        }

        Ok(nvfwupd_adapter::map_activation_summary(summary))
    }

    async fn activate_firmware(&self) -> Result<()> {
        self.activate_firmware_with(FirmwareActivationRequest {
            mode: FirmwareActivationMode::FullGb200Compute,
            cancellation: None,
        })
        .await
        .map(|_| ())
    }
}

impl RecoveryDevice for NvidiaGb200Compute {
    fn name(&self) -> &str {
        &self.id
    }

    async fn inventory(&self) -> Result<Vec<FirmwareInfo>> {
        Node::get_firmware_inventory(self).await
    }

    /// The power block of one round: force off, aux power cycle, stabilize,
    /// then poll until every `presence` component reports back. Holds the op
    /// lock for the whole block.
    async fn power_block(&self, presence: &[String]) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        tracing::info!(node = %self.id, ?presence, "recovery power block via NVFWUPD");
        self.firmware_workflow()
            .run_compute_recovery_power_block(self.nvfwupd_target_config(), presence)
            .await
            .map_err(nvfwupd_adapter::map_error)
    }

    /// Pushes `firmware_file` at the HMC chassis target with the update gates
    /// skipped, then returns when the BMC task reaches a terminal state,
    /// `RECOVERY_PUSH_TASK_TIMEOUT` passes, or the job is cancelled.
    async fn push(&self, firmware_file: &str, cancel: &CancellationToken) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        tracing::info!(node = %self.id, firmware_file, "recovery bundle push via NVFWUPD");
        let target = FirmwareTarget {
            component: RECOVERY_PUSH_TARGET.to_owned(),
            firmware_file: firmware_file.to_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        };
        let request =
            nvfwupd_adapter::to_update_request(&target, true, FirmwareUpdateOptions::default());
        // Compute targets report Started right after the BMC accepts the
        // upload. The recovery flow must not power-cycle the tray while that
        // task still runs, so wait for a terminal state here.
        match self.start_update(request, UpdateGates::Skipped).await? {
            FirmwareUpdateOutcome::Started(handle) => {
                self.wait_for_recovery_task(&handle.task_id, cancel).await
            }
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl Recoverable for NvidiaGb200Compute {
    /// The roster's watched members that read the recovery signature.
    fn detect(&self, inventory: &[FirmwareInfo]) -> Vec<String> {
        match &self.recovery {
            Some(roster) => components_in_recovery_state(inventory, &roster.watch_set()),
            None => Vec::new(),
        }
    }

    /// Roster, bundles, and the detected list give a plan, the node walks it.
    /// Nothing detected inside the roster's lists returns `Ok` without touching
    /// the node.
    async fn recover(
        &self,
        in_recovery: &[String],
        bundles: RecoveryBundles,
        cancel: &CancellationToken,
    ) -> Result<()> {
        // No op lock here: the sequence takes this node's locked operations
        // one at a time.
        let Some(roster) = self.recovery.clone() else {
            return Err(RmsError::internal(format!(
                "node {}: recover without a bound recovery profile",
                self.id
            )));
        };
        let Some(plan) = RecoveryPlan::new(roster, bundles, in_recovery) else {
            return Ok(());
        };
        plan.run(self, cancel).await
    }
}

pub(crate) fn flint_device_family(component: &str) -> Option<nvfwupd::workflow::FlintDeviceFamily> {
    nvfwupd::workflow::FlintDeviceFamily::from_component(component)
}

/// A profile-resolved Redfish target stays out of band regardless of its label.
pub(crate) fn flint_family_for_target(
    target: &FirmwareTarget,
) -> Option<nvfwupd::workflow::FlintDeviceFamily> {
    target
        .resolution
        .is_none()
        .then(|| flint_device_family(&target.component))
        .flatten()
}

fn flint_targets(
    targets: &[FirmwareTarget],
) -> Result<Vec<nvfwupd::workflow::FlintFirmwareTarget>> {
    let mut grouped = std::collections::BTreeMap::new();
    for target in targets {
        let Some(family) = flint_family_for_target(target) else {
            continue;
        };
        grouped
            .entry(family)
            .or_insert_with(Vec::new)
            .push(target.firmware_file.clone());
    }
    Ok(grouped
        .into_iter()
        .map(
            |(family, firmware_files)| nvfwupd::workflow::FlintFirmwareTarget {
                family,
                firmware_files,
            },
        )
        .collect())
}

// ── Helper functions ──

fn nv_redfish_reset_type(op: PowerOp) -> RedfishResetType {
    match op {
        PowerOp::On => RedfishResetType::On,
        PowerOp::Off => RedfishResetType::GracefulShutdown,
        PowerOp::ForceOn => RedfishResetType::ForceOn,
        PowerOp::ForceOff => RedfishResetType::ForceOff,
        PowerOp::PowerCycle => RedfishResetType::PowerCycle,
        PowerOp::GracefulShutdown => RedfishResetType::GracefulShutdown,
        PowerOp::GracefulRestart => RedfishResetType::GracefulRestart,
        PowerOp::ForceRestart => RedfishResetType::ForceRestart,
        PowerOp::Nmi => RedfishResetType::Nmi,
    }
}

#[cfg(test)]
fn classify_firmware_type(name: &str) -> FirmwareType {
    let lower = name.to_ascii_lowercase();
    if lower.contains("bmc") {
        FirmwareType::BMC
    } else if lower.contains("bios") || lower.contains("uefi") {
        FirmwareType::BIOS
    } else if lower.contains("cpld") {
        FirmwareType::CPLD
    } else if lower.contains("fpga") {
        FirmwareType::FPGA
    } else if lower.contains("nic") {
        FirmwareType::NIC
    } else if lower.contains("hba") {
        FirmwareType::HBA
    } else if lower.contains("hmc") || lower.contains("hgx") {
        FirmwareType::HMC
    } else {
        FirmwareType::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::test::{gb200_roster_table, hmc_entry, recovery_profile};
    use super::*;
    use crate::utilities::error::ErrorCode;

    fn test_runtime_config() -> RedfishNvfwupdComputeConfig {
        RedfishNvfwupdComputeConfig {
            nvfwupd_server_profile: NvfwupdServerProfile::ComputeGb200,
            computer_system_id: std::sync::Arc::from("System_0"),
            bmc_manager_id: std::sync::Arc::from("BMC_0"),
            hmc_manager_id: std::sync::Arc::from("HGX_BMC_0"),
            device_info_timeout: COMPUTE_DEVICE_INFO_FETCH_TIMEOUT,
            redfish_connect_timeout: std::time::Duration::from_secs(10),
            redfish_request_timeout: std::time::Duration::from_secs(30),
            redfish_upload_timeout: std::time::Duration::from_secs(600),
            power_paths: crate::domain::compute_power::ComputePowerPaths::default(),
            firmware_timing: crate::domain::compute_timing::ComputeFirmwareTiming::default(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn get_mnnvlink_topology_waits_on_op_lock_without_timing_out() {
        let compute = NvidiaGb200Compute::new(
            "c-01".into(),
            "rack-01".into(),
            "10.0.0.1".into(),
            443,
            "admin",
            "pass",
            "aa:bb:cc:dd:ee:ff".into(),
            vec![],
            vec![],
            false,
        )
        .unwrap();

        // Simulate a concurrent long-running operation holding the node op-lock
        // (e.g. a firmware upload).
        let guard = compute.op_lock.lock().await;

        let read = compute.get_mnnvlink_topology();
        tokio::pin!(read);

        // Advance virtual time well past the I/O backstop. The read must still
        // be parked on the op-lock rather than timed out: the deadline covers
        // only the client I/O, so ordinary lock contention queues instead of
        // failing an otherwise healthy read.
        tokio::time::advance(crate::domain::node::COMPUTE_DEVICE_INFO_FETCH_TIMEOUT * 2).await;
        assert!(
            futures::poll!(read.as_mut()).is_pending(),
            "op-lock wait must not be charged against the device-info deadline"
        );

        drop(guard);
    }

    #[test]
    fn from_config_binds_the_recovery_capability_and_rejects_a_bad_profile() {
        let mut config = NodeConfig {
            runtime_config: None,
            id: "c-01".into(),
            node_type: NodeType::ComputeGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.1".into(),
                    mac_address: "aa:bb:cc:dd:ee:ff".into(),
                    port: 443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "pass")),
                false,
            )),
            host_endpoint: None,
            expected_inventory: None,
            recovery: Some(recovery_profile(gb200_roster_table())),
        };

        let compute = NvidiaGb200Compute::from_config(&config, "rack-01").unwrap();
        let recovery = compute
            .recovery()
            .expect("a bound profile arms the recovery capability");
        // Both halves of the signature on a watched member count, one half
        // does not, and a presence member is never watched.
        let inventory = [
            hmc_entry("HGX_FW_FPGA_0", "Critical", "StandbyOffline"),
            hmc_entry("HGX_FW_FPGA_1", "Critical", "Enabled"),
            hmc_entry("HGX_FW_ERoT_CPU_0", "OK", "StandbyOffline"),
            hmc_entry("HGX_FW_GPU_0", "OK", "Enabled"),
            hmc_entry("HGX_FW_BMC_0", "Critical", "StandbyOffline"),
        ];
        assert_eq!(recovery.detect(&inventory), ["HGX_FW_FPGA_0"]);

        config.recovery = None;
        let compute = NvidiaGb200Compute::from_config(&config, "rack-01").unwrap();
        assert!(
            compute.recovery().is_none(),
            "no profile leaves detection off"
        );

        config.recovery = Some(recovery_profile(serde_json::json!({
            "presence": ["HGX_FW_BMC_0"],
            "push_then_cycle": [],
            "push_only": ["HGX_FW_GPU_0"],
        })));
        let error = NvidiaGb200Compute::from_config(&config, "rack-01")
            .err()
            .expect("an empty bundle list must fail construction");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn classify_firmware_types() {
        assert_eq!(classify_firmware_type("BMC_FW"), FirmwareType::BMC);
        assert_eq!(classify_firmware_type("BIOS_Update"), FirmwareType::BIOS);
        assert_eq!(classify_firmware_type("uefi_fw"), FirmwareType::BIOS);
        assert_eq!(classify_firmware_type("CPLD_v1"), FirmwareType::CPLD);
        assert_eq!(classify_firmware_type("FPGA_image"), FirmwareType::FPGA);
        assert_eq!(classify_firmware_type("NIC_firmware"), FirmwareType::NIC);
        assert_eq!(classify_firmware_type("HBA_driver"), FirmwareType::HBA);
        assert_eq!(classify_firmware_type("HMC_update"), FirmwareType::HMC);
        assert_eq!(classify_firmware_type("HGX_BMC"), FirmwareType::BMC);
        assert_eq!(classify_firmware_type("HGX_Manager"), FirmwareType::HMC);
        assert_eq!(
            classify_firmware_type("unknown_thing"),
            FirmwareType::Unknown
        );
    }

    #[test]
    fn get_info_includes_host_addresses() {
        let compute = NvidiaGb200Compute::new(
            "c-01".into(),
            "rack-01".into(),
            "10.0.0.1".into(),
            443,
            "admin",
            "pass",
            "aa:bb:cc:dd:ee:ff".into(),
            vec!["11:22:33:44:55:66".into()],
            vec!["192.168.1.1".into()],
            false,
        )
        .unwrap();

        let info = compute.get_info();
        assert_eq!(info["type"], "compute_gb200_nvidia");
        assert_eq!(info["host"], "10.0.0.1");
        assert_eq!(info["hostMac_0"], "11:22:33:44:55:66");
        assert_eq!(info["hostIp_0"], "192.168.1.1");
    }

    #[test]
    fn from_config_omits_empty_host_mac() {
        let config = NodeConfig {
            id: "c-01".into(),
            node_type: NodeType::ComputeGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.1".into(),
                    mac_address: "aa:bb:cc:dd:ee:ff".into(),
                    port: 443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "pass")),
                false,
            )),
            host_endpoint: Some(EndpointConfig::new(Endpoint {
                ip_address: "192.168.1.1".into(),
                mac_address: String::new(),
                port: 443,
                host_name: None,
            })),
            expected_inventory: None,
            recovery: None,
            runtime_config: None,
        };

        let result = NvidiaGb200Compute::from_config(&config, "rack-01");
        let Ok(compute) = result else {
            panic!("expected compute config to be accepted");
        };

        let info = compute.get_info();

        assert!(!info.contains_key("hostMac_0"));
        assert_eq!(info["hostIp_0"], "192.168.1.1");
    }

    #[test]
    fn nvfwupd_target_config_uses_stored_compute_credentials() {
        let compute = NvidiaGb200Compute::new(
            "c-01".into(),
            "rack-01".into(),
            "10.0.0.1".into(),
            8443,
            "admin",
            "secret",
            String::new(),
            Vec::new(),
            Vec::new(),
            true,
        )
        .unwrap();

        assert!(!compute.has_hardware_profile);
        let cfg = compute.nvfwupd_target_config();
        assert_eq!(cfg.ip, "10.0.0.1");
        assert_eq!(cfg.port, Some(8443));
        assert_eq!(cfg.username, "admin");
        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.server_type, nvfwupd::workflow::ServerType::GB200);
        assert!(!cfg.verify_tls);
        assert!(!format!("{cfg:?}").contains("secret"));
    }

    #[test]
    fn flint_target_config_uses_compute_host_and_requires_bmc_credentials() {
        let mut config = NodeConfig {
            id: "c-01".into(),
            node_type: NodeType::ComputeGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.1".into(),
                    mac_address: "aa:bb:cc:dd:ee:ff".into(),
                    port: 443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("bmc-admin", "bmc-pass")),
                false,
            )),
            host_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "192.168.1.1".into(),
                    mac_address: String::new(),
                    port: 22,
                    host_name: None,
                },
                Some(EndpointCredentials::new("host-admin", "host-pass")),
                false,
            )),
            expected_inventory: None,
            recovery: None,
            runtime_config: None,
        };
        let compute = NvidiaGb200Compute::from_config(&config, "rack-01").unwrap();

        assert!(!compute.has_hardware_profile);
        // Figment configuration tests clear the process environment in parallel.
        let target = compute
            .flint_host_target_config_with_home(Some(PathBuf::from("/test-home")))
            .unwrap();

        assert_eq!(target.ip, "192.168.1.1");
        assert_eq!(target.port, Some(22));
        assert_eq!(target.username, "host-admin");
        assert_eq!(target.password, "host-pass");
        assert_eq!(
            target.ssh_known_hosts.as_deref(),
            Some("/test-home/.ssh/known_hosts")
        );
        assert_eq!(
            target.ssh_host_key_mode,
            nvfwupd::workflow::SshHostKeyMode::TrustOnFirstUse
        );

        config.bmc_endpoint.as_mut().unwrap().credentials = None;
        let error = NvidiaGb200Compute::from_config(&config, "rack-01")
            .err()
            .expect("BMC credentials are required before node construction");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
        assert!(error.message.contains("BMC credentials"));
    }

    #[tokio::test]
    async fn profile_power_paths_control_direct_host_bmc_and_hmc_operations() {
        use crate::config::hardware_profiles::HardwareUpdatePlan;
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut yaml: noyalib::compat::serde_yaml::Value = noyalib::compat::serde_yaml::from_str(
            include_str!("../../../../../hardware-profiles/v1/compute_gb200_nvidia.yaml"),
        )
        .unwrap();
        for name in [
            "system_state_path",
            "system_reset_path",
            "bmc_reset_path",
            "hmc_reset_path",
        ] {
            yaml["driver"]["redfish"]["power"][name] =
                noyalib::compat::serde_yaml::Value::from(format!("/redfish/v1/Oem/{name}"));
        }
        let plan = HardwareUpdatePlan::from_yaml_str(
            &noyalib::compat::serde_yaml::to_string(&yaml).unwrap(),
        )
        .unwrap();
        let NodeRuntimeConfig::RedfishNvfwupdCompute(runtime) = plan.runtime_config();
        let mut compute = NvidiaGb200Compute::new(
            "test".into(),
            "rack".into(),
            "127.0.0.1".into(),
            server.address().port(),
            "admin",
            "secret",
            String::new(),
            Vec::new(),
            Vec::new(),
            true,
        )
        .unwrap();
        compute.runtime_config = runtime.clone();
        compute.redfish = RedfishClient::new(
            "127.0.0.1",
            server.address().port(),
            redfish_client::BmcCredentials::username_password(
                "admin".into(),
                Some("secret".into()),
            ),
            true,
            false,
        )
        .unwrap();
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Oem/system_state_path"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "@odata.id": "/redfish/v1/Oem/system_state_path", "Id": "CustomSystem",
                "Name": "Custom host", "PowerState": "Off"
            })))
            .expect(1)
            .mount(&server)
            .await;
        for (target, op, name, reset_type) in [
            (
                PowerTargetType::System,
                PowerOp::On,
                "system_reset_path",
                "On",
            ),
            (
                PowerTargetType::System,
                PowerOp::ForceOff,
                "system_reset_path",
                "ForceOff",
            ),
            (
                PowerTargetType::BMC,
                PowerOp::ForceRestart,
                "bmc_reset_path",
                "ForceRestart",
            ),
            (
                PowerTargetType::HMC,
                PowerOp::ForceRestart,
                "hmc_reset_path",
                "ForceRestart",
            ),
        ] {
            Mock::given(method("POST"))
                .and(path(format!("/redfish/v1/Oem/{name}")))
                .and(body_json(serde_json::json!({"ResetType": reset_type})))
                .respond_with(ResponseTemplate::new(204))
                .expect(1)
                .mount(&server)
                .await;
            compute.set_power_state(op, target).await.unwrap();
        }
        assert_eq!(compute.get_power_state().await.unwrap(), PowerState::Off);
        assert_eq!(server.received_requests().await.unwrap().len(), 5);
    }

    #[test]
    fn hardware_profile_parameters_layer_onto_concrete_implementation() {
        let mut runtime_config = test_runtime_config();
        runtime_config.nvfwupd_server_profile = NvfwupdServerProfile::ComputeGb300;
        runtime_config.computer_system_id = std::sync::Arc::from("ConfiguredSystem");
        runtime_config.bmc_manager_id = std::sync::Arc::from("ConfiguredBmc");
        runtime_config.hmc_manager_id = std::sync::Arc::from("ConfiguredHmc");
        runtime_config.device_info_timeout = std::time::Duration::from_secs(42);
        let config = NodeConfig {
            recovery: None,
            id: "c-01".into(),
            node_type: NodeType::ComputeGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.1".into(),
                    mac_address: "aa:bb:cc:dd:ee:ff".into(),
                    port: 8443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "secret")),
                true,
            )),
            host_endpoint: None,
            expected_inventory: None,
            runtime_config: Some(NodeRuntimeConfig::RedfishNvfwupdCompute(runtime_config)),
        };

        let compute = NvidiaGb200Compute::from_config(&config, "rack-01").unwrap();

        assert!(compute.has_hardware_profile);
        assert_eq!(compute.node_type(), NodeType::ComputeGb200Nvidia);
        assert_eq!(
            compute.runtime_config.computer_system_id.as_ref(),
            "ConfiguredSystem"
        );
        assert_eq!(
            compute.runtime_config.bmc_manager_id.as_ref(),
            "ConfiguredBmc"
        );
        assert_eq!(
            compute.runtime_config.hmc_manager_id.as_ref(),
            "ConfiguredHmc"
        );
        assert_eq!(
            compute.runtime_config.device_info_timeout,
            std::time::Duration::from_secs(42)
        );
        assert_eq!(
            compute.nvfwupd_target_config().server_type,
            nvfwupd::workflow::ServerType::GB300
        );
    }

    #[tokio::test]
    async fn recovery_power_block_honors_profile_reset_path_and_retry_timing() {
        let mockup = redfish_test_support::RedfishSimulator::builder()
            .add_gb200_compute(0)
            .start()
            .await;
        let mut runtime = default_compute_config(NodeType::ComputeGb200Nvidia);
        // This action is deliberately unavailable. Recovery must fail here,
        // rather than use the simulator's working default reset action.
        runtime.power_paths.system_reset_path =
            Some("/redfish/v1/Oem/UnavailableReset".parse().unwrap());
        runtime.firmware_timing.activation_command_retries = 0;
        runtime.firmware_timing.recovery_probe_timeout = std::time::Duration::from_secs(1);
        let config = NodeConfig {
            id: "recovery-node".into(),
            node_type: NodeType::ComputeGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "127.0.0.1".into(),
                    mac_address: String::new(),
                    port: mockup.ports()[0],
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "password")),
                true,
            )),
            host_endpoint: None,
            expected_inventory: None,
            recovery: Some(recovery_profile(gb200_roster_table())),
            runtime_config: Some(NodeRuntimeConfig::RedfishNvfwupdCompute(runtime)),
        };
        let compute = NvidiaGb200Compute::from_config(&config, "rack").unwrap();
        let initial_power = compute.get_power_state().await.unwrap();
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            RecoveryDevice::power_block(&compute, &["HGX_FW_BMC_0".to_owned()]),
        )
        .await
        .expect("recovery must honor the profile's zero command retries")
        .unwrap_err();
        assert!(
            error
                .message
                .contains("activation command RF_PWR_OFF failed")
        );
        assert_eq!(compute.get_power_state().await.unwrap(), initial_power);
        mockup.stop();
    }

    /// A GB200 compute node on the simulator's BMC at `port`, no recovery profile.
    fn simulated_compute(port: u16) -> NvidiaGb200Compute {
        let config = NodeConfig {
            id: "recovery-node".into(),
            node_type: NodeType::ComputeGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "127.0.0.1".into(),
                    mac_address: String::new(),
                    port,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "password")),
                true,
            )),
            host_endpoint: None,
            expected_inventory: None,
            recovery: None,
            runtime_config: None,
        };
        NvidiaGb200Compute::from_config(&config, "rack").unwrap()
    }

    #[tokio::test]
    async fn cancelled_job_outranks_a_failed_recovery_push_task() {
        // The BMC fails the push task. Read uncancelled, the wait reports that
        // failure. Read with the job cancelled, the cancel is why the job stops.
        let mockup = redfish_test_support::RedfishSimulator::builder()
            .add_gb200_compute(0)
            .with_task_delay(0.01)
            .always_fail_firmware_updates()
            .start()
            .await;
        let compute = simulated_compute(mockup.ports()[0]);
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("recovery.fwpkg");
        std::fs::write(&file, b"recovery").unwrap();
        let target = FirmwareTarget {
            component: RECOVERY_PUSH_TARGET.to_owned(),
            firmware_file: file.to_string_lossy().into_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        };
        let request =
            nvfwupd_adapter::to_update_request(&target, true, FirmwareUpdateOptions::default());
        let FirmwareUpdateOutcome::Started(handle) = compute
            .start_update(request, UpdateGates::Skipped)
            .await
            .unwrap()
        else {
            panic!("the simulator starts a task for every push");
        };
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let failed = compute
            .wait_for_recovery_task(&handle.task_id, &CancellationToken::new())
            .await
            .unwrap_err();
        assert_ne!(
            failed.code,
            ErrorCode::Cancelled,
            "the fixture fails the task"
        );

        let cancel = CancellationToken::new();
        cancel.cancel();
        let error = compute
            .wait_for_recovery_task(&handle.task_id, &cancel)
            .await
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Cancelled);
        mockup.stop();
    }

    #[tokio::test]
    async fn cancelled_job_ends_the_push_task_wait_after_the_current_poll() {
        // The simulator knows no such task, so every status read fails and
        // the wait would otherwise poll until RECOVERY_PUSH_TASK_TIMEOUT.
        let mockup = redfish_test_support::RedfishSimulator::builder()
            .add_gb200_compute(0)
            .start()
            .await;
        let compute = simulated_compute(mockup.ports()[0]);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            compute.wait_for_recovery_task("no-such-task", &cancel),
        )
        .await
        .expect("a cancelled job must not wait for the task deadline")
        .unwrap_err();

        assert_eq!(error.code, ErrorCode::Cancelled);
        mockup.stop();
    }

    #[tokio::test]
    async fn expected_inventory_mismatch_prevents_firmware_package_processing() {
        let mockup = redfish_test_support::RedfishSimulator::builder()
            .add_gb200_compute(0)
            .start()
            .await;
        let port = mockup.ports()[0];
        let config = NodeConfig {
            id: "c-01".to_owned(),
            node_type: NodeType::ComputeGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "127.0.0.1".to_owned(),
                    mac_address: String::new(),
                    port,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "password")),
                true,
            )),
            host_endpoint: None,
            expected_inventory: Some(ExpectedInventoryPolicy {
                profile: std::sync::Arc::from("profile-a"),
                ap_names: std::sync::Arc::from([String::from("MISSING_AP")]),
                flint_device_counts: std::sync::Arc::default(),
            }),
            recovery: None,
            runtime_config: None,
        };
        let compute = NvidiaGb200Compute::from_config(&config, "rack-01").unwrap();
        let target = FirmwareTarget {
            component: "BMC".to_owned(),
            // This file deliberately does not exist. A missing-inventory
            // failure proves NVFWUPD rejected the update before package work.
            firmware_file: "/does/not/exist.fwpkg".to_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        };

        let error = compute
            .update_firmware(&target, false, FirmwareUpdateOptions::default())
            .await
            .unwrap_err();

        mockup.stop();
        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert!(error.message.contains("MISSING_AP"));
        assert!(!error.message.contains("package parse"));
    }
}
