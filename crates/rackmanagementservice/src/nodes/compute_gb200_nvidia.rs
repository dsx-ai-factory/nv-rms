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

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};

use crate::domain::node::*;
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::transport::redfish_client::{
    NvidiaMnnvlinkTopology, RedfishClient, ResetType as RedfishResetType,
    redfish_client_from_endpoint_config,
};
use crate::utilities::error::{Result, RmsError};

/// Concrete node for NVIDIA reference GB200 compute BMCs (Redfish).
///
/// Power:     nv-redfish typed ComputerSystem/Manager reads and reset actions
/// Firmware:  nv-redfish UpdateService multipart update
/// Activate:  NVFWUPD workflow activation
pub struct NvidiaGb200Compute {
    id: String,
    rack_id: String,
    bmc_endpoint: EndpointConfig,
    host_endpoint: Option<EndpointConfig>,
    host_mac_addresses: Vec<String>,
    host_ip_addresses: Vec<String>,
    expected_inventory: Option<ExpectedInventoryPolicy>,
    redfish: RedfishClient,

    // Serializes full node operations across shared clients. GB300 wrappers
    // use this same lock for direct work and delegate without reacquiring it.
    pub(super) op_lock: tokio::sync::Mutex<()>,
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
            redfish: redfish_client_from_endpoint_config(&bmc_endpoint)?,
            bmc_endpoint,
            host_endpoint: None,
            host_mac_addresses,
            host_ip_addresses,
            expected_inventory: None,
            op_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
            return Err(RmsError::invalid_argument(
                "bmc_endpoint is required for compute nodes",
            ));
        };

        Ok(Self {
            id: config.id.clone(),
            rack_id: rack_id.to_owned(),
            redfish: redfish_client_from_endpoint_config(bmc_endpoint)?,
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
        let timeout = crate::domain::node::COMPUTE_DEVICE_INFO_FETCH_TIMEOUT;
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

    // ── Private helpers ──

    pub(crate) fn nvfwupd_target_config_for(
        &self,
        node_type: NodeType,
    ) -> nvfwupd::workflow::TargetConfig {
        let credentials = self.bmc_endpoint.credentials.as_ref();
        let username = credentials
            .map(|credentials| credentials.username.as_str())
            .unwrap_or_default();
        let default_password = SecretString::default();
        let password = credentials
            .map(|credentials| &credentials.password)
            .unwrap_or(&default_password);

        nvfwupd_adapter::to_target_config_with_secret(
            node_type,
            &self.bmc_endpoint.endpoint.ip_address,
            self.bmc_endpoint.endpoint.port,
            username,
            password,
            !self.bmc_endpoint.dangerously_accept_invalid_certs,
        )
    }

    fn nvfwupd_target_config(&self) -> nvfwupd::workflow::TargetConfig {
        self.nvfwupd_target_config_for(NodeType::ComputeGb200Nvidia)
    }

    fn flint_host_target_config(&self) -> Result<nvfwupd::workflow::HostTargetConfig> {
        let bmc_credentials = self.bmc_endpoint.credentials.as_ref().ok_or_else(|| {
            RmsError::failed_precondition(
                "BMC username/password credentials are required to activate GB200 in-band firmware updates",
            )
        })?;
        if bmc_credentials.username.trim().is_empty()
            || bmc_credentials.password.expose_secret().is_empty()
        {
            return Err(RmsError::failed_precondition(
                "BMC username/password credentials are required to activate GB200 in-band firmware updates",
            ));
        }
        let host = self.host_endpoint.as_ref().ok_or_else(|| {
            RmsError::failed_precondition(
                "host_endpoint is required for GB200 in-band firmware updates",
            )
        })?;
        let credentials = host.credentials.as_ref().ok_or_else(|| {
            RmsError::failed_precondition(
                "host_endpoint username/password credentials are required for GB200 in-band firmware updates",
            )
        })?;
        if credentials.username.trim().is_empty() || credentials.password.expose_secret().is_empty()
        {
            return Err(RmsError::failed_precondition(
                "host_endpoint username/password credentials are required for GB200 in-band firmware updates",
            ));
        }
        let ssh_known_hosts = std::env::var_os("HOME")
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "SSH host-key verification requires HOME or an explicit known_hosts path",
                )
            })
            .map(PathBuf::from)?
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
            username: credentials.username.clone(),
            password: credentials.password.expose_secret().to_owned(),
            port: Some(host.endpoint.port),
            ssh_known_hosts: Some(ssh_known_hosts),
            // Match the NVFWUPD CLI default: learn the first key in the RMS
            // process user's known_hosts file, then reject changed keys.
            ssh_host_key_mode: nvfwupd::workflow::SshHostKeyMode::TrustOnFirstUse,
        })
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
        NodeType::ComputeGb200Nvidia
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = HashMap::from([
            (
                "type".to_owned(),
                NodeType::ComputeGb200Nvidia.as_str().to_owned(),
            ),
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

    async fn get_power_state(&self) -> Result<PowerState> {
        let _op_guard = self.op_lock.lock().await;
        tracing::debug!(node = %self.id, "get_power_state");

        Ok(self
            .redfish
            .computer_system_power_state("System_0")
            .await?
            .into())
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;

        let manager_id = match target {
            PowerTargetType::System => {
                tracing::info!(node = %self.id, ?op, "set_power_state via nv-redfish ComputerSystem.Reset");

                self.redfish
                    .reset_computer_system("System_0", nv_redfish_reset_type(op))
                    .await?;

                return Ok(());
            }
            PowerTargetType::BMC => "BMC_0",
            PowerTargetType::HMC => "HGX_BMC_0",
        };

        tracing::info!(node = %self.id, ?op, manager_id, "set_power_state via nv-redfish Manager.Reset");
        self.redfish
            .reset_manager(manager_id, nv_redfish_reset_type(op))
            .await?;

        Ok(())
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let _op_guard = self.op_lock.lock().await;
        tracing::debug!(node = %self.id, "get_firmware_inventory");
        let inventory = nvfwupd::workflow_api::get_firmware_inventory(self.nvfwupd_target_config())
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
        let summary = nvfwupd::workflow_api::verify_firmware_target_versions(
            self.nvfwupd_target_config(),
            targets,
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
        if flint_device_family(&target.component).is_some() {
            return Err(RmsError::failed_precondition(
                "CX7, CX8, and BF3_NIC targets must use the grouped GB200 in-band firmware workflow",
            ));
        }
        let _op_guard = self.op_lock.lock().await;

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
        let outcome = nvfwupd::workflow_api::update_firmware_with_expected_inventory(
            self.nvfwupd_target_config(),
            request,
            expected_inventory,
        )
        .await
        .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_outcome(outcome))
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
        let boot_summary = nvfwupd::workflow_api::ensure_gb200_host_os_ready(
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
        let outcome = nvfwupd::workflow_api::update_gb200_flint_firmware(
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
        let _op_guard = self.op_lock.lock().await;

        tracing::info!(
            node = %self.id,
            component = %target.component,
            file = %target.firmware_file,
            "start_firmware_upload"
        );

        let targets = if target.component.is_empty() {
            Vec::new()
        } else {
            vec![target.component.clone()]
        };

        tracing::info!(node = %self.id, "uploading firmware");
        let task_id = self
            .redfish
            .multipart_update_firmware_from_path(&target.firmware_file, targets, force_update)
            .await?;

        tracing::info!(node = %self.id, task_id, "firmware upload task created");
        Ok(task_id)
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let _op_guard = self.op_lock.lock().await;
        tracing::debug!(node = %self.id, task_id, "poll_firmware_task via NVFWUPD");
        let status = nvfwupd::workflow_api::get_task_status(self.nvfwupd_target_config(), task_id)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::to_task_status(status))
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
        let mut summary =
            nvfwupd::workflow_api::activate_firmware(self.nvfwupd_target_config(), request)
                .await
                .map_err(nvfwupd_adapter::map_error)?;

        if wait_for_host_os {
            let host_summary = nvfwupd::workflow_api::ensure_gb200_host_os_ready(
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

pub(crate) fn flint_device_family(component: &str) -> Option<nvfwupd::workflow::FlintDeviceFamily> {
    nvfwupd::workflow::FlintDeviceFamily::from_component(component)
}

fn flint_targets(
    targets: &[FirmwareTarget],
) -> Result<Vec<nvfwupd::workflow::FlintFirmwareTarget>> {
    let mut grouped = std::collections::BTreeMap::new();
    for target in targets {
        let Some(family) = flint_device_family(&target.component) else {
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
    use super::*;
    use crate::utilities::error::ErrorCode;

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
        };
        let compute = NvidiaGb200Compute::from_config(&config, "rack-01").unwrap();

        let target = compute.flint_host_target_config().unwrap();

        assert_eq!(target.ip, "192.168.1.1");
        assert_eq!(target.port, Some(22));
        assert_eq!(target.username, "host-admin");
        assert_eq!(target.password, "host-pass");
        assert!(
            target
                .ssh_known_hosts
                .as_deref()
                .is_some_and(|path| path.ends_with("/.ssh/known_hosts"))
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
        };
        let compute = NvidiaGb200Compute::from_config(&config, "rack-01").unwrap();
        let target = FirmwareTarget {
            component: "BMC".to_owned(),
            // This file deliberately does not exist. A missing-inventory
            // failure proves NVFWUPD rejected the update before package work.
            firmware_file: "/does/not/exist.fwpkg".to_owned(),
            expected_version: None,
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
