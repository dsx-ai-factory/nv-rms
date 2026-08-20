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

use std::collections::HashMap;

use async_trait::async_trait;
use secrecy::SecretString;

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

    // ── MNNVLink topology (tray device info from BMC) ──

    // Returns normalized `{chassis_sn, slot_number, tray_index}` extracted from
    // `.Oem.Nvidia.MNNVLinkTopology` on a ComputerSystem Processor resource.
    pub async fn get_mnnvlink_topology(&self) -> Result<NvidiaMnnvlinkTopology> {
        let _op_guard = self.op_lock.lock().await;
        tracing::info!(node = %self.id, "get_mnnvlink_topology");

        self.redfish
            .nvidia_mnnvlink_topology()
            .await
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
        let request = nvfwupd_adapter::to_activation_request(request);
        let summary =
            nvfwupd::workflow_api::activate_firmware(self.nvfwupd_target_config(), request)
                .await
                .map_err(nvfwupd_adapter::map_error)?;
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
