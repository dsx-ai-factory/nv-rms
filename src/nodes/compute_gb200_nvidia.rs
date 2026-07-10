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
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

use crate::domain::node::*;
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{Result, RmsError};
/// Concrete node for NVIDIA reference GB200 compute BMCs (Redfish).
///
/// Power:     GET/POST  /redfish/v1/Systems/System_0  (System target)
///                      /redfish/v1/Managers/BMC_0     (BMC target)
///                      /redfish/v1/Managers/HGX_BMC_0 (HMC target)
/// Firmware:  multipart POST to /UpdateService/update-multipart
/// Activate:  AuxPowerCycle via /Chassis/BMC_0/Actions/Oem/NvidiaChassis.AuxPowerReset
pub struct NvidiaGb200Compute {
    id: String,
    rack_id: String,
    bmc_endpoint: EndpointConfig,
    host_mac_addresses: Vec<String>,
    host_ip_addresses: Vec<String>,
    http: tokio::sync::Mutex<HttpClient>,
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
        let http = HttpClient::new(
            &host,
            port,
            username,
            password,
            dangerously_accept_invalid_certs,
            true,
        )?;
        Ok(Self {
            id,
            rack_id,
            bmc_endpoint: EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: host,
                    mac_address,
                    port,
                    host_name: None,
                },
                Some(EndpointCredentials::new(username, password)),
                dangerously_accept_invalid_certs,
            ),
            host_mac_addresses,
            host_ip_addresses,
            http: tokio::sync::Mutex::new(http),
        })
    }

    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
            return Err(RmsError::invalid_argument(
                "bmc_endpoint is required for compute nodes",
            ));
        };
        let credentials = bmc_endpoint.credentials.as_ref();
        let username = credentials
            .map(|credentials| credentials.username.as_str())
            .unwrap_or_default();
        let password = credentials
            .map(|credentials| credentials.password.expose_secret())
            .unwrap_or_default();

        let http = HttpClient::new(
            &bmc_endpoint.endpoint.ip_address,
            bmc_endpoint.endpoint.port,
            username,
            password,
            bmc_endpoint.dangerously_accept_invalid_certs,
            true,
        )?;
        Ok(Self {
            id: config.id.clone(),
            rack_id: rack_id.to_owned(),
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
            http: tokio::sync::Mutex::new(http),
        })
    }

    // ── MNNVLink topology (tray device info from BMC) ──

    // Returns normalized `{chassis_sn, slot_number, tray_index}` extracted from
    // `.Oem.Nvidia.MNNVLinkTopology` on the HGX baseboard GPU_0 Redfish resource.
    pub async fn get_mnnvlink_topology(&self) -> Result<Value> {
        let http = self.http.lock().await;
        tracing::info!(node = %self.id, "get_mnnvlink_topology");

        let resp = http
            .get(
                "/redfish/v1/Systems/HGX_Baseboard_0/Processors/GPU_0",
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;

        let topo = resp
            .get("Oem")
            .and_then(|v| v.get("Nvidia"))
            .and_then(|v| v.get("MNNVLinkTopology"))
            .ok_or_else(|| {
                RmsError::not_found(format!(
                    "MNNVLinkTopology not found in GPU_0 response for {}",
                    self.id
                ))
            })?;

        let chassis_sn = topo
            .get("ChassisSerialNumber")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                RmsError::invalid_argument(format!(
                    "MNNVLinkTopology missing or invalid ChassisSerialNumber for {}",
                    self.id
                ))
            })?
            .to_owned();

        let slot_number = topo
            .get("TraySlotNumber")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| {
                RmsError::invalid_argument(format!(
                    "MNNVLinkTopology missing or invalid TraySlotNumber for {}",
                    self.id
                ))
            })?;

        let tray_index = topo
            .get("TraySlotIndex")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| {
                RmsError::invalid_argument(format!(
                    "MNNVLinkTopology missing or invalid TraySlotIndex for {}",
                    self.id
                ))
            })?;

        Ok(serde_json::json!({
            "chassis_sn": chassis_sn,
            "slot_number": slot_number,
            "tray_index": tray_index,
        }))
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

    async fn get_power_state(&self) -> Result<PowerState> {
        let http = self.http.lock().await;
        tracing::debug!(node = %self.id, "get_power_state");
        let resp = http
            .get("/redfish/v1/Systems/System_0", HttpClient::DEFAULT_TIMEOUT)
            .await?;
        Ok(parse_power_state(&resp))
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        let http = self.http.lock().await;
        let endpoint = match target {
            PowerTargetType::System => "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
            PowerTargetType::BMC => "/redfish/v1/Managers/BMC_0/Actions/Manager.Reset",
            PowerTargetType::HMC => "/redfish/v1/Managers/HGX_BMC_0/Actions/Manager.Reset",
        };

        tracing::info!(node = %self.id, ?op, endpoint, "set_power_state");
        let payload = serde_json::json!({"ResetType": redfish_reset_type(op)});
        http.post(endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
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
        tracing::info!(
            node = %self.id,
            component = %target.component,
            file = %target.firmware_file,
            "update_firmware via NVFWUPD"
        );
        let request = nvfwupd_adapter::to_update_request(target, force_update, options);
        let outcome = nvfwupd::workflow_api::update_firmware(self.nvfwupd_target_config(), request)
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
        let http = self.http.lock().await;
        tracing::info!(
            node = %self.id,
            component = %target.component,
            file = %target.firmware_file,
            "start_firmware_upload"
        );

        let params = build_firmware_upload_params(target, force_update);

        tracing::info!(node = %self.id, "uploading firmware");
        let resp = http
            .post_multipart(
                "/redfish/v1/UpdateService/update-multipart",
                &target.firmware_file,
                &params,
                HttpClient::UPLOAD_TIMEOUT,
            )
            .await?;

        let task_id = extract_task_id(&resp);
        if task_id.is_empty() {
            return Err(RmsError::internal("no task ID in firmware upload response"));
        }

        tracing::info!(node = %self.id, task_id, "firmware upload task created");
        Ok(task_id)
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
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

fn redfish_reset_type(op: PowerOp) -> &'static str {
    match op {
        PowerOp::On => "On",
        PowerOp::Off => "GracefulShutdown",
        PowerOp::ForceOn => "ForceOn",
        PowerOp::ForceOff => "ForceOff",
        PowerOp::PowerCycle => "PowerCycle",
        PowerOp::GracefulShutdown => "GracefulShutdown",
        PowerOp::GracefulRestart => "GracefulRestart",
        PowerOp::ForceRestart => "ForceRestart",
        PowerOp::Nmi => "Nmi",
    }
}

fn extract_task_id(response: &Value) -> String {
    let uri = response
        .get("@odata.id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    uri.rsplit_once('/')
        .map(|(_, task_id)| task_id)
        .filter(|task_id| !task_id.is_empty())
        .unwrap_or(uri)
        .to_owned()
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

fn parse_power_state(response: &Value) -> PowerState {
    match response
        .get("PowerState")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
    {
        "On" => PowerState::On,
        "Off" => PowerState::Off,
        _ => PowerState::Unknown,
    }
}

// Build the Redfish UpdateService-multipart JSON parameter payload for a
// firmware upload. Exposed so the ForceUpdate plumbing can be unit-tested
// without a running BMC.
fn build_firmware_upload_params(target: &FirmwareTarget, force_update: bool) -> Value {
    serde_json::json!({
        "ForceUpdate": force_update,
        "Targets": if target.component.is_empty() {
            Vec::<String>::new()
        } else {
            vec![target.component.clone()]
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn firmware_upload_params_honor_force_update_flag() {
        let target = FirmwareTarget {
            component: "HGX_FW_BMC_0".into(),
            firmware_file: "/tmp/x.fwpkg".into(),
        };

        let forced = build_firmware_upload_params(&target, true);
        assert_eq!(forced["ForceUpdate"], true);
        assert_eq!(forced["Targets"], serde_json::json!(["HGX_FW_BMC_0"]));

        let not_forced = build_firmware_upload_params(&target, false);
        assert_eq!(not_forced["ForceUpdate"], false);
    }

    #[test]
    fn firmware_upload_params_omit_targets_when_component_empty() {
        let target = FirmwareTarget {
            component: String::new(),
            firmware_file: "/tmp/x.fwpkg".into(),
        };
        let params = build_firmware_upload_params(&target, true);
        assert_eq!(params["ForceUpdate"], true);
        assert_eq!(params["Targets"], serde_json::json!([]));
    }

    #[test]
    fn redfish_reset_type_mapping() {
        assert_eq!(redfish_reset_type(PowerOp::On), "On");
        assert_eq!(redfish_reset_type(PowerOp::Off), "GracefulShutdown");
        assert_eq!(redfish_reset_type(PowerOp::ForceOff), "ForceOff");
        assert_eq!(redfish_reset_type(PowerOp::PowerCycle), "PowerCycle");
        assert_eq!(redfish_reset_type(PowerOp::Nmi), "Nmi");
    }

    #[test]
    fn extract_task_id_from_odata() {
        let resp = serde_json::json!({"@odata.id": "/redfish/v1/TaskService/Tasks/42"});
        assert_eq!(extract_task_id(&resp), "42");
    }

    #[test]
    fn extract_task_id_missing() {
        let resp = serde_json::json!({});
        assert_eq!(extract_task_id(&resp), "");
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
    fn parse_power_states() {
        assert_eq!(
            parse_power_state(&serde_json::json!({"PowerState": "On"})),
            PowerState::On
        );
        assert_eq!(
            parse_power_state(&serde_json::json!({"PowerState": "Off"})),
            PowerState::Off
        );
        assert_eq!(
            parse_power_state(&serde_json::json!({"PowerState": "Pausing"})),
            PowerState::Unknown
        );
        assert_eq!(
            parse_power_state(&serde_json::json!({})),
            PowerState::Unknown
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
}
