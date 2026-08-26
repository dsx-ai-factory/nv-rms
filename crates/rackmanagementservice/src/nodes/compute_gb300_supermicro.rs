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
use std::path::Path;

use async_trait::async_trait;
use serde_json::json;

use crate::domain::node::*;
use crate::domain::rack::NodeConfig;
use crate::nodes::compute_gb300_nvidia::NvidiaGb300Compute;
use crate::transport::redfish_client::NvidiaMnnvlinkTopology;
use crate::utilities::error::{Result, RmsError};

pub(crate) const SUPERMICRO_HGX_TARGET: &str = "/redfish/v1/Chassis/HGX_Chassis_0";
pub(crate) const SUPERMICRO_BIOS_TARGET: &str = "supermicro_bios";
pub(crate) const SUPERMICRO_CPU0_TARGET: &str =
    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0";
pub(crate) const SUPERMICRO_CPU1_TARGET: &str =
    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1";
pub(crate) const SUPERMICRO_BMC_TARGET: &str =
    "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0";

/// Supermicro GB300 compute node.
///
/// Power and Redfish behavior is shared with NVIDIA GB300. The wrapper keeps
/// the descriptor-only identity distinct and supplies the multipart targets
/// required by the Supermicro nosbios, host BIOS, and host BMC payloads.
pub struct SupermicroGb300Compute {
    inner: NvidiaGb300Compute,
}

impl SupermicroGb300Compute {
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::ComputeGb300Supermicro {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        let mut nvidia_config = config.clone();
        nvidia_config.node_type = NodeType::ComputeGb300Nvidia;
        NvidiaGb300Compute::from_config(&nvidia_config, rack_id).map(|inner| Self { inner })
    }

    pub async fn get_mnnvlink_topology(&self) -> Result<NvidiaMnnvlinkTopology> {
        self.inner.get_mnnvlink_topology().await
    }
}

fn supermicro_update_options_for_target(
    target: &FirmwareTarget,
    mut options: FirmwareUpdateOptions,
) -> Result<FirmwareUpdateOptions> {
    let component = target.component.trim();
    if component.eq_ignore_ascii_case("HMC")
        || component.eq_ignore_ascii_case(SUPERMICRO_HGX_TARGET)
    {
        ensure_supermicro_payload(target, "nosbios", |filename| {
            filename.ends_with(".fwpkg") && filename.contains("nosbios")
        })?;
        options.special_json = Some(json!({"Targets": [SUPERMICRO_HGX_TARGET]}));
    } else if component.eq_ignore_ascii_case(SUPERMICRO_BIOS_TARGET) {
        ensure_supermicro_payload(target, "BIOS", |filename| {
            filename.ends_with(".bin") && filename.contains("bios")
        })?;
        options.special_json = Some(json!({
            "Targets": [SUPERMICRO_CPU0_TARGET, SUPERMICRO_CPU1_TARGET]
        }));
    } else if supermicro_bmc_target(target) {
        ensure_supermicro_payload(target, "BMC", |filename| {
            filename.ends_with(".bin") && filename.contains("obmc")
        })?;
        options.special_json = Some(json!({"Targets": [SUPERMICRO_BMC_TARGET]}));
    } else {
        let component = if component.is_empty() {
            "<empty>"
        } else {
            component
        };
        return Err(RmsError::invalid_argument(format!(
            "unsupported Supermicro GB300 firmware target {component} for file {}",
            target.firmware_file
        )));
    }

    Ok(options)
}

fn ensure_supermicro_payload(
    target: &FirmwareTarget,
    payload_name: &str,
    matches: impl FnOnce(&str) -> bool,
) -> Result<()> {
    let filename = Path::new(&target.firmware_file)
        .file_name()
        .and_then(|filename| filename.to_str())
        .unwrap_or(&target.firmware_file)
        .to_ascii_lowercase();
    if matches(&filename) {
        Ok(())
    } else {
        Err(RmsError::invalid_argument(format!(
            "Supermicro GB300 {payload_name} target has incompatible firmware file {}",
            target.firmware_file
        )))
    }
}

pub(crate) fn supermicro_bmc_filename(firmware_file: &str) -> bool {
    let filename = Path::new(firmware_file)
        .file_name()
        .and_then(|filename| filename.to_str())
        .unwrap_or(firmware_file)
        .to_ascii_lowercase();
    filename.ends_with(".bin") && filename.contains("obmc")
}

pub(crate) fn supermicro_bmc_target(target: &FirmwareTarget) -> bool {
    let component = target.component.trim();
    component.eq_ignore_ascii_case("BMC")
        || component.eq_ignore_ascii_case(SUPERMICRO_BMC_TARGET)
        || (component.is_empty() && supermicro_bmc_filename(&target.firmware_file))
}

fn supermicro_version_check_targets(targets: &[FirmwareTarget]) -> Vec<FirmwareTarget> {
    targets
        .iter()
        .flat_map(|target| {
            if target
                .component
                .trim()
                .eq_ignore_ascii_case(SUPERMICRO_BIOS_TARGET)
            {
                [SUPERMICRO_CPU0_TARGET, SUPERMICRO_CPU1_TARGET]
                    .into_iter()
                    .map(|component| FirmwareTarget {
                        component: component.to_owned(),
                        firmware_file: target.firmware_file.clone(),
                        expected_version: target.expected_version.clone(),
                    })
                    .collect()
            } else if supermicro_bmc_target(target) {
                vec![FirmwareTarget {
                    component: SUPERMICRO_BMC_TARGET.to_owned(),
                    firmware_file: target.firmware_file.clone(),
                    expected_version: target.expected_version.clone(),
                }]
            } else {
                vec![target.clone()]
            }
        })
        .collect()
}

fn supermicro_bmc_firmware_manifest_version_check(
    targets: &[FirmwareTarget],
    inventory: &[FirmwareInfo],
) -> FirmwareVersionCheckSummary {
    let bmc_inventory = inventory
        .iter()
        .find(|component| component.target.eq_ignore_ascii_case(SUPERMICRO_BMC_TARGET));
    let installed_version = bmc_inventory.map(|component| component.version.as_str());

    let components = targets
        .iter()
        .filter(|target| supermicro_bmc_target(target))
        .map(|target| {
            let expected_version = target.expected_version.as_deref().unwrap_or("missing");
            let matched = installed_version.is_some_and(|installed| {
                installed
                    .trim()
                    .eq_ignore_ascii_case(expected_version.trim())
            });
            json!({
                "name": "FW_BMC_0",
                "status": if matched { "matched" } else { "mismatched" },
                "package_version": expected_version,
                "system_version": installed_version.unwrap_or("missing"),
                "inventory_path": SUPERMICRO_BMC_TARGET,
            })
        })
        .collect::<Vec<_>>();
    let mismatches = components
        .iter()
        .filter(|component| component["status"] == "mismatched")
        .cloned()
        .collect::<Vec<_>>();

    FirmwareVersionCheckSummary {
        matched: mismatches.is_empty(),
        details: json!({
            "status": if mismatches.is_empty() { "matched" } else { "mismatch" },
            "verification": "supermicro_firmware_manifest_version",
            "components": components,
            "mismatches": mismatches,
        }),
    }
}

#[async_trait]
impl Node for SupermicroGb300Compute {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::ComputeGb300Supermicro
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::ComputeGb300Supermicro.as_str().to_owned(),
        );
        info
    }

    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        self.inner.expected_inventory_policy()
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        self.inner.get_power_state().await
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        self.inner.set_power_state(op, target).await
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        self.inner.get_firmware_inventory().await
    }

    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        let bmc_targets_have_firmware_manifest_versions = !targets.is_empty()
            && targets.iter().all(|target| {
                supermicro_bmc_target(target)
                    && target
                        .expected_version
                        .as_deref()
                        .is_some_and(|version| !version.trim().is_empty())
            });
        if bmc_targets_have_firmware_manifest_versions {
            let inventory = self.inner.get_firmware_inventory().await?;
            return Ok(supermicro_bmc_firmware_manifest_version_check(
                targets, &inventory,
            ));
        }

        let version_check_targets = supermicro_version_check_targets(targets);
        self.inner
            .verify_firmware_package_versions(&version_check_targets)
            .await
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let options = supermicro_update_options_for_target(target, options)?;
        self.inner
            .update_firmware(target, force_update, options)
            .await
    }

    async fn start_firmware_upload(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
    ) -> Result<String> {
        match self
            .update_firmware(target, force_update, FirmwareUpdateOptions::default())
            .await?
        {
            FirmwareUpdateOutcome::Started(handle) => Ok(handle.task_id),
            FirmwareUpdateOutcome::Completed(summary) => Err(RmsError::internal(format!(
                "GB300 Supermicro firmware update completed without task id: {}",
                summary.message
            ))),
            FirmwareUpdateOutcome::Skipped { reason } => Err(RmsError::already_exists(reason)),
        }
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        self.inner.poll_firmware_task(task_id).await
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        self.inner.activate_firmware_with(request).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
    use crate::utilities::error::ErrorCode;

    fn config() -> NodeConfig {
        NodeConfig {
            id: "c-01".into(),
            node_type: NodeType::ComputeGb300Supermicro,
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
        }
    }

    fn target(component: &str, firmware_file: &str) -> FirmwareTarget {
        FirmwareTarget {
            component: component.to_owned(),
            firmware_file: firmware_file.to_owned(),
            expected_version: None,
        }
    }

    fn bmc_inventory(version: &str) -> FirmwareInfo {
        FirmwareInfo {
            name: "FW_BMC_0".to_owned(),
            version: version.to_owned(),
            firmware_type: FirmwareType::BMC,
            updateable: true,
            target: SUPERMICRO_BMC_TARGET.to_owned(),
            health: "OK".to_owned(),
            sku: String::new(),
        }
    }

    #[test]
    fn from_config_reports_supermicro_type() {
        let compute = SupermicroGb300Compute::from_config(&config(), "rack-01").unwrap();

        assert_eq!(compute.node_type(), NodeType::ComputeGb300Supermicro);
        assert_eq!(
            compute.get_info()["type"],
            NodeType::ComputeGb300Supermicro.as_str()
        );
    }

    #[test]
    fn update_options_target_nosbios_bios_and_host_bmc() {
        let nosbios = supermicro_update_options_for_target(
            &target(
                SUPERMICRO_HGX_TARGET,
                "nvfw_GB300_custom_nosbios_prod-signed.fwpkg",
            ),
            FirmwareUpdateOptions::default(),
        )
        .unwrap();
        let bios = supermicro_update_options_for_target(
            &target(SUPERMICRO_BIOS_TARGET, "BIOS_GPU-NVGB300_2.3b.bin"),
            FirmwareUpdateOptions::default(),
        )
        .unwrap();
        let bmc = supermicro_update_options_for_target(
            &target("", "NvOBMC_GBNVL72_70.02.01.05.bin"),
            FirmwareUpdateOptions::default(),
        )
        .unwrap();

        assert_eq!(
            nosbios.special_json,
            Some(json!({"Targets": [SUPERMICRO_HGX_TARGET]}))
        );
        assert_eq!(
            bios.special_json,
            Some(json!({"Targets": [SUPERMICRO_CPU0_TARGET, SUPERMICRO_CPU1_TARGET]}))
        );
        assert_eq!(
            bmc.special_json,
            Some(json!({"Targets": [SUPERMICRO_BMC_TARGET]}))
        );
    }

    #[test]
    fn update_options_reject_wrong_payload_for_supermicro_target() {
        let error = supermicro_update_options_for_target(
            &target(SUPERMICRO_BIOS_TARGET, "nvfw_custom_nosbios.fwpkg"),
            FirmwareUpdateOptions::default(),
        )
        .unwrap_err();

        assert!(error.message.contains("BIOS target has incompatible"));
    }

    #[test]
    fn update_options_reject_targets_without_supermicro_mapping() {
        for target in [
            target("HGX_FW_GPU_0", "gpu-firmware.bin"),
            target("", "BIOS_GPU-NVGB300_2.3b.bin"),
        ] {
            let error =
                supermicro_update_options_for_target(&target, FirmwareUpdateOptions::default())
                    .unwrap_err();

            assert_eq!(error.code, ErrorCode::InvalidArgument);
            assert!(
                error
                    .message
                    .contains("unsupported Supermicro GB300 firmware target")
            );
        }
    }

    #[test]
    fn version_check_expands_bios_and_host_bmc_inventory_targets() {
        let targets = supermicro_version_check_targets(&[
            target(SUPERMICRO_BIOS_TARGET, "BIOS_GPU-NVGB300_2.3b.bin"),
            target("", "NvOBMC_GBNVL72_70.02.01.05.bin"),
        ]);

        assert_eq!(
            targets
                .iter()
                .map(|target| target.component.as_str())
                .collect::<Vec<_>>(),
            vec![
                SUPERMICRO_CPU0_TARGET,
                SUPERMICRO_CPU1_TARGET,
                SUPERMICRO_BMC_TARGET
            ]
        );
    }

    #[test]
    fn bmc_firmware_manifest_version_check_matches_installed_inventory_version() {
        let mut bmc = target("", "NvOBMC_GBNVL72_70.02.01.05.bin");
        bmc.expected_version = Some("70.02.01.05".to_owned());

        let summary =
            supermicro_bmc_firmware_manifest_version_check(&[bmc], &[bmc_inventory("70.02.01.05")]);

        assert!(summary.matched);
        assert_eq!(summary.details["status"], "matched");
        assert_eq!(
            summary.details["verification"],
            "supermicro_firmware_manifest_version"
        );
        assert_eq!(
            summary.details["components"][0]["system_version"],
            "70.02.01.05"
        );
    }

    #[test]
    fn bmc_firmware_manifest_version_check_reports_installed_inventory_mismatch() {
        let mut bmc = target("", "NvOBMC_GBNVL72_70.02.01.05.bin");
        bmc.expected_version = Some("70.02.01.05".to_owned());

        let summary =
            supermicro_bmc_firmware_manifest_version_check(&[bmc], &[bmc_inventory("70.01.00.14")]);

        assert!(!summary.matched);
        assert_eq!(summary.details["status"], "mismatch");
        assert_eq!(
            summary.details["mismatches"][0]["package_version"],
            "70.02.01.05"
        );
        assert_eq!(
            summary.details["mismatches"][0]["system_version"],
            "70.01.00.14"
        );
    }
}
