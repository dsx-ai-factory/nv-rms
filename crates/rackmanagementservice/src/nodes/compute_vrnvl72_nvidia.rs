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

use async_trait::async_trait;

use crate::domain::node::*;
use crate::domain::rack::NodeConfig;
use crate::nodes::compute_gb200_nvidia::NvidiaGb200Compute;
use crate::nodes::nvfwupd_adapter;
use crate::transport::redfish_client::NvidiaMnnvlinkTopology;
use crate::utilities::error::{Result, RmsError};

/// NVIDIA VRNVL72 compute node.
///
/// Reuses RMS's standard NVIDIA compute-tray power and topology operations,
/// while firmware workflows identify VRNVL72 as the distinct NVFWUPD VRNVL72
/// platform.
pub struct NvidiaVrnvl72Compute {
    inner: NvidiaGb200Compute,
}

impl NvidiaVrnvl72Compute {
    async fn update_linkx(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        if force_update {
            return Err(RmsError::failed_precondition(
                "LINKX does not permit forced flashing or health-gate bypasses",
            ));
        }
        let policy = target.linkx_policy.clone().ok_or_else(|| RmsError::failed_precondition(
            "LINKX requires ApplyFirmwareObject with an explicit LinkxRollout policy; direct firmware requests cannot supply this policy",
        ))?;
        let report = nvfwupd::workflow_api::update_linkx_firmware(
            self.inner.inband_host_target_config()?,
            nvfwupd::flint::LinkxUpdateRequest {
                targets: vec![nvfwupd::flint::LinkxFirmwareTarget {
                    connectors: nvfwupd::flint::LinkxConnector::ALL.to_vec(),
                    firmware_file: target.firmware_file.clone(),
                    policy,
                }],
                cancellation: options.cancellation,
            },
        )
        .await
        .map_err(nvfwupd_adapter::map_error)?;
        let details =
            serde_json::to_value(&report).map_err(|e| RmsError::internal(e.to_string()))?;
        if !report.completed {
            // Preserve partial-update evidence even when the standard RMS job
            // error envelope carries the failure as a message.
            return Err(RmsError::failed_precondition(format!(
                "LinkX update incomplete (firmware may have changed): {details}"
            )));
        }
        if report
            .connectors
            .iter()
            .all(|c| c.status == nvfwupd::flint::LinkxDisposition::Skipped)
        {
            return Ok(FirmwareUpdateOutcome::Skipped { reason: "all selected LinkX connectors already report the target version on both module paths".to_owned() });
        }
        Ok(FirmwareUpdateOutcome::Completed(FirmwareUpdateSummary {
            message: "LinkX firmware flashed, module-activated, and verified on both paths with eight links recovered per connector".to_owned(),
            task_ids: vec![], details,
        }))
    }

    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::ComputeVrnvl72Nvidia {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        NvidiaGb200Compute::from_config(config, rack_id).map(|inner| Self { inner })
    }

    pub async fn get_mnnvlink_topology(&self) -> Result<NvidiaMnnvlinkTopology> {
        self.inner.get_mnnvlink_topology().await
    }

    fn nvfwupd_target_config(&self) -> nvfwupd::workflow::TargetConfig {
        self.inner.nvfwupd_target_config()
    }
}

#[async_trait]
impl Node for NvidiaVrnvl72Compute {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::ComputeVrnvl72Nvidia
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::ComputeVrnvl72Nvidia.as_str().to_owned(),
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
        let _op_guard = self.inner.op_lock.lock().await;

        tracing::debug!(node = %self.id(), "get_firmware_inventory");
        let inventory = self
            .inner
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
        let _op_guard = self.inner.op_lock.lock().await;

        if targets
            .iter()
            .any(|t| t.component.trim().eq_ignore_ascii_case("LINKX"))
        {
            let [target] = targets else {
                return Err(RmsError::invalid_argument(
                    "verify LINKX separately from out-of-band targets",
                ));
            };
            let version = target
                .linkx_policy
                .as_ref()
                .map(|p| p.target_version.as_str())
                .ok_or_else(|| {
                    RmsError::failed_precondition(
                        "LINKX version verification requires its manifest rollout policy",
                    )
                })?;
            let evidence = nvfwupd::flint::inspect_linkx_firmware(
                self.inner.inband_host_target_config()?,
                &nvfwupd::flint::LinkxConnector::ALL,
                version,
            )
            .await
            .map_err(nvfwupd_adapter::map_error)?;
            return Ok(FirmwareVersionCheckSummary {
                matched: evidence.iter().all(|(_, e)| {
                    !e.inspection_timed_out
                        && e.query_errors.is_empty()
                        && e.version_state == nvfwupd::flint::LinkxVersionState::Target
                }),
                details: serde_json::json!({ "connectors": evidence }),
            });
        }

        tracing::info!(node = %self.id(), "verify_firmware_package_versions via NVFWUPD VRNVL72");
        let targets = nvfwupd_adapter::to_version_check_targets(targets);
        let summary = self
            .inner
            .firmware_workflow()
            .verify_firmware_target_versions_after_activation_with_expected_inventory(
                self.nvfwupd_target_config(),
                targets,
                self.expected_inventory_policy()
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
        let _op_guard = self.inner.op_lock.lock().await;

        if target.component.trim().eq_ignore_ascii_case("LINKX") {
            return self.update_linkx(target, force_update, options).await;
        }

        tracing::info!(
            node = %self.id(),
            component = %target.component,
            file = %target.firmware_file,
            "update_firmware via NVFWUPD VRNVL72"
        );
        let request = nvfwupd_adapter::to_update_request(target, force_update, options);
        let expected_inventory = self
            .expected_inventory_policy()
            .map(|policy| policy.ap_names.as_ref().to_vec());
        let outcome = self
            .inner
            .firmware_workflow()
            .update_firmware_with_expected_inventory(
                self.nvfwupd_target_config(),
                request,
                expected_inventory,
            )
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_outcome(outcome))
    }

    async fn start_firmware_upload(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
    ) -> Result<String> {
        if target.component.trim().eq_ignore_ascii_case("LINKX") {
            return Err(RmsError::unimplemented(
                "task-id-only upload for LINKX; use ApplyFirmwareObject",
                self.node_type(),
            ));
        }
        match self
            .update_firmware(target, force_update, FirmwareUpdateOptions::default())
            .await?
        {
            FirmwareUpdateOutcome::Started(handle) => Ok(handle.task_id),
            FirmwareUpdateOutcome::Completed(summary) => Err(RmsError::internal(format!(
                "VRNVL72 firmware update completed without task id: {}",
                summary.message
            ))),
            FirmwareUpdateOutcome::Skipped { reason } => Err(RmsError::already_exists(reason)),
        }
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let _op_guard = self.inner.op_lock.lock().await;

        tracing::debug!(node = %self.id(), task_id, "poll_firmware_task via NVFWUPD VRNVL72");
        let status = self
            .inner
            .firmware_workflow()
            .get_task_status(self.nvfwupd_target_config(), task_id)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::to_task_status(status))
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        let _op_guard = self.inner.op_lock.lock().await;

        tracing::info!(node = %self.id(), "activate_firmware via NVFWUPD VRNVL72");
        let request = nvfwupd_adapter::to_activation_request(request);
        let summary = self
            .inner
            .firmware_workflow()
            .activate_firmware(self.nvfwupd_target_config(), request)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};

    fn config() -> NodeConfig {
        NodeConfig {
            id: "c-01".into(),
            node_type: NodeType::ComputeVrnvl72Nvidia,
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
            recovery: None,
            runtime_config: None,
        }
    }

    #[test]
    fn from_config_reports_vrnvl72_type() {
        let compute = NvidiaVrnvl72Compute::from_config(&config(), "rack-01").unwrap();

        assert_eq!(compute.node_type(), NodeType::ComputeVrnvl72Nvidia);
        assert_eq!(
            compute.get_info()["type"],
            NodeType::ComputeVrnvl72Nvidia.as_str()
        );
    }

    #[test]
    fn nvfwupd_target_config_uses_vrnvl72_server_type() {
        let compute = NvidiaVrnvl72Compute::from_config(&config(), "rack-01").unwrap();
        let cfg = compute.nvfwupd_target_config();

        assert_eq!(cfg.ip, "10.0.0.1");
        assert_eq!(cfg.port, Some(8443));
        assert_eq!(cfg.username, "admin");
        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.server_type, nvfwupd::workflow::ServerType::VRNVL72);
        assert!(!cfg.verify_tls);
    }

    #[test]
    fn rejects_other_compute_node_types() {
        let mut config = config();
        config.node_type = NodeType::ComputeGb200Nvidia;

        let error = NvidiaVrnvl72Compute::from_config(&config, "rack-01")
            .err()
            .expect("GB200 config should be rejected");

        assert!(error.message.contains("compute_gb200_nvidia"));
    }
}
