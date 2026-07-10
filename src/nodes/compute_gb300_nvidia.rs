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
use serde_json::Value;

use crate::domain::node::*;
use crate::domain::rack::NodeConfig;
use crate::nodes::compute_gb200_nvidia::NvidiaGb200Compute;
use crate::nodes::nvfwupd_adapter;
use crate::utilities::error::{Result, RmsError};

/// NVIDIA GB300 compute node.
///
/// GB300 currently follows the GB200 Redfish and power behavior. Firmware
/// workflows use the NVFWUPD GB300 server type so package/target handling can
/// diverge from GB200 inside NVFWUPD.
pub struct NvidiaGb300Compute {
    inner: NvidiaGb200Compute,
}

impl NvidiaGb300Compute {
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::ComputeGb300Nvidia {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        NvidiaGb200Compute::from_config(config, rack_id).map(|inner| Self { inner })
    }

    pub async fn get_mnnvlink_topology(&self) -> Result<Value> {
        self.inner.get_mnnvlink_topology().await
    }

    fn nvfwupd_target_config(&self) -> nvfwupd::workflow::TargetConfig {
        self.inner
            .nvfwupd_target_config_for(NodeType::ComputeGb300Nvidia)
    }
}

#[async_trait]
impl Node for NvidiaGb300Compute {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::ComputeGb300Nvidia
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::ComputeGb300Nvidia.as_str().to_owned(),
        );
        info
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        self.inner.get_power_state().await
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        self.inner.set_power_state(op, target).await
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        tracing::debug!(node = %self.id(), "get_firmware_inventory");
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
        tracing::info!(node = %self.id(), "verify_firmware_package_versions via NVFWUPD GB300");
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
            node = %self.id(),
            component = %target.component,
            file = %target.firmware_file,
            "update_firmware via NVFWUPD GB300"
        );
        let request = nvfwupd_adapter::to_update_request(target, force_update, options);
        let outcome = nvfwupd::workflow_api::update_firmware(self.nvfwupd_target_config(), request)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_outcome(outcome))
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
                "GB300 firmware update completed without task id: {}",
                summary.message
            ))),
            FirmwareUpdateOutcome::Skipped { reason } => Err(RmsError::already_exists(reason)),
        }
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        tracing::debug!(node = %self.id(), task_id, "poll_firmware_task via NVFWUPD GB300");
        let status = nvfwupd::workflow_api::get_task_status(self.nvfwupd_target_config(), task_id)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::to_task_status(status))
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        tracing::info!(node = %self.id(), "activate_firmware via NVFWUPD GB300");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};

    fn config() -> NodeConfig {
        NodeConfig {
            id: "c-01".into(),
            node_type: NodeType::ComputeGb300Nvidia,
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
        }
    }

    #[test]
    fn from_config_reports_gb300_type() {
        let compute = NvidiaGb300Compute::from_config(&config(), "rack-01").unwrap();

        assert_eq!(compute.node_type(), NodeType::ComputeGb300Nvidia);
        assert_eq!(
            compute.get_info()["type"],
            NodeType::ComputeGb300Nvidia.as_str()
        );
    }

    #[test]
    fn nvfwupd_target_config_uses_gb300_server_type() {
        let compute = NvidiaGb300Compute::from_config(&config(), "rack-01").unwrap();
        let cfg = compute.nvfwupd_target_config();

        assert_eq!(cfg.ip, "10.0.0.1");
        assert_eq!(cfg.port, Some(8443));
        assert_eq!(cfg.username, "admin");
        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.server_type, nvfwupd::workflow::ServerType::GB300);
        assert!(!cfg.verify_tls);
    }
}
