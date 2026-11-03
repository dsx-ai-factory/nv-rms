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

//! VR NVL72 switch policy with shared NVUE/SSH transports.
//!
//! Inventory, comparison, and activation use the BMC directly to avoid a potential
//! BMC/NVOS communication issue.
//! Firmware uploads and NVOS installation still use the shared NVUE/SSH workflow.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use nvue_client::system::SystemImageReboot;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::switch_gb200_nvidia::{
    NvosInstallationState, SwitchGb200Nvidia, SwitchSystemImageState,
    is_valid_firmware_inventory_component,
};
use super::{SwitchFirmwareManagement, SwitchSystemImageNode, nvfwupd_adapter};
use crate::api::grpc::switch_certificate_handlers::SwitchCertificateInstallNode;
use crate::domain::node::{
    ExpectedInventoryPolicy, FirmwareActivationMode, FirmwareActivationRequest,
    FirmwareActivationSummary, FirmwareInfo, FirmwareTarget, FirmwareTaskStatus,
    FirmwareUpdateOptions, FirmwareUpdateOutcome, FirmwareVersionCheckSummary, Node, NodeType,
    PowerOp, PowerState, PowerTargetType,
};
use crate::domain::rack::{EndpointConfig, NodeConfig};
use crate::transport::redfish_client::RedfishClient;
use crate::transport::ssh::SshUploadOptions;
use crate::utilities::error::{Result, RmsError};

/// VR-specific management policy. Shared operations retain the VR node identity
/// and use the same operation lock and transports as the BMC-backed operations.
pub struct SwitchVrnvl72Nvidia {
    inner: SwitchGb200Nvidia,
}

impl SwitchVrnvl72Nvidia {
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::SwitchVrnvl72Nvidia {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }
        Ok(Self {
            inner: SwitchGb200Nvidia::from_shared_config(config, rack_id)?,
        })
    }

    /// Expose shared fabric/password/device-info operations explicitly; firmware
    /// and system-image dispatch must use this wrapper's capabilities instead.
    pub(super) fn shared_switch(&self) -> &SwitchGb200Nvidia {
        &self.inner
    }

    pub(crate) fn into_shared_switch(self) -> SwitchGb200Nvidia {
        self.inner
    }

    fn require_bmc_connection(&self) -> Result<(&EndpointConfig, &RedfishClient)> {
        self.inner.bmc_connection().ok_or_else(|| {
            RmsError::failed_precondition(
                "VR NVL72 switch BMC endpoint and credentials are required for power, firmware inventory, version comparison, and NVOS activation",
            )
        })
    }

    fn bmc_workflow(
        &self,
    ) -> Result<(
        nvfwupd::workflow_api::WorkflowContext,
        nvfwupd::workflow::TargetConfig,
    )> {
        let (endpoint, client) = self.require_bmc_connection()?;
        Ok((
            nvfwupd::workflow_api::WorkflowContext::new().with_switch_bmc_client(client.clone()),
            nvfwupd_adapter::to_target_config_with_credentials(
                self.node_type(),
                &endpoint.endpoint.ip_address,
                endpoint.endpoint.port,
                endpoint.credentials.as_ref(),
                !endpoint.dangerously_accept_invalid_certs,
            ),
        ))
    }
}

#[async_trait]
impl Node for SwitchVrnvl72Nvidia {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::SwitchVrnvl72Nvidia
    }

    fn get_info(&self) -> HashMap<String, String> {
        self.inner.get_info()
    }

    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        self.inner.expected_inventory_policy()
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        // Require the BMC before entering the shared path: no NVUE fallback on VR.
        self.require_bmc_connection()?;
        self.inner.get_power_state().await
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        self.require_bmc_connection()?;
        self.inner.set_power_state(op, target).await
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let _op_guard = self.inner.op_lock.lock().await;
        tracing::debug!(node = %self.id(), "get_firmware_inventory via NVFWUPD BMC");
        let (workflow, target) = self.bmc_workflow()?;
        let inventory = workflow
            .get_firmware_inventory(target)
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
        tracing::info!(node = %self.id(), "verify_firmware_package_versions via NVFWUPD BMC");
        let (workflow, target) = self.bmc_workflow()?;
        let summary = workflow
            .verify_firmware_target_versions(
                target,
                nvfwupd_adapter::to_version_check_targets(targets),
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
        // Upload/install stays on NVUE, but its inventory preflight uses the BMC.
        let (_, bmc) = self.require_bmc_connection()?;
        let workflow = self
            .inner
            .nvfwupd_workflow()?
            .with_switch_bmc_client(bmc.clone());
        self.inner
            .update_firmware_with_workflow(target, force_update, options, workflow)
            .await
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        Node::poll_firmware_task(&self.inner, task_id).await
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        let _op_guard = self.inner.op_lock.lock().await;
        tracing::info!(node = %self.id(), "activate_firmware via NVFWUPD BMC");
        let (workflow, target) = self.bmc_workflow()?;
        let summary = workflow
            .activate_firmware(target, nvfwupd_adapter::to_activation_request(request))
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_activation_summary(summary))
    }

    async fn activate_firmware(&self) -> Result<()> {
        self.activate_firmware_with(FirmwareActivationRequest {
            mode: FirmwareActivationMode::SwitchPowerCycle,
            cancellation: None,
        })
        .await
        .map(|_| ())
    }

    fn supports_bmc_aux_powercycle(&self) -> bool {
        self.inner.supports_bmc_aux_powercycle()
    }

    async fn bmc_aux_powercycle(&self) -> Result<()> {
        self.require_bmc_connection()?;
        self.inner.bmc_aux_powercycle().await
    }

    async fn verify_management_api_reachable(&self) -> Result<()> {
        // VR activates through the BMC, so NVUE may still be booting when the
        // version check has passed. GB200 and GB300 keep the `Node` default:
        // their version check reads firmware inventory through NVUE, so NVUE
        // has answered before the readiness poll. No operation lock: the
        // probe is one read-only GET with the body ignored, and a status read
        // must not wait behind it.
        self.inner.probe_nvue_reachable().await
    }

    async fn verify_system_ready(&self) -> Result<()> {
        Node::verify_system_ready(&self.inner).await
    }
}

#[async_trait]
impl SwitchFirmwareManagement for SwitchVrnvl72Nvidia {
    async fn list_firmware(&self, component: &str, list_files: bool) -> Result<Value> {
        if list_files {
            return SwitchFirmwareManagement::list_firmware(&self.inner, component, true).await;
        }
        if !is_valid_firmware_inventory_component(component) {
            return Err(RmsError::invalid_argument(format!(
                "invalid firmware component '{component}' \
                 (valid: bmc, fpga, erot, cpld, bios, transceiver, cpld<N>)"
            )));
        }
        // Lock exactly once at the capability boundary, including registered-node calls.
        let _op_guard = self.inner.op_lock.lock().await;
        let (workflow, target) = self.bmc_workflow()?;
        let inventory = workflow
            .get_switch_firmware_inventory(target, component)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        if inventory.is_empty() {
            return Err(RmsError::not_found(format!(
                "switch BMC firmware component {component}"
            )));
        }
        let entries = inventory
            .into_iter()
            .map(|entry| (entry.inventory_path.unwrap_or(entry.name), entry.details))
            .collect();
        Ok(Value::Object(entries))
    }

    async fn push_firmware_file(
        &self,
        local_file_path: &str,
        component: &str,
        filename: &str,
    ) -> Result<()> {
        SwitchFirmwareManagement::push_firmware_file(
            &self.inner,
            local_file_path,
            component,
            filename,
        )
        .await
    }

    async fn list_system_images(&self) -> Result<Value> {
        SwitchFirmwareManagement::list_system_images(&self.inner).await
    }
}

#[async_trait]
impl SwitchSystemImageNode for SwitchVrnvl72Nvidia {
    fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
        self.inner.optional_nvue_client()
    }

    fn system_image_requires_bmc_activation(&self) -> bool {
        true
    }

    fn require_certificate_install_node(&self) -> Result<&dyn SwitchCertificateInstallNode> {
        SwitchSystemImageNode::require_certificate_install_node(&self.inner)
    }

    async fn verify_nvos_installation(
        &self,
        onie_endpoint: Option<&EndpointConfig>,
    ) -> Result<NvosInstallationState> {
        SwitchSystemImageNode::verify_nvos_installation(&self.inner, onie_endpoint).await
    }

    async fn stage_nvos_recovery_image(
        &self,
        onie_endpoint: &EndpointConfig,
        local_file_path: &str,
        image_filename: &str,
        upload_options: SshUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()> {
        SwitchSystemImageNode::stage_nvos_recovery_image(
            &self.inner,
            onie_endpoint,
            local_file_path,
            image_filename,
            upload_options,
            cancel,
        )
        .await
    }

    async fn install_staged_nvos_image_and_wait(
        &mut self,
        onie_endpoint: &EndpointConfig,
        image_filename: &str,
        recovery_timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<()> {
        SwitchSystemImageNode::install_staged_nvos_image_and_wait(
            &mut self.inner,
            onie_endpoint,
            image_filename,
            recovery_timeout,
            cancel,
        )
        .await
    }

    async fn save_current_applied_config(&self) -> Result<()> {
        SwitchSystemImageNode::save_current_applied_config(&self.inner).await
    }

    async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
        SwitchSystemImageNode::get_normalized_system_image_state(&self.inner).await
    }

    async fn ensure_nvos_asic_firmware_config(&self) -> Result<()> {
        SwitchSystemImageNode::ensure_nvos_asic_firmware_config(&self.inner).await
    }

    async fn poll_firmware_task(
        &self,
        task_id: &str,
        staged_install: bool,
    ) -> Result<FirmwareTaskStatus> {
        SwitchSystemImageNode::poll_firmware_task(&self.inner, task_id, staged_install).await
    }

    async fn uninstall_system_image(&self) -> Result<String> {
        SwitchSystemImageNode::uninstall_system_image(&self.inner).await
    }

    async fn list_system_images(&self) -> Result<Value> {
        SwitchSystemImageNode::list_system_images(&self.inner).await
    }

    async fn push_system_image_file(
        &self,
        local_file_path: &str,
        image_filename: &str,
        sftp_upload_options: SshUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()> {
        SwitchSystemImageNode::push_system_image_file(
            &self.inner,
            local_file_path,
            image_filename,
            sftp_upload_options,
            cancel,
        )
        .await
    }

    async fn install_system_image(&self, image_filename: &str) -> Result<String> {
        let _op_guard = self.inner.op_lock.lock().await;
        self.require_bmc_connection()?;
        // Use direct BMC activation to avoid a potential BMC/NVOS communication issue.
        // The system-image worker performs one direct BMC cycle after staged success.
        self.inner
            .install_system_image_with_reboot(image_filename, Some(SystemImageReboot::No))
            .await
    }

    async fn activate_system_image_via_bmc(&self, cancel: &CancellationToken) -> Result<()> {
        cancel
            .run_until_cancelled(self.bmc_aux_powercycle())
            .await
            .ok_or_else(|| RmsError::cancelled("NVOS BMC activation cancelled"))?
    }

    async fn recover_admin_password_after_boot(&mut self, target_password: &str) -> Result<()> {
        SwitchSystemImageNode::recover_admin_password_after_boot(&mut self.inner, target_password)
            .await
    }

    async fn verify_system_ready(&self) -> Result<()> {
        SwitchSystemImageNode::verify_system_ready(&self.inner).await
    }
}

#[cfg(test)]
impl SwitchVrnvl72Nvidia {
    pub(crate) fn for_test(base_url: &str) -> Self {
        Self {
            inner: SwitchGb200Nvidia::for_test(base_url)
                .with_node_type_for_test(NodeType::SwitchVrnvl72Nvidia),
        }
    }

    pub(crate) fn with_redfish_for_test(mut self, base_url: &str) -> Self {
        self.inner = self.inner.with_redfish_for_test(base_url);
        self
    }
}

#[cfg(test)]
mod tests;
