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

//! Switch system-image capability shared by the node implementations and RPC worker.

use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::switch_gb200_nvidia::{NvosInstallationState, SwitchSystemImageState};
use crate::api::grpc::switch_certificate_handlers::SwitchCertificateInstallNode;
use crate::domain::rack::EndpointConfig;
use crate::transport::ssh::SshUploadOptions;
use crate::utilities::error::{Result, RmsError};

/// Capability required by the switch system-image workflow.
///
/// This is separate from the other switch capability traits because image
/// updates need mutable password recovery after boot and a narrower set of NVOS
/// image operations.
#[async_trait]
pub(crate) trait SwitchSystemImageNode {
    fn nvue_client(&self) -> Option<&nvue_client::SharedClient>;
    fn system_image_requires_bmc_activation(&self) -> bool;

    /// Require certificate installation support after ONIE recovery.
    /// Image-only test nodes fail closed when secure bootstrap is requested.
    fn require_certificate_install_node(&self) -> Result<&dyn SwitchCertificateInstallNode> {
        Err(RmsError::failed_precondition(
            "switch does not support NVUE certificate installation",
        ))
    }

    /// Probe NVUE REST first; a valid response identifies NVOS. An ONIE endpoint
    /// authorizes SSH fallback on any REST failure: query the NVOS version, then
    /// check for the NVUE binary if the query fails, before considering ONIE.
    /// Without an ONIE endpoint, return the REST error without attempting SSH.
    async fn verify_nvos_installation(
        &self,
        onie_endpoint: Option<&EndpointConfig>,
    ) -> Result<NvosInstallationState>;

    async fn stage_nvos_recovery_image(
        &self,
        onie_endpoint: &EndpointConfig,
        local_file_path: &str,
        image_filename: &str,
        upload_options: SshUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()>;

    /// Install the staged NVOS image and normalize the fresh installation to
    /// the requested `admin` password before returning.
    async fn install_staged_nvos_image_and_wait(
        &mut self,
        onie_endpoint: &EndpointConfig,
        image_filename: &str,
        recovery_timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<()>;

    /// Save the currently applied NVOS configuration for reboot persistence.
    async fn save_current_applied_config(&self) -> Result<()>;

    async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState>;
    async fn ensure_nvos_asic_firmware_config(&self) -> Result<()>;
    async fn poll_firmware_task(
        &self,
        task_id: &str,
        staged_install: bool,
    ) -> Result<crate::domain::node::FirmwareTaskStatus>;
    async fn uninstall_system_image(&self) -> Result<String>;
    async fn list_system_images(&self) -> Result<serde_json::Value>;
    async fn push_system_image_file(
        &self,
        local_file_path: &str,
        image_filename: &str,
        sftp_upload_options: SshUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()>;
    async fn install_system_image(&self, image_filename: &str) -> Result<String>;
    async fn activate_system_image_via_bmc(&self, cancel: &CancellationToken) -> Result<()>;
    async fn recover_admin_password_after_boot(&mut self, target_password: &str) -> Result<()>;
    /// `Ok` once the switch reports ready and healthy, `Aborted` when it
    /// reports a failed boot, `FailedPrecondition` while it reports anything
    /// else. The caller's poll stops on `Aborted` and retries every other
    /// error.
    async fn verify_system_ready(&self) -> Result<()>;
}
