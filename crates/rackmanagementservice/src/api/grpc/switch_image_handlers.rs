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

//! Handlers for UpdateSwitchSystemImage (batch NVOS image update driven by
//! an explicit node list) and GetSwitchSystemImageJobStatus.
//!
//! `switch_image_rpcs.cpp`: each node becomes an ephemeral
//! Switch node (credentials / IP from the request, not the inventory); per-node
//! work runs under a lifecycle supervisor that drives SFTP upload then NVUE
//! install. Progress is tracked via the unified `JobTracker`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::server::RackManagerServiceImpl;
use super::switch_certificate_handlers::{
    NvueTlsTransition, SwitchCertificateInstallNode, SwitchCertificateInstallProgress,
    SwitchCertificateInstallStage, install_switch_certificate_material,
    prepare_nvue_tls_transition,
};
use crate::api::grpc::conversions::{
    additional_host_ssh_endpoint, flatten_node_info, proto_node_type_to_domain,
    proto_node_type_to_string, timestamp_from_datetime,
};
use crate::api::grpc::firmware_handlers::{resolve_firmware_file, wait_for_system_ready};
use crate::config::hardware_profiles::VerificationPolicy;
use crate::domain::node::{Node, NodeKind, NodeType};
use crate::domain::rack::{EndpointConfig, NodeConfig};
use crate::nodes::nvos::GnmiClientCa;
#[cfg(test)]
use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;
use crate::nodes::switch_gb200_nvidia::{
    NvosInstallationState, SwitchSystemImageState, UnusedPartitionCleanup, describe_install_poll,
    infer_target_build_id, is_http_unauthorized_error, is_install_handoff_to_reboot,
    is_install_poll_success, is_install_progress_state, is_install_reboot_transition_state,
    is_retryable_install_poll_error, is_retryable_nvue_transport_error,
    is_retryable_steady_state_error, is_uninstall_poll_success, plan_unused_partition_cleanup,
    system_image_listing_contains,
};
use crate::nodes::switch_mtls::{
    CertificateBindings, CertificateInventory, CertificateStore, MtlsSftpCopyOutcome,
    SwitchMtlsMaterialPaths, SwitchMtlsService,
};
#[cfg(test)]
use crate::nodes::switch_vrnvl72_nvidia::SwitchVrnvl72Nvidia;
use crate::nodes::{NodeInstance, SwitchSystemImageNode};
use crate::observability::tracing::record_phase_result;
use crate::orchestrator::job_lifecycle::{JobError, JobFailure};
use crate::orchestrator::job_tracker::{JobType, RmsJobHandle};
use crate::orchestrator::stage_timeline::{Stage, StageTimeline, fail_job};
use crate::transport::ssh::SshUploadOptions;
use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::insert_json_field;
use librms::protos::rack_manager as rm;

pub(crate) type SharedSwitchSystemImageNode =
    Arc<tokio::sync::Mutex<Box<dyn SwitchSystemImageNode + Send + Sync>>>;

/// A switch that `UpdateSwitchSystemImage` admitted, carried from the admission
/// pass to the worker that is spawned for it.
///
/// Only per-switch state lives here. The rest of what a worker needs is either
/// the same for every switch in the batch (`image_filename`, `local_file_path`,
/// `target_build_id`, `sftp_upload_options`) and so is cloned per worker at the
/// spawn site, or is derived from `pending` (the cancellation token).
struct AdmittedSwitchImageJob {
    pending: RmsJobHandle,
    switch: Box<dyn SwitchSystemImageNode + Send + Sync>,
    node_type: NodeType,
    recovery: SwitchImageRecoveryOptions,
    rack_id: String,
    node_id: String,
    user: String,
    pass: String,
}

/// Recovery authorization and the optional ONIE endpoint resolved from one
/// switch image request.
#[derive(Debug, Clone)]
pub(crate) struct SwitchImageRecoveryOptions {
    pub(crate) allow_onie_recovery: bool,
    pub(crate) onie_endpoint: Option<EndpointConfig>,
}

impl SwitchImageRecoveryOptions {
    /// Resolve the image-RPC meaning of `NodeInfo.additional_host_endpoints`.
    ///
    /// When recovery is authorized, exactly one entry is required and is
    /// interpreted as the ONIE SSH endpoint. When recovery is not
    /// authorized, supplemental endpoints remain available for other RPC
    /// contracts but are not parsed or used by the image workflow.
    pub(crate) fn from_node_info(
        node_info: &rm::NodeInfo,
        allow_onie_recovery: bool,
    ) -> Result<Self> {
        let onie_endpoint = if allow_onie_recovery {
            match node_info.additional_host_endpoints.as_slice() {
                [endpoint] => Some(additional_host_ssh_endpoint(endpoint)?),
                endpoints => {
                    return Err(RmsError::invalid_argument(format!(
                        "switch image recovery requires exactly one additional host endpoint; node {} supplied {}",
                        node_info.node_id,
                        endpoints.len()
                    )));
                }
            }
        } else {
            None
        };

        Ok(Self {
            allow_onie_recovery,
            onie_endpoint,
        })
    }
}

/// Owns the admitted image node so the shared installer can clean staged
/// material even if the installation future is dropped.
struct SharedSwitchCertificateInstallNode {
    node_id: String,
    switch: SharedSwitchSystemImageNode,
}

#[async_trait]
impl SwitchCertificateInstallNode for SharedSwitchCertificateInstallNode {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    async fn read_certificate_inventory(&self) -> Result<CertificateInventory> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .read_certificate_inventory()
            .await
    }

    async fn read_certificate_bindings(&self) -> Result<CertificateBindings> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .read_certificate_bindings()
            .await
    }

    async fn delete_certificate_entry(&self, store: CertificateStore, id: &str) -> Result<()> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .delete_certificate_entry(store, id)
            .await
    }

    async fn prepare_mtls_remote_dirs(&self, material: &SwitchMtlsMaterialPaths) -> Result<()> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .prepare_mtls_remote_dirs(material)
            .await
    }

    async fn sftp_copy_mtls_file(
        &self,
        material: &SwitchMtlsMaterialPaths,
        remote_file_name: &str,
        cancel: CancellationToken,
    ) -> Result<MtlsSftpCopyOutcome> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .sftp_copy_mtls_file(material, remote_file_name, cancel)
            .await
    }

    async fn import_mtls_material(&self, material: &SwitchMtlsMaterialPaths) -> Result<()> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .import_mtls_material(material)
            .await
    }

    async fn initialize_nvue_capabilities(&self) -> Result<GnmiClientCa> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .initialize_nvue_capabilities()
            .await
    }

    async fn cleanup_mtls_remote_dir_best_effort(&self, material: &SwitchMtlsMaterialPaths) {
        let switch = self.switch.lock().await;

        if let Ok(switch) = switch.require_certificate_install_node() {
            switch.cleanup_mtls_remote_dir_best_effort(material).await;
        }
    }

    async fn bind_mtls_service_for_install(
        &self,
        material: &SwitchMtlsMaterialPaths,
        service: SwitchMtlsService,
        nvue_transition: Option<&NvueTlsTransition>,
    ) -> Result<()> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .bind_mtls_service_for_install(material, service, nvue_transition)
            .await
    }

    async fn verify_mtls_services(
        &self,
        material: &SwitchMtlsMaterialPaths,
        services: &[SwitchMtlsService],
    ) -> Result<Value> {
        let switch = self.switch.lock().await;

        switch
            .require_certificate_install_node()?
            .verify_mtls_services(material, services)
            .await
    }
}

struct NvueCertificateBootstrapProgress<'a> {
    cancel: &'a CancellationToken,
    current_stage: Option<SwitchCertificateInstallStage>,
}

impl SwitchCertificateInstallProgress for NvueCertificateBootstrapProgress<'_> {
    fn start_stage(&mut self, stage: SwitchCertificateInstallStage) -> Result<()> {
        self.current_stage = Some(stage);

        if self.cancel.is_cancelled() {
            return Err(RmsError::cancelled(format!(
                "NVUE certificate bootstrap cancelled before {}",
                stage.name()
            )));
        }

        Ok(())
    }

    fn complete_stage(&mut self, _details: Value) {}
}

pub(crate) async fn recover_nvos_through_onie(
    switch: &mut (dyn SwitchSystemImageNode + Send + Sync),
    onie_endpoint: &EndpointConfig,
    local_file_path: &str,
    image_filename: &str,
    upload_options: SshUploadOptions,
    recovery_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<()> {
    switch
        .stage_nvos_recovery_image(
            onie_endpoint,
            local_file_path,
            image_filename,
            upload_options,
            cancel,
        )
        .await?;

    switch
        .install_staged_nvos_image_and_wait(onie_endpoint, image_filename, recovery_timeout, cancel)
        .await
}

/// Persist credentials established by ONIE recovery after NVUE transport is
/// initialized. Transient boot-time transport failures and HTTP 401 responses
/// are retried within a bounded, cancellable window.
pub(crate) async fn persist_recovered_nvos_credentials(
    switch: &(dyn SwitchSystemImageNode + Send + Sync),
    save_timeout: Duration,
    cancel: &CancellationToken,
) -> Result<()> {
    let deadline = Instant::now().checked_add(save_timeout).ok_or_else(|| {
        RmsError::invalid_argument("recovered credential save timeout is too large")
    })?;

    let last_error = loop {
        if cancel.is_cancelled() {
            return Err(RmsError::cancelled(
                "NVOS recovery cancelled while persisting recovered credentials",
            ));
        }

        match switch.save_current_applied_config().await {
            Ok(()) => return Ok(()),
            Err(error)
                if is_retryable_nvue_transport_error(&error)
                    || is_http_unauthorized_error(&error) =>
            {
                if Instant::now() >= deadline {
                    break error;
                }

                tracing::info!(
                    error = %error.message,
                    "NVUE is not ready to persist recovered NVOS credentials"
                );
            }
            Err(error) => {
                return Err(RmsError::new(
                    error.code,
                    format!(
                        "failed to persist recovered NVOS credentials: {}",
                        error.message
                    ),
                ));
            }
        }

        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(RmsError::cancelled(
                    "NVOS recovery cancelled while persisting recovered credentials",
                ));
            }
            _ = tokio::time::sleep(RECOVERED_CREDENTIAL_SAVE_POLL_INTERVAL) => {}
        }
    };

    Err(RmsError::timeout(format!(
        "timed out persisting recovered NVOS credentials: {}",
        last_error.message
    )))
}

/// Establishes the secure NVUE transport required after ONIE recovery.
///
/// Secure recovery must not continue to credential persistence until this
/// operation installs the RMS-managed certificate and verifies mTLS. The
/// factory-certificate fallback remains confined to the readiness check used
/// by certificate installation.
pub(crate) async fn bootstrap_nvue_certificate(
    service: &RackManagerServiceImpl,
    switch: &SharedSwitchSystemImageNode,
    node_id: &str,
    cancel: &CancellationToken,
) -> Result<()> {
    if cancel.is_cancelled() {
        return Err(RmsError::cancelled(
            "NVUE certificate bootstrap cancelled before readiness",
        ));
    }

    let nvue_client = {
        let switch = switch.lock().await;
        switch.nvue_client().cloned()
    }
    .ok_or_else(|| {
        RmsError::failed_precondition(
            "NVUE certificate readiness failed: switch has no NVUE client",
        )
    })?;

    let endpoint = nvue_client.endpoint();

    service
        .ensure_nvue_client_for_certificate(Some(&nvue_client), None, Some(&endpoint.host))
        .await
        .map_err(|error| {
            RmsError::new(
                error.code,
                format!("NVUE certificate readiness failed: {}", error.message),
            )
        })?;

    if cancel.is_cancelled() {
        return Err(RmsError::cancelled(
            "NVUE certificate bootstrap cancelled before material resolution",
        ));
    }

    let material_error = |message| {
        RmsError::failed_precondition(format!(
            "NVUE certificate material resolution failed: {message}"
        ))
    };

    let domain = service
        .switch_tls_roots
        .effective_domain(None)
        .map_err(material_error)?;

    let tls_material = service
        .switch_tls_roots
        .resolve_switch_cert(None)
        .map_err(material_error)?;

    let client_tls = service
        .switch_tls_roots
        .resolve_required_nvue_client_tls(None)
        .map_err(material_error)?;

    let server_name = service
        .switch_tls_roots
        .tls_server_name_for_endpoint(&endpoint.host, None)
        .map_err(material_error)?;

    let (Some(ca_cert_path), Some(entity_cert_path), Some(entity_key_path)) = (
        tls_material.ca_cert_path,
        tls_material.client_cert_path,
        tls_material.client_key_path,
    ) else {
        return Err(RmsError::failed_precondition(
            "NVUE certificate material resolution failed: switch certificate material requires ca.pem, client.pem, and client.key",
        ));
    };

    let material = SwitchMtlsMaterialPaths::load_stable(
        domain,
        ca_cert_path,
        entity_cert_path,
        entity_key_path,
    )
    .await
    .map_err(|error| material_error(error.message))?;

    let nvue_transition =
        prepare_nvue_tls_transition(Some(&nvue_client), Some(&client_tls), server_name, None)
            .await
            .map_err(|error| {
                RmsError::new(
                    error.code,
                    format!(
                        "NVUE certificate binding preparation failed: {}",
                        error.message
                    ),
                )
            })?;

    let install_switch: Arc<dyn SwitchCertificateInstallNode> =
        Arc::new(SharedSwitchCertificateInstallNode {
            node_id: node_id.to_owned(),
            switch: Arc::clone(switch),
        });

    let mut progress = NvueCertificateBootstrapProgress {
        cancel,
        current_stage: None,
    };

    if let Err(failure) = install_switch_certificate_material(
        install_switch,
        &material,
        nvue_transition.as_ref(),
        &[SwitchMtlsService::NvueApi],
        cancel,
        &mut progress,
    )
    .await
    {
        let phase = progress
            .current_stage
            .map(SwitchCertificateInstallStage::name)
            .unwrap_or_else(|| "certificate installation".to_owned());

        return Err(RmsError::new(
            failure.error.code,
            format!(
                "NVUE certificate bootstrap failed during {phase}: {}",
                failure.error.message
            ),
        ));
    }

    if cancel.is_cancelled() {
        return Err(RmsError::cancelled(
            "NVUE certificate bootstrap cancelled after certificate verification",
        ));
    }

    if !nvue_client.uses_verified_client_tls() {
        return Err(RmsError::failed_precondition(
            "NVUE certificate post-install mTLS verification did not activate verified client TLS",
        ));
    }

    Ok(())
}

/// Build the switch system-image capability for a request-scoped switch.
pub(crate) fn create_switch_system_image_node(
    config: &NodeConfig,
    rack_id: &str,
) -> Result<Box<dyn SwitchSystemImageNode + Send + Sync>> {
    if config.node_type.kind() != NodeKind::Switch {
        return Err(RmsError::invalid_argument(format!(
            "unexpected switch node type {}",
            config.node_type
        )));
    }

    match NodeInstance::from_config(config, rack_id)? {
        NodeInstance::SwitchVrnvl72Nvidia(node) => {
            if !node.supports_bmc_aux_powercycle() {
                return Err(RmsError::failed_precondition(
                    "VR NVL72 NVOS updates require a BMC endpoint and credentials for activation",
                ));
            }
            Ok(node as Box<dyn SwitchSystemImageNode + Send + Sync>)
        }
        NodeInstance::SwitchGb200Nvidia(node) => {
            Ok(node as Box<dyn SwitchSystemImageNode + Send + Sync>)
        }
        NodeInstance::SwitchGb300Nvidia(node) => {
            Ok(node as Box<dyn SwitchSystemImageNode + Send + Sync>)
        }
        other => Err(RmsError::invalid_argument(format!(
            "unexpected switch node type {}",
            other.node_type()
        ))),
    }
}

// Timing constants - using different values for test mode vs. production mode.
#[cfg(not(test))]
mod timing {
    use std::time::Duration;
    pub const INSTALL_POLL_INTERVAL: Duration = Duration::from_secs(5);
    pub const INSTALL_COMPLETION_TIMEOUT: Duration = Duration::from_secs(10 * 60);
    pub const STATE_POLL_INTERVAL: Duration = Duration::from_secs(10);
    pub const UNINSTALL_POLL_INTERVAL: Duration = Duration::from_secs(5);
    pub const UNINSTALL_COMPLETION_TIMEOUT: Duration = Duration::from_secs(5 * 60);
    pub const INITIAL_STEADY_STATE_DELAY: Duration = Duration::from_secs(30);
    pub const STEADY_STATE_RETRY_WINDOW_1: Duration = Duration::from_secs(5 * 60);
    pub const STEADY_STATE_RETRY_WINDOW_2: Duration = Duration::from_secs(10 * 60);
    pub const STEADY_STATE_RETRY_WINDOW_3: Duration = Duration::from_secs(15 * 60);
    pub const RECOVERED_CREDENTIAL_SAVE_POLL_INTERVAL: Duration = Duration::from_secs(5);
}

#[cfg(test)]
mod timing {
    use std::time::Duration;
    pub const INSTALL_POLL_INTERVAL: Duration = Duration::from_millis(1);
    pub const INSTALL_COMPLETION_TIMEOUT: Duration = Duration::from_millis(50);
    pub const STATE_POLL_INTERVAL: Duration = Duration::from_millis(10);
    pub const UNINSTALL_POLL_INTERVAL: Duration = Duration::from_millis(1);
    pub const UNINSTALL_COMPLETION_TIMEOUT: Duration = Duration::from_millis(500);
    pub const INITIAL_STEADY_STATE_DELAY: Duration = Duration::from_millis(30);
    pub const STEADY_STATE_RETRY_WINDOW_1: Duration = Duration::from_millis(10);
    pub const STEADY_STATE_RETRY_WINDOW_2: Duration = Duration::from_millis(20);
    pub const STEADY_STATE_RETRY_WINDOW_3: Duration = Duration::from_millis(30);
    pub const RECOVERED_CREDENTIAL_SAVE_POLL_INTERVAL: Duration = Duration::from_millis(1);
}

use timing::*;

/// Sleep for `duration` unless `cancel` is signalled first. Returns `true`
/// if the sleep completed naturally and `false` if it was interrupted by
/// cancellation — the caller is expected to bail out in that case.
async fn sleep_unless_cancelled(duration: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => true,
        _ = cancel.cancelled() => false,
    }
}

/// Polls the NVUE uninstall job until it either completes or fails.
/// Wraps the actual work in an instrumented span for tracing.
async fn wait_for_uninstall_completion(
    switch: &(dyn SwitchSystemImageNode + Send + Sync),
    uninstall_job_id: &str,
    cancel: &CancellationToken,
    job_id: &str,
) -> Result<()> {
    use tracing::Instrument;
    let span = tracing::info_span!(
        "wait_for_uninstall_completion",
        job_id = job_id,
        uninstall_job_id = uninstall_job_id,
    );
    let result = wait_for_uninstall_completion_inner(switch, uninstall_job_id, cancel)
        .instrument(span.clone())
        .await;
    record_phase_result(&span, &result, false);
    result
}

/// Helper function for wait_for_uninstall_completion.
/// Doesn't need to be as complicated as wait_for_install_completion because we don't need to handle
/// reboot transitions or transient errors; uninstall should only take a few seconds.
async fn wait_for_uninstall_completion_inner(
    switch: &(dyn SwitchSystemImageNode + Send + Sync),
    uninstall_job_id: &str,
    cancel: &CancellationToken,
) -> Result<()> {
    let deadline = Instant::now() + UNINSTALL_COMPLETION_TIMEOUT;
    while Instant::now() < deadline {
        if cancel.is_cancelled() {
            return Err(RmsError::internal(
                "switch system image job cancelled during uninstall polling",
            ));
        }
        match switch.poll_firmware_task(uninstall_job_id, false).await {
            Err(e) => {
                return Err(e);
            }
            Ok(status) => {
                tracing::info!(
                    uninstall_job_id,
                    status = %describe_install_poll(&status),
                    "uninstall poll"
                );
                if is_uninstall_poll_success(&status) {
                    return Ok(());
                }
            }
        }
        if !sleep_unless_cancelled(UNINSTALL_POLL_INTERVAL, cancel).await {
            return Err(RmsError::internal(
                "switch system image job cancelled during uninstall polling",
            ));
        }
    }
    Err(RmsError::timeout(format!(
        "timed out waiting for switch system image uninstall job {uninstall_job_id}"
    )))
}
// Poll the NVUE install job until it either completes, hands off to reboot,
// or transitions the switch offline. On transient transport errors after the
// switch starts rebooting, or on HTTP 401 which we interpret as the switch
// being mid-boot, we return Ok(()) so the caller can move straight to the
// steady-state wait (fail fast on irrecoverable errors, tolerate reboot
// flakiness). VR NVL72 installs explicitly disable reboot: only action_success
// permits a subsequent BMC cycle; reboot-time errors are never success evidence.
async fn wait_for_install_completion(
    switch: &(dyn SwitchSystemImageNode + Send + Sync),
    install_job_id: &str,
    cancel: &CancellationToken,
    job_id: &str,
) -> Result<()> {
    use tracing::Instrument;
    let span = tracing::info_span!(
        "wait_for_install_completion",
        job_id = job_id,
        install_job_id = install_job_id,
    );
    let result = wait_for_install_completion_inner(switch, install_job_id, cancel)
        .instrument(span.clone())
        .await;
    record_phase_result(&span, &result, false);
    result
}

async fn wait_for_install_completion_inner(
    switch: &(dyn SwitchSystemImageNode + Send + Sync),
    install_job_id: &str,
    cancel: &CancellationToken,
) -> Result<()> {
    let deadline = Instant::now() + INSTALL_COMPLETION_TIMEOUT;
    let mut reboot_transition_observed = false;
    let bmc_activation = switch.system_image_requires_bmc_activation();

    while Instant::now() < deadline {
        if cancel.is_cancelled() {
            return Err(RmsError::internal(
                "switch system image job cancelled during install polling",
            ));
        }
        match switch
            .poll_firmware_task(install_job_id, bmc_activation)
            .await
        {
            Err(e) => {
                if is_http_unauthorized_error(&e) {
                    if bmc_activation {
                        return Err(e);
                    }
                    tracing::info!(
                        install_job_id,
                        error = %e.message,
                        "install poll hit HTTP 401 after install start; moving to steady-state wait"
                    );
                    return Ok(());
                }
                if reboot_transition_observed && is_retryable_install_poll_error(&e) {
                    tracing::info!(
                        install_job_id,
                        error = %e.message,
                        "install poll hit reboot-time action error after reboot transition; moving to steady-state wait"
                    );
                    return Ok(());
                }
                if is_retryable_install_poll_error(&e) {
                    tracing::info!(
                        install_job_id,
                        error = %e.message,
                        "install poll hit transient transport error"
                    );
                    if !sleep_unless_cancelled(INSTALL_POLL_INTERVAL, cancel).await {
                        return Err(RmsError::internal(
                            "switch system image job cancelled during install polling",
                        ));
                    }
                    continue;
                }
                return Err(e);
            }
            Ok(status) => {
                tracing::info!(
                    install_job_id,
                    status = %describe_install_poll(&status),
                    "install poll"
                );

                if !bmc_activation && is_install_reboot_transition_state(&status) {
                    tracing::info!(
                        install_job_id,
                        "install reports reboot transition; moving to steady-state wait"
                    );
                    return Ok(());
                }
                if status.state == "action_success"
                    || (!bmc_activation && is_install_poll_success(&status))
                {
                    return Ok(());
                }
                if !bmc_activation && is_install_handoff_to_reboot(&status) {
                    tracing::info!(
                        install_job_id,
                        "install reports completed handoff; moving to steady-state wait"
                    );
                    return Ok(());
                }
                if !bmc_activation
                    && status.state == "action_error"
                    && !status.completed
                    && is_install_progress_state(&status)
                {
                    reboot_transition_observed =
                        reboot_transition_observed || is_install_reboot_transition_state(&status);
                    if !sleep_unless_cancelled(INSTALL_POLL_INTERVAL, cancel).await {
                        return Err(RmsError::internal(
                            "switch system image job cancelled during install polling",
                        ));
                    }
                    continue;
                }
                if status.state == "action_error"
                    || status.state == "action_failed"
                    || (bmc_activation && status.completed)
                {
                    let msg = if !status.message.is_empty() {
                        status.message
                    } else if !status.status.is_empty() {
                        status.status
                    } else {
                        status.state
                    };
                    return Err(RmsError::internal(format!(
                        "switch system image install failed: {msg}"
                    )));
                }
            }
        }

        if !sleep_unless_cancelled(INSTALL_POLL_INTERVAL, cancel).await {
            return Err(RmsError::internal(
                "switch system image job cancelled during install polling",
            ));
        }
    }

    Err(RmsError::timeout(format!(
        "timed out waiting for switch system image install job {install_job_id}"
    )))
}

/// A staged NVOS install must select the requested build for the next boot
/// before RMS sends a single BMC reset. Never reset on an ambiguous install or
/// retry a failed reset submission: the switch may already be cycling.
async fn activate_staged_system_image_via_bmc(
    switch: &(dyn SwitchSystemImageNode + Send + Sync),
    target_build_id: &str,
    cancel: &CancellationToken,
) -> Result<SwitchSystemImageState> {
    let staged = cancel
        .run_until_cancelled(switch.get_normalized_system_image_state())
        .await
        .ok_or_else(|| RmsError::cancelled("NVOS boot-next verification cancelled"))??;
    if staged.next_build_id != target_build_id {
        return Err(RmsError::failed_precondition(format!(
            "NVOS install did not select target build {target_build_id} for next boot (reported '{}'); refusing BMC power cycle",
            staged.next_build_id
        )));
    }
    if cancel.is_cancelled() {
        return Err(RmsError::cancelled("NVOS BMC activation cancelled"));
    }
    switch.activate_system_image_via_bmc(cancel).await?;
    Ok(staged)
}

// Wait until the switch reports `target_build_id` on both its current and
// next boot partitions. Uses three progressively-longer retry windows
// (5 min, 10 min, 15 min) and attempts a one-time admin-password recovery
// on HTTP 401 (forced default-password change after reboot).
async fn wait_for_target_steady_state(
    switch: &mut (dyn SwitchSystemImageNode + Send + Sync),
    target_build_id: &str,
    target_user: &str,
    target_password: &str,
    cancel: &CancellationToken,
    job_id: &str,
) -> Result<SwitchSystemImageState> {
    use tracing::Instrument;
    let span = tracing::info_span!(
        "wait_for_target_steady_state",
        job_id = job_id,
        target_build_id = target_build_id,
    );
    let result = wait_for_target_steady_state_inner(
        switch,
        target_build_id,
        target_user,
        target_password,
        cancel,
    )
    .instrument(span.clone())
    .await;
    record_phase_result(&span, &result, true);
    result
}

async fn wait_for_target_steady_state_inner(
    switch: &mut (dyn SwitchSystemImageNode + Send + Sync),
    target_build_id: &str,
    target_user: &str,
    target_password: &str,
    cancel: &CancellationToken,
) -> Result<SwitchSystemImageState> {
    const RETRY_WINDOWS: [Duration; 3] = [
        STEADY_STATE_RETRY_WINDOW_1,
        STEADY_STATE_RETRY_WINDOW_2,
        STEADY_STATE_RETRY_WINDOW_3,
    ];

    tracing::info!(
        target_build_id,
        initial_delay_s = INITIAL_STEADY_STATE_DELAY.as_secs(),
        "steady-state wait: initial reboot grace period"
    );
    // Cancellable sleep: wakes up early if a cancellation is requested during
    // the 30-second grace period.
    tokio::select! {
        _ = tokio::time::sleep(INITIAL_STEADY_STATE_DELAY) => {}
        _ = cancel.cancelled() => {
            return Err(RmsError::internal(
                "switch system image job cancelled during steady-state grace period",
            ));
        }
    }

    let mut last_retryable_error: Option<String> = None;
    let mut password_recovery_attempted = false;

    for (idx, window) in RETRY_WINDOWS.iter().enumerate() {
        let deadline = Instant::now() + *window;
        tracing::info!(
            attempt = idx + 1,
            total_attempts = RETRY_WINDOWS.len(),
            window_s = window.as_secs(),
            target_build_id,
            "steady-state wait attempt"
        );

        while Instant::now() < deadline {
            if cancel.is_cancelled() {
                return Err(RmsError::internal(
                    "switch system image job cancelled while waiting for steady state",
                ));
            }
            match switch.get_normalized_system_image_state().await {
                Ok(state) => {
                    if state.current_build_id == target_build_id
                        && state.next_build_id == target_build_id
                    {
                        return Ok(state);
                    }
                }
                Err(e) => {
                    if !password_recovery_attempted
                        && target_user == "admin"
                        && is_http_unauthorized_error(&e)
                    {
                        tracing::info!(
                            attempt = idx + 1,
                            "steady-state wait saw HTTP 401 after reboot; attempting admin password recovery"
                        );

                        password_recovery_attempted = true;
                        switch
                            .recover_admin_password_after_boot(target_password)
                            .await
                            .map_err(|e| {
                                if e.code == ErrorCode::Unauthenticated {
                                    RmsError::new(
                                        e.code,
                                        "post-boot admin recovery failed: recovery credential mismatch",
                                    )
                                } else {
                                    e
                                }
                            })?;

                        tracing::info!(
                            attempt = idx + 1,
                            "steady-state wait repaired post-reboot credentials"
                        );
                    } else if is_retryable_steady_state_error(&e) {
                        last_retryable_error = Some(e.message.clone());
                        tracing::info!(
                            attempt = idx + 1,
                            error = %e.message,
                            "steady-state wait saw transient reboot error"
                        );
                    } else {
                        return Err(e);
                    }
                }
            }

            if !sleep_unless_cancelled(STATE_POLL_INTERVAL, cancel).await {
                return Err(RmsError::internal(
                    "switch system image job cancelled while waiting for steady state",
                ));
            }
        }

        if idx + 1 < RETRY_WINDOWS.len() {
            tracing::warn!(
                attempt = idx + 1,
                total_attempts = RETRY_WINDOWS.len(),
                target_build_id,
                "steady-state wait attempt did not converge; retrying with larger window"
            );
        }
    }

    let mut msg =
        format!("timed out waiting for switch to boot target build-id '{target_build_id}'");
    if let Some(err) = last_retryable_error {
        msg.push_str(&format!(" after transient reboot error(s): {err}"));
    }
    Err(RmsError::timeout(msg))
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_update_switch_system_image(
        &self,
        req: tonic::Request<rm::UpdateSwitchSystemImageRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateSwitchSystemImageResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let allow_onie_recovery = r.allow_onie_recovery;

        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::new(),
            job_id: String::new(),
            stats: None,
        };
        let mut jobs: Vec<rm::SwitchSystemImageUpdateJobInfo> = Vec::new();

        let nodes: Vec<rm::NodeInfo> = r.nodes.map(|n| n.nodes).unwrap_or_default();
        let total_nodes = nodes.len() as u32;
        if nodes.is_empty() {
            batch.message = "no nodes specified in request".into();
            tracing::warn!("{}", batch.message);
            return Ok(tonic::Response::new(update_switch_system_image_response(
                batch,
                jobs,
                total_nodes,
                0,
                0,
            )));
        }

        if r.image_filename.is_empty() {
            batch.message = "image_filename is required".into();
            tracing::warn!(total_nodes = %total_nodes, "{}", batch.message);
            return Ok(tonic::Response::new(update_switch_system_image_response(
                batch,
                jobs,
                total_nodes,
                0,
                0,
            )));
        }

        if r.local_file_path.is_empty() {
            batch.message = "local_file_path is required".into();
            tracing::warn!(total_nodes = %total_nodes, "{}", batch.message);
            return Ok(tonic::Response::new(update_switch_system_image_response(
                batch,
                jobs,
                total_nodes,
                0,
                0,
            )));
        }

        let resolved_local_file_path =
            match resolve_firmware_file(&r.local_file_path, &self.firmware_dir) {
                Ok(path) => path,
                Err(e) => {
                    batch.message = format!("invalid local_file_path: {e}");
                    tracing::error!(local_file_path = %r.local_file_path, "{}", batch.message);
                    return Ok(tonic::Response::new(update_switch_system_image_response(
                        batch,
                        jobs,
                        total_nodes,
                        0,
                        0,
                    )));
                }
            };

        if !resolved_local_file_path.is_file() {
            batch.message = format!(
                "local_file_path does not exist or is not a file: {}",
                resolved_local_file_path.display()
            );
            tracing::error!(
                local_file_path = %resolved_local_file_path.display(),
                "{}",
                batch.message
            );
            return Ok(tonic::Response::new(update_switch_system_image_response(
                batch,
                jobs,
                total_nodes,
                0,
                0,
            )));
        }
        let local_file_path = resolved_local_file_path.to_string_lossy().into_owned();

        let target_build_id = match infer_target_build_id(&r.image_filename) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(image_filename = %r.image_filename, error = %e.message, "failed to infer target build id");
                batch.message = e.message;
                return Ok(tonic::Response::new(update_switch_system_image_response(
                    batch,
                    jobs,
                    total_nodes,
                    0,
                    0,
                )));
            }
        };

        let parent_rack_id = nodes[0].rack_id.as_str();
        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(parent_rack_id, JobType::SwitchSystemImageUpdate)
        else {
            batch.message = "failed to create parent switch system image job".into();
            return Ok(tonic::Response::new(update_switch_system_image_response(
                batch,
                jobs,
                total_nodes,
                0,
                total_nodes,
            )));
        };
        batch.job_id = parent_id.clone();

        let mut skipped = 0u32;
        let mut admitted = Vec::new();

        for node_info in nodes {
            let node_type_key = node_info.r#type.unwrap_or(0);
            let Some(node_type) = proto_node_type_to_domain(node_type_key) else {
                let err_msg = format!(
                    "node {} has unsupported node type for switch system image update (type={})",
                    node_info.node_id,
                    proto_node_type_to_string(node_type_key).unwrap_or("unknown")
                );
                tracing::warn!(node = node_info.node_id, "{err_msg}");
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_info.node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: err_msg,
                });
                skipped += 1;
                continue;
            };
            if node_type.kind() != NodeKind::Switch {
                let err_msg = format!(
                    "node {} is not a switch (type={})",
                    node_info.node_id,
                    proto_node_type_to_string(node_type_key).unwrap_or("unknown")
                );
                tracing::warn!(node = node_info.node_id, "{err_msg}");
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_info.node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: err_msg,
                });
                skipped += 1;
                continue;
            }

            let recovery =
                match SwitchImageRecoveryOptions::from_node_info(&node_info, allow_onie_recovery) {
                    Ok(recovery) => recovery,
                    Err(error) => {
                        tracing::warn!(
                            node = node_info.node_id,
                            rack = node_info.rack_id,
                            error = error.message,
                            "invalid ONIE recovery endpoint"
                        );

                        batch.node_results.push(rm::NodeOperationResult {
                            node_id: node_info.node_id.clone(),
                            status: rm::ReturnCode::Failure.into(),
                            error_message: error.message,
                        });

                        skipped += 1;
                        continue;
                    }
                };

            let flat = match flatten_node_info(&node_info) {
                Ok(flat) => flat,
                Err(e) => {
                    let message = e.message;
                    tracing::error!(
                        node = node_info.node_id,
                        rack = node_info.rack_id,
                        error = message,
                        "invalid endpoint credentials"
                    );

                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_info.node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: message,
                    });

                    skipped += 1;
                    continue;
                }
            };

            // Switches authenticate NVUE/SSH via the host endpoint.
            let Some((user, pass)) = flat.creds_for_node_type(node_type) else {
                let err_msg = format!("missing host credentials for switch {}", node_info.node_id);
                tracing::error!(node = node_info.node_id, error = %err_msg, "missing host credentials");
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_info.node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: err_msg.clone(),
                });
                skipped += 1;
                continue;
            };
            let user = user.to_owned();
            let pass = pass.to_owned();

            // Switch image updates run through host/NVUE and SSH. Require the
            // host endpoint for the work. Invalid optional BMC data is dropped;
            // VR NVL72 is then rejected by capability construction because its
            // activation requires a usable BMC endpoint.
            let bmc_endpoint = match flat.optional_bmc_endpoint(self.insecure_bmc) {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    tracing::warn!(
                        node = node_info.node_id,
                        rack = node_info.rack_id,
                        error = e.message,
                        "ignoring malformed BMC endpoint for switch image update"
                    );

                    None
                }
            };
            let host_endpoint = match flat.switch_host_management_endpoint() {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    let err_msg = format!(
                        "invalid host endpoint for switch {}: {}",
                        node_info.node_id, e.message
                    );
                    tracing::error!(node = node_info.node_id, error = %e.message, "invalid host endpoint for switch");
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_info.node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: err_msg,
                    });
                    skipped += 1;
                    continue;
                }
            };
            let config = NodeConfig {
                id: node_info.node_id.clone(),
                node_type,
                bmc_endpoint,
                host_endpoint: Some(host_endpoint),
                expected_inventory: None,
                recovery: None,
                runtime_config: None,
            };
            let rack_id = node_info.rack_id.clone();
            let node_id = node_info.node_id.clone();

            let switch_result = create_switch_system_image_node(&config, &rack_id);
            let switch = match switch_result {
                Ok(s) => s,
                Err(e) => {
                    let err_msg = format!(
                        "failed to construct switch {}: {}",
                        node_info.node_id, e.message
                    );
                    tracing::error!(node = node_info.node_id, error = %e.message, "failed to construct switch");
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: err_msg,
                    });
                    skipped += 1;
                    continue;
                }
            };

            let pending = match self.job_tracker.create_child_job(
                &parent_id,
                &rack_id,
                &node_id,
                JobType::SwitchSystemImageUpdate,
            ) {
                Ok(pending) => pending,
                Err(failure) => {
                    tracing::warn!(
                        node = node_id,
                        rack = rack_id,
                        message = %failure.message,
                        "switch image job rejected"
                    );
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: failure.message,
                    });
                    skipped += 1;
                    continue;
                }
            };

            admitted.push(AdmittedSwitchImageJob {
                pending,
                switch,
                node_type: config.node_type,
                recovery,
                rack_id,
                node_id,
                user,
                pass,
            });
        }

        // Admit every child before workers can finish and refresh the parent.
        // Otherwise an early setup failure could seal the parent while later
        // nodes are still being admitted.
        for admitted_job in admitted {
            jobs.push(rm::SwitchSystemImageUpdateJobInfo {
                node_id: admitted_job.node_id.clone(),
                job_id: admitted_job.pending.id().to_string(),
            });

            let pending = admitted_job.pending;
            let cancel = pending.cancellation_token();
            let switch = Arc::new(tokio::sync::Mutex::new(admitted_job.switch));
            let service = self.clone();
            let image_filename = r.image_filename.clone();
            let local_file_path = local_file_path.clone();
            let target_build_id = target_build_id.clone();
            let sftp_upload_options = self.sftp_upload_options;
            let readiness = self.switch_readiness_policy(admitted_job.node_type);

            self.job_tracker
                .spawn_job(pending, move |job| async move {
                    run_switch_system_image_job(
                        &service,
                        job,
                        switch,
                        &image_filename,
                        &local_file_path,
                        &target_build_id,
                        &admitted_job.user,
                        &admitted_job.pass,
                        &admitted_job.rack_id,
                        &admitted_job.node_id,
                        admitted_job.recovery,
                        readiness,
                        sftp_upload_options,
                        &cancel,
                    )
                    .await;
                })
                .detach();
        }

        let jobs_created = jobs.len() as u32;

        if jobs_created == 0 {
            batch.status = rm::ReturnCode::Failure.into();
            batch.message = "no switch system image jobs created".into();
            self.job_tracker
                .mark_failed_message(&parent_id, &batch.message);

            tracing::error!(total_nodes, "{}", batch.message);

            return Ok(tonic::Response::new(update_switch_system_image_response(
                batch,
                jobs,
                total_nodes,
                0,
                skipped,
            )));
        }

        batch.status = if skipped == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();

        tracing::info!(job_id = %batch.job_id, total_nodes, jobs_created, "created switch system image jobs");
        batch.message = format!(
            "Created {jobs_created} switch system image jobs out of {total_nodes} nodes. \
             Use GetSwitchSystemImageJobStatus with job_id to track overall batch status."
        );
        Ok(tonic::Response::new(update_switch_system_image_response(
            batch,
            jobs,
            total_nodes,
            jobs_created,
            skipped,
        )))
    }

    pub(crate) async fn handle_get_switch_system_image_job_status(
        &self,
        req: tonic::Request<rm::GetSwitchSystemImageJobStatusRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::GetSwitchSystemImageJobStatusResponse>,
        tonic::Status,
    > {
        let r = req.into_inner();
        let mut resp = rm::GetSwitchSystemImageJobStatusResponse {
            status: rm::ReturnCode::Failure.into(),
            job_id: r.job_id.clone(),
            state: String::new(),
            message: String::new(),
            rack_id: String::new(),
            node_id: String::new(),
            error_message: String::new(),
            result_json: String::new(),
            created_at: None,
            updated_at: None,
        };

        if r.job_id.is_empty() {
            resp.message = "job_id is required".into();
            return Ok(tonic::Response::new(resp));
        }

        let Some(info) = self.job_tracker.get_job(&r.job_id) else {
            resp.message = format!("job {} not found", r.job_id);
            return Ok(tonic::Response::new(resp));
        };

        resp.status = rm::ReturnCode::Success.into();
        resp.job_id = info.job_id;
        resp.state = info.state.as_str().to_owned();
        resp.message = info.state_description;
        resp.rack_id = info.rack_id;
        resp.node_id = info.node_id;
        resp.error_message = info.error_message;
        resp.result_json = info.result_json;
        resp.created_at = Some(timestamp_from_datetime(info.created_at));
        resp.updated_at = Some(timestamp_from_datetime(info.updated_at));
        Ok(tonic::Response::new(resp))
    }
}

impl RackManagerServiceImpl {
    /// The budget for the readiness poll after the image reboot: the same
    /// verification policy the firmware job's version check takes from the
    /// node's profile, `None` when the node type has no request-matching
    /// profile.
    pub(crate) fn switch_readiness_policy(
        &self,
        node_type: NodeType,
    ) -> Option<VerificationPolicy> {
        self.hardware_profiles
            .shared_for_request(node_type)
            .map(|profile| profile.verification())
    }
}

/// Runs the readiness stage at the timeline cursor: starts it, polls the
/// switch, and completes it. On failure returns the outcome, message, and
/// details for `fail_stage`, which the caller owns because its before and
/// after arguments differ between the install path and the recovered path.
async fn run_readiness_stage(
    timeline: &mut StageTimeline,
    job: &RmsJobHandle,
    switch: &SharedSwitchSystemImageNode,
    cancel: &CancellationToken,
    job_id: &str,
    readiness: Option<VerificationPolicy>,
    target_build_id: &str,
) -> std::result::Result<(), (&'static str, String, serde_json::Value)> {
    if cancel.is_cancelled() {
        let stage = timeline.current_name().unwrap_or("verify_system_ready");
        return Err((
            "cancelled",
            format!("job cancelled before stage {stage}"),
            serde_json::json!({"cancelled_before_start": true}),
        ));
    }
    let (stage, description) = timeline.start_stage().map_err(|e| {
        (
            "failed",
            "stage list exhausted".to_owned(),
            serde_json::json!({"error": e.message}),
        )
    })?;
    job.progress(description);
    match wait_for_switch_system_ready(switch, cancel, job_id, readiness).await {
        Ok(()) => {
            timeline.complete_current(serde_json::json!({}));
            tracing::debug!(job_id, stage = %stage, "stage completed: NVOS ready and healthy");
            Ok(())
        }
        Err(e) => {
            let was_cancelled = e.code == ErrorCode::Cancelled;
            let outcome = if was_cancelled { "cancelled" } else { "failed" };
            Err((
                outcome,
                format!(
                    "NVOS readiness check {outcome} (build_id={target_build_id}): {}",
                    e.message
                ),
                serde_json::json!({"error": e.message, "cancelled": was_cancelled}),
            ))
        }
    }
}

/// Waits for the switch that came back on the target image to report its
/// operating system ready and healthy, on the node profile's verification
/// budget when it has one.
async fn wait_for_switch_system_ready(
    switch: &SharedSwitchSystemImageNode,
    cancel: &CancellationToken,
    job_id: &str,
    readiness: Option<VerificationPolicy>,
) -> Result<()> {
    use tracing::Instrument;

    let sw = switch.lock().await;

    wait_for_system_ready(|| sw.verify_system_ready(), cancel, readiness)
        .instrument(tracing::info_span!(
            "wait_for_switch_system_ready",
            job_id = job_id
        ))
        .await
}

fn update_switch_system_image_response(
    mut batch: rm::NodeBatchResponse,
    jobs: Vec<rm::SwitchSystemImageUpdateJobInfo>,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) -> rm::UpdateSwitchSystemImageResponse {
    batch.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });

    rm::UpdateSwitchSystemImageResponse {
        response: Some(batch),
        jobs,
    }
}

// Top-level per-node orchestrator for UpdateSwitchSystemImage. Setup verifies
// or recovers NVOS, initializes NVUE, and persists recovered credentials before
// image inspection, staging, installation, and verification. VR activation uses
// the BMC after staged installation succeeds; other switches use reboot handoff.

/// Predefined, ordered stage sequence driving `run_switch_system_image_job`.
/// Each `(name, description)` pair seeds a `Stage`'s name and immutable,
/// imperative description once, here, instead of retyping either at every
/// `timeline`/`job.progress`/`tracing` call site below.
fn switch_system_image_job_stages(bmc_activation: bool) -> Vec<Stage> {
    let mut stages = vec![
        (
            "verify_nvos_installation",
            "Verify the NVOS installation state",
        ),
        ("recover_nvos", "Recover NVOS through ONIE when required"),
        (
            "bootstrap_nvue_certificate",
            "Configure NVUE certificate when required",
        ),
        ("initialize_nvue", "Initialize the NVUE client"),
        (
            "persist_recovered_credentials",
            "Persist credentials established during NVOS recovery",
        ),
        ("inspect_state", "Inspect current switch system image state"),
        (
            "verify_asic_auto_update",
            "Verify and heal ASIC firmware auto-update configuration",
        ),
        ("cleanup_old_partition", "Clean up unused partition image"),
        (
            "stage_image",
            "Ensure switch system image is staged on target",
        ),
        ("trigger_install", "Trigger switch system image install"),
    ];
    if bmc_activation {
        stages.extend([
            (
                "wait_for_install_completion",
                "Wait for staged NVOS install to succeed",
            ),
            (
                "activate_via_bmc",
                "Verify NVOS boot-next and power-cycle through the BMC",
            ),
        ]);
    } else {
        stages.push((
            "wait_for_install_handoff",
            "Wait for switch system image install to start reboot",
        ));
    }
    stages.push((
        "wait_for_target_steady_state",
        "Wait for switch to return with target system image",
    ));
    stages.push((
        "verify_system_ready",
        "Verify switch reports its operating system ready and healthy",
    ));
    stages
        .into_iter()
        .map(|(name, description)| Stage::new(name.to_owned(), description.to_owned()))
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn run_switch_system_image_setup(
    service: &RackManagerServiceImpl,
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    switch: &SharedSwitchSystemImageNode,
    recovery: &SwitchImageRecoveryOptions,
    local_file_path: &str,
    image_filename: &str,
    sftp_upload_options: SshUploadOptions,
    cancel: &CancellationToken,
) -> Result<bool> {
    let start_stage = |timeline: &mut StageTimeline| -> Result<()> {
        if cancel.is_cancelled() {
            let stage = timeline.current_name().unwrap_or("unknown_stage");

            return Err(RmsError::cancelled(format!(
                "switch system image job cancelled before {stage}"
            )));
        }

        let (_, description) = timeline.start_stage()?;

        job.progress(description);

        Ok(())
    };

    start_stage(timeline)?;
    let installation_state = {
        let switch = switch.lock().await;

        // Prepare site TLS policy when possible; the REST probe decides whether
        // the current transport works before considering authorized SSH fallback.
        if let Err(error) = service
            .initialize_nvue_client(switch.nvue_client(), None)
            .await
        {
            tracing::warn!(
                node = %timeline.node(),
                error = %error.message,
                "NVUE initialization failed; probing the current transport"
            );
        }

        switch
            .verify_nvos_installation(recovery.onie_endpoint.as_ref())
            .await?
    };

    timeline.complete_current(serde_json::json!({
        "installation_state": match installation_state {
            NvosInstallationState::Installed => "installed",
            NvosInstallationState::NotInstalled => "not_installed",
        },
    }));

    start_stage(timeline)?;
    let recovered_nvos = match installation_state {
        NvosInstallationState::Installed => {
            timeline.skip_current(serde_json::json!({"reason": "NVOS is already installed"}));
            false
        }
        NvosInstallationState::NotInstalled => {
            let onie_endpoint = recovery.onie_endpoint.as_ref().ok_or_else(|| {
                RmsError::internal(
                    "NVOS probe reported ONIE without a configured recovery endpoint",
                )
            })?;

            if !service.switch_tls_roots.insecure_switch {
                let preflight_error = |message| {
                    RmsError::failed_precondition(format!(
                        "ONIE recovery certificate preflight failed before image installation: {message}"
                    ))
                };

                service
                    .switch_tls_roots
                    .resolve_switch_cert(None)
                    .map_err(preflight_error)?;

                service
                    .switch_tls_roots
                    .resolve_required_nvue_client_tls(None)
                    .map_err(preflight_error)?;
            }

            {
                let mut switch = switch.lock().await;

                recover_nvos_through_onie(
                    switch.as_mut(),
                    onie_endpoint,
                    local_file_path,
                    image_filename,
                    sftp_upload_options,
                    service.workflow_timings.onie_recovery_timeout,
                    cancel,
                )
                .await?;
            }

            timeline.complete_current(serde_json::json!({"recovered": true}));
            true
        }
    };

    start_stage(timeline)?;
    if !recovered_nvos {
        timeline.skip_current(serde_json::json!({"reason": "NVOS recovery was not required"}));
    } else if service.switch_tls_roots.insecure_switch {
        timeline.skip_current(serde_json::json!({"reason": "insecure_switch is enabled"}));
    } else {
        bootstrap_nvue_certificate(service, switch, timeline.node(), cancel).await?;
        timeline.complete_current(serde_json::json!({
            "configured_services": [SwitchMtlsService::NvueApi.as_str()],
            "verified_mtls": true,
        }));
    }

    start_stage(timeline)?;
    {
        let switch = switch.lock().await;

        service
            .initialize_nvue_client(switch.nvue_client(), None)
            .await?;
    }

    timeline.complete_current(serde_json::json!({}));

    start_stage(timeline)?;
    if recovered_nvos {
        let switch = switch.lock().await;

        persist_recovered_nvos_credentials(
            switch.as_ref(),
            service.workflow_timings.recovered_credential_save_timeout,
            cancel,
        )
        .await?;

        timeline.complete_current(serde_json::json!({"persisted": true}));
    } else {
        timeline.skip_current(serde_json::json!({"reason": "NVOS recovery was not required"}));
    }

    Ok(recovered_nvos)
}

/// Builds the `StageTimeline` driving `run_switch_system_image_job`, tagged
/// with the node/rack it runs against so `StageTimeline`'s own stage
/// transition logs carry them without repeating them at every call site.
/// Factored out of `run_switch_system_image_job` so this wiring -- easy to
/// silently drop when refactoring -- has a direct unit test.
fn switch_system_image_job_timeline(
    job_id: &str,
    rack_id: &str,
    node_id: &str,
    bmc_activation: bool,
) -> StageTimeline {
    StageTimeline::with_stages(job_id, switch_system_image_job_stages(bmc_activation))
        .with_node_rack(node_id, rack_id)
}

/// Fails the stage at `timeline`'s cursor, marks every stage after it
/// `skipped` (since the job is about to exit early), builds the final
/// `result_json` (with the full timing_summary) via `build_result_json`,
/// and seals the job as failed. Collapses what would otherwise be a
/// `timeline.fail` + `build_result_json` + `job.fail` block, repeated at
/// every stage boundary, into a single call followed by `return`. A thin,
/// job-specific wrapper around `stage_timeline::fail_job` -- see that
/// function's docs for why the shared sequence lives there and not here.
///
/// `build_result_json` is threaded in rather than captured, since the
/// caller's closure closes over job-specific context (`rack_id`,
/// `image_filename`, `local_image_size_bytes`, etc.) that this
/// job-control-flow helper has no need to know about.
#[allow(clippy::too_many_arguments)]
fn fail_stage(
    timeline: &mut StageTimeline,
    job: RmsJobHandle,
    outcome: &str,
    error: &str,
    details: serde_json::Value,
    install_job_id: &Option<String>,
    before: &Option<SwitchSystemImageState>,
    after: &Option<SwitchSystemImageState>,
    image_visible_before_stage: bool,
    image_visible_after_stage: bool,
    build_result_json: &impl Fn(
        &str,
        &Option<String>,
        &Option<SwitchSystemImageState>,
        &Option<SwitchSystemImageState>,
        bool,
        bool,
        &StageTimeline,
    ) -> String,
) {
    fail_job(
        timeline,
        job,
        outcome,
        error,
        details,
        |outcome, timeline| {
            build_result_json(
                outcome,
                install_job_id,
                before,
                after,
                image_visible_before_stage,
                image_visible_after_stage,
                timeline,
            )
        },
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_switch_system_image_job(
    service: &RackManagerServiceImpl,
    job: RmsJobHandle,
    switch: SharedSwitchSystemImageNode,
    image_filename: &str,
    local_file_path: &str,
    target_build_id: &str,
    switch_username: &str,
    switch_password: &str,
    rack_id: &str,
    node_id: &str,
    recovery: SwitchImageRecoveryOptions,
    readiness: Option<VerificationPolicy>,
    sftp_upload_options: SshUploadOptions,
    cancel: &CancellationToken,
) {
    // Copy the ID out so the many `job_id` log fields keep working without
    // holding an immutable borrow on `job`, which is moved when the worker
    // seals its terminal state via `complete`/`fail`.
    let job_id_owned = job.id().to_string();
    let job_id = job_id_owned.as_str();
    let bmc_activation = switch.lock().await.system_image_requires_bmc_activation();
    let mut timeline = switch_system_image_job_timeline(job_id, rack_id, node_id, bmc_activation);
    let local_image_size_bytes = std::fs::metadata(local_file_path).ok().map(|m| m.len());

    // Builds the final `result_json` embedded in the job's `complete` /
    // `fail` seal. Includes the full stage timeline so clients can see
    // where time was spent across inspect / stage / install / handoff /
    // steady-state.
    let build_result_json = |status: &str,
                             install_job_id: &Option<String>,
                             before: &Option<SwitchSystemImageState>,
                             after: &Option<SwitchSystemImageState>,
                             image_visible_before_stage: bool,
                             image_visible_after_stage: bool,
                             timeline: &StageTimeline|
     -> String {
        let mut result = serde_json::json!({
            "rack_id": rack_id,
            "node_id": node_id,
            "image_filename": image_filename,
            "target_build_id": target_build_id,
            "allow_onie_recovery": recovery.allow_onie_recovery,
            "onie_recovery_endpoint_configured": recovery.onie_endpoint.is_some(),
            "status": status,
            "image_visible_before_stage": image_visible_before_stage,
            "image_visible_after_stage": image_visible_after_stage,
            "timing_summary": timeline.to_json(),
        });
        if let Some(size) = local_image_size_bytes {
            insert_json_field(&mut result, "image_size_bytes", serde_json::json!(size));
        }
        if let Some(id) = install_job_id {
            insert_json_field(&mut result, "install_job_id", serde_json::json!(id));
        }
        if let Some(state) = before {
            insert_json_field(&mut result, "before", system_image_state_to_json(state));
        }
        if let Some(state) = after {
            insert_json_field(&mut result, "after", system_image_state_to_json(state));
        }
        result.to_string()
    };

    // Returns a cancellation message for the stage at the timeline's cursor
    // if `cancel` has fired, without touching the timeline itself --
    // `fail_stage` below is the single place that actually records a
    // failure, so cancellation and genuine errors both flow through it.
    let check_cancel = |timeline: &StageTimeline| -> Option<String> {
        if cancel.is_cancelled() {
            let stage = timeline.current_name().unwrap_or("unknown_stage");
            Some(format!("switch system image job cancelled before {stage}"))
        } else {
            None
        }
    };

    tracing::info!(
        job_id,
        rack_id,
        node_id,
        image_filename,
        target_build_id,
        local_file_path,
        image_size_bytes = local_image_size_bytes,
        "switch system image job starting"
    );

    let setup_result = run_switch_system_image_setup(
        service,
        &job,
        &mut timeline,
        &switch,
        &recovery,
        local_file_path,
        image_filename,
        sftp_upload_options,
        cancel,
    )
    .await;

    let recovered_nvos = match setup_result {
        Ok(recovered_nvos) => recovered_nvos,
        Err(error) => {
            let outcome = if error.code == ErrorCode::Cancelled {
                "cancelled"
            } else {
                "failed"
            };

            // Setup failures share one policy across switch-image RPCs;
            // cancellation follows the later-stage path because JobError has no
            // dedicated cancellation variant.
            let error_code = match error.code {
                ErrorCode::FailedPrecondition => JobError::FailedPrecondition,
                ErrorCode::Internal => JobError::Internal,
                ErrorCode::Cancelled => JobError::Other,
                _ => JobError::ClientError,
            };

            timeline.fail_current(
                outcome == "cancelled",
                &error.message,
                serde_json::json!({"error": &error.message}),
            );

            timeline.skip_remaining();

            let result_json =
                build_result_json(outcome, &None, &None, &None, false, false, &timeline);

            job.fail(JobFailure::new(error_code, error.message).with_result_json(result_json));

            return;
        }
    };

    // ── Inspect current state ──
    if let Some(err_msg) = check_cancel(&timeline) {
        fail_stage(
            &mut timeline,
            job,
            "cancelled",
            &err_msg,
            serde_json::json!({"cancelled_before_start": true}),
            &None,
            &None,
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    }
    // Starts the next predefined stage, or fails the job and returns if
    // the predefined stage list and this function's control flow have
    // drifted out of sync -- a programming error, not a runtime condition.
    // Without this, `start_stage()` running past the end of the list would
    // panic through `.expect(...)`; the job supervisor's Drop-based safety
    // net (see `JobHandle`'s `Drop` impl) still catches that and marks the
    // job failed, but only with a generic "job abandoned" message and a
    // noisy panic backtrace, dropping the timeline's partial progress that
    // `fail_stage` would otherwise record.
    //
    // The `before`/`after`/image-visibility context `fail_stage` normally
    // reports is deliberately left at its "unknown" default here rather
    // than threaded in from each call site: this path only fires for an
    // internal consistency bug, where a clean, low-detail failure is far
    // more valuable than perfectly preserving in-flight state snapshots.
    let Ok((stage, description)) = timeline.start_stage() else {
        fail_stage(
            &mut timeline,
            job,
            "failed",
            "stage list exhausted",
            serde_json::json!({}),
            &None,
            &None,
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    };
    job.progress(description);

    let before_state = {
        let sw = switch.lock().await;
        sw.as_ref().get_normalized_system_image_state().await
    };
    let before_state = match before_state {
        Ok(s) => s,
        Err(e) => {
            let error_message = format!("could not fetch /nvue_v1/system/image: {}", e.message);
            fail_stage(
                &mut timeline,
                job,
                "failed",
                &error_message,
                serde_json::json!({"error": e.message}),
                &None,
                &None,
                &None,
                false,
                false,
                &build_result_json,
            );
            return;
        }
    };
    tracing::info!(
        job_id,
        stage = %stage,
        current_build_id = %before_state.current_build_id,
        next_build_id = %before_state.next_build_id,
        current_partition = %before_state.current_partition,
        next_partition = %before_state.next_partition,
        "current switch system image state"
    );
    timeline.complete_current(system_image_state_to_json(&before_state));

    // ── Early-exit: already at target on both partitions ──
    let target_is_active = before_state.current_build_id == target_build_id
        && before_state.next_build_id == target_build_id;

    if target_is_active && !recovered_nvos {
        tracing::info!(
            job_id,
            target_build_id,
            "switch is already up-to-date on both partitions; skipping install"
        );
        timeline.skip_remaining();
        let result_json = build_result_json(
            "already_up_to_date",
            &None,
            &Some(before_state.clone()),
            &None,
            false,
            false,
            &timeline,
        );
        job.complete_verified("Completed", result_json);
        return;
    }

    // ── Verify and heal ASIC firmware auto-update configuration ──
    if let Some(err_msg) = check_cancel(&timeline) {
        fail_stage(
            &mut timeline,
            job,
            "cancelled",
            &err_msg,
            serde_json::json!({"cancelled_before_start": true}),
            &None,
            &Some(before_state.clone()),
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    }
    let Ok((stage, description)) = timeline.start_stage() else {
        fail_stage(
            &mut timeline,
            job,
            "failed",
            "stage list exhausted",
            serde_json::json!({}),
            &None,
            &None,
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    };
    job.progress(description);

    let asic_preflight = {
        let sw = switch.lock().await;
        sw.as_ref().ensure_nvos_asic_firmware_config().await
    };
    if let Err(e) = asic_preflight {
        let error_message = format!(
            "ASIC firmware auto-update preflight failed: {}; collect Switch Tray logs before retrying",
            e.message
        );
        fail_stage(
            &mut timeline,
            job,
            "failed",
            &error_message,
            serde_json::json!({"error": e.message}),
            &None,
            &Some(before_state),
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    }
    timeline.complete_current(serde_json::json!({
        "auto-update": "enabled",
        "fw-source": "default",
    }));
    tracing::debug!(job_id, stage = %stage, "ASIC firmware auto-update configuration is ready");

    if recovered_nvos && target_is_active {
        tracing::info!(
            job_id,
            target_build_id,
            "ONIE recovery installed the target image and ASIC configuration is ready"
        );

        // The recovered image booted fresh, so it meets the same readiness
        // criterion as an installed one. The install stages between do not
        // apply and are skipped up to the readiness stage, which is last.
        while timeline
            .current_name()
            .is_some_and(|name| name != "verify_system_ready")
        {
            timeline.skip_current(serde_json::json!({"recovered": true}));
        }
        if let Err((outcome, error_message, details)) = run_readiness_stage(
            &mut timeline,
            &job,
            &switch,
            cancel,
            job_id,
            readiness,
            target_build_id,
        )
        .await
        {
            fail_stage(
                &mut timeline,
                job,
                outcome,
                &error_message,
                details,
                &None,
                &Some(before_state),
                &None,
                false,
                false,
                &build_result_json,
            );
            return;
        }

        let result_json = build_result_json(
            "recovered",
            &None,
            &Some(before_state),
            &None,
            false,
            false,
            &timeline,
        );

        job.complete_verified("Completed", result_json);
        return;
    }

    // ── Clean up unused partition image ──
    if let Some(err_msg) = check_cancel(&timeline) {
        fail_stage(
            &mut timeline,
            job,
            "cancelled",
            &err_msg,
            serde_json::json!({"cancelled_before_start": true}),
            &None,
            &Some(before_state.clone()),
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    }
    let Ok((stage, description)) = timeline.start_stage() else {
        fail_stage(
            &mut timeline,
            job,
            "failed",
            "stage list exhausted",
            serde_json::json!({}),
            &None,
            &None,
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    };
    job.progress(description);

    let cleanup_plan = match plan_unused_partition_cleanup(&before_state, target_build_id) {
        Ok(plan) => plan,
        Err(e) => {
            fail_stage(
                &mut timeline,
                job,
                "failed",
                &e.message,
                serde_json::json!({"error": e.message}),
                &None,
                &Some(before_state),
                &None,
                false,
                false,
                &build_result_json,
            );
            return;
        }
    };

    match cleanup_plan {
        UnusedPartitionCleanup::NoUnusedImage => {
            tracing::info!(
                job_id,
                "Stage {stage} skipped; no unused partition image to clean up"
            );
            timeline.skip_current(serde_json::json!({}));
        }
        UnusedPartitionCleanup::SkipProtected {
            partition,
            build_id,
            reason,
        } => {
            tracing::info!(
                job_id,
                partition,
                build_id,
                reason,
                "Stage {stage} skipped; unused partition image is protected"
            );
            timeline.skip_current(serde_json::json!({
                "skipped": true,
                "reason": reason,
                "partition": partition,
                "build_id": build_id,
            }));
        }
        UnusedPartitionCleanup::Uninstall {
            partition,
            build_id,
        } => {
            tracing::info!(
                job_id,
                "cleaning up unused {partition} image with build ID {build_id}"
            );

            let uninstall_job_id = {
                let sw = switch.lock().await;
                sw.as_ref().uninstall_system_image().await
            };

            let uninstall_job_id = match uninstall_job_id {
                Ok(id) => id,
                Err(e) => {
                    let error_message = format!(
                        "NVUE rejected system image uninstall trigger: {}",
                        e.message
                    );
                    fail_stage(
                        &mut timeline,
                        job,
                        "failed",
                        &error_message,
                        serde_json::json!({"error": e.message}),
                        &None,
                        &Some(before_state),
                        &None,
                        false,
                        false,
                        &build_result_json,
                    );
                    return;
                }
            };

            let sw = switch.lock().await;
            if let Err(e) =
                wait_for_uninstall_completion(sw.as_ref(), &uninstall_job_id, cancel, job_id).await
            {
                let error_message = format!("NVUE system image uninstall failed: {}", e.message);
                fail_stage(
                    &mut timeline,
                    job,
                    "failed",
                    &error_message,
                    serde_json::json!({"error": e.message}),
                    &None,
                    &Some(before_state),
                    &None,
                    false,
                    false,
                    &build_result_json,
                );
                return;
            }
            timeline.complete_current(serde_json::json!({}));
        }
    }

    // ── Stage 2: pre-staging visibility + SFTP push ──
    if let Some(err_msg) = check_cancel(&timeline) {
        fail_stage(
            &mut timeline,
            job,
            "cancelled",
            &err_msg,
            serde_json::json!({"cancelled_before_start": true}),
            &None,
            &Some(before_state.clone()),
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    }
    let Ok((stage, description)) = timeline.start_stage() else {
        fail_stage(
            &mut timeline,
            job,
            "failed",
            "stage list exhausted",
            serde_json::json!({}),
            &None,
            &None,
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    };
    job.progress(description);
    tracing::info!(
        job_id,
        stage = %stage,
        image_filename,
        local_file_path,
        "pre-staging image visibility"
    );

    let mut image_visible_before_stage = false;
    {
        let sw = switch.lock().await;
        match sw.as_ref().list_system_images().await {
            Ok(listing) => {
                image_visible_before_stage =
                    system_image_listing_contains(&listing, image_filename);
                tracing::info!(
                    job_id,
                    stage = %stage,
                    image_visible_before_stage,
                    "pre-staging image listing checked"
                );
            }
            Err(e) => tracing::warn!(
                job_id,
                stage = %stage,
                error = %e.message,
                "could not list system images before staging; continuing with upload"
            ),
        }
    }

    // Track how long the actual SFTP upload + verify took so we can emit
    // a throughput estimate when the upload actually happened (skipped
    // when the remote file already matched local size).
    let stage_image_started = Instant::now();
    {
        let sw = switch.lock().await;
        if let Err(e) = sw
            .as_ref()
            .push_system_image_file(local_file_path, image_filename, sftp_upload_options, cancel)
            .await
        {
            let was_cancelled = e.code == ErrorCode::Cancelled;
            let outcome = if was_cancelled { "cancelled" } else { "failed" };
            let error_message = format!(
                "SFTP upload / verification interrupted (local_file_path={local_file_path}, image_filename={image_filename}): {}",
                e.message
            );
            fail_stage(
                &mut timeline,
                job,
                outcome,
                &error_message,
                serde_json::json!({"error": e.message, "cancelled": was_cancelled}),
                &None,
                &Some(before_state),
                &None,
                image_visible_before_stage,
                false,
                &build_result_json,
            );
            return;
        }
    }
    let stage_image_duration_ms = stage_image_started.elapsed().as_millis() as i64;

    let mut image_visible_after_stage = false;
    {
        let sw = switch.lock().await;
        if let Ok(listing) = sw.as_ref().list_system_images().await {
            image_visible_after_stage = system_image_listing_contains(&listing, image_filename);
            tracing::info!(
                job_id,
                stage = %stage,
                image_visible_after_stage,
                "post-staging image listing checked"
            );
        }
    }

    // Build stage_image details: visibility flags +
    // image_size_bytes always; average_rate_bytes_per_second +
    // average_rate_mib_per_second only when a real upload happened
    // (i.e. the image was not already staged) and duration > 0.
    let mut stage_image_details = serde_json::json!({
        "image_visible_before_stage": image_visible_before_stage,
        "image_visible_after_stage": image_visible_after_stage,
        "image_size_bytes": local_image_size_bytes,
    });
    if !image_visible_before_stage
        && stage_image_duration_ms > 0
        && let Some(size) = local_image_size_bytes
    {
        let seconds = stage_image_duration_ms as f64 / 1000.0;
        let bytes_per_second = size as f64 / seconds;
        insert_json_field(
            &mut stage_image_details,
            "average_rate_bytes_per_second",
            serde_json::json!(bytes_per_second),
        );
        insert_json_field(
            &mut stage_image_details,
            "average_rate_mib_per_second",
            serde_json::json!(bytes_per_second / (1024.0 * 1024.0)),
        );
    }
    timeline.complete_current(stage_image_details);
    tracing::info!(job_id, stage = %stage, "image staged");

    // ── Stage 3: trigger install ──
    if let Some(err_msg) = check_cancel(&timeline) {
        fail_stage(
            &mut timeline,
            job,
            "cancelled",
            &err_msg,
            serde_json::json!({"cancelled_before_start": true}),
            &None,
            &Some(before_state.clone()),
            &None,
            image_visible_before_stage,
            image_visible_after_stage,
            &build_result_json,
        );
        return;
    }
    let Ok((stage, description)) = timeline.start_stage() else {
        fail_stage(
            &mut timeline,
            job,
            "failed",
            "stage list exhausted",
            serde_json::json!({}),
            &None,
            &None,
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    };
    job.progress(description);

    let install_job_id = {
        let sw = switch.lock().await;
        sw.as_ref().install_system_image(image_filename).await
    };
    let install_job_id = match install_job_id {
        Ok(id) => id,
        Err(e) => {
            let error_message = format!("NVUE install rejected trigger: {}", e.message);
            fail_stage(
                &mut timeline,
                job,
                "failed",
                &error_message,
                serde_json::json!({"error": e.message}),
                &None,
                &Some(before_state),
                &None,
                image_visible_before_stage,
                image_visible_after_stage,
                &build_result_json,
            );
            return;
        }
    };
    timeline.complete_current(serde_json::json!({"install_job_id": install_job_id}));
    tracing::debug!(
        job_id,
        stage = %stage,
        install_job_id = %install_job_id,
        "stage completed: NVUE install triggered"
    );

    // ── Wait for staged install success (VR), or automatic reboot handoff (GB) ──
    if let Some(err_msg) = check_cancel(&timeline) {
        fail_stage(
            &mut timeline,
            job,
            "cancelled",
            &err_msg,
            serde_json::json!({"cancelled_before_start": true}),
            &Some(install_job_id.clone()),
            &Some(before_state.clone()),
            &None,
            image_visible_before_stage,
            image_visible_after_stage,
            &build_result_json,
        );
        return;
    }
    let Ok((stage, description)) = timeline.start_stage() else {
        fail_stage(
            &mut timeline,
            job,
            "failed",
            "stage list exhausted",
            serde_json::json!({}),
            &None,
            &None,
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    };
    job.progress(description);
    tracing::info!(
        job_id,
        stage = %stage,
        install_job_id = %install_job_id,
        "waiting for install result"
    );

    {
        let sw = switch.lock().await;
        if let Err(e) =
            wait_for_install_completion(sw.as_ref(), &install_job_id, cancel, job_id).await
        {
            let error_message = format!(
                "NVUE install did not complete cleanly (install_job_id={install_job_id}): {}",
                e.message
            );
            fail_stage(
                &mut timeline,
                job,
                "failed",
                &error_message,
                serde_json::json!({"error": e.message}),
                &Some(install_job_id),
                &Some(before_state),
                &None,
                image_visible_before_stage,
                image_visible_after_stage,
                &build_result_json,
            );
            return;
        }
    }
    timeline.complete_current(serde_json::json!({}));
    tracing::debug!(job_id, stage = %stage, "stage completed: install ready for activation verification");

    if bmc_activation {
        let Ok((stage, description)) = timeline.start_stage() else {
            fail_stage(
                &mut timeline,
                job,
                "failed",
                "stage list exhausted",
                serde_json::json!({}),
                &Some(install_job_id),
                &Some(before_state),
                &None,
                image_visible_before_stage,
                image_visible_after_stage,
                &build_result_json,
            );
            return;
        };
        job.progress(description);
        let activation = {
            let sw = switch.lock().await;
            activate_staged_system_image_via_bmc(sw.as_ref(), target_build_id, cancel).await
        };
        match activation {
            Ok(staged) => {
                timeline.complete_current(serde_json::json!({
                    "boot_next_verified": system_image_state_to_json(&staged),
                    "reset_type": "PowerCycle",
                    "transport": "bmc_redfish",
                }));
                tracing::info!(job_id, stage = %stage, "NVOS activation submitted through BMC");
            }
            Err(error) => {
                let outcome = if cancel.is_cancelled() || error.code == ErrorCode::Cancelled {
                    "cancelled"
                } else {
                    "failed"
                };
                fail_stage(
                    &mut timeline,
                    job,
                    outcome,
                    &format!("NVOS BMC activation failed: {}", error.message),
                    serde_json::json!({"error": error.message}),
                    &Some(install_job_id),
                    &Some(before_state),
                    &None,
                    image_visible_before_stage,
                    image_visible_after_stage,
                    &build_result_json,
                );
                return;
            }
        }
    }

    // ── Stage 5: wait for target steady state ──
    if let Some(err_msg) = check_cancel(&timeline) {
        fail_stage(
            &mut timeline,
            job,
            "cancelled",
            &err_msg,
            serde_json::json!({"cancelled_before_start": true}),
            &Some(install_job_id.clone()),
            &Some(before_state.clone()),
            &None,
            image_visible_before_stage,
            image_visible_after_stage,
            &build_result_json,
        );
        return;
    }
    let Ok((stage, description)) = timeline.start_stage() else {
        fail_stage(
            &mut timeline,
            job,
            "failed",
            "stage list exhausted",
            serde_json::json!({}),
            &None,
            &None,
            &None,
            false,
            false,
            &build_result_json,
        );
        return;
    };
    job.progress(description);
    tracing::info!(
        job_id,
        stage = %stage,
        target_build_id,
        "waiting for target steady state"
    );

    let after_state = {
        let mut sw = switch.lock().await;
        wait_for_target_steady_state(
            sw.as_mut(),
            target_build_id,
            switch_username,
            switch_password,
            cancel,
            job_id,
        )
        .await
    };
    let after_state = match after_state {
        Ok(s) => s,
        Err(e) => {
            let error_message = format!(
                "switch did not converge to target build {target_build_id}: {}",
                e.message
            );
            fail_stage(
                &mut timeline,
                job,
                "failed",
                &error_message,
                serde_json::json!({"error": e.message}),
                &Some(install_job_id),
                &Some(before_state),
                &None,
                image_visible_before_stage,
                image_visible_after_stage,
                &build_result_json,
            );
            return;
        }
    };
    timeline.complete_current(system_image_state_to_json(&after_state));
    tracing::debug!(
        job_id,
        stage = %stage,
        current_build_id = %after_state.current_build_id,
        next_build_id = %after_state.next_build_id,
        current_partition = %after_state.current_partition,
        next_partition = %after_state.next_partition,
        "stage completed: switch returned with target system image"
    );

    // ── Stage 6: verify NVOS reports ready and healthy ──
    if let Err((outcome, error_message, details)) = run_readiness_stage(
        &mut timeline,
        &job,
        &switch,
        cancel,
        job_id,
        readiness,
        target_build_id,
    )
    .await
    {
        fail_stage(
            &mut timeline,
            job,
            outcome,
            &error_message,
            details,
            &Some(install_job_id),
            &Some(before_state),
            &Some(after_state),
            image_visible_before_stage,
            image_visible_after_stage,
            &build_result_json,
        );
        return;
    }

    let result_json = build_result_json(
        "success",
        &Some(install_job_id),
        &Some(before_state),
        &Some(after_state),
        image_visible_before_stage,
        image_visible_after_stage,
        &timeline,
    );
    job.complete_verified("Completed", result_json);
}

fn system_image_state_to_json(state: &SwitchSystemImageState) -> serde_json::Value {
    serde_json::json!({
        "current": state.current,
        "next": state.next,
        "current_partition": state.current_partition,
        "next_partition": state.next_partition,
        "current_build_id": state.current_build_id,
        "next_build_id": state.next_build_id,
        "partition1_build_id": state.partition1_build_id,
        "partition2_build_id": state.partition2_build_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
    use crate::config::{ExpectedInventoryCatalog, WorkflowTimings};
    use crate::domain::node::NodeType;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
    use crate::libnmxc::TlsMaterialStore;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use opentelemetry::trace::{Status, TracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::sync::Arc;
    use std::sync::LazyLock;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tracing::Instrument;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    /// Works around tokio-rs/tracing#3611: with a single registered dispatcher,
    /// tracing-core can cache `Interest::never` for a callsite first reached on
    /// a thread without a subscriber, so a scoped subscriber silently misses
    /// that span. A second registered dispatcher avoids it. Remove once a
    /// tracing-core release includes tokio-rs/tracing#3614.
    fn keep_tracing_interest_accurate() {
        static EXTRA: LazyLock<tracing::Dispatch> =
            LazyLock::new(|| tracing::Dispatch::new(tracing_subscriber::registry()));
        LazyLock::force(&EXTRA);
    }

    fn phase_exporter() -> (SdkTracerProvider, InMemorySpanExporter, tracing::Dispatch) {
        keep_tracing_interest_accurate();
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("system-image-phase-test"))
                .with_filter(tracing_subscriber::filter::filter_fn(
                    crate::observability::tracing::should_export,
                )),
        );
        (provider, exporter, tracing::Dispatch::new(subscriber))
    }

    async fn capture_phase<T>(
        future: impl std::future::Future<Output = Result<T>>,
    ) -> (Result<T>, SpanData) {
        let (provider, exporter, subscriber) = phase_exporter();

        let result = async {
            // Other tests can cache these callsites while no subscriber is active.
            tracing::callsite::rebuild_interest_cache();

            future
                .instrument(tracing::info_span!(parent: None, "request"))
                .await
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2, "the phase must close before returning");
        let parent = spans.iter().find(|span| span.name == "request").unwrap();
        assert_eq!(parent.status, Status::Unset);
        assert!(
            !parent
                .attributes
                .iter()
                .any(|attr| attr.key.as_str() == "error.type")
        );
        let parent_context = parent.span_context.clone();
        let phase = spans
            .into_iter()
            .find(|span| span.name != "request")
            .unwrap();
        assert_eq!(phase.parent_span_id, parent_context.span_id());
        assert_eq!(phase.span_context.trace_id(), parent_context.trace_id());
        assert_ne!(phase.span_context.span_id(), parent_context.span_id());
        (result, phase)
    }

    fn assert_phase_error(span: &SpanData, code: ErrorCode) {
        assert_eq!(span.status, Status::error(""));
        let category = span
            .attributes
            .iter()
            .find(|attr| attr.key.as_str() == "error.type")
            .expect("failed phase should carry a bounded error category");
        assert_eq!(category.value.as_str(), code.to_string());
    }

    fn test_service(job_tracker: Arc<JobTracker>) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker,
            firmware_download_cancellations: Arc::new(Mutex::new(Default::default())),
            backends: Backends::memory(),
            firmware_dir: "firmware".into(),
            artifact_require_sha256: false,
            insecure_bmc: true,
            artifact_download_policy:
                crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
            switch_tls_roots: SwitchTlsRoots::default(),
            sftp_upload_options: SshUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: ExpectedInventoryCatalog::default(),
            workflow_timings: WorkflowTimings::default(),
            recovery_profiles: crate::config::RecoveryProfiles::default(),
            fabric_operations: std::sync::Arc::new(Default::default()),
            fabric_layout_catalog: std::sync::Arc::new(
                crate::config::fabric_layout::FabricLayoutCatalog::default(),
            ),
            hardware_profiles: Arc::new(
                crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
            ),
        }
    }

    struct SequentialResponder {
        count: Arc<AtomicUsize>,
        responses: Vec<ResponseTemplate>,
    }

    impl SequentialResponder {
        fn json(count: Arc<AtomicUsize>, bodies: Vec<serde_json::Value>) -> Self {
            let responses = bodies
                .into_iter()
                .map(|body| ResponseTemplate::new(200).set_body_json(body))
                .collect();

            Self { count, responses }
        }
    }

    impl Respond for SequentialResponder {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            let index = self.count.fetch_add(1, Ordering::SeqCst);

            self.responses
                .get(index)
                .or_else(|| self.responses.last())
                .cloned()
                .unwrap_or_else(|| ResponseTemplate::new(500))
        }
    }

    #[test]
    fn switch_system_image_job_timeline_attaches_node_and_rack() {
        // Regression test: `run_switch_system_image_job` builds its
        // `StageTimeline` through this helper specifically so the
        // node/rack it runs against are attached -- without this, the
        // "stage starting"/"stage completed"/"stage failed" logs
        // `StageTimeline` emits end up with empty node/rack fields.
        let timeline = switch_system_image_job_timeline("job-1", "rack-01", "sw-01", false);
        assert_eq!(timeline.node(), "sw-01");
        assert_eq!(timeline.rack(), "rack-01");
    }

    fn onie_host_endpoint(ip_address: &str) -> rm::Endpoint {
        rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: ip_address.to_owned(),
                ..Default::default()
            }),
            port: 22,
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "onie".to_owned(),
                    password: "recovery-password".to_owned(),
                })),
            }),
        }
    }

    #[test]
    fn recovery_options_digest_one_additional_host_when_authorized() -> Result<()> {
        let node = rm::NodeInfo {
            node_id: "sw-01".to_owned(),
            additional_host_endpoints: vec![onie_host_endpoint("192.0.2.50")],
            ..Default::default()
        };

        let recovery = SwitchImageRecoveryOptions::from_node_info(&node, true)?;

        assert!(recovery.allow_onie_recovery);

        assert_eq!(
            recovery
                .onie_endpoint
                .as_ref()
                .map(|endpoint| endpoint.endpoint.ip_address.as_str()),
            Some("192.0.2.50")
        );

        Ok(())
    }

    #[test]
    fn recovery_options_ignore_additional_hosts_without_authorization() -> Result<()> {
        for additional_host_endpoints in [
            Vec::new(),
            vec![rm::Endpoint::default(), rm::Endpoint::default()],
        ] {
            let node = rm::NodeInfo {
                node_id: "sw-01".to_owned(),
                additional_host_endpoints,
                ..Default::default()
            };

            let recovery = SwitchImageRecoveryOptions::from_node_info(&node, false)?;

            assert!(!recovery.allow_onie_recovery);
            assert!(recovery.onie_endpoint.is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn onie_recovery_stages_scp_before_install_and_nvos_verification() -> Result<()> {
        let directory = tempfile::tempdir().expect("create installer directory");
        let local_path = directory.path().join("nvos-25.02.0.bin");
        let image_size = crate::nodes::switch_gb200_nvidia::config::MIN_FIRMWARE_FILE_SIZE;

        std::fs::write(&local_path, vec![0_u8; image_size as usize])
            .expect("create installer image");

        let events = Arc::new(Mutex::new(Vec::new()));
        let upload_events = Arc::clone(&events);
        let exec_events = Arc::clone(&events);

        let mut switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
            .with_host_credentials_for_test("admin", "requested-password")
            .with_scp_upload_for_test(move |_, remote_path| {
                upload_events
                    .lock()
                    .unwrap()
                    .push(format!("scp:{remote_path}"));

                Ok(())
            })
            .with_ssh_exec_for_test(move |command| {
                let event = if command.starts_with("wc -c < ") {
                    "confirm"
                } else if command.contains("onie-nos-install '/tmp/nvos-25.02.0.bin'") {
                    "install"
                } else {
                    assert_eq!(command, "command -v nv >/dev/null 2>&1");
                    "verify"
                };

                exec_events.lock().unwrap().push(event.to_owned());

                Ok(if event == "confirm" {
                    image_size.to_string()
                } else {
                    String::new()
                })
            });
        let onie_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: "192.0.2.50".to_owned(),
                mac_address: String::new(),
                port: 22,
                host_name: None,
            },
            Some(EndpointCredentials::new("onie", "recovery-password")),
            false,
        );

        recover_nvos_through_onie(
            &mut switch,
            &onie_endpoint,
            local_path.to_str().expect("UTF-8 test path"),
            "nvos-25.02.0.bin",
            SshUploadOptions::default(),
            Duration::from_millis(100),
            &CancellationToken::new(),
        )
        .await?;

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["scp:/tmp/nvos-25.02.0.bin", "confirm", "install", "verify"]
        );

        Ok(())
    }

    #[tokio::test]
    async fn onie_recovery_replaces_factory_default_admin_password() -> Result<()> {
        let directory = tempfile::tempdir().expect("create installer directory");
        let local_path = directory.path().join("nvos-25.02.0.bin");
        let image_size = crate::nodes::switch_gb200_nvidia::config::MIN_FIRMWARE_FILE_SIZE;

        std::fs::write(&local_path, vec![0_u8; image_size as usize])
            .expect("create installer image");

        let events = Arc::new(Mutex::new(Vec::new()));
        let upload_events = Arc::clone(&events);
        let exec_events = Arc::clone(&events);
        let password_events = Arc::clone(&events);
        let probe_count = Arc::new(AtomicUsize::new(0));
        let probes = Arc::clone(&probe_count);

        let mut switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
            .with_host_credentials_for_test("admin", "requested-password")
            .with_scp_upload_for_test(move |_, remote_path| {
                upload_events
                    .lock()
                    .unwrap()
                    .push(format!("scp:{remote_path}"));

                Ok(())
            })
            .with_ssh_exec_for_test(move |command| {
                if command.starts_with("wc -c < ") {
                    exec_events.lock().unwrap().push("confirm".to_owned());

                    return Ok(image_size.to_string());
                }

                if command.contains("onie-nos-install '/tmp/nvos-25.02.0.bin'") {
                    exec_events.lock().unwrap().push("install".to_owned());

                    return Ok(String::new());
                }

                if command.starts_with("if [ -f '/tmp/rms-onie-nos-install.status'") {
                    return Ok(String::new());
                }

                assert_eq!(command, "command -v nv >/dev/null 2>&1");
                exec_events.lock().unwrap().push("verify".to_owned());

                if probes.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(RmsError::unauthenticated(
                        "configured NVOS credentials were rejected",
                    ))
                } else {
                    Ok(String::new())
                }
            })
            .with_ssh_password_recovery_for_test(move |current_password, new_password| {
                password_events
                    .lock()
                    .unwrap()
                    .push(format!("password:{current_password}->{new_password}"));

                Ok(())
            });
        let onie_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: "192.0.2.50".to_owned(),
                mac_address: String::new(),
                port: 22,
                host_name: None,
            },
            Some(EndpointCredentials::new("onie", "recovery-password")),
            false,
        );

        recover_nvos_through_onie(
            &mut switch,
            &onie_endpoint,
            local_path.to_str().expect("UTF-8 test path"),
            "nvos-25.02.0.bin",
            SshUploadOptions::default(),
            Duration::from_millis(100),
            &CancellationToken::new(),
        )
        .await?;

        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                "scp:/tmp/nvos-25.02.0.bin",
                "confirm",
                "install",
                "verify",
                "password:admin->requested-password",
                "verify",
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn onie_recovery_rejects_non_admin_nvos_username_before_staging() {
        let mut switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
            .with_host_credentials_for_test("operator", "requested-password")
            .with_scp_upload_for_test(|_, _| panic!("invalid recovery must not stage an image"));

        let onie_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: "192.0.2.50".to_owned(),
                mac_address: String::new(),
                port: 22,
                host_name: None,
            },
            Some(EndpointCredentials::new("onie", "recovery-password")),
            false,
        );

        let error = recover_nvos_through_onie(
            &mut switch,
            &onie_endpoint,
            "/var/lib/rms/nvos-25.02.0.bin",
            "nvos-25.02.0.bin",
            SshUploadOptions::default(),
            Duration::from_millis(100),
            &CancellationToken::new(),
        )
        .await
        .expect_err("a non-admin target must be rejected before ONIE recovery");

        assert_eq!(error.code, ErrorCode::FailedPrecondition);

        assert_eq!(
            error.message,
            "ONIE recovery requires the configured NVOS username to be admin"
        );
    }

    #[tokio::test]
    async fn recovered_credential_persistence_retries_transient_nvue_failure() -> Result<()> {
        let nvue = MockServer::start().await;
        let request_count = Arc::new(AtomicUsize::new(0));

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/applied"))
            .respond_with(SequentialResponder {
                count: Arc::clone(&request_count),
                responses: vec![ResponseTemplate::new(503), ResponseTemplate::new(200)],
            })
            .expect(2)
            .mount(&nvue)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&nvue.uri());

        persist_recovered_nvos_credentials(
            &switch,
            Duration::from_millis(100),
            &CancellationToken::new(),
        )
        .await?;

        nvue.verify().await;

        assert_eq!(request_count.load(Ordering::SeqCst), 2);

        Ok(())
    }

    #[tokio::test]
    async fn recovered_credential_persistence_retries_unauthorized_nvue_response() -> Result<()> {
        let nvue = MockServer::start().await;
        let request_count = Arc::new(AtomicUsize::new(0));

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/applied"))
            .respond_with(SequentialResponder {
                count: Arc::clone(&request_count),
                responses: vec![ResponseTemplate::new(401), ResponseTemplate::new(200)],
            })
            .expect(2)
            .mount(&nvue)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&nvue.uri());

        persist_recovered_nvos_credentials(
            &switch,
            Duration::from_millis(100),
            &CancellationToken::new(),
        )
        .await?;

        nvue.verify().await;

        assert_eq!(request_count.load(Ordering::SeqCst), 2);

        Ok(())
    }

    #[tokio::test]
    async fn recovered_credential_persistence_respects_the_save_timeout() {
        let nvue = MockServer::start().await;
        let request_count = Arc::new(AtomicUsize::new(0));

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/applied"))
            .respond_with(SequentialResponder {
                count: Arc::clone(&request_count),
                responses: vec![ResponseTemplate::new(503)],
            })
            .mount(&nvue)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&nvue.uri());

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            persist_recovered_nvos_credentials(
                &switch,
                Duration::from_millis(5),
                &CancellationToken::new(),
            ),
        )
        .await
        .expect("configured credential save timeout should finish promptly")
        .expect_err("NVUE must time out when every save attempt fails");

        assert_eq!(error.code, ErrorCode::Timeout);
        assert!(request_count.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn recovery_options_require_exactly_one_additional_host_when_authorized() {
        for (name, endpoints, expected_count) in [
            ("missing", Vec::new(), 0),
            (
                "multiple",
                vec![
                    onie_host_endpoint("192.0.2.50"),
                    onie_host_endpoint("192.0.2.51"),
                ],
                2,
            ),
        ] {
            let node = rm::NodeInfo {
                node_id: "sw-01".to_owned(),
                additional_host_endpoints: endpoints,
                ..Default::default()
            };

            let result = SwitchImageRecoveryOptions::from_node_info(&node, true);

            let Err(error) = result else {
                panic!("{name} additional host endpoint case should be rejected");
            };

            assert_eq!(error.code, ErrorCode::InvalidArgument, "{name}");

            assert!(
                error.message.contains("requires exactly one"),
                "{name}: {}",
                error.message
            );

            assert!(
                error.message.contains(&expected_count.to_string()),
                "{name}: {}",
                error.message
            );
        }
    }

    // ── wait_for_uninstall tests ──────────────────────────────────────

    fn action_running_resp() -> serde_json::Value {
        serde_json::json!({"state": "action_running"})
    }

    fn action_success_resp() -> serde_json::Value {
        serde_json::json!({"state": "action_success"})
    }

    #[test]
    fn create_switch_system_image_node_requires_bmc_only_for_vrnvl72() {
        let mut config = NodeConfig {
            id: "vr-switch-01".to_owned(),
            node_type: NodeType::SwitchVrnvl72Nvidia,
            bmc_endpoint: None,
            host_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "192.0.2.10".to_owned(),
                    mac_address: String::new(),
                    port: 443,
                    host_name: Some("vr-switch.example.com".to_owned()),
                },
                Some(EndpointCredentials::new("admin", "password")),
                false,
            )),
            expected_inventory: None,
            recovery: None,
            runtime_config: None,
        };

        for node_type in [NodeType::SwitchGb200Nvidia, NodeType::SwitchGb300Nvidia] {
            config.node_type = node_type;
            assert!(create_switch_system_image_node(&config, "rack-01").is_ok());
        }
        config.node_type = NodeType::SwitchVrnvl72Nvidia;
        assert!(create_switch_system_image_node(&config, "rack-01").is_err());
        config.bmc_endpoint = config.host_endpoint.clone();
        let node = create_switch_system_image_node(&config, "rack-01").unwrap();
        assert!(node.system_image_requires_bmc_activation());
        config.bmc_endpoint.as_mut().unwrap().credentials = None;
        assert!(create_switch_system_image_node(&config, "rack-01").is_err());
    }

    /// Tests wait_for_uninstall_completion under normal circumstances, when no sleep is needed.
    /// (Immediate success)
    #[tokio::test]
    async fn wait_for_uninstall_immediate_success() {
        let server = MockServer::start().await;
        let system_image_job_id = "1daca8cc-fb39-467a-baca-9ee17b040c2f";
        let uninstall_job_id = "15";
        let action_uri = format!("/nvue_v1/action/{uninstall_job_id}");

        // Set up mock response for first poll (success)
        Mock::given(method("GET"))
            .and(path(&action_uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(action_success_resp()))
            .mount(&server)
            .await;

        // Create test version of Switch with mock HTTP client, and cancellation token
        let sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();

        let (result, span) = capture_phase(wait_for_uninstall_completion(
            &sw,
            uninstall_job_id,
            &cancel,
            system_image_job_id,
        ))
        .await;
        result.unwrap();
        assert_eq!(span.status, Status::Unset);
    }

    /// Tests wait_for_uninstall_completion after running a single poll, expecting success.
    /// UNINSTALL_POLL_INTERVAL is tiny in test mode, while the completion timeout
    /// leaves enough room for loaded CI workers to observe the second poll.
    #[tokio::test]
    async fn wait_for_uninstall_success_after_running_poll() {
        let server = MockServer::start().await;
        let system_image_job_id = "1daca8cc-fb39-467a-baca-9ee17b040c2f";
        let uninstall_job_id = "15";
        let action_uri = format!("/nvue_v1/action/{uninstall_job_id}");

        let poll_count = Arc::new(AtomicUsize::new(0));
        let response_count = poll_count.clone();

        Mock::given(method("GET"))
            .and(path(&action_uri))
            .respond_with(SequentialResponder::json(
                response_count,
                vec![action_running_resp(), action_success_resp()],
            ))
            .mount(&server)
            .await;

        // Create test version of Switch with mock HTTP client, and cancellation token
        let sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();

        // UNINSTALL_POLL_INTERVAL is 1 ms in test mode; the sleep between polls
        // completes almost instantly with real time.
        wait_for_uninstall_completion(&sw, uninstall_job_id, &cancel, system_image_job_id)
            .await
            .unwrap();
        assert!(poll_count.load(Ordering::SeqCst) >= 2);
    }

    /// Tests wait_for_uninstall_completion when poll_firmware_task returns an HTTP error.
    /// The error must be propagated immediately without retrying.
    #[tokio::test]
    async fn wait_for_uninstall_poll_error() {
        let server = MockServer::start().await;
        let uninstall_job_id = "15";
        let action_uri = format!("/nvue_v1/action/{uninstall_job_id}");

        Mock::given(method("GET"))
            .and(path(&action_uri))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();

        let (result, span) = capture_phase(wait_for_uninstall_completion(
            &sw,
            uninstall_job_id,
            &cancel,
            "job-id",
        ))
        .await;
        let err = result.unwrap_err();
        assert_phase_error(&span, err.code);
        assert!(
            err.message.contains("500"),
            "expected HTTP 500 error, got: {}",
            err.message
        );
    }

    /// Tests wait_for_uninstall_completion when the cancellation token is already set.
    /// The cancel check at the top of the poll loop fires before any HTTP request is made.
    #[tokio::test]
    async fn wait_for_uninstall_cancelled_before_poll() {
        let server = MockServer::start().await;
        let sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();
        cancel.cancel();

        let (result, span) =
            capture_phase(wait_for_uninstall_completion(&sw, "15", &cancel, "job-id")).await;
        let err = result.unwrap_err();
        assert_phase_error(&span, ErrorCode::Internal);
        assert!(
            err.message.contains("cancelled"),
            "expected cancelled error, got: {}",
            err.message
        );
    }

    /// Tests wait_for_uninstall_completion when the poll never succeeds and the deadline
    /// expires. UNINSTALL_COMPLETION_TIMEOUT is 500 ms in test mode so this completes quickly.
    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_uninstall_timeout() {
        let server = MockServer::start().await;
        let uninstall_job_id = "15";
        let action_uri = format!("/nvue_v1/action/{uninstall_job_id}");

        Mock::given(method("GET"))
            .and(path(&action_uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(action_running_resp()))
            .mount(&server)
            .await;

        let sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();

        let (result, span) = capture_phase(wait_for_uninstall_completion(
            &sw,
            uninstall_job_id,
            &cancel,
            "job-id",
        ))
        .await;
        let err = result.unwrap_err();
        assert_phase_error(&span, ErrorCode::Timeout);
        assert!(
            err.message.contains("timed out"),
            "expected timeout error, got: {}",
            err.message
        );
    }

    // ── wait_for_install tests ────────────────────────────────────────

    fn install_running(detail: &str) -> ResponseTemplate {
        ResponseTemplate::new(200)
            .set_body_json(serde_json::json!({"state": "running", "detail": detail}))
    }

    /// Scripts GET /nvue_v1/action/2: the replies in order, the last one repeated.
    async fn install_poll_script(replies: Vec<ResponseTemplate>) -> (MockServer, Arc<AtomicUsize>) {
        let server = MockServer::start().await;
        let polls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/2"))
            .respond_with(SequentialResponder {
                count: polls.clone(),
                responses: replies,
            })
            .mount(&server)
            .await;
        (server, polls)
    }

    /// NVUE reports "Performing reboot cold..." shortly before it stops answering, and the
    /// next poll can meet the outage (HTTP 502). The text announces the reboot, so the
    /// install wait hands off to the steady-state wait on it, before the outage.
    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_install_hands_off_on_the_cold_message() {
        let (server, polls) = install_poll_script(vec![
            install_running("Installing image: nvos-amd64-25.02.4430.bin"),
            install_running("Installing image: nvos-amd64-25.02.4430.bin"),
            install_running("Performing reboot cold..."),
            ResponseTemplate::new(502),
        ])
        .await;
        let sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();

        if let Err(err) = wait_for_install_completion(&sw, "2", &cancel, "job-id").await {
            panic!("expected a handoff on the cold text, got: {}", err.message);
        }
        assert_eq!(
            polls.load(Ordering::SeqCst),
            3,
            "RMS stops polling at the cold reply"
        );
    }

    #[tokio::test]
    async fn install_phase_records_only_terminal_failures() {
        for (response, expected_error) in [
            (
                ResponseTemplate::new(200).set_body_json(action_success_resp()),
                None,
            ),
            (ResponseTemplate::new(401), None),
            (
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "state": "action_error", "detail": "system is offline during reboot"
                })),
                None,
            ),
            (
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "state": "action_failed", "detail": "private-install-error"
                })),
                Some(ErrorCode::Internal),
            ),
            (
                ResponseTemplate::new(200).set_body_json(action_running_resp()),
                Some(ErrorCode::Timeout),
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/nvue_v1/action/15"))
                .respond_with(response)
                .mount(&server)
                .await;
            let sw = SwitchGb200Nvidia::for_test(&server.uri());
            let cancel = CancellationToken::new();
            let (result, span) =
                capture_phase(wait_for_install_completion(&sw, "15", &cancel, "job-id")).await;
            if let Some(code) = expected_error {
                assert_eq!(result.unwrap_err().code, code);
                assert_phase_error(&span, code);
            } else {
                result.unwrap();
                assert_eq!(span.status, Status::Unset);
                assert!(
                    !span
                        .attributes
                        .iter()
                        .any(|attr| attr.key.as_str() == "error.type")
                );
            }
            assert!(!format!("{span:?}").contains("private-install-error"));
        }
    }

    #[tokio::test]
    async fn install_phase_records_cancellation() {
        let server = MockServer::start().await;
        let sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (result, span) =
            capture_phase(wait_for_install_completion(&sw, "15", &cancel, "job-id")).await;
        assert_eq!(result.unwrap_err().code, ErrorCode::Internal);
        assert_phase_error(&span, ErrorCode::Internal);
    }

    #[tokio::test]
    async fn vrnvl72_install_requires_explicit_success_before_bmc_activation() {
        for (response, expected_error) in [
            (ResponseTemplate::new(200).set_body_json(action_success_resp()), None),
            (ResponseTemplate::new(401), Some(ErrorCode::Unauthenticated)),
            (ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "action_error", "detail": "system is offline during reboot", "percent": 100
            })), Some(ErrorCode::Internal)),
            (ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "action_error", "detail": "Installing image", "percent": 100
            })), Some(ErrorCode::Internal)),
            (ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "action_failed", "detail": "install failed"
            })), Some(ErrorCode::Internal)),
            (ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "action_running", "status": "system is offline during reboot"
            })), Some(ErrorCode::Timeout)),
            (ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "action_error", "issue": [{"message": "already installed"}]
            })), Some(ErrorCode::Internal)),
            (ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "action_success"
            })), None),
            (ResponseTemplate::new(503), Some(ErrorCode::Timeout)),
            (ResponseTemplate::new(200).set_body_json(action_running_resp()), Some(ErrorCode::Timeout)),
        ] {
            let nvos = MockServer::start().await;
            Mock::given(method("GET")).and(path("/nvue_v1/action/10"))
                .respond_with(response).mount(&nvos).await;
            let switch = SwitchVrnvl72Nvidia::for_test(&nvos.uri());
            let result = wait_for_install_completion_inner(&switch, "10", &CancellationToken::new()).await;
            assert_eq!(result.err().map(|error| error.code), expected_error);
        }
    }

    #[tokio::test]
    async fn vrnvl72_nvos_activation_sends_one_bmc_reset_and_no_nvue_power_cycle() {
        use wiremock::matchers::body_json;
        for reset_status in [204, 500] {
            let nvos = MockServer::start().await;
            let bmc = MockServer::start().await;
            for (uri, body) in [
                (
                    "/redfish/v1",
                    serde_json::json!({
                        "@odata.id": "/redfish/v1", "Id": "RootService", "Name": "Root Service", "Links": {},
                        "Systems": {"@odata.id": "/redfish/v1/Systems"}
                    }),
                ),
                (
                    "/redfish/v1/Systems",
                    serde_json::json!({
                        "@odata.id": "/redfish/v1/Systems", "Name": "Systems",
                        "Members": [{"@odata.id": "/redfish/v1/Systems/System_0"}]
                    }),
                ),
                (
                    "/redfish/v1/Systems/System_0",
                    serde_json::json!({
                        "@odata.id": "/redfish/v1/Systems/System_0", "Id": "System_0", "Name": "System",
                        "Actions": {"#ComputerSystem.Reset": {"target": "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset"}}
                    }),
                ),
            ] {
                Mock::given(method("GET"))
                    .and(path(uri))
                    .respond_with(ResponseTemplate::new(200).set_body_json(body))
                    .mount(&bmc)
                    .await;
            }
            Mock::given(method("GET"))
                .and(path("/nvue_v1/system/image"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "current": "nvos-old", "next": "nvos-new"
                })))
                .expect(1)
                .mount(&nvos)
                .await;
            Mock::given(method("POST"))
                .and(path(
                    "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
                ))
                .and(body_json(serde_json::json!({"ResetType": "PowerCycle"})))
                .respond_with(ResponseTemplate::new(reset_status))
                .expect(1)
                .mount(&bmc)
                .await;
            let switch =
                SwitchVrnvl72Nvidia::for_test(&nvos.uri()).with_redfish_for_test(&bmc.uri());
            let result = activate_staged_system_image_via_bmc(
                &switch,
                "nvos-new",
                &CancellationToken::new(),
            )
            .await;
            assert_eq!(result.is_ok(), reset_status == 204);
            assert!(
                nvos.received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .all(|r| r.method.to_string() == "GET")
            );
        }
    }

    #[tokio::test]
    async fn vrnvl72_nvos_activation_does_not_reset_on_bad_boot_next_or_cancellation() {
        for next in ["nvos-old", ""] {
            let nvos = MockServer::start().await;
            let bmc = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/nvue_v1/system/image"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "current": "nvos-old", "next": next
                })))
                .mount(&nvos)
                .await;
            let switch =
                SwitchVrnvl72Nvidia::for_test(&nvos.uri()).with_redfish_for_test(&bmc.uri());
            let cancel = CancellationToken::new();
            let error = activate_staged_system_image_via_bmc(&switch, "nvos-new", &cancel)
                .await
                .unwrap_err();
            assert_eq!(error.code, ErrorCode::FailedPrecondition);
            cancel.cancel();
            let error = activate_staged_system_image_via_bmc(&switch, "nvos-new", &cancel)
                .await
                .unwrap_err();
            assert_eq!(error.code, ErrorCode::Cancelled);
            assert!(bmc.received_requests().await.unwrap().is_empty());
        }
    }

    // ── sleep_unless_cancelled tests ──────────────────────────────────────

    #[tokio::test]
    async fn sleep_unless_cancelled_returns_true_on_natural_completion() {
        let cancel = CancellationToken::new();
        let completed = sleep_unless_cancelled(Duration::from_millis(5), &cancel).await;
        assert!(completed);
    }

    #[tokio::test]
    async fn sleep_unless_cancelled_returns_false_when_signalled() {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        // Signal cancellation after 5ms; we ask to sleep for 60s. The helper
        // must return ~immediately with `false`.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            cancel_clone.cancel();
        });
        let start = std::time::Instant::now();
        let completed = sleep_unless_cancelled(Duration::from_secs(60), &cancel).await;
        let elapsed = start.elapsed();
        assert!(!completed, "sleep should report cancellation");
        assert!(
            elapsed < Duration::from_millis(500),
            "sleep should wake up immediately on cancellation (took {elapsed:?})"
        );
    }

    #[tokio::test]
    async fn sleep_unless_cancelled_returns_false_when_already_cancelled() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let completed = sleep_unless_cancelled(Duration::from_secs(60), &cancel).await;
        assert!(!completed);
    }

    // ── stage-2 push cancellation tests ───────────────────────────────

    use crate::nodes::switch_gb200_nvidia::NVOS_PARTITION_1_ID;
    use crate::orchestrator::job_lifecycle::JobState;
    use crate::orchestrator::job_tracker::{JobInfo, JobTracker, JobType};
    use tokio::sync::Mutex as TokioMutex;

    /// How [`StageTwoPushMock`] fails the stage-2 SFTP push.
    #[derive(Clone, Copy)]
    enum PushFailureMode {
        /// The transport reports cancellation via `ErrorCode::Cancelled`
        /// without the job token being cancelled: exercises error-code-based
        /// outcome classification on its own.
        CancelledError,
        /// A genuine upload failure with no cancellation involved.
        GenuineFailure,
        /// The push never runs (ONIE recovery installed the image) and the
        /// recovered switch reports unhealthy on the readiness poll.
        UnhealthyAfterRecovery,
    }

    struct SecureRecoveryContext {
        client: nvue_client::SharedClient,
        client_tls: nvue_client::ClientTls,
        switch_tls_roots: SwitchTlsRoots,
        require_client_certificate: Arc<AtomicBool>,
        credentials_persisted: Arc<AtomicBool>,
        behavior: SecureRecoveryBehavior,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum SecureRecoveryBehavior {
        ActivateClientTls,
        LeaveClientTlsUnverified,
        NoCertificateInstallNode,
    }

    enum TestSwitchTls {
        Default,
        Insecure,
        Secure(Box<SecureRecoveryContext>),
    }

    /// Mock switch node that advances through inspect + cleanup (uninstall
    /// skipped: the unused partition has no image) and then fails the stage-2
    /// SFTP push according to `failure_mode`.
    struct StageTwoPushMock {
        failure_mode: PushFailureMode,
        fail_asic_preflight: bool,
        image_state: SwitchSystemImageState,
        recovery_expected: bool,
        secure_recovery: Option<SecureRecoveryContext>,
    }

    #[async_trait]
    impl SwitchSystemImageNode for StageTwoPushMock {
        fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
            self.secure_recovery.as_ref().map(|context| &context.client)
        }

        fn require_certificate_install_node(&self) -> Result<&dyn SwitchCertificateInstallNode> {
            if self.secure_recovery.as_ref().is_some_and(|context| {
                context.behavior != SecureRecoveryBehavior::NoCertificateInstallNode
            }) {
                Ok(self)
            } else {
                Err(RmsError::failed_precondition(
                    "switch does not support NVUE certificate installation",
                ))
            }
        }

        async fn verify_nvos_installation(
            &self,
            onie_endpoint: Option<&EndpointConfig>,
        ) -> Result<NvosInstallationState> {
            if self.recovery_expected && onie_endpoint.is_none() {
                return Err(RmsError::connection_refused("NVOS SSH probe failed"));
            }

            Ok(if self.recovery_expected {
                NvosInstallationState::NotInstalled
            } else {
                NvosInstallationState::Installed
            })
        }

        async fn stage_nvos_recovery_image(
            &self,
            _onie_endpoint: &EndpointConfig,
            _local_file_path: &str,
            _image_filename: &str,
            _upload_options: SshUploadOptions,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            assert!(
                self.recovery_expected,
                "unexpected ONIE recovery image staging"
            );

            Ok(())
        }

        async fn install_staged_nvos_image_and_wait(
            &mut self,
            _onie_endpoint: &EndpointConfig,
            _image_filename: &str,
            _recovery_timeout: Duration,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            assert!(self.recovery_expected, "unexpected ONIE recovery install");
            Ok(())
        }

        async fn save_current_applied_config(&self) -> Result<()> {
            assert!(
                self.recovery_expected,
                "unexpected recovered credential persistence"
            );

            if let Some(context) = &self.secure_recovery {
                if !context.require_client_certificate.load(Ordering::SeqCst)
                    || !context.client.uses_verified_client_tls()
                {
                    return Err(RmsError::failed_precondition(
                        "recovered credentials require verified NVUE client TLS",
                    ));
                }

                context.credentials_persisted.store(true, Ordering::SeqCst);
            }

            Ok(())
        }

        fn system_image_requires_bmc_activation(&self) -> bool {
            false
        }

        async fn activate_system_image_via_bmc(&self, _cancel: &CancellationToken) -> Result<()> {
            unreachable!("BMC activation should not be reached")
        }

        async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
            Ok(self.image_state.clone())
        }

        async fn ensure_nvos_asic_firmware_config(&self) -> Result<()> {
            if self.fail_asic_preflight {
                Err(RmsError::internal("could not read ASIC firmware config"))
            } else {
                Ok(())
            }
        }

        async fn poll_firmware_task(
            &self,
            _task_id: &str,
            _staged_install: bool,
        ) -> Result<crate::domain::node::FirmwareTaskStatus> {
            unreachable!("poll_firmware_task should not be reached")
        }

        async fn uninstall_system_image(&self) -> Result<String> {
            unreachable!("uninstall should be skipped")
        }

        async fn list_system_images(&self) -> Result<serde_json::Value> {
            Ok(serde_json::json!([]))
        }

        async fn push_system_image_file(
            &self,
            _local_file_path: &str,
            _image_filename: &str,
            _sftp_upload_options: SshUploadOptions,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            match self.failure_mode {
                PushFailureMode::CancelledError => {
                    Err(RmsError::cancelled("SSH SFTP remote file write cancelled"))
                }
                PushFailureMode::GenuineFailure => Err(RmsError::internal("SFTP write failed")),
                PushFailureMode::UnhealthyAfterRecovery => {
                    unreachable!("the recovered path never pushes an image")
                }
            }
        }

        async fn install_system_image(&self, _image_filename: &str) -> Result<String> {
            unreachable!("install should not be reached after a failed push")
        }

        async fn recover_admin_password_after_boot(
            &mut self,
            _target_password: &str,
        ) -> Result<()> {
            unreachable!("password recovery should not be reached")
        }

        async fn verify_system_ready(&self) -> Result<()> {
            if matches!(self.failure_mode, PushFailureMode::UnhealthyAfterRecovery) {
                return Err(RmsError::failed_precondition(
                    "NVOS not ready: status=ready, health=Not OK",
                ));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl SwitchCertificateInstallNode for StageTwoPushMock {
        fn node_id(&self) -> &str {
            "sw-01"
        }

        async fn read_certificate_inventory(&self) -> Result<CertificateInventory> {
            Ok(CertificateInventory::default())
        }

        async fn read_certificate_bindings(&self) -> Result<CertificateBindings> {
            Ok(CertificateBindings::default())
        }

        async fn delete_certificate_entry(&self, store: CertificateStore, id: &str) -> Result<()> {
            panic!("a recovery bootstrap with empty stores must not delete {store} entry {id}");
        }

        async fn prepare_mtls_remote_dirs(
            &self,
            _material: &SwitchMtlsMaterialPaths,
        ) -> Result<()> {
            Ok(())
        }

        async fn sftp_copy_mtls_file(
            &self,
            material: &SwitchMtlsMaterialPaths,
            remote_file_name: &str,
            _cancel: CancellationToken,
        ) -> Result<MtlsSftpCopyOutcome> {
            Ok(MtlsSftpCopyOutcome {
                remote_path: format!("{}/{remote_file_name}", material.remote_dir()),
                bytes: 1,
            })
        }

        async fn import_mtls_material(&self, _material: &SwitchMtlsMaterialPaths) -> Result<()> {
            Ok(())
        }

        async fn initialize_nvue_capabilities(&self) -> Result<crate::nodes::nvos::GnmiClientCa> {
            Ok(crate::nodes::nvos::GnmiClientCa::Unsupported)
        }

        async fn cleanup_mtls_remote_dir_best_effort(&self, _material: &SwitchMtlsMaterialPaths) {}

        async fn bind_mtls_service_for_install(
            &self,
            _material: &SwitchMtlsMaterialPaths,
            service: SwitchMtlsService,
            transition: Option<&NvueTlsTransition>,
        ) -> Result<()> {
            let context = self
                .secure_recovery
                .as_ref()
                .expect("secure recovery context");

            assert_eq!(service, SwitchMtlsService::NvueApi);
            assert!(transition.is_some());
            assert!(context.client.uses_client_tls());
            assert!(!context.client.uses_verified_client_tls());

            context
                .require_client_certificate
                .store(true, Ordering::SeqCst);

            if context.behavior == SecureRecoveryBehavior::LeaveClientTlsUnverified {
                return Ok(());
            }

            let prepared = context
                .client
                .prepare_verified_client_tls(context.client_tls.clone(), Some("127.0.0.1"))
                .await?;

            context
                .client
                .activate_prepared_client_tls(
                    &prepared,
                    "/nvue_v1/system",
                    nvue_client::DEFAULT_TIMEOUT,
                )
                .await?;

            Ok(())
        }

        async fn verify_mtls_services(
            &self,
            _material: &SwitchMtlsMaterialPaths,
            services: &[SwitchMtlsService],
        ) -> Result<Value> {
            assert_eq!(services, &[SwitchMtlsService::NvueApi]);

            Ok(serde_json::json!({"verified": true}))
        }
    }

    async fn run_stage_two_push_job_with_preflight(
        failure_mode: PushFailureMode,
        fail_asic_preflight: bool,
    ) -> JobInfo {
        run_stage_two_push_job_with_state(
            failure_mode,
            fail_asic_preflight,
            SwitchSystemImageState {
                current_partition: NVOS_PARTITION_1_ID.to_owned(),
                current_build_id: "nvos-old".to_owned(),
                next_build_id: "nvos-old".to_owned(),
                // Empty unused partition -> cleanup skips uninstall.
                partition2_build_id: String::new(),
                ..Default::default()
            },
            false,
            None,
            TestSwitchTls::Default,
            false,
        )
        .await
    }

    async fn run_stage_two_push_job_with_state(
        failure_mode: PushFailureMode,
        fail_asic_preflight: bool,
        image_state: SwitchSystemImageState,
        allow_onie_recovery: bool,
        installation_state: Option<NvosInstallationState>,
        switch_tls: TestSwitchTls,
        cancel_before_start: bool,
    ) -> JobInfo {
        let tracker = Arc::new(JobTracker::new());
        let mut service = test_service(Arc::clone(&tracker));

        let secure_recovery = match switch_tls {
            TestSwitchTls::Default => None,
            TestSwitchTls::Insecure => {
                service.switch_tls_roots.insecure_switch = true;
                None
            }
            TestSwitchTls::Secure(context) => {
                service.switch_tls_roots = context.switch_tls_roots.clone();
                Some(*context)
            }
        };

        let pending = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .expect("create pending job");
        let job_id = pending.id().to_string();
        let cancel = pending.cancellation_token();

        if cancel_before_start {
            cancel.cancel();
        }

        let switch: SharedSwitchSystemImageNode =
            Arc::new(TokioMutex::new(Box::new(StageTwoPushMock {
                failure_mode,
                fail_asic_preflight,
                image_state,
                recovery_expected: installation_state == Some(NvosInstallationState::NotInstalled),
                secure_recovery,
            })));

        let recovery = SwitchImageRecoveryOptions {
            allow_onie_recovery,
            onie_endpoint: allow_onie_recovery.then(|| {
                EndpointConfig::with_credentials(
                    Endpoint {
                        ip_address: "192.0.2.50".to_owned(),
                        mac_address: String::new(),
                        port: 22,
                        host_name: None,
                    },
                    Some(EndpointCredentials::new("onie", "recovery-password")),
                    false,
                )
            }),
        };

        let handle = tracker.spawn_job(pending, move |job| async move {
            run_switch_system_image_job(
                &service,
                job,
                switch,
                "img.bin",
                "/nonexistent/img.bin",
                "nvos-new",
                "admin",
                "password",
                "rack-01",
                "sw-01",
                recovery,
                None,
                SshUploadOptions::default(),
                &cancel,
            )
            .await;
        });
        handle.wait().await.expect("supervisor join");

        tracker.get_job(&job_id).expect("job should exist")
    }

    async fn run_stage_two_push_job(
        failure_mode: PushFailureMode,
        allow_onie_recovery: bool,
    ) -> JobInfo {
        run_stage_two_push_job_with_state(
            failure_mode,
            false,
            SwitchSystemImageState {
                current_partition: NVOS_PARTITION_1_ID.to_owned(),
                current_build_id: "nvos-old".to_owned(),
                next_build_id: "nvos-old".to_owned(),
                partition2_build_id: String::new(),
                ..Default::default()
            },
            allow_onie_recovery,
            None,
            TestSwitchTls::Default,
            false,
        )
        .await
    }

    #[tokio::test]
    async fn nvos_preflight_only_skips_when_current_and_next_match() {
        for (current, next, skip) in [
            ("nvos-new", "nvos-new", true),
            ("nvos-new", "nvos-old", false),
            ("nvos-old", "nvos-new", false),
            ("nvos-old", "nvos-old", false),
        ] {
            let job = run_stage_two_push_job_with_state(
                PushFailureMode::GenuineFailure,
                true,
                SwitchSystemImageState {
                    current_build_id: current.to_owned(),
                    next_build_id: next.to_owned(),
                    ..Default::default()
                },
                false,
                None,
                TestSwitchTls::Default,
                false,
            )
            .await;

            let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();

            if skip {
                assert_eq!(job.state, JobState::Completed);
                assert_eq!(result["status"], "already_up_to_date");
                let stages = result["timing_summary"]["stages"].as_array().unwrap();

                let inspect_index = stages
                    .iter()
                    .position(|stage| stage["name"] == "inspect_state")
                    .expect("inspect_state stage should be present");

                assert!(
                    stages
                        .iter()
                        .skip(inspect_index + 1)
                        .all(|stage| stage["status"] == "skipped")
                );
            } else {
                // Reaching the ASIC preflight proves that a stale next-boot
                // build, even with the running build current, cannot trigger the skip.
                assert_eq!(job.state, JobState::Failed);
                assert!(
                    job.error_message
                        .contains("ASIC firmware auto-update preflight failed")
                );
            }
        }
    }

    #[tokio::test]
    async fn nvos_probe_error_without_recovery_is_preserved() {
        let info = run_stage_two_push_job_with_state(
            PushFailureMode::GenuineFailure,
            false,
            SwitchSystemImageState::default(),
            false,
            Some(NvosInstallationState::NotInstalled),
            TestSwitchTls::Default,
            false,
        )
        .await;

        assert_eq!(info.state, JobState::Failed);
        assert_eq!(info.error_code, JobError::ClientError);
        assert_eq!(info.error_message, "NVOS SSH probe failed");

        let result: serde_json::Value = serde_json::from_str(&info.result_json).unwrap();
        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        assert_eq!(stages[0]["status"], "failed");

        assert!(
            stages
                .iter()
                .skip(1)
                .all(|stage| stage["status"] == "skipped")
        );
    }

    #[tokio::test]
    async fn secure_onie_recovery_rejects_missing_certificates_before_installation() {
        let info = run_stage_two_push_job_with_state(
            PushFailureMode::GenuineFailure,
            false,
            SwitchSystemImageState::default(),
            true,
            Some(NvosInstallationState::NotInstalled),
            TestSwitchTls::Default,
            false,
        )
        .await;

        let result: serde_json::Value = serde_json::from_str(&info.result_json).unwrap();
        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        assert_eq!(info.error_code, JobError::FailedPrecondition);
        assert!(info.error_message.contains("switch_cert_root"));
        assert!(info.error_message.contains("before image installation"));
        assert_eq!(stages[1]["status"], "failed");
        assert_eq!(stages[2]["name"], "bootstrap_nvue_certificate");
        assert_eq!(stages[2]["status"], "skipped");
        assert_eq!(stages[3]["status"], "skipped");
        assert_eq!(stages[4]["status"], "skipped");
    }

    struct NvueHttpsTestServer {
        port: u16,
        require_client_certificate: Arc<AtomicBool>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for NvueHttpsTestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn start_nvue_https_test_server(
        ca_cert: &rcgen::Certificate,
        factory_cert: &rcgen::Certificate,
        factory_key: &KeyPair,
        server_cert: &rcgen::Certificate,
        server_key: &KeyPair,
    ) -> anyhow::Result<NvueHttpsTestServer> {
        let mut client_roots = rustls::RootCertStore::empty();
        client_roots.add(CertificateDer::from(ca_cert.der().to_vec()))?;

        let client_verifier =
            rustls::server::WebPkiClientVerifier::builder(Arc::new(client_roots)).build()?;

        let factory_tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(factory_cert.der().to_vec())],
                PrivatePkcs8KeyDer::from(factory_key.serialize_der()).into(),
            )?;

        let verified_tls = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(
                vec![CertificateDer::from(server_cert.der().to_vec())],
                PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            )?;

        let factory_acceptor = TlsAcceptor::from(Arc::new(factory_tls));
        let verified_acceptor = TlsAcceptor::from(Arc::new(verified_tls));
        let require_client_certificate = Arc::new(AtomicBool::new(false));
        let server_requires_client_certificate = Arc::clone(&require_client_certificate);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();

        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let acceptor = if server_requires_client_certificate.load(Ordering::SeqCst) {
                    &verified_acceptor
                } else {
                    &factory_acceptor
                };

                if let Ok(mut stream) = acceptor.accept(socket).await {
                    let mut request = [0; 4096];

                    if stream.read(&mut request).await.is_ok() {
                        let _ = stream
                            .write_all(
                                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                            )
                            .await;
                    }
                }
            }
        });

        Ok(NvueHttpsTestServer {
            port,
            require_client_certificate,
            task,
        })
    }

    async fn run_secure_onie_recovery_with(
        behavior: SecureRecoveryBehavior,
    ) -> anyhow::Result<(JobInfo, bool, bool)> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

        let ca_key = KeyPair::generate()?;
        let ca_cert = ca_params.self_signed(&ca_key)?;
        let factory = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
        let server_key = KeyPair::generate()?;

        let server_cert = CertificateParams::new(vec!["127.0.0.1".to_owned()])?.signed_by(
            &server_key,
            &ca_cert,
            &ca_key,
        )?;

        let client_key = KeyPair::generate()?;

        let client_cert = CertificateParams::new(vec!["rms-client".to_owned()])?.signed_by(
            &client_key,
            &ca_cert,
            &ca_key,
        )?;

        let root = tempfile::tempdir()?;
        let domain_dir = root.path().join("site-wide");

        std::fs::create_dir(&domain_dir)?;
        std::fs::write(domain_dir.join("ca.pem"), ca_cert.pem())?;
        std::fs::write(domain_dir.join("client.pem"), client_cert.pem())?;
        std::fs::write(domain_dir.join("client.key"), client_key.serialize_pem())?;

        let server = start_nvue_https_test_server(
            &ca_cert,
            &factory.cert,
            &factory.key_pair,
            &server_cert,
            &server_key,
        )
        .await?;

        let config = nvue_client::ClientConfig {
            endpoint: nvue_client::ClientEndpoint::https("127.0.0.1", "127.0.0.1", server.port),
            credentials: nvue_client::ClientCredentials::new("admin", "password"),
            dangerously_accept_invalid_certs: false,
        };

        let client = nvue_client::Client::new(config)?;

        let client_tls = nvue_client::ClientTls::new(
            domain_dir.join("ca.pem"),
            domain_dir.join("client.pem"),
            domain_dir.join("client.key"),
        );

        let material_store = TlsMaterialStore::new(root.path());

        let switch_tls_roots = SwitchTlsRoots {
            switch_cert: Some(material_store.clone()),
            client_tls: Some(material_store),
            default_domain: Some("site-wide".to_owned()),
            ..Default::default()
        };

        let credentials_persisted = Arc::new(AtomicBool::new(false));

        let job = run_stage_two_push_job_with_state(
            PushFailureMode::GenuineFailure,
            false,
            SwitchSystemImageState {
                current_build_id: "nvos-new".to_owned(),
                next_build_id: "nvos-new".to_owned(),
                ..Default::default()
            },
            true,
            Some(NvosInstallationState::NotInstalled),
            TestSwitchTls::Secure(Box::new(SecureRecoveryContext {
                client,
                client_tls,
                switch_tls_roots,
                require_client_certificate: Arc::clone(&server.require_client_certificate),
                credentials_persisted: Arc::clone(&credentials_persisted),
                behavior,
            })),
            false,
        )
        .await;

        Ok((
            job,
            server.require_client_certificate.load(Ordering::SeqCst),
            credentials_persisted.load(Ordering::SeqCst),
        ))
    }

    #[tokio::test]
    async fn secure_onie_recovery_verifies_nvue_mtls_before_persisting_credentials()
    -> anyhow::Result<()> {
        let (job, client_certificate_required, credentials_persisted) =
            run_secure_onie_recovery_with(SecureRecoveryBehavior::ActivateClientTls).await?;

        let result: Value = serde_json::from_str(&job.result_json)?;

        let stages = result["timing_summary"]["stages"]
            .as_array()
            .expect("timeline should contain stages");

        assert_eq!(job.state, JobState::Completed);
        assert_eq!(result["status"], "recovered");
        assert_eq!(stages[1]["status"], "completed");
        assert_eq!(stages[2]["status"], "completed");
        assert_eq!(stages[2]["details"]["verified_mtls"], true);
        assert_eq!(stages[3]["status"], "completed");
        assert_eq!(stages[4]["status"], "completed");
        assert!(client_certificate_required);
        assert!(credentials_persisted);

        Ok(())
    }

    #[tokio::test]
    async fn secure_onie_recovery_rejects_unverified_client_tls_before_persisting_credentials()
    -> anyhow::Result<()> {
        let (job, client_certificate_required, credentials_persisted) =
            run_secure_onie_recovery_with(SecureRecoveryBehavior::LeaveClientTlsUnverified).await?;

        let result: Value = serde_json::from_str(&job.result_json)?;
        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_code, JobError::FailedPrecondition);
        assert!(job.error_message.contains("post-install mTLS verification"));
        assert_eq!(stages[2]["status"], "failed");
        assert_eq!(stages[3]["status"], "skipped");
        assert_eq!(stages[4]["status"], "skipped");
        assert!(client_certificate_required);
        assert!(!credentials_persisted);

        Ok(())
    }

    #[tokio::test]
    async fn secure_onie_recovery_rejects_node_without_certificate_installation()
    -> anyhow::Result<()> {
        let (job, client_certificate_required, credentials_persisted) =
            run_secure_onie_recovery_with(SecureRecoveryBehavior::NoCertificateInstallNode).await?;

        let result: Value = serde_json::from_str(&job.result_json)?;
        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_code, JobError::FailedPrecondition);
        assert!(job.error_message.contains("reclaim_certificate_capacity"));

        assert!(
            job.error_message
                .contains("does not support NVUE certificate installation")
        );

        assert_eq!(stages[2]["status"], "failed");
        assert_eq!(stages[3]["status"], "skipped");
        assert_eq!(stages[4]["status"], "skipped");
        assert!(!client_certificate_required);
        assert!(!credentials_persisted);

        Ok(())
    }

    #[tokio::test]
    async fn insecure_onie_recovery_skips_certificate_bootstrap_and_persists_credentials() {
        let info = run_stage_two_push_job_with_state(
            PushFailureMode::GenuineFailure,
            true,
            SwitchSystemImageState {
                current_build_id: "nvos-old".to_owned(),
                next_build_id: "nvos-old".to_owned(),
                ..Default::default()
            },
            true,
            Some(NvosInstallationState::NotInstalled),
            TestSwitchTls::Insecure,
            false,
        )
        .await;

        let result: serde_json::Value = serde_json::from_str(&info.result_json).unwrap();
        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        assert_eq!(stages[1]["status"], "completed");
        assert_eq!(stages[2]["status"], "skipped");
        assert_eq!(stages[2]["details"]["reason"], "insecure_switch is enabled");
        assert_eq!(stages[3]["status"], "completed");
        assert_eq!(stages[4]["status"], "completed");
    }

    #[tokio::test]
    async fn onie_recovery_runs_asic_preflight_without_reinstalling_the_target_image() {
        let info = run_stage_two_push_job_with_state(
            PushFailureMode::GenuineFailure,
            false,
            SwitchSystemImageState {
                current_build_id: "nvos-new".to_owned(),
                next_build_id: "nvos-new".to_owned(),
                ..Default::default()
            },
            true,
            Some(NvosInstallationState::NotInstalled),
            TestSwitchTls::Insecure,
            false,
        )
        .await;

        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");

        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        assert_eq!(info.state, JobState::Completed);
        assert_eq!(result["status"], "recovered");
        assert_eq!(stages[1]["status"], "completed");
        assert_eq!(stages[1]["details"]["recovered"], true);
        assert_eq!(stages[6]["name"], "verify_asic_auto_update");
        assert_eq!(stages[6]["status"], "completed");

        let last = stages.last().unwrap();
        assert_eq!(last["name"], "verify_system_ready");
        assert_eq!(last["status"], "completed");
        assert!(
            stages[7..stages.len() - 1]
                .iter()
                .all(|stage| stage["status"] == "skipped")
        );
    }

    #[tokio::test]
    async fn onie_recovery_fails_when_the_recovered_switch_reports_unhealthy() {
        let info = run_stage_two_push_job_with_state(
            PushFailureMode::UnhealthyAfterRecovery,
            false,
            SwitchSystemImageState {
                current_build_id: "nvos-new".to_owned(),
                next_build_id: "nvos-new".to_owned(),
                ..Default::default()
            },
            true,
            Some(NvosInstallationState::NotInstalled),
            TestSwitchTls::Insecure,
            false,
        )
        .await;

        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        assert_eq!(info.state, JobState::Failed);
        assert_eq!(result["status"], "failed");
        let last = stages.last().unwrap();
        assert_eq!(last["name"], "verify_system_ready");
        assert_eq!(last["status"], "failed");
        assert!(
            info.error_message.contains("health=Not OK"),
            "{}",
            info.error_message
        );
    }

    #[tokio::test]
    async fn onie_recovery_fails_when_asic_preflight_cannot_be_healed() {
        let info = run_stage_two_push_job_with_state(
            PushFailureMode::GenuineFailure,
            true,
            SwitchSystemImageState {
                current_build_id: "nvos-new".to_owned(),
                next_build_id: "nvos-new".to_owned(),
                ..Default::default()
            },
            true,
            Some(NvosInstallationState::NotInstalled),
            TestSwitchTls::Insecure,
            false,
        )
        .await;

        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");

        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        assert_eq!(info.state, JobState::Failed);
        assert_eq!(result["status"], "failed");
        assert_eq!(stages[6]["name"], "verify_asic_auto_update");
        assert_eq!(stages[6]["status"], "failed");

        assert!(
            stages
                .iter()
                .skip(7)
                .all(|stage| stage["status"] == "skipped")
        );
    }

    #[tokio::test]
    async fn nvos_setup_cancellation_records_other_job_error() {
        let info = run_stage_two_push_job_with_state(
            PushFailureMode::GenuineFailure,
            false,
            SwitchSystemImageState::default(),
            false,
            None,
            TestSwitchTls::Default,
            true,
        )
        .await;

        assert_eq!(info.state, JobState::Failed);
        assert_eq!(info.error_code, JobError::Other);

        let result: serde_json::Value = serde_json::from_str(&info.result_json).unwrap();

        assert_eq!(result["status"], "cancelled");
    }

    #[tokio::test]
    async fn stage_two_push_cancelled_error_records_cancelled_outcome() {
        let info = run_stage_two_push_job(PushFailureMode::CancelledError, false).await;

        assert_eq!(info.state, JobState::Failed);
        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");

        assert_eq!(result["allow_onie_recovery"], false);
        assert_eq!(result["onie_recovery_endpoint_configured"], false);

        assert_eq!(
            result["status"], "cancelled",
            "an ErrorCode::Cancelled push error should record a cancelled \
             outcome even when the job token is not cancelled"
        );
    }

    #[tokio::test]
    async fn stage_two_push_failure_records_failed_outcome() {
        let info = run_stage_two_push_job(PushFailureMode::GenuineFailure, true).await;

        assert_eq!(info.state, JobState::Failed);
        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");

        assert_eq!(result["allow_onie_recovery"], true);
        assert_eq!(result["onie_recovery_endpoint_configured"], true);

        let stages = result["timing_summary"]["stages"].as_array().unwrap();

        let setup: Vec<_> = stages
            .iter()
            .take(5)
            .map(|stage| (stage["name"].as_str(), stage["status"].as_str()))
            .collect();

        assert_eq!(
            setup,
            vec![
                (Some("verify_nvos_installation"), Some("completed")),
                (Some("recover_nvos"), Some("skipped")),
                (Some("bootstrap_nvue_certificate"), Some("skipped")),
                (Some("initialize_nvue"), Some("completed")),
                (Some("persist_recovered_credentials"), Some("skipped")),
            ]
        );

        assert_eq!(
            result["status"], "failed",
            "a genuine push failure should record a failed outcome"
        );
    }

    #[tokio::test]
    async fn cleanup_stage_is_recorded_skipped_not_completed_when_no_unused_image() {
        // Regression test: `StageTwoPushMock` reports an empty unused
        // partition build ID, so stage 1.5 has nothing to clean up. It
        // must show up in the timeline as "skipped" -- not "completed",
        // which would misleadingly imply an uninstall actually ran.
        let info = run_stage_two_push_job(PushFailureMode::GenuineFailure, false).await;

        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        let cleanup_stage = stages
            .iter()
            .find(|s| s["name"] == "cleanup_old_partition")
            .expect("cleanup_old_partition stage should be present");
        assert_eq!(cleanup_stage["status"], "skipped");
    }

    #[tokio::test]
    async fn asic_preflight_failure_stops_before_partition_cleanup_and_staging() {
        let info =
            run_stage_two_push_job_with_preflight(PushFailureMode::GenuineFailure, true).await;

        assert_eq!(info.state, JobState::Failed);
        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        let preflight = stages
            .iter()
            .find(|stage| stage["name"] == "verify_asic_auto_update")
            .expect("ASIC preflight stage should be present");
        let cleanup = stages
            .iter()
            .find(|stage| stage["name"] == "cleanup_old_partition")
            .expect("cleanup stage should be present");
        let staging = stages
            .iter()
            .find(|stage| stage["name"] == "stage_image")
            .expect("staging stage should be present");

        assert_eq!(preflight["status"], "failed");
        assert_eq!(cleanup["status"], "skipped");
        assert_eq!(staging["status"], "skipped");
        assert!(
            info.error_message
                .contains("collect Switch Tray logs before retrying")
        );
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ImageJobFailure {
        None,
        Install,
        BootNextRead,
        BootNextMismatch,
        BmcReset,
        PostBootMismatch,
        CancelBeforeCycle,
        /// The readiness probe answers `ErrorCode::Cancelled` instead of ready.
        ReadinessCancelled,
    }

    struct ImageJobMock {
        bmc_activation: bool,
        failure: ImageJobFailure,
        state_reads: AtomicUsize,
        events: Arc<std::sync::Mutex<Vec<&'static str>>>,
        cancel: CancellationToken,
    }

    impl ImageJobMock {
        fn record(&self, event: &'static str) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[async_trait]
    impl SwitchSystemImageNode for ImageJobMock {
        fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
            None
        }
        fn system_image_requires_bmc_activation(&self) -> bool {
            self.bmc_activation
        }

        async fn verify_nvos_installation(
            &self,
            _onie_endpoint: Option<&EndpointConfig>,
        ) -> Result<NvosInstallationState> {
            self.record("verify_nvos");
            Ok(NvosInstallationState::Installed)
        }

        async fn stage_nvos_recovery_image(
            &self,
            _onie_endpoint: &EndpointConfig,
            _local_file_path: &str,
            _image_filename: &str,
            _upload_options: SshUploadOptions,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            unreachable!("ONIE recovery should not run when NVOS is installed")
        }

        async fn install_staged_nvos_image_and_wait(
            &mut self,
            _onie_endpoint: &EndpointConfig,
            _image_filename: &str,
            _recovery_timeout: Duration,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            unreachable!("ONIE recovery should not run when NVOS is installed")
        }

        async fn save_current_applied_config(&self) -> Result<()> {
            unreachable!("credential persistence should not run when NVOS is installed")
        }

        async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
            self.record("state");
            let read = self.state_reads.fetch_add(1, Ordering::SeqCst);
            let mut state = SwitchSystemImageState {
                current_partition: NVOS_PARTITION_1_ID.to_owned(),
                current_build_id: "nvos-old".to_owned(),
                next_build_id: "nvos-old".to_owned(),
                ..Default::default()
            };
            if read == 0 {
                return Ok(state);
            }
            if self.bmc_activation && read == 1 {
                if self.failure == ImageJobFailure::BootNextRead {
                    return Err(RmsError::unavailable("boot-next read failed"));
                }
                if self.failure == ImageJobFailure::CancelBeforeCycle {
                    self.cancel.cancel();
                }
                if self.failure != ImageJobFailure::BootNextMismatch {
                    state.next_build_id = "nvos-new".to_owned();
                }
            } else if self.failure != ImageJobFailure::PostBootMismatch {
                state.current_build_id = "nvos-new".to_owned();
                state.next_build_id = "nvos-new".to_owned();
            }
            Ok(state)
        }

        async fn ensure_nvos_asic_firmware_config(&self) -> Result<()> {
            self.record("asic_preflight");
            Ok(())
        }
        async fn poll_firmware_task(
            &self,
            _task_id: &str,
            _staged_install: bool,
        ) -> Result<crate::domain::node::FirmwareTaskStatus> {
            self.record("poll");
            Ok(crate::domain::node::FirmwareTaskStatus {
                state: if self.failure == ImageJobFailure::Install {
                    "action_error"
                } else {
                    "action_success"
                }
                .to_owned(),
                completed: true,
                percent: 100,
                ..Default::default()
            })
        }
        async fn uninstall_system_image(&self) -> Result<String> {
            unreachable!("empty unused partition")
        }
        async fn list_system_images(&self) -> Result<serde_json::Value> {
            self.record("list");
            Ok(serde_json::json!(["nvos-new.bin"]))
        }
        async fn push_system_image_file(
            &self,
            _local: &str,
            _image: &str,
            _options: SshUploadOptions,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            self.record("upload");
            Ok(())
        }
        async fn install_system_image(&self, _image: &str) -> Result<String> {
            self.record("install");
            Ok("10".to_owned())
        }
        async fn activate_system_image_via_bmc(&self, _cancel: &CancellationToken) -> Result<()> {
            self.record("bmc_reset");
            if self.failure == ImageJobFailure::BmcReset {
                Err(RmsError::unavailable("BMC reset failed"))
            } else {
                Ok(())
            }
        }
        async fn recover_admin_password_after_boot(&mut self, _password: &str) -> Result<()> {
            unreachable!("password recovery not expected")
        }
        async fn verify_system_ready(&self) -> Result<()> {
            self.record("ready");
            if self.failure == ImageJobFailure::ReadinessCancelled {
                return Err(RmsError::cancelled("readiness poll cancelled"));
            }
            Ok(())
        }
    }

    async fn run_image_job(
        bmc_activation: bool,
        failure: ImageJobFailure,
    ) -> (JobInfo, Vec<&'static str>) {
        let tracker = Arc::new(JobTracker::new());
        let service = test_service(Arc::clone(&tracker));
        let pending = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let cancel = pending.cancellation_token();
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let switch: SharedSwitchSystemImageNode =
            Arc::new(TokioMutex::new(Box::new(ImageJobMock {
                bmc_activation,
                failure,
                state_reads: AtomicUsize::new(0),
                events: events.clone(),
                cancel: cancel.clone(),
            })));
        tracker
            .spawn_job(pending, move |job| async move {
                run_switch_system_image_job(
                    &service,
                    job,
                    switch,
                    "nvos-new.bin",
                    "/nonexistent/nvos-new.bin",
                    "nvos-new",
                    "admin",
                    "password",
                    "rack-01",
                    "sw-01",
                    SwitchImageRecoveryOptions {
                        allow_onie_recovery: false,
                        onie_endpoint: None,
                    },
                    None,
                    SshUploadOptions::default(),
                    &cancel,
                )
                .await;
            })
            .wait()
            .await
            .unwrap();
        let recorded = events.lock().unwrap().clone();
        (tracker.get_job(&job_id).unwrap(), recorded)
    }

    #[tokio::test]
    async fn image_job_activates_via_bmc_only_after_install_and_boot_next_verification() {
        let (job, events) = run_image_job(true, ImageJobFailure::None).await;
        assert_eq!(job.state, JobState::Completed, "{}", job.error_message);
        assert_eq!(
            events,
            [
                "verify_nvos",
                "state",
                "asic_preflight",
                "list",
                "upload",
                "list",
                "install",
                "poll",
                "state",
                "bmc_reset",
                "state",
                "ready"
            ]
        );
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        let activation = stages
            .iter()
            .find(|stage| stage["name"] == "activate_via_bmc")
            .unwrap();
        assert_eq!(activation["status"], "completed");
        assert_eq!(result["after"]["current_build_id"], "nvos-new");
    }

    #[tokio::test]
    async fn legacy_image_job_does_not_add_bmc_activation() {
        let (job, events) = run_image_job(false, ImageJobFailure::None).await;
        assert_eq!(job.state, JobState::Completed);
        assert_eq!(
            events,
            [
                "verify_nvos",
                "state",
                "asic_preflight",
                "list",
                "upload",
                "list",
                "install",
                "poll",
                "state",
                "ready"
            ]
        );
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        assert!(
            stages
                .iter()
                .all(|stage| stage["name"] != "activate_via_bmc")
        );
        let readiness = stages
            .iter()
            .find(|stage| stage["name"] == "verify_system_ready")
            .unwrap();
        assert_eq!(readiness["status"], "completed");
    }

    #[tokio::test]
    async fn image_job_records_a_cancelled_readiness_wait_as_cancelled() {
        let (job, _) = run_image_job(false, ImageJobFailure::ReadinessCancelled).await;
        assert_eq!(job.state, JobState::Failed);
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(result["status"], "cancelled");
        assert!(
            job.error_message.contains("NVOS readiness check cancelled"),
            "{}",
            job.error_message
        );
    }

    #[tokio::test]
    async fn image_job_never_cycles_on_install_or_boot_next_failure() {
        for failure in [
            ImageJobFailure::Install,
            ImageJobFailure::BootNextRead,
            ImageJobFailure::BootNextMismatch,
            ImageJobFailure::CancelBeforeCycle,
        ] {
            let (job, events) = run_image_job(true, failure).await;
            assert_ne!(job.state, JobState::Completed);
            assert!(!events.contains(&"bmc_reset"));
        }
    }

    #[tokio::test]
    async fn image_job_does_not_retry_bmc_reset_or_claim_success_on_wrong_running_version() {
        for failure in [ImageJobFailure::BmcReset, ImageJobFailure::PostBootMismatch] {
            let (job, events) = run_image_job(true, failure).await;
            assert_eq!(job.state, JobState::Failed);
            assert_eq!(
                events.iter().filter(|event| **event == "bmc_reset").count(),
                1
            );
        }
    }

    /// Mock switch node that always reports HTTP 401 on state reads and then
    /// fails post-boot admin recovery with `recovery_error`.
    struct SteadyStateRecoveryFailureMock {
        recovery_error: RmsError,
    }

    #[async_trait]
    impl SwitchSystemImageNode for SteadyStateRecoveryFailureMock {
        fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
            None
        }

        async fn verify_nvos_installation(
            &self,
            _onie_endpoint: Option<&EndpointConfig>,
        ) -> Result<NvosInstallationState> {
            Ok(NvosInstallationState::Installed)
        }

        async fn stage_nvos_recovery_image(
            &self,
            _onie_endpoint: &EndpointConfig,
            _local_file_path: &str,
            _image_filename: &str,
            _upload_options: SshUploadOptions,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            unreachable!("ONIE recovery should not run when NVOS is installed")
        }

        async fn install_staged_nvos_image_and_wait(
            &mut self,
            _onie_endpoint: &EndpointConfig,
            _image_filename: &str,
            _recovery_timeout: Duration,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            unreachable!("ONIE recovery should not run when NVOS is installed")
        }

        async fn save_current_applied_config(&self) -> Result<()> {
            unreachable!("credential persistence should not run")
        }

        fn system_image_requires_bmc_activation(&self) -> bool {
            false
        }

        async fn activate_system_image_via_bmc(&self, _cancel: &CancellationToken) -> Result<()> {
            unreachable!("BMC activation should not be reached")
        }

        async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
            Err(RmsError::new(ErrorCode::Unauthenticated, "unauthorized"))
        }

        async fn ensure_nvos_asic_firmware_config(&self) -> Result<()> {
            unreachable!("ASIC firmware preflight should not be reached")
        }

        async fn poll_firmware_task(
            &self,
            _task_id: &str,
            _staged_install: bool,
        ) -> Result<crate::domain::node::FirmwareTaskStatus> {
            unreachable!("poll_firmware_task should not be reached")
        }

        async fn uninstall_system_image(&self) -> Result<String> {
            unreachable!("uninstall should not be reached")
        }

        async fn list_system_images(&self) -> Result<serde_json::Value> {
            unreachable!("list_system_images should not be reached")
        }

        async fn push_system_image_file(
            &self,
            _local_file_path: &str,
            _image_filename: &str,
            _sftp_upload_options: SshUploadOptions,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            unreachable!("push should not be reached")
        }

        async fn install_system_image(&self, _image_filename: &str) -> Result<String> {
            unreachable!("install should not be reached")
        }

        async fn recover_admin_password_after_boot(
            &mut self,
            _target_password: &str,
        ) -> Result<()> {
            Err(RmsError::new(
                self.recovery_error.code,
                self.recovery_error.message.clone(),
            ))
        }

        async fn verify_system_ready(&self) -> Result<()> {
            unreachable!("these job tests end before the readiness stage")
        }
    }

    #[tokio::test]
    async fn steady_state_wait_reports_recovery_credential_mismatch_on_ssh_auth_failure() {
        let mut sw = SteadyStateRecoveryFailureMock {
            recovery_error: RmsError::new(
                ErrorCode::Unauthenticated,
                "SSH credentials rejected by 10.0.0.1",
            ),
        };

        let cancel = CancellationToken::new();

        let (result, span) = capture_phase(wait_for_target_steady_state(
            &mut sw, "nvos-new", "admin", "newpass", &cancel, "job-id",
        ))
        .await;
        let err = result.expect_err("SSH auth failure during recovery should surface as an error");
        assert_phase_error(&span, ErrorCode::Unauthenticated);

        assert_eq!(err.code, ErrorCode::Unauthenticated);

        assert_eq!(
            err.message,
            "post-boot admin recovery failed: recovery credential mismatch"
        );
    }

    /// An SSH authentication-exchange error must pass through unchanged, not
    /// get relabeled as a credential mismatch.
    #[tokio::test]
    async fn steady_state_wait_passes_through_ssh_auth_exchange_failure() {
        let mut sw = SteadyStateRecoveryFailureMock {
            recovery_error: RmsError::new(
                ErrorCode::InvalidArgument,
                "SSH auth to 10.0.0.1: connection closed",
            ),
        };

        let cancel = CancellationToken::new();

        let (result, span) = capture_phase(wait_for_target_steady_state(
            &mut sw, "nvos-new", "admin", "newpass", &cancel, "job-id",
        ))
        .await;
        let err = result.expect_err("recovery failure should surface as an error");
        assert_phase_error(&span, ErrorCode::InvalidArgument);

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert_eq!(err.message, "SSH auth to 10.0.0.1: connection closed");
    }

    #[tokio::test]
    async fn steady_state_phase_records_verified_success_after_transient_error() {
        let server = MockServer::start().await;
        let count = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path("/nvue_v1/system/image"))
            .respond_with(SequentialResponder {
                count: count.clone(),
                responses: vec![
                    ResponseTemplate::new(500),
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "current": "nvos-new", "next": "nvos-new"
                    })),
                ],
            })
            .mount(&server)
            .await;
        let mut sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();
        let (result, span) = capture_phase(wait_for_target_steady_state(
            &mut sw, "nvos-new", "admin", "password", &cancel, "job-id",
        ))
        .await;
        let state = result.unwrap();
        assert_eq!(state.current_build_id, "nvos-new");
        assert_eq!(state.next_build_id, "nvos-new");
        assert_eq!(span.status, Status::Ok);
        assert!(
            !span
                .attributes
                .iter()
                .any(|attr| attr.key.as_str() == "error.type")
        );
        assert!(count.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn steady_state_phase_requires_both_target_builds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/system/image"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "current": "nvos-new", "next": "nvos-old"
            })))
            .mount(&server)
            .await;
        let mut sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();
        let (result, span) = capture_phase(wait_for_target_steady_state(
            &mut sw, "nvos-new", "admin", "password", &cancel, "job-id",
        ))
        .await;
        assert_eq!(result.unwrap_err().code, ErrorCode::Timeout);
        assert_phase_error(&span, ErrorCode::Timeout);
    }

    #[tokio::test]
    async fn steady_state_phase_records_cancellation_before_polling() {
        let server = MockServer::start().await;
        let mut sw = SwitchGb200Nvidia::for_test(&server.uri());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (result, span) = capture_phase(wait_for_target_steady_state(
            &mut sw, "nvos-new", "admin", "password", &cancel, "job-id",
        ))
        .await;
        assert_eq!(result.unwrap_err().code, ErrorCode::Internal);
        assert_phase_error(&span, ErrorCode::Internal);
    }

    #[tokio::test]
    async fn dropped_steady_state_phase_remains_unset() {
        let mut sw = SwitchGb200Nvidia::for_test("http://127.0.0.1:1");
        let cancel = CancellationToken::new();
        let (result, span) = capture_phase(async {
            let mut pending = Box::pin(wait_for_target_steady_state(
                &mut sw, "nvos-new", "admin", "password", &cancel, "job-id",
            ));
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(pending);
            Ok(())
        })
        .await;
        result.unwrap();
        assert_eq!(span.status, Status::Unset);
        assert!(
            !span
                .attributes
                .iter()
                .any(|attr| attr.key.as_str() == "error.type")
        );
    }
}
