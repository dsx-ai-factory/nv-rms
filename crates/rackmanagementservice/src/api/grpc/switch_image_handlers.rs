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
use tokio_util::sync::CancellationToken;

use super::server::RackManagerServiceImpl;
use crate::api::grpc::conversions::{
    flatten_node_info, proto_node_type_to_domain, proto_node_type_to_string,
    timestamp_from_datetime,
};
use crate::api::grpc::firmware_handlers::resolve_firmware_file;
use crate::domain::node::{Node, NodeKind};
use crate::domain::rack::NodeConfig;
use crate::nodes::NodeInstance;
use crate::nodes::switch_gb200_nvidia::{
    NVOS_PARTITION_1_ID, NVOS_PARTITION_2_ID, SwitchGb200Nvidia, SwitchSystemImageState,
    describe_install_poll, infer_target_build_id, is_http_unauthorized_error,
    is_install_handoff_to_reboot, is_install_poll_success, is_install_progress_state,
    is_install_reboot_transition_state, is_retryable_install_poll_error,
    is_retryable_steady_state_error, is_uninstall_poll_success, system_image_listing_contains,
};
use crate::orchestrator::job_lifecycle::{JobError, JobFailure, JobId};
use crate::orchestrator::job_tracker::{JobType, RmsJobHandle};
use crate::orchestrator::stage_timeline::{Stage, StageTimeline, fail_job};
use crate::transport::ssh::SftpUploadOptions;
use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::insert_json_field;
use librms::protos::rack_manager as rm;

pub(crate) type SharedSwitchSystemImageNode =
    Arc<tokio::sync::Mutex<Box<dyn SwitchSystemImageNode + Send + Sync>>>;

/// A switch that `UpdateSwitchSystemImage` admitted, carried from the admission
/// pass through the NVUE setup pass to the worker that is spawned for it.
///
/// Only per-switch state lives here. The rest of what a worker needs is either
/// the same for every switch in the batch (`image_filename`, `local_file_path`,
/// `target_build_id`, `sftp_upload_options`) and so is cloned per worker at the
/// spawn site, or is derived from `pending` (the cancellation token).
struct AdmittedSwitchImageJob {
    pending: RmsJobHandle,
    switch: Box<dyn SwitchSystemImageNode + Send + Sync>,
    rack_id: String,
    node_id: String,
    user: String,
    pass: String,
}

/// Capability required by the switch system-image workflow.
///
/// This is separate from the other switch capability traits because image
/// updates need mutable password recovery after boot and a narrower set of NVOS
/// image operations.
#[async_trait]
pub(crate) trait SwitchSystemImageNode {
    fn nvue_client(&self) -> Option<&nvue_client::SharedClient>;

    async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState>;
    async fn poll_firmware_task(
        &self,
        task_id: &str,
    ) -> Result<crate::domain::node::FirmwareTaskStatus>;
    async fn uninstall_system_image(&self) -> Result<String>;
    async fn list_system_images(&self) -> Result<serde_json::Value>;
    async fn push_system_image_file(
        &self,
        local_file_path: &str,
        image_filename: &str,
        sftp_upload_options: SftpUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()>;
    async fn install_system_image(&self, image_filename: &str) -> Result<String>;
    async fn recover_admin_password_after_boot(&mut self, target_password: &str) -> Result<()>;
}

#[async_trait]
impl SwitchSystemImageNode for SwitchGb200Nvidia {
    fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
        self.optional_nvue_client()
    }

    async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
        let _op_guard = self.op_lock.lock().await;
        self.get_normalized_system_image_state().await
    }

    async fn poll_firmware_task(
        &self,
        task_id: &str,
    ) -> Result<crate::domain::node::FirmwareTaskStatus> {
        Node::poll_firmware_task(self, task_id).await
    }

    async fn uninstall_system_image(&self) -> Result<String> {
        let _op_guard = self.op_lock.lock().await;
        self.uninstall_system_image().await
    }

    async fn list_system_images(&self) -> Result<serde_json::Value> {
        let _op_guard = self.op_lock.lock().await;
        self.list_system_images().await
    }

    async fn push_system_image_file(
        &self,
        local_file_path: &str,
        image_filename: &str,
        sftp_upload_options: SftpUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;

        self.push_system_image_file_with_options(
            local_file_path,
            image_filename,
            sftp_upload_options,
            cancel,
        )
        .await
    }

    async fn install_system_image(&self, image_filename: &str) -> Result<String> {
        let _op_guard = self.op_lock.lock().await;
        self.install_system_image(image_filename).await
    }

    async fn recover_admin_password_after_boot(&mut self, target_password: &str) -> Result<()> {
        self.recover_admin_password_after_boot(target_password)
            .await
    }
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
        NodeInstance::SwitchGb200Nvidia(node) | NodeInstance::SwitchVrnvl72Nvidia(node) => {
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
    wait_for_uninstall_completion_inner(switch, uninstall_job_id, cancel)
        .instrument(tracing::info_span!(
            "wait_for_uninstall_completion",
            job_id = job_id,
            uninstall_job_id = uninstall_job_id,
        ))
        .await
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
        match switch.poll_firmware_task(uninstall_job_id).await {
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
// flakiness).
async fn wait_for_install_completion(
    switch: &(dyn SwitchSystemImageNode + Send + Sync),
    install_job_id: &str,
    cancel: &CancellationToken,
    job_id: &str,
) -> Result<()> {
    use tracing::Instrument;
    wait_for_install_completion_inner(switch, install_job_id, cancel)
        .instrument(tracing::info_span!(
            "wait_for_install_completion",
            job_id = job_id,
            install_job_id = install_job_id,
        ))
        .await
}

async fn wait_for_install_completion_inner(
    switch: &(dyn SwitchSystemImageNode + Send + Sync),
    install_job_id: &str,
    cancel: &CancellationToken,
) -> Result<()> {
    let deadline = Instant::now() + INSTALL_COMPLETION_TIMEOUT;
    let mut reboot_transition_observed = false;

    while Instant::now() < deadline {
        if cancel.is_cancelled() {
            return Err(RmsError::internal(
                "switch system image job cancelled during install polling",
            ));
        }
        match switch.poll_firmware_task(install_job_id).await {
            Err(e) => {
                if is_http_unauthorized_error(&e) {
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
                        "install poll hit transient transport error during reboot"
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

                if is_install_reboot_transition_state(&status) {
                    tracing::info!(
                        install_job_id,
                        "install reports reboot transition; moving to steady-state wait"
                    );
                    return Ok(());
                }
                if is_install_poll_success(&status) {
                    return Ok(());
                }
                if is_install_handoff_to_reboot(&status) {
                    tracing::info!(
                        install_job_id,
                        "install reports completed handoff; moving to steady-state wait"
                    );
                    return Ok(());
                }
                if status.state == "action_error"
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
                if status.state == "action_error" || status.state == "action_failed" {
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
    wait_for_target_steady_state_inner(
        switch,
        target_build_id,
        target_user,
        target_password,
        cancel,
    )
    .instrument(tracing::info_span!(
        "wait_for_target_steady_state",
        job_id = job_id,
        target_build_id = target_build_id,
    ))
    .await
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
        let mut queued_jobs = Vec::new();

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
            // host endpoint for the work, and only carry BMC data when it is
            // valid so future power paths can use it safely. Malformed BMC
            // endpoint configuration is dropped with a warning rather than
            // failing the node.
            let bmc_endpoint = match flat.optional_bmc_endpoint() {
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
                rack_id,
                node_id,
                user,
                pass,
            });
        }

        // NVUE setup runs only once the whole batch is admitted. Sealing a
        // child inside the admission loop can leave the parent momentarily
        // all-terminal (every switch admitted so far having failed setup); the
        // reaper's parent refresh would seal it there, and every switch still
        // waiting to be admitted would be refused with an internal "parent is
        // terminal" error rather than being updated.
        let admitted_count = admitted.len();
        for admitted_job in admitted {
            if let Err(e) = self
                .initialize_nvue_client(admitted_job.switch.nvue_client(), None)
                .await
            {
                tracing::warn!(
                    node = %admitted_job.node_id,
                    rack = %admitted_job.rack_id,
                    error = %e.message,
                    "skipping switch image update: failed to initialize NVUE client"
                );
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: admitted_job.node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: e.message.clone(),
                });

                let error_code = if e.code == ErrorCode::FailedPrecondition {
                    JobError::FailedPrecondition
                } else {
                    JobError::ClientError
                };
                let child_id = JobId::from(admitted_job.pending.id().as_ref());
                admitted_job
                    .pending
                    .fail(JobFailure::new(error_code, e.message));
                self.job_tracker.refresh_parent_for_child(&child_id);

                skipped += 1;
                continue;
            }

            jobs.push(rm::SwitchSystemImageUpdateJobInfo {
                node_id: admitted_job.node_id.clone(),
                job_id: admitted_job.pending.id().to_string(),
            });

            queued_jobs.push(admitted_job);
        }

        let jobs_created = jobs.len() as u32;
        if jobs_created == 0 {
            batch.status = rm::ReturnCode::Failure.into();
            batch.message = "no switch system image jobs created".into();
            // Admitted children that failed NVUE setup already drove the parent
            // to a terminal state through child aggregation.
            if admitted_count == 0 {
                self.job_tracker
                    .mark_failed_message(&parent_id, &batch.message);
            }
            tracing::error!(total_nodes, "{}", batch.message);
            return Ok(tonic::Response::new(update_switch_system_image_response(
                batch,
                jobs,
                total_nodes,
                0,
                skipped,
            )));
        }

        for queued_job in queued_jobs {
            let AdmittedSwitchImageJob {
                pending,
                switch,
                rack_id,
                node_id,
                user,
                pass,
            } = queued_job;

            // The worker outlives this handler, so it takes owned copies of the
            // request values the whole batch shares.
            let image_filename = r.image_filename.clone();
            let local_file_path = local_file_path.clone();
            let target_build_id = target_build_id.clone();
            let sftp_upload_options = self.sftp_upload_options;
            let switch = Arc::new(tokio::sync::Mutex::new(switch));
            let cancel = pending.cancellation_token();

            // The switch workflow seals its own job through the tracked handle.
            // Running it under a tracked supervisor still records panic/abort
            // failures and avoids clobbering its terminal state on normal exit.
            self.job_tracker
                .spawn_job(pending, move |job| async move {
                    run_switch_system_image_job(
                        job,
                        switch,
                        &image_filename,
                        &local_file_path,
                        &target_build_id,
                        &user,
                        &pass,
                        &rack_id,
                        &node_id,
                        sftp_upload_options,
                        &cancel,
                    )
                    .await;
                })
                .detach();
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

// Top-level per-node orchestrator for UpdateSwitchSystemImage.
// `runSwitchSystemImageJob` function's stages: inspect state, stage image,
// trigger install, wait for install handoff, wait for target steady state.

/// Predefined, ordered stage sequence driving `run_switch_system_image_job`.
/// Each `(name, description)` pair seeds a `Stage`'s name and immutable,
/// imperative description once, here, instead of retyping either at every
/// `timeline`/`job.progress`/`tracing` call site below.
fn switch_system_image_job_stages() -> Vec<Stage> {
    [
        ("inspect_state", "Inspect current switch system image state"),
        ("cleanup_old_partition", "Clean up unused partition image"),
        (
            "stage_image",
            "Ensure switch system image is staged on target",
        ),
        ("trigger_install", "Trigger switch system image install"),
        (
            "wait_for_install_handoff",
            "Wait for switch system image install to start reboot",
        ),
        (
            "wait_for_target_steady_state",
            "Wait for switch to return with target system image",
        ),
    ]
    .into_iter()
    .map(|(name, description)| Stage::new(name.to_owned(), description.to_owned()))
    .collect()
}

/// Builds the `StageTimeline` driving `run_switch_system_image_job`, tagged
/// with the node/rack it runs against so `StageTimeline`'s own stage
/// transition logs carry them without repeating them at every call site.
/// Factored out of `run_switch_system_image_job` so this wiring -- easy to
/// silently drop when refactoring -- has a direct unit test.
fn switch_system_image_job_timeline(job_id: &str, rack_id: &str, node_id: &str) -> StageTimeline {
    StageTimeline::with_stages(job_id, switch_system_image_job_stages())
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
    job: RmsJobHandle,
    switch: SharedSwitchSystemImageNode,
    image_filename: &str,
    local_file_path: &str,
    target_build_id: &str,
    switch_username: &str,
    switch_password: &str,
    rack_id: &str,
    node_id: &str,
    sftp_upload_options: SftpUploadOptions,
    cancel: &CancellationToken,
) {
    // Copy the ID out so the many `job_id` log fields keep working without
    // holding an immutable borrow on `job`, which is moved when the worker
    // seals its terminal state via `complete`/`fail`.
    let job_id_owned = job.id().to_string();
    let job_id = job_id_owned.as_str();
    let mut timeline = switch_system_image_job_timeline(job_id, rack_id, node_id);
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

    // ── Stage 1: inspect current state ──
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
    if before_state.current_build_id == target_build_id
        && before_state.next_build_id == target_build_id
    {
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
        job.complete("Completed", result_json);
        return;
    }

    // ── Stage 1.5: Clean up unused partition image ──
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

    // Determine the unused partition ID from the current partition
    let (unused_build_id, unused_partition) = match before_state.current_partition.as_str() {
        NVOS_PARTITION_2_ID => (
            before_state.partition1_build_id.as_str(),
            NVOS_PARTITION_1_ID,
        ),
        NVOS_PARTITION_1_ID => (
            before_state.partition2_build_id.as_str(),
            NVOS_PARTITION_2_ID,
        ),
        _ => {
            let error_message = format!(
                "unrecognized current partition ID: {}",
                before_state.current_partition
            );
            fail_stage(
                &mut timeline,
                job,
                "failed",
                &error_message,
                serde_json::json!({"job_id": job_id, "partition_id": before_state.current_partition.clone()}),
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

    if !unused_build_id.is_empty() {
        tracing::info!(
            job_id,
            "cleaning up unused {unused_partition} image with build ID {unused_build_id}"
        );

        // Remove image from unused partition on switch - this just gives us a job ID to track
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

        // Track progress of uninstall job until completion
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
    } else {
        tracing::info!(
            job_id,
            "Stage {stage} skipped; no unused partition image to clean up"
        );
        timeline.skip_current(serde_json::json!({}));
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

    // ── Stage 4: wait for install to hand off to reboot ──
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
        "triggering install job"
    );

    {
        let sw = switch.lock().await;
        if let Err(e) =
            wait_for_install_completion(sw.as_ref(), &install_job_id, cancel, job_id).await
        {
            let error_message = format!(
                "NVUE install did not hand off cleanly (install_job_id={install_job_id}): {}",
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
    tracing::debug!(job_id, stage = %stage, "stage completed: install handed off to reboot");

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

    let result_json = build_result_json(
        "success",
        &Some(install_job_id),
        &Some(before_state),
        &Some(after_state),
        image_visible_before_stage,
        image_visible_after_stage,
        &timeline,
    );
    job.complete("Completed", result_json);
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
    use crate::domain::node::NodeType;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

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
        let timeline = switch_system_image_job_timeline("job-1", "rack-01", "sw-01");
        assert_eq!(timeline.node(), "sw-01");
        assert_eq!(timeline.rack(), "rack-01");
    }

    // ── wait_for_uninstall tests ──────────────────────────────────────

    fn action_running_resp() -> serde_json::Value {
        serde_json::json!({"state": "action_running"})
    }

    fn action_success_resp() -> serde_json::Value {
        serde_json::json!({"state": "action_success"})
    }

    #[test]
    fn create_switch_system_image_node_accepts_vrnvl72_switch() {
        let config = NodeConfig {
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
        };

        assert!(create_switch_system_image_node(&config, "rack-01").is_ok());
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

        wait_for_uninstall_completion(&sw, uninstall_job_id, &cancel, system_image_job_id)
            .await
            .unwrap();
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

        let result = wait_for_uninstall_completion(&sw, uninstall_job_id, &cancel, "job-id").await;
        let err = result.unwrap_err();
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

        let result = wait_for_uninstall_completion(&sw, "15", &cancel, "job-id").await;
        let err = result.unwrap_err();
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

        let result = wait_for_uninstall_completion(&sw, uninstall_job_id, &cancel, "job-id").await;
        let err = result.unwrap_err();
        assert!(
            err.message.contains("timed out"),
            "expected timeout error, got: {}",
            err.message
        );
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
    }

    /// Mock switch node that advances through inspect + cleanup (uninstall
    /// skipped: the unused partition has no image) and then fails the stage-2
    /// SFTP push according to `failure_mode`.
    struct StageTwoPushMock {
        failure_mode: PushFailureMode,
    }

    #[async_trait]
    impl SwitchSystemImageNode for StageTwoPushMock {
        fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
            None
        }

        async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
            Ok(SwitchSystemImageState {
                current_partition: NVOS_PARTITION_1_ID.to_owned(),
                current_build_id: "nvos-old".to_owned(),
                next_build_id: "nvos-old".to_owned(),
                // Empty unused (partition2) build id -> stage 1.5 skips uninstall.
                partition2_build_id: String::new(),
                ..Default::default()
            })
        }

        async fn poll_firmware_task(
            &self,
            _task_id: &str,
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
            _sftp_upload_options: SftpUploadOptions,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            match self.failure_mode {
                PushFailureMode::CancelledError => {
                    Err(RmsError::cancelled("SSH SFTP remote file write cancelled"))
                }
                PushFailureMode::GenuineFailure => Err(RmsError::internal("SFTP write failed")),
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
    }

    async fn run_stage_two_push_job(failure_mode: PushFailureMode) -> JobInfo {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .expect("create pending job");
        let job_id = pending.id().to_string();
        let cancel = pending.cancellation_token();

        let switch: SharedSwitchSystemImageNode =
            Arc::new(TokioMutex::new(Box::new(StageTwoPushMock { failure_mode })));

        let handle = tracker.spawn_job(pending, move |job| async move {
            run_switch_system_image_job(
                job,
                switch,
                "img.bin",
                "/nonexistent/img.bin",
                "nvos-new",
                "admin",
                "password",
                "rack-01",
                "sw-01",
                SftpUploadOptions::default(),
                &cancel,
            )
            .await;
        });
        handle.wait().await.expect("supervisor join");

        tracker.get_job(&job_id).expect("job should exist")
    }

    #[tokio::test]
    async fn stage_two_push_cancelled_error_records_cancelled_outcome() {
        let info = run_stage_two_push_job(PushFailureMode::CancelledError).await;

        assert_eq!(info.state, JobState::Failed);
        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
        assert_eq!(
            result["status"], "cancelled",
            "an ErrorCode::Cancelled push error should record a cancelled \
             outcome even when the job token is not cancelled"
        );
    }

    #[tokio::test]
    async fn stage_two_push_failure_records_failed_outcome() {
        let info = run_stage_two_push_job(PushFailureMode::GenuineFailure).await;

        assert_eq!(info.state, JobState::Failed);
        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
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
        let info = run_stage_two_push_job(PushFailureMode::GenuineFailure).await;

        let result: serde_json::Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        let cleanup_stage = stages
            .iter()
            .find(|s| s["name"] == "cleanup_old_partition")
            .expect("cleanup_old_partition stage should be present");
        assert_eq!(cleanup_stage["status"], "skipped");
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

        async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
            Err(RmsError::new(ErrorCode::Unauthenticated, "unauthorized"))
        }

        async fn poll_firmware_task(
            &self,
            _task_id: &str,
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
            _sftp_upload_options: SftpUploadOptions,
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

        let err =
            wait_for_target_steady_state_inner(&mut sw, "nvos-new", "admin", "newpass", &cancel)
                .await
                .expect_err("SSH auth failure during recovery should surface as an error");

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

        let err =
            wait_for_target_steady_state_inner(&mut sw, "nvos-new", "admin", "newpass", &cancel)
                .await
                .expect_err("recovery failure should surface as an error");

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert_eq!(err.message, "SSH auth to 10.0.0.1: connection closed");
    }
}
