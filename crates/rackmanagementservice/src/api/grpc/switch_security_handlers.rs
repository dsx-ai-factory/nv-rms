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

//! Handlers for switch security RPCs.
//!
//! `UpdateSwitchSystemPassword` creates asynchronous per-switch jobs and
//! returns a parent job ID that callers can poll with `GetJobStatus`.
//! Per-node results in the RPC response report job admission only; final
//! password update status lives in the job tracker.

use std::collections::HashSet;
use std::sync::Arc;

use librms::protos::rack_manager as rm;
use serde::Serialize;

use super::server::RackManagerServiceImpl;
use crate::api::grpc::conversions::{flatten_node_info, proto_node_type_to_domain};
use crate::domain::node::NodeKind;
use crate::domain::rack::NodeConfig;
use crate::nodes::NodeInstance;
use crate::nodes::switch_gb200_nvidia::{SwitchGb200Nvidia, SwitchSystemPasswordUpdateOutcome};
use crate::orchestrator::job_lifecycle::{JobError, JobFailure};
use crate::orchestrator::job_tracker::{JobType, RmsJobHandle};
use crate::utilities::error::{ErrorCode, RmsError};

fn redact_password_rotation_error(message: &str, secrets: &[&str]) -> String {
    // NVUE and lower transport layers can echo rejected credentials in error
    // bodies. Scrub endpoint and requested credentials before the message
    // enters job status or logs.
    let mut redacted = message.to_owned();
    let mut secrets = secrets
        .iter()
        .copied()
        .filter(|secret| !secret.is_empty())
        .collect::<Vec<_>>();

    // Replace longer values first so a shorter overlapping password cannot
    // prevent the complete longer password from being redacted.
    secrets.sort_unstable_by_key(|secret| std::cmp::Reverse(secret.len()));

    for secret in secrets {
        redacted = redacted.replace(secret, "XXXX");
    }

    nvfwupd::utils::Util::redact_secret_fields(&redacted)
}

fn validate_unique_switch_password_targets(devices: &[rm::NodeInfo]) -> Result<(), tonic::Status> {
    let mut seen_targets = HashSet::with_capacity(devices.len());

    for device in devices {
        let target = (device.rack_id.as_str(), device.node_id.as_str());

        if !seen_targets.insert(target) {
            return Err(tonic::Status::invalid_argument(format!(
                "duplicate switch target in request: rack_id={} node_id={}",
                device.rack_id, device.node_id
            )));
        }
    }

    Ok(())
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_update_switch_system_password(
        &self,
        req: tonic::Request<rm::UpdateSwitchSystemPasswordRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateSwitchSystemPasswordResponse>, tonic::Status>
    {
        let r = req.into_inner();
        if r.username.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "username is required and must be non-empty",
            ));
        }

        if !r.username.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(tonic::Status::invalid_argument(
                "username may only contain ASCII letters and digits",
            ));
        }

        if r.password.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "password is required and must be non-empty",
            ));
        }

        let devices = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        if devices.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "nodes is required and must contain at least one device",
            ));
        }

        let total_nodes = devices.len() as u32;
        let username: Arc<str> = Arc::from(r.username);
        let password: Arc<str> = Arc::from(r.password);

        validate_unique_switch_password_targets(&devices)?;

        tracing::info!(
            target_user = username.as_ref(),
            device_count = total_nodes,
            "update switch system password request received"
        );

        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::new(),
            job_id: String::new(),
            stats: None,
        };

        let parent_rack_id = devices[0].rack_id.as_str();
        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(parent_rack_id, JobType::SwitchSystemPasswordUpdate)
        else {
            batch.message = "failed to create parent job for switch system password update".into();
            return Ok(tonic::Response::new(
                rm::UpdateSwitchSystemPasswordResponse {
                    response: Some(batch),
                },
            ));
        };
        batch.job_id = parent_id.clone();

        let mut queued_jobs = Vec::new();
        let mut skipped = 0u32;

        for device in devices {
            let node_id = device.node_id.clone();
            let rack_id = device.rack_id.clone();

            let prepared = match self.prepare_switch_password_job(device, username.as_ref()) {
                Ok(prepared) => prepared,
                Err(result) => {
                    tracing::warn!(
                        node = %node_id,
                        rack = %rack_id,
                        error = %result.error_message,
                        "skipping switch password update: failed to prepare job"
                    );
                    batch.node_results.push(result);
                    skipped += 1;
                    continue;
                }
            };

            let pending = match self.job_tracker.create_child_job_if_node_idle(
                &parent_id,
                &rack_id,
                &node_id,
                JobType::SwitchSystemPasswordUpdate,
            ) {
                Ok(pending) => pending,
                Err(failure) => {
                    tracing::warn!(
                        node = %node_id,
                        rack = %rack_id,
                        message = %failure.message,
                        "switch password update job rejected"
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

            let job_input = SwitchPasswordJobInput {
                endpoint_username: prepared.endpoint_username,
                endpoint_password: prepared.endpoint_password,
                username: Arc::clone(&username),
                password: Arc::clone(&password),
                rack_id: rack_id.clone(),
                node_id: node_id.clone(),
            };

            queued_jobs.push(SwitchPasswordQueuedJob {
                pending,
                switch: prepared.switch,
                input: job_input,
            });
        }

        let jobs_created = queued_jobs.len() as u32;

        if jobs_created == 0 {
            batch.message = "no switch system password update jobs created".into();
            self.job_tracker
                .mark_failed_message(&parent_id, &batch.message);

            batch.stats = Some(rm::NodeOperationStats {
                total_nodes,
                successful_nodes: 0,
                failed_nodes: skipped,
            });

            return Ok(tonic::Response::new(
                rm::UpdateSwitchSystemPasswordResponse {
                    response: Some(batch),
                },
            ));
        }

        for queued in queued_jobs {
            let job_tracker = self.job_tracker.clone();
            let service = self.clone();
            let switch = queued.switch;
            let input = queued.input;
            let node_id = input.node_id.clone();

            batch.node_results.push(rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Success.into(),
                error_message: String::new(),
            });

            job_tracker
                .spawn_job(queued.pending, move |job| async move {
                    run_switch_password_update_job(job, service, switch, input).await;
                })
                .detach();
        }

        batch.status = if skipped == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();

        batch.message = format!(
            "Created {jobs_created} switch system password update jobs out of {total_nodes} nodes. \
             Use GetJobStatus with job_id to track overall batch status."
        );

        // This RPC reports async admission. `successful_nodes` is therefore
        // the number of child jobs accepted, not terminal password updates;
        // callers must use GetJobStatus for execution results.
        batch.stats = Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes: jobs_created,
            failed_nodes: skipped,
        });

        tracing::info!(
            target_user = username.as_ref(),
            parent_job_id = %batch.job_id,
            jobs_created,
            skipped,
            total_nodes,
            "created switch password update jobs"
        );

        Ok(tonic::Response::new(
            rm::UpdateSwitchSystemPasswordResponse {
                response: Some(batch),
            },
        ))
    }

    fn prepare_switch_password_job(
        &self,
        device: rm::NodeInfo,
        username: &str,
    ) -> std::result::Result<PreparedSwitchPasswordJob, rm::NodeOperationResult> {
        let node_id = device.node_id.clone();
        let rack_id = device.rack_id.clone();

        tracing::info!(
            node = %node_id,
            rack = %rack_id,
            target_user = %username,
            "processing switch password rotation request"
        );

        let node_type_raw = device.r#type.unwrap_or(rm::NodeType::Unspecified as i32);

        let Some(node_type) = proto_node_type_to_domain(node_type_raw) else {
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                target_user = %username,
                node_type = node_type_raw,
                "skipping password rotation for unknown node type"
            );

            let error_message = format!("device {node_id} is not a switch (type={node_type_raw})");

            return Err(rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message,
            });
        };

        if node_type.kind() != NodeKind::Switch {
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                target_user = %username,
                node_type = %node_type,
                "skipping password rotation for non-switch node"
            );

            let error_message = format!("device {node_id} is not a switch (type={node_type})");

            return Err(rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message,
            });
        }

        let flat = match flatten_node_info(&device) {
            Ok(flat) => flat,
            Err(e) => {
                let message = e.message;

                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %message,
                    "skipping password rotation due to invalid endpoint credentials"
                );

                return Err(rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message: message,
                });
            }
        };

        let (endpoint_username, endpoint_password) = match flat.creds_for_node_type(node_type) {
            Some((username, password)) => (username.to_owned(), password.to_owned()),
            None => {
                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    "skipping password rotation due to missing switch credentials"
                );

                let error_message = format!("Missing host credentials for switch {node_id}");

                return Err(rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message,
                });
            }
        };

        // Password rotation is a host/NVOS workflow. The host endpoint is
        // required for the operation; BMC details are optional and only
        // retained when valid for later Redfish power use. Malformed BMC
        // endpoint configuration is dropped with a warning rather than
        // failing the node.
        let bmc_endpoint = match flat.optional_bmc_endpoint() {
            Ok(endpoint) => endpoint,
            Err(e) => {
                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %e.message,
                    "ignoring malformed BMC endpoint for switch password rotation"
                );

                None
            }
        };

        let host_endpoint = match flat.switch_host_management_endpoint() {
            Ok(endpoint) => endpoint,
            Err(e) => {
                let message = e.message;

                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %message,
                    "skipping password rotation due to invalid switch host endpoint"
                );

                let error_message =
                    format!("Invalid host endpoint for switch {node_id}: {message}");

                return Err(rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message,
                });
            }
        };

        let config = NodeConfig {
            id: node_id.clone(),
            node_type,
            bmc_endpoint,
            host_endpoint: Some(host_endpoint),
            expected_inventory: None,
        };

        let switch_result = NodeInstance::create_switch_password(&config, &rack_id);

        let switch = match switch_result {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %e.message,
                    "failed to construct switch for password rotation"
                );

                let error_message = format!("Failed to construct switch: {}", e.message);

                return Err(rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message,
                });
            }
        };

        Ok(PreparedSwitchPasswordJob {
            switch,
            endpoint_username,
            endpoint_password,
        })
    }
}

/// Switch state prepared before a per-node job is registered.
///
/// Endpoint credentials are kept separately from the target username/password
/// because the endpoint user is the current management identity and may differ
/// from the NVOS user whose password is being updated.
struct PreparedSwitchPasswordJob {
    switch: Box<SwitchGb200Nvidia>,
    endpoint_username: String,
    endpoint_password: String,
}

/// Pending job plus owned worker state.
///
/// Parent job creation can still fail after child jobs are reserved, so jobs
/// stay unstarted in this wrapper until the parent job exists.
struct SwitchPasswordQueuedJob {
    pending: RmsJobHandle,
    switch: Box<SwitchGb200Nvidia>,
    input: SwitchPasswordJobInput,
}

/// Secret-free status payload stored in the job tracker.
#[derive(Serialize)]
struct SwitchPasswordJobResult<'a> {
    rack_id: &'a str,
    node_id: &'a str,
    username: &'a str,
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<&'a str>,
}

/// Owned worker input for one switch password update job.
///
/// Passwords remain task-local and are never serialized into the job result.
struct SwitchPasswordJobInput {
    endpoint_username: String,
    endpoint_password: String,
    username: Arc<str>,
    password: Arc<str>,
    rack_id: String,
    node_id: String,
}

fn switch_password_job_result_json(
    rack_id: &str,
    node_id: &str,
    username: &str,
    status: &str,
    phase: Option<&str>,
    revision_id: Option<&str>,
    error_message: Option<&str>,
) -> String {
    // Keep the job result to phase and revision metadata only. Request
    // passwords must not cross into the serialized job payload.
    serde_json::to_string(&SwitchPasswordJobResult {
        rack_id,
        node_id,
        username,
        status,
        phase,
        revision_id,
        error_message,
    })
    .unwrap_or_else(|_| "{}".to_owned())
}

fn switch_password_job_error(error: &RmsError) -> JobError {
    match error.code {
        ErrorCode::InvalidArgument | ErrorCode::FailedPrecondition => JobError::InvalidArgument,
        ErrorCode::Unauthenticated => JobError::Unauthenticated,
        ErrorCode::Timeout => JobError::Timeout,
        ErrorCode::NotFound => JobError::TargetNotFound,
        ErrorCode::Unavailable | ErrorCode::ConnectionRefused | ErrorCode::DnsResolutionFailed => {
            JobError::ClientError
        }
        ErrorCode::Cancelled => JobError::Other,
        ErrorCode::AlreadyExists | ErrorCode::Internal | ErrorCode::Unimplemented => {
            JobError::Internal
        }
    }
}

fn fail_switch_password_update_job(
    job: RmsJobHandle,
    input: &SwitchPasswordJobInput,
    error: RmsError,
) {
    let error_code = switch_password_job_error(&error);

    let error_message = redact_password_rotation_error(
        &error.message,
        &[input.endpoint_password.as_str(), input.password.as_ref()],
    );

    let result_json = switch_password_job_result_json(
        &input.rack_id,
        &input.node_id,
        input.username.as_ref(),
        "failed",
        None,
        None,
        Some(&error_message),
    );

    tracing::error!(
        node = %input.node_id,
        rack = %input.rack_id,
        target_user = input.username.as_ref(),
        error = %error_message,
        "switch password update job failed"
    );

    job.fail(JobFailure::new(error_code, error_message).with_result_json(result_json));
}

/// Execute one accepted switch password update job.
///
/// The worker initializes NVUE transport with endpoint credentials first. If
/// those credentials are rejected for the target endpoint user, it probes the
/// requested password as a resume signal for a prior partial success. If NVUE
/// cannot proceed for the `admin` endpoint, RMS attempts the factory-default
/// `admin/admin` expired-password recovery over SSH.
async fn run_switch_password_update_job(
    job: RmsJobHandle,
    service: RackManagerServiceImpl,
    mut switch: Box<SwitchGb200Nvidia>,
    input: SwitchPasswordJobInput,
) {
    let cancel = job.cancellation_token();

    let outcome_result = async {
        job.progress("Initializing switch password update");

        let target_is_endpoint_user =
            input.endpoint_username.as_str() == input.username.as_ref();
        let target_is_admin_endpoint = target_is_endpoint_user && input.username.as_ref() == "admin";

        // Initialize transport before mutating the switch. If factory reset
        // replaced NVUE credentials or TLS material, admin recovery can still
        // proceed through the switch's expired-password SSH prompt.
        if let Err(error) = service
            .initialize_nvue_client(switch.optional_nvue_client(), None)
            .await
        {
            if target_is_admin_endpoint {
                if cancel.is_cancelled() {
                    return Err(RmsError::internal(
                        "switch password update job cancelled before factory-default admin recovery",
                    ));
                }

                job.progress("Recovering factory-default admin password");

                switch
                    .recover_admin_password_after_boot(input.password.as_ref())
                    .await?;

                return Ok(SwitchSystemPasswordUpdateOutcome::RecoveredFactoryDefault);
            }

            if error.code != ErrorCode::Unauthenticated || !target_is_endpoint_user {
                return Err(error);
            }

            tracing::debug!(
                node = %input.node_id,
                rack = %input.rack_id,
                target_user = input.username.as_ref(),
                "continuing switch password update after initial credential rejection"
            );
        }

        if cancel.is_cancelled() {
            return Err(RmsError::internal(
                "switch password update job cancelled before updating password",
            ));
        }

        job.progress("Updating switch system password");

        // Do not observe cancellation after this point: once NVUE mutation
        // starts, completing apply/save is safer than abandoning partial state.
        switch
            .update_system_user_password_persisted_resumable(
                input.username.as_ref(),
                input.password.as_ref(),
                None,
            )
            .await
    }
    .await;

    let outcome = match outcome_result {
        Ok(outcome) => outcome,
        Err(error) => {
            fail_switch_password_update_job(job, &input, error);

            return;
        }
    };

    let result_json = switch_password_job_result_json(
        &input.rack_id,
        &input.node_id,
        input.username.as_ref(),
        "completed",
        Some(outcome.phase()),
        outcome.revision_id(),
        None,
    );

    tracing::info!(
        node = %input.node_id,
        rack = %input.rack_id,
        target_user = input.username.as_ref(),
        "switch password update job completed"
    );

    job.complete("Completed", result_json);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
    use crate::orchestrator::job_lifecycle::JobState;
    use crate::orchestrator::job_tracker::{JobTracker, JobType};
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use crate::transport::ssh::SftpUploadOptions;

    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const OPERATOR_CURRENT_AUTH: &str = "Basic b3BlcmF0b3I6Y3VycmVudC1wYXNzd29yZA==";
    const OPERATOR_NEXT_AUTH: &str = "Basic b3BlcmF0b3I6bmV4dC1wYXNzd29yZA==";

    fn node_info(rack_id: &str, node_id: &str) -> rm::NodeInfo {
        rm::NodeInfo {
            rack_id: rack_id.to_string(),
            node_id: node_id.to_string(),
            ..Default::default()
        }
    }

    fn switch_password_node_info(rack_id: &str, node_id: &str) -> rm::NodeInfo {
        switch_password_node_info_with_port(rack_id, node_id, 443)
    }

    fn switch_password_node_info_with_port(
        rack_id: &str,
        node_id: &str,
        port: u32,
    ) -> rm::NodeInfo {
        rm::NodeInfo {
            rack_id: rack_id.to_string(),
            node_id: node_id.to_string(),
            r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
            host_endpoint: Some(rm::Endpoint {
                interface: Some(rm::NetworkInterface {
                    ip_address: "127.0.0.1".to_owned(),
                    mac_address: String::new(),
                    host_name: Some("switch.example.com".to_owned()),
                }),
                port,
                credentials: Some(rm::Credentials {
                    auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                        username: "admin".to_owned(),
                        password: "current-password".to_owned(),
                    })),
                }),
            }),
            ..Default::default()
        }
    }

    fn test_service_with_tracker(job_tracker: Arc<JobTracker>) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker,
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            switch_tls_roots: SwitchTlsRoots {
                insecure_switch: true,
                ..SwitchTlsRoots::default()
            },
            sftp_upload_options: SftpUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        }
    }

    #[test]
    fn redact_password_rotation_error_masks_password_fields() {
        let message = r#"revision failed: {"hashed-password":"$6$salt$hash","password":"secret"} password=plain current-old next-new"#;

        let redacted = redact_password_rotation_error(message, &["current-old", "next-new"]);

        assert!(!redacted.contains("$6$salt$hash"));
        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("plain"));
        assert!(!redacted.contains("current-old"));
        assert!(!redacted.contains("next-new"));
        assert!(redacted.contains("hashed-password"));
        assert!(redacted.contains("password"));
    }

    #[test]
    fn redact_password_rotation_error_masks_overlapping_passwords() {
        let redacted = redact_password_rotation_error(
            "requested credential secret-next was rejected",
            &["secret", "secret-next"],
        );

        assert_eq!(redacted, "requested credential XXXX was rejected");
    }

    #[test]
    fn validate_unique_switch_password_targets_rejects_duplicate_rack_node() {
        let devices = vec![
            node_info("rack-01", "node-01"),
            node_info("rack-01", "node-02"),
            node_info("rack-01", "node-01"),
        ];

        let error = validate_unique_switch_password_targets(&devices).unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        assert!(
            error
                .message()
                .contains("duplicate switch target in request: rack_id=rack-01 node_id=node-01")
        );
    }

    #[test]
    fn validate_unique_switch_password_targets_allows_same_node_in_different_racks() {
        let devices = vec![
            node_info("rack-01", "node-01"),
            node_info("rack-02", "node-01"),
        ];

        validate_unique_switch_password_targets(&devices).unwrap();
    }

    #[tokio::test]
    async fn update_switch_system_password_fails_when_child_job_cannot_be_created() {
        let tracker = Arc::new(JobTracker::builder().max_tracked_jobs(1).build().unwrap());
        let service = test_service_with_tracker(Arc::clone(&tracker));

        let response = service
            .handle_update_switch_system_password(tonic::Request::new(
                rm::UpdateSwitchSystemPasswordRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![switch_password_node_info("rack-01", "sw-01")],
                    }),
                    username: "admin".to_owned(),
                    password: "next-password".to_owned(),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        let batch = response.response.expect("batch response");

        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert!(!batch.job_id.is_empty());
        let parent_id = batch.job_id.clone();

        assert_eq!(
            batch.message,
            "no switch system password update jobs created"
        );

        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].node_id, "sw-01");
        assert_eq!(batch.node_results[0].status, rm::ReturnCode::Failure as i32);

        let stats = batch.stats.expect("stats");

        assert_eq!(stats.total_nodes, 1);
        assert_eq!(stats.successful_nodes, 0);
        assert_eq!(stats.failed_nodes, 1);

        assert_eq!(
            tracker.get_job(&parent_id).unwrap().state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
    }

    #[tokio::test]
    async fn update_switch_system_password_shutdown_cancels_before_password_mutation() {
        let nvue = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({}))
                    .set_delay(Duration::from_millis(100)),
            )
            .mount(&nvue)
            .await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/revision"))
            .respond_with(ResponseTemplate::new(200).set_body_json("55"))
            .mount(&nvue)
            .await;

        let tracker = Arc::new(JobTracker::new());
        let service = test_service_with_tracker(Arc::clone(&tracker));

        let response = service
            .handle_update_switch_system_password(tonic::Request::new(
                rm::UpdateSwitchSystemPasswordRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![switch_password_node_info_with_port(
                            "rack-01",
                            "sw-01",
                            u32::from(nvue.address().port()),
                        )],
                    }),
                    username: "admin".to_owned(),
                    password: "next-password".to_owned(),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        let batch = response.response.expect("batch response");

        assert_eq!(batch.status, rm::ReturnCode::Success as i32);
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].status, rm::ReturnCode::Success as i32);
        assert_eq!(tracker.shutdown(), 1);

        tokio::time::sleep(Duration::from_millis(250)).await;

        let parent = tracker.get_job(&batch.job_id).expect("parent job");

        assert_eq!(parent.state, JobState::Failed);
        assert_eq!(parent.child_job_ids.len(), 1);

        let child = tracker
            .get_job(&parent.child_job_ids[0])
            .expect("child job");

        assert_eq!(child.state, JobState::Failed);

        assert!(
            child.error_message.contains("cancelled"),
            "unexpected child error: {}",
            child.error_message
        );

        let requests = nvue
            .received_requests()
            .await
            .expect("wiremock should record requests");

        assert!(!requests.iter().any(|request| {
            request.method == wiremock::http::Method::Post
                || request.method == wiremock::http::Method::Patch
        }));
    }

    #[tokio::test]
    async fn password_update_probes_requested_password_for_non_admin_endpoint_user() {
        let nvue = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system"))
            .and(header("authorization", OPERATOR_CURRENT_AUTH))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&nvue)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system"))
            .and(header("authorization", OPERATOR_NEXT_AUTH))
            .respond_with(ResponseTemplate::new(418))
            .expect(1)
            .mount(&nvue)
            .await;

        let mut switch = SwitchGb200Nvidia::for_test(&nvue.uri())
            .with_host_credentials_for_test("operator", "current-password");

        let error = switch
            .update_system_user_password_persisted_resumable("operator", "next-password", None)
            .await
            .expect_err("requested password probe should fail");

        nvue.verify().await;

        assert_eq!(error.code, ErrorCode::Internal);
    }

    #[tokio::test]
    async fn password_update_recovers_factory_default_admin_after_nvue_auth_rejections() {
        let nvue = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system"))
            .respond_with(ResponseTemplate::new(401))
            .expect(2)
            .mount(&nvue)
            .await;

        let recovered_passwords = Arc::new(Mutex::new(Vec::new()));
        let observed_passwords = Arc::clone(&recovered_passwords);

        let mut switch = SwitchGb200Nvidia::for_test(&nvue.uri())
            .with_host_credentials_for_test("admin", "stale-password")
            .with_ssh_password_recovery_for_test(move |current_password, new_password| {
                observed_passwords
                    .lock()
                    .unwrap()
                    .push((current_password.to_owned(), new_password.to_owned()));

                Ok(())
            });

        let outcome = switch
            .update_system_user_password_persisted_resumable("admin", "next-password", None)
            .await
            .expect("recover factory-default admin password");

        nvue.verify().await;

        assert_eq!(
            recovered_passwords.lock().unwrap().as_slice(),
            [("admin".to_owned(), "next-password".to_owned())]
        );

        assert_eq!(outcome.phase(), "factory_default_admin_password_recovered");
    }

    #[tokio::test]
    async fn password_update_worker_recovers_factory_default_admin_when_nvue_initialization_fails()
    {
        let nvue = MockServer::start().await;
        let tracker = Arc::new(JobTracker::new());
        let mut service = test_service_with_tracker(Arc::clone(&tracker));

        service.switch_tls_roots = SwitchTlsRoots::default();

        let recovered_passwords = Arc::new(Mutex::new(Vec::new()));
        let observed_passwords = Arc::clone(&recovered_passwords);

        let switch = SwitchGb200Nvidia::for_test(&nvue.uri())
            .with_host_credentials_for_test("admin", "stale-password")
            .with_ssh_password_recovery_for_test(move |current_password, new_password| {
                observed_passwords
                    .lock()
                    .unwrap()
                    .push((current_password.to_owned(), new_password.to_owned()));

                Ok(())
            });

        let pending = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemPasswordUpdate)
            .expect("create password update job");

        let job_id = pending.id().to_string();

        let input = SwitchPasswordJobInput {
            endpoint_username: "admin".to_owned(),
            endpoint_password: "stale-password".to_owned(),
            username: Arc::from("admin"),
            password: Arc::from("next-password"),
            rack_id: "rack-01".to_owned(),
            node_id: "sw-01".to_owned(),
        };

        let worker = tracker.spawn_job(pending, move |job| async move {
            run_switch_password_update_job(job, service, Box::new(switch), input).await;
        });

        worker.wait().await.expect("password update worker");

        assert_eq!(
            recovered_passwords.lock().unwrap().as_slice(),
            [("admin".to_owned(), "next-password".to_owned())]
        );

        let job = tracker.get_job(&job_id).expect("password update job");

        assert_eq!(job.state, JobState::Completed);

        let result: serde_json::Value =
            serde_json::from_str(&job.result_json).expect("parse password update result");

        assert_eq!(result["phase"], "factory_default_admin_password_recovered");
    }

    #[tokio::test]
    async fn update_switch_system_password_rejects_active_node_job() {
        let job_tracker = Arc::new(JobTracker::new());
        let _active_job_id = job_tracker
            .create_job_if_node_idle("rack-01", "sw-01", JobType::FirmwareUpdate)
            .expect("active job");

        let service = test_service_with_tracker(job_tracker);

        let response = service
            .handle_update_switch_system_password(tonic::Request::new(
                rm::UpdateSwitchSystemPasswordRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![switch_password_node_info("rack-01", "sw-01")],
                    }),
                    username: "admin".to_owned(),
                    password: "next-password".to_owned(),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        let batch = response.response.expect("batch response");

        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert!(!batch.job_id.is_empty());
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].node_id, "sw-01");

        assert!(
            batch.node_results[0]
                .error_message
                .contains("job already in progress")
        );
    }
}
