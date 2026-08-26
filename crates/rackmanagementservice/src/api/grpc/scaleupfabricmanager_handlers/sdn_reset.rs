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

//! Async SDN factory-default reset handlers.
//!
//! This RPC submits one in-memory RMS child job per target switch and returns a
//! parent job ID for progress polling through the generic job API. The switch
//! reset itself is idempotent, so RMS intentionally does not persist these jobs:
//! if RMS restarts and forgets in-flight state, the caller can safely submit the
//! same reset request again.

use std::collections::HashSet;

use librms::protos::rack_manager as rm;

use crate::api::grpc::server::RackManagerServiceImpl;
use crate::nodes::SwitchScaleUpManagement;
use crate::orchestrator::job_lifecycle::{JobError, JobFailure};
use crate::orchestrator::job_tracker::{JobTracker, JobType, RmsJobHandle};

type BatchResetResponse = std::result::Result<
    tonic::Response<rm::BatchResetSwitchSdnFactoryDefaultResponse>,
    tonic::Status,
>;

/// Attach aggregate stats and wrap the shared batch response shape.
fn finish_batch(
    mut batch: rm::NodeBatchResponse,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) -> BatchResetResponse {
    batch.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });

    Ok(tonic::Response::new(
        rm::BatchResetSwitchSdnFactoryDefaultResponse {
            response: Some(batch),
        },
    ))
}

/// Per-switch state captured before spawning the child job.
///
/// The optional `request_domain` is an NVL domain selector. It tells RMS which
/// switch certificate set to use for mTLS; it is not a DNS domain or suffix.
struct SdnResetTarget {
    switch: Box<dyn SwitchScaleUpManagement>,
    nvue_client: Option<nvue_client::SharedClient>,
    request_domain: Option<String>,
    tls_server_name: String,
    rack_id: String,
    node_id: String,
}

/// Pending child job and its target payload.
struct PendingSdnReset {
    pending: RmsJobHandle,
    target: SdnResetTarget,
}

/// Result of request validation plus child-job reservation.
struct CreatedSdnResetJobs {
    pending_resets: Vec<PendingSdnReset>,
    skipped: u32,
}

/// Run one switch reset inside a tracked RMS job.
///
/// The switch implementation waits for the runtime reset state before returning.
/// This wrapper owns only job progress, mTLS client initialization, and terminal
/// job state.
async fn run_switch_sdn_factory_default_reset_job(
    job: RmsJobHandle,
    service: RackManagerServiceImpl,
    target: SdnResetTarget,
) {
    let SdnResetTarget {
        switch,
        nvue_client,
        request_domain,
        tls_server_name,
        rack_id,
        node_id,
    } = target;

    let job_id = job.id().to_string();

    tracing::info!(
        job_id = %job_id,
        node = %node_id,
        rack = %rack_id,
        "switch SDN factory-default reset job starting"
    );

    job.progress("Initializing switch NVUE client for SDN factory-default reset");

    // Client initialization happens in the child job because certificate
    // material is selected per target and may use the request NVL domain.
    if let Err(error) = service
        .initialize_nvue_client_with_options(
            nvue_client.as_ref(),
            request_domain.as_deref(),
            Some(&tls_server_name),
            false,
        )
        .await
    {
        let message = format!("failed to initialize switch NVUE client: {}", error.message);

        tracing::warn!(
            job_id = %job_id,
            node = %node_id,
            rack = %rack_id,
            error = %error.message,
            "switch SDN factory-default reset job failed before reset"
        );

        job.fail(JobFailure::new(JobError::Other, message));

        return;
    }

    job.progress("Resetting switch SDN factory-default through NVUE");

    // `reset_sdn_factory_default` returns only after the switch-side runtime
    // state exits factory-reset-in-progress, not merely after action submit.
    match switch.reset_sdn_factory_default().await {
        Ok(action_id) => {
            tracing::info!(
                job_id = %job_id,
                node = %node_id,
                rack = %rack_id,
                action_id = %action_id,
                "switch SDN factory-default reset job completed"
            );

            // RMS stores only in-memory job state for this idempotent reset.
            // The action ID is logged for switch-side audit/correlation.
            job.complete("SDN factory-default reset completed", "");
        }
        Err(error) => {
            let message = format!("failed to reset SDN factory-default: {}", error.message);

            tracing::warn!(
                job_id = %job_id,
                node = %node_id,
                rack = %rack_id,
                error = %error.message,
                "switch SDN factory-default reset job failed"
            );

            job.fail(JobFailure::new(JobError::Other, message));
        }
    }
}

/// Validate targets and build ephemeral switch handles.
///
/// Invalid targets are reported synchronously in `NodeBatchResponse` and are not
/// converted into jobs. Duplicate detection uses `(rack_id, node_id)` so the
/// same physical switch is reset at most once per request.
fn prepare_sdn_reset_targets(
    devices: Vec<rm::NodeInfo>,
    request_domain: Option<&str>,
    batch: &mut rm::NodeBatchResponse,
) -> (Vec<SdnResetTarget>, u32) {
    let mut targets = Vec::new();
    let mut skipped = 0_u32;
    let mut seen = HashSet::new();

    for device in devices {
        let node_id = device.node_id.trim().to_owned();
        let rack_id = device.rack_id.trim().to_owned();
        let node_type = device.r#type;

        tracing::info!(
            node = %node_id,
            rack = %rack_id,
            node_type,
            domain = request_domain.unwrap_or(""),
            "processing SDN factory-default reset target"
        );

        if node_id.is_empty() {
            tracing::warn!(
                rack = %rack_id,
                node_type,
                "SDN factory-default reset target rejected: device node_id is required"
            );

            batch.node_results.push(rm::NodeOperationResult {
                node_id: device.node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message: "device node_id is required".into(),
            });

            skipped += 1;
            continue;
        }

        if rack_id.is_empty() {
            tracing::warn!(
                node = %node_id,
                node_type,
                "SDN factory-default reset target rejected: device rack_id is required"
            );

            batch.node_results.push(rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message: "device rack_id is required".into(),
            });

            skipped += 1;
            continue;
        }

        if !seen.insert((rack_id.clone(), node_id.clone())) {
            let error_message = format!("duplicate target rack_id/node_id: {rack_id}/{node_id}");

            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                "SDN factory-default reset target rejected: duplicate target"
            );

            batch.node_results.push(rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message,
            });

            skipped += 1;
            continue;
        }

        // Build the switch after validation so malformed targets do not touch
        // backend discovery or NVUE client setup.
        let ephemeral = match super::build_ephemeral_switch(&device) {
            Ok(ephemeral) => ephemeral,
            Err(e) => {
                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    error = %e.message,
                    "failed to build switch for SDN factory-default reset"
                );

                batch.node_results.push(rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message: e.message,
                });

                skipped += 1;
                continue;
            }
        };

        let nvue_client = ephemeral.switch.nvue_client().cloned();
        let tls_server_name = ephemeral.tls_server_name;

        // Carry the NVL domain into the job task. The task initializes the
        // client later, after the RPC has returned the parent job ID.
        targets.push(SdnResetTarget {
            switch: ephemeral.switch,
            nvue_client,
            request_domain: request_domain.map(str::to_owned),
            tls_server_name,
            rack_id,
            node_id,
        });
    }

    (targets, skipped)
}

/// Reserve one child job per valid target.
///
/// `create_pending_job_if_node_idle` provides the per-node exclusion policy:
/// a destructive reset is rejected while another tracked operation owns the
/// same rack/node.
fn create_sdn_reset_jobs(
    job_tracker: &JobTracker,
    parent_job_id: &str,
    targets: Vec<SdnResetTarget>,
    batch: &mut rm::NodeBatchResponse,
    mut skipped: u32,
) -> CreatedSdnResetJobs {
    let mut pending_resets = Vec::new();

    for target in targets {
        let pending = match job_tracker.create_child_job_if_node_idle(
            parent_job_id,
            &target.rack_id,
            &target.node_id,
            JobType::SwitchSdnFactoryDefaultReset,
        ) {
            Ok(pending) => pending,
            Err(failure) => {
                tracing::warn!(
                    node = %target.node_id,
                    rack = %target.rack_id,
                    error = %failure.message,
                    "SDN factory-default reset job rejected"
                );

                batch.node_results.push(rm::NodeOperationResult {
                    node_id: target.node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message: failure.message,
                });

                skipped += 1;
                continue;
            }
        };

        pending_resets.push(PendingSdnReset { pending, target });
    }

    CreatedSdnResetJobs {
        pending_resets,
        skipped,
    }
}

impl RackManagerServiceImpl {
    /// Spawn all reserved child jobs.
    ///
    /// Parent aggregation is handled by `JobTracker`; each child reports its
    /// own terminal state and the parent reflects aggregate progress on read.
    fn dispatch_sdn_reset_jobs(&self, pending_resets: Vec<PendingSdnReset>) {
        // Ownership moves into the tracked task. JobTracker records terminal
        // fallback state if the task exits before explicitly completing/failing.
        for reset in pending_resets {
            let service = self.clone();
            let pending = reset.pending;
            let target = reset.target;

            tracing::info!(
                job_id = %pending.id(),
                node = %target.node_id,
                rack = %target.rack_id,
                domain = target.request_domain.as_deref().unwrap_or(""),
                "dispatching SDN factory-default reset job"
            );

            self.job_tracker
                .spawn_job(pending, move |job| async move {
                    run_switch_sdn_factory_default_reset_job(job, service, target).await;
                })
                .detach();
        }
    }

    /// Submit destructive SDN factory-default reset jobs for switch targets.
    ///
    /// The RPC is asynchronous from the RMS API perspective: successful initial
    /// validation returns a parent job ID, and callers should poll `GetJobStatus`
    /// for completion. Per-node validation failures are returned in the batch
    /// response without creating child jobs for those targets.
    ///
    /// Note: post hard reset, operator has to trigger the nmxconfigure rack state machine substate.
    pub(crate) async fn handle_batch_reset_switch_sdn_factory_default(
        &self,
        req: tonic::Request<rm::BatchResetSwitchSdnFactoryDefaultRequest>,
    ) -> BatchResetResponse {
        let r = req.into_inner();
        let request_domain_owned = r.domain;
        let request_domain = request_domain_owned.as_deref();
        let devices = r.nodes.map(|nodes| nodes.nodes).unwrap_or_default();
        let total_nodes = devices.len() as u32;

        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            ..Default::default()
        };

        tracing::info!(
            total_nodes,
            domain = request_domain.unwrap_or(""),
            "batch SDN factory-default reset request received"
        );

        if devices.is_empty() {
            tracing::warn!(
                domain = request_domain.unwrap_or(""),
                "batch SDN factory-default reset rejected: nodes is empty"
            );

            batch.message = "nodes is required and must contain at least one device".into();

            return finish_batch(batch, total_nodes, 0, 0);
        }

        // Request validation is intentionally synchronous so callers get
        // deterministic feedback for malformed targets and duplicates.
        let (targets, skipped) = prepare_sdn_reset_targets(devices, request_domain, &mut batch);
        if targets.is_empty() {
            batch.message = "no switch SDN factory-default reset jobs created".into();
            return finish_batch(batch, total_nodes, 0, skipped);
        }

        let parent_rack_id = targets[0].rack_id.as_str();
        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(parent_rack_id, JobType::SwitchSdnFactoryDefaultReset)
        else {
            batch.message = "parent SDN factory-default reset job was not created".into();
            return finish_batch(batch, total_nodes, 0, total_nodes);
        };
        batch.job_id = parent_id.clone();

        let CreatedSdnResetJobs {
            pending_resets,
            skipped,
        } = create_sdn_reset_jobs(
            self.job_tracker.as_ref(),
            &parent_id,
            targets,
            &mut batch,
            skipped,
        );

        let jobs_created = pending_resets.len() as u32;

        if jobs_created == 0 {
            tracing::warn!(
                total_nodes,
                skipped,
                domain = request_domain.unwrap_or(""),
                "batch SDN factory-default reset created no jobs"
            );

            batch.message = "no switch SDN factory-default reset jobs created".into();
            self.job_tracker
                .mark_failed_message(&parent_id, &batch.message);

            return finish_batch(batch, total_nodes, 0, skipped);
        }

        self.dispatch_sdn_reset_jobs(pending_resets);

        batch.status = if skipped == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();

        batch.message = format!(
            "Created {jobs_created} switch SDN factory-default reset jobs out of {total_nodes} nodes. \
             Use GetJobStatus with response.job_id to track progress."
        );

        tracing::info!(
            parent_job_id = %batch.job_id,
            total_nodes,
            jobs_created,
            skipped,
            domain = request_domain.unwrap_or(""),
            "batch SDN factory-default reset jobs created"
        );

        finish_batch(batch, total_nodes, jobs_created, skipped)
    }
}
