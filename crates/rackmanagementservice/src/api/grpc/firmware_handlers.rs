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
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::api::grpc::conversions::{
    failed_node_results, flatten_node_info, timestamp_from_datetime,
};
use crate::api::grpc::firmware_task_util::terminal_firmware_task_error;
use crate::api::grpc::node_type_resolver::{
    resolve_firmware_target_selectors, resolve_node_info, resolve_node_type,
};
use crate::config::FirmwareTaskPollOptions;
use crate::config::hardware_profiles::{
    ActivationStrategy, FirmwareTargetPlan, HardwareProfileCatalog, HardwareUpdatePlan,
    VerificationPolicy,
};
use crate::domain::node::{
    ExpectedInventoryPolicy, FirmwareActivationMode, FirmwareActivationRequest, FirmwareInfo,
    FirmwareTarget, FirmwareTaskStatus, FirmwareType, FirmwareUpdateOptions, FirmwareUpdateOutcome,
    FirmwareUpdateSummary, FirmwareVersionCheckSummary, Node, NodeKind, NodeType, Recoverable,
    RecoveryBundles, RecoveryProfile,
};
use crate::domain::rack::NodeConfig;
use crate::nodes::NodeInstance;
use crate::nodes::compute_gb200_nvidia::flint_family_for_target;
use crate::nodes::compute_gb300_supermicro::{
    SUPERMICRO_BIOS_TARGET, SUPERMICRO_BMC_TARGET, SUPERMICRO_HGX_TARGET, supermicro_bmc_target,
};
use crate::nodes::nvfwupd_adapter::{effective_force_update, is_identical_image_rejection};
use crate::orchestrator::job_lifecycle::{JobError, JobFailure, JobId};
use crate::orchestrator::job_tracker::{JobTracker, JobType, RmsJobHandle, job_error_of};
use crate::persistence::FirmwareObjectStore;
use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::insert_json_field;
use librms::protos::rack_manager as rm;

use super::server::{RackManagerServiceImpl, find_node, find_rack};

mod profile_trace;

const POST_ACTIVATION_VERSION_CHECK_ATTEMPTS: usize = 5;
/// Probes of the management API after activation before the readiness poll,
/// the budget the GB200 power cycle gives NVUE to come back (45 x 15 s).
const POST_ACTIVATION_REACHABILITY_ATTEMPTS: usize = 45;
/// Maximum number of stateless firmware inventory reads performed concurrently.
const FIRMWARE_INVENTORY_FETCH_CONCURRENCY: usize = 16;
/// Upper bound on the pause before re-reading a task that already reported a
/// terminal failure, to catch a late-arriving diagnostic message. Bounded
/// independently of the poll interval so that tuning poll verbosity does not
/// slow failure reporting.
const TERMINAL_TASK_REFETCH_GRACE: Duration = Duration::from_secs(5);

pub(crate) type FirmwareTargetExpectedVersions = HashMap<NodeType, HashMap<String, String>>;

#[cfg(not(test))]
mod timing {
    use std::time::Duration;
    pub(super) const POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY: Duration =
        Duration::from_secs(60);
    pub(super) const POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL: Duration =
        Duration::from_secs(60);
    pub(super) const POST_ACTIVATION_REACHABILITY_INTERVAL: Duration = Duration::from_secs(15);
}

#[cfg(test)]
mod timing {
    use std::time::Duration;
    pub(super) const POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY: Duration = Duration::ZERO;
    pub(super) const POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL: Duration = Duration::ZERO;
    pub(super) const POST_ACTIVATION_REACHABILITY_INTERVAL: Duration = Duration::ZERO;
}

use crate::api::grpc::firmware_object_handlers::{
    firmware_versions_match, resolve_recovery_bundles_from_latest_object,
};
use crate::api::grpc::node_recovery::{attempt_bmc_aux_powercycle_recovery, is_unreachable_err};

use timing::{
    POST_ACTIVATION_REACHABILITY_INTERVAL, POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY,
    POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL,
};

/// Sleeps for `duration` unless `cancel` fires first. Returns `true` if the
/// sleep elapsed naturally and `false` if cancellation interrupted it, so
/// long-running firmware polls can bail out promptly (parity with the
/// switch-image job path, which observes the same cooperative token).
async fn sleep_unless_cancelled(duration: Duration, cancel: &CancellationToken) -> bool {
    if duration.is_zero() {
        return !cancel.is_cancelled();
    }
    tokio::select! {
        _ = tokio::time::sleep(duration) => true,
        _ = cancel.cancelled() => false,
    }
}

/// Error surfaced when a cooperative cancellation aborts an in-flight firmware
/// poll. Uses a `Cancelled` code so operators can distinguish a deliberate
/// cancellation (e.g. graceful shutdown) from a genuine device/poll failure.
fn firmware_poll_cancelled_error(task_id: &str) -> RmsError {
    RmsError::cancelled(format!(
        "firmware task {task_id} polling cancelled before completion"
    ))
}

/// Error surfaced when a cooperative cancellation aborts the post-activation
/// firmware version-check retry loop. `Cancelled`-coded for the same reason as
/// `firmware_poll_cancelled_error`.
fn post_activation_version_check_cancelled_error() -> RmsError {
    RmsError::cancelled("post-activation firmware version check cancelled before completion")
}

/// Distinguishes a completed device task from a failed task eligible for a skip.
#[derive(Debug)]
enum PolledFirmwareTask {
    Completed(FirmwareTaskStatus),
    IdenticalImageRejected(FirmwareTaskStatus),
}

// Poll `node.poll_firmware_task(task_id)` until completion, returning poll
// errors, timeouts, and failed terminal task states as RMS errors.
#[cfg(test)]
async fn poll_firmware_task_until_complete<N: Node>(
    node: &N,
    task_id: &str,
    options: FirmwareTaskPollOptions,
) -> Result<PolledFirmwareTask> {
    poll_firmware_task_until_complete_with_retry_timeout(
        node,
        task_id,
        options,
        false,
        &CancellationToken::new(),
    )
    .await
}

async fn poll_firmware_task_until_complete_with_retry_timeout<N: Node>(
    node: &N,
    task_id: &str,
    options: FirmwareTaskPollOptions,
    allow_identical_image_skip: bool,
    cancel: &CancellationToken,
) -> Result<PolledFirmwareTask> {
    let FirmwareTaskPollOptions {
        timeout,
        interval,
        transient_error_timeout,
    } = options;

    let deadline = tokio::time::Instant::now() + timeout;
    let mut transient_poll_error_started_at = None;
    loop {
        if cancel.is_cancelled() {
            return Err(firmware_poll_cancelled_error(task_id));
        }
        tracing::info!(node = %node.id(), interval_secs = interval.as_secs(), timeout_secs = timeout.as_secs(), task_id, "polling firmware task");
        match node.poll_firmware_task(task_id).await {
            Ok(st) if st.completed => {
                if let Some(error) = terminal_firmware_task_error("firmware task", task_id, &st) {
                    let initial_identical_image = is_identical_image_rejection(&st);

                    let (status, error) = refetch_terminal_firmware_task_failure(
                        node, task_id, st, error, interval, cancel,
                    )
                    .await?;

                    if allow_identical_image_skip
                        && initial_identical_image
                        && status.completed
                        && is_identical_image_rejection(&status)
                    {
                        if cancel.is_cancelled() {
                            return Err(firmware_poll_cancelled_error(task_id));
                        }

                        return Ok(PolledFirmwareTask::IdenticalImageRejected(status));
                    }

                    return Err(error);
                }

                return Ok(PolledFirmwareTask::Completed(st));
            }
            Ok(_) => {
                transient_poll_error_started_at = None;
                let now = tokio::time::Instant::now();

                if now >= deadline {
                    tracing::error!(node = %node.id(), timeout_secs = timeout.as_secs(), task_id, "polling firmware task exceeded timeout");
                    return Err(RmsError::internal(format!(
                        "polling firmware task {task_id} exceeded timeout of {}s",
                        timeout.as_secs()
                    )));
                }
                let sleep_for = std::cmp::min(interval, deadline.saturating_duration_since(now));
                if !sleep_unless_cancelled(sleep_for, cancel).await {
                    return Err(firmware_poll_cancelled_error(task_id));
                }
            }
            Err(e) if is_retryable_firmware_poll_error(&e) => {
                let now = tokio::time::Instant::now();
                let retry_started_at = *transient_poll_error_started_at.get_or_insert(now);
                let retry_deadline = retry_started_at + transient_error_timeout;
                let error_deadline = std::cmp::min(deadline, retry_deadline);

                if now >= error_deadline {
                    return Err(e);
                }

                tracing::warn!(
                    task_id,
                    error = %e.message,
                    "transient firmware task status poll failed; retrying"
                );
                let sleep_for =
                    std::cmp::min(interval, error_deadline.saturating_duration_since(now));
                if !sleep_for.is_zero() && !sleep_unless_cancelled(sleep_for, cancel).await {
                    return Err(firmware_poll_cancelled_error(task_id));
                }
            }
            Err(e) => return Err(e),
        }
    }
}

fn is_retryable_firmware_poll_error(error: &RmsError) -> bool {
    let message = error.message.to_ascii_lowercase();
    message.contains("nvfwupd task failed for")
        && message.contains("failed to retrieve task status")
}

/// Re-reads a failed terminal task after a short grace to catch late Redfish messages.
/// Cancellation returns the initial result so the caller can preserve a failure or reject a skip.
async fn refetch_terminal_firmware_task_failure<N: Node>(
    node: &N,
    task_id: &str,
    initial_status: FirmwareTaskStatus,
    initial_error: RmsError,
    interval: Duration,
    cancel: &CancellationToken,
) -> Result<(FirmwareTaskStatus, RmsError)> {
    let grace = std::cmp::min(interval, TERMINAL_TASK_REFETCH_GRACE);

    if !sleep_unless_cancelled(grace, cancel).await {
        return Ok((initial_status, initial_error));
    }

    let refetched = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok((initial_status, initial_error)),
        result = node.poll_firmware_task(task_id) => result,
    };

    let Ok(refetched_status) = refetched else {
        // An unreadable second response cannot confirm an identical-image-only rejection.
        return Err(initial_error);
    };

    let Some(refetched_error) =
        terminal_firmware_task_error("firmware task", task_id, &refetched_status)
    else {
        return Ok((refetched_status, initial_error));
    };

    let newly_discovered_failure = is_identical_image_rejection(&initial_status)
        && !is_identical_image_rejection(&refetched_status);

    let error = if refetched_status.message.len() > initial_status.message.len()
        || newly_discovered_failure
    {
        refetched_error
    } else {
        initial_error
    };

    Ok((refetched_status, error))
}

#[derive(Debug, Clone, PartialEq)]
enum FirmwareTargetDisposition {
    Applied {
        summary: Option<FirmwareUpdateSummary>,
    },
    Skipped {
        reason: String,
    },
}

#[derive(Debug)]
enum FirmwareTargetUpdateError {
    Update(RmsError),
    Poll(RmsError),
}

struct FirmwareTaskProgress<'a> {
    tracker: &'a JobTracker,
    job_id: &'a str,
    target: &'a FirmwareTarget,
}

/// Builds the per-node firmware update options, wiring the cooperative
/// cancellation token into the request handed to nvfwupd.
///
/// How the callee uses it: nvfwupd consumes this token only as a one-shot
/// pre-flight check at the very top of its update workflow
/// (`update_firmware_with_context`). If the token is already cancelled it bails
/// before submitting anything; it is NOT threaded into the work that follows.
/// Consequently, once the firmware update has actually started (the
/// background-copy settle wait, the package upload, and the task submission),
/// the cancellation token is no longer checked by nvfwupd.
///
/// When a cancellation could realistically be observed at that pre-flight gate:
/// in the common path almost never, because the firmware job already checks the
/// same token at the workflow boundary immediately before this call, so the two
/// checks are effectively adjacent (only synchronous setup runs between them).
/// The one window that matters is contention on the node's `op_lock` (acquired
/// inside `update_firmware`): if another operation holds that lock, the token
/// can flip while we wait to acquire it, and the pre-flight check then bails as
/// soon as the lock is taken instead of proceeding into the upload.
///
/// Cancellation *during* a started update is handled elsewhere: once the task
/// reaches `Started`, the RMS poll loop
/// (`poll_firmware_task_until_complete_with_retry_timeout`) observes the token
/// and stops polling.
fn firmware_update_options_for_node<N: Node>(
    node: &N,
    cancel: &CancellationToken,
) -> FirmwareUpdateOptions {
    let mut options = FirmwareUpdateOptions::default();
    if node.node_type().kind() == NodeKind::Powershelf {
        options.apply_time = Some("OnReset".to_owned());
    }
    // Passed through to nvfwupd's pre-flight cancellation check only; see the
    // doc comment above for exactly when this is (and is not) consulted.
    options.cancellation = Some(cancel.clone());
    options
}

pub(crate) fn validate_firmware_request(
    node_type: NodeType,
    targets: &[FirmwareTarget],
    activate: bool,
) -> Result<()> {
    validate_firmware_request_with_policy(node_type, targets, activate, false)
}

pub(crate) fn validate_firmware_object_request(
    node_type: NodeType,
    targets: &[FirmwareTarget],
    activate: bool,
) -> Result<()> {
    validate_firmware_request_with_policy(node_type, targets, activate, true)
}

fn validate_firmware_request_with_policy(
    node_type: NodeType,
    targets: &[FirmwareTarget],
    activate: bool,
    allow_mixed_inband_oob: bool,
) -> Result<()> {
    if node_type == NodeType::ComputeGb200Dell {
        crate::nodes::compute_gb200_dell::validate_firmware_targets(targets)?;
    }
    let linkx: Vec<_> = targets.iter().filter(|t| is_linkx_target(t)).collect();
    if !linkx.is_empty() {
        if node_type != NodeType::ComputeVrnvl72Nvidia {
            return Err(RmsError::invalid_argument(
                "LINKX is supported only on NVIDIA VRNVL72 compute trays",
            ));
        }
        if linkx.len() != 1 {
            return Err(RmsError::invalid_argument(
                "select exactly one LINKX image for the four physical connectors",
            ));
        }
        if !activate {
            return Err(RmsError::failed_precondition(
                "LINKX requires activate=true because the flash command activates the module immediately",
            ));
        }
        if linkx[0].linkx_policy.is_none() {
            return Err(RmsError::failed_precondition(
                "LINKX requires ApplyFirmwareObject with a LinkxRollout policy",
            ));
        }
        if targets.len() != 1 && !allow_mixed_inband_oob {
            return Err(RmsError::invalid_argument(
                "out-of-band and LINKX targets cannot be mixed in a direct request",
            ));
        }
    }
    let inband_count = targets
        .iter()
        .filter(|target| flint_family_for_target(target).is_some())
        .count();
    if targets
        .iter()
        .any(|target| target.component.trim().eq_ignore_ascii_case("BF3_BFB"))
    {
        return Err(RmsError::failed_precondition(
            "BF3_BFB installation is not supported by the Flint in-band workflow; provide a Flint-compatible BF3_NIC image",
        ));
    }
    if inband_count == 0 {
        return Ok(());
    }
    if !node_type.supports_flint_inband_firmware() {
        return Err(RmsError::invalid_argument(format!(
            "CX7, CX8, and BF3_NIC in-band targets are not supported for {node_type}"
        )));
    }
    if inband_count != targets.len() && !allow_mixed_inband_oob {
        return Err(RmsError::invalid_argument(
            "out-of-band and in-band firmware targets cannot be mixed in one request",
        ));
    }
    if !activate {
        return Err(RmsError::failed_precondition(
            "GB200 in-band CX7, CX8, and BF3_NIC updates require activation for the full compute power cycle and version verification",
        ));
    }
    Ok(())
}

fn firmware_update_groups(
    node_type: NodeType,
    targets: &[FirmwareTarget],
) -> Vec<Vec<FirmwareTarget>> {
    if node_type.supports_flint_inband_firmware()
        && targets
            .iter()
            .all(|target| flint_family_for_target(target).is_some())
    {
        vec![targets.to_vec()]
    } else {
        targets.iter().cloned().map(|target| vec![target]).collect()
    }
}

fn is_linkx_target(target: &FirmwareTarget) -> bool {
    target.component.trim().eq_ignore_ascii_case("LINKX")
}

async fn update_firmware_group_with_bmc_recovery<N: Node>(
    node: &N,
    targets: &[FirmwareTarget],
    force_update: bool,
    poll_options: FirmwareTaskPollOptions,
    progress: Option<FirmwareTaskProgress<'_>>,
    cancel: &CancellationToken,
) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
    let inband = node.node_type().supports_flint_inband_firmware()
        && targets
            .iter()
            .all(|target| flint_family_for_target(target).is_some());
    if !inband {
        let [target] = targets else {
            return Err(FirmwareTargetUpdateError::Update(
                RmsError::invalid_argument(
                    "out-of-band firmware update groups must contain exactly one target",
                ),
            ));
        };
        return update_firmware_target_with_bmc_recovery(
            node,
            target,
            force_update,
            poll_options,
            progress,
            cancel,
        )
        .await;
    }

    let Some(progress_target) = targets.first() else {
        return Err(FirmwareTargetUpdateError::Update(
            RmsError::invalid_argument("in-band firmware update group is empty"),
        ));
    };
    if let Some(progress) = progress.as_ref() {
        progress.tracker.mark_running(
            progress.job_id,
            &format!(
                "Running host Flint workflow for {} firmware image(s)",
                targets.len()
            ),
        );
    }
    let outcome = node
        .update_firmware_group(
            targets,
            force_update,
            firmware_update_options_for_node(node, cancel),
        )
        .await
        .map_err(FirmwareTargetUpdateError::Update)?;
    firmware_update_outcome_until_done(
        node,
        progress_target,
        outcome,
        force_update,
        poll_options,
        progress,
        cancel,
    )
    .await
}

/// Wraps `update_firmware_target_until_done_with_progress` with a single
/// BMC-aux-powercycle retry for nodes when the host (i.e. NVOS) is unreachable.
/// Relies on concrete node implementations for firmware update and fallback powercycling.
async fn update_firmware_target_with_bmc_recovery<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    force_update: bool,
    poll_options: FirmwareTaskPollOptions,
    progress: Option<FirmwareTaskProgress<'_>>,
    cancel: &CancellationToken,
) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
    match update_firmware_target_until_done_with_progress(
        node,
        target,
        force_update,
        poll_options,
        progress,
        cancel,
    )
    .await
    {
        Err(FirmwareTargetUpdateError::Update(ref e))
            if is_unreachable_err(e) && node.supports_bmc_aux_powercycle() =>
        {
            tracing::warn!(
                node = %node.id(),
                error = %e.message,
                "firmware update failed with host-unreachable error; will attempt BMC aux powercycle recovery"
            );
            match attempt_bmc_aux_powercycle_recovery(node).await {
                Ok(()) => {
                    tracing::info!(
                        node = %node.id(),
                        "BMC aux powercycle recovery complete; retrying firmware update"
                    );
                    update_firmware_target_until_done_with_progress(
                        node,
                        target,
                        force_update,
                        poll_options,
                        None,
                        cancel,
                    )
                    .await
                    .map_err(|e| match e {
                        FirmwareTargetUpdateError::Update(inner) => {
                            FirmwareTargetUpdateError::Update(RmsError {
                                message: format!(
                                    "{} (after BMC aux powercycle recovery)",
                                    inner.message
                                ),
                                ..inner
                            })
                        }
                        FirmwareTargetUpdateError::Poll(inner) => {
                            FirmwareTargetUpdateError::Poll(RmsError {
                                message: format!(
                                    "{} (after BMC aux powercycle recovery)",
                                    inner.message
                                ),
                                ..inner
                            })
                        }
                    })
                }
                Err(recovery_err) => Err(FirmwareTargetUpdateError::Update(RmsError {
                    message: format!(
                        "BMC aux powercycle recovery failed: {}",
                        recovery_err.message
                    ),
                    ..recovery_err
                })),
            }
        }
        other => other,
    }
}

#[cfg(test)]
async fn update_firmware_target_until_done<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    force_update: bool,
    poll_options: FirmwareTaskPollOptions,
) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
    update_firmware_target_until_done_with_progress(
        node,
        target,
        force_update,
        poll_options,
        None,
        &CancellationToken::new(),
    )
    .await
}

/// Wraps `node.update_firmware` with progress tracking and firmware task polling.
/// Relies on concrete node implementation for firmware update.
async fn update_firmware_target_until_done_with_progress<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    force_update: bool,
    poll_options: FirmwareTaskPollOptions,
    progress: Option<FirmwareTaskProgress<'_>>,
    cancel: &CancellationToken,
) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
    if let Some(progress) = progress.as_ref() {
        progress.tracker.mark_running(
            progress.job_id,
            &format!(
                "Running firmware workflow for target {}",
                firmware_target_label(progress.target)
            ),
        );
    }

    let outcome = match node
        .update_firmware(
            target,
            force_update,
            firmware_update_options_for_node(node, cancel),
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(e) if e.code == ErrorCode::AlreadyExists => {
            return Ok(FirmwareTargetDisposition::Skipped { reason: e.message });
        }
        Err(e) => return Err(FirmwareTargetUpdateError::Update(e)),
    };

    firmware_update_outcome_until_done(
        node,
        target,
        outcome,
        force_update,
        poll_options,
        progress,
        cancel,
    )
    .await
}

async fn firmware_update_outcome_until_done<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    outcome: FirmwareUpdateOutcome,
    force_update: bool,
    poll_options: FirmwareTaskPollOptions,
    progress: Option<FirmwareTaskProgress<'_>>,
    cancel: &CancellationToken,
) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
    match outcome {
        FirmwareUpdateOutcome::Started(handle) => {
            if let Some(progress) = progress.as_ref() {
                progress.tracker.mark_running(
                    progress.job_id,
                    &format!(
                        "Polling firmware task {} for target {}",
                        handle.task_id,
                        firmware_target_label(target)
                    ),
                );
            }

            let recover_supermicro_bmc = supermicro_bmc_task_loss_recovery_allowed(node, target);
            let recover_liteon_vr_pmc = liteon_vr_pmc_task_loss_recovery_allowed(node, target);
            // Recovery may not outlive the poll loop's own task deadline.
            let task_deadline = tokio::time::Instant::now() + poll_options.timeout;

            let effective_force = effective_force_update(
                target,
                force_update,
                &firmware_update_options_for_node(node, cancel),
            );

            let poll_result = poll_firmware_task_until_complete_with_retry_timeout(
                node,
                &handle.task_id,
                poll_options,
                !effective_force,
                cancel,
            )
            .await;
            // Cleanup is deliberately outside the cancellation token. A device
            // task may outlive RMS polling; the adapter decides whether its
            // temporary settings can be restored and reports failures here.
            let poll_result = finish_firmware_task(node, &handle.task_id, poll_result).await;

            let poll_error = match poll_result {
                Ok(PolledFirmwareTask::Completed(status)) => {
                    debug_assert!(status.completed);
                    return Ok(FirmwareTargetDisposition::Applied { summary: None });
                }
                Ok(PolledFirmwareTask::IdenticalImageRejected(status)) => {
                    if cancel.is_cancelled() {
                        return Err(FirmwareTargetUpdateError::Poll(
                            firmware_poll_cancelled_error(&handle.task_id),
                        ));
                    }

                    return Ok(FirmwareTargetDisposition::Skipped {
                        reason: status.message,
                    });
                }
                Err(error) => error,
            };

            // On this platform the exhausted transient-error window is the
            // trigger for version verification rather than a failure, so
            // `transient_error_timeout` doubles as the grace period before
            // RMS concludes the task is gone. Setting it far below the
            // flash duration verifies against pre-update firmware and
            // reports a version mismatch instead of retrying.
            if recover_supermicro_bmc && is_retryable_firmware_poll_error(&poll_error) {
                if let Some(progress) = progress.as_ref() {
                    progress.tracker.mark_running(
                        progress.job_id,
                        &format!(
                            "Firmware task {} is unavailable; verifying installed Supermicro BMC version {}",
                            handle.task_id,
                            target.expected_version.as_deref().unwrap_or("unknown")
                        ),
                    );
                }

                let summary = recover_supermicro_bmc_update_after_task_loss(
                    node,
                    target,
                    &handle.task_id,
                    &poll_error,
                    cancel,
                )
                .await
                .map_err(FirmwareTargetUpdateError::Poll)?;

                return Ok(FirmwareTargetDisposition::Applied {
                    summary: Some(summary),
                });
            }

            if recover_liteon_vr_pmc && is_retryable_firmware_poll_error(&poll_error) {
                if let Some(progress) = progress.as_ref() {
                    progress.tracker.mark_running(
                        progress.job_id,
                        &format!(
                            "Firmware task {} is unavailable; verifying installed LiteOn PMC version {}",
                            handle.task_id,
                            target.expected_version.as_deref().unwrap_or("unknown")
                        ),
                    );
                }

                let summary = recover_liteon_vr_pmc_update_after_task_loss(
                    node,
                    target,
                    &handle.task_id,
                    &poll_error,
                    poll_options,
                    task_deadline,
                    cancel,
                )
                .await
                .map_err(FirmwareTargetUpdateError::Poll)?;

                return Ok(FirmwareTargetDisposition::Applied {
                    summary: Some(summary),
                });
            }

            Err(FirmwareTargetUpdateError::Poll(poll_error))
        }
        FirmwareUpdateOutcome::Completed(summary) => Ok(FirmwareTargetDisposition::Applied {
            summary: Some(summary),
        }),
        FirmwareUpdateOutcome::Skipped { reason } => {
            Ok(FirmwareTargetDisposition::Skipped { reason })
        }
    }
}

async fn finish_firmware_task<N: Node, T>(node: &N, task_id: &str, result: Result<T>) -> Result<T> {
    let Some(cleanup) = node.firmware_task_cleanup() else {
        return result;
    };
    match (result, cleanup.finish(task_id).await) {
        (result, Ok(())) => result,
        // The firmware is already applied; failing here would skip activation.
        // The adapter keeps the pending setting and retries before its next upload.
        (Ok(status), Err(cleanup_error)) => {
            tracing::warn!(
                node = node.id(),
                task_id,
                error = %cleanup_error.message,
                "firmware task succeeded but cleanup failed; continuing"
            );
            Ok(status)
        }
        (Err(error), Err(cleanup_error)) => Err(RmsError::new(
            error.code,
            format!(
                "{}; firmware task cleanup failed: {}",
                error.message, cleanup_error.message
            ),
        )),
    }
}

/// Enables the Supermicro GB300 BMC task-loss special case.
///
/// Some Supermicro BMC updates activate immediately and remove their Redfish
/// task while RMS is polling it. Keep this exception limited to BMC targets
/// whose expected version came from the firmware manifest.
fn supermicro_bmc_task_loss_recovery_allowed<N: Node>(node: &N, target: &FirmwareTarget) -> bool {
    node.node_type() == NodeType::ComputeGb300Supermicro
        && supermicro_bmc_target(target)
        && target
            .expected_version
            .as_deref()
            .is_some_and(|version| !version.trim().is_empty())
}

/// Enables VR NVL72 LiteOn PMC task-loss recovery.
///
/// The PMC restarts to activate its image, rejects authenticated requests for
/// many minutes, and comes back without the Redfish task (HTTP 404). Only the
/// PMC target carries a manifest `expected_version` for this node type, so that
/// version is both the selector and the independent post-condition.
fn liteon_vr_pmc_task_loss_recovery_allowed<N: Node>(node: &N, target: &FirmwareTarget) -> bool {
    node.node_type() == NodeType::PowershelfVrnvl72Liteon
        && target
            .expected_version
            .as_deref()
            .is_some_and(|version| !version.trim().is_empty())
}

/// Completes a VR LiteOn PMC update whose task vanished across the PMC restart.
///
/// This is not general task-failure recovery: the update succeeds only when the
/// PMC firmware inventory reports the manifest version. The PMC may still be
/// restarting, so the inventory is polled until `transient_error_timeout`
/// elapses or the poll loop's `task_deadline` passes, whichever is first, then
/// the last completed observation is reported as a failure. No read starts at
/// or after the deadline. A read that reaches the deadline reports a read
/// timeout, including a matching response received at that boundary. Reads are
/// abandoned on cancellation, which takes precedence over accepting a match.
async fn recover_liteon_vr_pmc_update_after_task_loss<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    task_id: &str,
    poll_error: &RmsError,
    poll_options: FirmwareTaskPollOptions,
    task_deadline: tokio::time::Instant,
    cancel: &CancellationToken,
) -> Result<FirmwareUpdateSummary> {
    let Some(expected_version) = target
        .expected_version
        .as_deref()
        .map(str::trim)
        .filter(|version| !version.is_empty())
    else {
        return Err(RmsError::internal(
            "LiteOn PMC task-loss recovery requires an expected firmware manifest version",
        ));
    };
    tracing::warn!(
        node = %node.id(),
        task_id,
        expected_version,
        error = %poll_error.message,
        "LiteOn PMC firmware task became unavailable; checking installed PMC version"
    );

    let deadline = std::cmp::min(
        tokio::time::Instant::now() + poll_options.transient_error_timeout,
        task_deadline,
    );

    let mut last_observation =
        "firmware inventory recovery budget expired before any read".to_owned();

    loop {
        if cancel.is_cancelled() {
            return Err(firmware_poll_cancelled_error(task_id));
        }

        // A deferred select branch must check expiry before starting a read;
        // the timeout also bounds a read held by a rebooting PMC.
        let read = tokio::select! {
            _ = cancel.cancelled() => return Err(firmware_poll_cancelled_error(task_id)),
            read = async {
                if tokio::time::Instant::now() >= deadline {
                    return None;
                }

                Some(tokio::time::timeout_at(deadline, node.get_firmware_inventory()).await)
            } => read,
        };

        if cancel.is_cancelled() {
            return Err(firmware_poll_cancelled_error(task_id));
        }

        let Some(read) = read else {
            break;
        };

        // Tokio polls the read before its timeout, so a ready result can still
        // arrive at an expired deadline.
        let read_finished_at = tokio::time::Instant::now();

        match read {
            Ok(Ok(inventory)) if read_finished_at < deadline => {
                let pmc = inventory
                    .iter()
                    .find(|item| item.name.to_ascii_uppercase().starts_with("FW_PMC"));
                match pmc {
                    Some(item) if firmware_versions_match(&item.version, expected_version) => {
                        return Ok(FirmwareUpdateSummary {
                            message: format!(
                                "LiteOn PMC task disappeared across the PMC restart; installed version matches firmware manifest version {expected_version}"
                            ),
                            task_ids: vec![task_id.to_owned()],
                            details: serde_json::json!({
                                "recovery": "liteon_vr_pmc_task_loss_firmware_manifest_version_match",
                                "task_id": task_id,
                                "expected_version": expected_version,
                                "installed_version": item.version.trim(),
                                "inventory_item": item.name,
                                "health": item.health,
                                "poll_error": &poll_error.message,
                            }),
                        });
                    }
                    Some(item) => {
                        last_observation =
                            format!("{} reports version {}", item.name, item.version.trim());
                    }
                    None => {
                        last_observation = "no FW_PMC entry in the firmware inventory".to_owned();
                    }
                }
            }
            Ok(Err(error)) if read_finished_at < deadline => {
                last_observation = format!("firmware inventory read failed: {}", error.message);
            }
            _ => {
                last_observation =
                    "firmware inventory read did not finish within the recovery budget".to_owned();

                break;
            }
        }

        let now = tokio::time::Instant::now();

        if now >= deadline {
            break;
        }

        let sleep_for = std::cmp::min(poll_options.interval, deadline - now);

        if !sleep_unless_cancelled(sleep_for, cancel).await {
            return Err(firmware_poll_cancelled_error(task_id));
        }
    }

    Err(RmsError::internal(format!(
        "LiteOn PMC task {task_id} disappeared and the installed version could not be confirmed as firmware manifest version {expected_version}: {last_observation}"
    )))
}

/// Handles the Supermicro GB300 BMC task-loss special case by verifying the
/// installed version against the firmware manifest.
///
/// This is not general task-failure recovery: the update succeeds only when
/// the post-update inventory matches the expected manifest version.
async fn recover_supermicro_bmc_update_after_task_loss<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    task_id: &str,
    poll_error: &RmsError,
    cancel: &CancellationToken,
) -> Result<FirmwareUpdateSummary> {
    let Some(expected_version) = target.expected_version.as_deref() else {
        return Err(RmsError::internal(
            "Supermicro BMC task-loss recovery requires an expected firmware manifest version",
        ));
    };
    tracing::warn!(
        node = %node.id(),
        task_id,
        expected_version,
        error = %poll_error.message,
        "Supermicro BMC firmware task became unavailable; checking installed firmware manifest version"
    );

    let version_check =
        post_activation_version_check(node, std::slice::from_ref(target), cancel).await?;
    let summary = version_check.ok_or_else(|| {
        RmsError::internal(
            "Supermicro BMC task disappeared, but installed firmware version verification was skipped",
        )
    })?;
    let verified_from_firmware_manifest = summary.matched
        && summary
            .details
            .get("status")
            .and_then(serde_json::Value::as_str)
            == Some("matched")
        && summary
            .details
            .get("verification")
            .and_then(serde_json::Value::as_str)
            == Some("supermicro_firmware_manifest_version");
    if !verified_from_firmware_manifest {
        return Err(RmsError::internal(format!(
            "Supermicro BMC task {task_id} disappeared and installed version could not be confirmed against firmware manifest version {expected_version}"
        )));
    }

    Ok(FirmwareUpdateSummary {
        message: format!(
            "Supermicro BMC task disappeared during immediate activation; installed version matches firmware manifest version {expected_version}"
        ),
        task_ids: vec![task_id.to_owned()],
        details: serde_json::json!({
            "recovery": "supermicro_bmc_task_loss_firmware_manifest_version_match",
            "task_id": task_id,
            "expected_version": expected_version,
            "poll_error": &poll_error.message,
            "version_check": summary.details,
        }),
    })
}

fn firmware_target_result_json(
    target: &FirmwareTarget,
    disposition: &FirmwareTargetDisposition,
) -> serde_json::Value {
    let mut result = serde_json::json!({
        "component": target.component.clone(),
        "firmware_file": target.firmware_file.clone(),
        "target": firmware_target_label(target),
    });
    if let Some(expected_version) = &target.expected_version {
        insert_json_field(
            &mut result,
            "expected_version",
            serde_json::json!(expected_version),
        );
    }

    match disposition {
        FirmwareTargetDisposition::Applied { summary } => {
            insert_json_field(&mut result, "status", serde_json::json!("completed"));
            if let Some(summary) = summary {
                insert_json_field(
                    &mut result,
                    "nvfwupd_summary",
                    serde_json::json!({
                        "message": summary.message.clone(),
                        "task_ids": summary.task_ids.clone(),
                        "details": summary.details.clone(),
                    }),
                );
            }
        }
        FirmwareTargetDisposition::Skipped { reason } => {
            insert_json_field(&mut result, "status", serde_json::json!("skipped"));
            insert_json_field(&mut result, "reason", serde_json::json!(reason));
        }
    }

    result
}

#[cfg(test)]
async fn activate_firmware_for_node<N: Node>(node: &N, cancel: &CancellationToken) -> Result<()> {
    activate_firmware_for_node_with_strategy(node, cancel, None).await
}

#[cfg(test)]
async fn activate_firmware_for_node_with_strategy<N: Node>(
    node: &N,
    cancel: &CancellationToken,
    strategy: Option<ActivationStrategy>,
) -> Result<()> {
    activate_firmware_with_mode(
        node,
        cancel,
        activation_mode_for_node_and_strategy(node, strategy)?,
    )
    .await
}

/// Preserve the NVUE-to-BMC recovery fallback for GB200/GB300. VR NVL72 already
/// activates via the BMC, so its failed recovery must not trigger a second reset.
async fn activate_firmware_with_mode<N: Node>(
    node: &N,
    cancel: &CancellationToken,
    mode: FirmwareActivationMode,
) -> Result<()> {
    let fallback_cancellation = cancel.clone();
    let result = node
        .activate_firmware_with(FirmwareActivationRequest {
            mode,
            cancellation: Some(cancel.clone()),
        })
        .await
        .map(|_| ());
    match result {
        Err(ref error)
            if node.node_type() != NodeType::SwitchVrnvl72Nvidia
                && is_unreachable_err(error)
                && node.supports_bmc_aux_powercycle() =>
        {
            if fallback_cancellation.is_cancelled() {
                return result;
            }
            tracing::warn!(node = %node.id(), error = %error.message,
                "firmware activation via host unreachable; falling back to BMC aux powercycle");
            attempt_bmc_aux_powercycle_recovery(node).await
        }
        other => other,
    }
}

#[cfg(test)]
fn activation_mode_for_node_and_strategy<N: Node>(
    node: &N,
    strategy: Option<ActivationStrategy>,
) -> Result<FirmwareActivationMode> {
    activation_mode_for_type_and_strategy(node.node_type(), strategy)
}

fn activation_mode_for_type_and_strategy(
    node_type: NodeType,
    strategy: Option<ActivationStrategy>,
) -> Result<FirmwareActivationMode> {
    match strategy {
        Some(ActivationStrategy::FullComputePowerCycle)
            if node_type.kind() == NodeKind::Compute =>
        {
            Ok(FirmwareActivationMode::FullGb200Compute)
        }
        Some(ActivationStrategy::FullComputePowerCycle) => {
            Err(RmsError::invalid_argument(format!(
                "activation strategy full_compute_power_cycle is not valid for {}",
                node_type.as_str()
            )))
        }
        None => activation_mode_for_type(node_type),
    }
}

fn activation_mode_for_type(node_type: NodeType) -> Result<FirmwareActivationMode> {
    match node_type {
        NodeType::ComputeGb200Nvidia
        | NodeType::ComputeGb200Wiwynn
        | NodeType::ComputeGb200Dell
        | NodeType::ComputeGb300Nvidia
        | NodeType::ComputeGb300Lenovo
        | NodeType::ComputeGb300Supermicro
        | NodeType::ComputeVrnvl72Nvidia => Ok(FirmwareActivationMode::FullGb200Compute),
        NodeType::SwitchGb200Nvidia
        | NodeType::SwitchGb300Nvidia
        | NodeType::SwitchVrnvl72Nvidia => Ok(FirmwareActivationMode::SwitchPowerCycle),
        NodeType::PowershelfGb200Liteon
        | NodeType::PowershelfGb200Delta
        | NodeType::PowershelfGb300Liteon
        | NodeType::PowershelfGb300Delta
        | NodeType::PowershelfVrnvl72Liteon
        | NodeType::PowershelfVrnvl72Delta => {
            Ok(FirmwareActivationMode::PowerShelfReset { force: false })
        }
    }
}

fn node_supports_post_activation_version_check<N: Node>(node: &N) -> bool {
    matches!(
        node.node_type(),
        NodeType::ComputeGb200Nvidia
            | NodeType::ComputeGb200Wiwynn
            | NodeType::ComputeGb200Dell
            | NodeType::ComputeGb300Nvidia
            | NodeType::ComputeGb300Lenovo
            | NodeType::ComputeGb300Supermicro
            | NodeType::ComputeVrnvl72Nvidia
            | NodeType::SwitchGb200Nvidia
            | NodeType::SwitchGb300Nvidia
            | NodeType::SwitchVrnvl72Nvidia
    )
}

/// Compare the selected switch PLDM payloads before flashing.
/// Raw images have no package version metadata and retain their existing update
/// behavior. This preflight performs no recovery or activation on read failure.
async fn pre_update_firmware_version_check<N: Node>(
    node: &N,
    targets: &[FirmwareTarget],
    force_update: bool,
    cancel: &CancellationToken,
) -> Result<Option<FirmwareVersionCheckSummary>> {
    if force_update
        || node.node_type().kind() != NodeKind::Switch
        || targets.is_empty()
        || targets.iter().any(|target| {
            !Path::new(&target.firmware_file)
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("fwpkg"))
        })
    {
        return Ok(None);
    }

    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(RmsError::cancelled(
            "pre-update firmware version check cancelled",
        )),
        result = node.verify_firmware_package_versions(targets) => result.map(Some),
    }
}

async fn post_activation_version_check<N: Node>(
    node: &N,
    targets: &[FirmwareTarget],
    cancel: &CancellationToken,
) -> Result<Option<FirmwareVersionCheckSummary>> {
    post_activation_version_check_with_policy(node, targets, cancel, None).await
}

async fn post_activation_version_check_with_policy<N: Node>(
    node: &N,
    targets: &[FirmwareTarget],
    cancel: &CancellationToken,
    verification: Option<VerificationPolicy>,
) -> Result<Option<FirmwareVersionCheckSummary>> {
    if verification.is_none() && !node_supports_post_activation_version_check(node) {
        return Ok(None);
    }

    if verification.is_some_and(|policy| !policy.enabled()) {
        return Ok(None);
    }

    if targets.iter().all(|target| target.firmware_file.is_empty()) {
        return Ok(None);
    }

    // Cancel-aware initial reboot/settle grace period: a cancellation after
    // activation (e.g. graceful shutdown) stops the verification here instead
    // of waiting out the delay.
    let settle_delay = verification.map_or(POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY, |policy| {
        policy.settle_delay()
    });
    if !sleep_unless_cancelled(settle_delay, cancel).await {
        return Err(post_activation_version_check_cancelled_error());
    }

    let summary =
        verify_firmware_package_versions_after_activation(node, targets, cancel, verification)
            .await?;
    if !summary.matched {
        return Err(RmsError::internal(format!(
            "post-activation firmware version check failed: {}",
            version_check_mismatch_summary(&summary.details)
        )));
    }

    wait_for_management_api(|| node.verify_management_api_reachable(), cancel).await?;
    wait_for_system_ready(|| node.verify_system_ready(), cancel, verification).await?;

    Ok(Some(summary))
}

/// Attempt count and retry interval of a post-activation poll: the stage's
/// policy when it has one, the built-in constants otherwise.
fn post_activation_poll_budget(verification: Option<VerificationPolicy>) -> (usize, Duration) {
    verification.map_or(
        (
            POST_ACTIVATION_VERSION_CHECK_ATTEMPTS,
            POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL,
        ),
        |policy| (policy.attempts().get() as usize, policy.retry_interval()),
    )
}

async fn verify_firmware_package_versions_after_activation<N: Node>(
    node: &N,
    targets: &[FirmwareTarget],
    cancel: &CancellationToken,
    verification: Option<VerificationPolicy>,
) -> Result<FirmwareVersionCheckSummary> {
    let (attempts, retry_interval) = post_activation_poll_budget(verification);
    let mut last_error = None;
    let mut last_mismatched_summary = None;
    for attempt in 1..=attempts {
        if cancel.is_cancelled() {
            return Err(post_activation_version_check_cancelled_error());
        }
        match node.verify_firmware_package_versions(targets).await {
            Ok(summary) if summary.matched => return Ok(summary),
            Ok(summary) => {
                let will_retry = attempt < attempts;
                tracing::warn!(
                    node = %node.id(),
                    attempt,
                    attempts,
                    will_retry,
                    summary = %version_check_mismatch_summary(&summary.details),
                    "post-activation firmware versions do not match"
                );
                last_mismatched_summary = Some(summary);
                if will_retry && !sleep_unless_cancelled(retry_interval, cancel).await {
                    return Err(post_activation_version_check_cancelled_error());
                }
            }
            Err(error) if is_retryable_post_activation_version_check_error(&error) => {
                let will_retry = attempt < attempts;
                tracing::warn!(
                    node = %node.id(),
                    attempt,
                    attempts,
                    will_retry,
                    error = %error.message,
                    "post-activation firmware version check failed"
                );
                last_error = Some(error);
                if will_retry && !sleep_unless_cancelled(retry_interval, cancel).await {
                    return Err(post_activation_version_check_cancelled_error());
                }
            }
            Err(error) => return Err(error),
        }
    }

    if let Some(summary) = last_mismatched_summary {
        return Ok(summary);
    }

    Err(last_error.unwrap_or_else(|| {
        RmsError::timeout("post-activation firmware version check retry budget exhausted")
    }))
}

fn is_retryable_post_activation_version_check_error(error: &RmsError) -> bool {
    matches!(
        error.code,
        ErrorCode::Unavailable
            | ErrorCode::Timeout
            | ErrorCode::ConnectionRefused
            | ErrorCode::DnsResolutionFailed
    )
}

/// Polls `probe` until the node's management API answers, on the budget the
/// GB200 power cycle gives NVUE to return (45 probes, 15 s apart). On a
/// switch activated through its BMC, the version check reads the BMC and can
/// pass while the OS still boots, so the readiness poll runs after this.
/// On a node whose API already answers, or has none, the first probe returns.
pub(super) async fn wait_for_management_api<Fut>(
    probe: impl Fn() -> Fut,
    cancel: &CancellationToken,
) -> Result<()>
where
    Fut: Future<Output = Result<()>>,
{
    poll_after_activation(
        probe,
        cancel,
        POST_ACTIVATION_REACHABILITY_ATTEMPTS,
        POST_ACTIVATION_REACHABILITY_INTERVAL,
        "management API reachability",
    )
    .await
}

/// Polls `probe` until the node reports ready, on the same budget as
/// `verify_firmware_package_versions_after_activation`: the attempt count and
/// interval of `verification` when the stage has a policy, the post-activation
/// constants otherwise. No initial delay, because callers reach this after
/// `wait_for_management_api`.
///
/// `probe` is one readiness call: `Ok(())` when the node reports ready, or
/// has no such signal, `FailedPrecondition` while it reports not ready, and
/// `Aborted` when it reports a state that waiting will not change. A
/// closure, because the two update jobs hold different types (a `Node`, a
/// `SwitchSystemImageNode`) and both answer the same question.
pub(super) async fn wait_for_system_ready<Fut>(
    probe: impl Fn() -> Fut,
    cancel: &CancellationToken,
    verification: Option<VerificationPolicy>,
) -> Result<()>
where
    Fut: Future<Output = Result<()>>,
{
    let (attempts, retry_interval) = post_activation_poll_budget(verification);
    poll_after_activation(probe, cancel, attempts, retry_interval, "system readiness").await
}

/// The post-activation poll loop. Retries the errors
/// `is_retryable_after_activation` accepts, `attempts` times with
/// `retry_interval` between them. Any other error ends the wait at once, and
/// so does cancellation. When the budget runs out, the
/// last error is returned. `what` names the check in the log and the
/// messages.
async fn poll_after_activation<Fut>(
    probe: impl Fn() -> Fut,
    cancel: &CancellationToken,
    attempts: usize,
    retry_interval: Duration,
    what: &str,
) -> Result<()>
where
    Fut: Future<Output = Result<()>>,
{
    let cancelled = || RmsError::cancelled(format!("{what} check cancelled before completion"));
    let mut last_error = None;

    for attempt in 1..=attempts {
        if cancel.is_cancelled() {
            return Err(cancelled());
        }

        match probe().await {
            Ok(()) => return Ok(()),
            Err(error) if is_retryable_after_activation(&error) => {
                let will_retry = attempt < attempts;
                tracing::warn!(
                    attempt,
                    attempts,
                    will_retry,
                    error = %error.message,
                    "{what} check not satisfied after activation"
                );
                last_error = Some(error);
                if will_retry && !sleep_unless_cancelled(retry_interval, cancel).await {
                    return Err(cancelled());
                }
            }
            Err(error) => return Err(error),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        RmsError::internal(format!(
            "{what} check ran out of attempts without a probe result"
        ))
    }))
}

/// While NVOS boots, NVUE can answer with any error before it answers ready:
/// a proxy 502, an auth 401, a 500, or a body this client cannot decode, such
/// as a health word a later NVOS adds. Two errors end the wait: `Aborted`, a
/// state that waiting will not change, and `InvalidArgument`, a node without
/// a management API endpoint.
fn is_retryable_after_activation(error: &RmsError) -> bool {
    !matches!(error.code, ErrorCode::Aborted | ErrorCode::InvalidArgument)
}

fn version_check_mismatch_summary(details: &serde_json::Value) -> String {
    let Some(mismatches) = details.get("mismatches").and_then(|value| value.as_array()) else {
        return "installed versions do not match package versions".to_owned();
    };

    let mut parts = Vec::new();
    for mismatch in mismatches.iter().take(5) {
        let name = mismatch
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown component");
        let expected = mismatch
            .get("package_version")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let actual = mismatch
            .get("system_version")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        parts.push(format!("{name}: expected {expected}, found {actual}"));
    }

    if mismatches.len() > parts.len() {
        parts.push(format!("and {} more", mismatches.len() - parts.len()));
    }

    if parts.is_empty() {
        "installed versions do not match package versions".to_owned()
    } else {
        parts.join("; ")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RedfishFailureDetails {
    code: Option<String>,
    message: Option<String>,
    severity: Option<String>,
    resolution: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FirmwareFailureDetails {
    summary: String,
    raw_error: String,
    target_id: Option<String>,
    redfish: Option<RedfishFailureDetails>,
}

const MAX_FIRMWARE_FAILURE_SUMMARY_CHARS: usize = 1200;

fn firmware_failure_message_and_result_json(
    phase: &str,
    target: Option<&FirmwareTarget>,
    error_code: JobError,
    message: &str,
) -> (String, String) {
    let details = firmware_failure_details(message);
    let summary = details.summary.clone();
    let mut result = serde_json::json!({
        "status": "failed",
        "phase": phase,
        "error_code": format!("{:?}", error_code),
        "error_message": details.summary.clone(),
        "summary": details.summary.clone(),
    });
    if let Some(target) = target {
        let mut target_json = serde_json::json!({
            "component": target.component.clone(),
            "firmware_file": target.firmware_file.clone(),
        });
        if let Some(expected_version) = &target.expected_version {
            insert_json_field(
                &mut target_json,
                "expected_version",
                serde_json::json!(expected_version),
            );
        }
        insert_json_field(&mut result, "target", target_json);
    }
    if let Some(target_id) = details.target_id {
        insert_json_field(&mut result, "target_id", serde_json::json!(target_id));
    }
    if let Some(redfish) = details.redfish {
        let mut redfish_json = serde_json::Map::new();
        if let Some(code) = redfish.code {
            redfish_json.insert("code".to_owned(), serde_json::json!(code));
        }
        if let Some(message) = redfish.message {
            redfish_json.insert("message".to_owned(), serde_json::json!(message));
        }
        if let Some(severity) = redfish.severity {
            redfish_json.insert("severity".to_owned(), serde_json::json!(severity));
        }
        if let Some(resolution) = redfish.resolution {
            redfish_json.insert("resolution".to_owned(), serde_json::json!(resolution));
        }
        if !redfish_json.is_empty() {
            insert_json_field(
                &mut result,
                "redfish",
                serde_json::Value::Object(redfish_json),
            );
        }
    }
    if details.raw_error != details.summary {
        insert_json_field(
            &mut result,
            "raw_error",
            serde_json::json!(details.raw_error),
        );
    }

    (summary, result.to_string())
}

fn firmware_failure_details(message: &str) -> FirmwareFailureDetails {
    let raw_error = message.trim().to_owned();
    let target_id = extract_firmware_failure_target_id(&raw_error);
    let redfish = extract_redfish_failure_details(&raw_error);
    let summary = summarize_firmware_failure(&raw_error, target_id.as_deref(), redfish.as_ref());

    FirmwareFailureDetails {
        summary,
        raw_error,
        target_id,
        redfish,
    }
}

fn summarize_firmware_failure(
    message: &str,
    target_id: Option<&str>,
    redfish: Option<&RedfishFailureDetails>,
) -> String {
    let target_is_psu = target_id
        .map(|id| id.trim_start().starts_with("powerdevice"))
        .unwrap_or(false);
    if message.contains("LiteOn PSU") || target_is_psu {
        let reason = if message.contains("LiteOn PSU update timed out") {
            "LiteOn PSU update timed out"
        } else if message.contains("Failed to acquire task ID from firmware update response") {
            "Failed to acquire task ID from firmware update response"
        } else if message.contains("LiteOn PSU serial fallback failed") {
            "LiteOn PSU serial fallback failed"
        } else {
            "LiteOn PSU update failed"
        };

        let mut summary = match target_id {
            Some(id) => format!("PSU update failed on {id}: {reason}"),
            None => format!("PSU update failed: {reason}"),
        };
        append_redfish_summary(&mut summary, redfish);
        return truncate_failure_summary(&summary);
    }

    truncate_failure_summary(&normalize_failure_text(message))
}

fn append_redfish_summary(summary: &mut String, redfish: Option<&RedfishFailureDetails>) {
    let Some(redfish) = redfish else {
        return;
    };

    match (&redfish.code, &redfish.message) {
        (Some(code), Some(message)) => {
            push_failure_sentence(summary, &format!("Redfish returned {code}: {message}"));
        }
        (Some(code), None) => {
            push_failure_sentence(summary, &format!("Redfish returned {code}"));
        }
        (None, Some(message)) => {
            push_failure_sentence(summary, &format!("Redfish returned: {message}"));
        }
        (None, None) => {}
    }

    if let Some(resolution) = &redfish.resolution {
        push_failure_sentence(summary, &format!("Resolution: {resolution}"));
    }
}

fn push_failure_sentence(summary: &mut String, sentence: &str) {
    if !summary.ends_with('.') && !summary.ends_with('!') && !summary.ends_with('?') {
        summary.push('.');
    }
    summary.push(' ');
    summary.push_str(sentence);
}

fn extract_firmware_failure_target_id(message: &str) -> Option<String> {
    extract_token_after(
        message,
        "LiteOn PSU firmware update failed for ",
        &[':', ';'],
    )
    .or_else(|| {
        extract_token_after(
            message,
            "LiteOn PSU firmware update did not finish for ",
            &[':', ';'],
        )
    })
    .or_else(|| extract_string_field(message, "task_id"))
    .or_else(|| extract_token_after(message, "NVFWUPD task failed for ", &[':', ';']))
    .or_else(|| extract_token_after(message, "Task Id: ", &['\n', '\r', ';', ',']))
    .filter(|id| !id.trim().is_empty())
}

fn extract_redfish_failure_details(message: &str) -> Option<RedfishFailureDetails> {
    let details = RedfishFailureDetails {
        code: extract_string_field(message, "MessageId")
            .or_else(|| extract_string_field(message, "code")),
        message: extract_string_field(message, "Message")
            .or_else(|| extract_string_field(message, "message")),
        severity: extract_string_field(message, "MessageSeverity"),
        resolution: extract_string_field(message, "Resolution"),
    };

    if details.code.is_some()
        || details.message.is_some()
        || details.severity.is_some()
        || details.resolution.is_some()
    {
        Some(details)
    } else {
        None
    }
}

fn extract_token_after(message: &str, marker: &str, terminators: &[char]) -> Option<String> {
    let after = message.split_once(marker)?.1.trim_start();
    let end = after
        .find(|c| terminators.contains(&c))
        .unwrap_or(after.len());
    let token = after
        .get(..end)?
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == ',' || c == ';');
    if token.is_empty() {
        None
    } else {
        Some(token.to_owned())
    }
}

fn extract_string_field(message: &str, field: &str) -> Option<String> {
    let needle = format!("\"{field}\"");
    let mut search_start = 0usize;

    while let Some(remaining) = message.get(search_start..) {
        let Some(relative_pos) = remaining.find(&needle) else {
            break;
        };
        let after_key_index = search_start + relative_pos + needle.len();
        let Some(after_key) = message.get(after_key_index..) else {
            break;
        };
        let after_key = after_key.trim_start();
        let Some(after_colon) = after_key.strip_prefix(':') else {
            search_start = after_key_index;
            continue;
        };
        let value = after_colon.trim_start();
        if let Some(value) = value.strip_prefix("String(\"") {
            return take_quoted_value(value).map(|s| normalize_failure_text(&s));
        }
        if let Some(value) = value.strip_prefix('"') {
            return take_quoted_value(value).map(|s| normalize_failure_text(&s));
        }

        search_start = after_key_index;
    }

    None
}

fn take_quoted_value(value: &str) -> Option<String> {
    let mut output = String::new();
    let mut escaped = false;

    for ch in value.chars() {
        if escaped {
            let decoded = match ch {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '"' => '"',
                '\\' => '\\',
                other => other,
            };
            output.push(decoded);
            escaped = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            '"' => return Some(output),
            _ => output.push(ch),
        }
    }

    None
}

fn normalize_failure_text(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_failure_summary(message: &str) -> String {
    let char_count = message.chars().count();
    if char_count <= MAX_FIRMWARE_FAILURE_SUMMARY_CHARS {
        return message.to_owned();
    }

    message
        .chars()
        .take(MAX_FIRMWARE_FAILURE_SUMMARY_CHARS.saturating_sub(3))
        .collect::<String>()
        + "..."
}

fn firmware_target_label(target: &FirmwareTarget) -> &str {
    if target.component.trim().is_empty() {
        "package-derived"
    } else {
        target.component.as_str()
    }
}

fn firmware_type_to_int(ft: FirmwareType) -> i32 {
    match ft {
        FirmwareType::BMC => 1,
        FirmwareType::BIOS => 2,
        FirmwareType::CPLD => 3,
        FirmwareType::FPGA => 4,
        FirmwareType::NIC => 5,
        FirmwareType::HBA => 6,
        FirmwareType::HMC => 7,
        FirmwareType::Unknown => 0,
    }
}

/// Resolve a caller-provided firmware file path, following symlinks and
/// requiring the final canonical path to stay under the RMS firmware directory.
///
/// This is the "bring-your-own-files" path used by the direct firmware RPCs
/// (`UpdateFirmware`, `BatchUpdateFirmware`, `PushSwitchFirmware`, and direct
/// `ApplySwitchSystemImage`), where the caller stages files into `--firmware-dir`
/// itself rather than having RMS download them from a firmware manifest.
///
/// Integrity note: this function enforces **location safety only** — it
/// canonicalizes the path, confirms the file exists, and confirms it resolves
/// *inside* the firmware directory (preventing path traversal). It does **not**
/// hash or otherwise verify file contents, because on this path RMS never
/// fetched the bytes and has no trusted expected digest to compare against.
/// Content integrity is therefore the caller's responsibility: the caller
/// should verify its own SHA-256 (or equivalent) before invoking the RPC.
///
/// Contrast with the firmware manifest / firmware-object flow
/// ([`super::artifact_download`]), where RMS downloads artifacts and, when the
/// firmware manifest declares an optional `Sha256`, verifies the fetched bytes end-to-end.
pub(crate) fn resolve_firmware_file(
    filename: &str,
    firmware_dir: &Path,
) -> std::result::Result<PathBuf, String> {
    let firmware_dir = firmware_dir.canonicalize().map_err(|e| {
        format!(
            "failed to canonicalize firmware directory {}: {e}",
            firmware_dir.display()
        )
    })?;
    let path = Path::new(filename);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        firmware_dir.join(path)
    };
    let path = path.canonicalize().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            format!("firmware file not found: {}", path.display())
        } else {
            format!(
                "failed to canonicalize firmware file {}: {e}",
                path.display()
            )
        }
    })?;

    if path.starts_with(&firmware_dir) {
        Ok(path)
    } else {
        Err(format!(
            "firmware file {} is outside firmware directory {}",
            path.display(),
            firmware_dir.display()
        ))
    }
}

// Resolve firmware paths to canonical files under the RMS firmware directory.
fn build_firmware_targets(
    r: &rm::UpdateFirmwareRequest,
    firmware_dir: &Path,
    node_type: NodeType,
) -> std::result::Result<Vec<FirmwareTarget>, String> {
    let mut targets = Vec::new();

    if !r.firmware_targets.is_empty() {
        for ft in &r.firmware_targets {
            validate_firmware_target_entry(ft)?;
            let path = resolve_firmware_file(&ft.filename, firmware_dir)
                .map_err(|e| format!("{e} (component: {})", ft.target))?;
            targets.push(FirmwareTarget {
                component: firmware_target_component_for_node_type(node_type, &ft.target),
                firmware_file: path.to_string_lossy().into_owned(),
                expected_version: None,
                resolution: None,
                linkx_policy: None,
            });
        }
    } else if !r.filename.is_empty() {
        let path = resolve_firmware_file(&r.filename, firmware_dir)?;
        targets.push(FirmwareTarget {
            component: firmware_target_component_for_node_type(node_type, &r.target),
            firmware_file: path.to_string_lossy().into_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        });
    }

    Ok(targets)
}

// Build FirmwareTargets from proto entries, resolving relative
// filenames under the RMS firmware directory and validating each file exists.
#[cfg(test)]
pub(crate) fn build_firmware_targets_from_list(
    node_type: NodeType,
    target_list: &[rm::FirmwareTarget],
    firmware_dir: &Path,
) -> std::result::Result<Vec<FirmwareTarget>, String> {
    build_firmware_targets_from_list_with_expected_versions(
        node_type,
        target_list,
        firmware_dir,
        None,
    )
}

pub(crate) fn build_firmware_targets_from_list_with_expected_versions(
    node_type: NodeType,
    target_list: &[rm::FirmwareTarget],
    firmware_dir: &Path,
    expected_versions: Option<&HashMap<String, String>>,
) -> std::result::Result<Vec<FirmwareTarget>, String> {
    let mut targets = Vec::new();
    for ft in target_list {
        validate_firmware_target_entry(ft)?;
        let path = resolve_firmware_file(&ft.filename, firmware_dir)
            .map_err(|e| format!("{e} (component: {})", ft.target))?;
        targets.push(FirmwareTarget {
            component: firmware_target_component_for_node_type(node_type, &ft.target),
            firmware_file: path.to_string_lossy().into_owned(),
            expected_version: expected_versions
                .and_then(|versions| versions.get(&ft.filename))
                .cloned(),
            resolution: None,
            linkx_policy: None,
        });
    }
    Ok(targets)
}

fn firmware_target_component_for_node_type(node_type: NodeType, target: &str) -> String {
    if node_type.kind() == NodeKind::Powershelf {
        String::new()
    } else {
        target.to_owned()
    }
}

fn validate_firmware_target_entry(target: &rm::FirmwareTarget) -> std::result::Result<(), String> {
    if target.filename.is_empty() {
        return Err("firmware_targets entry has empty filename".into());
    }
    Ok(())
}

// Construct an ephemeral node instance for firmware-by-node-list. Credentials
// and target host come directly from the request; the caller has already
// validated that credentials required by the node type are non-empty.
pub(crate) fn build_ephemeral_node(
    node_info: &rm::NodeInfo,
    node_type: NodeType,
    expected_inventory: Option<ExpectedInventoryPolicy>,
    recovery: Option<RecoveryProfile>,
    insecure_bmc: bool,
) -> Result<Arc<NodeInstance>> {
    build_ephemeral_node_with_profiles(
        node_info,
        node_type,
        expected_inventory,
        recovery,
        &HardwareProfileCatalog::default(),
        insecure_bmc,
    )
}

pub(crate) fn build_ephemeral_node_with_profiles(
    node_info: &rm::NodeInfo,
    node_type: NodeType,
    expected_inventory: Option<ExpectedInventoryPolicy>,
    recovery: Option<RecoveryProfile>,
    profiles: &HardwareProfileCatalog,
    insecure_bmc: bool,
) -> Result<Arc<NodeInstance>> {
    let flat = flatten_node_info(node_info)?;
    // Direct switch firmware is a host/NVUE workflow, so it requires
    // host_endpoint and does not force callers to provide a BMC endpoint.
    // Compute and powershelf firmware are BMC/Redfish workflows.
    let bmc_endpoint = if node_type.kind() == NodeKind::Switch {
        flat.optional_bmc_endpoint(insecure_bmc)?
    } else {
        Some(flat.bmc_endpoint(insecure_bmc)?)
    };
    let host_endpoint = if node_type.kind() == NodeKind::Switch {
        Some(flat.switch_host_management_endpoint()?)
    } else if node_type.supports_flint_inband_firmware() {
        flat.optional_compute_host_ssh_endpoint()?
    } else {
        flat.optional_host_endpoint()?
    };

    let config = NodeConfig {
        id: node_info.node_id.clone(),
        node_type,
        bmc_endpoint,
        host_endpoint,
        expected_inventory,
        recovery,
        runtime_config: profiles.runtime_config_for_request(node_type),
    };

    NodeInstance::create(&config, node_info.rack_id.as_str())
}

/// Where an update job's recovery bundles come from. Decided where the
/// update is prepared, consumed at detection, never re-derived in between.
#[derive(Debug, Clone)]
pub(crate) enum RecoverySource {
    /// The two HMC files were materialized next to the update's payloads,
    /// from the same manifest, stored or inline. The job never touches the
    /// object store for them.
    Prepared(RecoveryBundles),
    /// The update's manifest carries no HMC recovery package for this node
    /// type (or the object predates recovery packages in the catalog).
    /// Detection that finds a component in recovery fails closed.
    Absent,
    /// A raw `update_firmware` has no manifest in hand: the latest
    /// available object that carries a recovery package. Fails closed when
    /// the candidates span several hardware types.
    Latest,
}

/// What recovery detection needs besides the node: the bundle source, and
/// the store plus digest policy the `Latest` source resolves through.
pub(crate) struct RecoveryDispatch {
    pub(crate) firmware_objects: Arc<dyn FirmwareObjectStore>,
    pub(crate) firmware_dir: PathBuf,
    pub(crate) artifact_require_sha256: bool,
    pub(crate) source: RecoverySource,
}

impl RackManagerServiceImpl {
    async fn prepare_firmware_node_for_job(
        &self,
        job: &RmsJobHandle,
        node: &NodeInstance,
    ) -> std::result::Result<(), (&'static str, JobFailure)> {
        self.initialize_nvue_client(node.nvue_client(), None)
            .await
            .map_err(|error| {
                (
                    "nvue_setup",
                    JobFailure::new(firmware_update_job_error(&error), error.message),
                )
            })?;

        self.admit_firmware_node_for_job(job, node)
            .map_err(|failure| ("scope_admission", failure))
    }

    /// Reserves configured switch identities before dispatch without device I/O.
    /// The firmware worker adds physical identity when NVUE can report it.
    pub(crate) fn admit_firmware_node_for_job(
        &self,
        job: &RmsJobHandle,
        node: &NodeInstance,
    ) -> std::result::Result<(), JobFailure> {
        if node.node_type().kind() != NodeKind::Switch {
            return Ok(());
        }

        let scopes =
            crate::api::grpc::switch_scope::configured_switch_scopes(node, node.nvue_client())
                .map_err(|error| JobFailure::new(JobError::Internal, error.message))?;

        self.admit_known_switch_job_scopes(job, scopes)
    }

    /// The recovery inputs of one update job, taken from the service's
    /// firmware object store, firmware directory, and digest policy.
    pub(crate) fn recovery_dispatch(&self, source: RecoverySource) -> RecoveryDispatch {
        RecoveryDispatch {
            firmware_objects: self.backends.firmware_objects.clone(),
            firmware_dir: self.firmware_dir.clone(),
            artifact_require_sha256: self.artifact_require_sha256,
            source,
        }
    }
}

/// VR NVL72 inventory and comparison need only the BMC; retain the existing
/// host endpoint requirements for GB200/GB300 switch inventory.
pub(crate) fn build_ephemeral_inventory_node(
    node_info: &rm::NodeInfo,
    node_type: NodeType,
    profiles: &HardwareProfileCatalog,
    insecure_bmc: bool,
) -> Result<Arc<NodeInstance>> {
    if node_type == NodeType::SwitchVrnvl72Nvidia {
        super::power_handlers::build_ephemeral_power_node(node_info, profiles, insecure_bmc)
            .map_err(RmsError::invalid_argument)
    } else {
        build_ephemeral_node_with_profiles(node_info, node_type, None, None, profiles, insecure_bmc)
    }
}

#[allow(clippy::too_many_arguments)] // the update request's shape: targets, two flags, poll timing, recovery dispatch
#[cfg(test)]
pub(crate) fn spawn_firmware_update_job<N: Node + Send + Sync + 'static>(
    tracker: Arc<JobTracker>,
    job_handle: RmsJobHandle,
    node: Arc<N>,
    targets: Vec<FirmwareTarget>,
    activate: bool,
    force_update: bool,
    poll_options: FirmwareTaskPollOptions,
    recovery: RecoveryDispatch,
) {
    spawn_firmware_update_job_with_policy(
        tracker,
        job_handle,
        node,
        targets,
        activate,
        force_update,
        poll_options,
        false,
        recovery,
        None,
        None,
    );
}

#[allow(clippy::too_many_arguments)] // same shape as spawn_firmware_update_job
#[cfg(test)]
pub(crate) fn spawn_firmware_object_update_job<N: Node + Send + Sync + 'static>(
    tracker: Arc<JobTracker>,
    job_handle: RmsJobHandle,
    node: Arc<N>,
    targets: Vec<FirmwareTarget>,
    activate: bool,
    force_update: bool,
    poll_options: FirmwareTaskPollOptions,
    recovery: RecoveryDispatch,
) {
    spawn_firmware_update_job_with_policy(
        tracker,
        job_handle,
        node,
        targets,
        activate,
        force_update,
        poll_options,
        true,
        recovery,
        None,
        None,
    );
}

/// Detect through the node's recovery capability and recover before the
/// update, inside the update job itself.
///
/// `Ok` means the update may proceed: recovery ran and the flow's
/// closing confirm verified every listed component healthy, or nothing
/// watched was in recovery. The stricter pre-flash proceed condition
/// (every watched component Health == "OK", or no flash) is a separate
/// change. Until it lands, non-recovery unhealthy readings enter the
/// update path unchanged.
async fn dispatch_compute_oob_recovery(
    node: &dyn Node,
    recovery: &dyn Recoverable,
    source: RecoverySource,
    firmware_objects: &dyn FirmwareObjectStore,
    firmware_dir: &Path,
    artifact_require_sha256: bool,
    cancel: &CancellationToken,
) -> Result<()> {
    // An unreadable inventory maps to an empty one by policy: detection is
    // fail-open. An empty inventory shows no recovery signature, so the
    // update proceeds as it did before detection existed and fails on its
    // own terms if the node is genuinely sick. Post-flash verification
    // reads stay fail-closed, there the read is the proof of success.
    // "Empty" stands in for "healthy" only here.
    // TODO: propagate the error once the pre-flash health check makes an
    // unreadable inventory blocking.
    let inventory = match node.get_firmware_inventory().await {
        Ok(inventory) => inventory,
        Err(error) => {
            tracing::warn!(
                node = %node.id(),
                error = %error.message,
                "recovery detection skipped: firmware inventory read failed"
            );
            Vec::new()
        }
    };
    // The read above is network time. A cancel that landed in it is why
    // the job stops, whatever the read showed.
    if cancel.is_cancelled() {
        return Err(RmsError::cancelled(
            "recovery flow cancelled during recovery detection",
        ));
    }
    let in_recovery = recovery.detect(&inventory);
    if in_recovery.is_empty() {
        // Not an error: nothing watched reports the recovery signature.
        return Ok(());
    }

    let bundles = match source {
        RecoverySource::Prepared(bundles) => bundles,
        RecoverySource::Absent => {
            return Err(RmsError::failed_precondition(format!(
                "components in recovery state ({}) but the update's manifest carries no HMC \
                 recovery package for {}",
                in_recovery.join(", "),
                node.node_type()
            )));
        }
        RecoverySource::Latest => {
            let resolved = resolve_recovery_bundles_from_latest_object(
                firmware_objects,
                firmware_dir,
                node.node_type(),
                artifact_require_sha256,
            )
            .await;
            // Resolution reads the object store. Same rule as the read above.
            if cancel.is_cancelled() {
                return Err(RmsError::cancelled(
                    "recovery flow cancelled during bundle resolution",
                ));
            }
            resolved?
        }
    };
    recovery.recover(&in_recovery, bundles, cancel).await
}

#[allow(clippy::too_many_arguments)] // the three policy flags are the update request's own shape
fn spawn_firmware_update_job_with_policy<N: Node + Send + Sync + 'static>(
    tracker: Arc<JobTracker>,
    job_handle: RmsJobHandle,
    node: Arc<N>,
    targets: Vec<FirmwareTarget>,
    activate: bool,
    force_update: bool,
    poll_options: FirmwareTaskPollOptions,
    allow_mixed_inband_oob: bool,
    recovery: RecoveryDispatch,
    profile: Option<Arc<HardwareUpdatePlan>>,
    switch_admission: Option<(RackManagerServiceImpl, nvue_client::SharedClient)>,
) {
    let job_handle =
        profile_trace::trace_firmware_job(job_handle, Some(node.node_type()), profile.as_deref());
    let poll_options = profile
        .as_deref()
        .map_or(poll_options, |profile| FirmwareTaskPollOptions {
            timeout: profile.polling().task_timeout(),
            interval: profile.polling().interval(),
            transient_error_timeout: profile.polling().transient_error_budget(),
        });
    let RecoveryDispatch {
        firmware_objects,
        firmware_dir,
        artifact_require_sha256,
        source,
    } = recovery;
    let progress_tracker = tracker.clone();
    tracker
        .spawn_job(job_handle, move |job| async move {
            job.progress("Starting firmware update");
            let job_id = job.id().to_string();
            // Cooperative cancellation token, checked at each workflow boundary
            // and threaded into the firmware poll loop so a cancelled job (e.g.
            // graceful shutdown) bails out promptly instead of holding the node
            // for up to the full poll timeout. Mirrors the switch-image job path.
            let cancel = job.cancellation_token();

            if let Err(error) = validate_firmware_request_with_policy(
                node.node_type(),
                &targets,
                activate,
                allow_mixed_inband_oob,
            ) {
                fail_tracked_firmware_job(
                    job,
                    "validation",
                    None,
                    firmware_update_job_error(&error),
                    &error.message,
                );
                return;
            }

            if force_update && targets.iter().any(is_linkx_target) {
                fail_tracked_firmware_job(job, "validation", None, JobError::InvalidArgument,
                    "LINKX does not permit force_update or health-gate bypasses");
                return;
            }

            // Optional NVUE discovery belongs to the accepted worker so offline
            // switches do not delay RPC acceptance or the rest of a batch.
            if let Some((service, client)) = switch_admission {
                let discovery = crate::api::grpc::switch_scope::discover_switch_scopes(
                    &client, node.rack_id(), node.id(),
                );

                let Some(scopes) = cancel.run_until_cancelled(discovery).await else {
                    fail_tracked_firmware_job(job, "cancelled", None, JobError::Other,
                        "firmware update cancelled during switch identity discovery");

                    return;
                };

                // Missing metadata permits BMC recovery; a discovered conflict
                // must stop the worker before any device write.
                if let Ok(scopes) = scopes
                    && let Err(failure) = service.admit_known_switch_job_scopes(&job, scopes)
                {
                    fail_tracked_firmware_job(job, "scope_admission", None,
                        job_error_of(&failure.error), &failure.message);

                    return;
                }
            }

            if let Some(recovery) = node.recovery() {
                // A cancel already pending ends the job before the detection
                // read and the bundle resolution. The flow's own checks start
                // at its first stage.
                if cancel.is_cancelled() {
                    fail_tracked_firmware_job(
                        job,
                        "cancelled",
                        None,
                        JobError::Other,
                        "firmware update job cancelled before recovery detection",
                    );
                    return;
                }
                if let Err(error) = dispatch_compute_oob_recovery(
                    node.as_ref(),
                    recovery,
                    source,
                    firmware_objects.as_ref(),
                    &firmware_dir,
                    artifact_require_sha256,
                    &cancel,
                )
                .await
                {
                    if error.code == ErrorCode::Cancelled {
                        fail_tracked_firmware_job(
                            job,
                            "cancelled",
                            None,
                            JobError::Other,
                            &error.message,
                        );
                        return;
                    }
                    fail_tracked_firmware_job(
                        job,
                        "recovery",
                        None,
                        firmware_update_job_error(&error),
                        &format!("compute OOB recovery: {}", error.message),
                    );
                    return;
                }
            }

            let stages = match firmware_update_stages_with_profile_policy(
                node.node_type(),
                &targets,
                activate,
                allow_mixed_inband_oob,
                profile.as_deref(),
            ) {
                Ok(stages) => stages,
                Err(error) => {
                    let job_error = firmware_update_job_error(&error);
                    fail_tracked_firmware_job(job, "planning", None, job_error, &error.message);
                    return;
                }
            };

            let targets: Vec<_> = stages
                .iter()
                .flat_map(|stage| stage.targets.iter().cloned())
                .collect();
            let pre_update_check = match pre_update_firmware_version_check(
                node.as_ref(),
                &targets,
                force_update,
                &cancel,
            )
            .await
            {
                Ok(summary) => summary,
                Err(error) => {
                    fail_tracked_firmware_job(
                        job,
                        "pre_update_version_check",
                        None,
                        firmware_update_job_error(&error),
                        &error.message,
                    );
                    return;
                }
            };
            let already_current = pre_update_check.as_ref().is_some_and(|summary| {
                summary.matched && summary.details["all_targets_matched"] == true
            });

            let mut skipped = 0usize;
            let mut target_results = Vec::new();
            let mut completed_targets = 0usize;
            let mut activation_count = 0usize;
            let mut post_activation_checks = Vec::new();
            for (stage_index, stage) in stages.iter().enumerate() {
                let mut stage_applied_targets = Vec::new();
                let groups = firmware_update_groups(node.node_type(), &stage.targets);
                for group in &groups {
                    if cancel.is_cancelled() {
                        fail_tracked_firmware_job(
                            job,
                            "cancelled",
                            None,
                            JobError::Other,
                            "firmware update job cancelled before starting the next target group",
                        );
                        return;
                    }
                    let Some(target) = group.first() else {
                        continue;
                    };
                    let disposition = if already_current {
                        Ok(FirmwareTargetDisposition::Skipped {
                            reason: "installed firmware matches all selected package versions"
                                .to_owned(),
                        })
                    } else {
                        job.progress(format!(
                            "Uploading target {}/{}: {}",
                            completed_targets + 1,
                            targets.len(),
                            firmware_target_label(target)
                        ));
                        update_firmware_group_with_bmc_recovery(
                            node.as_ref(),
                            group,
                            force_update,
                            poll_options,
                            Some(FirmwareTaskProgress {
                                tracker: progress_tracker.as_ref(),
                                job_id: &job_id,
                                target,
                            }),
                            &cancel,
                        )
                        .await
                    };

                    match disposition {
                        Ok(disposition) => {
                            target_results.extend(
                                group.iter().map(|target| {
                                    firmware_target_result_json(target, &disposition)
                                }),
                            );
                            match disposition {
                                FirmwareTargetDisposition::Applied { .. } => {
                                    stage_applied_targets.extend(group.iter().cloned());
                                }
                                FirmwareTargetDisposition::Skipped { reason } => {
                                    tracing::info!(
                                        job_id = %job_id,
                                        target = %firmware_target_label(target),
                                        reason = %reason,
                                        "skipping firmware target: no update applied"
                                    );
                                    skipped += group.len();
                                }
                            }
                        }
                        Err(FirmwareTargetUpdateError::Update(e)) => {
                            let job_error = firmware_update_job_error(&e);
                            let message = format!(
                                "Upload failed for target {}: {}",
                                firmware_target_label(target),
                                e.message
                            );
                            fail_tracked_firmware_job(
                                job,
                                "update",
                                Some(target),
                                job_error,
                                &message,
                            );
                            return;
                        }
                        Err(FirmwareTargetUpdateError::Poll(e)) => {
                            let message = format!(
                                "Poll failed for target {}: {}",
                                firmware_target_label(target),
                                e.message
                            );
                            fail_tracked_firmware_job(
                                job,
                                "poll",
                                Some(target),
                                JobError::Other,
                                &message,
                            );
                            return;
                        }
                    }
                    completed_targets += group.len();
                }

                if let Some(activation_mode) = stage.activation.clone()
                    && !stage_applied_targets.is_empty()
                {
                    if cancel.is_cancelled() {
                        fail_tracked_firmware_job(
                            job,
                            "cancelled",
                            None,
                            JobError::Other,
                            "firmware update job cancelled before activation",
                        );
                        return;
                    }
                    if stages.len() == 1 {
                        job.progress("Activating firmware");
                    } else {
                        job.progress(format!(
                            "Activating firmware after stage {}/{}",
                            stage_index + 1,
                            stages.len()
                        ));
                    }
                    if let Err(e) =
                        activate_firmware_with_mode(node.as_ref(), &cancel, activation_mode).await
                    {
                        let message =
                            format!("Firmware flashed but activation failed: {}", e.message);
                        fail_tracked_firmware_job(
                            job,
                            "activation",
                            None,
                            JobError::Other,
                            &message,
                        );
                        return;
                    }
                    activation_count += 1;

                    let Some(verification) = stage.verification else {
                        continue;
                    };

                    job.progress("Verifying activated firmware versions and system readiness");
                    match post_activation_version_check_with_policy(
                        node.as_ref(),
                        &stage_applied_targets,
                        &cancel,
                        Some(verification),
                    )
                    .await
                    {
                        Ok(summary) => {
                            if let Some(summary) = summary {
                                post_activation_checks.push(summary.details);
                            }
                        }
                        Err(e) => {
                            fail_tracked_firmware_job(
                                job,
                                "post_activation_version_check",
                                None,
                                JobError::Other,
                                &e.message,
                            );
                            return;
                        }
                    }
                }
            }

            let all_skipped = skipped == targets.len();
            let post_activation_check = match post_activation_checks.len() {
                0 => None,
                1 => post_activation_checks.pop(),
                _ => Some(serde_json::json!({"stages": post_activation_checks})),
            };
            let mut result = serde_json::json!({
                "rack_id": node.rack_id(),
                "node_id": node.id(),
                "targets_completed": targets.len() - skipped,
                "targets_skipped": skipped,
                "targets": target_results,
                "activated": activation_count > 0 || target_results.iter().any(|result| {
                    result.get("component").and_then(serde_json::Value::as_str).is_some_and(|c| c.eq_ignore_ascii_case("LINKX"))
                        && result.get("status").and_then(serde_json::Value::as_str) == Some("completed")
                }),
                "pre_update_version_check": pre_update_check.map(|summary| summary.details),
                "post_activation_version_check": post_activation_check,
                "status": if all_skipped {
                    "already_up_to_date".to_owned()
                } else {
                    "completed".to_owned()
                },
            });
            if node.node_type() == NodeType::ComputeGb300Supermicro {
                insert_json_field(
                    &mut result,
                    "activation_count",
                    serde_json::json!(activation_count),
                );
            }
            job.complete("Completed", result.to_string());
        })
        .detach();
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FirmwareUpdateStage {
    targets: Vec<FirmwareTarget>,
    activate_after: bool,
}

#[cfg(test)]
fn firmware_update_stages(
    node_type: NodeType,
    targets: &[FirmwareTarget],
    activate: bool,
) -> Result<Vec<FirmwareUpdateStage>> {
    firmware_update_stages_with_policy(node_type, targets, activate, false)
}

fn firmware_update_stages_with_policy(
    node_type: NodeType,
    targets: &[FirmwareTarget],
    activate: bool,
    allow_mixed_inband_oob: bool,
) -> Result<Vec<FirmwareUpdateStage>> {
    if targets.iter().any(is_linkx_target) {
        validate_firmware_request_with_policy(
            node_type,
            targets,
            activate,
            allow_mixed_inband_oob,
        )?;
        let (linkx, out_of_band): (Vec<_>, Vec<_>) =
            targets.iter().cloned().partition(is_linkx_target);
        let mut stages = Vec::new();
        if !out_of_band.is_empty() {
            stages.push(FirmwareUpdateStage {
                targets: out_of_band,
                activate_after: true,
            });
        }
        // LinkX performs module activation and verification inside NVFWUPD.
        // Never follow it with the GB200/VR tray AC-cycle activation path.
        stages.push(FirmwareUpdateStage {
            targets: linkx,
            activate_after: false,
        });
        return Ok(stages);
    }
    if node_type.supports_flint_inband_firmware() {
        let (inband, out_of_band): (Vec<_>, Vec<_>) = targets
            .iter()
            .cloned()
            .partition(|target| flint_family_for_target(target).is_some());
        if !inband.is_empty() && !out_of_band.is_empty() {
            if !allow_mixed_inband_oob {
                return Err(RmsError::invalid_argument(
                    "out-of-band and in-band firmware targets cannot be mixed in one request",
                ));
            }
            if !activate {
                return Err(RmsError::failed_precondition(
                    "mixed GB200 in-band and out-of-band firmware updates require activation between stages",
                ));
            }
            return Ok(vec![
                FirmwareUpdateStage {
                    targets: inband,
                    activate_after: true,
                },
                FirmwareUpdateStage {
                    targets: out_of_band,
                    activate_after: true,
                },
            ]);
        }
    }

    if node_type != NodeType::ComputeGb300Supermicro {
        return Ok(vec![FirmwareUpdateStage {
            targets: targets.to_vec(),
            activate_after: activate,
        }]);
    }

    let mut nosbios = Vec::new();
    let mut bios = Vec::new();
    let mut bmc = Vec::new();
    for target in targets {
        let component = target.component.trim();
        if component.eq_ignore_ascii_case("HMC")
            || component.eq_ignore_ascii_case(SUPERMICRO_HGX_TARGET)
        {
            nosbios.push(target.clone());
        } else if component.eq_ignore_ascii_case(SUPERMICRO_BIOS_TARGET) {
            bios.push(target.clone());
        } else if component.is_empty()
            || component.eq_ignore_ascii_case("BMC")
            || component.eq_ignore_ascii_case(SUPERMICRO_BMC_TARGET)
        {
            bmc.push(target.clone());
        } else {
            return Err(RmsError::invalid_argument(format!(
                "unsupported Supermicro GB300 firmware target {}",
                firmware_target_label(target)
            )));
        }
    }

    if !nosbios.is_empty() && !bios.is_empty() && !activate {
        return Err(RmsError::failed_precondition(
            "Supermicro GB300 nosbios and BIOS updates require activation so RMS can power-cycle between the two payloads",
        ));
    }

    let mut stages = Vec::new();
    if !nosbios.is_empty() {
        stages.push(FirmwareUpdateStage {
            targets: nosbios,
            activate_after: activate,
        });
    }
    if !bios.is_empty() {
        stages.push(FirmwareUpdateStage {
            targets: bios,
            activate_after: activate,
        });
    }
    if !bmc.is_empty() {
        stages.push(FirmwareUpdateStage {
            targets: bmc,
            // The host-BMC multipart update is immediate on the Supermicro
            // UpdateService and does not require another host AC cycle.
            activate_after: false,
        });
    }

    Ok(stages)
}

/// All decisions needed by execution; the executor never reinterprets a profile.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutableFirmwareStage {
    targets: Vec<FirmwareTarget>,
    activation: Option<FirmwareActivationMode>,
    verification: Option<VerificationPolicy>,
}

impl ExecutableFirmwareStage {
    /// Resolve activation and verification once, before the executor runs.
    fn new(
        node_type: NodeType,
        targets: Vec<FirmwareTarget>,
        activate: bool,
        strategy: Option<ActivationStrategy>,
        verification: VerificationPolicy,
    ) -> Result<Self> {
        let activation = activate
            .then(|| activation_mode_for_type_and_strategy(node_type, strategy))
            .transpose()?;
        let verification = (activation.is_some()
            && node_type.kind() != NodeKind::Powershelf
            && verification.enabled())
        .then_some(verification);
        Ok(Self {
            targets,
            activation,
            verification,
        })
    }
}

fn default_stage_verification() -> VerificationPolicy {
    VerificationPolicy::required(
        POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY,
        std::num::NonZeroU32::new(POST_ACTIVATION_VERSION_CHECK_ATTEMPTS as u32)
            .expect("verification attempts are nonzero"),
        POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL,
    )
}

fn firmware_update_stages_with_profile_policy(
    node_type: NodeType,
    targets: &[FirmwareTarget],
    activate: bool,
    allow_mixed_inband_oob: bool,
    profile: Option<&HardwareUpdatePlan>,
) -> Result<Vec<ExecutableFirmwareStage>> {
    validate_firmware_request_with_policy(node_type, targets, activate, allow_mixed_inband_oob)?;
    let Some(profile) = profile else {
        return firmware_update_stages_with_policy(
            node_type,
            targets,
            activate,
            allow_mixed_inband_oob,
        )?
        .into_iter()
        .map(|stage| {
            ExecutableFirmwareStage::new(
                node_type,
                stage.targets,
                stage.activate_after,
                None,
                default_stage_verification(),
            )
        })
        .collect();
    };
    if profile.node_type() != node_type {
        return Err(RmsError::invalid_argument(format!(
            "hardware profile {} is for {}, not {}",
            profile.profile_id(),
            profile.node_type().as_str(),
            node_type.as_str()
        )));
    }
    firmware_update_stages_from_profile(targets, activate, profile)
}

/// Build the NIC stage first, profile stages in order, then module-activated LinkX.
fn firmware_update_stages_from_profile(
    targets: &[FirmwareTarget],
    activate: bool,
    profile: &HardwareUpdatePlan,
) -> Result<Vec<ExecutableFirmwareStage>> {
    let mut remaining: Vec<_> = targets.iter().collect();
    let mut stages = Vec::new();
    let linkx: Vec<_> = remaining
        .extract_if(.., |target| is_linkx_target(target))
        .cloned()
        .collect();
    let inband: Vec<_> = remaining
        .extract_if(.., |target| flint_family_for_target(target).is_some())
        .cloned()
        .collect();
    if !inband.is_empty() {
        // NIC updates retain their mandatory activation and verification boundary.
        stages.push(ExecutableFirmwareStage::new(
            profile.node_type(),
            inband,
            true,
            None,
            default_stage_verification(),
        )?);
    }
    for configured_stage in profile.stages() {
        let mut stage_targets = Vec::new();
        for configured_target in configured_stage.targets() {
            let resolved = remaining
                .extract_if(.., |target| {
                    profile_target_matches(configured_target, target)
                })
                .map(|target| configured_target.resolve(target))
                .collect::<Result<Vec<_>>>()?;
            // One manifest artifact can fan out to several explicit request paths.
            // Reject duplicate uploads of the same package to the same destination.
            let mut uploads = std::collections::HashSet::new();
            for target in &resolved {
                let destination = target
                    .resolution
                    .as_ref()
                    .map(|resolved| &resolved.destination);
                if !uploads.insert((&target.firmware_file, destination)) {
                    return Err(RmsError::invalid_argument(format!(
                        "duplicate firmware upload for hardware profile target {}",
                        configured_target.id()
                    )));
                }
            }
            stage_targets.extend(resolved);
        }
        if !stage_targets.is_empty() {
            stages.push(ExecutableFirmwareStage::new(
                profile.node_type(),
                stage_targets,
                activate && configured_stage.activate_after(),
                profile.activation(),
                profile.verification(),
            )?);
        }
    }

    if !remaining.is_empty() {
        let unmatched = remaining
            .into_iter()
            .map(firmware_target_label)
            .collect::<Vec<_>>();
        return Err(RmsError::invalid_argument(format!(
            "firmware target(s) are not allowed by hardware profile {}: {}",
            profile.profile_id(),
            unmatched.join(", ")
        )));
    }

    if !linkx.is_empty() {
        // The LinkX workflow activates and verifies its modules itself. Keep it
        // after every profile-defined OOB stage, without a subsequent tray cycle.
        stages.push(ExecutableFirmwareStage::new(
            profile.node_type(),
            linkx,
            false,
            None,
            default_stage_verification(),
        )?);
    }

    Ok(stages)
}

fn profile_target_matches(configured: &FirmwareTargetPlan, target: &FirmwareTarget) -> bool {
    let filename = Path::new(&target.firmware_file)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&target.firmware_file);
    if !configured.filename().matches(filename) {
        return false;
    }

    target.resolution.as_ref().map_or_else(
        || configured.matches_component(&target.component),
        |resolved| resolved.profile_target_id == configured.id(),
    )
}

fn fail_tracked_firmware_job(
    job: RmsJobHandle,
    phase: &str,
    target: Option<&FirmwareTarget>,
    error_code: JobError,
    raw_message: &str,
) {
    let (message, result_json) =
        firmware_failure_message_and_result_json(phase, target, error_code, raw_message);
    job.fail(
        crate::orchestrator::job_lifecycle::JobFailure::new(error_code, message)
            .with_result_json(result_json),
    );
}

fn firmware_update_job_error(error: &RmsError) -> JobError {
    if error.code == ErrorCode::FailedPrecondition {
        JobError::FailedPrecondition
    } else {
        JobError::ClientError
    }
}

/// Uses the caller-resolved node type; descriptor-only types have no protobuf enum value.
fn build_firmware_targets_by_type(
    r: &rm::BatchUpdateFirmwareByNodeTypeRequest,
    firmware_dir: &Path,
    node_type: NodeType,
) -> std::result::Result<Vec<FirmwareTarget>, String> {
    let mut targets = Vec::new();

    if !r.firmware_targets.is_empty() {
        for ft in &r.firmware_targets {
            validate_firmware_target_entry(ft)?;
            let path = resolve_firmware_file(&ft.filename, firmware_dir)
                .map_err(|e| format!("{e} (component: {})", ft.target))?;
            targets.push(FirmwareTarget {
                component: firmware_target_component_for_node_type(node_type, &ft.target),
                firmware_file: path.to_string_lossy().into_owned(),
                expected_version: None,
                resolution: None,
                linkx_policy: None,
            });
        }
    } else if !r.filename.is_empty() {
        let path = resolve_firmware_file(&r.filename, firmware_dir)?;
        targets.push(FirmwareTarget {
            component: firmware_target_component_for_node_type(node_type, &r.target),
            firmware_file: path.to_string_lossy().into_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        });
    }

    Ok(targets)
}

fn firmware_inventory_info_to_proto(info: FirmwareInfo) -> rm::FirmwareInventoryInfo {
    rm::FirmwareInventoryInfo {
        name: info.name,
        version: info.version,
        updateable: info.updateable,
        target: info.target,
        health: info.health,
        firmware_type: firmware_type_to_int(info.firmware_type),
    }
}

impl RackManagerServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_profiled_firmware_update_job(
        &self,
        pending: RmsJobHandle,
        node: Arc<NodeInstance>,
        targets: Vec<FirmwareTarget>,
        activate: bool,
        force_update: bool,
        from_manifest: bool,
        recovery: RecoveryDispatch,
    ) {
        let profile = self.hardware_profiles.shared_for_request(node.node_type());
        let switch_admission = node
            .nvue_client()
            .cloned()
            .map(|client| (self.clone(), client));

        spawn_firmware_update_job_with_policy(
            self.job_tracker.clone(),
            pending,
            node,
            targets,
            activate,
            force_update,
            self.workflow_timings.firmware_task_poll,
            from_manifest,
            recovery,
            profile,
            switch_admission,
        );
    }

    pub(crate) async fn handle_get_node_firmware_inventory(
        &self,
        req: tonic::Request<rm::GetNodeFirmwareInventoryRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetNodeFirmwareInventoryResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut resp = rm::GetNodeFirmwareInventoryResponse {
            status: rm::ReturnCode::Failure.into(),
            firmware_list: Vec::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(_) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, "node not found");
                return Ok(tonic::Response::new(resp));
            }
        };

        if node.node_type() != NodeType::SwitchVrnvl72Nvidia
            && let Err(error) = self.initialize_nvue_client(node.nvue_client(), None).await
        {
            tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %error.message, "failed to configure NVUE client");
            return Ok(tonic::Response::new(resp));
        }

        match node.get_firmware_inventory().await {
            Ok(inventory) => {
                resp.firmware_list = inventory
                    .into_iter()
                    .map(firmware_inventory_info_to_proto)
                    .collect();
                resp.status = rm::ReturnCode::Success.into();
            }
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e.message, "GetNodeFirmwareInventory failed")
            }
        }
        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_batch_get_firmware_inventory(
        &self,
        req: tonic::Request<rm::BatchGetFirmwareInventoryRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchGetFirmwareInventoryResponse>, tonic::Status>
    {
        type InventoryOutcome = (
            usize,
            String,
            std::result::Result<Vec<FirmwareInfo>, String>,
        );

        let request = req.into_inner();
        let nodes = request.nodes.map(|nodes| nodes.nodes).unwrap_or_default();
        let total_nodes = nodes.len() as u32;
        let mut common = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::with_capacity(nodes.len()),
            job_id: String::new(),
            stats: Some(rm::NodeOperationStats {
                total_nodes,
                successful_nodes: 0,
                failed_nodes: 0,
            }),
        };
        let mut response = rm::BatchGetFirmwareInventoryResponse {
            response: None,
            nodes: Vec::with_capacity(nodes.len()),
        };

        if nodes.is_empty() {
            common.message = "No nodes specified in request".to_owned();
            response.response = Some(common);
            return Ok(tonic::Response::new(response));
        }

        let mut outcomes = Vec::new();
        let mut ready = Vec::new();
        for (index, node_info) in nodes.iter().enumerate() {
            let node_type = match resolve_node_info(node_info) {
                Ok(node_type) => node_type,
                Err(error) => {
                    outcomes.push((index, node_info.node_id.clone(), Err(error.to_string())));
                    continue;
                }
            };

            match build_ephemeral_inventory_node(
                node_info,
                node_type,
                &self.hardware_profiles,
                self.insecure_bmc,
            ) {
                Ok(node) => ready.push((index, node_info.node_id.clone(), node)),
                Err(error) => outcomes.push((index, node_info.node_id.clone(), Err(error.message))),
            }
        }

        let fetched: Vec<InventoryOutcome> =
            futures::stream::iter(ready.into_iter().map(|(index, node_id, node)| async move {
                let inventory = async {
                    if node.node_type() != NodeType::SwitchVrnvl72Nvidia {
                        self.initialize_nvue_client(node.nvue_client(), None)
                            .await
                            .map_err(|error| error.message)?;
                    }
                    node.get_firmware_inventory()
                        .await
                        .map_err(|error| error.message)
                }
                .await;
                (index, node_id, inventory)
            }))
            .buffer_unordered(FIRMWARE_INVENTORY_FETCH_CONCURRENCY)
            .collect()
            .await;
        outcomes.extend(fetched);
        outcomes.sort_by_key(|(index, _, _)| *index);

        let mut successful_nodes = 0u32;
        let mut failed_nodes = 0u32;
        for (_, node_id, outcome) in outcomes {
            match outcome {
                Ok(inventory) => {
                    successful_nodes += 1;
                    common.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Success.into(),
                        error_message: String::new(),
                    });
                    response.nodes.push(rm::NodeFirmwareInventory {
                        node_id,
                        firmware_list: inventory
                            .into_iter()
                            .map(firmware_inventory_info_to_proto)
                            .collect(),
                    });
                }
                Err(error_message) => {
                    failed_nodes += 1;
                    tracing::error!(node = %node_id, error = %error_message, "stateless firmware inventory query failed");
                    common.node_results.push(rm::NodeOperationResult {
                        node_id,
                        status: rm::ReturnCode::Failure.into(),
                        error_message,
                    });
                }
            }
        }

        common.stats = Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes,
            failed_nodes,
        });
        if failed_nodes == 0 {
            common.status = rm::ReturnCode::Success.into();
            common.message =
                format!("Firmware inventory queried for {successful_nodes}/{total_nodes} nodes");
        } else {
            common.message = format!(
                "Firmware inventory query completed with {successful_nodes} successes and {failed_nodes} failures out of {total_nodes} nodes"
            );
        }
        response.response = Some(common);

        Ok(tonic::Response::new(response))
    }

    /// The main handler for the UpdateFirmware RPC.
    /// Initializes a firmware update on a specific node asynchronously.
    /// Returns a job ID immediately. Use GetFirmwareJobStatus to track progress.
    pub(crate) async fn handle_update_firmware(
        &self,
        req: tonic::Request<rm::UpdateFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateFirmwareResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::UpdateFirmwareResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            error_code: 0,
            job_id: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(_) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, "node not found");
                resp.message = "node not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };

        // Claim the node before touching it: NVUE setup rebuilds the registered
        // node's shared client transport, which must not happen while another
        // job still owns the node.
        //
        // Every early return below this point must seal `pending` explicitly.
        // Dropping the handle instead seals the job as "abandoned" with an
        // internal error code, which buries the real, caller-caused reason.
        let pending = match self.job_tracker.create_visible_workflow_job_if_node_idle(
            &r.rack_id,
            &r.node_id,
            JobType::FirmwareUpdate,
        ) {
            Ok(pending) => pending,
            Err(failure) => {
                tracing::warn!(
                    node = r.node_id,
                    rack = r.rack_id,
                    message = %failure.message,
                    "firmware job rejected"
                );

                resp.error_code =
                    rm::FirmwareUpdateError::from(job_error_of(&failure.error)).into();
                resp.message = failure.message;
                return Ok(tonic::Response::new(resp));
            }
        };

        let pending = self.trace_firmware_job(pending, Some(node.node_type()));

        if let Err((phase, failure)) = self.prepare_firmware_node_for_job(&pending, &node).await {
            let code = job_error_of(&failure.error);

            tracing::error!(node = %r.node_id, rack = %r.rack_id, phase, error = %failure.message, "firmware preparation failed");

            resp.status = rm::ReturnCode::Failure.into();
            resp.error_code = rm::FirmwareUpdateError::from(code).into();
            fail_tracked_firmware_job(pending, phase, None, code, &failure.message);

            resp.message = failure.message;
            return Ok(tonic::Response::new(resp));
        }

        let targets = match build_firmware_targets(&r, &self.firmware_dir, node.node_type()) {
            Ok(t) if t.is_empty() => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, "no firmware targets specified");
                resp.message = "no firmware targets specified".into();
                fail_tracked_firmware_job(
                    pending,
                    "planning",
                    None,
                    JobError::InvalidArgument,
                    &resp.message,
                );
                return Ok(tonic::Response::new(resp));
            }
            Ok(t) => t,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "invalid firmware targets");
                resp.message = e;
                resp.error_code = rm::FirmwareUpdateError::FileNotFound.into();
                fail_tracked_firmware_job(
                    pending,
                    "planning",
                    None,
                    JobError::FileNotFound,
                    &resp.message,
                );
                return Ok(tonic::Response::new(resp));
            }
        };
        if let Err(error) = validate_firmware_request(node.node_type(), &targets, r.activate) {
            tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %error.message, "invalid firmware workflow");
            resp.message = error.message;
            resp.error_code = rm::FirmwareUpdateError::ClientFailure.into();
            fail_tracked_firmware_job(
                pending,
                "validation",
                None,
                JobError::InvalidArgument,
                &resp.message,
            );
            return Ok(tonic::Response::new(resp));
        }

        let job_id = pending.id().to_string();

        resp.job_id = job_id.clone();
        resp.status = rm::ReturnCode::Success.into();
        resp.error_code = rm::FirmwareUpdateError::Success.into();
        resp.message = format!("Firmware update job created for {}", r.node_id);

        // Spawn background task: upload each target, poll until done, optionally activate
        self.spawn_profiled_firmware_update_job(
            pending,
            node,
            targets,
            r.activate,
            r.force_update,
            false,
            self.recovery_dispatch(RecoverySource::Latest),
        );

        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_batch_update_firmware_by_node_type(
        &self,
        req: tonic::Request<rm::BatchUpdateFirmwareByNodeTypeRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::BatchUpdateFirmwareByNodeTypeResponse>,
        tonic::Status,
    > {
        let r = req.into_inner();
        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::new(),
            job_id: String::new(),
            stats: None,
        };

        let mut jobs: Vec<rm::NodeFirmwareJobInfo> = Vec::new();

        let target_type = match resolve_node_type(Some(r.node_type), r.node_descriptor.as_ref()) {
            Ok(node_type) => node_type,
            Err(error) => {
                tracing::error!(node_type = r.node_type, "unknown node type");
                batch.message = error.to_string();
                return Ok(tonic::Response::new(
                    batch_update_firmware_by_node_type_response(batch, jobs, 0, 0, 0),
                ));
            }
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                batch.message = "rack not found".into();
                return Ok(tonic::Response::new(
                    batch_update_firmware_by_node_type_response(batch, jobs, 0, 0, 0),
                ));
            }
        };

        let targets = match build_firmware_targets_by_type(&r, &self.firmware_dir, target_type) {
            Ok(t) if t.is_empty() => {
                tracing::error!(rack = %r.rack_id, "no firmware targets specified");
                batch.message = "no firmware targets specified".into();
                return Ok(tonic::Response::new(
                    batch_update_firmware_by_node_type_response(batch, jobs, 0, 0, 0),
                ));
            }
            Ok(t) => t,
            Err(e) => {
                tracing::error!(rack = %r.rack_id, error = %e, "invalid firmware targets");
                batch.message = e;
                return Ok(tonic::Response::new(
                    batch_update_firmware_by_node_type_response(batch, jobs, 0, 0, 0),
                ));
            }
        };
        if let Err(error) = validate_firmware_request(target_type, &targets, r.activate) {
            batch.message = error.message;
            return Ok(tonic::Response::new(
                batch_update_firmware_by_node_type_response(batch, jobs, 0, 0, 0),
            ));
        }

        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(&r.rack_id, JobType::FirmwareUpdate)
        else {
            batch.message = "failed to create parent firmware update job".into();
            return Ok(tonic::Response::new(
                batch_update_firmware_by_node_type_response(batch, jobs, 0, 0, 0),
            ));
        };
        batch.job_id = parent_id.clone();

        let matching_nodes: Vec<_> = rack
            .list_nodes()
            .into_iter()
            .filter(|node| node.node_type() == target_type)
            .collect();
        let matched_nodes = matching_nodes.len() as u32;

        // Admit every matching node before touching any of them, in two steps
        // that are both load-bearing.
        //
        // Claiming a node before its NVUE setup is required because setup
        // rebuilds the registered node's shared client transport, which must
        // not happen while another job still owns the node.
        //
        // Claiming the *whole* batch before any setup is required because
        // sealing an admitted child inside the admission loop can leave the
        // parent momentarily all-terminal (every node admitted so far having
        // failed setup). The reaper's `refresh_all_parents` sweep would then
        // seal the parent, and every node still waiting to be admitted would be
        // refused with an internal "parent is terminal" error instead of being
        // updated. Admitting up front keeps a non-terminal child in the parent
        // for as long as admission is open.
        let (admitted, rejected) = self.job_tracker.create_batch_jobs(
            &parent_id,
            JobType::FirmwareUpdate,
            matching_nodes
                .into_iter()
                .map(|node| (r.rack_id.clone(), node.id().to_owned(), node))
                .collect(),
            |_| Ok(()),
        );

        let admitted_nodes = admitted.len();
        let mut failed_nodes = rejected.len() as u32;
        batch.node_results.extend(failed_node_results(rejected));

        let mut queued_jobs = Vec::with_capacity(admitted_nodes);

        // Capture every admitted child's provenance before the first setup await.
        let admitted = admitted
            .into_iter()
            .map(|(node, pending, ())| {
                let pending = self.trace_firmware_job(pending, Some(node.node_type()));
                (node, pending)
            })
            .collect::<Vec<_>>();

        for (node, pending) in admitted {
            if let Err((phase, failure)) = self.prepare_firmware_node_for_job(&pending, &node).await
            {
                tracing::error!(node = %node.id(), rack = %r.rack_id, phase, error = %failure.message, "firmware preparation failed");

                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node.id().to_owned(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: failure.message.clone(),
                });
                failed_nodes += 1;
                let child_id = JobId::from(pending.id().as_ref());
                fail_tracked_firmware_job(
                    pending,
                    phase,
                    None,
                    job_error_of(&failure.error),
                    &failure.message,
                );
                // Sealing a handle directly does not aggregate its parent, so
                // refresh it here rather than leaving the parent `Running`
                // until the next reaper sweep.
                self.job_tracker.refresh_parent_for_child(&child_id);
                continue;
            }

            jobs.push(rm::NodeFirmwareJobInfo {
                node_id: node.id().to_owned(),
                job_id: pending.id().to_string(),
            });

            queued_jobs.push((pending, node));
        }

        if queued_jobs.is_empty() {
            if matched_nodes == 0 {
                tracing::error!(rack = %r.rack_id, node_type = %target_type, "no matching nodes found");
                batch.message = "no matching nodes found".into();
            } else {
                batch.message = "no firmware update jobs created".into();
            }
            // Admitted children that failed NVUE setup already drove the parent
            // to a terminal state through child aggregation.
            if admitted_nodes == 0 {
                self.job_tracker
                    .mark_failed_message(&parent_id, &batch.message);
            }

            return Ok(tonic::Response::new(
                batch_update_firmware_by_node_type_response(
                    batch,
                    jobs,
                    matched_nodes,
                    0,
                    failed_nodes,
                ),
            ));
        }

        for (pending, node) in queued_jobs {
            self.spawn_profiled_firmware_update_job(
                pending,
                node,
                targets.clone(),
                r.activate,
                r.force_update,
                false,
                self.recovery_dispatch(RecoverySource::Latest),
            );
        }

        batch.status = if failed_nodes == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();
        batch.message = format!(
            "Created {} firmware update jobs for rack {}",
            jobs.len(),
            r.rack_id
        );
        let jobs_created = jobs.len() as u32;
        Ok(tonic::Response::new(
            batch_update_firmware_by_node_type_response(
                batch,
                jobs,
                matched_nodes,
                jobs_created,
                failed_nodes,
            ),
        ))
    }

    pub(crate) async fn handle_batch_update_firmware(
        &self,
        req: tonic::Request<rm::BatchUpdateFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchUpdateFirmwareResponse>, tonic::Status> {
        self.handle_batch_update_firmware_with_expected_versions(req, HashMap::new())
            .await
    }

    pub(crate) async fn handle_batch_update_firmware_with_expected_versions(
        &self,
        req: tonic::Request<rm::BatchUpdateFirmwareRequest>,
        expected_versions: FirmwareTargetExpectedVersions,
    ) -> std::result::Result<tonic::Response<rm::BatchUpdateFirmwareResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::new(),
            job_id: String::new(),
            stats: None,
        };
        let mut jobs: Vec<rm::NodeFirmwareJobInfo> = Vec::new();

        let nodes: Vec<rm::NodeInfo> = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        let total_nodes = nodes.len() as u32;
        if nodes.is_empty() {
            batch.message = "No nodes specified in request".into();
            return Ok(tonic::Response::new(batch_update_firmware_response(
                batch,
                jobs,
                total_nodes,
                0,
                0,
            )));
        }

        if r.firmware_targets.is_empty() && r.node_descriptor_firmware_targets.is_empty() {
            batch.message = "No firmware targets specified in request".into();
            return Ok(tonic::Response::new(batch_update_firmware_response(
                batch,
                jobs,
                total_nodes,
                0,
                0,
            )));
        }

        let target_lists = match resolve_firmware_target_selectors(
            &r.firmware_targets,
            &r.node_descriptor_firmware_targets,
        ) {
            Ok(target_lists) => target_lists,
            Err(error) => {
                batch.message = format!("Failed to resolve firmware targets: {error}");
                return Ok(tonic::Response::new(batch_update_firmware_response(
                    batch,
                    jobs,
                    total_nodes,
                    0,
                    0,
                )));
            }
        };

        // Pre-validate and resolve firmware target file paths per node type.
        let mut resolved = HashMap::new();
        for (node_type, target_list) in target_lists {
            match build_firmware_targets_from_list_with_expected_versions(
                node_type,
                &target_list.targets,
                &self.firmware_dir,
                expected_versions.get(&node_type),
            ) {
                Ok(targets) if targets.is_empty() => {
                    batch.message = format!(
                        "Failed to resolve firmware targets for node type {}: empty list",
                        node_type.as_str()
                    );
                    return Ok(tonic::Response::new(batch_update_firmware_response(
                        batch,
                        jobs,
                        total_nodes,
                        0,
                        0,
                    )));
                }
                Ok(targets) => {
                    if let Err(error) = validate_firmware_request(node_type, &targets, r.activate) {
                        batch.message = format!(
                            "Invalid firmware workflow for node type {}: {}",
                            node_type.as_str(),
                            error.message
                        );
                        return Ok(tonic::Response::new(batch_update_firmware_response(
                            batch,
                            jobs,
                            total_nodes,
                            0,
                            0,
                        )));
                    }
                    resolved.insert(node_type, targets);
                }
                Err(e) => {
                    batch.message = format!(
                        "Failed to resolve firmware targets for node type {}: {e}",
                        node_type.as_str()
                    );
                    return Ok(tonic::Response::new(batch_update_firmware_response(
                        batch,
                        jobs,
                        total_nodes,
                        0,
                        0,
                    )));
                }
            }
        }

        let parent_rack_id = nodes[0].rack_id.clone();
        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(&parent_rack_id, JobType::FirmwareUpdate)
        else {
            batch.message = "failed to create parent firmware update job".into();
            return Ok(tonic::Response::new(batch_update_firmware_response(
                batch,
                jobs,
                total_nodes,
                0,
                total_nodes,
            )));
        };
        batch.job_id = parent_id.clone();

        let activate = r.activate;
        let force_update = r.force_update;
        let mut skipped_nodes = 0u32;
        let mut queued_jobs = Vec::new();

        for node_info in nodes {
            let node_type = match resolve_node_info(&node_info) {
                Ok(node_type) => node_type,
                Err(error) => {
                    tracing::error!(node = %node_info.node_id, "unknown node type");
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_info.node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: error.to_string(),
                    });
                    skipped_nodes += 1;
                    continue;
                }
            };

            let targets = match resolved.get(&node_type) {
                Some(t) => t.clone(),
                None => {
                    tracing::warn!(
                        node = %node_info.node_id,
                        node_type = node_type.as_str(),
                        "no firmware targets for this node type"
                    );
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_info.node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: format!(
                            "No firmware targets provided for node type {}",
                            node_type.as_str()
                        ),
                    });
                    skipped_nodes += 1;
                    continue;
                }
            };

            // Switches authenticate to NVUE/NVOS via their host endpoint;
            // compute/powershelf authenticate to the BMC.
            let flat = match flatten_node_info(&node_info) {
                Ok(flat) => flat,
                Err(e) => {
                    let message = e.message;
                    tracing::error!(
                        node = node_info.node_id,
                        node_type = node_type.as_str(),
                        error = message,
                        "invalid endpoint credentials"
                    );

                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_info.node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: message,
                    });

                    skipped_nodes += 1;
                    continue;
                }
            };

            if !flat.has_credentials_for_node_type(node_type) {
                let source = if node_type.kind() == NodeKind::Switch {
                    "host"
                } else {
                    "BMC"
                };
                tracing::error!(
                    node = %node_info.node_id,
                    node_type = node_type.as_str(),
                    source,
                    "missing credentials"
                );
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_info.node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: format!(
                        "Missing {source} credentials for node {}",
                        node_info.node_id
                    ),
                });
                skipped_nodes += 1;
                continue;
            }

            let node_id = node_info.node_id.clone();
            let rack_id = node_info.rack_id.clone();
            let expected_inventory = match self.expected_inventory_policy_for_node(&node_info) {
                Ok(policy) => policy,
                Err(e) => {
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: e.message,
                    });
                    skipped_nodes += 1;
                    continue;
                }
            };
            let recovery = match self.recovery_profile_for_node(&node_info) {
                Ok(profile) => profile,
                Err(e) => {
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: e.message,
                    });
                    skipped_nodes += 1;
                    continue;
                }
            };
            let built = build_ephemeral_node_with_profiles(
                &node_info,
                node_type,
                expected_inventory,
                recovery,
                &self.hardware_profiles,
                self.insecure_bmc,
            );
            let node = match built {
                Ok(n) => n,
                Err(e) => {
                    let message = e.message;
                    tracing::error!(
                        node = node_id,
                        error = message,
                        "failed to construct ephemeral node"
                    );

                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: format!(
                            "Failed to construct node for type {}: {message}",
                            node_type.as_str()
                        ),
                    });

                    skipped_nodes += 1;
                    continue;
                }
            };

            // Setup runs before admission here, the opposite of the registered
            // paths, and that is safe only because `node` is ephemeral: it was
            // just built from this request, so its NVUE client is private to
            // this call and rebuilding its transport cannot disturb a job that
            // owns the node. Do not copy this ordering to a registered node
            // obtained from the rack; there the client is shared.
            if let Err(e) = self.initialize_nvue_client(node.nvue_client(), None).await {
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: e.message,
                });

                skipped_nodes += 1;
                continue;
            }

            let pending = match self.job_tracker.create_child_job(
                &parent_id,
                &rack_id,
                &node_id,
                JobType::FirmwareUpdate,
            ) {
                Ok(pending) => pending,
                Err(failure) => {
                    tracing::warn!(
                        node = node_id,
                        rack = rack_id,
                        message = %failure.message,
                        "firmware job rejected"
                    );

                    batch.node_results.push(rm::NodeOperationResult {
                        node_id,
                        status: rm::ReturnCode::Failure.into(),
                        error_message: failure.message,
                    });

                    skipped_nodes += 1;
                    continue;
                }
            };

            let pending = self.trace_firmware_job(pending, Some(node.node_type()));

            if let Err(failure) = self.admit_firmware_node_for_job(&pending, &node) {
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: failure.message.clone(),
                });

                skipped_nodes += 1;
                let child_id = JobId::from(pending.id().as_ref());

                fail_tracked_firmware_job(
                    pending,
                    "scope_admission",
                    None,
                    job_error_of(&failure.error),
                    &failure.message,
                );

                self.job_tracker.refresh_parent_for_child(&child_id);
                continue;
            }

            let job_id = pending.id().to_string();

            jobs.push(rm::NodeFirmwareJobInfo {
                node_id: node_id.clone(),
                job_id: job_id.clone(),
            });
            queued_jobs.push((pending, node, targets));
        }

        let jobs_created = jobs.len() as u32;
        if jobs_created == 0 {
            batch.status = rm::ReturnCode::Failure.into();
            batch.message = "No firmware update jobs created".into();
            self.job_tracker
                .mark_failed_message(&parent_id, &batch.message);
            return Ok(tonic::Response::new(batch_update_firmware_response(
                batch,
                jobs,
                total_nodes,
                0,
                skipped_nodes,
            )));
        }

        for (pending, node, targets) in queued_jobs {
            self.spawn_profiled_firmware_update_job(
                pending,
                node,
                targets,
                activate,
                force_update,
                false,
                self.recovery_dispatch(RecoverySource::Latest),
            );
        }

        batch.status = if skipped_nodes == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();
        batch.message = format!(
            "Created {jobs_created} firmware update jobs out of {total_nodes} nodes. \
             Use GetFirmwareJobStatus with job_id to track overall batch status."
        );
        Ok(tonic::Response::new(batch_update_firmware_response(
            batch,
            jobs,
            total_nodes,
            jobs_created,
            skipped_nodes,
        )))
    }

    pub(crate) async fn handle_get_rack_firmware_inventory(
        &self,
        req: tonic::Request<rm::GetRackFirmwareInventoryRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetRackFirmwareInventoryResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut resp = rm::GetRackFirmwareInventoryResponse {
            status: rm::ReturnCode::Failure.into(),
            nodes: Vec::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                return Ok(tonic::Response::new(resp));
            }
        };

        let mut failed_nodes = 0usize;

        for node in rack.list_nodes() {
            if node.node_type() != NodeType::SwitchVrnvl72Nvidia
                && let Err(error) = self.initialize_nvue_client(node.nvue_client(), None).await
            {
                tracing::error!(node = %node.id(), rack = %r.rack_id, error = %error.message, "failed to configure NVUE client");
                failed_nodes += 1;
                continue;
            }
            let inventory = match node.get_firmware_inventory().await {
                Ok(inv) => inv,
                Err(e) => {
                    tracing::error!(node = %node.id(), rack = %r.rack_id, error = %e.message, "failed to get firmware inventory for node");
                    failed_nodes += 1;
                    continue;
                }
            };
            let nfi = rm::NodeFirmwareInventory {
                node_id: node.id().to_owned(),
                firmware_list: inventory
                    .into_iter()
                    .map(firmware_inventory_info_to_proto)
                    .collect(),
            };
            resp.nodes.push(nfi);
        }

        if failed_nodes == 0 {
            resp.status = rm::ReturnCode::Success.into();
        } else {
            resp.status = rm::ReturnCode::Failure.into();
        }

        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_get_firmware_job_status(
        &self,
        req: tonic::Request<rm::GetFirmwareJobStatusRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetFirmwareJobStatusResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::GetFirmwareJobStatusResponse {
            status: rm::ReturnCode::Failure.into(),
            ..Default::default()
        };

        match self.job_tracker.get_job(&r.job_id) {
            Some(job) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.job_id = job.job_id;
                resp.job_state = rm::FirmwareJobState::from(job.state).into();
                resp.state_description = job.state_description;
                resp.rack_id = job.rack_id;
                resp.node_id = job.node_id;
                resp.error_code = rm::FirmwareUpdateError::from(job.error_code).into();
                resp.error_message = job.error_message;
                resp.result_json = job.result_json;
                resp.created_at = Some(timestamp_from_datetime(job.created_at));
                resp.updated_at = Some(timestamp_from_datetime(job.updated_at));
            }
            None => {
                tracing::error!(job_id = %r.job_id, "job not found");
                resp.error_message = format!("job {} not found", r.job_id);
            }
        }
        Ok(tonic::Response::new(resp))
    }
}

fn batch_update_firmware_by_node_type_response(
    mut batch: rm::NodeBatchResponse,
    jobs: Vec<rm::NodeFirmwareJobInfo>,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) -> rm::BatchUpdateFirmwareByNodeTypeResponse {
    batch.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });

    rm::BatchUpdateFirmwareByNodeTypeResponse {
        response: Some(batch),
        jobs,
    }
}

fn batch_update_firmware_response(
    mut batch: rm::NodeBatchResponse,
    jobs: Vec<rm::NodeFirmwareJobInfo>,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) -> rm::BatchUpdateFirmwareResponse {
    batch.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });

    rm::BatchUpdateFirmwareResponse {
        response: Some(batch),
        jobs,
    }
}

#[cfg(test)]
mod tests {
    mod hardware_profile_startup;
    mod hardware_profiles;
    use super::*;
    use crate::domain::node::{
        FirmwareActivationSummary, FirmwareInfo, FirmwareTaskRejection, FirmwareTaskStatus,
        NodeKind, NodeType, PowerState, ProductFamily,
    };
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, NodeConfig};
    use async_trait::async_trait;
    use std::collections::{HashMap, VecDeque};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::super::server::SwitchTlsRoots;
    use crate::libnmxc::TlsMaterialStore;
    use crate::orchestrator::job_tracker::JobTracker;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use crate::racks::ManagedRack;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    pub(super) fn poll_options(timeout: Duration, interval: Duration) -> FirmwareTaskPollOptions {
        FirmwareTaskPollOptions {
            timeout,
            interval,
            ..FirmwareTaskPollOptions::default()
        }
    }

    /// Production bounds with a transient-error window short enough to run in a
    /// test. The retry sleep is clamped to the remaining window, so this also
    /// keeps the poll interval from being waited out.
    fn task_loss_poll_options() -> FirmwareTaskPollOptions {
        FirmwareTaskPollOptions {
            transient_error_timeout: Duration::from_millis(10),
            ..FirmwareTaskPollOptions::default()
        }
    }

    struct CountingPollNode {
        complete_after: usize,
        calls: AtomicUsize,
        always_fail: bool,
    }

    struct TransientPollErrorNode {
        transient_failures: usize,
        calls: AtomicUsize,
    }

    struct SupermicroSequenceNode {
        events: Arc<Mutex<Vec<String>>>,
    }

    /// VR LiteOn PMC whose task vanishes across the PMC restart. Inventory reads
    /// replay `inventory` in order and then repeat the last entry.
    struct LiteonPmcTaskLossNode {
        node_type: NodeType,
        inventory: Mutex<VecDeque<Result<Vec<FirmwareInfo>>>>,
        poll_calls: AtomicUsize,
        inventory_calls: AtomicUsize,
        /// Time every inventory read takes, like a PMC that is still rebooting.
        read_delay: Duration,
    }

    impl LiteonPmcTaskLossNode {
        fn new(inventory: Vec<Result<Vec<FirmwareInfo>>>) -> Self {
            Self {
                node_type: NodeType::PowershelfVrnvl72Liteon,
                inventory: Mutex::new(inventory.into()),
                poll_calls: AtomicUsize::new(0),
                inventory_calls: AtomicUsize::new(0),
                read_delay: Duration::ZERO,
            }
        }

        fn with_read_delay(mut self, read_delay: Duration) -> Self {
            self.read_delay = read_delay;
            self
        }
    }

    struct SupermicroTaskLossNode {
        version_summary: FirmwareVersionCheckSummary,
        poll_calls: AtomicUsize,
        version_check_calls: AtomicUsize,
    }

    #[async_trait]
    impl Node for CountingPollNode {
        fn id(&self) -> &str {
            "stub-node"
        }
        fn rack_id(&self) -> &str {
            "stub-rack"
        }
        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb200Nvidia
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
            if self.always_fail {
                return Err(RmsError::internal("simulated poll failure"));
            }
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n >= self.complete_after {
                Ok(FirmwareTaskStatus {
                    completed: true,
                    ..Default::default()
                })
            } else {
                Ok(FirmwareTaskStatus::default())
            }
        }
    }

    #[async_trait]
    impl Node for TransientPollErrorNode {
        fn id(&self) -> &str {
            "transient-poll-node"
        }
        fn rack_id(&self) -> &str {
            "transient-poll-rack"
        }
        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb200Nvidia
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.transient_failures {
                Err(RmsError::internal(
                    "NVFWUPD task failed for 1: failed to retrieve task status",
                ))
            } else {
                Ok(FirmwareTaskStatus {
                    completed: true,
                    percent: 100,
                    state: "Completed".to_owned(),
                    status: "OK".to_owned(),
                    ..Default::default()
                })
            }
        }
    }

    #[async_trait]
    impl Node for SupermicroSequenceNode {
        fn id(&self) -> &str {
            "supermicro-node"
        }

        fn rack_id(&self) -> &str {
            "rack-01"
        }

        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb300Supermicro
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        async fn update_firmware(
            &self,
            target: &FirmwareTarget,
            _force_update: bool,
            _options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            self.events
                .lock()
                .unwrap()
                .push(format!("update:{}", target.component));
            Ok(completed_update_outcome())
        }

        async fn activate_firmware_with(
            &self,
            _request: FirmwareActivationRequest,
        ) -> Result<FirmwareActivationSummary> {
            self.events.lock().unwrap().push("activate".to_owned());
            Ok(FirmwareActivationSummary {
                message: "activated".to_owned(),
                details: serde_json::json!({}),
            })
        }

        async fn verify_firmware_package_versions(
            &self,
            targets: &[FirmwareTarget],
        ) -> Result<FirmwareVersionCheckSummary> {
            self.events.lock().unwrap().push(format!(
                "verify:{}",
                targets
                    .iter()
                    .map(|target| target.component.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            Ok(FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({"matched": true}),
            })
        }
    }

    #[async_trait]
    impl Node for SupermicroTaskLossNode {
        fn id(&self) -> &str {
            "supermicro-task-loss-node"
        }

        fn rack_id(&self) -> &str {
            "rack-01"
        }

        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb300Supermicro
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        async fn update_firmware(
            &self,
            target: &FirmwareTarget,
            _force_update: bool,
            _options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            assert_eq!(target.expected_version.as_deref(), Some("70.02.01.05"));
            Ok(FirmwareUpdateOutcome::Started(
                crate::domain::node::FirmwareTaskHandle {
                    task_id: "0".to_owned(),
                },
            ))
        }

        async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
            assert_eq!(task_id, "0");
            self.poll_calls.fetch_add(1, Ordering::SeqCst);
            Err(RmsError::internal(
                "NVFWUPD task failed for 0: failed to retrieve task status",
            ))
        }

        async fn verify_firmware_package_versions(
            &self,
            targets: &[FirmwareTarget],
        ) -> Result<FirmwareVersionCheckSummary> {
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0].expected_version.as_deref(), Some("70.02.01.05"));
            self.version_check_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.version_summary.clone())
        }
    }

    #[async_trait]
    impl Node for LiteonPmcTaskLossNode {
        fn id(&self) -> &str {
            "liteon-pmc-task-loss-node"
        }

        fn rack_id(&self) -> &str {
            "rack-01"
        }

        fn node_type(&self) -> NodeType {
            self.node_type
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        async fn update_firmware(
            &self,
            _target: &FirmwareTarget,
            _force_update: bool,
            _options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            Ok(FirmwareUpdateOutcome::Started(
                crate::domain::node::FirmwareTaskHandle {
                    task_id: "0".to_owned(),
                },
            ))
        }

        async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
            assert_eq!(task_id, "0");
            self.poll_calls.fetch_add(1, Ordering::SeqCst);
            Err(RmsError::internal(
                "NVFWUPD task failed for 0: failed to retrieve task status",
            ))
        }

        async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
            self.inventory_calls.fetch_add(1, Ordering::SeqCst);

            if !self.read_delay.is_zero() {
                tokio::time::sleep(self.read_delay).await;
            }

            let mut inventory = self.inventory.lock().unwrap();
            if inventory.len() > 1 {
                inventory.pop_front().unwrap()
            } else {
                match inventory.front().unwrap() {
                    Ok(inventory) => Ok(inventory.clone()),
                    Err(error) => Err(RmsError::unavailable(error.message.clone())),
                }
            }
        }
    }

    struct OutcomeNode {
        node_type: NodeType,
        outcome: Mutex<Option<FirmwareUpdateOutcome>>,
        update_options: Mutex<Vec<FirmwareUpdateOptions>>,
        poll_status: FirmwareTaskStatus,
        poll_calls: AtomicUsize,
    }

    struct ActivationNode {
        node_type: NodeType,
        calls: AtomicUsize,
    }

    struct InbandJobNode {
        node_type: NodeType,
        outcome: Mutex<Option<Result<FirmwareUpdateOutcome>>>,
        events: Arc<Mutex<Vec<String>>>,
        cancel_on_activation: bool,
        group_calls: AtomicUsize,
        activation_calls: AtomicUsize,
        verification_calls: AtomicUsize,
    }

    struct VersionCheckNode {
        node_type: NodeType,
        summary: FirmwareVersionCheckSummary,
        transient_failures: usize,
        calls: AtomicUsize,
        /// Readiness probes that answer `not_ready_error` before the first
        /// ready one.
        not_ready_probes: usize,
        not_ready_error: fn() -> RmsError,
        readiness_calls: AtomicUsize,
        /// Reachability probes that fail before the first answer, as NVUE
        /// does while NVOS boots, with `unreachable_error` as the answer.
        unreachable_probes: usize,
        unreachable_error: fn() -> RmsError,
        reachability_calls: AtomicUsize,
    }

    struct StaticPollNode {
        status: FirmwareTaskStatus,
    }

    struct SequencePollNode {
        statuses: Mutex<VecDeque<FirmwareTaskStatus>>,
        calls: AtomicUsize,
        fail_refetch: bool,
    }

    struct InventoryNode {
        id: &'static str,
        node_type: NodeType,
        result: Mutex<Option<Result<Vec<FirmwareInfo>>>>,
    }

    #[async_trait]
    impl Node for InventoryNode {
        fn id(&self) -> &str {
            self.id
        }

        fn rack_id(&self) -> &str {
            "rack-01"
        }

        fn node_type(&self) -> NodeType {
            self.node_type
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
            self.result
                .lock()
                .unwrap()
                .take()
                .expect("get_firmware_inventory called more times than expected")
        }
    }

    fn test_service(rack_manager: Arc<RackManager>) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager,
            job_tracker: Arc::new(JobTracker::new()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            artifact_require_sha256: false,
            insecure_bmc: true,
            artifact_download_policy:
                crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
            switch_tls_roots: SwitchTlsRoots::default(),
            sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
            workflow_timings: crate::config::WorkflowTimings::default(),
            recovery_profiles: crate::config::RecoveryProfiles::default(),
            fabric_layout_catalog: std::sync::Arc::new(
                crate::config::fabric_layout::FabricLayoutCatalog::default(),
            ),
            hardware_profiles: Arc::new(
                crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
            ),
            fabric_operations: std::sync::Arc::new(Default::default()),
        }
    }

    fn rack_manager_with_rack() -> TestResult<(Arc<RackManager>, Arc<ManagedRack>)> {
        let rack_manager = Arc::new(RackManager::new());
        rack_manager.create_rack("rack-01", ProductFamily::Gb200.rack_type())?;
        let Some(rack) = rack_manager.find_rack("rack-01") else {
            return Err("rack not found".into());
        };

        Ok((rack_manager, rack))
    }

    fn inventory_node(id: &'static str, result: Result<Vec<FirmwareInfo>>) -> NodeInstance {
        NodeInstance::from_test_node(InventoryNode {
            id,
            node_type: NodeType::ComputeGb200Nvidia,
            result: Mutex::new(Some(result)),
        })
    }

    fn switch_node_with_nvue_client() -> Result<NodeInstance> {
        NodeInstance::from_config(
            &NodeConfig {
                id: "switch-01".to_owned(),
                node_type: NodeType::SwitchGb200Nvidia,
                bmc_endpoint: None,
                host_endpoint: Some(EndpointConfig::with_credentials(
                    Endpoint {
                        ip_address: "10.0.0.11".to_owned(),
                        mac_address: String::new(),
                        port: 443,
                        host_name: Some("switch.example.com".to_owned()),
                    },
                    Some(EndpointCredentials::new("admin", "password")),
                    false,
                )),
                expected_inventory: None,
                recovery: None,
                runtime_config: None,
            },
            "rack-01",
        )
    }

    #[tokio::test]
    async fn batch_update_rejects_unresolved_inventory_profiles_before_creating_jobs() {
        let firmware_dir = tempfile::tempdir().unwrap();
        std::fs::write(firmware_dir.path().join("bmc.fwpkg"), b"test").unwrap();
        let mut service = test_service(Arc::new(RackManager::new()));
        service.firmware_dir = firmware_dir.path().to_owned();

        let endpoint = rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: "192.0.2.10".to_owned(),
                mac_address: "00:11:22:33:44:55".to_owned(),
                host_name: None,
            }),
            port: 443,
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "admin".to_owned(),
                    password: "password".to_owned(),
                })),
            }),
        };
        let unknown_node = rm::NodeInfo {
            node_id: "compute-1".to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            node_descriptor: Some(rm::NodeDescriptor {
                attributes: HashMap::from([(
                    crate::api::grpc::node_type_resolver::INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                    "unknown-profile".to_owned(),
                )]),
            }),
            bmc_endpoint: Some(endpoint.clone()),
            ..Default::default()
        };
        let empty_node = rm::NodeInfo {
            node_id: "compute-2".to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            node_descriptor: Some(rm::NodeDescriptor {
                attributes: HashMap::from([(
                    crate::api::grpc::node_type_resolver::INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                    "  ".to_owned(),
                )]),
            }),
            bmc_endpoint: Some(endpoint),
            ..Default::default()
        };

        let response = service
            .handle_batch_update_firmware(tonic::Request::new(rm::BatchUpdateFirmwareRequest {
                nodes: Some(rm::NodeSet {
                    nodes: vec![unknown_node, empty_node],
                }),
                firmware_targets: HashMap::from([(
                    rm::NodeType::ComputeGb200Nvidia as i32,
                    rm::FirmwareTargetList {
                        targets: vec![rm::FirmwareTarget {
                            target: "BMC".to_owned(),
                            filename: "bmc.fwpkg".to_owned(),
                        }],
                    },
                )]),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.jobs.is_empty());
        let batch = response.response.unwrap();
        assert!(!batch.job_id.is_empty());
        assert_eq!(batch.node_results.len(), 2);
        assert!(
            batch.node_results[0]
                .error_message
                .contains("unknown inventory_profile")
        );
        assert!(
            batch.node_results[1]
                .error_message
                .contains("inventory_profile must not be empty")
        );
        assert_eq!(batch.stats.unwrap().failed_nodes, 2);
        assert_eq!(
            service.job_tracker.get_job(&batch.job_id).unwrap().state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
    }

    #[tokio::test]
    async fn update_firmware_reports_failure_when_nvue_tls_initialization_fails() -> TestResult {
        let client_root = tempfile::tempdir()?;
        let (rack_manager, rack) = rack_manager_with_rack()?;

        rack.add_node("switch-01", Arc::new(switch_node_with_nvue_client()?))?;

        let mut service = test_service(rack_manager);

        service.switch_tls_roots = SwitchTlsRoots {
            client_tls: Some(TlsMaterialStore::new(client_root.path())),
            ..SwitchTlsRoots::default()
        };

        let response = service
            .handle_update_firmware(tonic::Request::new(rm::UpdateFirmwareRequest {
                rack_id: "rack-01".to_owned(),
                node_id: "switch-01".to_owned(),
                ..Default::default()
            }))
            .await?
            .into_inner();

        assert_eq!(response.status, rm::ReturnCode::Failure as i32);

        assert_eq!(
            response.error_code,
            rm::FirmwareUpdateError::ClientFailure as i32
        );

        assert!(response.message.contains("default_switch_domain"));

        Ok(())
    }

    /// The node is claimed before NVUE setup, so a setup failure has to seal the
    /// claimed job with the real reason. Letting the handle drop instead seals it
    /// as "abandoned" with an `internal` error code, which hides a caller-caused
    /// failure behind what looks like an RMS bug.
    #[tokio::test]
    async fn update_firmware_seals_the_claimed_job_when_nvue_setup_fails() -> TestResult {
        let client_root = tempfile::tempdir()?;
        let (rack_manager, rack) = rack_manager_with_rack()?;
        rack.add_node("switch-01", Arc::new(switch_node_with_nvue_client()?))?;

        let registry = prometheus::Registry::new_custom(Some("rms".to_owned()), None)?;
        let mut service = test_service(rack_manager);
        service.job_tracker = Arc::new(JobTracker::builder().metrics(&registry).build()?);
        service.switch_tls_roots = SwitchTlsRoots {
            client_tls: Some(TlsMaterialStore::new(client_root.path())),
            ..SwitchTlsRoots::default()
        };

        let response = service
            .handle_update_firmware(tonic::Request::new(rm::UpdateFirmwareRequest {
                rack_id: "rack-01".to_owned(),
                node_id: "switch-01".to_owned(),
                ..Default::default()
            }))
            .await?
            .into_inner();

        assert_eq!(response.status, rm::ReturnCode::Failure as i32);

        let body = prometheus::TextEncoder::new().encode_to_string(&registry.gather())?;
        let failed_samples: Vec<&str> = body
            .lines()
            .filter(|line| {
                line.starts_with("rms_workflows_counts_total{")
                    && line.contains(r#"workflow_state="failed""#)
            })
            .collect();

        assert_eq!(
            failed_samples.len(),
            1,
            "expected exactly one failed workflow sample, got: {failed_samples:?}"
        );
        assert!(
            !failed_samples[0].contains(r#"error_code="internal""#),
            "the job was abandoned instead of sealed with the setup failure: {}",
            failed_samples[0]
        );

        // The node must not be left occupied by the failed attempt.
        assert!(
            service
                .job_tracker
                .create_visible_workflow_job_if_node_idle(
                    "rack-01",
                    "switch-01",
                    JobType::FirmwareUpdate,
                )
                .is_ok(),
            "a sealed job must release the node"
        );

        Ok(())
    }

    #[tokio::test]
    async fn batch_update_firmware_by_node_type_preserves_descriptor_selection() -> TestResult {
        use crate::api::grpc::conversions::domain_node_type_to_proto;
        use crate::api::grpc::node_type_resolver::domain_node_type_to_descriptor;

        use librms::protos::rack_manager::rack_manager_server::RackManager as RackManagerRpc;

        let firmware_dir = tempfile::tempdir()?;
        std::fs::write(firmware_dir.path().join("hmc.fwpkg"), b"firmware")?;

        for (node_type, other_type) in [
            (NodeType::ComputeGb200Nvidia, NodeType::ComputeGb200Wiwynn),
            (NodeType::ComputeGb200Wiwynn, NodeType::ComputeGb200Dell),
            (NodeType::ComputeGb200Dell, NodeType::ComputeGb200Wiwynn),
            (
                NodeType::ComputeGb300Supermicro,
                NodeType::ComputeGb300Nvidia,
            ),
            (
                NodeType::PowershelfVrnvl72Liteon,
                NodeType::PowershelfVrnvl72Delta,
            ),
            (
                NodeType::PowershelfVrnvl72Delta,
                NodeType::PowershelfVrnvl72Liteon,
            ),
        ] {
            let rack_manager = Arc::new(RackManager::new());
            rack_manager.create_rack("rack-01", node_type.product_family().rack_type())?;

            let rack = rack_manager.find_rack("rack-01").ok_or("rack not found")?;

            for (id, node_type) in [("selected-node", node_type), ("other-node", other_type)] {
                rack.add_node(
                    id,
                    Arc::new(NodeInstance::from_test_node(InventoryNode {
                        id,
                        node_type,
                        result: Mutex::new(None),
                    })),
                )?;
            }

            let mut service = test_service(rack_manager);
            service.firmware_dir = firmware_dir.path().to_owned();

            // Keep both nodes busy to test type selection without preparing devices.
            let busy = service
                .job_tracker
                .create_job("rack-01", "selected-node", JobType::FirmwareUpdate)
                .expect("occupy selected node");

            let _other_busy = service
                .job_tracker
                .create_job("rack-01", "other-node", JobType::FirmwareUpdate)
                .expect("occupy other node");

            let proto_type = domain_node_type_to_proto(node_type);

            let descriptor_type = if proto_type == rm::NodeType::Unspecified {
                node_type
            } else {
                other_type
            };

            for multi_target in [false, true] {
                let request = rm::BatchUpdateFirmwareByNodeTypeRequest {
                    rack_id: "rack-01".into(),
                    node_type: proto_type as i32,
                    node_descriptor: Some(domain_node_type_to_descriptor(descriptor_type)),
                    filename: "hmc.fwpkg".into(),
                    target: "HMC".into(),
                    firmware_targets: if multi_target {
                        vec![rm::FirmwareTarget {
                            filename: "hmc.fwpkg".into(),
                            target: "HMC".into(),
                        }]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                };

                let response = RackManagerRpc::batch_update_firmware_by_node_type(
                    &service,
                    tonic::Request::new(request),
                )
                .await?
                .into_inner();

                let batch = response.response.ok_or("missing batch response")?;

                assert_eq!(
                    batch.node_results.len(),
                    1,
                    "{node_type}, multi_target={multi_target}: {batch:?}"
                );

                assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
                assert!(!batch.job_id.is_empty());
                assert_eq!(batch.node_results[0].node_id, "selected-node");
                assert_eq!(batch.node_results[0].status, rm::ReturnCode::Failure as i32);

                assert!(
                    batch.node_results[0]
                        .error_message
                        .contains(busy.id().as_ref())
                );

                assert!(response.jobs.is_empty());

                let stats = batch.stats.ok_or("missing batch stats")?;

                assert_eq!(stats.total_nodes, 1);
                assert_eq!(stats.failed_nodes, 1);
                assert_eq!(stats.successful_nodes, 0);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn batch_update_firmware_by_node_type_refuses_busy_node_before_nvue_setup() -> TestResult
    {
        let client_root = tempfile::tempdir()?;
        let firmware_dir = tempfile::tempdir()?;
        std::fs::write(firmware_dir.path().join("switch.fwpkg"), b"test")?;

        let (rack_manager, rack) = rack_manager_with_rack()?;
        rack.add_node("switch-01", Arc::new(switch_node_with_nvue_client()?))?;

        let mut service = test_service(rack_manager);
        service.firmware_dir = firmware_dir.path().to_owned();

        // An empty client TLS store makes NVUE setup fail, so a TLS error in the
        // node result would mean the switch was reconfigured before admission.
        service.switch_tls_roots = SwitchTlsRoots {
            client_tls: Some(TlsMaterialStore::new(client_root.path())),
            ..SwitchTlsRoots::default()
        };

        let busy = service
            .job_tracker
            .create_job("rack-01", "switch-01", JobType::FirmwareUpdate)
            .expect("occupy the switch");

        let response = service
            .handle_batch_update_firmware_by_node_type(tonic::Request::new(
                rm::BatchUpdateFirmwareByNodeTypeRequest {
                    rack_id: "rack-01".to_owned(),
                    node_type: rm::NodeType::SwitchGb200Nvidia as i32,
                    filename: "switch.fwpkg".to_owned(),
                    ..Default::default()
                },
            ))
            .await?
            .into_inner();

        assert!(response.jobs.is_empty());
        let batch = response.response.ok_or("missing batch response")?;
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].node_id, "switch-01");
        assert!(
            batch.node_results[0]
                .error_message
                .contains(busy.id().as_ref()),
            "expected the node-busy refusal naming the active job, got: {}",
            batch.node_results[0].error_message
        );
        assert_eq!(
            batch.stats.ok_or("missing batch stats")?.failed_nodes,
            1,
            "the refused switch must be counted as failed"
        );

        Ok(())
    }

    /// A repeated `(rack_id, node_id)` in one batch must produce exactly one
    /// job. Node-idle admission is what deduplicates here: the first occurrence
    /// claims the node, so the second is refused as busy. Without this, a node
    /// named twice would run two concurrent firmware updates against itself.
    #[tokio::test]
    async fn batch_update_firmware_creates_one_job_for_a_node_named_twice() {
        let firmware_dir = tempfile::tempdir().unwrap();
        std::fs::write(firmware_dir.path().join("bmc.fwpkg"), b"test").unwrap();
        let mut service = test_service(Arc::new(RackManager::new()));
        service.firmware_dir = firmware_dir.path().to_owned();

        let node = rm::NodeInfo {
            node_id: "compute-1".to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            bmc_endpoint: Some(rm::Endpoint {
                interface: Some(rm::NetworkInterface {
                    ip_address: "192.0.2.10".to_owned(),
                    mac_address: "00:11:22:33:44:55".to_owned(),
                    host_name: None,
                }),
                port: 443,
                credentials: Some(rm::Credentials {
                    auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                        username: "admin".to_owned(),
                        password: "password".to_owned(),
                    })),
                }),
            }),
            ..Default::default()
        };

        let response = service
            .handle_batch_update_firmware(tonic::Request::new(rm::BatchUpdateFirmwareRequest {
                nodes: Some(rm::NodeSet {
                    nodes: vec![node.clone(), node],
                }),
                firmware_targets: HashMap::from([(
                    rm::NodeType::ComputeGb200Nvidia as i32,
                    rm::FirmwareTargetList {
                        targets: vec![rm::FirmwareTarget {
                            target: "BMC".to_owned(),
                            filename: "bmc.fwpkg".to_owned(),
                        }],
                    },
                )]),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.jobs.len(), 1);
        let batch = response.response.unwrap();
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].node_id, "compute-1");
        assert!(
            batch.node_results[0]
                .error_message
                .contains("job already in progress"),
            "expected a node-busy refusal for the repeated target, got: {}",
            batch.node_results[0].error_message
        );
        let stats = batch.stats.unwrap();
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.successful_nodes, 1);
        assert_eq!(stats.failed_nodes, 1);

        let parent = service
            .job_tracker
            .get_job(&batch.job_id)
            .expect("the batch parent job should be tracked");
        assert_eq!(
            parent.child_job_ids.len(),
            1,
            "the repeated target must not reserve a second child"
        );
    }

    #[tokio::test]
    async fn rack_firmware_inventory_reports_failure_when_a_node_fails() -> TestResult {
        let (rack_manager, rack) = rack_manager_with_rack()?;
        rack.add_node(
            "ok",
            Arc::new(inventory_node(
                "ok",
                Ok(vec![FirmwareInfo {
                    name: "bmc".to_owned(),
                    version: "1.0.0".to_owned(),
                    firmware_type: FirmwareType::BMC,
                    updateable: true,
                    target: "bmc".to_owned(),
                    health: "OK".to_owned(),
                    state: "Enabled".to_owned(),
                    sku: String::new(),
                }]),
            )),
        )?;
        rack.add_node(
            "bad",
            Arc::new(inventory_node("bad", Err(RmsError::internal("boom")))),
        )?;
        let service = test_service(rack_manager);

        let resp = service
            .handle_get_rack_firmware_inventory(tonic::Request::new(
                rm::GetRackFirmwareInventoryRequest {
                    rack_id: "rack-01".to_owned(),
                },
            ))
            .await?
            .into_inner();

        assert_eq!(resp.status, rm::ReturnCode::Failure as i32);
        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].node_id, "ok");
        Ok(())
    }

    #[tokio::test]
    async fn firmware_job_status_bridges_switch_image_jobs() -> TestResult {
        let service = test_service(Arc::new(RackManager::new()));
        let job_handle = service
            .job_tracker
            .create_job("rack-01", "switch-01", JobType::SwitchSystemImageUpdate)
            .expect("create switch image job");
        let job_id = job_handle.id().to_string();

        job_handle.fail(crate::orchestrator::job_lifecycle::JobFailure::new(
            JobError::Other,
            "switch image update failed",
        ));

        let response = service
            .handle_get_firmware_job_status(tonic::Request::new(rm::GetFirmwareJobStatusRequest {
                job_id: job_id.clone(),
            }))
            .await?
            .into_inner();

        assert_eq!(response.status, rm::ReturnCode::Success as i32);
        assert_eq!(response.job_id, job_id);
        assert_eq!(response.job_state, rm::FirmwareJobState::Failed as i32);

        assert_eq!(
            response.error_code,
            rm::FirmwareUpdateError::TaskFailed as i32
        );

        assert_eq!(response.error_message, "switch image update failed");

        Ok(())
    }

    impl OutcomeNode {
        fn new(outcome: FirmwareUpdateOutcome) -> Self {
            Self::new_for_node_type(NodeType::ComputeGb200Nvidia, outcome)
        }

        fn new_for_node_type(node_type: NodeType, outcome: FirmwareUpdateOutcome) -> Self {
            Self {
                node_type,
                outcome: Mutex::new(Some(outcome)),
                update_options: Mutex::new(Vec::new()),
                poll_status: FirmwareTaskStatus {
                    completed: true,
                    percent: 100,
                    state: "Completed".to_owned(),
                    status: "OK".to_owned(),
                    message: "done".to_owned(),
                    rejection: None,
                },
                poll_calls: AtomicUsize::new(0),
            }
        }
    }

    #[test]
    fn expected_inventory_mismatch_uses_failed_precondition_job_classification() {
        let error = RmsError::failed_precondition(
            "expected firmware inventory APs were not present: HGX_FW_GPU_4. \
             Present APs: FW_BMC_0, HGX_FW_GPU_0",
        );
        let job_error = firmware_update_job_error(&error);

        assert_eq!(job_error, JobError::FailedPrecondition);

        let (_, result_json) = firmware_failure_message_and_result_json(
            "update",
            Some(&firmware_target()),
            job_error,
            &error.message,
        );
        let result: serde_json::Value = serde_json::from_str(&result_json).unwrap();
        assert_eq!(result["error_code"], "FailedPrecondition");
        assert!(
            result["error_message"]
                .as_str()
                .unwrap()
                .contains("HGX_FW_GPU_4")
        );
        assert!(
            result["error_message"]
                .as_str()
                .unwrap()
                .contains("HGX_FW_GPU_0")
        );

        // Both protobuf error enums predate failed-precondition. Keep their
        // lossy compatibility mapping while result_json carries the exact code.
        assert_eq!(
            rm::FirmwareUpdateError::from(job_error),
            rm::FirmwareUpdateError::ClientFailure
        );
        assert_eq!(rm::JobError::from(job_error), rm::JobError::ClientError);
    }

    #[test]
    fn firmware_failure_summary_extracts_liteon_psu_redfish_details() {
        let target = FirmwareTarget {
            component: "psu".to_owned(),
            firmware_file: "/tmp/psu.tar".to_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        };
        let raw = r#"Upload failed for target psu: NVFWUPD task failed for powerdevice0: firmware update failed with status 1: error=LiteOn PSU firmware update failed for powerdevice0: Object {"error": String("LiteOn PSU update timed out"), "task_id": String("powerdevice0"), "last_response": Object {"error": Object {"@Message.ExtendedInfo": Array [Object {"Message": String("The request failed due to an internal service error.  The service is still operational."), "MessageId": String("Base.1.11.0.InternalError"), "MessageSeverity": String("Critical"), "Resolution": String("Resubmit the request.  If the problem persists, consider resetting the service.")}]}}}; error=LiteOn PSU serial fallback failed"#;

        let (summary, result_json) = firmware_failure_message_and_result_json(
            "update",
            Some(&target),
            JobError::ClientError,
            raw,
        );

        assert_eq!(
            summary,
            "PSU update failed on powerdevice0: LiteOn PSU update timed out. Redfish returned Base.1.11.0.InternalError: The request failed due to an internal service error. The service is still operational. Resolution: Resubmit the request. If the problem persists, consider resetting the service."
        );
        let result: serde_json::Value = serde_json::from_str(&result_json).unwrap();
        assert_eq!(result["summary"], summary);
        assert_eq!(result["error_message"], summary);
        assert_eq!(result["target_id"], "powerdevice0");
        assert_eq!(result["redfish"]["code"], "Base.1.11.0.InternalError");
        assert_eq!(result["redfish"]["severity"], "Critical");
        assert_eq!(result["target"]["component"], "psu");
        assert!(result["raw_error"].as_str().unwrap().contains("Object"));
    }

    #[test]
    fn firmware_failure_summary_prefers_inner_liteon_psu_target() {
        let target = FirmwareTarget {
            component: "psu".to_owned(),
            firmware_file: "/tmp/psu.tar".to_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        };
        let raw = r#"Upload failed for target psu: NVFWUPD task failed for powerdevice0: firmware update failed with status 1: error=LiteOn PSU firmware update failed for powerdevice5: Object {"error": String("LiteOn PSU update timed out"), "task_id": String("powerdevice5"), "last_response": Object {"error": Object {"@Message.ExtendedInfo": Array [Object {"Message": String("The request failed due to an internal service error.  The service is still operational."), "MessageId": String("Base.1.11.0.InternalError"), "MessageSeverity": String("Critical"), "Resolution": String("Resubmit the request.  If the problem persists, consider resetting the service.")}]}}}; error=LiteOn PSU serial fallback failed"#;

        let (summary, result_json) = firmware_failure_message_and_result_json(
            "update",
            Some(&target),
            JobError::ClientError,
            raw,
        );

        assert_eq!(
            summary,
            "PSU update failed on powerdevice5: LiteOn PSU update timed out. Redfish returned Base.1.11.0.InternalError: The request failed due to an internal service error. The service is still operational. Resolution: Resubmit the request. If the problem persists, consider resetting the service."
        );
        let result: serde_json::Value = serde_json::from_str(&result_json).unwrap();
        assert_eq!(result["summary"], summary);
        assert_eq!(result["error_message"], summary);
        assert_eq!(result["target_id"], "powerdevice5");
        assert!(
            result["raw_error"]
                .as_str()
                .unwrap()
                .contains("NVFWUPD task failed for powerdevice0")
        );
    }

    #[async_trait]
    impl Node for OutcomeNode {
        fn id(&self) -> &str {
            "outcome-node"
        }
        fn rack_id(&self) -> &str {
            "outcome-rack"
        }
        fn node_type(&self) -> NodeType {
            self.node_type
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn update_firmware(
            &self,
            _target: &FirmwareTarget,
            _force_update: bool,
            options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            self.update_options.lock().unwrap().push(options);
            Ok(self
                .outcome
                .lock()
                .unwrap()
                .take()
                .expect("update_firmware called more than once"))
        }
        async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
            self.poll_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.poll_status.clone())
        }
    }

    #[async_trait]
    impl Node for ActivationNode {
        fn id(&self) -> &str {
            "activation-node"
        }
        fn rack_id(&self) -> &str {
            "activation-rack"
        }
        fn node_type(&self) -> NodeType {
            self.node_type
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn activate_firmware_with(
            &self,
            request: FirmwareActivationRequest,
        ) -> Result<FirmwareActivationSummary> {
            let expected = match self.node_type {
                NodeType::ComputeGb200Nvidia
                | NodeType::ComputeGb200Wiwynn
                | NodeType::ComputeGb200Dell
                | NodeType::ComputeGb300Nvidia
                | NodeType::ComputeGb300Lenovo
                | NodeType::ComputeGb300Supermicro
                | NodeType::ComputeVrnvl72Nvidia => FirmwareActivationMode::FullGb200Compute,
                NodeType::SwitchGb200Nvidia
                | NodeType::SwitchGb300Nvidia
                | NodeType::SwitchVrnvl72Nvidia => FirmwareActivationMode::SwitchPowerCycle,
                NodeType::PowershelfGb200Liteon
                | NodeType::PowershelfGb200Delta
                | NodeType::PowershelfGb300Liteon
                | NodeType::PowershelfGb300Delta
                | NodeType::PowershelfVrnvl72Liteon
                | NodeType::PowershelfVrnvl72Delta => {
                    FirmwareActivationMode::PowerShelfReset { force: false }
                }
            };
            assert_eq!(request.mode, expected);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(FirmwareActivationSummary {
                message: "activated".to_owned(),
                details: serde_json::json!({}),
            })
        }
    }

    #[async_trait]
    impl Node for InbandJobNode {
        fn id(&self) -> &str {
            "inband-node"
        }
        fn rack_id(&self) -> &str {
            "inband-rack"
        }
        fn node_type(&self) -> NodeType {
            self.node_type
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn update_firmware(
            &self,
            target: &FirmwareTarget,
            _force_update: bool,
            options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            assert!(options.cancellation.is_some());
            self.events
                .lock()
                .unwrap()
                .push(format!("out_of_band:{}", target.component));
            Ok(completed_update_outcome())
        }
        async fn update_firmware_group(
            &self,
            targets: &[FirmwareTarget],
            _force_update: bool,
            options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            assert_eq!(targets.len(), 2);
            assert!(options.cancellation.is_some());
            self.events.lock().unwrap().push("in_band".to_owned());
            self.group_calls.fetch_add(1, Ordering::SeqCst);
            self.outcome
                .lock()
                .unwrap()
                .take()
                .expect("group update called more than once")
        }
        async fn activate_firmware_with(
            &self,
            request: FirmwareActivationRequest,
        ) -> Result<FirmwareActivationSummary> {
            assert_eq!(request.mode, FirmwareActivationMode::FullGb200Compute);
            assert!(request.cancellation.is_some());
            self.events.lock().unwrap().push("activate".to_owned());
            if self.cancel_on_activation {
                request
                    .cancellation
                    .expect("activation cancellation token is required")
                    .cancel();
            }
            self.activation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(FirmwareActivationSummary {
                message: "activated".to_owned(),
                details: serde_json::json!({}),
            })
        }
        async fn verify_firmware_package_versions(
            &self,
            targets: &[FirmwareTarget],
        ) -> Result<FirmwareVersionCheckSummary> {
            assert!(!targets.is_empty());
            self.events.lock().unwrap().push(format!(
                "verify:{}",
                targets
                    .iter()
                    .map(|target| target.component.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            self.verification_calls.fetch_add(1, Ordering::SeqCst);
            Ok(FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({"status": "matched"}),
            })
        }
    }

    #[async_trait]
    impl Node for VersionCheckNode {
        fn id(&self) -> &str {
            "version-check-node"
        }
        fn rack_id(&self) -> &str {
            "version-check-rack"
        }
        fn node_type(&self) -> NodeType {
            self.node_type
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn verify_firmware_package_versions(
            &self,
            targets: &[FirmwareTarget],
        ) -> Result<FirmwareVersionCheckSummary> {
            assert_eq!(targets, &[firmware_target()]);
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call < self.transient_failures {
                return Err(RmsError::unavailable(
                    "NVFWUPD error: failed to retrieve firmware inventory",
                ));
            }
            Ok(self.summary.clone())
        }
        async fn verify_management_api_reachable(&self) -> Result<()> {
            let call = self.reachability_calls.fetch_add(1, Ordering::SeqCst);
            if call < self.unreachable_probes {
                return Err((self.unreachable_error)());
            }
            Ok(())
        }
        async fn verify_system_ready(&self) -> Result<()> {
            let call = self.readiness_calls.fetch_add(1, Ordering::SeqCst);
            if call < self.not_ready_probes {
                return Err((self.not_ready_error)());
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Node for StaticPollNode {
        fn id(&self) -> &str {
            "static-poll-node"
        }
        fn rack_id(&self) -> &str {
            "static-poll-rack"
        }
        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb200Nvidia
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
            Ok(self.status.clone())
        }
    }

    #[async_trait]
    impl Node for SequencePollNode {
        fn id(&self) -> &str {
            "sequence-poll-node"
        }
        fn rack_id(&self) -> &str {
            "sequence-poll-rack"
        }
        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb200Nvidia
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);

            if self.fail_refetch && call == 1 {
                return Err(RmsError::internal("refetch failed"));
            }

            let mut statuses = self.statuses.lock().unwrap();
            if statuses.len() > 1 {
                Ok(statuses.pop_front().unwrap())
            } else {
                Ok(statuses
                    .front()
                    .cloned()
                    .expect("sequence poll node needs at least one status"))
            }
        }
    }

    fn firmware_target() -> FirmwareTarget {
        FirmwareTarget {
            component: "BMC".to_owned(),
            firmware_file: "/tmp/fw.fwpkg".to_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        }
    }

    fn supermicro_target(component: &str, firmware_file: &str) -> FirmwareTarget {
        FirmwareTarget {
            component: component.to_owned(),
            firmware_file: firmware_file.to_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        }
    }

    fn supermicro_bmc_target_with_firmware_manifest_version() -> FirmwareTarget {
        FirmwareTarget {
            component: String::new(),
            firmware_file: "/tmp/NvOBMC_GBNVL72_70.02.01.05.bin".to_owned(),
            expected_version: Some("70.02.01.05".to_owned()),
            resolution: None,
            linkx_policy: None,
        }
    }

    #[test]
    fn supermicro_gb300_stages_nosbios_then_bios_then_bmc() {
        let nosbios = supermicro_target(
            SUPERMICRO_HGX_TARGET,
            "nvfw_GB300_custom_nosbios_prod-signed.fwpkg",
        );
        let bios = supermicro_target(SUPERMICRO_BIOS_TARGET, "BIOS_GPU-NVGB300_2.3b.bin");
        let bmc = supermicro_target("", "NvOBMC_GBNVL72_70.02.01.05.bin");

        // Input order is intentionally different from the required flash order.
        let stages = firmware_update_stages(
            NodeType::ComputeGb300Supermicro,
            &[bmc.clone(), bios.clone(), nosbios.clone()],
            true,
        )
        .unwrap();

        assert_eq!(
            stages,
            vec![
                FirmwareUpdateStage {
                    targets: vec![nosbios],
                    activate_after: true,
                },
                FirmwareUpdateStage {
                    targets: vec![bios],
                    activate_after: true,
                },
                FirmwareUpdateStage {
                    targets: vec![bmc],
                    activate_after: false,
                },
            ]
        );
    }

    #[test]
    fn supermicro_gb300_rejects_nosbios_plus_bios_without_activation() {
        let targets = [
            supermicro_target(
                SUPERMICRO_HGX_TARGET,
                "nvfw_GB300_custom_nosbios_prod-signed.fwpkg",
            ),
            supermicro_target(SUPERMICRO_BIOS_TARGET, "BIOS_GPU-NVGB300_2.3b.bin"),
        ];

        let error =
            firmware_update_stages(NodeType::ComputeGb300Supermicro, &targets, false).unwrap_err();

        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert!(error.message.contains("power-cycle between"));
    }

    #[test]
    fn lenovo_gb300_keeps_single_firmware_stage() {
        let targets = vec![firmware_target(), firmware_target()];

        let stages = firmware_update_stages(NodeType::ComputeGb300Lenovo, &targets, true).unwrap();

        assert_eq!(
            stages,
            vec![FirmwareUpdateStage {
                targets,
                activate_after: true,
            }]
        );
    }

    #[tokio::test]
    async fn supermicro_gb300_job_activates_between_manifest_payloads() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("rack-01", "supermicro-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let events = Arc::new(Mutex::new(Vec::new()));
        let node = Arc::new(SupermicroSequenceNode {
            events: Arc::clone(&events),
        });
        let targets = vec![
            supermicro_target("", "NvOBMC_GBNVL72_70.02.01.05.bin"),
            supermicro_target(SUPERMICRO_BIOS_TARGET, "BIOS_GPU-NVGB300_2.3b.bin"),
            supermicro_target(
                SUPERMICRO_HGX_TARGET,
                "nvfw_GB300_custom_nosbios_prod-signed.fwpkg",
            ),
        ];

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node,
            targets,
            true,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed
        );
        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                format!("update:{SUPERMICRO_HGX_TARGET}"),
                "activate".to_owned(),
                format!("verify:{SUPERMICRO_HGX_TARGET}"),
                format!("update:{SUPERMICRO_BIOS_TARGET}"),
                "activate".to_owned(),
                format!("verify:{SUPERMICRO_BIOS_TARGET}"),
                "update:".to_owned(),
            ]
        );
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(result["activation_count"], 2);
    }

    fn completed_update_outcome() -> FirmwareUpdateOutcome {
        FirmwareUpdateOutcome::Completed(crate::domain::node::FirmwareUpdateSummary {
            message: "done".to_owned(),
            task_ids: Vec::new(),
            details: serde_json::json!({}),
        })
    }

    fn inband_target(component: &str, filename: &str) -> FirmwareTarget {
        FirmwareTarget {
            component: component.to_owned(),
            firmware_file: filename.to_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        }
    }

    fn linkx_target() -> FirmwareTarget {
        FirmwareTarget {
            component: "LINKX".to_owned(),
            firmware_file: "/tmp/linkx.bin".to_owned(),
            expected_version: Some("44.40.2035".to_owned()),
            resolution: None,
            linkx_policy: Some(nvfwupd::flint::LinkxPolicy {
                current_version: "44.40.2019".to_owned(),
                target_version: "44.40.2035".to_owned(),
                sha256: "a".repeat(64),
            }),
        }
    }

    #[test]
    fn linkx_is_vr_only_policy_required_and_module_activated() {
        let target = linkx_target();
        for kind in [
            NodeType::ComputeGb200Nvidia,
            NodeType::ComputeGb300Nvidia,
            NodeType::SwitchVrnvl72Nvidia,
        ] {
            assert!(validate_firmware_request(kind, std::slice::from_ref(&target), true).is_err());
        }
        assert!(
            validate_firmware_request(
                NodeType::ComputeVrnvl72Nvidia,
                std::slice::from_ref(&target),
                false
            )
            .is_err()
        );
        let mut no_policy = target.clone();
        no_policy.linkx_policy = None;
        assert!(
            validate_firmware_request(NodeType::ComputeVrnvl72Nvidia, &[no_policy], true).is_err()
        );
        let stages = firmware_update_stages(
            NodeType::ComputeVrnvl72Nvidia,
            std::slice::from_ref(&target),
            true,
        )
        .unwrap();
        assert_eq!(stages.len(), 1);
        assert!(!stages[0].activate_after);
        let mixed = [target.clone(), firmware_target()];
        assert!(firmware_update_stages(NodeType::ComputeVrnvl72Nvidia, &mixed, true).is_err());
        let stages =
            firmware_update_stages_with_policy(NodeType::ComputeVrnvl72Nvidia, &mixed, true, true)
                .unwrap();
        assert_eq!(stages[0].targets, vec![firmware_target()]);
        assert!(stages[0].activate_after);
        assert_eq!(stages[1].targets, vec![target.clone()]);
        assert!(!stages[1].activate_after);
        assert!(
            validate_firmware_request(
                NodeType::ComputeVrnvl72Nvidia,
                &[target.clone(), target],
                true
            )
            .is_err()
        );
    }

    #[test]
    fn powershelf_stages_never_request_post_activation_version_verification() {
        // Power shelves do not implement `verify_firmware_package_versions`, so an
        // activated shelf stage must never schedule the post-activation check.
        for node_type in [
            NodeType::PowershelfGb200Liteon,
            NodeType::PowershelfGb200Delta,
            NodeType::PowershelfGb300Liteon,
            NodeType::PowershelfGb300Delta,
            NodeType::PowershelfVrnvl72Liteon,
            NodeType::PowershelfVrnvl72Delta,
        ] {
            let stages = firmware_update_stages_with_profile_policy(
                node_type,
                &[firmware_target()],
                true,
                false,
                None,
            )
            .unwrap();
            assert!(!stages.is_empty());
            for stage in stages {
                assert!(stage.activation.is_some(), "{node_type:?} activates");
                assert_eq!(
                    stage.verification, None,
                    "{node_type:?} must not verify versions"
                );
            }
        }
    }

    #[tokio::test]
    async fn linkx_job_completes_without_tray_activation_and_force_fails_before_io() {
        for force in [false, true] {
            let tracker = Arc::new(JobTracker::new());
            let pending = tracker
                .create_job("rack", "node", JobType::FirmwareUpdate)
                .unwrap();
            let job_id = pending.id().to_string();
            // This mock does not implement activation: accidentally requesting
            // a tray power cycle would fail the job.
            let node = Arc::new(OutcomeNode::new_for_node_type(
                NodeType::ComputeVrnvl72Nvidia,
                completed_update_outcome(),
            ));
            spawn_firmware_update_job(
                tracker.clone(),
                pending,
                node.clone(),
                vec![linkx_target()],
                true,
                force,
                FirmwareTaskPollOptions::default(),
                recovery_disarmed(),
            );
            let job = wait_for_firmware_job_terminal(&tracker, &job_id).await;
            if force {
                assert_eq!(
                    job.state,
                    crate::orchestrator::job_lifecycle::JobState::Failed
                );
                assert!(node.update_options.lock().unwrap().is_empty());
            } else {
                assert_eq!(
                    job.state,
                    crate::orchestrator::job_lifecycle::JobState::Completed,
                    "{}",
                    job.error_message
                );
                assert_eq!(node.update_options.lock().unwrap().len(), 1);
                let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
                assert_eq!(result["activated"], true);
            }
        }
    }

    struct FirmwarePreflightNode {
        node_type: NodeType,
        first_check: Mutex<Option<Result<FirmwareVersionCheckSummary>>>,
        events: Mutex<Vec<String>>,
        cancel_on_check: Option<CancellationToken>,
    }

    impl FirmwarePreflightNode {
        fn new(node_type: NodeType, summary: Result<FirmwareVersionCheckSummary>) -> Self {
            Self {
                node_type,
                first_check: Mutex::new(Some(summary)),
                events: Mutex::new(Vec::new()),
                cancel_on_check: None,
            }
        }
    }

    #[async_trait]
    impl Node for FirmwarePreflightNode {
        fn id(&self) -> &str {
            "preflight-switch"
        }
        fn rack_id(&self) -> &str {
            "preflight-rack"
        }
        fn node_type(&self) -> NodeType {
            self.node_type
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn verify_firmware_package_versions(
            &self,
            targets: &[FirmwareTarget],
        ) -> Result<FirmwareVersionCheckSummary> {
            assert_eq!(targets, switch_preflight_targets());
            self.events.lock().unwrap().push("compare".to_owned());
            if let Some(cancel) = &self.cancel_on_check {
                cancel.cancel();
                return std::future::pending().await;
            }
            self.first_check
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| Ok(switch_preflight_summary(true)))
        }
        async fn update_firmware(
            &self,
            target: &FirmwareTarget,
            _force_update: bool,
            _options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            self.events
                .lock()
                .unwrap()
                .push(format!("flash:{}", target.component));
            Ok(completed_update_outcome())
        }
        async fn activate_firmware_with(
            &self,
            _request: FirmwareActivationRequest,
        ) -> Result<FirmwareActivationSummary> {
            self.events.lock().unwrap().push("activate".to_owned());
            Ok(FirmwareActivationSummary {
                message: "activated".to_owned(),
                details: serde_json::json!({}),
            })
        }
    }

    fn switch_preflight_targets() -> Vec<FirmwareTarget> {
        vec![
            inband_target("BMC", "/tmp/switch.fwpkg"),
            inband_target("FPGA", "/tmp/switch.fwpkg"),
        ]
    }

    fn switch_preflight_summary(all_targets_matched: bool) -> FirmwareVersionCheckSummary {
        FirmwareVersionCheckSummary {
            matched: all_targets_matched,
            details: serde_json::json!({
                "status": if all_targets_matched { "matched" } else { "mismatch" },
                "all_targets_matched": all_targets_matched,
            }),
        }
    }

    async fn run_switch_preflight_job(
        node: Arc<FirmwarePreflightNode>,
        activate: bool,
        force_update: bool,
        firmware_object: bool,
    ) -> crate::orchestrator::job_tracker::JobInfo {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job(node.rack_id(), node.id(), JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        spawn_firmware_update_job_with_policy(
            tracker.clone(),
            pending,
            node,
            switch_preflight_targets(),
            activate,
            force_update,
            FirmwareTaskPollOptions::default(),
            firmware_object,
            recovery_disarmed(),
            None,
            None,
        );
        wait_for_firmware_job_terminal(&tracker, &job_id).await
    }

    #[tokio::test]
    async fn switch_preflight_all_current_skips_flash_and_activation() {
        for node_type in [
            NodeType::SwitchGb200Nvidia,
            NodeType::SwitchGb300Nvidia,
            NodeType::SwitchVrnvl72Nvidia,
        ] {
            for firmware_object in [false, true] {
                let node = Arc::new(FirmwarePreflightNode::new(
                    node_type,
                    Ok(switch_preflight_summary(true)),
                ));
                let job =
                    run_switch_preflight_job(node.clone(), true, false, firmware_object).await;
                assert_eq!(
                    job.state,
                    crate::orchestrator::job_lifecycle::JobState::Completed
                );
                assert_eq!(*node.events.lock().unwrap(), ["compare"]);
                let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
                assert_eq!(result["status"], "already_up_to_date");
                assert_eq!(result["targets_skipped"], 2);
                assert_eq!(result["targets_completed"], 0);
                assert_eq!(result["activated"], false);
                assert_eq!(
                    result["pre_update_version_check"]["all_targets_matched"],
                    true
                );
            }
        }
    }

    #[tokio::test]
    async fn switch_preflight_mismatch_or_unverified_continues_update() {
        for summary in [
            switch_preflight_summary(false),
            FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({"status": "unverified", "all_targets_matched": false}),
            },
            FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({"status": "matched", "all_targets_matched": false}),
            },
            FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({"status": "matched"}),
            },
        ] {
            let node = Arc::new(FirmwarePreflightNode::new(
                NodeType::SwitchGb200Nvidia,
                Ok(summary),
            ));
            let job = run_switch_preflight_job(node.clone(), true, false, false).await;
            assert_eq!(
                job.state,
                crate::orchestrator::job_lifecycle::JobState::Completed
            );
            assert_eq!(
                *node.events.lock().unwrap(),
                ["compare", "flash:BMC", "flash:FPGA", "activate", "compare"]
            );
        }
    }

    #[tokio::test]
    async fn switch_preflight_force_bypasses_comparison() {
        let node = Arc::new(FirmwarePreflightNode::new(
            NodeType::SwitchGb200Nvidia,
            Ok(switch_preflight_summary(true)),
        ));
        let job = run_switch_preflight_job(node.clone(), false, true, false).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed
        );
        assert_eq!(*node.events.lock().unwrap(), ["flash:BMC", "flash:FPGA"]);
    }

    #[tokio::test]
    async fn switch_preflight_read_failure_stops_before_flash_and_activation() {
        let node = Arc::new(FirmwarePreflightNode::new(
            NodeType::SwitchGb200Nvidia,
            Err(RmsError::unavailable("firmware inventory read failed")),
        ));
        let job = run_switch_preflight_job(node.clone(), true, false, true).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
        assert_eq!(*node.events.lock().unwrap(), ["compare"]);
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(result["phase"], "pre_update_version_check");
        assert!(job.error_message.contains("firmware inventory read failed"));
    }

    #[tokio::test]
    async fn switch_preflight_cancellation_stops_inventory_read() {
        for cancel_before_start in [true, false] {
            let cancel = CancellationToken::new();
            let mut node = FirmwarePreflightNode::new(
                NodeType::SwitchGb200Nvidia,
                Ok(switch_preflight_summary(true)),
            );
            node.cancel_on_check = Some(cancel.clone());
            if cancel_before_start {
                cancel.cancel();
            }
            let error = tokio::time::timeout(
                Duration::from_secs(1),
                pre_update_firmware_version_check(
                    &node,
                    &switch_preflight_targets(),
                    false,
                    &cancel,
                ),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert_eq!(error.code, ErrorCode::Cancelled);
            assert_eq!(
                node.events.lock().unwrap().len(),
                usize::from(!cancel_before_start)
            );
        }
    }

    #[tokio::test]
    async fn switch_preflight_leaves_raw_payloads_and_compute_updates_unchanged() {
        for (node_type, filename) in [
            (NodeType::SwitchGb200Nvidia, "/tmp/cpld.vme"),
            (NodeType::SwitchGb200Nvidia, "/tmp/nvos.bin"),
            (NodeType::ComputeGb200Nvidia, "/tmp/compute.fwpkg"),
        ] {
            let node = FirmwarePreflightNode::new(node_type, Ok(switch_preflight_summary(true)));
            let summary = pre_update_firmware_version_check(
                &node,
                &[inband_target("BMC", filename)],
                false,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
            assert!(summary.is_none());
            assert!(node.events.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn gb200_inband_validation_requires_activation_and_direct_requests_reject_mixed_targets() {
        let inband = vec![inband_target("CX7", "/tmp/cx7.bin")];
        for node_type in [NodeType::ComputeGb200Nvidia, NodeType::ComputeGb200Wiwynn] {
            let error = validate_firmware_request(node_type, &inband, false)
                .expect_err("in-band updates require activation");
            assert_eq!(error.code, ErrorCode::FailedPrecondition);
            validate_firmware_request(node_type, &inband, true)
                .expect("supported GB200 compute type should accept Flint targets");
        }

        let mixed = vec![inband[0].clone(), firmware_target()];
        let error = validate_firmware_request(NodeType::ComputeGb200Nvidia, &mixed, true)
            .expect_err("mixed OOB and in-band updates must be rejected");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
        validate_firmware_object_request(NodeType::ComputeGb200Nvidia, &mixed, true)
            .expect("firmware object apply should stage mixed targets internally");

        let error = validate_firmware_request(NodeType::ComputeGb300Nvidia, &inband, true)
            .expect_err("Flint targets are limited to supported GB200 compute types");
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn gb200_inband_targets_are_grouped_into_one_operation() {
        let targets = vec![
            inband_target("CX7", "/tmp/cx7-a.bin"),
            inband_target("CX7", "/tmp/cx7-b.bin"),
            inband_target("CX8", "/tmp/cx8.bin"),
            inband_target("BF3", "/tmp/bf3.bin"),
        ];

        for node_type in [NodeType::ComputeGb200Nvidia, NodeType::ComputeGb200Wiwynn] {
            let groups = firmware_update_groups(node_type, &targets);
            assert_eq!(groups, vec![targets.clone()]);
        }
    }

    #[tokio::test]
    async fn gb200_inband_job_groups_flashes_and_activates_once() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("inband-rack", "inband-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let node = Arc::new(InbandJobNode {
            node_type: NodeType::ComputeGb200Wiwynn,
            outcome: Mutex::new(Some(Ok(completed_update_outcome()))),
            events: Arc::new(Mutex::new(Vec::new())),
            cancel_on_activation: false,
            group_calls: AtomicUsize::new(0),
            activation_calls: AtomicUsize::new(0),
            verification_calls: AtomicUsize::new(0),
        });

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![
                inband_target("CX7", "/tmp/cx7-a.bin"),
                inband_target("CX7", "/tmp/cx7-b.bin"),
            ],
            true,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed
        );
        assert_eq!(node.group_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.verification_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn gb200_all_current_inband_job_avoids_activation() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("inband-rack", "inband-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let node = Arc::new(InbandJobNode {
            node_type: NodeType::ComputeGb200Nvidia,
            outcome: Mutex::new(Some(Ok(FirmwareUpdateOutcome::Skipped {
                reason: "already current".to_owned(),
            }))),
            events: Arc::new(Mutex::new(Vec::new())),
            cancel_on_activation: false,
            group_calls: AtomicUsize::new(0),
            activation_calls: AtomicUsize::new(0),
            verification_calls: AtomicUsize::new(0),
        });

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![
                inband_target("CX8", "/tmp/cx8-a.bin"),
                inband_target("CX8", "/tmp/cx8-b.bin"),
            ],
            true,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed
        );
        assert_eq!(node.group_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(node.verification_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn gb200_firmware_object_job_runs_inband_stage_before_out_of_band_stage() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("gb200-rack", "gb200-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let events = Arc::new(Mutex::new(Vec::new()));
        let node = Arc::new(InbandJobNode {
            node_type: NodeType::ComputeGb200Nvidia,
            outcome: Mutex::new(Some(Ok(completed_update_outcome()))),
            events: Arc::clone(&events),
            cancel_on_activation: false,
            group_calls: AtomicUsize::new(0),
            activation_calls: AtomicUsize::new(0),
            verification_calls: AtomicUsize::new(0),
        });

        spawn_firmware_object_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![
                firmware_target(),
                inband_target("CX7", "/tmp/cx7-a.bin"),
                inband_target("CX7", "/tmp/cx7-b.bin"),
            ],
            true,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed
        );
        assert_eq!(node.group_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 2);
        assert_eq!(node.verification_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            events.lock().unwrap().as_slice(),
            [
                "in_band",
                "activate",
                "verify:CX7,CX7",
                "out_of_band:BMC",
                "activate",
                "verify:BMC",
            ]
        );
    }

    #[tokio::test]
    async fn gb200_firmware_object_job_stops_before_out_of_band_stage_when_inband_fails() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("gb200-rack", "gb200-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let events = Arc::new(Mutex::new(Vec::new()));
        let node = Arc::new(InbandJobNode {
            node_type: NodeType::ComputeGb200Nvidia,
            outcome: Mutex::new(Some(Err(RmsError::internal("host Flint workflow failed")))),
            events: Arc::clone(&events),
            cancel_on_activation: false,
            group_calls: AtomicUsize::new(0),
            activation_calls: AtomicUsize::new(0),
            verification_calls: AtomicUsize::new(0),
        });

        spawn_firmware_object_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![
                firmware_target(),
                inband_target("CX7", "/tmp/cx7-a.bin"),
                inband_target("CX7", "/tmp/cx7-b.bin"),
            ],
            true,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
        assert_eq!(node.group_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(events.lock().unwrap().as_slice(), ["in_band"]);
    }

    #[tokio::test]
    async fn gb200_firmware_object_job_stops_before_out_of_band_stage_when_cancelled() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("gb200-rack", "gb200-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let events = Arc::new(Mutex::new(Vec::new()));
        let node = Arc::new(InbandJobNode {
            node_type: NodeType::ComputeGb200Nvidia,
            outcome: Mutex::new(Some(Ok(completed_update_outcome()))),
            events: Arc::clone(&events),
            cancel_on_activation: true,
            group_calls: AtomicUsize::new(0),
            activation_calls: AtomicUsize::new(0),
            verification_calls: AtomicUsize::new(0),
        });

        spawn_firmware_object_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![
                firmware_target(),
                inband_target("CX7", "/tmp/cx7-a.bin"),
                inband_target("CX7", "/tmp/cx7-b.bin"),
            ],
            true,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
        assert_eq!(node.group_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.verification_calls.load(Ordering::SeqCst), 0);
        assert_eq!(events.lock().unwrap().as_slice(), ["in_band", "activate"]);
    }

    #[tokio::test]
    async fn spawned_firmware_job_uses_completed_success_status() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("rack-1", "node-1", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let node = Arc::new(OutcomeNode::new(completed_update_outcome()));

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node,
            vec![firmware_target()],
            false,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed
        );
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(result["status"], "completed");
    }

    /// Recovery dispatch inputs for a node without a roster: an empty object store.
    fn recovery_disarmed() -> RecoveryDispatch {
        RecoveryDispatch {
            firmware_objects: crate::persistence::Backends::memory().firmware_objects,
            firmware_dir: std::path::PathBuf::from("firmware"),
            artifact_require_sha256: false,
            source: RecoverySource::Latest,
        }
    }

    /// Compute node with the recovery capability: `recovery()` is `Some`, so
    /// the job layer detects through it and hands the result to `recover`,
    /// which this mock records.
    struct RecoveryDetectMockNode {
        /// Inventories returned per `get_firmware_inventory` call. The last
        /// entry repeats, which makes the recovery flow's re-reads
        /// scriptable.
        inventories: Mutex<VecDeque<Vec<FirmwareInfo>>>,
        /// Error returned by the next `get_firmware_inventory` instead of an
        /// inventory. The real node's read can fail, and the detection's Err
        /// arm needs a mock that can show it.
        inventory_error: Mutex<Option<RmsError>>,
        update_calls: AtomicUsize,
        /// Each `recover` call as one line: the detected list and the two
        /// bundle files it received.
        recovery_ops: Mutex<Vec<String>>,
        /// Error the next `recover` returns after logging the call, so a test
        /// can show the job what the flow reports.
        recover_error: Mutex<Option<RmsError>>,
        /// Token fired from inside `get_firmware_inventory`, so a test can
        /// land a cancel in the detection read.
        cancel_in_inventory: Mutex<Option<CancellationToken>>,
        /// The node's recovery watch set, as its roster would supply it.
        watched: Vec<String>,
    }

    impl RecoveryDetectMockNode {
        fn with_inventory(inventory: Vec<FirmwareInfo>) -> Self {
            Self::with_inventories([inventory])
        }

        fn with_inventories(inventories: impl IntoIterator<Item = Vec<FirmwareInfo>>) -> Self {
            Self {
                inventories: Mutex::new(inventories.into_iter().collect()),
                inventory_error: Mutex::new(None),
                update_calls: AtomicUsize::new(0),
                recovery_ops: Mutex::new(Vec::new()),
                recover_error: Mutex::new(None),
                cancel_in_inventory: Mutex::new(None),
                watched: vec!["HGX_FW_FPGA_0".to_owned()],
            }
        }

        fn cancelling_in_inventory(self, cancel: CancellationToken) -> Self {
            *self.cancel_in_inventory.lock().unwrap() = Some(cancel);
            self
        }

        fn with_recover_error(self, error: RmsError) -> Self {
            *self.recover_error.lock().unwrap() = Some(error);
            self
        }

        fn with_failing_inventory(error: RmsError) -> Self {
            let node = Self::with_inventory(Vec::new());
            *node.inventory_error.lock().unwrap() = Some(error);
            node
        }
    }

    #[async_trait]
    impl Node for RecoveryDetectMockNode {
        fn id(&self) -> &str {
            "recovery-node"
        }
        fn rack_id(&self) -> &str {
            "recovery-rack"
        }
        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb200Nvidia
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        fn recovery(&self) -> Option<&dyn Recoverable> {
            Some(self)
        }
        async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
            if let Some(e) = self.inventory_error.lock().unwrap().take() {
                return Err(e);
            }
            if let Some(cancel) = self.cancel_in_inventory.lock().unwrap().take() {
                cancel.cancel();
            }
            let mut inventories = self.inventories.lock().unwrap();
            if inventories.len() > 1 {
                Ok(inventories.pop_front().expect("len checked"))
            } else {
                Ok(inventories.front().cloned().unwrap_or_default())
            }
        }
        async fn update_firmware(
            &self,
            _target: &FirmwareTarget,
            _force_update: bool,
            _options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            self.update_calls.fetch_add(1, Ordering::SeqCst);
            Ok(completed_update_outcome())
        }
    }

    #[async_trait]
    impl Recoverable for RecoveryDetectMockNode {
        fn detect(&self, inventory: &[FirmwareInfo]) -> Vec<String> {
            inventory
                .iter()
                .filter(|info| {
                    self.watched.contains(&info.name)
                        && info.health == "Critical"
                        && info.state == "StandbyOffline"
                })
                .map(|info| info.name.clone())
                .collect()
        }

        async fn recover(
            &self,
            in_recovery: &[String],
            bundles: crate::domain::node::RecoveryBundles,
            _cancel: &CancellationToken,
        ) -> Result<()> {
            self.recovery_ops.lock().unwrap().push(format!(
                "recover {in_recovery:?} regular={} recovery={}",
                bundles.regular, bundles.recovery
            ));
            match self.recover_error.lock().unwrap().take() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }
    }

    fn watched_entry(health: &str, state: &str) -> FirmwareInfo {
        FirmwareInfo {
            name: "HGX_FW_FPGA_0".to_owned(),
            version: "1.0.0".to_owned(),
            firmware_type: FirmwareType::FPGA,
            updateable: true,
            target: String::new(),
            health: health.to_owned(),
            state: state.to_owned(),
            sku: String::new(),
        }
    }

    #[tokio::test]
    async fn recovery_dispatch_fails_the_job_when_no_bundles_are_resolvable() {
        // A component in recovery dispatches the recovery flow, but with no
        // available firmware object the bundles cannot be resolved: the job
        // must fail before any flash or recovery push.
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("recovery-rack", "recovery-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let node = Arc::new(RecoveryDetectMockNode::with_inventory(vec![watched_entry(
            "Critical",
            "StandbyOffline",
        )]));
        let service = test_service(Arc::new(RackManager::new()));

        // Via the object entry point on purpose: detection is shared code,
        // and this pins the object flow's wiring of the detection params.
        spawn_firmware_object_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![firmware_target()],
            false,
            false,
            FirmwareTaskPollOptions::default(),
            service.recovery_dispatch(RecoverySource::Latest),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
        assert_eq!(job.error_code, JobError::FailedPrecondition);
        assert!(
            job.error_message.contains("compute OOB recovery")
                && job.error_message.contains("no available firmware object"),
            "unexpected message: {}",
            job.error_message
        );
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 0);
        assert!(node.recovery_ops.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn recovery_dispatch_runs_the_flow_and_proceeds_in_the_same_job() {
        // Automatic remediation, end to end in one job: detection trips
        // on the recovery signature, bundles resolve from the latest firmware
        // object, the node runs its recovery procedure, the update then
        // proceeds.
        let firmware_dir = tempfile::tempdir().unwrap();
        let regular = firmware_dir.path().join("regular.fwpkg");
        let recovery = firmware_dir.path().join("recovery.fwpkg");
        std::fs::write(&regular, b"regular").unwrap();
        std::fs::write(&recovery, b"recovery").unwrap();
        let manifest: crate::domain::firmware_manifest::FirmwareManifest =
            serde_json::from_value(serde_json::json!({
                "ProductName": "GB200", "Milestones": [{"Name": "test", "BoardSKUs": [{
                    "Type": "Compute Node", "Components": {"Firmware": [{
                        "Component": "HMC", "Version": "1.0", "Type": "Prod",
                        "Locations": [
                            {"Location": regular, "LocationType": "file", "Type": "Firmware"},
                            {"Location": recovery, "LocationType": "file", "Type": "Firmware-Recovery"}
                        ]
                    }]}
                }]}]
            }))
            .unwrap();

        let mut service = test_service(Arc::new(RackManager::new()));
        service.firmware_dir = firmware_dir.path().to_owned();
        crate::api::grpc::firmware_object_handlers::store_prepared_manifest(
            &service,
            "obj-1",
            crate::persistence::RackHardwareType("gb200-nvl72".to_owned()),
            &manifest,
        )
        .await;

        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("recovery-rack", "recovery-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        // Detection read: FPGA-named component in recovery. Flow confirm:
        // healthy. The update then runs on the healthy tray.
        let node = Arc::new(RecoveryDetectMockNode::with_inventories([
            vec![watched_entry("Critical", "StandbyOffline")],
            vec![watched_entry("OK", "Enabled")],
        ]));

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![firmware_target()],
            false,
            false,
            FirmwareTaskPollOptions::default(),
            service.recovery_dispatch(RecoverySource::Latest),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed,
            "error: {}",
            job.error_message
        );
        // The dispatch hands the node the detected list and both resolved
        // bundle files, once, and then runs the actual update. The sequence
        // itself is the node's, pinned in nodes/compute_gb200_nvidia/recovery.rs.
        let ops = node.recovery_ops.lock().unwrap().clone();
        assert_eq!(ops.len(), 1, "ops: {ops:?}");
        assert!(
            ops[0].starts_with("recover [\"HGX_FW_FPGA_0\"] regular="),
            "ops: {ops:?}"
        );
        assert!(
            ops[0].contains("/regular.fwpkg recovery=") && ops[0].ends_with("/recovery.fwpkg"),
            "ops: {ops:?}"
        );
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn node_instance_dispatches_recovery_capabilities() {
        // The wrapper must route the recovery capability to the wrapped node.
        // A missing dispatch arm silently falls back to the trait default's
        // Unimplemented error, even when the inner node implements the
        // method (caught live against StratumSim, 2026-08-28).
        struct NodeMock;
        #[async_trait]
        impl Node for NodeMock {
            fn id(&self) -> &str {
                "probe"
            }
            fn rack_id(&self) -> &str {
                "rack"
            }
            fn node_type(&self) -> NodeType {
                NodeType::ComputeGb200Nvidia
            }
            fn get_info(&self) -> HashMap<String, String> {
                HashMap::new()
            }
            fn recovery(&self) -> Option<&dyn Recoverable> {
                Some(self)
            }
        }
        #[async_trait]
        impl Recoverable for NodeMock {
            fn detect(&self, _inventory: &[FirmwareInfo]) -> Vec<String> {
                Vec::new()
            }
            async fn recover(
                &self,
                _in_recovery: &[String],
                _bundles: crate::domain::node::RecoveryBundles,
                _cancel: &CancellationToken,
            ) -> Result<()> {
                Ok(())
            }
        }

        let instance = NodeInstance::from_test_node(NodeMock);
        instance
            .recovery()
            .expect("recovery must reach the wrapped node")
            .recover(
                &["HGX_FW_FPGA_0".to_owned()],
                crate::domain::node::RecoveryBundles {
                    regular: "regular.fwpkg".to_owned(),
                    recovery: "recovery.fwpkg".to_owned(),
                },
                &CancellationToken::new(),
            )
            .await
            .expect("recover must reach the wrapped node");
    }

    #[tokio::test]
    async fn recovery_dispatch_respects_a_pre_cancelled_job() {
        // Without the pre-dispatch cancel check the flow would power-cycle
        // the node before the stage loop's own cancel checks run.
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("recovery-rack", "recovery-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        pending.cancellation_token().cancel();
        let node = Arc::new(RecoveryDetectMockNode::with_inventory(vec![watched_entry(
            "Critical",
            "StandbyOffline",
        )]));
        let service = test_service(Arc::new(RackManager::new()));

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![firmware_target()],
            false,
            false,
            FirmwareTaskPollOptions::default(),
            service.recovery_dispatch(RecoverySource::Latest),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
        assert!(node.recovery_ops.lock().unwrap().is_empty());
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn recovery_dispatch_reports_a_cancel_from_inside_the_flow_as_cancelled() {
        // The flow stopped at one of its safe points. The job ends in the
        // same phase as the job's own cancel guards, not as a recovery
        // failure, and no flash follows.
        let node = Arc::new(
            RecoveryDetectMockNode::with_inventory(vec![watched_entry(
                "Critical",
                "StandbyOffline",
            )])
            .with_recover_error(RmsError::cancelled("recovery flow cancelled")),
        );
        let bundles = crate::domain::node::RecoveryBundles {
            regular: "regular.fwpkg".to_owned(),
            recovery: "recovery.fwpkg".to_owned(),
        };

        let job = run_detection_job(node.clone(), RecoverySource::Prepared(bundles)).await;

        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
        assert_eq!(job.error_code, JobError::Other);
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(result["phase"], "cancelled");
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn recovery_dispatch_stops_after_a_cancel_in_the_detection_read() {
        // The cancel lands during the inventory read. With no bundles the
        // dispatch would otherwise end the job as a bundle failure.
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("recovery-rack", "recovery-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let node = Arc::new(
            RecoveryDetectMockNode::with_inventory(vec![watched_entry(
                "Critical",
                "StandbyOffline",
            )])
            .cancelling_in_inventory(pending.cancellation_token()),
        );
        let service = test_service(Arc::new(RackManager::new()));

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![firmware_target()],
            false,
            false,
            FirmwareTaskPollOptions::default(),
            service.recovery_dispatch(RecoverySource::Absent),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(job.error_code, JobError::Other);
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(result["phase"], "cancelled");
        assert!(node.recovery_ops.lock().unwrap().is_empty());
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 0);
    }

    /// Drives one update job through recovery detection and returns the terminal job.
    async fn run_detection_job(
        node: Arc<RecoveryDetectMockNode>,
        source: RecoverySource,
    ) -> crate::orchestrator::job_tracker::JobInfo {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("recovery-rack", "recovery-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let service = test_service(Arc::new(RackManager::new()));

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node,
            vec![firmware_target()],
            false,
            false,
            FirmwareTaskPollOptions::default(),
            service.recovery_dispatch(source),
        );

        wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await
    }

    #[tokio::test]
    async fn recovery_detection_ignores_non_signature_readings() {
        // Only the full recovery signature (Critical + StandbyOffline)
        // dispatches the flow. A healthy reading, Warning health, Critical
        // with another State, OK + StandbyOffline, and a watched component
        // absent from inventory all enter the update path unchanged. The
        // stricter pre-flash proceed condition (block unless everything reads
        // OK) is a separate change.
        for inventory in [
            vec![watched_entry("OK", "Enabled")],
            vec![watched_entry("Warning", "Enabled")],
            vec![watched_entry("Critical", "Unknown")],
            vec![watched_entry("OK", "StandbyOffline")],
            Vec::new(),
        ] {
            let node = Arc::new(RecoveryDetectMockNode::with_inventory(inventory));
            let job = run_detection_job(node.clone(), RecoverySource::Latest).await;

            assert_eq!(
                job.state,
                crate::orchestrator::job_lifecycle::JobState::Completed
            );
            assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
            assert!(node.recovery_ops.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn recovery_detection_skips_on_a_failed_inventory_read() {
        // An unreadable inventory cannot prove recovery state. Detection is
        // best-effort until the pre-flash health check lands. The
        // update proceeds as it did before this change.
        let node = Arc::new(RecoveryDetectMockNode::with_failing_inventory(
            RmsError::new(ErrorCode::Timeout, "inventory read timed out"),
        ));
        let job = run_detection_job(node.clone(), RecoverySource::Latest).await;

        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed
        );
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
        assert!(node.recovery_ops.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn firmware_job_bails_before_touching_node_when_cancelled() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("rack-1", "node-1", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        // Cancel before the worker starts so the first workflow-boundary guard
        // fails the job without ever invoking the node's update path.
        pending.cancellation_token().cancel();
        let node = Arc::new(OutcomeNode::new(completed_update_outcome()));

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node,
            vec![firmware_target()],
            false,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
        assert!(
            job.error_message.contains("cancelled"),
            "unexpected error message: {}",
            job.error_message
        );
    }

    /// End-to-end graceful-shutdown proof: a real firmware worker polling a task
    /// that never completes must observe the token fired by
    /// `cancel_active_leaves()` and reach a terminal state inside the bounded
    /// shutdown grace window, rather than pinning the node until the poll
    /// timeout. This exercises the actual shutdown sequencer, not just a token
    /// cancelled by hand.
    #[tokio::test]
    async fn graceful_shutdown_seals_a_hung_firmware_job_within_grace_window() {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("rack-1", "node-1", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();

        // The firmware task starts but its poll never reports completion, so
        // without cancellation the worker would poll until the per-kind timeout.
        let node = Arc::new(OutcomeNode {
            node_type: NodeType::ComputeGb200Nvidia,
            outcome: Mutex::new(Some(FirmwareUpdateOutcome::Started(
                crate::domain::node::FirmwareTaskHandle {
                    task_id: "task-hang".to_owned(),
                },
            ))),
            update_options: Mutex::new(Vec::new()),
            poll_status: FirmwareTaskStatus {
                completed: false,
                ..Default::default()
            },
            poll_calls: AtomicUsize::new(0),
        });

        spawn_firmware_update_job(
            tracker.clone(),
            pending,
            node.clone(),
            vec![firmware_target()],
            false,
            false,
            FirmwareTaskPollOptions::default(),
            recovery_disarmed(),
        );

        // Wait until the worker is actually in-flight (polling the hung task) so
        // shutdown exercises the real mid-poll cancellation path.
        tokio::time::timeout(Duration::from_secs(5), async {
            while node.poll_calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("worker should begin polling the hung firmware task");

        // Real graceful shutdown: begin_shutdown + cancel_active_leaves, then
        // wait (bounded) for the job to seal. A generous window would still be
        // hit as a *timeout* if the worker ignored the token; instead it should
        // return promptly with the job already terminal.
        let cancelled = tokio::time::timeout(
            Duration::from_secs(5),
            tracker.shutdown_and_await_terminal(Duration::from_secs(30)),
        )
        .await
        .expect("shutdown must not block waiting on the hung job");

        assert_eq!(
            cancelled, 1,
            "the in-flight firmware job should be signalled"
        );
        let job = tracker.get_job(&job_id).expect("job should exist");
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed,
            "cancelled firmware job must reach a terminal state within the grace window"
        );
        assert!(
            job.error_message.contains("cancelled"),
            "unexpected error message: {}",
            job.error_message
        );
    }

    #[test]
    fn target_result_json_preserves_nvfwupd_summary_details() {
        let disposition = FirmwareTargetDisposition::Applied {
            summary: Some(crate::domain::node::FirmwareUpdateSummary {
                message: "firmware update completed".to_owned(),
                task_ids: vec!["liteon-psu-serial-complete".to_owned()],
                details: serde_json::json!({
                    "Output": [{
                        "Id": "liteon-psu-serial-complete",
                        "stage": "liteon_psu_serial",
                        "devices": [{
                            "task_id": "powerdevice0",
                            "updateProgress": 100,
                            "status": "OK"
                        }]
                    }]
                }),
            }),
        };

        let result = firmware_target_result_json(&firmware_target(), &disposition);

        assert_eq!(result["status"], "completed");
        assert_eq!(
            result["nvfwupd_summary"]["task_ids"][0],
            "liteon-psu-serial-complete"
        );
        assert_eq!(
            result["nvfwupd_summary"]["details"]["Output"][0]["devices"][0]["task_id"],
            "powerdevice0"
        );
    }

    #[tokio::test]
    async fn update_target_outcome_completed_finishes_without_polling() {
        let node = OutcomeNode::new(FirmwareUpdateOutcome::Completed(
            crate::domain::node::FirmwareUpdateSummary {
                message: "done".to_owned(),
                task_ids: Vec::new(),
                details: serde_json::json!({}),
            },
        ));

        let disposition = update_firmware_target_until_done(
            &node,
            &firmware_target(),
            false,
            FirmwareTaskPollOptions::default(),
        )
        .await
        .expect("completed outcome should succeed");

        assert!(matches!(
            disposition,
            FirmwareTargetDisposition::Applied { summary: Some(_) }
        ));
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn started_task_cleanup_runs_after_success_failure_and_cancellation() {
        use crate::domain::node::{FirmwareTaskCleanup, FirmwareTaskHandle};

        struct CleanupNode {
            status: FirmwareTaskStatus,
            fail_cleanup: bool,
            calls: AtomicUsize,
        }
        #[async_trait]
        impl FirmwareTaskCleanup for CleanupNode {
            async fn finish(&self, task_id: &str) -> Result<()> {
                assert_eq!(task_id, "force-task");
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.fail_cleanup {
                    return Err(RmsError::failed_precondition("ForceUpdate cleanup pending"));
                }
                Ok(())
            }
        }
        #[async_trait]
        impl Node for CleanupNode {
            fn id(&self) -> &str {
                "cleanup-node"
            }
            fn rack_id(&self) -> &str {
                "rack"
            }
            fn node_type(&self) -> NodeType {
                NodeType::ComputeGb200Dell
            }
            fn get_info(&self) -> HashMap<String, String> {
                HashMap::new()
            }
            fn firmware_task_cleanup(&self) -> Option<&dyn FirmwareTaskCleanup> {
                Some(self)
            }
            async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
                Ok(self.status.clone())
            }
        }

        for (state, cancelled) in [("Completed", false), ("Failed", false), ("Running", true)] {
            for fail_cleanup in [false, true] {
                let node = CleanupNode {
                    status: FirmwareTaskStatus {
                        state: state.to_owned(),
                        status: if state == "Failed" { "Critical" } else { "OK" }.to_owned(),
                        completed: state != "Running",
                        ..Default::default()
                    },
                    fail_cleanup,
                    calls: AtomicUsize::new(0),
                };
                let cancel = CancellationToken::new();
                if cancelled {
                    cancel.cancel();
                }
                let result = firmware_update_outcome_until_done(
                    &node,
                    &firmware_target(),
                    FirmwareUpdateOutcome::Started(FirmwareTaskHandle {
                        task_id: "force-task".to_owned(),
                    }),
                    false,
                    poll_options(Duration::from_secs(1), Duration::ZERO),
                    None,
                    &cancel,
                )
                .await;
                assert_eq!(node.calls.load(Ordering::SeqCst), 1);
                if state == "Completed" {
                    // A cleanup failure must not fail a task whose firmware was applied.
                    assert!(result.is_ok(), "{fail_cleanup}: {result:?}");
                } else {
                    let FirmwareTargetUpdateError::Poll(error) = result.unwrap_err() else {
                        panic!("expected poll/cleanup failure");
                    };
                    if cancelled {
                        assert_eq!(error.code, ErrorCode::Cancelled);
                    }
                    if state == "Failed" {
                        assert!(error.message.contains("ended unsuccessfully"));
                    }
                    if fail_cleanup {
                        assert!(error.message.contains("ForceUpdate cleanup pending"));
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn update_target_outcome_skipped_finishes_without_polling() {
        let node = OutcomeNode::new(FirmwareUpdateOutcome::Skipped {
            reason: "already current".to_owned(),
        });

        let disposition = update_firmware_target_until_done(
            &node,
            &firmware_target(),
            false,
            FirmwareTaskPollOptions::default(),
        )
        .await
        .expect("skipped outcome should succeed");

        assert_eq!(
            disposition,
            FirmwareTargetDisposition::Skipped {
                reason: "already current".to_owned()
            }
        );
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn completed_switch_and_powershelf_outcomes_do_not_poll_empty_task_ids() {
        for node_type in [
            NodeType::SwitchGb200Nvidia,
            NodeType::SwitchGb300Nvidia,
            NodeType::SwitchVrnvl72Nvidia,
            NodeType::PowershelfGb200Liteon,
            NodeType::PowershelfGb200Delta,
            NodeType::PowershelfGb300Liteon,
            NodeType::PowershelfGb300Delta,
            NodeType::PowershelfVrnvl72Liteon,
            NodeType::PowershelfVrnvl72Delta,
        ] {
            let node = OutcomeNode::new_for_node_type(
                node_type,
                FirmwareUpdateOutcome::Completed(crate::domain::node::FirmwareUpdateSummary {
                    message: "workflow completed internally".to_owned(),
                    task_ids: Vec::new(),
                    details: serde_json::json!({}),
                }),
            );

            let disposition = update_firmware_target_until_done(
                &node,
                &firmware_target(),
                false,
                FirmwareTaskPollOptions::default(),
            )
            .await
            .expect("completed outcome should succeed without task polling");

            assert!(matches!(
                disposition,
                FirmwareTargetDisposition::Applied { summary: Some(_) }
            ));
            assert_eq!(node.poll_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn powershelf_updates_always_use_on_reset_apply_time() {
        for node_type in [
            NodeType::PowershelfGb200Liteon,
            NodeType::PowershelfGb200Delta,
            NodeType::PowershelfGb300Liteon,
            NodeType::PowershelfGb300Delta,
            NodeType::PowershelfVrnvl72Liteon,
            NodeType::PowershelfVrnvl72Delta,
        ] {
            let node = OutcomeNode::new_for_node_type(node_type, completed_update_outcome());

            let disposition = update_firmware_target_until_done(
                &node,
                &firmware_target(),
                false,
                FirmwareTaskPollOptions::default(),
            )
            .await
            .expect("powershelf update should succeed");

            assert!(matches!(
                disposition,
                FirmwareTargetDisposition::Applied { summary: Some(_) }
            ));
            let options = node.update_options.lock().unwrap();
            assert_eq!(options.len(), 1);
            assert_eq!(options[0].apply_time.as_deref(), Some("OnReset"));
        }
    }

    #[tokio::test]
    async fn non_powershelf_updates_leave_apply_time_unset() {
        for node_type in [
            NodeType::ComputeGb200Nvidia,
            NodeType::ComputeGb300Nvidia,
            NodeType::ComputeGb300Lenovo,
            NodeType::ComputeVrnvl72Nvidia,
            NodeType::SwitchGb200Nvidia,
            NodeType::SwitchGb300Nvidia,
        ] {
            let node = OutcomeNode::new_for_node_type(node_type, completed_update_outcome());

            let disposition = update_firmware_target_until_done(
                &node,
                &firmware_target(),
                false,
                FirmwareTaskPollOptions::default(),
            )
            .await
            .expect("non-powershelf update should succeed");

            assert!(matches!(
                disposition,
                FirmwareTargetDisposition::Applied { summary: Some(_) }
            ));
            let options = node.update_options.lock().unwrap();
            assert_eq!(options.len(), 1);
            assert_eq!(options[0].apply_time, None);
        }
    }

    #[tokio::test]
    async fn update_target_outcome_started_polls_until_complete() {
        let node = OutcomeNode::new(FirmwareUpdateOutcome::Started(
            crate::domain::node::FirmwareTaskHandle {
                task_id: "Task-1".to_owned(),
            },
        ));

        let disposition = update_firmware_target_until_done(
            &node,
            &firmware_target(),
            false,
            FirmwareTaskPollOptions::default(),
        )
        .await
        .expect("started outcome should poll and succeed");

        assert_eq!(
            disposition,
            FirmwareTargetDisposition::Applied { summary: None }
        );
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn identical_image_task_skips_only_when_effective_force_is_false() {
        let rejection = "The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'.";

        for message in [
            rejection.to_owned(),
            format!("{rejection} Resolution: Retry firmware update operation with the force flag"),
        ] {
            for (force_update, profile_force) in [(false, false), (true, false), (false, true)] {
                let mut node = OutcomeNode::new(FirmwareUpdateOutcome::Started(
                    crate::domain::node::FirmwareTaskHandle {
                        task_id: "Task-1".to_owned(),
                    },
                ));

                node.poll_status = FirmwareTaskStatus {
                    completed: true,
                    state: "Failed".to_owned(),
                    status: "Critical".to_owned(),
                    message: message.clone(),
                    rejection: Some(FirmwareTaskRejection::IdenticalImage),
                    ..Default::default()
                };

                let mut target = firmware_target();

                if profile_force {
                    target.resolution =
                        Some(crate::domain::firmware_target::FirmwareTargetResolution {
                            profile_target_id: "component".to_owned(),
                            destination:
                                crate::domain::firmware_target::FirmwareDestination::DriverDefault,
                            update_parameters: Some(serde_json::Map::from_iter([(
                                "ForceUpdate".to_owned(),
                                serde_json::Value::Bool(true),
                            )])),
                            oem_parameters: None,
                        });
                }

                let disposition = update_firmware_target_until_done(
                    &node,
                    &target,
                    force_update,
                    poll_options(Duration::from_secs(1), Duration::ZERO),
                )
                .await;

                if force_update || profile_force {
                    let error = disposition.expect_err("forced task rejection must fail");

                    let FirmwareTargetUpdateError::Poll(error) = error else {
                        panic!("forced task rejection must be a polling error");
                    };

                    assert!(error.message.contains(&message));
                } else {
                    let disposition = disposition.expect("unforced rejection must skip");
                    let result = firmware_target_result_json(&target, &disposition);

                    assert_eq!(
                        disposition,
                        FirmwareTargetDisposition::Skipped {
                            reason: message.clone()
                        }
                    );

                    assert_eq!(result["status"], "skipped");
                    assert_eq!(result["reason"], message);
                }

                assert_eq!(node.poll_calls.load(Ordering::SeqCst), 2);
            }
        }
    }

    #[tokio::test]
    async fn identical_image_rejection_has_a_distinct_poll_outcome() {
        let rejected = FirmwareTaskStatus {
            completed: true,
            state: "Failed".to_owned(),
            status: "Critical".to_owned(),
            message: "Component image is identical".to_owned(),
            rejection: Some(FirmwareTaskRejection::IdenticalImage),
            ..Default::default()
        };

        let node = SequencePollNode {
            statuses: Mutex::new(VecDeque::from([rejected.clone(), rejected])),
            calls: AtomicUsize::new(0),
            fail_refetch: false,
        };

        let result = poll_firmware_task_until_complete_with_retry_timeout(
            &node,
            "Task-1",
            poll_options(Duration::from_secs(1), Duration::ZERO),
            true,
            &CancellationToken::new(),
        )
        .await
        .expect("confirmed identical-image rejection should be returned for skip handling");

        assert!(matches!(
            result,
            PolledFirmwareTask::IdenticalImageRejected(status) if status.state == "Failed"
        ));

        assert_eq!(node.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shorter_mixed_failure_on_refetch_is_not_skipped() {
        let node = SequencePollNode {
            statuses: Mutex::new(VecDeque::from([
                FirmwareTaskStatus {
                    completed: true,
                    state: "Failed".to_owned(),
                    status: "Critical".to_owned(),
                    message: "The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'. Resolution: Retry the firmware update with the force flag after reviewing all selected components".to_owned(),
                    rejection: Some(FirmwareTaskRejection::IdenticalImage),
                    ..Default::default()
                },
                FirmwareTaskStatus {
                    completed: true,
                    state: "Failed".to_owned(),
                    status: "Critical".to_owned(),
                    message: "The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'.; Another component failed.".to_owned(),
                    ..Default::default()
                },
            ])),
            calls: AtomicUsize::new(0),
            fail_refetch: false,
        };

        let error = poll_firmware_task_until_complete_with_retry_timeout(
            &node,
            "Task-1",
            poll_options(Duration::from_secs(1), Duration::ZERO),
            true,
            &CancellationToken::new(),
        )
        .await
        .expect_err("later mixed failure must remain failed");

        assert!(error.message.contains("Another component failed"));
        assert_eq!(node.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn nonterminal_critical_refetch_does_not_confirm_identical_image_skip() {
        for state in ["Running", "Unknown"] {
            let node = SequencePollNode {
                statuses: Mutex::new(VecDeque::from([
                    FirmwareTaskStatus {
                        completed: true,
                        state: "Failed".to_owned(),
                        status: "Critical".to_owned(),
                        message: "Component image is identical".to_owned(),
                        rejection: Some(FirmwareTaskRejection::IdenticalImage),
                        ..Default::default()
                    },
                    FirmwareTaskStatus {
                        completed: false,
                        state: state.to_owned(),
                        status: "Critical".to_owned(),
                        message: "Another component failed".to_owned(),
                        ..Default::default()
                    },
                ])),
                calls: AtomicUsize::new(0),
                fail_refetch: false,
            };

            let error = poll_firmware_task_until_complete_with_retry_timeout(
                &node,
                "Task-1",
                poll_options(Duration::from_secs(1), Duration::ZERO),
                true,
                &CancellationToken::new(),
            )
            .await
            .expect_err("a nonterminal critical refetch must not confirm a skip");

            assert!(error.message.contains("Another component failed"));
            assert_eq!(node.calls.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    async fn later_successful_task_response_does_not_confirm_an_identical_image_skip() {
        let node = SequencePollNode {
            statuses: Mutex::new(VecDeque::from([
                FirmwareTaskStatus {
                    completed: true,
                    state: "Failed".to_owned(),
                    status: "Critical".to_owned(),
                    message: "The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'.".to_owned(),
                    rejection: Some(FirmwareTaskRejection::IdenticalImage),
                    ..Default::default()
                },
                FirmwareTaskStatus {
                    completed: true,
                    state: "Completed".to_owned(),
                    status: "OK".to_owned(),
                    message: "Another component was updated.".to_owned(),
                    ..Default::default()
                },
            ])),
            calls: AtomicUsize::new(0),
            fail_refetch: false,
        };

        let error = poll_firmware_task_until_complete_with_retry_timeout(
            &node,
            "Task-1",
            poll_options(Duration::from_secs(1), Duration::ZERO),
            true,
            &CancellationToken::new(),
        )
        .await
        .expect_err("a later success response must not turn the initial failure into a skip");

        assert!(error.message.contains("Component image is identical"));
        assert_eq!(node.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_refetch_does_not_confirm_an_identical_image_skip() {
        let node = SequencePollNode {
            statuses: Mutex::new(VecDeque::from([FirmwareTaskStatus {
                completed: true,
                state: "Failed".to_owned(),
                status: "Critical".to_owned(),
                message: "The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'.".to_owned(),
                rejection: Some(FirmwareTaskRejection::IdenticalImage),
                ..Default::default()
            }])),
            calls: AtomicUsize::new(0),
            fail_refetch: true,
        };

        let error = poll_firmware_task_until_complete_with_retry_timeout(
            &node,
            "Task-1",
            poll_options(Duration::from_secs(1), Duration::ZERO),
            true,
            &CancellationToken::new(),
        )
        .await
        .expect_err("an unconfirmed rejection must remain a task failure");

        assert!(error.message.contains("Component image is identical"));
        assert_eq!(node.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_during_identical_image_refetch_prevents_skip() {
        let mut node = OutcomeNode::new(FirmwareUpdateOutcome::Started(
            crate::domain::node::FirmwareTaskHandle {
                task_id: "Task-1".to_owned(),
            },
        ));

        node.poll_status = FirmwareTaskStatus {
            completed: true,
            state: "Failed".to_owned(),
            status: "Critical".to_owned(),
            message: "The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'.".to_owned(),
            rejection: Some(FirmwareTaskRejection::IdenticalImage),
            ..Default::default()
        };

        let cancel = CancellationToken::new();
        let target = firmware_target();

        let update = firmware_update_outcome_until_done(
            &node,
            &target,
            FirmwareUpdateOutcome::Started(crate::domain::node::FirmwareTaskHandle {
                task_id: "Task-1".to_owned(),
            }),
            false,
            poll_options(Duration::from_secs(60), Duration::from_secs(60)),
            None,
            &cancel,
        );

        tokio::pin!(update);

        assert!(futures::poll!(&mut update).is_pending());

        cancel.cancel();

        let error = update.await.expect_err("cancellation must prevent a skip");

        let FirmwareTargetUpdateError::Poll(error) = error else {
            panic!("cancellation must be reported as a polling error");
        };

        assert_eq!(error.code, ErrorCode::Cancelled);
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_during_non_identical_refetch_preserves_device_failure() {
        let node = StaticPollNode {
            status: FirmwareTaskStatus {
                completed: true,
                state: "Failed".to_owned(),
                status: "Critical".to_owned(),
                message: "Firmware image verification failed".to_owned(),
                ..Default::default()
            },
        };

        let cancel = CancellationToken::new();

        let poll = poll_firmware_task_until_complete_with_retry_timeout(
            &node,
            "Task-1",
            poll_options(Duration::from_secs(60), Duration::from_secs(60)),
            true,
            &cancel,
        );

        tokio::pin!(poll);

        assert!(futures::poll!(&mut poll).is_pending());

        cancel.cancel();

        let error = poll
            .await
            .expect_err("terminal device failure must remain visible");

        assert_eq!(error.code, ErrorCode::Internal);
        assert!(error.message.contains("Firmware image verification failed"));
    }

    fn liteon_pmc_target() -> FirmwareTarget {
        FirmwareTarget {
            component: String::new(),
            firmware_file: "/tmp/cm14mp3rd_2.3.4.fwpkg".to_owned(),
            expected_version: Some("2.3.4".to_owned()),
            resolution: None,
            linkx_policy: None,
        }
    }

    fn pmc_inventory(version: &str) -> Result<Vec<FirmwareInfo>> {
        let item = |name: &str, version: &str| FirmwareInfo {
            name: name.to_owned(),
            version: version.to_owned(),
            firmware_type: crate::domain::node::FirmwareType::Unknown,
            updateable: false,
            target: format!("/redfish/v1/UpdateService/FirmwareInventory/{name}"),
            health: "OK".to_owned(),
            state: "Enabled".to_owned(),
            sku: String::new(),
        };
        Ok(vec![
            item("FW_PSU_1", "0x6 / 0x6"),
            item("FW_PMC_0", version),
        ])
    }

    /// Short recovery window; the first poll loop uses the same transient budget.
    fn pmc_task_loss_poll_options() -> FirmwareTaskPollOptions {
        FirmwareTaskPollOptions {
            timeout: Duration::from_secs(30),
            interval: Duration::from_millis(5),
            transient_error_timeout: Duration::from_millis(300),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_waits_out_the_restart_then_accepts_the_manifest_version() {
        // Observed on hardware: the PMC rejects requests for ~13 minutes, comes
        // back with the task gone (404), and reports the new version.
        let node = LiteonPmcTaskLossNode::new(vec![
            Err(RmsError::unavailable("PMC returned HTTP 401")),
            pmc_inventory("2.3.3"),
            pmc_inventory("2.3.4"),
        ]);

        let disposition = update_firmware_target_until_done(
            &node,
            &liteon_pmc_target(),
            true,
            pmc_task_loss_poll_options(),
        )
        .await
        .expect("a matching installed PMC version should recover the lost task");

        let FirmwareTargetDisposition::Applied {
            summary: Some(summary),
        } = disposition
        else {
            panic!("task-loss recovery should return an applied summary");
        };
        assert_eq!(summary.task_ids, ["0"]);
        assert_eq!(
            summary.details["recovery"],
            "liteon_vr_pmc_task_loss_firmware_manifest_version_match"
        );
        assert_eq!(summary.details["installed_version"], "2.3.4");
        assert_eq!(summary.details["inventory_item"], "FW_PMC_0");
        assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_matches_versions_like_compare_firmware_object() {
        // CompareFirmwareObject trims and ignores ASCII case; so does recovery.
        let node = LiteonPmcTaskLossNode::new(vec![pmc_inventory(" 2.3.4-rc1 ")]);
        let mut target = liteon_pmc_target();
        target.expected_version = Some("2.3.4-RC1".to_owned());

        let disposition =
            update_firmware_target_until_done(&node, &target, true, pmc_task_loss_poll_options())
                .await
                .expect("versions that differ only by case or padding should match");
        assert!(matches!(
            disposition,
            FirmwareTargetDisposition::Applied { summary: Some(_) }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_fails_when_the_pmc_never_reports_the_manifest_version() {
        let node = LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.3")]);

        let error = update_firmware_target_until_done(
            &node,
            &liteon_pmc_target(),
            true,
            pmc_task_loss_poll_options(),
        )
        .await
        .expect_err("an unchanged PMC version must not recover the lost task");

        let FirmwareTargetUpdateError::Poll(error) = error else {
            panic!("task-loss version mismatch should be a poll failure");
        };
        assert!(error.message.contains("FW_PMC_0 reports version 2.3.3"));
        assert!(error.message.contains("2.3.4"));
        assert!(node.inventory_calls.load(Ordering::SeqCst) > 1);
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_fails_when_inventory_stays_unreadable() {
        let node = LiteonPmcTaskLossNode::new(vec![Err(RmsError::unavailable("HTTP 401"))]);

        let error = update_firmware_target_until_done(
            &node,
            &liteon_pmc_target(),
            true,
            pmc_task_loss_poll_options(),
        )
        .await
        .expect_err("an unreadable inventory must not recover the lost task");

        let FirmwareTargetUpdateError::Poll(error) = error else {
            panic!("unreadable inventory should be a poll failure");
        };
        assert!(error.message.contains("firmware inventory read failed"));
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_preserves_the_last_observation_when_the_budget_expires() {
        for (inventory, expected_observation) in [
            (pmc_inventory("2.3.3"), "FW_PMC_0 reports version 2.3.3"),
            (
                Err(RmsError::unavailable("HTTP 401")),
                "firmware inventory read failed: HTTP 401",
            ),
            (Ok(Vec::new()), "no FW_PMC entry in the firmware inventory"),
        ] {
            let node = LiteonPmcTaskLossNode::new(vec![inventory])
                .with_read_delay(Duration::from_millis(10));

            let options = FirmwareTaskPollOptions {
                timeout: Duration::from_secs(30),
                interval: Duration::from_secs(1),
                transient_error_timeout: Duration::from_millis(100),
            };

            let task_deadline = tokio::time::Instant::now() + options.timeout;

            let error = recover_liteon_vr_pmc_update_after_task_loss(
                &node,
                &liteon_pmc_target(),
                "0",
                &RmsError::internal("firmware task disappeared"),
                options,
                task_deadline,
                &CancellationToken::new(),
            )
            .await
            .expect_err("an unverified PMC version must fail recovery");

            assert!(
                error.message.contains(expected_observation),
                "unexpected recovery diagnostic: {}",
                error.message
            );

            assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_does_not_read_inventory_with_an_expired_budget() {
        let now = tokio::time::Instant::now();

        for (recovery_budget, task_deadline) in [
            (Duration::ZERO, now + Duration::from_secs(30)),
            (Duration::from_millis(100), now),
            (Duration::from_millis(100), now - Duration::from_millis(1)),
        ] {
            let node = LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.4")]);

            let options = FirmwareTaskPollOptions {
                timeout: Duration::from_secs(30),
                interval: Duration::from_millis(5),
                transient_error_timeout: recovery_budget,
            };

            let error = recover_liteon_vr_pmc_update_after_task_loss(
                &node,
                &liteon_pmc_target(),
                "0",
                &RmsError::internal("firmware task disappeared"),
                options,
                task_deadline,
                &CancellationToken::new(),
            )
            .await
            .expect_err("an expired budget must not accept a matching version");

            assert!(error.message.contains("budget expired before any read"));
            assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_does_not_start_a_deferred_read_after_expiry() {
        let node = LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.4")]);

        let options = FirmwareTaskPollOptions {
            timeout: Duration::from_secs(30),
            interval: Duration::from_millis(5),
            transient_error_timeout: Duration::from_millis(100),
        };

        let target = liteon_pmc_target();
        let poll_error = RmsError::internal("firmware task disappeared");
        let task_deadline = tokio::time::Instant::now() + options.timeout;
        let cancel = CancellationToken::new();

        let recovery = recover_liteon_vr_pmc_update_after_task_loss(
            &node,
            &target,
            "0",
            &poll_error,
            options,
            task_deadline,
            &cancel,
        );

        tokio::pin!(recovery);

        // Defer the selected read without starting device I/O.
        loop {
            let consume = tokio::task::consume_budget();
            tokio::pin!(consume);

            if futures::poll!(&mut consume).is_pending() {
                break;
            }
        }

        assert!(futures::poll!(&mut recovery).is_pending());
        assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 0);
        tokio::time::advance(options.transient_error_timeout).await;

        let error = recovery
            .await
            .expect_err("expiry must prevent a deferred read");

        assert_eq!(
            node.inventory_calls.load(Ordering::SeqCst),
            0,
            "deferred read started after expiry: {}",
            error.message
        );

        assert!(error.message.contains("budget expired before any read"));
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_accepts_a_match_only_before_the_deadline() {
        for (read_delay_ms, should_succeed, elapsed_ms) in
            [(30, true, 80), (40, false, 100), (50, false, 100)]
        {
            let node =
                LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.3"), pmc_inventory("2.3.4")])
                    .with_read_delay(Duration::from_millis(read_delay_ms));

            let options = FirmwareTaskPollOptions {
                timeout: Duration::from_secs(30),
                interval: Duration::from_millis(20),
                transient_error_timeout: Duration::from_millis(100),
            };

            let started = tokio::time::Instant::now();

            let result = recover_liteon_vr_pmc_update_after_task_loss(
                &node,
                &liteon_pmc_target(),
                "0",
                &RmsError::internal("firmware task disappeared"),
                options,
                started + options.timeout,
                &CancellationToken::new(),
            )
            .await;

            if should_succeed {
                let summary = result.expect("a match before expiry should succeed");

                assert_eq!(summary.details["installed_version"], "2.3.4");
                assert_eq!(summary.task_ids, ["0"]);
            } else {
                let error = result.expect_err("a match at or after expiry must fail");

                assert!(
                    error
                        .message
                        .contains("did not finish within the recovery budget")
                );

                assert!(!error.message.contains("FW_PMC_0 reports version 2.3.3"));
            }

            assert_eq!(started.elapsed(), Duration::from_millis(elapsed_ms));
            assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_cancellation_precedes_a_ready_match_and_deadline() {
        for cancel_at_ms in [10, 100] {
            let node = LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.4")])
                .with_read_delay(Duration::from_millis(10));

            let options = FirmwareTaskPollOptions {
                timeout: Duration::from_secs(30),
                interval: Duration::from_millis(5),
                transient_error_timeout: Duration::from_millis(100),
            };

            let target = liteon_pmc_target();
            let poll_error = RmsError::internal("firmware task disappeared");
            let task_deadline = tokio::time::Instant::now() + options.timeout;
            let cancel = CancellationToken::new();

            let recovery = recover_liteon_vr_pmc_update_after_task_loss(
                &node,
                &target,
                "0",
                &poll_error,
                options,
                task_deadline,
                &cancel,
            );

            tokio::pin!(recovery);

            assert!(futures::poll!(&mut recovery).is_pending());
            tokio::time::advance(Duration::from_millis(cancel_at_ms)).await;
            cancel.cancel();

            let error = recovery
                .await
                .expect_err("cancellation must precede accepting a ready match");

            assert_eq!(error.code, ErrorCode::Cancelled);
            assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 1);
        }
    }

    async fn run_pmc_task_loss_recovery(
        node: &LiteonPmcTaskLossNode,
        options: FirmwareTaskPollOptions,
        cancel: &CancellationToken,
    ) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
        let outcome = FirmwareUpdateOutcome::Started(crate::domain::node::FirmwareTaskHandle {
            task_id: "0".to_owned(),
        });
        firmware_update_outcome_until_done(
            node,
            &liteon_pmc_target(),
            outcome,
            false,
            options,
            None,
            cancel,
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_cancel_interrupts_a_slow_inventory_read() {
        let node = LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.4")])
            .with_read_delay(Duration::from_secs(2));
        // The poll loop gives up after 400ms; recovery then has 400ms more.
        let options = FirmwareTaskPollOptions {
            timeout: Duration::from_secs(30),
            interval: Duration::from_millis(5),
            transient_error_timeout: Duration::from_millis(400),
        };
        let cancel = CancellationToken::new();
        let canceller = cancel.clone();

        let cancellation = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            canceller.cancel();
        });

        let started = tokio::time::Instant::now();

        let error = run_pmc_task_loss_recovery(&node, options, &cancel)
            .await
            .expect_err("a cancelled job must not report the PMC update as applied");

        cancellation.await.expect("cancellation task should finish");

        let FirmwareTargetUpdateError::Poll(error) = error else {
            panic!("cancellation should surface as a poll error");
        };

        assert_eq!(error.code, ErrorCode::Cancelled);
        assert_eq!(started.elapsed(), Duration::from_millis(600));
        assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_does_not_accept_a_match_read_after_the_budget() {
        let node = LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.4")])
            .with_read_delay(Duration::from_secs(2));
        let options = FirmwareTaskPollOptions {
            timeout: Duration::from_secs(30),
            interval: Duration::from_millis(5),
            transient_error_timeout: Duration::from_millis(400),
        };

        let started = tokio::time::Instant::now();

        let error = run_pmc_task_loss_recovery(&node, options, &CancellationToken::new())
            .await
            .expect_err("a read that outlasts the budget must not report success");

        let FirmwareTargetUpdateError::Poll(error) = error else {
            panic!("an exhausted budget should be a poll failure");
        };
        assert!(
            error
                .message
                .contains("did not finish within the recovery budget")
        );

        assert_eq!(started.elapsed(), Duration::from_millis(800));
        assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn liteon_vr_pmc_task_loss_recovery_does_not_outlive_the_task_deadline() {
        // The poll loop gives up because the overall task deadline passed, not
        // because the transient budget did. Recovery must not add a new budget.
        let node = LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.3")]);
        let options = FirmwareTaskPollOptions {
            timeout: Duration::from_millis(150),
            interval: Duration::from_millis(5),
            transient_error_timeout: Duration::from_secs(30),
        };

        let started = tokio::time::Instant::now();

        let error = run_pmc_task_loss_recovery(&node, options, &CancellationToken::new())
            .await
            .expect_err("an old version must not be accepted");

        let FirmwareTargetUpdateError::Poll(error) = error else {
            panic!("a missed version should be a poll failure");
        };

        assert!(error.message.contains("could not be confirmed"));
        assert!(error.message.contains("budget expired before any read"));
        assert_eq!(started.elapsed(), Duration::from_millis(150));
        assert_eq!(node.inventory_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn liteon_vr_pmc_task_loss_recovery_is_limited_to_versioned_vr_targets() {
        let vr = LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.4")]);
        assert!(liteon_vr_pmc_task_loss_recovery_allowed(
            &vr,
            &liteon_pmc_target()
        ));

        // The PSU target carries no manifest version, so it never qualifies.
        let mut psu = liteon_pmc_target();
        psu.expected_version = None;
        assert!(!liteon_vr_pmc_task_loss_recovery_allowed(&vr, &psu));
        psu.expected_version = Some("  ".to_owned());
        assert!(!liteon_vr_pmc_task_loss_recovery_allowed(&vr, &psu));

        for node_type in [
            NodeType::PowershelfGb200Liteon,
            NodeType::PowershelfGb300Liteon,
            NodeType::PowershelfGb200Delta,
            NodeType::ComputeGb300Nvidia,
        ] {
            let other = LiteonPmcTaskLossNode {
                node_type,
                ..LiteonPmcTaskLossNode::new(vec![pmc_inventory("2.3.4")])
            };
            assert!(
                !liteon_vr_pmc_task_loss_recovery_allowed(&other, &liteon_pmc_target()),
                "{node_type}"
            );
        }
    }

    #[tokio::test]
    async fn supermicro_bmc_task_loss_succeeds_when_installed_version_matches_firmware_manifest() {
        let node = SupermicroTaskLossNode {
            version_summary: FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({
                    "status": "matched",
                    "verification": "supermicro_firmware_manifest_version",
                    "components": [{
                        "name": "FW_BMC_0",
                        "package_version": "70.02.01.05",
                        "system_version": "70.02.01.05",
                        "status": "matched"
                    }],
                    "mismatches": []
                }),
            },
            poll_calls: AtomicUsize::new(0),
            version_check_calls: AtomicUsize::new(0),
        };

        let disposition = update_firmware_target_until_done(
            &node,
            &supermicro_bmc_target_with_firmware_manifest_version(),
            false,
            task_loss_poll_options(),
        )
        .await
        .expect("matching installed firmware manifest version should recover the lost BMC task");

        let FirmwareTargetDisposition::Applied {
            summary: Some(summary),
        } = disposition
        else {
            panic!("task-loss recovery should return an applied summary");
        };
        assert_eq!(summary.task_ids, ["0"]);
        assert_eq!(
            summary.details["recovery"],
            "supermicro_bmc_task_loss_firmware_manifest_version_match"
        );
        assert_eq!(summary.details["expected_version"], "70.02.01.05");
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 2);
        assert_eq!(node.version_check_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn supermicro_bmc_task_loss_fails_when_installed_version_mismatches_firmware_manifest() {
        let node = SupermicroTaskLossNode {
            version_summary: FirmwareVersionCheckSummary {
                matched: false,
                details: serde_json::json!({
                    "status": "mismatch",
                    "verification": "supermicro_firmware_manifest_version",
                    "mismatches": [{
                        "name": "FW_BMC_0",
                        "package_version": "70.02.01.05",
                        "system_version": "70.01.00.14"
                    }]
                }),
            },
            poll_calls: AtomicUsize::new(0),
            version_check_calls: AtomicUsize::new(0),
        };

        let error = update_firmware_target_until_done(
            &node,
            &supermicro_bmc_target_with_firmware_manifest_version(),
            false,
            task_loss_poll_options(),
        )
        .await
        .expect_err("mismatched installed version must not recover the lost BMC task");

        let FirmwareTargetUpdateError::Poll(error) = error else {
            panic!("task-loss version mismatch should be a poll failure");
        };
        assert!(error.message.contains("FW_BMC_0"));
        assert!(
            error
                .message
                .contains("expected 70.02.01.05, found 70.01.00.14")
        );
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            node.version_check_calls.load(Ordering::SeqCst),
            POST_ACTIVATION_VERSION_CHECK_ATTEMPTS
        );
    }

    #[tokio::test]
    async fn update_target_outcome_started_updates_job_description_before_polling() {
        let node = OutcomeNode::new(FirmwareUpdateOutcome::Started(
            crate::domain::node::FirmwareTaskHandle {
                task_id: "Task-1".to_owned(),
            },
        ));
        let tracker = JobTracker::new();
        let job_handle = tracker
            .create_job("rack-1", "node-1", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = job_handle.id().to_string();
        let target = firmware_target();

        let disposition = update_firmware_target_until_done_with_progress(
            &node,
            &target,
            false,
            FirmwareTaskPollOptions::default(),
            Some(FirmwareTaskProgress {
                tracker: &tracker,
                job_id: &job_id,
                target: &target,
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("started outcome should update progress and poll");

        assert_eq!(
            disposition,
            FirmwareTargetDisposition::Applied { summary: None }
        );
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 1);
        let job = tracker.get_job(&job_id).expect("job should exist");
        assert_eq!(
            job.state_description,
            "Polling firmware task Task-1 for target BMC"
        );
    }

    #[tokio::test]
    async fn switch_update_updates_job_description_before_nvfwupd_owned_workflow() {
        let node =
            OutcomeNode::new_for_node_type(NodeType::SwitchGb200Nvidia, completed_update_outcome());
        let tracker = JobTracker::new();
        let job_handle = tracker
            .create_job("rack-1", "switch-1", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = job_handle.id().to_string();
        let target = firmware_target();

        let disposition = update_firmware_target_until_done_with_progress(
            &node,
            &target,
            false,
            FirmwareTaskPollOptions::default(),
            Some(FirmwareTaskProgress {
                tracker: &tracker,
                job_id: &job_id,
                target: &target,
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("switch completed outcome should succeed");

        assert!(matches!(
            disposition,
            FirmwareTargetDisposition::Applied { summary: Some(_) }
        ));
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 0);
        let job = tracker.get_job(&job_id).expect("job should exist");
        assert_eq!(
            job.state_description,
            "Running firmware workflow for target BMC"
        );
    }

    #[tokio::test]
    async fn activate_firmware_for_node_uses_platform_activation_mode() {
        for node_type in [
            NodeType::ComputeGb200Nvidia,
            NodeType::ComputeGb300Nvidia,
            NodeType::ComputeGb300Lenovo,
            NodeType::ComputeVrnvl72Nvidia,
            NodeType::SwitchGb200Nvidia,
            NodeType::SwitchGb300Nvidia,
            NodeType::PowershelfGb200Liteon,
            NodeType::PowershelfGb200Delta,
            NodeType::PowershelfGb300Liteon,
            NodeType::PowershelfGb300Delta,
            NodeType::PowershelfVrnvl72Liteon,
            NodeType::PowershelfVrnvl72Delta,
        ] {
            let node = ActivationNode {
                node_type,
                calls: AtomicUsize::new(0),
            };

            activate_firmware_for_node(&node, &CancellationToken::new())
                .await
                .expect("activation should succeed");

            assert_eq!(node.calls.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn firmware_update_options_forward_cancellation_token() {
        // Compute node: no apply_time override, but the cancellation token must
        // be forwarded so nvfwupd's pre-flight cancel check is live rather than
        // a no-op `None`.
        let compute = ActivationNode {
            node_type: NodeType::ComputeGb200Nvidia,
            calls: AtomicUsize::new(0),
        };
        let cancel = CancellationToken::new();
        let options = firmware_update_options_for_node(&compute, &cancel);
        assert!(
            options.apply_time.is_none(),
            "compute nodes must not set apply_time"
        );
        let forwarded = options
            .cancellation
            .expect("cancellation token must be forwarded to nvfwupd");
        // The forwarded token shares state with ours: cancelling the source
        // cancels what nvfwupd sees.
        assert!(!forwarded.is_cancelled());
        cancel.cancel();
        assert!(forwarded.is_cancelled());

        // Powershelf node: still forwards the token AND keeps the OnReset
        // apply_time override.
        let powershelf = ActivationNode {
            node_type: NodeType::PowershelfGb200Liteon,
            calls: AtomicUsize::new(0),
        };
        let options = firmware_update_options_for_node(&powershelf, &CancellationToken::new());
        assert_eq!(options.apply_time.as_deref(), Some("OnReset"));
        assert!(
            options.cancellation.is_some(),
            "powershelf update options must also forward the cancellation token"
        );
    }

    #[tokio::test]
    async fn post_activation_version_check_fails_on_compute_mismatch() {
        let node = VersionCheckNode {
            node_type: NodeType::ComputeGb200Nvidia,
            summary: FirmwareVersionCheckSummary {
                matched: false,
                details: serde_json::json!({
                    "status": "mismatch",
                    "mismatches": [{
                        "name": "FW_BMC_0",
                        "package_version": "1.2.2",
                        "system_version": "1.2.3"
                    }]
                }),
            },
            transient_failures: 0,
            calls: AtomicUsize::new(0),
            not_ready_probes: 0,
            not_ready_error: || {
                RmsError::failed_precondition("NVOS not ready: status=System is ready, health=N/A")
            },
            readiness_calls: AtomicUsize::new(0),
            unreachable_probes: 0,
            unreachable_error: || RmsError::unavailable("HTTP GET /nvue_v1/system returned 503"),
            reachability_calls: AtomicUsize::new(0),
        };

        let err =
            post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
                .await
                .expect_err("mismatch should fail");

        assert!(err.message.contains("FW_BMC_0"));
        assert!(err.message.contains("expected 1.2.2, found 1.2.3"));
        assert_eq!(
            node.calls.load(Ordering::SeqCst),
            POST_ACTIVATION_VERSION_CHECK_ATTEMPTS
        );
    }

    #[tokio::test]
    async fn post_activation_version_check_does_not_require_preflight_completeness() {
        let node = VersionCheckNode {
            node_type: NodeType::SwitchGb200Nvidia,
            summary: FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({
                    "status": "matched",
                    "all_targets_matched": false,
                    "components": [{"name": "BMC", "status": "matched"}],
                    "mismatches": []
                }),
            },
            transient_failures: 0,
            calls: AtomicUsize::new(0),
            not_ready_probes: 0,
            not_ready_error: || {
                RmsError::failed_precondition("NVOS not ready: status=System is ready, health=N/A")
            },
            readiness_calls: AtomicUsize::new(0),
            unreachable_probes: 0,
            unreachable_error: || RmsError::unavailable("HTTP GET /nvue_v1/system returned 503"),
            reachability_calls: AtomicUsize::new(0),
        };
        let summary =
            post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
                .await
                .expect("package completeness must not fail post-activation verification")
                .expect("switch version check should run");

        assert!(summary.matched);
        assert_eq!(summary.details["all_targets_matched"], false);
        assert_eq!(node.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn post_activation_version_check_retries_transient_inventory_failures() {
        let node = VersionCheckNode {
            node_type: NodeType::ComputeGb200Nvidia,
            summary: FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({"status": "matched"}),
            },
            transient_failures: 2,
            calls: AtomicUsize::new(0),
            not_ready_probes: 0,
            not_ready_error: || {
                RmsError::failed_precondition("NVOS not ready: status=System is ready, health=N/A")
            },
            readiness_calls: AtomicUsize::new(0),
            unreachable_probes: 0,
            unreachable_error: || RmsError::unavailable("HTTP GET /nvue_v1/system returned 503"),
            reachability_calls: AtomicUsize::new(0),
        };

        let summary =
            post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
                .await
                .expect("transient inventory failures should retry")
                .expect("version check should run");

        assert!(summary.matched);
        assert_eq!(node.calls.load(Ordering::SeqCst), 3);
    }

    fn matched_switch_node(not_ready_probes: usize) -> VersionCheckNode {
        VersionCheckNode {
            node_type: NodeType::SwitchVrnvl72Nvidia,
            summary: FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({"status": "matched"}),
            },
            transient_failures: 0,
            calls: AtomicUsize::new(0),
            not_ready_probes,
            not_ready_error: || {
                RmsError::failed_precondition("NVOS not ready: status=System is ready, health=N/A")
            },
            readiness_calls: AtomicUsize::new(0),
            unreachable_probes: 0,
            unreachable_error: || RmsError::unavailable("HTTP GET /nvue_v1/system returned 503"),
            reachability_calls: AtomicUsize::new(0),
        }
    }

    #[tokio::test]
    async fn post_activation_version_check_waits_for_switch_readiness() {
        let node = matched_switch_node(1);

        post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
            .await
            .expect("versions match and the switch becomes ready")
            .expect("version check should run");

        assert_eq!(node.readiness_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn post_activation_version_check_waits_for_the_switch_api_before_readiness() {
        // A BMC-activated switch: versions match while NVOS still boots.
        let mut node = matched_switch_node(0);
        node.unreachable_probes = 2;

        post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
            .await
            .expect("NVUE comes back within the budget")
            .expect("version check should run");

        assert_eq!(node.reachability_calls.load(Ordering::SeqCst), 3);
        assert_eq!(node.readiness_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn post_activation_version_check_retries_any_http_error_while_nvue_boots() {
        // A proxy in front of NVUE answers 502 until the service is up.
        let mut node = matched_switch_node(0);
        node.unreachable_probes = 1;
        node.unreachable_error = || RmsError::internal("HTTP GET /nvue_v1/system returned 502");

        post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
            .await
            .expect("a 502 during boot is retried")
            .expect("version check should run");

        assert_eq!(node.reachability_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn post_activation_version_check_stops_on_a_failed_boot() {
        // The readiness probe answers with the terminal code: no retry.
        let mut node = matched_switch_node(0);
        node.not_ready_probes = usize::MAX;
        node.not_ready_error = || {
            RmsError::aborted(
                "NVOS reports a failed boot: status=System is not ready (failed), health=OK",
            )
        };

        let err =
            post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
                .await
                .expect_err("a failed boot fails the job at once");

        assert_eq!(err.code, ErrorCode::Aborted);
        assert_eq!(node.readiness_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn post_activation_version_check_retries_an_http_error_from_the_readiness_read() {
        // NVUE answered once, then a 502 arrives before the ready body.
        let mut node = matched_switch_node(1);
        node.not_ready_error = || RmsError::internal("HTTP GET /nvue_v1/system returned 502");

        post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
            .await
            .expect("a 502 on the readiness read is retried")
            .expect("version check should run");

        assert_eq!(node.readiness_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn post_activation_version_check_fails_when_the_switch_never_becomes_ready() {
        let node = matched_switch_node(usize::MAX);

        let err =
            post_activation_version_check(&node, &[firmware_target()], &CancellationToken::new())
                .await
                .expect_err("versions match but the switch never reports ready");

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("health=N/A"), "{}", err.message);
        assert_eq!(
            node.readiness_calls.load(Ordering::SeqCst),
            POST_ACTIVATION_VERSION_CHECK_ATTEMPTS
        );
    }

    struct CancelDuringVerifyNode {
        node_type: NodeType,
        cancel: CancellationToken,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Node for CancelDuringVerifyNode {
        fn id(&self) -> &str {
            "cancel-during-verify-node"
        }
        fn rack_id(&self) -> &str {
            "cancel-during-verify-rack"
        }
        fn node_type(&self) -> NodeType {
            self.node_type
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn verify_firmware_package_versions(
            &self,
            _targets: &[FirmwareTarget],
        ) -> Result<FirmwareVersionCheckSummary> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // Simulate a cancellation (e.g. graceful shutdown) arriving while the
            // post-activation verification loop is running.
            self.cancel.cancel();
            Ok(FirmwareVersionCheckSummary {
                matched: false,
                details: serde_json::json!({"status": "mismatch", "mismatches": []}),
            })
        }
    }

    #[tokio::test]
    async fn post_activation_version_check_stops_on_cancellation_instead_of_retrying() {
        let cancel = CancellationToken::new();
        let node = CancelDuringVerifyNode {
            node_type: NodeType::ComputeGb200Nvidia,
            cancel: cancel.clone(),
            calls: AtomicUsize::new(0),
        };

        let err = post_activation_version_check(&node, &[firmware_target()], &cancel)
            .await
            .expect_err("cancellation should abort the verification loop");

        assert_eq!(err.code, ErrorCode::Cancelled);
        // The loop bailed after the first verify rather than exhausting all
        // POST_ACTIVATION_VERSION_CHECK_ATTEMPTS retries.
        assert_eq!(node.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn post_activation_version_check_skips_powershelf_nodes() {
        for node_type in [
            NodeType::PowershelfGb200Liteon,
            NodeType::PowershelfGb200Delta,
            NodeType::PowershelfGb300Liteon,
            NodeType::PowershelfGb300Delta,
            NodeType::PowershelfVrnvl72Liteon,
            NodeType::PowershelfVrnvl72Delta,
        ] {
            let node = OutcomeNode::new_for_node_type(node_type, completed_update_outcome());

            let summary = post_activation_version_check(
                &node,
                &[firmware_target()],
                &CancellationToken::new(),
            )
            .await
            .expect("powershelf should not run post-activation version check");

            assert!(summary.is_none(), "node type {node_type}");
        }
    }

    /// The n-th reply of a scripted readiness probe.
    type ScriptedAnswer = fn(usize) -> Result<()>;

    /// Readiness probe scripted by call number: `answer(n)` is the n-th reply.
    struct ScriptedProbe {
        calls: AtomicUsize,
        answer: ScriptedAnswer,
    }

    impl ScriptedProbe {
        fn new(answer: ScriptedAnswer) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                answer,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl ScriptedProbe {
        async fn probe_system_ready(&self) -> Result<()> {
            (self.answer)(self.calls.fetch_add(1, Ordering::SeqCst))
        }
    }

    #[tokio::test]
    async fn wait_for_system_ready_polls_until_the_node_reports_ready() {
        let probe = ScriptedProbe::new(|call| {
            if call < 2 {
                Err(RmsError::failed_precondition(
                    "NVOS not ready: status=System is booting, health=OK",
                ))
            } else {
                Ok(())
            }
        });

        wait_for_system_ready(
            || probe.probe_system_ready(),
            &CancellationToken::new(),
            None,
        )
        .await
        .expect("node became ready within the budget");

        assert_eq!(probe.calls(), 3);
    }

    #[tokio::test]
    async fn wait_for_system_ready_returns_the_last_not_ready_error_after_the_budget() {
        let probe = ScriptedProbe::new(|_| {
            Err(RmsError::failed_precondition(
                "NVOS not ready: status=System is ready, health=Not OK",
            ))
        });

        let err = wait_for_system_ready(
            || probe.probe_system_ready(),
            &CancellationToken::new(),
            None,
        )
        .await
        .expect_err("a node that never becomes ready fails the check");

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("Not OK"));
        assert_eq!(probe.calls(), POST_ACTIVATION_VERSION_CHECK_ATTEMPTS);
    }

    #[tokio::test]
    async fn wait_for_system_ready_takes_its_budget_from_the_stage_policy() {
        let probe = ScriptedProbe::new(|_| {
            Err(RmsError::failed_precondition(
                "NVOS not ready: status=System is ready, health=N/A",
            ))
        });
        let two_quick_attempts = VerificationPolicy::required(
            Duration::ZERO,
            std::num::NonZeroU32::new(2).unwrap(),
            Duration::ZERO,
        );

        let err = wait_for_system_ready(
            || probe.probe_system_ready(),
            &CancellationToken::new(),
            Some(two_quick_attempts),
        )
        .await
        .expect_err("the policy's two attempts run out");

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert_eq!(probe.calls(), 2);
    }

    #[tokio::test]
    async fn wait_for_system_ready_retries_a_rebooting_node_until_it_answers() {
        // What NVUE returns while NVOS initializes: HTTP 503, mapped to Unavailable.
        let probe = ScriptedProbe::new(|call| {
            if call == 0 {
                Err(RmsError::unavailable(
                    "HTTP GET /nvue_v1/system returned 503",
                ))
            } else {
                Ok(())
            }
        });

        wait_for_system_ready(
            || probe.probe_system_ready(),
            &CancellationToken::new(),
            None,
        )
        .await
        .expect("a transport error during the reboot is retried");

        assert_eq!(probe.calls(), 2);
    }

    #[tokio::test]
    async fn wait_for_system_ready_retries_an_answer_it_cannot_decode() {
        // A health word outside the known set fails the decode as `Internal`.
        let probe = ScriptedProbe::new(|_| {
            Err(RmsError::internal(
                "invalid NVUE system response: unknown variant `DEGRADED`",
            ))
        });

        let err = wait_for_system_ready(
            || probe.probe_system_ready(),
            &CancellationToken::new(),
            None,
        )
        .await
        .expect_err("an answer that never decodes fails once the budget runs out");

        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(probe.calls(), POST_ACTIVATION_VERSION_CHECK_ATTEMPTS);
    }

    #[tokio::test]
    async fn wait_for_system_ready_stops_at_once_on_errors_waiting_cannot_fix() {
        // A failed boot, and a node without a management API endpoint.
        let terminal: [(ErrorCode, ScriptedAnswer); 2] = [
            (ErrorCode::Aborted, |_| {
                Err(RmsError::aborted(
                    "NVOS reports a failed boot: status=System is not ready (failed), health=OK",
                ))
            }),
            (ErrorCode::InvalidArgument, |_| {
                Err(RmsError::invalid_argument(
                    "node has no management API endpoint",
                ))
            }),
        ];

        for (code, answer) in terminal {
            let probe = ScriptedProbe::new(answer);

            let err = wait_for_system_ready(
                || probe.probe_system_ready(),
                &CancellationToken::new(),
                None,
            )
            .await
            .expect_err("an error waiting cannot fix is not retried");

            assert_eq!(err.code, code);
            assert_eq!(probe.calls(), 1, "{code:?}");
        }
    }

    #[tokio::test]
    async fn wait_for_system_ready_returns_on_the_first_ready_probe() {
        let probe = ScriptedProbe::new(|_| Ok(()));

        wait_for_system_ready(
            || probe.probe_system_ready(),
            &CancellationToken::new(),
            None,
        )
        .await
        .expect("a ready node passes at once");

        assert_eq!(probe.calls(), 1);
    }

    #[test]
    fn resolve_firmware_file_returns_canonical_path_inside_firmware_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_dir = temp_dir.path().join("firmware");
        std::fs::create_dir(&firmware_dir).unwrap();
        let firmware_file = firmware_dir.join("bmc.bin");
        std::fs::write(&firmware_file, b"firmware").unwrap();

        let resolved = resolve_firmware_file("bmc.bin", &firmware_dir).unwrap();

        assert_eq!(resolved, firmware_file.canonicalize().unwrap());
    }

    #[test]
    fn resolve_firmware_file_rejects_parent_traversal() {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_dir = temp_dir.path().join("firmware");
        std::fs::create_dir(&firmware_dir).unwrap();
        let outside_file = temp_dir.path().join("outside.bin");
        std::fs::write(&outside_file, b"firmware").unwrap();

        let err = resolve_firmware_file("../outside.bin", &firmware_dir).unwrap_err();

        assert!(err.contains("outside firmware directory"));
    }

    #[test]
    fn resolve_firmware_file_rejects_absolute_path_outside_firmware_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_dir = temp_dir.path().join("firmware");
        std::fs::create_dir(&firmware_dir).unwrap();
        let outside_file = temp_dir.path().join("outside.bin");
        std::fs::write(&outside_file, b"firmware").unwrap();

        let err = resolve_firmware_file(outside_file.to_str().unwrap(), &firmware_dir).unwrap_err();

        assert!(err.contains("outside firmware directory"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_firmware_file_rejects_symlink_escape() {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_dir = temp_dir.path().join("firmware");
        std::fs::create_dir(&firmware_dir).unwrap();
        let outside_file = temp_dir.path().join("outside.bin");
        std::fs::write(&outside_file, b"firmware").unwrap();
        let symlink = firmware_dir.join("link.bin");
        std::os::unix::fs::symlink(&outside_file, &symlink).unwrap();

        let err = resolve_firmware_file("link.bin", &firmware_dir).unwrap_err();

        assert!(err.contains("outside firmware directory"));
    }

    #[test]
    fn build_firmware_targets_by_type_preserves_resolved_types() -> TestResult {
        use crate::api::grpc::conversions::domain_node_type_to_proto;
        use crate::api::grpc::node_type_resolver::domain_node_type_to_descriptor;

        let firmware_dir = tempfile::tempdir()?;
        std::fs::write(firmware_dir.path().join("hmc.fwpkg"), b"hmc")?;
        std::fs::write(firmware_dir.path().join("bmc.fwpkg"), b"bmc")?;

        let hmc_path = firmware_dir.path().join("hmc.fwpkg").canonicalize()?;
        let bmc_path = firmware_dir.path().join("bmc.fwpkg").canonicalize()?;
        let hmc_path = hmc_path.to_string_lossy();
        let bmc_path = bmc_path.to_string_lossy();

        for (node_type, components) in [
            (NodeType::ComputeGb200Nvidia, ["HMC", "BMC"]),
            (NodeType::ComputeGb200Wiwynn, ["HMC", "BMC"]),
            (NodeType::ComputeGb200Dell, ["HMC", "BMC"]),
            (NodeType::ComputeGb300Supermicro, ["HMC", "BMC"]),
            (NodeType::PowershelfVrnvl72Liteon, ["", ""]),
            (NodeType::PowershelfVrnvl72Delta, ["", ""]),
        ] {
            for multi_target in [false, true] {
                let request = rm::BatchUpdateFirmwareByNodeTypeRequest {
                    node_type: domain_node_type_to_proto(node_type) as i32,
                    node_descriptor: Some(domain_node_type_to_descriptor(node_type)),
                    filename: if multi_target {
                        "missing.fwpkg"
                    } else {
                        "hmc.fwpkg"
                    }
                    .into(),
                    target: "HMC".into(),
                    firmware_targets: if multi_target {
                        vec![
                            rm::FirmwareTarget {
                                filename: "hmc.fwpkg".into(),
                                target: "HMC".into(),
                            },
                            rm::FirmwareTarget {
                                filename: "bmc.fwpkg".into(),
                                target: "BMC".into(),
                            },
                        ]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                };

                let resolved =
                    resolve_node_type(Some(request.node_type), request.node_descriptor.as_ref())?;

                let expected = if multi_target {
                    vec![
                        (components[0], hmc_path.as_ref()),
                        (components[1], bmc_path.as_ref()),
                    ]
                } else {
                    vec![(components[0], hmc_path.as_ref())]
                };

                let targets =
                    build_firmware_targets_by_type(&request, firmware_dir.path(), resolved)?;

                let actual: Vec<_> = targets
                    .iter()
                    .map(|target| (target.component.as_str(), target.firmware_file.as_str()))
                    .collect();

                assert_eq!(actual, expected, "{node_type}, multi_target={multi_target}");
            }
        }

        Ok(())
    }

    #[test]
    fn build_firmware_targets_from_list_allows_empty_targets_for_all_node_types() {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_dir = temp_dir.path().join("firmware");
        std::fs::create_dir(&firmware_dir).unwrap();
        let firmware_file = firmware_dir.join("bmc.bin");
        std::fs::write(&firmware_file, b"firmware").unwrap();
        let target_list = vec![rm::FirmwareTarget {
            target: String::new(),
            filename: "bmc.bin".into(),
        }];

        for node_type in [
            NodeType::ComputeGb200Nvidia,
            NodeType::ComputeGb300Nvidia,
            NodeType::ComputeGb300Lenovo,
            NodeType::ComputeVrnvl72Nvidia,
            NodeType::SwitchGb200Nvidia,
            NodeType::SwitchGb300Nvidia,
            NodeType::PowershelfGb200Liteon,
            NodeType::PowershelfGb200Delta,
            NodeType::PowershelfGb300Liteon,
            NodeType::PowershelfGb300Delta,
            NodeType::PowershelfVrnvl72Liteon,
            NodeType::PowershelfVrnvl72Delta,
        ] {
            let targets =
                build_firmware_targets_from_list(node_type, &target_list, &firmware_dir).unwrap();

            assert_eq!(targets.len(), 1);
            assert!(targets[0].component.is_empty());
            assert_eq!(
                targets[0].firmware_file,
                firmware_file
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            );
        }
    }

    #[test]
    fn build_firmware_targets_from_list_drops_powershelf_target_labels() {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_dir = temp_dir.path().join("firmware");
        std::fs::create_dir(&firmware_dir).unwrap();
        let firmware_file = firmware_dir.join("psu.bin");
        std::fs::write(&firmware_file, b"firmware").unwrap();
        let target_list = vec![rm::FirmwareTarget {
            target: "PowerShelfFW".to_owned(),
            filename: "psu.bin".into(),
        }];

        let targets = build_firmware_targets_from_list(
            NodeType::PowershelfGb200Liteon,
            &target_list,
            &firmware_dir,
        )
        .unwrap();

        assert_eq!(targets.len(), 1);
        assert!(targets[0].component.is_empty());
        assert_eq!(
            targets[0].firmware_file,
            firmware_file
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        );
    }

    #[test]
    fn build_firmware_targets_from_list_carries_private_expected_version() {
        let temp_dir = tempfile::tempdir().unwrap();
        let firmware_dir = temp_dir.path().join("firmware");
        std::fs::create_dir(&firmware_dir).unwrap();
        let firmware_file = firmware_dir.join("bmc.bin");
        std::fs::write(&firmware_file, b"firmware").unwrap();
        let target_list = vec![rm::FirmwareTarget {
            target: String::new(),
            filename: "bmc.bin".to_owned(),
        }];
        let versions = HashMap::from([("bmc.bin".to_owned(), "70.02.01.05".to_owned())]);

        let targets = build_firmware_targets_from_list_with_expected_versions(
            NodeType::ComputeGb300Supermicro,
            &target_list,
            &firmware_dir,
            Some(&versions),
        )
        .unwrap();

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].expected_version.as_deref(), Some("70.02.01.05"));
    }

    async fn wait_for_firmware_job_terminal(
        tracker: &JobTracker,
        job_id: &str,
    ) -> crate::orchestrator::job_tracker::JobInfo {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let job = tracker.get_job(job_id).expect("job should exist");
                if matches!(
                    job.state,
                    crate::orchestrator::job_lifecycle::JobState::Completed
                        | crate::orchestrator::job_lifecycle::JobState::Failed
                ) {
                    return job;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("firmware job should become terminal")
    }

    #[tokio::test]
    async fn poll_until_complete_returns_ok_when_task_completes_immediately() {
        let node = CountingPollNode {
            complete_after: 0,
            calls: AtomicUsize::new(0),
            always_fail: false,
        };
        let res = poll_firmware_task_until_complete(
            &node,
            "task-1",
            poll_options(Duration::from_secs(1), Duration::from_millis(10)),
        )
        .await;

        assert!(res.is_ok(), "expected Ok, got {res:?}");
        assert!(matches!(res, Ok(PolledFirmwareTask::Completed(status)) if status.completed));
        assert_eq!(node.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn poll_until_complete_succeeds_after_several_polls() {
        let node = CountingPollNode {
            complete_after: 3,
            calls: AtomicUsize::new(0),
            always_fail: false,
        };
        let res = poll_firmware_task_until_complete(
            &node,
            "task-1",
            poll_options(Duration::from_secs(5), Duration::from_millis(1)),
        )
        .await;
        assert!(res.is_ok(), "expected Ok, got {res:?}");
        assert!(node.calls.load(Ordering::SeqCst) >= 4);
    }

    #[tokio::test]
    async fn poll_until_complete_propagates_poll_error() {
        let node = CountingPollNode {
            complete_after: 0,
            calls: AtomicUsize::new(0),
            always_fail: true,
        };
        let res = poll_firmware_task_until_complete(
            &node,
            "task-1",
            poll_options(Duration::from_secs(1), Duration::from_millis(10)),
        )
        .await;
        let err = res.expect_err("expected Err, got Ok");
        assert!(
            err.message.contains("simulated poll failure"),
            "unexpected message: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn poll_until_complete_retries_transient_nvfwupd_status_errors() {
        let node = TransientPollErrorNode {
            transient_failures: 2,
            calls: AtomicUsize::new(0),
        };

        let res = poll_firmware_task_until_complete(
            &node,
            "1",
            poll_options(Duration::from_secs(1), Duration::from_millis(1)),
        )
        .await;

        assert!(res.is_ok(), "expected Ok, got {res:?}");
        assert_eq!(node.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn poll_until_complete_returns_cancelled_when_token_already_cancelled() {
        // A never-completing task with a 3h timeout would otherwise pin the node.
        let node = CountingPollNode {
            complete_after: usize::MAX,
            calls: AtomicUsize::new(0),
            always_fail: false,
        };
        let cancel = CancellationToken::new();
        cancel.cancel();

        let err = poll_firmware_task_until_complete_with_retry_timeout(
            &node,
            "task-cancel",
            poll_options(Duration::from_secs(3 * 60 * 60), Duration::from_secs(5)),
            false,
            &cancel,
        )
        .await
        .expect_err("expected cancellation error");

        assert_eq!(err.code, ErrorCode::Cancelled);
        assert!(
            err.message.contains("cancelled"),
            "unexpected message: {}",
            err.message
        );
        // The top-of-loop guard fires before the node is ever polled.
        assert_eq!(node.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn poll_until_complete_bails_out_when_cancelled_during_wait() {
        // Interval and timeout are both huge; only cancellation should end this.
        let node = Arc::new(CountingPollNode {
            complete_after: usize::MAX,
            calls: AtomicUsize::new(0),
            always_fail: false,
        });
        let cancel = CancellationToken::new();

        let poll_node = node.clone();
        let poll_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            poll_firmware_task_until_complete_with_retry_timeout(
                poll_node.as_ref(),
                "task-cancel-wait",
                poll_options(
                    Duration::from_secs(3 * 60 * 60),
                    Duration::from_secs(3 * 60 * 60),
                ),
                false,
                &poll_cancel,
            )
            .await
        });

        // Let the worker poll once and settle into the long inter-poll sleep.
        while node.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        cancel.cancel();

        let err = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("cancellation must interrupt the long wait promptly")
            .expect("poll task should not panic")
            .expect_err("expected cancellation error");
        assert_eq!(err.code, ErrorCode::Cancelled);
        // Cancellation broke the wait; no second poll was issued.
        assert_eq!(node.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn poll_until_complete_bounds_transient_nvfwupd_status_errors_by_timeout() {
        let node = TransientPollErrorNode {
            transient_failures: usize::MAX,
            calls: AtomicUsize::new(0),
        };

        let err = poll_firmware_task_until_complete(
            &node,
            "1",
            poll_options(Duration::from_millis(30), Duration::from_millis(5)),
        )
        .await
        .expect_err("persistent transient status errors should eventually fail");

        assert!(
            err.message.contains("failed to retrieve task status"),
            "unexpected message: {}",
            err.message
        );
        assert!(node.calls.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn poll_until_complete_rejects_failed_terminal_states() {
        for status in [
            FirmwareTaskStatus {
                completed: true,
                state: "Failed".to_owned(),
                status: "Critical".to_owned(),
                message: "firmware update failed".to_owned(),
                ..Default::default()
            },
            FirmwareTaskStatus {
                completed: true,
                state: "Cancelled".to_owned(),
                status: "Cancelled".to_owned(),
                message: "cancelled by device".to_owned(),
                ..Default::default()
            },
            FirmwareTaskStatus {
                completed: true,
                state: "Exception".to_owned(),
                status: "Critical".to_owned(),
                message: "Redfish task exception".to_owned(),
                ..Default::default()
            },
            FirmwareTaskStatus {
                completed: true,
                state: "action_failed".to_owned(),
                status: "install failed".to_owned(),
                message: "NVUE action failed".to_owned(),
                ..Default::default()
            },
            FirmwareTaskStatus {
                completed: true,
                percent: 42,
                state: "action_error".to_owned(),
                status: "bad image".to_owned(),
                message: "bad image".to_owned(),
                rejection: None,
            },
            FirmwareTaskStatus {
                completed: true,
                percent: 100,
                state: "Completed".to_owned(),
                status: "Warning".to_owned(),
                message: "The resource property Firmware Update Service has detected errors of type 'No Matching Devices'. Resolution: Verify the FW package has devices that are listed in the Redfish FW Inventory".to_owned(),
                rejection: None,
            },
        ] {
            let node = StaticPollNode { status };
            let err = poll_firmware_task_until_complete(
                &node,
                "Task-Failed",
                poll_options(Duration::from_secs(1), Duration::from_millis(1)),
            )
            .await
            .expect_err("failed terminal state should return an error");

            assert!(
                err.message.contains("ended unsuccessfully"),
                "unexpected error: {}",
                err.message
            );
        }
    }

    #[tokio::test]
    async fn poll_until_complete_refetches_failed_terminal_status_for_late_messages() {
        let node = SequencePollNode {
            statuses: Mutex::new(VecDeque::from([
                FirmwareTaskStatus {
                    completed: true,
                    state: "Failed".to_owned(),
                    status: "Critical".to_owned(),
                    message: "The task with Id '0' has completed with errors.".to_owned(),
                    ..Default::default()
                },
                FirmwareTaskStatus {
                    completed: true,
                    state: "Failed".to_owned(),
                    status: "Critical".to_owned(),
                    message: "The task with Id '0' has completed with errors.; Transfer of image '01.04.0036.0000_n04' to 'FW_ERoT_BMC_0' failed.; The resource property 'FW_BMC_0' has detected errors of type 'ERoT is busy'. Resolution: Wait for background copy operation to complete and rate limit threshold to be cleared.".to_owned(),
                    ..Default::default()
                },
            ])),
            calls: AtomicUsize::new(0),
            fail_refetch: false,
        };

        let err = poll_firmware_task_until_complete(
            &node,
            "0",
            poll_options(Duration::from_secs(1), Duration::from_millis(1)),
        )
        .await
        .expect_err("failed terminal state should return an error");

        assert_eq!(node.calls.load(Ordering::SeqCst), 2);
        assert!(
            err.message.contains("FW_ERoT_BMC_0") && err.message.contains("ERoT is busy"),
            "unexpected error: {}",
            err.message
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refetch_of_failed_terminal_status_does_not_wait_a_whole_poll_interval() {
        // A poll interval raised for quieter logs must not slow failure
        // reporting: the confirming refetch waits the grace, not the interval.
        let node = SequencePollNode {
            statuses: Mutex::new(VecDeque::from([
                FirmwareTaskStatus {
                    completed: true,
                    state: "Failed".to_owned(),
                    status: "Critical".to_owned(),
                    message: "The task with Id '0' has completed with errors.".to_owned(),
                    ..Default::default()
                },
                FirmwareTaskStatus {
                    completed: true,
                    state: "Failed".to_owned(),
                    status: "Critical".to_owned(),
                    message: "The task with Id '0' has completed with errors.; ERoT is busy."
                        .to_owned(),
                    ..Default::default()
                },
            ])),
            calls: AtomicUsize::new(0),
            fail_refetch: false,
        };

        let started_at = tokio::time::Instant::now();
        poll_firmware_task_until_complete(
            &node,
            "0",
            poll_options(Duration::from_secs(3 * 60 * 60), Duration::from_secs(60)),
        )
        .await
        .expect_err("failed terminal state should return an error");

        assert_eq!(node.calls.load(Ordering::SeqCst), 2);
        assert_eq!(started_at.elapsed(), TERMINAL_TASK_REFETCH_GRACE);
    }

    #[tokio::test]
    async fn poll_until_complete_accepts_action_error_only_at_100_percent() {
        let node = StaticPollNode {
            status: FirmwareTaskStatus {
                completed: true,
                percent: 100,
                state: "action_error".to_owned(),
                status: "rebooting to install image".to_owned(),
                message: "Installing image after reboot".to_owned(),
                rejection: None,
            },
        };

        poll_firmware_task_until_complete(
            &node,
            "Task-Reboot-Handoff",
            poll_options(Duration::from_secs(1), Duration::from_millis(1)),
        )
        .await
        .expect("action_error at 100 percent should preserve switch reboot-handoff success");
    }

    #[tokio::test]
    async fn poll_until_complete_does_not_fail_on_diagnostic_message_text() {
        let node = StaticPollNode {
            status: FirmwareTaskStatus {
                completed: true,
                percent: 100,
                state: "Completed".to_owned(),
                status: "OK".to_owned(),
                message: "3 components updated; 2 skipped due to failed pre-checks".to_owned(),
                rejection: None,
            },
        };

        poll_firmware_task_until_complete(
            &node,
            "Task-Completed-With-Diagnostics",
            poll_options(Duration::from_secs(1), Duration::from_millis(1)),
        )
        .await
        .expect("completed OK task should not fail only because message contains failed");
    }

    #[tokio::test]
    async fn poll_until_complete_times_out_when_task_never_completes() {
        let node = CountingPollNode {
            complete_after: usize::MAX,
            calls: AtomicUsize::new(0),
            always_fail: false,
        };
        let res = poll_firmware_task_until_complete(
            &node,
            "task-1",
            poll_options(Duration::from_millis(50), Duration::from_millis(5)),
        )
        .await;
        let err = res.expect_err("expected timeout Err, got Ok");
        assert!(
            err.message.to_lowercase().contains("timeout"),
            "unexpected message: {}",
            err.message
        );
        assert!(node.calls.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn poll_until_complete_does_not_overshoot_timeout_by_a_full_interval() {
        // An interval far longer than the timeout: the loop must clamp its
        // sleep to the deadline instead of waking up an interval past it.
        let node = CountingPollNode {
            complete_after: usize::MAX,
            calls: AtomicUsize::new(0),
            always_fail: false,
        };

        let started_at = std::time::Instant::now();

        let err = poll_firmware_task_until_complete(
            &node,
            "task-1",
            poll_options(Duration::from_millis(20), Duration::from_secs(5)),
        )
        .await
        .expect_err("expected timeout Err, got Ok");

        assert!(
            err.message.to_lowercase().contains("timeout"),
            "unexpected message: {}",
            err.message
        );
        let elapsed = started_at.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "gave up after {elapsed:?}, expected roughly the 20ms timeout"
        );
    }

    // ── BMC Aux Powercycle Recovery ──────────────────────────────────────────

    struct RecoveryMockNode {
        node_type: NodeType,
        /// Outcomes returned by successive `update_firmware` calls.
        update_results: Mutex<VecDeque<Result<FirmwareUpdateOutcome>>>,
        /// Errors returned by `activate_firmware_with`; empty queue → `Ok(())`.
        activation_errors: Mutex<VecDeque<RmsError>>,
        /// Errors returned by `bmc_aux_powercycle`; empty queue → `Ok(())`.
        powercycle_errors: Mutex<VecDeque<RmsError>>,
        /// Power states returned by `get_power_state`; empty queue → `power_state_default`.
        power_states: Mutex<VecDeque<PowerState>>,
        /// Returned by `get_power_state` once the `power_states` queue is exhausted.
        power_state_default: PowerState,
        update_calls: Arc<AtomicUsize>,
        activation_calls: AtomicUsize,
        powercycle_calls: Arc<AtomicUsize>,
    }

    impl RecoveryMockNode {
        fn switch_with_updates(
            updates: impl Into<VecDeque<Result<FirmwareUpdateOutcome>>>,
        ) -> Self {
            Self {
                node_type: NodeType::SwitchGb200Nvidia,
                update_results: Mutex::new(updates.into()),
                activation_errors: Mutex::new(VecDeque::new()),
                powercycle_errors: Mutex::new(VecDeque::new()),
                power_states: Mutex::new(VecDeque::new()),
                power_state_default: PowerState::On,
                update_calls: Arc::new(AtomicUsize::new(0)),
                activation_calls: AtomicUsize::new(0),
                powercycle_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_activation_error(mut self, code: ErrorCode, msg: &str) -> Self {
            self.activation_errors
                .get_mut()
                .unwrap()
                .push_back(RmsError::new(code, msg));
            self
        }

        fn with_powercycle_error(mut self, code: ErrorCode, msg: &str) -> Self {
            self.powercycle_errors
                .get_mut()
                .unwrap()
                .push_back(RmsError::new(code, msg));
            self
        }
    }

    #[async_trait]
    impl Node for RecoveryMockNode {
        fn id(&self) -> &str {
            "recovery-mock-node"
        }
        fn rack_id(&self) -> &str {
            "recovery-mock-rack"
        }
        fn node_type(&self) -> NodeType {
            self.node_type
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::from([("host".to_owned(), "192.0.2.11".to_owned())])
        }

        fn supports_bmc_aux_powercycle(&self) -> bool {
            self.node_type.kind() == NodeKind::Switch
        }

        async fn update_firmware(
            &self,
            _target: &FirmwareTarget,
            _force_update: bool,
            _options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            self.update_calls.fetch_add(1, Ordering::SeqCst);
            self.update_results
                .lock()
                .unwrap()
                .pop_front()
                .expect("update_firmware called more times than expected")
        }

        async fn activate_firmware_with(
            &self,
            _request: FirmwareActivationRequest,
        ) -> Result<FirmwareActivationSummary> {
            self.activation_calls.fetch_add(1, Ordering::SeqCst);
            match self.activation_errors.lock().unwrap().pop_front() {
                Some(e) => Err(e),
                None => Ok(FirmwareActivationSummary {
                    message: "mock activation ok".to_owned(),
                    details: serde_json::json!({}),
                }),
            }
        }

        async fn bmc_aux_powercycle(&self) -> Result<()> {
            self.powercycle_calls.fetch_add(1, Ordering::SeqCst);
            match self.powercycle_errors.lock().unwrap().pop_front() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }

        async fn get_power_state(&self) -> Result<PowerState> {
            Ok(self
                .power_states
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(self.power_state_default))
        }

        async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
            Ok(FirmwareTaskStatus {
                completed: true,
                percent: 100,
                state: "Completed".to_owned(),
                status: "OK".to_owned(),
                ..Default::default()
            })
        }
    }

    fn completed_outcome() -> FirmwareUpdateOutcome {
        FirmwareUpdateOutcome::Completed(FirmwareUpdateSummary {
            message: "updated".to_owned(),
            task_ids: vec![],
            details: serde_json::json!({}),
        })
    }

    fn dummy_target() -> FirmwareTarget {
        FirmwareTarget {
            component: "bmc".to_owned(),
            firmware_file: "/tmp/fw.bin".to_owned(),
            expected_version: None,
            resolution: None,
            linkx_policy: None,
        }
    }

    #[tokio::test]
    async fn firmware_admission_preserves_offline_bmc_recovery_and_invalidates_activation()
    -> TestResult {
        use crate::api::grpc::switch_scope::fabric_scope_keys;
        use crate::orchestrator::fabric_operation::{ControllerIdentity, FabricOperationIntent};
        use crate::orchestrator::job_lifecycle::{JobScope, JobState};

        let firmware_dir = tempfile::tempdir()?;
        std::fs::write(firmware_dir.path().join("bmc.bin"), b"test")?;
        let rack_manager = Arc::new(RackManager::new());
        rack_manager.create_rack("recovery-mock-rack", ProductFamily::Gb200.rack_type())?;

        let rack = rack_manager
            .find_rack("recovery-mock-rack")
            .ok_or("missing rack")?;

        let mock = RecoveryMockNode::switch_with_updates([
            Err(RmsError::connection_refused("NVOS unreachable")),
            Ok(completed_outcome()),
        ]);

        let updates = Arc::clone(&mock.update_calls);
        let powercycles = Arc::clone(&mock.powercycle_calls);

        rack.add_node(
            "recovery-mock-node",
            Arc::new(NodeInstance::from_test_node(mock)),
        )?;

        let mut service = test_service(rack_manager);
        service.firmware_dir = firmware_dir.path().to_owned();
        service.switch_tls_roots.insecure_switch = true;
        let keys = fabric_scope_keys(&[JobScope::endpoint("192.0.2.11", 443)])?;

        let controller = ControllerIdentity {
            chassis_serial_number: "chassis-a".to_owned(),
            tray_index: 0,
        };

        service.fabric_operations.admit(
            FabricOperationIntent {
                scope_keys: keys.clone(),
                rule_id: "rule".to_owned(),
            },
            "completed-layout",
            None,
        )?;

        service
            .fabric_operations
            .mark_pending("completed-layout", std::slice::from_ref(&controller))?;

        service.fabric_operations.record_activation(
            "completed-layout",
            controller.clone(),
            "config".to_owned(),
            keys,
        )?;

        service.fabric_operations.finish("completed-layout")?;

        let response = service
            .handle_update_firmware(tonic::Request::new(rm::UpdateFirmwareRequest {
                rack_id: "recovery-mock-rack".to_owned(),
                node_id: "recovery-mock-node".to_owned(),
                filename: "bmc.bin".to_owned(),
                target: "bmc".to_owned(),
                force_update: true,
                ..Default::default()
            }))
            .await?
            .into_inner();

        assert_eq!(
            response.status,
            rm::ReturnCode::Success as i32,
            "{}",
            response.message
        );

        let completed =
            wait_for_firmware_job_terminal(&service.job_tracker, &response.job_id).await;

        assert_eq!(
            completed.state,
            JobState::Completed,
            "{}",
            completed.error_message
        );

        assert_eq!(updates.load(Ordering::SeqCst), 2);
        assert_eq!(powercycles.load(Ordering::SeqCst), 1);

        assert!(
            !service
                .fabric_operations
                .has_activation_proof(&controller, "config")?
        );

        Ok(())
    }

    #[tokio::test]
    async fn firmware_admission_allows_bmc_recovery_after_failed_identity_discovery() -> TestResult
    {
        use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;
        use crate::orchestrator::job_lifecycle::JobState;

        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        for status in [404, 200] {
            let server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/platform/chassis-location"))
                .respond_with(ResponseTemplate::new(status).set_body_json(serde_json::json!({})))
                .expect(1)
                .mount(&server)
                .await;

            let switch = SwitchGb200Nvidia::for_test(&server.uri());
            let client = switch.optional_nvue_client().unwrap().clone();
            let service = test_service(Arc::new(RackManager::new()));

            let node = RecoveryMockNode::switch_with_updates([
                Err(RmsError::connection_refused("NVOS unreachable")),
                Ok(completed_outcome()),
            ]);

            let updates = Arc::clone(&node.update_calls);
            let powercycles = Arc::clone(&node.powercycle_calls);

            let job = service
                .job_tracker
                .create_job(node.rack_id(), node.id(), JobType::FirmwareUpdate)
                .map_err(|failure| failure.message)?;

            let job_id = job.id().to_string();
            let recovery = service.recovery_dispatch(RecoverySource::Latest);

            spawn_firmware_update_job_with_policy(
                Arc::clone(&service.job_tracker),
                job,
                Arc::new(node),
                vec![dummy_target()],
                false,
                true,
                FirmwareTaskPollOptions::default(),
                false,
                recovery,
                None,
                Some((service.clone(), client)),
            );

            let completed = wait_for_firmware_job_terminal(&service.job_tracker, &job_id).await;

            assert_eq!(
                completed.state,
                JobState::Completed,
                "{}",
                completed.error_message
            );

            assert_eq!(updates.load(Ordering::SeqCst), 2);
            assert_eq!(powercycles.load(Ordering::SeqCst), 1);
            server.verify().await;
        }

        Ok(())
    }

    #[tokio::test]
    async fn firmware_admission_rejects_observed_physical_alias_conflicts() -> TestResult {
        use crate::api::grpc::switch_scope::fabric_scope_keys;
        use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;
        use crate::orchestrator::fabric_operation::FabricOperationIntent;
        use crate::orchestrator::job_lifecycle::{JobFailure, JobScope};

        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        for active in [true, false] {
            let server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/platform/chassis-location"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "chassis-sn": "chassis-a", "tray-index": "2"
                })))
                .expect(1)
                .mount(&server)
                .await;

            let node = NodeInstance::SwitchGb200Nvidia(Box::new(SwitchGb200Nvidia::for_test(
                &server.uri(),
            )));

            let service = test_service(Arc::new(RackManager::new()));

            let scopes = vec![JobScope::PhysicalSwitch {
                chassis_serial_number: "chassis-a".to_owned(),
                tray_index: 2,
            }];

            let owner = service
                .job_tracker
                .create_scoped_job(
                    "other-rack",
                    "other-label",
                    JobType::ConfigureScaleUpFabricManagerV2,
                    scopes.clone(),
                )
                .map_err(|failure| failure.message)?;

            service.fabric_operations.admit(
                FabricOperationIntent {
                    scope_keys: fabric_scope_keys(&scopes)?,
                    rule_id: "rule".to_owned(),
                },
                owner.id().as_ref(),
                None,
            )?;

            if !active {
                owner.fail(JobFailure::new(
                    JobError::Internal,
                    "unresolved configuration",
                ));
            }

            let job = service
                .job_tracker
                .create_job(node.rack_id(), node.id(), JobType::FirmwareUpdate)
                .map_err(|failure| failure.message)?;

            service
                .admit_firmware_node_for_job(&job, &node)
                .map_err(|failure| failure.message)?;

            let job_id = job.id().to_string();

            service.spawn_profiled_firmware_update_job(
                job,
                Arc::new(node),
                vec![dummy_target()],
                false,
                true,
                false,
                service.recovery_dispatch(RecoverySource::Latest),
            );

            let failure = wait_for_firmware_job_terminal(&service.job_tracker, &job_id).await;

            assert_eq!(
                failure.state,
                crate::orchestrator::job_lifecycle::JobState::Failed
            );

            assert_eq!(failure.error_code, JobError::UpdateInProgress);
            assert!(failure.result_json.contains("scope_admission"));

            assert!(
                server
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .all(|request| request.method == wiremock::http::Method::Get)
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn firmware_requests_dispatch_before_slow_identity_discovery() -> TestResult {
        use crate::api::grpc::switch_handlers::tests::factory_reset_test_target;
        use crate::orchestrator::job_lifecycle::{JobScope, JobState};

        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        for batch in [false, true] {
            let server = MockServer::start().await;
            let (_proxy, node) = factory_reset_test_target(&server).await;
            let rack_id = node.rack_id().to_owned();
            let node_id = node.id().to_owned();

            let discovery_started = Arc::new(tokio::sync::Notify::new());
            let started = Arc::clone(&discovery_started);

            Mock::given(method("GET"))
                .and(path("/nvue_v1/platform/chassis-location"))
                .respond_with(move |_: &wiremock::Request| {
                    started.notify_one();

                    ResponseTemplate::new(200)
                        .set_body_json(
                            serde_json::json!({"chassis-sn": "chassis-a", "tray-index": "2"}),
                        )
                        .set_delay(Duration::from_secs(30))
                })
                .expect(1)
                .mount(&server)
                .await;

            let firmware_dir = tempfile::tempdir()?;
            std::fs::write(firmware_dir.path().join("bmc.bin"), b"test")?;
            let rack_manager = Arc::new(RackManager::new());
            rack_manager.create_rack(&rack_id, ProductFamily::Gb200.rack_type())?;

            rack_manager
                .find_rack(&rack_id)
                .ok_or("missing rack")?
                .add_node(&node_id, Arc::new(node))?;

            let mut service = test_service(rack_manager);
            service.firmware_dir = firmware_dir.path().to_owned();
            service.switch_tls_roots.insecure_switch = true;

            let dispatch = async {
                if batch {
                    let response = service
                        .handle_batch_update_firmware_by_node_type(tonic::Request::new(
                            rm::BatchUpdateFirmwareByNodeTypeRequest {
                                rack_id: rack_id.clone(),
                                node_type: rm::NodeType::SwitchGb200Nvidia as i32,
                                filename: "bmc.bin".into(),
                                target: "bmc".into(),
                                force_update: true,
                                ..Default::default()
                            },
                        ))
                        .await?
                        .into_inner();

                    Ok::<_, Box<dyn std::error::Error>>(
                        response
                            .jobs
                            .into_iter()
                            .next()
                            .ok_or("missing job")?
                            .job_id,
                    )
                } else {
                    let response = service
                        .handle_update_firmware(tonic::Request::new(rm::UpdateFirmwareRequest {
                            rack_id: rack_id.clone(),
                            node_id: node_id.clone(),
                            filename: "bmc.bin".into(),
                            target: "bmc".into(),
                            force_update: true,
                            ..Default::default()
                        }))
                        .await?
                        .into_inner();

                    assert_eq!(
                        response.status,
                        rm::ReturnCode::Success as i32,
                        "{}",
                        response.message
                    );

                    Ok(response.job_id)
                }
            };

            let job_id = tokio::time::timeout(Duration::from_secs(5), dispatch).await??;
            tokio::time::timeout(Duration::from_secs(5), discovery_started.notified()).await?;

            assert!(
                service
                    .job_tracker
                    .reserve_mutation(vec![JobScope::endpoint("127.0.0.1", 22)])
                    .is_err()
            );

            service
                .job_tracker
                .shutdown_and_await_terminal(Duration::from_secs(2))
                .await;

            let job = service
                .job_tracker
                .get_job(&job_id)
                .ok_or("missing firmware job")?;

            assert_eq!(job.state, JobState::Failed);
            assert_eq!(job.error_code, JobError::Other);

            assert!(
                job.error_message
                    .contains("cancelled during switch identity discovery")
            );

            assert!(
                server
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .all(|request| request.method == wiremock::http::Method::Get)
            );

            assert!(
                service
                    .job_tracker
                    .registry()
                    .non_terminal_job_ids()
                    .is_empty()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn firmware_admission_rejects_active_and_unresolved_fabric_aliases() -> TestResult {
        use crate::api::grpc::switch_scope::fabric_scope_keys;
        use crate::orchestrator::fabric_operation::FabricOperationIntent;
        use crate::orchestrator::job_lifecycle::{JobFailure, JobScope};

        for active in [true, false] {
            let firmware_dir = tempfile::tempdir()?;
            std::fs::write(firmware_dir.path().join("bmc.bin"), b"test")?;
            let rack_manager = Arc::new(RackManager::new());
            rack_manager.create_rack("recovery-mock-rack", ProductFamily::Gb200.rack_type())?;

            let rack = rack_manager
                .find_rack("recovery-mock-rack")
                .ok_or("missing rack")?;

            let mock = RecoveryMockNode::switch_with_updates([]);
            let updates = Arc::clone(&mock.update_calls);
            let powercycles = Arc::clone(&mock.powercycle_calls);

            rack.add_node(
                "recovery-mock-node",
                Arc::new(NodeInstance::from_test_node(mock)),
            )?;

            let mut service = test_service(rack_manager);
            service.firmware_dir = firmware_dir.path().to_owned();
            service.switch_tls_roots.insecure_switch = true;
            let scopes = vec![JobScope::endpoint("192.0.2.11", 443)];

            let owner = service
                .job_tracker
                .create_scoped_job(
                    "other-rack",
                    "other-label",
                    JobType::ConfigureScaleUpFabricManagerV2,
                    scopes.clone(),
                )
                .map_err(|failure| failure.message)?;

            service.fabric_operations.admit(
                FabricOperationIntent {
                    scope_keys: fabric_scope_keys(&scopes)?,
                    rule_id: "rule".to_owned(),
                },
                owner.id().as_ref(),
                None,
            )?;

            if !active {
                owner.fail(JobFailure::new(
                    JobError::Internal,
                    "unresolved configuration",
                ));
            }

            let response = service
                .handle_update_firmware(tonic::Request::new(rm::UpdateFirmwareRequest {
                    rack_id: "recovery-mock-rack".to_owned(),
                    node_id: "recovery-mock-node".to_owned(),
                    filename: "bmc.bin".to_owned(),
                    target: "bmc".to_owned(),
                    force_update: true,
                    ..Default::default()
                }))
                .await?
                .into_inner();

            assert_eq!(response.status, rm::ReturnCode::Failure as i32);

            assert_eq!(
                response.error_code,
                rm::FirmwareUpdateError::UpdateInProgress as i32
            );

            assert_eq!(updates.load(Ordering::SeqCst), 0);
            assert_eq!(powercycles.load(Ordering::SeqCst), 0);
        }

        Ok(())
    }

    // ── update_firmware_target_with_bmc_recovery ─────────────────────────────

    #[tokio::test]
    async fn bmc_recovery_wrapper_passes_through_success_without_powercycle() {
        let node = RecoveryMockNode::switch_with_updates([Ok(completed_outcome())]);
        let result = update_firmware_target_with_bmc_recovery(
            &node,
            &dummy_target(),
            false,
            FirmwareTaskPollOptions::default(),
            None,
            &CancellationToken::new(),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn bmc_recovery_wrapper_does_not_recover_non_switch_connection_refused() {
        let mut updates = VecDeque::new();
        updates.push_back(Err(RmsError::connection_refused("simulated")));
        let node = RecoveryMockNode {
            node_type: NodeType::ComputeGb200Nvidia,
            update_results: Mutex::new(updates),
            activation_errors: Mutex::new(VecDeque::new()),
            powercycle_errors: Mutex::new(VecDeque::new()),
            power_states: Mutex::new(VecDeque::new()),
            power_state_default: PowerState::On,
            update_calls: Arc::new(AtomicUsize::new(0)),
            activation_calls: AtomicUsize::new(0),
            powercycle_calls: Arc::new(AtomicUsize::new(0)),
        };
        let err = update_firmware_target_with_bmc_recovery(
            &node,
            &dummy_target(),
            false,
            FirmwareTaskPollOptions::default(),
            None,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        let FirmwareTargetUpdateError::Update(e) = err else {
            panic!("expected Update error");
        };
        assert_eq!(e.code, ErrorCode::ConnectionRefused);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn bmc_recovery_wrapper_retries_and_succeeds_after_powercycle() {
        let updates: VecDeque<_> = [
            Err(RmsError::connection_refused("NVOS unreachable")),
            Ok(completed_outcome()),
        ]
        .into();
        let node = RecoveryMockNode::switch_with_updates(updates);
        let result = update_firmware_target_with_bmc_recovery(
            &node,
            &dummy_target(),
            false,
            FirmwareTaskPollOptions::default(),
            None,
            &CancellationToken::new(),
        )
        .await;
        assert!(result.is_ok(), "expected Ok after recovery retry");
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 2);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bmc_recovery_wrapper_annotates_error_after_powercycle_retry_fails() {
        let updates: VecDeque<_> = [
            Err(RmsError::connection_refused("NVOS unreachable")),
            Err(RmsError::connection_refused(
                "still unreachable after reboot",
            )),
        ]
        .into();
        let node = RecoveryMockNode::switch_with_updates(updates);
        let err = update_firmware_target_with_bmc_recovery(
            &node,
            &dummy_target(),
            false,
            FirmwareTaskPollOptions::default(),
            None,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        let FirmwareTargetUpdateError::Update(e) = err else {
            panic!("expected Update error");
        };
        assert!(
            e.message.contains("after BMC aux powercycle recovery"),
            "expected annotation in error message; got: {}",
            e.message
        );
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 2);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bmc_recovery_wrapper_propagates_powercycle_failure() {
        let updates: VecDeque<_> = [Err(RmsError::connection_refused("NVOS unreachable"))].into();
        let node = RecoveryMockNode::switch_with_updates(updates)
            .with_powercycle_error(ErrorCode::FailedPrecondition, "no BMC endpoint");
        let err = update_firmware_target_with_bmc_recovery(
            &node,
            &dummy_target(),
            false,
            FirmwareTaskPollOptions::default(),
            None,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        let FirmwareTargetUpdateError::Update(e) = err else {
            panic!("expected Update error");
        };
        assert!(
            e.message.contains("BMC aux powercycle recovery failed"),
            "unexpected error: {}",
            e.message
        );
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
    }

    // ── activate_firmware_for_node with BMC fallback ─────────────────────────

    #[tokio::test]
    async fn activate_firmware_for_node_succeeds_without_bmc_fallback() {
        let node = RecoveryMockNode::switch_with_updates(VecDeque::new());
        activate_firmware_for_node(&node, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn vrnvl72_activation_preserves_bmc_failure_without_another_power_cycle() {
        for code in [
            ErrorCode::ConnectionRefused,
            ErrorCode::Timeout,
            ErrorCode::Unavailable,
        ] {
            let mut node = RecoveryMockNode::switch_with_updates(VecDeque::new())
                .with_activation_error(code, "BMC recovery failed");
            node.node_type = NodeType::SwitchVrnvl72Nvidia;
            let error = activate_firmware_for_node(&node, &CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(error.message, "BMC recovery failed");
            assert_eq!(
                node.activation_calls.load(Ordering::SeqCst),
                1,
                "activation should be called once for {code:?}"
            );
            assert_eq!(
                node.powercycle_calls.load(Ordering::SeqCst),
                0,
                "BMC powercycle must not be repeated for {code:?}"
            );
        }
    }

    #[tokio::test]
    async fn cancelled_activation_does_not_fall_back_to_bmc() {
        let node = RecoveryMockNode::switch_with_updates(VecDeque::new())
            .with_activation_error(ErrorCode::Unavailable, "NVOS unreachable");
        let cancellation = tokio_util::sync::CancellationToken::new();
        cancellation.cancel();

        let error = activate_firmware_for_node(&node, &cancellation)
            .await
            .expect_err("cancelled activation must preserve its original error");

        assert_eq!(error.code, ErrorCode::Unavailable);
        assert_eq!(error.message, "NVOS unreachable");
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn activate_firmware_for_node_does_not_fall_back_for_non_switch_node() {
        let node = RecoveryMockNode {
            node_type: NodeType::ComputeGb200Nvidia,
            update_results: Mutex::new(VecDeque::new()),
            activation_errors: Mutex::new(
                [RmsError::connection_refused("NVOS unreachable")].into(),
            ),
            powercycle_errors: Mutex::new(VecDeque::new()),
            power_states: Mutex::new(VecDeque::new()),
            power_state_default: PowerState::On,
            update_calls: Arc::new(AtomicUsize::new(0)),
            activation_calls: AtomicUsize::new(0),
            powercycle_calls: Arc::new(AtomicUsize::new(0)),
        };
        let err = activate_firmware_for_node(&node, &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionRefused);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn activate_firmware_for_node_does_not_fall_back_for_non_unreachable_error() {
        let node = RecoveryMockNode::switch_with_updates(VecDeque::new())
            .with_activation_error(ErrorCode::Internal, "nvfwupd task failed");
        let err = activate_firmware_for_node(&node, &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn activate_firmware_for_node_propagates_bmc_fallback_error() {
        let node = RecoveryMockNode::switch_with_updates(VecDeque::new())
            .with_activation_error(ErrorCode::ConnectionRefused, "NVOS unreachable")
            .with_powercycle_error(ErrorCode::FailedPrecondition, "no BMC endpoint");
        let err = activate_firmware_for_node(&node, &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("no BMC endpoint"));
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn gb_switch_activation_retains_bmc_fallback_when_nvos_is_unreachable() {
        for node_type in [NodeType::SwitchGb200Nvidia, NodeType::SwitchGb300Nvidia] {
            let mut node = RecoveryMockNode::switch_with_updates(VecDeque::new())
                .with_activation_error(ErrorCode::Unavailable, "NVOS unreachable");
            node.node_type = node_type;
            activate_firmware_for_node(&node, &CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
            assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
        }
    }
}
