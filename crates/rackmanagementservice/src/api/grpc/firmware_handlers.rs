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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::grpc::conversions::{
    flatten_node_info, proto_node_type_to_domain, timestamp_from_datetime,
};
use crate::api::grpc::firmware_task_util::terminal_firmware_task_error;
use crate::api::grpc::node_type_resolver::{
    resolve_firmware_target_selectors, resolve_node_info, resolve_node_type,
};
use crate::domain::node::{
    ExpectedInventoryPolicy, FirmwareActivationMode, FirmwareActivationRequest, FirmwareTarget,
    FirmwareTaskStatus, FirmwareType, FirmwareUpdateOptions, FirmwareUpdateOutcome,
    FirmwareUpdateSummary, FirmwareVersionCheckSummary, Node, NodeKind, NodeType,
};
use crate::domain::rack::NodeConfig;
use crate::nodes::NodeInstance;
use crate::nodes::compute_gb300_supermicro::{
    SUPERMICRO_BIOS_TARGET, SUPERMICRO_BMC_TARGET, SUPERMICRO_HGX_TARGET, supermicro_bmc_target,
};
use crate::orchestrator::job_lifecycle::JobError;
use crate::orchestrator::job_tracker::{JobTracker, JobType, RmsJobHandle, job_error_of};
use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::insert_json_field;
use librms::protos::rack_manager as rm;

use super::server::{RackManagerServiceImpl, find_node, find_rack};

// Upper bound on how long to poll a single firmware task for completion before
// giving up. Firmware updates can legitimately take tens of minutes on some
// components, so this is intentionally generous; 3 hours is comfortably above
// observed worst-case durations while still preventing indefinite loops when
// the task gets stuck in `running` (e.g., device hang, network partition).
const FIRMWARE_POLL_TIMEOUT: Duration = Duration::from_secs(3 * 60 * 60);
/// Delay between successive polls of an in-flight firmware task.
const FIRMWARE_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// How long to tolerate task-status read failures before surfacing the error.
const FIRMWARE_TRANSIENT_POLL_ERROR_RETRY_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const POST_ACTIVATION_VERSION_CHECK_ATTEMPTS: usize = 5;

pub(crate) type FirmwareTargetExpectedVersions = HashMap<NodeType, HashMap<String, String>>;

#[cfg(not(test))]
mod timing {
    use std::time::Duration;
    pub(super) const POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY: Duration =
        Duration::from_secs(60);
    pub(super) const POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL: Duration =
        Duration::from_secs(60);
}

#[cfg(test)]
mod timing {
    use std::time::Duration;
    pub(super) const POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY: Duration = Duration::ZERO;
    pub(super) const POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL: Duration = Duration::ZERO;
}

use crate::api::grpc::node_recovery::{attempt_bmc_aux_powercycle_recovery, is_unreachable_err};

use timing::{
    POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY, POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL,
};

// Poll `node.poll_firmware_task(task_id)` until completion, returning poll
// errors, timeouts, and failed terminal task states as RMS errors.
#[cfg(test)]
pub(crate) async fn poll_firmware_task_until_complete<N: Node>(
    node: &N,
    task_id: &str,
    timeout: Duration,
    interval: Duration,
) -> Result<FirmwareTaskStatus> {
    poll_firmware_task_until_complete_with_retry_timeout(
        node,
        task_id,
        timeout,
        interval,
        FIRMWARE_TRANSIENT_POLL_ERROR_RETRY_TIMEOUT,
    )
    .await
}

async fn poll_firmware_task_until_complete_with_retry_timeout<N: Node>(
    node: &N,
    task_id: &str,
    timeout: Duration,
    interval: Duration,
    transient_error_retry_timeout: Duration,
) -> Result<FirmwareTaskStatus> {
    let deadline = Instant::now() + timeout;
    let mut transient_poll_error_started_at = None;
    loop {
        tracing::info!(node = %node.id(), interval_secs = interval.as_secs(), timeout_secs = timeout.as_secs(), task_id, "polling firmware task");
        match node.poll_firmware_task(task_id).await {
            Ok(st) if st.completed => {
                if let Some(error) = terminal_firmware_task_error("firmware task", task_id, &st) {
                    return Err(refetch_terminal_firmware_task_error(
                        node, task_id, st, error, interval,
                    )
                    .await);
                }
                return Ok(st);
            }
            Ok(_) => {
                transient_poll_error_started_at = None;
                if Instant::now() >= deadline {
                    tracing::error!(node = %node.id(), timeout_secs = timeout.as_secs(), task_id, "polling firmware task exceeded timeout");
                    return Err(RmsError::internal(format!(
                        "polling firmware task {task_id} exceeded timeout of {}s",
                        timeout.as_secs()
                    )));
                }
                tokio::time::sleep(interval).await;
            }
            Err(e) if is_retryable_firmware_poll_error(&e) => {
                let now = Instant::now();
                let retry_started_at = *transient_poll_error_started_at.get_or_insert(now);
                let retry_deadline = retry_started_at + transient_error_retry_timeout;
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
                if !sleep_for.is_zero() {
                    tokio::time::sleep(sleep_for).await;
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

async fn refetch_terminal_firmware_task_error<N: Node>(
    node: &N,
    task_id: &str,
    initial_status: FirmwareTaskStatus,
    initial_error: RmsError,
    interval: Duration,
) -> RmsError {
    if !interval.is_zero() {
        tokio::time::sleep(interval).await;
    }

    let Ok(refetched_status) = node.poll_firmware_task(task_id).await else {
        return initial_error;
    };
    let Some(refetched_error) =
        terminal_firmware_task_error("firmware task", task_id, &refetched_status)
    else {
        return initial_error;
    };

    if refetched_status.completed && refetched_status.message.len() > initial_status.message.len() {
        refetched_error
    } else {
        initial_error
    }
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

fn firmware_update_options_for_node<N: Node>(node: &N) -> FirmwareUpdateOptions {
    let mut options = FirmwareUpdateOptions::default();
    if node.node_type().kind() == NodeKind::Powershelf {
        options.apply_time = Some("OnReset".to_owned());
    }
    options
}

/// Wraps `update_firmware_target_until_done_with_progress` with a single
/// BMC-aux-powercycle retry for nodes when the host (i.e. NVOS) is unreachable.
/// Relies on concrete node implementations for firmware update and fallback powercycling.
async fn update_firmware_target_with_bmc_recovery<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    force_update: bool,
    progress: Option<FirmwareTaskProgress<'_>>,
) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
    match update_firmware_target_until_done_with_progress(node, target, force_update, progress)
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
                        None,
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
) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
    update_firmware_target_until_done_with_progress(node, target, force_update, None).await
}

/// Wraps `node.update_firmware` with progress tracking and firmware task polling.
/// Relies on concrete node implementation for firmware update.
async fn update_firmware_target_until_done_with_progress<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    force_update: bool,
    progress: Option<FirmwareTaskProgress<'_>>,
) -> std::result::Result<FirmwareTargetDisposition, FirmwareTargetUpdateError> {
    if let Some(progress) = progress.as_ref() {
        tracing::info!(node = %node.id(), rack = %node.rack_id(), job_id = progress.job_id, "marking firmware workflow running");
        progress.tracker.mark_running(
            progress.job_id,
            &format!(
                "Running firmware workflow for target {}",
                firmware_target_label(progress.target)
            ),
        );
    }

    let outcome = match node
        .update_firmware(target, force_update, firmware_update_options_for_node(node))
        .await
    {
        Ok(outcome) => outcome,
        Err(e) if e.code == ErrorCode::AlreadyExists => {
            return Ok(FirmwareTargetDisposition::Skipped { reason: e.message });
        }
        Err(e) => return Err(FirmwareTargetUpdateError::Update(e)),
    };

    match outcome {
        FirmwareUpdateOutcome::Started(handle) => {
            if let Some(progress) = progress.as_ref() {
                progress.tracker.mark_running(
                    progress.job_id,
                    &format!(
                        "Polling firmware task {} for target {}",
                        handle.task_id,
                        firmware_target_label(progress.target)
                    ),
                );
            }

            let recover_supermicro_bmc = supermicro_bmc_task_loss_recovery_allowed(node, target);
            let transient_error_retry_timeout = if recover_supermicro_bmc {
                Duration::ZERO
            } else {
                FIRMWARE_TRANSIENT_POLL_ERROR_RETRY_TIMEOUT
            };
            let poll_result = poll_firmware_task_until_complete_with_retry_timeout(
                node,
                &handle.task_id,
                FIRMWARE_POLL_TIMEOUT,
                FIRMWARE_POLL_INTERVAL,
                transient_error_retry_timeout,
            )
            .await;
            if let Err(poll_error) = poll_result {
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
                    )
                    .await
                    .map_err(FirmwareTargetUpdateError::Poll)?;
                    return Ok(FirmwareTargetDisposition::Applied {
                        summary: Some(summary),
                    });
                }
                return Err(FirmwareTargetUpdateError::Poll(poll_error));
            }
            Ok(FirmwareTargetDisposition::Applied { summary: None })
        }
        FirmwareUpdateOutcome::Completed(summary) => Ok(FirmwareTargetDisposition::Applied {
            summary: Some(summary),
        }),
        FirmwareUpdateOutcome::Skipped { reason } => {
            Ok(FirmwareTargetDisposition::Skipped { reason })
        }
    }
}

fn supermicro_bmc_task_loss_recovery_allowed<N: Node>(node: &N, target: &FirmwareTarget) -> bool {
    node.node_type() == NodeType::ComputeGb300Supermicro
        && supermicro_bmc_target(target)
        && target
            .expected_version
            .as_deref()
            .is_some_and(|version| !version.trim().is_empty())
}

async fn recover_supermicro_bmc_update_after_task_loss<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    task_id: &str,
    poll_error: &RmsError,
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

    let version_check = post_activation_version_check(node, std::slice::from_ref(target)).await?;
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

/// Wraps `node.activate_firmware_with` with a single BMC-aux-powercycle retry
/// for nodes when the host (i.e. NVOS) is unreachable. Relies on concrete node
/// implementations for firmware activation and fallback powercycling.
async fn activate_firmware_for_node<N: Node>(node: &N) -> Result<()> {
    let result = node
        .activate_firmware_with(FirmwareActivationRequest {
            mode: activation_mode_for_node(node)?,
            cancellation: None,
        })
        .await
        .map(|_| ());

    match result {
        Err(ref e) if is_unreachable_err(e) && node.supports_bmc_aux_powercycle() => {
            tracing::warn!(
                node = %node.id(),
                error = %e.message,
                "firmware activation via host unreachable; falling back to BMC aux powercycle"
            );
            attempt_bmc_aux_powercycle_recovery(node).await
        }
        other => other,
    }
}

fn activation_mode_for_node<N: Node>(node: &N) -> Result<FirmwareActivationMode> {
    match node.node_type() {
        NodeType::ComputeGb200Nvidia
        | NodeType::ComputeGb200Wiwynn
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
        | NodeType::PowershelfGb300Delta => {
            Ok(FirmwareActivationMode::PowerShelfReset { force: false })
        }
    }
}

fn node_supports_post_activation_version_check<N: Node>(node: &N) -> bool {
    matches!(
        node.node_type(),
        NodeType::ComputeGb200Nvidia
            | NodeType::ComputeGb200Wiwynn
            | NodeType::ComputeGb300Nvidia
            | NodeType::ComputeGb300Lenovo
            | NodeType::ComputeGb300Supermicro
            | NodeType::ComputeVrnvl72Nvidia
            | NodeType::SwitchGb200Nvidia
            | NodeType::SwitchGb300Nvidia
            | NodeType::SwitchVrnvl72Nvidia
    )
}

async fn post_activation_version_check<N: Node>(
    node: &N,
    targets: &[FirmwareTarget],
) -> Result<Option<FirmwareVersionCheckSummary>> {
    if !node_supports_post_activation_version_check(node) {
        return Ok(None);
    }

    if targets.iter().all(|target| target.firmware_file.is_empty()) {
        return Ok(None);
    }

    if !POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY.is_zero() {
        tokio::time::sleep(POST_ACTIVATION_VERSION_CHECK_INITIAL_DELAY).await;
    }

    let summary = verify_firmware_package_versions_after_activation(node, targets).await?;
    if summary.matched {
        Ok(Some(summary))
    } else {
        Err(RmsError::internal(format!(
            "post-activation firmware version check failed: {}",
            version_check_mismatch_summary(&summary.details)
        )))
    }
}

async fn verify_firmware_package_versions_after_activation<N: Node>(
    node: &N,
    targets: &[FirmwareTarget],
) -> Result<FirmwareVersionCheckSummary> {
    let mut last_error = None;
    let mut last_mismatched_summary = None;
    for attempt in 1..=POST_ACTIVATION_VERSION_CHECK_ATTEMPTS {
        match node.verify_firmware_package_versions(targets).await {
            Ok(summary) if summary.matched => return Ok(summary),
            Ok(summary) => {
                let will_retry = attempt < POST_ACTIVATION_VERSION_CHECK_ATTEMPTS;
                tracing::warn!(
                    node = %node.id(),
                    attempt,
                    attempts = POST_ACTIVATION_VERSION_CHECK_ATTEMPTS,
                    will_retry,
                    summary = %version_check_mismatch_summary(&summary.details),
                    "post-activation firmware versions do not match"
                );
                last_mismatched_summary = Some(summary);
                if will_retry && !POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL.is_zero() {
                    tokio::time::sleep(POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL).await;
                }
            }
            Err(error) if is_retryable_post_activation_version_check_error(&error) => {
                let will_retry = attempt < POST_ACTIVATION_VERSION_CHECK_ATTEMPTS;
                tracing::warn!(
                    node = %node.id(),
                    attempt,
                    attempts = POST_ACTIVATION_VERSION_CHECK_ATTEMPTS,
                    will_retry,
                    error = %error.message,
                    "post-activation firmware version check failed"
                );
                last_error = Some(error);
                if will_retry && !POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL.is_zero() {
                    tokio::time::sleep(POST_ACTIVATION_VERSION_CHECK_RETRY_INTERVAL).await;
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
            });
        }
    } else if !r.filename.is_empty() {
        let path = resolve_firmware_file(&r.filename, firmware_dir)?;
        targets.push(FirmwareTarget {
            component: firmware_target_component_for_node_type(node_type, &r.target),
            firmware_file: path.to_string_lossy().into_owned(),
            expected_version: None,
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
// validated that username/password are non-empty.
pub(crate) fn build_ephemeral_node(
    node_info: &rm::NodeInfo,
    node_type: NodeType,
    expected_inventory: Option<ExpectedInventoryPolicy>,
) -> Result<Arc<NodeInstance>> {
    let flat = flatten_node_info(node_info)?;
    // Direct switch firmware is a host/NVUE workflow, so it requires
    // host_endpoint and does not force callers to provide a BMC endpoint.
    // Compute and powershelf firmware are BMC/Redfish workflows.
    let bmc_endpoint = if node_type.kind() == NodeKind::Switch {
        flat.optional_bmc_endpoint()?
    } else {
        Some(flat.bmc_endpoint()?)
    }
    .map(|mut endpoint| {
        // BMC/Redfish endpoints use HTTPS with Basic auth in current
        // deployments, but RMS cannot validate BMC certificates. Keep this
        // hardcoded here rather than honoring a client-supplied endpoint field.
        endpoint.dangerously_accept_invalid_certs = true;
        endpoint
    });
    let host_endpoint = if node_type.kind() == NodeKind::Switch {
        Some(flat.switch_host_management_endpoint()?)
    } else {
        flat.optional_host_endpoint()?
    };

    let config = NodeConfig {
        id: node_info.node_id.clone(),
        node_type,
        bmc_endpoint,
        host_endpoint,
        expected_inventory,
    };

    NodeInstance::create(&config, node_info.rack_id.as_str())
}

pub(crate) fn spawn_firmware_update_job<N: Node + Send + Sync + 'static>(
    tracker: Arc<JobTracker>,
    job_handle: RmsJobHandle,
    node: Arc<N>,
    targets: Vec<FirmwareTarget>,
    activate: bool,
    force_update: bool,
) {
    let progress_tracker = tracker.clone();
    tracker
        .spawn_job(job_handle, move |job| async move {
            job.progress("Starting firmware update");
            let job_id = job.id().to_string();

            let stages = match firmware_update_stages(node.node_type(), &targets, activate) {
                Ok(stages) => stages,
                Err(error) => {
                    let job_error = firmware_update_job_error(&error);
                    fail_tracked_firmware_job(job, "planning", None, job_error, &error.message);
                    return;
                }
            };

            let mut skipped = 0usize;
            let mut target_results = Vec::new();
            let mut completed_targets = 0usize;
            let mut activation_count = 0usize;
            let mut post_activation_checks = Vec::new();
            for (stage_index, stage) in stages.iter().enumerate() {
                let mut stage_applied_targets = Vec::new();
                for target in &stage.targets {
                    job.progress(format!(
                        "Uploading target {}/{}: {}",
                        completed_targets + 1,
                        targets.len(),
                        firmware_target_label(target)
                    ));

                    match update_firmware_target_with_bmc_recovery(
                        node.as_ref(),
                        target,
                        force_update,
                        Some(FirmwareTaskProgress {
                            tracker: progress_tracker.as_ref(),
                            job_id: &job_id,
                            target,
                        }),
                    )
                    .await
                    {
                        Ok(disposition) => {
                            target_results.push(firmware_target_result_json(target, &disposition));
                            match disposition {
                                FirmwareTargetDisposition::Applied { .. } => {
                                    stage_applied_targets.push(target.clone());
                                }
                                FirmwareTargetDisposition::Skipped { reason } => {
                                    tracing::info!(
                                        job_id = %job_id,
                                        target = %firmware_target_label(target),
                                        reason = %reason,
                                        "skipping firmware target: no update applied"
                                    );
                                    skipped += 1;
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
                    completed_targets += 1;
                }

                if stage.activate_after && !stage_applied_targets.is_empty() {
                    if stages.len() == 1 {
                        job.progress("Activating firmware");
                    } else {
                        job.progress(format!(
                            "Activating firmware after stage {}/{}",
                            stage_index + 1,
                            stages.len()
                        ));
                    }
                    if let Err(e) = activate_firmware_for_node(node.as_ref()).await {
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

                    if !node_supports_post_activation_version_check(node.as_ref()) {
                        continue;
                    }

                    job.progress("Verifying activated firmware versions");
                    match post_activation_version_check(node.as_ref(), &stage_applied_targets).await
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
                "activated": activation_count > 0,
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

fn firmware_update_stages(
    node_type: NodeType,
    targets: &[FirmwareTarget],
    activate: bool,
) -> Result<Vec<FirmwareUpdateStage>> {
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

fn build_firmware_targets_by_type(
    r: &rm::BatchUpdateFirmwareByNodeTypeRequest,
    firmware_dir: &Path,
) -> std::result::Result<Vec<FirmwareTarget>, String> {
    let mut targets = Vec::new();
    let node_type =
        proto_node_type_to_domain(r.node_type).ok_or_else(|| "unknown node type".to_owned())?;

    if !r.firmware_targets.is_empty() {
        for ft in &r.firmware_targets {
            validate_firmware_target_entry(ft)?;
            let path = resolve_firmware_file(&ft.filename, firmware_dir)
                .map_err(|e| format!("{e} (component: {})", ft.target))?;
            targets.push(FirmwareTarget {
                component: firmware_target_component_for_node_type(node_type, &ft.target),
                firmware_file: path.to_string_lossy().into_owned(),
                expected_version: None,
            });
        }
    } else if !r.filename.is_empty() {
        let path = resolve_firmware_file(&r.filename, firmware_dir)?;
        targets.push(FirmwareTarget {
            component: firmware_target_component_for_node_type(node_type, &r.target),
            firmware_file: path.to_string_lossy().into_owned(),
            expected_version: None,
        });
    }

    Ok(targets)
}
impl RackManagerServiceImpl {
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

        if let Err(error) = self.initialize_nvue_client(node.nvue_client(), None).await {
            tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %error.message, "failed to configure NVUE client");
            return Ok(tonic::Response::new(resp));
        }

        match node.get_firmware_inventory().await {
            Ok(inventory) => {
                for fw in &inventory {
                    resp.firmware_list.push(rm::FirmwareInventoryInfo {
                        name: fw.name.clone(),
                        version: fw.version.clone(),
                        updateable: fw.updateable,
                        target: fw.target.clone(),
                        health: fw.health.clone(),
                        firmware_type: firmware_type_to_int(fw.firmware_type),
                    });
                }
                resp.status = rm::ReturnCode::Success.into();
            }
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e.message, "GetNodeFirmwareInventory failed")
            }
        }
        Ok(tonic::Response::new(resp))
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

        if let Err(error) = self.initialize_nvue_client(node.nvue_client(), None).await {
            tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %error.message, "failed to configure NVUE client");
            resp.status = rm::ReturnCode::Failure.into();
            resp.message = error.message;
            resp.error_code = rm::FirmwareUpdateError::ClientFailure.into();
            return Ok(tonic::Response::new(resp));
        }

        let targets = match build_firmware_targets(&r, &self.firmware_dir, node.node_type()) {
            Ok(t) if t.is_empty() => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, "no firmware targets specified");
                resp.message = "no firmware targets specified".into();
                return Ok(tonic::Response::new(resp));
            }
            Ok(t) => t,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "invalid firmware targets");
                resp.message = e;
                resp.error_code = rm::FirmwareUpdateError::FileNotFound.into();
                return Ok(tonic::Response::new(resp));
            }
        };

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
        let job_id = pending.id().to_string();

        resp.job_id = job_id.clone();
        resp.status = rm::ReturnCode::Success.into();
        resp.error_code = rm::FirmwareUpdateError::Success.into();
        resp.message = format!("Firmware update job created for {}", r.node_id);

        // Spawn background task: upload each target, poll until done, optionally activate
        spawn_firmware_update_job(
            self.job_tracker.clone(),
            pending,
            node,
            targets,
            r.activate,
            r.force_update,
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

        let targets = match build_firmware_targets_by_type(&r, &self.firmware_dir) {
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

        let mut matched_nodes = 0u32;
        let mut failed_nodes = 0u32;
        let mut queued_jobs = Vec::new();

        for node in rack.list_nodes() {
            if node.node_type() != target_type {
                continue;
            }
            matched_nodes += 1;

            if let Err(error) = self.initialize_nvue_client(node.nvue_client(), None).await {
                tracing::error!(node = %node.id(), rack = %r.rack_id, error = %error.message, "failed to configure NVUE client");
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node.id().to_owned(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: error.message,
                });
                failed_nodes += 1;
                continue;
            }

            let pending = match self.job_tracker.create_child_job_if_node_idle(
                &parent_id,
                &r.rack_id,
                node.id(),
                JobType::FirmwareUpdate,
            ) {
                Ok(pending) => pending,
                Err(failure) => {
                    tracing::warn!(
                        node = node.id(),
                        rack = r.rack_id,
                        message = %failure.message,
                        "firmware job rejected"
                    );

                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node.id().to_owned(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: failure.message,
                    });

                    failed_nodes += 1;
                    continue;
                }
            };
            let job_id = pending.id().to_string();

            jobs.push(rm::NodeFirmwareJobInfo {
                node_id: node.id().to_owned(),
                job_id: job_id.clone(),
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
            self.job_tracker
                .mark_failed_message(&parent_id, &batch.message);

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
            spawn_firmware_update_job(
                self.job_tracker.clone(),
                pending,
                node,
                targets.clone(),
                r.activate,
                r.force_update,
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

            if flat.creds_for_node_type(node_type).is_none() {
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
            let node = match build_ephemeral_node(&node_info, node_type, expected_inventory) {
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

            if let Err(e) = self.initialize_nvue_client(node.nvue_client(), None).await {
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: e.message,
                });

                skipped_nodes += 1;
                continue;
            }

            let pending = match self.job_tracker.create_child_job_if_node_idle(
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
            spawn_firmware_update_job(
                self.job_tracker.clone(),
                pending,
                node,
                targets,
                activate,
                force_update,
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
            if let Err(error) = self.initialize_nvue_client(node.nvue_client(), None).await {
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
            let mut nfi = rm::NodeFirmwareInventory {
                node_id: node.id().to_owned(),
                firmware_list: Vec::new(),
            };
            for fw in &inventory {
                nfi.firmware_list.push(rm::FirmwareInventoryInfo {
                    name: fw.name.clone(),
                    version: fw.version.clone(),
                    updateable: fw.updateable,
                    target: fw.target.clone(),
                    health: fw.health.clone(),
                    firmware_type: firmware_type_to_int(fw.firmware_type),
                });
            }
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
    use super::*;
    use crate::domain::node::{
        FirmwareActivationSummary, FirmwareInfo, FirmwareTaskStatus, NodeKind, NodeType,
        PowerState, ProductFamily,
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

    struct VersionCheckNode {
        node_type: NodeType,
        summary: FirmwareVersionCheckSummary,
        transient_failures: usize,
        calls: AtomicUsize,
    }

    struct StaticPollNode {
        status: FirmwareTaskStatus,
    }

    struct SequencePollNode {
        statuses: Mutex<VecDeque<FirmwareTaskStatus>>,
        calls: AtomicUsize,
    }

    struct InventoryNode {
        id: &'static str,
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
            NodeType::ComputeGb200Nvidia
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
            switch_tls_roots: SwitchTlsRoots::default(),
            sftp_upload_options: crate::transport::ssh::SftpUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
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
                | NodeType::PowershelfGb300Delta => {
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
            self.calls.fetch_add(1, Ordering::SeqCst);
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
        }
    }

    fn supermicro_target(component: &str, firmware_file: &str) -> FirmwareTarget {
        FirmwareTarget {
            component: component.to_owned(),
            firmware_file: firmware_file.to_owned(),
            expected_version: None,
        }
    }

    fn supermicro_bmc_target_with_firmware_manifest_version() -> FirmwareTarget {
        FirmwareTarget {
            component: String::new(),
            firmware_file: "/tmp/NvOBMC_GBNVL72_70.02.01.05.bin".to_owned(),
            expected_version: Some("70.02.01.05".to_owned()),
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

        spawn_firmware_update_job(tracker.clone(), pending, node, targets, true, false);

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
        );

        let job = wait_for_firmware_job_terminal(tracker.as_ref(), &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Completed
        );
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(result["status"], "completed");
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

        let disposition = update_firmware_target_until_done(&node, &firmware_target(), false)
            .await
            .expect("completed outcome should succeed");

        assert!(matches!(
            disposition,
            FirmwareTargetDisposition::Applied { summary: Some(_) }
        ));
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn update_target_outcome_skipped_finishes_without_polling() {
        let node = OutcomeNode::new(FirmwareUpdateOutcome::Skipped {
            reason: "already current".to_owned(),
        });

        let disposition = update_firmware_target_until_done(&node, &firmware_target(), false)
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
        ] {
            let node = OutcomeNode::new_for_node_type(
                node_type,
                FirmwareUpdateOutcome::Completed(crate::domain::node::FirmwareUpdateSummary {
                    message: "workflow completed internally".to_owned(),
                    task_ids: Vec::new(),
                    details: serde_json::json!({}),
                }),
            );

            let disposition = update_firmware_target_until_done(&node, &firmware_target(), false)
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
        ] {
            let node = OutcomeNode::new_for_node_type(node_type, completed_update_outcome());

            let disposition = update_firmware_target_until_done(&node, &firmware_target(), false)
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

            let disposition = update_firmware_target_until_done(&node, &firmware_target(), false)
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

        let disposition = update_firmware_target_until_done(&node, &firmware_target(), false)
            .await
            .expect("started outcome should poll and succeed");

        assert_eq!(
            disposition,
            FirmwareTargetDisposition::Applied { summary: None }
        );
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 1);
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
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 1);
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
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 1);
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
            Some(FirmwareTaskProgress {
                tracker: &tracker,
                job_id: &job_id,
                target: &target,
            }),
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
            Some(FirmwareTaskProgress {
                tracker: &tracker,
                job_id: &job_id,
                target: &target,
            }),
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
        ] {
            let node = ActivationNode {
                node_type,
                calls: AtomicUsize::new(0),
            };

            activate_firmware_for_node(&node)
                .await
                .expect("activation should succeed");

            assert_eq!(node.calls.load(Ordering::SeqCst), 1);
        }
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
        };

        let err = post_activation_version_check(&node, &[firmware_target()])
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
    async fn post_activation_version_check_retries_transient_inventory_failures() {
        let node = VersionCheckNode {
            node_type: NodeType::ComputeGb200Nvidia,
            summary: FirmwareVersionCheckSummary {
                matched: true,
                details: serde_json::json!({"status": "matched"}),
            },
            transient_failures: 2,
            calls: AtomicUsize::new(0),
        };

        let summary = post_activation_version_check(&node, &[firmware_target()])
            .await
            .expect("transient inventory failures should retry")
            .expect("version check should run");

        assert!(summary.matched);
        assert_eq!(node.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn post_activation_version_check_skips_powershelf_nodes() {
        let node = OutcomeNode::new_for_node_type(
            NodeType::PowershelfGb200Liteon,
            completed_update_outcome(),
        );

        let summary = post_activation_version_check(&node, &[firmware_target()])
            .await
            .expect("powershelf should not run post-activation version check");

        assert!(summary.is_none());
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
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .await;
        assert!(res.is_ok(), "expected Ok, got {res:?}");
        assert!(res.unwrap().completed);
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
            Duration::from_secs(5),
            Duration::from_millis(1),
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
            Duration::from_secs(1),
            Duration::from_millis(10),
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
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .await;

        assert!(res.is_ok(), "expected Ok, got {res:?}");
        assert_eq!(node.calls.load(Ordering::SeqCst), 3);
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
            Duration::from_millis(30),
            Duration::from_millis(5),
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
            },
            FirmwareTaskStatus {
                completed: true,
                percent: 100,
                state: "Completed".to_owned(),
                status: "Warning".to_owned(),
                message: "The resource property Firmware Update Service has detected errors of type 'No Matching Devices'. Resolution: Verify the FW package has devices that are listed in the Redfish FW Inventory".to_owned(),
            },
        ] {
            let node = StaticPollNode { status };
            let err = poll_firmware_task_until_complete(
                &node,
                "Task-Failed",
                Duration::from_secs(1),
                Duration::from_millis(1),
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
        };

        let err = poll_firmware_task_until_complete(
            &node,
            "0",
            Duration::from_secs(1),
            Duration::from_millis(1),
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

    #[tokio::test]
    async fn poll_until_complete_accepts_action_error_only_at_100_percent() {
        let node = StaticPollNode {
            status: FirmwareTaskStatus {
                completed: true,
                percent: 100,
                state: "action_error".to_owned(),
                status: "rebooting to install image".to_owned(),
                message: "Installing image after reboot".to_owned(),
            },
        };

        poll_firmware_task_until_complete(
            &node,
            "Task-Reboot-Handoff",
            Duration::from_secs(1),
            Duration::from_millis(1),
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
            },
        };

        poll_firmware_task_until_complete(
            &node,
            "Task-Completed-With-Diagnostics",
            Duration::from_secs(1),
            Duration::from_millis(1),
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
            Duration::from_millis(50),
            Duration::from_millis(5),
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
        update_calls: AtomicUsize,
        activation_calls: AtomicUsize,
        powercycle_calls: AtomicUsize,
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
                update_calls: AtomicUsize::new(0),
                activation_calls: AtomicUsize::new(0),
                powercycle_calls: AtomicUsize::new(0),
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
            HashMap::new()
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
        }
    }

    // ── update_firmware_target_with_bmc_recovery ─────────────────────────────

    #[tokio::test]
    async fn bmc_recovery_wrapper_passes_through_success_without_powercycle() {
        let node = RecoveryMockNode::switch_with_updates([Ok(completed_outcome())]);
        let result =
            update_firmware_target_with_bmc_recovery(&node, &dummy_target(), false, None).await;
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
            update_calls: AtomicUsize::new(0),
            activation_calls: AtomicUsize::new(0),
            powercycle_calls: AtomicUsize::new(0),
        };
        let err = update_firmware_target_with_bmc_recovery(&node, &dummy_target(), false, None)
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
        let result =
            update_firmware_target_with_bmc_recovery(&node, &dummy_target(), false, None).await;
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
        let err = update_firmware_target_with_bmc_recovery(&node, &dummy_target(), false, None)
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
        let err = update_firmware_target_with_bmc_recovery(&node, &dummy_target(), false, None)
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
        activate_firmware_for_node(&node).await.unwrap();
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn activate_firmware_for_node_falls_back_to_bmc_when_nvos_unreachable() {
        for code in [
            ErrorCode::ConnectionRefused,
            ErrorCode::Timeout,
            ErrorCode::Unavailable,
        ] {
            let node = RecoveryMockNode::switch_with_updates(VecDeque::new())
                .with_activation_error(code, "NVOS unreachable");
            activate_firmware_for_node(&node)
                .await
                .unwrap_or_else(|e| panic!("expected Ok for {code:?}, got Err: {}", e.message));
            assert_eq!(
                node.activation_calls.load(Ordering::SeqCst),
                1,
                "activation should be called once for {code:?}"
            );
            assert_eq!(
                node.powercycle_calls.load(Ordering::SeqCst),
                1,
                "BMC powercycle should be called as fallback for {code:?}"
            );
        }
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
            update_calls: AtomicUsize::new(0),
            activation_calls: AtomicUsize::new(0),
            powercycle_calls: AtomicUsize::new(0),
        };
        let err = activate_firmware_for_node(&node).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionRefused);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn activate_firmware_for_node_does_not_fall_back_for_non_unreachable_error() {
        let node = RecoveryMockNode::switch_with_updates(VecDeque::new())
            .with_activation_error(ErrorCode::Internal, "nvfwupd task failed");
        let err = activate_firmware_for_node(&node).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn activate_firmware_for_node_propagates_bmc_fallback_error() {
        let node = RecoveryMockNode::switch_with_updates(VecDeque::new())
            .with_activation_error(ErrorCode::ConnectionRefused, "NVOS unreachable")
            .with_powercycle_error(ErrorCode::FailedPrecondition, "no BMC endpoint");
        let err = activate_firmware_for_node(&node).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("no BMC endpoint"));
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
    }
}
