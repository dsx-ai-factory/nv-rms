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
use std::fmt::Display;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::orchestrator::job_lifecycle::{
    JobDomain, JobFailure, JobId, JobLifecycleState, JobRegistry, JobSnapshot, JobSpec,
    ParentJobSpec, PendingJob, TrackedJob, TrackedJoinHandle,
};

// ── Job state types ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Running,
    Completed,
    Failed,
}

impl JobState {
    /// Lowercase wire string used by status RPCs (e.g.
    /// `GetSwitchSystemImageJobStatus.state`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// Domain-neutral classification of why an async job ended. These codes are
/// generic (I/O, client/server failure, timeout, task failure, ...) rather
/// than firmware-specific. `conversions.rs` maps them to the generic
/// `pb::JobError` enum for `GetJobStatus` and to the legacy firmware-specific
/// enum for `GetFirmwareJobStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobError {
    Unspecified,
    Internal,
    Other,
    InvalidArgument,
    ClientError,
    ServerError,
    Timeout,
    InvalidResponse,
    TargetNotFound,
    FileNotFound,
    UpdateInProgress,
}

impl Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Unspecified => "unspecified",
            Self::Internal => "internal",
            Self::Other => "other",
            Self::InvalidArgument => "invalid argument",
            Self::ClientError => "client error",
            Self::ServerError => "server error",
            Self::Timeout => "timeout",
            Self::InvalidResponse => "invalid response",
            Self::TargetNotFound => "target not found",
            Self::FileNotFound => "file not found",
            Self::UpdateInProgress => "update in progress",
        };
        f.write_str(text)
    }
}

impl std::error::Error for JobError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobType {
    FirmwareUpdate,
    SwitchCertificate,
    SwitchSystemImageUpdate,
}

impl JobType {
    pub const fn span_name(self) -> &'static str {
        match self {
            Self::FirmwareUpdate => "firmware_update",
            Self::SwitchCertificate => "switch_certificate",
            Self::SwitchSystemImageUpdate => "switch_system_image_update",
        }
    }

    /// Singular noun used in batch progress descriptions (e.g. "Batch
    /// {noun} in progress").
    const fn batch_noun(self) -> &'static str {
        match self {
            Self::FirmwareUpdate => "firmware update",
            Self::SwitchCertificate => "switch certificate install",
            Self::SwitchSystemImageUpdate => "switch system image update",
        }
    }

    /// Plural noun used in batch completion / failure summaries (e.g. "All N
    /// {noun} completed successfully").
    const fn batch_noun_plural(self) -> &'static str {
        match self {
            Self::FirmwareUpdate => "firmware updates",
            Self::SwitchCertificate => "switch certificate installs",
            Self::SwitchSystemImageUpdate => "switch system image updates",
        }
    }
}

/// The shared lifecycle domain for every async job the `JobTracker` owns
/// (firmware updates and switch system image updates alike). Because it is not
/// tied to a single `JobType`, its failure codes ([`JobError`]) and supervisor
/// fallback messages stay domain-neutral; per-`JobType` wording is applied at
/// aggregation time instead.
pub(crate) struct RmsJobDomain;

pub(crate) type PendingRmsJob = PendingJob<RmsJobDomain>;
pub(crate) type TrackedRmsJob = TrackedJob<RmsJobDomain>;

impl JobDomain for RmsJobDomain {
    fn dropped_failure() -> JobFailure {
        // Supervisor fallbacks fire for any job type sharing this domain
        // (firmware + switch), so the wording stays job-type-neutral.
        job_failure(
            JobError::Internal,
            "job task exited before recording a terminal state",
        )
    }

    fn panic_failure() -> JobFailure {
        job_failure(JobError::Internal, "job task panicked")
    }

    fn cancelled_failure() -> JobFailure {
        job_failure(JobError::Internal, "job task was cancelled")
    }

    fn at_capacity_failure(current: usize, max: usize) -> JobFailure {
        job_failure(
            JobError::Internal,
            format!(
                "job tracker at capacity ({current}/{max} jobs); retry after in-flight jobs finish"
            ),
        )
    }

    fn node_busy_failure(rack_id: &str, node_id: &str, active_job_id: &JobId) -> JobFailure {
        job_failure(
            JobError::UpdateInProgress,
            format!(
                "job already in progress for rack {rack_id} node {node_id} (job_id {active_job_id})"
            ),
        )
    }
}

#[derive(Debug, Clone)]
pub struct JobInfo {
    pub job_id: String,
    pub span: tracing::Span,
    pub state: JobState,
    pub state_description: String,
    pub rack_id: String,
    pub node_id: String,
    pub error_code: JobError,
    pub error_message: String,
    pub result_json: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub child_job_ids: Vec<String>,
    pub is_parent_job: bool,
}

// ── JobTracker ──

// This TT constant is used to define the retention period for terminal jobs
const DEFAULT_TTL_MS: i64 = 24 * 3600 * 1000;

/// How often the background reaper prunes expired terminal jobs, so eviction
/// does not depend on a new `create_*` call arriving.
const REAPER_INTERVAL: Duration = Duration::from_secs(300);

/// Thread-safe registry for tracking asynchronous RMS jobs (firmware updates
/// and switch system image updates), keyed by `JobType`.
///
/// Stores `JobInfo` entries keyed by UUID. Background tokio tasks update
/// job progress, gRPC handlers query status. Terminal jobs (completed/failed)
/// are retained for 1 hour then cleaned up lazily.
///
/// Parent jobs aggregate state from their children on read.
pub struct JobTracker {
    registry: Arc<JobRegistry<RmsJobDomain>>,
    /// Remembers the `JobType` of each parent job so child aggregation can
    /// pick domain-appropriate wording / result payloads. The generic
    /// `JobRegistry` is domain-agnostic and does not carry this, so the
    /// adapter keeps it alongside. Entries are pruned lazily once the parent
    /// is no longer in the registry.
    parent_job_types: Mutex<HashMap<JobId, JobType>>,
}

impl JobTracker {
    pub fn new() -> Self {
        Self::with_max_tracked_jobs(crate::orchestrator::job_lifecycle::MAX_TRACKED_JOBS)
    }

    /// Builds a tracker whose registry retains at most `max_tracked_jobs`
    /// records. Used to honor the operator-configured cap and to exercise the
    /// capacity path in tests with a small bound.
    pub fn with_max_tracked_jobs(max_tracked_jobs: usize) -> Self {
        Self {
            registry: Arc::new(JobRegistry::with_capacity(
                Duration::from_millis(DEFAULT_TTL_MS as u64),
                max_tracked_jobs,
            )),
            parent_job_types: Mutex::new(HashMap::new()),
        }
    }

    /// Spawns a periodic reaper so terminal-job eviction does not depend on a
    /// new job being created. The task holds a [`Weak`] reference and exits once
    /// every strong owner of the tracker is dropped.
    pub fn spawn_reaper(self: &Arc<Self>) {
        self.spawn_reaper_with_interval(REAPER_INTERVAL);
    }

    /// Spawns the reaper with an explicit tick interval. Keeps the interval a
    /// seam so tests can exercise the loop without waiting the production
    /// cadence.
    fn spawn_reaper_with_interval(self: &Arc<Self>, interval: Duration) {
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                let Some(tracker) = weak.upgrade() else {
                    break;
                };
                tracker.reaper_sweep();
            }
        });
    }

    /// One reaper pass: aggregate parent state, then evict expired terminal
    /// jobs, then prune the parent-type side map.
    ///
    /// The parent refresh is what keeps eviction self-sufficient. A parent job
    /// only transitions to terminal via [`refresh_parent_state`], and outside
    /// this pass its sole caller is [`Self::get_job`]. Without the refresh
    /// here, a parent whose children are all terminal stays `Running` until an
    /// external caller polls it, so [`JobRegistry::cleanup_expired`] (which
    /// only evicts terminal jobs) can never reap it. A batch that is created
    /// but never polled would then leak its parent record permanently and
    /// eventually exhaust tracker capacity, rejecting new jobs.
    fn reaper_sweep(&self) {
        self.refresh_all_parents();
        self.registry.cleanup_expired();
        self.prune_parent_job_types();
    }

    /// Aggregates every tracked parent's state so parents whose children are
    /// all terminal transition to terminal without an external `get_job`.
    ///
    /// The `(id, job_type)` pairs are collected under the side-map lock and the
    /// lock is released before touching the registry, so this never holds the
    /// parent-type lock across a registry write.
    fn refresh_all_parents(&self) {
        let parents: Vec<(JobId, JobType)> = {
            let guard = self.parent_job_types.lock().unwrap();
            guard
                .iter()
                .map(|(id, job_type)| (id.clone(), *job_type))
                .collect()
        };
        for (parent_id, job_type) in parents {
            refresh_parent_state(&self.registry, &parent_id, job_type);
        }
    }

    /// Drops side-map entries for parents the registry no longer tracks (e.g.
    /// evicted by the preceding `cleanup_expired`). `get_job` prunes lazily on
    /// read, but the reaper must not rely on reads happening.
    fn prune_parent_job_types(&self) {
        let ids: Vec<JobId> = {
            let guard = self.parent_job_types.lock().unwrap();
            guard.keys().cloned().collect()
        };
        for id in ids {
            if self.registry.get(&id).is_none() {
                self.parent_job_types.lock().unwrap().remove(&id);
            }
        }
    }

    /// Create a firmware job without checking for an active job on the same node.
    ///
    /// Returns only the new job's ID; the job is registered in `Queued` state
    /// with no worker attached. The caller is responsible for actually running
    /// the work (e.g. via `tokio::spawn`) and driving the job to a terminal
    /// state with [`Self::mark_running`] / [`Self::mark_completed`] /
    /// [`Self::mark_failed`] using the returned ID. A job that is created but
    /// never spawned stays `Queued` until it is reaped. Use this for the batch
    /// pre-create pattern; prefer [`Self::create_pending_job`], which yields a
    /// `#[must_use]` [`PendingRmsJob`], when spawning immediately.
    ///
    /// Fails with a [`JobError::Internal`] failure when the tracker is full.
    pub fn create_job(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<String, JobFailure> {
        self.create_pending_job(rack_id, node_id, job_type)
            .map(|pending| pending.id().to_string())
    }

    pub(crate) fn create_pending_job(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<PendingRmsJob, JobFailure> {
        let pending = self.registry.create_leaf(JobSpec::new(
            rack_id,
            node_id,
            "Queued",
            job_span(job_type, ""),
        ))?;
        self.record_job_id_in_span(&pending);
        tracing::info!(
            job_id = %pending.id(),
            rack_id,
            node_id,
            "job created"
        );
        Ok(pending)
    }

    /// Create a firmware job only when the target node has no active job.
    ///
    /// Fails with a [`JobError::UpdateInProgress`] failure when a queued or running
    /// leaf job already targets the same `(rack_id, node_id)`, or
    /// [`JobError::Internal`] when the tracker is full.
    pub fn create_job_if_node_idle(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<String, JobFailure> {
        self.create_pending_job_if_node_idle(rack_id, node_id, job_type)
            .map(|pending| pending.id().to_string())
    }

    pub(crate) fn create_pending_job_if_node_idle(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<PendingRmsJob, JobFailure> {
        let pending = self.registry.create_leaf_if_node_idle(JobSpec::new(
            rack_id,
            node_id,
            "Queued",
            job_span(job_type, ""),
        ))?;
        self.record_job_id_in_span(&pending);
        tracing::info!(
            job_id = %pending.id(),
            rack_id,
            node_id,
            "job created"
        );
        Ok(pending)
    }

    pub(crate) fn spawn_tracked<F, Fut>(&self, pending: PendingRmsJob, run: F) -> TrackedJoinHandle
    where
        F: FnOnce(TrackedRmsJob) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.registry.spawn_tracked(pending, run)
    }

    /// Hands out the underlying lifecycle registry so generic `job_lifecycle`
    /// utilities (such as `CleanupPlan`) can operate on the jobs this tracker
    /// owns without re-implementing registry mechanics on the tracker.
    pub(crate) fn registry(&self) -> Arc<JobRegistry<RmsJobDomain>> {
        self.registry.clone()
    }

    pub fn create_parent_job(
        &self,
        rack_id: &str,
        child_job_ids: &[String],
        job_type: JobType,
    ) -> Option<String> {
        if child_job_ids.is_empty() {
            tracing::warn!(rack_id, "create_parent_job called with empty child list");
            return None;
        }

        let total = child_job_ids.len();
        let child_ids: Vec<JobId> = child_job_ids
            .iter()
            .map(|id| JobId::from(id.as_str()))
            .collect();
        let parent_id = self.registry.create_parent(
            ParentJobSpec::new(
                rack_id,
                format!(
                    "Batch {} in progress (0/{total} complete)",
                    job_type.batch_noun()
                ),
                job_span(job_type, ""),
            ),
            &child_ids,
        )?;
        self.parent_job_types
            .lock()
            .unwrap()
            .insert(parent_id.clone(), job_type);
        let job_id = parent_id.to_string();
        if let Some(span) = self.registry.span_for_job(&parent_id) {
            span.record("job_id", job_id.as_str());
        }

        for child_job_id in child_job_ids {
            if let Some(span) = self.span_for_job(child_job_id) {
                span.record("parent_job_id", job_id.as_str());
            }
        }

        tracing::info!(
            job_id = %job_id,
            rack_id,
            children = total,
            "parent job created"
        );
        Some(job_id)
    }

    pub fn get_job(&self, job_id: &str) -> Option<JobInfo> {
        let id = JobId::from(job_id);
        let Some(job) = self.registry.get(&id) else {
            // The job (possibly a parent) has been evicted; drop any retained
            // parent-type entry so the side map tracks the registry.
            self.parent_job_types.lock().unwrap().remove(&id);
            return None;
        };

        if job.is_parent_job {
            refresh_parent_state(&self.registry, &id, self.parent_job_type(&id));
        }

        self.registry.get(&id).map(snapshot_to_job_info)
    }

    fn parent_job_type(&self, parent_id: &JobId) -> JobType {
        self.parent_job_types
            .lock()
            .unwrap()
            .get(parent_id)
            .copied()
            .unwrap_or(JobType::FirmwareUpdate)
    }

    pub fn span_for_job(&self, job_id: &str) -> Option<tracing::Span> {
        self.registry.span_for_job(&JobId::from(job_id))
    }

    pub fn mark_running(&self, job_id: &str, description: &str) {
        let id = JobId::from(job_id);
        if !self.registry.mark_running(&id, description) {
            if self.registry.get(&id).is_some() {
                tracing::error!(job_id, "mark_running called for job in terminal state");
            } else {
                tracing::error!(job_id, "mark_running called for unknown job");
            }
        }
    }

    pub fn mark_completed(&self, job_id: &str, result_json: Option<String>) {
        if self.registry.mark_completed(
            &JobId::from(job_id),
            "Completed",
            result_json.unwrap_or_default(),
        ) {
            let span = self
                .span_for_job(job_id)
                .unwrap_or_else(tracing::Span::none);
            let _entered = span.enter();
            tracing::info!("job completed");
        } else {
            tracing::error!(job_id, "mark_completed called for unknown or terminal job");
        }
    }

    pub fn mark_failed(&self, job_id: &str, error_code: JobError, error_message: &str) {
        self.mark_failed_with_result(job_id, error_code, error_message, None);
    }

    pub fn mark_failed_with_result(
        &self,
        job_id: &str,
        error_code: JobError,
        error_message: &str,
        result_json: Option<String>,
    ) {
        let mut failure = JobFailure::new(error_code, error_message);
        if let Some(result_json) = result_json {
            failure = failure.with_result_json(result_json);
        } else {
            failure =
                failure.with_result_json(default_failure_result_json(error_code, error_message));
        }

        if self.registry.mark_failed(&JobId::from(job_id), failure) {
            let span = self
                .span_for_job(job_id)
                .unwrap_or_else(tracing::Span::none);
            let _entered = span.enter();
            tracing::error!(error_message, "job failed");
        } else {
            tracing::error!(job_id, "mark_failed called for unknown or terminal job");
        }
    }

    /// Mark a job failed for job types that do not surface a domain error
    /// code (for example switch system image updates). The terminal
    /// `result_json` is left empty unless
    /// [`Self::mark_failed_message_with_result`] is used.
    pub fn mark_failed_message(&self, job_id: &str, error_message: &str) {
        self.mark_failed_message_with_result(job_id, error_message, None);
    }

    /// Like [`Self::mark_failed_message`] but lets the caller attach a
    /// `result_json` payload (e.g. a switch image stage timeline). Records
    /// [`JobError::Other`]; the code is not surfaced by switch
    /// status RPCs, which only expose `error_message` and `result_json`.
    ///
    /// Delegates to [`Self::mark_failed_with_result`]; passing an explicit
    /// (possibly empty) `result_json` avoids the firmware default payload so
    /// these jobs report exactly what the caller supplied.
    pub fn mark_failed_message_with_result(
        &self,
        job_id: &str,
        error_message: &str,
        result_json: Option<String>,
    ) {
        self.mark_failed_with_result(
            job_id,
            JobError::Other,
            error_message,
            Some(result_json.unwrap_or_default()),
        );
    }

    /// Cooperatively cancel every non-terminal leaf job, returning the number
    /// of jobs signalled. Intended for graceful server shutdown.
    pub fn shutdown(&self) -> usize {
        self.registry.shutdown_active_leaves()
    }

    // Two-phase TTL cleanup: first remove expired leaf jobs (protecting
    // children of live parents), then remove childless expired parents.
    #[cfg(test)]
    fn cleanup_expired(&self) {
        self.registry.cleanup_expired();
    }

    fn record_job_id_in_span(&self, pending: &PendingRmsJob) {
        if let Some(span) = self.registry.span_for_job(pending.id()) {
            span.record("job_id", pending.id().as_ref());
        }
    }
}

impl Default for JobTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct ParentCounts {
    total: usize,
    completed: usize,
    failed: usize,
    running: usize,
    queued: usize,
    missing: usize,
    failed_children: Vec<FailedChild>,
}

impl ParentCounts {
    fn done(&self) -> usize {
        self.completed + self.failed + self.missing
    }

    fn running_description(&self, job_type: JobType) -> String {
        format!(
            "Batch {} in progress ({}/{} complete{}{})",
            job_type.batch_noun(),
            self.done(),
            self.total,
            count_fragment(self.running, "running"),
            count_fragment(self.queued, "queued"),
        )
    }
}

/// Domain-neutral snapshot of a failed child, formatted into the appropriate
/// per-`JobType` detail when the parent failure result is built.
struct FailedChild {
    job_id: String,
    node_id: String,
    error_code: JobError,
    error_message: String,
    result_json: String,
}

#[derive(Serialize)]
struct JobFailureResult<'a> {
    status: &'static str,
    error_code: String,
    error_message: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FirmwareFailedChildDetail {
    job_id: String,
    node_id: String,
    error_code: String,
    #[serde(rename = "error")]
    error_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<FirmwareChildResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_result_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FirmwareParentFailureResult {
    completed: usize,
    failed: usize,
    missing: usize,
    total: usize,
    failed_jobs: Vec<FirmwareFailedChildDetail>,
}

/// Switch system image batches surface a lean per-child detail (no firmware
/// error codes or Redfish/target parsing), matching what the switch workflow
/// records and the switch status RPC exposes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SwitchFailedChildDetail {
    job_id: String,
    node_id: String,
    #[serde(rename = "error")]
    error_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SwitchParentFailureResult {
    completed: usize,
    failed: usize,
    missing: usize,
    total: usize,
    failed_jobs: Vec<SwitchFailedChildDetail>,
}

#[derive(Deserialize)]
struct FirmwareResultStatus {
    status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FirmwareChildResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<FirmwareChildResultTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redfish: Option<FirmwareChildResultRedfish>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_error: Option<String>,
}

impl FirmwareChildResult {
    fn is_empty(&self) -> bool {
        self.status.is_none()
            && self.phase.is_none()
            && self.error_code.is_none()
            && self.error_message.is_none()
            && self.summary.is_none()
            && self.target.is_none()
            && self.target_id.is_none()
            && self.redfish.is_none()
            && self.raw_error.is_none()
    }

    fn summary(&self) -> Option<&str> {
        self.summary.as_deref().or(self.error_message.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FirmwareChildResultTarget {
    component: String,
    firmware_file: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FirmwareChildResultRedfish {
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    severity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolution: Option<String>,
}

struct ParsedFirmwareChildResult {
    summary: Option<String>,
    result: Option<FirmwareChildResult>,
    raw_result_json: Option<String>,
}

fn job_span(job_type: JobType, job_id: &str) -> tracing::Span {
    // The span name must be a compile-time constant for the macro callsite, so
    // each `JobType` needs an explicit arm rather than a runtime `span_name()`.
    match (job_type, job_id.is_empty()) {
        (JobType::FirmwareUpdate, true) => tracing::info_span!(
            "firmware_update",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::FirmwareUpdate, false) => tracing::info_span!(
            "firmware_update",
            job_id = job_id,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchCertificate, true) => tracing::info_span!(
            "switch_certificate",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchCertificate, false) => tracing::info_span!(
            "switch_certificate",
            job_id = job_id,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchSystemImageUpdate, true) => tracing::info_span!(
            "switch_system_image_update",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchSystemImageUpdate, false) => tracing::info_span!(
            "switch_system_image_update",
            job_id = job_id,
            parent_job_id = tracing::field::Empty
        ),
    }
}

fn default_failure_result_json(error_code: JobError, error_message: &str) -> String {
    serialize_result_json(&JobFailureResult {
        status: "failed",
        error_code: format!("{error_code:?}"),
        error_message,
    })
}

fn job_failure(error_code: JobError, error_message: impl Display) -> JobFailure {
    let error_message = error_message.to_string();
    JobFailure::new(error_code, error_message.clone())
        .with_result_json(default_failure_result_json(error_code, &error_message))
}

/// Recovers the typed [`JobError`] embedded as the root cause of a lifecycle
/// failure. Falls back to [`JobError::Internal`] if the cause is not a
/// `JobError` (which should not happen for jobs created through this domain).
pub(crate) fn job_error_of(error: &anyhow::Error) -> JobError {
    error
        .downcast_ref::<JobError>()
        .copied()
        .unwrap_or(JobError::Internal)
}

fn snapshot_to_job_info(snapshot: JobSnapshot) -> JobInfo {
    let state = snapshot_state(&snapshot.state);
    let state_description = snapshot.state.description().to_owned();
    let (error_code, error_message, result_json) = snapshot_payload(snapshot.state);

    JobInfo {
        job_id: snapshot.job_id.to_string(),
        span: snapshot.span,
        state,
        state_description,
        rack_id: snapshot.rack_id,
        node_id: snapshot.node_id,
        error_code,
        error_message,
        result_json,
        created_at: snapshot.created_at,
        updated_at: snapshot.updated_at,
        child_job_ids: snapshot
            .child_job_ids
            .into_iter()
            .map(String::from)
            .collect(),
        is_parent_job: snapshot.is_parent_job,
    }
}

fn snapshot_state(state: &JobLifecycleState) -> JobState {
    match state {
        JobLifecycleState::Queued { .. } => JobState::Queued,
        JobLifecycleState::Running { .. } => JobState::Running,
        JobLifecycleState::Completed { .. } => JobState::Completed,
        JobLifecycleState::Failed { .. } => JobState::Failed,
    }
}

fn snapshot_payload(state: JobLifecycleState) -> (JobError, String, String) {
    match state {
        JobLifecycleState::Failed {
            failure,
            message,
            result_json,
            ..
        } => (job_error_of(&failure), message, result_json),
        JobLifecycleState::Completed { result_json, .. } => {
            (JobError::Unspecified, String::new(), result_json)
        }
        JobLifecycleState::Queued { .. } | JobLifecycleState::Running { .. } => {
            (JobError::Unspecified, String::new(), String::new())
        }
    }
}

fn refresh_parent_state(
    registry: &JobRegistry<RmsJobDomain>,
    parent_id: &JobId,
    job_type: JobType,
) {
    let Some(parent) = registry.get(parent_id) else {
        return;
    };
    if !parent.is_parent_job || parent.child_job_ids.is_empty() {
        return;
    }

    let counts = collect_parent_counts(registry, &parent.child_job_ids);
    if counts.done() == counts.total {
        refresh_terminal_parent(registry, parent_id, &parent.child_job_ids, counts, job_type);
    } else {
        registry.mark_running(parent_id, counts.running_description(job_type));
    }
}

fn collect_parent_counts(
    registry: &JobRegistry<RmsJobDomain>,
    child_ids: &[JobId],
) -> ParentCounts {
    let mut counts = ParentCounts {
        total: child_ids.len(),
        ..ParentCounts::default()
    };

    for child_id in child_ids {
        match registry.get(child_id) {
            None => counts.missing += 1,
            Some(child) => count_child(&mut counts, child),
        }
    }

    counts
}

fn count_child(counts: &mut ParentCounts, child: JobSnapshot) {
    match &child.state {
        JobLifecycleState::Completed { .. } => counts.completed += 1,
        JobLifecycleState::Failed { .. } => {
            counts.failed += 1;
            counts.failed_children.push(failed_child(child));
        }
        JobLifecycleState::Running { .. } => counts.running += 1,
        JobLifecycleState::Queued { .. } => counts.queued += 1,
    }
}

fn failed_child(child: JobSnapshot) -> FailedChild {
    let info = snapshot_to_job_info(child);
    FailedChild {
        job_id: info.job_id,
        node_id: info.node_id,
        error_code: info.error_code,
        error_message: info.error_message,
        result_json: info.result_json,
    }
}

fn firmware_failed_child_detail(child: FailedChild) -> FirmwareFailedChildDetail {
    let parsed_result = parse_child_result(child.result_json);
    FirmwareFailedChildDetail {
        job_id: child.job_id,
        node_id: child.node_id,
        error_code: format!("{:?}", child.error_code),
        error_message: child.error_message,
        summary: parsed_result.summary,
        result: parsed_result.result,
        raw_result_json: parsed_result.raw_result_json,
    }
}

fn switch_failed_child_detail(child: FailedChild) -> SwitchFailedChildDetail {
    SwitchFailedChildDetail {
        job_id: child.job_id,
        node_id: child.node_id,
        error_message: child.error_message,
    }
}

fn refresh_terminal_parent(
    registry: &JobRegistry<RmsJobDomain>,
    parent_id: &JobId,
    child_ids: &[JobId],
    counts: ParentCounts,
    job_type: JobType,
) {
    if counts.failed > 0 || counts.missing > 0 {
        fail_parent(registry, parent_id, counts, job_type);
    } else {
        complete_parent(registry, parent_id, child_ids, counts.total, job_type);
    }
}

fn fail_parent(
    registry: &JobRegistry<RmsJobDomain>,
    parent_id: &JobId,
    counts: ParentCounts,
    job_type: JobType,
) {
    let ParentCounts {
        total,
        completed,
        failed,
        missing,
        failed_children,
        ..
    } = counts;

    let description = format!(
        "Batch complete: {completed}/{total} succeeded, {failed} failed{}",
        count_fragment(missing, "missing"),
    );
    let message = format!(
        "{} of {total} {} failed{}",
        failed + missing,
        job_type.batch_noun_plural(),
        missing_jobs_suffix(missing),
    );
    let result_json =
        parent_failure_result_json(job_type, completed, failed, missing, total, failed_children);
    let failure = JobFailure::new(JobError::Other, message)
        .with_description(description)
        .with_result_json(result_json);

    registry.mark_failed(parent_id, failure);
}

fn parent_failure_result_json(
    job_type: JobType,
    completed: usize,
    failed: usize,
    missing: usize,
    total: usize,
    failed_children: Vec<FailedChild>,
) -> String {
    match job_type {
        JobType::FirmwareUpdate => serialize_result_json(&FirmwareParentFailureResult {
            completed,
            failed,
            missing,
            total,
            failed_jobs: failed_children
                .into_iter()
                .map(firmware_failed_child_detail)
                .collect(),
        }),
        JobType::SwitchCertificate | JobType::SwitchSystemImageUpdate => {
            serialize_result_json(&SwitchParentFailureResult {
                completed,
                failed,
                missing,
                total,
                failed_jobs: failed_children
                    .into_iter()
                    .map(switch_failed_child_detail)
                    .collect(),
            })
        }
    }
}

fn complete_parent(
    registry: &JobRegistry<RmsJobDomain>,
    parent_id: &JobId,
    child_ids: &[JobId],
    total: usize,
    job_type: JobType,
) {
    let skipped = count_skipped_children(registry, child_ids);
    let description = if skipped == total {
        format!("All {total} nodes already up to date")
    } else if skipped > 0 {
        format!(
            "{}/{total} {} applied, {skipped} already up to date",
            total - skipped,
            job_type.batch_noun_plural()
        )
    } else {
        format!(
            "All {total} {} completed successfully",
            job_type.batch_noun_plural()
        )
    };

    registry.mark_completed(parent_id, description, "");
}

fn count_skipped_children(registry: &JobRegistry<RmsJobDomain>, child_ids: &[JobId]) -> usize {
    child_ids
        .iter()
        .filter(|child_id| {
            registry.get(child_id).is_some_and(|child| {
                let info = snapshot_to_job_info(child);
                result_is_already_up_to_date(&info.result_json)
            })
        })
        .count()
}

fn result_is_already_up_to_date(result_json: &str) -> bool {
    match serde_json::from_str::<FirmwareResultStatus>(result_json) {
        Ok(result) => result.status.as_deref() == Some("already_up_to_date"),
        Err(_) => false,
    }
}

fn parse_child_result(result_json: String) -> ParsedFirmwareChildResult {
    if result_json.is_empty() {
        return ParsedFirmwareChildResult {
            summary: None,
            result: None,
            raw_result_json: None,
        };
    }

    match serde_json::from_str::<FirmwareChildResult>(&result_json) {
        Ok(result) if !result.is_empty() => ParsedFirmwareChildResult {
            summary: result.summary().map(str::to_owned),
            result: Some(result),
            raw_result_json: None,
        },
        Ok(_) | Err(_) => ParsedFirmwareChildResult {
            summary: None,
            result: None,
            raw_result_json: Some(result_json),
        },
    }
}

fn serialize_result_json(payload: &impl Serialize) -> String {
    match serde_json::to_string(payload) {
        Ok(result_json) => result_json,
        Err(error) => {
            tracing::warn!(error = %error, "failed to serialize firmware job result");
            String::new()
        }
    }
}

fn count_fragment(count: usize, label: &str) -> String {
    if count > 0 {
        format!(", {count} {label}")
    } else {
        String::new()
    }
}

fn missing_jobs_suffix(missing: usize) -> String {
    if missing > 0 {
        format!(" ({missing} jobs missing)")
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use serde::Serialize;
    use tracing_subscriber::prelude::*;

    #[derive(Clone)]
    struct TestWriter {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    impl TestWriter {
        fn new() -> Self {
            Self {
                buf: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn text(&self) -> String {
            let guard = self.buf.lock().unwrap();
            String::from_utf8_lossy(&guard).into_owned()
        }
    }

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.buf.lock().unwrap();
            guard.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[derive(Serialize)]
    struct FirmwareChildResultFixture<'a> {
        status: &'static str,
        summary: &'a str,
        raw_error: &'static str,
    }

    fn age_job(tracker: &JobTracker, job_id: &str, age: Duration) {
        tracker.registry.age_job_for_test(&JobId::from(job_id), age);
    }

    #[test]
    fn create_job_returns_unique_ids() {
        let tracker = JobTracker::new();
        let id1 = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let id2 = tracker
            .create_job("rack-01", "node-02", JobType::FirmwareUpdate)
            .unwrap();
        assert_ne!(id1, id2);
        assert!(!id1.is_empty());
    }

    #[test]
    fn new_job_is_queued() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Queued);
        assert_eq!(job.rack_id, "rack-01");
        assert_eq!(job.node_id, "node-01");
        assert!(job.created_at <= Utc::now());
    }

    #[test]
    fn job_span_logfmt_output_contains_job_id_once() {
        let writer = TestWriter::new();
        let cloned_writer = writer.clone();
        let layer = crate::logging::logfmt::layer()
            .with_writer(Arc::new(move || Box::new(cloned_writer.clone())))
            .with_span_logs(false)
            .with_event_fields(vec!["job_id".to_owned(), "parent_job_id".to_owned()]);
        let subscriber = tracing_subscriber::registry().with(layer);

        let (job_id, parent_job_id) = tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            let job_id = uuid::Uuid::new_v4().to_string();
            let parent_job_id = uuid::Uuid::new_v4().to_string();
            let span = tracing::info_span!(
                JobType::FirmwareUpdate.span_name(),
                job_id = job_id.as_str(),
                parent_job_id = parent_job_id.as_str()
            );
            let _entered = span.enter();
            tracing::info!("job completed");
            (job_id, parent_job_id)
        });

        let output = writer.text();
        let completed_line = output
            .lines()
            .find(|line| line.contains(r#"msg="job completed""#))
            .unwrap_or_else(|| panic!("logfmt output did not contain job completion: {output}"));
        let job_id_fields = completed_line
            .split_ascii_whitespace()
            .filter(|field| field.starts_with("job_id="))
            .count();
        assert_eq!(
            job_id_fields, 1,
            "job completion event should contain job_id only from the span: {completed_line}"
        );
        assert!(
            output.contains(&format!("job_id={job_id}")),
            "logfmt output did not contain job_id {job_id}: {output}"
        );
        assert!(
            output.contains(&format!("parent_job_id={parent_job_id}")),
            "logfmt output did not contain parent_job_id {parent_job_id}: {output}"
        );
    }

    #[test]
    fn create_job_if_node_idle_rejects_queued_job_for_same_node() {
        let tracker = JobTracker::new();
        let active = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();

        let Err(failure) =
            tracker.create_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
        else {
            panic!("queued firmware job should block same node");
        };

        assert_eq!(job_error_of(&failure.error), JobError::UpdateInProgress);
        assert!(failure.message.contains(&active));
        assert!(failure.message.contains("rack-01"));
        assert!(failure.message.contains("node-01"));
    }

    #[test]
    fn create_job_if_node_idle_rejects_active_job_for_same_node() {
        let tracker = JobTracker::new();
        let active = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_running(&active, "Uploading firmware");

        let Err(failure) =
            tracker.create_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
        else {
            panic!("active firmware job should block same node");
        };

        assert_eq!(job_error_of(&failure.error), JobError::UpdateInProgress);
        assert!(failure.message.contains(&active));
        assert!(failure.message.contains("rack-01"));
        assert!(failure.message.contains("node-01"));
    }

    #[test]
    fn create_job_if_node_idle_allows_after_terminal_job() {
        let tracker = JobTracker::new();
        let active = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_completed(&active, None);

        let Ok(next) =
            tracker.create_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
        else {
            panic!("terminal firmware job should not block same node");
        };

        assert_ne!(active, next);
    }

    #[test]
    fn mark_running() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_running(&id, "Uploading firmware");
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Running);
        assert_eq!(job.state_description, "Uploading firmware");
    }

    #[test]
    fn mark_completed() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_completed(&id, Some(r#"{"status":"ok"}"#.to_owned()));
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Completed);
        assert!(job.result_json.contains("ok"));
    }

    #[test]
    fn mark_failed() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_failed(&id, JobError::Other, "BMC unreachable");
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_code, JobError::Other);
        assert_eq!(job.error_message, "BMC unreachable");
        assert!(job.result_json.contains(r#""status":"failed""#));
        assert!(job.result_json.contains(r#""error_code":"Other""#));
    }

    #[test]
    fn terminal_jobs_are_not_overwritten() {
        let tracker = JobTracker::new();
        let completed = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let failed = tracker
            .create_job("rack-01", "node-02", JobType::FirmwareUpdate)
            .unwrap();

        tracker.mark_completed(&completed, Some(r#"{"status":"ok"}"#.to_owned()));
        tracker.mark_failed(&completed, JobError::Other, "late failure");
        let completed_job = tracker.get_job(&completed).unwrap();
        assert_eq!(completed_job.state, JobState::Completed);
        assert_eq!(completed_job.result_json, r#"{"status":"ok"}"#);

        tracker.mark_failed(&failed, JobError::Other, "initial failure");
        tracker.mark_completed(&failed, Some(r#"{"status":"overwritten"}"#.to_owned()));
        let failed_job = tracker.get_job(&failed).unwrap();
        assert_eq!(failed_job.state, JobState::Failed);
        assert_eq!(failed_job.error_message, "initial failure");
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let tracker = JobTracker::new();
        assert!(tracker.get_job("nonexistent").is_none());
    }

    #[test]
    fn parent_job_created_with_children() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::FirmwareUpdate,
            )
            .unwrap();

        let parent = tracker.get_job(&parent_id).unwrap();
        assert!(parent.is_parent_job);
        assert_eq!(parent.state, JobState::Running);
        assert_eq!(parent.child_job_ids.len(), 2);
    }

    #[test]
    fn parent_job_empty_children_returns_none() {
        let tracker = JobTracker::new();
        assert!(
            tracker
                .create_parent_job("rack-01", &[], JobType::FirmwareUpdate)
                .is_none()
        );
    }

    #[test]
    fn parent_aggregates_all_completed() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::FirmwareUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, None);
        tracker.mark_completed(&c2, None);

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Completed);
        assert!(parent.state_description.contains("completed successfully"));
    }

    #[test]
    fn parent_aggregates_with_failures() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::FirmwareUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, None);
        tracker.mark_failed(&c2, JobError::ClientError, "connection lost");

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Failed);
        assert!(parent.state_description.contains("1/2 succeeded"));
        assert!(parent.result_json.contains("connection lost"));
        assert!(parent.result_json.contains(r#""error_code":"ClientError""#));
        let result = serde_json::from_str::<FirmwareParentFailureResult>(&parent.result_json)
            .expect("parent failure result should deserialize");
        assert_eq!(result.failed_jobs.len(), 1);
        let child_result = result.failed_jobs[0]
            .result
            .as_ref()
            .expect("default child failure should parse as typed result");
        assert_eq!(
            child_result.error_message.as_deref(),
            Some("connection lost")
        );
    }

    #[test]
    fn parent_failed_jobs_include_child_summary_from_result_json() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::FirmwareUpdate,
            )
            .unwrap();
        let summary = "PSU update failed on powerdevice0: LiteOn PSU update timed out";
        let child_result = serialize_result_json(&FirmwareChildResultFixture {
            status: "failed",
            summary,
            raw_error: "raw NVFWUPD detail",
        });

        tracker.mark_completed(&c1, None);
        tracker.mark_failed_with_result(&c2, JobError::ClientError, summary, Some(child_result));

        let parent = tracker.get_job(&parent_id).unwrap();
        let result = serde_json::from_str::<FirmwareParentFailureResult>(&parent.result_json)
            .expect("parent failure result should deserialize");
        let failed_job = &result.failed_jobs[0];
        assert_eq!(failed_job.summary.as_deref(), Some(summary));
        assert_eq!(failed_job.error_message, summary);
        let child_result = failed_job
            .result
            .as_ref()
            .expect("child failure result should parse");
        assert_eq!(
            child_result.raw_error.as_deref(),
            Some("raw NVFWUPD detail")
        );
    }

    #[test]
    fn parent_stays_running_while_children_active() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::FirmwareUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, None);

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Running);
        assert!(parent.state_description.contains("1/2 complete"));
    }

    #[test]
    fn parent_detects_skipped_jobs() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::FirmwareUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, Some(r#"{"status":"already_up_to_date"}"#.to_owned()));
        tracker.mark_completed(&c2, Some(r#"{"status":"already_up_to_date"}"#.to_owned()));

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Completed);
        assert!(parent.state_description.contains("already up to date"));
    }

    #[test]
    fn timestamps_are_set() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let job = tracker.get_job(&id).unwrap();
        assert!(job.created_at <= Utc::now());
        assert_eq!(job.created_at, job.updated_at);

        tracker.mark_running(&id, "running");
        let job = tracker.get_job(&id).unwrap();
        assert!(job.updated_at >= job.created_at);
    }

    #[test]
    fn concurrent_access() {
        let tracker = std::sync::Arc::new(JobTracker::new());

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let t = tracker.clone();
                std::thread::spawn(move || {
                    let id = t
                        .create_job("rack-01", &format!("node-{i:02}"), JobType::FirmwareUpdate)
                        .unwrap();
                    t.mark_running(&id, "running");
                    t.mark_completed(&id, None);
                    t.get_job(&id).unwrap()
                })
            })
            .collect();

        for h in handles {
            let job = h.join().unwrap();
            assert_eq!(job.state, JobState::Completed);
        }
    }

    #[test]
    fn default_trait() {
        let tracker = JobTracker::default();
        let id = tracker
            .create_job("r", "n", JobType::FirmwareUpdate)
            .unwrap();
        assert!(tracker.get_job(&id).is_some());
    }

    #[test]
    fn ttl_cleanup_removes_expired_jobs() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_completed(&id, None);

        age_job(
            &tracker,
            &id,
            Duration::from_millis(DEFAULT_TTL_MS as u64 + 1000),
        );

        // Creating a new job triggers cleanup
        let _new = tracker
            .create_job("rack-01", "node-02", JobType::FirmwareUpdate)
            .unwrap();
        assert!(tracker.get_job(&id).is_none());
    }

    #[test]
    fn create_job_refuses_when_at_capacity() {
        let tracker = JobTracker::with_max_tracked_jobs(1);
        let _first = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();

        let failure = tracker
            .create_job("rack-01", "node-02", JobType::FirmwareUpdate)
            .unwrap_err();
        assert_eq!(job_error_of(&failure.error), JobError::Internal);
        assert!(
            failure.message.contains("at capacity"),
            "unexpected message: {}",
            failure.message
        );
    }

    #[test]
    fn create_job_if_node_idle_refuses_when_at_capacity() {
        let tracker = JobTracker::with_max_tracked_jobs(1);
        let _first = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();

        let failure = tracker
            .create_job_if_node_idle("rack-01", "node-02", JobType::FirmwareUpdate)
            .unwrap_err();
        assert_eq!(job_error_of(&failure.error), JobError::Internal);
    }

    #[test]
    fn capacity_frees_after_terminal_job_expires() {
        let tracker = JobTracker::with_max_tracked_jobs(1);
        let id = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_completed(&id, None);
        age_job(
            &tracker,
            &id,
            Duration::from_millis(DEFAULT_TTL_MS as u64 + 1000),
        );

        let next = tracker
            .create_job("rack-01", "node-02", JobType::FirmwareUpdate)
            .unwrap();
        assert!(tracker.get_job(&next).is_some());
        assert!(tracker.get_job(&id).is_none());
    }

    #[tokio::test]
    async fn reaper_evicts_expired_terminal_jobs_without_create() {
        let tracker = Arc::new(JobTracker::new());
        let id = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_completed(&id, None);
        age_job(
            &tracker,
            &id,
            Duration::from_millis(DEFAULT_TTL_MS as u64 + 1000),
        );

        // A fast interval so the test does not wait the production cadence; the
        // key property is that no create_* call happens after this point.
        tracker.spawn_reaper_with_interval(Duration::from_millis(10));

        for _ in 0..50 {
            if tracker.get_job(&id).is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(tracker.get_job(&id).is_none());
    }

    #[test]
    fn reaper_evicts_unpolled_parent_without_get_job() {
        // Regression: a parent whose children are all terminal must be
        // aggregated and evicted by the reaper without any intervening
        // get_job. Previously the parent only transitioned to terminal on a
        // read, so a batch that was never polled leaked its parent record
        // permanently and could eventually exhaust tracker capacity.
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_completed(&c1, None);
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                std::slice::from_ref(&c1),
                JobType::FirmwareUpdate,
            )
            .unwrap();

        // Deliberately never call get_job(&parent_id): the parent is Running.
        tracker.reaper_sweep();

        // Read the raw record (not get_job, which would also aggregate) to
        // prove the reaper itself transitioned the parent to terminal.
        let raw = tracker
            .registry
            .get(&JobId::from(parent_id.as_str()))
            .unwrap();
        assert!(
            raw.state.is_terminal(),
            "reaper must transition an unpolled parent whose children are all terminal"
        );

        // Age child and parent past TTL; two passes remove child then parent.
        age_job(
            &tracker,
            &c1,
            Duration::from_millis(DEFAULT_TTL_MS as u64 + 1000),
        );
        age_job(
            &tracker,
            &parent_id,
            Duration::from_millis(DEFAULT_TTL_MS as u64 + 1000),
        );
        tracker.reaper_sweep();
        tracker.reaper_sweep();

        assert!(tracker.get_job(&c1).is_none());
        assert!(tracker.get_job(&parent_id).is_none());
        assert!(
            tracker.parent_job_types.lock().unwrap().is_empty(),
            "reaper must prune the parent-type side map for evicted parents"
        );
    }

    #[test]
    fn ttl_cleanup_protects_children_of_live_parent() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_completed(&c1, None);
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                std::slice::from_ref(&c1),
                JobType::FirmwareUpdate,
            )
            .unwrap();

        // Age the child past TTL but leave the parent fresh
        age_job(
            &tracker,
            &c1,
            Duration::from_millis(DEFAULT_TTL_MS as u64 + 1000),
        );

        let _new = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();

        // Child should still exist because parent protects it
        assert!(tracker.get_job(&c1).is_some());
        assert!(tracker.get_job(&parent_id).is_some());
    }

    #[test]
    fn ttl_cleanup_removes_expired_parent_and_children() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        tracker.mark_completed(&c1, None);
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                std::slice::from_ref(&c1),
                JobType::FirmwareUpdate,
            )
            .unwrap();

        // Force parent to completed state by reading it (triggers aggregation)
        let _ = tracker.get_job(&parent_id);

        // Age both past TTL
        age_job(
            &tracker,
            &c1,
            Duration::from_millis(DEFAULT_TTL_MS as u64 + 1000),
        );
        age_job(
            &tracker,
            &parent_id,
            Duration::from_millis(DEFAULT_TTL_MS as u64 + 1000),
        );

        // First cleanup pass removes children; parent retained because
        // children were still in the map at check time.
        tracker.cleanup_expired();
        assert!(tracker.get_job(&c1).is_none());
        let parent_after_child_cleanup = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent_after_child_cleanup.state, JobState::Completed);

        // Second pass removes the now-childless parent.
        tracker.cleanup_expired();
        assert!(tracker.get_job(&parent_id).is_none());
    }

    #[test]
    fn parent_with_missing_child() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let fake_child = "nonexistent-job-id".to_owned();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), fake_child],
                JobType::FirmwareUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, None);

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Failed);
        assert!(parent.state_description.contains("missing"));
        assert!(parent.error_message.contains("missing"));
    }

    #[test]
    fn parent_with_running_child_shows_progress() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();
        let c3 = tracker
            .create_job("rack-01", "n-03", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone(), c3.clone()],
                JobType::FirmwareUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, None);
        tracker.mark_running(&c2, "Uploading");
        // c3 stays queued

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Running);
        assert!(parent.state_description.contains("1/3 complete"));
        assert!(parent.state_description.contains("1 running"));
        assert!(parent.state_description.contains("1 queued"));
    }

    #[test]
    fn job_state_as_str_is_lowercase_wire_form() {
        assert_eq!(JobState::Queued.as_str(), "queued");
        assert_eq!(JobState::Running.as_str(), "running");
        assert_eq!(JobState::Completed.as_str(), "completed");
        assert_eq!(JobState::Failed.as_str(), "failed");
    }

    #[test]
    fn switch_system_image_job_can_be_tracked() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        tracker.mark_running(&id, "Inspecting current switch system image state");
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Running);
        assert_eq!(job.node_id, "sw-01");
    }

    #[test]
    fn mark_failed_message_records_error_without_result_json() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        tracker.mark_failed_message(&id, "SSH timeout");
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_message, "SSH timeout");
        assert!(job.result_json.is_empty());
    }

    #[test]
    fn mark_failed_message_with_result_preserves_payload() {
        let tracker = JobTracker::new();
        let id = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        tracker.mark_failed_message_with_result(
            &id,
            "cancelled before stage_image",
            Some(r#"{"timing_summary":{"stages":[]}}"#.to_owned()),
        );
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_message, "cancelled before stage_image");
        assert_eq!(job.result_json, r#"{"timing_summary":{"stages":[]}}"#);
    }

    #[test]
    fn switch_parent_uses_switch_wording_while_running() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "sw-02", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let parent = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::SwitchSystemImageUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, None);

        let info = tracker.get_job(&parent).unwrap();
        assert_eq!(info.state, JobState::Running);
        assert!(
            info.state_description
                .contains("Batch switch system image update in progress"),
            "unexpected description: {}",
            info.state_description
        );
        assert!(info.state_description.contains("1/2 complete"));
        // Firmware wording must not leak into a switch batch.
        assert!(!info.state_description.contains("firmware"));
    }

    #[test]
    fn switch_parent_completed_uses_switch_wording() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "sw-02", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let parent = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::SwitchSystemImageUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, None);
        tracker.mark_completed(&c2, None);

        let info = tracker.get_job(&parent).unwrap();
        assert_eq!(info.state, JobState::Completed);
        assert_eq!(
            info.state_description,
            "All 2 switch system image updates completed successfully"
        );
    }

    #[test]
    fn switch_parent_failure_emits_switch_shaped_result() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "sw-02", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let parent = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::SwitchSystemImageUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, None);
        tracker.mark_failed_message_with_result(
            &c2,
            "SSH timeout",
            Some(r#"{"status":"failed","node_id":"sw-02"}"#.to_owned()),
        );

        let info = tracker.get_job(&parent).unwrap();
        assert_eq!(info.state, JobState::Failed);
        assert_eq!(
            info.error_message,
            "1 of 2 switch system image updates failed"
        );
        // The switch batch result is switch-shaped: a lean failed_jobs list with
        // no firmware error codes or Redfish/target child parsing.
        assert!(
            !info.result_json.contains("error_code"),
            "switch parent result leaked a firmware error_code: {}",
            info.result_json
        );
        let parsed: SwitchParentFailureResult =
            serde_json::from_str(&info.result_json).expect("switch parent result should parse");
        assert_eq!(parsed.failed_jobs.len(), 1);
        assert_eq!(parsed.failed_jobs[0].node_id, "sw-02");
        assert_eq!(parsed.failed_jobs[0].error_message, "SSH timeout");
    }

    #[test]
    fn shutdown_signals_active_leaf_jobs() {
        let tracker = JobTracker::new();
        let running = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        tracker.mark_running(&running, "uploading");
        let token = tracker
            .registry
            .cancellation_token_for(&JobId::from(running.as_str()))
            .unwrap();

        let cancelled = tracker.shutdown();

        assert_eq!(cancelled, 1);
        assert!(token.is_cancelled());
    }

    #[test]
    fn parent_partial_skip() {
        let tracker = JobTracker::new();
        let c1 = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        let c2 = tracker
            .create_job("rack-01", "n-02", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job(
                "rack-01",
                &[c1.clone(), c2.clone()],
                JobType::FirmwareUpdate,
            )
            .unwrap();

        tracker.mark_completed(&c1, Some(r#"{"status":"already_up_to_date"}"#.to_owned()));
        tracker.mark_completed(&c2, Some(r#"{"status":"ok"}"#.to_owned()));

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Completed);
        assert!(
            parent
                .state_description
                .contains("1/2 firmware updates applied")
        );
        assert!(parent.state_description.contains("1 already up to date"));
    }
}
