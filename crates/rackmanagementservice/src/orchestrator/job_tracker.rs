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
use std::fmt::Display;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use chrono::{DateTime, Utc};
use prometheus::{CounterVec, IntGaugeVec, Opts};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use tokio_util::sync::CancellationToken;

use crate::orchestrator::job_lifecycle::{
    JobDomain, JobError, JobFailure, JobHandle, JobId, JobJoinHandle, JobLifecycleState,
    JobRegistry, JobSnapshot, JobSpec, JobState,
};
use crate::utilities::error::RmsError;

pub use crate::orchestrator::job_lifecycle::JobType;

/// The shared lifecycle domain for every async job the `JobTracker` owns
/// (firmware updates and switch system image updates alike). Because it is not
/// tied to a single `JobType`, its failure codes ([`JobError`]) and supervisor
/// fallback messages stay domain-neutral; per-`JobType` wording is applied at
/// aggregation time instead.
pub(crate) struct RmsJobDomain;

pub(crate) type RmsJobHandle = JobHandle<RmsJobDomain>;

impl JobDomain for RmsJobDomain {
    fn dropped_failure() -> JobFailure {
        // Supervisor fallbacks fire for any job type sharing this domain
        // (firmware + switch), so the wording stays job-type-neutral.
        job_failure(
            JobError::Internal,
            "job abandoned before recording a terminal state",
        )
        .with_description("Abandoned")
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

    fn shutting_down_failure() -> JobFailure {
        job_failure(
            JobError::Internal,
            "server is shutting down; new jobs are not accepted",
        )
    }

    fn parent_unavailable_failure(parent_job_id: &JobId) -> JobFailure {
        job_failure(
            JobError::Internal,
            format!("parent job {parent_job_id} is missing, terminal, or already a child"),
        )
    }
}

/// The information about a job that is returned by the `GetJobStatus` RPC.
/// This is a flattened view of the `JobSnapshot` struct, with portable field
/// types.
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
    pub parent_job_id: Option<String>,
}

impl JobInfo {
    /// Returns `true` when this job aggregates child jobs (a parent job).
    pub fn is_parent(&self) -> bool {
        !self.child_job_ids.is_empty()
    }
}

// ── JobTracker ──

/// Default retention period for terminal jobs when not configured.
const DEFAULT_TTL_MS: u64 = 24 * 3600 * 1000;

/// `error_code` label for workflows that completed successfully.
pub(crate) const WORKFLOW_SUCCESS_ERROR_CODE: &str = "none";

/// Converts a boolean value to a string label.
fn bool_label(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Upper bound on the reaper cadence, so eviction does not wait for a
/// `create_*` call. Clamped down to the TTL by [`reaper_interval`].
const REAPER_INTERVAL: Duration = Duration::from_secs(300);

/// Floor on the reaper cadence, so a tiny TTL cannot spin the sweep hot.
const MIN_REAPER_INTERVAL: Duration = Duration::from_secs(1);

/// Reaper sweep cadence for a retention `ttl`, clamped to
/// `[MIN_REAPER_INTERVAL, REAPER_INTERVAL]` so a short TTL is not undercut by
/// a slow sweep on an idle system.
fn reaper_interval(ttl: Duration) -> Duration {
    ttl.clamp(MIN_REAPER_INTERVAL, REAPER_INTERVAL)
}

/// Thread-safe registry for tracking asynchronous RMS jobs, keyed by `JobType`.
///
/// Stores `JobInfo` entries keyed by UUID. Background tokio tasks update
/// job progress, gRPC handlers query status. Terminal jobs (completed/failed)
/// are retained for a configurable TTL (24h by default) then cleaned up by the
/// background reaper or when an admission encounters a full registry.
///
/// Parent jobs aggregate state from their children on read.
pub struct JobTracker {
    registry: Arc<JobRegistry<RmsJobDomain>>,
    /// Owned handles for aggregate parents, which do not have a dedicated
    /// worker task. Parent aggregation consumes the handle on completion or
    /// failure.
    parent_job_handles: Mutex<HashMap<JobId, RmsJobHandle>>,
    /// Retention period for terminal jobs. Kept alongside the registry so the
    /// reaper can size its sweep cadence to the TTL (see [`reaper_interval`]).
    terminal_job_ttl: Duration,
    workflows_counts: CounterVec,
    /// In-flight workflow gauge maintained by inc-on-start / dec-on-terminal so
    /// the metrics subsystem needs no knowledge of workflows.
    workflows_in_flight: IntGaugeVec,
}

/// Configures and builds a [`JobTracker`].
pub struct JobTrackerBuilder<'a> {
    terminal_job_ttl: Duration,
    max_tracked_jobs: usize,
    metrics: Option<&'a prometheus::Registry>,
}

fn new_workflow_metrics() -> (CounterVec, IntGaugeVec) {
    let counts = CounterVec::new(
        Opts::new(
            "workflows_counts_total",
            "Total count of externally visible async workflow lifecycle events",
        ),
        &["workflow_type", "is_leaf", "workflow_state", "error_code"],
    )
    .expect("workflows_counts_total opts");
    let in_flight = IntGaugeVec::new(
        Opts::new(
            "workflows_in_flight",
            "Number of externally visible async workflows currently in flight",
        ),
        &["workflow_type", "is_leaf"],
    )
    .expect("workflows_in_flight opts");
    (counts, in_flight)
}

impl<'a> JobTrackerBuilder<'a> {
    /// Sets how long terminal jobs remain available for status queries.
    pub fn terminal_job_ttl(mut self, terminal_job_ttl: Duration) -> Self {
        self.terminal_job_ttl = terminal_job_ttl;
        self
    }

    /// Sets the maximum number of job records the tracker retains.
    pub fn max_tracked_jobs(mut self, max_tracked_jobs: usize) -> Self {
        self.max_tracked_jobs = max_tracked_jobs;
        self
    }

    /// Registers workflow metrics with the supplied Prometheus registry when
    /// the tracker is built.
    pub fn metrics(mut self, registry: &'a prometheus::Registry) -> Self {
        self.metrics = Some(registry);
        self
    }

    /// Builds the tracker, registering workflow metrics when [`Self::metrics`]
    /// was called.
    pub fn build(self) -> Result<JobTracker, RmsError> {
        let (workflows_counts, workflows_in_flight) = new_workflow_metrics();

        let mut registry = JobRegistry::with_capacity(self.terminal_job_ttl, self.max_tracked_jobs);

        // The registry is the single chokepoint for lifecycle transitions, so
        // workflow gauge/counter maintenance is driven entirely by its
        // creation and terminal observers. Both capture cheap Prometheus metric
        // handles (not the tracker) to avoid an `Arc` cycle.
        //
        // Both observers fire after the registry write lock is released so a
        // panic in metrics code cannot poison the lock. Creation still precedes
        // any post-create parent refresh on the same thread, keeping queued
        // and terminal increments ordered for parents whose children are already
        // terminal at create time.
        let counts = workflows_counts.clone();
        let started_in_flight = workflows_in_flight.clone();
        registry.set_created_observer(Arc::new(move |snapshot: &JobSnapshot| {
            record_workflow_started_metrics(&counts, &started_in_flight, snapshot);
        }));

        let counts = workflows_counts.clone();
        let in_flight = workflows_in_flight.clone();
        registry.set_terminal_observer(Arc::new(move |snapshot: &JobSnapshot| {
            record_workflow_terminal_metrics(&counts, &in_flight, snapshot);
        }));

        let tracker = JobTracker {
            registry: Arc::new(registry),
            parent_job_handles: Mutex::new(HashMap::new()),
            terminal_job_ttl: self.terminal_job_ttl,
            workflows_counts,
            workflows_in_flight,
        };

        if let Some(metrics) = self.metrics {
            tracker.register_workflow_metrics(metrics)?;
        }

        Ok(tracker)
    }
}

impl JobTracker {
    pub fn builder() -> JobTrackerBuilder<'static> {
        JobTrackerBuilder {
            terminal_job_ttl: Duration::from_millis(DEFAULT_TTL_MS),
            max_tracked_jobs: crate::orchestrator::job_lifecycle::MAX_TRACKED_JOBS,
            metrics: None,
        }
    }

    pub fn new() -> Self {
        Self::builder()
            .build()
            .expect("building JobTracker without a metrics registry cannot fail")
    }

    /// Builds a tracker with explicit terminal-job retention bounds.
    pub fn with_retention_limits(terminal_job_ttl: Duration, max_tracked_jobs: usize) -> Self {
        Self::builder()
            .terminal_job_ttl(terminal_job_ttl)
            .max_tracked_jobs(max_tracked_jobs)
            .build()
            .expect("building JobTracker without a metrics registry cannot fail")
    }

    /// Registers the workflow metric vectors with `metrics`. Treats an
    /// already-registered metric as success so repeated initialization (e.g.
    /// across tests sharing a registry) is a no-op.
    fn register_workflow_metrics(&self, metrics: &prometheus::Registry) -> Result<(), RmsError> {
        let collectors: [Box<dyn prometheus::core::Collector>; 2] = [
            Box::new(self.workflows_counts.clone()),
            Box::new(self.workflows_in_flight.clone()),
        ];
        for collector in collectors {
            match metrics.register(collector) {
                Ok(()) | Err(prometheus::Error::AlreadyReg) => {}
                Err(e) => {
                    return Err(RmsError::internal(format!(
                        "failed to register workflow metrics: {e}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Spawns a periodic reaper so terminal-job eviction does not depend on a
    /// new job being created. The task holds a [`Weak`] reference and exits once
    /// every strong owner of the tracker is dropped.
    ///
    /// The sweep cadence is sized to the configured TTL via `reaper_interval`
    /// so a short `--terminal-job-ttl-seconds` is not silently undercut by a
    /// slow fixed sweep on an idle system.
    pub fn spawn_reaper(self: &Arc<Self>) {
        self.spawn_reaper_with_interval(reaper_interval(self.terminal_job_ttl));
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

    /// One reaper pass: aggregate parent state, then evict expired terminal jobs.
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
    }

    /// Aggregates every tracked parent's state so parents whose children are
    /// all terminal transition to terminal without an external `get_job`.
    fn refresh_all_parents(&self) {
        for parent_id in self.registry.parent_job_ids() {
            refresh_parent_state(self, &parent_id);
        }
    }

    /// Creates an owned job handle without checking for an active job on the
    /// same node.
    pub(crate) fn create_job(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let job = self.registry.create_job(
            JobSpec::new(rack_id, node_id, "Queued", job_span(job_type, "")),
            None,
            false,
        )?;

        self.record_job_id_in_span(&job);
        tracing::info!(
            job_id = %job.id(),
            rack_id,
            node_id,
            "job created"
        );
        Ok(job)
    }

    /// Create a firmware job only when the target node has no active job.
    ///
    /// Fails with a [`JobError::UpdateInProgress`] failure when a queued or running
    /// leaf job already targets the same `(rack_id, node_id)`, or
    /// [`JobError::Internal`] when the tracker is full.
    #[cfg(test)]
    pub(crate) fn create_job_if_node_idle(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let job = self.registry.create_job(
            JobSpec::new(rack_id, node_id, "Queued", job_span(job_type, "")),
            None,
            true,
        )?;

        self.record_job_id_in_span(&job);
        tracing::info!(
            job_id = %job.id(),
            rack_id,
            node_id,
            "job created"
        );
        Ok(job)
    }

    pub(crate) fn create_child_job(
        &self,
        parent_job_id: &str,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let parent_id = JobId::from(parent_job_id);
        let job = self.registry.create_job(
            JobSpec::new(
                rack_id,
                node_id,
                "Queued",
                job_span(job_type, parent_job_id),
            ),
            Some(&parent_id),
            false,
        )?;
        self.record_job_id_in_span(&job);
        tracing::info!(
            job_id = %job.id(),
            parent_job_id,
            rack_id,
            node_id,
            "child job created"
        );
        refresh_parent_state(self, &parent_id);
        Ok(job)
    }

    pub(crate) fn create_child_job_if_node_idle(
        &self,
        parent_job_id: &str,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let parent_id = JobId::from(parent_job_id);
        let job = self.registry.create_job(
            JobSpec::new(
                rack_id,
                node_id,
                "Queued",
                job_span(job_type, parent_job_id),
            ),
            Some(&parent_id),
            true,
        )?;
        self.record_job_id_in_span(&job);
        tracing::info!(
            job_id = %job.id(),
            parent_job_id,
            rack_id,
            node_id,
            "child job created"
        );
        refresh_parent_state(self, &parent_id);
        Ok(job)
    }

    /// Creates a leaf job when the target node is idle and registers it as an
    /// externally visible workflow (the job ID returned to API callers).
    pub(crate) fn create_visible_workflow_job_if_node_idle(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let job = self.registry.create_job(
            JobSpec::new(rack_id, node_id, "Queued", job_span(job_type, ""))
                .with_workflow_type(job_type),
            None,
            true,
        )?;

        self.record_job_id_in_span(&job);
        tracing::info!(
            job_id = %job.id(),
            rack_id,
            node_id,
            "job created"
        );
        // Started/in-flight metrics are emitted by the registry's creation
        // observer because the spec carries a `workflow_type`.
        Ok(job)
    }

    pub(crate) fn spawn_job<F, Fut>(self: &Arc<Self>, job: RmsJobHandle, run: F) -> JobJoinHandle
    where
        F: FnOnce(RmsJobHandle) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let weak = Arc::downgrade(self);
        let job_id = job.id().clone();
        self.registry.spawn_job(job, move |job| async move {
            run(job).await;
            if let Some(tracker) = weak.upgrade() {
                tracker.after_job_terminal(&job_id);
            }
        })
    }

    /// Returns the cooperative cancellation token for a manually spawned job.
    ///
    /// New workers should prefer [`Self::spawn_job`], which provides the token
    /// through [`RmsJobHandle`]. This bridge keeps legacy workers wired
    /// to registry shutdown until they migrate to supervised handles.
    #[cfg(test)]
    pub(crate) fn cancellation_token_for(&self, job_id: &str) -> Option<CancellationToken> {
        self.registry.cancellation_token_for(&JobId::from(job_id))
    }

    /// Hands out the underlying lifecycle registry so generic `job_lifecycle`
    /// utilities (such as `CleanupPlan`) can operate on the jobs this tracker
    /// owns without re-implementing registry mechanics on the tracker.
    pub(crate) fn registry(&self) -> Arc<JobRegistry<RmsJobDomain>> {
        self.registry.clone()
    }

    pub fn create_parent_job(&self, rack_id: &str, job_type: JobType) -> Option<String> {
        let parent_handle = self
            .registry
            .create_job(
                JobSpec::new_parent(
                    rack_id,
                    format!("Batch {} accepting child jobs", job_type.batch_noun()),
                    job_span(job_type, ""),
                )
                .with_workflow_type(job_type),
                None,
                false,
            )
            .ok()?;
        let parent_id = parent_handle.id().clone();
        let job_id = parent_id.to_string();
        self.parent_job_handles
            .lock()
            .unwrap()
            .insert(parent_id.clone(), parent_handle);

        // Started/in-flight metrics were already emitted by the registry's
        // creation observer inside `create_parent`. Refresh afterward
        // re-aggregates any children that completed before this thread returned
        // from `create_parent`.
        if let Some(span) = self.registry.span_for_job(&parent_id) {
            span.record("job_id", job_id.as_str());
        }

        tracing::info!(
            job_id = %job_id,
            rack_id,
            "parent job created"
        );
        Some(job_id)
    }

    pub fn get_job(&self, job_id: &str) -> Option<JobInfo> {
        let id = JobId::from(job_id);
        let job = self.registry.get(&id)?;

        if job.is_parent() {
            refresh_parent_state(self, &id);
        }

        self.registry.get(&id).map(snapshot_to_job_info)
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

    /// Completes a tracker-owned aggregate parent.
    ///
    /// Leaf workers must complete their owned `RmsJobHandle` instead.
    pub fn mark_completed(&self, job_id: &str, result_json: Option<String>) {
        let job_id = JobId::from(job_id);
        let result_json = result_json.unwrap_or_default();
        let transitioned =
            if let Some(handle) = self.parent_job_handles.lock().unwrap().remove(&job_id) {
                handle.complete("Completed", result_json);
                true
            } else {
                false
            };

        if transitioned {
            let span = self
                .registry
                .span_for_job(&job_id)
                .unwrap_or_else(tracing::Span::none);
            let _entered = span.enter();
            tracing::info!("job completed");
            self.after_job_terminal(&job_id);
        } else {
            tracing::error!(%job_id, "mark_completed called for unknown or terminal job");
        }
    }

    /// Fails a tracker-owned aggregate parent.
    ///
    /// Leaf workers must fail their owned `RmsJobHandle` instead.
    pub fn mark_failed(&self, job_id: &str, error_code: JobError, error_message: &str) {
        self.mark_failed_with_result(job_id, error_code, error_message, None);
    }

    /// Fails a tracker-owned aggregate parent with an optional result payload.
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

        let job_id = JobId::from(job_id);
        let transitioned =
            if let Some(handle) = self.parent_job_handles.lock().unwrap().remove(&job_id) {
                handle.fail(failure);
                true
            } else {
                false
            };

        if transitioned {
            let span = self
                .registry
                .span_for_job(&job_id)
                .unwrap_or_else(tracing::Span::none);
            let _entered = span.enter();
            tracing::error!(error_message, "job failed");
            self.after_job_terminal(&job_id);
        } else {
            tracing::error!(%job_id, "mark_failed called for unknown or terminal job");
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
    /// of jobs signalled. New job creation is refused from this point on, so
    /// an RPC still completing during shutdown cannot start work that would
    /// escape the cancellation snapshot. Intended for graceful server
    /// shutdown.
    pub fn shutdown(&self) -> usize {
        self.registry.begin_shutdown();
        self.registry.cancel_active_leaves().len()
    }

    /// Cooperatively cancel every non-terminal leaf job, then wait (bounded by
    /// `timeout`) for those jobs to reach a terminal state before returning the
    /// number of jobs signalled. Intended for graceful server shutdown so that
    /// in-flight work (e.g. a switch-image SFTP push) observes cancellation and
    /// unwinds rather than being torn down mid-operation when the runtime is
    /// dropped.
    ///
    /// A `false` result from the underlying wait (timeout elapsed with jobs
    /// still active) is logged; callers proceed with shutdown regardless so a
    /// wedged job cannot block teardown indefinitely.
    pub async fn shutdown_and_await_terminal(&self, timeout: Duration) -> usize {
        self.registry.begin_shutdown();
        let job_ids = self.registry.cancel_active_leaves();
        let to_cancel = job_ids.len();
        if to_cancel == 0 {
            return 0;
        }

        let reached_terminal = self
            .registry
            .wait_for_terminal_jobs(&job_ids, Some(timeout))
            .await;
        if !reached_terminal {
            tracing::warn!(
                jobs_cancelled = to_cancel,
                timeout_secs = timeout.as_secs(),
                "graceful shutdown wait timed out; some jobs did not reach a \
                 terminal state before the deadline"
            );
        }

        to_cancel
    }

    // Two-phase TTL cleanup: first remove expired leaf jobs (protecting
    // children of live parents), then remove childless expired parents.
    #[cfg(test)]
    fn cleanup_expired(&self) {
        self.registry.cleanup_expired();
    }

    fn record_job_id_in_span(&self, job: &RmsJobHandle) {
        if let Some(span) = self.registry.span_for_job(job.id()) {
            span.record("job_id", job.id().as_ref());
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

fn job_span(job_type: JobType, parent_job_id: &str) -> tracing::Span {
    // The span name must be a compile-time constant for the macro callsite, so
    // each `JobType` needs an explicit arm rather than a runtime `span_name()`.
    match (job_type, parent_job_id.is_empty()) {
        (JobType::FirmwareUpdate, true) => tracing::info_span!(
            "firmware_update",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::FirmwareUpdate, false) => tracing::info_span!(
            "firmware_update",
            job_id = tracing::field::Empty,
            parent_job_id
        ),
        (JobType::ConfigureScaleUpFabricManagerV2, true) => tracing::info_span!(
            "configure_scale_up_fabric_manager_v2",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::ConfigureScaleUpFabricManagerV2, false) => tracing::info_span!(
            "configure_scale_up_fabric_manager_v2",
            job_id = tracing::field::Empty,
            parent_job_id
        ),
        (JobType::SwitchCertificate, true) => tracing::info_span!(
            "switch_certificate",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchCertificate, false) => tracing::info_span!(
            "switch_certificate",
            job_id = tracing::field::Empty,
            parent_job_id
        ),
        (JobType::SwitchMtlsDisable, true) => tracing::info_span!(
            "switch_mtls_disable",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchMtlsDisable, false) => tracing::info_span!(
            "switch_mtls_disable",
            job_id = tracing::field::Empty,
            parent_job_id
        ),
        (JobType::SwitchSdnFactoryDefaultReset, true) => tracing::info_span!(
            "switch_sdn_factory_default_reset",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchSdnFactoryDefaultReset, false) => tracing::info_span!(
            "switch_sdn_factory_default_reset",
            job_id = tracing::field::Empty,
            parent_job_id
        ),
        (JobType::SwitchFactoryDefaultReset, true) => tracing::info_span!(
            "switch_factory_default_reset",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchFactoryDefaultReset, false) => tracing::info_span!(
            "switch_factory_default_reset",
            job_id = tracing::field::Empty,
            parent_job_id
        ),
        (JobType::SwitchSystemPasswordUpdate, true) => tracing::info_span!(
            "switch_system_password_update",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchSystemPasswordUpdate, false) => tracing::info_span!(
            "switch_system_password_update",
            job_id = tracing::field::Empty,
            parent_job_id
        ),
        (JobType::SwitchSystemImageUpdate, true) => tracing::info_span!(
            "switch_system_image_update",
            job_id = tracing::field::Empty,
            parent_job_id = tracing::field::Empty
        ),
        (JobType::SwitchSystemImageUpdate, false) => tracing::info_span!(
            "switch_system_image_update",
            job_id = tracing::field::Empty,
            parent_job_id
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

pub(crate) fn job_failure(error_code: JobError, error_message: impl Display) -> JobFailure {
    let error_message = error_message.to_string();
    JobFailure::new(error_code, error_message.clone())
        .with_result_json(default_failure_result_json(error_code, &error_message))
}

impl JobTracker {
    /// Propagates a child's terminal transition up to its parent so parent
    /// aggregation does not depend on an external poll. Workflow metrics are
    /// handled by the registry's terminal observer, not here.
    fn after_job_terminal(&self, job_id: &JobId) {
        self.refresh_parent_for_child(job_id);
    }

    /// Refreshes a child's parent after callers seal owned handles directly.
    pub(crate) fn refresh_parent_for_child(&self, child_id: &JobId) {
        let Some(parent_id) = self.registry.parent_for_job(child_id) else {
            return;
        };
        refresh_parent_state(self, &parent_id);
    }
}

/// Emits creation workflow metrics for a job that was just created:
/// increments the queued counter and the in-flight gauge. A no-op for jobs
/// that are not externally visible workflows.
///
/// Invoked from the registry's creation observer after the write lock is
/// released. Parent refresh that may follow creation on the same thread still
/// runs after this returns, so queued metrics precede terminal metrics when a
/// parent is created with already-terminal children.
fn record_workflow_started_metrics(
    workflows_counts: &CounterVec,
    workflows_in_flight: &IntGaugeVec,
    snapshot: &JobSnapshot,
) {
    let Some(workflow_type) = snapshot.workflow_type else {
        return;
    };
    let workflow_type_label = workflow_type.span_name();
    let is_leaf_label = bool_label(!snapshot.node_id.is_empty());
    workflows_counts
        .with_label_values(&[
            workflow_type_label,
            is_leaf_label,
            JobState::Queued.as_str(),
            WORKFLOW_SUCCESS_ERROR_CODE,
        ])
        .inc();
    workflows_in_flight
        .with_label_values(&[workflow_type_label, is_leaf_label])
        .inc();
}

/// Emits terminal workflow metrics for a job that just reached a terminal
/// state: increments the finished counter and decrements the in-flight gauge.
/// A no-op for jobs that are not externally visible workflows.
///
/// Invoked from the registry's terminal-transition observer, so it runs exactly
/// once per workflow regardless of how the job terminalized (worker
/// completion/failure, RAII drop, supervisor-observed panic/cancel, or parent
/// aggregation).
fn record_workflow_terminal_metrics(
    workflows_counts: &CounterVec,
    workflows_in_flight: &IntGaugeVec,
    snapshot: &JobSnapshot,
) {
    let Some(workflow_type) = snapshot.workflow_type else {
        return;
    };
    let workflow_type_label = workflow_type.span_name();
    let is_leaf_label = bool_label(!snapshot.node_id.is_empty());
    let (workflow_state, error_code) = match &snapshot.state {
        JobLifecycleState::Completed { .. } => {
            (JobState::Completed.as_str(), WORKFLOW_SUCCESS_ERROR_CODE)
        }
        JobLifecycleState::Failed { failure, .. } => (
            JobState::Failed.as_str(),
            job_error_of(failure).metrics_label(),
        ),
        JobLifecycleState::Queued { .. } | JobLifecycleState::Running { .. } => return,
    };
    workflows_counts
        .with_label_values(&[
            workflow_type_label,
            is_leaf_label,
            workflow_state,
            error_code,
        ])
        .inc();
    workflows_in_flight
        .with_label_values(&[workflow_type_label, is_leaf_label])
        .dec();
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
        parent_job_id: snapshot.parent_job_id.map(String::from),
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

fn refresh_parent_state(tracker: &JobTracker, parent_id: &JobId) {
    let registry = &tracker.registry;
    let Some(parent) = registry.get(parent_id) else {
        return;
    };
    if !parent.is_parent() {
        return;
    }
    let Some(job_type) = parent.workflow_type else {
        return;
    };

    let counts = collect_parent_counts(registry, &parent.child_job_ids);
    if counts.total == 0 {
        return;
    }
    if counts.done() == counts.total {
        refresh_terminal_parent(tracker, parent_id, &parent.child_job_ids, counts, job_type);
    } else if let Some(handle) = tracker.parent_job_handles.lock().unwrap().get(parent_id) {
        handle.progress(counts.running_description(job_type));
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
    tracker: &JobTracker,
    parent_id: &JobId,
    child_ids: &[JobId],
    counts: ParentCounts,
    job_type: JobType,
) {
    if counts.failed > 0 || counts.missing > 0 {
        fail_parent(tracker, parent_id, counts, job_type);
    } else {
        complete_parent(tracker, parent_id, child_ids, counts.total, job_type);
    }
}

fn fail_parent(tracker: &JobTracker, parent_id: &JobId, counts: ParentCounts, job_type: JobType) {
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

    // The registry's terminal observer records workflow metrics on the
    // resulting transition.
    if let Some(handle) = tracker.parent_job_handles.lock().unwrap().remove(parent_id) {
        handle.fail(failure);
    } else {
        tracing::error!(%parent_id, "parent job handle missing during failure aggregation");
    }
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
        JobType::SwitchCertificate
        | JobType::SwitchMtlsDisable
        | JobType::SwitchSdnFactoryDefaultReset
        | JobType::SwitchFactoryDefaultReset
        | JobType::SwitchSystemPasswordUpdate
        | JobType::SwitchSystemImageUpdate
        | JobType::ConfigureScaleUpFabricManagerV2 => {
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
    tracker: &JobTracker,
    parent_id: &JobId,
    child_ids: &[JobId],
    total: usize,
    job_type: JobType,
) {
    let registry = &tracker.registry;
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

    // The registry's terminal observer records workflow metrics on the
    // resulting transition.
    if let Some(handle) = tracker.parent_job_handles.lock().unwrap().remove(parent_id) {
        handle.complete(description, "");
    } else {
        tracing::error!(%parent_id, "parent job handle missing during completion aggregation");
    }
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

    use prometheus::{Registry, TextEncoder};
    use serde::Serialize;
    use tracing_subscriber::prelude::*;

    fn workflow_metrics_setup() -> (Arc<Registry>, Arc<JobTracker>) {
        let registry = Arc::new(Registry::new_custom(Some("rms".to_owned()), None).unwrap());
        let tracker = Arc::new(
            JobTracker::builder()
                .metrics(registry.as_ref())
                .build()
                .expect("register workflow metrics"),
        );
        (registry, tracker)
    }

    fn metric_value(body: &str, prefix: &str) -> f64 {
        body.lines()
            .find(|line| line.starts_with(prefix))
            .map(|sample| {
                sample
                    .rsplit_once(' ')
                    .map(|(_, value)| value)
                    .expect("sample line should end with a value")
                    .trim()
                    .parse()
                    .expect("metric value should parse as f64")
            })
            .unwrap_or(0.0)
    }

    fn gather_workflow_metrics(registry: &Registry) -> String {
        TextEncoder::new()
            .encode_to_string(&registry.gather())
            .expect("encode metrics")
    }

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

    fn create_test_batch(
        tracker: &JobTracker,
        rack_id: &str,
        node_ids: &[&str],
        job_type: JobType,
    ) -> (String, Vec<RmsJobHandle>) {
        let parent_id = tracker.create_parent_job(rack_id, job_type).unwrap();
        let children = node_ids
            .iter()
            .map(|node_id| {
                tracker
                    .create_child_job(&parent_id, rack_id, node_id, job_type)
                    .unwrap()
            })
            .collect();
        (parent_id, children)
    }

    fn complete_child_and_refresh(tracker: &JobTracker, child: RmsJobHandle, result_json: &str) {
        let child_id = child.id().clone();
        child.complete("Completed", result_json);
        tracker.refresh_parent_for_child(&child_id);
    }

    fn fail_child_and_refresh(tracker: &JobTracker, child: RmsJobHandle, failure: JobFailure) {
        let child_id = child.id().clone();
        child.fail(failure);
        tracker.refresh_parent_for_child(&child_id);
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
    fn child_job_span_records_child_and_parent_job_ids() {
        let writer = TestWriter::new();
        let cloned_writer = writer.clone();
        let layer = crate::logging::logfmt::layer()
            .with_writer(Arc::new(move || Box::new(cloned_writer.clone())))
            .with_span_logs(false)
            .with_event_fields(vec!["job_id".to_owned(), "parent_job_id".to_owned()]);
        let subscriber = tracing_subscriber::registry().with(layer);

        let (child_job_id, parent_job_id) = tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            let tracker = JobTracker::new();
            let parent_job_id = tracker
                .create_parent_job("rack-01", JobType::FirmwareUpdate)
                .unwrap();
            let child = tracker
                .create_child_job(
                    &parent_job_id,
                    "rack-01",
                    "node-01",
                    JobType::FirmwareUpdate,
                )
                .unwrap();
            let child_job_id = child.id().to_string();
            let span = tracker.span_for_job(&child_job_id).unwrap();
            let _entered = span.enter();
            tracing::info!("child job work");
            (child_job_id, parent_job_id)
        });

        let output = writer.text();
        let child_work_line = output
            .lines()
            .find(|line| line.contains(r#"msg="child job work""#))
            .unwrap_or_else(|| panic!("logfmt output did not contain child job work: {output}"));
        assert!(
            child_work_line.contains(&format!("job_id={child_job_id}")),
            "child span did not contain child job ID {child_job_id}: {child_work_line}"
        );
        assert!(
            child_work_line.contains(&format!("parent_job_id={parent_job_id}")),
            "child span did not contain parent job ID {parent_job_id}: {child_work_line}"
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
        assert!(failure.message.contains(active.id().as_ref()));
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
        assert!(failure.message.contains(active.id().as_ref()));
        assert!(failure.message.contains("rack-01"));
        assert!(failure.message.contains("node-01"));
    }

    #[test]
    fn create_job_if_node_idle_allows_after_terminal_job() {
        let tracker = JobTracker::new();
        let active = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let active_id = active.id().to_string();
        active.complete("Completed", "");

        let Ok(next) =
            tracker.create_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
        else {
            panic!("terminal firmware job should not block same node");
        };

        assert_ne!(active_id, next.id().as_ref());
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
    fn job_handle_complete() {
        let tracker = JobTracker::new();
        let job = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let id = job.id().to_string();
        job.complete("Completed", r#"{"status":"ok"}"#);
        let info = tracker.get_job(&id).unwrap();
        assert_eq!(info.state, JobState::Completed);
        assert!(info.result_json.contains("ok"));
    }

    #[test]
    fn job_handle_fail() {
        let tracker = JobTracker::new();
        let job = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let id = job.id().to_string();
        job.fail(
            JobFailure::new(JobError::Other, "BMC unreachable").with_result_json(
                default_failure_result_json(JobError::Other, "BMC unreachable"),
            ),
        );
        let info = tracker.get_job(&id).unwrap();
        assert_eq!(info.state, JobState::Failed);
        assert_eq!(info.error_code, JobError::Other);
        assert_eq!(info.error_message, "BMC unreachable");
        assert!(info.result_json.contains(r#""status":"failed""#));
        assert!(info.result_json.contains(r#""error_code":"Other""#));
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let tracker = JobTracker::new();
        assert!(tracker.get_job("nonexistent").is_none());
    }

    #[test]
    fn parent_job_created_with_children() {
        let tracker = JobTracker::new();
        let (parent_id, _children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let parent = tracker.get_job(&parent_id).unwrap();
        assert!(parent.is_parent());
        assert_eq!(parent.state, JobState::Running);
        assert_eq!(parent.child_job_ids.len(), 2);
    }

    #[test]
    fn child_job_reports_parent_job_id_after_parent_creation() {
        let tracker = JobTracker::new();
        let (parent_id, children) =
            create_test_batch(&tracker, "rack-01", &["n-01"], JobType::FirmwareUpdate);
        let child = &children[0];

        let child_info = tracker.get_job(child).unwrap();
        assert_eq!(
            child_info.parent_job_id.as_deref(),
            Some(parent_id.as_str())
        );
        assert_eq!(
            tracker
                .registry()
                .parent_for_job(child.id())
                .map(|id| id.to_string()),
            Some(parent_id)
        );
    }

    #[test]
    fn job_without_children_is_not_reported_as_parent() {
        let tracker = JobTracker::new();
        let parent_id = tracker
            .create_parent_job("rack-01", JobType::FirmwareUpdate)
            .expect("empty parent should be created");

        let parent = tracker.get_job(&parent_id).unwrap();
        assert!(!parent.is_parent());
        assert!(parent.child_job_ids.is_empty());
        assert_eq!(parent.state, JobState::Running);
    }

    #[test]
    fn parent_aggregates_all_completed() {
        let tracker = JobTracker::new();
        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        complete_child_and_refresh(&tracker, c1, "");
        complete_child_and_refresh(&tracker, c2, "");

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Completed);
        assert!(parent.state_description.contains("completed successfully"));
    }

    #[tokio::test]
    async fn spawned_child_completion_refreshes_parent_without_polling() {
        let tracker = Arc::new(JobTracker::new());
        let (parent_id, mut children) = create_test_batch(
            tracker.as_ref(),
            "rack-01",
            &["n-01"],
            JobType::SwitchCertificate,
        );
        let child = children.pop().expect("child job should be created");

        tracker
            .spawn_job(child, |job| async move {
                job.complete("Completed", "{}");
            })
            .wait()
            .await
            .expect("child job supervisor should complete");

        let parent = tracker
            .registry
            .get(&JobId::from(parent_id.as_str()))
            .expect("parent job should exist");
        assert_eq!(snapshot_state(&parent.state), JobState::Completed);
    }

    #[test]
    fn parent_aggregates_with_failures() {
        let tracker = JobTracker::new();
        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        c1.complete("Completed", "");
        c2.fail(
            JobFailure::new(JobError::ClientError, "connection lost").with_result_json(
                default_failure_result_json(JobError::ClientError, "connection lost"),
            ),
        );

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
        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });
        let summary = "PSU update failed on powerdevice0: LiteOn PSU update timed out";
        let child_result = serialize_result_json(&FirmwareChildResultFixture {
            status: "failed",
            summary,
            raw_error: "raw NVFWUPD detail",
        });

        c1.complete("Completed", "");
        c2.fail(JobFailure::new(JobError::ClientError, summary).with_result_json(child_result));

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
        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let [c1, _c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        c1.complete("Completed", "");

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Running);
        assert!(parent.state_description.contains("1/2 complete"));
    }

    #[test]
    fn parent_detects_skipped_jobs() {
        let tracker = JobTracker::new();
        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        c1.complete("Completed", r#"{"status":"already_up_to_date"}"#);
        c2.complete("Completed", r#"{"status":"already_up_to_date"}"#);

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
                    let job = t
                        .create_job("rack-01", &format!("node-{i:02}"), JobType::FirmwareUpdate)
                        .unwrap();
                    let id = job.id().to_string();
                    job.progress("running");
                    job.complete("Completed", "");
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
    fn explicit_ttl_cleanup_removes_expired_jobs() {
        let tracker = JobTracker::new();
        let job = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let id = job.id().to_string();
        job.complete("Completed", "");

        age_job(&tracker, &id, Duration::from_millis(DEFAULT_TTL_MS + 1000));

        tracker.cleanup_expired();
        assert!(tracker.get_job(&id).is_none());
    }

    #[test]
    fn create_job_refuses_when_at_capacity() {
        let tracker = JobTracker::builder().max_tracked_jobs(1).build().unwrap();
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
        let tracker = JobTracker::builder().max_tracked_jobs(1).build().unwrap();
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
        let tracker = JobTracker::builder().max_tracked_jobs(1).build().unwrap();
        let job = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let id = job.id().to_string();
        job.complete("Completed", "");
        age_job(&tracker, &id, Duration::from_millis(DEFAULT_TTL_MS + 1000));

        let next = tracker
            .create_job("rack-01", "node-02", JobType::FirmwareUpdate)
            .unwrap();
        assert!(tracker.get_job(&next).is_some());
        assert!(tracker.get_job(&id).is_none());
    }

    #[tokio::test]
    async fn reaper_evicts_expired_terminal_jobs_without_create() {
        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let id = job.id().to_string();
        job.complete("Completed", "");
        age_job(&tracker, &id, Duration::from_millis(DEFAULT_TTL_MS + 1000));

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
        let (parent_id, mut children) =
            create_test_batch(&tracker, "rack-01", &["n-01"], JobType::FirmwareUpdate);
        let c1 = children.pop().unwrap();
        let c1_id = c1.id().to_string();
        c1.complete("Completed", "");

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
            &c1_id,
            Duration::from_millis(DEFAULT_TTL_MS + 1000),
        );
        age_job(
            &tracker,
            &parent_id,
            Duration::from_millis(DEFAULT_TTL_MS + 1000),
        );
        tracker.reaper_sweep();
        tracker.reaper_sweep();

        assert!(tracker.get_job(&c1_id).is_none());
        assert!(tracker.get_job(&parent_id).is_none());
    }

    #[test]
    fn ttl_cleanup_protects_children_of_live_parent() {
        let tracker = JobTracker::new();
        let (parent_id, mut children) =
            create_test_batch(&tracker, "rack-01", &["n-01"], JobType::FirmwareUpdate);
        let c1 = children.pop().unwrap();
        let c1_id = c1.id().to_string();
        c1.complete("Completed", "");

        // Age the child past TTL but leave the parent fresh
        age_job(
            &tracker,
            &c1_id,
            Duration::from_millis(DEFAULT_TTL_MS + 1000),
        );

        tracker.cleanup_expired();

        // Child should still exist because parent protects it
        assert!(tracker.get_job(&c1_id).is_some());
        assert!(tracker.get_job(&parent_id).is_some());
    }

    #[test]
    fn ttl_cleanup_removes_expired_parent_and_children() {
        let tracker = JobTracker::new();
        let (parent_id, mut children) =
            create_test_batch(&tracker, "rack-01", &["n-01"], JobType::FirmwareUpdate);
        let c1 = children.pop().unwrap();
        let c1_id = c1.id().to_string();
        c1.complete("Completed", "");

        // Force parent to completed state by reading it (triggers aggregation)
        let _ = tracker.get_job(&parent_id);

        // Age both past TTL
        age_job(
            &tracker,
            &c1_id,
            Duration::from_millis(DEFAULT_TTL_MS + 1000),
        );
        age_job(
            &tracker,
            &parent_id,
            Duration::from_millis(DEFAULT_TTL_MS + 1000),
        );

        // First cleanup pass removes children; parent retained because
        // children were still in the map at check time.
        tracker.cleanup_expired();
        assert!(tracker.get_job(&c1_id).is_none());
        let parent_after_child_cleanup = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent_after_child_cleanup.state, JobState::Completed);

        // Second pass removes the now-childless parent.
        tracker.cleanup_expired();
        assert!(tracker.get_job(&parent_id).is_none());
    }

    #[test]
    fn child_creation_rejects_missing_parent() {
        let tracker = JobTracker::new();
        let result = tracker.create_child_job(
            "nonexistent-job-id",
            "rack-01",
            "n-01",
            JobType::FirmwareUpdate,
        );

        assert!(result.is_err());
    }

    #[test]
    fn parent_with_running_child_shows_progress() {
        let tracker = JobTracker::new();
        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02", "n-03"],
            JobType::FirmwareUpdate,
        );
        let [c1, c2, _c3] = children.try_into().unwrap_or_else(|_| {
            panic!("expected three child jobs");
        });

        c1.complete("Completed", "");
        c2.progress("Uploading");
        // c3 stays queued

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Running);
        assert!(parent.state_description.contains("1/3 complete"));
        assert!(parent.state_description.contains("1 running"));
        assert!(parent.state_description.contains("1 queued"));
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
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let id = job.id().to_string();
        job.fail(JobFailure::new(JobError::Other, "SSH timeout"));
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_message, "SSH timeout");
        assert!(job.result_json.is_empty());
    }

    #[test]
    fn mark_failed_message_with_result_preserves_payload() {
        let tracker = JobTracker::new();
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let id = job.id().to_string();
        job.fail(
            JobFailure::new(JobError::Other, "cancelled before stage_image")
                .with_result_json(r#"{"timing_summary":{"stages":[]}}"#),
        );
        let job = tracker.get_job(&id).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_message, "cancelled before stage_image");
        assert_eq!(job.result_json, r#"{"timing_summary":{"stages":[]}}"#);
    }

    #[test]
    fn switch_parent_uses_switch_wording_while_running() {
        let tracker = JobTracker::new();
        let (parent, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["sw-01", "sw-02"],
            JobType::SwitchSystemImageUpdate,
        );
        let [c1, _c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        c1.complete("Completed", "");

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
        let (parent, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["sw-01", "sw-02"],
            JobType::SwitchSystemImageUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        complete_child_and_refresh(&tracker, c1, "");
        complete_child_and_refresh(&tracker, c2, "");

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
        let (parent, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["sw-01", "sw-02"],
            JobType::SwitchSystemImageUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        c1.complete("Completed", "");
        c2.fail(
            JobFailure::new(JobError::Other, "SSH timeout")
                .with_result_json(r#"{"status":"failed","node_id":"sw-02"}"#),
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
        let token = tracker.cancellation_token_for(&running).unwrap();

        let cancelled = tracker.shutdown();

        assert_eq!(cancelled, 1);
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_active_leaves_returns_ids_and_cancels_tokens() {
        let tracker = JobTracker::new();
        let running = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        tracker.mark_running(&running, "uploading");
        let job_id = running.id().clone();
        let token = tracker.cancellation_token_for(&running).unwrap();

        let ids = tracker.registry.cancel_active_leaves();

        assert_eq!(ids, vec![job_id]);
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn shutdown_and_await_terminal_returns_after_jobs_reach_terminal() {
        let tracker = Arc::new(JobTracker::new());
        let running = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        running.progress("uploading");
        let job_id = running.id().to_string();

        // Simulate a cooperative worker that observes cancellation and seals
        // its job shortly after the shutdown wait begins.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            running.complete("Completed", "{}");
        });

        let cancelled = tracker
            .shutdown_and_await_terminal(Duration::from_secs(5))
            .await;

        assert_eq!(cancelled, 1);
        let job = tracker.get_job(&job_id).unwrap();
        assert_eq!(job.state, JobState::Completed);
    }

    #[tokio::test]
    async fn shutdown_and_await_terminal_times_out_when_job_never_terminates() {
        let tracker = JobTracker::new();
        let running = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        tracker.mark_running(&running, "uploading");

        // The job is cancelled but nothing seals it, so the wait returns once
        // the bounded timeout elapses rather than blocking teardown forever.
        let cancelled = tracker
            .shutdown_and_await_terminal(Duration::from_millis(50))
            .await;

        assert_eq!(cancelled, 1);
        let job = tracker.get_job(&running).unwrap();
        assert_eq!(job.state, JobState::Running);
    }

    #[tokio::test]
    async fn shutdown_and_await_terminal_returns_zero_without_active_jobs() {
        let tracker = JobTracker::new();
        let cancelled = tracker
            .shutdown_and_await_terminal(Duration::from_secs(5))
            .await;
        assert_eq!(cancelled, 0);
    }

    #[test]
    fn shutdown_refuses_new_job_creation() {
        let tracker = JobTracker::new();
        tracker.shutdown();

        let err = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap_err();
        assert!(
            err.message.contains("shutting down"),
            "unexpected refusal message: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn shutdown_and_await_terminal_refuses_new_job_creation() {
        let tracker = JobTracker::new();
        tracker
            .shutdown_and_await_terminal(Duration::from_secs(5))
            .await;

        // Both leaf create paths are gated.
        let err = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap_err();
        assert!(
            err.message.contains("shutting down"),
            "unexpected refusal message: {}",
            err.message
        );
        let err = tracker
            .create_job_if_node_idle("rack-01", "sw-02", JobType::FirmwareUpdate)
            .unwrap_err();
        assert!(
            err.message.contains("shutting down"),
            "unexpected refusal message: {}",
            err.message
        );
    }

    #[test]
    fn parent_partial_skip() {
        let tracker = JobTracker::new();
        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        c1.complete("Completed", r#"{"status":"already_up_to_date"}"#);
        c2.complete("Completed", r#"{"status":"ok"}"#);

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.state, JobState::Completed);
        assert!(
            parent
                .state_description
                .contains("1/2 firmware updates applied")
        );
        assert!(parent.state_description.contains("1 already up to date"));
    }

    #[test]
    fn visible_leaf_workflow_emits_started_finished_and_in_flight_metrics() {
        let (registry, tracker) = workflow_metrics_setup();
        let started_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="true",workflow_state="queued",workflow_type="firmware_update"}"#;
        let in_flight_key =
            r#"rms_workflows_in_flight{is_leaf="true",workflow_type="firmware_update"}"#;
        let finished_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="true",workflow_state="completed",workflow_type="firmware_update"}"#;

        let before = gather_workflow_metrics(registry.as_ref());
        let started_before = metric_value(&before, started_key);
        let in_flight_before = metric_value(&before, in_flight_key);
        let finished_before = metric_value(&before, finished_key);

        let pending = tracker
            .create_visible_workflow_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let started_body = gather_workflow_metrics(registry.as_ref());
        assert_eq!(
            metric_value(&started_body, started_key),
            started_before + 1.0
        );
        assert_eq!(
            metric_value(&started_body, in_flight_key),
            in_flight_before + 1.0
        );

        pending.complete("Completed", "");

        let finished_body = gather_workflow_metrics(registry.as_ref());
        assert_eq!(
            metric_value(&finished_body, finished_key),
            finished_before + 1.0
        );
        assert_eq!(
            metric_value(&finished_body, in_flight_key),
            in_flight_before
        );
    }

    #[test]
    fn visible_leaf_workflow_failure_records_error_code_label() {
        let (registry, tracker) = workflow_metrics_setup();
        let finished_key = r#"rms_workflows_counts_total{error_code="client_error",is_leaf="true",workflow_state="failed",workflow_type="firmware_update"}"#;
        let before = metric_value(&gather_workflow_metrics(registry.as_ref()), finished_key);

        let pending = tracker
            .create_visible_workflow_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        pending.fail(JobFailure::new(JobError::ClientError, "connection lost"));

        let body = gather_workflow_metrics(registry.as_ref());
        assert_eq!(metric_value(&body, finished_key), before + 1.0);
    }

    #[test]
    fn batch_child_jobs_do_not_emit_workflow_metrics() {
        let (registry, tracker) = workflow_metrics_setup();
        let started_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="true",workflow_state="queued",workflow_type="firmware_update"}"#;
        let finished_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="true",workflow_state="completed",workflow_type="firmware_update"}"#;
        let before = gather_workflow_metrics(registry.as_ref());
        let started_before = metric_value(&before, started_key);
        let finished_before = metric_value(&before, finished_key);

        let child = tracker
            .create_job("rack-01", "n-01", JobType::FirmwareUpdate)
            .unwrap();
        child.complete("Completed", "");

        let after = gather_workflow_metrics(registry.as_ref());
        assert_eq!(metric_value(&after, started_key), started_before);
        assert_eq!(metric_value(&after, finished_key), finished_before);
    }

    #[test]
    fn parent_workflow_emits_terminal_metrics_once_via_aggregation() {
        let (registry, tracker) = workflow_metrics_setup();
        let started_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="false",workflow_state="queued",workflow_type="firmware_update"}"#;
        let finished_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="false",workflow_state="completed",workflow_type="firmware_update"}"#;
        let in_flight_key =
            r#"rms_workflows_in_flight{is_leaf="false",workflow_type="firmware_update"}"#;
        let before = gather_workflow_metrics(registry.as_ref());
        let started_before = metric_value(&before, started_key);
        let finished_before = metric_value(&before, finished_key);
        let in_flight_before = metric_value(&before, in_flight_key);

        let (_parent_id, children) = create_test_batch(
            tracker.as_ref(),
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        let started_body = gather_workflow_metrics(registry.as_ref());
        assert_eq!(
            metric_value(&started_body, started_key),
            started_before + 1.0
        );
        assert_eq!(
            metric_value(&started_body, in_flight_key),
            in_flight_before + 1.0
        );

        complete_child_and_refresh(tracker.as_ref(), c1, "");
        complete_child_and_refresh(tracker.as_ref(), c2, "");

        let finished_body = gather_workflow_metrics(registry.as_ref());
        assert_eq!(
            metric_value(&finished_body, finished_key),
            finished_before + 1.0
        );
        assert_eq!(
            metric_value(&finished_body, in_flight_key),
            in_flight_before
        );
    }

    #[test]
    fn parent_workflow_emits_terminal_metrics_when_child_fails_without_poll() {
        let (registry, tracker) = workflow_metrics_setup();
        let started_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="false",workflow_state="queued",workflow_type="firmware_update"}"#;
        let finished_key = r#"rms_workflows_counts_total{error_code="other",is_leaf="false",workflow_state="failed",workflow_type="firmware_update"}"#;
        let in_flight_key =
            r#"rms_workflows_in_flight{is_leaf="false",workflow_type="firmware_update"}"#;
        let before = gather_workflow_metrics(registry.as_ref());
        let started_before = metric_value(&before, started_key);
        let finished_before = metric_value(&before, finished_key);
        let in_flight_before = metric_value(&before, in_flight_key);

        let (_parent_id, mut children) = create_test_batch(
            tracker.as_ref(),
            "rack-01",
            &["n-01"],
            JobType::FirmwareUpdate,
        );
        let child = children.pop().unwrap();

        let started_body = gather_workflow_metrics(registry.as_ref());
        assert_eq!(
            metric_value(&started_body, started_key),
            started_before + 1.0
        );
        assert_eq!(
            metric_value(&started_body, in_flight_key),
            in_flight_before + 1.0
        );

        fail_child_and_refresh(
            tracker.as_ref(),
            child,
            JobFailure::new(JobError::Other, "no matching firmware found"),
        );

        let finished_body = gather_workflow_metrics(registry.as_ref());
        assert_eq!(
            metric_value(&finished_body, finished_key),
            finished_before + 1.0
        );
        assert_eq!(
            metric_value(&finished_body, in_flight_key),
            in_flight_before
        );
    }

    #[test]
    fn parent_workflow_created_with_already_terminal_children_nets_zero_in_flight() {
        // The child completes before the parent is created, so the parent
        // terminalizes during the `refresh_parent_state` that follows
        // `create_parent`. Creation metrics run before that refresh returns,
        // leaving the gauge at its baseline rather than transiently negative.
        let (registry, tracker) = workflow_metrics_setup();
        let started_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="false",workflow_state="queued",workflow_type="firmware_update"}"#;
        let finished_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="false",workflow_state="completed",workflow_type="firmware_update"}"#;
        let in_flight_key =
            r#"rms_workflows_in_flight{is_leaf="false",workflow_type="firmware_update"}"#;

        let before = gather_workflow_metrics(registry.as_ref());
        let started_before = metric_value(&before, started_key);
        let finished_before = metric_value(&before, finished_key);
        let in_flight_before = metric_value(&before, in_flight_key);

        let (parent_id, mut children) = create_test_batch(
            tracker.as_ref(),
            "rack-01",
            &["n-01"],
            JobType::FirmwareUpdate,
        );
        let child = children.pop().unwrap();
        child.complete("Completed", "");

        // Parent terminalized immediately on creation.
        assert_eq!(
            tracker.get_job(&parent_id).unwrap().state,
            JobState::Completed
        );

        let after = gather_workflow_metrics(registry.as_ref());
        assert_eq!(metric_value(&after, started_key), started_before + 1.0);
        assert_eq!(metric_value(&after, finished_key), finished_before + 1.0);
        assert_eq!(metric_value(&after, in_flight_key), in_flight_before);
    }

    #[test]
    fn parent_workflow_finished_metric_not_incremented_on_repeated_get_job_poll() {
        let (registry, tracker) = workflow_metrics_setup();
        let finished_key = r#"rms_workflows_counts_total{error_code="none",is_leaf="false",workflow_state="completed",workflow_type="firmware_update"}"#;

        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::FirmwareUpdate,
        );
        let [c1, c2] = children.try_into().unwrap_or_else(|_| {
            panic!("expected two child jobs");
        });

        complete_child_and_refresh(tracker.as_ref(), c1, "");
        complete_child_and_refresh(tracker.as_ref(), c2, "");

        let finished_after_terminal =
            metric_value(&gather_workflow_metrics(registry.as_ref()), finished_key);

        for _ in 0..5 {
            tracker.get_job(&parent_id);
        }

        assert_eq!(
            metric_value(&gather_workflow_metrics(registry.as_ref()), finished_key),
            finished_after_terminal
        );
    }

    #[test]
    fn parent_workflow_finished_metric_not_incremented_on_repeated_get_job_after_failure() {
        let (registry, tracker) = workflow_metrics_setup();
        let finished_key = r#"rms_workflows_counts_total{error_code="other",is_leaf="false",workflow_state="failed",workflow_type="firmware_update"}"#;

        let (parent_id, mut children) = create_test_batch(
            tracker.as_ref(),
            "rack-01",
            &["n-01"],
            JobType::FirmwareUpdate,
        );
        let child = children.pop().unwrap();

        fail_child_and_refresh(
            tracker.as_ref(),
            child,
            JobFailure::new(JobError::Other, "no matching firmware found"),
        );

        let finished_after_terminal =
            metric_value(&gather_workflow_metrics(registry.as_ref()), finished_key);

        for _ in 0..5 {
            tracker.get_job(&parent_id);
        }

        assert_eq!(
            metric_value(&gather_workflow_metrics(registry.as_ref()), finished_key),
            finished_after_terminal
        );
    }

    #[tokio::test]
    async fn visible_leaf_workflow_panic_decrements_in_flight_gauge() {
        let (registry, tracker) = workflow_metrics_setup();
        let in_flight_key =
            r#"rms_workflows_in_flight{is_leaf="true",workflow_type="firmware_update"}"#;
        // Supervisor panic failure maps to JobError::Internal.
        let finished_key = r#"rms_workflows_counts_total{error_code="internal",is_leaf="true",workflow_state="failed",workflow_type="firmware_update"}"#;

        let before = gather_workflow_metrics(registry.as_ref());
        let in_flight_before = metric_value(&before, in_flight_key);
        let finished_before = metric_value(&before, finished_key);

        let pending = tracker
            .create_visible_workflow_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();

        // Worker panics without sealing: the registry supervisor terminalizes
        // the job, bypassing the tracker's own paths, so only the registry's
        // terminal observer can keep the in-flight gauge from leaking.
        let handle = tracker.spawn_job(pending, |_job| async move {
            panic!("simulated worker panic");
        });
        handle.wait().await.unwrap();

        let after = gather_workflow_metrics(registry.as_ref());
        assert_eq!(metric_value(&after, in_flight_key), in_flight_before);
        assert_eq!(metric_value(&after, finished_key), finished_before + 1.0);
    }

    #[tokio::test]
    async fn visible_leaf_workflow_dropped_without_sealing_decrements_in_flight_gauge() {
        let (registry, tracker) = workflow_metrics_setup();
        let in_flight_key =
            r#"rms_workflows_in_flight{is_leaf="true",workflow_type="firmware_update"}"#;
        // The RAII drop safety net records JobError::Internal.
        let finished_key = r#"rms_workflows_counts_total{error_code="internal",is_leaf="true",workflow_state="failed",workflow_type="firmware_update"}"#;

        let before = gather_workflow_metrics(registry.as_ref());
        let in_flight_before = metric_value(&before, in_flight_key);
        let finished_before = metric_value(&before, finished_key);

        let pending = tracker
            .create_visible_workflow_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();

        // Worker returns without marking the job terminal: the RAII drop guard
        // terminalizes it at the registry level, which must still decrement the
        // in-flight gauge via the terminal observer.
        let handle = tracker.spawn_job(pending, |_job| async move {});
        handle.wait().await.unwrap();

        let after = gather_workflow_metrics(registry.as_ref());
        assert_eq!(metric_value(&after, in_flight_key), in_flight_before);
        assert_eq!(metric_value(&after, finished_key), finished_before + 1.0);
    }

    #[test]
    fn configured_ttl_controls_terminal_job_eviction() {
        let ttl = Duration::from_millis(10);
        let tracker = JobTracker::with_retention_limits(ttl, 100);
        let job = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let id = job.id().to_string();
        job.complete("Completed", "");
        age_job(&tracker, &id, Duration::from_millis(50));

        tracker.cleanup_expired();
        assert!(tracker.get_job(&id).is_none());
    }

    #[test]
    fn aged_terminal_child_does_not_fail_unread_parent_batch() {
        let tracker = JobTracker::new();
        let (parent_id, mut children) =
            create_test_batch(&tracker, "rack-01", &["n-01"], JobType::FirmwareUpdate);
        let child = children.pop().unwrap();
        let child_id = child.id().to_string();
        child.complete("Completed", "");

        age_job(
            &tracker,
            &child_id,
            Duration::from_millis(DEFAULT_TTL_MS + 1000),
        );
        tracker.reaper_sweep();

        let raw = tracker
            .registry
            .get(&JobId::from(parent_id.as_str()))
            .unwrap();
        assert_eq!(snapshot_state(&raw.state), JobState::Completed);
        assert!(tracker.get_job(&child_id).is_some());
    }

    #[test]
    fn small_ttl_first_parent_poll_completes_before_child_eviction() {
        let tracker = JobTracker::with_retention_limits(Duration::from_millis(10), 100);
        let (parent_id, mut children) =
            create_test_batch(&tracker, "rack-01", &["n-01"], JobType::FirmwareUpdate);
        let child = children.pop().unwrap();
        let child_id = child.id().to_string();
        child.complete("Completed", "");

        age_job(&tracker, &child_id, Duration::from_millis(1000));

        assert_eq!(
            tracker.get_job(&parent_id).unwrap().state,
            JobState::Completed
        );
    }

    #[test]
    fn reaper_interval_caps_long_ttl_at_default_cadence() {
        assert_eq!(
            reaper_interval(Duration::from_millis(DEFAULT_TTL_MS)),
            REAPER_INTERVAL
        );
        assert_eq!(
            reaper_interval(REAPER_INTERVAL + Duration::from_secs(1)),
            REAPER_INTERVAL
        );
    }

    #[test]
    fn reaper_interval_tracks_short_ttl() {
        let ttl = Duration::from_secs(60);
        assert_eq!(reaper_interval(ttl), ttl);
    }

    #[test]
    fn reaper_interval_floors_tiny_ttl() {
        assert_eq!(
            reaper_interval(Duration::from_millis(1)),
            MIN_REAPER_INTERVAL
        );
    }
}
