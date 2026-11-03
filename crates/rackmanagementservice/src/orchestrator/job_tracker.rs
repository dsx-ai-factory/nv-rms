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

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::orchestrator::job_lifecycle::{
    JobDomain, JobError, JobFailure, JobHandle, JobId, JobJoinHandle, JobLifecycleState,
    JobRegistry, JobScope, JobSnapshot, JobSpec, JobState, MutationReservation,
};
use crate::utilities::error::RmsError;

use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use prometheus::{CounterVec, IntGaugeVec, Opts};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub use crate::orchestrator::job_lifecycle::JobType;
pub(crate) use crate::orchestrator::job_lifecycle::job_error_of;

const FORCED_TASK_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const CANCELLED_JOB_DESCRIPTION: &str = "Cancelled";

/// The shared lifecycle domain for every async job the `JobTracker` owns
/// (firmware updates and switch system image updates alike). Because it is not
/// tied to a single `JobType`, its failure codes ([`JobError`]) and supervisor
/// fallback messages stay domain-neutral; per-`JobType` wording is applied at
/// aggregation time instead.
pub(crate) struct RmsJobDomain;

pub(crate) type RmsJobHandle = JobHandle<RmsJobDomain>;

/// A batch target that was admitted by [`JobTracker::create_batch_jobs`]: the
/// caller's payload for that target, the child job reserved for it, and the
/// state the caller's check built for it.
pub(crate) type AdmittedBatchJob<P, S> = (P, RmsJobHandle, S);

/// A batch target that [`JobTracker::create_batch_jobs`] refused, with the
/// reason it was refused.
///
/// The `rack_id` is carried alongside the `node_id` because a batch may span
/// racks, so a bare `node_id` does not identify the refused target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RejectedBatchTarget {
    pub(crate) rack_id: String,
    pub(crate) node_id: String,
    pub(crate) error_message: String,
}

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

    fn scope_busy_failure(scope: &JobScope, owner_id: &JobId) -> JobFailure {
        job_failure(
            JobError::UpdateInProgress,
            format!("mutation scope {scope:?} is occupied (owner_id {owner_id})"),
        )
    }

    fn inactive_job_failure(job_id: &JobId) -> JobFailure {
        job_failure(
            JobError::Internal,
            format!("job {job_id} is missing or terminal; mutation aliases cannot be admitted"),
        )
    }

    fn shutdown_failure() -> JobFailure {
        job_failure(
            JobError::Other,
            "job cancelled because the server is shutting down",
        )
        .with_description(CANCELLED_JOB_DESCRIPTION)
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
/// are retained for a configurable TTL (24h by default), measured on a
/// monotonic clock, then cleaned up by the background reaper or when an
/// admission encounters a full registry.
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
    ///
    /// Test-only: every production path admits through a node-idle create, so
    /// nothing bypasses per-node exclusion on the way in. Tests keep this to
    /// occupy a node deliberately. Note the record is still indexed like any
    /// other, so a job made this way does block later node-idle admissions.
    #[cfg(test)]
    pub(crate) fn create_job(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let parent_context = current_otel_context();
        let job = self.registry.create_job(
            JobSpec::new_deferred(rack_id, node_id, "Queued", move |_job_id| {
                job_span(job_type, "", parent_context)
            }),
            None,
            false,
        )?;

        Ok(job)
    }

    /// Create a job only when the target node has no active job.
    ///
    /// Fails with a [`JobError::UpdateInProgress`] failure when any non-terminal
    /// job already targets the same `(rack_id, node_id)`, or
    /// [`JobError::Internal`] when the tracker is full.
    pub(crate) fn create_job_if_node_idle(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let parent_context = current_otel_context();
        let job = self.registry.create_job(
            JobSpec::new_deferred(rack_id, node_id, "Queued", move |_job_id| {
                job_span(job_type, "", parent_context)
            }),
            None,
            true,
        )?;

        Ok(job)
    }

    /// Removes a still-queued leaf job (an unstarted reservation), releasing
    /// its `(rack_id, node_id)` for other operations. No-op once the job has
    /// started running or reached a terminal state.
    pub(crate) fn remove_queued_job(&self, job_id: &str) -> bool {
        self.registry.remove_queued_leaf(&JobId::from(job_id))
    }

    /// Creates a child of `parent_job_id` only when the target node has no
    /// active job.
    /// Fails with a [`JobError::UpdateInProgress`] failure when any non-terminal
    /// job already targets the same `(rack_id, node_id)`, or
    /// [`JobError::Internal`] when the tracker is full.
    pub(crate) fn create_child_job(
        &self,
        parent_job_id: &str,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let parent_id = JobId::from(parent_job_id);
        let parent_context = self
            .registry
            .span_for_job(&parent_id)
            .and_then(|span| otel_context(&span));
        let parent_job_id = parent_job_id.to_string();
        let job = self.registry.create_job(
            JobSpec::new_deferred(rack_id, node_id, "Queued", move |_job_id| {
                job_span(job_type, &parent_job_id, parent_context)
            }),
            Some(&parent_id),
            true,
        )?;
        refresh_parent_state(self, &parent_id);
        Ok(job)
    }

    /// Reserves one child job per batch target, in request order.
    ///
    /// A target is admitted only when it is the first target for its
    /// `(rack_id, node_id)` in this batch, the caller's `check` accepts its
    /// payload, and [`Self::create_child_job`] finds the node idle
    pub(crate) fn create_batch_jobs<P, S>(
        &self,
        parent_job_id: &str,
        job_type: JobType,
        targets: Vec<(String, String, P)>,
        mut check: impl FnMut(&P) -> std::result::Result<S, String>,
    ) -> (Vec<AdmittedBatchJob<P, S>>, Vec<RejectedBatchTarget>) {
        let mut admitted = Vec::with_capacity(targets.len());
        let mut rejected = Vec::new();
        let mut seen_nodes = HashSet::with_capacity(targets.len());

        for (rack_id, node_id, payload) in targets {
            let admission = if seen_nodes.insert((rack_id.clone(), node_id.clone())) {
                check(&payload).and_then(|state| {
                    self.create_child_job(parent_job_id, &rack_id, &node_id, job_type)
                        .map(|job| (job, state))
                        .map_err(|failure| {
                            tracing::warn!(
                                operation = job_type.batch_noun(),
                                node = %node_id,
                                rack = %rack_id,
                                error = %failure.message,
                                "batch job rejected"
                            );

                            failure.message
                        })
                })
            } else {
                tracing::warn!(
                    operation = job_type.batch_noun(),
                    node = %node_id,
                    rack = %rack_id,
                    "batch job rejected: duplicate target"
                );

                Err(format!(
                    "duplicate target rack_id/node_id: {rack_id}/{node_id}"
                ))
            };

            match admission {
                Ok((job, state)) => admitted.push((payload, job, state)),
                Err(error_message) => rejected.push(RejectedBatchTarget {
                    rack_id,
                    node_id,
                    error_message,
                }),
            }
        }

        (admitted, rejected)
    }

    /// Creates a leaf job when the target node is idle and registers it as an
    /// externally visible workflow (the job ID returned to API callers).
    pub(crate) fn create_visible_workflow_job_if_node_idle(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let parent_context = current_otel_context();
        let job = self.registry.create_job(
            JobSpec::new_deferred(rack_id, node_id, "Queued", move |_job_id| {
                job_span(job_type, "", parent_context)
            })
            .with_workflow_type(job_type),
            None,
            true,
        )?;

        // Started/in-flight metrics are emitted by the registry's creation
        // observer because the spec carries a `workflow_type`.
        Ok(job)
    }

    /// Creates one explicitly completed workflow after admitting all aliases.
    ///
    /// The job has no aggregate-parent owner. Its worker must consume the handle
    /// after verification or failure, including workflows targeting multiple nodes.
    pub(crate) fn create_scoped_job(
        &self,
        rack_id: &str,
        node_id: &str,
        job_type: JobType,
        scopes: Vec<JobScope>,
    ) -> std::result::Result<RmsJobHandle, JobFailure> {
        let parent_context = current_otel_context();

        self.registry.create_job_with_scopes(
            JobSpec::new_deferred(rack_id, node_id, "Queued", move |_job_id| {
                job_span(job_type, "", parent_context)
            })
            .with_workflow_type(job_type),
            None,
            true,
            scopes,
        )
    }

    /// Reserves every alias for a direct mutation until its guard is dropped.
    pub(crate) fn reserve_mutation(
        &self,
        scopes: Vec<JobScope>,
    ) -> std::result::Result<MutationReservation<RmsJobDomain>, JobFailure> {
        self.registry.reserve_mutation(scopes)
    }

    pub(crate) fn spawn_job<F, Fut>(self: &Arc<Self>, job: RmsJobHandle, run: F) -> JobJoinHandle
    where
        F: FnOnce(RmsJobHandle) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let refresh = RefreshParentOnWorkerExit {
            tracker: Arc::downgrade(self),
            job_id: job.id().clone(),
        };
        self.registry.spawn_job(job, move |job| async move {
            // Bound before the await so it drops after `run`'s future, and with
            // it the job handle, has been dropped: the parent then always
            // aggregates a child that has reached its terminal state.
            let _refresh = refresh;
            run(job).await;
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
        let parent_context = current_otel_context();
        let parent_handle = self
            .registry
            .create_job(
                JobSpec::new_parent_deferred(
                    rack_id,
                    format!("Batch {} accepting child jobs", job_type.batch_noun()),
                    move |_job_id| job_span(job_type, "", parent_context),
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
        // Reserve the four bounded forced waits below from the budget: short
        // budgets split into five equal waits, and long ones keep all but four
        // `FORCED_TASK_DRAIN_TIMEOUT`s for cooperative cancellation.
        let forced_wait = (timeout / 5).min(FORCED_TASK_DRAIN_TIMEOUT);
        let cooperative_wait = timeout.saturating_sub(forced_wait * 4);
        let reached_terminal = job_ids.is_empty()
            || self
                .registry
                .wait_for_terminal_jobs(&job_ids, Some(cooperative_wait))
                .await;
        if !reached_terminal {
            tracing::warn!(
                jobs_cancelled = to_cancel,
                timeout_secs = timeout.as_secs(),
                "graceful shutdown wait timed out; some jobs did not reach a \
                 terminal state before the deadline"
            );
        }

        let task_job_ids = self.registry.task_job_ids();
        if !self
            .registry
            .wait_for_tasks(&task_job_ids, forced_wait)
            .await
        {
            self.registry.abort_workers(&task_job_ids);
            let workers_finished = self
                .registry
                .wait_for_workers(&task_job_ids, forced_wait)
                .await;
            let tasks_finished = self
                .registry
                .wait_for_tasks(&task_job_ids, forced_wait)
                .await;
            if !tasks_finished
                && (workers_finished || self.registry.all_workers_finished(&task_job_ids))
            {
                self.registry.abort_supervisors(&task_job_ids);
                let _ = self
                    .registry
                    .wait_for_tasks(&task_job_ids, forced_wait)
                    .await;
            } else if !tasks_finished {
                tracing::warn!(
                    tasks = task_job_ids.len(),
                    "job workers did not stop after abort before the shutdown deadline"
                );
            }
        }

        self.refresh_all_parents();
        self.fail_remaining_parents_for_shutdown();
        let still_active = self.registry.non_terminal_job_ids();
        self.registry.fail_non_terminal_for_shutdown(&still_active);
        self.refresh_all_parents();

        to_cancel
    }

    fn fail_remaining_parents_for_shutdown(&self) {
        let handles: Vec<_> = self
            .parent_job_handles
            .lock()
            .unwrap()
            .drain()
            .map(|(_, handle)| handle)
            .collect();
        for handle in handles {
            handle.fail(RmsJobDomain::shutdown_failure());
        }
    }

    // Two-phase TTL cleanup: first remove expired leaf jobs (protecting
    // children of live parents), then remove childless expired parents.
    #[cfg(test)]
    fn cleanup_expired(&self) {
        self.registry.cleanup_expired();
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

fn current_otel_context() -> Option<opentelemetry::Context> {
    otel_context(&tracing::Span::current())
}

fn otel_context(span: &tracing::Span) -> Option<opentelemetry::Context> {
    let context = span.context();
    context.span().span_context().is_valid().then_some(context)
}

// Span names must remain literals at their tracing callsites. This table also
// supplies the export catalog, so every job type's span is included there.
macro_rules! job_spans {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        pub(crate) const JOB_SPAN_NAMES: &[&str] = &[$($name),+];

        fn job_span_for(job_type: JobType) -> tracing::Span {
            match job_type {
                $(JobType::$variant => tracing::info_span!(
                    parent: None,
                    $name,
                    job_id = tracing::field::Empty,
                    parent_job_id = tracing::field::Empty,
                ),)+
            }
        }
    };
}

job_spans! {
    FirmwareUpdate => "firmware_update",
    ConfigureScaleUpFabricManagerV2 => "configure_scale_up_fabric_manager_v2",
    SwitchCertificate => "switch_certificate",
    SwitchMtlsDisable => "switch_mtls_disable",
    SwitchSdnFactoryDefaultReset => "switch_sdn_factory_default_reset",
    SwitchFactoryDefaultReset => "switch_factory_default_reset",
    SwitchSystemPasswordUpdate => "switch_system_password_update",
    SwitchSystemImageUpdate => "switch_system_image_update",
    ColdRebootSequence => "cold_reboot_sequence",
    SystemValidation => "system_validation",
}

fn job_span(
    job_type: JobType,
    parent_job_id: &str,
    otel_parent: Option<opentelemetry::Context>,
) -> tracing::Span {
    let span = job_span_for(job_type);
    if !parent_job_id.is_empty() {
        span.record("parent_job_id", parent_job_id);
    }

    if let Some(parent) = otel_parent
        && let Err(error) = span.set_parent(parent)
    {
        // Report the first attachment failure without flooding logs or changing job behavior.
        static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!(
                parent: None,
                job_type = job_type.span_name(),
                error = ?error,
                "could not apply job trace context"
            );
        }
    }
    // Start the SDK span now so its builder releases the parent Context. The
    // retained tracing span remains a tracing-registry root and cannot keep
    // the request or parent-job tracing span open for the terminal-job TTL.
    let _ = span.context();
    span
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

/// Refreshes a spawned job's parent when the worker task exits, whatever the
/// outcome: a normal return, a return that never sealed the handle, or a panic.
///
/// This lives in `Drop` rather than after `run(job).await` because a panic
/// unwinds past the tail of the future, which left the parent `Running` until
/// an external `get_job` or the next reaper sweep aggregated it.
struct RefreshParentOnWorkerExit {
    tracker: Weak<JobTracker>,
    job_id: JobId,
}

impl Drop for RefreshParentOnWorkerExit {
    fn drop(&mut self) {
        let Some(tracker) = self.tracker.upgrade() else {
            return;
        };

        // On the panic path this runs while unwinding, where panicking again
        // aborts the process instead of unwinding. Aggregation takes locks it
        // unwraps, so contain a poisoned-lock panic and let the original one
        // continue: the reaper still aggregates this parent later.
        let refresh = std::panic::AssertUnwindSafe(|| tracker.after_job_terminal(&self.job_id));
        if std::panic::catch_unwind(refresh).is_err() {
            tracing::error!(job_id = %self.job_id, "parent refresh panicked after worker exit");
        }
    }
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
    if !parent.is_parent() || parent.state.is_terminal() {
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
    } else if !parent_already_sealed(tracker, parent_id) {
        tracing::error!(%parent_id, "parent job handle missing during failure aggregation");
    }
}

fn parent_already_sealed(tracker: &JobTracker, parent_id: &JobId) -> bool {
    tracker
        .registry
        .get(parent_id)
        .is_some_and(|parent| parent.state.is_terminal())
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
        | JobType::ConfigureScaleUpFabricManagerV2
        | JobType::ColdRebootSequence
        | JobType::SystemValidation => serialize_result_json(&SwitchParentFailureResult {
            completed,
            failed,
            missing,
            total,
            failed_jobs: failed_children
                .into_iter()
                .map(switch_failed_child_detail)
                .collect(),
        }),
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
    } else if !parent_already_sealed(tracker, parent_id) {
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

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider};
    use prometheus::{Registry, TextEncoder};
    use serde::Serialize;
    use tracing_subscriber::prelude::*;

    #[test]
    fn scoped_job_admission_rejects_overlap_without_reserving_other_targets() {
        let tracker = JobTracker::default();

        let switch = JobScope::PhysicalSwitch {
            chassis_serial_number: "chassis-a".to_owned(),
            tray_index: 1,
        };

        let endpoint = JobScope::endpoint("switch.example", 443);

        let job = tracker
            .create_scoped_job(
                "rack-a",
                "",
                JobType::ConfigureScaleUpFabricManagerV2,
                vec![switch.clone()],
            )
            .unwrap();

        let refused = tracker.create_scoped_job(
            "rack-b",
            "",
            JobType::SwitchFactoryDefaultReset,
            vec![endpoint.clone(), switch.clone()],
        );

        let free_target = tracker.reserve_mutation(vec![endpoint.clone()]);

        assert!(refused.is_err());

        assert_eq!(
            job_error_of(&refused.err().unwrap().error),
            JobError::UpdateInProgress
        );

        assert!(free_target.is_ok());
        assert!(tracker.reserve_mutation(vec![switch.clone()]).is_err());

        job.complete_verified("Verified", "");

        assert!(tracker.reserve_mutation(vec![switch]).is_ok());
        drop(free_target);

        assert!(tracker.reserve_mutation(vec![endpoint]).is_ok());
    }

    #[test]
    fn mutation_reservation_conflicts_with_node_jobs_and_releases_on_drop() {
        let tracker = JobTracker::default();

        let node_scope = JobScope::LogicalNode {
            rack_id: "rack-a".to_owned(),
            node_id: "switch-a".to_owned(),
        };

        let reservation = tracker.reserve_mutation(vec![node_scope.clone()]).unwrap();

        let refused = tracker.create_job_if_node_idle(
            "rack-a",
            "switch-a",
            JobType::SwitchFactoryDefaultReset,
        );

        assert!(refused.is_err());
        drop(reservation);

        let job = tracker
            .create_job("rack-a", "switch-a", JobType::SwitchFactoryDefaultReset)
            .unwrap();

        assert!(tracker.reserve_mutation(vec![node_scope.clone()]).is_err());
        drop(job);

        assert!(tracker.reserve_mutation(vec![node_scope]).is_ok());
    }

    #[test]
    fn scope_endpoint_normalization_and_switch_tray_identity_are_preserved() {
        let tracker = JobTracker::default();

        let endpoint = tracker
            .reserve_mutation(vec![JobScope::endpoint("SWITCH.EXAMPLE.", 443)])
            .unwrap();

        let switch = tracker
            .reserve_mutation(vec![JobScope::PhysicalSwitch {
                chassis_serial_number: "shared-chassis".to_owned(),
                tray_index: 1,
            }])
            .unwrap();

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::endpoint("switch.example", 443)])
                .is_err()
        );

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::PhysicalSwitch {
                    chassis_serial_number: "shared-chassis".to_owned(),
                    tray_index: 2,
                }])
                .is_ok()
        );

        assert_eq!(
            JobScope::endpoint("[2001:db8::1]", 443),
            JobScope::endpoint("2001:0db8:0:0:0:0:0:1", 443)
        );

        drop((endpoint, switch));
    }

    #[test]
    fn equivalent_endpoint_addresses_share_mutation_reservations() {
        let tracker = JobTracker::default();

        for (first, equivalent) in [
            ("10.0.0.5", "[::ffff:10.0.0.5]"),
            ("[::ffff:10.0.0.5]", "10.0.0.5"),
            ("::ffff:a00:5", "10.0.0.5"),
            ("fe80::1%eth0", "[fe80:0:0:0:0:0:0:1%eth0]"),
            ("[fe80:0:0:0:0:0:0:1%ETH0]", "fe80::1%ETH0"),
        ] {
            // Raw variants exercise normalization at admission, not just the constructor.
            let reservation = tracker
                .reserve_mutation(vec![JobScope::Endpoint {
                    host: first.to_owned(),
                    port: 443,
                }])
                .unwrap();

            let refused = tracker.reserve_mutation(vec![JobScope::Endpoint {
                host: equivalent.to_owned(),
                port: 443,
            }]);

            let Err(failure) = refused else {
                panic!("equivalent endpoint {equivalent} must conflict with {first}");
            };

            assert_eq!(job_error_of(&failure.error), JobError::UpdateInProgress);
            assert!(failure.message.contains("owner_id"));
            assert!(!failure.message.contains("job_id"));

            drop(reservation);

            assert!(
                tracker
                    .reserve_mutation(vec![JobScope::endpoint(equivalent, 443)])
                    .is_ok()
            );
        }
    }

    #[test]
    fn scoped_ipv6_reservations_preserve_zone_and_port_identity() {
        let tracker = JobTracker::default();

        let reservation = tracker
            .reserve_mutation(vec![JobScope::endpoint("fe80::1%eth0", 443)])
            .unwrap();

        for host in ["fe80::1%eth1", "fe80::1%ETH0", "fe80::1%2"] {
            assert!(
                tracker
                    .reserve_mutation(vec![JobScope::endpoint(host, 443)])
                    .is_ok()
            );
        }

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::endpoint("fe80::1%eth0", 22)])
                .is_ok()
        );

        assert_eq!(
            JobScope::endpoint("fe80:0:0:0:0:0:0:1%ETH0", 443),
            JobScope::Endpoint {
                host: "fe80::1%ETH0".to_owned(),
                port: 443,
            }
        );

        drop(reservation);
    }

    #[test]
    fn fabric_job_and_direct_mutation_conflict_across_endpoint_spellings() {
        let tracker = JobTracker::default();

        let direct_scope = JobScope::LogicalNode {
            rack_id: "other-rack".to_owned(),
            node_id: "other-switch".to_owned(),
        };

        let job = tracker
            .create_scoped_job(
                "rack-a",
                "switch-a",
                JobType::ConfigureScaleUpFabricManagerV2,
                vec![JobScope::endpoint("10.0.0.5", 443)],
            )
            .unwrap();

        let refused = tracker.reserve_mutation(vec![
            direct_scope.clone(),
            JobScope::endpoint("[::ffff:10.0.0.5]", 443),
        ]);

        assert!(refused.is_err());
        assert!(tracker.reserve_mutation(vec![direct_scope.clone()]).is_ok());

        drop(job);

        let reservation = tracker
            .reserve_mutation(vec![
                direct_scope,
                JobScope::endpoint("[::ffff:10.0.0.5]", 443),
            ])
            .unwrap();

        let refused = tracker.create_scoped_job(
            "rack-a",
            "switch-a",
            JobType::ConfigureScaleUpFabricManagerV2,
            vec![JobScope::endpoint("10.0.0.5", 443)],
        );

        assert!(refused.is_err());

        drop(reservation);
    }

    #[test]
    fn explicit_fabric_job_holds_aliases_until_verified_completion() {
        let tracker = JobTracker::default();
        let scope = JobScope::endpoint("switch.example", 443);

        let job = tracker
            .create_scoped_job(
                "rack-a",
                "",
                JobType::ConfigureScaleUpFabricManagerV2,
                vec![scope.clone()],
            )
            .unwrap();

        let job_id = job.id().clone();

        assert!(job.progress("Verifying links"));
        tracker.refresh_all_parents();

        let running = tracker.get_job(job_id.as_ref()).unwrap();

        assert_eq!(running.state, JobState::Running);
        assert_eq!(running.state_description, "Verifying links");
        assert!(running.child_job_ids.is_empty());
        assert!(tracker.reserve_mutation(vec![scope.clone()]).is_err());

        job.complete_verified("Verified", "");

        let completed = tracker.get_job(job_id.as_ref()).unwrap();

        assert_eq!(completed.state, JobState::Completed);
        assert_eq!(completed.state_description, "Verified");
        assert!(completed.result_json.is_empty());
        assert!(tracker.reserve_mutation(vec![scope]).is_ok());
        assert!(!tracker.registry.mark_running(&job_id, "Late progress"));

        let retained = tracker.get_job(job_id.as_ref()).unwrap();

        assert_eq!(retained.state, JobState::Completed);
        assert_eq!(retained.state_description, "Verified");
        assert_eq!(retained.updated_at, completed.updated_at);
    }

    #[test]
    fn cancelled_scoped_job_holds_aliases_until_handle_drop() {
        let tracker = JobTracker::default();
        let scope = JobScope::endpoint("switch.example", 443);

        let job = tracker
            .create_scoped_job(
                "rack-a",
                "",
                JobType::ConfigureScaleUpFabricManagerV2,
                vec![scope.clone()],
            )
            .unwrap();

        let job_id = job.id().to_string();

        assert!(job.progress("Configuring"));
        assert!(tracker.registry.cancel(job.id()));
        assert!(job.cancellation_token().is_cancelled());
        assert!(tracker.reserve_mutation(vec![scope.clone()]).is_err());
        assert_eq!(tracker.get_job(&job_id).unwrap().state, JobState::Running);

        drop(job);

        let failed = tracker.get_job(&job_id).unwrap();

        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.state_description, CANCELLED_JOB_DESCRIPTION);
        assert_eq!(failed.error_code, JobError::Other);
        assert!(tracker.reserve_mutation(vec![scope]).is_ok());
    }

    #[test]
    fn failed_scoped_job_records_failure_and_releases_aliases() {
        let tracker = JobTracker::default();
        let scope = JobScope::endpoint("switch.example", 443);

        let job = tracker
            .create_scoped_job(
                "rack-a",
                "",
                JobType::ConfigureScaleUpFabricManagerV2,
                vec![scope.clone()],
            )
            .unwrap();

        let job_id = job.id().to_string();

        job.fail(job_failure(JobError::Timeout, "Verification failed"));

        let failed = tracker.get_job(&job_id).unwrap();

        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.error_code, JobError::Timeout);
        assert_eq!(failed.error_message, "Verification failed");

        assert!(tracker.reserve_mutation(vec![scope]).is_ok());
    }

    #[tokio::test]
    async fn forced_shutdown_terminalizes_scoped_job_and_drains_worker() {
        let tracker = Arc::new(JobTracker::default());
        let scope = JobScope::endpoint("switch.example", 443);

        let job = tracker
            .create_scoped_job(
                "rack-a",
                "",
                JobType::ConfigureScaleUpFabricManagerV2,
                vec![scope.clone()],
            )
            .unwrap();

        let job_id = job.id().clone();
        let (started, worker_started) = tokio::sync::oneshot::channel();

        tracker
            .spawn_job(job, |job| async move {
                assert!(job.progress("Reconciling"));
                let _job = job;

                started.send(()).unwrap();
                std::future::pending::<()>().await;
            })
            .detach();

        tokio::time::timeout(Duration::from_secs(1), worker_started)
            .await
            .unwrap()
            .unwrap();

        assert!(tracker.reserve_mutation(vec![scope]).is_err());

        let cancelled = tracker
            .shutdown_and_await_terminal(Duration::from_millis(50))
            .await;

        let failed = tracker.get_job(job_id.as_ref()).unwrap();

        assert_eq!(cancelled, 1);
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.state_description, CANCELLED_JOB_DESCRIPTION);
        assert_eq!(failed.error_code, JobError::Other);
        assert!(tracker.registry.task_job_ids().is_empty());
        assert!(!tracker.registry.mark_running(&job_id, "Late progress"));
    }

    #[test]
    fn concurrent_overlap_admits_exactly_one_complete_reservation() {
        let tracker = Arc::new(JobTracker::default());
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let scopes = vec![
            JobScope::endpoint("switch-a", 443),
            JobScope::endpoint("switch-b", 443),
        ];

        let threads: Vec<_> = (0..2)
            .map(|index| {
                let tracker = Arc::clone(&tracker);
                let barrier = Arc::clone(&barrier);
                let mut scopes = scopes.clone();

                if index == 1 {
                    scopes.reverse();
                }

                std::thread::spawn(move || {
                    barrier.wait();
                    let reservation = tracker.reserve_mutation(scopes);
                    let admitted = reservation.is_ok();

                    barrier.wait();
                    drop(reservation);
                    admitted
                })
            })
            .collect();

        let admitted = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .filter(|admitted| *admitted)
            .count();

        assert_eq!(admitted, 1);
        assert!(tracker.reserve_mutation(scopes).is_ok());
    }

    #[test]
    fn capacity_and_shutdown_rejections_leave_scopes_unreserved() {
        let tracker = JobTracker::builder().max_tracked_jobs(0).build().unwrap();
        let scope = JobScope::endpoint("switch-a", 443);

        let refused = tracker.create_scoped_job(
            "rack-a",
            "",
            JobType::SwitchFactoryDefaultReset,
            vec![scope.clone()],
        );

        assert!(refused.is_err());
        assert!(tracker.reserve_mutation(vec![scope.clone()]).is_ok());
        tracker.shutdown();

        assert!(tracker.reserve_mutation(vec![scope]).is_err());
    }

    #[test]
    fn worker_alias_attachment_is_atomic_and_retains_existing_job_aliases() {
        let tracker = JobTracker::default();

        let job = tracker
            .create_job_if_node_idle("rack-a", "switch-a", JobType::FirmwareUpdate)
            .unwrap();

        let scope = JobScope::endpoint("switch-a", 443);
        let occupied = JobScope::endpoint("switch-b", 443);
        let reservation = tracker.reserve_mutation(vec![occupied.clone()]).unwrap();

        assert!(
            tracker
                .registry
                .reserve_job_scopes(job.id(), vec![scope.clone(), occupied.clone()])
                .is_err()
        );

        assert!(tracker.reserve_mutation(vec![scope.clone()]).is_ok());
        drop(reservation);

        tracker
            .registry
            .reserve_job_scopes(job.id(), vec![scope.clone(), occupied.clone()])
            .unwrap();

        tracker
            .registry
            .reserve_job_scopes(job.id(), vec![scope.clone()])
            .unwrap();

        assert!(
            tracker
                .create_job_if_node_idle("rack-a", "switch-a", JobType::FirmwareUpdate)
                .is_err()
        );

        assert!(tracker.reserve_mutation(vec![occupied.clone()]).is_err());
        let job_id = job.id().clone();
        job.complete_verified("Verified", "");

        assert!(
            tracker
                .reserve_mutation(vec![scope.clone(), occupied])
                .is_ok()
        );

        assert!(
            tracker
                .registry
                .reserve_job_scopes(&job_id, vec![scope])
                .is_err()
        );
    }

    fn job_span_exporter() -> (SdkTracerProvider, InMemorySpanExporter, tracing::Dispatch) {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("job-lifecycle-error-test"))
                .with_filter(tracing_subscriber::filter::filter_fn(
                    crate::observability::tracing::should_export,
                )),
        );
        (provider, exporter, tracing::Dispatch::new(subscriber))
    }

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

    #[test]
    fn child_job_span_is_parented_to_the_parent_job_span() {
        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "child_job_span_is_parented_to_the_parent_job_span"
        )) {
            return;
        }

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let otel_layer =
            tracing_opentelemetry::layer().with_tracer(provider.tracer("job-parenting-test"));
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        tracing::subscriber::with_default(subscriber, || {
            let tracker = Arc::new(JobTracker::builder().build().unwrap());
            let request = tracing::info_span!("request");
            let _request_entered = request.enter();
            let parent_id = tracker
                .create_parent_job("rack-1", JobType::FirmwareUpdate)
                .expect("parent job");
            let child = tracker
                .create_child_job(&parent_id, "rack-1", "node-1", JobType::FirmwareUpdate)
                .expect("child job");

            drop(child);
            drop(_request_entered);
            drop(request);
            drop(tracker);
        });

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let request = spans
            .iter()
            .find(|span| span.name == "request")
            .expect("request span");
        let child = spans
            .iter()
            .find(|span| {
                span.name == "firmware_update"
                    && span
                        .attributes
                        .iter()
                        .any(|attribute| attribute.key.as_str() == "parent_job_id")
            })
            .expect("child job span");
        let parent = spans
            .iter()
            .find(|span| {
                span.name == "firmware_update"
                    && !span
                        .attributes
                        .iter()
                        .any(|attribute| attribute.key.as_str() == "parent_job_id")
            })
            .expect("parent job span");

        assert_eq!(parent.parent_span_id, request.span_context.span_id());
        assert_eq!(child.parent_span_id, parent.span_context.span_id());
        assert_eq!(
            child.span_context.trace_id(),
            request.span_context.trace_id()
        );
    }

    #[test]
    fn every_job_type_exports_parent_and_child_spans_for_success_and_failure() {
        use opentelemetry::trace::Status;

        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "every_job_type_exports_parent_and_child_spans_for_success_and_failure"
        )) {
            return;
        }

        let cases = [
            (JobType::FirmwareUpdate, "firmware_update"),
            (
                JobType::ConfigureScaleUpFabricManagerV2,
                "configure_scale_up_fabric_manager_v2",
            ),
            (JobType::SwitchCertificate, "switch_certificate"),
            (JobType::SwitchMtlsDisable, "switch_mtls_disable"),
            (
                JobType::SwitchSdnFactoryDefaultReset,
                "switch_sdn_factory_default_reset",
            ),
            (
                JobType::SwitchFactoryDefaultReset,
                "switch_factory_default_reset",
            ),
            (
                JobType::SwitchSystemPasswordUpdate,
                "switch_system_password_update",
            ),
            (
                JobType::SwitchSystemImageUpdate,
                "switch_system_image_update",
            ),
            (JobType::ColdRebootSequence, "cold_reboot_sequence"),
            (JobType::SystemValidation, "system_validation"),
        ];
        assert_eq!(JOB_SPAN_NAMES.len(), cases.len());
        let (provider, exporter, dispatch) = job_span_exporter();
        let _subscriber = tracing::dispatcher::set_default(&dispatch);
        let tracker = Arc::new(JobTracker::new());
        let request = tracing::info_span!("request");
        let request_context = request.context().span().span_context().clone();
        let entered = request.enter();
        drop(tracing::info_span!("unlisted_job_span"));

        for (job_type, name) in cases {
            assert_eq!(job_type.span_name(), name);
            for failed in [false, true] {
                let parent_id = tracker.create_parent_job("rack", job_type).unwrap();
                let child = tracker
                    .create_child_job(&parent_id, "rack", "node", job_type)
                    .unwrap();
                let child_id = child.id().to_string();
                if failed {
                    child.fail(job_failure(JobError::Timeout, "private-error-detail"));
                } else {
                    child.complete_verified("Verified", "");
                }
                assert_eq!(
                    tracker.get_job(&parent_id).unwrap().state,
                    if failed {
                        JobState::Failed
                    } else {
                        JobState::Completed
                    },
                );

                let spans = exporter.get_finished_spans().unwrap();
                let span_for_job = |job_id: &str| {
                    spans
                        .iter()
                        .find(|span| {
                            span.attributes.iter().any(|attribute| {
                                attribute.key.as_str() == "job_id"
                                    && attribute.value.as_str() == job_id
                            })
                        })
                        .expect("each terminal job must export through the catalog")
                };
                let parent = span_for_job(&parent_id);
                let child = span_for_job(&child_id);
                assert_eq!(parent.name, name);
                assert_eq!(child.name, name);
                assert_eq!(parent.parent_span_id, request_context.span_id());
                assert_eq!(child.parent_span_id, parent.span_context.span_id());
                assert_eq!(parent.span_context.trace_id(), request_context.trace_id());
                assert_eq!(child.span_context.trace_id(), request_context.trace_id());
                assert!(
                    !parent
                        .attributes
                        .iter()
                        .any(|attribute| attribute.key.as_str() == "parent_job_id")
                );
                assert_eq!(
                    child
                        .attributes
                        .iter()
                        .filter(|attribute| attribute.key.as_str() == "parent_job_id")
                        .count(),
                    1,
                );
                assert!(child.attributes.iter().any(|attribute| {
                    attribute.key.as_str() == "parent_job_id"
                        && attribute.value.as_str() == parent_id
                }));
                for span in [parent, child] {
                    assert_eq!(
                        span.attributes
                            .iter()
                            .filter(|attribute| attribute.key.as_str() == "job_id")
                            .count(),
                        1
                    );
                    assert!(!format!("{span:?}").contains("private-error-detail"));
                }
                assert_eq!(
                    parent.status,
                    if failed {
                        Status::error("")
                    } else {
                        Status::Unset
                    }
                );
                assert_eq!(
                    child.status,
                    if failed {
                        Status::error("")
                    } else {
                        Status::Ok
                    }
                );
                if failed {
                    for (span, error_type) in [(parent, "other"), (child, "timeout")] {
                        assert!(span.attributes.iter().any(|attribute| {
                            attribute.key.as_str() == "error.type"
                                && attribute.value.as_str() == error_type
                        }));
                    }
                }
            }
        }
        drop(entered);
        drop(request);
        provider.force_flush().unwrap();
        assert_eq!(
            exporter.get_finished_spans().unwrap().len(),
            cases.len() * 4 + 1
        );
    }

    #[test]
    fn job_parent_attachment_failure_warns_once_without_failing_jobs() {
        use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};

        // Isolate the warning budget and the shared job-span callsites from other tests.
        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "job_parent_attachment_failure_warns_once_without_failing_jobs"
        )) {
            return;
        }

        let parent_span_context = SpanContext::new(
            TraceId::from(1_u128),
            SpanId::from(2_u64),
            TraceFlags::SAMPLED,
            true,
            TraceState::from_key_value([("vendor", "private-trace-state")]).unwrap(),
        );
        let parent =
            opentelemetry::Context::new().with_remote_span_context(parent_span_context.clone());
        for has_otel_layer in [true, false] {
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            let writer = TestWriter::new();
            let cloned_writer = writer.clone();
            let log_layer = crate::logging::logfmt::layer()
                .with_writer(Arc::new(move || Box::new(cloned_writer.clone())))
                .with_span_logs(false);
            let subscriber =
                tracing_subscriber::registry()
                    .with(log_layer)
                    .with(has_otel_layer.then(|| {
                        tracing_opentelemetry::layer()
                            .with_tracer(provider.tracer("job-parent-attachment-test"))
                    }));

            tracing::subscriber::with_default(subscriber, || {
                let tracker = JobTracker::new();
                let request = tracing::info_span!("request", rpc.method = "UpdateFirmware");
                let _entered = request.enter();
                for job_type in [JobType::FirmwareUpdate, JobType::SwitchSystemImageUpdate] {
                    let parent = parent.clone();
                    let job = tracker
                        .registry
                        .create_job(
                            JobSpec::new_deferred(
                                "private-rack",
                                "private-node",
                                "Queued",
                                move |_| job_span(job_type, "private-parent-job", Some(parent)),
                            ),
                            None,
                            false,
                        )
                        .expect("trace attachment must not reject a job");
                    let job_id = job.id().to_string();
                    job.complete("Completed", "");
                    assert_eq!(tracker.get_job(&job_id).unwrap().state, JobState::Completed);
                }
            });

            provider.force_flush().unwrap();
            let spans = exporter.get_finished_spans().unwrap();
            let logs = writer.text();
            let warnings: Vec<_> = logs
                .lines()
                .filter(|line| line.starts_with("level=WARN"))
                .collect();
            if has_otel_layer {
                assert!(
                    warnings.is_empty(),
                    "successful parent attachment must not warn: {logs}"
                );
                let job_spans: Vec<_> =
                    spans.iter().filter(|span| span.name != "request").collect();
                assert_eq!(job_spans.len(), 2);
                for span in job_spans {
                    assert_eq!(span.parent_span_id, parent_span_context.span_id());
                    assert_eq!(span.span_context.trace_id(), parent_span_context.trace_id());
                }
            } else {
                assert!(spans.is_empty());
                assert_eq!(
                    warnings.len(),
                    1,
                    "repeated attachment failures must warn once: {logs}"
                );
                let warning = warnings[0];
                assert!(warning.contains("could not apply job trace context"));
                assert!(warning.contains("job_type=firmware_update"));
                assert!(warning.contains("error=LayerNotFound"));
                assert!(
                    !warning.contains("private-"),
                    "warning leaked context: {warning}"
                );
            }
        }
    }

    #[test]
    fn terminal_job_exports_before_its_retained_snapshot_is_evicted() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let otel_layer =
            tracing_opentelemetry::layer().with_tracer(provider.tracer("job-terminal-test"));
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        let (tracker, job_id) = tracing::subscriber::with_default(subscriber, || {
            let tracker = Arc::new(JobTracker::builder().build().unwrap());
            let request = tracing::info_span!("request");
            let _request_entered = request.enter();
            let job = tracker
                .create_visible_workflow_job_if_node_idle(
                    "rack-1",
                    "node-1",
                    JobType::FirmwareUpdate,
                )
                .expect("job");
            let job_id = job.id().to_string();
            job.complete("done", r#"{"status":"ok"}"#);
            (tracker, job_id)
        });

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let request_span = spans
            .iter()
            .find(|span| span.name == "request")
            .expect("request root must export while the terminal job remains retained");
        let job_span = spans
            .iter()
            .find(|span| span.name == "firmware_update")
            .expect("terminal job span must export while its snapshot remains retained");
        assert_eq!(job_span.parent_span_id, request_span.span_context.span_id());
        assert_eq!(job_span.status, opentelemetry::trace::Status::Unset);
        assert!(
            tracker.get_job(&job_id).is_some(),
            "terminal snapshot should still be retained"
        );
    }

    #[test]
    fn verified_job_exports_ok_before_its_snapshot_is_evicted() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let otel_layer =
            tracing_opentelemetry::layer().with_tracer(provider.tracer("job-verified-test"));
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        let (tracker, job_id) = tracing::subscriber::with_default(subscriber, || {
            let tracker = Arc::new(JobTracker::builder().build().unwrap());
            let job = tracker
                .create_visible_workflow_job_if_node_idle(
                    "rack-1",
                    "node-1",
                    JobType::SwitchFactoryDefaultReset,
                )
                .expect("job");
            let job_id = job.id().to_string();
            job.complete_verified("Factory-default readiness verified", "");
            (tracker, job_id)
        });

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "switch_factory_default_reset");
        assert_eq!(spans[0].status, opentelemetry::trace::Status::Ok);
        assert_eq!(tracker.get_job(&job_id).unwrap().state, JobState::Completed);
    }

    #[test]
    fn late_verified_completion_cannot_override_a_winning_failure() {
        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "late_verified_completion_cannot_override_a_winning_failure"
        )) {
            return;
        }

        struct CompleteDuringFailureLog(Arc<Mutex<Option<RmsJobHandle>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CompleteDuringFailureLog {
            fn on_event(
                &self,
                _event: &tracing::Event<'_>,
                _context: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let job = self.0.lock().unwrap().take();
                if let Some(job) = job {
                    // The failure has won the registry transition, but its
                    // terminal log runs before the SDK status is set or ended.
                    job.complete_verified("Late worker success", "");
                }
            }
        }

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let otel_layer =
            tracing_opentelemetry::layer().with_tracer(provider.tracer("job-terminal-race-test"));
        let late_completion = Arc::new(Mutex::new(None));
        let subscriber = tracing_subscriber::registry()
            .with(otel_layer)
            .with(CompleteDuringFailureLog(late_completion.clone()));

        let (tracker, job_id) = tracing::subscriber::with_default(subscriber, || {
            let tracker = Arc::new(JobTracker::builder().build().unwrap());
            let job = tracker
                .create_visible_workflow_job_if_node_idle(
                    "rack-1",
                    "node-1",
                    JobType::SwitchFactoryDefaultReset,
                )
                .expect("job");
            let job_id = job.id().clone();
            *late_completion.lock().unwrap() = Some(job);
            tracker
                .registry
                .fail_non_terminal_for_shutdown(std::slice::from_ref(&job_id));
            assert!(late_completion.lock().unwrap().is_none());
            (tracker, job_id)
        });

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(
            spans.len(),
            1,
            "the losing completion must not export again"
        );
        assert_eq!(spans[0].status, opentelemetry::trace::Status::error(""));
        assert_eq!(
            tracker.get_job(job_id.as_ref()).unwrap().state,
            JobState::Failed
        );
    }

    #[test]
    fn parent_of_an_uncertain_completed_child_keeps_unset_status() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let otel_layer =
            tracing_opentelemetry::layer().with_tracer(provider.tracer("job-parent-status-test"));
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        let (tracker, parent_id, verified_span_id, uncertain_span_id) =
            tracing::subscriber::with_default(subscriber, || {
                let tracker = Arc::new(JobTracker::builder().build().unwrap());
                let (parent_id, children) = create_test_batch(
                    tracker.as_ref(),
                    "rack-1",
                    &["node-1", "node-2"],
                    JobType::SwitchSystemImageUpdate,
                );
                let [verified, uncertain] = children.try_into().unwrap_or_else(|_| {
                    panic!("expected two child jobs");
                });
                let verified_span_id = verified.span().context().span().span_context().span_id();
                let uncertain_span_id = uncertain.span().context().span().span_context().span_id();
                verified.complete_verified("Target state verified", "");
                uncertain.complete("Submitted; target state not verified", "");
                assert_eq!(
                    tracker.get_job(&parent_id).unwrap().state,
                    JobState::Completed
                );
                (tracker, parent_id, verified_span_id, uncertain_span_id)
            });

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 3);
        for span in &spans {
            let expected = if span.span_context.span_id() == verified_span_id {
                opentelemetry::trace::Status::Ok
            } else {
                opentelemetry::trace::Status::Unset
            };
            assert_eq!(span.status, expected, "span: {span:?}");
        }
        assert!(spans.iter().any(|span| {
            span.span_context.span_id() == uncertain_span_id
                && span.status == opentelemetry::trace::Status::Unset
        }));
        assert_eq!(
            tracker.get_job(&parent_id).unwrap().state,
            JobState::Completed
        );
    }

    #[test]
    fn failed_job_exports_bounded_error_taxonomy() {
        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "failed_job_exports_bounded_error_taxonomy"
        )) {
            return;
        }

        let (provider, exporter, dispatch) = job_span_exporter();
        let _subscriber = tracing::dispatcher::set_default(&dispatch);
        let tracker = JobTracker::new();
        for (error, expected_code, error_type) in [
            (
                anyhow::Error::new(JobError::Timeout),
                JobError::Timeout,
                "timeout",
            ),
            (
                anyhow::Error::new(JobError::ClientError).context("private-error-context"),
                JobError::ClientError,
                "client_error",
            ),
            (
                anyhow::anyhow!("private-untyped-error"),
                JobError::Internal,
                "internal",
            ),
        ] {
            let job = tracker
                .create_visible_workflow_job_if_node_idle("rack", "node", JobType::FirmwareUpdate)
                .unwrap();
            let job_id = job.id().to_string();
            job.fail(JobFailure::new(error, "private-failure-detail"));
            assert_eq!(tracker.get_job(&job_id).unwrap().error_code, expected_code);

            provider.force_flush().unwrap();
            let spans = exporter.get_finished_spans().unwrap();
            let span = spans.last().expect("failed job span");
            assert_eq!(span.name, "firmware_update");
            assert_eq!(span.status, opentelemetry::trace::Status::error(""));
            assert!(
                span.attributes.iter().any(|attribute| {
                    attribute.key.as_str() == "error.type" && attribute.value.as_str() == error_type
                }),
                "span attributes: {:?}",
                span.attributes
            );
            assert!(
                !format!("{span:?}").contains("private-"),
                "raw failure detail must not enter trace data"
            );
        }
        assert_eq!(exporter.get_finished_spans().unwrap().len(), 3);
    }

    #[test]
    fn rejected_job_admission_does_not_emit_a_job_span() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let otel_layer =
            tracing_opentelemetry::layer().with_tracer(provider.tracer("job-rejection-test"));
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        tracing::subscriber::with_default(subscriber, || {
            let tracker = JobTracker::builder().max_tracked_jobs(1).build().unwrap();
            let accepted = tracker
                .create_job_if_node_idle("rack-1", "node-1", JobType::FirmwareUpdate)
                .expect("first job");
            let accepted_id = accepted.id().to_string();
            tracker
                .create_job_if_node_idle("rack-1", "node-2", JobType::FirmwareUpdate)
                .expect_err("capacity rejection");

            provider.force_flush().unwrap();
            assert!(
                exporter.get_finished_spans().unwrap().is_empty(),
                "rejected admission must not close an otherwise nonexistent job span"
            );
            accepted.complete("done", r#"{"status":"ok"}"#);
            let spans = exporter.get_finished_spans().unwrap();
            assert_eq!(spans.len(), 1, "the admitted job must still export");
            assert_eq!(spans[0].name, "firmware_update");
            assert!(spans[0].attributes.iter().any(|attribute| {
                attribute.key.as_str() == "job_id" && attribute.value.as_str() == accepted_id
            }));
        });
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

    /// A batch target whose payload identifies the node it came from, so tests
    /// can tell which target each admitted job belongs to.
    fn batch_target(rack_id: &str, node_id: &str) -> (String, String, String) {
        (
            rack_id.to_owned(),
            node_id.to_owned(),
            format!("payload for {node_id}"),
        )
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

    fn captured_logs(run: impl FnOnce()) -> String {
        let writer = TestWriter::new();
        let cloned_writer = writer.clone();
        let layer = crate::logging::logfmt::layer()
            .with_writer(Arc::new(move || Box::new(cloned_writer.clone())))
            .with_span_logs(false);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, run);

        writer.text()
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
        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "child_job_span_records_child_and_parent_job_ids"
        )) {
            return;
        }

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
    fn create_batch_jobs_admits_every_checked_target() {
        let tracker = JobTracker::new();
        let parent_id = tracker
            .create_parent_job("rack-01", JobType::FirmwareUpdate)
            .unwrap();

        let (admitted, rejected) = tracker.create_batch_jobs(
            &parent_id,
            JobType::FirmwareUpdate,
            vec![
                batch_target("rack-01", "node-01"),
                batch_target("rack-01", "node-02"),
            ],
            |payload| Ok(payload.len()),
        );

        assert!(rejected.is_empty());
        assert_eq!(admitted.len(), 2);
        assert_eq!(admitted[0].0, "payload for node-01");
        assert_eq!(admitted[0].2, "payload for node-01".len());
        assert_eq!(admitted[1].0, "payload for node-02");

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.child_job_ids.len(), 2);
    }

    #[test]
    fn create_batch_jobs_rejects_a_repeated_target_before_checking_it() {
        let tracker = JobTracker::new();
        let parent_id = tracker
            .create_parent_job("rack-01", JobType::FirmwareUpdate)
            .unwrap();
        let mut checked = Vec::new();

        let (admitted, rejected) = tracker.create_batch_jobs(
            &parent_id,
            JobType::FirmwareUpdate,
            vec![
                batch_target("rack-01", "node-01"),
                batch_target("rack-01", "node-01"),
            ],
            |payload| {
                checked.push(payload.clone());
                Ok(())
            },
        );

        // The duplicate is refused without running the caller's check, so it is
        // reported as a duplicate rather than as busy behind its own sibling.
        assert_eq!(checked, ["payload for node-01"]);
        assert_eq!(admitted.len(), 1);
        assert_eq!(
            rejected,
            [RejectedBatchTarget {
                rack_id: "rack-01".to_owned(),
                node_id: "node-01".to_owned(),
                error_message: "duplicate target rack_id/node_id: rack-01/node-01".to_owned(),
            }]
        );

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.child_job_ids.len(), 1);
    }

    #[test]
    fn create_batch_jobs_admits_the_same_node_id_in_another_rack() {
        let tracker = JobTracker::new();
        let parent_id = tracker
            .create_parent_job("rack-01", JobType::FirmwareUpdate)
            .unwrap();

        let (admitted, rejected) = tracker.create_batch_jobs(
            &parent_id,
            JobType::FirmwareUpdate,
            vec![
                batch_target("rack-01", "node-01"),
                batch_target("rack-02", "node-01"),
            ],
            |_| Ok(()),
        );

        assert_eq!(admitted.len(), 2);
        assert!(rejected.is_empty());
    }

    #[test]
    fn create_batch_jobs_reports_check_failures_without_creating_a_job() {
        let tracker = JobTracker::new();
        let parent_id = tracker
            .create_parent_job("rack-01", JobType::FirmwareUpdate)
            .unwrap();

        let (admitted, rejected) = tracker.create_batch_jobs(
            &parent_id,
            JobType::FirmwareUpdate,
            vec![
                batch_target("rack-01", "node-01"),
                batch_target("rack-01", "node-02"),
            ],
            |payload| match payload.ends_with("node-01") {
                true => Err("node-01 is not a switch".to_owned()),
                false => Ok(()),
            },
        );

        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].0, "payload for node-02");
        assert_eq!(
            rejected,
            [RejectedBatchTarget {
                rack_id: "rack-01".to_owned(),
                node_id: "node-01".to_owned(),
                error_message: "node-01 is not a switch".to_owned(),
            }]
        );

        let parent = tracker.get_job(&parent_id).unwrap();
        assert_eq!(parent.child_job_ids.len(), 1);
    }

    #[test]
    fn create_batch_jobs_reports_the_refusal_when_a_node_is_busy() {
        let tracker = JobTracker::new();

        // Own the node with an unrelated job so no child can be admitted for it.
        let busy = tracker
            .create_job("rack-01", "node-01", JobType::FirmwareUpdate)
            .unwrap();
        let parent_id = tracker
            .create_parent_job("rack-01", JobType::FirmwareUpdate)
            .unwrap();

        let (admitted, rejected) = tracker.create_batch_jobs(
            &parent_id,
            JobType::FirmwareUpdate,
            vec![batch_target("rack-01", "node-01")],
            |_| Ok(()),
        );

        assert!(admitted.is_empty());
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].rack_id, "rack-01");
        assert_eq!(rejected[0].node_id, "node-01");
        assert!(
            rejected[0].error_message.contains(busy.id().as_ref()),
            "expected a node-busy refusal naming the active job, got: {}",
            rejected[0].error_message
        );
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

    /// A panicking worker still terminalizes its child through the RAII drop
    /// guard, so the parent must aggregate without waiting for an external
    /// `get_job` or the next reaper sweep. The refresh runs from the registry's
    /// worker-exit hook; appending it to the worker future skipped it here,
    /// because the panic unwound past the tail of the future.
    #[tokio::test]
    async fn panicking_child_refreshes_parent_without_polling() {
        // Unsubscribed parallel tests use the same job-span callsites.
        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "panicking_child_refreshes_parent_without_polling"
        )) {
            return;
        }
        let (provider, exporter, subscriber) = job_span_exporter();
        let _subscriber = tracing::dispatcher::set_default(&subscriber);
        let tracker = Arc::new(JobTracker::new());
        let (parent_id, mut children) = create_test_batch(
            tracker.as_ref(),
            "rack-01",
            &["n-01"],
            JobType::FirmwareUpdate,
        );
        let child = children.pop().expect("child job should be created");
        let child_id = child.id().clone();

        // The supervisor catches the panic as a JoinError, so `wait` resolves.
        tracker
            .spawn_job(child, |job| async move {
                let _job = job;
                panic!("worker exploded");
            })
            .wait()
            .await
            .expect("the supervisor should survive a panicking worker");

        let child = tracker
            .registry
            .get(&child_id)
            .expect("child job should exist");
        assert_eq!(
            snapshot_state(&child.state),
            JobState::Failed,
            "the drop guard should seal a panicking child"
        );

        // Read through the registry, not `get_job`, so the assertion cannot be
        // satisfied by the refresh that polling would itself trigger.
        let parent = tracker
            .registry
            .get(&JobId::from(parent_id.as_str()))
            .expect("parent job should exist");
        assert_eq!(
            snapshot_state(&parent.state),
            JobState::Failed,
            "the parent must not still be Running after its only child panicked"
        );

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(
            spans.len(),
            2,
            "both failures must export while their snapshots are still retained"
        );
        let child_span = spans
            .iter()
            .find(|span| {
                span.attributes.iter().any(|attribute| {
                    attribute.key.as_str() == "job_id"
                        && attribute.value.as_str() == child_id.as_ref()
                })
            })
            .expect("panicking child span");
        let parent_span = spans
            .iter()
            .find(|span| {
                span.attributes.iter().any(|attribute| {
                    attribute.key.as_str() == "job_id" && attribute.value.as_str() == parent_id
                })
            })
            .expect("failed parent span");
        for (span, category) in [(child_span, "internal"), (parent_span, "other")] {
            assert_eq!(span.name, "firmware_update");
            assert_eq!(span.status, opentelemetry::trace::Status::error(""));
            assert!(span.attributes.iter().any(|attribute| {
                attribute.key.as_str() == "error.type" && attribute.value.as_str() == category
            }));
        }
        assert_eq!(
            child_span.parent_span_id,
            parent_span.span_context.span_id()
        );
        assert_eq!(
            child_span.span_context.trace_id(),
            parent_span.span_context.trace_id()
        );
        assert_ne!(
            child_span.span_context.span_id(),
            parent_span.span_context.span_id()
        );
        assert!(
            !format!("{spans:?}").contains("worker exploded"),
            "raw panic details must not enter trace data"
        );
    }

    /// Handlers that seal an admitted child directly -- rather than spawning a
    /// worker for it -- must call `refresh_parent_for_child`, because only the
    /// spawn path aggregates the parent on its own. Batch handlers rely on this
    /// to skip their "nothing was created" fallback once any child was admitted.
    #[test]
    fn directly_sealed_children_seal_the_parent_once_refreshed() {
        let tracker = JobTracker::new();
        let (parent_id, children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01", "n-02"],
            JobType::SwitchSystemImageUpdate,
        );

        for child in children {
            let child_id = JobId::from(child.id().as_ref());
            child.fail(JobFailure::new(JobError::ClientError, "nvue setup failed"));
            tracker.refresh_parent_for_child(&child_id);
        }

        // Read through the registry, not `get_job`, so the assertion cannot be
        // satisfied by the refresh that polling would itself trigger.
        let parent = tracker
            .registry
            .get(&JobId::from(parent_id.as_str()))
            .expect("parent job should exist");
        assert_eq!(
            snapshot_state(&parent.state),
            JobState::Failed,
            "a parent whose children were all sealed directly must not stay Running"
        );
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
    fn repeated_refresh_of_failed_parent_does_not_log_missing_handle() {
        let tracker = JobTracker::new();
        let (parent_id, mut children) = create_test_batch(
            &tracker,
            "rack-01",
            &["n-01"],
            JobType::SwitchSystemImageUpdate,
        );
        let child = children.pop().expect("child job should be created");

        let logs = captured_logs(|| {
            fail_child_and_refresh(
                &tracker,
                child,
                JobFailure::new(JobError::ClientError, "connection lost"),
            );

            for _ in 0..3 {
                tracker.get_job(&parent_id);
                tracker.reaper_sweep();
            }
        });

        assert!(
            !logs.contains("parent job handle missing"),
            "re-aggregating a sealed parent must not report a missing handle: {logs}"
        );
        let parent = tracker
            .get_job(&parent_id)
            .expect("parent job should exist");
        assert_eq!(parent.state, JobState::Failed);
        assert!(parent.result_json.contains("connection lost"));
    }

    #[test]
    fn repeated_refresh_of_completed_parent_does_not_log_missing_handle() {
        let tracker = JobTracker::new();
        let (parent_id, mut children) =
            create_test_batch(&tracker, "rack-01", &["n-01"], JobType::FirmwareUpdate);
        let child = children.pop().expect("child job should be created");

        let logs = captured_logs(|| {
            complete_child_and_refresh(&tracker, child, "");

            for _ in 0..3 {
                tracker.get_job(&parent_id);
                tracker.reaper_sweep();
            }
        });

        assert!(
            !logs.contains("parent job handle missing"),
            "re-aggregating a sealed parent must not report a missing handle: {logs}"
        );
        assert_eq!(
            tracker
                .get_job(&parent_id)
                .expect("parent job should exist")
                .state,
            JobState::Completed
        );
    }

    #[test]
    fn missing_handle_for_non_terminal_parent_is_still_reported() {
        let tracker = JobTracker::new();
        // Holding the handle here rather than in `parent_job_handles` leaves the
        // parent non-terminal with no handle to seal it. Removing a map entry
        // instead would drop the handle and seal the parent as failed.
        let orphan_parent = tracker
            .registry
            .create_job(
                JobSpec::new_parent(
                    "rack-01",
                    "Batch accepting child jobs",
                    job_span(JobType::FirmwareUpdate, "", None),
                )
                .with_workflow_type(JobType::FirmwareUpdate),
                None,
                false,
            )
            .expect("parent job should be created");
        let parent_id = orphan_parent.id().clone();
        let child = tracker
            .registry
            .create_job(
                JobSpec::new(
                    "rack-01",
                    "n-01",
                    "Queued",
                    job_span(JobType::FirmwareUpdate, "", None),
                ),
                Some(&parent_id),
                false,
            )
            .expect("child job should be created");

        let logs = captured_logs(|| {
            child.fail(JobFailure::new(JobError::ClientError, "connection lost"));
            refresh_parent_state(&tracker, &parent_id);
        });

        assert!(
            logs.contains("parent job handle missing during failure aggregation"),
            "a non-terminal parent with no handle must stay a reported invariant violation: {logs}"
        );
        assert!(
            !tracker
                .registry
                .get(&parent_id)
                .expect("parent job should exist")
                .state
                .is_terminal()
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
    async fn shutdown_and_await_terminal_fails_job_that_never_terminates() {
        let tracker = JobTracker::new();
        let running = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        tracker.mark_running(&running, "uploading");

        // The job is cancelled but nothing seals it, so shutdown terminalizes
        // it after the bounded cooperative wait.
        let cancelled = tracker
            .shutdown_and_await_terminal(Duration::from_millis(50))
            .await;

        assert_eq!(cancelled, 1);
        let job = tracker.get_job(&running).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_code, JobError::Other);
    }

    #[tokio::test]
    async fn shutdown_aborts_and_joins_unresponsive_worker() {
        // Unsubscribed parallel tests use the same job-span callsites.
        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "shutdown_aborts_and_joins_unresponsive_worker"
        )) {
            return;
        }
        let (provider, exporter, subscriber) = job_span_exporter();
        let _subscriber = tracing::dispatcher::set_default(&subscriber);
        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let job_id = job.id().to_string();
        let (started, worker_started) = tokio::sync::oneshot::channel();
        tracker
            .spawn_job(job, |job| async move {
                let _job = job;
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            })
            .detach();
        tokio::time::timeout(Duration::from_secs(1), worker_started)
            .await
            .expect("worker must start before shutdown")
            .unwrap();

        let cancelled = tracker
            .shutdown_and_await_terminal(Duration::from_millis(50))
            .await;

        assert_eq!(cancelled, 1);
        let job = tracker.get_job(&job_id).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_code, JobError::Other);
        assert!(tracker.registry.task_job_ids().is_empty());

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(
            spans.len(),
            1,
            "shutdown must export the failure before provider shutdown or snapshot eviction"
        );
        let span = &spans[0];
        assert_eq!(span.name, "switch_system_image_update");
        assert_eq!(span.status, opentelemetry::trace::Status::error(""));
        assert!(span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == "error.type" && attribute.value.as_str() == "other"
        }));
        assert!(span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == "job_id" && attribute.value.as_str() == job_id
        }));
    }

    #[tokio::test]
    async fn shutdown_rejected_worker_exports_cancelled_error() {
        use crate::orchestrator::job_lifecycle::CleanupReport;
        use std::sync::atomic::{AtomicBool, Ordering};

        if crate::observability::run_test_in_subprocess(concat!(
            "orchestrator::job_tracker::tests::",
            "shutdown_rejected_worker_exports_cancelled_error"
        )) {
            return;
        }
        let (provider, exporter, subscriber) = job_span_exporter();
        let _subscriber = tracing::dispatcher::set_default(&subscriber);
        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let job_id = job.id().clone();
        assert!(!job.cancellation_token().is_cancelled());
        let started = Arc::new(AtomicBool::new(false));
        let worker_started = started.clone();

        // Reproduce shutdown between admission and spawning, before its
        // cancellation sweep signals the admitted job's token.
        tracker.registry.begin_shutdown();
        let result = tracker
            .spawn_job(job, move |_job| async move {
                worker_started.store(true, Ordering::SeqCst);
            })
            .wait()
            .await;
        assert_eq!(result.unwrap(), CleanupReport::Skipped);
        assert!(
            tracker
                .registry
                .wait_for_terminal_jobs(
                    std::slice::from_ref(&job_id),
                    Some(Duration::from_secs(1)),
                )
                .await
        );
        assert!(!started.load(Ordering::SeqCst));
        assert!(tracker.registry.task_job_ids().is_empty());

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, "switch_system_image_update");
        assert_eq!(span.status, opentelemetry::trace::Status::error(""));
        let category = span
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "error.type")
            .expect("rejected worker must export a bounded error category");
        assert_eq!(category.value.as_str(), "other");
        let job = tracker.get_job(job_id.as_ref()).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.error_code, JobError::Other);
        assert_eq!(job.state_description, "Cancelled");
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drains_late_cleanup_or_aborts_it_at_the_deadline() {
        use crate::orchestrator::job_lifecycle::{CleanupPlan, CleanupReport};
        use std::sync::atomic::{AtomicBool, Ordering};

        for child_completes in [true, false] {
            let tracker = Arc::new(JobTracker::new());
            let mut child = Some(
                tracker
                    .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
                    .unwrap(),
            );
            let child_id = child.as_ref().unwrap().id().clone();
            let pending = tracker
                .registry
                .create_job(
                    JobSpec::new("", "", "Cache cleanup", tracing::Span::none()),
                    None,
                    false,
                )
                .unwrap();
            let cleanup_id = pending.id().clone();
            let temp = tempfile::tempdir().unwrap();
            let cache = temp.path().join("cache");
            std::fs::create_dir(&cache).unwrap();
            std::fs::write(cache.join("image.bin"), b"temporary image").unwrap();
            let started = Arc::new(AtomicBool::new(false));
            let worker_started = started.clone();

            tracker.registry.begin_shutdown();
            let cleanup = tracker.registry.spawn_job_with_cleanup(
                pending,
                CleanupPlan::remove_dir_after_jobs_terminate(
                    vec![child_id.clone()],
                    cache.clone(),
                    None,
                ),
                move |_job| async move {
                    worker_started.store(true, Ordering::SeqCst);
                },
            );
            assert!(
                tracker
                    .registry
                    .wait_for_terminal_jobs(
                        std::slice::from_ref(&cleanup_id),
                        Some(Duration::from_secs(1)),
                    )
                    .await
            );
            assert!(tracker.registry.task_job_ids().contains(&cleanup_id));
            assert!(cache.join("image.bin").exists());
            assert!(!started.load(Ordering::SeqCst));

            // Paused time keeps CI scheduling and filesystem latency from
            // consuming the shutdown budget while cleanup can still progress.
            // Poll shutdown before releasing the child: cleanup must remain
            // tracked and must not remove files still owned by active work.
            let mut drain =
                Box::pin(tracker.shutdown_and_await_terminal(Duration::from_millis(500)));
            assert!(
                std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(std::future::Future::poll(drain.as_mut(), cx))
                })
                .await
                .is_pending()
            );
            assert!(cache.join("image.bin").exists());
            if child_completes {
                child.take().unwrap().complete("Finished using cache", "");
            }

            tokio::time::timeout(Duration::from_secs(2), drain)
                .await
                .expect("shutdown must bound cleanup even if its dependency never finishes");
            let result = tokio::time::timeout(Duration::from_secs(1), cleanup.wait())
                .await
                .expect("shutdown must finish or abort the tracked supervisor");
            if child_completes {
                assert_eq!(result.unwrap(), CleanupReport::Removed);
                assert!(!cache.exists());
            } else {
                assert!(result.unwrap_err().is_cancelled());
                assert!(cache.join("image.bin").exists());
            }
            assert!(!started.load(Ordering::SeqCst));
            assert!(tracker.registry.task_job_ids().is_empty());
            assert_eq!(
                tracker.get_job(cleanup_id.as_ref()).unwrap().state,
                JobState::Failed
            );
            assert_eq!(
                tracker.get_job(child_id.as_ref()).unwrap().state,
                if child_completes {
                    JobState::Completed
                } else {
                    JobState::Failed
                }
            );
        }
    }

    #[tokio::test]
    async fn shutdown_drains_task_owned_by_terminal_job() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let job_id = job.id().to_string();
        let dropped = Arc::new(AtomicBool::new(false));
        let worker_dropped = dropped.clone();
        tracker
            .spawn_job(job, move |job| async move {
                job.complete("Completed", "{}");
                let _dropped = Dropped(worker_dropped);
                std::future::pending::<()>().await;
            })
            .detach();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if tracker
                    .get_job(&job_id)
                    .is_some_and(|job| job.state == JobState::Completed)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let cancelled = tracker
            .shutdown_and_await_terminal(Duration::from_millis(100))
            .await;

        assert_eq!(cancelled, 0);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(tracker.registry.task_job_ids().is_empty());
        assert_eq!(tracker.get_job(&job_id).unwrap().state, JobState::Completed);
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
