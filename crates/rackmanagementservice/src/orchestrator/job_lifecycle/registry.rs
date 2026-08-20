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

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Error;
use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use super::domain::{JobDomain, JobFailure};
use super::handle::JobHandle;
use super::ids::JobId;
use super::job_type::JobType;
use super::store::JobStore;

#[cfg(not(test))]
const TERMINAL_WAIT_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(test)]
const TERMINAL_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// State stored for a tracked job.
///
/// The `Failed` variant carries the failure cause as an `Arc<anyhow::Error>`
/// (shared so snapshots stay cheap to clone). Domains embed their typed error
/// code as the root cause and recover it via `error.downcast_ref`.
#[derive(Debug, Clone)]
pub enum JobLifecycleState {
    Queued {
        description: String,
    },
    Running {
        description: String,
    },
    Completed {
        description: String,
        result_json: String,
    },
    Failed {
        description: String,
        failure: Arc<Error>,
        message: String,
        result_json: String,
    },
}

impl JobLifecycleState {
    /// Returns true when the state is terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. })
    }

    /// Returns the user-visible state description.
    pub fn description(&self) -> &str {
        match self {
            Self::Queued { description }
            | Self::Running { description }
            | Self::Completed { description, .. }
            | Self::Failed { description, .. } => description,
        }
    }
}

/// Input for creating a job.
pub struct JobSpec {
    pub rack_id: String,
    pub node_id: String,
    pub span: tracing::Span,
    /// When set, this job is an externally visible async workflow (returned to API callers).
    pub workflow_type: Option<JobType>,
    pub initial_state: JobLifecycleState,
}

impl JobSpec {
    /// Creates a leaf job specification.
    pub fn new(
        rack_id: impl Into<String>,
        node_id: impl Into<String>,
        queued_description: impl Into<String>,
        span: tracing::Span,
    ) -> Self {
        Self {
            rack_id: rack_id.into(),
            node_id: node_id.into(),
            span,
            workflow_type: None,
            initial_state: JobLifecycleState::Queued {
                description: queued_description.into(),
            },
        }
    }

    /// Creates a running top-level job specification.
    pub fn new_parent(
        rack_id: impl Into<String>,
        running_description: impl Into<String>,
        span: tracing::Span,
    ) -> Self {
        Self {
            rack_id: rack_id.into(),
            node_id: String::new(),
            span,
            workflow_type: None,
            initial_state: JobLifecycleState::Running {
                description: running_description.into(),
            },
        }
    }

    /// Marks this job as an externally visible workflow for metrics.
    pub fn with_workflow_type(mut self, workflow_type: JobType) -> Self {
        self.workflow_type = Some(workflow_type);
        self
    }
}

#[cfg(test)]
pub struct ParentJobSpec(JobSpec);

#[cfg(test)]
impl ParentJobSpec {
    pub fn new(
        rack_id: impl Into<String>,
        running_description: impl Into<String>,
        span: tracing::Span,
    ) -> Self {
        Self(JobSpec::new_parent(rack_id, running_description, span))
    }

    pub fn with_workflow_type(mut self, workflow_type: JobType) -> Self {
        self.0 = self.0.with_workflow_type(workflow_type);
        self
    }
}

/// Point-in-time job record returned to status adapters.
#[derive(Debug, Clone)]
pub struct JobSnapshot {
    pub job_id: JobId,
    pub span: tracing::Span,
    pub state: JobLifecycleState,
    pub rack_id: String,
    pub node_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub child_job_ids: Vec<JobId>,
    pub parent_job_id: Option<JobId>,
    pub cancellation_token: CancellationToken,
    /// Workflow type for metrics when this job is externally visible to API callers.
    pub workflow_type: Option<JobType>,
}

impl JobSnapshot {
    /// Returns `true` when this job aggregates child jobs (a parent job).
    pub fn is_parent(&self) -> bool {
        !self.child_job_ids.is_empty()
    }

    fn new(spec: JobSpec, parent_job_id: Option<JobId>) -> Self {
        let now = Utc::now();
        Self {
            job_id: JobId::new(),
            span: spec.span,
            created_at: now,
            updated_at: now,
            cancellation_token: CancellationToken::new(),
            state: spec.initial_state,
            rack_id: spec.rack_id,
            node_id: spec.node_id,
            child_job_ids: Vec::new(),
            parent_job_id,
            workflow_type: spec.workflow_type,
        }
    }
}

/// Default upper bound on retained job records when no explicit cap is given.
pub const MAX_TRACKED_JOBS: usize = 10_000;

/// Callback invoked with a job snapshot on a lifecycle transition.
///
/// Used for both the creation and terminal-transition hooks (see
/// [`JobRegistry::set_created_observer`] and
/// [`JobRegistry::set_terminal_observer`]).
pub type JobObserver = Arc<dyn Fn(&JobSnapshot) + Send + Sync>;

/// Shared in-memory registry for asynchronous jobs.
///
/// Job records are stored domain-agnostically; the `D` parameter only selects
/// the [`JobDomain`] policy used to build supervisor and capacity failures, so
/// it is carried as a marker.
pub struct JobRegistry<D: JobDomain> {
    /// Job records and derived indexes guarded by the same lock.
    jobs: RwLock<JobStore>,
    ttl: Duration,
    max_jobs: usize,
    shutting_down: AtomicBool,
    /// Fired after a newly created job is inserted and the write lock is
    /// released. Running outside the lock prevents a panic in the callback from
    /// poisoning the registry. Must not re-enter the registry.
    created_observer: Option<JobObserver>,
    terminal_observer: Option<JobObserver>,
    _domain: PhantomData<fn() -> D>,
}

impl<D: JobDomain> JobRegistry<D> {
    /// Creates a registry retaining terminal jobs for `ttl`, bounded by the
    /// default [`MAX_TRACKED_JOBS`] cap.
    pub fn new(ttl: Duration) -> Self {
        Self::with_capacity(ttl, MAX_TRACKED_JOBS)
    }

    /// Creates a registry retaining terminal jobs for `ttl` and admitting at
    /// most `max_jobs` concurrent records. Lets callers (and tests) inject a
    /// non-default cap.
    pub fn with_capacity(ttl: Duration, max_jobs: usize) -> Self {
        Self {
            jobs: RwLock::new(JobStore::default()),
            ttl,
            max_jobs,
            shutting_down: AtomicBool::new(false),
            created_observer: None,
            terminal_observer: None,
            _domain: PhantomData,
        }
    }

    /// Registers a callback fired once per job after it is inserted and the
    /// write lock is released. The callback must not re-enter the registry.
    /// Intended to be configured once at construction, before the registry is
    /// shared.
    pub fn set_created_observer(&mut self, observer: JobObserver) {
        self.created_observer = Some(observer);
    }

    /// Registers a callback invoked exactly once per job the first time it
    /// enters a terminal state. Intended to be configured once at construction,
    /// before the registry is shared.
    pub fn set_terminal_observer(&mut self, observer: JobObserver) {
        self.terminal_observer = Some(observer);
    }

    /// Fires the creation observer, if configured. Called after the write lock
    /// is released so a panic in the callback cannot poison the registry lock.
    fn notify_created(&self, snapshot: &JobSnapshot) {
        if let Some(observer) = &self.created_observer {
            observer(snapshot);
        }
    }

    /// Creates a job with an optional immutable parent relationship.
    pub fn create_job(
        self: &Arc<Self>,
        spec: JobSpec,
        parent_job_id: Option<&JobId>,
        require_node_idle: bool,
    ) -> Result<JobHandle<D>, JobFailure> {
        let (job_id, span, cancellation_token, record) = {
            let mut jobs = self.jobs.write().unwrap();
            if self.shutting_down.load(Ordering::SeqCst) {
                return Err(D::shutting_down_failure());
            }
            if let Some(parent_job_id) = parent_job_id
                && !jobs.can_accept_child(parent_job_id)
            {
                return Err(D::parent_unavailable_failure(parent_job_id));
            }
            if require_node_idle
                && let Some(active_job_id) = jobs.active_job_for_node(&spec.rack_id, &spec.node_id)
            {
                return Err(D::node_busy_failure(
                    &spec.rack_id,
                    &spec.node_id,
                    &active_job_id,
                ));
            }
            if let Some(at_capacity) = self.capacity_check(&mut jobs) {
                return Err(at_capacity);
            }

            let record = JobSnapshot::new(spec, parent_job_id.cloned());
            let record = jobs.insert(record);
            if parent_job_id.is_some() {
                assert!(
                    jobs.attach_child(&record),
                    "validated parent must accept child under the same write lock"
                );
            }
            let job_id = record.job_id.clone();
            let span = record.span.clone();
            let cancellation_token = record.cancellation_token.clone();
            (job_id, span, cancellation_token, record)
        };
        let handle = JobHandle::new(Arc::clone(self), job_id, span, cancellation_token);
        self.notify_created(&record);

        Ok(handle)
    }

    #[cfg(test)]
    pub(crate) fn create_leaf(self: &Arc<Self>, spec: JobSpec) -> Result<JobHandle<D>, JobFailure> {
        self.create_job(spec, None, false)
    }

    #[cfg(test)]
    pub(crate) fn create_leaf_if_node_idle(
        self: &Arc<Self>,
        spec: JobSpec,
    ) -> Result<JobHandle<D>, JobFailure> {
        self.create_job(spec, None, true)
    }

    #[cfg(test)]
    pub(crate) fn create_parent(self: &Arc<Self>, spec: ParentJobSpec) -> Option<JobHandle<D>> {
        self.create_job(spec.0, None, false).ok()
    }

    #[cfg(test)]
    pub(crate) fn create_child(
        self: &Arc<Self>,
        parent_job_id: &JobId,
        spec: JobSpec,
    ) -> Result<JobHandle<D>, JobFailure> {
        self.create_job(spec, Some(parent_job_id), false)
    }

    /// Prunes expired jobs under admission pressure, then returns the domain's
    /// capacity failure when the store remains full.
    ///
    /// Call while holding the store write lock.
    fn capacity_check(&self, jobs: &mut JobStore) -> Option<JobFailure> {
        if jobs.tracked_job_count() >= self.max_jobs {
            jobs.cleanup_expired(Utc::now(), self.ttl);
        }

        let current = jobs.tracked_job_count();
        if current >= self.max_jobs {
            Some(D::at_capacity_failure(current, self.max_jobs))
        } else {
            None
        }
    }

    /// Returns a point-in-time snapshot for a known job.
    pub fn get(&self, job_id: &JobId) -> Option<JobSnapshot> {
        self.jobs.read().unwrap().get(job_id).cloned()
    }

    /// Returns the parent job ID for a known child job, if one was linked.
    pub fn parent_for_job(&self, job_id: &JobId) -> Option<JobId> {
        self.jobs
            .read()
            .unwrap()
            .get(job_id)
            .and_then(|job| job.parent_job_id.clone())
    }

    /// Returns the IDs of every parent job currently tracked.
    pub fn parent_job_ids(&self) -> Vec<JobId> {
        self.jobs
            .read()
            .unwrap()
            .snapshots()
            .filter(|job| job.is_parent())
            .map(|job| job.job_id.clone())
            .collect()
    }

    /// Returns the tracing span for a known job.
    pub fn span_for_job(&self, job_id: &JobId) -> Option<tracing::Span> {
        self.jobs
            .read()
            .unwrap()
            .get(job_id)
            .map(|job| job.span.clone())
    }

    /// Marks a job running with a new description.
    pub fn mark_running(&self, job_id: &JobId, description: impl Into<String>) -> bool {
        self.update(job_id, |state| {
            if state.is_terminal() {
                return None;
            }

            Some(JobLifecycleState::Running {
                description: description.into(),
            })
        })
    }

    /// Marks a job completed on behalf of its owned [`JobHandle`].
    pub(super) fn mark_completed(
        &self,
        job_id: &JobId,
        description: impl Into<String>,
        result_json: impl Into<String>,
    ) -> bool {
        self.update(job_id, |state| {
            if state.is_terminal() {
                tracing::error!(
                    "Job was terminal before mark_completed or mark_failed had been called"
                );
                return None;
            }

            Some(JobLifecycleState::Completed {
                description: description.into(),
                result_json: result_json.into(),
            })
        })
    }

    /// Marks a non-terminal job failed on behalf of its owned [`JobHandle`].
    pub(super) fn mark_failed(&self, job_id: &JobId, failure: JobFailure) -> bool {
        self.update(job_id, |state| {
            if state.is_terminal() {
                tracing::error!("Job was terminal before mark_failed had been called");
                return None;
            }

            Some(failed_state(failure))
        })
    }

    /// Removes an unstarted leaf job during caller-side pre-spawn rollback.
    ///
    /// Only queued leaf jobs are eligible. Running, terminal, and parent jobs
    /// remain tracked so callers cannot erase observable job history. A child
    /// already linked to a parent is also retained: removing it would leave the
    /// parent's `child_job_ids` referencing an absent record, which
    /// `collect_parent_counts` treats as missing and would fail the parent.
    pub fn remove_queued_leaf(&self, job_id: &JobId) -> bool {
        self.jobs.write().unwrap().remove_queued_leaf(job_id)
    }

    /// Returns a cancellation token for a known job.
    pub fn cancellation_token_for(&self, job_id: &JobId) -> Option<CancellationToken> {
        self.jobs
            .read()
            .unwrap()
            .get(job_id)
            .map(|job| job.cancellation_token.clone())
    }

    /// Signals cooperative cancellation for a single known job.
    pub fn cancel(&self, job_id: &JobId) -> bool {
        let Some(token) = self.cancellation_token_for(job_id) else {
            return false;
        };

        token.cancel();
        true
    }

    /// Marks the registry as shutting down: subsequent leaf creates are
    /// refused with the domain's shutting-down failure. Irreversible.
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    /// Signals cooperative cancellation for every active leaf job and returns
    /// their IDs.
    pub fn cancel_active_leaves(&self) -> Vec<JobId> {
        let active: Vec<(JobId, CancellationToken)> = self
            .jobs
            .read()
            .unwrap()
            .snapshots()
            .filter(|job| !job.is_parent() && !job.state.is_terminal())
            .map(|job| (job.job_id.clone(), job.cancellation_token.clone()))
            .collect();

        for (_, token) in &active {
            token.cancel();
        }

        active.into_iter().map(|(job_id, _)| job_id).collect()
    }

    // Note: missing job IDs are treated as already-done (because TTL cleanup may have evicted records between the call to cancel_active_leaves and the wait)
    pub async fn wait_for_terminal_jobs(
        &self,
        job_ids: &[JobId],
        timeout: Option<Duration>,
    ) -> bool {
        let mut remaining: HashSet<JobId> = job_ids.iter().cloned().collect();
        let wait = async move {
            while !remaining.is_empty() {
                self.retain_non_terminal_or_missing(&mut remaining);
                if remaining.is_empty() {
                    break;
                }

                tokio::time::sleep(TERMINAL_WAIT_POLL_INTERVAL).await;
            }
        };

        if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, wait).await.is_ok()
        } else {
            wait.await;
            true
        }
    }

    /// Evicts expired terminal jobs while preserving children of live parents.
    pub fn cleanup_expired(&self) {
        let mut jobs = self.jobs.write().unwrap();
        jobs.cleanup_expired(Utc::now(), self.ttl);
    }

    /// Test helper for timestamp-sensitive cleanup coverage.
    #[cfg(test)]
    pub fn age_job_for_test(&self, job_id: &JobId, age: Duration) {
        let mut jobs = self.jobs.write().unwrap();
        jobs.age_job_for_test(job_id, age);
    }

    /// Asserts that authoritative records and derived indexes are consistent.
    #[cfg(test)]
    pub(super) fn assert_store_consistent_for_test(&self) {
        self.jobs.read().unwrap().assert_consistent();
    }

    fn retain_non_terminal_or_missing(&self, remaining: &mut HashSet<JobId>) {
        let jobs = self.jobs.read().unwrap();
        remaining.retain(|job_id| match jobs.get(job_id) {
            Some(job) => !job.state.is_terminal(),
            // Waiters receive IDs for jobs that were already created. Missing
            // records were terminal long enough to be collected by TTL cleanup.
            None => false,
        });
    }

    /// Applies a lifecycle-state transition and notifies the terminal observer.
    fn update(
        &self,
        job_id: &JobId,
        transition: impl FnOnce(&JobLifecycleState) -> Option<JobLifecycleState>,
    ) -> bool {
        let (changed, terminal_snapshot) = {
            let mut jobs = self.jobs.write().unwrap();
            jobs.transition_state(job_id, transition)
        };

        if let (Some(observer), Some(snapshot)) = (&self.terminal_observer, terminal_snapshot) {
            observer(&snapshot);
        }

        changed
    }
}

fn failed_state(failure: JobFailure) -> JobLifecycleState {
    JobLifecycleState::Failed {
        description: failure.description,
        failure: Arc::new(failure.error),
        message: failure.message,
        result_json: failure.result_json.unwrap_or_default(),
    }
}
