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

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Error;
use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use super::domain::{JobDomain, JobFailure};
use super::handle::PendingJob;
use super::ids::JobId;

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

/// Input for creating a leaf job.
pub struct JobSpec {
    pub rack_id: String,
    pub node_id: String,
    pub queued_description: String,
    pub span: tracing::Span,
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
            queued_description: queued_description.into(),
            span,
        }
    }
}

/// Input for creating a parent job.
pub struct ParentJobSpec {
    pub rack_id: String,
    pub running_description: String,
    pub span: tracing::Span,
}

impl ParentJobSpec {
    /// Creates a parent job specification.
    pub fn new(
        rack_id: impl Into<String>,
        running_description: impl Into<String>,
        span: tracing::Span,
    ) -> Self {
        Self {
            rack_id: rack_id.into(),
            running_description: running_description.into(),
            span,
        }
    }
}

/// Snapshot returned to status adapters.
#[derive(Debug)]
pub struct JobSnapshot {
    pub job_id: JobId,
    pub span: tracing::Span,
    pub state: JobLifecycleState,
    pub rack_id: String,
    pub node_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub child_job_ids: Vec<JobId>,
    pub is_parent_job: bool,
    pub cancellation_token: CancellationToken,
}

impl Clone for JobSnapshot {
    fn clone(&self) -> Self {
        Self {
            job_id: self.job_id.clone(),
            span: self.span.clone(),
            state: self.state.clone(),
            rack_id: self.rack_id.clone(),
            node_id: self.node_id.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            child_job_ids: self.child_job_ids.clone(),
            is_parent_job: self.is_parent_job,
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

/// Default upper bound on retained job records when no explicit cap is given.
pub const MAX_TRACKED_JOBS: usize = 10_000;

/// Shared in-memory registry for asynchronous jobs.
///
/// Job records are stored domain-agnostically; the `D` parameter only selects
/// the [`JobDomain`] policy used to build supervisor and capacity failures, so
/// it is carried as a marker.
pub struct JobRegistry<D: JobDomain> {
    jobs: RwLock<HashMap<JobId, JobRecord>>,
    ttl: Duration,
    max_jobs: usize,
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
            jobs: RwLock::new(HashMap::new()),
            ttl,
            max_jobs,
            _domain: PhantomData,
        }
    }

    /// Creates a leaf job without checking for active work on the node.
    ///
    /// Returns the domain's capacity failure when the registry is full after
    /// pruning expired jobs.
    pub fn create_leaf(&self, spec: JobSpec) -> Result<PendingJob<D>, JobFailure> {
        self.cleanup_expired();

        let mut jobs = self.jobs.write().unwrap();
        if let Some(at_capacity) = self.capacity_check(&jobs) {
            return Err(at_capacity);
        }
        let record = JobRecord::new_leaf(spec);
        let job_id = record.job_id.clone();
        let span = record.span.clone();
        let cancellation_token = record.cancellation_token.clone();
        jobs.insert(job_id.clone(), record);

        Ok(PendingJob::new(job_id, span, cancellation_token))
    }

    /// Creates a leaf job only if no active leaf job exists for the same node
    /// and the registry has spare capacity.
    pub fn create_leaf_if_node_idle(&self, spec: JobSpec) -> Result<PendingJob<D>, JobFailure> {
        self.cleanup_expired();

        let mut jobs = self.jobs.write().unwrap();
        if let Some(active_job_id) = active_job_for_node(&jobs, &spec.rack_id, &spec.node_id) {
            return Err(D::node_busy_failure(
                &spec.rack_id,
                &spec.node_id,
                &active_job_id,
            ));
        }
        if let Some(at_capacity) = self.capacity_check(&jobs) {
            return Err(at_capacity);
        }

        let record = JobRecord::new_leaf(spec);
        let job_id = record.job_id.clone();
        let span = record.span.clone();
        let cancellation_token = record.cancellation_token.clone();
        jobs.insert(job_id.clone(), record);

        Ok(PendingJob::new(job_id, span, cancellation_token))
    }

    /// Creates a parent job aggregating the supplied child jobs.
    ///
    /// Returns `None` when the child list is empty or the registry is at
    /// capacity; in the latter case the children remain individually tracked.
    pub fn create_parent(&self, spec: ParentJobSpec, child_job_ids: &[JobId]) -> Option<JobId> {
        if child_job_ids.is_empty() {
            return None;
        }

        self.cleanup_expired();

        let mut jobs = self.jobs.write().unwrap();
        if let Some(at_capacity) = self.capacity_check(&jobs) {
            tracing::warn!(
                message = %at_capacity.message,
                "job tracker at capacity; skipping parent job aggregation"
            );
            return None;
        }
        let record = JobRecord::new_parent(spec, child_job_ids);
        let job_id = record.job_id.clone();
        jobs.insert(job_id.clone(), record);
        Some(job_id)
    }

    /// Returns the domain's capacity failure when the map is full, else `None`.
    /// Call while holding the write lock and after `cleanup_expired`, so the
    /// count reflects post-pruning occupancy.
    fn capacity_check(&self, jobs: &HashMap<JobId, JobRecord>) -> Option<JobFailure> {
        let current = jobs.len();
        if current >= self.max_jobs {
            Some(D::at_capacity_failure(current, self.max_jobs))
        } else {
            None
        }
    }

    /// Returns a point-in-time snapshot for a known job.
    pub fn get(&self, job_id: &JobId) -> Option<JobSnapshot> {
        self.jobs
            .read()
            .unwrap()
            .get(job_id)
            .map(JobRecord::snapshot)
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
        self.update(job_id, |record, now| {
            if record.state.is_terminal() {
                return false;
            }
            record.state = JobLifecycleState::Running {
                description: description.into(),
            };
            record.updated_at = now;
            true
        })
    }

    /// Marks a job completed.
    pub fn mark_completed(
        &self,
        job_id: &JobId,
        description: impl Into<String>,
        result_json: impl Into<String>,
    ) -> bool {
        self.update(job_id, |record, now| {
            if record.state.is_terminal() {
                return false;
            }
            record.state = JobLifecycleState::Completed {
                description: description.into(),
                result_json: result_json.into(),
            };
            record.updated_at = now;
            true
        })
    }

    /// Marks a non-terminal job failed.
    pub fn mark_failed(&self, job_id: &JobId, failure: JobFailure) -> bool {
        self.mark_failed_if(job_id, failure, |state| !state.is_terminal())
    }

    /// Marks an unsealed job as failed because the worker exited unexpectedly.
    pub fn mark_dropped_failed(&self, job_id: &JobId) -> bool {
        self.mark_failed_if_non_terminal(job_id, D::dropped_failure())
    }

    /// Marks an unsealed job as failed because the worker panicked.
    pub fn mark_panic_failed(&self, job_id: &JobId) -> bool {
        self.mark_failed_if_non_terminal(job_id, D::panic_failure())
    }

    // Used only after the tracked supervisor proves the failed state came from
    // the RAII drop path of a panicked worker, not from worker-owned failure.
    pub(crate) fn replace_unsealed_failure_with_panic(&self, job_id: &JobId) -> bool {
        self.mark_failed_unless_completed(job_id, D::panic_failure())
    }

    /// Marks an unsealed job as failed because the worker was aborted.
    pub fn mark_cancelled_failed(&self, job_id: &JobId) -> bool {
        self.mark_failed_if_non_terminal(job_id, D::cancelled_failure())
    }

    // Used only after the tracked supervisor proves the failed state came from
    // the RAII drop path of an aborted worker, not from worker-owned failure.
    pub(crate) fn replace_unsealed_failure_with_cancelled(&self, job_id: &JobId) -> bool {
        self.mark_failed_unless_completed(job_id, D::cancelled_failure())
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

    /// Signals cooperative cancellation for every active leaf job.
    pub fn shutdown_active_leaves(&self) -> usize {
        let tokens: Vec<CancellationToken> = self
            .jobs
            .read()
            .unwrap()
            .values()
            .filter(|job| !job.is_parent_job && !job.state.is_terminal())
            .map(|job| job.cancellation_token.clone())
            .collect();

        for token in &tokens {
            token.cancel();
        }

        tokens.len()
    }

    /// Waits until all supplied jobs are terminal.
    ///
    /// Missing job IDs are treated as done because callers pass IDs that were
    /// previously created, and records may have already been removed by TTL
    /// cleanup.
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
        let now = Utc::now();
        let mut jobs = self.jobs.write().unwrap();
        let protected_children = protected_children(&jobs, now, self.ttl);
        let expired = filter_job_ids(&jobs, |job_id, job| {
            let expired = job.state.is_terminal() && is_older_than(now, job.updated_at, self.ttl);
            let removable = !job.is_parent_job
                || !job
                    .child_job_ids
                    .iter()
                    .any(|child_job_id| jobs.contains_key(child_job_id));
            expired && !protected_children.contains(job_id) && removable
        });

        for job_id in expired {
            jobs.remove(&job_id);
        }
    }

    /// Test helper for timestamp-sensitive cleanup coverage.
    #[cfg(test)]
    pub fn age_job_for_test(&self, job_id: &JobId, age: Duration) {
        let delta = chrono::Duration::from_std(age).expect("test age fits in chrono::Duration");
        let mut jobs = self.jobs.write().unwrap();
        if let Some(job) = jobs.get_mut(job_id) {
            job.created_at -= delta;
            job.updated_at -= delta;
        }
    }

    fn mark_failed_if_non_terminal(&self, job_id: &JobId, failure: JobFailure) -> bool {
        self.mark_failed_if(job_id, failure, |state| !state.is_terminal())
    }

    fn mark_failed_unless_completed(&self, job_id: &JobId, failure: JobFailure) -> bool {
        self.mark_failed_if(job_id, failure, |state| {
            !matches!(state, JobLifecycleState::Completed { .. })
        })
    }

    fn mark_failed_if(
        &self,
        job_id: &JobId,
        failure: JobFailure,
        should_update: impl FnOnce(&JobLifecycleState) -> bool,
    ) -> bool {
        self.update(job_id, |record, now| {
            if !should_update(&record.state) {
                return false;
            }
            record.state = failed_state(failure);
            record.updated_at = now;
            true
        })
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

    fn update(
        &self,
        job_id: &JobId,
        f: impl FnOnce(&mut JobRecord, DateTime<Utc>) -> bool,
    ) -> bool {
        let mut jobs = self.jobs.write().unwrap();
        let Some(record) = jobs.get_mut(job_id) else {
            return false;
        };

        f(record, Utc::now())
    }
}

struct JobRecord {
    job_id: JobId,
    span: tracing::Span,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    cancellation_token: CancellationToken,
    state: JobLifecycleState,
    rack_id: String,
    node_id: String,
    child_job_ids: Vec<JobId>,
    is_parent_job: bool,
}

impl JobRecord {
    // Creates a new leaf (child) `JobRecord` with state `Queued`.
    fn new_leaf(spec: JobSpec) -> Self {
        Self::new(
            spec.rack_id,
            spec.node_id,
            JobLifecycleState::Queued {
                description: spec.queued_description,
            },
            spec.span,
            Vec::new(),
            false,
        )
    }

    // Creates a new parent `JobRecord` with state `Running`.
    fn new_parent(spec: ParentJobSpec, child_job_ids: &[JobId]) -> Self {
        Self::new(
            spec.rack_id,
            String::new(),
            JobLifecycleState::Running {
                description: spec.running_description,
            },
            spec.span,
            child_job_ids.to_vec(),
            true,
        )
    }

    // Creates a new `JobRecord` with the given parameters.
    fn new(
        rack_id: String,
        node_id: String,
        state: JobLifecycleState,
        span: tracing::Span,
        child_job_ids: Vec<JobId>,
        is_parent_job: bool,
    ) -> Self {
        let now = Utc::now();
        Self {
            job_id: JobId::new(),
            span,
            created_at: now,
            updated_at: now,
            cancellation_token: CancellationToken::new(),
            state,
            rack_id,
            node_id,
            child_job_ids,
            is_parent_job,
        }
    }

    fn snapshot(&self) -> JobSnapshot {
        JobSnapshot {
            job_id: self.job_id.clone(),
            span: self.span.clone(),
            state: self.state.clone(),
            rack_id: self.rack_id.clone(),
            node_id: self.node_id.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            child_job_ids: self.child_job_ids.clone(),
            is_parent_job: self.is_parent_job,
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

fn active_job_for_node(
    jobs: &HashMap<JobId, JobRecord>,
    rack_id: &str,
    node_id: &str,
) -> Option<JobId> {
    jobs.values()
        .find(|job| {
            !job.is_parent_job
                && job.rack_id == rack_id
                && job.node_id == node_id
                && !job.state.is_terminal()
        })
        .map(|job| job.job_id.clone())
}

fn failed_state(failure: JobFailure) -> JobLifecycleState {
    JobLifecycleState::Failed {
        description: failure.description,
        failure: Arc::new(failure.error),
        message: failure.message,
        result_json: failure.result_json.unwrap_or_default(),
    }
}

fn protected_children(
    jobs: &HashMap<JobId, JobRecord>,
    now: DateTime<Utc>,
    ttl: Duration,
) -> HashSet<JobId> {
    jobs.values()
        .filter(|job| job.is_parent_job)
        .filter(|job| {
            let expired = job.state.is_terminal() && is_older_than(now, job.updated_at, ttl);
            !expired
        })
        .flat_map(|job| job.child_job_ids.iter().cloned())
        .collect()
}

/// Returns true when `timestamp` is at least `ttl` in the past relative to
/// `now`. A non-positive elapsed interval (e.g. a backwards wall-clock step)
/// is treated as "not yet expired".
fn is_older_than(now: DateTime<Utc>, timestamp: DateTime<Utc>, ttl: Duration) -> bool {
    now.signed_duration_since(timestamp)
        .to_std()
        .map(|elapsed| elapsed >= ttl)
        .unwrap_or(false)
}

fn filter_job_ids(
    jobs: &HashMap<JobId, JobRecord>,
    predicate: impl Fn(&JobId, &JobRecord) -> bool,
) -> Vec<JobId> {
    jobs.iter()
        .filter(|(job_id, job)| predicate(job_id, job))
        .map(|(job_id, _)| job_id.clone())
        .collect()
}
