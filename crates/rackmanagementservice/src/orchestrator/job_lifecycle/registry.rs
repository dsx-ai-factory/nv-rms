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
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use super::domain::{JobDomain, JobFailure};
use super::error::job_error_of;
use super::handle::JobHandle;
use super::ids::JobId;
use super::job_type::JobType;
use super::store::JobStore;

use anyhow::Error;
use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use opentelemetry::trace::{Status, TraceContextExt};
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
    span: JobSpanSource,
    /// When set, this job is an externally visible async workflow (returned to API callers).
    pub workflow_type: Option<JobType>,
    pub initial_state: JobLifecycleState,
}

enum JobSpanSource {
    Ready(tracing::Span),
    Deferred(Box<dyn FnOnce(&JobId) -> tracing::Span + Send + 'static>),
}

impl JobSpanSource {
    fn resolve(self, job_id: &JobId) -> tracing::Span {
        match self {
            Self::Ready(span) => span,
            Self::Deferred(create) => create(job_id),
        }
    }
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
            span: JobSpanSource::Ready(span),
            workflow_type: None,
            initial_state: JobLifecycleState::Queued {
                description: queued_description.into(),
            },
        }
    }

    /// Creates a leaf specification whose span is built only after admission.
    pub fn new_deferred(
        rack_id: impl Into<String>,
        node_id: impl Into<String>,
        queued_description: impl Into<String>,
        create_span: impl FnOnce(&JobId) -> tracing::Span + Send + 'static,
    ) -> Self {
        Self {
            rack_id: rack_id.into(),
            node_id: node_id.into(),
            span: JobSpanSource::Deferred(Box::new(create_span)),
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
            span: JobSpanSource::Ready(span),
            workflow_type: None,
            initial_state: JobLifecycleState::Running {
                description: running_description.into(),
            },
        }
    }

    /// Creates a parent specification whose span is built only after admission.
    pub fn new_parent_deferred(
        rack_id: impl Into<String>,
        running_description: impl Into<String>,
        create_span: impl FnOnce(&JobId) -> tracing::Span + Send + 'static,
    ) -> Self {
        Self {
            rack_id: rack_id.into(),
            node_id: String::new(),
            span: JobSpanSource::Deferred(Box::new(create_span)),
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
    /// Monotonic last-touch used for TTL eviction. Wall-clock `updated_at` is
    /// for display only and can jump under NTP steps.
    pub(crate) updated_at_mono: Instant,
    /// Set after the first inverted-clock warn on a terminal job so a stuck
    /// stamp does not log on every reaper sweep.
    pub(crate) mono_inversion_warned: bool,
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
        let job_id = JobId::new();
        let span = spec.span.resolve(&job_id);
        span.record("job_id", job_id.as_ref());
        Self {
            job_id,
            span,
            created_at: now,
            updated_at: now,
            updated_at_mono: Instant::now(),
            mono_inversion_warned: false,
            cancellation_token: CancellationToken::new(),
            state: spec.initial_state,
            rack_id: spec.rack_id,
            node_id: spec.node_id,
            child_job_ids: Vec::new(),
            parent_job_id,
            workflow_type: spec.workflow_type,
        }
    }

    /// Records a lifecycle touch on both the display wall-clock and the
    /// monotonic TTL clock.
    pub(super) fn touch(&mut self) {
        self.updated_at = Utc::now();
        self.updated_at_mono = Instant::now();
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

/// Classification of a state change applied by [`JobStore::transition_state`].
///
/// Used only to select which chokepoint log line [`JobRegistry::update`]
/// emits; each variant carries the post-transition snapshot.
pub(super) enum LifecycleTransition {
    /// The first `Queued -> Running` move: the workflow has actually started.
    Started(JobSnapshot),

    /// A later `Running -> Running` description update (progress reporting).
    Progress(JobSnapshot),

    /// The first non-terminal to terminal move (`Completed` or `Failed`).
    Terminal(JobSnapshot),
}

/// An identity alias that must remain exclusive while a mutation is active.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub enum JobScope {
    /// A registered node alias, also checked against the active-node index.
    LogicalNode { rack_id: String, node_id: String },

    /// A switch tray within a chassis; the serial number identifies the chassis.
    PhysicalSwitch {
        chassis_serial_number: String,
        tray_index: u32,
    },

    /// A management socket whose host spelling is normalized during admission.
    Endpoint { host: String, port: u16 },
}

impl JobScope {
    /// Normalizes IP spelling and DNS case before indexing an endpoint alias.
    ///
    /// IPv4-mapped IPv6 addresses share the IPv4 key. Scoped IPv6 addresses
    /// retain their zone ID verbatim so distinct interfaces remain distinct.
    pub fn endpoint(host: impl AsRef<str>, port: u16) -> Self {
        let host = host
            .as_ref()
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']');

        let host = if let Some((address, zone)) = host.split_once('%')
            && let Ok(address) = address.parse::<Ipv6Addr>()
        {
            format!("{address}%{zone}")
        } else {
            host.parse::<IpAddr>().map_or_else(
                |_| host.trim_end_matches('.').to_ascii_lowercase(),
                |address| address.to_canonical().to_string(),
            )
        };

        Self::Endpoint { host, port }
    }

    pub(crate) fn normalized(self) -> Self {
        match self {
            Self::Endpoint { host, port } => Self::endpoint(host, port),
            Self::PhysicalSwitch {
                chassis_serial_number,
                tray_index,
            } => Self::PhysicalSwitch {
                chassis_serial_number: chassis_serial_number.trim().to_owned(),
                tray_index,
            },
            scope => scope,
        }
    }
}

/// Holds all direct-mutation aliases until the caller finishes or drops the guard.
#[must_use = "hold the reservation for the complete device mutation"]
pub struct MutationReservation<D: JobDomain> {
    registry: Arc<JobRegistry<D>>,
    owner_id: JobId,
}

impl<D: JobDomain> Drop for MutationReservation<D> {
    fn drop(&mut self) {
        self.registry
            .jobs
            .write()
            .unwrap()
            .release_scopes(&self.owner_id);
    }
}

/// Shared in-memory registry for asynchronous jobs.
///
/// The domain selects failure policy. Job state and mutation aliases share
/// one admission and transition lock.
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
    tasks: Mutex<HashMap<JobId, JobTaskControl>>,
    task_change: Notify,
    _domain: PhantomData<fn() -> D>,
}

struct JobTaskControl {
    worker: AbortHandle,
    supervisor: AbortHandle,
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
            tasks: Mutex::new(HashMap::new()),
            task_change: Notify::new(),
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

    /// Logs the job-created chokepoint event, then fires the creation
    /// observer, if configured. Called after the write lock is released so a
    /// panic in the callback cannot poison the registry lock.
    fn notify_created(&self, snapshot: &JobSnapshot) {
        log_created(snapshot);
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
        self.create_job_with_scopes(spec, parent_job_id, require_node_idle, Vec::new())
    }

    /// Atomically admits every supplied alias before publishing a job record.
    ///
    /// Scope overlap, node occupancy, shutdown, and capacity leave no reservation.
    /// The owned job handle releases aliases only after a terminal transition.
    pub fn create_job_with_scopes(
        self: &Arc<Self>,
        spec: JobSpec,
        parent_job_id: Option<&JobId>,
        require_node_idle: bool,
        scopes: Vec<JobScope>,
    ) -> Result<JobHandle<D>, JobFailure> {
        let mut scopes: Vec<_> = scopes.into_iter().map(JobScope::normalized).collect();

        if require_node_idle && !spec.node_id.is_empty() {
            scopes.push(JobScope::LogicalNode {
                rack_id: spec.rack_id.clone(),
                node_id: spec.node_id.clone(),
            });
        }

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

            if let Some((scope, owner_id)) = jobs.conflicting_scope(&scopes) {
                return Err(D::scope_busy_failure(&scope, &owner_id));
            }

            if let Some(at_capacity) = self.capacity_check(&mut jobs) {
                return Err(at_capacity);
            }

            let record = JobSnapshot::new(spec, parent_job_id.cloned());
            let record = jobs.insert(record);

            jobs.reserve_scopes(record.job_id.clone(), scopes);

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

    /// Reserves all aliases for a synchronous mutation under the admission lock.
    ///
    /// The guard shares exclusion with jobs without consuming a retained-job slot.
    pub fn reserve_mutation(
        self: &Arc<Self>,
        scopes: Vec<JobScope>,
    ) -> Result<MutationReservation<D>, JobFailure> {
        let scopes: Vec<_> = scopes.into_iter().map(JobScope::normalized).collect();

        let owner_id = {
            let mut jobs = self.jobs.write().unwrap();

            if self.shutting_down.load(Ordering::SeqCst) {
                return Err(D::shutting_down_failure());
            }

            if let Some((scope, owner_id)) = jobs.conflicting_scope(&scopes) {
                return Err(D::scope_busy_failure(&scope, &owner_id));
            }

            let mut owner_id = JobId::new();

            while jobs.has_scope_owner(&owner_id) || jobs.get(&owner_id).is_some() {
                owner_id = JobId::new();
            }

            jobs.reserve_scopes(owner_id.clone(), scopes);
            owner_id
        };

        Ok(MutationReservation {
            registry: Arc::clone(self),
            owner_id,
        })
    }

    /// Adds every alias to an active worker before the worker starts device mutation.
    ///
    /// Existing aliases owned by this job are accepted. Any other occupancy,
    /// terminal state, or shutdown leaves the job's prior aliases unchanged.
    pub fn reserve_job_scopes(
        &self,
        job_id: &JobId,
        scopes: Vec<JobScope>,
    ) -> Result<(), JobFailure> {
        let scopes: Vec<_> = scopes.into_iter().map(JobScope::normalized).collect();
        let mut jobs = self.jobs.write().unwrap();

        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(D::shutting_down_failure());
        }

        if jobs.get(job_id).is_none_or(|job| job.state.is_terminal()) {
            return Err(D::inactive_job_failure(job_id));
        }

        if let Some((scope, owner_id)) = jobs.conflicting_scope_except(&scopes, Some(job_id)) {
            return Err(D::scope_busy_failure(&scope, &owner_id));
        }

        jobs.reserve_scopes(job_id.clone(), scopes);
        Ok(())
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
            jobs.cleanup_expired(Instant::now(), self.ttl);
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
        self.update(job_id, false, |state| {
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
        verified_completion: bool,
    ) -> bool {
        self.update(job_id, verified_completion, |state| {
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
        self.update(job_id, false, |state| {
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

    pub(super) fn register_task(
        &self,
        job_id: JobId,
        worker: AbortHandle,
        supervisor: AbortHandle,
    ) -> bool {
        // Cleanup remains tracked even when shutdown rejects starting the worker.
        self.tasks
            .lock()
            .unwrap()
            .insert(job_id, JobTaskControl { worker, supervisor });
        !self.shutting_down.load(Ordering::SeqCst)
    }

    pub(super) fn remove_task(&self, job_id: &JobId) {
        self.tasks.lock().unwrap().remove(job_id);
        self.task_change.notify_waiters();
    }

    pub(crate) fn abort_workers(&self, job_ids: &[JobId]) {
        let tasks = self.tasks.lock().unwrap();
        for job_id in job_ids {
            if let Some(task) = tasks.get(job_id) {
                task.worker.abort();
            }
        }
    }

    pub(crate) fn task_job_ids(&self) -> Vec<JobId> {
        self.tasks.lock().unwrap().keys().cloned().collect()
    }

    pub(crate) fn abort_supervisors(&self, job_ids: &[JobId]) {
        let tasks = self.tasks.lock().unwrap();
        for job_id in job_ids {
            if let Some(task) = tasks.get(job_id) {
                task.supervisor.abort();
            }
        }
    }

    pub(crate) async fn wait_for_tasks(&self, job_ids: &[JobId], timeout: Duration) -> bool {
        let job_ids: HashSet<_> = job_ids.iter().cloned().collect();
        let wait = async {
            loop {
                let changed = self.task_change.notified();
                if !self
                    .tasks
                    .lock()
                    .unwrap()
                    .keys()
                    .any(|job_id| job_ids.contains(job_id))
                {
                    return;
                }
                changed.await;
            }
        };
        tokio::time::timeout(timeout, wait).await.is_ok()
    }

    pub(crate) async fn wait_for_workers(&self, job_ids: &[JobId], timeout: Duration) -> bool {
        let job_ids: HashSet<_> = job_ids.iter().cloned().collect();
        let wait = async {
            loop {
                if self.workers_finished(&job_ids) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::time::timeout(timeout, wait).await.is_ok()
    }

    fn workers_finished(&self, job_ids: &HashSet<JobId>) -> bool {
        self.tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|(job_id, _)| job_ids.contains(*job_id))
            .all(|(_, task)| task.worker.is_finished())
    }

    pub(crate) fn all_workers_finished(&self, job_ids: &[JobId]) -> bool {
        self.workers_finished(&job_ids.iter().cloned().collect())
    }

    pub(crate) fn non_terminal_job_ids(&self) -> Vec<JobId> {
        self.jobs
            .read()
            .unwrap()
            .snapshots()
            .filter(|job| !job.state.is_terminal())
            .map(|job| job.job_id.clone())
            .collect()
    }

    pub(crate) fn fail_non_terminal_for_shutdown(&self, job_ids: &[JobId]) {
        for job_id in job_ids {
            self.mark_failed(job_id, D::shutdown_failure());
        }
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

    /// Wait for all known jobs to become terminal. Missing/TTL-evicted records
    /// count as settled, but never as successful outcomes.
    pub async fn wait_for_terminal_jobs(
        &self,
        job_ids: &[JobId],
        timeout: Option<Duration>,
    ) -> bool {
        self.wait_for_terminal_outcomes(job_ids, timeout)
            .await
            .pending
            .is_empty()
    }

    /// Preserve terminal snapshots as they arrive, before TTL cleanup can evict
    /// them. Timeout returns partial outcomes without cancelling hardware work.
    pub(crate) async fn wait_for_terminal_outcomes(
        &self,
        job_ids: &[JobId],
        timeout: Option<Duration>,
    ) -> TerminalJobOutcomes {
        let mut outcomes = TerminalJobOutcomes {
            pending: job_ids.iter().cloned().collect(),
            ..TerminalJobOutcomes::default()
        };
        let wait = async {
            while !outcomes.pending.is_empty() {
                outcomes.pending.retain(|id| match self.get(id) {
                    Some(snapshot) if snapshot.state.is_terminal() => {
                        outcomes.terminal.push(snapshot);
                        false
                    }
                    Some(_) => true,
                    None => {
                        outcomes.missing.push(id.clone());
                        false
                    }
                });
                if !outcomes.pending.is_empty() {
                    tokio::time::sleep(TERMINAL_WAIT_POLL_INTERVAL).await;
                }
            }
        };
        if let Some(timeout) = timeout {
            // Partial snapshots remain owned by outcomes when the wait is dropped.
            tokio::time::timeout(timeout, wait).await.ok();
        } else {
            wait.await;
        }
        outcomes
    }

    /// Evicts expired terminal jobs while preserving children of live parents.
    pub fn cleanup_expired(&self) {
        let mut jobs = self.jobs.write().unwrap();
        jobs.cleanup_expired(Instant::now(), self.ttl);
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

    /// Applies a lifecycle-state transition, logs the resulting chokepoint
    /// event (`job started`/`job progress`/`job completed`/`job failed`), and
    /// notifies the terminal observer.
    fn update(
        &self,
        job_id: &JobId,
        verified_completion: bool,
        transition: impl FnOnce(&JobLifecycleState) -> Option<JobLifecycleState>,
    ) -> bool {
        let (changed, event) = {
            let mut jobs = self.jobs.write().unwrap();
            jobs.transition_state(job_id, transition)
        };

        match event {
            Some(LifecycleTransition::Started(snapshot)) => log_started(&snapshot),
            Some(LifecycleTransition::Progress(snapshot)) => log_progress(&snapshot),
            Some(LifecycleTransition::Terminal(snapshot)) => {
                log_terminal(&snapshot, verified_completion);
                if let Some(observer) = &self.terminal_observer {
                    observer(&snapshot);
                }
            }
            None => {}
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

/// Logs the create chokepoint event for every `JobRegistry::create_job` caller
/// with a uniform field set (`parent_job_id` is `"None"` when absent).
///
/// Enters the job's own span first: the caller (e.g. an API handler) hasn't
/// necessarily entered it yet, and doing so here lets this event carry the
/// job's span ID rather than whatever span happened to be ambient at the call
/// site.
fn log_created(snapshot: &JobSnapshot) {
    let _entered = snapshot.span.enter();
    let description = snapshot.state.description();
    let parent_job_id = snapshot
        .parent_job_id
        .as_ref()
        .map(|id| id.to_string())
        .unwrap_or_else(|| "None".to_string());
    tracing::info!(
        job_id = %snapshot.job_id,
        parent_job_id = %parent_job_id,
        rack_id = %snapshot.rack_id,
        node_id = %snapshot.node_id,
        description,
        "job created"
    );
}

/// Logs the first `Queued -> Running` transition: the workflow has started.
///
/// Enters the job's own span first so the event carries the job's span ID
/// even when `update` is driven from a context that hasn't entered it (e.g.
/// the tracker-compatibility path in `job_tracker.rs`).
fn log_started(snapshot: &JobSnapshot) {
    let _entered = snapshot.span.enter();
    tracing::info!(
        job_id = %snapshot.job_id,
        rack_id = %snapshot.rack_id,
        node_id = %snapshot.node_id,
        description = snapshot.state.description(),
        "job started"
    );
}

/// Logs a later `Running -> Running` description update.
///
/// Enters the job's own span first so the event carries the job's span ID
/// even when `update` is driven from a context that hasn't entered it.
fn log_progress(snapshot: &JobSnapshot) {
    let _entered = snapshot.span.enter();
    tracing::info!(
        job_id = %snapshot.job_id,
        rack_id = %snapshot.rack_id,
        node_id = %snapshot.node_id,
        description = snapshot.state.description(),
        "job progress"
    );
}

/// Logs the first non-terminal to terminal transition. Failed jobs log
/// `message` (the raw failure detail) rather than `description`, which
/// defaults to the generic placeholder `"Failed"` unless a caller opts in to
/// [`JobFailure::with_description`].
///
/// Enters the job's own span first so the event carries the job's span ID
/// even when `update` is driven from a context that hasn't entered it (e.g.
/// the tracker-compatibility path in `job_tracker.rs`, which drives a
/// previously created `JobHandle` from an unrelated call site).
fn log_terminal(snapshot: &JobSnapshot, verified_completion: bool) {
    let _entered = snapshot.span.enter();
    match &snapshot.state {
        JobLifecycleState::Completed { description, .. } => tracing::info!(
            job_id = %snapshot.job_id,
            rack_id = %snapshot.rack_id,
            node_id = %snapshot.node_id,
            description,
            "job completed"
        ),
        JobLifecycleState::Failed { message, .. } => tracing::info!(
            job_id = %snapshot.job_id,
            rack_id = %snapshot.rack_id,
            node_id = %snapshot.node_id,
            description = message,
            "job failed"
        ),
        JobLifecycleState::Queued { .. } | JobLifecycleState::Running { .. } => {}
    }

    // The registry retains snapshots for status queries and TTL cleanup, so
    // dropping the tracing::Span would otherwise delay OTLP export for hours.
    // End only the OpenTelemetry span after the terminal event is recorded;
    // the tracing span remains available for retained log metadata.
    let context = snapshot.span.context();
    let span = context.span();
    if let JobLifecycleState::Failed { failure, .. } = &snapshot.state {
        span.set_attribute(KeyValue::new(
            "error.type",
            job_error_of(failure.as_ref()).metrics_label(),
        ));
        // The taxonomy is in error.type; raw job errors may contain secrets.
        span.set_status(Status::error(""));
    } else if verified_completion && matches!(snapshot.state, JobLifecycleState::Completed { .. }) {
        // Only the winning terminal transition may assert verified success.
        span.set_status(Status::Ok);
    }
    span.end();
}

/// Terminal observations from one bounded wait; missing and pending jobs never
/// appear in `terminal` and therefore cannot be mistaken for successful work.
#[derive(Default)]
pub(crate) struct TerminalJobOutcomes {
    pub terminal: Vec<JobSnapshot>,
    pub missing: Vec<JobId>,
    pub pending: HashSet<JobId>,
}
