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

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::cleanup::{CleanupPlan, CleanupReport};
use super::domain::{JobDomain, JobFailure};
use super::ids::JobId;
use super::registry::JobRegistry;

const FINALIZER_ACTIVE: u8 = 0;
const FINALIZER_SEALED: u8 = 1;
const FINALIZER_DROPPED: u8 = 2;

/// Created job that should be started through `spawn_tracked`.
#[must_use = "pending jobs should be passed to spawn_tracked"]
pub struct PendingJob<D: JobDomain> {
    job_id: JobId,
    span: tracing::Span,
    cancellation_token: CancellationToken,
    _domain: PhantomData<D>,
}

impl<D: JobDomain> PendingJob<D> {
    pub(crate) fn new(
        job_id: JobId,
        span: tracing::Span,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            job_id,
            span,
            cancellation_token,
            _domain: PhantomData,
        }
    }

    /// Returns this pending job's ID.
    pub fn id(&self) -> &JobId {
        &self.job_id
    }

    /// Returns this pending job's tracing span.
    pub fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    /// Returns this pending job's cooperative cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

/// RAII handle used by a running tracked job.
#[must_use = "dropping an unsealed tracked job marks non-terminal work failed"]
pub struct TrackedJob<D: JobDomain> {
    registry: Arc<JobRegistry<D>>,
    job_id: JobId,
    cancellation_token: CancellationToken,
    finalizer_state: Arc<AtomicU8>,
}

impl<D: JobDomain> TrackedJob<D> {
    fn new(
        registry: Arc<JobRegistry<D>>,
        pending: PendingJob<D>,
        finalizer_state: Arc<AtomicU8>,
    ) -> Self {
        Self {
            registry,
            job_id: pending.job_id,
            cancellation_token: pending.cancellation_token,
            finalizer_state,
        }
    }

    /// Returns this job's ID.
    pub fn id(&self) -> &JobId {
        &self.job_id
    }

    /// Returns this job's cooperative cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Updates the visible progress message when the job is still active.
    ///
    /// Best-effort: if the job has already reached a terminal state (e.g. it was
    /// cancelled or failed mid-run), the drop is recorded as a warning.
    pub fn progress(&self, description: impl Into<String>) {
        if !self.registry.mark_running(&self.job_id, description) {
            tracing::warn!("progress update ignored; job no longer active");
        }
    }

    /// Marks the job completed and seals the RAII finalizer.
    pub fn complete(self, result_json: impl Into<String>) {
        self.complete_with_description("Completed", result_json);
    }

    /// Marks the job completed with a custom description.
    pub fn complete_with_description(
        self,
        description: impl Into<String>,
        result_json: impl Into<String>,
    ) {
        if self.seal() {
            self.registry
                .mark_completed(&self.job_id, description, result_json);
        }
    }

    /// Marks the job failed and seals the RAII finalizer.
    pub fn fail(self, failure: JobFailure) {
        if self.seal() {
            self.registry.mark_failed(&self.job_id, failure);
        }
    }

    fn seal(&self) -> bool {
        self.finalizer_state
            .compare_exchange(
                FINALIZER_ACTIVE,
                FINALIZER_SEALED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

impl<D: JobDomain> Drop for TrackedJob<D> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        mark_dropped_if_unsealed(&self.registry, &self.job_id, &self.finalizer_state);
    }
}

/// Join handle for a supervised tracked job.
#[must_use = "call wait or detach on tracked join handles"]
pub struct TrackedJoinHandle {
    job_id: JobId,
    supervisor: JoinHandle<CleanupReport>,
}

impl TrackedJoinHandle {
    /// Returns the tracked job ID.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Intentionally detaches the supervisor task.
    ///
    /// Cleanup still runs inside the supervisor task. Callers should prefer
    /// [`Self::wait`] when they need to observe cleanup completion.
    pub fn detach(self) {
        let Self { supervisor, .. } = self;
        drop(supervisor);
    }

    /// Waits for the supervisor and cleanup plan to finish.
    pub async fn wait(self) -> Result<CleanupReport, tokio::task::JoinError> {
        self.supervisor.await
    }
}

impl<D: JobDomain> JobRegistry<D> {
    /// Spawns a tracked worker with no cleanup plan.
    pub fn spawn_tracked<F, Fut>(
        self: &Arc<Self>,
        pending: PendingJob<D>,
        run: F,
    ) -> TrackedJoinHandle
    where
        F: FnOnce(TrackedJob<D>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn_tracked_inner(pending, None, run)
    }

    /// Spawns a tracked worker and runs cleanup after the worker exits.
    pub fn spawn_tracked_with_cleanup<F, Fut>(
        self: &Arc<Self>,
        pending: PendingJob<D>,
        cleanup_plan: CleanupPlan,
        run: F,
    ) -> TrackedJoinHandle
    where
        F: FnOnce(TrackedJob<D>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn_tracked_inner(pending, Some(cleanup_plan), run)
    }

    fn spawn_tracked_inner<F, Fut>(
        self: &Arc<Self>,
        pending: PendingJob<D>,
        cleanup_plan: Option<CleanupPlan>,
        run: F,
    ) -> TrackedJoinHandle
    where
        F: FnOnce(TrackedJob<D>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let job_id = pending.id().clone();
        let worker_span = pending.span();
        let supervisor_span = worker_span.clone();
        let cleanup_span = worker_span.clone();
        let finalizer_state = Arc::new(AtomicU8::new(FINALIZER_ACTIVE));
        let job = TrackedJob::new(Arc::clone(self), pending, Arc::clone(&finalizer_state));
        let worker_registry = Arc::clone(self);
        let worker_job_id = job_id.clone();
        let worker_finalizer_state = Arc::clone(&finalizer_state);

        let worker = tokio::spawn(
            async move {
                let _defer = defer::defer(move || {
                    if std::thread::panicking() {
                        return;
                    }
                    mark_dropped_if_unsealed(
                        &worker_registry,
                        &worker_job_id,
                        &worker_finalizer_state,
                    );
                });

                run(job).await;
            }
            .instrument(worker_span),
        );

        let supervisor_registry = Arc::clone(self);
        let supervisor_job_id = job_id.clone();
        let supervisor_finalizer_state = Arc::clone(&finalizer_state);
        let supervisor = tokio::spawn(
            async move {
                match worker.await {
                    Ok(()) => {}
                    Err(error) if error.is_panic() => {
                        mark_panic_if_unsealed(
                            &supervisor_registry,
                            &supervisor_job_id,
                            &supervisor_finalizer_state,
                        );
                    }
                    Err(error) if error.is_cancelled() => {
                        mark_cancelled_if_unsealed(
                            &supervisor_registry,
                            &supervisor_job_id,
                            &supervisor_finalizer_state,
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            job_id = %supervisor_job_id,
                            error = %error,
                            "tracked job worker exited with an unknown join error"
                        );
                        mark_dropped_if_unsealed(
                            &supervisor_registry,
                            &supervisor_job_id,
                            &supervisor_finalizer_state,
                        );
                    }
                }

                match cleanup_plan {
                    Some(cleanup_plan) => {
                        run_cleanup_supervised(cleanup_plan, supervisor_registry, cleanup_span)
                            .await
                    }
                    None => CleanupReport::Skipped,
                }
            }
            .instrument(supervisor_span),
        );

        TrackedJoinHandle { job_id, supervisor }
    }
}

/// Runs the cleanup plan inside its own task so a panic during cleanup is
/// caught as a [`tokio::task::JoinError`] instead of escaping as a bare Tokio
/// task error. The cleanup task is instrumented with the job's span, so its
/// logs (and the caught-panic log below) carry the job context, e.g. `job_id`,
/// without threading those fields through by hand. A caught panic is surfaced as
/// [`CleanupReport::Panicked`]. The worker's terminal job state is untouched.
async fn run_cleanup_supervised<D: JobDomain>(
    cleanup_plan: CleanupPlan,
    registry: Arc<JobRegistry<D>>,
    cleanup_span: tracing::Span,
) -> CleanupReport {
    // Run cleanup in a child task so an internal panic becomes a JoinError we
    // can observe, rather than unwinding (and aborting) the supervisor itself.
    // The span is propagated so the plan's own logs carry the job context too.
    let cleanup =
        tokio::spawn(async move { cleanup_plan.run(registry).await }.instrument(cleanup_span));

    match cleanup.await {
        Ok(report) => report,
        Err(error) if error.is_panic() => {
            let message = panic_message_from_join_error(error);
            tracing::error!(
                panic_message = %message,
                "tracked job cleanup panicked; resource may be leaked"
            );
            CleanupReport::Panicked(message)
        }
        Err(error) if error.is_cancelled() => {
            tracing::warn!("tracked job cleanup was cancelled before completing");
            CleanupReport::Skipped
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "tracked job cleanup exited with an unknown join error"
            );
            CleanupReport::Skipped
        }
    }
}

fn panic_message_from_join_error(error: tokio::task::JoinError) -> String {
    let payload = error.into_panic();
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => message.to_string(),
            Err(_) => "unknown panic payload".to_string(),
        },
    }
}

fn mark_dropped_if_unsealed<D: JobDomain>(
    registry: &JobRegistry<D>,
    job_id: &JobId,
    finalizer_state: &AtomicU8,
) {
    if finalizer_state
        .compare_exchange(
            FINALIZER_ACTIVE,
            FINALIZER_DROPPED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
    {
        if !registry.mark_dropped_failed(job_id) {
            finalizer_state.store(FINALIZER_SEALED, Ordering::Release);
        }
    }
}

fn mark_panic_if_unsealed<D: JobDomain>(
    registry: &JobRegistry<D>,
    job_id: &JobId,
    finalizer_state: &AtomicU8,
) {
    match finalizer_state.swap(FINALIZER_SEALED, Ordering::AcqRel) {
        FINALIZER_ACTIVE => {
            registry.mark_panic_failed(job_id);
        }
        FINALIZER_DROPPED => {
            // Tokio can drop the worker future before the supervisor observes
            // `JoinError::is_panic`, so the RAII path may have recorded only
            // a generic drop failure. Replace only that unsealed failure.
            registry.replace_unsealed_failure_with_panic(job_id);
        }
        FINALIZER_SEALED => {}
        _ => {}
    }
}

fn mark_cancelled_if_unsealed<D: JobDomain>(
    registry: &JobRegistry<D>,
    job_id: &JobId,
    finalizer_state: &AtomicU8,
) {
    match finalizer_state.swap(FINALIZER_SEALED, Ordering::AcqRel) {
        FINALIZER_ACTIVE => {
            registry.mark_cancelled_failed(job_id);
        }
        FINALIZER_DROPPED => {
            // Aborted Tokio tasks drop the worker future before `worker.await`
            // yields `JoinError::is_cancelled`, so the RAII path can only
            // record a generic drop failure. The supervisor is allowed to
            // replace that specific unsealed failure with cancellation.
            registry.replace_unsealed_failure_with_cancelled(job_id);
        }
        FINALIZER_SEALED => {}
        _ => {}
    }
}
