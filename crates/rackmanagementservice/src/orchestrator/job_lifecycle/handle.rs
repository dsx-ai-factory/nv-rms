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

use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::cleanup::{CleanupPlan, CleanupReport};
use super::domain::{JobDomain, JobFailure};
use super::ids::JobId;
use super::registry::JobRegistry;

/// Owned RAII handle for a created job.
///
/// The handle must be moved into the task that performs the job's work. A
/// worker completes or fails the job by consuming the handle. Dropping an
/// unfinished handle, including while a task is unwinding or being aborted,
/// marks the job failed with [`JobDomain::dropped_failure`].
#[must_use = "move job handles into their worker task and complete or fail them"]
pub struct JobHandle<D: JobDomain> {
    registry: Arc<JobRegistry<D>>,
    job_id: JobId,
    span: tracing::Span,
    cancellation_token: CancellationToken,
    finished: bool,
}

impl<D: JobDomain> JobHandle<D> {
    pub(crate) fn new(
        registry: Arc<JobRegistry<D>>,
        job_id: JobId,
        span: tracing::Span,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            registry,
            job_id,
            span,
            cancellation_token,
            finished: false,
        }
    }

    /// Returns this job's ID.
    pub fn id(&self) -> &JobId {
        &self.job_id
    }

    /// Returns this job's tracing span.
    pub fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    /// Returns this job's cooperative cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Updates the visible progress message when the job is still active.
    pub fn progress(&self, description: impl Into<String>) {
        if !self.registry.mark_running(&self.job_id, description) {
            tracing::warn!("progress update ignored; job no longer active");
        }
    }

    /// Marks the job completed with a description and result payload.
    pub fn complete(mut self, description: impl Into<String>, result_json: impl Into<String>) {
        self.finished = true;
        self.registry
            .mark_completed(&self.job_id, description, result_json);
    }

    /// Marks the job failed with the supplied failure payload.
    pub fn fail(mut self, failure: JobFailure) {
        self.finished = true;
        self.registry.mark_failed(&self.job_id, failure);
    }
}

impl<D: JobDomain> Deref for JobHandle<D> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.job_id.as_ref()
    }
}

impl<D: JobDomain> fmt::Display for JobHandle<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.job_id.fmt(formatter)
    }
}

impl<D: JobDomain> fmt::Debug for JobHandle<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobHandle")
            .field("job_id", &self.job_id)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl<D: JobDomain> PartialEq for JobHandle<D> {
    fn eq(&self, other: &Self) -> bool {
        self.job_id == other.job_id
    }
}

impl<D: JobDomain> Eq for JobHandle<D> {}

impl<D: JobDomain> Drop for JobHandle<D> {
    fn drop(&mut self) {
        if !self.finished {
            self.finished = true;
            self.registry
                .mark_failed(&self.job_id, D::dropped_failure());
        }
    }
}

/// Join handle for a supervised job task.
#[must_use = "call wait or detach on job join handles"]
pub struct JobJoinHandle {
    job_id: JobId,
    supervisor: JoinHandle<CleanupReport>,
}

impl JobJoinHandle {
    /// Returns the job ID.
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
    /// Spawns a job worker with no cleanup plan.
    pub fn spawn_job<F, Fut>(self: &Arc<Self>, job: JobHandle<D>, run: F) -> JobJoinHandle
    where
        F: FnOnce(JobHandle<D>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn_job_inner(job, None, run)
    }

    /// Spawns a job worker and runs cleanup after the worker exits.
    pub fn spawn_job_with_cleanup<F, Fut>(
        self: &Arc<Self>,
        job: JobHandle<D>,
        cleanup_plan: CleanupPlan,
        run: F,
    ) -> JobJoinHandle
    where
        F: FnOnce(JobHandle<D>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.spawn_job_inner(job, Some(cleanup_plan), run)
    }

    fn spawn_job_inner<F, Fut>(
        self: &Arc<Self>,
        job: JobHandle<D>,
        cleanup_plan: Option<CleanupPlan>,
        run: F,
    ) -> JobJoinHandle
    where
        F: FnOnce(JobHandle<D>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let job_id = job.id().clone();
        let worker_span = job.span();
        let supervisor_span = worker_span.clone();
        let cleanup_span = worker_span.clone();
        let worker = tokio::spawn(run(job).instrument(worker_span));
        let registry = Arc::clone(self);

        let supervisor = tokio::spawn(
            async move {
                if let Err(error) = worker.await {
                    tracing::warn!(%error, "job worker task exited unsuccessfully");
                }

                match cleanup_plan {
                    Some(cleanup_plan) => {
                        run_cleanup_supervised(cleanup_plan, registry, cleanup_span).await
                    }
                    None => CleanupReport::Skipped,
                }
            }
            .instrument(supervisor_span),
        );

        JobJoinHandle { job_id, supervisor }
    }
}

/// Runs the cleanup plan inside its own task so a panic during cleanup is
/// caught as a [`tokio::task::JoinError`] instead of escaping as a bare Tokio
/// task error.
async fn run_cleanup_supervised<D: JobDomain>(
    cleanup_plan: CleanupPlan,
    registry: Arc<JobRegistry<D>>,
    cleanup_span: tracing::Span,
) -> CleanupReport {
    let cleanup =
        tokio::spawn(async move { cleanup_plan.run(registry).await }.instrument(cleanup_span));

    match cleanup.await {
        Ok(report) => report,
        Err(error) if error.is_panic() => {
            let message = panic_message_from_join_error(error);
            tracing::error!(
                panic_message = %message,
                "job cleanup panicked; resource may be leaked"
            );
            CleanupReport::Panicked(message)
        }
        Err(error) if error.is_cancelled() => {
            tracing::warn!("job cleanup was cancelled before completing");
            CleanupReport::Skipped
        }
        Err(error) => {
            tracing::warn!(%error, "job cleanup exited with an unknown join error");
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
