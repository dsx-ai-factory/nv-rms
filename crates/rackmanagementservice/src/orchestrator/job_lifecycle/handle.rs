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
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt as _;
use tokio::sync::oneshot;
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
    result_metadata: serde_json::Map<String, serde_json::Value>,
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
            result_metadata: serde_json::Map::new(),
        }
    }

    /// Adds trusted metadata to every terminal result, including handle abandonment.
    ///
    /// Metadata keys take precedence over result keys. Empty results become an
    /// object; non-object payloads are preserved under `result`. Jobs without
    /// metadata retain their existing result bytes. This does not persist jobs
    /// or change the queued/running status payload.
    pub(crate) fn with_result_metadata(
        mut self,
        metadata: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        self.result_metadata.extend(metadata);
        self
    }

    fn result_with_metadata(&self, result_json: String) -> String {
        if self.result_metadata.is_empty() {
            return result_json;
        }
        let mut result = if result_json.is_empty() {
            serde_json::Map::new()
        } else {
            match serde_json::from_str(&result_json) {
                Ok(serde_json::Value::Object(result)) => result,
                parsed => serde_json::Map::from_iter([(
                    "result".to_owned(),
                    parsed.unwrap_or(serde_json::Value::String(result_json)),
                )]),
            }
        };
        result.extend(self.result_metadata.clone());
        serde_json::Value::Object(result).to_string()
    }

    fn failure_with_metadata(&self, mut failure: JobFailure) -> JobFailure {
        if !self.result_metadata.is_empty() {
            failure.result_json =
                Some(self.result_with_metadata(failure.result_json.take().unwrap_or_default()));
        }
        failure
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
    ///
    /// The registry logs the `job started`/`job progress` chokepoint event on
    /// a successful update. Returns `false` and warns when the job is missing
    /// or terminal; returns `true` when the description is accepted.
    pub fn progress(&self, description: impl Into<String>) -> bool {
        let accepted = self.registry.mark_running(&self.job_id, description);

        if !accepted {
            tracing::warn!(
                job_id = %self.job_id,
                "progress update ignored; job no longer active"
            );
        }

        accepted
    }

    /// Marks the job completed with a description and result payload.
    ///
    /// The registry logs the `job completed` chokepoint event on the
    /// transition. Completion alone leaves the span status unset.
    pub fn complete(mut self, description: impl Into<String>, result_json: impl Into<String>) {
        self.finished = true;
        self.registry.mark_completed(
            &self.job_id,
            description,
            self.result_with_metadata(result_json.into()),
            false,
        );
    }

    /// Completes a job whose requested outcome has been verified successful.
    ///
    /// The registry sets the span status to `Ok` only if this completion wins
    /// the terminal transition. Use [`Self::complete`] when the final outcome
    /// remains uncertain, including work that was only submitted or accepted.
    pub fn complete_verified(
        mut self,
        description: impl Into<String>,
        result_json: impl Into<String>,
    ) {
        self.finished = true;
        self.registry.mark_completed(
            &self.job_id,
            description,
            self.result_with_metadata(result_json.into()),
            true,
        );
    }

    /// Marks the job failed with the supplied failure payload.
    ///
    /// The registry logs the `job failed` chokepoint event on the
    /// transition.
    pub fn fail(mut self, failure: JobFailure) {
        self.finished = true;
        self.registry
            .mark_failed(&self.job_id, self.failure_with_metadata(failure));
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
            let failure = if self.cancellation_token.is_cancelled() {
                D::shutdown_failure()
            } else {
                D::dropped_failure()
            };
            self.registry
                .mark_failed(&self.job_id, self.failure_with_metadata(failure));
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
        let cancellation_token = job.cancellation_token();
        let worker_span = job.span();
        let supervisor_span = worker_span.clone();
        let cleanup_span = worker_span.clone();
        let (worker_start_tx, worker_start_rx) = oneshot::channel();
        let worker = tokio::spawn(
            async move {
                if worker_start_rx.await.is_ok() {
                    run(job).await;
                }
            }
            .instrument(worker_span),
        );
        let worker_abort = worker.abort_handle();
        let registry = Arc::clone(self);
        let registration = TaskRegistration {
            registry: Arc::clone(self),
            job_id: job_id.clone(),
        };

        let supervisor = tokio::spawn(
            async move {
                let _registration = registration;
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
        let supervisor_abort = supervisor.abort_handle();
        if self.register_task(job_id.clone(), worker_abort.clone(), supervisor_abort) {
            let _ = worker_start_tx.send(());
        } else {
            // Preserve shutdown classification when admission closes before the cancellation sweep.
            cancellation_token.cancel();
            worker_abort.abort();
        }

        JobJoinHandle { job_id, supervisor }
    }
}

struct TaskRegistration<D: JobDomain> {
    registry: Arc<JobRegistry<D>>,
    job_id: JobId,
}

impl<D: JobDomain> Drop for TaskRegistration<D> {
    fn drop(&mut self) {
        self.registry.remove_task(&self.job_id);
    }
}

/// Runs cleanup inside the tracked supervisor and converts panics into a
/// bounded report.
async fn run_cleanup_supervised<D: JobDomain>(
    cleanup_plan: CleanupPlan,
    registry: Arc<JobRegistry<D>>,
    cleanup_span: tracing::Span,
) -> CleanupReport {
    match AssertUnwindSafe(cleanup_plan.run(registry).instrument(cleanup_span))
        .catch_unwind()
        .await
    {
        Ok(report) => report,
        Err(payload) => {
            let message = panic_message(payload);
            tracing::error!(
                panic_message = %message,
                "job cleanup panicked; resource may be leaked"
            );
            CleanupReport::Panicked(message)
        }
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => message.to_string(),
            Err(_) => "unknown panic payload".to_string(),
        },
    }
}
