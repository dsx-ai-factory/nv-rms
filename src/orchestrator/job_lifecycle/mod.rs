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

//! Generic lifecycle primitives for asynchronous RMS jobs.
//!
//! This module owns the common mechanics that older job trackers had to hand
//! code at each call site:
//!
//! - storing job state behind an `Arc<JobRegistry<D>>`;
//! - protecting the registry with a short-held `RwLock`;
//! - exposing a [`PendingJob`] that is hard to ignore before work starts;
//! - exposing a [`TrackedJob`] whose `Drop` implementation fails unsealed
//!   non-terminal jobs;
//! - recording panic and cancellation failures from the Tokio supervisor;
//! - running cleanup plans after the worker task exits, including panic paths;
//! - keeping domain-specific error codes outside the generic registry.
//!
//! The intended integration pattern is to keep a small domain adapter beside
//! the public tracker that already exists for an API. The adapter implements
//! [`JobDomain`] and translates generic lifecycle failures into that API's
//! error type. API handlers then create a [`PendingJob`] and pass it to
//! [`JobRegistry::spawn_tracked`] rather than spawning raw Tokio tasks that
//! manually mark success or failure.
//!
//! # Lifecycle
//!
//! A job moves through these states:
//!
//! 1. [`JobLifecycleState::Queued`] after [`JobRegistry::create_leaf`].
//! 2. [`JobLifecycleState::Running`] after [`TrackedJob::progress`] or
//!    [`JobRegistry::mark_running`].
//! 3. [`JobLifecycleState::Completed`] after [`TrackedJob::complete`] or
//!    [`JobRegistry::mark_completed`].
//! 4. [`JobLifecycleState::Failed`] after [`TrackedJob::fail`], task drop,
//!    task panic, task abort, or [`JobRegistry::mark_failed`].
//!
//! Terminal states are final for ordinary state-transition APIs. Compatibility
//! adapters may still write terminal state through [`JobRegistry::mark_completed`]
//! or [`JobRegistry::mark_failed`] while running under a tracked supervisor; the
//! finalizer observes the terminal state and does not clobber it with a generic
//! drop failure. These adapters must keep the [`TrackedJob`] guard alive until
//! their async work finishes. New worker code should prefer pending and tracked
//! handles.
//!
//! # Panic and cleanup behavior
//!
//! [`TrackedJob`] is a RAII guard. If worker code returns without calling
//! [`TrackedJob::complete`] or [`TrackedJob::fail`], dropping the guard marks a
//! still-active job failed with [`JobDomain::dropped_failure`].
//!
//! Panics need a separate supervisor step. During unwinding, [`TrackedJob`]
//! avoids writing a generic drop failure. The supervisor awaits the worker
//! `JoinHandle`, observes `JoinError::is_panic`, records
//! [`JobDomain::panic_failure`], and then runs the attached [`CleanupPlan`].
//! This is why cleanup should use `spawn_tracked_with_cleanup`, not be placed
//! after awaited work inside the worker body.
//!
//! Cleanup plans that wait for child jobs treat a missing job ID as done. The
//! registry only receives IDs for jobs that were already created, so a missing
//! child means the terminal record was collected by TTL cleanup.
//!
//! Cleanup itself runs in its own supervised task instrumented with the job's
//! tracing span. A panic during cleanup is caught, logged within that span (so
//! the `job_id` and other span fields are attached automatically), and returned
//! as [`CleanupReport::Panicked`] instead of surfacing as a context-free Tokio
//! task error. A leaked resource (such as a directory that never got removed)
//! can thus be traced back to its job via the span. A cleanup panic does not
//! change the worker's terminal job state.
//!
//! # Example: basic tracked job
//!
//! ```rust
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use rackmanagementservice::orchestrator::job_lifecycle::{
//!     JobDomain, JobFailure, JobId, JobLifecycleState, JobRegistry, JobSpec,
//! };
//!
//! struct FirmwareJobs;
//!
//! impl JobDomain for FirmwareJobs {
//!     fn dropped_failure() -> JobFailure {
//!         JobFailure::new(anyhow::anyhow!("dropped"), "worker exited before completion")
//!     }
//!
//!     fn panic_failure() -> JobFailure {
//!         JobFailure::new(anyhow::anyhow!("panic"), "worker panicked")
//!     }
//!
//!     fn cancelled_failure() -> JobFailure {
//!         JobFailure::new(anyhow::anyhow!("cancelled"), "worker was cancelled")
//!     }
//!
//!     fn at_capacity_failure(current: usize, max: usize) -> JobFailure {
//!         JobFailure::new(
//!             anyhow::anyhow!("at capacity"),
//!             format!("tracker full ({current}/{max})"),
//!         )
//!     }
//!
//!     fn node_busy_failure(
//!         rack_id: &str,
//!         node_id: &str,
//!         active_job_id: &JobId,
//!     ) -> JobFailure {
//!         JobFailure::new(
//!             anyhow::anyhow!("node busy"),
//!             format!("{rack_id}/{node_id} busy (job {active_job_id})"),
//!         )
//!     }
//! }
//!
//! # async fn example() -> Result<(), tokio::task::JoinError> {
//! let registry = Arc::new(JobRegistry::<FirmwareJobs>::new(Duration::from_secs(3600)));
//! let pending = registry
//!     .create_leaf(JobSpec::new(
//!         "rack-1",
//!         "node-1",
//!         "Queued",
//!         tracing::Span::none(),
//!     ))
//!     .expect("registry has capacity");
//! let job_id = pending.id().clone();
//!
//! let handle = registry.spawn_tracked(pending, |job| async move {
//!     job.progress("Uploading firmware");
//!     job.complete(r#"{"status":"ok"}"#);
//! });
//!
//! handle.wait().await?;
//! if let Some(snapshot) = registry.get(&job_id) {
//!     assert!(matches!(
//!         snapshot.state,
//!         JobLifecycleState::Completed { .. }
//!     ));
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Example: cleanup after child jobs finish
//!
//! Attach cleanup to the supervisor when a worker owns a temporary artifact
//! directory. The cleanup runs even when the worker panics.
//!
//! ```rust,no_run
//! use std::path::PathBuf;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use rackmanagementservice::orchestrator::job_lifecycle::{
//!     CleanupPlan, JobDomain, JobFailure, JobId, JobRegistry, JobSpec,
//! };
//!
//! struct CleanupJobs;
//!
//! impl JobDomain for CleanupJobs {
//!     fn dropped_failure() -> JobFailure {
//!         JobFailure::new(anyhow::anyhow!("dropped"), "worker exited before completion")
//!     }
//!
//!     fn panic_failure() -> JobFailure {
//!         JobFailure::new(anyhow::anyhow!("panic"), "worker panicked")
//!     }
//!
//!     fn cancelled_failure() -> JobFailure {
//!         JobFailure::new(anyhow::anyhow!("cancelled"), "worker was cancelled")
//!     }
//!
//!     fn at_capacity_failure(current: usize, max: usize) -> JobFailure {
//!         JobFailure::new(
//!             anyhow::anyhow!("at capacity"),
//!             format!("tracker full ({current}/{max})"),
//!         )
//!     }
//!
//!     fn node_busy_failure(
//!         rack_id: &str,
//!         node_id: &str,
//!         active_job_id: &JobId,
//!     ) -> JobFailure {
//!         JobFailure::new(
//!             anyhow::anyhow!("node busy"),
//!             format!("{rack_id}/{node_id} busy (job {active_job_id})"),
//!         )
//!     }
//! }
//!
//! # async fn example() -> Result<(), tokio::task::JoinError> {
//! let registry = Arc::new(JobRegistry::<CleanupJobs>::new(Duration::from_secs(3600)));
//! let child = registry
//!     .create_leaf(JobSpec::new(
//!         "rack-1",
//!         "node-1",
//!         "Queued",
//!         tracing::Span::none(),
//!     ))
//!     .expect("registry has capacity");
//! let child_id = child.id().clone();
//! let cleanup_root = PathBuf::from("/tmp/rms-artifact-cache/job-123");
//!
//! let child_handle = registry.spawn_tracked(child, |job| async move {
//!     job.complete(r#"{"status":"ok"}"#);
//! });
//!
//! let parent = registry
//!     .create_leaf(JobSpec::new(
//!         "rack-1",
//!         "batch",
//!         "Queued",
//!         tracing::Span::none(),
//!     ))
//!     .expect("registry has capacity");
//! let cleanup = CleanupPlan::remove_dir_after_jobs_terminate(
//!     vec![child_id],
//!     cleanup_root,
//!     Some(Duration::from_secs(300)),
//! );
//! let parent_handle = registry.spawn_tracked_with_cleanup(parent, cleanup, |job| async move {
//!     job.complete(r#"{"status":"ok"}"#);
//! });
//!
//! child_handle.wait().await?;
//! parent_handle.wait().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Waiting or detaching
//!
//! [`TrackedJoinHandle`] is `must_use`. Tests and structured background
//! services should usually call [`TrackedJoinHandle::wait`] so panic handling
//! and cleanup completion are observable. Request handlers that intentionally
//! launch background work can call [`TrackedJoinHandle::detach`]; the supervisor
//! task continues to run cleanup, but the caller no longer observes its result.
//!
//! # Locking and async work
//!
//! Registry methods take short synchronous `RwLock` guards and never hold them
//! across `.await`. Async cleanup and terminal-job waiting clone or copy the
//! data they need, release the lock, and only then await.
mod cleanup;
mod domain;
mod handle;
mod ids;
mod registry;

pub use cleanup::{CleanupFailure, CleanupOperation, CleanupPlan, CleanupReport};
pub use domain::{JobDomain, JobFailure};
pub use handle::{PendingJob, TrackedJob, TrackedJoinHandle};
pub use ids::JobId;
pub use registry::{
    JobLifecycleState, JobRegistry, JobSnapshot, JobSpec, MAX_TRACKED_JOBS, ParentJobSpec,
};

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    const TTL: Duration = Duration::from_secs(60);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestFailure {
        Dropped,
        Panic,
        Cancelled,
        Explicit,
        AtCapacity,
        NodeBusy,
    }

    impl std::fmt::Display for TestFailure {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    impl std::error::Error for TestFailure {}

    struct TestDomain;

    impl JobDomain for TestDomain {
        fn dropped_failure() -> JobFailure {
            JobFailure::new(TestFailure::Dropped, "job exited before completion")
        }

        fn panic_failure() -> JobFailure {
            JobFailure::new(TestFailure::Panic, "job task panicked")
        }

        fn cancelled_failure() -> JobFailure {
            JobFailure::new(TestFailure::Cancelled, "job task was cancelled")
        }

        fn at_capacity_failure(current: usize, max: usize) -> JobFailure {
            JobFailure::new(
                TestFailure::AtCapacity,
                format!("registry at capacity ({current}/{max})"),
            )
        }

        fn node_busy_failure(rack_id: &str, node_id: &str, active_job_id: &JobId) -> JobFailure {
            JobFailure::new(
                TestFailure::NodeBusy,
                format!("{rack_id}/{node_id} busy (job {active_job_id})"),
            )
        }
    }

    #[tokio::test]
    async fn completed_job_is_not_overwritten_by_defer_or_drop() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();

        let handle = registry.spawn_tracked(pending, |job| async move {
            job.complete(r#"{"status":"ok"}"#);
        });

        handle.wait().await.unwrap();
        let snapshot = registry.get(&job_id).unwrap();
        assert_completed(&snapshot.state, "Completed", r#"{"status":"ok"}"#);
    }

    #[tokio::test]
    async fn early_return_marks_job_failed() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();

        let handle = registry.spawn_tracked(pending, |_job| async move {});

        handle.wait().await.unwrap();
        let snapshot = registry.get(&job_id).unwrap();
        assert_failed(snapshot.state, TestFailure::Dropped, "before completion");
    }

    #[tokio::test]
    async fn panic_marks_job_failed_and_runs_cleanup() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();
        let cleanup_dir = create_cleanup_dir();

        let handle = registry.spawn_tracked_with_cleanup(
            pending,
            CleanupPlan::remove_dir(cleanup_dir.clone()),
            |_job| async move {
                panic!("simulated worker panic");
            },
        );

        assert_eq!(handle.wait().await.unwrap(), CleanupReport::Removed);
        assert!(!cleanup_dir.exists());
        let snapshot = registry.get(&job_id).unwrap();
        assert_failed(snapshot.state, TestFailure::Panic, "panicked");
    }

    #[tokio::test]
    async fn cleanup_waits_for_child_panic_to_become_terminal() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let child = registry.create_leaf(test_spec("rack", "child")).unwrap();
        let child_id = child.id().clone();
        let parent = registry.create_leaf(test_spec("rack", "parent")).unwrap();
        let cleanup_dir = create_cleanup_dir();

        let child_handle = registry.spawn_tracked(child, |_job| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            panic!("child panic");
        });
        let parent_handle = registry.spawn_tracked_with_cleanup(
            parent,
            CleanupPlan::remove_dir_after_jobs_terminate(
                vec![child_id.clone()],
                cleanup_dir.clone(),
                Some(Duration::from_secs(1)),
            ),
            |job| async move {
                job.complete("{}");
            },
        );

        assert_eq!(parent_handle.wait().await.unwrap(), CleanupReport::Removed);
        child_handle.wait().await.unwrap();
        assert!(!cleanup_dir.exists());
        let child_snapshot = registry.get(&child_id).unwrap();
        assert_failed(child_snapshot.state, TestFailure::Panic, "panicked");
    }

    #[tokio::test]
    async fn cleanup_panic_returns_panicked_report_without_supervisor_panic() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();
        let cleanup_path = create_cleanup_dir();

        let handle = registry.spawn_tracked_with_cleanup(
            pending,
            CleanupPlan::panic_for_test(cleanup_path.clone()),
            |job| async move {
                job.complete("{}");
            },
        );

        // The supervisor catches the cleanup panic and reports it, so awaiting
        // it succeeds (Ok) instead of yielding a JoinError of its own.
        let CleanupReport::Panicked(message) = handle.wait().await.unwrap() else {
            panic!("expected cleanup panic report");
        };
        assert!(message.contains("simulated cleanup panic"));
        // Cleanup never finished, so the directory is left behind (the leak the
        // panic report exists to surface).
        assert!(cleanup_path.exists());

        // The worker's terminal state is unaffected by the cleanup panic.
        let snapshot = registry.get(&job_id).unwrap();
        assert_completed(&snapshot.state, "Completed", "{}");

        std::fs::remove_dir_all(cleanup_path).unwrap();
    }

    #[tokio::test]
    async fn cleanup_failure_reports_operation_path_and_error() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let cleanup_file = create_cleanup_file();

        let handle = registry.spawn_tracked_with_cleanup(
            pending,
            CleanupPlan::remove_dir(cleanup_file.clone()),
            |job| async move {
                job.complete("{}");
            },
        );

        let CleanupReport::Failed(failure) = handle.wait().await.unwrap() else {
            panic!("expected cleanup failure");
        };
        assert_eq!(failure.operation, CleanupOperation::RemoveDir);
        assert_eq!(failure.path, cleanup_file);
        assert_ne!(failure.kind, std::io::ErrorKind::NotFound);
        assert!(!failure.message.is_empty());

        std::fs::remove_file(cleanup_file).unwrap();
    }

    #[tokio::test]
    async fn explicit_failure_seals_job() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();

        let handle = registry.spawn_tracked(pending, |job| async move {
            job.fail(JobFailure::new(TestFailure::Explicit, "explicit failure"));
        });

        handle.wait().await.unwrap();
        let snapshot = registry.get(&job_id).unwrap();
        assert_failed(snapshot.state, TestFailure::Explicit, "explicit failure");
    }

    #[tokio::test]
    async fn panic_after_explicit_failure_preserves_worker_failure() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();
        let cleanup_dir = create_cleanup_dir();

        let handle = registry.spawn_tracked_with_cleanup(
            pending,
            CleanupPlan::remove_dir(cleanup_dir.clone()),
            |job| async move {
                job.fail(JobFailure::new(TestFailure::Explicit, "explicit failure"));
                panic!("panic after worker-owned failure");
            },
        );

        assert_eq!(handle.wait().await.unwrap(), CleanupReport::Removed);
        assert!(!cleanup_dir.exists());
        let snapshot = registry.get(&job_id).unwrap();
        assert_failed(snapshot.state, TestFailure::Explicit, "explicit failure");
    }

    #[tokio::test]
    async fn compatibility_terminal_state_is_not_clobbered_by_drop() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();
        let worker_registry = registry.clone();
        let worker_job_id = job_id.clone();

        let handle = registry.spawn_tracked(pending, move |job| async move {
            let _tracked_job = job;
            worker_registry.mark_completed(&worker_job_id, "Completed", r#"{"status":"ok"}"#);
        });

        handle.wait().await.unwrap();
        let snapshot = registry.get(&job_id).unwrap();
        assert_completed(&snapshot.state, "Completed", r#"{"status":"ok"}"#);
    }

    #[test]
    fn mark_completed_does_not_overwrite_terminal_jobs() {
        let registry = JobRegistry::<TestDomain>::new(TTL);
        let failed = registry.create_leaf(test_spec("rack", "failed")).unwrap();
        let failed_id = failed.id().clone();
        let completed = registry
            .create_leaf(test_spec("rack", "completed"))
            .unwrap();
        let completed_id = completed.id().clone();

        assert!(registry.mark_failed(
            &failed_id,
            JobFailure::new(TestFailure::Explicit, "explicit failure")
        ));
        assert!(!registry.mark_completed(&failed_id, "Completed", "{}"));
        let failed_snapshot = registry.get(&failed_id).unwrap();
        assert_failed(
            failed_snapshot.state,
            TestFailure::Explicit,
            "explicit failure",
        );

        assert!(registry.mark_completed(&completed_id, "Completed", r#"{"status":"ok"}"#));
        assert!(!registry.mark_completed(
            &completed_id,
            "Completed again",
            r#"{"status":"overwritten"}"#
        ));
        let completed_snapshot = registry.get(&completed_id).unwrap();
        assert_completed(&completed_snapshot.state, "Completed", r#"{"status":"ok"}"#);
    }

    #[test]
    fn mark_failed_does_not_overwrite_terminal_jobs() {
        let registry = JobRegistry::<TestDomain>::new(TTL);
        let completed = registry
            .create_leaf(test_spec("rack", "completed"))
            .unwrap();
        let completed_id = completed.id().clone();
        let failed = registry.create_leaf(test_spec("rack", "failed")).unwrap();
        let failed_id = failed.id().clone();

        assert!(registry.mark_completed(&completed_id, "Completed", r#"{"status":"ok"}"#));
        assert!(!registry.mark_failed(
            &completed_id,
            JobFailure::new(TestFailure::Explicit, "late failure")
        ));
        let completed_snapshot = registry.get(&completed_id).unwrap();
        assert_completed(&completed_snapshot.state, "Completed", r#"{"status":"ok"}"#);

        assert!(registry.mark_failed(
            &failed_id,
            JobFailure::new(TestFailure::Explicit, "initial failure")
        ));
        assert!(!registry.mark_failed(
            &failed_id,
            JobFailure::new(TestFailure::Cancelled, "late failure")
        ));
        assert!(!registry.mark_panic_failed(&failed_id));
        assert!(!registry.mark_cancelled_failed(&failed_id));
        let failed_snapshot = registry.get(&failed_id).unwrap();
        assert_failed(
            failed_snapshot.state,
            TestFailure::Explicit,
            "initial failure",
        );
    }

    #[test]
    fn create_leaf_refuses_at_capacity() {
        let registry = JobRegistry::<TestDomain>::with_capacity(TTL, 2);
        let _a = registry.create_leaf(test_spec("rack", "a")).unwrap();
        let _b = registry.create_leaf(test_spec("rack", "b")).unwrap();

        let err = match registry.create_leaf(test_spec("rack", "c")) {
            Err(err) => err,
            Ok(_) => panic!("expected capacity failure"),
        };
        assert_eq!(
            err.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::AtCapacity)
        );
        assert!(err.message.contains("2/2"), "message was {:?}", err.message);
    }

    #[test]
    fn create_leaf_if_node_idle_prefers_node_busy_over_capacity() {
        let registry = JobRegistry::<TestDomain>::with_capacity(TTL, 1);
        let _a = registry
            .create_leaf_if_node_idle(test_spec("rack", "a"))
            .unwrap();

        // Same node on a full map: the node-busy gate is reported first.
        let Err(busy) = registry.create_leaf_if_node_idle(test_spec("rack", "a")) else {
            panic!("same node should be refused");
        };
        assert_eq!(
            busy.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::NodeBusy)
        );

        // Different node on a full map: capacity refusal.
        let Err(full) = registry.create_leaf_if_node_idle(test_spec("rack", "b")) else {
            panic!("full map should be refused");
        };
        assert_eq!(
            full.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::AtCapacity)
        );
    }

    #[test]
    fn capacity_reopens_after_expired_terminal_cleanup() {
        let registry = JobRegistry::<TestDomain>::with_capacity(TTL, 1);
        let pending = registry.create_leaf(test_spec("rack", "a")).unwrap();
        let id = pending.id().clone();
        assert!(registry.mark_completed(&id, "done", "{}"));

        registry.age_job_for_test(&id, TTL + Duration::from_secs(1));

        // create_leaf prunes the expired terminal job, then admits the new one.
        let next = registry.create_leaf(test_spec("rack", "b")).unwrap();
        assert!(registry.get(next.id()).is_some());
        assert!(registry.get(&id).is_none());
    }

    fn test_spec(rack_id: &str, node_id: &str) -> JobSpec {
        JobSpec::new(rack_id, node_id, "Queued", tracing::Span::none())
    }

    fn assert_failed(
        state: JobLifecycleState,
        expected_failure: TestFailure,
        message_fragment: &str,
    ) {
        let JobLifecycleState::Failed {
            failure, message, ..
        } = state
        else {
            panic!("expected failed state");
        };

        assert_eq!(
            failure.downcast_ref::<TestFailure>().copied(),
            Some(expected_failure)
        );
        assert!(
            message.contains(message_fragment),
            "message {message:?} did not contain {message_fragment:?}"
        );
    }

    fn assert_completed(
        state: &JobLifecycleState,
        expected_description: &str,
        expected_json: &str,
    ) {
        let JobLifecycleState::Completed {
            description,
            result_json,
        } = state
        else {
            panic!("expected completed state, got {state:?}");
        };

        assert_eq!(description, expected_description);
        assert_eq!(result_json, expected_json);
    }

    fn create_cleanup_dir() -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("rms-job-lifecycle-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        path
    }

    fn create_cleanup_file() -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("rms-job-lifecycle-test-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, b"not a directory").unwrap();
        path
    }
}
