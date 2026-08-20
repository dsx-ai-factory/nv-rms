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
//! - returning an owned [`JobHandle`] for every created job;
//! - failing abandoned jobs through [`JobHandle`]'s `Drop` implementation;
//! - running cleanup plans after the worker task exits, including panic paths;
//! - keeping domain-specific error codes outside the generic registry.
//!
//! The intended integration pattern is to keep a small domain adapter beside
//! the public tracker that already exists for an API. The adapter implements
//! [`JobDomain`] and translates generic lifecycle failures into that API's
//! error type. API handlers then create a [`JobHandle`] and pass it to
//! [`JobRegistry::spawn_job`] rather than spawning raw Tokio tasks that
//! manually mark success or failure.
//!
//! # Lifecycle
//!
//! A job moves through these states:
//!
//! 1. [`JobLifecycleState::Queued`] after [`JobRegistry::create_job`] with a
//!    [`JobSpec::new`] specification.
//! 2. [`JobLifecycleState::Running`] after [`JobHandle::progress`] or
//!    [`JobRegistry::mark_running`].
//! 3. [`JobLifecycleState::Completed`] after [`JobHandle::complete`] or
//!    `JobRegistry::mark_completed`.
//! 4. [`JobLifecycleState::Failed`] after [`JobHandle::fail`],
//!    `JobRegistry::mark_failed`, or dropping the handle without completing
//!    or failing it.
//!
//! Terminal states are final for ordinary state-transition APIs. Compatibility
//! adapters may still write terminal state through
//! `JobRegistry::mark_completed` or `JobRegistry::mark_failed`. The handle
//! must remain owned by the work until that transition happens; dropping it
//! sooner records an abandoned failure.
//!
//! # Panic and cleanup behavior
//!
//! [`JobHandle`] is a RAII guard. If worker code returns without calling
//! [`JobHandle::complete`] or [`JobHandle::fail`], dropping the guard marks a
//! still-active job failed with [`JobDomain::dropped_failure`].
//!
//! Because the worker task owns its [`JobHandle`], unwinding or aborting the
//! task drops the handle and records the same abandoned failure. The supervisor
//! then runs the attached [`CleanupPlan`]. This is why cleanup should use
//! [`JobRegistry::spawn_job_with_cleanup`] instead of being placed after
//! awaited work inside the worker body.
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
//! # Example: basic job
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
//!         JobFailure::new(anyhow::anyhow!("dropped"), "job abandoned")
//!             .with_description("Abandoned")
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
//!
//!     fn shutting_down_failure() -> JobFailure {
//!         JobFailure::new(anyhow::anyhow!("shutting down"), "server is shutting down")
//!     }
//!
//!     fn parent_unavailable_failure(parent_job_id: &JobId) -> JobFailure {
//!         JobFailure::new(
//!             anyhow::anyhow!("parent unavailable"),
//!             format!("parent {parent_job_id} is unavailable"),
//!         )
//!     }
//! }
//!
//! # async fn example() -> Result<(), tokio::task::JoinError> {
//! let registry = Arc::new(JobRegistry::<FirmwareJobs>::new(Duration::from_secs(3600)));
//! let job = registry
//!     .create_job(
//!         JobSpec::new(
//!             "rack-1",
//!             "node-1",
//!             "Queued",
//!             tracing::Span::none(),
//!         ),
//!         None,
//!         false,
//!     )
//!     .expect("registry has capacity");
//! let job_id = job.id().clone();
//!
//! let handle = registry.spawn_job(job, |job| async move {
//!     job.progress("Uploading firmware");
//!     job.complete("Completed", r#"{"status":"ok"}"#);
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
//!         JobFailure::new(anyhow::anyhow!("dropped"), "job abandoned")
//!             .with_description("Abandoned")
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
//!
//!     fn shutting_down_failure() -> JobFailure {
//!         JobFailure::new(anyhow::anyhow!("shutting down"), "server is shutting down")
//!     }
//!
//!     fn parent_unavailable_failure(parent_job_id: &JobId) -> JobFailure {
//!         JobFailure::new(
//!             anyhow::anyhow!("parent unavailable"),
//!             format!("parent {parent_job_id} is unavailable"),
//!         )
//!     }
//! }
//!
//! # async fn example() -> Result<(), tokio::task::JoinError> {
//! let registry = Arc::new(JobRegistry::<CleanupJobs>::new(Duration::from_secs(3600)));
//! let parent = registry
//!     .create_job(
//!         JobSpec::new_parent(
//!             "rack-1",
//!             "Waiting for child jobs",
//!             tracing::Span::none(),
//!         ),
//!         None,
//!         false,
//!     )
//!     .expect("registry has capacity");
//! let parent_id = parent.id().clone();
//! let child = registry
//!     .create_job(
//!         JobSpec::new(
//!             "rack-1",
//!             "node-1",
//!             "Queued",
//!             tracing::Span::none(),
//!         ),
//!         Some(&parent_id),
//!         false,
//!     )
//!     .expect("registry has capacity");
//! let child_id = child.id().clone();
//! let cleanup_root = PathBuf::from("/tmp/rms-artifact-cache/job-123");
//!
//! let child_handle = registry.spawn_job(child, |job| async move {
//!     job.complete("Completed", r#"{"status":"ok"}"#);
//! });
//!
//! let cleanup = CleanupPlan::remove_dir_after_jobs_terminate(
//!     vec![child_id],
//!     cleanup_root,
//!     Some(Duration::from_secs(300)),
//! );
//! let parent_handle = registry.spawn_job_with_cleanup(parent, cleanup, |job| async move {
//!     job.complete("Completed", r#"{"status":"ok"}"#);
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
//! [`JobJoinHandle`] is `must_use`. Tests and structured background
//! services should usually call [`JobJoinHandle::wait`] so panic handling
//! and cleanup completion are observable. Request handlers that intentionally
//! launch background work can call [`JobJoinHandle::detach`]; the supervisor
//! task continues to run cleanup, but the caller no longer observes its result.
//!
//! # Locking and async work
//!
//! Registry methods take short synchronous `RwLock` guards and never hold them
//! across `.await`. Async cleanup and terminal-job waiting clone or copy the
//! data they need, release the lock, and only then await.
mod cleanup;
mod domain;
mod error;
mod handle;
mod ids;
mod job_type;
mod registry;
mod state;
mod store;

pub use cleanup::{CleanupFailure, CleanupOperation, CleanupPlan, CleanupReport};
pub use domain::{JobDomain, JobFailure};
pub use error::JobError;
pub use handle::{JobHandle, JobJoinHandle};
pub use ids::JobId;
pub use job_type::JobType;
pub use registry::{JobLifecycleState, JobRegistry, JobSnapshot, JobSpec, MAX_TRACKED_JOBS};
pub use state::JobState;

#[cfg(test)]
mod tests {
    use registry::ParentJobSpec;
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use super::*;

    const TTL: Duration = Duration::from_secs(60);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestFailure {
        Dropped,
        Explicit,
        AtCapacity,
        NodeBusy,
        ShuttingDown,
        ParentUnavailable,
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
            JobFailure::new(TestFailure::Dropped, "job abandoned").with_description("Abandoned")
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

        fn shutting_down_failure() -> JobFailure {
            JobFailure::new(TestFailure::ShuttingDown, "registry shutting down")
        }

        fn parent_unavailable_failure(parent_job_id: &JobId) -> JobFailure {
            JobFailure::new(
                TestFailure::ParentUnavailable,
                format!("parent {parent_job_id} is unavailable"),
            )
        }
    }

    #[tokio::test]
    async fn completed_job_is_not_overwritten_by_defer_or_drop() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();

        let handle = registry.spawn_job(pending, |job| async move {
            job.complete("Completed", r#"{"status":"ok"}"#);
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

        let handle = registry.spawn_job(pending, |_job| async move {});

        handle.wait().await.unwrap();
        let snapshot = registry.get(&job_id).unwrap();
        assert_failed(snapshot.state, TestFailure::Dropped, "abandoned");
    }

    #[tokio::test]
    async fn panic_marks_job_failed_and_runs_cleanup() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();
        let cleanup_dir = create_cleanup_dir();

        let handle = registry.spawn_job_with_cleanup(
            pending,
            CleanupPlan::remove_dir(cleanup_dir.clone()),
            |_job| async move {
                panic!("simulated worker panic");
            },
        );

        assert_eq!(handle.wait().await.unwrap(), CleanupReport::Removed);
        assert!(!cleanup_dir.exists());
        let snapshot = registry.get(&job_id).unwrap();
        assert_failed(snapshot.state, TestFailure::Dropped, "abandoned");
    }

    #[tokio::test]
    async fn cleanup_waits_for_child_panic_to_become_terminal() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let child = registry.create_leaf(test_spec("rack", "child")).unwrap();
        let child_id = child.id().clone();
        let parent = registry.create_leaf(test_spec("rack", "parent")).unwrap();
        let cleanup_dir = create_cleanup_dir();

        let child_handle = registry.spawn_job(child, |_job| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            panic!("child panic");
        });
        let parent_handle = registry.spawn_job_with_cleanup(
            parent,
            CleanupPlan::remove_dir_after_jobs_terminate(
                vec![child_id.clone()],
                cleanup_dir.clone(),
                Some(Duration::from_secs(1)),
            ),
            |job| async move {
                job.complete("Completed", "{}");
            },
        );

        assert_eq!(parent_handle.wait().await.unwrap(), CleanupReport::Removed);
        child_handle.wait().await.unwrap();
        assert!(!cleanup_dir.exists());
        let child_snapshot = registry.get(&child_id).unwrap();
        assert_failed(child_snapshot.state, TestFailure::Dropped, "abandoned");
    }

    #[tokio::test]
    async fn cleanup_panic_returns_panicked_report_without_supervisor_panic() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let job_id = pending.id().clone();
        let cleanup_path = create_cleanup_dir();

        let handle = registry.spawn_job_with_cleanup(
            pending,
            CleanupPlan::panic_for_test(cleanup_path.clone()),
            |job| async move {
                job.complete("Completed", "{}");
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

        let handle = registry.spawn_job_with_cleanup(
            pending,
            CleanupPlan::remove_dir(cleanup_file.clone()),
            |job| async move {
                job.complete("Completed", "{}");
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

        let handle = registry.spawn_job(pending, |job| async move {
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

        let handle = registry.spawn_job_with_cleanup(
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

    #[test]
    fn create_leaf_refuses_at_capacity() {
        let registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 2));
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
        let registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 1));
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
    fn early_admission_rejections_do_not_sweep_expired_jobs() {
        let busy_registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 2));
        let _active = busy_registry
            .create_leaf_if_node_idle(test_spec("rack", "busy"))
            .unwrap();
        let expired = busy_registry
            .create_leaf(test_spec("rack", "expired"))
            .unwrap();
        let expired_id = expired.id().clone();
        expired.complete("done", "{}");
        busy_registry.age_job_for_test(&expired_id, TTL + Duration::from_secs(1));

        let Err(busy) = busy_registry.create_leaf_if_node_idle(test_spec("rack", "busy")) else {
            panic!("active work must keep the node busy");
        };
        assert_eq!(
            busy.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::NodeBusy)
        );
        assert!(busy_registry.get(&expired_id).is_some());

        let shutdown_registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 1));
        let expired = shutdown_registry
            .create_leaf(test_spec("rack", "expired"))
            .unwrap();
        let expired_id = expired.id().clone();
        expired.complete("done", "{}");
        shutdown_registry.age_job_for_test(&expired_id, TTL + Duration::from_secs(1));
        shutdown_registry.begin_shutdown();

        let Err(shutting_down) = shutdown_registry.create_leaf(test_spec("rack", "new")) else {
            panic!("create must be refused during shutdown");
        };
        assert_eq!(
            shutting_down.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::ShuttingDown)
        );
        let Err(shutting_down) =
            shutdown_registry.create_leaf_if_node_idle(test_spec("rack", "expired"))
        else {
            panic!("idle create must be refused during shutdown");
        };
        assert_eq!(
            shutting_down.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::ShuttingDown)
        );
        assert!(shutdown_registry.get(&expired_id).is_some());
    }

    #[test]
    fn active_node_index_tracks_all_unrestricted_jobs() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let first = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let second = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let third = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let first_id = first.id().clone();
        let second_id = second.id().clone();

        assert!(registry.remove_queued_leaf(&first_id));
        registry.assert_store_consistent_for_test();

        let Err(busy) = registry.create_leaf_if_node_idle(test_spec("rack", "node")) else {
            panic!("remaining unrestricted jobs must keep the node busy");
        };
        assert_eq!(
            busy.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::NodeBusy)
        );

        third.complete("done", "{}");
        registry.assert_store_consistent_for_test();

        let Err(busy) = registry.create_leaf_if_node_idle(test_spec("rack", "node")) else {
            panic!("the second unrestricted job must keep the node busy");
        };
        assert!(
            busy.message.contains(second_id.as_ref()),
            "busy failure should identify the sole active job: {}",
            busy.message
        );

        assert!(registry.mark_running(&second_id, "running"));
        second.fail(JobFailure::new(TestFailure::Explicit, "failed"));
        let next = registry
            .create_leaf_if_node_idle(test_spec("rack", "node"))
            .unwrap();
        assert!(registry.remove_queued_leaf(next.id()));
        registry.assert_store_consistent_for_test();
    }

    #[test]
    fn active_node_index_scopes_keys_and_excludes_parents() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let _rack_a_node_1 = registry
            .create_leaf_if_node_idle(test_spec("rack-a", "node-1"))
            .unwrap();
        let _rack_b_node_1 = registry
            .create_leaf_if_node_idle(test_spec("rack-b", "node-1"))
            .unwrap();
        let _rack_a_node_2 = registry
            .create_leaf_if_node_idle(test_spec("rack-a", "node-2"))
            .unwrap();

        let parent = registry
            .create_parent(ParentJobSpec::new(
                "parent-rack",
                "parent",
                tracing::Span::none(),
            ))
            .unwrap();
        let parent_id = parent.id().clone();
        let _child = registry
            .create_child(&parent_id, test_spec("parent-rack", "child"))
            .unwrap();

        let _empty_node = registry
            .create_leaf_if_node_idle(test_spec("parent-rack", ""))
            .expect("a parent record must not occupy its implicit empty node ID");
        registry.assert_store_consistent_for_test();
    }

    #[test]
    fn cleanup_of_old_terminal_job_preserves_new_active_node_entry() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let old = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let old_id = old.id().clone();
        old.complete("done", "{}");

        let active = registry.create_leaf(test_spec("rack", "node")).unwrap();
        let active_id = active.id().clone();
        registry.age_job_for_test(&old_id, TTL + Duration::from_secs(1));
        registry.cleanup_expired();

        assert!(registry.get(&old_id).is_none());
        registry.assert_store_consistent_for_test();
        let Err(busy) = registry.create_leaf_if_node_idle(test_spec("rack", "node")) else {
            panic!("cleanup must not remove a newer active-node entry");
        };
        assert!(
            busy.message.contains(active_id.as_ref()),
            "busy failure should identify the active job: {}",
            busy.message
        );
    }

    #[test]
    fn admissions_below_capacity_do_not_sweep_expired_jobs() {
        let leaf_registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 3));
        let expired = leaf_registry
            .create_leaf(test_spec("rack", "expired"))
            .unwrap();
        let expired_id = expired.id().clone();
        expired.complete("done", "{}");
        leaf_registry.age_job_for_test(&expired_id, TTL + Duration::from_secs(1));

        let _pending = leaf_registry.create_leaf(test_spec("rack", "new")).unwrap();
        assert!(leaf_registry.get(&expired_id).is_some());

        let idle_registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 3));
        let expired = idle_registry
            .create_leaf(test_spec("rack", "expired"))
            .unwrap();
        let expired_id = expired.id().clone();
        expired.complete("done", "{}");
        idle_registry.age_job_for_test(&expired_id, TTL + Duration::from_secs(1));

        let _pending = idle_registry
            .create_leaf_if_node_idle(test_spec("rack", "new"))
            .unwrap();
        assert!(idle_registry.get(&expired_id).is_some());

        let parent_registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 3));
        let expired = parent_registry
            .create_leaf(test_spec("rack", "expired"))
            .unwrap();
        let expired_id = expired.id().clone();
        expired.complete("done", "{}");
        parent_registry.age_job_for_test(&expired_id, TTL + Duration::from_secs(1));
        let parent = parent_registry
            .create_parent(ParentJobSpec::new("rack", "parent", tracing::Span::none()))
            .unwrap();
        let parent_id = parent.id().clone();
        let _child = parent_registry
            .create_child(&parent_id, test_spec("rack", "child"))
            .unwrap();
        assert!(parent_registry.get(&expired_id).is_some());
    }

    #[test]
    fn idle_and_parent_admissions_sweep_only_under_capacity_pressure() {
        let idle_registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 1));
        let expired = idle_registry
            .create_leaf(test_spec("rack", "node"))
            .unwrap();
        let expired_id = expired.id().clone();
        expired.complete("done", "{}");
        idle_registry.age_job_for_test(&expired_id, TTL + Duration::from_secs(1));

        let next = idle_registry
            .create_leaf_if_node_idle(test_spec("rack", "node"))
            .unwrap();
        assert!(idle_registry.get(next.id()).is_some());
        assert!(idle_registry.get(&expired_id).is_none());

        let parent_registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 2));
        let expired = parent_registry
            .create_leaf(test_spec("rack", "expired"))
            .unwrap();
        let expired_id = expired.id().clone();
        expired.complete("done", "{}");
        parent_registry.age_job_for_test(&expired_id, TTL + Duration::from_secs(1));
        let parent = parent_registry
            .create_parent(ParentJobSpec::new("rack", "parent", tracing::Span::none()))
            .expect("expired terminal job should be pruned for the parent");
        let parent_id = parent.id().clone();
        let child = parent_registry
            .create_child(&parent_id, test_spec("rack", "child"))
            .unwrap();
        let child_id = child.id().clone();
        assert!(parent_registry.get(&expired_id).is_none());
        assert_eq!(
            parent_registry.parent_for_job(&child_id).as_ref(),
            Some(&parent_id)
        );
    }

    #[test]
    fn concurrent_same_node_admission_allows_exactly_one_job() {
        const THREADS: usize = 32;

        let registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, THREADS));
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    registry
                        .create_leaf_if_node_idle(test_spec("rack", "node"))
                        .map_err(|failure| failure.error.downcast_ref::<TestFailure>().copied())
                })
            })
            .collect();

        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(Some(TestFailure::NodeBusy))))
                .count(),
            THREADS - 1
        );
        registry.assert_store_consistent_for_test();
    }

    #[test]
    fn cancellation_keeps_node_busy_until_terminal_transition() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry
            .create_leaf_if_node_idle(test_spec("rack", "node"))
            .unwrap();
        let job_id = pending.id().clone();

        assert!(registry.cancel(&job_id));
        let Err(busy) = registry.create_leaf_if_node_idle(test_spec("rack", "node")) else {
            panic!("signalling cancellation must not release active work");
        };
        assert_eq!(
            busy.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::NodeBusy)
        );

        drop(pending);
        let next = registry
            .create_leaf_if_node_idle(test_spec("rack", "node"))
            .unwrap();
        assert!(registry.remove_queued_leaf(next.id()));
        registry.assert_store_consistent_for_test();
    }

    #[test]
    fn create_parent_refuses_after_shutdown_begins() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));

        registry.begin_shutdown();

        let parent =
            registry.create_parent(ParentJobSpec::new("rack", "parent", tracing::Span::none()));

        assert!(parent.is_none());
    }

    #[test]
    fn children_can_be_attached_after_parent_creation() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let parent_handle = registry
            .create_parent(ParentJobSpec::new("rack", "parent", tracing::Span::none()))
            .expect("parent should be created");
        let parent_id = parent_handle.id().clone();

        let parent = registry.get(&parent_id).unwrap();
        assert!(!parent.is_parent());
        assert!(parent.child_job_ids.is_empty());

        let c1 = registry
            .create_child(&parent_id, test_spec("rack", "n-01"))
            .unwrap();
        let c2 = registry
            .create_child(&parent_id, test_spec("rack", "n-02"))
            .unwrap();
        let c1_id = c1.id().clone();
        let c2_id = c2.id().clone();

        let parent = registry.get(&parent_id).unwrap();
        assert!(parent.is_parent());
        assert!(parent.parent_job_id.is_none());
        assert_eq!(parent.child_job_ids, vec![c1_id.clone(), c2_id.clone()]);

        assert_eq!(registry.parent_for_job(&c1_id).as_ref(), Some(&parent_id));
        assert_eq!(registry.parent_for_job(&c2_id).as_ref(), Some(&parent_id));
    }

    #[test]
    fn top_level_job_can_become_parent_without_leaving_active_index() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let root = registry.create_leaf(test_spec("rack", "root")).unwrap();
        let root_id = root.id().clone();
        let child = registry
            .create_child(&root_id, test_spec("rack", "child"))
            .unwrap();
        let child_id = child.id().clone();

        assert!(registry.parent_for_job(&root_id).is_none());

        let root = registry.get(&root_id).unwrap();
        assert!(root.is_parent());
        assert!(root.parent_job_id.is_none());
        assert_eq!(root.child_job_ids, vec![child_id.clone()]);
        assert_eq!(registry.parent_for_job(&child_id).as_ref(), Some(&root_id));
        let Err(failure) = registry.create_leaf_if_node_idle(test_spec("rack", "root")) else {
            panic!("non-terminal top-level job should remain in the active-node index");
        };
        assert_eq!(
            failure.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::NodeBusy)
        );
        registry.assert_store_consistent_for_test();
    }

    #[test]
    fn child_job_cannot_accept_children() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let first_parent = registry
            .create_parent(ParentJobSpec::new(
                "rack",
                "first parent",
                tracing::Span::none(),
            ))
            .expect("first parent should be created");
        let first_parent_id = first_parent.id().clone();
        let child = registry
            .create_child(&first_parent_id, test_spec("rack", "child"))
            .unwrap();
        let Err(failure) = registry.create_child(child.id(), test_spec("rack", "grandchild"))
        else {
            panic!("child job must not accept children");
        };

        assert_eq!(
            failure.error.downcast_ref::<TestFailure>().copied(),
            Some(TestFailure::ParentUnavailable)
        );
    }

    #[test]
    fn attach_child_rejects_terminal_parents() {
        for fail_parent in [false, true] {
            let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
            let parent = registry
                .create_parent(ParentJobSpec::new("rack", "parent", tracing::Span::none()))
                .expect("parent should be created");
            let parent_id = parent.id().clone();
            if fail_parent {
                parent.fail(JobFailure::new(TestFailure::Explicit, "failed"));
            } else {
                parent.complete("done", "{}");
            }

            let result = registry.create_child(&parent_id, test_spec("rack", "child"));
            assert!(result.is_err());
            assert!(registry.get(&parent_id).unwrap().child_job_ids.is_empty());
        }
    }

    #[test]
    fn remove_queued_leaf_refuses_child_linked_to_parent() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let parent = registry
            .create_parent(ParentJobSpec::new("rack", "parent", tracing::Span::none()))
            .expect("parent should be created");
        let parent_id = parent.id().clone();
        let child = registry
            .create_child(&parent_id, test_spec("rack", "child"))
            .unwrap();
        let child_id = child.id().clone();

        assert!(!registry.remove_queued_leaf(&child_id));
        assert!(registry.get(&child_id).is_some());
    }

    #[test]
    fn capacity_reopens_after_expired_terminal_cleanup() {
        let registry = Arc::new(JobRegistry::<TestDomain>::with_capacity(TTL, 1));
        let pending = registry.create_leaf(test_spec("rack", "a")).unwrap();
        let id = pending.id().clone();
        pending.complete("done", "{}");

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

    #[test]
    fn workflow_type_on_leaf_spec_is_stored_on_snapshot() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let pending = registry
            .create_leaf(
                JobSpec::new("rack", "node", "Queued", tracing::Span::none())
                    .with_workflow_type(JobType::FirmwareUpdate),
            )
            .unwrap();
        let snapshot = registry.get(pending.id()).unwrap();
        assert_eq!(snapshot.workflow_type, Some(JobType::FirmwareUpdate));
    }

    #[test]
    fn workflow_type_on_parent_spec_is_stored_on_snapshot() {
        let registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let parent = registry
            .create_parent(
                ParentJobSpec::new("rack", "parent", tracing::Span::none())
                    .with_workflow_type(JobType::SwitchCertificate),
            )
            .unwrap();
        let parent_id = parent.id().clone();
        let _child = registry
            .create_child(&parent_id, test_spec("rack", "node"))
            .unwrap();
        let snapshot = registry.get(&parent_id).unwrap();
        assert_eq!(snapshot.workflow_type, Some(JobType::SwitchCertificate));
    }

    #[test]
    fn created_observer_panic_does_not_poison_registry_lock() {
        let mut registry = Arc::new(JobRegistry::<TestDomain>::new(TTL));
        let observed_job_id = Arc::new(std::sync::Mutex::new(None));
        let observer_job_id = observed_job_id.clone();
        Arc::get_mut(&mut registry)
            .unwrap()
            .set_created_observer(Arc::new(move |snapshot| {
                *observer_job_id.lock().unwrap() = Some(snapshot.job_id.clone());
                panic!("created observer panic");
            }));

        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = registry.create_leaf(test_spec("rack", "node"));
        }));
        assert!(panicked.is_err());
        let observed_job_id = observed_job_id.lock().unwrap().clone().unwrap();
        let snapshot = registry.get(&observed_job_id).unwrap();
        assert_failed(snapshot.state, TestFailure::Dropped, "job abandoned");

        // A second create after the observer panic must still acquire the lock.
        Arc::get_mut(&mut registry)
            .unwrap()
            .set_created_observer(Arc::new(|_| {}));
        let pending = registry
            .create_leaf(test_spec("rack", "other"))
            .expect("registry write lock must remain usable after observer panic");
        assert!(registry.get(pending.id()).is_some());
    }
}
