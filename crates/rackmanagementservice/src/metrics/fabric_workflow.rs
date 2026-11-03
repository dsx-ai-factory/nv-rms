/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Timings and progress logs for managed fabric reconciliation.
//! Metric labels describe fixed phases and outcomes; logs inherit job context.

use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use prometheus::{HistogramOpts, HistogramVec, IntGauge, Registry};
use tokio_util::sync::CancellationToken;

const DURATION_BUCKETS: &[f64] = &[
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0,
];

static METRICS: OnceLock<WorkflowMetrics> = OnceLock::new();

/// Whole-workflow and major-phase timing scopes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// Complete worker lifetime, including failure recovery.
    Workflow,

    /// Initial drift classification across the owned inventory.
    Inspection,

    /// Controller shutdown, static staging, and shared-link preparation.
    Preparation,

    /// Concurrent activation and convergence of all fabric groups.
    Activation,

    /// Final link and selected-primary verification.
    Verification,

    /// Single-fabric status collection after a full-capacity target succeeds.
    Readback,

    /// Failure compensation after dispatched device operations finish.
    Recovery,
}

impl Stage {
    fn label(self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::Inspection => "inspection",
            Self::Preparation => "preparation",
            Self::Activation => "activation",
            Self::Verification => "verification",
            Self::Readback => "readback",
            Self::Recovery => "recovery",
        }
    }
}

struct WorkflowMetrics {
    duration: HistogramVec,
    active: IntGauge,
}

impl WorkflowMetrics {
    fn new() -> Result<Self, prometheus::Error> {
        Ok(Self {
            duration: HistogramVec::new(
                HistogramOpts::new(
                    "fabric_reconciliation_duration_seconds",
                    "Managed fabric reconciliation elapsed time by stage and outcome",
                )
                .buckets(DURATION_BUCKETS.to_vec()),
                &["stage", "outcome"],
            )?,
            active: IntGauge::new(
                "fabric_reconciliations_active",
                "Managed fabric reconciliation workers currently running, including recovery",
            )?,
        })
    }
}

/// Records elapsed time even when an operation is dropped or unwinds.
/// A workflow scope also retains the active-worker gauge through recovery.
#[must_use]
pub(crate) struct StageTimer<'a> {
    stage: Stage,
    started: Instant,
    outcome: &'static str,
    metrics: Option<&'a WorkflowMetrics>,
    span: tracing::Span,
    dispatcher: tracing::Dispatch,
}

impl<'a> StageTimer<'a> {
    /// Starts a timing scope under the worker's existing job tracing span.
    pub(crate) fn start(stage: Stage) -> Self {
        // Initialization errors are reported by register; workers can also run
        // in tests without initializing the process-wide metrics registry.
        let metrics = METRICS.get();

        if stage == Stage::Workflow
            && let Some(metrics) = metrics
        {
            metrics.active.inc();
        }

        tracing::info!(stage = stage.label(), "fabric stage started");

        Self {
            stage,
            started: Instant::now(),
            outcome: "interrupted",
            metrics,
            span: tracing::Span::current(),
            dispatcher: tracing::dispatcher::get_default(Clone::clone),
        }
    }

    /// Marks an explicit result; unfinished scopes retain the interrupted outcome.
    /// Success takes precedence over a cancellation request. Otherwise, `cancelled`
    /// records that cancellation was requested, not that it caused the error.
    pub(crate) fn finish(mut self, succeeded: bool, cancelled: bool) {
        self.outcome = if succeeded {
            "success"
        } else if cancelled {
            "cancelled"
        } else {
            "failure"
        };
    }
}

impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        let elapsed_seconds = self.started.elapsed().as_secs_f64();

        if let Some(metrics) = self.metrics {
            // The collector declares exactly these two labels, satisfying the
            // cardinality required by `with_label_values`.
            metrics
                .duration
                .with_label_values(&[self.stage.label(), self.outcome])
                .observe(elapsed_seconds);

            if self.stage == Stage::Workflow {
                metrics.active.dec();
            }
        }

        // A future's scoped subscriber is installed during polling, not drop.
        // Restore it and the captured span so aborted tasks retain their context.
        tracing::dispatcher::with_default(&self.dispatcher, || {
            let _entered = self.span.enter();

            if self.outcome == "success" {
                tracing::info!(
                    stage = self.stage.label(),
                    outcome = self.outcome,
                    elapsed_seconds,
                    "fabric stage finished"
                );
            } else {
                tracing::warn!(
                    stage = self.stage.label(),
                    outcome = self.outcome,
                    elapsed_seconds,
                    "fabric stage finished"
                );
            }
        });
    }
}

/// Registers shared worker collectors with each RMS registry created at startup.
pub(super) fn register(registry: &Registry) -> Result<(), prometheus::Error> {
    let metrics = match METRICS.get() {
        Some(metrics) => metrics,
        None => {
            let metrics = WorkflowMetrics::new()?;

            METRICS.get_or_init(|| metrics)
        }
    };

    registry.register(Box::new(metrics.duration.clone()))?;
    registry.register(Box::new(metrics.active.clone()))
}

/// Logs waits without dropping, cancelling, or restarting the observed operation.
/// Errors remain unchanged and are not copied into telemetry payloads.
/// Pass the wait-log interval from the service's validated `WorkflowTimings`.
pub(crate) async fn observe<F, T, E>(
    stage: Stage,
    cancellation: &CancellationToken,
    wait_log_interval: Duration,
    operation: F,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let timer = StageTimer::start(stage);

    let mut progress = tokio::time::interval_at(
        tokio::time::Instant::now() + wait_log_interval,
        wait_log_interval,
    );

    progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(operation);

    let result = loop {
        tokio::select! {
            biased;
            result = &mut operation => break result,
            _ = progress.tick() => {
                tracing::info!(
                    stage = stage.label(),
                    elapsed_seconds = timer.started.elapsed().as_secs_f64(),
                    cancellation_requested = cancellation.is_cancelled(),
                    "fabric stage waiting"
                );
            }
        }
    };

    timer.finish(result.is_ok(), cancellation.is_cancelled());

    result
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::config::{RmsWorkflowConfig, WorkflowTimings};

    use figment::Figment;
    use figment::providers::{Format, Toml};
    use prometheus::TextEncoder;
    use tracing::Instrument;
    use tracing::instrument::WithSubscriber;

    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for LogBuffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn timers_record_bounded_outcomes_and_release_active_workers_on_drop() {
        if crate::observability::run_test_in_subprocess(concat!(
            "metrics::fabric_workflow::tests::",
            "timers_record_bounded_outcomes_and_release_active_workers_on_drop"
        )) {
            return;
        }

        let registry = Registry::new_custom(Some("rms".to_owned()), None).unwrap();
        register(&registry).unwrap();

        assert!(matches!(
            register(&registry),
            Err(prometheus::Error::AlreadyReg)
        ));

        let metrics = METRICS.get().unwrap();

        for (result, outcome) in [
            (Some((true, true)), "success"),
            (Some((false, false)), "failure"),
            (Some((false, true)), "cancelled"),
            (None, "interrupted"),
        ] {
            let before_start = Instant::now();
            let timer = StageTimer::start(Stage::Workflow);
            let after_start = Instant::now();

            assert_eq!(metrics.active.get(), 1);

            // Use the real clock, matching the production timer. Bounds are
            // measured around the scope, so scheduler delays remain valid.
            std::thread::sleep(Duration::from_millis(2));

            let minimum_seconds = after_start.elapsed().as_secs_f64();

            if let Some((succeeded, cancelled)) = result {
                timer.finish(succeeded, cancelled);
            } else {
                drop(timer);
            }

            let maximum_seconds = before_start.elapsed().as_secs_f64();

            assert_eq!(metrics.active.get(), 0);

            let histogram = metrics.duration.with_label_values(&["workflow", outcome]);

            assert_eq!(histogram.get_sample_count(), 1);

            assert!(
                (minimum_seconds..=maximum_seconds).contains(&histogram.get_sample_sum()),
                "recorded duration must match the scope's real elapsed seconds"
            );
        }

        let text = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .unwrap();

        for outcome in ["success", "failure", "cancelled", "interrupted"] {
            assert!(text.contains(&format!(
                "rms_fabric_reconciliation_duration_seconds_count{{outcome=\"{outcome}\",stage=\"workflow\"}} 1"
            )), "{text}");
        }

        assert!(text.contains("rms_fabric_reconciliations_active 0"));

        assert!(!text.contains("node_id"));

        assert!(!text.contains("job_id"));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_logs_preserve_job_context_and_do_not_cancel_or_expose_errors() {
        if crate::observability::run_test_in_subprocess(concat!(
            "metrics::fabric_workflow::tests::",
            "wait_logs_preserve_job_context_and_do_not_cancel_or_expose_errors"
        )) {
            return;
        }

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&buffer);

        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || LogBuffer(Arc::clone(&writer)))
            .finish();

        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let config: RmsWorkflowConfig =
            Figment::from(Toml::string("fabric_config_wait_log_interval = 10"))
                .extract()
                .unwrap();

        let timings = WorkflowTimings::try_from(&config).unwrap();

        let result = async {
            observe(
                Stage::Activation,
                &cancellation,
                timings.fabric_config_wait_log_interval,
                async {
                    tokio::time::sleep(Duration::from_secs(61)).await;
                    Err::<(), _>("private error body")
                },
            )
            .instrument(tracing::info_span!("test_job", job_id = "job-test"))
            .await
        }
        .with_subscriber(subscriber)
        .await;

        assert_eq!(result, Err("private error body"));

        let logs = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();

        assert_eq!(logs.matches("fabric stage waiting").count(), 6);

        assert!(logs.contains("job-test"));

        assert!(logs.contains("stage=\"activation\""));

        assert!(logs.contains("cancellation_requested=true"));

        assert!(logs.contains("outcome=\"cancelled\""));

        assert!(!logs.contains("private error body"));
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_observed_operation_is_logged_as_interrupted() {
        if crate::observability::run_test_in_subprocess(concat!(
            "metrics::fabric_workflow::tests::",
            "dropped_observed_operation_is_logged_as_interrupted"
        )) {
            return;
        }

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&buffer);

        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || LogBuffer(Arc::clone(&writer)))
            .finish();

        let cancellation = CancellationToken::new();

        let result = async {
            tokio::time::timeout(
                Duration::from_secs(1),
                observe(
                    Stage::Preparation,
                    &cancellation,
                    WorkflowTimings::default().fabric_config_wait_log_interval,
                    std::future::pending::<Result<(), ()>>(),
                )
                .instrument(tracing::info_span!("test_job", job_id = "dropped-job")),
            )
            .await
        }
        .with_subscriber(subscriber)
        .await;

        assert!(result.is_err());

        let logs = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();

        assert!(logs.contains("outcome=\"interrupted\""));

        assert!(logs.contains("dropped-job"));

        assert!(!logs.contains("fabric stage waiting"));
    }

    #[tokio::test]
    async fn aborted_task_retains_its_scoped_subscriber_and_job_span() {
        if crate::observability::run_test_in_subprocess(concat!(
            "metrics::fabric_workflow::tests::",
            "aborted_task_retains_its_scoped_subscriber_and_job_span"
        )) {
            return;
        }

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&buffer);

        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || LogBuffer(Arc::clone(&writer)))
            .finish();

        let (started, ready) = tokio::sync::oneshot::channel();

        let task = tokio::spawn(
            async move {
                observe(
                    Stage::Preparation,
                    &CancellationToken::new(),
                    WorkflowTimings::default().fabric_config_wait_log_interval,
                    async move {
                        started.send(()).unwrap();
                        std::future::pending::<Result<(), ()>>().await
                    },
                )
                .instrument(tracing::info_span!("test_job", job_id = "aborted-job"))
                .await
            }
            .with_subscriber(subscriber),
        );

        // Abort only after the timer captured the task's subscriber and span.
        ready.await.unwrap();
        task.abort();

        assert!(task.await.unwrap_err().is_cancelled());

        let logs = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();

        let completion = logs
            .lines()
            .find(|line| line.contains("fabric stage finished"))
            .expect("aborted task must log completion to its scoped subscriber");

        assert!(completion.contains("outcome=\"interrupted\""));

        assert!(completion.contains("aborted-job"));
    }
}
