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

//! Bounded span batching on a Tokio export worker, with loss counted by reason.
//!
//! The OpenTelemetry Rust SDK (`opentelemetry_sdk`) defines the synchronous
//! `SpanProcessor` trait. `on_end` can run during span `Drop`, so it queues
//! spans without awaiting export. The exporter and retry timers need Tokio
//! and run on one async worker. Flush/shutdown replies use standard-library
//! channels for synchronous deadline waits; Tokio's `blocking_recv` panics
//! inside async tasks. Async callers must run `force_flush` and
//! `shutdown_with_timeout` through `spawn_blocking`, as `TracingGuard::shutdown`
//! does, so the exporter can keep running.
//!
//! In `opentelemetry_sdk` 0.32.1, the default processor has separate span and
//! control channels, but polls exports on a plain thread without Tokio. Its
//! experimental async processor supports Tokio but shares one bounded channel
//! for spans and control, so a full queue can reject shutdown. RMS uses a
//! separate control channel, reason-specific drop counters, and an atomic
//! claim of queued and in-flight losses at the synchronous shutdown deadline.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};

use opentelemetry::Context;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{Span, SpanData, SpanExporter, SpanProcessor};
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle};

use crate::config::OtlpTraceProtocol;

use super::span_queue::SpanQueue;
use super::trace_metrics::{TraceMetrics, drop_reason};

#[derive(Clone, Copy, Debug)]
pub(crate) struct BatchSettings {
    pub protocol: OtlpTraceProtocol,
    pub max_queue_size: usize,
    pub max_export_batch_size: usize,
    pub scheduled_delay: Duration,
    pub export_timeout: Duration,
}

enum Control {
    Flush(std_mpsc::SyncSender<OTelSdkResult>),
    SetResource(Resource),
    Shutdown {
        deadline: Instant,
        timeout: Duration,
        reply: std_mpsc::SyncSender<OTelSdkResult>,
    },
}

struct ShutdownRequest {
    deadline: Instant,
    timeout: Duration,
    reply: std_mpsc::SyncSender<OTelSdkResult>,
}

enum ExportOutcome {
    Finished {
        failed: bool,
    },
    Shutdown {
        request: ShutdownRequest,
        failed: bool,
    },
    ShutdownExpired {
        request: ShutdownRequest,
    },
}

/// Accounts for remaining spans even if the runtime drops the worker before
/// its first poll, or an exporter panics.
struct WorkerQueue(Arc<SpanQueue>);

impl Drop for WorkerQueue {
    fn drop(&mut self) {
        self.0.stop(drop_reason::PROCESSOR_CLOSED);
    }
}

/// A batch processor with one bounded waiting queue and at most one batch in
/// flight. Its control channel remains independent of queue capacity.
pub(crate) struct ObservableBatchSpanProcessor {
    queue: Arc<SpanQueue>,
    control_tx: mpsc::UnboundedSender<Control>,
    worker: AbortHandle,
    shutdown_requested: Arc<AtomicBool>,
    shutdown_timeout: Duration,
}

impl fmt::Debug for ObservableBatchSpanProcessor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservableBatchSpanProcessor")
            .field("accepting", &self.queue.is_accepting())
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish_non_exhaustive()
    }
}

impl ObservableBatchSpanProcessor {
    pub(crate) fn new<E>(
        exporter: E,
        settings: BatchSettings,
        shutdown_timeout: Duration,
        metrics: Arc<TraceMetrics>,
    ) -> Self
    where
        E: SpanExporter + 'static,
    {
        let protocol = match settings.protocol {
            OtlpTraceProtocol::Grpc => "grpc",
            OtlpTraceProtocol::HttpProtobuf => "http_protobuf",
        };
        let queue = Arc::new(SpanQueue::new(
            settings.max_queue_size,
            settings.max_export_batch_size,
            metrics,
            protocol,
        ));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let worker_task = tokio::spawn(run_worker(
            exporter,
            settings,
            WorkerQueue(queue.clone()),
            control_rx,
        ));
        let worker = worker_task.abort_handle();
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        tokio::spawn(supervise_worker(
            worker_task,
            shutdown_requested.clone(),
            protocol,
        ));

        Self {
            queue,
            control_tx,
            worker,
            shutdown_requested,
            shutdown_timeout,
        }
    }
}

impl SpanProcessor for ObservableBatchSpanProcessor {
    fn on_start(&self, _span: &mut Span, _context: &Context) {}

    fn on_end(&self, span: SpanData) {
        if !span.span_context.is_sampled() {
            return;
        }
        self.queue.push(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        if !self.queue.is_accepting() {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.control_tx
            .send(Control::Flush(reply_tx))
            .map_err(|_| OTelSdkError::InternalFailure("trace worker stopped".to_string()))?;
        reply_rx
            .recv_timeout(self.shutdown_timeout)
            .map_err(|_| OTelSdkError::Timeout(self.shutdown_timeout))?
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        let deadline = Instant::now() + timeout;
        if !self.queue.close_admission() {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        self.shutdown_requested.store(true, Ordering::Release);

        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        if self
            .control_tx
            .send(Control::Shutdown {
                deadline,
                timeout,
                reply: reply_tx,
            })
            .is_err()
        {
            self.queue.stop(drop_reason::PROCESSOR_CLOSED);
            return Err(OTelSdkError::InternalFailure(
                "trace worker stopped".to_string(),
            ));
        }

        match reply_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(result) => result,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                // Do not wait for the worker to observe its async timer: it may
                // not be scheduled at all before the process begins teardown.
                self.queue.stop(drop_reason::SHUTDOWN_TIMEOUT);
                self.worker.abort();
                Err(OTelSdkError::Timeout(timeout))
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                self.queue.stop(drop_reason::PROCESSOR_CLOSED);
                Err(OTelSdkError::InternalFailure(
                    "trace worker stopped".to_string(),
                ))
            }
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        let _ = self.control_tx.send(Control::SetResource(resource.clone()));
    }
}

async fn supervise_worker(
    worker: JoinHandle<()>,
    shutdown_requested: Arc<AtomicBool>,
    protocol: &'static str,
) {
    let result = worker.await;
    let reason = match result {
        Err(error) if error.is_panic() => "panic",
        _ if shutdown_requested.load(Ordering::Acquire) => return,
        Err(_) => "cancelled",
        Ok(()) => "unexpected_exit",
    };
    // Do not format JoinError: an exporter panic may contain sensitive data.
    // Observation is independent of the caller's bounded shutdown deadline.
    tracing::warn!(
        protocol,
        reason,
        "OpenTelemetry trace worker stopped unexpectedly; trace export has stopped"
    );
}

async fn run_worker<E>(
    mut exporter: E,
    settings: BatchSettings,
    worker_queue: WorkerQueue,
    mut control_rx: mpsc::UnboundedReceiver<Control>,
) where
    E: SpanExporter + 'static,
{
    let queue = &worker_queue.0;
    let mut ticker = tokio::time::interval(settings.scheduled_delay);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut flush_waiters = Vec::new();
    let mut flush_requested = false;

    loop {
        tokio::select! {
            biased;
            _ = queue.wait_until_stopped() => {
                complete_flush_waiters(&mut flush_waiters, true);
                return;
            }
            control = control_rx.recv() => {
                match control {
                    Some(Control::Flush(reply)) => {
                        flush_waiters.push(reply);
                        flush_requested = true;
                    }
                    Some(Control::SetResource(resource)) => exporter.set_resource(&resource),
                    Some(Control::Shutdown { deadline, timeout, reply }) => {
                        let failed = shutdown_worker(
                            &exporter,
                            ShutdownRequest { deadline, timeout, reply },
                            queue,
                            settings,
                            false,
                        )
                        .await;
                        complete_flush_waiters(&mut flush_waiters, failed);
                        return;
                    }
                    None => return,
                }
            }
            _ = queue.wait_for_batch() => {}
            _ = std::future::ready(()), if queue.len() >= settings.max_export_batch_size || flush_requested => {}
            _ = ticker.tick() => {
                flush_requested = queue.len() > 0;
            }
        }

        if queue.len() >= settings.max_export_batch_size || flush_requested {
            let mut failed = false;
            loop {
                if queue.is_stopped() {
                    complete_flush_waiters(&mut flush_waiters, true);
                    return;
                }
                let batch = queue.take_batch(settings.max_export_batch_size);
                if batch.is_empty() {
                    break;
                }
                match export_batch(
                    &exporter,
                    batch,
                    settings.export_timeout,
                    &mut control_rx,
                    &mut flush_waiters,
                    queue,
                )
                .await
                {
                    ExportOutcome::Finished {
                        failed: batch_failed,
                    } => failed |= batch_failed,
                    ExportOutcome::Shutdown {
                        request,
                        failed: batch_failed,
                    } => {
                        failed |= batch_failed;
                        let shutdown_failed =
                            shutdown_worker(&exporter, request, queue, settings, failed).await;
                        complete_flush_waiters(&mut flush_waiters, shutdown_failed);
                        return;
                    }
                    ExportOutcome::ShutdownExpired { request } => {
                        queue.stop(drop_reason::SHUTDOWN_TIMEOUT);
                        let _ = request
                            .reply
                            .send(Err(OTelSdkError::Timeout(request.timeout)));
                        complete_flush_waiters(&mut flush_waiters, true);
                        return;
                    }
                }
            }
            complete_flush_waiters(&mut flush_waiters, failed || queue.is_stopped());
            flush_requested = false;
        }
    }
}

async fn export_batch<E>(
    exporter: &E,
    batch: Vec<SpanData>,
    timeout: Duration,
    control_rx: &mut mpsc::UnboundedReceiver<Control>,
    flush_waiters: &mut Vec<std_mpsc::SyncSender<OTelSdkResult>>,
    queue: &SpanQueue,
) -> ExportOutcome
where
    E: SpanExporter,
{
    let export_deadline = Instant::now() + timeout;
    let export = export_while_running(exporter, batch, queue);
    tokio::pin!(export);

    loop {
        tokio::select! {
            result = &mut export => {
                let failed = result.is_err();
                queue.finish_batch(failed.then_some(drop_reason::EXPORT_ERROR));
                return ExportOutcome::Finished { failed };
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(export_deadline)) => {
                queue.finish_batch(Some(drop_reason::EXPORT_TIMEOUT));
                return ExportOutcome::Finished { failed: true };
            }
            control = control_rx.recv() => {
                match control {
                    Some(Control::Flush(reply)) => flush_waiters.push(reply),
                    Some(Control::SetResource(_)) => {
                        // Resource identity is immutable after provider construction.
                    }
                    Some(Control::Shutdown { deadline, timeout, reply }) => {
                        let request = ShutdownRequest { deadline, timeout, reply };
                        let effective_deadline = export_deadline.min(deadline);
                        tokio::select! {
                            result = &mut export => {
                                let failed = result.is_err();
                                queue.finish_batch(failed.then_some(drop_reason::EXPORT_ERROR));
                                return ExportOutcome::Shutdown { request, failed };
                            }
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(effective_deadline)) => {
                                if deadline <= export_deadline {
                                    queue.finish_batch(Some(drop_reason::SHUTDOWN_TIMEOUT));
                                    return ExportOutcome::ShutdownExpired { request };
                                }
                                queue.finish_batch(Some(drop_reason::EXPORT_TIMEOUT));
                                return ExportOutcome::Shutdown { request, failed: true };
                            }
                        }
                    }
                    None => {
                        queue.finish_batch(Some(drop_reason::PROCESSOR_CLOSED));
                        return ExportOutcome::Finished { failed: true };
                    }
                }
            }
        }
    }
}

async fn export_while_running<E: SpanExporter>(
    exporter: &E,
    batch: Vec<SpanData>,
    queue: &SpanQueue,
) -> OTelSdkResult {
    // Stop before polling another ready export, without biasing the outer
    // control/deadline selection and starving flush requests under load.
    tokio::select! {
        biased;
        _ = queue.wait_until_stopped() => Err(OTelSdkError::AlreadyShutdown),
        result = exporter.export(batch) => result,
    }
}

async fn shutdown_worker<E>(
    exporter: &E,
    request: ShutdownRequest,
    queue: &SpanQueue,
    settings: BatchSettings,
    mut failed: bool,
) -> bool
where
    E: SpanExporter,
{
    loop {
        if queue.is_stopped() {
            failed = true;
            break;
        }
        let now = Instant::now();
        if now >= request.deadline {
            queue.stop(drop_reason::SHUTDOWN_TIMEOUT);
            let _ = request
                .reply
                .send(Err(OTelSdkError::Timeout(request.timeout)));
            return true;
        }

        let batch = queue.take_batch(settings.max_export_batch_size);
        if batch.is_empty() {
            break;
        }
        let remaining = request.deadline.saturating_duration_since(now);
        let timeout = settings.export_timeout.min(remaining);
        match tokio::time::timeout(timeout, export_while_running(exporter, batch, queue)).await {
            Ok(Ok(())) => queue.finish_batch(None),
            Ok(Err(_)) => {
                queue.finish_batch(Some(drop_reason::EXPORT_ERROR));
                failed = true;
            }
            Err(_) if remaining <= settings.export_timeout => {
                queue.stop(drop_reason::SHUTDOWN_TIMEOUT);
                let _ = request
                    .reply
                    .send(Err(OTelSdkError::Timeout(request.timeout)));
                return true;
            }
            Err(_) => {
                queue.finish_batch(Some(drop_reason::EXPORT_TIMEOUT));
                failed = true;
            }
        }
    }

    let remaining = request.deadline.saturating_duration_since(Instant::now());
    let exporter_result = exporter.shutdown_with_timeout(remaining);
    failed |= exporter_result.is_err() || queue.is_stopped();
    let result = if failed {
        Err(OTelSdkError::InternalFailure(
            "one or more trace batches were not delivered".to_string(),
        ))
    } else {
        Ok(())
    };
    let _ = request.reply.send(result);
    failed
}

fn complete_flush_waiters(waiters: &mut Vec<std_mpsc::SyncSender<OTelSdkResult>>, failed: bool) {
    for waiter in waiters.drain(..) {
        let result = if failed {
            Err(OTelSdkError::InternalFailure(
                "one or more trace batches were not delivered".to_string(),
            ))
        } else {
            Ok(())
        };
        let _ = waiter.send(result);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use opentelemetry_sdk::testing::trace::new_test_export_span_data;

    use super::*;

    #[derive(Debug)]
    struct DelayedExporter {
        delay: Duration,
        exported: Arc<AtomicUsize>,
    }

    impl SpanExporter for DelayedExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            tokio::time::sleep(self.delay).await;
            self.exported.fetch_add(batch.len(), Ordering::SeqCst);
            Ok(())
        }
    }

    fn settings(queue: usize) -> BatchSettings {
        BatchSettings {
            protocol: OtlpTraceProtocol::Grpc,
            max_queue_size: queue,
            max_export_batch_size: 1,
            scheduled_delay: Duration::from_millis(1),
            export_timeout: Duration::from_secs(1),
        }
    }

    #[derive(Debug)]
    struct GatedExporter {
        gate: Arc<tokio::sync::Semaphore>,
        started: mpsc::UnboundedSender<usize>,
        exported: Arc<AtomicUsize>,
    }

    impl SpanExporter for GatedExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.started.send(batch.len()).unwrap();
            self.gate.acquire().await.unwrap().forget();
            self.exported.fetch_add(batch.len(), Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Default)]
    struct Diagnostics {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Diagnostics {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Fields(String);
            impl tracing::field::Visit for Fields {
                fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
                    use std::fmt::Write;
                    write!(&mut self.0, " {}={value:?}", field.name()).unwrap();
                }
            }
            let mut fields = Fields(event.metadata().level().to_string());
            event.record(&mut fields);
            self.events.lock().unwrap().push(fields.0);
        }
    }

    fn poison_queue(queue: &SpanQueue, already_stopped: bool) {
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                queue.panic_while_locked(already_stopped, "test queue poison");
            }))
            .is_err()
        );
        assert!(queue.is_poisoned());
    }

    #[tokio::test]
    async fn batch_threshold_and_timer_export_without_explicit_flush() {
        for (batch_size, offered, scheduled_delay) in [
            (1, 1, Duration::from_secs(60)),
            (3, 3, Duration::from_secs(60)),
            (3, 1, Duration::from_millis(10)),
        ] {
            let metrics = Arc::new(TraceMetrics::new().unwrap());
            let exported = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let (started, mut started_rx) = mpsc::unbounded_channel();
            let processor = ObservableBatchSpanProcessor::new(
                GatedExporter {
                    gate: gate.clone(),
                    started,
                    exported: exported.clone(),
                },
                BatchSettings {
                    max_export_batch_size: batch_size,
                    scheduled_delay,
                    ..settings(4)
                },
                Duration::from_secs(1),
                metrics.clone(),
            );
            tokio::task::yield_now().await;
            for _ in 0..offered {
                processor.on_end(new_test_export_span_data());
            }
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
                    .await
                    .expect("batch threshold or timer must start export"),
                Some(offered)
            );
            gate.add_permits(1);
            tokio::task::spawn_blocking(move || {
                processor.shutdown_with_timeout(Duration::from_secs(1))
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(exported.load(Ordering::SeqCst), offered);
            assert_eq!(metrics.total_dropped(), 0);
            assert_eq!(metrics.failed_batches_for("grpc"), 0);
        }
    }

    #[tokio::test]
    async fn poisoned_queue_cancels_export_and_fails_flush_or_shutdown() {
        for shutdown in [false, true] {
            let metrics = Arc::new(TraceMetrics::new().unwrap());
            let exported = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let (started, mut started_rx) = mpsc::unbounded_channel();
            let processor = ObservableBatchSpanProcessor::new(
                GatedExporter {
                    gate: gate.clone(),
                    started,
                    exported: exported.clone(),
                },
                BatchSettings {
                    export_timeout: Duration::from_secs(60),
                    ..settings(4)
                },
                Duration::from_secs(60),
                metrics.clone(),
            );
            processor.on_end(new_test_export_span_data());
            assert_eq!(started_rx.recv().await, Some(1));
            processor.on_end(new_test_export_span_data());
            processor.on_end(new_test_export_span_data());
            let (reply, response) = std_mpsc::sync_channel(1);
            let control = if shutdown {
                assert!(processor.queue.close_admission());
                processor.shutdown_requested.store(true, Ordering::Release);
                Control::Shutdown {
                    deadline: Instant::now() + Duration::from_secs(60),
                    timeout: Duration::from_secs(60),
                    reply,
                }
            } else {
                Control::Flush(reply)
            };
            processor.control_tx.send(control).unwrap();
            gate.add_permits(1);
            // A received or still-queued control request must fail when
            // poison closes the queue during the next export.
            assert_eq!(started_rx.recv().await, Some(1));
            poison_queue(&processor.queue, shutdown);
            processor.on_end(new_test_export_span_data());
            // Both stop and export are ready before this current-thread
            // runtime polls the worker again. Stop must prevent delivery.
            gate.add_permits(1);
            tokio::time::timeout(Duration::from_secs(1), async {
                while !processor.worker.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("poison must cancel export before its 60-second timeout");

            assert!(
                matches!(
                    response.try_recv(),
                    Ok(Err(_)) | Err(std_mpsc::TryRecvError::Disconnected)
                ),
                "shutdown={shutdown}"
            );
            assert_eq!(exported.load(Ordering::SeqCst), 1);
            assert_eq!(metrics.dropped_for(drop_reason::PROCESSOR_CLOSED), 3);
            assert_eq!(metrics.total_dropped(), 3);
            assert_eq!(metrics.failed_batches_for("grpc"), 1);
            assert!(matches!(
                processor.force_flush(),
                Err(OTelSdkError::AlreadyShutdown)
            ));
            assert!(matches!(
                processor.shutdown_with_timeout(Duration::from_secs(1)),
                Err(OTelSdkError::AlreadyShutdown)
            ));
        }
    }

    #[test]
    fn worker_cleanup_recovers_poison_during_unwinding() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::batch::tests::",
            "worker_cleanup_recovers_poison_during_unwinding"
        )) {
            return;
        }

        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let queue = Arc::new(SpanQueue::new(4, 1, metrics.clone(), "grpc"));
        for _ in 0..3 {
            queue.push(new_test_export_span_data());
        }
        assert_eq!(queue.take_batch(1).len(), 1);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _worker_queue = WorkerQueue(queue.clone());
            // The guard poisons first; WorkerQueue then runs during this
            // same unwind and must not panic a second time in its destructor.
            queue.panic_while_locked(false, "original worker panic");
        }))
        .expect_err("original panic must remain observable");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"original worker panic"));
        assert!(!queue.is_poisoned());
        assert!(!queue.is_accepting());
        assert_eq!(queue.len(), 0);
        queue.finish_batch(Some(drop_reason::EXPORT_ERROR));
        queue.stop(drop_reason::SHUTDOWN_TIMEOUT);
        assert_eq!(metrics.dropped_for(drop_reason::PROCESSOR_CLOSED), 3);
        assert_eq!(metrics.total_dropped(), 3);
        assert_eq!(metrics.failed_batches_for("grpc"), 1);
    }

    #[tokio::test]
    async fn worker_supervision_reports_only_abnormal_exits() {
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        if crate::observability::run_test_in_subprocess(concat!(
            "observability::batch::tests::",
            "worker_supervision_reports_only_abnormal_exits"
        )) {
            return;
        }

        for shutdown in [false, true] {
            for outcome in ["completed", "cancelled", "panic"] {
                let worker = tokio::spawn(async move {
                    match outcome {
                        "panic" => panic!("private exporter panic payload"),
                        "cancelled" => std::future::pending::<()>().await,
                        _ => {}
                    }
                });
                if outcome == "cancelled" {
                    worker.abort();
                }
                let diagnostics = Diagnostics::default();
                let events = diagnostics.events.clone();
                supervise_worker(worker, Arc::new(AtomicBool::new(shutdown)), "grpc")
                    .with_subscriber(tracing_subscriber::registry().with(diagnostics))
                    .await;
                let events = events.lock().unwrap();
                let expected_warning = !shutdown || outcome == "panic";
                assert_eq!(
                    events.len(),
                    usize::from(expected_warning),
                    "{outcome}, shutdown={shutdown}"
                );
                if expected_warning {
                    assert!(events[0].contains("trace worker stopped unexpectedly"));
                    assert!(events[0].contains(if outcome == "completed" {
                        "unexpected_exit"
                    } else {
                        outcome
                    }));
                    assert!(!events[0].contains("private exporter panic payload"));
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_control_is_not_rejected_by_a_full_span_queue() {
        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let exported = Arc::new(AtomicUsize::new(0));
        let (started, mut started_rx) = mpsc::unbounded_channel();
        let processor = Arc::new(ObservableBatchSpanProcessor::new(
            GatedExporter {
                gate: Arc::new(tokio::sync::Semaphore::new(0)),
                started,
                exported: exported.clone(),
            },
            settings(1),
            Duration::from_millis(50),
            metrics.clone(),
        ));

        processor.on_end(new_test_export_span_data());
        assert_eq!(started_rx.recv().await, Some(1));
        processor.on_end(new_test_export_span_data());
        processor.on_end(new_test_export_span_data());

        let shutdown_processor = processor.clone();
        let result = tokio::task::spawn_blocking(move || {
            shutdown_processor.shutdown_with_timeout(Duration::from_millis(50))
        })
        .await
        .unwrap();

        assert!(result.is_err());
        assert_eq!(metrics.dropped_for(drop_reason::QUEUE_FULL), 1);
        assert_eq!(metrics.dropped_for(drop_reason::SHUTDOWN_TIMEOUT), 2);
        assert_eq!(metrics.failed_batches_for("grpc"), 1);
        assert_eq!(exported.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sustained_overload_keeps_only_the_queue_and_one_batch() {
        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let exported = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (started, mut started_rx) = mpsc::unbounded_channel();
        let processor = Arc::new(ObservableBatchSpanProcessor::new(
            GatedExporter {
                gate: gate.clone(),
                started,
                exported: exported.clone(),
            },
            settings(4),
            Duration::from_millis(50),
            metrics.clone(),
        ));
        let mut offered = 1;
        processor.on_end(new_test_export_span_data());
        assert_eq!(started_rx.recv().await, Some(1));

        for _ in 0..20 {
            for _ in 0..100 {
                processor.on_end(new_test_export_span_data());
                offered += 1;
            }
            let retained =
                offered - exported.load(Ordering::SeqCst) - metrics.total_dropped() as usize;
            assert_eq!(retained, 5, "queue capacity plus one in-flight batch");
            gate.add_permits(1);
            assert_eq!(started_rx.recv().await, Some(1));
        }

        let shutdown_processor = processor.clone();
        let result = tokio::task::spawn_blocking(move || {
            shutdown_processor.shutdown_with_timeout(Duration::from_millis(50))
        })
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(
            offered,
            exported.load(Ordering::SeqCst) + metrics.total_dropped() as usize,
        );
        assert_eq!(metrics.dropped_for(drop_reason::SHUTDOWN_TIMEOUT), 4);
    }

    #[test]
    fn shutdown_accounts_queued_spans_before_the_worker_is_polled() {
        check_shutdown_with_stalled_runtime(false);
    }

    #[test]
    fn shutdown_accounts_inflight_spans_while_the_runtime_is_stalled() {
        check_shutdown_with_stalled_runtime(true);
    }

    #[test]
    fn worker_loss_accounts_for_spans_and_closes_admission() {
        for (case, start_export, panic_exporter) in [
            ("before_first_poll", false, false),
            ("runtime_dropped", true, false),
            ("exporter_panicked", true, true),
        ] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let metrics = Arc::new(TraceMetrics::new().unwrap());
            let exported = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let (started, mut started_rx) = mpsc::unbounded_channel();
            let processor = {
                let _entered = runtime.enter();
                ObservableBatchSpanProcessor::new(
                    GatedExporter {
                        gate: gate.clone(),
                        started,
                        exported: exported.clone(),
                    },
                    settings(4),
                    Duration::from_secs(1),
                    metrics.clone(),
                )
            };
            processor.on_end(new_test_export_span_data());
            if start_export {
                assert_eq!(runtime.block_on(started_rx.recv()), Some(1), "{case}");
            }
            processor.on_end(new_test_export_span_data());
            processor.on_end(new_test_export_span_data());

            if panic_exporter {
                // Closing the gate makes the test exporter panic during export.
                gate.close();
                runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while !processor.worker.is_finished() {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("panicked worker must finish");
                });
            }
            drop(runtime);

            assert_eq!(
                metrics.dropped_for(drop_reason::PROCESSOR_CLOSED),
                3,
                "{case}"
            );
            assert_eq!(metrics.total_dropped(), 3, "{case}");
            assert_eq!(
                metrics.failed_batches_for("grpc"),
                u64::from(start_export),
                "{case}"
            );
            assert_eq!(exported.load(Ordering::SeqCst), 0, "{case}");
            processor.on_end(new_test_export_span_data());
            assert_eq!(
                metrics.dropped_for(drop_reason::PROCESSOR_CLOSED),
                4,
                "{case}"
            );
            assert!(
                matches!(processor.force_flush(), Err(OTelSdkError::AlreadyShutdown)),
                "{case}"
            );
            assert!(
                matches!(
                    processor.shutdown_with_timeout(Duration::from_secs(1)),
                    Err(OTelSdkError::AlreadyShutdown)
                ),
                "{case}"
            );
            assert_eq!(metrics.total_dropped(), 4, "{case}");
            assert_eq!(
                metrics.failed_batches_for("grpc"),
                u64::from(start_export),
                "{case}"
            );
        }
    }

    fn check_shutdown_with_stalled_runtime(start_export: bool) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let exported = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (started, mut started_rx) = mpsc::unbounded_channel();
        let processor = {
            let _entered = runtime.enter();
            ObservableBatchSpanProcessor::new(
                GatedExporter {
                    gate: gate.clone(),
                    started,
                    exported: exported.clone(),
                },
                settings(4),
                Duration::from_millis(20),
                metrics.clone(),
            )
        };
        processor.on_end(new_test_export_span_data());
        if start_export {
            assert_eq!(runtime.block_on(started_rx.recv()), Some(1));
        }
        processor.on_end(new_test_export_span_data());
        processor.on_end(new_test_export_span_data());

        // No runtime is being driven while the synchronous deadline expires.
        let started = Instant::now();
        assert!(matches!(
            processor.shutdown_with_timeout(Duration::from_millis(20)),
            Err(OTelSdkError::Timeout(_))
        ));
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(metrics.dropped_for(drop_reason::SHUTDOWN_TIMEOUT), 3);
        assert_eq!(metrics.total_dropped(), 3);
        assert_eq!(metrics.failed_batches_for("grpc"), u64::from(start_export));
        assert_eq!(exported.load(Ordering::SeqCst), 0);

        // Even when the exporter becomes ready, cancellation and cleanup may
        // neither deliver the timed-out queue nor count its spans again.
        gate.add_permits(10);
        runtime.block_on(async {
            while !processor.worker.is_finished() {
                tokio::task::yield_now().await;
            }
        });
        assert_eq!(metrics.total_dropped(), 3);
        assert_eq!(metrics.dropped_for(drop_reason::PROCESSOR_CLOSED), 0);
        assert_eq!(metrics.failed_batches_for("grpc"), u64::from(start_export));
        assert_eq!(exported.load(Ordering::SeqCst), 0);
    }

    #[derive(Debug)]
    struct FailingExporter;

    impl SpanExporter for FailingExporter {
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            Err(OTelSdkError::InternalFailure("test export failed".into()))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_flush_drains_batches_and_keeps_admission_open() {
        for queued in [0, 1, 5] {
            let metrics = Arc::new(TraceMetrics::new().unwrap());
            let exported = Arc::new(AtomicUsize::new(0));
            let processor = Arc::new(ObservableBatchSpanProcessor::new(
                DelayedExporter {
                    delay: Duration::from_millis(1),
                    exported: exported.clone(),
                },
                BatchSettings {
                    max_export_batch_size: 2,
                    scheduled_delay: Duration::from_secs(60),
                    ..settings(8)
                },
                Duration::from_secs(1),
                metrics.clone(),
            ));
            let mut expected = 0;
            for count in [queued, 1] {
                for _ in 0..count {
                    processor.on_end(new_test_export_span_data());
                }
                expected += count;
                let flushing = processor.clone();
                tokio::task::spawn_blocking(move || flushing.force_flush())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(exported.load(Ordering::SeqCst), expected);
            }
            tokio::task::spawn_blocking(move || {
                processor.shutdown_with_timeout(Duration::from_secs(1))
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(exported.load(Ordering::SeqCst), expected);
            assert_eq!(metrics.total_dropped(), 0);
            assert_eq!(metrics.failed_batches_for("grpc"), 0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_and_shutdown_report_export_errors_once() {
        for shutdown in [false, true] {
            let metrics = Arc::new(TraceMetrics::new().unwrap());
            let processor = Arc::new(ObservableBatchSpanProcessor::new(
                FailingExporter,
                BatchSettings {
                    max_export_batch_size: 4,
                    scheduled_delay: Duration::from_secs(60),
                    ..settings(4)
                },
                Duration::from_secs(1),
                metrics.clone(),
            ));
            processor.on_end(new_test_export_span_data());
            processor.on_end(new_test_export_span_data());
            let flush_processor = processor.clone();
            let result = tokio::task::spawn_blocking(move || {
                if shutdown {
                    flush_processor.shutdown_with_timeout(Duration::from_secs(1))
                } else {
                    flush_processor.force_flush()
                }
            })
            .await
            .unwrap();
            assert!(result.is_err());
            assert_eq!(metrics.dropped_for(drop_reason::EXPORT_ERROR), 2);
            assert_eq!(metrics.failed_batches_for("grpc"), 1);
            assert_eq!(metrics.total_dropped(), 2);
            if !shutdown {
                tokio::task::spawn_blocking(move || {
                    processor.shutdown_with_timeout(Duration::from_secs(1))
                })
                .await
                .unwrap()
                .unwrap();
            }
            assert_eq!(metrics.total_dropped(), 2);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn export_timeout_is_counted_once_before_shutdown() {
        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let (started, _started_rx) = mpsc::unbounded_channel();
        let processor = Arc::new(ObservableBatchSpanProcessor::new(
            GatedExporter {
                gate: Arc::new(tokio::sync::Semaphore::new(0)),
                started,
                exported: Arc::new(AtomicUsize::new(0)),
            },
            BatchSettings {
                max_export_batch_size: 4,
                scheduled_delay: Duration::from_secs(60),
                export_timeout: Duration::from_millis(20),
                ..settings(4)
            },
            Duration::from_secs(1),
            metrics.clone(),
        ));
        processor.on_end(new_test_export_span_data());
        processor.on_end(new_test_export_span_data());
        let flush_processor = processor.clone();
        assert!(
            tokio::task::spawn_blocking(move || flush_processor.force_flush())
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(metrics.dropped_for(drop_reason::EXPORT_TIMEOUT), 2);
        assert_eq!(metrics.failed_batches_for("grpc"), 1);
        tokio::task::spawn_blocking(move || {
            processor.shutdown_with_timeout(Duration::from_secs(1))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(metrics.total_dropped(), 2);
        assert_eq!(metrics.failed_batches_for("grpc"), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn export_timeout_during_shutdown_counts_one_failed_batch() {
        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let (started, _started_rx) = mpsc::unbounded_channel();
        let processor = Arc::new(ObservableBatchSpanProcessor::new(
            GatedExporter {
                gate: Arc::new(tokio::sync::Semaphore::new(0)),
                started,
                exported: Arc::new(AtomicUsize::new(0)),
            },
            BatchSettings {
                protocol: OtlpTraceProtocol::HttpProtobuf,
                max_export_batch_size: 4,
                scheduled_delay: Duration::from_secs(60),
                export_timeout: Duration::from_millis(20),
                ..settings(4)
            },
            Duration::from_secs(1),
            metrics.clone(),
        ));
        processor.on_end(new_test_export_span_data());
        processor.on_end(new_test_export_span_data());
        let result = tokio::task::spawn_blocking(move || {
            processor.shutdown_with_timeout(Duration::from_secs(1))
        })
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(metrics.dropped_for(drop_reason::EXPORT_TIMEOUT), 2);
        assert_eq!(metrics.failed_batches_for("http_protobuf"), 1);
        assert_eq!(metrics.failed_batches_for("grpc"), 0);
        assert_eq!(metrics.total_dropped(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_shutdown_reports_success_only_after_draining() {
        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let exported = Arc::new(AtomicUsize::new(0));
        let processor = Arc::new(ObservableBatchSpanProcessor::new(
            DelayedExporter {
                delay: Duration::from_millis(5),
                exported: exported.clone(),
            },
            settings(8),
            Duration::from_secs(1),
            metrics.clone(),
        ));
        processor.on_end(new_test_export_span_data());
        processor.on_end(new_test_export_span_data());

        let shutdown_processor = processor.clone();
        let result = tokio::task::spawn_blocking(move || {
            shutdown_processor.shutdown_with_timeout(Duration::from_secs(1))
        })
        .await
        .unwrap();

        assert!(result.is_ok());
        assert_eq!(exported.load(Ordering::SeqCst), 2);
        assert_eq!(metrics.total_dropped(), 0);
        assert_eq!(metrics.failed_batches_for("grpc"), 0);
    }
}
