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

//! Bounded span storage and loss accounting shared by the worker and shutdown caller.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use opentelemetry_sdk::trace::SpanData;
use tokio::sync::Notify;

use super::trace_metrics::{TraceMetrics, drop_reason};

#[derive(Default)]
struct QueueState {
    pending: VecDeque<SpanData>,
    in_flight: usize,
    accepting: bool,
    stopped: bool,
    export_failure_active: bool,
}

enum ExportDiagnostic {
    Failing { span_count: usize },
    Recovered,
}

/// The waiting queue and the currently exporting batch share accounting with
/// the shutdown caller. No span can escape this accounting when the async
/// worker is delayed past the synchronous shutdown deadline.
/// Admission, transfer to in-flight, and timeout claims must be atomic with
/// respect to each other. A channel plus separate atomic counters would leave
/// gaps between those operations where shutdown could miss or double-count a
/// span; replacing this lock requires an equivalent ownership protocol.
pub(super) struct SpanQueue {
    state: Mutex<QueueState>,
    available: Notify,
    stopped: Notify,
    capacity: usize,
    batch_size: usize,
    metrics: Arc<TraceMetrics>,
    protocol: &'static str,
}

impl SpanQueue {
    pub(super) fn new(
        capacity: usize,
        batch_size: usize,
        metrics: Arc<TraceMetrics>,
        protocol: &'static str,
    ) -> Self {
        Self {
            state: Mutex::new(QueueState {
                pending: VecDeque::with_capacity(capacity),
                accepting: true,
                ..QueueState::default()
            }),
            available: Notify::new(),
            stopped: Notify::new(),
            capacity,
            batch_size,
            metrics,
            protocol,
        }
    }

    pub(super) async fn wait_for_batch(&self) {
        self.available.notified().await;
    }

    #[cfg(test)]
    pub(super) fn panic_while_locked(&self, already_stopped: bool, message: &'static str) -> ! {
        let mut state = self.state.lock().unwrap();
        state.stopped = already_stopped;
        std::panic::panic_any(message);
    }

    #[cfg(test)]
    pub(super) fn is_poisoned(&self) -> bool {
        self.state.is_poisoned()
    }

    fn lock_state(&self) -> Option<MutexGuard<'_, QueueState>> {
        match self.state.lock() {
            Ok(state) => Some(state),
            Err(poisoned) => {
                // A panic may interrupt a state/metric update. Do not resume
                // export from this state, even if `stopped` was already set.
                // Retained-span accounting here is necessarily best effort.
                let mut state = poisoned.into_inner();
                state.accepting = false;
                state.stopped = true;
                let pending = std::mem::take(&mut state.pending);
                let in_flight = std::mem::take(&mut state.in_flight);
                self.state.clear_poison();
                drop(state);

                self.metrics.drop_spans(
                    drop_reason::PROCESSOR_CLOSED,
                    pending.len().saturating_add(in_flight),
                );
                if in_flight > 0 {
                    self.metrics.fail_batch(self.protocol);
                }
                drop(pending);
                self.stopped.notify_one();
                // A subscriber can call back into the queue; never log while
                // holding its mutex, including on this exceptional path.
                tracing::warn!(
                    target: "rackmanagementservice::observability::batch",
                    protocol = self.protocol,
                    "OpenTelemetry span queue state was poisoned; trace export has stopped"
                );
                None
            }
        }
    }

    pub(super) fn push(&self, span: SpanData) {
        let Some(mut state) = self.lock_state() else {
            self.metrics.drop_spans(drop_reason::PROCESSOR_CLOSED, 1);
            return;
        };
        if !state.accepting {
            self.metrics.drop_spans(drop_reason::PROCESSOR_CLOSED, 1);
        } else if state.pending.len() == self.capacity {
            self.metrics.drop_spans(drop_reason::QUEUE_FULL, 1);
        } else {
            state.pending.push_back(span);
            if state.pending.len() == self.batch_size {
                self.available.notify_one();
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        self.lock_state().map_or(0, |state| state.pending.len())
    }

    pub(super) fn is_accepting(&self) -> bool {
        self.lock_state().is_some_and(|state| state.accepting)
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.lock_state().is_none_or(|state| state.stopped)
    }

    pub(super) async fn wait_until_stopped(&self) {
        if !self.is_stopped() {
            // There is only one worker. notify_one retains a permit if stop
            // races with this check, before the notification is polled.
            self.stopped.notified().await;
        }
    }

    pub(super) fn close_admission(&self) -> bool {
        let Some(mut state) = self.lock_state() else {
            return false;
        };
        std::mem::replace(&mut state.accepting, false)
    }

    pub(super) fn take_batch(&self, max: usize) -> Vec<SpanData> {
        let Some(mut state) = self.lock_state() else {
            return Vec::new();
        };
        let count = state.pending.len().min(max);
        state.in_flight = count;
        state.pending.drain(..count).collect()
    }

    pub(super) fn finish_batch(&self, failure: Option<&'static str>) {
        let Some(mut state) = self.lock_state() else {
            return;
        };
        let diagnostic = self.finish_batch_locked(&mut state, failure);
        drop(state);
        self.log_export_diagnostic(diagnostic);
    }

    /// The processor owns delivery outcomes, including cancellation before the
    /// exporter returns. Claim each active batch once, even during shutdown.
    fn finish_batch_locked(
        &self,
        state: &mut QueueState,
        failure: Option<&'static str>,
    ) -> Option<ExportDiagnostic> {
        let span_count = std::mem::take(&mut state.in_flight);
        if span_count == 0 {
            return None;
        }
        let was_failing = std::mem::replace(&mut state.export_failure_active, failure.is_some());
        if let Some(reason) = failure {
            self.metrics.drop_spans(reason, span_count);
            self.metrics.fail_batch(self.protocol);
            (!was_failing).then_some(ExportDiagnostic::Failing { span_count })
        } else {
            was_failing.then_some(ExportDiagnostic::Recovered)
        }
    }

    fn log_export_diagnostic(&self, diagnostic: Option<ExportDiagnostic>) {
        match diagnostic {
            Some(ExportDiagnostic::Failing { span_count }) => tracing::warn!(
                    target: "rackmanagementservice::observability::batch",
                span_count,
                protocol = self.protocol,
                "OpenTelemetry span export is failing; trace batches may be lost"
            ),
            Some(ExportDiagnostic::Recovered) => tracing::info!(
                target: "rackmanagementservice::observability::batch",
                protocol = self.protocol,
                "OpenTelemetry span export recovered"
            ),
            None => {}
        }
    }

    /// Claims all remaining spans once. Called by the synchronous shutdown
    /// deadline as well as worker exit, so the final counters do not depend on
    /// Tokio scheduling. Later batch completion sees zero in-flight spans.
    pub(super) fn stop(&self, reason: &'static str) {
        let Some(mut state) = self.lock_state() else {
            return;
        };
        state.accepting = false;
        if !state.stopped {
            state.stopped = true;
            let diagnostic = self.finish_batch_locked(&mut state, Some(reason));
            self.metrics.drop_spans(reason, state.pending.len());
            let pending = std::mem::take(&mut state.pending);
            drop(state);
            drop(pending);
            self.stopped.notify_one();
            self.log_export_diagnostic(diagnostic);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use futures::FutureExt;
    use opentelemetry_sdk::testing::trace::new_test_export_span_data;

    use super::*;

    #[test]
    fn notifications_follow_batch_threshold_and_rearm_after_drain() {
        for batch_size in [1, 2, 4] {
            let metrics = Arc::new(TraceMetrics::new().unwrap());
            let queue = SpanQueue::new(4, batch_size, metrics.clone(), "grpc");
            for _ in 0..2 {
                for offered in 1..=5 {
                    queue.push(new_test_export_span_data());
                    assert_eq!(
                        queue.wait_for_batch().now_or_never().is_some(),
                        offered == batch_size,
                        "batch_size={batch_size}, offered={offered}"
                    );
                }
                assert_eq!(queue.take_batch(4).len(), 4);
                queue.finish_batch(None);
            }
            assert_eq!(metrics.dropped_for(drop_reason::QUEUE_FULL), 2);
            assert!(queue.close_admission());
            queue.push(new_test_export_span_data());
            assert!(queue.wait_for_batch().now_or_never().is_none());
            assert_eq!(metrics.dropped_for(drop_reason::PROCESSOR_CLOSED), 1);
            assert_eq!(metrics.total_dropped(), 3);
        }
    }

    #[derive(Default)]
    struct Diagnostics {
        events: Arc<Mutex<Vec<String>>>,
        queue: Option<Arc<SpanQueue>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Diagnostics {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            assert_eq!(
                event.metadata().target(),
                "rackmanagementservice::observability::batch"
            );
            struct Fields(String);
            impl tracing::field::Visit for Fields {
                fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
                    use std::fmt::Write;
                    write!(&mut self.0, " {}={value:?}", field.name()).unwrap();
                }
            }
            if let Some(queue) = &self.queue {
                drop(queue.state.try_lock().expect("diagnostic holds queue lock"));
                assert!(!queue.is_accepting());
                assert_eq!(queue.len(), 0);
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

    #[test]
    fn poisoned_queue_fails_closed_and_logs_without_holding_its_lock() {
        use tracing_subscriber::prelude::*;

        if crate::observability::run_test_in_subprocess(concat!(
            "observability::span_queue::tests::",
            "poisoned_queue_fails_closed_and_logs_without_holding_its_lock"
        )) {
            return;
        }

        for (case, offered, in_flight, poison, already_stopped) in [
            ("healthy", 3, false, false, false),
            ("empty", 0, false, true, false),
            ("queued", 3, false, true, false),
            ("in_flight", 3, true, true, false),
            ("partially_stopped", 3, true, true, true),
        ] {
            let metrics = Arc::new(TraceMetrics::new().unwrap());
            let queue = Arc::new(SpanQueue::new(4, 1, metrics.clone(), "grpc"));
            for _ in 0..offered {
                queue.push(new_test_export_span_data());
            }
            if in_flight {
                assert_eq!(queue.take_batch(1).len(), 1, "{case}");
            }
            if poison {
                poison_queue(&queue, already_stopped);
            }

            let diagnostics = Diagnostics {
                queue: Some(queue.clone()),
                ..Diagnostics::default()
            };
            let events = diagnostics.events.clone();
            tracing::subscriber::with_default(
                tracing_subscriber::registry().with(diagnostics),
                || {
                    queue.push(new_test_export_span_data());
                    assert!(!queue.state.is_poisoned(), "{case}");
                    assert_eq!(queue.is_accepting(), !poison, "{case}");
                    assert_eq!(queue.len(), if poison { 0 } else { offered + 1 }, "{case}");
                    assert_eq!(
                        queue.take_batch(4).len(),
                        if poison { 0 } else { 4 },
                        "{case}"
                    );
                    queue.finish_batch(None);
                    assert_eq!(queue.close_admission(), !poison, "{case}");
                    queue.stop(drop_reason::PROCESSOR_CLOSED);
                    queue.stop(drop_reason::SHUTDOWN_TIMEOUT);
                    queue.finish_batch(Some(drop_reason::EXPORT_ERROR));
                },
            );
            assert_eq!(
                metrics.total_dropped(),
                if poison { (offered + 1) as u64 } else { 0 },
                "{case}"
            );
            assert_eq!(
                metrics.dropped_for(drop_reason::PROCESSOR_CLOSED),
                metrics.total_dropped(),
                "{case}"
            );
            assert_eq!(
                metrics.failed_batches_for("grpc"),
                u64::from(in_flight),
                "{case}"
            );
            let events = events.lock().unwrap();
            assert_eq!(events.len(), usize::from(poison), "{case}");
            if poison {
                assert!(
                    events[0].contains("span queue state was poisoned"),
                    "{case}"
                );
            }
        }
    }

    #[test]
    fn batch_failures_warn_once_recover_and_ignore_stale_completion() {
        use tracing_subscriber::prelude::*;

        // Other batch tests share these log callsites without a subscriber.
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::span_queue::tests::",
            "batch_failures_warn_once_recover_and_ignore_stale_completion"
        )) {
            return;
        }

        struct ExportEvents(Arc<Mutex<Vec<tracing::Level>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ExportEvents {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _context: tracing_subscriber::layer::Context<'_, S>,
            ) {
                assert_eq!(
                    event.metadata().target(),
                    "rackmanagementservice::observability::batch"
                );
                self.0.lock().unwrap().push(*event.metadata().level());
            }
        }

        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let queue = SpanQueue::new(4, 1, metrics.clone(), "grpc");
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(ExportEvents(events.clone()));
        tracing::subscriber::with_default(subscriber, || {
            for result in [
                Some(drop_reason::EXPORT_TIMEOUT),
                Some(drop_reason::EXPORT_ERROR),
                None,
            ] {
                queue.push(new_test_export_span_data());
                assert_eq!(queue.take_batch(1).len(), 1);
                queue.finish_batch(result);
            }
            queue.push(new_test_export_span_data());
            assert_eq!(queue.take_batch(1).len(), 1);
            queue.push(new_test_export_span_data());
            queue.stop(drop_reason::SHUTDOWN_TIMEOUT);
            // A cancelled export may become ready while shutdown claims the
            // batch. Neither late success nor failure owns its outcome now.
            queue.finish_batch(None);
            queue.finish_batch(Some(drop_reason::EXPORT_ERROR));
            queue.stop(drop_reason::PROCESSOR_CLOSED);
        });
        assert_eq!(metrics.failed_batches_for("grpc"), 3);
        assert_eq!(metrics.failed_batches_for("http_protobuf"), 0);
        assert_eq!(metrics.total_dropped(), 4);
        assert_eq!(
            *events.lock().unwrap(),
            [
                tracing::Level::WARN,
                tracing::Level::INFO,
                tracing::Level::WARN
            ]
        );
    }
}
