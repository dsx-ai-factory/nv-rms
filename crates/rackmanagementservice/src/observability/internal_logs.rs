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

//! Audited bridge for one numeric OpenTelemetry internal diagnostic.

use std::marker::PhantomData;
use std::sync::Arc;

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use super::trace_metrics::TraceMetrics;

const OTLP_TARGET: &str = "opentelemetry-otlp";
const GRPC_PARTIAL_SUCCESS_EVENT: &str = "TonicTracesClient.PartialSuccess";

struct OtlpInternalMetricsLayer<S> {
    metrics: Arc<TraceMetrics>,
    _subscriber: PhantomData<fn(S)>,
}

/// Observe only the audited diagnostic without enabling other layers' events.
pub(crate) fn otlp_internal_metrics_layer<S>(metrics: Arc<TraceMetrics>) -> impl Layer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    OtlpInternalMetricsLayer {
        metrics,
        _subscriber: PhantomData,
    }
    .with_filter(filter_fn(|metadata| {
        metadata.is_event()
            && metadata.target() == OTLP_TARGET
            && metadata.name() == GRPC_PARTIAL_SUCCESS_EVENT
    }))
}

impl<S: Subscriber> Layer<S> for OtlpInternalMetricsLayer<S> {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let metadata = event.metadata();
        if metadata.target() != OTLP_TARGET || metadata.name() != GRPC_PARTIAL_SUCCESS_EVENT {
            return;
        }

        let mut visitor = RejectedSpansVisitor::default();
        event.record(&mut visitor);
        self.metrics.reject_spans("grpc", visitor.rejected_spans);
    }
}

#[derive(Default)]
struct RejectedSpansVisitor {
    rejected_spans: u64,
}

impl Visit for RejectedSpansVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "rejected_spans" {
            self.rejected_spans = u64::try_from(value).unwrap_or(0);
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "rejected_spans" {
            self.rejected_spans = value;
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {
        // Collector-controlled text is intentionally ignored.
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::filter::{EnvFilter, FilterExt as _};
    use tracing_subscriber::prelude::*;

    use super::*;

    #[test]
    fn captures_only_numeric_grpc_partial_success_data() {
        let debug_count = tracing::field::debug(7_i64);
        for (count, expected) in [
            (&3_i64 as &dyn tracing::field::Value, 3_u64),
            (&0_i64, 0),
            (&-1_i64, 0),
            (&4_u64, 4),
            (&"9", 0),
            (&"collector-controlled secret", 0),
            (&debug_count, 0),
        ] {
            let metrics = Arc::new(TraceMetrics::new().unwrap());
            let subscriber =
                tracing_subscriber::registry().with(otlp_internal_metrics_layer(metrics.clone()));

            tracing::subscriber::with_default(subscriber, || {
                tracing::warn!(
                    name: GRPC_PARTIAL_SUCCESS_EVENT,
                    target: OTLP_TARGET,
                    name = GRPC_PARTIAL_SUCCESS_EVENT,
                    rejected_spans = count,
                    error_message = "collector-controlled secret"
                );
            });

            assert_eq!(metrics.total_rejected(), expected);
        }
    }

    #[test]
    fn filters_before_evaluating_disabled_events_without_suppressing_other_layers() {
        // Callsite interest is process-wide, so parallel tests' subscribers can
        // cause disabled event arguments to be evaluated. Isolate this scenario.
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::internal_logs::tests::",
            "filters_before_evaluating_disabled_events_without_suppressing_other_layers"
        )) {
            return;
        }

        struct EventCounter(Arc<AtomicUsize>);

        impl<S: Subscriber> Layer<S> for EventCounter {
            fn on_event(&self, _event: &Event<'_>, _context: Context<'_, S>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let metrics = Arc::new(TraceMetrics::new().unwrap());
        let event_count = Arc::new(AtomicUsize::new(0));
        let argument_evaluations = AtomicUsize::new(0);
        let stdout_layer = EventCounter(event_count.clone()).with_filter(
            EnvFilter::new("warn").and(filter_fn(|metadata| metadata.target() != OTLP_TARGET)),
        );
        let provider = SdkTracerProvider::builder().build();
        let otel_layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("internal-log-filter-test"))
            .with_filter(filter_fn(|metadata| metadata.is_span()));
        let subscriber = tracing_subscriber::registry()
            .with(stdout_layer)
            .with(otlp_internal_metrics_layer(metrics.clone()))
            .with(otel_layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(
                value = argument_evaluations.fetch_add(1, Ordering::Relaxed),
                "suppressed debug event"
            );
            tracing::trace!(
                value = argument_evaluations.fetch_add(1, Ordering::Relaxed),
                "suppressed trace event"
            );
            tracing::warn!(
                target: OTLP_TARGET,
                value = argument_evaluations.fetch_add(1, Ordering::Relaxed),
                "unaudited diagnostic"
            );
            tracing::warn!("visible application warning");

            let request = tracing::info_span!("request");
            assert!(request.context().span().span_context().is_valid());
            request.in_scope(|| {
                tracing::warn!(
                    name: GRPC_PARTIAL_SUCCESS_EVENT,
                    target: OTLP_TARGET,
                    rejected_spans = 2_i64,
                    error_message = "collector-controlled secret"
                );
            });
        });

        assert_eq!(argument_evaluations.load(Ordering::Relaxed), 0);
        assert_eq!(event_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.total_rejected(), 2);
    }
}
