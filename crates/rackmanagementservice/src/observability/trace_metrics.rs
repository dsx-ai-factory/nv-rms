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

//! Low-cardinality Prometheus self-telemetry for trace delivery.

use prometheus::{IntCounterVec, Opts, Registry};

/// Fixed reasons accepted by `rms_trace_spans_dropped_total`.
pub(crate) mod drop_reason {
    pub const EXPORT_ERROR: &str = "export_error";
    pub const EXPORT_TIMEOUT: &str = "export_timeout";
    pub const PROCESSOR_CLOSED: &str = "processor_closed";
    pub const QUEUE_FULL: &str = "queue_full";
    pub const SHUTDOWN_TIMEOUT: &str = "shutdown_timeout";
}

/// Trace-delivery counters registered into RMS's existing Prometheus registry.
#[derive(Clone, Debug)]
pub struct TraceMetrics {
    dropped_spans: IntCounterVec,
    rejected_spans: IntCounterVec,
    failed_batches: IntCounterVec,
}

impl TraceMetrics {
    pub(crate) fn new() -> Result<Self, prometheus::Error> {
        let metrics = Self {
            dropped_spans: IntCounterVec::new(
                Opts::new(
                    "trace_spans_dropped_total",
                    "OpenTelemetry spans RMS could not deliver, by bounded reason",
                ),
                &["reason"],
            )?,
            rejected_spans: IntCounterVec::new(
                Opts::new(
                    "trace_spans_rejected_total",
                    "OpenTelemetry spans rejected by the Collector, by OTLP protocol",
                ),
                &["protocol"],
            )?,
            failed_batches: IntCounterVec::new(
                Opts::new(
                    "trace_export_batches_failed_total",
                    "OpenTelemetry trace batches that exhausted delivery, by OTLP protocol",
                ),
                &["protocol"],
            )?,
        };
        for reason in [
            drop_reason::EXPORT_ERROR,
            drop_reason::EXPORT_TIMEOUT,
            drop_reason::PROCESSOR_CLOSED,
            drop_reason::QUEUE_FULL,
            drop_reason::SHUTDOWN_TIMEOUT,
        ] {
            metrics.dropped_spans.with_label_values(&[reason]);
        }
        for protocol in ["grpc", "http_protobuf"] {
            metrics.rejected_spans.with_label_values(&[protocol]);
            metrics.failed_batches.with_label_values(&[protocol]);
        }
        Ok(metrics)
    }

    pub(crate) fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        for collector in [
            Box::new(self.dropped_spans.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(self.rejected_spans.clone()),
            Box::new(self.failed_batches.clone()),
        ] {
            registry.register(collector)?;
        }
        Ok(())
    }

    pub(crate) fn drop_spans(&self, reason: &'static str, count: usize) {
        if count > 0 {
            self.dropped_spans
                .with_label_values(&[reason])
                .inc_by(count as u64);
        }
    }

    pub(crate) fn reject_spans(&self, protocol: &'static str, count: u64) {
        if count > 0 {
            self.rejected_spans
                .with_label_values(&[protocol])
                .inc_by(count);
        }
    }

    pub(crate) fn fail_batch(&self, protocol: &'static str) {
        self.failed_batches.with_label_values(&[protocol]).inc();
    }

    pub(crate) fn total_dropped(&self) -> u64 {
        [
            drop_reason::EXPORT_ERROR,
            drop_reason::EXPORT_TIMEOUT,
            drop_reason::PROCESSOR_CLOSED,
            drop_reason::QUEUE_FULL,
            drop_reason::SHUTDOWN_TIMEOUT,
        ]
        .iter()
        .map(|reason| self.dropped_spans.with_label_values(&[reason]).get())
        .sum()
    }

    pub(crate) fn total_rejected(&self) -> u64 {
        ["grpc", "http_protobuf"]
            .iter()
            .map(|protocol| self.rejected_spans.with_label_values(&[protocol]).get())
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn dropped_for(&self, reason: &'static str) -> u64 {
        self.dropped_spans.with_label_values(&[reason]).get()
    }

    #[cfg(test)]
    pub(crate) fn failed_batches_for(&self, protocol: &'static str) -> u64 {
        self.failed_batches.with_label_values(&[protocol]).get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_register_and_keep_labels_bounded() {
        let metrics = TraceMetrics::new().unwrap();
        let registry = Registry::new_custom(Some("rms".to_string()), None).unwrap();
        metrics.register(&registry).unwrap();

        metrics.drop_spans(drop_reason::QUEUE_FULL, 2);
        metrics.reject_spans("grpc", 3);
        metrics.fail_batch("http_protobuf");

        let names: Vec<_> = registry
            .gather()
            .into_iter()
            .map(|family| family.name().to_string())
            .collect();
        assert!(names.contains(&"rms_trace_spans_dropped_total".to_string()));
        assert!(names.contains(&"rms_trace_spans_rejected_total".to_string()));
        assert!(names.contains(&"rms_trace_export_batches_failed_total".to_string()));
        assert_eq!(metrics.total_dropped(), 2);
        assert_eq!(metrics.total_rejected(), 3);
    }

    #[test]
    fn registration_rejects_descriptor_collisions() {
        let metrics = TraceMetrics::new().unwrap();
        let registry = Registry::new_custom(Some("rms".to_string()), None).unwrap();
        let conflict = IntCounterVec::new(
            Opts::new(
                "trace_spans_dropped_total",
                "OpenTelemetry spans RMS could not deliver, by bounded reason",
            ),
            &["reason"],
        )
        .unwrap();
        registry.register(Box::new(conflict)).unwrap();

        assert!(matches!(
            metrics.register(&registry),
            Err(prometheus::Error::AlreadyReg)
        ));
    }
}
