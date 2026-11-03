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

#[path = "support/tracing_tls.rs"]
mod tracing_tls;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use rackmanagementservice::config::{OtlpTraceProtocol, RmsTracingConfig};
use rackmanagementservice::logging::setup::setup_logging;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

#[derive(Clone, Debug, Default)]
struct PartialSuccessService {
    requests: Arc<AtomicUsize>,
    response: ExportTraceServiceResponse,
}

#[tonic::async_trait]
impl TraceService for PartialSuccessService {
    async fn export(
        &self,
        _request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(self.response.clone()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_partial_success_is_counted() {
    const TEST_NAME: &str = "grpc_partial_success_is_counted";
    const CASE_ENV: &str = "RMS_GRPC_PARTIAL_SUCCESS_CASE";
    const SECRET: &str = "collector-controlled secret";
    const WARNING: &str = "OpenTelemetry Collector reported partial success for a trace batch";
    let cases = [
        ("accepted", None, Some("warn"), 0, false),
        ("empty_partial", Some((0, "")), Some("warn"), 0, false),
        ("rejected", Some((2, SECRET)), Some("warn"), 2, true),
        (
            "rejected_without_message",
            Some((2, "")),
            Some("warn"),
            2,
            true,
        ),
        ("warning_only", Some((0, SECRET)), Some("warn"), 0, true),
        ("invalid_count", Some((-1, SECRET)), Some("warn"), 0, true),
        (
            "invalid_count_without_message",
            Some((-1, "")),
            Some("warn"),
            0,
            false,
        ),
        ("error_filter", Some((2, SECRET)), Some("error"), 2, false),
        ("default_filter", Some((2, SECRET)), None, 2, true),
    ];
    if std::env::var("RMS_ISOLATED_TRACING_TEST").as_deref() != Ok(TEST_NAME) {
        for (case, partial, _, _, warning_expected) in cases {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST_NAME, "--nocapture"])
                .env("RMS_ISOLATED_TRACING_TEST", TEST_NAME)
                .env(CASE_ENV, case)
                .env_remove("SSL_CERT_DIR")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success() && stdout.contains("test result: ok. 1 passed; 0 failed;"),
                "{case}: {stdout}\n{stderr}"
            );
            let warnings: Vec<_> = stdout
                .lines()
                .filter(|line| line.contains(WARNING))
                .collect();
            assert_eq!(
                warnings.len(),
                usize::from(warning_expected),
                "{case}: {stdout}"
            );
            if warning_expected {
                assert!(warnings[0].contains("level=WARN"), "{case}");
                assert!(warnings[0].contains("protocol=grpc"), "{case}");
                assert!(
                    warnings[0].contains(&format!("rejected_spans={}", partial.unwrap().0)),
                    "{case}"
                );
                assert!(
                    warnings[0].len() < 512,
                    "{case}: diagnostic must stay bounded"
                );
            }
            assert!(
                !stdout.contains("TonicTracesClient.PartialSuccess"),
                "{case}"
            );
            assert!(
                !stdout.contains(SECRET) && !stderr.contains(SECRET),
                "{case}"
            );
        }
        return;
    }
    let case = std::env::var(CASE_ENV).unwrap();
    let (_, partial, log_filter, expected_rejected, _) =
        cases.into_iter().find(|(name, ..)| *name == case).unwrap();
    let tls = tracing_tls::CollectorTls::new("localhost", false);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let incoming = TcpListenerStream::new(listener);
    let service = PartialSuccessService {
        response: ExportTraceServiceResponse {
            partial_success: partial.map(|(rejected_spans, error_message)| {
                ExportTracePartialSuccess {
                    rejected_spans,
                    error_message: error_message.repeat(1_000),
                }
            }),
        },
        ..PartialSuccessService::default()
    };
    let requests = service.requests.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let identity = tls.identity();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
            .unwrap()
            .add_service(TraceServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let config = RmsTracingConfig {
        endpoint: Some(endpoint),
        protocol: OtlpTraceProtocol::Grpc,
        scheduled_delay_ms: 60_000,
        export_attempt_timeout_ms: 500,
        export_timeout_ms: 1_000,
        shutdown_timeout_ms: 2_000,
        max_retries: 2,
        retry_initial_delay_ms: 1,
        retry_max_delay_ms: 2,
        retry_jitter_ms: 0,
        ..RmsTracingConfig::default()
    };
    let guard = rackmanagementservice::with_test_env!(
        "SSL_CERT_FILE" => tls.ca_file.path().to_str().unwrap(),
        setup_logging(Vec::new(), log_filter, false, &config).unwrap()
    );
    let registry = prometheus::Registry::new_custom(Some("rms".to_string()), None).unwrap();
    guard.register_metrics(&registry).unwrap();

    for _ in 0..3 {
        let span = tracing::info_span!("request");
        let _entered = span.enter();
    }
    guard.shutdown().await;

    assert_eq!(requests.load(Ordering::SeqCst), 1);
    let rejected = registry
        .gather()
        .into_iter()
        .find(|family| family.name() == "rms_trace_spans_rejected_total")
        .expect("rejected-spans metric");
    let grpc = rejected
        .get_metric()
        .iter()
        .find(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "protocol" && label.value() == "grpc")
        })
        .expect("grpc series");
    assert_eq!(grpc.get_counter().value(), f64::from(expected_rejected));
    for name in [
        "rms_trace_spans_dropped_total",
        "rms_trace_export_batches_failed_total",
    ] {
        let total: f64 = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .unwrap()
            .get_metric()
            .iter()
            .map(|metric| metric.get_counter().value())
            .sum();
        assert_eq!(total, 0.0, "{name}");
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}
