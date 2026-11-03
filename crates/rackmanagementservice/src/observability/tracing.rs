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

//! OpenTelemetry trace generation and OTLP export.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use opentelemetry::trace::{Status, TracerProvider as _};
use opentelemetry::{InstrumentationScope, KeyValue, Value};
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{Protocol, RetryPolicy, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::resource::TelemetryResourceDetector;
use opentelemetry_sdk::trace::{
    Sampler, SdkTracer, SdkTracerProvider, SpanData, SpanExporter as SpanExporterTrait, SpanLimits,
};
use prost::Message as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::config::{OtlpTraceProtocol, RmsTracingConfig, is_loopback_ip_endpoint};

use super::batch::{BatchSettings, ObservableBatchSpanProcessor};
use super::grpc::{GrpcSpanExporter, OtlpSpanExporter};
use super::trace_metrics::TraceMetrics;

const SERVICE_NAME: &str = "rackmanagementservice";
const INSTRUMENTATION_SCOPE: &str = "rms.spans";
const MAX_HTTP_RESPONSE_BYTES: u64 = 1024 * 1024;
const MAX_EXPORTED_STRING_CHARS: usize = 1_024;
const UNSUPPORTED_OTEL_ENV_VARS: [&str; 4] = [
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
    "OTEL_EXPORTER_OTLP_COMPRESSION",
    "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
];
const RMS_SPAN_LIMITS: SpanLimits = SpanLimits {
    max_events_per_span: 0,
    max_attributes_per_span: 128,
    max_links_per_span: 0,
    max_attributes_per_event: 0,
    max_attributes_per_link: 0,
};

/// Owns the provider so queued spans can be flushed after RMS drains work.
#[derive(Debug)]
pub struct TracingGuard {
    provider: SdkTracerProvider,
    shutdown_timeout: Duration,
    export_enabled: bool,
    metrics: Arc<TraceMetrics>,
}

impl TracingGuard {
    /// Whether this process is configured to export spans to a collector.
    #[must_use]
    pub fn export_enabled(&self) -> bool {
        self.export_enabled
    }

    pub fn register_metrics(
        &self,
        registry: &prometheus::Registry,
    ) -> Result<(), prometheus::Error> {
        self.metrics.register(registry)
    }

    pub(crate) fn metrics(&self) -> Arc<TraceMetrics> {
        self.metrics.clone()
    }

    /// Shut down the provider without allowing a slow collector to hold RMS
    /// process termination indefinitely.
    pub async fn shutdown(self) {
        let timeout = self.shutdown_timeout;
        let provider = self.provider;
        let metrics = self.metrics;
        match tokio::task::spawn_blocking(move || provider.shutdown_with_timeout(timeout)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => tracing::warn!("failed to flush all OpenTelemetry spans on shutdown"),
            Err(error) => tracing::warn!(%error, "OpenTelemetry shutdown task failed"),
        }

        let dropped_spans = metrics.total_dropped();
        let rejected_spans = metrics.total_rejected();
        if dropped_spans > 0 || rejected_spans > 0 {
            tracing::warn!(
                dropped_spans,
                rejected_spans,
                "OpenTelemetry tracing stopped with delivery loss"
            );
        }
    }
}

/// Build the tracer and its shutdown guard.
pub fn build_tracer(config: &RmsTracingConfig) -> eyre::Result<(SdkTracer, TracingGuard)> {
    config.validate().map_err(eyre::Report::msg)?;

    let service_version = bounded_redacted(env!("VERGEN_GIT_DESCRIBE"));
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    let resource = Resource::builder_empty()
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_attributes([
            KeyValue::new("service.name", SERVICE_NAME),
            KeyValue::new("service.version", service_version.clone()),
            KeyValue::new("service.instance.id", service_instance_id),
        ])
        .build();
    let metrics = Arc::new(TraceMetrics::new()?);

    // RMS owns sampling even when a caller supplies a parent. The fixed ratio
    // and shared trace ID keep the decision consistent for local descendants.
    let mut provider_builder = SdkTracerProvider::builder()
        .with_sampler(Sampler::TraceIdRatioBased(config.sampling_ratio))
        .with_span_limits(RMS_SPAN_LIMITS)
        .with_resource(resource);

    let export_enabled = config.endpoint.is_some();
    if let Some(endpoint) = config.endpoint.as_deref() {
        reject_ambient_exporter_configuration()?;
        let exporter = SanitizingSpanExporter {
            inner: build_exporter(config, endpoint, metrics.clone())?,
        };
        let processor = ObservableBatchSpanProcessor::new(
            exporter,
            BatchSettings {
                protocol: config.protocol,
                max_queue_size: config.max_queue_size,
                max_export_batch_size: config.max_export_batch_size,
                scheduled_delay: Duration::from_millis(config.scheduled_delay_ms),
                export_timeout: Duration::from_millis(config.export_timeout_ms),
            },
            Duration::from_millis(config.shutdown_timeout_ms),
            metrics.clone(),
        );
        provider_builder = provider_builder.with_span_processor(processor);
    }

    let provider = provider_builder.build();
    let scope = InstrumentationScope::builder(INSTRUMENTATION_SCOPE)
        .with_version(service_version)
        .build();
    let tracer = provider.tracer_with_scope(scope);
    let guard = TracingGuard {
        provider,
        shutdown_timeout: Duration::from_millis(config.shutdown_timeout_ms),
        export_enabled,
        metrics,
    };

    Ok((tracer, guard))
}

fn reject_ambient_exporter_configuration() -> eyre::Result<()> {
    if let Some(variable) = UNSUPPORTED_OTEL_ENV_VARS
        .iter()
        .find(|variable| std::env::var_os(variable).is_some())
    {
        eyre::bail!(
            "{variable} is not supported; configure authentication, routing, and compression on \
             the deployment-owned OpenTelemetry Collector"
        );
    }
    Ok(())
}

fn build_exporter(
    config: &RmsTracingConfig,
    endpoint: &str,
    metrics: Arc<TraceMetrics>,
) -> eyre::Result<OtlpSpanExporter> {
    let timeout = Duration::from_millis(config.export_attempt_timeout_ms);
    let retry_policy = RetryPolicy {
        max_retries: config.max_retries,
        initial_delay_ms: config.retry_initial_delay_ms,
        max_delay_ms: config.retry_max_delay_ms,
        jitter_ms: config.retry_jitter_ms,
    };

    match config.protocol {
        OtlpTraceProtocol::Grpc => {
            GrpcSpanExporter::new(endpoint, timeout, retry_policy).map(OtlpSpanExporter::Grpc)
        }
        OtlpTraceProtocol::HttpProtobuf => {
            let client = OtlpHttpClient::new(endpoint, timeout, metrics)?;
            SpanExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(endpoint)
                .with_timeout(timeout)
                .with_http_client(client)
                .with_retry_policy(retry_policy)
                .build()
                .map(OtlpSpanExporter::Http)
                .map_err(Into::into)
        }
    }
}

/// Return true only for the initial audited RMS span catalog.
#[must_use]
pub fn should_export(metadata: &tracing::Metadata<'_>) -> bool {
    metadata.is_span()
        && (crate::orchestrator::job_tracker::JOB_SPAN_NAMES.contains(&metadata.name())
            || matches!(
                metadata.name(),
                "request"
                    | "wait_for_uninstall_completion"
                    | "wait_for_install_completion"
                    | "wait_for_target_steady_state"
                    | "switch_system_image_sftp_upload"
            ))
}

/// Record a phase's final result before its span closes. Success is explicit
/// only when the caller has verified the postcondition that this span represents.
pub(crate) fn record_phase_result<T>(
    span: &tracing::Span,
    result: &crate::utilities::error::Result<T>,
    verified_success: bool,
) {
    match result {
        Err(error) => {
            // ErrorCode has a finite vocabulary; the message may contain secrets.
            span.set_attribute("error.type", error.code.to_string());
            span.set_status(Status::error(""));
        }
        Ok(_) if verified_success => span.set_status(Status::Ok),
        Ok(_) => {}
    }
}

/// Keeps HTTP response metadata available to the OTLP retry classifier and
/// narrows its retry set to the OTLP specification.
#[derive(Clone)]
struct OtlpHttpClient {
    inner: reqwest::Client,
    metrics: Arc<TraceMetrics>,
}

impl OtlpHttpClient {
    fn new(endpoint: &str, timeout: Duration, metrics: Arc<TraceMetrics>) -> eyre::Result<Self> {
        let inner = reqwest::Client::builder()
            .timeout(timeout)
            .https_only(!is_loopback_ip_endpoint(endpoint))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .tls_built_in_native_certs(true)
            .tls_built_in_webpki_certs(false)
            .build()?;
        Ok(Self { inner, metrics })
    }
}

impl fmt::Debug for OtlpHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpHttpClient")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl HttpClient for OtlpHttpClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (parts, body) = request.into_parts();
        let request = self
            .inner
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers)
            .body(body)
            .build()?;
        let mut response = self.inner.execute(request).await?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_HTTP_RESPONSE_BYTES)
        {
            return non_retryable_http_response();
        }

        let original_status = response.status();
        let mut headers = std::mem::take(response.headers_mut());
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if body.len().saturating_add(chunk.len()) > MAX_HTTP_RESPONSE_BYTES as usize {
                return non_retryable_http_response();
            }
            body.extend_from_slice(&chunk);
        }

        let mut status = normalized_retry_status(original_status, &headers);
        if original_status.is_success() {
            let content_type_is_protobuf = headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(';').next())
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/x-protobuf"));
            let decoded =
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse::decode(
                    body.as_slice(),
                );
            // OTLP acknowledges full and partial acceptance with HTTP 200;
            // another 2xx response from a gateway is not an acknowledgment.
            if original_status != http::StatusCode::OK
                || !content_type_is_protobuf
                || decoded.is_err()
            {
                status = http::StatusCode::BAD_REQUEST;
                headers.remove(http::header::RETRY_AFTER);
                body.clear();
            } else if let Some(partial) = decoded.ok().and_then(|response| response.partial_success)
                && (partial.rejected_spans > 0 || !partial.error_message.is_empty())
            {
                self.metrics.reject_spans(
                    "http_protobuf",
                    u64::try_from(partial.rejected_spans).unwrap_or(0),
                );
                tracing::warn!(
                    protocol = "http_protobuf",
                    rejected_spans = partial.rejected_spans,
                    "OpenTelemetry Collector reported partial success for a trace batch"
                );
            }
        }

        let mut response = Response::builder().status(status).body(Bytes::from(body))?;
        *response.headers_mut() = headers;
        Ok(response)
    }
}

fn non_retryable_http_response() -> Result<Response<Bytes>, HttpError> {
    Ok(Response::builder()
        .status(http::StatusCode::BAD_REQUEST)
        .body(Bytes::new())?)
}

fn normalized_retry_status(
    status: http::StatusCode,
    headers: &http::HeaderMap,
) -> http::StatusCode {
    match status.as_u16() {
        // Upstream 0.32 honors Retry-After only for 429. OTLP permits the same
        // server-directed delay on 503, so normalize it for classification.
        503 if headers.contains_key(http::header::RETRY_AFTER) => {
            http::StatusCode::TOO_MANY_REQUESTS
        }
        // OTLP/HTTP allows retries for these statuses.
        429 | 502 | 503 | 504 => status,
        // Upstream retries every 5xx. Map permanent 5xx responses into its
        // non-retryable 4xx class; the response is internal to the exporter.
        500..=599 => http::StatusCode::BAD_REQUEST,
        _ => status,
    }
}

/// Removes attributes that have not completed an export-safety review.
#[derive(Debug)]
struct SanitizingSpanExporter {
    inner: OtlpSpanExporter,
}

impl SpanExporterTrait for SanitizingSpanExporter {
    async fn export(&self, mut batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        for span in &mut batch {
            span.attributes
                .retain(|attribute| approved_attribute(attribute.key.as_str()));
            for attribute in &mut span.attributes {
                if let Value::String(value) = &mut attribute.value {
                    *value = bounded_redacted(value.as_ref()).into();
                }
            }
            if let Status::Error { description } = &mut span.status {
                *description = Cow::Owned(bounded_redacted(description));
            }
            span.events = Default::default();
            span.links = Default::default();
        }
        self.inner.export(batch).await
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

fn bounded_redacted(value: &str) -> String {
    common::redaction::redact_secret_fields_bounded(value, MAX_EXPORTED_STRING_CHARS)
}

fn approved_attribute(key: &str) -> bool {
    matches!(
        key,
        "rpc.system.name"
            | "rpc.method"
            | "rpc.response.status_code"
            | "job_id"
            | "parent_job_id"
            | "error.type"
    )
}

#[cfg(test)]
#[path = "../../tests/support/tracing_tls.rs"]
mod tracing_tls;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex};

    use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt as _, TraceId, TraceState};
    use opentelemetry::{Context, TraceFlags};
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
        TraceService, TraceServiceServer,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    };
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic_types::{ErrorDetails, StatusExt as _};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::prelude::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::tracing_tls::CollectorTls;
    use super::*;

    fn with_trust<T>(tls: &CollectorTls, body: impl FnOnce() -> T) -> T {
        crate::with_test_env!("SSL_CERT_FILE" => tls.ca_file.path().to_str().unwrap(), body())
    }

    /// TLS terminates before WireMock so its response/retry assertions remain unchanged.
    /// Every connection and listener is owned by the fixture's abortable task.
    struct TlsMockServer {
        server: MockServer,
        endpoint: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl TlsMockServer {
        async fn start(tls: &CollectorTls) -> Self {
            let server = MockServer::start().await;
            let backend = *server.address();
            let certificates = rustls_pemfile::certs(&mut tls.certificate_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let key = rustls_pemfile::private_key(&mut tls.private_key_pem.as_bytes())
                .unwrap()
                .unwrap();
            let config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certificates, key)
                .unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let endpoint = format!(
                "https://localhost:{}",
                listener.local_addr().unwrap().port()
            );
            let task = tokio::spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (socket, _) = accepted.unwrap();
                            let acceptor = acceptor.clone();
                            connections.spawn(async move {
                                if let Ok(mut tls) = acceptor.accept(socket).await {
                                    let mut upstream = tokio::net::TcpStream::connect(backend).await.unwrap();
                                    let _ = tokio::io::copy_bidirectional(&mut tls, &mut upstream).await;
                                }
                            });
                        }
                        Some(result) = connections.join_next() => { result.unwrap(); }
                    }
                }
            });
            Self {
                server,
                endpoint,
                task,
            }
        }

        fn uri(&self) -> &str {
            &self.endpoint
        }
    }

    impl std::ops::Deref for TlsMockServer {
        type Target = MockServer;
        fn deref(&self) -> &Self::Target {
            &self.server
        }
    }

    impl Drop for TlsMockServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn test_metrics() -> Arc<TraceMetrics> {
        Arc::new(TraceMetrics::new().unwrap())
    }

    fn export_config(endpoint: String, protocol: OtlpTraceProtocol) -> RmsTracingConfig {
        RmsTracingConfig {
            endpoint: Some(endpoint),
            protocol,
            max_queue_size: 16,
            max_export_batch_size: 8,
            scheduled_delay_ms: 10,
            export_attempt_timeout_ms: 500,
            export_timeout_ms: 1_000,
            shutdown_timeout_ms: 2_000,
            max_retries: 0,
            ..RmsTracingConfig::default()
        }
    }

    fn emit_request_span(tracer: SdkTracer) {
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "request",
                otel.name = "rack_manager.RackManager/GetVersion",
                otel.kind = "server",
                rpc.system.name = "grpc",
                rpc.method = "rack_manager.RackManager/GetVersion",
                rpc.response.status_code = "OK",
                password = "plain_secret",
            );
            let _entered = span.enter();
        });
    }

    /// Ambient settings that could send spans somewhere other than the configured endpoint.
    const AMBIENT_REDIRECT_VARS: [&str; 8] = [
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ];

    /// Run `test_name` in isolation with every [`AMBIENT_REDIRECT_VARS`] entry pointing at a
    /// listener this process owns. Returns `true` in the parent once that listener stayed unused.
    fn run_test_with_ambient_redirects(test_name: &str) -> bool {
        let decoy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        decoy.set_nonblocking(true).unwrap();
        let decoy_url = format!("http://{}", decoy.local_addr().unwrap());
        let mut env: Vec<(&str, &str)> = AMBIENT_REDIRECT_VARS
            .iter()
            .map(|variable| (*variable, decoy_url.as_str()))
            .collect();
        // An inherited NO_PROXY entry for 127.0.0.1 would hide a proxied request.
        env.extend([("NO_PROXY", ""), ("no_proxy", "")]);
        if !crate::observability::run_test_in_subprocess_with_env(test_name, &env) {
            return false;
        }
        assert_eq!(
            decoy
                .accept()
                .expect_err("spans must not reach an ambient proxy or endpoint")
                .kind(),
            std::io::ErrorKind::WouldBlock,
        );
        true
    }

    #[test]
    fn only_audited_spans_are_exported() {
        let request = tracing::info_span!("request");
        let arbitrary = tracing::info_span!("arbitrary");
        assert!(should_export(request.metadata().unwrap()));
        assert!(!should_export(arbitrary.metadata().unwrap()));
    }

    #[test]
    fn only_audited_attributes_are_exported() {
        assert!(approved_attribute("rpc.method"));
        assert!(approved_attribute("job_id"));
        assert!(!approved_attribute("request"));
        assert!(!approved_attribute("rpc.grpc.status_description"));
        assert!(!approved_attribute("password"));
    }

    #[test]
    fn exported_strings_are_redacted_and_bounded() {
        let value = format!("password=plain_secret {}", "x".repeat(2_000));
        let bounded = bounded_redacted(&value);
        assert!(!bounded.contains("plain_secret"));
        assert!(bounded.contains("XXXX"));
        assert!(bounded.chars().count() <= MAX_EXPORTED_STRING_CHARS + 1);
    }

    #[test]
    fn ambient_span_limits_cannot_remove_required_attributes() {
        // The ambient SDK setting must not affect other tests' providers.
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "ambient_span_limits_cannot_remove_required_attributes"
        )) {
            return;
        }

        let exporter = InMemorySpanExporter::default();
        let provider = crate::with_test_env!("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT" => "0", {
            SdkTracerProvider::builder()
                .with_span_limits(RMS_SPAN_LIMITS)
                .with_simple_exporter(exporter.clone())
                .build()
        });
        emit_request_span(provider.tracer("span-limit-test"));
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        assert!(
            spans[0]
                .attributes
                .iter()
                .any(|attribute| attribute.key.as_str() == "rpc.method")
        );
        assert_eq!(RMS_SPAN_LIMITS.max_events_per_span, 0);
        assert_eq!(RMS_SPAN_LIMITS.max_links_per_span, 0);
    }

    #[tokio::test]
    async fn http_client_preserves_error_status_and_retry_after() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "http_client_preserves_error_status_and_retry_after"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        let server = TlsMockServer::start(&tls).await;
        Mock::given(method("POST"))
            .and(path("/v1/traces"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "3")
                    .set_body_bytes(Vec::new()),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = with_trust(&tls, || {
            OtlpHttpClient::new(server.uri(), Duration::from_secs(1), test_metrics()).unwrap()
        });
        let response = client
            .send_bytes(
                Request::post(format!("{}/v1/traces", server.uri()))
                    .body(Bytes::new())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), 429);
        assert_eq!(response.headers()["retry-after"], "3");
    }

    #[tokio::test]
    async fn http_client_rejects_malformed_success_responses() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "http_client_rejects_malformed_success_responses"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        for (status, content_type, body) in [
            (200, "text/plain", Vec::new()),
            (200, "application/x-protobuf-invalid", Vec::new()),
            (200, "application/x-protobuf", vec![0xff]),
            (202, "application/x-protobuf", Vec::new()),
            (204, "application/x-protobuf", Vec::new()),
        ] {
            let server = TlsMockServer::start(&tls).await;
            Mock::given(method("POST"))
                .and(path("/v1/traces"))
                .respond_with(
                    ResponseTemplate::new(status)
                        .insert_header("content-type", content_type)
                        .set_body_bytes(body),
                )
                .expect(1)
                .mount(&server)
                .await;

            let client = with_trust(&tls, || {
                OtlpHttpClient::new(server.uri(), Duration::from_secs(1), test_metrics()).unwrap()
            });
            let response = client
                .send_bytes(
                    Request::post(format!("{}/v1/traces", server.uri()))
                        .body(Bytes::new())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
            assert!(response.body().is_empty());
        }
    }

    #[tokio::test]
    async fn http_partial_success_updates_rejection_metric_without_retry() {
        use tracing::instrument::WithSubscriber as _;

        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "http_partial_success_updates_rejection_metric_without_retry"
        )) {
            return;
        }

        struct Warnings(Arc<Mutex<Vec<String>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warnings {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _context: tracing_subscriber::layer::Context<'_, S>,
            ) {
                struct Fields(String);
                impl tracing::field::Visit for Fields {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn fmt::Debug,
                    ) {
                        use std::fmt::Write as _;
                        write!(&mut self.0, " {}={value:?}", field.name()).unwrap();
                    }
                }
                if *event.metadata().level() == tracing::Level::WARN
                    && event.metadata().target() == "rackmanagementservice::observability::tracing"
                {
                    let mut fields = Fields(String::new());
                    event.record(&mut fields);
                    self.0.lock().unwrap().push(fields.0);
                }
            }
        }

        const SECRET: &str = "collector-controlled secret";
        let tls = CollectorTls::new("localhost", false);
        for (case, partial, expected_rejected, expected_warning) in [
            ("accepted", None, 0, None),
            ("empty_partial", Some((0, "")), 0, None),
            ("rejected", Some((2, SECRET)), 2, Some(2)),
            ("rejected_without_message", Some((2, "")), 2, Some(2)),
            ("warning_only", Some((0, SECRET)), 0, Some(0)),
            ("invalid_count", Some((-1, SECRET)), 0, Some(-1)),
            ("invalid_count_without_message", Some((-1, "")), 0, None),
        ] {
            let body = ExportTraceServiceResponse {
                partial_success: partial.map(|(rejected_spans, error_message)| {
                    opentelemetry_proto::tonic::collector::trace::v1::ExportTracePartialSuccess {
                        rejected_spans,
                        error_message: error_message.to_string(),
                    }
                }),
            }
            .encode_to_vec();
            let server = TlsMockServer::start(&tls).await;
            Mock::given(method("POST"))
                .and(path("/v1/traces"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "application/x-protobuf")
                        .set_body_bytes(body),
                )
                .expect(1)
                .mount(&server)
                .await;

            let metrics = test_metrics();
            let client = with_trust(&tls, || {
                OtlpHttpClient::new(server.uri(), Duration::from_secs(1), metrics.clone()).unwrap()
            });
            let warnings = Arc::new(Mutex::new(Vec::new()));
            let response = client
                .send_bytes(
                    Request::post(format!("{}/v1/traces", server.uri()))
                        .body(Bytes::new())
                        .unwrap(),
                )
                .with_subscriber(tracing_subscriber::registry().with(Warnings(warnings.clone())))
                .await
                .unwrap();

            assert_eq!(response.status(), http::StatusCode::OK, "{case}");
            assert_eq!(metrics.total_rejected(), expected_rejected, "{case}");
            assert_eq!(metrics.total_dropped(), 0, "{case}");
            let warnings = warnings.lock().unwrap();
            match expected_warning {
                Some(rejected_spans) => {
                    assert_eq!(warnings.len(), 1, "{case}: {warnings:?}");
                    let warning = &warnings[0];
                    assert!(
                        warning.contains("OpenTelemetry Collector reported partial success"),
                        "{case}: {warning}"
                    );
                    assert!(
                        warning.contains(r#"protocol="http_protobuf""#),
                        "{case}: {warning}"
                    );
                    assert!(
                        warning.contains(&format!("rejected_spans={rejected_spans}")),
                        "{case}: {warning}"
                    );
                    assert!(!warning.contains(SECRET), "{case}: {warning}");
                }
                None => assert!(warnings.is_empty(), "{case}: {warnings:?}"),
            }
        }
    }

    #[test]
    fn http_retry_statuses_follow_otlp() {
        let mut headers = http::HeaderMap::new();
        assert_eq!(
            normalized_retry_status(http::StatusCode::INTERNAL_SERVER_ERROR, &headers),
            http::StatusCode::BAD_REQUEST
        );
        assert_eq!(
            normalized_retry_status(http::StatusCode::BAD_GATEWAY, &headers),
            http::StatusCode::BAD_GATEWAY
        );
        headers.insert(http::header::RETRY_AFTER, "3".parse().unwrap());
        assert_eq!(
            normalized_retry_status(http::StatusCode::SERVICE_UNAVAILABLE, &headers),
            http::StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exports_spans_over_http_protobuf() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "exports_spans_over_http_protobuf"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        let server = TlsMockServer::start(&tls).await;
        Mock::given(method("POST"))
            .and(path("/v1/traces"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/x-protobuf")
                    .set_body_bytes(Vec::new()),
            )
            .expect(1)
            .mount(&server)
            .await;

        let config = export_config(
            format!("{}/v1/traces", server.uri()),
            OtlpTraceProtocol::HttpProtobuf,
        );
        let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
        emit_request_span(tracer);
        guard.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plaintext_http_protobuf_export_reaches_only_the_configured_loopback_collector() {
        if run_test_with_ambient_redirects(concat!(
            "observability::tracing::tests::",
            "plaintext_http_protobuf_export_reaches_only_the_configured_loopback_collector"
        )) {
            return;
        }
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/traces"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/x-protobuf")
                    .set_body_bytes(Vec::new()),
            )
            .expect(1)
            .mount(&server)
            .await;

        let config = export_config(
            format!("{}/v1/traces", server.uri()),
            OtlpTraceProtocol::HttpProtobuf,
        );
        let (tracer, guard) = build_tracer(&config).unwrap();
        emit_request_span(tracer);
        guard.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exports_phase_statuses_without_error_messages() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "exports_phase_statuses_without_error_messages"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        use opentelemetry_proto::tonic::common::v1::any_value;
        use opentelemetry_proto::tonic::trace::v1::status::StatusCode;

        use crate::utilities::error::RmsError;

        let server = TlsMockServer::start(&tls).await;
        Mock::given(method("POST"))
            .and(path("/v1/traces"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/x-protobuf")
                    .set_body_bytes(Vec::new()),
            )
            .mount(&server)
            .await;
        let config = export_config(
            format!("{}/v1/traces", server.uri()),
            OtlpTraceProtocol::HttpProtobuf,
        );
        let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(tracing_subscriber::filter::filter_fn(should_export)),
        );
        tracing::subscriber::with_default(subscriber, || {
            let failed = tracing::info_span!("wait_for_uninstall_completion");
            record_phase_result::<()>(
                &failed,
                &Err(RmsError::timeout("password=private-value")),
                false,
            );
            let verified = tracing::info_span!("wait_for_target_steady_state");
            record_phase_result(&verified, &Ok(()), true);
            let uncertain = tracing::info_span!("wait_for_install_completion");
            record_phase_result(&uncertain, &Ok(()), false);
        });
        guard.shutdown().await;

        let spans: Vec<_> = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|request| {
                ExportTraceServiceRequest::decode(request.body.as_slice())
                    .unwrap()
                    .resource_spans
            })
            .flat_map(|resource| resource.scope_spans)
            .flat_map(|scope| scope.spans)
            .collect();
        assert_eq!(spans.len(), 3);
        for (name, expected_code, expected_error) in [
            (
                "wait_for_uninstall_completion",
                StatusCode::Error,
                Some("Timeout"),
            ),
            ("wait_for_target_steady_state", StatusCode::Ok, None),
            ("wait_for_install_completion", StatusCode::Unset, None),
        ] {
            let span = spans.iter().find(|span| span.name == name).unwrap();
            let status = span.status.as_ref().unwrap();
            assert_eq!(status.code, expected_code as i32);
            assert!(status.message.is_empty());
            let error = span.attributes.iter().find(|attr| attr.key == "error.type");
            assert_eq!(
                error
                    .and_then(|attr| attr.value.as_ref())
                    .and_then(|value| {
                        match value.value.as_ref() {
                            Some(any_value::Value::StringValue(value)) => Some(value.as_str()),
                            _ => None,
                        }
                    }),
                expected_error,
            );
            assert!(span.events.is_empty());
            assert!(!format!("{span:?}").contains("private-value"));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_parent_flags_do_not_override_rms_sampling() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "remote_parent_flags_do_not_override_rms_sampling"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        // These fixed trace IDs exercise both sides of the pinned SDK's 50%
        // threshold without making the test depend on random IDs.
        for (ratio, trace_id_bytes, expected_sampled) in [
            (1.0, [0xff; 16], true),
            (0.5, [1; 16], true),
            (0.5, [0xff; 16], false),
        ] {
            for parent_sampled in [false, true] {
                let server = TlsMockServer::start(&tls).await;
                Mock::given(method("POST"))
                    .and(path("/v1/traces"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .insert_header("content-type", "application/x-protobuf")
                            .set_body_bytes(Vec::new()),
                    )
                    .mount(&server)
                    .await;
                let mut config = export_config(
                    format!("{}/v1/traces", server.uri()),
                    OtlpTraceProtocol::HttpProtobuf,
                );
                config.sampling_ratio = ratio;
                let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
                let subscriber = tracing_subscriber::registry().with(
                    tracing_opentelemetry::layer()
                        .with_tracer(tracer)
                        .with_filter(tracing_subscriber::filter::filter_fn(should_export)),
                );
                let trace_id = TraceId::from_bytes(trace_id_bytes);
                let parent_span_id = SpanId::from_bytes([2; 8]);
                let parent = Context::new().with_remote_span_context(SpanContext::new(
                    trace_id,
                    parent_span_id,
                    TraceFlags::new(u8::from(parent_sampled)),
                    true,
                    TraceState::default(),
                ));
                let contexts = tracing::subscriber::with_default(subscriber, || {
                    let request =
                        tracing::info_span!(parent: None, "request", otel.kind = "server");
                    request.set_parent(parent).unwrap();
                    let _request = request.enter();
                    let child = tracing::info_span!("firmware_update");
                    let _child = child.enter();
                    let grandchild = tracing::info_span!("wait_for_target_steady_state");
                    [&request, &child, &grandchild]
                        .map(|span| span.context().span().span_context().clone())
                });
                guard.shutdown().await;

                for context in &contexts {
                    assert!(context.is_valid());
                    assert!(!context.is_remote());
                    assert_eq!(context.trace_id(), trace_id);
                    assert_ne!(context.span_id(), parent_span_id);
                    assert_eq!(
                        context.is_sampled(),
                        expected_sampled,
                        "ratio={ratio}, parent_sampled={parent_sampled}"
                    );
                }
                let spans: Vec<_> = server
                    .received_requests()
                    .await
                    .unwrap()
                    .into_iter()
                    .flat_map(|request| {
                        ExportTraceServiceRequest::decode(request.body.as_slice())
                            .unwrap()
                            .resource_spans
                    })
                    .flat_map(|resource| resource.scope_spans)
                    .flat_map(|scope| scope.spans)
                    .collect();
                assert_eq!(spans.len(), if expected_sampled { 3 } else { 0 });
                if expected_sampled {
                    for (index, name) in
                        ["request", "firmware_update", "wait_for_target_steady_state"]
                            .into_iter()
                            .enumerate()
                    {
                        let span = spans.iter().find(|span| span.name == name).unwrap();
                        let expected_parent = if index == 0 {
                            parent_span_id
                        } else {
                            contexts[index - 1].span_id()
                        };
                        assert_eq!(span.trace_id, trace_id_bytes);
                        assert_eq!(span.span_id, contexts[index].span_id().to_bytes());
                        assert_eq!(span.parent_span_id, expected_parent.to_bytes());
                        assert_eq!(span.flags & 1, 1);
                    }
                }
            }
        }
    }

    #[derive(Clone, Debug, Default)]
    struct CapturingTraceService {
        span_count: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<ExportTraceServiceRequest>>>,
        request_times: Arc<Mutex<Vec<std::time::Instant>>>,
        responses: Arc<Mutex<VecDeque<Result<ExportTraceServiceResponse, tonic::Status>>>>,
    }

    #[tonic::async_trait]
    impl TraceService for CapturingTraceService {
        async fn export(
            &self,
            request: tonic::Request<ExportTraceServiceRequest>,
        ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
            self.request_times
                .lock()
                .unwrap()
                .push(std::time::Instant::now());
            let request = request.into_inner();
            let count = request
                .resource_spans
                .iter()
                .flat_map(|resource| &resource.scope_spans)
                .map(|scope| scope.spans.len())
                .sum::<usize>();
            self.span_count.fetch_add(count, AtomicOrdering::SeqCst);
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(ExportTraceServiceResponse::default()))
                .map(tonic::Response::new)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_retry_info_respects_collector_delay_and_status() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "grpc_retry_info_respects_collector_delay_and_status"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let service = CapturingTraceService::default();
        let requests = service.requests.clone();
        let request_times = service.request_times.clone();
        let responses = service.responses.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let identity = tls.identity();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                .unwrap()
                .add_service(TraceServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        for (code, hint, attempts, minimum_delay) in [
            (
                tonic::Code::Unavailable,
                Some(Duration::from_millis(2_250)),
                2,
                Duration::from_millis(2_250),
            ),
            (
                tonic::Code::ResourceExhausted,
                Some(Duration::from_millis(250)),
                2,
                Duration::from_millis(250),
            ),
            (tonic::Code::Unavailable, None, 2, Duration::ZERO),
            (tonic::Code::ResourceExhausted, None, 1, Duration::ZERO),
            (
                tonic::Code::InvalidArgument,
                Some(Duration::from_millis(250)),
                1,
                Duration::ZERO,
            ),
        ] {
            requests.lock().unwrap().clear();
            request_times.lock().unwrap().clear();
            let status = match hint {
                Some(delay) => tonic::Status::with_error_details(
                    code,
                    "collector-controlled message",
                    ErrorDetails::with_retry_info(Some(delay)),
                ),
                None => tonic::Status::new(code, "collector-controlled message"),
            };
            *responses.lock().unwrap() = VecDeque::from([Err(status)]);
            let mut config = export_config(endpoint.clone(), OtlpTraceProtocol::Grpc);
            config.max_retries = 2;
            config.retry_initial_delay_ms = 1;
            config.retry_max_delay_ms = 2;
            config.retry_jitter_ms = 0;
            config.export_timeout_ms = 5_000;
            config.shutdown_timeout_ms = 6_000;
            let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
            let metrics = guard.metrics();
            emit_request_span(tracer);
            guard.shutdown().await;

            let requests = requests.lock().unwrap();
            let times = request_times.lock().unwrap();
            assert_eq!(requests.len(), attempts, "{code:?}, {hint:?}");
            if attempts == 2 {
                assert_eq!(requests[0], requests[1], "retry must preserve the batch");
                assert!(
                    times[1].duration_since(times[0]) >= minimum_delay,
                    "{code:?}: retry arrived before the collector's {minimum_delay:?} delay"
                );
            }
            assert_eq!(metrics.total_dropped(), u64::from(attempts == 1));
            assert_eq!(metrics.failed_batches_for("grpc"), u64::from(attempts == 1));
            assert_eq!(metrics.total_rejected(), 0);
        }

        // A collector hint cannot extend the batch deadline or cause an early retry.
        requests.lock().unwrap().clear();
        *responses.lock().unwrap() = VecDeque::from([Err(tonic::Status::with_error_details(
            tonic::Code::Unavailable,
            "collector is throttling",
            ErrorDetails::with_retry_info(Some(Duration::from_secs(10))),
        ))]);
        let mut config = export_config(endpoint, OtlpTraceProtocol::Grpc);
        config.max_retries = 2;
        config.export_attempt_timeout_ms = 1_000;
        config.export_timeout_ms = 1_000;
        config.shutdown_timeout_ms = 5_000;
        let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
        let metrics = guard.metrics();
        emit_request_span(tracer);
        guard.shutdown().await;
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert_eq!(metrics.failed_batches_for("grpc"), 1);
        assert_eq!(metrics.total_dropped(), 1);
        assert_eq!(
            metrics.dropped_for(super::super::trace_metrics::drop_reason::EXPORT_TIMEOUT),
            1
        );
        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exporter_retries_recover_exhaust_and_stop_on_permanent_or_partial_responses() {
        // gRPC partial-success diagnostics use the global subscriber on the
        // exporter worker. Isolate that subscriber from other tests.
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "exporter_retries_recover_exhaust_and_stop_on_permanent_or_partial_responses"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);
        let grpc_metrics = test_metrics();
        tracing_subscriber::registry()
            .with(
                crate::observability::internal_logs::otlp_internal_metrics_layer(
                    grpc_metrics.clone(),
                ),
            )
            .try_init()
            .unwrap();

        let http_server = TlsMockServer::start(&tls).await;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let grpc_endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let service = CapturingTraceService::default();
        let grpc_requests = service.requests.clone();
        let grpc_responses = service.responses.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let identity = tls.identity();
        let grpc_server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                .unwrap()
                .add_service(TraceServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        for protocol in [OtlpTraceProtocol::HttpProtobuf, OtlpTraceProtocol::Grpc] {
            for (case, statuses, failed, partial) in [
                ("recovers", vec![503, 200], false, false),
                ("exhausts", vec![503, 503, 503], true, false),
                ("permanent", vec![401], true, false),
                ("partial", vec![200], false, true),
            ] {
                http_server.reset().await;
                grpc_requests.lock().unwrap().clear();
                let response = ExportTraceServiceResponse {
                    partial_success: partial.then(|| {
                        opentelemetry_proto::tonic::collector::trace::v1::ExportTracePartialSuccess {
                            rejected_spans: 1,
                            error_message: "collector-controlled secret".to_string(),
                        }
                    }),
                };
                *grpc_responses.lock().unwrap() = statuses
                    .iter()
                    .map(|status| match status {
                        503 => Err(tonic::Status::unavailable("temporarily unavailable")),
                        401 => Err(tonic::Status::unauthenticated("invalid credential")),
                        _ => Ok(response.clone()),
                    })
                    .collect();
                let http_statuses = statuses.clone();
                let attempts = AtomicUsize::new(0);
                Mock::given(method("POST"))
                    .and(path("/v1/traces"))
                    .respond_with(move |_request: &wiremock::Request| {
                        let attempt = attempts.fetch_add(1, AtomicOrdering::SeqCst);
                        let status = http_statuses[attempt.min(http_statuses.len() - 1)];
                        ResponseTemplate::new(status)
                            .insert_header("content-type", "application/x-protobuf")
                            .set_body_bytes(response.encode_to_vec())
                    })
                    .mount(&http_server)
                    .await;
                let endpoint = match protocol {
                    OtlpTraceProtocol::Grpc => grpc_endpoint.clone(),
                    OtlpTraceProtocol::HttpProtobuf => format!("{}/v1/traces", http_server.uri()),
                };
                let mut config = export_config(endpoint, protocol);
                config.max_retries = 2;
                config.retry_initial_delay_ms = 1;
                config.retry_max_delay_ms = 2;
                config.retry_jitter_ms = 0;
                let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
                let metrics = guard.metrics();
                let registry = prometheus::Registry::new();
                metrics.register(&registry).unwrap();
                let rejected_before = grpc_metrics.total_rejected();
                emit_request_span(tracer);
                guard.shutdown().await;

                let requests: Vec<_> = match protocol {
                    OtlpTraceProtocol::Grpc => grpc_requests.lock().unwrap().clone(),
                    OtlpTraceProtocol::HttpProtobuf => http_server
                        .received_requests()
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|request| {
                            ExportTraceServiceRequest::decode(request.body.as_slice()).unwrap()
                        })
                        .collect(),
                };
                assert_eq!(requests.len(), statuses.len(), "{protocol:?}: {case}");
                let spans: Vec<_> = requests[0]
                    .resource_spans
                    .iter()
                    .flat_map(|resource| &resource.scope_spans)
                    .flat_map(|scope| &scope.spans)
                    .collect();
                assert_eq!(spans.len(), 1);
                assert_eq!(spans[0].name, "rack_manager.RackManager/GetVersion");
                assert!(
                    spans[0]
                        .attributes
                        .iter()
                        .all(|attribute| attribute.key != "password")
                );
                for request in &requests[1..] {
                    assert_eq!(request, &requests[0], "retries must preserve the batch");
                }
                assert_eq!(
                    metrics.total_dropped(),
                    u64::from(failed),
                    "{protocol:?}: {case}"
                );
                assert_eq!(
                    metrics.dropped_for(super::super::trace_metrics::drop_reason::EXPORT_ERROR),
                    u64::from(failed)
                );
                let failed_batches: f64 = registry
                    .gather()
                    .into_iter()
                    .filter(|family| family.name() == "trace_export_batches_failed_total")
                    .flat_map(|family| family.get_metric().to_vec())
                    .map(|metric| metric.get_counter().value())
                    .sum();
                assert_eq!(
                    failed_batches,
                    f64::from(u8::from(failed)),
                    "{protocol:?}: {case}"
                );
                let rejected = match protocol {
                    OtlpTraceProtocol::Grpc => grpc_metrics.total_rejected() - rejected_before,
                    OtlpTraceProtocol::HttpProtobuf => metrics.total_rejected(),
                };
                assert_eq!(rejected, u64::from(partial), "{protocol:?}: {case}");
            }
        }
        let _ = shutdown_tx.send(());
        grpc_server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scheduled_export_delivers_a_partial_batch_before_shutdown() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "scheduled_export_delivers_a_partial_batch_before_shutdown"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        let delivered = Arc::new(tokio::sync::Notify::new());
        let observed = delivered.clone();
        let server = TlsMockServer::start(&tls).await;
        Mock::given(method("POST"))
            .and(path("/v1/traces"))
            .respond_with(move |_request: &wiremock::Request| {
                observed.notify_one();
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/x-protobuf")
                    .set_body_bytes(Vec::new())
            })
            .expect(1)
            .mount(&server)
            .await;
        let config = export_config(
            format!("{}/v1/traces", server.uri()),
            OtlpTraceProtocol::HttpProtobuf,
        );
        let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
        let metrics = guard.metrics();
        // One span cannot trigger the configured batch-size threshold of eight.
        emit_request_span(tracer);
        tokio::time::timeout(Duration::from_secs(5), delivered.notified())
            .await
            .expect("scheduled export must occur before shutdown");
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let request = ExportTraceServiceRequest::decode(requests[0].body.as_slice()).unwrap();
        let spans: Vec<_> = request
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "rack_manager.RackManager/GetVersion");
        guard.shutdown().await;
        assert_eq!(metrics.total_dropped(), 0);
        assert_eq!(metrics.total_rejected(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exports_spans_over_grpc() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "exports_spans_over_grpc"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let incoming = TcpListenerStream::new(listener);
        let service = CapturingTraceService::default();
        let span_count = service.span_count.clone();
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

        let config = export_config(endpoint, OtlpTraceProtocol::Grpc);
        let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
        emit_request_span(tracer);
        guard.shutdown().await;

        assert_eq!(span_count.load(AtomicOrdering::SeqCst), 1);
        {
            let requests = requests.lock().unwrap();
            let resource_spans = &requests[0].resource_spans[0];
            let resource_keys: Vec<_> = resource_spans
                .resource
                .as_ref()
                .unwrap()
                .attributes
                .iter()
                .map(|attribute| attribute.key.as_str())
                .collect();
            assert!(resource_keys.contains(&"service.name"));
            assert!(resource_keys.contains(&"service.version"));
            assert!(resource_keys.contains(&"service.instance.id"));
            assert!(resource_keys.contains(&"telemetry.sdk.name"));
            let scope_spans = &resource_spans.scope_spans[0];
            assert_eq!(
                scope_spans.scope.as_ref().unwrap().name,
                INSTRUMENTATION_SCOPE
            );
            let span = &scope_spans.spans[0];
            assert_eq!(span.name, "rack_manager.RackManager/GetVersion");
            assert_eq!(
                span.kind,
                opentelemetry_proto::tonic::trace::v1::span::SpanKind::Server as i32
            );
            assert_eq!(span.trace_id.len(), 16);
            assert_eq!(span.span_id.len(), 8);
            assert!(
                span.attributes
                    .iter()
                    .any(|attribute| attribute.key == "rpc.method")
            );
            assert!(
                span.attributes
                    .iter()
                    .any(|attribute| attribute.key == "rpc.response.status_code")
            );
            assert!(
                span.attributes
                    .iter()
                    .all(|attribute| attribute.key != "password")
            );
        }
        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plaintext_grpc_export_reaches_only_the_configured_loopback_collector() {
        if run_test_with_ambient_redirects(concat!(
            "observability::tracing::tests::",
            "plaintext_grpc_export_reaches_only_the_configured_loopback_collector"
        )) {
            return;
        }
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let endpoint = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let incoming = TcpListenerStream::new(listener);
        let service = CapturingTraceService::default();
        let span_count = service.span_count.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TraceServiceServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let config = export_config(endpoint, OtlpTraceProtocol::Grpc);
        let (tracer, guard) = build_tracer(&config).unwrap();
        emit_request_span(tracer);
        guard.shutdown().await;

        assert_eq!(span_count.load(AtomicOrdering::SeqCst), 1);
        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exports_spans_over_https_grpc_with_native_root() {
        // SSL_CERT_FILE is process-wide; other tests read it without the writer lock.
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "exports_spans_over_https_grpc_with_native_root"
        )) {
            return;
        }

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().unwrap();
        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .unwrap();
        let identity = tonic::transport::Identity::from_pem(
            format!("{}{}", server_cert.pem(), ca_cert.pem()),
            server_key.serialize_pem(),
        );

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let incoming = TcpListenerStream::new(listener);
        let service = CapturingTraceService::default();
        let span_count = service.span_count.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
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

        let ca_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(ca_file.path(), ca_cert.pem()).unwrap();
        let config = export_config(endpoint, OtlpTraceProtocol::Grpc);
        let (tracer, guard) = crate::with_test_env!(
            "SSL_CERT_FILE" => ca_file.path().to_str().unwrap(),
            build_tracer(&config).unwrap()
        );
        emit_request_span(tracer);
        guard.shutdown().await;

        assert_eq!(span_count.load(AtomicOrdering::SeqCst), 1);
        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn https_grpc_rejects_invalid_collector_certificates() {
        // SSL_CERT_FILE is process-wide; other tests read it without the writer lock.
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "https_grpc_rejects_invalid_collector_certificates"
        )) {
            return;
        }

        for (case, hostname, expired) in [
            ("unknown_ca", "localhost", false),
            ("wrong_hostname", "other.example", false),
            ("expired", "localhost", true),
        ] {
            let tls = CollectorTls::new(hostname, expired);
            let unrelated = CollectorTls::new("localhost", false);
            let trusted = if case == "unknown_ca" {
                &unrelated
            } else {
                &tls
            };
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let endpoint = format!(
                "https://localhost:{}",
                listener.local_addr().unwrap().port()
            );
            let service = CapturingTraceService::default();
            let requests = service.requests.clone();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let identity = tls.identity();
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                    .unwrap()
                    .add_service(TraceServiceServer::new(service))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .unwrap();
            });

            let config = export_config(endpoint, OtlpTraceProtocol::Grpc);
            let (tracer, guard) = with_trust(trusted, || build_tracer(&config).unwrap());
            let metrics = guard.metrics();
            emit_request_span(tracer);
            guard.shutdown().await;

            assert!(requests.lock().unwrap().is_empty(), "{case}");
            assert_eq!(metrics.total_dropped(), 1, "{case}");
            assert_eq!(metrics.failed_batches_for("grpc"), 1, "{case}");
            let _ = shutdown_tx.send(());
            server.await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unavailable_collector_does_not_block_startup_or_shutdown() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "unavailable_collector_does_not_block_startup_or_shutdown"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);

        for (endpoint, protocol, label) in [
            ("https://127.0.0.1:9", OtlpTraceProtocol::Grpc, "grpc"),
            ("http://127.0.0.1:9", OtlpTraceProtocol::Grpc, "grpc"),
            (
                "http://127.0.0.1:9/v1/traces",
                OtlpTraceProtocol::HttpProtobuf,
                "http_protobuf",
            ),
        ] {
            let mut config = export_config(endpoint.to_string(), protocol);
            config.export_attempt_timeout_ms = 50;
            config.export_timeout_ms = 100;
            config.shutdown_timeout_ms = 500;

            let started = std::time::Instant::now();
            let (tracer, guard) = with_trust(&tls, || {
                build_tracer(&config).expect("exporter builds lazily")
            });
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "{endpoint}: collector connectivity must not gate startup"
            );
            let metrics = guard.metrics();
            emit_request_span(tracer);
            guard.shutdown().await;
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "{endpoint}: collector failure must not hold process shutdown"
            );
            assert_eq!(metrics.total_dropped(), 1, "{endpoint}");
            assert_eq!(metrics.failed_batches_for(label), 1, "{endpoint}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn https_http_rejects_invalid_collector_certificates() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "https_http_rejects_invalid_collector_certificates"
        )) {
            return;
        }
        for (case, hostname, expired) in [
            ("unknown_ca", "localhost", false),
            ("wrong_hostname", "other.example", false),
            ("expired", "localhost", true),
        ] {
            let tls = CollectorTls::new(hostname, expired);
            let unrelated = CollectorTls::new("localhost", false);
            let trusted = if case == "unknown_ca" {
                &unrelated
            } else {
                &tls
            };
            let server = TlsMockServer::start(&tls).await;
            let config = export_config(
                format!("{}/v1/traces", server.uri()),
                OtlpTraceProtocol::HttpProtobuf,
            );
            let (tracer, guard) = with_trust(trusted, || build_tracer(&config).unwrap());
            let metrics = guard.metrics();
            emit_request_span(tracer);
            guard.shutdown().await;
            assert!(
                server.received_requests().await.unwrap().is_empty(),
                "{case}"
            );
            assert_eq!(metrics.total_dropped(), 1, "{case}");
            assert_eq!(metrics.failed_batches_for("http_protobuf"), 1, "{case}");
        }
    }

    #[tokio::test]
    async fn http_client_refuses_plaintext_requests() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let server = MockServer::start().await;
        let client = OtlpHttpClient::new(
            "https://collector.example:4318/v1/traces",
            Duration::from_secs(1),
            test_metrics(),
        )
        .unwrap();
        let result = client
            .send_bytes(
                Request::post(format!("{}/v1/traces", server.uri()))
                    .body(Bytes::new())
                    .unwrap(),
            )
            .await;
        assert!(result.is_err());
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_export_requires_valid_acknowledgment_and_does_not_follow_redirects() {
        if crate::observability::run_test_in_subprocess(concat!(
            "observability::tracing::tests::",
            "http_export_requires_valid_acknowledgment_and_does_not_follow_redirects"
        )) {
            return;
        }
        let tls = CollectorTls::new("localhost", false);
        let server = TlsMockServer::start(&tls).await;
        let redirect_target = TlsMockServer::start(&tls).await;
        Mock::given(path("/v1/traces"))
            .respond_with(
                ResponseTemplate::new(200).insert_header("content-type", "application/x-protobuf"),
            )
            .mount(&redirect_target)
            .await;

        for (case, response, failed, attempts) in [
            (
                "accepted",
                ResponseTemplate::new(200)
                    .insert_header("content-type", "Application/X-Protobuf; charset=utf-8"),
                false,
                1,
            ),
            (
                "wrong_status",
                ResponseTemplate::new(202).insert_header("content-type", "application/x-protobuf"),
                true,
                1,
            ),
            (
                "wrong_media_type",
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/x-protobuf-invalid"),
                true,
                1,
            ),
            (
                "oversized_response",
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/x-protobuf")
                    .set_body_bytes(vec![0; MAX_HTTP_RESPONSE_BYTES as usize + 1]),
                true,
                1,
            ),
            (
                "redirect",
                // Retrying the configured URL must never forward data to this target.
                ResponseTemplate::new(302).insert_header(
                    "location",
                    format!("{}/v1/traces", redirect_target.uri()).as_str(),
                ),
                true,
                3,
            ),
        ] {
            server.reset().await;
            Mock::given(method("POST"))
                .and(path("/v1/traces"))
                .respond_with(response)
                .mount(&server)
                .await;
            let mut config = export_config(
                format!("{}/v1/traces", server.uri()),
                OtlpTraceProtocol::HttpProtobuf,
            );
            config.max_retries = 2;
            let (tracer, guard) = with_trust(&tls, || build_tracer(&config).unwrap());
            let metrics = guard.metrics();
            emit_request_span(tracer);
            guard.shutdown().await;

            assert_eq!(
                server.received_requests().await.unwrap().len(),
                attempts,
                "{case}"
            );
            assert_eq!(metrics.total_dropped(), u64::from(failed), "{case}");
            assert_eq!(
                metrics.failed_batches_for("http_protobuf"),
                u64::from(failed),
                "{case}"
            );
            assert!(
                redirect_target
                    .received_requests()
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[derive(Debug)]
    struct SlowExporter {
        delay: Duration,
    }

    impl SpanExporterTrait for SlowExporter {
        async fn export(&self, _batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
            tokio::time::sleep(self.delay).await;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_deadline_bounds_a_slow_exporter() {
        let metrics = test_metrics();
        let processor = ObservableBatchSpanProcessor::new(
            SlowExporter {
                delay: Duration::from_millis(250),
            },
            BatchSettings {
                protocol: OtlpTraceProtocol::Grpc,
                max_queue_size: 8,
                max_export_batch_size: 8,
                scheduled_delay: Duration::from_secs(60),
                export_timeout: Duration::from_secs(1),
            },
            Duration::from_millis(25),
            metrics.clone(),
        );
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .build();
        emit_request_span(provider.tracer("slow-exporter-test"));
        let guard = TracingGuard {
            provider,
            shutdown_timeout: Duration::from_millis(25),
            export_enabled: true,
            metrics: metrics.clone(),
        };

        let started = std::time::Instant::now();
        guard.shutdown().await;
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "shutdown exceeded its configured deadline"
        );

        assert!(metrics.total_dropped() > 0);
    }
}
