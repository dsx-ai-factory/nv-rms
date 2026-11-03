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

//! A logging middleware for RMS API requests

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::FutureExt as _;
use http_body::{Body, Frame, SizeHint};
use opentelemetry::trace::{
    SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use x509_parser::prelude::FromDer;

const MAX_LOG_VALUE_CHARS: usize = 1_024;
const MAX_TRACEPARENT_BYTES: usize = 512;
// Finite catalog of the service RPCs in the pinned librms protobufs. librms does
// not expose its descriptor set, so the list is maintained here and
// `rpc_catalog_matches_pinned_protobuf_descriptors` fails when they differ.
// Unknown paths are deliberately collapsed to `_OTHER`.
const RACK_MANAGER_SERVICE: &str = "rack_manager.RackManager";
const RACK_MANAGER_V2_SERVICE: &str = "rack_manager_v2.RackManagerV2";
const RACK_MANAGER_METHODS: &[&str] = &[
    "SetPowerState",
    "BatchSetPowerState",
    "GetPowerState",
    "BatchGetPowerState",
    "ExecuteColdReboot",
    "ListNodeInventory",
    "CreateNodes",
    "UpdateNode",
    "DeleteNode",
    "ListRacks",
    "GetNodeDeviceInfo",
    "ListNodeDeviceInfoByNodeType",
    "BatchGetNodeDeviceInfo",
    "GetNodeFirmwareInventory",
    "BatchGetFirmwareInventory",
    "CompareFirmwareObject",
    "UpdateFirmware",
    "BatchUpdateFirmwareByNodeType",
    "BatchUpdateFirmware",
    "UpdateSwitchSystemImage",
    "GetRackFirmwareInventory",
    "AddFirmwareObject",
    "GetFirmwareObject",
    "ListFirmwareObjects",
    "DeleteFirmwareObject",
    "SetDefaultFirmwareObject",
    "ApplyStoredFirmwareObject",
    "ApplyFirmwareObject",
    "ApplySwitchSystemImage",
    "ApplyStoredSwitchSystemImage",
    "GetFirmwareObjectHistory",
    "ListSwitchFirmware",
    "PushSwitchFirmware",
    "BatchResetSwitchFactoryDefault",
    "ConfigureScaleUpFabricManager",
    "GetScaleUpFabricStatus",
    "InspectScaleUpFabrics",
    "BatchResetSwitchSdnFactoryDefault",
    "BatchSetScaleUpFabricState",
    "BatchGetScaleUpFabricServiceStatus",
    "GetScaleUpFabricState",
    "SetScaleUpFabricTelemetryInterfaceState",
    "ConfigureSwitchCertificate",
    "BatchDisableSwitchMtls",
    "GetConfigureSwitchCertificateJobStatus",
    "ListSwitchSystemImages",
    "GetSwitchSystemImageJobStatus",
    "UpdateSwitchSystemPassword",
    "BatchCollectSwitchSpdmAttestationEvidence",
    "GetVersion",
    "GetFirmwareJobStatus",
    "GetJobStatus",
];
const RACK_MANAGER_V2_METHODS: &[&str] =
    &["ConfigureScaleUpFabricManager", "StartSystemValidation"];

struct TraceParent {
    trace_id: TraceId,
    parent_id: SpanId,
    flags: TraceFlags,
}

impl TraceParent {
    fn parse(value: &str) -> Option<Self> {
        let bytes = value.as_bytes();
        if bytes.len() < 55
            || bytes[2] != b'-'
            || bytes[35] != b'-'
            || bytes[52] != b'-'
            || [&bytes[..2], &bytes[3..35], &bytes[36..52], &bytes[53..55]]
                .iter()
                .any(|field| {
                    !field
                        .iter()
                        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                })
            || bytes[..2] == *b"ff"
            || (bytes[..2] == *b"00" && bytes.len() != 55)
            || (bytes.len() > 55 && bytes[55] != b'-')
        {
            return None;
        }

        let trace_id = TraceId::from_hex(&value[3..35]).ok()?;
        let parent_id = SpanId::from_hex(&value[36..52]).ok()?;
        let flags =
            TraceFlags::new(u8::from_str_radix(&value[53..55], 16).ok()?) & TraceFlags::SAMPLED;
        (trace_id != TraceId::INVALID && parent_id != SpanId::INVALID).then_some(Self {
            trace_id,
            parent_id,
            flags,
        })
    }

    fn into_context(self) -> opentelemetry::Context {
        opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
            self.trace_id,
            self.parent_id,
            self.flags,
            true,
            TraceState::default(),
        ))
    }
}

/// Extract only W3C trace identity; caller vendor state and baggage are not
/// part of RMS's approved export data or its sampling policy.
fn request_parent_context(headers: &http::HeaderMap) -> Option<opentelemetry::Context> {
    let mut values = headers.get_all("traceparent").iter();
    let value = values.next()?;
    if values.next().is_some() || value.as_bytes().len() > MAX_TRACEPARENT_BYTES {
        return None;
    }
    TraceParent::parse(value.to_str().ok()?.trim()).map(TraceParent::into_context)
}

fn is_safe_rpc_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'))
}

fn canonical_rpc(service: &str, method: &str) -> Option<(&'static str, &'static str)> {
    if !is_safe_rpc_component(service) || !is_safe_rpc_component(method) {
        return None;
    }
    let (service, methods) = match service {
        RACK_MANAGER_SERVICE => (RACK_MANAGER_SERVICE, RACK_MANAGER_METHODS),
        RACK_MANAGER_V2_SERVICE => (RACK_MANAGER_V2_SERVICE, RACK_MANAGER_V2_METHODS),
        _ => return None,
    };
    let method = methods.iter().copied().find(|known| *known == method)?;
    Some((service, method))
}

fn grpc_status_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

fn grpc_server_status_is_error(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Unimplemented
            | tonic::Code::Internal
            | tonic::Code::Unavailable
            | tonic::Code::DataLoss
    )
}

fn record_grpc_status(span: &tracing::Span, code: tonic::Code) {
    span.record("rpc.grpc.status_code", code as u64);
    span.record("rpc.response.status_code", grpc_status_name(code));
    if grpc_server_status_is_error(code) {
        span.record("otel.status_code", "error");
        span.record("error.type", grpc_status_name(code));
        // The code is already recorded; it does not establish a safe error detail.
    }
}

/// Returns a stable caller identity from the leaf client certificate: subject DN
/// when parseable, otherwise a SHA-256 fingerprint of the DER encoding.
fn cert_peer_identity(cert_der: &[u8]) -> String {
    if let Ok((_, cert)) = x509_parser::certificate::X509Certificate::from_der(cert_der) {
        let subject: String = cert
            .subject()
            .to_string()
            .chars()
            .filter(|c| !c.is_control())
            .collect();
        if !subject.is_empty() {
            return subject;
        }
    }
    format!("sha256:{}", hex::encode(Sha256::digest(cert_der)))
}

/// A tower Layer which creates a `LogService` for every request
#[derive(Debug, Default, Clone)]
pub struct LogLayer {
    requests: TaskTracker,
    force_shutdown: CancellationToken,
}

impl LogLayer {
    pub(crate) fn with_request_tracker(
        self,
        requests: TaskTracker,
        force_shutdown: CancellationToken,
    ) -> Self {
        Self {
            requests,
            force_shutdown,
        }
    }
}

impl<S> tower::Layer<S> for LogLayer {
    type Service = LogService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LogService {
            service,
            requests: self.requests.clone(),
            force_shutdown: self.force_shutdown.clone(),
            _service: self.requests.token(),
        }
    }
}

// This service implements the RMS API server logging behavior
#[derive(Clone, Debug)]
pub struct LogService<S> {
    service: S,
    requests: TaskTracker,
    force_shutdown: CancellationToken,
    // Hyper clones the service into an unpolled request future before
    // LogService::call creates its request token. Track that interval too.
    // TaskTrackerToken::clone acquires a separate token for each service clone.
    _service: TaskTrackerToken,
}

struct RequestSpan {
    span: tracing::Span,
    _request: TaskTrackerToken,
    force_shutdown: CancellationToken,
    status_recorded: bool,
    // Only a verified RPC whose successful gRPC response proves its operation
    // succeeded may become Ok. Any observed failure clears this permission.
    can_confirm_success: bool,
}

impl Drop for RequestSpan {
    fn drop(&mut self) {
        if !std::thread::panicking() && !self.status_recorded && self.force_shutdown.is_cancelled()
        {
            record_grpc_status(&self.span, tonic::Code::Cancelled);
        }
        // Close the span before `_request` releases the drain token; otherwise
        // shutdown can close trace admission before this span is queued. This
        // must be the last handle: the `.instrument` and per-poll clones are
        // already dropped.
        drop(std::mem::replace(&mut self.span, tracing::Span::none()));
    }
}

impl std::ops::Deref for RequestSpan {
    type Target = tracing::Span;

    fn deref(&self) -> &Self::Target {
        &self.span
    }
}

/// Keeps the request span alive until the response body reaches end-of-stream.
pub struct LogResponseBody<B: Body> {
    inner: B,
    request_span: Option<RequestSpan>,
    grpc_status: Option<tonic::Code>,
}

impl<B: Body> LogResponseBody<B> {
    fn new(inner: B, request_span: RequestSpan, grpc_status: Option<tonic::Code>) -> Self {
        Self {
            inner,
            request_span: Some(request_span),
            grpc_status,
        }
    }

    fn record_status(&mut self, code: tonic::Code) {
        let Some(span) = &mut self.request_span else {
            return;
        };
        record_grpc_status(span, code);
        span.status_recorded = true;
        span.can_confirm_success &= code == tonic::Code::Ok;
        self.grpc_status = Some(code);
    }

    fn finish(&mut self) {
        if self.grpc_status.is_none()
            && let Some(span) = &mut self.request_span
        {
            span.record("rpc.response.status_code", "UNKNOWN");
            span.record("otel.status_code", "error");
            span.record("error.type", "missing_grpc_status");
            span.record(
                "otel.status_description",
                "gRPC response ended without a status",
            );
            span.status_recorded = true;
        } else if self.grpc_status == Some(tonic::Code::Ok)
            && let Some(span) = &self.request_span
            && span.can_confirm_success
        {
            // Ok takes precedence over Error in the SDK. Record it only at
            // completion, after all response failures could be observed.
            span.record("otel.status_code", "ok");
        }
        self.request_span.take();
    }

    fn fail(&mut self, description: &'static str) {
        if let Some(span) = &mut self.request_span {
            if !matches!(self.grpc_status, Some(code) if code != tonic::Code::Ok) {
                span.record("rpc.response.status_code", "UNKNOWN");
            }
            span.record("otel.status_code", "error");
            span.record("error.type", "response_body");
            span.record("otel.status_description", description);
            span.status_recorded = true;
            span.can_confirm_success = false;
        }
        self.grpc_status = Some(tonic::Code::Unknown);
        self.finish();
    }
}

impl<B> Body for LogResponseBody<B>
where
    B: Body + Unpin,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let span = self
                .request_span
                .as_ref()
                .map(|request| request.span.clone());
            let _entered = span.as_ref().map(tracing::Span::enter);
            Pin::new(&mut self.inner).poll_frame(cx)
        }));
        let result = match result {
            Ok(result) => result,
            Err(panic) => {
                self.fail("gRPC response body panicked");
                std::panic::resume_unwind(panic);
            }
        };

        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    if let Some(header) = trailers.get("grpc-status") {
                        self.record_status(tonic::Code::from_bytes(header.as_bytes()));
                    }
                    self.finish();
                }
            }
            Poll::Ready(Some(Err(_))) => self.fail("gRPC response body failed"),
            Poll::Ready(None) => self.finish(),
            Poll::Pending => {}
        }

        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<B: Body> Drop for LogResponseBody<B> {
    fn drop(&mut self) {
        if !std::thread::panicking() && self.request_span.is_some() {
            if self.inner.is_end_stream() {
                self.finish();
            } else {
                self.fail("gRPC response body dropped before completion");
            }
        }
    }
}

impl<S, RequestBody, ResponseBody> tower::Service<http::Request<RequestBody>> for LogService<S>
where
    S: tower::Service<http::Request<RequestBody>, Response = http::Response<ResponseBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    RequestBody: tonic::codegen::Body + Send + 'static,
    ResponseBody: tonic::codegen::Body + Send + Unpin + 'static,
{
    type Response = http::Response<LogResponseBody<ResponseBody>>;
    type Error = S::Error;
    type Future = tonic::codegen::BoxFuture<Self::Response, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<RequestBody>) -> Self::Future {
        let mut service = self.service.clone();
        let request_token = self.requests.token();
        let force_shutdown = self.force_shutdown.clone();

        Box::pin(async move {
            // Start a span which tracks the API request
            // Some information about the request is only known when the request finishes
            // or the payload has been deserialized.
            // For these `tracing::field::Empty` has to be used, so that the missing
            // information can be populated later.

            // Field names are taken from the crate opentelemetry_semantic_conventions,
            // e.g. `opentelemetry_semantic_conventions::trace::HTTP_STATUS_CODE`.
            // However we can't reference these external definitions in the tracing macro
            let tls_info = request.extensions().get::<TlsConnectInfo<TcpConnectInfo>>();

            let peer_addr = tls_info
                .and_then(|i| i.get_ref().remote_addr())
                .or_else(|| {
                    request
                        .extensions()
                        .get::<TcpConnectInfo>()
                        .and_then(|i| i.remote_addr())
                });

            // Capture mTLS client-certificate identity for audit attribution (threat #1).
            let peer_identity = tls_info
                .and_then(|i| i.peer_certs())
                .and_then(|certs| certs.first().map(|cert| cert_peer_identity(cert.as_ref())))
                .unwrap_or_else(|| "unknown".to_string());
            let peer_identity = common::redaction::redact_secret_fields_bounded(
                &peer_identity,
                MAX_LOG_VALUE_CHARS,
            );
            let peer_addr = peer_addr
                .map(|address| address.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            let mut request_span = RequestSpan {
                _request: request_token,
                force_shutdown,
                status_recorded: false,
                can_confirm_success: false,
                span: tracing::span!(
                parent: None,
                tracing::Level::INFO,
                "request",
                otel.name = "_OTHER",
                otel.kind = "server",
                http.response.status_code = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_description = tracing::field::Empty,
                rpc.system.name = "grpc",
                rpc.method = tracing::field::Empty,
                rpc.service = tracing::field::Empty,
                rpc.response.status_code = tracing::field::Empty,
                rpc.grpc.status_code = tracing::field::Empty,
                error.type = tracing::field::Empty,
                logfmt.suppress = tracing::field::Empty,
                ),
            };

            // Set the remote parent before logging materializes the SDK span.
            // Missing/invalid context keeps the explicit independent root.
            if let Some(parent) = request_parent_context(request.headers())
                && request_span.span.set_parent(parent).is_err()
            {
                tracing::warn!("could not apply incoming trace context");
            }

            // Try to extract the gRPC service and method from the URI
            let mut grpc_method: Option<&'static str> = None;
            let mut grpc_service: Option<&'static str> = None;
            if let Some(path) = request.uri().path_and_query()
                && *request.method() == http::Method::POST
                && path.query().is_none()
            {
                let parts: Vec<&str> = path.path().split('/').collect();
                if let ["", service, method] = parts.as_slice()
                    && let Some((service, method)) = canonical_rpc(service, method)
                {
                    // the path starts with an empty segment, and the middle
                    // segment is the service name, the last segment is the
                    // method
                    grpc_service = Some(service);
                    grpc_method = Some(method);
                }
            }

            // Record rpc.method once to avoid duplicate OpenTelemetry attributes.
            if let (Some(service), Some(method)) = (grpc_service, grpc_method) {
                let fully_qualified_method = format!("{service}/{method}");
                request_span.record("otel.name", &fully_qualified_method);
                request_span.record("rpc.service", service);
                request_span.record("rpc.method", &fully_qualified_method);
                // GetVersion has no in-band failure or deferred job result.
                request_span.can_confirm_success =
                    service == RACK_MANAGER_SERVICE && method == "GetVersion";
            } else {
                request_span.record("rpc.method", "_OTHER");
            }

            // Emit caller attribution for every inbound request. Unknown or
            // malformed routes collapse to fixed values; the raw path is never
            // logged or exported.
            let event_method = grpc_method.unwrap_or("_OTHER");
            let event_service = grpc_service.unwrap_or("_OTHER");
            request_span.in_scope(|| {
                tracing::info!(
                    rpc.method = event_method,
                    rpc.service = event_service,
                    peer_addr,
                    peer_identity,
                    "request received"
                );
            });

            // Keep the span outside the catch boundary so status is recorded
            // after either Service::call or its future has finished unwinding.
            let result = std::panic::AssertUnwindSafe(async {
                service
                    .call(request)
                    .instrument(request_span.span.clone())
                    .await
            })
            .catch_unwind()
            .await;
            let result = match result {
                Ok(result) => result,
                Err(panic) => {
                    request_span.status_recorded = true;
                    request_span.can_confirm_success = false;
                    request_span.record("error.type", "request_execution");
                    request_span.record("otel.status_code", "error");
                    request_span.record(
                        "otel.status_description",
                        "gRPC service panicked before returning a response",
                    );
                    drop(request_span);
                    std::panic::resume_unwind(panic);
                }
            };

            let mut grpc_status = None;

            match &result {
                Ok(result) => {
                    request_span.record("http.response.status_code", result.status().as_u16());

                    if result.status() == http::StatusCode::OK {
                        // In gRPC the actual message status is not in the http status code,
                        // but in a header or the final trailer. Record a header
                        // immediately when present; LogResponseBody handles trailers.
                        //
                        // Unfortunately we have to reconstruct the status here, by parsing
                        // those headers again
                        if let Some(header) = result.headers().get("grpc-status") {
                            let code = tonic::Code::from_bytes(header.as_ref());
                            grpc_status = Some(code);
                            record_grpc_status(&request_span, code);
                            request_span.status_recorded = true;
                            request_span.can_confirm_success &= code == tonic::Code::Ok;
                            if code != tonic::Code::Ok {
                                request_span.in_scope(|| {
                                    tracing::warn!(
                                        rpc.grpc.status_code = code as u64,
                                        "gRPC request failed"
                                    );
                                });
                            }
                        }
                    } else {
                        grpc_status = Some(tonic::Code::Unknown);
                        request_span.record("rpc.response.status_code", "UNKNOWN");
                        request_span.record("error.type", "http_status");
                        request_span.record("otel.status_code", "error");
                        request_span.record(
                            "otel.status_description",
                            format!("gRPC response returned HTTP {}", result.status().as_u16()),
                        );
                        request_span.status_recorded = true;
                        request_span.can_confirm_success = false;
                    }
                }
                Err(_) => {
                    request_span.record("error.type", "request_execution");
                    request_span.record("otel.status_code", "error");
                    request_span.record(
                        "otel.status_description",
                        "gRPC service failed before returning a response",
                    );
                    request_span.status_recorded = true;
                    request_span.can_confirm_success = false;
                }
            }

            result.map(|response| {
                response.map(|body| LogResponseBody::new(body, request_span, grpc_status))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use http_body::Body as _;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
        TracerProvider as _,
    };
    use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
    use rcgen::{CertificateParams, DnType, KeyPair};

    use std::sync::{Arc, Mutex};
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    use tower::Layer as _;
    use tower::Service as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::prelude::*;

    use super::{
        LogLayer, LogResponseBody, LogService, canonical_rpc, cert_peer_identity,
        grpc_server_status_is_error,
    };

    #[test]
    fn rpc_catalog_accepts_only_generated_service_methods() {
        assert_eq!(
            canonical_rpc("rack_manager.RackManager", "GetVersion"),
            Some(("rack_manager.RackManager", "GetVersion"))
        );
        assert_eq!(
            canonical_rpc(
                "rack_manager_v2.RackManagerV2",
                "ConfigureScaleUpFabricManager"
            ),
            Some((
                "rack_manager_v2.RackManagerV2",
                "ConfigureScaleUpFabricManager"
            ))
        );
        assert!(canonical_rpc("rack_manager.RackManager", "Unknown").is_none());
        assert!(canonical_rpc("attacker.Service", "GetVersion").is_none());
    }

    #[test]
    #[allow(clippy::result_large_err, reason = "required by Figment's test API")]
    fn rpc_catalog_matches_pinned_protobuf_descriptors() {
        use prost::Message as _;
        use std::process::Command;

        // Config tests clear the process environment under Figment's lock.
        figment::Jail::expect_with(|_| {
            // librms generates but does not expose its descriptor bytes. Resolve
            // its pinned sources through Cargo instead of guessing a cache path.
            let rustc = Command::new(
                std::env::var_os("RUSTC").unwrap_or_else(|| std::ffi::OsString::from("rustc")),
            )
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .arg("-vV")
            .output()
            .expect("read rustc host for Cargo metadata");
            assert!(rustc.status.success(), "rustc -vV failed");
            let rustc = String::from_utf8(rustc.stdout).unwrap();
            let host = rustc
                .lines()
                .find_map(|line| line.strip_prefix("host: "))
                .expect("rustc host triple");
            let metadata = Command::new(env!("CARGO"))
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .args(["metadata", "--locked", "--offline", "--format-version", "1"])
                .args(["--filter-platform", host])
                .arg("--manifest-path")
                .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
                .output()
                .expect("run Cargo metadata for the pinned librms sources");
            assert!(
                metadata.status.success(),
                "Cargo metadata failed: {}",
                String::from_utf8_lossy(&metadata.stderr)
            );
            let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout).unwrap();
            let packages: Vec<_> = metadata["packages"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|package| package["name"] == "librms")
                .collect();
            assert_eq!(packages.len(), 1, "expected one resolved librms package");
            let proto_dir = std::path::Path::new(packages[0]["manifest_path"].as_str().unwrap())
                .parent()
                .unwrap()
                .join("proto");
            let output_dir = tempfile::tempdir().unwrap();
            let descriptor_path = output_dir.path().join("rms.bin");
            let mut protoc = Command::new(
                std::env::var_os("PROTOC").unwrap_or_else(|| std::ffi::OsString::from("protoc")),
            );
            protoc
                .current_dir(env!("CARGO_MANIFEST_DIR"))
                .arg("--experimental_allow_proto3_optional")
                .arg("--descriptor_set_out")
                .arg(&descriptor_path)
                .arg("-I")
                .arg(&proto_dir);
            if let Some(include) = std::env::var_os("PROTOC_INCLUDE") {
                protoc.arg("-I").arg(include);
            }
            for entry in std::fs::read_dir(&proto_dir).unwrap() {
                let path = entry.unwrap().path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "proto")
                {
                    protoc.arg(path);
                }
            }
            let output = protoc
                .output()
                .expect("run protoc for the RPC catalog check");
            assert!(
                output.status.success(),
                "protoc failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let descriptor = prost_types::FileDescriptorSet::decode(
                std::fs::read(descriptor_path).unwrap().as_slice(),
            )
            .unwrap();
            let mut declared = Vec::new();
            for file in descriptor.file {
                for service in file.service {
                    let name = format!(
                        "{}.{}",
                        file.package.as_ref().unwrap(),
                        service.name.unwrap()
                    );
                    let mut methods: Vec<_> = service
                        .method
                        .into_iter()
                        .map(|m| m.name.unwrap())
                        .collect();
                    methods.sort();
                    declared.push((name, methods));
                }
            }
            declared.sort();
            let mut catalog = Vec::new();
            for (service, methods) in [
                (super::RACK_MANAGER_SERVICE, super::RACK_MANAGER_METHODS),
                (
                    super::RACK_MANAGER_V2_SERVICE,
                    super::RACK_MANAGER_V2_METHODS,
                ),
            ] {
                for method in methods {
                    assert_eq!(canonical_rpc(service, method), Some((service, *method)));
                }
                let mut methods: Vec<_> =
                    methods.iter().map(|method| (*method).to_owned()).collect();
                methods.sort();
                catalog.push((service.to_owned(), methods));
            }
            catalog.sort();
            assert_eq!(catalog, declared, "update the finite tracing RPC catalog");
            Ok(())
        });
    }

    #[test]
    fn grpc_server_error_classification_matches_semantic_conventions() {
        for code in [
            tonic::Code::Unknown,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Unimplemented,
            tonic::Code::Internal,
            tonic::Code::Unavailable,
            tonic::Code::DataLoss,
        ] {
            assert!(grpc_server_status_is_error(code), "{code:?}");
        }
        for code in [
            tonic::Code::Ok,
            tonic::Code::Cancelled,
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
            tonic::Code::AlreadyExists,
            tonic::Code::PermissionDenied,
            tonic::Code::ResourceExhausted,
            tonic::Code::FailedPrecondition,
            tonic::Code::Aborted,
            tonic::Code::OutOfRange,
            tonic::Code::Unauthenticated,
        ] {
            assert!(!grpc_server_status_is_error(code), "{code:?}");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn grpc_status_descriptions_do_not_repeat_codes_or_copy_messages() {
        use opentelemetry::trace::Status;

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let _guard = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("grpc-status-test")))
            .set_default();

        for (header, status_name, status_code, expected_status, expected_error) in [
            ("0", "OK", 0, Status::Ok, None),
            ("13", "INTERNAL", 13, Status::error(""), Some("INTERNAL")),
            (
                "14",
                "UNAVAILABLE",
                14,
                Status::error(""),
                Some("UNAVAILABLE"),
            ),
            ("3", "INVALID_ARGUMENT", 3, Status::Unset, None),
            (
                "private-malformed-status",
                "UNKNOWN",
                2,
                Status::error(""),
                Some("UNKNOWN"),
            ),
        ] {
            for in_trailers in [false, true] {
                let inner = tower::service_fn(
                    move |_request: http::Request<tonic::body::Body>| async move {
                        let mut headers = http::HeaderMap::new();
                        headers.insert("grpc-status", header.parse().unwrap());
                        headers.insert("grpc-message", "private-request-detail".parse().unwrap());
                        let mut response = http::Response::new(TrailersBody(None));
                        if in_trailers {
                            response.body_mut().0 = Some(headers);
                        } else {
                            *response.headers_mut() = headers;
                        }
                        Ok::<_, std::convert::Infallible>(response)
                    },
                );
                let mut service = LogLayer::default().layer(inner);
                let mut response = service
                    .call(grpc_post("/rack_manager.RackManager/GetVersion"))
                    .await
                    .unwrap();
                while std::future::poll_fn(|cx| {
                    std::pin::Pin::new(response.body_mut()).poll_frame(cx)
                })
                .await
                .is_some()
                {}
                drop(response);

                let spans = exporter.get_finished_spans().unwrap();
                let span = spans.last().expect("completed request span");
                assert_eq!(span.status, expected_status);
                assert!(span.attributes.iter().any(|attribute| {
                    attribute.key.as_str() == "rpc.response.status_code"
                        && attribute.value.as_str() == status_name
                }));
                assert!(span.attributes.iter().any(|attribute| {
                    attribute.key.as_str() == "rpc.grpc.status_code"
                        && attribute.value.as_str() == status_code.to_string()
                }));
                let error_type = span
                    .attributes
                    .iter()
                    .find(|attribute| attribute.key.as_str() == "error.type")
                    .map(|attribute| attribute.value.as_str());
                assert_eq!(error_type, expected_error.map(std::borrow::Cow::Borrowed));
                assert!(!format!("{span:?}").contains("private-request-detail"));
                assert!(!format!("{span:?}").contains("private-malformed-status"));
            }
        }
        assert_eq!(exporter.get_finished_spans().unwrap().len(), 10);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_failure_descriptions_report_only_observed_context() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let _guard = tracing_subscriber::registry()
            .with(
                tracing_opentelemetry::layer().with_tracer(provider.tracer("request-failure-test")),
            )
            .set_default();

        for (status, description) in [
            (Some(503), "gRPC response returned HTTP 503"),
            (None, "gRPC service failed before returning a response"),
        ] {
            let inner = tower::service_fn(
                move |_request: http::Request<tonic::body::Body>| async move {
                    match status {
                        Some(status) => Ok(http::Response::builder()
                            .status(status)
                            .body(tonic::body::Body::default())
                            .unwrap()),
                        None => Err("private-service-error-detail"),
                    }
                },
            );
            let mut service = LogLayer::default().layer(inner);
            let response = service
                .call(grpc_post("/rack_manager.RackManager/GetVersion"))
                .await;
            assert_eq!(response.is_ok(), status.is_some());
            drop(response);

            let spans = exporter.get_finished_spans().unwrap();
            let span = spans.last().expect("completed request span");
            assert_eq!(
                span.status,
                opentelemetry::trace::Status::error(description)
            );
            assert!(!format!("{span:?}").contains("private-service-error-detail"));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panicking_request_exports_safe_error_before_drain() {
        use futures::FutureExt as _;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct ObserveCleanup {
            callbacks_during_unwind: Arc<AtomicBool>,
            tokens_at_close: Arc<AtomicUsize>,
            requests: TaskTracker,
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ObserveCleanup {
            fn on_record(
                &self,
                _: &tracing::span::Id,
                _: &tracing::span::Record<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.callbacks_during_unwind
                    .fetch_or(std::thread::panicking(), Ordering::SeqCst);
            }

            fn on_close(&self, _: tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
                self.callbacks_during_unwind
                    .fetch_or(std::thread::panicking(), Ordering::SeqCst);
                self.tokens_at_close
                    .store(self.requests.len(), Ordering::SeqCst);
            }
        }

        if crate::observability::run_test_in_subprocess(concat!(
            "logging::api_logs::tests::",
            "panicking_request_exports_safe_error_before_drain"
        )) {
            return;
        }

        for (phase, description, error_type) in [
            (
                "call",
                "gRPC service panicked before returning a response",
                "request_execution",
            ),
            (
                "future",
                "gRPC service panicked before returning a response",
                "request_execution",
            ),
            ("body", "gRPC response body panicked", "response_body"),
        ] {
            let requests = TaskTracker::new();
            let callbacks_during_unwind = Arc::new(AtomicBool::new(false));
            let tokens_at_close = Arc::new(AtomicUsize::new(0));
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            let _subscriber = tracing_subscriber::registry()
                .with(
                    crate::logging::logfmt::layer()
                        .with_writer(Arc::new(|| Box::new(std::io::sink()))),
                )
                .with(
                    tracing_opentelemetry::layer()
                        .with_tracer(provider.tracer("panicking-request-test")),
                )
                .with(ObserveCleanup {
                    callbacks_during_unwind: callbacks_during_unwind.clone(),
                    tokens_at_close: tokens_at_close.clone(),
                    requests: requests.clone(),
                })
                .set_default();
            let inner = tower::service_fn(move |_request: http::Request<tonic::body::Body>| {
                if phase == "call" {
                    panic!("private-handler-panic-detail");
                }
                std::future::poll_fn(move |_| {
                    if phase == "future" {
                        panic!("private-handler-panic-detail");
                    }
                    std::task::Poll::Ready(Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .header("grpc-status", "0")
                            .body(OutcomeBody::Panic)
                            .unwrap(),
                    ))
                })
            });
            let mut service = LogLayer::default()
                .with_request_tracker(requests.clone(), CancellationToken::new())
                .layer(inner);
            let result = std::panic::AssertUnwindSafe(async {
                let mut response = service
                    .call(grpc_post("/rack_manager.RackManager/GetVersion"))
                    .await
                    .unwrap();
                let _ = std::future::poll_fn(|cx| {
                    std::pin::Pin::new(response.body_mut()).poll_frame(cx)
                })
                .await;
            })
            .catch_unwind()
            .await;
            assert!(
                result.is_err(),
                "middleware must preserve the original panic"
            );
            let panic = result.unwrap_err();
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&if phase == "body" {
                    "private-body-panic-detail"
                } else {
                    "private-handler-panic-detail"
                })
            );
            assert!(!callbacks_during_unwind.load(Ordering::SeqCst), "{phase}");
            assert_eq!(
                tokens_at_close.load(Ordering::SeqCst),
                2,
                "the service and request tokens must remain live until span close"
            );
            drop(service);
            assert!(requests.is_empty());

            let spans = exporter.get_finished_spans().unwrap();
            assert_eq!(spans.len(), 1, "panicked request must export before drain");
            assert_eq!(
                spans[0].status,
                opentelemetry::trace::Status::error(description)
            );
            assert!(spans[0].attributes.iter().any(|attribute| {
                attribute.key.as_str() == "error.type" && attribute.value.as_str() == error_type
            }));
            assert!(!format!("{:?}", spans[0]).contains("private-"));
        }
    }

    enum OutcomeBody {
        Frames(
            std::collections::VecDeque<
                Result<http_body::Frame<tonic::codegen::Bytes>, &'static str>,
            >,
        ),
        Pending,
        Panic,
    }

    impl http_body::Body for OutcomeBody {
        type Data = tonic::codegen::Bytes;
        type Error = &'static str;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            match &mut *self {
                Self::Frames(frames) => std::task::Poll::Ready(frames.pop_front()),
                Self::Pending => std::task::Poll::Pending,
                Self::Panic => panic!("private-body-panic-detail"),
            }
        }

        fn is_end_stream(&self) -> bool {
            matches!(self, Self::Frames(frames) if frames.is_empty())
        }
    }

    fn outcome_trailers(status: Option<&str>) -> OutcomeBody {
        let mut trailers = http::HeaderMap::new();
        if let Some(status) = status {
            trailers.insert("grpc-status", status.parse().unwrap());
        }
        OutcomeBody::Frames([Ok(http_body::Frame::trailers(trailers))].into())
    }

    async fn export_response_outcome(
        request: http::Request<tonic::body::Body>,
        response: http::Response<OutcomeBody>,
        consume_body: bool,
        force_shutdown: bool,
    ) -> SpanData {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let _subscriber = tracing_subscriber::registry()
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("request-outcome-test"))
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        crate::observability::tracing::should_export,
                    )),
            )
            .set_default();
        let response = Arc::new(Mutex::new(Some(response)));
        let inner = tower::service_fn(move |_request: http::Request<tonic::body::Body>| {
            let response = response.lock().unwrap().take().unwrap();
            async move {
                // Production entry logging starts the SDK span. Do the same
                // here so an early Ok cannot be overwritten in a lazy builder.
                let _ = tracing::Span::current().context();
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        let shutdown = CancellationToken::new();
        let mut service = LogLayer::default()
            .with_request_tracker(TaskTracker::new(), shutdown.clone())
            .layer(inner);
        let mut response = service.call(request).await.unwrap();
        assert!(exporter.get_finished_spans().unwrap().is_empty());
        if force_shutdown {
            shutdown.cancel();
        }
        if consume_body {
            while std::future::poll_fn(|cx| std::pin::Pin::new(response.body_mut()).poll_frame(cx))
                .await
                .is_some()
            {}
        }
        drop(response);
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        spans.into_iter().next().unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn only_get_version_confirms_success_after_response_completion() {
        for (uri, expected_status) in [
            (
                "/rack_manager.RackManager/GetVersion",
                opentelemetry::trace::Status::Ok,
            ),
            (
                "/rack_manager.RackManager/UpdateFirmware",
                opentelemetry::trace::Status::Unset,
            ),
            (
                "/unknown.Service/GetVersion",
                opentelemetry::trace::Status::Unset,
            ),
            (
                "/rack_manager.RackManager/GetVersion?query=1",
                opentelemetry::trace::Status::Unset,
            ),
        ] {
            for in_trailers in [false, true] {
                let response = if in_trailers {
                    http::Response::new(outcome_trailers(Some("0")))
                } else {
                    http::Response::builder()
                        .header("grpc-status", "0")
                        .body(OutcomeBody::Frames(Default::default()))
                        .unwrap()
                };
                let span = export_response_outcome(grpc_post(uri), response, true, false).await;
                assert_eq!(
                    span.status, expected_status,
                    "{uri}, trailers={in_trailers}"
                );
                assert!(
                    !span
                        .attributes
                        .iter()
                        .any(|attribute| attribute.key.as_str() == "error.type")
                );
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_version_response_failures_never_become_ok() {
        for (body, consume_body, force_shutdown, expected_error) in [
            (
                OutcomeBody::Frames([Err("private-body-error")].into()),
                true,
                false,
                "response_body",
            ),
            (OutcomeBody::Pending, false, false, "response_body"),
            (OutcomeBody::Pending, false, true, "response_body"),
        ] {
            let response = http::Response::builder()
                .header("grpc-status", "0")
                .body(body)
                .unwrap();
            let span = export_response_outcome(
                grpc_post("/rack_manager.RackManager/GetVersion"),
                response,
                consume_body,
                force_shutdown,
            )
            .await;
            assert!(matches!(
                span.status,
                opentelemetry::trace::Status::Error { .. }
            ));
            assert!(
                span.attributes
                    .iter()
                    .any(|attribute| attribute.key.as_str() == "error.type"
                        && attribute.value.as_str() == expected_error)
            );
            assert!(!format!("{span:?}").contains("private-body-error"));
        }
        for body in [
            OutcomeBody::Frames(Default::default()),
            outcome_trailers(None),
        ] {
            let span = export_response_outcome(
                grpc_post("/rack_manager.RackManager/GetVersion"),
                http::Response::new(body),
                true,
                false,
            )
            .await;
            assert_eq!(
                span.status,
                opentelemetry::trace::Status::error("gRPC response ended without a status")
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_version_later_ok_does_not_override_earlier_failure() {
        for (http_status, grpc_status, expected_status, error_type) in [
            (
                503,
                "0",
                opentelemetry::trace::Status::error("gRPC response returned HTTP 503"),
                Some("http_status"),
            ),
            (
                200,
                "13",
                opentelemetry::trace::Status::error(""),
                Some("INTERNAL"),
            ),
            (200, "3", opentelemetry::trace::Status::Unset, None),
        ] {
            let response = http::Response::builder()
                .status(http_status)
                .header("grpc-status", grpc_status)
                .body(outcome_trailers(Some("0")))
                .unwrap();
            let span = export_response_outcome(
                grpc_post("/rack_manager.RackManager/GetVersion"),
                response,
                true,
                false,
            )
            .await;
            assert_eq!(span.status, expected_status);
            let actual_error = span
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == "error.type");
            assert_eq!(
                actual_error.map(|attribute| attribute.value.as_str()),
                error_type.map(std::borrow::Cow::Borrowed)
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_get_version_dropped_before_response_never_becomes_ok() {
        for force_shutdown in [false, true] {
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            let _subscriber = tracing_subscriber::registry()
                .with(
                    tracing_opentelemetry::layer()
                        .with_tracer(provider.tracer("pending-request-test")),
                )
                .set_default();
            let inner = tower::service_fn(|_request: http::Request<tonic::body::Body>| async {
                let _ = tracing::Span::current().context();
                std::future::pending::<
                    Result<http::Response<tonic::body::Body>, std::convert::Infallible>,
                >()
                .await
            });
            let shutdown = CancellationToken::new();
            let mut service = LogLayer::default()
                .with_request_tracker(TaskTracker::new(), shutdown.clone())
                .layer(inner);
            let mut future = service.call(grpc_post("/rack_manager.RackManager/GetVersion"));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            if force_shutdown {
                shutdown.cancel();
            }
            drop(future);
            let spans = exporter.get_finished_spans().unwrap();
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0].status, opentelemetry::trace::Status::Unset);
            let rpc_status = spans[0]
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == "rpc.response.status_code");
            assert_eq!(
                rpc_status.map(|attribute| attribute.value.as_str()),
                force_shutdown.then_some(std::borrow::Cow::Borrowed("CANCELLED"))
            );
        }
    }

    #[test]
    fn cert_peer_identity_returns_subject_dn() {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "rms-audit-client");
        let cert = params.self_signed(&key).unwrap();
        let identity = cert_peer_identity(cert.der());
        assert!(
            identity.contains("rms-audit-client"),
            "expected subject DN, got {identity}"
        );
    }

    #[test]
    fn cert_peer_identity_strips_control_characters_from_subject() {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "evil\nCN=spoofed");
        let cert = params.self_signed(&key).unwrap();
        let identity = cert_peer_identity(cert.der());
        assert!(
            !identity.contains('\n'),
            "control characters must be stripped, got {identity:?}"
        );
        assert!(
            identity.contains("evil") && identity.contains("CN=spoofed"),
            "expected sanitized subject, got {identity}"
        );
    }

    #[test]
    fn cert_peer_identity_falls_back_to_sha256_fingerprint() {
        let identity = cert_peer_identity(b"not-a-valid-cert");
        assert!(
            identity.starts_with("sha256:"),
            "expected fingerprint fallback, got {identity}"
        );
    }

    // ── LogService entry-event tests ─────────────────────────────────────────

    /// A `Clone`able, thread-safe byte buffer that implements `std::io::Write`.
    /// Used to capture `LogFmtLayer` output inside tests without touching stdout.
    #[derive(Clone)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    struct TrailersBody(Option<http::HeaderMap>);

    impl http_body::Body for TrailersBody {
        type Data = tonic::codegen::Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            std::task::Poll::Ready(self.0.take().map(http_body::Frame::trailers).map(Ok))
        }
    }

    impl SharedBuffer {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Vec::new())))
        }

        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Builds a `LogService` wrapping a no-op inner service, installs a
    /// `LogFmtLayer` capturing all output into a `SharedBuffer`, dispatches
    /// `request`, and returns the captured log lines.
    async fn call_log_service(request: http::Request<tonic::body::Body>) -> Vec<String> {
        call_log_service_with_span(request).await.0
    }

    async fn call_log_service_with_span(
        request: http::Request<tonic::body::Body>,
    ) -> (Vec<String>, SpanData) {
        let buf = SharedBuffer::new();
        let buf_clone = buf.clone();

        let fmt_layer = crate::logging::logfmt::layer().with_writer(Arc::new(
            move || -> Box<dyn std::io::Write> { Box::new(buf_clone.clone()) },
        ));
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::TraceIdRatioBased(1.0))
            .with_simple_exporter(exporter.clone())
            .build();
        let otel_layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("api-logs-test"))
            .with_filter(tracing_subscriber::filter::filter_fn(
                crate::observability::tracing::should_export,
            ));
        let _guard = tracing_subscriber::registry()
            .with(fmt_layer)
            .with(otel_layer)
            .set_default();

        let inner = tower::service_fn(|_req: http::Request<tonic::body::Body>| async {
            Ok::<_, std::convert::Infallible>(http::Response::new(tonic::body::Body::default()))
        });

        let mut service: LogService<_> = LogLayer::default().layer(inner);
        let _ = service.call(request).await;

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        let methods: Vec<_> = spans[0]
            .attributes
            .iter()
            .filter(|attribute| attribute.key.as_str() == "rpc.method")
            .map(|attribute| attribute.value.as_str())
            .collect();
        // Inspect every value: a map or first-match lookup would hide duplicates.
        assert_eq!(methods, vec![spans[0].name.as_ref()]);

        (
            buf.text().lines().map(str::to_string).collect(),
            spans.into_iter().next().unwrap(),
        )
    }

    fn grpc_post(uri: &str) -> http::Request<tonic::body::Body> {
        http::Request::builder()
            .method("POST")
            .uri(uri)
            .body(tonic::body::Body::default())
            .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn entry_event_is_emitted_for_grpc_request() {
        let lines = call_log_service(grpc_post("/rack_manager.RackManager/UpdateFirmware")).await;

        let entry = lines
            .iter()
            .find(|l| l.contains("request received"))
            .unwrap_or_else(|| panic!("entry event missing; lines: {lines:?}"));

        assert!(
            entry.contains("level=INFO"),
            "expected level=INFO in entry event: {entry}"
        );
        // LogFmtLayer renders dots in field names as underscores.
        assert!(
            entry.contains("rpc_method=UpdateFirmware"),
            "expected rpc_method in entry event: {entry}"
        );
        assert!(
            entry.contains("rpc_service=rack_manager.RackManager"),
            "expected rpc_service in entry event: {entry}"
        );
        let closing_span = lines
            .iter()
            .find(|line| line.contains("span_name=request"))
            .unwrap();
        assert!(closing_span.contains("rpc_method=rack_manager.RackManager/UpdateFirmware"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_preserves_completed_response_status() {
        for (grpc_status, expected_status) in [("0", "OK"), ("14", "UNAVAILABLE")] {
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            let _subscriber = tracing_subscriber::registry()
                .with(
                    tracing_opentelemetry::layer().with_tracer(provider.tracer("shutdown-status")),
                )
                .set_default();
            let requests = TaskTracker::new();
            let shutdown = CancellationToken::new();
            let inner = tower::service_fn(
                move |_request: http::Request<tonic::body::Body>| async move {
                    Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .header("grpc-status", grpc_status)
                            .body(tonic::body::Body::empty())
                            .unwrap(),
                    )
                },
            );
            let mut service = LogLayer::default()
                .with_request_tracker(requests.clone(), shutdown.clone())
                .layer(inner);
            let response = service
                .call(grpc_post("/rack_manager.RackManager/GetVersion"))
                .await
                .unwrap();
            shutdown.cancel();
            drop(response);
            drop(service);
            requests.close();
            requests.wait().await;
            provider.force_flush().unwrap();
            let spans = exporter.get_finished_spans().unwrap();
            assert_eq!(spans.len(), 1);
            assert_eq!(
                spans[0].status,
                if grpc_status == "0" {
                    opentelemetry::trace::Status::Ok
                } else {
                    opentelemetry::trace::Status::error("")
                }
            );
            assert!(spans[0].attributes.iter().any(|attribute| {
                attribute.key.as_str() == "rpc.response.status_code"
                    && attribute.value.as_str() == expected_status
            }));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_tracks_hyper_requests_before_their_first_poll() {
        for poll_request in [false, true] {
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            let _subscriber = tracing_subscriber::registry()
                .with(
                    tracing_opentelemetry::layer()
                        .with_tracer(provider.tracer("shutdown-unpolled-request")),
                )
                .set_default();
            let requests = TaskTracker::new();
            let inner =
                tower::service_fn(|_request: http::Request<tonic::body::Body>| async move {
                    Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .header("grpc-status", "0")
                            .body(tonic::body::Body::empty())
                            .unwrap(),
                    )
                });
            let service = LogLayer::default()
                .with_request_tracker(requests.clone(), CancellationToken::new())
                .layer(inner);
            let hyper_service = hyper_util::service::TowerToHyperService::new(service);
            // Hyper constructs this Oneshot before spawning the H2 stream task.
            // LogService::call has not run yet, so only its service token can
            // prevent an ended connection from racing this task's first poll.
            let future = hyper::service::Service::call(
                &hyper_service,
                grpc_post("/rack_manager.RackManager/GetVersion"),
            );
            drop(hyper_service);
            requests.close();
            let mut drain = Box::pin(requests.wait());
            let state =
                std::future::poll_fn(|cx| std::task::Poll::Ready(drain.as_mut().poll(cx))).await;
            assert!(
                state.is_pending(),
                "unpolled Hyper request must keep drain open"
            );
            assert!(exporter.get_finished_spans().unwrap().is_empty());

            if poll_request {
                let response = future.await.unwrap();
                assert!(exporter.get_finished_spans().unwrap().is_empty());
                drop(response);
            } else {
                drop(future);
            }
            drain.await;
            provider.force_flush().unwrap();
            assert_eq!(
                exporter.get_finished_spans().unwrap().len(),
                usize::from(poll_request),
                "started request span must close before the tracker drains",
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_span_stays_open_until_the_response_body_is_dropped() {
        let buf = SharedBuffer::new();
        let buf_clone = buf.clone();
        let fmt_layer = crate::logging::logfmt::layer().with_writer(Arc::new(
            move || -> Box<dyn std::io::Write> { Box::new(buf_clone.clone()) },
        ));
        let provider = SdkTracerProvider::builder().build();
        let otel_layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("response-body-test"))
            .with_filter(tracing_subscriber::filter::filter_fn(
                crate::observability::tracing::should_export,
            ));
        let _guard = tracing_subscriber::registry()
            .with(fmt_layer)
            .with(otel_layer)
            .set_default();
        let inner = tower::service_fn(|_request: http::Request<tonic::body::Body>| async {
            Ok::<_, std::convert::Infallible>(http::Response::new(tonic::body::Body::default()))
        });
        let mut service: LogService<_> = LogLayer::default().layer(inner);

        let response = service
            .call(grpc_post("/rack_manager.RackManager/GetVersion"))
            .await
            .unwrap();
        assert!(
            !buf.text()
                .lines()
                .any(|line| line.contains("level=SPAN") && line.contains("span_name=request")),
            "request span closed before response body completion"
        );

        drop(response);
        let output = buf.text();
        let closing_span = output
            .lines()
            .find(|line| line.contains("level=SPAN") && line.contains("span_name=request"))
            .expect("request span did not close when response body was dropped");
        assert!(closing_span.contains("rpc_response_status_code=UNKNOWN"));
        assert!(!closing_span.contains("rpc_response_status_code=OK"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trailers_without_grpc_status_are_recorded_as_unknown() {
        let buf = SharedBuffer::new();
        let buf_clone = buf.clone();
        let fmt_layer = crate::logging::logfmt::layer().with_writer(Arc::new(
            move || -> Box<dyn std::io::Write> { Box::new(buf_clone.clone()) },
        ));
        let provider = SdkTracerProvider::builder().build();
        let otel_layer =
            tracing_opentelemetry::layer().with_tracer(provider.tracer("trailer-test"));
        let _guard = tracing_subscriber::registry()
            .with(fmt_layer)
            .with(otel_layer)
            .set_default();
        let span = tracing::info_span!(
            "request",
            rpc.response.status_code = tracing::field::Empty,
            rpc.grpc.status_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            error.type = tracing::field::Empty,
        );
        let request_span = super::RequestSpan {
            span,
            _request: tokio_util::task::TaskTracker::new().token(),
            force_shutdown: CancellationToken::new(),
            status_recorded: false,
            can_confirm_success: false,
        };
        let mut body = LogResponseBody::new(
            TrailersBody(Some(http::HeaderMap::new())),
            request_span,
            None,
        );
        std::future::poll_fn(|context| std::pin::Pin::new(&mut body).poll_frame(context))
            .await
            .expect("trailer frame")
            .expect("infallible body");
        drop(body);

        let output = buf.text();
        let closing_span = output
            .lines()
            .find(|line| line.contains("span_name=request"))
            .expect("request close line");
        assert!(closing_span.contains("rpc_response_status_code=UNKNOWN"));
        assert!(closing_span.contains("error_type=missing_grpc_status"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn non_post_request_emits_bounded_caller_attribution() {
        let request = http::Request::builder()
            .method("GET")
            .uri("/rack_manager.RackManager/UpdateFirmware")
            .body(tonic::body::Body::default())
            .unwrap();

        let lines = call_log_service(request).await;
        let entry = lines
            .iter()
            .find(|line| line.contains("request received"))
            .unwrap();
        assert!(entry.contains("rpc_method=_OTHER"));
        assert!(entry.contains("rpc_service=_OTHER"));
        assert!(!entry.contains("UpdateFirmware"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_path_emits_bounded_caller_attribution() {
        let lines =
            call_log_service(grpc_post("/rack_manager.RackManager/UpdateFirmware?foo=1")).await;

        let entry = lines
            .iter()
            .find(|line| line.contains("request received"))
            .unwrap();
        assert!(entry.contains("rpc_method=_OTHER"));
        assert!(!entry.contains("foo=1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_path_emits_bounded_caller_attribution() {
        // A path with more than two segments does not match the /<service>/<method> shape.
        let lines = call_log_service(grpc_post("/some/extra/segments")).await;

        let entry = lines
            .iter()
            .find(|line| line.contains("request received"))
            .unwrap();
        assert!(entry.contains("rpc_method=_OTHER"));
        assert!(!entry.contains("/some/extra/segments"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unknown_rpc_method_uses_fixed_trace_cardinality() {
        let lines =
            call_log_service(grpc_post("/rack_manager.RackManager/AttackerControlled")).await;

        let entry = lines
            .iter()
            .find(|line| line.contains("request received"))
            .unwrap();
        assert!(entry.contains("rpc_method=_OTHER"));
        assert!(!entry.contains("AttackerControlled"));
        let closing_span = lines
            .iter()
            .find(|line| line.contains("span_name=request"))
            .unwrap_or_else(|| panic!("closing span missing; lines: {lines:?}"));
        assert!(closing_span.contains("rpc_method=_OTHER"));
        assert!(!closing_span.contains("AttackerControlled"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn entry_event_trace_context_matches_closing_span() {
        let lines = call_log_service(grpc_post("/rack_manager.RackManager/GetVersion")).await;

        let entry = lines
            .iter()
            .find(|l| l.contains("request received"))
            .unwrap_or_else(|| panic!("entry event missing; lines: {lines:?}"));
        let closing_span = lines
            .iter()
            .find(|l| l.contains("level=SPAN") && l.contains("span_name=request"))
            .unwrap_or_else(|| panic!("closing span missing; lines: {lines:?}"));

        fn field_from<'a>(line: &'a str, field: &str) -> &'a str {
            let prefix = format!("{field}=");
            let start = line
                .find(&prefix)
                .unwrap_or_else(|| panic!("{field} field"))
                + prefix.len();
            let end = line[start..]
                .find(' ')
                .map(|i| start + i)
                .unwrap_or(line.len());
            &line[start..end]
        }

        for (field, expected_len) in [("trace_id", 32), ("span_id", 16), ("trace_flags", 2)] {
            let entry_value = field_from(entry, field);
            assert_eq!(entry_value, field_from(closing_span, field));
            assert_eq!(entry_value.len(), expected_len);
            assert!(entry_value.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inbound_traceparent_preserves_identity_for_both_sampling_flags() {
        const CLIENT_TRACE_ID: &str = "11111111111111111111111111111111";
        const CLIENT_SPAN_ID: &str = "2222222222222222";
        for flags in ["00", "01", "fe", "ff"] {
            let mut request = grpc_post("/rack_manager.RackManager/GetVersion");
            request.headers_mut().insert(
                "traceparent",
                format!("00-{CLIENT_TRACE_ID}-{CLIENT_SPAN_ID}-{flags}")
                    .parse()
                    .unwrap(),
            );
            request
                .headers_mut()
                .insert("tracestate", "vendor=private-caller-value".parse().unwrap());
            request
                .headers_mut()
                .insert("baggage", "secret=private-caller-value".parse().unwrap());

            let (lines, span) = call_log_service_with_span(request).await;
            assert_eq!(span.span_context.trace_id().to_string(), CLIENT_TRACE_ID);
            assert_eq!(span.parent_span_id.to_string(), CLIENT_SPAN_ID);
            assert_ne!(span.span_context.span_id().to_string(), CLIENT_SPAN_ID);
            assert!(span.span_context.is_sampled());
            assert!(!span.span_context.is_remote());
            assert!(span.span_context.trace_state().header().is_empty());
            assert!(!format!("{span:?}{lines:?}").contains("private-caller-value"));
            for marker in ["request received", "span_name=request"] {
                let line = lines.iter().find(|line| line.contains(marker)).unwrap();
                assert!(line.contains(&format!("trace_id={CLIENT_TRACE_ID}")));
                assert!(line.contains(&format!("span_id={}", span.span_context.span_id())));
                assert!(line.contains("trace_flags=01"));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_or_missing_traceparent_starts_an_independent_root() {
        const VALID: &str = "00-11111111111111111111111111111111-2222222222222222-01";
        let ambient_trace = TraceId::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let ambient = opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
            ambient_trace,
            SpanId::from_hex("bbbbbbbbbbbbbbbb").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ));
        let _ambient_guard = ambient.attach();
        let oversized = format!(
            "01-{}-{}-01-{}",
            "1".repeat(32),
            "2".repeat(16),
            "x".repeat(512)
        );
        for headers in [
            vec![],
            vec!["malformed-private-value"],
            vec!["00-1-2-01"],
            vec!["00-00000000000000000000000000000000-2222222222222222-01"],
            vec!["00-11111111111111111111111111111111-0000000000000000-01"],
            vec!["00-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA-2222222222222222-01"],
            vec!["00-11111111111111111111111111111111-AAAAAAAAAAAAAAAA-01"],
            vec!["ff-11111111111111111111111111111111-2222222222222222-01"],
            vec!["0A-11111111111111111111111111111111-2222222222222222-01"],
            vec!["00:11111111111111111111111111111111-2222222222222222-01"],
            vec!["00-11111111111111111111111111111111:2222222222222222-01"],
            vec!["00-11111111111111111111111111111111-2222222222222222:01"],
            vec!["00-11111111111111111111111111111111-2222222222222222-01-"],
            vec!["00-11111111111111111111111111111111-2222222222222222-0g"],
            vec!["00-11111111111111111111111111111111-2222222222222222-FF"],
            vec!["00-11111111111111111111111111111111-2222222222222222-1"],
            vec!["01-11111111111111111111111111111111-2222222222222222-01future"],
            vec![VALID, VALID],
            vec![VALID, "malformed-private-value"],
            vec!["malformed-private-value", VALID],
            vec![oversized.as_str()],
        ] {
            let mut request = grpc_post("/rack_manager.RackManager/GetVersion");
            for value in headers {
                request
                    .headers_mut()
                    .append("traceparent", value.parse().unwrap());
            }
            request
                .headers_mut()
                .insert("tracestate", "vendor=private-caller-value".parse().unwrap());
            let (lines, span) = call_log_service_with_span(request).await;
            assert_eq!(span.parent_span_id, SpanId::INVALID);
            assert_ne!(span.span_context.trace_id(), ambient_trace);
            assert_ne!(
                span.span_context.trace_id().to_string(),
                "11111111111111111111111111111111"
            );
            assert!(span.span_context.is_sampled());
            assert!(span.span_context.trace_state().header().is_empty());
            assert!(!format!("{span:?}{lines:?}").contains("private-"));
        }
    }

    #[test]
    fn traceparent_extraction_accepts_future_versions_and_http_whitespace() {
        for (value, flags) in [
            ("00-11111111111111111111111111111111-2222222222222222-00", 0),
            ("00-11111111111111111111111111111111-2222222222222222-01", 1),
            ("00-11111111111111111111111111111111-2222222222222222-fe", 0),
            ("00-11111111111111111111111111111111-2222222222222222-ff", 1),
            (
                " \t00-11111111111111111111111111111111-2222222222222222-01\t ",
                1,
            ),
            ("01-11111111111111111111111111111111-2222222222222222-01", 1),
            (
                "01-11111111111111111111111111111111-2222222222222222-01-future",
                1,
            ),
            (
                "01-11111111111111111111111111111111-2222222222222222-01-",
                1,
            ),
            ("fe-11111111111111111111111111111111-2222222222222222-fe", 0),
        ] {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                http::header::HeaderName::from_bytes(b"TraceParent").unwrap(),
                value.parse().unwrap(),
            );
            let parent = super::request_parent_context(&headers).expect("valid W3C context");
            let span = parent.span();
            let context = span.span_context();
            assert!(context.is_remote());
            assert!(context.is_valid());
            assert_eq!(
                context.trace_id().to_string(),
                "11111111111111111111111111111111"
            );
            assert_eq!(context.span_id().to_string(), "2222222222222222");
            assert_eq!(context.trace_flags().to_u8(), flags, "{value}");
            assert!(context.trace_state().header().is_empty());
        }
    }

    #[test]
    fn traceparent_extraction_bounds_raw_headers_and_rejects_non_ascii() {
        const VALID: &str = "00-11111111111111111111111111111111-2222222222222222-01";
        const FUTURE: &str = "01-11111111111111111111111111111111-2222222222222222-01-";
        for (value, accepted) in [
            (format!("{FUTURE}{}", "x".repeat(512 - FUTURE.len())), true),
            (format!("{FUTURE}{}", "x".repeat(513 - FUTURE.len())), false),
            (format!("{}{VALID}", " ".repeat(512 - VALID.len())), true),
            (format!("{}{VALID}", " ".repeat(513 - VALID.len())), false),
            (format!("{FUTURE}\u{00e9}"), false),
        ] {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                "traceparent",
                http::HeaderValue::from_bytes(value.as_bytes()).unwrap(),
            );
            assert_eq!(
                super::request_parent_context(&headers).is_some(),
                accepted,
                "{value:?}"
            );
        }
    }
}
