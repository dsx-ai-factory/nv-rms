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

use std::task::{Context, Poll};

use sha2::{Digest, Sha256};
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tracing::Instrument;
use x509_parser::prelude::FromDer;

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
pub struct LogLayer {}

impl<S> tower::Layer<S> for LogLayer {
    type Service = LogService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LogService { service }
    }
}

// This service implements the RMS API server logging behavior
#[derive(Clone, Debug)]
pub struct LogService<S> {
    service: S,
}

impl<S, RequestBody, ResponseBody> tower::Service<http::Request<RequestBody>> for LogService<S>
where
    S: tower::Service<http::Request<RequestBody>, Response = http::Response<ResponseBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    RequestBody: tonic::codegen::Body + Send + 'static,
    ResponseBody: tonic::codegen::Body + Send + 'static,
{
    type Response = http::Response<ResponseBody>;
    type Error = S::Error;
    type Future = tonic::codegen::BoxFuture<Self::Response, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<RequestBody>) -> Self::Future {
        let mut service = self.service.clone();
        let span_id = format!("{:#x}", u64::from_le_bytes(rand::random::<[u8; 8]>()));

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

            let request_span = tracing::span!(
                parent: None,
                tracing::Level::INFO,
                "request",
                span_id,
                http.url = %request.uri(),
                http.response.status_code = tracing::field::Empty,
                request = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                otel.status_message = tracing::field::Empty,
                rpc.method = tracing::field::Empty,
                rpc.service = tracing::field::Empty,
                rpc.grpc.status_code = tracing::field::Empty,
                rpc.grpc.status_description = tracing::field::Empty,
                logfmt.suppress = tracing::field::Empty,
                peer_addr = peer_addr.map(|a| a.to_string()).unwrap_or_else(|| "unknown".to_string()),
                peer_identity,
            );

            // Try to extract the gRPC service and method from the URI
            let mut grpc_method: Option<String> = None;
            let mut grpc_service: Option<String> = None;
            if let Some(path) = request.uri().path_and_query()
                && *request.method() == http::Method::POST
                && path.query().is_none()
            {
                let parts: Vec<&str> = path.path().split('/').collect();
                if let ["", service, method] = parts.as_slice() {
                    // the path starts with an empty segment, and the middle
                    // segment is the service name, the last segment is the
                    // method
                    grpc_service = Some((*service).to_string());
                    grpc_method = Some((*method).to_string());
                }
            }

            if let Some(service) = &grpc_service {
                request_span.record("rpc.service", service);
            }
            if let Some(method) = &grpc_method {
                request_span.record("rpc.method", method);
            }

            let result = service.call(request).instrument(request_span.clone()).await;

            // Holds the overall outcome of the request as a single log message
            let mut outcome: Result<(), String> = Ok(());

            match &result {
                Ok(result) => {
                    request_span.record("http.response.status_code", result.status().as_u16());

                    if result.status() == http::StatusCode::OK {
                        // In gRPC the actual message status is not in the http status code,
                        // but actually in a header (and sometimes even a trailer - but we ignore this case here since
                        // we don't do streaming).
                        //
                        // Unfortunately we have to reconstruct the status here, by parsing
                        // those headers again
                        let code = match result.headers().get("grpc-status") {
                            Some(header) => tonic::Code::from_bytes(header.as_ref()),
                            None => {
                                // The header is not set in case of successful responses
                                tonic::Code::Ok
                            }
                        };
                        let message = result
                            .headers()
                            .get("grpc-message")
                            .map(|header| {
                                // TODO: The header is percent encoded
                                // We only do basic (space) decoding for now
                                // percent_decode(header.as_bytes())
                                //     .decode_utf8()
                                //     .map(|cow| cow.to_string())
                                std::str::from_utf8(header.as_bytes())
                                    .unwrap_or("Invalid UTF8 Message")
                                    .replace("%20", " ")
                            })
                            .unwrap_or_else(String::new);

                        request_span.record("rpc.grpc.status_code", code as u64);
                        request_span.record(
                            "rpc.grpc.status_description",
                            format!("Code: {}, Message: {}", code.description(), message),
                        );
                        if code != tonic::Code::Ok {
                            outcome = Err(format!(
                                "gRPC Error: {}. Message: {}",
                                code.description(),
                                message
                            ));
                        }
                    } else {
                        outcome = Err(format!("HTTP status: {}", result.status()));
                    }
                }
                Err(_) => {
                    outcome = Err("HTTP execution error".to_string());
                }
            }

            request_span.record(
                "otel.status_code",
                if outcome.is_ok() { "ok" } else { "error" },
            );
            if let Err(e) = outcome {
                // Writing this field will set the span status to error
                // Therefore we only write it on errors
                request_span.record("otel.status_message", e);
            }

            result
        })
    }
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, DnType, KeyPair};

    use super::cert_peer_identity;

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
}
