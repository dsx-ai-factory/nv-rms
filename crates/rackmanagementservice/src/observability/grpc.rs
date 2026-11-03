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

//! OTLP/gRPC export with collector-directed retry delays.

use std::sync::Mutex;
use std::time::Duration;

use opentelemetry_otlp::RetryPolicy;
use opentelemetry_otlp::retry::{RetryErrorType, retry_with_backoff};
use opentelemetry_otlp::retry_classification::grpc::classify_tonic_status;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
};
use opentelemetry_proto::transform::common::tonic::ResourceAttributesWithSchema;
use opentelemetry_proto::transform::trace::tonic::group_spans_by_resource_and_scope;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic_types::StatusExt as _;

use crate::config::is_loopback_ip_endpoint;

#[derive(Debug)]
pub(super) enum OtlpSpanExporter {
    Grpc(GrpcSpanExporter),
    Http(opentelemetry_otlp::SpanExporter),
}

impl SpanExporter for OtlpSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        match self {
            Self::Grpc(exporter) => exporter.export(batch).await,
            Self::Http(exporter) => exporter.export(batch).await,
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        match self {
            Self::Grpc(exporter) => exporter.force_flush(),
            Self::Http(exporter) => exporter.force_flush(),
        }
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        match self {
            Self::Grpc(exporter) => exporter.shutdown_with_timeout(timeout),
            Self::Http(exporter) => exporter.shutdown_with_timeout(timeout),
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        match self {
            Self::Grpc(exporter) => exporter.set_resource(resource),
            Self::Http(exporter) => exporter.set_resource(resource),
        }
    }
}

#[derive(Debug)]
pub(super) struct GrpcSpanExporter {
    client: Mutex<Option<TraceServiceClient<Channel>>>,
    retry_policy: RetryPolicy,
    resource: ResourceAttributesWithSchema,
}

impl GrpcSpanExporter {
    pub(super) fn new(
        endpoint: &str,
        timeout: Duration,
        retry_policy: RetryPolicy,
    ) -> eyre::Result<Self> {
        let loopback = is_loopback_ip_endpoint(endpoint);
        let endpoint = Endpoint::from_shared(endpoint.to_owned())?.timeout(timeout);
        let endpoint = match endpoint.uri().scheme_str() {
            Some("https") => endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?,
            Some("http") if loopback => endpoint,
            _ => eyre::bail!(
                "OTLP/gRPC trace export requires https unless the endpoint is a loopback IP address"
            ),
        };
        Ok(Self {
            client: Mutex::new(Some(TraceServiceClient::new(endpoint.connect_lazy()))),
            retry_policy,
            resource: ResourceAttributesWithSchema::default(),
        })
    }
}

impl SpanExporter for GrpcSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let client = self
            .client
            .lock()
            .map_err(|_| OTelSdkError::InternalFailure("OTLP client lock poisoned".into()))?
            .clone()
            .ok_or(OTelSdkError::AlreadyShutdown)?;
        let request = ExportTraceServiceRequest {
            resource_spans: group_spans_by_resource_and_scope(batch, &self.resource),
        };
        let response = retry_with_backoff(
            Tokio,
            self.retry_policy.clone(),
            classify_status,
            "TonicTracesClient.Export",
            || {
                let mut client = client.clone();
                let request = request.clone();
                async move { client.export(request).await }
            },
        )
        .await
        .map_err(|status| {
            // Collector-controlled messages and details must not enter RMS logs.
            OTelSdkError::InternalFailure(format!(
                "OTLP/gRPC export failed with code {:?}",
                status.code()
            ))
        })?;
        if let Some(partial) = response.into_inner().partial_success
            && (partial.rejected_spans > 0 || !partial.error_message.is_empty())
        {
            // Preserve the audited numeric bridge without exporting collector text.
            tracing::event!(
                name: "TonicTracesClient.PartialSuccess",
                target: "opentelemetry-otlp",
                tracing::Level::WARN,
                rejected_spans = partial.rejected_spans,
            );
            tracing::warn!(
                protocol = "grpc",
                rejected_spans = partial.rejected_spans,
                "OpenTelemetry Collector reported partial success for a trace batch"
            );
        }
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.client
            .lock()
            .map_err(|_| OTelSdkError::InternalFailure("OTLP client lock poisoned".into()))?
            .take()
            .map(|_| ())
            .ok_or(OTelSdkError::AlreadyShutdown)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }
}

fn classify_status(status: &tonic::Status) -> RetryErrorType {
    // OTLP allows RetryInfo on both statuses. Upstream 0.32 handles only
    // RESOURCE_EXHAUSTED and truncates its delay to whole seconds.
    if matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::ResourceExhausted
    ) && let Some(delay) = status
        .get_details_retry_info()
        .and_then(|info| info.retry_delay)
    {
        // The batch deadline bounds this wait; never retry sooner than requested.
        return RetryErrorType::Throttled(delay);
    }
    classify_tonic_status(status)
}

#[cfg(test)]
mod tests {
    use tonic_types::ErrorDetails;

    use super::*;

    fn exporter(endpoint: &str) -> eyre::Result<GrpcSpanExporter> {
        GrpcSpanExporter::new(
            endpoint,
            Duration::from_secs(1),
            RetryPolicy {
                max_retries: 0,
                initial_delay_ms: 1,
                max_delay_ms: 1,
                jitter_ms: 0,
            },
        )
    }

    #[test]
    fn plaintext_grpc_exporter_is_rejected_unless_http_to_a_loopback_ip() {
        for endpoint in [
            "http://collector.example:4317",
            "http://localhost:4317",
            "http://10.0.0.1:4317",
            "http://127.1:4317",
            "grpc://127.0.0.1:4317",
        ] {
            let error =
                exporter(endpoint).expect_err("the transport must not allow plaintext export");
            assert!(
                error.to_string().contains("requires https"),
                "{endpoint}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn plaintext_grpc_exporter_is_allowed_for_loopback_ip_addresses() {
        for endpoint in [
            "http://127.0.0.1:4317",
            "HTTP://127.0.0.1:4317",
            "http://[::1]:4317",
        ] {
            exporter(endpoint).expect("a loopback Collector may use plaintext");
        }
    }

    #[test]
    fn collector_delays_are_not_capped_and_malformed_hints_keep_status_classification() {
        let delay = Duration::from_millis(700_250);
        for (code, malformed_class) in [
            (tonic::Code::Unavailable, RetryErrorType::Retryable),
            (tonic::Code::ResourceExhausted, RetryErrorType::NonRetryable),
        ] {
            let status = tonic::Status::with_error_details(
                code,
                "collector-controlled message",
                ErrorDetails::with_retry_info(Some(delay)),
            );
            assert_eq!(classify_status(&status), RetryErrorType::Throttled(delay));
            let malformed = tonic::Status::with_details(code, "", vec![0xff].into());
            assert_eq!(classify_status(&malformed), malformed_class);
        }
    }
}
