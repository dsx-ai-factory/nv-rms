/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

use std::sync::Arc;

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get};
use prometheus::{Registry, TextEncoder};
use tonic_prometheus_layer::metrics::{Error as MetricsError, GlobalSettings, try_init_settings};

use crate::api::grpc::server::TlsMode;
use crate::utilities::error::{Result, RmsError};

pub mod server;

pub use server::MetricsServer;

/// Wrapper around key configuration values for RMS, meant to be consumed by the metrics subsystem.
pub struct MetricsInfo {
    /// The port on which the gRPC server is listening. Example: 8801.
    pub grpc_port: u16,
    /// The directory where RMS stores and reads firmware artifacts. Example: "/tmp/firmware".
    pub firmware_dir: String,
    /// The type of persistence backend used by RMS. Example: "postgres".
    pub persistence_type: String,
    /// The TLS mode used by RMS.
    pub tls_mode: TlsMode,
}

// Build metadata injected by vergen in build.rs.
const BUILD_TIMESTAMP: &str = env!("VERGEN_BUILD_TIMESTAMP");
const GIT_SHA: &str = env!("VERGEN_GIT_SHA");
const GIT_DESCRIBE: &str = env!("VERGEN_GIT_DESCRIBE");
// VERGEN_RUSTC_SEMVER is the bare semver string; prefix to match Prometheus convention.
const RUSTC_SEMVER: &str = env!("VERGEN_RUSTC_SEMVER");

const GRPC_HISTOGRAM_BUCKETS: &[f64] = &[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0];
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Prefix of metric families suppressed from `GET /metrics` output.
///
/// `tonic_prometheus_layer` registers two parallel sets of gRPC metrics:
///
/// * **Go-grpc style** (`rms_grpc_server_*`) — labelled by service, method, and
///   status code. These are the useful ones.
/// * **Backward-compat** (`rms_function_calls_*`) — labelled only by HTTP
///   method and path. These are redundant noise excluded here.
///
/// The prefix is compared against the fully-prefixed family name (i.e. after
/// `registry.gather()` has applied the `rms_` namespace).
const SUPPRESSED_METRIC_PREFIX: &str = "rms_function_calls_";

/// The namespace/prefix applied to every metric in this service's registry.
///
/// `prometheus::Registry::new_custom` prepends `{RMS_METRIC_PREFIX}_` to each
/// metric name at gather-time, so registering a metric named `api_version`
/// produces `rms_api_version` in the scraped output.
const RMS_METRIC_PREFIX: &str = "rms";

/// Initialize the process-wide metrics collector.
///
/// Must be called once before the gRPC server starts. Creates a dedicated
/// Prometheus registry with the `rms_` namespace so that every metric —
/// both the gRPC call counters/histograms from `tonic_prometheus_layer` and
/// the `rms_api_version` / `rms_api_config` info gauges — is automatically
/// prefixed with `rms_`.
///
/// Returns an `Arc` to the registry so the caller can pass it to
/// `prometheus_route` and expose it on `GET /metrics`.
pub fn init(config: &MetricsInfo) -> Result<Arc<Registry>> {
    let registry = Registry::new_custom(Some(RMS_METRIC_PREFIX.to_string()), None)
        .map_err(|e| RmsError::internal(format!("failed to create metrics registry: {e}")))?;

    // try_init_settings owns an internal OnceLock; AlreadyInitialized means
    // a previous call already configured the layer — treat as a no-op so that
    // calling init() more than once (e.g. across unit tests in the same binary)
    // does not produce a hard error.
    match try_init_settings(GlobalSettings {
        histogram_buckets: GRPC_HISTOGRAM_BUCKETS.to_vec(),
        registry: registry.clone(),
    }) {
        Ok(()) | Err(MetricsError::AlreadyInitialized) => {}
        Err(e) => {
            return Err(RmsError::internal(format!(
                "failed to initialize gRPC metrics: {e}"
            )));
        }
    }

    register_version_gauge(&registry)
        .map_err(|e| RmsError::internal(format!("failed to register version gauge: {e}")))?;

    register_config_gauge(&registry, config)
        .map_err(|e| RmsError::internal(format!("failed to register config gauge: {e}")))?;

    Ok(Arc::new(registry))
}

/// Register the `api_version` info gauge with the supplied registry.
///
/// The base name is `api_version`; the `rms_` namespace is added by the
/// custom registry at gather-time, producing `rms_api_version` in output.
fn register_version_gauge(registry: &Registry) -> std::result::Result<(), prometheus::Error> {
    let rust_version = format!("rustc {RUSTC_SEMVER}");

    // Trace build metadata on startup.
    tracing::info!(
        build_date = BUILD_TIMESTAMP,
        build_version = GIT_DESCRIBE,
        git_sha = GIT_SHA,
        rust_version,
        "build metadata"
    );

    let opts = prometheus::opts!(
        "api_version",
        "Version (git sha, build date, etc) of the RMS service"
    )
    .const_label("build_date", BUILD_TIMESTAMP)
    .const_label("build_version", GIT_DESCRIBE)
    .const_label("git_sha", GIT_SHA)
    .const_label("rust_version", &rust_version);

    let gauge = prometheus::Gauge::with_opts(opts)?;
    gauge.set(1.0);
    // AlreadyReg means an earlier init() call already registered the gauge to
    // this same registry — treat as a no-op.
    match registry.register(Box::new(gauge)) {
        Ok(()) | Err(prometheus::Error::AlreadyReg) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Register the `api_config` info gauge with the supplied registry.
///
/// The base name is `api_config`; the `rms_` namespace is added by the
/// custom registry at gather-time, producing `rms_api_config` in output.
fn register_config_gauge(
    registry: &Registry,
    config: &MetricsInfo,
) -> std::result::Result<(), prometheus::Error> {
    let opts = prometheus::opts!("api_config", "API configuration of the RMS service")
        .const_label("tls_mode", config.tls_mode.as_label().to_owned())
        .const_label("persistence_type", config.persistence_type.clone())
        .const_label("grpc_port", config.grpc_port.to_string())
        .const_label("firmware_dir", config.firmware_dir.clone());

    let gauge = prometheus::Gauge::with_opts(opts)?;
    gauge.set(1.0);
    match registry.register(Box::new(gauge)) {
        Ok(()) | Err(prometheus::Error::AlreadyReg) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Build the gRPC metrics layer for tonic services.
#[must_use]
pub(crate) fn grpc_layer() -> tonic_prometheus_layer::MetricsLayer {
    tonic_prometheus_layer::MetricsLayer::new()
}

/// Build the axum `MethodRouter` for `GET /metrics`.
///
/// `registry` must be the same `Arc<Registry>` returned by [`init`].
pub(crate) fn prometheus_route(registry: Arc<Registry>) -> MethodRouter {
    get(move || prometheus_response(registry))
}

async fn prometheus_response(registry: Arc<Registry>) -> Response {
    let families: Vec<_> = registry
        .gather()
        .into_iter()
        .filter(|mf| !mf.name().starts_with(SUPPRESSED_METRIC_PREFIX))
        .collect();

    let encoder = TextEncoder::new();
    match encoder.encode_to_string(&families) {
        Ok(body) => ([(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)], body).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to encode metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to encode metrics\n",
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use super::*;
    use crate::api::grpc::server::TlsMode;
    use axum::body::to_bytes;
    use axum::http::StatusCode;

    /// Returns the registry created by the first `init()` call in this test
    /// binary. Tests run in the same process and share tonic's internal global
    /// state, so we initialise once and hand out clones of the same registry.
    fn ensure_init() -> Arc<Registry> {
        static REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();
        REGISTRY
            .get_or_init(|| {
                let config = MetricsInfo {
                    grpc_port: 8801,
                    firmware_dir: "/tmp/firmware".to_string(),
                    persistence_type: "memory".to_string(),
                    tls_mode: TlsMode::Insecure,
                };
                init(&config).expect("metrics init failed")
            })
            .clone()
    }

    #[test]
    fn init_succeeds() {
        assert!(!ensure_init().gather().is_empty());
    }

    #[test]
    fn grpc_layer_can_be_constructed() {
        let _layer = grpc_layer();
    }

    #[test]
    fn prometheus_route_can_be_constructed() {
        let registry = ensure_init();
        let _route = prometheus_route(registry);
    }

    #[tokio::test]
    async fn prometheus_response_returns_200_with_prometheus_content_type() {
        let registry = ensure_init();
        let response = prometheus_response(registry).await;

        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("Content-Type header missing")
            .to_str()
            .unwrap();
        assert!(
            content_type.contains("text/plain"),
            "unexpected Content-Type: {content_type}"
        );
        assert!(
            content_type.contains("0.0.4"),
            "missing version=0.0.4 in Content-Type: {content_type}"
        );
    }

    #[tokio::test]
    async fn prometheus_response_body_contains_rms_version_gauge_lines() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        assert!(
            body.contains("# HELP rms_api_version"),
            "HELP line missing:\n{body}"
        );
        assert!(
            body.contains("# TYPE rms_api_version gauge"),
            "TYPE line missing:\n{body}"
        );
        assert!(
            body.contains("rms_api_version{"),
            "sample line missing:\n{body}"
        );
    }

    #[tokio::test]
    async fn prometheus_response_body_includes_all_build_metadata_labels() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        for label in &["build_date=", "build_version=", "git_sha=", "rust_version="] {
            assert!(body.contains(label), "label {label:?} missing:\n{body}");
        }
    }

    #[tokio::test]
    async fn prometheus_response_gauge_value_is_one() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        let sample = body
            .lines()
            .find(|l| l.starts_with("rms_api_version{"))
            .expect("no rms_api_version sample line found");

        let value_str = sample
            .rsplit_once("} ")
            .map(|(_, v)| v)
            .expect("sample line has no '} value' suffix");
        let value: f64 = value_str
            .trim()
            .parse()
            .expect("gauge value should be parseable as f64");
        assert_eq!(value, 1.0, "expected gauge value 1.0, got {value}");
    }

    #[tokio::test]
    async fn prometheus_response_rust_version_label_has_rustc_prefix() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        assert!(
            body.contains("rust_version=\"rustc "),
            "rust_version label should start with 'rustc ': body was\n{body}"
        );
    }

    #[tokio::test]
    async fn prometheus_response_body_contains_rms_config_gauge_lines() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        assert!(
            body.contains("# HELP rms_api_config"),
            "HELP line missing:\n{body}"
        );
        assert!(
            body.contains("# TYPE rms_api_config gauge"),
            "TYPE line missing:\n{body}"
        );
        assert!(
            body.contains("rms_api_config{"),
            "sample line missing:\n{body}"
        );
    }

    #[tokio::test]
    async fn prometheus_response_config_gauge_tls_mode_label_is_insecure() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        assert!(
            body.contains(r#"tls_mode="insecure""#),
            "tls_mode=\"insecure\" label missing:\n{body}"
        );
    }

    #[tokio::test]
    async fn prometheus_response_config_gauge_includes_all_config_labels() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        for label in &[
            "tls_mode=",
            "persistence_type=",
            "grpc_port=",
            "firmware_dir=",
        ] {
            assert!(body.contains(label), "label {label:?} missing:\n{body}");
        }
    }

    #[tokio::test]
    async fn prometheus_response_config_gauge_value_is_one() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        let sample = body
            .lines()
            .find(|l| l.starts_with("rms_api_config{"))
            .expect("no rms_api_config sample line found");

        let value_str = sample
            .rsplit_once("} ")
            .map(|(_, v)| v)
            .expect("sample line has no '} value' suffix");
        let value: f64 = value_str
            .trim()
            .parse()
            .expect("gauge value should be parseable as f64");
        assert_eq!(value, 1.0, "expected gauge value 1.0, got {value}");
    }

    #[tokio::test]
    async fn prometheus_response_suppresses_legacy_function_calls_families() {
        let body = body_text(prometheus_response(ensure_init()).await).await;

        assert!(
            !body.contains(SUPPRESSED_METRIC_PREFIX),
            "suppressed metric prefix {SUPPRESSED_METRIC_PREFIX:?} appeared in output:\n{body}"
        );
    }

    #[tokio::test]
    async fn prometheus_response_body_contains_workflow_metric_families() {
        use crate::orchestrator::job_tracker::{JobTracker, JobType};

        let registry = Arc::new(
            Registry::new_custom(Some(RMS_METRIC_PREFIX.to_string()), None)
                .expect("create test registry"),
        );
        let tracker = Arc::new(
            JobTracker::builder()
                .metrics(registry.as_ref())
                .build()
                .expect("register workflow metrics"),
        );

        let pending = tracker
            .create_visible_workflow_job_if_node_idle("rack-01", "node-01", JobType::FirmwareUpdate)
            .expect("create visible workflow");
        pending.complete("Completed", "null");

        let body = body_text(prometheus_response(registry).await).await;

        for family in &[
            "# HELP rms_workflows_counts_total",
            "# TYPE rms_workflows_counts_total counter",
            "# HELP rms_workflows_in_flight",
            "# TYPE rms_workflows_in_flight gauge",
        ] {
            assert!(body.contains(family), "missing {family}:\n{body}");
        }
    }

    async fn body_text(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("failed to read response body");
        String::from_utf8(bytes.to_vec()).expect("response body is not valid UTF-8")
    }
}
