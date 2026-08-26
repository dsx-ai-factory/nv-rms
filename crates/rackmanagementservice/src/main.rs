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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

use rackmanagementservice::api::grpc::server::{
    ApiListenerMode, GrpcServer, SwitchTlsRoots, TlsConfig, TlsMode, assert_insecure_allowed,
};
use rackmanagementservice::config::{DEFAULT_CONFIG_PATH, ExpectedInventoryCatalog, RmsConfig};
use rackmanagementservice::libnmxc::{
    TlsMaterialStore, normalize_tls_server_name, validate_domain,
};
use rackmanagementservice::logging::setup::setup_logging;
use rackmanagementservice::metrics::{self, MetricsInfo, MetricsServer};
use rackmanagementservice::orchestrator::job_tracker::JobTracker;
use rackmanagementservice::orchestrator::rack_manager::RackManager;
use rackmanagementservice::persistence::Backends;
use rackmanagementservice::persistence::postgres::{
    PostgresFirmwareObjectStore, connect as pg_connect, run_bootstrap,
};
use rackmanagementservice::transport::ssh::SftpUploadOptions;

// RMS fans out many async gRPC, Redfish, SSH, and firmware tasks that allocate
// request buffers, JSON/protobuf payloads, task state, and transport objects
// across tokio worker threads. Mimalloc is a good default for this shape because
// it has low cross-thread contention, aggressive per-thread caching, and steady
// fragmentation behavior. It also gives Linux glibc and possible static musl
// builds the same allocator policy.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Rack Management Service -- manages data center racks at scale.
///
/// All runtime configuration lives in a TOML file (see [`RmsConfig`]); the only
/// command-line argument is the path to that file.
#[derive(Parser)]
#[command(name = "rackmanagementservice", version)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,
}

fn validate_metrics_config(config: &RmsConfig, tls: &Option<TlsConfig>) -> Result<(), String> {
    if config.metrics.port != 0 && config.metrics.port == config.port {
        return Err(format!(
            "metrics.port ({}) must differ from port ({})",
            config.metrics.port, config.port
        ));
    }

    if config.metrics.tls && tls.is_none() {
        return Err("metrics.tls requires gRPC TLS material (tls.cert and tls.key)".into());
    }

    Ok(())
}

/// Reject a blank or whitespace-only `nmx_gateway_id` so NMX-C requests
/// (`hello`, `set_static_config`, ...) are never issued with an empty gateway
/// identity. Mirrors the trim/empty guarding applied to the switch domain
/// fields.
fn validate_nmx_gateway_id(config: &RmsConfig) -> Result<(), String> {
    if config.switches.nmx_gateway_id.trim().is_empty() {
        return Err("switches.nmx_gateway_id must not be blank or whitespace-only".into());
    }

    Ok(())
}

fn validate_tls_roots(config: &RmsConfig) -> Result<(), String> {
    if config.switches.insecure_switch {
        return Ok(());
    }

    for (label, dir) in [
        (
            "switches.switch_cert_root",
            config.switches.switch_cert_root.as_ref(),
        ),
        (
            "switches.client_tls_root",
            config.switches.client_tls_root.as_ref(),
        ),
    ] {
        let Some(dir) = dir else {
            continue;
        };
        if !dir.is_dir() {
            return Err(format!(
                "{label} directory does not exist: {}",
                dir.display()
            ));
        }
    }

    if config.switches.client_tls_root.is_some()
        && config
            .switches
            .default_switch_domain
            .as_deref()
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .is_none()
    {
        return Err(
            "switches.client_tls_root requires switches.default_switch_domain \
             (legacy switch RPCs omit domain and rely on the default)"
                .into(),
        );
    }

    Ok(())
}

fn validate_default_switch_domain(config: &RmsConfig) -> Result<(), String> {
    let Some(domain) = config
        .switches
        .default_switch_domain
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
    else {
        return Ok(());
    };
    validate_domain(domain).map_err(|e| format!("switches.default_switch_domain: {e}"))
}

fn validate_dns_domain(config: &RmsConfig) -> Result<(), String> {
    let Some(domain) = config
        .switches
        .dns_domain
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
    else {
        return Ok(());
    };

    normalize_tls_server_name(domain)
        .map(|_| ())
        .map_err(|e| format!("switches.dns_domain: {e}"))
}

fn validate_switch_tls_config(config: &RmsConfig) -> Result<(), String> {
    validate_tls_roots(config)?;

    if config.switches.insecure_switch {
        // insecure_switch is a full switch mTLS opt-out. Startup should not
        // require local cert roots, an NVLink default domain, or DNS suffixes
        // when all switch cert material is ignored.
        return Ok(());
    }

    validate_default_switch_domain(config)?;
    validate_dns_domain(config)
}

fn build_switch_tls_roots(config: &RmsConfig) -> SwitchTlsRoots {
    let default_domain = config
        .switches
        .default_switch_domain
        .as_ref()
        .map(|d| d.trim().to_owned())
        .filter(|d| !d.is_empty());
    let dns_domain = config
        .switches
        .dns_domain
        .as_ref()
        .map(|d| d.trim().to_owned())
        .filter(|d| !d.is_empty());

    SwitchTlsRoots {
        switch_cert: config
            .switches
            .switch_cert_root
            .as_ref()
            .map(|dir| TlsMaterialStore::new(dir.clone())),
        client_tls: config
            .switches
            .client_tls_root
            .as_ref()
            .map(|dir| TlsMaterialStore::new(dir.clone())),
        default_domain,
        dns_domain,
        insecure_switch: config.switches.insecure_switch,
    }
}

/// Builds the persistence backends from the configuration.
///
/// With `postgres.db_url` set (from the config file or the `DATABASE_URL`
/// override): connects to Postgres, runs embedded migrations, and returns a
/// `Backends` whose stores hit the database. Failures here are fatal -- if
/// the operator asked for a database, they get the database, not a silent
/// fallback.
///
/// Without a URL: returns an in-memory `Backends` and logs a warning so
/// it's obvious in the logs that nothing is persisted across restarts.
async fn build_backends(config: &RmsConfig) -> Result<Backends, String> {
    let Some(url) = config.postgres.db_url.as_deref() else {
        tracing::warn!(
            "no postgres.db_url / DATABASE_URL set -- using in-memory persistence \
             (data will not survive restarts)"
        );
        return Ok(Backends::memory());
    };

    let max_connections = u32::try_from(config.postgres.db_pool_max.get())
        .map_err(|_| "postgres.db_pool_max exceeds u32::MAX".to_owned())?;
    let pool = pg_connect(url, max_connections)
        .await
        .map_err(|e| format!("failed to connect to database: {}", e.message))?;
    run_bootstrap(&pool)
        .await
        .map_err(|e| format!("failed to run database migrations: {}", e.message))?;
    tracing::info!(max_connections, "connected to Postgres and ran migrations");

    Ok(Backends {
        firmware_objects: Arc::new(PostgresFirmwareObjectStore::new(pool)),
    })
}

/// Running RMS process with independent gRPC and metrics listeners.
struct RunningService {
    grpc_server: GrpcServer,
    metrics_server: MetricsServer,
}

impl RunningService {
    /// Drain in-flight jobs, then stop and await the metrics server task.
    async fn stop(&mut self) {
        self.grpc_server.stop_and_await_terminal().await;
        self.metrics_server.stop();
        self.metrics_server.join().await;
    }
}

/// Creates the server components and starts the gRPC and metrics servers.
async fn start_service(
    config: &RmsConfig,
    tls: Option<TlsConfig>,
    listener_mode: ApiListenerMode,
    switch_tls_roots: SwitchTlsRoots,
) -> Result<RunningService, String> {
    validate_metrics_config(config, &tls)?;
    validate_nmx_gateway_id(config)?;

    let sftp_upload_options = SftpUploadOptions::new(
        std::time::Duration::from_secs(config.workflows.sftp_upload_timeout_seconds),
        std::time::Duration::from_secs(config.workflows.sftp_step_timeout_seconds),
    )
    .map_err(|e| e.message)?;

    let rack_manager = Arc::new(RackManager::new());
    let backends = build_backends(config).await?;

    // Create metrics registry with gathered RMS config.
    let metrics_info = MetricsInfo {
        grpc_port: config.port,
        firmware_dir: config.workflows.firmware_dir.to_string_lossy().into_owned(),
        persistence_type: backends.firmware_objects.name().to_owned(),
        tls_mode: TlsMode::from_config(&tls),
    };
    let metrics_registry = metrics::init(&metrics_info)
        .map_err(|e| format!("failed to initialize metrics: {}", e.message))?;

    // The tracker owns its workflow-metric registration, keeping the base
    // metrics module free of any workflow-specific knowledge.
    let job_tracker = Arc::new(
        JobTracker::builder()
            .terminal_job_ttl(Duration::from_secs(
                config.workflows.terminal_job_ttl_seconds.get(),
            ))
            .max_tracked_jobs(config.workflows.max_tracked_jobs.get())
            .metrics(metrics_registry.as_ref())
            .build()
            .map_err(|e| format!("failed to register workflow metrics: {}", e.message))?,
    );
    job_tracker.spawn_reaper();

    let metrics_tls = if config.metrics.tls {
        Some(
            tls.as_ref()
                .ok_or_else(|| {
                    "metrics.tls requires gRPC TLS material (tls.cert and tls.key)".to_string()
                })?
                .metrics_rustls_config()
                .map_err(|e| format!("failed to build metrics TLS config: {e}"))?,
        )
    } else {
        None
    };

    tokio::fs::create_dir_all(&config.workflows.firmware_dir)
        .await
        .map_err(|e| {
            format!(
                "failed to create firmware directory {}: {e}",
                config.workflows.firmware_dir.display()
            )
        })?;

    let mut grpc_server = GrpcServer::new(config.port, rack_manager, job_tracker, backends)
        .with_firmware_dir(config.workflows.firmware_dir.clone())
        .with_sftp_upload_options(sftp_upload_options)
        .with_nmx_gateway_id(config.switches.nmx_gateway_id.clone())
        .with_expected_inventory_catalog(ExpectedInventoryCatalog::from(
            &config.workflows.expected_inventory_profiles,
        ));

    if let Some(tls) = tls {
        grpc_server = grpc_server.with_tls(tls);
    } else {
        grpc_server = grpc_server.with_listener_mode(listener_mode);
    }

    if switch_tls_roots.insecure_switch {
        tracing::warn!(
            "insecure_switch disables switch client mTLS; NVUE uses unverified HTTPS and NMX-C uses plaintext HTTP"
        );
    }

    if switch_tls_roots.switch_cert.is_some()
        || switch_tls_roots.client_tls.is_some()
        || switch_tls_roots.default_domain.is_some()
        || switch_tls_roots.dns_domain.is_some()
        || switch_tls_roots.insecure_switch
    {
        grpc_server = grpc_server.with_switch_tls_roots(switch_tls_roots);
    }

    grpc_server
        .start()
        .await
        .map_err(|e| format!("failed to start gRPC server: {}", e.message))?;

    let mut metrics_server = MetricsServer::new(config.metrics.port);
    metrics_server
        .start(metrics_registry, metrics_tls)
        .await
        .map_err(|e| format!("failed to start metrics server: {}", e.message))?;

    Ok(RunningService {
        grpc_server,
        metrics_server,
    })
}

/// Wait until the process receives a shutdown signal.
///
/// On Unix, listens for both `SIGINT` (Ctrl+C) and `SIGTERM` (Kubernetes pod
/// termination). On other platforms, only `SIGINT` is handled.
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.expect("failed to listen for SIGINT");
                tracing::info!("received SIGINT");
            }
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for SIGINT");
        tracing::info!("received SIGINT");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Pin rustls' crypto provider; `aws-lc-rs` and `ring` are both linked in,
    // so auto-selection fails and rustls panics on first TLS use.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let cli = Cli::parse();

    // Parse configuration before logging is initialized so a bad config surfaces
    // immediately; report the failure to stderr since the tracing subscriber is
    // not yet installed.
    let config = match RmsConfig::load(&cli.config) {
        Ok(config) => config,
        Err(msg) => {
            eprintln!("configuration error: {msg}");
            std::process::exit(1);
        }
    };

    // Promote only job_id from active spans so nested SFTP progress events can
    // be correlated with jobs without adding node IDs or paths to upload spans.
    setup_logging(
        vec!["job_id".to_string()],
        config.logging.log_level.as_deref(),
        config.logging.enable_timestamps,
    )?;

    // Plaintext gRPC requires `tls.insecure = true` in the config *and* the
    // RMS_ALLOW_INSECURE=1 env gate (sourced from a different channel than the
    // config file) as an independent production safeguard.
    let insecure_permit = match assert_insecure_allowed(config.tls.insecure) {
        Ok(permit) => permit,
        Err(msg) => {
            tracing::error!("{msg}");
            std::process::exit(1);
        }
    };

    // Resolve TLS up front: validates the tls.* / tls.insecure combination and
    // reads the cert/key/CA once, before any side-effecting startup step.
    let tls = match TlsConfig::resolve(
        config.tls.cert.as_deref(),
        config.tls.key.as_deref(),
        config.tls.ca.as_deref(),
        insecure_permit,
    ) {
        Ok(tls) => tls,
        Err(msg) => {
            tracing::error!("{msg}");
            std::process::exit(1);
        }
    };

    if let Err(msg) = validate_switch_tls_config(&config) {
        tracing::error!("{msg}");
        std::process::exit(1);
    }

    let switch_tls_roots = build_switch_tls_roots(&config);
    let listener_mode = ApiListenerMode::from_tls_config(&tls);

    let mut service = match start_service(&config, tls, listener_mode, switch_tls_roots).await {
        Ok(s) => s,
        Err(msg) => {
            tracing::error!("{msg}");
            std::process::exit(1);
        }
    };

    tracing::info!(
        grpc_port = config.port,
        metrics_port = config.metrics.port,
        metrics_tls = config.metrics.tls,
        "Rack Management Service running (send SIGINT/Ctrl+C or SIGTERM to stop)"
    );

    wait_for_shutdown().await;

    tracing::info!("shutting down...");
    service.stop().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    use rackmanagementservice::api::grpc::server::RMS_ALLOW_INSECURE_ENV;
    use rackmanagementservice::config::{RmsMetricsConfig, RmsSwitchConfig, RmsTlsConfig};

    /// Test config: historical defaults with a non-default gRPC port (so it
    /// differs from the default metrics port) and a caller-chosen insecure flag.
    fn config(insecure: bool) -> RmsConfig {
        RmsConfig {
            port: 50051,
            tls: RmsTlsConfig {
                insecure,
                ..RmsTlsConfig::default()
            },
            ..RmsConfig::default()
        }
    }

    #[test]
    fn config_defaults_match_historical_flag_defaults() {
        let c = RmsConfig::default();
        assert_eq!(c.port, 8801);
        assert_eq!(c.metrics.port, 8802);
        assert!(!c.metrics.tls);
        assert_eq!(c.postgres.db_pool_max, NonZeroUsize::new(20).unwrap());

        let sftp_upload_options = SftpUploadOptions::default();
        assert_eq!(
            c.workflows.sftp_upload_timeout_seconds,
            sftp_upload_options.overall_timeout.as_secs()
        );
        assert_eq!(
            c.workflows.sftp_step_timeout_seconds,
            sftp_upload_options.step_timeout.as_secs()
        );
    }

    #[test]
    fn metrics_port_must_differ_from_grpc_port() {
        let c = RmsConfig {
            port: 8801,
            metrics: RmsMetricsConfig {
                port: 8801,
                ..RmsMetricsConfig::default()
            },
            ..config(true)
        };
        assert!(validate_metrics_config(&c, &None).is_err());
    }

    #[test]
    fn metrics_tls_requires_grpc_tls_material() {
        let c = RmsConfig {
            metrics: RmsMetricsConfig {
                tls: true,
                ..RmsMetricsConfig::default()
            },
            ..config(true)
        };
        assert!(validate_metrics_config(&c, &None).is_err());
    }

    #[test]
    fn default_nmx_gateway_id_is_accepted() {
        assert!(validate_nmx_gateway_id(&config(true)).is_ok());
    }

    #[test]
    fn blank_nmx_gateway_id_is_rejected() {
        for blank in ["", "   ", "\t\n"] {
            let c = RmsConfig {
                switches: RmsSwitchConfig {
                    nmx_gateway_id: blank.to_owned(),
                    ..RmsSwitchConfig::default()
                },
                ..config(true)
            };
            assert!(
                validate_nmx_gateway_id(&c).is_err(),
                "blank gateway id {blank:?} should be rejected"
            );
        }
    }

    #[cfg(unix)]
    mod shutdown_signal_tests {
        use super::*;
        use std::time::Duration;

        async fn send_signal_after(signal: &str, delay: Duration) {
            let pid = std::process::id();
            let signal = signal.to_owned();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                std::process::Command::new("kill")
                    .args(["-s", &signal, &pid.to_string()])
                    .status()
                    .expect("send shutdown signal");
            });
        }

        #[tokio::test]
        async fn wait_for_shutdown_responds_to_sigterm() {
            send_signal_after("TERM", Duration::from_millis(50)).await;
            tokio::time::timeout(Duration::from_secs(2), wait_for_shutdown())
                .await
                .expect("wait_for_shutdown should complete on SIGTERM");
        }

        #[tokio::test]
        async fn wait_for_shutdown_responds_to_sigint() {
            send_signal_after("INT", Duration::from_millis(50)).await;
            tokio::time::timeout(Duration::from_secs(2), wait_for_shutdown())
                .await
                .expect("wait_for_shutdown should complete on SIGINT");
        }
    }

    /// Writes two readable placeholder files into a fresh temp dir and returns
    /// it alongside their paths. The content is intentionally NOT valid PEM:
    /// `TlsConfig::resolve` only reads the files (tonic parses the bytes lazily
    /// at bind time), so this exercises resolve's read-and-accept path without
    /// generating real certificates. Do NOT pass these to a real
    /// `GrpcServer::start()` -- the bind would reject them. Use rcgen (see
    /// tests/grpc_mtls.rs) when a server that actually binds TLS is needed.
    fn temp_readable_files() -> (tempfile::TempDir, String, String) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, b"placeholder, not valid PEM").expect("write cert");
        std::fs::write(&key, b"placeholder, not valid PEM").expect("write key");
        let (c, k) = (
            cert.to_str().unwrap().to_string(),
            key.to_str().unwrap().to_string(),
        );
        (dir, c, k)
    }

    #[test]
    fn tls_without_ca_rejected() {
        // cert + key without tls.ca is TLS-only: server is authenticated but
        // callers are not. Rejected at startup to enforce mTLS.
        let (_dir, cert, key) = temp_readable_files();
        let Err(e) = TlsConfig::resolve(Some(&cert), Some(&key), None, None) else {
            panic!("cert + key without tls.ca should be rejected");
        };
        assert!(
            e.contains("tls.ca"),
            "error must name the missing config key: {e}"
        );
    }

    #[test]
    fn cert_without_key_rejected() {
        let Err(e) = TlsConfig::resolve(Some("cert.pem"), None, None, None) else {
            panic!("cert without key should be rejected");
        };
        assert!(e.contains("together"));
    }

    #[test]
    fn key_without_cert_rejected() {
        let Err(e) = TlsConfig::resolve(None, Some("key.pem"), None, None) else {
            panic!("key without cert should be rejected");
        };
        assert!(e.contains("together"));
    }

    #[test]
    fn no_tls_no_insecure_rejected() {
        let Err(e) = TlsConfig::resolve(None, None, None, None) else {
            panic!("no TLS without insecure = true should be rejected");
        };
        assert!(
            e.contains("insecure = true"),
            "error must mention insecure = true: {e}"
        );
    }

    #[test]
    fn ca_without_cert_rejected() {
        let Err(e) = TlsConfig::resolve(None, None, Some("ca.pem"), None) else {
            panic!("tls.ca without cert should be rejected");
        };
        assert!(e.contains("tls.ca requires"));
    }

    #[test]
    fn tls_without_ca_rejected_even_with_insecure() {
        // insecure = true only enables plaintext (no certs at all). Providing certs
        // without tls.ca is still rejected because TLS-only is not a valid mode.
        let (_dir, cert, key) = temp_readable_files();
        // `with_var` restores the prior value on drop, even if `body` panics, so a
        // failed assertion here can't leak `RMS_ALLOW_INSECURE_ENV=1` into other tests.
        let result = rackmanagementservice::with_test_env!(RMS_ALLOW_INSECURE_ENV => "1", {
            let permit = assert_insecure_allowed(true)
                .expect("env gate")
                .expect("insecure permit");
            TlsConfig::resolve(Some(&cert), Some(&key), None, Some(permit))
        });
        let Err(e) = result else {
            panic!("cert + key without tls.ca should be rejected even with insecure = true");
        };
        assert!(
            e.contains("tls.ca"),
            "error must name the missing config key: {e}"
        );
    }

    #[test]
    fn resolve_rejects_unreadable_cert() {
        // A typoed/missing cert path fails at resolve -- before any
        // side-effecting startup step -- rather than later at bind.
        let Err(e) = TlsConfig::resolve(
            Some("/nonexistent/rms-cert.pem"),
            Some("/nonexistent/rms-key.pem"),
            Some("/nonexistent/rms-ca.pem"),
            None,
        ) else {
            panic!("unreadable cert path should be rejected");
        };
        assert!(e.contains("failed to read"), "got: {e}");
    }

    #[test]
    fn default_switch_domain_is_validated() {
        let c = RmsConfig {
            switches: RmsSwitchConfig {
                default_switch_domain: Some("../bad".into()),
                ..RmsSwitchConfig::default()
            },
            ..config(true)
        };
        assert!(validate_default_switch_domain(&c).is_err());
    }

    #[test]
    fn dns_domain_is_validated() {
        for invalid in [
            "../bad",
            "bad host",
            "switch.example.com:443",
            "*.example.com",
            "10.0.0.1",
            "fd00::1",
        ] {
            let c = RmsConfig {
                switches: RmsSwitchConfig {
                    dns_domain: Some(invalid.into()),
                    ..RmsSwitchConfig::default()
                },
                ..config(true)
            };

            assert!(
                validate_dns_domain(&c).is_err(),
                "{invalid} must not be accepted as a TLS server name"
            );
        }
    }

    #[test]
    fn valid_dns_domain_is_propagated_to_tls_roots() {
        let c = RmsConfig {
            switches: RmsSwitchConfig {
                dns_domain: Some("switch.example.com".into()),
                ..RmsSwitchConfig::default()
            },
            ..config(true)
        };

        assert!(validate_dns_domain(&c).is_ok());

        let roots = build_switch_tls_roots(&c);

        assert_eq!(roots.dns_domain.as_deref(), Some("switch.example.com"));
    }

    #[test]
    fn insecure_switch_flag_is_propagated_to_tls_roots() {
        let c = RmsConfig {
            switches: RmsSwitchConfig {
                insecure_switch: true,
                ..RmsSwitchConfig::default()
            },
            ..config(true)
        };

        assert!(c.switches.insecure_switch);

        let roots = build_switch_tls_roots(&c);

        assert!(roots.insecure_switch);
    }

    #[test]
    fn tls_root_dirs_must_exist_when_set() {
        let c = RmsConfig {
            switches: RmsSwitchConfig {
                client_tls_root: Some(PathBuf::from("/nonexistent/client-tls")),
                ..RmsSwitchConfig::default()
            },
            ..config(true)
        };
        let result = validate_tls_roots(&c);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn client_tls_root_requires_default_switch_domain() {
        let dir = tempfile::tempdir().unwrap();
        let client_tls = dir.path().join("client-tls");
        std::fs::create_dir_all(&client_tls).unwrap();
        let c = RmsConfig {
            switches: RmsSwitchConfig {
                client_tls_root: Some(client_tls),
                ..RmsSwitchConfig::default()
            },
            ..config(true)
        };
        let result = validate_tls_roots(&c);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("switches.client_tls_root requires switches.default_switch_domain")
        );
    }

    #[test]
    fn insecure_switch_skips_switch_tls_startup_requirements() {
        let c = RmsConfig {
            switches: RmsSwitchConfig {
                switch_cert_root: Some(PathBuf::from("/nonexistent/switch-cert")),
                client_tls_root: Some(PathBuf::from("/nonexistent/client-tls")),
                default_switch_domain: Some("../bad".into()),
                dns_domain: Some("bad host".into()),
                insecure_switch: true,
                ..RmsSwitchConfig::default()
            },
            ..config(true)
        };

        assert!(validate_switch_tls_config(&c).is_ok());
    }

    #[test]
    fn tls_roots_built_from_existing_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let c = RmsConfig {
            switches: RmsSwitchConfig {
                switch_cert_root: Some(dir.path().join("switch-certs")),
                client_tls_root: Some(dir.path().join("client-tls")),
                default_switch_domain: Some("site-wide".into()),
                ..RmsSwitchConfig::default()
            },
            ..config(true)
        };
        std::fs::create_dir_all(c.switches.switch_cert_root.as_ref().unwrap()).unwrap();
        std::fs::create_dir_all(c.switches.client_tls_root.as_ref().unwrap()).unwrap();
        assert!(validate_tls_roots(&c).is_ok());
        let roots = build_switch_tls_roots(&c);
        assert!(roots.switch_cert.is_some());
        assert!(roots.client_tls.is_some());
    }

    #[tokio::test]
    async fn start_service_binds_to_port() {
        // Bind two ephemeral listeners simultaneously so the OS assigns two
        // distinct ports; calling pick_unused_port twice can return the same
        // port and trip the metrics/grpc "must differ" check.
        let grpc_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind grpc port");
        let metrics_listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind metrics port");
        let grpc_port = grpc_listener.local_addr().expect("grpc addr").port();
        let metrics_port = metrics_listener.local_addr().expect("metrics addr").port();
        // Release the ports so start_service can bind them.
        drop(grpc_listener);
        drop(metrics_listener);
        let c = RmsConfig {
            port: grpc_port,
            metrics: RmsMetricsConfig {
                port: metrics_port,
                ..RmsMetricsConfig::default()
            },
            ..config(true)
        };
        let mut service = start_service(
            &c,
            None,
            ApiListenerMode::Insecure,
            SwitchTlsRoots::default(),
        )
        .await
        .unwrap();
        service.stop().await;
    }

    #[tokio::test]
    async fn start_service_invalid_port_zero_succeeds() {
        let metrics_port = portpicker::pick_unused_port().expect("no free metrics port");
        let c = RmsConfig {
            port: 0,
            metrics: RmsMetricsConfig {
                port: metrics_port,
                ..RmsMetricsConfig::default()
            },
            ..config(true)
        };
        if let Ok(mut service) = start_service(
            &c,
            None,
            ApiListenerMode::Insecure,
            SwitchTlsRoots::default(),
        )
        .await
        {
            service.stop().await;
        }
    }
}
