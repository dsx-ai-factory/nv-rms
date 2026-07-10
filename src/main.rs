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

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

use rackmanagementservice::api::grpc::server::{
    ApiListenerMode, GrpcServer, SwitchTlsRoots, TlsConfig, TlsMode, assert_insecure_allowed,
};
use rackmanagementservice::libnmxc::{
    TlsMaterialStore, normalize_tls_server_name, validate_domain,
};
use rackmanagementservice::logging::setup::setup_logging;
use rackmanagementservice::metrics::{self, RmsConfig};
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
#[derive(Parser)]
#[command(name = "rackmanagementservice", version)]
struct Cli {
    /// gRPC server port
    #[arg(long, default_value = "8801")]
    port: u16,

    /// Path to X.509 certificate file for TLS
    #[arg(long)]
    tls_cert: Option<String>,

    /// Path to private key file for TLS
    #[arg(long)]
    tls_key: Option<String>,

    /// Path to CA certificate for client verification (mTLS)
    #[arg(long)]
    tls_ca: Option<String>,

    /// Disable TLS (plaintext, UNAUTHENTICATED). Development/testing only.
    #[arg(long)]
    insecure: bool,

    /// Root directory of switch-side certificate material to install. Each NVLink
    /// domain contains a client-trust `ca.pem` plus the switch server identity in
    /// legacy-named `client.pem` and `client.key` files.
    #[arg(long, env = "SWITCH_CERT_ROOT")]
    switch_cert_root: Option<PathBuf>,

    /// Root directory of RMS client mTLS material for outbound switch
    /// NVUE/NMX-C connections. Required unless `--insecure-switch` is set.
    /// Each NVLink domain contains server-trust `ca.pem` plus RMS `client.pem`
    /// and `client.key`.
    #[arg(long, env = "CLIENT_TLS_ROOT")]
    client_tls_root: Option<PathBuf>,

    /// Default NVLink domain for switch TLS material lookup when an RPC omits `domain`.
    #[arg(long, env = "SWITCH_DEFAULT_DOMAIN")]
    default_switch_domain: Option<String>,

    /// Optional DNS domain override used as TLS server name (SNI) authority.
    #[arg(long, env = "SWITCH_DNS_DOMAIN")]
    dns_domain: Option<String>,

    /// Disable outbound switch client mTLS. NVUE still uses HTTPS with
    /// server certificate verification disabled; NMX-C uses plaintext HTTP.
    #[arg(long, env = "RMS_INSECURE_SWITCH")]
    insecure_switch: bool,

    /// Postgres connection URL for the persistence layer. If unset the
    /// service falls back to an in-memory store (data is lost on
    /// restart -- intended for development only). Also reads the
    /// `DATABASE_URL` environment variable.
    #[arg(long, env = "DATABASE_URL")]
    db_url: Option<String>,

    /// Maximum size of the Postgres connection pool. Ignored when no
    /// `--db-url` is provided.
    #[arg(long, default_value = "20")]
    db_pool_max: u32,

    /// Directory where RMS stores and reads firmware artifacts. Relative paths
    /// are resolved from the RMS process working directory.
    #[arg(long, env = "RMS_FIRMWARE_DIR", default_value = "firmware")]
    firmware_dir: PathBuf,

    /// Overall timeout for switch NVOS SFTP image uploads.
    #[arg(
        long,
        env = "RMS_SFTP_UPLOAD_TIMEOUT_SECONDS",
        default_value_t = SftpUploadOptions::default().overall_timeout.as_secs()
    )]
    sftp_upload_timeout_seconds: u64,

    /// Stall timeout for one switch NVOS SFTP upload step.
    #[arg(
        long,
        env = "RMS_SFTP_STEP_TIMEOUT_SECONDS",
        default_value_t = SftpUploadOptions::default().step_timeout.as_secs()
    )]
    sftp_step_timeout_seconds: u64,

    /// Maximum number of async job records the tracker retains. New jobs are
    /// refused once this many are resident, bounding worst-case memory.
    #[arg(long, env = "RMS_MAX_TRACKED_JOBS", default_value = "10000")]
    max_tracked_jobs: NonZeroUsize,
}

fn validate_tls_roots(cli: &Cli) -> Result<(), String> {
    if cli.insecure_switch {
        return Ok(());
    }

    for (label, dir) in [
        ("--switch-cert-root", cli.switch_cert_root.as_ref()),
        ("--client-tls-root", cli.client_tls_root.as_ref()),
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

    if cli.client_tls_root.is_some()
        && cli
            .default_switch_domain
            .as_deref()
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .is_none()
    {
        return Err("--client-tls-root requires --default-switch-domain \
             (legacy switch RPCs omit domain and rely on the default)"
            .into());
    }

    Ok(())
}

fn validate_default_switch_domain(cli: &Cli) -> Result<(), String> {
    let Some(domain) = cli
        .default_switch_domain
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
    else {
        return Ok(());
    };
    validate_domain(domain).map_err(|e| format!("--default-switch-domain: {e}"))
}

fn validate_dns_domain(cli: &Cli) -> Result<(), String> {
    let Some(domain) = cli
        .dns_domain
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
    else {
        return Ok(());
    };

    normalize_tls_server_name(domain)
        .map(|_| ())
        .map_err(|e| format!("--dns-domain: {e}"))
}

fn validate_switch_tls_config(cli: &Cli) -> Result<(), String> {
    validate_tls_roots(cli)?;

    if cli.insecure_switch {
        // --insecure-switch is a full switch mTLS opt-out. Startup should not
        // require local cert roots, an NVLink default domain, or DNS suffixes
        // when all switch cert material is ignored.
        return Ok(());
    }

    validate_default_switch_domain(cli)?;
    validate_dns_domain(cli)
}

fn build_switch_tls_roots(cli: &Cli) -> SwitchTlsRoots {
    let default_domain = cli
        .default_switch_domain
        .as_ref()
        .map(|d| d.trim().to_owned())
        .filter(|d| !d.is_empty());
    let dns_domain = cli
        .dns_domain
        .as_ref()
        .map(|d| d.trim().to_owned())
        .filter(|d| !d.is_empty());

    SwitchTlsRoots {
        switch_cert: cli
            .switch_cert_root
            .as_ref()
            .map(|dir| TlsMaterialStore::new(dir.clone())),
        client_tls: cli
            .client_tls_root
            .as_ref()
            .map(|dir| TlsMaterialStore::new(dir.clone())),
        default_domain,
        dns_domain,
        insecure_switch: cli.insecure_switch,
    }
}

/// Builds the persistence backends from the CLI configuration.
///
/// With `--db-url` (or `DATABASE_URL`) set: connects to Postgres, runs
/// embedded migrations, and returns a `Backends` whose stores hit the
/// database. Failures here are fatal -- if the operator asked for a
/// database, they get the database, not a silent fallback.
///
/// Without a URL: returns an in-memory `Backends` and logs a warning so
/// it's obvious in the logs that nothing is persisted across restarts.
async fn build_backends(cli: &Cli) -> Result<Backends, String> {
    let Some(url) = cli.db_url.as_deref() else {
        tracing::warn!(
            "no --db-url / DATABASE_URL set -- using in-memory persistence \
             (data will not survive restarts)"
        );
        return Ok(Backends::memory());
    };

    let pool = pg_connect(url, cli.db_pool_max)
        .await
        .map_err(|e| format!("failed to connect to database: {}", e.message))?;
    run_bootstrap(&pool)
        .await
        .map_err(|e| format!("failed to run database migrations: {}", e.message))?;
    tracing::info!(
        max_connections = cli.db_pool_max,
        "connected to Postgres and ran migrations"
    );

    Ok(Backends {
        firmware_objects: Arc::new(PostgresFirmwareObjectStore::new(pool)),
    })
}

/// Creates the server components and starts the gRPC server.
async fn start_service(
    cli: &Cli,
    tls: Option<TlsConfig>,
    listener_mode: ApiListenerMode,
    switch_tls_roots: SwitchTlsRoots,
) -> Result<GrpcServer, String> {
    let sftp_upload_options = SftpUploadOptions::new(
        std::time::Duration::from_secs(cli.sftp_upload_timeout_seconds),
        std::time::Duration::from_secs(cli.sftp_step_timeout_seconds),
    )
    .map_err(|e| e.message)?;

    let rack_manager = Arc::new(RackManager::new());
    let job_tracker = Arc::new(JobTracker::with_max_tracked_jobs(
        cli.max_tracked_jobs.get(),
    ));
    job_tracker.spawn_reaper();
    let backends = build_backends(cli).await?;

    // Create metrics registry with gathered RMS config.
    let rms_config = RmsConfig {
        grpc_port: cli.port,
        firmware_dir: cli.firmware_dir.to_string_lossy().into_owned(),
        persistence_type: backends.firmware_objects.name().to_owned(),
        tls_mode: TlsMode::from_config(&tls),
    };
    let metrics_registry = metrics::init(&rms_config)
        .map_err(|e| format!("failed to initialize metrics: {}", e.message))?;

    tokio::fs::create_dir_all(&cli.firmware_dir)
        .await
        .map_err(|e| {
            format!(
                "failed to create firmware directory {}: {e}",
                cli.firmware_dir.display()
            )
        })?;

    let mut grpc_server = GrpcServer::new(
        cli.port,
        rack_manager,
        job_tracker,
        backends,
        metrics_registry,
    )
    .with_firmware_dir(cli.firmware_dir.clone())
    .with_sftp_upload_options(sftp_upload_options);

    if let Some(tls) = tls {
        grpc_server = grpc_server.with_tls(tls);
    } else {
        grpc_server = grpc_server.with_listener_mode(listener_mode);
    }

    if switch_tls_roots.insecure_switch {
        tracing::warn!(
            "--insecure-switch disables switch client mTLS; NVUE uses unverified HTTPS and NMX-C uses plaintext HTTP"
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

    Ok(grpc_server)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Pin rustls' crypto provider; `aws-lc-rs` and `ring` are both linked in,
    // so auto-selection fails and rustls panics on first TLS use.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // Promote only job_id from active spans so nested SFTP progress events can
    // be correlated with jobs without adding node IDs or paths to upload spans.
    setup_logging(vec!["job_id".to_string()])?;

    let cli = Cli::parse();

    let insecure_permit = match assert_insecure_allowed(cli.insecure) {
        Ok(permit) => permit,
        Err(msg) => {
            tracing::error!("{msg}");
            std::process::exit(1);
        }
    };

    // Resolve TLS up front: validates the --tls-* / --insecure combination and
    // reads the cert/key/CA once, before any side-effecting startup step.
    let tls = match TlsConfig::resolve(
        cli.tls_cert.as_deref(),
        cli.tls_key.as_deref(),
        cli.tls_ca.as_deref(),
        insecure_permit,
    ) {
        Ok(tls) => tls,
        Err(msg) => {
            tracing::error!("{msg}");
            std::process::exit(1);
        }
    };

    if let Err(msg) = validate_switch_tls_config(&cli) {
        tracing::error!("{msg}");
        std::process::exit(1);
    }

    let switch_tls_roots = build_switch_tls_roots(&cli);
    let listener_mode = ApiListenerMode::from_tls_config(&tls);

    let mut grpc_server = match start_service(&cli, tls, listener_mode, switch_tls_roots).await {
        Ok(s) => s,
        Err(msg) => {
            tracing::error!("{msg}");
            std::process::exit(1);
        }
    };

    tracing::info!(
        port = cli.port,
        "Rack Management Service running (press Ctrl+C to stop)"
    );

    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl+C");

    tracing::info!("shutting down...");
    grpc_server.stop();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(insecure: bool) -> Cli {
        let sftp_upload_options = SftpUploadOptions::default();

        Cli {
            port: 50051,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            insecure,
            switch_cert_root: None,
            client_tls_root: None,
            default_switch_domain: None,
            dns_domain: None,
            insecure_switch: false,
            db_url: None,
            db_pool_max: 20,
            firmware_dir: PathBuf::from("firmware"),
            sftp_upload_timeout_seconds: sftp_upload_options.overall_timeout.as_secs(),
            sftp_step_timeout_seconds: sftp_upload_options.step_timeout.as_secs(),
            max_tracked_jobs: NonZeroUsize::new(10_000).unwrap(),
        }
    }

    #[test]
    fn cli_defaults_sftp_upload_options() {
        use clap::CommandFactory;

        let command = Cli::command();
        let upload_default = command
            .get_arguments()
            .find(|arg| arg.get_id() == "sftp_upload_timeout_seconds")
            .expect("upload timeout arg")
            .get_default_values();

        let step_default = command
            .get_arguments()
            .find(|arg| arg.get_id() == "sftp_step_timeout_seconds")
            .expect("step timeout arg")
            .get_default_values();

        let sftp_upload_options = SftpUploadOptions::default();

        assert_eq!(
            upload_default,
            &[sftp_upload_options.overall_timeout.as_secs().to_string()]
        );

        assert_eq!(
            step_default,
            &[sftp_upload_options.step_timeout.as_secs().to_string()]
        );
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
        // cert + key without --tls-ca is TLS-only: server is authenticated but
        // callers are not. Rejected at startup to enforce mTLS.
        let (_dir, cert, key) = temp_readable_files();
        let Err(e) = TlsConfig::resolve(Some(&cert), Some(&key), None, None) else {
            panic!("cert + key without --tls-ca should be rejected");
        };
        assert!(
            e.contains("--tls-ca"),
            "error must name the missing flag: {e}"
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
            panic!("no TLS without --insecure should be rejected");
        };
        assert!(
            e.contains("--insecure"),
            "error must mention --insecure: {e}"
        );
    }

    #[test]
    fn ca_without_cert_rejected() {
        let Err(e) = TlsConfig::resolve(None, None, Some("ca.pem"), None) else {
            panic!("--tls-ca without cert should be rejected");
        };
        assert!(e.contains("--tls-ca requires"));
    }

    #[test]
    fn tls_without_ca_rejected_even_with_insecure() {
        // --insecure only enables plaintext (no certs at all). Providing certs
        // without --tls-ca is still rejected because TLS-only is not a valid mode.
        let (_dir, cert, key) = temp_readable_files();
        let Err(e) = TlsConfig::resolve(Some(&cert), Some(&key), None, None) else {
            panic!("cert + key without --tls-ca should be rejected even with --insecure");
        };
        assert!(
            e.contains("--tls-ca"),
            "error must name the missing flag: {e}"
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
        let c = Cli {
            default_switch_domain: Some("../bad".into()),
            ..cli(true)
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
            let c = Cli {
                dns_domain: Some(invalid.into()),
                ..cli(true)
            };

            assert!(
                validate_dns_domain(&c).is_err(),
                "{invalid} must not be accepted as a TLS server name"
            );
        }
    }

    #[test]
    fn valid_dns_domain_is_propagated_to_tls_roots() {
        let c = Cli {
            dns_domain: Some("switch.example.com".into()),
            ..cli(true)
        };

        assert!(validate_dns_domain(&c).is_ok());

        let roots = build_switch_tls_roots(&c);

        assert_eq!(roots.dns_domain.as_deref(), Some("switch.example.com"));
    }

    #[test]
    fn insecure_switch_flag_is_propagated_to_tls_roots() {
        let c = Cli::parse_from(["rackmanagementservice", "--insecure-switch"]);

        assert!(c.insecure_switch);

        let roots = build_switch_tls_roots(&c);

        assert!(roots.insecure_switch);
    }

    #[test]
    fn tls_root_dirs_must_exist_when_set() {
        let c = Cli {
            client_tls_root: Some(PathBuf::from("/nonexistent/client-tls")),
            ..cli(true)
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
        let c = Cli {
            client_tls_root: Some(client_tls),
            ..cli(true)
        };
        let result = validate_tls_roots(&c);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--client-tls-root requires --default-switch-domain")
        );
    }

    #[test]
    fn insecure_switch_skips_switch_tls_startup_requirements() {
        let c = Cli {
            switch_cert_root: Some(PathBuf::from("/nonexistent/switch-cert")),
            client_tls_root: Some(PathBuf::from("/nonexistent/client-tls")),
            default_switch_domain: Some("../bad".into()),
            dns_domain: Some("bad host".into()),
            insecure_switch: true,
            ..cli(true)
        };

        assert!(validate_switch_tls_config(&c).is_ok());
    }

    #[test]
    fn tls_roots_built_from_existing_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cli {
            switch_cert_root: Some(dir.path().join("switch-certs")),
            client_tls_root: Some(dir.path().join("client-tls")),
            default_switch_domain: Some("site-wide".into()),
            ..cli(true)
        };
        std::fs::create_dir_all(c.switch_cert_root.as_ref().unwrap()).unwrap();
        std::fs::create_dir_all(c.client_tls_root.as_ref().unwrap()).unwrap();
        assert!(validate_tls_roots(&c).is_ok());
        let roots = build_switch_tls_roots(&c);
        assert!(roots.switch_cert.is_some());
        assert!(roots.client_tls.is_some());
    }

    #[tokio::test]
    async fn start_service_binds_to_port() {
        let port = portpicker::pick_unused_port().expect("no free port");
        let c = Cli { port, ..cli(true) };
        let mut server = start_service(
            &c,
            None,
            ApiListenerMode::Insecure,
            SwitchTlsRoots::default(),
        )
        .await
        .unwrap();
        server.stop();
    }

    #[tokio::test]
    async fn start_service_invalid_port_zero_succeeds() {
        let c = Cli {
            port: 0,
            ..cli(true)
        };
        if let Ok(mut s) = start_service(
            &c,
            None,
            ApiListenerMode::Insecure,
            SwitchTlsRoots::default(),
        )
        .await
        {
            s.stop();
        }
    }
}
