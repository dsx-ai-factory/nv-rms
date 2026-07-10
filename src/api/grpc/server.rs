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

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use prometheus::Registry;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::service::{LayerExt, Routes};
use tonic::transport::{Identity, Server, ServerTlsConfig};

use crate::api::grpc::conversions::timestamp_from_datetime;
use crate::libnmxc::TlsMaterialStore;
use crate::nodes::NodeInstance;
use crate::orchestrator::job_tracker::{JobInfo, JobTracker};
use crate::orchestrator::rack_manager::RackManager;
use crate::persistence::Backends;
use crate::racks::ManagedRack;
use crate::transport::ssh::{SFTP_UPLOAD_BUFFER_SIZE_BYTES, SftpUploadOptions};
use crate::utilities::error::{Result, RmsError};
use librms::protos::rack_manager as rm;
use librms::protos::rack_manager::rack_manager_server::{
    RackManager as RackManagerTrait, RackManagerServer,
};

const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Environment variable that must be set to `1` before `--insecure` is honored.
/// Development and testing only; production images should not set this.
pub const RMS_ALLOW_INSECURE_ENV: &str = "RMS_ALLOW_INSECURE";

/// Returns `Ok(())` when plaintext gRPC is explicitly permitted via
/// [`RMS_ALLOW_INSECURE_ENV`].
pub fn insecure_permitted() -> std::result::Result<(), String> {
    if std::env::var(RMS_ALLOW_INSECURE_ENV).as_deref() == Ok("1") {
        Ok(())
    } else {
        Err(format!(
            "--insecure requires {RMS_ALLOW_INSECURE_ENV}=1 (development/testing only)"
        ))
    }
}

/// Proof that [`assert_insecure_allowed`] passed for `--insecure`. Required to
/// request plaintext via [`TlsConfig::resolve`]. Library unit tests use
/// [`Self::for_test`].
#[derive(Clone, Copy, Debug)]
pub struct InsecurePermit {
    _private: (),
}

impl InsecurePermit {
    /// Mint a permit after the env gate passes. **`pub(crate)` by design** — do
    /// not widen to `pub`.
    ///
    /// External callers cannot construct a permit directly (compile-fail guard):
    ///
    /// ```compile_fail
    /// use rackmanagementservice::api::grpc::server::InsecurePermit;
    /// InsecurePermit::new();
    /// ```
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }

    /// Unit tests that exercise [`TlsConfig::resolve`] without the startup env gate.
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self { _private: () }
    }
}

/// One-time startup check: `--insecure` is only allowed when
/// [`RMS_ALLOW_INSECURE_ENV`] is set. Returns a permit to pass to
/// [`TlsConfig::resolve`] when `insecure` is true.
pub fn assert_insecure_allowed(
    insecure: bool,
) -> std::result::Result<Option<InsecurePermit>, String> {
    if insecure {
        insecure_permitted()?;
        Ok(Some(InsecurePermit::new()))
    } else {
        Ok(None)
    }
}

/// Per-domain TLS material roots configured at RMS startup.
#[derive(Clone, Debug, Default)]
pub struct SwitchTlsRoots {
    /// Material installed onto switches (`<switch_cert_root>/<domain>/`).
    pub switch_cert: Option<TlsMaterialStore>,

    /// RMS client mTLS material for outbound switch NVUE/NMX-C connections.
    /// Required unless [`Self::insecure_switch`] is true.
    pub client_tls: Option<TlsMaterialStore>,

    /// Fallback when switch RPCs omit `domain`.
    pub default_domain: Option<String>,

    /// Optional DNS domain override used as TLS server name (SNI) authority.
    pub dns_domain: Option<String>,

    /// Force outbound switch clients to avoid switch mTLS.
    pub insecure_switch: bool,
}

fn nonempty_trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn normalize_endpoint_host(host: &str) -> &str {
    let host = host.trim();

    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

impl SwitchTlsRoots {
    /// Default NVLink material domain configured via `--default-switch-domain`.
    pub fn configured_default_switch_domain(&self) -> Option<&str> {
        nonempty_trimmed(self.default_domain.as_deref())
    }

    /// DNS domain configured via `--dns-domain`, when set.
    pub fn configured_dns_domain(&self) -> Option<&str> {
        nonempty_trimmed(self.dns_domain.as_deref())
    }

    /// TLS identity for switch connections.
    ///
    /// Configured `--dns-domain` is the exact authority. When it is absent, keep
    /// the endpoint host/name; the NVLink domain is only for TLS material lookup.
    pub(crate) fn tls_server_name_for_endpoint(
        &self,
        endpoint_server_name: &str,
        _request_domain: Option<&str>,
    ) -> std::result::Result<String, String> {
        if let Some(dns_domain) = self.configured_dns_domain() {
            return crate::libnmxc::normalize_tls_server_name(dns_domain);
        }

        Ok(normalize_endpoint_host(endpoint_server_name).to_owned())
    }

    /// NVLink domain from the RPC request, or [`Self::default_domain`] when omitted.
    pub fn effective_domain(
        &self,
        request_domain: Option<&str>,
    ) -> std::result::Result<String, String> {
        if let Some(domain) = nonempty_trimmed(request_domain) {
            return crate::libnmxc::normalize_domain(domain);
        }

        self.configured_default_switch_domain()
            .map(crate::libnmxc::normalize_domain)
            .transpose()?
            .ok_or_else(|| {
                "domain was not provided and --default-switch-domain is not configured".into()
            })
    }

    pub fn resolve_client_tls(
        &self,
        request_domain: Option<&str>,
    ) -> std::result::Result<crate::libnmxc::NmxcTlsConfig, String> {
        let store = self
            .client_tls
            .as_ref()
            .ok_or_else(|| "client TLS is not configured (--client-tls-root)".to_string())?;

        let domain = self.effective_domain(request_domain)?;

        store.resolve_for_nvlink_domain(&domain, None)
    }

    /// Resolve RMS's outbound NVUE identity for one NVLink domain.
    pub fn resolve_required_nvue_client_tls(
        &self,
        request_domain: Option<&str>,
    ) -> std::result::Result<nvue_client::ClientTls, String> {
        let store = self
            .client_tls
            .as_ref()
            .ok_or_else(|| "client TLS is not configured (--client-tls-root)".to_string())?;

        let domain = self.effective_domain(request_domain)?;
        let tls = store.resolve_for_nvlink_domain(&domain, None)?;

        Self::nvue_client_tls(&tls)
    }

    /// Resolve RMS's outbound NVUE identity unless `--insecure-switch` disabled it.
    ///
    /// `Ok(None)` means NVUE client mTLS has been explicitly disabled by
    /// `--insecure-switch`. Missing client material is an error so switch NVUE
    /// paths cannot silently fall back to non-mTLS.
    pub fn resolve_nvue_client_tls(
        &self,
        request_domain: Option<&str>,
    ) -> std::result::Result<Option<nvue_client::ClientTls>, String> {
        if self.insecure_switch {
            return Ok(None);
        }

        self.resolve_required_nvue_client_tls(request_domain)
            .map(Some)
    }

    fn nvue_client_tls(
        tls: &crate::libnmxc::NmxcTlsConfig,
    ) -> std::result::Result<nvue_client::ClientTls, String> {
        let ca_cert_path = tls
            .ca_cert_path
            .clone()
            .ok_or_else(|| "NVUE TLS CA certificate path is required".to_owned())?;

        let client_cert_path = tls
            .client_cert_path
            .clone()
            .ok_or_else(|| "NVUE TLS client certificate path is required".to_owned())?;

        let client_key_path = tls
            .client_key_path
            .clone()
            .ok_or_else(|| "NVUE TLS client key path is required".to_owned())?;

        Ok(nvue_client::ClientTls::new(
            ca_cert_path,
            client_cert_path,
            client_key_path,
        ))
    }

    pub fn resolve_switch_cert(
        &self,
        request_domain: Option<&str>,
    ) -> std::result::Result<crate::libnmxc::NmxcTlsConfig, String> {
        let store = self.switch_cert.as_ref().ok_or_else(|| {
            "switch certificate material is not configured (--switch-cert-root)".to_string()
        })?;
        let domain = self.effective_domain(request_domain)?;
        store.resolve_for_nvlink_domain(&domain, None)
    }
}

/// TLS material that has already been read from disk and parsed, ready to hand
/// to the tonic server. Produced by [`TlsConfig::resolve`]; the parsed bytes
/// are held in memory so the actual bind never re-reads the files.
pub struct TlsConfig {
    config: ServerTlsConfig,
}

impl TlsConfig {
    /// Validates the `--tls-cert` / `--tls-key` / `--tls-ca` / `--insecure`
    /// argument combination and, when TLS is requested, reads and parses the
    /// PEM files exactly once.
    ///
    /// * `Ok(None)` -- no TLS material (`--insecure`); the caller binds plaintext.
    /// * `Ok(Some(_))` -- mTLS material (cert/key/CA) read and parsed.
    /// * `Err(msg)` -- an inconsistent argument combination or an unreadable
    ///   file, with an operator-facing message.
    ///
    /// `--tls-ca` is required whenever `--tls-cert`/`--tls-key` are provided.
    /// TLS without a client CA authenticates the server but not the caller,
    /// leaving all RPCs open to any TLS client. `--insecure` is the only
    /// supported non-mTLS mode (development/testing only). Pass
    /// `Some([`InsecurePermit`])` only after [`assert_insecure_allowed`] succeeds.
    ///
    /// Reading happens here, so call it before any side-effecting startup step:
    /// a bad path fails fast, and the parsed bytes returned are the exact
    /// material used to bind, with no later re-read that could race a rotation.
    pub fn resolve(
        cert: Option<&str>,
        key: Option<&str>,
        ca: Option<&str>,
        insecure: Option<InsecurePermit>,
    ) -> std::result::Result<Option<Self>, String> {
        let (cert, key) = match (cert, key) {
            (Some(cert), Some(key)) => (cert, key),
            (None, None) if ca.is_some() => {
                return Err("--tls-ca requires --tls-cert and --tls-key".into());
            }
            (None, None) if insecure.is_some() => return Ok(None),
            (None, None) => {
                return Err("provide --tls-cert, --tls-key, and --tls-ca for mTLS, \
                     or --insecure for testing"
                    .into());
            }
            _ => return Err("--tls-cert and --tls-key must be provided together".into()),
        };

        // --tls-ca is mandatory with certs.
        if ca.is_none() {
            return Err(
                "--tls-ca is required; TLS without client-certificate verification \
                 leaves all RPCs unauthenticated. Use --insecure for development."
                    .into(),
            );
        }

        let read =
            |path: &str| std::fs::read(path).map_err(|e| format!("failed to read {path}: {e}"));

        // ca is Some: the ca.is_none() guard above returned early.
        let ca = ca.unwrap();
        let identity = Identity::from_pem(read(cert)?, read(key)?);
        let config = ServerTlsConfig::new()
            .identity(identity)
            .client_ca_root(tonic::transport::Certificate::from_pem(read(ca)?));

        Ok(Some(Self { config }))
    }
}

/// The TLS configuration mode the server was started with.
///
/// Used as a label value on the `rms_api_config` gauge so that operators can
/// tell at a glance whether the running instance is plaintext, TLS, or mTLS.
#[derive(Debug, Clone, Copy)]
pub enum TlsMode {
    /// Server bound without TLS (`--insecure`). Development/testing only.
    Insecure,
    /// Mutual TLS — both server and clients present certificates.
    Mtls,
}

const TLS_MODE_LABEL_INSECURE: &str = "insecure";
const TLS_MODE_LABEL_MTLS: &str = "mTLS";

impl TlsMode {
    pub fn as_label(self) -> &'static str {
        match self {
            TlsMode::Insecure => TLS_MODE_LABEL_INSECURE,
            TlsMode::Mtls => TLS_MODE_LABEL_MTLS,
        }
    }

    pub fn from_config(config: &Option<TlsConfig>) -> Self {
        match config {
            // TlsConfig::resolve currently requires --tls-ca, so Some always means mTLS.
            // Update this arm if TLS-without-CA is ever supported.
            Some(_) => Self::Mtls,
            None => Self::Insecure,
        }
    }
}

impl fmt::Display for TlsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_label())
    }
}

/// How the RMS API gRPC listener binds at startup.
///
/// The process entrypoint resolves this after validating CLI flags and the
/// `RMS_ALLOW_INSECURE` gate; tests may inject [`Self::Insecure`] directly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ApiListenerMode {
    /// Require mTLS material; refuse plaintext binding.
    #[default]
    Secure,
    /// Plaintext gRPC (development/testing only).
    Insecure,
}

impl ApiListenerMode {
    pub fn from_tls_config(tls: &Option<TlsConfig>) -> Self {
        if tls.is_some() {
            Self::Secure
        } else {
            Self::Insecure
        }
    }
}

#[cfg(test)]
mod switch_tls_roots_tests {
    use super::*;

    #[test]
    fn configured_domains_return_trimmed_values() {
        let roots = SwitchTlsRoots {
            default_domain: Some(" site-wide ".into()),
            dns_domain: Some(" switch.example.com ".into()),
            ..SwitchTlsRoots::default()
        };

        assert_eq!(roots.configured_default_switch_domain(), Some("site-wide"));
        assert_eq!(roots.configured_dns_domain(), Some("switch.example.com"));
    }

    #[test]
    fn tls_server_name_uses_dns_domain_override_or_endpoint() {
        for (dns_domain, endpoint, expected) in [
            (
                Some("switch.example.com"),
                "sw100.example.com",
                "switch.example.com",
            ),
            (
                Some("switch.example.com"),
                "10.0.0.11",
                "switch.example.com",
            ),
            (Some("switch.example.com"), "fd00::11", "switch.example.com"),
            (None, "10.0.0.11", "10.0.0.11"),
        ] {
            let roots = SwitchTlsRoots {
                dns_domain: dns_domain.map(str::to_owned),
                ..SwitchTlsRoots::default()
            };

            assert_eq!(
                roots.tls_server_name_for_endpoint(endpoint, None).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn tls_server_name_uses_endpoint_when_client_tls_enabled_without_dns_domain() {
        let client_root = tempfile::tempdir().unwrap();

        let roots = SwitchTlsRoots {
            client_tls: Some(TlsMaterialStore::new(client_root.path())),
            default_domain: Some("fabric-a".into()),
            ..SwitchTlsRoots::default()
        };

        assert_eq!(
            roots
                .tls_server_name_for_endpoint("switch.example.com", None)
                .unwrap(),
            "switch.example.com"
        );

        assert_eq!(
            roots
                .tls_server_name_for_endpoint("10.0.0.11", Some("rack-a"))
                .unwrap(),
            "10.0.0.11"
        );
    }

    #[test]
    fn effective_domain_uses_request_when_set() {
        let roots = SwitchTlsRoots {
            default_domain: Some("site-wide".into()),
            ..SwitchTlsRoots::default()
        };
        assert_eq!(
            roots.effective_domain(Some("fabric-a")).unwrap(),
            "fabric-a"
        );
    }

    #[test]
    fn effective_domain_falls_back_to_default() {
        let roots = SwitchTlsRoots {
            default_domain: Some("site-wide".into()),
            ..SwitchTlsRoots::default()
        };

        assert_eq!(roots.effective_domain(None).unwrap(), "site-wide");
        assert_eq!(roots.effective_domain(Some("")).unwrap(), "site-wide");
    }

    #[test]
    fn effective_domain_errors_without_request_or_default() {
        let roots = SwitchTlsRoots::default();
        assert!(roots.effective_domain(None).is_err());
    }

    #[test]
    fn effective_domain_rejects_dot_default() {
        let roots = SwitchTlsRoots {
            default_domain: Some(".".into()),
            ..SwitchTlsRoots::default()
        };
        assert!(roots.effective_domain(None).is_err());
    }

    #[test]
    fn resolve_client_tls_uses_client_material_root() {
        let switch_root = tempfile::tempdir().unwrap();
        let client_root = tempfile::tempdir().unwrap();
        let domain = "site-wide";

        write_tls_material(switch_root.path(), domain, b"switch");
        write_tls_material(client_root.path(), domain, b"client");

        let roots = SwitchTlsRoots {
            switch_cert: Some(TlsMaterialStore::new(switch_root.path())),
            client_tls: Some(TlsMaterialStore::new(client_root.path())),
            default_domain: Some(domain.into()),
            dns_domain: Some("switch.example.com".into()),
            insecure_switch: false,
        };

        let tls = roots.resolve_client_tls(None).unwrap();
        let expected_ca_path = client_root.path().join(domain).join("ca.pem");

        assert_eq!(
            tls.ca_cert_path.as_deref(),
            Some(expected_ca_path.as_path())
        );

        assert_eq!(tls.authority, None);

        let nvue_tls = roots.resolve_nvue_client_tls(None).unwrap().unwrap();

        assert_eq!(nvue_tls.as_paths().ca_cert_path, expected_ca_path.as_path());

        let switch_tls = roots.resolve_switch_cert(None).unwrap();

        assert_eq!(switch_tls.authority, None);
    }

    #[test]
    fn insecure_switch_suppresses_nvue_client_tls_material_lookup() {
        let roots = SwitchTlsRoots {
            client_tls: Some(TlsMaterialStore::new("missing-client-tls-root")),
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        };

        assert!(roots.resolve_nvue_client_tls(None).unwrap().is_none());
    }

    #[test]
    fn nmx_client_tls_resolver_requires_material() {
        let roots = SwitchTlsRoots::default();

        let error = roots.resolve_client_tls(None).unwrap_err();

        assert_eq!(error, "client TLS is not configured (--client-tls-root)");
    }

    #[test]
    fn nvue_client_tls_resolver_requires_material_unless_insecure() {
        let roots = SwitchTlsRoots::default();

        let error = roots.resolve_nvue_client_tls(None).unwrap_err();

        assert_eq!(error, "client TLS is not configured (--client-tls-root)");
    }

    fn write_tls_material(root: &std::path::Path, domain: &str, contents: &[u8]) {
        let dir = root.join(domain);
        std::fs::create_dir_all(&dir).unwrap();

        for file_name in ["ca.pem", "client.pem", "client.key"] {
            std::fs::write(dir.join(file_name), contents).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::job_tracker::{JobError, JobType};
    use crate::with_test_env;
    use std::time::Duration;

    // ── TlsMode ──────────────────────────────────────────────────────────────

    #[test]
    fn tls_mode_labels() {
        // Test enum values match their labels.
        assert_eq!(TlsMode::Insecure.as_label(), TLS_MODE_LABEL_INSECURE);
        assert_eq!(TlsMode::Mtls.as_label(), TLS_MODE_LABEL_MTLS);

        // Also test display matches label.
        for mode in [TlsMode::Insecure, TlsMode::Mtls] {
            assert_eq!(mode.to_string(), mode.as_label());
        }
    }

    #[test]
    fn tls_mode_from_config_none_is_insecure() {
        assert!(matches!(TlsMode::from_config(&None), TlsMode::Insecure));
    }

    #[test]
    fn tls_mode_from_config_some_is_mtls() {
        let (_dir, cert, key, ca) = temp_readable_files_with_ca();
        let tls = TlsConfig::resolve(Some(&cert), Some(&key), Some(&ca), None)
            .expect("resolve ok")
            .expect("should be Some with cert+key+ca");
        assert!(matches!(TlsMode::from_config(&Some(tls)), TlsMode::Mtls));
    }

    /// Creates three placeholder (non-PEM) readable files in a temp directory.
    /// Content is intentionally invalid PEM; tonic parses lazily at bind time,
    /// so `TlsConfig::resolve` accepts them.
    fn temp_readable_files_with_ca() -> (tempfile::TempDir, String, String, String) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        let ca = dir.path().join("ca.pem");
        std::fs::write(&cert, b"placeholder").expect("write cert");
        std::fs::write(&key, b"placeholder").expect("write key");
        std::fs::write(&ca, b"placeholder").expect("write ca");
        let (c, k, a) = (
            cert.to_str().unwrap().to_string(),
            key.to_str().unwrap().to_string(),
            ca.to_str().unwrap().to_string(),
        );
        (dir, c, k, a)
    }

    #[test]
    fn get_job_status_returns_parent_only_by_default() {
        let tracker = JobTracker::with_max_tracked_jobs(8);
        let child_id = tracker
            .create_job("rack-1", "node-1", JobType::FirmwareUpdate)
            .expect("child job should be created");
        let parent_id = tracker
            .create_parent_job(
                "rack-1",
                std::slice::from_ref(&child_id),
                JobType::FirmwareUpdate,
            )
            .expect("parent job should be created");

        let response = get_job_status_from_tracker(
            &tracker,
            rm::GetJobStatusRequest {
                job_id: parent_id.clone(),
                include_child_job_states: false,
            },
        )
        .expect("parent job should be returned");

        assert_eq!(response.job_states.len(), 1);
        assert_eq!(response.job_states[0].job_id, parent_id);
        assert_eq!(response.job_states[0].child_job_ids, vec![child_id]);
        assert_eq!(response.job_states[0].parent_job_id, None);
    }

    #[test]
    fn get_job_status_includes_child_states_when_requested() {
        let tracker = JobTracker::with_max_tracked_jobs(8);
        let completed_child = tracker
            .create_job("rack-1", "node-1", JobType::FirmwareUpdate)
            .expect("completed child job should be created");
        let queued_child = tracker
            .create_job("rack-1", "node-2", JobType::FirmwareUpdate)
            .expect("queued child job should be created");
        tracker.mark_completed(&completed_child, Some("{\"updated\":true}".to_owned()));
        let parent_id = tracker
            .create_parent_job(
                "rack-1",
                &[completed_child.clone(), queued_child.clone()],
                JobType::FirmwareUpdate,
            )
            .expect("parent job should be created");

        let response = get_job_status_from_tracker(
            &tracker,
            rm::GetJobStatusRequest {
                job_id: parent_id.clone(),
                include_child_job_states: true,
            },
        )
        .expect("parent and child jobs should be returned");

        assert_eq!(response.job_states.len(), 3);
        assert_eq!(response.job_states[0].job_id, parent_id);
        assert_eq!(response.job_states[1].job_id, completed_child);
        assert_eq!(
            response.job_states[1].parent_job_id.as_deref(),
            Some(response.job_states[0].job_id.as_str())
        );
        assert_eq!(
            response.job_states[1].execution_state,
            rm::JobExecutionState::Completed as i32
        );
        assert_eq!(
            response.job_states[1].error_code,
            i32::from(rm::JobError::from(JobError::Unspecified))
        );
        assert_eq!(response.job_states[1].result_json, "{\"updated\":true}");
        assert_eq!(response.job_states[2].job_id, queued_child);
        assert_eq!(
            response.job_states[2].parent_job_id.as_deref(),
            Some(response.job_states[0].job_id.as_str())
        );
    }

    #[test]
    fn get_job_status_rejects_empty_job_id() {
        let tracker = JobTracker::with_max_tracked_jobs(8);
        let error = get_job_status_from_tracker(
            &tracker,
            rm::GetJobStatusRequest {
                job_id: " ".to_owned(),
                include_child_job_states: false,
            },
        )
        .expect_err("empty job_id should be rejected");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    // ── TlsConfig ────────────────────────────────────────────────────────────

    #[test]
    fn resolve_rejects_tls_without_ca() {
        // File paths are never read when ca is None, so they need not exist.
        match TlsConfig::resolve(Some("cert.pem"), Some("key.pem"), None, None) {
            Err(e) => assert!(
                e.contains("--tls-ca"),
                "error must name the missing flag: {e}"
            ),
            Ok(_) => panic!("expected error for TLS without --tls-ca"),
        }
    }

    #[test]
    fn assert_insecure_allowed_requires_env_gate() {
        assert!(assert_insecure_allowed(false).unwrap().is_none());

        with_test_env!(RMS_ALLOW_INSECURE_ENV, unset, {
            match assert_insecure_allowed(true) {
                Err(err) => assert!(
                    err.contains(RMS_ALLOW_INSECURE_ENV),
                    "error must name the env gate: {err}"
                ),
                Ok(Some(_)) => panic!("expected error without {RMS_ALLOW_INSECURE_ENV}=1"),
                Ok(None) => panic!("expected error without {RMS_ALLOW_INSECURE_ENV}=1"),
            }
        });

        with_test_env!(RMS_ALLOW_INSECURE_ENV => "1", {
            assert!(assert_insecure_allowed(true).unwrap().is_some());
        });
    }

    #[test]
    fn resolve_insecure_requires_permit() {
        match TlsConfig::resolve(None, None, None, None) {
            Err(err) => assert!(
                err.contains("--insecure"),
                "must reject plaintext without InsecurePermit: {err}"
            ),
            Ok(_) => panic!("expected error without InsecurePermit"),
        }
    }

    #[test]
    fn resolve_insecure_returns_none_with_permit() {
        let result =
            TlsConfig::resolve(None, None, None, Some(InsecurePermit::for_test())).unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn start_rejects_plaintext_without_insecure_mode() {
        use crate::metrics::{self, RmsConfig};

        let port = portpicker::pick_unused_port().expect("no free port");

        let metrics_registry = metrics::init(&RmsConfig {
            grpc_port: port,
            firmware_dir: "firmware".to_string(),
            persistence_type: "memory".to_string(),
            tls_mode: TlsMode::Insecure,
        })
        .expect("metrics init");

        let mut server = GrpcServer::new(
            port,
            Arc::new(RackManager::new()),
            Arc::new(JobTracker::new()),
            Backends::memory(),
            metrics_registry,
        );
        let err = server.start().await.unwrap_err();
        assert!(
            err.message.contains("refusing to start"),
            "expected fail-closed error, got: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn start_rejects_invalid_sftp_upload_options() {
        use crate::metrics::{self, RmsConfig};

        let port = portpicker::pick_unused_port().expect("no free port");

        let metrics_registry = metrics::init(&RmsConfig {
            grpc_port: port,
            firmware_dir: "firmware".to_string(),
            persistence_type: "memory".to_string(),
            tls_mode: TlsMode::Insecure,
        })
        .expect("metrics init");

        let invalid_options = SftpUploadOptions {
            overall_timeout: Duration::from_secs(30),
            step_timeout: Duration::from_secs(31),
        };

        let mut server = GrpcServer::new(
            port,
            Arc::new(RackManager::new()),
            Arc::new(JobTracker::new()),
            Backends::memory(),
            metrics_registry,
        )
        .with_sftp_upload_options(invalid_options);

        let err = server.start().await.unwrap_err();

        assert_eq!(
            err.code,
            crate::utilities::error::ErrorCode::InvalidArgument
        );

        assert!(err.message.contains("step timeout"));
    }

    #[tokio::test]
    async fn firmware_dir_write_check_creates_and_cleans_probe_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let firmware_dir = temp.path().join("firmware");

        validate_firmware_dir_writable(&firmware_dir)
            .await
            .expect("writable firmware directory should pass startup check");

        let mut entries = tokio::fs::read_dir(&firmware_dir)
            .await
            .expect("read firmware dir");
        assert!(
            entries
                .next_entry()
                .await
                .expect("read next entry")
                .is_none(),
            "startup probe file should be removed"
        );
    }

    #[tokio::test]
    async fn firmware_dir_write_check_rejects_non_directory_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let firmware_dir = temp.path().join("firmware");
        tokio::fs::write(&firmware_dir, b"not a directory")
            .await
            .expect("create conflicting file");

        let err = validate_firmware_dir_writable(&firmware_dir)
            .await
            .expect_err("file path should fail firmware directory startup check");

        assert_eq!(
            err.code,
            crate::utilities::error::ErrorCode::FailedPrecondition
        );
        assert!(
            err.message.contains("failed to create firmware directory"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn nvue_tls_fallback_requires_an_active_non_strict_transport() {
        for (strict, active, expected) in [
            (false, false, false),
            (false, true, true),
            (true, false, false),
            (true, true, false),
        ] {
            assert_eq!(can_keep_current_nvue_transport(strict, active), expected);
        }
    }

    async fn write_nvue_test_material(
        root: &std::path::Path,
        domain: &str,
    ) -> nvue_client::ClientTls {
        let domain_dir = root.join(domain);
        let certificate =
            rcgen::generate_simple_self_signed(vec!["switch.example.com".to_owned()]).unwrap();

        tokio::fs::create_dir_all(&domain_dir).await.unwrap();

        let ca = domain_dir.join("ca.pem");
        let cert = domain_dir.join("client.pem");
        let key = domain_dir.join("client.key");

        tokio::fs::write(&ca, certificate.cert.pem()).await.unwrap();
        tokio::fs::write(&cert, certificate.cert.pem())
            .await
            .unwrap();

        tokio::fs::write(&key, certificate.key_pair.serialize_pem())
            .await
            .unwrap();

        nvue_client::ClientTls::new(ca, cert, key)
    }

    async fn active_nvue_test_client(tls: nvue_client::ClientTls) -> nvue_client::SharedClient {
        nvue_client::Client::connect(
            nvue_client::ClientConfig {
                endpoint: nvue_client::ClientEndpoint::https(
                    "switch.example.com",
                    "127.0.0.1",
                    443,
                ),
                credentials: nvue_client::ClientCredentials::new("admin", "password"),
                dangerously_accept_invalid_certs: false,
            },
            Some(tls),
        )
        .await
        .unwrap()
    }

    fn test_service(switch_tls_roots: SwitchTlsRoots) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::new()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            switch_tls_roots,
            sftp_upload_options: SftpUploadOptions::default(),
        }
    }

    #[tokio::test]
    async fn nvue_initialization_rejects_missing_domain_with_active_tls() {
        let root = tempfile::tempdir().unwrap();
        let tls = write_nvue_test_material(root.path(), "fabric-a").await;
        let client = active_nvue_test_client(tls).await;

        assert!(client.uses_client_tls());

        let service = test_service(SwitchTlsRoots {
            client_tls: Some(TlsMaterialStore::new(root.path())),
            ..SwitchTlsRoots::default()
        });

        let error = service
            .initialize_nvue_client(Some(&client), None)
            .await
            .unwrap_err();

        assert_eq!(
            error.message,
            "domain was not provided and --default-switch-domain is not configured"
        );

        assert!(client.uses_client_tls());
    }

    #[tokio::test]
    async fn nvue_initialization_rejects_missing_client_tls_root_with_active_tls() {
        let root = tempfile::tempdir().unwrap();
        let tls = write_nvue_test_material(root.path(), "fabric-a").await;
        let client = active_nvue_test_client(tls).await;
        let service = test_service(SwitchTlsRoots::default());

        let error = service
            .initialize_nvue_client_with_options(Some(&client), None, None, true)
            .await
            .unwrap_err();

        assert_eq!(
            error.message,
            "client TLS is not configured (--client-tls-root)"
        );

        assert!(client.uses_client_tls());
    }

    #[tokio::test]
    async fn insecure_nvue_initialization_ignores_tls_root_and_dns_domain() {
        let client = nvue_client::Client::new(nvue_client::ClientConfig {
            endpoint: nvue_client::ClientEndpoint::https("10.0.0.11", "10.0.0.11", 443),
            credentials: nvue_client::ClientCredentials::new("admin", "password"),
            dangerously_accept_invalid_certs: false,
        })
        .unwrap();

        let service = test_service(SwitchTlsRoots {
            client_tls: Some(TlsMaterialStore::new("missing-client-tls-root")),
            dns_domain: Some("switch.example.com".into()),
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        });

        service
            .initialize_nvue_client(Some(&client), None)
            .await
            .unwrap();

        assert!(!client.uses_client_tls());
        assert_eq!(client.endpoint().host, "10.0.0.11");
    }

    #[tokio::test]
    async fn insecure_nvue_initialization_replaces_active_tls() {
        let root = tempfile::tempdir().unwrap();
        let tls = write_nvue_test_material(root.path(), "fabric-a").await;
        let client = active_nvue_test_client(tls).await;

        let service = test_service(SwitchTlsRoots {
            dns_domain: Some("switch.example.com".into()),
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        });

        service
            .initialize_nvue_client(Some(&client), None)
            .await
            .unwrap();

        let endpoint = client.endpoint();

        assert!(!client.uses_client_tls());
        assert_eq!(endpoint.host, "switch.example.com");
        assert_eq!(endpoint.connect_host, "127.0.0.1");
    }
}

/// gRPC server hosting the RackManager service.
pub struct GrpcServer {
    port: u16,
    rack_manager: Arc<RackManager>,
    job_tracker: Arc<JobTracker>,
    firmware_download_cancellations: Arc<Mutex<HashMap<String, CancellationToken>>>,
    backends: Backends,
    firmware_dir: PathBuf,
    tls: Option<TlsConfig>,
    listener_mode: ApiListenerMode,
    switch_tls_roots: SwitchTlsRoots,
    sftp_upload_options: SftpUploadOptions,
    metrics_registry: Arc<Registry>,
    shutdown_tx: Option<watch::Sender<()>>,
}

impl GrpcServer {
    pub fn new(
        port: u16,
        rack_manager: Arc<RackManager>,
        job_tracker: Arc<JobTracker>,
        backends: Backends,
        metrics_registry: Arc<Registry>,
    ) -> Self {
        Self {
            port,
            rack_manager,
            job_tracker,
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends,
            firmware_dir: PathBuf::from("firmware"),
            tls: None,
            listener_mode: ApiListenerMode::Secure,
            switch_tls_roots: SwitchTlsRoots::default(),
            sftp_upload_options: SftpUploadOptions::default(),
            metrics_registry,
            shutdown_tx: None,
        }
    }

    pub fn with_firmware_dir(mut self, firmware_dir: impl Into<PathBuf>) -> Self {
        self.firmware_dir = firmware_dir.into();
        self
    }

    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_listener_mode(mut self, mode: ApiListenerMode) -> Self {
        self.listener_mode = mode;
        self
    }

    pub fn with_insecure_listener(self) -> Self {
        self.with_listener_mode(ApiListenerMode::Insecure)
    }

    pub fn with_switch_tls_roots(mut self, switch_tls_roots: SwitchTlsRoots) -> Self {
        self.switch_tls_roots = switch_tls_roots;
        self
    }

    pub fn with_sftp_upload_options(mut self, sftp_upload_options: SftpUploadOptions) -> Self {
        self.sftp_upload_options = sftp_upload_options;
        self
    }

    pub async fn start(&mut self) -> Result<()> {
        self.sftp_upload_options.validate()?;

        let addr: SocketAddr = format!("0.0.0.0:{}", self.port)
            .parse()
            .map_err(|e| RmsError::invalid_argument(format!("invalid address: {e}")))?;

        // Build the server (and apply TLS if configured) before spawning so any
        // error is reported via this `Result` rather than swallowed inside the
        // background task. Borrow (don't take) `self.tls` and clone the parsed
        // config so a repeated `start()` cannot silently fall back to plaintext.
        let mut builder = Server::builder().accept_http1(true);
        if let Some(tls) = &self.tls {
            tracing::info!(port = self.port, mode = "mTLS", "gRPC server starting");
            builder = builder
                .tls_config(tls.config.clone())
                .map_err(|e| RmsError::internal(format!("failed to apply TLS config: {e}")))?;
        } else if self.listener_mode == ApiListenerMode::Insecure {
            tracing::warn!(
                port = self.port,
                mode = "insecure",
                "gRPC server starting WITHOUT TLS -- all RPCs are UNAUTHENTICATED and sent \
                 in plaintext. Development/testing only; never use --insecure in production. \
                 Provide --tls-cert, --tls-key, and --tls-ca for mTLS."
            );
        } else {
            return Err(RmsError::internal(
                "refusing to start gRPC server without mTLS. Provide --tls-cert, --tls-key, \
                 and --tls-ca. For local development only: --insecure with \
                 RMS_ALLOW_INSECURE=1",
            ));
        }
        if let Some(store) = &self.switch_tls_roots.switch_cert {
            tracing::info!(
                switch_cert_root = %store.base_dir().display(),
                "switch certificate install material root configured"
            );
        }
        if let Some(store) = &self.switch_tls_roots.client_tls {
            tracing::info!(
                client_tls_root = %store.base_dir().display(),
                "client TLS root configured (material keyed by NVLink domain)"
            );
        }
        if let Some(domain) = &self.switch_tls_roots.default_domain {
            tracing::info!(
                default_switch_domain = %domain,
                "default NVLink material domain configured"
            );
        }

        if let Some(domain) = self.switch_tls_roots.configured_dns_domain() {
            tracing::info!(dns_domain = %domain, "switch DNS domain configured");
        }

        tracing::info!(
            upload_timeout_secs = self.sftp_upload_options.overall_timeout.as_secs(),
            step_timeout_secs = self.sftp_upload_options.step_timeout.as_secs(),
            buffer_size_bytes = SFTP_UPLOAD_BUFFER_SIZE_BYTES,
            "SFTP upload options configured"
        );

        validate_firmware_dir_writable(&self.firmware_dir).await?;

        let rms_impl = RackManagerServiceImpl {
            rack_manager: self.rack_manager.clone(),
            job_tracker: self.job_tracker.clone(),
            firmware_download_cancellations: self.firmware_download_cancellations.clone(),
            backends: self.backends.clone(),
            firmware_dir: self.firmware_dir.clone(),
            switch_tls_roots: self.switch_tls_roots.clone(),
            sftp_upload_options: self.sftp_upload_options,
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        self.shutdown_tx = Some(shutdown_tx);

        let log_layer = crate::logging::api_logs::LogLayer::default();
        let metric_layer = crate::metrics::grpc_layer();

        let metrics_route = crate::metrics::prometheus_route(self.metrics_registry.clone());
        let routes = Routes::new(metric_layer.named_layer(RackManagerServer::new(rms_impl)))
            .into_axum_router()
            .route("/metrics", metrics_route);
        let router = builder.layer(log_layer).add_routes(Routes::from(routes));

        tokio::spawn(async move {
            if let Err(e) = router
                .serve_with_shutdown(addr, async {
                    let mut rx = shutdown_rx;
                    let _ = rx.changed().await;
                })
                .await
            {
                tracing::error!(error = %e, "gRPC server error");
            }
        });

        Ok(())
    }

    pub fn stop(&mut self) {
        // Cancel any in-flight jobs (firmware + switch-OS) cooperatively so
        // their long poll / steady-state waits exit promptly instead of
        // running until the tokio runtime is dropped under them.
        let cancelled = self.job_tracker.shutdown();
        if cancelled > 0 {
            tracing::info!(
                jobs_cancelled = cancelled,
                "gRPC server shutdown signalling in-flight jobs"
            );
        }
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            tracing::info!("gRPC server shutdown signal sent");
        }
    }
}

async fn validate_firmware_dir_writable(firmware_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(firmware_dir).await.map_err(|e| {
        RmsError::failed_precondition(format!(
            "failed to create firmware directory {}: {e}",
            firmware_dir.display()
        ))
    })?;

    let probe_path = firmware_dir.join(format!(
        ".rms-firmware-dir-write-check-{}.tmp",
        uuid::Uuid::new_v4()
    ));
    let mut probe = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe_path)
        .await
        .map_err(|e| {
            RmsError::failed_precondition(format!(
                "firmware directory {} is not writable: {e}",
                firmware_dir.display()
            ))
        })?;

    if let Err(e) = probe.write_all(b"ok").await {
        let _ = tokio::fs::remove_file(&probe_path).await;
        return Err(RmsError::failed_precondition(format!(
            "firmware directory {} is not writable: {e}",
            firmware_dir.display()
        )));
    }
    if let Err(e) = probe.flush().await {
        let _ = tokio::fs::remove_file(&probe_path).await;
        return Err(RmsError::failed_precondition(format!(
            "firmware directory {} is not writable: {e}",
            firmware_dir.display()
        )));
    }
    drop(probe);

    tokio::fs::remove_file(&probe_path).await.map_err(|e| {
        RmsError::failed_precondition(format!(
            "failed to remove firmware directory write-check file {}: {e}",
            probe_path.display()
        ))
    })?;

    Ok(())
}

// ── Helper: find rack + node ──

pub(crate) fn find_rack(
    rm: &RackManager,
    rack_id: &str,
) -> std::result::Result<Arc<ManagedRack>, rm::ReturnCode> {
    rm.find_rack(rack_id).ok_or(rm::ReturnCode::Failure)
}

pub(crate) fn find_node(
    rack: &ManagedRack,
    node_id: &str,
) -> std::result::Result<Arc<NodeInstance>, rm::ReturnCode> {
    rack.find_node(node_id).ok_or(rm::ReturnCode::Failure)
}

// ══════════════════════════════════════════════════════════════════════
//  RackManager gRPC service
// ══════════════════════════════════════════════════════════════════════

#[derive(Clone)]
pub struct RackManagerServiceImpl {
    pub rack_manager: Arc<RackManager>,
    pub job_tracker: Arc<JobTracker>,
    pub firmware_download_cancellations: Arc<Mutex<HashMap<String, CancellationToken>>>,
    pub backends: Backends,
    pub firmware_dir: PathBuf,
    pub switch_tls_roots: SwitchTlsRoots,
    pub sftp_upload_options: SftpUploadOptions,
}

fn can_keep_current_nvue_transport(strict: bool, has_active_tls: bool) -> bool {
    !strict && has_active_tls
}

impl RackManagerServiceImpl {
    pub(crate) async fn initialize_nvue_client(
        &self,
        client: Option<&nvue_client::SharedClient>,
        request_domain: Option<&str>,
    ) -> Result<()> {
        self.initialize_nvue_client_with_options(client, request_domain, None, false)
            .await
    }

    pub(crate) async fn initialize_nvue_client_with_options(
        &self,
        client: Option<&nvue_client::SharedClient>,
        request_domain: Option<&str>,
        endpoint_server_name: Option<&str>,
        require_configured_tls: bool,
    ) -> Result<()> {
        let Some(client) = client else {
            return Ok(());
        };

        if self.switch_tls_roots.insecure_switch {
            client.configure_insecure_https().await?;

            return Ok(());
        }

        let endpoint = client.endpoint();
        let endpoint_server_name = endpoint_server_name.unwrap_or(&endpoint.host);
        let server_name = self
            .switch_tls_roots
            .tls_server_name_for_endpoint(endpoint_server_name, request_domain)
            .map_err(RmsError::failed_precondition)?;

        let strict = require_configured_tls || nonempty_trimmed(request_domain).is_some();
        let tls = self
            .switch_tls_roots
            .resolve_required_nvue_client_tls(request_domain)
            .map_err(RmsError::failed_precondition)?;

        let prepared = match client
            .prepare_client_tls_if_changed(tls, Some(&server_name))
            .await
        {
            Ok(Some(prepared)) => prepared,
            Ok(None) => return Ok(()),
            Err(error) if can_keep_current_nvue_transport(strict, client.uses_client_tls()) => {
                tracing::debug!(%error, "keeping current NVUE transport during TLS material reload");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };

        if let Err(error) = client
            .activate_prepared_client_tls(
                &prepared,
                "/nvue_v1/system",
                nvue_client::DEFAULT_TIMEOUT,
            )
            .await
        {
            if !can_keep_current_nvue_transport(strict, client.uses_client_tls()) {
                return Err(error.into());
            }

            tracing::debug!(
                %error,
                "keeping current NVUE transport because configured client TLS is not ready"
            );
        }

        Ok(())
    }

    pub(crate) async fn ensure_nvue_client_for_certificate(
        &self,
        client: Option<&nvue_client::SharedClient>,
        request_domain: Option<&str>,
        endpoint_server_name: Option<&str>,
    ) -> Result<()> {
        let Some(client) = client else {
            return Ok(());
        };

        if client
            .get_json("/nvue_v1/system", nvue_client::DEFAULT_TIMEOUT)
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.initialize_nvue_client_with_options(
            Some(client),
            request_domain,
            endpoint_server_name,
            true,
        )
        .await?;

        client
            .get_json("/nvue_v1/system", nvue_client::DEFAULT_TIMEOUT)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }
}

fn job_status_from_info(info: JobInfo, parent_job_id: Option<String>) -> rm::JobStatus {
    rm::JobStatus {
        job_id: info.job_id,
        parent_job_id,
        child_job_ids: info.child_job_ids,
        execution_state: rm::JobExecutionState::from(info.state).into(),
        error_message: info.error_message,
        error_code: rm::JobError::from(info.error_code).into(),
        result_json: info.result_json,
        state_description: info.state_description,
        rack_id: Some(info.rack_id),
        node_id: Some(info.node_id),
        created_at: Some(timestamp_from_datetime(info.created_at)),
        updated_at: Some(timestamp_from_datetime(info.updated_at)),
    }
}

fn get_job_status_from_tracker(
    tracker: &JobTracker,
    request: rm::GetJobStatusRequest,
) -> std::result::Result<rm::GetJobStatusResponse, tonic::Status> {
    if request.job_id.trim().is_empty() {
        return Err(tonic::Status::invalid_argument("job_id is required"));
    }

    let Some(parent) = tracker.get_job(&request.job_id) else {
        return Err(tonic::Status::not_found(format!(
            "job {} not found",
            request.job_id
        )));
    };

    let parent_job_id = parent.job_id.clone();
    let child_job_ids = parent.child_job_ids.clone();
    let mut job_states = vec![job_status_from_info(parent, None)];

    if request.include_child_job_states {
        for child_job_id in child_job_ids {
            let Some(child) = tracker.get_job(&child_job_id) else {
                tracing::warn!(
                    job_id = %parent_job_id,
                    child_job_id,
                    "child job not found while building GetJobStatus response"
                );
                continue;
            };

            job_states.push(job_status_from_info(child, Some(parent_job_id.clone())));
        }
    }

    Ok(rm::GetJobStatusResponse { job_states })
}

#[tonic::async_trait]
impl RackManagerTrait for RackManagerServiceImpl {
    // ── Power ──

    async fn set_power_state(
        &self,
        req: tonic::Request<rm::SetPowerStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::SetPowerStateResponse>, tonic::Status> {
        self.handle_set_power_state(req).await
    }

    async fn batch_set_power_state(
        &self,
        req: tonic::Request<rm::BatchSetPowerStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchSetPowerStateResponse>, tonic::Status> {
        self.handle_batch_set_power_state(req).await
    }

    async fn batch_get_power_state(
        &self,
        req: tonic::Request<rm::BatchGetPowerStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchGetPowerStateResponse>, tonic::Status> {
        self.handle_batch_get_power_state(req).await
    }

    async fn get_power_state(
        &self,
        req: tonic::Request<rm::GetPowerStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetPowerStateResponse>, tonic::Status> {
        self.handle_get_power_state(req).await
    }

    async fn sequence_rack_power(
        &self,
        req: tonic::Request<rm::SequenceRackPowerRequest>,
    ) -> std::result::Result<tonic::Response<rm::SequenceRackPowerResponse>, tonic::Status> {
        self.handle_sequence_rack_power(req).await
    }

    // ── Inventory ──

    async fn list_node_inventory(
        &self,
        req: tonic::Request<rm::ListNodeInventoryRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListNodeInventoryResponse>, tonic::Status> {
        self.handle_list_node_inventory(req).await
    }

    async fn create_nodes(
        &self,
        req: tonic::Request<rm::CreateNodesRequest>,
    ) -> std::result::Result<tonic::Response<rm::CreateNodesResponse>, tonic::Status> {
        self.handle_create_nodes(req).await
    }

    async fn update_node(
        &self,
        req: tonic::Request<rm::UpdateNodeRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateNodeResponse>, tonic::Status> {
        self.handle_update_node(req).await
    }

    async fn delete_node(
        &self,
        req: tonic::Request<rm::DeleteNodeRequest>,
    ) -> std::result::Result<tonic::Response<rm::DeleteNodeResponse>, tonic::Status> {
        self.handle_delete_node(req).await
    }

    async fn get_rack_power_on_sequence(
        &self,
        req: tonic::Request<rm::GetRackPowerOnSequenceRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetRackPowerOnSequenceResponse>, tonic::Status>
    {
        self.handle_get_rack_power_on_sequence(req).await
    }

    async fn set_rack_power_on_sequence(
        &self,
        req: tonic::Request<rm::SetRackPowerOnSequenceRequest>,
    ) -> std::result::Result<tonic::Response<rm::SetRackPowerOnSequenceResponse>, tonic::Status>
    {
        self.handle_set_rack_power_on_sequence(req).await
    }

    async fn list_racks(
        &self,
        req: tonic::Request<rm::ListRacksRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListRacksResponse>, tonic::Status> {
        self.handle_list_racks(req).await
    }

    // ── Device info (placeholder) ──

    async fn get_node_device_info(
        &self,
        req: tonic::Request<rm::GetNodeDeviceInfoRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetNodeDeviceInfoResponse>, tonic::Status> {
        self.handle_get_node_device_info(req).await
    }

    async fn list_node_device_info_by_node_type(
        &self,
        req: tonic::Request<rm::ListNodeDeviceInfoByNodeTypeRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListNodeDeviceInfoByNodeTypeResponse>, tonic::Status>
    {
        self.handle_list_node_device_info_by_node_type(req).await
    }

    async fn batch_get_node_device_info(
        &self,
        req: tonic::Request<rm::BatchGetNodeDeviceInfoRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchGetNodeDeviceInfoResponse>, tonic::Status>
    {
        self.handle_batch_get_node_device_info(req).await
    }

    // ── Firmware ──

    async fn get_node_firmware_inventory(
        &self,
        req: tonic::Request<rm::GetNodeFirmwareInventoryRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetNodeFirmwareInventoryResponse>, tonic::Status>
    {
        self.handle_get_node_firmware_inventory(req).await
    }

    async fn update_firmware(
        &self,
        req: tonic::Request<rm::UpdateFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateFirmwareResponse>, tonic::Status> {
        self.handle_update_firmware(req).await
    }

    async fn batch_update_firmware_by_node_type(
        &self,
        req: tonic::Request<rm::BatchUpdateFirmwareByNodeTypeRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::BatchUpdateFirmwareByNodeTypeResponse>,
        tonic::Status,
    > {
        self.handle_batch_update_firmware_by_node_type(req).await
    }

    async fn batch_update_firmware(
        &self,
        req: tonic::Request<rm::BatchUpdateFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchUpdateFirmwareResponse>, tonic::Status> {
        self.handle_batch_update_firmware(req).await
    }

    async fn update_switch_system_image(
        &self,
        req: tonic::Request<rm::UpdateSwitchSystemImageRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateSwitchSystemImageResponse>, tonic::Status>
    {
        self.handle_update_switch_system_image(req).await
    }

    async fn get_switch_system_image_job_status(
        &self,
        req: tonic::Request<rm::GetSwitchSystemImageJobStatusRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::GetSwitchSystemImageJobStatusResponse>,
        tonic::Status,
    > {
        self.handle_get_switch_system_image_job_status(req).await
    }

    async fn update_switch_system_password(
        &self,
        req: tonic::Request<rm::UpdateSwitchSystemPasswordRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateSwitchSystemPasswordResponse>, tonic::Status>
    {
        self.handle_update_switch_system_password(req).await
    }

    async fn get_rack_firmware_inventory(
        &self,
        req: tonic::Request<rm::GetRackFirmwareInventoryRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetRackFirmwareInventoryResponse>, tonic::Status>
    {
        self.handle_get_rack_firmware_inventory(req).await
    }

    async fn add_firmware_object(
        &self,
        req: tonic::Request<rm::AddFirmwareObjectRequest>,
    ) -> std::result::Result<tonic::Response<rm::AddFirmwareObjectResponse>, tonic::Status> {
        self.handle_add_firmware_object(req).await
    }

    async fn get_firmware_object(
        &self,
        req: tonic::Request<rm::GetFirmwareObjectRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetFirmwareObjectResponse>, tonic::Status> {
        self.handle_get_firmware_object(req).await
    }

    async fn list_firmware_objects(
        &self,
        req: tonic::Request<rm::ListFirmwareObjectsRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListFirmwareObjectsResponse>, tonic::Status> {
        self.handle_list_firmware_objects(req).await
    }

    async fn delete_firmware_object(
        &self,
        req: tonic::Request<rm::DeleteFirmwareObjectRequest>,
    ) -> std::result::Result<tonic::Response<rm::DeleteFirmwareObjectResponse>, tonic::Status> {
        self.handle_delete_firmware_object(req).await
    }

    async fn set_default_firmware_object(
        &self,
        req: tonic::Request<rm::SetDefaultFirmwareObjectRequest>,
    ) -> std::result::Result<tonic::Response<rm::SetDefaultFirmwareObjectResponse>, tonic::Status>
    {
        self.handle_set_default_firmware_object(req).await
    }

    async fn apply_stored_firmware_object(
        &self,
        req: tonic::Request<rm::ApplyStoredFirmwareObjectRequest>,
    ) -> std::result::Result<tonic::Response<rm::ApplyStoredFirmwareObjectResponse>, tonic::Status>
    {
        self.handle_apply_stored_firmware_object(req).await
    }

    async fn apply_firmware_object(
        &self,
        req: tonic::Request<rm::ApplyFirmwareObjectRequest>,
    ) -> std::result::Result<tonic::Response<rm::ApplyFirmwareObjectResponse>, tonic::Status> {
        self.handle_apply_firmware_object(req).await
    }

    async fn apply_stored_switch_system_image(
        &self,
        req: tonic::Request<rm::ApplyStoredSwitchSystemImageRequest>,
    ) -> std::result::Result<tonic::Response<rm::ApplyStoredSwitchSystemImageResponse>, tonic::Status>
    {
        self.handle_apply_stored_switch_system_image(req).await
    }

    async fn apply_switch_system_image(
        &self,
        req: tonic::Request<rm::ApplySwitchSystemImageRequest>,
    ) -> std::result::Result<tonic::Response<rm::ApplySwitchSystemImageResponse>, tonic::Status>
    {
        self.handle_apply_switch_system_image(req).await
    }

    async fn get_firmware_object_history(
        &self,
        req: tonic::Request<rm::GetFirmwareObjectHistoryRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetFirmwareObjectHistoryResponse>, tonic::Status>
    {
        self.handle_get_firmware_object_history(req).await
    }

    async fn get_firmware_job_status(
        &self,
        req: tonic::Request<rm::GetFirmwareJobStatusRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetFirmwareJobStatusResponse>, tonic::Status> {
        self.handle_get_firmware_job_status(req).await
    }

    async fn get_job_status(
        &self,
        req: tonic::Request<rm::GetJobStatusRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetJobStatusResponse>, tonic::Status> {
        get_job_status_from_tracker(&self.job_tracker, req.into_inner()).map(tonic::Response::new)
    }

    // ── Switch firmware ──

    async fn list_switch_firmware(
        &self,
        req: tonic::Request<rm::ListSwitchFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListSwitchFirmwareResponse>, tonic::Status> {
        self.handle_list_switch_firmware(req).await
    }

    async fn push_switch_firmware(
        &self,
        req: tonic::Request<rm::PushSwitchFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::PushSwitchFirmwareResponse>, tonic::Status> {
        self.handle_push_switch_firmware(req).await
    }

    async fn upgrade_switch_firmware(
        &self,
        req: tonic::Request<rm::UpgradeSwitchFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpgradeSwitchFirmwareResponse>, tonic::Status>
    {
        self.handle_upgrade_switch_firmware(req).await
    }

    async fn list_switch_system_images(
        &self,
        req: tonic::Request<rm::ListSwitchSystemImagesRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListSwitchSystemImagesResponse>, tonic::Status>
    {
        self.handle_list_switch_system_images(req).await
    }

    // ── Switch fabric ──

    async fn configure_scale_up_fabric_manager(
        &self,
        req: tonic::Request<rm::ConfigureScaleUpFabricManagerRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::ConfigureScaleUpFabricManagerResponse>,
        tonic::Status,
    > {
        self.handle_configure_scale_up_fabric_manager(req).await
    }

    async fn batch_set_scale_up_fabric_state(
        &self,
        req: tonic::Request<rm::BatchSetScaleUpFabricStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchSetScaleUpFabricStateResponse>, tonic::Status>
    {
        self.handle_batch_set_scale_up_fabric_state(req).await
    }

    async fn get_scale_up_fabric_state(
        &self,
        req: tonic::Request<rm::GetScaleUpFabricStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetScaleUpFabricStateResponse>, tonic::Status>
    {
        self.handle_get_scale_up_fabric_state(req).await
    }

    async fn batch_get_scale_up_fabric_service_status(
        &self,
        req: tonic::Request<rm::BatchGetScaleUpFabricServiceStatusRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::BatchGetScaleUpFabricServiceStatusResponse>,
        tonic::Status,
    > {
        self.handle_batch_get_scale_up_fabric_service_status(req)
            .await
    }

    async fn set_scale_up_fabric_telemetry_interface_state(
        &self,
        req: tonic::Request<rm::SetScaleUpFabricTelemetryInterfaceStateRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::SetScaleUpFabricTelemetryInterfaceStateResponse>,
        tonic::Status,
    > {
        self.handle_set_scale_up_fabric_telemetry_interface_state(req)
            .await
    }

    async fn configure_switch_certificate(
        &self,
        req: tonic::Request<rm::ConfigureSwitchCertificateRequest>,
    ) -> std::result::Result<tonic::Response<rm::ConfigureSwitchCertificateResponse>, tonic::Status>
    {
        self.handle_configure_switch_certificate(req).await
    }

    async fn get_configure_switch_certificate_job_status(
        &self,
        req: tonic::Request<rm::GetConfigureSwitchCertificateJobStatusRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::GetConfigureSwitchCertificateJobStatusResponse>,
        tonic::Status,
    > {
        self.handle_get_configure_switch_certificate_job_status(req)
            .await
    }

    // ── Utility ──

    async fn get_version(
        &self,
        _req: tonic::Request<rm::GetVersionRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetVersionResponse>, tonic::Status> {
        Ok(tonic::Response::new(rm::GetVersionResponse {
            version: SERVER_VERSION.to_owned(),
        }))
    }

    async fn poll_switch_firmware_job_status(
        &self,
        req: tonic::Request<rm::PollSwitchFirmwareJobStatusRequest>,
    ) -> std::result::Result<tonic::Response<rm::PollSwitchFirmwareJobStatusResponse>, tonic::Status>
    {
        self.handle_poll_switch_firmware_job_status(req).await
    }
}
