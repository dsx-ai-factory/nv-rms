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

//! Runtime configuration for RMS.
//!
//! RMS reads all of its runtime configuration from a single TOML file (default
//! `/etc/rms/config.toml`, overridable with `--config`). There are no per-value
//! CLI flags or environment variables. Two narrowly-scoped environment inputs
//! are the only exceptions:
//!
//! * `DATABASE_URL` -- when set, overrides the `postgres.db_url` field so
//!   Kubernetes can inject the connection string (with its password) from a
//!   Secret rather than baking it into a ConfigMap.
//! * `RMS_ALLOW_INSECURE` -- must be set to `1`, in addition to
//!   `tls.insecure = true` in this file, before RMS will bind plaintext gRPC.
//!   Because it is sourced from a different channel than the config file, it
//!   is an independent production safeguard so a stale `tls.insecure = true`
//!   cannot by itself downgrade the API to unauthenticated plaintext. See
//!   [`crate::api::grpc::server::RMS_ALLOW_INSECURE_ENV`].
//!
//! Beyond the top-level `port` field, configuration is grouped into TOML
//! sections, each backed by its own struct:
//!
//! | Section       | Struct              |
//! | ------------- | ------------------- |
//! | `[metrics]`   | [`RmsMetricsConfig`]  |
//! | `[tls]`       | [`RmsTlsConfig`]      |
//! | `[switches]`  | [`RmsSwitchConfig`]   |
//! | `[postgres]`  | [`RmsPostgresConfig`] |
//! | `[logging]`   | [`RmsLoggingConfig`]  |
//! | `[workflows]` | [`RmsWorkflowConfig`] |
//!
//! The loader is figment-based: the file is read once into a `Toml::string`
//! provider merged with a narrowly scoped `Env` provider, then extracted into
//! [`RmsConfig`]. Every field carries a serde default, so an empty or partial
//! file (including one that omits a section entirely) yields a
//! fully-populated config.

use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::domain::node::ExpectedInventoryPolicy;
use crate::transport::ssh::SftpUploadOptions;

/// Default path RMS reads its configuration from when `--config` is omitted.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/rms/config.toml";

/// Environment variable that, when set, overrides the `postgres.db_url`
/// field. It lets Kubernetes source the Postgres connection string (which
/// embeds a password) from a Secret while the rest of the configuration ships
/// in a ConfigMap. It is the only value-overriding environment input; the
/// sole other sanctioned environment input is the `RMS_ALLOW_INSECURE`
/// plaintext gate (see [`crate::api::grpc::server::RMS_ALLOW_INSECURE_ENV`]),
/// which gates behavior rather than overriding a field.
pub const DATABASE_URL_ENV: &str = "DATABASE_URL";

/// The dotted figment key that [`DATABASE_URL_ENV`] is merged into.
const DATABASE_URL_TARGET_KEY: &str = "postgres.db_url";

fn default_port() -> u16 {
    8801
}

fn default_metrics_port() -> u16 {
    8802
}

fn default_db_pool_max() -> NonZeroUsize {
    NonZeroUsize::new(20).expect("20 is non-zero")
}

fn default_firmware_dir() -> PathBuf {
    PathBuf::from("firmware")
}

fn default_nmx_gateway_id() -> String {
    crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned()
}

fn default_sftp_upload_timeout_seconds() -> u64 {
    SftpUploadOptions::default().overall_timeout.as_secs()
}

fn default_sftp_step_timeout_seconds() -> u64 {
    SftpUploadOptions::default().step_timeout.as_secs()
}

fn default_max_tracked_jobs() -> NonZeroUsize {
    NonZeroUsize::new(10_000).expect("10_000 is non-zero")
}

fn default_terminal_job_ttl_seconds() -> NonZeroU64 {
    NonZeroU64::new(86_400).expect("86_400 is non-zero")
}

fn default_enable_timestamps() -> bool {
    false
}

/// Validated expected-inventory profiles loaded from `[workflows]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ExpectedInventoryProfiles(BTreeMap<String, Vec<String>>);

impl ExpectedInventoryProfiles {
    /// Return the validated configured profiles.
    pub fn as_map(&self) -> &BTreeMap<String, Vec<String>> {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ExpectedInventoryProfiles {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error as _;

        let raw = BTreeMap::<String, Vec<String>>::deserialize(deserializer)?;
        let mut profiles = BTreeMap::new();

        for (raw_name, raw_ap_names) in raw {
            let profile = raw_name.trim();
            if profile.is_empty() {
                return Err(D::Error::custom(
                    "expected inventory profile names must not be empty",
                ));
            }
            if raw_ap_names.iter().any(|ap_name| ap_name.trim().is_empty()) {
                return Err(D::Error::custom(format!(
                    "expected inventory profile {profile:?} contains an empty AP name"
                )));
            }

            let value = serde_json::to_value(raw_ap_names).map_err(D::Error::custom)?;
            let ap_names = nvfwupd::expected_inventory::parse_expected_inventory_value(&value)
                .map_err(|message| {
                    D::Error::custom(format!(
                        "invalid expected inventory profile {profile:?}: {message}"
                    ))
                })?;

            if profiles.insert(profile.to_owned(), ap_names).is_some() {
                return Err(D::Error::custom(format!(
                    "duplicate expected inventory profile after trimming: {profile:?}"
                )));
            }
        }

        Ok(Self(profiles))
    }
}

/// Immutable startup catalog used to resolve node descriptor profile names.
#[derive(Clone, Debug, Default)]
pub struct ExpectedInventoryCatalog {
    profiles: Arc<BTreeMap<String, Arc<[String]>>>,
}

impl From<&ExpectedInventoryProfiles> for ExpectedInventoryCatalog {
    fn from(config: &ExpectedInventoryProfiles) -> Self {
        let profiles = config
            .as_map()
            .iter()
            .map(|(profile, ap_names)| (profile.clone(), Arc::<[String]>::from(ap_names.clone())))
            .collect();
        Self {
            profiles: Arc::new(profiles),
        }
    }
}

impl ExpectedInventoryCatalog {
    /// Resolve an optional descriptor value using exact, case-sensitive
    /// matching after trimming.
    pub fn resolve(
        &self,
        raw_profile: Option<&str>,
    ) -> Result<Option<ExpectedInventoryPolicy>, ExpectedInventoryProfileError> {
        let Some(raw_profile) = raw_profile else {
            return Ok(None);
        };
        let profile = raw_profile.trim();
        if profile.is_empty() {
            return Err(ExpectedInventoryProfileError::Empty);
        }
        let ap_names = self
            .profiles
            .get(profile)
            .cloned()
            .ok_or_else(|| ExpectedInventoryProfileError::Unknown(profile.to_owned()))?;

        Ok(Some(ExpectedInventoryPolicy {
            profile: Arc::from(profile),
            ap_names,
        }))
    }
}

/// Failure to resolve a supplied expected-inventory profile.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ExpectedInventoryProfileError {
    #[error("inventory_profile must not be empty")]
    Empty,
    #[error("unknown inventory_profile {0:?}")]
    Unknown(String),
}

/// `[metrics]` section: the Prometheus `/metrics` HTTP listener.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RmsMetricsConfig {
    /// Metrics server port for the Prometheus `/metrics` endpoint.
    #[serde(default = "default_metrics_port")]
    pub port: u16,

    /// Serve `/metrics` over TLS using the gRPC server certificate (no client
    /// auth).
    #[serde(default)]
    pub tls: bool,
}

impl Default for RmsMetricsConfig {
    fn default() -> Self {
        Self {
            port: default_metrics_port(),
            tls: false,
        }
    }
}

/// `[tls]` section: the gRPC API listener's TLS/mTLS material and the
/// plaintext opt-out.
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RmsTlsConfig {
    /// Path to the X.509 certificate file for gRPC TLS.
    #[serde(default)]
    pub cert: Option<String>,

    /// Path to the private key file for gRPC TLS.
    #[serde(default)]
    pub key: Option<String>,

    /// Path to the CA certificate for client verification / mTLS.
    #[serde(default)]
    pub ca: Option<String>,

    /// Disable gRPC TLS (plaintext, UNAUTHENTICATED). Development/testing only;
    /// production configuration MUST leave this `false`. Even when `true`, RMS
    /// binds plaintext only if the `RMS_ALLOW_INSECURE=1` environment gate is
    /// also present -- an independent safeguard sourced outside this file.
    #[serde(default)]
    pub insecure: bool,
}

/// `[switches]` section: outbound RMS-to-switch TLS material and identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RmsSwitchConfig {
    /// Root directory of switch-side certificate material to install.
    #[serde(default)]
    pub switch_cert_root: Option<PathBuf>,

    /// Root directory of RMS client mTLS material for outbound switch
    /// connections.
    #[serde(default)]
    pub client_tls_root: Option<PathBuf>,

    /// Default NVLink domain for switch TLS material lookup when an RPC omits
    /// `domain`.
    #[serde(default)]
    pub default_switch_domain: Option<String>,

    /// Optional DNS domain override used as the TLS server name (SNI) authority.
    #[serde(default)]
    pub dns_domain: Option<String>,

    /// Disable outbound switch client mTLS.
    #[serde(default)]
    pub insecure_switch: bool,

    /// `gateway_id` sent on NMX-C gRPC requests from RMS.
    #[serde(default = "default_nmx_gateway_id")]
    pub nmx_gateway_id: String,
}

impl Default for RmsSwitchConfig {
    fn default() -> Self {
        Self {
            switch_cert_root: None,
            client_tls_root: None,
            default_switch_domain: None,
            dns_domain: None,
            insecure_switch: false,
            nmx_gateway_id: default_nmx_gateway_id(),
        }
    }
}

/// `[postgres]` section: the persistence-layer database connection.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RmsPostgresConfig {
    /// Postgres connection URL for the persistence layer. If unset the service
    /// falls back to an in-memory store.
    ///
    /// A `DATABASE_URL` environment variable, when present, overrides whatever
    /// is set here (see [`DATABASE_URL_ENV`]).
    #[serde(default)]
    pub db_url: Option<String>,

    /// Maximum size of the Postgres connection pool. Must be greater than zero.
    #[serde(default = "default_db_pool_max")]
    pub db_pool_max: NonZeroUsize,
}

impl Default for RmsPostgresConfig {
    fn default() -> Self {
        Self {
            db_url: None,
            db_pool_max: default_db_pool_max(),
        }
    }
}

/// `[logging]` section: tracing output configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RmsLoggingConfig {
    /// Optional log level / filter directive (e.g. `info` or `info,carbide=debug`).
    /// When unset the compiled-in default (`info`) plus dependency caps apply.
    #[serde(default)]
    pub log_level: Option<String>,

    /// When true, event log lines include a native `time=<rfc3339>` field.
    /// Defaults to false: assuming a logging collector is adding its own
    /// timestamps, so this is disabled by default to prevent duplicate
    /// timestamp fields. If no logging collector is being used, set this to
    /// true to output timestamps natively.
    #[serde(default = "default_enable_timestamps")]
    pub enable_timestamps: bool,
}

impl Default for RmsLoggingConfig {
    fn default() -> Self {
        Self {
            log_level: None,
            enable_timestamps: default_enable_timestamps(),
        }
    }
}

/// `[workflows]` section: firmware staging and long-running job tracking.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RmsWorkflowConfig {
    /// Directory where RMS stores and reads firmware artifacts.
    #[serde(default = "default_firmware_dir")]
    pub firmware_dir: PathBuf,

    /// Overall timeout (seconds) for switch NVOS SFTP image uploads.
    #[serde(default = "default_sftp_upload_timeout_seconds")]
    pub sftp_upload_timeout_seconds: u64,

    /// Stall timeout (seconds) for one switch NVOS SFTP upload step.
    #[serde(default = "default_sftp_step_timeout_seconds")]
    pub sftp_step_timeout_seconds: u64,

    /// Maximum number of async job records the tracker retains.
    #[serde(default = "default_max_tracked_jobs")]
    pub max_tracked_jobs: NonZeroUsize,

    /// Retention period (seconds) for terminal job records before eviction.
    #[serde(default = "default_terminal_job_ttl_seconds")]
    pub terminal_job_ttl_seconds: NonZeroU64,

    /// Deployment-defined AP inventories selected by the
    /// `inventory_profile` node descriptor attribute.
    #[serde(default)]
    pub expected_inventory_profiles: ExpectedInventoryProfiles,
}

impl Default for RmsWorkflowConfig {
    fn default() -> Self {
        Self {
            firmware_dir: default_firmware_dir(),
            sftp_upload_timeout_seconds: default_sftp_upload_timeout_seconds(),
            sftp_step_timeout_seconds: default_sftp_step_timeout_seconds(),
            max_tracked_jobs: default_max_tracked_jobs(),
            terminal_job_ttl_seconds: default_terminal_job_ttl_seconds(),
            expected_inventory_profiles: ExpectedInventoryProfiles::default(),
        }
    }
}

/// Fully-parsed RMS runtime configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RmsConfig {
    /// gRPC server port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// `[metrics]` section: the Prometheus `/metrics` HTTP listener.
    #[serde(default)]
    pub metrics: RmsMetricsConfig,

    /// `[tls]` section: the gRPC API listener's TLS/mTLS material and the
    /// plaintext opt-out.
    #[serde(default)]
    pub tls: RmsTlsConfig,

    /// `[switches]` section: outbound RMS-to-switch TLS material and identity.
    #[serde(default)]
    pub switches: RmsSwitchConfig,

    /// `[postgres]` section: the persistence-layer database connection.
    #[serde(default)]
    pub postgres: RmsPostgresConfig,

    /// `[logging]` section: tracing output configuration.
    #[serde(default)]
    pub logging: RmsLoggingConfig,

    /// `[workflows]` section: firmware staging and long-running job tracking.
    #[serde(default)]
    pub workflows: RmsWorkflowConfig,
}

impl Default for RmsConfig {
    fn default() -> Self {
        // Route through serde so the single source of truth for defaults is the
        // `#[serde(default = ...)]` attributes above.
        Figment::new()
            .merge(Toml::string(""))
            .extract()
            .expect("empty TOML must extract into all-default RmsConfig")
    }
}

impl RmsConfig {
    /// Load configuration from the TOML file at `path`, then apply the
    /// `DATABASE_URL` environment override (if present).
    ///
    /// Returns a human-readable error string suitable for logging to stderr on
    /// startup before the tracing subscriber is installed.
    pub fn load(path: &Path) -> Result<Self, String> {
        // Read the file in a single syscall rather than checking for existence
        // and then letting figment open it separately. `Toml::file` silently
        // treats a missing file as an empty provider (yielding an all-default
        // config), and any stat-then-open sequence has a TOCTOU window in which
        // the file can be deleted or atomically swapped (e.g. a ConfigMap
        // remount) between the check and the read. Reading exactly once closes
        // that window and lets us classify the failure precisely: a genuinely
        // absent file versus an inaccessible one (e.g. a mis-scoped mount).
        let contents = std::fs::read_to_string(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                format!("configuration file not found: {}", path.display())
            }
            _ => format!("cannot access configuration file {}: {e}", path.display()),
        })?;

        Self::figment(&contents)
            .extract()
            .map_err(|e| format!("failed to load configuration from {}: {e}", path.display()))
    }

    /// Build the figment used by [`Self::load`] from already-read TOML
    /// `contents`. Split out so tests can inspect the provider chain, and so
    /// the file is read from disk exactly once by [`Self::load`] (avoiding a
    /// TOCTOU between an existence check and figment's own file read).
    fn figment(contents: &str) -> Figment {
        Figment::new().merge(Toml::string(contents)).merge(
            Env::raw()
                .only(&[DATABASE_URL_ENV])
                .map(|_| DATABASE_URL_TARGET_KEY.into()),
        )
    }
}

#[cfg(test)]
// The `figment::Jail::expect_with` closures return `Result<(), figment::Error>`
// (a large error type) as dictated by figment's testing API, so the
// `result_large_err` lint is unavoidable here.
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;
    use figment::Jail;

    #[test]
    fn empty_file_yields_historical_defaults() {
        Jail::expect_with(|jail| {
            // Isolate from any ambient `DATABASE_URL` (common in CI), which the
            // loader would otherwise merge into `postgres.db_url`.
            jail.clear_env();
            jail.create_file("config.toml", "")?;
            let config = RmsConfig::load(Path::new("config.toml")).expect("empty config loads");

            assert_eq!(config.port, 8801);
            assert_eq!(config.metrics.port, 8802);
            assert!(!config.metrics.tls);
            assert!(config.tls.cert.is_none());
            assert!(config.tls.key.is_none());
            assert!(config.tls.ca.is_none());
            assert!(!config.tls.insecure);
            assert!(config.switches.switch_cert_root.is_none());
            assert!(config.switches.client_tls_root.is_none());
            assert!(config.switches.default_switch_domain.is_none());
            assert!(config.switches.dns_domain.is_none());
            assert!(!config.switches.insecure_switch);
            assert_eq!(
                config.switches.nmx_gateway_id,
                crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID
            );
            assert!(config.postgres.db_url.is_none());
            assert_eq!(config.postgres.db_pool_max, NonZeroUsize::new(20).unwrap());
            assert_eq!(config.workflows.firmware_dir, PathBuf::from("firmware"));

            let sftp_defaults = SftpUploadOptions::default();
            assert_eq!(
                config.workflows.sftp_upload_timeout_seconds,
                sftp_defaults.overall_timeout.as_secs()
            );
            assert_eq!(
                config.workflows.sftp_step_timeout_seconds,
                sftp_defaults.step_timeout.as_secs()
            );
            assert_eq!(
                config.workflows.max_tracked_jobs,
                NonZeroUsize::new(10_000).unwrap()
            );
            assert_eq!(
                config.workflows.terminal_job_ttl_seconds,
                NonZeroU64::new(86_400).unwrap()
            );
            assert!(
                config
                    .workflows
                    .expected_inventory_profiles
                    .as_map()
                    .is_empty()
            );
            assert!(config.logging.log_level.is_none());
            assert!(!config.logging.enable_timestamps);
            Ok(())
        });
    }

    #[test]
    fn defaults_match_serde_defaults() {
        // `RmsConfig::default()` routes through the empty-TOML path; verify it
        // agrees with an explicitly loaded empty file.
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file("config.toml", "")?;
            let loaded = RmsConfig::load(Path::new("config.toml")).unwrap();
            let defaulted = RmsConfig::default();
            assert_eq!(loaded.port, defaulted.port);
            assert_eq!(defaulted.port, 8801);
            assert_eq!(
                defaulted.postgres.db_pool_max,
                NonZeroUsize::new(20).unwrap()
            );
            assert!(!defaulted.logging.enable_timestamps);
            Ok(())
        });
    }

    #[test]
    fn full_config_parses_all_fields() {
        Jail::expect_with(|jail| {
            // Isolate from any ambient `DATABASE_URL` (common in CI) so the
            // file's `postgres.db_url` is the only source.
            jail.clear_env();
            jail.create_file(
                "config.toml",
                r#"
                port = 9001

                [metrics]
                port = 9002
                tls = true

                [tls]
                cert = "/certs/tls.crt"
                key = "/certs/tls.key"
                ca = "/certs/ca.crt"
                insecure = true

                [switches]
                switch_cert_root = "/switch-cert"
                client_tls_root = "/client-tls"
                default_switch_domain = "site-wide"
                dns_domain = "switch.example.com"
                insecure_switch = true
                nmx_gateway_id = "custom-gateway"

                [postgres]
                db_url = "postgres://user:pw@host/db"
                db_pool_max = 42

                [workflows]
                firmware_dir = "/var/lib/rms/firmware"
                sftp_upload_timeout_seconds = 1200
                sftp_step_timeout_seconds = 15
                max_tracked_jobs = 500
                terminal_job_ttl_seconds = 600

                [workflows.expected_inventory_profiles]
                "gb200-compute-variant-a" = ["FW_BMC_0", "HGX_FW_GPU_0"]
                "gb200-compute-variant-b" = ["FW_BMC_0", "HGX_FW_GPU_0", "HGX_FW_GPU_1"]

                [logging]
                log_level = "info,carbide=debug"
                enable_timestamps = true
                "#,
            )?;
            let config = RmsConfig::load(Path::new("config.toml")).expect("full config loads");

            assert_eq!(config.port, 9001);
            assert_eq!(config.metrics.port, 9002);
            assert!(config.metrics.tls);
            assert_eq!(config.tls.cert.as_deref(), Some("/certs/tls.crt"));
            assert_eq!(config.tls.key.as_deref(), Some("/certs/tls.key"));
            assert_eq!(config.tls.ca.as_deref(), Some("/certs/ca.crt"));
            assert!(config.tls.insecure);
            assert_eq!(
                config.switches.switch_cert_root,
                Some(PathBuf::from("/switch-cert"))
            );
            assert_eq!(
                config.switches.client_tls_root,
                Some(PathBuf::from("/client-tls"))
            );
            assert_eq!(
                config.switches.default_switch_domain.as_deref(),
                Some("site-wide")
            );
            assert_eq!(
                config.switches.dns_domain.as_deref(),
                Some("switch.example.com")
            );
            assert!(config.switches.insecure_switch);
            assert_eq!(config.switches.nmx_gateway_id, "custom-gateway");
            assert_eq!(
                config.postgres.db_url.as_deref(),
                Some("postgres://user:pw@host/db")
            );
            assert_eq!(config.postgres.db_pool_max, NonZeroUsize::new(42).unwrap());
            assert_eq!(
                config.workflows.firmware_dir,
                PathBuf::from("/var/lib/rms/firmware")
            );
            assert_eq!(config.workflows.sftp_upload_timeout_seconds, 1200);
            assert_eq!(config.workflows.sftp_step_timeout_seconds, 15);
            assert_eq!(
                config.workflows.max_tracked_jobs,
                NonZeroUsize::new(500).unwrap()
            );
            assert_eq!(
                config.workflows.terminal_job_ttl_seconds,
                NonZeroU64::new(600).unwrap()
            );
            assert_eq!(
                config
                    .workflows
                    .expected_inventory_profiles
                    .as_map()
                    .get("gb200-compute-variant-a"),
                Some(&vec!["FW_BMC_0".to_owned(), "HGX_FW_GPU_0".to_owned()])
            );
            assert_eq!(
                config.workflows.expected_inventory_profiles.as_map().len(),
                2
            );
            assert_eq!(
                config.logging.log_level.as_deref(),
                Some("info,carbide=debug")
            );
            assert!(config.logging.enable_timestamps);
            Ok(())
        });
    }

    #[test]
    fn expected_inventory_profiles_trim_and_deduplicate_ap_names() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "config.toml",
                r#"
                [workflows.expected_inventory_profiles]
                " profile-a " = [" FW_BMC_0 ", "fw_bmc_0", "HGX_FW_GPU_0"]
                "#,
            )?;

            let config = RmsConfig::load(Path::new("config.toml")).unwrap();
            assert_eq!(
                config
                    .workflows
                    .expected_inventory_profiles
                    .as_map()
                    .get("profile-a"),
                Some(&vec!["FW_BMC_0".to_owned(), "HGX_FW_GPU_0".to_owned()])
            );
            Ok(())
        });
    }

    #[test]
    fn expected_inventory_profiles_reject_invalid_entries() {
        for (name, toml) in [
            (
                "blank profile",
                "[workflows.expected_inventory_profiles]\n\"   \" = [\"FW_BMC_0\"]\n",
            ),
            (
                "empty inventory",
                "[workflows.expected_inventory_profiles]\nprofile-a = []\n",
            ),
            (
                "empty AP name",
                "[workflows.expected_inventory_profiles]\nprofile-a = [\"FW_BMC_0\", \"  \"]\n",
            ),
            (
                "non-string AP name",
                "[workflows.expected_inventory_profiles]\nprofile-a = [\"FW_BMC_0\", 7]\n",
            ),
            (
                "duplicate trimmed profile",
                "[workflows.expected_inventory_profiles]\nprofile-a = [\"FW_BMC_0\"]\n\" profile-a \" = [\"HGX_FW_GPU_0\"]\n",
            ),
        ] {
            Jail::expect_with(|jail| {
                jail.clear_env();
                jail.create_file("config.toml", toml)?;
                let error = RmsConfig::load(Path::new("config.toml")).unwrap_err();
                assert!(
                    error.contains("expected inventory")
                        || error.contains("invalid type")
                        || error.contains("AP name"),
                    "{name}: {error}"
                );
                Ok(())
            });
        }
    }

    #[test]
    fn expected_inventory_catalog_matches_exactly_after_trimming() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "config.toml",
                r#"
                [workflows.expected_inventory_profiles]
                profile-a = ["FW_BMC_0"]
                "#,
            )?;
            let config = RmsConfig::load(Path::new("config.toml")).unwrap();
            let catalog =
                ExpectedInventoryCatalog::from(&config.workflows.expected_inventory_profiles);

            let policy = catalog.resolve(Some(" profile-a ")).unwrap().unwrap();
            assert_eq!(policy.profile.as_ref(), "profile-a");
            assert_eq!(policy.ap_names.as_ref(), ["FW_BMC_0"]);
            assert_eq!(catalog.resolve(None), Ok(None));
            assert_eq!(
                catalog.resolve(Some("")),
                Err(ExpectedInventoryProfileError::Empty)
            );
            assert_eq!(
                catalog.resolve(Some("PROFILE-A")),
                Err(ExpectedInventoryProfileError::Unknown(
                    "PROFILE-A".to_owned()
                ))
            );
            Ok(())
        });
    }

    #[test]
    fn enable_timestamps_defaults_to_false_when_omitted() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "config.toml",
                r#"
                [logging]
                log_level = "warn"
                "#,
            )?;
            let config = RmsConfig::load(Path::new("config.toml")).unwrap();
            assert_eq!(config.logging.log_level.as_deref(), Some("warn"));
            assert!(!config.logging.enable_timestamps);
            Ok(())
        });
    }

    #[test]
    fn enable_timestamps_can_be_set_false_explicitly() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file("config.toml", "[logging]\nenable_timestamps = false\n")?;
            let config = RmsConfig::load(Path::new("config.toml")).unwrap();
            assert!(!config.logging.enable_timestamps);
            Ok(())
        });
    }

    #[test]
    fn database_url_env_overrides_file() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "config.toml",
                "[postgres]\ndb_url = \"postgres://in-file/db\"\n",
            )?;
            jail.set_env(DATABASE_URL_ENV, "postgres://from-env/db");
            let config = RmsConfig::load(Path::new("config.toml")).unwrap();
            assert_eq!(
                config.postgres.db_url.as_deref(),
                Some("postgres://from-env/db")
            );
            Ok(())
        });
    }

    #[test]
    fn database_url_env_populates_when_file_omits_it() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file("config.toml", "")?;
            jail.set_env(DATABASE_URL_ENV, "postgres://from-env/db");
            let config = RmsConfig::load(Path::new("config.toml")).unwrap();
            assert_eq!(
                config.postgres.db_url.as_deref(),
                Some("postgres://from-env/db")
            );
            Ok(())
        });
    }

    #[test]
    fn missing_file_is_an_error() {
        Jail::expect_with(|_jail| {
            let err = RmsConfig::load(Path::new("does-not-exist.toml"))
                .expect_err("missing file must error");
            assert!(err.contains("configuration file not found"), "got: {err}");
            assert!(err.contains("does-not-exist.toml"), "got: {err}");
            Ok(())
        });
    }

    // A file the process cannot stat (e.g. a mis-scoped ConfigMap mount) must be
    // reported as an access error, not conflated with a genuinely absent file.
    #[cfg(unix)]
    #[test]
    fn inaccessible_file_reports_access_error() {
        use std::os::unix::fs::PermissionsExt;

        // `figment::Error` has no `From<io::Error>`, so adapt filesystem errors.
        fn io(e: std::io::Error) -> figment::Error {
            figment::Error::from(e.to_string())
        }

        Jail::expect_with(|_jail| {
            std::fs::create_dir("locked").map_err(io)?;
            std::fs::write("locked/config.toml", "").map_err(io)?;
            // Drop search permission on the parent so `metadata()` on the child
            // fails with EACCES.
            std::fs::set_permissions("locked", std::fs::Permissions::from_mode(0o000))
                .map_err(io)?;

            let result = RmsConfig::load(Path::new("locked/config.toml"));

            // Restore permissions so the jail temp dir can be cleaned up.
            std::fs::set_permissions("locked", std::fs::Permissions::from_mode(0o755))
                .map_err(io)?;

            // Running as root bypasses directory permission bits, in which case
            // `try_exists` succeeds and the file loads. Only assert the
            // access-error message when access was actually denied.
            if let Err(err) = result {
                assert!(
                    err.contains("cannot access configuration file"),
                    "got: {err}"
                );
                assert!(err.contains("locked/config.toml"), "got: {err}");
            }
            Ok(())
        });
    }

    #[test]
    fn unknown_key_is_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", "not_a_real_key = 5\n")?;
            let err =
                RmsConfig::load(Path::new("config.toml")).expect_err("unknown key must error");
            assert!(err.contains("not_a_real_key"), "got: {err}");
            Ok(())
        });
    }

    #[test]
    fn unknown_key_in_section_is_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", "[postgres]\nnot_a_real_key = 5\n")?;
            let err = RmsConfig::load(Path::new("config.toml"))
                .expect_err("unknown key in a section must error");
            assert!(err.contains("not_a_real_key"), "got: {err}");
            Ok(())
        });
    }

    #[test]
    fn invalid_value_type_is_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", "port = \"not-a-number\"\n")?;
            let err = RmsConfig::load(Path::new("config.toml")).expect_err("bad type must error");
            assert!(err.contains("port"), "got: {err}");
            Ok(())
        });
    }

    #[test]
    fn db_pool_max_zero_is_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", "[postgres]\ndb_pool_max = 0\n")?;
            let err =
                RmsConfig::load(Path::new("config.toml")).expect_err("zero pool size must error");
            assert!(err.contains("db_pool_max"), "got: {err}");
            Ok(())
        });
    }
}
