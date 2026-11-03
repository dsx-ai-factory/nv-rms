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
//! are the only RMS configuration/behavior exceptions (TLS libraries may also
//! use standard process-wide trust-store discovery for tracing HTTPS):
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
//! | `[bmc]`       | [`RmsBmcConfig`]      |
//! | `[postgres]`  | [`RmsPostgresConfig`] |
//! | `[logging]`   | [`RmsLoggingConfig`]  |
//! | `[tracing]`   | [`RmsTracingConfig`]  |
//! | `[workflows]` | [`RmsWorkflowConfig`] |
//!
//! The loader is figment-based: the file is read once into a `Toml::string`
//! provider merged with a narrowly scoped `Env` provider, then extracted into
//! [`RmsConfig`]. Every field carries a serde default, so an empty or partial
//! file (including one that omits a section entirely) yields a
//! fully-populated config. Unknown keys are not fatal; they are collected and
//! reported by [`RmsConfig::load`] so the caller can warn about likely typos.

pub mod fabric_layout;
pub mod hardware_profiles;

use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use figment::value::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use nvfwupd::workflow::{FlintDeviceFamily, FlintExpectedDeviceCounts};

use crate::domain::node::{ExpectedInventoryPolicy, RecoveryProfile};
use crate::transport::ssh::SshUploadOptions;

mod workflow_timings;

pub use workflow_timings::{ComputePowerOnOptions, FirmwareTaskPollOptions, WorkflowTimings};

/// Default path RMS reads its configuration from when `--config` is omitted.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/rms/config.toml";

const MAX_TRACE_ENDPOINT_CHARS: usize = 2_048;
const MAX_TRACE_QUEUE_SIZE: usize = 65_536;
const MAX_TRACE_EXPORT_BATCH_SIZE: usize = 4_096;
const MAX_TRACE_SCHEDULED_DELAY_MS: u64 = 60_000;
const MAX_TRACE_EXPORT_ATTEMPT_TIMEOUT_MS: u64 = 60_000;
const MAX_TRACE_EXPORT_TIMEOUT_MS: u64 = 120_000;
const MAX_TRACE_SHUTDOWN_TIMEOUT_MS: u64 = 30_000;
const MAX_TRACE_RETRIES: usize = 10;
const MAX_TRACE_RETRY_DELAY_MS: u64 = 60_000;

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

fn default_insecure_bmc() -> bool {
    true
}

/// Default `[tls] cert_reload_interval_seconds`: five minutes.
pub const DEFAULT_CERT_RELOAD_INTERVAL_SECONDS: u64 = 5 * 60;

fn default_cert_reload_interval_seconds() -> u64 {
    DEFAULT_CERT_RELOAD_INTERVAL_SECONDS
}

fn default_sftp_upload_timeout_seconds() -> u64 {
    SshUploadOptions::default().overall_timeout.as_secs()
}

fn default_sftp_step_timeout_seconds() -> u64 {
    SshUploadOptions::default().step_timeout.as_secs()
}

fn default_firmware_task_timeout_seconds() -> NonZeroU64 {
    nonzero_secs(FirmwareTaskPollOptions::default().timeout)
}

fn default_apply_history_timeout_seconds() -> NonZeroU64 {
    nonzero_secs(WorkflowTimings::default().apply_history_timeout)
}

fn default_fabric_config_wait_log_interval_seconds() -> NonZeroU64 {
    nonzero_secs(WorkflowTimings::default().fabric_config_wait_log_interval)
}

fn default_onie_recovery_timeout_seconds() -> NonZeroU64 {
    nonzero_secs(WorkflowTimings::default().onie_recovery_timeout)
}

fn default_recovered_credential_save_timeout_seconds() -> NonZeroU64 {
    nonzero_secs(WorkflowTimings::default().recovered_credential_save_timeout)
}

fn default_firmware_task_poll_interval_seconds() -> NonZeroU64 {
    nonzero_secs(FirmwareTaskPollOptions::default().interval)
}

fn default_firmware_task_transient_error_timeout_seconds() -> NonZeroU64 {
    nonzero_secs(FirmwareTaskPollOptions::default().transient_error_timeout)
}

fn default_compute_power_on_timeout_seconds() -> NonZeroU64 {
    nonzero_secs(ComputePowerOnOptions::default().timeout)
}

fn default_compute_power_on_poll_interval_seconds() -> NonZeroU64 {
    nonzero_secs(ComputePowerOnOptions::default().interval)
}

/// Whole seconds of `duration` as a [`NonZeroU64`], for serde defaults sourced
/// from an options struct whose fields are always non-zero.
fn nonzero_secs(duration: std::time::Duration) -> NonZeroU64 {
    NonZeroU64::new(duration.as_secs()).expect("workflow timing defaults are non-zero")
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

fn default_trace_sampling_ratio() -> f64 {
    1.0
}

fn default_trace_max_queue_size() -> usize {
    2_048
}

fn default_trace_max_export_batch_size() -> usize {
    512
}

fn default_trace_scheduled_delay_ms() -> u64 {
    5_000
}

fn default_trace_export_attempt_timeout_ms() -> u64 {
    10_000
}

fn default_trace_export_timeout_ms() -> u64 {
    30_000
}

fn default_trace_shutdown_timeout_ms() -> u64 {
    5_000
}

fn default_trace_max_retries() -> usize {
    3
}

fn default_trace_retry_initial_delay_ms() -> u64 {
    100
}

fn default_trace_retry_max_delay_ms() -> u64 {
    1_600
}

fn default_trace_retry_jitter_ms() -> u64 {
    100
}

fn default_artifact_allow_private_targets() -> bool {
    true
}

/// One validated deployment-defined expected-inventory profile.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedInventoryProfile {
    /// Expected NVFWUPD AP names for out-of-band inventory validation.
    pub ap_names: Vec<String>,
    /// Expected host-visible physical adapter counts for Flint validation.
    #[serde(serialize_with = "serialize_flint_device_counts")]
    pub flint_devices: FlintExpectedDeviceCounts,
}

fn serialize_flint_device_counts<S>(
    counts: &FlintExpectedDeviceCounts,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let values: BTreeMap<_, _> = [
        ("cx7", FlintDeviceFamily::ConnectX7),
        ("cx8", FlintDeviceFamily::ConnectX8),
        ("bf3_nic", FlintDeviceFamily::BlueField3),
    ]
    .into_iter()
    .filter_map(|(name, family)| counts.get(&family).map(|count| (name, *count)))
    .collect();
    values.serialize(serializer)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawExpectedInventoryProfile {
    Legacy(Vec<String>),
    Detailed(RawDetailedExpectedInventoryProfile),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDetailedExpectedInventoryProfile {
    #[serde(default)]
    ap_names: Vec<String>,
    #[serde(default)]
    flint_devices: RawFlintDeviceCounts,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFlintDeviceCounts {
    cx7: Option<usize>,
    cx8: Option<usize>,
    bf3_nic: Option<usize>,
}

impl RawFlintDeviceCounts {
    fn into_expected_counts(self) -> FlintExpectedDeviceCounts {
        [
            (FlintDeviceFamily::ConnectX7, self.cx7),
            (FlintDeviceFamily::ConnectX8, self.cx8),
            (FlintDeviceFamily::BlueField3, self.bf3_nic),
        ]
        .into_iter()
        .filter_map(|(family, count)| count.map(|count| (family, count)))
        .collect()
    }
}

/// Recovery profiles loaded from `[workflows]`, one opaque table per
/// profile. RMS does not interpret a table: the node type that binds to
/// the profile at add_nodes parses the fields it understands and rejects
/// the rest. Startup checks only that every profile has a non-empty name
/// and maps to a table.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct RecoveryProfiles(BTreeMap<String, RecoveryProfile>);

impl RecoveryProfiles {
    /// The profile a node selects by name. A name without a table here
    /// leaves recovery detection off for that node.
    pub fn for_profile(&self, profile: &str) -> Option<&RecoveryProfile> {
        self.0.get(profile)
    }
}

impl<'de> Deserialize<'de> for RecoveryProfiles {
    /// Trims the profile names and rejects empty or duplicate ones. The
    /// tables stay opaque here, the node type parses them.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error as _;

        let raw = BTreeMap::<String, RecoveryProfile>::deserialize(deserializer)?;
        let mut profiles = BTreeMap::new();
        for (raw_name, table) in raw {
            // Trimmed like the sibling expected-inventory table: the two share
            // a profile key, so they must resolve the same names.
            let name = raw_name.trim();
            if name.is_empty() {
                return Err(D::Error::custom(
                    "recovery_profiles names must not be empty",
                ));
            }
            if profiles.insert(name.to_owned(), table).is_some() {
                return Err(D::Error::custom(format!(
                    "duplicate recovery_profiles entry after trimming: {name:?}"
                )));
            }
        }
        Ok(Self(profiles))
    }
}

/// Validated expected-inventory profiles loaded from `[workflows]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ExpectedInventoryProfiles(BTreeMap<String, ExpectedInventoryProfile>);

impl ExpectedInventoryProfiles {
    /// Return the validated configured profiles.
    pub fn as_map(&self) -> &BTreeMap<String, ExpectedInventoryProfile> {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ExpectedInventoryProfiles {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error as _;

        let raw = BTreeMap::<String, RawExpectedInventoryProfile>::deserialize(deserializer)?;
        let mut profiles = BTreeMap::new();

        for (raw_name, raw_profile) in raw {
            let profile = raw_name.trim();
            if profile.is_empty() {
                return Err(D::Error::custom(
                    "expected inventory profile names must not be empty",
                ));
            }
            let (raw_ap_names, flint_devices, legacy) = match raw_profile {
                RawExpectedInventoryProfile::Legacy(ap_names) => {
                    (ap_names, FlintExpectedDeviceCounts::new(), true)
                }
                RawExpectedInventoryProfile::Detailed(profile) => (
                    profile.ap_names,
                    profile.flint_devices.into_expected_counts(),
                    false,
                ),
            };
            if raw_ap_names.iter().any(|ap_name| ap_name.trim().is_empty()) {
                return Err(D::Error::custom(format!(
                    "expected inventory profile {profile:?} contains an empty AP name"
                )));
            }

            let ap_names = if raw_ap_names.is_empty() {
                if legacy || flint_devices.is_empty() {
                    return Err(D::Error::custom(format!(
                        "expected inventory profile {profile:?} must contain AP names or Flint device counts"
                    )));
                }
                Vec::new()
            } else {
                let value = serde_json::to_value(raw_ap_names).map_err(D::Error::custom)?;
                nvfwupd::expected_inventory::parse_expected_inventory_value(&value).map_err(
                    |message| {
                        D::Error::custom(format!(
                            "invalid expected inventory profile {profile:?}: {message}"
                        ))
                    },
                )?
            };

            if profiles
                .insert(
                    profile.to_owned(),
                    ExpectedInventoryProfile {
                        ap_names,
                        flint_devices,
                    },
                )
                .is_some()
            {
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
    profiles: Arc<BTreeMap<String, ExpectedInventoryCatalogEntry>>,
}

#[derive(Clone, Debug)]
struct ExpectedInventoryCatalogEntry {
    ap_names: Arc<[String]>,
    flint_device_counts: Arc<FlintExpectedDeviceCounts>,
}

impl From<&ExpectedInventoryProfiles> for ExpectedInventoryCatalog {
    fn from(config: &ExpectedInventoryProfiles) -> Self {
        let profiles = config
            .as_map()
            .iter()
            .map(|(profile, expected)| {
                (
                    profile.clone(),
                    ExpectedInventoryCatalogEntry {
                        ap_names: Arc::<[String]>::from(expected.ap_names.clone()),
                        flint_device_counts: Arc::new(expected.flint_devices.clone()),
                    },
                )
            })
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
        let expected = self
            .profiles
            .get(profile)
            .cloned()
            .ok_or_else(|| ExpectedInventoryProfileError::Unknown(profile.to_owned()))?;

        Ok(Some(ExpectedInventoryPolicy {
            profile: Arc::from(profile),
            ap_names: expected.ap_names,
            flint_device_counts: expected.flint_device_counts,
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
#[derive(Clone, Debug, Deserialize, Serialize)]
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

    /// How often, in seconds, the gRPC server re-reads `cert` and `key`.
    /// A matching renewed pair is presented on new handshakes. The client CA
    /// is not re-read. Must be greater than zero.
    #[serde(default = "default_cert_reload_interval_seconds")]
    pub cert_reload_interval_seconds: u64,
}

impl Default for RmsTlsConfig {
    fn default() -> Self {
        Self {
            cert: None,
            key: None,
            ca: None,
            insecure: false,
            cert_reload_interval_seconds: default_cert_reload_interval_seconds(),
        }
    }
}

impl RmsTlsConfig {
    /// Reject a reload interval that would busy-poll the certificate files.
    pub fn validate(&self) -> Result<(), String> {
        if self.cert_reload_interval_seconds == 0 {
            return Err("tls.cert_reload_interval_seconds must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// `[switches]` section: outbound RMS-to-switch TLS material and identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
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

/// `[bmc]` section: outbound RMS-to-BMC Redfish TLS policy.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RmsBmcConfig {
    /// Skip BMC/Redfish HTTPS server certificate verification.
    ///
    /// Historical default is `true` because production BMC certificates are
    /// typically self-signed and not issued by an RMS-trusted CA. Set `false`
    /// to verify BMC certificates against the process trust store (and the
    /// endpoint hostname or IP). gRPC callers cannot override this flag.
    #[serde(default = "default_insecure_bmc")]
    pub insecure_bmc: bool,
}

impl Default for RmsBmcConfig {
    fn default() -> Self {
        Self {
            insecure_bmc: default_insecure_bmc(),
        }
    }
}

/// `[postgres]` section: the persistence-layer database connection.
#[derive(Clone, Debug, Deserialize, Serialize)]
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

/// OTLP transport used to export RMS spans.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpTraceProtocol {
    /// OTLP over gRPC.
    #[default]
    Grpc,
    /// OTLP over HTTP with a protobuf request body.
    HttpProtobuf,
}

/// `[tracing]` section: OpenTelemetry span generation and OTLP export.
///
/// RMS always installs an OpenTelemetry layer so local log records can use real
/// trace and span identifiers. Remote export is enabled only when `endpoint`
/// is configured; an unreachable endpoint never prevents RMS from operating.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RmsTracingConfig {
    /// Explicit OTLP endpoint. HTTPS is required unless the host is a loopback
    /// IP address, such as a sidecar Collector at `http://127.0.0.1:4317`. For
    /// HTTP/protobuf this is the complete `/v1/traces` URL. When omitted, spans
    /// are created but not exported.
    #[serde(default)]
    pub endpoint: Option<String>,

    /// OTLP transport.
    #[serde(default)]
    pub protocol: OtlpTraceProtocol,

    /// RMS trace sampling ratio, independent of caller sampling flags. `1.0` records every trace.
    #[serde(default = "default_trace_sampling_ratio")]
    pub sampling_ratio: f64,

    /// Maximum number of completed spans waiting for export.
    #[serde(default = "default_trace_max_queue_size")]
    pub max_queue_size: usize,

    /// Maximum spans in one export request.
    #[serde(default = "default_trace_max_export_batch_size")]
    pub max_export_batch_size: usize,

    /// Maximum delay before exporting a non-full batch.
    #[serde(default = "default_trace_scheduled_delay_ms")]
    pub scheduled_delay_ms: u64,

    /// Per-attempt export timeout.
    #[serde(default = "default_trace_export_attempt_timeout_ms")]
    pub export_attempt_timeout_ms: u64,

    /// Total time allowed for one batch, including retries and backoff.
    #[serde(default = "default_trace_export_timeout_ms")]
    pub export_timeout_ms: u64,

    /// Maximum time RMS waits for trace shutdown after draining work.
    #[serde(default = "default_trace_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,

    /// Maximum export retries after the first failed attempt.
    #[serde(default = "default_trace_max_retries")]
    pub max_retries: usize,

    /// Initial retry delay.
    #[serde(default = "default_trace_retry_initial_delay_ms")]
    pub retry_initial_delay_ms: u64,

    /// Maximum retry delay.
    #[serde(default = "default_trace_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,

    /// Maximum jitter added to a retry delay.
    #[serde(default = "default_trace_retry_jitter_ms")]
    pub retry_jitter_ms: u64,
}

impl RmsTracingConfig {
    /// Validate tracing settings before RMS starts accepting work.
    pub fn validate(&self) -> Result<(), String> {
        if !self.sampling_ratio.is_finite()
            || self.sampling_ratio <= 0.0
            || self.sampling_ratio > 1.0
        {
            return Err("tracing.sampling_ratio must be finite and in (0.0, 1.0]".to_string());
        }
        if self.max_queue_size == 0 {
            return Err("tracing.max_queue_size must be greater than zero".to_string());
        }
        if self.max_queue_size > MAX_TRACE_QUEUE_SIZE {
            return Err(format!(
                "tracing.max_queue_size must be <= {MAX_TRACE_QUEUE_SIZE}"
            ));
        }
        if self.max_export_batch_size == 0 {
            return Err("tracing.max_export_batch_size must be greater than zero".to_string());
        }
        if self.max_export_batch_size > MAX_TRACE_EXPORT_BATCH_SIZE {
            return Err(format!(
                "tracing.max_export_batch_size must be <= {MAX_TRACE_EXPORT_BATCH_SIZE}"
            ));
        }
        if self.max_export_batch_size > self.max_queue_size {
            return Err(
                "tracing.max_export_batch_size must be <= tracing.max_queue_size".to_string(),
            );
        }
        if self.scheduled_delay_ms == 0 {
            return Err("tracing.scheduled_delay_ms must be greater than zero".to_string());
        }
        if self.scheduled_delay_ms > MAX_TRACE_SCHEDULED_DELAY_MS {
            return Err(format!(
                "tracing.scheduled_delay_ms must be <= {MAX_TRACE_SCHEDULED_DELAY_MS}"
            ));
        }
        if self.export_attempt_timeout_ms == 0 {
            return Err("tracing.export_attempt_timeout_ms must be greater than zero".to_string());
        }
        if self.export_attempt_timeout_ms > MAX_TRACE_EXPORT_ATTEMPT_TIMEOUT_MS {
            return Err(format!(
                "tracing.export_attempt_timeout_ms must be <= \
                 {MAX_TRACE_EXPORT_ATTEMPT_TIMEOUT_MS}"
            ));
        }
        if self.export_timeout_ms == 0 {
            return Err("tracing.export_timeout_ms must be greater than zero".to_string());
        }
        if self.export_timeout_ms > MAX_TRACE_EXPORT_TIMEOUT_MS {
            return Err(format!(
                "tracing.export_timeout_ms must be <= {MAX_TRACE_EXPORT_TIMEOUT_MS}"
            ));
        }
        if self.export_attempt_timeout_ms > self.export_timeout_ms {
            return Err(
                "tracing.export_attempt_timeout_ms must be <= tracing.export_timeout_ms"
                    .to_string(),
            );
        }
        if self.shutdown_timeout_ms == 0 {
            return Err("tracing.shutdown_timeout_ms must be greater than zero".to_string());
        }
        if self.shutdown_timeout_ms > MAX_TRACE_SHUTDOWN_TIMEOUT_MS {
            return Err(format!(
                "tracing.shutdown_timeout_ms must be <= {MAX_TRACE_SHUTDOWN_TIMEOUT_MS}"
            ));
        }
        if self.max_retries > MAX_TRACE_RETRIES {
            return Err(format!(
                "tracing.max_retries must be <= {MAX_TRACE_RETRIES}"
            ));
        }
        if self.retry_initial_delay_ms == 0 {
            return Err("tracing.retry_initial_delay_ms must be greater than zero".to_string());
        }
        if self.retry_max_delay_ms == 0 {
            return Err("tracing.retry_max_delay_ms must be greater than zero".to_string());
        }
        if self.retry_max_delay_ms > MAX_TRACE_RETRY_DELAY_MS {
            return Err(format!(
                "tracing.retry_max_delay_ms must be <= {MAX_TRACE_RETRY_DELAY_MS}"
            ));
        }
        if self.retry_jitter_ms > MAX_TRACE_RETRY_DELAY_MS {
            return Err(format!(
                "tracing.retry_jitter_ms must be <= {MAX_TRACE_RETRY_DELAY_MS}"
            ));
        }
        if self.retry_initial_delay_ms > self.retry_max_delay_ms {
            return Err(
                "tracing.retry_initial_delay_ms must be <= tracing.retry_max_delay_ms".to_string(),
            );
        }

        let Some(configured_endpoint) = self.endpoint.as_deref() else {
            return Ok(());
        };
        let endpoint = configured_endpoint.trim();
        if endpoint.is_empty() {
            return Err("tracing.endpoint must not be empty when set".to_string());
        }
        if endpoint != configured_endpoint {
            return Err("tracing.endpoint must not contain surrounding whitespace".to_string());
        }
        if endpoint.chars().count() > MAX_TRACE_ENDPOINT_CHARS {
            return Err(format!(
                "tracing.endpoint must be <= {MAX_TRACE_ENDPOINT_CHARS} characters"
            ));
        }
        let url = url::Url::parse(endpoint)
            .map_err(|e| format!("tracing.endpoint must be an absolute URL: {e}"))?;
        if url.scheme() != "https" && !(url.scheme() == "http" && is_loopback_ip_endpoint(endpoint))
        {
            return Err(
                "tracing.endpoint scheme must be https; plaintext http is allowed only for a \
                 loopback IP address such as 127.0.0.1 or [::1]"
                    .to_string(),
            );
        }
        if url.host_str().is_none() {
            return Err("tracing.endpoint must include a host".to_string());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("tracing.endpoint must not contain user information".to_string());
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err("tracing.endpoint must not contain a query or fragment".to_string());
        }
        if self.protocol == OtlpTraceProtocol::HttpProtobuf && !url.path().ends_with("/v1/traces") {
            return Err("tracing.endpoint must end with /v1/traces for http_protobuf".to_string());
        }
        if self.protocol == OtlpTraceProtocol::Grpc && url.path() != "/" {
            return Err("tracing.endpoint must not contain a path for grpc".to_string());
        }

        Ok(())
    }
}

/// Whether a trace endpoint's host is a loopback IP address, the only target
/// RMS exports to without TLS.
///
/// The host must be a standard literal in `127.0.0.0/8` or `::1`, so the
/// connection cannot leave the host or depend on name resolution. Hostnames,
/// including `localhost`, and alternate forms such as `127.1` do not qualify.
pub(crate) fn is_loopback_ip_endpoint(endpoint: &str) -> bool {
    let Ok(uri) = endpoint.parse::<http::Uri>() else {
        return false;
    };
    let Some(host) = uri.host() else {
        return false;
    };
    let (literal, ip) = match host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
    {
        Some(literal) => (literal, literal.parse().ok().map(std::net::IpAddr::V6)),
        None => (host, host.parse().ok().map(std::net::IpAddr::V4)),
    };
    ip.is_some_and(|ip| ip.is_loopback() && ip.to_string() == literal)
}

impl Default for RmsTracingConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            protocol: OtlpTraceProtocol::default(),
            sampling_ratio: default_trace_sampling_ratio(),
            max_queue_size: default_trace_max_queue_size(),
            max_export_batch_size: default_trace_max_export_batch_size(),
            scheduled_delay_ms: default_trace_scheduled_delay_ms(),
            export_attempt_timeout_ms: default_trace_export_attempt_timeout_ms(),
            export_timeout_ms: default_trace_export_timeout_ms(),
            shutdown_timeout_ms: default_trace_shutdown_timeout_ms(),
            max_retries: default_trace_max_retries(),
            retry_initial_delay_ms: default_trace_retry_initial_delay_ms(),
            retry_max_delay_ms: default_trace_retry_max_delay_ms(),
            retry_jitter_ms: default_trace_retry_jitter_ms(),
        }
    }
}

/// `[workflows]` section: firmware staging and long-running job tracking.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RmsWorkflowConfig {
    /// Delay between fabric configuration wait logs, in seconds. Does not change device polling.
    #[serde(
        rename = "fabric_config_wait_log_interval",
        default = "default_fabric_config_wait_log_interval_seconds"
    )]
    pub fabric_config_wait_log_interval_seconds: NonZeroU64,

    /// Directory where RMS stores and reads firmware artifacts.
    #[serde(default = "default_firmware_dir")]
    pub firmware_dir: PathBuf,

    /// Optional directory of versioned hardware update profiles.
    ///
    /// When omitted, RMS uses the profiles distributed with the binary. When
    /// set, this directory replaces the built-in catalog and is validated in
    /// full before the gRPC server starts. The compiled catalog is immutable for
    /// the lifetime of the process. Empty and partial catalogs are valid; node
    /// types without an enabled profile use their code-defined defaults. Profile
    /// edits and directory-path changes take effect only after RMS restarts.
    #[serde(default)]
    pub hardware_profiles_dir: Option<PathBuf>,

    /// Optional deployment-owned sizing, grouping, and link calculation rules.
    /// Definitions are validated during configuration loading and selected by V2 layout requests.
    #[serde(default)]
    pub fabric_layout: Option<fabric_layout::FabricLayoutConfig>,

    /// Overall timeout (seconds) for switch NVOS SFTP image uploads.
    #[serde(default = "default_sftp_upload_timeout_seconds")]
    pub sftp_upload_timeout_seconds: u64,

    /// Stall timeout (seconds) for one switch NVOS SFTP upload step.
    #[serde(default = "default_sftp_step_timeout_seconds")]
    pub sftp_step_timeout_seconds: u64,

    /// Upper bound (seconds) on polling a single firmware task for completion.
    #[serde(default = "default_firmware_task_timeout_seconds")]
    pub firmware_task_timeout_seconds: NonZeroU64,

    /// Delay (seconds) between successive polls of an in-flight firmware task.
    #[serde(default = "default_firmware_task_poll_interval_seconds")]
    pub firmware_task_poll_interval_seconds: NonZeroU64,

    /// How long (seconds) to tolerate consecutive firmware task-status read
    /// failures before surfacing the error.
    #[serde(default = "default_firmware_task_transient_error_timeout_seconds")]
    pub firmware_task_transient_error_timeout_seconds: NonZeroU64,

    /// How long (seconds) a compute firmware update waits for the tray to
    /// report power `On` after the power-on command that opens the flow.
    #[serde(default = "default_compute_power_on_timeout_seconds")]
    pub compute_power_on_timeout_seconds: NonZeroU64,

    /// Delay (seconds) between power-state reads during that wait.
    #[serde(default = "default_compute_power_on_poll_interval_seconds")]
    pub compute_power_on_poll_interval_seconds: NonZeroU64,

    /// Independent observation deadline for stored-apply history; does not
    /// cancel updates or mark unfinished nodes successful when it expires.
    #[serde(default = "default_apply_history_timeout_seconds")]
    pub apply_history_timeout_seconds: NonZeroU64,

    /// Wait after ONIE starts installing until NVOS SSH is ready.
    #[serde(default = "default_onie_recovery_timeout_seconds")]
    pub onie_recovery_timeout_seconds: NonZeroU64,

    /// Wait for NVUE to save recovered NVOS credentials after boot.
    #[serde(default = "default_recovered_credential_save_timeout_seconds")]
    pub recovered_credential_save_timeout_seconds: NonZeroU64,

    /// Maximum number of async job records the tracker retains.
    #[serde(default = "default_max_tracked_jobs")]
    pub max_tracked_jobs: NonZeroUsize,

    /// Retention period (seconds) for terminal job records before eviction.
    #[serde(default = "default_terminal_job_ttl_seconds")]
    pub terminal_job_ttl_seconds: NonZeroU64,

    /// Deployment-defined AP inventories and Flint device counts selected by
    /// the `inventory_profile` node descriptor attribute.
    #[serde(default)]
    pub expected_inventory_profiles: ExpectedInventoryProfiles,

    /// When true, HTTP(S) firmware artifacts must include a SHA-256 digest.
    #[serde(default)]
    pub artifact_require_sha256: bool,
    /// Hostname or `*.suffix` allow-list for HTTP(S) firmware artifact
    /// downloads (`*.suffix` matches the suffix and its subdomains). Entries
    /// are parsed as URL hosts at startup (IDNA, no interior whitespace).
    /// IPv6 literals need brackets. Empty preserves historical behavior
    /// (any host).
    #[serde(default)]
    pub artifact_https_hosts: Vec<String>,

    /// When false, HTTP(S) artifact URLs may not resolve to IANA
    /// special-purpose addresses (loopback, link-local, multicast, RFC1918/ULA,
    /// CGNAT, NAT64, reserved, and documentation ranges).
    #[serde(default = "default_artifact_allow_private_targets")]
    pub artifact_allow_private_targets: bool,

    /// Deployment-defined recovery profiles, one opaque table per profile,
    /// selected by the same `inventory_profile` node descriptor attribute.
    #[serde(default)]
    pub recovery_profiles: RecoveryProfiles,
}

impl Default for RmsWorkflowConfig {
    fn default() -> Self {
        Self {
            fabric_config_wait_log_interval_seconds:
                default_fabric_config_wait_log_interval_seconds(),
            firmware_dir: default_firmware_dir(),
            hardware_profiles_dir: None,
            fabric_layout: None,
            sftp_upload_timeout_seconds: default_sftp_upload_timeout_seconds(),
            sftp_step_timeout_seconds: default_sftp_step_timeout_seconds(),
            firmware_task_timeout_seconds: default_firmware_task_timeout_seconds(),
            firmware_task_poll_interval_seconds: default_firmware_task_poll_interval_seconds(),
            firmware_task_transient_error_timeout_seconds:
                default_firmware_task_transient_error_timeout_seconds(),
            compute_power_on_timeout_seconds: default_compute_power_on_timeout_seconds(),
            compute_power_on_poll_interval_seconds: default_compute_power_on_poll_interval_seconds(
            ),
            apply_history_timeout_seconds: default_apply_history_timeout_seconds(),
            onie_recovery_timeout_seconds: default_onie_recovery_timeout_seconds(),
            recovered_credential_save_timeout_seconds:
                default_recovered_credential_save_timeout_seconds(),
            max_tracked_jobs: default_max_tracked_jobs(),
            terminal_job_ttl_seconds: default_terminal_job_ttl_seconds(),
            expected_inventory_profiles: ExpectedInventoryProfiles::default(),
            artifact_require_sha256: false,
            artifact_https_hosts: Vec::new(),
            artifact_allow_private_targets: default_artifact_allow_private_targets(),
            recovery_profiles: RecoveryProfiles::default(),
        }
    }
}

/// Fully-parsed RMS runtime configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
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

    /// `[bmc]` section: outbound RMS-to-BMC Redfish TLS policy.
    #[serde(default)]
    pub bmc: RmsBmcConfig,

    /// `[postgres]` section: the persistence-layer database connection.
    #[serde(default)]
    pub postgres: RmsPostgresConfig,

    /// `[logging]` section: tracing output configuration.
    #[serde(default)]
    pub logging: RmsLoggingConfig,

    /// `[tracing]` section: OpenTelemetry span and OTLP export configuration.
    #[serde(default)]
    pub tracing: RmsTracingConfig,

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
    /// Returns the config alongside the dotted paths of any keys the config
    /// structs do not define. Unknown keys are usually typos, so the caller is
    /// expected to warn about them once logging is initialized.
    ///
    /// Errors are human-readable strings suitable for logging to stderr on
    /// startup before the tracing subscriber is installed.
    pub fn load(path: &Path) -> Result<(Self, Vec<String>), String> {
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

        let figment = Self::figment(&contents);
        let config: Self = figment
            .extract()
            .map_err(|e| format!("failed to load configuration from {}: {e}", path.display()))?;
        config
            .tracing
            .validate()
            .map_err(|e| format!("failed to load configuration from {}: {e}", path.display()))?;
        config
            .tls
            .validate()
            .map_err(|e| format!("failed to load configuration from {}: {e}", path.display()))?;

        if let Some(settings) = &config.workflows.fabric_layout {
            fabric_layout::FabricLayoutCatalog::from_config(settings).map_err(|error| {
                format!(
                    "failed to load configuration from {}: {error}",
                    path.display()
                )
            })?;
        }

        // Second pass purely to name the keys no field claims; values were
        // already validated above, and figment reports those errors better.
        let mut unknown_keys = Vec::new();
        if let Ok(merged) = figment.extract::<Value>() {
            let _ = serde_ignored::deserialize::<_, _, Self>(&merged, |key| {
                unknown_keys.push(key.to_string());
            });
        }

        Ok((config, unknown_keys))
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

    /// Load a config, discarding the unknown-key report.
    fn load(path: &str) -> Result<RmsConfig, String> {
        RmsConfig::load(Path::new(path)).map(|(config, _)| config)
    }

    #[test]
    fn empty_file_yields_historical_defaults() {
        Jail::expect_with(|jail| {
            // Isolate from any ambient `DATABASE_URL` (common in CI), which the
            // loader would otherwise merge into `postgres.db_url`.
            jail.clear_env();
            jail.create_file("config.toml", "")?;
            let config = load("config.toml").expect("empty config loads");

            assert_eq!(config.port, 8801);
            assert_eq!(config.metrics.port, 8802);
            assert!(!config.metrics.tls);
            assert!(config.tls.cert.is_none());
            assert!(config.tls.key.is_none());
            assert!(config.tls.ca.is_none());
            assert!(!config.tls.insecure);
            assert_eq!(
                config.tls.cert_reload_interval_seconds,
                DEFAULT_CERT_RELOAD_INTERVAL_SECONDS
            );
            assert!(config.switches.switch_cert_root.is_none());
            assert!(config.switches.client_tls_root.is_none());
            assert!(config.switches.default_switch_domain.is_none());
            assert!(config.switches.dns_domain.is_none());
            assert!(!config.switches.insecure_switch);
            assert!(config.bmc.insecure_bmc);
            assert_eq!(
                config.switches.nmx_gateway_id,
                crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID
            );
            assert!(config.postgres.db_url.is_none());
            assert_eq!(config.postgres.db_pool_max, NonZeroUsize::new(20).unwrap());
            assert_eq!(config.workflows.firmware_dir, PathBuf::from("firmware"));

            assert_eq!(
                config
                    .workflows
                    .fabric_config_wait_log_interval_seconds
                    .get(),
                30
            );

            let sftp_defaults = SshUploadOptions::default();

            assert_eq!(
                config.workflows.sftp_upload_timeout_seconds,
                sftp_defaults.overall_timeout.as_secs()
            );
            assert_eq!(
                config.workflows.sftp_step_timeout_seconds,
                sftp_defaults.step_timeout.as_secs()
            );

            let firmware_task_defaults = FirmwareTaskPollOptions::default();

            assert_eq!(
                config.workflows.firmware_task_timeout_seconds.get(),
                firmware_task_defaults.timeout.as_secs()
            );

            assert_eq!(config.workflows.onie_recovery_timeout_seconds.get(), 900);

            assert_eq!(
                config
                    .workflows
                    .recovered_credential_save_timeout_seconds
                    .get(),
                300
            );

            assert_eq!(
                config.workflows.firmware_task_poll_interval_seconds.get(),
                firmware_task_defaults.interval.as_secs()
            );
            assert_eq!(
                config
                    .workflows
                    .firmware_task_transient_error_timeout_seconds
                    .get(),
                firmware_task_defaults.transient_error_timeout.as_secs()
            );

            let power_on_defaults = ComputePowerOnOptions::default();
            assert_eq!(
                config.workflows.compute_power_on_timeout_seconds.get(),
                power_on_defaults.timeout.as_secs()
            );
            assert_eq!(
                config
                    .workflows
                    .compute_power_on_poll_interval_seconds
                    .get(),
                power_on_defaults.interval.as_secs()
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
            assert!(!config.workflows.artifact_require_sha256);
            assert!(config.workflows.artifact_https_hosts.is_empty());
            assert!(config.workflows.artifact_allow_private_targets);
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
            let loaded = load("config.toml").unwrap();
            let defaulted = RmsConfig::default();
            assert_eq!(loaded.port, defaulted.port);
            assert_eq!(defaulted.port, 8801);
            assert_eq!(
                defaulted.postgres.db_pool_max,
                NonZeroUsize::new(20).unwrap()
            );
            assert!(!defaulted.logging.enable_timestamps);
            assert_eq!(
                loaded.tls.cert_reload_interval_seconds,
                defaulted.tls.cert_reload_interval_seconds
            );
            Ok(())
        });
    }

    #[test]
    fn cert_reload_interval_rejects_zero() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file("config.toml", "[tls]\ncert_reload_interval_seconds = 0\n")?;
            let err = load("config.toml").expect_err("zero interval must fail");
            assert!(
                err.contains("cert_reload_interval_seconds must be greater than zero"),
                "{err}"
            );
            Ok(())
        });
    }

    #[test]
    fn fabric_layout_toml_rejects_unknown_fields_and_empty_catalogs() {
        Jail::expect_with(|jail| {
            jail.clear_env();

            jail.create_file(
                "config.toml",
                "[workflows.fabric_layout]\nprofiles = []\nunrecognized = true\n",
            )?;

            let error = load("config.toml").unwrap_err();

            assert!(error.contains("unrecognized"), "{error}");

            jail.create_file("config.toml", "[workflows.fabric_layout]\nprofiles = []\n")?;

            assert!(load("config.toml").is_err());

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
                cert_reload_interval_seconds = 90

                [switches]
                switch_cert_root = "/switch-cert"
                client_tls_root = "/client-tls"
                default_switch_domain = "site-wide"
                dns_domain = "switch.example.com"
                insecure_switch = true
                nmx_gateway_id = "custom-gateway"

                [bmc]
                insecure_bmc = false

                [postgres]
                db_url = "postgres://user:pw@host/db"
                db_pool_max = 42

                [workflows]
                firmware_dir = "/var/lib/rms/firmware"
                fabric_config_wait_log_interval = 10
                hardware_profiles_dir = "/etc/rms/hardware-profiles/v1"
                sftp_upload_timeout_seconds = 1200
                sftp_step_timeout_seconds = 15
                firmware_task_timeout_seconds = 7200
                firmware_task_poll_interval_seconds = 45
                firmware_task_transient_error_timeout_seconds = 300
                compute_power_on_timeout_seconds = 90
                compute_power_on_poll_interval_seconds = 10
                onie_recovery_timeout_seconds = 1200
                recovered_credential_save_timeout_seconds = 420
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
            let config = load("config.toml").expect("full config loads");

            assert_eq!(config.port, 9001);
            assert_eq!(config.metrics.port, 9002);
            assert!(config.metrics.tls);
            assert_eq!(config.tls.cert.as_deref(), Some("/certs/tls.crt"));
            assert_eq!(config.tls.key.as_deref(), Some("/certs/tls.key"));
            assert_eq!(config.tls.ca.as_deref(), Some("/certs/ca.crt"));
            assert!(config.tls.insecure);
            assert_eq!(config.tls.cert_reload_interval_seconds, 90);
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
            assert!(!config.bmc.insecure_bmc);
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
            assert_eq!(
                config.workflows.hardware_profiles_dir,
                Some(PathBuf::from("/etc/rms/hardware-profiles/v1"))
            );
            assert_eq!(config.workflows.sftp_upload_timeout_seconds, 1200);
            assert_eq!(config.workflows.sftp_step_timeout_seconds, 15);

            assert_eq!(
                config
                    .workflows
                    .fabric_config_wait_log_interval_seconds
                    .get(),
                10
            );

            assert_eq!(
                config.workflows.firmware_task_timeout_seconds,
                NonZeroU64::new(7200).unwrap()
            );

            assert_eq!(config.workflows.onie_recovery_timeout_seconds.get(), 1200);

            assert_eq!(
                config
                    .workflows
                    .recovered_credential_save_timeout_seconds
                    .get(),
                420
            );

            assert_eq!(
                config.workflows.firmware_task_poll_interval_seconds,
                NonZeroU64::new(45).unwrap()
            );
            assert_eq!(
                config
                    .workflows
                    .firmware_task_transient_error_timeout_seconds,
                NonZeroU64::new(300).unwrap()
            );
            assert_eq!(
                config.workflows.compute_power_on_timeout_seconds,
                NonZeroU64::new(90).unwrap()
            );
            assert_eq!(
                config.workflows.compute_power_on_poll_interval_seconds,
                NonZeroU64::new(10).unwrap()
            );
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
                    .get("gb200-compute-variant-a")
                    .map(|profile| profile.ap_names.as_slice()),
                Some(["FW_BMC_0".to_owned(), "HGX_FW_GPU_0".to_owned()].as_slice())
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
    fn firmware_task_poll_keys_reject_zero() {
        for key in [
            "fabric_config_wait_log_interval",
            "firmware_task_timeout_seconds",
            "firmware_task_poll_interval_seconds",
            "firmware_task_transient_error_timeout_seconds",
            "compute_power_on_timeout_seconds",
            "compute_power_on_poll_interval_seconds",
            "apply_history_timeout_seconds",
            "onie_recovery_timeout_seconds",
            "recovered_credential_save_timeout_seconds",
        ] {
            Jail::expect_with(|jail| {
                jail.clear_env();
                jail.create_file("config.toml", &format!("[workflows]\n{key} = 0\n"))?;
                let error = RmsConfig::load(Path::new("config.toml"))
                    .expect_err("a zero-second workflow bound must be rejected");
                assert!(error.contains(key), "{key}: {error}");
                Ok(())
            });
        }
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

            let config = load("config.toml").unwrap();
            assert_eq!(
                config
                    .workflows
                    .expected_inventory_profiles
                    .as_map()
                    .get("profile-a")
                    .map(|profile| profile.ap_names.as_slice()),
                Some(["FW_BMC_0".to_owned(), "HGX_FW_GPU_0".to_owned()].as_slice())
            );
            Ok(())
        });
    }

    #[test]
    fn expected_inventory_profile_accepts_flint_device_counts() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "config.toml",
                r#"
                [workflows.expected_inventory_profiles."sku-gb200"]
                ap_names = ["FW_BMC_0", "HGX_FW_GPU_0"]

                [workflows.expected_inventory_profiles."sku-gb200".flint_devices]
                cx7 = 4
                cx8 = 0
                bf3_nic = 2
                "#,
            )?;

            let config = load("config.toml").unwrap();
            let catalog =
                ExpectedInventoryCatalog::from(&config.workflows.expected_inventory_profiles);
            let policy = catalog.resolve(Some("sku-gb200")).unwrap().unwrap();

            assert_eq!(policy.ap_names.as_ref(), ["FW_BMC_0", "HGX_FW_GPU_0"]);
            assert_eq!(
                policy
                    .flint_device_counts
                    .get(&FlintDeviceFamily::ConnectX7),
                Some(&4)
            );
            assert_eq!(
                policy
                    .flint_device_counts
                    .get(&FlintDeviceFamily::ConnectX8),
                Some(&0)
            );
            assert_eq!(
                policy
                    .flint_device_counts
                    .get(&FlintDeviceFamily::BlueField3),
                Some(&2)
            );
            Ok(())
        });
    }

    #[test]
    fn recovery_profiles_are_opaque_tables_keyed_by_trimmed_profile_name() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "config.toml",
                r#"
                [workflows.recovery_profiles." gb200-compute-variant-a "]
                regular_bundle = ["HGX_FW_FPGA_0"]
                some_other_platforms_field = { nested = 1 }
                "#,
            )?;

            let (config, _warnings) = RmsConfig::load(Path::new("config.toml")).unwrap();
            let profiles = &config.workflows.recovery_profiles;
            let profile = profiles
                .for_profile("gb200-compute-variant-a")
                .expect("the trimmed name resolves");

            // The table stays opaque here. The consumer decides the schema and
            // parses it as whatever it asks for.
            let table = profile.parse::<serde_json::Value>().unwrap();
            assert_eq!(
                table["regular_bundle"],
                serde_json::json!(["HGX_FW_FPGA_0"])
            );

            assert!(profiles.for_profile("unknown-variant").is_none());
            Ok(())
        });
    }

    #[test]
    fn recovery_profiles_reject_bad_shapes() {
        for (name, toml, needle) in [
            (
                "blank profile name",
                "[workflows.recovery_profiles]\n\"  \" = { regular_bundle = [\"HGX_FW_FPGA_0\"] }\n",
                "recovery_profiles names must not be empty",
            ),
            (
                "duplicate after trimming",
                "[workflows.recovery_profiles]\n\"p\" = { a = 1 }\n\" p \" = { b = 2 }\n",
                "duplicate recovery_profiles entry",
            ),
            (
                "profile is not a table",
                "[workflows.recovery_profiles]\n\"p\" = [\"HGX_FW_FPGA_0\"]\n",
                "",
            ),
        ] {
            Jail::expect_with(|jail| {
                jail.clear_env();
                jail.create_file("config.toml", toml)?;
                let error = RmsConfig::load(Path::new("config.toml")).unwrap_err();
                assert!(error.contains(needle), "{name}: {error}");
                Ok(())
            });
        }
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
            (
                "empty detailed profile",
                "[workflows.expected_inventory_profiles.profile-a]\nap_names = []\n",
            ),
            (
                "unknown Flint family",
                "[workflows.expected_inventory_profiles.profile-a.flint_devices]\ncx9 = 1\n",
            ),
        ] {
            Jail::expect_with(|jail| {
                jail.clear_env();
                jail.create_file("config.toml", toml)?;
                let error = load("config.toml").unwrap_err();
                assert!(
                    error.contains("expected inventory")
                        || error.contains("invalid type")
                        || error.contains("AP name")
                        || error.contains("did not match")
                        || error.contains("unknown field"),
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
            let config = load("config.toml").unwrap();
            let catalog =
                ExpectedInventoryCatalog::from(&config.workflows.expected_inventory_profiles);

            let policy = catalog.resolve(Some(" profile-a ")).unwrap().unwrap();
            assert_eq!(policy.profile.as_ref(), "profile-a");
            assert_eq!(policy.ap_names.as_ref(), ["FW_BMC_0"]);
            assert!(policy.flint_device_counts.is_empty());
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
            let config = load("config.toml").unwrap();
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
            let config = load("config.toml").unwrap();
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
            let config = load("config.toml").unwrap();
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
            let config = load("config.toml").unwrap();
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
            let err = load("does-not-exist.toml").expect_err("missing file must error");
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

            let result = load("locked/config.toml");

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
    fn invalid_value_type_is_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", "port = \"not-a-number\"\n")?;
            let err = load("config.toml").expect_err("bad type must error");
            assert!(err.contains("port"), "got: {err}");
            Ok(())
        });
    }

    #[test]
    fn unknown_keys_are_reported_but_accepted() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "config.toml",
                "prot = 9001\n\n[postgres]\ndb_pool_maxx = 42\n",
            )?;
            let (config, unknown_keys) = RmsConfig::load(Path::new("config.toml")).unwrap();

            assert_eq!(config.port, 8801);
            assert_eq!(unknown_keys, ["postgres.db_pool_maxx", "prot"]);
            Ok(())
        });
    }

    #[test]
    fn tracing_defaults_are_bounded_and_do_not_export() {
        let tracing = RmsTracingConfig::default();
        assert!(tracing.endpoint.is_none());
        assert_eq!(tracing.protocol, OtlpTraceProtocol::Grpc);
        assert_eq!(tracing.sampling_ratio, 1.0);
        assert_eq!(tracing.max_queue_size, 2_048);
        assert_eq!(tracing.max_export_batch_size, 512);
        assert_eq!(tracing.scheduled_delay_ms, 5_000);
        assert_eq!(tracing.export_attempt_timeout_ms, 10_000);
        assert_eq!(tracing.export_timeout_ms, 30_000);
        assert_eq!(tracing.shutdown_timeout_ms, 5_000);
        assert_eq!(tracing.max_retries, 3);
        tracing.validate().unwrap();
    }

    #[test]
    fn tracing_config_parses_both_protocols() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "grpc.toml",
                r#"
                    [tracing]
                    endpoint = "https://collector.example:4317"
                    protocol = "grpc"
                    sampling_ratio = 0.25
                    max_queue_size = 128
                    max_export_batch_size = 64
                    scheduled_delay_ms = 250
                    export_attempt_timeout_ms = 1000
                    export_timeout_ms = 2000
                    shutdown_timeout_ms = 1000
                    max_retries = 2
                    retry_initial_delay_ms = 10
                    retry_max_delay_ms = 100
                    retry_jitter_ms = 5
                "#,
            )?;
            let grpc = load("grpc.toml").unwrap();
            assert_eq!(
                grpc.tracing.endpoint.as_deref(),
                Some("https://collector.example:4317")
            );
            assert_eq!(grpc.tracing.protocol, OtlpTraceProtocol::Grpc);
            assert_eq!(grpc.tracing.sampling_ratio, 0.25);
            assert_eq!(grpc.tracing.max_queue_size, 128);
            assert_eq!(grpc.tracing.max_export_batch_size, 64);
            assert_eq!(grpc.tracing.scheduled_delay_ms, 250);
            assert_eq!(grpc.tracing.export_attempt_timeout_ms, 1000);
            assert_eq!(grpc.tracing.export_timeout_ms, 2000);
            assert_eq!(grpc.tracing.shutdown_timeout_ms, 1000);
            assert_eq!(grpc.tracing.max_retries, 2);
            assert_eq!(grpc.tracing.retry_initial_delay_ms, 10);
            assert_eq!(grpc.tracing.retry_max_delay_ms, 100);
            assert_eq!(grpc.tracing.retry_jitter_ms, 5);

            jail.create_file(
                "http.toml",
                r#"
                    [tracing]
                    endpoint = "https://collector.example:4318/v1/traces"
                    protocol = "http_protobuf"
                    sampling_ratio = 1.00000000000000000e+00
                "#,
            )?;
            let http = load("http.toml").unwrap();
            assert_eq!(
                http.tracing.endpoint.as_deref(),
                Some("https://collector.example:4318/v1/traces")
            );
            assert_eq!(http.tracing.protocol, OtlpTraceProtocol::HttpProtobuf);
            assert_eq!(http.tracing.sampling_ratio, 1.0);
            Ok(())
        });
    }

    #[test]
    fn invalid_tracing_settings_are_rejected() {
        for (key, value, expected) in [
            ("sampling_ratio", "0.0", "sampling_ratio"),
            ("sampling_ratio", "1.1", "sampling_ratio"),
            ("sampling_ratio", "-0.1", "sampling_ratio"),
            ("sampling_ratio", "nan", "sampling_ratio"),
            ("sampling_ratio", "inf", "sampling_ratio"),
            ("sampling_ratio", "-inf", "sampling_ratio"),
            ("max_queue_size", "0", "max_queue_size"),
            ("max_queue_size", "65537", "max_queue_size"),
            ("max_export_batch_size", "0", "max_export_batch_size"),
            ("max_export_batch_size", "4097", "max_export_batch_size"),
            ("scheduled_delay_ms", "0", "scheduled_delay_ms"),
            ("scheduled_delay_ms", "60001", "scheduled_delay_ms"),
            (
                "export_attempt_timeout_ms",
                "0",
                "export_attempt_timeout_ms",
            ),
            (
                "export_attempt_timeout_ms",
                "60001",
                "export_attempt_timeout_ms",
            ),
            ("export_timeout_ms", "0", "export_timeout_ms"),
            ("export_timeout_ms", "120001", "export_timeout_ms"),
            ("shutdown_timeout_ms", "0", "shutdown_timeout_ms"),
            ("shutdown_timeout_ms", "30001", "shutdown_timeout_ms"),
            ("max_retries", "11", "max_retries"),
            ("retry_initial_delay_ms", "0", "retry_initial_delay_ms"),
            ("retry_max_delay_ms", "0", "retry_max_delay_ms"),
            ("retry_max_delay_ms", "60001", "retry_max_delay_ms"),
            ("retry_jitter_ms", "60001", "retry_jitter_ms"),
        ] {
            Jail::expect_with(|jail| {
                jail.clear_env();
                jail.create_file("config.toml", &format!("[tracing]\n{key} = {value}\n"))?;
                let error = load("config.toml").expect_err("invalid tracing value must fail");
                assert!(error.contains(expected), "got: {error}");
                Ok(())
            });
        }
    }

    fn plaintext_tracing_config(
        protocol: OtlpTraceProtocol,
        scheme: &str,
        host: &str,
    ) -> RmsTracingConfig {
        let path = match protocol {
            OtlpTraceProtocol::Grpc => "",
            OtlpTraceProtocol::HttpProtobuf => "/v1/traces",
        };
        RmsTracingConfig {
            endpoint: Some(format!("{scheme}://{host}:4317{path}")),
            protocol,
            ..RmsTracingConfig::default()
        }
    }

    #[test]
    fn plaintext_tracing_endpoints_are_accepted_for_loopback_ip_addresses() {
        for protocol in [OtlpTraceProtocol::Grpc, OtlpTraceProtocol::HttpProtobuf] {
            for host in ["127.0.0.1", "127.255.255.254", "[::1]"] {
                for scheme in ["http", "HTTP"] {
                    let config = plaintext_tracing_config(protocol, scheme, host);
                    assert_eq!(config.validate(), Ok(()), "{scheme}://{host} {protocol:?}");
                }
            }
        }
    }

    #[test]
    fn plaintext_tracing_endpoints_are_rejected_for_other_hosts() {
        for protocol in [OtlpTraceProtocol::Grpc, OtlpTraceProtocol::HttpProtobuf] {
            for host in [
                "collector.example",
                "localhost",
                "10.0.0.1",
                "0.0.0.0",
                "[::]",
                "[::ffff:127.0.0.1]",
                "127.1",
                "127.0.0.01",
                "[0:0:0:0:0:0:0:1]",
                "0x7f.0.0.1",
                "2130706433",
                "127.0.0.1.",
                "%31%32%37.0.0.1",
                "127.0.0.1@collector.example",
            ] {
                for scheme in ["http", "HTTP"] {
                    let config = plaintext_tracing_config(protocol, scheme, host);
                    let error = config.validate().expect_err("plaintext export must fail");
                    assert!(error.contains("scheme must be https"), "{host}: {error}");
                }
            }
        }
    }

    #[test]
    fn tracing_cross_field_and_endpoint_validation_is_strict() {
        for (contents, expected) in [
            ("[tracing]\nendpoint = \"\"\n", "must not be empty"),
            ("[tracing]\nendpoint = \"collector\"\n", "absolute URL"),
            (
                "[tracing]\nendpoint = \"https://collector.example:4317?token=secret\"\n",
                "query or fragment",
            ),
            (
                "[tracing]\nendpoint = \"https://collector.example:4317#secret\"\n",
                "query or fragment",
            ),
            (
                "[tracing]\nmax_queue_size = 1\nmax_export_batch_size = 2\n",
                "max_export_batch_size",
            ),
            (
                "[tracing]\nretry_initial_delay_ms = 20\nretry_max_delay_ms = 10\n",
                "retry_initial_delay_ms",
            ),
            (
                "[tracing]\nexport_attempt_timeout_ms = 20\nexport_timeout_ms = 10\n",
                "export_attempt_timeout_ms",
            ),
            (
                "[tracing]\nendpoint = \"ftp://collector.example\"\n",
                "scheme",
            ),
            (
                "[tracing]\nendpoint = \"https://collector.example/path\"\n",
                "must not contain a path",
            ),
            (
                "[tracing]\nendpoint = \"https://collector.example:4318\"\nprotocol = \"http_protobuf\"\n",
                "/v1/traces",
            ),
            (
                "[tracing]\nendpoint = \"https://user:pass@collector.example\"\n",
                "user information",
            ),
            (
                "[tracing]\nendpoint = \"http://user:pass@127.0.0.1:4317\"\n",
                "user information",
            ),
            (
                "[tracing]\nendpoint = \"http://127.0.0.1:4317/path\"\n",
                "must not contain a path",
            ),
            (
                "[tracing]\nendpoint = \"http://127.0.0.1:4318\"\nprotocol = \"http_protobuf\"\n",
                "/v1/traces",
            ),
            (
                "[tracing]\nendpoint = \"grpc://127.0.0.1:4317\"\n",
                "scheme must be https",
            ),
            (
                "[tracing]\nendpoint = \" https://collector.example:4317\"\n",
                "surrounding whitespace",
            ),
        ] {
            Jail::expect_with(|jail| {
                jail.clear_env();
                jail.create_file("config.toml", contents)?;
                let error = load("config.toml").expect_err("invalid tracing config must fail");
                assert!(error.contains(expected), "got: {error}");
                Ok(())
            });
        }
    }

    #[test]
    fn unknown_tracing_keys_are_rejected() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "config.toml",
                "[tracing]\nendpiont = \"http://localhost:4317\"\n",
            )?;
            let error = load("config.toml").expect_err("unknown tracing key must fail");
            assert!(error.contains("endpiont"), "got: {error}");
            Ok(())
        });
    }

    #[test]
    fn db_pool_max_zero_is_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file("config.toml", "[postgres]\ndb_pool_max = 0\n")?;
            let err = load("config.toml").expect_err("zero pool size must error");
            assert!(err.contains("db_pool_max"), "got: {err}");
            Ok(())
        });
    }
}
