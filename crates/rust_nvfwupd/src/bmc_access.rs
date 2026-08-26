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

//! BMC endpoint access module for nvfwupd CLI.
//!
//! Provides connectivity to BMC servers via HTTPS/Redfish protocol.
//! Supports three access modes:
//! - **Login**: Direct BMC access with IP, username, and password credentials
//! - **PortForward**: BMC access through localhost port forwarding
//! - **NVSwitch**: NVUE REST API access for GB200/GB300/VR switch platforms
//!
//! The [`BmcAccess::get_bmc_access`] factory method probes the target and
//! returns an appropriately configured instance.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use tokio_util::io::ReaderStream;

use crate::expected_inventory;
use crate::ssh_options::{
    self, SSH_HOST_KEY_MODE_ARG, SSH_HOST_KEY_MODE_DISABLED, SSH_HOST_KEY_MODE_STRICT,
    SSH_HOST_KEY_MODE_TOFU, SSH_KNOWN_HOSTS_ARG,
};
use crate::ssh_transport;
use crate::util::{BailAction, TraceFlags, Util};
use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default HTTP request timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 900;
const VERIFY_TLS_ARG: &str = "verify_tls";
const BMC_CA_CERT_ARG: &str = "bmc_ca_cert";

/// Timeout for reachability probes (seconds).
const REACHABILITY_TIMEOUT_SECS: u64 = 120;

/// Per-attempt SSH connect/auth timeout for switch firmware uploads.
const SWITCH_SSH_CONNECT_TIMEOUT_SECS: u64 = 30;

/// Per-attempt setup command and SFTP transfer timeout for switch firmware uploads.
const SWITCH_SSH_UPLOAD_TIMEOUT_SECS: u64 = 300;

/// Delay between retryable switch SSH upload attempts.
const SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS: u64 = 10;

/// Worst-case duration of one switch SSH upload attempt.
const SWITCH_SSH_UPLOAD_ATTEMPT_TIMEOUT_SECS: u64 =
    SWITCH_SSH_CONNECT_TIMEOUT_SECS + SWITCH_SSH_UPLOAD_TIMEOUT_SECS;

/// Controls multipart upload failure reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartUploadOptions {
    /// When true, report upload failures through the normal CLI/RMS error path.
    pub bail_on_failure: bool,
}

impl Default for MultipartUploadOptions {
    fn default() -> Self {
        Self {
            bail_on_failure: true,
        }
    }
}

/// Total retry window for switch SSH setup after NVUE firmware actions.
const SWITCH_SSH_UPLOAD_RETRY_WINDOW_SECS: u64 =
    (2 * SWITCH_SSH_UPLOAD_ATTEMPT_TIMEOUT_SECS) + SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS;

/// Ensure the retry window can cover two worst-case attempts plus one retry delay.
const _: () = assert!(
    SWITCH_SSH_UPLOAD_RETRY_WINDOW_SECS
        >= (2 * SWITCH_SSH_UPLOAD_ATTEMPT_TIMEOUT_SECS) + SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS
);

/// Switch server types that use the NVSwitch access path.
const SWITCH_SERVER_TYPES: &[&str] = &["gb200switch", "gb300switch", "vrnvl72switch"];

/// Platform-to-model mapping used during Chassis discovery.
const PLATFORM_DICT: &[(&str, &str)] = &[
    ("HGX_BMC_0", "NVIDIA HGX H100"),
    ("Bluefield_BMC", "Bluefield_BMC"),
];

/// Long-lived runtime for the temporary synchronous HTTP bridge.
///
/// Reqwest response bodies may depend on Hyper tasks spawned while sending the
/// request, so this runtime must outlive individual `send()` calls.
static HTTP_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("nvfwupd-http-bridge")
        .build()
        .expect("failed to build temporary HTTP runtime")
});

#[cfg(test)]
const TEST_TIMEOUT_SENTINEL_SECS: u64 = 4321;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct MegmeetSystemInfo {
    model: Option<String>,
    partnumber: Option<String>,
    serialnumber: Option<String>,
}

#[derive(Debug, Clone)]
struct MegmeetFruCandidate {
    fru_name: String,
    model: Option<String>,
    partnumber: Option<String>,
    serialnumber: Option<String>,
}

fn error_chain_message(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        let detail = next.to_string();
        if !detail.is_empty() && !message.contains(&detail) {
            message.push_str(": ");
            message.push_str(&detail);
        }
        source = next.source();
    }
    NvUtils::sanitize_log(&message)
}

fn is_tls_certificate_verification_detail(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("invalid peer certificate")
        || lower.contains("certificate verify failed")
        || lower.contains("certificate verification")
        || lower.contains("unknownissuer")
        || lower.contains("unknown issuer")
        || lower.contains("notvalidforname")
        || lower.contains("not valid for name")
        || lower.contains("self-signed")
        || lower.contains("self signed")
        || (lower.contains("certificate")
            && (lower.contains("invalid")
                || lower.contains("verify")
                || lower.contains("expired")
                || lower.contains("issuer")))
}

fn tls_certificate_verification_message_for_detail(url: &str, detail: &str) -> Option<String> {
    if !is_tls_certificate_verification_detail(detail) {
        return None;
    }

    Some(format!(
        "Connection Error: TLS certificate verification failed while connecting to {}. \
         Certificate validation is disabled by default; pass verify_tls=true to use built-in \
         public roots plus the host trust store, or pass bmc_ca_cert=/path/to/ca.pem to trust a \
         per-command CA. \
         Details: {}",
        NvUtils::sanitize_log(url),
        NvUtils::sanitize_log(detail)
    ))
}

fn is_tls_certificate_verification_message(message: &str) -> bool {
    message.contains("TLS certificate verification failed")
}

fn connection_error_message_for_detail(url: &str, detail: &str) -> String {
    tls_certificate_verification_message_for_detail(url, detail)
        .unwrap_or_else(|| "Connection Error: Failed to connect with the system.".to_string())
}

fn connection_error_message(url: &str, error: &dyn std::error::Error) -> String {
    let detail = error_chain_message(error);
    connection_error_message_for_detail(url, &detail)
}

#[cfg(test)]
static LAST_TEST_DISPATCH_GET_TIMEOUT_SECS: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// AccessType
// ---------------------------------------------------------------------------

/// Discriminates the BMC connection strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    /// Direct HTTPS login with username/password.
    Login,
    /// Port-forwarded access (connects to `localhost:<port>`).
    PortForward,
    /// NVUE REST API access for NVSwitch platforms.
    NVSwitch,
}

// ---------------------------------------------------------------------------
// BmcAccess
// ---------------------------------------------------------------------------

/// Unified BMC access handle.
///
/// Wraps an HTTP client and connection metadata required to communicate with
/// a BMC endpoint over Redfish or NVUE REST APIs.
#[derive(Clone)]
pub struct BmcAccess {
    /// BMC IP address (may include brackets for IPv6).
    pub ip: String,
    /// Login username.
    pub user: String,
    /// Login password.
    pub password: String,
    /// Discovered or user-supplied model name.
    pub model: String,
    /// Part number reported by the BMC.
    pub partnumber: String,
    /// Serial number reported by the BMC.
    pub serialnumber: String,
    /// Optional port for port-forward access.
    pub port: String,
    /// Server type string (e.g. "gb200switch").
    pub servertype: String,
    /// Fully-qualified base URL (e.g. `https://10.0.0.1`).
    pub base_url: String,
    /// Transport scheme, either "https" or "http".
    pub transport_type: String,
    /// Which access strategy is in use.
    pub access_type: AccessType,
    /// Optional known_hosts file for SSH/SFTP host-key verification.
    pub ssh_known_hosts: Option<String>,
    /// SSH/SFTP host-key verification mode: `disabled`, `tofu`, or `strict`.
    pub ssh_host_key_mode: String,
    /// Shared reqwest async client.
    pub client: Client,
}

impl fmt::Debug for BmcAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BmcAccess")
            .field("ip", &self.ip)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("model", &self.model)
            .field("partnumber", &self.partnumber)
            .field("serialnumber", &self.serialnumber)
            .field("port", &self.port)
            .field("servertype", &self.servertype)
            .field("base_url", &self.base_url)
            .field("transport_type", &self.transport_type)
            .field("access_type", &self.access_type)
            .field("ssh_known_hosts", &self.ssh_known_hosts)
            .field("ssh_host_key_mode", &self.ssh_host_key_mode)
            .field("client", &"<reqwest::Client>")
            .finish()
    }
}

fn pretty_4space(value: &Value) -> String {
    let buf = Vec::new();
    let fmt = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(buf, fmt);
    value.serialize(&mut ser).unwrap_or(());
    String::from_utf8(ser.into_inner()).unwrap_or_default()
}

fn pretty_4space_redacted(value: &Value) -> String {
    pretty_4space(&NvUtils::redact_secret_json_value(value))
}

/// Classify switch SSH/SFTP setup failures that are worth retrying.
///
/// The switch can transiently refuse sessions while NVUE firmware actions reboot
/// or restart services. Permanent command failures are intentionally excluded.
fn switch_ssh_upload_error_is_retriable(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("connect timed out")
        || lower.contains("ssh connect failed")
        || lower.contains("ssh auth timed out")
        || lower.contains("channel open failed")
        || lower.contains("subsystem request failed")
        || lower.contains("session creation failed")
        || lower.contains("timed out after")
}

/// Return true only when the retry window can cover the delay and one full attempt.
///
/// This prevents starting a final upload attempt that cannot fit inside the
/// configured retry budget.
fn switch_ssh_upload_has_retry_budget(
    elapsed: Duration,
    retry_interval: Duration,
    retry_window: Duration,
) -> bool {
    let next_attempt_budget =
        retry_interval + Duration::from_secs(SWITCH_SSH_UPLOAD_ATTEMPT_TIMEOUT_SECS);
    elapsed + next_attempt_budget <= retry_window
}

/// Quote a value for the remote switch shell.
///
/// Remote setup commands interpolate paths, so single quotes are escaped using
/// the standard POSIX `'\''` sequence.
fn switch_shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Build and validate the owned remote path used for switch firmware uploads.
///
/// Only an absolute staging directory plus a plain filename is accepted. This
/// keeps cleanup scoped to the file for this upload and avoids shell path escape.
fn switch_remote_upload_path(remote_dir: &str, remote_filename: &str) -> Result<String, String> {
    let remote_dir = remote_dir.trim();
    let normalized_remote_dir = remote_dir.trim_end_matches('/');
    if normalized_remote_dir.is_empty() {
        return Err("refusing to upload into an empty or root remote_dir".to_string());
    }
    if !normalized_remote_dir.starts_with('/') {
        return Err("refusing to upload into a relative remote_dir".to_string());
    }

    let remote_filename = remote_filename.trim();
    if remote_filename.is_empty() {
        return Err("refusing to upload with an empty remote filename".to_string());
    }
    if remote_filename == "."
        || remote_filename == ".."
        || remote_filename.contains('/')
        || remote_filename.contains('\\')
        || remote_filename.chars().any(char::is_control)
    {
        return Err("refusing to upload with an unsafe remote filename".to_string());
    }

    Ok(format!("{normalized_remote_dir}/{remote_filename}"))
}

impl BmcAccess {
    fn clean_redfish_str(value: Option<&str>) -> Option<String> {
        let value = value?.trim();
        if value.is_empty() {
            return None;
        }
        Some(value.to_string())
    }

    fn clean_redfish_value(value: Option<&Value>) -> Option<String> {
        match value? {
            Value::String(s) => Self::clean_redfish_str(Some(s)),
            Value::Null => None,
            other => Self::clean_redfish_str(Some(&other.to_string())),
        }
    }

    fn is_empty_redfish_str(value: Option<&str>) -> bool {
        let Some(value) = Self::clean_redfish_str(value) else {
            return true;
        };
        matches!(value.to_ascii_uppercase().as_str(), "N/A" | "NA")
    }

    /// Extract Megmeet PowerShelf identity data from chassis FRU blocks.
    ///
    /// Megmeet shelves can report `Model: N/A` at the chassis level and place
    /// useful identity in `Oem.InsydeFRUData.FRUInfo` Product/Board sections.
    fn get_megmeet_system_info_from_chassis(chassis_dict: &Value) -> MegmeetSystemInfo {
        let mut candidates = Vec::new();
        let fru_info = chassis_dict
            .pointer("/Oem/InsydeFRUData/FRUInfo")
            .and_then(Value::as_array);

        if let Some(fru_info) = fru_info {
            for fru_entry in fru_info {
                let Some(fru_obj) = fru_entry.as_object() else {
                    continue;
                };
                let fru_name = fru_obj
                    .get("Name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();

                for section_name in ["Product", "Board"] {
                    let Some(section) = fru_obj.get(section_name).and_then(Value::as_object) else {
                        continue;
                    };
                    let manufacturer = section
                        .get("Manufacturer")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    if !manufacturer.contains("megmeet") {
                        continue;
                    }

                    candidates.push(MegmeetFruCandidate {
                        fru_name: fru_name.clone(),
                        model: Self::clean_redfish_value(section.get("Name")),
                        partnumber: Self::clean_redfish_value(section.get("PartNumber")),
                        serialnumber: Self::clean_redfish_value(
                            section
                                .get("Serial")
                                .or_else(|| section.get("SerialNumber")),
                        ),
                    });
                }
            }
        }

        let model_candidate = candidates
            .iter()
            .find(|candidate| {
                candidate
                    .model
                    .as_deref()
                    .map(|model| model.to_ascii_lowercase().contains("mpsc"))
                    .unwrap_or(false)
            })
            .or_else(|| {
                candidates.iter().find(|candidate| {
                    matches!(candidate.fru_name.as_str(), "mcu" | "baseboard")
                        && candidate.model.is_some()
                })
            })
            .or_else(|| {
                candidates.iter().find(|candidate| {
                    !candidate.fru_name.starts_with("psu") && candidate.model.is_some()
                })
            });

        let mut info = MegmeetSystemInfo {
            model: model_candidate.and_then(|candidate| candidate.model.clone()),
            ..MegmeetSystemInfo::default()
        };

        let mut info_candidates: Vec<&MegmeetFruCandidate> = Vec::new();
        for fru_name in ["chassis", "mcu", "baseboard"] {
            info_candidates.extend(
                candidates
                    .iter()
                    .filter(|candidate| candidate.fru_name == fru_name),
            );
        }
        info_candidates.extend(candidates.iter().filter(|candidate| {
            !matches!(candidate.fru_name.as_str(), "chassis" | "mcu" | "baseboard")
                && !candidate.fru_name.starts_with("psu")
        }));

        info.partnumber = info_candidates
            .iter()
            .find_map(|candidate| candidate.partnumber.clone());
        info.serialnumber = info_candidates
            .iter()
            .find_map(|candidate| candidate.serialnumber.clone());
        info
    }

    // -----------------------------------------------------------------
    // Construction helpers
    // -----------------------------------------------------------------

    fn parse_bool_arg(arg_name: &str, value: &str) -> Result<bool, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!(
                "{arg_name} must be one of 1/0, true/false, yes/no, or on/off"
            )),
        }
    }

    fn should_verify_bmc_tls(
        verify_arg: Option<&str>,
        ca_cert_path: Option<&str>,
    ) -> Result<bool, String> {
        match verify_arg {
            Some(value) => Self::parse_bool_arg(VERIFY_TLS_ARG, value),
            // TLS verification is wired and ready in NVFWUPD; default enablement
            // is pending final customer discussions.
            None => Ok(ca_cert_path
                .map(str::trim)
                .is_some_and(|path| !path.is_empty())),
        }
    }

    /// Build the host-key verification policy used for switch SSH/SFTP access.
    pub(crate) fn ssh_host_key_policy(&self) -> ssh_transport::SshHostKeyPolicy {
        ssh_options::host_key_policy(self.ssh_known_hosts.clone(), &self.ssh_host_key_mode)
    }

    fn add_bmc_ca_cert(
        mut builder: reqwest::ClientBuilder,
        ca_cert_path: Option<&str>,
    ) -> Result<reqwest::ClientBuilder, String> {
        let Some(path) = ca_cert_path.map(str::trim).filter(|path| !path.is_empty()) else {
            return Ok(builder);
        };

        let pem = std::fs::read(path)
            .map_err(|e| format!("Failed to read {BMC_CA_CERT_ARG} {path}: {e}"))?;
        let cert = reqwest::Certificate::from_pem(&pem)
            .map_err(|e| format!("Failed to parse {BMC_CA_CERT_ARG} {path}: {e}"))?;
        builder = builder.add_root_certificate(cert);
        Ok(builder)
    }

    fn build_client_from_tls_options(
        timeout_secs: u64,
        verify_arg: Option<&str>,
        ca_cert_path: Option<&str>,
    ) -> Result<Client, String> {
        let verify_tls = Self::should_verify_bmc_tls(verify_arg, ca_cert_path)?;
        let ca_cert_path = ca_cert_path.map(str::trim).filter(|path| !path.is_empty());
        if !verify_tls && ca_cert_path.is_some() {
            return Err(format!(
                "{BMC_CA_CERT_ARG} cannot be used when {VERIFY_TLS_ARG}=false; \
                 remove {BMC_CA_CERT_ARG} or enable TLS verification"
            ));
        }

        let mut builder = Client::builder().timeout(Duration::from_secs(timeout_secs));
        if verify_tls {
            builder = Self::add_bmc_ca_cert(builder, ca_cert_path)?;
        } else {
            builder = builder.danger_accept_invalid_certs(true);
        }

        builder
            .build()
            .map_err(|e| format!("Failed to build HTTP client: {e}"))
    }

    /// Build a [`reqwest::Client`] with certificate validation disabled by default.
    ///
    /// Operators can opt in by passing `verify_tls=true` in the target
    /// arguments. Passing `bmc_ca_cert=/path/to/ca.pem` also enables validation
    /// and adds that PEM CA as a trusted root.
    fn build_client(
        timeout_secs: u64,
        arg_dict: &HashMap<String, String>,
    ) -> Result<Client, String> {
        let verify_arg = arg_dict.get(VERIFY_TLS_ARG).map(String::as_str);
        let ca_cert_path = arg_dict.get(BMC_CA_CERT_ARG).map(String::as_str);
        Self::build_client_from_tls_options(timeout_secs, verify_arg, ca_cert_path)
    }

    /// Temporary bridge for sync callers while the wider target call graph is
    /// migrated to the async BmcAccess API.
    fn block_on_http<F>(future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            return std::thread::scope(|scope| {
                scope
                    .spawn(move || HTTP_RUNTIME.block_on(future))
                    .join()
                    .expect("temporary HTTP runtime thread panicked")
            });
        }

        HTTP_RUNTIME.block_on(future)
    }

    /// Create a `BmcAccess` configured for direct login.
    fn new_login(arg_dict: &HashMap<String, String>) -> Result<Self, String> {
        let client = Self::build_client(DEFAULT_TIMEOUT_SECS, arg_dict)?;
        let ssh_host_key_options = ssh_options::parse_ssh_options(arg_dict)?;
        let mut ip = arg_dict.get("ip").cloned().unwrap_or_default();

        // IPv6 addresses require brackets.
        if ip.contains(':') && !ip.starts_with('[') {
            ip = format!("[{}]", ip);
        }

        let transport_type = "https".to_string();
        let base_url = format!("{transport_type}://{ip}");

        Ok(Self {
            ip,
            user: arg_dict.get("user").cloned().unwrap_or_default(),
            password: arg_dict.get("password").cloned().unwrap_or_default(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port: String::new(),
            servertype: arg_dict.get("servertype").cloned().unwrap_or_default(),
            base_url,
            transport_type,
            access_type: AccessType::Login,
            ssh_known_hosts: ssh_host_key_options.known_hosts,
            ssh_host_key_mode: ssh_host_key_options.mode,
            client,
        })
    }

    /// Create a `BmcAccess` configured for port-forwarded access.
    fn new_port_forward(arg_dict: &HashMap<String, String>) -> Result<Self, String> {
        let mut access = Self::new_login(arg_dict)?;
        access.access_type = AccessType::PortForward;
        access.port = arg_dict.get("port").cloned().unwrap_or_default();

        // Port-forward uses localhost:<port> as the transport address.
        if !access.port.is_empty() {
            access.base_url = format!("https://{}:{}", access.ip, access.port);
        }

        Ok(access)
    }

    /// Create a `BmcAccess` configured for NVSwitch NVUE access.
    fn new_nvswitch(arg_dict: &HashMap<String, String>) -> Result<Self, String> {
        let client = Self::build_client(1200, arg_dict)?;
        let ssh_host_key_options = ssh_options::parse_ssh_options(arg_dict)?;
        let mut ip = arg_dict.get("ip").cloned().unwrap_or_default();

        if ip.contains(':') && !ip.starts_with('[') {
            ip = format!("[{}]", ip);
        }

        let port = arg_dict.get("port").cloned().unwrap_or_default();
        let base_url = if !port.is_empty() {
            format!("https://{}:{}", ip, port)
        } else {
            format!("https://{}", ip)
        };

        Ok(Self {
            ip,
            user: arg_dict.get("user").cloned().unwrap_or_default(),
            password: arg_dict.get("password").cloned().unwrap_or_default(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port,
            servertype: arg_dict.get("servertype").cloned().unwrap_or_default(),
            base_url,
            transport_type: "https".to_string(),
            access_type: AccessType::NVSwitch,
            ssh_known_hosts: ssh_host_key_options.known_hosts,
            ssh_host_key_mode: ssh_host_key_options.mode,
            client,
        })
    }

    // -----------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------

    /// Create a [`BmcAccess`] instance from command-line target arguments.
    ///
    /// Target arguments are `key=value` pairs (e.g. `ip=10.0.0.1 user=admin
    /// password=secret`).  The method probes the target using the following
    /// strategy:
    ///
    /// 1. If `servertype` is a known switch type, use [`AccessType::NVSwitch`].
    /// 2. Otherwise try, in order: port-forward, login, NVSwitch.
    ///
    /// # Returns
    /// `Ok((bmc_access, Option<server_type>))` on success, or `Err(message)`
    /// on failure.
    pub fn get_bmc_access_sync(
        target_args: &[String],
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
    ) -> Result<(Self, Option<String>), String> {
        Self::block_on_http(Self::get_bmc_access(target_args, trace, json_dict))
    }

    /// Async variant of [`get_bmc_access_sync`](Self::get_bmc_access_sync).
    pub async fn get_bmc_access(
        target_args: &[String],
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
    ) -> Result<(Self, Option<String>), String> {
        // Parse key=value pairs.
        let mut arg_dict = HashMap::new();

        // Python parity: the user-facing error diagnostic uses a version of
        // the target string that (a) always contains `UpdateDelay=<n>` just
        // before `servertype=` (Python stores UpdateDelay defaulting to 0),
        // and (b) is run through the sanitize_log filter so IPs/users/
        // passwords show as XXXX when SANITIZE_LOG is enabled (the default).
        //
        // Python populates `Util.sanitize_config` from the CLI target args
        // inside `BMCAccess.__init__` (see bmc_access.py:55-68); do the
        // equivalent here so NvUtils::sanitize_log(...) has the set of
        // credential values to redact. Without this the sanitizer has an
        // empty filter list and would pass credential strings through
        // unchanged.
        if NvUtils::is_sanitize() {
            let log_config: Vec<String> = target_args.to_vec();
            let parsed = NvUtils::get_log_sanitize_config(Some(&log_config), json_dict.is_some());
            NvUtils::set_sanitize_config(parsed);
        }

        let display_target_str = {
            let mut parts: Vec<String> = Vec::new();
            let mut servertype_val: Option<String> = None;
            let mut has_update_delay = false;
            for each in target_args {
                if let Some(rest) = each.strip_prefix("servertype=") {
                    servertype_val = Some(rest.to_string());
                    continue;
                }
                if each.starts_with("UpdateDelay=") {
                    has_update_delay = true;
                }
                parts.push(NvUtils::redact_secret_key_value_arg(each));
            }
            if !has_update_delay {
                parts.push("UpdateDelay=0".to_string());
            }
            if let Some(st) = servertype_val {
                parts.push(format!("servertype={}", st));
            }
            NvUtils::sanitize_log(&parts.join(" "))
        };

        for each in target_args {
            let tokens: Vec<&str> = each.splitn(2, '=').collect();
            if tokens.len() < 2 {
                let msg = format!(
                    "Error: invalid target arguments:{}, token length: {}",
                    display_target_str,
                    tokens.len()
                );
                return Err(msg);
            }
            arg_dict.insert(tokens[0].to_ascii_lowercase(), tokens[1].to_string());
        }

        let server_type = arg_dict.get("servertype").map(|s| s.to_lowercase());

        // Check for deprecated NVOS servertype.
        if server_type.as_deref() == Some("nvos") {
            return Err("Error: The NVOS servertype was deprecated in NVFWUPD. \
                 Please use the appropriate servertype for your system."
                .to_string());
        }

        let mut bmc_access: Option<BmcAccess> = None;

        if let Some(ref st) = server_type {
            if SWITCH_SERVER_TYPES.contains(&st.as_str()) {
                // Direct NVSwitch path.
                let mut access = Self::new_nvswitch(&arg_dict)?;
                access.validate_ip()?;
                let (reachable, message) = access.is_reachable(trace).await;
                if !reachable {
                    if !trace.json_mode && is_tls_certificate_verification_message(&message) {
                        println!("{message}");
                    }
                    // Python parity: print the "Failed to connect to target"
                    // line before the caller prints "Unable to access BMC".
                    if !trace.json_mode {
                        println!("Error: Failed to connect to target: {display_target_str}");
                    }
                    if is_tls_certificate_verification_message(&message) {
                        return Err(format!(
                            "{message}\nError: Failed to connect to target: {display_target_str}"
                        ));
                    }
                    return Err(format!(
                        "Error: Failed to connect to target: {display_target_str}"
                    ));
                }
                bmc_access = Some(access);
            }
        }

        // General probe order: PortForward -> Login -> NVSwitch
        let mut connection_failure_message: Option<String> = None;
        if bmc_access.is_none() {
            let access_builders: Vec<(
                &str,
                fn(&HashMap<String, String>) -> Result<BmcAccess, String>,
                usize,
            )> = vec![
                (
                    "BMCPortForwardAccess",
                    Self::new_port_forward
                        as fn(&HashMap<String, String>) -> Result<BmcAccess, String>,
                    2,
                ),
                (
                    "BMCLoginAccess",
                    Self::new_login as fn(&HashMap<String, String>) -> Result<BmcAccess, String>,
                    3,
                ),
                (
                    "GB200NVSwitchAccess",
                    Self::new_nvswitch as fn(&HashMap<String, String>) -> Result<BmcAccess, String>,
                    3,
                ),
            ];

            for (name, builder, expected_args) in &access_builders {
                // Skip port-forward if no port argument given.
                if *name == "BMCPortForwardAccess" && !arg_dict.contains_key("port") {
                    continue;
                }

                let mut access = match builder(&arg_dict) {
                    Ok(a) => a,
                    Err(e) => {
                        connection_failure_message = Some(e);
                        break;
                    }
                };

                // Validate IP.
                if access.validate_ip().is_err() {
                    continue;
                }

                // For port-forward, validate the port is connectable.
                if *name == "BMCPortForwardAccess" {
                    if access.validate_port().await.is_err() {
                        continue;
                    }
                }

                let (reachable, message) = access.is_reachable(trace).await;
                if !reachable {
                    if is_tls_certificate_verification_message(&message) {
                        connection_failure_message = Some(message);
                        break;
                    }
                    continue;
                }

                if arg_dict.len() < *expected_args {
                    let target_str = display_target_str.as_str();
                    tracing::debug!(
                        cli_verbose = trace.cli_verbose,
                        json_mode = trace.json_mode,
                        "Error: invalid target arguments: {target_str}"
                    );
                    continue;
                }

                bmc_access = Some(access);
                break;
            }
        }

        // Python parity: print "Error: Failed to connect to target: ..."
        // before returning Err so the user sees the same diagnostic chain
        // Python emits. The caller then prints "Error : Unable to access BMC
        // ip=..." on top of it.
        let mut access = match bmc_access {
            Some(a) => a,
            None => {
                if let Some(message) = connection_failure_message {
                    if !trace.json_mode {
                        println!("{message}");
                        println!("Error: Failed to connect to target: {display_target_str}");
                    }
                    return Err(format!(
                        "{message}\nError: Failed to connect to target: {display_target_str}"
                    ));
                }
                if !trace.json_mode {
                    println!("Error: Failed to connect to target: {display_target_str}");
                }
                return Err(format!(
                    "Error: Failed to connect to target: {display_target_str}"
                ));
            }
        };

        // Store the server type.
        if let Some(ref st) = server_type {
            access.servertype = st.clone();
        }

        // Get system info.
        let status = access.get_system_info(trace, json_dict).await;
        if !status {
            if !trace.json_mode {
                println!("Error: Failed to connect to target: {display_target_str}");
            }
            return Err(format!(
                "Error: Failed to connect to target: {display_target_str}"
            ));
        }

        Ok((access, server_type))
    }

    // -----------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------

    /// Validate that the stored IP address is a valid IP.
    fn validate_ip(&self) -> Result<(), String> {
        let test_ip = self.ip.replace(['[', ']'], "");
        test_ip
            .parse::<IpAddr>()
            .map_err(|_| format!("invalid IP address: {}", self.ip))?;
        Ok(())
    }

    /// Validate that the port-forward port is connectable.
    async fn validate_port(&self) -> Result<(), String> {
        if self.port.is_empty() {
            return Err("No port specified".to_string());
        }
        let port_num: u16 = self
            .port
            .parse()
            .map_err(|_| format!("invalid port: {}", self.port))?;
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::TcpStream::connect(("127.0.0.1", port_num)),
        )
        .await
        .map_err(|e| format!("Socket error for ({}, {}): {e}", self.ip, self.port))?
        .map_err(|e| format!("Socket error for ({}, {}): {e}", self.ip, self.port))?;
        Ok(())
    }

    /// Update the transport type (e.g. fall back to `http`).
    fn update_transport_type(&mut self, transport_type: &str) {
        self.transport_type = transport_type.to_string();
        self.base_url = format!("{}://{}", self.transport_type, self.ip);
        if !self.port.is_empty() {
            self.base_url = format!("{}://{}:{}", self.transport_type, self.ip, self.port);
        }
    }

    // -----------------------------------------------------------------
    // Reachability
    // -----------------------------------------------------------------

    /// Check whether the BMC endpoint is reachable.
    ///
    /// For [`AccessType::NVSwitch`] targets, probes the NVUE `/nvue_v1/platform`
    /// endpoint.  For all others, probes `/redfish/v1/Chassis`.  On HTTPS
    /// failure, automatically falls back to HTTP and retries.
    ///
    /// # Returns
    /// `(reachable, message)` where `message` describes the failure when
    /// `reachable` is `false`.
    pub fn is_reachable_sync(&mut self, trace: TraceFlags) -> (bool, String) {
        Self::block_on_http(self.is_reachable(trace))
    }

    /// Async reachability check.
    pub async fn is_reachable(&mut self, trace: TraceFlags) -> (bool, String) {
        match self.access_type {
            AccessType::NVSwitch => self.is_reachable_nvswitch_async(trace).await,
            _ => self.is_reachable_redfish_async(trace).await,
        }
    }

    /// Redfish reachability check (Login / PortForward).
    ///
    /// On HTTPS failure, falls back to HTTP.  When the HTTP probe succeeds
    /// the transport type is **persisted** via [`update_transport_type`] so
    /// that all subsequent requests use `http://` — matching the Python
    /// `BMCLoginAccess.is_reachable` behaviour.
    async fn is_reachable_redfish_async(&mut self, trace: TraceFlags) -> (bool, String) {
        let url = format!("{}/redfish/v1/Chassis", self.base_url);
        let result = self
            .client
            .get(&url)
            .basic_auth(&self.user, Some(&self.password))
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(REACHABILITY_TIMEOUT_SECS))
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => (true, String::new()),
            https_result => {
                let https_failure_message = match &https_result {
                    Err(e) => tls_certificate_verification_message_for_detail(
                        &url,
                        &error_chain_message(e),
                    ),
                    _ => None,
                };
                if let Some(message) = https_failure_message {
                    return (false, message);
                }

                // Try HTTP fallback.
                let http_url = url.replacen("https://", "http://", 1);
                let fallback = self
                    .client
                    .get(&http_url)
                    .basic_auth(&self.user, Some(&self.password))
                    .header("Content-Type", "application/json")
                    .timeout(Duration::from_secs(REACHABILITY_TIMEOUT_SECS))
                    .send()
                    .await;

                match fallback {
                    Ok(resp) if resp.status().is_success() => {
                        // Persist the HTTP transport so all subsequent
                        // requests go through http:// instead of https://.
                        self.update_transport_type("http");
                        (true, String::new())
                    }
                    _ => {
                        tracing::debug!(
                            cli_verbose = trace.cli_verbose,
                            json_mode = trace.json_mode,
                            "Failed to connect to the system via HTTPS and HTTP"
                        );
                        (false, "Failed to connect to the system".to_string())
                    }
                }
            }
        }
    }

    /// NVUE reachability check (NVSwitch).
    ///
    /// Python parity (I3/I4 diagnostic verbosity): on a non-200 response,
    /// Python's `dispatch_nvue_request_get` prints the response body as
    /// pretty-printed JSON (`json.dumps(indent=4, sort_keys=False)`), or
    /// the raw text if JSON parsing fails. On a connection exception,
    /// Python prints `"Connection Error: Failed to connect with the system."`.
    /// Mirror both behaviors here so credential / routing failures produce
    /// the same user-facing diagnostic output that Python does.
    async fn is_reachable_nvswitch_async(&self, trace: TraceFlags) -> (bool, String) {
        let platform_url = format!("https://{}/nvue_v1/platform", self.ip);
        let result = self
            .client
            .get(&platform_url)
            .basic_auth(&self.user, Some(&self.password))
            .timeout(Duration::from_secs(1200))
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                if status_code == 200 {
                    match resp.text().await {
                        Ok(text) => match serde_json::from_str::<Value>(&text) {
                            Ok(_) => (true, String::new()),
                            Err(e) => {
                                let text = NvUtils::sanitize_log(&text);
                                tracing::debug!(
                                    cli_verbose = trace.cli_verbose,
                                    json_mode = trace.json_mode,
                                    "{text}, {e}"
                                );
                                (false, format!("JSON decode error: {e}"))
                            }
                        },
                        Err(e) => (false, format!("Failed to read response: {e}")),
                    }
                } else {
                    let status_line = format!(
                        "{}, {}, {}",
                        platform_url,
                        resp.status(),
                        resp.status().canonical_reason().unwrap_or("Unknown")
                    );
                    tracing::debug!(
                        cli_verbose = trace.cli_verbose,
                        json_mode = trace.json_mode,
                        "{status_line}"
                    );

                    // Python reads response.text, then either pretty-prints
                    // parsed JSON (indent=4, sort_keys=False) or prints the
                    // raw text. Match that output shape.
                    if let Ok(body) = resp.text().await {
                        match serde_json::from_str::<Value>(&body) {
                            Ok(parsed) => {
                                // `serde_json::to_string_pretty` uses 2-space
                                // indent by default. Python uses 4-space.
                                println!("{}", pretty_4space_redacted(&parsed));
                            }
                            Err(_) => {
                                println!("{}", NvUtils::sanitize_log(&body));
                            }
                        }
                    }
                    (false, format!("HTTP {}", status_code))
                }
            }
            Err(e) => {
                // Return transport-level failures to the caller so probing can
                // de-duplicate diagnostics across access strategies.
                let message = connection_error_message(&platform_url, &e);
                (false, message)
            }
        }
    }

    // -----------------------------------------------------------------
    // System info
    // -----------------------------------------------------------------

    /// Query the BMC for system model, part number, and serial number.
    ///
    /// Populates `self.model`, `self.partnumber`, and `self.serialnumber`.
    pub fn get_system_info_sync(
        &mut self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
    ) -> bool {
        Self::block_on_http(self.get_system_info(trace, json_dict))
    }

    /// Async system information query.
    pub async fn get_system_info(
        &mut self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
    ) -> bool {
        match self.access_type {
            AccessType::NVSwitch => self.get_system_info_nvswitch_async(trace, json_dict).await,
            _ => self.get_system_info_redfish_async(trace, json_dict).await,
        }
    }

    /// NVSwitch system info via NVUE `/nvue_v1/platform`.
    async fn get_system_info_nvswitch_async(
        &mut self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
    ) -> bool {
        let (reachable, _) = self.is_reachable_nvswitch_async(trace).await;
        if !reachable {
            return false;
        }

        let url = format!("{}/nvue_v1/platform", self.base_url);
        let (status, resp) = self.dispatch_get_async(trace, &url, true).await;
        if status {
            self.model = resp
                .get("product-name")
                .and_then(Value::as_str)
                .unwrap_or("N/A")
                .to_string();
            // Fallback to system-type for VRNVL144/VRNVL72.
            if self.model == "N/A" {
                self.model = resp
                    .get("system-type")
                    .and_then(Value::as_str)
                    .unwrap_or("N/A")
                    .to_string();
            }
            self.partnumber = resp
                .get("part-number")
                .and_then(Value::as_str)
                .unwrap_or("N/A")
                .to_string();
            self.serialnumber = resp
                .get("serial-number")
                .and_then(Value::as_str)
                .unwrap_or("N/A")
                .to_string();
        }
        let resp_log = NvUtils::redact_secret_json_value(&resp);
        tracing::debug!(
            cli_verbose = trace.cli_verbose,
            json_mode = trace.json_mode,
            "System info /nvue_v1/platform = {resp_log}"
        );
        status
    }

    /// Redfish system info via Chassis URIs.
    async fn get_system_info_redfish_async(
        &mut self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
    ) -> bool {
        // Check NVFWUPD_DUT_MAP environment override.
        if let Ok(dut_map) = std::env::var("NVFWUPD_DUT_MAP") {
            if !dut_map.contains(':') {
                Util::bail_nvfwupd(
                    1,
                    &format!("Invalid configuration {dut_map} in NVFWUPD_DUT_MAP."),
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                );
                return false;
            }
            let parts: Vec<&str> = dut_map.splitn(2, ':').collect();
            self.model = parts[0].to_string();
            self.partnumber = "N/A".to_string();
            self.serialnumber = "N/A".to_string();
            return true;
        }

        // Try /redfish/v1/Chassis/DGX first.
        let dgx_url = format!("{}/redfish/v1/Chassis/DGX", self.base_url);
        let (status, mut chassis_dict) = self.dispatch_get_async(trace, &dgx_url, true).await;
        let mut curr_platform: Option<String> = None;
        let mut megmeet_system_info: Option<MegmeetSystemInfo> = None;

        if status {
            curr_platform = Self::clean_redfish_value(chassis_dict.get("Model"));
        } else {
            // Enumerate Chassis members.
            let chassis_url = format!("{}/redfish/v1/Chassis/", self.base_url);
            let (chassis_status, chassis_resp) =
                self.dispatch_get_async(trace, &chassis_url, true).await;

            if !chassis_status {
                Util::bail_nvfwupd(
                    1,
                    &format!("Unable to access BMC: {}", self.ip),
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                );
            }

            let members = chassis_resp
                .get("Members")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            let chassis_list: Vec<String> = members
                .iter()
                .filter_map(|m| {
                    m.get("@odata.id")
                        .and_then(Value::as_str)
                        .and_then(|s| s.split('/').last())
                        .map(String::from)
                })
                .collect();

            for (platform_key, model_name) in PLATFORM_DICT {
                if chassis_list.iter().any(|c| c == platform_key) {
                    let member_url = format!("{}/redfish/v1/Chassis/{platform_key}", self.base_url);
                    let (ok, member_dict) = self.dispatch_get_async(trace, &member_url, true).await;
                    if ok {
                        chassis_dict = member_dict;
                        let model_val = Self::clean_redfish_value(chassis_dict.get("Model"));
                        curr_platform = match model_val.as_deref() {
                            Some(m) if m.starts_with('$') => Some(model_name.to_string()),
                            Some(m) if !Self::is_empty_redfish_str(Some(m)) => Some(m.to_string()),
                            _ => None,
                        };
                        if !Self::is_empty_redfish_str(curr_platform.as_deref()) {
                            break;
                        }
                    }
                }
            }

            // Try BMC_0 fallback.
            if Self::is_empty_redfish_str(curr_platform.as_deref()) {
                let bmc0_url = format!("{}/redfish/v1/Chassis/BMC_0", self.base_url);
                let (ok, bmc0_dict) = self.dispatch_get_async(trace, &bmc0_url, true).await;
                if ok {
                    chassis_dict = bmc0_dict;
                    if let Some(model_val) = Self::clean_redfish_value(chassis_dict.get("Model")) {
                        for (platform_key, model_name) in PLATFORM_DICT {
                            let platform_name = platform_key
                                .split("_Management_Board")
                                .next()
                                .unwrap_or(platform_key);
                            if model_val.contains(platform_name) {
                                curr_platform = Some(model_name.to_string());
                                break;
                            }
                        }
                        if Self::is_empty_redfish_str(curr_platform.as_deref()) {
                            curr_platform = Some(model_val);
                        }
                    }
                }
            }

            // Try LiteOn Powershelf.
            if Self::is_empty_redfish_str(curr_platform.as_deref()) {
                let ps_url = format!("{}/redfish/v1/Chassis/powershelf", self.base_url);
                let (ok, ps_dict) = self.dispatch_get_async(trace, &ps_url, true).await;
                if ok {
                    chassis_dict = ps_dict;
                    curr_platform = Self::clean_redfish_value(chassis_dict.get("Model"));
                }
            }

            // Try Delta/Megmeet Powershelf.
            if Self::is_empty_redfish_str(curr_platform.as_deref()) {
                let ps_url = format!("{}/redfish/v1/Chassis/PowerShelf_0", self.base_url);
                let (ok, ps_dict) = self.dispatch_get_async(trace, &ps_url, true).await;
                if ok {
                    chassis_dict = ps_dict;
                    curr_platform = Self::clean_redfish_value(chassis_dict.get("Model"));
                    if Self::is_empty_redfish_str(curr_platform.as_deref()) {
                        let info = Self::get_megmeet_system_info_from_chassis(&chassis_dict);
                        curr_platform = info.model.clone();
                        megmeet_system_info = Some(info);
                    }
                }
            }

            // Megmeet backup URIs if PowerShelf_0 is absent or incomplete.
            if Self::is_empty_redfish_str(curr_platform.as_deref()) {
                for chassis_uri in [
                    "/redfish/v1/Chassis/MCU",
                    "/redfish/v1/Chassis/megmeet_baseboard",
                ] {
                    let url = format!("{}{}", self.base_url, chassis_uri);
                    let (ok, ps_dict) = self.dispatch_get_async(trace, &url, true).await;
                    if ok {
                        chassis_dict = ps_dict;
                        let info = Self::get_megmeet_system_info_from_chassis(&chassis_dict);
                        curr_platform = info.model.clone();
                        megmeet_system_info = Some(info);
                        if !Self::is_empty_redfish_str(curr_platform.as_deref()) {
                            break;
                        }
                    }
                }
            }
        }

        self.model = chassis_dict
            .get("Model")
            .and_then(Value::as_str)
            .unwrap_or("N/A")
            .to_string();
        self.partnumber = chassis_dict
            .get("PartNumber")
            .and_then(Value::as_str)
            .unwrap_or("N/A")
            .to_string();
        self.serialnumber = chassis_dict
            .get("SerialNumber")
            .and_then(Value::as_str)
            .unwrap_or("N/A")
            .to_string();

        if !Self::is_empty_redfish_str(curr_platform.as_deref()) {
            let platform = curr_platform.unwrap_or_default();
            self.model = platform;
            if megmeet_system_info.is_none()
                && (self.model.to_ascii_lowercase().contains("mpsc")
                    || self.model.to_ascii_lowercase().contains("megmeet"))
            {
                megmeet_system_info =
                    Some(Self::get_megmeet_system_info_from_chassis(&chassis_dict));
            }
            if let Some(info) = megmeet_system_info {
                if Self::is_empty_redfish_str(Some(&self.partnumber)) {
                    self.partnumber = info.partnumber.unwrap_or_else(|| "N/A".to_string());
                }
                if Self::is_empty_redfish_str(Some(&self.serialnumber)) {
                    self.serialnumber = info.serialnumber.unwrap_or_else(|| "N/A".to_string());
                }
            }
        } else {
            // Python returns True here with model/part/serial populated
            // from chassis_dict — it only debug-prints a warning.
            tracing::debug!(
                cli_verbose = trace.cli_verbose,
                json_mode = trace.json_mode,
                "Could not find Chassis URI containing BMC Model data"
            );
        }

        // Python writes this line to the verbose log FILE only (not stdout).
        // Marking it log_only keeps `-v` console output byte-exact with Python.
        tracing::debug!(
            cli_verbose = trace.cli_verbose,
            json_mode = trace.json_mode,
            log_only = true,
            "System info in Chassis - Model:{} Partno:{} Serialno:{}",
            self.model,
            self.partnumber,
            self.serialnumber
        );
        // Return true as long as we got *some* chassis data, even if
        // the platform name couldn't be resolved.  Matches Python
        // BMCLoginAccess.get_system_info which returns `status` (True
        // when any Chassis GET succeeded).
        true
    }

    // -----------------------------------------------------------------
    // Request dispatch
    // -----------------------------------------------------------------

    /// Dispatch a Redfish HTTP request to the BMC.
    ///
    /// Supports `GET`, `POST`, and `PATCH` methods. The full URL is formed
    /// by appending `url` to `self.base_url`.
    ///
    /// # Arguments
    /// * `method` - HTTP method (`"GET"`, `"POST"`, `"PATCH"`)
    /// * `url` - URI path (e.g. `/redfish/v1/Chassis`)
    /// * `input_data` - Optional body payload (JSON value or file path depending on method)
    /// * `json_prints` - When `Some`, errors are recorded in the JSON dict
    ///
    /// # Returns
    /// `(success, response_body)` where `response_body` is the parsed JSON.
    pub fn dispatch_request_sync(
        &self,
        method: &str,
        url: &str,
        input_data: Option<&Value>,
        json_prints: Option<&mut Value>,
    ) -> (bool, Value) {
        Self::block_on_http(self.dispatch_request(method, url, input_data, json_prints))
    }

    /// Async request dispatch using default timeout and no extra params.
    pub async fn dispatch_request(
        &self,
        method: &str,
        url: &str,
        input_data: Option<&Value>,
        json_prints: Option<&mut Value>,
    ) -> (bool, Value) {
        self.dispatch_request_full(
            method,
            url,
            input_data,
            None,
            DEFAULT_TIMEOUT_SECS,
            false,
            json_prints,
        )
        .await
    }

    /// Full-featured request dispatch with all parameters.
    ///
    /// # Arguments
    /// * `method` - HTTP method
    /// * `url` - URI path
    /// * `input_data` - Optional JSON body
    /// * `param_data` - Optional parameter payload (for PATCH, or multipart POST)
    /// * `time_out` - Timeout in seconds
    /// * `suppress_err` - When true, do not print errors on failure
    /// * `json_prints` - Optional JSON dict for recording errors
    pub fn dispatch_request_full_sync(
        &self,
        method: &str,
        url: &str,
        input_data: Option<&Value>,
        param_data: Option<&Value>,
        time_out: u64,
        suppress_err: bool,
        json_prints: Option<&mut Value>,
    ) -> (bool, Value) {
        Self::block_on_http(self.dispatch_request_full(
            method,
            url,
            input_data,
            param_data,
            time_out,
            suppress_err,
            json_prints,
        ))
    }

    /// Async full-featured request dispatch with all parameters.
    pub async fn dispatch_request_full(
        &self,
        method: &str,
        url: &str,
        input_data: Option<&Value>,
        param_data: Option<&Value>,
        time_out: u64,
        suppress_err: bool,
        json_prints: Option<&mut Value>,
    ) -> (bool, Value) {
        let full_url = format!("{}{}", self.base_url, url);
        let empty = Value::Object(serde_json::Map::new());

        match method {
            "GET" => {
                self.dispatch_get_full_async(&full_url, time_out, suppress_err, json_prints)
                    .await
            }
            "PATCH" => {
                self.dispatch_patch_async(
                    &full_url,
                    // Callers pass PATCH body as input_data; use param_data as
                    // fallback for backwards compatibility.
                    param_data.or(input_data),
                    time_out,
                    json_prints,
                )
                .await
            }
            "POST" => {
                self.dispatch_post_async(&full_url, input_data, param_data, time_out, json_prints)
                    .await
            }
            _ => {
                tracing::warn!("Unsupported HTTP method: {method}");
                (false, empty)
            }
        }
    }

    /// Async internal GET helper (no error reporting).
    async fn dispatch_get_async(
        &self,
        trace: TraceFlags,
        url: &str,
        suppress_err: bool,
    ) -> (bool, Value) {
        self.dispatch_get_with_timeout_async(trace, url, REACHABILITY_TIMEOUT_SECS, suppress_err)
            .await
    }

    /// Async internal GET helper with an explicit per-call timeout.
    async fn dispatch_get_with_timeout_async(
        &self,
        trace: TraceFlags,
        url: &str,
        time_out: u64,
        suppress_err: bool,
    ) -> (bool, Value) {
        #[cfg(test)]
        if time_out == TEST_TIMEOUT_SENTINEL_SECS {
            LAST_TEST_DISPATCH_GET_TIMEOUT_SECS.store(time_out, Ordering::SeqCst);
        }

        let empty = Value::Object(serde_json::Map::new());
        let result = self
            .client
            .get(url)
            .basic_auth(&self.user, Some(&self.password))
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(time_out))
            .send()
            .await;

        match result {
            Ok(resp) => {
                if resp.status().as_u16() == 200 {
                    match resp.text().await {
                        Ok(text) => match serde_json::from_str::<Value>(&text) {
                            Ok(val) => (true, val),
                            Err(e) => {
                                let text = NvUtils::sanitize_log(&text);
                                tracing::debug!(
                                    cli_verbose = trace.cli_verbose,
                                    json_mode = trace.json_mode,
                                    "{text}, {e}"
                                );
                                (false, empty)
                            }
                        },
                        Err(_) => (false, empty),
                    }
                } else {
                    if !suppress_err {
                        tracing::debug!(
                            cli_verbose = trace.cli_verbose,
                            json_mode = trace.json_mode,
                            "{}, {}, {}",
                            url,
                            resp.status(),
                            resp.status().canonical_reason().unwrap_or("Unknown")
                        );
                    }
                    (false, empty)
                }
            }
            Err(_) => (false, empty),
        }
    }

    /// Async GET with full error handling.
    async fn dispatch_get_full_async(
        &self,
        url: &str,
        time_out: u64,
        suppress_err: bool,
        json_prints: Option<&mut Value>,
    ) -> (bool, Value) {
        let empty = Value::Object(serde_json::Map::new());
        let result = self
            .client
            .get(url)
            .basic_auth(&self.user, Some(&self.password))
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(time_out))
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();

                if status_code == 200 {
                    match serde_json::from_str::<Value>(&body) {
                        Ok(val) => (true, val),
                        Err(e) => {
                            let body = NvUtils::sanitize_log(&body);
                            tracing::debug!("GET response JSON parse error: {body}, {e}");
                            (false, empty)
                        }
                    }
                } else {
                    if !suppress_err {
                        if let Ok(val) = serde_json::from_str::<Value>(&body) {
                            if json_prints.is_none() {
                                println!("{}", pretty_4space_redacted(&val));
                            }
                            return (false, val);
                        }
                        if let Some(jp) = json_prints {
                            if let Some(obj) = jp.as_object_mut() {
                                obj.insert("Error Code".to_string(), Value::Number(1.into()));
                            }
                        } else {
                            tracing::debug!("{}", NvUtils::sanitize_log(&body));
                        }
                    }
                    (false, empty)
                }
            }
            Err(e) => {
                let detail = error_chain_message(&e);
                if !suppress_err {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Error: Error sending HTTPs GET request to {url}: {detail}"),
                        BailAction::DoNothing,
                        json_prints.as_deref(),
                    );
                }
                (
                    false,
                    json!({
                        "error": "HTTPS GET request failed",
                        "method": "GET",
                        "url": NvUtils::sanitize_log(url),
                        "details": NvUtils::sanitize_log(&detail),
                    }),
                )
            }
        }
    }

    /// Async PATCH request dispatch.
    async fn dispatch_patch_async(
        &self,
        url: &str,
        param_data: Option<&Value>,
        time_out: u64,
        json_prints: Option<&mut Value>,
    ) -> (bool, Value) {
        let empty = Value::Object(serde_json::Map::new());
        let body_str = param_data
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default();

        // First attempt with If-Match: *
        let mut headers_with_match = true;

        loop {
            let mut req = self
                .client
                .patch(url)
                .basic_auth(&self.user, Some(&self.password))
                .header("Content-Type", "application/json")
                .timeout(Duration::from_secs(time_out))
                .body(body_str.clone());

            if headers_with_match {
                req = req.header("If-Match", "*");
            }

            match req.send().await {
                Ok(resp) => {
                    let status_code = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();

                    if [200, 201, 204, 400].contains(&status_code) {
                        let mut my_dict = empty.clone();
                        let mut status = true;
                        if !body.trim().is_empty() {
                            if let Ok(val) = serde_json::from_str::<Value>(&body) {
                                my_dict = val.clone();
                                if status_code == 400 {
                                    if let Some(err_code) =
                                        val.pointer("/error/code").and_then(Value::as_str)
                                    {
                                        if !err_code.contains("PatchValueAlreadyExists") {
                                            status = false;
                                        }
                                    }
                                }
                            }
                        }
                        return (status, my_dict);
                    }

                    if status_code == 412 {
                        // Retry without If-Match header.
                        headers_with_match = false;
                        continue;
                    }

                    return (false, empty);
                }
                Err(e) => {
                    Util::bail_nvfwupd(
                        1,
                        &format!("PATCH: Error sending HTTPs request: {e}"),
                        BailAction::DoNothing,
                        json_prints.as_deref(),
                    );
                    return (false, empty);
                }
            }
        }
    }

    /// Async POST request dispatch.
    async fn dispatch_post_async(
        &self,
        url: &str,
        input_data: Option<&Value>,
        param_data: Option<&Value>,
        time_out: u64,
        json_prints: Option<&mut Value>,
    ) -> (bool, Value) {
        let empty = Value::Object(serde_json::Map::new());

        if let Some(params) = param_data {
            // POST with JSON body.
            let result = self
                .client
                .post(url)
                .basic_auth(&self.user, Some(&self.password))
                .header("Content-Type", "application/json")
                .json(params)
                .timeout(Duration::from_secs(time_out))
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let status_code = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();
                    let my_dict = serde_json::from_str::<Value>(&body).unwrap_or(empty.clone());

                    if [200, 201, 202, 204].contains(&status_code) {
                        (true, my_dict)
                    } else {
                        if json_prints.is_none() {
                            println!("{}", pretty_4space_redacted(&my_dict));
                        }
                        Util::bail_nvfwupd(
                            1,
                            &format!("Error sending POST request: ({status_code})"),
                            BailAction::DoNothing,
                            json_prints.as_deref(),
                        );
                        (false, my_dict)
                    }
                }
                Err(e) => {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Error: Error sending HTTPs request: {e}"),
                        BailAction::DoNothing,
                        json_prints.as_deref(),
                    );
                    (false, empty)
                }
            }
        } else if input_data.is_none()
            || input_data
                .and_then(Value::as_str)
                .map(|s| s.is_empty())
                .unwrap_or(true)
        {
            // POST with no body (e.g. factory reset, power actions).
            let result = self
                .client
                .post(url)
                .basic_auth(&self.user, Some(&self.password))
                .timeout(Duration::from_secs(time_out))
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let status_code = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();
                    let my_dict = serde_json::from_str::<Value>(&body).unwrap_or(empty.clone());

                    if [200, 201, 202, 204].contains(&status_code) {
                        (true, my_dict)
                    } else {
                        if json_prints.is_none() {
                            println!("{}", pretty_4space_redacted(&my_dict));
                        }
                        Util::bail_nvfwupd(
                            1,
                            &format!("Error sending POST request: ({status_code})"),
                            BailAction::DoNothing,
                            json_prints.as_deref(),
                        );
                        (false, my_dict)
                    }
                }
                Err(e) => {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Error: Error sending HTTPs request: {e}"),
                        BailAction::DoNothing,
                        json_prints.as_deref(),
                    );
                    (false, empty)
                }
            }
        } else {
            // POST with binary file upload (input_data is treated as file path string).
            let file_path = input_data.and_then(Value::as_str).unwrap_or("");

            let file = match tokio::fs::File::open(file_path).await {
                Ok(file) => file,
                Err(e) => {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Failed to open file {file_path}: {e}"),
                        BailAction::DoNothing,
                        json_prints.as_deref(),
                    );
                    return (false, empty);
                }
            };
            let file_len = match file.metadata().await {
                Ok(metadata) => metadata.len(),
                Err(e) => {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Failed to read file metadata {file_path}: {e}"),
                        BailAction::DoNothing,
                        json_prints.as_deref(),
                    );
                    return (false, empty);
                }
            };
            let file_body = reqwest::Body::wrap_stream(ReaderStream::new(file));

            // Acquire auth token via /login first.
            let login_url = format!("{}/login", self.base_url);
            let login_body = serde_json::json!({
                "data": [self.user, self.password]
            });

            let login_resp = self
                .client
                .post(&login_url)
                .header("Content-Type", "application/json")
                .json(&login_body)
                .timeout(Duration::from_secs(30))
                .send()
                .await;

            let mut auth_token = String::new();
            if let Ok(resp) = login_resp {
                if resp.status().as_u16() == 200 {
                    if let Some(cookie) = resp.headers().get("Set-Cookie") {
                        if let Ok(cookie_str) = cookie.to_str() {
                            let re = regex::Regex::new(r"SESSION=(\w+);")
                                .unwrap_or_else(|_| regex::Regex::new(r"$^").unwrap());
                            if let Some(caps) = re.captures(cookie_str) {
                                auth_token = caps[1].to_string();
                            }
                        }
                    }
                } else {
                    Util::bail_nvfwupd(
                        1,
                        "Unable to get valid token for POST message",
                        BailAction::DoNothing,
                        json_prints.as_deref(),
                    );
                    return (false, empty);
                }
            }

            let mut req = self
                .client
                .post(url)
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", file_len.to_string())
                .body(file_body)
                .timeout(Duration::from_secs(time_out));

            if !auth_token.is_empty() {
                req = req.header("X-Auth-Token", &auth_token);
            }

            match req.send().await {
                Ok(resp) => {
                    let status_code = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();
                    let my_dict = serde_json::from_str::<Value>(&body).unwrap_or(empty.clone());
                    if [200, 201, 202, 204].contains(&status_code) {
                        (true, my_dict)
                    } else {
                        (false, my_dict)
                    }
                }
                Err(e) => {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Error: Error sending HTTPs request: {e}"),
                        BailAction::DoNothing,
                        json_prints.as_deref(),
                    );
                    (false, empty)
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // File upload
    // -----------------------------------------------------------------

    /// Upload a firmware file via HTTP POST with `application/octet-stream`.
    ///
    /// # Arguments
    /// * `url` - URI path for the upload endpoint
    /// * `input_data` - Path to the file to upload
    /// * `time_out` - Timeout in seconds
    /// * `json_output` - Optional JSON dict for error recording
    ///
    /// # Returns
    /// `(success, response_body)`
    pub fn dispatch_file_upload_sync(
        &self,
        url: &str,
        input_data: &str,
        time_out: u64,
        json_output: Option<&Mutex<Value>>,
        parallel_update: bool,
        extra_headers: Option<&HashMap<String, String>>,
    ) -> (bool, Value) {
        Self::block_on_http(self.dispatch_file_upload(
            url,
            input_data,
            time_out,
            json_output,
            parallel_update,
            extra_headers,
        ))
    }

    /// Async firmware upload via HTTP POST with `application/octet-stream`.
    pub async fn dispatch_file_upload(
        &self,
        url: &str,
        input_data: &str,
        time_out: u64,
        json_output: Option<&Mutex<Value>>,
        parallel_update: bool,
        extra_headers: Option<&HashMap<String, String>>,
    ) -> (bool, Value) {
        let empty = Value::Object(serde_json::Map::new());

        let file = match tokio::fs::File::open(input_data).await {
            Ok(file) => file,
            Err(e) => {
                let msg = format!("Failed to open or read given file {input_data} error: ({e})");
                if json_output.is_some() {
                    tracing::warn!("{}", NvUtils::sanitize_log(&msg));
                } else {
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        &msg,
                        BailAction::DoNothing,
                        None,
                        parallel_update,
                    );
                }
                return (
                    false,
                    serde_json::json!({"error": "Failed to read given file"}),
                );
            }
        };
        let file_len = match file.metadata().await {
            Ok(metadata) => metadata.len(),
            Err(e) => {
                let msg = format!("Failed to read given file metadata {input_data} error: ({e})");
                if json_output.is_some() {
                    tracing::warn!("{}", NvUtils::sanitize_log(&msg));
                } else {
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        &msg,
                        BailAction::DoNothing,
                        None,
                        parallel_update,
                    );
                }
                return (
                    false,
                    serde_json::json!({"error": "Failed to read given file metadata"}),
                );
            }
        };
        let file_body = reqwest::Body::wrap_stream(ReaderStream::new(file));

        let mut req = self
            .client
            .post(&format!("{}{}", self.base_url, url))
            .basic_auth(&self.user, Some(&self.password))
            .header("Content-Type", "application/octet-stream")
            .header("Expect", "100-continue")
            .header("Content-Length", file_len.to_string())
            .timeout(Duration::from_secs(time_out))
            .body(file_body);

        if let Some(extra) = extra_headers {
            for (k, v) in extra {
                req = req.header(k.as_str(), v.as_str());
            }
        }

        match req.send().await {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();

                if status_code == 405 {
                    if json_output.is_some() {
                        tracing::warn!("HTTPPushURI is not supported for this platform.");
                    } else {
                        Util::bail_nvfwupd_threadsafe(
                            1,
                            "HTTPPushURI is not supported for this platform.",
                            BailAction::DoNothing,
                            None,
                            parallel_update,
                        );
                    }
                    return (
                        false,
                        serde_json::json!({"error": "HTTPPushURI is not supported for this platform"}),
                    );
                }

                let response_dict = serde_json::from_str::<Value>(&body).unwrap_or(empty.clone());

                if status_code == 200 || status_code == 202 {
                    (true, response_dict)
                } else {
                    (false, response_dict)
                }
            }
            Err(e) => (
                false,
                serde_json::json!({"error": "Request failed", "details": e.to_string()}),
            ),
        }
    }

    /// Build the `UpdateParameters` multipart part from file or inline JSON.
    async fn build_update_params_part(
        upd_params_file: Option<&str>,
        updparams_json: Option<&str>,
        parallel_update: bool,
        bail_on_failure: bool,
    ) -> Result<Option<reqwest::multipart::Part>, ()> {
        if let Some(params_file) = upd_params_file {
            match tokio::fs::read(params_file).await {
                Ok(params_bytes) => {
                    let params_name = Path::new(params_file)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("UpdateParameters.json")
                        .to_string();
                    Ok(Some(
                        reqwest::multipart::Part::bytes(params_bytes)
                            .file_name(params_name)
                            .mime_str("application/json")
                            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(vec![])),
                    ))
                }
                Err(e) => {
                    Self::report_multipart_preflight_failure(
                        &format!("Failed to read params file {params_file}: {e}"),
                        bail_on_failure,
                        parallel_update,
                    );
                    Err(())
                }
            }
        } else if let Some(json_str) = updparams_json {
            Ok(Some(
                reqwest::multipart::Part::bytes(json_str.as_bytes().to_vec())
                    .mime_str("application/json")
                    .unwrap_or_else(|_| reqwest::multipart::Part::bytes(vec![])),
            ))
        } else {
            Ok(None)
        }
    }

    fn report_multipart_preflight_failure(
        message: &str,
        bail_on_failure: bool,
        parallel_update: bool,
    ) {
        if bail_on_failure {
            Util::bail_nvfwupd_threadsafe(1, message, BailAction::DoNothing, None, parallel_update);
        } else {
            tracing::warn!("{message}");
        }
    }

    /// Perform a multipart firmware upload with default behavior.
    ///
    /// The default follows the current Python CLI behavior:
    /// `UpdateParameters` before `UpdateFile`, with failures reported through
    /// the normal nvfwupd error path.
    pub fn multipart_file_upload_sync(
        &self,
        url: &str,
        pkg_file: &str,
        upd_params_file: Option<&str>,
        time_out: u64,
        updparams_json: Option<&str>,
        oem_params_file: Option<&str>,
        oemparams_json: Option<&str>,
        json_prints: Option<&Mutex<Value>>,
        parallel_update: bool,
    ) -> (bool, Value) {
        Self::block_on_http(self.multipart_file_upload_with_options(
            url,
            pkg_file,
            upd_params_file,
            time_out,
            updparams_json,
            oem_params_file,
            oemparams_json,
            json_prints,
            parallel_update,
            MultipartUploadOptions::default(),
        ))
    }

    /// Perform a multipart firmware upload with caller-selected failure
    /// reporting behavior.
    #[allow(clippy::too_many_arguments)]
    pub fn multipart_file_upload_sync_with_options(
        &self,
        url: &str,
        pkg_file: &str,
        upd_params_file: Option<&str>,
        time_out: u64,
        updparams_json: Option<&str>,
        oem_params_file: Option<&str>,
        oemparams_json: Option<&str>,
        json_prints: Option<&Mutex<Value>>,
        parallel_update: bool,
        options: MultipartUploadOptions,
    ) -> (bool, Value) {
        Self::block_on_http(self.multipart_file_upload_with_options(
            url,
            pkg_file,
            upd_params_file,
            time_out,
            updparams_json,
            oem_params_file,
            oemparams_json,
            json_prints,
            parallel_update,
            options,
        ))
    }

    /// Async multipart firmware upload with default behavior.
    #[allow(clippy::too_many_arguments)]
    pub async fn multipart_file_upload(
        &self,
        url: &str,
        pkg_file: &str,
        upd_params_file: Option<&str>,
        time_out: u64,
        updparams_json: Option<&str>,
        oem_params_file: Option<&str>,
        oemparams_json: Option<&str>,
        json_prints: Option<&Mutex<Value>>,
        parallel_update: bool,
    ) -> (bool, Value) {
        self.multipart_file_upload_with_options(
            url,
            pkg_file,
            upd_params_file,
            time_out,
            updparams_json,
            oem_params_file,
            oemparams_json,
            json_prints,
            parallel_update,
            MultipartUploadOptions::default(),
        )
        .await
    }

    /// Async multipart firmware upload with caller-selected failure-reporting
    /// behavior.
    #[allow(clippy::too_many_arguments)]
    pub async fn multipart_file_upload_with_options(
        &self,
        url: &str,
        pkg_file: &str,
        upd_params_file: Option<&str>,
        time_out: u64,
        updparams_json: Option<&str>,
        oem_params_file: Option<&str>,
        oemparams_json: Option<&str>,
        json_prints: Option<&Mutex<Value>>,
        parallel_update: bool,
        options: MultipartUploadOptions,
    ) -> (bool, Value) {
        let empty = Value::Null;

        let pkg_reader = match tokio::fs::File::open(pkg_file).await {
            Ok(file) => file,
            Err(e) => {
                Self::report_multipart_preflight_failure(
                    &format!("Failed to read package file {pkg_file}: {e}"),
                    options.bail_on_failure,
                    parallel_update,
                );
                return (false, empty);
            }
        };
        let pkg_len = match pkg_reader.metadata().await {
            Ok(metadata) => metadata.len(),
            Err(e) => {
                Self::report_multipart_preflight_failure(
                    &format!("Failed to read package file metadata {pkg_file}: {e}"),
                    options.bail_on_failure,
                    parallel_update,
                );
                return (false, empty);
            }
        };

        let pkg_filename = Path::new(pkg_file)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("firmware.bin")
            .to_string();

        let pkg_body = reqwest::Body::wrap_stream(ReaderStream::new(pkg_reader));
        let update_file_part = reqwest::multipart::Part::stream_with_length(pkg_body, pkg_len)
            .file_name(pkg_filename)
            .mime_str("application/octet-stream")
            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(vec![]));

        let update_params_part = match Self::build_update_params_part(
            upd_params_file,
            updparams_json,
            parallel_update,
            options.bail_on_failure,
        )
        .await
        {
            Ok(p) => p,
            Err(()) => return (false, empty),
        };

        // Build form in the platform-supported order.
        let mut form = reqwest::multipart::Form::new();

        if let Some(part) = update_params_part {
            form = form.part("UpdateParameters", part);
        }
        form = form.part("UpdateFile", update_file_part);

        // Add OemParameters (always last).
        if let Some(oem_file) = oem_params_file {
            match tokio::fs::read(oem_file).await {
                Ok(oem_bytes) => {
                    let oem_name = Path::new(oem_file)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("OemParameters.json")
                        .to_string();
                    form = form.part(
                        "OemParameters",
                        reqwest::multipart::Part::bytes(oem_bytes)
                            .file_name(oem_name)
                            .mime_str("application/json")
                            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(vec![])),
                    );
                }
                Err(e) => {
                    Self::report_multipart_preflight_failure(
                        &format!("Failed to read OEM params file {oem_file}: {e}"),
                        options.bail_on_failure,
                        parallel_update,
                    );
                    return (false, empty);
                }
            }
        } else if let Some(json_str) = oemparams_json {
            form = form.part(
                "OemParameters",
                reqwest::multipart::Part::bytes(json_str.as_bytes().to_vec())
                    .mime_str("application/json")
                    .unwrap_or_else(|_| reqwest::multipart::Part::bytes(vec![])),
            );
        }

        let full_url = format!("{}{}", self.base_url, url);
        let result = self
            .client
            .post(&full_url)
            .basic_auth(&self.user, Some(&self.password))
            .multipart(form)
            .timeout(Duration::from_secs(time_out))
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();

                let response_dict = match serde_json::from_str::<Value>(&body) {
                    Ok(val) => val,
                    Err(_) => {
                        let body = NvUtils::sanitize_log(&body);
                        tracing::debug!(
                            "Could not decode update response body: response text: {} status: {}",
                            body,
                            status_code
                        );
                        if options.bail_on_failure {
                            Util::bail_nvfwupd_threadsafe(
                                1,
                                &format!(
                                    "Could not decode update response body: \
                                     response text: {body} status: {status_code}"
                                ),
                                BailAction::DoNothing,
                                None,
                                parallel_update,
                            );
                        }
                        return (false, empty);
                    }
                };

                if status_code == 200 || status_code == 202 {
                    (true, response_dict)
                } else {
                    if json_prints.is_none() && options.bail_on_failure {
                        println!("{}", pretty_4space_redacted(&response_dict));
                    }
                    if options.bail_on_failure {
                        Util::bail_nvfwupd_threadsafe(
                            1,
                            &format!("Error sending POST request: ({status_code})"),
                            BailAction::DoNothing,
                            None,
                            parallel_update,
                        );
                    }
                    (false, response_dict)
                }
            }
            Err(e) => {
                if options.bail_on_failure {
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        &format!("Error: Error sending HTTPs request: {e}"),
                        BailAction::DoNothing,
                        None,
                        parallel_update,
                    );
                }
                (false, empty)
            }
        }
    }

    // -----------------------------------------------------------------
    // Firmware inventory
    // -----------------------------------------------------------------

    /// Retrieve firmware inventory from the BMC.
    ///
    /// For [`AccessType::NVSwitch`] targets, queries the NVUE firmware endpoint.
    /// For all others, enumerates the Redfish `FirmwareInventory` collection.
    ///
    /// # Returns
    /// `(success, error_code, inventory_map)` where `inventory_map` keys are
    /// URI strings and values are JSON objects with firmware details.
    pub fn get_firmware_inventory_sync(
        &self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
        model: Option<&str>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        Self::block_on_http(self.get_firmware_inventory(trace, json_dict, model))
    }

    /// Async firmware inventory retrieval.
    pub async fn get_firmware_inventory(
        &self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
        model: Option<&str>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        match self.access_type {
            AccessType::NVSwitch => {
                self.get_firmware_inventory_nvswitch_async(trace, json_dict)
                    .await
            }
            _ => {
                self.get_firmware_inventory_redfish_async(trace, json_dict, model)
                    .await
            }
        }
    }

    /// Return AP names advertised by the firmware inventory source.
    ///
    /// Redfish BMC targets read only the FirmwareInventory collection and use
    /// its `Members` leaves. NVSwitch targets use their single NVUE firmware
    /// endpoint because they do not expose the same Redfish collection path.
    pub async fn get_expected_inventory_ap_names(
        &self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
    ) -> (bool, i32, Vec<String>) {
        match self.access_type {
            AccessType::NVSwitch => {
                let (ok, err_code, inventory) = self
                    .get_firmware_inventory_nvswitch_async(trace, json_dict)
                    .await;
                let names = expected_inventory::present_ap_names(&inventory);
                let normalized_err = if ok { err_code } else { 1 };
                (ok, normalized_err, names)
            }
            _ => {
                let fw_inv_uri = format!(
                    "{}/redfish/v1/UpdateService/FirmwareInventory",
                    self.base_url
                );
                let (status, collection) = self
                    .dispatch_get_with_timeout_async(
                        trace,
                        &fw_inv_uri,
                        DEFAULT_TIMEOUT_SECS,
                        false,
                    )
                    .await;
                if !status {
                    return (false, 1, Vec::new());
                }

                let names = expected_inventory::present_ap_names_from_collection(&collection);
                (true, 0, names)
            }
        }
    }

    /// NVSwitch firmware inventory via NVUE.
    async fn get_firmware_inventory_nvswitch_async(
        &self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        let mut inv_dict = serde_json::Map::new();
        let inv_url = format!("{}/nvue_v1/platform/firmware", self.base_url);
        let (status, resp) = self.dispatch_get_async(trace, &inv_url, false).await;

        if status {
            if let Some(obj) = resp.as_object() {
                for (ap_name, ap_data) in obj {
                    let version = ap_data
                        .get("actual-firmware")
                        .or_else(|| ap_data.get("version"))
                        .or_else(|| ap_data.get("Version"))
                        .and_then(Value::as_str)
                        .unwrap_or("N/A");
                    let mut entry = serde_json::json!({
                        "Id": ap_name,
                        "Version": version,
                        "Updateable": true,
                    });
                    if let Some(health) = ap_data
                        .pointer("/Status/Health")
                        .or_else(|| ap_data.get("Health"))
                        .and_then(Value::as_str)
                    {
                        entry["Status"] = serde_json::json!({ "Health": health });
                    }
                    inv_dict.insert(ap_name.clone(), entry);
                }
            }
        }

        (status, 0, inv_dict)
    }

    /// Redfish firmware inventory.
    async fn get_firmware_inventory_redfish_async(
        &self,
        trace: TraceFlags,
        json_dict: Option<&mut Value>,
        model: Option<&str>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        let mut inv_dict = serde_json::Map::new();
        let mut inv_error = 0i32;
        let mut status = false;

        let fw_inv_uri = format!(
            "{}/redfish/v1/UpdateService/FirmwareInventory",
            self.base_url
        );
        let (members, collection_response) = self
            .get_resource_members_with_response(trace, &fw_inv_uri, DEFAULT_TIMEOUT_SECS)
            .await;
        let oem_inv_dict = Self::get_oem_firmware_inventory(&collection_response);

        if members.is_empty() {
            if !oem_inv_dict.is_empty() {
                return (true, 0, oem_inv_dict);
            }
            tracing::debug!(
                cli_verbose = trace.cli_verbose,
                json_mode = trace.json_mode,
                "Firmware Inventory returned by target is empty"
            );
            return (false, 1, inv_dict);
        }

        if !oem_inv_dict.is_empty()
            && members
                .iter()
                .all(|inv_url| oem_inv_dict.contains_key(inv_url))
        {
            return (true, 0, oem_inv_dict);
        }

        for inv_url in &members {
            let full_url = format!("{}{}", self.base_url, inv_url);
            let (ok, fd_dict) = self.dispatch_get_async(trace, &full_url, true).await;
            if ok {
                inv_dict.insert(inv_url.clone(), fd_dict);
            } else {
                inv_error = 1;
                tracing::debug!(
                    cli_verbose = trace.cli_verbose,
                    json_mode = trace.json_mode,
                    "error fetching {inv_url}"
                );
            }
        }

        if !oem_inv_dict.is_empty() {
            for (inv_url, fd_dict) in oem_inv_dict {
                if !inv_dict.contains_key(&inv_url) {
                    inv_dict.insert(inv_url, fd_dict);
                }
            }
            if members.iter().all(|inv_url| inv_dict.contains_key(inv_url)) {
                inv_error = 0;
            }
        }

        // Add LiteOn PowerUnits inventory for PowerShelf targets.
        let model_hint = model.unwrap_or(self.model.as_str());
        let model_lower = model_hint.to_ascii_lowercase();
        let is_liteon_powershelf = model_lower.contains("liteon")
            || model_lower.starts_with("pf-1333")
            || model_lower.starts_with("pf-1114");

        if is_liteon_powershelf {
            if let Some(powerunits) = self.get_liteon_powerunits_inventory_async(trace).await {
                inv_dict.extend(powerunits);
            }
        }

        if !inv_dict.is_empty() {
            status = true;
        }

        (status, inv_error, inv_dict)
    }

    /// Extract FirmwareInventory entries embedded in collection `Oem` data.
    fn get_oem_firmware_inventory(collection_response: &Value) -> serde_json::Map<String, Value> {
        let mut inv_dict = serde_json::Map::new();
        let Some(oem_dict) = collection_response.get("Oem").and_then(Value::as_object) else {
            return inv_dict;
        };

        for vendor_data in oem_dict.values().filter_map(Value::as_object) {
            let Some(firmware_inventory) = vendor_data
                .get("FirmwareInventory")
                .and_then(Value::as_array)
            else {
                continue;
            };

            for firmware_device in firmware_inventory {
                let Some(device_dict) = firmware_device.as_object() else {
                    continue;
                };
                let inv_uri = device_dict
                    .get("DataSourceUri")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .or_else(|| {
                        device_dict
                            .get("Name")
                            .and_then(Value::as_str)
                            .filter(|name| !name.is_empty())
                            .map(|name| {
                                format!("/redfish/v1/UpdateService/FirmwareInventory/{name}")
                            })
                    });
                let has_version = device_dict
                    .get("Version")
                    .map(|version| !version.is_null())
                    .unwrap_or(false);

                if let Some(inv_uri) = inv_uri {
                    if has_version {
                        inv_dict.insert(inv_uri, firmware_device.clone());
                    }
                }
            }
        }

        inv_dict
    }

    /// Get software inventory from BMC Redfish (e.g. IST test vector versions).
    ///
    /// Returns a map of SoftwareInventory member URI → full response dict.
    /// Uses `suppress_err` so missing/unsupported endpoints don't fail the caller.
    /// Always returns a `Map` (possibly empty) — matches Python behavior.
    pub fn get_software_inventory_sync(
        &self,
        trace: TraceFlags,
        _json_dict: Option<&mut Value>,
    ) -> serde_json::Map<String, Value> {
        Self::block_on_http(self.get_software_inventory(trace, _json_dict))
    }

    /// Async software inventory retrieval.
    pub async fn get_software_inventory(
        &self,
        trace: TraceFlags,
        _json_dict: Option<&mut Value>,
    ) -> serde_json::Map<String, Value> {
        // NVSwitch targets don't support software inventory — return empty map.
        if self.access_type == AccessType::NVSwitch {
            return serde_json::Map::new();
        }
        let sw_inv_url = format!(
            "{}/redfish/v1/UpdateService/SoftwareInventory",
            self.base_url
        );
        let members = self.get_resource_members(trace, &sw_inv_url).await;
        let mut inv_dict = serde_json::Map::new();
        for inv_url in &members {
            let full_url = format!("{}{}", self.base_url, inv_url);
            let (ok, fd_dict) = self.dispatch_get_async(trace, &full_url, true).await;
            if ok {
                inv_dict.insert(inv_url.clone(), fd_dict);
            }
        }
        inv_dict
    }

    /// Get Redfish resource members from a collection URI.
    fn get_resource_members_sync(&self, trace: TraceFlags, url: &str) -> Vec<String> {
        Self::block_on_http(self.get_resource_members(trace, url))
    }

    async fn get_resource_members(&self, trace: TraceFlags, url: &str) -> Vec<String> {
        let (members, _) = self
            .get_resource_members_with_response(trace, url, REACHABILITY_TIMEOUT_SECS)
            .await;
        members
    }

    async fn get_resource_members_with_response(
        &self,
        trace: TraceFlags,
        url: &str,
        time_out: u64,
    ) -> (Vec<String>, Value) {
        let (status, resp) = self
            .dispatch_get_with_timeout_async(trace, url, time_out, false)
            .await;
        if !status {
            return (Vec::new(), Value::Object(serde_json::Map::new()));
        }

        let members = resp
            .get("Members")
            .and_then(Value::as_array)
            .map(|members| {
                members
                    .iter()
                    .filter_map(|m| m.get("@odata.id").and_then(Value::as_str).map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        (members, resp)
    }

    /// Get Chassis members from Redfish.
    pub fn get_chassis_members_sync(&self, trace: TraceFlags) -> Vec<String> {
        let url = format!("{}/redfish/v1/Chassis", self.base_url);
        self.get_resource_members_sync(trace, &url)
    }

    /// Async Chassis members query.
    pub async fn get_chassis_members(&self, trace: TraceFlags) -> Vec<String> {
        let url = format!("{}/redfish/v1/Chassis", self.base_url);
        self.get_resource_members(trace, &url).await
    }

    /// Get Systems members from Redfish.
    pub fn get_systems_members_sync(&self, trace: TraceFlags) -> Vec<String> {
        let url = format!("{}/redfish/v1/Systems", self.base_url);
        self.get_resource_members_sync(trace, &url)
    }

    /// Async Systems members query.
    pub async fn get_systems_members(&self, trace: TraceFlags) -> Vec<String> {
        let url = format!("{}/redfish/v1/Systems", self.base_url);
        self.get_resource_members(trace, &url).await
    }

    /// Query LiteOn PowerUnits inventory for powershelf targets.
    async fn get_liteon_powerunits_inventory_async(
        &self,
        trace: TraceFlags,
    ) -> Option<serde_json::Map<String, Value>> {
        let powerunits_uri = format!(
            "{}/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            self.base_url
        );
        let (status, resp) = self.dispatch_get_async(trace, &powerunits_uri, true).await;
        if !status {
            return None;
        }

        let members = resp.get("Members").and_then(Value::as_array)?;
        let mut result = serde_json::Map::new();

        for member in members {
            if let Some(device_uri) = member.get("@odata.id").and_then(Value::as_str) {
                let full_url = format!("{}{}", self.base_url, device_uri);
                let (ok, device_resp) = self.dispatch_get_async(trace, &full_url, true).await;
                if ok {
                    if let Some(version) = device_resp.get("Version").and_then(Value::as_str) {
                        if version != "N/A" {
                            result.insert(
                                device_uri.to_string(),
                                serde_json::json!({"Version": version}),
                            );
                        }
                    }
                }
            }
        }

        Some(result)
    }

    // -----------------------------------------------------------------
    // NVSwitch REST helpers
    // -----------------------------------------------------------------

    /// Send a GET request to the NVUE REST endpoint (NVSwitch access).
    pub fn dispatch_rest_request_get_sync(
        &self,
        trace: TraceFlags,
        url: &str,
        time_out: u64,
        print_json: Option<&mut Value>,
    ) -> (bool, Value) {
        Self::block_on_http(self.dispatch_rest_request_get(trace, url, time_out, print_json))
    }

    /// Async GET request to the NVUE REST endpoint.
    pub async fn dispatch_rest_request_get(
        &self,
        trace: TraceFlags,
        url: &str,
        time_out: u64,
        print_json: Option<&mut Value>,
    ) -> (bool, Value) {
        let transport_url = format!("{}{}", self.base_url, url);
        self.dispatch_get_with_timeout_async(trace, &transport_url, time_out, false)
            .await
    }

    /// Send a POST request to the NVUE REST endpoint (NVSwitch access).
    pub fn dispatch_rest_request_post_sync(
        &self,
        trace: TraceFlags,
        url: &str,
        json_data: &Value,
        time_out: u64,
        print_json: Option<&mut Value>,
    ) -> (bool, Value, String) {
        Self::block_on_http(
            self.dispatch_rest_request_post(trace, url, json_data, time_out, print_json),
        )
    }

    /// Async POST request to the NVUE REST endpoint.
    pub async fn dispatch_rest_request_post(
        &self,
        trace: TraceFlags,
        url: &str,
        json_data: &Value,
        time_out: u64,
        print_json: Option<&mut Value>,
    ) -> (bool, Value, String) {
        let transport_url = format!("{}{}", self.base_url, url);
        let empty = Value::Object(serde_json::Map::new());

        let result = self
            .client
            .post(&transport_url)
            .basic_auth(&self.user, Some(&self.password))
            .header("Content-Type", "application/json")
            .json(json_data)
            .timeout(Duration::from_secs(time_out))
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                let resp_dict = serde_json::from_str::<Value>(&body).unwrap_or(empty);

                if (200..300).contains(&status_code) {
                    (true, resp_dict, body)
                } else {
                    let resp_dict_log = NvUtils::redact_secret_json_value(&resp_dict);
                    tracing::debug!(
                        cli_verbose = trace.cli_verbose,
                        json_mode = trace.json_mode,
                        "resp_dict = {resp_dict_log}"
                    );
                    tracing::debug!(
                        cli_verbose = trace.cli_verbose,
                        json_mode = trace.json_mode,
                        "POST: Error sending request: status {status_code}"
                    );
                    Util::bail_nvfwupd(
                        1,
                        &format!("Error sending POST request: ({status_code})"),
                        BailAction::DoNothing,
                        print_json.as_deref(),
                    );
                    (false, resp_dict, body)
                }
            }
            Err(e) => {
                if let Some(jp) = print_json {
                    if let Some(obj) = jp.as_object_mut() {
                        obj.entry("Error")
                            .or_insert_with(|| Value::Array(vec![]))
                            .as_array_mut()
                            .map(|arr| {
                                arr.push(Value::String(
                                    "Connection Error: Failed to connect with the system."
                                        .to_string(),
                                ))
                            });
                        obj.insert("Error Code".to_string(), Value::Number(1.into()));
                    }
                } else {
                    tracing::warn!("Connection Error: Failed to connect with the system.");
                }
                (false, empty, String::new())
            }
        }
    }

    /// Check a NVUE job status by ID.
    pub fn get_job_status_sync(
        &self,
        trace: TraceFlags,
        job_id: &str,
        json_dict: Option<&mut Value>,
    ) -> (bool, Value) {
        Self::block_on_http(self.get_job_status(trace, job_id, json_dict))
    }

    /// Async NVUE job status query.
    pub async fn get_job_status(
        &self,
        trace: TraceFlags,
        job_id: &str,
        json_dict: Option<&mut Value>,
    ) -> (bool, Value) {
        let task_url = format!("/nvue_v1/action/{job_id}");
        self.dispatch_rest_request_get(trace, &task_url, 1200, json_dict)
            .await
    }

    /// Poll the NVSwitch system after reboot to verify it has come back up.
    ///
    /// Polls for up to `reboot_eta` minutes (default 4), waiting for the
    /// NVUE firmware endpoint to respond.
    ///
    /// # Returns
    /// `true` if the system responded after a detected reboot.
    pub fn get_system_rebooted_status_sync(&self, reboot_eta: u64) -> bool {
        Self::block_on_http(self.get_system_rebooted_status(reboot_eta))
    }

    /// Async reboot-status poll for NVSwitch systems.
    pub async fn get_system_rebooted_status(&self, reboot_eta: u64) -> bool {
        let transport_url = format!("{}/nvue_v1/platform/firmware", self.base_url);
        let polling_timeout = Duration::from_secs(reboot_eta * 60);
        let poll_interval = Duration::from_secs(30);
        let start = std::time::Instant::now();
        let mut system_rebooted = false;

        loop {
            let result = self
                .client
                .get(&transport_url)
                .basic_auth(&self.user, Some(&self.password))
                .timeout(Duration::from_secs(30))
                .send()
                .await;

            match result {
                Ok(_) => {
                    if !system_rebooted {
                        tokio::time::sleep(poll_interval).await;
                        if start.elapsed() >= polling_timeout {
                            break;
                        }
                    } else {
                        return true;
                    }
                }
                Err(_) => {
                    // Timeout / connection error means system is rebooting.
                    system_rebooted = true;
                    if start.elapsed() >= polling_timeout {
                        break;
                    }
                }
            }
        }

        false
    }

    /// Upload a file to the switch via SFTP (SSH).
    ///
    /// Returns the remote file path on success, or `None` on failure.
    pub fn scp_upload(
        &self,
        local_path: &str,
        remote_dir: &str,
        remote_filename: &str,
        print_json: bool,
    ) -> Option<String> {
        Self::block_on_http(self.scp_upload_async(
            local_path,
            remote_dir,
            remote_filename,
            print_json,
        ))
    }

    /// Async SFTP upload helper used by switch firmware workflows.
    pub async fn scp_upload_async(
        &self,
        local_path: &str,
        remote_dir: &str,
        remote_filename: &str,
        print_json: bool,
    ) -> Option<String> {
        self.scp_upload_async_result(local_path, remote_dir, remote_filename, print_json)
            .await
            .ok()
    }

    /// Async SFTP upload helper that preserves the failure reason for hosted callers.
    pub async fn scp_upload_async_result(
        &self,
        local_path: &str,
        remote_dir: &str,
        remote_filename: &str,
        print_json: bool,
    ) -> std::result::Result<String, String> {
        let ip_clean = self.ip.replace('[', "").replace(']', "");
        let remote_dir = remote_dir.trim();
        let remote_path = switch_remote_upload_path(remote_dir, remote_filename)?;
        let setup_commands = vec![
            format!("mkdir -p {}", switch_shell_quote(remote_dir)),
            format!("rm -f {}", switch_shell_quote(&remote_path)),
        ];
        let local_path_buf = Path::new(local_path).to_path_buf();
        let started = Instant::now();
        let retry_window = Duration::from_secs(SWITCH_SSH_UPLOAD_RETRY_WINDOW_SECS);
        let retry_interval = Duration::from_secs(SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS);
        let mut attempt = 0_u32;

        loop {
            attempt += 1;
            match ssh_transport::upload_file_with_setup_async(
                &ip_clean,
                22,
                &self.user,
                &self.password,
                SWITCH_SSH_CONNECT_TIMEOUT_SECS,
                SWITCH_SSH_UPLOAD_TIMEOUT_SECS,
                &setup_commands,
                local_path_buf.clone(),
                remote_path.clone(),
                self.ssh_host_key_policy(),
            )
            .await
            {
                Ok(()) => {
                    if !print_json {
                        println!("Update file {} was uploaded successfully", local_path);
                    }
                    return Ok(remote_path);
                }
                Err(e) => {
                    let elapsed = started.elapsed();
                    let retryable = switch_ssh_upload_error_is_retriable(&e);
                    if !retryable
                        || !switch_ssh_upload_has_retry_budget(
                            elapsed,
                            retry_interval,
                            retry_window,
                        )
                    {
                        let msg = format!(
                            "SSH setup/upload failed after {} attempt(s) over {}s: {}",
                            attempt,
                            elapsed.as_secs(),
                            e
                        );
                        if !print_json {
                            println!("{}", msg);
                        }
                        return Err(NvUtils::sanitize_log(&msg));
                    }

                    if !print_json {
                        println!(
                            "SSH setup/upload attempt {} failed after {}s: {}; retrying in {}s",
                            attempt,
                            elapsed.as_secs(),
                            e,
                            SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS
                        );
                    }
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
impl BmcAccess {
    /// Lightweight stub for unit tests that don't need a live connection.
    pub fn default_stub() -> Self {
        Self {
            ip: "127.0.0.1".into(),
            user: "test".into(),
            password: "test".into(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port: String::new(),
            servertype: String::new(),
            base_url: "https://127.0.0.1".into(),
            transport_type: "https".into(),
            access_type: AccessType::Login,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            client: Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
        }
    }

    /// Lightweight test handle pointed at an async HTTP mock server.
    pub fn mock_with_base_url(base_url: impl Into<String>, ip: impl Into<String>) -> Self {
        Self::mock_with_base_url_and_type(base_url, ip, AccessType::Login)
    }

    /// Lightweight test handle pointed at an async HTTP mock server.
    pub fn mock_with_base_url_and_type(
        base_url: impl Into<String>,
        ip: impl Into<String>,
        access_type: AccessType,
    ) -> Self {
        Self {
            ip: ip.into(),
            user: "test".into(),
            password: "test".into(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port: String::new(),
            servertype: String::new(),
            base_url: base_url.into(),
            transport_type: "http".into(),
            access_type,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            client: Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};
    use tracing::Level;
    use tracing_subscriber::fmt::writer::MakeWriter;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Clone, Default)]
    struct CapturedWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedWriter {
        fn contents(&self) -> String {
            let bytes = self.buffer.lock().expect("capture lock").clone();
            String::from_utf8(bytes).expect("captured tracing output is utf8")
        }
    }

    struct CapturedWriterHandle {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for CapturedWriterHandle {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer
                .lock()
                .expect("capture lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedWriter {
        type Writer = CapturedWriterHandle;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedWriterHandle {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    fn multipart_field_position(body: &[u8], field_name: &str) -> usize {
        let body = String::from_utf8_lossy(body);
        body.find(&format!("name=\"{field_name}\""))
            .unwrap_or_else(|| panic!("missing multipart field {field_name}: {body}"))
    }

    #[test]
    fn test_access_type_variants() {
        assert_ne!(AccessType::Login, AccessType::PortForward);
        assert_ne!(AccessType::Login, AccessType::NVSwitch);
        assert_ne!(AccessType::PortForward, AccessType::NVSwitch);
    }

    #[test]
    fn bmc_tls_verification_defaults_to_disabled_mode() {
        assert!(!BmcAccess::should_verify_bmc_tls(None, None).unwrap());
        assert!(BmcAccess::build_client_from_tls_options(1, None, None).is_ok());
    }

    #[test]
    fn bmc_tls_verification_arg_accepts_common_boolean_values() {
        for value in ["1", "true", "yes", "on", " TRUE "] {
            assert!(BmcAccess::should_verify_bmc_tls(Some(value), None).unwrap());
        }

        for value in ["0", "false", "no", "off", " FALSE "] {
            assert!(!BmcAccess::should_verify_bmc_tls(Some(value), Some("/ignored")).unwrap());
        }
    }

    #[test]
    fn bmc_tls_verification_arg_rejects_unknown_values() {
        let err = BmcAccess::should_verify_bmc_tls(Some("maybe"), None).unwrap_err();
        assert!(err.contains(VERIFY_TLS_ARG));
        assert!(err.contains("true/false"));
    }

    #[test]
    fn bmc_ca_cert_path_enables_verification_when_verify_arg_is_unset() {
        assert!(BmcAccess::should_verify_bmc_tls(None, Some("/tmp/bmc-ca.pem")).unwrap());
        assert!(!BmcAccess::should_verify_bmc_tls(None, Some("   ")).unwrap());
    }

    #[test]
    fn bmc_client_builder_loads_ca_only_when_verification_is_enabled() {
        let missing_path = "/tmp/nvfwupd-definitely-missing-bmc-ca.pem";

        let err =
            BmcAccess::build_client_from_tls_options(1, None, Some(missing_path)).unwrap_err();
        assert!(err.contains(BMC_CA_CERT_ARG));
        assert!(err.contains(missing_path));

        assert!(
            BmcAccess::build_client_from_tls_options(1, Some("0"), None).is_ok(),
            "explicitly disabled verification should allow insecure/self-signed endpoints"
        );
    }

    #[test]
    fn bmc_ca_cert_is_rejected_when_verification_is_disabled() {
        let missing_path = "/tmp/nvfwupd-definitely-missing-bmc-ca.pem";
        let err =
            BmcAccess::build_client_from_tls_options(1, Some("0"), Some(missing_path)).unwrap_err();
        assert!(
            err.contains(BMC_CA_CERT_ARG),
            "expected {BMC_CA_CERT_ARG} in error, got: {err}"
        );
        assert!(
            err.contains(VERIFY_TLS_ARG),
            "expected {VERIFY_TLS_ARG} in error, got: {err}"
        );
    }

    #[test]
    fn ssh_host_verification_defaults_to_disabled() {
        let options = ssh_options::parse_ssh_options(&HashMap::new()).unwrap();

        assert_eq!(options.known_hosts, None);
        assert_eq!(options.mode, SSH_HOST_KEY_MODE_DISABLED);
    }

    #[test]
    fn ssh_host_verification_accepts_known_hosts_and_strict_mode() {
        let mut args = HashMap::new();
        args.insert(
            SSH_KNOWN_HOSTS_ARG.to_string(),
            "/tmp/known_hosts".to_string(),
        );
        args.insert(SSH_HOST_KEY_MODE_ARG.to_string(), "STRICT".to_string());

        let options = ssh_options::parse_ssh_options(&args).unwrap();

        assert_eq!(options.known_hosts.as_deref(), Some("/tmp/known_hosts"));
        assert_eq!(options.mode, SSH_HOST_KEY_MODE_STRICT);
    }

    #[test]
    fn ssh_host_verification_rejects_invalid_values() {
        let mut args = HashMap::new();
        args.insert(SSH_HOST_KEY_MODE_ARG.to_string(), "loose".to_string());
        let err = ssh_options::parse_ssh_options(&args).unwrap_err();
        assert!(err.contains(SSH_HOST_KEY_MODE_ARG));
    }

    #[test]
    fn ssh_host_verification_disabled_mode_is_supported() {
        let mut args = HashMap::new();
        args.insert(
            SSH_HOST_KEY_MODE_ARG.to_string(),
            SSH_HOST_KEY_MODE_DISABLED.to_string(),
        );

        let options = ssh_options::parse_ssh_options(&args).unwrap();

        assert_eq!(options.known_hosts, None);
        assert_eq!(options.mode, SSH_HOST_KEY_MODE_DISABLED);
    }

    #[test]
    fn ssh_known_hosts_is_rejected_when_verification_disabled() {
        let mut args = HashMap::new();
        args.insert(
            SSH_HOST_KEY_MODE_ARG.to_string(),
            SSH_HOST_KEY_MODE_DISABLED.to_string(),
        );
        args.insert(
            SSH_KNOWN_HOSTS_ARG.to_string(),
            "/tmp/known_hosts".to_string(),
        );
        let err = ssh_options::parse_ssh_options(&args).unwrap_err();
        assert!(err.contains(SSH_KNOWN_HOSTS_ARG));
        assert!(err.contains(SSH_HOST_KEY_MODE_DISABLED));
    }

    #[test]
    fn get_bmc_access_surfaces_tls_option_conflicts_without_servertype() {
        let trace = TraceFlags::default();
        let args = vec![
            "ip=127.0.0.1".to_string(),
            "user=admin".to_string(),
            "password=password".to_string(),
            "bmc_ca_cert=/tmp/nvfwupd-ca.pem".to_string(),
            "verify_tls=false".to_string(),
        ];

        let err = BmcAccess::get_bmc_access_sync(&args, trace, None).unwrap_err();

        assert!(
            err.contains(BMC_CA_CERT_ARG),
            "expected {BMC_CA_CERT_ARG} in error, got: {err}"
        );
        assert!(
            err.contains(VERIFY_TLS_ARG),
            "expected {VERIFY_TLS_ARG} in error, got: {err}"
        );
        assert!(err.contains("Failed to connect to target"), "{err}");
    }

    #[test]
    fn tls_certificate_verification_message_explains_fix_options() {
        let detail = "error trying to connect: invalid peer certificate: UnknownIssuer";
        let message = tls_certificate_verification_message_for_detail(
            "https://192.0.2.10/redfish/v1",
            detail,
        )
        .expect("certificate detail should be classified");

        assert!(message.contains("TLS certificate verification failed"));
        assert!(message.contains("bmc_ca_cert=/path/to/ca.pem"));
        assert!(message.contains("verify_tls=true"));
        assert!(message.contains("UnknownIssuer"));
    }

    #[test]
    fn non_tls_connection_detail_uses_generic_connection_message() {
        let message = connection_error_message_for_detail(
            "https://192.0.2.10/redfish/v1",
            "tcp connect error: Connection refused",
        );

        assert_eq!(
            message,
            "Connection Error: Failed to connect with the system."
        );
    }

    #[test]
    fn switch_ssh_upload_retry_budget_reserves_full_next_attempt() {
        let retry_interval = Duration::from_secs(SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS);
        let retry_window = Duration::from_secs(SWITCH_SSH_UPLOAD_RETRY_WINDOW_SECS);
        let next_attempt_budget =
            retry_interval + Duration::from_secs(SWITCH_SSH_UPLOAD_ATTEMPT_TIMEOUT_SECS);
        let last_valid_elapsed = retry_window
            .checked_sub(next_attempt_budget)
            .expect("retry window should cover another attempt");

        assert!(switch_ssh_upload_has_retry_budget(
            Duration::ZERO,
            retry_interval,
            retry_window
        ));
        assert!(switch_ssh_upload_has_retry_budget(
            last_valid_elapsed,
            retry_interval,
            retry_window
        ));
        assert!(!switch_ssh_upload_has_retry_budget(
            last_valid_elapsed + Duration::from_secs(1),
            retry_interval,
            retry_window
        ));
    }

    #[test]
    fn switch_remote_upload_path_rejects_empty_and_root_dirs() {
        for remote_dir in ["", "   ", "/", "///"] {
            let err = switch_remote_upload_path(remote_dir, "image.fwpkg").unwrap_err();
            assert!(err.contains("empty or root remote_dir"));
        }
    }

    #[test]
    fn switch_remote_upload_path_rejects_relative_dirs_and_path_like_filenames() {
        let err = switch_remote_upload_path("host/fw-images/bmc", "image.fwpkg").unwrap_err();
        assert!(err.contains("relative remote_dir"));

        for remote_filename in [
            ".",
            "..",
            "../image.fwpkg",
            "dir/image.fwpkg",
            "dir\\image.fwpkg",
            "image\n.fwpkg",
        ] {
            let err = switch_remote_upload_path("/host/fw-images/bmc", remote_filename)
                .expect_err("path-like filenames should be rejected");
            assert!(err.contains("unsafe remote filename"));
        }
    }

    #[test]
    fn switch_remote_upload_path_joins_with_single_separator() {
        assert_eq!(
            switch_remote_upload_path("/host/fw-images/bmc/", "image.fwpkg").unwrap(),
            "/host/fw-images/bmc/image.fwpkg"
        );
        assert_eq!(
            switch_remote_upload_path("/host/fw-images/bmc", "image.fwpkg").unwrap(),
            "/host/fw-images/bmc/image.fwpkg"
        );
    }

    #[test]
    fn switch_shell_quote_escapes_single_quotes() {
        assert_eq!(
            switch_shell_quote("/host/fw-images/bmc"),
            "'/host/fw-images/bmc'"
        );
        assert_eq!(
            switch_shell_quote("/host/fw-images/bmc's"),
            "'/host/fw-images/bmc'\\''s'"
        );
    }

    #[tokio::test]
    async fn scp_upload_setup_removes_only_owned_remote_path() {
        let mock_host = "mock-switch-upload-cleanup";
        ssh_transport::install_mock_for_host(mock_host);
        let access = BmcAccess::mock_with_base_url_and_type(
            "http://127.0.0.1",
            mock_host,
            AccessType::NVSwitch,
        );

        let remote_path = access
            .scp_upload_async_result(
                "/tmp/local-package.fwpkg",
                "/host/fw-images/bmc/",
                "remote-package.fwpkg",
                true,
            )
            .await
            .unwrap();

        let ssh = ssh_transport::take_mock_snapshot(mock_host).unwrap();
        assert_eq!(remote_path, "/host/fw-images/bmc/remote-package.fwpkg");
        assert!(ssh
            .exec_calls
            .iter()
            .any(|call| call.command == "mkdir -p '/host/fw-images/bmc/'"));
        assert!(ssh
            .exec_calls
            .iter()
            .any(|call| call.command == "rm -f '/host/fw-images/bmc/remote-package.fwpkg'"));
        assert!(!ssh
            .exec_calls
            .iter()
            .any(|call| call.command.contains("/*")));
    }

    #[test]
    fn test_debug_redacts_password() {
        let mut access = BmcAccess::default_stub();
        access.password = "plain_secret".to_string();

        let debug = format!("{access:?}");
        assert!(!debug.contains("plain_secret"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn test_validate_ip_v4() {
        let access = BmcAccess {
            ip: "10.0.0.1".to_string(),
            user: String::new(),
            password: String::new(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port: String::new(),
            servertype: String::new(),
            base_url: "https://10.0.0.1".to_string(),
            transport_type: "https".to_string(),
            access_type: AccessType::Login,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            client: Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
        };
        assert!(access.validate_ip().is_ok());
    }

    #[test]
    fn test_validate_ip_v6() {
        let access = BmcAccess {
            ip: "[::1]".to_string(),
            user: String::new(),
            password: String::new(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port: String::new(),
            servertype: String::new(),
            base_url: "https://[::1]".to_string(),
            transport_type: "https".to_string(),
            access_type: AccessType::Login,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            client: Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
        };
        assert!(access.validate_ip().is_ok());
    }

    #[test]
    fn test_validate_ip_invalid() {
        let access = BmcAccess {
            ip: "not-an-ip".to_string(),
            user: String::new(),
            password: String::new(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port: String::new(),
            servertype: String::new(),
            base_url: "https://not-an-ip".to_string(),
            transport_type: "https".to_string(),
            access_type: AccessType::Login,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            client: Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap(),
        };
        assert!(access.validate_ip().is_err());
    }

    #[test]
    fn test_new_login_basic() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "secret".to_string());

        let access = BmcAccess::new_login(&args).unwrap();
        assert_eq!(access.ip, "10.0.0.1");
        assert_eq!(access.user, "admin");
        assert_eq!(access.password, "secret");
        assert_eq!(access.access_type, AccessType::Login);
        assert_eq!(access.base_url, "https://10.0.0.1");
    }

    #[test]
    fn test_new_login_ipv6() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "fd00::1".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "pass".to_string());

        let access = BmcAccess::new_login(&args).unwrap();
        assert_eq!(access.ip, "[fd00::1]");
        assert_eq!(access.base_url, "https://[fd00::1]");
    }

    #[test]
    fn test_new_port_forward() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "127.0.0.1".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "pass".to_string());
        args.insert("port".to_string(), "8443".to_string());

        let access = BmcAccess::new_port_forward(&args).unwrap();
        assert_eq!(access.access_type, AccessType::PortForward);
        assert_eq!(access.port, "8443");
        assert_eq!(access.base_url, "https://127.0.0.1:8443");
    }

    #[test]
    fn test_new_nvswitch() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.5".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "pass".to_string());
        args.insert("servertype".to_string(), "gb200switch".to_string());

        let access = BmcAccess::new_nvswitch(&args).unwrap();
        assert_eq!(access.access_type, AccessType::NVSwitch);
        assert_eq!(access.base_url, "https://10.0.0.5");
    }

    #[test]
    fn test_new_nvswitch_with_port() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.5".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "pass".to_string());
        args.insert("port".to_string(), "443".to_string());

        let access = BmcAccess::new_nvswitch(&args).unwrap();
        assert_eq!(access.base_url, "https://10.0.0.5:443");
    }

    #[test]
    fn test_update_transport_type() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "pass".to_string());

        let mut access = BmcAccess::new_login(&args).unwrap();
        assert_eq!(access.transport_type, "https");
        assert_eq!(access.base_url, "https://10.0.0.1");

        access.update_transport_type("http");
        assert_eq!(access.transport_type, "http");
        assert_eq!(access.base_url, "http://10.0.0.1");
    }

    #[tokio::test]
    async fn test_block_on_http_inside_runtime() {
        let value = BmcAccess::block_on_http(async { 7 });
        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn test_block_on_http_keeps_spawned_tasks_alive_between_calls() {
        let (tx, rx) = std::sync::mpsc::channel();
        BmcAccess::block_on_http(async move {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                tx.send("bridge-body").unwrap();
            });
        });

        let body = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(body, "bridge-body");
    }

    #[tokio::test]
    async fn test_dispatch_rest_request_get_forwards_timeout_override() {
        LAST_TEST_DISPATCH_GET_TIMEOUT_SECS.store(0, Ordering::SeqCst);
        let trace = TraceFlags::default();
        let mut access = BmcAccess::default_stub();
        access.base_url = "not a valid url".to_string();

        let _ = access
            .dispatch_rest_request_get(trace, "", TEST_TIMEOUT_SENTINEL_SECS, None)
            .await;

        assert_eq!(
            LAST_TEST_DISPATCH_GET_TIMEOUT_SECS.load(Ordering::SeqCst),
            TEST_TIMEOUT_SENTINEL_SECS
        );
    }

    #[test]
    fn test_phase_2b_public_bmc_access_methods_are_async_send() {
        fn assert_send_future<T: std::future::Future + Send>(_future: T) {}

        let trace = TraceFlags::default();
        let target_args = vec![
            "ip=127.0.0.1".to_string(),
            "user=admin".to_string(),
            "password=secret".to_string(),
        ];
        let mut access = BmcAccess::default_stub();
        let post_body = serde_json::json!({});

        assert_send_future(BmcAccess::get_bmc_access(&target_args, trace, None));
        assert_send_future(access.is_reachable(trace));
        assert_send_future(access.get_system_info(trace, None));
        assert_send_future(access.dispatch_request("GET", "/redfish/v1", None, None));
        assert_send_future(access.dispatch_request_full(
            "GET",
            "/redfish/v1",
            None,
            None,
            1,
            true,
            None,
        ));
        assert_send_future(access.dispatch_file_upload(
            "/redfish/v1/UpdateService/update",
            "/tmp/fw.bin",
            1,
            None,
            false,
            None,
        ));
        assert_send_future(access.multipart_file_upload(
            "/redfish/v1/UpdateService/update",
            "/tmp/fw.bin",
            None,
            1,
            None,
            None,
            None,
            None,
            false,
        ));
        assert_send_future(access.get_firmware_inventory(trace, None, None));
        assert_send_future(access.get_software_inventory(trace, None));
        assert_send_future(access.get_chassis_members(trace));
        assert_send_future(access.get_systems_members(trace));
        assert_send_future(access.dispatch_rest_request_get(trace, "/nvue_v1/platform", 1, None));
        assert_send_future(access.dispatch_rest_request_post(
            trace,
            "/nvue_v1/platform",
            &post_body,
            1,
            None,
        ));
        assert_send_future(access.get_job_status(trace, "1", None));
        assert_send_future(access.get_system_rebooted_status(1));
    }

    #[tokio::test]
    async fn multipart_upload_default_order_sends_update_parameters_first() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"Id": "Task-1"})))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("fw.fwpkg");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();
        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");

        let (status, _) = access
            .multipart_file_upload(
                "/upload",
                update_file.to_str().unwrap(),
                None,
                30,
                Some(r#"{"Targets":[]}"#),
                None,
                None,
                None,
                false,
            )
            .await;

        assert!(status);
        let requests = server.received_requests().await.unwrap();
        let body = &requests[0].body;
        assert!(
            multipart_field_position(body, "UpdateParameters")
                < multipart_field_position(body, "UpdateFile")
        );
    }

    #[tokio::test]
    async fn multipart_upload_soft_json_decode_failure_logs_body_and_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(202).set_body_string("not-json"))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("fw.fwpkg");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();
        let writer = CapturedWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(Level::DEBUG)
            .with_writer(writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");

        let (status, response) = access
            .multipart_file_upload_with_options(
                "/upload",
                update_file.to_str().unwrap(),
                None,
                30,
                None,
                None,
                None,
                None,
                false,
                MultipartUploadOptions {
                    bail_on_failure: false,
                },
            )
            .await;

        assert!(!status);
        assert!(response.is_null());
        let output = writer.contents();
        assert!(output.contains("Could not decode update response body"));
        assert!(output.contains("not-json"));
        assert!(output.contains("202"));
    }

    #[tokio::test]
    async fn multipart_upload_soft_missing_package_file_warns_without_bail() {
        let tmp = tempfile::tempdir().unwrap();
        let missing_package = tmp.path().join("missing.fwpkg");
        let writer = CapturedWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(Level::WARN)
            .with_writer(writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let access = BmcAccess::mock_with_base_url("http://127.0.0.1:1".to_string(), "mock");

        let (status, response) = access
            .multipart_file_upload_with_options(
                "/upload",
                missing_package.to_str().unwrap(),
                None,
                30,
                None,
                None,
                None,
                None,
                false,
                MultipartUploadOptions {
                    bail_on_failure: false,
                },
            )
            .await;

        assert!(!status);
        assert!(response.is_null());
        let output = writer.contents();
        assert!(output.contains("Failed to read package file"));
        assert!(output.contains("missing.fwpkg"));
    }

    #[tokio::test]
    async fn multipart_upload_soft_missing_update_params_file_warns_without_bail() {
        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("fw.fwpkg");
        let missing_params = tmp.path().join("missing-update-params.json");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();
        let writer = CapturedWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(Level::WARN)
            .with_writer(writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let access = BmcAccess::mock_with_base_url("http://127.0.0.1:1".to_string(), "mock");

        let (status, response) = access
            .multipart_file_upload_with_options(
                "/upload",
                update_file.to_str().unwrap(),
                missing_params.to_str(),
                30,
                None,
                None,
                None,
                None,
                false,
                MultipartUploadOptions {
                    bail_on_failure: false,
                },
            )
            .await;

        assert!(!status);
        assert!(response.is_null());
        let output = writer.contents();
        assert!(output.contains("Failed to read params file"));
        assert!(output.contains("missing-update-params.json"));
    }

    #[tokio::test]
    async fn multipart_upload_soft_missing_oem_params_file_warns_without_bail() {
        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("fw.fwpkg");
        let missing_oem_params = tmp.path().join("missing-oem-params.json");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();
        let writer = CapturedWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(Level::WARN)
            .with_writer(writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let access = BmcAccess::mock_with_base_url("http://127.0.0.1:1".to_string(), "mock");

        let (status, response) = access
            .multipart_file_upload_with_options(
                "/upload",
                update_file.to_str().unwrap(),
                None,
                30,
                None,
                missing_oem_params.to_str(),
                None,
                None,
                false,
                MultipartUploadOptions {
                    bail_on_failure: false,
                },
            )
            .await;

        assert!(!status);
        assert!(response.is_null());
        let output = writer.contents();
        assert!(output.contains("Failed to read OEM params file"));
        assert!(output.contains("missing-oem-params.json"));
    }

    #[tokio::test]
    async fn nvswitch_inventory_preserves_reported_health_without_defaulting_ok() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "BMC": {
                    "actual-firmware": "88.0002.1979",
                    "Status": {"Health": "Warning"}
                },
                "CPLD1": {"version": "CPLD000370_REV0600"},
                "FPGA": {
                    "Version": "0.24",
                    "Health": "Critical"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url_and_type(
            server.uri(),
            "mock-switch",
            AccessType::NVSwitch,
        );
        let (status, err_code, inventory) = access
            .get_firmware_inventory(TraceFlags::default(), None, None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert_eq!(inventory["BMC"]["Id"], "BMC");
        assert_eq!(inventory["BMC"]["Version"], "88.0002.1979");
        assert_eq!(inventory["BMC"]["Updateable"], true);
        assert_eq!(inventory["BMC"]["Status"]["Health"], "Warning");
        assert_eq!(inventory["CPLD1"]["Version"], "CPLD000370_REV0600");
        assert!(inventory["CPLD1"].get("Status").is_none());
        assert_eq!(inventory["FPGA"]["Status"]["Health"], "Critical");
    }

    #[tokio::test]
    async fn nvswitch_expected_inventory_failure_normalizes_error_code() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/firmware"))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "error": "failed"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url_and_type(
            server.uri(),
            "mock-switch",
            AccessType::NVSwitch,
        );
        let (status, err_code, present) = access
            .get_expected_inventory_ap_names(TraceFlags::default(), None)
            .await;

        assert!(!status);
        assert_eq!(err_code, 1);
        assert!(present.is_empty());
    }

    #[tokio::test]
    async fn expected_inventory_ap_names_allows_empty_redfish_collection() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        let (status, err_code, present) = access
            .get_expected_inventory_ap_names(TraceFlags::default(), None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert!(present.is_empty());
    }

    #[tokio::test]
    async fn redfish_inventory_uses_oem_embedded_inventory_when_complete() {
        let server = MockServer::start().await;
        let uri = "/redfish/v1/UpdateService/FirmwareInventory/HostBMC_0";
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": uri}],
                "Oem": {
                    "Ami": {
                        "FirmwareInventory": [{
                            "DataSourceUri": uri,
                            "Name": "HostBMC_0",
                            "Version": "00.00.27"
                        }]
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "HostBMC_0",
                "Version": "should-not-fetch"
            })))
            .expect(0)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        let (status, err_code, inventory) = access
            .get_firmware_inventory(TraceFlags::default(), None, None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert_eq!(inventory[uri]["Name"], "HostBMC_0");
        assert_eq!(inventory[uri]["Version"], "00.00.27");
    }

    #[tokio::test]
    async fn expected_inventory_ap_names_reads_collection_only() {
        let server = MockServer::start().await;
        let uri = "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0";
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": uri}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "FW_BMC_0",
                "Version": "should-not-fetch"
            })))
            .expect(0)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        let (status, err_code, present) = access
            .get_expected_inventory_ap_names(TraceFlags::default(), None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert_eq!(present, vec!["FW_BMC_0"]);
    }

    #[test]
    fn megmeet_fru_info_extracts_system_identity() {
        let chassis = serde_json::json!({
            "Model": "N/A",
            "Oem": {
                "InsydeFRUData": {
                    "FRUInfo": [
                        {
                            "Name": "PSU1",
                            "Product": {
                                "Manufacturer": "MEGMEET",
                                "Name": "IPS495500-AIT-N1BA"
                            }
                        },
                        {
                            "Name": "MCU",
                            "Product": {
                                "Manufacturer": "MEGMEET",
                                "Name": "MPSC6000-MCU"
                            }
                        },
                        {
                            "Name": "Chassis",
                            "Board": {
                                "Manufacturer": "MEGMEET",
                                "Name": "IPU433K-NB-AA1616-L",
                                "PartNumber": "R07020556",
                                "Serial": "Z270060254600002"
                            }
                        }
                    ]
                }
            }
        });

        let info = BmcAccess::get_megmeet_system_info_from_chassis(&chassis);

        assert_eq!(info.model.as_deref(), Some("MPSC6000-MCU"));
        assert_eq!(info.partnumber.as_deref(), Some("R07020556"));
        assert_eq!(info.serialnumber.as_deref(), Some("Z270060254600002"));
    }

    #[tokio::test]
    async fn megmeet_powershelf_0_chassis_fru_sets_model() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": "/redfish/v1/Chassis/Unknown_Board"}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/PowerShelf_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Name": "PowerShelf_0",
                "Model": "N/A",
                "Oem": {
                    "InsydeFRUData": {
                        "FRUInfo": [
                            {
                                "Name": "MCU",
                                "Product": {
                                    "Manufacturer": "MEGMEET",
                                    "Name": "MPSC6000-MCU"
                                }
                            },
                            {
                                "Name": "Chassis",
                                "Board": {
                                    "Manufacturer": "MEGMEET",
                                    "PartNumber": "R07020556",
                                    "SerialNumber": "Z270060254600002"
                                }
                            }
                        ]
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-megmeet");
        let status = access.get_system_info(TraceFlags::default(), None).await;

        assert!(status);
        assert_eq!(access.model, "MPSC6000-MCU");
        assert_eq!(access.partnumber, "R07020556");
        assert_eq!(access.serialnumber, "Z270060254600002");
    }

    #[tokio::test]
    async fn megmeet_mcu_chassis_used_when_powershelf_0_missing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": "/redfish/v1/Chassis/Unknown_Board"}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/MCU"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Name": "MCU",
                "Oem": {
                    "InsydeFRUData": {
                        "FRUInfo": [
                            {
                                "Name": "MCU",
                                "Product": {
                                    "Manufacturer": "MEGMEET",
                                    "Name": "MPSC6000-MCU"
                                }
                            }
                        ]
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-megmeet");
        let status = access.get_system_info(TraceFlags::default(), None).await;

        assert!(status);
        assert_eq!(access.model, "MPSC6000-MCU");
    }

    #[tokio::test]
    async fn redfish_inventory_does_not_add_liteon_powerunits_for_megmeet() {
        let server = MockServer::start().await;
        let uri = "/redfish/v1/UpdateService/FirmwareInventory/mcu_active";
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": uri}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "mcu_active",
                "Version": "V02B01"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": []
            })))
            .expect(0)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-megmeet");
        access.servertype = "powershelf".to_string();
        access.model = "MPSC8000-MCU".to_string();
        let (status, err_code, inventory) = access
            .get_firmware_inventory(TraceFlags::default(), None, None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert_eq!(inventory[uri]["Version"], "V02B01");
    }

    #[tokio::test]
    async fn redfish_inventory_does_not_add_liteon_powerunits_for_flex() {
        let server = MockServer::start().await;
        let uri = "/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0";
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": uri}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "FW_PMC_0",
                "Version": "v3.0.12"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": []
            })))
            .expect(0)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-flex");
        access.servertype = "powershelf".to_string();
        access.model = "NVD-P-0000ADT00".to_string();
        let (status, err_code, inventory) = access
            .get_firmware_inventory(TraceFlags::default(), None, None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert_eq!(inventory[uri]["Version"], "v3.0.12");
    }

    #[tokio::test]
    async fn redfish_inventory_does_not_add_liteon_powerunits_for_delta() {
        let server = MockServer::start().await;
        let uri = "/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0";
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": uri}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "FW_PMC_0",
                "Version": "1.2.3"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": []
            })))
            .expect(0)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-delta");
        access.servertype = "powershelf".to_string();
        access.model = "AHE50V2200A".to_string();
        let (status, err_code, inventory) = access
            .get_firmware_inventory(TraceFlags::default(), None, None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert_eq!(inventory[uri]["Version"], "1.2.3");
    }

    #[tokio::test]
    async fn redfish_inventory_adds_liteon_powerunits_for_liteon_model() {
        let server = MockServer::start().await;
        let inventory_uri = "/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0";
        let powerunit_uri =
            "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1";
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": inventory_uri}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(inventory_uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "FW_PMC_0",
                "Version": "1.2.3"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": powerunit_uri}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(powerunit_uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Version": "0xf6 / 0xf8"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-liteon");
        access.servertype = "powershelf".to_string();
        access.model = "PF-1333-7R".to_string();
        let (status, err_code, inventory) = access
            .get_firmware_inventory(TraceFlags::default(), None, None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert_eq!(inventory[inventory_uri]["Version"], "1.2.3");
        assert_eq!(inventory[powerunit_uri]["Version"], "0xf6 / 0xf8");
    }

    #[test]
    fn test_get_bmc_access_invalid_args() {
        let trace = TraceFlags::default();
        let args = vec!["invalid_no_equals".to_string()];
        let result = BmcAccess::get_bmc_access_sync(&args, trace, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_bmc_access_invalid_args_redacts_password_in_error() {
        let _guard = crate::utils::SanitizeTestGuard::new();
        let trace = TraceFlags::default();
        let args = vec![
            "ip=127.0.0.1".to_string(),
            "user=admin".to_string(),
            "password=plain secret suffix".to_string(),
            "invalid_no_equals".to_string(),
        ];
        let err = BmcAccess::get_bmc_access_sync(&args, trace, None).unwrap_err();

        assert!(!err.contains("plain"));
        assert!(!err.contains("secret"));
        assert!(!err.contains("suffix"));
        assert!(err.contains("password=XXXX"));
    }

    #[test]
    fn test_get_bmc_access_connect_error_redacts_password() {
        let _guard = crate::utils::SanitizeTestGuard::new();
        let trace = TraceFlags::default();
        let args = vec![
            "ip=not_an_ip".to_string(),
            "user=admin".to_string(),
            "password=plain secret suffix".to_string(),
        ];
        let err = BmcAccess::get_bmc_access_sync(&args, trace, None).unwrap_err();

        assert!(!err.contains("plain"));
        assert!(!err.contains("secret"));
        assert!(!err.contains("suffix"));
        assert!(err.contains("password=XXXX"));
    }

    #[tokio::test]
    async fn test_dispatch_get_debug_redacts_response_body_for_structured_subscribers() {
        let _sanitize_guard = crate::utils::SanitizeTestGuard::new();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/bad-json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("password=plain_secret"))
            .mount(&server)
            .await;

        let writer = CapturedWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_ansi(false)
            .with_max_level(Level::DEBUG)
            .with_writer(writer.clone())
            .finish();
        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        let url = format!("{}/bad-json", server.uri());
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);

        let _ = access
            .dispatch_get_async(TraceFlags::new(true, false), &url, false)
            .await;

        let output = writer.contents();
        assert!(!output.contains("plain_secret"), "{output}");
        assert!(output.contains("password=XXXX"), "{output}");
    }

    #[test]
    fn test_get_bmc_access_nvos_deprecated() {
        let trace = TraceFlags::default();
        let args = vec![
            "ip=10.0.0.1".to_string(),
            "user=admin".to_string(),
            "password=pass".to_string(),
            "servertype=nvos".to_string(),
        ];
        let result = BmcAccess::get_bmc_access_sync(&args, trace, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("deprecated"));
    }

    #[test]
    fn test_switch_server_types() {
        for st in SWITCH_SERVER_TYPES {
            assert!(["gb200switch", "gb300switch", "vrnvl72switch"].contains(st));
        }
    }
}
