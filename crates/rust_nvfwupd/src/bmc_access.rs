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

//! BMC endpoint access module for nvfwupd.
//!
//! Provides connectivity to BMC servers via HTTPS/Redfish protocol.
//! The CLI supports three access modes:
//! - **Login**: Direct BMC access with IP, username, and password credentials
//! - **PortForward**: BMC access through localhost port forwarding
//! - **NVSwitch**: NVUE REST API access for GB200/GB300/VR switch platforms
//!
//! The workflow API also supports caller-supplied BMC session tokens for Redfish.
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

use reqwest::header::HeaderValue;
use reqwest::{Client, Url};
use serde::Serialize;
use serde_json::{json, Value};
use tokio_util::io::ReaderStream;

use crate::expected_inventory;
use crate::nvos_image;
use crate::powershelf_rftarget::liteon_oem_powerunits_shelf;
use crate::ssh_options::{
    self, SSH_HOST_KEY_MODE_ARG, SSH_HOST_KEY_MODE_DISABLED, SSH_HOST_KEY_MODE_STRICT,
    SSH_HOST_KEY_MODE_TOFU, SSH_KNOWN_HOSTS_ARG,
};
use crate::ssh_transport;
use crate::switch_cli::{SwitchCliHandler, SwitchCliTransport};
use crate::util::{BailAction, TraceFlags, Util};
use crate::utils::Util as NvUtils;
use nvswitch_cli::{RemotePath, ShellCommand};

// Redact before truncation so a boundary cannot expose a partial credential.
fn bounded_submission_text(value: &str, limit: usize, credentials: &[&str]) -> String {
    let mut redacted = NvUtils::redact_secret_fields(value);
    // Responses and legacy Debug output can escape echoed credentials. Mask
    // those forms too, before truncation and independently of global state.
    let mut secrets: Vec<String> = credentials
        .iter()
        .filter(|value| !value.is_empty())
        .flat_map(|value| {
            let json = serde_json::to_string(value).expect("strings serialize to JSON");
            let debug = format!("{value:?}");
            [
                (*value).to_string(),
                json[1..json.len() - 1].to_string(),
                debug[1..debug.len() - 1].to_string(),
            ]
        })
        .collect();
    secrets.sort_by_key(|value| std::cmp::Reverse(value.len()));
    secrets.dedup();
    for secret in secrets {
        redacted = redacted.replace(&secret, "XXXX");
    }
    let sanitized = NvUtils::sanitize_log(&redacted);
    sanitized.chars().take(limit).collect()
}

tokio::task_local! {
    static SWITCH_SFTP_PROGRESS_LABEL: String;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default HTTP request timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 900;
const VERIFY_TLS_ARG: &str = "verify_tls";
const BMC_CA_CERT_ARG: &str = "bmc_ca_cert";
const ALLOW_HTTP_ARG: &str = "allow_http";
const SESSION_TOKEN_ARG: &str = "session_token";

/// Timeout for reachability probes (seconds).
const REACHABILITY_TIMEOUT_SECS: u64 = 120;

/// Per-attempt SSH connect/auth timeout for switch firmware uploads.
const SWITCH_SSH_CONNECT_TIMEOUT_SECS: u64 = 30;

/// Per-attempt SFTP transfer timeout for switch firmware uploads.
const SWITCH_SSH_UPLOAD_TIMEOUT_SECS: u64 = 15 * 60;

/// Per-command timeout for the quick `mkdir -p` / `rm -f` setup steps that
/// precede the transfer. These are trivial operations, so they get a short
/// budget of their own rather than inheriting the large upload timeout.
const SWITCH_SSH_SETUP_TIMEOUT_SECS: u64 = 30;

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
         Certificate validation is enabled for this connection; check target certificate trust \
         configuration. \
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

fn nvswitch_reachability_body_diagnostic(trace: TraceFlags, body: &str) -> Option<String> {
    if trace.json_mode {
        return None;
    }

    match serde_json::from_str::<Value>(body) {
        Ok(parsed) => Some(pretty_4space_redacted(&parsed)),
        Err(_) => Some(NvUtils::sanitize_log(body)),
    }
}

#[cfg(test)]
static LAST_TEST_DISPATCH_GET_TIMEOUT_SECS: AtomicU64 = AtomicU64::new(0);

// NUL cannot come from CLI arguments, so only the validated workflow builder
// can select session-token authentication without changing the public struct.
// It is set only from `WorkflowTargetBuilder::build_bmc_access`, driven by
// `TargetConfig::bmc_session_token()` (an explicit field, not a heuristic).
pub(crate) const BMC_SESSION_TOKEN_USER: &str = "\0nvfwupd-bmc-session-token";

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

/// Outcome of an observed NVUE GET used by retry pollers.
///
/// These three states are mutually exclusive, and callers must distinguish
/// them: a switch that answers is still alive, while a missing response means
/// it stopped answering. Collapsing either into the other changes whether a
/// reboot is treated as an outage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObservedRead {
    /// A success response whose body parsed as JSON.
    Readable(Value),
    /// The switch answered, but not with a readable body.
    Answered(Value),
    /// No HTTP response arrived, carrying the transport cause for diagnostics.
    NoResponse(String),
}

impl ObservedRead {
    /// An answer whose body could not be parsed, reported as an empty object so
    /// every access path agrees on the placeholder.
    pub(crate) fn answered_unparsed() -> Self {
        Self::Answered(Value::Object(serde_json::Map::new()))
    }
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

    /// Username used for Basic authentication.
    pub user: String,

    /// Basic-auth password or workflow-supplied Redfish session token.
    pub password: String,

    /// Discovered or user-supplied model name.
    pub model: String,
    /// Part number reported by the BMC.
    pub partnumber: String,
    /// Serial number reported by the BMC.
    pub serialnumber: String,
    /// Optional port for port-forward or direct non-default Redfish access.
    pub port: String,
    /// Server type string (e.g. "gb200switch").
    pub servertype: String,
    /// Fully-qualified base URL (e.g. `https://10.0.0.1`).
    pub base_url: String,
    /// Transport scheme, either "https" or "http".
    pub transport_type: String,
    /// Whether cleartext HTTP fallback is allowed after HTTPS reachability fails.
    pub allow_http: bool,
    /// Which access strategy is in use.
    pub access_type: AccessType,
    /// Optional known_hosts file for SSH/SFTP host-key verification.
    pub ssh_known_hosts: Option<String>,
    /// SSH/SFTP host-key verification mode: `disabled`, `tofu`, or `strict`.
    pub ssh_host_key_mode: String,
    /// Shared reqwest async client.
    pub client: Client,
    /// Per-instance compute policy; absent for legacy CLI and non-compute callers.
    pub compute_timing: Option<crate::compute_timing::ComputeFirmwareTiming>,
    /// Request-local compute power paths; empty keeps legacy discovery.
    pub compute_power_paths: crate::compute_power::ComputePowerPaths,
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
            .field("allow_http", &self.allow_http)
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
    attempt_timeout_secs: u64,
) -> bool {
    let next_attempt_budget = retry_interval + Duration::from_secs(attempt_timeout_secs);
    elapsed + next_attempt_budget <= retry_window
}

/// Quote a value for the remote switch shell.
///
/// Remote setup commands interpolate paths, so single quotes are escaped using
/// the standard POSIX `'\''` sequence.
fn switch_shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn format_bytes_for_progress(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * MIB;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes / GIB)
    } else {
        format!("{:.1} MiB", bytes / MIB)
    }
}

fn format_switch_sftp_upload_progress(
    attempt: u32,
    progress: ssh_transport::SftpUploadProgress,
) -> String {
    let elapsed_secs = progress.elapsed_secs.max(1);
    let rate_mib = progress.uploaded_bytes as f64 / elapsed_secs as f64 / (1024.0 * 1024.0);
    let uploaded = format_bytes_for_progress(progress.uploaded_bytes);

    match progress.total_bytes.filter(|total| *total > 0) {
        Some(total) => {
            let total_display = format_bytes_for_progress(total);
            let percent = (progress.uploaded_bytes as f64 / total as f64) * 100.0;
            format!(
                "SFTP upload attempt {attempt} progress: {uploaded} / {total_display} ({percent:.1}%), {rate_mib:.1} MiB/s"
            )
        }
        None => format!(
            "SFTP upload attempt {attempt} progress: {uploaded} transferred, {rate_mib:.1} MiB/s"
        ),
    }
}

fn clean_switch_sftp_progress_label(label: &str) -> Option<String> {
    let sanitized = NvUtils::sanitize_log(label.trim());
    let label = sanitized.split_whitespace().collect::<Vec<_>>().join(" ");
    if label.is_empty() {
        None
    } else {
        Some(label)
    }
}

fn current_switch_sftp_progress_label() -> Option<String> {
    SWITCH_SFTP_PROGRESS_LABEL
        .try_with(|label| clean_switch_sftp_progress_label(label))
        .ok()
        .flatten()
}

fn format_labeled_switch_sftp_message(label: Option<&str>, message: &str) -> String {
    match label.and_then(clean_switch_sftp_progress_label) {
        Some(label) => format!("[{label}] {message}"),
        None => message.to_string(),
    }
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

fn switch_remote_cleanup_path(remote_path: &str) -> Result<&str, String> {
    let remote_path = remote_path.trim();
    if remote_path.is_empty()
        || remote_path == "/"
        || !remote_path.starts_with('/')
        || remote_path.ends_with('/')
        || remote_path.contains("//")
        || remote_path.chars().any(char::is_control)
    {
        return Err("refusing to remove an unsafe remote path".to_string());
    }

    if remote_path
        .split('/')
        .any(|segment| segment == "." || segment == ".." || segment.contains('\\'))
    {
        return Err("refusing to remove an unsafe remote path".to_string());
    }

    Ok(remote_path)
}

/// Build a `HeaderValue` for a bearer/session token and mark it sensitive so
/// reqwest omits it from debug output and tracing/logging integrations.
///
/// Callers that validate the token as a legal header value beforehand (e.g.
/// at client construction) will never hit the fallback in practice; callers
/// with an unvalidated token (e.g. extracted from a response header) may hit
/// it for real.
pub fn sensitive_header_value(token: &str) -> HeaderValue {
    let mut value = HeaderValue::from_str(token)
        .unwrap_or_else(|_| HeaderValue::from_static("invalid-session-token"));
    value.set_sensitive(true);
    value
}

impl BmcAccess {
    /// Bound submission text after masking this target's authentication values.
    pub(crate) fn bounded_submission_text(&self, value: &str, limit: usize) -> String {
        bounded_submission_text(value, limit, &[&self.user, &self.password])
    }

    fn redacted_submission_response(&self, value: &Value) -> Value {
        fn mask_echoes(access: &BmcAccess, value: &mut Value) {
            match value {
                Value::String(text) => {
                    *text = access.bounded_submission_text(text, usize::MAX);
                }
                Value::Array(entries) => {
                    for entry in entries {
                        mask_echoes(access, entry);
                    }
                }
                Value::Object(fields) => {
                    *fields = std::mem::take(fields)
                        .into_iter()
                        .map(|(key, mut entry)| {
                            mask_echoes(access, &mut entry);
                            (access.bounded_submission_text(&key, usize::MAX), entry)
                        })
                        .collect();
                }
                _ => {}
            }
        }
        let mut redacted = NvUtils::redact_secret_json_value(value);
        mask_echoes(self, &mut redacted);
        redacted
    }

    /// Run an async operation with a task-local switch SFTP progress label.
    ///
    /// Parallel CLI workers use this to keep interleaved upload output readable
    /// without changing RMS/library APIs.
    pub(crate) async fn with_switch_sftp_progress_label<F, T>(label: String, future: F) -> T
    where
        F: Future<Output = T>,
    {
        SWITCH_SFTP_PROGRESS_LABEL.scope(label, future).await
    }

    /// Prefix a switch SFTP status line with the active task-local label, if any.
    pub(crate) fn format_switch_sftp_status_line(message: &str) -> String {
        format_labeled_switch_sftp_message(current_switch_sftp_progress_label().as_deref(), message)
    }

    fn same_url_origin(left: &Url, right: &Url) -> bool {
        left.scheme() == right.scheme()
            && left.host_str() == right.host_str()
            && left.port_or_known_default() == right.port_or_known_default()
    }

    /// Resolve a Redfish/NVUE request URI against the selected target origin.
    ///
    /// Wrapper request methods attach BMC credentials after URL construction, so
    /// device/config supplied links must remain on the same scheme, host, and
    /// port as `base_url`. Relative Redfish paths and same-origin absolute URLs
    /// are accepted; authority-like, off-origin, userinfo-bearing, and
    /// control-character-containing values are rejected.
    fn same_origin_request_url(&self, uri: &str, label: &str) -> Result<String, String> {
        let uri = uri.trim();
        if uri.is_empty() || uri.starts_with("//") || uri.chars().any(char::is_control) {
            return Err(format!(
                "{label} is not a safe same-origin URI: {}",
                NvUtils::sanitize_log(uri)
            ));
        }

        let base = Url::parse(&self.base_url).map_err(|e| {
            format!(
                "Invalid BMC base URL {}: {e}",
                NvUtils::sanitize_log(&self.base_url)
            )
        })?;
        let resolved = if uri.starts_with("http://") || uri.starts_with("https://") {
            Url::parse(uri)
                .map_err(|e| format!("Invalid {label} {}: {e}", NvUtils::sanitize_log(uri)))?
        } else if uri.starts_with('/') {
            base.join(uri)
                .map_err(|e| format!("Invalid {label} {}: {e}", NvUtils::sanitize_log(uri)))?
        } else {
            return Err(format!(
                "{label} must be a Redfish/NVUE path or same-origin absolute URL: {}",
                NvUtils::sanitize_log(uri)
            ));
        };

        if !Self::same_url_origin(&base, &resolved) {
            return Err(format!(
                "{label} must stay on the selected target origin: {}",
                NvUtils::sanitize_log(uri)
            ));
        }
        if !resolved.username().is_empty() || resolved.password().is_some() {
            return Err(format!(
                "{label} must not contain user info: {}",
                NvUtils::sanitize_log(uri)
            ));
        }

        Ok(resolved.to_string())
    }

    /// Resolve a device/config URI to the path/query form expected by callers
    /// that still append the result to `base_url` themselves.
    fn same_origin_request_path(&self, uri: &str, label: &str) -> Result<String, String> {
        let resolved = Url::parse(&self.same_origin_request_url(uri, label)?).map_err(|e| {
            format!(
                "Invalid resolved {label} {}: {e}",
                NvUtils::sanitize_log(uri.trim())
            )
        })?;
        let mut path = resolved.path().to_string();
        if let Some(query) = resolved.query() {
            path.push('?');
            path.push_str(query);
        }
        Ok(path)
    }

    fn session_token(&self) -> Option<&str> {
        (self.user == BMC_SESSION_TOKEN_USER && !self.password.is_empty())
            .then_some(self.password.as_str())
    }

    pub(crate) fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = self.session_token() {
            request.header("X-Auth-Token", sensitive_header_value(token))
        } else {
            request.basic_auth(&self.user, Some(&self.password))
        }
    }

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
            None => Ok(ca_cert_path
                .map(str::trim)
                .is_some_and(|path| !path.is_empty())),
        }
    }

    fn should_allow_http_fallback(allow_arg: Option<&str>) -> Result<bool, String> {
        match allow_arg {
            Some(value) => Self::parse_bool_arg(ALLOW_HTTP_ARG, value),
            None => Ok(false),
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

    /// Start every BMC/NVUE client builder from the same transport baseline.
    ///
    /// Disable reqwest's default 30-second `TCP_USER_TIMEOUT` on Linux to avoid
    /// premature firmware-upload failures. HTTP deadlines remain unchanged.
    pub(crate) fn client_builder(timeout_secs: u64) -> reqwest::ClientBuilder {
        let builder = Client::builder().timeout(Duration::from_secs(timeout_secs));
        #[cfg(target_os = "linux")]
        let builder = builder.tcp_user_timeout(None);
        builder
    }

    /// Redirect-following is disabled when `allow_redirects` is false.
    /// Session-token auth must never follow a redirect with the token
    /// attached (see `apply_auth`).
    fn build_client_from_tls_options(
        timeout_secs: u64,
        verify_arg: Option<&str>,
        ca_cert_path: Option<&str>,
        allow_redirects: bool,
    ) -> Result<Client, String> {
        let verify_tls = Self::should_verify_bmc_tls(verify_arg, ca_cert_path)?;
        let ca_cert_path = ca_cert_path.map(str::trim).filter(|path| !path.is_empty());
        if !verify_tls && ca_cert_path.is_some() {
            return Err(format!(
                "{BMC_CA_CERT_ARG} cannot be used when {VERIFY_TLS_ARG}=false; \
                 remove {BMC_CA_CERT_ARG} or enable TLS verification"
            ));
        }

        let mut builder = Self::client_builder(timeout_secs);
        if verify_tls {
            builder = Self::add_bmc_ca_cert(builder, ca_cert_path)?;
        } else {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if !allow_redirects {
            builder = builder.redirect(reqwest::redirect::Policy::none());
        }

        builder
            .build()
            .map_err(|e| format!("Failed to build HTTP client: {e}"))
    }

    /// Build a [`reqwest::Client`] for BMC/NVUE HTTPS requests.
    ///
    /// TLS certificate verification and SSH host-key verification are
    /// implemented and ready for NVFWUPD, but default CLI enablement is pending
    /// customer discussions. Passing a CA certificate or explicitly enabling
    /// verification keeps the opt-in path available.
    ///
    /// Redirect-following is disabled when `arg_dict` carries a session token,
    /// so the token is never replayed to a redirect target (see `apply_auth`).
    fn build_client(
        timeout_secs: u64,
        arg_dict: &HashMap<String, String>,
    ) -> Result<Client, String> {
        let verify_arg = arg_dict.get(VERIFY_TLS_ARG).map(String::as_str);
        let ca_cert_path = arg_dict.get(BMC_CA_CERT_ARG).map(String::as_str);
        let allow_redirects = !arg_dict.contains_key(SESSION_TOKEN_ARG);
        Self::build_client_from_tls_options(timeout_secs, verify_arg, ca_cert_path, allow_redirects)
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

    /// Resolve target credentials as `(user, password)` for the `BmcAccess`
    /// fields, honoring the same sentinel convention as the RMS-facing
    /// `WorkflowTargetBuilder::build_bmc_access`: a session token is stored
    /// as `(BMC_SESSION_TOKEN_USER, token)`, so `session_token()` and every
    /// existing session-token guard (HTTP-fallback skip, login-cookie skip)
    /// apply automatically regardless of construction path.
    ///
    /// `session_token` is mutually exclusive with `user`/`password`: mixing
    /// them is rejected rather than silently picking one. `TargetConfig`
    /// avoids the same ambiguity by construction (its builder clears
    /// username/password when a token is set); the CLI's flat key=value
    /// input has no such builder step, so it is enforced here explicitly.
    fn resolve_login_credentials(
        arg_dict: &HashMap<String, String>,
    ) -> Result<(String, String), String> {
        let Some(token) = arg_dict.get(SESSION_TOKEN_ARG) else {
            return Ok((
                arg_dict.get("user").cloned().unwrap_or_default(),
                arg_dict.get("password").cloned().unwrap_or_default(),
            ));
        };

        if arg_dict.contains_key("user") || arg_dict.contains_key("password") {
            return Err(format!(
                "{SESSION_TOKEN_ARG} cannot be combined with user/password; \
                 provide one credential form"
            ));
        }
        let token = token.trim();
        if token.is_empty() {
            return Err(format!("{SESSION_TOKEN_ARG} must not be empty"));
        }
        HeaderValue::from_str(token)
            .map_err(|error| format!("invalid {SESSION_TOKEN_ARG} header value: {error}"))?;

        Ok((BMC_SESSION_TOKEN_USER.to_owned(), token.to_owned()))
    }

    /// Create a `BmcAccess` configured for direct login.
    fn new_login(arg_dict: &HashMap<String, String>) -> Result<Self, String> {
        let client = Self::build_client(DEFAULT_TIMEOUT_SECS, arg_dict)?;
        let ssh_host_key_options = ssh_options::parse_ssh_options(arg_dict)?;
        let allow_http =
            Self::should_allow_http_fallback(arg_dict.get(ALLOW_HTTP_ARG).map(String::as_str))?;
        let (user, password) = Self::resolve_login_credentials(arg_dict)?;
        let mut ip = arg_dict.get("ip").cloned().unwrap_or_default();

        // IPv6 addresses require brackets.
        if ip.contains(':') && !ip.starts_with('[') {
            ip = format!("[{}]", ip);
        }

        let transport_type = "https".to_string();
        let base_url = format!("{transport_type}://{ip}");

        Ok(Self {
            ip,
            user,
            password,
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port: String::new(),
            servertype: arg_dict.get("servertype").cloned().unwrap_or_default(),
            base_url,
            transport_type,
            allow_http,
            access_type: AccessType::Login,
            ssh_known_hosts: ssh_host_key_options.known_hosts,
            ssh_host_key_mode: ssh_host_key_options.mode,
            client,
            compute_timing: None,
            compute_power_paths: Default::default(),
        })
    }

    /// Create a direct Redfish login using an explicit non-default port.
    fn new_login_with_port(arg_dict: &HashMap<String, String>) -> Result<Self, String> {
        let mut access = Self::new_login(arg_dict)?;
        let port = arg_dict
            .get("port")
            .map(|value| value.trim())
            .unwrap_or_default();
        if port.is_empty() {
            return Err("invalid port: ".to_string());
        }
        port.parse::<u16>()
            .map_err(|_| format!("invalid port: {}", NvUtils::sanitize_log(port)))?;
        access.port = port.to_string();
        access.base_url = format!("https://{}:{}", access.ip, access.port);
        Ok(access)
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
        if arg_dict.contains_key(SESSION_TOKEN_ARG) {
            return Err(format!(
                "{SESSION_TOKEN_ARG} is not supported for NVUE/NVOS switch targets; \
                 use user/password"
            ));
        }
        let client = Self::build_client(1200, arg_dict)?;
        let ssh_host_key_options = ssh_options::parse_ssh_options(arg_dict)?;
        let allow_http =
            Self::should_allow_http_fallback(arg_dict.get(ALLOW_HTTP_ARG).map(String::as_str))?;
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
            allow_http,
            access_type: AccessType::NVSwitch,
            ssh_known_hosts: ssh_host_key_options.known_hosts,
            ssh_host_key_mode: ssh_host_key_options.mode,
            client,
            compute_timing: None,
            compute_power_paths: Default::default(),
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
    /// 2. Otherwise try, in order: port-forward, direct-port login, login, NVSwitch.
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

    /// Whether a reachable probe with `arg_count` target keys is a complete
    /// credential for a builder calibrated to need `expected_args` keys.
    ///
    /// `expected_args` is a Python-parity minimum sized for ip+user+password.
    /// A session token makes ip+session_token (2 keys) a complete, valid
    /// credential on its own, so it must not be discarded for looking too
    /// short relative to that minimum.
    fn probe_has_enough_args(
        arg_count: usize,
        expected_args: usize,
        has_session_token: bool,
    ) -> bool {
        arg_count >= expected_args || has_session_token
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

        // General probe order: PortForward -> DirectPort -> Login -> NVSwitch
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
                    "BMCDirectPortAccess",
                    Self::new_login_with_port
                        as fn(&HashMap<String, String>) -> Result<BmcAccess, String>,
                    3,
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

            let has_session_token = arg_dict.contains_key(SESSION_TOKEN_ARG);

            for (name, builder, expected_args) in &access_builders {
                // Skip port-forward if no port argument given.
                if *name == "BMCPortForwardAccess" && !arg_dict.contains_key("port") {
                    continue;
                }
                if *name == "BMCDirectPortAccess" && !arg_dict.contains_key("port") {
                    continue;
                }
                // A session token is never valid for NVUE/NVOS switch access;
                // skip this probe rather than let its rejection error
                // overwrite a more useful failure message from an earlier
                // probe (e.g. why the BMC login attempt was unreachable).
                if *name == "GB200NVSwitchAccess" && has_session_token {
                    continue;
                }

                let mut access = match builder(&arg_dict) {
                    Ok(a) => a,
                    Err(e) => {
                        if connection_failure_message.is_none() {
                            connection_failure_message = Some(e);
                        }
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
                    if !message.is_empty() && connection_failure_message.is_none() {
                        connection_failure_message = Some(message);
                    }
                    continue;
                }

                if !Self::probe_has_enough_args(arg_dict.len(), *expected_args, has_session_token) {
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

    /// Redfish reachability check.
    ///
    /// On HTTPS failure, fails closed by default. If `allow_http=true` was
    /// explicitly supplied, falls back to HTTP and persists that transport via
    /// [`update_transport_type`] so subsequent requests use `http://`.
    async fn is_reachable_redfish_async(&mut self, trace: TraceFlags) -> (bool, String) {
        let url = format!("{}/redfish/v1/Chassis", self.base_url);

        let result = self
            .apply_auth(self.client.get(&url))
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(self.reachability_timeout_secs()))
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => (true, String::new()),
            Ok(resp) => {
                let status = resp.status();
                tracing::debug!(
                    cli_verbose = trace.cli_verbose,
                    json_mode = trace.json_mode,
                    "HTTPS reachability returned non-success status: {status}"
                );
                (
                    false,
                    format!("Failed to connect to the system via HTTPS ({status})"),
                )
            }
            Err(e) => {
                let https_failure_message =
                    tls_certificate_verification_message_for_detail(&url, &error_chain_message(&e));
                if let Some(message) = https_failure_message {
                    return (false, message);
                }

                if self.session_token().is_some() {
                    return (
                        false,
                        "Connection Error: Failed to connect with the system.".to_string(),
                    );
                }

                if !self.allow_http {
                    tracing::debug!(
                        cli_verbose = trace.cli_verbose,
                        json_mode = trace.json_mode,
                        "Failed to connect to the system via HTTPS; HTTP fallback is disabled"
                    );
                    return (
                        false,
                        format!(
                            "Failed to connect to the system via HTTPS. Cleartext HTTP fallback \
                             is disabled by default; pass {ALLOW_HTTP_ARG}=true only for trusted \
                             HTTP-only targets or port-forwarding cases."
                        ),
                    );
                }

                tracing::warn!(
                    cli_verbose = trace.cli_verbose,
                    json_mode = trace.json_mode,
                    "Cleartext HTTP fallback is enabled for Redfish reachability"
                );

                // Try HTTP fallback.
                let http_url = url.replacen("https://", "http://", 1);

                let fallback = self
                    .apply_auth(self.client.get(&http_url))
                    .header("Content-Type", "application/json")
                    .timeout(Duration::from_secs(self.reachability_timeout_secs()))
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
        let platform_url = format!("{}/nvue_v1/platform", self.base_url);
        let result = self
            .apply_auth(self.client.get(&platform_url))
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
                    // raw text. Keep that legacy text-mode output, but do not
                    // contaminate JSON-mode stdout.
                    if let Ok(body) = resp.text().await {
                        if let Some(diagnostic) =
                            nvswitch_reachability_body_diagnostic(trace, &body)
                        {
                            println!("{diagnostic}");
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

    /// GET a same-origin Redfish resource without discarding its HTTP status.
    /// Callers must distinguish a missing task from transport or JSON failures.
    pub(crate) async fn dispatch_get_response(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<reqwest::Response, String> {
        let url = self.same_origin_request_url(url, "Redfish request URI")?;
        self.apply_auth(self.client.get(url))
            .header("Content-Type", "application/json")
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| NvUtils::sanitize_log(&error_chain_message(&error)))
    }

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

    fn default_request_timeout_secs(&self) -> u64 {
        self.compute_timing.map_or(DEFAULT_TIMEOUT_SECS, |timing| {
            timing.request_timeout.as_secs()
        })
    }

    fn reachability_timeout_secs(&self) -> u64 {
        self.compute_timing
            .map_or(REACHABILITY_TIMEOUT_SECS, |timing| {
                timing.reachability_timeout.as_secs()
            })
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
            self.default_request_timeout_secs(),
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
        let empty = Value::Object(serde_json::Map::new());
        let full_url = match self.same_origin_request_url(url, "Redfish request URI") {
            Ok(full_url) => full_url,
            Err(message) => {
                if !suppress_err {
                    Util::bail_nvfwupd(1, &message, BailAction::DoNothing, json_prints.as_deref());
                }
                return (false, json!({ "error": message }));
            }
        };

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
        self.dispatch_get_with_timeout_async(
            trace,
            url,
            self.reachability_timeout_secs(),
            suppress_err,
        )
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
            .apply_auth(self.client.get(url))
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
            .apply_auth(self.client.get(url))
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
                .apply_auth(self.client.patch(url))
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

        let json_body = param_data.or_else(|| input_data.filter(|value| !value.is_string()));

        if let Some(params) = json_body {
            // POST with JSON body.
            let result = self
                .apply_auth(self.client.post(url))
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
                .apply_auth(self.client.post(url))
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

            let login_resp = if self.session_token().is_none() {
                let login_url = format!("{}/login", self.base_url);

                let login_body = serde_json::json!({
                    "data": [self.user, self.password]
                });

                Some(
                    self.client
                        .post(&login_url)
                        .header("Content-Type", "application/json")
                        .json(&login_body)
                        .timeout(Duration::from_secs(30))
                        .send()
                        .await,
                )
            } else {
                None
            };

            let mut auth_token = self.session_token().unwrap_or_default().to_owned();

            if let Some(login_result) = login_resp {
                match login_result {
                    Ok(resp) if resp.status().as_u16() == 200 => {
                        let session_cookie = resp
                            .headers()
                            .get_all("Set-Cookie")
                            .iter()
                            .filter_map(|value| value.to_str().ok())
                            .find_map(|cookie_str| {
                                cookie_str
                                    .split(';')
                                    .find_map(|part| part.trim().strip_prefix("SESSION="))
                            })
                            .filter(|session| !session.is_empty());

                        match session_cookie {
                            Some(session) => auth_token = session.to_string(),
                            None => {
                                Util::bail_nvfwupd(
                                    1,
                                    "Unable to get valid token for POST message",
                                    BailAction::DoNothing,
                                    json_prints.as_deref(),
                                );
                                return (false, empty);
                            }
                        }
                    }
                    Ok(_) => {
                        Util::bail_nvfwupd(
                            1,
                            "Unable to get valid token for POST message",
                            BailAction::DoNothing,
                            json_prints.as_deref(),
                        );
                        return (false, empty);
                    }
                    Err(e) => {
                        Util::bail_nvfwupd(
                            1,
                            &format!("Error: Error sending HTTPs request: {e}"),
                            BailAction::DoNothing,
                            json_prints.as_deref(),
                        );
                        return (false, empty);
                    }
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
                req = req.header("X-Auth-Token", sensitive_header_value(&auth_token));
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
        let time_out = self
            .compute_timing
            .map_or(time_out, |timing| timing.upload_timeout.as_secs());
        let full_url = match self.same_origin_request_url(url, "HTTP push update URI") {
            Ok(full_url) => full_url,
            Err(message) => {
                tracing::warn!(
                    parallel_update,
                    json_mode = json_output.is_some(),
                    "{message}"
                );
                return (false, json!({ "error": message }));
            }
        };

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
            .apply_auth(self.client.post(&full_url))
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
    /// See [`Self::multipart_file_upload_with_options`] for the HTTP/JSON
    /// success boolean and diagnostic-history contract.
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
    ///
    /// See [`Self::multipart_file_upload_with_options`] for the HTTP/JSON
    /// success boolean and diagnostic-history contract.
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
    ///
    /// Returns `(http_json_success, response)`. The boolean is true only for
    /// HTTP 200/202 with valid JSON; it does not guarantee a usable task ID.
    /// Callers must validate the task reference before declaring an update
    /// started. The sync/default wrappers retain this same contract.
    ///
    /// Failed-submission diagnostics are appended to `Output` in call order.
    /// Reusing the output object preserves earlier failures, even after a later
    /// upload succeeds; those historical entries do not change this call's
    /// boolean result. Each diagnostic's body and truncation flag are computed
    /// from the same redacted text.
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

        let time_out = self
            .compute_timing
            .map_or(time_out, |timing| timing.upload_timeout.as_secs());
        let full_url = match self.same_origin_request_url(url, "multipart update URI") {
            Ok(full_url) => full_url,
            Err(message) => {
                Self::report_multipart_preflight_failure(
                    &message,
                    options.bail_on_failure,
                    parallel_update,
                );
                return (false, empty);
            }
        };

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

        let result = self
            .apply_auth(self.client.post(&full_url))
            .multipart(form)
            .timeout(Duration::from_secs(time_out))
            .send()
            .await;

        let (success, response, diagnostic) = match result {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let mut headers = serde_json::Map::new();
                for name in ["location", "retry-after", "content-type", "x-request-id"] {
                    if let Some(value) = resp.headers().get(name).and_then(|v| v.to_str().ok()) {
                        headers.insert(
                            name.to_string(),
                            json!(self.bounded_submission_text(value, 512)),
                        );
                    }
                }
                match resp.text().await {
                    Ok(body) => {
                        let parsed = serde_json::from_str::<Value>(&body);
                        let accepted = matches!(status, 200 | 202);
                        let category = match (accepted, parsed.as_ref()) {
                            (false, _) => "http_rejection",
                            (true, Err(_)) => "json_decode_failure",
                            (true, Ok(value)) => {
                                if crate::rf_target::task_id_from_update_response(value).is_some() {
                                    "accepted"
                                } else if ["Id", "@odata.id", "Messages"]
                                    .iter()
                                    .any(|key| value.get(key).is_some())
                                {
                                    "malformed_task_reference"
                                } else {
                                    "missing_task_reference"
                                }
                            }
                        };
                        let redacted = match &parsed {
                            Ok(value) => NvUtils::redact_secret_json_value(value).to_string(),
                            Err(_) => body,
                        };
                        let sanitized = self.bounded_submission_text(&redacted, usize::MAX);
                        let body_truncated = sanitized.chars().count() > 4096;
                        let body: String = sanitized.chars().take(4096).collect();
                        let diagnostic = json!({
                            "category": category,
                            "http_status": status,
                            "headers": headers,
                            "body": body,
                            "body_truncated": body_truncated,
                        });
                        (
                            accepted && parsed.is_ok(),
                            parsed.unwrap_or(Value::Null),
                            diagnostic,
                        )
                    }
                    Err(error) => (
                        false,
                        Value::Null,
                        json!({
                            "category": "response_read_failure",
                            "http_status": status,
                            "headers": headers,
                            "message": self.bounded_submission_text(&error.without_url().to_string(), 512),
                        }),
                    ),
                }
            }
            Err(error) => (
                false,
                Value::Null,
                json!({
                    "category": "transport_failure",
                    "message": self.bounded_submission_text(&error.without_url().to_string(), 512),
                }),
            ),
        };
        if let Some(output) = json_prints {
            if let Ok(mut output) = output.lock() {
                // Keep the CLI's Error/Error Code/Output envelope intact. Parallel
                // workers merge Output entries, so diagnostics must travel there.
                if let Some(entries) = output.get_mut("Output").and_then(Value::as_array_mut) {
                    // Failed attempts form an ordered history when callers
                    // reuse an output object. A later success must not erase it.
                    if diagnostic["category"] != "accepted" {
                        entries.push(json!({"SubmissionDiagnostic": diagnostic}));
                    }
                }
            }
        }
        if diagnostic["category"] != "accepted" {
            tracing::warn!(json_mode = json_prints.is_some(), submission_diagnostic = %diagnostic, "Firmware submission did not return a usable task reference");
            // JSON-mode callers already have the diagnostic in their output object.
            // Passing None to the legacy bail helper would print plain text to stdout.
            if options.bail_on_failure && json_prints.is_none() {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &format!("Firmware submission failed: {diagnostic}"),
                    BailAction::DoNothing,
                    None,
                    parallel_update,
                );
            }
        }
        // Legacy callers may also append the parsed failure response to their
        // JSON output. Mask its string values before handing it back, rather
        // than protecting only the bounded diagnostic above.
        let response = if diagnostic["category"] == "accepted" {
            response
        } else {
            self.redacted_submission_response(&response)
        };
        (success, response)
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
                        self.default_request_timeout_secs(),
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

        if status {
            let image_url = format!("{}{}", self.base_url, nvos_image::SYSTEM_IMAGE_URI);
            let (image_status, image_response) =
                self.dispatch_get_async(trace, &image_url, true).await;
            if image_status {
                if let Some(entry) = nvos_image::inventory_entry_from_system_image(&image_response)
                {
                    inv_dict.insert(nvos_image::NVOS_AP_NAME.to_string(), entry);
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
            .get_resource_members_with_response(
                trace,
                &fw_inv_uri,
                self.default_request_timeout_secs(),
            )
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
            let full_url =
                match self.same_origin_request_url(inv_url, "FirmwareInventory member URI") {
                    Ok(full_url) => full_url,
                    Err(message) => {
                        inv_error = 1;
                        tracing::warn!("{message}");
                        continue;
                    }
                };
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

        // Add LiteOn PowerUnits inventory from its fixed URI. PF-1114-1RD
        // exposes this resource even though Redfish discovery omits its links.
        let model_hint = model.unwrap_or(self.model.as_str());

        if liteon_oem_powerunits_shelf(model_hint) {
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
                if let Some(inv_uri) = inv_uri {
                    let mut firmware_device = firmware_device.clone();
                    if let Value::Object(ref mut device) = firmware_device {
                        if device.get("Version").map(Value::is_null).unwrap_or(true) {
                            device.insert("Version".to_string(), Value::String(String::new()));
                        }
                    }
                    inv_dict.insert(inv_uri, firmware_device);
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
            let full_url =
                match self.same_origin_request_url(inv_url, "SoftwareInventory member URI") {
                    Ok(full_url) => full_url,
                    Err(message) => {
                        tracing::warn!("{message}");
                        continue;
                    }
                };
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
            .get_resource_members_with_response(trace, url, self.reachability_timeout_secs())
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
                    .filter_map(|m| {
                        let uri = m.get("@odata.id").and_then(Value::as_str)?;
                        match self.same_origin_request_path(uri, "Redfish collection member URI") {
                            Ok(path) => Some(path),
                            Err(message) => {
                                tracing::warn!("{message}");
                                None
                            }
                        }
                    })
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

    /// Select the configured host resource only for compute power operations.
    pub(crate) async fn compute_power_system_members(&self, trace: TraceFlags) -> Vec<String> {
        match self.compute_power_paths.system_state_path.as_deref() {
            Some(path) => vec![path.to_owned()],
            None => self.get_systems_members(trace).await,
        }
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
                let full_url =
                    match self.same_origin_request_url(device_uri, "LiteOn PowerUnit member URI") {
                        Ok(full_url) => full_url,
                        Err(message) => {
                            tracing::warn!("{message}");
                            continue;
                        }
                    };
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
        let empty = Value::Object(serde_json::Map::new());
        let transport_url = match self.same_origin_request_url(url, "NVUE REST request URI") {
            Ok(transport_url) => transport_url,
            Err(message) => {
                Util::bail_nvfwupd(1, &message, BailAction::DoNothing, print_json.as_deref());
                return (false, empty);
            }
        };
        self.dispatch_get_with_timeout_async(trace, &transport_url, time_out, false)
            .await
    }

    /// GET an NVUE path and distinguish an HTTP answer from a switch that did not respond.
    ///
    /// A request this method could not send counts as an answer, since the
    /// switch was never asked and so cannot be presumed down.
    pub(crate) async fn observe_nvue_get(&self, url: &str, time_out: u64) -> ObservedRead {
        let transport_url = match self.same_origin_request_url(url, "NVUE REST request URI") {
            Ok(transport_url) => transport_url,
            Err(_) => return ObservedRead::answered_unparsed(),
        };

        let response = match self
            .apply_auth(self.client.get(&transport_url))
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(time_out))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => return ObservedRead::NoResponse(error.to_string()),
        };

        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        match serde_json::from_str::<Value>(&text) {
            Ok(value) if status == 200 => ObservedRead::Readable(value),
            Ok(value) => ObservedRead::Answered(value),
            Err(_) => ObservedRead::answered_unparsed(),
        }
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
        let empty = Value::Object(serde_json::Map::new());
        let transport_url = match self.same_origin_request_url(url, "NVUE REST request URI") {
            Ok(transport_url) => transport_url,
            Err(message) => {
                Util::bail_nvfwupd(1, &message, BailAction::DoNothing, print_json.as_deref());
                return (false, empty, String::new());
            }
        };

        let result = self
            .apply_auth(self.client.post(&transport_url))
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
                .apply_auth(self.client.get(&transport_url))
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
        self.scp_upload_async_result_with_timeout(
            local_path,
            remote_dir,
            remote_filename,
            print_json,
            SWITCH_SSH_UPLOAD_TIMEOUT_SECS,
        )
        .await
    }

    /// Async SFTP upload helper that allows callers to use a larger upload timeout.
    pub(crate) async fn scp_upload_async_result_with_timeout(
        &self,
        local_path: &str,
        remote_dir: &str,
        remote_filename: &str,
        print_json: bool,
        upload_timeout_secs: u64,
    ) -> std::result::Result<String, String> {
        let ip_clean = self.ip.replace('[', "").replace(']', "");
        let remote_dir = remote_dir.trim();
        let remote_path = switch_remote_upload_path(remote_dir, remote_filename)?;

        // The typed `ShellCommand`s below are rendered to strings and handed to
        // the legacy `ssh_transport` upload path rather than `NvSwitchCliHandler`
        // on purpose: the handler has no file-transfer primitive, and these
        // setup commands must run in the *same* SSH session as the SFTP transfer
        // that follows, which the handler cannot share. Constructing them as
        // typed commands still gets the `RemotePath` validation (rejecting
        // NUL/newline/option-shaped paths) before they are flattened to strings.
        let setup_commands = vec![
            ShellCommand::CreateDirectoryTree {
                path: RemotePath::new(remote_dir).map_err(|e| e.to_string())?,
            }
            .to_string(),
            ShellCommand::RemoveFile {
                path: RemotePath::new(remote_path.clone()).map_err(|e| e.to_string())?,
            }
            .to_string(),
        ];
        let local_path_buf = Path::new(local_path).to_path_buf();
        let started = Instant::now();
        let attempt_timeout_secs = SWITCH_SSH_CONNECT_TIMEOUT_SECS + upload_timeout_secs;
        let retry_window =
            Duration::from_secs((2 * attempt_timeout_secs) + SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS);
        let retry_interval = Duration::from_secs(SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS);
        let mut attempt = 0_u32;
        let progress_label = current_switch_sftp_progress_label();

        loop {
            attempt += 1;
            let start_msg = format!(
                "Starting SFTP upload attempt {attempt} to {remote_path} (timeout: {upload_timeout_secs}s)"
            );
            let display_start_msg =
                format_labeled_switch_sftp_message(progress_label.as_deref(), &start_msg);
            tracing::info!(
                log_only = true,
                attempt,
                remote_path = %remote_path,
                progress_label = progress_label.as_deref().unwrap_or(""),
                upload_timeout_secs,
                "starting switch SFTP upload attempt"
            );
            if !print_json {
                println!("{}", NvUtils::sanitize_log(&display_start_msg));
            }

            let progress_reporter: ssh_transport::SftpUploadProgressReporter = {
                let progress_label = progress_label.clone();
                std::sync::Arc::new(move |progress| {
                    let msg = format_switch_sftp_upload_progress(attempt, progress);
                    let display_msg =
                        format_labeled_switch_sftp_message(progress_label.as_deref(), &msg);
                    tracing::info!(
                        log_only = true,
                        attempt,
                        progress_label = progress_label.as_deref().unwrap_or(""),
                        uploaded_bytes = progress.uploaded_bytes,
                        total_bytes = progress.total_bytes.unwrap_or(0),
                        elapsed_secs = progress.elapsed_secs,
                        summary = %display_msg,
                        "switch SFTP upload progress"
                    );
                    if !print_json {
                        println!("{}", NvUtils::sanitize_log(&display_msg));
                    }
                })
            };

            match ssh_transport::upload_file_with_setup_async(
                &ip_clean,
                22,
                &self.user,
                &self.password,
                SWITCH_SSH_CONNECT_TIMEOUT_SECS,
                SWITCH_SSH_SETUP_TIMEOUT_SECS,
                upload_timeout_secs,
                &setup_commands,
                local_path_buf.clone(),
                remote_path.clone(),
                self.ssh_host_key_policy(),
                Some(progress_reporter),
            )
            .await
            {
                Ok(()) => {
                    if !print_json {
                        let msg = format!("Update file {} was uploaded successfully", local_path);
                        println!(
                            "{}",
                            NvUtils::sanitize_log(&format_labeled_switch_sftp_message(
                                progress_label.as_deref(),
                                &msg
                            ))
                        );
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
                            attempt_timeout_secs,
                        )
                    {
                        let msg = format!(
                            "SSH setup/upload failed after {} attempt(s) over {}s: {}",
                            attempt,
                            elapsed.as_secs(),
                            e
                        );
                        if !print_json {
                            println!(
                                "{}",
                                NvUtils::sanitize_log(&format_labeled_switch_sftp_message(
                                    progress_label.as_deref(),
                                    &msg
                                ))
                            );
                        }
                        return Err(NvUtils::sanitize_log(&msg));
                    }

                    if !print_json {
                        let msg = format!(
                            "SSH setup/upload attempt {} failed after {}s: {}; retrying in {}s",
                            attempt,
                            elapsed.as_secs(),
                            e,
                            SWITCH_SSH_UPLOAD_RETRY_INTERVAL_SECS
                        );
                        println!(
                            "{}",
                            NvUtils::sanitize_log(&format_labeled_switch_sftp_message(
                                progress_label.as_deref(),
                                &msg
                            ))
                        );
                    }
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
    }

    /// Remove a single switch-side file that was previously staged for upload.
    pub(crate) async fn scp_remove_remote_file_async_result(
        &self,
        remote_path: &str,
    ) -> std::result::Result<(), String> {
        let remote_path = switch_remote_cleanup_path(remote_path)?;
        let remote_path = RemotePath::new(remote_path).map_err(|e| e.to_string())?;

        self.switch_cli(SWITCH_SSH_CONNECT_TIMEOUT_SECS)
            .remove_file(&remote_path)
            .await
            .map_err(|e| format!("SSH cleanup command failed: {e}"))
    }

    pub(crate) fn switch_cli(&self, timeout_secs: u64) -> SwitchCliHandler {
        SwitchCliTransport::new(
            &self.ip,
            &self.user,
            &self.password,
            self.ssh_host_key_policy(),
        )
        .into_handler(timeout_secs)
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
            allow_http: false,
            access_type: AccessType::Login,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            compute_timing: None,
            compute_power_paths: Default::default(),
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
            allow_http: false,
            access_type,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            compute_timing: None,
            compute_power_paths: Default::default(),
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
    use wiremock::matchers::{header, method, path};
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
    fn sensitive_header_value_marks_valid_token_sensitive() {
        let value = sensitive_header_value("a-session-token");

        assert!(value.is_sensitive());
        assert_eq!(value.to_str().unwrap(), "a-session-token");
    }

    #[test]
    fn sensitive_header_value_falls_back_on_invalid_token() {
        let value = sensitive_header_value("invalid\ntoken");

        assert!(value.is_sensitive());
        assert_eq!(value.to_str().unwrap(), "invalid-session-token");
    }

    #[test]
    fn test_access_type_variants() {
        assert_ne!(AccessType::Login, AccessType::PortForward);
        assert_ne!(AccessType::Login, AccessType::NVSwitch);
        assert_ne!(AccessType::PortForward, AccessType::NVSwitch);
    }

    #[test]
    fn same_origin_request_url_accepts_paths_and_same_origin_absolute_urls() {
        let access = BmcAccess::mock_with_base_url("https://10.0.0.1:8443", "mock");

        assert_eq!(
            access
                .same_origin_request_url("/redfish/v1/Systems/0", "test URI")
                .unwrap(),
            "https://10.0.0.1:8443/redfish/v1/Systems/0"
        );
        assert_eq!(
            access
                .same_origin_request_url(
                    "https://10.0.0.1:8443/redfish/v1/Systems/0?expand=1",
                    "test URI",
                )
                .unwrap(),
            "https://10.0.0.1:8443/redfish/v1/Systems/0?expand=1"
        );
        assert_eq!(
            access
                .same_origin_request_path(
                    "https://10.0.0.1:8443/redfish/v1/Systems/0?expand=1",
                    "test URI",
                )
                .unwrap(),
            "/redfish/v1/Systems/0?expand=1"
        );
    }

    #[test]
    fn same_origin_request_url_rejects_unsafe_or_off_origin_urls() {
        let access = BmcAccess::mock_with_base_url("https://10.0.0.1", "mock");

        for uri in [
            "",
            "redfish/v1/Systems/0",
            "https://attacker.example/redfish/v1/Systems/0",
            "http://10.0.0.1/redfish/v1/Systems/0",
            "https://user:pass@10.0.0.1/redfish/v1/Systems/0",
            "//attacker.example/redfish/v1/Systems/0",
            "/redfish/v1/Systems/0\nX-Test: yes",
            "/redfish/v1/Systems/0\u{7}",
        ] {
            let err = access
                .same_origin_request_url(uri, "test URI")
                .expect_err("unsafe or off-origin URI should be rejected");
            assert!(err.contains("test URI"), "unexpected error: {err}");
        }
    }

    #[test]
    fn bmc_tls_verification_defaults_to_disabled_mode() {
        assert!(!BmcAccess::should_verify_bmc_tls(None, None).unwrap());
        assert!(BmcAccess::build_client_from_tls_options(1, None, None, true).is_ok());
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
    fn bmc_http_fallback_defaults_to_disabled() {
        assert!(!BmcAccess::should_allow_http_fallback(None).unwrap());
        assert!(BmcAccess::should_allow_http_fallback(Some("true")).unwrap());
        assert!(!BmcAccess::should_allow_http_fallback(Some("off")).unwrap());

        let err = BmcAccess::should_allow_http_fallback(Some("maybe")).unwrap_err();
        assert!(err.contains(ALLOW_HTTP_ARG));
        assert!(err.contains("true/false"));
    }

    #[tokio::test]
    async fn redfish_reachability_does_not_fallback_to_http_by_default() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        let port = server.address().port();
        let mut access =
            BmcAccess::mock_with_base_url(format!("https://127.0.0.1:{port}"), "127.0.0.1");
        access.port = port.to_string();
        access.transport_type = "https".to_string();

        let (reachable, message) = access
            .is_reachable_redfish_async(TraceFlags::default())
            .await;

        assert!(!reachable);
        assert!(message.contains("HTTP fallback is disabled"));
        assert!(message.contains("allow_http=true"));
        assert_eq!(access.transport_type, "https");
        assert_eq!(access.base_url, format!("https://127.0.0.1:{port}"));
    }

    #[tokio::test]
    async fn redfish_reachability_fallbacks_to_http_when_explicitly_allowed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        let port = server.address().port();
        let mut access =
            BmcAccess::mock_with_base_url(format!("https://127.0.0.1:{port}"), "127.0.0.1");
        access.port = port.to_string();
        access.transport_type = "https".to_string();
        access.allow_http = true;

        let (reachable, message) = access
            .is_reachable_redfish_async(TraceFlags::default())
            .await;

        assert!(reachable, "{message}");
        assert_eq!(message, "");
        assert_eq!(access.transport_type, "http");
        assert_eq!(access.base_url, format!("http://127.0.0.1:{port}"));
    }

    #[tokio::test]
    async fn redfish_reachability_does_not_fallback_on_https_status_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({})))
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "127.0.0.1");
        access.transport_type = "https".to_string();
        access.allow_http = true;

        let (reachable, message) = access
            .is_reachable_redfish_async(TraceFlags::default())
            .await;

        assert!(!reachable);
        assert!(message.contains("Failed to connect to the system via HTTPS (401"));
        assert!(!message.contains("HTTP fallback is disabled"));
        assert_eq!(access.transport_type, "https");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[test]
    fn bmc_ca_cert_path_keeps_verification_enabled_when_verify_arg_is_unset() {
        assert!(BmcAccess::should_verify_bmc_tls(None, Some("/tmp/bmc-ca.pem")).unwrap());
        assert!(!BmcAccess::should_verify_bmc_tls(None, Some("   ")).unwrap());
    }

    #[test]
    fn bmc_client_builder_loads_ca_only_when_verification_is_enabled() {
        let missing_path = "/tmp/nvfwupd-definitely-missing-bmc-ca.pem";

        let err = BmcAccess::build_client_from_tls_options(1, None, Some(missing_path), true)
            .unwrap_err();
        assert!(err.contains(BMC_CA_CERT_ARG));
        assert!(err.contains(missing_path));

        assert!(
            BmcAccess::build_client_from_tls_options(1, Some("0"), None, true).is_ok(),
            "explicitly disabled verification should allow insecure/self-signed endpoints"
        );
    }

    #[test]
    fn bmc_ca_cert_is_rejected_when_verification_is_disabled() {
        let missing_path = "/tmp/nvfwupd-definitely-missing-bmc-ca.pem";
        let err = BmcAccess::build_client_from_tls_options(1, Some("0"), Some(missing_path), true)
            .unwrap_err();
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
        let _guard = crate::utils::SanitizeTestGuard::new();
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
        assert!(message.contains("Certificate validation is enabled for this connection"));
        assert!(!message.contains("bmc_ca_cert=/path/to/ca.pem"));
        assert!(!message.contains("verify_tls=false"));
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
    fn nvswitch_reachability_body_diagnostic_is_suppressed_in_json_mode() {
        let html_401 = "<html><body>401 Authorization Required</body></html>";

        assert_eq!(
            nvswitch_reachability_body_diagnostic(TraceFlags::new(false, true), html_401),
            None
        );
        assert_eq!(
            nvswitch_reachability_body_diagnostic(TraceFlags::new(false, false), html_401)
                .as_deref(),
            Some(html_401)
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
            retry_window,
            SWITCH_SSH_UPLOAD_ATTEMPT_TIMEOUT_SECS
        ));
        assert!(switch_ssh_upload_has_retry_budget(
            last_valid_elapsed,
            retry_interval,
            retry_window,
            SWITCH_SSH_UPLOAD_ATTEMPT_TIMEOUT_SECS
        ));
        assert!(!switch_ssh_upload_has_retry_budget(
            last_valid_elapsed + Duration::from_secs(1),
            retry_interval,
            retry_window,
            SWITCH_SSH_UPLOAD_ATTEMPT_TIMEOUT_SECS
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

    #[tokio::test]
    async fn switch_sftp_progress_label_prefixes_status_lines() {
        assert_eq!(
            BmcAccess::format_switch_sftp_status_line("Starting SFTP upload"),
            "Starting SFTP upload"
        );

        let labeled =
            BmcAccess::with_switch_sftp_progress_label("  Switch\nA  ".to_string(), async {
                BmcAccess::format_switch_sftp_status_line("Starting SFTP upload")
            })
            .await;

        assert_eq!(labeled, "[Switch A] Starting SFTP upload");
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

    #[tokio::test]
    async fn scp_remove_remote_file_sanitizes_transport_errors() {
        // Serialize with other tests that mutate the global sanitize state and
        // pin `is_sanitize=true` + the default config, so the `ip=`/`password=`
        // redaction assertions below are deterministic under parallel runs.
        let _guard = crate::utils::SanitizeTestGuard::new();
        let mock_host = "mock-switch-cleanup-transport-error";
        ssh_transport::install_mock_exec_error_for_host(
            mock_host,
            "connect failed for ip=192.0.2.105 password=plain_secret",
        );
        let access = BmcAccess::mock_with_base_url_and_type(
            "http://127.0.0.1",
            mock_host,
            AccessType::NVSwitch,
        );

        let err = access
            .scp_remove_remote_file_async_result("/host/nos-images/nvos.bin")
            .await
            .expect_err("cleanup transport errors should be returned");

        assert!(err.contains("SSH cleanup command failed"));
        assert!(err.contains("ip=XXXX"));
        assert!(err.contains("password=XXXX"));
        assert!(!err.contains("192.0.2.105"));
        assert!(!err.contains("plain_secret"));
        let _ = ssh_transport::take_mock_snapshot(mock_host);
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
            allow_http: false,
            access_type: AccessType::Login,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            compute_timing: None,
            compute_power_paths: Default::default(),
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
            allow_http: false,
            access_type: AccessType::Login,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            compute_timing: None,
            compute_power_paths: Default::default(),
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
            allow_http: false,
            access_type: AccessType::Login,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            compute_timing: None,
            compute_power_paths: Default::default(),
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
    fn cli_login_without_username_remains_basic_auth() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("password".to_string(), "session-token".to_string());

        let access = BmcAccess::new_login(&args).unwrap();

        assert_eq!(access.access_type, AccessType::Login);
        assert!(access.session_token().is_none());
    }

    #[test]
    fn new_login_with_session_token_uses_sentinel_user() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("session_token".to_string(), "a-real-token".to_string());

        let access = BmcAccess::new_login(&args).unwrap();

        assert_eq!(access.user, BMC_SESSION_TOKEN_USER);
        assert_eq!(access.session_token(), Some("a-real-token"));
    }

    #[test]
    fn new_login_trims_whitespace_padded_session_token() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("session_token".to_string(), "  a-real-token  ".to_string());

        let access = BmcAccess::new_login(&args).unwrap();

        assert_eq!(access.session_token(), Some("a-real-token"));
    }

    #[test]
    fn new_login_rejects_whitespace_only_session_token() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("session_token".to_string(), "   ".to_string());

        let err = BmcAccess::new_login(&args).unwrap_err();

        assert!(err.contains("must not be empty"));
    }

    #[test]
    fn new_login_rejects_session_token_combined_with_user() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("session_token".to_string(), "a-real-token".to_string());

        let err = BmcAccess::new_login(&args).unwrap_err();

        assert!(err.contains("cannot be combined with user/password"));
    }

    #[test]
    fn new_login_rejects_empty_session_token() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("session_token".to_string(), String::new());

        let err = BmcAccess::new_login(&args).unwrap_err();

        assert!(err.contains("must not be empty"));
    }

    #[test]
    fn new_login_rejects_invalid_session_token_header_value() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("session_token".to_string(), "invalid\ntoken".to_string());

        let err = BmcAccess::new_login(&args).unwrap_err();

        assert!(err.contains("invalid session_token header value"));
    }

    #[test]
    fn new_nvswitch_rejects_session_token() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.5".to_string());
        args.insert("servertype".to_string(), "gb200switch".to_string());
        args.insert("session_token".to_string(), "a-real-token".to_string());

        let err = BmcAccess::new_nvswitch(&args).unwrap_err();

        assert!(err.contains("not supported for NVUE/NVOS switch targets"));
    }

    #[tokio::test]
    async fn new_login_with_session_token_does_not_follow_redirects() {
        let destination = MockServer::start().await;
        let source = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("Location", destination.uri().as_str()),
            )
            .expect(1)
            .mount(&source)
            .await;

        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.1".to_string());
        args.insert("session_token".to_string(), "a-real-token".to_string());

        let access = BmcAccess::new_login(&args).unwrap();

        let response = access
            .apply_auth(access.client.get(format!("{}/redirect", source.uri())))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        assert!(destination.received_requests().await.unwrap().is_empty());
    }

    #[test]
    fn probe_has_enough_args_accepts_ip_and_session_token_shorthand() {
        // A reachable BMCLoginAccess probe (expected_args=3, ip+user+password)
        // must not be discarded just because ip+session_token is only 2 keys.
        assert!(BmcAccess::probe_has_enough_args(2, 3, true));
    }

    #[test]
    fn probe_has_enough_args_rejects_short_non_token_probe() {
        assert!(!BmcAccess::probe_has_enough_args(2, 3, false));
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
    fn test_new_login_with_port() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.5".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "pass".to_string());
        args.insert("port".to_string(), "2444".to_string());

        let access = BmcAccess::new_login_with_port(&args).unwrap();
        assert_eq!(access.access_type, AccessType::Login);
        assert_eq!(access.port, "2444");
        assert_eq!(access.base_url, "https://10.0.0.5:2444");
    }

    #[test]
    fn test_new_login_with_port_ipv6() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "fd00::1".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "pass".to_string());
        args.insert("port".to_string(), "2444".to_string());

        let access = BmcAccess::new_login_with_port(&args).unwrap();
        assert_eq!(access.ip, "[fd00::1]");
        assert_eq!(access.port, "2444");
        assert_eq!(access.base_url, "https://[fd00::1]:2444");
    }

    #[test]
    fn test_new_login_with_port_rejects_invalid_port() {
        let mut args = HashMap::new();
        args.insert("ip".to_string(), "10.0.0.5".to_string());
        args.insert("user".to_string(), "admin".to_string());
        args.insert("password".to_string(), "pass".to_string());

        args.insert("port".to_string(), " ".to_string());
        assert_eq!(
            BmcAccess::new_login_with_port(&args).unwrap_err(),
            "invalid port: "
        );

        args.insert("port".to_string(), "not-a-port".to_string());
        assert_eq!(
            BmcAccess::new_login_with_port(&args).unwrap_err(),
            "invalid port: not-a-port"
        );

        args.insert("port".to_string(), "70000".to_string());
        assert_eq!(
            BmcAccess::new_login_with_port(&args).unwrap_err(),
            "invalid port: 70000"
        );
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

    #[tokio::test]
    async fn nvswitch_reachability_uses_configured_base_url() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"platform": "mock"})))
            .expect(1)
            .mount(&server)
            .await;

        let mut access =
            BmcAccess::mock_with_base_url_and_type(server.uri(), "mock", AccessType::NVSwitch);
        let (reachable, message) = access.is_reachable(TraceFlags::default()).await;

        assert!(reachable, "{message}");
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
        access.base_url = "http://127.0.0.1:1".to_string();

        let _ = access
            .dispatch_rest_request_get(trace, "/nvue_v1/platform", TEST_TIMEOUT_SENTINEL_SECS, None)
            .await;

        assert_eq!(
            LAST_TEST_DISPATCH_GET_TIMEOUT_SECS.load(Ordering::SeqCst),
            TEST_TIMEOUT_SENTINEL_SECS
        );
    }

    #[tokio::test]
    async fn dispatch_request_full_rejects_off_origin_before_request() {
        let access = BmcAccess::mock_with_base_url("http://127.0.0.1:1", "mock");

        let (ok, response) = access
            .dispatch_request_full(
                "GET",
                "https://attacker.example/redfish/v1",
                None,
                None,
                30,
                true,
                None,
            )
            .await;

        assert!(!ok);
        assert!(response["error"]
            .as_str()
            .unwrap()
            .contains("must stay on the selected target origin"));
    }

    #[tokio::test]
    async fn dispatch_request_full_accepts_same_origin_absolute_url() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "System_0"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        let uri = format!("{}/redfish/v1/Systems/0", server.uri());
        let (ok, response) = access
            .dispatch_request_full("GET", &uri, None, None, 30, true, None)
            .await;

        assert!(ok);
        assert_eq!(response["Id"], "System_0");
    }

    #[tokio::test]
    async fn file_upload_rejects_off_origin_uri_before_file_read() {
        let access = BmcAccess::mock_with_base_url("http://127.0.0.1:1", "mock");
        let missing_package = "/tmp/nvfwupd-missing-fw-image.fwpkg";

        let (ok, response) = access
            .dispatch_file_upload(
                "https://attacker.example/upload",
                missing_package,
                30,
                None,
                false,
                None,
            )
            .await;

        assert!(!ok);
        assert_eq!(
            response["error"],
            "HTTP push update URI must stay on the selected target origin: https://attacker.example/upload"
        );
        assert_ne!(response["error"], "Failed to read given file");
    }

    #[tokio::test]
    async fn resource_members_are_normalized_and_off_origin_members_skipped() {
        let server = MockServer::start().await;
        let same_origin_absolute = format!("{}/redfish/v1/Systems/System_1", server.uri());
        let same_origin_userinfo = server.uri().replacen("http://", "http://user:pass@", 1);
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Systems/System_0"},
                    {"@odata.id": same_origin_absolute},
                    {"@odata.id": "https://attacker.example/redfish/v1/Systems/System_2"},
                    {"@odata.id": format!("{same_origin_userinfo}/redfish/v1/Systems/System_3")},
                    {"@odata.id": "/redfish/v1/Systems/System_4\nX-Test: yes"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        let members = access
            .get_resource_members(
                TraceFlags::default(),
                &format!("{}/redfish/v1/Systems", server.uri()),
            )
            .await;

        assert_eq!(
            members,
            vec![
                "/redfish/v1/Systems/System_0".to_string(),
                "/redfish/v1/Systems/System_1".to_string(),
            ]
        );
    }

    #[test]
    fn test_phase_2b_public_bmc_access_methods_are_async_send() {
        fn assert_send_future<T: std::future::Future + Send>(_future: T) {}

        let _guard = crate::utils::SanitizeTestGuard::new();
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
    async fn configured_compute_request_timeout_reaches_http_transport() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/slow"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({}))
                    .set_delay(Duration::from_secs(2)),
            )
            .mount(&server)
            .await;
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        access.compute_timing = Some(crate::compute_timing::ComputeFirmwareTiming {
            request_timeout: Duration::from_secs(1),
            ..crate::compute_timing::ComputeFirmwareTiming::default()
        });
        let (ok, _) = access.dispatch_request("GET", "/slow", None, None).await;
        assert!(!ok, "configured request timeout must expire");
        assert_eq!(
            BmcAccess::default_stub().default_request_timeout_secs(),
            DEFAULT_TIMEOUT_SECS
        );
    }

    #[tokio::test]
    async fn configured_upload_timeout_overrides_the_legacy_upload_argument() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_body_json(json!({"Id": "Task-1"}))
                    .set_delay(Duration::from_secs(2)),
            )
            .mount(&server)
            .await;
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"firmware").unwrap();
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        access.compute_timing = Some(crate::compute_timing::ComputeFirmwareTiming {
            upload_timeout: Duration::from_secs(1),
            ..crate::compute_timing::ComputeFirmwareTiming::default()
        });
        let (ok, _) = access
            .multipart_file_upload_with_options(
                "/upload",
                file.path().to_str().unwrap(),
                None,
                30,
                Some(r#"{"Targets":[]}"#),
                None,
                None,
                None,
                false,
                MultipartUploadOptions {
                    bail_on_failure: false,
                },
            )
            .await;
        assert!(
            !ok,
            "profile timeout must replace the legacy 30-second argument"
        );
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
    async fn multipart_upload_uses_session_token_auth() {
        // Firmware updates on session-token-authenticated targets (GB200/
        // GB300/HGX compute) go through this multipart path, not the legacy
        // login-cookie binary-upload path -- confirm apply_auth's session-
        // token branch is actually reached here, not just Basic auth.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .and(header("X-Auth-Token", "session-token"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"Id": "Task-1"})))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("fw.fwpkg");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        access.user = BMC_SESSION_TOKEN_USER.to_owned();
        access.password = "session-token".to_owned();

        let (status, _) = access
            .multipart_file_upload(
                "/upload",
                update_file.to_str().unwrap(),
                None,
                30,
                None,
                None,
                None,
                None,
                false,
            )
            .await;

        assert!(
            status,
            "multipart upload should succeed with a session token"
        );
    }

    #[tokio::test]
    async fn multipart_upload_rejects_off_origin_uri_before_file_read() {
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
                "https://attacker.example/upload",
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
        assert!(output.contains("multipart update URI must stay on the selected target origin"));
        assert!(!output.contains("Failed to read package file"));
    }

    /// Write a throwaway firmware file and dispatch it through `dispatch_post_async`.
    async fn dispatch_test_upload(access: &BmcAccess, upload_url: &str) -> bool {
        let temp_dir = tempfile::tempdir().unwrap();
        let update_file = temp_dir.path().join("fw.bin");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();

        let (success, _) = access
            .dispatch_post_async(
                upload_url,
                Some(&Value::String(update_file.to_string_lossy().into_owned())),
                None,
                30,
                None,
            )
            .await;
        success
    }

    #[tokio::test]
    async fn binary_upload_uses_supplied_session_token_without_login() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .and(header("X-Auth-Token", "session-token"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"Id": "Task-1"})))
            .expect(1)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        access.user = BMC_SESSION_TOKEN_USER.to_owned();
        access.password = "session-token".to_owned();

        assert!(dispatch_test_upload(&access, &format!("{}/upload", server.uri())).await);

        let requests = server.received_requests().await.unwrap();

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url.path(), "/upload");
    }

    #[tokio::test]
    async fn binary_upload_extracts_session_cookie_with_non_word_characters() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Set-Cookie", "SESSION=abc-123=xyz; Path=/; HttpOnly"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .and(header("X-Auth-Token", "abc-123=xyz"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"Id": "Task-1"})))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");

        assert!(
            dispatch_test_upload(&access, &format!("{}/upload", server.uri())).await,
            "upload should succeed with a non-word session id"
        );
    }

    #[tokio::test]
    async fn binary_upload_fails_closed_when_login_response_has_no_session_cookie() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        // A mock that expects zero calls fails the test if /upload is ever
        // reached, proving the upload is never sent without a session token.
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(202))
            .expect(0)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");

        assert!(
            !dispatch_test_upload(&access, &format!("{}/upload", server.uri())).await,
            "upload must fail closed when no session cookie is returned"
        );
    }

    #[tokio::test]
    async fn binary_upload_fails_closed_when_session_cookie_is_empty() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(
                ResponseTemplate::new(200).insert_header("Set-Cookie", "SESSION=; Path=/"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(202))
            .expect(0)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");

        assert!(
            !dispatch_test_upload(&access, &format!("{}/upload", server.uri())).await,
            "an empty SESSION cookie value must be treated as no cookie"
        );
    }

    #[tokio::test]
    async fn binary_upload_fails_closed_when_login_request_errors() {
        // The upload target is a live server with a deny-mock, so the test
        // fails if the upload is ever sent. The login target (base_url) is a
        // bound-then-closed port, so the login POST fails at the transport
        // layer (connection refused) rather than returning a status.
        let upload_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(202))
            .expect(0)
            .mount(&upload_server)
            .await;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let login_base_url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let access = BmcAccess::mock_with_base_url(login_base_url, "mock");

        assert!(
            !dispatch_test_upload(&access, &format!("{}/upload", upload_server.uri())).await,
            "a transport-level login failure must not fall through to an unauthenticated upload"
        );
    }

    #[tokio::test]
    async fn multipart_submission_diagnostics_preserve_failure_context() {
        let _guard = crate::utils::SanitizeTestGuard::new();
        for (status_code, body, category) in [
            (
                409,
                r#"{"error":{"Message":"busy","password":"secret-value"}}"#,
                "http_rejection",
            ),
            (503, "unavailable password=secret-value", "http_rejection"),
            (202, "not-json password=secret-value", "json_decode_failure"),
            (202, "{}", "missing_task_reference"),
            (202, r#"{"Id":""}"#, "malformed_task_reference"),
            (202, r#"{"Id":"42"}"#, "accepted"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/upload"))
                .respond_with(
                    ResponseTemplate::new(status_code)
                        .insert_header("Retry-After", "30")
                        .insert_header("Set-Cookie", "session=private-cookie")
                        .insert_header("Location", "https://bmc/redfish/v1/foo?token=header-secret")
                        .set_body_string(body),
                )
                .mount(&server)
                .await;
            let tmp = tempfile::tempdir().unwrap();
            let file = tmp.path().join("fw.fwpkg");
            tokio::fs::write(&file, b"firmware").await.unwrap();
            let output = Mutex::new(json!({"Error": [], "Error Code": 0, "Output": [
                {"SubmissionDiagnostic": {"category":"http_rejection", "http_status":503, "target_index":7}}
            ]}));
            let access = BmcAccess::mock_with_base_url(server.uri(), "mock");
            let (success, _) = access
                .multipart_file_upload_with_options(
                    "/upload",
                    file.to_str().unwrap(),
                    None,
                    30,
                    None,
                    None,
                    None,
                    Some(&output),
                    false,
                    MultipartUploadOptions {
                        bail_on_failure: false,
                    },
                )
                .await;
            let output = output.into_inner().unwrap();
            assert_eq!(
                success,
                matches!(
                    category,
                    "accepted" | "missing_task_reference" | "malformed_task_reference"
                ),
                "HTTP/JSON success contract changed for {category}"
            );
            assert_eq!(
                output["Output"][0]["SubmissionDiagnostic"],
                json!({
                    "category":"http_rejection", "http_status":503, "target_index":7
                })
            );
            if category == "accepted" {
                assert_eq!(output["Output"].as_array().unwrap().len(), 1);
            } else {
                assert_eq!(output["Output"].as_array().unwrap().len(), 2);
                let diagnostic = &output["Output"][1]["SubmissionDiagnostic"];
                assert_eq!(diagnostic["category"], category);
                assert_eq!(diagnostic["http_status"], status_code);
                assert_eq!(diagnostic["headers"]["retry-after"], "30");
                assert_eq!(
                    diagnostic["headers"]["location"],
                    "https://bmc/redfish/v1/foo?token=XXXX"
                );
                let rendered = diagnostic.to_string();
                assert!(!rendered.contains("secret-value"));
                assert!(!rendered.contains("private-cookie"));
            }
        }
    }

    #[tokio::test]
    async fn multipart_submission_history_survives_success_and_later_failure() {
        let _guard = crate::utils::SanitizeTestGuard::new();
        let server = MockServer::start().await;
        for (route, status, response) in [
            ("/first", 409, json!({"error":{"Message":"first busy"}})),
            ("/success", 202, json!({"Id":"42"})),
            ("/second", 503, json!({"error":{"Message":"second busy"}})),
        ] {
            Mock::given(method("POST"))
                .and(path(route))
                .respond_with(ResponseTemplate::new(status).set_body_json(response))
                .expect(1)
                .mount(&server)
                .await;
        }
        let tmp = tempfile::tempdir().unwrap();
        let package = tmp.path().join("fw.fwpkg");
        tokio::fs::write(&package, b"firmware").await.unwrap();
        let access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        let output = Mutex::new(json!({"Error": [], "Error Code": 0, "Output": []}));
        for (route, expected_success, expected_count) in [
            ("/first", false, 1),
            ("/success", true, 1),
            ("/second", false, 2),
        ] {
            let (success, response) = access
                .multipart_file_upload_with_options(
                    route,
                    package.to_str().unwrap(),
                    None,
                    30,
                    None,
                    None,
                    None,
                    Some(&output),
                    false,
                    MultipartUploadOptions {
                        bail_on_failure: false,
                    },
                )
                .await;
            assert_eq!(success, expected_success);
            let mut output = output.lock().unwrap();
            assert_eq!(output["Output"].as_array().unwrap().len(), expected_count);
            if route == "/first" {
                assert_eq!(response["error"]["Message"], "first busy");
                output["Output"][0]["SubmissionDiagnostic"]["target_index"] = json!(7);
            }
            assert_eq!(
                output["Output"][0]["SubmissionDiagnostic"]["http_status"],
                409
            );
            assert_eq!(
                output["Output"][0]["SubmissionDiagnostic"]["target_index"],
                7
            );
            if route == "/second" {
                assert_eq!(response["error"]["Message"], "second busy");
                assert_eq!(
                    output["Output"][1]["SubmissionDiagnostic"]["http_status"],
                    503
                );
            }
        }
    }

    #[tokio::test]
    async fn multipart_submission_truncation_matches_the_redacted_body() {
        let _guard = crate::utils::SanitizeTestGuard::new();
        let server = MockServer::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let package = tmp.path().join("fw.fwpkg");
        tokio::fs::write(&package, b"firmware").await.unwrap();
        for (index, (body, password, expected_cut, expected_chars)) in [
            ("1.1.1.1 ".repeat(520), "secret", false, 2600),
            ("1.1.1.1 ".repeat(1200), "secret", true, 4096),
            ("x ".repeat(1200), "x", true, 4096),
            ("é".repeat(4096), "secret", false, 4096),
        ]
        .into_iter()
        .enumerate()
        {
            let route = format!("/upload-{index}");
            Mock::given(method("POST"))
                .and(path(route.as_str()))
                .respond_with(ResponseTemplate::new(503).set_body_string(body))
                .expect(1)
                .mount(&server)
                .await;
            let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock");
            access.password = password.to_string();
            let output = Mutex::new(json!({"Error": [], "Error Code": 0, "Output": []}));
            let (success, _) = access
                .multipart_file_upload_with_options(
                    &route,
                    package.to_str().unwrap(),
                    None,
                    30,
                    None,
                    None,
                    None,
                    Some(&output),
                    false,
                    MultipartUploadOptions {
                        bail_on_failure: false,
                    },
                )
                .await;
            assert!(!success);
            let output = output.into_inner().unwrap();
            let diagnostic = &output["Output"][0]["SubmissionDiagnostic"];
            assert_eq!(diagnostic["body_truncated"], expected_cut, "case {index}");
            assert_eq!(
                diagnostic["body"].as_str().unwrap().chars().count(),
                expected_chars,
                "case {index}"
            );
        }
    }

    #[tokio::test]
    async fn multipart_submission_retains_truncated_body_and_read_failures() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

        let _guard = crate::utils::SanitizeTestGuard::new();
        for broken_body in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut content_length = None;
                loop {
                    let mut line = String::new();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            content_length = Some(value.trim().parse::<u64>().unwrap());
                        }
                    }
                }
                // Consume the complete multipart body before closing the socket.
                // A single read can leave unread request bytes and cause a reset.
                let length = content_length.expect("multipart request has a known length");
                let mut request_body = Vec::new();
                (&mut reader)
                    .take(length)
                    .read_to_end(&mut request_body)
                    .await
                    .unwrap();
                assert_eq!(request_body.len() as u64, length);
                assert!(request_body.len() > 8192);
                let mut stream = reader.into_inner();
                let body = if broken_body {
                    "short".to_string()
                } else {
                    "é".repeat(5000)
                };
                let length = if broken_body {
                    body.len() + 100
                } else {
                    body.len()
                };
                let response = format!("HTTP/1.1 503 Unavailable\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}");
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            });
            let tmp = tempfile::tempdir().unwrap();
            let file = tmp.path().join("fw.fwpkg");
            tokio::fs::write(&file, b"firmware".repeat(4096))
                .await
                .unwrap();
            let output = Mutex::new(json!({"Error": [], "Error Code": 0, "Output": []}));
            let access = BmcAccess::mock_with_base_url(format!("http://{address}"), "mock");
            let (success, _) = access
                .multipart_file_upload_with_options(
                    "/upload",
                    file.to_str().unwrap(),
                    None,
                    30,
                    None,
                    None,
                    None,
                    Some(&output),
                    false,
                    MultipartUploadOptions {
                        bail_on_failure: false,
                    },
                )
                .await;
            server.await.unwrap();
            assert!(!success);
            let output = output.into_inner().unwrap();
            let diagnostic = &output["Output"][0]["SubmissionDiagnostic"];
            assert_eq!(diagnostic["http_status"], 503);
            if broken_body {
                assert_eq!(diagnostic["category"], "response_read_failure");
            } else {
                assert_eq!(diagnostic["category"], "http_rejection");
                assert_eq!(diagnostic["body_truncated"], true);
                assert_eq!(diagnostic["body"].as_str().unwrap().chars().count(), 4096);
            }
        }
    }

    #[test]
    fn submission_text_masks_target_credentials_after_global_config_changes() {
        let _guard = crate::utils::SanitizeTestGuard::new();
        let mut access = BmcAccess::mock_with_base_url("http://localhost".to_string(), "mock");
        access.user = "synthetic-operator".to_string();
        access.password = "synthetic-\"secret\\value\nend".to_string();
        // Deterministically model another parallel worker replacing the config.
        NvUtils::set_sanitize_config(Some(HashMap::from([
            ("user".to_string(), "other-operator".to_string()),
            ("password".to_string(), "other-secret".to_string()),
        ])));
        for sanitize in [true, false] {
            NvUtils::set_is_sanitize(sanitize);
            for value in [
                format!("Echoed {} for {}", access.password, access.user),
                json!({"Message": format!("Echoed {} for {}", access.password, access.user)})
                    .to_string(),
                format!("{:?}", json!({"Message": access.password})),
            ] {
                let bounded = access.bounded_submission_text(&value, 4096);
                assert!(!bounded.contains("synthetic-"), "{bounded}");
                assert!(!bounded.contains("secret"), "{bounded}");
                assert!(bounded.contains("XXXX"), "{bounded}");
            }
        }
        assert_eq!(
            bounded_submission_text(
                "prefix-long-secret",
                4096,
                &["prefix", "prefix-long-secret"]
            ),
            "XXXX"
        );
        assert_eq!(
            bounded_submission_text("secret-boundary", 3, &["secret-boundary"]),
            "XXX"
        );
    }

    #[tokio::test]
    async fn multipart_submission_masks_echoed_credentials_from_its_own_target() {
        let _guard = crate::utils::SanitizeTestGuard::new();
        let server = MockServer::start().await;
        let secret = "target-alpha-\"secret\\value";
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(
                ResponseTemplate::new(409)
                    .insert_header("x-request-id", "echoed-target-alpha-operator")
                    .set_body_json(json!({
                        "error": {"Message": format!("Echoed {secret}")},
                        (secret): "unlabelled key echo",
                    })),
            )
            .mount(&server)
            .await;
        let tmp = tempfile::tempdir().unwrap();
        let package = tmp.path().join("fw.fwpkg");
        tokio::fs::write(&package, b"firmware").await.unwrap();
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        access.user = "target-alpha-operator".to_string();
        access.password = secret.to_string();
        NvUtils::set_sanitize_config(Some(HashMap::from([(
            "password".to_string(),
            "target-beta-secret".to_string(),
        )])));
        let output = Mutex::new(json!({"Error": [], "Error Code": 0, "Output": []}));
        let (success, response) = access
            .multipart_file_upload_with_options(
                "/upload",
                package.to_str().unwrap(),
                None,
                30,
                None,
                None,
                None,
                Some(&output),
                false,
                MultipartUploadOptions {
                    bail_on_failure: false,
                },
            )
            .await;
        assert!(!success);
        assert!(!response.to_string().contains("target-alpha"));
        let output = output.into_inner().unwrap();
        let diagnostic = &output["Output"][0]["SubmissionDiagnostic"];
        assert_eq!(diagnostic["category"], "http_rejection");
        assert_eq!(diagnostic["headers"]["x-request-id"], "echoed-XXXX");
        let body = diagnostic["body"].as_str().unwrap();
        assert!(body.contains("Echoed XXXX"), "{body}");
        assert!(!output.to_string().contains("target-alpha"));
    }

    #[test]
    fn submission_text_is_redacted_before_unicode_safe_truncation() {
        let _guard = crate::utils::SanitizeTestGuard::new();
        let value = format!("password={} end", "secret".repeat(1000));
        let bounded = bounded_submission_text(&value, 64, &[]);
        assert!(!bounded.contains("secret"));
        assert!(bounded.chars().count() <= 64);
        assert_eq!(
            bounded_submission_text(&"é".repeat(5000), 4096, &[])
                .chars()
                .count(),
            4096
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
        assert!(output.contains("json_decode_failure"));
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
    async fn nvswitch_inventory_includes_nvos_system_image() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "BMC": {"actual-firmware": "88.0002.1979"}
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/system/image"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "current": "1",
                "next": "partition2",
                "partition1": {"build-id": "nvos-25.02.4440"},
                "partition2": {"build-id": "nvos-25.02.5555"}
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
        assert_eq!(inventory["NVOS"]["Id"], "NVOS");
        assert_eq!(inventory["NVOS"]["Version"], "nvos-25.02.4440");
        assert_eq!(inventory["NVOS"]["NextBootVersion"], "nvos-25.02.5555");
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
    async fn redfish_inventory_uses_complete_oem_embedded_inventory_with_missing_versions() {
        let server = MockServer::start().await;
        let uri = "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_NVSwitch_0";
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{"@odata.id": uri}],
                "Oem": {
                    "Ami": {
                        "FirmwareInventory": [{
                            "DataSourceUri": uri,
                            "Name": "HGX_FW_NVSwitch_0"
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
                "Id": "HGX_FW_NVSwitch_0",
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
        assert_eq!(inventory[uri]["Name"], "HGX_FW_NVSwitch_0");
        assert_eq!(inventory[uri]["Version"], "");
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
    async fn redfish_inventory_adds_undiscoverable_liteon_powerunits_for_pf_1114_1rd() {
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
                "Version": "1.0.0"
            })))
            .expect(1)
            .mount(&server)
            .await;
        // FirmwareInventory does not link to PowerUnits; probe the fixed URI.
        let powerunit_uri =
            "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1";
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
                "Model": "SP-2183-1RD",
                "Version": "0x6 / 0x6"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-vr-liteon");
        access.servertype = "powershelf".to_string();
        access.model = "PF-1114-1RD".to_string();
        let (status, err_code, inventory) = access
            .get_firmware_inventory(TraceFlags::default(), None, None)
            .await;

        assert!(status);
        assert_eq!(err_code, 0);
        assert_eq!(inventory[uri]["Version"], "1.0.0");
        assert_eq!(inventory[powerunit_uri]["Version"], "0x6 / 0x6");
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
        let _guard = crate::utils::SanitizeTestGuard::new();
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
        let _guard = crate::utils::SanitizeTestGuard::new();
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

    /// Verifies the intentional security guard in `is_reachable_redfish_async`:
    /// when HTTPS fails and the access uses a session token, the HTTP fallback
    /// must NOT be attempted so the token is never sent over plaintext.
    ///
    /// If the guard were removed, the HTTP fallback would also fail (port 1 is
    /// unreachable), but it would return "Failed to connect to the system"
    /// instead of the session-token-specific "Connection Error:" message --
    /// so the assertion pins the exact early-return branch.
    #[tokio::test]
    async fn session_token_skips_http_fallback_on_https_failure() {
        // Port 1 is refused immediately by the OS -- no timeout wait.
        let mut access = BmcAccess::mock_with_base_url("https://127.0.0.1:1", "127.0.0.1");
        access.user = BMC_SESSION_TOKEN_USER.to_owned();
        access.password = "test-token".to_owned();

        let (ok, message) = access.is_reachable(TraceFlags::default()).await;

        assert!(!ok);
        assert!(
            message.contains("Connection Error:") && !message.contains("TLS certificate"),
            "session token guard should fire with connection-error message before HTTP fallback; got: {message:?}"
        );
    }

    /// Session token must appear on every Redfish GET in the firmware-inventory
    /// chain, including the collection fetch and each per-member fetch.  If any
    /// request is sent with Basic auth instead of `X-Auth-Token`, the
    /// header-matcher mocks below will not match and wiremock returns 404,
    /// causing the inventory retrieval to fail and the assertions to trip.
    #[tokio::test]
    async fn session_token_propagates_through_get_firmware_inventory() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .and(header("X-Auth-Token", "inv-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0"))
            .and(header("X-Auth-Token", "inv-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "FW_BMC_0",
                "Name": "BMC Firmware",
                "Version": "2.0.0"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock");
        access.user = BMC_SESSION_TOKEN_USER.to_owned();
        access.password = "inv-token".to_owned();

        let (ok, err_code, inventory) = access
            .get_firmware_inventory(TraceFlags::default(), None, None)
            .await;

        assert!(
            ok,
            "inventory retrieval should succeed when session token is forwarded"
        );
        assert_eq!(err_code, 0);
        assert!(
            inventory.contains_key("/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0"),
            "inventory map should contain the mocked component key"
        );
    }

    #[tokio::test]
    async fn observe_nvue_get_classifies_every_answered_read() {
        let server = MockServer::start().await;
        let cases = [
            (
                "/nvue_v1/readable",
                200,
                r#"{"state":"action_success"}"#,
                ObservedRead::Readable(json!({"state": "action_success"})),
            ),
            (
                "/nvue_v1/answered-json",
                503,
                r#"{"status":"error"}"#,
                ObservedRead::Answered(json!({"status": "error"})),
            ),
            (
                "/nvue_v1/unparsed-success",
                200,
                "not json at all",
                ObservedRead::answered_unparsed(),
            ),
            (
                "/nvue_v1/unparsed-error",
                503,
                "<html>503</html>",
                ObservedRead::answered_unparsed(),
            ),
        ];

        for (case_path, status, body, _) in &cases {
            Mock::given(method("GET"))
                .and(path(*case_path))
                .respond_with(ResponseTemplate::new(*status).set_body_string(*body))
                .expect(1)
                .mount(&server)
                .await;
        }

        let access = BmcAccess::mock_with_base_url_and_type(
            server.uri(),
            "mock-switch",
            AccessType::NVSwitch,
        );

        for (case_path, status, _, expected) in &cases {
            let observed = access.observe_nvue_get(case_path, 5).await;

            assert_eq!(
                &observed, expected,
                "unexpected outcome for HTTP {status} at {case_path}"
            );
        }
    }

    #[tokio::test]
    async fn observe_nvue_get_reports_a_missing_response_with_its_cause() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/no-answer"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url_and_type(
            server.uri(),
            "mock-switch",
            AccessType::NVSwitch,
        );

        match access.observe_nvue_get("/nvue_v1/no-answer", 1).await {
            ObservedRead::NoResponse(cause) => assert!(
                !cause.is_empty(),
                "a missing response must carry its transport cause"
            ),
            other => panic!("a read that times out must not look answered: {other:?}"),
        }
    }
}
