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
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nvue_client::cluster::{ClusterAppStartRequest, ClusterAppStopRequest, app_endpoint};
use nvue_client::revision::{
    REVISION_ENDPOINT, RevisionCreate, RevisionIdResponse, RevisionResponse, RevisionUpdate,
    applied_revision_endpoint, revision_endpoint,
};
use nvue_client::system::{SYSTEM_ENDPOINT, UserPasswordPatch, user_endpoint_for_revision};
use nvue_client::{
    Client as NvueClient, ClientConfig as NvueConnectConfig, ClientCredentials as NvueCredentials,
    ClientEndpoint as NvueEndpoint, DEFAULT_TIMEOUT as NVUE_DEFAULT_TIMEOUT,
    SharedClient as SharedNvueClient,
};
use regex::Regex;
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

use crate::domain::node::*;
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::transport::http_client::HttpClient;
use crate::transport::ssh_client::{SftpUploadOptions, SshClient, SshEndpoint};
use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::insert_json_field;

#[cfg(test)]
type SshExecForTest = dyn Fn(&str) -> Result<String> + Send + Sync;

// ── Configuration constants ─────────────────────────────────────────

pub mod config {
    pub const MIN_FIRMWARE_FILE_SIZE: u64 = 1024;
    pub const SFTP_BUFFER_SIZE: usize = crate::transport::ssh::SFTP_UPLOAD_BUFFER_SIZE_BYTES;
    pub const DEFAULT_JOB_TIMEOUT_SECONDS: u64 = 1800;
    pub const DEFAULT_POLL_INTERVAL_SECONDS: u64 = 5;
    pub const DEFAULT_NVUE_PORT: u16 = 443;
    pub const DEFAULT_VERIFY_SSL: bool = false;
    pub const MAX_RETRY_ATTEMPTS: u32 = 3;
    pub const RETRY_WAIT_SECONDS: u64 = 30;
    pub const REVISION_POLL_INTERVAL_SECONDS: u64 = 1;
    pub const REVISION_APPLY_TIMEOUT_SECONDS: u64 = 60;
    pub const GNMI_CONFIG_WAIT_SECONDS: u64 = 15;
    pub const GNMI_RETRY_DELAY_1_SECONDS: u64 = 20;
    pub const GNMI_RETRY_DELAY_2_SECONDS: u64 = 30;
    pub const GRPC_PORT_NMX_CONTROLLER: u16 = 9370;
    pub const GRPC_PORT_NMX_TELEMETRY: u16 = 9352;
    /// NVOS pauses gRPC while cluster manager actions run; wait before the next one.
    pub const CLUSTER_MANAGER_ACTION_SETTLE_SECONDS: u64 = 5;
    pub const VALID_COMPONENTS: &[&str] = &["bmc", "fpga", "erot", "cpld", "bios", "transceiver"];
}

// ── Switch System Constants ──────────────────────────────────────────

pub const NVOS_PARTITION_1_ID: &str = "partition1";
pub const NVOS_PARTITION_2_ID: &str = "partition2";

// ── Validation helpers ──────────────────────────────────────────────

pub(crate) fn shell_quote(s: &str) -> String {
    let mut quoted = String::with_capacity(s.len() + 2);
    quoted.push('\'');
    for c in s.chars() {
        if c == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(c);
        }
    }
    quoted.push('\'');
    quoted
}

pub(crate) fn is_valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn is_valid_system_username(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric())
}

fn redact_password_update_error(
    message: &str,
    new_password: &str,
    encoded_password: &str,
) -> String {
    // NVUE policy errors can echo the rejected password value. Scrub both the
    // caller's plaintext and the base64 wire value before reporting upstream.
    let mut redacted = message.to_owned();

    for secret in [new_password, encoded_password] {
        if !secret.is_empty() {
            redacted = redacted.replace(secret, "XXXX");
        }
    }

    nvfwupd::utils::Util::redact_secret_fields(&redacted)
}

fn redact_password_update_rms_error(
    err: RmsError,
    new_password: &str,
    encoded_password: &str,
) -> RmsError {
    RmsError::new(
        err.code,
        redact_password_update_error(&err.message, new_password, encoded_password),
    )
}

fn is_valid_firmware_filename(name: &str) -> bool {
    if name.is_empty() || name.starts_with('.') {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

fn is_valid_component(component: &str) -> bool {
    let lower = component.to_ascii_lowercase();
    config::VALID_COMPONENTS.iter().any(|&v| lower == v)
}

fn is_valid_firmware_inventory_component(component: &str) -> bool {
    if is_valid_component(component) {
        return true;
    }
    let lower = component.to_ascii_lowercase();
    if lower.len() <= 4 || !lower.starts_with("cpld") {
        return false;
    }
    lower
        .strip_prefix("cpld")
        .is_some_and(|suffix| suffix.chars().all(|c| c.is_ascii_digit()))
}

/// NVUE exposes CPLD version inventory via subcomponents like CPLD1,
/// while uploaded files remain under the aggregate CPLD namespace.
fn firmware_inventory_endpoint_component(component: &str, list_files: bool) -> String {
    let lower = component.to_ascii_lowercase();
    if !list_files && lower == "cpld" {
        return "CPLD1".to_owned();
    }
    component.to_owned()
}

// ── Firmware classification ─────────────────────────────────────────

#[cfg(test)]
fn classify_firmware_type_switch(name: &str) -> FirmwareType {
    let lower = name.to_ascii_lowercase();
    if lower.contains("bmc") {
        FirmwareType::BMC
    } else if lower.contains("bios") {
        FirmwareType::BIOS
    } else if lower.contains("fpga") {
        FirmwareType::FPGA
    } else if lower.contains("cpld") {
        FirmwareType::CPLD
    } else {
        FirmwareType::Unknown
    }
}

#[cfg(test)]
#[allow(dead_code)]
struct FwpkgCpldSubcomponent {
    cpld_name: String,
    component_id: String,
    revision: String,
    parent_device_name: String,
    component_index: usize,
}

#[cfg(test)]
fn map_package_device_name(component: &str) -> Option<&'static str> {
    match component.to_ascii_lowercase().as_str() {
        "bmc" => Some("BMC"),
        "fpga" => Some("SMR"),
        "erot" => Some("EROT"),
        "bios" => Some("SBIOS"),
        "transceiver" => Some("TRANSCEIVER"),
        _ => None,
    }
}

#[cfg(test)]
fn extract_switch_version_string(firmware: &Value) -> String {
    for field in &["actual-firmware", "version", "Version", "sys_version"] {
        if let Some(v) = firmware.get(*field).and_then(Value::as_str) {
            return v.to_owned();
        }
    }
    "unknown".to_owned()
}

#[cfg(test)]
fn build_cpld_package_version(sub: &FwpkgCpldSubcomponent) -> String {
    format!("CPLD{}_{}", sub.component_id, sub.revision)
}

#[cfg(test)]
fn strip_leading_zeros(value: &str) -> String {
    match value.find(|c: char| c != '0') {
        Some(pos) => value[pos..].to_owned(),
        None => "0".to_owned(),
    }
}

#[cfg(test)]
fn normalize_cpld_version_string(version: &str) -> String {
    if version.is_empty() {
        return String::new();
    }

    let upper = version.to_ascii_uppercase();
    let mut collapsed = String::with_capacity(upper.len());
    let mut prev_separator = false;
    for c in upper.chars() {
        if c.is_ascii_alphanumeric() {
            collapsed.push(c);
            prev_separator = false;
        } else if !prev_separator {
            collapsed.push('_');
            prev_separator = true;
        }
    }

    let re = Regex::new(r"(?:^|_)(?:CPLD_*)?([0-9]+)_*REV_?([0-9]+)(?:_|$)").expect("valid regex");
    if let Some(caps) = re.captures(&collapsed) {
        format!(
            "CPLD{}_REV{}",
            strip_leading_zeros(&caps[1]),
            strip_leading_zeros(&caps[2])
        )
    } else {
        collapsed
    }
}

// ── Port connectivity / reboot hints / job ID extraction ────────────

async fn test_port_connectivity(
    hostname: &str,
    target_port: u16,
    timeout: Duration,
) -> Result<bool> {
    let probe = HttpClient::new(hostname, target_port, "", "", false, false)?;
    match probe.get("/", timeout).await {
        Ok(_) => Ok(true),
        Err(e) if e.code == ErrorCode::ConnectionRefused || e.code == ErrorCode::Timeout => {
            Ok(false)
        }
        // For gRPC ports, a non-HTTP response still means the port is
        // reachable — only ConnectionRefused/Timeout mean truly unreachable.
        Err(_) => Ok(true),
    }
}

fn contains_reboot_hint(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("power cycle") || lower.contains("reboot") || lower.contains("system is offline")
}

/// True when an NVUE action message indicates a no-op operation, like an uninstall action that has no effect.
/// These should be treated as success and not errors.
fn indicates_noop(message: &str) -> bool {
    message.contains("Nothing to uninstall")
}

// ── System image state & poll heuristics ──────────────────────────────

/// Normalized NVUE system image state.
///
/// NVUE shapes the `/nvue_v1/system/image` response slightly differently across
/// switch software versions. `normalize_system_image_state` reduces that to a
/// stable representation that callers (install-then-wait flow) can compare
/// directly against a target build-id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SwitchSystemImageState {
    pub current: String,
    pub next: String,
    pub current_partition: String,
    pub next_partition: String,
    pub current_build_id: String,
    pub next_build_id: String,
    pub partition1_build_id: String,
    pub partition2_build_id: String,
}

// Parse a "build-id" style value - may be a plain string or an object with a
// string `build-id` field. Returns the empty string when neither shape matches.
fn extract_system_image_build_id(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.to_owned();
    }
    if let Some(obj) = value.as_object() {
        if let Some(s) = obj.get("build-id").and_then(|v| v.as_str()) {
            return s.to_owned();
        }
    }
    String::new()
}

fn normalize_system_image_partition_id(value: &str) -> String {
    match value {
        "1" | NVOS_PARTITION_1_ID => NVOS_PARTITION_1_ID.to_owned(),
        "2" | NVOS_PARTITION_2_ID => NVOS_PARTITION_2_ID.to_owned(),
        _ => String::new(),
    }
}

fn find_system_image_partition_id(state: &SwitchSystemImageState, build_id: &str) -> String {
    if build_id.is_empty() {
        return String::new();
    }
    if !state.partition1_build_id.is_empty() && state.partition1_build_id == build_id {
        return NVOS_PARTITION_1_ID.to_owned();
    }
    if !state.partition2_build_id.is_empty() && state.partition2_build_id == build_id {
        return NVOS_PARTITION_2_ID.to_owned();
    }
    String::new()
}

fn resolve_system_image_build_id(
    state: &SwitchSystemImageState,
    value: &str,
    partition_id: &str,
) -> String {
    if partition_id == NVOS_PARTITION_1_ID || value == NVOS_PARTITION_1_ID {
        return state.partition1_build_id.clone();
    }
    if partition_id == NVOS_PARTITION_2_ID || value == NVOS_PARTITION_2_ID {
        return state.partition2_build_id.clone();
    }
    value.to_owned()
}

fn switch_redfish_reset_type(op: PowerOp) -> Result<&'static str> {
    match op {
        PowerOp::On => Ok("On"),
        PowerOp::Off => Ok("GracefulShutdown"),
        PowerOp::ForceOn => Ok("ForceOn"),
        PowerOp::ForceOff => Ok("ForceOff"),
        PowerOp::PowerCycle => Ok("PowerCycle"),
        PowerOp::GracefulShutdown => Ok("GracefulShutdown"),
        PowerOp::GracefulRestart => Ok("GracefulRestart"),
        PowerOp::ForceRestart => Ok("ForceRestart"),
        PowerOp::Nmi => Err(RmsError::invalid_argument(
            "switch nodes do not support NMI reset",
        )),
    }
}

fn parse_switch_redfish_power_state(response: &Value) -> PowerState {
    match response
        .get("PowerState")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
    {
        "On" => PowerState::On,
        "Off" => PowerState::Off,
        _ => PowerState::Unknown,
    }
}

pub fn normalize_system_image_state(raw: &Value) -> SwitchSystemImageState {
    let mut state = SwitchSystemImageState::default();
    if let Some(p1) = raw.get(NVOS_PARTITION_1_ID) {
        state.partition1_build_id = extract_system_image_build_id(p1);
    }
    if let Some(p2) = raw.get(NVOS_PARTITION_2_ID) {
        state.partition2_build_id = extract_system_image_build_id(p2);
    }
    state.current = raw
        .get("current")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    state.next = raw
        .get("next")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    state.current_partition = normalize_system_image_partition_id(&state.current);
    state.next_partition = normalize_system_image_partition_id(&state.next);
    state.current_build_id =
        resolve_system_image_build_id(&state, &state.current, &state.current_partition);
    state.next_build_id = resolve_system_image_build_id(&state, &state.next, &state.next_partition);
    if state.current_partition.is_empty() {
        state.current_partition = find_system_image_partition_id(&state, &state.current_build_id);
    }
    if state.next_partition.is_empty() {
        state.next_partition = find_system_image_partition_id(&state, &state.next_build_id);
    }
    state
}

/// Walk an NVUE system-image listing and return true if `image_filename`
/// appears anywhere in the structure (as an array entry, an object key,
/// or a nested value).
pub fn system_image_listing_contains(value: &Value, image_filename: &str) -> bool {
    match value {
        Value::String(s) => s == image_filename,
        Value::Array(arr) => arr
            .iter()
            .any(|v| system_image_listing_contains(v, image_filename)),
        Value::Object(obj) => {
            if obj.contains_key(image_filename) {
                return true;
            }
            obj.iter().any(|(k, v)| {
                k == image_filename || system_image_listing_contains(v, image_filename)
            })
        }
        _ => false,
    }
}

/// Infer the NVOS build-id (`nvos-X.Y.Z`) from a system-image filename.
/// The filename stem must contain exactly one `X.Y.Z` pattern.
pub fn infer_target_build_id(image_filename: &str) -> Result<String> {
    let stem = Path::new(image_filename)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    // Numeric semver triple. Bounded by word boundaries so "1.2.3.4" is
    // still matched as "1.2.3" (first match wins when unique).
    let re = Regex::new(r"(\d+\.\d+\.\d+)").expect("regex compiles");

    let mut version: Option<String> = None;
    for caps in re.captures_iter(stem) {
        let candidate = caps
            .get(1)
            .map(|m| m.as_str().to_owned())
            .unwrap_or_default();
        match &version {
            None => version = Some(candidate),
            Some(existing) if existing != &candidate => {
                return Err(RmsError::invalid_argument(format!(
                    "image filename '{image_filename}' contains multiple version patterns, cannot infer target build-id"
                )));
            }
            _ => {}
        }
    }

    match version {
        Some(v) => Ok(format!("nvos-{v}")),
        None => Err(RmsError::invalid_argument(format!(
            "unable to infer NVOS build-id from image filename '{image_filename}'"
        ))),
    }
}

/// True when an HTTP call failed with 401 Unauthorized.
pub fn is_http_unauthorized_error(err: &RmsError) -> bool {
    err.code == ErrorCode::Unauthenticated
}

/// Transport-level errors that are worth retrying during the long-running
/// system-image install + steady-state phases (the switch is rebooting).
pub fn is_retryable_system_image_error(err: &RmsError) -> bool {
    matches!(
        err.code,
        ErrorCode::ConnectionRefused
            | ErrorCode::Timeout
            | ErrorCode::Unavailable
            | ErrorCode::DnsResolutionFailed
    )
}

pub fn is_retryable_steady_state_error(err: &RmsError) -> bool {
    is_retryable_system_image_error(err)
        || err
            .message
            .contains("HTTP GET /nvue_v1/system/image returned ")
}

pub fn is_retryable_install_poll_error(err: &RmsError) -> bool {
    is_retryable_system_image_error(err) || err.message.contains("HTTP GET /nvue_v1/action/")
}

fn contains_case_insensitive(text: &str, needle: &str) -> bool {
    text.to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

pub fn is_uninstall_poll_success(status: &FirmwareTaskStatus) -> bool {
    status.state == "action_success"
}

fn is_install_progress_text(text: &str) -> bool {
    text.to_ascii_lowercase().contains("installing image")
}

fn is_install_reboot_transition_text(text: &str) -> bool {
    contains_case_insensitive(text, "disconnecting from nvos")
        || contains_case_insensitive(text, "offline during reboot")
        || contains_case_insensitive(text, "system is offline")
}

/// An `action_success` state, or the `action_error + percent==100` handoff
/// that NVUE sometimes reports in place of action_success for reboot-driven
/// installs.
pub fn is_install_poll_success(status: &FirmwareTaskStatus) -> bool {
    status.state == "action_success" || (status.state == "action_error" && status.percent == 100)
}

pub fn is_install_progress_state(status: &FirmwareTaskStatus) -> bool {
    is_install_progress_text(&status.status) || is_install_progress_text(&status.message)
}

pub fn is_install_reboot_transition_state(status: &FirmwareTaskStatus) -> bool {
    is_install_reboot_transition_text(&status.status)
        || is_install_reboot_transition_text(&status.message)
}

/// `action_error` with percent==100 and "Installing image" context is NVUE's
/// way of handing off from the install phase to the reboot phase. Once we see
/// this, further polling of the install job is pointless - move to the
/// steady-state wait.
pub fn is_install_handoff_to_reboot(status: &FirmwareTaskStatus) -> bool {
    status.state == "action_error"
        && status.completed
        && status.percent == 100
        && is_install_progress_state(status)
}

pub fn describe_install_poll(status: &FirmwareTaskStatus) -> String {
    let mut out = format!("state={}", status.state);
    if !status.status.is_empty() {
        out.push_str(&format!(", status={}", status.status));
    }
    if !status.message.is_empty() {
        out.push_str(&format!(", message={}", status.message));
    }
    out.push_str(&format!(", completed={}", status.completed));
    out.push_str(&format!(", percent={}", status.percent));
    out
}

// Extract a human-readable error from an NVUE action `issue` array.
// Each issue entry may be a string, an object with `message`, or an object
// with nested `data.msg`. Multiple entries are joined with "; ". Returns an
// empty string if no usable message is found.
fn extract_action_issue_message(response: &Value) -> String {
    let Some(issues) = response.get("issue").and_then(|v| v.as_array()) else {
        return String::new();
    };

    let mut parts: Vec<String> = Vec::with_capacity(issues.len());
    for issue in issues {
        let part = if let Some(s) = issue.as_str() {
            s.to_owned()
        } else if issue.is_object() {
            if let Some(msg) = issue.get("message").and_then(|v| v.as_str()) {
                msg.to_owned()
            } else if let Some(msg) = issue
                .get("data")
                .and_then(|d| d.get("msg"))
                .and_then(|v| v.as_str())
            {
                msg.to_owned()
            } else {
                continue;
            }
        } else {
            continue;
        };

        if !part.is_empty() {
            parts.push(part);
        }
    }
    parts.join("; ")
}

fn extract_job_id(response: &Value) -> Result<String> {
    nvue_client::action::extract_action_id(response).map_err(Into::into)
}

fn build_nvue_connect_config(
    host_endpoint: &EndpointConfig,
    username: &str,
    password: &str,
) -> Result<NvueConnectConfig> {
    Ok(NvueConnectConfig {
        endpoint: NvueEndpoint::new(
            &host_endpoint.endpoint.ip_address,
            host_endpoint.endpoint.port,
            host_endpoint.endpoint.host_name.as_deref(),
        )?,
        credentials: NvueCredentials::new(username, password),
        dangerously_accept_invalid_certs: host_endpoint.dangerously_accept_invalid_certs,
    })
}

fn build_nvue_api(config: NvueConnectConfig) -> Result<SharedNvueClient> {
    NvueClient::new(config).map_err(Into::into)
}

fn revision_apply_poll_outcome(
    response: &RevisionResponse,
    revision_id: &str,
) -> Result<RevisionApplyPollOutcome> {
    let state = response.state(revision_id).unwrap_or_default().to_owned();

    if response.is_applied(revision_id) {
        return Ok(RevisionApplyPollOutcome::Applied);
    }

    if response.has_no_config_diff(revision_id) {
        return Ok(RevisionApplyPollOutcome::NoConfigDiff);
    }

    if response.is_failed(revision_id) {
        let issue = response.issue_summary(revision_id);

        let message = if issue.is_empty() {
            format!("revision {revision_id} apply failed (state={state})")
        } else {
            format!("revision {revision_id} apply failed (state={state}): {issue}")
        };

        return Err(RmsError::failed_precondition(message));
    }

    Ok(RevisionApplyPollOutcome::Pending { state })
}

fn revision_apply_timeout(
    revision_id: &str,
    last_observed_state: Option<String>,
    last_retryable_error: Option<RmsError>,
) -> RmsError {
    let mut message = format!(
        "timed out waiting for revision {revision_id} to reach applied state or report no config diff"
    );

    if let Some(state) = last_observed_state {
        message.push_str("; last observed ");
        message.push_str(&state);
    }

    if let Some(error) = last_retryable_error {
        message.push_str("; last retryable error: ");
        message.push_str(&error.message);
    }

    RmsError::timeout(message)
}

fn revision_pending_state(state_prefix: Option<&str>, state: &str) -> Option<String> {
    if state.is_empty() {
        return None;
    }

    match state_prefix {
        Some(prefix) => Some(format!("{prefix} state={state}")),
        None => Some(format!("state={state}")),
    }
}

fn revision_poll_decision(
    response: &RevisionResponse,
    revision_id: &str,
    pending_state_prefix: Option<&str>,
) -> Result<RevisionPollDecision> {
    match revision_apply_poll_outcome(response, revision_id)? {
        RevisionApplyPollOutcome::Applied => Ok(RevisionPollDecision::Complete {
            no_config_diff: false,
        }),
        RevisionApplyPollOutcome::NoConfigDiff => Ok(RevisionPollDecision::Complete {
            no_config_diff: true,
        }),
        RevisionApplyPollOutcome::Pending { state } => Ok(RevisionPollDecision::Pending {
            observed_state: revision_pending_state(pending_state_prefix, &state),
        }),
    }
}

fn is_candidate_revision_poll_error(err: &RmsError) -> bool {
    // Candidate credentials may return 401 while NVUE is still applying an
    // active-user password change. Keep that retryable only for candidate
    // polling; current-credential polling treats 401 as an auth failure.
    is_http_unauthorized_error(err) || is_retryable_system_image_error(err)
}

fn rms_http_status_code(status: u16) -> ErrorCode {
    match status {
        401 => ErrorCode::Unauthenticated,
        404 => ErrorCode::NotFound,
        408 => ErrorCode::Timeout,
        409 => ErrorCode::AlreadyExists,
        503 => ErrorCode::Unavailable,
        _ => ErrorCode::Internal,
    }
}

enum RevisionApplyPollOutcome {
    Applied,
    NoConfigDiff,
    Pending { state: String },
}

enum RevisionPollDecision {
    Complete { no_config_diff: bool },
    Pending { observed_state: Option<String> },
}

enum CandidateRevisionDiagnostic {
    Continue {
        observed_state: Option<String>,
        retryable_error: Option<RmsError>,
    },
}

const NVUE_STATE_ENABLED: &str = "enabled";
const NVUE_NMXC_CONN_UP: &str = "up";
const NVUE_APP_STATUS_OK: &str = "ok";
const NMX_CONTROL_PLANE_READY_STATES: &[&str] = &[
    "CONTROL_PLANE_STATE_CONFIGURED",
    "CONTROL_PLANE_STATE_UNCONFIGURED",
];

const NMX_CONTROL_PLANE_STOPPED_STATES: &[&str] = &["STOPPED", "CONTROL_PLANE_STATE_STOPPED"];

// Keep these predicates narrow until the NVUE response shapes move into
// nvue_client. They model only the fields needed before manager actions.
fn nvue_state_field(value: Option<&Value>) -> Option<&str> {
    match value {
        Some(Value::String(state)) => Some(state),
        Some(Value::Object(fields)) => fields.get("state").and_then(Value::as_str),
        _ => None,
    }
}

fn cluster_ready_for_app_manager_action(cluster: &Value) -> bool {
    let state = cluster.get("state").and_then(Value::as_str);
    let nmxc_conn = nvue_state_field(cluster.get("nmxc-conn"));

    state == Some(NVUE_STATE_ENABLED) && nmxc_conn == Some(NVUE_NMXC_CONN_UP)
}

fn cluster_enabled_for_app_manager_action(cluster: &Value) -> bool {
    cluster.get("state").and_then(Value::as_str) == Some(NVUE_STATE_ENABLED)
}

fn app_manager_state(app_status: &Value) -> Option<&str> {
    app_status
        .get("manager")
        .and_then(|manager| manager.get("state"))
        .and_then(Value::as_str)
}

fn app_control_plane_state(app_status: &Value) -> Option<&str> {
    ["additional-info", "addition-info"]
        .iter()
        .find_map(|field| app_status.get(*field).and_then(Value::as_str))
}

fn app_ready_for_manager_action(app_status: &Value) -> bool {
    let status = app_status.get("status").and_then(Value::as_str);
    let control_plane_state = app_control_plane_state(app_status);

    status == Some(NVUE_APP_STATUS_OK)
        || control_plane_state.is_some_and(|state| NMX_CONTROL_PLANE_READY_STATES.contains(&state))
}

fn app_waiting_for_manager_action(app_status: &Value) -> bool {
    !app_ready_for_manager_action(app_status)
        && (app_status.get("status").and_then(Value::as_str).is_some()
            || app_control_plane_state(app_status).is_some())
}

fn app_stopped_for_manager_action(app_status: &Value) -> bool {
    app_status
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("stopped"))
        || app_control_plane_state(app_status)
            .is_some_and(|state| NMX_CONTROL_PLANE_STOPPED_STATES.contains(&state))
}

fn app_manager_reached_state(app_status: &Value, desired: &str) -> bool {
    app_manager_state(app_status).is_some_and(|state| {
        state == desired || (desired == NVUE_STATE_ENABLED && matches!(state, "start" | "active"))
    })
}

#[cfg(test)]
const RETRY_WAIT_DELAY: Duration = Duration::ZERO;
#[cfg(not(test))]
const RETRY_WAIT_DELAY: Duration = Duration::from_secs(config::RETRY_WAIT_SECONDS);

pub fn grpc_port_for_app(app_name: &str) -> u16 {
    match app_name {
        "nmx-telemetry" => config::GRPC_PORT_NMX_TELEMETRY,
        "nmx-controller" => config::GRPC_PORT_NMX_CONTROLLER,
        _ => config::GRPC_PORT_NMX_CONTROLLER,
    }
}

// ═══════════════════════════════════════════════════════════════════
//  NVIDIA GB200 switch node (NVUE REST + SSH/SFTP)
// ═══════════════════════════════════════════════════════════════════

/// Concrete node for NVIDIA reference GB200 network switches (NVUE REST + SSH/SFTP).
///
/// Power:     Redfish ComputerSystem.Reset when BMC details are available;
///            legacy PowerCycle fallback via POST /nvue_v1/system
/// Firmware:  SFTP push then NVUE install, poll via /nvue_v1/action/{id}
/// Activate:  power cycle
///
/// Credentials are stored at construction time. One shared NVUE client is
/// kept for REST calls; SshClient instances are created on demand
/// for each SFTP transfer or SSH command.
pub struct SwitchGb200Nvidia {
    id: String,
    rack_id: String,
    node_type: NodeType,
    bmc_endpoint: Option<EndpointConfig>,
    host_endpoint: Option<EndpointConfig>,
    nvue: Option<SharedNvueClient>,
    power_http: Option<Arc<tokio::sync::Mutex<HttpClient>>>,
    #[cfg(test)]
    pub(crate) ssh_exec_for_test: Option<Arc<SshExecForTest>>,
}

impl SwitchGb200Nvidia {
    pub const TYPE: &'static str = "switch_gb200_nvidia";

    /// Node identifier (inherent accessor — use this instead of the private `id` field).
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn rack_id(&self) -> &str {
        &self.rack_id
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        rack_id: String,
        host: String,
        port: u16,
        username: &str,
        password: &str,
        mac_address: String,
        host_mac_address: String,
        host_ip_address: String,
        host_name: String,
        dangerously_accept_invalid_certs: bool,
    ) -> Result<Self> {
        if host_ip_address.is_empty() {
            return Err(RmsError::invalid_argument(
                "host_ip_address is required for switch nodes",
            ));
        }

        let host_name = if host_name.is_empty() {
            None
        } else {
            Some(host_name)
        };
        let host_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: host_ip_address,
                mac_address: host_mac_address,
                port,
                host_name,
            },
            Some(EndpointCredentials::new(username, password)),
            dangerously_accept_invalid_certs,
        );
        let nvue_config = build_nvue_connect_config(&host_endpoint, username, password)?;
        let nvue = build_nvue_api(nvue_config)?;

        Ok(Self {
            id,
            rack_id,
            node_type: NodeType::SwitchGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::new(Endpoint {
                ip_address: host,
                mac_address,
                port,
                host_name: None,
            })),
            host_endpoint: Some(host_endpoint),
            nvue: Some(nvue),
            power_http: None,
            #[cfg(test)]
            ssh_exec_for_test: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_redfish_power(
        id: String,
        rack_id: String,
        bmc_host: String,
        bmc_port: u16,
        username: &str,
        password: &str,
        mac_address: String,
        dangerously_accept_invalid_certs: bool,
    ) -> Result<Self> {
        let http = Arc::new(tokio::sync::Mutex::new(HttpClient::new(
            &bmc_host,
            bmc_port,
            username,
            password,
            dangerously_accept_invalid_certs,
            true,
        )?));

        Ok(Self {
            id,
            rack_id,
            node_type: NodeType::SwitchGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: bmc_host,
                    mac_address,
                    port: bmc_port,
                    host_name: None,
                },
                Some(EndpointCredentials::new(username, password)),
                dangerously_accept_invalid_certs,
            )),
            host_endpoint: None,
            nvue: None,
            power_http: Some(http),
            #[cfg(test)]
            ssh_exec_for_test: None,
        })
    }

    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type.kind() != NodeKind::Switch {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        // Host/NVUE traffic must use host_endpoint. BMC-only switches can still
        // be created for BMC Redfish power, but they must not bind the NVUE REST
        // client to the BMC endpoint because that sends host operations to the
        // wrong management plane.
        let nvue = if let Some(host_endpoint) = config.host_endpoint.as_ref() {
            if host_endpoint.endpoint.ip_address.is_empty() {
                return Err(RmsError::invalid_argument(
                    "host_ip_address is required for switch nodes",
                ));
            }

            let http_credentials = host_endpoint.credentials.as_ref();
            let username = http_credentials
                .map(|credentials| credentials.username.as_str())
                .unwrap_or_default();
            let password = http_credentials
                .map(|credentials| credentials.password.expose_secret())
                .unwrap_or_default();
            let nvue_config = build_nvue_connect_config(host_endpoint, username, password)?;
            Some(build_nvue_api(nvue_config)?)
        } else {
            let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
                return Err(RmsError::invalid_argument(
                    "bmc_endpoint is required when host_endpoint is not set for switch nodes",
                ));
            };

            if bmc_endpoint.endpoint.ip_address.is_empty() {
                return Err(RmsError::invalid_argument(
                    "bmc_ip_address is required for BMC-only switch nodes",
                ));
            }

            if bmc_endpoint.endpoint.mac_address.is_empty() {
                return Err(RmsError::invalid_argument(
                    "bmc_mac_address is required for BMC-only switch nodes",
                ));
            }

            None
        };

        // Power traffic is separate from host/NVUE traffic. When valid BMC
        // details and BMC credentials exist, create a Redfish client with the
        // BMC endpoint auth. This prevents host/NVUE credentials from being
        // accidentally reused for BMC power when both endpoints are present.
        let power_http = if let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() {
            let bmc_credentials = bmc_endpoint.credentials.as_ref();
            let bmc_username = bmc_credentials
                .map(|credentials| credentials.username.as_str())
                .unwrap_or_default();
            let bmc_password = bmc_credentials
                .map(|credentials| credentials.password.expose_secret())
                .unwrap_or_default();

            if bmc_endpoint.endpoint.ip_address.is_empty()
                || bmc_endpoint.endpoint.port == 0
                || bmc_username.is_empty()
                || bmc_password.is_empty()
            {
                None
            } else {
                Some(Arc::new(tokio::sync::Mutex::new(HttpClient::new(
                    &bmc_endpoint.endpoint.ip_address,
                    bmc_endpoint.endpoint.port,
                    bmc_username,
                    bmc_password,
                    bmc_endpoint.dangerously_accept_invalid_certs,
                    true,
                )?)))
            }
        } else {
            None
        };

        Ok(Self {
            id: config.id.clone(),
            rack_id: rack_id.to_owned(),
            node_type: config.node_type,
            bmc_endpoint: config.bmc_endpoint.clone(),
            host_endpoint: config.host_endpoint.clone(),
            nvue,
            power_http,
            #[cfg(test)]
            ssh_exec_for_test: None,
        })
    }

    fn nvfwupd_target_config(&self) -> Result<nvfwupd::workflow::TargetConfig> {
        let Some(host_endpoint) = self.host_endpoint.as_ref() else {
            return Err(RmsError::failed_precondition(
                "switch host endpoint is required for NVUE/NVOS operations",
            ));
        };
        let Some(credentials) = host_endpoint.credentials.as_ref() else {
            return Err(RmsError::failed_precondition(
                "switch host credentials are required for NVUE/NVOS operations",
            ));
        };
        Ok(nvfwupd_adapter::to_target_config_with_secret(
            self.node_type,
            &host_endpoint.endpoint.ip_address,
            host_endpoint.endpoint.port,
            &credentials.username,
            &credentials.password,
            !host_endpoint.dangerously_accept_invalid_certs,
        ))
    }

    fn nvfwupd_workflow(&self) -> Result<nvfwupd::workflow_api::WorkflowContext> {
        Ok(nvfwupd::workflow_api::WorkflowContext::with_nvue_client(
            self.nvue_client()?.clone(),
        ))
    }

    fn require_host_endpoint(&self) -> Result<&Endpoint> {
        self.host_endpoint
            .as_ref()
            .map(|endpoint| &endpoint.endpoint)
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "switch host endpoint is required for NVUE/NVOS operations",
                )
            })
    }

    fn require_host_credentials(&self) -> Result<&EndpointCredentials> {
        self.host_endpoint
            .as_ref()
            .and_then(|endpoint| endpoint.credentials.as_ref())
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "switch host credentials are required for NVUE/NVOS operations",
                )
            })
    }

    pub(crate) fn host_credentials(&self) -> Result<(&str, &str)> {
        let credentials = self.require_host_credentials()?;
        Ok((&credentials.username, credentials.password.expose_secret()))
    }

    pub(crate) fn nvue_client(&self) -> Result<&SharedNvueClient> {
        self.nvue.as_ref().ok_or_else(|| {
            RmsError::invalid_argument("host_endpoint is required for NVUE operations")
        })
    }

    pub(crate) fn optional_nvue_client(&self) -> Option<&SharedNvueClient> {
        self.nvue.as_ref()
    }

    pub(crate) fn validate_nvue_client_endpoint(&self, nvue: &SharedNvueClient) -> Result<()> {
        let host_endpoint = self.require_host_endpoint()?;
        let client_endpoint = nvue.endpoint();
        let expected_ip = host_endpoint
            .ip_address
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .map_err(|error| {
                RmsError::invalid_argument(format!(
                    "invalid switch management IP {}: {error}",
                    host_endpoint.ip_address
                ))
            })?;

        let actual_ip = client_endpoint
            .connect_host
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .map_err(|error| {
                RmsError::invalid_argument(format!(
                    "invalid NVUE client management IP {}: {error}",
                    client_endpoint.connect_host
                ))
            })?;

        if expected_ip != actual_ip || host_endpoint.port != client_endpoint.port {
            return Err(RmsError::invalid_argument(format!(
                "NVUE client {}:{} does not match switch management endpoint {}:{}",
                client_endpoint.connect_host,
                client_endpoint.port,
                host_endpoint.ip_address,
                host_endpoint.port
            )));
        }

        Ok(())
    }

    pub(crate) fn with_nvue_client(mut self, nvue: SharedNvueClient) -> Result<Self> {
        self.validate_nvue_client_endpoint(&nvue)?;

        self.nvue = Some(nvue);

        Ok(self)
    }

    #[cfg(test)]
    pub(crate) async fn configure_nvue_client_tls(
        &self,
        tls: Option<nvue_client::ClientTls>,
    ) -> Result<bool> {
        let client = self.nvue_client()?;

        match tls {
            Some(tls) => client
                .prepare_client_tls_if_changed(tls, None)
                .await
                .map(|prepared| prepared.is_some())
                .map_err(Into::into),
            None => client.configure_server_name(None).await.map_err(Into::into),
        }
    }

    async fn nvue_http_get(&self, endpoint: &str, timeout: Duration) -> Result<Value> {
        Ok(self.nvue_client()?.get_json(endpoint, timeout).await?)
    }

    async fn nvue_http_post(
        &self,
        endpoint: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<Value> {
        Ok(self
            .nvue_client()?
            .post_json(endpoint, payload, timeout)
            .await?)
    }

    async fn nvue_http_patch(
        &self,
        endpoint: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<Value> {
        Ok(self
            .nvue_client()?
            .patch_json(endpoint, payload, timeout)
            .await?)
    }

    async fn nvue_http_patch_with_error_body(
        &self,
        endpoint: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<()> {
        let response = self
            .nvue_client()?
            .patch(endpoint, payload, timeout)
            .await?;

        if response.is_success() {
            return Ok(());
        }

        let message = if response.body.trim().is_empty() {
            format!("HTTP PATCH {endpoint} returned {}", response.status)
        } else {
            format!(
                "HTTP PATCH {endpoint} returned {}: {}",
                response.status, response.body
            )
        };

        Err(RmsError::new(
            rms_http_status_code(response.status),
            message,
        ))
    }

    pub(crate) fn ssh_endpoint(&self) -> Result<SshEndpoint> {
        let host_endpoint = self.require_host_endpoint()?;
        let credentials = self.require_host_credentials()?;

        Ok(SshEndpoint::new(
            &host_endpoint.ip_address,
            &credentials.username,
            credentials.password.expose_secret(),
        ))
    }

    // ── Switch-specific firmware operations ──────────────────────────

    pub async fn list_firmware(&self, component: &str, list_files: bool) -> Result<Value> {
        if !is_valid_firmware_inventory_component(component) {
            return Err(RmsError::invalid_argument(format!(
                "invalid firmware component '{component}' \
                 (valid: bmc, fpga, erot, cpld, bios, transceiver, cpld<N>)"
            )));
        }

        let mut endpoint = format!(
            "/nvue_v1/platform/firmware/{}",
            firmware_inventory_endpoint_component(component, list_files)
        );
        if list_files {
            endpoint.push_str("/files");
        }

        self.nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await
    }

    pub async fn push_firmware_file(
        &self,
        local_path: &str,
        component: &str,
        filename: &str,
    ) -> Result<()> {
        if !is_valid_firmware_filename(filename) {
            return Err(RmsError::invalid_argument(
                "firmware filename is invalid \
                 (only alphanumeric, '-', '_', '.' allowed, no hidden files)",
            ));
        }
        if component.is_empty() || !is_valid_component(component) {
            return Err(RmsError::invalid_argument(format!(
                "firmware component '{component}' is invalid \
                 (valid: bmc, fpga, erot, cpld, bios, transceiver)"
            )));
        }

        let lower = component.to_ascii_lowercase();
        let remote_path = format!("/host/fw-images/{lower}/{filename}");
        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        // Skip upload if remote file already matches local size
        if let Ok(remote_size) = ssh.sftp_file_size(&remote_path).await {
            if let Ok(local_meta) = std::fs::metadata(local_path) {
                if local_meta.len() == remote_size {
                    tracing::info!(
                        node_id = %self.id,
                        component = %lower,
                        "firmware file already staged on switch, skipping SFTP upload"
                    );

                    return Ok(());
                }
            }
        }

        tracing::info!(
            node_id = %self.id,
            component = %lower,
            upload_timeout_secs = SshClient::UPLOAD_TIMEOUT.as_secs(),
            buffer_size_bytes = config::SFTP_BUFFER_SIZE,
            "uploading firmware file via SFTP"
        );

        ssh.sftp_upload(local_path, &remote_path, SshClient::UPLOAD_TIMEOUT)
            .await?;

        let verify_cmd = format!("test -f {} && echo ok", shell_quote(&remote_path));
        let verify_out = ssh.exec(&verify_cmd, SshClient::DEFAULT_TIMEOUT).await?;
        if !verify_out.contains("ok") {
            return Err(RmsError::internal(format!(
                "SFTP upload verification failed for {remote_path}"
            )));
        }

        Ok(())
    }

    pub async fn poll_job_blocking(
        &self,
        job_id: &str,
        timeout: Duration,
        interval: Duration,
    ) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            let st = self.poll_nvue_action_task(job_id).await?;

            if st.state == "action_success" {
                return Ok(true);
            }
            if st.state == "action_error" {
                if st.percent == 100 {
                    return Ok(true);
                }
                return Err(RmsError::internal(format!(
                    "job {job_id} error: {}",
                    st.message
                )));
            }
            if st.state == "action_failed" {
                return Err(RmsError::internal(format!(
                    "job {job_id} failed: {}",
                    st.message
                )));
            }
            tokio::time::sleep(interval).await;
        }

        Err(RmsError::timeout(format!(
            "job {job_id} timed out after {}s",
            timeout.as_secs()
        )))
    }

    // ── System image operations ─────────────────────────────────────

    pub async fn fetch_system_image(&self, remote_url: &str) -> Result<String> {
        let payload = serde_json::json!({
            "@fetch": {
                "state": "start",
                "parameters": {"remote-url": remote_url}
            }
        });
        let resp = self
            .nvue_http_post(
                "/nvue_v1/system/image",
                &payload,
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;
        extract_job_id(&resp)
    }

    pub async fn install_system_image(&self, image_filename: &str) -> Result<String> {
        if !is_valid_firmware_filename(image_filename) {
            return Err(RmsError::invalid_argument(
                "invalid image filename \
                 (only alphanumeric, '-', '_', '.' allowed, no hidden files)",
            ));
        }

        let payload = serde_json::json!({
            "@install": {
                "state": "start",
                "parameters": {"force": true, "image-file": image_filename}
            }
        });
        let endpoint = format!("/nvue_v1/system/image/files/{image_filename}");
        let resp = self
            .nvue_http_post(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        extract_job_id(&resp)
    }

    pub async fn list_system_images(&self) -> Result<Value> {
        self.nvue_http_get("/nvue_v1/system/image/files", HttpClient::DEFAULT_TIMEOUT)
            .await
    }

    /// Removes old unused images that are not current or boot-next.
    /// Returns the NVOS action job ID.
    pub async fn uninstall_system_image(&self) -> Result<String> {
        // No need to specify image-id, NVUE will infer it from the current system image state
        let payload = serde_json::json!({
            "@uninstall": {
                "state": "start",
                "parameters": {"force": true}
            }
        });
        let resp = self
            .nvue_http_post(
                "/nvue_v1/system/image",
                &payload,
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;
        extract_job_id(&resp)
    }

    /// Raw `/nvue_v1/system/image` view — current/next boot selection and the
    /// images present in each partition, in whatever shape the switch reports.
    pub async fn get_system_image_state(&self) -> Result<Value> {
        self.nvue_http_get("/nvue_v1/system/image", HttpClient::DEFAULT_TIMEOUT)
            .await
    }

    /// `get_system_image_state()` normalized to the stable
    /// `SwitchSystemImageState` representation. See `normalize_system_image_state`.
    pub async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
        let raw = self.get_system_image_state().await?;
        Ok(normalize_system_image_state(&raw))
    }

    /// Resolve which partition currently contains `build_id`. Returns
    /// `Ok(None)` if neither partition has a matching build-id.
    pub async fn find_system_image_partition(&self, build_id: &str) -> Result<Option<String>> {
        if build_id.is_empty() {
            return Err(RmsError::invalid_argument(
                "system image build-id must not be empty",
            ));
        }
        let state = self.get_normalized_system_image_state().await?;
        let partition_id = find_system_image_partition_id(&state, build_id);
        if partition_id.is_empty() {
            Ok(None)
        } else {
            Ok(Some(partition_id))
        }
    }

    /// SFTP a local image file to `/host/nos-images/<image_filename>`,
    /// skipping the transfer when the same file already exists remotely with
    /// a matching size. Verifies the uploaded file is present afterward.
    pub async fn push_system_image_file(
        &self,
        local_path: &str,
        image_filename: &str,
    ) -> Result<()> {
        self.push_system_image_file_with_options(
            local_path,
            image_filename,
            SftpUploadOptions::default(),
        )
        .await
    }

    /// SFTP a local image file with explicit upload timeout tunables.
    ///
    /// Emits node-scoped staging logs here while lower-level SFTP progress logs
    /// rely on the caller's tracing span for job correlation.
    pub async fn push_system_image_file_with_options(
        &self,
        local_path: &str,
        image_filename: &str,
        sftp_upload_options: SftpUploadOptions,
    ) -> Result<()> {
        if !is_valid_firmware_filename(image_filename) {
            return Err(RmsError::invalid_argument(
                "system image filename is invalid \
                 (only alphanumeric, '-', '_', '.' allowed, no hidden files)",
            ));
        }
        let local_meta = std::fs::metadata(local_path).map_err(|e| {
            RmsError::invalid_argument(format!("system image file {local_path} not found: {e}"))
        })?;
        if local_meta.len() < config::MIN_FIRMWARE_FILE_SIZE {
            return Err(RmsError::invalid_argument(format!(
                "system image file too small (minimum {} bytes)",
                config::MIN_FIRMWARE_FILE_SIZE
            )));
        }

        let remote_path = format!("/host/nos-images/{image_filename}");
        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        if let Ok(remote_size) = ssh.sftp_file_size(&remote_path).await {
            if local_meta.len() == remote_size {
                tracing::info!(
                    node = %self.id,
                    remote_path,
                    "system image already staged on switch, skipping SFTP upload"
                );
                return Ok(());
            }
        }

        tracing::info!(
            node = %self.id,
            remote_path,
            upload_timeout_secs = sftp_upload_options.overall_timeout.as_secs(),
            step_timeout_secs = sftp_upload_options.step_timeout.as_secs(),
            buffer_size_bytes = config::SFTP_BUFFER_SIZE,
            "uploading system image via SFTP"
        );

        use tracing::Instrument;

        ssh.sftp_upload_with_options(local_path, &remote_path, sftp_upload_options)
            .instrument(tracing::info_span!("switch_system_image_sftp_upload"))
            .await?;

        let verify = ssh
            .exec(
                &format!("test -f {} && echo ok", shell_quote(&remote_path)),
                SshClient::DEFAULT_TIMEOUT,
            )
            .await?;
        if !verify.contains("ok") {
            return Err(RmsError::internal(format!(
                "SFTP upload verification failed for {remote_path}"
            )));
        }
        Ok(())
    }

    // ── Switch System user security operations ──────────────────────────────

    /// Update an NVOS user's password and persist it across reboots.
    ///
    /// Flow:
    /// 1. Verify current credentials and return when the active user already
    ///    has the requested password.
    /// 2. Create an NVUE revision.
    /// 3. Stage the password change on that revision.
    /// 4. Apply the revision.
    /// 5. For active-user rotation, poll with candidate credentials while
    ///    retaining old credentials for terminal diagnostics; temporary 401s
    ///    are retryable only in this candidate-credential handoff path.
    /// 6. Treat NVUE no-config-diff as apply success for idempotency.
    /// 7. Commit in-memory credentials after candidate credentials prove the
    ///    requested active-user password is usable.
    /// 8. Save the applied config to startup, including after no-config-diff,
    ///    so retries can complete persistence after an earlier save failure.
    pub async fn update_system_user_password_persisted(
        &mut self,
        user_id: &str,
        new_password: &str,
    ) -> Result<()> {
        if user_id.is_empty() {
            return Err(RmsError::invalid_argument("username must not be empty"));
        }
        if new_password.is_empty() {
            return Err(RmsError::invalid_argument("password must not be empty"));
        }

        let (active_auth_username, requested_password_matches_active_credentials) = {
            let current_credentials = self.require_host_credentials()?;
            let matches_active_user = user_id == current_credentials.username.as_str();
            (
                matches_active_user.then(|| current_credentials.username.clone()),
                matches_active_user && current_credentials.password.expose_secret() == new_password,
            )
        };

        if requested_password_matches_active_credentials {
            // A no-op must still prove that the configured endpoint credentials
            // pass NVUE authentication. Some NVUE builds can return other
            // non-2xx statuses from `/system`; stale credentials are the 401
            // case we must reject before reporting success.
            self.verify_current_nvue_credentials().await?;

            tracing::info!(
                node = %self.id,
                user = %user_id,
                "switch password rotation skipped because requested password already authenticates"
            );

            return Ok(());
        }

        let revision_id = self.create_revision().await?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            revision = %revision_id,
            "created NVUE revision for switch password rotation"
        );

        let encoded_password = self
            .patch_system_user_password(&revision_id, user_id, new_password)
            .await?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            revision = %revision_id,
            "staged switch password update in NVUE revision"
        );

        self.nvue_start_config_revision(&revision_id)
            .await
            .map_err(|e| redact_password_update_rms_error(e, new_password, &encoded_password))?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            revision = %revision_id,
            "started NVUE revision apply for switch password rotation"
        );

        // Active-user rotation has a short ambiguity window: a successful apply
        // may require the new password, but an invalid or failed apply may
        // still only be visible with the old password. Use a candidate client
        // for polling, and keep the main client on old credentials until the
        // revision is confirmed applied or NVUE reports an idempotent no-diff.
        let candidate_nvue = active_auth_username
            .as_deref()
            .map(|username| self.build_candidate_nvue_client(username, new_password))
            .transpose()?;

        let no_config_diff = self
            .wait_for_revision_applied_or_no_config_diff(
                &revision_id,
                Duration::from_secs(config::REVISION_APPLY_TIMEOUT_SECONDS),
                candidate_nvue.as_ref(),
            )
            .await
            .map_err(|e| redact_password_update_rms_error(e, new_password, &encoded_password))?;

        if no_config_diff {
            tracing::info!(
                node = %self.id,
                user = %user_id,
                revision = %revision_id,
                "switch password revision had no config diff; continuing to save applied config"
            );
        }

        if active_auth_username.is_some() {
            // Candidate polling only completes after the requested password can
            // read the revision. Refresh before save because active-user save
            // must use the new credentials even when apply reports no diff.
            tracing::info!(
                node = %self.id,
                user = %user_id,
                revision = %revision_id,
                "refreshing RMS NVUE credentials after active-user password rotation"
            );

            self.refresh_http_credentials(new_password).await?;
        }

        self.save_applied_revision(&revision_id)
            .await
            .map_err(|e| redact_password_update_rms_error(e, new_password, &encoded_password))?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            revision = %revision_id,
            "saved applied switch password revision to startup configuration"
        );

        Ok(())
    }

    async fn verify_current_nvue_credentials(&self) -> Result<()> {
        let status = match self
            .nvue_client()?
            .probe(SYSTEM_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
            .await
        {
            Ok(status) => status,
            Err(e) => {
                let e = RmsError::from(e);

                return Err(RmsError::new(
                    e.code,
                    format!(
                        "failed to verify current switch credentials before skipping password rotation: {}",
                        e.message
                    ),
                ));
            }
        };

        if status == 401 {
            return Err(RmsError::unauthenticated(format!(
                "failed to verify current switch credentials before skipping password rotation: HTTP GET {SYSTEM_ENDPOINT} returned 401"
            )));
        }

        Ok(())
    }

    async fn create_revision(&self) -> Result<String> {
        let payload = RevisionCreate::default();

        let payload = serde_json::to_value(&payload).map_err(|e| {
            RmsError::internal(format!("failed to serialize revision create request: {e}"))
        })?;

        let response = self
            .nvue_http_post(REVISION_ENDPOINT, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .map_err(|e| {
                RmsError::new(e.code, format!("failed to create revision: {}", e.message))
            })?;

        let response = serde_json::from_value::<RevisionIdResponse>(response).map_err(|e| {
            RmsError::internal(format!("failed to parse revision create response: {e}"))
        })?;

        response
            .revision_id()
            .ok_or_else(|| RmsError::internal("no revision id in NVUE response"))
    }

    async fn patch_system_user_password(
        &self,
        revision_id: &str,
        user_id: &str,
        new_password: &str,
    ) -> Result<String> {
        if !is_valid_system_username(user_id) {
            return Err(RmsError::invalid_argument(format!(
                "invalid user_id '{user_id}': only ASCII letters and digits are allowed"
            )));
        }

        let payload = UserPasswordPatch::new(new_password);
        let encoded_password = payload.password.clone();

        let payload = serde_json::to_value(&payload).map_err(|e| {
            RmsError::internal(format!("failed to serialize password update for NVUE: {e}"))
        })?;

        let endpoint = user_endpoint_for_revision(user_id, revision_id);

        self.nvue_http_patch_with_error_body(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .map_err(|e| {
                let message =
                    redact_password_update_error(&e.message, new_password, &encoded_password);

                RmsError::new(
                    e.code,
                    format!("failed to stage password update for user '{user_id}': {message}"),
                )
            })?;

        Ok(encoded_password)
    }

    pub(crate) async fn nvue_start_config_revision(&self, revision_id: &str) -> Result<()> {
        let payload = RevisionUpdate::apply_with_yes_prompt();

        let payload = serde_json::to_value(&payload).map_err(|e| {
            RmsError::internal(format!("failed to serialize revision apply request: {e}"))
        })?;

        let endpoint = revision_endpoint(revision_id);

        self.nvue_http_patch_with_error_body(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .map_err(|e| {
                RmsError::new(
                    e.code,
                    format!("failed to apply revision {revision_id}: {}", e.message),
                )
            })?;
        Ok(())
    }

    async fn wait_for_revision_applied_or_no_config_diff(
        &self,
        revision_id: &str,
        timeout: Duration,
        candidate_nvue: Option<&SharedNvueClient>,
    ) -> Result<bool> {
        if let Some(candidate_nvue) = candidate_nvue {
            return self
                .wait_for_revision_with_candidate_credentials(revision_id, timeout, candidate_nvue)
                .await;
        }

        self.wait_for_revision_with_current_credentials(revision_id, timeout)
            .await
    }

    async fn wait_for_revision_with_current_credentials(
        &self,
        revision_id: &str,
        timeout: Duration,
    ) -> Result<bool> {
        let deadline = std::time::Instant::now() + timeout;
        let mut last_retryable_error = None;
        let mut last_observed_state = None;

        tracing::info!(
            node = %self.id,
            revision = %revision_id,
            "waiting for NVUE revision apply with current credentials"
        );

        while std::time::Instant::now() < deadline {
            match self
                .poll_revision_with_current_credentials(revision_id)
                .await
            {
                Ok(response) => {
                    let decision = revision_poll_decision(&response, revision_id, None)?;
                    if let Some(no_config_diff) = self.note_revision_poll_decision(
                        revision_id,
                        decision,
                        &mut last_observed_state,
                    ) {
                        return Ok(no_config_diff);
                    }
                }
                Err(e) if is_retryable_system_image_error(&e) => {
                    tracing::warn!(
                        node = %self.id,
                        revision = %revision_id,
                        error = %e.message,
                        "revision poll transient error; retrying"
                    );

                    last_retryable_error = Some(e);
                }
                Err(e) => return Err(e),
            }

            tokio::time::sleep(Duration::from_secs(config::REVISION_POLL_INTERVAL_SECONDS)).await;
        }

        Err(revision_apply_timeout(
            revision_id,
            last_observed_state,
            last_retryable_error,
        ))
    }

    async fn wait_for_revision_with_candidate_credentials(
        &self,
        revision_id: &str,
        timeout: Duration,
        candidate_nvue: &SharedNvueClient,
    ) -> Result<bool> {
        let deadline = std::time::Instant::now() + timeout;
        let mut last_retryable_error = None;
        let mut last_observed_state = None;
        let mut reported_current_credential_diagnostics = false;

        tracing::info!(
            node = %self.id,
            revision = %revision_id,
            "waiting for password revision apply with candidate credentials"
        );

        while std::time::Instant::now() < deadline {
            match Self::poll_revision_with_nvue(candidate_nvue, revision_id).await {
                Ok(response) => {
                    let decision = revision_poll_decision(
                        &response,
                        revision_id,
                        Some("candidate credentials"),
                    )?;

                    if let Some(no_config_diff) = self.note_revision_poll_decision(
                        revision_id,
                        decision,
                        &mut last_observed_state,
                    ) {
                        return Ok(no_config_diff);
                    }
                }
                Err(e) if is_candidate_revision_poll_error(&e) => {
                    tracing::debug!(
                        node = %self.id,
                        revision = %revision_id,
                        error = %e.message,
                        "revision poll with candidate credentials not ready; checking current credentials for terminal apply diagnostics"
                    );

                    last_retryable_error = Some(e);

                    if !reported_current_credential_diagnostics {
                        tracing::info!(
                            node = %self.id,
                            revision = %revision_id,
                            "candidate credentials not ready; checking current credentials for revision diagnostics"
                        );

                        reported_current_credential_diagnostics = true;
                    }

                    match self
                        .poll_current_credentials_for_candidate_diagnostics(revision_id)
                        .await?
                    {
                        CandidateRevisionDiagnostic::Continue {
                            observed_state,
                            retryable_error,
                        } => {
                            if let Some(state) = observed_state {
                                last_observed_state = Some(state);
                            }

                            if let Some(error) = retryable_error {
                                last_retryable_error = Some(error);
                            }
                        }
                    }
                }
                Err(e) => return Err(e),
            }

            tokio::time::sleep(Duration::from_secs(config::REVISION_POLL_INTERVAL_SECONDS)).await;
        }

        Err(revision_apply_timeout(
            revision_id,
            last_observed_state,
            last_retryable_error,
        ))
    }

    fn note_revision_poll_decision(
        &self,
        revision_id: &str,
        decision: RevisionPollDecision,
        last_observed_state: &mut Option<String>,
    ) -> Option<bool> {
        match decision {
            RevisionPollDecision::Complete { no_config_diff } => {
                if no_config_diff {
                    tracing::info!(
                        node = %self.id,
                        revision = %revision_id,
                        "revision apply reported no config diff"
                    );
                } else {
                    tracing::info!(node = %self.id, revision = %revision_id, "revision reached applied state");
                }

                Some(no_config_diff)
            }
            RevisionPollDecision::Pending { observed_state } => {
                if let Some(state) = observed_state {
                    *last_observed_state = Some(state);
                }

                None
            }
        }
    }

    async fn poll_current_credentials_for_candidate_diagnostics(
        &self,
        revision_id: &str,
    ) -> Result<CandidateRevisionDiagnostic> {
        match self
            .poll_revision_with_current_credentials(revision_id)
            .await
        {
            Ok(response) => {
                self.current_credential_candidate_diagnostic_from_response(&response, revision_id)
            }
            Err(e) if is_candidate_revision_poll_error(&e) => {
                // Once NVUE accepts an active-user password apply, old
                // credentials can stop working before candidate credentials are
                // accepted for revision polling. Treat old-credential 401 as
                // handoff noise only in this candidate path; current-only
                // revision polling remains fail-fast on 401.
                tracing::debug!(
                    node = %self.id,
                    revision = %revision_id,
                    error = %e.message,
                    "revision poll with current credentials also not ready during credential handoff; retrying"
                );

                Ok(CandidateRevisionDiagnostic::Continue {
                    observed_state: None,
                    retryable_error: Some(e),
                })
            }
            Err(e) => Err(e),
        }
    }

    fn current_credential_candidate_diagnostic_from_response(
        &self,
        response: &RevisionResponse,
        revision_id: &str,
    ) -> Result<CandidateRevisionDiagnostic> {
        match revision_apply_poll_outcome(response, revision_id)? {
            RevisionApplyPollOutcome::Applied => {
                // Do not accept `applied` from the old credentials for an
                // active-user password change. Rotation is complete only after
                // the new credentials can read the applied revision. Old
                // credentials are retained here to expose terminal NVUE
                // diagnostics when candidate auth is rejected.
                Ok(CandidateRevisionDiagnostic::Continue {
                    observed_state: Some("current credentials state=applied".to_owned()),
                    retryable_error: None,
                })
            }
            RevisionApplyPollOutcome::NoConfigDiff => {
                tracing::info!(
                    node = %self.id,
                    revision = %revision_id,
                    "current credentials reported no config diff while candidate credentials are not ready; retrying candidate credentials"
                );

                Ok(CandidateRevisionDiagnostic::Continue {
                    observed_state: Some("current credentials no config diff".to_owned()),
                    retryable_error: None,
                })
            }
            RevisionApplyPollOutcome::Pending { state } => {
                Ok(CandidateRevisionDiagnostic::Continue {
                    observed_state: revision_pending_state(Some("current credentials"), &state),
                    retryable_error: None,
                })
            }
        }
    }

    async fn poll_revision_with_current_credentials(
        &self,
        revision_id: &str,
    ) -> Result<RevisionResponse> {
        let endpoint = revision_endpoint(revision_id);
        let response = self
            .nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        serde_json::from_value(response).map_err(|e| {
            RmsError::internal(format!(
                "failed to parse revision {revision_id} response: {e}"
            ))
        })
    }

    async fn poll_revision_with_nvue(
        nvue: &SharedNvueClient,
        revision_id: &str,
    ) -> Result<RevisionResponse> {
        let endpoint = revision_endpoint(revision_id);
        let response = nvue
            .get_json(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        serde_json::from_value(response).map_err(|e| {
            RmsError::internal(format!(
                "failed to parse revision {revision_id} response: {e}"
            ))
        })
    }

    async fn save_applied_revision(&self, revision_id: &str) -> Result<()> {
        let payload = RevisionUpdate::save_with_yes_prompt();

        let payload = serde_json::to_value(&payload).map_err(|e| {
            RmsError::internal(format!("failed to serialize revision save request: {e}"))
        })?;

        // Password rotation reaches this point even after an idempotent
        // no-diff apply result. Saving `/revision/applied` persists the current
        // running config, which lets retries repair an earlier apply-succeeded
        // but save-failed attempt.
        let endpoint = applied_revision_endpoint();
        let primary = self
            .nvue_http_patch_with_error_body(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await;

        if primary.is_ok() {
            return Ok(());
        }

        // Some NVUE builds accept save on the concrete revision endpoint rather
        // than `/revision/applied`; keep both NVUE-only paths before failing.
        let endpoint = revision_endpoint(revision_id);
        let fallback = self
            .nvue_http_patch_with_error_body(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await;

        match (primary, fallback) {
            (Err(primary_err), Err(fallback_err)) => {
                let code = match (primary_err.code, fallback_err.code) {
                    (ErrorCode::Unauthenticated, _) | (_, ErrorCode::Unauthenticated) => {
                        ErrorCode::Unauthenticated
                    }
                    (ErrorCode::NotFound, fallback_code) => fallback_code,
                    (primary_code, _) => primary_code,
                };

                Err(RmsError::new(
                    code,
                    format!(
                        "failed to save configuration after apply: /revision/applied: {}; /revision/{revision_id}: {}",
                        primary_err.message, fallback_err.message
                    ),
                ))
            }
            _ => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn mtls_skip_nvue_for_test(&self) -> bool {
        self.ssh_exec_for_test.is_some()
    }

    #[cfg(not(test))]
    pub(crate) fn mtls_skip_nvue_for_test(&self) -> bool {
        false
    }

    pub(crate) async fn wait_for_nvue_action_completion(&self, action_id: &str) -> Result<()> {
        let timeout = Duration::from_secs(config::REVISION_APPLY_TIMEOUT_SECONDS);
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            let status = self.poll_nvue_action_task(action_id).await?;
            if status.completed {
                if status.state == "action_success" || status.percent == 100 {
                    return Ok(());
                }
                let message = if status.message.is_empty() {
                    format!("NVUE action {action_id} failed with state {}", status.state)
                } else {
                    status.message
                };
                return Err(RmsError::internal(message));
            }
            tokio::time::sleep(Duration::from_secs(config::REVISION_POLL_INTERVAL_SECONDS)).await;
        }

        Err(RmsError::timeout(format!(
            "timed out waiting for NVUE action {action_id} to complete"
        )))
    }

    pub(crate) async fn nvue_start_action(
        &self,
        endpoint: &str,
        action: &str,
        parameters: Value,
    ) -> Result<()> {
        let payload = serde_json::json!({
            action: {
                "state": "start",
                "parameters": parameters,
            }
        });

        self.nvue_run_action_payload(endpoint, action, payload)
            .await
    }

    async fn nvue_run_action_payload(
        &self,
        endpoint: &str,
        action_name: &str,
        payload: Value,
    ) -> Result<()> {
        let response = self
            .nvue_http_post(endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .map_err(|e| {
                RmsError::internal(format!(
                    "NVUE POST {endpoint} {action_name} failed: {}",
                    e.message
                ))
            })?;

        if let Ok(job_id) = extract_job_id(&response) {
            self.wait_for_nvue_action_completion(&job_id).await?;
        }

        Ok(())
    }

    pub(crate) async fn nvue_apply_config_patches(&self, patches: &[(&str, Value)]) -> Result<()> {
        if patches.is_empty() {
            return Ok(());
        }

        let revision_id = self.nvue_stage_config_patches(patches).await?;
        self.nvue_start_config_revision(&revision_id).await?;
        self.nvue_finish_config_revision(&revision_id).await
    }

    pub(crate) async fn nvue_stage_config_patches(
        &self,
        patches: &[(&str, Value)],
    ) -> Result<String> {
        if patches.is_empty() {
            return Err(RmsError::invalid_argument(
                "at least one NVUE configuration patch is required",
            ));
        }

        let revision_id = self.create_revision().await?;
        for (path, payload) in patches {
            let endpoint = format!("{path}?rev={revision_id}");
            self.nvue_http_patch(&endpoint, payload, HttpClient::DEFAULT_TIMEOUT)
                .await
                .map_err(|e| {
                    RmsError::internal(format!("NVUE PATCH {endpoint} failed: {}", e.message))
                })?;
        }

        Ok(revision_id)
    }

    pub(crate) async fn nvue_finish_config_revision(&self, revision_id: &str) -> Result<()> {
        let no_config_diff = self
            .wait_for_revision_applied_or_no_config_diff(
                revision_id,
                Duration::from_secs(config::REVISION_APPLY_TIMEOUT_SECONDS),
                None,
            )
            .await?;
        if !no_config_diff {
            self.save_applied_revision(revision_id).await?;
        }
        Ok(())
    }

    async fn nvue_get_system_hello(&self) -> Result<()> {
        self.nvue_http_get("/nvue_v1/system", NVUE_DEFAULT_TIMEOUT)
            .await
            .map(|_| ())
    }

    pub(crate) async fn verify_nvue_api_hello(&self) -> Result<()> {
        self.nvue_get_system_hello().await
    }

    /// True when a short NVUE connectivity retry may succeed.
    pub(crate) fn is_nvue_retryable_error(err: &RmsError) -> bool {
        if matches!(
            err.code,
            ErrorCode::Timeout
                | ErrorCode::Unavailable
                | ErrorCode::ConnectionRefused
                | ErrorCode::DnsResolutionFailed
                | ErrorCode::Unauthenticated
        ) {
            return true;
        }

        if err.code != ErrorCode::Internal {
            return false;
        }

        let message = err.message.to_ascii_lowercase();

        [
            "tls",
            "certificate",
            "connection",
            "connect",
            "http 401",
            "http 403",
            "returned 401",
            "returned 403",
        ]
        .iter()
        .any(|needle| message.contains(needle))
    }

    fn build_candidate_nvue_client(
        &self,
        username: &str,
        password: &str,
    ) -> Result<SharedNvueClient> {
        Ok(self
            .nvue_client()?
            .clone_with_credentials(NvueCredentials::new(username, password)))
    }

    async fn refresh_http_credentials(&mut self, new_password: &str) -> Result<()> {
        let Some(host_endpoint) = self.host_endpoint.as_mut() else {
            return Err(RmsError::failed_precondition(
                "switch host endpoint is required for NVUE/NVOS operations",
            ));
        };
        let Some(credentials) = host_endpoint.credentials.as_mut() else {
            return Err(RmsError::failed_precondition(
                "switch host credentials are required for NVUE/NVOS operations",
            ));
        };

        let username = credentials.username.clone();
        credentials.password = SecretString::from(new_password);
        self.nvue_client()?
            .update_credentials(NvueCredentials::new(&username, new_password))
            .await?;

        tracing::debug!(
            node = %self.id,
            user = %username,
            "refreshed in-memory NVUE credentials after password rotation"
        );

        Ok(())
    }

    /// Recover the `admin` password after a reboot that put the switch into
    /// the forced expired-password change flow. SSHes in with the default
    /// `admin`/`admin` credentials, completes the expired-password change
    /// with `new_password`, then validates a fresh SSH login with the new
    /// credentials and refreshes this Switch's internal HTTP client and host
    /// endpoint credentials.
    ///
    /// Precondition: this Switch's host endpoint user must be `admin`.
    pub async fn recover_admin_password_after_boot(&mut self, new_password: &str) -> Result<()> {
        let host_username = self.require_host_credentials()?.username.clone();
        if host_username != "admin" {
            return Err(RmsError::failed_precondition(
                "post-boot admin password recovery requires the switch user to be admin",
            ));
        }
        if new_password.is_empty() {
            return Err(RmsError::invalid_argument("new password must not be empty"));
        }

        let host_ip_address = self.require_host_endpoint()?.ip_address.clone();

        tracing::info!(node = %self.id, "attempting expired admin recovery over SSH");

        let recovery = SshClient::connect(
            SshEndpoint::new(&host_ip_address, "admin", "admin"),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await?;

        tracing::info!(node = %self.id, "connected with default admin credentials");

        recovery
            .complete_expired_password_change(new_password, Duration::from_secs(90))
            .await?;
        tracing::info!(node = %self.id, "completed SSH password change; validating restored login");

        let validation_timeout = Duration::from_secs(60);
        let validation_interval = Duration::from_secs(5);
        let deadline = std::time::Instant::now() + validation_timeout;

        loop {
            let err = match SshClient::connect(
                SshEndpoint::new(&host_ip_address, &host_username, new_password),
                validation_interval,
            )
            .await
            {
                Ok(_) => {
                    self.refresh_http_credentials(new_password).await?;
                    tracing::info!(node = %self.id, "refreshed live credentials after SSH recovery");
                    return Ok(());
                }
                Err(e) => {
                    tracing::info!(
                        node = %self.id,
                        error = %e.message,
                        "restored SSH validation attempt failed"
                    );
                    e
                }
            };

            if std::time::Instant::now() >= deadline {
                return Err(RmsError::internal(format!(
                    "SSH password recovery completed but restored SSH login validation failed: {}",
                    err.message
                )));
            }
            tokio::time::sleep(validation_interval).await;
        }
    }

    pub async fn get_chassis_location_info(&self) -> Result<Value> {
        let resp = self
            .nvue_http_get(
                "/nvue_v1/platform/chassis-location",
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;

        Ok(serde_json::json!({
            "switch_info": {
                "chassis_sn": resp.get("chassis-sn").and_then(|v| v.as_str()).unwrap_or_default(),
                "slot_number": resp.get("slot-number").and_then(|v| v.as_str()).unwrap_or_default(),
                "topology_id": resp.get("topology-id").and_then(|v| v.as_str()).unwrap_or_default(),
                "tray_index": resp.get("tray-index").and_then(|v| v.as_str()).unwrap_or_default(),
            }
        }))
    }

    // ── ScaleUpFabric cluster configuration ─────────────────────────

    fn desired_cluster_state(enabled: bool) -> &'static str {
        if enabled { "enabled" } else { "disabled" }
    }

    pub async fn set_cluster_state(&self, enabled: bool) -> Result<()> {
        let state = Self::desired_cluster_state(enabled);
        tracing::info!(node = %self.id, enabled, desired_state = state, "set_cluster_state");

        {
            let cluster = self
                .nvue_http_get("/nvue_v1/cluster", HttpClient::DEFAULT_TIMEOUT)
                .await?;
            let current_state = cluster
                .get("state")
                .and_then(|value| value.as_str())
                .ok_or_else(|| RmsError::internal("unable to determine current cluster state"))?;

            tracing::info!(
                node = %self.id,
                enabled,
                current_state,
                desired_state = state,
                "scale-up fabric current state observed"
            );

            if current_state == state || (enabled && current_state == "start") {
                tracing::info!(
                    node = %self.id,
                    enabled,
                    current_state,
                    desired_state = state,
                    "scale-up fabric state already set; skipping nv cli update"
                );
                return Ok(());
            }
        }

        self.run_cluster_state_nv_commands(state).await?;

        self.wait_for_cluster_state(state, enabled)
            .await
            .map(|_| ())
    }

    async fn run_cluster_state_nv_commands(&self, state: &str) -> Result<()> {
        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            exec(&format!("nv set cluster state {state}"))?;
            exec("nv config apply --assume-yes")?;
            exec("nv config save")?;
            return Ok(());
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;
        ssh.exec(
            &format!("nv set cluster state {state}"),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await?;
        ssh.exec("nv config apply --assume-yes", SshClient::DEFAULT_TIMEOUT)
            .await?;
        ssh.exec("nv config save", SshClient::DEFAULT_TIMEOUT)
            .await?;
        Ok(())
    }

    // Set cluster state via SSH then poll NVUE REST until it converges
    pub async fn update_cluster_config(&self, enabled: bool) -> Result<()> {
        let state = Self::desired_cluster_state(enabled);

        tracing::info!(node = %self.id, enabled, "update_cluster_config");
        self.run_cluster_state_nv_commands(state).await?;

        self.wait_for_cluster_state(state, enabled)
            .await
            .map(|_| ())
    }

    async fn wait_for_cluster_state(&self, expected: &str, allow_start: bool) -> Result<Value> {
        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            if let Ok(cluster) = self
                .nvue_http_get("/nvue_v1/cluster", HttpClient::DEFAULT_TIMEOUT)
                .await
            {
                if let Some(state) = cluster.get("state").and_then(|v| v.as_str()) {
                    if state == expected || (allow_start && state == "start") {
                        return Ok(cluster);
                    }
                }
            }

            if attempt < config::MAX_RETRY_ATTEMPTS {
                tokio::time::sleep(RETRY_WAIT_DELAY).await;
            }
        }

        Err(RmsError::internal(format!(
            "cluster state did not reach {expected} after retries"
        )))
    }

    pub async fn get_cluster_state(&self) -> Result<Value> {
        let mut result = self
            .nvue_http_get("/nvue_v1/cluster", HttpClient::DEFAULT_TIMEOUT)
            .await?;

        let state = result
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_owned();

        if state == "disabled" || state == "unknown" {
            insert_json_field(&mut result, "cluster_healthy", serde_json::json!(false));
            insert_json_field(
                &mut result,
                "warning",
                serde_json::json!("Cluster is disabled"),
            );
            insert_json_field(
                &mut result,
                "recommendation",
                serde_json::json!(
                    "Enable cluster with update_cluster_config before performing cluster operations"
                ),
            );
        } else {
            let nmxc_down = result
                .get("nmxc-conn")
                .and_then(|v| v.as_object())
                .and_then(|obj| obj.get("state"))
                .and_then(|v| v.as_str())
                .is_some_and(|s| s == "down" || s == "disabled");

            insert_json_field(
                &mut result,
                "cluster_healthy",
                serde_json::json!(!nmxc_down),
            );
            if nmxc_down {
                insert_json_field(
                    &mut result,
                    "warning",
                    serde_json::json!("nmxc-conn is down"),
                );
            }
        }

        Ok(result)
    }

    pub(crate) async fn get_switch_security_certificate_object(
        &self,
        certificate_id: &str,
    ) -> Result<Value> {
        if !is_valid_identifier(certificate_id) {
            return Err(RmsError::invalid_argument(format!(
                "invalid switch certificate id: {certificate_id}"
            )));
        }
        self.nvue_http_get(
            &format!("/nvue_v1/system/security/certificate/{certificate_id}"),
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn get_switch_security_ca_certificate_object(
        &self,
        ca_certificate_id: &str,
    ) -> Result<Value> {
        if !is_valid_identifier(ca_certificate_id) {
            return Err(RmsError::invalid_argument(format!(
                "invalid switch CA certificate id: {ca_certificate_id}"
            )));
        }
        self.nvue_http_get(
            &format!("/nvue_v1/system/security/ca-certificate/{ca_certificate_id}"),
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn get_gnmi_server_mtls_configuration(&self) -> Result<Value> {
        self.nvue_http_get(
            "/nvue_v1/system/gnmi-server/mtls",
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn get_nvue_api_mtls_configuration(&self) -> Result<Value> {
        self.nvue_http_get("/nvue_v1/system/api/mtls", HttpClient::DEFAULT_TIMEOUT)
            .await
    }

    pub(crate) async fn get_cluster_app_manager_leaf(
        &self,
        app_name: &str,
        leaf: &str,
    ) -> Result<Value> {
        if !is_valid_identifier(app_name) || !is_valid_identifier(leaf) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app manager path: {app_name}/{leaf}"
            )));
        }
        self.nvue_http_get(
            &format!("/nvue_v1/cluster/apps/{app_name}/manager/{leaf}"),
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub async fn get_cluster_apps_status(&self, app_name: &str) -> Result<Value> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        match self
            .nvue_http_get(
                &format!("/nvue_v1/cluster/apps/{app_name}"),
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await
        {
            Ok(mut result) => {
                let status = result
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_owned();
                insert_json_field(&mut result, "installed", serde_json::json!(true));
                insert_json_field(
                    &mut result,
                    "health_status",
                    serde_json::json!(if status == "ok" { "green" } else { "red" }),
                );
                let status_description = if status == "ok" {
                    serde_json::json!(format!("{app_name} is running and healthy"))
                } else {
                    serde_json::json!(format!("{app_name} status: {status}"))
                };
                insert_json_field(&mut result, "status_description", status_description);
                Ok(result)
            }
            Err(e) if e.code == ErrorCode::NotFound => Ok(serde_json::json!({
                "installed": false,
                "health_status": "unknown",
                "status_description": format!("App {app_name} is not installed"),
            })),
            Err(e) => Err(e),
        }
    }

    // Idempotent: checks current gRPC state before toggling via SSH
    pub async fn enable_grpc_for_external_clients(
        &self,
        app_name: &str,
        enabled: bool,
    ) -> Result<Value> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        let desired = if enabled { "enabled" } else { "disabled" };
        let target_switch = self.require_host_endpoint()?.ip_address.clone();

        {
            tracing::info!(node = %self.id, app_name, enabled, "enable_grpc_for_external_clients");

            // Short-circuit if already in desired state.
            if let Ok(pre) = self.check_grpc_status(app_name, &target_switch).await {
                if let Some(summary) = pre.get("summary") {
                    let already_enabled = summary
                        .get("grpc_enabled")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let port_reachable = summary
                        .get("port_reachable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if enabled && already_enabled && port_reachable {
                        tracing::info!(node = %self.id, "gRPC already enabled, skipping");
                        return Ok(pre);
                    }
                    if !enabled && !already_enabled {
                        tracing::info!(node = %self.id, "gRPC already disabled, skipping");
                        return Ok(pre);
                    }
                }
            }
        }

        if enabled {
            match self
                .wait_for_cluster_app_manager_action_ready(app_name)
                .await?
            {
                Some(status) if app_manager_reached_state(&status, desired) => return Ok(status),
                Some(_) => {}
                None => {
                    tracing::warn!(
                        node = %self.id,
                        app_name,
                        "cluster app manager-action readiness was not observed; attempting action"
                    );
                }
            }
        }

        self.run_cluster_app_manager_action(app_name, desired)
            .await?;

        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            let status = self.get_cluster_apps_status(app_name).await;

            if let Ok(status) = status {
                if app_manager_reached_state(&status, desired) {
                    return Ok(status);
                }
            }

            if attempt < config::MAX_RETRY_ATTEMPTS {
                tokio::time::sleep(RETRY_WAIT_DELAY).await;
            }
        }

        Err(RmsError::internal(format!(
            "gRPC manager for {app_name} did not reach {desired}"
        )))
    }

    async fn wait_for_cluster_app_manager_action_ready(
        &self,
        app_name: &str,
    ) -> Result<Option<Value>> {
        // NVOS can report the cluster enabled before the app manager callback
        // accepts manager actions. Wait only while NVUE reports an explicit
        // transitional state; when readiness is not observable, let the SSH
        // action remain the source of truth.
        let mut start_requested = false;

        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            let should_wait = match self
                .nvue_http_get("/nvue_v1/cluster", HttpClient::DEFAULT_TIMEOUT)
                .await
            {
                Ok(cluster) if !cluster_enabled_for_app_manager_action(&cluster) => true,
                Ok(cluster) => {
                    let cluster_ready = cluster_ready_for_app_manager_action(&cluster);
                    let nmxc_conn_pending = nvue_state_field(cluster.get("nmxc-conn"))
                        .is_some_and(|state| state != NVUE_NMXC_CONN_UP);

                    match self.get_cluster_apps_status(app_name).await {
                        Ok(app_status)
                            if cluster_ready && app_ready_for_manager_action(&app_status) =>
                        {
                            return Ok(Some(app_status));
                        }
                        Ok(app_status) if app_stopped_for_manager_action(&app_status) => {
                            if !start_requested {
                                tracing::info!(
                                    node = %self.id,
                                    app_name,
                                    "starting stopped cluster app before manager action"
                                );

                                self.start_cluster_app(app_name).await?;

                                start_requested = true;
                            }

                            true
                        }
                        Ok(app_status) => {
                            nmxc_conn_pending || app_waiting_for_manager_action(&app_status)
                        }
                        Err(error) => {
                            tracing::debug!(
                                node = %self.id,
                                app_name,
                                error = %error.message,
                                "cluster app readiness was not observable before manager action"
                            );

                            nmxc_conn_pending
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        node = %self.id,
                        app_name,
                        error = %error.message,
                        "cluster readiness was not observable before manager action"
                    );
                    false
                }
            };

            if !should_wait {
                return Ok(None);
            }

            if attempt < config::MAX_RETRY_ATTEMPTS {
                tokio::time::sleep(RETRY_WAIT_DELAY).await;
            }
        }

        if start_requested {
            Err(RmsError::internal(format!(
                "cluster app {app_name} did not become ready after start"
            )))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn ensure_cluster_app_manager_action_ready(
        &self,
        app_name: &str,
    ) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        if self
            .wait_for_cluster_app_manager_action_ready(app_name)
            .await?
            .is_none()
        {
            tracing::warn!(
                node = %self.id,
                app_name,
                "cluster app manager-action readiness was not observed; attempting action"
            );
        }

        Ok(())
    }

    async fn start_cluster_app(&self, app_name: &str) -> Result<()> {
        let cmd = format!("nv action start cluster apps {app_name}");

        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            exec(&cmd)?;
            return Ok(());
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;
        ssh.exec(&cmd, SshClient::DEFAULT_TIMEOUT).await?;
        Ok(())
    }

    async fn run_cluster_app_manager_nv_action(&self, app_name: &str, args: &str) -> Result<()> {
        let cmd = format!("nv action update cluster apps {app_name} manager {args}");

        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            exec(&cmd)?;
            return Ok(());
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;
        ssh.exec(&cmd, SshClient::DEFAULT_TIMEOUT).await?;
        Ok(())
    }

    async fn run_cluster_app_manager_action(&self, app_name: &str, desired: &str) -> Result<()> {
        self.run_cluster_app_manager_nv_action(app_name, desired)
            .await
    }

    /// Runs `nv action update cluster apps <app> manager <field> <value>` over SSH.
    pub(crate) async fn run_cluster_app_manager_field_action(
        &self,
        app_name: &str,
        field: &str,
        value: &str,
    ) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app name: {app_name}"
            )));
        }
        if field.is_empty() || value.is_empty() {
            return Err(RmsError::invalid_argument(
                "cluster app manager field and value must be non-empty",
            ));
        }
        self.run_cluster_app_manager_nv_action(app_name, &format!("{field} {value}"))
            .await
    }

    // Enable/disable gNMI via SSH, then poll until state converges
    pub async fn gnmi_service(&self, enabled: bool) -> Result<Value> {
        let state = if enabled { "enabled" } else { "disabled" };

        tracing::info!(node = %self.id, enabled, "gnmi_service");

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        ssh.exec(
            &format!("nv set system gnmi-server state {state}"),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await?;

        ssh.exec("nv config apply --assume-yes", SshClient::DEFAULT_TIMEOUT)
            .await?;

        ssh.exec("nv config save", SshClient::DEFAULT_TIMEOUT)
            .await?;

        tokio::time::sleep(Duration::from_secs(config::GNMI_CONFIG_WAIT_SECONDS)).await;

        let delays = [
            config::GNMI_RETRY_DELAY_1_SECONDS,
            config::GNMI_RETRY_DELAY_2_SECONDS,
            config::GNMI_RETRY_DELAY_2_SECONDS,
        ];

        for delay in delays.iter().take(config::MAX_RETRY_ATTEMPTS as usize) {
            if let Ok(ssh) =
                SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await
            {
                if let Ok(output) = ssh
                    .exec(
                        "nv show system gnmi-server -o json",
                        SshClient::DEFAULT_TIMEOUT,
                    )
                    .await
                {
                    if let Ok(j) = serde_json::from_str::<Value>(&output) {
                        if j.get("state").and_then(|v| v.as_str()) == Some(state) {
                            return Ok(serde_json::json!({"status": "success", "state": state}));
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(*delay)).await;
        }

        Err(RmsError::internal(format!(
            "gNMI service did not reach state {state} after retries"
        )))
    }

    pub async fn restart_cluster_app(&self, app_name: &str) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        let endpoint = app_endpoint(app_name);

        let stop_payload = serde_json::to_value(ClusterAppStopRequest::new()).map_err(|e| {
            RmsError::internal(format!("failed to serialize cluster app stop action: {e}"))
        })?;

        let start_payload = serde_json::to_value(ClusterAppStartRequest::new()).map_err(|e| {
            RmsError::internal(format!("failed to serialize cluster app start action: {e}"))
        })?;

        tracing::info!(node = %self.id, app_name, "restart_cluster_app: stop");

        self.nvue_run_action_payload(&endpoint, "@stop", stop_payload)
            .await?;

        tracing::info!(node = %self.id, app_name, "restart_cluster_app: start");

        self.nvue_run_action_payload(&endpoint, "@start", start_payload)
            .await?;

        Ok(())
    }

    pub async fn check_grpc_status(&self, app_name: &str, target_switch: &str) -> Result<Value> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }
        if target_switch.is_empty() {
            return Err(RmsError::invalid_argument(
                "target switch name cannot be empty",
            ));
        }

        let grpc_port = grpc_port_for_app(app_name);
        let endpoint = format!("/nvue_v1/cluster/apps/{app_name}/manager");
        let manager_resp = self
            .nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await;

        let (grpc_enabled, grpc_status) = match manager_resp {
            Ok(resp) => {
                let s = resp
                    .get("state")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let enabled = s == "start" || s == "enabled" || s == "active";
                (enabled, resp)
            }
            Err(_) => (false, Value::Null),
        };

        let port_reachable =
            test_port_connectivity(target_switch, grpc_port, Duration::from_secs(10))
                .await
                .unwrap_or(false);

        if port_reachable {
            tracing::info!("gRPC port {} is reachable", grpc_port);
        } else {
            tracing::info!("gRPC port {} is not reachable", grpc_port);
        }

        Ok(serde_json::json!({
            "app_name": app_name,
            "target_switch": target_switch,
            "grpc_status": grpc_status,
            "port_connectivity": {
                "reachable": port_reachable,
                "port": grpc_port,
            },
            "summary": {
                "grpc_enabled": grpc_enabled,
                "port_reachable": port_reachable,
                "ready_for_external_clients": grpc_enabled && port_reachable,
            },
        }))
    }

    async fn poll_nvue_action_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let endpoint = format!("/nvue_v1/action/{task_id}");
        let resp = self
            .nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        Self::parse_firmware_task_response(&resp)
    }

    // ── Private inner methods (called with lock already held) ───────

    // Interpret NVUE action state machine: action_success/action_error/
    // action_running/action_failed, with reboot-hint detection.
    fn parse_firmware_task_response(resp: &Value) -> Result<FirmwareTaskStatus> {
        let state = resp
            .get("state")
            .and_then(|v| v.as_str())
            .or_else(|| resp.get("status").and_then(|v| v.as_str()))
            .unwrap_or_default()
            .to_owned();
        let status = resp
            .get("detail")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();

        let mut st = FirmwareTaskStatus {
            state: state.clone(),
            status,
            ..Default::default()
        };

        if state == "action_success" {
            st.completed = true;
            st.percent = 100;
        } else if state == "action_error" {
            let detail = resp
                .get("detail")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let status_field = resp
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let issue_message = extract_action_issue_message(resp);
            st.completed = true;

            if !issue_message.is_empty() {
                st.message = issue_message;
                if indicates_noop(&st.message) {
                    st.completed = true;
                    st.percent = 100;
                    st.state = "action_success".to_owned();
                }
            } else if contains_reboot_hint(&detail) || contains_reboot_hint(&status_field) {
                // NVUE reports "action_error" for a reboot; treat as success
                st.percent = 100;
            } else if !detail.is_empty() {
                st.message = detail;
            } else {
                st.message = status_field;
            }
        } else if state == "action_running" {
            let status_field = resp
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            if contains_reboot_hint(&status_field) {
                st.completed = true;
                st.percent = 100;
                st.state = "action_success".to_owned();
            }
        } else if state == "action_failed" {
            st.completed = true;
            let issue_message = extract_action_issue_message(resp);
            if !issue_message.is_empty() {
                st.message = issue_message;
            } else {
                st.message = resp
                    .get("detail")
                    .and_then(|v| v.as_str())
                    .unwrap_or("firmware job failed")
                    .to_owned();
            }
        }

        Ok(st)
    }
}

// ── Node trait implementation ───────────────────────────────────────

#[async_trait]
impl Node for SwitchGb200Nvidia {
    fn id(&self) -> &str {
        SwitchGb200Nvidia::id(self)
    }

    fn rack_id(&self) -> &str {
        SwitchGb200Nvidia::rack_id(self)
    }

    fn node_type(&self) -> NodeType {
        self.node_type
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = HashMap::from([("type".to_owned(), self.node_type.as_str().to_owned())]);
        if let Some(endpoint) = &self.bmc_endpoint {
            info.insert("host".to_owned(), endpoint.endpoint.ip_address.clone());
            info.insert("port".to_owned(), endpoint.endpoint.port.to_string());
            info.insert(
                "macAddress".to_owned(),
                endpoint.endpoint.mac_address.clone(),
            );
        }

        if let Some(endpoint) = &self.host_endpoint {
            if !endpoint.endpoint.mac_address.is_empty() {
                info.insert(
                    "hostMac_0".to_owned(),
                    endpoint.endpoint.mac_address.clone(),
                );
            }

            info.insert("hostIp_0".to_owned(), endpoint.endpoint.ip_address.clone());
        }

        info
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        if let Some(power_http) = &self.power_http {
            let http = power_http.lock().await;
            tracing::debug!(node = %self.id, "get_power_state via BMC Redfish");
            return match http
                .get("/redfish/v1/Systems/System_0", HttpClient::DEFAULT_TIMEOUT)
                .await
            {
                Ok(resp) => Ok(parse_switch_redfish_power_state(&resp)),
                Err(e) => {
                    tracing::warn!(
                        node = %self.id,
                        error_code = ?e.code,
                        error_msg = %e.message,
                        "get_power_state: BMC Redfish status unavailable; returning Unknown"
                    );
                    Ok(PowerState::Unknown)
                }
            };
        }

        let host = self
            .host_endpoint
            .as_ref()
            .map(|endpoint| endpoint.endpoint.ip_address.as_str())
            .unwrap_or_default();

        tracing::debug!(node = %self.id, host, "get_power_state via NVOS fallback");
        match self
            .nvue_http_get("/nvue_v1/system", HttpClient::DEFAULT_TIMEOUT)
            .await
        {
            Ok(_) => Ok(PowerState::On),
            Err(e) => {
                tracing::warn!(
                    node = %self.id,
                    host,
                    error_code = ?e.code,
                    error_msg = %e.message,
                    "get_power_state: NVOS status unavailable; returning Unknown"
                );
                Ok(PowerState::Unknown)
            }
        }
    }

    async fn set_power_state(&self, op: PowerOp, _target: PowerTargetType) -> Result<()> {
        tracing::info!(node = %self.id, ?op, "set_power_state");

        if let Some(power_http) = &self.power_http {
            let reset_type = switch_redfish_reset_type(op)?;
            let payload = serde_json::json!({"ResetType": reset_type});
            let http = power_http.lock().await;
            http.post(
                "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
                &payload,
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;
            return Ok(());
        }

        if op != PowerOp::PowerCycle {
            return Err(RmsError::invalid_argument(
                "switch nodes require BMC Redfish power details for non-PowerCycle operations",
            ));
        }

        let payload = serde_json::json!({
            "@power-cycle": {
                "state": "start",
                "parameters": {"force": true}
            }
        });
        self.nvue_http_post("/nvue_v1/system", &payload, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let host_endpoint = self.require_host_endpoint()?;
        tracing::debug!(node = %self.id, host = %host_endpoint.ip_address, "get_firmware_inventory via NVFWUPD");

        let inventory = self
            .nvfwupd_workflow()?
            .get_firmware_inventory(self.nvfwupd_target_config()?)
            .await
            .map_err(nvfwupd_adapter::map_error)?;

        Ok(inventory
            .into_iter()
            .map(nvfwupd_adapter::to_firmware_info)
            .collect())
    }

    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        let host_endpoint = self.require_host_endpoint()?;
        tracing::info!(
            node = %self.id,
            host = %host_endpoint.ip_address,
            "verify_firmware_package_versions via NVFWUPD"
        );
        let targets = nvfwupd_adapter::to_version_check_targets(targets);
        let summary = self
            .nvfwupd_workflow()?
            .verify_firmware_target_versions(self.nvfwupd_target_config()?, targets)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_version_check_summary(summary))
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let host_endpoint = self.require_host_endpoint()?;

        tracing::info!(
            node = %self.id,
            host = %host_endpoint.ip_address,
            component = %target.component,
            file = %target.firmware_file,
            force_update,
            "update_firmware via NVFWUPD"
        );
        let request = nvfwupd_adapter::to_update_request(target, force_update, options);
        let outcome = self
            .nvfwupd_workflow()?
            .update_firmware(self.nvfwupd_target_config()?, request)
            .await
            .map_err(nvfwupd_adapter::map_error)?;

        Ok(nvfwupd_adapter::map_outcome(outcome))
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        tracing::debug!(node = %self.id, task_id, "poll_firmware_task via legacy NVUE poller");
        self.poll_nvue_action_task(task_id).await
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        tracing::info!(node = %self.id, "activate_firmware via NVFWUPD");
        let request = nvfwupd_adapter::to_activation_request(request);
        let summary = self
            .nvfwupd_workflow()?
            .activate_firmware(self.nvfwupd_target_config()?, request)
            .await
            .map_err(nvfwupd_adapter::map_error)?;

        Ok(nvfwupd_adapter::map_activation_summary(summary))
    }

    async fn activate_firmware(&self) -> Result<()> {
        self.activate_firmware_with(FirmwareActivationRequest {
            mode: FirmwareActivationMode::SwitchPowerCycle,
            cancellation: None,
        })
        .await
        .map(|_| ())
    }

    /// Returns true if an HTTP client for the BMC is configured.
    fn supports_bmc_aux_powercycle(&self) -> bool {
        self.power_http.is_some()
    }

    /// Power-cycles the switch host (NVOS) through the BMC using the Redfish
    /// `ComputerSystem.Reset` action.  This is used as a fallback when the
    /// switch host is unreachable via the NVOS API and a hard reset is needed
    /// to recover it.
    ///
    /// Requires that a BMC endpoint (`power_http`) is configured for this node.
    async fn bmc_aux_powercycle(&self) -> Result<()> {
        let power_http = self.power_http.as_ref().ok_or_else(|| {
            RmsError::failed_precondition(
                "BMC endpoint is required for aux powercycle on switch nodes",
            )
        })?;

        let payload = serde_json::json!({"ResetType": "PowerCycle"});
        let http = power_http.lock().await;
        let url = "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset";
        tracing::info!(node = %self.id, payload = ?payload, host = http.host(), url, "bmc_aux_powercycle via BMC Redfish");
        http.post(url, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
impl SwitchGb200Nvidia {
    /// Test-only constructor that creates a Switch instance with a mock HTTP client.
    /// Public so it can be used by other test modules (i.e. switch_image_handlers.rs).
    ///
    /// Note: `host_endpoint.endpoint.ip_address` is set to the full mock URL,
    /// so this constructor only supports testing HTTP-based methods. SSH/SFTP
    /// operations will fail.
    pub fn for_test(base_url: &str) -> Self {
        let host_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: base_url.to_owned(),
                mac_address: "00:00:00:00:00:00".to_owned(),
                port: 443,
                host_name: Some("test-host".to_owned()),
            },
            Some(EndpointCredentials::new("test-username", "test-password")),
            true,
        );

        let url = reqwest::Url::parse(base_url).expect("test NVUE URL");

        let config = NvueConnectConfig {
            endpoint: NvueEndpoint::http(
                url.host_str().expect("test NVUE host"),
                url.port_or_known_default().expect("test NVUE port"),
            ),
            credentials: NvueCredentials::new("test-username", "test-password"),
            dangerously_accept_invalid_certs: true,
        };

        let nvue = NvueClient::new(config).expect("test NVUE client");
        Self {
            id: "test-switch".to_owned(),
            rack_id: "test-rack".to_owned(),
            node_type: NodeType::SwitchGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::new(Endpoint {
                ip_address: "test-host".to_owned(),
                mac_address: "00:00:00:00:00:00".to_owned(),
                port: 443,
                host_name: None,
            })),
            host_endpoint: Some(host_endpoint),
            nvue: Some(nvue),
            power_http: None,
            #[cfg(test)]
            ssh_exec_for_test: None,
        }
    }

    /// Replace test host credentials and rebuild the embedded NVUE mock client.
    pub fn with_host_credentials_for_test(mut self, username: &str, password: &str) -> Self {
        let host_endpoint = self
            .host_endpoint
            .as_mut()
            .expect("test switch has host endpoint");

        host_endpoint.credentials = Some(EndpointCredentials::new(username, password));

        let url = reqwest::Url::parse(&host_endpoint.endpoint.ip_address).expect("test NVUE URL");

        let config = NvueConnectConfig {
            endpoint: NvueEndpoint::http(
                url.host_str().expect("test NVUE host"),
                url.port_or_known_default().expect("test NVUE port"),
            ),
            credentials: NvueCredentials::new(username, password),
            dangerously_accept_invalid_certs: true,
        };

        self.nvue = Some(NvueClient::new(config).expect("test NVUE client"));
        self
    }

    /// Clears the BMC endpoint so the node represents a switch that has no BMC
    /// endpoint configured. Use this in tests that exercise the "no BMC
    /// endpoint" code paths (e.g. `bmc_aux_powercycle` returning
    /// `FailedPrecondition`).
    pub fn without_bmc_endpoint_for_test(mut self) -> Self {
        self.bmc_endpoint = None;
        self.power_http = None;
        self
    }

    pub fn with_power_http_for_test(mut self, base_url: &str) -> Self {
        self.bmc_endpoint = Some(EndpointConfig::with_credentials(
            Endpoint {
                ip_address: base_url.to_owned(),
                mac_address: "00:00:00:00:00:00".to_owned(),
                port: 443,
                host_name: None,
            },
            Some(EndpointCredentials::new(
                "test-bmc-username",
                "test-bmc-password",
            )),
            true,
        ));
        self.power_http = Some(Arc::new(tokio::sync::Mutex::new(HttpClient::for_test(
            base_url,
        ))));
        self
    }

    pub fn with_ssh_exec_for_test(
        mut self,
        exec: impl Fn(&str) -> Result<String> + Send + Sync + 'static,
    ) -> Self {
        self.ssh_exec_for_test = Some(Arc::new(exec));
        self
    }
}

#[cfg(test)]
#[path = "switch_tests.rs"]
mod tests;
