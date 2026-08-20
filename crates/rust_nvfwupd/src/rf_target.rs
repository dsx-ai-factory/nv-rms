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

//! Abstract base module defining firmware update operations via Redfish protocol.
//!
//! The [`RFTarget`] trait provides default implementations for common Redfish
//! update workflows (task monitoring, version comparison, multipart upload,
//! push-URI upload) while requiring platform-specific implementations for
//! component update, version lookup, and chassis identification.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use regex::Regex;
use serde::Serialize;
use serde_json::{json, Value};

use crate::bmc_access::{BmcAccess, MultipartUploadOptions};
use crate::util::{BailAction, Util};
use crate::utils::Util as NvUtils;

/// Compact JSON with spaces after `:` and `,` — matches Python `json.dumps(obj)`.
fn json_dumps(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let entries: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: {}",
                        json_dumps(&Value::String(k.clone())),
                        json_dumps(v)
                    )
                })
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_dumps).collect();
            format!("[{}]", items.join(", "))
        }
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn json_pretty_4space(value: &Value) -> String {
    let buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(buf, formatter);
    value.serialize(&mut ser).unwrap_or(());
    String::from_utf8(ser.into_inner()).unwrap_or_default()
}

fn redacted_json_value(value: &Value) -> Value {
    NvUtils::redact_secret_json_value(value)
}

fn json_value_has_content(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
        _ => true,
    }
}

const COMPLETION_MESSAGE_SUPPRESS_TARGETS: &[&str] = &[
    "/redfish/v1/UpdateService/SoftwareInventory/IST_Vectors",
    "/redfish/v1/UpdateService/SoftwareInventory/HGX_IST_Vectors",
];

fn target_list_contains_completion_suppress_target(data: &Value) -> bool {
    let Some(targets) = data.get("Targets") else {
        return false;
    };

    let target_matches = |target: &str| {
        COMPLETION_MESSAGE_SUPPRESS_TARGETS
            .iter()
            .any(|uri| target.contains(uri))
    };

    match targets {
        Value::String(target) => target_matches(target),
        Value::Array(targets) => targets.iter().filter_map(Value::as_str).any(target_matches),
        _ => false,
    }
}

pub(crate) async fn should_suppress_update_completion_msg(
    updparams_json: Option<&Value>,
    upd_params_file: Option<&str>,
) -> bool {
    if let Some(data) = updparams_json {
        let parsed;
        let data = if let Some(json_str) = data.as_str() {
            match serde_json::from_str::<Value>(json_str) {
                Ok(value) => {
                    parsed = value;
                    &parsed
                }
                Err(_) => return false,
            }
        } else {
            data
        };
        return target_list_contains_completion_suppress_target(data);
    }

    let Some(params_file) = upd_params_file else {
        return false;
    };
    let Ok(contents) = tokio::fs::read_to_string(params_file).await else {
        return false;
    };
    let Ok(data) = serde_json::from_str::<Value>(&contents) else {
        return false;
    };

    target_list_contains_completion_suppress_target(&data)
}

fn push_json_failure(json_dict: Option<&mut Value>, message: &str, details: Option<&Value>) {
    let Some(json_dict) = json_dict else {
        return;
    };

    let message = NvUtils::sanitize_log(message);
    json_dict["Error Code"] = json!(1);

    if !json_dict.get("Error").is_some_and(Value::is_array) {
        json_dict["Error"] = json!([]);
    }
    if !message.is_empty() {
        if let Some(errors) = json_dict.get_mut("Error").and_then(Value::as_array_mut) {
            if !errors.iter().any(|value| value.as_str() == Some(&message)) {
                errors.push(Value::String(message));
            }
        }
    }

    if let Some(details) = details {
        let redacted = redacted_json_value(details);
        if json_value_has_content(&redacted) {
            if !json_dict.get("Output").is_some_and(Value::is_array) {
                json_dict["Output"] = json!([]);
            }
            if let Some(output) = json_dict.get_mut("Output").and_then(Value::as_array_mut) {
                output.push(redacted);
            }
        }
    }
}

fn json_dumps_redacted(value: &Value) -> String {
    json_dumps(&redacted_json_value(value))
}

fn json_pretty_4space_redacted(value: &Value) -> String {
    json_pretty_4space(&redacted_json_value(value))
}

fn should_emit_console(quiet: bool, json_dict: Option<&Value>) -> bool {
    !quiet && json_dict.is_none()
}

/// Treat placeholder firmware versions as unknown, not as comparable versions.
fn firmware_version_is_sentinel(version: &str) -> bool {
    matches!(
        version.trim().to_ascii_lowercase().as_str(),
        "unknown" | "n/a"
    )
}

/// Extract a task id only from real Redfish TaskService paths.
///
/// MessageArgs can contain many Redfish paths, so this deliberately rejects
/// non-task inventory URIs instead of returning the last path segment blindly.
fn task_id_from_redfish_path(path: &str) -> Option<String> {
    let trimmed = path.trim().trim_end_matches('/');
    let mut segments = trimmed.rsplit('/');
    let task_id = segments.next()?.trim();
    let parent = segments.next()?.trim();

    if !parent.eq_ignore_ascii_case("tasks") || task_id.is_empty() {
        return None;
    }

    Some(task_id.to_string())
}

/// Extract the task id returned by a Redfish update response.
///
/// Update services vary between `Id`, `@odata.id`, and TaskService references in
/// message args. Only real task references are accepted from path-shaped values.
pub(crate) fn task_id_from_update_response(response_dict: &Value) -> Option<String> {
    if let Some(task_id) = response_dict
        .get("Id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        return Some(task_id.to_string());
    }

    if let Some(task_id) = response_dict
        .get("@odata.id")
        .and_then(Value::as_str)
        .and_then(task_id_from_redfish_path)
    {
        return Some(task_id);
    }

    response_dict
        .get("Messages")
        .and_then(Value::as_array)
        .and_then(|messages| {
            messages.iter().find_map(|message| {
                message
                    .get("MessageArgs")
                    .and_then(Value::as_array)
                    .and_then(|args| {
                        args.iter()
                            .filter_map(Value::as_str)
                            .find_map(task_id_from_redfish_path)
                    })
            })
        })
}

/// Normalize Redfish status/severity strings into failure detection.
fn task_status_is_failure(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "critical" | "error" | "failed" | "failure" | "exception"
    )
}

/// Detect terminal failure message ids while avoiding explicit no-error ids.
///
/// Redfish message ids are dotted identifiers; checking the terminal segment
/// avoids false positives from registry names while still catching vendor ids.
fn message_id_has_failure_marker(message_id: &str) -> bool {
    let normalized = message_id.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.contains("noerror")
        || normalized.contains("no_error")
        || normalized.contains("nofailure")
        || normalized.contains("no_failure")
    {
        return false;
    }

    let terminal = normalized.rsplit('.').next().unwrap_or(&normalized);
    [
        "failed",
        "failure",
        "error",
        "exception",
        "aborted",
        "violation",
    ]
    .iter()
    .any(|marker| terminal.contains(marker))
}

/// Return true when a Redfish message carries structured failure information.
///
/// This intentionally uses severity and message id, not free-form message text,
/// to avoid treating successful diagnostic text as a failure.
fn message_has_structured_failure(message: &Value) -> bool {
    let severity = message
        .get("Severity")
        .or_else(|| message.get("MessageSeverity"))
        .and_then(Value::as_str)
        .unwrap_or_default();

    task_status_is_failure(severity)
        || message
            .get("MessageId")
            .and_then(Value::as_str)
            .map(message_id_has_failure_marker)
            .unwrap_or(false)
}

/// Return true when a Redfish task is terminally failed by structured fields.
///
/// TaskState is checked first because some BMCs report `TaskStatus: OK` even
/// when the terminal state is `Cancelled` or `Exception`.
fn task_has_structured_failure(task_dict: &Value) -> bool {
    let task_state = task_dict
        .get("TaskState")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if TASK_FAILURE_STATES.contains(&task_state.as_str()) {
        return true;
    }

    let task_status = task_dict
        .get("TaskStatus")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if task_status_is_failure(task_status) {
        return true;
    }

    task_dict
        .get("Messages")
        .and_then(Value::as_array)
        .map(|messages| messages.iter().any(message_has_structured_failure))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Static dictionaries
// ---------------------------------------------------------------------------

/// Maps server-type CLI argument strings to their corresponding RFTarget
/// implementation class name.
pub(crate) static SERVER_TYPE_CLASS_DICT: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("gh", "GHRFTarget");
        m.insert("mgx", "GHRFTarget");
        m.insert("gh200", "GH200RFTarget");
        m.insert("dgx", "DGX_RFTarget");
        m.insert("dgxrubin", "DGX_RFTarget");
        m.insert("hgx", "GHRFTarget");
        m.insert("hgxb100", "HGXB100RFTarget");
        m.insert("hgxb300", "HGXB100RFTarget");
        m.insert("hgxrubin", "HGXRUBINRFTarget");
        m.insert("gb200", "GB200RFTarget");
        m.insert("gb300", "GB200RFTarget");
        m.insert("gb200switch", "GB200SwitchRFTarget");
        m.insert("gb300switch", "GB200SwitchRFTarget");
        m.insert("vrnvl72switch", "GB200SwitchRFTarget");
        m.insert("vrnvl72", "GB200RFTarget");
        m.insert("powershelf", "PowerShelfRFTarget");
        m
    });

/// Maps chassis model identification strings (lowercase) to their
/// corresponding RFTarget implementation class name.
pub(crate) static TARGET_CLASS_DICT: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("hgx", "GHRFTarget");
        m.insert("dgx", "DGX_RFTarget");
        m.insert("dgxrubin", "DGX_RFTarget");
        m.insert("dgx rubin", "DGX_RFTarget");
        m.insert("gb200 nvl", "GB200RFTarget");
        m.insert("gb hmc", "GB200RFTarget");
        m.insert("gb300 nvl", "GB200RFTarget");
        m.insert("gb300 station bmc", "GB200RFTarget");
        m.insert("gb300 ws", "GB200RFTarget");
        m.insert("vr nvl", "GB200RFTarget");
        m.insert("vr bmc", "GB200RFTarget");
        m.insert("vr hmc", "GB200RFTarget");
        m.insert("vr nvl72", "GB200RFTarget");
        m.insert("vera mgx", "GB200RFTarget");
        m.insert("vera mgx c1", "GB200RFTarget");
        m.insert("vera mgx c2", "GB200RFTarget");
        m.insert("n5110_ld", "GB200SwitchRFTarget");
        m.insert("n5400_ld", "GB200SwitchRFTarget");
        m.insert("n5500_ld", "GB200SwitchRFTarget");
        m.insert("n6100_ld", "GB200SwitchRFTarget");
        m.insert("n6150_ld", "GB200SwitchRFTarget");
        m.insert("n6300_ld", "GB200SwitchRFTarget");
        m.insert("p2312-c00", "HGXRUBINRFTarget");
        m.insert("pf-1333-13rd", "PowerShelfRFTarget");
        m.insert("pf-1333-7r", "PowerShelfRFTarget");
        m.insert("pf-1114-1r", "PowerShelfRFTarget");
        m.insert("ahe50v2200a", "PowerShelfRFTarget");
        m.insert("ahe50v660a33kwl", "PowerShelfRFTarget");
        m.insert("ahe54v2037a", "PowerShelfRFTarget");
        m.insert("810", "PowerShelfRFTarget");
        m.insert("mpsc6000", "PowerShelfRFTarget");
        m.insert("mpsc6000-mcu", "PowerShelfRFTarget");
        m.insert("megmeet", "PowerShelfRFTarget");
        m.insert("nvd-p", "PowerShelfRFTarget");
        m
    });

/// Maps BMC type configuration strings to their corresponding RFTarget
/// implementation class name.
pub(crate) static TARGET_TYPE_CONFIG_DICT: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("ami", "DGX_RFTarget");
        m.insert("openbmc", "GHRFTarget");
        m
    });

// ---------------------------------------------------------------------------
// Task state constants
// ---------------------------------------------------------------------------

/// Task states that indicate the update has failed or been stopped.
pub const TASK_FAILURE_STATES: &[&str] = &[
    "cancelled",
    "cancelling",
    "exception",
    "interrupted",
    "killed",
    "stopping",
    "suspended",
];

/// Task states that indicate the update is still in progress.
pub const TASK_PENDING_STATES: &[&str] = &["new", "pending", "running", "service", "starting"];

// ---------------------------------------------------------------------------
// Command args placeholder (referenced by trait methods)
// ---------------------------------------------------------------------------

/// Command-line arguments passed through to update operations.
#[derive(Clone)]
pub struct CmdArgs {
    pub cmd: String,
    pub background: bool,
    pub details: bool,
    pub staged_update: bool,
    pub staged_activate_update: bool,
    /// Suppress direct stdout/stderr writes when NVFWUPD is hosted as a library.
    pub quiet: bool,
    /// `-s/--special` — JSON update parameters (inline JSON or file paths).
    pub special: Option<Vec<String>>,
    /// `-o/--oem_parameters` — OEM parameter JSON (inline JSON or file paths).
    pub oem_parameters: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// PkgParser placeholder
// ---------------------------------------------------------------------------

/// Placeholder trait for the package parser used by `start_update_monitor`.
#[async_trait::async_trait]
pub trait PkgParser: Send + Sync {
    /// Parse a firmware package file and return success status with an
    /// optional error message.
    async fn parse_pkg(&mut self, pkg_path: &str) -> (bool, String);

    /// Unpack the package and populate the internal file-to-AP mapping.
    async fn get_unpack_file_dict(&mut self, _pkg_path: &str) {}

    /// Return the AP-name to [version, file_path] mapping from the last unpack.
    fn unpack_file_ap_dict(&self) -> &std::collections::HashMap<String, Vec<String>> {
        static EMPTY: std::sync::OnceLock<std::collections::HashMap<String, Vec<String>>> =
            std::sync::OnceLock::new();
        EMPTY.get_or_init(std::collections::HashMap::new)
    }

    /// Return the AP-name to version mapping from the parsed package.
    fn apname_version_dict_json(&self) -> Value {
        Value::Object(serde_json::Map::new())
    }

    /// Return the raw PLDM dict for inspecting package metadata
    /// (e.g. DeviceDescriptors / UUIDs). Empty for non-PLDM packages.
    fn pldm_raw_dict(&self) -> Value {
        Value::Object(serde_json::Map::new())
    }
}

// ---------------------------------------------------------------------------
// RFTarget trait
// ---------------------------------------------------------------------------

/// How firmware update precondition checks should be evaluated.
#[derive(Debug, Clone, Copy)]
pub enum UpdatePreconditionMode {
    /// Evaluate preconditions once and fail immediately if they are not ready.
    SingleShot,
    /// Poll preconditions until they are ready or the timeout expires.
    Wait {
        /// Total time allowed for the precondition to become ready.
        timeout: Duration,
        /// Delay between readiness checks.
        interval: Duration,
    },
}

/// Base trait defining firmware update operations via Redfish protocol.
///
/// Platform-specific implementations (GHRFTarget, DGX_RFTarget, etc.) must
/// implement the required abstract methods. Default implementations are
/// provided for common Redfish workflows such as task monitoring, version
/// comparison, and firmware upload.
#[async_trait::async_trait]
pub trait RFTarget: Send + Sync {
    // ------------------------------------------------------------------
    // Accessors for the underlying BMC connection
    // ------------------------------------------------------------------

    /// Return a shared reference to the BMC access layer.
    fn target_access(&self) -> &BmcAccess;

    /// Return a mutable reference to the BMC access layer.
    fn target_access_mut(&mut self) -> &mut BmcAccess;

    /// Retrieve firmware inventory through this target's management transport.
    async fn get_firmware_inventory(
        &self,
        trace: crate::util::TraceFlags,
        json_output: Option<&mut Value>,
        model: Option<&str>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        self.target_access()
            .get_firmware_inventory(trace, json_output, model)
            .await
    }

    /// Retrieve only AP names required for expected-inventory validation.
    async fn get_expected_inventory_ap_names(
        &self,
        trace: crate::util::TraceFlags,
        json_output: Option<&mut Value>,
    ) -> (bool, i32, Vec<String>) {
        self.target_access()
            .get_expected_inventory_ap_names(trace, json_output)
            .await
    }

    // ------------------------------------------------------------------
    // Mutable state accessors
    // ------------------------------------------------------------------

    /// Return a reference to the list of fungible component names.
    fn fungible_components(&self) -> &[String];

    /// Return the human-readable update completion message.
    fn update_completion_msg(&self) -> &str;

    /// Set the update completion message.
    fn set_update_completion_msg(&mut self, msg: &str);

    /// Whether the update completion guidance should be suppressed for the
    /// current monitoring session.
    fn update_completion_msg_suppressed(&self) -> bool {
        false
    }

    /// Set whether the update completion guidance should be suppressed.
    fn set_update_completion_msg_suppressed(&mut self, _suppressed: bool) {}

    /// Whether the progress table header has been printed for the
    /// current monitoring session.
    fn progress_table_header_printed(&self) -> bool;

    /// Set whether the progress table header has been printed.
    fn set_progress_table_header_printed(&mut self, printed: bool);

    /// Return the optional config dictionary associated with this target.
    fn config_dict(&self) -> Option<&Value>;

    /// Return the class name of this target (used for dynamic dispatch
    /// identification, e.g. "PowerShelfRFTarget").
    fn class_name(&self) -> &str;

    /// Validate target-specific conditions that must be ready before update.
    async fn check_update_preconditions(
        &self,
        _mode: UpdatePreconditionMode,
        _json_dict: Option<&mut Value>,
    ) -> Result<(), String> {
        Ok(())
    }

    // ------------------------------------------------------------------
    // Abstract methods -- must be implemented by each platform target
    // ------------------------------------------------------------------

    /// Perform a firmware update on the target component.
    ///
    /// Returns the task ID string on success, or `None` on failure.
    async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String>;

    /// Check whether the given component name is a fungible component on
    /// this target.
    fn is_fungible_component(&self, component_name: &str) -> bool;

    /// Get the version of an application processor from a PLDM version
    /// dictionary.
    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String>;

    /// Get an AP identifier from a Chassis Redfish response URI.
    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String>;

    /// Get the version from a PLDM dict for a given identifier and AP name.
    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> Option<String>;

    // ------------------------------------------------------------------
    // Default implementations
    // ------------------------------------------------------------------

    /// Dispatch a Redfish request with automatic retry on failure.
    ///
    /// Retries up to `max_retries` times with `interval` seconds between
    /// attempts. Returns `(success, response_body)`.
    async fn dispatch_request_with_retry(
        &self,
        method: &str,
        uri: &str,
        data: Option<&Value>,
        mut json_prints: Option<&mut Value>,
        max_retries: u32,
        interval: u64,
    ) -> (bool, Value) {
        let mut status = false;
        let mut task_dict = json!({});

        for attempt in 0..max_retries {
            let (s, d) = self
                .target_access()
                .dispatch_request(method, uri, data, json_prints.as_deref_mut())
                .await;
            status = s;
            task_dict = d;

            if status {
                return (status, task_dict);
            }

            if attempt < max_retries - 1 {
                if json_prints.is_none() {
                    tracing::debug!("Retrying Task Status Request: {}", uri);
                }
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        }

        (status, task_dict)
    }

    /// Check whether `input` is a valid JSON string.
    ///
    /// Mirrors Python `RFTarget.validate_json`: returns `true` when
    /// `serde_json::from_str` succeeds, `false` otherwise.
    fn validate_json(&self, input: &str) -> bool {
        serde_json::from_str::<Value>(input).is_ok()
    }

    /// Resolve `special` / `oem_parameters` from `CmdArgs` into their
    /// effective string form.  The value is either inline JSON (returned
    /// directly) or a file path (contents are read and returned).
    /// Returns `Ok(None)` when the field is absent, `Ok(Some(json_str))`
    /// on success, or `Err(message)` on I/O / validation failure.
    async fn resolve_json_or_file(
        &self,
        values: Option<&[String]>,
        label: &str,
    ) -> Result<Option<String>, String> {
        let vals = match values {
            Some(v) if !v.is_empty() => v,
            _ => return Ok(None),
        };
        let first = &vals[0];

        if self.validate_json(first) {
            return Ok(Some(first.clone()));
        }

        // Treat as a file path list — try opening the first entry.
        let contents = tokio::fs::read_to_string(first).await.map_err(|e| {
            format!(
                "Failed to open or read given {} file {} error: ({})",
                label, first, e
            )
        })?;

        if !self.validate_json(&contents) {
            return Err(format!("Invalid JSON format in {} file {}", label, first));
        }

        Ok(Some(contents))
    }

    /// Perform a factory reset on the BMC via Redfish.
    ///
    /// Queries `/redfish/v1/Managers` to discover the BMC ID, then POSTs
    /// the `Manager.ResetToDefaults` action.
    async fn factory_reset(&mut self, reset_params: Option<&Value>) -> (bool, Value) {
        let (status, response_dict) = self
            .target_access()
            .dispatch_request("GET", "/redfish/v1/Managers", None, None)
            .await;

        let mut bmc_id = "BMC".to_string();
        if status {
            if let Some(members) = response_dict.get("Members").and_then(|m| m.as_array()) {
                if let Some(first) = members.first() {
                    if let Some(uri) = first.get("@odata.id").and_then(|u| u.as_str()) {
                        if let Some(id) = uri.rsplit('/').next() {
                            bmc_id = id.to_string();
                        }
                    }
                }
            } else {
                Util::bail_nvfwupd(
                    1,
                    "Error: perform_factory_reset could not find any Managers",
                    BailAction::Exit,
                    None,
                );
            }
        }

        let reset_uri = format!(
            "/redfish/v1/Managers/{}/Actions/Manager.ResetToDefaults",
            bmc_id
        );
        let (status, response_dict) = self
            .target_access()
            .dispatch_request_full("POST", &reset_uri, None, reset_params, 30, false, None)
            .await;

        if !status {
            Util::bail_nvfwupd(
                1,
                &format!("perform_factory_reset status: {}", status),
                BailAction::Exit,
                None,
            );
        }

        (status, response_dict)
    }

    /// Send a background copy Redfish request.
    ///
    /// Reads copy parameters from a JSON file, queries the UpdateService
    /// for the CommitImage action URI, then POSTs the request.
    async fn background_copy(&self, copy_parameters: &str) -> (bool, Value) {
        // Read and parse the JSON parameters file
        let json_params: Value = match tokio::fs::read_to_string(copy_parameters).await {
            Ok(contents) => match serde_json::from_str(&contents) {
                Ok(v) => v,
                Err(_) => {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Error: {} is not valid JSON", copy_parameters),
                        BailAction::Exit,
                        None,
                    );
                    return (false, json!({}));
                }
            },
            Err(_) => {
                Util::bail_nvfwupd(
                    1,
                    &format!("Error: {} is not a valid file", copy_parameters),
                    BailAction::Exit,
                    None,
                );
                return (false, json!({}));
            }
        };

        // Get UpdateService response
        let (status, response_dict) = self
            .target_access()
            .dispatch_request("GET", "/redfish/v1/UpdateService", None, None)
            .await;

        let mut target_uri: Option<String> = None;

        if status {
            // Obtain platform target URI for the background copy
            if let Some(uri) = response_dict
                .pointer("/Actions/Oem/#NvidiaUpdateService.CommitImage/target")
                .and_then(|v| v.as_str())
            {
                target_uri = Some(uri.to_string());
            } else {
                Util::bail_nvfwupd(
                    1,
                    "Error: background copy not supported for this system",
                    BailAction::Exit,
                    None,
                );
            }
        } else {
            Util::bail_nvfwupd(
                1,
                &format!("Redfish Update Service inaccessible: {}", status),
                BailAction::Exit,
                None,
            );
        }

        if let Some(ref uri) = target_uri {
            let (post_status, post_response) = self
                .target_access()
                .dispatch_request("POST", uri, Some(&json_params), None)
                .await;

            if !post_status {
                Util::bail_nvfwupd(
                    1,
                    &format!("background_copy status: {}", post_status),
                    BailAction::Exit,
                    None,
                );
            }
            return (post_status, post_response);
        }

        (false, json!({}))
    }

    /// Compare two version strings, returning `true` if `pkg_version` is
    /// newer than `sys_version`.
    ///
    /// Splits on `.` and `-` delimiters and compares each segment
    /// lexicographically after zero-padding to equal width.
    fn version_newer(&self, pkg_version: &str, sys_version: &str) -> bool {
        let re = Regex::new(r"[.|\-]").unwrap();
        let pkg_arr: Vec<&str> = re.split(pkg_version).collect();
        let sys_arr: Vec<&str> = re.split(sys_version).collect();

        if pkg_arr.len() != sys_arr.len() {
            return true;
        }

        for (pkg_seg, sys_seg) in pkg_arr.iter().zip(sys_arr.iter()) {
            let pkg_trimmed = pkg_seg.trim();
            let sys_trimmed = sys_seg.trim();
            let max_len = pkg_trimmed.len().max(sys_trimmed.len());
            let pkg_padded = format!("{:0>width$}", pkg_trimmed, width = max_len);
            let sys_padded = format!("{:0>width$}", sys_trimmed, width = max_len);

            if pkg_padded != sys_padded {
                return pkg_padded > sys_padded;
            }
        }

        false
    }

    /// Return whether a package version should be displayed as up-to-date
    /// against a system version in `show_version`.
    fn firmware_version_up_to_date(&self, pkg_version: &str, sys_version: &str) -> bool {
        if firmware_version_is_sentinel(pkg_version) || firmware_version_is_sentinel(sys_version) {
            return false;
        }

        !self.version_newer(
            &pkg_version.trim().to_ascii_lowercase(),
            &sys_version.trim().to_ascii_lowercase(),
        )
    }

    /// Query the status of a Redfish task without printing.
    ///
    /// Returns `(success, task_response_body)`.
    async fn query_job_status(
        &self,
        task_id: &str,
        print_json: Option<&mut Value>,
    ) -> (bool, Value) {
        let task_service_uri = self.get_task_service_uri(task_id);
        self.target_access()
            .dispatch_request("GET", &task_service_uri, None, print_json)
            .await
    }

    /// Print the status of a Redfish task and return `(error_code, task_state)`.
    ///
    /// `error_code` is 0 on success, 1 on failure.
    fn print_job_status(
        &self,
        task_id: &str,
        my_dict: &Value,
        status: bool,
        print_json: Option<&mut Value>,
    ) -> (i32, Option<String>) {
        let task_state = my_dict
            .get("TaskState")
            .and_then(|v| v.as_str())
            .map(|s| s.to_lowercase());

        if let Some(json_dict) = print_json {
            // JSON output mode: append task dict to Output array
            if let Some(output) = json_dict.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(redacted_json_value(my_dict));
            }
            if !status {
                return (1, task_state);
            }
            return (0, task_state);
        }

        // Human-readable output
        tracing::info!(indent = 2, "{}", "-".repeat(120));
        tracing::info!(indent = 2, "Task Info for Id: {}", task_id);

        let mut err_status = 0;

        if my_dict.get("error").is_some() {
            tracing::debug!("{}", redacted_json_value(my_dict));
            tracing::info!(indent = 2, "Input Taskid does not exist: {} ", task_id);
            return (1, task_state);
        }

        if !status {
            tracing::debug!("{}", redacted_json_value(my_dict));
            tracing::info!(indent = 1, "{} ", redacted_json_value(my_dict));
            return (1, task_state);
        }

        tracing::info!(
            indent = 1,
            "StartTime: {}",
            NvUtils::sanitize_log(
                my_dict
                    .get("StartTime")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            )
        );
        tracing::info!(
            indent = 1,
            "TaskState: {}",
            NvUtils::sanitize_log(
                my_dict
                    .get("TaskState")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            )
        );

        if let Some(pct) = my_dict.get("PercentComplete") {
            tracing::info!(indent = 1, "PercentComplete: {}", pct);
        }

        tracing::info!(
            indent = 1,
            "TaskStatus: {}",
            NvUtils::sanitize_log(
                my_dict
                    .get("TaskStatus")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown")
            )
        );

        if let Some(end_time) = my_dict.get("EndTime").and_then(|v| v.as_str()) {
            tracing::info!(indent = 1, "EndTime: {}", NvUtils::sanitize_log(end_time));
        }

        // Calculate overall time if task is completed or failed
        if let Some(ref state) = task_state {
            if TASK_FAILURE_STATES.contains(&state.as_str()) || state == "completed" {
                if let (Some(start_str), Some(end_str)) = (
                    my_dict.get("StartTime").and_then(|v| v.as_str()),
                    my_dict.get("EndTime").and_then(|v| v.as_str()),
                ) {
                    if let (Some(start_ts), Some(end_ts)) =
                        (get_timestamp(start_str), get_timestamp(end_str))
                    {
                        let duration = end_ts.signed_duration_since(start_ts);
                        tracing::info!(
                            indent = 1,
                            "Overall Time Taken: {}",
                            format_duration_hms(&duration)
                        );
                    }
                }
            }
        }

        tracing::info!(
            indent = 1,
            "Overall Task Status: {}",
            json_pretty_4space_redacted(my_dict)
        );

        if let Some(ref state) = task_state {
            if TASK_PENDING_STATES.contains(&state.as_str()) {
                tracing::info!(indent = 2, "Update is still running.");
                err_status = 0;
            } else if state == "completed" {
                let task_status = my_dict
                    .get("TaskStatus")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown");
                if task_status == "OK" {
                    tracing::info!(indent = 2, "Update is successful.");
                    err_status = 0;
                } else {
                    tracing::info!(
                        indent = 2,
                        "Update completed with TaskStatus {}",
                        NvUtils::sanitize_log(task_status)
                    );
                    err_status = 1;
                }
            } else {
                tracing::info!(indent = 2, "Update failed with errors");
                err_status = 1;
            }
        }

        (err_status, task_state)
    }

    /// Parse and print the full task status for a given task ID.
    ///
    /// Returns 0 on success, 1 on failure.
    async fn process_job_status(&self, task_id: &str, mut print_json: Option<&mut Value>) -> i32 {
        if print_json.is_none() {
            tracing::info!(indent = 2, "Task Info for Id: {}", task_id);
        }

        let task_service_uri = self.get_task_service_uri(task_id);
        let (status, my_dict) = self
            .target_access()
            .dispatch_request("GET", &task_service_uri, None, print_json.as_deref_mut())
            .await;

        if let Some(ref mut json_dict) = print_json {
            if let Some(output) = json_dict.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(redacted_json_value(&my_dict));
            }
            if !status {
                return 1;
            }
            return 0;
        }

        if my_dict.get("error").is_some() {
            tracing::debug!("{}", redacted_json_value(&my_dict));
            tracing::info!(indent = 2, "Input Taskid does not exist: {} ", task_id);
            return 1;
        }

        if !status {
            tracing::debug!("{}", redacted_json_value(&my_dict));
            tracing::info!(indent = 1, "{} ", redacted_json_value(&my_dict));
            return 1;
        }

        let start_time_str = my_dict
            .get("StartTime")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let task_state_str = my_dict
            .get("TaskState")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let task_status_str = my_dict
            .get("TaskStatus")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        tracing::info!(
            indent = 1,
            "StartTime: {}",
            NvUtils::sanitize_log(start_time_str)
        );
        tracing::info!(
            indent = 1,
            "TaskState: {}",
            NvUtils::sanitize_log(task_state_str)
        );

        if let Some(pct) = my_dict.get("PercentComplete") {
            tracing::info!(indent = 1, "PercentComplete: {}", pct);
        }

        tracing::info!(
            indent = 1,
            "TaskStatus: {}",
            NvUtils::sanitize_log(task_status_str)
        );

        if let Some(end_time) = my_dict.get("EndTime").and_then(|v| v.as_str()) {
            tracing::info!(indent = 1, "EndTime: {}", NvUtils::sanitize_log(end_time));
        }

        let task_state_lower = task_state_str.to_lowercase();

        // Calculate overall time if task is completed or failed
        if TASK_FAILURE_STATES.contains(&task_state_lower.as_str())
            || task_state_lower == "completed"
        {
            if let (Some(start_str), Some(end_str)) = (
                my_dict.get("StartTime").and_then(|v| v.as_str()),
                my_dict.get("EndTime").and_then(|v| v.as_str()),
            ) {
                if let (Some(start_ts), Some(end_ts)) =
                    (get_timestamp(start_str), get_timestamp(end_str))
                {
                    let dur = end_ts.signed_duration_since(start_ts);
                    tracing::info!(
                        indent = 1,
                        "Overall Time Taken: {}",
                        format_duration_hms(&dur)
                    );
                }
            }
        }

        tracing::info!(
            indent = 1,
            "Overall Task Status: {}",
            json_pretty_4space_redacted(&my_dict)
        );

        let mut err_status = 0;
        if TASK_PENDING_STATES.contains(&task_state_lower.as_str()) {
            tracing::info!(indent = 2, "Update is still running.");
        } else if task_state_str == "Completed" {
            let task_status = my_dict
                .get("TaskStatus")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");
            if task_status == "OK" {
                tracing::info!(indent = 2, "Update is successful.");
            } else {
                tracing::info!(
                    indent = 2,
                    "Update completed with TaskStatus {}",
                    NvUtils::sanitize_log(task_status)
                );
                err_status = 1;
            }
        } else {
            tracing::info!(indent = 2, "Update failed with errors");
            err_status = 1;
        }

        err_status
    }

    // get_timestamp moved to standalone function below the trait

    /// Check update task status and structured message metadata for failure indicators.
    ///
    /// Returns `true` for explicit terminal failure states/statuses or
    /// structured message metadata such as critical severity or failure
    /// MessageIds. Raw message prose is intentionally ignored.
    fn check_for_failure(&self, task_dict: &Value) -> bool {
        task_has_structured_failure(task_dict)
    }

    /// Print the task completion summary with overall time taken.
    fn print_task_completion(&self, task_dict: &Value) {
        if let (Some(start_str), Some(end_str)) = (
            task_dict.get("StartTime").and_then(|v| v.as_str()),
            task_dict.get("EndTime").and_then(|v| v.as_str()),
        ) {
            if let (Some(start_ts), Some(end_ts)) =
                (get_timestamp(start_str), get_timestamp(end_str))
            {
                let duration = end_ts.signed_duration_since(start_ts);
                tracing::info!(
                    indent = 0,
                    "Overall Time Taken: {}",
                    format_duration_hms(&duration)
                );
                let update_completion_msg = self.update_completion_msg();
                if !update_completion_msg.is_empty() && !self.update_completion_msg_suppressed() {
                    tracing::info!(indent = 0, "{}", update_completion_msg);
                }
            }
        }
    }

    /// Get the Redfish UpdateService URI.
    ///
    /// The `_update_service_response` argument is accepted for
    /// compatibility with overrides in platform subclasses.
    fn get_update_uri(&self, _update_service_response: &Value) -> String {
        "/redfish/v1/UpdateService".to_string()
    }

    /// Generate update target JSON files from the firmware inventory.
    ///
    /// Creates one JSON file per firmware component in `dir_path`, each
    /// containing `{"HttpPushUriTargets": [<inventory_uri>]}` or
    /// platform-specific variants.  Returns `true` on success.
    ///
    /// Default implementation returns `false` (not supported).
    async fn make_update_target_json(&self, _dir_path: &str) -> bool {
        Util::bail_nvfwupd(
            1,
            "make_upd_targets is not supported for this platform",
            BailAction::DoNothing,
            None,
        );
        false
    }

    /// Build the task monitoring URI for a given task ID.
    fn get_task_service_uri(&self, task_id: &str) -> String {
        let raw = format!("/redfish/v1/TaskService/Tasks/{}", task_id);
        // Collapse duplicate slashes
        let re = Regex::new(r"/+").unwrap();
        re.replace_all(&raw, "/").to_string()
    }

    /// Determine the expected task type (BMC or HMC) from the package
    /// recipe list.
    ///
    /// Returns `"UNKNOWN"` by default; platform-specific implementations
    /// override this to return `"BMC"` or `"HMC"`.
    fn get_expected_task_type_from_package(&self, _recipe_list: &[String]) -> String {
        "UNKNOWN".to_string()
    }

    /// Check if an update task of the same type (BMC or HMC) is already
    /// running on the target.
    ///
    /// Returns `(is_running, task_id, error_message)`.
    async fn check_for_running_update_task_by_type(
        &self,
        recipe_list: &[String],
        mut json_dict: Option<&mut Value>,
    ) -> (bool, Option<String>, Option<String>) {
        let (status, tasks_response) = self
            .target_access()
            .dispatch_request(
                "GET",
                "/redfish/v1/TaskService/Tasks",
                None,
                json_dict.as_deref_mut(),
            )
            .await;

        if !status || tasks_response.get("Members").is_none() {
            // If we cannot get tasks, do not block the update
            return (false, None, None);
        }

        let members = tasks_response
            .get("Members")
            .and_then(|m| m.as_array())
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        let expected_type = self.get_expected_task_type_from_package(recipe_list);

        for member in members {
            let task_uri = member
                .get("@odata.id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if task_uri.is_empty() {
                continue;
            }

            // Extract task ID from URI (e.g., /redfish/v1/TaskService/Tasks/0 -> 0)
            let task_id = task_uri.rsplit('/').next().unwrap_or("");

            // Determine task type from ID
            // BMC tasks: numerical (0, 1, 2, etc.)
            // HMC tasks: HGX_0, HGX_1, HGX_2, etc.
            let task_type = if task_id.starts_with("HGX_") {
                "HMC"
            } else {
                "BMC"
            };

            // If expected_type is UNKNOWN, do not block the update.
            // If types do not match and we know the expected type, skip.
            if task_type != expected_type {
                continue;
            }

            // Query the specific task
            let (task_status, task_dict) = self
                .target_access()
                .dispatch_request("GET", task_uri, None, json_dict.as_deref_mut())
                .await;

            if !task_status {
                continue;
            }

            // Check if this task is in a running/pending state
            let task_state = task_dict
                .get("TaskState")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();

            if TASK_PENDING_STATES.contains(&task_state.as_str()) {
                let task_type_str = if task_type == "HMC" { "HMC" } else { "BMC" };
                let error_msg = format!(
                    "An update task (ID: {}, Type: {}) is already running. \
                     Please wait for it to complete before starting a new update.",
                    task_id, task_type_str
                );
                return (true, Some(task_id.to_string()), Some(error_msg));
            }
        }

        (false, None, None)
    }

    /// Perform a firmware update using Redfish multipart HTTP upload.
    ///
    /// Returns the task ID on success, or `None` on failure.
    #[allow(clippy::too_many_arguments)]
    async fn update_component_multipart(
        &mut self,
        param_list: Option<&[String]>,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        param_json: Option<&Value>,
        oem_param_list: Option<&[String]>,
        oem_param_json: Option<&Value>,
        mut json_dict: Option<&mut Value>,
        parallel_update: bool,
        quiet: bool,
    ) -> Option<String> {
        self.update_component_multipart_with_options(
            param_list,
            update_uri,
            update_file,
            time_out,
            param_json,
            oem_param_list,
            oem_param_json,
            json_dict,
            parallel_update,
            quiet,
            MultipartUploadOptions::default(),
        )
        .await
    }

    /// Perform a firmware update using Redfish multipart HTTP upload with
    /// explicit upload behavior.
    ///
    /// Returns the task ID on success, or `None` on failure.
    #[allow(clippy::too_many_arguments)]
    async fn update_component_multipart_with_options(
        &mut self,
        param_list: Option<&[String]>,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        param_json: Option<&Value>,
        oem_param_list: Option<&[String]>,
        oem_param_json: Option<&Value>,
        mut json_dict: Option<&mut Value>,
        parallel_update: bool,
        quiet: bool,
        upload_options: MultipartUploadOptions,
    ) -> Option<String> {
        let emit_console = should_emit_console(quiet, json_dict.as_deref());
        let upd_params_file = param_list.and_then(|list| list.first().map(String::as_str));
        let oem_params_file = oem_param_list.and_then(|list| list.first().map(String::as_str));

        let oem_param_str = oem_param_json.and_then(|v| v.as_str());
        self.set_update_completion_msg_suppressed(
            should_suppress_update_completion_msg(param_json, upd_params_file).await,
        );

        // Bridge Option<&mut Value> → Option<&Mutex<Value>> for the multipart API.
        let json_mutex = json_dict
            .as_deref_mut()
            .map(|v| std::mem::replace(v, Value::Null));
        let mutex_holder = json_mutex.map(|v| std::sync::Mutex::new(v));

        let (status, response_dict) = if let Some(params_file) = upd_params_file {
            self.target_access()
                .multipart_file_upload_with_options(
                    update_uri,
                    update_file,
                    Some(params_file),
                    time_out,
                    None,
                    oem_params_file,
                    oem_param_str,
                    mutex_holder.as_ref(),
                    parallel_update,
                    upload_options,
                )
                .await
        } else if let Some(pj) = param_json {
            let pj_str = serde_json::to_string(pj).unwrap_or_default();
            self.target_access()
                .multipart_file_upload_with_options(
                    update_uri,
                    update_file,
                    None,
                    time_out,
                    Some(&pj_str),
                    oem_params_file,
                    oem_param_str,
                    mutex_holder.as_ref(),
                    parallel_update,
                    upload_options,
                )
                .await
        } else {
            self.target_access()
                .multipart_file_upload_with_options(
                    update_uri,
                    update_file,
                    None,
                    time_out,
                    None,
                    oem_params_file,
                    oem_param_str,
                    mutex_holder.as_ref(),
                    parallel_update,
                    upload_options,
                )
                .await
        };

        // Move the value back from the Mutex into json_dict.
        if let (Some(ref mut jd), Some(holder)) = (&mut json_dict, mutex_holder) {
            if let Ok(val) = holder.into_inner() {
                **jd = val;
            }
        }

        if !status {
            if !upload_options.bail_on_failure {
                return None;
            }
            Util::bail_nvfwupd_threadsafe(
                1,
                &format!(
                    "File upload failed with error {:?}",
                    redacted_json_value(&response_dict)
                ),
                BailAction::DoNothing,
                json_dict.as_deref(),
                parallel_update,
            );
            return None;
        }

        let Some(task_id) = task_id_from_update_response(&response_dict) else {
            if !upload_options.bail_on_failure {
                return None;
            }
            let message = "Failed to acquire task ID from firmware update response";
            push_json_failure(json_dict.as_deref_mut(), message, Some(&response_dict));
            Util::bail_nvfwupd_threadsafe(
                1,
                message,
                BailAction::DoNothing,
                json_dict.as_deref(),
                parallel_update,
            );
            return None;
        };

        if emit_console {
            println!("{}", json_dumps_redacted(&response_dict));
        }

        // Append response for JSON output
        if let Some(ref mut jd) = json_dict {
            if let Some(output) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(redacted_json_value(&response_dict));
            }
        }

        Some(task_id)
    }

    /// Perform a firmware update using Redfish HTTP Push URI.
    ///
    /// Optionally sends a PATCH with update parameters first, then
    /// uploads the firmware file via POST. Returns the task ID on
    /// success, or `None` on failure.
    #[allow(clippy::too_many_arguments)]
    async fn update_component_pushuri(
        &mut self,
        param_list: Option<&Value>,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        mut json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        // Send PATCH request for update params if provided
        if let Some(params) = param_list {
            let (status, err_dict) = self
                .target_access()
                .dispatch_request("PATCH", update_uri, Some(params), json_dict.as_deref_mut())
                .await;
            if !status {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &format!(
                        "Patch update request failed! {:?}",
                        redacted_json_value(&err_dict)
                    ),
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                    parallel_update,
                );
                return None;
            }
        }

        // POST firmware file via dispatch_file_upload.
        // Bridge Option<&mut Value> → Option<&Mutex<Value>> for the upload API.
        let json_mutex = json_dict.as_deref_mut().map(|v| {
            // Move the current value into a Mutex, upload, then move back.
            std::mem::replace(v, Value::Null)
        });
        let mutex_holder = json_mutex.map(|v| std::sync::Mutex::new(v));
        let (status, response_dict) = self
            .target_access()
            .dispatch_file_upload(
                update_uri,
                update_file,
                time_out,
                mutex_holder.as_ref(),
                parallel_update,
                None,
            )
            .await;
        // Move the value back from the Mutex into json_dict.
        if let (Some(ref mut jd), Some(holder)) = (&mut json_dict, mutex_holder) {
            if let Ok(val) = holder.into_inner() {
                **jd = val;
            }
        }

        if !status {
            Util::bail_nvfwupd_threadsafe(
                1,
                &format!(
                    "File upload failed with error {:?}",
                    redacted_json_value(&response_dict)
                ),
                BailAction::DoNothing,
                None,
                parallel_update,
            );
            return None;
        }

        // Append response for JSON output
        if let Some(ref mut jd) = json_dict {
            if let Some(output) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(redacted_json_value(&response_dict));
            }
        }

        let Some(task_id) = task_id_from_update_response(&response_dict) else {
            let message = "Failed to acquire task ID from firmware update response";
            push_json_failure(json_dict.as_deref_mut(), message, Some(&response_dict));
            Util::bail_nvfwupd_threadsafe(
                1,
                message,
                BailAction::DoNothing,
                json_dict.as_deref(),
                parallel_update,
            );
            return None;
        };

        Some(task_id)
    }

    /// Start a firmware update and monitor it until completion.
    ///
    /// Iterates over the recipe list, dispatches update requests, and
    /// monitors task progress. Returns `(error_status, task_id_list)`.
    #[allow(clippy::too_many_arguments)]
    async fn start_update_monitor(
        &mut self,
        recipe_list: &[String],
        pkg_parser: &mut dyn PkgParser,
        cmd_args: &CmdArgs,
        time_out: u64,
        parallel_update: bool,
        json_dict: Option<&mut Value>,
        update_delay: u64,
        skip_pre_flight_checks: bool,
        update_precondition_mode: Option<UpdatePreconditionMode>,
    ) -> (i32, Vec<String>) {
        self.start_update_monitor_default(
            recipe_list,
            pkg_parser,
            cmd_args,
            time_out,
            parallel_update,
            json_dict,
            update_delay,
            skip_pre_flight_checks,
            update_precondition_mode,
        )
        .await
    }

    /// Common Redfish update monitor used by targets that do not need a
    /// platform-specific upload/install workflow.
    #[allow(clippy::too_many_arguments)]
    async fn start_update_monitor_default(
        &mut self,
        recipe_list: &[String],
        pkg_parser: &mut dyn PkgParser,
        cmd_args: &CmdArgs,
        time_out: u64,
        parallel_update: bool,
        mut json_dict: Option<&mut Value>,
        update_delay: u64,
        skip_pre_flight_checks: bool,
        update_precondition_mode: Option<UpdatePreconditionMode>,
    ) -> (i32, Vec<String>) {
        let mut err_status: i32 = 0;
        let mut task_id_list: Vec<String> = Vec::new();
        let emit_console = should_emit_console(cmd_args.quiet, json_dict.as_deref());

        if !skip_pre_flight_checks {
            if let Some(mode) = update_precondition_mode {
                if let Err(message) = self
                    .check_update_preconditions(mode, json_dict.as_deref_mut())
                    .await
                {
                    push_json_failure(json_dict.as_deref_mut(), &message, None);
                    Util::bail_nvfwupd(1, &message, BailAction::DoNothing, json_dict.as_deref());
                    return (1, Vec::new());
                }
            }
        }

        // Check if an update task is already running (unless skipping pre-flight checks)
        if !skip_pre_flight_checks {
            let (is_running, _running_task_id, error_msg) = self
                .check_for_running_update_task_by_type(recipe_list, json_dict.as_deref_mut())
                .await;
            if is_running {
                Util::bail_nvfwupd(
                    1,
                    error_msg.as_deref().unwrap_or("Update already running"),
                    BailAction::PrintDivider,
                    json_dict.as_deref(),
                );
                return (1, Vec::new());
            }
        }

        // Verify that UpdateService is enabled
        let (mut status, mut my_dict) = self
            .target_access()
            .dispatch_request(
                "GET",
                "/redfish/v1/UpdateService",
                None,
                json_dict.as_deref_mut(),
            )
            .await;

        // Optional override to skip pre-flight checks (typically for early board bringup)
        if skip_pre_flight_checks {
            for each in recipe_list {
                if emit_console {
                    tracing::info!(
                        indent = 2,
                        "Skipping pre-flight checks for the update using package {}",
                        each
                    );
                }
            }
            status = true;
            my_dict = json!({"ServiceEnabled": true});
        }

        let service_enabled = my_dict
            .get("ServiceEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !status {
            let message = "Failed to query UpdateService before starting firmware update";
            push_json_failure(json_dict.as_deref_mut(), message, Some(&my_dict));
            Util::bail_nvfwupd(1, message, BailAction::PrintDivider, json_dict.as_deref());
            return (1, Vec::new());
        }

        if !service_enabled {
            let message = "UpdateService is not enabled in the system";
            push_json_failure(json_dict.as_deref_mut(), message, Some(&my_dict));
            Util::bail_nvfwupd(1, message, BailAction::PrintDivider, json_dict.as_deref());
            return (1, Vec::new());
        }

        // Check staged update support
        if cmd_args.staged_update || cmd_args.staged_activate_update {
            let supported = my_dict
                .pointer("/Oem/Nvidia/MultipartHttpPushUriOptions/UpdateOptionSupport")
                .and_then(|v| v.as_array());
            match supported {
                Some(options) => {
                    let has_stage_and_activate = options
                        .iter()
                        .any(|v| v.as_str() == Some("StageAndActivate"));
                    let has_stage_only = options.iter().any(|v| v.as_str() == Some("StageOnly"));
                    if !has_stage_and_activate || !has_stage_only {
                        Util::bail_nvfwupd(
                            1,
                            "System does not support staged update",
                            BailAction::DoNothing,
                            json_dict.as_deref(),
                        );
                        return (1, Vec::new());
                    }
                }
                None => {
                    Util::bail_nvfwupd(
                        1,
                        "System does not support staged update",
                        BailAction::DoNothing,
                        json_dict.as_deref(),
                    );
                    return (1, Vec::new());
                }
            }
        }

        // Fetch the URI service for POST
        let update_uri = self.get_update_uri(&my_dict);
        let mut final_result: Vec<Value> = Vec::new();
        let mut seen_result: HashMap<String, Value> = HashMap::new();

        for each in recipe_list {
            let (parse_ok, msg) = pkg_parser.parse_pkg(each).await;
            if !parse_ok {
                if emit_console {
                    tracing::info!(
                        indent = 2,
                        "WARN: {} is not a valid package. Ignoring",
                        each
                    );
                }
                Util::bail_nvfwupd(1, &msg, BailAction::DoNothing, json_dict.as_deref());
                err_status = 1;
                break;
            }

            // Delay between updates to the same system
            if update_delay > 0 {
                if emit_console {
                    println!(
                        "Waiting {} seconds before beginning update with {}",
                        update_delay, each
                    );
                }
                tokio::time::sleep(Duration::from_secs(update_delay)).await;
            }

            // Send update request
            let task_id = self
                .update_component(
                    cmd_args,
                    &update_uri,
                    each,
                    time_out,
                    json_dict.as_mut().map(|r| &mut **r),
                    parallel_update,
                )
                .await;

            let task_id = match task_id {
                Some(id) if !id.trim().is_empty() => id,
                None => {
                    let message = "Failed to acquire task ID from firmware update response";
                    push_json_failure(json_dict.as_deref_mut(), message, None);
                    Util::bail_nvfwupd(1, message, BailAction::DoNothing, json_dict.as_deref());
                    return (1, Vec::new());
                }
                Some(_) => {
                    let message = "Failed to acquire task ID from firmware update response";
                    push_json_failure(json_dict.as_deref_mut(), message, None);
                    Util::bail_nvfwupd(1, message, BailAction::DoNothing, json_dict.as_deref());
                    return (1, Vec::new());
                }
            };

            let task_service_uri = self.get_task_service_uri(&task_id);

            if !task_id.is_empty() {
                if emit_console {
                    tracing::info!(indent = 2, "FW update started, Task Id: {}", task_id);
                }

                // PowerShelf BMC resets itself after update, so skip task monitoring.
                // Python checks both self.__class__.__name__ AND config_platform_target.
                let is_powershelf = self.class_name() == "PowerShelfRFTarget"
                    || self
                        .config_dict()
                        .and_then(|cfg| cfg.get("TargetPlatform"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.eq_ignore_ascii_case("powershelf"))
                        .unwrap_or(false);

                if parallel_update {
                    task_id_list.push(task_id.clone());
                    continue;
                }

                // Foreground monitoring
                if !cmd_args.background && !is_powershelf {
                    // Initial task query
                    let (_, task_dict) = self
                        .target_access()
                        .dispatch_request("GET", &task_service_uri, None, json_dict.as_deref_mut())
                        .await;

                    if task_dict.get("error").is_some() {
                        if emit_console {
                            tracing::info!(indent = 2, "Input Taskid does not exist: {} ", task_id);
                        }
                        err_status = 1;
                        break;
                    }

                    // Wait for FirmwareUpdateStarted message
                    let mut count = 0u32;
                    let mut break_out = false;
                    let mut last_task_dict = task_dict;

                    while count < 10 {
                        if emit_console && count == 0 {
                            println!("Wait for Firmware Update to Start...");
                        }
                        let (_, td) = self
                            .target_access()
                            .dispatch_request(
                                "GET",
                                &task_service_uri,
                                None,
                                json_dict.as_deref_mut(),
                            )
                            .await;
                        last_task_dict = td;

                        if emit_console {
                            tracing::debug!("{:?}", redacted_json_value(&last_task_dict));
                        }

                        // Check messages for InstallingOnComponent or failure
                        let task_state = last_task_dict
                            .get("TaskState")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");

                        if let Some(messages) =
                            last_task_dict.get("Messages").and_then(|m| m.as_array())
                        {
                            let mut found_installing = false;
                            for each_msg in messages {
                                let msg_id = each_msg
                                    .get("MessageId")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                if msg_id.contains("Update.1.0.InstallingOnComponent") {
                                    if emit_console {
                                        println!();
                                        if let Some(args) =
                                            each_msg.get("MessageArgs").and_then(|a| a.as_array())
                                        {
                                            if let Some(component) =
                                                args.get(1).and_then(|v| v.as_str())
                                            {
                                                tracing::info!(
                                                    indent = 2,
                                                    "Started Updating: {}",
                                                    NvUtils::sanitize_log(component)
                                                );
                                            }
                                        }
                                    }
                                    found_installing = true;
                                    break;
                                } else if TASK_FAILURE_STATES
                                    .contains(&task_state.to_lowercase().as_str())
                                {
                                    if emit_console {
                                        println!();
                                        println!(
                                            "{}",
                                            json_pretty_4space_redacted(&last_task_dict)
                                        );
                                    }
                                    Util::bail_nvfwupd(
                                        1,
                                        "FW update failed with the errors",
                                        BailAction::DoNothing,
                                        json_dict.as_deref(),
                                    );
                                    err_status = 1;
                                    break_out = true;
                                    break;
                                }
                            }
                            // Break the while loop once we observe the installing state or a
                            // task failure, then proceed to the outer monitoring loop.
                            if found_installing || break_out {
                                break;
                            }
                        }

                        count += 1;
                        tokio::time::sleep(Duration::from_secs(15)).await;
                    }

                    // Commented out to reduce console noise for now.
                    // If count reached 10 without seeing FirmwareUpdateStarted
                    // if count == 10 && !break_out {
                    //     if emit_console {
                    //         println!(
                    //             "Waiting for Task Id {} to start. Current task details:",
                    //             task_id
                    //         );
                    //         println!("{}", json_pretty_4space_redacted(&last_task_dict));
                    //     }
                    // }

                    if break_out {
                        break;
                    }

                    tokio::time::sleep(Duration::from_secs(10)).await;

                    // Main progress monitoring loop
                    let mut last_progress: i64 = 0;
                    let mut _last_update_time = Instant::now();
                    let task_timeout = Duration::from_secs(600);

                    loop {
                        tokio::time::sleep(Duration::from_secs(5)).await;

                        let (poll_status, task_dict) = self
                            .dispatch_request_with_retry(
                                "GET",
                                &task_service_uri,
                                None,
                                json_dict.as_deref_mut(),
                                3,
                                5,
                            )
                            .await;

                        if !poll_status {
                            Util::bail_nvfwupd(
                                1,
                                "Failed to get task status.",
                                BailAction::DoNothing,
                                json_dict.as_deref(),
                            );
                            err_status = 1;
                            break;
                        }

                        let task_state = task_dict
                            .get("TaskState")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let task_status_val = task_dict
                            .get("TaskStatus")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown")
                            .to_string();
                        let progress = task_dict
                            .get("PercentComplete")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);

                        if emit_console {
                            tracing::debug!("{:?}", redacted_json_value(&task_dict));
                        }

                        // Print task details when progress changes
                        if progress != last_progress {
                            last_progress = progress;
                            if emit_console {
                                if cmd_args.details {
                                    self.start_update_monitor_table(
                                        &redacted_json_value(&task_dict),
                                        &mut final_result,
                                        &mut seen_result,
                                    );
                                } else {
                                    tracing::info!(
                                        indent = 2,
                                        "TaskState: {}",
                                        NvUtils::sanitize_log(&task_state)
                                    );
                                    tracing::info!(indent = 2, "PercentComplete: {}", progress);
                                    tracing::info!(
                                        indent = 2,
                                        "TaskStatus: {}",
                                        NvUtils::sanitize_log(&task_status_val)
                                    );
                                }
                            }
                        }

                        if task_state == "Completed" {
                            if task_status_val == "OK" {
                                if emit_console {
                                    tracing::info!(indent = 2, "Firmware update successful!");
                                    self.print_task_completion(&task_dict);
                                }
                                err_status = 0;
                            } else {
                                // Completed with non-OK status
                                if emit_console {
                                    // Print last message and severity from Messages array
                                    if let Some(messages) =
                                        task_dict.get("Messages").and_then(|m| m.as_array())
                                    {
                                        if let Some(last_msg) = messages.last() {
                                            if let Some(msg_text) =
                                                last_msg.get("Message").and_then(|v| v.as_str())
                                            {
                                                tracing::info!(
                                                    indent = 2,
                                                    "Task Message: {}",
                                                    NvUtils::sanitize_log(msg_text)
                                                );
                                            }
                                            if let Some(severity) =
                                                last_msg.get("Severity").and_then(|v| v.as_str())
                                            {
                                                tracing::info!(
                                                    indent = 2,
                                                    "Severity: {}",
                                                    severity
                                                );
                                            }
                                        }
                                    }
                                    tracing::info!(
                                        indent = 2,
                                        "TaskState: {}",
                                        NvUtils::sanitize_log(&task_state)
                                    );
                                    tracing::info!(
                                        indent = 2,
                                        "{}",
                                        json_pretty_4space_redacted(&task_dict)
                                    );
                                }
                                if task_has_structured_failure(&task_dict) {
                                    Util::bail_nvfwupd(
                                        1,
                                        "Firmware update had errors.",
                                        BailAction::DoNothing,
                                        json_dict.as_deref(),
                                    );
                                    err_status = 1;
                                } else {
                                    if emit_console {
                                        tracing::info!(
                                            indent = 2,
                                            "Firmware update was completed with status {}",
                                            task_status_val
                                        );
                                        self.print_task_completion(&task_dict);
                                    }
                                    err_status = 0;
                                }
                            }
                            break;
                        }

                        if task_state == "Cancelled" {
                            if emit_console {
                                // Print last message and severity from Messages array
                                if let Some(messages) =
                                    task_dict.get("Messages").and_then(|m| m.as_array())
                                {
                                    if let Some(last_msg) = messages.last() {
                                        if let Some(msg_text) =
                                            last_msg.get("Message").and_then(|v| v.as_str())
                                        {
                                            tracing::info!(
                                                indent = 2,
                                                "TaskStatus: {}",
                                                NvUtils::sanitize_log(msg_text)
                                            );
                                        }
                                        if let Some(severity) =
                                            last_msg.get("Severity").and_then(|v| v.as_str())
                                        {
                                            tracing::info!(indent = 2, "Severity: {}", severity);
                                        }
                                    }
                                }
                                tracing::info!(
                                    indent = 2,
                                    "TaskState: {}",
                                    NvUtils::sanitize_log(&task_state)
                                );
                            }
                            Util::bail_nvfwupd(
                                1,
                                "Firmware update request cancelled by host BMC",
                                BailAction::DoNothing,
                                json_dict.as_deref(),
                            );
                            err_status = 1;
                            break;
                        }

                        if TASK_FAILURE_STATES.contains(&task_state.to_lowercase().as_str()) {
                            if emit_console {
                                if cmd_args.details {
                                    self.start_update_monitor_table(
                                        &redacted_json_value(&task_dict),
                                        &mut final_result,
                                        &mut seen_result,
                                    );
                                } else {
                                    println!("{}", json_pretty_4space_redacted(&task_dict));
                                }
                            }
                            Util::bail_nvfwupd(
                                1,
                                "Update failed with exception",
                                BailAction::DoNothing,
                                json_dict.as_deref(),
                            );
                            err_status = 1;
                            break;
                        }

                        _last_update_time = Instant::now();
                        // TODO: The Python code has a bug where last_update_time is
                        // reset every iteration, making the timeout unreachable.
                        // Preserved here for fidelity; fix when confirmed.

                        tokio::time::sleep(Duration::from_secs(15)).await;
                    }

                    if break_out || err_status != 0 {
                        break;
                    }
                } else {
                    // Background/PowerShelf mode: print status and exit loop.
                    // Python always emits the progress hint regardless of json mode.
                    if emit_console {
                        tracing::info!(indent = 2, "Firmware update in progress");
                    }
                    Util::bail_nvfwupd(
                        0,
                        "use show_update_progress command to monitor the progress",
                        BailAction::DoNothing,
                        json_dict.as_deref(),
                    );
                    err_status = 0;
                    break;
                }
            }

            // Check for errors in the UpdateService response
            if my_dict.get("error").is_some() {
                err_status = 1;
                if let Some(error_obj) = my_dict.get("error") {
                    if let Some(msg) = error_obj.get("message").and_then(|v| v.as_str()) {
                        if emit_console {
                            tracing::info!(indent = 2, "Error: {}", msg);
                        }
                    }
                }
                Util::bail_nvfwupd(
                    1,
                    "Firmware Update request failed",
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                );
                break;
            } else if !status {
                err_status = 1;
                Util::bail_nvfwupd(
                    1,
                    "Firmware update request failed!",
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                );
                break;
            }
        }

        (err_status, task_id_list)
    }

    /// Print update progress to the console in tabular format.
    ///
    /// Deduplicates messages by MessageId→Message using the `seen_result`
    /// map (so repeated identical messages aren't re-printed), and
    /// accumulates new rows in `final_result`.  Matches Python's
    /// `start_update_monitor_table` + `Util.compare_dict` behavior.
    fn start_update_monitor_table(
        &mut self,
        task_dict: &Value,
        final_result: &mut Vec<Value>,
        seen_result: &mut HashMap<String, Value>,
    ) {
        let messages = task_dict
            .get("Messages")
            .and_then(|m| m.as_array())
            .map(|messages| {
                messages
                    .iter()
                    .map(redacted_json_value)
                    .collect::<Vec<Value>>()
            })
            .unwrap_or_default();

        // compare_dict: only add (MessageId, Message) pairs we haven't seen.
        for msg in &messages {
            let key = msg
                .get("MessageId")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown")
                .to_string();
            let value = msg
                .get("Message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let entry = seen_result
                .entry(key.clone())
                .or_insert_with(|| Value::Array(Vec::new()));
            let already_seen = entry
                .as_array()
                .map(|arr| arr.iter().any(|v| v.as_str() == Some(value.as_str())))
                .unwrap_or(false);
            if !already_seen {
                if let Some(arr) = entry.as_array_mut() {
                    arr.push(Value::String(value.clone()));
                }
                final_result.push(json!({ key.clone(): value.clone() }));
            }
        }

        if final_result.is_empty() {
            return;
        }

        // Print header once per monitoring session
        let header_width_id = 25usize;
        let header_width_msg = 60usize;
        let border = format!(
            "+{}+{}+",
            "-".repeat(header_width_id + 2),
            "-".repeat(header_width_msg + 2),
        );
        if !self.progress_table_header_printed() {
            println!("{}", border);
            println!(
                "| {:<id_w$} | {:<msg_w$} |",
                "MessageId",
                "Message",
                id_w = header_width_id,
                msg_w = header_width_msg,
            );
            println!("{}", border);
            self.set_progress_table_header_printed(true);
        }

        for item in final_result.iter() {
            if let Some(obj) = item.as_object() {
                for (k, v) in obj {
                    let msg = v.as_str().unwrap_or("");
                    // Simple wrap for long lines: split into chunks.
                    let id_text = Util::wrap_text(k, header_width_id);
                    let msg_text = Util::wrap_text(msg, header_width_msg);
                    for (id_line, msg_line) in id_text
                        .lines()
                        .zip(msg_text.lines().chain(std::iter::repeat("")))
                        .take(id_text.lines().count().max(msg_text.lines().count()))
                    {
                        println!(
                            "| {:<id_w$} | {:<msg_w$} |",
                            id_line,
                            msg_line,
                            id_w = header_width_id,
                            msg_w = header_width_msg,
                        );
                    }
                }
            }
        }
        println!("{}", border);
        final_result.clear();
    }

    /// Perform out-of-band activation through the Redfish API.
    ///
    /// The default implementation reports that the command is not
    /// supported for this target. Platform-specific implementations
    /// override this method.
    async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        Util::bail_nvfwupd(
            1,
            &format!(
                "Activation {} command not supported for target.",
                cmd_args.cmd
            ),
            BailAction::Exit,
            None,
        );
        1
    }
}

/// Parse a Redfish timestamp string into a `DateTime<FixedOffset>`.
///
/// Supports formats: `%Y-%m-%dT%H:%M:%S%z` and `%Y-%m-%dT%H:%M:%S-0000`.
pub fn get_timestamp(str_time: &str) -> Option<DateTime<FixedOffset>> {
    if let Ok(dt) = DateTime::parse_from_str(str_time, "%Y-%m-%dT%H:%M:%S%z") {
        return Some(dt);
    }
    // Fallback: parse without timezone and assume UTC
    if let Ok(naive) = NaiveDateTime::parse_from_str(str_time, "%Y-%m-%dT%H:%M:%S-0000") {
        let offset = FixedOffset::east_opt(0).unwrap();
        return Some(DateTime::<FixedOffset>::from_naive_utc_and_offset(
            naive, offset,
        ));
    }
    None
}

/// Format a `chrono::Duration` as `H:MM:SS` (e.g. `0:05:33`).
pub fn format_duration_hms(dur: &chrono::Duration) -> String {
    let total_secs = dur.num_seconds().unsigned_abs();
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    format!("{}:{:02}:{:02}", h, m, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingPreconditionTarget {
        bmc_access: BmcAccess,
        fungible_components: Vec<String>,
        update_completion_msg: String,
        progress_table_header_printed: bool,
    }

    impl FailingPreconditionTarget {
        fn new() -> Self {
            Self::with_access(BmcAccess::default_stub())
        }

        fn with_access(bmc_access: BmcAccess) -> Self {
            Self {
                bmc_access,
                fungible_components: Vec::new(),
                update_completion_msg: String::new(),
                progress_table_header_printed: false,
            }
        }
    }

    #[async_trait::async_trait]
    impl RFTarget for FailingPreconditionTarget {
        fn target_access(&self) -> &BmcAccess {
            &self.bmc_access
        }

        fn target_access_mut(&mut self) -> &mut BmcAccess {
            &mut self.bmc_access
        }

        fn fungible_components(&self) -> &[String] {
            &self.fungible_components
        }

        fn update_completion_msg(&self) -> &str {
            &self.update_completion_msg
        }

        fn set_update_completion_msg(&mut self, msg: &str) {
            self.update_completion_msg = msg.to_string();
        }

        fn progress_table_header_printed(&self) -> bool {
            self.progress_table_header_printed
        }

        fn set_progress_table_header_printed(&mut self, printed: bool) {
            self.progress_table_header_printed = printed;
        }

        fn config_dict(&self) -> Option<&Value> {
            None
        }

        fn class_name(&self) -> &str {
            "FailingPreconditionTarget"
        }

        async fn check_update_preconditions(
            &self,
            _mode: UpdatePreconditionMode,
            _json_dict: Option<&mut Value>,
        ) -> Result<(), String> {
            Err("BackgroundCopy is currently in progress".to_string())
        }

        async fn update_component(
            &mut self,
            _cmd_args: &CmdArgs,
            _update_uri: &str,
            _update_file: &str,
            _time_out: u64,
            _json_dict: Option<&mut Value>,
            _parallel_update: bool,
        ) -> Option<String> {
            None
        }

        fn is_fungible_component(&self, _component_name: &str) -> bool {
            false
        }

        async fn get_component_version(
            &self,
            _pldm_version_dict: &Value,
            _ap_name: &str,
            _pkg_parser: Option<&dyn PkgParser>,
        ) -> Option<String> {
            None
        }

        async fn get_identifier_from_chassis(&self, _ap_inv_uri: &str) -> Option<String> {
            None
        }

        fn get_version_sku(
            &self,
            _identifier: &str,
            _pldm_version_dict: &Value,
            _ap_name: &str,
        ) -> Option<String> {
            None
        }
    }

    struct UnusedParser;

    #[async_trait::async_trait]
    impl PkgParser for UnusedParser {
        async fn parse_pkg(&mut self, _pkg_path: &str) -> (bool, String) {
            panic!("precondition failure should stop before package parsing");
        }
    }

    struct RecordingParser {
        called: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait::async_trait]
    impl PkgParser for RecordingParser {
        async fn parse_pkg(&mut self, _pkg_path: &str) -> (bool, String) {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            (
                false,
                "parser reached after skipped preconditions".to_string(),
            )
        }
    }

    #[tokio::test]
    async fn update_precondition_failure_is_reported_in_json_mode() {
        let mut target = FailingPreconditionTarget::new();
        let mut parser = UnusedParser;
        let cmd_args = CmdArgs {
            cmd: "update_fw".to_string(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special: None,
            oem_parameters: None,
        };
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });

        let (status, task_ids) = target
            .start_update_monitor(
                &["firmware.fwpkg".to_string()],
                &mut parser,
                &cmd_args,
                30,
                false,
                Some(&mut output),
                0,
                false,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        assert_eq!(status, 1);
        assert!(task_ids.is_empty());
        assert_eq!(output["Error Code"], 1);
        assert_eq!(
            output["Error"][0],
            "BackgroundCopy is currently in progress"
        );
    }

    #[tokio::test]
    async fn skipped_update_preconditions_do_not_run_second_check() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ServiceEnabled": true,
                "MultipartHttpPushUri": "/redfish/v1/UpdateService/update-multipart"
            })))
            .mount(&server)
            .await;

        let mut target = FailingPreconditionTarget::with_access(BmcAccess::mock_with_base_url(
            server.uri(),
            "mock-gb200",
        ));
        let parser_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut parser = RecordingParser {
            called: parser_called.clone(),
        };
        let cmd_args = CmdArgs {
            cmd: "update_fw".to_string(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special: None,
            oem_parameters: None,
        };

        let (status, task_ids) = target
            .start_update_monitor(
                &["firmware.fwpkg".to_string()],
                &mut parser,
                &cmd_args,
                30,
                false,
                None,
                0,
                false,
                None,
            )
            .await;

        assert_eq!(status, 1);
        assert!(task_ids.is_empty());
        assert!(parser_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn skip_preflight_checks_skips_update_precondition_mode() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ServiceEnabled": false
            })))
            .mount(&server)
            .await;

        let mut target = FailingPreconditionTarget::with_access(BmcAccess::mock_with_base_url(
            server.uri(),
            "mock-gb200",
        ));
        let parser_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut parser = RecordingParser {
            called: parser_called.clone(),
        };
        let cmd_args = CmdArgs {
            cmd: "update_fw".to_string(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special: None,
            oem_parameters: None,
        };
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });

        let (status, task_ids) = target
            .start_update_monitor(
                &["firmware.fwpkg".to_string()],
                &mut parser,
                &cmd_args,
                30,
                false,
                Some(&mut output),
                0,
                true,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        assert_eq!(status, 1);
        assert!(task_ids.is_empty());
        assert!(parser_called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!output["Error"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error == "BackgroundCopy is currently in progress"));
    }

    #[test]
    fn redacted_json_formatters_mask_password_like_fields() {
        let response = json!({
            "Id": "Task-1",
            "Password": "plain secret",
            "Messages": [
                {
                    "Message": "BMC echoed password=message secret",
                    "MessageArgs": ["username=admin", "password=arg secret"]
                }
            ]
        });

        let compact = json_dumps_redacted(&response);
        let pretty = json_pretty_4space_redacted(&response);

        for rendered in [compact, pretty] {
            assert!(!rendered.contains("plain secret"));
            assert!(!rendered.contains("message secret"));
            assert!(!rendered.contains("arg secret"));
            assert!(rendered.contains("XXXX"));
        }
    }

    #[test]
    fn quiet_mode_suppresses_console_without_json_sink() {
        let json_sink = json!({"Output": []});

        assert!(should_emit_console(false, None));
        assert!(!should_emit_console(true, None));
        assert!(!should_emit_console(false, Some(&json_sink)));
    }

    #[test]
    fn firmware_version_sentinels_are_case_insensitive_and_trimmed() {
        for version in ["unknown", "UNKNOWN", " Unknown ", "N/A", "n/a", " n/A "] {
            assert!(firmware_version_is_sentinel(version));
        }

        assert!(!firmware_version_is_sentinel("1.2.3"));
    }

    #[tokio::test]
    async fn completion_msg_suppression_detects_ist_vector_param_json() {
        assert!(
            should_suppress_update_completion_msg(
                Some(&json!({
                    "Targets": [
                        "/redfish/v1/UpdateService/SoftwareInventory/IST_Vectors"
                    ]
                })),
                None,
            )
            .await
        );
        assert!(
            !should_suppress_update_completion_msg(
                Some(&json!({
                    "Targets": [
                        "/redfish/v1/UpdateService/FirmwareInventory/BMC"
                    ]
                })),
                None,
            )
            .await
        );
    }

    #[tokio::test]
    async fn completion_msg_suppression_detects_ist_vector_param_file() {
        let tmp = tempfile::tempdir().unwrap();
        let params_file = tmp.path().join("params.json");
        tokio::fs::write(
            &params_file,
            r#"{"Targets":["/redfish/v1/UpdateService/SoftwareInventory/HGX_IST_Vectors"]}"#,
        )
        .await
        .unwrap();

        assert!(should_suppress_update_completion_msg(None, params_file.to_str()).await);
    }

    #[test]
    fn update_response_task_id_extraction_rejects_empty_ids() {
        assert_eq!(task_id_from_update_response(&json!({})), None);
        assert_eq!(task_id_from_update_response(&json!({"Id": ""})), None);
        assert_eq!(
            task_id_from_update_response(&json!({"@odata.id": "/redfish/v1/TaskService/Tasks/"})),
            None
        );
        for response in [
            json!({"Messages": []}),
            json!({"Messages": [{"MessageArgs": []}]}),
            json!({"Messages": [{"MessageArgs": [123]}]}),
        ] {
            assert_eq!(task_id_from_update_response(&response), None);
        }
    }

    #[test]
    fn update_response_task_id_extraction_rejects_non_task_redfish_paths() {
        assert_eq!(
            task_id_from_update_response(
                &json!({"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/BMC"})
            ),
            None
        );
        assert_eq!(
            task_id_from_update_response(&json!({
                "Messages": [{
                    "MessageArgs": ["/redfish/v1/UpdateService/FirmwareInventory/BMC"]
                }]
            })),
            None
        );
        assert_eq!(
            task_id_from_update_response(&json!({
                "Messages": [{
                    "MessageArgs": [
                        "/redfish/v1/UpdateService/FirmwareInventory/BMC",
                        "/redfish/v1/TaskService/Tasks/Task-11"
                    ]
                }]
            }))
            .as_deref(),
            Some("Task-11")
        );
    }

    #[test]
    fn update_response_task_id_extraction_accepts_redfish_locations() {
        assert_eq!(
            task_id_from_update_response(&json!({"Id": "Task-7"})).as_deref(),
            Some("Task-7")
        );
        assert_eq!(
            task_id_from_update_response(
                &json!({"@odata.id": "/redfish/v1/TaskService/Tasks/Task-8"})
            )
            .as_deref(),
            Some("Task-8")
        );
        assert_eq!(
            task_id_from_update_response(
                &json!({"@odata.id": "/redfish/v1/TaskService/Tasks/Task-8/"})
            )
            .as_deref(),
            Some("Task-8")
        );
        assert_eq!(
            task_id_from_update_response(&json!({
                "Messages": [{
                    "MessageArgs": ["/redfish/v1/TaskService/Tasks/Task-9"]
                }]
            }))
            .as_deref(),
            Some("Task-9")
        );
        assert_eq!(
            task_id_from_update_response(&json!({
                "Messages": [
                    {"MessageArgs": []},
                    {"MessageArgs": ["/redfish/v1/TaskService/Tasks/", "/redfish/v1/TaskService/Tasks/Task-10"]}
                ]
            }))
            .as_deref(),
            Some("Task-10")
        );
    }

    #[test]
    fn structured_task_failure_ignores_benign_message_text() {
        let task = json!({
            "TaskState": "Completed",
            "TaskStatus": "Warning",
            "Messages": [{
                "Message": "No errors detected",
                "Severity": "Warning",
                "MessageId": "Update.1.0.CompletedWithWarning"
            }]
        });

        assert!(!task_has_structured_failure(&task));
    }

    #[test]
    fn structured_task_failure_uses_status_severity_and_message_id() {
        assert!(task_has_structured_failure(&json!({
            "TaskState": "Cancelled",
            "TaskStatus": "OK"
        })));

        assert!(task_has_structured_failure(&json!({
            "TaskState": "Completed",
            "TaskStatus": "Critical"
        })));

        assert!(task_has_structured_failure(&json!({
            "TaskState": "Completed",
            "TaskStatus": "Warning",
            "Messages": [{
                "Message": "Completed",
                "MessageSeverity": "Critical"
            }]
        })));

        assert!(task_has_structured_failure(&json!({
            "TaskState": "Completed",
            "TaskStatus": "Warning",
            "Messages": [{
                "Message": "Completed",
                "MessageId": "TaskEvent.1.0.3.TaskAborted"
            }]
        })));
    }
}
