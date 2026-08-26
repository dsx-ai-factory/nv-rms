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

//! GB200SwitchRFTarget implementation for GB200/GB300/VR network switches.
//!
//! Uses NVSwitch access mode and REST API (not standard Redfish push URI)
//! for firmware updates.

use std::collections::HashMap;
use std::time::Duration;

use nvue_client::Client as NvueClient;
use serde_json::{json, Value};

use crate::bmc_access::BmcAccess;
use crate::rf_target::{CmdArgs, PkgParser, RFTarget, UpdatePreconditionMode};
use crate::ssh_transport;
use crate::util::{BailAction, Util};
use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// REST API endpoint for switch firmware update.
const SWITCH_UPDATE_URI: &str = "/nvue_v1/system/firmware";

/// REST API endpoint for switch task status.
const SWITCH_TASK_URI: &str = "/nvue_v1/system/firmware/status";

/// Per-component update URL: /nvue_v1/platform/firmware/{target}/files/{filename}
const PER_COMPONENT_UPDATE_URL: &str = "/nvue_v1/platform/firmware";

/// Job status URL for NVUE actions.
const NVUE_ACTION_URL: &str = "/nvue_v1/action";

/// Destination upload path on the switch filesystem.
const DEST_UPLOAD_PATH: &str = "/host/fw-images/";

/// Update ordering for package type 0000 (BMC/EROT/FPGA).
const UPDATE_ORDER_0000: &[&str] = &["bmc", "erot", "fpga"];

/// Update ordering for package type 0002 (BIOS/EROT).
const UPDATE_ORDER_0002: &[&str] = &["bios", "erot"];

/// Update ordering for VRNVL72 platforms.
const UPDATE_ORDER_VRNVL72: &[&str] = &["bmc", "bios", "sma", "erot", "cpld1"];

/// Task states indicating the task is still running.
const PENDING_TASK_STATES: &[&str] = &["running", "start", "action_running"];

/// Parallel firmware fetch URL for VRNVL72.
const PARALLEL_FETCH_URL: &str = "/nvue_v1/platform/firmware";

/// Parallel firmware install URL template for VRNVL72.
/// Format: /nvue_v1/platform/firmware/files/{filename}
const PARALLEL_UPDATE_URL_PREFIX: &str = "/nvue_v1/platform/firmware/files";

/// Temporary upload path for VRNVL72 parallel updates.
const PARALLEL_UPLOAD_PATH: &str = "/tmp/";

const POWER_CYCLE_COMMAND: &str = "NVUE_PWR_CYCLE";
const POWER_CYCLE_POST_TIMEOUT_SECS: u64 = 120;
const POWER_CYCLE_ACTION_RETRIES: u32 = 3;
const POWER_CYCLE_ACTION_RETRY_INTERVAL_SECS: u64 = 5;

#[derive(Clone, Copy)]
enum SwitchNvueAccess<'a> {
    Legacy,
    Hosted(&'a NvueClient),
}

impl SwitchNvueAccess<'_> {
    async fn get(
        &self,
        legacy_access: &BmcAccess,
        path: &str,
        timeout_secs: u64,
        json_output: Option<&mut Value>,
    ) -> (bool, Value) {
        match self {
            Self::Legacy => {
                legacy_access
                    .dispatch_rest_request_get(
                        crate::util::TraceFlags::default(),
                        path,
                        timeout_secs,
                        json_output,
                    )
                    .await
            }
            Self::Hosted(client) => match client.get(path, Duration::from_secs(timeout_secs)).await
            {
                Ok(response) if response.status == 200 => {
                    match serde_json::from_str::<Value>(&response.body) {
                        Ok(value) => (true, value),
                        Err(error) => {
                            record_hosted_nvue_error(json_output, &error.to_string());
                            (false, json!({"error": error.to_string()}))
                        }
                    }
                }
                Ok(response) => (false, response.value),
                Err(error) => {
                    record_hosted_nvue_error(json_output, &error.to_string());
                    (false, json!({"error": error.to_string()}))
                }
            },
        }
    }

    async fn post(
        &self,
        legacy_access: &BmcAccess,
        path: &str,
        payload: &Value,
        timeout_secs: u64,
        json_output: Option<&mut Value>,
    ) -> (bool, Value, String) {
        match self {
            Self::Legacy => {
                legacy_access
                    .dispatch_rest_request_post(
                        crate::util::TraceFlags::default(),
                        path,
                        payload,
                        timeout_secs,
                        json_output,
                    )
                    .await
            }
            Self::Hosted(client) => match client
                .post(path, payload, Duration::from_secs(timeout_secs))
                .await
            {
                Ok(response) => (
                    (200..300).contains(&response.status),
                    response.value,
                    response.body,
                ),
                Err(error) => {
                    record_hosted_nvue_error(json_output, &error.to_string());
                    (false, json!({"error": error.to_string()}), String::new())
                }
            },
        }
    }
}

fn record_hosted_nvue_error(json_output: Option<&mut Value>, message: &str) {
    tracing::warn!(error = %message, "NVUE request failed");

    let Some(output) = json_output.and_then(Value::as_object_mut) else {
        return;
    };

    output
        .entry("Error")
        .or_insert_with(|| Value::Array(Vec::new()));

    if let Some(errors) = output.get_mut("Error").and_then(Value::as_array_mut) {
        errors.push(Value::String(message.to_owned()));
    }

    output.insert("Error Code".to_owned(), Value::Number(1.into()));
}

async fn hosted_transport_reachable(client: &NvueClient, path: &str, timeout: Duration) -> bool {
    client.probe(path, timeout).await.is_ok()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SwitchSerialUpdatePlan {
    target: String,
    file_path: String,
    remote_dir: String,
    remote_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SwitchSerialAction {
    target: String,
    action_id: String,
    final_state: String,
}

/// Summary returned after a switch NVUE power-cycle action is accepted.
#[derive(Debug, Clone)]
pub struct SwitchPowerCycleSummary {
    /// NVUE action id created by the power-cycle request.
    pub action_id: String,
    /// Queried action status/details returned by the NVUE action endpoint.
    pub details: Value,
}

impl SwitchPowerCycleSummary {
    /// Return the action details with secret-like fields redacted.
    pub fn redacted_details(&self) -> Value {
        NvUtils::redact_secret_json_value(&self.details)
    }
}

/// Failure cases for the switch NVUE power-cycle activation path.
#[derive(Debug, Clone)]
pub enum SwitchPowerCycleError {
    /// POST `/nvue_v1/system` failed before an action was created.
    PostFailed {
        /// Structured response details from the failed POST.
        details: Value,
        /// Raw response body captured from the failed POST.
        body: String,
    },
    /// POST succeeded but the response did not contain an NVUE action id.
    MissingActionId {
        /// Structured response details from the POST.
        details: Value,
        /// Raw response body from the POST.
        body: String,
    },
    /// The action id was created but its status could not be queried.
    ActionStatusUnavailable {
        /// NVUE action id being monitored.
        action_id: String,
        /// Structured status-query failure details.
        details: Value,
    },
    /// The NVUE action reached a failed state.
    ActionFailed {
        /// NVUE action id that failed.
        action_id: String,
        /// Structured failed action details.
        details: Value,
    },
}

impl SwitchPowerCycleError {
    /// Return the NVUE action id when the failure happened after action creation.
    pub fn action_id(&self) -> Option<&str> {
        match self {
            Self::ActionStatusUnavailable { action_id, .. }
            | Self::ActionFailed { action_id, .. } => Some(action_id),
            Self::PostFailed { .. } | Self::MissingActionId { .. } => None,
        }
    }

    /// Return a sanitized human-readable activation failure message.
    pub fn message(&self) -> String {
        match self {
            Self::PostFailed { details, body } => format!(
                "activation command {POWER_CYCLE_COMMAND} failed while posting /nvue_v1/system: {}",
                response_summary(details, body)
            ),
            Self::MissingActionId { details, body } => format!(
                "missing NVUE action id in POST /nvue_v1/system response: {}",
                response_summary(details, body)
            ),
            Self::ActionStatusUnavailable { details, .. } => format!(
                "activation command {POWER_CYCLE_COMMAND} could not query NVUE action status: {}",
                response_summary(details, "")
            ),
            Self::ActionFailed { details, .. } => format!(
                "activation command {POWER_CYCLE_COMMAND} failed while monitoring NVUE action: {}",
                response_summary(details, "")
            ),
        }
    }
}

/// Extract the NVUE action id from the different response shapes switches return.
fn nvue_action_id(details: &Value, body: &str) -> Option<String> {
    if let Some(id) = details.as_str().map(str::trim).filter(|id| !id.is_empty()) {
        return Some(id.to_string());
    }

    for key in ["id", "Id", "action_id", "actionId", "job_id", "JobId"] {
        if let Some(id) = details.get(key).and_then(Value::as_str) {
            let id = id.trim();
            if !id.is_empty() {
                return Some(id.to_string());
            }
        }
    }

    for key in ["@odata.id", "href", "location"] {
        if let Some(id) = details.get(key).and_then(Value::as_str) {
            let id = id.trim().trim_end_matches('/');
            if !id.is_empty() {
                return Some(id.rsplit('/').next().unwrap_or(id).to_string());
            }
        }
    }

    let body = body.trim().trim_matches('"');
    if body.is_empty() || body.starts_with('{') || body.starts_with('[') {
        None
    } else {
        Some(body.to_string())
    }
}

/// Produce a redacted response summary for switch update/activation errors.
fn response_summary(details: &Value, body: &str) -> String {
    let redacted = NvUtils::redact_secret_json_value(details);
    let has_details = match &redacted {
        Value::Null => false,
        Value::Object(map) => !map.is_empty(),
        _ => true,
    };
    if has_details {
        return redacted.to_string();
    }

    let body = NvUtils::sanitize_log(body.trim());
    if body.is_empty() {
        "{}".to_string()
    } else {
        body
    }
}

/// Append a failed NVUE action record to CLI/RMS JSON output.
fn push_switch_action_failure(json_dict: &mut Value, job_id: &str, details: &Value) {
    if let Some(output) = switch_output_mut(json_dict) {
        output.push(json!({
            "Id": job_id,
            "action_id": job_id,
            "error": NvUtils::redact_secret_json_value(details),
        }));
    }
}

/// Return true when a JSON value carries useful switch progress details.
fn switch_value_has_content(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
        _ => true,
    }
}

/// Ensure switch JSON output has an `Output` array and return it.
fn switch_output_mut(json_dict: &mut Value) -> Option<&mut Vec<Value>> {
    if !json_dict.get("Output").is_some_and(Value::is_array) {
        json_dict["Output"] = json!([]);
    }
    json_dict.get_mut("Output").and_then(Value::as_array_mut)
}

/// Append a structured switch workflow progress event for RMS and JSON CLI mode.
fn push_switch_progress_event(
    json_dict: &mut Value,
    stage: &str,
    target: Option<&str>,
    action_id: Option<&str>,
    state: Option<&str>,
    status: Option<&str>,
    details: Option<Value>,
) {
    let Some(output) = switch_output_mut(json_dict) else {
        return;
    };

    let mut event = serde_json::Map::new();
    event.insert("stage".to_string(), json!(stage));
    if let Some(target) = target.map(str::trim).filter(|target| !target.is_empty()) {
        event.insert("target".to_string(), json!(target));
    }
    if let Some(action_id) = action_id
        .map(str::trim)
        .filter(|action_id| !action_id.is_empty())
    {
        event.insert("Id".to_string(), json!(action_id));
        event.insert("action_id".to_string(), json!(action_id));
    }
    if let Some(state) = state.map(str::trim).filter(|state| !state.is_empty()) {
        event.insert("state".to_string(), json!(state));
    }
    if let Some(status) = status.map(str::trim).filter(|status| !status.is_empty()) {
        event.insert("status".to_string(), json!(status));
    }
    if let Some(details) = details {
        let details = NvUtils::redact_secret_json_value(&details);
        if switch_value_has_content(&details) {
            event.insert("details".to_string(), details);
        }
    }

    output.push(Value::Object(event));
}

/// Record a sanitized switch workflow failure in the CLI-compatible JSON shape.
fn push_switch_update_failure(json_dict: &mut Value, message: &str, details: Value) {
    let message = NvUtils::sanitize_log(message);
    json_dict["Error Code"] = json!(1);

    if !json_dict.get("Error").is_some_and(Value::is_array) {
        json_dict["Error"] = json!([]);
    }
    if let Some(errors) = json_dict.get_mut("Error").and_then(Value::as_array_mut) {
        if !errors.iter().any(|value| value.as_str() == Some(&message)) {
            errors.push(Value::String(message.clone()));
        }
    }

    if let Some(output) = switch_output_mut(json_dict) {
        output.push(json!({
            "error": NvUtils::redact_secret_json_value(&details),
        }));
    }
}

// ---------------------------------------------------------------------------
// GB200SwitchRFTarget
// ---------------------------------------------------------------------------

/// Platform-specific RFTarget for GB200, GB300, and VR network switches.
///
/// Uses NVUE REST API rather than standard Redfish push URI or multipart
/// upload. Task monitoring uses a switch-specific task format.
pub struct GB200SwitchRFTarget {
    /// BMC connection handle (NVSwitch access type).
    pub bmc_access: BmcAccess,
    /// Component names treated as fungible (switches typically have none).
    pub fungible_components: Vec<String>,
    /// Message displayed upon successful update completion.
    pub update_completion_msg: String,
    /// Whether the progress table header has been printed during monitoring.
    pub progress_table_header_printed: bool,
    /// Optional platform configuration dictionary.
    pub config_dict: Option<Value>,
}

impl GB200SwitchRFTarget {
    /// Create a new `GB200SwitchRFTarget` with sensible defaults.
    pub fn new(bmc_access: BmcAccess, config_dict: Option<Value>) -> Self {
        Self {
            bmc_access,
            fungible_components: Vec::new(),
            update_completion_msg:
                "Switch firmware update complete. Please reboot the switch to activate.".to_string(),
            progress_table_header_printed: false,
            config_dict,
        }
    }

    /// Launch and query the switch NVUE power-cycle activation action.
    ///
    /// This posts the `@power-cycle` action to `/nvue_v1/system`, extracts the
    /// returned action id, then queries the action endpoint with retries. Query
    /// failure or an already-failed action status is returned as an error.
    pub async fn run_nvue_power_cycle(
        &self,
    ) -> std::result::Result<SwitchPowerCycleSummary, SwitchPowerCycleError> {
        self.run_nvue_power_cycle_with_access(SwitchNvueAccess::Legacy)
            .await
    }

    pub(crate) async fn run_nvue_power_cycle_with_client(
        &self,
        client: &NvueClient,
    ) -> std::result::Result<SwitchPowerCycleSummary, SwitchPowerCycleError> {
        self.run_nvue_power_cycle_with_access(SwitchNvueAccess::Hosted(client))
            .await
    }

    async fn run_nvue_power_cycle_with_access(
        &self,
        access: SwitchNvueAccess<'_>,
    ) -> std::result::Result<SwitchPowerCycleSummary, SwitchPowerCycleError> {
        let payload = json!({
            "@power-cycle": {
                "state": "start",
                "parameters": { "force": true }
            }
        });

        let mut quiet_json = json!({"Error": [], "Error Code": 0, "Output": []});
        let (post_ok, post_details, post_body) = access
            .post(
                &self.bmc_access,
                "/nvue_v1/system",
                &payload,
                POWER_CYCLE_POST_TIMEOUT_SECS,
                Some(&mut quiet_json),
            )
            .await;

        if !post_ok {
            return Err(SwitchPowerCycleError::PostFailed {
                details: post_details,
                body: post_body,
            });
        }

        let action_id = nvue_action_id(&post_details, &post_body).ok_or(
            SwitchPowerCycleError::MissingActionId {
                details: post_details,
                body: post_body,
            },
        )?;

        let details = self.query_power_cycle_action(access, &action_id).await?;

        Ok(SwitchPowerCycleSummary { action_id, details })
    }

    async fn query_power_cycle_action(
        &self,
        access: SwitchNvueAccess<'_>,
        action_id: &str,
    ) -> std::result::Result<Value, SwitchPowerCycleError> {
        let (status, details) = self
            .get_job_status_with_retry(
                access,
                action_id,
                POWER_CYCLE_ACTION_RETRIES,
                POWER_CYCLE_ACTION_RETRY_INTERVAL_SECS,
                true,
            )
            .await;

        if !status {
            return Err(SwitchPowerCycleError::ActionStatusUnavailable {
                action_id: action_id.to_string(),
                details,
            });
        }

        if Self::task_is_failed(&details) {
            return Err(SwitchPowerCycleError::ActionFailed {
                action_id: action_id.to_string(),
                details,
            });
        }

        Ok(details)
    }

    fn bundle_to_target() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("sbios", "bios");
        m.insert("bmc", "bmc");
        m.insert("smr", "fpga");
        m.insert("cpld", "cpld1");
        m.insert("erot", "erot");
        m.insert("sma", "sma");
        m
    }

    fn ap_file_ext() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("bios", ".fwpkg");
        m.insert("bmc", ".fwpkg");
        m.insert("cpld1", ".vme");
        m.insert("erot", ".fwpkg");
        m.insert("fpga", ".fwpkg");
        m.insert("sma", ".fwpkg");
        m
    }

    /// Find the extracted firmware file for a given target component from the
    /// unpacked package dictionary.
    fn get_update_file(
        target_comp: &str,
        unpack_dict: &HashMap<String, Vec<String>>,
    ) -> Option<String> {
        let b2t = Self::bundle_to_target();
        for (bundle_name, ap_data) in unpack_dict {
            let mut ap = bundle_name.split(':').next().unwrap_or(bundle_name);
            if ap.contains(',') {
                ap = bundle_name.split(',').next().unwrap_or(ap);
            }
            let lower = ap.to_lowercase();
            let target_name = b2t
                .get(lower.as_str())
                .unwrap_or(&lower.as_str())
                .to_string();
            if target_comp.to_lowercase().contains(&target_name) {
                return ap_data.get(1).cloned();
            }
        }
        None
    }

    /// Determine the upload subdirectory for the given component.
    fn upload_subdir(ap_name: &str) -> String {
        if ap_name == "cpld1" {
            "cpld".to_string()
        } else {
            ap_name.to_string()
        }
    }

    /// Build the upload/install plan for a serial switch component update.
    ///
    /// CPLD uploads use a shared remote subdirectory while fwpkg-style targets
    /// reuse the original package path as the local upload file.
    fn serial_update_plan(
        target: &str,
        recipe_path: &str,
        unpack_dict: &HashMap<String, Vec<String>>,
    ) -> Option<SwitchSerialUpdatePlan> {
        let target_lower = target.to_lowercase();
        let file_ext_map = Self::ap_file_ext();
        let expected_ext = file_ext_map.get(target_lower.as_str()).unwrap_or(&".bin");

        let file_path = if *expected_ext != ".fwpkg" {
            Self::get_update_file(&target_lower, unpack_dict)?
        } else {
            recipe_path.to_string()
        };

        let subdir = Self::upload_subdir(&target_lower);
        let remote_dir = format!("{}{}/", DEST_UPLOAD_PATH, subdir);
        let local_filename = file_path.rsplit('/').next().unwrap_or(&file_path);
        let remote_name = local_filename.replace(".bin", expected_ext);

        Some(SwitchSerialUpdatePlan {
            target: target.to_string(),
            file_path,
            remote_dir,
            remote_name,
        })
    }

    /// Build the per-component install URL from an uploaded firmware path.
    fn install_url_for_uploaded_path(target: &str, uploaded_path: &str) -> String {
        let file_name = uploaded_path.rsplit('/').next().unwrap_or(uploaded_path);
        format!(
            "{}/{}/files/{}",
            PER_COMPONENT_UPDATE_URL, target, file_name
        )
    }

    /// Get task status from the NVUE action endpoint.
    async fn get_job_status_with_retry(
        &self,
        access: SwitchNvueAccess<'_>,
        task_id: &str,
        max_retries: u32,
        interval_secs: u64,
        is_json: bool,
    ) -> (bool, Value) {
        let url = format!("{}/{}", NVUE_ACTION_URL, task_id);

        for attempt in 0..max_retries {
            let (status, resp_dict) = access.get(&self.bmc_access, &url, 1200, None).await;

            if status {
                return (true, resp_dict);
            }

            if attempt < max_retries - 1 {
                if !is_json {
                    println!("Retrying Task Status Request: {}", task_id);
                }

                tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
            }
        }

        let (_, resp) = access.get(&self.bmc_access, &url, 1200, None).await;

        (false, resp)
    }

    async fn get_system_rebooted_status(
        &self,
        access: SwitchNvueAccess<'_>,
        reboot_eta: u64,
    ) -> bool {
        if matches!(access, SwitchNvueAccess::Legacy) {
            return self.bmc_access.get_system_rebooted_status(reboot_eta).await;
        }

        let SwitchNvueAccess::Hosted(client) = access else {
            return false;
        };

        let polling_timeout = std::time::Duration::from_secs(reboot_eta * 60);
        let poll_interval = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();
        let mut system_rebooted = false;

        loop {
            let reachable = hosted_transport_reachable(
                client,
                "/nvue_v1/platform/firmware",
                Duration::from_secs(30),
            )
            .await;

            if reachable {
                if system_rebooted {
                    return true;
                }

                tokio::time::sleep(poll_interval).await;
            } else {
                system_rebooted = true;
            }

            if start.elapsed() >= polling_timeout {
                return false;
            }
        }
    }

    async fn get_nvue_firmware_inventory(
        &self,
        access: SwitchNvueAccess<'_>,
        json_output: Option<&mut Value>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        let (status, response) = access
            .get(
                &self.bmc_access,
                "/nvue_v1/platform/firmware",
                120,
                json_output,
            )
            .await;

        let mut inventory = serde_json::Map::new();

        if let Some(components) = status.then_some(&response).and_then(Value::as_object) {
            for (component_name, component) in components {
                let version = component
                    .get("actual-firmware")
                    .or_else(|| component.get("version"))
                    .or_else(|| component.get("Version"))
                    .and_then(Value::as_str)
                    .unwrap_or("N/A");

                let mut entry = json!({
                    "Id": component_name,
                    "Version": version,
                    "Updateable": true,
                });

                if let Some(health) = component
                    .pointer("/Status/Health")
                    .or_else(|| component.get("Health"))
                    .and_then(Value::as_str)
                {
                    entry["Status"] = json!({"Health": health});
                }

                inventory.insert(component_name.clone(), entry);
            }
        }

        (status, 0, inventory)
    }

    /// Query task status and print results.
    async fn get_task_status(
        &self,
        access: SwitchNvueAccess<'_>,
        task_id: &str,
        json_dict: Option<&mut Value>,
    ) -> (i32, String, Option<Value>) {
        let is_json = json_dict.is_some();
        let (status, resp_dict) = self
            .get_job_status_with_retry(access, task_id, 3, 5, is_json)
            .await;

        let job_state = if status {
            Self::task_state(&resp_dict)
        } else {
            "error".to_string()
        };

        if !status || Self::task_is_failed(&resp_dict) {
            if let Some(jd) = json_dict {
                if let Some(output) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                    output.push(NvUtils::redact_secret_json_value(&resp_dict));
                }
            } else {
                Util::bail_nvfwupd(
                    1,
                    &format!(
                        "Failure status for job {}: Error {}",
                        task_id,
                        NvUtils::redacted_json_pretty_4space(&resp_dict)
                    ),
                    BailAction::DoNothing,
                    None,
                );
            }
            return (1, job_state, Some(resp_dict));
        }

        if let Some(jd) = json_dict {
            if let Some(output) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(NvUtils::redact_secret_json_value(&resp_dict));
            }
        } else {
            println!("Status for Job Id {}:", task_id);
            self.print_task_completion_inner(&resp_dict);
        }

        (0, job_state, Some(resp_dict))
    }

    fn print_task_completion_inner(&self, task_dict: &Value) {
        println!("{}", NvUtils::redacted_json_pretty_4space(task_dict));
        println!();
    }

    /// Normalize NVUE task/action state fields into a lowercase state string.
    fn task_state(task_dict: &Value) -> String {
        task_dict
            .get("state")
            .or_else(|| task_dict.get("status"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_ascii_lowercase()
    }

    /// Return the human status/detail text carried by an NVUE task response.
    fn task_status_text(task_dict: &Value) -> String {
        task_dict
            .get("status")
            .or_else(|| task_dict.get("detail"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }

    /// Extract progress from the different percent fields seen across NVUE responses.
    fn task_percent(task_dict: &Value) -> i64 {
        task_dict
            .get("percentage")
            .or_else(|| task_dict.get("percent"))
            .or_else(|| task_dict.get("PercentComplete"))
            .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|n| n as i64)))
            .unwrap_or(0)
    }

    /// Detect action-error text that actually means the switch is rebooting.
    fn task_status_indicates_reboot(task_status: &str) -> bool {
        let lower = task_status.to_ascii_lowercase();
        lower.contains("reboot")
            || lower.contains("offline during reboot")
            || lower.contains("system is offline")
    }

    /// Decide whether an NVUE task response should fail the firmware workflow.
    ///
    /// `action_error` is tolerated once progress reaches 100 percent, or earlier
    /// when the status text indicates the expected reboot/offline transition.
    fn task_is_failed(task_dict: &Value) -> bool {
        let state = Self::task_state(task_dict);
        let status = Self::task_status_text(task_dict);
        state.contains("failed")
            || state == "action_failed"
            || (state.contains("error") && state != "action_error")
            || (state == "action_error"
                && Self::task_percent(task_dict) < 100
                && !Self::task_status_indicates_reboot(&status))
    }

    /// Return true for terminal success state labels used by NVUE actions.
    fn task_state_is_success(state: &str) -> bool {
        matches!(
            state,
            "completed" | "complete" | "success" | "action_success"
        )
    }

    /// Decide whether an NVUE task response has reached successful completion.
    fn task_is_success(task_dict: &Value) -> bool {
        let state = Self::task_state(task_dict);
        Self::task_state_is_success(&state)
            || (state == "action_error" && Self::task_percent(task_dict) >= 100)
    }

    async fn monitor_task_completion(
        &self,
        access: SwitchNvueAccess<'_>,
        job_id: &str,
        target: Option<&str>,
        job_status: &str,
        task_status: &str,
        task_state: &str,
        background: bool,
        mut json_dict: Option<&mut Value>,
    ) -> (i32, String) {
        let mut final_state = task_state.to_string();
        let is_json = json_dict.is_some();

        if background {
            return (0, final_state);
        }

        if job_status.contains("error") {
            if let Some(output) = json_dict.as_deref_mut() {
                push_switch_progress_event(
                    output,
                    "install",
                    target,
                    Some(job_id),
                    Some("error"),
                    Some(job_status),
                    None,
                );
            }
            return (1, final_state);
        }

        if Self::task_state_is_success(task_state) {
            return (0, final_state);
        }

        if Self::task_status_indicates_reboot(task_status) {
            let reboot_status = self.get_system_rebooted_status(access, 4).await;

            if !reboot_status {
                if let Some(output) = json_dict.as_deref_mut() {
                    push_switch_progress_event(
                        output,
                        "install",
                        target,
                        Some(job_id),
                        Some("reboot_incomplete"),
                        Some(task_status),
                        None,
                    );
                }
                if !is_json {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Task {} reboot not complete", job_id),
                        BailAction::DoNothing,
                        None,
                    );
                }
                return (1, final_state);
            }
            return (0, final_state);
        }

        let mut last_progress_state = String::new();
        let mut last_progress_status = String::new();

        loop {
            let (status, task_dict) = self
                .get_job_status_with_retry(access, job_id, 3, 5, is_json)
                .await;

            if !status {
                if let Some(output) = json_dict.as_deref_mut() {
                    push_switch_progress_event(
                        output,
                        "install",
                        target,
                        Some(job_id),
                        Some("status_unavailable"),
                        None,
                        Some(task_dict.clone()),
                    );
                    push_switch_action_failure(output, job_id, &task_dict);
                }
                if !is_json {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Task {} failed", job_id),
                        BailAction::DoNothing,
                        None,
                    );
                }
                return (1, final_state);
            }

            final_state = Self::task_state(&task_dict);
            let current_status = Self::task_status_text(&task_dict);
            if let Some(output) = json_dict.as_deref_mut() {
                let progress_changed =
                    final_state != last_progress_state || current_status != last_progress_status;
                if progress_changed
                    || Self::task_is_failed(&task_dict)
                    || Self::task_is_success(&task_dict)
                {
                    push_switch_progress_event(
                        output,
                        "install",
                        target,
                        Some(job_id),
                        Some(&final_state),
                        Some(&current_status),
                        Some(task_dict.clone()),
                    );
                    last_progress_state.clone_from(&final_state);
                    last_progress_status.clone_from(&current_status);
                }
            }

            if !is_json {
                println!("Status for Job Id {}:", job_id);
                self.print_task_completion_inner(&task_dict);
            }

            if Self::task_is_failed(&task_dict) {
                if let Some(output) = json_dict.as_deref_mut() {
                    push_switch_action_failure(output, job_id, &task_dict);
                }
                if !is_json {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Task {} failed", job_id),
                        BailAction::DoNothing,
                        None,
                    );
                }
                return (1, final_state);
            }

            if Self::task_is_success(&task_dict) {
                return (0, final_state);
            }

            if Self::task_status_indicates_reboot(&current_status) {
                let reboot_status = self.get_system_rebooted_status(access, 4).await;

                if !reboot_status {
                    if let Some(output) = json_dict.as_deref_mut() {
                        push_switch_action_failure(output, job_id, &task_dict);
                    }
                    if !is_json {
                        Util::bail_nvfwupd(
                            1,
                            &format!("Task {} reboot not complete", job_id),
                            BailAction::DoNothing,
                            None,
                        );
                    }
                    return (1, final_state);
                }
                return (0, final_state);
            }

            if !PENDING_TASK_STATES.contains(&final_state.as_str()) {
                if let Some(output) = json_dict.as_deref_mut() {
                    push_switch_action_failure(output, job_id, &task_dict);
                }
                if !is_json {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Task {} ended in unexpected state {}", job_id, final_state),
                        BailAction::DoNothing,
                        None,
                    );
                }
                return (1, final_state);
            }

            tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        }
    }

    /// Translate a PLDM bundle AP name (e.g. "ERoT,BMC::,0x4d35368b") into the
    /// target-side component name (e.g. "erot").
    fn get_target_apname(bundle_ap: &str) -> String {
        let mut ap_name: &str = bundle_ap.split(':').next().unwrap_or(bundle_ap);
        if ap_name.contains(',') {
            ap_name = bundle_ap.split(',').next().unwrap_or(ap_name);
        }
        let lower = ap_name.to_lowercase();
        Self::bundle_to_target()
            .get(lower.as_str())
            .unwrap_or(&lower.as_str())
            .to_string()
    }

    /// Infer the switch platform family from top-level PLDM package keys.
    fn get_platform_type(pldm_version_dict: &Value) -> Option<&'static str> {
        let outer = pldm_version_dict.as_object()?;
        for key in outer.keys() {
            if key.contains("GB200") {
                return Some("GB200");
            }
            if key.contains("GB300") {
                return Some("GB300");
            }
            if key.contains("VRNVL144") || key.contains("VR-NVL72") || key.contains("VRNVL72") {
                return Some("VRNVL72");
            }
        }
        None
    }

    /// Normalize component name for version lookup.
    /// For example, on GB200, cpld1-cpld4 all normalise to "cpld1" because
    /// the package stores a single combined CPLD version string.
    fn normalize_component_name(ap_name: &str, platform_type: &str) -> String {
        let lower = ap_name.to_lowercase();
        match platform_type {
            "GB200" if ["cpld1", "cpld2", "cpld3", "cpld4"].contains(&lower.as_str()) => {
                "cpld1".to_string()
            }
            "GB300" if ["cpld1", "cpld2", "cpld3"].contains(&lower.as_str()) => "cpld1".to_string(),
            "VRNVL72" if ["cpld1", "cpld2"].contains(&lower.as_str()) => "cpld1".to_string(),
            _ => lower,
        }
    }

    /// Extract a component version from PLDM package data after name normalization.
    fn extract_component_version(
        pldm_version_dict: &Value,
        ap_name: &str,
        platform_type: &str,
    ) -> Option<String> {
        let lookup_name = Self::normalize_component_name(ap_name, platform_type);
        let outer = pldm_version_dict.as_object()?;
        for (_pkg_name, pkg_dict_val) in outer {
            if let Some(pkg_dict) = pkg_dict_val.as_object() {
                for (ap_pkg, ap_data) in pkg_dict {
                    let bundle_ap_name = Self::get_target_apname(ap_pkg);
                    if lookup_name.contains(&bundle_ap_name)
                        || bundle_ap_name.contains(&lookup_name)
                    {
                        if let Some(arr) = ap_data.as_array() {
                            return arr.first().and_then(|v| v.as_str()).map(|s| s.to_string());
                        }
                    }
                }
            }
        }
        None
    }

    /// Return true when the component is one of the platform's split CPLD views.
    fn is_cpld_component(ap_name: &str, platform_type: &str) -> bool {
        let lower = ap_name.to_lowercase();
        match platform_type {
            "GB200" => ["cpld1", "cpld2", "cpld3", "cpld4"].contains(&lower.as_str()),
            "GB300" => ["cpld1", "cpld2", "cpld3"].contains(&lower.as_str()),
            "VRNVL72" => ["cpld1", "cpld2"].contains(&lower.as_str()),
            _ => false,
        }
    }

    /// Return the number of underscore-delimited CPLD version segments per platform.
    fn expected_cpld_segments(platform_type: &str) -> usize {
        match platform_type {
            "GB200" => 8,
            "GB300" => 6,
            "VRNVL72" => 4,
            _ => 0,
        }
    }

    /// Format one logical CPLD component from the combined package CPLD version.
    fn format_cpld_version(ap_name: &str, platform_type: &str, parts: &[&str]) -> String {
        let mapping: &[(&str, usize, usize)] = match platform_type {
            "GB200" => &[
                ("cpld1", 0, 1),
                ("cpld2", 2, 3),
                ("cpld3", 4, 5),
                ("cpld4", 6, 7),
            ],
            "GB300" => &[("cpld1", 0, 1), ("cpld2", 2, 3), ("cpld3", 4, 5)],
            "VRNVL72" => &[("cpld1", 0, 1), ("cpld2", 2, 3)],
            _ => &[],
        };
        let lower = ap_name.to_lowercase();
        for &(name, si, ei) in mapping {
            if lower == name {
                if si < parts.len() && ei < parts.len() {
                    return format!("CPLD{}_{}", parts[si], parts[ei]);
                }
            }
        }
        parts.join("_")
    }

    /// Format package component versions for display/comparison.
    fn format_component_version(ap_version: &str, ap_name: &str, platform_type: &str) -> String {
        if !Self::is_cpld_component(ap_name, platform_type) {
            return ap_version.to_string();
        }
        let parts: Vec<&str> = ap_version.split('_').collect();
        let expected = Self::expected_cpld_segments(platform_type);
        if parts.len() != expected {
            return ap_version.to_string();
        }
        Self::format_cpld_version(ap_name, platform_type, &parts)
    }

    /// Upload a firmware file to the switch via SSH/SFTP.
    ///
    /// When `use_tmp` is true, uploads to `/tmp/` (for VRNVL72 parallel).
    /// Otherwise uploads to `/host/fw-images/{ap_name}/`.
    async fn upload_image(
        &self,
        file_path: &str,
        use_tmp: bool,
        json_dict: bool,
    ) -> Option<String> {
        let upload_path = if use_tmp {
            PARALLEL_UPLOAD_PATH.to_string()
        } else {
            DEST_UPLOAD_PATH.to_string()
        };

        let connection_ip = self.bmc_access.ip.replace('[', "").replace(']', "");
        let ssh_host_key_policy = self.bmc_access.ssh_host_key_policy();
        if let Err(e) = ssh_transport::execute_command_async(
            &connection_ip,
            22,
            &self.bmc_access.user,
            &self.bmc_access.password,
            30,
            &format!("mkdir -p {}", upload_path),
            None,
            ssh_host_key_policy.clone(),
        )
        .await
        {
            if !json_dict {
                println!("SSH mkdir failed for upload: {}", e);
            }
            return None;
        }

        // Build remote file path
        let local_name = std::path::Path::new(file_path)
            .file_name()?
            .to_string_lossy()
            .to_string();
        let remote_file = format!("{}{}", upload_path, local_name);

        if let Err(e) = ssh_transport::upload_file_async(
            &connection_ip,
            22,
            &self.bmc_access.user,
            &self.bmc_access.password,
            300,
            std::path::Path::new(file_path).to_path_buf(),
            remote_file.clone(),
            ssh_host_key_policy,
        )
        .await
        {
            if !json_dict {
                println!("SFTP upload error: {}", e);
            }
            return None;
        }

        if !json_dict {
            println!("Update file {} was uploaded successfully", file_path);
        }
        Some(remote_file)
    }

    /// Try parallel update method for VRNVL72 platforms.
    ///
    /// Returns `(success, task_id_list, err_code)`.
    /// `success=true` means parallel method was used; `false` means
    /// fall back to serial.
    ///
    /// Flow:
    ///   1. Upload firmware to /tmp via SCP
    ///   2. Fetch firmware via NVUE `@fetch` endpoint
    ///   3. Install via NVUE `@install` endpoint
    ///   4. Monitor completion
    async fn try_parallel_update_vrnvl72(
        &mut self,
        access: SwitchNvueAccess<'_>,
        all_targets: &[String],
        recipe_list: &[String],
        cmd_args: &CmdArgs,
        parallel_update: bool,
        force_update: bool,
        json_dict: &mut Option<&mut Value>,
    ) -> (bool, Vec<String>, i32) {
        let is_json = json_dict.is_some();

        // Check that ALL required components are present
        let required: std::collections::HashSet<String> = UPDATE_ORDER_VRNVL72
            .iter()
            .map(|s| s.to_lowercase())
            .collect();
        let provided: std::collections::HashSet<String> =
            all_targets.iter().map(|s| s.to_lowercase()).collect();

        if provided != required {
            if !is_json {
                println!(
                    "Parallel update requires all components. User specified subset of targets."
                );
                println!("Falling back to serial update method.");
            }
            return (false, Vec::new(), 0);
        }

        if !is_json {
            println!("Attempting parallel update method for VRNVL72...");
        }

        let file_path = &recipe_list[0];

        // Step 1: Upload to /tmp
        if !is_json {
            println!("Step 1: Uploading firmware package to /tmp...");
        }
        let dest_path = match self.upload_image(file_path, true, is_json).await {
            Some(p) => p,
            None => {
                if !is_json {
                    println!(
                        "Parallel file upload failed. Falling back to serial update method..."
                    );
                }
                return (false, Vec::new(), 1);
            }
        };

        let file_name = dest_path.rsplit('/').next().unwrap_or(&dest_path);

        // Step 2: Fetch firmware via NVUE
        if !is_json {
            println!(
                "Step 2: Fetching firmware file for all components: {:?}",
                all_targets
            );
        }

        let fetch_json = json!({
            "@fetch": {
                "state": "start",
                "parameters": {
                    "remote-url": format!("file://{}", dest_path)
                }
            }
        });

        let (status, _err_dict, msg) = access
            .post(
                &self.bmc_access,
                PARALLEL_FETCH_URL,
                &fetch_json,
                1200,
                json_dict.as_deref_mut(),
            )
            .await;

        if !status {
            if !is_json {
                println!("Parallel fetch failed. Falling back to serial update method...");
            }
            return (false, Vec::new(), 1);
        }

        let fetch_job_id = msg.trim().to_string();
        if fetch_job_id.is_empty() {
            if !is_json {
                println!("No job ID in parallel fetch response. Falling back to serial...");
            }
            return (false, Vec::new(), 1);
        }

        if !is_json {
            println!("Parallel fetch task was created with ID {}", fetch_job_id);
        }

        // Poll fetch task to completion.
        // Python uses skip_output=True to suppress both JSON append and
        // stdout printing.  We call get_job_status_with_retry directly
        // instead of get_task_status to avoid printing.
        loop {
            let (status, resp_dict) = self
                .get_job_status_with_retry(access, &fetch_job_id, 3, 5, is_json)
                .await;
            if !status {
                if !is_json {
                    println!(
                        "Fetch task {} failed. Falling back to serial...",
                        fetch_job_id
                    );
                }
                return (false, Vec::new(), 1);
            }
            let state = resp_dict
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !PENDING_TASK_STATES.contains(&state) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        if !is_json {
            println!("Parallel fetch completed successfully");
        }

        // Step 3: Install firmware via NVUE parallel install
        if !is_json {
            println!(
                "Step 3: Installing firmware for all components: {:?}",
                all_targets
            );
        }

        let install_url = format!("{}/{}", PARALLEL_UPDATE_URL_PREFIX, file_name);
        let install_json = json!({
            "@install": {"state": "start", "parameters": {"force": force_update}}
        });

        let (status, _err_dict, msg) = access
            .post(
                &self.bmc_access,
                &install_url,
                &install_json,
                1200,
                json_dict.as_deref_mut(),
            )
            .await;

        if !status {
            if !is_json {
                println!("Parallel install API failed. Falling back to serial...");
            }
            return (false, Vec::new(), 1);
        }

        let job_id = msg.trim().to_string();
        if job_id.is_empty() {
            if !is_json {
                println!("No job ID in parallel install response. Falling back to serial...");
            }
            return (false, Vec::new(), 1);
        }

        if let Some(ref mut jd) = json_dict {
            if let Some(output) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(json!({"Id": &job_id}));
            }
        }

        if !is_json {
            println!("Parallel FW update task was created with ID {}", job_id);
        }

        let (ret_code, job_status, task_dict) = self
            .get_task_status(access, &job_id, json_dict.as_deref_mut())
            .await;

        if task_dict.is_none() || ret_code == 1 {
            if !is_json {
                println!("Parallel FW update task failed with ID {}", job_id);
                println!("Falling back to serial update method...");
            }
            return (false, Vec::new(), 1);
        }

        let task_status = task_dict
            .as_ref()
            .and_then(|d| d.get("status"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let task_state = task_dict
            .as_ref()
            .and_then(|d| d.get("state"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut task_id_list = vec![job_id.clone()];

        if parallel_update {
            if !is_json {
                println!("Parallel update method initiated successfully for all components");
            }
            return (true, task_id_list, 0);
        }

        // Monitor task completion
        let (err_code, _final_state) = self
            .monitor_task_completion(
                access,
                &job_id,
                Some("parallel"),
                &job_status,
                &task_status,
                &task_state,
                cmd_args.background,
                json_dict.as_deref_mut(),
            )
            .await;

        if !is_json {
            if err_code == 0 {
                println!("Parallel update method completed successfully for all components.");
            } else {
                println!("Parallel update method completed, but reported errors.");
            }
        }

        (true, task_id_list, err_code)
    }
}

impl GB200SwitchRFTarget {
    async fn get_firmware_inventory_with_access(
        &self,
        access: SwitchNvueAccess<'_>,
        _trace: crate::util::TraceFlags,
        json_output: Option<&mut Value>,
        _model: Option<&str>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        self.get_nvue_firmware_inventory(access, json_output).await
    }

    // ------------------------------------------------------------------
    // Overrides
    // ------------------------------------------------------------------

    /// GB200Switch: creates fixed component target JSONs for
    /// BMC, ERoT, CPLD1, FPGA, BIOS.
    async fn make_update_target_json_impl(&self, dir_path: &str) -> bool {
        if let Err(e) = tokio::fs::create_dir_all(dir_path).await {
            tracing::warn!("Error creating directory {}: {}", dir_path, e);
            return false;
        }
        let members = ["BMC", "ERoT", "CPLD1", "FPGA", "BIOS"];
        let mut file_list: Vec<String> = Vec::new();
        for each in &members {
            let target_json = json!({"Targets": [each]});
            let fp = format!("{}/{}.json", dir_path, each);
            if tokio::fs::write(&fp, serde_json::to_string_pretty(&target_json).unwrap())
                .await
                .is_ok()
            {
                file_list.push(fp);
            }
        }
        println!("Created following update parameter files:");
        for f in &file_list {
            println!("{}", f);
        }
        true
    }

    /// Python returns `(False, None)` silently — no bail.
    async fn factory_reset_impl(&mut self, _reset_params: Option<&Value>) -> (bool, Value) {
        (false, Value::Null)
    }

    /// Python returns `(False, None)` silently — no bail.
    async fn background_copy_impl(&self, _copy_parameters: &str) -> (bool, Value) {
        (false, Value::Null)
    }

    async fn run_oob_activation_with_access(
        &mut self,
        access: SwitchNvueAccess<'_>,
        cmd_args: &CmdArgs,
    ) -> i32 {
        let mut quiet_json = json!({"Error": [], "Error Code": 0, "Output": []});
        if cmd_args.cmd != POWER_CYCLE_COMMAND {
            Util::bail_nvfwupd(
                1,
                &format!("Activation command {} not supported", cmd_args.cmd),
                BailAction::DoNothing,
                cmd_args.quiet.then_some(&quiet_json),
            );
            return 1;
        }

        match self.run_nvue_power_cycle_with_access(access).await {
            Ok(summary) => {
                if !cmd_args.quiet {
                    println!("Power cycle task was created with ID {}", summary.action_id);
                    println!("Status for Job Id {}:", summary.action_id);
                    self.print_task_completion_inner(&summary.details);
                }
                0
            }
            Err(error) => {
                Util::bail_nvfwupd(
                    1,
                    &error.message(),
                    BailAction::DoNothing,
                    cmd_args.quiet.then_some(&quiet_json),
                );
                1
            }
        }
    }

    /// Override task service URI for switch-specific task format.
    ///
    /// Switches use a flat task status endpoint rather than the standard
    /// Redfish `/redfish/v1/TaskService/Tasks/<id>` hierarchy.
    fn get_task_service_uri_impl(&self, task_id: &str) -> String {
        if task_id.is_empty() {
            return SWITCH_TASK_URI.to_string();
        }
        format!("{}/{}", SWITCH_TASK_URI, task_id)
    }

    /// Override update URI to use the switch REST API endpoint.
    fn get_update_uri_impl(&self, _update_service_response: &Value) -> String {
        SWITCH_UPDATE_URI.to_string()
    }

    // ------------------------------------------------------------------
    // Switch-specific start_update_monitor override
    // ------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    async fn start_update_monitor_with_access(
        &mut self,
        access: SwitchNvueAccess<'_>,
        recipe_list: &[String],
        pkg_parser: &mut dyn PkgParser,
        cmd_args: &CmdArgs,
        _time_out: u64,
        parallel_update: bool,
        mut json_dict: Option<&mut Value>,
        update_delay: u64,
        _skip_pre_flight_checks: bool,
        _update_precondition_mode: Option<UpdatePreconditionMode>,
    ) -> (i32, Vec<String>) {
        let mut task_id_list: Vec<String> = Vec::new();
        let mut err_code: i32 = 0;

        let (status, msg) = pkg_parser.parse_pkg(&recipe_list[0]).await;
        if !status {
            Util::bail_nvfwupd(
                1,
                &format!("Invalid input file {}", recipe_list[0]),
                BailAction::DoNothing,
                json_dict.as_ref().map(|v| v as &Value),
            );
            return (1, Vec::new());
        }

        pkg_parser.get_unpack_file_dict(&recipe_list[0]).await;
        let unpack_dict = pkg_parser.unpack_file_ap_dict().clone();
        let version_dict = pkg_parser.apname_version_dict_json();
        // Switch NVUE installs must not force update. On some NVOS releases
        // `force=true` can trigger an automatic reboot outside the explicit
        // activation phase, so ignore any ForceUpdate request from hosted
        // callers or CLI special JSON.
        let force_update = false;

        // Priority 1: cmd_args.special -> parse JSON for "Targets".
        let all_targets: Vec<String> = if cmd_args.special.is_some() {
            match self
                .resolve_json_or_file(cmd_args.special.as_deref(), "special")
                .await
            {
                Ok(Some(json_str)) => {
                    let parsed: Value = serde_json::from_str(&json_str).unwrap_or_default();
                    match parsed.get("Targets") {
                        Some(targets) => match targets.as_array() {
                            Some(arr) => arr
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect(),
                            None => {
                                Util::bail_nvfwupd(
                                    1,
                                    "Invalid target input",
                                    BailAction::DoNothing,
                                    json_dict.as_ref().map(|v| v as &Value),
                                );
                                return (1, Vec::new());
                            }
                        },
                        None => Vec::new(),
                    }
                }
                Ok(None) => Vec::new(),
                Err(msg) => {
                    Util::bail_nvfwupd(
                        1,
                        &msg,
                        BailAction::DoNothing,
                        json_dict.as_ref().map(|v| v as &Value),
                    );
                    return (1, Vec::new());
                }
            }
        }
        // Priority 2: config_dict → "UpdateParametersTargets"
        else if let Some(cfg) = self.config_dict.as_ref() {
            if let Some(targets_val) = cfg.get("UpdateParametersTargets") {
                match targets_val.as_array() {
                    Some(arr) => arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect(),
                    None => {
                        Util::bail_nvfwupd(
                            1,
                            "No targets specified for UpdateParametersTargets in config file",
                            BailAction::DoNothing,
                            json_dict.as_ref().map(|v| v as &Value),
                        );
                        return (1, Vec::new());
                    }
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Priority 3: Derive from package contents if not specified above
        let all_targets: Vec<String> = if !all_targets.is_empty() {
            all_targets
        } else {
            let b2t = Self::bundle_to_target();
            let mut targets: Vec<String> = Vec::new();
            for ap_name in unpack_dict.keys() {
                let mut ap: &str = ap_name.split(':').next().unwrap_or(ap_name);
                if ap.contains(',') {
                    ap = ap_name.split(',').next().unwrap_or(ap);
                }
                let lower = ap.to_lowercase();
                let target_name = b2t
                    .get(lower.as_str())
                    .unwrap_or(&lower.as_str())
                    .to_string();
                targets.push(target_name);
            }

            let platform_type = Self::get_platform_type(&version_dict);

            if platform_type == Some("VRNVL72") {
                UPDATE_ORDER_VRNVL72
                    .iter()
                    .filter(|t| targets.contains(&t.to_string()))
                    .map(|t| t.to_uppercase())
                    .collect()
            } else if targets.contains(&"bios".to_string()) {
                UPDATE_ORDER_0002
                    .iter()
                    .filter(|t| targets.contains(&t.to_string()))
                    .map(|t| t.to_uppercase())
                    .collect()
            } else if targets.contains(&"cpld1".to_string()) {
                targets.iter().map(|t| t.to_uppercase()).collect()
            } else {
                UPDATE_ORDER_0000
                    .iter()
                    .filter(|t| targets.contains(&t.to_string()))
                    .map(|t| t.to_uppercase())
                    .collect()
            }
        };

        if all_targets.is_empty() {
            Util::bail_nvfwupd(
                1,
                "Unable to determine update targets",
                BailAction::DoNothing,
                json_dict.as_ref().map(|v| v as &Value),
            );
            return (1, Vec::new());
        }

        if json_dict.is_none() {
            println!("The following targets will be updated {:?}", all_targets);
        }

        if update_delay > 0 {
            if json_dict.is_none() {
                println!(
                    "Waiting {} seconds before beginning update for {:?}",
                    update_delay, all_targets
                );
            }
            tokio::time::sleep(std::time::Duration::from_secs(update_delay)).await;
        }

        // Try parallel update for VRNVL72 platforms
        let platform_type = Self::get_platform_type(&version_dict);
        if platform_type == Some("VRNVL72") {
            let (parallel_success, parallel_tasks, parallel_err) = self
                .try_parallel_update_vrnvl72(
                    access,
                    &all_targets,
                    recipe_list,
                    cmd_args,
                    parallel_update,
                    force_update,
                    &mut json_dict,
                )
                .await;
            if parallel_success {
                return (parallel_err, parallel_tasks);
            }
            // Fall through to serial if parallel failed
        }

        let mut started_actions: Vec<SwitchSerialAction> = Vec::new();
        let mut completed_actions: Vec<SwitchSerialAction> = Vec::new();

        for (idx, target) in all_targets.iter().enumerate() {
            let plan = match Self::serial_update_plan(target, &recipe_list[0], &unpack_dict) {
                Some(plan) => plan,
                None => {
                    if json_dict.is_none() {
                        println!("Could not find a matching firmware file for {}", target);
                    }
                    err_code = 1;
                    continue;
                }
            };

            let is_json = json_dict.is_some();
            if let Some(ref mut jd) = json_dict {
                push_switch_progress_event(
                    jd,
                    "upload",
                    Some(&plan.target),
                    None,
                    Some("running"),
                    None,
                    Some(json!({
                        "target_index": idx + 1,
                        "target_count": all_targets.len(),
                        "local_file": &plan.file_path,
                        "remote_dir": &plan.remote_dir,
                        "remote_name": &plan.remote_name,
                    })),
                );
            }

            let dest_path = match self
                .bmc_access
                .scp_upload_async_result(
                    &plan.file_path,
                    &plan.remote_dir,
                    &plan.remote_name,
                    is_json,
                )
                .await
            {
                Ok(p) => {
                    if let Some(ref mut jd) = json_dict {
                        push_switch_progress_event(
                            jd,
                            "upload",
                            Some(&plan.target),
                            None,
                            Some("success"),
                            None,
                            Some(json!({
                                "target_index": idx + 1,
                                "target_count": all_targets.len(),
                                "local_file": &plan.file_path,
                                "remote_path": &p,
                            })),
                        );
                    }
                    p
                }
                Err(error) => {
                    let message = format!(
                        "Switch firmware upload failed for {}: {}",
                        plan.target, error
                    );
                    if let Some(ref mut json_dict) = json_dict {
                        push_switch_update_failure(
                            json_dict,
                            &message,
                            json!({
                                "stage": "upload",
                                "target": plan.target,
                                "local_file": plan.file_path,
                                "remote_dir": plan.remote_dir,
                                "remote_name": plan.remote_name,
                                "error": error,
                            }),
                        );
                    }
                    return (1, Vec::new());
                }
            };

            if json_dict.is_none() {
                println!("Starting FW update for: {}", plan.target);
            }

            let url = Self::install_url_for_uploaded_path(&plan.target, &dest_path);
            let post_json = json!({
                "@install": {
                    "state": "start",
                    "parameters": {
                        "force": force_update
                    }
                }
            });
            if let Some(ref mut jd) = json_dict {
                push_switch_progress_event(
                    jd,
                    "install",
                    Some(&plan.target),
                    None,
                    Some("posting"),
                    None,
                    Some(json!({
                        "target_index": idx + 1,
                        "target_count": all_targets.len(),
                        "url": &url,
                        "remote_path": &dest_path,
                    })),
                );
            }

            let (post_status, post_details, resp_text) = access
                .post(
                    &self.bmc_access,
                    &url,
                    &post_json,
                    1200,
                    json_dict.as_deref_mut(),
                )
                .await;

            if !post_status {
                Util::bail_nvfwupd(
                    1,
                    &format!("Update failed with status: {}", resp_text),
                    BailAction::DoNothing,
                    json_dict.as_ref().map(|v| v as &Value),
                );
                err_code = 1;
                continue;
            }

            let job_id = nvue_action_id(&post_details, &resp_text)
                .unwrap_or_else(|| resp_text.trim().to_string());
            if job_id.is_empty() {
                Util::bail_nvfwupd(
                    1,
                    "No job ID in response",
                    BailAction::DoNothing,
                    json_dict.as_ref().map(|v| v as &Value),
                );
                err_code = 1;
                continue;
            }

            if let Some(ref mut jd) = json_dict {
                push_switch_progress_event(
                    jd,
                    "install",
                    Some(&plan.target),
                    Some(&job_id),
                    Some("accepted"),
                    None,
                    Some(json!({
                        "target_index": idx + 1,
                        "target_count": all_targets.len(),
                        "url": &url,
                    })),
                );
            }

            if json_dict.is_none() {
                println!("FW update task was created with ID {}", job_id);
            }

            task_id_list.push(job_id.clone());
            started_actions.push(SwitchSerialAction {
                target: plan.target.clone(),
                action_id: job_id.clone(),
                final_state: String::new(),
            });

            if parallel_update {
                continue;
            }

            let (task_err_code, _final_state) = self
                .monitor_task_completion(
                    access,
                    &job_id,
                    Some(&plan.target),
                    "",
                    "",
                    "",
                    cmd_args.background,
                    json_dict.as_deref_mut(),
                )
                .await;
            if task_err_code != 0 {
                err_code = task_err_code;
            } else {
                completed_actions.push(SwitchSerialAction {
                    target: plan.target.clone(),
                    action_id: job_id,
                    final_state: _final_state,
                });
                if completed_actions.len() == started_actions.len()
                    && completed_actions.len() == idx + 1
                    && idx + 1 == all_targets.len()
                {
                    break;
                }
            }
        }

        if err_code == 0
            && !cmd_args.background
            && !parallel_update
            && !started_actions.is_empty()
            && completed_actions.len() == started_actions.len()
        {
            if let Some(ref mut jd) = json_dict {
                let actions = completed_actions
                    .iter()
                    .map(|action| {
                        json!({
                            "target": action.target,
                            "action_id": action.action_id,
                            "state": action.final_state,
                        })
                    })
                    .collect::<Vec<_>>();
                push_switch_progress_event(
                    jd,
                    "complete",
                    None,
                    None,
                    Some("success"),
                    None,
                    Some(json!({ "actions": actions })),
                );
            }
            return (0, task_id_list);
        }

        (err_code, task_id_list)
    }

    // ------------------------------------------------------------------
    // Abstract method implementations
    // ------------------------------------------------------------------

    /// Switch update_component is a no-op.
    /// Python: `return ""` — the switch uses start_update_monitor exclusively
    /// for firmware updates (SSH upload + NVUE REST API).
    async fn update_component_impl(
        &mut self,
        _cmd_args: &CmdArgs,
        _update_uri: &str,
        _update_file: &str,
        _time_out: u64,
        _json_dict: Option<&mut Value>,
        _parallel_update: bool,
    ) -> Option<String> {
        Some(String::new())
    }

    /// Switches do not have fungible components.
    fn is_fungible_component_impl(&self, _component_name: &str) -> bool {
        false
    }

    async fn get_component_version_impl(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        _pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        let platform_type = Self::get_platform_type(pldm_version_dict)?;
        let ap_version =
            Self::extract_component_version(pldm_version_dict, ap_name, platform_type)?;
        Some(Self::format_component_version(
            &ap_version,
            ap_name,
            platform_type,
        ))
    }

    async fn get_identifier_from_chassis_impl(&self, _ap_inv_uri: &str) -> Option<String> {
        None
    }

    fn get_version_sku_impl(
        &self,
        _identifier: &str,
        _pldm_version_dict: &Value,
        _ap_name: &str,
    ) -> Option<String> {
        None
    }

    // ------------------------------------------------------------------
    // NVUE job status overrides
    // ------------------------------------------------------------------
    // GB200Switch uses NVUE REST API endpoints for task management
    // instead of the default Redfish TaskService paths.

    /// Override: delegates to the existing `get_task_status` which uses
    /// NVUE action endpoints instead of Redfish TaskService.
    async fn process_job_status_with_access(
        &self,
        access: SwitchNvueAccess<'_>,
        task_id: &str,
        print_json: Option<&mut Value>,
    ) -> i32 {
        let (ret_val, _state, _resp) = self.get_task_status(access, task_id, print_json).await;
        ret_val
    }

    /// Override: query job status via NVUE `/nvue_v1/action/{task_id}`
    /// without printing results.
    async fn query_job_status_with_access(
        &self,
        access: SwitchNvueAccess<'_>,
        task_id: &str,
        print_json: Option<&mut Value>,
    ) -> (bool, Value) {
        let task_url = format!("{NVUE_ACTION_URL}/{task_id}");

        access
            .get(&self.bmc_access, &task_url, 1200, print_json)
            .await
    }

    /// Override: print previously-acquired NVUE job status.
    ///
    /// Checks `http_status` and `state` from the NVUE response dict
    /// instead of the Redfish `TaskState` field.
    fn print_job_status_impl(
        &self,
        task_id: &str,
        my_dict: &Value,
        status: bool,
        mut print_json: Option<&mut Value>,
    ) -> (i32, Option<String>) {
        let mut job_state = "running".to_string();

        if print_json.is_none() {
            println!("{}", "-".repeat(120));
            println!("Status for Job Id: {}", task_id);
        }

        if status {
            let http_status = my_dict
                .get("http_status")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if !(200..300).contains(&http_status) {
                job_state = "error".to_string();
            } else {
                job_state = my_dict
                    .get("state")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
            }
        } else {
            job_state = "error".to_string();
        }

        if job_state.contains("error") {
            let redacted = NvUtils::redact_secret_json_value(my_dict);
            if let Some(json_dict) = print_json.as_deref_mut() {
                if let Some(output) = json_dict.get_mut("Output").and_then(|v| v.as_array_mut()) {
                    output.push(json!({
                        "Id": task_id,
                        "error": redacted.clone(),
                    }));
                }
            }
            Util::bail_nvfwupd(
                1,
                &format!("Failure status for job {}: Error {:?}", task_id, redacted),
                BailAction::DoNothing,
                print_json.as_deref(),
            );
            return (1, Some(job_state.to_lowercase()));
        }

        if let Some(json_dict) = print_json {
            if let Some(output) = json_dict.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(NvUtils::redact_secret_json_value(my_dict));
            }
        } else {
            self.print_task_completion_inner(my_dict);
        }

        (0, Some(job_state.to_lowercase()))
    }

    pub(crate) async fn get_firmware_inventory_with_client(
        &self,
        client: &NvueClient,
        trace: crate::util::TraceFlags,
        json_output: Option<&mut Value>,
        model: Option<&str>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        self.get_firmware_inventory_with_access(
            SwitchNvueAccess::Hosted(client),
            trace,
            json_output,
            model,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start_update_monitor_with_client(
        &mut self,
        client: &NvueClient,
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
        self.start_update_monitor_with_access(
            SwitchNvueAccess::Hosted(client),
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

    pub(crate) async fn query_job_status_with_client(
        &self,
        client: &NvueClient,
        task_id: &str,
        print_json: Option<&mut Value>,
    ) -> (bool, Value) {
        self.query_job_status_with_access(SwitchNvueAccess::Hosted(client), task_id, print_json)
            .await
    }

    pub(crate) async fn run_oob_activation_with_client(
        &mut self,
        client: &NvueClient,
        cmd_args: &CmdArgs,
    ) -> i32 {
        self.run_oob_activation_with_access(SwitchNvueAccess::Hosted(client), cmd_args)
            .await
    }
}

#[async_trait::async_trait]
impl RFTarget for GB200SwitchRFTarget {
    fn target_access(&self) -> &BmcAccess {
        &self.bmc_access
    }

    fn target_access_mut(&mut self) -> &mut BmcAccess {
        &mut self.bmc_access
    }

    async fn get_firmware_inventory(
        &self,
        trace: crate::util::TraceFlags,
        json_output: Option<&mut Value>,
        model: Option<&str>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        self.get_firmware_inventory_with_access(SwitchNvueAccess::Legacy, trace, json_output, model)
            .await
    }

    fn fungible_components(&self) -> &[String] {
        &self.fungible_components
    }

    fn update_completion_msg(&self) -> &str {
        &self.update_completion_msg
    }

    fn set_update_completion_msg(&mut self, msg: &str) {
        self.update_completion_msg = msg.to_owned();
    }

    fn progress_table_header_printed(&self) -> bool {
        self.progress_table_header_printed
    }

    fn set_progress_table_header_printed(&mut self, printed: bool) {
        self.progress_table_header_printed = printed;
    }

    fn config_dict(&self) -> Option<&Value> {
        self.config_dict.as_ref()
    }

    fn class_name(&self) -> &str {
        "GB200SwitchRFTarget"
    }

    async fn make_update_target_json(&self, dir_path: &str) -> bool {
        self.make_update_target_json_impl(dir_path).await
    }

    async fn factory_reset(&mut self, reset_params: Option<&Value>) -> (bool, Value) {
        self.factory_reset_impl(reset_params).await
    }

    async fn background_copy(&self, copy_parameters: &str) -> (bool, Value) {
        self.background_copy_impl(copy_parameters).await
    }

    async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        self.run_oob_activation_with_access(SwitchNvueAccess::Legacy, cmd_args)
            .await
    }

    fn get_task_service_uri(&self, task_id: &str) -> String {
        self.get_task_service_uri_impl(task_id)
    }

    fn get_update_uri(&self, update_service_response: &Value) -> String {
        self.get_update_uri_impl(update_service_response)
    }

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
        self.start_update_monitor_with_access(
            SwitchNvueAccess::Legacy,
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

    async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        self.update_component_impl(
            cmd_args,
            update_uri,
            update_file,
            time_out,
            json_dict,
            parallel_update,
        )
        .await
    }

    fn is_fungible_component(&self, component_name: &str) -> bool {
        self.is_fungible_component_impl(component_name)
    }

    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        self.get_component_version_impl(pldm_version_dict, ap_name, pkg_parser)
            .await
    }

    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        self.get_identifier_from_chassis_impl(ap_inv_uri).await
    }

    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> Option<String> {
        self.get_version_sku_impl(identifier, pldm_version_dict, ap_name)
    }

    async fn process_job_status(&self, task_id: &str, print_json: Option<&mut Value>) -> i32 {
        self.process_job_status_with_access(SwitchNvueAccess::Legacy, task_id, print_json)
            .await
    }

    async fn query_job_status(
        &self,
        task_id: &str,
        print_json: Option<&mut Value>,
    ) -> (bool, Value) {
        self.query_job_status_with_access(SwitchNvueAccess::Legacy, task_id, print_json)
            .await
    }

    fn print_job_status(
        &self,
        task_id: &str,
        my_dict: &Value,
        status: bool,
        print_json: Option<&mut Value>,
    ) -> (i32, Option<String>) {
        self.print_job_status_impl(task_id, my_dict, status, print_json)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use wiremock::matchers::{body_string_contains, method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::pldm::{FirmwarePkg, PLDM};

    struct RealSwitchPkgParser {
        inner: PLDM,
        parse_calls: usize,
        unpack_calls: usize,
    }

    #[async_trait::async_trait]
    impl PkgParser for RealSwitchPkgParser {
        async fn parse_pkg(&mut self, pkg_path: &str) -> (bool, String) {
            self.parse_calls += 1;
            self.inner.parse_pkg(pkg_path, None).await
        }

        async fn get_unpack_file_dict(&mut self, pkg_path: &str) {
            self.unpack_calls += 1;
            self.inner.prepare_unpack_file_dict(pkg_path).await;
        }

        fn unpack_file_ap_dict(&self) -> &HashMap<String, Vec<String>> {
            self.inner.unpack_file_ap_dict()
        }

        fn apname_version_dict_json(&self) -> Value {
            json!(self.inner.apname_version_dict())
        }
    }

    fn cmd_args() -> CmdArgs {
        CmdArgs {
            cmd: "update_fw".to_string(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special: None,
            oem_parameters: None,
        }
    }

    #[tokio::test]
    async fn hosted_reboot_probe_treats_http_error_as_transport_reachable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/firmware"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;

        let client = NvueClient::connect(
            nvue_client::ClientConfig {
                endpoint: nvue_client::ClientEndpoint::http("127.0.0.1", server.address().port()),
                credentials: nvue_client::ClientCredentials::new("admin", "secret"),
                dangerously_accept_invalid_certs: false,
            },
            None,
        )
        .await
        .unwrap();

        assert!(
            hosted_transport_reachable(
                client.as_ref(),
                "/nvue_v1/platform/firmware",
                Duration::from_secs(1),
            )
            .await
        );
    }

    #[tokio::test]
    async fn legacy_requests_use_current_bmc_access() {
        let first = MockServer::start().await;
        let second = MockServer::start().await;

        for server in [&first, &second] {
            Mock::given(method("GET"))
                .and(path("/nvue_v1/action/job"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "state": "completed"
                })))
                .expect(1)
                .mount(server)
                .await;
        }

        let mut target = GB200SwitchRFTarget::new(
            BmcAccess::mock_with_base_url_and_type(
                first.uri(),
                "mock-switch",
                crate::bmc_access::AccessType::NVSwitch,
            ),
            None,
        );

        assert!(target.query_job_status("job", None).await.0);

        target.bmc_access.base_url = second.uri();

        assert!(target.query_job_status("job", None).await.0);
    }

    struct SyntheticSwitchComponent<'a> {
        ap_name: &'a str,
        version: &'a str,
        payload: &'a [u8],
    }

    fn build_synthetic_switch_pldm_package(
        package_version: &str,
        components: &[SyntheticSwitchComponent<'_>],
    ) -> Vec<u8> {
        let mut pkg = Vec::new();
        pkg.extend_from_slice(&[
            0xF0, 0x18, 0x87, 0x8C, 0xCB, 0x7D, 0x49, 0x43, 0x98, 0x00, 0xA0, 0x2F, 0x05, 0x9A,
            0xCA, 0x02,
        ]);
        pkg.push(1);
        pkg.extend_from_slice(&0u16.to_le_bytes());
        pkg.extend_from_slice(&[0u8; 13]);
        let bitmap_bits = ((components.len().max(1) + 7) / 8 * 8) as u16;
        let bitmap_bytes = (bitmap_bits / 8) as usize;
        pkg.extend_from_slice(&bitmap_bits.to_le_bytes());
        pkg.push(1);
        pkg.push(package_version.len() as u8);
        pkg.extend_from_slice(package_version.as_bytes());

        pkg.push(components.len() as u8);
        for (index, component) in components.iter().enumerate() {
            pkg.extend_from_slice(&0u16.to_le_bytes());
            pkg.push(0);
            pkg.extend_from_slice(&0u32.to_le_bytes());
            pkg.push(1);
            pkg.push(component.ap_name.len() as u8);
            pkg.extend_from_slice(&0u16.to_le_bytes());
            let mut bitmap = vec![0u8; bitmap_bytes];
            bitmap[index / 8] = 1u8 << (index % 8);
            pkg.extend_from_slice(&bitmap);
            pkg.extend_from_slice(component.ap_name.as_bytes());
        }

        pkg.extend_from_slice(&(components.len() as u16).to_le_bytes());

        let component_table_len: usize = components
            .iter()
            .map(|component| 2 + 2 + 4 + 2 + 2 + 4 + 4 + 1 + 1 + component.version.len())
            .sum();
        let payload_start = pkg.len() + component_table_len + 4;
        let mut next_payload_offset = payload_start as u32;

        for (index, component) in components.iter().enumerate() {
            pkg.extend_from_slice(&0x000Au16.to_le_bytes());
            pkg.extend_from_slice(&((index + 1) as u16).to_le_bytes());
            pkg.extend_from_slice(&0u32.to_le_bytes());
            pkg.extend_from_slice(&0u16.to_le_bytes());
            pkg.extend_from_slice(&0x01u16.to_le_bytes());
            pkg.extend_from_slice(&next_payload_offset.to_le_bytes());
            pkg.extend_from_slice(&(component.payload.len() as u32).to_le_bytes());
            pkg.push(1);
            pkg.push(component.version.len() as u8);
            pkg.extend_from_slice(component.version.as_bytes());
            next_payload_offset += component.payload.len() as u32;
        }

        pkg.extend_from_slice(&0u32.to_le_bytes());
        for component in components {
            pkg.extend_from_slice(component.payload);
        }
        pkg
    }

    fn build_minimal_pldm_package(package_version: &str, device_version: &str) -> Vec<u8> {
        build_synthetic_switch_pldm_package(
            package_version,
            &[SyntheticSwitchComponent {
                ap_name: device_version,
                version: "1.0.0",
                payload: b"FWFW",
            }],
        )
    }

    async fn parse_synthetic_switch_package(
        tmp: &tempfile::TempDir,
        file_name: &str,
        package_version: &str,
        components: &[SyntheticSwitchComponent<'_>],
    ) -> PLDM {
        let package_path = tmp.path().join(file_name);
        let package = build_synthetic_switch_pldm_package(package_version, components);
        let mut package_file = std::fs::File::create(&package_path).unwrap();
        package_file.write_all(&package).unwrap();
        drop(package_file);

        let mut parser = PLDM::new();
        let package_path = package_path.to_string_lossy().to_string();
        let (ok, msg) = parser.parse_pkg(&package_path, None).await;
        assert!(ok, "failed to parse {file_name}: {msg}");
        parser.prepare_unpack_file_dict(&package_path).await;
        parser
    }

    fn assert_switch_package_shape(
        parser: &PLDM,
        package_version: &str,
        expected_components: &[(&str, &str, &str)],
    ) {
        let version_dict = parser.apname_version_dict();
        let package = version_dict
            .get(package_version)
            .unwrap_or_else(|| panic!("missing package version {package_version}"));

        for (ap_name, version, target_name) in expected_components {
            let (parsed_ap, parsed_data) = package
                .iter()
                .find(|(name, _)| name.split(',').next() == Some(*ap_name))
                .unwrap_or_else(|| panic!("missing AP {ap_name} in {package_version}"));
            assert_eq!(&parsed_data[0], version);
            assert_eq!(
                GB200SwitchRFTarget::get_target_apname(parsed_ap),
                *target_name
            );
            assert!(
                parser.unpack_file_ap_dict().contains_key(*ap_name),
                "missing unpack entry {ap_name}"
            );
        }

        let parsed_targets: Vec<String> = package
            .keys()
            .map(|name| GB200SwitchRFTarget::get_target_apname(name))
            .collect();
        let expected_targets: Vec<String> = expected_components
            .iter()
            .map(|(_, _, target)| (*target).to_string())
            .collect();
        assert_eq!(parsed_targets, expected_targets);
        assert_eq!(
            GB200SwitchRFTarget::get_platform_type(&json!(version_dict)),
            Some("GB200")
        );
    }

    #[tokio::test]
    async fn synthetic_gb200_switch_package_shapes_parse() {
        let tmp = tempfile::tempdir().unwrap();

        let package_0004 = parse_synthetic_switch_package(
            &tmp,
            "nvfw_GB200-P4978_0004_260413.1.0_prod-signed.fwpkg",
            "GB200-P4978_0004_260413.1.0",
            &[
                SyntheticSwitchComponent {
                    ap_name: "BMC",
                    version: "88.0002.1979",
                    payload: b"BMC-PAYLOAD",
                },
                SyntheticSwitchComponent {
                    ap_name: "ERoT",
                    version: "01.04.0049.0000_n04",
                    payload: b"EROT-PAYLOAD",
                },
                SyntheticSwitchComponent {
                    ap_name: "SMR",
                    version: "0.24",
                    payload: b"FPGA-PAYLOAD",
                },
            ],
        )
        .await;
        assert_switch_package_shape(
            &package_0004,
            "GB200-P4978_0004_260413.1.0",
            &[
                ("BMC", "88.0002.1979", "bmc"),
                ("ERoT", "01.04.0049.0000_n04", "erot"),
                ("SMR", "0.24", "fpga"),
            ],
        );

        let package_0006 = parse_synthetic_switch_package(
            &tmp,
            "nvfw_GB200-P4978_0006_260413.1.0_prod-signed.fwpkg",
            "GB200-P4978_0006_260413.1.0",
            &[
                SyntheticSwitchComponent {
                    ap_name: "SBIOS",
                    version: "0ACTV_00.01.022",
                    payload: b"SBIOS-PAYLOAD",
                },
                SyntheticSwitchComponent {
                    ap_name: "ERoT",
                    version: "01.04.0049.0000_n04",
                    payload: b"EROT-PAYLOAD",
                },
            ],
        )
        .await;
        assert_switch_package_shape(
            &package_0006,
            "GB200-P4978_0006_260413.1.0",
            &[
                ("SBIOS", "0ACTV_00.01.022", "bios"),
                ("ERoT", "01.04.0049.0000_n04", "erot"),
            ],
        );

        let package_0007 = parse_synthetic_switch_package(
            &tmp,
            "nvfw_GB200-P4978_0007_260413.1.0_prod-signed.fwpkg",
            "GB200-P4978_0007_260413.1.0",
            &[SyntheticSwitchComponent {
                ap_name: "CPLD",
                version: "000370_REV0600_000377_REV1900_000373_REV1200_000390_REV0400",
                payload: b"CPLD-VME-PAYLOAD",
            }],
        )
        .await;
        assert_switch_package_shape(
            &package_0007,
            "GB200-P4978_0007_260413.1.0",
            &[(
                "CPLD",
                "000370_REV0600_000377_REV1900_000373_REV1200_000390_REV0400",
                "cpld1",
            )],
        );
    }

    #[tokio::test]
    async fn synthetic_gb200_cpld_versions_split_and_detect_stale_cpld2() {
        let tmp = tempfile::tempdir().unwrap();
        let package_0007 = parse_synthetic_switch_package(
            &tmp,
            "nvfw_GB200-P4978_0007_260413.1.0_prod-signed.fwpkg",
            "GB200-P4978_0007_260413.1.0",
            &[SyntheticSwitchComponent {
                ap_name: "CPLD",
                version: "000370_REV0600_000377_REV1900_000373_REV1200_000390_REV0400",
                payload: b"CPLD-VME-PAYLOAD",
            }],
        )
        .await;

        let pldm_version_dict = json!(package_0007.apname_version_dict());
        let target = GB200SwitchRFTarget::new(
            BmcAccess::mock_with_base_url_and_type(
                "http://127.0.0.1",
                "mock-switch-cpld-split",
                crate::bmc_access::AccessType::NVSwitch,
            ),
            None,
        );

        let expected_pkg_versions = [
            ("CPLD1", "CPLD000370_REV0600"),
            ("CPLD2", "CPLD000377_REV1900"),
            ("CPLD3", "CPLD000373_REV1200"),
            ("CPLD4", "CPLD000390_REV0400"),
        ];
        for (ap_name, expected_version) in expected_pkg_versions {
            assert_eq!(
                target
                    .get_component_version(&pldm_version_dict, ap_name, None)
                    .await
                    .as_deref(),
                Some(expected_version)
            );
        }

        let installed_versions = [
            ("CPLD1", "CPLD000370_REV0600"),
            ("CPLD2", "CPLD000377_REV1700"),
            ("CPLD3", "CPLD000373_REV1200"),
            ("CPLD4", "CPLD000390_REV0400"),
        ];
        let mut stale = Vec::new();
        for (ap_name, installed_version) in installed_versions {
            let pkg_version = target
                .get_component_version(&pldm_version_dict, ap_name, None)
                .await
                .expect("package version");
            if !target.firmware_version_up_to_date(&pkg_version, installed_version) {
                stale.push(ap_name);
            }
        }

        assert_eq!(stale, vec!["CPLD2"]);
    }

    #[test]
    fn switch_serial_update_plan_ties_cpld_upload_name_to_install_url() {
        let mut unpack_dict = HashMap::new();
        unpack_dict.insert(
            "CPLD".to_string(),
            vec![
                "000370_REV0600_000377_REV1900_000373_REV1200_000390_REV0400".to_string(),
                "/tmp/CPLD_000370_REV0600_000377_REV1900_000373_REV1200_000390_REV0400_image.bin"
                    .to_string(),
            ],
        );

        let plan = GB200SwitchRFTarget::serial_update_plan(
            "CPLD1",
            "/tmp/nvfw_GB200-P4978_0007_260413.1.0_prod-signed.fwpkg",
            &unpack_dict,
        )
        .expect("CPLD serial update plan");

        assert_eq!(plan.target, "CPLD1");
        assert_eq!(plan.remote_dir, "/host/fw-images/cpld/");
        assert_eq!(
            plan.remote_name,
            "CPLD_000370_REV0600_000377_REV1900_000373_REV1200_000390_REV0400_image.vme"
        );
        let uploaded_path = format!("{}{}", plan.remote_dir, plan.remote_name);
        assert_eq!(
            GB200SwitchRFTarget::install_url_for_uploaded_path(&plan.target, &uploaded_path),
            format!(
                "/nvue_v1/platform/firmware/CPLD1/files/{}",
                plan.remote_name
            )
        );
    }

    #[tokio::test]
    async fn switch_cpld_workflow_uploads_vme_and_installs_cpld1() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex(
                r"^/nvue_v1/platform/firmware/CPLD1/files/.+\.vme$",
            ))
            .and(body_string_contains("@install"))
            .respond_with(ResponseTemplate::new(202).set_body_string("job-cpld-install"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-cpld-install"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": "completed",
                "status": "completed"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp
            .path()
            .join("nvfw_GB200-P4978_0007_260413.1.0_prod-signed.fwpkg");
        let package = build_synthetic_switch_pldm_package(
            "GB200-P4978_0007_260413.1.0",
            &[SyntheticSwitchComponent {
                ap_name: "CPLD",
                version: "000370_REV0600_000377_REV1900_000373_REV1200_000390_REV0400",
                payload: b"CPLD-VME-PAYLOAD",
            }],
        );
        let mut package_file = std::fs::File::create(&update_file).unwrap();
        package_file.write_all(&package).unwrap();
        drop(package_file);

        let mock_host = "mock-switch-phase-4-4-cpld";
        ssh_transport::install_mock_for_host(mock_host);
        let mut target = GB200SwitchRFTarget::new(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                mock_host,
                crate::bmc_access::AccessType::NVSwitch,
            ),
            None,
        );
        let mut parser = RealSwitchPkgParser {
            inner: PLDM::new(),
            parse_calls: 0,
            unpack_calls: 0,
        };
        let args = cmd_args();

        let (err_code, task_ids) = target
            .start_update_monitor(
                &[update_file.to_string_lossy().to_string()],
                &mut parser,
                &args,
                1200,
                false,
                None,
                0,
                true,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        let ssh = ssh_transport::take_mock_snapshot(mock_host).unwrap();
        assert_eq!(err_code, 0);
        assert_eq!(task_ids, vec!["job-cpld-install".to_string()]);
        assert_eq!(parser.parse_calls, 1);
        assert_eq!(parser.unpack_calls, 1);
        assert!(parser.inner.unpack_file_ap_dict().contains_key("CPLD"));
        assert!(ssh
            .exec_calls
            .iter()
            .any(|call| call.command == "mkdir -p '/host/fw-images/cpld/'"));
        assert_eq!(ssh.upload_calls.len(), 1);
        let expected_cleanup = format!("rm -f '{}'", ssh.upload_calls[0].remote_path);
        assert!(ssh
            .exec_calls
            .iter()
            .any(|call| call.command.as_str() == expected_cleanup));
        assert!(!ssh
            .exec_calls
            .iter()
            .any(|call| call.command.contains("/*")));
        assert_eq!(
            ssh.upload_calls[0]
                .local_path
                .extension()
                .and_then(|e| e.to_str()),
            Some("bin")
        );
        assert!(ssh.upload_calls[0]
            .remote_path
            .starts_with("/host/fw-images/cpld/"));
        assert!(
            ssh.upload_calls[0].remote_path.ends_with(".vme"),
            "remote path should use .vme: {}",
            ssh.upload_calls[0].remote_path
        );
        let uploaded_file_name = std::path::Path::new(&ssh.upload_calls[0].remote_path)
            .file_name()
            .and_then(|name| name.to_str())
            .expect("uploaded remote file name");
        let expected_install_path = format!(
            "/nvue_v1/platform/firmware/CPLD1/files/{}",
            uploaded_file_name
        );
        let post_paths: Vec<String> = server
            .received_requests()
            .await
            .expect("request recording enabled")
            .into_iter()
            .filter(|request| request.method.to_string() == "POST")
            .map(|request| request.url.path().to_string())
            .collect();
        assert_eq!(post_paths, vec![expected_install_path]);
    }

    #[tokio::test]
    async fn switch_package_workflow_uploads_installs_and_monitors() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/nvue_v1/platform/firmware/BMC/files/switch_bundle.fwpkg",
            ))
            .and(body_string_contains("@install"))
            .respond_with(ResponseTemplate::new(202).set_body_string("job-install"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-install"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": "completed",
                "status": "completed"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("switch_bundle.fwpkg");
        let package = build_minimal_pldm_package("GB200-SWITCH-0000", "BMC");
        let mut package_file = std::fs::File::create(&update_file).unwrap();
        package_file.write_all(&package).unwrap();
        drop(package_file);

        let mock_host = "mock-switch-phase-2i";
        ssh_transport::install_mock_for_host(mock_host);
        let mut target = GB200SwitchRFTarget::new(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                mock_host,
                crate::bmc_access::AccessType::NVSwitch,
            ),
            None,
        );
        let mut parser = RealSwitchPkgParser {
            inner: PLDM::new(),
            parse_calls: 0,
            unpack_calls: 0,
        };
        let args = cmd_args();

        let (err_code, task_ids) = target
            .start_update_monitor(
                &[update_file.to_string_lossy().to_string()],
                &mut parser,
                &args,
                1200,
                false,
                None,
                0,
                true,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        let ssh = ssh_transport::take_mock_snapshot(mock_host).unwrap();
        assert_eq!(err_code, 0);
        assert_eq!(task_ids, vec!["job-install".to_string()]);
        assert_eq!(parser.parse_calls, 1);
        assert_eq!(parser.unpack_calls, 1);
        assert!(parser
            .inner
            .apname_version_dict()
            .contains_key("GB200-SWITCH-0000"));
        assert!(parser.inner.unpack_file_ap_dict().contains_key("BMC"));
        assert!(ssh
            .exec_calls
            .iter()
            .any(|call| call.command == "mkdir -p '/host/fw-images/bmc/'"));
        assert_eq!(ssh.upload_calls.len(), 1);
        assert_eq!(ssh.upload_calls[0].local_path, update_file);
        assert_eq!(
            ssh.upload_calls[0].remote_path,
            "/host/fw-images/bmc/switch_bundle.fwpkg"
        );
        assert!(ssh
            .exec_calls
            .iter()
            .any(|call| call.command == "rm -f '/host/fw-images/bmc/switch_bundle.fwpkg'"));
        assert!(!ssh
            .exec_calls
            .iter()
            .any(|call| call.command.contains("/*")));
    }

    #[tokio::test]
    async fn switch_json_package_derived_actions_record_progress_and_complete() {
        let server = MockServer::start().await;
        for (target_name, job_id) in [
            ("BMC", "job-bmc-install"),
            ("EROT", "job-erot-install"),
            ("FPGA", "job-fpga-install"),
        ] {
            Mock::given(method("POST"))
                .and(path(format!(
                    "/nvue_v1/platform/firmware/{target_name}/files/switch_bundle.fwpkg"
                )))
                .and(body_string_contains("@install"))
                .and(body_string_contains("\"force\":false"))
                .respond_with(ResponseTemplate::new(202).set_body_string(job_id))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path(format!("/nvue_v1/action/{job_id}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "state": "action_success",
                    "status": "Firmware switch_bundle.fwpkg installed successfully\nNext reboot will perform a power cycle to load the new firmware"
                })))
                .expect(1)
                .mount(&server)
                .await;
        }

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("switch_bundle.fwpkg");
        let package = build_synthetic_switch_pldm_package(
            "GB200-P4978_0004_260413.1.0",
            &[
                SyntheticSwitchComponent {
                    ap_name: "BMC",
                    version: "88.0002.1979",
                    payload: b"BMC-PAYLOAD",
                },
                SyntheticSwitchComponent {
                    ap_name: "ERoT",
                    version: "01.04.0049.0000_n04",
                    payload: b"EROT-PAYLOAD",
                },
                SyntheticSwitchComponent {
                    ap_name: "SMR",
                    version: "0.24",
                    payload: b"FPGA-PAYLOAD",
                },
            ],
        );
        let mut package_file = std::fs::File::create(&update_file).unwrap();
        package_file.write_all(&package).unwrap();
        drop(package_file);

        let mock_host = "mock-switch-json-package-derived-progress";
        ssh_transport::install_mock_for_host(mock_host);
        let mut target = GB200SwitchRFTarget::new(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                mock_host,
                crate::bmc_access::AccessType::NVSwitch,
            ),
            None,
        );
        let mut parser = RealSwitchPkgParser {
            inner: PLDM::new(),
            parse_calls: 0,
            unpack_calls: 0,
        };
        let mut args = cmd_args();
        args.special = Some(vec![json!({"ForceUpdate": true}).to_string()]);
        let mut json_output = json!({"Output": []});

        let (err_code, task_ids) = target
            .start_update_monitor(
                &[update_file.to_string_lossy().to_string()],
                &mut parser,
                &args,
                1200,
                false,
                Some(&mut json_output),
                0,
                true,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        let _ = ssh_transport::take_mock_snapshot(mock_host).unwrap();
        assert_eq!(err_code, 0);
        assert_eq!(
            task_ids,
            vec![
                "job-bmc-install".to_string(),
                "job-erot-install".to_string(),
                "job-fpga-install".to_string(),
            ]
        );

        let output = json_output["Output"].as_array().expect("output array");
        let action_successes: Vec<(&str, &str)> = output
            .iter()
            .filter(|entry| entry["stage"] == "install" && entry["state"] == "action_success")
            .map(|entry| {
                (
                    entry["target"].as_str().expect("target"),
                    entry["action_id"].as_str().expect("action id"),
                )
            })
            .collect();
        assert_eq!(
            action_successes,
            vec![
                ("BMC", "job-bmc-install"),
                ("EROT", "job-erot-install"),
                ("FPGA", "job-fpga-install"),
            ]
        );

        let complete = output
            .iter()
            .find(|entry| entry["stage"] == "complete")
            .expect("complete progress event");
        assert_eq!(complete["state"], "success");
        assert_eq!(
            complete["details"]["actions"]
                .as_array()
                .expect("action summary")
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn switch_json_update_monitors_nvue_action_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/nvue_v1/platform/firmware/BMC/files/switch_bundle.fwpkg",
            ))
            .and(body_string_contains("@install"))
            .respond_with(ResponseTemplate::new(202).set_body_string("job-install-failed"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-install-failed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": "action_failed",
                "status": "bad image"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("switch_bundle.fwpkg");
        let package = build_minimal_pldm_package("GB200-SWITCH-0000", "BMC");
        let mut package_file = std::fs::File::create(&update_file).unwrap();
        package_file.write_all(&package).unwrap();
        drop(package_file);

        let mock_host = "mock-switch-json-action-failure";
        ssh_transport::install_mock_for_host(mock_host);
        let mut target = GB200SwitchRFTarget::new(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                mock_host,
                crate::bmc_access::AccessType::NVSwitch,
            ),
            None,
        );
        let mut parser = RealSwitchPkgParser {
            inner: PLDM::new(),
            parse_calls: 0,
            unpack_calls: 0,
        };
        let args = cmd_args();
        let mut json_output = json!({"Output": []});

        let (err_code, task_ids) = target
            .start_update_monitor(
                &[update_file.to_string_lossy().to_string()],
                &mut parser,
                &args,
                1200,
                false,
                Some(&mut json_output),
                0,
                true,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        let _ = ssh_transport::take_mock_snapshot(mock_host).unwrap();
        assert_eq!(err_code, 1);
        assert_eq!(task_ids, vec!["job-install-failed".to_string()]);
        let output = json_output["Output"].as_array().expect("output array");
        assert!(output.iter().any(|entry| {
            entry["stage"] == "install"
                && entry["action_id"] == "job-install-failed"
                && entry["state"] == "accepted"
        }));
        assert!(output.iter().any(|entry| {
            entry["stage"] == "install"
                && entry["action_id"] == "job-install-failed"
                && entry["state"] == "action_failed"
                && entry["status"] == "bad image"
        }));
        assert!(output.iter().any(|entry| {
            entry["Id"] == "job-install-failed"
                && entry["error"]["state"] == "action_failed"
                && entry["error"]["status"] == "bad image"
        }));
    }

    #[tokio::test]
    async fn switch_activation_succeeds_when_power_cycle_action_is_running() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/nvue_v1/system"))
            .and(body_string_contains("@power-cycle"))
            .respond_with(ResponseTemplate::new(202).set_body_string("job-power-cycle"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "http_status": 200,
                "state": "running",
                "status": ""
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut target = GB200SwitchRFTarget::new(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                "mock-switch-activation-running",
                crate::bmc_access::AccessType::NVSwitch,
            ),
            None,
        );
        let args = CmdArgs {
            cmd: "NVUE_PWR_CYCLE".to_string(),
            quiet: true,
            ..cmd_args()
        };

        let ret_code = target.run_oob_activation(&args).await;

        assert_eq!(ret_code, 0);
    }

    #[tokio::test]
    async fn switch_activation_fails_when_power_cycle_action_fails() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/nvue_v1/system"))
            .and(body_string_contains("@power-cycle"))
            .respond_with(ResponseTemplate::new(202).set_body_string("job-power-cycle"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": "action_failed",
                "status": "power cycle failed"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut target = GB200SwitchRFTarget::new(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                "mock-switch-activation-failure",
                crate::bmc_access::AccessType::NVSwitch,
            ),
            None,
        );
        let args = CmdArgs {
            cmd: "NVUE_PWR_CYCLE".to_string(),
            quiet: false,
            ..cmd_args()
        };

        let ret_code = target.run_oob_activation(&args).await;

        assert_eq!(ret_code, 1);
    }
}
