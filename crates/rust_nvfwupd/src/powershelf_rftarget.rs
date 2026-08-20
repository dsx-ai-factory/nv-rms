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

//! PowerShelfRFTarget implementation for LiteOn/Delta/Megmeet/Flex power shelf platforms.
//!
//! Supports both PLDM and tar update methods. BMC resets during update
//! so task monitoring is not possible after upload.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

use crate::bmc_access::BmcAccess;
use crate::rf_target::{
    task_id_from_update_response, CmdArgs, PkgParser, RFTarget, UpdatePreconditionMode,
};
use crate::util::{BailAction, Util};
use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Known UUID for Delta PMC packages that contain BMC firmware.
const DELTA_PMC_UUID: &str = "0x0b4e248c8bcf41379b99d0394c808ba5";
const LITEON_POWERUNITS_URI: &str = "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits";
const LITEON_PSU_UPDATE_URI: &str =
    "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/Actions/UpdateService/";
const LITEON_PSU_SERIAL_COMPLETE_TASK_ID: &str = "liteon-psu-serial-complete";
#[cfg(not(test))]
const LITEON_PSU_SERIAL_SETTLE_SECS: u64 = 30;
#[cfg(not(test))]
const LITEON_PSU_SERIAL_MONITOR_TIMEOUT_SECS: u64 = 1800;
#[cfg(test)]
const LITEON_PSU_SERIAL_MONITOR_TIMEOUT_SECS: u64 = 1;
const LITEON_PSU_SERIAL_DEVICE_MAX_ATTEMPTS: u32 = 2;
#[cfg(not(test))]
const LITEON_PSU_SERIAL_DEVICE_RETRY_SETTLE_SECS: u64 = 60;
#[cfg(test)]
const LITEON_PSU_SERIAL_DEVICE_RETRY_SETTLE_SECS: u64 = 0;
#[cfg(not(test))]
const LITEON_PSU_STATUS_POLL_INTERVAL_SECS: u64 = 5;
#[cfg(test)]
const LITEON_PSU_STATUS_POLL_INTERVAL_SECS: u64 = 1;
const LITEON_PSU_UPDATE_POST_MAX_ATTEMPTS: u32 = 3;
#[cfg(not(test))]
const LITEON_PSU_UPDATE_POST_RETRY_INTERVAL_SECS: u64 = 10;
#[cfg(test)]
const LITEON_PSU_UPDATE_POST_RETRY_INTERVAL_SECS: u64 = 0;
#[cfg(not(test))]
const LITEON_PSU_ALL_TARGET_SETTLE_TIMEOUT_SECS: u64 = 300;
#[cfg(test)]
const LITEON_PSU_ALL_TARGET_SETTLE_TIMEOUT_SECS: u64 = 1;
#[cfg(not(test))]
const LITEON_PSU_ALL_TARGET_SETTLE_POLL_INTERVAL_SECS: u64 = 15;
#[cfg(test)]
const LITEON_PSU_ALL_TARGET_SETTLE_POLL_INTERVAL_SECS: u64 = 1;
const POWERSHELF_ON_RESET_UPLOAD_RESPONSE_TIMEOUT_SECS: u64 = 60;
#[cfg(not(test))]
const POWERSHELF_RESET_RECOVERY_ATTEMPTS: u32 = 24;
#[cfg(test)]
const POWERSHELF_RESET_RECOVERY_ATTEMPTS: u32 = 3;
#[cfg(not(test))]
const POWERSHELF_RESET_RECOVERY_INTERVAL_SECS: u64 = 5;
#[cfg(test)]
const POWERSHELF_RESET_RECOVERY_INTERVAL_SECS: u64 = 0;
#[cfg(not(test))]
const POWERSHELF_RESET_RECOVERY_PROBE_TIMEOUT_SECS: u64 = 5;
#[cfg(test)]
const POWERSHELF_RESET_RECOVERY_PROBE_TIMEOUT_SECS: u64 = 1;

static LITEON_PSU_ALL_TASK_UPDATE_FILES: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();

// ---------------------------------------------------------------------------
// PowerShelfRFTarget
// ---------------------------------------------------------------------------

/// Platform-specific RFTarget for power shelf systems.
pub struct PowerShelfRFTarget {
    pub bmc_access: BmcAccess,
    pub fungible_components: Vec<String>,
    pub update_completion_msg: String,
    pub progress_table_header_printed: bool,
    pub config_dict: Option<Value>,
    reset_recovery_attempts: Option<u32>,
}

/// Detect PowerShelf OnReset tasks that have reached the reset handoff point.
///
/// Some shelves encode a successful handoff as a warning/cancelled task, while
/// older LiteOn PMC firmware leaves a running task with an internal-error
/// message until the reset applies the firmware.
pub(crate) fn is_powershelf_on_reset_handoff_task(task: &Value) -> bool {
    is_cancelled_warning_on_reset_handoff_task(task)
        || is_old_liteon_pmc_on_reset_handoff_task(task)
}

/// Match newer PowerShelf OnReset handoff tasks.
///
/// These tasks report cancelled/warning at 100 percent with an aborted/completed
/// message even though the update payload has reached the point that requires a
/// reset/activation.
fn is_cancelled_warning_on_reset_handoff_task(task: &Value) -> bool {
    let state = task
        .get("TaskState")
        .or_else(|| task.get("state"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let task_status = task
        .get("TaskStatus")
        .or_else(|| task.get("status"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let progress = task
        .get("PercentComplete")
        .and_then(Value::as_u64)
        .unwrap_or_default();

    if !matches!(
        state.to_ascii_lowercase().as_str(),
        "cancelled" | "canceled"
    ) || !task_status.eq_ignore_ascii_case("warning")
        || progress != 100
    {
        return false;
    }

    task_messages_contain(task, |message| {
        message
            .get("MessageId")
            .and_then(Value::as_str)
            .map(|id| id.contains("TaskAborted"))
            .unwrap_or(false)
            || message
                .get("Message")
                .and_then(Value::as_str)
                .map(|text| text.contains("completed with errors"))
                .unwrap_or(false)
    })
}

/// Match the old LiteOn PMC OnReset handoff shape.
///
/// Older LiteOn firmware can leave the task in `Running` with progress 0 and a
/// non-empty `EndTime` plus a critical internal-service-error message forever.
/// After a graceful PowerShelf reset, the shelf comes back with the new PMC
/// version.
fn is_old_liteon_pmc_on_reset_handoff_task(task: &Value) -> bool {
    let state = task
        .get("TaskState")
        .or_else(|| task.get("state"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let task_status = task
        .get("TaskStatus")
        .or_else(|| task.get("status"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let progress = task
        .get("PercentComplete")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let has_end_time = task
        .get("EndTime")
        .and_then(Value::as_str)
        .map(|end_time| !end_time.trim().is_empty())
        .unwrap_or(false);

    if !state.eq_ignore_ascii_case("running")
        || !task_status.eq_ignore_ascii_case("ok")
        || progress != 0
        || !has_end_time
    {
        return false;
    }

    task_messages_contain(task, |message| {
        let message_id = message
            .get("MessageId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let text = message
            .get("Message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let severity = message
            .get("MessageSeverity")
            .or_else(|| message.get("Severity"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let internal_error_id = message_id
            .rsplit('.')
            .next()
            .map(|segment| segment.eq_ignore_ascii_case("InternalError"))
            .unwrap_or(false);
        let internal_service_error = text.contains("internal service error");

        (internal_error_id || internal_service_error) && severity.eq_ignore_ascii_case("critical")
    })
}

/// Search Redfish task messages with a caller-supplied structured predicate.
fn task_messages_contain<F>(task: &Value, predicate: F) -> bool
where
    F: Fn(&Value) -> bool,
{
    task.get("Messages")
        .and_then(Value::as_array)
        .map(|messages| messages.iter().any(predicate))
        .unwrap_or(false)
}

/// Extract a TaskService task id from a task collection member URI.
fn task_id_from_task_member_uri(uri: &str) -> Option<String> {
    let trimmed = uri.trim().trim_end_matches('/');
    let mut segments = trimmed.rsplit('/');
    let task_id = segments.next()?.trim();
    let parent = segments.next()?.trim();
    if !parent.eq_ignore_ascii_case("tasks") || task_id.is_empty() {
        return None;
    }
    Some(task_id.to_string())
}

impl PowerShelfRFTarget {
    pub fn new(bmc_access: BmcAccess, config_dict: Option<Value>) -> Self {
        Self {
            bmc_access,
            fungible_components: Vec::new(),
            update_completion_msg: "PowerShelf firmware update completed. \
                 Refer to 'NVIDIA Firmware Update Document' on \
                 activation steps for new firmware to take effect."
                .to_string(),
            progress_table_header_printed: false,
            config_dict,
            reset_recovery_attempts: None,
        }
    }

    /// Number of recovery probes used when activation already observed reset recovery.
    pub fn reset_recovery_attempts(&self) -> Option<u32> {
        self.reset_recovery_attempts
    }

    /// Consider reset recovery successful only after seeing an outage first.
    ///
    /// This avoids treating a still-running controller as recovered before the
    /// graceful PowerShelf reset has actually taken effect.
    fn reset_recovery_probe_confirms_reset(ok: bool, observed_unreachable: &mut bool) -> bool {
        if !ok {
            *observed_unreachable = true;
            return false;
        }

        *observed_unreachable
    }

    /// Build the Manager.Reset POST URL from either relative or absolute targets.
    fn manager_reset_request_url(base_url: &str, target_uri: &str) -> String {
        if target_uri.starts_with("http://") || target_uri.starts_with("https://") {
            target_uri.to_string()
        } else if target_uri.starts_with('/') {
            format!("{}{}", base_url.trim_end_matches('/'), target_uri)
        } else {
            format!("{}/{}", base_url.trim_end_matches('/'), target_uri)
        }
    }

    /// Probe for the expected outage and recovery after a PowerShelf reset.
    async fn target_recovers_after_reset_transport_error(&mut self) -> bool {
        let mut observed_unreachable = false;
        self.reset_recovery_attempts = None;
        for attempt in 1..=POWERSHELF_RESET_RECOVERY_ATTEMPTS {
            sleep(Duration::from_secs(POWERSHELF_RESET_RECOVERY_INTERVAL_SECS)).await;
            let (ok, _) = self
                .bmc_access
                .dispatch_request_full(
                    "GET",
                    "/redfish/v1",
                    None,
                    None,
                    POWERSHELF_RESET_RECOVERY_PROBE_TIMEOUT_SECS,
                    true,
                    None,
                )
                .await;
            if Self::reset_recovery_probe_confirms_reset(ok, &mut observed_unreachable) {
                self.reset_recovery_attempts = Some(attempt);
                return true;
            }
        }
        false
    }

    /// Wrapper used by instance methods and tests for PowerShelf handoff detection.
    fn is_on_reset_handoff_task(task: &Value) -> bool {
        is_powershelf_on_reset_handoff_task(task)
    }

    fn liteon_psu_all_task_registry() -> &'static Mutex<HashMap<String, String>> {
        LITEON_PSU_ALL_TASK_UPDATE_FILES.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn liteon_psu_all_task_registry_key(&self, task_id: &str) -> String {
        format!(
            "{}|{}",
            self.bmc_access.base_url.trim_end_matches('/'),
            task_id
        )
    }

    fn register_liteon_psu_all_task_for_repair(&self, task_id: &str, update_file: &str) {
        let key = self.liteon_psu_all_task_registry_key(task_id);
        if let Ok(mut registry) = Self::liteon_psu_all_task_registry().lock() {
            registry.insert(key, update_file.to_string());
        }
    }

    fn liteon_psu_all_task_update_file(&self, task_id: &str) -> Option<String> {
        let key = self.liteon_psu_all_task_registry_key(task_id);
        Self::liteon_psu_all_task_registry()
            .lock()
            .ok()
            .and_then(|registry| registry.get(&key).cloned())
    }

    fn clear_liteon_psu_all_task_for_repair(&self, task_id: &str) {
        let key = self.liteon_psu_all_task_registry_key(task_id);
        if let Ok(mut registry) = Self::liteon_psu_all_task_registry().lock() {
            registry.remove(&key);
        }
    }

    fn liteon_psu_target_version_label(target_version: (u8, u8)) -> String {
        format!("{:02X}{:02X}", target_version.0, target_version.1)
    }

    fn redfish_task_failed_for_liteon_psu_repair(task: &Value) -> bool {
        let task_state = task
            .get("TaskState")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if matches!(
            task_state.as_str(),
            "failed" | "exception" | "cancelled" | "canceled" | "killed"
        ) {
            return true;
        }

        task_state == "completed"
            && Self::liteon_psu_status_is_failure(task.get("TaskStatus").and_then(Value::as_str))
    }

    fn redfish_task_completed_without_liteon_psu_failure(task: &Value) -> bool {
        task.get("TaskState")
            .and_then(Value::as_str)
            .map(|state| state.eq_ignore_ascii_case("Completed"))
            .unwrap_or(false)
            && !Self::liteon_psu_status_is_failure(task.get("TaskStatus").and_then(Value::as_str))
    }

    /// Return existing TaskService entries that already look like OnReset handoff.
    async fn on_reset_handoff_tasks(&self) -> Vec<(String, Value)> {
        let (status, tasks) = self
            .target_access()
            .dispatch_request("GET", "/redfish/v1/TaskService/Tasks", None, None)
            .await;
        if !status {
            return Vec::new();
        }

        let Some(members) = tasks.get("Members").and_then(Value::as_array) else {
            return Vec::new();
        };

        let mut handoff_tasks = Vec::new();
        for member in members {
            let Some(uri) = member.get("@odata.id").and_then(Value::as_str) else {
                continue;
            };
            let Some(task_id) = task_id_from_task_member_uri(uri) else {
                continue;
            };
            let (task_ok, task) = self
                .target_access()
                .dispatch_request("GET", uri, None, None)
                .await;
            if task_ok && Self::is_on_reset_handoff_task(&task) {
                handoff_tasks.push((task_id, task));
            }
        }
        handoff_tasks
    }

    /// Find the handoff task created by the current upload attempt.
    async fn new_on_reset_handoff_task(
        &self,
        existing_task_ids: &HashSet<String>,
    ) -> Option<(String, Value)> {
        self.on_reset_handoff_tasks()
            .await
            .into_iter()
            .rev()
            .find(|(task_id, _)| !existing_task_ids.contains(task_id))
    }

    /// Add a sanitized handoff task to JSON output for RMS/library callers.
    fn push_on_reset_handoff_task_json(
        mut json_dict: Option<&mut Value>,
        task_id: &str,
        task: &Value,
    ) {
        let Some(ref mut jd) = json_dict else {
            return;
        };
        let Some(output) = jd.get_mut("Output").and_then(Value::as_array_mut) else {
            return;
        };

        let mut task = NvUtils::redact_secret_json_value(task);
        if let Some(obj) = task.as_object_mut() {
            obj.entry("Id".to_string())
                .or_insert_with(|| Value::String(task_id.to_string()));
        } else {
            task = json!({
                "Id": task_id,
                "TaskState": "Completed",
                "TaskStatus": "OK",
                "PercentComplete": 100,
            });
        }
        output.push(task);
    }

    /// Check whether the given path is a real tar archive using the POSIX
    /// "ustar" magic at byte offset 257, falling back to extension check
    /// if the file cannot be read.
    async fn is_tar_package(filename: &str) -> bool {
        if crate::pldm::is_tar_file(filename).await {
            return true;
        }
        let lower = filename.to_lowercase();
        lower.ends_with(".tar") || lower.ends_with(".tar.gz") || lower.ends_with(".tgz")
    }

    /// Extract component type from a Redfish Description field.
    fn get_component_type_from_description(description: &str) -> &'static str {
        let lower = description.to_lowercase();
        if lower.contains("psu") {
            return "psu";
        }
        if lower.contains("mcu") || lower.contains("pldm") {
            return "mcu";
        }
        if lower.contains("bmc") || lower.contains("pmc") {
            return "bmc";
        }
        "unknown"
    }

    /// Return whether the target should probe the LiteOn-only PowerUnits tree.
    fn liteon_powerunits_inventory_supported(model: &str) -> bool {
        let model_lower = model.to_ascii_lowercase();
        model_lower.contains("liteon")
            || model_lower.starts_with("pf-1333")
            || model_lower.starts_with("pf-1114")
    }

    /// Check if a PLDM package contains the Delta PMC UUID by inspecting
    /// the raw PLDM dict's FirmwareDeviceRecords / DeviceDescriptors.
    fn has_delta_pmc_uuid(pkg_parser: &dyn PkgParser, pkg_version: &str) -> bool {
        let raw = pkg_parser.pldm_raw_dict();
        let raw_obj = match raw.as_object() {
            Some(o) => o,
            None => return false,
        };

        for (_pkg_path, pkg_data) in raw_obj {
            let pvs = pkg_data
                .pointer("/PackageHeaderInformation/PackageVersionString")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if pvs != pkg_version {
                continue;
            }

            if let Some(records) = pkg_data
                .get("FirmwareDeviceRecords")
                .and_then(|v| v.as_array())
            {
                for record in records {
                    if let Some(descriptors) =
                        record.get("DeviceDescriptors").and_then(|v| v.as_array())
                    {
                        for desc in descriptors {
                            let dtype = desc
                                .get("InitialDescriptorType")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            if dtype == "UUID" {
                                let uuid_data = desc
                                    .get("InitialDescriptorData")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                if uuid_data.eq_ignore_ascii_case(DELTA_PMC_UUID) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        false
    }

    /// Check if `ap_name` is a LiteOn PowerUnit and return its Model.
    async fn get_liteon_model_from_inventory(&self, ap_name: &str) -> Option<String> {
        if !Self::liteon_powerunits_inventory_supported(&self.bmc_access.model) {
            return None;
        }

        let powerunits_uri = "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits";
        let (status, response) = self
            .bmc_access
            .dispatch_request("GET", powerunits_uri, None, None)
            .await;
        if !status {
            return None;
        }
        let members = response.get("Members")?.as_array()?;
        for member in members {
            let device_uri = member.get("@odata.id")?.as_str()?;
            let uri_id = device_uri.rsplit('/').next().unwrap_or("");
            if uri_id.eq_ignore_ascii_case(ap_name) {
                let (ok, device_resp) = self
                    .bmc_access
                    .dispatch_request("GET", device_uri, None, None)
                    .await;
                if ok {
                    if let Some(model) = device_resp.get("Model").and_then(|v| v.as_str()) {
                        if !model.is_empty() {
                            return Some(model.to_string());
                        }
                    }
                }
            }
        }
        None
    }

    /// Get component type from Redfish FirmwareInventory by matching the AP name URI.
    async fn get_component_type_from_inventory(&self, ap_name: &str) -> &'static str {
        let (status, fw_inv) = self
            .bmc_access
            .dispatch_request(
                "GET",
                "/redfish/v1/UpdateService/FirmwareInventory",
                None,
                None,
            )
            .await;
        if !status {
            return "unknown";
        }
        if let Some(members) = fw_inv.get("Members").and_then(|v| v.as_array()) {
            for member in members {
                if let Some(inv_uri) = member.get("@odata.id").and_then(|v| v.as_str()) {
                    let uri_ap = inv_uri.rsplit('/').next().unwrap_or("");
                    if uri_ap.eq_ignore_ascii_case(ap_name) {
                        let (ok, inv_detail) = self
                            .bmc_access
                            .dispatch_request("GET", inv_uri, None, None)
                            .await;
                        if ok {
                            let desc = inv_detail
                                .get("Description")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            return Self::get_component_type_from_description(desc);
                        }
                    }
                }
            }
        }
        "unknown"
    }

    /// Read `ApplyTime` from special/cmd_args JSON. Defaults to `"Immediate"`.
    ///
    /// Python: first tries inline JSON string via `validate_json` / `json.loads`,
    /// then falls back to file path.
    async fn get_apply_time(cmd_args: &CmdArgs) -> Result<String, String> {
        if let Some(ref special) = cmd_args.special {
            if let Some(param) = special.first() {
                // Try inline JSON first (Python: validate_json + json.loads)
                match serde_json::from_str::<Value>(param) {
                    Ok(val) => {
                        return Self::apply_time_from_json(&val);
                    }
                    Err(inline_err) => match tokio::fs::read_to_string(param).await {
                        Ok(contents) => {
                            let val = serde_json::from_str::<Value>(&contents).map_err(|e| {
                                format!(
                                    "Special command JSON file '{}' could not be properly parsed: {}",
                                    param, e
                                )
                            })?;
                            return Self::apply_time_from_json(&val);
                        }
                        Err(read_err) => {
                            return Err(format!(
                                "Special command JSON could not be properly parsed: {}; also failed to read '{}' as a file: {}",
                                inline_err, param, read_err
                            ));
                        }
                    },
                }
            }
        }
        Ok("Immediate".to_string())
    }

    fn apply_time_from_json(val: &Value) -> Result<String, String> {
        match val.get("ApplyTime") {
            Some(Value::String(at)) => {
                if at == "Immediate" || at == "OnReset" {
                    Ok(at.to_string())
                } else {
                    Err(format!(
                        "Invalid ApplyTime '{}'; expected 'Immediate' or 'OnReset'",
                        at
                    ))
                }
            }
            Some(_) => Err("Invalid ApplyTime value; expected a string".to_string()),
            None => Ok("Immediate".to_string()),
        }
    }

    /// Read `LiteOnPowerDeviceId` from special/cmd_args JSON.
    ///
    /// Supports both inline JSON strings and file paths, like `get_apply_time`.
    async fn get_liteon_device_id(cmd_args: &CmdArgs) -> Option<String> {
        let special = cmd_args.special.as_ref()?;
        let param = special.first()?;
        // Try inline JSON first, then file path
        let val: Value = match serde_json::from_str(param) {
            Ok(val) => val,
            Err(_) => {
                let contents = tokio::fs::read_to_string(param).await.ok()?;
                serde_json::from_str(&contents).ok()?
            }
        };
        val.get("LiteOnPowerDeviceId")
            .and_then(|v| v.as_str().or_else(|| v.as_i64().map(|_| "")))
            .map(|s| {
                if s.is_empty() {
                    val.get("LiteOnPowerDeviceId")
                        .map(|v| v.to_string())
                        .unwrap_or_default()
                } else {
                    s.to_string()
                }
            })
    }

    /// Set `HttpPushUriApplyTime` on the UpdateService.
    async fn set_applytime(
        &self,
        apply_time: &str,
        json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> bool {
        if self.update_service_apply_time_matches(apply_time).await {
            if !parallel_update && json_dict.is_none() {
                println!("ApplyTime already set to '{}'.", apply_time);
            }
            return true;
        }

        let patch_data = json!({
            "HttpPushUriOptions": {
                "HttpPushUriApplyTime": {
                    "ApplyTime": apply_time
                }
            }
        });

        let (status, response) = self
            .bmc_access
            .dispatch_request(
                "PATCH",
                "/redfish/v1/UpdateService",
                Some(&patch_data),
                None,
            )
            .await;

        if status {
            if !parallel_update && json_dict.is_none() {
                println!("ApplyTime set to '{}' successfully.", apply_time);
            }
            true
        } else {
            if !parallel_update && json_dict.is_none() {
                println!(
                    "Warning: Failed to set ApplyTime to {}: {:?}",
                    apply_time, response
                );
            }
            false
        }
    }

    /// Return whether the UpdateService already advertises the requested ApplyTime.
    async fn update_service_apply_time_matches(&self, apply_time: &str) -> bool {
        let (status, response) = self
            .bmc_access
            .dispatch_request("GET", "/redfish/v1/UpdateService", None, None)
            .await;
        if !status {
            return false;
        }

        response
            .pointer("/HttpPushUriOptions/HttpPushUriApplyTime/ApplyTime")
            .and_then(Value::as_str)
            .map(|current| current.eq_ignore_ascii_case(apply_time))
            .unwrap_or(false)
    }

    fn liteon_powerdevice_name(device_id: &str) -> String {
        if device_id.to_ascii_lowercase().starts_with("powerdevice") {
            device_id.to_string()
        } else {
            format!("powerdevice{}", device_id)
        }
    }

    /// Extract the raw LiteOn PSU index from a PowerUnits member URI.
    ///
    /// LiteOn update POSTs expect the raw numeric `LiteOnPowerDeviceId` value, so
    /// `/PowerUnits/powerdevice3` maps to `3`.
    fn liteon_psu_device_id_from_member_uri(uri: &str) -> Option<String> {
        uri.trim_end_matches('/')
            .rsplit('/')
            .next()
            .and_then(|member| {
                member
                    .strip_prefix("powerdevice")
                    .filter(|id| !id.is_empty() && id.chars().all(|ch| ch.is_ascii_digit()))
            })
            .map(ToOwned::to_owned)
    }

    /// Detect PSU intent from target lists in UpdateParameters-style special input.
    ///
    /// This helper only inspects `Targets` and `HttpPushUriTargets`. A direct
    /// `LiteOnPowerDeviceId` value is handled separately when update parameters
    /// are built.
    fn liteon_psu_request_from_special(cmd_args: &CmdArgs) -> bool {
        let Some(special) = cmd_args.special.as_ref().and_then(|params| params.first()) else {
            return false;
        };

        let value = serde_json::from_str::<Value>(special).ok().or_else(|| {
            std::fs::read_to_string(special)
                .ok()
                .and_then(|contents| serde_json::from_str::<Value>(&contents).ok())
        });

        let Some(value) = value else {
            return false;
        };

        for key in ["Targets", "HttpPushUriTargets"] {
            if let Some(targets) = value.get(key).and_then(Value::as_array) {
                if targets.iter().filter_map(Value::as_str).any(|target| {
                    let target = target.to_ascii_lowercase();
                    target.contains("psu")
                        || target.contains("powerunit")
                        || target.contains("powerdevice")
                }) {
                    return true;
                }
            }
        }

        false
    }

    /// Infer PSU intent from LiteOn package naming when no explicit targets exist.
    fn looks_like_liteon_psu_package(update_file: &str) -> bool {
        Path::new(update_file)
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| {
                let name = name.to_ascii_lowercase();
                name.starts_with("sp-") || name.contains("liteon-psu") || name.contains("_psu")
            })
            .unwrap_or(false)
    }

    /// Determine whether an update request should use the LiteOn PSU path.
    ///
    /// Newer shelves can update all PSUs with no device ID. Older shelves reject
    /// that request and are handled by serial per-PSU fallback later.
    fn is_liteon_psu_update_request(cmd_args: &CmdArgs, update_file: &str) -> bool {
        Self::liteon_psu_request_from_special(cmd_args)
            || Self::looks_like_liteon_psu_package(update_file)
    }

    /// Parse LiteOn PSU firmware strings such as `0xf6 / 0xf8` into comparable bytes.
    fn parse_liteon_psu_version_pair(version: &str) -> Option<(u8, u8)> {
        let mut parts = version.split('/');
        let first = parts.next()?.trim();
        let second = parts.next()?.trim();
        if parts.next().is_some() {
            return None;
        }

        fn parse_part(part: &str) -> Option<u8> {
            let trimmed = part.trim();
            let hex = trimmed
                .strip_prefix("0x")
                .or_else(|| trimmed.strip_prefix("0X"))
                .unwrap_or(trimmed);
            u8::from_str_radix(hex, 16).ok()
        }

        Some((parse_part(first)?, parse_part(second)?))
    }

    /// Extract the expected PSU version pair from package names like `..._F6F8_...`.
    fn liteon_psu_package_target_version(update_file: &str) -> Option<(u8, u8)> {
        let filename = Path::new(update_file).file_name()?.to_str()?;
        filename.split('_').find_map(|part| {
            if part.len() == 4 && part.chars().all(|ch| ch.is_ascii_hexdigit()) {
                Some((
                    u8::from_str_radix(&part[0..2], 16).ok()?,
                    u8::from_str_radix(&part[2..4], 16).ok()?,
                ))
            } else {
                None
            }
        })
    }

    /// Compare LiteOn PSU version bytes exactly.
    ///
    /// `0606` and `C6C6` are distinct LiteOn PSU versions, so matching must not
    /// collapse high-nibble variants.
    fn liteon_psu_version_matches_target(reported: (u8, u8), target: (u8, u8)) -> bool {
        reported == target
    }

    /// Check whether a PowerUnit response reports the package target version.
    fn liteon_psu_response_matches_target_version(
        response_dict: &Value,
        target_version: (u8, u8),
    ) -> bool {
        response_dict
            .get("Version")
            .and_then(Value::as_str)
            .and_then(Self::parse_liteon_psu_version_pair)
            .map(|version| Self::liteon_psu_version_matches_target(version, target_version))
            .unwrap_or(false)
    }

    /// Check whether a PowerUnit is at the target version with no failure signal.
    fn liteon_psu_response_ready_at_target_version(
        response_dict: &Value,
        target_version: (u8, u8),
    ) -> bool {
        Self::liteon_psu_status_is_complete_for_target(response_dict, Some(target_version))
    }

    /// Decide whether a LiteOn PSU response is complete for the requested target.
    ///
    /// When a target version is known, completion means the reported version
    /// matches, the response has no failure state/status/error, and the shelf has
    /// rolled `updateProgress` back to 0. Without a target version, fall back to
    /// status-only completion, which accepts both 0 and 100 progress.
    fn liteon_psu_status_is_complete_for_target(
        response_dict: &Value,
        target_version: Option<(u8, u8)>,
    ) -> bool {
        if let Some(target_version) = target_version {
            return Self::liteon_psu_response_matches_target_version(response_dict, target_version)
                && Self::liteon_psu_status_is_non_failure(response_dict)
                && matches!(
                    response_dict.get("updateProgress").and_then(Value::as_i64),
                    Some(0)
                );
        }

        Self::liteon_psu_status_is_complete(response_dict)
    }

    /// Shared health gate for LiteOn PSU responses.
    fn liteon_psu_status_is_non_failure(response_dict: &Value) -> bool {
        response_dict.get("error").is_none()
            && !Self::liteon_psu_status_is_failure(
                response_dict.get("state").and_then(Value::as_str),
            )
            && !Self::liteon_psu_status_is_failure(
                response_dict.get("status").and_then(Value::as_str),
            )
    }

    /// Return true once a PSU reports active update progress.
    fn liteon_psu_update_activity_observed(response_dict: &Value) -> bool {
        matches!(
            response_dict.get("updateProgress").and_then(Value::as_i64),
            Some(1..=100)
        )
    }

    /// Decide whether polling observed a successful LiteOn PSU update completion.
    ///
    /// LiteOn shelves can report completion as either 100 or a rollover back to 0.
    /// When the package version is known, prefer the stronger signal: the device
    /// must report the target version and be back at progress 0.
    fn liteon_psu_status_is_successful_update_completion(
        response_dict: &Value,
        target_version: Option<(u8, u8)>,
        _observed_update_activity: bool,
    ) -> bool {
        if !Self::liteon_psu_status_is_non_failure(response_dict) {
            return false;
        }

        let update_progress = response_dict.get("updateProgress").and_then(Value::as_i64);
        let Some(target_version) = target_version else {
            return matches!(update_progress, Some(0 | 100));
        };

        Self::liteon_psu_response_matches_target_version(response_dict, target_version)
            && matches!(update_progress, Some(0))
    }

    /// Normalize LiteOn state/status strings into terminal failure detection.
    fn liteon_psu_status_is_failure(value: Option<&str>) -> bool {
        value
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "critical"
                        | "error"
                        | "exception"
                        | "failed"
                        | "failure"
                        | "cancelled"
                        | "canceled"
                        | "disabled"
                        | "warning"
                )
            })
            .unwrap_or(false)
    }

    /// Identify transient errors while polling LiteOn PSU status.
    ///
    /// During PSU updates the controller can briefly reset connections or return
    /// internal service errors even though the update continues.
    fn liteon_psu_status_error_is_retryable(response_dict: &Value) -> bool {
        if response_dict.get("error").is_none() {
            return false;
        }
        let error = response_dict.to_string().to_ascii_lowercase();
        error.contains("internalerror")
            || error.contains("internal service error")
            || error.contains("service is still operational")
            || error.contains("https get request failed")
            || error.contains("connection reset by peer")
            || error.contains("connection refused")
            || error.contains("connection closed")
            || error.contains("error sending request")
            || error.contains("operation timed out")
            || error.contains("timed out")
    }

    /// Identify transient failures from a LiteOn PSU update POST.
    ///
    /// Used for both all-PSU launch attempts and direct per-device POSTs. Older
    /// or busy shelves can return these errors even though a retry may succeed.
    fn liteon_psu_update_post_error_is_retryable(response_dict: &Value) -> bool {
        let error = response_dict.to_string().to_ascii_lowercase();
        error.contains("internalerror")
            || error.contains("internal service error")
            || error.contains("service is still operational")
            || error.contains("https post request failed")
            || error.contains("https request failed")
            || error.contains("request failed")
            || error.contains("connection reset by peer")
            || error.contains("connection refused")
            || error.contains("connection closed")
            || error.contains("error sending request")
    }

    /// Return true for structured errors that should stop polling immediately.
    fn liteon_psu_status_has_non_retryable_error(response_dict: &Value) -> bool {
        response_dict.get("error").is_some()
            && !Self::liteon_psu_status_error_is_retryable(response_dict)
    }

    /// Status-only LiteOn PSU completion check.
    ///
    /// This is used when no target version is available, so completion is based on
    /// non-failure state/status/error plus `updateProgress` at either 0 or 100.
    fn liteon_psu_status_is_complete(response_dict: &Value) -> bool {
        let state = response_dict.get("state").and_then(Value::as_str);
        let status = response_dict.get("status").and_then(Value::as_str);
        if Self::liteon_psu_status_is_failure(state)
            || Self::liteon_psu_status_is_failure(status)
            || response_dict.get("error").is_some()
        {
            return false;
        }

        matches!(
            response_dict.get("updateProgress").and_then(Value::as_i64),
            Some(0 | 100)
        )
    }

    fn record_json_error(
        mut json_dict: Option<&mut Value>,
        message: &str,
        details: Option<&Value>,
    ) {
        let Some(ref mut json_dict) = json_dict else {
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
            if !json_dict.get("Output").is_some_and(Value::is_array) {
                json_dict["Output"] = json!([]);
            }
            if let Some(output) = json_dict.get_mut("Output").and_then(Value::as_array_mut) {
                output.push(NvUtils::redact_secret_json_value(details));
            }
        }
    }

    /// Enumerate LiteOn PSU raw device IDs from the PowerUnits collection.
    async fn get_liteon_psu_device_ids(&self) -> Result<Vec<String>, Value> {
        let (status, response_dict) = self
            .bmc_access
            .dispatch_request("GET", LITEON_POWERUNITS_URI, None, None)
            .await;
        if !status {
            return Err(response_dict);
        }

        let mut device_ids: Vec<String> = response_dict
            .get("Members")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|member| member.get("@odata.id").and_then(Value::as_str))
            .filter_map(Self::liteon_psu_device_id_from_member_uri)
            .collect();
        device_ids.sort_by_key(|id| id.parse::<u64>().unwrap_or(u64::MAX));
        device_ids.dedup();

        Ok(device_ids)
    }

    /// Fetch the latest LiteOn PowerUnit status for a serial-update task ID.
    async fn liteon_psu_status(&self, task_id: &str) -> Result<Value, Value> {
        let liteon_uri = format!("{}/{}", LITEON_POWERUNITS_URI, task_id);
        let (status, response_dict) = self
            .target_access()
            .dispatch_request("GET", &liteon_uri, None, None)
            .await;

        if status {
            Ok(response_dict)
        } else {
            Err(response_dict)
        }
    }

    /// Validate every LiteOn PowerUnit after an all-PSU task reports success.
    ///
    /// Newer LiteOn firmware can launch a single all-PSU task, but the TaskService
    /// success signal can arrive before every PowerUnit inventory entry has
    /// converged to the package target version. RMS should only see success after
    /// the PowerUnit reads agree with the package, or a bounded settle window
    /// expires with a clear validation failure.
    async fn validate_liteon_all_psus_at_target(
        &self,
        task_id: &str,
        update_file: &str,
    ) -> Result<Value, Value> {
        let Some(target_version) = Self::liteon_psu_package_target_version(update_file) else {
            return Ok(json!({
                "stage": "liteon_psu_all_validation",
                "task_id": task_id,
                "skipped": true,
                "reason": "package target version could not be inferred",
            }));
        };

        let expected = Self::liteon_psu_target_version_label(target_version);
        let started = Instant::now();
        loop {
            match self
                .validate_liteon_all_psus_at_target_once(task_id, target_version, &expected)
                .await
            {
                Ok(validation) => return Ok(validation),
                Err(mut validation_failure) => {
                    if started.elapsed()
                        >= Duration::from_secs(LITEON_PSU_ALL_TARGET_SETTLE_TIMEOUT_SECS)
                    {
                        validation_failure["validation_wait_seconds"] =
                            json!(LITEON_PSU_ALL_TARGET_SETTLE_TIMEOUT_SECS);
                        if let Some(message) = validation_failure
                            .get("Messages")
                            .and_then(Value::as_array)
                            .and_then(|messages| messages.first())
                            .and_then(|message| message.get("Message"))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                        {
                            validation_failure["Messages"][0]["Message"] = json!(format!(
                                "{message} after waiting up to {}s for PowerUnit inventory convergence.",
                                LITEON_PSU_ALL_TARGET_SETTLE_TIMEOUT_SECS
                            ));
                        }
                        return Err(validation_failure);
                    }

                    tracing::warn!(
                        task_id = %NvUtils::sanitize_log(task_id),
                        expected = %expected,
                        elapsed_seconds = started.elapsed().as_secs(),
                        timeout_seconds = LITEON_PSU_ALL_TARGET_SETTLE_TIMEOUT_SECS,
                        "LiteOn all-PSU task completed before all PowerUnits reported the target version; retrying validation"
                    );
                    sleep(Duration::from_secs(
                        LITEON_PSU_ALL_TARGET_SETTLE_POLL_INTERVAL_SECS,
                    ))
                    .await;
                }
            }
        }
    }

    async fn validate_liteon_all_psus_at_target_once(
        &self,
        task_id: &str,
        target_version: (u8, u8),
        expected: &str,
    ) -> Result<Value, Value> {
        let device_ids = self.get_liteon_psu_device_ids().await.map_err(|error| {
            Self::liteon_psu_validation_failure(
                task_id,
                expected,
                Vec::new(),
                Some(format!(
                    "unable to enumerate LiteOn PowerUnits: {:?}",
                    NvUtils::redact_secret_json_value(&error)
                )),
            )
        })?;
        if device_ids.is_empty() {
            return Err(Self::liteon_psu_validation_failure(
                task_id,
                expected,
                Vec::new(),
                Some("no LiteOn PowerUnits were found".to_string()),
            ));
        }

        let mut devices = Vec::new();
        let mut mismatched = Vec::new();
        for device_id in device_ids {
            let powerdevice = Self::liteon_powerdevice_name(&device_id);
            let status = match self.liteon_psu_status(&powerdevice).await {
                Ok(status) => status,
                Err(error) => {
                    let observed = format!(
                        "{powerdevice}=unreadable ({:?})",
                        NvUtils::redact_secret_json_value(&error)
                    );
                    mismatched.push(observed.clone());
                    devices.push(json!({
                        "device_id": device_id,
                        "task_id": powerdevice,
                        "expected": expected,
                        "at_target": false,
                        "error": NvUtils::redact_secret_json_value(&error),
                    }));
                    continue;
                }
            };

            let version = status
                .get("Version")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let at_target =
                Self::liteon_psu_response_ready_at_target_version(&status, target_version);
            if !at_target {
                mismatched.push(format!("{powerdevice}={version}"));
            }

            devices.push(json!({
                "device_id": device_id,
                "task_id": powerdevice,
                "version": version,
                "expected": expected,
                "at_target": at_target,
                "state": status.get("state").cloned().unwrap_or(Value::Null),
                "status": status.get("status").cloned().unwrap_or(Value::Null),
                "updateProgress": status.get("updateProgress").cloned().unwrap_or(Value::Null),
            }));
        }

        if mismatched.is_empty() {
            Ok(json!({
                "stage": "liteon_psu_all_validation",
                "task_id": task_id,
                "expected": expected,
                "TaskState": "Completed",
                "TaskStatus": "OK",
                "PercentComplete": 100,
                "devices": devices,
            }))
        } else {
            let mut failure =
                Self::liteon_psu_validation_failure(task_id, expected, mismatched, None);
            failure["devices"] = Value::Array(devices);
            Err(failure)
        }
    }

    fn liteon_psu_validation_failure(
        task_id: &str,
        expected: &str,
        mismatched: Vec<String>,
        reason: Option<String>,
    ) -> Value {
        let mismatch_text = if mismatched.is_empty() {
            reason.unwrap_or_else(|| "unable to validate LiteOn PSU versions".to_string())
        } else {
            format!("mismatched PSUs: {}", mismatched.join(", "))
        };
        let message = format!(
            "LiteOn all-PSU update task {task_id} completed, but PSU version validation failed: expected {expected}; {mismatch_text}"
        );
        json!({
            "Id": task_id,
            "TaskState": "Exception",
            "TaskStatus": "Critical",
            "PercentComplete": 100,
            "stage": "liteon_psu_all_validation",
            "expected": expected,
            "mismatched": mismatched,
            "Messages": [{
                "Message": message,
                "MessageSeverity": "Critical",
                "Resolution": "Retry the LiteOn PSU update after the PowerShelf is healthy."
            }]
        })
    }

    /// Poll a single LiteOn PSU until the update succeeds, fails, or times out.
    ///
    /// Older shelves update one PSU at a time and expose progress through the
    /// PowerUnit resource instead of a TaskService task.
    async fn wait_for_liteon_psu_update_complete(
        &self,
        task_id: &str,
        time_out: u64,
        target_version: Option<(u8, u8)>,
    ) -> Result<Value, Value> {
        let deadline = Instant::now() + Duration::from_secs(time_out.max(1));
        let mut observed_update_activity = false;

        loop {
            let response_dict = match self.liteon_psu_status(task_id).await {
                Ok(response_dict) => response_dict,
                Err(response_dict) => {
                    if Self::liteon_psu_status_error_is_retryable(&response_dict) {
                        response_dict
                    } else {
                        return Err(response_dict);
                    }
                }
            };

            if Self::liteon_psu_status_has_non_retryable_error(&response_dict)
                || Self::liteon_psu_status_is_failure(
                    response_dict.get("state").and_then(Value::as_str),
                )
                || Self::liteon_psu_status_is_failure(
                    response_dict.get("status").and_then(Value::as_str),
                )
            {
                return Err(response_dict);
            }

            if Self::liteon_psu_update_activity_observed(&response_dict) {
                observed_update_activity = true;
            }

            if Self::liteon_psu_status_is_successful_update_completion(
                &response_dict,
                target_version,
                observed_update_activity,
            ) {
                return Ok(response_dict);
            }

            if Instant::now() >= deadline {
                return Err(json!({
                    "error": "LiteOn PSU update timed out",
                    "task_id": task_id,
                    "last_response": NvUtils::redact_secret_json_value(&response_dict),
                }));
            }

            sleep(Duration::from_secs(LITEON_PSU_STATUS_POLL_INTERVAL_SECS)).await;
        }
    }

    /// Re-read a PSU after monitor failure and accept it if it reached the target.
    ///
    /// Older LiteOn shelves can briefly report stale timeout/error state while the
    /// update has actually landed. This final read keeps a completed PSU from
    /// being retried or failed unnecessarily.
    async fn liteon_psu_final_status_if_complete(
        &self,
        task_id: &str,
        target_version: Option<(u8, u8)>,
    ) -> Option<Value> {
        let target_version = target_version?;
        let status = self.liteon_psu_status(task_id).await.ok()?;
        if Self::liteon_psu_status_is_complete_for_target(&status, Some(target_version)) {
            Some(status)
        } else {
            None
        }
    }

    /// Whether a per-PSU monitor error is transient enough to retry the device.
    fn liteon_psu_serial_monitor_error_is_retryable(error: &Value) -> bool {
        error
            .get("error")
            .and_then(Value::as_str)
            .map(|message| message.eq_ignore_ascii_case("LiteOn PSU update timed out"))
            .unwrap_or(false)
            || error
                .get("last_response")
                .map(Self::liteon_psu_status_error_is_retryable)
                .unwrap_or(false)
            || Self::liteon_psu_status_error_is_retryable(error)
    }

    /// Fallback path for older LiteOn shelves that cannot launch an all-PSU update.
    ///
    /// Each PowerUnit is updated and monitored serially. The aggregate task is
    /// only reported complete after every PSU either reaches the package version
    /// or finishes its per-device update.
    async fn update_liteon_psus_serial(
        &self,
        update_file: &str,
        time_out: u64,
        mut json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        let device_ids = match self.get_liteon_psu_device_ids().await {
            Ok(device_ids) if !device_ids.is_empty() => device_ids,
            Ok(_) => {
                let message = "LiteOn PSU fallback failed: no PowerUnits members were found";
                Self::record_json_error(json_dict.as_deref_mut(), message, None);
                Util::bail_nvfwupd_threadsafe(
                    1,
                    message,
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                    parallel_update,
                );
                return None;
            }
            Err(error) => {
                let message = format!(
                    "LiteOn PSU fallback failed: unable to enumerate PowerUnits {:?}",
                    NvUtils::redact_secret_json_value(&error)
                );
                Self::record_json_error(json_dict.as_deref_mut(), &message, Some(&error));
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &message,
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                    parallel_update,
                );
                return None;
            }
        };

        let mut devices = Vec::new();
        let target_version = Self::liteon_psu_package_target_version(update_file);
        let monitor_timeout = time_out.max(LITEON_PSU_SERIAL_MONITOR_TIMEOUT_SECS);
        let device_count = device_ids.len();
        for (device_index, device_id) in device_ids.into_iter().enumerate() {
            let task_id = Self::liteon_powerdevice_name(&device_id);
            if let Some(target_version) = target_version {
                if let Ok(status) = self.liteon_psu_status(&task_id).await {
                    let status = if Self::liteon_psu_response_ready_at_target_version(
                        &status,
                        target_version,
                    ) {
                        status
                    } else if Self::liteon_psu_response_matches_target_version(
                        &status,
                        target_version,
                    ) {
                        match self
                            .wait_for_liteon_psu_update_complete(
                                &task_id,
                                monitor_timeout,
                                Some(target_version),
                            )
                            .await
                        {
                            Ok(status) => status,
                            Err(error) => {
                                let message = format!(
                                    "LiteOn PSU firmware update did not finish for {}: {:?}",
                                    task_id,
                                    NvUtils::redact_secret_json_value(&error)
                                );
                                Self::record_json_error(
                                    json_dict.as_deref_mut(),
                                    &message,
                                    Some(&error),
                                );
                                Util::bail_nvfwupd_threadsafe(
                                    1,
                                    &message,
                                    BailAction::DoNothing,
                                    json_dict.as_deref(),
                                    parallel_update,
                                );
                                return None;
                            }
                        }
                    } else {
                        status
                    };

                    if Self::liteon_psu_response_ready_at_target_version(&status, target_version) {
                        devices.push(json!({
                            "device_id": device_id,
                            "task_id": task_id,
                            "stage": "skipped",
                            "reason": "already_at_target_version",
                            "version": status.get("Version").cloned().unwrap_or(Value::Null),
                            "state": status.get("state").cloned().unwrap_or(Value::Null),
                            "status": status.get("status").cloned().unwrap_or(Value::Null),
                            "updateProgress": status.get("updateProgress").cloned().unwrap_or(Value::Null),
                        }));
                        continue;
                    }
                }
            }

            let mut status = None;
            let mut last_error = None;
            let mut attempts = 0;
            while attempts < LITEON_PSU_SERIAL_DEVICE_MAX_ATTEMPTS {
                attempts += 1;
                if attempts > 1 {
                    tracing::warn!(
                        "Retrying LiteOn PSU serial update for {} after previous monitor failure",
                        task_id
                    );
                    Self::settle_before_liteon_psu_serial_retry().await;
                }

                let started_task_id = self
                    .update_liteon_psu(
                        update_file,
                        &device_id,
                        time_out,
                        json_dict.as_deref_mut(),
                        parallel_update,
                    )
                    .await?;

                match self
                    .wait_for_liteon_psu_update_complete(
                        &started_task_id,
                        monitor_timeout,
                        target_version,
                    )
                    .await
                {
                    Ok(completed_status) => {
                        status = Some(completed_status);
                        break;
                    }
                    Err(error) => {
                        if let Some(completed_status) = self
                            .liteon_psu_final_status_if_complete(&task_id, target_version)
                            .await
                        {
                            status = Some(completed_status);
                            break;
                        }

                        let retryable = Self::liteon_psu_serial_monitor_error_is_retryable(&error);
                        last_error = Some(error);
                        if !retryable || attempts >= LITEON_PSU_SERIAL_DEVICE_MAX_ATTEMPTS {
                            break;
                        }
                    }
                }
            }

            let status = match status {
                Some(status) => status,
                None => {
                    let error = last_error.unwrap_or_else(|| {
                        json!({
                            "error": "LiteOn PSU serial update did not return status",
                            "task_id": task_id.clone(),
                        })
                    });
                    let message = format!(
                        "LiteOn PSU firmware update failed for {} after {} attempt(s): {:?}",
                        task_id,
                        attempts,
                        NvUtils::redact_secret_json_value(&error)
                    );
                    Self::record_json_error(json_dict.as_deref_mut(), &message, Some(&error));
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        &message,
                        BailAction::DoNothing,
                        json_dict.as_deref(),
                        parallel_update,
                    );
                    return None;
                }
            };

            devices.push(json!({
                "device_id": device_id,
                "task_id": task_id,
                "attempts": attempts,
                "version": status.get("Version").cloned().unwrap_or(Value::Null),
                "state": status.get("state").cloned().unwrap_or(Value::Null),
                "status": status.get("status").cloned().unwrap_or(Value::Null),
                "updateProgress": status.get("updateProgress").cloned().unwrap_or(Value::Null),
            }));
            if device_index + 1 < device_count {
                Self::settle_after_liteon_psu_serial_update().await;
            }
        }

        if let Some(ref mut jd) = json_dict {
            if let Some(output) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(json!({
                    "Id": LITEON_PSU_SERIAL_COMPLETE_TASK_ID,
                    "TaskState": "Completed",
                    "TaskStatus": "OK",
                    "PercentComplete": 100,
                    "stage": "liteon_psu_serial",
                    "devices": devices,
                }));
            }
        }

        Some(LITEON_PSU_SERIAL_COMPLETE_TASK_ID.to_string())
    }

    async fn repair_failed_liteon_all_psu_task(
        &self,
        task_id: &str,
        update_file: &str,
        mut json_dict: Option<&mut Value>,
    ) -> Option<Value> {
        tracing::warn!(
            "LiteOn all-PSU task {} failed; checking PSU versions and retrying incomplete devices serially",
            NvUtils::sanitize_log(task_id)
        );

        let repair_task_id = self
            .update_liteon_psus_serial(
                update_file,
                LITEON_PSU_SERIAL_MONITOR_TIMEOUT_SECS,
                json_dict.as_deref_mut(),
                true,
            )
            .await?;

        if let Some(ref jd) = json_dict {
            if let Some(summary) = jd
                .get("Output")
                .and_then(Value::as_array)
                .and_then(|output| {
                    output.iter().rev().find(|entry| {
                        entry.get("Id").and_then(Value::as_str) == Some(repair_task_id.as_str())
                    })
                })
            {
                return Some(summary.clone());
            }
        }

        Some(json!({
            "Id": repair_task_id,
            "TaskState": "Completed",
            "TaskStatus": "OK",
            "PercentComplete": 100,
            "stage": "liteon_psu_serial_repair",
            "repaired_after_task": task_id,
        }))
    }

    #[cfg(not(test))]
    async fn settle_after_liteon_psu_serial_update() {
        sleep(Duration::from_secs(LITEON_PSU_SERIAL_SETTLE_SECS)).await;
    }

    #[cfg(not(test))]
    async fn settle_before_liteon_psu_serial_retry() {
        sleep(Duration::from_secs(
            LITEON_PSU_SERIAL_DEVICE_RETRY_SETTLE_SECS,
        ))
        .await;
    }

    #[cfg(test)]
    async fn settle_after_liteon_psu_serial_update() {}

    #[cfg(test)]
    async fn settle_before_liteon_psu_serial_retry() {}

    async fn dispatch_liteon_psu_update_post_with_retry(
        &self,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        parallel_update: bool,
        extra_headers: Option<&HashMap<String, String>>,
    ) -> (bool, Value) {
        let mut attempt = 1;
        loop {
            let (status, response_dict) = self
                .target_access()
                .dispatch_file_upload(
                    update_uri,
                    update_file,
                    time_out,
                    None,
                    parallel_update,
                    extra_headers,
                )
                .await;

            if status
                || attempt >= LITEON_PSU_UPDATE_POST_MAX_ATTEMPTS
                || !Self::liteon_psu_update_post_error_is_retryable(&response_dict)
            {
                return (status, response_dict);
            }

            tracing::warn!(
                "LiteOn PSU update POST attempt {} failed with retryable error {}; retrying in {}s",
                attempt,
                NvUtils::sanitize_log(&response_dict.to_string()),
                LITEON_PSU_UPDATE_POST_RETRY_INTERVAL_SECS
            );
            sleep(Duration::from_secs(
                LITEON_PSU_UPDATE_POST_RETRY_INTERVAL_SECS,
            ))
            .await;
            attempt += 1;
        }
    }

    /// Perform LiteOn PSU-specific update via custom URI and device ID header.
    async fn update_liteon_psu(
        &self,
        update_file: &str,
        device_id: &str,
        time_out: u64,
        mut json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        let mut extra_headers = HashMap::new();
        extra_headers.insert("LiteonPowerDeviceId".to_string(), device_id.to_string());
        let task_id = Self::liteon_powerdevice_name(device_id);

        if !parallel_update && json_dict.is_none() {
            println!(
                "Performing LiteOn PSU firmware update (Device ID: {})...",
                device_id
            );
        }

        let (status, response_dict) = self
            .dispatch_liteon_psu_update_post_with_retry(
                LITEON_PSU_UPDATE_URI,
                update_file,
                time_out,
                parallel_update,
                Some(&extra_headers),
            )
            .await;

        if !status {
            let message = format!(
                "LiteOn PSU firmware upload failed for Device ID {} with error {:?}",
                device_id,
                NvUtils::redact_secret_json_value(&response_dict)
            );
            Self::record_json_error(json_dict.as_deref_mut(), &message, Some(&response_dict));
            Util::bail_nvfwupd_threadsafe(
                1,
                &message,
                BailAction::DoNothing,
                json_dict.as_deref(),
                parallel_update,
            );
            return None;
        }

        let result = response_dict
            .get("Result")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !result.eq_ignore_ascii_case("success") {
            let message = format!(
                "LiteOn PSU firmware update failed for Device ID {}. Response: {:?}",
                device_id,
                NvUtils::redact_secret_json_value(&response_dict)
            );
            Self::record_json_error(json_dict.as_deref_mut(), &message, Some(&response_dict));
            Util::bail_nvfwupd_threadsafe(
                1,
                &message,
                BailAction::DoNothing,
                json_dict.as_deref(),
                parallel_update,
            );
            return None;
        }

        if let Some(ref mut jd) = json_dict {
            if let Some(output) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(json!({
                    "Id": task_id.clone(),
                    "Result": result,
                }));
            }
        }

        if !parallel_update && json_dict.is_none() {
            println!(
                "LiteOn PSU firmware update started successfully (Device ID: {})",
                device_id
            );
            println!(
                "Run \"show_update_progress -i powerdevice{}\" to monitor update progress.",
                device_id
            );
        }

        Some(task_id)
    }
}

#[async_trait::async_trait]
impl RFTarget for PowerShelfRFTarget {
    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

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
        self.config_dict.as_ref()
    }

    fn class_name(&self) -> &str {
        "PowerShelfRFTarget"
    }

    // ------------------------------------------------------------------
    // get_update_uri override
    // ------------------------------------------------------------------

    /// PowerShelf uses `HttpPushUri` (not `MultipartHttpPushUri`).
    fn get_update_uri(&self, update_service_response: &Value) -> String {
        if let Some(uri) = update_service_response
            .get("HttpPushUri")
            .and_then(|v| v.as_str())
        {
            return uri.to_string();
        }
        "/redfish/v1/UpdateService/update".to_string()
    }

    // ------------------------------------------------------------------
    // factory_reset override
    // ------------------------------------------------------------------

    async fn factory_reset(&mut self, _reset_params: Option<&Value>) -> (bool, Value) {
        Util::bail_nvfwupd(
            1,
            "Factory reset is not supported for PowerShelf",
            BailAction::Exit,
            None,
        );
        (false, Value::Null)
    }

    // ------------------------------------------------------------------
    // run_oob_activation override
    // ------------------------------------------------------------------

    /// Supports RF_PWRSHELF_RESET (GracefulRestart) and
    /// RF_PWRSHELF_RESET_FORCE (ForceRestart) via Manager.Reset.
    async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        let mut quiet_json = json!({"Error": [], "Error Code": 0, "Output": []});
        let command_to_arg = [
            ("RF_PWRSHELF_RESET", "GracefulRestart"),
            ("RF_PWRSHELF_RESET_FORCE", "ForceRestart"),
        ];
        let supported: Vec<&str> = command_to_arg.iter().map(|(k, _)| *k).collect();

        if !supported.contains(&cmd_args.cmd.as_str()) {
            Util::bail_nvfwupd(
                1,
                &format!(
                    "Activation command {} not supported for PowerShelf. \
                     Supported commands: {:?}",
                    cmd_args.cmd, supported
                ),
                BailAction::DoNothing,
                cmd_args.quiet.then_some(&quiet_json),
            );
            return 1;
        }

        let reset_type = command_to_arg
            .iter()
            .find(|(k, _)| *k == cmd_args.cmd.as_str())
            .map(|(_, v)| *v)
            .unwrap();
        self.reset_recovery_attempts = None;

        let (status, managers_response) = self
            .bmc_access
            .dispatch_request_full("GET", "/redfish/v1/Managers", None, None, 30, true, None)
            .await;

        let mut target_uri: Option<String> = None;
        if status {
            if let Some(members) = managers_response.get("Members").and_then(|v| v.as_array()) {
                for member in members {
                    if let Some(manager_uri) = member.get("@odata.id").and_then(|v| v.as_str()) {
                        let (ok, manager_dict) = self
                            .bmc_access
                            .dispatch_request_full("GET", manager_uri, None, None, 30, true, None)
                            .await;
                        if !ok {
                            continue;
                        }
                        if let Some(uri) = manager_dict
                            .get("Actions")
                            .and_then(|a| a.get("#Manager.Reset"))
                            .and_then(|r| r.get("target"))
                            .and_then(|t| t.as_str())
                        {
                            target_uri = Some(uri.to_string());
                            break;
                        }
                    }
                }
            }
        }

        if target_uri.is_none() {
            Util::bail_nvfwupd(
                1,
                "Error: Unable to find Manager Reset URI on target system.",
                BailAction::DoNothing,
                cmd_args.quiet.then_some(&quiet_json),
            );
            return 1;
        }

        let uri = target_uri.unwrap();
        let body = json!({"ResetType": reset_type});
        let full_url = Self::manager_reset_request_url(&self.bmc_access.base_url, &uri);
        let post_result = self
            .bmc_access
            .client
            .post(&full_url)
            .basic_auth(&self.bmc_access.user, Some(&self.bmc_access.password))
            .header("Content-Type", "application/json")
            .json(&body)
            .timeout(Duration::from_secs(30))
            .send()
            .await;

        match post_result {
            Ok(response) => {
                let status_code = response.status().as_u16();
                let response_body = response.text().await.unwrap_or_default();
                let response_json = serde_json::from_str::<Value>(&response_body)
                    .unwrap_or_else(|_| json!({"response": response_body}));

                if [200, 201, 202, 204].contains(&status_code) {
                    if !cmd_args.quiet {
                        println!("{} requested successfully.", cmd_args.cmd);
                        println!(
                            "PowerShelf Manager reset ({}) initiated at {}",
                            reset_type, uri
                        );
                        println!("Server response:");
                        println!("{}", NvUtils::redacted_json_pretty_4space(&response_json));
                    }
                    0
                } else {
                    Util::bail_nvfwupd(
                        1,
                        &format!(
                            "Error: {} failed with HTTP status {}. Response: {:?}",
                            cmd_args.cmd,
                            status_code,
                            NvUtils::redact_secret_json_value(&response_json)
                        ),
                        BailAction::DoNothing,
                        cmd_args.quiet.then_some(&quiet_json),
                    );
                    1
                }
            }
            Err(error) => {
                if self.target_recovers_after_reset_transport_error().await {
                    if !cmd_args.quiet {
                        println!(
                            "{} request lost its HTTPS response while the PowerShelf reset was in progress.",
                            cmd_args.cmd
                        );
                        println!(
                            "PowerShelf Manager reset ({}) initiated at {}",
                            reset_type, uri
                        );
                        println!("Management endpoint is reachable again.");
                    }
                    0
                } else {
                    Util::bail_nvfwupd(
                        1,
                        &format!(
                            "Error: {} failed; HTTPS request error: {}",
                            cmd_args.cmd,
                            NvUtils::sanitize_log(&error.to_string())
                        ),
                        BailAction::DoNothing,
                        cmd_args.quiet.then_some(&quiet_json),
                    );
                    1
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // process_job_status override
    // ------------------------------------------------------------------

    /// Handles LiteOn `powerdevice<id>` task IDs via the LiteOn PowerUnit URI.
    /// Falls back to the standard Redfish task monitoring for other IDs.
    async fn process_job_status(&self, task_id: &str, mut print_json: Option<&mut Value>) -> i32 {
        if task_id == LITEON_PSU_SERIAL_COMPLETE_TASK_ID {
            if let Some(ref mut pj) = print_json {
                if let Some(output) = pj.get_mut("Output").and_then(|v| v.as_array_mut()) {
                    output.push(json!({
                        "Id": LITEON_PSU_SERIAL_COMPLETE_TASK_ID,
                        "TaskState": "Completed",
                        "TaskStatus": "OK",
                        "PercentComplete": 100,
                        "stage": "liteon_psu_serial",
                    }));
                }
            } else {
                tracing::info!(indent = 0, "LiteOn PSU serial update completed.");
            }
            return 0;
        }

        if !task_id.starts_with("powerdevice") {
            // Non-LiteOn path: use the standard Redfish task service
            // with full print_job_status (StartTime, EndTime, duration, etc.)
            if print_json.is_none() {
                tracing::info!(indent = 2, "Task Info for Id: {}", task_id);
            }
            let task_service_uri = self.get_task_service_uri(task_id);
            let (status, my_dict) = self
                .target_access()
                .dispatch_request("GET", &task_service_uri, None, print_json.as_deref_mut())
                .await;
            if !status {
                if let Some(ref mut pj) = print_json {
                    if let Some(err_arr) = pj.get_mut("Error").and_then(|v| v.as_array_mut()) {
                        err_arr.push(json!(format!("Error: Unable to query task {}", task_id)));
                    }
                } else {
                    println!("Error: Unable to query task {}", task_id);
                }
                return 1;
            }

            if Self::is_on_reset_handoff_task(&my_dict) {
                if let Some(ref mut pj) = print_json {
                    if let Some(output) = pj.get_mut("Output").and_then(|v| v.as_array_mut()) {
                        output.push(NvUtils::redact_secret_json_value(&my_dict));
                    }
                } else {
                    tracing::info!(
                        indent = 2,
                        "PowerShelf OnReset update reached reset handoff; run activation to apply firmware."
                    );
                    tracing::info!(
                        indent = 2,
                        "{}",
                        NvUtils::redacted_json_pretty_4space(&my_dict)
                    );
                }
                return 0;
            }

            // Delegate to print_job_status for full task details
            let (err_status, _) = self.print_job_status(task_id, &my_dict, status, print_json);
            return err_status;
        }

        // LiteOn powerdevice handler
        let liteon_uri = format!(
            "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/{}",
            task_id
        );

        let (status, response_dict) = self
            .target_access()
            .dispatch_request("GET", &liteon_uri, None, None)
            .await;

        if let Some(ref mut pj) = print_json {
            if response_dict.get("error").is_some() {
                if let Some(err_arr) = pj.get_mut("Error").and_then(|v| v.as_array_mut()) {
                    err_arr.push(json!(format!(
                        "Input Device ID does not exist: {}",
                        task_id
                    )));
                }
                return 1;
            }
            let filtered_keys = [
                "@odata.id",
                "@odata.type",
                "Id",
                "Model",
                "Name",
                "SerialNumber",
                "Version",
                "state",
                "status",
                "updateProgress",
            ];
            let mut filtered = serde_json::Map::new();
            if let Some(obj) = response_dict.as_object() {
                for key in &filtered_keys {
                    if let Some(val) = obj.get(*key) {
                        filtered.insert(key.to_string(), NvUtils::redact_secret_json_value(val));
                    }
                }
            }
            if let Some(output) = pj.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(Value::Object(filtered));
            }
            return if status { 0 } else { 1 };
        }

        // Non-JSON mode
        if !status {
            tracing::info!(
                indent = 0,
                "Error querying LiteOn PSU update progress: {:?}",
                NvUtils::redact_secret_json_value(&response_dict)
            );
            return 1;
        }

        if response_dict.get("error").is_some() {
            tracing::info!(indent = 0, "Input Device ID does not exist: {}", task_id);
            return 1;
        }

        tracing::info!(indent = 0, "LiteOn PSU Update Info for Device: {}", task_id);

        if let Some(v) = response_dict.get("Name").and_then(|v| v.as_str()) {
            tracing::info!(indent = 1, "Name: {}", NvUtils::sanitize_log(v));
        }
        if let Some(v) = response_dict.get("Model").and_then(|v| v.as_str()) {
            tracing::info!(indent = 1, "Model: {}", NvUtils::sanitize_log(v));
        }
        if let Some(v) = response_dict.get("SerialNumber").and_then(|v| v.as_str()) {
            tracing::info!(indent = 1, "SerialNumber: {}", NvUtils::sanitize_log(v));
        }
        if let Some(v) = response_dict.get("Version").and_then(|v| v.as_str()) {
            tracing::info!(indent = 1, "Version: {}", NvUtils::sanitize_log(v));
        }
        if let Some(v) = response_dict.get("status").and_then(|v| v.as_str()) {
            tracing::info!(indent = 1, "Status: {}", NvUtils::sanitize_log(v));
        }
        if let Some(v) = response_dict.get("state").and_then(|v| v.as_str()) {
            tracing::info!(indent = 1, "State: {}", NvUtils::sanitize_log(v));
        }

        if let Some(progress) = response_dict.get("updateProgress").and_then(|v| v.as_i64()) {
            tracing::info!(indent = 1, "UpdateProgress: {}%", progress);
            if progress == 0 {
                tracing::info!(indent = 1, "Update has not started or is in initial stage.");
            } else if progress < 100 {
                tracing::info!(indent = 1, "Update is in progress.");
            } else if progress == 100 {
                tracing::info!(indent = 1, "Update is complete.");
            }
        } else {
            tracing::info!(indent = 1, "UpdateProgress: Not available");
        }

        tracing::info!(
            indent = 1,
            "Full Response: {}",
            NvUtils::redacted_json_pretty_4space(&response_dict)
        );

        0
    }

    async fn query_job_status(
        &self,
        task_id: &str,
        mut print_json: Option<&mut Value>,
    ) -> (bool, Value) {
        if task_id == LITEON_PSU_SERIAL_COMPLETE_TASK_ID {
            let details = json!({
                "Id": LITEON_PSU_SERIAL_COMPLETE_TASK_ID,
                "TaskState": "Completed",
                "TaskStatus": "OK",
                "PercentComplete": 100,
                "stage": "liteon_psu_serial",
            });
            if let Some(ref mut pj) = print_json {
                if let Some(output) = pj.get_mut("Output").and_then(|v| v.as_array_mut()) {
                    output.push(details.clone());
                }
            }
            return (true, details);
        }

        if task_id.starts_with("powerdevice") {
            let liteon_uri = format!("{}/{}", LITEON_POWERUNITS_URI, task_id);
            let (status, response_dict) = self
                .target_access()
                .dispatch_request("GET", &liteon_uri, None, None)
                .await;
            if let Some(ref mut pj) = print_json {
                if let Some(output) = pj.get_mut("Output").and_then(|v| v.as_array_mut()) {
                    output.push(NvUtils::redact_secret_json_value(&response_dict));
                }
            }
            return (status, response_dict);
        }

        let task_service_uri = self.get_task_service_uri(task_id);
        let (status, response_dict) = self
            .target_access()
            .dispatch_request("GET", &task_service_uri, None, print_json.as_deref_mut())
            .await;

        if status {
            if Self::redfish_task_failed_for_liteon_psu_repair(&response_dict) {
                if let Some(update_file) = self.liteon_psu_all_task_update_file(task_id) {
                    let repaired_details = self
                        .repair_failed_liteon_all_psu_task(task_id, &update_file, print_json)
                        .await;
                    self.clear_liteon_psu_all_task_for_repair(task_id);
                    if let Some(repaired_details) = repaired_details {
                        return (true, repaired_details);
                    }
                }
            } else if Self::redfish_task_completed_without_liteon_psu_failure(&response_dict) {
                if let Some(update_file) = self.liteon_psu_all_task_update_file(task_id) {
                    match self
                        .validate_liteon_all_psus_at_target(task_id, &update_file)
                        .await
                    {
                        Ok(validation) => {
                            if let Some(ref mut pj) = print_json {
                                if let Some(output) =
                                    pj.get_mut("Output").and_then(|v| v.as_array_mut())
                                {
                                    output.push(validation);
                                }
                            }
                        }
                        Err(validation_failure) => {
                            self.clear_liteon_psu_all_task_for_repair(task_id);
                            if let Some(ref mut pj) = print_json {
                                if let Some(output) =
                                    pj.get_mut("Output").and_then(|v| v.as_array_mut())
                                {
                                    output.push(validation_failure.clone());
                                }
                            }
                            return (true, validation_failure);
                        }
                    }
                }
                self.clear_liteon_psu_all_task_for_repair(task_id);
            }
        }

        (status, response_dict)
    }

    // ------------------------------------------------------------------
    // update_component override
    // ------------------------------------------------------------------

    /// Perform firmware update for power shelf platforms.
    ///
    /// - LiteOn PSU with explicit `LiteOnPowerDeviceId`: direct OEM single-PSU update.
    /// - Standard tar/push-URI: sets ApplyTime, then uploads via push URI. If this
    ///   fails for a LiteOn PSU request, fallback updates each PSU serially.
    /// - PLDM: uses multipart upload.
    async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        mut json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        // Check for LiteOn PSU update path
        if let Some(device_id) = Self::get_liteon_device_id(cmd_args).await {
            return self
                .update_liteon_psu(
                    update_file,
                    &device_id,
                    time_out,
                    json_dict,
                    parallel_update,
                )
                .await;
        }

        // Standard PowerShelf update: set ApplyTime then push-URI upload
        // for ALL package types (tar, fwpkg, etc.) — no multipart needed.
        let apply_time = match Self::get_apply_time(cmd_args).await {
            Ok(apply_time) => apply_time,
            Err(message) => {
                Self::record_json_error(json_dict.as_deref_mut(), &message, None);
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &message,
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                    parallel_update,
                );
                return None;
            }
        };

        if !self
            .set_applytime(&apply_time, json_dict.as_deref_mut(), parallel_update)
            .await
        {
            let message = format!(
                "Error: Could not set Redfish Powershelf ApplyTime to '{}'",
                apply_time
            );
            Self::record_json_error(json_dict.as_deref_mut(), &message, None);
            Util::bail_nvfwupd_threadsafe(
                1,
                &message,
                BailAction::DoNothing,
                json_dict.as_deref(),
                parallel_update,
            );
            return None;
        }

        let is_liteon_psu_request = Self::is_liteon_psu_update_request(cmd_args, update_file);
        let is_on_reset_request = apply_time.eq_ignore_ascii_case("OnReset");
        let existing_on_reset_handoff_task_ids: HashSet<String> =
            if is_on_reset_request && !is_liteon_psu_request {
                self.on_reset_handoff_tasks()
                    .await
                    .into_iter()
                    .map(|(task_id, _)| task_id)
                    .collect()
            } else {
                HashSet::new()
            };
        let upload_timeout = if is_on_reset_request && !is_liteon_psu_request {
            time_out.min(POWERSHELF_ON_RESET_UPLOAD_RESPONSE_TIMEOUT_SECS)
        } else {
            time_out
        };
        let (status, response_dict) = if is_liteon_psu_request {
            self.dispatch_liteon_psu_update_post_with_retry(
                update_uri,
                update_file,
                upload_timeout,
                parallel_update,
                None,
            )
            .await
        } else {
            self.target_access()
                .dispatch_file_upload(
                    update_uri,
                    update_file,
                    upload_timeout,
                    None,
                    parallel_update,
                    None,
                )
                .await
        };

        if !status {
            if is_liteon_psu_request {
                if let Some(task_id) = self
                    .update_liteon_psus_serial(
                        update_file,
                        time_out,
                        json_dict.as_deref_mut(),
                        parallel_update,
                    )
                    .await
                {
                    return Some(task_id);
                }
                let message = "LiteOn PSU serial fallback failed";
                Self::record_json_error(json_dict.as_deref_mut(), message, Some(&response_dict));
                Util::bail_nvfwupd_threadsafe(
                    1,
                    message,
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                    parallel_update,
                );
                return None;
            }

            if is_on_reset_request {
                if let Some((task_id, task)) = self
                    .new_on_reset_handoff_task(&existing_on_reset_handoff_task_ids)
                    .await
                {
                    Self::push_on_reset_handoff_task_json(
                        json_dict.as_deref_mut(),
                        &task_id,
                        &task,
                    );
                    return Some(task_id);
                }
            }

            Util::bail_nvfwupd_threadsafe(
                1,
                &format!(
                    "File upload failed with error {:?}",
                    NvUtils::redact_secret_json_value(&response_dict)
                ),
                BailAction::DoNothing,
                None,
                parallel_update,
            );
            return None;
        }

        if let Some(ref mut jd) = json_dict {
            if let Some(output) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                output.push(NvUtils::redact_secret_json_value(&response_dict));
            }
        }

        let task_id = task_id_from_update_response(&response_dict);
        if is_liteon_psu_request {
            if let Some(task_id) = task_id.as_deref() {
                self.register_liteon_psu_all_task_for_repair(task_id, update_file);
            }
        }
        task_id
    }

    // ------------------------------------------------------------------
    // Version / identifier overrides
    // ------------------------------------------------------------------

    fn is_fungible_component(&self, _component_name: &str) -> bool {
        false
    }

    /// Match component version from the PLDM version dict.
    /// Handles LiteOn model matching, tar MANIFEST info, Delta PMC UUID,
    /// and standard PLDM name matching.
    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        let component_model = self.get_liteon_model_from_inventory(ap_name).await;
        let is_liteon = component_model.is_some();

        let component_type = if is_liteon {
            "psu"
        } else {
            self.get_component_type_from_inventory(ap_name).await
        };

        let outer = pldm_version_dict.as_object()?;

        for (pkg, pkg_dict_val) in outer {
            // Tar package path: TarPkg::parse_pkg stores MANIFEST data as
            // { comp_name: [version, model, type] }.
            if Self::is_tar_package(pkg).await {
                if let Some(pkg_obj) = pkg_dict_val.as_object() {
                    for (_comp_key, comp_data) in pkg_obj {
                        let vals = if let Some(arr) = comp_data.as_array() {
                            // Flat: [version, model, type]
                            if arr.len() >= 3 && arr[0].is_string() {
                                Some(arr.as_slice())
                            }
                            // Nested: [[version, model, type]] (legacy)
                            else if let Some(inner) = arr.first().and_then(|v| v.as_array()) {
                                if inner.len() >= 3 {
                                    Some(inner.as_slice())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        if let Some(vals) = vals {
                            let pkg_type = vals[2].as_str().unwrap_or("");
                            let pkg_ver = vals[0].as_str().unwrap_or("N/A");
                            if pkg_type == component_type {
                                if is_liteon {
                                    let pkg_model = vals[1].as_str().unwrap_or("");
                                    if let Some(ref cm) = component_model {
                                        if cm.eq_ignore_ascii_case(pkg_model) {
                                            return Some(pkg_ver.to_string());
                                        }
                                    }
                                } else {
                                    return Some(pkg_ver.to_string());
                                }
                            }
                        }
                    }
                }
                continue;
            }

            // Delta PMC UUID check for BMC firmware
            if component_type == "bmc" {
                if let Some(parser) = pkg_parser {
                    if Self::has_delta_pmc_uuid(parser, pkg) {
                        return Some(pkg.to_string());
                    }
                }
            }

            // Standard PLDM name matching
            let normalized = ap_name.to_lowercase().replace('_', "").replace('-', "");
            if let Some(pkg_obj) = pkg_dict_val.as_object() {
                for (ap_full, pkg_version_val) in pkg_obj {
                    let version_str = match pkg_version_val.as_array() {
                        Some(arr) => arr.first().and_then(|v| v.as_str()).unwrap_or("N/A"),
                        None => pkg_version_val.as_str().unwrap_or("N/A"),
                    };

                    let temp_pkg = ap_full.split(',').next().unwrap_or("").to_lowercase();
                    let ap_pkg_raw = temp_pkg.split(':').next().unwrap_or("");
                    let ap_pkg = ap_pkg_raw.replace('_', "").replace('-', "");

                    if normalized.contains("psu") && ap_pkg.contains("psu") {
                        return Some(version_str.to_string());
                    }
                    if (normalized.contains("bmc") && ap_pkg.contains("bmc"))
                        || (normalized.contains("pmc") && ap_pkg.contains("pmc"))
                    {
                        return Some(version_str.to_string());
                    }
                    if (normalized.contains("mcu") || normalized.contains("pldm"))
                        && (ap_pkg.contains("mcu") || ap_pkg.contains("pldm"))
                    {
                        return Some(version_str.to_string());
                    }
                    // Python: `if ap_pkg in normalized_ap_name:` — one-directional
                    if normalized.contains(&ap_pkg) {
                        return Some(version_str.to_string());
                    }
                }
            }
        }

        None
    }

    /// Follow `RelatedItem[0]` to get Model/PartNumber, but only if the
    /// URI points to a `/PowerSupplies/` or `/Managers/` resource.
    /// Python filters by URI path before following the link.
    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        let (status, fw_inv_dict) = self
            .target_access()
            .dispatch_request("GET", ap_inv_uri, None, None)
            .await;
        if !status {
            return None;
        }

        let related_uri = fw_inv_dict
            .get("RelatedItem")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|item| item.get("@odata.id"))
            .and_then(|v| v.as_str())?;

        // Python: only follows /PowerSupplies/ or /Managers/ URIs
        if !related_uri.contains("/PowerSupplies/") && !related_uri.contains("/Managers/") {
            return None;
        }

        let (ok, related_dict) = self
            .target_access()
            .dispatch_request("GET", related_uri, None, None)
            .await;
        if !ok {
            return None;
        }

        related_dict
            .get("Model")
            .or_else(|| related_dict.get("PartNumber"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Match identifier against nested package data `[version, model, ...]`.
    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        _ap_name: &str,
    ) -> Option<String> {
        if let Some(outer) = pldm_version_dict.as_object() {
            for (_pkg, pkg_dict_val) in outer {
                if let Some(pkg_obj) = pkg_dict_val.as_object() {
                    for (_ap, pkg_data_val) in pkg_obj {
                        if let Some(arr) = pkg_data_val.as_array() {
                            if arr.len() > 1 {
                                let sku = arr[1].as_str().unwrap_or("");
                                if sku == identifier {
                                    return arr[0].as_str().map(|s| s.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pldm::{FirmwarePkg, TarPkg};
    use std::io::Cursor;
    use std::path::Path;
    use wiremock::matchers::{body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct RealTarPkgParser {
        inner: TarPkg,
        parse_calls: usize,
    }

    #[async_trait::async_trait]
    impl PkgParser for RealTarPkgParser {
        async fn parse_pkg(&mut self, pkg_path: &str) -> (bool, String) {
            self.parse_calls += 1;
            self.inner.parse_pkg(pkg_path, None).await
        }

        fn apname_version_dict_json(&self) -> Value {
            json!(self.inner.apname_version_dict())
        }
    }

    fn cmd_args(special: Option<Vec<String>>) -> CmdArgs {
        CmdArgs {
            cmd: "update_fw".to_string(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special,
            oem_parameters: None,
        }
    }

    fn write_test_tar(tar_path: &Path, manifest: &[u8], payload: &[u8]) {
        let tar_file = std::fs::File::create(tar_path).expect("create tar package");
        let mut builder = tar::Builder::new(tar_file);

        let mut manifest_header = tar::Header::new_gnu();
        manifest_header.set_size(manifest.len() as u64);
        manifest_header.set_mode(0o644);
        manifest_header.set_cksum();
        builder
            .append_data(&mut manifest_header, "MANIFEST", Cursor::new(manifest))
            .expect("append manifest");

        let mut payload_header = tar::Header::new_gnu();
        payload_header.set_size(payload.len() as u64);
        payload_header.set_mode(0o644);
        payload_header.set_cksum();
        builder
            .append_data(&mut payload_header, "firmware.bin", Cursor::new(payload))
            .expect("append payload");

        builder.finish().expect("finish tar package");
    }

    #[test]
    fn component_type_from_description_detects_mcu_and_pldm() {
        assert_eq!(
            PowerShelfRFTarget::get_component_type_from_description("MCU firmware"),
            "mcu"
        );
        assert_eq!(
            PowerShelfRFTarget::get_component_type_from_description("PLDM image"),
            "mcu"
        );
        assert_eq!(
            PowerShelfRFTarget::get_component_type_from_description("openbmc project mcu image"),
            "mcu"
        );
        assert_eq!(
            PowerShelfRFTarget::get_component_type_from_description("BMC firmware"),
            "bmc"
        );
        assert_eq!(
            PowerShelfRFTarget::get_component_type_from_description("PMC firmware"),
            "bmc"
        );
    }

    #[test]
    fn liteon_powerunits_inventory_supported_is_liteon_positive() {
        assert!(!PowerShelfRFTarget::liteon_powerunits_inventory_supported(
            "NVD-P-0000ADT00"
        ));
        assert!(!PowerShelfRFTarget::liteon_powerunits_inventory_supported(
            "AHE50V2200A"
        ));
        assert!(!PowerShelfRFTarget::liteon_powerunits_inventory_supported(
            "AHE54V2037A"
        ));
        assert!(!PowerShelfRFTarget::liteon_powerunits_inventory_supported(
            "MPSC8000-MCU"
        ));
        assert!(!PowerShelfRFTarget::liteon_powerunits_inventory_supported(
            "Megmeet"
        ));
        assert!(PowerShelfRFTarget::liteon_powerunits_inventory_supported(
            "PF-1333-7R"
        ));
        assert!(PowerShelfRFTarget::liteon_powerunits_inventory_supported(
            "PF-1114-1R"
        ));
        assert!(PowerShelfRFTarget::liteon_powerunits_inventory_supported(
            "LiteOn PowerShelf"
        ));
    }

    #[tokio::test]
    async fn flex_pmc_inventory_matches_sample_pmc_package_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "FW_PMC_0",
                "Description": "BMC firmware",
                "Version": "v3.0.12"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({})))
            .expect(0)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-flex-powershelf");
        access.model = "NVD-P-0000ADT00".to_string();
        let target = PowerShelfRFTarget::new(access, None);
        let pldm_dict = json!({
            "v3.0.13-17-gbecd6cf07d7-dirty": {
                "FLEX-PMC-GENERIC,N/A": ["v3.0.13-17-gbecd6cf07d7-dirty", "n/a"]
            }
        });

        let version = target
            .get_component_version(&pldm_dict, "FW_PMC_0", None)
            .await;

        assert_eq!(version.as_deref(), Some("v3.0.13-17-gbecd6cf07d7-dirty"));
    }

    #[tokio::test]
    async fn flex_pmc_inventory_matches_pmc_package_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "FW_PMC_0",
                "Description": "PMC firmware",
                "Version": "v3.0.12"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({})))
            .expect(0)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-flex-powershelf");
        access.model = "NVD-P-0000ADT00".to_string();
        let target = PowerShelfRFTarget::new(access, None);
        let pldm_dict = json!({
            "flex-pmc.fwpkg": {
                "FLEX-PMC-GENERIC,N/A": ["v3.0.14", "n/a"]
            }
        });

        let version = target
            .get_component_version(&pldm_dict, "FW_PMC_0", None)
            .await;

        assert_eq!(version.as_deref(), Some("v3.0.14"));
    }

    #[tokio::test]
    async fn flex_pmc_inventory_does_not_cross_match_bmc_package_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory/FW_PMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "FW_PMC_0",
                "Description": "PMC firmware",
                "Version": "v3.0.12"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({})))
            .expect(0)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-flex-powershelf");
        access.model = "NVD-P-0000ADT00".to_string();
        let target = PowerShelfRFTarget::new(access, None);
        let pldm_dict = json!({
            "flex-combined.fwpkg": {
                "FLEX-BMC-GENERIC,N/A": ["bmc-version", "n/a"],
                "FLEX-PMC-GENERIC,N/A": ["pmc-version", "n/a"]
            }
        });

        let version = target
            .get_component_version(&pldm_dict, "FW_PMC_0", None)
            .await;

        assert_eq!(version.as_deref(), Some("pmc-version"));
    }

    #[tokio::test]
    async fn flex_psu_inventory_matches_psu_package_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_PSU_1"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory/FW_PSU_1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "FW_PSU_1",
                "Description": "PSU firmware",
                "Version": "004C0A0B"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({})))
            .expect(0)
            .mount(&server)
            .await;

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-flex-powershelf");
        access.model = "NVD-P-0000ADT00".to_string();
        let target = PowerShelfRFTarget::new(access, None);
        let pldm_dict = json!({
            "005C0A0B": {
                "FLEX-PSU-GENERIC,N/A": ["005C0A0B", "n/a"]
            }
        });

        let version = target
            .get_component_version(&pldm_dict, "FW_PSU_1", None)
            .await;

        assert_eq!(version.as_deref(), Some("005C0A0B"));
    }

    #[tokio::test]
    async fn megmeet_mcu_tar_package_matches_mcu_inventory_type() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/mcu_active"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/mcu_active",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Description": "MCU firmware"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let pldm_dict = json!({
            "megmeet-mcu.tar": {
                "mcu_firmware": ["V02B01", "PowerShelf", "mcu"]
            }
        });

        let version = target
            .get_component_version(&pldm_dict, "mcu_active", None)
            .await;

        assert_eq!(version.as_deref(), Some("V02B01"));
    }

    #[tokio::test]
    async fn fallback_mcu_inventory_name_matches_pldm_package_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/mcu_active"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/mcu_active",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Description": "unknown component"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let pldm_dict = json!({
            "megmeet.fwpkg": {
                "pldm_fw": ["V02B01"]
            }
        });

        let version = target
            .get_component_version(&pldm_dict, "mcu_active", None)
            .await;

        assert_eq!(version.as_deref(), Some("V02B01"));
    }

    #[test]
    fn on_reset_handoff_task_matches_powershelf_cancelled_warning_at_100_percent() {
        let handoff = json!({
            "TaskState": "Cancelled",
            "TaskStatus": "Warning",
            "PercentComplete": 100,
            "Messages": [{
                "Message": "The task with Id '0' has completed with errors.",
                "MessageId": "TaskEvent.1.0.3.TaskAborted"
            }]
        });

        assert!(PowerShelfRFTarget::is_on_reset_handoff_task(&handoff));

        let still_running = json!({
            "TaskState": "Running",
            "TaskStatus": "OK",
            "PercentComplete": 95
        });
        assert!(!PowerShelfRFTarget::is_on_reset_handoff_task(
            &still_running
        ));
    }

    #[test]
    fn on_reset_handoff_task_matches_old_liteon_running_internal_error() {
        let handoff = json!({
            "EndTime": "2026-05-22T07:01:44+00:00",
            "TaskState": "Running",
            "TaskStatus": "OK",
            "PercentComplete": 0,
            "Messages": [
                {
                    "Message": "The task with id 1 has started.",
                    "MessageId": "TaskEvent.1.0.1.TaskStarted",
                    "Severity": "OK"
                },
                {
                    "Message": "The request failed due to an internal service error.  The service is still operational.",
                    "MessageId": "Base.1.11.0.InternalError",
                    "MessageSeverity": "Critical",
                    "Resolution": "Resubmit the request.  If the problem persists, consider resetting the service."
                }
            ]
        });

        assert!(PowerShelfRFTarget::is_on_reset_handoff_task(&handoff));

        let mut non_terminal_running = handoff.clone();
        non_terminal_running["EndTime"] = Value::Null;
        assert!(!PowerShelfRFTarget::is_on_reset_handoff_task(
            &non_terminal_running
        ));
    }

    #[test]
    fn reset_recovery_requires_observed_outage_before_success() {
        let mut observed_unreachable = false;

        assert!(!PowerShelfRFTarget::reset_recovery_probe_confirms_reset(
            true,
            &mut observed_unreachable
        ));
        assert!(!observed_unreachable);

        assert!(!PowerShelfRFTarget::reset_recovery_probe_confirms_reset(
            false,
            &mut observed_unreachable
        ));
        assert!(observed_unreachable);

        assert!(PowerShelfRFTarget::reset_recovery_probe_confirms_reset(
            true,
            &mut observed_unreachable
        ));
    }

    #[test]
    fn manager_reset_request_url_accepts_absolute_and_relative_targets() {
        assert_eq!(
            PowerShelfRFTarget::manager_reset_request_url(
                "https://10.0.0.5",
                "/redfish/v1/Managers/bmc/Actions/Manager.Reset"
            ),
            "https://10.0.0.5/redfish/v1/Managers/bmc/Actions/Manager.Reset"
        );
        assert_eq!(
            PowerShelfRFTarget::manager_reset_request_url(
                "https://10.0.0.5/",
                "redfish/v1/Managers/bmc/Actions/Manager.Reset"
            ),
            "https://10.0.0.5/redfish/v1/Managers/bmc/Actions/Manager.Reset"
        );
        assert_eq!(
            PowerShelfRFTarget::manager_reset_request_url(
                "https://10.0.0.5",
                "https://bmc.example/redfish/v1/Managers/bmc/Actions/Manager.Reset"
            ),
            "https://bmc.example/redfish/v1/Managers/bmc/Actions/Manager.Reset"
        );
    }

    #[test]
    fn liteon_powerdevice_member_uris_map_to_raw_ids() {
        assert_eq!(
            PowerShelfRFTarget::liteon_psu_device_id_from_member_uri(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice0"
            )
            .as_deref(),
            Some("0")
        );
        assert_eq!(
            PowerShelfRFTarget::liteon_psu_device_id_from_member_uri(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice6/"
            )
            .as_deref(),
            Some("6")
        );
        assert_eq!(
            PowerShelfRFTarget::liteon_psu_device_id_from_member_uri(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/psu6"
            ),
            None
        );
    }

    #[test]
    fn liteon_psu_package_and_device_versions_are_comparable() {
        assert_eq!(
            PowerShelfRFTarget::liteon_psu_package_target_version(
                "SP-2552-1R_F6F8_Bootloader_20250319.D2DAppA.tar"
            ),
            Some((0xf6, 0xf8))
        );
        assert_eq!(
            PowerShelfRFTarget::liteon_psu_package_target_version(
                "SP-2552-1R_0608_Bootloader_20250314.D2DAppA.tar"
            ),
            Some((0x06, 0x08))
        );
        assert_eq!(
            PowerShelfRFTarget::parse_liteon_psu_version_pair("0xf6 / 0xf8"),
            Some((0xf6, 0xf8))
        );
        assert_eq!(
            PowerShelfRFTarget::parse_liteon_psu_version_pair("0x6 / 0x8"),
            Some((0x06, 0x08))
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "error": {"code": "Base.1.11.0.InternalError"},
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                (0xf6, 0xf8)
            )
        );
        assert!(PowerShelfRFTarget::liteon_psu_status_error_is_retryable(
            &json!({
                "error": {
                    "code": "Base.1.11.0.InternalError",
                    "message": "The request failed due to an internal service error. The service is still operational."
                }
            })
        ));
        assert!(
            !PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "state": "Boot",
                    "status": "OK",
                    "updateProgress": 1
                }),
                (0xf6, 0xf8)
            )
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "state": "Boot",
                    "status": "OK",
                    "updateProgress": 100
                }),
                (0xf6, 0xf8)
            )
        );
        assert!(
            PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                (0xf6, 0xf8)
            )
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_status_is_complete_for_target(
                &json!({
                    "Version": "0x6 / 0x8",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                Some((0xf6, 0xf8))
            )
        );
        assert!(
            PowerShelfRFTarget::liteon_psu_status_is_complete_for_target(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                Some((0xf6, 0xf8))
            )
        );
        assert!(
            PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0xc6 / 0xc6",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                (0xc6, 0xc6)
            )
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0xc6 / 0xc6",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                (0x06, 0x06)
            )
        );
        assert!(
            PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0x6 / 0x6",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                (0x06, 0x06)
            )
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0x6 / 0x6",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                (0xc6, 0xc6)
            )
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                (0x06, 0x08)
            )
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_response_ready_at_target_version(
                &json!({
                    "Version": "0x6 / 0x8",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                }),
                (0xf6, 0xf8)
            )
        );
    }

    #[test]
    fn liteon_psu_update_completion_requires_target_version_when_known() {
        let target = Some((0x06, 0x08));
        assert!(
            !PowerShelfRFTarget::liteon_psu_status_is_successful_update_completion(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "state": "Sleep",
                    "status": "OK",
                    "updateProgress": 0
                }),
                target,
                false
            )
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_status_is_successful_update_completion(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "state": "Sleep",
                    "status": "OK",
                    "updateProgress": 0
                }),
                target,
                true
            )
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_status_is_successful_update_completion(
                &json!({
                    "Version": "0xf6 / 0xf8",
                    "state": "Boot",
                    "status": "OK",
                    "updateProgress": 100
                }),
                target,
                false
            )
        );
        assert!(
            PowerShelfRFTarget::liteon_psu_status_is_successful_update_completion(
                &json!({
                    "Version": "0x6 / 0x8",
                    "state": "Sleep",
                    "status": "OK",
                    "updateProgress": 0
                }),
                target,
                true
            )
        );
    }

    #[tokio::test]
    async fn liteon_update_posts_to_oem_uri_with_device_header() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/Actions/UpdateService/",
            ))
            .and(header("LiteonPowerDeviceId", "1"))
            .and(body_string_contains("liteon firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Result": "success"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("liteon_psu.bin");
        tokio::fs::write(&update_file, b"liteon firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![json!({"LiteOnPowerDeviceId": "1"}).to_string()])),
                "/redfish/v1/UpdateService/upload",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                false,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("powerdevice1"));
        assert_eq!(output["Output"][0]["Id"], "powerdevice1");
        assert_eq!(output["Output"][0]["Result"], "success");
    }

    #[test]
    fn liteon_psu_update_post_retryability_is_transient_only() {
        assert!(
            PowerShelfRFTarget::liteon_psu_update_post_error_is_retryable(&json!({
                "error": {
                    "@Message.ExtendedInfo": [{
                        "MessageId": "Base.1.11.0.InternalError",
                        "Message": "The request failed due to an internal service error. The service is still operational."
                    }]
                }
            }))
        );
        assert!(
            PowerShelfRFTarget::liteon_psu_update_post_error_is_retryable(&json!({
                "error": "Request failed",
                "details": "error sending request for url: connection refused"
            }))
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_update_post_error_is_retryable(&json!({
                "error": "LiteOn PSU update requires LiteonPowerDeviceId"
            }))
        );
        assert!(
            !PowerShelfRFTarget::liteon_psu_update_post_error_is_retryable(&json!({
                "error": "HTTPPushURI is not supported for this platform"
            }))
        );
    }

    #[tokio::test]
    async fn liteon_update_retries_retryable_post_failure_with_device_header() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/Actions/UpdateService/",
            ))
            .and(header("LiteonPowerDeviceId", "1"))
            .and(body_string_contains("liteon firmware"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "error": {
                    "@Message.ExtendedInfo": [{
                        "MessageId": "Base.1.11.0.InternalError",
                        "Message": "The request failed due to an internal service error. The service is still operational."
                    }]
                }
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/Actions/UpdateService/",
            ))
            .and(header("LiteonPowerDeviceId", "1"))
            .and(body_string_contains("liteon firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Result": "success"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("liteon_psu.bin");
        tokio::fs::write(&update_file, b"liteon firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![json!({"LiteOnPowerDeviceId": "1"}).to_string()])),
                "/redfish/v1/UpdateService/upload",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("powerdevice1"));
        assert_eq!(output["Output"][0]["Id"], "powerdevice1");
        assert_eq!(output["Output"][0]["Result"], "success");
    }

    #[tokio::test]
    async fn liteon_psu_standard_upload_retries_retryable_post_before_fallback() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("Immediate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "error": {
                    "@Message.ExtendedInfo": [{
                        "MessageId": "Base.1.11.0.InternalError",
                        "Message": "The request failed due to an internal service error. The service is still operational."
                    }]
                }
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/Task-AllPsu"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp
            .path()
            .join("SP-2183-1RD_Bootloader_C6C6_20260511.fwpkg");
        tokio::fs::write(&update_file, b"liteon psu firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![
                    json!({"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/psu"]})
                        .to_string(),
                ])),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("Task-AllPsu"));
    }

    #[tokio::test]
    async fn liteon_all_psu_task_success_fails_when_any_psu_is_not_at_target_version() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("Immediate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/Task-AllPsu"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks/Task-AllPsu"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "Task-AllPsu",
                "TaskState": "Completed",
                "TaskStatus": "OK",
                "PercentComplete": 100
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1"},
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice2"}
                ]
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "1",
                "Version": "0xc6 / 0xc6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice2",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "2",
                "Version": "0x6 / 0x6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .expect(2)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp
            .path()
            .join("SP-2183-1RD_Bootloader_C6C6_20260511.fwpkg");
        tokio::fs::write(&update_file, b"liteon psu firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![
                    json!({"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/psu"]})
                        .to_string(),
                ])),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;
        assert_eq!(task_id.as_deref(), Some("Task-AllPsu"));

        let (status, details) = target
            .query_job_status("Task-AllPsu", Some(&mut output))
            .await;

        assert!(status);
        assert_eq!(details["TaskState"], "Exception");
        assert_eq!(details["TaskStatus"], "Critical");
        assert_eq!(details["expected"], "C6C6");
        assert_eq!(details["mismatched"][0], "powerdevice2=0x6 / 0x6");
        let message = details["Messages"][0]["Message"].as_str().unwrap();
        assert!(message.contains("PSU version validation failed"));
        assert!(message.contains("expected C6C6"));
        assert!(message.contains("powerdevice2=0x6 / 0x6"));
    }

    #[tokio::test]
    async fn liteon_all_psu_validation_requires_progress_zero_at_target_version() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "1",
                "Version": "0xc6 / 0xc6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 50
            })))
            .expect(1)
            .mount(&server)
            .await;

        let target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );

        let failure = target
            .validate_liteon_all_psus_at_target_once("Task-AllPsu", (0xc6, 0xc6), "C6C6")
            .await
            .expect_err("nonzero progress should not validate as ready");

        assert_eq!(failure["TaskState"], "Exception");
        assert_eq!(failure["expected"], "C6C6");
        assert_eq!(failure["devices"][0]["at_target"], false);
        assert_eq!(failure["devices"][0]["updateProgress"], 50);
    }

    #[tokio::test]
    async fn liteon_all_psu_task_success_waits_for_powerunit_versions_to_converge() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("Immediate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/Task-AllPsu"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks/Task-AllPsu"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "Task-AllPsu",
                "TaskState": "Completed",
                "TaskStatus": "OK",
                "PercentComplete": 100
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1"},
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice2"}
                ]
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "1",
                "Version": "0xc6 / 0xc6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice2",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "2",
                "Version": "0x6 / 0x6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice2",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "2",
                "Version": "0xc6 / 0xc6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp
            .path()
            .join("SP-2183-1RD_Bootloader_C6C6_20260511.fwpkg");
        tokio::fs::write(&update_file, b"liteon psu firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![
                    json!({"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/psu"]})
                        .to_string(),
                ])),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;
        assert_eq!(task_id.as_deref(), Some("Task-AllPsu"));

        let (status, details) = target
            .query_job_status("Task-AllPsu", Some(&mut output))
            .await;

        assert!(status);
        assert_eq!(details["TaskState"], "Completed");
        let validation = output["Output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["stage"] == "liteon_psu_all_validation")
            .unwrap();
        assert_eq!(validation["TaskState"], "Completed");
        let devices = validation["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 2);
        assert!(devices.iter().all(|device| device["at_target"] == true));
        assert_eq!(devices[1]["version"], "0xc6 / 0xc6");
    }

    #[tokio::test]
    async fn liteon_all_psu_task_failure_repairs_incomplete_psus_serially() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("Immediate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/Task-AllPsu"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks/Task-AllPsu"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "Task-AllPsu",
                "TaskState": "Failed",
                "TaskStatus": "Critical",
                "PercentComplete": 100,
                "Messages": [{
                    "Message": "PSU_5 firmware update error: failure: new firmware upgrade"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice5"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice5",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "5",
                "Version": "0x5 / 0x5",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/Actions/UpdateService/",
            ))
            .and(header("LiteonPowerDeviceId", "5"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Result": "success"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice5",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "5",
                "Version": "0xc6 / 0xc6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp
            .path()
            .join("SP-2183-1RD_Bootloader_C6C6_20260511.fwpkg");
        tokio::fs::write(&update_file, b"liteon psu firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![
                    json!({"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/psu"]})
                        .to_string(),
                ])),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;
        assert_eq!(task_id.as_deref(), Some("Task-AllPsu"));

        let (status, details) = target
            .query_job_status("Task-AllPsu", Some(&mut output))
            .await;

        assert!(status);
        assert_eq!(details["Id"], LITEON_PSU_SERIAL_COMPLETE_TASK_ID);
        assert_eq!(details["TaskState"], "Completed");
        let summary = output["Output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["Id"] == LITEON_PSU_SERIAL_COMPLETE_TASK_ID)
            .unwrap();
        let devices = summary["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0]["device_id"], "5");
        assert_eq!(devices[0]["version"], "0xc6 / 0xc6");
    }

    #[tokio::test]
    async fn liteon_failed_all_psu_repair_attempt_clears_registry_even_on_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks/Task-AllPsu"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "Task-AllPsu",
                "TaskState": "Failed",
                "TaskStatus": "Critical",
                "PercentComplete": 100
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        let target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        target.register_liteon_psu_all_task_for_repair(
            "Task-AllPsu",
            "/tmp/SP-2183-1RD_Bootloader_C6C6_20260511.fwpkg",
        );
        assert!(target
            .liteon_psu_all_task_update_file("Task-AllPsu")
            .is_some());

        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let (status, details) = target
            .query_job_status("Task-AllPsu", Some(&mut output))
            .await;

        assert!(status);
        assert_eq!(details["TaskState"], "Failed");
        assert!(target
            .liteon_psu_all_task_update_file("Task-AllPsu")
            .is_none());
    }

    #[tokio::test]
    async fn liteon_all_psu_task_failure_repairs_non_matching_final_psu_versions() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("Immediate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/Task-AllPsu"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks/Task-AllPsu"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "Task-AllPsu",
                "TaskState": "Cancelled",
                "TaskStatus": "Warning",
                "PercentComplete": 16,
                "Messages": [{
                    "Message": "The task with Id 'Task-AllPsu' has completed with errors."
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "1",
                "Version": "0xc6 / 0xc6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/Actions/UpdateService/",
            ))
            .and(header("LiteonPowerDeviceId", "1"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Result": "success"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "1",
                "Version": "0x6 / 0x6",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp
            .path()
            .join("SP-2183-1RD_Bootloader_0606_20260430.fwpkg");
        tokio::fs::write(&update_file, b"liteon psu firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![
                    json!({"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/psu"]})
                        .to_string(),
                ])),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;
        assert_eq!(task_id.as_deref(), Some("Task-AllPsu"));

        let (status, details) = target
            .query_job_status("Task-AllPsu", Some(&mut output))
            .await;

        assert!(status);
        assert_eq!(details["Id"], LITEON_PSU_SERIAL_COMPLETE_TASK_ID);
        assert_eq!(details["TaskState"], "Completed");
        let devices = details["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0]["device_id"], "1");
        assert!(devices[0]["stage"].is_null());
        assert_eq!(devices[0]["attempts"], 1);
        assert_eq!(devices[0]["version"], "0x6 / 0x6");
    }

    #[tokio::test]
    async fn liteon_psu_request_falls_back_to_serial_updates_when_standard_upload_fails() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("Immediate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": "LiteOn PSU update requires LiteonPowerDeviceId"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice2"},
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        for id in ["1", "2"] {
            Mock::given(method("POST"))
                .and(path(
                    "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/Actions/UpdateService/",
                ))
                .and(header("LiteonPowerDeviceId", id))
                .and(body_string_contains("liteon psu firmware"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "Result": "success"
                })))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path(format!(
                    "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice{}",
                    id
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "Id": id,
                    "state": "Enabled",
                    "status": "OK",
                    "updateProgress": if id == "1" { 100 } else { 0 }
                })))
                .expect(1)
                .mount(&server)
                .await;
        }

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("SP-2183-1RD_Bootloader_C6C6.fwpkg");
        tokio::fs::write(&update_file, b"liteon psu firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![
                    json!({"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/psu"]})
                        .to_string(),
                ])),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some(LITEON_PSU_SERIAL_COMPLETE_TASK_ID));
        let summary = output["Output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["Id"] == LITEON_PSU_SERIAL_COMPLETE_TASK_ID)
            .unwrap();
        assert_eq!(summary["TaskState"], "Completed");
        assert_eq!(summary["devices"].as_array().unwrap().len(), 2);
        assert_eq!(summary["devices"][0]["device_id"], "1");
        assert_eq!(summary["devices"][1]["device_id"], "2");
    }

    #[tokio::test]
    async fn liteon_psu_serial_update_skips_devices_already_at_package_version() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice0"},
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        for id in ["0", "1"] {
            Mock::given(method("GET"))
                .and(path(format!(
                    "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice{}",
                    id
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "Id": id,
                    "Version": "0xf6 / 0xf8",
                    "state": "Standby",
                    "status": "OK",
                    "updateProgress": 0
                })))
                .expect(1)
                .mount(&server)
                .await;
        }

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp
            .path()
            .join("SP-2552-1R_F6F8_Bootloader_20250319.D2DAppA.tar");
        tokio::fs::write(&update_file, b"liteon psu firmware")
            .await
            .unwrap();

        let target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_liteon_psus_serial(update_file.to_str().unwrap(), 30, Some(&mut output), true)
            .await;

        assert_eq!(task_id.as_deref(), Some(LITEON_PSU_SERIAL_COMPLETE_TASK_ID));
        let devices = output["Output"][0]["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0]["stage"], "skipped");
        assert_eq!(devices[1]["stage"], "skipped");
    }

    #[tokio::test]
    async fn liteon_psu_serial_update_retries_timed_out_device_and_resumes() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "1",
                "Version": "0x6 / 0x8",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .up_to_n_times(4)
            .expect(4)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/Actions/UpdateService/",
            ))
            .and(header("LiteonPowerDeviceId", "1"))
            .and(body_string_contains("liteon psu firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Result": "success"
            })))
            .expect(2)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits/powerdevice1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Id": "1",
                "Version": "0xf6 / 0xf8",
                "state": "Standby",
                "status": "OK",
                "updateProgress": 0
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp
            .path()
            .join("SP-2552-1R_F6F8_Bootloader_20250319.D2DAppA.tar");
        tokio::fs::write(&update_file, b"liteon psu firmware")
            .await
            .unwrap();

        let target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_liteon_psus_serial(update_file.to_str().unwrap(), 1, Some(&mut output), true)
            .await;

        assert_eq!(task_id.as_deref(), Some(LITEON_PSU_SERIAL_COMPLETE_TASK_ID));
        let summary = output["Output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["Id"] == LITEON_PSU_SERIAL_COMPLETE_TASK_ID)
            .unwrap();
        let devices = summary["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0]["device_id"], "1");
        assert_eq!(devices[0]["attempts"], 2);
        assert_eq!(devices[0]["version"], "0xf6 / 0xf8");
    }

    #[tokio::test]
    async fn powershelf_apply_time_invalid_json_file_is_error() {
        let tmp = tempfile::tempdir().unwrap();
        let special_file = tmp.path().join("OnResetActivation.json");
        tokio::fs::write(&special_file, "{\n    “ApplyTime” : “OnReset”\n}\n")
            .await
            .unwrap();

        let err = PowerShelfRFTarget::get_apply_time(&cmd_args(Some(vec![special_file
            .to_string_lossy()
            .to_string()])))
        .await
        .expect_err("invalid JSON file should not default to Immediate");

        assert!(err.contains("could not be properly parsed"));
        assert!(err.contains("OnResetActivation.json"));
    }

    #[tokio::test]
    async fn powershelf_apply_time_valid_json_file_is_used() {
        let tmp = tempfile::tempdir().unwrap();
        let special_file = tmp.path().join("OnResetActivation.json");
        tokio::fs::write(&special_file, "{\n  \"ApplyTime\": \"OnReset\"\n}\n")
            .await
            .unwrap();

        let apply_time = PowerShelfRFTarget::get_apply_time(&cmd_args(Some(vec![special_file
            .to_string_lossy()
            .to_string()])))
        .await
        .unwrap();

        assert_eq!(apply_time, "OnReset");
    }

    #[tokio::test]
    async fn powershelf_update_skips_apply_time_patch_when_already_configured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "HttpPushUri": "/redfish/v1/UpdateService/update",
                "HttpPushUriOptions": {
                    "HttpPushUriApplyTime": {
                        "ApplyTime": "Immediate"
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({})))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("generic flex pmc firmware"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/FlexTask-1"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("generic-flex-pmc-v1.0.0.fwpkg");
        tokio::fs::write(&update_file, b"generic flex pmc firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-flex-powershelf"),
            None,
        );
        let task_id = target
            .update_component(
                &cmd_args(None),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                None,
                true,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("FlexTask-1"));
    }

    #[tokio::test]
    async fn powershelf_apply_time_failure_records_json_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "HttpPushUri": "/redfish/v1/UpdateService/update"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("Immediate"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": {
                    "code": "Base.1.11.0.PropertyNotWritable",
                    "message": "ApplyTime is not writable"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/FlexTask-1"
            })))
            .expect(0)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("generic-flex-pmc-v1.0.0.fwpkg");
        tokio::fs::write(&update_file, b"generic flex pmc firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-flex-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(None),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;

        assert!(task_id.is_none());
        assert_eq!(output["Error Code"], 1);
        assert_eq!(
            output["Error"][0],
            "Error: Could not set Redfish Powershelf ApplyTime to 'Immediate'"
        );
    }

    #[tokio::test]
    async fn on_reset_upload_failure_returns_new_handoff_task() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("OnReset"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/TaskService/Tasks/Task-Old"}
                ]
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/TaskService/Tasks/Task-Old"},
                    {"@odata.id": "/redfish/v1/TaskService/Tasks/Task-New"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let handoff = json!({
            "TaskState": "Running",
            "TaskStatus": "OK",
            "PercentComplete": 0,
            "EndTime": "2026-05-22T07:01:44+00:00",
            "Messages": [{
                "Message": "The request failed due to an internal service error.  The service is still operational.",
                "MessageId": "Base.1.11.0.InternalError",
                "MessageSeverity": "Critical"
            }]
        });
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks/Task-Old"))
            .respond_with(ResponseTemplate::new(200).set_body_json(handoff.clone()))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks/Task-New"))
            .respond_with(ResponseTemplate::new(200).set_body_json(handoff))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("pmc firmware"))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "error": "Request failed",
                "details": "operation timed out"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("cm14mp1r-r1.3.8_to_r1.3.9.tar");
        tokio::fs::write(&update_file, b"pmc firmware")
            .await
            .unwrap();

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let task_id = target
            .update_component(
                &cmd_args(Some(vec![json!({"ApplyTime": "OnReset"}).to_string()])),
                "/redfish/v1/UpdateService/update",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("Task-New"));
        assert_eq!(output["Output"][0]["Id"], "Task-New");
        assert_eq!(output["Output"][0]["TaskState"], "Running");
    }

    #[tokio::test]
    async fn tar_package_workflow_parses_manifest_sets_apply_time_and_pushes_package() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ServiceEnabled": true,
                "HttpPushUri": "/redfish/v1/UpdateService/upload"
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .and(body_string_contains("OnReset"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .and(body_string_contains("tar firmware"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/Task-PowerShelf"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("powershelf.tar");
        write_test_tar(
            &update_file,
            b"purpose=PSU\nversion=1.2.3\nmodel=PowerShelf\n",
            b"tar firmware",
        );

        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut parser = RealTarPkgParser {
            inner: TarPkg::new(),
            parse_calls: 0,
        };
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });
        let args = cmd_args(Some(vec![json!({"ApplyTime": "OnReset"}).to_string()]));
        let (err_code, task_ids) = target
            .start_update_monitor(
                &[update_file.to_string_lossy().to_string()],
                &mut parser,
                &args,
                30,
                false,
                Some(&mut output),
                0,
                true,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        assert_eq!(err_code, 0);
        assert!(task_ids.is_empty());
        assert_eq!(parser.parse_calls, 1);
        let components = parser
            .inner
            .apname_version_dict()
            .get(update_file.to_str().unwrap())
            .expect("tar package metadata");
        assert_eq!(
            components.get("psu_firmware").expect("psu metadata"),
            &vec![
                "1.2.3".to_string(),
                "PowerShelf".to_string(),
                "psu".to_string()
            ]
        );
        assert_eq!(
            output["Output"][0]["@odata.id"],
            "/redfish/v1/TaskService/Tasks/Task-PowerShelf"
        );
    }
}
