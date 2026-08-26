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

//! Input parameters for nvfwupd parallel firmware updates.
//!
//! Defines [`TaskId`] for tracking firmware update tasks, [`InputParams`]
//! for passing per-target update parameters, and [`WorkerResult`] for
//! collecting results from parallel update workers.

use std::fmt;

use serde_json::Value;

use crate::rf_target::RFTarget;
use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// TaskId
// ---------------------------------------------------------------------------

/// Tracks the status of a single firmware-update task.
#[derive(Clone)]
pub struct TaskId {
    pub task_id: String,
    pub status: Option<bool>,
    pub response_dict: Option<Value>,
}

impl fmt::Debug for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let response_dict = self
            .response_dict
            .as_ref()
            .map(NvUtils::redact_secret_json_value);

        f.debug_struct("TaskId")
            .field("task_id", &self.task_id)
            .field("status", &self.status)
            .field("response_dict", &response_dict)
            .finish()
    }
}

impl TaskId {
    pub fn new(task_id: String, status: Option<bool>, response_dict: Option<Value>) -> Self {
        Self {
            task_id,
            status,
            response_dict,
        }
    }
}

// ---------------------------------------------------------------------------
// InputParams
// ---------------------------------------------------------------------------

/// Per-target input parameters for the firmware update pipeline.
///
/// Each parallel worker receives its own `InputParams` so updates run
/// without shared mutable state.
#[derive(Clone)]
pub struct InputParams {
    /// Raw target key=value args (e.g. `["ip=10.0.0.1", "user=root", ...]`).
    pub target_args: Vec<String>,
    /// Resolved IP address.
    pub ip: String,
    /// Firmware package file paths for this target.
    pub package_list: Vec<String>,
    /// Special/override parameters from config (JSON string or file path).
    pub special: Option<String>,
    /// OEM-specific parameters from config (JSON string or file path).
    pub oem_parameters: Option<String>,
    /// Human-readable system name (e.g. "DGX H100").
    pub system_name: Option<String>,
    /// Delay in seconds before starting update on this target.
    pub update_delay: u64,
}

impl fmt::Debug for InputParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let target_args: Vec<String> = self
            .target_args
            .iter()
            .map(|value| NvUtils::redact_secret_key_value_arg(value))
            .collect();
        let special = self
            .special
            .as_ref()
            .map(|value| NvUtils::redact_secret_fields(value));
        let oem_parameters = self
            .oem_parameters
            .as_ref()
            .map(|value| NvUtils::redact_secret_fields(value));

        f.debug_struct("InputParams")
            .field("target_args", &target_args)
            .field("ip", &self.ip)
            .field("package_list", &self.package_list)
            .field("special", &special)
            .field("oem_parameters", &oem_parameters)
            .field("system_name", &self.system_name)
            .field("update_delay", &self.update_delay)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// WorkerResult
// ---------------------------------------------------------------------------

/// Result returned by each parallel update worker task.
pub struct WorkerResult {
    /// The input params this worker processed (preserved for post-processing).
    pub input: InputParams,
    /// Task IDs collected during `start_update_monitor(parallel_update=true)`.
    pub task_id_list: Vec<TaskId>,
    /// The platform target handle, for subsequent polling.
    pub rf_target: Option<Box<dyn RFTarget + Send + Sync>>,
    /// Per-worker JSON output (deep-cloned from template, accumulated errors).
    pub json_dict: Option<Value>,
    /// Aggregate error status from this worker (0 = ok, nonzero = error).
    pub err_status: i32,
    /// Whether this target is a PowerShelf (BMC resets, no task monitoring).
    pub is_powershelf: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_debug_redacts_response_passwords() {
        let task_id = TaskId::new(
            "Task.1".to_string(),
            Some(false),
            Some(serde_json::json!({"RF_PASSWORD": "plain_secret"})),
        );

        let debug = format!("{task_id:?}");
        assert!(!debug.contains("plain_secret"));
        assert!(debug.contains("XXXX"));
    }

    #[test]
    fn input_params_debug_redacts_passwords() {
        let params = InputParams {
            target_args: vec![
                "ip=192.0.2.1".to_string(),
                "password=plain secret suffix".to_string(),
            ],
            ip: "192.0.2.1".to_string(),
            package_list: vec!["pkg.fwpkg".to_string()],
            special: Some(r#"{"RF_PASSWORD":"special_secret"}"#.to_string()),
            oem_parameters: Some("BMC_PASSWORD: oem_secret".to_string()),
            system_name: Some("system".to_string()),
            update_delay: 0,
        };

        let debug = format!("{params:?}");
        assert!(!debug.contains("plain"));
        assert!(!debug.contains("suffix"));
        assert!(!debug.contains("special_secret"));
        assert!(!debug.contains("oem_secret"));
        assert!(debug.contains("XXXX"));
    }
}
