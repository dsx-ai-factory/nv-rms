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

//! GH200RFTarget implementation for GH200 platforms.
//!
//! Extends GH behavior with multipart upload support for specific
//! board configurations.

use regex::Regex;
use serde_json::{json, Value};

use crate::bmc_access::BmcAccess;
use crate::gh_rftarget;
use crate::rf_target::{CmdArgs, PkgParser, RFTarget};
use crate::util::{BailAction, TraceFlags, Util};
use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// Shared version comparison (used by GH200 and GB200)
// ---------------------------------------------------------------------------

/// GH200-level version comparison.
///
/// Python: `GH200RFTarget.version_newer` →
///   1. If sys has no '.', remove dots from pkg.
///   2. `re.match(r"[a-zA-Z0-9]*GH[a-zA-Z0-9]*[_|-]", ...)` — start-anchored.
///   3. If sys matches but pkg doesn't, strip prefix + trailing alpha suffix.
///   4. Delegate to `super().version_newer()` → GH → RFTarget base.
pub fn gh200_version_compare(pkg_version: &str, sys_version: &str) -> bool {
    let mut pkg = pkg_version.to_string();
    let mut sys = sys_version.to_string();

    if !sys.contains('.') {
        pkg = pkg.replace('.', "");
    }

    // Python uses re.match (start-anchored).
    // Python: `[_|-]` — inside [] the `|` is a LITERAL pipe character.
    let gh_re = Regex::new(r"(?i)^[a-zA-Z0-9]*GH[a-zA-Z0-9]*[_|\-]").unwrap();
    let match_sys = gh_re.is_match(&sys);
    let match_pkg = gh_re.is_match(&pkg);

    if match_sys && !match_pkg {
        sys = gh_re.replace(&sys, "").to_string();
        // Python: re.search("-[a-zA-Z]+", sys_version) — first match, truncate.
        let end_re = Regex::new(r"-[a-zA-Z]+").unwrap();
        if let Some(m) = end_re.find(&sys) {
            sys.truncate(m.start());
        }
    }

    gh_rftarget::gh_version_compare(&pkg, &sys)
}

// ---------------------------------------------------------------------------
// GH200RFTarget
// ---------------------------------------------------------------------------

/// Platform-specific RFTarget for GH200 systems
///
/// Extends GH behavior with multipart upload for certain board
/// configurations and a custom `get_update_uri` override.
pub struct GH200RFTarget {
    /// BMC connection handle.
    pub bmc_access: BmcAccess,
    /// Component names treated as fungible (default: `["gpu"]`).
    pub fungible_components: Vec<String>,
    /// Message displayed upon successful update completion.
    pub update_completion_msg: String,
    /// Whether the progress table header has been printed during monitoring.
    pub progress_table_header_printed: bool,
    /// Optional platform configuration dictionary.
    pub config_dict: Option<Value>,
}

impl GH200RFTarget {
    /// Create a new `GH200RFTarget` with sensible defaults.
    pub fn new(bmc_access: BmcAccess, config_dict: Option<Value>) -> Self {
        Self {
            bmc_access,
            fungible_components: vec!["gpu".to_string()],
            update_completion_msg: "Refer to 'NVIDIA Firmware Update Document' on \
                                    activation steps for new firmware to take effect."
                .to_string(),
            progress_table_header_printed: false,
            config_dict,
        }
    }
}

#[async_trait::async_trait]
impl RFTarget for GH200RFTarget {
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
        "GH200RFTarget"
    }

    // ------------------------------------------------------------------
    // get_update_uri override
    // ------------------------------------------------------------------

    /// Returns the multipart HTTP push URI from the UpdateService response.
    fn get_update_uri(&self, update_service_response: &Value) -> String {
        // Try to extract MultipartHttpPushUri from the UpdateService response
        if let Some(uri) = update_service_response
            .get("MultipartHttpPushUri")
            .and_then(|v| v.as_str())
        {
            return uri.to_string();
        }

        "/redfish/v1/UpdateService/update-multipart".to_string()
    }

    // ------------------------------------------------------------------
    // make_update_target_json override
    // ------------------------------------------------------------------

    /// GH200: uses `{"Targets": [...]}` instead of `HttpPushUriTargets`.
    /// Also creates HGX_Full.json and BMC_Full.json.
    async fn make_update_target_json(&self, dir_path: &str) -> bool {
        let (status, inv_dict) = self
            .target_access()
            .dispatch_request(
                "GET",
                "/redfish/v1/UpdateService/FirmwareInventory",
                None,
                None,
            )
            .await;
        if !status {
            return false;
        }
        let members = match inv_dict.get("Members").and_then(|v| v.as_array()) {
            Some(m) => m,
            None => return false,
        };
        if let Err(e) = tokio::fs::create_dir_all(dir_path).await {
            tracing::warn!("Error creating directory {}: {}", dir_path, e);
            return false;
        }
        let mut file_list: Vec<String> = Vec::new();
        let mut cpu_list: Vec<String> = Vec::new();

        for member in members {
            let inv_url = match member.get("@odata.id").and_then(|v| v.as_str()) {
                Some(u) => u,
                None => continue,
            };
            let name = inv_url.rsplit('/').next().unwrap_or("");
            if name.contains("CPU") && !name.contains("ERoT") {
                cpu_list.push(inv_url.to_string());
                continue;
            }
            let target_json = json!({"Targets": [inv_url]});
            let fp = format!("{}/{}.json", dir_path, name);
            if tokio::fs::write(&fp, serde_json::to_string_pretty(&target_json).unwrap())
                .await
                .is_ok()
            {
                file_list.push(fp);
            }
        }
        if !cpu_list.is_empty() {
            let target_json = json!({"Targets": cpu_list});
            let fp = format!("{}/CPU.json", dir_path);
            if tokio::fs::write(&fp, serde_json::to_string_pretty(&target_json).unwrap())
                .await
                .is_ok()
            {
                file_list.push(fp);
            }
        }
        // Check Chassis for HGX baseboard
        let (cs, chassis_dict) = self
            .target_access()
            .dispatch_request("GET", "/redfish/v1/Chassis", None, None)
            .await;
        if cs {
            if let Some(ch_members) = chassis_dict.get("Members").and_then(|v| v.as_array()) {
                let hgx_names = ["5B247A_Baseboard_0", "HGX_Baseboard_0", "HGX_Chassis_0"];
                for ch in ch_members {
                    if let Some(uri) = ch.get("@odata.id").and_then(|v| v.as_str()) {
                        let name = uri.rsplit('/').next().unwrap_or("");
                        if hgx_names.contains(&name) {
                            let target_json = json!({"Targets": [uri]});
                            let fp = format!("{}/HGX_Full.json", dir_path);
                            if tokio::fs::write(
                                &fp,
                                serde_json::to_string_pretty(&target_json).unwrap(),
                            )
                            .await
                            .is_ok()
                            {
                                file_list.push(fp);
                            }
                            break;
                        }
                    }
                }
            }
        }
        // BMC_Full.json with empty Targets
        let fp = format!("{}/BMC_Full.json", dir_path);
        if tokio::fs::write(
            &fp,
            serde_json::to_string_pretty(&json!({"Targets": []})).unwrap(),
        )
        .await
        .is_ok()
        {
            file_list.push(fp);
        }
        println!("Created following update parameter files:");
        for f in &file_list {
            println!("{}", f);
        }
        true
    }

    // ------------------------------------------------------------------
    // version_newer override
    // ------------------------------------------------------------------

    /// GH200-specific version comparison — delegates to the public
    /// [`gh200_version_compare`] so GB200 (which inherits from GH200
    /// in Python) can also call it.
    fn version_newer(&self, pkg_version: &str, sys_version: &str) -> bool {
        gh200_version_compare(pkg_version, sys_version)
    }

    // ------------------------------------------------------------------
    // factory_reset override — inherits GH behavior
    // ------------------------------------------------------------------

    /// GH200 inherits factory_reset from GH: always sends
    /// `{"ResetToDefaultsType": "ResetAll"}`.
    async fn factory_reset(&mut self, _reset_params: Option<&Value>) -> (bool, Value) {
        // Delegate to GH-style factory reset (same as GHRFTarget override)
        let params = json!({"ResetToDefaultsType": "ResetAll"});
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
            }
        }

        let reset_uri = format!(
            "/redfish/v1/Managers/{}/Actions/Manager.ResetToDefaults",
            bmc_id
        );
        self.target_access()
            .dispatch_request("POST", &reset_uri, Some(&params), None)
            .await
    }

    // ------------------------------------------------------------------
    // get_expected_task_type_from_package override
    // ------------------------------------------------------------------

    /// Determine if the package will create a BMC or HMC task.
    ///
    /// Python: checks recipe filenames for HGX platform indicators
    /// (P4059, P4764, P4974, P4975, GB200-NVL4, HMC). Returns "HMC" if
    /// any match, otherwise "BMC".
    fn get_expected_task_type_from_package(&self, recipe_list: &[String]) -> String {
        let hgx_platforms = ["P4059", "P4764", "P4974", "P4975", "GB200-NVL4", "HMC"];
        for package_file in recipe_list {
            if hgx_platforms.iter().any(|p| package_file.contains(p)) {
                return "HMC".to_string();
            }
        }
        "BMC".to_string()
    }

    // ------------------------------------------------------------------
    // OOB activation override
    // ------------------------------------------------------------------

    /// Supports RF_PWR_ON, RF_PWR_OFF, RF_PWR_CYCLE, RF_AUX_PWR_CYCLE,
    /// and RF_PWR_STATUS via ComputerSystem.Reset / Oem AuxPowerReset.
    async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        let mut quiet_json = json!({"Error": [], "Error Code": 0, "Output": []});
        let supported = [
            "RF_AUX_PWR_CYCLE",
            "RF_PWR_ON",
            "RF_PWR_OFF",
            "RF_PWR_CYCLE",
            "RF_PWR_STATUS",
        ];
        if !supported.contains(&cmd_args.cmd.as_str()) {
            Util::bail_nvfwupd(
                1,
                &format!("Activation command {} not supported", cmd_args.cmd),
                BailAction::DoNothing,
                cmd_args.quiet.then_some(&quiet_json),
            );
            return 1;
        }

        let trace = TraceFlags::default();

        if cmd_args.cmd == "RF_PWR_STATUS" {
            let systems = self.bmc_access.get_systems_members(trace).await;
            let mut found_any = false;
            let mut first = true;
            for system_uri in &systems {
                let (status, response) = self
                    .bmc_access
                    .dispatch_request_full("GET", system_uri, None, None, 30, true, None)
                    .await;
                if !status {
                    continue;
                }
                let power_state = response
                    .get("PowerState")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown");
                let system_name = response
                    .get("Name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(system_uri);
                if !found_any {
                    if !cmd_args.quiet {
                        println!("Power Status:");
                    }
                    found_any = true;
                }
                if !first && !cmd_args.quiet {
                    println!();
                }
                if !cmd_args.quiet {
                    println!("  System: {}", system_name);
                    println!("  URI: {}", system_uri);
                    println!("  PowerState: {}", power_state);
                }
                first = false;
            }
            if found_any && !cmd_args.quiet {
                println!();
            }
            if found_any {
                return 0;
            }
            Util::bail_nvfwupd(
                1,
                "Error: Unable to query power state from any system.",
                BailAction::DoNothing,
                cmd_args.quiet.then_some(&quiet_json),
            );
            return 1;
        }

        let reset_type = match cmd_args.cmd.as_str() {
            "RF_PWR_ON" => "On",
            "RF_PWR_OFF" => "ForceOff",
            "RF_PWR_CYCLE" => "ForceRestart",
            "RF_AUX_PWR_CYCLE" => "AuxPowerCycle",
            _ => unreachable!(),
        };

        let mut target_uri: Option<String> = None;

        if cmd_args.cmd == "RF_AUX_PWR_CYCLE" {
            let chassis_list = self.bmc_access.get_chassis_members(trace).await;
            for chassis in &chassis_list {
                let (status, response) = self
                    .bmc_access
                    .dispatch_request_full("GET", chassis, None, None, 30, true, None)
                    .await;
                if !status {
                    continue;
                }
                if let Some(uri) = response
                    .get("Actions")
                    .and_then(|a| a.get("Oem"))
                    .and_then(|o| o.get("#NvidiaChassis.AuxPowerReset"))
                    .and_then(|r| r.get("target"))
                    .and_then(|t| t.as_str())
                {
                    target_uri = Some(uri.to_string());
                    break;
                }
            }
        } else {
            let systems = self.bmc_access.get_systems_members(trace).await;
            for system_uri in &systems {
                let (status, response) = self
                    .bmc_access
                    .dispatch_request_full("GET", system_uri, None, None, 30, true, None)
                    .await;
                if !status {
                    continue;
                }
                if let Some(uri) = response
                    .get("Actions")
                    .and_then(|a| a.get("#ComputerSystem.Reset"))
                    .and_then(|r| r.get("target"))
                    .and_then(|t| t.as_str())
                {
                    target_uri = Some(uri.to_string());
                    break;
                }
            }
        }

        if let Some(uri) = target_uri {
            let body = json!({"ResetType": reset_type});
            let (status, response) = self
                .bmc_access
                .dispatch_request_full(
                    "POST",
                    &uri,
                    None,
                    Some(&body),
                    30,
                    false,
                    cmd_args.quiet.then_some(&mut quiet_json),
                )
                .await;
            if !cmd_args.quiet {
                if status {
                    println!("{} requested successfully.", cmd_args.cmd);
                } else {
                    println!("{} request failed.", cmd_args.cmd);
                }
                println!("Server response:");
                println!("{}", NvUtils::redacted_json_pretty_4space(&response));
            }
            return if status { 0 } else { 1 };
        }

        Util::bail_nvfwupd(
            1,
            &format!("Error: Target does not support {}.", cmd_args.cmd),
            BailAction::DoNothing,
            cmd_args.quiet.then_some(&quiet_json),
        );
        1
    }

    // ------------------------------------------------------------------
    // Abstract method implementations
    // ------------------------------------------------------------------

    /// Perform firmware update via multipart. Resolves `cmd_args.special`
    /// for Targets JSON (defaulting based on filename) and
    /// `cmd_args.oem_parameters` for OemParameters.
    async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        // Resolve special → UpdateParameters JSON
        let param_list: Option<String> = match self
            .resolve_json_or_file(cmd_args.special.as_deref(), "special")
            .await
        {
            Ok(s) => s,
            Err(msg) => {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &msg,
                    BailAction::DoNothing,
                    None,
                    parallel_update,
                );
                return None;
            }
        };

        // If no special provided, build default Targets from filename
        let param_json_str = match param_list {
            Some(s) => s,
            None => {
                let file_name = std::path::Path::new(update_file)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(update_file);
                let hgx_platforms = ["P4059", "P4764", "P4974", "P4975", "GB200-NVL4", "HMC"];
                let targets = if hgx_platforms.iter().any(|p| file_name.contains(p)) {
                    json!({"Targets": ["/redfish/v1/Chassis/HGX_Chassis_0"]})
                } else {
                    json!({"Targets": []})
                };
                serde_json::to_string(&targets).unwrap_or_default()
            }
        };

        // Resolve oem_parameters → OemParameters JSON
        let oem_params_json = match self
            .resolve_json_or_file(cmd_args.oem_parameters.as_deref(), "oem_parameters")
            .await
        {
            Ok(s) => s,
            Err(msg) => {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &msg,
                    BailAction::DoNothing,
                    None,
                    parallel_update,
                );
                return None;
            }
        };

        // Multipart upload with resolved params
        let param_json_val: Value = serde_json::from_str(&param_json_str).unwrap_or_default();
        let oem_val = oem_params_json.map(Value::String);
        self.update_component_multipart(
            None,
            update_uri,
            update_file,
            time_out,
            Some(&param_json_val),
            None,
            oem_val.as_ref(),
            json_dict,
            parallel_update,
            cmd_args.quiet,
        )
        .await
    }

    /// GH200 has no fungible components.
    fn is_fungible_component(&self, _component_name: &str) -> bool {
        false
    }

    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        _pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        gh_rftarget::gh_get_component_version(pldm_version_dict, ap_name)
    }

    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        gh_rftarget::gh_get_identifier_from_chassis(self.target_access(), ap_inv_uri).await
    }

    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> Option<String> {
        gh_rftarget::gh_get_version_sku(identifier, pldm_version_dict, ap_name)
    }
}
