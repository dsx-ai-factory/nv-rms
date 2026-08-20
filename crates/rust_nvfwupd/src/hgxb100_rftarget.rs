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

//! HGXB100RFTarget and HGXRUBINRFTarget implementations.
//!
//! - `HGXB100RFTarget` -- HGX B100/B300 platforms (multipart upload).
//! - `HGXRUBINRFTarget` -- HGX Rubin NVL8 platforms.

use regex::Regex;
use serde_json::{json, Value};

use crate::bmc_access::BmcAccess;
use crate::gh_rftarget::{self, GHRFTarget};
use crate::rf_target::{CmdArgs, PkgParser, RFTarget};
use crate::util::{BailAction, Util};
use crate::utils::Util as NvUtils;

// ===========================================================================
// HGXB100RFTarget
// ===========================================================================

/// Platform-specific RFTarget for HGX B100 and HGX B300 systems.
///
/// Always uses multipart upload for firmware updates.
pub struct HGXB100RFTarget {
    /// BMC connection handle.
    pub bmc_access: BmcAccess,
    /// Component names treated as fungible.
    pub fungible_components: Vec<String>,
    /// Message displayed upon successful update completion.
    pub update_completion_msg: String,
    /// Whether the progress table header has been printed during monitoring.
    pub progress_table_header_printed: bool,
    /// Optional platform configuration dictionary.
    pub config_dict: Option<Value>,
}

impl HGXB100RFTarget {
    /// Create a new `HGXB100RFTarget` with sensible defaults.
    pub fn new(bmc_access: BmcAccess, config_dict: Option<Value>) -> Self {
        Self {
            bmc_access,
            fungible_components: vec![
                "gpu".to_string(),
                "nvswitch".to_string(),
                "fpga".to_string(),
                "erot".to_string(),
                "sma".to_string(),
                "connectx".to_string(),
            ],
            update_completion_msg: "Refer to Firmware Update Document on \
                                    activation steps for new firmware to take effect."
                .to_string(),
            progress_table_header_printed: false,
            config_dict,
        }
    }
}

#[async_trait::async_trait]
impl RFTarget for HGXB100RFTarget {
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
        "HGXB100RFTarget"
    }

    // HGXB100 inherits get_update_uri from trait default:
    // "/redfish/v1/UpdateService" (HttpPushUri), matching Python.

    // ------------------------------------------------------------------
    // Abstract method implementations
    // ------------------------------------------------------------------

    /// HGX B100/B300: push-URI vs multipart based on special content.
    /// If special has Targets or is `{}`, uses multipart. Otherwise
    /// PATCH + push-URI (like GH). oem_parameters on multipart path.
    async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        mut json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        // Resolve oem_parameters
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

        // Resolve special → parsed JSON data
        let special_str = match self
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

        let mut push_uri = true;
        let mut json_data: Option<Value> = special_str
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());

        // Presence of empty object or Targets means multipart
        if let Some(ref d) = json_data {
            if d.get("Targets").is_some() || d.as_object().map_or(false, |o| o.is_empty()) {
                push_uri = false;
            }
        }

        if cmd_args.staged_update || cmd_args.staged_activate_update {
            push_uri = false;

            if let Some(ref d) = json_data {
                if d.get("HttpPushUriTargets").is_some() {
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        "Error: HttpPushUriTargets is not supported with staged updates",
                        BailAction::DoNothing,
                        None,
                        parallel_update,
                    );
                    return None;
                }
            }

            if json_data.is_none() {
                json_data = Some(json!({}));
            }

            if cmd_args.staged_update {
                json_data.as_mut().unwrap()["Oem"] =
                    json!({"Nvidia": {"UpdateOption": "StageOnly"}});
            } else if cmd_args.staged_activate_update {
                json_data.as_mut().unwrap()["Oem"] =
                    json!({"Nvidia": {"UpdateOption": "StageAndActivate"}});
            }
        }

        if push_uri {
            // PATCH + push-URI path (like GH)
            if let Some(ref data) = json_data {
                let (status, err_dict) = self
                    .target_access()
                    .dispatch_request("PATCH", "/redfish/v1/UpdateService", Some(data), None)
                    .await;
                if !status {
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        &format!(
                            "Patch update request failed! {:?}",
                            NvUtils::redact_secret_json_value(&err_dict)
                        ),
                        BailAction::DoNothing,
                        None,
                        parallel_update,
                    );
                    return None;
                }
            }

            let (status, response_dict) = self
                .target_access()
                .dispatch_file_upload(
                    update_uri,
                    update_file,
                    time_out,
                    None,
                    parallel_update,
                    None,
                )
                .await;
            if !status {
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
            let task_id = response_dict
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Some(task_id)
        } else {
            // Multipart path — fetch MultipartHttpPushUri
            let (us_status, us_response) = self
                .target_access()
                .dispatch_request("GET", "/redfish/v1/UpdateService", None, None)
                .await;
            // Python: checks both status AND ServiceEnabled field.
            let service_enabled = us_response
                .get("ServiceEnabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !us_status || !service_enabled {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    "UpdateService is not enabled in the system",
                    BailAction::PrintDivider,
                    None,
                    parallel_update,
                );
                return None;
            }
            let mp_uri = us_response
                .get("MultipartHttpPushUri")
                .and_then(|v| v.as_str())
                .unwrap_or("/redfish/v1/UpdateService")
                .to_string();

            let param_val = json_data.unwrap_or(json!({}));
            let oem_val = oem_params_json.map(Value::String);
            self.update_component_multipart(
                None,
                &mp_uri,
                update_file,
                time_out,
                Some(&param_val),
                None,
                oem_val.as_ref(),
                json_dict,
                parallel_update,
                cmd_args.quiet,
            )
            .await
        }
    }

    fn version_newer(&self, pkg_version: &str, sys_version: &str) -> bool {
        hgxb100_version_compare(pkg_version, sys_version)
    }

    fn is_fungible_component(&self, component_name: &str) -> bool {
        // Python: any(component_name.__contains__, fungible_components)
        //   AND NOT any(component_name.__contains__, ["inforom", "driver"])
        let lower = component_name.to_lowercase();
        self.fungible_components
            .iter()
            .any(|f| lower.contains(f.as_str()))
            && !lower.contains("inforom")
            && !lower.contains("driver")
    }

    /// HGXB100-specific component version lookup from nested PLDM dict.
    /// Mirrors Python `HGXB100RFTarget.get_component_version`.
    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        _pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        hgxb100_get_component_version(pldm_version_dict, ap_name)
    }

    /// Follow `RelatedItem[0]` to the chassis resource and return its `SKU`.
    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        gh_get_identifier_from_chassis(self.target_access(), ap_inv_uri).await
    }

    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> Option<String> {
        hgxb100_get_version_sku(identifier, pldm_version_dict)
    }
}

// ---------------------------------------------------------------------------
// Shared helpers for HGXB100/HGXRUBIN
// ---------------------------------------------------------------------------

/// HGXB100-style `get_version_sku`: iterate all packages/APs and do an
/// exact match on `pkg_data[1] == identifier` (no "gpu" filter).
///
/// Python:
/// ```python
/// for _, pkg_dict in pldm_version_dict.items():
///     for _, pkg_data in pkg_dict.items():
///         if pkg_data[1] == identifier:
///             return pkg_data[0]
/// return "N/A"
/// ```
fn hgxb100_get_version_sku(identifier: &str, pldm_version_dict: &Value) -> Option<String> {
    if let Some(outer) = pldm_version_dict.as_object() {
        for (_pkg, pkg_dict_val) in outer {
            if let Some(pkg_dict) = pkg_dict_val.as_object() {
                for (_ap_full, pkg_version_val) in pkg_dict {
                    if let Some(arr) = pkg_version_val.as_array() {
                        let pkg_identifier = arr.get(1).and_then(|v| v.as_str()).unwrap_or("");
                        if pkg_identifier == identifier {
                            return arr.first().and_then(|v| v.as_str()).map(|s| s.to_string());
                        }
                    }
                }
            }
        }
    }
    Some("N/A".to_string())
}

/// HGXB100-style `get_component_version` shared across all three struct types.
fn hgxb100_get_component_version(pldm_version_dict: &Value, ap_name: &str) -> Option<String> {
    let mut ap_name = ap_name.to_lowercase();
    let mut hgx_pkg_only = false;

    if ap_name.starts_with("hgx_fw_") {
        ap_name = ap_name["hgx_fw_".len()..].to_string();
        hgx_pkg_only = true;
        if ap_name.starts_with("bmc") {
            ap_name = "hmc".to_string();
        }
    } else if ap_name.starts_with("hgx_") {
        ap_name = ap_name["hgx_".len()..].to_string();
        hgx_pkg_only = true;
        if ap_name.starts_with("bmc") {
            ap_name = "hmc".to_string();
        }
    }

    if ap_name.contains("erot") {
        ap_name = "erot".to_string();
    }

    if ap_name.contains("gpu") && !ap_name.contains("inforom") {
        ap_name = "gpu".to_string();
    } else if ap_name.contains("cpu") {
        ap_name = "sbios".to_string();
    } else if ap_name.contains("nvlink") {
        ap_name = "cx7".to_string();
    }

    ap_name = ap_name.replace('_', "");

    let mut ap_version = "N/A".to_string();

    let outer = pldm_version_dict.as_object()?;

    for (pkg, pkg_dict_val) in outer {
        let pkg_is_hgx = GHRFTarget::is_hgx_pkg(pkg);
        if hgx_pkg_only && !pkg_is_hgx {
            continue;
        }
        if !hgx_pkg_only && pkg_is_hgx {
            continue;
        }

        let pkg_dict = match pkg_dict_val.as_object() {
            Some(d) => d,
            None => continue,
        };

        for (ap_full, pkg_version_val) in pkg_dict {
            let version_str = match pkg_version_val.as_array() {
                Some(arr) => arr.first().and_then(|v| v.as_str()).unwrap_or("N/A"),
                None => pkg_version_val.as_str().unwrap_or("N/A"),
            };

            let ap_pkg = ap_full.split(',').next().unwrap_or("").to_lowercase();
            let ap_pkg = ap_pkg.split(':').next().unwrap_or("");
            let ap_pkg = ap_pkg.replace('_', "").replace('-', "");

            if ap_name.contains("inforom") && !ap_pkg.contains("inforom") {
                continue;
            }

            if ap_name.contains(&ap_pkg) {
                ap_version = version_str.to_string();
            } else if ap_pkg.contains("smr") && ap_name.contains("fpga") {
                ap_version = version_str.to_string();
            } else {
                let alt_ap = format!("{}0", ap_pkg);
                if alt_ap == ap_name {
                    ap_version = version_str.to_string();
                }
            }
        }
    }

    if ap_version == "N/A" {
        None
    } else {
        Some(ap_version)
    }
}

/// GH-style `get_identifier_from_chassis` via `RelatedItem[0]` -> `SKU`.
async fn gh_get_identifier_from_chassis(bmc: &BmcAccess, ap_inv_uri: &str) -> Option<String> {
    let (status, fw_inv_dict) = bmc.dispatch_request("GET", ap_inv_uri, None, None).await;
    if !status {
        return None;
    }

    let chassis_uri = fw_inv_dict
        .get("RelatedItem")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|item| item.get("@odata.id"))
        .and_then(|v| v.as_str())?;

    let (status2, chassis_dict) = bmc.dispatch_request("GET", chassis_uri, None, None).await;
    if !status2 {
        return None;
    }

    chassis_dict
        .get("SKU")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// HGXB100 version comparison. Mirrors Python `HGXB100RFTarget.version_newer`.
fn hgxb100_version_compare(pkg_version: &str, sys_version: &str) -> bool {
    let mut pkg = pkg_version.to_string();
    let mut sys = sys_version.to_string();

    if !sys.contains('.') {
        pkg = pkg.replace('.', "");
    }

    let underscore_re = Regex::new(r"^[0-9]+_[0-9]+_[0-9]+").unwrap();
    if underscore_re.is_match(&sys) || underscore_re.is_match(&pkg) {
        pkg = pkg.replace('_', "");
        sys = sys.replace('_', "");
    }

    // Python: `[_|-]` — `|` is literal inside character class.
    let prefix_re = Regex::new(r"^[a-zA-Z0-9]*[_|\-]").unwrap();
    // Python: re.search("-[a-zA-Z]+", ...) — finds the FIRST occurrence
    // anywhere, then truncates at that position.
    let end_re = Regex::new(r"-[a-zA-Z]+").unwrap();

    if prefix_re.is_match(&sys) {
        sys = prefix_re.replace(&sys, "").to_string();
        if let Some(m) = end_re.find(&sys) {
            sys.truncate(m.start());
        }
    }

    if prefix_re.is_match(&pkg) {
        pkg = prefix_re.replace(&pkg, "").to_string();
        if let Some(m) = end_re.find(&pkg) {
            pkg.truncate(m.start());
        }
    }

    gh_rftarget::gh_version_compare(&pkg, &sys)
}

// ===========================================================================
// HGXRUBINRFTarget
// ===========================================================================

/// Platform-specific RFTarget for HGX Rubin NVL8 systems.
///
/// Uses multipart upload and behaves similarly to HGXB100RFTarget
/// with platform-specific identification.
pub struct HGXRUBINRFTarget {
    /// BMC connection handle.
    pub bmc_access: BmcAccess,
    /// Component names treated as fungible.
    pub fungible_components: Vec<String>,
    /// Message displayed upon successful update completion.
    pub update_completion_msg: String,
    /// Whether the progress table header has been printed during monitoring.
    pub progress_table_header_printed: bool,
    /// Optional platform configuration dictionary.
    pub config_dict: Option<Value>,
}

impl HGXRUBINRFTarget {
    /// Create a new `HGXRUBINRFTarget` with sensible defaults.
    pub fn new(bmc_access: BmcAccess, config_dict: Option<Value>) -> Self {
        Self {
            bmc_access,
            fungible_components: vec!["gpu".to_string(), "sma".to_string(), "connectx".to_string()],
            update_completion_msg: "Refer to Firmware Update Document on \
                                    activation steps for new firmware to take effect."
                .to_string(),
            progress_table_header_printed: false,
            config_dict,
        }
    }
}

#[async_trait::async_trait]
impl RFTarget for HGXRUBINRFTarget {
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
        "HGXRUBINRFTarget"
    }

    fn get_update_uri(&self, update_service_response: &Value) -> String {
        if let Some(uri) = update_service_response
            .get("MultipartHttpPushUri")
            .and_then(|v| v.as_str())
        {
            return uri.to_string();
        }
        "/redfish/v1/UpdateService/update-multipart".to_string()
    }

    /// HGX RUBIN supports only RF_PWR_CYCLE via Manager.Reset.
    async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        let mut quiet_json = json!({"Error": [], "Error Code": 0, "Output": []});
        let command_to_arg = [("RF_PWR_CYCLE", "ForceRestart")];
        let supported: Vec<&str> = command_to_arg.iter().map(|(k, _)| *k).collect();

        if !supported.contains(&cmd_args.cmd.as_str()) {
            Util::bail_nvfwupd(
                1,
                &format!(
                    "Activation command {} not supported for HGX RUBIN. Supported commands: {:?}",
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
        let (post_status, response) = self
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
            if post_status {
                println!("{} requested successfully.", cmd_args.cmd);
            } else {
                println!("{} request failed.", cmd_args.cmd);
            }

            println!("Server response:");
            println!("{}", NvUtils::redacted_json_pretty_4space(&response));
        }

        if post_status {
            0
        } else {
            1
        }
    }

    /// HGX Rubin: always multipart. Resolves special (defaults to
    /// `{"Targets":[]}`), matching the Python 2.1.2 default.
    /// oem_parameters, and staged flags with Oem.Nvidia.UpdateOption.
    async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        // Resolve oem_parameters
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

        // Resolve special
        let special_str = match self
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

        let mut json_data: Value = special_str
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(json!({"Targets": []}));

        // Staged flags
        if cmd_args.staged_update {
            json_data["Oem"] = json!({"Nvidia": {"UpdateOption": "StageOnly"}});
        } else if cmd_args.staged_activate_update {
            json_data["Oem"] = json!({"Nvidia": {"UpdateOption": "StageAndActivate"}});
        }

        let oem_val = oem_params_json.map(Value::String);
        self.update_component_multipart(
            None,
            update_uri,
            update_file,
            time_out,
            Some(&json_data),
            None,
            oem_val.as_ref(),
            json_dict,
            parallel_update,
            cmd_args.quiet,
        )
        .await
    }

    fn version_newer(&self, pkg_version: &str, sys_version: &str) -> bool {
        hgxb100_version_compare(pkg_version, sys_version)
    }

    fn is_fungible_component(&self, component_name: &str) -> bool {
        // HGXRUBIN inherits HGXB100 is_fungible_component — excludes "inforom" and "driver"
        let lower = component_name.to_lowercase();
        self.fungible_components
            .iter()
            .any(|f| lower.contains(f.as_str()))
            && !lower.contains("inforom")
            && !lower.contains("driver")
    }

    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        _pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        hgxb100_get_component_version(pldm_version_dict, ap_name)
    }

    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        gh_get_identifier_from_chassis(self.target_access(), ap_inv_uri).await
    }

    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> Option<String> {
        hgxb100_get_version_sku(identifier, pldm_version_dict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn cmd_args() -> CmdArgs {
        CmdArgs {
            cmd: "update_fw".to_string(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: true,
            special: None,
            oem_parameters: None,
        }
    }

    #[tokio::test]
    async fn hgxrubin_update_defaults_to_empty_targets() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"Id": "Task-1"})))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("HGXRubin_fw.fwpkg");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();

        let mut target = HGXRUBINRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-hgx-rubin"),
            None,
        );

        let task_id = target
            .update_component(
                &cmd_args(),
                "/upload",
                update_file.to_str().unwrap(),
                30,
                None,
                false,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("Task-1"));
        let requests = server.received_requests().await.unwrap();
        let body = String::from_utf8_lossy(&requests[0].body);
        assert!(body.contains("name=\"UpdateParameters\""));
        assert!(body.contains("{\"Targets\":[]}"));
    }
}
