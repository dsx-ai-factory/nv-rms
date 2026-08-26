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

//! GHRFTarget implementation for GH/HGX/MGX platforms

use regex::Regex;
use serde_json::{json, Value};

use crate::bmc_access::BmcAccess;
use crate::rf_target::{CmdArgs, PkgParser, RFTarget};
use crate::util::{BailAction, Util};
use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Substrings in a package version string that mark it as an HGX-tray package.
const HGX_PLATFORM_TOKENS: &[&str] = &["HGX", "4059", "4764", "4974", "4975", "GB200-NVL4", "HMC"];

// ---------------------------------------------------------------------------
// GHRFTarget
// ---------------------------------------------------------------------------

/// Platform-specific RFTarget for GH, HGX, MGX
pub struct GHRFTarget {
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

impl GHRFTarget {
    /// Create a new `GHRFTarget` with sensible defaults.
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

    /// Check whether a component name matches a fungible GPU component,
    /// excluding inforom and erot sub-components.
    fn is_gpu_fungible(name: &str) -> bool {
        let lower = name.to_lowercase();
        lower.contains("gpu")
            && lower.contains("hgx")
            && !lower.contains("inforom")
            && !lower.contains("erot")
            && !lower.contains("driver")
    }

    /// Check whether a package version string belongs to an HGX-tray package.
    pub fn is_hgx_pkg(pkg_name: &str) -> bool {
        HGX_PLATFORM_TOKENS.iter().any(|tok| pkg_name.contains(tok))
    }
}

// ---------------------------------------------------------------------------
// Shared version comparison helpers
// ---------------------------------------------------------------------------

/// Base version comparison: split on `.` and `-`, pad segments, compare.
/// Equivalent to Python `RFTarget.version_newer`.
pub fn base_version_compare(pkg_version: &str, sys_version: &str) -> bool {
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

/// GH-level version normalization + comparison (GraceBMC stripping, then base).
/// This is the shared function that GH200 and HGXB100 can chain through.
pub fn gh_version_compare(pkg_version: &str, sys_version: &str) -> bool {
    let mut sys = sys_version.to_string();

    let pkg = if !sys_version.contains('.') {
        pkg_version.replace('.', "")
    } else {
        pkg_version.to_string()
    };

    // Python: `"GraceBMC[_|-]"` — inside [] the `|` is a LITERAL pipe
    // character, not alternation. Match `_`, `|`, or `-`.
    let grace_re = Regex::new(r"(?i)^GraceBMC[_|\-]").unwrap();
    if grace_re.is_match(&sys) {
        sys = grace_re.replace(&sys, "").to_string();
        // Python: re.search("-[a-zA-Z]+", sys_version) — finds the
        // FIRST occurrence anywhere, then truncates at that position.
        let end_re = Regex::new(r"-[a-zA-Z]+").unwrap();
        if let Some(m) = end_re.find(&sys) {
            sys.truncate(m.start());
        }
    }

    base_version_compare(&pkg, &sys)
}

#[async_trait::async_trait]
impl RFTarget for GHRFTarget {
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
        "GHRFTarget"
    }

    // ------------------------------------------------------------------
    // Version comparison override
    // ------------------------------------------------------------------

    /// GH-specific version comparison that strips the `GraceBMC_` prefix
    /// from `sys_version` and removes dots when the system version has none.
    fn version_newer(&self, pkg_version: &str, sys_version: &str) -> bool {
        gh_version_compare(pkg_version, sys_version)
    }

    // ------------------------------------------------------------------
    // make_update_target_json override
    // ------------------------------------------------------------------

    /// Generate update target JSON files from FirmwareInventory.
    ///
    /// Python: GHRFTarget.make_update_target_json
    /// - One JSON per component: `{"HttpPushUriTargets": [uri]}`
    /// - CPU entries (non-ERoT) grouped into `CPU.json`
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

        // Create output directory
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
            let component_name = inv_url.rsplit('/').next().unwrap_or("");

            // Group CPU components (non-ERoT) into a single file
            if component_name.contains("CPU") && !component_name.contains("ERoT") {
                cpu_list.push(inv_url.to_string());
                continue;
            }

            let target_json = json!({"HttpPushUriTargets": [inv_url]});
            let file_path = format!("{}/{}.json", dir_path, component_name);
            match tokio::fs::write(
                &file_path,
                serde_json::to_string_pretty(&target_json).unwrap(),
            )
            .await
            {
                Ok(_) => file_list.push(file_path),
                Err(e) => tracing::warn!("Error writing {}: {}", file_path, e),
            }
        }

        // Write grouped CPU file
        if !cpu_list.is_empty() {
            let target_json = json!({"HttpPushUriTargets": cpu_list});
            let file_path = format!("{}/CPU.json", dir_path);
            match tokio::fs::write(
                &file_path,
                serde_json::to_string_pretty(&target_json).unwrap(),
            )
            .await
            {
                Ok(_) => file_list.push(file_path),
                Err(e) => tracing::warn!("Error writing {}: {}", file_path, e),
            }
        }

        println!("Created following update parameter files:");
        for f in &file_list {
            println!("{}", f);
        }
        true
    }

    // ------------------------------------------------------------------
    // Factory reset override
    // ------------------------------------------------------------------

    /// Perform factory reset with `{"ResetToDefaultsType": "ResetAll"}`.
    async fn factory_reset(&mut self, _reset_params: Option<&Value>) -> (bool, Value) {
        let params = json!({"ResetToDefaultsType": "ResetAll"});
        // Delegate to the default implementation with GH-specific params
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
        let (status, response_dict) = self
            .target_access()
            .dispatch_request("POST", &reset_uri, Some(&params), None)
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

    // ------------------------------------------------------------------
    // Abstract method implementations
    // ------------------------------------------------------------------

    /// Perform firmware update using push URI upload.
    /// If `cmd_args.special` is set, PATCHes `/redfish/v1/UpdateService`
    /// with the user-supplied JSON before uploading.
    async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        mut json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        // Resolve special → JSON string (inline or from file)
        let special_targets = match self
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

        // PATCH UpdateService with the special JSON before uploading
        if let Some(ref targets_json) = special_targets {
            let param_data: Value = serde_json::from_str(targets_json).unwrap_or_default();
            let (status, err_dict) = self
                .target_access()
                .dispatch_request(
                    "PATCH",
                    "/redfish/v1/UpdateService",
                    Some(&param_data),
                    None,
                )
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

        // POST firmware file upload
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
    }

    /// A component is fungible if it contains "gpu" but not "inforom" or "erot".
    fn is_fungible_component(&self, component_name: &str) -> bool {
        Self::is_gpu_fungible(component_name)
    }

    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        _pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        gh_get_component_version(pldm_version_dict, ap_name)
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
        gh_get_version_sku(identifier, pldm_version_dict, ap_name)
    }
}

// ---------------------------------------------------------------------------
// Public shared helpers (used by GH200 and other GH-family targets)
// ---------------------------------------------------------------------------

/// GH-style `get_component_version` from nested PLDM dict.
/// Mirrors Python `GHRFTarget.get_component_version`.
pub fn gh_get_component_version(pldm_version_dict: &Value, ap_name: &str) -> Option<String> {
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

    if ap_name.contains("gpu") && !ap_name.contains("inforom") && !ap_name.contains("sma") {
        ap_name = "gpu".to_string();
    } else if ap_name.contains("cpu") && !ap_name.contains("sbios") {
        ap_name = "sbios".to_string();
    } else if !hgx_pkg_only && ap_name.contains("pcie") {
        ap_name = "pcieswitch".to_string();
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

            let temp_pkg = ap_full.split(',').next().unwrap_or("").to_lowercase();
            let ap_pkg_raw = temp_pkg.split(':').next().unwrap_or("");
            let ap_type = temp_pkg.split(':').nth(1).unwrap_or("");
            let ap_pkg = ap_pkg_raw.replace('_', "").replace('-', "");

            let ap_pkg = if ap_pkg == "cx9" {
                "fwcx".to_string()
            } else {
                ap_pkg
            };

            if ap_name.contains("inforom") && !ap_pkg.contains("inforom") {
                continue;
            }

            if ap_pkg.contains("sma") && ap_type.contains("cx8") && ap_name.contains("smacx8") {
                ap_version = version_str.to_string();
                break;
            }
            if ap_pkg.contains("sma") && ap_type.contains("sxm7") && ap_name.contains("smagpu") {
                ap_version = version_str.to_string();
                break;
            }
            if ap_pkg.contains("sma") && ap_type.contains("gpu") && ap_name.contains("gpusma") {
                ap_version = version_str.to_string();
                break;
            }
            if ap_pkg.contains("sma")
                && ap_type.contains("hpm")
                && ap_name.contains("processormodulesma")
            {
                ap_version = version_str.to_string();
                break;
            }
            if ap_pkg.contains("sbios") && ap_type.contains("fmc") && ap_name.contains("sbiosfmc") {
                ap_version = version_str.to_string();
                break;
            }
            if ap_pkg.contains("sbios") && ap_type.contains("fws") && ap_name.contains("sbiosfw") {
                ap_version = version_str.to_string();
                break;
            }

            if ap_name.contains(&ap_pkg) {
                ap_version = version_str.to_string();
            } else if ap_pkg.contains("smr") && ap_name.contains("fpga") {
                ap_version = version_str.to_string();
                break;
            } else {
                let alt_ap = format!("{}0", ap_pkg);
                if alt_ap == ap_name {
                    ap_version = version_str.to_string();
                    break;
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
pub async fn gh_get_identifier_from_chassis(bmc: &BmcAccess, ap_inv_uri: &str) -> Option<String> {
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

/// GH-specific `get_version_sku`: requires "gpu" in the AP key AND
/// an exact match on `pkg_data[1] == identifier`.
///
/// Python:
/// ```python
/// for _, pkg_dict in pldm_version_dict.items():
///     for pkg_ap, pkg_data in pkg_dict.items():
///         if "gpu" in pkg_ap.lower():
///             if pkg_data[1] == identifier:
///                 return pkg_data[0]
/// return "N/A"
/// ```
pub fn gh_get_version_sku(
    identifier: &str,
    pldm_version_dict: &Value,
    _ap_name: &str,
) -> Option<String> {
    if let Some(outer) = pldm_version_dict.as_object() {
        for (_pkg, pkg_dict_val) in outer {
            if let Some(pkg_dict) = pkg_dict_val.as_object() {
                for (ap_full, pkg_version_val) in pkg_dict {
                    let key_lower = ap_full.to_lowercase();
                    // GH requires "gpu" in the AP key name.
                    if !key_lower.contains("gpu") {
                        continue;
                    }
                    // Exact match on identifier (pkg_data[1]).
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
