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

//! DGX_RFTarget implementation for DGX (AMI BMC) platforms.

use serde_json::{json, Value};

use crate::bmc_access::BmcAccess;
use crate::rf_target::{CmdArgs, PkgParser, RFTarget};
use crate::util::{BailAction, TraceFlags, Util};
use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// DGX_RFTarget
// ---------------------------------------------------------------------------

/// Platform-specific RFTarget for DGX systems with AMI BMC.
pub struct DGXRFTarget {
    /// BMC connection handle.
    pub bmc_access: BmcAccess,
    /// Component names treated as fungible.
    pub fungible_components: Vec<String>,
    /// Message displayed upon successful update completion.
    pub update_completion_msg: String,
    /// Whether the progress table header has been printed during monitoring.
    pub progress_table_header_printed: bool,
    /// Whether update completion guidance should be suppressed.
    pub suppress_update_completion_msg: bool,
    /// Optional platform configuration dictionary.
    pub config_dict: Option<Value>,
}

impl DGXRFTarget {
    /// Create a new `DGXRFTarget` with sensible defaults.
    pub fn new(bmc_access: BmcAccess, config_dict: Option<Value>) -> Self {
        Self {
            bmc_access,
            fungible_components: vec![
                "gpu".to_string(),
                "psu".to_string(),
                "sma".to_string(),
                "connectx".to_string(),
                "nvswitch".to_string(),
            ],
            update_completion_msg: "Refer to the 'DGX Firmware Update Guide' on \
                                    activation steps for new firmware to take effect."
                .to_string(),
            progress_table_header_printed: false,
            suppress_update_completion_msg: false,
            config_dict,
        }
    }

    fn normalized_platform(value: &str) -> String {
        value
            .to_ascii_lowercase()
            .chars()
            .filter(|ch| !matches!(ch, '-' | '_' | ' '))
            .collect()
    }

    fn platform_is_dgxrubin(value: &str) -> bool {
        Self::normalized_platform(value).contains("dgxrubin")
    }

    fn config_target_platform(&self) -> Option<&str> {
        let config = self.config_dict.as_ref()?;
        if let Some(platform) = config.get("TargetPlatform").and_then(Value::as_str) {
            return Some(platform);
        }

        let test_ip = self.bmc_access.ip.replace(['[', ']'], "");
        config
            .get("Targets")
            .and_then(Value::as_array)?
            .iter()
            .find(|target| {
                target
                    .get("BMC_IP")
                    .and_then(Value::as_str)
                    .is_some_and(|ip| ip == test_ip)
            })
            .and_then(|target| {
                target
                    .get("TARGET_PLATFORM")
                    .or_else(|| target.get("TargetPlatform"))
                    .and_then(Value::as_str)
            })
    }

    fn is_dgxrubin(&self) -> bool {
        [
            Some(self.bmc_access.servertype.as_str()),
            Some(self.bmc_access.model.as_str()),
            self.config_target_platform(),
        ]
        .into_iter()
        .flatten()
        .any(Self::platform_is_dgxrubin)
    }
}

#[async_trait::async_trait]
impl RFTarget for DGXRFTarget {
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

    fn update_completion_msg_suppressed(&self) -> bool {
        self.suppress_update_completion_msg
    }

    fn set_update_completion_msg_suppressed(&mut self, suppressed: bool) {
        self.suppress_update_completion_msg = suppressed;
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
        "DGX_RFTarget"
    }

    // ------------------------------------------------------------------
    // make_update_target_json override
    // ------------------------------------------------------------------

    /// DGX: creates Targets JSON per component (skipping HGX entries),
    /// plus HGX_Full.json and DGX_Full.json.
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
        for member in members {
            let inv_url = match member.get("@odata.id").and_then(|v| v.as_str()) {
                Some(u) => u,
                None => continue,
            };
            if inv_url.contains("HGX") {
                continue;
            }
            let name = inv_url.rsplit('/').next().unwrap_or("");
            let target_json = json!({"Targets": [inv_url]});
            let fp = format!("{}/{}.json", dir_path, name);
            if tokio::fs::write(&fp, serde_json::to_string_pretty(&target_json).unwrap())
                .await
                .is_ok()
            {
                file_list.push(fp);
            }
        }
        // HGX_Full.json. DGXRubin full-wrapper updates expect empty parameters.
        let hgx = if self.is_dgxrubin() {
            json!({})
        } else {
            json!({"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/HGX_0"]})
        };
        let fp = format!("{}/HGX_Full.json", dir_path);
        if tokio::fs::write(&fp, serde_json::to_string_pretty(&hgx).unwrap())
            .await
            .is_ok()
        {
            file_list.push(fp);
        }
        // DGX_Full.json (empty)
        let fp = format!("{}/DGX_Full.json", dir_path);
        if tokio::fs::write(&fp, serde_json::to_string_pretty(&json!({})).unwrap())
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
    // Factory reset override
    // ------------------------------------------------------------------

    /// Perform factory reset with `{"ResetType": "ResetAll"}`.
    async fn factory_reset(&mut self, _reset_params: Option<&Value>) -> (bool, Value) {
        let params = json!({"ResetType": "ResetAll"});

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
    // OOB activation override
    // ------------------------------------------------------------------

    /// DGX supports RF_PWR_ON, RF_PWR_OFF, RF_PWR_CYCLE, RF_PWR_STATUS.
    /// No RF_AUX_PWR_CYCLE. Uses ComputerSystem.Reset.
    async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        let mut quiet_json = json!({"Error": [], "Error Code": 0, "Output": []});
        let supported = ["RF_PWR_ON", "RF_PWR_OFF", "RF_PWR_CYCLE", "RF_PWR_STATUS"];
        if !supported.contains(&cmd_args.cmd.as_str()) {
            Util::bail_nvfwupd(
                1,
                &format!("Activation command {} not supported in DGX", cmd_args.cmd),
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
            _ => unreachable!(),
        };

        let systems = self.bmc_access.get_systems_members(trace).await;
        let mut target_uri: Option<String> = None;
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

        // Default Targets when special not provided
        let param_json_str = match param_list {
            Some(s) => s,
            None => {
                let file_name = std::path::Path::new(update_file)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(update_file);
                let targets = if file_name.contains("HGX") && !self.is_dgxrubin() {
                    json!({"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/HGX_0"]})
                } else {
                    json!({})
                };
                serde_json::to_string(&targets).unwrap_or_default()
            }
        };

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

    /// A component is fungible if it contains any of the fungible names
    /// but does NOT contain "inforom", "erot", or "driver".
    /// Python: `not any(map(component_name.__contains__, ["inforom", "erot", "driver"]))`
    fn is_fungible_component(&self, component_name: &str) -> bool {
        let lower = component_name.to_lowercase();
        if lower.contains("inforom") || lower.contains("erot") || lower.contains("driver") {
            return false;
        }
        self.fungible_components
            .iter()
            .any(|fc| lower.contains(&fc.to_lowercase()))
    }

    /// DGX `get_update_uri`: fallback is `/redfish/v1/UpdateService/upload`.
    fn get_update_uri(&self, update_service_response: &Value) -> String {
        update_service_response
            .get("MultipartHttpPushUri")
            .and_then(|v| v.as_str())
            .unwrap_or("/redfish/v1/UpdateService/upload")
            .to_string()
    }

    /// DGX-specific component version lookup from nested PLDM dict.
    /// Mirrors Python `DGX_RFTarget.get_component_version`.
    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        _pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        let mut ap_name = ap_name.to_lowercase();
        let mut hgx_pkg_only = false;

        if ap_name.starts_with("hgx_fw_") {
            ap_name = ap_name["hgx_fw_".len()..].to_string();
            hgx_pkg_only = true;
            if ap_name.starts_with("bmc") {
                ap_name = "hmc".to_string();
            }
        }

        if ap_name.starts_with("erot") {
            ap_name = "erot".to_string();
        }

        // DGX does not resolve GPU versions via name matching
        if ap_name.contains("gpu") && !ap_name.contains("inforom") {
            return None;
        }

        if ap_name.contains("bios") {
            ap_name = "sbios".to_string();
        } else if ap_name.contains("bmc") {
            ap_name = "bmc".to_string();
        } else if ap_name.contains("nvlink") {
            ap_name = "cx7".to_string();
        } else if ap_name.contains("cxswitch") {
            ap_name = "cx9".to_string();
        } else if ap_name.contains("cx7nic") {
            ap_name = "bluefield3".to_string();
        }

        ap_name = ap_name.replace('_', "");

        let mut ap_version = "N/A".to_string();

        let outer = match pldm_version_dict.as_object() {
            Some(o) => o,
            None => return None,
        };

        for (pkg, pkg_dict_val) in outer {
            if hgx_pkg_only && !pkg.contains("HGX") {
                continue;
            }
            if !hgx_pkg_only && pkg.contains("HGX") {
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

                if ap_name.contains("inforom") && !ap_pkg.contains("inforom") {
                    continue;
                }

                // B300 CPLD differentiation
                if ap_pkg.contains("cpldbp0") && ap_name.contains("cpldback0") {
                    ap_version = version_str.to_string();
                    break;
                }
                if ap_pkg.contains("cpld")
                    && ap_type.contains("dcscm")
                    && ap_name.contains("cplddcsm0")
                {
                    ap_version = version_str.to_string();
                    break;
                }
                if ap_pkg.contains("e1s")
                    && ap_type.contains("swcpld")
                    && ap_name.contains("cplde1sbp0")
                {
                    ap_version = version_str.to_string();
                    break;
                }
                if ap_pkg.contains("cpldmb") && ap_name.contains("cpldmb0") {
                    ap_version = version_str.to_string();
                    break;
                }
                if ap_pkg.contains("pcieswitch1") && ap_name.contains("pcieswitch1") {
                    ap_version = version_str.to_string();
                    break;
                }

                if ap_name.contains(&ap_pkg) {
                    ap_version = version_str.to_string();
                }

                if !hgx_pkg_only
                    && (ap_pkg.contains("pcieretimer") || ap_pkg.contains("pcieswitch"))
                {
                    if ap_pkg == ap_name {
                        ap_version = version_str.to_string();
                    } else {
                        let alt_ap = format!("{}0", ap_pkg);
                        if alt_ap == ap_name {
                            ap_version = version_str.to_string();
                        }
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

    /// DGX `get_identifier_from_chassis`: PSU uses `PartNumber` from
    /// PowerSupplies collection; all others use `SKU` from chassis.
    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        let ap_name = ap_inv_uri.rsplit('/').next().unwrap_or("");
        if ap_name.to_lowercase().contains("psu") {
            self.get_partno_from_chassis(ap_name).await
        } else {
            self.get_sku_from_chassis(ap_name).await
        }
    }

    /// DGX-specific version-by-SKU lookup.
    ///
    /// Python:
    /// - Skips non-HGX packages when `"gpu" in ap_name`
    /// - Non-PSU: exact match on `pkg_data[1] == identifier`
    /// - PSU: substring match `identifier.lower() in pkg_ap.lower()`
    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> Option<String> {
        let ap_lower = ap_name.to_lowercase();

        if let Some(outer) = pldm_version_dict.as_object() {
            for (pkg_name, pkg_dict_val) in outer {
                // Skip non-HGX packages when ap_name contains "gpu"
                if !pkg_name.to_uppercase().contains("HGX") && ap_lower.contains("gpu") {
                    continue;
                }
                if let Some(pkg_dict) = pkg_dict_val.as_object() {
                    for (ap_full, pkg_version_val) in pkg_dict {
                        let key_lower = ap_full.to_lowercase();
                        if let Some(arr) = pkg_version_val.as_array() {
                            if key_lower.contains("psu") {
                                // PSU: substring match on identifier in key
                                if key_lower.contains(&identifier.to_lowercase()) {
                                    return arr
                                        .first()
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string());
                                }
                            } else {
                                // Non-PSU: exact match on pkg_data[1] == identifier
                                let pkg_identifier =
                                    arr.get(1).and_then(|v| v.as_str()).unwrap_or("");
                                if pkg_identifier == identifier {
                                    return arr
                                        .first()
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        Some("N/A".to_string())
    }
}

// ---------------------------------------------------------------------------
// DGX helper methods
// ---------------------------------------------------------------------------

impl DGXRFTarget {
    /// Get `SKU` from `/redfish/v1/Chassis/{ap_name}` (with `FW_` stripped).
    async fn get_sku_from_chassis(&self, ap_name: &str) -> Option<String> {
        let ap_chassis = ap_name.replace("FW_", "");
        let uri = format!("/redfish/v1/Chassis/{}", ap_chassis);
        let (status, gpu_dict) = self
            .bmc_access
            .dispatch_request("GET", &uri, None, None)
            .await;
        if !status {
            return None;
        }
        gpu_dict
            .get("SKU")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Get `PartNumber` from DGX PowerSupplies collection.
    /// Tries both with and without underscores (B300 naming).
    async fn get_partno_from_chassis(&self, ap_name: &str) -> Option<String> {
        let ap_chassis = ap_name.replace("FW_", "");
        let base_ap_name = ap_chassis.clone();
        let ap_no_underscore = ap_name.replace('_', "");

        let uri = format!(
            "/redfish/v1/Chassis/DGX/PowerSubsystem/PowerSupplies/{}",
            ap_no_underscore
        );
        let (status, psu_dict) = self
            .bmc_access
            .dispatch_request("GET", &uri, None, None)
            .await;

        if !status {
            let uri2 = format!(
                "/redfish/v1/Chassis/DGX/PowerSubsystem/PowerSupplies/{}",
                base_ap_name
            );
            let (status2, psu_dict2) = self
                .bmc_access
                .dispatch_request("GET", &uri2, None, None)
                .await;
            if !status2 {
                return None;
            }
            return psu_dict2
                .get("PartNumber")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        psu_dict
            .get("PartNumber")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
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

    fn multipart_field_position(body: &[u8], field_name: &str) -> usize {
        let body = String::from_utf8_lossy(body);
        body.find(&format!("name=\"{field_name}\""))
            .unwrap_or_else(|| panic!("missing multipart field {field_name}: {body}"))
    }

    fn multipart_body(body: &[u8]) -> String {
        String::from_utf8_lossy(body).into_owned()
    }

    #[tokio::test]
    async fn cxswitch_inventory_ap_maps_to_cx9_package_component() {
        let target = DGXRFTarget::new(BmcAccess::default_stub(), None);
        let pldm_dict = json!({
            "DGX-Rubin-NVL8_0006_260518.1.0_custom": {
                "CX9:NVD0000000134": ["82.48.1492", "sku"]
            }
        });

        let version = target
            .get_component_version(&pldm_dict, "cxswitch_0", None)
            .await;

        assert_eq!(version.as_deref(), Some("82.48.1492"));
    }

    #[tokio::test]
    async fn update_component_uses_update_parameters_first() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"Id": "Task-1"})))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("DGX_fw.fwpkg");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();
        let mut target = DGXRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-dgx"),
            None,
        );
        let mut json_output = json!({
            "Error": [],
            "Error Code": 0,
            "Output": [{"sentinel": true}],
        });

        let task_id = target
            .update_component(
                &cmd_args(),
                "/upload",
                update_file.to_str().unwrap(),
                30,
                Some(&mut json_output),
                false,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("Task-1"));
        let requests = server.received_requests().await.unwrap();
        let first_body = &requests[0].body;
        assert!(
            multipart_field_position(first_body, "UpdateParameters")
                < multipart_field_position(first_body, "UpdateFile")
        );
        let output = json_output["Output"].as_array().unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0]["sentinel"], true);
        assert_eq!(output[1]["Id"], "Task-1");
    }

    #[tokio::test]
    async fn dgxrubin_hgx_update_defaults_to_empty_parameters() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"Id": "Task-1"})))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("HGX_fw.fwpkg");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();

        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-dgx-rubin");
        access.servertype = "DGXRubin".to_string();
        let mut target = DGXRFTarget::new(access, None);

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
        let body = multipart_body(&requests[0].body);
        assert!(body.contains("name=\"UpdateParameters\""));
        assert!(body.contains("{}"));
        assert!(!body.contains("/redfish/v1/UpdateService/FirmwareInventory/HGX_0"));
    }
}
