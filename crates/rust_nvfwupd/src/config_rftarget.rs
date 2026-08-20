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

//! Config-driven Redfish target module for nvfwupd.
//!
//! [`ConfigRFTarget`] reads a YAML configuration dictionary to determine
//! the platform type and delegates all [`RFTarget`] operations to the
//! dynamically selected platform-specific implementation.

use regex::Regex;
use serde_json::{json, Value};

use crate::bmc_access::BmcAccess;
use crate::dgx_rftarget::DGXRFTarget;
use crate::gb200_rftarget::GB200RFTarget;
use crate::gb200_switch_rftarget::GB200SwitchRFTarget;
use crate::gh200_rftarget::GH200RFTarget;
use crate::gh_rftarget::GHRFTarget;
use crate::hgxb100_rftarget::{HGXB100RFTarget, HGXRUBINRFTarget};
use crate::powershelf_rftarget::PowerShelfRFTarget;
use crate::rf_target::{
    CmdArgs, PkgParser, RFTarget, UpdatePreconditionMode, SERVER_TYPE_CLASS_DICT,
};
use crate::util::{BailAction, Util};

// ---------------------------------------------------------------------------
// Platform target factory
// ---------------------------------------------------------------------------

/// Create a platform-specific [`RFTarget`] implementation from a class name.
///
/// This function maps the string class name (e.g. "GHRFTarget") to a
/// concrete platform target instance. Each branch constructs the
/// appropriate struct with the given `bmc_access`.
///
/// Unknown class names receive a no-op stub that still satisfies the async
/// [`RFTarget`] contract.
fn create_platform_target(
    class_name: &str,
    bmc_access: BmcAccess,
    config_dict: Option<Value>,
) -> Option<Box<dyn RFTarget + Send + Sync>> {
    let rf_target: Box<dyn RFTarget + Send + Sync> = match class_name {
        "GHRFTarget" => Box::new(GHRFTarget::new(bmc_access, config_dict)),
        "DGX_RFTarget" | "DGXRFTarget" => Box::new(DGXRFTarget::new(bmc_access, config_dict)),
        "GH200RFTarget" => Box::new(GH200RFTarget::new(bmc_access, config_dict)),
        "GB200RFTarget" => Box::new(GB200RFTarget::new(bmc_access, config_dict)),
        "GB200SwitchRFTarget" => Box::new(GB200SwitchRFTarget::new(bmc_access, config_dict)),
        "HGXB100RFTarget" => Box::new(HGXB100RFTarget::new(bmc_access, config_dict)),
        "HGXRUBINRFTarget" => Box::new(HGXRUBINRFTarget::new(bmc_access, config_dict)),
        "PowerShelfRFTarget" => Box::new(PowerShelfRFTarget::new(bmc_access, config_dict)),
        _ => Box::new(PlatformTargetStub::new(class_name, bmc_access)),
    };
    Some(rf_target)
}

// ---------------------------------------------------------------------------
// PlatformTargetStub
// ---------------------------------------------------------------------------

/// Stub platform target used when the concrete platform modules are not
/// yet linked. Provides default/no-op implementations.
struct PlatformTargetStub {
    name: String,
    bmc_access: BmcAccess,
}

impl PlatformTargetStub {
    fn new(class_name: &str, bmc_access: BmcAccess) -> Self {
        Self {
            name: class_name.to_string(),
            bmc_access,
        }
    }
}

#[async_trait::async_trait]
impl RFTarget for PlatformTargetStub {
    fn target_access(&self) -> &BmcAccess {
        &self.bmc_access
    }

    fn target_access_mut(&mut self) -> &mut BmcAccess {
        &mut self.bmc_access
    }

    fn fungible_components(&self) -> &[String] {
        &[]
    }

    fn update_completion_msg(&self) -> &str {
        ""
    }

    fn set_update_completion_msg(&mut self, _msg: &str) {}

    fn progress_table_header_printed(&self) -> bool {
        false
    }

    fn set_progress_table_header_printed(&mut self, _printed: bool) {}

    fn config_dict(&self) -> Option<&Value> {
        None
    }

    fn class_name(&self) -> &str {
        &self.name
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

    async fn factory_reset(&mut self, _reset_params: Option<&Value>) -> (bool, Value) {
        (false, json!({}))
    }

    async fn background_copy(&self, _copy_parameters: &str) -> (bool, Value) {
        (false, Value::Null)
    }

    async fn start_update_monitor(
        &mut self,
        _recipe_list: &[String],
        _pkg_parser: &mut dyn PkgParser,
        _cmd_args: &CmdArgs,
        _time_out: u64,
        _parallel_update: bool,
        _json_dict: Option<&mut Value>,
        _update_delay: u64,
        _skip_pre_flight_checks: bool,
        _update_precondition_mode: Option<UpdatePreconditionMode>,
    ) -> (i32, Vec<String>) {
        (1, Vec::new())
    }

    async fn process_job_status(&self, _task_id: &str, _print_json: Option<&mut Value>) -> i32 {
        1
    }

    async fn run_oob_activation(&mut self, _cmd_args: &CmdArgs) -> i32 {
        1
    }
}

// ---------------------------------------------------------------------------
// ConfigRFTarget
// ---------------------------------------------------------------------------

/// Config-driven Redfish target that delegates to a dynamically chosen
/// platform-specific implementation.
///
/// Reads `TargetPlatform` from the YAML config dictionary, resolves it
/// through `SERVER_TYPE_CLASS_DICT`, and creates the appropriate
/// platform target to handle all Redfish operations.
pub struct ConfigRFTarget {
    /// BMC access layer for this target.
    pub target_access: BmcAccess,
    /// List of fungible component names.
    pub fungible_components: Vec<String>,
    /// Human-readable update completion message.
    pub update_completion_msg: String,
    /// Whether the progress table header has been printed during monitoring.
    pub progress_table_header_printed: bool,
    /// The full configuration dictionary from the YAML config file.
    pub config_dict: Value,
    /// The dynamically resolved platform target implementation.
    config_platform_target: Option<Box<dyn RFTarget + Send + Sync>>,
}

impl ConfigRFTarget {
    /// Create a new `ConfigRFTarget`.
    ///
    /// Reads the `TargetPlatform` key from `config_dict`, looks it up in
    /// `SERVER_TYPE_CLASS_DICT`, and instantiates the appropriate
    /// platform-specific target.
    pub fn new(bmc_access: BmcAccess, config_dict: Value, json_dict: Option<&mut Value>) -> Self {
        let update_msg = "Update successful. \
            Perform activation steps for new firmware to take effect."
            .to_string();

        let mut crt = Self {
            target_access: bmc_access.clone(),
            fungible_components: Vec::new(),
            update_completion_msg: update_msg,
            progress_table_header_printed: false,
            config_dict,
            config_platform_target: None,
        };

        crt.init_platform_obj(json_dict);
        crt
    }

    /// Resolve `TargetPlatform` from the config and create the inner
    /// platform target.
    fn init_platform_obj(&mut self, json_dict: Option<&mut Value>) {
        let mut target_platform: Option<String> = self
            .config_dict
            .get("TargetPlatform")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Remove brackets for IPv6 comparison
        let test_ip = self.target_access.ip.replace('[', "").replace(']', "");

        // If no top-level TargetPlatform, check the Targets array
        if target_platform.is_none() {
            if let Some(targets) = self.config_dict.get("Targets").and_then(|v| v.as_array()) {
                for target in targets {
                    let ip = target.get("BMC_IP").and_then(|v| v.as_str()).unwrap_or("");
                    if ip == test_ip {
                        target_platform = target
                            .get("TARGET_PLATFORM")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        break;
                    }
                }
            }
        }

        // Normalize to lowercase
        let target_platform = target_platform.map(|s| s.to_lowercase());

        if let Some(ref platform) = target_platform {
            if let Some(class_name) = SERVER_TYPE_CLASS_DICT.get(platform.as_str()) {
                self.config_platform_target = create_platform_target(
                    class_name,
                    self.target_access.clone(),
                    Some(self.config_dict.clone()),
                );
            } else {
                Util::bail_nvfwupd(
                    1,
                    &format!("TargetPlatform {} is not supported", platform),
                    BailAction::DoNothing,
                    json_dict.as_deref(),
                );
            }
        } else {
            Util::bail_nvfwupd(
                1,
                "TargetPlatform not specified in configuration",
                BailAction::DoNothing,
                json_dict.as_deref(),
            );
        }
    }

    /// Check if a component is fungible on the configured platform.
    pub fn is_fungible_component(&self, component_name: &str) -> bool {
        if let Some(ref target) = self.config_platform_target {
            return target.is_fungible_component(component_name);
        }
        false
    }

    /// Get the AP version from the PLDM dictionary.
    pub async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        pkg_parser: Option<&dyn PkgParser>,
    ) -> String {
        if let Some(ref target) = self.config_platform_target {
            return target
                .get_component_version(pldm_version_dict, ap_name, pkg_parser)
                .await
                .unwrap_or_else(|| "N/A".to_string());
        }
        "N/A".to_string()
    }

    /// Get AP identifier from Chassis response.
    pub async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        if let Some(ref target) = self.config_platform_target {
            return target.get_identifier_from_chassis(ap_inv_uri).await;
        }
        None
    }

    /// Get version from PLDM for a given identifier.
    pub fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> String {
        if let Some(ref target) = self.config_platform_target {
            return target
                .get_version_sku(identifier, pldm_version_dict, ap_name)
                .unwrap_or_else(|| "N/A".to_string());
        }
        "N/A".to_string()
    }

    /// Compare package vs system version.
    pub fn version_newer(&self, pkg_version: &str, sys_version: &str) -> bool {
        if let Some(ref target) = self.config_platform_target {
            return target.version_newer(pkg_version, sys_version);
        }
        false
    }

    /// Determine expected task type from the package recipe list.
    pub fn get_expected_task_type_from_package(&self, recipe_list: &[String]) -> String {
        if let Some(ref target) = self.config_platform_target {
            return target.get_expected_task_type_from_package(recipe_list);
        }
        "UNKNOWN".to_string()
    }

    /// Get the update URI from the UpdateService response.
    ///
    /// Respects config overrides for `FwUpdateMethod`, `HttpPushUri`,
    /// and `MultipartHttpPushUri`.
    pub fn get_update_uri(&self, update_service_response: &Value) -> String {
        let method = self
            .config_dict
            .get("FwUpdateMethod")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match method {
            "HttpPushUri" => self
                .config_dict
                .get("HttpPushUri")
                .and_then(|v| v.as_str())
                .unwrap_or("/redfish/v1/UpdateService")
                .to_string(),
            "MultipartHttpPushUri" => {
                let system_uri = update_service_response
                    .get("MultipartHttpPushUri")
                    .and_then(|v| v.as_str())
                    .unwrap_or("/redfish/v1/UpdateService/update-multipart");
                self.config_dict
                    .get("MultipartHttpPushUri")
                    .and_then(|v| v.as_str())
                    .unwrap_or(system_uri)
                    .to_string()
            }
            _ => {
                if let Some(ref target) = self.config_platform_target {
                    target.get_update_uri(update_service_response)
                } else {
                    "/redfish/v1/UpdateService".to_string()
                }
            }
        }
    }

    /// Get the task service URI for monitoring a given task.
    ///
    /// Uses `TaskServiceUri` from config if present, otherwise defaults
    /// to `/redfish/v1/TaskService/Tasks/`.
    pub fn get_task_service_uri(&self, task_id: &str) -> String {
        let base = self
            .config_dict
            .get("TaskServiceUri")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("/redfish/v1/TaskService/Tasks/");

        // Collapse duplicate slashes
        let raw = format!("{}/{}", base, task_id);
        let re = Regex::new(r"/+").unwrap();
        re.replace_all(&raw, "/").to_string()
    }

    /// Perform a firmware update on the configured target.
    ///
    /// Dispatches to the appropriate update method based on
    /// `FwUpdateMethod` in the config (HttpPushUri, MultipartHttpPushUri,
    /// or platform default).
    pub async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> String {
        let update_method = self
            .config_dict
            .get("FwUpdateMethod")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let upd_params_config = self.config_dict.get("UpdateParametersTargets").cloned();
        let oem_params_config = self.config_dict.get("OemParameters").cloned();
        let multipart_opts = self.config_dict.get("MultipartOptions").cloned();
        let special_params = self.config_dict.get("SpecialUpdateParameters").cloned();

        match update_method {
            "HttpPushUri" => {
                // Python: builds HttpPushUriTargets from UpdateParametersTargets,
                // then calls super().update_component_pushuri(params_json, ...)
                let params_json: Option<Value> = upd_params_config.map(|upc| {
                    if upc.is_object() {
                        json!({})
                    } else {
                        json!({"HttpPushUriTargets": upc})
                    }
                });
                if let Some(ref mut target) = self.config_platform_target {
                    return target
                        .update_component_pushuri(
                            params_json.as_ref(),
                            update_uri,
                            update_file,
                            time_out,
                            json_dict,
                            parallel_update,
                        )
                        .await
                        .unwrap_or_default();
                }
                String::new()
            }
            "MultipartHttpPushUri" => {
                // Python: builds Targets + MultipartOptions dict, then calls
                // super().update_component_multipart(None, uri, file, ...)
                let mut updparams_dict = multipart_opts
                    .and_then(|v| if v.is_object() { Some(v) } else { None })
                    .unwrap_or_else(|| json!({}));

                if let Some(upc) = upd_params_config {
                    if upc.is_object() {
                        updparams_dict = json!({});
                    } else {
                        updparams_dict["Targets"] = upc;
                    }
                }

                let oem_json = oem_params_config.map(|v| {
                    if v.is_string() {
                        v
                    } else {
                        Value::String(v.to_string())
                    }
                });

                if let Some(ref mut target) = self.config_platform_target {
                    return target
                        .update_component_multipart(
                            None,
                            update_uri,
                            update_file,
                            time_out,
                            Some(&updparams_dict),
                            None,
                            oem_json.as_ref(),
                            json_dict,
                            parallel_update,
                            cmd_args.quiet,
                        )
                        .await
                        .unwrap_or_default();
                }
                String::new()
            }
            _ => {
                // Default: delegate to platform target's update_component
                // Wire SpecialUpdateParameters into cmd_args.special
                let mut cmd_args_mut = cmd_args.clone();
                if let Some(sp) = special_params {
                    if let Ok(serialized) = serde_json::to_string(&sp) {
                        cmd_args_mut.special = Some(vec![serialized]);
                    }
                }
                if let Some(ref mut target) = self.config_platform_target {
                    target
                        .update_component(
                            &cmd_args_mut,
                            update_uri,
                            update_file,
                            time_out,
                            json_dict,
                            parallel_update,
                        )
                        .await
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            }
        }
    }

    /// Perform a factory reset.
    ///
    /// Uses `BMCResetParameters` from config if present, otherwise
    /// delegates to the platform target.
    pub async fn factory_reset(&mut self, reset_params: Option<&Value>) -> (bool, Value) {
        let factory_reset_config = self.config_dict.get("BMCResetParameters").cloned();

        // Python: if BMCResetParameters is None, delegate to platform target
        if factory_reset_config.is_none() {
            if let Some(ref mut target) = self.config_platform_target {
                return target.factory_reset(reset_params).await;
            }
        }

        match factory_reset_config {
            Some(ref params) if params.is_object() => {
                // Python: super().factory_reset(factory_reset)
                // Delegate to platform target's factory_reset with config params
                if let Some(ref mut target) = self.config_platform_target {
                    target.factory_reset(Some(params)).await
                } else {
                    (false, json!({}))
                }
            }
            _ => {
                Util::bail_nvfwupd(
                    1,
                    &format!(
                        "Missing/Invalid configuration for BMCResetParameters. \
                         Given value {:?}, expected reset dict.",
                        factory_reset_config
                    ),
                    BailAction::DoNothing,
                    None,
                );
                (false, json!({}))
            }
        }
    }

    /// Execute a background copy operation.
    pub async fn background_copy(&self, copy_parameters: &str) -> (bool, Value) {
        if let Some(ref target) = self.config_platform_target {
            return target.background_copy(copy_parameters).await;
        }
        (false, Value::Null)
    }

    /// Create update target JSON files (not supported with config file).
    pub async fn make_update_target_json(&self, _dir_path: &str) -> bool {
        Util::bail_nvfwupd(
            1,
            "Target JSON files created using make_upd_targets \
             are not supported with config file.",
            BailAction::DoNothing,
            None,
        );
        false
    }

    /// Start the update monitoring loop.
    ///
    /// Use the base [`RFTarget`] monitor with `ConfigRFTarget` as `self` so
    /// config-specific update URI and update method routing stays in effect.
    pub async fn start_update_monitor(
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
        <Self as RFTarget>::start_update_monitor(
            self,
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

    /// Process job status for a given task ID.
    pub async fn process_job_status(&self, task_id: &str, print_json: Option<&mut Value>) -> i32 {
        if let Some(ref target) = self.config_platform_target {
            return target.process_job_status(task_id, print_json).await;
        }
        1
    }

    /// Perform out-of-band activation.
    pub async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        if let Some(ref mut target) = self.config_platform_target {
            return target.run_oob_activation(cmd_args).await;
        }
        1
    }
}

#[async_trait::async_trait]
impl RFTarget for ConfigRFTarget {
    fn target_access(&self) -> &BmcAccess {
        &self.target_access
    }

    fn target_access_mut(&mut self) -> &mut BmcAccess {
        &mut self.target_access
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
        Some(&self.config_dict)
    }

    fn class_name(&self) -> &str {
        "ConfigRFTarget"
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
        let task_id = ConfigRFTarget::update_component(
            self,
            cmd_args,
            update_uri,
            update_file,
            time_out,
            json_dict,
            parallel_update,
        )
        .await;
        if task_id.is_empty() {
            None
        } else {
            Some(task_id)
        }
    }

    fn is_fungible_component(&self, component_name: &str) -> bool {
        ConfigRFTarget::is_fungible_component(self, component_name)
    }

    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        let version =
            ConfigRFTarget::get_component_version(self, pldm_version_dict, ap_name, pkg_parser)
                .await;
        if version == "N/A" {
            None
        } else {
            Some(version)
        }
    }

    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        ConfigRFTarget::get_identifier_from_chassis(self, ap_inv_uri).await
    }

    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> Option<String> {
        let version = ConfigRFTarget::get_version_sku(self, identifier, pldm_version_dict, ap_name);
        if version == "N/A" {
            None
        } else {
            Some(version)
        }
    }

    fn version_newer(&self, pkg_version: &str, sys_version: &str) -> bool {
        ConfigRFTarget::version_newer(self, pkg_version, sys_version)
    }

    fn get_expected_task_type_from_package(&self, recipe_list: &[String]) -> String {
        ConfigRFTarget::get_expected_task_type_from_package(self, recipe_list)
    }

    fn get_update_uri(&self, update_service_response: &Value) -> String {
        ConfigRFTarget::get_update_uri(self, update_service_response)
    }

    fn get_task_service_uri(&self, task_id: &str) -> String {
        ConfigRFTarget::get_task_service_uri(self, task_id)
    }

    async fn factory_reset(&mut self, reset_params: Option<&Value>) -> (bool, Value) {
        ConfigRFTarget::factory_reset(self, reset_params).await
    }

    async fn background_copy(&self, copy_parameters: &str) -> (bool, Value) {
        ConfigRFTarget::background_copy(self, copy_parameters).await
    }

    async fn make_update_target_json(&self, dir_path: &str) -> bool {
        ConfigRFTarget::make_update_target_json(self, dir_path).await
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
        if let Some(ref mut target) = self.config_platform_target {
            if target.class_name() == "GB200SwitchRFTarget" {
                return target
                    .start_update_monitor(
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
                    .await;
            }
        }

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

    async fn process_job_status(&self, task_id: &str, print_json: Option<&mut Value>) -> i32 {
        ConfigRFTarget::process_job_status(self, task_id, print_json).await
    }

    async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        ConfigRFTarget::run_oob_activation(self, cmd_args).await
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    struct NoopPkgParser;

    #[async_trait::async_trait]
    impl PkgParser for NoopPkgParser {
        async fn parse_pkg(&mut self, _pkg_path: &str) -> (bool, String) {
            (true, String::new())
        }
    }

    struct RecordingTarget {
        bmc_access: BmcAccess,
        class_name: &'static str,
        push_uris: Arc<Mutex<Vec<String>>>,
        direct_update_uris: Arc<Mutex<Vec<String>>>,
        start_monitor_calls: Arc<Mutex<usize>>,
        fungible_components: Vec<String>,
        update_completion_msg: String,
        progress_table_header_printed: bool,
    }

    impl RecordingTarget {
        fn new(
            bmc_access: BmcAccess,
            push_uris: Arc<Mutex<Vec<String>>>,
            direct_update_uris: Arc<Mutex<Vec<String>>>,
        ) -> Self {
            Self::new_with_class_name(
                bmc_access,
                "RecordingTarget",
                push_uris,
                direct_update_uris,
                Arc::new(Mutex::new(0)),
            )
        }

        fn new_with_class_name(
            bmc_access: BmcAccess,
            class_name: &'static str,
            push_uris: Arc<Mutex<Vec<String>>>,
            direct_update_uris: Arc<Mutex<Vec<String>>>,
            start_monitor_calls: Arc<Mutex<usize>>,
        ) -> Self {
            Self {
                bmc_access,
                class_name,
                push_uris,
                direct_update_uris,
                start_monitor_calls,
                fungible_components: Vec::new(),
                update_completion_msg: String::new(),
                progress_table_header_printed: false,
            }
        }
    }

    #[async_trait::async_trait]
    impl RFTarget for RecordingTarget {
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
            self.class_name
        }

        #[allow(clippy::too_many_arguments)]
        async fn update_component(
            &mut self,
            _cmd_args: &CmdArgs,
            update_uri: &str,
            _update_file: &str,
            _time_out: u64,
            _json_dict: Option<&mut Value>,
            _parallel_update: bool,
        ) -> Option<String> {
            self.direct_update_uris
                .lock()
                .unwrap()
                .push(update_uri.to_string());
            Some("direct-task".to_string())
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

        #[allow(clippy::too_many_arguments)]
        async fn update_component_pushuri(
            &mut self,
            _param_list: Option<&Value>,
            update_uri: &str,
            _update_file: &str,
            _time_out: u64,
            _json_dict: Option<&mut Value>,
            _parallel_update: bool,
        ) -> Option<String> {
            self.push_uris.lock().unwrap().push(update_uri.to_string());
            Some("push-task".to_string())
        }

        #[allow(clippy::too_many_arguments)]
        async fn start_update_monitor(
            &mut self,
            _recipe_list: &[String],
            _pkg_parser: &mut dyn PkgParser,
            _cmd_args: &CmdArgs,
            _time_out: u64,
            _parallel_update: bool,
            _json_dict: Option<&mut Value>,
            _update_delay: u64,
            _skip_pre_flight_checks: bool,
            _update_precondition_mode: Option<UpdatePreconditionMode>,
        ) -> (i32, Vec<String>) {
            *self.start_monitor_calls.lock().unwrap() += 1;
            (0, vec!["switch-task".to_string()])
        }
    }

    #[test]
    fn test_get_task_service_uri_default() {
        let config = json!({});
        let crt = ConfigRFTarget {
            target_access: BmcAccess::default_stub(),
            fungible_components: Vec::new(),
            update_completion_msg: String::new(),
            progress_table_header_printed: false,
            config_dict: config,
            config_platform_target: None,
        };
        let uri = crt.get_task_service_uri("42");
        assert!(uri.contains("42"));
        assert!(uri.starts_with("/redfish"));
    }

    #[test]
    fn test_get_task_service_uri_custom() {
        let config = json!({"TaskServiceUri": "/custom/tasks"});
        let crt = ConfigRFTarget {
            target_access: BmcAccess::default_stub(),
            fungible_components: Vec::new(),
            update_completion_msg: String::new(),
            progress_table_header_printed: false,
            config_dict: config,
            config_platform_target: None,
        };
        let uri = crt.get_task_service_uri("99");
        assert!(uri.contains("custom"));
        assert!(uri.contains("99"));
    }

    #[test]
    fn test_get_update_uri_http_push() {
        let config = json!({
            "FwUpdateMethod": "HttpPushUri",
            "HttpPushUri": "/custom/push"
        });
        let crt = ConfigRFTarget {
            target_access: BmcAccess::default_stub(),
            fungible_components: Vec::new(),
            update_completion_msg: String::new(),
            progress_table_header_printed: false,
            config_dict: config,
            config_platform_target: None,
        };
        let uri = crt.get_update_uri(&json!({}));
        assert_eq!(uri, "/custom/push");
    }

    #[tokio::test]
    async fn start_update_monitor_routes_through_config_update_method() {
        let mut access = BmcAccess::default_stub();
        access.base_url = "http://127.0.0.1:1".to_string();
        access.transport_type = "http".to_string();

        let push_uris = Arc::new(Mutex::new(Vec::new()));
        let direct_update_uris = Arc::new(Mutex::new(Vec::new()));
        let inner_target = RecordingTarget::new(
            access.clone(),
            Arc::clone(&push_uris),
            Arc::clone(&direct_update_uris),
        );

        let config = json!({
            "FwUpdateMethod": "HttpPushUri",
            "HttpPushUri": "/configured/push"
        });
        let mut target: Box<dyn RFTarget + Send + Sync> = Box::new(ConfigRFTarget {
            target_access: access,
            fungible_components: Vec::new(),
            update_completion_msg: String::new(),
            progress_table_header_printed: false,
            config_dict: config,
            config_platform_target: Some(Box::new(inner_target)),
        });
        let cmd_args = CmdArgs {
            cmd: String::new(),
            background: true,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special: None,
            oem_parameters: None,
        };
        let recipes = vec!["/tmp/fw.fwpkg".to_string()];
        let mut pkg_parser = NoopPkgParser;

        let (err_status, task_ids) = target
            .start_update_monitor(
                &recipes,
                &mut pkg_parser,
                &cmd_args,
                1,
                false,
                None,
                0,
                true,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        assert_eq!(err_status, 0);
        assert!(task_ids.is_empty());
        assert_eq!(push_uris.lock().unwrap().as_slice(), ["/configured/push"]);
        assert!(direct_update_uris.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn start_update_monitor_preserves_switch_workflow_for_config_switch() {
        let access = BmcAccess::default_stub();
        let push_uris = Arc::new(Mutex::new(Vec::new()));
        let direct_update_uris = Arc::new(Mutex::new(Vec::new()));
        let start_monitor_calls = Arc::new(Mutex::new(0));
        let inner_target = RecordingTarget::new_with_class_name(
            access.clone(),
            "GB200SwitchRFTarget",
            Arc::clone(&push_uris),
            Arc::clone(&direct_update_uris),
            Arc::clone(&start_monitor_calls),
        );

        let config = json!({
            "TargetPlatform": "gb200switch",
            "FwUpdateMethod": "HttpPushUri",
            "HttpPushUri": "/configured/push"
        });
        let mut target: Box<dyn RFTarget + Send + Sync> = Box::new(ConfigRFTarget {
            target_access: access,
            fungible_components: Vec::new(),
            update_completion_msg: String::new(),
            progress_table_header_printed: false,
            config_dict: config,
            config_platform_target: Some(Box::new(inner_target)),
        });
        let cmd_args = CmdArgs {
            cmd: String::new(),
            background: true,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special: None,
            oem_parameters: None,
        };
        let recipes = vec!["/tmp/fw.fwpkg".to_string()];
        let mut pkg_parser = NoopPkgParser;

        let (err_status, task_ids) = target
            .start_update_monitor(
                &recipes,
                &mut pkg_parser,
                &cmd_args,
                1,
                false,
                None,
                0,
                true,
                Some(UpdatePreconditionMode::SingleShot),
            )
            .await;

        assert_eq!(err_status, 0);
        assert_eq!(task_ids, ["switch-task"]);
        assert_eq!(*start_monitor_calls.lock().unwrap(), 1);
        assert!(push_uris.lock().unwrap().is_empty());
        assert!(direct_update_uris.lock().unwrap().is_empty());
    }

    #[test]
    fn config_factory_passes_config_to_switch_target() {
        let config = json!({
            "TargetPlatform": "gb200switch",
            "UpdateParametersTargets": ["BMC"]
        });
        let crt = ConfigRFTarget::new(BmcAccess::default_stub(), config, None);
        let inner = crt
            .config_platform_target
            .as_ref()
            .expect("config target should create inner switch target");

        assert_eq!(inner.class_name(), "GB200SwitchRFTarget");
        assert_eq!(
            inner
                .config_dict()
                .and_then(|cfg| cfg.pointer("/UpdateParametersTargets/0"))
                .and_then(Value::as_str),
            Some("BMC")
        );
    }
}
