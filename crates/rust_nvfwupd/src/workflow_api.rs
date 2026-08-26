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

//! RMS-facing workflow facade.
//!
//! This module owns target construction and exposes structured request/result
//! functions so hosted callers do not need to construct concrete `RFTarget`
//! implementations or call CLI-shaped command modules directly.

use std::collections::{BTreeSet, HashMap};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use nvue_client::Client as NvueClient;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::bmc_access::{AccessType, BmcAccess};
use crate::dgx_rftarget::DGXRFTarget;
use crate::expected_inventory;
use crate::gb200_rftarget::GB200RFTarget;
use crate::gb200_switch_rftarget::{GB200SwitchRFTarget, SwitchPowerCycleError};
use crate::gh200_rftarget::GH200RFTarget;
use crate::gh_rftarget::GHRFTarget;
use crate::hgxb100_rftarget::{HGXB100RFTarget, HGXRUBINRFTarget};
use crate::pldm::{self, FirmwarePkg};
use crate::powershelf_rftarget::{is_powershelf_on_reset_handoff_task, PowerShelfRFTarget};
use crate::rf_target::{CmdArgs, PkgParser, RFTarget, UpdatePreconditionMode};
use crate::util::TraceFlags;
use crate::utils::Util as NvUtils;
use crate::workflow::{
    ActivationMode, ActivationRequest, ActivationSummary, FirmwareComponent, FirmwareUpdateOutcome,
    FirmwareUpdateRequest, FirmwareVersionCheckRequest, FirmwareVersionCheckSummary,
    FirmwareVersionCheckTarget, NvFwUpdError, Result, ServerType, StagedMode, TargetConfig,
    TaskHandle, TaskState, TaskStatus, UpdateSummary,
};

const DEFAULT_REDFISH_TIMEOUT_SECS: u64 = 900;
const DEFAULT_SWITCH_TIMEOUT_SECS: u64 = 1200;
const BACKGROUND_COPY_RMS_TIMEOUT_SECS: u64 = 15 * 60;
const BACKGROUND_COPY_RMS_POLL_INTERVAL_SECS: u64 = 10;
const EXPECTED_INVENTORY_POST_ACTIVATION_RETRIES: u32 = 3;
#[cfg(not(test))]
const EXPECTED_INVENTORY_POST_ACTIVATION_RETRY_INTERVAL_SECS: u64 = 120;
#[cfg(test)]
const EXPECTED_INVENTORY_POST_ACTIVATION_RETRY_INTERVAL_SECS: u64 = 0;
const COMPUTE_ACTIVATION_POWER_OFF_DELAY_SECS: u64 = 20;
const COMPUTE_ACTIVATION_COMMAND_RETRIES: u32 = 10;
#[cfg(not(test))]
const COMPUTE_ACTIVATION_COMMAND_RETRY_DELAY_SECS: u64 = 60;
#[cfg(test)]
const COMPUTE_ACTIVATION_COMMAND_RETRY_DELAY_SECS: u64 = 0;
const COMPUTE_ACTIVATION_BMC_WAIT_ATTEMPTS: u64 = 60;
const COMPUTE_ACTIVATION_BMC_WAIT_INTERVAL_SECS: u64 = 5;
const COMPUTE_ACTIVATION_POWER_ON_RESEND_INTERVAL_SECS: u64 = 60;
const SWITCH_ACTIVATION_RECOVERY_PATH: &str = "/nvue_v1/platform/firmware";
const POWERSHELF_ACTIVATION_RECOVERY_PATH: &str = "/redfish/v1";

#[cfg(not(test))]
const SWITCH_ACTIVATION_RECOVERY_ATTEMPTS: u32 = 45;
#[cfg(test)]
const SWITCH_ACTIVATION_RECOVERY_ATTEMPTS: u32 = 3;
#[cfg(not(test))]
const SWITCH_ACTIVATION_RECOVERY_INTERVAL_SECS: u64 = 15;
#[cfg(test)]
const SWITCH_ACTIVATION_RECOVERY_INTERVAL_SECS: u64 = 0;

#[cfg(not(test))]
const POWERSHELF_ACTIVATION_RECOVERY_ATTEMPTS: u32 = 90;
#[cfg(test)]
const POWERSHELF_ACTIVATION_RECOVERY_ATTEMPTS: u32 = 3;
#[cfg(not(test))]
const POWERSHELF_ACTIVATION_RECOVERY_INTERVAL_SECS: u64 = 15;
#[cfg(test)]
const POWERSHELF_ACTIVATION_RECOVERY_INTERVAL_SECS: u64 = 0;

#[cfg(not(test))]
const ACTIVATION_RECOVERY_PROBE_TIMEOUT_SECS: u64 = 5;
#[cfg(test)]
const ACTIVATION_RECOVERY_PROBE_TIMEOUT_SECS: u64 = 1;

/// Additive workflow facade for callers that already own an NVUE transport.
///
/// The process CLI and existing library callers can continue using the free
/// workflow functions. Hosted switch callers can attach request-scoped NVUE
/// access without placing TLS material in [`TargetConfig`].
#[derive(Clone, Default)]
pub struct WorkflowContext {
    nvue_client: Option<Arc<NvueClient>>,
}

impl WorkflowContext {
    /// Create a context that uses NVFWUPD's existing target access behavior.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a context that routes switch NVUE requests through `client`.
    pub fn with_nvue_client(client: Arc<NvueClient>) -> Self {
        Self {
            nvue_client: Some(client),
        }
    }

    /// Retrieve firmware inventory.
    pub async fn get_firmware_inventory(
        &self,
        target: TargetConfig,
    ) -> Result<Vec<FirmwareComponent>> {
        get_firmware_inventory_with_context(self, target).await
    }

    /// Compare installed firmware versions against package metadata.
    pub async fn verify_firmware_versions(
        &self,
        target: TargetConfig,
        request: FirmwareVersionCheckRequest,
    ) -> Result<FirmwareVersionCheckSummary> {
        verify_firmware_versions_with_context(self, target, request).await
    }

    /// Compare installed firmware versions and validate expected AP inventory first.
    pub async fn verify_firmware_versions_with_expected_inventory(
        &self,
        target: TargetConfig,
        request: FirmwareVersionCheckRequest,
        expected_inventory: Option<Vec<String>>,
    ) -> Result<FirmwareVersionCheckSummary> {
        verify_firmware_versions_with_expected_inventory_context(
            self,
            target,
            request,
            expected_inventory.as_deref(),
            ExpectedInventoryVersionCheckMode::SingleFetch,
        )
        .await
    }

    /// Compare installed firmware versions after activation, retrying expected inventory recovery.
    pub async fn verify_firmware_versions_after_activation_with_expected_inventory(
        &self,
        target: TargetConfig,
        request: FirmwareVersionCheckRequest,
        expected_inventory: Option<Vec<String>>,
    ) -> Result<FirmwareVersionCheckSummary> {
        verify_firmware_versions_with_expected_inventory_context(
            self,
            target,
            request,
            expected_inventory.as_deref(),
            ExpectedInventoryVersionCheckMode::PostActivationRetry,
        )
        .await
    }

    /// Compare only the firmware targets applied by the caller.
    pub async fn verify_firmware_target_versions(
        &self,
        target: TargetConfig,
        targets: Vec<FirmwareVersionCheckTarget>,
    ) -> Result<FirmwareVersionCheckSummary> {
        verify_firmware_target_versions_with_context(self, target, targets).await
    }

    /// Compare explicit update targets and validate expected AP inventory first.
    pub async fn verify_firmware_target_versions_with_expected_inventory(
        &self,
        target: TargetConfig,
        targets: Vec<FirmwareVersionCheckTarget>,
        expected_inventory: Option<Vec<String>>,
    ) -> Result<FirmwareVersionCheckSummary> {
        verify_firmware_target_versions_with_expected_inventory_context(
            self,
            target,
            targets,
            expected_inventory.as_deref(),
            ExpectedInventoryVersionCheckMode::SingleFetch,
        )
        .await
    }

    /// Compare explicit update targets after activation, retrying expected inventory recovery.
    pub async fn verify_firmware_target_versions_after_activation_with_expected_inventory(
        &self,
        target: TargetConfig,
        targets: Vec<FirmwareVersionCheckTarget>,
        expected_inventory: Option<Vec<String>>,
    ) -> Result<FirmwareVersionCheckSummary> {
        verify_firmware_target_versions_with_expected_inventory_context(
            self,
            target,
            targets,
            expected_inventory.as_deref(),
            ExpectedInventoryVersionCheckMode::PostActivationRetry,
        )
        .await
    }

    /// Start or run a firmware update.
    pub async fn update_firmware(
        &self,
        target: TargetConfig,
        request: FirmwareUpdateRequest,
    ) -> Result<FirmwareUpdateOutcome> {
        update_firmware_with_context(self, target, request, None).await
    }

    /// Start or run a firmware update after validating expected AP inventory.
    pub async fn update_firmware_with_expected_inventory(
        &self,
        target: TargetConfig,
        request: FirmwareUpdateRequest,
        expected_inventory: Option<Vec<String>>,
    ) -> Result<FirmwareUpdateOutcome> {
        update_firmware_with_context(self, target, request, expected_inventory.as_deref()).await
    }

    /// Query a firmware task.
    pub async fn get_task_status(&self, target: TargetConfig, task_id: &str) -> Result<TaskStatus> {
        get_task_status_with_context(self, target, task_id).await
    }

    /// Run a firmware activation request.
    pub async fn activate_firmware(
        &self,
        target: TargetConfig,
        request: ActivationRequest,
    ) -> Result<ActivationSummary> {
        activate_firmware_with_context(self, target, request).await
    }

    fn target_builder<'a>(&'a self, target: &'a TargetConfig) -> WorkflowTargetBuilder<'a> {
        WorkflowTargetBuilder::with_nvue_client(target, self.nvue_client.as_ref())
    }
}

struct WorkflowPkgParser<'a> {
    inner: &'a mut dyn FirmwarePkg,
    allow_unparsed_package: bool,
    last_parse_failure: Option<(String, String)>,
}

impl<'a> WorkflowPkgParser<'a> {
    fn new(inner: &'a mut dyn FirmwarePkg, allow_unparsed_package: bool) -> Self {
        Self {
            inner,
            allow_unparsed_package,
            last_parse_failure: None,
        }
    }

    fn take_last_parse_failure(&mut self) -> Option<(String, String)> {
        self.last_parse_failure.take()
    }
}

enum WorkflowTarget {
    Standard(Box<dyn RFTarget + Send + Sync>),
    Switch {
        target: Box<GB200SwitchRFTarget>,
        client: Option<Arc<NvueClient>>,
    },
}

impl WorkflowTarget {
    fn as_rf_target(&self) -> &(dyn RFTarget + Send + Sync) {
        match self {
            Self::Standard(target) => target.as_ref(),
            Self::Switch { target, .. } => target.as_ref(),
        }
    }

    fn as_rf_target_mut(&mut self) -> &mut (dyn RFTarget + Send + Sync) {
        match self {
            Self::Standard(target) => target.as_mut(),
            Self::Switch { target, .. } => target.as_mut(),
        }
    }

    fn target_access(&self) -> &BmcAccess {
        self.as_rf_target().target_access()
    }

    fn class_name(&self) -> &str {
        self.as_rf_target().class_name()
    }

    fn config_dict(&self) -> Option<&Value> {
        self.as_rf_target().config_dict()
    }

    async fn check_update_preconditions(
        &self,
        mode: UpdatePreconditionMode,
        json_dict: Option<&mut Value>,
    ) -> std::result::Result<(), String> {
        self.as_rf_target()
            .check_update_preconditions(mode, json_dict)
            .await
    }

    async fn get_firmware_inventory(
        &self,
        trace: TraceFlags,
        json_output: Option<&mut Value>,
        model: Option<&str>,
    ) -> (bool, i32, serde_json::Map<String, Value>) {
        if let Self::Switch {
            target,
            client: Some(client),
        } = self
        {
            return target
                .get_firmware_inventory_with_client(client, trace, json_output, model)
                .await;
        }

        self.as_rf_target()
            .get_firmware_inventory(trace, json_output, model)
            .await
    }

    async fn get_expected_inventory_ap_names(
        &self,
        trace: TraceFlags,
        json_output: Option<&mut Value>,
    ) -> (bool, i32, Vec<String>) {
        if let Self::Switch {
            target,
            client: Some(client),
        } = self
        {
            let (ok, err_code, inventory) = target
                .get_firmware_inventory_with_client(client, trace, json_output, None)
                .await;
            return (
                ok,
                err_code,
                expected_inventory::present_ap_names(&inventory),
            );
        }

        self.as_rf_target()
            .get_expected_inventory_ap_names(trace, json_output)
            .await
    }

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
        if let Self::Switch {
            target,
            client: Some(client),
        } = self
        {
            return target
                .start_update_monitor_with_client(
                    client,
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

        self.as_rf_target_mut()
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
            .await
    }

    async fn query_job_status(
        &self,
        task_id: &str,
        print_json: Option<&mut Value>,
    ) -> (bool, Value) {
        if let Self::Switch {
            target,
            client: Some(client),
        } = self
        {
            return target
                .query_job_status_with_client(client, task_id, print_json)
                .await;
        }

        self.as_rf_target()
            .query_job_status(task_id, print_json)
            .await
    }

    async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
        if let Self::Switch {
            target,
            client: Some(client),
        } = self
        {
            return target
                .run_oob_activation_with_client(client, cmd_args)
                .await;
        }

        self.as_rf_target_mut().run_oob_activation(cmd_args).await
    }

    async fn run_switch_power_cycle(
        &self,
    ) -> Result<crate::gb200_switch_rftarget::SwitchPowerCycleSummary> {
        let Self::Switch { target, client } = self else {
            return Err(NvFwUpdError::Unsupported("switch power-cycle activation"));
        };

        match client {
            Some(client) => target.run_nvue_power_cycle_with_client(client).await,
            None => target.run_nvue_power_cycle().await,
        }
        .map_err(map_switch_power_cycle_error)
    }

    async fn recovery_probe(&self, path: &'static str, timeout_secs: u64) -> bool {
        if let Self::Switch {
            client: Some(client),
            ..
        } = self
        {
            return client
                .get(path, Duration::from_secs(timeout_secs))
                .await
                .is_ok_and(|response| {
                    response.status == 200 && serde_json::from_str::<Value>(&response.body).is_ok()
                });
        }

        self.target_access()
            .dispatch_request_full("GET", path, None, None, timeout_secs, true, None)
            .await
            .0
    }
}

struct WorkflowTargetBuilder<'a> {
    target: &'a TargetConfig,
    nvue_client: Option<&'a Arc<NvueClient>>,
}

impl<'a> WorkflowTargetBuilder<'a> {
    fn new(target: &'a TargetConfig) -> Self {
        Self {
            target,
            nvue_client: None,
        }
    }

    fn with_nvue_client(
        target: &'a TargetConfig,
        nvue_client: Option<&'a Arc<NvueClient>>,
    ) -> Self {
        Self {
            target,
            nvue_client,
        }
    }

    fn build_target(&self) -> Result<WorkflowTarget> {
        let config_dict = None;

        if is_switch(self.target.server_type) {
            let access = self.build_nvue_access()?;
            let target = GB200SwitchRFTarget::new(access, config_dict);

            if let Some(client) = self.nvue_client {
                self.validate_hosted_target(client)?;
            }

            return Ok(WorkflowTarget::Switch {
                target: Box::new(target),
                client: self.nvue_client.cloned(),
            });
        }

        let access = self.build_bmc_access()?;

        let rf_target: Box<dyn RFTarget + Send + Sync> = match self.target.server_type {
            ServerType::DGX | ServerType::DGXRubin => {
                Box::new(DGXRFTarget::new(access, config_dict))
            }
            ServerType::HGX | ServerType::MGX => Box::new(GHRFTarget::new(access, config_dict)),
            ServerType::GH200 => Box::new(GH200RFTarget::new(access, config_dict)),
            ServerType::HGXB100 | ServerType::HGXB300 => {
                Box::new(HGXB100RFTarget::new(access, config_dict))
            }
            ServerType::HGXRubin => Box::new(HGXRUBINRFTarget::new(access, config_dict)),
            ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72 => {
                Box::new(GB200RFTarget::new(access, config_dict))
            }
            ServerType::GB200Switch | ServerType::GB300Switch | ServerType::VRNVL72Switch => {
                return Err(NvFwUpdError::Unsupported(
                    "switch target reached BMC target construction",
                ));
            }
            ServerType::PowerShelf => Box::new(PowerShelfRFTarget::new(access, config_dict)),
        };

        Ok(WorkflowTarget::Standard(rf_target))
    }

    fn validate_hosted_target(&self, client: &NvueClient) -> Result<()> {
        let endpoint = client.endpoint();
        let target_port = self.target.port.unwrap_or(443);

        if hosts_match(&self.target.ip, &endpoint.connect_host) && target_port == endpoint.port {
            return Ok(());
        }

        Err(NvFwUpdError::InvalidResponse {
            context: "hosted NVUE target validation",
            message: format!(
                "target {}:{target_port} does not match NVUE client {}:{}",
                self.target.ip, endpoint.connect_host, endpoint.port
            ),
        })
    }

    fn build_bmc_access(&self) -> Result<BmcAccess> {
        if is_switch(self.target.server_type) {
            return Err(NvFwUpdError::Unsupported(
                "BMC access construction for an NVUE switch target",
            ));
        }

        let mut client_builder =
            reqwest::Client::builder().timeout(Duration::from_secs(self.timeout_secs()));

        if !self.target.verify_tls {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        let client = client_builder
            .build()
            .map_err(|e| NvFwUpdError::Transport {
                operation: "build HTTP client",
                message: e.to_string(),
            })?;

        let ip = bracket_ipv6(&self.target.ip);
        let port = self.target.port.map(|p| p.to_string()).unwrap_or_default();

        let base_url = if port.is_empty() {
            format!("https://{ip}")
        } else {
            format!("https://{ip}:{port}")
        };

        Ok(BmcAccess {
            ip,
            user: self.target.username.clone(),
            password: self.target.password.clone(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port,
            servertype: server_type_arg(self.target.server_type).to_string(),
            base_url,
            transport_type: "https".to_string(),
            access_type: self.access_type(),
            ssh_known_hosts: self.target.ssh_known_hosts.clone(),
            ssh_host_key_mode: self.target.ssh_host_key_mode.as_target_arg().to_string(),
            client,
        })
    }

    fn build_nvue_access(&self) -> Result<BmcAccess> {
        if !is_switch(self.target.server_type) {
            return Err(NvFwUpdError::Unsupported(
                "NVUE access for a non-switch target",
            ));
        }

        let mut client_builder =
            reqwest::Client::builder().timeout(Duration::from_secs(DEFAULT_SWITCH_TIMEOUT_SECS));

        if !self.target.verify_tls {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        let client = client_builder
            .build()
            .map_err(|e| NvFwUpdError::Transport {
                operation: "build NVUE HTTP client",
                message: e.to_string(),
            })?;

        let ip = bracket_ipv6(&self.target.ip);
        let port = self.target.port.map(|p| p.to_string()).unwrap_or_default();

        let base_url = if port.is_empty() {
            format!("https://{ip}")
        } else {
            format!("https://{ip}:{port}")
        };

        Ok(BmcAccess {
            ip,
            user: self.target.username.clone(),
            password: self.target.password.clone(),
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port,
            servertype: server_type_arg(self.target.server_type).to_string(),
            base_url,
            transport_type: "https".to_string(),
            access_type: AccessType::NVSwitch,
            ssh_known_hosts: self.target.ssh_known_hosts.clone(),
            ssh_host_key_mode: self.target.ssh_host_key_mode.as_target_arg().to_string(),
            client,
        })
    }

    fn access_type(&self) -> AccessType {
        if is_switch(self.target.server_type) {
            AccessType::NVSwitch
        } else {
            AccessType::Login
        }
    }

    fn timeout_secs(&self) -> u64 {
        if is_switch(self.target.server_type) {
            DEFAULT_SWITCH_TIMEOUT_SECS
        } else {
            DEFAULT_REDFISH_TIMEOUT_SECS
        }
    }
}

#[async_trait::async_trait]
impl<'a> PkgParser for WorkflowPkgParser<'a> {
    async fn parse_pkg(&mut self, pkg_path: &str) -> (bool, String) {
        let (status, msg) = self.inner.parse_pkg(pkg_path, None).await;
        if status {
            self.last_parse_failure = None;
        } else if self.allow_unparsed_package {
            tracing::warn!(
                package = %pkg_path,
                error = %msg,
                "Firmware package could not be parsed as PLDM/tar; continuing with raw upload"
            );
            self.last_parse_failure = None;
            return (true, String::new());
        } else {
            self.last_parse_failure = Some((pkg_path.to_string(), msg.clone()));
        }
        (status, msg)
    }

    async fn get_unpack_file_dict(&mut self, pkg_path: &str) {
        self.inner.prepare_unpack_file_dict(pkg_path).await;
    }

    fn unpack_file_ap_dict(&self) -> &HashMap<String, Vec<String>> {
        self.inner.unpack_file_ap_dict()
    }

    fn apname_version_dict_json(&self) -> Value {
        json!(self.inner.apname_version_dict())
    }

    fn pldm_raw_dict(&self) -> Value {
        self.inner.pldm_raw_dict()
    }
}

#[derive(Clone, Copy)]
enum ExpectedInventoryVersionCheckMode {
    SingleFetch,
    PostActivationRetry,
}

/// Retrieve firmware inventory through the NVFWUPD workflow facade.
pub async fn get_firmware_inventory(target: TargetConfig) -> Result<Vec<FirmwareComponent>> {
    WorkflowContext::new().get_firmware_inventory(target).await
}

async fn get_firmware_inventory_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
) -> Result<Vec<FirmwareComponent>> {
    let rf_target = context.target_builder(&target).build_target()?;
    let trace = TraceFlags::default();
    let (ok, err_code, inventory) = rf_target
        .get_firmware_inventory(trace, None, Some(&rf_target.target_access().model))
        .await;

    if !ok || err_code != 0 {
        return Err(NvFwUpdError::BmcUnreachable {
            target: target.ip,
            message: format!("failed to retrieve firmware inventory, error code {err_code}"),
        });
    }

    Ok(inventory
        .into_iter()
        .map(|(path, details)| firmware_component_from_inventory(path, details))
        .collect())
}

fn expected_inventory_validation_error_from_present(
    phase: &'static str,
    expected_inventory: &[String],
    present: &[String],
) -> Option<NvFwUpdError> {
    let missing =
        expected_inventory::missing_expected_ap_names_from_present(expected_inventory, present);
    if missing.is_empty() {
        return None;
    }

    Some(NvFwUpdError::InvalidResponse {
        context: "expected firmware inventory",
        message: expected_inventory::missing_expected_inventory_message(phase, &missing, present),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedInventoryChassisLink {
    ap_name: String,
    chassis_uri: String,
}

fn normalized_inventory_ap_name(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_ascii_lowercase()
}

fn path_leaf(raw: &str) -> &str {
    raw.trim()
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(raw)
}

fn redfish_path(raw: &str) -> &str {
    let trimmed = raw.trim().trim_end_matches('/');
    trimmed
        .find("/redfish/v1/")
        .map(|index| &trimmed[index..])
        .unwrap_or(trimmed)
}

fn normalized_chassis_uri(raw: &str) -> Option<String> {
    let path = redfish_path(raw);
    path.strip_prefix("/redfish/v1/Chassis/")
        .filter(|leaf| !leaf.is_empty())
        .map(|_| path.to_string())
}

fn inventory_entry_matches_expected_ap(path: &str, details: &Value, expected_ap: &str) -> bool {
    let expected = normalized_inventory_ap_name(expected_ap);
    if expected.is_empty() {
        return false;
    }

    let mut aliases = vec![
        normalized_inventory_ap_name(path),
        normalized_inventory_ap_name(path_leaf(path)),
    ];

    for field in ["Id", "Name", "@odata.id"] {
        if let Some(value) = details.get(field).and_then(Value::as_str) {
            aliases.push(normalized_inventory_ap_name(value));
            aliases.push(normalized_inventory_ap_name(path_leaf(value)));
        }
    }

    aliases.into_iter().any(|alias| alias == expected)
}

fn related_chassis_uris(details: &Value) -> BTreeSet<String> {
    let mut uris = BTreeSet::new();

    if let Some(items) = details.get("RelatedItem").and_then(Value::as_array) {
        for item in items {
            if let Some(uri) = item
                .get("@odata.id")
                .and_then(Value::as_str)
                .and_then(normalized_chassis_uri)
            {
                uris.insert(uri);
            }
        }
    } else if let Some(uri) = details
        .get("RelatedItem")
        .and_then(|item| item.get("@odata.id"))
        .and_then(Value::as_str)
        .and_then(normalized_chassis_uri)
    {
        uris.insert(uri);
    }

    uris
}

fn expected_inventory_chassis_links(
    expected_inventory: &[String],
    inventory: &Map<String, Value>,
) -> Vec<ExpectedInventoryChassisLink> {
    let mut links = BTreeSet::new();

    for expected_ap in expected_inventory {
        for (path, details) in inventory {
            if !inventory_entry_matches_expected_ap(path, details, expected_ap) {
                continue;
            }

            for chassis_uri in related_chassis_uris(details) {
                links.insert(ExpectedInventoryChassisLink {
                    ap_name: expected_ap.clone(),
                    chassis_uri,
                });
            }
        }
    }

    links.into_iter().collect()
}

fn expected_inventory_chassis_validation_error(
    phase: &'static str,
    expected_links: &[ExpectedInventoryChassisLink],
    chassis_members: &[String],
) -> Option<NvFwUpdError> {
    let present = chassis_members
        .iter()
        .filter_map(|member| normalized_chassis_uri(member))
        .collect::<BTreeSet<_>>();

    let missing = expected_links
        .iter()
        .filter(|link| !present.contains(&link.chassis_uri))
        .map(|link| format!("{} -> {}", link.ap_name, link.chassis_uri))
        .collect::<Vec<_>>();

    if missing.is_empty() {
        return None;
    }

    Some(NvFwUpdError::InvalidResponse {
        context: "expected firmware inventory chassis RelatedItem",
        message: format!(
            "{phase}: expected firmware inventory RelatedItem Chassis pages were not present in /redfish/v1/Chassis: {}",
            missing.join(", ")
        ),
    })
}

async fn fetch_firmware_inventory_map(
    rf_target: &WorkflowTarget,
    target_ip: &str,
    json_output: Option<&mut Value>,
) -> Result<Map<String, Value>> {
    let trace = TraceFlags::default();
    let (ok, err_code, inventory) = rf_target
        .get_firmware_inventory(trace, json_output, Some(&rf_target.target_access().model))
        .await;
    if !ok || err_code != 0 {
        return Err(NvFwUpdError::BmcUnreachable {
            target: target_ip.to_string(),
            message: format!("failed to retrieve firmware inventory, error code {err_code}"),
        });
    }

    Ok(inventory)
}

async fn validate_expected_inventory_related_chassis_once(
    rf_target: &WorkflowTarget,
    expected_inventory: &[String],
    inventory: &Map<String, Value>,
    phase: &'static str,
) -> Result<()> {
    if expected_inventory.is_empty()
        || rf_target.target_access().access_type == AccessType::NVSwitch
    {
        return Ok(());
    }

    let expected_links = expected_inventory_chassis_links(expected_inventory, inventory);
    if expected_links.is_empty() {
        return Ok(());
    }

    let chassis_members = rf_target
        .target_access()
        .get_chassis_members(TraceFlags::default())
        .await;
    if let Some(error) =
        expected_inventory_chassis_validation_error(phase, &expected_links, &chassis_members)
    {
        return Err(error);
    }

    Ok(())
}

async fn fetch_expected_inventory_ap_names(
    rf_target: &WorkflowTarget,
    target_ip: &str,
    json_output: Option<&mut Value>,
) -> Result<Vec<String>> {
    let trace = TraceFlags::default();
    let (ok, err_code, present) = rf_target
        .get_expected_inventory_ap_names(trace, json_output)
        .await;
    if !ok || err_code != 0 {
        return Err(NvFwUpdError::BmcUnreachable {
            target: target_ip.to_string(),
            message: format!(
                "failed to retrieve firmware inventory collection, error code {err_code}"
            ),
        });
    }

    Ok(present)
}

async fn validate_expected_inventory_once(
    rf_target: &WorkflowTarget,
    target_ip: &str,
    expected_inventory: Option<&[String]>,
    phase: &'static str,
    json_output: Option<&mut Value>,
) -> Result<()> {
    let Some(expected_inventory) = expected_inventory.filter(|expected| !expected.is_empty())
    else {
        return Ok(());
    };

    let present = fetch_expected_inventory_ap_names(rf_target, target_ip, json_output).await?;
    if let Some(error) =
        expected_inventory_validation_error_from_present(phase, expected_inventory, &present)
    {
        return Err(error);
    }

    Ok(())
}

async fn fetch_inventory_with_expected_check_once(
    rf_target: &WorkflowTarget,
    target_ip: &str,
    expected_inventory: Option<&[String]>,
    phase: &'static str,
    json_output: Option<&mut Value>,
) -> Result<Map<String, Value>> {
    if let Some(expected_inventory) = expected_inventory.filter(|expected| !expected.is_empty()) {
        validate_expected_inventory_once(
            rf_target,
            target_ip,
            Some(expected_inventory),
            phase,
            json_output,
        )
        .await?;
        return fetch_firmware_inventory_map(rf_target, target_ip, None).await;
    }

    fetch_firmware_inventory_map(rf_target, target_ip, json_output).await
}

async fn fetch_post_activation_inventory(
    rf_target: &WorkflowTarget,
    target_ip: &str,
    expected_inventory: Option<&[String]>,
) -> Result<Map<String, Value>> {
    let Some(expected_inventory) = expected_inventory.filter(|expected| !expected.is_empty())
    else {
        return fetch_firmware_inventory_map(rf_target, target_ip, None).await;
    };
    let mut last_failure = None;
    for attempt in 0..=EXPECTED_INVENTORY_POST_ACTIVATION_RETRIES {
        match fetch_expected_inventory_ap_names(rf_target, target_ip, None).await {
            Ok(present) => {
                if let Some(error) = expected_inventory_validation_error_from_present(
                    "post-activation expected inventory check",
                    expected_inventory,
                    &present,
                ) {
                    last_failure = Some(error);
                } else {
                    match fetch_firmware_inventory_map(rf_target, target_ip, None).await {
                        Ok(inventory) => {
                            let full_inventory_present =
                                expected_inventory::present_ap_names(&inventory);
                            if let Some(error) = expected_inventory_validation_error_from_present(
                                "post-activation expected inventory check",
                                expected_inventory,
                                &full_inventory_present,
                            ) {
                                last_failure = Some(error);
                            } else if let Err(error) =
                                validate_expected_inventory_related_chassis_once(
                                    rf_target,
                                    expected_inventory,
                                    &inventory,
                                    "post-activation expected inventory check",
                                )
                                .await
                            {
                                last_failure = Some(error);
                            } else {
                                return Ok(inventory);
                            }
                        }
                        Err(error) => {
                            last_failure = Some(error);
                        }
                    }
                }
            }
            Err(error) => {
                last_failure = Some(error);
            }
        }

        if attempt < EXPECTED_INVENTORY_POST_ACTIVATION_RETRIES {
            tracing::warn!(
                attempt = attempt + 1,
                retries = EXPECTED_INVENTORY_POST_ACTIVATION_RETRIES,
                "expected firmware inventory was not ready after activation; retrying"
            );
            tokio::time::sleep(Duration::from_secs(
                EXPECTED_INVENTORY_POST_ACTIVATION_RETRY_INTERVAL_SECS,
            ))
            .await;
        }
    }

    Err(
        last_failure.unwrap_or_else(|| NvFwUpdError::InvalidResponse {
            context: "expected firmware inventory",
            message: "post-activation expected inventory check failed".to_string(),
        }),
    )
}

/// Compare installed firmware versions against package metadata.
pub async fn verify_firmware_versions(
    target: TargetConfig,
    request: FirmwareVersionCheckRequest,
) -> Result<FirmwareVersionCheckSummary> {
    WorkflowContext::new()
        .verify_firmware_versions(target, request)
        .await
}

/// Compare installed firmware versions and validate expected AP inventory first.
pub async fn verify_firmware_versions_with_expected_inventory(
    target: TargetConfig,
    request: FirmwareVersionCheckRequest,
    expected_inventory: Option<Vec<String>>,
) -> Result<FirmwareVersionCheckSummary> {
    WorkflowContext::new()
        .verify_firmware_versions_with_expected_inventory(target, request, expected_inventory)
        .await
}

/// Compare installed firmware versions after activation, retrying expected inventory recovery.
pub async fn verify_firmware_versions_after_activation_with_expected_inventory(
    target: TargetConfig,
    request: FirmwareVersionCheckRequest,
    expected_inventory: Option<Vec<String>>,
) -> Result<FirmwareVersionCheckSummary> {
    WorkflowContext::new()
        .verify_firmware_versions_after_activation_with_expected_inventory(
            target,
            request,
            expected_inventory,
        )
        .await
}

/// Compare only the firmware targets applied by the caller.
pub async fn verify_firmware_target_versions(
    target: TargetConfig,
    targets: Vec<FirmwareVersionCheckTarget>,
) -> Result<FirmwareVersionCheckSummary> {
    WorkflowContext::new()
        .verify_firmware_target_versions(target, targets)
        .await
}

/// Compare explicit update targets and validate expected AP inventory first.
pub async fn verify_firmware_target_versions_with_expected_inventory(
    target: TargetConfig,
    targets: Vec<FirmwareVersionCheckTarget>,
    expected_inventory: Option<Vec<String>>,
) -> Result<FirmwareVersionCheckSummary> {
    WorkflowContext::new()
        .verify_firmware_target_versions_with_expected_inventory(
            target,
            targets,
            expected_inventory,
        )
        .await
}

/// Compare explicit update targets after activation, retrying expected inventory recovery.
pub async fn verify_firmware_target_versions_after_activation_with_expected_inventory(
    target: TargetConfig,
    targets: Vec<FirmwareVersionCheckTarget>,
    expected_inventory: Option<Vec<String>>,
) -> Result<FirmwareVersionCheckSummary> {
    WorkflowContext::new()
        .verify_firmware_target_versions_after_activation_with_expected_inventory(
            target,
            targets,
            expected_inventory,
        )
        .await
}

async fn verify_firmware_versions_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
    request: FirmwareVersionCheckRequest,
) -> Result<FirmwareVersionCheckSummary> {
    verify_firmware_versions_with_expected_inventory_context(
        context,
        target,
        request,
        None,
        ExpectedInventoryVersionCheckMode::SingleFetch,
    )
    .await
}

async fn verify_firmware_versions_with_expected_inventory_context(
    context: &WorkflowContext,
    target: TargetConfig,
    request: FirmwareVersionCheckRequest,
    expected_inventory: Option<&[String]>,
    expected_inventory_mode: ExpectedInventoryVersionCheckMode,
) -> Result<FirmwareVersionCheckSummary> {
    let targets = request
        .firmware_files
        .into_iter()
        .map(|firmware_file| FirmwareVersionCheckTarget {
            component: String::new(),
            firmware_file,
        })
        .collect();
    verify_firmware_target_versions_with_expected_inventory_context(
        context,
        target,
        targets,
        expected_inventory,
        expected_inventory_mode,
    )
    .await
}

async fn verify_firmware_target_versions_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
    targets: Vec<FirmwareVersionCheckTarget>,
) -> Result<FirmwareVersionCheckSummary> {
    verify_firmware_target_versions_with_expected_inventory_context(
        context,
        target,
        targets,
        None,
        ExpectedInventoryVersionCheckMode::SingleFetch,
    )
    .await
}

async fn verify_firmware_target_versions_with_expected_inventory_context(
    context: &WorkflowContext,
    target: TargetConfig,
    targets: Vec<FirmwareVersionCheckTarget>,
    expected_inventory: Option<&[String]>,
    expected_inventory_mode: ExpectedInventoryVersionCheckMode,
) -> Result<FirmwareVersionCheckSummary> {
    let target_groups = group_firmware_version_check_targets(targets);
    let mut firmware_files = Vec::new();
    for (firmware_file, _) in &target_groups {
        if !firmware_files.contains(firmware_file) {
            firmware_files.push(firmware_file.clone());
        }
    }
    if firmware_files.is_empty() {
        return Err(NvFwUpdError::PackageParse {
            path: String::new(),
            message: "firmware version check requires at least one firmware file".to_string(),
        });
    }

    let mut parse_errors = Vec::new();
    let mut checks = Vec::new();
    for (firmware_file, grouped_targets) in target_groups {
        let scope = firmware_version_check_scope(target.server_type, &grouped_targets);
        let mut pkg_parser = pldm::get_pkg_parser(&firmware_file, false, false, None).await;
        let (status, message) = pkg_parser.parse_pkg(&firmware_file, None).await;
        if !status {
            pkg_parser.remove_files().await;
            if server_type_allows_unparsed_package_update(target.server_type) {
                let parse_error = json!({
                    "firmware_file": &firmware_file,
                    "message": message,
                });
                if !parse_errors.contains(&parse_error) {
                    parse_errors.push(parse_error);
                }
                checks.push((scope, json!({}), false));
                continue;
            }

            return Err(NvFwUpdError::PackageParse {
                path: firmware_file,
                message,
            });
        }

        let has_package_versions = !pkg_parser.apname_version_dict().is_empty();
        checks.push((
            scope,
            json!(pkg_parser.apname_version_dict()),
            has_package_versions,
        ));
        pkg_parser.remove_files().await;
    }

    if checks
        .iter()
        .all(|(_, _, has_package_versions)| !has_package_versions)
    {
        return Ok(FirmwareVersionCheckSummary {
            matched: true,
            details: json!({
                "status": "unverified",
                "reason": "no parseable package version metadata was available",
                "packages": firmware_files,
                "package_parse_errors": parse_errors,
                "components": [],
                "mismatches": [],
            }),
        });
    }

    let rf_target = context.target_builder(&target).build_target()?;
    let inventory = match expected_inventory_mode {
        ExpectedInventoryVersionCheckMode::SingleFetch => {
            fetch_inventory_with_expected_check_once(
                &rf_target,
                &target.ip,
                expected_inventory,
                "expected inventory check",
                None,
            )
            .await?
        }
        ExpectedInventoryVersionCheckMode::PostActivationRetry => {
            fetch_post_activation_inventory(&rf_target, &target.ip, expected_inventory).await?
        }
    };

    let mut components = Vec::new();
    for (scope, package_versions, _) in checks {
        let replace_unverified = scope.replaces_concrete_with_unverified();
        let mut scoped_inventory = inventory.clone();
        let missing_components =
            scope_firmware_inventory(target.server_type, &scope, &mut scoped_inventory);
        let checked_components = compare_inventory_to_package_versions(
            rf_target.as_rf_target(),
            &scoped_inventory,
            &package_versions,
        )
        .await;
        merge_firmware_version_components(&mut components, checked_components, replace_unverified);
        merge_firmware_version_components(&mut components, missing_components, true);
    }

    let mismatches: Vec<Value> = components
        .iter()
        .filter(|component| {
            component
                .get("status")
                .and_then(Value::as_str)
                .map(|status| status == "mismatched")
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    let comparable_count = components
        .iter()
        .filter(|component| {
            component
                .get("status")
                .and_then(Value::as_str)
                .map(|status| status != "unverified")
                .unwrap_or(false)
        })
        .count();
    let matched = mismatches.is_empty();
    let status = if !matched {
        "mismatch"
    } else if comparable_count == 0 {
        "unverified"
    } else {
        "matched"
    };

    Ok(FirmwareVersionCheckSummary {
        matched,
        details: json!({
            "status": status,
            "packages": firmware_files,
            "package_parse_errors": parse_errors,
            "comparable_components": comparable_count,
            "components": components,
            "mismatches": mismatches,
        }),
    })
}

/// Start or run a firmware update through the NVFWUPD workflow facade.
pub async fn update_firmware(
    target: TargetConfig,
    request: FirmwareUpdateRequest,
) -> Result<FirmwareUpdateOutcome> {
    WorkflowContext::new()
        .update_firmware(target, request)
        .await
}

/// Start or run a firmware update after validating expected AP inventory.
pub async fn update_firmware_with_expected_inventory(
    target: TargetConfig,
    request: FirmwareUpdateRequest,
    expected_inventory: Option<Vec<String>>,
) -> Result<FirmwareUpdateOutcome> {
    WorkflowContext::new()
        .update_firmware_with_expected_inventory(target, request, expected_inventory)
        .await
}

async fn update_firmware_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
    request: FirmwareUpdateRequest,
    expected_inventory: Option<&[String]>,
) -> Result<FirmwareUpdateOutcome> {
    if request
        .cancellation
        .as_ref()
        .map(|token| token.is_cancelled())
        .unwrap_or(false)
    {
        return Err(NvFwUpdError::Message(
            "firmware update cancelled".to_string(),
        ));
    }

    if request.firmware_file.is_empty() {
        return Err(NvFwUpdError::PackageParse {
            path: request.firmware_file,
            message: "firmware update requires a firmware_file".to_string(),
        });
    }

    let mut rf_target = context.target_builder(&target).build_target()?;
    let mut json_output = json!({"Error": [], "Error Code": 0, "Output": []});
    validate_expected_inventory_once(
        &rf_target,
        &target.ip,
        expected_inventory,
        "pre-update expected inventory check",
        Some(&mut json_output),
    )
    .await?;
    rf_target
        .check_update_preconditions(
            UpdatePreconditionMode::Wait {
                timeout: Duration::from_secs(BACKGROUND_COPY_RMS_TIMEOUT_SECS),
                interval: Duration::from_secs(BACKGROUND_COPY_RMS_POLL_INTERVAL_SECS),
            },
            Some(&mut json_output),
        )
        .await
        .map_err(|message| NvFwUpdError::TaskFailed {
            task_id: None,
            message,
        })?;

    let recipe_list = vec![request.firmware_file.clone()];
    let mut pkg_parser =
        pldm::get_pkg_parser(&request.firmware_file, false, false, Some(&json_output)).await;

    let cmd_args = update_cmd_args(target.server_type, &request);
    let nvfwupd_owned_monitoring = owns_update_monitoring(target.server_type);
    let monitoring_timeout_secs = target_monitoring_timeout_secs(target.server_type);
    let (status, task_ids, parse_failure) = {
        let mut adapter = WorkflowPkgParser::new(
            pkg_parser.as_mut(),
            server_type_allows_unparsed_package_update(target.server_type),
        );
        let (status, task_ids) = rf_target
            .start_update_monitor(
                &recipe_list,
                &mut adapter,
                &cmd_args,
                monitoring_timeout_secs,
                !nvfwupd_owned_monitoring,
                Some(&mut json_output),
                0,
                false,
                None,
            )
            .await;
        (status, task_ids, adapter.take_last_parse_failure())
    };
    pkg_parser.remove_files().await;

    if status != 0 {
        return Err(update_failure_error(
            status,
            &task_ids,
            parse_failure,
            &json_output,
        ));
    }

    Ok(update_success_outcome(
        target.server_type,
        task_ids,
        json_output,
    ))
}

/// Query a firmware task through the NVFWUPD workflow facade.
pub async fn get_task_status(target: TargetConfig, task_id: &str) -> Result<TaskStatus> {
    WorkflowContext::new()
        .get_task_status(target, task_id)
        .await
}

async fn get_task_status_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
    task_id: &str,
) -> Result<TaskStatus> {
    if task_id.is_empty() {
        return Err(NvFwUpdError::InvalidResponse {
            context: "task status request",
            message: "task status requires a task_id".to_string(),
        });
    }

    let rf_target = context.target_builder(&target).build_target()?;
    let mut json_output = json!({"Error": [], "Error Code": 0, "Output": []});
    let (ok, details) = rf_target
        .query_job_status(task_id, Some(&mut json_output))
        .await;
    if !ok {
        return Err(NvFwUpdError::TaskFailed {
            task_id: Some(task_id.to_string()),
            message: "failed to retrieve task status".to_string(),
        });
    }

    Ok(task_status_from_details_for_server(
        target.server_type,
        task_id,
        details,
    ))
}

/// Run a firmware activation request through the NVFWUPD workflow facade.
pub async fn activate_firmware(
    target: TargetConfig,
    request: ActivationRequest,
) -> Result<ActivationSummary> {
    WorkflowContext::new()
        .activate_firmware(target, request)
        .await
}

async fn activate_firmware_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
    request: ActivationRequest,
) -> Result<ActivationSummary> {
    if request
        .cancellation
        .as_ref()
        .map(|token| token.is_cancelled())
        .unwrap_or(false)
    {
        return Err(NvFwUpdError::Message("activation cancelled".to_string()));
    }

    match request.mode {
        ActivationMode::FullGb200Compute => activate_full_gb200_compute(target).await,
        ActivationMode::SwitchPowerCycle => activate_switch_power_cycle(context, target).await,
        ActivationMode::PowerShelfReset { force } => activate_powershelf_reset(target, force).await,
        mode => {
            let command = activation_command_for_mode(target.server_type, &mode)?;
            let mut rf_target = context.target_builder(&target).build_target()?;

            run_workflow_activation_command(&mut rf_target, command).await?;

            Ok(ActivationSummary {
                message: format!("activation command {command} completed"),
                details: json!({"command": command}),
            })
        }
    }
}

async fn activate_switch_power_cycle(
    context: &WorkflowContext,
    target: TargetConfig,
) -> Result<ActivationSummary> {
    if !is_switch(target.server_type) {
        return Err(NvFwUpdError::Unsupported("switch power-cycle activation"));
    }

    let target = context.target_builder(&target).build_target()?;

    activate_switch_power_cycle_with_target(target).await
}

async fn activate_switch_power_cycle_with_target(
    target: WorkflowTarget,
) -> Result<ActivationSummary> {
    let command = "NVUE_PWR_CYCLE";
    let summary = target.run_switch_power_cycle().await?;
    let action_id = summary.action_id.clone();
    let action = summary.redacted_details();

    let recovery_attempts = wait_for_workflow_target_recovery(
        &target,
        SWITCH_ACTIVATION_RECOVERY_PATH,
        "wait for switch after activation",
        SWITCH_ACTIVATION_RECOVERY_ATTEMPTS,
        SWITCH_ACTIVATION_RECOVERY_INTERVAL_SECS,
    )
    .await?;

    Ok(ActivationSummary {
        message: format!("activation command {command} completed"),
        details: json!({
            "command": command,
            "action_id": action_id,
            "action": action,
            "recovery": {
                "endpoint": SWITCH_ACTIVATION_RECOVERY_PATH,
                "attempts": recovery_attempts,
            },
        }),
    })
}

async fn wait_for_workflow_target_recovery(
    target: &WorkflowTarget,
    path: &'static str,
    operation: &'static str,
    attempts: u32,
    interval_secs: u64,
) -> Result<u32> {
    let mut observed_unreachable = false;

    for attempt in 1..=attempts {
        let reachable = target
            .recovery_probe(path, ACTIVATION_RECOVERY_PROBE_TIMEOUT_SECS)
            .await;

        if reachable && observed_unreachable {
            return Ok(attempt);
        }

        if !reachable {
            observed_unreachable = true;
        }

        if attempt < attempts {
            tokio::time::sleep(Duration::from_secs(interval_secs)).await;
        }
    }

    Err(NvFwUpdError::Timeout {
        operation,
        seconds: recovery_wait_budget_secs(attempts, interval_secs),
    })
}

async fn activate_powershelf_reset(target: TargetConfig, force: bool) -> Result<ActivationSummary> {
    if target.server_type != ServerType::PowerShelf {
        return Err(NvFwUpdError::Unsupported("powershelf activation"));
    }

    let command = if force {
        "RF_PWRSHELF_RESET_FORCE"
    } else {
        "RF_PWRSHELF_RESET"
    };
    let access = build_bmc_access(&target)?;
    activate_powershelf_reset_with_access(access, command).await
}

async fn activate_powershelf_reset_with_access(
    access: BmcAccess,
    command: &'static str,
) -> Result<ActivationSummary> {
    let mut target = PowerShelfRFTarget::new(access.clone(), None);
    run_activation_command(&mut target, command).await?;
    let (recovery_attempts, observed_by) = if let Some(attempts) = target.reset_recovery_attempts()
    {
        (attempts, "activation_command")
    } else {
        (
            wait_for_activation_recovery(
                &access,
                POWERSHELF_ACTIVATION_RECOVERY_PATH,
                "wait for PowerShelf after activation",
                POWERSHELF_ACTIVATION_RECOVERY_ATTEMPTS,
                POWERSHELF_ACTIVATION_RECOVERY_INTERVAL_SECS,
            )
            .await?,
            "workflow",
        )
    };

    Ok(ActivationSummary {
        message: format!("activation command {command} completed"),
        details: json!({
            "command": command,
            "recovery": {
                "endpoint": POWERSHELF_ACTIVATION_RECOVERY_PATH,
                "attempts": recovery_attempts,
                "observed_by": observed_by,
            },
        }),
    })
}

fn map_switch_power_cycle_error(error: SwitchPowerCycleError) -> NvFwUpdError {
    if matches!(error, SwitchPowerCycleError::MissingActionId { .. }) {
        return NvFwUpdError::InvalidResponse {
            context: "NVUE power-cycle activation",
            message: error.message(),
        };
    }

    NvFwUpdError::TaskFailed {
        task_id: error.action_id().map(ToOwned::to_owned),
        message: error.message(),
    }
}

async fn activate_full_gb200_compute(target: TargetConfig) -> Result<ActivationSummary> {
    if !matches!(
        target.server_type,
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72
    ) {
        return Err(NvFwUpdError::Unsupported(
            "full GB200 compute activation workflow",
        ));
    }

    let mut rf_target = build_target(&target)?;
    run_compute_activation_command(rf_target.as_rf_target_mut(), "RF_PWR_OFF").await?;
    tokio::time::sleep(Duration::from_secs(COMPUTE_ACTIVATION_POWER_OFF_DELAY_SECS)).await;
    run_compute_activation_command(rf_target.as_rf_target_mut(), "RF_AUX_PWR_CYCLE").await?;
    wait_for_compute_bmc(&target).await?;
    run_compute_activation_command(rf_target.as_rf_target_mut(), "RF_PWR_ON").await?;
    wait_for_compute_power_on(rf_target.as_rf_target_mut()).await?;

    Ok(ActivationSummary {
        message: "full GB200 compute activation completed".to_string(),
        details: json!({
            "commands": ["RF_PWR_OFF", "RF_AUX_PWR_CYCLE", "RF_PWR_ON"],
            "bmc_wait_seconds": 2 * COMPUTE_ACTIVATION_BMC_WAIT_ATTEMPTS
                * COMPUTE_ACTIVATION_BMC_WAIT_INTERVAL_SECS,
            "power_on_resend_interval_seconds": COMPUTE_ACTIVATION_POWER_ON_RESEND_INTERVAL_SECS,
            "verified_power_state": "On",
        }),
    })
}

async fn wait_for_activation_recovery(
    access: &BmcAccess,
    path: &'static str,
    operation: &'static str,
    attempts: u32,
    interval_secs: u64,
) -> Result<u32> {
    let mut observed_unreachable = false;

    for attempt in 1..=attempts {
        let (ok, _) = access
            .dispatch_request_full(
                "GET",
                path,
                None,
                None,
                ACTIVATION_RECOVERY_PROBE_TIMEOUT_SECS,
                true,
                None,
            )
            .await;

        if ok && observed_unreachable {
            return Ok(attempt);
        }
        if !ok {
            observed_unreachable = true;
        }

        if attempt < attempts {
            tokio::time::sleep(Duration::from_secs(interval_secs)).await;
        }
    }

    Err(NvFwUpdError::Timeout {
        operation,
        seconds: recovery_wait_budget_secs(attempts, interval_secs),
    })
}

fn recovery_wait_budget_secs(attempts: u32, interval_secs: u64) -> u64 {
    attempts as u64 * ACTIVATION_RECOVERY_PROBE_TIMEOUT_SECS
        + attempts.saturating_sub(1) as u64 * interval_secs
}

async fn run_activation_command(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    command: &'static str,
) -> Result<()> {
    let status = rf_target
        .run_oob_activation(&activation_cmd_args(command))
        .await;

    activation_status_result(status, command)
}

async fn run_workflow_activation_command(
    rf_target: &mut WorkflowTarget,
    command: &'static str,
) -> Result<()> {
    let status = rf_target
        .run_oob_activation(&activation_cmd_args(command))
        .await;

    activation_status_result(status, command)
}

fn activation_cmd_args(command: &'static str) -> CmdArgs {
    CmdArgs {
        cmd: command.to_string(),
        background: false,
        details: false,
        staged_update: false,
        staged_activate_update: false,
        quiet: true,
        special: None,
        oem_parameters: None,
    }
}

fn activation_status_result(status: i32, command: &'static str) -> Result<()> {
    if status != 0 {
        return Err(NvFwUpdError::TaskFailed {
            task_id: None,
            message: format!("activation command {command} failed with status {status}"),
        });
    }

    Ok(())
}

async fn run_compute_activation_command(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    command: &'static str,
) -> Result<()> {
    run_activation_command_with_retry(
        rf_target,
        command,
        COMPUTE_ACTIVATION_COMMAND_RETRIES,
        COMPUTE_ACTIVATION_COMMAND_RETRY_DELAY_SECS,
    )
    .await
}

async fn run_activation_command_with_retry(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    command: &'static str,
    retries: u32,
    retry_delay_secs: u64,
) -> Result<()> {
    let mut retries_used = 0;
    loop {
        match run_activation_command(rf_target, command).await {
            Ok(()) => return Ok(()),
            Err(error) if retries_used < retries => {
                retries_used += 1;
                tracing::warn!(
                    command,
                    retry = retries_used,
                    retries,
                    retry_delay_secs,
                    error = %error,
                    "activation command failed; retrying"
                );
                tokio::time::sleep(Duration::from_secs(retry_delay_secs)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn wait_for_compute_bmc(target: &TargetConfig) -> Result<()> {
    let access = build_bmc_access(target)?;
    for _ in 0..COMPUTE_ACTIVATION_BMC_WAIT_ATTEMPTS {
        tokio::time::sleep(Duration::from_secs(
            COMPUTE_ACTIVATION_BMC_WAIT_INTERVAL_SECS,
        ))
        .await;
        let (ok, _) = access
            .dispatch_request_full("GET", "/redfish/v1", None, None, 30, true, None)
            .await;
        if ok {
            return Ok(());
        }
    }

    Err(NvFwUpdError::Timeout {
        operation: "wait for GB200 compute BMC after activation",
        seconds: COMPUTE_ACTIVATION_BMC_WAIT_ATTEMPTS * COMPUTE_ACTIVATION_BMC_WAIT_INTERVAL_SECS,
    })
}

fn preferred_compute_system_uri(system_uris: &[String]) -> Option<&str> {
    system_uris
        .iter()
        .find(|uri| uri.trim_end_matches('/').rsplit('/').next() == Some("System_0"))
        .or_else(|| system_uris.first())
        .map(String::as_str)
}

#[derive(Debug, Deserialize)]
struct ComputeSystemPowerState {
    #[serde(rename = "PowerState")]
    power_state: String,
}

async fn query_compute_power_state(access: &BmcAccess) -> Option<String> {
    let systems = access.get_systems_members(TraceFlags::default()).await;
    let system_uri = preferred_compute_system_uri(&systems)?;
    let (ok, response) = access
        .dispatch_request_full("GET", system_uri, None, None, 30, true, None)
        .await;
    if !ok {
        return None;
    }

    serde_json::from_value::<ComputeSystemPowerState>(response)
        .ok()
        .map(|system| system.power_state)
}

async fn wait_for_compute_power_on(rf_target: &mut (dyn RFTarget + Send + Sync)) -> Result<()> {
    let resend_poll_count = (COMPUTE_ACTIVATION_POWER_ON_RESEND_INTERVAL_SECS
        / COMPUTE_ACTIVATION_BMC_WAIT_INTERVAL_SECS)
        .max(1);

    wait_for_compute_power_on_with_policy(
        rf_target,
        COMPUTE_ACTIVATION_BMC_WAIT_ATTEMPTS,
        COMPUTE_ACTIVATION_BMC_WAIT_INTERVAL_SECS,
        resend_poll_count,
    )
    .await
}

async fn wait_for_compute_power_on_with_policy(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    attempts: u64,
    poll_interval_secs: u64,
    resend_poll_count: u64,
) -> Result<()> {
    let mut last_power_state = None;
    let mut polls_since_power_on = 0_u64;
    let resend_poll_count = resend_poll_count.max(1);

    for attempt in 1..=attempts {
        tokio::time::sleep(Duration::from_secs(poll_interval_secs)).await;

        polls_since_power_on = polls_since_power_on.saturating_add(1);
        let observed_power_state = query_compute_power_state(rf_target.target_access()).await;
        if observed_power_state
            .as_deref()
            .is_some_and(|state| state.eq_ignore_ascii_case("On"))
        {
            return Ok(());
        }

        let is_off = observed_power_state
            .as_deref()
            .is_some_and(|state| state.eq_ignore_ascii_case("Off"));

        if observed_power_state.is_some() {
            last_power_state = observed_power_state;
        }

        if is_off && attempt < attempts && polls_since_power_on >= resend_poll_count {
            tracing::warn!(
                attempt,
                resend_interval_secs = COMPUTE_ACTIVATION_POWER_ON_RESEND_INTERVAL_SECS,
                "compute system remains Off after accepted power-on; resending RF_PWR_ON"
            );
            if let Err(error) = run_activation_command(rf_target, "RF_PWR_ON").await {
                tracing::warn!(
                    attempt,
                    error = %error,
                    "periodic compute RF_PWR_ON resend failed; continuing power-state polling"
                );
            }
            polls_since_power_on = 0;
        }
    }

    tracing::warn!(
        power_state = last_power_state.as_deref().unwrap_or("unavailable"),
        "compute system did not reach the On power state after activation"
    );
    Err(NvFwUpdError::Timeout {
        operation: "wait for compute system power state On",
        seconds: attempts * poll_interval_secs,
    })
}

fn build_target(target: &TargetConfig) -> Result<WorkflowTarget> {
    WorkflowTargetBuilder::new(target).build_target()
}

fn build_bmc_access(target: &TargetConfig) -> Result<BmcAccess> {
    WorkflowTargetBuilder::new(target).build_bmc_access()
}

fn build_nvue_access(target: &TargetConfig) -> Result<BmcAccess> {
    WorkflowTargetBuilder::new(target).build_nvue_access()
}

fn bracket_ipv6(ip: &str) -> String {
    if ip.contains(':') && !ip.starts_with('[') {
        format!("[{ip}]")
    } else {
        ip.to_string()
    }
}

fn hosts_match(left: &str, right: &str) -> bool {
    let left = left.trim().trim_start_matches('[').trim_end_matches(']');
    let right = right.trim().trim_start_matches('[').trim_end_matches(']');

    match (left.parse::<IpAddr>(), right.parse::<IpAddr>()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left.eq_ignore_ascii_case(right),
    }
}

fn server_type_arg(server_type: ServerType) -> &'static str {
    match server_type {
        ServerType::DGX => "dgx",
        ServerType::DGXRubin => "dgxrubin",
        ServerType::HGX => "hgx",
        ServerType::MGX => "mgx",
        ServerType::GH200 => "gh200",
        ServerType::HGXB100 => "hgxb100",
        ServerType::HGXB300 => "hgxb300",
        ServerType::HGXRubin => "hgxrubin",
        ServerType::GB200 => "gb200",
        ServerType::GB300 => "gb300",
        ServerType::VRNVL72 => "vrnvl72",
        ServerType::GB200Switch => "gb200switch",
        ServerType::GB300Switch => "gb300switch",
        ServerType::VRNVL72Switch => "vrnvl72switch",
        ServerType::PowerShelf => "powershelf",
    }
}

fn is_switch(server_type: ServerType) -> bool {
    matches!(
        server_type,
        ServerType::GB200Switch | ServerType::GB300Switch | ServerType::VRNVL72Switch
    )
}

fn target_monitoring_timeout_secs(server_type: ServerType) -> u64 {
    if is_switch(server_type) {
        DEFAULT_SWITCH_TIMEOUT_SECS
    } else {
        DEFAULT_REDFISH_TIMEOUT_SECS
    }
}

fn server_type_allows_unparsed_package_update(server_type: ServerType) -> bool {
    matches!(
        server_type,
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72
    )
}

fn owns_update_monitoring(server_type: ServerType) -> bool {
    is_switch(server_type)
}

fn group_firmware_version_check_targets(
    targets: Vec<FirmwareVersionCheckTarget>,
) -> Vec<(String, Vec<FirmwareVersionCheckTarget>)> {
    let mut groups: Vec<(String, Vec<FirmwareVersionCheckTarget>)> = Vec::new();
    for target in targets {
        if target.firmware_file.is_empty() {
            continue;
        }
        if let Some((firmware_file, grouped_targets)) = groups.last_mut() {
            if firmware_file == &target.firmware_file {
                grouped_targets.push(target);
                continue;
            }
        }
        groups.push((target.firmware_file.clone(), vec![target]));
    }
    groups
}

#[derive(Debug, PartialEq, Eq)]
enum FirmwareVersionCheckScope {
    Package,
    Components(Vec<String>),
}

impl FirmwareVersionCheckScope {
    fn replaces_concrete_with_unverified(&self) -> bool {
        // Package-wide unverified rows are ambiguous and must not erase concrete evidence.
        matches!(self, Self::Components(_))
    }
}

fn firmware_version_check_scope(
    server_type: ServerType,
    targets: &[FirmwareVersionCheckTarget],
) -> FirmwareVersionCheckScope {
    let mut components = Vec::new();

    for target in targets
        .iter()
        .filter(|target| !target.firmware_file.is_empty())
    {
        if target.component.trim().is_empty() {
            return FirmwareVersionCheckScope::Package;
        }

        let update_targets = component_update_targets(server_type, &target.component);
        if update_targets.is_empty()
            || (!is_switch(server_type)
                && update_targets
                    .iter()
                    .any(|target| !is_firmware_inventory_target(target)))
        {
            return FirmwareVersionCheckScope::Package;
        }

        for update_target in update_targets {
            for component in version_check_components(server_type, update_target) {
                if !components.contains(&component) {
                    components.push(component);
                }
            }
        }
    }

    if components.is_empty() {
        FirmwareVersionCheckScope::Package
    } else {
        FirmwareVersionCheckScope::Components(components)
    }
}

fn version_check_components(server_type: ServerType, update_target: String) -> Vec<String> {
    let cpld_count = match server_type {
        ServerType::GB200Switch => 4,
        ServerType::GB300Switch => 3,
        ServerType::VRNVL72Switch => 2,
        _ => 0,
    };
    if update_target.eq_ignore_ascii_case("cpld1") && cpld_count > 0 {
        return (1..=cpld_count)
            .map(|index| format!("CPLD{index}"))
            .collect();
    }
    vec![update_target]
}

fn is_firmware_inventory_target(target: &str) -> bool {
    target
        .to_ascii_lowercase()
        .contains("/updateservice/firmwareinventory/")
}

fn scope_firmware_inventory(
    server_type: ServerType,
    scope: &FirmwareVersionCheckScope,
    inventory: &mut serde_json::Map<String, Value>,
) -> Vec<Value> {
    let FirmwareVersionCheckScope::Components(targets) = scope else {
        return Vec::new();
    };

    let missing_components = targets
        .iter()
        .filter(|target| {
            !inventory.keys().any(|inventory_path| {
                firmware_inventory_target_matches(server_type, target, inventory_path)
            })
        })
        .map(|target| missing_firmware_component(target))
        .collect();

    inventory.retain(|inventory_path, _| {
        targets
            .iter()
            .any(|target| firmware_inventory_target_matches(server_type, target, inventory_path))
    });

    missing_components
}

fn firmware_inventory_target_matches(
    server_type: ServerType,
    target: &str,
    inventory_path: &str,
) -> bool {
    if is_switch(server_type) {
        let target = normalize_switch_component(target);
        if target == "transceiver" {
            return inventory_path.to_ascii_lowercase().contains("osfp");
        }
        return target == normalize_switch_component(inventory_path);
    }

    target
        .trim_end_matches('/')
        .eq_ignore_ascii_case(inventory_path.trim_end_matches('/'))
}

fn normalize_switch_component(component: &str) -> String {
    let component = component
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(component)
        .to_ascii_lowercase();

    match component.as_str() {
        "cpld" => "cpld1".to_owned(),
        "smr" => "fpga".to_owned(),
        "sbios" => "bios".to_owned(),
        _ => component,
    }
}

fn missing_firmware_component(target: &str) -> Value {
    json!({
        "name": target.trim_end_matches('/').rsplit('/').next().unwrap_or(target),
        "inventory_path": target,
        "system_version": "missing",
        "package_version": "selected target",
        "status": "mismatched",
        "reason": "selected firmware target was not present in inventory",
    })
}

fn merge_firmware_version_components(
    components: &mut Vec<Value>,
    checked: Vec<Value>,
    replace_unverified: bool,
) {
    for component in checked {
        if let Some(existing) = components
            .iter_mut()
            .find(|existing| same_firmware_component(existing, &component))
        {
            if replace_unverified
                || component.get("status").and_then(Value::as_str) != Some("unverified")
                || existing.get("status").and_then(Value::as_str) == Some("unverified")
            {
                *existing = component;
            }
        } else {
            components.push(component);
        }
    }
}

fn same_firmware_component(left: &Value, right: &Value) -> bool {
    let string_field_matches = |field| match (
        left.get(field).and_then(Value::as_str),
        right.get(field).and_then(Value::as_str),
    ) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => false,
    };

    string_field_matches("inventory_path") || string_field_matches("name")
}

async fn compare_inventory_to_package_versions(
    rf_target: &(dyn RFTarget + Send + Sync),
    inventory: &serde_json::Map<String, Value>,
    package_versions: &Value,
) -> Vec<Value> {
    let mut components = Vec::new();

    for (dev_url, val) in inventory {
        let ap_inv_name = if !dev_url.contains("OSFP") {
            dev_url.rsplit('/').next().unwrap_or(dev_url).to_string()
        } else {
            dev_url.clone()
        };
        let ap_name = ap_inv_name.to_lowercase();
        let system_version = val
            .get("Version")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();

        let package_version = if rf_target.is_fungible_component(&ap_name) {
            match rf_target.get_identifier_from_chassis(dev_url).await {
                Some(identifier) => rf_target
                    .get_version_sku(&identifier.to_lowercase(), package_versions, &ap_name)
                    .unwrap_or_else(|| "N/A".to_string()),
                None => "N/A".to_string(),
            }
        } else {
            rf_target
                .get_component_version(package_versions, &ap_name, None)
                .await
                .unwrap_or_else(|| "N/A".to_string())
        };

        let (status, reason) = if package_version_is_unknown(&package_version) {
            (
                "unverified",
                Some("package does not contain a comparable version for this component"),
            )
        } else if firmware_versions_exactly_match(&package_version, &system_version) {
            ("matched", None)
        } else {
            (
                "mismatched",
                Some("installed version does not match package version"),
            )
        };

        let mut component = json!({
            "name": ap_inv_name,
            "inventory_path": dev_url,
            "system_version": system_version,
            "package_version": package_version,
            "status": status,
        });
        if let Some(reason) = reason {
            component["reason"] = json!(reason);
        }
        components.push(component);
    }

    components
}

fn package_version_is_unknown(version: &str) -> bool {
    matches!(
        version.trim().to_ascii_lowercase().as_str(),
        "" | "unknown" | "n/a"
    )
}

fn firmware_versions_exactly_match(package_version: &str, system_version: &str) -> bool {
    package_version
        .trim()
        .eq_ignore_ascii_case(system_version.trim())
}

fn update_cmd_args(server_type: ServerType, request: &FirmwareUpdateRequest) -> CmdArgs {
    CmdArgs {
        cmd: "update_fw".to_string(),
        background: false,
        details: false,
        staged_update: request.options.staged == StagedMode::StageOnly,
        staged_activate_update: request.options.staged == StagedMode::StageAndActivate,
        quiet: true,
        special: build_update_parameters(server_type, request).map(|value| vec![value.to_string()]),
        oem_parameters: request
            .options
            .oem_parameters
            .as_ref()
            .map(|value| vec![value.to_string()]),
    }
}

fn update_failure_error(
    status: i32,
    task_ids: &[String],
    parse_failure: Option<(String, String)>,
    details: &Value,
) -> NvFwUpdError {
    if task_ids.is_empty() {
        if let Some((path, message)) = parse_failure {
            return NvFwUpdError::PackageParse {
                path,
                message: if message.is_empty() {
                    "failed to parse firmware package".to_string()
                } else {
                    message
                },
            };
        }
    }

    let task_id = task_ids
        .first()
        .cloned()
        .or_else(|| first_task_id_from_details(details));
    let mut message = format!("firmware update failed with status {status}");
    if let Some(summary) = failure_details_summary(details) {
        message.push_str(": ");
        message.push_str(&summary);
    }

    NvFwUpdError::TaskFailed { task_id, message }
}

/// Find the first task/job id carried in CLI-style JSON output details.
fn first_task_id_from_details(details: &Value) -> Option<String> {
    output_entries(details).iter().find_map(|entry| {
        ["Id", "id", "TaskId", "task_id", "JobId", "job_id"]
            .iter()
            .find_map(|key| entry.get(*key).and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

/// Build a short, redacted failure summary suitable for RMS error messages.
///
/// The full JSON is still preserved in structured details, but callers need a
/// readable message that surfaces the most useful error/output fields first.
fn failure_details_summary(details: &Value) -> Option<String> {
    let redacted = NvUtils::redact_secret_json_value(details);
    let mut parts = Vec::new();

    if let Some(errors) = redacted.get("Error").and_then(Value::as_array) {
        for error in errors.iter().filter(|value| !value_is_empty(value)).take(3) {
            parts.push(format!("error={}", compact_value(error)));
        }
    }

    if let Some(output) = redacted.get("Output").and_then(Value::as_array) {
        for entry in output
            .iter()
            .filter(|value| !value_is_empty(value))
            .rev()
            .take(2)
        {
            parts.push(format!("output={}", compact_value(entry)));
        }
    }

    for key in [
        "error",
        "message",
        "Message",
        "detail",
        "status",
        "TaskStatus",
        "state",
        "issue",
    ] {
        if let Some(value) = redacted.get(key).filter(|value| !value_is_empty(value)) {
            parts.push(format!("{key}={}", compact_value(value)));
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(truncate_detail(&parts.join("; "), 1200))
    }
}

fn value_is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => value.trim().is_empty(),
        Value::Array(values) => values.is_empty(),
        Value::Object(values) => values.is_empty(),
        _ => false,
    }
}

fn compact_value(value: &Value) -> String {
    let rendered = match value {
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    };
    NvUtils::sanitize_log(&truncate_detail(&rendered, 600))
}

fn truncate_detail(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }

    let mut out: String = value.chars().take(max_chars.saturating_sub(3)).collect();
    out.push_str("...");
    out
}

/// Map a successful CLI/library response into the library update outcome model.
///
/// Targets that own their monitoring can complete synchronously. Other targets
/// return a task id for caller-side polling, unless the response indicates an
/// explicit skip/no-op.
fn update_success_outcome(
    server_type: ServerType,
    task_ids: Vec<String>,
    details: Value,
) -> FirmwareUpdateOutcome {
    if let Some(reason) = skipped_reason_from_details(&details) {
        return FirmwareUpdateOutcome::Skipped { reason };
    }

    if completed_update_summary_from_details(server_type, &details) {
        return FirmwareUpdateOutcome::Completed(UpdateSummary {
            message: "firmware update completed".to_string(),
            task_ids,
            details,
        });
    }

    if !owns_update_monitoring(server_type) {
        if let Some(task_id) = task_ids
            .first()
            .cloned()
            .or_else(|| first_task_id_from_details(&details))
        {
            return FirmwareUpdateOutcome::Started(TaskHandle { task_id });
        }

        if output_entries(&details).is_empty() {
            return FirmwareUpdateOutcome::Skipped {
                reason: "firmware update did not create a task; no update was required".to_string(),
            };
        }
    }

    FirmwareUpdateOutcome::Completed(UpdateSummary {
        message: "firmware update completed".to_string(),
        task_ids,
        details,
    })
}

/// Detect aggregate completion records generated by PowerShelf serial PSU fallback.
fn completed_update_summary_from_details(server_type: ServerType, details: &Value) -> bool {
    server_type == ServerType::PowerShelf
        && output_entries(details).iter().any(|entry| {
            entry
                .get("Id")
                .and_then(Value::as_str)
                .map(|id| id == "liteon-psu-serial-complete")
                .unwrap_or(false)
                && entry
                    .get("TaskState")
                    .and_then(Value::as_str)
                    .map(|state| state.eq_ignore_ascii_case("Completed"))
                    .unwrap_or(false)
        })
}

/// Apply server-specific task-status interpretation on top of generic Redfish parsing.
///
/// PowerShelf OnReset handoff and LiteOn PSU completion both require synthetic
/// completion states so RMS does not keep polling an unhelpful task forever.
fn task_status_from_details_for_server(
    server_type: ServerType,
    task_id: &str,
    details: Value,
) -> TaskStatus {
    let mut status = task_status_from_details(task_id, details);
    if server_type == ServerType::PowerShelf && powershelf_on_reset_handoff(&status.details) {
        status.state = TaskState::Completed;
        status.progress_percent = Some(100);
        let detail = status
            .message
            .unwrap_or_else(|| "PowerShelf OnReset update reached reset handoff".to_string());
        status.message = Some(format!(
            "PowerShelf OnReset update reached reset handoff; activation required: {detail}"
        ));
    }
    if server_type == ServerType::PowerShelf && powershelf_liteon_psu_complete(&status.details) {
        status.state = TaskState::Completed;
        status.progress_percent = Some(100);
        status.message = Some("LiteOn PSU update completed".to_string());
    }
    status
}

/// Detect PowerShelf task details that represent reset-handoff completion.
fn powershelf_on_reset_handoff(details: &Value) -> bool {
    is_powershelf_on_reset_handoff_task(details)
}

/// Detect completed LiteOn PSU PowerUnit status details for RMS polling.
///
/// LiteOn PSUs can report completion at 100 or after rolling progress back to 0,
/// as long as state/status are not failure-like values.
fn powershelf_liteon_psu_complete(details: &Value) -> bool {
    let Some(progress) = details.get("updateProgress").and_then(Value::as_u64) else {
        return false;
    };

    if !matches!(progress, 0 | 100) {
        return false;
    }

    let is_failure = |key: &str| {
        details
            .get(key)
            .and_then(Value::as_str)
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
    };

    !is_failure("state") && !is_failure("status")
}

/// Borrow the CLI-compatible `Output` array from response details.
fn output_entries(details: &Value) -> &[Value] {
    details
        .get("Output")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// Recursively detect skip/no-op responses from CLI-style JSON details.
fn skipped_reason_from_details(details: &Value) -> Option<String> {
    match details {
        Value::Object(map) => {
            if map
                .get("Skipped")
                .or_else(|| map.get("skipped"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Some(reason_from_map(map));
            }

            if map
                .get("AlreadyUpToDate")
                .or_else(|| map.get("already_up_to_date"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Some(reason_from_map(map));
            }

            if let Some(status) = map.get("status").and_then(Value::as_str) {
                let normalized = status.to_ascii_lowercase().replace('-', "_");
                if matches!(
                    normalized.as_str(),
                    "skipped" | "already_current" | "already_up_to_date" | "no_update_required"
                ) {
                    return Some(reason_from_map(map));
                }
            }

            for key in ["reason", "message", "Message"] {
                if let Some(message) = map.get(key).and_then(Value::as_str) {
                    if message_is_skip_reason(message) {
                        return Some(message.to_string());
                    }
                }
            }

            map.values().find_map(skipped_reason_from_details)
        }
        Value::Array(values) => values.iter().find_map(skipped_reason_from_details),
        Value::String(message) => {
            if message_is_skip_reason(message) {
                Some(message.to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn reason_from_map(map: &serde_json::Map<String, Value>) -> String {
    for key in ["reason", "message", "Message"] {
        if let Some(reason) = map.get(key).and_then(Value::as_str) {
            if !reason.is_empty() {
                return reason.to_string();
            }
        }
    }

    "firmware already up to date".to_string()
}

fn message_is_skip_reason(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase().replace('-', "_");
    normalized.contains("already up to date")
        || normalized.contains("already current")
        || normalized.contains("already_current")
        || normalized.contains("already_up_to_date")
        || normalized.contains("up_to_date")
        || normalized.contains("no update required")
        || normalized.contains("no_update_required")
}

/// Build UpdateParameters from RMS/library request options.
///
/// Existing caller-supplied JSON is preserved, while component targets,
/// ForceUpdate, ApplyTime, and LiteOn PSU IDs are filled in when needed.
fn build_update_parameters(
    server_type: ServerType,
    request: &FirmwareUpdateRequest,
) -> Option<Value> {
    let mut params = request
        .options
        .special_json
        .clone()
        .unwrap_or_else(|| json!({}));

    if !params.is_object() {
        return Some(params);
    }

    let obj = params.as_object_mut()?;
    if !request.component.is_empty()
        && !powershelf_component_uses_package_level_update(server_type, &request.component)
        && !obj.contains_key("Targets")
        && !obj.contains_key("HttpPushUriTargets")
    {
        let (key, targets) = component_update_parameter(server_type, &request.component);
        obj.insert(key.to_string(), targets);
    }
    if request.force_update {
        obj.insert("ForceUpdate".to_string(), Value::Bool(true));
    }
    if let Some(apply_time) = &request.options.apply_time {
        obj.insert("ApplyTime".to_string(), Value::String(apply_time.clone()));
    }
    if let Some(device_id) = &request.options.liteon_device_id {
        obj.insert(
            "LiteOnPowerDeviceId".to_string(),
            Value::String(device_id.clone()),
        );
    }

    if obj.is_empty() {
        None
    } else {
        Some(params)
    }
}

/// Return true for PowerShelf components that must not be converted into Targets.
fn powershelf_component_uses_package_level_update(
    server_type: ServerType,
    component: &str,
) -> bool {
    server_type == ServerType::PowerShelf
        && matches!(
            component.trim().to_ascii_lowercase().as_str(),
            "pmc" | "bmc"
        )
}

/// Select the Redfish update-parameter key expected by each platform family.
fn component_update_parameter_key(server_type: ServerType) -> &'static str {
    match server_type {
        ServerType::HGX | ServerType::MGX => "HttpPushUriTargets",
        _ => "Targets",
    }
}

/// Build the component target parameter for a single-component update request.
fn component_update_parameter(server_type: ServerType, component: &str) -> (&'static str, Value) {
    (
        component_update_parameter_key(server_type),
        json!(component_update_targets(server_type, component)),
    )
}

fn component_update_targets(server_type: ServerType, component: &str) -> Vec<String> {
    if matches!(
        server_type,
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72
    ) && component.eq_ignore_ascii_case("bmc")
    {
        return Vec::new();
    }

    vec![component_to_update_target(server_type, component)]
}

/// Convert a friendly component name into the target string expected by NVFWUPD.
fn component_to_update_target(server_type: ServerType, component: &str) -> String {
    if component.starts_with('/') {
        component.to_string()
    } else if is_switch(server_type) {
        match component.to_lowercase().as_str() {
            "cpld" | "cpld1" | "cpld2" | "cpld3" | "cpld4" => "CPLD1".to_string(),
            "bmc" => "BMC".to_string(),
            "erot" => "EROT".to_string(),
            "fpga" | "smr" => "FPGA".to_string(),
            "bios" | "sbios" => "BIOS".to_string(),
            other => other.to_uppercase(),
        }
    } else {
        format!("/redfish/v1/UpdateService/FirmwareInventory/{component}")
    }
}

fn activation_command_for_mode(
    server_type: ServerType,
    mode: &ActivationMode,
) -> Result<&'static str> {
    match mode {
        ActivationMode::SingleCommand(command) => {
            let cli_command = command.as_cli_command();
            if activation_command_supported(server_type, cli_command) {
                Ok(cli_command)
            } else {
                Err(NvFwUpdError::Unsupported("activation command"))
            }
        }
        ActivationMode::FullGb200Compute => Err(NvFwUpdError::Unsupported(
            "full GB200 compute activation workflow",
        )),
        ActivationMode::SwitchPowerCycle => {
            if is_switch(server_type) {
                Ok("NVUE_PWR_CYCLE")
            } else {
                Err(NvFwUpdError::Unsupported("switch power-cycle activation"))
            }
        }
        ActivationMode::PowerShelfReset { force } => {
            if server_type == ServerType::PowerShelf {
                if *force {
                    Ok("RF_PWRSHELF_RESET_FORCE")
                } else {
                    Ok("RF_PWRSHELF_RESET")
                }
            } else {
                Err(NvFwUpdError::Unsupported("powershelf activation"))
            }
        }
    }
}

fn activation_command_supported(server_type: ServerType, command: &str) -> bool {
    match server_type {
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72 => matches!(
            command,
            "RF_AUX_PWR_CYCLE" | "RF_PWR_ON" | "RF_PWR_OFF" | "RF_PWR_CYCLE" | "RF_PWR_STATUS"
        ),
        ServerType::PowerShelf => {
            matches!(command, "RF_PWRSHELF_RESET" | "RF_PWRSHELF_RESET_FORCE")
        }
        ServerType::GB200Switch | ServerType::GB300Switch | ServerType::VRNVL72Switch => false,
        ServerType::DGX | ServerType::DGXRubin => matches!(
            command,
            "RF_PWR_ON" | "RF_PWR_OFF" | "RF_PWR_CYCLE" | "RF_PWR_STATUS"
        ),
        ServerType::GH200 => matches!(
            command,
            "RF_AUX_PWR_CYCLE" | "RF_PWR_ON" | "RF_PWR_OFF" | "RF_PWR_CYCLE" | "RF_PWR_STATUS"
        ),
        ServerType::HGXRubin => command == "RF_PWR_CYCLE",
        ServerType::HGX | ServerType::MGX | ServerType::HGXB100 | ServerType::HGXB300 => false,
    }
}

fn firmware_component_from_inventory(path: String, details: Value) -> FirmwareComponent {
    let name = details
        .get("Id")
        .and_then(Value::as_str)
        .or_else(|| details.get("Name").and_then(Value::as_str))
        .map(ToString::to_string)
        .unwrap_or_else(|| path.rsplit('/').next().unwrap_or(&path).to_string());
    let version = details
        .get("Version")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let device_class = details
        .get("SoftwareId")
        .and_then(Value::as_str)
        .map(ToString::to_string);

    FirmwareComponent {
        name,
        version,
        device_class,
        inventory_path: Some(path),
        details,
    }
}

fn task_status_from_details(task_id: &str, details: Value) -> TaskStatus {
    let state_str = details
        .get("TaskState")
        .or_else(|| details.get("state"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let state = task_state_from_str(state_str);
    let message = task_message_from_details(&details);
    let progress_percent = details
        .get("PercentComplete")
        .and_then(Value::as_u64)
        .and_then(|progress| u8::try_from(progress).ok());

    TaskStatus {
        task_id: task_id.to_string(),
        state,
        message,
        progress_percent,
        details,
    }
}

fn task_message_from_details(details: &Value) -> Option<String> {
    let messages = details.get("Messages").and_then(Value::as_array);
    if let Some(messages) = messages {
        let mut failures = Vec::new();
        for msg in messages {
            let Some(message) = msg.get("Message").and_then(Value::as_str) else {
                continue;
            };
            let severity = msg
                .get("Severity")
                .or_else(|| msg.get("MessageSeverity"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            if severity.eq_ignore_ascii_case("OK") {
                continue;
            }

            let resolution = msg
                .get("Resolution")
                .and_then(Value::as_str)
                .filter(|resolution| {
                    !resolution.trim().is_empty() && !resolution.eq_ignore_ascii_case("None.")
                });
            if let Some(resolution) = resolution {
                failures.push(format!("{message} Resolution: {resolution}"));
            } else {
                failures.push(message.to_string());
            }
        }

        if !failures.is_empty() {
            return Some(failures.join("; "));
        }

        if let Some(message) = messages
            .last()
            .and_then(|msg| msg.get("Message"))
            .and_then(Value::as_str)
        {
            return Some(message.to_string());
        }
    }

    details
        .get("TaskStatus")
        .and_then(Value::as_str)
        .or_else(|| details.get("status").and_then(Value::as_str))
        .map(ToString::to_string)
}

fn task_state_from_str(state: &str) -> TaskState {
    match state.to_lowercase().as_str() {
        "pending" | "new" | "starting" => TaskState::Pending,
        "running" | "start" | "inprogress" | "in_progress" => TaskState::Running,
        "completed" | "complete" | "ok" => TaskState::Completed,
        "exception" | "failed" | "error" | "critical" => TaskState::Failed,
        "cancelled" | "canceled" => TaskState::Cancelled,
        _ => TaskState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use indexmap::IndexMap;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn target(server_type: ServerType) -> TargetConfig {
        TargetConfig {
            ip: "192.0.2.44".to_string(),
            username: "admin".to_string(),
            password: "secret".to_string(),
            port: Some(443),
            server_type,
            verify_tls: false,
            ssh_known_hosts: None,
            ssh_host_key_mode: crate::workflow::SshHostKeyMode::TrustOnFirstUse,
        }
    }

    fn legacy_switch_target(access: BmcAccess) -> WorkflowTarget {
        WorkflowTarget::Switch {
            target: Box::new(GB200SwitchRFTarget::new(access, None)),
            client: None,
        }
    }

    fn hosted_switch_context(server: &MockServer) -> (WorkflowContext, TargetConfig) {
        let client = NvueClient::new(nvue_client::ClientConfig {
            endpoint: nvue_client::ClientEndpoint::http("127.0.0.1", server.address().port()),
            credentials: nvue_client::ClientCredentials::new("admin", "secret"),
            dangerously_accept_invalid_certs: false,
        })
        .unwrap();

        let mut switch_target = target(ServerType::GB200Switch);

        switch_target.ip = "127.0.0.1".to_owned();
        switch_target.port = Some(server.address().port());

        (WorkflowContext::with_nvue_client(client), switch_target)
    }

    #[derive(Default)]
    struct FailingFirmwarePkg {
        apname_version_dict: HashMap<String, IndexMap<String, Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl FirmwarePkg for FailingFirmwarePkg {
        async fn parse_pkg(
            &mut self,
            _package_name: &str,
            _json_dict: Option<&mut Value>,
        ) -> (bool, String) {
            (false, "Not a valid PLDM package".to_string())
        }

        async fn remove_files(&mut self) {}

        fn apname_version_dict(&self) -> &HashMap<String, IndexMap<String, Vec<String>>> {
            &self.apname_version_dict
        }

        fn print_package_content(&self, _package_name: &str) {}
    }

    struct RetryingActivationTarget {
        access: BmcAccess,
        statuses: VecDeque<i32>,
        commands: Vec<String>,
        fungible_components: Vec<String>,
        update_completion_msg: String,
        progress_table_header_printed: bool,
    }

    impl RetryingActivationTarget {
        fn new(statuses: impl IntoIterator<Item = i32>) -> Self {
            Self::with_access(
                statuses,
                BmcAccess::mock_with_base_url("http://127.0.0.1".to_string(), "mock-activation"),
            )
        }

        fn with_access(statuses: impl IntoIterator<Item = i32>, access: BmcAccess) -> Self {
            Self {
                access,
                statuses: statuses.into_iter().collect(),
                commands: Vec::new(),
                fungible_components: Vec::new(),
                update_completion_msg: String::new(),
                progress_table_header_printed: false,
            }
        }
    }

    struct VersionCheckTarget {
        access: BmcAccess,
        package_versions: HashMap<String, String>,
    }

    impl VersionCheckTarget {
        fn new(package_versions: impl IntoIterator<Item = (&'static str, &'static str)>) -> Self {
            Self {
                access: BmcAccess::mock_with_base_url("http://127.0.0.1", "mock-version-check"),
                package_versions: package_versions
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            }
        }
    }

    fn version_check_target(component: &str, firmware_file: &str) -> FirmwareVersionCheckTarget {
        FirmwareVersionCheckTarget {
            component: component.to_owned(),
            firmware_file: firmware_file.to_owned(),
        }
    }

    #[async_trait::async_trait]
    impl RFTarget for RetryingActivationTarget {
        fn target_access(&self) -> &BmcAccess {
            &self.access
        }

        fn target_access_mut(&mut self) -> &mut BmcAccess {
            &mut self.access
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
            "RetryingActivationTarget"
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

        async fn run_oob_activation(&mut self, cmd_args: &CmdArgs) -> i32 {
            self.commands.push(cmd_args.cmd.clone());
            self.statuses.pop_front().unwrap_or(0)
        }
    }

    #[async_trait::async_trait]
    impl RFTarget for VersionCheckTarget {
        fn target_access(&self) -> &BmcAccess {
            &self.access
        }

        fn target_access_mut(&mut self) -> &mut BmcAccess {
            &mut self.access
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
            "VersionCheckTarget"
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
            ap_name: &str,
            _pkg_parser: Option<&dyn PkgParser>,
        ) -> Option<String> {
            self.package_versions.get(ap_name).cloned()
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

    #[tokio::test]
    async fn version_check_requires_exact_package_version_match() {
        let target = VersionCheckTarget::new([("fw_bmc_0", "1.2.2")]);
        let inventory = serde_json::Map::from_iter([(
            "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0".to_string(),
            json!({"Version": "1.2.3"}),
        )]);

        let components =
            compare_inventory_to_package_versions(&target, &inventory, &json!({})).await;

        assert_eq!(components.len(), 1);
        assert_eq!(components[0]["name"], "FW_BMC_0");
        assert_eq!(components[0]["package_version"], "1.2.2");
        assert_eq!(components[0]["system_version"], "1.2.3");
        assert_eq!(components[0]["status"], "mismatched");
    }

    #[tokio::test]
    async fn version_check_marks_missing_package_versions_unverified() {
        let target = VersionCheckTarget::new([]);
        let inventory = serde_json::Map::from_iter([(
            "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0".to_string(),
            json!({"Version": "1.2.3"}),
        )]);

        let components =
            compare_inventory_to_package_versions(&target, &inventory, &json!({})).await;

        assert_eq!(components.len(), 1);
        assert_eq!(components[0]["package_version"], "N/A");
        assert_eq!(components[0]["status"], "unverified");
    }

    #[tokio::test]
    async fn version_check_compares_only_the_applied_component() {
        let target = VersionCheckTarget::new([("bios", "1.0"), ("erot", "2.0")]);
        let inventory = serde_json::Map::from_iter([
            ("BIOS".to_owned(), json!({"Version": "1.0"})),
            ("EROT".to_owned(), json!({"Version": "old"})),
        ]);
        let selected_target = version_check_target("bios", "/tmp/switch.fwpkg");

        let mut selected_inventory = inventory.clone();
        let scope = firmware_version_check_scope(ServerType::GB200Switch, &[selected_target]);
        assert!(
            scope_firmware_inventory(ServerType::GB200Switch, &scope, &mut selected_inventory)
                .is_empty()
        );
        let selected =
            compare_inventory_to_package_versions(&target, &selected_inventory, &json!({})).await;

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0]["name"], "BIOS");
        assert_eq!(selected[0]["status"], "matched");

        let package_scope = firmware_version_check_scope(
            ServerType::GB200Switch,
            &[version_check_target("", "/tmp/switch.fwpkg")],
        );
        let mut package_inventory = inventory;
        assert!(scope_firmware_inventory(
            ServerType::GB200Switch,
            &package_scope,
            &mut package_inventory,
        )
        .is_empty());
        let package =
            compare_inventory_to_package_versions(&target, &package_inventory, &json!({})).await;

        assert_eq!(package.len(), 2);
        assert!(package.iter().any(|component| {
            component["name"] == "EROT" && component["status"] == "mismatched"
        }));
    }

    #[test]
    fn version_check_keeps_scope_per_file_and_reports_missing_targets() {
        let groups = group_firmware_version_check_targets(vec![
            version_check_target("", "/tmp/full.fwpkg"),
            version_check_target("bios", "/tmp/scoped.fwpkg"),
            version_check_target("erot", "/tmp/scoped.fwpkg"),
        ]);

        assert_eq!(groups.len(), 2);
        assert_eq!(
            firmware_version_check_scope(ServerType::GB200Switch, &groups[0].1),
            FirmwareVersionCheckScope::Package
        );

        let scope = firmware_version_check_scope(ServerType::GB200Switch, &groups[1].1);
        let mut partial_inventory =
            serde_json::Map::from_iter([("BIOS".to_owned(), json!({"Version": "1"}))]);
        let missing =
            scope_firmware_inventory(ServerType::GB200Switch, &scope, &mut partial_inventory);

        assert_eq!(partial_inventory.len(), 1);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0]["name"], "EROT");
        assert_eq!(missing[0]["status"], "mismatched");

        let mut empty_inventory = serde_json::Map::new();
        assert_eq!(
            scope_firmware_inventory(ServerType::GB200Switch, &scope, &mut empty_inventory).len(),
            2
        );

        let mut components = vec![json!({"inventory_path": "BIOS", "status": "mismatched"})];
        merge_firmware_version_components(
            &mut components,
            vec![json!({"inventory_path": "BIOS", "status": "matched"})],
            FirmwareVersionCheckScope::Package.replaces_concrete_with_unverified(),
        );
        assert_eq!(components.len(), 1);
        assert_eq!(components[0]["status"], "matched");

        merge_firmware_version_components(
            &mut components,
            vec![json!({"inventory_path": "BIOS", "status": "unverified"})],
            FirmwareVersionCheckScope::Package.replaces_concrete_with_unverified(),
        );
        assert_eq!(components[0]["status"], "matched");

        merge_firmware_version_components(
            &mut components,
            vec![json!({"inventory_path": "BIOS", "status": "unverified"})],
            FirmwareVersionCheckScope::Components(vec!["BIOS".to_owned()])
                .replaces_concrete_with_unverified(),
        );
        assert_eq!(components[0]["status"], "unverified");
    }

    #[test]
    fn package_wide_unverified_result_does_not_erase_component_mismatch() {
        let mut components = vec![json!({
            "inventory_path": "BIOS",
            "status": "mismatched",
        })];

        merge_firmware_version_components(
            &mut components,
            vec![json!({"inventory_path": "BIOS", "status": "unverified"})],
            FirmwareVersionCheckScope::Package.replaces_concrete_with_unverified(),
        );

        assert_eq!(components[0]["status"], "mismatched");
    }

    #[test]
    fn version_check_matches_platform_inventory_targets() {
        assert!(firmware_inventory_target_matches(
            ServerType::GB200,
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
        ));
        assert!(!firmware_inventory_target_matches(
            ServerType::GB200,
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_1",
        ));
        assert!(firmware_inventory_target_matches(
            ServerType::GB200Switch,
            "TRANSCEIVER",
            "/nvue_v1/platform/firmware/OSFP1",
        ));
        let cpld_scope = firmware_version_check_scope(
            ServerType::GB200Switch,
            &[version_check_target("cpld", "/tmp/cpld.fwpkg")],
        );
        let mut partial_cpld_inventory =
            serde_json::Map::from_iter([("CPLD1".to_owned(), json!({"Version": "1"}))]);
        let missing = scope_firmware_inventory(
            ServerType::GB200Switch,
            &cpld_scope,
            &mut partial_cpld_inventory,
        );
        assert_eq!(missing.len(), 3);
        assert_eq!(missing[0]["name"], "CPLD2");
    }

    #[test]
    fn expected_inventory_chassis_links_use_related_item_for_expected_aps_only() {
        let inventory = serde_json::Map::from_iter([
            (
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0".to_string(),
                json!({
                    "Id": "HGX_FW_GPU_0",
                    "RelatedItem": [
                        {"@odata.id": "/redfish/v1/Chassis/HGX_GPU_0"}
                    ]
                }),
            ),
            (
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_1".to_string(),
                json!({
                    "Id": "HGX_FW_GPU_1",
                    "RelatedItem": [
                        {"@odata.id": "/redfish/v1/Chassis/HGX_GPU_1"}
                    ]
                }),
            ),
        ]);

        let links = expected_inventory_chassis_links(&["hgx_fw_gpu_0".to_string()], &inventory);

        assert_eq!(
            links,
            vec![ExpectedInventoryChassisLink {
                ap_name: "hgx_fw_gpu_0".to_string(),
                chassis_uri: "/redfish/v1/Chassis/HGX_GPU_0".to_string(),
            }]
        );
    }

    #[test]
    fn expected_inventory_chassis_validation_reports_missing_related_chassis() {
        let links = vec![
            ExpectedInventoryChassisLink {
                ap_name: "HGX_FW_GPU_0".to_string(),
                chassis_uri: "/redfish/v1/Chassis/HGX_GPU_0".to_string(),
            },
            ExpectedInventoryChassisLink {
                ap_name: "HGX_FW_GPU_1".to_string(),
                chassis_uri: "/redfish/v1/Chassis/HGX_GPU_1".to_string(),
            },
        ];
        let chassis_members = vec!["/redfish/v1/Chassis/HGX_GPU_0".to_string()];

        let err = expected_inventory_chassis_validation_error(
            "post-activation expected inventory check",
            &links,
            &chassis_members,
        )
        .expect("missing RelatedItem chassis should fail");

        assert!(matches!(
            err,
            NvFwUpdError::InvalidResponse {
                context: "expected firmware inventory chassis RelatedItem",
                message,
            } if message.contains("HGX_FW_GPU_1 -> /redfish/v1/Chassis/HGX_GPU_1")
                && !message.contains("HGX_FW_GPU_0 -> /redfish/v1/Chassis/HGX_GPU_0")
        ));
    }

    #[tokio::test]
    async fn workflow_pkg_parser_allows_unparsed_compute_package_upload() {
        let mut pkg = FailingFirmwarePkg::default();
        let mut parser = WorkflowPkgParser::new(&mut pkg, true);

        let (ok, msg) = parser.parse_pkg("/tmp/generic_bmc_signed.ima").await;

        assert!(ok);
        assert!(msg.is_empty());
        assert!(parser.take_last_parse_failure().is_none());
    }

    #[tokio::test]
    async fn workflow_pkg_parser_preserves_parse_error_when_raw_upload_disabled() {
        let mut pkg = FailingFirmwarePkg::default();
        let mut parser = WorkflowPkgParser::new(&mut pkg, false);

        let (ok, msg) = parser.parse_pkg("/tmp/bad.fwpkg").await;

        assert!(!ok);
        assert_eq!(msg, "Not a valid PLDM package");
        assert_eq!(
            parser.take_last_parse_failure(),
            Some((
                "/tmp/bad.fwpkg".to_string(),
                "Not a valid PLDM package".to_string()
            ))
        );
    }

    #[test]
    fn unparsed_package_update_fallback_is_compute_family_only() {
        assert!(server_type_allows_unparsed_package_update(
            ServerType::GB200
        ));
        assert!(server_type_allows_unparsed_package_update(
            ServerType::GB300
        ));
        assert!(server_type_allows_unparsed_package_update(
            ServerType::VRNVL72
        ));
        assert!(!server_type_allows_unparsed_package_update(
            ServerType::GB200Switch
        ));
        assert!(!server_type_allows_unparsed_package_update(
            ServerType::PowerShelf
        ));
    }

    #[test]
    fn build_nvue_access_uses_switch_access_type_and_servertype() {
        let access = build_nvue_access(&target(ServerType::GB200Switch)).unwrap();

        assert_eq!(access.access_type, AccessType::NVSwitch);
        assert_eq!(access.servertype, "gb200switch");
        assert_eq!(access.base_url, "https://192.0.2.44:443");
    }

    #[test]
    fn build_bmc_access_remains_redfish_only() {
        let config = target(ServerType::GB200);
        let access = WorkflowTargetBuilder::new(&config)
            .build_bmc_access()
            .unwrap();

        assert_eq!(access.access_type, AccessType::Login);
        assert_eq!(access.base_url, "https://192.0.2.44:443");
    }

    #[tokio::test]
    async fn hosted_context_routes_switch_inventory_through_shared_nvue_client() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "BMC": {"actual-firmware": "88.0002.1979"}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (context, switch_target) = hosted_switch_context(&server);

        let inventory = context.get_firmware_inventory(switch_target).await.unwrap();

        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].name, "BMC");
        assert_eq!(inventory[0].version.as_deref(), Some("88.0002.1979"));
    }

    #[test]
    fn hosted_context_validates_management_target_and_port() {
        for (connect_host, client_port, target_ip, target_port, expected) in [
            ("192.0.2.44", 443, "192.0.2.44", 443, true),
            ("198.51.100.7", 443, "192.0.2.44", 443, false),
            ("192.0.2.44", 8443, "192.0.2.44", 443, false),
        ] {
            let client = NvueClient::new(nvue_client::ClientConfig {
                endpoint: nvue_client::ClientEndpoint::https(
                    "switch.example.com",
                    connect_host,
                    client_port,
                ),
                credentials: nvue_client::ClientCredentials::new("admin", "secret"),
                dangerously_accept_invalid_certs: true,
            })
            .unwrap();
            let context = WorkflowContext::with_nvue_client(client);
            let mut switch_target = target(ServerType::GB200Switch);

            switch_target.ip = target_ip.to_owned();
            switch_target.port = Some(target_port);

            assert_eq!(
                context
                    .target_builder(&switch_target)
                    .build_target()
                    .is_ok(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn hosted_context_rejects_empty_or_malformed_inventory_json() {
        for body in ["", "not-json"] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/nvue_v1/platform/firmware"))
                .respond_with(ResponseTemplate::new(200).set_body_string(body))
                .expect(1)
                .mount(&server)
                .await;

            let (context, switch_target) = hosted_switch_context(&server);

            let error = context
                .get_firmware_inventory(switch_target)
                .await
                .unwrap_err();

            assert!(matches!(error, NvFwUpdError::BmcUnreachable { .. }));
        }
    }

    #[test]
    fn firmware_component_from_inventory_prefers_id_over_display_name() {
        let component = firmware_component_from_inventory(
            "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0".to_string(),
            json!({
                "Id": "FW_BMC_0",
                "Name": "Software Inventory",
                "Version": "1.2.3",
            }),
        );

        assert_eq!(component.name, "FW_BMC_0");
        assert_eq!(component.version.as_deref(), Some("1.2.3"));
    }

    #[test]
    fn target_builder_maps_every_server_type_to_expected_target_class() {
        let cases = [
            (ServerType::DGX, "DGX_RFTarget", "dgx", AccessType::Login),
            (
                ServerType::DGXRubin,
                "DGX_RFTarget",
                "dgxrubin",
                AccessType::Login,
            ),
            (ServerType::HGX, "GHRFTarget", "hgx", AccessType::Login),
            (ServerType::MGX, "GHRFTarget", "mgx", AccessType::Login),
            (
                ServerType::GH200,
                "GH200RFTarget",
                "gh200",
                AccessType::Login,
            ),
            (
                ServerType::HGXB100,
                "HGXB100RFTarget",
                "hgxb100",
                AccessType::Login,
            ),
            (
                ServerType::HGXB300,
                "HGXB100RFTarget",
                "hgxb300",
                AccessType::Login,
            ),
            (
                ServerType::HGXRubin,
                "HGXRUBINRFTarget",
                "hgxrubin",
                AccessType::Login,
            ),
            (
                ServerType::GB200,
                "GB200RFTarget",
                "gb200",
                AccessType::Login,
            ),
            (
                ServerType::GB300,
                "GB200RFTarget",
                "gb300",
                AccessType::Login,
            ),
            (
                ServerType::VRNVL72,
                "GB200RFTarget",
                "vrnvl72",
                AccessType::Login,
            ),
            (
                ServerType::GB200Switch,
                "GB200SwitchRFTarget",
                "gb200switch",
                AccessType::NVSwitch,
            ),
            (
                ServerType::GB300Switch,
                "GB200SwitchRFTarget",
                "gb300switch",
                AccessType::NVSwitch,
            ),
            (
                ServerType::VRNVL72Switch,
                "GB200SwitchRFTarget",
                "vrnvl72switch",
                AccessType::NVSwitch,
            ),
            (
                ServerType::PowerShelf,
                "PowerShelfRFTarget",
                "powershelf",
                AccessType::Login,
            ),
        ];

        for (server_type, class_name, servertype_arg, access_type) in cases {
            let rf_target = WorkflowTargetBuilder::new(&target(server_type))
                .build_target()
                .unwrap();
            assert_eq!(rf_target.class_name(), class_name);
            assert_eq!(rf_target.target_access().servertype, servertype_arg);
            assert_eq!(rf_target.target_access().access_type, access_type);
            assert!(rf_target.config_dict().is_none());
        }
    }

    #[test]
    fn target_builder_builds_access_from_target_config_without_cli_input_params() {
        let config = TargetConfig {
            ip: "2001:db8::44".to_string(),
            username: "admin".to_string(),
            password: "secret".to_string(),
            port: None,
            server_type: ServerType::GB200,
            verify_tls: true,
            ssh_known_hosts: None,
            ssh_host_key_mode: crate::workflow::SshHostKeyMode::TrustOnFirstUse,
        };

        let access = WorkflowTargetBuilder::new(&config)
            .build_bmc_access()
            .unwrap();
        assert_eq!(access.ip, "[2001:db8::44]");
        assert_eq!(access.user, "admin");
        assert_eq!(access.password, "secret");
        assert_eq!(access.port, "");
        assert_eq!(access.base_url, "https://[2001:db8::44]");
        assert_eq!(access.transport_type, "https");
        assert_eq!(access.access_type, AccessType::Login);
        assert_eq!(access.servertype, "gb200");
    }

    #[test]
    fn update_monitoring_timeout_matches_target_type() {
        for server_type in [
            ServerType::GB200Switch,
            ServerType::GB300Switch,
            ServerType::VRNVL72Switch,
        ] {
            assert_eq!(
                target_monitoring_timeout_secs(server_type),
                DEFAULT_SWITCH_TIMEOUT_SECS
            );
        }

        for server_type in [ServerType::GB200, ServerType::PowerShelf, ServerType::HGX] {
            assert_eq!(
                target_monitoring_timeout_secs(server_type),
                DEFAULT_REDFISH_TIMEOUT_SECS
            );
        }
    }

    #[test]
    fn task_status_from_details_maps_redfish_and_nvue_states() {
        let redfish = task_status_from_details(
            "Task-1",
            json!({
                "TaskState": "Completed",
                "TaskStatus": "OK",
                "PercentComplete": 100
            }),
        );
        assert_eq!(redfish.state, TaskState::Completed);
        assert_eq!(redfish.progress_percent, Some(100));
        assert_eq!(redfish.message.as_deref(), Some("OK"));

        let nvue = task_status_from_details(
            "job-1",
            json!({
                "state": "running",
                "status": "in progress"
            }),
        );
        assert_eq!(nvue.state, TaskState::Running);
        assert_eq!(nvue.message.as_deref(), Some("in progress"));
    }

    #[test]
    fn powershelf_liteon_psu_progress_zero_or_complete_maps_to_completed() {
        for progress in [0, 100] {
            let status = task_status_from_details_for_server(
                ServerType::PowerShelf,
                "powerdevice1",
                json!({
                    "state": "Enabled",
                    "status": "OK",
                    "updateProgress": progress
                }),
            );
            assert_eq!(status.state, TaskState::Completed);
            assert_eq!(status.progress_percent, Some(100));
            assert_eq!(
                status.message.as_deref(),
                Some("LiteOn PSU update completed")
            );
        }

        let running = task_status_from_details_for_server(
            ServerType::PowerShelf,
            "powerdevice1",
            json!({
                "state": "Enabled",
                "status": "OK",
                "updateProgress": 50
            }),
        );
        assert_ne!(running.state, TaskState::Completed);
    }

    #[test]
    fn powershelf_liteon_serial_summary_maps_to_completed_outcome() {
        let outcome = update_success_outcome(
            ServerType::PowerShelf,
            vec!["liteon-psu-serial-complete".to_string()],
            json!({
                "Output": [{
                    "Id": "liteon-psu-serial-complete",
                    "TaskState": "Completed",
                    "TaskStatus": "OK",
                    "PercentComplete": 100
                }]
            }),
        );

        assert!(matches!(outcome, FirmwareUpdateOutcome::Completed(_)));
    }

    #[test]
    fn task_status_from_details_summarizes_redfish_failure_messages() {
        let redfish = task_status_from_details(
            "Task-0",
            json!({
                "TaskState": "Exception",
                "TaskStatus": "Critical",
                "Messages": [
                    {
                        "Message": "The task with Id '0' has started.",
                        "MessageSeverity": "OK"
                    },
                    {
                        "Message": "The task with Id '0' has completed with errors.",
                        "MessageSeverity": "Critical",
                        "Resolution": "None."
                    },
                    {
                        "Message": "Transfer of image '01.04.0036.0000_n04' to 'FW_ERoT_BMC_0' failed.",
                        "Severity": "Critical"
                    },
                    {
                        "Message": "The resource property 'FW_BMC_0' has detected errors of type 'ERoT is busy'.",
                        "Severity": "Critical",
                        "Resolution": "Wait for background copy operation to complete and rate limit threshold to be cleared."
                    }
                ]
            }),
        );

        let message = redfish
            .message
            .expect("failure message should be summarized");
        assert!(message.contains("completed with errors"));
        assert!(message.contains("FW_ERoT_BMC_0"));
        assert!(message.contains("ERoT is busy"));
        assert!(message.contains("Wait for background copy operation"));
        assert!(!message.contains("has started"));
    }

    #[tokio::test]
    async fn facade_preflight_errors_use_typed_error_categories() {
        let update_err = update_firmware(
            target(ServerType::GB200),
            FirmwareUpdateRequest {
                component: "BMC".to_string(),
                firmware_file: String::new(),
                force_update: false,
                options: crate::workflow::UpdateOptions::default(),
                cancellation: None,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            update_err,
            NvFwUpdError::PackageParse { path, .. } if path.is_empty()
        ));

        let status_err = get_task_status(target(ServerType::GB200), "")
            .await
            .unwrap_err();
        assert!(matches!(
            status_err,
            NvFwUpdError::InvalidResponse {
                context: "task status request",
                ..
            }
        ));
    }

    #[test]
    fn update_failure_error_maps_parser_failures_before_task_creation() {
        let err = update_failure_error(
            1,
            &[],
            Some(("/tmp/bad.fwpkg".to_string(), "bad package".to_string())),
            &json!({}),
        );
        assert!(matches!(
            err,
            NvFwUpdError::PackageParse { path, message }
                if path == "/tmp/bad.fwpkg" && message == "bad package"
        ));

        let err = update_failure_error(
            1,
            &[],
            Some(("/tmp/bad.fwpkg".to_string(), String::new())),
            &json!({}),
        );
        assert!(matches!(
            err,
            NvFwUpdError::PackageParse { path, message }
                if path == "/tmp/bad.fwpkg" && message == "failed to parse firmware package"
        ));
    }

    #[test]
    fn update_failure_error_preserves_task_failures_when_task_exists() {
        let err = update_failure_error(
            1,
            &["Task-1".to_string()],
            Some(("/tmp/bad.fwpkg".to_string(), "bad package".to_string())),
            &json!({
                "Error": ["device said no"],
                "Output": [{"Id": "Task-1", "TaskState": "Exception"}],
            }),
        );
        assert!(matches!(
            err,
            NvFwUpdError::TaskFailed {
                task_id: Some(task_id),
                message,
            } if task_id == "Task-1"
                && message.contains("firmware update failed with status 1")
                && message.contains("device said no")
                && message.contains("TaskState")
        ));
    }

    #[test]
    fn update_failure_error_summarizes_pre_task_transport_details() {
        let err = update_failure_error(
            1,
            &[],
            None,
            &json!({
                "Error": ["Failed to query UpdateService before starting firmware update"],
                "Output": [{
                    "error": "HTTPS GET request failed",
                    "method": "GET",
                    "url": "https://127.0.0.1:9/redfish/v1/UpdateService",
                    "details": "tcp connect error: Connection refused (os error 111)"
                }],
                "Error Code": 1,
            }),
        );
        assert!(matches!(
            err,
            NvFwUpdError::TaskFailed {
                task_id: None,
                message,
            } if message.contains("firmware update failed with status 1")
                && message.contains("Failed to query UpdateService")
                && message.contains("HTTPS GET request failed")
                && message.contains("Connection refused")
                && message.contains("/redfish/v1/UpdateService")
        ));
    }

    #[test]
    fn update_success_outcome_maps_explicit_skip_details_to_skipped() {
        let outcome = update_success_outcome(
            ServerType::GB200Switch,
            vec![],
            json!({
                "Output": [{
                    "status": "already_up_to_date",
                    "reason": "firmware already up to date for switch-1"
                }]
            }),
        );

        assert!(matches!(
            outcome,
            FirmwareUpdateOutcome::Skipped { reason }
                if reason == "firmware already up to date for switch-1"
        ));

        let outcome = update_success_outcome(
            ServerType::GB200,
            vec![],
            json!({
                "Output": [{
                    "Skipped": true,
                    "Message": "no update required for BMC"
                }]
            }),
        );
        assert!(matches!(
            outcome,
            FirmwareUpdateOutcome::Skipped { reason }
                if reason == "no update required for BMC"
        ));

        let outcome = update_success_outcome(
            ServerType::GB200Switch,
            vec![],
            json!({
                "Output": [{
                    "status": "already_current",
                    "message": "firmware already current"
                }]
            }),
        );
        assert!(matches!(
            outcome,
            FirmwareUpdateOutcome::Skipped { reason }
                if reason == "firmware already current"
        ));
    }

    #[test]
    fn update_success_outcome_maps_taskless_empty_redfish_success_to_skipped() {
        let outcome = update_success_outcome(
            ServerType::GB200,
            vec![],
            json!({"Error": [], "Error Code": 0, "Output": []}),
        );

        assert!(matches!(
            outcome,
            FirmwareUpdateOutcome::Skipped { reason }
                if reason.contains("no update was required")
        ));
    }

    #[test]
    fn update_success_outcome_preserves_started_and_completed_paths() {
        let outcome = update_success_outcome(
            ServerType::GB200,
            vec!["Task-1".to_string()],
            json!({"Output": [{"Id": "Task-1"}]}),
        );
        assert!(matches!(
            outcome,
            FirmwareUpdateOutcome::Started(TaskHandle { task_id }) if task_id == "Task-1"
        ));

        let outcome = update_success_outcome(
            ServerType::GB200Switch,
            vec![],
            json!({"Output": [{"Id": "job-1"}]}),
        );
        assert!(matches!(
            outcome,
            FirmwareUpdateOutcome::Completed(UpdateSummary { message, .. })
                if message == "firmware update completed"
        ));
    }

    #[test]
    fn phase_4b6_compute_redfish_outcomes_are_started_or_skipped() {
        let started = update_success_outcome(
            ServerType::GB200,
            vec!["Task-Compute".to_string()],
            json!({"Output": [{"Id": "Task-Compute"}]}),
        );
        assert!(matches!(
            started,
            FirmwareUpdateOutcome::Started(TaskHandle { task_id })
                if task_id == "Task-Compute"
        ));

        let skipped = update_success_outcome(
            ServerType::GB200,
            vec![],
            json!({
                "Output": [{
                    "status": "already_current",
                    "message": "compute firmware already current"
                }]
            }),
        );
        assert!(matches!(
            skipped,
            FirmwareUpdateOutcome::Skipped { reason }
                if reason == "compute firmware already current"
        ));
    }

    #[test]
    fn phase_4b6_switch_outcomes_are_completed_or_skipped() {
        let completed = update_success_outcome(
            ServerType::GB200Switch,
            vec!["job-42".to_string()],
            json!({"Output": [{"Id": "job-42"}]}),
        );
        assert!(matches!(
            completed,
            FirmwareUpdateOutcome::Completed(UpdateSummary {
                task_ids,
                details,
                ..
            }) if task_ids == vec!["job-42".to_string()] && details["Output"][0]["Id"] == "job-42"
        ));

        let skipped = update_success_outcome(
            ServerType::GB200Switch,
            vec![],
            json!({
                "Output": [{
                    "AlreadyUpToDate": true,
                    "reason": "switch firmware already up to date"
                }]
            }),
        );
        assert!(matches!(
            skipped,
            FirmwareUpdateOutcome::Skipped { reason }
                if reason == "switch firmware already up to date"
        ));
    }

    #[test]
    fn phase_4b6_powershelf_outcomes_are_started_or_skipped() {
        let started = update_success_outcome(
            ServerType::PowerShelf,
            vec!["Task-PowerShelf".to_string()],
            json!({"Output": [{"Id": "Task-PowerShelf", "Result": "success"}]}),
        );
        assert!(matches!(
            started,
            FirmwareUpdateOutcome::Started(TaskHandle { task_id })
                if task_id == "Task-PowerShelf"
        ));

        let started_from_details = update_success_outcome(
            ServerType::PowerShelf,
            vec![],
            json!({"Output": [{"Id": "Task-PowerShelf", "Result": "success"}]}),
        );
        assert!(matches!(
            started_from_details,
            FirmwareUpdateOutcome::Started(TaskHandle { task_id })
                if task_id == "Task-PowerShelf"
        ));

        let skipped = update_success_outcome(
            ServerType::PowerShelf,
            vec![],
            json!({
                "Output": [{
                    "Skipped": true,
                    "message": "powershelf no update required"
                }]
            }),
        );
        assert!(matches!(
            skipped,
            FirmwareUpdateOutcome::Skipped { reason }
                if reason == "powershelf no update required"
        ));
    }

    #[test]
    fn powershelf_on_reset_handoff_maps_to_completed_task_status() {
        let status = task_status_from_details_for_server(
            ServerType::PowerShelf,
            "0",
            json!({
                "TaskState": "Cancelled",
                "TaskStatus": "Warning",
                "PercentComplete": 100,
                "Messages": [
                    {
                        "Message": "The task with Id '0' has completed with errors.",
                        "MessageId": "TaskEvent.1.0.3.TaskAborted",
                        "MessageSeverity": "Critical",
                        "Resolution": "None."
                    }
                ]
            }),
        );

        assert_eq!(status.state, TaskState::Completed);
        assert_eq!(status.progress_percent, Some(100));
        assert!(status
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("activation required"));
    }

    #[test]
    fn powershelf_old_liteon_pmc_handoff_maps_to_completed_task_status() {
        let details = json!({
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

        let status =
            task_status_from_details_for_server(ServerType::PowerShelf, "1", details.clone());

        assert_eq!(status.state, TaskState::Completed);
        assert_eq!(status.progress_percent, Some(100));
        let message = status.message.as_deref().unwrap_or_default();
        assert!(message.contains("activation required"));
        assert!(message.contains("internal service error"));

        let non_powershelf = task_status_from_details_for_server(ServerType::GB200, "1", details);
        assert_eq!(non_powershelf.state, TaskState::Running);
        assert_eq!(non_powershelf.progress_percent, Some(0));
    }

    #[test]
    fn powershelf_on_reset_handoff_is_platform_specific() {
        let status = task_status_from_details_for_server(
            ServerType::GB200,
            "0",
            json!({
                "TaskState": "Cancelled",
                "TaskStatus": "Warning",
                "PercentComplete": 100,
                "Messages": [{
                    "Message": "The task with Id '0' has completed with errors.",
                    "MessageId": "TaskEvent.1.0.3.TaskAborted"
                }]
            }),
        );

        assert_eq!(status.state, TaskState::Cancelled);
    }

    #[test]
    fn phase_4b6_activation_requests_map_to_supported_commands() {
        let cases = [
            (
                ServerType::GB200,
                ActivationMode::SingleCommand(crate::workflow::ActivationCommand::RfPowerOff),
                "RF_PWR_OFF",
            ),
            (
                ServerType::GB200Switch,
                ActivationMode::SwitchPowerCycle,
                "NVUE_PWR_CYCLE",
            ),
            (
                ServerType::PowerShelf,
                ActivationMode::PowerShelfReset { force: false },
                "RF_PWRSHELF_RESET",
            ),
            (
                ServerType::PowerShelf,
                ActivationMode::PowerShelfReset { force: true },
                "RF_PWRSHELF_RESET_FORCE",
            ),
        ];

        for (server_type, mode, command) in cases {
            assert_eq!(
                activation_command_for_mode(server_type, &mode).unwrap(),
                command
            );
        }

        assert!(matches!(
            activation_command_for_mode(ServerType::GB200, &ActivationMode::SwitchPowerCycle),
            Err(NvFwUpdError::Unsupported("switch power-cycle activation"))
        ));
        assert!(matches!(
            activation_command_for_mode(ServerType::PowerShelf, &ActivationMode::FullGb200Compute),
            Err(NvFwUpdError::Unsupported(
                "full GB200 compute activation workflow"
            ))
        ));
    }

    #[test]
    fn dgxrubin_activation_command_matrix_matches_dgx_behavior() {
        assert!(activation_command_supported(
            ServerType::DGXRubin,
            "RF_PWR_ON"
        ));
        assert!(activation_command_supported(
            ServerType::DGXRubin,
            "RF_PWR_OFF"
        ));
        assert!(activation_command_supported(
            ServerType::DGXRubin,
            "RF_PWR_CYCLE"
        ));
        assert!(activation_command_supported(
            ServerType::DGXRubin,
            "RF_PWR_STATUS"
        ));
        assert!(!activation_command_supported(
            ServerType::DGXRubin,
            "RF_AUX_PWR_CYCLE"
        ));
    }

    #[test]
    fn phase_4b6_error_mapping_uses_typed_categories() {
        let parse_error = update_failure_error(
            1,
            &[],
            Some(("/tmp/fw.fwpkg".to_string(), "bad".to_string())),
            &json!({}),
        );
        assert!(matches!(
            parse_error,
            NvFwUpdError::PackageParse { path, message }
                if path == "/tmp/fw.fwpkg" && message == "bad"
        ));

        let task_error = update_failure_error(
            1,
            &["Task-1".to_string()],
            None,
            &json!({"Output": [{"Id": "Task-1", "status": "bad image"}]}),
        );
        assert!(matches!(
            task_error,
            NvFwUpdError::TaskFailed {
                task_id: Some(task_id),
                message,
            } if task_id == "Task-1"
                && message.contains("firmware update failed with status 1")
                && message.contains("bad image")
        ));

        let invalid_response = task_status_from_details("Task-Unknown", json!({}));
        assert_eq!(invalid_response.state, TaskState::Unknown);
        assert_eq!(invalid_response.message, None);

        let unsupported =
            activation_command_for_mode(ServerType::HGX, &ActivationMode::SwitchPowerCycle)
                .unwrap_err();
        assert!(matches!(
            unsupported,
            NvFwUpdError::Unsupported("switch power-cycle activation")
        ));
    }

    #[test]
    fn activation_modes_map_to_supported_commands() {
        assert_eq!(
            activation_command_for_mode(ServerType::GB200Switch, &ActivationMode::SwitchPowerCycle)
                .unwrap(),
            "NVUE_PWR_CYCLE"
        );
        assert_eq!(
            activation_command_for_mode(
                ServerType::PowerShelf,
                &ActivationMode::PowerShelfReset { force: true },
            )
            .unwrap(),
            "RF_PWRSHELF_RESET_FORCE"
        );
        assert!(matches!(
            activation_command_for_mode(ServerType::GB200, &ActivationMode::FullGb200Compute),
            Err(NvFwUpdError::Unsupported(_))
        ));
        assert!(matches!(
            activation_command_for_mode(
                ServerType::HGX,
                &ActivationMode::SingleCommand(crate::workflow::ActivationCommand::RfPowerCycle),
            ),
            Err(NvFwUpdError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn full_gb200_compute_activation_rejects_non_compute_targets_without_io() {
        let err = activate_firmware(
            TargetConfig {
                ip: "127.0.0.1".to_string(),
                username: "user".to_string(),
                password: "password".to_string(),
                port: Some(443),
                server_type: ServerType::PowerShelf,
                verify_tls: false,
                ssh_known_hosts: None,
                ssh_host_key_mode: crate::workflow::SshHostKeyMode::TrustOnFirstUse,
            },
            ActivationRequest {
                mode: ActivationMode::FullGb200Compute,
                cancellation: None,
            },
        )
        .await
        .expect_err("full GB200 activation should reject non-compute targets");

        assert!(matches!(
            err,
            NvFwUpdError::Unsupported("full GB200 compute activation workflow")
        ));
    }

    #[tokio::test]
    async fn activation_command_reports_failed_redfish_post() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Systems/System_0"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/System_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Actions": {
                    "#ComputerSystem.Reset": {
                        "target": "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset"
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
            ))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "error": {"message": "reset rejected"}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );
        let err = run_activation_command(&mut target, "RF_PWR_OFF")
            .await
            .expect_err("failed Redfish activation POST must fail the facade");

        assert!(matches!(
            err,
            NvFwUpdError::TaskFailed {
                task_id: None,
                message,
            } if message == "activation command RF_PWR_OFF failed with status 1"
        ));
    }

    #[tokio::test]
    async fn activation_command_retry_succeeds_after_initial_failure() {
        let mut target = RetryingActivationTarget::new([1, 0]);

        run_activation_command_with_retry(&mut target, "RF_PWR_OFF", 3, 0)
            .await
            .expect("second activation attempt should succeed");

        assert_eq!(target.commands, vec!["RF_PWR_OFF", "RF_PWR_OFF"]);
    }

    #[tokio::test]
    async fn activation_command_retry_succeeds_on_final_retry() {
        let mut target = RetryingActivationTarget::new([1, 1, 1, 0]);

        run_activation_command_with_retry(&mut target, "RF_PWR_OFF", 3, 0)
            .await
            .expect("third retry should succeed");

        assert_eq!(
            target.commands,
            vec!["RF_PWR_OFF", "RF_PWR_OFF", "RF_PWR_OFF", "RF_PWR_OFF"]
        );
    }

    #[tokio::test]
    async fn compute_activation_retry_policy_applies_to_aux_and_power_on() {
        let mut target = RetryingActivationTarget::new([1, 0, 1, 0]);

        run_compute_activation_command(&mut target, "RF_AUX_PWR_CYCLE")
            .await
            .expect("aux power cycle retry should succeed");
        run_compute_activation_command(&mut target, "RF_PWR_ON")
            .await
            .expect("power-on retry should succeed");

        assert_eq!(
            target.commands,
            vec![
                "RF_AUX_PWR_CYCLE",
                "RF_AUX_PWR_CYCLE",
                "RF_PWR_ON",
                "RF_PWR_ON"
            ]
        );
    }

    #[tokio::test]
    async fn compute_power_on_wait_completes_when_system_zero_is_on() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Systems/System_0"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/System_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "PowerState": "On"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock-compute");
        let mut target = RetryingActivationTarget::with_access([], access);
        wait_for_compute_power_on_with_policy(&mut target, 3, 0, 1)
            .await
            .expect("System_0 reporting On should complete activation");

        assert!(target.commands.is_empty());
        server.verify().await;
    }

    #[tokio::test]
    async fn compute_power_on_wait_resends_accepted_command_until_system_turns_on() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Systems/System_0"}
                ]
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/System_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "PowerState": "Off"
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/System_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "PowerState": "On"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock-compute");
        let mut target = RetryingActivationTarget::with_access([0], access);
        wait_for_compute_power_on_with_policy(&mut target, 3, 0, 1)
            .await
            .expect("periodic resend should allow an initially ignored power-on to recover");

        assert_eq!(target.commands, vec!["RF_PWR_ON"]);
        server.verify().await;
    }

    #[tokio::test]
    async fn compute_power_on_wait_times_out_when_system_remains_off() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Systems/System_0"}
                ]
            })))
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/System_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "PowerState": "Off"
            })))
            .expect(3)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock-compute");
        let mut target = RetryingActivationTarget::with_access([0, 0], access);
        let err = wait_for_compute_power_on_with_policy(&mut target, 3, 0, 1)
            .await
            .expect_err("an Off system should time out after periodic resends");

        assert!(matches!(
            err,
            NvFwUpdError::Timeout {
                operation: "wait for compute system power state On",
                seconds: 0,
            }
        ));
        assert_eq!(target.commands, vec!["RF_PWR_ON", "RF_PWR_ON"]);
        server.verify().await;
    }

    #[tokio::test]
    async fn activation_command_retry_returns_final_failure() {
        let mut target = RetryingActivationTarget::new([1, 1, 1, 1]);

        let err = run_activation_command_with_retry(&mut target, "RF_PWR_OFF", 3, 0)
            .await
            .expect_err("failure after all retries should be returned");

        assert_eq!(
            target.commands,
            vec!["RF_PWR_OFF", "RF_PWR_OFF", "RF_PWR_OFF", "RF_PWR_OFF"]
        );
        assert!(matches!(
            err,
            NvFwUpdError::TaskFailed {
                task_id: None,
                message,
            } if message == "activation command RF_PWR_OFF failed with status 1"
        ));
    }

    #[tokio::test]
    async fn switch_activation_accepts_running_power_cycle_action() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

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
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "switch rebooting"}
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "ready"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let summary = activate_switch_power_cycle_with_target(legacy_switch_target(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                "mock-switch",
                AccessType::NVSwitch,
            ),
        ))
        .await
        .expect("running NVUE power-cycle action should be accepted");

        assert_eq!(summary.details["action_id"], "job-power-cycle");
        assert_eq!(summary.details["action"]["state"], "running");
        assert_eq!(
            summary.details["recovery"]["endpoint"],
            SWITCH_ACTIVATION_RECOVERY_PATH
        );
        assert_eq!(summary.details["recovery"]["attempts"], 2);
    }

    #[tokio::test]
    async fn powershelf_activation_waits_for_recovery_after_reset() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Managers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Managers/powershelf"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Managers/powershelf"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Actions": {
                    "#Manager.Reset": {
                        "target": "/redfish/v1/Managers/powershelf/Actions/Manager.Reset"
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Managers/powershelf/Actions/Manager.Reset",
            ))
            .and(body_string_contains("GracefulRestart"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "Reset": "accepted"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(POWERSHELF_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "powershelf rebooting"}
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(POWERSHELF_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "RedfishVersion": "1.0.0"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let summary = activate_powershelf_reset_with_access(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            "RF_PWRSHELF_RESET",
        )
        .await
        .expect("PowerShelf reset activation should wait for recovery");

        assert_eq!(summary.details["command"], "RF_PWRSHELF_RESET");
        assert_eq!(
            summary.details["recovery"]["endpoint"],
            POWERSHELF_ACTIVATION_RECOVERY_PATH
        );
        assert_eq!(summary.details["recovery"]["attempts"], 2);
        assert_eq!(summary.details["recovery"]["observed_by"], "workflow");
    }

    #[tokio::test]
    async fn powershelf_activation_skips_second_wait_when_reset_post_already_observed_recovery() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Managers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Managers/powershelf"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Managers/powershelf"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Actions": {
                    "#Manager.Reset": {
                        "target": "http://127.0.0.1:9/redfish/v1/Managers/powershelf/Actions/Manager.Reset"
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(POWERSHELF_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "powershelf rebooting"}
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(POWERSHELF_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "RedfishVersion": "1.0.0"
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        let summary = activate_powershelf_reset_with_access(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            "RF_PWRSHELF_RESET",
        )
        .await
        .expect("PowerShelf reset activation should reuse internally observed recovery");

        assert_eq!(summary.details["command"], "RF_PWRSHELF_RESET");
        assert_eq!(summary.details["recovery"]["attempts"], 2);
        assert_eq!(
            summary.details["recovery"]["observed_by"],
            "activation_command"
        );
    }

    #[tokio::test]
    async fn switch_activation_error_preserves_nvue_action_details() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

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
                "status": "power cycle failed",
                "detail": "bad activation state"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let err = activate_switch_power_cycle_with_target(legacy_switch_target(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                "mock-switch",
                AccessType::NVSwitch,
            ),
        ))
        .await
        .expect_err("failed NVUE action must fail switch activation");

        assert!(matches!(
            err,
            NvFwUpdError::TaskFailed {
                task_id: Some(action_id),
                message,
            } if action_id == "job-power-cycle"
                && message.contains("NVUE_PWR_CYCLE")
                && message.contains("action_failed")
                && message.contains("power cycle failed")
                && message.contains("bad activation state")
        ));
    }

    #[tokio::test]
    async fn switch_activation_error_reports_missing_nvue_action_id() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/nvue_v1/system"))
            .and(body_string_contains("@power-cycle"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let err = activate_switch_power_cycle_with_target(legacy_switch_target(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                "mock-switch",
                AccessType::NVSwitch,
            ),
        ))
        .await
        .expect_err("missing action id must fail switch activation");

        assert!(matches!(
            err,
            NvFwUpdError::InvalidResponse {
                context: "NVUE power-cycle activation",
                message,
            } if message.contains("missing NVUE action id")
                && message.contains("POST /nvue_v1/system")
        ));
    }

    #[test]
    fn update_cmd_args_preserves_structured_request_fields() {
        let request = FirmwareUpdateRequest {
            component: "BMC".to_string(),
            firmware_file: "/tmp/fw.fwpkg".to_string(),
            force_update: true,
            options: crate::workflow::UpdateOptions {
                staged: StagedMode::StageAndActivate,
                special_json: Some(json!({"Existing": true})),
                oem_parameters: Some(json!({"Oem": {"Nvidia": {"Foo": "Bar"}}})),
                liteon_device_id: Some("PSU-1".to_string()),
                apply_time: Some("OnReset".to_string()),
            },
            cancellation: None,
        };

        let cmd_args = update_cmd_args(ServerType::GB200, &request);
        assert!(cmd_args.quiet);
        assert!(cmd_args.staged_activate_update);
        assert_eq!(
            cmd_args.oem_parameters.as_ref().unwrap()[0],
            json!({"Oem": {"Nvidia": {"Foo": "Bar"}}}).to_string()
        );

        let special: Value = serde_json::from_str(&cmd_args.special.as_ref().unwrap()[0]).unwrap();
        assert_eq!(special["Existing"], true);
        assert_eq!(special["ForceUpdate"], true);
        assert_eq!(special["ApplyTime"], "OnReset");
        assert_eq!(special["LiteOnPowerDeviceId"], "PSU-1");
        assert_eq!(special["Targets"], json!([]));
    }

    #[test]
    fn update_cmd_args_maps_gb200_generic_bmc_to_bmc_full_targets() {
        for server_type in [ServerType::GB200, ServerType::GB300, ServerType::VRNVL72] {
            let request = FirmwareUpdateRequest {
                component: "bmc".to_string(),
                firmware_file: "/tmp/fw.fwpkg".to_string(),
                force_update: false,
                options: crate::workflow::UpdateOptions::default(),
                cancellation: None,
            };

            let cmd_args = update_cmd_args(server_type, &request);
            let special: Value =
                serde_json::from_str(&cmd_args.special.as_ref().unwrap()[0]).unwrap();
            assert_eq!(special["Targets"], json!([]));
        }
    }

    #[test]
    fn update_cmd_args_maps_switch_cpld_component_to_cpld1_target() {
        let request = FirmwareUpdateRequest {
            component: "CPLD".to_string(),
            firmware_file: "/tmp/fw.fwpkg".to_string(),
            force_update: false,
            options: crate::workflow::UpdateOptions::default(),
            cancellation: None,
        };

        let cmd_args = update_cmd_args(ServerType::GB200Switch, &request);
        let special: Value = serde_json::from_str(&cmd_args.special.as_ref().unwrap()[0]).unwrap();
        assert_eq!(special["Targets"], json!(["CPLD1"]));
    }

    #[test]
    fn update_cmd_args_maps_gh_style_components_to_http_push_uri_targets() {
        for server_type in [ServerType::HGX, ServerType::MGX] {
            let request = FirmwareUpdateRequest {
                component: "BMC".to_string(),
                firmware_file: "/tmp/fw.fwpkg".to_string(),
                force_update: false,
                options: crate::workflow::UpdateOptions::default(),
                cancellation: None,
            };

            let cmd_args = update_cmd_args(server_type, &request);
            let special: Value =
                serde_json::from_str(&cmd_args.special.as_ref().unwrap()[0]).unwrap();

            assert_eq!(
                special["HttpPushUriTargets"],
                json!(["/redfish/v1/UpdateService/FirmwareInventory/BMC"])
            );
            assert!(special.get("Targets").is_none());
        }
    }

    #[test]
    fn update_cmd_args_keeps_powershelf_pmc_package_level() {
        let request = FirmwareUpdateRequest {
            component: "PMC".to_string(),
            firmware_file: "/tmp/fw.tar".to_string(),
            force_update: true,
            options: crate::workflow::UpdateOptions {
                apply_time: Some("OnReset".to_string()),
                ..Default::default()
            },
            cancellation: None,
        };

        let cmd_args = update_cmd_args(ServerType::PowerShelf, &request);
        let special: Value = serde_json::from_str(&cmd_args.special.as_ref().unwrap()[0]).unwrap();

        assert_eq!(special["ApplyTime"], "OnReset");
        assert_eq!(special["ForceUpdate"], true);
        assert!(special.get("Targets").is_none());
        assert!(special.get("HttpPushUriTargets").is_none());
    }
}
