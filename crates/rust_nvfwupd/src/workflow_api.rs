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
use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use nvue_client::Client as NvueClient;
use redfish_client::RedfishClient;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::bmc_access::{AccessType, BmcAccess, BMC_SESSION_TOKEN_USER};
use crate::dgx_rftarget::DGXRFTarget;
use crate::expected_inventory;
use crate::gb200_rftarget::GB200RFTarget;
use crate::gb200_switch_rftarget::{
    GB200SwitchRFTarget, StatusReadOutcome, SwitchPowerCycleError, SwitchPowerCycleSummary,
};
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
    FirmwareVersionCheckTarget, FlintFirmwareUpdateRequest, FlintFirmwareVersionCheckRequest,
    HostTargetConfig, NvFwUpdError, Result, ServerType, StagedMode, TargetConfig, TaskHandle,
    TaskState, TaskStatus, UpdateSummary,
};

use crate::compute_power::ComputePowerPaths;
use crate::compute_timing::ComputeFirmwareTiming;

mod switch_bmc;

const DEFAULT_REDFISH_TIMEOUT_SECS: u64 = 900;
const DEFAULT_SWITCH_TIMEOUT_SECS: u64 = 1200;
#[cfg(not(test))]
const EXPECTED_INVENTORY_POST_ACTIVATION_RETRY_INTERVAL_SECS: u64 = 120;
#[cfg(test)]
const EXPECTED_INVENTORY_POST_ACTIVATION_RETRY_INTERVAL_SECS: u64 = 0;
const COMPUTE_ACTIVATION_COMMAND_RETRIES: u32 = 10;
#[cfg(not(test))]
const COMPUTE_ACTIVATION_COMMAND_RETRY_DELAY_SECS: u64 = 60;
#[cfg(test)]
const COMPUTE_ACTIVATION_COMMAND_RETRY_DELAY_SECS: u64 = 0;
const COMPUTE_ACTIVATION_POWER_STATE_ATTEMPTS: u64 = 12;
const COMPUTE_ACTIVATION_POWER_STATE_POLL_INTERVAL_SECS: u64 = 5;
const COMPUTE_ACTIVATION_AC_CYCLE_ATTEMPTS: u32 = 2;
#[cfg(not(test))]
const COMPUTE_ACTIVATION_AC_CYCLE_STABILIZATION: Duration = Duration::from_secs(3 * 60);
#[cfg(test)]
const COMPUTE_ACTIVATION_AC_CYCLE_STABILIZATION: Duration = Duration::ZERO;
const LEGACY_COMPUTE_ACTIVATION_POWER_OFF_DELAY_SECS: u64 = 20;
const LEGACY_COMPUTE_ACTIVATION_BMC_WAIT_ATTEMPTS: u64 = 60;
const LEGACY_COMPUTE_ACTIVATION_BMC_WAIT_INTERVAL_SECS: u64 = 5;
const LEGACY_COMPUTE_ACTIVATION_POWER_ON_RESEND_INTERVAL_SECS: u64 = 60;
#[cfg(not(test))]
const COMPUTE_HOST_BOOT_AC_CYCLE_STABILIZATION: Duration = Duration::from_secs(3 * 60);
#[cfg(test)]
const COMPUTE_HOST_BOOT_AC_CYCLE_STABILIZATION: Duration = Duration::ZERO;
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

/// Additive workflow facade for callers that own NVUE and/or switch BMC transports.
///
/// The process CLI and existing library callers can continue using the free
/// workflow functions. Hosted switch callers can attach request-scoped NVUE
/// access without placing TLS material in [`TargetConfig`].
#[derive(Clone, Default)]
pub struct WorkflowContext {
    nvue_client: Option<Arc<NvueClient>>,
    compute_timing: Option<ComputeFirmwareTiming>,
    compute_power_paths: ComputePowerPaths,
    switch_bmc_client: Option<RedfishClient>,
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
            ..Self::default()
        }
    }

    /// Configure this compute workflow without changing other nodes or CLI defaults.
    pub fn with_compute_timing(mut self, timing: ComputeFirmwareTiming) -> Self {
        self.compute_timing = Some(timing);
        self
    }

    /// Configure power endpoints for this compute workflow only.
    /// Empty paths retain existing resource and action discovery.
    pub fn with_compute_power_paths(mut self, paths: ComputePowerPaths) -> Self {
        self.compute_power_paths = paths;
        self
    }

    fn compute_timing(&self) -> ComputeFirmwareTiming {
        self.compute_timing.unwrap_or_else(legacy_compute_timing)
    }

    /// Use a VR NVL72 switch BMC for inventory, comparisons, and power-cycle activation.
    ///
    /// With an NVUE client attached, `TargetConfig` still describes the host for
    /// uploads; otherwise it describes the BMC and NVUE-only operations fail.
    /// BMC failures never fall back to NVUE or reuse the host's credentials.
    /// Other server types reject this BMC context; their workflows remain unchanged.
    ///
    /// ```no_run
    /// # use nvfwupd::workflow_api::WorkflowContext;
    /// # fn example(bmc: redfish_client::RedfishClient) {
    /// let workflow = WorkflowContext::new().with_switch_bmc_client(bmc);
    /// # }
    /// ```
    pub fn with_switch_bmc_client(mut self, client: RedfishClient) -> Self {
        self.switch_bmc_client = Some(client);
        self
    }

    /// Retrieve firmware inventory.
    pub async fn get_firmware_inventory(
        &self,
        target: TargetConfig,
    ) -> Result<Vec<FirmwareComponent>> {
        get_firmware_inventory_with_context(self, target).await
    }

    /// Retrieve all installed instances of one logical switch firmware component.
    ///
    /// For example, `EROT` includes every reported ERoT, never the BMC firmware itself.
    pub async fn get_switch_firmware_inventory(
        &self,
        target: TargetConfig,
        component: &str,
    ) -> Result<Vec<FirmwareComponent>> {
        if !is_switch(target.server_type) {
            return Err(NvFwUpdError::Unsupported("switch component inventory"));
        }
        let server_type = target.server_type;
        Ok(self
            .get_firmware_inventory(target)
            .await?
            .into_iter()
            .filter(|entry| {
                firmware_inventory_target_matches(
                    server_type,
                    component,
                    entry.inventory_path.as_deref().unwrap_or(&entry.name),
                )
            })
            .collect())
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

    /// Push a recovery bundle without expected-inventory or update precondition checks.
    pub async fn update_firmware_skipping_preconditions(
        &self,
        target: TargetConfig,
        request: FirmwareUpdateRequest,
    ) -> Result<FirmwareUpdateOutcome> {
        update_firmware_skipping_preconditions_with_context(self, target, request).await
    }

    /// Run recovery power and presence checks using this context's power paths and timing.
    pub async fn run_compute_recovery_power_block(
        &self,
        target: TargetConfig,
        presence: &[String],
    ) -> Result<()> {
        run_compute_recovery_power_block_with_context(self, target, presence).await
    }

    /// Run a firmware activation request.
    pub async fn activate_firmware(
        &self,
        target: TargetConfig,
        request: ActivationRequest,
    ) -> Result<ActivationSummary> {
        activate_firmware_with_context(self, target, request).await
    }

    /// Ensure the GB200 host is ready using this context's power paths and timing.
    pub async fn ensure_gb200_host_os_ready(
        &self,
        host: HostTargetConfig,
        target: TargetConfig,
        cancellation: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<ActivationSummary> {
        ensure_gb200_host_os_ready_with_context(self, host, target, cancellation).await
    }

    /// Update GB200 NICs with context-preserving device-discovery recovery.
    pub async fn update_gb200_flint_firmware(
        &self,
        host: HostTargetConfig,
        target: TargetConfig,
        request: FlintFirmwareUpdateRequest,
    ) -> Result<FirmwareUpdateOutcome> {
        update_gb200_flint_firmware_with_context(self, host, target, request).await
    }

    /// Verify and, when needed, repair the NVOS ASIC firmware auto-update configuration.
    pub async fn ensure_nvos_asic_firmware_config(&self, target: TargetConfig) -> Result<()> {
        ensure_nvos_asic_firmware_config_with_context(self, target).await
    }

    fn target_builder<'a>(&'a self, target: &'a TargetConfig) -> WorkflowTargetBuilder<'a> {
        let mut builder =
            WorkflowTargetBuilder::with_nvue_client(target, self.nvue_client.as_ref());
        builder.compute_timing = self.compute_timing;
        builder.compute_power_paths = self.compute_power_paths.clone();
        builder.switch_bmc_client = self.switch_bmc_client.as_ref();
        builder
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
        bmc_client: Option<RedfishClient>,
    },
}

impl WorkflowTarget {
    fn switch_bmc_client(&self) -> Option<&RedfishClient> {
        match self {
            Self::Switch { bmc_client, .. } => bmc_client.as_ref(),
            Self::Standard(_) => None,
        }
    }

    fn require_nvue_for_switch(&self) -> Result<()> {
        if matches!(self, Self::Switch { .. })
            && self.target_access().access_type != AccessType::NVSwitch
        {
            return Err(NvFwUpdError::Unsupported(
                "NVUE operation through a BMC-only switch context",
            ));
        }
        Ok(())
    }

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
            ..
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
            ..
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
            ..
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
            ..
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
            ..
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
        self.require_nvue_for_switch()?;
        let Self::Switch { target, client, .. } = self else {
            return Err(NvFwUpdError::Unsupported("switch power-cycle activation"));
        };

        let power_cycle = match client {
            Some(client) => target.run_nvue_power_cycle_with_client(client).await,
            None => target.run_nvue_power_cycle().await,
        };

        // An accepted action can reboot NVUE before its status is readable.
        // Only a missed response or a reboot signal from that window is an
        // outage. A status HTTP error by itself is not. Recovery polling then
        // decides whether the switch came back, including when it is already
        // back. A rejected request or an explicit action failure still stops
        // activation here.
        match power_cycle {
            Ok(summary) => Ok(summary),
            Err(SwitchPowerCycleError::ActionStatusUnavailable {
                action_id,
                details,
                saw_outage,
                cause,
            }) => {
                tracing::warn!(
                    action_id = %action_id,
                    saw_outage,
                    cause = cause.as_deref().unwrap_or("none"),
                    details = %NvUtils::redact_secret_json_value(&details),
                    "NVUE power-cycle action status was not readable; continuing recovery"
                );
                Ok(SwitchPowerCycleSummary {
                    action_id,
                    details,
                    status_read: StatusReadOutcome {
                        saw_outage,
                        was_readable: false,
                        // An unreadable status never claimed to be running, so
                        // a missed response stays the outage evidence here.
                        action_pending: false,
                    },
                })
            }
            Err(error) => Err(map_switch_power_cycle_error(error)),
        }
    }

    async fn ensure_nvos_asic_firmware_config(&self) -> Result<()> {
        self.require_nvue_for_switch()?;
        let Self::Switch { target, client, .. } = self else {
            return Err(NvFwUpdError::Unsupported(
                "NVOS ASIC firmware auto-update configuration",
            ));
        };

        let result = match client {
            Some(client) => {
                target
                    .ensure_nvos_asic_firmware_config_with_client(client)
                    .await
            }
            None => target.ensure_nvos_asic_firmware_config().await,
        };

        result.map_err(NvFwUpdError::Message)
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
    compute_timing: Option<ComputeFirmwareTiming>,
    compute_power_paths: ComputePowerPaths,
    switch_bmc_client: Option<&'a RedfishClient>,
}

impl<'a> WorkflowTargetBuilder<'a> {
    fn new(target: &'a TargetConfig) -> Self {
        Self {
            target,
            nvue_client: None,
            compute_timing: None,
            compute_power_paths: ComputePowerPaths::default(),
            switch_bmc_client: None,
        }
    }

    fn with_nvue_client(
        target: &'a TargetConfig,
        nvue_client: Option<&'a Arc<NvueClient>>,
    ) -> Self {
        Self {
            target,
            nvue_client,
            compute_timing: None,
            compute_power_paths: ComputePowerPaths::default(),
            switch_bmc_client: None,
        }
    }

    fn build_target(&self) -> Result<WorkflowTarget> {
        if self.switch_bmc_client.is_some() && self.target.server_type != ServerType::VRNVL72Switch
        {
            return Err(NvFwUpdError::Unsupported(
                "BMC switch workflows require VR NVL72",
            ));
        }
        let config_dict = None;

        if is_switch(self.target.server_type) {
            let access = if self.switch_bmc_client.is_some() && self.nvue_client.is_none() {
                self.build_bmc_access()?
            } else {
                self.build_nvue_access()?
            };
            let target = GB200SwitchRFTarget::new(access, config_dict);

            if let Some(client) = self.nvue_client {
                self.validate_hosted_target(client)?;
            }

            return Ok(WorkflowTarget::Switch {
                target: Box::new(target),
                client: self.nvue_client.cloned(),
                bmc_client: self.switch_bmc_client.cloned(),
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
            ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72 | ServerType::LP30 => {
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
        if is_switch(self.target.server_type) && self.switch_bmc_client.is_none() {
            return Err(NvFwUpdError::Unsupported(
                "BMC access construction for an NVUE switch target",
            ));
        }

        let session_token = self.target.bmc_session_token();
        let has_user_pass = !self.target.username.is_empty() && !self.target.password.is_empty();

        if session_token.is_none() && !has_user_pass {
            return Err(NvFwUpdError::AuthFailed {
                target: self.target.ip.clone(),
                message: "target credentials must not be empty".to_string(),
            });
        }

        if let Some(token) = session_token {
            HeaderValue::try_from(token).map_err(|error| NvFwUpdError::AuthFailed {
                target: self.target.ip.clone(),
                message: format!("invalid Redfish session token: {error}"),
            })?;
        }

        tracing::debug!(
            credential_auth_type = if session_token.is_some() {
                "session_token"
            } else {
                "user_pass"
            },
            "building NVFWUPD BMC access"
        );

        // BmcAccess carries the token in `password`, paired with the `user`
        // sentinel; TargetConfig keeps it in `session_token`.
        let (user, password) = match session_token {
            Some(token) => (BMC_SESSION_TOKEN_USER.to_owned(), token.to_owned()),
            None => (self.target.username.clone(), self.target.password.clone()),
        };

        self.build_bmc_access_common(
            user,
            password,
            AccessType::Login,
            self.timeout_secs(),
            session_token.is_some(),
        )
    }

    fn build_nvue_access(&self) -> Result<BmcAccess> {
        if !is_switch(self.target.server_type) {
            return Err(NvFwUpdError::Unsupported(
                "NVUE access for a non-switch target",
            ));
        }

        if self.target.username.is_empty() || self.target.password.is_empty() {
            return Err(NvFwUpdError::AuthFailed {
                target: self.target.ip.clone(),
                message: "direct NVOS targets require username/password credentials".to_string(),
            });
        }

        self.build_bmc_access_common(
            self.target.username.clone(),
            self.target.password.clone(),
            AccessType::NVSwitch,
            DEFAULT_SWITCH_TIMEOUT_SECS,
            false,
        )
    }

    /// Shared `BmcAccess` construction for both the BMC Redfish path and the
    /// NVUE switch path: build the client, derive `base_url`, and assemble
    /// the struct. Credential fields, access type, and per-path client
    /// options (timeout, redirect policy) are supplied by the caller.
    fn build_bmc_access_common(
        &self,
        user: String,
        password: String,
        access_type: AccessType,
        timeout_secs: u64,
        disable_redirects: bool,
    ) -> Result<BmcAccess> {
        let mut client_builder = BmcAccess::client_builder(timeout_secs);
        if let Some(timing) = self.compute_timing {
            client_builder = client_builder.connect_timeout(timing.connect_timeout);
        }

        if !self.target.verify_tls {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        if disable_redirects {
            client_builder = client_builder.redirect(reqwest::redirect::Policy::none());
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
            user,
            password,
            model: String::new(),
            partnumber: String::new(),
            serialnumber: String::new(),
            port,
            servertype: server_type_arg(self.target.server_type).to_string(),
            base_url,
            transport_type: "https".to_string(),
            allow_http: self.target.allow_http,
            access_type,
            ssh_known_hosts: self.target.ssh_known_hosts.clone(),
            ssh_host_key_mode: self.target.ssh_host_key_mode.as_target_arg().to_string(),
            client,
            compute_timing: self.compute_timing,
            compute_power_paths: self.compute_power_paths.clone(),
        })
    }

    fn timeout_secs(&self) -> u64 {
        if is_switch(self.target.server_type) {
            DEFAULT_SWITCH_TIMEOUT_SECS
        } else {
            self.compute_timing
                .map_or(DEFAULT_REDFISH_TIMEOUT_SECS, |timing| {
                    timing.request_timeout.as_secs()
                })
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

/// Verify and, when needed, repair the NVOS ASIC firmware auto-update configuration.
pub async fn ensure_nvos_asic_firmware_config(target: TargetConfig) -> Result<()> {
    WorkflowContext::new()
        .ensure_nvos_asic_firmware_config(target)
        .await
}

async fn ensure_nvos_asic_firmware_config_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
) -> Result<()> {
    context
        .target_builder(&target)
        .build_target()?
        .ensure_nvos_asic_firmware_config()
        .await
}

async fn get_firmware_inventory_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
) -> Result<Vec<FirmwareComponent>> {
    let rf_target = context.target_builder(&target).build_target()?;
    let inventory = fetch_firmware_inventory_map(&rf_target, &target.ip, None).await?;

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
    if let Some(bmc) = rf_target.switch_bmc_client() {
        return switch_bmc::firmware_inventory(bmc).await;
    }
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
    if let Some(bmc) = rf_target.switch_bmc_client() {
        let inventory = switch_bmc::firmware_inventory(bmc).await?;
        let mut names = expected_inventory::present_ap_names(&inventory);
        // Preserve raw Redfish AP identities and support existing NVUE-named profiles.
        names.extend(inventory.keys().map(|uri| normalize_switch_component(uri)));
        return Ok(names);
    }
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
    let timing = rf_target
        .target_access()
        .compute_timing
        .unwrap_or_else(legacy_compute_timing);
    for attempt in 0..=timing.inventory_recovery_retries {
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

        if attempt < timing.inventory_recovery_retries {
            tracing::warn!(
                attempt = attempt + 1,
                retries = timing.inventory_recovery_retries,
                "expected firmware inventory was not ready after activation; retrying"
            );
            tokio::time::sleep(timing.inventory_recovery_retry_interval).await;
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
                checks.push((scope, None, json!({}), false));
                continue;
            }

            return Err(NvFwUpdError::PackageParse {
                path: firmware_file,
                message,
            });
        }

        let has_package_versions = !pkg_parser.apname_version_dict().is_empty();
        let pre_update_scope = match &scope {
            FirmwareVersionCheckScope::Package if is_switch(target.server_type) => {
                Some(switch_package_version_check_scope(
                    target.server_type,
                    pkg_parser.apname_version_dict(),
                ))
            }
            _ => None,
        };
        checks.push((
            scope,
            pre_update_scope,
            json!(pkg_parser.apname_version_dict()),
            has_package_versions,
        ));
        pkg_parser.remove_files().await;
    }

    if checks
        .iter()
        .all(|(_, _, _, has_package_versions)| !has_package_versions)
    {
        return Ok(FirmwareVersionCheckSummary {
            matched: true,
            details: json!({
                "status": "unverified",
                "all_targets_matched": false,
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
    let mut all_targets_matched = true;
    for (scope, pre_update_scope, package_versions, _) in checks {
        let mut scoped_inventory = inventory.clone();
        let missing_components =
            scope_firmware_inventory(target.server_type, &scope, &mut scoped_inventory);
        let checked_components = compare_inventory_to_package_versions(
            rf_target.as_rf_target(),
            &scoped_inventory,
            &package_versions,
            rf_target.switch_bmc_client().is_some(),
        )
        .await;
        // Package completeness is required only to skip an update. Keep the
        // original scope and comparison rows for post-activation verification:
        // package APs absent from inventory must not become new mismatches.
        let all_targets_present = if let Some(pre_update_scope) = pre_update_scope {
            scope_firmware_inventory(target.server_type, &pre_update_scope, &mut scoped_inventory)
                .is_empty()
        } else {
            missing_components.is_empty()
        };
        // A pre-update skip needs positive evidence for every package/target,
        // before overlapping rows from later packages replace earlier rows.
        all_targets_matched &= all_targets_present
            && !scoped_inventory.is_empty()
            && checked_components
                .iter()
                .filter(|component| {
                    component["inventory_path"]
                        .as_str()
                        .is_some_and(|path| scoped_inventory.contains_key(path))
                })
                .all(|component| component["status"] == "matched");
        merge_firmware_version_components(
            &mut components,
            checked_components,
            scope.replaces_concrete_with_unverified(),
        );
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
            "all_targets_matched": all_targets_matched,
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

/// Flash and immediately activate VRNVL72 compute LinkX modules with guarded preflight.
pub async fn update_linkx_firmware(
    host: HostTargetConfig,
    request: crate::flint::LinkxUpdateRequest,
) -> Result<crate::flint::LinkxUpdateReport> {
    crate::flint::update_linkx_firmware(host, request).await
}

/// Update host-visible ConnectX or BlueField NIC firmware with Flint.
pub async fn update_flint_firmware(
    host: HostTargetConfig,
    request: FlintFirmwareUpdateRequest,
) -> Result<FirmwareUpdateOutcome> {
    crate::flint::update_firmware(host, request).await
}

/// Update GB200 host-visible firmware, with one pre-flash device-discovery recovery cycle.
pub async fn update_gb200_flint_firmware(
    host: HostTargetConfig,
    target: TargetConfig,
    request: FlintFirmwareUpdateRequest,
) -> Result<FirmwareUpdateOutcome> {
    WorkflowContext::new()
        .update_gb200_flint_firmware(host, target, request)
        .await
}

async fn update_gb200_flint_firmware_with_context(
    context: &WorkflowContext,
    host: HostTargetConfig,
    target: TargetConfig,
    request: FlintFirmwareUpdateRequest,
) -> Result<FirmwareUpdateOutcome> {
    if target.server_type != ServerType::GB200 {
        return Err(NvFwUpdError::Unsupported(
            "GB200 Flint device-discovery recovery workflow",
        ));
    }

    let mut rf_target = context.target_builder(&target).build_target()?;
    let mut recovery = Gb200FlintDeviceDiscoveryRecovery {
        rf_target: rf_target.as_rf_target_mut(),
        host: host.clone(),
        cancellation: request.cancellation.clone(),
    };
    crate::flint::update_firmware_with_discovery_recovery(host, request, &mut recovery).await
}

/// Compare host-visible ConnectX or BlueField NIC versions with Flint images.
pub async fn verify_flint_firmware_versions(
    host: HostTargetConfig,
    request: FlintFirmwareVersionCheckRequest,
) -> Result<FirmwareVersionCheckSummary> {
    crate::flint::verify_firmware_versions(host, request).await
}

/// Ensure a GB200 compute host is booted before a host-side Flint workflow.
pub async fn ensure_gb200_host_os_ready(
    host: HostTargetConfig,
    target: TargetConfig,
    cancellation: Option<tokio_util::sync::CancellationToken>,
) -> Result<ActivationSummary> {
    WorkflowContext::new()
        .ensure_gb200_host_os_ready(host, target, cancellation)
        .await
}

async fn ensure_gb200_host_os_ready_with_context(
    context: &WorkflowContext,
    host: HostTargetConfig,
    target: TargetConfig,
    cancellation: Option<tokio_util::sync::CancellationToken>,
) -> Result<ActivationSummary> {
    if target.server_type != ServerType::GB200 {
        return Err(NvFwUpdError::Unsupported(
            "GB200 host OS boot readiness workflow",
        ));
    }
    if cancellation
        .as_ref()
        .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
    {
        return Err(NvFwUpdError::Cancelled {
            operation: "GB200 host OS boot readiness",
        });
    }

    let mut rf_target = context.target_builder(&target).build_target()?;
    ensure_gb200_host_os_ready_with(
        rf_target.as_rf_target_mut(),
        || crate::flint::wait_for_host_os_ready(host.clone(), cancellation.clone()),
        COMPUTE_HOST_BOOT_AC_CYCLE_STABILIZATION,
    )
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
    rf_target.require_nvue_for_switch()?;
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
                timeout: context.compute_timing().background_copy_timeout,
                interval: context.compute_timing().background_copy_poll_interval,
            },
            Some(&mut json_output),
        )
        .await
        .map_err(|message| NvFwUpdError::TaskFailed {
            task_id: None,
            message,
        })?;

    run_update_monitor_core(&mut rf_target, &target, request, json_output).await
}

/// The push-and-monitor core shared by the normal update (which runs the
/// expected-inventory and precondition gates first) and the recovery push
/// (which must not: a component in recovery state cannot answer them).
async fn run_update_monitor_core(
    rf_target: &mut WorkflowTarget,
    target: &TargetConfig,
    request: FirmwareUpdateRequest,
    mut json_output: Value,
) -> Result<FirmwareUpdateOutcome> {
    let recipe_list = vec![request.firmware_file.clone()];
    let mut pkg_parser =
        pldm::get_pkg_parser(&request.firmware_file, false, false, Some(&json_output)).await;

    let cmd_args = update_cmd_args(target.server_type, &request);
    let nvfwupd_owned_monitoring = owns_update_monitoring(target.server_type);
    let monitoring_timeout_secs =
        target_update_timeout_secs(target.server_type, &request.firmware_file);
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

/// Push a firmware bundle without the expected-inventory and update
/// precondition checks, for recovery flows. A component in recovery state
/// cannot answer a background-copy or presence check. The normal checks
/// would stall against the very condition that starts a recovery.
pub async fn update_firmware_skipping_preconditions(
    target: TargetConfig,
    request: FirmwareUpdateRequest,
) -> Result<FirmwareUpdateOutcome> {
    WorkflowContext::new()
        .update_firmware_skipping_preconditions(target, request)
        .await
}

async fn update_firmware_skipping_preconditions_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
    request: FirmwareUpdateRequest,
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
    let json_output = json!({"Error": [], "Error Code": 0, "Output": []});
    run_update_monitor_core(&mut rf_target, &target, request, json_output).await
}

/// The compute OOB recovery flow's power block:
/// power off, aux cycle, power on, then wait for the named components to be
/// present. The whole cycle gets one shared re-run when either half fails
/// (`CYCLE_ATTEMPTS` = 2 total = initial + one retry).
pub async fn run_compute_recovery_power_block(
    target: TargetConfig,
    presence: &[String],
) -> Result<()> {
    WorkflowContext::new()
        .run_compute_recovery_power_block(target, presence)
        .await
}

async fn run_compute_recovery_power_block_with_context(
    context: &WorkflowContext,
    target: TargetConfig,
    presence: &[String],
) -> Result<()> {
    const CYCLE_ATTEMPTS: u32 = 2;

    let mut last_error = None;
    for attempt in 1..=CYCLE_ATTEMPTS {
        match activate_full_compute(context, target.clone()).await {
            Ok(_) => {
                let rf_target = context.target_builder(&target).build_target()?;
                match fetch_post_activation_inventory(&rf_target, &target.ip, Some(presence)).await
                {
                    Ok(_) => return Ok(()),
                    Err(error) => last_error = Some(error),
                }
            }
            Err(error) => last_error = Some(error),
        }
        if attempt < CYCLE_ATTEMPTS {
            if let Some(error) = &last_error {
                tracing::warn!(
                    attempt,
                    error = %error,
                    "recovery power block attempt failed; re-running the cycle"
                );
            }
        }
    }

    Err(last_error.expect("loop ran at least once"))
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
    rf_target.require_nvue_for_switch()?;
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
    if let Some(bmc) = &context.switch_bmc_client {
        if target.server_type != ServerType::VRNVL72Switch {
            return Err(NvFwUpdError::Unsupported(
                "BMC switch workflows require VR NVL72",
            ));
        }
        if request.mode != ActivationMode::SwitchPowerCycle {
            return Err(NvFwUpdError::Unsupported("switch BMC activation mode"));
        }
        return switch_bmc::activate(bmc, request.cancellation).await;
    }

    if request
        .cancellation
        .as_ref()
        .map(|token| token.is_cancelled())
        .unwrap_or(false)
    {
        return Err(NvFwUpdError::Message("activation cancelled".to_string()));
    }

    match request.mode {
        ActivationMode::FullGb200Compute => activate_full_compute(context, target).await,
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
        &action_id,
        SWITCH_ACTIVATION_RECOVERY_PATH,
        "wait for switch after activation",
        SWITCH_ACTIVATION_RECOVERY_ATTEMPTS,
        SWITCH_ACTIVATION_RECOVERY_INTERVAL_SECS,
        summary.status_read,
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

/// Poll `path` until the target goes down and returns, or the budget runs out.
///
/// Completing needs a reachable probe that follows an outage, so a target that
/// never stops answering never completes here. `status_read` only decides which
/// state the wait starts in.
///
/// ```text
///            start: saw_outage && !action_pending
///              false |                    | true
///                    v                    v
///          +------------------+  down  +------------------+
///          | A not seen down  |------->| B seen down      |
///          +------------------+        +------------------+
///            |                           |
///            | up: keep polling          | up: Ok(attempt)
///            | out of budget:            | out of budget: Timeout
///            |   Timeout, or             |
///            |   TaskFailed if the       |
///            |   status was unreadable   |
/// ```
///
/// Starting at B is what lets a switch that already rebooted and came back
/// finish on its first probe. [`StatusReadOutcome`] tabulates which reads earn
/// that start, and why a still-running action does not.
async fn wait_for_workflow_target_recovery(
    target: &WorkflowTarget,
    action_id: &str,
    path: &'static str,
    operation: &'static str,
    attempts: u32,
    interval_secs: u64,
    status_read: StatusReadOutcome,
) -> Result<u32> {
    // An earlier outage is enough to accept a switch that is already back, but
    // only once its action reached a terminal state. While the action is still
    // running it has yet to take the switch down, so recovery has to see that
    // outage for itself rather than reuse one from before the status read.
    let mut observed_unreachable = status_read.saw_outage && !status_read.action_pending;

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

    // Only an unreadable status proves the switch answered for the whole wait,
    // which means its action failed rather than left a reboot outstanding.
    // Every other case keeps the timeout that drives the BMC auxiliary AC cycle.
    if observed_unreachable || status_read.was_readable {
        Err(NvFwUpdError::Timeout {
            operation,
            seconds: recovery_wait_budget_secs(attempts, interval_secs),
        })
    } else {
        Err(NvFwUpdError::TaskFailed {
            task_id: Some(action_id.to_string()),
            message: format!(
                "firmware endpoint {path} stayed reachable; action status never showed an outage"
            ),
        })
    }
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

/// Dispatch the full compute sequence to the implementation for the selected driver family.
async fn activate_full_compute(
    context: &WorkflowContext,
    target: TargetConfig,
) -> Result<ActivationSummary> {
    if !matches!(
        target.server_type,
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72 | ServerType::LP30
    ) {
        return Err(NvFwUpdError::Unsupported(
            "full compute activation workflow",
        ));
    }
    if target.server_type != ServerType::GB200 {
        return activate_full_compute_legacy(context, target).await;
    }

    let mut rf_target = context.target_builder(&target).build_target()?;
    activate_full_compute_gb200(context, rf_target.as_rf_target_mut()).await
}

/// Run the verified Off/AC-cycle/On sequence with the selected timing policy.
async fn activate_full_compute_gb200(
    context: &WorkflowContext,
    rf_target: &mut (dyn RFTarget + Send + Sync),
) -> Result<ActivationSummary> {
    let power_attempts = context
        .compute_timing
        .map_or(COMPUTE_ACTIVATION_POWER_STATE_ATTEMPTS, |timing| {
            timing.power_on_attempts as u64
        });
    let power_interval = context.compute_timing.map_or(
        COMPUTE_ACTIVATION_POWER_STATE_POLL_INTERVAL_SECS,
        |timing| timing.power_on_interval.as_secs(),
    );
    let ac_cycle_stabilization = context
        .compute_timing
        .map_or(COMPUTE_ACTIVATION_AC_CYCLE_STABILIZATION, |timing| {
            timing.ac_cycle_stabilization
        });
    let max_ac_cycle_attempts = context
        .compute_timing
        .map_or(COMPUTE_ACTIVATION_AC_CYCLE_ATTEMPTS, |timing| {
            timing.ac_cycle_attempts
        });
    let ac_cycle_attempts = activate_full_compute_gb200_with_policy(
        rf_target,
        power_attempts,
        power_interval,
        ac_cycle_stabilization,
        max_ac_cycle_attempts,
    )
    .await?;

    Ok(ActivationSummary {
        message: "full GB200 compute activation completed".to_string(),
        details: json!({
            "commands": ["RF_PWR_OFF", "RF_AUX_PWR_CYCLE", "RF_PWR_ON"],
            "verified_power_states": ["Off", "On"],
            "power_state_timeout_seconds": power_attempts.saturating_mul(power_interval),
            "ac_cycle_stabilization_seconds": ac_cycle_stabilization.as_secs(),
            "max_ac_cycle_attempts": max_ac_cycle_attempts,
            "ac_cycle_attempts": ac_cycle_attempts,
        }),
    })
}

/// Preserve the existing BMC-readiness and power-on sequence for other compute drivers.
async fn activate_full_compute_legacy(
    context: &WorkflowContext,
    target: TargetConfig,
) -> Result<ActivationSummary> {
    let timing = context.compute_timing();
    let mut rf_target = context.target_builder(&target).build_target()?;
    run_compute_activation_command(rf_target.as_rf_target_mut(), "RF_PWR_OFF").await?;
    tokio::time::sleep(timing.power_off_delay).await;
    run_compute_activation_command(rf_target.as_rf_target_mut(), "RF_AUX_PWR_CYCLE").await?;
    wait_for_compute_bmc(context, &target).await?;
    run_compute_activation_command(rf_target.as_rf_target_mut(), "RF_PWR_ON").await?;
    wait_for_compute_power_on(rf_target.as_rf_target_mut()).await?;

    Ok(ActivationSummary {
        message: "full compute activation completed".to_string(),
        details: json!({
            "commands": ["RF_PWR_OFF", "RF_AUX_PWR_CYCLE", "RF_PWR_ON"],
            "bmc_wait_seconds": u64::from(timing.bmc_recovery_attempts)
                * timing.bmc_recovery_interval.as_secs(),
            "power_on_resend_interval_seconds": timing.power_on_resend_interval.as_secs(),
            "verified_power_state": "On",
        }),
    })
}

/// Retry complete auxiliary cycles when BMC readiness or verified power-on times out.
async fn activate_full_compute_gb200_with_policy(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    power_state_attempts: u64,
    power_state_poll_interval_secs: u64,
    ac_cycle_stabilization: Duration,
    ac_cycle_attempts: u32,
) -> Result<u32> {
    run_compute_activation_command(rf_target, "RF_PWR_OFF").await?;
    wait_for_compute_power_state_with_timeout(
        rf_target,
        "Off",
        power_state_attempts,
        power_state_poll_interval_secs,
    )
    .await?;

    // Unconfigured library callers retain their historical sequence. Profile-backed
    // callers additionally opt into the power-off delay and BMC readiness probe.
    if let Some(timing) = rf_target.target_access().compute_timing {
        tokio::time::sleep(timing.power_off_delay).await;
    }
    let ac_cycle_attempts = ac_cycle_attempts.max(1);
    for ac_cycle_attempt in 1..=ac_cycle_attempts {
        run_compute_activation_command(rf_target, "RF_AUX_PWR_CYCLE").await?;
        tokio::time::sleep(ac_cycle_stabilization).await;
        let recovery = async {
            if let Some(timing) = rf_target.target_access().compute_timing {
                wait_for_compute_bmc_with_access(rf_target.target_access(), timing).await?;
            }
            run_compute_activation_command(rf_target, "RF_PWR_ON").await?;
            wait_for_compute_power_state_with_timeout(
                rf_target,
                "On",
                power_state_attempts,
                power_state_poll_interval_secs,
            )
            .await
        }
        .await;

        match recovery {
            Ok(()) => return Ok(ac_cycle_attempt),
            Err(error)
                if matches!(error, NvFwUpdError::Timeout { .. })
                    && ac_cycle_attempt < ac_cycle_attempts =>
            {
                tracing::warn!(
                    attempt = ac_cycle_attempt,
                    max_attempts = ac_cycle_attempts,
                    error = %error,
                    "compute activation recovery timed out; retrying the auxiliary AC cycle"
                );
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("the activation loop always executes at least once")
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
    let timing = rf_target
        .target_access()
        .compute_timing
        .unwrap_or_else(legacy_compute_timing);
    run_compute_activation_command_with_timing(rf_target, command, timing).await
}

async fn run_compute_activation_command_with_timing(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    command: &'static str,
    timing: ComputeFirmwareTiming,
) -> Result<()> {
    run_activation_command_with_retry(
        rf_target,
        command,
        timing.activation_command_retries,
        timing.activation_command_retry_interval.as_secs(),
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

async fn wait_for_compute_bmc(context: &WorkflowContext, target: &TargetConfig) -> Result<()> {
    let access = context.target_builder(target).build_bmc_access()?;
    wait_for_compute_bmc_with_access(&access, context.compute_timing()).await
}

async fn wait_for_compute_bmc_with_access(
    access: &BmcAccess,
    timing: ComputeFirmwareTiming,
) -> Result<()> {
    for _ in 0..timing.bmc_recovery_attempts {
        tokio::time::sleep(timing.bmc_recovery_interval).await;
        let (ok, _) = access
            .dispatch_request_full(
                "GET",
                access
                    .compute_power_paths
                    .bmc_ready_path
                    .as_deref()
                    .unwrap_or("/redfish/v1"),
                None,
                None,
                timing.recovery_probe_timeout.as_secs(),
                true,
                None,
            )
            .await;
        if ok {
            return Ok(());
        }
    }

    Err(NvFwUpdError::Timeout {
        operation: "wait for GB200 compute BMC after activation",
        seconds: timing.bmc_recovery_attempts as u64 * timing.bmc_recovery_interval.as_secs(),
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
    let systems = access
        .compute_power_system_members(TraceFlags::default())
        .await;
    let system_uri = preferred_compute_system_uri(&systems)?;
    let (ok, response) = access
        .dispatch_request_full(
            "GET",
            system_uri,
            None,
            None,
            access
                .compute_timing
                .unwrap_or_else(legacy_compute_timing)
                .recovery_probe_timeout
                .as_secs(),
            true,
            None,
        )
        .await;
    if !ok {
        return None;
    }

    serde_json::from_value::<ComputeSystemPowerState>(response)
        .ok()
        .map(|system| system.power_state)
}

async fn ensure_gb200_host_os_ready_with<F, Fut>(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    mut wait_for_host_os: F,
    ac_cycle_stabilization: Duration,
) -> Result<ActivationSummary>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<u32>>,
{
    let power_state = query_compute_power_state(rf_target.target_access()).await;
    let initial_power_on_requested = !power_state
        .as_deref()
        .is_some_and(|state| state.eq_ignore_ascii_case("On"));
    if initial_power_on_requested {
        run_activation_command(rf_target, "RF_PWR_ON").await?;
    }

    match wait_for_host_os().await {
        Ok(attempts) => {
            return Ok(ActivationSummary {
                message: "GB200 host OS is ready".to_owned(),
                details: json!({
                    "initial_power_state": power_state.as_deref(),
                    "initial_power_on_requested": initial_power_on_requested,
                    "host_probe_attempts": attempts,
                    "recovery_ac_cycle": false,
                }),
            });
        }
        Err(error)
            if !matches!(
                &error,
                NvFwUpdError::HostUnreachable { .. } | NvFwUpdError::Timeout { .. }
            ) =>
        {
            return Err(error);
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "GB200 host did not boot; retrying once after an auxiliary AC cycle"
            );
        }
    }

    run_activation_command(rf_target, "RF_AUX_PWR_CYCLE").await?;
    tokio::time::sleep(ac_cycle_stabilization).await;
    run_activation_command(rf_target, "RF_PWR_ON").await?;
    let attempts = wait_for_host_os().await?;

    Ok(ActivationSummary {
        message: "GB200 host OS is ready after auxiliary AC-cycle recovery".to_owned(),
        details: json!({
            "initial_power_state": power_state.as_deref(),
            "initial_power_on_requested": initial_power_on_requested,
            "host_probe_attempts_after_recovery": attempts,
            "recovery_ac_cycle": true,
            "recovery_commands": ["RF_AUX_PWR_CYCLE", "RF_PWR_ON"],
            "ac_cycle_stabilization_seconds": ac_cycle_stabilization.as_secs(),
        }),
    })
}

async fn wait_for_compute_power_on(rf_target: &mut (dyn RFTarget + Send + Sync)) -> Result<()> {
    let timing = rf_target
        .target_access()
        .compute_timing
        .unwrap_or_else(legacy_compute_timing);
    let resend_poll_count = timing
        .power_on_resend_interval
        .as_secs()
        .div_ceil(timing.power_on_interval.as_secs().max(1))
        .max(1);

    wait_for_compute_power_on_with_policy(
        rf_target,
        timing.power_on_attempts as u64,
        timing.power_on_interval.as_secs(),
        resend_poll_count,
    )
    .await
}

struct Gb200FlintDeviceDiscoveryRecovery<'a> {
    rf_target: &'a mut (dyn RFTarget + Send + Sync),
    host: HostTargetConfig,
    cancellation: Option<tokio_util::sync::CancellationToken>,
}

#[async_trait::async_trait]
impl crate::flint::FlintDeviceDiscoveryRecovery for Gb200FlintDeviceDiscoveryRecovery<'_> {
    async fn recover(&mut self) -> Result<()> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
        {
            return Err(NvFwUpdError::Cancelled {
                operation: "GB200 Flint device discovery recovery",
            });
        }

        recover_gb200_host_after_flint_discovery_failure_with(
            self.rf_target,
            || crate::flint::wait_for_host_os_ready(self.host.clone(), self.cancellation.clone()),
            COMPUTE_HOST_BOOT_AC_CYCLE_STABILIZATION,
        )
        .await
    }
}

async fn recover_gb200_host_after_flint_discovery_failure_with<F, Fut>(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    wait_for_host_os: F,
    ac_cycle_stabilization: Duration,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<u32>>,
{
    run_activation_command(rf_target, "RF_AUX_PWR_CYCLE").await?;
    tokio::time::sleep(ac_cycle_stabilization).await;
    run_activation_command(rf_target, "RF_PWR_ON").await?;
    wait_for_host_os().await?;
    Ok(())
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
                resend_interval_secs = resend_poll_count * poll_interval_secs,
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

async fn wait_for_compute_power_state_with_timeout(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    expected_power_state: &'static str,
    attempts: u64,
    poll_interval_secs: u64,
) -> Result<()> {
    let timeout_secs = attempts.saturating_mul(poll_interval_secs);
    if timeout_secs == 0 {
        return wait_for_compute_power_state_with_policy(
            rf_target,
            expected_power_state,
            attempts,
            poll_interval_secs,
        )
        .await;
    }

    match tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        wait_for_compute_power_state_with_policy(
            rf_target,
            expected_power_state,
            attempts,
            poll_interval_secs,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(compute_power_state_timeout(
            expected_power_state,
            timeout_secs,
        )),
    }
}

async fn wait_for_compute_power_state_with_policy(
    rf_target: &mut (dyn RFTarget + Send + Sync),
    expected_power_state: &'static str,
    attempts: u64,
    poll_interval_secs: u64,
) -> Result<()> {
    // Preserve the configured resend policy inside the same wall-clock timeout
    // and AC-cycle retry envelope used by the GB200 activation workflow.
    if expected_power_state == "On" {
        if let Some(timing) = rf_target.target_access().compute_timing {
            let resend_poll_count = timing
                .power_on_resend_interval
                .as_secs()
                .div_ceil(poll_interval_secs.max(1))
                .max(1);
            return wait_for_compute_power_on_with_policy(
                rf_target,
                attempts,
                poll_interval_secs,
                resend_poll_count,
            )
            .await;
        }
    }
    let mut last_power_state = None;

    for _ in 0..attempts {
        tokio::time::sleep(Duration::from_secs(poll_interval_secs)).await;

        let observed_power_state = query_compute_power_state(rf_target.target_access()).await;
        if observed_power_state
            .as_deref()
            .is_some_and(|state| state.eq_ignore_ascii_case(expected_power_state))
        {
            return Ok(());
        }

        if observed_power_state.is_some() {
            last_power_state = observed_power_state;
        }
    }

    tracing::warn!(
        expected_power_state,
        power_state = last_power_state.as_deref().unwrap_or("unavailable"),
        "compute system did not reach the expected power state during activation"
    );
    Err(compute_power_state_timeout(
        expected_power_state,
        attempts.saturating_mul(poll_interval_secs),
    ))
}

fn compute_power_state_timeout(expected_power_state: &'static str, seconds: u64) -> NvFwUpdError {
    let operation = match expected_power_state {
        "Off" => "wait for compute system power state Off",
        "On" => "wait for compute system power state On",
        _ => "wait for compute system power state",
    };

    NvFwUpdError::Timeout { operation, seconds }
}

fn legacy_compute_timing() -> ComputeFirmwareTiming {
    ComputeFirmwareTiming {
        activation_command_retry_interval: Duration::from_secs(
            COMPUTE_ACTIVATION_COMMAND_RETRY_DELAY_SECS,
        ),
        inventory_recovery_retry_interval: Duration::from_secs(
            EXPECTED_INVENTORY_POST_ACTIVATION_RETRY_INTERVAL_SECS,
        ),
        ..ComputeFirmwareTiming::default()
    }
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
        ServerType::LP30 => "lp30",
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

fn target_update_timeout_secs(server_type: ServerType, firmware_file: &str) -> u64 {
    if is_switch(server_type) && crate::nvos_image::is_nvos_image_path(firmware_file) {
        GB200SwitchRFTarget::default_nvos_sftp_upload_timeout_secs()
    } else {
        target_monitoring_timeout_secs(server_type)
    }
}

fn server_type_allows_unparsed_package_update(server_type: ServerType) -> bool {
    matches!(
        server_type,
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72 | ServerType::LP30
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

        let update_targets = version_check_update_targets(server_type, &target.component);
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

fn switch_cpld_count(server_type: ServerType) -> u32 {
    match server_type {
        ServerType::GB200Switch => 4,
        ServerType::GB300Switch => 3,
        ServerType::VRNVL72Switch => 2,
        _ => 0,
    }
}

fn version_check_components(server_type: ServerType, update_target: String) -> Vec<String> {
    let cpld_count = switch_cpld_count(server_type);
    if update_target.eq_ignore_ascii_case("cpld1") && cpld_count > 0 {
        return (1..=cpld_count)
            .map(|index| format!("CPLD{index}"))
            .collect();
    }
    vec![update_target]
}

/// Limit a whole-switch package comparison to components carried by that package.
fn switch_package_version_check_scope(
    server_type: ServerType,
    package_versions: &HashMap<String, indexmap::IndexMap<String, Vec<String>>>,
) -> FirmwareVersionCheckScope {
    let components = package_versions
        .values()
        .flat_map(|package| package.keys())
        .map(|ap_name| GB200SwitchRFTarget::get_target_apname(ap_name))
        .flat_map(|target| version_check_components(server_type, target))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    FirmwareVersionCheckScope::Components(components)
}

fn version_check_update_targets(server_type: ServerType, component: &str) -> Vec<String> {
    if matches!(
        server_type,
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72 | ServerType::LP30
    ) && component.eq_ignore_ascii_case("bmc")
    {
        return vec![component_to_update_target(server_type, "FW_BMC_0")];
    }

    component_update_targets(server_type, component)
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
        let inventory_component = normalize_switch_component(inventory_path);
        // Redfish exposes a combined CPLD_0 while NVUE exposes CPLD1..N.
        if inventory_component == "cpld" {
            return target == "cpld"
                || target
                    .strip_prefix("cpld")
                    .and_then(|index| index.parse::<u32>().ok())
                    .is_some_and(|index| (1..=switch_cpld_count(server_type)).contains(&index));
        }
        if target == "erot" {
            return inventory_component.starts_with("erot");
        }
        return target == inventory_component;
    }

    target
        .trim_end_matches('/')
        .eq_ignore_ascii_case(inventory_path.trim_end_matches('/'))
}

fn normalize_switch_component(component: &str) -> String {
    let combined_cpld = component
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .is_some_and(|leaf| {
            ["CPLD_0", "FW_CPLD_0", "MGX_FW_CPLD_0", "HGX_FW_CPLD_0"]
                .iter()
                .any(|name| leaf.eq_ignore_ascii_case(name))
        });
    let component = GB200SwitchRFTarget::normalize_inventory_component(component);

    match component.as_str() {
        "cpld" if !combined_cpld => "cpld1".to_owned(),
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
    switch_bmc_inventory: bool,
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
        } else if if switch_bmc_inventory {
            switch_firmware_versions_match(&ap_name, &package_version, &system_version)
        } else {
            firmware_versions_exactly_match(&package_version, &system_version)
        } {
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

/// Compare switch firmware versions, accepting the BMC's combined CPLD labels.
///
/// Other components retain exact, case-insensitive version comparison.
/// ```
/// assert!(nvfwupd::workflow_api::switch_firmware_versions_match(
///     "CPLD_0", "000472_REV0006_000450_REV0009",
///     "CPLD000472_REV0006_CPLD000450_REV0009"));
/// ```
pub fn switch_firmware_versions_match(component: &str, left: &str, right: &str) -> bool {
    if !GB200SwitchRFTarget::normalize_inventory_component(component).starts_with("cpld") {
        return firmware_versions_exactly_match(left, right);
    }
    // Only ignore explicit CPLD labels, separators, and zero-padding. Keep REV
    // and segment order: a different device revision must remain a mismatch.
    let normalize = |version: &str| {
        version
            .trim()
            .to_ascii_lowercase()
            .replace("cpld", "")
            .split('_')
            .map(|part| {
                let (prefix, digits) = part.strip_prefix("rev").map_or(("", part), |v| ("rev", v));
                if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
                    format!(
                        "{prefix}{}",
                        digits
                            .parse::<u64>()
                            .map_or_else(|_| digits.to_owned(), |n| n.to_string())
                    )
                } else {
                    part.to_owned()
                }
            })
            .collect::<Vec<_>>()
    };
    normalize(left) == normalize(right)
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

// The workflow core supplies one target and one firmware file per call.
// CLI multi-target aggregation does not pass through this conversion.
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

    if let Some(diagnostic) = output_entries(details)
        .iter()
        .rev()
        .find_map(|entry| entry.get("SubmissionDiagnostic"))
        .filter(|_| task_ids.is_empty())
    {
        return NvFwUpdError::SubmissionFailed {
            details: NvUtils::redact_secret_json_value(diagnostic),
        };
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
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72 | ServerType::LP30
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
        ServerType::GB200 | ServerType::GB300 | ServerType::VRNVL72 | ServerType::LP30 => matches!(
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
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn target(server_type: ServerType) -> TargetConfig {
        TargetConfig {
            ip: "192.0.2.44".to_string(),
            username: "admin".to_string(),
            password: "secret".to_string(),
            session_token: None,
            port: Some(443),
            server_type,
            verify_tls: false,
            allow_http: false,
            ssh_known_hosts: None,
            ssh_host_key_mode: crate::workflow::SshHostKeyMode::TrustOnFirstUse,
        }
    }

    fn legacy_switch_target(access: BmcAccess) -> WorkflowTarget {
        WorkflowTarget::Switch {
            target: Box::new(GB200SwitchRFTarget::new(access, None)),
            client: None,
            bmc_client: None,
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
            compare_inventory_to_package_versions(&target, &inventory, &json!({}), false).await;

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
            compare_inventory_to_package_versions(&target, &inventory, &json!({}), false).await;

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
            compare_inventory_to_package_versions(&target, &selected_inventory, &json!({}), false)
                .await;

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
            compare_inventory_to_package_versions(&target, &package_inventory, &json!({}), false)
                .await;

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

    async fn check_switch_package_versions(
        packages: &[(&str, &[(&str, &str)])],
        inventory: Value,
    ) -> FirmwareVersionCheckSummary {
        use crate::gb200_switch_rftarget::tests::{
            build_synthetic_switch_pldm_package, SyntheticSwitchComponent,
        };

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/firmware"))
            .respond_with(ResponseTemplate::new(200).set_body_json(inventory))
            .expect(1)
            .mount(&server)
            .await;
        let tmp = tempfile::tempdir().unwrap();
        let mut targets = Vec::new();
        for (index, (component, versions)) in packages.iter().enumerate() {
            let payloads = versions
                .iter()
                .map(|(ap_name, version)| SyntheticSwitchComponent {
                    ap_name,
                    version,
                    payload: b"firmware",
                })
                .collect::<Vec<_>>();
            let file = tmp.path().join(format!("switch-{index}.fwpkg"));
            tokio::fs::write(
                &file,
                build_synthetic_switch_pldm_package("GB200-SWITCH", &payloads),
            )
            .await
            .unwrap();
            targets.push(version_check_target(component, file.to_str().unwrap()));
        }
        let (context, target) = hosted_switch_context(&server);
        let summary = context
            .verify_firmware_target_versions(target, targets)
            .await
            .unwrap();
        assert!(server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == wiremock::http::Method::Get));
        summary
    }

    #[tokio::test]
    async fn switch_bmc_context_rejects_gb200_and_gb300_before_any_request() {
        let bmc = MockServer::start().await;
        let bmc_client = RedfishClient::new(
            "127.0.0.1",
            bmc.address().port(),
            redfish_client::BmcCredentials::token("test-bmc-token".to_owned()),
            false,
            false,
        )
        .unwrap();
        let context = WorkflowContext::new().with_switch_bmc_client(bmc_client);
        for server_type in [ServerType::GB200Switch, ServerType::GB300Switch] {
            assert!(matches!(
                context.get_firmware_inventory(target(server_type)).await,
                Err(NvFwUpdError::Unsupported(_))
            ));
            assert!(matches!(
                context
                    .activate_firmware(
                        target(server_type),
                        ActivationRequest {
                            mode: ActivationMode::SwitchPowerCycle,
                            cancellation: None,
                        }
                    )
                    .await,
                Err(NvFwUpdError::Unsupported(_))
            ));
        }
        assert!(bmc.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn switch_bmc_package_comparison_preserves_erot_instances_and_combined_cpld() {
        use crate::gb200_switch_rftarget::tests::{
            build_synthetic_switch_pldm_package, SyntheticSwitchComponent,
        };
        {
            let (server_type, family) = (ServerType::VRNVL72Switch, "VRNVL72-SWITCH");
            let bmc = MockServer::start().await;
            let host = MockServer::start().await;
            let root = "/redfish/v1/UpdateService/FirmwareInventory";
            let entries = [
                ("MGX_FW_BMC_0", "88.1"),
                ("MGX_FW_CPU_0", "1.0"),
                ("MGX_FW_ERoT_BMC_0", "2.0"),
                ("MGX_FW_ERoT_CPU_0", "2.0"),
                ("MGX_FW_NVSwitch_SMA_0", "3.0"),
                ("MGX_FW_NVSwitch_SMA_1", "3.0"),
                ("CPLD_0", "CPLD000472_REV0006_CPLD000450_REV0009"),
            ];
            Mock::given(method("GET")).and(path(root)).and(header("X-Auth-Token", "bmc-only"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Members":entries.iter()
                    .map(|(name, _)| json!({"@odata.id":format!("{root}/{name}")})).collect::<Vec<_>>()})))
                .mount(&bmc).await;
            for (name, version) in entries {
                Mock::given(method("GET"))
                    .and(path(format!("{root}/{name}")))
                    .and(header("X-Auth-Token", "bmc-only"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(json!({"Id":name,"Version":version})),
                    )
                    .mount(&bmc)
                    .await;
            }
            let bmc_client = RedfishClient::new(
                "127.0.0.1",
                bmc.address().port(),
                redfish_client::BmcCredentials::token("bmc-only".to_owned()),
                false,
                false,
            )
            .unwrap();
            let tmp = tempfile::tempdir().unwrap();
            let file = tmp.path().join("switch.fwpkg");
            let payloads = [
                ("BMC", "88.1"),
                ("SBIOS", "1.0"),
                ("ERoT", "2.0"),
                ("CPLD", "000472_REV0006_000450_REV0009"),
                ("SMA", "3.0"),
            ]
            .map(|(ap_name, version)| SyntheticSwitchComponent {
                ap_name,
                version,
                payload: b"firmware",
            });
            tokio::fs::write(
                &file,
                build_synthetic_switch_pldm_package(family, &payloads),
            )
            .await
            .unwrap();
            let targets = ["BMC", "BIOS", "EROT", "CPLD", "SMA"]
                .map(|component| version_check_target(component, file.to_str().unwrap()))
                .to_vec();
            let config = target(server_type).with_bmc_session_token("bmc-only");
            let context = WorkflowContext::new().with_switch_bmc_client(bmc_client.clone());
            let summary = context
                .verify_firmware_target_versions(config, targets.clone())
                .await
                .unwrap();
            assert!(summary.matched, "{}", summary.details);
            assert_eq!(
                summary.details["all_targets_matched"], true,
                "{}",
                summary.details
            );
            assert_eq!(
                summary.details["components"].as_array().unwrap().len(),
                entries.len()
            );

            // The host-backed upload context must use BMC inventory too, including preflight.
            Mock::given(method("GET"))
                .and(path(format!("{root}/MGX_FW_ERoT_CPU_0")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(json!({"Id":"MGX_FW_ERoT_CPU_0","Version":"old"})),
                )
                .with_priority(1)
                .mount(&bmc)
                .await;
            let (context, mut config) = hosted_switch_context(&host);
            config.server_type = server_type;
            let context = context.with_switch_bmc_client(bmc_client);
            let summary = context
                .verify_firmware_target_versions(config.clone(), targets)
                .await
                .unwrap();
            assert!(!summary.matched);
            assert_eq!(summary.details["mismatches"].as_array().unwrap().len(), 1);
            assert_eq!(
                summary.details["mismatches"][0]["name"],
                "MGX_FW_ERoT_CPU_0"
            );
            let workflow_target = context.target_builder(&config).build_target().unwrap();
            validate_expected_inventory_once(
                &workflow_target,
                &config.ip,
                Some(&["MGX_FW_BMC_0".to_owned()]),
                "pre-update",
                None,
            )
            .await
            .unwrap();
            assert!(host.received_requests().await.unwrap().is_empty());
        }
    }

    #[test]
    fn switch_cpld_comparison_preserves_device_order_and_revision() {
        for (left, right, expected) in [
            (
                "CPLD000472_REV0006_CPLD000450_REV0009",
                "000472_REV0006_000450_REV0009",
                true,
            ),
            (
                "CPLD472_REV6_CPLD450_REV9",
                "000472_REV0006_000450_REV0009",
                true,
            ),
            (
                "CPLD472_REV7_CPLD450_REV9",
                "000472_REV0006_000450_REV0009",
                false,
            ),
            (
                "CPLD450_REV9_CPLD472_REV6",
                "000472_REV0006_000450_REV0009",
                false,
            ),
        ] {
            assert_eq!(
                switch_firmware_versions_match("CPLD_0", left, right),
                expected
            );
        }
        assert!(!switch_firmware_versions_match("BMC", "88.01", "88.1"));
        for name in ["CPLD_0", "FW_CPLD_0", "MGX_FW_CPLD_0", "HGX_FW_CPLD_0"] {
            assert_eq!(normalize_switch_component(name), "cpld");
            assert!(firmware_inventory_target_matches(
                ServerType::GB200Switch,
                "CPLD4",
                name
            ));
            assert!(!firmware_inventory_target_matches(
                ServerType::VRNVL72Switch,
                "CPLD4",
                name
            ));
            assert!(!firmware_inventory_target_matches(
                ServerType::GB200Switch,
                "CPLDX",
                name
            ));
        }
    }

    #[tokio::test]
    async fn switch_package_preflight_ignores_components_absent_from_package() {
        let summary = check_switch_package_versions(
            &[("", &[("BMC", "88.1"), ("SMR", "0.24")])],
            json!({
                "BMC": {"actual-firmware": "88.1"},
                "FPGA": {"actual-firmware": "0.24"},
                "BIOS": {"actual-firmware": "old"},
                "ASIC": {"actual-firmware": "unrelated"}
            }),
        )
        .await;
        assert!(summary.matched);
        assert_eq!(summary.details["all_targets_matched"], true);
        let components = summary.details["components"].as_array().unwrap();
        assert_eq!(components.len(), 4);
        for name in ["BIOS", "ASIC"] {
            assert!(components.iter().any(|component| {
                component["name"] == name && component["status"] == "unverified"
            }));
        }
    }

    #[tokio::test]
    async fn switch_package_preflight_requires_all_package_components_in_inventory() {
        let summary = check_switch_package_versions(
            &[("", &[("BMC", "88.1"), ("SBIOS", "1.0")])],
            json!({"BMC": {"actual-firmware": "88.1"}}),
        )
        .await;
        // The shared post-activation result retains package-wide semantics,
        // while the stricter pre-update result prevents an early skip.
        assert!(summary.matched);
        assert_eq!(summary.details["all_targets_matched"], false);
        assert!(summary.details["mismatches"].as_array().unwrap().is_empty());
        assert_eq!(summary.details["components"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn switch_package_verification_still_fails_for_explicit_missing_targets() {
        let summary = check_switch_package_versions(
            &[("BIOS", &[("BMC", "88.1"), ("SBIOS", "1.0")])],
            json!({"BMC": {"actual-firmware": "88.1"}}),
        )
        .await;

        assert!(!summary.matched);
        assert_eq!(summary.details["all_targets_matched"], false);
        assert_eq!(
            summary.details["mismatches"][0]["system_version"],
            "missing"
        );
    }

    #[tokio::test]
    async fn switch_package_verification_preserves_package_wide_mismatches() {
        let summary = check_switch_package_versions(
            &[("", &[("BMC", "88.1"), ("SBIOS", "1.0")])],
            json!({"BMC": {"actual-firmware": "old"}}),
        )
        .await;

        assert!(!summary.matched);
        assert_eq!(summary.details["all_targets_matched"], false);
        let mismatches = summary.details["mismatches"].as_array().unwrap();
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0]["name"], "BMC");
        assert_eq!(mismatches[0]["system_version"], "old");
    }

    #[tokio::test]
    async fn switch_package_preflight_never_skips_an_empty_inventory() {
        let summary = check_switch_package_versions(&[("", &[("BMC", "88.1")])], json!({})).await;

        assert!(summary.matched);
        assert_eq!(summary.details["status"], "unverified");
        assert_eq!(summary.details["all_targets_matched"], false);
        assert!(summary.details["mismatches"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn switch_package_preflight_respects_selected_targets() {
        let summary = check_switch_package_versions(
            &[("BMC", &[("BMC", "88.1"), ("SBIOS", "new")])],
            json!({
                "BMC": {"actual-firmware": "88.1"},
                "BIOS": {"actual-firmware": "old"}
            }),
        )
        .await;
        assert_eq!(summary.details["all_targets_matched"], true);
        assert_eq!(summary.details["components"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn switch_package_preflight_never_treats_unverified_versions_as_current() {
        for inventory in [
            json!({"BMC": {"actual-firmware": "88.1"}, "BIOS": {"actual-firmware": "1.0"}}),
            json!({"BMC": {"actual-firmware": "88.1"}, "BIOS": {}}),
        ] {
            let summary = check_switch_package_versions(
                &[("BMC", &[("BMC", "88.1")]), ("BIOS", &[("BMC", "88.1")])],
                inventory,
            )
            .await;
            assert_eq!(summary.details["all_targets_matched"], false);
            assert!(summary.details["components"]
                .as_array()
                .unwrap()
                .iter()
                .any(|component| component["status"] == "unverified"));
        }
    }

    #[tokio::test]
    async fn switch_package_preflight_checks_every_erot_against_erot_package_version() {
        for (asic_version, expected_match) in [("2.0", true), ("old", false)] {
            let summary = check_switch_package_versions(
                &[("EROT", &[("BMC", "88.1"), ("ERoT", "2.0")])],
                json!({
                    "BMC": {"actual-firmware": "88.1"},
                    "EROT": {"actual-firmware": "2.0"},
                    "EROT-BMC": {"actual-firmware": "2.0"},
                    "EROT-ASIC1": {"actual-firmware": asic_version}
                }),
            )
            .await;
            assert_eq!(summary.details["all_targets_matched"], expected_match);
            let components = summary.details["components"].as_array().unwrap();
            assert_eq!(components.len(), 3);
            assert!(components.iter().all(|c| c["package_version"] == "2.0"));
        }
    }

    #[tokio::test]
    async fn switch_package_preflight_checks_all_split_cpld_versions() {
        for (second_version, expected_match) in [("CPLD3_4", true), ("old", false)] {
            let summary = check_switch_package_versions(
                &[("CPLD1", &[("CPLD", "1_2_3_4_5_6_7_8")])],
                json!({
                    "CPLD1": {"actual-firmware": "CPLD1_2"},
                    "CPLD2": {"actual-firmware": second_version},
                    "CPLD3": {"actual-firmware": "CPLD5_6"},
                    "CPLD4": {"actual-firmware": "CPLD7_8"}
                }),
            )
            .await;
            assert_eq!(summary.details["all_targets_matched"], expected_match);
            assert_eq!(summary.details["components"].as_array().unwrap().len(), 4);
        }
    }

    #[tokio::test]
    async fn switch_package_preflight_does_not_hide_an_earlier_package_mismatch() {
        let summary = check_switch_package_versions(
            &[("BMC", &[("BMC", "old")]), ("BMC", &[("BMC", "88.1")])],
            json!({"BMC": {"actual-firmware": "88.1"}}),
        )
        .await;
        // The post-update result describes the last applied package, but neither
        // package may be skipped unless both comparisons passed beforehand.
        assert!(summary.matched);
        assert_eq!(summary.details["all_targets_matched"], false);
    }

    #[tokio::test]
    async fn switch_package_preflight_uses_erot_from_the_selected_bios_bundle() {
        for (installed_erot, matches) in [("1.5", true), ("2.0", false)] {
            // RMS selects BMC from 0004, but EROT and BIOS from 0006. The
            // unselected EROT version in 0004 must not affect either check.
            let summary = check_switch_package_versions(
                &[
                    ("BMC", &[("BMC", "88.1"), ("ERoT", "2.0")]),
                    ("EROT", &[("SBIOS", "1.0"), ("ERoT", "1.5")]),
                    ("BIOS", &[("SBIOS", "1.0"), ("ERoT", "1.5")]),
                ],
                json!({
                    "BMC": {"actual-firmware": "88.1"},
                    "BIOS": {"actual-firmware": "1.0"},
                    "EROT-BMC": {"actual-firmware": installed_erot},
                    "EROT-ASIC1": {"actual-firmware": installed_erot}
                }),
            )
            .await;
            assert_eq!(summary.matched, matches);
            assert_eq!(summary.details["all_targets_matched"], matches);
            let erots = summary.details["components"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|component| component["name"].as_str().unwrap().starts_with("EROT"))
                .collect::<Vec<_>>();
            assert_eq!(erots.len(), 2);
            assert!(erots
                .iter()
                .all(|component| component["package_version"] == "1.5"));
        }
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
    fn gb200_bmc_version_check_scopes_to_host_bmc_inventory() {
        assert_eq!(
            component_update_targets(ServerType::GB200, "BMC"),
            Vec::<String>::new(),
            "GB200 BMC update payloads use Targets: []"
        );

        let scope = firmware_version_check_scope(
            ServerType::GB200,
            &[version_check_target("BMC", "/tmp/bmc.fwpkg")],
        );
        assert_eq!(
            scope,
            FirmwareVersionCheckScope::Components(vec![
                "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0".to_owned()
            ])
        );
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

        let (ok, msg) = parser
            .parse_pkg("/tmp/generic_raw_bmc_update_signed.ima")
            .await;

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
        assert!(server_type_allows_unparsed_package_update(ServerType::LP30));
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
    async fn build_bmc_access_uses_session_token_authentication() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1"))
            .and(header("X-Auth-Token", "session-token"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let config = target(ServerType::GB200).with_bmc_session_token("session-token");

        let access = WorkflowTargetBuilder::new(&config)
            .build_bmc_access()
            .unwrap();

        assert_eq!(access.access_type, AccessType::Login);

        let response = access
            .apply_auth(access.client.get(format!("{}/redfish/v1", server.uri())))
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());
    }

    #[test]
    fn build_bmc_access_rejects_invalid_session_token_header() {
        let config = target(ServerType::GB200).with_bmc_session_token("invalid\ntoken");

        let result = WorkflowTargetBuilder::new(&config).build_bmc_access();

        assert!(matches!(result, Err(NvFwUpdError::AuthFailed { .. })));
    }

    #[test]
    fn build_bmc_access_rejects_empty_session_token() {
        // An empty token must not be treated as present: it would otherwise
        // build a BmcAccess whose sentinel username pairs with an empty
        // password, silently degrading to unauthenticated Basic auth.
        let config = target(ServerType::GB200).with_bmc_session_token("");

        let result = WorkflowTargetBuilder::new(&config).build_bmc_access();

        assert!(
            matches!(result, Err(NvFwUpdError::AuthFailed { ref message, .. }) if message.contains("must not be empty")),
            "an empty session token should be rejected as empty credentials"
        );
    }

    #[test]
    fn build_bmc_access_rejects_whitespace_only_session_token() {
        let config = target(ServerType::GB200).with_bmc_session_token("   ");

        let result = WorkflowTargetBuilder::new(&config).build_bmc_access();

        assert!(
            matches!(result, Err(NvFwUpdError::AuthFailed { ref message, .. }) if message.contains("must not be empty")),
            "a whitespace-only session token should be rejected as empty credentials"
        );
    }

    #[test]
    fn switch_target_rejects_bmc_session_token() {
        let config = target(ServerType::GB200Switch).with_bmc_session_token("session-token");

        let result = WorkflowTargetBuilder::new(&config).build_target();

        assert!(matches!(result, Err(NvFwUpdError::AuthFailed { .. })));
    }

    #[tokio::test]
    async fn session_token_client_does_not_follow_redirects() {
        let destination = MockServer::start().await;
        let source = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("Location", destination.uri().as_str()),
            )
            .expect(1)
            .mount(&source)
            .await;

        let config = target(ServerType::GB200).with_bmc_session_token("session-token");

        let access = WorkflowTargetBuilder::new(&config)
            .build_bmc_access()
            .unwrap();

        let response = access
            .apply_auth(access.client.get(format!("{}/redirect", source.uri())))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        assert!(destination.received_requests().await.unwrap().is_empty());
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

    #[tokio::test]
    async fn hosted_context_runs_nvos_asic_firmware_preflight() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/firmware/ASIC"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "auto-update": "enabled",
                "fw-source": "default"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/applied"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": "save"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (context, switch_target) = hosted_switch_context(&server);

        context
            .ensure_nvos_asic_firmware_config(switch_target)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn nvos_asic_firmware_preflight_rejects_compute_targets() {
        let error = ensure_nvos_asic_firmware_config(target(ServerType::GB200))
            .await
            .unwrap_err();

        assert!(matches!(error, NvFwUpdError::Unsupported(_)));
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
            (ServerType::LP30, "GB200RFTarget", "lp30", AccessType::Login),
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
            session_token: None,
            port: None,
            server_type: ServerType::GB200,
            verify_tls: true,
            allow_http: true,
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
        assert!(access.allow_http);
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
    fn update_timeout_uses_nvos_default_for_switch_nvos_images() {
        let nvos_image = "nvos-amd64-25.02.4440.bin";

        for server_type in [
            ServerType::GB200Switch,
            ServerType::GB300Switch,
            ServerType::VRNVL72Switch,
        ] {
            assert_eq!(
                target_update_timeout_secs(server_type, nvos_image),
                GB200SwitchRFTarget::default_nvos_sftp_upload_timeout_secs()
            );
        }

        assert_eq!(
            target_update_timeout_secs(ServerType::GB200Switch, "switch-firmware.fwpkg"),
            DEFAULT_SWITCH_TIMEOUT_SECS
        );
        assert_eq!(
            target_update_timeout_secs(ServerType::GB200, nvos_image),
            DEFAULT_REDFISH_TIMEOUT_SECS
        );
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

    #[test]
    fn task_status_from_details_summarizes_identical_image_resource_error() {
        let details = json!({
            "TaskState": "Exception",
            "TaskStatus": "Critical",
            "Messages": [{
                "MessageId": "ResourceEvent.1.0.ResourceErrorsDetected",
                "Severity": "Critical",
                "Message": "The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'.",
                "MessageArgs": ["FW_COMPONENT_0", "Component image is identical"],
                "Resolution": "Retry firmware update operation with the force flag"
            }]
        });

        let status = task_status_from_details("firmware-task", details.clone());

        assert_eq!(status.state, TaskState::Failed);
        assert_eq!(status.details, details);

        assert_eq!(
            status.message.as_deref(),
            Some("The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'. Resolution: Retry firmware update operation with the force flag")
        );
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
    fn submission_diagnostics_survive_workflow_conversion_without_summary_truncation() {
        let diagnostic = json!({"category":"http_rejection", "http_status":409,
            "body":"busy ".repeat(700), "password":"secret-value"});
        let err = update_failure_error(
            1,
            &[],
            None,
            &json!({
                "Output": [{"SubmissionDiagnostic": diagnostic}],
                "Error": ["Failed to acquire task ID from firmware update response"],
            }),
        );
        let NvFwUpdError::SubmissionFailed { details } = err else {
            panic!("expected a submission failure");
        };
        assert_eq!(details["http_status"], 409);
        assert_eq!(details["body"].as_str().unwrap().len(), 3500);
        assert!(!details.to_string().contains("secret-value"));
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
                ActivationMode::SingleCommand(crate::workflow::ActivationCommand::PowerOff),
                "RF_PWR_OFF",
            ),
            (
                ServerType::LP30,
                ActivationMode::SingleCommand(crate::workflow::ActivationCommand::AuxPowerCycle),
                "RF_AUX_PWR_CYCLE",
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
                &ActivationMode::SingleCommand(crate::workflow::ActivationCommand::PowerCycle),
            ),
            Err(NvFwUpdError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn full_compute_activation_rejects_non_compute_targets_without_io() {
        let err = activate_firmware(
            TargetConfig {
                ip: "127.0.0.1".to_string(),
                username: "user".to_string(),
                password: "password".to_string(),
                session_token: None,
                port: Some(443),
                server_type: ServerType::PowerShelf,
                verify_tls: false,
                allow_http: false,
                ssh_known_hosts: None,
                ssh_host_key_mode: crate::workflow::SshHostKeyMode::TrustOnFirstUse,
            },
            ActivationRequest {
                mode: ActivationMode::FullGb200Compute,
                cancellation: None,
            },
        )
        .await
        .expect_err("full compute activation should reject non-compute targets");

        assert!(matches!(
            err,
            NvFwUpdError::Unsupported("full compute activation workflow")
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

    #[test]
    fn compute_power_paths_are_request_local_and_survive_target_construction() {
        let config = target(ServerType::GB200);
        let paths = ComputePowerPaths {
            system_state_path: Some("/redfish/v1/Oem/Host".into()),
            system_reset_path: Some("/redfish/v1/Oem/Reset".into()),
            aux_power_cycle_path: Some("/redfish/v1/Oem/Aux".into()),
            bmc_ready_path: Some("/redfish/v1/Oem/Ready".into()),
        };
        let configured = WorkflowContext::new().with_compute_power_paths(paths.clone());
        assert_eq!(
            configured
                .target_builder(&config)
                .build_bmc_access()
                .unwrap()
                .compute_power_paths,
            paths
        );
        assert_eq!(
            configured
                .target_builder(&config)
                .build_target()
                .unwrap()
                .target_access()
                .compute_power_paths,
            paths
        );
        assert_eq!(
            WorkflowContext::new()
                .target_builder(&config)
                .build_bmc_access()
                .unwrap()
                .compute_power_paths,
            ComputePowerPaths::default()
        );
    }

    #[tokio::test]
    async fn configured_power_paths_route_activation_and_state_checks_without_discovery() {
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "configured-compute");
        access.compute_power_paths = ComputePowerPaths {
            system_state_path: Some("/redfish/v1/Oem/Host".into()),
            system_reset_path: Some("/redfish/v1/Oem/Reset".into()),
            aux_power_cycle_path: Some("/redfish/v1/Oem/Aux".into()),
            bmc_ready_path: Some("/redfish/v1/Oem/Ready".into()),
        };
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Oem/Host"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"PowerState": "On"})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Oem/Ready"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        assert_eq!(
            query_compute_power_state(&access).await.as_deref(),
            Some("On")
        );
        wait_for_compute_bmc_with_access(
            &access,
            ComputeFirmwareTiming {
                bmc_recovery_interval: Duration::ZERO,
                bmc_recovery_attempts: 1,
                ..ComputeFirmwareTiming::default()
            },
        )
        .await
        .unwrap();

        let mut rf_target = GB200RFTarget::new(access, None);
        for (command, reset_type, action_path) in [
            ("RF_PWR_OFF", "ForceOff", "/redfish/v1/Oem/Reset"),
            ("RF_PWR_ON", "On", "/redfish/v1/Oem/Reset"),
            ("RF_PWR_CYCLE", "ForceRestart", "/redfish/v1/Oem/Reset"),
            ("RF_AUX_PWR_CYCLE", "AuxPowerCycle", "/redfish/v1/Oem/Aux"),
        ] {
            Mock::given(method("POST"))
                .and(path(action_path))
                .and(body_json(json!({"ResetType": reset_type})))
                .respond_with(ResponseTemplate::new(204))
                .expect(1)
                .mount(&server)
                .await;
            run_activation_command(&mut rf_target, command)
                .await
                .unwrap();
        }
        assert_eq!(server.received_requests().await.unwrap().len(), 6);
    }

    #[tokio::test]
    async fn configured_power_paths_do_not_fall_back_when_unavailable() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "configured-compute");
        access.compute_power_paths = ComputePowerPaths {
            system_state_path: Some("/redfish/v1/Oem/MissingHost".into()),
            system_reset_path: Some("/redfish/v1/Oem/FailedReset".into()),
            aux_power_cycle_path: Some("/redfish/v1/Oem/FailedAux".into()),
            bmc_ready_path: Some("/redfish/v1/Oem/NotReady".into()),
        };
        for resource in ["/redfish/v1/Oem/MissingHost", "/redfish/v1/Oem/NotReady"] {
            Mock::given(method("GET"))
                .and(path(resource))
                .respond_with(ResponseTemplate::new(404))
                .expect(1)
                .mount(&server)
                .await;
        }
        for action in ["/redfish/v1/Oem/FailedReset", "/redfish/v1/Oem/FailedAux"] {
            Mock::given(method("POST"))
                .and(path(action))
                .respond_with(ResponseTemplate::new(500))
                .expect(1)
                .mount(&server)
                .await;
        }
        assert_eq!(query_compute_power_state(&access).await, None);
        assert!(wait_for_compute_bmc_with_access(
            &access,
            ComputeFirmwareTiming {
                bmc_recovery_interval: Duration::ZERO,
                bmc_recovery_attempts: 1,
                ..ComputeFirmwareTiming::default()
            }
        )
        .await
        .is_err());
        let mut rf_target = GB200RFTarget::new(access, None);
        assert!(run_activation_command(&mut rf_target, "RF_PWR_OFF")
            .await
            .is_err());
        assert!(run_activation_command(&mut rf_target, "RF_AUX_PWR_CYCLE")
            .await
            .is_err());
        assert_eq!(server.received_requests().await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn configured_system_resource_supplies_activation_reset_discovery() {
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "configured-compute");
        access.compute_power_paths.system_state_path = Some("/redfish/v1/Oem/Host".into());
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Oem/Host"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Actions": {"#ComputerSystem.Reset": {"target": "/redfish/v1/Oem/Reset"}}
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Oem/Reset"))
            .and(body_json(json!({"ResetType": "On"})))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        run_activation_command(&mut GB200RFTarget::new(access, None), "RF_PWR_ON")
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[test]
    fn compute_timing_is_request_local_and_survives_target_construction() {
        let config = target(ServerType::GB200);
        let timing = ComputeFirmwareTiming {
            request_timeout: Duration::from_secs(41),
            upload_timeout: Duration::from_secs(43),
            background_copy_timeout: Duration::from_secs(47),
            background_copy_poll_interval: Duration::from_secs(7),
            ..ComputeFirmwareTiming::default()
        };
        let configured = WorkflowContext::new().with_compute_timing(timing);
        let access = configured
            .target_builder(&config)
            .build_bmc_access()
            .unwrap();
        assert_eq!(access.compute_timing, Some(timing));
        let rf_target = configured.target_builder(&config).build_target().unwrap();
        assert_eq!(rf_target.target_access().compute_timing, Some(timing));
        assert_eq!(
            WorkflowContext::new()
                .target_builder(&config)
                .build_bmc_access()
                .unwrap()
                .compute_timing,
            None
        );
    }

    #[tokio::test]
    async fn configured_compute_activation_retry_count_is_enforced() {
        let mut target = RetryingActivationTarget::new([1, 1, 0]);
        let timing = ComputeFirmwareTiming {
            activation_command_retries: 1,
            activation_command_retry_interval: Duration::ZERO,
            ..ComputeFirmwareTiming::default()
        };
        assert!(
            run_compute_activation_command_with_timing(&mut target, "RF_PWR_OFF", timing)
                .await
                .is_err()
        );
        assert_eq!(target.commands.len(), 2);
    }

    #[tokio::test]
    async fn configured_power_on_attempts_are_enforced() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"Members": [{"@odata.id": "/redfish/v1/Systems/System_0"}]}),
                ),
            )
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/System_0"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"PowerState": "PoweringOn"})),
            )
            .expect(2)
            .mount(&server)
            .await;
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "mock-gb200");
        access.compute_timing = Some(ComputeFirmwareTiming {
            power_on_attempts: 2,
            power_on_interval: Duration::ZERO,
            ..ComputeFirmwareTiming::default()
        });
        let mut target = GB200RFTarget::new(access, None);
        assert!(wait_for_compute_power_on(&mut target).await.is_err());
    }

    #[tokio::test]
    async fn configured_power_on_resend_survives_gb200_timeout_wrapper() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Oem/Host"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"PowerState": "Off"})))
            .expect(2)
            .mount(&server)
            .await;
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "configured-compute");
        access.compute_power_paths.system_state_path = Some("/redfish/v1/Oem/Host".into());
        access.compute_timing = Some(ComputeFirmwareTiming {
            power_on_resend_interval: Duration::ZERO,
            ..ComputeFirmwareTiming::default()
        });
        let mut target = RetryingActivationTarget::with_access([0], access);
        assert!(
            wait_for_compute_power_state_with_timeout(&mut target, "On", 2, 0)
                .await
                .is_err()
        );
        assert_eq!(target.commands, ["RF_PWR_ON"]);
        server.verify().await;
    }

    #[tokio::test]
    async fn configured_activation_preserves_off_verification_and_ac_retry() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let server = MockServer::start().await;
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&reads);
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Oem/Host"))
            .respond_with(move |_: &wiremock::Request| {
                let state = if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                    "Off"
                } else {
                    "On"
                };
                ResponseTemplate::new(200).set_body_json(json!({"PowerState": state}))
            })
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Oem/Ready"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(2)
            .mount(&server)
            .await;
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "configured-compute");
        access.compute_power_paths.system_state_path = Some("/redfish/v1/Oem/Host".into());
        access.compute_power_paths.bmc_ready_path = Some("/redfish/v1/Oem/Ready".into());
        access.compute_timing = Some(ComputeFirmwareTiming {
            power_off_delay: Duration::ZERO,
            bmc_recovery_interval: Duration::ZERO,
            bmc_recovery_attempts: 1,
            activation_command_retries: 0,
            ..ComputeFirmwareTiming::default()
        });
        let mut target = RetryingActivationTarget::with_access([0, 0, 0, 0, 0], access);
        let attempts =
            activate_full_compute_gb200_with_policy(&mut target, 1, 0, Duration::ZERO, 2)
                .await
                .unwrap();
        assert_eq!(attempts, 2);
        assert_eq!(
            target.commands,
            [
                "RF_PWR_OFF",
                "RF_AUX_PWR_CYCLE",
                "RF_PWR_ON",
                "RF_AUX_PWR_CYCLE",
                "RF_PWR_ON"
            ]
        );
        server.verify().await;
    }

    #[tokio::test]
    async fn configured_activation_retries_bmc_readiness_with_the_configured_cycle_budget() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        for recovers in [true, false] {
            let server = MockServer::start().await;
            let state_reads = Arc::new(AtomicUsize::new(0));
            Mock::given(method("GET"))
                .and(path("/redfish/v1/Oem/Host"))
                .respond_with(move |_: &wiremock::Request| {
                    let state = if state_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                        "Off"
                    } else {
                        "On"
                    };
                    ResponseTemplate::new(200).set_body_json(json!({"PowerState": state}))
                })
                .expect(if recovers { 2 } else { 1 })
                .mount(&server)
                .await;
            let ready_reads = Arc::new(AtomicUsize::new(0));
            Mock::given(method("GET"))
                .and(path("/redfish/v1/Oem/Ready"))
                .respond_with(move |_: &wiremock::Request| {
                    let attempt = ready_reads.fetch_add(1, Ordering::SeqCst) + 1;
                    ResponseTemplate::new(if recovers && attempt == 3 { 200 } else { 503 })
                        .set_body_json(json!({}))
                })
                .expect(3)
                .mount(&server)
                .await;
            let timing = ComputeFirmwareTiming {
                power_off_delay: Duration::ZERO,
                ac_cycle_attempts: 3,
                ac_cycle_stabilization: Duration::from_millis(5),
                bmc_recovery_interval: Duration::ZERO,
                bmc_recovery_attempts: 1,
                activation_command_retries: 0,
                power_on_attempts: 2,
                power_on_interval: Duration::from_secs(1),
                ..ComputeFirmwareTiming::default()
            };
            let context = WorkflowContext::new().with_compute_timing(timing);
            let mut access = BmcAccess::mock_with_base_url(server.uri(), "configured-compute");
            access.compute_power_paths.system_state_path = Some("/redfish/v1/Oem/Host".into());
            access.compute_power_paths.bmc_ready_path = Some("/redfish/v1/Oem/Ready".into());
            access.compute_timing = Some(timing);
            let mut target = RetryingActivationTarget::with_access([0; 5], access);
            let started = tokio::time::Instant::now();
            let result = activate_full_compute_gb200(&context, &mut target).await;
            assert!(started.elapsed() >= timing.ac_cycle_stabilization * 3);

            let mut commands = vec![
                "RF_PWR_OFF",
                "RF_AUX_PWR_CYCLE",
                "RF_AUX_PWR_CYCLE",
                "RF_AUX_PWR_CYCLE",
            ];
            if recovers {
                let summary = result.expect("BMC should recover on the third AC cycle");
                assert_eq!(summary.details["max_ac_cycle_attempts"], 3);
                assert_eq!(summary.details["ac_cycle_attempts"], 3);
                commands.push("RF_PWR_ON");
            } else {
                assert!(matches!(result, Err(NvFwUpdError::Timeout { .. })));
            }
            assert_eq!(target.commands, commands);
            server.verify().await;
        }
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
    async fn configured_host_recovery_retains_context_paths_and_timing() {
        use wiremock::matchers::body_json;
        let server = MockServer::start().await;
        let timing = ComputeFirmwareTiming {
            recovery_probe_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
            power_off_delay: Duration::ZERO,
            ..ComputeFirmwareTiming::default()
        };
        let context = WorkflowContext::new()
            .with_compute_timing(timing)
            .with_compute_power_paths(ComputePowerPaths {
                system_state_path: Some("/redfish/v1/Oem/Host".into()),
                system_reset_path: Some("/redfish/v1/Oem/Reset".into()),
                aux_power_cycle_path: Some("/redfish/v1/Oem/Aux".into()),
                ..ComputePowerPaths::default()
            });
        let config = target(ServerType::GB200);
        let mut target = context.target_builder(&config).build_target().unwrap();
        let rf_target = target.as_rf_target_mut();
        // Replace only transport addressing with the local mock. The production
        // context builder supplies all paths and timing used by recovery.
        rf_target.target_access_mut().base_url = server.uri();
        assert_eq!(rf_target.target_access().compute_timing, Some(timing));
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Oem/Host"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"PowerState":"Off"})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Oem/Reset"))
            .and(body_json(json!({"ResetType":"On"})))
            .respond_with(ResponseTemplate::new(204))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Oem/Aux"))
            .and(body_json(json!({"ResetType":"AuxPowerCycle"})))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let mut probes = VecDeque::from([
            Err(NvFwUpdError::Timeout {
                operation: "mock host readiness",
                seconds: 1,
            }),
            Ok(1),
        ]);
        let summary = ensure_gb200_host_os_ready_with(
            rf_target,
            || std::future::ready(probes.pop_front().unwrap()),
            Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(summary.details["recovery_ac_cycle"], true);
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests
            .iter()
            .all(|request| request.url.path().starts_with("/redfish/v1/Oem/")));
        server.verify().await;
    }

    #[tokio::test]
    async fn gb200_host_boot_requests_power_on_before_waiting_for_os() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{"@odata.id": "/redfish/v1/Systems/System_0"}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/System_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "PowerState": "Off"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock-compute");
        let mut target = RetryingActivationTarget::with_access([0], access);
        let summary = ensure_gb200_host_os_ready_with(
            &mut target,
            || std::future::ready(Ok(2)),
            Duration::ZERO,
        )
        .await
        .expect("powered-off host should boot without recovery");

        assert_eq!(target.commands, vec!["RF_PWR_ON"]);
        assert_eq!(summary.details["recovery_ac_cycle"], false);
        assert_eq!(summary.details["host_probe_attempts"], 2);
        server.verify().await;
    }

    #[tokio::test]
    async fn gb200_host_boot_retries_once_after_auxiliary_ac_cycle() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{"@odata.id": "/redfish/v1/Systems/System_0"}]
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
        let mut target = RetryingActivationTarget::with_access([0, 0], access);
        let mut host_results = VecDeque::from([
            Err(NvFwUpdError::Timeout {
                operation: "wait for GB200 host OS boot",
                seconds: 360,
            }),
            Ok(3),
        ]);
        let summary = ensure_gb200_host_os_ready_with(
            &mut target,
            || std::future::ready(host_results.pop_front().expect("host result")),
            Duration::ZERO,
        )
        .await
        .expect("second boot attempt should succeed");

        assert_eq!(target.commands, vec!["RF_AUX_PWR_CYCLE", "RF_PWR_ON"]);
        assert_eq!(summary.details["recovery_ac_cycle"], true);
        assert_eq!(summary.details["host_probe_attempts_after_recovery"], 3);
        server.verify().await;
    }

    #[tokio::test]
    async fn gb200_host_boot_does_not_ac_cycle_for_authentication_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [{"@odata.id": "/redfish/v1/Systems/System_0"}]
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
        let error = ensure_gb200_host_os_ready_with(
            &mut target,
            || {
                std::future::ready(Err(NvFwUpdError::AuthFailed {
                    target: "host".to_owned(),
                    message: "invalid credentials".to_owned(),
                }))
            },
            Duration::ZERO,
        )
        .await
        .expect_err("authentication failures are not boot failures");

        assert!(matches!(error, NvFwUpdError::AuthFailed { .. }));
        assert!(target.commands.is_empty());
        server.verify().await;
    }

    #[tokio::test]
    async fn gb200_flint_discovery_recovery_ac_cycles_and_waits_for_host() {
        let mut target = RetryingActivationTarget::new([0, 0]);

        recover_gb200_host_after_flint_discovery_failure_with(
            &mut target,
            || std::future::ready(Ok(2)),
            Duration::ZERO,
        )
        .await
        .expect("device-discovery recovery should reboot the host");

        assert_eq!(target.commands, vec!["RF_AUX_PWR_CYCLE", "RF_PWR_ON"]);
    }

    #[tokio::test]
    async fn compute_power_state_wait_completes_when_system_zero_matches() {
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
        wait_for_compute_power_state_with_policy(&mut target, "On", 3, 0)
            .await
            .expect("System_0 reporting the expected state should complete activation");

        assert!(target.commands.is_empty());
        server.verify().await;
    }

    #[tokio::test]
    async fn full_compute_activation_retries_the_ac_cycle_when_power_on_times_out() {
        use std::sync::atomic::{AtomicUsize, Ordering};

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
        let power_state_queries = Arc::new(AtomicUsize::new(0));
        let response_counter = Arc::clone(&power_state_queries);
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Systems/System_0"))
            .respond_with(move |_: &wiremock::Request| {
                let state = match response_counter.fetch_add(1, Ordering::SeqCst) {
                    0 | 1 => "Off",
                    _ => "On",
                };
                ResponseTemplate::new(200).set_body_json(json!({"PowerState": state}))
            })
            .expect(3)
            .mount(&server)
            .await;

        let access = BmcAccess::mock_with_base_url(server.uri(), "mock-compute");
        let mut target = RetryingActivationTarget::with_access([0, 0, 0, 0, 0], access);
        let attempts =
            activate_full_compute_gb200_with_policy(&mut target, 1, 0, Duration::ZERO, 2)
                .await
                .expect("the second complete AC cycle should recover the host");

        assert_eq!(attempts, 2);
        assert_eq!(
            target.commands,
            vec![
                "RF_PWR_OFF",
                "RF_AUX_PWR_CYCLE",
                "RF_PWR_ON",
                "RF_AUX_PWR_CYCLE",
                "RF_PWR_ON"
            ]
        );
        assert_eq!(power_state_queries.load(Ordering::SeqCst), 3);
        server.verify().await;
    }

    #[tokio::test]
    async fn full_compute_activation_fails_after_one_ac_cycle_retry() {
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
        let mut target = RetryingActivationTarget::with_access([0, 0, 0, 0, 0], access);
        let err = activate_full_compute_gb200_with_policy(&mut target, 1, 0, Duration::ZERO, 2)
            .await
            .expect_err("an Off system should fail after one complete AC-cycle retry");

        assert!(matches!(
            err,
            NvFwUpdError::Timeout {
                operation: "wait for compute system power state On",
                seconds: 0,
            }
        ));
        assert_eq!(
            target.commands,
            vec![
                "RF_PWR_OFF",
                "RF_AUX_PWR_CYCLE",
                "RF_PWR_ON",
                "RF_AUX_PWR_CYCLE",
                "RF_PWR_ON"
            ]
        );
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
    async fn switch_activation_accepts_temporarily_unavailable_power_cycle_action_status() {
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
        // The switch accepted the action, then dropped action-status reads for
        // the whole retry window. Recovery is what should decide success.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "switch rebooting"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "switch rebooting"}
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "ready"
            })))
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
        .expect(
            "an accepted power cycle should succeed when action status is temporarily unavailable and the switch recovers",
        );

        assert_eq!(summary.details["action_id"], "job-power-cycle");
        assert_eq!(
            summary.details["recovery"]["endpoint"],
            SWITCH_ACTIVATION_RECOVERY_PATH
        );
        assert_eq!(summary.details["recovery"]["attempts"], 2);
    }

    #[tokio::test]
    async fn switch_activation_does_not_treat_http_status_failures_as_an_outage() {
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
        // The switch answered every status read with an HTTP error. That is not
        // a reboot signal, so an already-reachable firmware endpoint must not
        // count as recovery.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "switch rebooting"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "ready"
            })))
            .expect(3)
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
        .expect_err("HTTP status errors alone must not skip the outage requirement");

        assert!(
            matches!(
                &err,
                NvFwUpdError::TaskFailed {
                    task_id: Some(task_id),
                    message,
                } if task_id == "job-power-cycle"
                    && message
                        == "firmware endpoint /nvue_v1/platform/firmware stayed reachable; action status never showed an outage"
            ),
            "answered status failures should fail without an outage timeout, got {err:?}"
        );
    }

    #[tokio::test]
    async fn hosted_switch_activation_treats_a_reboot_status_body_as_an_outage() {
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
        // An answered HTTP error whose status text reports the reboot. The
        // Hosted client keeps that body, and recovery may already find firmware up.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "status": "System is rebooting",
                "detail": "System is rebooting"
            })))
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

        let (context, switch_target) = hosted_switch_context(&server);
        let summary = context
            .activate_firmware(
                switch_target,
                ActivationRequest {
                    mode: ActivationMode::SwitchPowerCycle,
                    cancellation: None,
                },
            )
            .await
            .expect("a hosted reboot status body should count as the outage");

        assert_eq!(summary.details["action_id"], "job-power-cycle");
        assert_eq!(summary.details["action"]["status"], "System is rebooting");
        assert_eq!(summary.details["recovery"]["attempts"], 1);
    }

    #[tokio::test]
    async fn hosted_switch_activation_treats_a_missed_status_read_as_an_outage() {
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
        // The Hosted client times out before an HTTP response. That is the
        // missed-read arm, so no status body is available to recovery.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(json!({
                        "status": "still starting"
                    })),
            )
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

        let (context, switch_target) = hosted_switch_context(&server);
        let summary = context
            .activate_firmware(
                switch_target,
                ActivationRequest {
                    mode: ActivationMode::SwitchPowerCycle,
                    cancellation: None,
                },
            )
            .await
            .expect("a missed hosted status read should count as the outage");

        assert_eq!(summary.details["action_id"], "job-power-cycle");
        assert_eq!(summary.details["action"], json!({}));
        assert_eq!(summary.details["recovery"]["attempts"], 1);
    }

    #[tokio::test]
    async fn switch_activation_accepts_unreachable_status_when_firmware_is_already_reachable() {
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
        // The status reads time out, so the switch stopped answering. Firmware
        // is already back when recovery starts.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(json!({
                        "error": {"message": "switch rebooting"}
                    })),
            )
            .expect(4)
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
        .expect(
            "a missed status response should count as the outage when firmware is already back",
        );

        assert_eq!(summary.details["action_id"], "job-power-cycle");
        assert_eq!(
            summary.details["recovery"]["endpoint"],
            SWITCH_ACTIVATION_RECOVERY_PATH
        );
        assert_eq!(summary.details["recovery"]["attempts"], 1);
    }

    #[tokio::test]
    async fn switch_activation_keeps_an_earlier_outage_after_the_status_becomes_readable() {
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
        // The first status read misses its response, so the switch stopped
        // answering. A later read then succeeds with a non-failed status.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(json!({
                        "error": {"message": "switch rebooting"}
                    })),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": "action_success",
                "status": "power cycle complete"
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        // The switch is already back, so recovery never sees another outage and
        // has to rely on the outage observed before the status became readable.
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
        .expect("a readable status must not discard an outage seen by an earlier read");

        assert_eq!(summary.details["action_id"], "job-power-cycle");
        assert_eq!(summary.details["recovery"]["attempts"], 1);
    }

    /// Mount a power-cycle POST, a lost status read, and a still-running
    /// status, which is the window that leaves a reboot outstanding.
    async fn mount_running_action_after_a_lost_status_read(server: &MockServer) {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, ResponseTemplate};

        Mock::given(method("POST"))
            .and(path("/nvue_v1/system"))
            .and(body_string_contains("@power-cycle"))
            .respond_with(ResponseTemplate::new(202).set_body_string("job-power-cycle"))
            .expect(1)
            .mount(server)
            .await;
        // A reset or a refused connection loses a read immediately, so this
        // needs none of the long request timeout to happen.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(
                ResponseTemplate::new(503)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(json!({"error": {"message": "connection reset"}})),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": "action_running",
                "status": ""
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn switch_activation_does_not_complete_while_the_action_is_still_running() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        mount_running_action_after_a_lost_status_read(&server).await;
        // The switch answers every probe, so it never took the reboot its
        // running action still owes.
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "ready"})))
            .mount(&server)
            .await;

        let error = activate_switch_power_cycle_with_target(legacy_switch_target(
            BmcAccess::mock_with_base_url_and_type(
                server.uri(),
                "mock-switch",
                AccessType::NVSwitch,
            ),
        ))
        .await
        .expect_err("a running action must not complete while the switch stays up");

        assert!(
            matches!(error, NvFwUpdError::Timeout { .. }),
            "expected the timeout that drives the BMC auxiliary AC cycle, got {error:?}"
        );
    }

    #[tokio::test]
    async fn switch_activation_waits_for_the_reboot_when_the_action_is_still_running() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        mount_running_action_after_a_lost_status_read(&server).await;
        // The endpoint is still up on the first probe, goes down for the
        // pending reboot, and comes back. Only the outage after the status
        // read proves the action finished what it started.
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "ready"})))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({"status": "down"})))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "ready"})))
            .up_to_n_times(1)
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
        .expect("the switch came back after the reboot its running action owed");

        assert_eq!(
            summary.details["recovery"]["attempts"], 3,
            "activation must not complete on the first probe while the action runs"
        );
    }

    #[tokio::test]
    async fn switch_activation_rejects_unavailable_power_cycle_when_switch_does_not_recover() {
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
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "switch rebooting"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "switch rebooting"}
            })))
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
        .expect_err("an accepted power cycle should fail when the switch never recovers");

        assert!(
            matches!(
                err,
                NvFwUpdError::Timeout {
                    operation: "wait for switch after activation",
                    seconds: 3,
                }
            ),
            "temporary action-status loss should reach recovery polling, got {err:?}"
        );
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
    async fn powershelf_activation_reuses_activation_command_recovery() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn write_json_response(socket: &mut tokio::net::TcpStream, status: &str, body: &str) {
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let recovery_gets = Arc::new(AtomicUsize::new(0));
        let recovery_gets_for_server = Arc::clone(&recovery_gets);
        let server_task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let recovery_gets = Arc::clone(&recovery_gets_for_server);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let request_line = request.lines().next().unwrap_or_default();
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or_default();
                    let path = parts.next().unwrap_or_default();

                    match (method, path) {
                        ("GET", "/redfish/v1/Managers") => {
                            write_json_response(
                                &mut socket,
                                "200 OK",
                                r#"{"Members":[{"@odata.id":"/redfish/v1/Managers/powershelf"}]}"#,
                            )
                            .await;
                        }
                        ("GET", "/redfish/v1/Managers/powershelf") => {
                            write_json_response(
                                &mut socket,
                                "200 OK",
                                r##"{"Actions":{"#Manager.Reset":{"target":"/redfish/v1/Managers/powershelf/Actions/Manager.Reset"}}}"##,
                            )
                            .await;
                        }
                        ("POST", "/redfish/v1/Managers/powershelf/Actions/Manager.Reset") => {
                            // Simulate the real reset handoff case: the request reaches
                            // the device, but the HTTPS response is lost as it reboots.
                        }
                        ("GET", "/redfish/v1") => {
                            let attempt = recovery_gets.fetch_add(1, Ordering::SeqCst);
                            if attempt == 0 {
                                write_json_response(
                                    &mut socket,
                                    "503 Service Unavailable",
                                    r#"{"error":{"message":"powershelf rebooting"}}"#,
                                )
                                .await;
                            } else {
                                write_json_response(
                                    &mut socket,
                                    "200 OK",
                                    r#"{"RedfishVersion":"1.0.0"}"#,
                                )
                                .await;
                            }
                        }
                        _ => {
                            write_json_response(
                                &mut socket,
                                "404 Not Found",
                                r#"{"error":{"message":"not found"}}"#,
                            )
                            .await;
                        }
                    }
                });
            }
        });

        let summary = activate_powershelf_reset_with_access(
            BmcAccess::mock_with_base_url(base_url, "mock-powershelf"),
            "RF_PWRSHELF_RESET",
        )
        .await
        .expect("PowerShelf reset activation should reuse internally observed recovery");

        server_task.abort();
        assert_eq!(summary.details["command"], "RF_PWRSHELF_RESET");
        assert_eq!(summary.details["recovery"]["attempts"], 2);
        assert_eq!(
            summary.details["recovery"]["observed_by"],
            "activation_command"
        );
        assert_eq!(recovery_gets.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn powershelf_activation_accepts_same_origin_absolute_reset_uri() {
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
                        "target": format!(
                            "{}/redfish/v1/Managers/powershelf/Actions/Manager.Reset",
                            server.uri()
                        )
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
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        let summary = activate_powershelf_reset_with_access(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            "RF_PWRSHELF_RESET",
        )
        .await
        .expect("PowerShelf reset activation should accept same-origin absolute reset URI");

        assert_eq!(summary.details["command"], "RF_PWRSHELF_RESET");
        assert_eq!(summary.details["recovery"]["attempts"], 2);
        assert_eq!(summary.details["recovery"]["observed_by"], "workflow");
    }

    #[tokio::test]
    async fn switch_activation_times_out_when_a_readable_status_never_goes_down() {
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
                "state": "action_success"
            })))
            .expect(1)
            .mount(&server)
            .await;
        // The switch never goes down, so recovery exhausts its attempts.
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "ready"
            })))
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
        .expect_err("a switch that never goes down must not report success");

        // A readable status keeps the timeout that drives the BMC aux AC-cycle
        // fallback for GB200 and GB300.
        assert!(
            matches!(
                err,
                NvFwUpdError::Timeout {
                    operation: "wait for switch after activation",
                    ..
                }
            ),
            "expected a timeout, got {err:?}"
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
    async fn switch_activation_rejects_action_failure_on_the_final_status_read() {
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
        // The retry loop misses the action, then the final GET reports failure.
        // That response must not be discarded and recovered as an outage.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/job-power-cycle"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {"message": "switch rebooting"}
            })))
            .up_to_n_times(3)
            .expect(3)
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
        Mock::given(method("GET"))
            .and(path(SWITCH_ACTIVATION_RECOVERY_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "ready"
            })))
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
        .expect_err("a failed action on the final status read must not recover");

        assert!(
            matches!(
                err,
                NvFwUpdError::TaskFailed {
                    task_id: Some(ref action_id),
                    ref message,
                } if action_id == "job-power-cycle"
                    && message.contains("action_failed")
                    && message.contains("power cycle failed")
            ),
            "final action failure should fail activation, got {err:?}"
        );
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

    #[tokio::test]
    async fn skipping_preconditions_rejects_an_already_cancelled_token() {
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        let request = FirmwareUpdateRequest {
            component: String::new(),
            firmware_file: "/tmp/recovery.fwpkg".to_string(),
            force_update: true,
            options: crate::workflow::UpdateOptions::default(),
            cancellation: Some(token),
        };

        let err = update_firmware_skipping_preconditions(target(ServerType::GB200), request)
            .await
            .expect_err("a cancelled token must stop the push before any work");
        assert!(err.to_string().contains("cancelled"));
    }

    #[tokio::test]
    async fn skipping_preconditions_requires_a_firmware_file() {
        let request = FirmwareUpdateRequest {
            component: String::new(),
            firmware_file: String::new(),
            force_update: true,
            options: crate::workflow::UpdateOptions::default(),
            cancellation: None,
        };

        let err = update_firmware_skipping_preconditions(target(ServerType::GB200), request)
            .await
            .expect_err("an empty firmware_file must be rejected up front");
        assert!(matches!(err, NvFwUpdError::PackageParse { .. }));
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
        for server_type in [
            ServerType::GB200,
            ServerType::GB300,
            ServerType::VRNVL72,
            ServerType::LP30,
        ] {
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

    #[test]
    fn build_bmc_access_rejects_incomplete_credentials() {
        // Any half-set username/password pair is rejected, not silently sent
        // as Basic auth with a blank field and not reinterpreted as
        // session-token auth (which is signaled by a dedicated field, not by
        // username emptiness).
        let cases: &[(&str, fn(&mut TargetConfig))] = &[
            ("both empty", |c| {
                c.username.clear();
                c.password.clear();
            }),
            ("username set, password empty", |c| c.password.clear()),
            ("username empty, password set", |c| c.username.clear()),
        ];

        for (label, mutate) in cases {
            let mut config = target(ServerType::GB200);
            mutate(&mut config);

            let result = WorkflowTargetBuilder::new(&config).build_bmc_access();

            assert!(
                matches!(result, Err(NvFwUpdError::AuthFailed { ref message, .. }) if message.contains("must not be empty")),
                "case '{label}' should be rejected as empty credentials"
            );
        }
    }

    #[tokio::test]
    async fn build_bmc_access_uses_basic_auth_for_user_pass() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let config = target(ServerType::GB200);
        let access = WorkflowTargetBuilder::new(&config)
            .build_bmc_access()
            .unwrap();

        access
            .apply_auth(access.client.get(format!("{}/redfish/v1", server.uri())))
            .send()
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let header_names: Vec<&str> = requests[0]
            .headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert!(
            !header_names.contains(&"x-auth-token"),
            "basic-auth path must not inject X-Auth-Token; headers: {header_names:?}"
        );
        assert!(
            header_names.contains(&"authorization"),
            "basic-auth path must inject Authorization header; headers: {header_names:?}"
        );
    }

    #[tokio::test]
    async fn build_bmc_access_uses_session_token_for_powershelf() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1"))
            .and(header("X-Auth-Token", "shelf-token"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let config = target(ServerType::PowerShelf).with_bmc_session_token("shelf-token");
        let access = WorkflowTargetBuilder::new(&config)
            .build_bmc_access()
            .unwrap();

        let response = access
            .apply_auth(access.client.get(format!("{}/redfish/v1", server.uri())))
            .send()
            .await
            .unwrap();

        assert!(
            response.status().is_success(),
            "PowerShelf session-token access should send X-Auth-Token"
        );
    }

    #[tokio::test]
    async fn build_bmc_access_uses_session_token_for_hgxb100() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1"))
            .and(header("X-Auth-Token", "hgx-token"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let config = target(ServerType::HGXB100).with_bmc_session_token("hgx-token");
        let access = WorkflowTargetBuilder::new(&config)
            .build_bmc_access()
            .unwrap();

        let response = access
            .apply_auth(access.client.get(format!("{}/redfish/v1", server.uri())))
            .send()
            .await
            .unwrap();

        assert!(
            response.status().is_success(),
            "HGXB100 session-token access should send X-Auth-Token"
        );
    }
}
