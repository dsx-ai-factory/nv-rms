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

//! GB200RFTarget implementation for GB200/GB300/VR NVL platforms.
//!
//! Extends GH200 behavior with additional HGX platform patterns
//! and special fungible component handling for SMA, CPLD, and
//! IO board components.

use std::time::{Duration, Instant};

use regex::Regex;
use serde_json::{json, Value};
use tokio::time::sleep;

use crate::bmc_access::BmcAccess;
use crate::gh200_rftarget;
use crate::rf_target::{CmdArgs, PkgParser, RFTarget, UpdatePreconditionMode};
use crate::util::{BailAction, TraceFlags, Util};
use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Filename patterns that trigger multipart upload.
/// Extends GH200 patterns with GB200-NVL4.
const MULTIPART_PLATFORMS: &[&str] = &[
    "P4059",
    "P4764",
    "P4974",
    "P4975",
    "HGX",
    "HMC",
    "GB200-NVL4",
];

/// Substrings in a package version string that mark it as an HGX-tray package.
const HGX_PLATFORM_TOKENS: &[&str] = &["HGX", "4059", "4764", "4974", "4975", "GB200-NVL4", "HMC"];

/// Component name fragments that are fungible on GB200 platforms.
const GB200_FUNGIBLE_FRAGMENTS: &[&str] = &["fw_sma_bluefield", "fw_bluefield_sma", "nvme_e1s"];

/// SMA component fragments whose chassis resource carries the SKU identifier.
const GB200_SMA_CHASSIS_SKU_FRAGMENTS: &[&str] = &[
    "fw_io_board_sma",
    "fw_processormodule_sma",
    "fw_storagebackplane_sma",
    "fw_gpu_sma",
    "fw_bay_c_sma",
];

const BACKGROUND_COPY_STATUS_POINTER: &str = "/Oem/Nvidia/BackgroundCopyStatus";
const BACKGROUND_COPY_QUERY_TIMEOUT_SECS: u64 = 30;
const BACKGROUND_COPY_SINGLE_SHOT_QUERY_RETRIES: usize = 3;
const BACKGROUND_COPY_SINGLE_SHOT_QUERY_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const BACKGROUND_COPY_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(60);
const WRITE_PROTECT_QUERY_TIMEOUT_SECS: u64 = 30;
const GLOBAL_WRITE_PROTECT_URI: &str = "/redfish/v1/Chassis/Chassis_0";
const GLOBAL_WRITE_PROTECT_POINTER: &str = "/Oem/Nvidia/HardwareWriteProtectEnable";
const FIRMWARE_INVENTORY_URI: &str = "/redfish/v1/UpdateService/FirmwareInventory";

#[derive(Debug, Clone, PartialEq, Eq)]
struct BackgroundCopyComponent {
    uri: String,
    status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteProtectComponent {
    uri: String,
    write_protected: bool,
}

// ---------------------------------------------------------------------------
// GB200RFTarget
// ---------------------------------------------------------------------------

/// Platform-specific RFTarget for GB200, GB300, and VR NVL systems.
///
/// Extends GH200 behavior with additional multipart upload patterns
/// and GB200-specific fungible component definitions.
pub struct GB200RFTarget {
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

impl GB200RFTarget {
    /// Create a new `GB200RFTarget` with sensible defaults.
    pub fn new(bmc_access: BmcAccess, config_dict: Option<Value>) -> Self {
        Self {
            bmc_access,
            fungible_components: vec![
                "gpu".to_string(),
                "fw_io_board_sma".to_string(),
                "fw_processormodule_sma".to_string(),
                "fw_storagebackplane_sma".to_string(),
                "fw_gpu_sma".to_string(),
                "fw_bay_c_sma".to_string(),
                "cpld".to_string(),
            ],
            update_completion_msg: "Refer to 'NVIDIA Firmware Update Document' on \
                                    activation steps for new firmware to take effect."
                .to_string(),
            progress_table_header_printed: false,
            config_dict,
        }
    }

    /// Check whether a filename matches a multipart upload platform pattern.
    fn requires_multipart(filename: &str) -> bool {
        let upper = filename.to_uppercase();
        MULTIPART_PLATFORMS
            .iter()
            .any(|p| upper.contains(&p.to_uppercase()))
    }

    /// Check whether the package name belongs to an HGX tray.
    fn is_hgx_pkg(pkg_name: &str) -> bool {
        HGX_PLATFORM_TOKENS.iter().any(|tok| pkg_name.contains(tok))
    }

    /// Return true when a reported BackgroundCopyStatus value is complete.
    ///
    /// Redfish payloads are treated case-insensitively so minor casing
    /// differences from platform firmware do not block updates.
    fn background_copy_status_is_completed(status: &str) -> bool {
        status.trim().eq_ignore_ascii_case("Completed")
    }

    /// Select the BackgroundCopy components that are not ready for update.
    ///
    /// The returned list is used both for CLI/RMS error messages and for the
    /// RMS wait loop, where only incomplete components are refreshed on later
    /// polls.
    fn incomplete_background_copy_components(
        components: &[BackgroundCopyComponent],
    ) -> Vec<BackgroundCopyComponent> {
        components
            .iter()
            .filter(|component| !Self::background_copy_status_is_completed(&component.status))
            .cloned()
            .collect()
    }

    /// Build the user-facing error for a single-shot BackgroundCopy check.
    ///
    /// This message is returned when the CLI finds an in-progress background
    /// copy and therefore refuses to start a firmware update.
    fn background_copy_not_ready_message(incomplete: &[BackgroundCopyComponent]) -> String {
        format!(
            "BackgroundCopy is currently in progress for {} component(s): {}; firmware updates \
             are not allowed until all Oem.Nvidia.BackgroundCopyStatus values are Completed",
            incomplete.len(),
            Self::background_copy_component_summary(incomplete)
        )
    }

    /// Render BackgroundCopy component status details for logs and errors.
    ///
    /// The format intentionally includes each Redfish URI so operators can see
    /// exactly which chassis resources are blocking the firmware update.
    fn background_copy_component_summary(incomplete: &[BackgroundCopyComponent]) -> String {
        incomplete
            .iter()
            .map(|component| format!("{} status={}", component.uri, component.status))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Emit a throttled RMS wait-loop progress log for BackgroundCopy polling.
    ///
    /// Callers control throttling; this helper keeps the structured log fields
    /// consistent for each progress message.
    fn log_background_copy_wait(incomplete: &[BackgroundCopyComponent]) {
        tracing::info!(
            incomplete_count = incomplete.len(),
            components = %Self::background_copy_component_summary(incomplete),
            "Waiting for BackgroundCopyStatus to reach Completed before firmware update"
        );
    }

    /// Build the timeout error returned when BackgroundCopy never completes.
    ///
    /// The timeout duration and the final incomplete component list are included
    /// so RMS job results explain both how long we waited and what remained.
    fn background_copy_timeout_message(
        incomplete: &[BackgroundCopyComponent],
        timeout: Duration,
    ) -> String {
        let mut message = format!(
            "BackgroundCopyStatus did not reach Completed within {}s. ",
            timeout.as_secs()
        );
        message.push_str(&Self::background_copy_not_ready_message(incomplete));
        message
    }

    /// Represent a failed BackgroundCopy query as an incomplete component.
    ///
    /// RMS wait mode keeps polling after transient failures, so query errors are
    /// modeled as a synthetic component status instead of immediately failing.
    fn background_copy_query_failed_component(
        uri: impl Into<String>,
        error: impl AsRef<str>,
    ) -> BackgroundCopyComponent {
        BackgroundCopyComponent {
            uri: uri.into(),
            status: format!("QueryFailed: {}", error.as_ref()),
        }
    }

    /// Extract FirmwareInventory member URIs for per-component checks.
    ///
    /// Malformed member entries are logged and skipped so unexpected inventory
    /// shapes are visible without blocking the entire update precondition.
    fn firmware_inventory_member_uris(inventory: &Value) -> Result<Vec<String>, String> {
        let members = inventory
            .get("Members")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                format!("Failed to enumerate {FIRMWARE_INVENTORY_URI}: missing Members array")
            })?;

        let mut uris = Vec::new();
        for (index, member) in members.iter().enumerate() {
            if let Some(uri) = member.get("@odata.id").and_then(Value::as_str) {
                uris.push(uri.to_string());
                continue;
            }

            tracing::warn!(
                member_index = index,
                member = %NvUtils::redact_secret_json_value(member),
                "Skipping malformed FirmwareInventory member while checking WriteProtected"
            );
        }

        Ok(uris)
    }

    /// Build the hard-failure message for enabled component write protect.
    ///
    /// The message lists each protected FirmwareInventory URI so RMS and CLI
    /// callers can identify exactly which component must be fixed before retry.
    fn write_protect_enabled_message(components: &[WriteProtectComponent]) -> String {
        format!(
            "Firmware component write protect is enabled for {} component(s): {}; firmware \
             updates are not allowed until all WriteProtected values are false",
            components.len(),
            components
                .iter()
                .map(|component| format!("{} WriteProtected=true", component.uri))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    /// GET a Redfish resource used by write-protect precondition checks.
    ///
    /// This centralizes the timeout and dispatch options used for both the
    /// global chassis check and per-component FirmwareInventory checks.
    async fn dispatch_write_protect_get(
        &self,
        uri: &str,
        json_dict: Option<&mut Value>,
    ) -> (bool, Value) {
        self.target_access()
            .dispatch_request_full(
                "GET",
                uri,
                None,
                None,
                WRITE_PROTECT_QUERY_TIMEOUT_SECS,
                true,
                json_dict,
            )
            .await
    }

    /// Check the global hardware write-protect bit on Chassis_0.
    ///
    /// A present `true` value blocks firmware updates. Missing or unqueryable
    /// global state is logged and treated as advisory so the per-component
    /// `WriteProtected` checks can still run.
    async fn check_global_hardware_write_protect(
        &self,
        json_dict: Option<&mut Value>,
    ) -> Result<(), String> {
        let (ok, chassis) = self
            .dispatch_write_protect_get(GLOBAL_WRITE_PROTECT_URI, json_dict)
            .await;
        if !ok {
            tracing::warn!(
                uri = GLOBAL_WRITE_PROTECT_URI,
                response = %NvUtils::redact_secret_json_value(&chassis),
                "Unable to query global hardware write protect state; continuing with \
                 per-component WriteProtected checks"
            );
            return Ok(());
        }

        match chassis
            .pointer(GLOBAL_WRITE_PROTECT_POINTER)
            .and_then(Value::as_bool)
        {
            Some(false) => Ok(()),
            Some(true) => Err(format!(
                "Hardware write protect is enabled at {GLOBAL_WRITE_PROTECT_URI} \
                 Oem.Nvidia.HardwareWriteProtectEnable=true; firmware updates are not allowed \
                 until hardware write protect is disabled"
            )),
            None => {
                tracing::warn!(
                    uri = GLOBAL_WRITE_PROTECT_URI,
                    "Global hardware write protect field is missing; continuing with \
                     per-component WriteProtected checks"
                );
                Ok(())
            }
        }
    }

    /// Discover FirmwareInventory component URIs for write-protect inspection.
    ///
    /// Failure to query the inventory collection blocks the update because RMS
    /// cannot safely inspect component-level write protection without it.
    async fn discover_firmware_inventory_uris(
        &self,
        json_dict: Option<&mut Value>,
    ) -> Result<Vec<String>, String> {
        let (ok, inventory) = self
            .dispatch_write_protect_get(FIRMWARE_INVENTORY_URI, json_dict)
            .await;
        if !ok {
            tracing::warn!(
                uri = FIRMWARE_INVENTORY_URI,
                response = %NvUtils::redact_secret_json_value(&inventory),
                "Failed to query FirmwareInventory while checking WriteProtected values"
            );
            return Err(format!(
                "Failed to query {FIRMWARE_INVENTORY_URI} while checking FirmwareInventory \
                 WriteProtected values"
            ));
        }

        Self::firmware_inventory_member_uris(&inventory)
    }

    /// Read the optional `WriteProtected` value for one FirmwareInventory item.
    ///
    /// Components that cannot be queried are warned and skipped. Components
    /// without a `WriteProtected` field return `None` because the platform does
    /// not expose a write-protect precondition for them.
    async fn read_firmware_write_protect_component(
        &self,
        uri: &str,
        json_dict: Option<&mut Value>,
    ) -> Result<Option<WriteProtectComponent>, String> {
        let (ok, inventory_component) = self.dispatch_write_protect_get(uri, json_dict).await;
        if !ok {
            tracing::warn!(
                uri,
                "Unable to query firmware inventory component while checking WriteProtected; \
                 skipping component"
            );
            return Ok(None);
        }

        Ok(inventory_component
            .get("WriteProtected")
            .and_then(Value::as_bool)
            .map(|write_protected| WriteProtectComponent {
                uri: uri.to_string(),
                write_protected,
            }))
    }

    /// Read component write-protect state for all discovered inventory URIs.
    ///
    /// The returned vector contains only components that expose an explicit
    /// boolean `WriteProtected` field; absent fields are intentionally ignored.
    async fn read_firmware_write_protect_components(
        &self,
        component_uris: &[String],
        mut json_dict: Option<&mut Value>,
    ) -> Result<Vec<WriteProtectComponent>, String> {
        let mut components = Vec::new();
        for uri in component_uris {
            if let Some(component) = self
                .read_firmware_write_protect_component(uri, json_dict.as_deref_mut())
                .await?
            {
                components.push(component);
            }
        }
        Ok(components)
    }

    /// Enforce component-level write-protect preconditions before flashing.
    ///
    /// Any discovered component with `WriteProtected=true` causes a hard
    /// failure before upload/task creation, with a message naming each component.
    async fn check_firmware_inventory_write_protect(
        &self,
        mut json_dict: Option<&mut Value>,
    ) -> Result<(), String> {
        let component_uris = self
            .discover_firmware_inventory_uris(json_dict.as_deref_mut())
            .await?;
        let components = self
            .read_firmware_write_protect_components(&component_uris, json_dict)
            .await?;
        let protected = components
            .into_iter()
            .filter(|component| component.write_protected)
            .collect::<Vec<_>>();

        if protected.is_empty() {
            return Ok(());
        }

        Err(Self::write_protect_enabled_message(&protected))
    }

    /// Run all write-protect preconditions required before firmware upload.
    ///
    /// The global chassis check runs first, followed by per-component
    /// FirmwareInventory checks. The update may proceed only when neither layer
    /// reports enabled write protection.
    async fn check_write_protect_disabled(
        &self,
        mut json_dict: Option<&mut Value>,
    ) -> Result<(), String> {
        self.check_global_hardware_write_protect(json_dict.as_deref_mut())
            .await?;
        self.check_firmware_inventory_write_protect(json_dict).await
    }

    /// Discover chassis resources that expose BackgroundCopyStatus.
    ///
    /// The check starts at `/redfish/v1/Chassis` and keeps only members with
    /// `Oem.Nvidia.BackgroundCopyStatus`, which are the resources that can
    /// block firmware updates while background copy is active.
    async fn discover_background_copy_components(
        &self,
        mut json_dict: Option<&mut Value>,
    ) -> Result<Vec<BackgroundCopyComponent>, String> {
        let (ok, chassis_collection) = self
            .dispatch_background_copy_get("/redfish/v1/Chassis", json_dict.as_deref_mut())
            .await;
        if !ok {
            return Err(
                "Failed to query /redfish/v1/Chassis while checking BackgroundCopyStatus"
                    .to_string(),
            );
        }

        let members = chassis_collection
            .get("Members")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                "Failed to enumerate /redfish/v1/Chassis: missing Members array".to_string()
            })?;

        let mut components = Vec::new();
        for member in members {
            let Some(uri) = member.get("@odata.id").and_then(Value::as_str) else {
                continue;
            };
            if let Some(component) = self
                .read_background_copy_component(uri, json_dict.as_deref_mut())
                .await?
            {
                components.push(component);
            }
        }

        Ok(components)
    }

    /// Discover BackgroundCopy components with short retry handling.
    ///
    /// Single-shot CLI checks use this to absorb transient Redfish read
    /// failures before deciding whether firmware updates are allowed.
    async fn discover_background_copy_components_with_query_retries(
        &self,
        mut json_dict: Option<&mut Value>,
    ) -> Result<Vec<BackgroundCopyComponent>, String> {
        let mut last_error = None;
        for attempt in 0..=BACKGROUND_COPY_SINGLE_SHOT_QUERY_RETRIES {
            match self
                .discover_background_copy_components(json_dict.as_deref_mut())
                .await
            {
                Ok(components) => return Ok(components),
                Err(err) => {
                    last_error = Some(err);
                    if attempt < BACKGROUND_COPY_SINGLE_SHOT_QUERY_RETRIES {
                        sleep(BACKGROUND_COPY_SINGLE_SHOT_QUERY_RETRY_INTERVAL).await;
                    }
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| "Failed to query BackgroundCopyStatus components".to_string()))
    }

    /// GET a Redfish resource used by BackgroundCopy precondition checks.
    ///
    /// This centralizes the timeout and dispatch options used while discovering
    /// and polling chassis BackgroundCopyStatus resources.
    async fn dispatch_background_copy_get(
        &self,
        uri: &str,
        json_dict: Option<&mut Value>,
    ) -> (bool, Value) {
        self.target_access()
            .dispatch_request_full(
                "GET",
                uri,
                None,
                None,
                BACKGROUND_COPY_QUERY_TIMEOUT_SECS,
                true,
                json_dict,
            )
            .await
    }

    /// Read BackgroundCopyStatus from one chassis resource.
    ///
    /// Returns `Some` only when the resource exposes
    /// `Oem.Nvidia.BackgroundCopyStatus`; chassis members without that property
    /// are not part of the update precondition.
    async fn read_background_copy_component(
        &self,
        uri: &str,
        json_dict: Option<&mut Value>,
    ) -> Result<Option<BackgroundCopyComponent>, String> {
        let (ok, chassis) = self.dispatch_background_copy_get(uri, json_dict).await;
        if !ok {
            return Err(format!(
                "Failed to query chassis component {uri} while checking BackgroundCopyStatus"
            ));
        }

        Ok(chassis
            .pointer(BACKGROUND_COPY_STATUS_POINTER)
            .and_then(Value::as_str)
            .map(|status| BackgroundCopyComponent {
                uri: uri.to_string(),
                status: status.to_string(),
            }))
    }

    /// Refresh BackgroundCopyStatus for a known list of components.
    ///
    /// RMS wait mode uses this to repoll only components that were previously
    /// incomplete. Missing or failed reads are retained as incomplete synthetic
    /// statuses so the wait loop can retry until timeout.
    async fn read_background_copy_components(
        &self,
        components: &[BackgroundCopyComponent],
        mut json_dict: Option<&mut Value>,
    ) -> Result<Vec<BackgroundCopyComponent>, String> {
        let mut refreshed = Vec::with_capacity(components.len());
        for component in components {
            match self
                .read_background_copy_component(&component.uri, json_dict.as_deref_mut())
                .await
            {
                Ok(Some(latest)) => refreshed.push(latest),
                Ok(None) => refreshed.push(BackgroundCopyComponent {
                    uri: component.uri.clone(),
                    status: "Missing".to_string(),
                }),
                Err(err) => {
                    refreshed.push(Self::background_copy_query_failed_component(
                        component.uri.clone(),
                        err,
                    ));
                }
            }
        }
        Ok(refreshed)
    }

    /// Enforce the BackgroundCopy precondition for CLI and RMS update flows.
    ///
    /// Single-shot mode fails immediately when any discovered component is not
    /// `Completed`. Wait mode polls until all discovered components complete or
    /// the caller-provided timeout expires.
    async fn check_background_copy_status(
        &self,
        mode: UpdatePreconditionMode,
        mut json_dict: Option<&mut Value>,
    ) -> Result<(), String> {
        let UpdatePreconditionMode::Wait { timeout, interval } = mode else {
            let components = self
                .discover_background_copy_components_with_query_retries(json_dict.as_deref_mut())
                .await?;
            let incomplete = Self::incomplete_background_copy_components(&components);
            if incomplete.is_empty() {
                return Ok(());
            }

            return Err(Self::background_copy_not_ready_message(&incomplete));
        };

        let started = Instant::now();
        let (mut discovered, mut incomplete) = match self
            .discover_background_copy_components(json_dict.as_deref_mut())
            .await
        {
            Ok(components) => (
                true,
                Self::incomplete_background_copy_components(&components),
            ),
            Err(err) => (
                false,
                vec![Self::background_copy_query_failed_component(
                    "/redfish/v1/Chassis",
                    err,
                )],
            ),
        };

        if incomplete.is_empty() {
            return Ok(());
        }
        Self::log_background_copy_wait(&incomplete);
        let mut last_wait_log = started;

        loop {
            if started.elapsed() >= timeout {
                return Err(Self::background_copy_timeout_message(&incomplete, timeout));
            }

            let remaining = timeout.saturating_sub(started.elapsed());
            sleep(interval.min(remaining)).await;

            if discovered {
                incomplete = self
                    .read_background_copy_components(&incomplete, json_dict.as_deref_mut())
                    .await?;
                incomplete = Self::incomplete_background_copy_components(&incomplete);
            } else {
                match self
                    .discover_background_copy_components(json_dict.as_deref_mut())
                    .await
                {
                    Ok(components) => {
                        discovered = true;
                        incomplete = Self::incomplete_background_copy_components(&components);
                    }
                    Err(err) => {
                        incomplete = vec![Self::background_copy_query_failed_component(
                            "/redfish/v1/Chassis",
                            err,
                        )];
                    }
                }
            }

            if incomplete.is_empty() {
                return Ok(());
            }
            let now = Instant::now();
            if now.duration_since(last_wait_log) >= BACKGROUND_COPY_WAIT_LOG_INTERVAL {
                Self::log_background_copy_wait(&incomplete);
                last_wait_log = now;
            }
        }
    }

    /// Normalize an inventory AP name for matching against PLDM component keys.
    ///
    /// Mirrors the Python `GHRFTarget.get_component_version` normalization:
    /// strip hgx_fw_ / hgx_ prefix, collapse erot variants, map bmc→hmc for
    /// HGX, gpu→gpu, cpu→sbios, pcie→pcieswitch, then remove underscores.
    /// Returns `(normalized_name, hgx_pkg_only)`.
    fn normalize_ap_name(ap_name: &str) -> (String, bool) {
        let mut name = ap_name.to_lowercase();
        let mut hgx_pkg_only = false;

        if name.starts_with("hgx_") {
            if name.starts_with("hgx_fw_") {
                name = name["hgx_fw_".len()..].to_string();
            } else {
                name = name["hgx_".len()..].to_string();
            }
            hgx_pkg_only = true;
            if name.starts_with("bmc") {
                name = "hmc".to_string();
            }
        }
        if name.contains("erot") {
            name = "erot".to_string();
        }
        if name.contains("gpu") && !name.contains("inforom") && !name.contains("sma") {
            name = "gpu".to_string();
        } else if name.contains("cpu") && !name.contains("sbios") {
            name = "sbios".to_string();
        } else if !hgx_pkg_only && name.contains("pcie") {
            name = "pcieswitch".to_string();
        }
        name = name.replace('_', "");
        (name, hgx_pkg_only)
    }
}

#[async_trait::async_trait]
impl RFTarget for GB200RFTarget {
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
        "GB200RFTarget"
    }

    async fn check_update_preconditions(
        &self,
        mode: UpdatePreconditionMode,
        mut json_dict: Option<&mut Value>,
    ) -> Result<(), String> {
        self.check_write_protect_disabled(json_dict.as_deref_mut())
            .await?;
        self.check_background_copy_status(mode, json_dict).await
    }

    // ------------------------------------------------------------------
    // get_update_uri override
    // ------------------------------------------------------------------

    /// Returns the multipart HTTP push URI from the UpdateService response.
    fn get_update_uri(&self, update_service_response: &Value) -> String {
        if let Some(uri) = update_service_response
            .get("MultipartHttpPushUri")
            .and_then(|v| v.as_str())
        {
            return uri.to_string();
        }
        "/redfish/v1/UpdateService/update-multipart".to_string()
    }

    // ------------------------------------------------------------------
    // make_update_target_json — inherits GH200 behaviour
    // ------------------------------------------------------------------

    /// GB200 inherits `make_update_target_json` from GH200 in Python.
    /// Uses `{"Targets": [...]}` format with HGX_Full.json and BMC_Full.json.
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
            let fp = format!("{}/CPU.json", dir_path);
            if tokio::fs::write(
                &fp,
                serde_json::to_string_pretty(&json!({"Targets": cpu_list})).unwrap(),
            )
            .await
            .is_ok()
            {
                file_list.push(fp);
            }
        }
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
                            let fp = format!("{}/HGX_Full.json", dir_path);
                            if tokio::fs::write(
                                &fp,
                                serde_json::to_string_pretty(&json!({"Targets": [uri]})).unwrap(),
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
    // version_newer — inherits GH200 behaviour (Python: no override)
    // ------------------------------------------------------------------

    /// GB200 inherits `version_newer` from GH200 in Python (no override).
    /// Delegates to `gh200_version_compare` which chains through GH →
    /// RFTarget base comparison.
    fn version_newer(&self, pkg_version: &str, sys_version: &str) -> bool {
        gh200_rftarget::gh200_version_compare(pkg_version, sys_version)
    }

    /// GB200 inherits `get_expected_task_type_from_package` from GH200.
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
    // Abstract method implementations
    // ------------------------------------------------------------------

    /// OOB activation via Redfish for GB200 (inherited from GH200).
    ///
    /// Supported commands:
    ///   RF_PWR_STATUS  – query power state of all systems
    ///   RF_PWR_ON      – power on via ComputerSystem.Reset "On"
    ///   RF_PWR_OFF     – power off via ComputerSystem.Reset "ForceOff"
    ///   RF_PWR_CYCLE   – restart via ComputerSystem.Reset "ForceRestart"
    ///   RF_AUX_PWR_CYCLE – aux power reset via Oem Chassis action
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

    /// Perform factory reset with GH-family parameters.
    ///
    /// GB200 inherits from GH200 → GH, which sends
    /// `{"ResetToDefaultsType": "ResetAll"}` as the reset body.
    async fn factory_reset(&mut self, _reset_params: Option<&Value>) -> (bool, Value) {
        let params = json!({"ResetToDefaultsType": "ResetAll"});
        let (status, response_dict) = self
            .bmc_access
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
            } else {
                Util::bail_nvfwupd(
                    1,
                    "Error: perform_factory_reset could not find any Managers",
                    BailAction::Exit,
                    None,
                );
            }
        }

        let reset_uri = format!(
            "/redfish/v1/Managers/{}/Actions/Manager.ResetToDefaults",
            bmc_id
        );
        let (status, response_dict) = self
            .bmc_access
            .dispatch_request_full("POST", &reset_uri, None, Some(&params), 30, false, None)
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

    /// Perform firmware update via multipart upload.
    ///
    /// GB200 **always** uses multipart upload (never pushuri).
    /// Mirrors the full Python `GB200RFTarget.update_component` flow:
    /// 1. Resolve `-s/--special` (inline JSON or file) for UpdateParameters
    /// 2. Resolve `-o/--oem_parameters` (inline JSON or file) for OemParameters
    /// 3. Generate default Targets JSON only when `special` is absent
    /// 4. Block VRNVL72 (P4109/P4110) staged updates
    /// 5. Merge staged OEM options into UpdateParameters
    /// 6. Pass everything to `update_component_multipart`
    async fn update_component(
        &mut self,
        cmd_args: &CmdArgs,
        update_uri: &str,
        update_file: &str,
        time_out: u64,
        json_dict: Option<&mut Value>,
        parallel_update: bool,
    ) -> Option<String> {
        // --- Resolve OEM parameters (-o/--oem_parameters) ---
        let oem_params_json: Option<Value> = match self
            .resolve_json_or_file(cmd_args.oem_parameters.as_deref(), "Oem Parameters")
            .await
        {
            Ok(Some(s)) => Some(Value::String(s)),
            Ok(None) => None,
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

        // --- Resolve special update parameters (-s/--special) ---
        let mut param_list: Option<String> = match &cmd_args.special {
            Some(vals) if !vals.is_empty() => {
                let first = &vals[0];
                if self.validate_json(first) {
                    Some(first.clone())
                } else if std::path::Path::new(first).is_file() {
                    match tokio::fs::read_to_string(first).await {
                        Ok(contents) => Some(contents),
                        Err(e) => {
                            Util::bail_nvfwupd_threadsafe(
                                1,
                                &format!(
                                    "Failed to open or read given file {} error: ({})",
                                    first, e
                                ),
                                BailAction::DoNothing,
                                None,
                                parallel_update,
                            );
                            return None;
                        }
                    }
                } else {
                    Some(first.clone())
                }
            }
            _ => None,
        };

        // --- Generate default Targets when special is not provided ---
        if param_list.is_none() {
            let file_name = std::path::Path::new(update_file)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(update_file);

            let targets: Value = if Self::requires_multipart(file_name) {
                json!(["/redfish/v1/Chassis/HGX_Chassis_0"])
            } else {
                json!([])
            };
            param_list = Some(serde_json::to_string(&json!({ "Targets": targets })).unwrap());
        }

        // --- Staged update handling ---
        if cmd_args.staged_update || cmd_args.staged_activate_update {
            let file_name = std::path::Path::new(update_file)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(update_file);

            let vr_nvl72_platforms = ["P4109", "P4110"];
            if vr_nvl72_platforms.iter().any(|p| file_name.contains(p)) {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    "Target Platform does not support staged update",
                    BailAction::DoNothing,
                    None,
                    parallel_update,
                );
                return None;
            }

            let mut json_data: Value = if let Some(ref pl) = param_list {
                if self.validate_json(pl) {
                    serde_json::from_str(pl).unwrap_or_else(|_| json!({}))
                } else if std::path::Path::new(pl).is_file() {
                    match tokio::fs::read_to_string(pl).await {
                        Ok(contents) => {
                            serde_json::from_str(&contents).unwrap_or_else(|_| json!({}))
                        }
                        Err(_) => json!({}),
                    }
                } else {
                    json!({})
                }
            } else {
                json!({})
            };

            if cmd_args.staged_update {
                json_data["Oem"] = json!({ "Nvidia": { "UpdateOption": "StageOnly" } });
            } else if cmd_args.staged_activate_update {
                json_data["Oem"] = json!({ "Nvidia": { "UpdateOption": "StageAndActivate" } });
            }
            param_list = Some(serde_json::to_string(&json_data).unwrap());
        }

        // --- Dispatch multipart upload ---
        if let Some(ref pl) = param_list {
            if self.validate_json(pl) {
                let param_value: Value = serde_json::from_str(pl).unwrap_or(json!({}));
                self.update_component_multipart(
                    None,
                    update_uri,
                    update_file,
                    time_out,
                    Some(&param_value),
                    None,
                    oem_params_json.as_ref(),
                    json_dict,
                    parallel_update,
                    cmd_args.quiet,
                )
                .await
            } else {
                let pl_vec = vec![pl.clone()];
                self.update_component_multipart(
                    Some(&pl_vec),
                    update_uri,
                    update_file,
                    time_out,
                    None,
                    None,
                    oem_params_json.as_ref(),
                    json_dict,
                    parallel_update,
                    cmd_args.quiet,
                )
                .await
            }
        } else {
            self.update_component_multipart(
                None,
                update_uri,
                update_file,
                time_out,
                None,
                None,
                oem_params_json.as_ref(),
                json_dict,
                parallel_update,
                cmd_args.quiet,
            )
            .await
        }
    }

    /// GB200-specific fungible component check.
    ///
    /// Mirrors Python `GB200RFTarget.is_fungible_component`:
    /// - Specific SMA/NVMe fragments are always fungible
    /// - CPLD is fungible except HGX CPLD
    /// - "gpu" is fungible unless it also contains inforom/erot/sma/driver
    fn is_fungible_component(&self, component_name: &str) -> bool {
        let lower = component_name.to_lowercase();

        for fragment in GB200_SMA_CHASSIS_SKU_FRAGMENTS {
            if lower.contains(fragment) {
                return true;
            }
        }

        for fragment in GB200_FUNGIBLE_FRAGMENTS {
            if lower.contains(fragment) {
                return true;
            }
        }

        if lower.contains("cpld") && !lower.contains("hgx") {
            return true;
        }

        if lower.contains("gpu")
            && !lower.contains("inforom")
            && !lower.contains("erot")
            && !lower.contains("sma")
            && !lower.contains("driver")
        {
            return true;
        }

        false
    }

    /// Match the AP name in the PLDM version dictionary and return its version.
    ///
    /// The dict has the shape `{pkg_name: {component_key: [version, sku]}}`.
    /// We normalise both the inventory AP name and the PLDM component key,
    /// then do substring matching — the same algorithm as Python's
    /// `GHRFTarget.get_component_version`.
    async fn get_component_version(
        &self,
        pldm_version_dict: &Value,
        ap_name: &str,
        _pkg_parser: Option<&dyn PkgParser>,
    ) -> Option<String> {
        let (norm_ap, hgx_pkg_only) = Self::normalize_ap_name(ap_name);

        let outer = pldm_version_dict.as_object()?;
        let mut ap_version: Option<String> = None;

        for (pkg_name, pkg_dict_val) in outer {
            let pkg_is_hgx = Self::is_hgx_pkg(pkg_name);
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
                    None => continue,
                };

                // Normalise the PLDM component key:
                //   "ERoT,0x4d35368b" -> split(",") -> "ERoT" -> split(":") -> "erot"
                //   "BMC:SKU_178:,0xb2000010" -> split(",") -> "BMC:SKU_178:" -> split(":") -> "bmc"
                //   "CPLD-Backplane:LCMX03LF-I2C:," -> split(",") -> "CPLD-Backplane:LCMX03LF-I2C:" -> split(":") -> "cpld-backplane"
                let temp_pkg = ap_full.split(',').next().unwrap_or("").to_lowercase();
                let ap_pkg_raw = temp_pkg.split(':').next().unwrap_or("");
                let ap_type = temp_pkg.split(':').nth(1).unwrap_or("");
                let ap_pkg = ap_pkg_raw.replace('_', "").replace('-', "");

                let cx9_pkg = if ap_pkg == "cx9" { "fwcx" } else { &ap_pkg };

                if norm_ap.contains("inforom") && !cx9_pkg.contains("inforom") {
                    continue;
                }

                // SMA-specific matching — Python uses `break` (exits inner
                // loop only), not `return`.
                if ap_pkg.contains("sma") && ap_type.contains("cx8") && norm_ap.contains("smacx8") {
                    ap_version = Some(version_str.to_string());
                    break;
                }
                if ap_pkg.contains("sma") && ap_type.contains("sxm7") && norm_ap.contains("smagpu")
                {
                    ap_version = Some(version_str.to_string());
                    break;
                }
                if ap_pkg.contains("sma") && ap_type.contains("gpu") && norm_ap.contains("gpusma") {
                    ap_version = Some(version_str.to_string());
                    break;
                }
                if ap_pkg.contains("sma")
                    && ap_type.contains("hpm")
                    && norm_ap.contains("processormodulesma")
                {
                    ap_version = Some(version_str.to_string());
                    break;
                }

                // SBIOS type-specific matching — also `break`, not `return`.
                if ap_pkg.contains("sbios")
                    && ap_type.contains("fmc")
                    && norm_ap.contains("sbiosfmc")
                {
                    ap_version = Some(version_str.to_string());
                    break;
                }
                if ap_pkg.contains("sbios")
                    && ap_type.contains("fws")
                    && norm_ap.contains("sbiosfw")
                {
                    ap_version = Some(version_str.to_string());
                    break;
                }

                if norm_ap.contains(cx9_pkg) {
                    // General matching — don't break, later entries can overwrite
                    ap_version = Some(version_str.to_string());
                } else if ap_pkg.contains("smr") && norm_ap.contains("fpga") {
                    return Some(version_str.to_string());
                } else {
                    // Fallback: try appending "0" to match e.g. "bmc0" == "fwbmc0"
                    let alt_ap = format!("{}0", cx9_pkg);
                    if alt_ap == norm_ap {
                        return Some(version_str.to_string());
                    }
                }
            }
        }

        ap_version
    }

    /// Query chassis or firmware inventory URI to retrieve the component
    /// identifier used for fungible matching.
    ///
    /// - CPLD → query `/redfish/v1/Chassis/{name}` and return the `Model`
    ///   field (with "BP" expanded to "Backplane").
    /// - SMA  → query `/redfish/v1/Chassis/{name}` and return `SKU`.
    /// - NVMe → follow `RelatedItem` to the Chassis Drive and return `Model`.
    /// - Default (GPU etc.) → follow `RelatedItem` to Chassis and return `SKU`.
    async fn get_identifier_from_chassis(&self, ap_inv_uri: &str) -> Option<String> {
        let ap_name = ap_inv_uri.rsplit('/').next().unwrap_or(ap_inv_uri);
        let ap_lower = ap_name.to_lowercase();

        // SMA components – get SKU from Chassis
        if GB200_SMA_CHASSIS_SKU_FRAGMENTS
            .iter()
            .any(|component| ap_lower.contains(component))
        {
            let ap_chassis = ap_name.replacen("FW_", "", 1);
            let uri = format!("/redfish/v1/Chassis/{}", ap_chassis);
            let (status, sma_dict) = self
                .target_access()
                .dispatch_request("GET", &uri, None, None)
                .await;
            if !status {
                return None;
            }
            return sma_dict
                .get("SKU")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        // CPLD components – get Model from Chassis
        if ap_lower.contains("cpld") {
            let ap_chassis = ap_name.replacen("FW_", "", 1);
            let uri = format!("/redfish/v1/Chassis/{}", ap_chassis);
            let (status, cpld_dict) = self
                .target_access()
                .dispatch_request("GET", &uri, None, None)
                .await;
            if !status {
                return None;
            }
            let model = cpld_dict.get("Model").and_then(|v| v.as_str()).map(|s| {
                if s == "BP" {
                    "Backplane".to_string()
                } else {
                    s.to_string()
                }
            });
            return model;
        }

        // NVMe E1.S drives – follow RelatedItem to Chassis Drive and return Model
        if ap_lower.contains("nvme_e1s") {
            let (status, fw_inv_dict) = self
                .target_access()
                .dispatch_request("GET", ap_inv_uri, None, None)
                .await;
            if !status {
                return None;
            }
            let drive_uri = fw_inv_dict
                .get("RelatedItem")
                .and_then(|v| v.as_array())
                .and_then(|arr| {
                    arr.iter().find_map(|item| {
                        let id = item.get("@odata.id")?.as_str()?;
                        if id.contains("/Chassis/") {
                            Some(id.to_string())
                        } else {
                            None
                        }
                    })
                });
            if let Some(ref du) = drive_uri {
                let (status2, drive_dict) = self
                    .target_access()
                    .dispatch_request("GET", du, None, None)
                    .await;
                if status2 {
                    return drive_dict
                        .get("Model")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
            }
            return None;
        }

        // Default path (GPU etc.) – follow RelatedItem → Chassis → SKU
        let (status, fw_inv_dict) = self
            .target_access()
            .dispatch_request("GET", ap_inv_uri, None, None)
            .await;
        if !status {
            tracing::debug!(
                "get_identifier_from_chassis: failed to query {}",
                ap_inv_uri
            );
            return None;
        }

        let chassis_uri = fw_inv_dict
            .get("RelatedItem")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|item| item.get("@odata.id"))
            .and_then(|v| v.as_str());

        if let Some(uri) = chassis_uri {
            let (status2, chassis_dict) = self
                .target_access()
                .dispatch_request("GET", uri, None, None)
                .await;
            if status2 {
                return chassis_dict
                    .get("SKU")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
        }

        None
    }

    /// Look up version by identifier (SKU / model) in the nested PLDM dict.
    ///
    /// The dict has the shape `{pkg_name: {component_key: [version, sku]}}`.
    /// Matches GPU/SMA by sku_id equality, CPLD by model substring in key,
    /// and NVMe by model match — mirroring
    /// Python `GB200RFTarget.get_version_sku`.
    fn get_version_sku(
        &self,
        identifier: &str,
        pldm_version_dict: &Value,
        ap_name: &str,
    ) -> Option<String> {
        let hgx_component = ap_name.starts_with("hgx_");

        let outer = pldm_version_dict.as_object()?;

        for (pkg_name, pkg_dict_val) in outer {
            let pkg_is_hgx = Self::is_hgx_pkg(pkg_name);
            if hgx_component && !pkg_is_hgx {
                continue;
            }
            if !hgx_component && pkg_is_hgx {
                continue;
            }

            let pkg_dict = match pkg_dict_val.as_object() {
                Some(d) => d,
                None => continue,
            };

            for (pkg_ap, pkg_data_val) in pkg_dict {
                let pkg_data = match pkg_data_val.as_array() {
                    Some(arr) => arr,
                    None => continue,
                };
                let version = pkg_data.first().and_then(|v| v.as_str()).unwrap_or("N/A");
                let sku = pkg_data.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let key_lower = pkg_ap.to_lowercase();

                // GPU matching: raw comparison (Python compares without lowering)
                if key_lower.contains("gpu") && sku == identifier {
                    return Some(version.to_string());
                }
                // SMA matching: raw comparison (Python compares without lowering)
                if key_lower.contains("sma") && sku == identifier {
                    return Some(version.to_string());
                }
                // CPLD matching: identifier (model) must appear in the key (case-insensitive)
                let id_lower = identifier.to_lowercase();
                if key_lower.contains("cpld") && key_lower.contains(&id_lower) {
                    return Some(version.to_string());
                }
                // NVMe matching: normalize and compare model (case-insensitive)
                if key_lower.starts_with("nvme:") {
                    let name_part = pkg_ap.split(',').next().unwrap_or("");
                    if let Some(model_in_pkg) = name_part.split(':').nth(1) {
                        let norm_pkg = model_in_pkg.trim().replace('-', "_").to_lowercase();
                        let norm_id = id_lower.replace('-', "_");
                        if !norm_pkg.is_empty()
                            && !norm_id.is_empty()
                            && norm_id.contains(&norm_pkg)
                        {
                            return Some(version.to_string());
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
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // ---------------------------------------------------------------
    // normalize_ap_name
    // ---------------------------------------------------------------

    #[test]
    fn test_normalize_hgx_fw_bmc() {
        let (name, hgx) = GB200RFTarget::normalize_ap_name("HGX_FW_BMC_0");
        assert_eq!(name, "hmc");
        assert!(hgx);
    }

    #[test]
    fn test_normalize_hgx_erot() {
        let (name, hgx) = GB200RFTarget::normalize_ap_name("HGX_FW_ERoT_GPU_SXM_0");
        assert_eq!(name, "erot");
        assert!(hgx);
    }

    #[test]
    fn test_normalize_hgx_gpu() {
        let (name, hgx) = GB200RFTarget::normalize_ap_name("HGX_FW_GPU_SXM_0");
        assert_eq!(name, "gpu");
        assert!(hgx);
    }

    #[test]
    fn test_normalize_non_hgx_bmc() {
        let (name, hgx) = GB200RFTarget::normalize_ap_name("FW_BMC_0");
        assert_eq!(name, "fwbmc0");
        assert!(!hgx);
    }

    #[test]
    fn test_normalize_pcie_switch() {
        let (name, hgx) = GB200RFTarget::normalize_ap_name("FW_PCIeSwitch_0");
        assert_eq!(name, "pcieswitch");
        assert!(!hgx);
    }

    #[test]
    fn test_normalize_cpu_only() {
        // "CPU" without "sbios" maps to "sbios"
        let (name, hgx) = GB200RFTarget::normalize_ap_name("FW_CPU_0");
        assert_eq!(name, "sbios");
        assert!(!hgx);
    }

    #[test]
    fn test_normalize_cpu_sbios_passthrough() {
        // Already contains "sbios" so the cpu→sbios mapping doesn't fire;
        // the name is kept and underscores stripped.
        let (name, hgx) = GB200RFTarget::normalize_ap_name("FW_CPU_SBIOS_0");
        assert_eq!(name, "fwcpusbios0");
        assert!(!hgx);
    }

    #[test]
    fn test_normalize_inforom() {
        // GPU branch is NOT taken because name also contains "inforom".
        // Result keeps all fragments with underscores stripped.
        let (name, hgx) = GB200RFTarget::normalize_ap_name("HGX_FW_GPU_SXM_InfoROM_0");
        assert_eq!(name, "gpusxminforom0");
        assert!(hgx);
    }

    // ---------------------------------------------------------------
    // is_hgx_pkg
    // ---------------------------------------------------------------

    #[test]
    fn test_is_hgx_pkg_true() {
        assert!(GB200RFTarget::is_hgx_pkg("GB200-P4972_0009-HGX"));
        assert!(GB200RFTarget::is_hgx_pkg("Something-4059-image"));
        assert!(GB200RFTarget::is_hgx_pkg("HMC_firmware"));
    }

    #[test]
    fn test_is_hgx_pkg_false() {
        assert!(!GB200RFTarget::is_hgx_pkg("BMC-update-001"));
        assert!(!GB200RFTarget::is_hgx_pkg("random_firmware"));
    }

    // ---------------------------------------------------------------
    // requires_multipart
    // ---------------------------------------------------------------

    #[test]
    fn test_requires_multipart() {
        assert!(GB200RFTarget::requires_multipart("P4059_update.fwpkg"));
        assert!(GB200RFTarget::requires_multipart("hgx_firmware.bin"));
        assert!(GB200RFTarget::requires_multipart("HMC_update.fwpkg"));
        assert!(!GB200RFTarget::requires_multipart("random_file.bin"));
    }

    #[test]
    fn firmware_inventory_member_uris_skips_malformed_members() {
        let inventory = json!({
            "Members": [
                {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0"},
                {"Name": "missing uri"},
                {"@odata.id": 42},
                {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"}
            ]
        });

        let uris = GB200RFTarget::firmware_inventory_member_uris(&inventory).unwrap();

        assert_eq!(
            uris,
            vec![
                "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0".to_string(),
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0".to_string(),
            ]
        );
    }

    async fn mock_write_protect_disabled(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/Chassis_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "HardwareWriteProtectEnable": false
                    }
                }
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0"},
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"},
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_CPLD_0"}
                ]
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "WriteProtected": false
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "WriteProtected": false
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/FW_CPLD_0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Version": "0001"
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn write_protect_precondition_rejects_global_enabled() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/Chassis_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "HardwareWriteProtectEnable": true
                    }
                }
            })))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        let err = target
            .check_update_preconditions(UpdatePreconditionMode::SingleShot, None)
            .await
            .expect_err("global hardware write protect should block update");
        assert!(err.contains("Hardware write protect is enabled"));
        assert!(err.contains("/redfish/v1/Chassis/Chassis_0"));
        assert!(err.contains("HardwareWriteProtectEnable=true"));
    }

    #[tokio::test]
    async fn write_protect_precondition_allows_missing_global_chassis() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/Chassis_0"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "error": "not found"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "WriteProtected": false
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": []
            })))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        target
            .check_update_preconditions(UpdatePreconditionMode::SingleShot, None)
            .await
            .expect("missing global Chassis_0 should not block update");
    }

    #[tokio::test]
    async fn write_protect_precondition_allows_missing_global_field() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/Chassis_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {}
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "WriteProtected": false
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": []
            })))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        target
            .check_update_preconditions(UpdatePreconditionMode::SingleShot, None)
            .await
            .expect("missing global write-protect field should not block update");
    }

    #[tokio::test]
    async fn write_protect_precondition_rejects_component_enabled() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/Chassis_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "HardwareWriteProtectEnable": false
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"},
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_CPLD_0"}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "WriteProtected": true
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/FW_CPLD_0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "WriteProtected": false
            })))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        let err = target
            .check_update_preconditions(UpdatePreconditionMode::SingleShot, None)
            .await
            .expect_err("component write protect should block update");
        assert!(err.contains("Firmware component write protect is enabled"));
        assert!(err.contains("/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"));
        assert!(err.contains("WriteProtected=true"));
        assert!(!err.contains("FW_CPLD_0 WriteProtected=true"));
    }

    #[tokio::test]
    async fn write_protect_precondition_skips_unqueryable_component() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/Chassis_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "HardwareWriteProtectEnable": false
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"},
                    {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_CPLD_0"}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
            ))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": "temporarily unavailable"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/UpdateService/FirmwareInventory/FW_CPLD_0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "WriteProtected": false
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": []
            })))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        target
            .check_update_preconditions(UpdatePreconditionMode::SingleShot, None)
            .await
            .expect("unqueryable component should warn and be skipped");
    }

    #[tokio::test]
    async fn background_copy_precondition_passes_when_completed() {
        let server = MockServer::start().await;
        mock_write_protect_disabled(&server).await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/HGX_Chassis_0"}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/HGX_Chassis_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "BackgroundCopyStatus": "Completed"
                    }
                }
            })))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        target
            .check_update_preconditions(UpdatePreconditionMode::SingleShot, None)
            .await
            .expect("completed background copy should allow update");
    }

    #[tokio::test]
    async fn background_copy_precondition_rejects_in_progress_status() {
        let server = MockServer::start().await;
        mock_write_protect_disabled(&server).await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/HGX_Chassis_0"},
                    {"@odata.id": "/redfish/v1/Chassis/BMC_0"}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/HGX_Chassis_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "BackgroundCopyStatus": "Running"
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/BMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        let err = target
            .check_update_preconditions(UpdatePreconditionMode::SingleShot, None)
            .await
            .expect_err("in-progress background copy should block update");
        assert!(err.contains("BackgroundCopy is currently in progress"));
        assert!(err.contains("/redfish/v1/Chassis/HGX_Chassis_0 status=Running"));
        assert!(!err.contains("/redfish/v1/Chassis/BMC_0"));
    }

    #[tokio::test]
    async fn background_copy_precondition_single_shot_retries_transient_query_failure() {
        let server = MockServer::start().await;
        mock_write_protect_disabled(&server).await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/ERoT_BMC_0"}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/ERoT_BMC_0"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": "temporarily unavailable"
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/ERoT_BMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "BackgroundCopyStatus": "Completed"
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        target
            .check_update_preconditions(UpdatePreconditionMode::SingleShot, None)
            .await
            .expect("single-shot mode should retry transient query failures");
    }

    #[tokio::test]
    async fn background_copy_precondition_wait_reports_timeout() {
        let server = MockServer::start().await;
        mock_write_protect_disabled(&server).await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/HGX_Chassis_0"}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/HGX_Chassis_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "BackgroundCopyStatus": "Running"
                    }
                }
            })))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        let err = target
            .check_update_preconditions(
                UpdatePreconditionMode::Wait {
                    timeout: Duration::from_millis(5),
                    interval: Duration::from_millis(1),
                },
                None,
            )
            .await
            .expect_err("in-progress background copy should time out");
        assert!(err.contains("BackgroundCopyStatus did not reach Completed"));
        assert!(err.contains("/redfish/v1/Chassis/HGX_Chassis_0 status=Running"));
    }

    #[tokio::test]
    async fn background_copy_precondition_wait_retries_transient_component_query_failure() {
        let server = MockServer::start().await;
        mock_write_protect_disabled(&server).await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/ERoT_BMC_0"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/ERoT_BMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "BackgroundCopyStatus": "Running"
                    }
                }
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/ERoT_BMC_0"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": "temporarily unavailable"
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/ERoT_BMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "BackgroundCopyStatus": "Completed"
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        target
            .check_update_preconditions(
                UpdatePreconditionMode::Wait {
                    timeout: Duration::from_millis(50),
                    interval: Duration::from_millis(1),
                },
                None,
            )
            .await
            .expect("wait mode should retry transient component query failures");
    }

    #[tokio::test]
    async fn background_copy_precondition_wait_repolls_only_incomplete_components() {
        let server = MockServer::start().await;
        mock_write_protect_disabled(&server).await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/HGX_IRoT_GPU_0"},
                    {"@odata.id": "/redfish/v1/Chassis/HGX_IRoT_GPU_1"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/HGX_IRoT_GPU_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "BackgroundCopyStatus": "Completed"
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/HGX_IRoT_GPU_1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Oem": {
                    "Nvidia": {
                        "BackgroundCopyStatus": "Running"
                    }
                }
            })))
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        let err = target
            .check_update_preconditions(
                UpdatePreconditionMode::Wait {
                    timeout: Duration::from_millis(5),
                    interval: Duration::from_millis(1),
                },
                None,
            )
            .await
            .expect_err("in-progress background copy should time out");
        assert!(err.contains("/redfish/v1/Chassis/HGX_IRoT_GPU_1 status=Running"));
        assert!(!err.contains("/redfish/v1/Chassis/HGX_IRoT_GPU_0"));
    }

    #[tokio::test]
    async fn update_component_posts_multipart_and_records_task_id() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update-multipart"))
            .and(body_string_contains("UpdateFile"))
            .and(body_string_contains("UpdateParameters"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "Id": "Task-GB200"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("P4059_update.fwpkg");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();

        let mut target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );
        let cmd_args = CmdArgs {
            cmd: "update_fw".to_string(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special: None,
            oem_parameters: None,
        };
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });

        let task_id = target
            .update_component(
                &cmd_args,
                "/redfish/v1/UpdateService/update-multipart",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                false,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("Task-GB200"));
        assert_eq!(output["Output"][0]["Id"], "Task-GB200");
    }

    #[tokio::test]
    async fn update_component_extracts_task_id_from_odata_id() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update-multipart"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "@odata.id": "/redfish/v1/TaskService/Tasks/Task-Odata"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = tempfile::tempdir().unwrap();
        let update_file = tmp.path().join("P4059_update.fwpkg");
        tokio::fs::write(&update_file, b"firmware").await.unwrap();

        let mut target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );
        let cmd_args = CmdArgs {
            cmd: "update_fw".to_string(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: false,
            special: None,
            oem_parameters: None,
        };
        let mut output = json!({
            "Output": [],
            "Error": [],
            "Error Code": 0
        });

        let task_id = target
            .update_component(
                &cmd_args,
                "/redfish/v1/UpdateService/update-multipart",
                update_file.to_str().unwrap(),
                30,
                Some(&mut output),
                false,
            )
            .await;

        assert_eq!(task_id.as_deref(), Some("Task-Odata"));
        assert_eq!(
            output["Output"][0]["@odata.id"],
            "/redfish/v1/TaskService/Tasks/Task-Odata"
        );
    }

    // ---------------------------------------------------------------
    // is_fungible_component
    // ---------------------------------------------------------------

    #[test]
    fn test_fungible_gpu() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        assert!(target.is_fungible_component("HGX_FW_GPU_SXM_0"));
    }

    #[test]
    fn test_fungible_sma() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        assert!(target.is_fungible_component("FW_IO_Board_SMA_0"));
        assert!(target.is_fungible_component("FW_Bay_C_SMA_0"));
        assert!(target.is_fungible_component("FW_StorageBackplane_SMA_0"));
    }

    #[tokio::test]
    async fn test_bay_c_sma_identifier_uses_chassis_sku() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/Bay_C_SMA_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "SKU": "bay-c-sku"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let target = GB200RFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-gb200"),
            None,
        );

        let identifier = target
            .get_identifier_from_chassis(
                "/redfish/v1/UpdateService/FirmwareInventory/FW_Bay_C_SMA_0",
            )
            .await;

        assert_eq!(identifier.as_deref(), Some("bay-c-sku"));
    }

    #[test]
    fn test_fungible_cpld_not_hgx() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        assert!(target.is_fungible_component("FW_CPLD_MB_0"));
        assert!(!target.is_fungible_component("HGX_FW_CPLD_0"));
    }

    #[test]
    fn test_not_fungible_erot() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        assert!(!target.is_fungible_component("HGX_FW_ERoT_GPU_SXM_0"));
    }

    // ---------------------------------------------------------------
    // get_component_version
    // ---------------------------------------------------------------

    fn make_pldm_version_dict() -> Value {
        json!({
            "GB200-P4972_0009-HGX": {
                "HMC:SKU_178:,0xb2000010": ["25.05-1", "sku_178"],
                "ERoT,0x4d35368b": ["01.01.00.04", ""],
                "GPU:SXM7:,0x7a220010": ["570.86.15", "2324-890-a1"],
                "CPLD-Backplane:LCMX03LF-I2C:,0x48150013": ["0x00001647", "backplane"],
                "NVMe:Generic_Ent_NVMe_Model:,0x22220001": ["2.1.0", "generic_ent_nvme_model"],
                "SMA:GPU:,0x88001234": ["4.0.2", "sxm7_sku"],
                "SMA:CX8:,0x88002345": ["3.0.1", "cx8_sku"],
                "InfoROM:SXM7:,0x7a330000": ["G536.0200.00.04", ""],
                "SMR:FPGA:,0x44440001": ["2.0.0", ""],
                "SBIOS:FMC:,0x55550001": ["1.2.3", ""]
            },
            "GB200-BMC-001": {
                "BMC,0x12340001": ["26.03-1", "bmc_sku"]
            }
        })
    }

    #[tokio::test]
    async fn test_get_component_version_hgx_bmc() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target
            .get_component_version(&dict, "HGX_FW_BMC_0", None)
            .await;
        assert_eq!(result, Some("25.05-1".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_hgx_erot() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target
            .get_component_version(&dict, "HGX_FW_ERoT_GPU_SXM_0", None)
            .await;
        assert_eq!(result, Some("01.01.00.04".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_hgx_gpu() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target
            .get_component_version(&dict, "HGX_FW_GPU_SXM_0", None)
            .await;
        assert_eq!(result, Some("570.86.15".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_hgx_inforom() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        // AP "HGX_FW_GPU_SXM_InfoROM_0" → norm="gpusxminforom0"
        // PLDM key "InfoROM:SXM7:" → cx9_pkg="inforom"
        // "gpusxminforom0".contains("inforom") → true
        let result = target
            .get_component_version(&dict, "HGX_FW_GPU_SXM_InfoROM_0", None)
            .await;
        assert_eq!(result, Some("G536.0200.00.04".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_non_hgx_bmc() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target.get_component_version(&dict, "FW_BMC_0", None).await;
        assert_eq!(result, Some("26.03-1".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_hgx_cpld() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target
            .get_component_version(&dict, "HGX_FW_CPLD_Backplane_0", None)
            .await;
        assert_eq!(result, Some("0x00001647".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_nvme() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        // "NVMe:Generic_..." normalizes to "nvme", AP "HGX_FW_NVMe_E1S_0" → "nvmee1s0"
        // "nvmee1s0".contains("nvme") → true
        let result = target
            .get_component_version(&dict, "HGX_FW_NVMe_E1S_0", None)
            .await;
        assert_eq!(result, Some("2.1.0".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_sma_gpu() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        // "SMA:GPU:" → ap_pkg="sma", ap_type="gpu"
        // AP "HGX_FW_GPU_SMA_0" → norm="gpusma0"
        // sma && gpu && norm_ap.contains("gpusma") → true
        let result = target
            .get_component_version(&dict, "HGX_FW_GPU_SMA_0", None)
            .await;
        assert_eq!(result, Some("4.0.2".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_fpga_via_smr() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target
            .get_component_version(&dict, "HGX_FW_FPGA_0", None)
            .await;
        assert_eq!(result, Some("2.0.0".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_sbios_fmc() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target
            .get_component_version(&dict, "HGX_FW_CPU_SBIOS_FMC_0", None)
            .await;
        assert_eq!(result, Some("1.2.3".to_string()));
    }

    #[tokio::test]
    async fn test_get_component_version_no_match() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target
            .get_component_version(&dict, "HGX_FW_UnknownDevice_0", None)
            .await;
        assert_eq!(result, None);
    }

    // ---------------------------------------------------------------
    // get_version_sku
    // ---------------------------------------------------------------

    #[test]
    fn test_get_version_sku_gpu_by_sku() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        // identifier "2324-890-a1" matches sku "2324-890-a1" (both lowercase)
        let result = target.get_version_sku("2324-890-a1", &dict, "hgx_fw_gpu_sxm_0");
        assert_eq!(result, Some("570.86.15".to_string()));
    }

    #[test]
    fn test_get_version_sku_cpld_by_model() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        // key "CPLD-Backplane:..." lowered contains "backplane"
        let result = target.get_version_sku("backplane", &dict, "hgx_fw_cpld_backplane_0");
        assert_eq!(result, Some("0x00001647".to_string()));
    }

    #[test]
    fn test_get_version_sku_nvme_by_model() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target.get_version_sku("Generic_Ent_NVMe_Model", &dict, "hgx_fw_nvme_e1s_0");
        assert_eq!(result, Some("2.1.0".to_string()));
    }

    #[test]
    fn test_get_version_sku_no_match() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target.get_version_sku("unknown_sku", &dict, "hgx_some_component");
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_version_sku_sma_by_sku() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        // SMA matching: sku "sxm7_sku" matches identifier "sxm7_sku"
        let result = target.get_version_sku("sxm7_sku", &dict, "hgx_fw_gpu_sma_0");
        assert_eq!(result, Some("4.0.2".to_string()));
    }

    // ---------------------------------------------------------------
    // HGX vs non-HGX filtering
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_hgx_component_sees_only_hgx_pkg() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target
            .get_component_version(&dict, "HGX_FW_BMC_0", None)
            .await;
        assert_eq!(
            result,
            Some("25.05-1".to_string()),
            "HGX BMC should come from HGX pkg"
        );
    }

    #[tokio::test]
    async fn test_non_hgx_component_sees_only_non_hgx_pkg() {
        let bmc = BmcAccess::default_stub();
        let target = GB200RFTarget::new(bmc, None);
        let dict = make_pldm_version_dict();
        let result = target.get_component_version(&dict, "FW_BMC_0", None).await;
        assert_eq!(
            result,
            Some("26.03-1".to_string()),
            "Non-HGX BMC should come from BMC pkg"
        );
    }
}
