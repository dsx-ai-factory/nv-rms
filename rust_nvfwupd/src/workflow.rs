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

//! Public request and outcome types for library callers.

use std::fmt;

use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub type Result<T> = std::result::Result<T, NvFwUpdError>;

/// Error type returned by the NVFWUPD workflow API.
#[derive(Debug, Error)]
pub enum NvFwUpdError {
    /// Generic workflow error when no more specific category applies.
    #[error("{0}")]
    Message(String),
    /// Transport-level failure while performing the named operation.
    #[error("transport error during {operation}: {message}")]
    Transport {
        /// Operation that was running when the transport failed.
        operation: &'static str,
        /// Sanitized transport error message.
        message: String,
    },
    /// Target BMC or management endpoint could not be reached.
    #[error("BMC unreachable for {target}: {message}")]
    BmcUnreachable {
        /// Target endpoint or logical target name.
        target: String,
        /// Sanitized reachability error message.
        message: String,
    },
    /// Credentials were rejected by the target.
    #[error("authentication failed for {target}: {message}")]
    AuthFailed {
        /// Target endpoint or logical target name.
        target: String,
        /// Sanitized authentication failure message.
        message: String,
    },
    /// Firmware package parsing failed before an update could be launched.
    #[error("package parse error for {path}: {message}")]
    PackageParse {
        /// Package path that failed to parse.
        path: String,
        /// Parser failure message.
        message: String,
    },
    /// Target-side update task failed or reported terminal failure details.
    #[error("task failed for {task_id:?}: {message}")]
    TaskFailed {
        /// Task id when one was created before the failure.
        task_id: Option<String>,
        /// Sanitized task failure summary.
        message: String,
    },
    /// Operation exceeded its configured timeout.
    #[error("operation {operation} timed out after {seconds}s")]
    Timeout {
        /// Operation that timed out.
        operation: &'static str,
        /// Timeout duration in seconds.
        seconds: u64,
    },
    /// Target returned a response shape the workflow could not interpret.
    #[error("invalid response for {context}: {message}")]
    InvalidResponse {
        /// Response context being parsed.
        context: &'static str,
        /// Sanitized parsing/validation message.
        message: String,
    },
    /// IPMI activation was requested but ipmitool could not be used.
    #[error("IPMI unavailable: {message}")]
    IpmiNotAvailable {
        /// Sanitized IPMI availability message.
        message: String,
    },
    /// Version comparison failed for a component.
    #[error("version comparison failed for {component}: {message}")]
    VersionComparison {
        /// Component whose versions could not be compared.
        component: String,
        /// Sanitized comparison failure message.
        message: String,
    },
    /// Requested workflow is not supported for the selected target.
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
}

/// Connection and platform information for one firmware target.
#[derive(Clone, PartialEq, Eq)]
pub struct TargetConfig {
    /// BMC or switch management endpoint.
    pub ip: String,
    /// Username used for Redfish, NVUE, SSH, or IPMI as needed by the target.
    pub username: String,
    /// Password for the target credentials. Redacted from `Debug`.
    pub password: String,
    /// Optional management port override.
    pub port: Option<u16>,
    /// Platform family that selects the NVFWUPD target implementation.
    pub server_type: ServerType,
    /// Whether TLS certificates should be verified for HTTPS requests.
    pub verify_tls: bool,
}

impl fmt::Debug for TargetConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetConfig")
            .field("ip", &self.ip)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("port", &self.port)
            .field("server_type", &self.server_type)
            .field("verify_tls", &self.verify_tls)
            .finish()
    }
}

/// Platform families supported by the workflow API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerType {
    /// DGX Redfish target.
    DGX,
    /// DGX Rubin Redfish target.
    DGXRubin,
    /// Generic HGX Redfish target.
    HGX,
    /// MGX Redfish target.
    MGX,
    /// GH200 Redfish target.
    GH200,
    /// HGX B100 Redfish target.
    HGXB100,
    /// HGX B300 Redfish target.
    HGXB300,
    /// HGX Rubin Redfish target.
    HGXRubin,
    /// GB200 compute Redfish target.
    GB200,
    /// GB300 compute Redfish target.
    GB300,
    /// VR NVL72 compute Redfish target.
    VRNVL72,
    /// MGX NVL Redfish target.
    MGXNVL,
    /// GB200 switch NVUE/Redfish target.
    GB200Switch,
    /// GB300 switch NVUE/Redfish target.
    GB300Switch,
    /// VR NVL72 switch NVUE/Redfish target.
    VRNVL72Switch,
    /// PowerShelf Redfish target.
    PowerShelf,
}

/// Request to update one firmware component or package on a target.
#[derive(Debug, Clone)]
pub struct FirmwareUpdateRequest {
    /// Friendly component name, Redfish target URI, or empty for package-derived targets.
    pub component: String,
    /// Local firmware package path.
    pub firmware_file: String,
    /// Whether the platform should force the update when supported.
    pub force_update: bool,
    /// Optional platform-specific update controls.
    pub options: UpdateOptions,
    /// Optional cancellation token for long-running workflows.
    pub cancellation: Option<CancellationToken>,
}

/// Optional controls used to build platform-specific update parameters.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpdateOptions {
    /// Staging behavior requested by callers that support staged updates.
    pub staged: StagedMode,
    /// Caller-provided UpdateParameters JSON to preserve and augment.
    pub special_json: Option<Value>,
    /// Additional OEM parameters reserved for platform-specific flows.
    pub oem_parameters: Option<Value>,
    /// Raw LiteOn PSU device id for single-PSU PowerShelf updates.
    pub liteon_device_id: Option<String>,
    /// Redfish `ApplyTime` value such as `Immediate` or `OnReset`.
    pub apply_time: Option<String>,
}

/// Staging mode requested for update flows that support staging.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StagedMode {
    /// No staging mode requested.
    #[default]
    None,
    /// Stage the firmware without activation.
    StageOnly,
    /// Stage firmware and request activation when the target supports it.
    StageAndActivate,
}

/// Result of launching a firmware update.
#[derive(Debug, Clone, PartialEq)]
pub enum FirmwareUpdateOutcome {
    /// The target created a task and the caller should poll it.
    Started(TaskHandle),
    /// The update completed synchronously or through target-owned monitoring.
    Completed(UpdateSummary),
    /// No update was needed or the target explicitly skipped the request.
    Skipped { reason: String },
}

/// Handle for a target-side firmware update task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskHandle {
    /// Redfish TaskService id, NVUE action id, or synthetic aggregate id.
    pub task_id: String,
}

/// Synchronous update completion summary.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateSummary {
    /// Human-readable completion message.
    pub message: String,
    /// Task ids observed while completing the update.
    pub task_ids: Vec<String>,
    /// Structured target details preserved for RMS/debugging.
    pub details: Value,
}

/// Request to activate firmware that was staged or handed off.
#[derive(Debug, Clone)]
pub struct ActivationRequest {
    /// Activation workflow to run for the target.
    pub mode: ActivationMode,
    /// Optional cancellation token for long-running activation workflows.
    pub cancellation: Option<CancellationToken>,
}

/// Activation workflows supported by NVFWUPD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationMode {
    /// Run one explicit legacy activation command.
    SingleCommand(ActivationCommand),
    /// Run the full GB200 compute activation flow.
    FullGb200Compute,
    /// Run the NVUE switch power-cycle activation flow.
    SwitchPowerCycle,
    /// Gracefully reset or force-reset a PowerShelf.
    PowerShelfReset { force: bool },
}

/// Legacy activation commands accepted by the CLI-compatible path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationCommand {
    /// Power on the target.
    RfPowerOn,
    /// Power off the target.
    RfPowerOff,
    /// Power-cycle the target.
    RfPowerCycle,
    /// Auxiliary power-cycle for platforms that require it.
    RfAuxPowerCycle,
    /// Query target power status.
    RfPowerStatus,
    /// Gracefully reset a PowerShelf.
    RfPowerShelfReset,
    /// Force reset a PowerShelf.
    RfPowerShelfResetForce,
}

impl ActivationCommand {
    /// Return the CLI command string used by the existing NVFWUPD command path.
    pub fn as_cli_command(self) -> &'static str {
        match self {
            Self::RfPowerOn => "RF_PWR_ON",
            Self::RfPowerOff => "RF_PWR_OFF",
            Self::RfPowerCycle => "RF_PWR_CYCLE",
            Self::RfAuxPowerCycle => "RF_AUX_PWR_CYCLE",
            Self::RfPowerStatus => "RF_PWR_STATUS",
            Self::RfPowerShelfReset => "RF_PWRSHELF_RESET",
            Self::RfPowerShelfResetForce => "RF_PWRSHELF_RESET_FORCE",
        }
    }
}

/// Activation completion summary.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivationSummary {
    /// Human-readable activation message.
    pub message: String,
    /// Structured activation details preserved for RMS/debugging.
    pub details: Value,
}

/// Request to compare installed firmware versions against package metadata.
#[derive(Debug, Clone)]
pub struct FirmwareVersionCheckRequest {
    /// Local firmware packages used for the update.
    pub firmware_files: Vec<String>,
}

/// One applied firmware target to verify after activation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirmwareVersionCheckTarget {
    pub component: String,
    /// Local firmware package used for the update.
    pub firmware_file: String,
}

/// Summary of package-to-system firmware version comparison.
#[derive(Debug, Clone, PartialEq)]
pub struct FirmwareVersionCheckSummary {
    /// True when every comparable component in the requested scope reports the package version.
    ///
    /// This is also true for unverified packages that do not expose parseable
    /// package metadata; callers can inspect `details.status` for that case.
    pub matched: bool,
    /// Structured comparison details for RMS/debugging.
    pub details: Value,
}

/// Firmware inventory entry returned by the workflow API.
#[derive(Debug, Clone, PartialEq)]
pub struct FirmwareComponent {
    /// Component name from inventory, falling back to the inventory path leaf.
    pub name: String,
    /// Reported firmware version when available.
    pub version: Option<String>,
    /// Optional device class/type from inventory.
    pub device_class: Option<String>,
    /// Source inventory path for the component.
    pub inventory_path: Option<String>,
    /// Full structured inventory response for callers that need platform fields.
    pub details: Value,
}

/// Normalized task status returned by the workflow API.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskStatus {
    /// Task/action id being reported.
    pub task_id: String,
    /// Normalized terminal/running state.
    pub state: TaskState,
    /// Optional status or failure message.
    pub message: Option<String>,
    /// Optional normalized progress percentage.
    pub progress_percent: Option<u8>,
    /// Full structured task response for RMS/debugging.
    pub details: Value,
}

/// Normalized task state used across Redfish, NVUE, and synthetic tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// State could not be determined from the target response.
    Unknown,
    /// Task has been accepted but is not running yet.
    Pending,
    /// Task is actively running.
    Running,
    /// Task completed successfully or reached a synthetic successful handoff.
    Completed,
    /// Task failed according to target status or structured messages.
    Failed,
    /// Task was cancelled by the target or caller.
    Cancelled,
}

/// Factory-reset parameters passed through to platform implementations.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResetParams {
    /// Platform-specific reset parameters.
    pub details: Value,
}

/// Factory-reset completion report.
#[derive(Debug, Clone, PartialEq)]
pub struct ResetReport {
    /// Human-readable reset message.
    pub message: String,
    /// Structured reset response details.
    pub details: Value,
}

/// Background-copy parameters passed through to platform implementations.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BgCopyParams {
    /// Platform-specific background-copy parameters.
    pub details: Value,
}

/// Background-copy completion report.
#[derive(Debug, Clone, PartialEq)]
pub struct BgCopyReport {
    /// Human-readable background-copy message.
    pub message: String,
    /// Structured background-copy response details.
    pub details: Value,
}
