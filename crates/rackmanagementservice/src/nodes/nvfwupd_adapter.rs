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

use std::sync::LazyLock;

use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

use crate::domain::node::{
    FirmwareActivationCommand, FirmwareActivationMode, FirmwareActivationRequest,
    FirmwareActivationSummary, FirmwareInfo, FirmwareTarget, FirmwareTaskHandle,
    FirmwareTaskRejection, FirmwareTaskStatus, FirmwareType, FirmwareUpdateOptions,
    FirmwareUpdateOutcome, FirmwareUpdateSummary, FirmwareVersionCheckSummary, NodeType,
    NvfwupdServerProfile,
};
use crate::domain::rack::EndpointCredentials;
use crate::utilities::error::RmsError;

use nvfwupd::workflow as nv;

impl From<crate::domain::compute_timing::ComputeFirmwareTiming>
    for nvfwupd::compute_timing::ComputeFirmwareTiming
{
    fn from(timing: crate::domain::compute_timing::ComputeFirmwareTiming) -> Self {
        Self {
            connect_timeout: timing.connect_timeout,
            request_timeout: timing.request_timeout,
            upload_timeout: timing.upload_timeout,
            reachability_timeout: timing.reachability_timeout,
            precondition_query_timeout: timing.precondition_query_timeout,
            background_copy_timeout: timing.background_copy_timeout,
            background_copy_poll_interval: timing.background_copy_poll_interval,
            write_protect_inventory_attempts: timing.write_protect_inventory_attempts,
            write_protect_inventory_retry_interval: timing.write_protect_inventory_retry_interval,
            power_off_delay: timing.power_off_delay,
            ac_cycle_attempts: timing.ac_cycle_attempts,
            ac_cycle_stabilization: timing.ac_cycle_stabilization,
            activation_command_retries: timing.activation_command_retries,
            activation_command_retry_interval: timing.activation_command_retry_interval,
            bmc_recovery_attempts: timing.bmc_recovery_attempts,
            bmc_recovery_interval: timing.bmc_recovery_interval,
            power_on_attempts: timing.power_on_attempts,
            power_on_interval: timing.power_on_interval,
            power_on_resend_interval: timing.power_on_resend_interval,
            recovery_probe_timeout: timing.recovery_probe_timeout,
            inventory_recovery_retries: timing.inventory_recovery_retries,
            inventory_recovery_retry_interval: timing.inventory_recovery_retry_interval,
        }
    }
}

impl From<&crate::domain::compute_power::ComputePowerPaths>
    for nvfwupd::compute_power::ComputePowerPaths
{
    fn from(paths: &crate::domain::compute_power::ComputePowerPaths) -> Self {
        Self {
            system_state_path: paths
                .system_state_path
                .as_ref()
                .map(|path| path.as_str().into()),
            system_reset_path: paths
                .system_reset_path
                .as_ref()
                .map(|path| path.as_str().into()),
            aux_power_cycle_path: paths
                .aux_power_cycle_path
                .as_ref()
                .map(|path| path.as_str().into()),
            bmc_ready_path: paths
                .bmc_ready_path
                .as_ref()
                .map(|path| path.as_str().into()),
        }
    }
}

fn to_target_config(
    server_profile: NvfwupdServerProfile,
    host: &str,
    port: u16,
    username: String,
    password: String,
    bmc_session_token: Option<String>,
    verify_tls: bool,
) -> nv::TargetConfig {
    let target = nv::TargetConfig {
        ip: host.to_owned(),
        username,
        password,
        session_token: None,
        port: Some(port),
        server_type: to_server_type(server_profile),
        verify_tls,
        allow_http: false,
        ssh_known_hosts: None,
        // RMS does not manage an NVFWUPD known_hosts file yet, so preserve the
        // existing RMS SSH/SFTP behavior until RMS can provide explicit policy.
        ssh_host_key_mode: nv::SshHostKeyMode::Disabled,
    };

    if let Some(token) = bmc_session_token {
        target.with_bmc_session_token(token)
    } else {
        target
    }
}

pub fn to_target_config_with_secret(
    node_type: NodeType,
    host: &str,
    port: u16,
    user: &str,
    password: &SecretString,
    verify_tls: bool,
) -> nv::TargetConfig {
    to_target_config(
        node_type.nvfwupd_server_profile(),
        host,
        port,
        user.to_owned(),
        password.expose_secret().to_owned(),
        None,
        verify_tls,
    )
}

/// Build an NVFWUPD target from an explicitly configured server profile.
///
/// Generic profile-driven node adapters use this entrypoint so their runtime
/// behavior does not depend on a hard-coded [`NodeType`] mapping.
pub fn to_target_config_for_server_profile_with_secret(
    server_profile: NvfwupdServerProfile,
    host: &str,
    port: u16,
    user: &str,
    password: &SecretString,
    verify_tls: bool,
) -> nv::TargetConfig {
    to_target_config(
        server_profile,
        host,
        port,
        user.to_owned(),
        password.expose_secret().to_owned(),
        None,
        verify_tls,
    )
}

/// Convert optional RMS BMC credentials into an NVFWUPD target configuration.
pub fn to_target_config_with_credentials(
    node_type: NodeType,
    host: &str,
    port: u16,
    credentials: Option<&EndpointCredentials>,
    verify_tls: bool,
) -> nv::TargetConfig {
    let (username, password, bmc_session_token) = match credentials {
        Some(EndpointCredentials::UserPass { username, password }) => (
            username.to_owned(),
            password.expose_secret().to_owned(),
            None,
        ),
        Some(EndpointCredentials::BmcSessionToken(token)) => (
            String::new(),
            String::new(),
            Some(token.expose_secret().to_owned()),
        ),
        None => (String::new(), String::new(), None),
    };

    to_target_config(
        node_type.nvfwupd_server_profile(),
        host,
        port,
        username,
        password,
        bmc_session_token,
        verify_tls,
    )
}

/// Render a resolved destination only at the NVFWUPD transport boundary.
pub(crate) fn firmware_target_selector(target: &FirmwareTarget) -> &str {
    use crate::domain::firmware_target::FirmwareDestination;
    match target
        .resolution
        .as_ref()
        .map(|resolved| &resolved.destination)
    {
        Some(FirmwareDestination::Redfish(path)) => path.as_str(),
        Some(FirmwareDestination::Multipart(paths)) => match paths.as_slice() {
            [path] => path.as_str(),
            _ => "",
        },
        Some(FirmwareDestination::DriverDefault) => "",
        None => &target.component,
    }
}

pub fn to_update_request(
    target: &FirmwareTarget,
    force_update: bool,
    mut options: FirmwareUpdateOptions,
) -> nv::FirmwareUpdateRequest {
    if let Some(resolved) = &target.resolution {
        if let Some(parameters) = &resolved.update_parameters {
            options.special_json = Some(Value::Object(parameters.clone()));
            if parameters.contains_key("ApplyTime") {
                options.apply_time = None;
            }
        }
        if let Some(parameters) = &resolved.oem_parameters {
            options.oem_parameters = Some(Value::Object(parameters.clone()));
        }
    }

    let force_update = effective_force_update(target, force_update, &options);

    nv::FirmwareUpdateRequest {
        component: firmware_target_selector(target).to_owned(),
        firmware_file: target.firmware_file.clone(),
        force_update,
        options: nv::UpdateOptions {
            staged: nv::StagedMode::None,
            special_json: options.special_json,
            oem_parameters: options.oem_parameters,
            liteon_device_id: options.liteon_device_id,
            apply_time: options.apply_time,
        },
        cancellation: options.cancellation,
    }
}

/// Returns the force policy sent to nvfwupd after profile parameters override options.
pub fn effective_force_update(
    target: &FirmwareTarget,
    force_update: bool,
    options: &FirmwareUpdateOptions,
) -> bool {
    let profile_parameters = target
        .resolution
        .as_ref()
        .and_then(|resolution| resolution.update_parameters.as_ref());

    let force_setting = match profile_parameters {
        Some(parameters) => parameters.get("ForceUpdate"),
        None => options
            .special_json
            .as_ref()
            .and_then(|parameters| parameters.get("ForceUpdate")),
    };

    force_update || force_setting.and_then(Value::as_bool).unwrap_or(false)
}

/// Use embedded parameter defaults for legacy calls without a resolved profile.
///
/// Returns an error if the binary's embedded catalog has no matching target.
pub(super) fn default_update_options(
    node_type: NodeType,
    component: &str,
    mut options: FirmwareUpdateOptions,
) -> crate::utilities::error::Result<FirmwareUpdateOptions> {
    let target = default_update_target(node_type, component)?;
    if options.special_json.is_none() {
        options.special_json = target.update_parameters().cloned().map(Value::Object);
    }
    if options.oem_parameters.is_none() {
        options.oem_parameters = target.oem_parameters().cloned().map(Value::Object);
    }
    Ok(options)
}

/// Resolve a code-default target without imposing its component keys on custom profiles.
pub(super) fn default_update_target(
    node_type: NodeType,
    component: &str,
) -> crate::utilities::error::Result<&'static crate::config::hardware_profiles::FirmwareTargetPlan>
{
    let target = crate::config::hardware_profiles::built_in_profile(node_type)
        .and_then(|profile| {
            profile
                .stages()
                .iter()
                .flat_map(|stage| stage.targets())
                .find(|target| target.component_key() == component)
        })
        .ok_or_else(|| {
            RmsError::internal(format!(
                "embedded hardware profile for {} has no firmware target {component:?}",
                node_type.as_str()
            ))
        })?;
    Ok(target)
}

pub fn to_activation_request(request: FirmwareActivationRequest) -> nv::ActivationRequest {
    nv::ActivationRequest {
        mode: to_activation_mode(request.mode),
        cancellation: request.cancellation,
    }
}

pub fn to_version_check_targets(targets: &[FirmwareTarget]) -> Vec<nv::FirmwareVersionCheckTarget> {
    targets
        .iter()
        .flat_map(|target| {
            use crate::domain::firmware_target::FirmwareDestination;
            let components = match target
                .resolution
                .as_ref()
                .map(|resolved| &resolved.destination)
            {
                Some(FirmwareDestination::Multipart(paths)) if !paths.is_empty() => {
                    paths.iter().map(|path| path.as_str()).collect::<Vec<_>>()
                }
                _ => vec![firmware_target_selector(target)],
            };
            components
                .into_iter()
                .map(|component| nv::FirmwareVersionCheckTarget {
                    component: component.to_owned(),
                    firmware_file: target.firmware_file.clone(),
                })
        })
        .collect()
}

pub fn map_error(error: nv::NvFwUpdError) -> RmsError {
    match error {
        nv::NvFwUpdError::Message(message) => {
            RmsError::internal(format!("NVFWUPD error: {}", sanitize(&message)))
        }
        nv::NvFwUpdError::Transport { operation, message } => {
            map_transport_error(operation, &message)
        }
        nv::NvFwUpdError::BmcUnreachable { target, message } => RmsError::unavailable(format!(
            "NVFWUPD BMC unreachable for {}: {}",
            sanitize(&target),
            sanitize(&message)
        )),
        nv::NvFwUpdError::HostUnreachable { target, message } => RmsError::unavailable(format!(
            "NVFWUPD host unreachable for {}: {}",
            sanitize(&target),
            sanitize(&message)
        )),
        nv::NvFwUpdError::AuthFailed { target, message } => RmsError::invalid_argument(format!(
            "NVFWUPD authentication failed for {}: {}",
            sanitize(&target),
            sanitize(&message)
        )),
        nv::NvFwUpdError::HostToolUnavailable { tool, message } => {
            RmsError::failed_precondition(format!(
                "NVFWUPD required host tool {} is unavailable: {}",
                sanitize(&tool),
                sanitize(&message)
            ))
        }
        nv::NvFwUpdError::HostSudoUnavailable { message } => {
            RmsError::failed_precondition(format!(
                "NVFWUPD required host sudo access is unavailable: {}",
                sanitize(&message)
            ))
        }
        nv::NvFwUpdError::IncompatibleFirmware { component, message } => {
            RmsError::failed_precondition(format!(
                "NVFWUPD incompatible firmware selection for {}: {}",
                sanitize(&component),
                sanitize(&message)
            ))
        }
        nv::NvFwUpdError::Cancelled { operation } => {
            RmsError::cancelled(format!("NVFWUPD operation cancelled: {operation}"))
        }
        nv::NvFwUpdError::PackageParse { path, message } => RmsError::invalid_argument(format!(
            "NVFWUPD package parse failed for {}: {}",
            sanitize(&path),
            sanitize(&message)
        )),
        nv::NvFwUpdError::SubmissionFailed { details } => {
            let details = sanitize(&details.to_string());
            tracing::warn!(submission_diagnostic = %details, "NVFWUPD firmware submission failed");
            RmsError::internal(format!("NVFWUPD firmware submission failed: {details}"))
        }
        nv::NvFwUpdError::TaskFailed {
            task_id: Some(task_id),
            message,
        } => RmsError::internal(format!(
            "NVFWUPD task failed for {}: {}",
            sanitize(&task_id),
            sanitize(&message)
        )),
        nv::NvFwUpdError::TaskFailed {
            task_id: None,
            message,
        } => RmsError::internal(format!(
            "NVFWUPD update failed without a usable task reference: {}",
            sanitize(&message)
        )),
        nv::NvFwUpdError::Timeout { operation, seconds } => RmsError::timeout(format!(
            "NVFWUPD operation {operation} timed out after {seconds}s"
        )),
        nv::NvFwUpdError::InvalidResponse { context, message }
            if context.starts_with("expected firmware inventory") =>
        {
            RmsError::failed_precondition(format!(
                "NVFWUPD expected inventory mismatch: {}",
                sanitize(&message)
            ))
        }
        nv::NvFwUpdError::InvalidResponse { context, message } => RmsError::internal(format!(
            "NVFWUPD invalid response for {context}: {}",
            sanitize(&message)
        )),
        nv::NvFwUpdError::IpmiNotAvailable { message } => RmsError::failed_precondition(format!(
            "NVFWUPD IPMI unavailable: {}",
            sanitize(&message)
        )),
        nv::NvFwUpdError::VersionComparison { component, message } => RmsError::internal(format!(
            "NVFWUPD version comparison failed for {}: {}",
            sanitize(&component),
            sanitize(&message)
        )),
        nv::NvFwUpdError::Unsupported(operation) => RmsError::unimplemented(operation, "nvfwupd"),
    }
}

pub fn map_outcome(outcome: nv::FirmwareUpdateOutcome) -> FirmwareUpdateOutcome {
    match outcome {
        nv::FirmwareUpdateOutcome::Started(handle) => {
            FirmwareUpdateOutcome::Started(FirmwareTaskHandle {
                task_id: handle.task_id,
            })
        }
        nv::FirmwareUpdateOutcome::Completed(summary) => {
            FirmwareUpdateOutcome::Completed(FirmwareUpdateSummary {
                message: summary.message,
                task_ids: summary.task_ids,
                details: summary.details,
            })
        }
        nv::FirmwareUpdateOutcome::Skipped { reason } => FirmwareUpdateOutcome::Skipped { reason },
    }
}

pub fn map_activation_summary(summary: nv::ActivationSummary) -> FirmwareActivationSummary {
    FirmwareActivationSummary {
        message: summary.message,
        details: summary.details,
    }
}

pub fn map_version_check_summary(
    summary: nv::FirmwareVersionCheckSummary,
) -> FirmwareVersionCheckSummary {
    FirmwareVersionCheckSummary {
        matched: summary.matched,
        details: summary.details,
    }
}

pub fn to_firmware_info(component: nv::FirmwareComponent) -> FirmwareInfo {
    let nv::FirmwareComponent {
        name,
        version,
        device_class,
        inventory_path,
        details,
    } = component;

    let target = inventory_path
        .or_else(|| {
            details
                .get("@odata.id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_default();
    let type_hint = format!(
        "{} {} {}",
        name,
        device_class.as_deref().unwrap_or_default(),
        target
    );

    let status = details.get("Status");
    let health = status
        .and_then(|value| value.get("Health"))
        .and_then(Value::as_str)
        .unwrap_or("Unknown")
        .to_owned();
    let raw_state = status
        .and_then(|value| value.get("State"))
        .and_then(Value::as_str);
    // A Critical component with no State reads as not-in-recovery
    // downstream, because the recovery signature needs an exact
    // StandbyOffline. The recovery flow will not pick it up. Surface the
    // reading rather than hide it behind the "Unknown" fallback.
    if health == "Critical" && raw_state.is_none() {
        tracing::warn!(
            component = %name,
            "firmware reports Critical health with no Status.State; \
             recovery detection cannot classify it"
        );
    }

    FirmwareInfo {
        firmware_type: classify_firmware_type(&type_hint),
        name,
        version: version.unwrap_or_default(),
        updateable: details
            .get("Updateable")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        target,
        health,
        state: raw_state.unwrap_or("Unknown").to_owned(),
        sku: details
            .get("SKU")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    }
}

pub fn to_task_status(status: nv::TaskStatus) -> FirmwareTaskStatus {
    let state = task_state_str(status.state).to_owned();
    let task_status = status
        .details
        .get("TaskStatus")
        .or_else(|| status.details.get("status"))
        .and_then(Value::as_str)
        .unwrap_or(&state)
        .to_owned();

    let rejection = (status.state == nv::TaskState::Failed
        && task_status.eq_ignore_ascii_case("Critical")
        && identical_image_rejection_from_details(&status.details))
    .then_some(FirmwareTaskRejection::IdenticalImage);

    FirmwareTaskStatus {
        completed: is_terminal_task_state(status.state),
        percent: i32::from(status.progress_percent.unwrap_or(0)),
        state,
        status: task_status,
        message: status.message.unwrap_or_default(),
        rejection,
    }
}

fn identical_image_rejection_from_details(details: &Value) -> bool {
    let Some(messages) = details.get("Messages").and_then(Value::as_array) else {
        return false;
    };

    let mut found_rejection = false;

    for message in messages {
        let message_id = message.get("MessageId").and_then(Value::as_str);

        if message_id
            .is_some_and(|id| id.ends_with(".UpdateSuccessful") || id.ends_with(".AwaitToActivate"))
        {
            return false;
        }

        let severity = message
            .get("Severity")
            .or_else(|| message.get("MessageSeverity"))
            .and_then(Value::as_str)
            .unwrap_or_default();

        if severity.eq_ignore_ascii_case("OK") {
            continue;
        }

        if message_id.is_some_and(|id| id.starts_with("TaskEvent.")) {
            continue;
        }

        if message_id.is_none()
            && message
                .get("Message")
                .and_then(Value::as_str)
                .is_some_and(|text| {
                    text.starts_with("The task with Id '")
                        && text.ends_with("' has completed with errors.")
                })
        {
            continue;
        }

        let identical_image = message_id.is_some_and(|id| {
            id.starts_with("ResourceEvent.") && id.ends_with(".ResourceErrorsDetected")
        }) && message
            .get("MessageArgs")
            .and_then(Value::as_array)
            .and_then(|arguments| arguments.get(1))
            .and_then(Value::as_str)
            == Some("Component image is identical");

        if !identical_image {
            return false;
        }

        found_rejection = true;
    }

    found_rejection
}

/// Reports the identical-image rejection classified from the complete Redfish task details.
pub fn is_identical_image_rejection(status: &FirmwareTaskStatus) -> bool {
    status.rejection == Some(FirmwareTaskRejection::IdenticalImage)
}

pub(crate) fn to_server_type(server_profile: NvfwupdServerProfile) -> nv::ServerType {
    match server_profile {
        NvfwupdServerProfile::ComputeGb200 => nv::ServerType::GB200,
        NvfwupdServerProfile::ComputeGb300 => nv::ServerType::GB300,
        NvfwupdServerProfile::ComputeVrnvl72 => nv::ServerType::VRNVL72,
        NvfwupdServerProfile::Powershelf => nv::ServerType::PowerShelf,
        NvfwupdServerProfile::SwitchGb200 => nv::ServerType::GB200Switch,
        NvfwupdServerProfile::SwitchGb300 => nv::ServerType::GB300Switch,
        NvfwupdServerProfile::SwitchVrnvl72 => nv::ServerType::VRNVL72Switch,
    }
}

fn to_activation_mode(mode: FirmwareActivationMode) -> nv::ActivationMode {
    match mode {
        FirmwareActivationMode::SingleCommand(command) => {
            nv::ActivationMode::SingleCommand(to_activation_command(command))
        }
        FirmwareActivationMode::FullGb200Compute => nv::ActivationMode::FullGb200Compute,
        FirmwareActivationMode::SwitchPowerCycle => nv::ActivationMode::SwitchPowerCycle,
        FirmwareActivationMode::PowerShelfReset { force } => {
            nv::ActivationMode::PowerShelfReset { force }
        }
    }
}

fn to_activation_command(command: FirmwareActivationCommand) -> nv::ActivationCommand {
    match command {
        FirmwareActivationCommand::PowerOn => nv::ActivationCommand::PowerOn,
        FirmwareActivationCommand::PowerOff => nv::ActivationCommand::PowerOff,
        FirmwareActivationCommand::PowerCycle => nv::ActivationCommand::PowerCycle,
        FirmwareActivationCommand::AuxPowerCycle => nv::ActivationCommand::AuxPowerCycle,
        FirmwareActivationCommand::PowerStatus => nv::ActivationCommand::PowerStatus,
        FirmwareActivationCommand::PowerShelfReset => nv::ActivationCommand::PowerShelfReset,
        FirmwareActivationCommand::PowerShelfResetForce => {
            nv::ActivationCommand::PowerShelfResetForce
        }
    }
}

fn map_transport_error(operation: &'static str, message: &str) -> RmsError {
    let rendered = format!(
        "NVFWUPD transport error during {operation}: {}",
        sanitize(message)
    );
    let lower = message.to_ascii_lowercase();

    if lower.contains("connection refused") {
        RmsError::connection_refused(rendered)
    } else if lower.contains("dns")
        || lower.contains("name or service not known")
        || lower.contains("failed to lookup address")
    {
        RmsError::dns_resolution_failed(rendered)
    } else if lower.contains("timed out") || lower.contains("timeout") {
        RmsError::timeout(rendered)
    } else {
        RmsError::unavailable(rendered)
    }
}

fn classify_firmware_type(value: &str) -> FirmwareType {
    let lower = value.to_ascii_lowercase();
    if lower.contains("bmc") {
        FirmwareType::BMC
    } else if lower.contains("bios") || lower.contains("uefi") || lower.contains("fw_cpu_") {
        FirmwareType::BIOS
    } else if lower.contains("cpld") {
        FirmwareType::CPLD
    } else if lower.contains("fpga") || lower.contains("smr") {
        FirmwareType::FPGA
    } else if lower.contains("nic") {
        FirmwareType::NIC
    } else if lower.contains("hba") {
        FirmwareType::HBA
    } else if lower.contains("hmc") || lower.contains("hgx") {
        FirmwareType::HMC
    } else {
        FirmwareType::Unknown
    }
}

fn task_state_str(state: nv::TaskState) -> &'static str {
    match state {
        nv::TaskState::Unknown => "Unknown",
        nv::TaskState::Pending => "Pending",
        nv::TaskState::Running => "Running",
        nv::TaskState::Completed => "Completed",
        nv::TaskState::Failed => "Failed",
        nv::TaskState::Cancelled => "Cancelled",
    }
}

fn is_terminal_task_state(state: nv::TaskState) -> bool {
    matches!(
        state,
        nv::TaskState::Completed | nv::TaskState::Failed | nv::TaskState::Cancelled
    )
}

/// Shared IPv4/IPv6 log sanitizer. Its configuration is fixed
/// (`new(None, None, None)`), so compile the address-matching regexes exactly
/// once and reuse the instance: `sanitize` runs per output line of an
/// nvfwupd/SSH transcript during a firmware update, and rebuilding the regex on
/// every call would be needless work.
static LOG_SANITIZER: LazyLock<common::log_sanitize::LogSanitizer> =
    LazyLock::new(|| common::log_sanitize::LogSanitizer::new(None, None, None));

fn sanitize(value: &str) -> String {
    // Always-on secret redaction followed by the default IPv4/IPv6 address
    // sanitizer. This mirrors nvfwupd's `sanitize_log` in RMS (which never
    // configures nvfwupd's CLI-only global sanitize state), while depending on
    // the shared `utils` crate rather than nvfwupd for a pure logging concern.
    let redacted = common::redaction::redact_secret_fields(value);
    LOG_SANITIZER.sanitize(&redacted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn missing_embedded_target_returns_a_contextual_error() {
        for (node_type, component) in [
            (NodeType::ComputeGb300Lenovo, "MISSING"),
            (NodeType::ComputeGb300Supermicro, "MISSING"),
            (NodeType::SwitchGb200Nvidia, "HMC"),
        ] {
            let error =
                default_update_options(node_type, component, FirmwareUpdateOptions::default())
                    .expect_err("missing defaults must not panic");
            assert_eq!(error.code, ErrorCode::Internal);
            assert!(error.message.contains(node_type.as_str()));
            assert!(error.message.contains(component));
        }
    }

    #[test]
    fn embedded_defaults_preserve_explicit_multipart_objects() {
        let options = default_update_options(
            NodeType::ComputeGb300Lenovo,
            "BMC",
            FirmwareUpdateOptions {
                special_json: Some(json!({"ForceUpdate": false})),
                oem_parameters: Some(json!({})),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(options.special_json, Some(json!({"ForceUpdate": false})));
        assert_eq!(options.oem_parameters, Some(json!({})));
    }

    #[test]
    fn target_config_maps_node_type_and_secret_credentials() {
        let secret = SecretString::from("p@ssw0rd".to_owned());
        let cfg = to_target_config_with_secret(
            NodeType::SwitchGb200Nvidia,
            "10.0.0.8",
            8443,
            "admin",
            &secret,
            false,
        );

        assert_eq!(cfg.ip, "10.0.0.8");
        assert_eq!(cfg.username, "admin");
        assert_eq!(cfg.password, "p@ssw0rd");
        assert_eq!(cfg.port, Some(8443));
        assert_eq!(cfg.server_type, nv::ServerType::GB200Switch);
        assert!(!cfg.verify_tls);
        assert_eq!(cfg.ssh_known_hosts, None);
        assert_eq!(cfg.ssh_host_key_mode, nv::SshHostKeyMode::Disabled);
        assert!(!format!("{cfg:?}").contains("p@ssw0rd"));

        assert_eq!(
            to_target_config_with_secret(
                NodeType::ComputeGb200Nvidia,
                "10.0.0.9",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::GB200
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::ComputeGb200Wiwynn,
                "10.0.0.9",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::GB200
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::ComputeGb300Nvidia,
                "10.0.0.9",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::GB300
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::ComputeGb300Lenovo,
                "10.0.0.9",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::GB300
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::ComputeVrnvl72Nvidia,
                "10.0.0.9",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::VRNVL72
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::PowershelfGb200Liteon,
                "10.0.0.10",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::PowerShelf
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::PowershelfGb300Liteon,
                "10.0.0.10",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::PowerShelf
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::PowershelfGb200Delta,
                "10.0.0.10",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::PowerShelf
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::PowershelfGb300Delta,
                "10.0.0.10",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::PowerShelf
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::PowershelfVrnvl72Delta,
                "10.0.0.10",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::PowerShelf
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::PowershelfVrnvl72Liteon,
                "10.0.0.10",
                443,
                "u",
                &SecretString::from("p".to_owned()),
                true
            )
            .server_type,
            nv::ServerType::PowerShelf
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::SwitchGb300Nvidia,
                "10.0.0.8",
                8443,
                "admin",
                &secret,
                false
            )
            .server_type,
            nv::ServerType::GB300Switch
        );
        assert_eq!(
            to_target_config_with_secret(
                NodeType::SwitchVrnvl72Nvidia,
                "10.0.0.7",
                443,
                "admin",
                &secret,
                false
            )
            .server_type,
            nv::ServerType::VRNVL72Switch
        );
    }

    #[test]
    fn target_config_maps_bmc_session_token_without_exposing_it() {
        let credentials = EndpointCredentials::from_session_token("session-token");

        let config = to_target_config_with_credentials(
            NodeType::ComputeGb200Nvidia,
            "10.0.0.9",
            443,
            Some(&credentials),
            true,
        );

        assert!(config.username.is_empty());
        assert!(config.password.is_empty());
        assert_eq!(config.session_token.as_deref(), Some("session-token"));
        assert!(!format!("{config:?}").contains("session-token"));
    }

    #[test]
    fn target_config_maps_bmc_session_token_for_powershelf() {
        let credentials = EndpointCredentials::from_session_token("shelf-token");

        let config = to_target_config_with_credentials(
            NodeType::PowershelfGb200Delta,
            "10.0.0.11",
            443,
            Some(&credentials),
            true,
        );

        assert_eq!(config.server_type, nv::ServerType::PowerShelf);
        assert!(config.username.is_empty());
        assert!(config.password.is_empty());
        assert_eq!(config.session_token.as_deref(), Some("shelf-token"));
        assert!(!format!("{config:?}").contains("shelf-token"));
    }

    #[test]
    fn credentials_none_produces_empty_config_fields() {
        let config = to_target_config_with_credentials(
            NodeType::ComputeGb200Nvidia,
            "10.0.0.9",
            443,
            None,
            true,
        );

        assert!(
            config.username.is_empty(),
            "no credentials should produce empty username"
        );
        assert!(
            config.password.is_empty(),
            "no credentials should produce empty password"
        );
    }

    #[test]
    fn update_request_preserves_targets_force_options_and_cancellation() {
        let token = CancellationToken::new();
        let request = to_update_request(
            &FirmwareTarget {
                component: "BMC".to_owned(),
                firmware_file: "/tmp/fw.fwpkg".to_owned(),
                expected_version: None,
                resolution: None,
                linkx_policy: None,
            },
            true,
            FirmwareUpdateOptions {
                special_json: Some(json!({"Targets": ["BMC"]})),
                oem_parameters: Some(json!({"Oem": true})),
                liteon_device_id: Some("PSU-1".to_owned()),
                apply_time: Some("OnReset".to_owned()),
                cancellation: Some(token.clone()),
            },
        );

        assert_eq!(request.component, "BMC");
        assert_eq!(request.firmware_file, "/tmp/fw.fwpkg");
        assert!(request.force_update);
        assert_eq!(
            request.options.special_json,
            Some(json!({"Targets": ["BMC"]}))
        );
        assert_eq!(request.options.oem_parameters, Some(json!({"Oem": true})));
        assert_eq!(request.options.liteon_device_id.as_deref(), Some("PSU-1"));
        assert_eq!(request.options.apply_time.as_deref(), Some("OnReset"));
        assert_eq!(request.options.staged, nv::StagedMode::None);
        assert!(!request.cancellation.unwrap().is_cancelled());
        token.cancel();
    }

    #[test]
    fn activation_request_maps_all_modes() {
        let single = to_activation_request(FirmwareActivationRequest {
            mode: FirmwareActivationMode::SingleCommand(FirmwareActivationCommand::PowerStatus),
            cancellation: None,
        });
        assert!(matches!(
            single.mode,
            nv::ActivationMode::SingleCommand(nv::ActivationCommand::PowerStatus)
        ));

        let compute = to_activation_request(FirmwareActivationRequest {
            mode: FirmwareActivationMode::FullGb200Compute,
            cancellation: None,
        });
        assert_eq!(compute.mode, nv::ActivationMode::FullGb200Compute);

        let switch = to_activation_request(FirmwareActivationRequest {
            mode: FirmwareActivationMode::SwitchPowerCycle,
            cancellation: None,
        });
        assert_eq!(switch.mode, nv::ActivationMode::SwitchPowerCycle);

        let powershelf = to_activation_request(FirmwareActivationRequest {
            mode: FirmwareActivationMode::PowerShelfReset { force: true },
            cancellation: None,
        });
        assert_eq!(
            powershelf.mode,
            nv::ActivationMode::PowerShelfReset { force: true }
        );
    }

    #[test]
    fn error_mapping_uses_rms_codes_and_sanitizes_messages() {
        let err = map_error(nv::NvFwUpdError::Transport {
            operation: "GET",
            message: "connection refused".to_owned(),
        });
        assert_eq!(err.code, ErrorCode::ConnectionRefused);

        let err = map_error(nv::NvFwUpdError::Transport {
            operation: "GET",
            message: "dns lookup failed".to_owned(),
        });
        assert_eq!(err.code, ErrorCode::DnsResolutionFailed);

        let err = map_error(nv::NvFwUpdError::Timeout {
            operation: "monitor",
            seconds: 30,
        });
        assert_eq!(err.code, ErrorCode::Timeout);

        let err = map_error(nv::NvFwUpdError::PackageParse {
            path: "/tmp/fw.fwpkg".to_owned(),
            message: "password=secret failed".to_owned(),
        });
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(!err.message.contains("secret"));

        let err = map_error(nv::NvFwUpdError::TaskFailed {
            task_id: None,
            message: "password=secret connection refused".to_owned(),
        });
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("without a usable task reference"));
        assert!(!err.message.contains("secret"));

        let err = map_error(nv::NvFwUpdError::TaskFailed {
            task_id: Some("Task-1".to_owned()),
            message: "device rejected package".to_owned(),
        });
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("Task-1"));
        assert!(!err.message.contains("before task creation"));

        let err = map_error(nv::NvFwUpdError::Unsupported("full activation"));
        assert_eq!(err.code, ErrorCode::Unimplemented);
    }

    #[test]
    fn submission_error_preserves_bounded_diagnostics_in_adapter_error() {
        let err = map_error(nv::NvFwUpdError::SubmissionFailed {
            details: serde_json::json!({"category":"http_rejection", "http_status":409,
                "body":"busy ".repeat(700), "password":"secret-value"}),
        });
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("http_rejection"));
        assert!(err.message.contains("409"));
        assert!(err.message.len() > 3500);
        assert!(!err.message.contains("secret-value"));
    }

    #[test]
    fn expected_inventory_mismatch_maps_to_failed_precondition_with_details() {
        let err = map_error(nv::NvFwUpdError::InvalidResponse {
            context: "expected firmware inventory",
            message: "missing APs: [HGX_FW_GPU_1]; present APs: [FW_BMC_0]".to_owned(),
        });

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("HGX_FW_GPU_1"));
        assert!(err.message.contains("FW_BMC_0"));
    }

    #[test]
    fn outcome_mapping_preserves_started_completed_and_skipped() {
        let started = map_outcome(nv::FirmwareUpdateOutcome::Started(nv::TaskHandle {
            task_id: "Task-1".to_owned(),
        }));
        assert!(matches!(
            started,
            FirmwareUpdateOutcome::Started(FirmwareTaskHandle { task_id })
                if task_id == "Task-1"
        ));

        let completed = map_outcome(nv::FirmwareUpdateOutcome::Completed(nv::UpdateSummary {
            message: "done".to_owned(),
            task_ids: vec!["Task-2".to_owned()],
            details: json!({"ok": true}),
        }));
        assert!(matches!(
            completed,
            FirmwareUpdateOutcome::Completed(FirmwareUpdateSummary {
                message,
                task_ids,
                details,
            }) if message == "done" && task_ids == vec!["Task-2".to_owned()] && details["ok"] == true
        ));

        let skipped = map_outcome(nv::FirmwareUpdateOutcome::Skipped {
            reason: "already current".to_owned(),
        });
        assert!(matches!(
            skipped,
            FirmwareUpdateOutcome::Skipped { reason } if reason == "already current"
        ));
    }

    #[test]
    fn firmware_info_mapping_normalizes_details() {
        let info = to_firmware_info(nv::FirmwareComponent {
            name: "HGX_BMC".to_owned(),
            version: Some("1.2.3".to_owned()),
            device_class: Some("BMC".to_owned()),
            inventory_path: Some("/redfish/v1/UpdateService/FirmwareInventory/BMC".to_owned()),
            details: json!({
                "Updateable": false,
                "Status": {"Health": "OK", "State": "StandbyOffline"},
                "SKU": "sku-1"
            }),
        });

        assert_eq!(info.name, "HGX_BMC");
        assert_eq!(info.version, "1.2.3");
        assert_eq!(info.firmware_type, FirmwareType::BMC);
        assert!(!info.updateable);
        assert_eq!(info.health, "OK");
        assert_eq!(info.state, "StandbyOffline");
        assert_eq!(info.sku, "sku-1");
        assert_eq!(
            info.target,
            "/redfish/v1/UpdateService/FirmwareInventory/BMC"
        );

        let missing_updateable = to_firmware_info(nv::FirmwareComponent {
            name: "HGX_BMC".to_owned(),
            version: Some("1.2.3".to_owned()),
            device_class: Some("BMC".to_owned()),
            inventory_path: None,
            details: json!({}),
        });
        assert!(
            !missing_updateable.updateable,
            "missing Updateable should preserve RMS's conservative false default"
        );
        assert_eq!(
            missing_updateable.state, "Unknown",
            "missing Status.State should mirror the health fallback"
        );
    }

    #[test]
    fn redfish_cpu_firmware_is_classified_as_bios() {
        for name in ["MGX_FW_CPU_0", "FW_CPU_0"] {
            let info = to_firmware_info(nv::FirmwareComponent {
                name: name.to_owned(),
                version: Some("0ACTV_01.01.030".to_owned()),
                device_class: Some("0x00DA".to_owned()),
                inventory_path: Some(format!(
                    "/redfish/v1/UpdateService/FirmwareInventory/{name}"
                )),
                details: json!({}),
            });
            assert_eq!(info.firmware_type, FirmwareType::BIOS);
        }
    }

    #[test]
    fn task_status_mapping_preserves_state_progress_and_message() {
        let status = to_task_status(nv::TaskStatus {
            task_id: "Task-1".to_owned(),
            state: nv::TaskState::Completed,
            message: Some("firmware update complete".to_owned()),
            progress_percent: Some(100),
            details: json!({"TaskStatus": "OK"}),
        });

        assert!(status.completed);
        assert_eq!(status.percent, 100);
        assert_eq!(status.state, "Completed");
        assert_eq!(status.status, "OK");
        assert_eq!(status.message, "firmware update complete");
    }

    #[test]
    fn task_status_mapping_preserves_identical_image_response_for_rms() {
        let message = "The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'.";

        let details = json!({
            "Id": "firmware-task",
            "TaskState": "Exception",
            "TaskStatus": "Critical",
            "Messages": [
                {
                    "Message": "The task with Id 'firmware-task' has started.",
                    "MessageSeverity": "OK"
                },
                {
                    "MessageId": "TaskEvent.1.0.3.TaskAborted",
                    "MessageSeverity": "Critical",
                    "Message": "The task with Id 'firmware-task' was aborted."
                },
                {
                    "MessageSeverity": "Critical",
                    "Message": "The task with Id 'firmware-task' has completed with errors."
                },
                {
                    "MessageId": "ResourceEvent.1.0.3.ResourceErrorsDetected",
                    "Severity": "Critical",
                    "Message": message,
                    "MessageArgs": ["FW_COMPONENT_0", "Component image is identical"]
                }
            ]
        });

        let status = to_task_status(nv::TaskStatus {
            task_id: "firmware-task".to_owned(),
            state: nv::TaskState::Failed,
            message: Some(message.to_owned()),
            progress_percent: None,
            details,
        });

        assert!(status.completed);
        assert_eq!(status.state, "Failed");
        assert_eq!(status.message, message);

        assert_eq!(
            status.rejection,
            Some(FirmwareTaskRejection::IdenticalImage)
        );

        assert!(is_identical_image_rejection(&status));
    }

    #[test]
    fn task_status_mapping_rejects_mixed_or_unproven_identical_image_responses() {
        let rejection = json!({
            "MessageId": "ResourceEvent.1.0.ResourceErrorsDetected",
            "Severity": "Critical",
            "MessageArgs": ["FW_COMPONENT_0", "Component image is identical"]
        });

        for messages in [
            json!([]),
            json!([{
                "MessageId": "ResourceEvent.1.0.ResourceErrorsDetected",
                "Severity": "Critical",
                "MessageArgs": ["FW_COMPONENT_0", "Firmware image verification failed"]
            }]),
            json!([rejection, {
                "MessageId": "ResourceEvent.1.0.ResourceErrorsDetected",
                "Severity": "Critical",
                "MessageArgs": ["FW_COMPONENT_1", "Firmware image verification failed"]
            }]),
            json!([rejection, {
                "MessageId": "Update.1.0.UpdateSuccessful",
                "Severity": "OK",
                "Message": "Another component was flashed"
            }]),
            json!([rejection, {
                "MessageId": "Update.1.0.AwaitToActivate",
                "Severity": "OK",
                "Message": "Another component awaits activation"
            }]),
            json!([rejection, {
                "MessageId": "TaskEvent.1.0.3.TaskAborted",
                "Severity": "Critical"
            }, {
                "Severity": "Critical",
                "Message": "An unclassified component failure occurred"
            }]),
        ] {
            let status = to_task_status(nv::TaskStatus {
                task_id: "firmware-task".to_owned(),
                state: nv::TaskState::Failed,
                message: Some("The resource property 'FW_COMPONENT_0' has detected errors of type 'Component image is identical'.".to_owned()),
                progress_percent: None,
                details: json!({"TaskStatus": "Critical", "Messages": messages}),
            });

            assert_eq!(status.rejection, None);
            assert!(!is_identical_image_rejection(&status), "{status:?}");
        }
    }

    #[test]
    fn task_status_mapping_treats_failed_and_cancelled_as_terminal() {
        for (state, expected_state, expected_status) in [
            (nv::TaskState::Failed, "Failed", "Exception"),
            (nv::TaskState::Cancelled, "Cancelled", "Cancelled"),
        ] {
            let status = to_task_status(nv::TaskStatus {
                task_id: "Task-1".to_owned(),
                state,
                message: Some("terminal task state".to_owned()),
                progress_percent: Some(42),
                details: json!({"TaskStatus": expected_status}),
            });

            assert!(status.completed);
            assert_eq!(status.percent, 42);
            assert_eq!(status.state, expected_state);
            assert_eq!(status.status, expected_status);
            assert_eq!(status.message, "terminal task state");
        }

        let running = to_task_status(nv::TaskStatus {
            task_id: "Task-2".to_owned(),
            state: nv::TaskState::Running,
            message: Some("still running".to_owned()),
            progress_percent: Some(10),
            details: json!({}),
        });
        assert!(!running.completed);
        assert_eq!(running.status, "Running");
    }

    #[test]
    fn cancelled_nvfwupd_error_maps_to_cancelled_rms_error() {
        let error = map_error(nv::NvFwUpdError::Cancelled {
            operation: "Flint update",
        });

        assert_eq!(error.code, ErrorCode::Cancelled);
        assert_eq!(error.message, "NVFWUPD operation cancelled: Flint update");
    }
}
