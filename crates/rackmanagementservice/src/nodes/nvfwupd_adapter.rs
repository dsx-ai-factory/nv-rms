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

use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

use crate::domain::node::{
    FirmwareActivationCommand, FirmwareActivationMode, FirmwareActivationRequest,
    FirmwareActivationSummary, FirmwareInfo, FirmwareTarget, FirmwareTaskHandle,
    FirmwareTaskStatus, FirmwareType, FirmwareUpdateOptions, FirmwareUpdateOutcome,
    FirmwareUpdateSummary, FirmwareVersionCheckSummary, NodeType, NvfwupdServerProfile,
};
use crate::utilities::error::RmsError;

use nvfwupd::workflow as nv;

fn to_target_config(
    node_type: NodeType,
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    verify_tls: bool,
) -> nv::TargetConfig {
    nv::TargetConfig {
        ip: host.to_owned(),
        username: user.to_owned(),
        password: password.to_owned(),
        port: Some(port),
        server_type: to_server_type(node_type),
        verify_tls,
        ssh_known_hosts: None,
        // RMS does not manage an NVFWUPD known_hosts file yet, so preserve the
        // existing RMS SSH/SFTP behavior until RMS can provide explicit policy.
        ssh_host_key_mode: nv::SshHostKeyMode::Disabled,
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
        node_type,
        host,
        port,
        user,
        password.expose_secret(),
        verify_tls,
    )
}

pub fn to_update_request(
    target: &FirmwareTarget,
    force_update: bool,
    options: FirmwareUpdateOptions,
) -> nv::FirmwareUpdateRequest {
    nv::FirmwareUpdateRequest {
        component: target.component.clone(),
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

pub fn to_activation_request(request: FirmwareActivationRequest) -> nv::ActivationRequest {
    nv::ActivationRequest {
        mode: to_activation_mode(request.mode),
        cancellation: request.cancellation,
    }
}

pub fn to_version_check_targets(targets: &[FirmwareTarget]) -> Vec<nv::FirmwareVersionCheckTarget> {
    targets
        .iter()
        .map(|target| nv::FirmwareVersionCheckTarget {
            component: target.component.clone(),
            firmware_file: target.firmware_file.clone(),
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
        nv::NvFwUpdError::AuthFailed { target, message } => RmsError::invalid_argument(format!(
            "NVFWUPD authentication failed for {}: {}",
            sanitize(&target),
            sanitize(&message)
        )),
        nv::NvFwUpdError::PackageParse { path, message } => RmsError::invalid_argument(format!(
            "NVFWUPD package parse failed for {}: {}",
            sanitize(&path),
            sanitize(&message)
        )),
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
            "NVFWUPD update failed before task creation: {}",
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

    FirmwareInfo {
        firmware_type: classify_firmware_type(&type_hint),
        name,
        version: version.unwrap_or_default(),
        updateable: details
            .get("Updateable")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        target,
        health: details
            .get("Status")
            .and_then(|status| status.get("Health"))
            .and_then(Value::as_str)
            .unwrap_or("Unknown")
            .to_owned(),
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

    FirmwareTaskStatus {
        completed: is_terminal_task_state(status.state),
        percent: i32::from(status.progress_percent.unwrap_or(0)),
        state,
        status: task_status,
        message: status.message.unwrap_or_default(),
    }
}

fn to_server_type(node_type: NodeType) -> nv::ServerType {
    match node_type.nvfwupd_server_profile() {
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
        FirmwareActivationCommand::RfPowerOn => nv::ActivationCommand::RfPowerOn,
        FirmwareActivationCommand::RfPowerOff => nv::ActivationCommand::RfPowerOff,
        FirmwareActivationCommand::RfPowerCycle => nv::ActivationCommand::RfPowerCycle,
        FirmwareActivationCommand::RfAuxPowerCycle => nv::ActivationCommand::RfAuxPowerCycle,
        FirmwareActivationCommand::RfPowerStatus => nv::ActivationCommand::RfPowerStatus,
        FirmwareActivationCommand::RfPowerShelfReset => nv::ActivationCommand::RfPowerShelfReset,
        FirmwareActivationCommand::RfPowerShelfResetForce => {
            nv::ActivationCommand::RfPowerShelfResetForce
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
    } else if lower.contains("bios") || lower.contains("uefi") || lower.contains("sbios") {
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

fn sanitize(value: &str) -> String {
    nvfwupd::utils::Util::sanitize_log(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

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
    fn update_request_preserves_targets_force_options_and_cancellation() {
        let token = CancellationToken::new();
        let request = to_update_request(
            &FirmwareTarget {
                component: "BMC".to_owned(),
                firmware_file: "/tmp/fw.fwpkg".to_owned(),
                expected_version: None,
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
            mode: FirmwareActivationMode::SingleCommand(FirmwareActivationCommand::RfPowerStatus),
            cancellation: None,
        });
        assert!(matches!(
            single.mode,
            nv::ActivationMode::SingleCommand(nv::ActivationCommand::RfPowerStatus)
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
        assert!(err.message.contains("before task creation"));
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
                "Status": {"Health": "OK"},
                "SKU": "sku-1"
            }),
        });

        assert_eq!(info.name, "HGX_BMC");
        assert_eq!(info.version, "1.2.3");
        assert_eq!(info.firmware_type, FirmwareType::BMC);
        assert!(!info.updateable);
        assert_eq!(info.health, "OK");
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
}
