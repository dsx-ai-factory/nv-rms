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

use nvfwupd::workflow::{
    ActivationCommand, ActivationMode, ActivationRequest, FirmwareUpdateOutcome,
    FirmwareUpdateRequest, FirmwareVersionCheckRequest, FirmwareVersionCheckTarget, ServerType,
    StagedMode, TargetConfig, TaskHandle, UpdateOptions, UpdateSummary,
};
use serde_json::json;
use std::future::Future;
use std::time::Duration;

fn assert_type_is_public<T: ?Sized>() {
    let _ = std::any::type_name::<T>();
}

fn assert_rftarget_impl<T: nvfwupd::rf_target::RFTarget + Send + Sync>() {}

fn assert_send_future<F: Future + Send>(future: F) {
    drop(future);
}

#[test]
fn phase_4b1_workflow_module_is_the_canonical_public_import_path() {
    let target: nvfwupd::workflow::TargetConfig = nvfwupd::workflow::TargetConfig {
        ip: "192.0.2.20".to_string(),
        username: "admin".to_string(),
        password: "secret".to_string(),
        port: Some(443),
        server_type: nvfwupd::workflow::ServerType::GB200Switch,
        verify_tls: false,
        ssh_known_hosts: None,
        ssh_host_key_mode: nvfwupd::workflow::SshHostKeyMode::TrustOnFirstUse,
    };
    assert_eq!(
        std::any::type_name::<nvfwupd::workflow::TargetConfig>(),
        "nvfwupd::workflow::TargetConfig"
    );
    assert_eq!(
        target.server_type,
        nvfwupd::workflow::ServerType::GB200Switch
    );

    let request: nvfwupd::workflow::FirmwareUpdateRequest =
        nvfwupd::workflow::FirmwareUpdateRequest {
            component: "CPLD".to_string(),
            firmware_file: "/tmp/fw.fwpkg".to_string(),
            force_update: false,
            options: nvfwupd::workflow::UpdateOptions {
                staged: nvfwupd::workflow::StagedMode::None,
                special_json: None,
                oem_parameters: None,
                liteon_device_id: None,
                apply_time: None,
            },
            cancellation: None,
        };
    assert_eq!(request.component, "CPLD");

    let outcome: nvfwupd::workflow::FirmwareUpdateOutcome =
        nvfwupd::workflow::FirmwareUpdateOutcome::Skipped {
            reason: "already current".to_string(),
        };
    assert!(matches!(
        outcome,
        nvfwupd::workflow::FirmwareUpdateOutcome::Skipped { .. }
    ));

    let activation: nvfwupd::workflow::ActivationRequest = nvfwupd::workflow::ActivationRequest {
        mode: nvfwupd::workflow::ActivationMode::SwitchPowerCycle,
        cancellation: None,
    };
    assert!(matches!(
        activation.mode,
        nvfwupd::workflow::ActivationMode::SwitchPowerCycle
    ));

    let activation_summary: nvfwupd::workflow::ActivationSummary =
        nvfwupd::workflow::ActivationSummary {
            message: "activated".to_string(),
            details: json!({"ok": true}),
        };
    assert_eq!(activation_summary.message, "activated");

    let component: nvfwupd::workflow::FirmwareComponent = nvfwupd::workflow::FirmwareComponent {
        name: "BMC".to_string(),
        version: Some("1.0.0".to_string()),
        device_class: Some("Manager".to_string()),
        inventory_path: Some("/redfish/v1/UpdateService/FirmwareInventory/BMC".to_string()),
        details: json!({"component": "BMC"}),
    };
    assert_eq!(component.name, "BMC");

    let task_status: nvfwupd::workflow::TaskStatus = nvfwupd::workflow::TaskStatus {
        task_id: "Task-1".to_string(),
        state: nvfwupd::workflow::TaskState::Completed,
        message: Some("done".to_string()),
        progress_percent: Some(100),
        details: json!({"state": "Completed"}),
    };
    assert_eq!(task_status.state, nvfwupd::workflow::TaskState::Completed);

    let reset_params: nvfwupd::workflow::ResetParams = nvfwupd::workflow::ResetParams {
        details: json!({"ResetType": "ForceRestart"}),
    };
    let reset_report: nvfwupd::workflow::ResetReport = nvfwupd::workflow::ResetReport {
        message: "reset".to_string(),
        details: reset_params.details.clone(),
    };
    assert_eq!(reset_report.message, "reset");

    let bg_copy_params: nvfwupd::workflow::BgCopyParams = nvfwupd::workflow::BgCopyParams {
        details: json!({"copy": true}),
    };
    let bg_copy_report: nvfwupd::workflow::BgCopyReport = nvfwupd::workflow::BgCopyReport {
        message: "copied".to_string(),
        details: bg_copy_params.details.clone(),
    };
    assert_eq!(bg_copy_report.message, "copied");

    let workflow_error: nvfwupd::workflow::NvFwUpdError =
        nvfwupd::workflow::NvFwUpdError::Unsupported("inventory");
    assert!(matches!(
        workflow_error,
        nvfwupd::workflow::NvFwUpdError::Unsupported("inventory")
    ));
    let typed_errors = [
        nvfwupd::workflow::NvFwUpdError::Message("legacy".to_string()),
        nvfwupd::workflow::NvFwUpdError::Transport {
            operation: "GET",
            message: "connection reset".to_string(),
        },
        nvfwupd::workflow::NvFwUpdError::BmcUnreachable {
            target: "192.0.2.10".to_string(),
            message: "timed out".to_string(),
        },
        nvfwupd::workflow::NvFwUpdError::AuthFailed {
            target: "192.0.2.10".to_string(),
            message: "401".to_string(),
        },
        nvfwupd::workflow::NvFwUpdError::PackageParse {
            path: "/tmp/fw.fwpkg".to_string(),
            message: "bad manifest".to_string(),
        },
        nvfwupd::workflow::NvFwUpdError::TaskFailed {
            task_id: Some("Task-1".to_string()),
            message: "failed".to_string(),
        },
        nvfwupd::workflow::NvFwUpdError::Timeout {
            operation: "monitor",
            seconds: 120,
        },
        nvfwupd::workflow::NvFwUpdError::InvalidResponse {
            context: "task",
            message: "missing state".to_string(),
        },
        nvfwupd::workflow::NvFwUpdError::IpmiNotAvailable {
            message: "ipmitool missing".to_string(),
        },
        nvfwupd::workflow::NvFwUpdError::VersionComparison {
            component: "BMC".to_string(),
            message: "invalid version".to_string(),
        },
        nvfwupd::workflow::NvFwUpdError::Unsupported("inventory"),
    ];
    assert_eq!(typed_errors.len(), 11);

    let workflow_result: nvfwupd::workflow::Result<()> = Ok(());
    assert!(workflow_result.is_ok());

    let server_variants = [
        nvfwupd::workflow::ServerType::DGX,
        nvfwupd::workflow::ServerType::DGXRubin,
        nvfwupd::workflow::ServerType::HGX,
        nvfwupd::workflow::ServerType::MGX,
        nvfwupd::workflow::ServerType::GH200,
        nvfwupd::workflow::ServerType::HGXB100,
        nvfwupd::workflow::ServerType::HGXB300,
        nvfwupd::workflow::ServerType::HGXRubin,
        nvfwupd::workflow::ServerType::GB200,
        nvfwupd::workflow::ServerType::GB300,
        nvfwupd::workflow::ServerType::VRNVL72,
        nvfwupd::workflow::ServerType::GB200Switch,
        nvfwupd::workflow::ServerType::GB300Switch,
        nvfwupd::workflow::ServerType::VRNVL72Switch,
        nvfwupd::workflow::ServerType::PowerShelf,
    ];
    assert_eq!(server_variants.len(), 15);

    let staged_variants = [
        nvfwupd::workflow::StagedMode::None,
        nvfwupd::workflow::StagedMode::StageOnly,
        nvfwupd::workflow::StagedMode::StageAndActivate,
    ];
    assert_eq!(staged_variants.len(), 3);

    let task_states = [
        nvfwupd::workflow::TaskState::Unknown,
        nvfwupd::workflow::TaskState::Pending,
        nvfwupd::workflow::TaskState::Running,
        nvfwupd::workflow::TaskState::Completed,
        nvfwupd::workflow::TaskState::Failed,
        nvfwupd::workflow::TaskState::Cancelled,
    ];
    assert_eq!(task_states.len(), 6);

    let activation_modes = [
        nvfwupd::workflow::ActivationMode::SingleCommand(
            nvfwupd::workflow::ActivationCommand::RfPowerCycle,
        ),
        nvfwupd::workflow::ActivationMode::FullGb200Compute,
        nvfwupd::workflow::ActivationMode::SwitchPowerCycle,
        nvfwupd::workflow::ActivationMode::PowerShelfReset { force: true },
    ];
    assert_eq!(activation_modes.len(), 4);

    let activation_commands = [
        nvfwupd::workflow::ActivationCommand::RfPowerOn,
        nvfwupd::workflow::ActivationCommand::RfPowerOff,
        nvfwupd::workflow::ActivationCommand::RfPowerCycle,
        nvfwupd::workflow::ActivationCommand::RfAuxPowerCycle,
        nvfwupd::workflow::ActivationCommand::RfPowerStatus,
        nvfwupd::workflow::ActivationCommand::RfPowerShelfReset,
        nvfwupd::workflow::ActivationCommand::RfPowerShelfResetForce,
    ];
    assert_eq!(activation_commands.len(), 7);
}

#[test]
fn phase_4b2_workflow_api_facade_functions_are_public_async_entrypoints() {
    let target = nvfwupd::workflow::TargetConfig {
        ip: "192.0.2.21".to_string(),
        username: "admin".to_string(),
        password: "secret".to_string(),
        port: Some(443),
        server_type: nvfwupd::workflow::ServerType::GB200,
        verify_tls: false,
        ssh_known_hosts: None,
        ssh_host_key_mode: nvfwupd::workflow::SshHostKeyMode::TrustOnFirstUse,
    };

    assert_send_future(nvfwupd::workflow_api::get_firmware_inventory(
        target.clone(),
    ));
    assert_send_future(nvfwupd::workflow_api::get_task_status(
        target.clone(),
        "Task-1",
    ));
    assert_send_future(nvfwupd::workflow_api::verify_firmware_versions(
        target.clone(),
        FirmwareVersionCheckRequest {
            firmware_files: vec!["/tmp/full.fwpkg".to_owned()],
        },
    ));
    assert_send_future(
        nvfwupd::workflow_api::verify_firmware_versions_with_expected_inventory(
            target.clone(),
            FirmwareVersionCheckRequest {
                firmware_files: vec!["/tmp/full.fwpkg".to_owned()],
            },
            Some(vec!["FW_BMC_0".to_owned()]),
        ),
    );
    assert_send_future(
        nvfwupd::workflow_api::verify_firmware_versions_after_activation_with_expected_inventory(
            target.clone(),
            FirmwareVersionCheckRequest {
                firmware_files: vec!["/tmp/full.fwpkg".to_owned()],
            },
            Some(vec!["FW_BMC_0".to_owned()]),
        ),
    );
    assert_send_future(nvfwupd::workflow_api::verify_firmware_target_versions(
        target.clone(),
        vec![FirmwareVersionCheckTarget {
            component: "BMC".to_owned(),
            firmware_file: "/tmp/bmc.fwpkg".to_owned(),
        }],
    ));
    assert_send_future(
        nvfwupd::workflow_api::verify_firmware_target_versions_with_expected_inventory(
            target.clone(),
            vec![FirmwareVersionCheckTarget {
                component: "BMC".to_owned(),
                firmware_file: "/tmp/bmc.fwpkg".to_owned(),
            }],
            Some(vec!["FW_BMC_0".to_owned()]),
        ),
    );
    assert_send_future(
        nvfwupd::workflow_api::verify_firmware_target_versions_after_activation_with_expected_inventory(
            target.clone(),
            vec![FirmwareVersionCheckTarget {
                component: "BMC".to_owned(),
                firmware_file: "/tmp/bmc.fwpkg".to_owned(),
            }],
            Some(vec!["FW_BMC_0".to_owned()]),
        ),
    );
    assert_send_future(nvfwupd::workflow_api::update_firmware(
        target.clone(),
        nvfwupd::workflow::FirmwareUpdateRequest {
            component: "BMC".to_string(),
            firmware_file: "/tmp/fw.fwpkg".to_string(),
            force_update: false,
            options: nvfwupd::workflow::UpdateOptions::default(),
            cancellation: None,
        },
    ));
    assert_send_future(
        nvfwupd::workflow_api::update_firmware_with_expected_inventory(
            target.clone(),
            nvfwupd::workflow::FirmwareUpdateRequest {
                component: "BMC".to_string(),
                firmware_file: "/tmp/fw.fwpkg".to_string(),
                force_update: false,
                options: nvfwupd::workflow::UpdateOptions::default(),
                cancellation: None,
            },
            Some(vec!["FW_BMC_0".to_owned()]),
        ),
    );
    assert_send_future(nvfwupd::workflow_api::activate_firmware(
        target.clone(),
        nvfwupd::workflow::ActivationRequest {
            mode: nvfwupd::workflow::ActivationMode::SingleCommand(
                nvfwupd::workflow::ActivationCommand::RfPowerStatus,
            ),
            cancellation: None,
        },
    ));

    let _constructor: fn(
        std::sync::Arc<nvue_client::Client>,
    ) -> nvfwupd::workflow_api::WorkflowContext =
        nvfwupd::workflow_api::WorkflowContext::with_nvue_client;

    let context = nvfwupd::workflow_api::WorkflowContext::new();
    let mut switch_target = target;

    switch_target.server_type = ServerType::GB200Switch;

    assert_send_future(context.get_firmware_inventory(switch_target));
}

#[test]
fn public_workflow_types_are_constructible() {
    let target = TargetConfig {
        ip: "192.0.2.10".to_string(),
        username: "admin".to_string(),
        password: "secret".to_string(),
        port: Some(443),
        server_type: ServerType::GB200,
        verify_tls: false,
        ssh_known_hosts: None,
        ssh_host_key_mode: nvfwupd::workflow::SshHostKeyMode::TrustOnFirstUse,
    };

    let debug = format!("{target:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("secret"));

    let update = FirmwareUpdateRequest {
        component: "GPU_SXM_1".to_string(),
        firmware_file: "/tmp/fw.fwpkg".to_string(),
        force_update: true,
        options: UpdateOptions {
            staged: StagedMode::StageAndActivate,
            special_json: Some(json!({"Targets": ["/redfish/v1/UpdateService"]})),
            oem_parameters: Some(json!({"ApplyTime": "Immediate"})),
            liteon_device_id: Some("1".to_string()),
            apply_time: Some("Immediate".to_string()),
        },
        cancellation: None,
    };

    assert_eq!(update.component, "GPU_SXM_1");

    let started = FirmwareUpdateOutcome::Started(TaskHandle {
        task_id: "Task-1".to_string(),
    });
    assert!(matches!(started, FirmwareUpdateOutcome::Started(_)));

    let completed = FirmwareUpdateOutcome::Completed(UpdateSummary {
        message: "done".to_string(),
        task_ids: vec!["Task-1".to_string()],
        details: json!({"ok": true}),
    });
    assert!(matches!(completed, FirmwareUpdateOutcome::Completed(_)));

    let activation = ActivationRequest {
        mode: ActivationMode::SingleCommand(ActivationCommand::RfPowerCycle),
        cancellation: None,
    };
    assert_eq!(
        ActivationCommand::RfPowerCycle.as_cli_command(),
        "RF_PWR_CYCLE"
    );
    assert!(matches!(
        activation.mode,
        ActivationMode::SingleCommand(ActivationCommand::RfPowerCycle)
    ));
}

#[test]
fn public_modules_and_targets_are_addressable() {
    fn construct_switch_target(
        bmc_access: nvfwupd::bmc_access::BmcAccess,
        config_dict: Option<serde_json::Value>,
    ) -> nvfwupd::gb200_switch_rftarget::GB200SwitchRFTarget {
        nvfwupd::gb200_switch_rftarget::GB200SwitchRFTarget {
            bmc_access,
            fungible_components: Vec::new(),
            update_completion_msg: String::new(),
            progress_table_header_printed: false,
            config_dict,
        }
    }

    assert_type_is_public::<nvfwupd::bmc_access::BmcAccess>();
    assert_type_is_public::<nvfwupd::bmc_access::AccessType>();
    assert_type_is_public::<nvfwupd::cli_format::NvfwupdCliFormat>();
    assert_type_is_public::<nvfwupd::cli_schema::CLISchema>();
    assert_type_is_public::<nvfwupd::cli_schema::CommandSchema>();
    assert_type_is_public::<nvfwupd::config_parser::ConfigParser>();
    assert_type_is_public::<nvfwupd::config_rftarget::ConfigRFTarget>();
    assert_type_is_public::<nvfwupd::dgx_rftarget::DGXRFTarget>();
    assert_type_is_public::<nvfwupd::gb200_rftarget::GB200RFTarget>();
    assert_type_is_public::<nvfwupd::gb200_switch_rftarget::GB200SwitchRFTarget>();
    let _: fn(
        nvfwupd::bmc_access::BmcAccess,
        Option<serde_json::Value>,
    ) -> nvfwupd::gb200_switch_rftarget::GB200SwitchRFTarget =
        nvfwupd::gb200_switch_rftarget::GB200SwitchRFTarget::new;
    let _: fn(
        nvfwupd::bmc_access::BmcAccess,
        Option<serde_json::Value>,
    ) -> nvfwupd::gb200_switch_rftarget::GB200SwitchRFTarget = construct_switch_target;
    assert_type_is_public::<nvfwupd::gh_rftarget::GHRFTarget>();
    assert_type_is_public::<nvfwupd::gh200_rftarget::GH200RFTarget>();
    assert_type_is_public::<nvfwupd::hgxb100_rftarget::HGXB100RFTarget>();
    assert_type_is_public::<nvfwupd::hgxb100_rftarget::HGXRUBINRFTarget>();
    assert_type_is_public::<nvfwupd::input_params::InputParams>();
    assert_type_is_public::<nvfwupd::input_params::TaskId>();
    assert_type_is_public::<nvfwupd::input_params::WorkerResult>();
    assert_type_is_public::<nvfwupd::ipmitool_api::IpmiToolActivation>();
    assert_type_is_public::<nvfwupd::os_access::OsAccess>();
    assert_type_is_public::<dyn nvfwupd::pldm::FirmwarePkg>();
    assert_type_is_public::<nvfwupd::pldm::PLDM>();
    assert_type_is_public::<nvfwupd::pldm::TarPkg>();
    assert_type_is_public::<nvfwupd::powershelf_rftarget::PowerShelfRFTarget>();
    assert_type_is_public::<nvfwupd::rf_target::CmdArgs>();
    assert_type_is_public::<dyn nvfwupd::rf_target::PkgParser>();
    assert_type_is_public::<dyn nvfwupd::rf_target::RFTarget>();
    assert_type_is_public::<nvfwupd::updcommand::FwUpdCmdBase<'static>>();
    assert_type_is_public::<nvfwupd::updcommand::FwUpdCmdHelp<'static>>();
    assert_type_is_public::<nvfwupd::updcommand::ParsedArgs>();
    assert_type_is_public::<nvfwupd::util::BailAction>();
    assert_type_is_public::<nvfwupd::util::TraceFlags>();
    assert_type_is_public::<nvfwupd::util::Util>();
    assert_type_is_public::<nvfwupd::utils::BuiltInLogSanitizers>();
    assert_type_is_public::<nvfwupd::utils::LogSanitizer>();
    assert_type_is_public::<nvfwupd::utils::Util>();
    assert_type_is_public::<nvfwupd::workflow::TargetConfig>();

    assert_type_is_public::<nvfwupd::targets::ConfigRFTarget>();
    assert_type_is_public::<nvfwupd::targets::DGXRFTarget>();
    assert_type_is_public::<nvfwupd::targets::GB200RFTarget>();
    assert_type_is_public::<nvfwupd::targets::GB200SwitchRFTarget>();
    assert_type_is_public::<nvfwupd::targets::GH200RFTarget>();
    assert_type_is_public::<nvfwupd::targets::GHRFTarget>();
    assert_type_is_public::<nvfwupd::targets::HGXB100RFTarget>();
    assert_type_is_public::<nvfwupd::targets::HGXRUBINRFTarget>();
    assert_type_is_public::<nvfwupd::targets::PowerShelfRFTarget>();

    assert_rftarget_impl::<nvfwupd::config_rftarget::ConfigRFTarget>();
    assert_rftarget_impl::<nvfwupd::targets::ConfigRFTarget>();

    assert!(!nvfwupd::version::NVFWUPD_CLI_VERSION.is_empty());
}

#[tokio::test]
async fn phase_2a_async_stack_is_available() {
    assert_type_is_public::<russh::client::Config>();
    assert_type_is_public::<russh::keys::PrivateKey>();
    assert_type_is_public::<russh_sftp::client::SftpSession>();
    assert_type_is_public::<wiremock::MockServer>();

    let cancellation = tokio_util::sync::CancellationToken::new();
    assert!(!cancellation.is_cancelled());

    tokio::time::timeout(Duration::from_millis(10), async {})
        .await
        .expect("empty async block should complete immediately");
}

#[test]
fn phase_2c_rftarget_async_contract_is_send_sync() {
    use nvfwupd::bmc_access::{AccessType, BmcAccess};
    use nvfwupd::gh_rftarget::GHRFTarget;
    use nvfwupd::rf_target::{CmdArgs, PkgParser, RFTarget, UpdatePreconditionMode};

    struct NoopPkgParser;

    #[async_trait::async_trait]
    impl PkgParser for NoopPkgParser {
        async fn parse_pkg(&mut self, _pkg_path: &str) -> (bool, String) {
            (true, String::new())
        }
    }

    let bmc_access = BmcAccess {
        ip: "127.0.0.1".to_string(),
        user: "test".to_string(),
        password: "test".to_string(),
        model: String::new(),
        partnumber: String::new(),
        serialnumber: String::new(),
        port: String::new(),
        servertype: String::new(),
        base_url: "https://127.0.0.1".to_string(),
        transport_type: "https".to_string(),
        access_type: AccessType::Login,
        ssh_known_hosts: None,
        ssh_host_key_mode: "tofu".to_string(),
        client: reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("test client should build"),
    };

    let mut target: Box<dyn RFTarget + Send + Sync> = Box::new(GHRFTarget::new(bmc_access, None));
    let cmd_args = CmdArgs {
        cmd: String::new(),
        background: false,
        details: false,
        staged_update: false,
        staged_activate_update: false,
        quiet: false,
        special: None,
        oem_parameters: None,
    };
    let recipes = vec!["/tmp/fw.fwpkg".to_string()];
    let mut pkg_parser = NoopPkgParser;

    assert_send_future(target.update_component(
        &cmd_args,
        "/redfish/v1/UpdateService",
        "/tmp/fw",
        1,
        None,
        false,
    ));
    assert_send_future(target.get_identifier_from_chassis("/redfish/v1/Chassis/1"));
    assert_send_future(target.get_component_version(&serde_json::json!({}), "AP", None));
    assert_send_future(target.dispatch_request_with_retry("GET", "/redfish/v1", None, None, 1, 0));
    assert_send_future(target.factory_reset(None));
    assert_send_future(target.background_copy("{}"));
    assert_send_future(target.query_job_status("Task-1", None));
    assert_send_future(target.process_job_status("Task-1", None));
    assert_send_future(target.make_update_target_json("/tmp"));
    assert_send_future(target.update_component_pushuri(
        None,
        "/redfish/v1/UpdateService",
        "/tmp/fw",
        1,
        None,
        false,
    ));
    assert_send_future(target.update_component_multipart(
        None,
        "/redfish/v1/UpdateService",
        "/tmp/fw",
        1,
        None,
        None,
        None,
        None,
        false,
        false,
    ));
    assert_send_future(target.start_update_monitor(
        &recipes,
        &mut pkg_parser,
        &cmd_args,
        1,
        false,
        None,
        0,
        true,
        Some(UpdatePreconditionMode::SingleShot),
    ));
    assert_send_future(target.run_oob_activation(&cmd_args));
}
