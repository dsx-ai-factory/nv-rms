// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use noyalib::compat::serde_yaml;

use super::*;

#[tokio::test]
async fn recovery_failure_preserves_profile_provenance_before_any_flash() {
    let tracker = Arc::new(JobTracker::new());
    let pending = tracker
        .create_job("rack", "recovery-node", JobType::FirmwareUpdate)
        .unwrap();
    let job_id = pending.id().to_string();
    let node = Arc::new(RecoveryDetectMockNode::with_inventory(vec![watched_entry(
        "Critical",
        "StandbyOffline",
    )]));
    let profile = Arc::new(HardwareUpdatePlan::from_yaml_str(GB200).unwrap());
    let mut recovery = recovery_disarmed();
    recovery.source = RecoverySource::Absent;

    spawn_firmware_update_job_with_policy(
        tracker.clone(),
        pending,
        node.clone(),
        vec![firmware_target()],
        false,
        false,
        FirmwareTaskPollOptions::default(),
        true,
        recovery,
        Some(profile.clone()),
        None,
    );

    let job = wait_for_firmware_job_terminal(&tracker, &job_id).await;
    assert_eq!(
        job.state,
        crate::orchestrator::job_lifecycle::JobState::Failed
    );
    assert!(
        job.error_message
            .contains("manifest carries no HMC recovery package")
    );
    assert_eq!(node.update_calls.load(Ordering::SeqCst), 0);
    assert!(node.recovery_ops.lock().unwrap().is_empty());
    let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
    assert_eq!(result["hardware_profile"], profile.profile_id());
    assert_eq!(result["hardware_profile_sha256"], profile.source_sha256());
}

#[test]
fn profile_aware_node_construction_preserves_rms_bmc_tls_policy() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("profile.yaml"),
        GB200.replace(
            "nvfwupd_server_profile: compute_gb200",
            "nvfwupd_server_profile: compute_gb300",
        ),
    )
    .unwrap();
    let catalog = HardwareProfileCatalog::load_directory(directory.path()).unwrap();
    let node_info = rm::NodeInfo {
        node_id: "compute-1".to_owned(),
        rack_id: "rack-1".to_owned(),
        r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
        bmc_endpoint: Some(rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: "192.0.2.10".to_owned(),
                mac_address: "00:11:22:33:44:55".to_owned(),
                ..Default::default()
            }),
            port: 443,
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "admin".to_owned(),
                    password: "test-password".to_owned(),
                })),
            }),
        }),
        ..Default::default()
    };
    for insecure_bmc in [false, true] {
        let firmware = build_ephemeral_node_with_profiles(
            &node_info,
            NodeType::ComputeGb200Nvidia,
            None,
            None,
            &catalog,
            insecure_bmc,
        )
        .unwrap();
        let power = crate::api::grpc::power_handlers::build_ephemeral_power_node(
            &node_info,
            &catalog,
            insecure_bmc,
        )
        .unwrap();
        let inventory = build_ephemeral_inventory_node(
            &node_info,
            NodeType::ComputeGb200Nvidia,
            &catalog,
            insecure_bmc,
        )
        .unwrap();
        for node in [firmware, power, inventory] {
            let NodeInstance::ComputeGb200Nvidia(compute) = node.as_ref() else {
                panic!("expected the configured compute implementation");
            };
            let config = compute.nvfwupd_target_config();
            assert_eq!(config.verify_tls, !insecure_bmc);
            assert_eq!(config.server_type, nvfwupd::workflow::ServerType::GB300);
        }
    }
}

// Compare transport behavior with code defaults without discarding the identity
// assertions on the actual resolved plan.
fn transport_stages(stages: &[ExecutableFirmwareStage]) -> Vec<FirmwareUpdateStage> {
    stages
        .iter()
        .map(|stage| FirmwareUpdateStage {
            targets: stage
                .targets
                .iter()
                .map(|target| FirmwareTarget {
                    component: crate::nodes::nvfwupd_adapter::firmware_target_selector(target)
                        .to_owned(),
                    resolution: None,
                    ..target.clone()
                })
                .collect(),
            activate_after: stage.activation.is_some(),
        })
        .collect()
}

#[test]
fn repeated_components_for_the_same_package_and_destination_are_rejected() {
    let profile = HardwareUpdatePlan::from_yaml_str(GB200).unwrap();
    for (left, right) in [("HMC", "HMC"), ("HMC", "hmc"), ("BMC", "BMC")] {
        let error = firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb200Nvidia,
            &[
                inband_target(left, "/tmp/package.bin"),
                inband_target(right, "/tmp/package.bin"),
            ],
            true,
            false,
            Some(&profile),
        )
        .unwrap_err();
        assert!(
            error.message.contains("duplicate firmware upload"),
            "{left}/{right}: {}",
            error.message
        );
    }
}

#[test]
fn profile_requests_require_configured_component_keys() {
    let profile = HardwareUpdatePlan::from_yaml_str(
        &GB200
            .replacen("    - id: hmc", "    - id: chassis_update", 1)
            .replacen("component_key: HMC", "component_key: CHASSIS", 1),
    )
    .unwrap();
    for component in ["CHASSIS", "chassis"] {
        let stages = firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb200Nvidia,
            &[inband_target(component, "/tmp/package.bin")],
            true,
            false,
            Some(&profile),
        )
        .unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].targets.len(), 1);
        assert_eq!(stages[0].targets[0].component, "CHASSIS");
    }
    for component in [
        "UNCONFIGURED_COMPONENT",
        "HMC",
        "chassis_update",
        "/redfish/v1/Chassis/HGX_Chassis_0",
        "",
    ] {
        let error = firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb200Nvidia,
            &[
                inband_target("CHASSIS", "/tmp/selected.bin"),
                inband_target(component, "/tmp/package.bin"),
            ],
            true,
            false,
            Some(&profile),
        )
        .unwrap_err();
        assert!(
            error.message.contains("not allowed by hardware profile"),
            "{component}: {error}"
        );
    }
}

struct MandatoryVerificationNode {
    events: Mutex<Vec<&'static str>>,
    inventory_failure: bool,
}

#[async_trait]
impl Node for MandatoryVerificationNode {
    fn id(&self) -> &str {
        "nic-test"
    }
    fn rack_id(&self) -> &str {
        "rack"
    }
    fn node_type(&self) -> NodeType {
        NodeType::ComputeGb200Nvidia
    }
    fn get_info(&self) -> HashMap<String, String> {
        HashMap::new()
    }
    async fn update_firmware(
        &self,
        _: &FirmwareTarget,
        _: bool,
        _: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        self.events.lock().unwrap().push("oob");
        Ok(completed_update_outcome())
    }
    async fn update_firmware_group(
        &self,
        _: &[FirmwareTarget],
        _: bool,
        _: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        self.events.lock().unwrap().push("nic");
        Ok(completed_update_outcome())
    }
    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        assert_eq!(request.mode, FirmwareActivationMode::FullGb200Compute);
        self.events.lock().unwrap().push("activate");
        Ok(FirmwareActivationSummary {
            message: "activated".into(),
            details: serde_json::json!({}),
        })
    }
    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        assert!(
            targets
                .iter()
                .all(|target| flint_family_for_target(target).is_some())
        );
        self.events.lock().unwrap().push("verify");
        if self.inventory_failure {
            Err(RmsError::failed_precondition(
                "NIC device count mismatch: expected 4, found 3",
            ))
        } else {
            Ok(FirmwareVersionCheckSummary {
                matched: false,
                details: serde_json::json!({"status":"mismatch"}),
            })
        }
    }
}

#[tokio::test]
async fn disabled_oob_verification_cannot_skip_mandatory_nic_checks() {
    for mixed in [false, true] {
        for inventory_failure in [false, true] {
            let profile = HardwareUpdatePlan::from_yaml_str(&GB200.replace(
                "verification:\n    enabled: true",
                "verification:\n    enabled: false",
            ))
            .unwrap();
            let mut targets = vec![inband_target("CX7", "/tmp/cx7.bin")];
            if mixed {
                targets.push(inband_target("BMC", "/tmp/bmc.bin"));
            }
            let stages = firmware_update_stages_with_profile_policy(
                NodeType::ComputeGb200Nvidia,
                &targets,
                true,
                mixed,
                Some(&profile),
            )
            .unwrap();
            assert!(stages[0].verification.is_some());
            if mixed {
                assert!(stages[1].verification.is_none());
            }
            let tracker = Arc::new(JobTracker::new());
            let job = tracker
                .create_job("rack", "nic-test", JobType::FirmwareUpdate)
                .unwrap();
            let id = job.id().to_string();
            let node = Arc::new(MandatoryVerificationNode {
                events: Mutex::new(Vec::new()),
                inventory_failure,
            });
            spawn_firmware_update_job_with_policy(
                tracker.clone(),
                job,
                node.clone(),
                targets,
                true,
                true,
                FirmwareTaskPollOptions::default(),
                mixed,
                recovery_disarmed(),
                Some(Arc::new(profile)),
                None,
            );
            let job = wait_for_firmware_job_terminal(&tracker, &id).await;
            assert_eq!(
                job.state,
                crate::orchestrator::job_lifecycle::JobState::Failed,
                "{}",
                job.error_message
            );
            let events = node.events.lock().unwrap();
            assert_eq!(&events[..2], &["nic", "activate"]);
            assert!(events.contains(&"verify"));
            assert!(!events.contains(&"oob"));
            if !inventory_failure {
                assert_eq!(events.iter().filter(|event| **event == "verify").count(), 5);
            }
        }
    }
}

const GB200: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
));

#[test]
fn resolved_redfish_component_label_does_not_select_inband_transport() {
    let profile = HardwareUpdatePlan::from_yaml_str(&GB200.replacen(
        "component_key: HMC",
        "component_key: CX7",
        1,
    ))
    .unwrap();
    let target = profile.stages()[0].targets()[0]
        .resolve(&inband_target("CX7", "/tmp/package.bin"))
        .unwrap();
    let stages = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb200Nvidia,
        &[target],
        false,
        false,
        Some(&profile),
    )
    .unwrap();
    assert_eq!(stages[0].targets[0].component, "CX7");
    assert!(flint_family_for_target(&stages[0].targets[0]).is_none());
    assert!(stages[0].activation.is_none());
}

#[test]
fn supermicro_custom_parameters_filename_and_activation_are_authoritative() {
    let source = SUPERMICRO
        .replace(
            "filename: {suffix: .fwpkg, contains: nosbios, case_insensitive: true}",
            "filename: {}",
        )
        .replace(
            "/redfish/v1/Chassis/HGX_Chassis_0",
            "/redfish/v1/Chassis/Custom",
        )
        .replace("activate_after_stage: false", "activate_after_stage: true");
    let profile = HardwareUpdatePlan::from_yaml_str(&source).unwrap();
    let stages = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb300Supermicro,
        &[
            supermicro_target("HMC", "renamed.bin"),
            supermicro_target("BMC", "obmc.bin"),
        ],
        true,
        false,
        Some(&profile),
    )
    .unwrap();
    assert!(stages[1].activation.is_some());
    let target = &stages[0].targets[0];
    let options = crate::nodes::compute_gb300_supermicro::supermicro_update_options_for_target(
        target,
        FirmwareUpdateOptions::default(),
    )
    .unwrap();
    let request = crate::nodes::nvfwupd_adapter::to_update_request(target, false, options);
    assert_eq!(
        request.options.special_json,
        Some(serde_json::json!({"Targets":["/redfish/v1/Chassis/Custom"]}))
    );
    assert!(
        firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb300Supermicro,
            &[
                supermicro_target("HMC", "renamed.bin"),
                supermicro_target("hmc", "renamed.bin")
            ],
            false,
            false,
            Some(&profile)
        )
        .unwrap_err()
        .message
        .contains("duplicate firmware upload")
    );
}

#[test]
fn configured_stages_resolve_components_and_preserve_order() {
    let mut document: serde_json::Value = serde_yaml::from_str(GB200).unwrap();
    for target in document["firmware"]["targets"].as_array_mut().unwrap() {
        target["stage"] = match target["component_key"].as_str().unwrap() {
            "HMC" => serde_json::json!(5),
            "BMC" => serde_json::json!(20),
            _ => serde_json::json!(10),
        };
    }
    let profile =
        HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&document).unwrap()).unwrap();
    let targets = [
        inband_target("BMC", "/tmp/bmc-3.fwpkg"),
        inband_target("HMC", "/tmp/hmc-2.fwpkg"),
        inband_target("BMC", "/tmp/bmc-1.fwpkg"),
        inband_target("HMC", "/tmp/hmc-1.fwpkg"),
    ];
    for activate in [false, true] {
        let stages = firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb200Nvidia,
            &targets,
            activate,
            false,
            Some(&profile),
        )
        .unwrap();
        // Unselected stage 10 is omitted; packages retain request order per target.
        assert_eq!(
            stages
                .iter()
                .map(|stage| stage
                    .targets
                    .iter()
                    .map(|target| (target.component.as_str(), target.firmware_file.as_str()))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![
                vec![("HMC", "/tmp/hmc-2.fwpkg"), ("HMC", "/tmp/hmc-1.fwpkg")],
                vec![("BMC", "/tmp/bmc-3.fwpkg"), ("BMC", "/tmp/bmc-1.fwpkg")],
            ]
        );
        for stage in stages {
            assert_eq!(
                stage.activation,
                activate.then_some(FirmwareActivationMode::FullGb200Compute)
            );
            assert_eq!(
                stage.verification,
                activate.then_some(profile.verification())
            );
        }
    }
}

#[test]
fn configured_vr_oob_stages_precede_linkx_without_tray_activation_after_linkx() {
    let source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hardware-profiles/v1/compute_vrnvl72_nvidia.yaml"
    ));
    let mut document: serde_json::Value = serde_yaml::from_str(source).unwrap();
    // Exercise configured ordering instead of relying on built-in HMC/BMC order.
    document["firmware"]["targets"][0]["stage"] = serde_json::json!(20);
    let profile =
        HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&document).unwrap()).unwrap();
    let linkx = linkx_target();
    let targets = [
        linkx.clone(),
        inband_target("HMC", "/tmp/hmc.fwpkg"),
        inband_target("BMC", "/tmp/bmc.fwpkg"),
    ];
    let stages = firmware_update_stages_with_profile_policy(
        NodeType::ComputeVrnvl72Nvidia,
        &targets,
        true,
        true,
        Some(&profile),
    )
    .unwrap();
    assert_eq!(stages.len(), 3);
    assert_eq!(stages[0].targets[0].component, "BMC");
    assert_eq!(stages[1].targets[0].component, "HMC");
    for stage in &stages[..2] {
        assert!(stage.activation.is_some());
        assert!(stage.verification.is_some());
        assert!(stage.targets[0].resolution.is_some());
    }
    assert_eq!(stages[2].targets, vec![linkx.clone()]);
    assert!(stages[2].activation.is_none());
    assert!(stages[2].verification.is_none());
    let linkx_only = firmware_update_stages_with_profile_policy(
        NodeType::ComputeVrnvl72Nvidia,
        std::slice::from_ref(&linkx),
        true,
        true,
        Some(&profile),
    )
    .unwrap();
    assert_eq!(linkx_only, vec![stages[2].clone()]);
    for (activate, allow_mixed) in [(false, true), (true, false)] {
        assert!(
            firmware_update_stages_with_profile_policy(
                NodeType::ComputeVrnvl72Nvidia,
                &targets,
                activate,
                allow_mixed,
                Some(&profile),
            )
            .is_err()
        );
    }
}

#[test]
fn configured_oob_stages_preserve_required_inband_activation() {
    let profile = HardwareUpdatePlan::from_yaml_str(
        &GB200.replace("activate_after_stage: true", "activate_after_stage: false"),
    )
    .unwrap();
    let targets = [
        inband_target("BMC", "/tmp/bmc.fwpkg"),
        inband_target("CX7", "/tmp/cx7.bin"),
    ];
    let stages = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb200Nvidia,
        &targets,
        true,
        true,
        Some(&profile),
    )
    .unwrap();
    assert_eq!(stages.len(), 2);
    assert_eq!(stages[0].targets[0].component, "CX7");
    assert!(stages[0].activation.is_some());
    assert!(stages[1].activation.is_none());
    assert!(
        firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb200Nvidia,
            &targets,
            false,
            true,
            Some(&profile)
        )
        .is_err()
    );
}

#[test]
fn configured_stage_rejects_mismatched_node_type() {
    let profile = HardwareUpdatePlan::from_yaml_str(GB200).unwrap();
    let error = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb300Nvidia,
        &[inband_target("BMC", "/tmp/firmware.fwpkg")],
        true,
        false,
        Some(&profile),
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert!(
        error
            .message
            .contains("is for compute_gb200_nvidia, not compute_gb300_nvidia"),
        "{}",
        error.message
    );
}

#[test]
fn gb200_profile_reorders_and_validates_runtime_targets() {
    let catalog = crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap();
    let profile = catalog
        .resolve_request(NodeType::ComputeGb200Nvidia)
        .expect("GB200 profile");
    let hmc_request = supermicro_target("HMC", "/tmp/hgx.fwpkg");
    let hmc = supermicro_target("/redfish/v1/Chassis/HGX_Chassis_0", "/tmp/hgx.fwpkg");
    let gpu = supermicro_target(
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
        "/tmp/hgx.fwpkg",
    );
    let bmc = supermicro_target("", "/tmp/bmc.fwpkg");

    let stages = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb200Nvidia,
        &[
            supermicro_target("BMC", &bmc.firmware_file),
            supermicro_target("GPU_0", &gpu.firmware_file),
            hmc_request,
        ],
        true,
        false,
        Some(profile),
    )
    .unwrap();

    assert_eq!(
        transport_stages(&stages),
        vec![FirmwareUpdateStage {
            targets: vec![hmc, gpu, bmc],
            activate_after: true,
        }]
    );
}

#[test]
fn gb200_profile_selects_targets_without_filename_checks() {
    let catalog = crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap();
    let profile = catalog
        .resolve_request(NodeType::ComputeGb200Nvidia)
        .expect("GB200 profile");
    let bmc = supermicro_target("", "/tmp/renamed-bmc-package.bin");
    let hmc = supermicro_target("HMC", "/tmp/renamed-hmc-package");
    let expected_hmc = supermicro_target(
        "/redfish/v1/Chassis/HGX_Chassis_0",
        "/tmp/renamed-hmc-package",
    );

    let stages = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb200Nvidia,
        &[supermicro_target("BMC", &bmc.firmware_file), hmc],
        true,
        false,
        Some(profile),
    )
    .unwrap();

    assert_eq!(
        transport_stages(&stages),
        vec![FirmwareUpdateStage {
            targets: vec![expected_hmc, bmc],
            activate_after: true,
        }]
    );
}

#[test]
fn supermicro_profile_keeps_explicit_filename_selectors() {
    let catalog = crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap();
    let profile = catalog
        .by_node_type(NodeType::ComputeGb300Supermicro)
        .expect("Supermicro profile");
    let hmc_target = &profile.stages()[0].targets()[0];

    for (filename, matches) in [
        ("/tmp/firmware-nosbios.fwpkg", true),
        ("/tmp/firmware-nosbios.bin", false),
        ("/tmp/firmware.fwpkg", false),
    ] {
        let target = supermicro_target("HMC", filename);
        assert_eq!(
            profile_target_matches(hmc_target, &target),
            matches,
            "{filename}"
        );
    }
    // A matching filename must not bypass component selection.
    let wrong_component = supermicro_target("BMC", "/tmp/firmware-nosbios.fwpkg");
    assert!(!profile_target_matches(hmc_target, &wrong_component));
}

#[test]
fn profile_activation_strategy_is_checked_against_node_kind() {
    let compute = ActivationNode {
        node_type: NodeType::ComputeGb200Nvidia,
        calls: AtomicUsize::new(0),
    };
    assert_eq!(
        activation_mode_for_node_and_strategy(
            &compute,
            Some(ActivationStrategy::FullComputePowerCycle),
        )
        .unwrap(),
        FirmwareActivationMode::FullGb200Compute
    );

    let switch = ActivationNode {
        node_type: NodeType::SwitchGb200Nvidia,
        calls: AtomicUsize::new(0),
    };
    let error = activation_mode_for_node_and_strategy(
        &switch,
        Some(ActivationStrategy::FullComputePowerCycle),
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument);
}

#[test]
fn one_package_can_fan_out_to_distinct_destinations() {
    let profile = HardwareUpdatePlan::from_yaml_str(&GB200.replacen(
        "request_targets: [/redfish/v1/Chassis/HGX_Chassis_0]",
        "request_targets: [/redfish/v1/Chassis/First, /redfish/v1/Chassis/Second]",
        1,
    ))
    .unwrap();
    let configured = &profile.stages()[0].targets()[0];
    let targets = ["/redfish/v1/Chassis/First", "/redfish/v1/Chassis/Second"].map(|path| {
        configured
            .resolve(&inband_target(path, "/tmp/hmc.fwpkg"))
            .unwrap()
    });
    let stages = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb200Nvidia,
        &targets,
        true,
        true,
        Some(&profile),
    )
    .unwrap();
    assert_eq!(stages[0].targets, targets);
    assert!(
        firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb200Nvidia,
            &[inband_target("HMC", "/tmp/hmc.fwpkg")],
            true,
            true,
            Some(&profile),
        )
        .unwrap_err()
        .message
        .contains("direct requests require a single destination")
    );
    assert!(
        firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb200Nvidia,
            &[targets[0].clone(), targets[0].clone()],
            true,
            true,
            Some(&profile)
        )
        .is_err()
    );
}

const SUPERMICRO: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../hardware-profiles/v1/compute_gb300_supermicro.yaml"
));

#[test]
fn supermicro_profile_preserves_vendor_stages_and_single_bios_upload() {
    let profile = HardwareUpdatePlan::from_yaml_str(SUPERMICRO).unwrap();
    let targets = vec![
        supermicro_target("", "NvOBMC_GBNVL72_70.02.01.05.bin"),
        supermicro_target(SUPERMICRO_BIOS_TARGET, "BIOS_GPU-NVGB300_2.3b.bin"),
        supermicro_target(SUPERMICRO_HGX_TARGET, "nvfw_custom_nosbios.fwpkg"),
    ];
    let mut baseline =
        firmware_update_stages(NodeType::ComputeGb300Supermicro, &targets, true).unwrap();
    baseline[1].targets[0].component.clear();
    baseline[2].targets[0].component = SUPERMICRO_BMC_TARGET.to_owned();
    let targets = vec![
        supermicro_target("BMC", &targets[0].firmware_file),
        supermicro_target("BIOS", &targets[1].firmware_file),
        supermicro_target("HMC", &targets[2].firmware_file),
    ];
    let configured = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb300Supermicro,
        &targets,
        true,
        false,
        Some(&profile),
    )
    .unwrap();
    assert_eq!(transport_stages(&configured), baseline);
    assert_eq!(configured[1].targets.len(), 1);
    assert_eq!(configured[1].targets[0].component, "BIOS");
    let bios_request = crate::nodes::nvfwupd_adapter::to_update_request(
        &configured[1].targets[0],
        false,
        FirmwareUpdateOptions::default(),
    );
    assert_eq!(
        bios_request.options.special_json,
        Some(serde_json::json!({"Targets": [
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1",
        ]}))
    );
    assert!(configured[2].activation.is_none());
    let custom_profile = HardwareUpdatePlan::from_yaml_str(&SUPERMICRO.replacen(
        "activate_after_stage: true",
        "activate_after_stage: false",
        1,
    ))
    .unwrap();
    assert!(
        firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb300Supermicro,
            &targets,
            true,
            false,
            Some(&custom_profile)
        )
        .is_ok()
    );

    assert!(
        firmware_update_stages_with_profile_policy(
            NodeType::ComputeGb300Supermicro,
            &targets,
            false,
            false,
            Some(&profile)
        )
        .is_ok()
    );
}

#[tokio::test]
async fn supermicro_profile_job_keeps_two_activation_boundaries() {
    let profile = HardwareUpdatePlan::from_yaml_str(
        &SUPERMICRO.replace("settle_delay: 60s", "settle_delay: 0s"),
    )
    .unwrap();
    let tracker = Arc::new(JobTracker::new());
    let pending = tracker
        .create_job("rack-01", "supermicro-node", JobType::FirmwareUpdate)
        .unwrap();
    let job_id = pending.id().to_string();
    let events = Arc::new(Mutex::new(Vec::new()));
    let node = Arc::new(SupermicroSequenceNode {
        events: Arc::clone(&events),
    });
    let targets = vec![
        supermicro_target("BMC", "NvOBMC_GBNVL72_70.02.01.05.bin"),
        supermicro_target("BIOS", "BIOS_GPU-NVGB300_2.3b.bin"),
        supermicro_target("HMC", "nvfw_custom_nosbios.fwpkg"),
    ];
    spawn_firmware_update_job_with_policy(
        tracker.clone(),
        pending,
        node,
        targets,
        true,
        false,
        FirmwareTaskPollOptions::default(),
        false,
        recovery_disarmed(),
        Some(Arc::new(profile)),
        None,
    );
    let job = wait_for_firmware_job_terminal(&tracker, &job_id).await;
    assert_eq!(
        job.state,
        crate::orchestrator::job_lifecycle::JobState::Completed,
        "{}",
        job.error_message
    );
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "update:HMC".to_owned(),
            "activate".to_owned(),
            "verify:HMC".to_owned(),
            "update:BIOS".to_owned(),
            "activate".to_owned(),
            "verify:BIOS".to_owned(),
            "update:BMC".to_owned(),
        ]
    );
}

#[tokio::test]
async fn supermicro_profile_bmc_task_loss_still_requires_matching_manifest_version() {
    for matched in [true, false] {
        let profile = HardwareUpdatePlan::from_yaml_str(
            &SUPERMICRO
                .replace("\n    interval: 5s", "\n    interval: 1ms")
                .replace("transient_error_budget: 15m", "transient_error_budget: 0s"),
        )
        .unwrap();
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("rack-01", "supermicro-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let node = Arc::new(SupermicroTaskLossNode {
            version_summary: FirmwareVersionCheckSummary {
                matched,
                details: serde_json::json!({
                    "status": if matched { "matched" } else { "mismatch" },
                    "verification": "supermicro_firmware_manifest_version"
                }),
            },
            poll_calls: AtomicUsize::new(0),
            version_check_calls: AtomicUsize::new(0),
        });
        spawn_firmware_update_job_with_policy(
            tracker.clone(),
            pending,
            node.clone(),
            vec![FirmwareTarget {
                component: "BMC".into(),
                ..supermicro_bmc_target_with_firmware_manifest_version()
            }],
            true,
            false,
            FirmwareTaskPollOptions::default(),
            false,
            recovery_disarmed(),
            Some(Arc::new(profile)),
            None,
        );
        let job = wait_for_firmware_job_terminal(&tracker, &job_id).await;
        use crate::orchestrator::job_lifecycle::JobState;
        assert_eq!(
            job.state,
            if matched {
                JobState::Completed
            } else {
                JobState::Failed
            },
            "{}",
            job.error_message
        );
        assert!(node.version_check_calls.load(Ordering::SeqCst) > 0);
    }
}

#[tokio::test]
async fn firmware_job_provenance_survives_pre_execution_cancellation() {
    for profile in [
        None,
        Some(Arc::new(HardwareUpdatePlan::from_yaml_str(GB200).unwrap())),
    ] {
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("rack", "node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        pending.cancellation_token().cancel();
        let node = Arc::new(FirmwarePreflightNode::new(
            NodeType::ComputeGb200Nvidia,
            Ok(switch_preflight_summary(false)),
        ));
        spawn_firmware_update_job_with_policy(
            Arc::clone(&tracker),
            pending,
            Arc::clone(&node),
            vec![inband_target("BMC", "/tmp/test.fwpkg")],
            false,
            true,
            FirmwareTaskPollOptions::default(),
            false,
            recovery_disarmed(),
            profile.clone(),
            None,
        );
        let job = wait_for_firmware_job_terminal(&tracker, &job_id).await;
        assert_eq!(
            job.state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
        assert!(node.events.lock().unwrap().is_empty());
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(
            result["hardware_config_source"],
            if profile.is_some() {
                "profile"
            } else {
                "code_defaults"
            }
        );
        if let Some(profile) = profile {
            assert_eq!(result["hardware_profile_sha256"], profile.source_sha256());
        } else {
            assert!(result.get("hardware_profile_sha256").is_none());
        }
    }
}

#[test]
fn lenovo_profile_maps_bmc_component_to_its_inventory_destination() {
    let catalog = crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap();
    let node_type = NodeType::ComputeGb300Lenovo;
    for filename in ["host-bmc.ima", "host-bmc.hpm"] {
        let stages = firmware_update_stages_with_profile_policy(
            node_type,
            &[supermicro_target("BMC", filename)],
            true,
            false,
            catalog.resolve_request(node_type),
        )
        .unwrap();
        assert_eq!(stages[0].targets[0].component, "BMC");
        assert_eq!(
            transport_stages(&stages)[0].targets,
            vec![supermicro_target(
                "/redfish/v1/UpdateService/FirmwareInventory/BMC",
                filename
            )]
        );
    }
}

#[test]
fn built_in_profile_stages_preserve_multiple_packages_in_request_order() {
    let catalog = crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap();
    for node_type in [
        NodeType::ComputeGb200Nvidia,
        NodeType::ComputeGb200Wiwynn,
        NodeType::ComputeGb300Nvidia,
        NodeType::ComputeGb300Lenovo,
        NodeType::ComputeGb300Supermicro,
        NodeType::ComputeVrnvl72Nvidia,
    ] {
        let filenames = ["3-nosbios.fwpkg", "1-nosbios.fwpkg", "2-nosbios.fwpkg"];
        let targets = filenames.map(|filename| supermicro_target("HMC", filename));

        let stages = firmware_update_stages_with_profile_policy(
            node_type,
            &targets,
            true,
            false,
            catalog.resolve_request(node_type),
        )
        .unwrap();

        assert_eq!(stages.len(), 1, "{node_type:?}");
        assert_eq!(
            stages[0]
                .targets
                .iter()
                .map(|target| target.firmware_file.as_str())
                .collect::<Vec<_>>(),
            filenames,
            "{node_type:?}",
        );
    }
}

#[test]
fn vendor_profile_stages_keep_multi_package_order() {
    let catalog = crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap();
    for (node_type, component) in [
        (NodeType::ComputeGb200Wiwynn, "BMC"),
        (NodeType::ComputeGb300Lenovo, "HMC"),
    ] {
        let targets = vec![
            supermicro_target(component, "first.fwpkg"),
            supermicro_target(component, "second.fwpkg"),
        ];
        let configured = firmware_update_stages_with_profile_policy(
            node_type,
            &targets,
            true,
            false,
            catalog.resolve_request(node_type),
        )
        .unwrap();
        let mut expected = firmware_update_stages(node_type, &targets, true).unwrap();
        if matches!(
            node_type,
            NodeType::ComputeGb300Lenovo | NodeType::ComputeGb200Wiwynn
        ) {
            for target in &mut expected[0].targets {
                target.component.clear();
            }
        }
        assert_eq!(transport_stages(&configured), expected);
        assert!(
            firmware_update_stages_with_profile_policy(
                node_type,
                &[targets[0].clone(), targets[0].clone()],
                true,
                false,
                catalog.resolve_request(node_type)
            )
            .is_err()
        );
    }
}

const DELL: &str =
    include_str!("../../../../../../../hardware-profiles/v1/compute_gb200_dell.yaml");

struct DellSequenceNode {
    events: Mutex<Vec<String>>,
    failure: &'static str,
    cancellation: tokio_util::sync::CancellationToken,
}

#[async_trait]
impl Node for DellSequenceNode {
    fn id(&self) -> &str {
        "dell-node"
    }
    fn rack_id(&self) -> &str {
        "rack-01"
    }
    fn node_type(&self) -> NodeType {
        NodeType::ComputeGb200Dell
    }
    fn get_info(&self) -> HashMap<String, String> {
        HashMap::new()
    }
    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        _: bool,
        _: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        self.events
            .lock()
            .unwrap()
            .push(format!("update:{}", target.component));
        if self.failure == "upload" && target.component == "BMC" {
            return Err(RmsError::internal("BMC upload rejected"));
        }
        if self.failure == "cancel" && target.component == "BMC" {
            self.cancellation.cancel();
        }
        Ok(FirmwareUpdateOutcome::Started(
            crate::domain::node::FirmwareTaskHandle {
                task_id: target.component.clone(),
            },
        ))
    }
    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let failed = self.failure == "task" && task_id == "BMC";
        Ok(FirmwareTaskStatus {
            completed: true,
            percent: 100,
            state: if failed { "Exception" } else { "Completed" }.into(),
            status: if failed { "Critical" } else { "OK" }.into(),
            message: String::new(),
            rejection: None,
        })
    }
    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        assert_eq!(request.mode, FirmwareActivationMode::FullGb200Compute);
        self.events.lock().unwrap().push("activate".into());
        if self.failure == "activation" {
            return Err(RmsError::internal("aux power cycle failed"));
        }
        Ok(FirmwareActivationSummary {
            message: "activated".into(),
            details: serde_json::Value::Null,
        })
    }
    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        let components = targets
            .iter()
            .map(|target| target.component.as_str())
            .collect::<Vec<_>>()
            .join(",");
        self.events
            .lock()
            .unwrap()
            .push(format!("verify:{components}"));
        let matched = self.failure != "verification";
        Ok(FirmwareVersionCheckSummary {
            matched,
            details: serde_json::json!({"status": if matched { "matched" } else { "mismatch" }}),
        })
    }
}

fn dell_targets() -> Vec<FirmwareTarget> {
    // Deliberately reverse the requested stage order. The profile owns execution order.
    vec![
        inband_target("BIOS", "/tmp/Dell_SBIOS.fwpkg"),
        inband_target("HMC", "/tmp/compute_nosbios.fwpkg"),
        inband_target("BMC", "/tmp/ExampleBmcFwV250729001500.ima"),
    ]
}

#[test]
fn dell_default_profile_keeps_two_activation_stages_and_all_packages() {
    let mut targets = dell_targets();
    targets.push(inband_target("HMC", "/tmp/second_nosbios.fwpkg"));
    // With no external override, the catalog resolves Dell's embedded profile.
    let embedded = HardwareProfileCatalog::default()
        .shared_for_request(NodeType::ComputeGb200Dell)
        .expect("Dell has an embedded profile");
    let stages = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb200Dell,
        &targets,
        true,
        false,
        Some(&embedded),
    )
    .unwrap();
    assert_eq!(stages.len(), 2);
    assert!(stages.iter().all(|stage| stage.activation
        == Some(FirmwareActivationMode::FullGb200Compute)
        && stage.verification.is_some()));
    assert_eq!(stages[0].targets[0].component, "BMC");
    assert_eq!(
        stages[1]
            .targets
            .iter()
            .map(|target| target.firmware_file.as_str())
            .collect::<Vec<_>>(),
        [
            "/tmp/compute_nosbios.fwpkg",
            "/tmp/second_nosbios.fwpkg",
            "/tmp/Dell_SBIOS.fwpkg"
        ]
    );
    let profile =
        HardwareUpdatePlan::from_yaml_str(&DELL.replace("stage: 10", "stage: 30")).unwrap();
    let custom = firmware_update_stages_with_profile_policy(
        NodeType::ComputeGb200Dell,
        &targets,
        true,
        false,
        Some(&profile),
    )
    .unwrap();
    assert_eq!(custom[0].targets[0].component, "HMC");
    assert_eq!(custom[1].targets[0].component, "BMC");
}

#[tokio::test]
async fn dell_stages_stop_on_upload_task_activation_verification_failure_or_cancellation() {
    for failure in [
        "none",
        "upload",
        "task",
        "activation",
        "verification",
        "cancel",
    ] {
        let profile = HardwareUpdatePlan::from_yaml_str(
            &DELL
                .replace("settle_delay: 60s", "settle_delay: 0s")
                .replace("attempts: 5", "attempts: 1")
                .replace("\n    interval: 5s", "\n    interval: 1ms"),
        )
        .unwrap();
        let tracker = Arc::new(JobTracker::new());
        let pending = tracker
            .create_job("rack-01", "dell-node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        let node = Arc::new(DellSequenceNode {
            events: Mutex::new(Vec::new()),
            failure,
            cancellation: pending.cancellation_token(),
        });
        spawn_firmware_update_job_with_policy(
            tracker.clone(),
            pending,
            node.clone(),
            dell_targets(),
            true,
            false,
            FirmwareTaskPollOptions::default(),
            false,
            recovery_disarmed(),
            Some(Arc::new(profile)),
            None,
        );
        let job = wait_for_firmware_job_terminal(&tracker, &job_id).await;
        use crate::orchestrator::job_lifecycle::JobState;
        let events = node.events.lock().unwrap();
        if failure == "none" {
            assert_eq!(job.state, JobState::Completed, "{}", job.error_message);
            assert_eq!(
                *events,
                [
                    "update:BMC",
                    "activate",
                    "verify:BMC",
                    "update:HMC",
                    "update:BIOS",
                    "activate",
                    "verify:HMC,BIOS"
                ]
            );
        } else {
            assert_ne!(job.state, JobState::Completed, "{failure}");
            assert!(
                !events
                    .iter()
                    .any(|event| event == "update:HMC" || event == "update:BIOS"),
                "{failure}: {events:?}"
            );
            if failure == "cancel" {
                assert_eq!(job.state, JobState::Failed);
                assert!(job.error_message.to_ascii_lowercase().contains("cancel"));
            }
        }
    }
}
