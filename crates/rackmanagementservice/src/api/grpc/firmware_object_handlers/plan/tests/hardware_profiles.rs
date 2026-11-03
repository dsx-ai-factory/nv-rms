// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::config::hardware_profiles::{HardwareProfileCatalog, HardwareUpdatePlan};

const GB200: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
));

fn custom_profile(target: &str) -> HardwareUpdatePlan {
    HardwareUpdatePlan::from_yaml_str(&GB200.replacen(
        "/redfish/v1/Chassis/HGX_Chassis_0",
        target,
        1,
    ))
    .unwrap()
}

#[test]
fn vr_profile_preserves_opt_in_linkx_policy_after_configured_oob_targets() {
    let mut manifest = compute_manifest();
    let linkx = super::super::super::tests::linkx_manifest();
    manifest.milestones[0].board_skus[0]
        .components
        .firmware
        .extend(
            linkx.milestones[0].board_skus[0]
                .components
                .firmware
                .clone(),
        );
    let source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hardware-profiles/v1/compute_vrnvl72_nvidia.yaml"
    ));
    for source in [source.to_owned(), source.replace("stage: 10", "stage: 30")] {
        let profile = HardwareUpdatePlan::from_yaml_str(&source).unwrap();
        for components in [None, Some(vec!["LINKX"]), Some(vec!["LINKX", "BMC", "HMC"])] {
            let selection = components.as_ref().map_or_else(
                FirmwareObjectComponentSelection::default_only,
                |values| {
                    FirmwareObjectComponentSelection::components(
                        values.iter().map(|value| (*value).to_owned()).collect(),
                    )
                },
            );
            let plan = firmware_plan_with_profile(
                &manifest,
                NodeType::ComputeVrnvl72Nvidia,
                "prod",
                &selection,
                Some(&profile),
            )
            .unwrap();
            let cache = tempfile::tempdir().unwrap();
            let ready = plan
                .targets
                .iter()
                .map(|target| {
                    let path = target.artifact.cache_path(cache.path()).unwrap();
                    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                    std::fs::write(path, b"firmware fixture").unwrap();
                    target.artifact.id.clone()
                })
                .collect();
            let targets = plan.resolve(cache.path(), &ready).unwrap();
            if let Some(components) = components {
                assert_eq!(targets.len(), components.len());
                let target = targets.last().unwrap();
                assert_eq!(target.component, "LINKX");
                assert_eq!(target.expected_version.as_deref(), Some("44.40.2035"));
                assert_eq!(
                    target.linkx_policy.as_ref().unwrap().current_version,
                    "44.40.2019"
                );
                assert!(target.resolution.is_none());
                assert!(
                    targets[..targets.len() - 1]
                        .iter()
                        .all(|target| target.resolution.is_some())
                );
            } else {
                assert_eq!(targets.len(), 2);
                assert!(targets.iter().all(|target| target.linkx_policy.is_none()));
            }
        }
    }
}

#[test]
fn profile_plan_excludes_recovery_images_without_changing_recovery_bundle_selection() {
    let manifest = with_hmc_location(
        compute_manifest(),
        serde_json::json!({
            "Location": "https://example.test/hmc_recovery.fwpkg",
            "LocationType": "https", "Type": "Firmware"
        }),
    );
    let profile = custom_profile("/redfish/v1/Chassis/Custom");
    let plan = firmware_plan_with_profile(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "prod",
        &FirmwareObjectComponentSelection::components(vec!["HMC".to_owned()]),
        Some(&profile),
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 1);
    assert_eq!(plan.targets[0].artifact.filename, "hmc.fwpkg");
    assert_eq!(
        plan.targets[0].transport_target(),
        "/redfish/v1/Chassis/Custom"
    );

    let recovery = recovery_artifacts(&manifest, NodeType::ComputeGb200Nvidia)
        .unwrap()
        .unwrap();
    assert_eq!(recovery.regular.id, plan.targets[0].artifact.id);
    assert_eq!(recovery.recovery.filename, "hmc_recovery.fwpkg");
}

#[test]
fn profile_targets_are_platform_local_and_do_not_change_artifact_identity() {
    let manifest = compute_manifest();
    let profile = custom_profile("/redfish/v1/Chassis/HGX_Custom_0");
    let selection = FirmwareObjectComponentSelection::default_only();
    let configured = firmware_plan_with_profile(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "prod",
        &selection,
        Some(&profile),
    )
    .unwrap();
    let other = firmware_plan(&manifest, NodeType::ComputeGb300Nvidia, "prod", &selection).unwrap();
    assert_eq!(
        configured.targets[0].transport_target(),
        "/redfish/v1/Chassis/HGX_Custom_0"
    );
    assert_eq!(
        other.targets[0].transport_target(),
        "/redfish/v1/Chassis/HGX_Chassis_0"
    );
    assert_eq!(
        configured.targets[0].artifact.id,
        other.targets[0].artifact.id
    );
    let configured_again = firmware_plan_with_profile(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "prod",
        &selection,
        Some(&profile),
    )
    .unwrap();
    assert_eq!(
        configured_again.targets[0].transport_target(),
        configured.targets[0].transport_target()
    );
    let expectations = expectations_with_profile(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "prod",
        &selection,
        Some(&profile),
    )
    .unwrap();
    assert!(
        expectations
            .iter()
            .any(|expected| expected.target == "/redfish/v1/Chassis/HGX_Custom_0")
    );
}

#[test]
fn profile_component_filters_use_only_keys_and_preserve_manifest_fanout() {
    let profile = HardwareUpdatePlan::from_yaml_str(
        &GB200
            .replacen("    - id: hmc", "    - id: chassis_update", 1)
            .replacen("component_key: HMC", "component_key: CHASSIS", 1)
            .replacen(
                "request_targets: [/redfish/v1/Chassis/HGX_Chassis_0]",
                "request_targets: [/redfish/v1/Chassis/First, /redfish/v1/Chassis/Second]",
                1,
            ),
    )
    .unwrap();
    let manifest = compute_manifest();
    for component in ["CHASSIS", "chassis"] {
        let selection = FirmwareObjectComponentSelection::components(vec![component.into()]);
        let plan = firmware_plan_with_profile(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "prod",
            &selection,
            Some(&profile),
        )
        .unwrap();
        assert_eq!(plan.targets.len(), 2);
        assert_eq!(plan.targets[0].firmware.component, "CHASSIS");
        assert_eq!(plan.targets[0].artifact.id, plan.targets[1].artifact.id);
        assert_eq!(
            plan.targets[0].transport_target(),
            "/redfish/v1/Chassis/First"
        );
        assert_eq!(
            plan.targets[1].transport_target(),
            "/redfish/v1/Chassis/Second"
        );
    }
    for component in [
        "HMC",
        "UNCONFIGURED_COMPONENT",
        "chassis_update",
        "/redfish/v1/Chassis/First",
        "",
    ] {
        let selection = FirmwareObjectComponentSelection::components(vec![component.into()]);
        let result = firmware_plan_with_profile(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "prod",
            &selection,
            Some(&profile),
        );
        assert!(
            matches!(result, Err(PlanError::Invalid(_))),
            "unexpected request selector {component:?}"
        );
    }
}

#[test]
fn profile_filename_filters_select_matching_manifest_packages() {
    let mut manifest = compute_manifest();
    let hmc = &mut manifest.milestones[0].board_skus[0].components.firmware[1];
    let mut extra = hmc.locations[0].clone();
    extra.location = Some("https://example.test/alternate.fwpkg".to_owned());
    hmc.locations.push(extra);
    let selection = FirmwareObjectComponentSelection::components(vec!["HMC".to_owned()]);
    let profile = custom_profile("/redfish/v1/Chassis/HGX_Custom_0");
    assert_eq!(
        firmware_plan_with_profile(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "prod",
            &selection,
            Some(&profile)
        )
        .unwrap()
        .targets
        .len(),
        2
    );
    let filtered = HardwareUpdatePlan::from_yaml_str(&GB200.replacen(
        "    - id: hmc",
        "    - id: hmc\n      filename: {contains: alternate}",
        1,
    ))
    .unwrap();
    let plan = firmware_plan_with_profile(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "prod",
        &selection,
        Some(&filtered),
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 1);
    assert_eq!(plan.targets[0].artifact.filename, "alternate.fwpkg");
}

#[test]
fn profile_default_selection_and_metadata_use_the_same_mapping() {
    let manifest = compute_manifest();
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("gb200.yaml"),
        GB200
            .replacen(
                "/redfish/v1/Chassis/HGX_Chassis_0",
                "/redfish/v1/Chassis/Custom",
                1,
            )
            .replacen("      default_apply: true", "      default_apply: false", 1),
    )
    .unwrap();
    let catalog = HardwareProfileCatalog::load_directory(directory.path()).unwrap();
    let profile = catalog.resolve_request(NodeType::ComputeGb200Nvidia);
    let plan = firmware_plan_with_profile(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "prod",
        &FirmwareObjectComponentSelection::default_only(),
        profile,
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 1);
    assert!(plan.targets[0].transport_target().is_empty());
    let metadata =
        super::super::super::metadata::from_manifest_with_profiles(&manifest, Some(&catalog))
            .unwrap();
    let nvidia = metadata
        .device_components
        .iter()
        .find(|device| {
            device.node_type
                == super::super::super::domain_node_type_to_proto(NodeType::ComputeGb200Nvidia)
                    as i32
        })
        .unwrap();
    assert!(
        nvidia
            .components
            .iter()
            .filter(|component| component.name == "HMC")
            .flat_map(|component| &component.artifacts)
            .any(|artifact| artifact.target == "/redfish/v1/Chassis/Custom")
    );
}

#[test]
fn built_in_compute_profiles_preserve_manifest_mappings_and_plans() {
    let catalog = HardwareProfileCatalog::built_in().unwrap();
    for node_type in [
        NodeType::ComputeGb200Nvidia,
        NodeType::ComputeGb200Wiwynn,
        NodeType::ComputeGb300Nvidia,
        NodeType::ComputeGb300Lenovo,
        NodeType::ComputeGb300Supermicro,
        NodeType::ComputeVrnvl72Nvidia,
    ] {
        let profile = catalog.resolve_request(node_type).unwrap();
        let baseline_mappings =
            super::super::super::get_firmware_component_mappings_for_node_type(node_type)
                .into_iter()
                .filter(|mapping| {
                    super::super::super::firmware_component_allowed_for_node_type(
                        node_type,
                        mapping.target_key,
                    )
                })
                .collect::<Vec<_>>();
        let configured_mappings = super::super::super::profile::mappings(node_type, Some(profile));
        assert_eq!(
            configured_mappings.len(),
            baseline_mappings.len(),
            "{node_type:?}"
        );
        for mapping in &baseline_mappings {
            assert!(
                configured_mappings
                    .iter()
                    .any(
                        |selected| selected.mapping.source_component == mapping.source_component
                            && selected.mapping.target_key == mapping.target_key
                            && selected.mapping.default_apply == mapping.default_apply
                    ),
                "{node_type:?}: {mapping:?}"
            );
        }
        let manifest = if node_type == NodeType::ComputeGb300Supermicro {
            supermicro_manifest()
        } else {
            compute_manifest()
        };
        let mut selections = vec![FirmwareObjectComponentSelection::default_only()];
        selections.extend(
            baseline_mappings
                .iter()
                .filter(|mapping| {
                    mapping.source_component == "HMC" || mapping.source_component == "BMC"
                })
                .map(|mapping| {
                    FirmwareObjectComponentSelection::components(vec![
                        mapping.target_key.to_owned(),
                    ])
                }),
        );
        for selection in selections {
            let mut baseline = firmware_plan(&manifest, node_type, "prod", &selection).unwrap();
            // The old planner also selects other targets sharing a manifest source.
            // Profile requests select only the literal component keys requested.
            if !selection.components.is_empty() {
                baseline.targets.retain(|target| {
                    baseline_mappings.iter().any(|mapping| {
                        mapping.redfish_target == target.firmware.component
                            && selection
                                .components
                                .iter()
                                .any(|component| mapping.target_key.eq_ignore_ascii_case(component))
                    })
                });
            }
            let configured =
                firmware_plan_with_profile(&manifest, node_type, "prod", &selection, Some(profile))
                    .unwrap();
            let describe = |plan: &FirmwarePlan| {
                plan.targets
                    .iter()
                    .map(|target| {
                        (
                            target.artifact.id.clone(),
                            target.artifact.filename.clone(),
                            target.firmware.expected_version.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(describe(&configured), describe(&baseline), "{node_type:?}");
        }
    }
}

#[test]
fn supermicro_profile_preserves_case_insensitive_package_selection() {
    let catalog = HardwareProfileCatalog::built_in().unwrap();
    let mut manifest = supermicro_manifest();
    for component in &mut manifest.milestones[0].board_skus[0].components.firmware {
        for location in &mut component.locations {
            if let Some(filename) = &mut location.file_name {
                *filename = filename.to_ascii_uppercase();
            }
        }
    }
    let node_type = NodeType::ComputeGb300Supermicro;
    let configured = firmware_plan_with_profile(
        &manifest,
        node_type,
        "prod",
        &FirmwareObjectComponentSelection::default_only(),
        catalog.resolve_request(node_type),
    )
    .unwrap();
    assert_eq!(configured.targets.len(), 3);
    assert_eq!(configured.targets[1].firmware.component, "BIOS");
    assert_eq!(
        configured.targets[2].firmware.expected_version.as_deref(),
        Some("70.02.01.05")
    );
}

#[test]
fn multipart_manifest_plan_uploads_once_and_verifies_each_configured_destination() {
    let manifest = supermicro_manifest();
    let catalog = HardwareProfileCatalog::built_in().unwrap();
    let node_type = NodeType::ComputeGb300Supermicro;
    let profile = catalog.resolve_request(node_type);
    let selection = FirmwareObjectComponentSelection::components(vec!["BIOS".into()]);
    let plan =
        firmware_plan_with_profile(&manifest, node_type, "prod", &selection, profile).unwrap();
    assert_eq!(plan.targets.len(), 1);
    let target = &plan.targets[0].firmware;
    let request = crate::nodes::nvfwupd_adapter::to_update_request(
        target,
        false,
        crate::domain::node::FirmwareUpdateOptions::default(),
    );
    let paths = [
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1",
    ];
    assert_eq!(
        request.options.special_json,
        Some(serde_json::json!({"Targets":paths}))
    );
    let checks =
        crate::nodes::nvfwupd_adapter::to_version_check_targets(std::slice::from_ref(target));
    assert_eq!(
        checks
            .iter()
            .map(|check| check.component.as_str())
            .collect::<Vec<_>>(),
        paths
    );
    let expectations =
        expectations_with_profile(&manifest, node_type, "prod", &selection, profile).unwrap();
    assert_eq!(
        expectations
            .iter()
            .map(|expected| expected.target.as_str())
            .collect::<Vec<_>>(),
        paths
    );
}

#[test]
fn built_in_profiles_preserve_multiple_packages_in_manifest_order() {
    let catalog = HardwareProfileCatalog::built_in().unwrap();
    for node_type in [
        NodeType::ComputeGb200Nvidia,
        NodeType::ComputeGb200Wiwynn,
        NodeType::ComputeGb300Nvidia,
        NodeType::ComputeGb300Lenovo,
        NodeType::ComputeGb300Supermicro,
        NodeType::ComputeVrnvl72Nvidia,
    ] {
        for (index, component, package_name) in
            [(0, "BMC", "obmc.bin"), (1, "HMC", "nosbios.fwpkg")]
        {
            let mut manifest = compute_manifest();
            let locations =
                &mut manifest.milestones[0].board_skus[0].components.firmware[index].locations;
            let filenames = [3, 1, 2].map(|order| format!("{order}-{package_name}"));
            *locations = filenames
                .iter()
                .map(|filename| {
                    let mut location = locations[0].clone();
                    location.location = Some(format!("https://example.test/{filename}"));
                    location
                })
                .collect();
            let selection =
                FirmwareObjectComponentSelection::components(vec![component.to_owned()]);
            let configured = firmware_plan_with_profile(
                &manifest,
                node_type,
                "prod",
                &selection,
                catalog.resolve_request(node_type),
            )
            .unwrap();
            assert_eq!(
                configured
                    .targets
                    .iter()
                    .map(|target| target.artifact.filename.as_str())
                    .collect::<Vec<_>>(),
                filenames,
                "{node_type:?} {component}",
            );
        }
    }
}

#[test]
fn dell_profile_preserves_packages_parameters_and_manifest_versions() {
    let manifest: FirmwareManifest = serde_json::from_value(serde_json::json!({
        "ProductName": "GB200",
        "Milestones": [{"Name": "1.0", "BoardSKUs": [{"Type": "Compute Node", "Components": {"Firmware": [
            {"Component": "BMC", "Type": "Prod", "Version": "25.07.29001500", "Locations": [
                {"Location": "https://example.test/3.ima", "Type": "Firmware", "LocationType": "HTTPS"},
                {"Location": "https://example.test/1.ima", "Type": "Firmware", "LocationType": "HTTPS"},
                {"Location": "https://example.test/2.ima", "Type": "Firmware", "LocationType": "HTTPS"}
            ]},
            {"Component": "HMC_NOSBIOS", "Type": "Prod", "Version": "25.06-A", "Locations": [
                {"Location": "https://example.test/first_nosbios.fwpkg", "Type": "Firmware", "LocationType": "HTTPS"},
                {"Location": "https://example.test/second_nosbios.fwpkg", "Type": "Firmware", "LocationType": "HTTPS"}
            ]},
            {"Component": "SBIOS", "Type": "Prod", "Version": "216", "Locations": [
                {"Location": "https://example.test/Dell_SBIOS.fwpkg", "Type": "Firmware", "LocationType": "HTTPS"}
            ]}
        ]}}]}]
    })).unwrap();
    let catalog = HardwareProfileCatalog::default();
    let node_type = NodeType::ComputeGb200Dell;
    let profile = catalog.resolve_request(node_type);
    let selection = FirmwareObjectComponentSelection::default_only();
    let plan =
        firmware_plan_with_profile(&manifest, node_type, "prod", &selection, profile).unwrap();
    assert_eq!(
        plan.targets
            .iter()
            .map(|target| target.artifact.filename.as_str())
            .collect::<Vec<_>>(),
        [
            "3.ima",
            "1.ima",
            "2.ima",
            "first_nosbios.fwpkg",
            "second_nosbios.fwpkg",
            "Dell_SBIOS.fwpkg"
        ]
    );
    for planned in &plan.targets[..3] {
        assert_eq!(planned.firmware.component, "BMC");
        assert_eq!(
            planned.firmware.expected_version.as_deref(),
            Some("25.07.29001500")
        );
    }
    let request = crate::nodes::nvfwupd_adapter::to_update_request(
        &plan.targets[5].firmware,
        false,
        Default::default(),
    );
    assert_eq!(
        request.options.special_json,
        Some(serde_json::json!({"Targets": [
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1"
        ]}))
    );
    assert_eq!(
        request.options.oem_parameters,
        Some(serde_json::json!({"ImageType": "PLDM", "Platform": "HGX"}))
    );
    let mut missing_version = manifest.clone();
    missing_version.milestones[0].board_skus[0]
        .components
        .firmware[0]
        .version = None;
    assert!(
        firmware_plan_with_profile(&missing_version, node_type, "prod", &selection, profile)
            .err()
            .unwrap()
            .to_string()
            .contains("manifest Version")
    );
}
