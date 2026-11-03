// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::super::get_firmware_component_mappings_for_node_type;
use super::*;

mod hardware_profiles;
mod switch_erot;

/// Model the API boundary: resolve one channel before entering any planner.
fn plan_request(
    manifest: &FirmwareManifest,
    node_type: NodeType,
    requested: &str,
    selection: &FirmwareObjectComponentSelection,
) -> Result<FirmwarePlan, PlanError> {
    let channel = resolve_channel(manifest, requested)?;
    super::firmware_plan(manifest, node_type, &channel, selection)
}

fn switch_request(manifest: &FirmwareManifest, requested: &str) -> Result<Artifact, PlanError> {
    let channel = resolve_channel(manifest, requested)?;
    super::switch_image(manifest, &channel)
}

fn compute_manifest() -> FirmwareManifest {
    serde_json::from_value(serde_json::json!({
        "ProductName": "GB200",
        "Milestones": [{"Name": "1.0", "BoardSKUs": [{
            "Type": "Compute Node",
            "Components": {"Firmware": [
                {"Component": "BMC", "Type": "Prod", "Version": "1.0",
                 "Locations": [{"Location": "https://example.test/bmc.fwpkg",
                     "LocationType": "https", "Type": "Firmware"}]},
                {"Component": "HMC", "Type": "Prod", "Version": "2.0",
                 "Locations": [{"Location": "https://example.test/hmc.fwpkg",
                     "LocationType": "https", "Type": "Firmware"}]}
            ]}
        }]}]
    }))
    .unwrap()
}

#[test]
fn every_supported_firmware_target_preserves_all_eligible_locations_in_order() {
    for (node_type, _) in super::super::firmware_node_type_lookup_keys() {
        for mapping in get_firmware_component_mappings_for_node_type(node_type) {
            // LinkX's rollout policy permits exactly one image, tested separately.
            if mapping.target_key == "LINKX" {
                continue;
            }
            if !firmware_component_allowed_for_node_type(node_type, mapping.target_key) {
                continue;
            }
            let filenames: Vec<_> = [3, 1, 2]
                .into_iter()
                .map(|index| match (node_type, mapping.target_key) {
                    (NodeType::ComputeGb300Supermicro, "HMC") => {
                        format!("compute-{index}-nosbios.fwpkg")
                    }
                    (NodeType::ComputeGb300Supermicro, "BIOS") => {
                        format!("BIOS-{index}.bin")
                    }
                    (NodeType::ComputeGb300Supermicro, "BMC") => {
                        format!("NvOBMC-{index}.bin")
                    }
                    _ => format!("firmware-{index}.bin"),
                })
                .collect();
            let mut locations: Vec<_> = filenames
                .iter()
                .map(|filename| {
                    serde_json::json!({
                        "Location": format!("https://example.test/{filename}"),
                        "LocationType": "HTTPS", "Type": "Firmware"
                    })
                })
                .collect();
            // Supermicro's HMC filter also selects the separately staged BIOS.
            if node_type == NodeType::ComputeGb300Supermicro && mapping.target_key == "HMC" {
                locations.push(serde_json::json!({
                    "Location": "https://example.test/BIOS.bin",
                    "LocationType": "HTTPS", "Type": "Firmware"
                }));
            }
            locations.insert(
                1,
                serde_json::json!({
                    "Location": "https://example.test/attestation.corim",
                    "LocationType": "HTTPS", "Type": "Certificate"
                }),
            );
            locations.push(serde_json::json!({
                "Location": null, "FileName": "metadata-only.bin", "Type": "Firmware"
            }));
            let mut manifest = compute_manifest();
            let board = &mut manifest.milestones[0].board_skus[0];
            board.board_type = match node_type.kind() {
                NodeKind::Compute => "Compute Node",
                NodeKind::Switch => "Switch Tray",
                NodeKind::Powershelf => "Power Shelf",
            }
            .to_owned();
            board.components.firmware = vec![
                serde_json::from_value(serde_json::json!({
                    "Component": mapping.source_component, "Type": "Prod", "Version": "1.0",
                    "Locations": locations
                }))
                .unwrap(),
            ];

            let plan = plan_request(
                &manifest,
                node_type,
                "prod",
                &FirmwareObjectComponentSelection::components(vec![mapping.target_key.to_owned()]),
            )
            .unwrap_or_else(|error| {
                panic!("{} {}: {error}", node_type.as_str(), mapping.target_key)
            });

            let targets: Vec<_> = plan
                .targets
                .iter()
                .filter(|target| target.transport_target() == mapping.redfish_target)
                .collect();
            assert_eq!(
                targets
                    .iter()
                    .map(|target| &target.artifact.filename)
                    .collect::<Vec<_>>(),
                filenames.iter().collect::<Vec<_>>(),
                "{} {}",
                node_type.as_str(),
                mapping.target_key
            );
            assert_eq!(
                targets
                    .iter()
                    .map(|target| &target.artifact.id)
                    .collect::<HashSet<_>>()
                    .len(),
                3
            );
        }
    }
}

fn supermicro_manifest() -> FirmwareManifest {
    let mut manifest = compute_manifest();
    let components = &mut manifest.milestones[0].board_skus[0].components.firmware;
    components[0].version = Some("70.02.01.05".to_owned());
    for (component, filenames) in components.iter_mut().zip([
        vec!["NvOBMC_test-image.bin", "unrelated.bin", "bmc.fwpkg"],
        vec![
            "test-bios-image.bin",
            "compute-nosbios.fwpkg",
            "compute.fwpkg",
            "notes.txt",
        ],
    ]) {
        component.locations = filenames
            .into_iter()
            .map(|filename| {
                serde_json::from_value(serde_json::json!({
                    "Location": format!("https://example.test/{filename}"), "FileName": filename,
                    "LocationType": "HTTPS", "Type": "Firmware"
                }))
                .unwrap()
            })
            .collect();
    }
    manifest
}

#[test]
fn linkx_plan_is_opt_in_vr_only_and_resolves_rollout_policy() {
    let manifest = super::super::tests::linkx_manifest();
    let node_type = NodeType::ComputeVrnvl72Nvidia;
    let selection = FirmwareObjectComponentSelection::components(vec!["LINKX".to_owned()]);
    assert!(matches!(
        plan_request(
            &manifest,
            node_type,
            "prod",
            &FirmwareObjectComponentSelection::default_only()
        ),
        Err(PlanError::NoComponents(_))
    ));
    for (other, _) in super::super::firmware_node_type_lookup_keys() {
        if other != node_type {
            assert!(plan_request(&manifest, other, "prod", &selection).is_err());
        }
    }
    let plan = plan_request(&manifest, node_type, "prod", &selection).unwrap();
    assert_eq!(plan.targets.len(), 1);
    let planned = &plan.targets[0];
    assert_eq!(planned.artifact.filename, "module.bin");
    let cache = tempfile::tempdir().unwrap();
    let path = planned.artifact.cache_path(cache.path()).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"firmware fixture").unwrap();
    let targets = plan
        .resolve(cache.path(), &HashSet::from([planned.artifact.id.clone()]))
        .unwrap();
    assert_eq!(targets[0].component, "LINKX");
    assert_eq!(targets[0].expected_version.as_deref(), Some("44.40.2035"));
    let policy = targets[0].linkx_policy.as_ref().unwrap();
    assert_eq!(policy.current_version, "44.40.2019");
    assert_eq!(policy.target_version, "44.40.2035");
    assert_eq!(policy.sha256, "a".repeat(64));
    // Prefetch and apply must filter the same non-firmware locations.
    let artifacts = catalog_artifacts(&manifest).unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].id, planned.artifact.id);
}

#[test]
fn linkx_plan_rejects_multiple_images_and_duplicate_components_or_boards() {
    for duplicate in ["image", "component", "board"] {
        let mut manifest = super::super::tests::linkx_manifest();
        let boards = &mut manifest.milestones[0].board_skus;
        let component = &mut boards[0].components.firmware[0];
        match duplicate {
            "image" => {
                let mut location = component.locations[1].clone();
                location.location = Some("https://example.test/other.bin".to_owned());
                component.locations.push(location);
            }
            "component" => {
                let duplicate = component.clone();
                boards[0].components.firmware.push(duplicate);
            }
            _ => boards.push(boards[0].clone()),
        }
        let error = plan_request(
            &manifest,
            NodeType::ComputeVrnvl72Nvidia,
            "prod",
            &FirmwareObjectComponentSelection::components(vec!["LINKX".to_owned()]),
        )
        .err()
        .unwrap();
        assert!(
            error.to_string().contains("ambiguous"),
            "{duplicate}: {error}"
        );
    }
}

#[test]
fn linkx_plan_requires_valid_rollout_and_matching_version() {
    for invalid in ["missing", "hash", "version"] {
        let mut manifest = super::super::tests::linkx_manifest();
        let component = &mut manifest.milestones[0].board_skus[0].components.firmware[0];
        match invalid {
            "missing" => component.linkx_rollout = None,
            "hash" => {
                component.linkx_rollout.as_mut().unwrap()["sha256"] = serde_json::json!("invalid")
            }
            _ => component.version = Some("different".to_owned()),
        }
        assert!(
            plan_request(
                &manifest,
                NodeType::ComputeVrnvl72Nvidia,
                "prod",
                &FirmwareObjectComponentSelection::components(vec!["LINKX".to_owned()]),
            )
            .is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn supermicro_planning_filters_packages_and_orders_hmc_bios_bmc() {
    let plan = plan_request(
        &supermicro_manifest(),
        NodeType::ComputeGb300Supermicro,
        "prod",
        &FirmwareObjectComponentSelection::default_only(),
    )
    .unwrap();
    assert_eq!(
        plan.targets
            .iter()
            .map(|t| t.artifact.filename.as_str())
            .collect::<Vec<_>>(),
        [
            "compute-nosbios.fwpkg",
            "test-bios-image.bin",
            "NvOBMC_test-image.bin"
        ]
    );
    assert_eq!(
        plan.targets
            .iter()
            .map(|t| t.transport_target())
            .collect::<Vec<_>>(),
        [
            super::super::SUPERMICRO_HGX_TARGET,
            super::super::SUPERMICRO_BIOS_TARGET,
            ""
        ]
    );
    assert_eq!(
        plan.targets[2].firmware.expected_version.as_deref(),
        Some("70.02.01.05")
    );
    assert!(
        plan.targets[..2]
            .iter()
            .all(|t| t.firmware.expected_version.is_none())
    );
}

#[test]
fn supermicro_component_filters_and_required_bmc_version() {
    let mut manifest = supermicro_manifest();
    for (key, filenames) in [
        ("HMC", vec!["compute-nosbios.fwpkg", "test-bios-image.bin"]),
        ("BIOS", vec!["test-bios-image.bin"]),
        ("BMC", vec!["NvOBMC_test-image.bin"]),
    ] {
        let plan = plan_request(
            &manifest,
            NodeType::ComputeGb300Supermicro,
            "prod",
            &FirmwareObjectComponentSelection::components(vec![key.to_owned()]),
        )
        .unwrap();
        assert_eq!(
            plan.targets
                .iter()
                .map(|t| t.artifact.filename.as_str())
                .collect::<Vec<_>>(),
            filenames
        );
    }
    for version in [None, Some("  ".to_owned())] {
        manifest.milestones[0].board_skus[0].components.firmware[0].version = version;
        let error = plan_request(
            &manifest,
            NodeType::ComputeGb300Supermicro,
            "prod",
            &FirmwareObjectComponentSelection::default_only(),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("requires its manifest Version"));
    }
}

#[test]
fn supermicro_metadata_only_exposes_eligible_packages() {
    let metadata = super::super::metadata::from_manifest(&supermicro_manifest()).unwrap();
    let descriptor = super::super::domain_node_type_to_descriptor(NodeType::ComputeGb300Supermicro);
    let device = metadata
        .device_components
        .iter()
        .find(|d| d.node_descriptor.as_ref() == Some(&descriptor))
        .unwrap();
    assert_eq!(
        device.node_type,
        super::super::rm::NodeType::Unspecified as i32
    );
    let files: HashSet<_> = device
        .components
        .iter()
        .flat_map(|c| &c.artifacts)
        .map(|a| a.filename.as_str())
        .collect();
    assert_eq!(
        files,
        HashSet::from([
            "compute-nosbios.fwpkg",
            "test-bios-image.bin",
            "NvOBMC_test-image.bin"
        ])
    );
}

#[test]
fn powershelf_separator_aliases_preserve_vendor_filtering_and_order() {
    for (liteon, delta) in [("LiteOn ", "Delta-"), ("LiteOn-", "Delta ")] {
        let mut manifest = compute_manifest();
        let board = &mut manifest.milestones[0].board_skus[0];
        board.board_type = "Power Shelf".to_owned();
        board.components.firmware = [format!("{delta}PSU"), format!("{liteon}PSU"), format!("{delta}PMC"), format!("{liteon}PMC")]
            .into_iter().map(|name| serde_json::from_value(serde_json::json!({
                "Component": name, "Version": "1.0", "Type": "Prod",
                "Locations": [{"Location": format!("https://example.test/{}.tar", name.replace(' ', "-")),
                    "LocationType": "HTTPS", "Type": "Firmware"}]
            })).unwrap()).collect();
        for (node_type, prefix) in [
            (NodeType::PowershelfGb200Liteon, liteon),
            (NodeType::PowershelfGb300Liteon, liteon),
            (NodeType::PowershelfVrnvl72Liteon, liteon),
            (NodeType::PowershelfGb200Delta, delta),
            (NodeType::PowershelfGb300Delta, delta),
            (NodeType::PowershelfVrnvl72Delta, delta),
        ] {
            let selected = select_components(
                &manifest,
                node_type,
                "prod",
                &FirmwareObjectComponentSelection::all(),
            )
            .unwrap();
            assert_eq!(selected.len(), 2);
            assert!(
                selected
                    .iter()
                    .all(|c| c.component.component.starts_with(prefix))
            );
            let plan = plan_request(
                &manifest,
                node_type,
                "prod",
                &FirmwareObjectComponentSelection::default_only(),
            )
            .unwrap();
            assert!(plan.targets[0].artifact.filename.contains("PMC"));
            assert!(plan.targets[1].artifact.filename.contains("PSU"));
            for filter in [
                format!("{prefix}PMC"),
                format!("{}-PMC", prefix.trim_end_matches([' ', '-'])),
            ] {
                let plan = plan_request(
                    &manifest,
                    node_type,
                    "prod",
                    &FirmwareObjectComponentSelection::components(vec![filter]),
                )
                .unwrap();
                assert_eq!(plan.targets.len(), 1);
                assert!(plan.targets[0].artifact.filename.contains("PMC"));
            }
            assert_eq!(
                expectations(
                    &manifest,
                    node_type,
                    "prod",
                    &FirmwareObjectComponentSelection::all()
                )
                .unwrap()
                .len(),
                2
            );
        }
        let metadata = super::super::metadata::from_manifest(&manifest).unwrap();
        assert_eq!(metadata.device_components.len(), 6);
        for device in &metadata.device_components {
            assert_eq!(device.components.len(), 2);
        }
    }
}

#[test]
fn separator_matching_does_not_guess_names_or_collapse_payloads() {
    for wrong in [
        "Liteon-PMC",
        "LiteOn_PMC",
        "LiteOnPMC",
        "LiteOn-PMU",
        "Delta-PMC",
    ] {
        assert!(!firmware_source_component_matches(wrong, "LiteOn PMC"));
    }
    let mut manifest = compute_manifest();
    let board = &mut manifest.milestones[0].board_skus[0];
    board.board_type = "Power Shelf".to_owned();
    board.components.firmware = ["LiteOn PMC", "LiteOn-PMC"].into_iter().map(|name| {
        serde_json::from_value(serde_json::json!({"Component": name, "Type": "Prod",
            "Locations": [{"Location": "https://example.test/pmc.tar", "LocationType": "HTTPS", "Type": "Firmware"}]})).unwrap()
    }).collect();
    let plan = plan_request(
        &manifest,
        NodeType::PowershelfGb200Liteon,
        "prod",
        &FirmwareObjectComponentSelection::all(),
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 2);
    assert!(
        plan.targets
            .iter()
            .all(|target| target.artifact.filename == "pmc.tar")
    );
    assert_ne!(plan.targets[0].artifact.id, plan.targets[1].artifact.id);
}

#[test]
fn metadata_propagates_ambiguous_boards_instead_of_omitting_the_platform() {
    let mut manifest = compute_manifest();
    let duplicate = manifest.milestones[0].board_skus[0].clone();
    manifest.milestones[0].board_skus.push(duplicate);
    let error = super::super::metadata::from_manifest(&manifest).unwrap_err();
    assert!(error.contains("ambiguous"), "{error}");
}

#[test]
fn catalog_and_apply_ignore_metadata_only_firmware_locations() {
    let mut manifest = compute_manifest();
    let placeholder: manifest::Location = serde_json::from_value(serde_json::json!({
        "FileName": "hmc-placeholder.fwpkg", "Type": "Firmware",
        "Location": null, "LocationType": null
    }))
    .unwrap();
    manifest.milestones[0].board_skus[0].components.firmware[1]
        .locations
        .push(placeholder.clone());
    assert_eq!(catalog_artifacts(&manifest).unwrap().len(), 2);
    assert_eq!(
        plan_request(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "Prod",
            &FirmwareObjectComponentSelection::default_only()
        )
        .unwrap()
        .targets
        .len(),
        2
    );

    // A selected target with only metadata must not silently disappear.
    manifest.milestones[0].board_skus[0].components.firmware[1].locations = vec![placeholder];
    assert_eq!(catalog_artifacts(&manifest).unwrap().len(), 1);
    let error = plan_request(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "Prod",
        &FirmwareObjectComponentSelection::default_only(),
    )
    .err()
    .unwrap();
    assert!(
        error.to_string().contains("no eligible firmware payload"),
        "{error}"
    );
    assert!(error.to_string().contains("HMC"), "{error}");
}

#[test]
fn catalog_still_rejects_a_real_url_with_missing_transport() {
    let mut manifest = compute_manifest();
    manifest.milestones[0].board_skus[0].components.firmware[0].locations[0].location_type = None;
    assert!(
        catalog_artifacts(&manifest)
            .err()
            .unwrap()
            .to_string()
            .contains("LocationType")
    );
}

fn add_nvos_locations(manifest: &mut FirmwareManifest, locations: serde_json::Value) {
    manifest.milestones[0].board_skus.push(
        serde_json::from_value(serde_json::json!({
            "Type": "Switch Tray", "Components": {"Software": [{
                "Component": "NVOS", "Type": "Prod", "Locations": locations
            }]}
        }))
        .unwrap(),
    );
}

#[test]
fn nvos_placeholders_remain_metadata_but_do_not_participate_in_selection() {
    let mut manifest = compute_manifest();
    add_nvos_locations(
        &mut manifest,
        serde_json::json!([
            {"FileName": "release-notes.bin", "Location": null, "Type": "Binary", "Required": false},
            {"FileName": "nvos-amd64-25.02.4440.bin", "Location": "https://example.test/image",
             "LocationType": "https", "Type": "Binary"}
        ]),
    );
    assert_eq!(catalog_artifacts(&manifest).unwrap().len(), 3);
    assert_eq!(
        switch_request(&manifest, "Prod").unwrap().filename,
        "nvos-amd64-25.02.4440.bin"
    );
    let metadata = super::super::metadata::from_manifest(&manifest).unwrap();
    assert_eq!(metadata.switch_system_images.len(), 2);
    assert!(!metadata.switch_system_images[0].required);
}

#[test]
fn nvos_uniqueness_applies_only_to_execution_not_prefetch() {
    for images in [
        serde_json::json!([]),
        serde_json::json!([
            {"Location": "https://example.test/one.bin", "LocationType": "https"},
            {"Location": "https://example.test/two.bin", "LocationType": "https"}
        ]),
    ] {
        let mut manifest = compute_manifest();
        let count = images.as_array().unwrap().len();
        add_nvos_locations(&mut manifest, images);
        assert_eq!(catalog_artifacts(&manifest).unwrap().len(), 2 + count);
        assert!(switch_request(&manifest, "Prod").is_err());
        assert!(
            plan_request(
                &manifest,
                NodeType::ComputeGb200Nvidia,
                "Prod",
                &FirmwareObjectComponentSelection::default_only()
            )
            .is_ok()
        );
    }
}

#[test]
fn metadata_projects_equivalent_channel_spellings_once() {
    let mut manifest = compute_manifest();
    let expected = super::super::metadata::from_manifest(&manifest).unwrap();
    manifest.milestones[0].board_skus[0].components.firmware[0].channel = Some(" pRoD ".to_owned());
    assert_eq!(
        super::super::metadata::from_manifest(&manifest).unwrap(),
        expected
    );
}

#[test]
fn only_the_vr_liteon_pmc_target_carries_the_manifest_version() {
    let mut manifest = compute_manifest();
    let board = &mut manifest.milestones[0].board_skus[0];
    board.board_type = "Power Shelf".to_owned();
    board.components.firmware = ["LiteOn PSU", "LiteOn PMC"]
        .into_iter()
        .map(|name| {
            serde_json::from_value(serde_json::json!({
                "Component": name, "Version": "2.3.4", "Type": "Prod",
                "Locations": [{"Location": format!("https://example.test/{}.fwpkg", name.replace(' ', "-")),
                    "LocationType": "HTTPS", "Type": "Firmware"}]
            }))
            .unwrap()
        })
        .collect();
    for (node_type, pmc_version) in [
        (NodeType::PowershelfVrnvl72Liteon, Some("2.3.4")),
        (NodeType::PowershelfGb200Liteon, None),
        (NodeType::PowershelfGb300Liteon, None),
    ] {
        let plan = plan_request(
            &manifest,
            node_type,
            "prod",
            &FirmwareObjectComponentSelection::default_only(),
        )
        .unwrap();
        assert!(plan.targets[0].artifact.filename.contains("PMC"));
        assert_eq!(
            plan.targets[0].firmware.expected_version.as_deref(),
            pmc_version,
            "{node_type}"
        );
        assert!(plan.targets[1].artifact.filename.contains("PSU"));
        assert_eq!(plan.targets[1].firmware.expected_version, None);
    }
}

#[test]
fn vr_delta_powershelf_targets_do_not_carry_a_manifest_version() {
    let mut manifest = compute_manifest();
    let board = &mut manifest.milestones[0].board_skus[0];
    board.board_type = "Power Shelf".to_owned();
    board.components.firmware = ["Delta-PSU", "Delta-PMC"]
        .into_iter()
        .map(|name| {
            serde_json::from_value(serde_json::json!({
                "Component": name, "Version": "2.3.4", "Type": "Prod",
                "Locations": [{"Location": format!("https://example.test/{name}.fwpkg"),
                    "LocationType": "HTTPS", "Type": "Firmware"}]
            }))
            .unwrap()
        })
        .collect();

    let plan = plan_request(
        &manifest,
        NodeType::PowershelfVrnvl72Delta,
        "prod",
        &FirmwareObjectComponentSelection::default_only(),
    )
    .unwrap();

    // Only the VR LiteOn PMC target opts into manifest-version verification.
    assert!(plan.targets[0].artifact.filename.contains("PMC"));
    assert!(plan.targets[1].artifact.filename.contains("PSU"));
    for target in &plan.targets {
        assert_eq!(target.firmware.expected_version, None);
    }
}

#[test]
fn compute_board_powershelf_compatibility_keeps_vendor_targets_separate() {
    let mut manifest = compute_manifest();
    for name in ["Delta-PMC", "LiteOn PMC"] {
        manifest.milestones[0].board_skus[0].components.firmware.push(
            serde_json::from_value(serde_json::json!({
                "Component": name, "Type": "Prod", "Locations": [{
                    "Location": "https://example.test/pmc.tar", "Type": "Firmware", "LocationType": "https"
                }]
            })).unwrap());
    }
    for (node_type, expected) in [
        (NodeType::PowershelfGb200Delta, "Delta-PMC"),
        (NodeType::PowershelfGb300Delta, "Delta-PMC"),
        (NodeType::PowershelfVrnvl72Delta, "Delta-PMC"),
        (NodeType::PowershelfGb200Liteon, "LiteOn PMC"),
        (NodeType::PowershelfGb300Liteon, "LiteOn PMC"),
    ] {
        let selected = select_components(
            &manifest,
            node_type,
            "Prod",
            &FirmwareObjectComponentSelection::default_only(),
        )
        .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].component.component, expected);
    }
    assert_eq!(
        plan_request(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "Prod",
            &FirmwareObjectComponentSelection::default_only()
        )
        .unwrap()
        .targets
        .len(),
        2
    );
}

#[test]
fn omitted_request_type_uses_the_manifests_only_channel() {
    for channel in ["Prod", "Dev", "Engineering"] {
        let mut manifest = compute_manifest();
        for component in &mut manifest.milestones[0].board_skus[0].components.firmware {
            component.channel = Some(channel.to_owned());
        }
        for requested in ["", "  ", channel] {
            assert_eq!(
                resolve_channel(&manifest, requested).unwrap(),
                channel.to_ascii_lowercase()
            );
            let plan = plan_request(
                &manifest,
                NodeType::ComputeGb200Nvidia,
                requested,
                &FirmwareObjectComponentSelection::default_only(),
            )
            .unwrap();
            assert_eq!(plan.targets.len(), 2);
            assert_eq!(plan.targets[0].artifact.filename, "hmc.fwpkg");
            assert_eq!(plan.targets[1].artifact.filename, "bmc.fwpkg");
            assert_eq!(
                expectations(
                    &manifest,
                    NodeType::ComputeGb200Nvidia,
                    &resolve_channel(&manifest, requested).unwrap(),
                    &FirmwareObjectComponentSelection::default_only()
                )
                .unwrap(),
                expectations(
                    &manifest,
                    NodeType::ComputeGb200Nvidia,
                    channel,
                    &FirmwareObjectComponentSelection::default_only()
                )
                .unwrap(),
            );
        }
    }
}

#[test]
fn channel_inference_normalizes_case_and_whitespace() {
    let mut manifest = compute_manifest();
    manifest.milestones[0].board_skus[0].components.firmware[0].channel = Some(" pRoD ".to_owned());
    assert_eq!(resolve_channel(&manifest, "").unwrap(), "prod");
    assert_eq!(resolve_channel(&manifest, " PROD ").unwrap(), "prod");
    assert_eq!(
        plan_request(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "",
            &FirmwareObjectComponentSelection::default_only()
        )
        .unwrap()
        .targets
        .len(),
        2
    );
}

#[test]
fn mixed_channels_require_an_explicit_request_even_with_a_component_filter() {
    let mut manifest = compute_manifest();
    manifest.milestones[0].board_skus[0].components.firmware[1].channel = Some("Dev".to_owned());
    let selection = FirmwareObjectComponentSelection::components(vec!["BMC".to_owned()]);
    let error = plan_request(&manifest, NodeType::ComputeGb200Nvidia, "", &selection)
        .err()
        .expect("a filter must not implicitly choose a channel");
    assert!(
        error
            .to_string()
            .contains("multiple firmware types: dev, prod"),
        "{error}"
    );
    assert!(error.to_string().contains("explicitly"), "{error}");
    let plan = plan_request(&manifest, NodeType::ComputeGb200Nvidia, "Prod", &selection).unwrap();
    assert_eq!(plan.targets[0].artifact.filename, "bmc.fwpkg");
    let dev = plan_request(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "Dev",
        &FirmwareObjectComponentSelection::default_only(),
    )
    .unwrap();
    assert_eq!(dev.targets.len(), 1);
    assert_eq!(dev.targets[0].artifact.filename, "hmc.fwpkg");
}

#[test]
fn inference_considers_software_on_other_boards_but_not_artifact_kinds() {
    let mut manifest = compute_manifest();
    // Locations.Type is "Firmware", not the component's channel "Prod".
    assert_eq!(resolve_channel(&manifest, "").unwrap(), "prod");
    let switch = serde_json::from_value(serde_json::json!({
        "Type": "Switch Tray", "Components": {"Software": [{
            "Component": "NVOS", "Type": "Dev", "Locations": [{
                "Location": "https://example.test/nvos-amd64-25.02.4440.bin",
                "LocationType": "https", "Type": "Binary"
            }]
        }]}
    }))
    .unwrap();
    manifest.milestones[0].board_skus.push(switch);
    assert!(resolve_channel(&manifest, "").is_err());
    assert!(switch_request(&manifest, "").is_err());
    assert_eq!(
        switch_request(&manifest, "Dev").unwrap().filename,
        "nvos-amd64-25.02.4440.bin"
    );
    manifest.milestones[0].board_skus[1].components.software[0].channel = Some("Prod".to_owned());
    assert_eq!(
        switch_request(&manifest, "").unwrap().filename,
        "nvos-amd64-25.02.4440.bin"
    );
}

#[test]
fn channel_inference_never_guesses_prod_or_overrides_an_explicit_request() {
    let mut manifest = compute_manifest();
    assert!(
        plan_request(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "Dev",
            &FirmwareObjectComponentSelection::default_only()
        )
        .is_err()
    );
    for component in &mut manifest.milestones[0].board_skus[0].components.firmware {
        component.channel = None;
    }
    assert!(
        resolve_channel(&manifest, "")
            .unwrap_err()
            .to_string()
            .contains("no component channels")
    );
    assert!(
        plan_request(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "Prod",
            &FirmwareObjectComponentSelection::default_only()
        )
        .is_err()
    );
}

#[test]
fn sanitized_sample_has_an_unambiguous_production_default() {
    let json = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/gb200_1.3.6_sanitized.json"
    ));
    let manifest = json.parse::<FirmwareManifest>().unwrap();
    assert_eq!(resolve_channel(&manifest, "").unwrap(), "prod");
}

#[test]
fn default_apply_rejects_missing_channel_before_planning_a_partial_update() {
    for channel in [None, Some(String::new()), Some("  ".to_owned())] {
        let mut manifest = compute_manifest();
        manifest.milestones[0].board_skus[0].components.firmware[1].channel = channel;
        // Importing bytes is not evidence that this manifest can be applied.
        assert_eq!(catalog_artifacts(&manifest).unwrap().len(), 2);
        for requested in ["Prod", ""] {
            let error = plan_request(
                &manifest,
                NodeType::ComputeGb200Nvidia,
                requested,
                &FirmwareObjectComponentSelection::default_only(),
            )
            .err()
            .expect("missing channel must reject the whole plan");
            assert!(
                error
                    .to_string()
                    .contains("BoardSKUs[0].Components.Firmware[1].Type"),
                "{error}"
            );
            assert!(error.to_string().contains("HMC"), "{error}");
        }
    }
}

#[test]
fn explicit_and_all_selection_reject_missing_selected_channel() {
    let mut manifest = compute_manifest();
    manifest.milestones[0].board_skus[0].components.firmware[1].channel = None;
    for selection in [
        FirmwareObjectComponentSelection::all(),
        FirmwareObjectComponentSelection::components(vec!["BMC".to_owned(), "HMC".to_owned()]),
    ] {
        let error = plan_request(&manifest, NodeType::ComputeGb200Nvidia, "Prod", &selection)
            .err()
            .expect("selected HMC requires a channel");
        assert!(error.to_string().contains(".Type"), "{error}");
    }
}

#[test]
fn explicit_bmc_selection_does_not_require_an_unselected_hmc_channel() {
    let mut manifest = compute_manifest();
    manifest.milestones[0].board_skus[0].components.firmware[1].channel = None;
    let plan = plan_request(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "Prod",
        &FirmwareObjectComponentSelection::components(vec!["BMC".to_owned()]),
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 1);
    assert_eq!(plan.targets[0].artifact.filename, "bmc.fwpkg");
}

#[test]
fn comparison_rejects_missing_selected_channel_without_requiring_downloads() {
    let mut manifest = compute_manifest();
    let firmware = &mut manifest.milestones[0].board_skus[0].components.firmware;
    for component in firmware.iter_mut() {
        component.locations.clear();
    }
    firmware[1].channel = None;
    let error = expectations(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "Prod",
        &FirmwareObjectComponentSelection::default_only(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("Firmware[1].Type"), "{error}");
}

#[test]
fn default_apply_preserves_hmc_before_bmc_and_filters_other_channels() {
    let mut manifest = compute_manifest();
    let mut dev = manifest.milestones[0].board_skus[0].components.firmware[1].clone();
    dev.channel = Some("Dev".to_owned());
    manifest.milestones[0].board_skus[0]
        .components
        .firmware
        .push(dev);
    let plan = plan_request(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "pRoD",
        &FirmwareObjectComponentSelection::default_only(),
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 2);
    assert_eq!(plan.targets[0].artifact.filename, "hmc.fwpkg");
    assert_eq!(plan.targets[1].artifact.filename, "bmc.fwpkg");
}

#[test]
fn missing_channel_on_an_unmapped_component_does_not_block_apply() {
    let mut manifest = compute_manifest();
    let mut unrelated = manifest.milestones[0].board_skus[0].components.firmware[1].clone();
    unrelated.component = "UnrelatedDevice".to_owned();
    unrelated.channel = None;
    manifest.milestones[0].board_skus[0]
        .components
        .firmware
        .push(unrelated);
    assert!(
        plan_request(
            &manifest,
            NodeType::ComputeGb200Nvidia,
            "Prod",
            &FirmwareObjectComponentSelection::default_only()
        )
        .is_ok()
    );
}

#[test]
fn vr_compatibility_defaults_still_supply_missing_channels() {
    let mut manifest = compute_manifest();
    manifest.product_name = "VR-NVL72".to_owned();
    for component in &mut manifest.milestones[0].board_skus[0].components.firmware {
        component.channel = None;
    }
    let plan = plan_request(
        &manifest,
        NodeType::ComputeVrnvl72Nvidia,
        "",
        &FirmwareObjectComponentSelection::default_only(),
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 2);
}

fn with_hmc_location(
    mut manifest: FirmwareManifest,
    location: serde_json::Value,
) -> FirmwareManifest {
    manifest.milestones[0].board_skus[0].components.firmware[1]
        .locations
        .push(serde_json::from_value(location).unwrap());
    manifest
}

#[test]
fn recovery_payload_selector_survives_every_observed_mislabeling() {
    // Correctly typed (1_3_6-GA.json).
    assert!(location_is_recovery_payload(
        Some("Firmware-Recovery"),
        "nvfw_GB200-P4975_recovery_prod-signed.fwpkg"
    ));
    // Typed empty, the name carries the infix (GB200NVL 1.3.10 JULY).
    assert!(location_is_recovery_payload(
        None,
        "nvfw_GB200-P4975_recovery_prod-signed.fwpkg"
    ));
    // Typed Firmware on a recovery name: the type lies, the name decides.
    assert!(location_is_recovery_payload(
        Some("Firmware"),
        "nvfw_GB200-P4975_recovery_prod-signed.fwpkg"
    ));
    // The CoRIM typed Firmware-Recovery (GB200CX8NVL 1.3.7) is not a package.
    assert!(!location_is_recovery_payload(
        Some("Firmware-Recovery"),
        "GB200-P4975_prod-signed.corim"
    ));
    // A regular package stays regular.
    assert!(!location_is_recovery_payload(
        Some("Firmware"),
        "nvfw_GB200-P4975_prod-signed.fwpkg"
    ));
}

#[test]
fn recovery_packages_are_prefetched_but_never_planned() {
    // The recovery package's Type lies: routing goes by the name, before the
    // payload filter, so the flash plan never sees it.
    let manifest = with_hmc_location(
        compute_manifest(),
        serde_json::json!({"Location": "https://example.test/hmc_recovery.fwpkg",
            "LocationType": "https", "Type": "Firmware"}),
    );
    let prefetched = catalog_artifacts(&manifest).unwrap();
    let names: BTreeSet<_> = prefetched.iter().map(|a| a.filename.as_str()).collect();
    assert_eq!(
        names,
        BTreeSet::from(["bmc.fwpkg", "hmc.fwpkg", "hmc_recovery.fwpkg"])
    );
    let plan = plan_request(
        &manifest,
        NodeType::ComputeGb200Nvidia,
        "",
        &FirmwareObjectComponentSelection::default_only(),
    )
    .unwrap();
    let planned: Vec<_> = plan
        .targets
        .iter()
        .map(|target| target.artifact.filename.as_str())
        .collect();
    assert_eq!(planned, ["hmc.fwpkg", "bmc.fwpkg"]);

    let recovery = recovery_artifacts(&manifest, NodeType::ComputeGb200Nvidia)
        .unwrap()
        .unwrap();
    assert_eq!(recovery.regular.filename, "hmc.fwpkg");
    assert_eq!(recovery.recovery.filename, "hmc_recovery.fwpkg");
    // The same identities the prefetch stored, so the cache lookup lines up.
    let ids: BTreeSet<_> = prefetched.iter().map(|a| a.id.as_str()).collect();
    assert!(ids.contains(recovery.regular.id.as_str()));
    assert!(ids.contains(recovery.recovery.id.as_str()));
}

#[test]
fn recovery_artifacts_prefer_the_production_hmc_package() {
    let mut manifest = with_hmc_location(
        compute_manifest(),
        serde_json::json!({"Location": "https://example.test/hmc_recovery.fwpkg",
            "LocationType": "https", "Type": "Firmware-Recovery"}),
    );
    let dev: manifest::Component = serde_json::from_value(serde_json::json!({
        "Component": "HMC", "Type": "Dev", "Version": "2.1", "Locations": [
            {"Location": "https://example.test/dev/hmc-dev.fwpkg",
             "LocationType": "https", "Type": "Firmware"},
            {"Location": "https://example.test/dev/hmc-dev_recovery.fwpkg",
             "LocationType": "https", "Type": "Firmware-Recovery"}]
    }))
    .unwrap();
    manifest.milestones[0].board_skus[0]
        .components
        .firmware
        .push(dev);

    let recovery = recovery_artifacts(&manifest, NodeType::ComputeGb200Nvidia)
        .unwrap()
        .unwrap();
    assert_eq!(recovery.regular.filename, "hmc.fwpkg");
    assert_eq!(recovery.recovery.filename, "hmc_recovery.fwpkg");
}

#[test]
fn recovery_selection_ignores_components_outside_the_hmc() {
    // A CX7 entry with no channel would fail a full selection ("Type is
    // required"). Recovery reads the HMC only, so the entry is never asked.
    let mut manifest = with_hmc_location(
        compute_manifest(),
        serde_json::json!({"Location": "https://example.test/hmc_recovery.fwpkg",
            "LocationType": "https", "Type": "Firmware-Recovery"}),
    );
    let untyped: manifest::Component = serde_json::from_value(serde_json::json!({
        "Component": "CX7", "Type": "", "Version": "9.9", "Locations": [
            {"Location": "https://example.test/cx7.bin", "LocationType": "https",
             "Type": "Firmware"}]
    }))
    .unwrap();
    manifest.milestones[0].board_skus[0]
        .components
        .firmware
        .push(untyped);

    let recovery = recovery_artifacts(&manifest, NodeType::ComputeGb200Nvidia)
        .unwrap()
        .unwrap();
    assert_eq!(recovery.regular.filename, "hmc.fwpkg");
    assert_eq!(recovery.recovery.filename, "hmc_recovery.fwpkg");
}

#[test]
fn manifest_without_a_recovery_package_yields_no_recovery_artifacts() {
    let manifest = compute_manifest();
    assert!(
        recovery_artifacts(&manifest, NodeType::ComputeGb200Nvidia)
            .unwrap()
            .is_none()
    );
    // A platform without an HMC component: none, not an error.
    assert!(
        recovery_artifacts(&manifest, NodeType::SwitchGb200Nvidia)
            .unwrap()
            .is_none()
    );
}

#[test]
fn supermicro_recovery_regular_bundle_is_the_hmc_package_not_the_bios() {
    // Supermicro manifests place BIOS payloads under the same HMC source
    // component. The HMC target's filename rule picks the regular bundle.
    let manifest = with_hmc_location(
        supermicro_manifest(),
        serde_json::json!({"Location": "https://example.test/compute_recovery.fwpkg",
            "FileName": "compute_recovery.fwpkg", "LocationType": "HTTPS",
            "Type": "Firmware-Recovery"}),
    );
    let recovery = recovery_artifacts(&manifest, NodeType::ComputeGb300Supermicro)
        .unwrap()
        .unwrap();
    assert_eq!(recovery.regular.filename, "compute-nosbios.fwpkg");
    assert_eq!(recovery.recovery.filename, "compute_recovery.fwpkg");
}
