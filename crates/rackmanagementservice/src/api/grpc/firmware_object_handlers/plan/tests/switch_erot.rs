// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn split_manifest() -> FirmwareManifest {
    serde_json::from_value(serde_json::json!({
        "ProductName": "GB200", "Milestones": [{"Name": "release", "BoardSKUs": [{
            "Type": "Switch Tray", "Components": {"Firmware": [
                {"Component": "BMC+FPGA+EROT", "Type": "Prod", "Locations": [{
                    "Location": "https://example.test/alpha.fwpkg", "LocationType": "HTTPS", "Type": "Firmware"
                }], "SubComponents": [
                    {"Component": "BMC", "Version": "88.1"},
                    {"Component": "SMR", "Version": "0.24"},
                    {"Component": "ERoT", "Version": "2.0"}
                ]},
                {"Component": "SBIOS+EROT", "Type": "Prod", "Locations": [{
                    "Location": "https://example.test/beta.fwpkg", "LocationType": "HTTPS", "Type": "Firmware"
                }], "SubComponents": [
                    {"Component": "SBIOS", "Version": "1.0"},
                    {"Component": "ERoT", "Version": "1.5"}
                ]}
            ]}
        }]}]
    }))
    .unwrap()
}

fn erot_only() -> FirmwareObjectComponentSelection {
    FirmwareObjectComponentSelection::components(vec!["EROT".to_owned()])
}

#[test]
fn preferred_source_is_independent_of_order_filename_and_version() {
    for node_type in [NodeType::SwitchGb200Nvidia, NodeType::SwitchGb300Nvidia] {
        for reverse in [false, true] {
            let mut manifest = split_manifest();
            if reverse {
                manifest.milestones[0].board_skus[0]
                    .components
                    .firmware
                    .reverse();
            }
            let selection = FirmwareObjectComponentSelection::default_only();
            let plan = plan_request(&manifest, node_type, "prod", &selection).unwrap();
            assert_eq!(
                plan.targets
                    .iter()
                    .map(|target| (target.transport_target(), target.artifact.filename.as_str()))
                    .collect::<Vec<_>>(),
                [
                    ("bmc", "alpha.fwpkg"),
                    ("fpga", "alpha.fwpkg"),
                    ("erot", "beta.fwpkg"),
                    ("bios", "beta.fwpkg")
                ]
            );
            let expected = expectations(&manifest, node_type, "prod", &selection).unwrap();
            assert_eq!(
                expected
                    .iter()
                    .find(|item| item.target == "erot")
                    .unwrap()
                    .version,
                "1.5"
            );
            let metadata = super::super::super::metadata::from_manifest(&manifest).unwrap();
            for device in metadata.device_components {
                let erot = device
                    .components
                    .iter()
                    .find(|component| component.name == "EROT")
                    .unwrap();
                assert_eq!(erot.source_component, "SBIOS+EROT");
                assert_eq!(erot.artifacts.len(), 1);
                assert_eq!(erot.artifacts[0].filename, "beta.fwpkg");
            }
        }
    }
}

#[test]
fn erot_only_plan_downloads_only_the_preferred_bundle() {
    let manifest = split_manifest();
    for node_type in [NodeType::SwitchGb200Nvidia, NodeType::SwitchGb300Nvidia] {
        let plan = plan_request(&manifest, node_type, "prod", &erot_only()).unwrap();
        assert_eq!(plan.targets.len(), 1);
        assert_eq!(plan.targets[0].transport_target(), "erot");
        assert_eq!(plan.targets[0].artifact.filename, "beta.fwpkg");
    }
}

#[test]
fn falls_back_without_an_eligible_same_channel_switch_bios_bundle() {
    for case in [
        "absent",
        "other-channel",
        "attestation-only",
        "other-board-role",
        "no-url",
    ] {
        let mut manifest = split_manifest();
        let boards = &mut manifest.milestones[0].board_skus;
        match case {
            "absent" => {
                boards[0].components.firmware.pop();
            }
            "other-channel" => boards[0].components.firmware[1].channel = Some("Dev".to_owned()),
            "attestation-only" => {
                boards[0].components.firmware[1].locations[0].artifact_type =
                    Some("Certificate".to_owned())
            }
            "other-board-role" => {
                let bios = boards[0].components.firmware.pop().unwrap();
                let mut other = boards[0].clone();
                other.board_type = "Compute Tray".to_owned();
                other.components.firmware = vec![bios];
                boards.push(other);
            }
            "no-url" => {
                let location = &mut boards[0].components.firmware[1].locations[0];
                location.file_name = Some("beta.fwpkg".to_owned());
                location.location = None;
            }
            _ => unreachable!(),
        }
        for node_type in [NodeType::SwitchGb200Nvidia, NodeType::SwitchGb300Nvidia] {
            let plan = plan_request(&manifest, node_type, "prod", &erot_only()).unwrap();
            assert_eq!(plan.targets.len(), 1, "{case}");
            assert_eq!(plan.targets[0].artifact.filename, "alpha.fwpkg", "{case}");
            let expected = expectations(&manifest, node_type, "prod", &erot_only()).unwrap();
            assert_eq!(expected[0].version, "2.0", "{case}");
        }
    }
}

#[test]
fn missing_preferred_channel_is_rejected_instead_of_silently_falling_back() {
    let mut manifest = split_manifest();
    manifest.milestones[0].board_skus[0].components.firmware[1].channel = None;
    let error = plan_request(&manifest, NodeType::SwitchGb200Nvidia, "prod", &erot_only())
        .err()
        .unwrap();
    assert!(error.to_string().contains(".Type is required"));
}

#[test]
fn preferred_source_does_not_hide_ambiguous_boards_or_change_vr_targets() {
    let mut manifest = split_manifest();
    let mut duplicate = manifest.milestones[0].board_skus[0].clone();
    duplicate.components.firmware.remove(0);
    manifest.milestones[0].board_skus.push(duplicate);
    assert!(
        plan_request(&manifest, NodeType::SwitchGb200Nvidia, "prod", &erot_only())
            .err()
            .unwrap()
            .to_string()
            .contains("ambiguous")
    );

    let mut manifest = split_manifest();
    let mut vr = manifest.milestones[0].board_skus[0].components.firmware[1].clone();
    vr.component = "BMC+CPLD+SMA+ERoT+SBIOS".to_owned();
    vr.locations[0].location = Some("https://example.test/vr.fwpkg".to_owned());
    manifest.milestones[0].board_skus[0]
        .components
        .firmware
        .push(vr);
    let plan = plan_request(
        &manifest,
        NodeType::SwitchVrnvl72Nvidia,
        "prod",
        &erot_only(),
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 1);
    assert_eq!(plan.targets[0].artifact.filename, "vr.fwpkg");
}
