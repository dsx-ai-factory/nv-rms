// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use serde_json::json;

use super::*;

const SAMPLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/gb200_1.3.6_sanitized.json"
));

fn fixture() -> Value {
    json!({
        "ProductName": "TestProduct",
        "Milestones": [{
            "Name": "1.0",
            "BoardSKUs": [{
                "Name": "compute-a",
                "SKUID": "sku-a",
                "Type": "Compute Node",
                "Components": {
                    "Firmware": [{
                        "Component": "HMC",
                        "Type": "Prod",
                        "Version": "",
                        "FileNames": ["hmc.fwpkg"],
                        "Locations": [{
                            "FileName": "hmc.fwpkg",
                            "Location": null,
                            "Type": "Firmware"
                        }],
                        "SubComponents": [{"Component": "GPU_0", "Version": "1.2"}]
                    }]
                }
            }]
        }]
    })
}

#[test]
fn parses_the_checked_in_sanitized_manifest_without_network_access() {
    let manifest: FirmwareManifest = SAMPLE.parse().unwrap();
    let boards = &manifest.single_milestone().unwrap().board_skus;
    assert_eq!(manifest.product_name, "DGXGB200");
    assert_eq!(boards.len(), 2);
    assert!(boards.iter().all(|board| board.sku_id.is_none()));
    assert_eq!(boards[0].components.software[0].component, "NVOS");
    assert_eq!(boards[1].components.software.len(), 5);
    assert_eq!(boards[1].components.firmware.len(), 6);
    let hmc = &boards[1].components.firmware[4];
    assert_eq!(hmc.component, "HMC");
    assert_eq!(hmc.version.as_deref(), Some(""));
    assert_eq!(hmc.subcomponents.len(), 7);
    assert_eq!(hmc.locations.len(), 5);
    assert_eq!(hmc.file_names.len(), 5);
    assert!(
        hmc.locations
            .iter()
            .any(|location| location.location.is_none())
    );
    assert!(
        hmc.locations
            .iter()
            .any(|location| { location.artifact_type.as_deref() == Some("Certificate") })
    );
}

#[test]
fn parser_preserves_all_locations_for_each_component() {
    let mut input = fixture();
    let locations: Vec<_> = (0..64)
        .map(|index| {
            json!({
                "FileName": format!("payload-{index}.bin"),
                "Location": format!("https://example.test/payload-{index}.bin"),
                "LocationType": "HTTPS", "Type": "Firmware"
            })
        })
        .collect();
    input["Milestones"][0]["BoardSKUs"][0]["Components"]["Firmware"] = json!(
        ["HMC", "BMC", "BIOS", "CPLD", "CX7", "LiteOn PMC"]
            .into_iter()
            .map(|component| json!({
                "Component": component, "Type": "Prod", "Locations": locations
            }))
            .collect::<Vec<_>>()
    );

    let manifest = FirmwareManifest::try_from(&input).unwrap();

    for component in &manifest.milestones[0].board_skus[0].components.firmware {
        assert_eq!(component.locations.len(), 64, "{}", component.component);
        for (index, location) in component.locations.iter().enumerate() {
            assert_eq!(location.file_name, Some(format!("payload-{index}.bin")));
        }
    }
}

#[test]
fn metadata_does_not_require_downloadable_files_or_invent_a_channel() {
    let mut input = fixture();
    let component = &mut input["Milestones"][0]["BoardSKUs"][0]["Components"]["Firmware"][0];
    component.as_object_mut().unwrap().remove("Type");
    let manifest = FirmwareManifest::try_from(&input).unwrap();
    let component = &manifest.milestones[0].board_skus[0].components.firmware[0];
    assert!(component.channel.is_none());
    assert!(component.locations[0].location.is_none());
    assert_eq!(
        component.locations[0].file_name.as_deref(),
        Some("hmc.fwpkg")
    );
    assert_eq!(component.subcomponents[0].version, "1.2");
}

#[test]
fn board_identity_and_repeated_component_names_are_not_collapsed() {
    let mut input = fixture();
    let mut board_b = input["Milestones"][0]["BoardSKUs"][0].clone();
    board_b["SKUID"] = json!("sku-b");
    board_b["Name"] = json!("compute-b");
    board_b["Components"]["Firmware"][0]["Version"] = json!("2.0");
    input["Milestones"][0]["BoardSKUs"]
        .as_array_mut()
        .unwrap()
        .push(board_b);
    let manifest = FirmwareManifest::try_from(&input).unwrap();
    let boards = &manifest.milestones[0].board_skus;
    assert_eq!(boards.len(), 2);
    assert_eq!(boards[0].sku_id.as_deref(), Some("sku-a"));
    assert_eq!(boards[1].sku_id.as_deref(), Some("sku-b"));
    assert_eq!(
        boards[0].components.firmware[0].version.as_deref(),
        Some("")
    );
    assert_eq!(
        boards[1].components.firmware[0].version.as_deref(),
        Some("2.0")
    );
}

#[test]
fn wrong_field_types_fail_with_the_full_json_path() {
    let cases = [
        ("/Milestones/0/BoardSKUs/0/Components/Firmware", json!({})),
        (
            "/Milestones/0/BoardSKUs/0/Components/Software",
            json!("not an array"),
        ),
        (
            "/Milestones/0/BoardSKUs/0/Components/Firmware/0/Locations",
            Value::Null,
        ),
        (
            "/Milestones/0/BoardSKUs/0/Components/Firmware/0/SubComponents",
            json!(false),
        ),
        (
            "/Milestones/0/BoardSKUs/0/Components/Firmware/0/Version",
            json!(2),
        ),
        (
            "/Milestones/0/BoardSKUs/0/Components/Firmware/0/Locations/0/FileName",
            json!(42),
        ),
        (
            "/Milestones/0/BoardSKUs/0/Components/Firmware/0/Locations/0/Sha256",
            json!(123),
        ),
        (
            "/Milestones/0/BoardSKUs/0/Components/Firmware/0/Locations/0/Required",
            json!("false"),
        ),
    ];
    for (pointer, value) in cases {
        let mut input = fixture();
        let (parent, field) = pointer.rsplit_once('/').unwrap();
        input.pointer_mut(parent).unwrap()[field] = value;
        let error = FirmwareManifest::try_from(&input).unwrap_err().to_string();
        assert!(
            error.contains("Milestones[0].BoardSKUs[0].Components"),
            "{error}"
        );
        assert!(error.contains(field), "{pointer}: {error}");
    }
}

#[test]
fn checksums_are_validated_before_payload_selection() {
    for digest in [
        json!(""),
        json!("not-a-digest"),
        json!("g".repeat(64)),
        json!(17),
    ] {
        let mut input = fixture();
        let location = &mut input["Milestones"][0]["BoardSKUs"][0]["Components"]["Firmware"][0]["Locations"]
            [0];
        location["Type"] = json!("Certificate");
        location["Sha256"] = digest;
        let error = FirmwareManifest::try_from(&input).unwrap_err().to_string();
        assert!(error.contains("Locations[0].Sha256"), "{error}");
    }
    let mut input = fixture();
    input["Milestones"][0]["BoardSKUs"][0]["Components"]["Firmware"][0]["Locations"][0]["Sha256"] =
        json!(format!(" {} ", "AB".repeat(32)));
    let manifest = FirmwareManifest::try_from(&input).unwrap();
    assert_eq!(
        manifest.milestones[0].board_skus[0].components.firmware[0].locations[0].sha256,
        Some("ab".repeat(32))
    );
}

#[test]
fn accepts_additional_informational_fields_without_format_guessing() {
    let mut input = fixture();
    input["Description"] = json!("future metadata");
    assert!(FirmwareManifest::try_from(&input).is_ok());
    assert!(FirmwareManifest::try_from(&json!({"Id": "legacy", "BoardSKUs": []})).is_err());
}

#[test]
fn raw_json_rejects_duplicate_fields_and_trailing_documents() {
    let json = fixture().to_string();
    let duplicate = json.replacen(
        "\"ProductName\":",
        "\"ProductName\":\"other\",\"ProductName\":",
        1,
    );
    assert!(
        duplicate
            .parse::<FirmwareManifest>()
            .unwrap_err()
            .to_string()
            .contains("duplicate field")
    );
    assert!(format!("{json} {{}}").parse::<FirmwareManifest>().is_err());
}

#[test]
fn required_identity_fields_cannot_be_empty_or_malformed() {
    for field in ["ProductName", "Milestones"] {
        let mut input = fixture();
        input.as_object_mut().unwrap().remove(field);
        assert!(
            FirmwareManifest::try_from(&input)
                .unwrap_err()
                .to_string()
                .contains(field)
        );
    }
    let mut input = fixture();
    input["Milestones"][0]["BoardSKUs"][0]["Components"]["Firmware"][0]["Component"] = json!("  ");
    assert!(FirmwareManifest::try_from(&input).is_err());
}

#[test]
fn the_object_contract_requires_exactly_one_release() {
    for count in [0, 2] {
        let mut input = fixture();
        input["Milestones"] = json!(vec![input["Milestones"][0].clone(); count]);
        assert!(
            matches!(FirmwareManifest::try_from(&input), Err(ManifestError::MilestoneCount(n)) if n == count)
        );
    }
}

#[test]
fn typed_round_trip_preserves_all_supported_metadata() {
    let parsed: FirmwareManifest = SAMPLE.parse().unwrap();
    let serialized = serde_json::to_string(&parsed).unwrap();
    let reparsed: FirmwareManifest = serialized.parse().unwrap();
    assert_eq!(parsed, reparsed);
}
