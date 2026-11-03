// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::super::{
    HardwareProfileCatalog, HardwareProfileDocument, HardwareUpdatePlan, serde_yaml,
};
use super::*;

const GB200: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
));

fn document() -> HardwareProfileDocument {
    let mut document: HardwareProfileDocument = serde_yaml::from_str(GB200).unwrap();
    document.firmware.targets.truncate(2);
    document.firmware.targets[0].id = "standard_hmc".to_owned();
    document.firmware.targets[1].id = "custom_hmc".to_owned();
    document
}

fn assert_conflict(document: HardwareProfileDocument, selector: &str) -> String {
    let error = HardwareUpdatePlan::compile(document, String::new())
        .unwrap_err()
        .to_string();
    for expected in [
        "compute_gb200_nvidia",
        "standard_hmc",
        "custom_hmc",
        selector,
    ] {
        assert!(error.contains(expected), "{error}");
    }
    error
}

#[test]
fn duplicate_component_keys_reject_the_example_regardless_of_stage_or_yaml_order() {
    let mut first = document();
    first.firmware.targets[1].component_key = "HMC".to_owned();
    first.firmware.targets[1].request_targets = vec!["/redfish/v1/Chassis/HGX_Custom_0".to_owned()];
    let error = assert_conflict(first, "component_key \"hmc\"");

    let mut reversed = document();
    reversed.firmware.targets[1].component_key = "HMC".to_owned();
    reversed.firmware.targets[0].stage = 30;
    reversed.firmware.targets.reverse();
    assert_eq!(assert_conflict(reversed, "component_key \"hmc\""), error);
}

#[test]
fn component_keys_are_unique_even_with_disjoint_filenames_or_disabled_matching() {
    let mut document = document();
    document.matches.enabled = false;
    document.firmware.targets[1].component_key = "hmc".to_owned();
    document.firmware.targets[1].default_apply = false;
    document.firmware.targets[0].filename.suffix = Some(".bin".to_owned());
    document.firmware.targets[1].filename.suffix = Some(".fwpkg".to_owned());
    assert_conflict(document, "component keys must be unique");
}

#[test]
fn component_keys_report_every_conflicting_target() {
    let mut document = document();
    document.firmware.targets[1].component_key = "hmc".to_owned();
    let mut third: HardwareProfileDocument = serde_yaml::from_str(GB200).unwrap();
    let mut target = third.firmware.targets.remove(2);
    target.id = "third_target".to_owned();
    target.component_key = "HMC".to_owned();
    document.firmware.targets.push(target);
    let error = assert_conflict(document, "component keys must be unique");
    assert!(
        error.contains("[custom_hmc, standard_hmc, third_target]"),
        "{error}"
    );
}

#[test]
fn component_selection_uses_configured_keys() {
    let mut document = document();
    document.firmware.targets[0].component_key = "PRIMARY".to_owned();
    document.firmware.targets[1].component_key = "SECONDARY".to_owned();
    let profile = HardwareUpdatePlan::compile(document, String::new()).unwrap();
    let targets = profile.stages()[0].targets();
    for (component, expected_ids) in [
        ("PRIMARY", vec!["standard_hmc"]),
        (" primary ", vec!["standard_hmc"]),
        ("SECONDARY", vec!["custom_hmc"]),
        (" secondary ", vec!["custom_hmc"]),
        ("UNCONFIGURED_COMPONENT", vec![]),
        ("", vec![]),
    ] {
        let matched_ids = targets
            .iter()
            .filter(|target| target.matches_component(component))
            .map(FirmwareTargetPlan::id)
            .collect::<Vec<_>>();
        assert_eq!(matched_ids, expected_ids, "{component:?}");
    }
}

#[test]
fn directory_loading_returns_profile_filename_and_conflicting_target_ids() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("conflicting.yaml");
    std::fs::write(
        &path,
        GB200.replacen("component_key: CPU_0", "component_key: HMC", 1),
    )
    .unwrap();
    let error = HardwareProfileCatalog::load_directory(directory.path()).unwrap_err();
    assert!(matches!(
        error,
        HardwareProfileError::InvalidProfileFile { .. }
    ));
    let message = error.to_string();
    for expected in [
        path.to_str().unwrap(),
        "compute_gb200_nvidia",
        "component_key \"hmc\"",
        "cpu_0",
        "hmc",
    ] {
        assert!(message.contains(expected), "{message}");
    }
}
