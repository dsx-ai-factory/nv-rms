// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::domain::node::{FirmwareTarget, FirmwareUpdateOptions};
use crate::nodes::nvfwupd_adapter::{to_update_request, to_version_check_targets};
use serde_json::json;

const GB200: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
));
const LENOVO: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../hardware-profiles/v1/compute_gb300_lenovo.yaml"
));

fn profile(parameters: &str) -> HardwareUpdatePlan {
    HardwareUpdatePlan::from_yaml_str(&GB200.replacen(
        "      request_targets: [/redfish/v1/Chassis/HGX_Chassis_0]",
        parameters,
        1,
    ))
    .unwrap()
}

fn resolve(plan: &HardwareUpdatePlan, component: &str, filename: &str) -> FirmwareTarget {
    plan.stages()
        .iter()
        .flat_map(|stage| stage.targets())
        .find(|target| target.matches_component(component))
        .unwrap()
        .resolve(&FirmwareTarget {
            component: component.into(),
            firmware_file: filename.into(),
            ..FirmwareTarget::default()
        })
        .unwrap()
}

#[test]
fn inline_json_and_yaml_objects_produce_identical_uploads() {
    let json_profile = profile(
        r#"      update_parameters: {"Targets": [], "ForceUpdate": true, "ApplyTime": "OnReset", "Vendor": {"enabled": false, "count": 3, "values": [null, 1.5]}}
      oem_parameters: {"ImageType": "Custom", "Nested": {"value": [1, true]}}"#,
    );
    let yaml_profile = profile(
        r#"      update_parameters:
        Targets: []
        ForceUpdate: true
        ApplyTime: OnReset
        Vendor:
          enabled: false
          count: 3
          values: [null, 1.5]
      oem_parameters:
        ImageType: Custom
        Nested:
          value: [1, true]"#,
    );
    let json_target = resolve(&json_profile, "HMC", "renamed.bin");
    let yaml_target = resolve(&yaml_profile, "HMC", "renamed.bin");
    assert_eq!(json_target, yaml_target);
    let request = to_update_request(
        &json_target,
        false,
        FirmwareUpdateOptions {
            apply_time: Some("Immediate".into()),
            ..FirmwareUpdateOptions::default()
        },
    );
    assert!(request.force_update);
    assert!(request.component.is_empty());
    assert!(request.options.apply_time.is_none());
    assert_eq!(
        request.options.special_json,
        Some(
            json!({"Targets":[], "ForceUpdate":true, "ApplyTime":"OnReset", "Vendor":{"enabled":false,"count":3,"values":[null,1.5]}})
        )
    );
    assert_eq!(
        request.options.oem_parameters,
        Some(json!({"ImageType":"Custom", "Nested":{"value":[1,true]}}))
    );
}

#[test]
fn multipart_targets_are_one_upload_with_matching_verification_scope() {
    for key in ["Targets", "HttpPushUriTargets"] {
        for paths in [
            vec![],
            vec!["/redfish/v1/Chassis/Custom"],
            vec!["/redfish/v1/Chassis/Custom", "/redfish/v1/Chassis/Other"],
        ] {
            let parameters = json!({key: paths});
            let plan = profile(&format!("      update_parameters: {parameters}"));
            let target = resolve(&plan, "HMC", "firmware.bin");
            let request = to_update_request(&target, false, FirmwareUpdateOptions::default());
            assert_eq!(request.options.special_json, Some(parameters));
            assert!(request.options.oem_parameters.is_none());
            let checks = to_version_check_targets(std::slice::from_ref(&target));
            let expected = if paths.is_empty() {
                vec![""]
            } else {
                paths.clone()
            };
            assert_eq!(
                checks
                    .iter()
                    .map(|check| check.component.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
            for path in paths {
                assert!(
                    !plan
                        .stages()
                        .iter()
                        .flat_map(|stage| stage.targets())
                        .any(|target| target.matches_component(path))
                );
            }
        }
    }
}

#[test]
fn parameter_fields_require_objects_instead_of_strings_or_scalars() {
    for field in ["update_parameters", "oem_parameters"] {
        for invalid in [
            r#"'{"Targets": []}'"#,
            "[]",
            "true",
            "42",
            "|\n        {\"Targets\": []}",
        ] {
            let source = GB200.replacen(
                "    - id: hmc",
                &format!("    - id: hmc\n      {field}: {invalid}"),
                1,
            );
            assert!(
                HardwareUpdatePlan::from_yaml_str(&source).is_err(),
                "{field}: {invalid}"
            );
        }
    }
}

#[test]
fn update_targets_validate_shape_paths_and_conflicting_destinations() {
    let invalid = [
        json!({"Targets":"/redfish/v1/Chassis/Test"}),
        json!({"Targets":[3]}),
        json!({"Targets":["https://example.test/redfish/v1/Chassis/Test"]}),
        json!({"Targets":["/redfish/v1/../secret"]}),
        json!({"Targets":["/redfish/v1/%2e%2e/secret"]}),
        json!({"Targets":["/redfish/v1/Chassis/A?query=1"]}),
        json!({"Targets":["/redfish/v1/Chassis/A", "/redfish/v1/Chassis/A"]}),
        json!({"Targets":[], "HttpPushUriTargets":[]}),
        json!({"Targets":[], "ForceUpdate":"true"}),
        json!({"Targets":[], "ApplyTime":false}),
    ];
    for parameters in invalid {
        let source = GB200.replacen(
            "      request_targets: [/redfish/v1/Chassis/HGX_Chassis_0]",
            &format!("      update_parameters: {parameters}"),
            1,
        );
        assert!(
            HardwareUpdatePlan::from_yaml_str(&source).is_err(),
            "{parameters}"
        );
    }
    for destination in [
        "request_targets: [/redfish/v1/Chassis/Custom]",
        "driver_default_targets: true",
    ] {
        let source = GB200.replacen(
            "      request_targets: [/redfish/v1/Chassis/HGX_Chassis_0]",
            &format!("      {destination}\n      update_parameters: {{\"Targets\": []}}"),
            1,
        );
        assert!(
            HardwareUpdatePlan::from_yaml_str(&source).is_err(),
            "{destination}"
        );
    }
}

#[test]
fn oem_values_are_opaque_and_image_type_is_explicit_for_every_filename() {
    for image_type in [json!("BMC"), json!("HPM"), json!(17)] {
        let source = LENOVO.replace(
            r#"{"ImageType": "BMC"}"#,
            &json!({"ImageType":image_type, "Vendor":{"unknown":[true,2,null]}}).to_string(),
        );
        let plan = HardwareUpdatePlan::from_yaml_str(&source).unwrap();
        for filename in ["image.ima", "image.HPM", "renamed.bin"] {
            let target = resolve(&plan, "BMC", filename);
            let request = to_update_request(&target, false, FirmwareUpdateOptions::default());
            assert_eq!(
                request.options.oem_parameters,
                Some(json!({"ImageType":image_type,"Vendor":{"unknown":[true,2,null]}}))
            );
        }
    }
}

#[test]
fn parameter_objects_without_targets_keep_explicit_destinations_and_force_precedence() {
    let source = GB200.replacen("    - id: hmc", "    - id: hmc\n      update_parameters: {\"ForceUpdate\": false, \"VendorOption\": true}\n      oem_parameters: {}", 1);
    let plan = HardwareUpdatePlan::from_yaml_str(&source).unwrap();
    let target = resolve(&plan, "HMC", "firmware.bin");
    for force in [false, true] {
        let request = to_update_request(&target, force, FirmwareUpdateOptions::default());
        assert_eq!(request.force_update, force);
        assert_eq!(request.component, "/redfish/v1/Chassis/HGX_Chassis_0");
        assert_eq!(request.options.oem_parameters, Some(json!({})));
    }
}

#[test]
fn multipart_parameters_are_platform_independent() {
    let source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hardware-profiles/v1/compute_gb300_supermicro.yaml"
    ));
    let paths = [
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1",
    ];
    let parameters = json!({
        "Targets": paths,
        "ForceUpdate": false,
        "VendorOption": {"mode": "custom"},
    });
    for node_type in [
        NodeType::ComputeGb200Nvidia,
        NodeType::ComputeGb200Wiwynn,
        NodeType::ComputeGb300Nvidia,
        NodeType::ComputeGb300Lenovo,
        NodeType::ComputeGb300Supermicro,
        NodeType::ComputeVrnvl72Nvidia,
    ] {
        let mut document: HardwareProfileDocument = serde_yaml::from_str(source).unwrap();
        document.matches.node_type = node_type.as_str().into();
        document.firmware.targets[1].update_parameters =
            Some(parameters.as_object().unwrap().clone());
        let profile = HardwareUpdatePlan::compile(document, String::new()).unwrap();
        let target = resolve(&profile, "BIOS", "bios.bin");
        let request = to_update_request(
            &target,
            true,
            FirmwareUpdateOptions {
                special_json: Some(json!({"Targets": [], "UnusedDefault": true})),
                ..FirmwareUpdateOptions::default()
            },
        );
        assert!(request.force_update);
        assert!(request.component.is_empty());
        assert_eq!(request.options.special_json, Some(parameters.clone()));
        let checks = to_version_check_targets(&[target]);
        assert_eq!(
            checks
                .iter()
                .map(|check| check.component.as_str())
                .collect::<Vec<_>>(),
            paths,
        );
    }
}

#[test]
fn shared_multipart_paths_do_not_make_component_keys_ambiguous() {
    let source = GB200.replacen("    - id: hmc", "    - id: hmc\n      update_parameters: {\"Targets\": [\"/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0\"]}", 1)
        .replacen("      request_targets: [/redfish/v1/Chassis/HGX_Chassis_0]\n", "", 1);
    let plan = HardwareUpdatePlan::from_yaml_str(&source).unwrap();
    assert_ne!(
        resolve(&plan, "HMC", "firmware.bin").resolution,
        resolve(&plan, "CPU_0", "firmware.bin").resolution,
    );
    assert!(
        !plan
            .stages()
            .iter()
            .flat_map(|stage| stage.targets())
            .any(|target| {
                target.matches_component("/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0")
            })
    );
}
