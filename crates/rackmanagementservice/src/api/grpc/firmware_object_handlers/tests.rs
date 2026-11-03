// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::super::server::SwitchTlsRoots;
use super::*;
use crate::orchestrator::job_lifecycle::{JobLifecycleState, JobSpec};
use crate::orchestrator::job_tracker::{JobTracker, RmsJobDomain};
use crate::orchestrator::rack_manager::RackManager;
use crate::persistence::Backends;

pub(super) fn test_service() -> RackManagerServiceImpl {
    RackManagerServiceImpl {
        rack_manager: Arc::new(RackManager::new()),
        job_tracker: Arc::new(JobTracker::new()),
        firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
        backends: Backends::memory(),
        firmware_dir: PathBuf::from("firmware"),
        artifact_require_sha256: false,
        insecure_bmc: true,
        artifact_download_policy:
            crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
        switch_tls_roots: SwitchTlsRoots::default(),
        sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
        nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
        expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        workflow_timings: crate::config::WorkflowTimings::default(),
        recovery_profiles: crate::config::RecoveryProfiles::default(),
        fabric_operations: std::sync::Arc::new(Default::default()),
        fabric_layout_catalog: std::sync::Arc::new(
            crate::config::fabric_layout::FabricLayoutCatalog::default(),
        ),
        hardware_profiles: std::sync::Arc::new(
            crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
        ),
    }
}

#[test]
fn firmware_flash_order_is_selected_by_node_type() {
    assert_eq!(
        get_firmware_flash_order(NodeType::SwitchVrnvl72Nvidia),
        &["bmc", "bios", "sma", "erot", "cpld1"]
    );
    assert_eq!(
        get_firmware_flash_order(NodeType::SwitchGb200Nvidia),
        &["bmc", "fpga", "erot", "cpld", "bios"]
    );
    assert_eq!(
        get_firmware_flash_order(NodeType::ComputeVrnvl72Nvidia),
        get_firmware_flash_order(NodeType::ComputeGb200Nvidia)
    );
    assert_eq!(
        get_firmware_flash_order(NodeType::PowershelfGb300Delta),
        &["DeltaPMC", "LiteOnPMC", "DeltaPSU", "LiteOnPSU"]
    );
    assert_eq!(
        get_firmware_flash_order(NodeType::PowershelfVrnvl72Liteon),
        &["LiteOnPMC", "LiteOnPSU"]
    );
}

#[test]
fn vr_liteon_power_shelf_accepts_pmc_and_all_psu_firmware_components() {
    assert!(firmware_component_allowed_for_node_type(
        NodeType::PowershelfVrnvl72Liteon,
        "LiteOnPMC"
    ));
    // All-PSU firmware uses HttpPushUri, independently of OEM action support.
    assert!(firmware_component_allowed_for_node_type(
        NodeType::PowershelfVrnvl72Liteon,
        "LiteOnPSU"
    ));
    assert!(!firmware_component_allowed_for_node_type(
        NodeType::PowershelfVrnvl72Liteon,
        "DeltaPMC"
    ));
    // GB200/GB300 LiteOn shelves keep both components.
    for node_type in [
        NodeType::PowershelfGb200Liteon,
        NodeType::PowershelfGb300Liteon,
    ] {
        assert!(firmware_component_allowed_for_node_type(
            node_type,
            "LiteOnPSU"
        ));
    }
}

#[test]
fn vr_liteon_power_shelf_uses_power_shelf_lookup_and_device_type() {
    assert_eq!(
        device_type_for_node_type(NodeType::PowershelfVrnvl72Liteon),
        Some(DeviceType::PowerShelf)
    );
    assert_eq!(
        firmware_node_type_lookup_keys()
            .iter()
            .find(|(node_type, _)| *node_type == NodeType::PowershelfVrnvl72Liteon)
            .map(|(_, key)| *key),
        Some(POWER_SHELF_LOOKUP_KEY)
    );
}

#[test]
fn vr_delta_power_shelf_accepts_only_delta_firmware_components() {
    for component in ["DeltaPMC", "DeltaPSU"] {
        assert!(firmware_component_allowed_for_node_type(
            NodeType::PowershelfVrnvl72Delta,
            component
        ));
    }
    for component in ["LiteOnPMC", "LiteOnPSU"] {
        assert!(!firmware_component_allowed_for_node_type(
            NodeType::PowershelfVrnvl72Delta,
            component
        ));
    }
    assert_eq!(
        get_firmware_flash_order(NodeType::PowershelfVrnvl72Delta),
        get_firmware_flash_order(NodeType::PowershelfGb300Delta)
    );
}

#[test]
fn vr_delta_power_shelf_uses_power_shelf_lookup_and_device_type() {
    assert_eq!(
        device_type_for_node_type(NodeType::PowershelfVrnvl72Delta),
        Some(DeviceType::PowerShelf)
    );
    assert_eq!(
        firmware_node_type_lookup_keys()
            .iter()
            .find(|(node_type, _)| *node_type == NodeType::PowershelfVrnvl72Delta)
            .map(|(_, key)| *key),
        Some(POWER_SHELF_LOOKUP_KEY)
    );
}

fn release_catalog(board_skus: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "ProductName": "TestProduct",
        "Milestones": [{
            "Name": "test-release",
            "State": "Released",
            "BoardSKUs": board_skus
        }]
    })
}

pub(super) fn linkx_manifest() -> FirmwareManifest {
    serde_json::from_value(serde_json::json!({
        "ProductName": "VR-NVL72",
        "Milestones": [{"Name": "test-release", "BoardSKUs": [{
            "SKUID": "vr-test", "Name": "P4109", "Type": "Compute Tray",
            "Components": {"Firmware": [{
                "Component": "LINKX", "Type": "Prod", "Version": "44.40.2035",
                "LinkxRollout": {
                    "current_version": "44.40.2019", "target_version": "44.40.2035",
                    "sha256": "a".repeat(64)
                },
                "Locations": [
                    {"Location": "https://example.test/module.cbor", "LocationType": "HTTPS", "Type": "Binary"},
                    {"Location": "https://example.test/module.bin", "LocationType": "HTTPS", "Type": "Binary"},
                    {"Location": "https://example.test/readme.json", "LocationType": "HTTPS", "Type": "Binary"}
                ]
            }]}
        }]}]
    })).unwrap()
}

#[test]
fn linkx_metadata_is_vr_only_and_comparison_requires_host_inspection() {
    let manifest = linkx_manifest();
    let metadata = metadata::from_manifest(&manifest).unwrap();
    let [device] = metadata.device_components.as_slice() else {
        panic!("only VRNVL72 must expose LINKX");
    };
    assert_eq!(
        device.node_descriptor,
        Some(domain_node_type_to_descriptor(
            NodeType::ComputeVrnvl72Nvidia
        ))
    );
    let [component] = device.components.as_slice() else {
        panic!("expected LINKX")
    };
    assert_eq!(component.name, "LINKX");
    assert_eq!(component.artifacts.len(), 1);
    assert_eq!(component.artifacts[0].filename, "module.bin");
    let expectations = plan::expectations(
        &manifest,
        NodeType::ComputeVrnvl72Nvidia,
        "prod",
        &FirmwareObjectComponentSelection::components(vec!["LINKX".to_owned()]),
    )
    .unwrap();
    let comparison = compare_firmware_object_for_node(
        "vr".to_owned(),
        NodeType::ComputeVrnvl72Nvidia,
        &expectations,
        &[],
    );
    assert!(!comparison.matches_manifest);
    assert_eq!(comparison.components.len(), 1);
    assert_eq!(
        comparison.components[0].status,
        rm::FirmwareComparisonStatus::Unverified as i32
    );
}

fn nvos_manifest() -> Value {
    release_catalog(serde_json::json!([{
        "Type": "Switch Tray",
        "Components": {"Software": [{
            "Component": "NVOS",
            "Version": "25.02.4440",
            "Type": "Prod",
            "Locations": [{
                "Location": "https://example.test/nvos-amd64-25.02.4440.bin",
                "LocationType": "https",
                "FileName": "nvos-amd64-25.02.4440.bin",
                "PackageName": "TestNVOS",
                "Required": false
            }]
        }]}
    }]))
}

#[test]
fn nvos_metadata_preserves_optional_image_without_a_download_url() {
    let mut config = nvos_manifest();
    let location =
        &mut config["Milestones"][0]["BoardSKUs"][0]["Components"]["Software"][0]["Locations"][0];
    location["Location"] = Value::Null;
    location["LocationType"] = Value::Null;
    let manifest = FirmwareManifest::try_from(&config).unwrap();

    let metadata = metadata::from_manifest(&manifest).unwrap();
    let [image] = metadata.switch_system_images.as_slice() else {
        panic!("metadata-only NVOS image must remain visible");
    };
    assert_eq!(image.software_type, "prod");
    assert_eq!(image.version, "25.02.4440");
    assert_eq!(image.image_filename, "nvos-amd64-25.02.4440.bin");
    assert_eq!(image.package_name, "TestNVOS");
    assert!(!image.required);
    assert!(plan::switch_image(&manifest, "Prod").is_err());
}

#[test]
fn nvos_metadata_preserves_required_policy_separately_from_apply() {
    for required in [Some(false), Some(true), None] {
        let mut config = nvos_manifest();
        config["Milestones"][0]["BoardSKUs"][0]["Components"]["Software"][0]["Locations"][0]["Required"] =
            serde_json::json!(required);
        let manifest = FirmwareManifest::try_from(&config).unwrap();
        let metadata = metadata::from_manifest(&manifest).unwrap();
        assert_eq!(metadata.switch_system_images.len(), 1);
        assert_eq!(
            metadata.switch_system_images[0].required,
            required.unwrap_or(true)
        );
        // Selecting an image for execution requires its bytes even when prefetch was optional.
        assert!(plan::switch_image(&manifest, "Prod").unwrap().required);
    }
}

#[test]
fn nvos_metadata_uses_image_package_not_a_documentation_location() {
    let mut config = nvos_manifest();
    let locations =
        config["Milestones"][0]["BoardSKUs"][0]["Components"]["Software"][0]["Locations"]
            .as_array_mut()
            .unwrap();
    locations[0].as_object_mut().unwrap().remove("FileName");
    locations.insert(
        0,
        serde_json::json!({
            "FileName": "release-notes.pdf", "PackageName": "NotTheImage", "Required": true
        }),
    );
    let metadata = metadata::from_manifest(&FirmwareManifest::try_from(&config).unwrap()).unwrap();
    let [image] = metadata.switch_system_images.as_slice() else {
        panic!("expected only the NVOS image");
    };
    assert_eq!(image.package_name, "TestNVOS");
    assert_eq!(image.image_filename, "nvos-amd64-25.02.4440.bin");
    assert!(!image.required);
}

#[test]
fn nvos_metadata_keeps_board_candidates_even_when_apply_is_ambiguous() {
    let mut config = nvos_manifest();
    let boards = config["Milestones"][0]["BoardSKUs"].as_array_mut().unwrap();
    let mut second = boards[0].clone();
    second["Components"]["Software"][0]["Locations"][0]["FileName"] =
        serde_json::json!("nvos-amd64-25.02.4441.bin");
    boards.push(second);
    let manifest = FirmwareManifest::try_from(&config).unwrap();
    let metadata = metadata::from_manifest(&manifest).unwrap();
    assert_eq!(metadata.switch_system_images.len(), 2);
    assert!(plan::switch_image(&manifest, "Prod").is_err());
}

#[tokio::test]
async fn object_add_get_and_list_expose_metadata_only_optional_nvos() {
    let temp = tempfile::tempdir().unwrap();
    let mut service = test_service();
    service.firmware_dir = temp.path().to_path_buf();
    let mut config = nvos_manifest();
    config["Milestones"][0]["BoardSKUs"][0]["Components"]["Software"][0]["Locations"][0]["Location"] =
        Value::Null;
    let added = service
        .handle_add_firmware_object(Request::new(rm::AddFirmwareObjectRequest {
            config_json: config.to_string(),
            hardware_type: "gb200".to_owned(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .object
        .unwrap();
    let retrieved = service
        .handle_get_firmware_object(Request::new(rm::GetFirmwareObjectRequest {
            id: added.id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .object
        .unwrap();
    let listed = service
        .handle_list_firmware_objects(Request::new(rm::ListFirmwareObjectsRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .objects;
    assert_eq!(listed.len(), 1);
    for object in std::iter::once(added)
        .chain(std::iter::once(retrieved))
        .chain(listed)
    {
        assert!(!object.available);
        let metadata = object.metadata.unwrap();
        let [image] = metadata.switch_system_images.as_slice() else {
            panic!("expected NVOS metadata");
        };
        assert_eq!(image.image_filename, "nvos-amd64-25.02.4440.bin");
        assert!(!image.required);
    }
}

#[tokio::test]
async fn add_object_rejects_wrong_field_types_before_persistence() {
    let service = test_service();
    let mut config = nvos_manifest();
    config["Milestones"][0]["BoardSKUs"][0]["Components"]["Software"][0]["Locations"][0]["Sha256"] =
        serde_json::json!(17);
    let error = service
        .handle_add_firmware_object(Request::new(rm::AddFirmwareObjectRequest {
            config_json: config.to_string(),
            hardware_type: "gb200".to_owned(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("Locations[0].Sha256"), "{error}");
    assert!(
        service
            .backends
            .firmware_objects
            .list(FirmwareObjectSearchFilter::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn ingestion_rejects_duplicate_known_fields_before_value_conversion() {
    let config = r#"{"ProductName":"first","ProductName":"second","Milestones":[{"Name":"1.0","BoardSKUs":[]}]}"#;
    let error = test_service()
        .handle_add_firmware_object(Request::new(rm::AddFirmwareObjectRequest {
            config_json: config.to_owned(),
            hardware_type: "gb200".to_owned(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("duplicate field"));
    let error = config.parse::<FirmwareManifest>().unwrap_err();
    assert!(error.to_string().contains("duplicate field"));
}

fn installed_firmware(name: &str, version: &str, target: &str, sku: &str) -> FirmwareInfo {
    FirmwareInfo {
        name: name.to_owned(),
        version: version.to_owned(),
        firmware_type: crate::domain::node::FirmwareType::Unknown,
        updateable: true,
        target: target.to_owned(),
        health: "OK".to_owned(),
        state: "Enabled".to_owned(),
        sku: sku.to_owned(),
    }
}

#[test]
fn firmware_manifest_comparison_reports_match_mismatch_and_missing() {
    let expectations = vec![
        FirmwareManifestExpectation {
            component: "HMC_SKU_179".to_owned(),
            version: "1.2.3".to_owned(),
            target: "/redfish/v1/Chassis/HGX_Chassis_0".to_owned(),
            sku_id: String::new(),
        },
        FirmwareManifestExpectation {
            component: "SBIOS_C03_2P_894".to_owned(),
            version: "2.0".to_owned(),
            target: String::new(),
            sku_id: String::new(),
        },
        FirmwareManifestExpectation {
            component: "CPLD".to_owned(),
            version: "3.0".to_owned(),
            target: String::new(),
            sku_id: String::new(),
        },
    ];
    let inventory = vec![
        installed_firmware(
            "HGX_FW_BMC_0",
            "1.2.3",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_BMC_0",
            "",
        ),
        installed_firmware(
            "HGX_FW_CPU_0",
            "1.9",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
            "",
        ),
    ];

    let comparison = compare_firmware_object_for_node(
        "compute-1".to_owned(),
        NodeType::ComputeGb200Nvidia,
        &expectations,
        &inventory,
    );

    assert!(!comparison.matches_manifest);
    assert_eq!(comparison.components.len(), 3);
    assert_eq!(
        comparison.components[0].status,
        rm::FirmwareComparisonStatus::Matched as i32
    );
    assert_eq!(
        comparison.components[1].status,
        rm::FirmwareComparisonStatus::Mismatched as i32
    );
    assert_eq!(
        comparison.components[2].status,
        rm::FirmwareComparisonStatus::Missing as i32
    );
}

#[test]
fn switch_bmc_manifest_comparison_resolves_raw_inventory_names() {
    for (component, name, installed, proposed) in [
        ("BMC", "MGX_FW_BMC_0", "88.1", "88.1"),
        ("SBIOS", "MGX_FW_CPU_0", "1.0", "1.0"),
        ("EROT", "MGX_FW_ERoT_BMC_0", "2.0", "2.0"),
        ("FPGA", "MGX_FW_FPGA_0", "3.0", "3.0"),
        ("SMA", "MGX_FW_NVSwitch_SMA_0", "4.0", "4.0"),
        (
            "CPLD",
            "CPLD_0",
            "CPLD000472_REV0006_CPLD000450_REV0009",
            "000472_REV0006_000450_REV0009",
        ),
    ] {
        let expectation = FirmwareManifestExpectation {
            component: component.to_owned(),
            version: proposed.to_owned(),
            target: component.to_owned(),
            sku_id: String::new(),
        };
        let inventory = [installed_firmware(
            name,
            installed,
            &format!("/redfish/v1/UpdateService/FirmwareInventory/{name}"),
            "",
        )];
        let result = compare_firmware_object_component(
            NodeType::SwitchVrnvl72Nvidia,
            &expectation,
            &inventory,
        );
        assert_eq!(
            result.status,
            rm::FirmwareComparisonStatus::Matched as i32,
            "{result:?}"
        );
        assert_eq!(result.installed_component, name);
    }
}

#[test]
fn switch_bmc_manifest_comparison_checks_every_erot_and_does_not_invent_nvos() {
    let inventory = [
        installed_firmware("MGX_FW_BMC_0", "88.1", "", ""),
        installed_firmware("MGX_FW_ERoT_BMC_0", "2.0", "", ""),
        installed_firmware("MGX_FW_ERoT_CPU_0", "old", "", ""),
    ];
    let expectation = FirmwareManifestExpectation {
        component: "EROT".to_owned(),
        version: "2.0".to_owned(),
        target: "EROT".to_owned(),
        sku_id: String::new(),
    };
    let result =
        compare_firmware_object_component(NodeType::SwitchVrnvl72Nvidia, &expectation, &inventory);
    assert_eq!(
        result.status,
        rm::FirmwareComparisonStatus::Mismatched as i32
    );
    assert!(result.installed_component.contains("MGX_FW_ERoT_BMC_0"));
    assert!(result.installed_component.contains("MGX_FW_ERoT_CPU_0"));
    assert!(
        !result
            .installed_component
            .split(',')
            .any(|name| name.trim() == "MGX_FW_BMC_0")
    );
    let result = compare_firmware_object_component(
        NodeType::SwitchVrnvl72Nvidia,
        &FirmwareManifestExpectation {
            component: "NVOS".to_owned(),
            version: "25.02".to_owned(),
            target: "NVOS".to_owned(),
            sku_id: String::new(),
        },
        &inventory,
    );
    assert_eq!(result.status, rm::FirmwareComparisonStatus::Missing as i32);
}

#[test]
fn firmware_manifest_comparison_uses_sku_to_disambiguate_fungible_components() {
    let expectation = FirmwareManifestExpectation {
        component: "GPU".to_owned(),
        version: "96.00.5E.00.01".to_owned(),
        target: String::new(),
        sku_id: "GPU-SKU-B".to_owned(),
    };
    let inventory = vec![
        installed_firmware("HGX_FW_GPU_0", "old", "", "GPU-SKU-A"),
        installed_firmware("HGX_FW_GPU_1", "96.00.5E.00.01", "", "GPU-SKU-B"),
    ];

    let comparison =
        compare_firmware_object_component(NodeType::ComputeGb200Nvidia, &expectation, &inventory);

    assert_eq!(
        comparison.status,
        rm::FirmwareComparisonStatus::Matched as i32
    );
    assert_eq!(comparison.installed_component, "HGX_FW_GPU_1");
}

#[test]
fn firmware_manifest_comparison_reports_only_candidates_without_sku_as_unverified() {
    let expectation = FirmwareManifestExpectation {
        component: "GPU".to_owned(),
        version: "96.00.5E.00.01".to_owned(),
        target: String::new(),
        sku_id: "GPU-SKU-B".to_owned(),
    };
    let inventory = vec![
        installed_firmware("HGX_FW_GPU_0", "old", "", "GPU-SKU-A"),
        installed_firmware("HGX_FW_GPU_1", "96.00.5E.00.01", "", ""),
    ];

    let comparison =
        compare_firmware_object_component(NodeType::ComputeGb200Nvidia, &expectation, &inventory);

    assert_eq!(
        comparison.status,
        rm::FirmwareComparisonStatus::Unverified as i32
    );
    assert_eq!(comparison.installed_component, "HGX_FW_GPU_1");
    assert_eq!(comparison.installed_version, "96.00.5E.00.01");
}

#[test]
fn firmware_manifest_comparison_does_not_match_a_sibling_inventory_target() {
    let comparison = compare_firmware_object_component(
        NodeType::ComputeGb200Nvidia,
        &FirmwareManifestExpectation {
            component: "GPU_0".to_owned(),
            version: "97.00.F8.00.1F".to_owned(),
            target: "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0".to_owned(),
            sku_id: String::new(),
        },
        &[installed_firmware(
            "HGX_FW_GPU_1",
            "97.00.F8.00.1F",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_1",
            "",
        )],
    );

    assert_eq!(
        comparison.status,
        rm::FirmwareComparisonStatus::Missing as i32
    );
}

#[test]
fn aggregate_hmc_comparison_keeps_indexed_inventory_instances_separate() {
    let comparison = compare_firmware_object_component(
        NodeType::ComputeGb200Nvidia,
        &FirmwareManifestExpectation {
            component: "GPU_0".to_owned(),
            version: "97.00.F8.00.1F".to_owned(),
            target: "/redfish/v1/Chassis/HGX_Chassis_0".to_owned(),
            sku_id: String::new(),
        },
        &[
            installed_firmware(
                "HGX_FW_GPU_0",
                "old",
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
                "",
            ),
            installed_firmware(
                "HGX_FW_GPU_1",
                "97.00.F8.00.1F",
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_1",
                "",
            ),
        ],
    );

    assert_eq!(
        comparison.status,
        rm::FirmwareComparisonStatus::Mismatched as i32
    );
    assert_eq!(comparison.installed_component, "HGX_FW_GPU_0");
    assert_eq!(comparison.installed_version, "old");
}

#[test]
fn firmware_manifest_comparison_marks_flint_components_unverified() {
    let comparison = compare_firmware_object_component(
        NodeType::ComputeGb200Nvidia,
        &FirmwareManifestExpectation {
            component: "CX7".to_owned(),
            version: "28.49.1118".to_owned(),
            target: "CX7".to_owned(),
            sku_id: String::new(),
        },
        &[],
    );

    assert_eq!(
        comparison.status,
        rm::FirmwareComparisonStatus::Unverified as i32
    );
    assert!(comparison.reason.contains("Flint"));
}

#[test]
fn manifest_component_instance_matching_distinguishes_component_families() {
    assert!(manifest_component_matches_target("GPU_0", "GPU_0"));
    assert!(!manifest_component_matches_target("GPU_0", "GPU_1"));
    assert!(!manifest_component_matches_target(
        "EROT_CPU_0",
        "EROT_FPGA_0"
    ));
    assert!(manifest_component_matches_target("GPU", "GPU_3"));
}

#[test]
fn firmware_object_id_must_be_single_safe_path_component() {
    assert!(validate_firmware_object_id("fw-1").is_ok());
    assert!(validate_firmware_object_id("").is_err());
    assert!(validate_firmware_object_id("..").is_err());
    assert!(validate_firmware_object_id("../evil").is_err());
    assert!(validate_firmware_object_id("nested/evil").is_err());
    assert!(validate_firmware_object_id("nested\\evil").is_err());
}

#[test]
fn accepted_batch_counts_admitted_nodes_not_completed_successes() {
    let response = accepted_batch_with_stats(
        "job-1".to_owned(),
        3,
        2,
        1,
        vec![rm::NodeOperationResult {
            node_id: "node-3".to_owned(),
            status: rm::ReturnCode::Failure.into(),
            error_message: "busy".to_owned(),
        }],
        "queued",
    );
    let Some(stats) = response.stats.as_ref() else {
        panic!("accepted batch should include stats");
    };

    // `successful_nodes` counts child jobs admitted, not firmware applied;
    // one rejected node makes the whole batch a failure and is reported
    // per-node rather than dropped.
    assert_eq!(response.status, rm::ReturnCode::Failure as i32);
    assert_eq!(stats.total_nodes, 3);
    assert_eq!(stats.successful_nodes, 2);
    assert_eq!(stats.failed_nodes, 1);
    assert_eq!(response.node_results.len(), 1);
}

#[tokio::test]
async fn apply_firmware_object_stats_count_created_and_rejected_jobs() {
    let service = test_service();
    let mut nodes = Vec::new();
    let mut active_jobs = Vec::new();
    for idx in 0..3 {
        let node_id = format!("node-active-{idx}");
        let active = service
            .job_tracker
            .create_job("rack-1", &node_id, JobType::FirmwareUpdate)
            .unwrap();
        active.progress("running");
        active_jobs.push(active);

        nodes.push(rm::NodeInfo {
            node_id,
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            ..Default::default()
        });
    }

    for idx in 0..7 {
        nodes.push(rm::NodeInfo {
            node_id: format!("node-created-{idx}"),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            ..Default::default()
        });
    }

    let request = rm::ApplyFirmwareObjectRequest {
        rack_id: "rack-1".to_owned(),
        config_json: "not-json".to_owned(),
        access_token: Some("token".to_owned()),
        firmware_type: "prod".to_owned(),
        hardware_type: "gb200".to_owned(),
        nodes: Some(rm::NodeSet { nodes }),
        ..Default::default()
    };

    let Ok(response) = service
        .handle_apply_firmware_object(Request::new(request))
        .await
    else {
        panic!("apply firmware object should return a response");
    };

    let response = response.into_inner();
    let Some(batch) = response.response.as_ref() else {
        panic!("batch response should be present");
    };

    let Some(stats) = batch.stats.as_ref() else {
        panic!("stats should be present");
    };

    assert_eq!(stats.total_nodes, 10);
    assert_eq!(stats.successful_nodes, 7);
    assert_eq!(stats.failed_nodes, 3);
    assert_eq!(response.jobs.len(), 7);
    assert_eq!(batch.node_results.len(), 3);
}

#[tokio::test]
async fn apply_switch_system_image_reports_rejected_switches() {
    let service = test_service();
    let mut nodes = Vec::new();
    let mut active_jobs = Vec::new();

    // Two switches are already owned by an in-flight image update, so their
    // apply must be refused rather than admitted alongside it.
    for idx in 0..2 {
        let node_id = format!("sw-busy-{idx}");
        let active = service
            .job_tracker
            .create_job("rack-1", &node_id, JobType::SwitchSystemImageUpdate)
            .unwrap();
        active.progress("running");
        active_jobs.push(active);

        nodes.push(rm::NodeInfo {
            node_id,
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
            ..Default::default()
        });
    }

    for idx in 0..3 {
        nodes.push(rm::NodeInfo {
            node_id: format!("sw-idle-{idx}"),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
            ..Default::default()
        });
    }

    let request = rm::ApplySwitchSystemImageRequest {
        rack_id: "rack-1".to_owned(),
        config_json: "not-json".to_owned(),
        hardware_type: "gb200".to_owned(),
        nodes: Some(rm::NodeSet { nodes }),
        ..Default::default()
    };

    let response = service
        .handle_apply_switch_system_image(Request::new(request))
        .await
        .expect("handler should return a response")
        .into_inner();

    let Some(batch) = response.response.as_ref() else {
        panic!("batch response should be present");
    };
    let Some(stats) = batch.stats.as_ref() else {
        panic!("stats should be present");
    };

    assert_eq!(stats.total_nodes, 5);
    assert_eq!(stats.successful_nodes, 3);
    assert_eq!(stats.failed_nodes, 2);
    assert_eq!(response.jobs.len(), 3);
    assert_eq!(batch.status, rm::ReturnCode::Failure as i32);

    let rejected: Vec<&str> = batch
        .node_results
        .iter()
        .map(|result| result.node_id.as_str())
        .collect();
    assert_eq!(rejected, ["sw-busy-0", "sw-busy-1"]);
}

#[tokio::test]
async fn apply_switch_system_image_rejects_duplicate_switch_targets() {
    let service = test_service();
    let device = rm::NodeInfo {
        node_id: "sw-1".to_owned(),
        rack_id: "rack-1".to_owned(),
        r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
        ..Default::default()
    };

    let request = rm::ApplySwitchSystemImageRequest {
        rack_id: "rack-1".to_owned(),
        config_json: "not-json".to_owned(),
        hardware_type: "gb200".to_owned(),
        nodes: Some(rm::NodeSet {
            nodes: vec![device.clone(), device],
        }),
        ..Default::default()
    };

    let response = service
        .handle_apply_switch_system_image(Request::new(request))
        .await
        .expect("handler should return a response")
        .into_inner();

    let Some(batch) = response.response.as_ref() else {
        panic!("batch response should be present");
    };

    // The repeated switch takes one job, not two concurrent installs.
    assert_eq!(response.jobs.len(), 1);
    assert_eq!(batch.node_results.len(), 1);
    assert!(
        batch.node_results[0].error_message.contains("duplicate"),
        "expected a duplicate-target rejection, got: {}",
        batch.node_results[0].error_message
    );
}

#[tokio::test]
async fn apply_firmware_object_reports_parent_creation_failure_when_tracker_full() {
    let service = RackManagerServiceImpl {
        rack_manager: Arc::new(RackManager::new()),
        job_tracker: Arc::new(JobTracker::builder().max_tracked_jobs(1).build().unwrap()),
        firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
        backends: Backends::memory(),
        firmware_dir: PathBuf::from("firmware"),
        artifact_require_sha256: false,
        insecure_bmc: true,
        artifact_download_policy:
            crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
        switch_tls_roots: SwitchTlsRoots::default(),
        sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
        nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
        expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        workflow_timings: crate::config::WorkflowTimings::default(),
        recovery_profiles: crate::config::RecoveryProfiles::default(),
        fabric_operations: std::sync::Arc::new(Default::default()),
        fabric_layout_catalog: std::sync::Arc::new(
            crate::config::fabric_layout::FabricLayoutCatalog::default(),
        ),
        hardware_profiles: std::sync::Arc::new(
            crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
        ),
    };

    let occupant = service
        .job_tracker
        .create_job("rack-1", "occupant", JobType::FirmwareUpdate)
        .unwrap();
    occupant.progress("running");

    let request = rm::ApplyFirmwareObjectRequest {
        rack_id: "rack-1".to_owned(),
        config_json: "not-json".to_owned(),
        access_token: Some("token".to_owned()),
        firmware_type: "prod".to_owned(),
        hardware_type: "gb200".to_owned(),
        nodes: Some(rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "node-1".to_owned(),
                rack_id: "rack-1".to_owned(),
                r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };

    let response = service
        .handle_apply_firmware_object(Request::new(request))
        .await
        .expect("handler should return a response")
        .into_inner();

    assert!(response.jobs.is_empty());
    let batch = response.response.expect("batch response should be present");
    assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
    assert!(batch.job_id.is_empty());
    assert_eq!(
        batch.message,
        "failed to create parent firmware object apply job"
    );
}

#[tokio::test]
async fn apply_switch_system_image_reports_parent_creation_failure_when_tracker_full() {
    let service = RackManagerServiceImpl {
        rack_manager: Arc::new(RackManager::new()),
        job_tracker: Arc::new(JobTracker::builder().max_tracked_jobs(1).build().unwrap()),
        firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
        backends: Backends::memory(),
        firmware_dir: PathBuf::from("firmware"),
        artifact_require_sha256: false,
        insecure_bmc: true,
        artifact_download_policy:
            crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
        switch_tls_roots: SwitchTlsRoots::default(),
        sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
        nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
        expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        workflow_timings: crate::config::WorkflowTimings::default(),
        recovery_profiles: crate::config::RecoveryProfiles::default(),
        fabric_operations: std::sync::Arc::new(Default::default()),
        fabric_layout_catalog: std::sync::Arc::new(
            crate::config::fabric_layout::FabricLayoutCatalog::default(),
        ),
        hardware_profiles: std::sync::Arc::new(
            crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
        ),
    };

    let occupant = service
        .job_tracker
        .create_job("rack-1", "occupant", JobType::SwitchSystemImageUpdate)
        .unwrap();
    occupant.progress("running");

    let request = rm::ApplySwitchSystemImageRequest {
        rack_id: "rack-1".to_owned(),
        config_json: "not-json".to_owned(),
        hardware_type: "gb200".to_owned(),
        nodes: Some(rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "sw-1".to_owned(),
                rack_id: "rack-1".to_owned(),
                r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };

    let response = service
        .handle_apply_switch_system_image(Request::new(request))
        .await
        .expect("handler should return a response")
        .into_inner();

    assert!(response.jobs.is_empty());
    let batch = response.response.expect("batch response should be present");
    assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
    assert!(batch.job_id.is_empty());
    assert_eq!(
        batch.message,
        "failed to create parent switch system image apply job"
    );
}

#[test]
fn precreate_firmware_jobs_rejects_active_node_job() {
    let service = test_service();
    let active = service
        .job_tracker
        .create_job("rack-1", "node-1", JobType::FirmwareUpdate)
        .unwrap();
    active.progress("running");
    let device = rm::NodeInfo {
        node_id: "node-1".to_owned(),
        rack_id: "rack-1".to_owned(),
        r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
        ..Default::default()
    };

    let parent_job_id = service
        .job_tracker
        .create_parent_job("rack-1", JobType::FirmwareUpdate)
        .expect("parent job should be created");
    let precreated = service.precreate_firmware_jobs(parent_job_id, vec![device]);

    assert!(!precreated.parent_job_id.is_empty());
    assert!(precreated.jobs.is_empty());
    assert!(precreated.node_jobs.is_empty());
    assert_eq!(precreated.node_results.len(), 1);
    assert_eq!(precreated.failed_nodes, 1);
    assert!(
        precreated.node_results[0]
            .error_message
            .contains(active.id().as_ref())
    );
    assert_eq!(
        service
            .job_tracker
            .get_job(&precreated.parent_job_id)
            .unwrap()
            .state,
        crate::orchestrator::job_lifecycle::JobState::Failed
    );
}

#[test]
fn precreate_firmware_jobs_rejects_unresolved_profiles_without_child_jobs() {
    let service = test_service();
    let unknown_device = rm::NodeInfo {
        node_id: "node-1".to_owned(),
        rack_id: "rack-1".to_owned(),
        r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
        node_descriptor: Some(rm::NodeDescriptor {
            attributes: HashMap::from([(
                crate::api::grpc::node_type_resolver::INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                "unknown-profile".to_owned(),
            )]),
        }),
        ..Default::default()
    };
    let empty_device = rm::NodeInfo {
        node_id: "node-2".to_owned(),
        rack_id: "rack-1".to_owned(),
        r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
        node_descriptor: Some(rm::NodeDescriptor {
            attributes: HashMap::from([(
                crate::api::grpc::node_type_resolver::INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                " ".to_owned(),
            )]),
        }),
        ..Default::default()
    };

    let parent_job_id = service
        .job_tracker
        .create_parent_job("rack-1", JobType::FirmwareUpdate)
        .expect("parent job should be created");
    let precreated =
        service.precreate_firmware_jobs(parent_job_id, vec![unknown_device, empty_device]);

    assert!(!precreated.parent_job_id.is_empty());
    assert!(precreated.jobs.is_empty());
    assert!(precreated.node_jobs.is_empty());
    assert_eq!(precreated.failed_nodes, 2);
    assert_eq!(precreated.node_results.len(), 2);
    assert!(
        precreated.node_results[0]
            .error_message
            .contains("unknown inventory_profile")
    );
    assert!(
        precreated.node_results[1]
            .error_message
            .contains("inventory_profile must not be empty")
    );
    assert_eq!(
        service
            .job_tracker
            .get_job(&precreated.parent_job_id)
            .unwrap()
            .state,
        crate::orchestrator::job_lifecycle::JobState::Failed
    );
}

#[test]
fn failing_precreated_firmware_jobs_seals_handles_and_refreshes_parent() {
    let service = test_service();
    let parent_job_id = service
        .job_tracker
        .create_parent_job("rack-1", JobType::FirmwareUpdate)
        .expect("parent job should be created");
    let jobs: Vec<PrecreatedFirmwareJob> = ["node-1", "node-2"]
        .into_iter()
        .map(|node_id| PrecreatedFirmwareJob {
            device: rm::NodeInfo {
                rack_id: "rack-1".to_owned(),
                node_id: node_id.to_owned(),
                ..Default::default()
            },
            pending: service
                .job_tracker
                .create_child_job(&parent_job_id, "rack-1", node_id, JobType::FirmwareUpdate)
                .expect("child job should be created"),
            expected_inventory: None,
            recovery: None,
        })
        .collect();
    let child_job_ids = firmware_job_ids(&jobs);

    mark_firmware_jobs_failed(
        &service.job_tracker,
        jobs,
        JobError::InvalidArgument,
        "invalid firmware object",
    );

    let parent = service
        .job_tracker
        .registry()
        .get(&JobId::from(parent_job_id.as_str()))
        .expect("parent job should exist");
    assert!(matches!(parent.state, JobLifecycleState::Failed { .. }));
    for child_job_id in child_job_ids {
        let child = service
            .job_tracker
            .registry()
            .get(&JobId::from(child_job_id))
            .expect("child job should exist");
        let JobLifecycleState::Failed { message, .. } = child.state else {
            panic!("child job should be failed");
        };
        assert_eq!(message, "invalid firmware object");
    }
}

#[test]
fn failing_precreated_switch_jobs_seals_handles_and_refreshes_parent() {
    let service = test_service();
    let parent_job_id = service
        .job_tracker
        .create_parent_job("rack-1", JobType::SwitchSystemImageUpdate)
        .expect("parent job should be created");
    let jobs: Vec<PrecreatedSwitchImageJob> = ["switch-1", "switch-2"]
        .into_iter()
        .map(|node_id| PrecreatedSwitchImageJob {
            device: rm::NodeInfo {
                rack_id: "rack-1".to_owned(),
                node_id: node_id.to_owned(),
                ..Default::default()
            },
            pending: service
                .job_tracker
                .create_child_job(
                    &parent_job_id,
                    "rack-1",
                    node_id,
                    JobType::SwitchSystemImageUpdate,
                )
                .expect("child job should be created"),
        })
        .collect();
    let child_job_ids = switch_image_job_ids(&jobs);

    mark_switch_jobs_failed(&service.job_tracker, jobs, "invalid switch image");

    let parent = service
        .job_tracker
        .registry()
        .get(&JobId::from(parent_job_id.as_str()))
        .expect("parent job should exist");
    assert!(matches!(parent.state, JobLifecycleState::Failed { .. }));
    for child_job_id in child_job_ids {
        let child = service
            .job_tracker
            .registry()
            .get(&JobId::from(child_job_id))
            .expect("child job should exist");
        let JobLifecycleState::Failed { message, .. } = child.state else {
            panic!("child job should be failed");
        };
        assert_eq!(message, "invalid switch image");
    }
}

// The ephemeral object cache cleanup is a generic job_lifecycle operation
// shared by firmware and switch batches, so exercise it directly against a
// bare JobRegistry rather than standing up a full JobTracker.
#[tokio::test]
async fn cleanup_removes_ephemeral_cache_after_jobs_terminal() {
    let registry = Arc::new(JobRegistry::<RmsJobDomain>::new(Duration::from_secs(3600)));
    let pending = registry
        .create_leaf(JobSpec::new(
            "rack-1",
            "switch-1",
            "Queued",
            tracing::Span::none(),
        ))
        .unwrap();
    let job_id = pending.id().to_string();

    let temp = tempfile::tempdir().unwrap();
    let cache_dir = temp.path().join("firmware_objects_ephemeral").join("fw-1");
    tokio::fs::create_dir_all(&cache_dir).await.unwrap();
    tokio::fs::write(cache_dir.join("nvos.bin"), b"image")
        .await
        .unwrap();

    let cleanup = tokio::spawn(cleanup_ephemeral_object_cache_after_jobs(
        registry.clone(),
        vec![job_id.clone()],
        cache_dir.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(cache_dir.exists());

    pending.complete("done", "");
    tokio::time::timeout(Duration::from_secs(1), cleanup)
        .await
        .unwrap()
        .unwrap();
    assert!(!cache_dir.exists());
}

#[tokio::test]
async fn cleanup_treats_missing_job_as_terminal() {
    let registry = Arc::new(JobRegistry::<RmsJobDomain>::new(Duration::from_secs(3600)));
    let temp = tempfile::tempdir().unwrap();
    let cache_dir = temp.path().join("firmware_objects_ephemeral").join("fw-1");
    tokio::fs::create_dir_all(&cache_dir).await.unwrap();

    cleanup_ephemeral_object_cache_after_jobs(
        registry,
        vec!["missing-job".to_owned()],
        cache_dir.clone(),
    )
    .await;

    assert!(!cache_dir.exists());
}

// The detached tracked supervisor must still remove the cache directory once
// the child jobs reach a terminal state, even though the caller never awaits
// the join handle.
#[tokio::test]
async fn spawn_cleanup_removes_cache_after_child_terminal() {
    let registry = Arc::new(JobRegistry::<RmsJobDomain>::new(Duration::from_secs(3600)));
    let pending = registry
        .create_leaf(JobSpec::new(
            "rack-1",
            "switch-1",
            "Queued",
            tracing::Span::none(),
        ))
        .unwrap();
    let job_id = pending.id().to_string();

    let temp = tempfile::tempdir().unwrap();
    let cache_dir = temp.path().join("firmware_objects_ephemeral").join("fw-1");
    tokio::fs::create_dir_all(&cache_dir).await.unwrap();

    spawn_ephemeral_object_cache_cleanup(registry.clone(), vec![job_id.clone()], cache_dir.clone());
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(cache_dir.exists());

    pending.complete("done", "");
    wait_until_removed(&cache_dir, Duration::from_secs(1)).await;
    assert!(!cache_dir.exists());
}

async fn wait_until_removed(path: &Path, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while path.exists() {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn gb200_bf3_bfb_selection_is_rejected() {
    for node_type in [NodeType::ComputeGb200Nvidia, NodeType::ComputeGb200Wiwynn] {
        let error = component_filter_selection(
            node_type,
            &rm::FirmwareObjectComponentFilter {
                components: vec!["BF3_BFB".to_owned()],
            },
        )
        .expect_err("BF3_BFB must remain unsupported");

        assert!(error.contains("BF3_NIC"));
    }
}
