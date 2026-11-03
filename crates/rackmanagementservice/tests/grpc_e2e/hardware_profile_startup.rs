// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use rackmanagementservice::config::hardware_profiles::HardwareProfileCatalog;
use rackmanagementservice::config::hardware_profiles::HardwareUpdatePlan;

#[tokio::test]
async fn firmware_preparation_failures_report_profile_or_code_default_provenance() {
    let yaml = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
    ));
    let customized = yaml.replace("HGX_Chassis_0", "HGX_Custom_0");
    let expected_hash = HardwareUpdatePlan::from_yaml_str(&customized)
        .unwrap()
        .source_sha256()
        .to_owned();
    for (source, contents) in [
        ("profile", Some(customized)),
        ("code_defaults", None),
        (
            "code_defaults",
            Some(yaml.replacen("enabled: true", "enabled: false", 1)),
        ),
    ] {
        let profiles = TempDir::new().unwrap();
        let firmware = TempDir::new().unwrap();
        if let Some(contents) = contents {
            std::fs::write(profiles.path().join("gb200.yaml"), contents).unwrap();
        }
        let (mut server, mut client) =
            start_profile_server(profiles.path(), firmware.path(), Backends::memory()).await;
        let response = client
            .apply_firmware_object(ApplyFirmwareObjectRequest {
                rack_id: "rack-01".into(),
                config_json: "not-json".into(),
                hardware_type: "gb200".into(),
                nodes: Some(NodeSet {
                    nodes: vec![compute_node("compute-01", "rack-01", 1)],
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.jobs.len(), 1);
        let child = poll_job_until_terminal_within(
            &mut client,
            &response.jobs[0].job_id,
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(child.job_state, FirmwareJobState::Failed as i32);
        let result: serde_json::Value = serde_json::from_str(&child.result_json).unwrap();
        assert_eq!(result["hardware_config_source"], source);
        assert_eq!(result["node_type"], "compute_gb200_nvidia");
        assert!(result["rms_build"]["git_sha"].is_string());
        if source == "profile" {
            assert_eq!(result["hardware_profile"], "compute_gb200_nvidia");
            assert_eq!(result["hardware_profile_sha256"], expected_hash);
            assert_eq!(result["hardware_profile_schema_version"], 1);
        } else {
            for key in [
                "hardware_profile",
                "hardware_profile_sha256",
                "hardware_profile_schema_version",
            ] {
                assert!(result.get(key).is_none(), "{key}: {result}");
            }
        }
        server.stop_and_await_terminal().await;
    }
}

fn bmc_target(object: &FirmwareObject) -> &str {
    &object
        .metadata
        .as_ref()
        .unwrap()
        .device_components
        .iter()
        .find(|device| device.node_type == NodeType::ComputeGb200Nvidia as i32)
        .unwrap()
        .components
        .iter()
        .find(|component| component.name == "BMC")
        .unwrap()
        .artifacts[0]
        .target
}

async fn start_profile_server(
    profiles: &Path,
    firmware: &Path,
    backends: Backends,
) -> (GrpcServer, RackManagerClient<tonic::transport::Channel>) {
    let catalog = HardwareProfileCatalog::load_directory(profiles).unwrap();
    let mut server = GrpcServer::new(
        0,
        Arc::new(RackManager::new()),
        Arc::new(JobTracker::new()),
        backends,
    )
    .with_firmware_dir(firmware)
    .with_insecure_listener()
    .with_hardware_profiles(catalog);
    let (incoming, port) = server.bind_localhost_ephemeral_port().unwrap();
    server.start_with_bound_incoming(incoming).await.unwrap();
    let client = RackManagerClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .unwrap();
    (server, client)
}

#[tokio::test]
async fn profile_changes_take_effect_after_server_restart() {
    let profiles = TempDir::new().unwrap();
    let yaml = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
    ));
    let path = profiles.path().join("gb200.yaml");
    std::fs::write(&path, yaml).unwrap();
    let firmware = TempDir::new().unwrap();
    let backends = Backends::memory();
    let (mut server, mut client) =
        start_profile_server(profiles.path(), firmware.path(), backends.clone()).await;
    let source = firmware.path().join("source.fwpkg");
    std::fs::write(&source, b"metadata fixture").unwrap();
    let original = client
        .add_firmware_object(AddFirmwareObjectRequest {
            hardware_type: "gb200".into(),
            config_json: firmware_object_release_catalog_json(
                "startup-profiles",
                serde_json::json!([{
                    "Type": "Compute Tray", "Components": {"Firmware": [{
                        "Component": "BMC", "Type": "Prod", "Locations": [{
                            "Location": source, "LocationType": "file", "Type": "Firmware"
                        }]
                    }]}
                }]),
            ),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .object
        .unwrap();
    assert_eq!(bmc_target(&original), "");
    let replacement = yaml.replace(
        "driver_default_targets: true",
        "request_targets: [/redfish/v1/Managers/AfterRestart]",
    );
    server.stop_and_await_terminal().await;
    std::fs::write(&path, replacement).unwrap();
    let (mut restarted, mut client) =
        start_profile_server(profiles.path(), firmware.path(), backends.clone()).await;
    let updated = client
        .get_firmware_object(GetFirmwareObjectRequest {
            id: original.id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .object
        .unwrap();
    assert_eq!(bmc_target(&updated), "/redfish/v1/Managers/AfterRestart");
    restarted.stop_and_await_terminal().await;

    // Removing or disabling the override restores code defaults on the next restart.
    for contents in [
        None,
        Some(yaml.replacen("enabled: true", "enabled: false", 1)),
    ] {
        match contents {
            Some(contents) => std::fs::write(&path, contents).unwrap(),
            None => std::fs::remove_file(&path).unwrap(),
        }
        let (mut fallback, mut client) =
            start_profile_server(profiles.path(), firmware.path(), backends.clone()).await;
        let restored = client
            .get_firmware_object(GetFirmwareObjectRequest {
                id: original.id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .object
            .unwrap();
        assert_eq!(bmc_target(&restored), "");
        fallback.stop_and_await_terminal().await;
    }
}
