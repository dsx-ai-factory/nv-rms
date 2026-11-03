// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::config::hardware_profiles::HardwareProfileCatalog;
use crate::nodes::nvfwupd_adapter::firmware_target_selector;

#[tokio::test]
async fn inline_and_stored_apply_resolve_current_profile_without_cross_platform_cache_changes() {
    let temp = tempfile::tempdir().unwrap();
    let mut service = test_service();
    let manifest = local_manifest(temp.path(), "Prod").await;
    let stored_cache = temp.path().join("stored");
    // Import with the built-in profile, then apply using a changed target.
    // The stored artifact catalog must not freeze platform-specific destinations.
    let firmware = store_ready_manifest(&service, &manifest, &stored_cache).await;
    let profiles = temp.path().join("profiles");
    std::fs::create_dir(&profiles).unwrap();
    let yaml = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
    ));
    std::fs::write(
        profiles.join("gb200.yaml"),
        yaml.replace(
            "driver_default_targets: true",
            "request_targets: [/redfish/v1/Managers/Custom]",
        ),
    )
    .unwrap();
    service.hardware_profiles =
        Arc::new(HardwareProfileCatalog::load_directory(&profiles).unwrap());

    for (source, cache) in [
        (
            FirmwareSource::Inline(Ok(manifest)),
            temp.path().join("inline"),
        ),
        (FirmwareSource::Stored(firmware), stored_cache),
    ] {
        let apply = FirmwareApply {
            source,
            selections: Ok(HashMap::from([
                (
                    NodeType::ComputeGb200Nvidia,
                    FirmwareObjectComponentSelection::default_only(),
                ),
                (
                    NodeType::ComputeGb300Nvidia,
                    FirmwareObjectComponentSelection::default_only(),
                ),
            ])),
            channel: String::new(),
            force_update: false,
            access_token: None,
            rack_id: "rack-1".to_owned(),
        };
        let prepared = service
            .prepare_firmware_apply(&apply, &cache)
            .await
            .unwrap();
        assert_eq!(prepared.channel, "prod");
        let configured = &prepared.targets[&NodeType::ComputeGb200Nvidia];
        let other = &prepared.targets[&NodeType::ComputeGb300Nvidia];
        assert_eq!(configured.len(), 1);
        assert_eq!(other.len(), 1);
        assert_eq!(configured[0].component, "BMC");
        assert_eq!(
            configured[0].resolution.as_ref().unwrap().profile_target_id,
            "bmc"
        );
        assert_eq!(
            firmware_target_selector(&configured[0]),
            "/redfish/v1/Managers/Custom"
        );
        assert!(other[0].component.is_empty());
        assert_eq!(configured[0].firmware_file, other[0].firmware_file);
        assert_eq!(
            tokio::fs::read(&configured[0].firmware_file).await.unwrap(),
            b"firmware fixture"
        );
    }
}
