// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::config::hardware_profiles::HardwareProfileCatalog;
use librms::protos::rack_manager::rack_manager_server::RackManager as RackManagerRpc;

const GB200: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
));

struct RecordingUpdateNode {
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Node for RecordingUpdateNode {
    fn id(&self) -> &str {
        "compute-1"
    }
    fn rack_id(&self) -> &str {
        "rack-01"
    }
    fn node_type(&self) -> NodeType {
        NodeType::ComputeGb200Nvidia
    }
    fn get_info(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        _force: bool,
        _options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let destination = crate::nodes::nvfwupd_adapter::firmware_target_selector(target);
        self.events.lock().unwrap().push(destination.to_owned());
        Ok(completed_update_outcome())
    }
}

#[tokio::test]
async fn firmware_update_uses_configured_targets_and_reports_profile_provenance() {
    let temporary = tempfile::tempdir().unwrap();
    let profile_path = temporary.path().join("gb200.yaml");
    let configured = GB200.replace("HGX_Chassis_0", "HGX_Custom_0").replace(
        "driver_default_targets: true",
        "request_targets: [/redfish/v1/Managers/CustomBMC]",
    );
    std::fs::write(&profile_path, &configured).unwrap();
    let catalog = Arc::new(HardwareProfileCatalog::load_directory(temporary.path()).unwrap());
    let (manager, rack) = rack_manager_with_rack().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    rack.add_node(
        "compute-1",
        Arc::new(NodeInstance::from_test_node(RecordingUpdateNode {
            events: Arc::clone(&events),
        })),
    )
    .unwrap();
    let mut service = test_service(manager);
    service.hardware_profiles = catalog;
    service.firmware_dir = temporary.path().to_path_buf();
    std::fs::write(temporary.path().join("test.fwpkg"), b"fixture").unwrap();
    let request = rm::UpdateFirmwareRequest {
        rack_id: "rack-01".to_owned(),
        node_id: "compute-1".to_owned(),
        firmware_targets: vec![
            rm::FirmwareTarget {
                target: "HMC".to_owned(),
                filename: "test.fwpkg".to_owned(),
            },
            rm::FirmwareTarget {
                target: "BMC".to_owned(),
                filename: "test.fwpkg".to_owned(),
            },
        ],
        activate: false,
        force_update: true,
        ..Default::default()
    };
    let response = service
        .update_firmware(tonic::Request::new(request))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.status,
        rm::ReturnCode::Success as i32,
        "{}",
        response.message
    );
    let finished = wait_for_firmware_job_terminal(&service.job_tracker, &response.job_id).await;
    assert_eq!(
        finished.state,
        crate::orchestrator::job_lifecycle::JobState::Completed,
        "{}",
        finished.error_message
    );
    assert_eq!(
        *events.lock().unwrap(),
        [
            "/redfish/v1/Chassis/HGX_Custom_0",
            "/redfish/v1/Managers/CustomBMC",
        ]
    );
    let result: serde_json::Value = serde_json::from_str(&finished.result_json).unwrap();
    let expected_hash = HardwareUpdatePlan::from_yaml_str(&configured)
        .unwrap()
        .source_sha256()
        .to_owned();
    assert_eq!(result["hardware_config_source"], "profile");
    assert_eq!(result["hardware_profile_sha256"], expected_hash);
    assert_eq!(result["hardware_profile_schema_version"], 1);
    assert_eq!(result["hardware_profile"], "compute_gb200_nvidia");
}
