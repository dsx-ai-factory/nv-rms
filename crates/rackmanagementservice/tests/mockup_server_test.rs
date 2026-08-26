/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::io::Write;

use rackmanagementservice::domain::node::{
    FirmwareTarget, Node, NodeType, PowerOp, PowerState, PowerTargetType,
};
use rackmanagementservice::domain::rack::{
    Endpoint, EndpointConfig, EndpointCredentials, NodeConfig,
};
use rackmanagementservice::nodes::compute_gb200_nvidia::NvidiaGb200Compute;
use rackmanagementservice::nodes::compute_vrnvl72_nvidia::NvidiaVrnvl72Compute;
use rackmanagementservice::nodes::powershelf_gb200_liteon::PowershelfGb200Liteon;
use rackmanagementservice::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;

use redfish_test_support::RedfishSimulator;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
}

fn https_url(port: u16, path: &str) -> String {
    format!("https://127.0.0.1:{port}{path}")
}

fn compute_node(port: u16) -> rackmanagementservice::utilities::error::Result<NvidiaGb200Compute> {
    NvidiaGb200Compute::new(
        "compute-1".to_owned(),
        "rack-1".to_owned(),
        "127.0.0.1".to_owned(),
        port,
        "admin",
        "password",
        "00:11:22:33:44:55".to_owned(),
        Vec::new(),
        Vec::new(),
        true,
    )
}

fn vrnvl72_compute_node(
    port: u16,
) -> rackmanagementservice::utilities::error::Result<NvidiaVrnvl72Compute> {
    NvidiaVrnvl72Compute::from_config(
        &NodeConfig {
            id: "vr-compute-1".to_owned(),
            node_type: NodeType::ComputeVrnvl72Nvidia,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "127.0.0.1".to_owned(),
                    mac_address: "00:11:22:33:44:56".to_owned(),
                    port,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "password")),
                true,
            )),
            host_endpoint: None,
            expected_inventory: None,
        },
        "rack-1",
    )
}

fn powershelf_node(
    port: u16,
) -> rackmanagementservice::utilities::error::Result<PowershelfGb200Liteon> {
    PowershelfGb200Liteon::new(
        "powershelf-1".to_owned(),
        "rack-1".to_owned(),
        "127.0.0.1".to_owned(),
        port,
        "admin",
        "password",
        "00:11:22:33:44:66".to_owned(),
        true,
    )
}

fn switch_redfish_power_node(
    port: u16,
) -> rackmanagementservice::utilities::error::Result<SwitchGb200Nvidia> {
    SwitchGb200Nvidia::new_redfish_power(
        "switch-1".to_owned(),
        "rack-1".to_owned(),
        "127.0.0.1".to_owned(),
        port,
        "admin",
        "password",
        "00:11:22:33:44:77".to_owned(),
        true,
    )
}

#[tokio::test]
async fn mockup_server_starts_and_serves_json() {
    let port = portpicker::pick_unused_port().expect("no free port");
    let server = RedfishSimulator::builder()
        .add_gb200_compute(port)
        .start()
        .await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let resp = client
        .get(format!("https://127.0.0.1:{port}/redfish/v1/"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body.get("@odata.id").is_some());

    server.stop();
}

#[tokio::test]
async fn mockup_server_firmware_upload_and_task_poll() {
    let port = portpicker::pick_unused_port().expect("no free port");
    let server = RedfishSimulator::builder()
        .with_task_delay(0.1)
        .never_fail_firmware_updates()
        .add_gb200_compute(port)
        .start()
        .await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let form = reqwest::multipart::Form::new()
        .text("UpdateParameters", "{}")
        .text("UpdateFile", "fake firmware");

    let resp = client
        .post(format!(
            "https://127.0.0.1:{port}/redfish/v1/UpdateService/update-multipart"
        ))
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    let task_url = body["@odata.id"].as_str().unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let resp = client
        .get(format!("https://127.0.0.1:{port}{task_url}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["TaskState"], "Completed");

    server.stop();
}

#[tokio::test]
async fn mockup_server_failed_task_poll_returns_ok_with_exception_state() {
    let server = RedfishSimulator::builder()
        .with_task_delay(0.01)
        .always_fail_firmware_updates()
        .add_gb200_compute(0)
        .start()
        .await;

    let client = test_client();
    let port = server.ports()[0];
    let form = reqwest::multipart::Form::new()
        .text("UpdateParameters", "{}")
        .text("UpdateFile", "fake firmware");

    let resp = client
        .post(https_url(
            port,
            "/redfish/v1/UpdateService/update-multipart",
        ))
        .multipart(form)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let task_url = body["@odata.id"].as_str().unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let resp = client.get(https_url(port, task_url)).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["TaskState"], "Exception");

    server.stop();
}

#[tokio::test]
async fn mockup_server_endpoint_manifest_covers_src_redfish_paths() {
    let server = RedfishSimulator::builder()
        .add_gb200_compute(0)
        .add_powershelf(0)
        .start()
        .await;

    let client = test_client();
    let compute_port = server.ports()[0];
    let powershelf_port = server.ports()[1];

    let compute_get_paths = [
        "/redfish/v1/",
        "/redfish/v1/Systems/System_0",
        "/redfish/v1/Systems/HGX_Baseboard_0/Processors/GPU_0",
        "/redfish/v1/Systems/HGX_Baseboard_0/Processors/GPU_0/EnvironmentMetrics",
        "/redfish/v1/Systems/HGX_Baseboard_0/Processors/GPU_0/Oem/Nvidia/WorkloadPowerProfile",
        "/redfish/v1/Systems/HGX_Baseboard_0/Processors/CPU_0",
        "/redfish/v1/Systems/HGX_Baseboard_0/Processors/CPU_0/EnvironmentMetrics",
        "/redfish/v1/UpdateService/FirmwareInventory",
        "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0",
        "/redfish/v1/Chassis/Chassis_0",
        "/redfish/v1/Chassis/Chassis_0/EnvironmentMetrics",
        "/redfish/v1/Chassis/HGX_Chassis_0",
        "/redfish/v1/Chassis/HGX_Chassis_0/EnvironmentMetrics",
        "/redfish/v1/Chassis/HGX_ProcessorModule_0/EnvironmentMetrics",
        "/redfish/v1/Chassis/HGX_ProcessorModule_0/Assembly",
        "/redfish/v1/Managers/HGX_BMC_0",
    ];

    for path in compute_get_paths {
        let resp = client
            .get(https_url(compute_port, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "missing compute mock for {path}");
    }

    let powershelf_get_paths = [
        "/redfish/v1/Chassis/PowerShelf_0",
        "/redfish/v1/UpdateService/FirmwareInventory",
    ];

    for path in powershelf_get_paths {
        let resp = client
            .get(https_url(powershelf_port, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "missing powershelf mock for {path}");
    }

    server.stop();
}

#[tokio::test]
async fn mockup_server_validates_actions_and_updates_power_state() {
    let server = RedfishSimulator::builder()
        .add_gb200_compute(0)
        .add_powershelf(0)
        .start()
        .await;

    let client = test_client();
    let compute_port = server.ports()[0];
    let powershelf_port = server.ports()[1];

    let resp = client
        .post(https_url(
            compute_port,
            "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
        ))
        .json(&serde_json::json!({"ResetType": "ForceOff"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);

    let body: serde_json::Value = client
        .get(https_url(compute_port, "/redfish/v1/Systems/System_0"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["PowerState"], "Off");

    let resp = client
        .post(https_url(
            compute_port,
            "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
        ))
        .json(&serde_json::json!({"ResetType": "On"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);

    let body: serde_json::Value = client
        .get(https_url(compute_port, "/redfish/v1/Systems/System_0"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["PowerState"], "On");

    let resp = client
        .post(https_url(
            compute_port,
            "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
        ))
        .json(&serde_json::json!({"ResetType": "Invalid"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let resp = client
        .post(https_url(
            powershelf_port,
            "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
        ))
        .json(&serde_json::json!({"ForceOffType": "ForceOff"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);

    let body: serde_json::Value = client
        .get(https_url(
            powershelf_port,
            "/redfish/v1/Chassis/PowerShelf_0",
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["PowerState"], "Off");

    let resp = client
        .post(https_url(
            powershelf_port,
            "/redfish/v1/Chassis/powershelf/Actions/Chassis.On",
        ))
        .json(&serde_json::json!({"OnType": "On"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);

    let body: serde_json::Value = client
        .get(https_url(
            powershelf_port,
            "/redfish/v1/Chassis/PowerShelf_0",
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["PowerState"], "On");

    server.stop();
}

#[tokio::test]
async fn mockup_server_validates_post_patch_upload_and_task_collection() {
    let server = RedfishSimulator::builder()
        .with_task_delay(0.01)
        .add_gb200_compute(0)
        .add_powershelf(0)
        .start()
        .await;

    let client = test_client();
    let compute_port = server.ports()[0];
    let powershelf_port = server.ports()[1];

    let resp = client
        .post(https_url(
            compute_port,
            "/redfish/v1/Systems/HGX_Baseboard_0/Processors/GPU_0/Oem/Nvidia/WorkloadPowerProfile/Actions/NvidiaWorkloadPower.EnableProfiles",
        ))
        .json(&serde_json::json!({"ProfileMask": "0x1"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);

    let resp = client
        .post(https_url(
            compute_port,
            "/redfish/v1/Systems/HGX_Baseboard_0/Processors/GPU_0/Oem/Nvidia/WorkloadPowerProfile/Actions/NvidiaWorkloadPower.EnableProfiles",
        ))
        .json(&serde_json::json!({"ProfileMask": ""}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let resp = client
        .patch(https_url(
            compute_port,
            "/redfish/v1/Systems/HGX_Baseboard_0/Processors/GPU_0/EnvironmentMetrics",
        ))
        .json(&serde_json::json!({"PowerLimitWatts": {"SetPoint": 700}}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);

    let resp = client
        .patch(https_url(
            compute_port,
            "/redfish/v1/Systems/HGX_Baseboard_0/Processors/GPU_0/EnvironmentMetrics",
        ))
        .json(&serde_json::json!({"PowerLimitWatts": {"SetPoint": 1}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let resp = client
        .patch(https_url(powershelf_port, "/redfish/v1/UpdateService"))
        .json(&serde_json::json!({
            "HttpPushUriOptions": {
                "HttpPushUriApplyTime": {
                    "ApplyTime": "OnReset"
                }
            }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204);

    let resp = client
        .patch(https_url(powershelf_port, "/redfish/v1/UpdateService"))
        .json(&serde_json::json!({
            "HttpPushUriOptions": {
                "HttpPushUriApplyTime": {
                    "ApplyTime": "AtMaintenanceWindowStart"
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let resp = client
        .post(https_url(compute_port, "/redfish/v1/Unknown/Actions/Nope"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let resp = client
        .post(https_url(
            compute_port,
            "/redfish/v1/UpdateService/update-multipart",
        ))
        .body("not multipart")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 415);

    let form = reqwest::multipart::Form::new()
        .text("UpdateParameters", "{}")
        .text("UpdateFile", "fake firmware");
    let resp = client
        .post(https_url(
            compute_port,
            "/redfish/v1/UpdateService/update-multipart",
        ))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    let task_url = body["@odata.id"].as_str().unwrap();

    let body: serde_json::Value = client
        .get(https_url(compute_port, "/redfish/v1/TaskService/Tasks"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["Members@odata.count"], 1);

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let resp = client
        .get(https_url(compute_port, task_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["TaskState"], "Completed");
    assert!(body.get("StartTime").is_some());

    let resp = client
        .put(https_url(
            powershelf_port,
            "/redfish/v1/UpdateService/update",
        ))
        .header("Content-Type", "application/octet-stream")
        .body("fake firmware")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    let resp = client
        .post(https_url(
            powershelf_port,
            "/redfish/v1/UpdateService/update",
        ))
        .header("Content-Type", "application/octet-stream")
        .body("fake firmware")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    server.stop();
}

#[tokio::test]
async fn src_power_nodes_redfish_calls_work_against_mock_server() -> TestResult {
    let server = RedfishSimulator::builder()
        .add_gb200_compute(0)
        .add_powershelf(0)
        .start()
        .await;

    let compute = compute_node(server.ports()[0])?;
    let switch = switch_redfish_power_node(server.ports()[0])?;
    let powershelf = powershelf_node(server.ports()[1])?;

    switch
        .set_power_state(PowerOp::ForceOff, PowerTargetType::System)
        .await?;
    assert_eq!(compute.get_power_state().await?, PowerState::Off);

    switch
        .set_power_state(PowerOp::On, PowerTargetType::System)
        .await?;
    assert_eq!(compute.get_power_state().await?, PowerState::On);

    powershelf
        .set_power_state(PowerOp::ForceOff, PowerTargetType::System)
        .await?;
    assert_eq!(powershelf.get_power_state().await?, PowerState::Off);

    powershelf
        .set_power_state(PowerOp::On, PowerTargetType::System)
        .await?;
    assert_eq!(powershelf.get_power_state().await?, PowerState::On);

    server.stop();
    Ok(())
}

#[tokio::test]
async fn vrnvl72_hmc_power_cycle_uses_manager_reset_flow() -> TestResult {
    let server = RedfishSimulator::builder()
        .add_gb200_compute(0)
        .start()
        .await;
    let port = server.ports()[0];
    let compute = vrnvl72_compute_node(port)?;

    compute
        .set_power_state(PowerOp::PowerCycle, PowerTargetType::HMC)
        .await?;

    let client = test_client();
    let root = client.get(https_url(port, "/redfish/v1/")).send().await?;
    assert_eq!(root.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    let system: serde_json::Value = client
        .get(https_url(port, "/redfish/v1/Systems/System_0"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(system["PowerState"], "On");

    server.stop();
    Ok(())
}

#[tokio::test]
async fn compute_firmware_uploads_work_against_mock_server() -> TestResult {
    let server = RedfishSimulator::builder()
        .with_task_delay(0.01)
        .add_gb200_compute(0)
        .start()
        .await;

    let compute = compute_node(server.ports()[0])?;
    let mut firmware = tempfile::NamedTempFile::new()?;
    firmware.write_all(b"fake firmware")?;
    let firmware_file = firmware.path().to_string_lossy().into_owned();

    let compute_target = FirmwareTarget {
        component: "BMC".to_owned(),
        firmware_file,
        expected_version: None,
    };
    let compute_task_id = compute.start_firmware_upload(&compute_target, true).await?;

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let compute_status = compute.poll_firmware_task(&compute_task_id).await?;
    assert!(compute_status.completed);
    assert_eq!(compute_status.state, "Completed");

    server.stop();
    Ok(())
}

#[tokio::test]
async fn mockup_server_config_endpoint() {
    let port = portpicker::pick_unused_port().expect("no free port");
    let server = RedfishSimulator::builder()
        .add_gb200_compute(port)
        .start()
        .await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let config_url = format!("https://127.0.0.1:{port}/bmc-sim/config");

    let resp = client
        .post(config_url.as_str())
        .json(&serde_json::json!({"delay_seconds": 0.5, "failure_rate": 0.0}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["delay_seconds"], 0.5);

    let resp = client
        .post(config_url.as_str())
        .json(&serde_json::json!({"delay_seconds": 0.0}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["delay_seconds"], 0.0);

    let resp = client
        .post(config_url.as_str())
        .json(&serde_json::json!({"delay_seconds": -0.1}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);

    let resp = client
        .post(config_url.as_str())
        .json(&serde_json::json!({"failure_rate": 1.1}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);

    server.stop();
}

#[tokio::test]
async fn mockup_server_404_for_unknown_path() {
    let port = portpicker::pick_unused_port().expect("no free port");
    let server = RedfishSimulator::builder()
        .add_gb200_compute(port)
        .start()
        .await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let resp = client
        .get(format!("https://127.0.0.1:{port}/nonexistent/path"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);

    server.stop();
}

#[tokio::test]
async fn mockup_server_multi_port() {
    let port1 = portpicker::pick_unused_port().expect("no free port");
    let port2 = portpicker::pick_unused_port().expect("no free port");

    let server = RedfishSimulator::builder()
        .add_gb200_compute(port1)
        .add_gb200_compute(port2)
        .start()
        .await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let resp1 = client
        .get(format!("https://127.0.0.1:{port1}/redfish/v1/"))
        .send()
        .await
        .unwrap();
    let resp2 = client
        .get(format!("https://127.0.0.1:{port2}/redfish/v1/"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp1.status(), 200);
    assert_eq!(resp2.status(), 200);

    server.stop();
}
