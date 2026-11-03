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

use super::*;
use std::time::Duration;

use crate::domain::rack::{Endpoint, EndpointCredentials};
use crate::nodes::NodeInstance;
use crate::utilities::error::ErrorCode;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The VR node is the one that waits for NVUE after a BMC activation: a 503
/// while NVOS boots is the retryable code, the first 2xx ends the wait.
#[tokio::test]
async fn management_api_probe_reports_a_booting_nvue_then_answers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;
    let node = SwitchVrnvl72Nvidia::for_test(&server.uri());

    let booting = Node::verify_management_api_reachable(&node)
        .await
        .unwrap_err();
    assert_eq!(booting.code, ErrorCode::Unavailable);

    Node::verify_management_api_reachable(&node)
        .await
        .expect("the first 2xx ends the wait");
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

fn vr_config() -> NodeConfig {
    let endpoint = |ip: &str, username: &str, password: &str| {
        EndpointConfig::with_credentials(
            Endpoint {
                ip_address: ip.to_owned(),
                mac_address: "00:11:22:33:44:55".to_owned(),
                port: 443,
                host_name: None,
            },
            Some(EndpointCredentials::new(username, password)),
            false,
        )
    };
    NodeConfig {
        id: "vr-switch".to_owned(),
        node_type: NodeType::SwitchVrnvl72Nvidia,
        bmc_endpoint: Some(endpoint("192.0.2.10", "bmc-user", "bmc-secret")),
        host_endpoint: Some(endpoint("192.0.2.11", "nvos-user", "nvos-secret")),
        expected_inventory: None,
        recovery: None,
        runtime_config: None,
    }
}

#[test]
fn vr_wrapper_preserves_identity_profile_and_endpoint_credentials() {
    let config = vr_config();
    let node = NodeInstance::from_config(&config, "vr-rack").unwrap();
    let NodeInstance::SwitchVrnvl72Nvidia(switch) = &node else {
        panic!("VR must use its concrete node implementation");
    };
    assert_eq!(node.id(), "vr-switch");
    assert_eq!(node.rack_id(), "vr-rack");
    assert_eq!(node.node_type(), NodeType::SwitchVrnvl72Nvidia);
    assert_eq!(node.get_info()["type"], "switch_vrnvl72_nvidia");
    assert!(node.as_switch_firmware().is_some());
    assert!(node.nvue_client().is_some());
    assert!(node.supports_bmc_aux_powercycle());
    assert!(node.recovery().is_none());
    assert!(switch.system_image_requires_bmc_activation());

    let (_, bmc) = switch.bmc_workflow().unwrap();
    let nvos = switch.inner.nvfwupd_target_config().unwrap();
    for (target, ip, username, password) in [
        (bmc, "192.0.2.10", "bmc-user", "bmc-secret"),
        (nvos, "192.0.2.11", "nvos-user", "nvos-secret"),
    ] {
        assert_eq!(
            target.server_type,
            nvfwupd::workflow::ServerType::VRNVL72Switch
        );
        assert_eq!(target.ip, ip);
        assert_eq!(target.username, username);
        assert_eq!(target.password, password);
        assert!(target.verify_tls);
        assert!(!format!("{target:?}").contains(password));
    }

    // Shared-only capabilities must keep VR identity and their host transport.
    let password = NodeInstance::create_switch_password(&config, "vr-rack").unwrap();
    assert_eq!(password.node_type(), NodeType::SwitchVrnvl72Nvidia);
    assert!(password.optional_nvue_client().is_some());
    let fabric = NodeInstance::create_switch_scale_up(&config, "vr-rack").unwrap();
    assert_eq!(fabric.node_type(), NodeType::SwitchVrnvl72Nvidia);
    assert!(fabric.nvue_client().is_some());
    assert_eq!(
        node.require_nvidia_switch().unwrap().node_type(),
        NodeType::SwitchVrnvl72Nvidia
    );
}

#[test]
fn constructors_reject_incompatible_switch_policy() {
    let mut config = vr_config();
    assert!(SwitchGb200Nvidia::from_config(&config, "rack").is_err());
    for node_type in [
        NodeType::SwitchGb200Nvidia,
        NodeType::SwitchGb300Nvidia,
        NodeType::ComputeVrnvl72Nvidia,
    ] {
        config.node_type = node_type;
        assert!(SwitchVrnvl72Nvidia::from_config(&config, "rack").is_err());
    }
}

#[tokio::test]
async fn vr_system_image_recovery_delegates_shared_operations() {
    let nvos = MockServer::start().await;
    let bmc = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "System is ready", "health-status": "OK"
        })))
        .expect(1)
        .mount(&nvos)
        .await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .and(body_json(serde_json::json!({
            "state": "save", "auto-prompt": {"ays": "ays_yes"}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&nvos)
        .await;

    let directory = tempfile::tempdir().unwrap();
    let image = directory.path().join("nvos.bin");
    let image_size = crate::nodes::switch_gb200_nvidia::config::MIN_FIRMWARE_FILE_SIZE;
    std::fs::write(&image, vec![0_u8; image_size as usize]).unwrap();
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let upload_events = events.clone();
    let exec_events = events.clone();
    let mut switch = SwitchVrnvl72Nvidia {
        inner: SwitchGb200Nvidia::for_test(&nvos.uri())
            .with_node_type_for_test(NodeType::SwitchVrnvl72Nvidia)
            .with_redfish_for_test(&bmc.uri())
            .with_host_credentials_for_test("admin", "requested-password")
            .with_scp_upload_for_test(move |_, remote_path| {
                assert_eq!(remote_path, "/tmp/nvos.bin");
                upload_events.lock().unwrap().push("scp");
                Ok(())
            })
            .with_ssh_exec_for_test(move |command| {
                let (event, output) = if command == "wc -c < '/tmp/nvos.bin'" {
                    ("confirm", image_size.to_string())
                } else if command.contains("onie-nos-install '/tmp/nvos.bin'") {
                    ("install", String::new())
                } else {
                    assert_eq!(command, "command -v nv >/dev/null 2>&1");
                    ("verify", String::new())
                };
                exec_events.lock().unwrap().push(event);
                Ok(output)
            }),
    };
    let onie = EndpointConfig::with_credentials(
        Endpoint {
            ip_address: "192.0.2.50".to_owned(),
            port: 22,
            mac_address: String::new(),
            host_name: None,
        },
        Some(EndpointCredentials::new("onie", "recovery-password")),
        false,
    );
    let cancel = CancellationToken::new();

    tokio::time::timeout(Duration::from_secs(5), async {
        assert_eq!(
            switch.verify_nvos_installation(None).await.unwrap(),
            NvosInstallationState::Installed
        );
        assert_eq!(
            switch.require_certificate_install_node().unwrap().node_id(),
            switch.id()
        );
        switch
            .stage_nvos_recovery_image(
                &onie,
                image.to_str().unwrap(),
                "nvos.bin",
                SshUploadOptions::default(),
                &cancel,
            )
            .await
            .unwrap();
        switch
            .install_staged_nvos_image_and_wait(&onie, "nvos.bin", Duration::from_secs(1), &cancel)
            .await
            .unwrap();
        switch.save_current_applied_config().await.unwrap();
    })
    .await
    .expect("delegated recovery operations must not lock recursively");

    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["scp", "confirm", "install", "verify"]
    );
    assert_eq!(switch.node_type(), NodeType::SwitchVrnvl72Nvidia);
    assert!(switch.system_image_requires_bmc_activation());
    assert!(bmc.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn vr_firmware_file_listing_still_uses_nvue() {
    let nvos = MockServer::start().await;
    let bmc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/firmware/BMC/files"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"bmc.bin": {}})))
        .expect(1)
        .mount(&nvos)
        .await;
    let node = NodeInstance::SwitchVrnvl72Nvidia(Box::new(
        SwitchVrnvl72Nvidia::for_test(&nvos.uri()).with_redfish_for_test(&bmc.uri()),
    ));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        node.as_switch_firmware()
            .unwrap()
            .list_firmware("BMC", true),
    )
    .await
    .expect("file listing must not lock recursively")
    .unwrap();
    assert!(result.get("bmc.bin").is_some());
    assert!(bmc.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn vr_inventory_failure_does_not_fall_back_to_nvue() {
    let nvos = MockServer::start().await;
    let bmc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&bmc)
        .await;
    let node = NodeInstance::SwitchVrnvl72Nvidia(Box::new(
        SwitchVrnvl72Nvidia::for_test(&nvos.uri()).with_redfish_for_test(&bmc.uri()),
    ));
    assert!(node.get_firmware_inventory().await.is_err());
    assert!(nvos.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn system_image_install_disables_automatic_reboot_only_for_vrnvl72() {
    for node_type in [
        NodeType::SwitchVrnvl72Nvidia,
        NodeType::SwitchGb200Nvidia,
        NodeType::SwitchGb300Nvidia,
    ] {
        let nvos = MockServer::start().await;
        let bmc = MockServer::start().await;
        let mut parameters = serde_json::json!({"force": true, "image-file": "nvos.bin"});
        if node_type == NodeType::SwitchVrnvl72Nvidia {
            parameters["reboot"] = serde_json::json!("no");
        }
        Mock::given(method("POST"))
            .and(path("/nvue_v1/system/image/files/nvos.bin"))
            .and(body_json(
                serde_json::json!({"@install": {"state": "start", "parameters": parameters}}),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"path": "/action/10"})),
            )
            .expect(1)
            .mount(&nvos)
            .await;
        let switch: Box<dyn SwitchSystemImageNode> = match node_type {
            NodeType::SwitchVrnvl72Nvidia => Box::new(
                SwitchVrnvl72Nvidia::for_test(&nvos.uri()).with_redfish_for_test(&bmc.uri()),
            ),
            _ => Box::new(
                SwitchGb200Nvidia::for_test(&nvos.uri())
                    .with_node_type_for_test(node_type)
                    .with_redfish_for_test(&bmc.uri()),
            ),
        };
        assert_eq!(switch.install_system_image("nvos.bin").await.unwrap(), "10");
        assert!(
            bmc.received_requests().await.unwrap().is_empty(),
            "install must not reset the BMC"
        );
    }
}

#[tokio::test]
async fn vrnvl72_system_image_install_requires_bmc_before_posting() {
    let nvos = MockServer::start().await;
    let switch = SwitchVrnvl72Nvidia::for_test(&nvos.uri());
    let error = switch.install_system_image("nvos.bin").await.unwrap_err();
    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(nvos.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn vrnvl72_inventory_and_component_listing_use_bmc_credentials_without_nvue() {
    let nvos = MockServer::start().await;
    let bmc = MockServer::start().await;
    let root = "/redfish/v1/UpdateService/FirmwareInventory";
    Mock::given(method("GET"))
        .and(path(root))
        .and(header(
            "Authorization",
            "Basic dGVzdC1ibWMtdXNlcm5hbWU6dGVzdC1ibWMtcGFzc3dvcmQ=",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Members":[{"@odata.id":format!("{root}/MGX_FW_BMC_0")}]
        })))
        .mount(&bmc)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{root}/MGX_FW_BMC_0")))
        .and(header(
            "Authorization",
            "Basic dGVzdC1ibWMtdXNlcm5hbWU6dGVzdC1ibWMtcGFzc3dvcmQ=",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Id":"MGX_FW_BMC_0", "Version":"88.0060.0451", "Updateable":true
        })))
        .mount(&bmc)
        .await;
    {
        // Drive the same capability dispatch as registered inventory-backed RPCs.
        let switch = NodeInstance::SwitchVrnvl72Nvidia(Box::new(SwitchVrnvl72Nvidia {
            inner: SwitchGb200Nvidia::for_test(&nvos.uri())
                .with_node_type_for_test(NodeType::SwitchVrnvl72Nvidia)
                .with_redfish_for_test(&bmc.uri())
                .without_host_endpoint_for_test(),
        }));
        let inventory = switch.get_firmware_inventory().await.unwrap();
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].name, "MGX_FW_BMC_0");
        assert_eq!(inventory[0].version, "88.0060.0451");
        let listing = tokio::time::timeout(
            Duration::from_secs(5),
            switch
                .as_switch_firmware()
                .unwrap()
                .list_firmware("BMC", false),
        )
        .await
        .expect("registered firmware listing must not lock recursively")
        .unwrap();
        assert_eq!(
            listing[format!("{root}/MGX_FW_BMC_0")]["Version"],
            "88.0060.0451"
        );
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let error = switch
            .activate_firmware_with(FirmwareActivationRequest {
                mode: FirmwareActivationMode::SwitchPowerCycle,
                cancellation: Some(cancel),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Cancelled);
    }
    assert!(nvos.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn vrnvl72_bmc_operations_reject_missing_credentials_without_touching_nvue() {
    let nvos = MockServer::start().await;
    let switch = SwitchVrnvl72Nvidia::for_test(&nvos.uri());
    for result in [
        switch.get_firmware_inventory().await.map(|_| ()),
        switch
            .verify_firmware_package_versions(&[])
            .await
            .map(|_| ()),
        switch.activate_firmware().await,
    ] {
        let error = result.unwrap_err();
        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert!(error.message.contains("BMC endpoint and credentials"));
    }
    assert!(nvos.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn vrnvl72_get_power_state_without_bmc_fails_without_calling_nvos() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(401))
        .expect(0)
        .mount(&server)
        .await;

    let sw = SwitchVrnvl72Nvidia::for_test(&server.uri());
    let error = sw.get_power_state().await.unwrap_err();
    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(error.message.contains("BMC endpoint and credentials"));
}

#[tokio::test]
async fn vrnvl72_power_cycle_fails_without_bmc_instead_of_using_nvue()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/system"))
        .and(body_json(serde_json::json!({
            "@power-cycle": {
                "state": "start",
                "parameters": {"force": true}
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchVrnvl72Nvidia::for_test(&server.uri());
    let error = switch
        .set_power_state(PowerOp::PowerCycle, PowerTargetType::System)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    Ok(())
}
