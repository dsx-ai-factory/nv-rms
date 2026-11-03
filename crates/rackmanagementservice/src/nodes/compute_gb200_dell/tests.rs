// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::domain::rack::EndpointCredentials;
use crate::nodes::compute_verification_tests::HttpsInventory;
use crate::utilities::error::ErrorCode;
use std::sync::{Arc, Mutex};
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, ResponseTemplate};

fn config(port: u16) -> NodeConfig {
    let mut config = NodeConfig::new("dell-01", NodeType::ComputeGb200Dell, "127.0.0.1");
    let endpoint = config.bmc_endpoint.as_mut().unwrap();
    endpoint.endpoint.port = port;
    endpoint.dangerously_accept_invalid_certs = true;
    endpoint.credentials = Some(EndpointCredentials::new("admin", "test-password"));
    config
}

fn target(component: &str, filename: &str) -> FirmwareTarget {
    FirmwareTarget {
        component: component.into(),
        firmware_file: filename.into(),
        ..Default::default()
    }
}

/// Resolve like the request handler does when no operator override is enabled.
fn resolved_target(target: &FirmwareTarget) -> Result<FirmwareTarget> {
    let profile = crate::config::hardware_profiles::built_in_profile(NodeType::ComputeGb200Dell)
        .expect("embedded Dell GB200 hardware profile");
    let configured = profile
        .stages()
        .iter()
        .flat_map(|stage| stage.targets())
        .find(|configured| configured.matches_component(&target.component))
        .ok_or_else(|| RmsError::invalid_argument("unsupported Dell GB200 component"))?;
    configured.resolve(target)
}

fn resolved(component: &str, filename: &str) -> FirmwareTarget {
    resolved_target(&target(component, filename)).unwrap()
}

async fn capture_server() -> HttpsInventory {
    let server = HttpsInventory::start().await;
    let responses: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(include_str!("../../../tests/fixtures/dell_gb200_ami.json")).unwrap();
    for (uri, response) in responses {
        Mock::given(method("GET"))
            .and(path(&uri))
            .respond_with(ResponseTemplate::new(200).set_body_json(response.clone()))
            .mount(&server.server)
            .await;
        if uri == "/redfish/v1/" {
            Mock::given(method("GET"))
                .and(path("/redfish/v1"))
                .respond_with(ResponseTemplate::new(200).set_body_json(response))
                .mount(&server.server)
                .await;
        }
    }
    server
}

fn captured_update_service() -> serde_json::Value {
    let responses: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(include_str!("../../../tests/fixtures/dell_gb200_ami.json")).unwrap();
    responses["/redfish/v1/UpdateService"].clone()
}

fn preserved_update_service() -> serde_json::Value {
    let mut service = captured_update_service();
    let preserve = &mut service["Oem"]["AMIUpdateService"]["PreserveConfiguration"];
    preserve["Authentication"] = json!(true);
    preserve["IPMI"] = json!(true);
    preserve["Network"] = json!(true);
    preserve["REDFISH"] = json!(true);
    service
}

async fn allow_force_updates(server: &HttpsInventory, initial: Value) -> Arc<Mutex<Value>> {
    let state = Arc::new(Mutex::new(initial));
    let get_state = state.clone();
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(move |_: &wiremock::Request| {
            ResponseTemplate::new(200).set_body_json(get_state.lock().unwrap().clone())
        })
        .with_priority(1)
        .mount(&server.server)
        .await;
    let patch_state = state.clone();
    Mock::given(method("PATCH"))
        .and(path(UPDATE_SERVICE))
        .and(header("If-Match", "*"))
        .and(|request: &wiremock::Request| {
            request
                .body_json::<Value>()
                .is_ok_and(|body| body.pointer(FORCE_UPDATE).is_some_and(Value::is_boolean))
        })
        .respond_with(move |request: &wiremock::Request| {
            let body: Value = request.body_json().unwrap();
            assert_eq!(
                body,
                json!({"HttpPushUriOptions": {"ForceUpdate": body.pointer(FORCE_UPDATE).unwrap()}})
            );
            patch_state.lock().unwrap()["HttpPushUriOptions"]["ForceUpdate"] =
                body.pointer(FORCE_UPDATE).unwrap().clone();
            ResponseTemplate::new(204)
        })
        .mount(&server.server)
        .await;
    state
}

async fn allow_bmc_preservation(
    server: &HttpsInventory,
    initial: serde_json::Value,
) -> Arc<Mutex<Value>> {
    let state = allow_force_updates(server, initial).await;
    let patch_state = state.clone();
    let retained = preserved_update_service();
    Mock::given(method("PATCH"))
        .and(path("/redfish/v1/UpdateService"))
        .and(header("If-Match", "*"))
        .and(body_json(json!({"Oem": {"AMIUpdateService": {
            "@odata.type": "#AMIUpdateService.v1_0_0.AMIUpdateService",
            "PreserveConfiguration": retained["Oem"]["AMIUpdateService"]["PreserveConfiguration"],
        }}})))
        .respond_with(move |_: &wiremock::Request| {
            patch_state.lock().unwrap()["Oem"] = retained["Oem"].clone();
            ResponseTemplate::new(204)
        })
        .expect(1)
        .mount(&server.server)
        .await;
    state
}

async fn allow_upload_and_task(server: &HttpsInventory, initial_task: &str) -> Arc<Mutex<String>> {
    Mock::given(method("POST"))
        .and(path("/redfish/v1/UpdateService/upload"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "@odata.id": "/redfish/v1/TaskService/Tasks/144", "Id": "144", "TaskState": "Running", "TaskStatus": "OK"
        })).insert_header("Location", "/redfish/v1/TaskService/Tasks/144"))
        .mount(&server.server).await;
    let state = Arc::new(Mutex::new(initial_task.to_owned()));
    let task_state = state.clone();
    Mock::given(method("GET"))
        .and(path("/redfish/v1/TaskService/Tasks/144"))
        .respond_with(move |_: &wiremock::Request| {
            let state = task_state.lock().unwrap();
            ResponseTemplate::new(200).set_body_json(json!({
                "Id": "144", "TaskState": *state,
                "TaskStatus": if *state == "Exception" {"Critical"} else {"OK"},
            }))
        })
        .mount(&server.server)
        .await;
    state
}

#[test]
fn constructor_preserves_identity_and_rejects_other_types() {
    let mut config = config(443);
    let node = DellGb200Compute::from_config(&config, "rack-01").unwrap();
    assert_eq!(node.node_type(), NodeType::ComputeGb200Dell);
    assert_eq!(node.get_info()["type"], "compute_gb200_dell");
    assert_eq!(node.rack_id(), "rack-01");
    assert!(node.recovery().is_none());
    assert!(!node.node_type().supports_flint_inband_firmware());
    config.node_type = NodeType::ComputeGb200Nvidia;
    assert!(DellGb200Compute::from_config(&config, "rack-01").is_err());
}

#[test]
fn raw_bmc_requires_unambiguous_version_evidence() {
    let mut image = target("BMC", "ExampleBmcFwV250729001500.ima");
    assert_eq!(expected_bmc_version(&image).unwrap(), "25.07.29001500");
    image.expected_version = Some("25.07.29001500".into());
    assert!(validate_firmware_targets(std::slice::from_ref(&image)).is_ok());
    image.expected_version = Some("25.08.01000000".into());
    assert!(
        expected_bmc_version(&image)
            .unwrap_err()
            .message
            .contains("conflicts")
    );
    image.firmware_file = "operator-image.ima".into();
    assert_eq!(expected_bmc_version(&image).unwrap(), "25.08.01000000");
    for name in [
        "image.ima",
        "BmcFwV2507290015009.ima",
        "BmcFwV250729001500-copy.ima",
        "V250729001500.ima",
    ] {
        let image = target("BMC", name);
        assert!(validate_firmware_targets(&[image]).is_err(), "{name}");
    }
}

#[tokio::test]
async fn capture_inventory_power_and_raw_bmc_verification() {
    let server = capture_server().await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let inventory = node.get_firmware_inventory().await.unwrap();
    assert_eq!(inventory.len(), 29);
    assert_eq!(
        inventory
            .iter()
            .find(|entry| entry.target == HOST_BMC_INVENTORY)
            .unwrap()
            .version,
        "25.07.29001500"
    );
    assert_eq!(node.get_power_state().await.unwrap(), PowerState::On);
    for (target, uri) in [
        (
            PowerTargetType::System,
            "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
        ),
        (
            PowerTargetType::BMC,
            "/redfish/v1/Managers/BMC_0/Actions/Manager.Reset",
        ),
        (
            PowerTargetType::HMC,
            "/redfish/v1/Managers/HGX_BMC_0/Actions/Manager.Reset",
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(uri))
            .and(body_json(json!({"ResetType": "ForceRestart"})))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server.server)
            .await;
        node.set_power_state(PowerOp::ForceRestart, target)
            .await
            .unwrap();
    }
    let image = resolved("BMC", "ExampleBmcFwV250729001500.ima");
    assert!(
        node.verify_firmware_package_versions(std::slice::from_ref(&image))
            .await
            .unwrap()
            .matched
    );
    assert!(
        !inventory_version_check(std::slice::from_ref(&image), &[])
            .unwrap()
            .matched
    );
    let wrong = target("BMC", "ExampleBmcFwV250801000000.ima");
    assert!(
        !node
            .verify_firmware_package_versions(&[wrong])
            .await
            .unwrap()
            .matched
    );
    let outcome = node
        .update_firmware(&image, false, FirmwareUpdateOptions::default())
        .await
        .unwrap();
    assert!(matches!(outcome, FirmwareUpdateOutcome::Skipped { .. }));
}

#[tokio::test]
async fn multipart_upload_uses_discovered_ami_endpoint_and_uft_parameters() {
    let server = capture_server().await;
    let service = allow_bmc_preservation(&server, captured_update_service()).await;
    allow_upload_and_task(&server, "Completed").await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let directory = tempfile::tempdir().unwrap();
    for (component, filename, params, oem) in [
        (
            "BMC",
            "ExampleBmcFwV250729001500.ima",
            json!({"Targets": []}),
            json!({"ImageType": "BMC"}),
        ),
        (
            "HMC",
            "compute_nosbios.fwpkg",
            json!({"Targets": []}),
            json!({"ImageType": "PLDM", "Platform": "HGX"}),
        ),
        (
            "BIOS",
            "Dell_SBIOS.fwpkg",
            json!({"Targets": [
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
                "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1"
            ]}),
            json!({"ImageType": "PLDM", "Platform": "HGX"}),
        ),
    ] {
        let file = directory.path().join(filename);
        std::fs::write(&file, b"opaque firmware fixture").unwrap();
        let image = resolved(component, file.to_str().unwrap());
        // Even a forced reflash must omit the transport ForceUpdate key.
        let outcome = node
            .update_firmware(&image, true, FirmwareUpdateOptions::default())
            .await
            .unwrap();
        assert!(matches!(outcome, FirmwareUpdateOutcome::Started(_)));
        let requests = server.server.received_requests().await.unwrap();
        let request = requests
            .iter()
            .rev()
            .find(|request| request.method == wiremock::http::Method::Post)
            .unwrap();
        let body = String::from_utf8_lossy(&request.body);
        assert!(body.contains("name=\"UpdateFile\""), "{body}");
        assert!(body.contains(&params.to_string()), "{body}");
        assert!(body.contains(&oem.to_string()), "{body}");
        assert!(!body.contains("ForceUpdate"), "{body}");
        assert_eq!(
            service.lock().unwrap().pointer(FORCE_UPDATE),
            Some(&json!(true))
        );
        assert!(node.poll_firmware_task("144").await.unwrap().completed);
        assert_eq!(
            service.lock().unwrap().pointer(FORCE_UPDATE),
            Some(&json!(false))
        );
    }
    // A configured ForceUpdate remains authoritative even when the RPC flag is
    // false and the captured inventory already matches the BMC image.
    let mut image = resolved_target(&target(
        "BMC",
        directory
            .path()
            .join("ExampleBmcFwV250729001500.ima")
            .to_str()
            .unwrap(),
    ))
    .unwrap();
    image
        .resolution
        .as_mut()
        .unwrap()
        .update_parameters
        .as_mut()
        .unwrap()
        .insert("ForceUpdate".into(), json!(true));
    let outcome = node
        .update_firmware(&image, false, FirmwareUpdateOptions::default())
        .await
        .unwrap();
    assert!(matches!(outcome, FirmwareUpdateOutcome::Started(_)));
    let requests = server.server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == wiremock::http::Method::Post)
            .count(),
        4
    );
    let request = requests
        .iter()
        .rev()
        .find(|request| request.method == wiremock::http::Method::Post)
        .unwrap();
    assert!(String::from_utf8_lossy(&request.body).contains("\"ForceUpdate\":true"));
    node.poll_firmware_task("144").await.unwrap();
    image
        .resolution
        .as_mut()
        .unwrap()
        .update_parameters
        .as_mut()
        .unwrap()
        .insert("ForceUpdate".into(), json!(false));
    let before_skipped = server.server.received_requests().await.unwrap().len();
    assert!(matches!(
        node.update_firmware(&image, true, FirmwareUpdateOptions::default())
            .await
            .unwrap(),
        FirmwareUpdateOutcome::Skipped { .. }
    ));
    // The skip is an inventory comparison only: no settings change and no upload.
    let after_skipped = server.server.received_requests().await.unwrap();
    for request in &after_skipped[before_skipped..] {
        assert_eq!(request.method, wiremock::http::Method::Get, "{request:?}");
        assert!(
            !matches!(
                request.url.path(),
                "/redfish/v1/UpdateService" | "/redfish/v1/UpdateService/upload"
            ),
            "{request:?}"
        );
    }
    let relevant: Vec<_> = requests
        .iter()
        .filter(|request| {
            matches!(
                request.url.path(),
                "/redfish/v1/UpdateService" | "/redfish/v1/UpdateService/upload"
            )
        })
        .collect();
    assert_eq!(relevant[0].method, wiremock::http::Method::Get);
    assert_eq!(relevant[1].method, wiremock::http::Method::Patch);
    assert_eq!(relevant[2].method, wiremock::http::Method::Get);
    assert!(
        relevant[3..]
            .iter()
            .any(|request| request.url.path() == "/redfish/v1/UpdateService/upload")
    );
}

#[tokio::test]
async fn force_option_is_scoped_to_the_task_and_restores_the_original_value() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    for (original, requested) in [(false, true), (false, false), (true, true), (true, false)] {
        let server = capture_server().await;
        let mut initial = captured_update_service();
        initial["HttpPushUriOptions"]["ForceUpdate"] = json!(original);
        let service = allow_force_updates(&server, initial).await;
        let task = allow_upload_and_task(&server, "Running").await;
        // Exercise capability dispatch through the production node enum.
        let node =
            crate::nodes::NodeInstance::from_config(&config(server.port), "rack-01").unwrap();
        let image = resolved("HMC", file.to_str().unwrap());
        let outcome = node
            .update_firmware(&image, requested, FirmwareUpdateOptions::default())
            .await
            .unwrap();
        assert!(matches!(outcome, FirmwareUpdateOutcome::Started(_)));
        assert_eq!(
            service.lock().unwrap().pointer(FORCE_UPDATE),
            Some(&json!(requested))
        );
        assert!(!node.poll_firmware_task("144").await.unwrap().completed);
        assert_eq!(
            service.lock().unwrap().pointer(FORCE_UPDATE),
            Some(&json!(requested))
        );

        *task.lock().unwrap() = "Completed".to_owned();
        assert!(node.poll_firmware_task("144").await.unwrap().completed);
        node.firmware_task_cleanup()
            .unwrap()
            .finish("144")
            .await
            .unwrap();
        assert_eq!(
            service.lock().unwrap().pointer(FORCE_UPDATE),
            Some(&json!(original))
        );
        let requests = server.server.received_requests().await.unwrap();
        let patches: Vec<_> = requests
            .iter()
            .filter(|request| request.method == wiremock::http::Method::Patch)
            .collect();
        assert_eq!(patches.len(), if requested == original { 0 } else { 2 });
        if !patches.is_empty() {
            assert_eq!(
                patches[0].body_json::<Value>().unwrap(),
                json!({"HttpPushUriOptions":{"ForceUpdate":requested}})
            );
            assert_eq!(
                patches[1].body_json::<Value>().unwrap(),
                json!({"HttpPushUriOptions":{"ForceUpdate":original}})
            );
        }
    }
}

#[tokio::test]
async fn active_task_blocks_cleanup_and_a_second_upload_then_failure_restores_force() {
    let server = capture_server().await;
    let service = allow_force_updates(&server, captured_update_service()).await;
    let task = allow_upload_and_task(&server, "Running").await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    let image = resolved("HMC", file.to_str().unwrap());
    node.update_firmware(&image, true, FirmwareUpdateOptions::default())
        .await
        .unwrap();
    node.firmware_task_cleanup()
        .unwrap()
        .finish("unrelated")
        .await
        .unwrap();
    let error = node
        .firmware_task_cleanup()
        .unwrap()
        .finish("144")
        .await
        .unwrap_err();
    assert!(error.message.contains("still active"), "{error}");
    let error = node
        .update_firmware(&image, false, FirmwareUpdateOptions::default())
        .await
        .unwrap_err();
    assert!(error.message.contains("still active"), "{error}");
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(true))
    );
    assert_eq!(
        server
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method == wiremock::http::Method::Post)
            .count(),
        1
    );
    *task.lock().unwrap() = "Exception".to_owned();
    let status = node.poll_firmware_task("144").await.unwrap();
    assert!(status.completed);
    assert_eq!(status.state, "Failed");
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(false))
    );
}

#[tokio::test]
async fn persistently_unreadable_task_is_terminal_for_cleanup_so_the_node_recovers() {
    let server = capture_server().await;
    let service = allow_force_updates(&server, captured_update_service()).await;
    allow_upload_and_task(&server, "Running").await;
    let mut node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    node.unreadable_task_grace = std::time::Duration::ZERO;
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    let image = resolved("HMC", file.to_str().unwrap());
    node.update_firmware(&image, true, FirmwareUpdateOptions::default())
        .await
        .unwrap();
    // The BMC drops the task (for example after restarting).
    Mock::given(method("GET"))
        .and(path("/redfish/v1/TaskService/Tasks/144"))
        .respond_with(ResponseTemplate::new(404))
        .with_priority(1)
        .mount(&server.server)
        .await;
    let cleanup = node.firmware_task_cleanup().unwrap();
    for _ in 1..MAX_UNREADABLE_TASK_POLLS {
        let error = cleanup.finish("144").await.unwrap_err();
        assert!(error.message.contains("cleanup pending"), "{error}");
        assert_eq!(
            service.lock().unwrap().pointer(FORCE_UPDATE),
            Some(&json!(true))
        );
        assert!(node.pending_update.lock().await.is_some());
    }
    // The next attempt gives up on the task and restores the setting.
    cleanup.finish("144").await.unwrap();
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(false))
    );
    assert!(node.pending_update.lock().await.is_none());
}

#[tokio::test]
async fn quick_unreadable_retries_within_the_grace_period_keep_the_setting() {
    let server = capture_server().await;
    let service = allow_force_updates(&server, captured_update_service()).await;
    allow_upload_and_task(&server, "Running").await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    let image = resolved("HMC", file.to_str().unwrap());
    node.update_firmware(&image, true, FirmwareUpdateOptions::default())
        .await
        .unwrap();
    // The BMC drops the task (for example after restarting).
    Mock::given(method("GET"))
        .and(path("/redfish/v1/TaskService/Tasks/144"))
        .respond_with(ResponseTemplate::new(404))
        .with_priority(1)
        .mount(&server.server)
        .await;
    let cleanup = node.firmware_task_cleanup().unwrap();
    // Retries in quick succession, as while the BMC reboots, never reach the
    // grace period, so the task stays pending however many reads fail.
    for _ in 0..=MAX_UNREADABLE_TASK_POLLS {
        let error = cleanup.finish("144").await.unwrap_err();
        assert!(error.message.contains("cleanup pending"), "{error}");
    }
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(true))
    );
    assert!(node.pending_update.lock().await.is_some());
}

#[tokio::test]
async fn readable_active_task_is_never_treated_as_unreadable() {
    let server = capture_server().await;
    let service = allow_force_updates(&server, captured_update_service()).await;
    let task = allow_upload_and_task(&server, "Running").await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    node.update_firmware(
        &resolved("HMC", file.to_str().unwrap()),
        true,
        FirmwareUpdateOptions::default(),
    )
    .await
    .unwrap();
    let cleanup = node.firmware_task_cleanup().unwrap();
    // A task that can be read and is still running never becomes terminal,
    // however many times cleanup runs.
    for _ in 0..=MAX_UNREADABLE_TASK_POLLS {
        let error = cleanup.finish("144").await.unwrap_err();
        assert!(error.message.contains("still active"), "{error}");
    }
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(true))
    );
    *task.lock().unwrap() = "Completed".to_owned();
    cleanup.finish("144").await.unwrap();
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(false))
    );
}

#[tokio::test]
async fn failed_upload_and_cancelled_force_preparation_restore_the_setting() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    for cancel_during_patch in [false, true] {
        let server = capture_server().await;
        let service = allow_force_updates(&server, captured_update_service()).await;
        let token = tokio_util::sync::CancellationToken::new();
        if cancel_during_patch {
            let patch_state = service.clone();
            let cancel = token.clone();
            Mock::given(method("PATCH"))
                .and(path(UPDATE_SERVICE))
                .and(body_json(
                    json!({"HttpPushUriOptions":{"ForceUpdate":true}}),
                ))
                .respond_with(move |_: &wiremock::Request| {
                    patch_state.lock().unwrap()["HttpPushUriOptions"]["ForceUpdate"] = json!(true);
                    cancel.cancel();
                    ResponseTemplate::new(204)
                })
                .with_priority(1)
                .expect(1)
                .mount(&server.server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/upload"))
            .respond_with(ResponseTemplate::new(500))
            .expect(if cancel_during_patch { 0 } else { 1 })
            .mount(&server.server)
            .await;
        let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
        let error = node
            .update_firmware(
                &resolved("HMC", file.to_str().unwrap()),
                true,
                FirmwareUpdateOptions {
                    cancellation: Some(token),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        if cancel_during_patch {
            assert_eq!(error.code, ErrorCode::Cancelled);
        }
        assert_eq!(
            service.lock().unwrap().pointer(FORCE_UPDATE),
            Some(&json!(false))
        );
        assert!(node.pending_update.lock().await.is_none());
        server.server.verify().await;
    }
}

#[tokio::test]
async fn force_preparation_requires_a_boolean_and_verified_patch_before_upload() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    for (value, patch_status) in [
        (Value::Null, 204),
        (json!("false"), 204),
        (json!(false), 400),
        (json!(false), 204),
    ] {
        let server = capture_server().await;
        let mut initial = captured_update_service();
        initial["HttpPushUriOptions"]["ForceUpdate"] = value;
        Mock::given(method("GET"))
            .and(path(UPDATE_SERVICE))
            .respond_with(ResponseTemplate::new(200).set_body_json(initial))
            .with_priority(1)
            .mount(&server.server)
            .await;
        // A 204 response alone is not evidence that the setting took effect.
        Mock::given(method("PATCH"))
            .and(path(UPDATE_SERVICE))
            .respond_with(ResponseTemplate::new(patch_status))
            .mount(&server.server)
            .await;
        let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
        assert!(
            node.update_firmware(
                &resolved("HMC", file.to_str().unwrap()),
                true,
                FirmwareUpdateOptions::default()
            )
            .await
            .is_err()
        );
        assert!(
            server
                .server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.method != wiremock::http::Method::Post)
        );
    }
}

#[tokio::test]
async fn restore_failure_is_reported_and_the_original_value_is_retained_for_retry() {
    let server = capture_server().await;
    let service = allow_force_updates(&server, captured_update_service()).await;
    allow_upload_and_task(&server, "Completed").await;
    Mock::given(method("PATCH"))
        .and(path(UPDATE_SERVICE))
        .and(body_json(
            json!({"HttpPushUriOptions":{"ForceUpdate":false}}),
        ))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .expect(2)
        .up_to_n_times(2)
        .mount(&server.server)
        .await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    node.update_firmware(
        &resolved("HMC", file.to_str().unwrap()),
        true,
        FirmwareUpdateOptions::default(),
    )
    .await
    .unwrap();
    // A failed restore must not fail the poll; the completed task result stays authoritative.
    let status = node.poll_firmware_task("144").await.unwrap();
    assert!(status.completed);
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(true))
    );
    // finish() retries the restore and reports the remaining failure.
    let cleanup = node.firmware_task_cleanup().unwrap();
    let error = cleanup.finish("144").await.unwrap_err();
    assert!(
        error.message.contains("cleanup pending (restore false)"),
        "{error}"
    );
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(true))
    );
    assert!(node.pending_update.lock().await.is_some());
    cleanup.finish("144").await.unwrap();
    assert_eq!(
        service.lock().unwrap().pointer(FORCE_UPDATE),
        Some(&json!(false))
    );
    assert!(node.pending_update.lock().await.is_none());
}

#[tokio::test]
async fn bmc_preservation_enables_ipmi_when_other_access_settings_are_already_enabled() {
    let server = HttpsInventory::start().await;
    let mut initial = preserved_update_service();
    initial["Oem"]["AMIUpdateService"]["PreserveConfiguration"]["IPMI"] = json!(false);
    allow_bmc_preservation(&server, initial).await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();

    node.preserve_bmc_access().await.unwrap();
    node.preserve_bmc_access().await.unwrap();
    server.server.verify().await;
}

#[tokio::test]
async fn bmc_upload_stops_when_preservation_is_missing_rejected_or_unverified() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("ExampleBmcFwV250729001500.ima");
    std::fs::write(&file, b"raw BMC fixture").unwrap();
    let image = resolved("BMC", file.to_str().unwrap());
    let mut missing_flag = captured_update_service();
    missing_flag["Oem"]["AMIUpdateService"]["PreserveConfiguration"]
        .as_object_mut()
        .unwrap()
        .remove("Authentication");
    let mut malformed_flag = captured_update_service();
    malformed_flag["Oem"]["AMIUpdateService"]["PreserveConfiguration"]["Network"] = json!("false");
    let mut missing_ipmi = captured_update_service();
    missing_ipmi["Oem"]["AMIUpdateService"]["PreserveConfiguration"]
        .as_object_mut()
        .unwrap()
        .remove("IPMI");
    let mut ipmi_not_retained = preserved_update_service();
    ipmi_not_retained["Oem"]["AMIUpdateService"]["PreserveConfiguration"]["IPMI"] = json!(false);
    let mut network_not_retained = preserved_update_service();
    network_not_retained["Oem"]["AMIUpdateService"]["PreserveConfiguration"]["Network"] =
        json!(false);
    for (initial_status, initial, patch_status, readback_status, readback) in [
        (401, json!({}), 204, 200, preserved_update_service()),
        (200, json!({}), 204, 200, preserved_update_service()),
        (200, missing_flag, 204, 200, preserved_update_service()),
        (200, malformed_flag, 204, 200, preserved_update_service()),
        (200, missing_ipmi, 204, 200, preserved_update_service()),
        (200, captured_update_service(), 204, 200, ipmi_not_retained),
        (
            200,
            captured_update_service(),
            204,
            200,
            network_not_retained,
        ),
        (
            200,
            captured_update_service(),
            400,
            200,
            preserved_update_service(),
        ),
        (
            200,
            captured_update_service(),
            204,
            200,
            captured_update_service(),
        ),
        (200, captured_update_service(), 204, 401, json!({})),
    ] {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let server = HttpsInventory::start().await;
        let patched = Arc::new(AtomicBool::new(false));
        let get_patched = patched.clone();
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(move |_: &wiremock::Request| {
                if get_patched.load(Ordering::SeqCst) {
                    ResponseTemplate::new(readback_status).set_body_json(readback.clone())
                } else {
                    ResponseTemplate::new(initial_status).set_body_json(initial.clone())
                }
            })
            .mount(&server.server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(move |_: &wiremock::Request| {
                patched.store(true, Ordering::SeqCst);
                ResponseTemplate::new(patch_status)
            })
            .mount(&server.server)
            .await;
        let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
        assert!(node.start_firmware_upload(&image, true).await.is_err());
        assert!(
            server
                .server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| { request.method != wiremock::http::Method::Post })
        );
    }
}

#[tokio::test]
async fn cancellation_during_bmc_preservation_stops_before_patch_or_upload() {
    let server = HttpsInventory::start().await;
    let token = tokio_util::sync::CancellationToken::new();
    let cancel = token.clone();
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(move |_: &wiremock::Request| {
            cancel.cancel();
            ResponseTemplate::new(200).set_body_json(captured_update_service())
        })
        .expect(1)
        .mount(&server.server)
        .await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let error = node
        .update_firmware(
            &resolved("BMC", "ExampleBmcFwV250729001500.ima"),
            true,
            FirmwareUpdateOptions {
                cancellation: Some(token),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Cancelled);
    assert!(
        server
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| { request.method == wiremock::http::Method::Get })
    );
}

#[tokio::test]
async fn cancellation_and_unsupported_components_perform_no_io() {
    let server = HttpsInventory::start().await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();
    let image = resolved("BMC", "ExampleBmcFwV250729001500.ima");
    let error = node
        .update_firmware(
            &image,
            true,
            FirmwareUpdateOptions {
                cancellation: Some(token),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Cancelled);
    for component in ["CX7", "CX8", "BF3_NIC", "BF3_BFB", "LINKX"] {
        let error = node
            .update_firmware(
                &target(component, "image.bin"),
                false,
                FirmwareUpdateOptions::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Unimplemented);
    }
    let error = node
        .update_firmware(
            &target("BMC", "ExampleBmcFwV250729001500.ima"),
            false,
            FirmwareUpdateOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(error.message.contains("not resolved by a hardware profile"));
    assert!(server.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn opaque_pldm_image_and_failed_tasks_are_not_verified_success() {
    let server = capture_server().await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("compute_nosbios.fwpkg");
    std::fs::write(&file, b"opaque firmware fixture").unwrap();
    let image = resolved("HMC", file.to_str().unwrap());
    assert!(
        !node
            .verify_firmware_package_versions(&[image])
            .await
            .unwrap()
            .matched
    );
    Mock::given(method("GET"))
        .and(path("/redfish/v1/TaskService/Tasks/144"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Id": "144", "TaskState": "Exception", "TaskStatus": "Critical", "PercentComplete": 100,
            "Messages": [{"Message": "Firmware update failed"}]
        })))
        .mount(&server.server)
        .await;
    let status = node.poll_firmware_task("144").await.unwrap();
    assert!(status.completed);
    assert_eq!(status.state, "Failed");
    assert_eq!(status.status, "Critical");
}

#[tokio::test]
async fn bmc_verification_and_skip_honor_operator_expected_inventory() {
    let server = capture_server().await;
    let mut config = config(server.port);
    config.expected_inventory = Some(ExpectedInventoryPolicy {
        profile: "operator_inventory".into(),
        ap_names: vec!["BMC".into(), "REQUIRED_BUT_MISSING".into()].into(),
        flint_device_counts: Default::default(),
    });
    let node = DellGb200Compute::from_config(&config, "rack-01").unwrap();
    let image = resolved("BMC", "ExampleBmcFwV250729001500.ima");
    let error = node
        .verify_firmware_package_versions(std::slice::from_ref(&image))
        .await
        .unwrap_err();
    assert!(error.message.contains("REQUIRED_BUT_MISSING"));
    let error = node
        .update_firmware(&image, false, FirmwareUpdateOptions::default())
        .await
        .unwrap_err();
    assert!(error.message.contains("REQUIRED_BUT_MISSING"));
}

#[tokio::test]
async fn sbios_manifest_version_checks_both_cpus_and_requires_exact_inventory_versions() {
    let server = capture_server().await;
    let node = DellGb200Compute::from_config(&config(server.port), "rack-01").unwrap();
    let mut image = resolved("BIOS", "Dell_SBIOS.fwpkg");
    image.expected_version = Some("216".into());
    let summary = node
        .verify_firmware_package_versions(std::slice::from_ref(&image))
        .await
        .unwrap();
    assert!(summary.matched, "{summary:?}");
    assert_eq!(
        summary.details["checks"][0]["components"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    image.expected_version = Some("217".into());
    assert!(
        !node
            .verify_firmware_package_versions(std::slice::from_ref(&image))
            .await
            .unwrap()
            .matched
    );
    let mut inventory = node.get_firmware_inventory().await.unwrap();
    inventory.retain(|entry| !entry.target.ends_with("HGX_FW_CPU_1"));
    image.expected_version = Some("216".into());
    assert!(
        !inventory_version_check(&[image], &inventory)
            .unwrap()
            .matched
    );
}

#[test]
fn custom_profile_can_use_pldm_for_bmc_without_raw_image_rules() {
    let profile = crate::config::hardware_profiles::HardwareUpdatePlan::from_yaml_str(
        &include_str!("../../../../../hardware-profiles/v1/compute_gb200_dell.yaml")
            .replace("suffix: .ima", "suffix: .fwpkg")
            .replace("\"ImageType\": \"BMC\"", "\"ImageType\": \"PLDM\""),
    )
    .unwrap();
    let configured = profile.stages()[0].targets().first().unwrap();
    let image = configured
        .resolve(&target("BMC", "operator.fwpkg"))
        .unwrap();
    assert!(!is_host_bmc(&image));
    assert!(validate_firmware_targets(&[image]).is_ok());
}

#[tokio::test]
async fn activation_discovers_host_system_and_auxiliary_action_from_ami_capture() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    let server = capture_server().await;
    let powered_on = Arc::new(AtomicBool::new(true));
    let events = Arc::new(Mutex::new(Vec::new()));
    let responses: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(include_str!("../../../tests/fixtures/dell_gb200_ami.json")).unwrap();
    let system = responses["/redfish/v1/Systems/System_0"].clone();
    let state = powered_on.clone();
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Systems/System_0"))
        .respond_with(move |_: &wiremock::Request| {
            let mut system = system.clone();
            system["PowerState"] = if state.load(Ordering::SeqCst) {
                "On"
            } else {
                "Off"
            }
            .into();
            ResponseTemplate::new(200).set_body_json(system)
        })
        .with_priority(1)
        .mount(&server.server)
        .await;
    for uri in [
        "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
        "/redfish/v1/Chassis/Chassis_0/Actions/Oem/NvidiaChassis.AuxPowerReset",
    ] {
        let state = powered_on.clone();
        let events = events.clone();
        Mock::given(method("POST"))
            .and(path(uri))
            .respond_with(move |request: &wiremock::Request| {
                let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                let reset = body["ResetType"].as_str().unwrap();
                events.lock().unwrap().push(reset.to_owned());
                match reset {
                    "ForceOff" => state.store(false, Ordering::SeqCst),
                    "On" => state.store(true, Ordering::SeqCst),
                    "AuxPowerCycle" => {}
                    _ => panic!("unexpected reset {reset}"),
                }
                ResponseTemplate::new(204)
            })
            .mount(&server.server)
            .await;
    }
    let mut config = config(server.port);
    let mut runtime = crate::config::hardware_profiles::HardwareProfileCatalog::default()
        .runtime_config_for_request(NodeType::ComputeGb200Dell)
        .unwrap();
    let NodeRuntimeConfig::RedfishNvfwupdCompute(compute) = &mut runtime;
    compute.firmware_timing.power_off_delay = Duration::ZERO;
    compute.firmware_timing.ac_cycle_stabilization = Duration::ZERO;
    compute.firmware_timing.bmc_recovery_interval = Duration::ZERO;
    compute.firmware_timing.power_on_interval = Duration::ZERO;
    compute.firmware_timing.activation_command_retry_interval = Duration::ZERO;
    config.runtime_config = Some(runtime);
    let node = DellGb200Compute::from_config(&config, "rack-01").unwrap();
    tokio::time::timeout(Duration::from_secs(10), node.activate_firmware())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*events.lock().unwrap(), ["ForceOff", "AuxPowerCycle", "On"]);
    assert!(powered_on.load(Ordering::SeqCst));
}
