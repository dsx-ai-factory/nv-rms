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

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;

use serde_json::json;
use url::Url;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate, Times};

const SYSTEM_RESET_TARGET: &str = "/custom/Systems/System_0/Reset";
const MANAGER_RESET_TARGET: &str = "/custom/Managers/BMC_0/Reset";

fn client_for(server: &MockServer) -> RedfishClient {
    let url = Url::parse(&server.uri()).unwrap();

    RedfishClient::new(
        url.host_str().unwrap(),
        url.port().unwrap(),
        credentials("secret"),
        true,
        false,
    )
    .unwrap()
}

fn test_client(
    host: &str,
    password: &str,
    dangerously_accept_invalid_certs: bool,
    https: bool,
) -> RedfishClient {
    RedfishClient::new(
        host,
        9443,
        credentials(password),
        dangerously_accept_invalid_certs,
        https,
    )
    .unwrap()
}

fn credentials(password: &str) -> BmcCredentials {
    BmcCredentials::username_password("admin".to_owned(), Some(password.to_owned()))
}

fn firmware_file() -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"firmware").unwrap();

    file
}

fn json_response(body: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(body)
}

async fn mount_get(
    server: &MockServer,
    request_path: &str,
    response: ResponseTemplate,
    expected_requests: impl Into<Times>,
) {
    Mock::given(method("GET"))
        .and(path(request_path))
        .respond_with(response)
        .expect(expected_requests)
        .mount(server)
        .await;
}

async fn mount_get_json(
    server: &MockServer,
    request_path: &str,
    body: serde_json::Value,
    expected_requests: impl Into<Times>,
) {
    mount_get(server, request_path, json_response(body), expected_requests).await;
}

async fn mount_get_status(
    server: &MockServer,
    request_path: &str,
    status: u16,
    expected_requests: u64,
) {
    mount_get(
        server,
        request_path,
        ResponseTemplate::new(status),
        expected_requests,
    )
    .await;
}

async fn mount_post_json(
    server: &MockServer,
    request_path: &str,
    expected_body: serde_json::Value,
    response: ResponseTemplate,
) {
    Mock::given(method("POST"))
        .and(path(request_path))
        .and(body_json(expected_body))
        .respond_with(response)
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_root_nav(
    server: &MockServer,
    nav_name: &str,
    nav_path: &str,
    expected_requests: impl Into<Times>,
) {
    mount_get_json(
        server,
        "/redfish/v1",
        root_nav_body(nav_name, nav_path),
        expected_requests,
    )
    .await;
}

async fn mount_dynamic_root_nav(
    server: &MockServer,
    nav_name: &str,
    nav_paths: &[&str],
    expected_requests: u64,
) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_response = Arc::clone(&calls);
    let nav_name = nav_name.to_owned();
    let nav_paths = nav_paths
        .iter()
        .map(|nav_path| (*nav_path).to_owned())
        .collect::<Vec<_>>();

    Mock::given(method("GET"))
        .and(path("/redfish/v1"))
        .respond_with(move |_: &wiremock::Request| {
            let index = calls_for_response.fetch_add(1, Ordering::SeqCst);
            let nav_path = nav_paths
                .get(index)
                .or_else(|| nav_paths.last())
                .expect("dynamic root needs at least one navigation path");

            json_response(root_nav_body(&nav_name, nav_path))
        })
        .expect(expected_requests)
        .mount(server)
        .await;

    calls
}

fn root_nav_body(nav_name: &str, nav_path: &str) -> serde_json::Value {
    let mut root = json!({
        "@odata.id": "/redfish/v1",
        "Id": "RootService",
        "Name": "Root Service",
        "Links": {}
    });

    root[nav_name] = json!({"@odata.id": nav_path});

    root
}

async fn mount_collection(
    server: &MockServer,
    nav_name: &str,
    collection_path: &str,
    collection_type: &str,
    collection_name: &str,
    member_paths: &[&str],
    expected_requests: (impl Into<Times>, impl Into<Times>),
) {
    mount_root_nav(server, nav_name, collection_path, expected_requests.0).await;

    mount_get_json(
        server,
        collection_path,
        collection_body(
            collection_path,
            collection_type,
            collection_name,
            member_paths,
        ),
        expected_requests.1,
    )
    .await;
}

fn collection_body(
    collection_path: &str,
    collection_type: &str,
    collection_name: &str,
    member_paths: &[&str],
) -> serde_json::Value {
    json!({
        "@odata.id": collection_path,
        "@odata.type": collection_type,
        "Name": collection_name,
        "Members": member_paths
            .iter()
            .map(|member_path| json!({"@odata.id": member_path}))
            .collect::<Vec<_>>()
    })
}

async fn mount_system_members(
    server: &MockServer,
    member_paths: &[&str],
    expected_root_requests: u64,
    expected_collection_requests: u64,
) {
    mount_collection(
        server,
        "Systems",
        "/redfish/v1/Systems",
        "#ComputerSystemCollection.ComputerSystemCollection",
        "Computer System Collection",
        member_paths,
        (expected_root_requests, expected_collection_requests),
    )
    .await;
}

async fn mount_manager_members(
    server: &MockServer,
    member_paths: &[&str],
    expected_root_requests: impl Into<Times>,
    expected_collection_requests: impl Into<Times>,
) {
    mount_collection(
        server,
        "Managers",
        "/redfish/v1/Managers",
        "#ManagerCollection.ManagerCollection",
        "Manager Collection",
        member_paths,
        (expected_root_requests, expected_collection_requests),
    )
    .await;
}

async fn mount_system_member(
    server: &MockServer,
    member_path: &str,
    member: serde_json::Value,
    expected_root_requests: u64,
    expected_collection_requests: u64,
    expected_member_requests: u64,
) {
    mount_system_members(
        server,
        &[member_path],
        expected_root_requests,
        expected_collection_requests,
    )
    .await;

    Mock::given(method("GET"))
        .and(path(member_path))
        .and(|request: &wiremock::Request| {
            request
                .headers
                .keys()
                .all(|name| !name.to_string().eq_ignore_ascii_case("if-none-match"))
        })
        .respond_with(json_response(member))
        .expect(expected_member_requests)
        .mount(server)
        .await;
}

async fn mount_manager_member(
    server: &MockServer,
    member_path: &str,
    member: serde_json::Value,
    expected_root_requests: impl Into<Times>,
    expected_collection_requests: impl Into<Times>,
    expected_member_requests: u64,
) {
    mount_manager_members(
        server,
        &[member_path],
        expected_root_requests,
        expected_collection_requests,
    )
    .await;

    mount_get_json(server, member_path, member, expected_member_requests).await;
}

async fn mount_update_service(
    server: &MockServer,
    multipart_uri: &str,
    expected_root_requests: u64,
    expected_service_requests: u64,
) {
    mount_root_nav(
        server,
        "UpdateService",
        "/redfish/v1/UpdateService",
        expected_root_requests,
    )
    .await;

    mount_get_json(
        server,
        "/redfish/v1/UpdateService",
        update_service_body("/redfish/v1/UpdateService", multipart_uri),
        expected_service_requests,
    )
    .await;
}

fn update_service_body(service_path: &str, multipart_uri: &str) -> serde_json::Value {
    json!({
        "@odata.id": service_path,
        "Id": "UpdateService",
        "Name": "Update Service",
        "MultipartHttpPushUri": multipart_uri
    })
}

async fn mount_multipart_update(
    server: &MockServer,
    upload_path: &str,
    force_update: bool,
    targets: &'static str,
    response: ResponseTemplate,
) {
    let force_update = format!(r#""ForceUpdate":{force_update}"#);
    let targets = format!(r#""Targets":{targets}"#);

    Mock::given(method("POST"))
        .and(path(upload_path))
        .and(move |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);

            body.contains(r#"name="UpdateParameters""#)
                && body.contains(r#"name="UpdateFile""#)
                && body.contains(&force_update)
                && body.contains(&targets)
        })
        .respond_with(response)
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_reset_action_response(
    server: &MockServer,
    action_path: &str,
    reset_type: &str,
    response: ResponseTemplate,
) {
    mount_post_json(
        server,
        action_path,
        json!({"ResetType": reset_type}),
        response,
    )
    .await;
}

fn system_with_power_state(path: &str, state: &str) -> serde_json::Value {
    json!({
        "@odata.id": path,
        "Id": "System_0",
        "Name": "System",
        "PowerState": state,
    })
}

fn system_with_reset(path: &str) -> serde_json::Value {
    json!({
        "@odata.id": path,
        "Id": "System_0",
        "Name": "System",
        "Actions": {"#ComputerSystem.Reset": {"target": SYSTEM_RESET_TARGET}}
    })
}

fn manager_with_reset(path: &str) -> serde_json::Value {
    json!({
        "@odata.id": path,
        "Id": "BMC_0",
        "Name": "Manager",
        "Actions": {"#Manager.Reset": {"target": MANAGER_RESET_TARGET}}
    })
}

fn system_without_processors(path: &str, id: &str, name: &str) -> serde_json::Value {
    json!({
        "@odata.id": path,
        "Id": id,
        "Name": name
    })
}

fn system_with_processors(
    path: &str,
    id: &str,
    name: &str,
    processors_path: &str,
) -> serde_json::Value {
    json!({
        "@odata.id": path,
        "Id": id,
        "Name": name,
        "Processors": {"@odata.id": processors_path}
    })
}

fn processor_collection(path: &str, member_paths: &[&str]) -> serde_json::Value {
    json!({
        "@odata.id": path,
        "@odata.type": "#ProcessorCollection.ProcessorCollection",
        "Name": "Processor Collection",
        "Members": member_paths
            .iter()
            .map(|member_path| json!({"@odata.id": member_path}))
            .collect::<Vec<_>>()
    })
}

fn processor_without_topology(
    path: &str,
    id: &str,
    name: &str,
    processor_type: &str,
) -> serde_json::Value {
    json!({
        "@odata.id": path,
        "Id": id,
        "Name": name,
        "ProcessorType": processor_type
    })
}

fn processor_with_topology(
    path: &str,
    id: &str,
    name: &str,
    chassis_sn: &str,
    slot_number: i64,
    tray_index: i64,
) -> serde_json::Value {
    json!({
        "@odata.id": path,
        "Id": id,
        "Name": name,
        "ProcessorType": "GPU",
        "Oem": {
            "Nvidia": {
                "MNNVLinkTopology": {
                    "ChassisSerialNumber": chassis_sn,
                    "TraySlotNumber": slot_number,
                    "TraySlotIndex": tray_index
                }
            }
        }
    })
}

#[test]
fn http_status_errors_map_to_shared_contract() {
    let url = Url::parse("https://bmc.example/redfish/v1").unwrap();

    for (status, expected) in [
        (401, RedfishError::Unauthenticated(String::new())),
        (403, RedfishError::Forbidden(String::new())),
        (404, RedfishError::NotFound(String::new())),
        (408, RedfishError::Timeout(String::new())),
        (409, RedfishError::AlreadyExists(String::new())),
        (502, RedfishError::Unavailable(String::new())),
        (503, RedfishError::Unavailable(String::new())),
        (504, RedfishError::Unavailable(String::new())),
        (500, RedfishError::Internal(String::new())),
    ] {
        let error = BmcError::InvalidResponse {
            url: url.clone(),
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            text: String::new(),
        };

        assert_eq!(
            std::mem::discriminant(&map_redfish_error("GET test", &error)),
            std::mem::discriminant(&expected)
        );
    }
}

#[test]
fn config_helpers_preserve_redfish_client_contracts() {
    let url = redfish_endpoint_url("fd00::1", 8443, true).unwrap();

    assert_eq!(url.as_str(), "https://[fd00::1]:8443/");

    let params = client_params(true);

    assert!(params.accept_invalid_certs);
    assert_eq!(params.timeout, Some(RedfishClient::REQUEST_TIMEOUT));
    assert_eq!(params.connect_timeout, Some(RedfishClient::CONNECT_TIMEOUT));

    assert_eq!(
        RedfishClient::DEFAULT_UPLOAD_TIMEOUT,
        Duration::from_secs(600)
    );
}

#[test]
fn new_caches_only_underlying_http_clients() {
    let first = test_client("client-instance.example", "secret", true, true);
    let second = test_client("client-instance.example", "secret", true, true);
    let different_credentials = test_client("client-instance.example", "other", true, true);
    let different_tls_policy = test_client("client-instance.example", "secret", false, true);
    let different_scheme = test_client("client-instance.example", "secret", true, false);

    assert!(!Arc::ptr_eq(&first.bmc, &second.bmc));
    assert!(!Arc::ptr_eq(&first.root, &second.root));
    assert!(!Arc::ptr_eq(&first.bmc, &different_credentials.bmc));
    assert!(!Arc::ptr_eq(&first.bmc, &different_tls_policy.bmc));
    assert!(!Arc::ptr_eq(&first.bmc, &different_scheme.bmc));

    assert!(
        HTTP_CLIENT_ACCEPTING_INVALID_CERTS
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .is_some()
    );

    assert!(
        HTTP_CLIENT_REJECTING_INVALID_CERTS
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .is_some()
    );
}

#[test]
fn construction_error_exposes_invalid_argument_code() {
    let error = RedfishClient::new("\n", 443, credentials("secret"), true, true)
        .map(|_| ())
        .unwrap_err();

    assert!(matches!(error, RedfishError::InvalidEndpoint { .. }));
}

#[test]
fn service_root_cache_is_scoped_to_client_instance() {
    let client = test_client("bmc.example", "secret", true, true);
    let cloned_client = client.clone();
    let other_client = test_client("bmc.example", "secret", true, true);

    assert!(Arc::ptr_eq(&client.root, &cloned_client.root));
    assert!(!Arc::ptr_eq(&client.root, &other_client.root));
}

#[tokio::test]
async fn concurrent_cloned_clients_initialize_service_root_once() {
    let server = MockServer::start().await;
    let member_path = "/redfish/v1/Oem/Nvidia/SystemInventory/primary";

    mount_system_member(
        &server,
        member_path,
        system_with_power_state(member_path, "On"),
        1,
        2,
        2,
    )
    .await;

    let client = client_for(&server);
    let cloned_client = client.clone();

    let (first, second) = tokio::join!(
        client.computer_system_power_state("System_0"),
        cloned_client.computer_system_power_state("System_0"),
    );

    assert_eq!(first.unwrap(), Some(RedfishPowerState::On));
    assert_eq!(second.unwrap(), Some(RedfishPowerState::On));
}

#[tokio::test]
async fn computer_system_collection_refreshes_stale_service_root_link() {
    let server = MockServer::start().await;
    let stale_collection_path = "/redfish/v1/Oem/Nvidia/StaleSystems";
    let collection_path = "/redfish/v1/Oem/Nvidia/SystemInventory";
    let member_path = "/redfish/v1/Oem/Nvidia/SystemInventory/primary";

    let root_calls = mount_dynamic_root_nav(
        &server,
        "Systems",
        &[stale_collection_path, collection_path],
        2,
    )
    .await;

    mount_get_status(&server, stale_collection_path, 404, 1).await;

    mount_get_json(
        &server,
        collection_path,
        collection_body(
            collection_path,
            "#ComputerSystemCollection.ComputerSystemCollection",
            "Computer System Collection",
            &[member_path],
        ),
        1,
    )
    .await;

    mount_get_json(
        &server,
        member_path,
        system_with_power_state(member_path, "On"),
        1,
    )
    .await;

    let power_state = client_for(&server)
        .computer_system_power_state("System_0")
        .await
        .unwrap();

    assert_eq!(power_state, Some(RedfishPowerState::On));
    assert_eq!(root_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn invalidate_cache_refetches_service_root() {
    let server = MockServer::start().await;
    let first_collection_path = "/redfish/v1/Oem/Nvidia/SystemsA";
    let second_collection_path = "/redfish/v1/Oem/Nvidia/SystemsB";
    let first_member_path = "/redfish/v1/Oem/Nvidia/SystemsA/primary";
    let second_member_path = "/redfish/v1/Oem/Nvidia/SystemsB/primary";

    let root_calls = mount_dynamic_root_nav(
        &server,
        "Systems",
        &[first_collection_path, second_collection_path],
        2,
    )
    .await;

    mount_get_json(
        &server,
        first_collection_path,
        collection_body(
            first_collection_path,
            "#ComputerSystemCollection.ComputerSystemCollection",
            "Computer System Collection",
            &[first_member_path],
        ),
        1,
    )
    .await;

    mount_get_json(
        &server,
        second_collection_path,
        collection_body(
            second_collection_path,
            "#ComputerSystemCollection.ComputerSystemCollection",
            "Computer System Collection",
            &[second_member_path],
        ),
        1,
    )
    .await;

    mount_get_json(
        &server,
        first_member_path,
        system_with_power_state(first_member_path, "On"),
        1,
    )
    .await;

    mount_get_json(
        &server,
        second_member_path,
        system_with_power_state(second_member_path, "Off"),
        1,
    )
    .await;

    let client = client_for(&server);

    assert_eq!(
        client
            .computer_system_power_state("System_0")
            .await
            .unwrap(),
        Some(RedfishPowerState::On)
    );

    client.invalidate_cache().await;

    assert_eq!(
        client
            .computer_system_power_state("System_0")
            .await
            .unwrap(),
        Some(RedfishPowerState::Off)
    );

    assert_eq!(root_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn manager_collection_refreshes_stale_service_root_link() {
    let server = MockServer::start().await;
    let stale_collection_path = "/redfish/v1/Oem/Nvidia/StaleManagers";
    let collection_path = "/redfish/v1/Oem/Nvidia/ManagerInventory";
    let member_path = "/redfish/v1/Oem/Nvidia/ManagerInventory/primary";

    let root_calls = mount_dynamic_root_nav(
        &server,
        "Managers",
        &[stale_collection_path, collection_path],
        2,
    )
    .await;

    mount_get_status(&server, stale_collection_path, 503, 1).await;

    mount_get_json(
        &server,
        collection_path,
        collection_body(
            collection_path,
            "#ManagerCollection.ManagerCollection",
            "Manager Collection",
            &[member_path],
        ),
        1,
    )
    .await;

    mount_get_json(&server, member_path, manager_with_reset(member_path), 1).await;

    mount_reset_action_response(
        &server,
        MANAGER_RESET_TARGET,
        "ForceRestart",
        ResponseTemplate::new(204),
    )
    .await;

    client_for(&server)
        .reset_manager("BMC_0", ResetType::ForceRestart)
        .await
        .unwrap();

    assert_eq!(root_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn computer_system_lookup_skips_failed_non_target_member() {
    let server = MockServer::start().await;
    let failed_member_path = "/redfish/v1/Systems/BrokenSibling";
    let member_path = "/redfish/v1/Oem/Nvidia/SystemInventory/primary";

    mount_system_members(&server, &[failed_member_path, member_path], 1, 1).await;
    mount_get_status(&server, failed_member_path, 500, 1).await;

    mount_get_json(
        &server,
        member_path,
        system_with_power_state(member_path, "On"),
        1,
    )
    .await;

    let power_state = client_for(&server)
        .computer_system_power_state("System_0")
        .await
        .unwrap();

    assert_eq!(power_state, Some(RedfishPowerState::On));
}

#[tokio::test]
async fn multipart_update_firmware_uses_update_service_multipart_uri() {
    let server = MockServer::start().await;
    let upload_path = "/redfish/v1/UpdateService/update-multipart";

    mount_update_service(&server, upload_path, 1, 1).await;

    mount_multipart_update(
        &server,
        upload_path,
        true,
        r#"["HGX_FW_BMC_0"]"#,
        json_response(json!({
            "@odata.id": "/redfish/v1/TaskService/Tasks/42",
            "Id": "42"
        })),
    )
    .await;

    let firmware = firmware_file();
    let firmware_reader = tokio::fs::File::open(firmware.path())
        .await
        .unwrap()
        .compat();

    let update_stream = DataStream::new("firmware.bin", firmware_reader).with_content_length(8);

    let task_id = client_for(&server)
        .multipart_update_firmware_with_timeout(
            update_stream,
            vec!["HGX_FW_BMC_0".to_owned()],
            true,
            Duration::from_secs(120),
        )
        .await
        .unwrap();

    assert_eq!(task_id, "42");
}

#[tokio::test]
async fn multipart_update_firmware_refreshes_stale_service_root_link() {
    let server = MockServer::start().await;
    let stale_service_path = "/redfish/v1/Oem/Nvidia/StaleUpdateService";
    let service_path = "/redfish/v1/UpdateService";
    let upload_path = "/redfish/v1/UpdateService/update-multipart";

    let root_calls = mount_dynamic_root_nav(
        &server,
        "UpdateService",
        &[stale_service_path, service_path],
        2,
    )
    .await;

    mount_get_status(&server, stale_service_path, 404, 1).await;

    mount_get_json(
        &server,
        service_path,
        update_service_body(service_path, upload_path),
        1,
    )
    .await;

    mount_multipart_update(
        &server,
        upload_path,
        false,
        "[]",
        json_response(json!({
            "@odata.id": "/redfish/v1/TaskService/Tasks/42",
            "Id": "42"
        })),
    )
    .await;

    let file = firmware_file();
    let task_id = client_for(&server)
        .multipart_update_firmware_from_path(file.path(), Vec::new(), false)
        .await
        .unwrap();

    assert_eq!(task_id, "42");
    assert_eq!(root_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn multipart_update_firmware_rejects_invalid_inputs() {
    let firmware = firmware_file();
    let firmware_reader = tokio::fs::File::open(firmware.path())
        .await
        .unwrap()
        .compat();

    let error = test_client("zero-upload-timeout.example", "secret", true, true)
        .multipart_update_firmware_with_timeout(
            DataStream::new("firmware.bin", firmware_reader),
            Vec::new(),
            false,
            Duration::ZERO,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RedfishError::InvalidArgument(_)));

    let error = test_client("invalid-firmware-path.example", "secret", true, true)
        .multipart_update_firmware_from_path("/", Vec::new(), false)
        .await
        .unwrap_err();

    assert!(matches!(error, RedfishError::InvalidArgument(_)));
}

#[tokio::test]
async fn multipart_update_firmware_returns_task_ids_from_poll_locations() {
    let locations = [
        "/redfish/v1/TaskService/Tasks/43?monitor=abc",
        "/redfish/v1/TaskService/TaskMonitors/43?monitor=abc",
        "/redfish/v1/TaskService/Tasks/43/Monitor?monitor=abc",
    ];
    let server = MockServer::start().await;
    let upload_path = "/redfish/v1/UpdateService/update-multipart";

    mount_update_service(&server, upload_path, 3, 3).await;

    let response_locations = locations;
    let response_index = Arc::new(AtomicUsize::new(0));
    let response_index_for_mock = response_index.clone();
    Mock::given(method("POST"))
        .and(path(upload_path))
        .and(|request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(r#"name="UpdateParameters""#)
                && body.contains(r#"name="UpdateFile""#)
                && body.contains(r#""ForceUpdate":false"#)
                && body.contains(r#""Targets":[]"#)
        })
        .respond_with(move |_: &wiremock::Request| {
            let index = response_index_for_mock.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(202).insert_header("Location", response_locations[index])
        })
        .expect(3)
        .mount(&server)
        .await;

    for _location in locations {
        let file = firmware_file();
        let task_id = client_for(&server)
            .multipart_update_firmware_from_path(file.path(), Vec::new(), false)
            .await
            .unwrap();

        assert_eq!(task_id, "43");
    }
    assert_eq!(response_index.load(Ordering::SeqCst), locations.len());
}

#[tokio::test]
async fn reset_computer_system_accepts_success_responses() {
    for response in [ResponseTemplate::new(204), ResponseTemplate::new(200)] {
        let server = MockServer::start().await;
        let member_path = "/redfish/v1/Oem/Nvidia/SystemInventory/primary";

        mount_system_member(
            &server,
            member_path,
            system_with_reset(member_path),
            1,
            1,
            1,
        )
        .await;

        mount_reset_action_response(&server, SYSTEM_RESET_TARGET, "PowerCycle", response).await;

        client_for(&server)
            .reset_computer_system("System_0", ResetType::PowerCycle)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn reset_manager_accepts_success_responses_and_invalidates_service_root() {
    for response in [ResponseTemplate::new(204), ResponseTemplate::new(200)] {
        let server = MockServer::start().await;
        let member_path = "/redfish/v1/Oem/Nvidia/ManagerInventory/primary";

        // Discovery may be retried once when nv-redfish surfaces a transient
        // NotFound/Unavailable response. The manager resource and reset action
        // must still be requested exactly once.
        mount_manager_member(
            &server,
            member_path,
            manager_with_reset(member_path),
            1..=2,
            1..=2,
            1,
        )
        .await;

        mount_reset_action_response(&server, MANAGER_RESET_TARGET, "ForceRestart", response).await;

        let client = client_for(&server);

        client
            .reset_manager("BMC_0", ResetType::ForceRestart)
            .await
            .unwrap();

        assert!(client.root.lock().await.is_none());
    }
}

#[tokio::test]
async fn reset_manager_propagates_success_body_decode_errors() {
    let server = MockServer::start().await;
    let member_path = "/redfish/v1/Oem/Nvidia/ManagerInventory/primary";

    mount_manager_member(
        &server,
        member_path,
        manager_with_reset(member_path),
        1,
        1,
        1,
    )
    .await;

    mount_reset_action_response(
        &server,
        MANAGER_RESET_TARGET,
        "ForceRestart",
        json_response(json!({"unexpected": true})),
    )
    .await;

    let error = client_for(&server)
        .reset_manager("BMC_0", ResetType::ForceRestart)
        .await
        .unwrap_err();

    assert!(matches!(error, RedfishError::Internal(_)));
}

#[tokio::test]
async fn manager_lookup_skips_failed_non_target_member() {
    let server = MockServer::start().await;
    let failed_member_path = "/redfish/v1/Managers/BrokenSibling";
    let member_path = "/redfish/v1/Oem/Nvidia/ManagerInventory/primary";

    mount_manager_members(&server, &[failed_member_path, member_path], 1, 1).await;
    mount_get_status(&server, failed_member_path, 500, 1).await;
    mount_get_json(&server, member_path, manager_with_reset(member_path), 1).await;

    mount_reset_action_response(
        &server,
        MANAGER_RESET_TARGET,
        "ForceRestart",
        ResponseTemplate::new(204),
    )
    .await;

    client_for(&server)
        .reset_manager("BMC_0", ResetType::ForceRestart)
        .await
        .unwrap();
}

#[tokio::test]
async fn processor_topology_uses_computer_system_processor_navigation() {
    let server = MockServer::start().await;
    let system_path = "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard";
    let processors_path = "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard/Accelerators";
    let processor_path =
        "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard/Accelerators/primary-gpu";

    mount_system_member(
        &server,
        system_path,
        system_with_processors(
            system_path,
            "HGX_Baseboard_0",
            "HGX Baseboard",
            processors_path,
        ),
        1,
        1,
        1,
    )
    .await;

    mount_get_json(
        &server,
        processors_path,
        processor_collection(processors_path, &[processor_path]),
        1,
    )
    .await;

    mount_get_json(
        &server,
        processor_path,
        processor_with_topology(processor_path, "GPU_0", "Primary GPU", "chassis-01", 7, 2),
        1,
    )
    .await;

    let topology = client_for(&server)
        .computer_system_processor_oem_nvidia_mnnvlink_topology("HGX_Baseboard_0", "GPU_0")
        .await
        .unwrap();

    assert_eq!(topology.chassis_sn, "chassis-01");
    assert_eq!(topology.slot_number, 7);
    assert_eq!(topology.tray_index, 2);
}

#[tokio::test]
async fn processor_topology_discovery_skips_non_topology_resources() {
    let server = MockServer::start().await;
    let failed_system_path = "/redfish/v1/Systems/Unavailable";
    let empty_system_path = "/redfish/v1/Systems/BMC_System";
    let topology_system_path = "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard-primary";
    let processors_path = "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard-primary/Accelerators";
    let non_topology_processor_path =
        "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard-primary/Accelerators/cpu-primary";

    let processor_path =
        "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard-primary/Accelerators/accelerator-primary";

    mount_system_members(
        &server,
        &[failed_system_path, empty_system_path, topology_system_path],
        1,
        1,
    )
    .await;

    mount_get_status(&server, failed_system_path, 500, 1).await;

    mount_get_json(
        &server,
        empty_system_path,
        system_without_processors(empty_system_path, "BMC_System", "BMC System"),
        1,
    )
    .await;

    mount_get_json(
        &server,
        topology_system_path,
        system_with_processors(
            topology_system_path,
            "Baseboard-Primary",
            "Baseboard Primary",
            processors_path,
        ),
        1,
    )
    .await;

    mount_get_json(
        &server,
        processors_path,
        processor_collection(
            processors_path,
            &[non_topology_processor_path, processor_path],
        ),
        1,
    )
    .await;

    mount_get_json(
        &server,
        non_topology_processor_path,
        processor_without_topology(
            non_topology_processor_path,
            "CPU-Primary",
            "Primary CPU",
            "CPU",
        ),
        1,
    )
    .await;

    mount_get_json(
        &server,
        processor_path,
        processor_with_topology(
            processor_path,
            "Accelerator-Primary",
            "Primary Accelerator",
            "chassis-02",
            8,
            3,
        ),
        1,
    )
    .await;

    let topology = client_for(&server)
        .nvidia_mnnvlink_topology()
        .await
        .unwrap();

    assert_eq!(topology.chassis_sn, "chassis-02");
    assert_eq!(topology.slot_number, 8);
    assert_eq!(topology.tray_index, 3);
}

#[tokio::test]
async fn processor_topology_discovery_returns_fetch_error_when_no_candidate_readable() {
    let server = MockServer::start().await;
    let system_path = "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard-primary";
    let processors_path = "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard-primary/Accelerators";
    let processor_path =
        "/redfish/v1/Oem/Nvidia/SystemInventory/baseboard-primary/Accelerators/accelerator-primary";

    mount_system_members(&server, &[system_path], 1, 1).await;

    mount_get_json(
        &server,
        system_path,
        system_with_processors(
            system_path,
            "Baseboard-Primary",
            "Baseboard Primary",
            processors_path,
        ),
        1,
    )
    .await;

    mount_get_json(
        &server,
        processors_path,
        processor_collection(processors_path, &[processor_path]),
        1,
    )
    .await;

    mount_get_status(&server, processor_path, 500, 1).await;

    let error = client_for(&server)
        .nvidia_mnnvlink_topology()
        .await
        .unwrap_err();

    match error {
        RedfishError::Internal(message) => {
            assert!(message.contains(processor_path));
            assert!(message.contains("HTTP 500"));
        }
        other => panic!("expected Internal, got {other:?}"),
    }
}
