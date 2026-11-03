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

use serde::Deserialize;
use serde_json::json;
use url::Url;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate, Times};

const SYSTEM_RESET_TARGET: &str = "/custom/Systems/System_0/Reset";
const MANAGER_RESET_TARGET: &str = "/custom/Managers/BMC_0/Reset";

#[test]
fn resource_lookup_matches_id_or_uri_suffix() {
    let resource = json!({
        "@odata.id": "/redfish/v1/Resources/uri-id",
        "Id": "resource-id",
        "Name": "Resource",
    });

    let system: ComputerSystemSchema = serde_json::from_value(resource.clone()).unwrap();
    let manager: ManagerSchema = serde_json::from_value(resource.clone()).unwrap();
    let processor: ProcessorSchema = serde_json::from_value(resource).unwrap();

    for (expected_id, matches) in [("resource-id", true), ("uri-id", true), ("other-id", false)] {
        let system_matches = computer_system_schema_matches(&system, expected_id);
        let manager_matches = manager_schema_matches(&manager, expected_id);
        let processor_matches = processor_schema_matches(&processor, expected_id);

        assert_eq!(system_matches, matches, "ComputerSystem: {expected_id}");
        assert_eq!(manager_matches, matches, "Manager: {expected_id}");
        assert_eq!(processor_matches, matches, "Processor: {expected_id}");
    }
}

fn client_for(server: &MockServer) -> RedfishClient {
    let url = Url::parse(&server.uri()).unwrap();

    let bmc = HttpBmc::new(
        http_client(
            true,
            RedfishClient::CONNECT_TIMEOUT,
            RedfishClient::REQUEST_TIMEOUT,
        )
        .unwrap(),
        url,
        credentials("secret"),
        CacheSettings::with_capacity(0),
    );

    RedfishClient {
        bmc: Arc::new(bmc),
        root: Arc::new(AsyncMutex::new(None)),
    }
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

#[tokio::test]
async fn configured_power_paths_bypass_discovery() {
    let server = MockServer::start().await;
    let state_path = "/redfish/v1/Oem/Compute/Host";
    let reset_path = "/redfish/v1/Oem/Compute/Reset";
    let manager_path = "/redfish/v1/Oem/Bmc/Reset";
    mount_get_json(
        &server,
        state_path,
        system_with_power_state(state_path, "Off"),
        1,
    )
    .await;
    mount_reset_action_response(&server, reset_path, "On", ResponseTemplate::new(204)).await;
    mount_reset_action_response(
        &server,
        manager_path,
        "ForceRestart",
        ResponseTemplate::new(204),
    )
    .await;

    let client = client_for(&server);
    assert_eq!(
        client
            .computer_system_power_state_with_path("IgnoredId", Some(state_path))
            .await
            .unwrap(),
        Some(RedfishPowerState::Off)
    );
    client
        .reset_computer_system_with_paths(
            "IgnoredId",
            Some(state_path),
            Some(reset_path),
            ResetType::On,
        )
        .await
        .unwrap();
    client
        .reset_manager_with_path("IgnoredId", Some(manager_path), ResetType::ForceRestart)
        .await
        .unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
}

#[tokio::test]
async fn configured_system_resource_supplies_discovered_reset_action() {
    let server = MockServer::start().await;
    let state_path = "/redfish/v1/Oem/Compute/Host";
    mount_get_json(&server, state_path, system_with_reset(state_path), 1).await;
    mount_reset_action_response(
        &server,
        SYSTEM_RESET_TARGET,
        "ForceOff",
        ResponseTemplate::new(204),
    )
    .await;
    client_for(&server)
        .reset_computer_system_with_paths("IgnoredId", Some(state_path), None, ResetType::ForceOff)
        .await
        .unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn failed_power_path_overrides_do_not_fall_back_to_discovery() {
    let server = MockServer::start().await;
    let state_path = "/redfish/v1/Oem/Compute/Missing";
    let reset_path = "/redfish/v1/Oem/Compute/Failed";
    mount_get_status(&server, state_path, 404, 1).await;
    mount_root_nav(&server, "Managers", "/redfish/v1/Managers", 1).await;
    Mock::given(method("POST"))
        .and(path(reset_path))
        .respond_with(ResponseTemplate::new(500))
        .expect(2)
        .mount(&server)
        .await;
    let client = client_for(&server);
    assert!(
        client
            .computer_system_power_state_with_path("System_0", Some(state_path))
            .await
            .is_err()
    );
    assert!(
        client
            .reset_computer_system_with_paths("System_0", None, Some(reset_path), ResetType::On)
            .await
            .is_err()
    );
    client.root().await.unwrap();
    assert!(client.root.lock().await.is_some());
    assert!(
        client
            .reset_manager_with_path("BMC_0", Some(reset_path), ResetType::ForceRestart)
            .await
            .is_err()
    );
    assert!(client.root.lock().await.is_none());
    assert_eq!(server.received_requests().await.unwrap().len(), 4);
}

#[tokio::test]
async fn unsafe_configured_power_paths_fail_before_transport() {
    let server = MockServer::start().await;
    let client = client_for(&server);
    for bad in [
        "https://other.invalid/redfish/v1/reset",
        "//other.invalid/reset",
        "/admin",
        "/redfish/v1beta",
        "/redfish/v1/../admin",
        "/redfish/v1/%2e%2e/admin",
        "/redfish/v1/%2fadmin",
        "/redfish/v1/reset?x=1",
        "/redfish/v1/reset#fragment",
        "/redfish/v1/./reset",
        "/redfish/v1//reset",
        "/redfish/v1/reset bad",
        "/redfish/v1/reset\\\\bad",
    ] {
        assert!(
            matches!(
                client
                    .computer_system_power_state_with_path("System_0", Some(bad))
                    .await,
                Err(RedfishError::InvalidArgument(_))
            ),
            "{bad}"
        );
        assert!(
            matches!(
                client
                    .reset_computer_system_with_paths("System_0", None, Some(bad), ResetType::On)
                    .await,
                Err(RedfishError::InvalidArgument(_))
            ),
            "{bad}"
        );
        assert!(
            matches!(
                client
                    .reset_manager_with_path("BMC_0", Some(bad), ResetType::ForceRestart)
                    .await,
                Err(RedfishError::InvalidArgument(_))
            ),
            "{bad}"
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn configured_request_timeout_is_enforced_without_changing_default_client() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(root_nav_body("Systems", "/redfish/v1/Systems"))
                .set_delay(Duration::from_millis(200)),
        )
        .mount(&server)
        .await;
    let client = RedfishClient::new_with_timeouts(
        "127.0.0.1",
        server.address().port(),
        credentials("secret"),
        true,
        false,
        Duration::from_secs(1),
        Duration::from_millis(20),
    )
    .unwrap();
    let error = client
        .fetch_root()
        .await
        .err()
        .expect("configured timeout must expire");
    assert!(matches!(error, RedfishError::Timeout(_)), "{error}");

    let default_client = RedfishClient::new(
        "127.0.0.1",
        server.address().port(),
        credentials("secret"),
        true,
        false,
    )
    .unwrap();
    let explicit_defaults = RedfishClient::new_with_timeouts(
        "127.0.0.1",
        server.address().port(),
        credentials("secret"),
        true,
        false,
        RedfishClient::CONNECT_TIMEOUT,
        RedfishClient::REQUEST_TIMEOUT,
    )
    .unwrap();
    default_client.fetch_root().await.unwrap();
    explicit_defaults.fetch_root().await.unwrap();
}

#[test]
fn configured_transport_rejects_zero_timeouts() {
    assert!(
        RedfishClient::new_with_timeouts(
            "127.0.0.1",
            443,
            credentials("secret"),
            false,
            true,
            Duration::ZERO,
            Duration::from_secs(30)
        )
        .is_err()
    );
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
    for reset_path in [None, Some("/redfish/v1/Oem/Compute/Reset")] {
        for response in [ResponseTemplate::new(204), ResponseTemplate::new(200)] {
            let server = MockServer::start().await;
            let member_path = "/redfish/v1/Oem/Nvidia/SystemInventory/primary";

            if reset_path.is_none() {
                mount_system_member(
                    &server,
                    member_path,
                    system_with_reset(member_path),
                    1,
                    1,
                    1,
                )
                .await;
            }

            mount_reset_action_response(
                &server,
                reset_path.unwrap_or(SYSTEM_RESET_TARGET),
                "PowerCycle",
                response,
            )
            .await;

            client_for(&server)
                .reset_computer_system_with_paths(
                    "System_0",
                    None,
                    reset_path,
                    ResetType::PowerCycle,
                )
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn reset_manager_accepts_success_responses_and_invalidates_service_root() {
    for reset_path in [None, Some("/redfish/v1/Oem/Bmc/Reset")] {
        for response in [ResponseTemplate::new(204), ResponseTemplate::new(200)] {
            let server = MockServer::start().await;
            let member_path = "/redfish/v1/Oem/Nvidia/ManagerInventory/primary";

            if reset_path.is_some() {
                mount_root_nav(&server, "Managers", "/redfish/v1/Managers", 1).await;
            } else {
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
            }

            mount_reset_action_response(
                &server,
                reset_path.unwrap_or(MANAGER_RESET_TARGET),
                "ForceRestart",
                response,
            )
            .await;

            let client = client_for(&server);
            client.root().await.unwrap();
            assert!(client.root.lock().await.is_some());

            client
                .reset_manager_with_path("BMC_0", reset_path, ResetType::ForceRestart)
                .await
                .unwrap();

            assert!(client.root.lock().await.is_none());
        }
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

#[tokio::test]
async fn get_json_returns_vendor_fields_nv_redfish_does_not_type() {
    let server = MockServer::start().await;
    mount_get_json(
        &server,
        "/redfish/v1/Chassis/PowerShelf_0/PowerSupplies/1",
        json!({
            "PowerState": false,
            "Oem": { "deltaenergysystems": { "Power": true } }
        }),
        1,
    )
    .await;

    let body = client_for(&server)
        .get_json("/redfish/v1/Chassis/PowerShelf_0/PowerSupplies/1")
        .await
        .unwrap();

    assert_eq!(body["PowerState"], json!(false));
    assert_eq!(body["Oem"]["deltaenergysystems"]["Power"], json!(true));
}

#[tokio::test]
async fn get_json_reads_the_next_response_after_the_server_closes_the_connection() {
    let server = MockServer::start().await;
    let uri = "/redfish/v1/UpdateService/FirmwareInventory";
    Mock::given(method("GET"))
        .and(path(uri))
        .respond_with(ResponseTemplate::new(500).insert_header("connection", "close"))
        .expect(2)
        .mount(&server)
        .await;

    let client = client_for(&server);
    for _ in 0..2 {
        let message = client.get_json(uri).await.unwrap_err().to_string();
        assert!(message.contains("HTTP 500"), "{message}");
    }
}

#[derive(Clone, Debug, Deserialize)]
struct TypedPsuOem {
    #[serde(rename = "PowerState")]
    power_state: Option<bool>,
}

#[tokio::test]
async fn get_resource_deserializes_typed_body() {
    let server = MockServer::start().await;
    mount_get_json(
        &server,
        "/redfish/v1/Chassis/PowerShelf_0/PowerSupplies/1",
        json!({
            "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/PowerSupplies/1",
            "PowerState": false
        }),
        1,
    )
    .await;

    let body: TypedPsuOem = client_for(&server)
        .get_resource("/redfish/v1/Chassis/PowerShelf_0/PowerSupplies/1")
        .await
        .unwrap();

    assert_eq!(body.power_state, Some(false));
}

#[test]
fn odata_link_nonempty_skips_empty_id() {
    let empty: ODataLink = serde_json::from_value(json!({ "@odata.id": "" })).unwrap();
    assert_eq!(empty.nonempty(), None);

    let present: ODataLink =
        serde_json::from_value(json!({ "@odata.id": "/redfish/v1/Chassis/1" })).unwrap();
    assert_eq!(present.nonempty(), Some("/redfish/v1/Chassis/1"));

    let collection: ResourceCollection = serde_json::from_value(json!({
        "Members": [{ "@odata.id": "/redfish/v1/Chassis/1" }]
    }))
    .unwrap();
    assert_eq!(collection.members.len(), 1);
}

#[tokio::test]
async fn get_json_rejects_cross_origin_odata_id() {
    let server = MockServer::start().await;
    let error = client_for(&server)
        .get_json("https://evil.example/redfish/v1/Chassis")
        .await
        .unwrap_err();

    match error {
        RedfishError::InvalidArgument(message) => {
            assert!(
                message.contains("origin-relative"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected origin rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn get_json_rejects_protocol_relative_odata_id() {
    let server = MockServer::start().await;
    let error = client_for(&server)
        .get_json("//evil.example/redfish/v1/Chassis")
        .await
        .unwrap_err();

    match error {
        RedfishError::InvalidArgument(message) => {
            assert!(
                message.contains("origin-relative"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected origin rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn post_action_posts_bmc_advertised_target() {
    let server = MockServer::start().await;
    mount_post_json(
        &server,
        "/redfish/v1/Chassis/powershelf/Actions/Chassis.On",
        json!({}),
        ResponseTemplate::new(200).set_body_json(json!({})),
    )
    .await;

    client_for(&server)
        .post_action(
            "/redfish/v1/Chassis/powershelf/Actions/Chassis.On",
            &json!({}),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn post_action_rejects_cross_origin_target() {
    let server = MockServer::start().await;
    let error = client_for(&server)
        .post_action("https://evil.example/Actions/Reset", &json!({}))
        .await
        .unwrap_err();

    match error {
        RedfishError::InvalidArgument(message) => {
            assert!(
                message.contains("origin-relative"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected origin rejection, got {other:?}"),
    }
}

/// A BMC that aborts every resumed TLS handshake and answers a full one with
/// the requested resource. The VR NVL72 switch BMC on s08 behaves this way.
struct BmcRejectingResumption {
    port: u16,
    full_handshakes: Arc<AtomicUsize>,
    resumed_handshakes: Arc<AtomicUsize>,
}

async fn start_bmc_rejecting_resumption() -> BmcRejectingResumption {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(certified.key_pair.serialize_der().into());
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(rustls::ALL_VERSIONS)
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certified.cert.der().clone()], key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let full_handshakes = Arc::new(AtomicUsize::new(0));
    let resumed_handshakes = Arc::new(AtomicUsize::new(0));
    let (full, resumed) = (
        Arc::clone(&full_handshakes),
        Arc::clone(&resumed_handshakes),
    );
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let Ok(mut tls) = acceptor.accept(tcp).await else {
                continue;
            };

            // Dropping the stream closes the connection before any answer.
            if tls.get_ref().1.handshake_kind() == Some(rustls::HandshakeKind::Resumed) {
                resumed.fetch_add(1, Ordering::SeqCst);
                continue;
            }

            full.fetch_add(1, Ordering::SeqCst);
            answer_one_get(&mut tls).await;
        }
    });

    BmcRejectingResumption {
        port,
        full_handshakes,
        resumed_handshakes,
    }
}

async fn answer_one_get<S>(stream: &mut S)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => request.extend_from_slice(&chunk[..read]),
        }
    }

    let request = String::from_utf8_lossy(&request);
    let path = request.split_whitespace().nth(1).unwrap_or("/");
    let body = json!({ "@odata.id": path, "Id": "System_0" }).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

fn tls_client_for(bmc: &BmcRejectingResumption, accept_invalid_certs: bool) -> RedfishClient {
    RedfishClient::new_with_timeouts(
        "127.0.0.1",
        bmc.port,
        credentials("secret"),
        accept_invalid_certs,
        true,
        Duration::from_secs(5),
        Duration::from_secs(10),
    )
    .unwrap()
}

#[tokio::test]
async fn every_bmc_request_completes_a_full_tls_handshake() {
    let bmc = start_bmc_rejecting_resumption().await;
    let client = tls_client_for(&bmc, true);

    for _ in 0..3 {
        client
            .get_json("/redfish/v1/Systems/System_0")
            .await
            .unwrap();
    }

    assert_eq!(bmc.resumed_handshakes.load(Ordering::SeqCst), 0);
    assert_eq!(bmc.full_handshakes.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn a_self_signed_bmc_certificate_is_refused_unless_invalid_certs_are_accepted() {
    let bmc = start_bmc_rejecting_resumption().await;

    let error = tls_client_for(&bmc, false)
        .get_json("/redfish/v1/Systems/System_0")
        .await;

    assert!(error.is_err());
    assert_eq!(bmc.full_handshakes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_redirect_to_another_origin_is_not_followed() {
    let bmc = MockServer::start().await;
    let elsewhere = MockServer::start().await;
    let target = format!("{}/redfish/v1/Systems/System_0", elsewhere.uri());
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Systems/System_0"))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", target.as_str()))
        .mount(&bmc)
        .await;
    mount_get_json(
        &elsewhere,
        "/redfish/v1/Systems/System_0",
        json!({ "Id": "System_0" }),
        0,
    )
    .await;

    let result = client_for(&bmc)
        .get_json("/redfish/v1/Systems/System_0")
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn a_redirect_within_the_bmc_is_followed() {
    let bmc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Systems/System_0"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("Location", "/redfish/v1/Systems/System_1"),
        )
        .mount(&bmc)
        .await;
    mount_get_json(
        &bmc,
        "/redfish/v1/Systems/System_1",
        json!({ "Id": "System_1" }),
        1,
    )
    .await;

    client_for(&bmc)
        .get_json("/redfish/v1/Systems/System_0")
        .await
        .unwrap();
}
