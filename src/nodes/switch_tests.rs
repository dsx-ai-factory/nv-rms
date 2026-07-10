/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

use std::sync::{Arc, Mutex};

use nvue_client::revision::{RevisionCreate, RevisionUpdate};
use nvue_client::system::UserPasswordPatch;
use serde::Serialize;

use super::*;
use crate::domain::node::NodeType;
use crate::domain::rack::{EndpointConfig, EndpointCredentials, NodeConfig};
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockBuilder, MockServer, ResponseTemplate};

type SshCommandLog = Arc<Mutex<Vec<String>>>;
type SshCommandRecorder = Box<dyn Fn(&str) -> Result<String> + Send + Sync + 'static>;

#[derive(Default, Serialize)]
struct EmptyBody {}

#[derive(Serialize)]
struct RevisionState<'a> {
    state: &'a str,
}

#[derive(Serialize)]
struct RevisionStateWithIssue<'a> {
    state: &'a str,
    transition: RevisionTransitionWithIssue<'a>,
}

#[derive(Serialize)]
struct RevisionTransitionWithIssue<'a> {
    issue: RevisionIssueMap<'a>,
}

#[derive(Serialize)]
struct RevisionIssueMap<'a> {
    #[serde(rename = "00000")]
    issue_00000: RevisionIssueEntry<'a>,
}

#[derive(Serialize)]
struct RevisionIssueEntry<'a> {
    message: &'a str,
    severity: &'a str,
}

#[derive(Serialize)]
struct PasswordPolicyError<'a> {
    detail: &'a str,
    status: u16,
    title: &'a str,
}

const TEST_AUTH: &str = "Basic dGVzdHVzZXJuYW1lOnRlc3QtcGFzc3dvcmQ=";
const ROTATED_AUTH: &str = "Basic dGVzdHVzZXJuYW1lOnJvdGF0ZWQtcGFzc3dvcmQ=";

fn with_optional_auth(mock: MockBuilder, auth: Option<&str>) -> MockBuilder {
    match auth {
        Some(auth) => mock.and(header("Authorization", auth)),
        None => mock,
    }
}

async fn mount_system_probe(server: &MockServer, auth: &str, status: u16) {
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .and(header("Authorization", auth))
        .respond_with(ResponseTemplate::new(status).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_create_revision(server: &MockServer, revision_id: &str, auth: Option<&str>) {
    mount_create_revision_response(
        server,
        auth,
        ResponseTemplate::new(200).set_body_json(revision_id),
    )
    .await;
}

async fn mount_create_revision_response(
    server: &MockServer,
    auth: Option<&str>,
    response: ResponseTemplate,
) {
    with_optional_auth(
        Mock::given(method("POST"))
            .and(path("/nvue_v1/revision"))
            .and(body_json(RevisionCreate::default())),
        auth,
    )
    .respond_with(response)
    .expect(1)
    .mount(server)
    .await;
}

async fn mount_stage_password(
    server: &MockServer,
    revision_id: &str,
    username: &str,
    password: &str,
    auth: Option<&str>,
    response: ResponseTemplate,
) {
    with_optional_auth(
        Mock::given(method("PATCH"))
            .and(path(format!("/nvue_v1/system/aaa/user/{username}")))
            .and(query_param("rev", revision_id))
            .and(body_json(UserPasswordPatch::new(password))),
        auth,
    )
    .respond_with(response)
    .expect(1)
    .mount(server)
    .await;
}

async fn mount_stage_password_success(
    server: &MockServer,
    revision_id: &str,
    username: &str,
    password: &str,
    auth: Option<&str>,
) {
    mount_stage_password(
        server,
        revision_id,
        username,
        password,
        auth,
        ResponseTemplate::new(200).set_body_json(EmptyBody::default()),
    )
    .await;
}

async fn mount_apply_revision(server: &MockServer, revision_id: &str, auth: Option<&str>) {
    mount_apply_revision_response(
        server,
        revision_id,
        auth,
        ResponseTemplate::new(200).set_body_json(EmptyBody::default()),
    )
    .await;
}

async fn mount_apply_revision_response(
    server: &MockServer,
    revision_id: &str,
    auth: Option<&str>,
    response: ResponseTemplate,
) {
    with_optional_auth(
        Mock::given(method("PATCH"))
            .and(path(format!("/nvue_v1/revision/{revision_id}")))
            .and(body_json(RevisionUpdate::apply_with_yes_prompt())),
        auth,
    )
    .respond_with(response)
    .expect(1)
    .mount(server)
    .await;
}

async fn mount_revision_get(
    server: &MockServer,
    revision_id: &str,
    auth: Option<&str>,
    response: ResponseTemplate,
) {
    mount_revision_get_with_limit(server, revision_id, auth, response, false).await;
}

async fn mount_revision_get_once(
    server: &MockServer,
    revision_id: &str,
    auth: Option<&str>,
    response: ResponseTemplate,
) {
    mount_revision_get_with_limit(server, revision_id, auth, response, true).await;
}

async fn mount_revision_get_with_limit(
    server: &MockServer,
    revision_id: &str,
    auth: Option<&str>,
    response: ResponseTemplate,
    up_to_once: bool,
) {
    let mock = with_optional_auth(
        Mock::given(method("GET")).and(path(format!("/nvue_v1/revision/{revision_id}"))),
        auth,
    )
    .respond_with(response);

    let mock = if up_to_once {
        mock.up_to_n_times(1)
    } else {
        mock
    };

    mock.expect(1).mount(server).await;
}

async fn mount_save_applied_revision(server: &MockServer, auth: Option<&str>) {
    mount_save_applied_revision_response(
        server,
        auth,
        ResponseTemplate::new(200).set_body_json(EmptyBody::default()),
    )
    .await;
}

async fn mount_save_applied_revision_response(
    server: &MockServer,
    auth: Option<&str>,
    response: ResponseTemplate,
) {
    with_optional_auth(
        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/applied"))
            .and(body_json(RevisionUpdate::save_with_yes_prompt())),
        auth,
    )
    .respond_with(response)
    .expect(1)
    .mount(server)
    .await;
}

async fn mount_save_revision_response(
    server: &MockServer,
    revision_id: &str,
    auth: Option<&str>,
    response: ResponseTemplate,
) {
    with_optional_auth(
        Mock::given(method("PATCH"))
            .and(path(format!("/nvue_v1/revision/{revision_id}")))
            .and(body_json(RevisionUpdate::save_with_yes_prompt())),
        auth,
    )
    .respond_with(response)
    .expect(1)
    .mount(server)
    .await;
}

fn endpoint_config(
    ip_address: &str,
    mac_address: &str,
    port: u16,
    username: &str,
    password: &str,
) -> EndpointConfig {
    endpoint_config_with_name(ip_address, mac_address, port, username, password, None)
}

fn endpoint_config_with_name(
    ip_address: &str,
    mac_address: &str,
    port: u16,
    username: &str,
    password: &str,
    host_name: Option<&str>,
) -> EndpointConfig {
    EndpointConfig::with_credentials(
        Endpoint {
            ip_address: ip_address.into(),
            mac_address: mac_address.into(),
            port,
            host_name: host_name.map(str::to_owned),
        },
        Some(EndpointCredentials::new(username, password)),
        true,
    )
}

// ── System image uninstallation ──────────────────────────────

/// Tests uninstall_system_image under normal circumstances, expecting
/// asserting the format of the request payload and the response job ID.
#[tokio::test]
async fn uninstall_system_image_posts_correct_payload() {
    let server = MockServer::start().await;
    let expected_payload = serde_json::json!({
        "@uninstall": {
            "state": "start",
            "parameters": {"force": true}
        }
    });
    let expected_path = "/nvue_v1/system/image";
    let expected_job_id = "15";

    // Set up mock response for POST request
    Mock::given(method("POST"))
        .and(path(expected_path))
        .and(body_json(&expected_payload))
        .respond_with(ResponseTemplate::new(200).set_body_json(expected_job_id))
        .mount(&server)
        .await;

    // Create test version of Switch with mock HTTP client
    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    // Call the method under test and verify result
    let response = switch.uninstall_system_image().await.unwrap();
    assert_eq!(response, expected_job_id);
}

/// Tests uninstall_system_image when an HTTP error is returned,
/// asserting it is processed into an RmsError correctly.
#[tokio::test]
async fn uninstall_system_image_propagates_http_error() {
    let server = MockServer::start().await;
    let expected_path = "/nvue_v1/system/image";
    Mock::given(method("POST"))
        .and(path(expected_path))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let sw = SwitchGb200Nvidia::for_test(&server.uri());
    let err = sw.uninstall_system_image().await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Unavailable);
}

fn record_ssh_commands() -> (SshCommandLog, SshCommandRecorder) {
    let commands = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&commands);
    let recorder = Box::new(move |command: &str| {
        recorded.lock().unwrap().push(command.to_owned());
        Ok(String::new())
    });

    (commands, recorder)
}

fn recorded_ssh_commands(commands: &SshCommandLog) -> Vec<String> {
    commands.lock().unwrap().clone()
}

async fn assert_set_cluster_state_noops(enabled: bool, current_state: &str) {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": current_state
        })))
        .expect(1)
        .named("current GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);
    switch.set_cluster_state(enabled).await.unwrap();
    assert!(recorded_ssh_commands(&commands).is_empty());
}

async fn assert_set_cluster_state_updates(
    enabled: bool,
    current_state: &str,
    expected_state: &str,
) {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": current_state
        })))
        .up_to_n_times(1)
        .expect(1)
        .named("initial GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": expected_state
        })))
        .expect(1)
        .named("final GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);
    switch.set_cluster_state(enabled).await.unwrap();

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec![
            format!("nv set cluster state {expected_state}"),
            "nv config apply --assume-yes".to_owned(),
            "nv config save".to_owned(),
        ]
    );
}

#[tokio::test]
async fn set_cluster_state_skips_disabled_state_when_already_disabled() {
    assert_set_cluster_state_noops(false, "disabled").await;
}

#[tokio::test]
async fn set_cluster_state_skips_enabled_state_when_already_enabled() {
    assert_set_cluster_state_noops(true, "enabled").await;
}

#[tokio::test]
async fn set_cluster_state_skips_start_state_when_enabling() {
    assert_set_cluster_state_noops(true, "start").await;
}

#[tokio::test]
async fn set_cluster_state_disables_enabled_cluster() {
    assert_set_cluster_state_updates(false, "enabled", "disabled").await;
}

#[tokio::test]
async fn set_cluster_state_enables_disabled_cluster() {
    assert_set_cluster_state_updates(true, "disabled", "enabled").await;
}

#[tokio::test]
async fn set_cluster_state_propagates_command_failure() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled"
        })))
        .expect(1)
        .named("current GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    let commands = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&commands);
    let switch =
        SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(move |command| {
            recorded.lock().unwrap().push(command.to_owned());
            if command == "nv config apply --assume-yes" {
                Err(RmsError::internal("nv config apply failed"))
            } else {
                Ok(String::new())
            }
        });

    let err = switch.set_cluster_state(false).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("nv config apply failed"));
    assert_eq!(
        recorded_ssh_commands(&commands),
        vec![
            "nv set cluster state disabled".to_owned(),
            "nv config apply --assume-yes".to_owned(),
        ]
    );
}

#[tokio::test]
async fn set_cluster_state_errors_when_state_missing() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "nmxc-conn": "down"
        })))
        .expect(1)
        .named("current GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch.set_cluster_state(false).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal);
    assert!(
        err.message
            .contains("unable to determine current cluster state")
    );
}

fn cluster_ready_response(state: &str) -> serde_json::Value {
    serde_json::json!({
        "state": state,
        "nmxc-conn": {"state": "up"}
    })
}

fn nmx_controller_status_response(manager_state: &str) -> serde_json::Value {
    serde_json::json!({
        "status": "ok",
        "additional-info": "CONTROL_PLANE_STATE_CONFIGURED",
        "manager": {
            "state": manager_state
        }
    })
}

fn nmx_controller_stopped_response() -> serde_json::Value {
    serde_json::json!({
        "status": "not ok",
        "addition-info": "STOPPED",
        "manager": {
            "state": "enabled"
        }
    })
}

fn nmx_controller_ready_without_manager_response() -> serde_json::Value {
    serde_json::json!({
        "status": "ok",
        "additional-info": "CONTROL_PLANE_STATE_CONFIGURED"
    })
}

fn nmx_controller_unknown_readiness_response() -> serde_json::Value {
    serde_json::json!({
        "manager": {
            "state": "disabled"
        }
    })
}

fn nmx_controller_unconfigured_response(manager_state: &str) -> serde_json::Value {
    serde_json::json!({
        "status": "not ok",
        "addition-info": "CONTROL_PLANE_STATE_UNCONFIGURED",
        "manager": {
            "state": manager_state
        }
    })
}

#[tokio::test]
async fn enable_grpc_for_external_clients_accepts_ready_app_without_manager_state() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .expect(1)
        .named("ready GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(nmx_controller_ready_without_manager_response()),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("pre-action GET /nvue_v1/cluster/apps/nmx-controller without manager")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .expect(1)
        .named("post-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap();

    assert_eq!(
        status
            .get("manager")
            .and_then(|manager| manager.get("state"))
            .and_then(|state| state.as_str()),
        Some("enabled")
    );
    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action update cluster apps nmx-controller manager enabled".to_owned()]
    );
}

#[tokio::test]
async fn enable_grpc_for_external_clients_attempts_action_when_readiness_unknown() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled"
        })))
        .expect(1)
        .named("cluster GET without nmxc-conn")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_unknown_readiness_response()),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("pre-action GET /nvue_v1/cluster/apps/nmx-controller without readiness fields")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .expect(1)
        .named("post-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap();

    assert_eq!(
        status
            .get("manager")
            .and_then(|manager| manager.get("state"))
            .and_then(|state| state.as_str()),
        Some("enabled")
    );
    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action update cluster apps nmx-controller manager enabled".to_owned()]
    );
}

#[tokio::test]
async fn enable_grpc_for_external_clients_waits_for_cluster_enabled_state() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("start")))
        .up_to_n_times(1)
        .expect(1)
        .named("transitional GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .expect(1)
        .named("ready GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("disabled")),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("pre-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .expect(1)
        .named("post-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap();

    assert_eq!(
        status
            .get("manager")
            .and_then(|manager| manager.get("state"))
            .and_then(|state| state.as_str()),
        Some("enabled")
    );
    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action update cluster apps nmx-controller manager enabled".to_owned()]
    );
}

#[tokio::test]
async fn enable_grpc_for_external_clients_attempts_action_after_readiness_timeout() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("start")))
        .expect(config::MAX_RETRY_ATTEMPTS as u64 + 1)
        .named("transitional GET /nvue_v1/cluster until timeout")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .expect(1)
        .named("post-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap();

    assert_eq!(status["manager"]["state"], "enabled");

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action update cluster apps nmx-controller manager enabled".to_owned()]
    );
}

#[tokio::test]
async fn enable_grpc_for_external_clients_waits_for_nmx_controller_ready_state() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
            "nmxc-conn": {"state": "down"}
        })))
        .up_to_n_times(1)
        .expect(1)
        .named("enabled cluster GET while ready app keeps nmxc-conn down")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .expect(1)
        .named("ready GET /nvue_v1/cluster after nmxc connection recovers")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(nmx_controller_unconfigured_response("disabled")),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("ready GET /nvue_v1/cluster/apps/nmx-controller while nmxc-conn is down")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(nmx_controller_unconfigured_response("disabled")),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("ready GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .expect(1)
        .named("post-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap();

    assert_eq!(
        status
            .get("manager")
            .and_then(|manager| manager.get("state"))
            .and_then(|state| state.as_str()),
        Some("enabled")
    );
    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action update cluster apps nmx-controller manager enabled".to_owned()]
    );
}

#[tokio::test]
async fn enable_grpc_for_external_clients_starts_stopped_nmx_controller() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .expect(2)
        .named("ready GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(nmx_controller_stopped_response()))
        .up_to_n_times(1)
        .expect(1)
        .named("stopped GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("started GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap();

    assert_eq!(
        status
            .get("manager")
            .and_then(|manager| manager.get("state"))
            .and_then(|state| state.as_str()),
        Some("enabled")
    );

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action start cluster apps nmx-controller".to_owned()]
    );
}

#[tokio::test]
async fn ensure_cluster_app_manager_action_ready_starts_stopped_nmx_controller() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .expect(2)
        .named("ready GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(nmx_controller_stopped_response()))
        .up_to_n_times(1)
        .expect(1)
        .named("stopped GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("started GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch
        .ensure_cluster_app_manager_action_ready("nmx-controller")
        .await
        .unwrap();

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action start cluster apps nmx-controller".to_owned()]
    );
}

#[tokio::test]
async fn restart_cluster_app_uses_nvue_stop_then_start_actions() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .and(body_json(serde_json::json!({"@stop": {"state": "start"}})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!("stop-1")))
        .expect(1)
        .named("POST /nvue_v1/cluster/apps/nmx-controller @stop")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/action/stop-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "action_success",
        })))
        .expect(1)
        .named("GET /nvue_v1/action/stop-1")
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .and(body_json(serde_json::json!({"@start": {"state": "start"}})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!("start-1")))
        .expect(1)
        .named("POST /nvue_v1/cluster/apps/nmx-controller @start")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/action/start-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "action_success",
        })))
        .expect(1)
        .named("GET /nvue_v1/action/start-1")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch.restart_cluster_app("nmx-controller").await.unwrap();
}

// ── System image state normalization ──────────────────────────────

#[test]
fn normalize_system_image_state_flat_string_partitions() {
    let raw = serde_json::json!({
        NVOS_PARTITION_1_ID: "nvos-1.2.3",
        NVOS_PARTITION_2_ID: "nvos-1.3.0",
        "current": NVOS_PARTITION_1_ID,
        "next": NVOS_PARTITION_1_ID,
    });
    let state = normalize_system_image_state(&raw);
    assert_eq!(state.partition1_build_id, "nvos-1.2.3");
    assert_eq!(state.partition2_build_id, "nvos-1.3.0");
    assert_eq!(state.current_partition, NVOS_PARTITION_1_ID);
    assert_eq!(state.next_partition, NVOS_PARTITION_1_ID);
    assert_eq!(state.current_build_id, "nvos-1.2.3");
    assert_eq!(state.next_build_id, "nvos-1.2.3");
}

#[test]
fn normalize_system_image_state_object_partitions() {
    let raw = serde_json::json!({
        NVOS_PARTITION_1_ID: {"build-id": "nvos-1.2.3"},
        NVOS_PARTITION_2_ID: {"build-id": "nvos-1.3.0"},
        "current": "1",
        "next": "2",
    });
    let state = normalize_system_image_state(&raw);
    assert_eq!(state.current_partition, NVOS_PARTITION_1_ID);
    assert_eq!(state.next_partition, NVOS_PARTITION_2_ID);
    assert_eq!(state.current_build_id, "nvos-1.2.3");
    assert_eq!(state.next_build_id, "nvos-1.3.0");
}

#[test]
fn normalize_system_image_state_infers_partition_from_build_id() {
    let raw = serde_json::json!({
        NVOS_PARTITION_1_ID: "nvos-1.2.3",
        NVOS_PARTITION_2_ID: "nvos-1.3.0",
        "current": "nvos-1.3.0",
        "next": "nvos-1.3.0",
    });
    let state = normalize_system_image_state(&raw);
    // `current`/`next` don't resolve as partition ids directly, but the
    // build-id fallback finds partition2 for both.
    assert_eq!(state.current_partition, NVOS_PARTITION_2_ID);
    assert_eq!(state.next_partition, NVOS_PARTITION_2_ID);
    assert_eq!(state.current_build_id, "nvos-1.3.0");
    assert_eq!(state.next_build_id, "nvos-1.3.0");
}

// ── system_image_listing_contains ────────────────────────────────

#[test]
fn system_image_listing_contains_recursive() {
    let listing = serde_json::json!({
        "files": {
            "nvos-1.2.3.bin": {"@odata.id": "/foo"},
            "other.bin": {"size": 42},
        }
    });
    assert!(system_image_listing_contains(&listing, "nvos-1.2.3.bin"));
    assert!(system_image_listing_contains(&listing, "other.bin"));
    assert!(!system_image_listing_contains(&listing, "missing.bin"));
}

#[test]
fn system_image_listing_contains_array_of_strings() {
    let listing = serde_json::json!(["a.bin", "b.bin"]);
    assert!(system_image_listing_contains(&listing, "a.bin"));
    assert!(!system_image_listing_contains(&listing, "c.bin"));
}

// ── infer_target_build_id ─────────────────────────────────────────

#[test]
fn infer_target_build_id_happy_path() {
    assert_eq!(
        infer_target_build_id("nvos-25.02.0123.bin").unwrap(),
        "nvos-25.02.0123"
    );
    assert_eq!(infer_target_build_id("1.2.3.bin").unwrap(), "nvos-1.2.3");
}

#[test]
fn infer_target_build_id_rejects_no_version() {
    let err = infer_target_build_id("nvos-latest.bin").unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("unable to infer"));
}

#[test]
fn infer_target_build_id_rejects_conflicting_versions() {
    let err = infer_target_build_id("nvos-1.2.3-also-4.5.6.bin").unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("multiple version patterns"));
}

// ── Error classification ──────────────────────────────────────────

#[test]
fn is_http_unauthorized_error_matches_typed_error() {
    let e = RmsError::unauthenticated("NVUE rejected credentials");

    assert!(is_http_unauthorized_error(&e));

    let e = RmsError::internal("HTTP GET /nvue_v1/system/image returned 500");
    assert!(!is_http_unauthorized_error(&e));
}

#[test]
fn is_retryable_system_image_error_covers_transports() {
    assert!(is_retryable_system_image_error(&RmsError::new(
        ErrorCode::ConnectionRefused,
        "connection refused"
    )));
    assert!(is_retryable_system_image_error(&RmsError::timeout("slow")));
    assert!(is_retryable_system_image_error(&RmsError::new(
        ErrorCode::Unavailable,
        "unavailable"
    )));
    assert!(is_retryable_system_image_error(&RmsError::new(
        ErrorCode::DnsResolutionFailed,
        "dns"
    )));
    assert!(!is_retryable_system_image_error(
        &RmsError::invalid_argument("x")
    ));
}

#[test]
fn is_retryable_steady_state_error_adds_system_image_401_like_errors() {
    let err = RmsError::internal("HTTP GET /nvue_v1/system/image returned 502 Bad Gateway");
    assert!(is_retryable_steady_state_error(&err));
    // But an unrelated internal error is not retryable.
    assert!(!is_retryable_steady_state_error(&RmsError::internal(
        "something else"
    )));
}

#[test]
fn is_retryable_install_poll_error_covers_action_path() {
    let err = RmsError::internal("HTTP GET /nvue_v1/action/42 returned 404");
    assert!(is_retryable_install_poll_error(&err));
    assert!(!is_retryable_install_poll_error(&RmsError::internal(
        "something else"
    )));
}

// ── Install poll heuristics ──────────────────────────────────────

fn mk_status(state: &str, percent: i32, completed: bool, msg: &str) -> FirmwareTaskStatus {
    FirmwareTaskStatus {
        state: state.into(),
        percent,
        completed,
        status: String::new(),
        message: msg.into(),
    }
}

#[test]
fn is_install_poll_success_accepts_action_success_and_action_error_100pct() {
    assert!(is_install_poll_success(&mk_status(
        "action_success",
        100,
        true,
        ""
    )));
    assert!(is_install_poll_success(&mk_status(
        "action_error",
        100,
        true,
        ""
    )));
    assert!(!is_install_poll_success(&mk_status(
        "action_error",
        99,
        true,
        ""
    )));
    assert!(!is_install_poll_success(&mk_status(
        "action_running",
        50,
        false,
        ""
    )));
}

#[test]
fn is_install_progress_state_detects_installing_image() {
    let s = mk_status("action_running", 50, false, "Installing image nvos-1.2.3");
    assert!(is_install_progress_state(&s));
    let s = mk_status("action_running", 50, false, "unrelated");
    assert!(!is_install_progress_state(&s));
}

#[test]
fn is_install_reboot_transition_state_detects_offline_variants() {
    assert!(is_install_reboot_transition_state(&mk_status(
        "action_error",
        100,
        true,
        "Disconnecting from NVOS"
    )));
    assert!(is_install_reboot_transition_state(&mk_status(
        "action_running",
        80,
        false,
        "System is offline during reboot"
    )));
    assert!(!is_install_reboot_transition_state(&mk_status(
        "action_running",
        50,
        false,
        "Still going"
    )));
}

#[test]
fn is_install_handoff_to_reboot_requires_completed_100pct_and_install_text() {
    // happy: action_error + completed + 100% + installing image
    let s = mk_status("action_error", 100, true, "Installing image nvos-1.2.3");
    assert!(is_install_handoff_to_reboot(&s));

    // percent != 100 -> no handoff
    let s = mk_status("action_error", 99, true, "Installing image nvos-1.2.3");
    assert!(!is_install_handoff_to_reboot(&s));

    // not completed -> no
    let s = mk_status("action_error", 100, false, "Installing image nvos-1.2.3");
    assert!(!is_install_handoff_to_reboot(&s));

    // action_failed never hands off
    let s = mk_status("action_failed", 100, true, "Installing image nvos-1.2.3");
    assert!(!is_install_handoff_to_reboot(&s));
}

#[test]
fn describe_install_poll_includes_all_fields() {
    let s = FirmwareTaskStatus {
        state: "action_running".into(),
        status: "progress".into(),
        message: "Installing image".into(),
        completed: false,
        percent: 42,
    };
    let desc = describe_install_poll(&s);
    assert!(desc.contains("state=action_running"));
    assert!(desc.contains("status=progress"));
    assert!(desc.contains("message=Installing image"));
    assert!(desc.contains("completed=false"));
    assert!(desc.contains("percent=42"));
}

// ── Issue-message extraction ──────────────────────────────────────

#[test]
fn extract_action_issue_message_empty_when_missing() {
    let resp = serde_json::json!({"detail": "something"});
    assert_eq!(extract_action_issue_message(&resp), "");
}

#[test]
fn extract_action_issue_message_joins_string_issues() {
    let resp = serde_json::json!({"issue": ["first", "second"]});
    assert_eq!(extract_action_issue_message(&resp), "first; second");
}

#[test]
fn extract_action_issue_message_reads_object_message_field() {
    let resp = serde_json::json!({
        "issue": [{"message": "nvos install aborted"}],
    });
    assert_eq!(extract_action_issue_message(&resp), "nvos install aborted");
}

#[test]
fn extract_action_issue_message_reads_nested_data_msg() {
    let resp = serde_json::json!({
        "issue": [{"data": {"msg": "disk full"}}],
    });
    assert_eq!(extract_action_issue_message(&resp), "disk full");
}

// ── Validation functions ────────────────────────────────────────

#[test]
fn valid_firmware_filenames() {
    assert!(is_valid_firmware_filename("firmware.bin"));
    assert!(is_valid_firmware_filename("fw-v1.2_3.fwpkg"));
    assert!(is_valid_firmware_filename("a"));
    assert!(!is_valid_firmware_filename(""));
    assert!(!is_valid_firmware_filename(".hidden"));
    assert!(!is_valid_firmware_filename("path/traversal"));
    assert!(!is_valid_firmware_filename("spaces here"));
    assert!(!is_valid_firmware_filename("special!char"));
}

#[test]
fn valid_identifiers() {
    assert!(is_valid_identifier("nmx-telemetry"));
    assert!(is_valid_identifier("app_1"));
    assert!(is_valid_identifier("abc123"));
    assert!(!is_valid_identifier(""));
    assert!(!is_valid_identifier("has space"));
    assert!(!is_valid_identifier("path/slash"));
    assert!(!is_valid_identifier("admin/../system"));
    assert!(!is_valid_identifier("admin?rev=other"));
}

#[test]
fn valid_system_usernames() {
    assert!(is_valid_system_username("admin"));
    assert!(is_valid_system_username("monitoring"));
    assert!(is_valid_system_username("admin123"));
    assert!(!is_valid_system_username(""));
    assert!(!is_valid_system_username("backup-admin"));
    assert!(!is_valid_system_username("admin_user"));
    assert!(!is_valid_system_username("admin$"));
    assert!(!is_valid_system_username("admin?rev=other"));
}

#[test]
fn valid_components() {
    for c in &["bmc", "fpga", "erot", "cpld", "bios", "transceiver"] {
        assert!(is_valid_component(c), "expected valid: {c}");
    }
    assert!(is_valid_component("BMC"));
    assert!(is_valid_component("Fpga"));
    assert!(!is_valid_component("gpu"));
    assert!(!is_valid_component(""));
}

#[test]
fn valid_firmware_inventory_components() {
    assert!(is_valid_firmware_inventory_component("bmc"));
    assert!(is_valid_firmware_inventory_component("cpld"));
    assert!(is_valid_firmware_inventory_component("cpld1"));
    assert!(is_valid_firmware_inventory_component("CPLD42"));
    assert!(!is_valid_firmware_inventory_component("cpldx"));
    assert!(!is_valid_firmware_inventory_component("gpu"));
    assert!(!is_valid_firmware_inventory_component(""));
}

#[test]
fn firmware_inventory_endpoint_cpld_redirect() {
    assert_eq!(
        firmware_inventory_endpoint_component("cpld", false),
        "CPLD1"
    );
    assert_eq!(firmware_inventory_endpoint_component("cpld", true), "cpld");
    assert_eq!(firmware_inventory_endpoint_component("bmc", false), "bmc");
    assert_eq!(firmware_inventory_endpoint_component("bmc", true), "bmc");
}

// ── Job ID extraction ───────────────────────────────────────────

#[test]
fn extract_job_id_from_string() {
    let resp = serde_json::json!("job-42");
    assert_eq!(extract_job_id(&resp).unwrap(), "job-42");
}

#[test]
fn extract_job_id_from_number() {
    let resp = serde_json::json!(42);
    assert_eq!(extract_job_id(&resp).unwrap(), "42");
}

#[test]
fn extract_job_id_from_job_id_field() {
    let resp = serde_json::json!({"job_id": "abc-123"});
    assert_eq!(extract_job_id(&resp).unwrap(), "abc-123");
}

#[test]
fn extract_job_id_from_id_field() {
    let resp = serde_json::json!({"id": "xyz-789"});
    assert_eq!(extract_job_id(&resp).unwrap(), "xyz-789");
}

#[test]
fn extract_job_id_rejects_unknown_object() {
    let resp = serde_json::json!({"other": "data"});

    assert!(extract_job_id(&resp).is_err());
}

#[test]
fn extract_job_id_rejects_null() {
    let resp = serde_json::json!(null);

    assert!(extract_job_id(&resp).is_err());
}

// ── Password rotation revision flow ─────────────────────────────

#[tokio::test]
async fn password_rotation_same_password_verifies_active_credentials_before_noop() {
    let server = MockServer::start().await;
    mount_system_probe(&server, TEST_AUTH, 200).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    switch
        .update_system_user_password_persisted("testusername", "test-password")
        .await
        .unwrap();

    let requests = server
        .received_requests()
        .await
        .expect("wiremock should record requests");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, wiremock::http::Method::Get);
    assert_eq!(requests[0].url.path(), "/nvue_v1/system");
}

#[tokio::test]
async fn password_rotation_same_password_accepts_non_401_probe_status_before_noop() {
    let server = MockServer::start().await;
    mount_system_probe(&server, TEST_AUTH, 400).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    switch
        .update_system_user_password_persisted("testusername", "test-password")
        .await
        .unwrap();

    let requests = server
        .received_requests()
        .await
        .expect("wiremock should record requests");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, wiremock::http::Method::Get);
    assert_eq!(requests[0].url.path(), "/nvue_v1/system");
}

#[tokio::test]
async fn password_rotation_same_password_fails_when_active_credentials_are_rejected() {
    let server = MockServer::start().await;
    mount_system_probe(&server, TEST_AUTH, 401).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let result = switch
        .update_system_user_password_persisted("testusername", "test-password")
        .await;

    let Err(err) = result else {
        panic!("expected credential verification failure");
    };

    assert!(
        err.message.contains(
            "failed to verify current switch credentials before skipping password rotation"
        )
    );

    assert_eq!(err.code, ErrorCode::Unauthenticated);
}

#[tokio::test]
async fn password_rotation_create_revision_401_preserves_unauthenticated_code() {
    let server = MockServer::start().await;
    mount_create_revision_response(&server, None, ResponseTemplate::new(401)).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());
    let result = switch
        .update_system_user_password_persisted("testusername", "new-password")
        .await;

    let Err(err) = result else {
        panic!("expected create revision auth failure");
    };

    assert_eq!(err.code, ErrorCode::Unauthenticated);
    assert!(err.message.contains("failed to create revision"));
}

#[tokio::test]
async fn password_rotation_stage_401_preserves_unauthenticated_code() {
    let server = MockServer::start().await;
    let encoded_password = UserPasswordPatch::new("new-password").password;

    mount_create_revision(&server, "15", None).await;

    mount_stage_password(
        &server,
        "15",
        "testusername",
        "new-password",
        None,
        ResponseTemplate::new(401)
            .set_body_string("password new-password or bmV3LXBhc3N3b3Jk rejected"),
    )
    .await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());
    let result = switch
        .update_system_user_password_persisted("testusername", "new-password")
        .await;

    let Err(err) = result else {
        panic!("expected stage password auth failure");
    };

    assert_eq!(err.code, ErrorCode::Unauthenticated);
    assert!(err.message.contains("failed to stage password update"));
    assert!(!err.message.contains("new-password"));
    assert!(!err.message.contains(&encoded_password));
}

#[tokio::test]
async fn password_rotation_stage_failure_reports_redacted_nvue_body() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "15", None).await;

    mount_stage_password(
        &server,
        "15",
        "testusername",
        "new-password",
        None,
        ResponseTemplate::new(400).set_body_json(PasswordPolicyError {
            detail: "password new-password or bmV3LXBhc3N3b3Jk does not meet requirements",
            status: 400,
            title: "Bad Request",
        }),
    )
    .await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());
    let result = switch
        .update_system_user_password_persisted("testusername", "new-password")
        .await;

    let Err(err) = result else {
        panic!("expected password policy failure");
    };

    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("does not meet requirements"));
    assert!(err.message.contains("Bad Request"));
    assert!(!err.message.contains("new-password"));
    assert!(!err.message.contains("bmV3LXBhc3N3b3Jk"));
    assert!(err.message.contains("XXXX"));
}

#[tokio::test]
async fn password_rotation_apply_failure_reports_redacted_nvue_body() {
    let server = MockServer::start().await;
    let encoded_password = UserPasswordPatch::new("new-password").password;

    mount_create_revision(&server, "16", None).await;
    mount_stage_password_success(&server, "16", "testusername", "new-password", None).await;

    mount_apply_revision_response(
        &server,
        "16",
        None,
        ResponseTemplate::new(400).set_body_json(PasswordPolicyError {
            detail: "password new-password or bmV3LXBhc3N3b3Jk does not meet requirements",
            status: 400,
            title: "Bad Request",
        }),
    )
    .await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());
    let result = switch
        .update_system_user_password_persisted("testusername", "new-password")
        .await;

    let Err(err) = result else {
        panic!("expected password policy failure");
    };

    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("failed to apply revision 16"));
    assert!(err.message.contains("does not meet requirements"));
    assert!(err.message.contains("Bad Request"));
    assert!(!err.message.contains("new-password"));
    assert!(!err.message.contains(&encoded_password));
    assert!(err.message.contains("XXXX"));
}

#[tokio::test]
async fn password_rotation_saves_applied_revision_with_nvue_only() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "21", None).await;
    mount_stage_password_success(&server, "21", "targetuser", "rotated-password", None).await;
    mount_apply_revision(&server, "21", None).await;

    mount_revision_get(
        &server,
        "21",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    mount_save_applied_revision(&server, None).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .update_system_user_password_persisted("targetuser", "rotated-password")
        .await
        .unwrap();
}

#[tokio::test]
async fn password_rotation_save_failure_reports_redacted_nvue_body() {
    let server = MockServer::start().await;
    let encoded_password = UserPasswordPatch::new("rotated-password").password;

    mount_create_revision(&server, "24", None).await;
    mount_stage_password_success(&server, "24", "targetuser", "rotated-password", None).await;
    mount_apply_revision(&server, "24", None).await;

    mount_revision_get(
        &server,
        "24",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    mount_save_applied_revision_response(
        &server,
        None,
        ResponseTemplate::new(400).set_body_json(PasswordPolicyError {
            detail: "password rotated-password or cm90YXRlZC1wYXNzd29yZA== cannot be saved",
            status: 400,
            title: "Bad Request",
        }),
    )
    .await;

    mount_save_revision_response(
        &server,
        "24",
        None,
        ResponseTemplate::new(400).set_body_json(PasswordPolicyError {
            detail: "password rotated-password or cm90YXRlZC1wYXNzd29yZA== cannot be saved",
            status: 400,
            title: "Bad Request",
        }),
    )
    .await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());
    let result = switch
        .update_system_user_password_persisted("targetuser", "rotated-password")
        .await;

    let Err(err) = result else {
        panic!("expected save failure");
    };

    assert_eq!(err.code, ErrorCode::Internal);

    assert!(
        err.message
            .contains("failed to save configuration after apply")
    );

    assert!(err.message.contains("cannot be saved"));
    assert!(err.message.contains("Bad Request"));
    assert!(!err.message.contains("rotated-password"));
    assert!(!err.message.contains(&encoded_password));
    assert!(err.message.contains("XXXX"));
}

#[tokio::test]
async fn password_rotation_save_401_preserves_unauthenticated_code() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "25", Some(TEST_AUTH)).await;

    mount_stage_password_success(
        &server,
        "25",
        "testusername",
        "rotated-password",
        Some(TEST_AUTH),
    )
    .await;

    mount_apply_revision(&server, "25", Some(TEST_AUTH)).await;

    mount_revision_get(
        &server,
        "25",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    mount_save_applied_revision_response(&server, Some(ROTATED_AUTH), ResponseTemplate::new(401))
        .await;

    mount_save_revision_response(
        &server,
        "25",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(401),
    )
    .await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let result = switch
        .update_system_user_password_persisted("testusername", "rotated-password")
        .await;

    let Err(err) = result else {
        panic!("expected save auth failure");
    };

    assert_eq!(err.code, ErrorCode::Unauthenticated);

    assert!(
        err.message
            .contains("failed to save configuration after apply")
    );
}

#[tokio::test]
async fn password_rotation_refreshes_active_user_credentials_before_save() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "31", Some(TEST_AUTH)).await;

    mount_stage_password_success(
        &server,
        "31",
        "testusername",
        "rotated-password",
        Some(TEST_AUTH),
    )
    .await;

    mount_apply_revision(&server, "31", Some(TEST_AUTH)).await;

    mount_revision_get(
        &server,
        "31",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    mount_save_applied_revision(&server, Some(ROTATED_AUTH)).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    switch
        .update_system_user_password_persisted("testusername", "rotated-password")
        .await
        .unwrap();
}

#[tokio::test]
async fn password_rotation_no_config_diff_still_saves_applied_config() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "35", None).await;

    mount_stage_password_success(&server, "35", "targetuser", "rotated-password", None).await;

    mount_apply_revision(&server, "35", None).await;

    mount_revision_get(
        &server,
        "35",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionStateWithIssue {
            state: "invalid",
            transition: RevisionTransitionWithIssue {
                issue: RevisionIssueMap {
                    issue_00000: RevisionIssueEntry {
                        message: "config apply executed with no config diff",
                        severity: "info",
                    },
                },
            },
        }),
    )
    .await;

    mount_save_applied_revision(&server, None).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .update_system_user_password_persisted("targetuser", "rotated-password")
        .await
        .unwrap();
}

#[tokio::test]
async fn password_rotation_active_user_no_config_diff_refreshes_before_save() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "36", Some(TEST_AUTH)).await;

    mount_stage_password_success(
        &server,
        "36",
        "testusername",
        "rotated-password",
        Some(TEST_AUTH),
    )
    .await;

    mount_apply_revision(&server, "36", Some(TEST_AUTH)).await;

    mount_revision_get(
        &server,
        "36",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(200).set_body_json(RevisionStateWithIssue {
            state: "invalid",
            transition: RevisionTransitionWithIssue {
                issue: RevisionIssueMap {
                    issue_00000: RevisionIssueEntry {
                        message: "config apply executed with no config diff",
                        severity: "info",
                    },
                },
            },
        }),
    )
    .await;

    mount_save_applied_revision(&server, Some(ROTATED_AUTH)).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    switch
        .update_system_user_password_persisted("testusername", "rotated-password")
        .await
        .unwrap();

    assert!(
        switch
            .nvue_client()
            .unwrap()
            .credentials_match("testusername", "rotated-password")
    );
}

#[tokio::test]
async fn password_rotation_retries_old_credential_401_during_active_user_handoff() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "33", Some(TEST_AUTH)).await;

    mount_stage_password_success(
        &server,
        "33",
        "testusername",
        "rotated-password",
        Some(TEST_AUTH),
    )
    .await;

    mount_apply_revision(&server, "33", Some(TEST_AUTH)).await;

    mount_revision_get_once(
        &server,
        "33",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(401),
    )
    .await;

    mount_revision_get_once(&server, "33", Some(TEST_AUTH), ResponseTemplate::new(401)).await;

    mount_revision_get(
        &server,
        "33",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    mount_save_applied_revision(&server, Some(ROTATED_AUTH)).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    switch
        .update_system_user_password_persisted("testusername", "rotated-password")
        .await
        .unwrap();
}

#[tokio::test]
async fn password_rotation_retries_candidate_after_current_no_config_diff_diagnostic() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "34", Some(TEST_AUTH)).await;

    mount_stage_password_success(
        &server,
        "34",
        "testusername",
        "rotated-password",
        Some(TEST_AUTH),
    )
    .await;

    mount_apply_revision(&server, "34", Some(TEST_AUTH)).await;

    mount_revision_get_once(
        &server,
        "34",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(401),
    )
    .await;

    mount_revision_get_once(
        &server,
        "34",
        Some(TEST_AUTH),
        ResponseTemplate::new(200).set_body_json(RevisionStateWithIssue {
            state: "invalid",
            transition: RevisionTransitionWithIssue {
                issue: RevisionIssueMap {
                    issue_00000: RevisionIssueEntry {
                        message: "config apply executed with no config diff",
                        severity: "info",
                    },
                },
            },
        }),
    )
    .await;

    mount_revision_get(
        &server,
        "34",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    mount_save_applied_revision(&server, Some(ROTATED_AUTH)).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    switch
        .update_system_user_password_persisted("testusername", "rotated-password")
        .await
        .unwrap();

    let revision_gets = server
        .received_requests()
        .await
        .expect("wiremock should record requests")
        .iter()
        .filter(|request| {
            request.method == wiremock::http::Method::Get
                && request.url.path() == "/nvue_v1/revision/34"
        })
        .count();

    assert_eq!(revision_gets, 3);
}

#[tokio::test]
async fn password_rotation_active_user_apply_failure_uses_current_credentials_for_revision_issue() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "32", Some(TEST_AUTH)).await;

    mount_stage_password_success(
        &server,
        "32",
        "testusername",
        "rotated-password",
        Some(TEST_AUTH),
    )
    .await;

    mount_apply_revision(&server, "32", Some(TEST_AUTH)).await;

    mount_revision_get(
        &server,
        "32",
        Some(ROTATED_AUTH),
        ResponseTemplate::new(401),
    )
    .await;

    mount_revision_get(
        &server,
        "32",
        Some(TEST_AUTH),
        ResponseTemplate::new(200).set_body_json(RevisionStateWithIssue {
            state: "invalid",
            transition: RevisionTransitionWithIssue {
                issue: RevisionIssueMap {
                    issue_00000: RevisionIssueEntry {
                        message: "password does not meet requirements",
                        severity: "error",
                    },
                },
            },
        }),
    )
    .await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let result = switch
        .update_system_user_password_persisted("testusername", "rotated-password")
        .await;

    let Err(err) = result else {
        panic!("expected revision failure");
    };

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert!(err.message.contains("revision 32 apply failed"));
    assert!(err.message.contains("state=invalid"));
    assert!(err.message.contains("password does not meet requirements"));
    assert!(!err.message.contains("timed out waiting for revision"));
}

#[tokio::test]
async fn password_rotation_retries_transient_revision_poll_error() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "22", None).await;
    mount_stage_password_success(&server, "22", "targetuser", "rotated-password", None).await;
    mount_apply_revision(&server, "22", None).await;
    mount_revision_get_once(&server, "22", None, ResponseTemplate::new(503)).await;

    mount_revision_get(
        &server,
        "22",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    mount_save_applied_revision(&server, None).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .update_system_user_password_persisted("targetuser", "rotated-password")
        .await
        .unwrap();
}

#[tokio::test]
async fn password_rotation_current_credential_revision_poll_401_fails_immediately() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "23", None).await;
    mount_stage_password_success(&server, "23", "targetuser", "rotated-password", None).await;
    mount_apply_revision(&server, "23", None).await;
    mount_revision_get(&server, "23", None, ResponseTemplate::new(401)).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());
    let result = switch
        .update_system_user_password_persisted("targetuser", "rotated-password")
        .await;

    let Err(err) = result else {
        panic!("expected current-credential 401 failure");
    };

    assert_eq!(err.code, ErrorCode::Unauthenticated);
    assert!(!err.message.contains("timed out waiting for revision"));
}

#[tokio::test]
async fn nvue_revision_can_finish_after_transport_handoff() {
    let server = MockServer::start().await;
    let revision = "rev-1";

    for (method_name, endpoint, body) in [
        ("POST", "/nvue_v1/revision", serde_json::json!({})),
        (
            "PATCH",
            "/nvue_v1/revision/rev-1",
            serde_json::json!({"state": "apply", "auto-prompt": {"ays": "ays_yes"}}),
        ),
        (
            "PATCH",
            "/nvue_v1/revision/applied",
            serde_json::json!({"state": "save", "auto-prompt": {"ays": "ays_yes"}}),
        ),
    ] {
        let response = if method_name == "POST" {
            serde_json::json!(revision)
        } else {
            serde_json::json!({})
        };

        Mock::given(method(method_name))
            .and(path(endpoint))
            .and(body_json(body))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .expect(1)
            .mount(&server)
            .await;
    }

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/api"))
        .and(query_param("rev", revision))
        .and(body_json(serde_json::json!({"certificate": "server-cert"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/revision/rev-1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"state": "applied"})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let revision_id = switch
        .nvue_stage_config_patches(&[(
            "/nvue_v1/system/api",
            serde_json::json!({"certificate": "server-cert"}),
        )])
        .await
        .unwrap();

    assert_eq!(revision_id, revision);

    switch
        .nvue_start_config_revision(&revision_id)
        .await
        .unwrap();
    switch
        .nvue_finish_config_revision(&revision_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn nvue_revision_apply_preserves_retryable_error_code() {
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/rev-1"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let error = SwitchGb200Nvidia::for_test(&server.uri())
        .nvue_start_config_revision("rev-1")
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Unavailable);
    assert!(SwitchGb200Nvidia::is_nvue_retryable_error(&error));
}

// ── Firmware type classification ────────────────────────────────

#[test]
fn classify_switch_firmware_types() {
    assert_eq!(classify_firmware_type_switch("BMC_FW"), FirmwareType::BMC);
    assert_eq!(
        classify_firmware_type_switch("BIOS_Update"),
        FirmwareType::BIOS
    );
    assert_eq!(
        classify_firmware_type_switch("FPGA_image"),
        FirmwareType::FPGA
    );
    assert_eq!(classify_firmware_type_switch("CPLD_v1"), FirmwareType::CPLD);
    assert_eq!(
        classify_firmware_type_switch("unknown_thing"),
        FirmwareType::Unknown
    );
}

// ── Shell quoting ───────────────────────────────────────────────

#[test]
fn shell_quote_simple() {
    assert_eq!(shell_quote("hello"), "'hello'");
}

#[test]
fn shell_quote_with_single_quotes() {
    assert_eq!(shell_quote("it's"), "'it'\\''s'");
}

#[test]
fn shell_quote_empty() {
    assert_eq!(shell_quote(""), "''");
}

#[test]
fn shell_quote_spaces_and_special() {
    assert_eq!(shell_quote("/path/to file"), "'/path/to file'");
}

// ── Config constants ────────────────────────────────────────────

#[test]
fn config_constant_values() {
    assert_eq!(config::MIN_FIRMWARE_FILE_SIZE, 1024);
    assert_eq!(config::SFTP_BUFFER_SIZE, 512 * 1024);
    assert_eq!(config::REVISION_POLL_INTERVAL_SECONDS, 1);
    assert_eq!(config::REVISION_APPLY_TIMEOUT_SECONDS, 60);
    assert_eq!(config::MAX_RETRY_ATTEMPTS, 3);
    assert_eq!(config::RETRY_WAIT_SECONDS, 30);
    assert_eq!(config::GNMI_CONFIG_WAIT_SECONDS, 15);
    assert_eq!(config::GNMI_RETRY_DELAY_1_SECONDS, 20);
    assert_eq!(config::GNMI_RETRY_DELAY_2_SECONDS, 30);
    assert_eq!(config::GRPC_PORT_NMX_CONTROLLER, 9370);
    assert_eq!(config::GRPC_PORT_NMX_TELEMETRY, 9352);
    assert_eq!(config::VALID_COMPONENTS.len(), 6);
    assert!(config::VALID_COMPONENTS.contains(&"cpld"));
}
// ── gRPC port mapping ───────────────────────────────────────────

#[test]
fn grpc_port_mapping() {
    assert_eq!(grpc_port_for_app("nmx-telemetry"), 9352);
    assert_eq!(grpc_port_for_app("nmx-controller"), 9370);
    assert_eq!(grpc_port_for_app("unknown-app"), 9370);
}

// ── CPLD version normalization ──────────────────────────────────

#[test]
fn strip_leading_zeros_cases() {
    assert_eq!(strip_leading_zeros("0042"), "42");
    assert_eq!(strip_leading_zeros("000"), "0");
    assert_eq!(strip_leading_zeros("1"), "1");
    assert_eq!(strip_leading_zeros("100"), "100");
}

#[test]
fn normalize_cpld_version_empty() {
    assert_eq!(normalize_cpld_version_string(""), "");
}

#[test]
fn normalize_cpld_version_standard() {
    assert_eq!(normalize_cpld_version_string("CPLD1_REV2"), "CPLD1_REV2");
}

#[test]
fn normalize_cpld_version_strips_leading_zeros() {
    assert_eq!(
        normalize_cpld_version_string("cpld_001_rev_002"),
        "CPLD1_REV2"
    );
}

#[test]
fn normalize_cpld_version_with_separators() {
    assert_eq!(
        normalize_cpld_version_string("CPLD 3 - REV 5"),
        "CPLD3_REV5"
    );
}

#[test]
fn normalize_cpld_version_no_match_returns_collapsed() {
    let result = normalize_cpld_version_string("UNKNOWN_FW");
    assert_eq!(result, "UNKNOWN_FW");
}

#[test]
fn build_cpld_package_version_formats() {
    let sub = FwpkgCpldSubcomponent {
        cpld_name: "cpld1".into(),
        component_id: "42".into(),
        revision: "REV3".into(),
        parent_device_name: "CPLD".into(),
        component_index: 0,
    };
    assert_eq!(build_cpld_package_version(&sub), "CPLD42_REV3");
}

// ── Switch version extraction ───────────────────────────────────

#[test]
fn extract_version_string_priority() {
    let fw = serde_json::json!({"actual-firmware": "1.0", "version": "2.0"});
    assert_eq!(extract_switch_version_string(&fw), "1.0");
}

#[test]
fn extract_version_string_fallback() {
    let fw = serde_json::json!({"sys_version": "3.0"});
    assert_eq!(extract_switch_version_string(&fw), "3.0");
}

#[test]
fn extract_version_string_unknown() {
    let fw = serde_json::json!({});
    assert_eq!(extract_switch_version_string(&fw), "unknown");
}

// ── Package device name mapping ─────────────────────────────────

#[test]
fn map_package_device_names() {
    assert_eq!(map_package_device_name("bmc"), Some("BMC"));
    assert_eq!(map_package_device_name("BMC"), Some("BMC"));
    assert_eq!(map_package_device_name("fpga"), Some("SMR"));
    assert_eq!(map_package_device_name("erot"), Some("EROT"));
    assert_eq!(map_package_device_name("bios"), Some("SBIOS"));
    assert_eq!(map_package_device_name("transceiver"), Some("TRANSCEIVER"));
    assert_eq!(map_package_device_name("gpu"), None);
}

// ── Reboot hint detection ───────────────────────────────────────

#[test]
fn reboot_hint_detection() {
    assert!(contains_reboot_hint("Please power cycle the device"));
    assert!(contains_reboot_hint("System requires reboot"));
    assert!(contains_reboot_hint("system is offline now"));
    assert!(!contains_reboot_hint("firmware installed successfully"));
}

#[test]
fn indicates_noop_detection() {
    assert!(indicates_noop("Nothing to uninstall"));
    assert!(!indicates_noop(""));
    assert!(!indicates_noop("Error: failed to uninstall firmware"));
}

// ── getInfo map ─────────────────────────────────────────────────

#[test]
fn get_info_includes_all_fields() {
    let sw = SwitchGb200Nvidia::new(
        "sw-01".into(),
        "rack-01".into(),
        "10.0.0.1".into(),
        443,
        "admin",
        "pass",
        "aa:bb:cc:dd:ee:ff".into(),
        "11:22:33:44:55:66".into(),
        "192.168.1.1".into(),
        "switch.test.local".into(),
        false,
    )
    .unwrap();

    let info = sw.get_info();
    assert_eq!(info["type"], "switch_gb200_nvidia");
    assert_eq!(info["host"], "10.0.0.1");
    assert_eq!(info["port"], "443");
    assert_eq!(info["macAddress"], "aa:bb:cc:dd:ee:ff");
    assert_eq!(info["hostMac_0"], "11:22:33:44:55:66");
    assert_eq!(info["hostIp_0"], "192.168.1.1");
}

#[test]
fn get_info_omits_empty_host_mac() {
    let sw = SwitchGb200Nvidia::new(
        "sw-01".into(),
        "rack-01".into(),
        "10.0.0.1".into(),
        443,
        "admin",
        "pass",
        "aa:bb:cc:dd:ee:ff".into(),
        String::new(),
        "192.168.1.1".into(),
        "switch.test.local".into(),
        false,
    )
    .unwrap();

    let info = sw.get_info();

    assert!(!info.contains_key("hostMac_0"));
    assert_eq!(info["hostIp_0"], "192.168.1.1");
}

// ── Power state behavior ────────────────────────────────────────

#[tokio::test]
async fn get_power_state_nvos_failure_without_bmc_returns_unknown() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;

    let sw = SwitchGb200Nvidia::for_test(&server.uri());

    let state = sw.get_power_state().await.unwrap();
    assert_eq!(state, PowerState::Unknown);
}

#[tokio::test]
async fn get_power_state_uses_bmc_redfish_when_available() {
    let nvos = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&nvos)
        .await;

    let bmc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Systems/System_0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "PowerState": "On"
        })))
        .mount(&bmc)
        .await;

    let sw = SwitchGb200Nvidia::for_test(&nvos.uri()).with_power_http_for_test(&bmc.uri());

    let state = sw.get_power_state().await.unwrap();
    assert_eq!(state, PowerState::On);
}

#[tokio::test]
async fn get_power_state_bmc_failure_returns_unknown() {
    let nvos = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&nvos)
        .await;

    let bmc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Systems/System_0"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&bmc)
        .await;

    let sw = SwitchGb200Nvidia::for_test(&nvos.uri()).with_power_http_for_test(&bmc.uri());

    let state = sw.get_power_state().await.unwrap();
    assert_eq!(state, PowerState::Unknown);
}

#[tokio::test]
async fn set_power_state_rejects_non_power_cycle() {
    let sw = SwitchGb200Nvidia::new(
        "sw-01".into(),
        "rack-01".into(),
        "127.0.0.1".into(),
        1,
        "",
        "",
        String::new(),
        "11:22:33:44:55:66".into(),
        "127.0.0.1".into(),
        "switch.test.local".into(),
        false,
    )
    .unwrap();

    let err = sw
        .set_power_state(PowerOp::On, PowerTargetType::System)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("PowerCycle"));
}

#[tokio::test]
async fn switch_from_config_with_bmc_and_host_endpoints_powers_off_via_bmc_redfish()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let mockup = redfish_test_support::RedfishSimulator::builder()
        .add_gb200_compute(0)
        .start()
        .await;
    let Some(&bmc_port) = mockup.ports().first() else {
        panic!("expected mockup port");
    };

    let config = NodeConfig {
        id: "sw-01".into(),
        node_type: NodeType::SwitchGb200Nvidia,
        bmc_endpoint: Some(endpoint_config(
            "127.0.0.1",
            "aa:bb:cc:dd:ee:ff",
            bmc_port,
            "admin",
            "password",
        )),
        host_endpoint: Some(endpoint_config_with_name(
            "127.0.0.1",
            "11:22:33:44:55:66",
            443,
            "host-admin",
            "host-password",
            Some("switch.test.local"),
        )),
    };
    let switch = SwitchGb200Nvidia::from_config(&config, "rack-01")?;

    let result = switch
        .set_power_state(PowerOp::Off, PowerTargetType::System)
        .await;

    mockup.stop();
    result?;
    Ok(())
}

#[tokio::test]
async fn switch_from_config_with_bmc_endpoint_reads_power_state_via_bmc_redfish()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let mockup = redfish_test_support::RedfishSimulator::builder()
        .add_gb200_compute(0)
        .start()
        .await;
    let Some(&bmc_port) = mockup.ports().first() else {
        panic!("expected mockup port");
    };

    let config = NodeConfig {
        id: "sw-01".into(),
        node_type: NodeType::SwitchGb200Nvidia,
        bmc_endpoint: Some(endpoint_config(
            "127.0.0.1",
            "aa:bb:cc:dd:ee:ff",
            bmc_port,
            "admin",
            "password",
        )),
        host_endpoint: None,
    };
    let switch = SwitchGb200Nvidia::from_config(&config, "rack-01")?;

    let state = switch.get_power_state().await;

    mockup.stop();
    assert_eq!(state?, PowerState::On);
    Ok(())
}

#[tokio::test]
async fn switch_from_config_host_only_rejects_power_off() {
    let config = NodeConfig {
        id: "sw-01".into(),
        node_type: NodeType::SwitchGb200Nvidia,
        bmc_endpoint: None,
        host_endpoint: Some(endpoint_config_with_name(
            "127.0.0.1",
            "11:22:33:44:55:66",
            443,
            "admin",
            "password",
            Some("switch.test.local"),
        )),
    };
    let Ok(switch) = SwitchGb200Nvidia::from_config(&config, "rack-01") else {
        panic!("expected host-only switch config to be accepted");
    };

    let result = switch
        .set_power_state(PowerOp::Off, PowerTargetType::System)
        .await;
    let Err(err) = result else {
        panic!("expected host-only switch power off to fail");
    };

    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("PowerCycle"));
}

#[tokio::test]
async fn bmc_only_switch_rejects_nvue_system_image_read()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let config = NodeConfig {
        id: "sw-01".into(),
        node_type: NodeType::SwitchGb200Nvidia,
        bmc_endpoint: Some(endpoint_config(
            "127.0.0.1",
            "aa:bb:cc:dd:ee:ff",
            443,
            "admin",
            "password",
        )),
        host_endpoint: None,
    };
    let switch = SwitchGb200Nvidia::from_config(&config, "rack-01")?;

    let result = switch.get_system_image_state().await;
    let Err(err) = result else {
        panic!("expected BMC-only switch NVUE read to fail");
    };

    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert_eq!(err.message, "host_endpoint is required for NVUE operations");
    Ok(())
}

#[tokio::test]
async fn switch_power_cycle_uses_nvue_fallback_without_bmc_power_client()
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
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .set_power_state(PowerOp::PowerCycle, PowerTargetType::System)
        .await?;
    Ok(())
}

// ── Switch host endpoint selection ───────────────────────────────────────

#[test]
fn switch_host_endpoint_uses_host_ip() {
    let sw = SwitchGb200Nvidia::new(
        "sw-01".into(),
        "rack-01".into(),
        "original-host".into(),
        443,
        "",
        "",
        String::new(),
        "11:22:33:44:55:66".into(),
        "10.0.0.99".into(),
        "switch.test.local".into(),
        false,
    )
    .unwrap();

    let Some(host_endpoint) = sw.host_endpoint.as_ref() else {
        panic!("expected host endpoint");
    };

    assert_eq!(host_endpoint.endpoint.ip_address, "10.0.0.99");
}

#[test]
fn switch_new_rejects_missing_host_ip() {
    let result = SwitchGb200Nvidia::new(
        "sw-01".into(),
        "rack-01".into(),
        "original-host".into(),
        443,
        "",
        "",
        String::new(),
        "11:22:33:44:55:66".into(),
        String::new(),
        "switch.test.local".into(),
        false,
    );
    let Err(err) = result else {
        panic!("expected missing host_ip_address error");
    };

    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("host_ip_address"));
}

#[test]
fn switch_new_accepts_missing_host_mac() {
    let result = SwitchGb200Nvidia::new(
        "sw-01".into(),
        "rack-01".into(),
        "original-host".into(),
        443,
        "admin",
        "password",
        String::new(),
        String::new(),
        "10.0.0.99".into(),
        "switch.test.local".into(),
        false,
    );
    let Ok(sw) = result else {
        panic!("expected missing host_mac_address to be accepted");
    };

    let Some(host_endpoint) = sw.host_endpoint.as_ref() else {
        panic!("expected host endpoint");
    };

    assert_eq!(host_endpoint.endpoint.mac_address, "");
}

#[test]
fn nvfwupd_target_config_uses_host_endpoint_credentials_and_ip() {
    let sw = SwitchGb200Nvidia::new(
        "sw-01".into(),
        "rack-01".into(),
        "bmc-host".into(),
        8443,
        "admin",
        "secret",
        String::new(),
        "11:22:33:44:55:66".into(),
        "10.0.0.5".into(),
        "switch.test.local".into(),
        true,
    )
    .unwrap();

    let cfg = sw.nvfwupd_target_config().unwrap();

    assert_eq!(cfg.ip, "10.0.0.5");
    assert_eq!(cfg.port, Some(8443));
    assert_eq!(cfg.username, "admin");
    assert_eq!(cfg.password, "secret");
    assert_eq!(cfg.server_type, nvfwupd::workflow::ServerType::GB200Switch);
    assert!(!cfg.verify_tls);
    assert!(!format!("{cfg:?}").contains("secret"));
}

#[test]
fn gb300_switch_from_config_uses_gb300_nvfwupd_server_type() {
    let mut config = NodeConfig::new("sw-01", NodeType::SwitchGb300Nvidia, "bmc-host");
    config.host_endpoint = Some(endpoint_config_with_name(
        "10.0.0.5",
        "11:22:33:44:55:66",
        8443,
        "admin",
        "secret",
        Some("switch.test.local"),
    ));

    let sw = SwitchGb200Nvidia::from_config(&config, "rack-01").unwrap();
    let cfg = sw.nvfwupd_target_config().unwrap();

    assert_eq!(sw.node_type(), NodeType::SwitchGb300Nvidia);
    assert_eq!(sw.get_info()["type"], "switch_gb300_nvidia");
    assert_eq!(cfg.server_type, nvfwupd::workflow::ServerType::GB300Switch);
}

#[test]
fn switch_from_config_rejects_missing_host_ip() {
    let mut config = NodeConfig::new("sw-01", NodeType::SwitchGb200Nvidia, "bmc-host");
    config.host_endpoint = Some(endpoint_config(
        "",
        "11:22:33:44:55:66",
        8443,
        "admin",
        "password",
    ));

    let result = SwitchGb200Nvidia::from_config(&config, "rack-01");
    let Err(err) = result else {
        panic!("expected missing host_ip_address error");
    };

    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("host_ip_address"));
}

#[test]
fn switch_from_config_accepts_missing_host_mac() {
    let mut config = NodeConfig::new("sw-01", NodeType::SwitchGb200Nvidia, "bmc-host");
    config.host_endpoint = Some(endpoint_config_with_name(
        "10.0.0.99",
        "",
        8443,
        "admin",
        "password",
        Some("switch.test.local"),
    ));

    let result = SwitchGb200Nvidia::from_config(&config, "rack-01");
    let Ok(sw) = result else {
        panic!("expected missing host_mac_address to be accepted");
    };

    let Some(host_endpoint) = sw.host_endpoint.as_ref() else {
        panic!("expected host endpoint");
    };

    assert_eq!(host_endpoint.endpoint.ip_address, "10.0.0.99");
    assert_eq!(host_endpoint.endpoint.mac_address, "");
}

#[test]
fn switch_from_config_accepts_host_only_endpoint() {
    let mut config = NodeConfig::new("sw-01", NodeType::SwitchGb200Nvidia, "bmc-host");
    config.bmc_endpoint = None;
    config.host_endpoint = Some(endpoint_config_with_name(
        "10.0.0.99",
        "11:22:33:44:55:66",
        8443,
        "admin",
        "password",
        Some("switch.test.local"),
    ));

    let result = SwitchGb200Nvidia::from_config(&config, "rack-01");
    let Ok(sw) = result else {
        panic!("expected switch config to be accepted");
    };

    let Some(host_endpoint) = sw.host_endpoint.as_ref() else {
        panic!("expected host endpoint");
    };

    assert_eq!(host_endpoint.endpoint.ip_address, "10.0.0.99");
    assert_eq!(host_endpoint.endpoint.port, 8443);
    assert!(sw.bmc_endpoint.is_none());
}

#[test]
fn switch_from_config_accepts_bmc_only_endpoint() {
    let mut config = NodeConfig::new("sw-01", NodeType::SwitchGb200Nvidia, "bmc-host");
    if let Some(endpoint) = config.bmc_endpoint.as_mut() {
        endpoint.endpoint.mac_address = "aa:bb:cc:dd:ee:ff".into();
        endpoint.endpoint.port = 9443;
        endpoint.credentials = Some(EndpointCredentials::new("admin", "password"));
        endpoint.dangerously_accept_invalid_certs = true;
    }
    config.host_endpoint = None;

    let result = SwitchGb200Nvidia::from_config(&config, "rack-01");
    let Ok(sw) = result else {
        panic!("expected BMC-only switch config to be accepted");
    };

    let Some(bmc_endpoint) = sw.bmc_endpoint.as_ref() else {
        panic!("expected BMC endpoint");
    };

    assert_eq!(bmc_endpoint.endpoint.ip_address, "bmc-host");
    assert_eq!(bmc_endpoint.endpoint.port, 9443);
    assert!(sw.host_endpoint.is_none());
}

#[tokio::test]
async fn bmc_only_switch_rejects_nvue_mtls_configuration_without_panicking() {
    let mut config = NodeConfig::new("sw-01", NodeType::SwitchGb200Nvidia, "bmc-host");

    if let Some(endpoint) = config.bmc_endpoint.as_mut() {
        endpoint.endpoint.mac_address = "aa:bb:cc:dd:ee:ff".into();
        endpoint.endpoint.port = 9443;
        endpoint.credentials = Some(EndpointCredentials::new("admin", "password"));
    }

    config.host_endpoint = None;

    let switch = SwitchGb200Nvidia::from_config(&config, "rack-01").unwrap();
    let directory = tempfile::tempdir().unwrap();

    let material = nvue_client::ClientTls::new(
        directory.path().join("ca.pem"),
        directory.path().join("client.pem"),
        directory.path().join("client.key"),
    );

    let error = switch
        .configure_nvue_client_tls(Some(material))
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert!(error.message.contains("host_endpoint"));
}

#[test]
fn switch_from_config_uses_host_tls_override_when_host_endpoint_exists() {
    let mut config = NodeConfig::new("sw-01", NodeType::SwitchGb200Nvidia, "bmc-host");
    if let Some(endpoint) = config.bmc_endpoint.as_mut() {
        endpoint.endpoint.mac_address = "aa:bb:cc:dd:ee:ff".into();
        endpoint.credentials = Some(EndpointCredentials::new("bmc-admin", "bmc-password"));
        endpoint.dangerously_accept_invalid_certs = true;
    }
    config.host_endpoint = Some(EndpointConfig::with_credentials(
        Endpoint {
            ip_address: "10.0.0.99".into(),
            mac_address: "11:22:33:44:55:66".into(),
            port: 8443,
            host_name: Some("switch.test.local".into()),
        },
        Some(EndpointCredentials::new("host-admin", "host-password")),
        false,
    ));

    let result = SwitchGb200Nvidia::from_config(&config, "rack-01");
    let Ok(sw) = result else {
        panic!("expected switch config to be accepted");
    };

    let Some(host_endpoint) = sw.host_endpoint.as_ref() else {
        panic!("expected host endpoint");
    };

    assert!(!host_endpoint.dangerously_accept_invalid_certs);
}

// ── Node type ───────────────────────────────────────────────────

#[test]
fn node_type_reports_switch_gb200() {
    let sw = SwitchGb200Nvidia::new(
        "sw-01".into(),
        "rack-01".into(),
        "10.0.0.1".into(),
        443,
        "",
        "",
        String::new(),
        "11:22:33:44:55:66".into(),
        "127.0.0.1".into(),
        "switch.test.local".into(),
        false,
    )
    .unwrap();

    assert_eq!(sw.node_type(), NodeType::SwitchGb200Nvidia);
}

async fn assert_set_cluster_state_via_nvue(enabled: bool, expected_state: &str) {
    let server = MockServer::start().await;
    let current_state = if enabled { "disabled" } else { "enabled" };

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": current_state
        })))
        .up_to_n_times(1)
        .expect(1)
        .named("initial GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": expected_state
        })))
        .expect(1)
        .named("final GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch.set_cluster_state(enabled).await.unwrap();

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec![
            format!("nv set cluster state {expected_state}"),
            "nv config apply --assume-yes".to_owned(),
            "nv config save".to_owned(),
        ]
    );
}

async fn assert_set_cluster_state_via_nvue_noops(enabled: bool, current_state: &str) {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": current_state
        })))
        .expect(1)
        .named("current GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch.set_cluster_state(enabled).await.unwrap();

    assert!(recorded_ssh_commands(&commands).is_empty());
}

async fn assert_set_cluster_state_via_nvue_no_config_diff() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled"
        })))
        .up_to_n_times(1)
        .expect(1)
        .named("initial GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "disabled"
        })))
        .expect(1)
        .named("final GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch.set_cluster_state(false).await.unwrap();

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec![
            "nv set cluster state disabled".to_owned(),
            "nv config apply --assume-yes".to_owned(),
            "nv config save".to_owned(),
        ]
    );
}

#[tokio::test]
async fn set_cluster_state_via_nvue_stages_enabled_state() {
    assert_set_cluster_state_via_nvue(true, "enabled").await;
}

#[tokio::test]
async fn set_cluster_state_via_nvue_stages_disabled_state() {
    assert_set_cluster_state_via_nvue(false, "disabled").await;
}

#[tokio::test]
async fn set_cluster_state_via_nvue_skips_disabled_state_when_already_disabled() {
    assert_set_cluster_state_via_nvue_noops(false, "disabled").await;
}

#[tokio::test]
async fn set_cluster_state_via_nvue_skips_enabled_state_when_already_enabled() {
    assert_set_cluster_state_via_nvue_noops(true, "enabled").await;
}

#[tokio::test]
async fn set_cluster_state_via_nvue_skips_start_state_when_enabling() {
    assert_set_cluster_state_via_nvue_noops(true, "start").await;
}

#[tokio::test]
async fn set_cluster_state_via_nvue_treats_no_config_diff_as_success_when_revision_matches() {
    assert_set_cluster_state_via_nvue_no_config_diff().await;
}

#[tokio::test]
async fn bmc_aux_powercycle_posts_to_correct_endpoint_with_correct_payload() {
    let bmc = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
        ))
        .and(body_json(serde_json::json!({"ResetType": "PowerCycle"})))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .named("ComputerSystem.Reset POST")
        .mount(&bmc)
        .await;

    let sw = SwitchGb200Nvidia::for_test(&bmc.uri()).with_power_http_for_test(&bmc.uri());
    sw.bmc_aux_powercycle().await.unwrap();
}

#[tokio::test]
async fn bmc_aux_powercycle_returns_failed_precondition_when_no_bmc_endpoint() {
    let nvos = MockServer::start().await;
    let sw = SwitchGb200Nvidia::for_test(&nvos.uri()).without_bmc_endpoint_for_test();

    let err = sw.bmc_aux_powercycle().await.unwrap_err();
    assert_eq!(
        err.code,
        crate::utilities::error::ErrorCode::FailedPrecondition
    );
    assert!(err.message.contains("BMC endpoint"));
}
