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

//! Unit tests for NVIDIA GB200 switch node workflows.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nvue_client::cluster::{ClusterNodeServerAddresses, ClusterUpdate, InterfaceType};
use nvue_client::revision::{RevisionCreate, RevisionUpdate};
use nvue_client::system::{GnmiServerUpdate, UserPasswordPatch};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;

use super::*;
use crate::domain::node::NodeType;
use crate::domain::rack::{EndpointConfig, EndpointCredentials, NodeConfig};
use crate::nodes::nvos;
use crate::transport::ssh_client::SshUploadOptions;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockBuilder, MockServer, Respond, ResponseTemplate};

type SshCommandLog = Arc<Mutex<Vec<String>>>;
type SshCommandRecorder = Box<dyn Fn(&str) -> Result<String> + Send + Sync + 'static>;

#[derive(Clone, Default)]
struct TestLogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for TestLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        std::io::Write::write(&mut *self.0.lock().unwrap(), buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

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

async fn mount_redfish_system(server: &MockServer, power_state: &str) {
    Mock::given(method("GET"))
        .and(path("/redfish/v1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "@odata.id": "/redfish/v1",
            "Id": "RootService",
            "Name": "Root Service",
            "Links": {},
            "Systems": {"@odata.id": "/redfish/v1/Systems"}
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/redfish/v1/Systems"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "@odata.id": "/redfish/v1/Systems",
            "@odata.type": "#ComputerSystemCollection.ComputerSystemCollection",
            "Name": "Computer System Collection",
            "Members": [{"@odata.id": "/redfish/v1/Systems/System_0"}]
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/redfish/v1/Systems/System_0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "@odata.id": "/redfish/v1/Systems/System_0",
            "Id": "System_0",
            "Name": "System",
            "PowerState": power_state,
            "Actions": {
                "#ComputerSystem.Reset": {
                    "target": "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset"
                }
            }
        })))
        .mount(server)
        .await;
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

#[tokio::test]
async fn host_credentials_rejects_explicit_empty_user_pass() {
    // A host UserPass credential with both fields explicitly empty must be
    // treated as absent, not as a present-but-blank credential: nothing
    // upstream normalizes this case to `None` for host endpoints.
    let server = MockServer::start().await;
    let mut switch = SwitchGb200Nvidia::for_test(&server.uri());
    switch.host_endpoint = Some(EndpointConfig::with_credentials(
        Endpoint {
            ip_address: "test-host".to_owned(),
            mac_address: "00:00:00:00:00:00".to_owned(),
            port: 443,
            host_name: None,
        },
        Some(EndpointCredentials::new("", "")),
        true,
    ));

    let err = switch.host_credentials().unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert!(err.message.contains("switch host credentials are required"));
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
            "parameters": {"force": false}
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

#[tokio::test]
async fn spdm_attestation_discovers_and_collects_components_serially() {
    let server = MockServer::start().await;
    let nonce = vec![0xab; 32];

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/security/spdm"))
        .and(query_param("rev", "operational"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ERoT_CPU_0": {},
            "ERoT_BMC_0": {}
        })))
        .expect(1)
        .mount(&server)
        .await;

    for (component_id, action_id) in [("ERoT_BMC_0", "spdm-bmc"), ("ERoT_CPU_0", "spdm-cpu")] {
        Mock::given(method("POST"))
            .and(path(format!(
                "/nvue_v1/system/security/spdm/{component_id}"
            )))
            .and(body_json(serde_json::json!({
                "@generate": {
                    "state": "start",
                    "parameters": {"nonce": "ab".repeat(32)}
                }
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(action_id))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/nvue_v1/action/{action_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "action_success"
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!(
                "/nvue_v1/system/security/spdm/{component_id}/measurements"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "signed_measurements": component_id
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!(
                "/nvue_v1/system/security/spdm/{component_id}/certificates"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "certificate_chain": component_id
            })))
            .expect(1)
            .mount(&server)
            .await;
    }

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let challenge = SpdmAttestationChallenge::new(nonce).unwrap();

    let results = switch
        .request_spdm_attestation_evidence(
            &challenge,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].component_id, "ERoT_BMC_0");
    assert_eq!(results[1].component_id, "ERoT_CPU_0");
    assert!(results.iter().all(|result| result.outcome.is_ok()));

    let request_paths: Vec<_> = server
        .received_requests()
        .await
        .expect("wiremock requests")
        .into_iter()
        .map(|request| request.url.path().to_owned())
        .collect();

    assert_eq!(
        request_paths,
        [
            "/nvue_v1/system/security/spdm",
            "/nvue_v1/system/security/spdm/ERoT_BMC_0",
            "/nvue_v1/action/spdm-bmc",
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/measurements",
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/certificates",
            "/nvue_v1/system/security/spdm/ERoT_CPU_0",
            "/nvue_v1/action/spdm-cpu",
            "/nvue_v1/system/security/spdm/ERoT_CPU_0/measurements",
            "/nvue_v1/system/security/spdm/ERoT_CPU_0/certificates",
        ]
    );
}

#[tokio::test]
async fn spdm_attestation_reports_component_failure_without_fetching_stale_evidence() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/security/spdm"))
        .and(query_param("rev", "operational"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ERoT_BMC_0": {}
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/system/security/spdm/ERoT_BMC_0"))
        .respond_with(ResponseTemplate::new(201).set_body_json("spdm-bmc"))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/action/spdm-bmc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "action_error",
            "detail": "measurement generation failed"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/measurements",
        ))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/certificates",
        ))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let challenge = SpdmAttestationChallenge::new(vec![0xcd; 32]).unwrap();

    let results = switch
        .request_spdm_attestation_evidence(
            &challenge,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();

    assert_eq!(results.len(), 1);

    assert!(
        results[0]
            .outcome
            .as_ref()
            .unwrap_err()
            .message
            .contains("measurement generation failed")
    );
}

#[test]
fn spdm_attestation_nonce_must_be_32_bytes() {
    let error = SpdmAttestationChallenge::new(vec![0; 31]).unwrap_err();

    assert_eq!(error.code, ErrorCode::InvalidArgument);
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

async fn mount_current_cluster_state(server: &MockServer, state: &str, up_to_once: bool) {
    let mock = Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": state
        })));

    let mock = if up_to_once {
        mock.up_to_n_times(1)
    } else {
        mock
    };

    mock.expect(1)
        .named("GET /nvue_v1/cluster")
        .mount(server)
        .await;
}

async fn mount_stage_cluster_state(server: &MockServer, revision_id: &str, desired_state: &str) {
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/cluster"))
        .and(query_param("rev", revision_id))
        .and(body_json(ClusterUpdate::new(desired_state)))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .named("PATCH /nvue_v1/cluster?rev")
        .mount(server)
        .await;
}

/// Mounts the create -> stage -> apply -> wait(applied) -> save revision
/// sequence RMS drives to transition the cluster to `desired_state`.
async fn mount_cluster_state_revision(server: &MockServer, revision_id: &str, desired_state: &str) {
    mount_create_revision(server, revision_id, None).await;
    mount_stage_cluster_state(server, revision_id, desired_state).await;
    mount_apply_revision(server, revision_id, None).await;
    mount_revision_get(
        server,
        revision_id,
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;
    mount_save_applied_revision(server, None).await;
}

async fn assert_set_cluster_state_noops(enabled: bool, current_state: &str) {
    let server = MockServer::start().await;

    // Mount only the state read: no revision mocks are registered, so any
    // attempt to mutate would hit a 404 and fail the call. Reaching Ok proves
    // the transition was a no-op.
    mount_current_cluster_state(&server, current_state, false).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch.set_cluster_state(enabled).await.unwrap();
}

async fn assert_set_cluster_state_updates(
    enabled: bool,
    current_state: &str,
    expected_state: &str,
) {
    let server = MockServer::start().await;

    mount_current_cluster_state(&server, current_state, true).await;
    mount_cluster_state_revision(&server, "42", expected_state).await;
    mount_current_cluster_state(&server, expected_state, false).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    // Each mounted revision mock carries an `expect(1)`, verified on drop, so
    // the transition must flow through the NVUE revision workflow.
    switch.set_cluster_state(enabled).await.unwrap();
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
async fn set_cluster_state_enables_when_preflight_state_is_not_enabled() {
    // The preflight no-op fires only on an exact match with the desired state.
    // Earlier code additionally short-circuited enabling when the cluster read
    // back the literal `"start"`; NVOS never reports that as a cluster state, so
    // this guards that the removed special-case stays removed: a `"start"`
    // reading drives a genuine enable revision and must converge to `enabled`.
    assert_set_cluster_state_updates(true, "start", "enabled").await;
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
async fn set_cluster_state_waits_through_start_until_enabled() {
    let server = MockServer::start().await;

    // Preflight read: cluster is disabled, so a genuine enable revision runs.
    mount_current_cluster_state(&server, "disabled", true).await;
    mount_cluster_state_revision(&server, "42", "enabled").await;

    // Post-apply convergence: the switch first reports the transient `start`
    // state, then settles on `enabled`. The `start` mock is consumed once and
    // the loop must keep polling until `enabled` is observed. If `start` were
    // (incorrectly) treated as terminal, the final `enabled` GET would never
    // fire and its expect(1) would fail on drop.
    mount_current_cluster_state(&server, "start", true).await;
    mount_current_cluster_state(&server, "enabled", false).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch.set_cluster_state(true).await.unwrap();
}

#[tokio::test]
async fn set_cluster_state_treats_no_config_diff_as_success() {
    let server = MockServer::start().await;

    mount_current_cluster_state(&server, "enabled", true).await;
    mount_create_revision(&server, "42", None).await;
    mount_stage_cluster_state(&server, "42", "disabled").await;
    mount_apply_revision(&server, "42", None).await;

    // NVUE reports the apply as idempotent; the revision helper treats this as
    // success and issues no save.
    mount_revision_get(
        &server,
        "42",
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

    mount_current_cluster_state(&server, "disabled", false).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch.set_cluster_state(false).await.unwrap();
}

#[tokio::test]
async fn set_cluster_state_propagates_stage_patch_failure() {
    let server = MockServer::start().await;

    mount_current_cluster_state(&server, "enabled", false).await;
    mount_create_revision(&server, "42", None).await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/cluster"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "detail": "state transition rejected"
        })))
        .expect(1)
        .named("PATCH /nvue_v1/cluster?rev failure")
        .mount(&server)
        .await;

    // A failed stage must discard the candidate revision instead of leaking it.
    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/revision/42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .named("DELETE /nvue_v1/revision/42 (discard candidate)")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch.set_cluster_state(false).await.unwrap_err();

    assert!(err.message.contains("NVUE PATCH /nvue_v1/cluster?rev=42"));
}

#[tokio::test]
async fn set_cluster_state_propagates_apply_failure() {
    let server = MockServer::start().await;

    mount_current_cluster_state(&server, "enabled", false).await;
    mount_create_revision(&server, "42", None).await;
    mount_stage_cluster_state(&server, "42", "disabled").await;
    mount_apply_revision(&server, "42", None).await;

    mount_revision_get(
        &server,
        "42",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionStateWithIssue {
            state: "failed",
            transition: RevisionTransitionWithIssue {
                issue: RevisionIssueMap {
                    issue_00000: RevisionIssueEntry {
                        message: "cluster apply rejected",
                        severity: "error",
                    },
                },
            },
        }),
    )
    .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch.set_cluster_state(false).await.unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert!(err.message.contains("cluster apply rejected"));
}

#[tokio::test]
async fn set_cluster_state_errors_when_state_never_converges() {
    let server = MockServer::start().await;

    // Every read reports the pre-transition state, so operational convergence
    // never succeeds even though the revision applies cleanly.
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled"
        })))
        .named("GET /nvue_v1/cluster never converges")
        .mount(&server)
        .await;

    mount_create_revision(&server, "42", None).await;
    mount_stage_cluster_state(&server, "42", "disabled").await;
    mount_apply_revision(&server, "42", None).await;
    mount_revision_get(
        &server,
        "42",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;
    mount_save_applied_revision(&server, None).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch.set_cluster_state(false).await.unwrap_err();

    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("cluster state did not reach disabled"));
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

// ── gNMI server state (NVUE) ─────────────────────────────────────

async fn mount_current_gnmi_state(server: &MockServer, state: &str, up_to_once: bool) {
    let mock = Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": state
        })));

    let mock = if up_to_once {
        mock.up_to_n_times(1)
    } else {
        mock
    };

    mock.expect(1)
        .named("GET /nvue_v1/system/gnmi-server")
        .mount(server)
        .await;
}

async fn mount_stage_gnmi_state(server: &MockServer, revision_id: &str, desired_state: &str) {
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .and(query_param("rev", revision_id))
        .and(body_json(GnmiServerUpdate::new(desired_state)))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .named("PATCH /nvue_v1/system/gnmi-server?rev")
        .mount(server)
        .await;
}

/// Mounts the create -> stage -> apply -> wait(applied) -> save revision
/// sequence RMS drives to transition the gNMI server to `desired_state`.
async fn mount_gnmi_state_revision(server: &MockServer, revision_id: &str, desired_state: &str) {
    mount_create_revision(server, revision_id, None).await;
    mount_stage_gnmi_state(server, revision_id, desired_state).await;
    mount_apply_revision(server, revision_id, None).await;
    mount_revision_get(
        server,
        revision_id,
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;
    mount_save_applied_revision(server, None).await;
}

async fn assert_gnmi_service_noops(enabled: bool, current_state: &str) {
    let server = MockServer::start().await;

    // Mount only the state read: no revision mocks are registered, so any
    // attempt to mutate would hit a 404 and fail the call. Reaching Ok proves
    // the transition was a no-op.
    mount_current_gnmi_state(&server, current_state, false).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let result = switch.gnmi_service(enabled).await.unwrap();
    assert_eq!(result["status"], "success");
    assert_eq!(result["state"], current_state);
}

async fn assert_gnmi_service_updates(enabled: bool, current_state: &str, expected_state: &str) {
    let server = MockServer::start().await;

    mount_current_gnmi_state(&server, current_state, true).await;
    mount_gnmi_state_revision(&server, "42", expected_state).await;
    mount_current_gnmi_state(&server, expected_state, false).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    // Each mounted revision mock carries an `expect(1)`, verified on drop, so
    // the transition must flow through the NVUE revision workflow. Reaching this
    // point proves RMS issued no `nv set` / `nv config` / `nv show` SSH command.
    let result = switch.gnmi_service(enabled).await.unwrap();
    assert_eq!(result["status"], "success");
    assert_eq!(result["state"], expected_state);
}

#[tokio::test]
async fn gnmi_service_skips_disable_when_already_disabled() {
    assert_gnmi_service_noops(false, "disabled").await;
}

#[tokio::test]
async fn gnmi_service_skips_enable_when_already_enabled() {
    assert_gnmi_service_noops(true, "enabled").await;
}

#[tokio::test]
async fn gnmi_service_enables_disabled_server() {
    assert_gnmi_service_updates(true, "disabled", "enabled").await;
}

#[tokio::test]
async fn gnmi_service_disables_enabled_server() {
    assert_gnmi_service_updates(false, "enabled", "disabled").await;
}

#[tokio::test]
async fn gnmi_service_waits_through_transient_state_until_converged() {
    let server = MockServer::start().await;

    // Preflight read: gNMI is disabled, so a genuine enable revision runs.
    mount_current_gnmi_state(&server, "disabled", true).await;
    mount_gnmi_state_revision(&server, "42", "enabled").await;

    // Post-apply convergence: the switch first reports the stale `disabled`
    // state, then settles on `enabled`. The loop must keep polling until the
    // desired value is observed; if it resolved on the first reading, the final
    // `enabled` GET would never fire and its expect(1) would fail on drop.
    mount_current_gnmi_state(&server, "disabled", true).await;
    mount_current_gnmi_state(&server, "enabled", false).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let result = switch.gnmi_service(true).await.unwrap();
    assert_eq!(result["state"], "enabled");
}

#[tokio::test]
async fn gnmi_service_treats_no_config_diff_as_success() {
    let server = MockServer::start().await;

    mount_current_gnmi_state(&server, "enabled", true).await;
    mount_create_revision(&server, "42", None).await;
    mount_stage_gnmi_state(&server, "42", "disabled").await;
    mount_apply_revision(&server, "42", None).await;

    // NVUE reports the apply as idempotent; the revision helper treats this as
    // success and issues no save.
    mount_revision_get(
        &server,
        "42",
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

    mount_current_gnmi_state(&server, "disabled", false).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let result = switch.gnmi_service(false).await.unwrap();
    assert_eq!(result["state"], "disabled");
}

#[tokio::test]
async fn gnmi_service_propagates_apply_failure() {
    let server = MockServer::start().await;

    mount_current_gnmi_state(&server, "enabled", false).await;
    mount_create_revision(&server, "42", None).await;
    mount_stage_gnmi_state(&server, "42", "disabled").await;
    mount_apply_revision(&server, "42", None).await;

    mount_revision_get(
        &server,
        "42",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionStateWithIssue {
            state: "failed",
            transition: RevisionTransitionWithIssue {
                issue: RevisionIssueMap {
                    issue_00000: RevisionIssueEntry {
                        message: "gnmi apply rejected",
                        severity: "error",
                    },
                },
            },
        }),
    )
    .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch.gnmi_service(false).await.unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert!(err.message.contains("gnmi apply rejected"));
}

#[tokio::test]
async fn gnmi_service_errors_when_state_never_converges() {
    let server = MockServer::start().await;

    // Every read reports the pre-transition state, so operational convergence
    // never succeeds even though the revision applies cleanly.
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled"
        })))
        .named("GET /nvue_v1/system/gnmi-server never converges")
        .mount(&server)
        .await;

    mount_create_revision(&server, "42", None).await;
    mount_stage_gnmi_state(&server, "42", "disabled").await;
    mount_apply_revision(&server, "42", None).await;
    mount_revision_get(
        &server,
        "42",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;
    mount_save_applied_revision(&server, None).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch.gnmi_service(false).await.unwrap_err();

    // Reachable switch that never converges after the bounded retries is a
    // switch-side, retryable condition, not an RMS-internal fault.
    assert_eq!(err.code, ErrorCode::Unavailable);
    assert!(
        err.message
            .contains("gNMI server state did not reach disabled")
    );
}

#[tokio::test]
async fn gnmi_service_surfaces_last_read_failure_when_never_converging() {
    let server = MockServer::start().await;

    // Preflight read succeeds (enabled != desired disabled), so a genuine
    // revision runs. Every post-apply convergence read then fails with 503.
    mount_current_gnmi_state(&server, "enabled", true).await;
    mount_gnmi_state_revision(&server, "42", "disabled").await;

    // GET-only mock: it shadows only the convergence reads (the stage PATCH
    // carries a `rev` query and a different method, so it is unaffected).
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .respond_with(ResponseTemplate::new(503))
        .named("convergence GET /nvue_v1/system/gnmi-server keeps failing")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch.gnmi_service(false).await.unwrap_err();

    // The real last read cause (HTTP 503) is surfaced rather than the generic
    // "did not reach ... after retries" timeout that previously discarded it.
    assert_eq!(err.code, ErrorCode::Unavailable);
    assert!(
        err.message.contains("503"),
        "expected the HTTP 503 read failure to surface, got: {}",
        err.message
    );
    assert!(
        !err.message.contains("did not reach"),
        "generic retry-exhaustion error masked the real read failure: {}",
        err.message
    );
}

#[tokio::test]
async fn gnmi_service_errors_when_state_missing() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "some-other-field": "value"
        })))
        .expect(1)
        .named("current GET /nvue_v1/system/gnmi-server")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch.gnmi_service(false).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal);
    assert!(
        err.message
            .contains("unable to determine current gNMI server state")
    );
}

async fn mount_applied_nvue_revision_followthrough(server: &MockServer, revision_id: &str) {
    mount_apply_revision(server, revision_id, None).await;
    mount_revision_get(
        server,
        revision_id,
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;
}

async fn mount_applied_nvue_revision(server: &MockServer, revision_id: &str) {
    mount_create_revision(server, revision_id, None).await;
    mount_applied_nvue_revision_followthrough(server, revision_id).await;
    mount_save_applied_revision(server, None).await;
}

struct SequentialJsonBodies {
    bodies: Vec<serde_json::Value>,
    next: AtomicUsize,
}

impl Respond for SequentialJsonBodies {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let index = self.next.fetch_add(1, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_json(self.bodies[index].clone())
    }
}

async fn mount_create_revisions(server: &MockServer, revision_ids: &[&str]) {
    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .and(body_json(RevisionCreate::default()))
        .respond_with(SequentialJsonBodies {
            bodies: revision_ids
                .iter()
                .map(|id| serde_json::json!(id))
                .collect(),
            next: AtomicUsize::new(0),
        })
        .expect(u64::try_from(revision_ids.len()).expect("revision count fits u64"))
        .mount(server)
        .await;
}

#[tokio::test]
async fn capability_discovery_treats_gnmi_mtls_404_as_unsupported() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let capabilities = nvos::GnmiClientCa::discover(&switch).await.unwrap();
    assert!(!capabilities.is_supported());
}

#[tokio::test]
async fn capability_discovery_fails_when_gnmi_mtls_probe_is_unavailable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = nvos::GnmiClientCa::discover(&switch).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Unavailable);
}

#[tokio::test]
async fn initialize_nvue_capabilities_classifies_once_and_reuses_the_session() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let first = switch.initialize_nvue_capabilities().await.unwrap();
    let second = switch.initialize_nvue_capabilities().await.unwrap();
    assert_eq!(first, second);
    assert!(!first.is_supported());
}

#[tokio::test]
async fn gnmi_client_ca_discover_does_not_probe_secondary_cluster_node() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .respond_with(ResponseTemplate::new(503))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let capabilities = nvos::GnmiClientCa::discover(&switch).await.unwrap();
    assert!(!capabilities.is_supported());
}

#[tokio::test]
async fn initialize_nvue_capabilities_ignores_unrelated_secondary_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .respond_with(ResponseTemplate::new(503))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let capabilities = switch.initialize_nvue_capabilities().await.unwrap();
    assert!(!capabilities.is_supported());
}

#[tokio::test]
async fn nvbridge_discover_treats_secondary_cluster_node_404_as_unsupported() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let capabilities = nvos::NvBridge::discover(&switch).await.unwrap();
    assert!(!capabilities.is_supported());
}

#[tokio::test]
async fn nvbridge_discover_treats_secondary_cluster_node_200_as_supported() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let capabilities = nvos::NvBridge::discover(&switch).await.unwrap();
    assert!(capabilities.is_supported());
}

#[tokio::test]
async fn nvbridge_discover_fails_when_secondary_cluster_node_probe_is_unavailable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = nvos::NvBridge::discover(&switch).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Unavailable);
}

#[tokio::test]
async fn bind_gnmi_commits_only_the_entity_certificate_when_mtls_is_unsupported() {
    let server = MockServer::start().await;
    let material = mtls::SwitchMtlsMaterialPaths::for_test();

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    mount_applied_nvue_revision(&server, "42").await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .and(query_param("rev", "42"))
        .and(body_json(serde_json::json!({
            "certificate": material.entity_cert_id()
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    switch
        .bind_mtls_service(
            &material,
            mtls::SwitchMtlsService::ScaleUpFabricTelemetryInterface,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn bind_gnmi_commits_entity_and_ca_in_one_revision_when_mtls_is_supported() {
    let server = MockServer::start().await;
    let material = mtls::SwitchMtlsMaterialPaths::for_test();

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    mount_create_revisions(&server, &["42"]).await;
    mount_applied_nvue_revision_followthrough(&server, "42").await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .and(query_param("rev", "42"))
        .and(body_json(serde_json::json!({
            "certificate": material.entity_cert_id()
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .and(query_param("rev", "42"))
        .and(body_json(serde_json::json!({
            "ca-certificate": material.ca_cert_id()
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .and(body_json(RevisionUpdate::save_with_yes_prompt()))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    switch
        .bind_mtls_service(
            &material,
            mtls::SwitchMtlsService::ScaleUpFabricTelemetryInterface,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn bind_gnmi_ca_patch_404_discards_the_revision_before_apply() {
    let server = MockServer::start().await;
    let material = mtls::SwitchMtlsMaterialPaths::for_test();

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    mount_create_revisions(&server, &["42"]).await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .and(query_param("rev", "42"))
        .and(body_json(serde_json::json!({
            "certificate": material.entity_cert_id()
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .and(query_param("rev", "42"))
        .and(body_json(serde_json::json!({
            "ca-certificate": material.ca_cert_id()
        })))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/revision/42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch
        .bind_mtls_service(
            &material,
            mtls::SwitchMtlsService::ScaleUpFabricTelemetryInterface,
        )
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::Internal);
    assert!(
        err.message
            .contains("NVUE PATCH /nvue_v1/system/gnmi-server/mtls?rev=42")
    );
    assert!(err.message.contains("returned 404"));
}

#[tokio::test]
async fn bind_gnmi_revision_apply_404_fails_the_bind() {
    let server = MockServer::start().await;
    let material = mtls::SwitchMtlsMaterialPaths::for_test();

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    mount_create_revisions(&server, &["42"]).await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .and(query_param("rev", "42"))
        .and(body_json(serde_json::json!({
            "certificate": material.entity_cert_id()
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .and(query_param("rev", "42"))
        .and(body_json(serde_json::json!({
            "ca-certificate": material.ca_cert_id()
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/42"))
        .and(body_json(RevisionUpdate::apply_with_yes_prompt()))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch
        .bind_mtls_service(
            &material,
            mtls::SwitchMtlsService::ScaleUpFabricTelemetryInterface,
        )
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::NotFound);
    assert!(err.message.contains("failed to apply revision 42"));
}

#[tokio::test]
async fn verify_gnmi_reports_null_client_ca_when_mtls_is_unsupported() {
    let server = MockServer::start().await;
    let material = mtls::SwitchMtlsMaterialPaths::for_test();
    let cert_id = material.entity_cert_id();
    let ca_id = material.ca_cert_id();

    Mock::given(method("GET"))
        .and(path(format!(
            "/nvue_v1/system/security/certificate/{cert_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "valid-to": "2999-01-01T00:00:00Z",
            "installed": ["gnmi-server"]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/nvue_v1/system/security/ca-certificate/{ca_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "certificate": cert_id
        })))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let result = switch
        .verify_mtls_services(
            &material,
            &[mtls::SwitchMtlsService::ScaleUpFabricTelemetryInterface],
        )
        .await
        .unwrap();

    assert_eq!(result["gnmi_server"]["certificate"], cert_id);
    assert!(result["gnmi_mtls"].is_null());
    assert_eq!(
        result["gnmi_client_authentication"],
        mtls::GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY
    );
}

#[tokio::test]
async fn verify_gnmi_requires_client_ca_when_mtls_is_supported() {
    let server = MockServer::start().await;
    let material = mtls::SwitchMtlsMaterialPaths::for_test();
    let cert_id = material.entity_cert_id();
    let ca_id = material.ca_cert_id();

    Mock::given(method("GET"))
        .and(path(format!(
            "/nvue_v1/system/security/certificate/{cert_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "valid-to": "2999-01-01T00:00:00Z",
            "installed": ["gnmi-server"]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/nvue_v1/system/security/ca-certificate/{ca_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "certificate": cert_id
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ca-certificate": ca_id
        })))
        .expect(2)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let result = switch
        .verify_mtls_services(
            &material,
            &[mtls::SwitchMtlsService::ScaleUpFabricTelemetryInterface],
        )
        .await
        .unwrap();

    assert_eq!(result["gnmi_server"]["certificate"], cert_id);
    assert_eq!(result["gnmi_mtls"]["ca-certificate"], ca_id);
    assert_eq!(
        result["gnmi_client_authentication"],
        mtls::GNMI_CLIENT_AUTHENTICATION_MTLS
    );
}

#[tokio::test]
async fn verify_gnmi_fails_when_supported_client_ca_disappears() {
    let server = MockServer::start().await;
    let material = mtls::SwitchMtlsMaterialPaths::for_test();
    let cert_id = material.entity_cert_id();
    let ca_id = material.ca_cert_id();

    Mock::given(method("GET"))
        .and(path(format!(
            "/nvue_v1/system/security/certificate/{cert_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "valid-to": "2999-01-01T00:00:00Z",
            "installed": ["gnmi-server"]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/nvue_v1/system/security/ca-certificate/{ca_id}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "certificate": cert_id
        })))
        .expect(1)
        .mount(&server)
        .await;

    struct PresentThenMissing {
        next: AtomicUsize,
    }

    impl Respond for PresentThenMissing {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            if self.next.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({}))
            } else {
                ResponseTemplate::new(404)
            }
        }
    }

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/gnmi-server/mtls"))
        .respond_with(PresentThenMissing {
            next: AtomicUsize::new(0),
        })
        .expect(2)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch
        .verify_mtls_services(
            &material,
            &[mtls::SwitchMtlsService::ScaleUpFabricTelemetryInterface],
        )
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert!(err.message.contains("returned 404"));
}

async fn mock_sdn_reset_submit(server: &MockServer, response_body: serde_json::Value) {
    Mock::given(method("POST"))
        .and(path("/nvue_v1/sdn/factory-default"))
        .and(body_json(serde_json::json!({
            "@reset": {
                "state": "start",
                "parameters": {"force": true}
            }
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(response_body))
        .expect(1)
        .named("POST /nvue_v1/sdn/factory-default")
        .mount(server)
        .await;
}

async fn mock_sdn_reset_action_success(server: &MockServer, action_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/nvue_v1/action/{action_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "action_success",
        })))
        .expect(1)
        .named("GET /nvue_v1/action/<sdn-reset>")
        .mount(server)
        .await;
}

async fn mock_nmx_controller_reset_state(
    server: &MockServer,
    reason: &str,
    control_plane_state: &str,
) {
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "not ok",
            "reason": reason,
            "addition-info": control_plane_state,
        })))
        .up_to_n_times(1)
        .expect(1)
        .named("GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(server)
        .await;
}

#[tokio::test]
async fn reset_sdn_factory_default_waits_for_runtime_in_progress_to_clear() {
    let server = MockServer::start().await;

    mock_sdn_reset_submit(&server, serde_json::json!("reset-1")).await;
    mock_sdn_reset_action_success(&server, "reset-1").await;

    for (reason, control_plane_state) in [
        ("FACTORY RESET IN PROGRESS", "CONTROL_PLANE_STATE_UNDEFINED"),
        (
            "GFM: FACTORY RESET IN PROGRESS, waiting for cleanup",
            "CONTROL_PLANE_STATE_UNCONFIGURED",
        ),
        ("GFM: UNCONFIGURED", "CONTROL_PLANE_STATE_UNCONFIGURED"),
    ] {
        mock_nmx_controller_reset_state(&server, reason, control_plane_state).await;
    }

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let action_id = switch.reset_sdn_factory_default().await.unwrap();

    assert_eq!(action_id, "reset-1");
}

#[tokio::test]
async fn reset_sdn_factory_default_completes_when_runtime_is_already_unconfigured() {
    let server = MockServer::start().await;

    mock_sdn_reset_submit(&server, serde_json::json!("reset-1")).await;
    mock_sdn_reset_action_success(&server, "reset-1").await;

    for _ in 0..SDN_FACTORY_RESET_UNCONFIGURED_CONFIRMATION_POLLS {
        mock_nmx_controller_reset_state(
            &server,
            "GFM: UNCONFIGURED",
            "CONTROL_PLANE_STATE_UNCONFIGURED",
        )
        .await;
    }

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let action_id = switch.reset_sdn_factory_default().await.unwrap();

    assert_eq!(action_id, "reset-1");
}

#[tokio::test]
async fn reset_sdn_factory_default_matches_in_progress_reason_case_insensitively() {
    let server = MockServer::start().await;

    mock_sdn_reset_submit(&server, serde_json::json!("reset-1")).await;
    mock_sdn_reset_action_success(&server, "reset-1").await;

    mock_nmx_controller_reset_state(
        &server,
        "factory reset in progress",
        "CONTROL_PLANE_STATE_UNCONFIGURED",
    )
    .await;

    mock_nmx_controller_reset_state(
        &server,
        "GFM: UNCONFIGURED",
        "CONTROL_PLANE_STATE_UNCONFIGURED",
    )
    .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let action_id = switch.reset_sdn_factory_default().await.unwrap();

    assert_eq!(action_id, "reset-1");
}

#[tokio::test]
async fn reset_sdn_factory_default_fails_when_runtime_state_unavailable() {
    let server = MockServer::start().await;

    mock_sdn_reset_submit(&server, serde_json::json!("reset-1")).await;
    mock_sdn_reset_action_success(&server, "reset-1").await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .named("GET /nvue_v1/cluster/apps/nmx-controller without runtime fields")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch.reset_sdn_factory_default().await.unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);

    assert!(
        err.message
            .contains("target switch may not be the primary switch")
    );
}

#[tokio::test]
async fn reset_sdn_factory_default_fails_when_action_reports_cluster_not_enabled() {
    let server = MockServer::start().await;

    mock_sdn_reset_submit(&server, serde_json::json!("reset-1")).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/action/reset-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "action_error",
            "issue": [
                {
                    "code": "generic",
                    "data": {"msg": "cluster is not enabled"},
                    "message": "cluster is not enabled",
                    "severity": "error",
                }
            ],
        })))
        .expect(1)
        .named("GET /nvue_v1/action/reset-1 cluster disabled")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch.reset_sdn_factory_default().await.unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert!(err.message.contains("cluster is not enabled"));

    assert!(
        err.message
            .contains("target switch may not be the primary switch")
    );
}

#[tokio::test]
async fn reset_sdn_factory_default_completes_when_nmx_controller_is_absent_after_progress() {
    let server = MockServer::start().await;

    mock_sdn_reset_submit(&server, serde_json::json!("reset-1")).await;
    mock_sdn_reset_action_success(&server, "reset-1").await;

    mock_nmx_controller_reset_state(
        &server,
        "FACTORY RESET IN PROGRESS",
        "CONTROL_PLANE_STATE_UNDEFINED",
    )
    .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "detail": "The requested item does not exist.",
            "status": 404,
            "title": "The requested item does not exist.",
            "type": "/errors/ItemDoesNotExist",
        })))
        .expect(1)
        .named("post-reset GET /nvue_v1/cluster/apps/nmx-controller missing")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let action_id = switch.reset_sdn_factory_default().await.unwrap();

    assert_eq!(action_id, "reset-1");
}

#[tokio::test]
async fn reset_sdn_factory_default_fails_when_nmx_controller_absent_without_progress() {
    let server = MockServer::start().await;

    mock_sdn_reset_submit(&server, serde_json::json!("reset-1")).await;
    mock_sdn_reset_action_success(&server, "reset-1").await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "detail": "The requested item does not exist.",
            "status": 404,
            "title": "The requested item does not exist.",
            "type": "/errors/ItemDoesNotExist",
        })))
        .expect(1)
        .named("post-reset GET /nvue_v1/cluster/apps/nmx-controller missing without progress")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch.reset_sdn_factory_default().await.unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert!(err.message.contains("reset progress was not observed"));

    assert!(
        err.message
            .contains("target switch may not be the primary switch")
    );
}

#[tokio::test]
async fn reset_sdn_factory_default_requires_action_id() {
    let server = MockServer::start().await;

    mock_sdn_reset_submit(&server, serde_json::json!({})).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch.reset_sdn_factory_default().await.unwrap_err();

    assert!(err.message.contains("missing action ID"));
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

/// NVUE view of an `nmx-controller` whose manager state has already converged
/// while the app behind it is still starting: NVOS reports the app health check
/// as failing and no control-plane state it recognizes, so the app cannot yet
/// service a manager configuration callback.
fn nmx_controller_starting_response() -> serde_json::Value {
    serde_json::json!({
        "status": "not ok",
        "additional-info": "CONTROL_PLANE_STATE_UNDEFINED",
        "manager": {
            "state": "enabled"
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

/// Mounts the NVUE cluster-app manager `@update` POST that sets the manager
/// state (replacing the former `nv action update ... manager <state>` SSH
/// command). The body matcher pins the exact action envelope.
async fn mount_manager_update(server: &MockServer, app: &str, desired: &str) {
    Mock::given(method("POST"))
        .and(path(format!("/nvue_v1/cluster/apps/{app}/manager")))
        .and(body_json(serde_json::json!({
            "@update": { "state": "start", "parameters": { "state": desired } }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .named("POST @update /nvue_v1/cluster/apps/<app>/manager")
        .mount(server)
        .await;
}

/// Mounts the NVUE cluster-app manager GET that `check_grpc_status` reads to
/// decide whether gRPC is currently enabled. This drives the idempotent
/// short-circuit in `enable_grpc_for_external_clients`: a state of
/// `enabled`/`start`/`active` reports gRPC on, anything else reports it off.
async fn mount_manager_state_get(server: &MockServer, app: &str, state: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/nvue_v1/cluster/apps/{app}/manager")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "state": state })),
        )
        .named("GET /nvue_v1/cluster/apps/<app>/manager")
        .mount(server)
        .await;
}

/// Mounts the NVUE cluster-app `@start` POST that starts a stopped app
/// (replacing the former `nv action start cluster apps <app>` SSH command).
async fn mount_app_start(server: &MockServer, app: &str) {
    Mock::given(method("POST"))
        .and(path(format!("/nvue_v1/cluster/apps/{app}")))
        .and(body_json(
            serde_json::json!({ "@start": { "state": "start" } }),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .named("POST @start /nvue_v1/cluster/apps/<app>")
        .mount(server)
        .await;
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

    mount_manager_update(&server, "nmx-controller", "enabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

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

    mount_manager_update(&server, "nmx-controller", "enabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

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

    mount_manager_update(&server, "nmx-controller", "enabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

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
}

#[tokio::test]
async fn enable_grpc_for_external_clients_attempts_action_after_readiness_timeout() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("disabled")))
        .expect(config::MAX_RETRY_ATTEMPTS as u64 + 1)
        .named("disabled GET /nvue_v1/cluster until timeout")
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

    mount_manager_update(&server, "nmx-controller", "enabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap();

    assert_eq!(status["manager"]["state"], "enabled");
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

    mount_manager_update(&server, "nmx-controller", "enabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

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
}

#[tokio::test]
async fn enable_grpc_for_external_clients_treats_empty_nmxc_conn_object_as_unknown() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
            "nmxc-conn": {}
        })))
        .expect(1)
        .named("cluster GET with nested nmxc-conn missing state")
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

    mount_manager_update(&server, "nmx-controller", "enabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

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

    mount_app_start(&server, "nmx-controller").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

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
}

#[tokio::test]
async fn require_cluster_app_manager_action_ready_starts_stopped_nmx_controller() {
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

    mount_app_start(&server, "nmx-controller").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .require_cluster_app_manager_action_ready("nmx-controller")
        .await
        .unwrap();
}

/// A manager state can converge while the app behind it is still starting, so
/// readiness has to be re-read rather than accepted from the first poll.
#[tokio::test]
async fn require_cluster_app_manager_action_ready_waits_for_a_starting_app() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .named("ready GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(nmx_controller_starting_response()))
        .up_to_n_times(1)
        .expect(1)
        .named("starting GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .expect(1)
        .named("ready GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .require_cluster_app_manager_action_ready("nmx-controller")
        .await
        .unwrap();
}

/// A manager whose app never reports itself ready must fail on the bounded
/// readiness wait rather than leaving the caller to submit a configuration
/// update that the app cannot service.
#[tokio::test]
async fn require_cluster_app_manager_action_ready_errors_when_the_app_never_becomes_ready() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .named("ready GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(nmx_controller_starting_response()))
        .expect(u64::from(config::MAX_RETRY_ATTEMPTS) + 1)
        .named("starting GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch
        .require_cluster_app_manager_action_ready("nmx-controller")
        .await
        .unwrap_err();

    assert_eq!(
        err.message,
        "cluster app nmx-controller did not become ready for manager configuration"
    );
}

/// NVUE does not always report the app fields readiness is read from. That is a
/// reporting gap rather than an unready app, so the caller proceeds and lets the
/// manager update report any real failure itself.
#[tokio::test]
async fn require_cluster_app_manager_action_ready_proceeds_when_readiness_is_not_observable() {
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
            ResponseTemplate::new(200).set_body_json(nmx_controller_unknown_readiness_response()),
        )
        .expect(1)
        .named("GET /nvue_v1/cluster/apps/nmx-controller without readiness fields")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .require_cluster_app_manager_action_ready("nmx-controller")
        .await
        .unwrap();
}

/// An app that already reports `status: ok` accepts manager updates even when
/// `nmxc-conn` is still down. Blocking on that connection exhausts the wait
/// before V1/V2 bring the link up.
#[tokio::test]
async fn require_cluster_app_manager_action_ready_proceeds_when_app_is_ok_and_nmxc_conn_is_down() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
            "nmxc-conn": {"state": "down"}
        })))
        .expect(1)
        .named("enabled cluster GET while ready app keeps nmxc-conn down")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .expect(1)
        .named("ready GET /nvue_v1/cluster/apps/nmx-controller while nmxc-conn is down")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .require_cluster_app_manager_action_ready("nmx-controller")
        .await
        .unwrap();
}

/// NMX-T (nmx-telemetry) manager enable must also flow through the NVUE
/// `@update` action, confirming the shared helper migrated both consumers.
#[tokio::test]
async fn enable_grpc_for_external_clients_enables_nmx_telemetry_via_nvue() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .named("ready GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-telemetry"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("disabled")),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("pre-action GET /nvue_v1/cluster/apps/nmx-telemetry")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-telemetry"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("enabled")),
        )
        .expect(1)
        .named("post-action GET /nvue_v1/cluster/apps/nmx-telemetry")
        .mount(&server)
        .await;

    mount_manager_update(&server, "nmx-telemetry", "enabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let status = switch
        .enable_grpc_for_external_clients("nmx-telemetry", true)
        .await
        .unwrap();

    assert_eq!(status["manager"]["state"], "enabled");
}

/// The disable path (`enabled = false`) is an independent NVUE flow: it skips
/// the manager-action readiness wait, POSTs the `disabled` manager `@update`,
/// then polls `get_cluster_apps_status` for convergence. Exercise it end to end
/// so regressions in the disable branch remain visible to the suite.
#[tokio::test]
async fn disables_grpc_for_external_clients_via_nvue() {
    let server = MockServer::start().await;

    // Pre-check reports gRPC still enabled so the idempotent short-circuit does
    // not fire and the disable action actually runs.
    mount_manager_state_get(&server, "nmx-controller", "enabled").await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("disabled")),
        )
        .expect(1)
        .named("post-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    mount_manager_update(&server, "nmx-controller", "disabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", false)
        .await
        .unwrap();

    assert_eq!(
        status
            .get("manager")
            .and_then(|manager| manager.get("state"))
            .and_then(|state| state.as_str()),
        Some("disabled")
    );
}

/// A disable that begins from an unexpected/transitional manager state must
/// still converge: the post-action poll tolerates a non-terminal reading
/// before `disabled` settles. Without this the disable retry loop is untested.
#[tokio::test]
async fn disable_grpc_for_external_clients_converges_from_transitional_state() {
    let server = MockServer::start().await;

    mount_manager_state_get(&server, "nmx-controller", "enabled").await;

    // First poll observes a transitional manager state (not yet `disabled`);
    // the loop must keep polling rather than error or return early.
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("start")),
        )
        .up_to_n_times(1)
        .expect(1)
        .named("transitional post-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("disabled")),
        )
        .expect(1)
        .named("settled post-action GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    mount_manager_update(&server, "nmx-controller", "disabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", false)
        .await
        .unwrap();

    assert_eq!(status["manager"]["state"], "disabled");
}

/// Idempotence on the disable path: when the pre-check already reports gRPC
/// disabled, the manager `@update` action must be skipped entirely.
#[tokio::test]
async fn disable_grpc_for_external_clients_short_circuits_when_already_disabled() {
    let server = MockServer::start().await;

    mount_manager_state_get(&server, "nmx-controller", "disabled").await;

    // The disable action must NOT be issued when gRPC is already disabled.
    Mock::given(method("POST"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(0)
        .named("manager @update must not run when already disabled")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", false)
        .await
        .unwrap();

    assert_eq!(status["summary"]["grpc_enabled"], false);
}

/// The `encryption` manager field binds its value from a `mode` parameter in
/// NVOS `cue_cluster_v1` (the action token is `<mode>`, not `<encryption>`), so
/// the NVUE `@update` body must carry `parameters.mode`, not the segment name.
/// This pins the path-segment vs parameter-name split in
/// `run_cluster_app_manager_field_action`; reverting to `parameters.encryption`
/// fails to match the mock and the action errors out.
#[tokio::test]
async fn run_cluster_app_manager_field_action_encryption_uses_mode_parameter() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
        ))
        .and(body_json(serde_json::json!({
            "@update": { "state": "start", "parameters": { "mode": "mtls" } }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .named("POST @update /manager/encryption carries the mode parameter")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch
        .run_cluster_app_manager_field_action("nmx-controller", "encryption", "mode", "mtls")
        .await
        .expect("encryption manager field action should post {\"mode\":\"mtls\"}");
}

/// Cert-install SSH backstop: when the NVUE `@update` encryption bind fails
/// (e.g. the NVUE channel is unavailable/mid-transition during mTLS bootstrap),
/// the manager-encryption bind must fall back to the SSH nvCLI form
/// `nv action update cluster apps <app> manager encryption mtls` and succeed.
#[tokio::test]
async fn bind_cluster_app_manager_encryption_falls_back_to_ssh_on_nvue_failure() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
        ))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .named("POST @update /manager/encryption fails, forcing SSH backstop")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch
        .bind_cluster_app_manager_encryption_mtls("nmx-controller")
        .await
        .expect("SSH backstop should complete the encryption bind when NVUE fails");

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action update cluster apps nmx-controller manager encryption mtls".to_owned()],
    );
}

/// When the NVUE `@update` encryption bind succeeds, the SSH backstop must not
/// run: cert installation stays NVUE-first and only touches SSH on failure.
#[tokio::test]
async fn bind_cluster_app_manager_encryption_uses_nvue_without_ssh_on_success() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
        ))
        .and(body_json(serde_json::json!({
            "@update": { "state": "start", "parameters": { "mode": "mtls" } }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .named("POST @update /manager/encryption succeeds via NVUE")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch
        .bind_cluster_app_manager_encryption_mtls("nmx-controller")
        .await
        .expect("NVUE encryption bind should succeed without SSH");

    assert!(recorded_ssh_commands(&commands).is_empty());
}

/// A non-transient client HTTP 4xx (here a 400 for a malformed/invalid request)
/// is a definitive NVUE rejection: it must surface directly rather than being
/// masked by the SSH backstop, which would otherwise hide a real API problem.
#[tokio::test]
async fn bind_cluster_app_manager_encryption_surfaces_client_4xx_without_ssh() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
        ))
        .respond_with(ResponseTemplate::new(400))
        .expect(1)
        .named("POST @update /manager/encryption returns client 4xx")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    let error = switch
        .bind_cluster_app_manager_encryption_mtls("nmx-controller")
        .await
        .expect_err("a client 4xx must surface, not trigger the SSH backstop");

    assert!(
        error.message.contains("returned 400"),
        "message: {}",
        error.message
    );
    assert!(
        recorded_ssh_commands(&commands).is_empty(),
        "SSH backstop must not run for a client 4xx rejection"
    );
}

/// A 409 (e.g. encryption already set) is a definitive rejection and must
/// surface rather than be papered over by the SSH backstop.
#[tokio::test]
async fn bind_cluster_app_manager_encryption_surfaces_conflict_without_ssh() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
        ))
        .respond_with(ResponseTemplate::new(409))
        .expect(1)
        .named("POST @update /manager/encryption returns conflict")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch
        .bind_cluster_app_manager_encryption_mtls("nmx-controller")
        .await
        .expect_err("a 409 conflict must surface, not trigger the SSH backstop");

    assert!(
        recorded_ssh_commands(&commands).is_empty(),
        "SSH backstop must not run for a 409 conflict"
    );
}

/// A failed NVUE action job (the POST is accepted but the async action ends in
/// a failure state) is a definitive rejection and must surface directly.
#[tokio::test]
async fn bind_cluster_app_manager_encryption_surfaces_failed_action_job_without_ssh() {
    let server = MockServer::start().await;
    let job_id = "enc-bind-1";

    Mock::given(method("POST"))
        .and(path(
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(job_id))
        .expect(1)
        .named("POST @update /manager/encryption returns action job")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/nvue_v1/action/{job_id}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"state": "action_failed"})),
        )
        .expect(1)
        .named("GET /nvue_v1/action/<job> reports failure")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch
        .bind_cluster_app_manager_encryption_mtls("nmx-controller")
        .await
        .expect_err("a failed action job must surface, not trigger the SSH backstop");

    assert!(
        recorded_ssh_commands(&commands).is_empty(),
        "SSH backstop must not run for a failed action job"
    );
}

/// A 403 models RMS's outbound identity not yet being trusted mid mTLS
/// transition; this is exactly what the SSH backstop exists for, so it must
/// still fall back rather than surface.
#[tokio::test]
async fn bind_cluster_app_manager_encryption_falls_back_to_ssh_on_forbidden() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
        ))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .named("POST @update /manager/encryption returns 403 (mTLS transition)")
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    switch
        .bind_cluster_app_manager_encryption_mtls("nmx-controller")
        .await
        .expect("a 403 during the mTLS transition should fall back to SSH");

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv action update cluster apps nmx-controller manager encryption mtls".to_owned()],
    );
}

/// When the manager `@update` POST returns an action job, RMS must poll the
/// job to completion (bounded by the action timeout) before checking state.
#[tokio::test]
async fn enable_grpc_for_external_clients_polls_manager_action_job_to_success() {
    let server = MockServer::start().await;
    let job_id = "mgr-update-1";

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
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

    Mock::given(method("POST"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
        .and(body_json(serde_json::json!({
            "@update": { "state": "start", "parameters": { "state": "enabled" } }
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(job_id))
        .expect(1)
        .named("POST @update returning action job")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/nvue_v1/action/{job_id}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"state": "action_success"})),
        )
        .expect(1)
        .named("GET /nvue_v1/action/<job> success")
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

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let status = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap();

    assert_eq!(status["manager"]["state"], "enabled");
}

/// A failed manager action job must surface as an error (with NVUE context)
/// and must not be followed by a post-action state check.
#[tokio::test]
async fn enable_grpc_for_external_clients_propagates_manager_action_failure() {
    let server = MockServer::start().await;
    let job_id = "mgr-update-fail";

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
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

    Mock::given(method("POST"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
        .and(body_json(serde_json::json!({
            "@update": { "state": "start", "parameters": { "state": "enabled" } }
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(job_id))
        .expect(1)
        .named("POST @update returning action job")
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/nvue_v1/action/{job_id}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"state": "action_failed"})),
        )
        .expect(1)
        .named("GET /nvue_v1/action/<job> failed")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let error = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Internal);
}

/// A manager `@update` that applies but never converges to the desired state
/// must time out with an actionable error instead of returning success.
#[tokio::test]
async fn enable_grpc_for_external_clients_errors_when_manager_never_converges() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(cluster_ready_response("enabled")))
        .named("ready GET /nvue_v1/cluster")
        .mount(&server)
        .await;

    // Both the readiness probe and every post-action poll observe the manager
    // still disabled, so convergence is never reached.
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(nmx_controller_status_response("disabled")),
        )
        .named("never-converging GET /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    mount_manager_update(&server, "nmx-controller", "enabled").await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let error = switch
        .enable_grpc_for_external_clients("nmx-controller", true)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Internal);
    assert!(
        error.message.contains("did not reach enabled"),
        "message: {}",
        error.message
    );
}

#[tokio::test]
async fn restart_cluster_app_posts_nvue_stop_and_start() {
    let server = MockServer::start().await;
    let app_path = "/nvue_v1/cluster/apps/nmx-controller";
    let stop_payload = serde_json::json!({
        "@stop": { "state": "start" }
    });
    let start_payload = serde_json::json!({
        "@start": { "state": "start" }
    });

    Mock::given(method("POST"))
        .and(path(app_path))
        .and(body_json(&stop_payload))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .named("POST @stop /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path(app_path))
        .and(body_json(&start_payload))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .named("POST @start /nvue_v1/cluster/apps/nmx-controller")
        .mount(&server)
        .await;

    // restart_cluster_app issues stop/start only and does not poll for status,
    // so no GET mock is required here.
    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);
    switch.restart_cluster_app("nmx-controller").await.unwrap();

    let post_bodies: Vec<serde_json::Value> = server
        .received_requests()
        .await
        .expect("wiremock requests")
        .into_iter()
        .filter(|request| request.method.to_string() == "POST" && request.url.path() == app_path)
        .map(|request| serde_json::from_slice(&request.body).expect("POST body should be JSON"))
        .collect();
    assert_eq!(post_bodies, vec![stop_payload, start_payload]);

    assert!(recorded_ssh_commands(&commands).is_empty());
}

#[tokio::test]
async fn restart_cluster_app_propagates_stop_http_error() {
    let server = MockServer::start().await;
    let app_path = "/nvue_v1/cluster/apps/nmx-controller";

    Mock::given(method("POST"))
        .and(path(app_path))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "error": {
                "message": "Cannot stop nmx-controller while a configuration is applying",
                "token": "device-response-secret",
            },
        })))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch
        .restart_cluster_app("nmx-controller")
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("@stop"), "message: {}", err.message);

    assert!(
        err.message
            .contains("HTTP POST /nvue_v1/cluster/apps/nmx-controller returned 503"),
        "message: {}",
        err.message
    );

    assert!(
        err.message
            .contains("Cannot stop nmx-controller while a configuration is applying"),
        "message: {}",
        err.message
    );

    assert!(err.message.contains("XXXX"), "message: {}", err.message);
    assert!(!err.message.contains("device-response-secret"));
}

/// Verifies restart fails when the `@start` NVUE POST returns an HTTP error,
/// after a successful `@stop` leg (symmetric to `restart_cluster_app_propagates_stop_http_error`).
#[tokio::test]
async fn restart_cluster_app_propagates_start_http_error() {
    let server = MockServer::start().await;
    let app_path = "/nvue_v1/cluster/apps/nmx-controller";
    let stop_payload = serde_json::json!({
        "@stop": { "state": "start" }
    });
    let start_payload = serde_json::json!({
        "@start": { "state": "start" }
    });

    // First leg of restart_cluster_app: stop succeeds.
    Mock::given(method("POST"))
        .and(path(app_path))
        .and(body_json(&stop_payload))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    // Second leg: start fails and the error should propagate to the caller.
    Mock::given(method("POST"))
        .and(path(app_path))
        .and(body_json(&start_payload))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch
        .restart_cluster_app("nmx-controller")
        .await
        .unwrap_err();
    // nvue_run_action_payload wraps the transport error with action context.
    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("@start"), "message: {}", err.message);
}

#[tokio::test]
async fn nvue_action_poll_retries_transient_error_without_resubmitting() {
    let server = MockServer::start().await;
    let action_endpoint = "/nvue_v1/system/security/ca-certificate/test-ca";
    let action_id = "import-ca-1";

    Mock::given(method("POST"))
        .and(path(action_endpoint))
        .respond_with(ResponseTemplate::new(201).set_body_json(action_id))
        .expect(1)
        .mount(&server)
        .await;

    let poll_count = Arc::new(AtomicUsize::new(0));
    let response_count = Arc::clone(&poll_count);

    Mock::given(method("GET"))
        .and(path(format!("/nvue_v1/action/{action_id}")))
        .respond_with(move |_: &wiremock::Request| {
            if response_count.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(503)
            } else {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "state": "action_success",
                }))
            }
        })
        .expect(2)
        .mount(&server)
        .await;

    SwitchGb200Nvidia::for_test(&server.uri())
        .nvue_start_action(
            action_endpoint,
            "@import",
            serde_json::json!({"uri": "file:///home/admin/certs/ca.pem"}),
        )
        .await
        .unwrap();

    assert_eq!(poll_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn nvue_action_poll_returns_non_retryable_error_immediately() {
    let server = MockServer::start().await;
    let action_id = "import-ca-1";

    Mock::given(method("GET"))
        .and(path(format!("/nvue_v1/action/{action_id}")))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;

    let error = SwitchGb200Nvidia::for_test(&server.uri())
        .wait_for_nvue_action_completion(action_id)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Unauthenticated);
}

// ── NVOS installation verification ──────────────────────────────

#[tokio::test]
async fn verify_nvos_installation_skips_ssh_after_a_valid_nvue_response() {
    for body in [
        serde_json::json!({"status": "System is ready", "health-status": "OK"}),
        serde_json::json!({"status": "System is booting", "health": {"status": "N/A"}}),
        serde_json::json!({"status": "System is ready", "health": {"status": "FATAL"}}),
    ] {
        for allow_onie in [false, true] {
            let server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/system"))
                .respond_with(ResponseTemplate::new(200).set_body_json(&body))
                .expect(1)
                .mount(&server)
                .await;

            let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
                .with_ssh_exec_for_test(|command| panic!("SSH must be skipped: {command}"));

            switch.host_endpoint.as_mut().unwrap().credentials = None;

            let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

            let state = switch
                .verify_nvos_installation(allow_onie.then_some(&onie_endpoint))
                .await
                .expect("a valid NVUE response identifies NVOS without SSH credentials");

            assert_eq!(state, NvosInstallationState::Installed);
        }
    }
}

#[tokio::test]
async fn verify_nvos_installation_gates_ssh_fallback_on_recovery_for_rest_failures() {
    for (response, expected_code) in [
        (ResponseTemplate::new(401), ErrorCode::Unauthenticated),
        (ResponseTemplate::new(403), ErrorCode::Internal),
        (ResponseTemplate::new(503), ErrorCode::Unavailable),
        (
            ResponseTemplate::new(200).set_body_string("not JSON"),
            ErrorCode::Internal,
        ),
        (
            ResponseTemplate::new(200).set_body_json(serde_json::json!({})),
            ErrorCode::Internal,
        ),
        (
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "status": "System is ready", "health-status": "OK"
                }))
                .set_delay(Duration::from_secs(6)),
            ErrorCode::Timeout,
        ),
    ] {
        for allow_onie in [false, true] {
            let server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/system"))
                .respond_with(response.clone())
                .expect(1)
                .mount(&server)
                .await;

            let (commands, exec) = record_ssh_commands();
            let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);
            let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

            let result = switch
                .verify_nvos_installation(allow_onie.then_some(&onie_endpoint))
                .await;

            if allow_onie {
                assert_eq!(result.unwrap(), NvosInstallationState::Installed);

                assert_eq!(
                    recorded_ssh_commands(&commands),
                    vec!["nv show system version".to_owned()]
                );
            } else {
                assert_eq!(result.unwrap_err().code, expected_code);
                assert!(recorded_ssh_commands(&commands).is_empty());
            }
        }
    }
}

#[tokio::test]
async fn verify_nvos_installation_gates_ssh_fallback_on_recovery_for_tls_rejection() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.key_pair.serialize_der());

    let tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.cert.der().clone()], key.into())
        .unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_config));

    for allow_onie in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

        let port = listener.local_addr().unwrap().port();
        let (commands, exec) = record_ssh_commands();

        let mut switch =
            SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);

        switch.nvue = Some(
            NvueClient::new(NvueConnectConfig {
                endpoint: NvueEndpoint::https("localhost", "127.0.0.1", port),
                credentials: NvueCredentials::new("admin", "configured-password"),
                dangerously_accept_invalid_certs: false,
            })
            .unwrap(),
        );

        let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

        let handshake = async {
            let (connection, _) = listener.accept().await.unwrap();
            acceptor.accept(connection).await
        };

        let (handshake, result) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                handshake,
                switch.verify_nvos_installation(allow_onie.then_some(&onie_endpoint))
            )
        })
        .await
        .expect("the untrusted NVUE certificate should be rejected promptly");

        assert!(handshake.is_err());

        if allow_onie {
            assert_eq!(result.unwrap(), NvosInstallationState::Installed);

            assert_eq!(
                recorded_ssh_commands(&commands),
                vec!["nv show system version".to_owned()]
            );
        } else {
            let error = result.unwrap_err();

            assert!(error.message.contains("/nvue_v1/system"));
            assert!(recorded_ssh_commands(&commands).is_empty());
        }
    }
}

#[tokio::test]
async fn verify_nvos_installation_skips_binary_and_onie_after_a_positive_version_probe() {
    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let state = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .await
        .expect("the successful NVOS version query should identify the installed operating system");

    assert_eq!(state, NvosInstallationState::Installed);

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec!["nv show system version".to_owned()]
    );
}

#[tokio::test]
async fn verify_nvos_installation_keeps_binary_detection_when_the_version_query_fails() {
    for code in [
        ErrorCode::Internal,
        ErrorCode::Timeout,
        ErrorCode::Unauthenticated,
    ] {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&commands);

        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(
            move |command| {
                recorded.lock().unwrap().push(command.to_owned());

                match command {
                    "nv show system version" => Err(RmsError::new(code, "version query failed")),
                    "command -v nv >/dev/null 2>&1" => Ok(String::new()),
                    unexpected => panic!("ONIE must be skipped when nv exists: {unexpected}"),
                }
            },
        );

        let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

        let state = switch
            .verify_nvos_installation(Some(&onie_endpoint))
            .await
            .expect("a version query failure must not hide an installed NVOS binary");

        assert_eq!(state, NvosInstallationState::Installed);

        assert_eq!(
            recorded_ssh_commands(&commands),
            vec![
                "nv show system version".to_owned(),
                "command -v nv >/dev/null 2>&1".to_owned(),
            ]
        );
    }
}

#[tokio::test]
async fn verify_nvos_installation_preserves_the_rest_error_without_an_onie_endpoint() {
    let switch =
        SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(|command| {
            panic!("SSH detection requires ONIE recovery authorization: {command}")
        });

    let error = switch
        .verify_nvos_installation(None)
        .await
        .expect_err("an NVOS failure without an ONIE endpoint must remain unknown");

    assert_eq!(error.code, ErrorCode::ConnectionRefused);
    assert!(error.message.contains("/nvue_v1/system"));
}

#[tokio::test]
async fn verify_nvos_installation_returns_not_installed_after_a_positive_onie_probe() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;

    let commands = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&commands);

    let exec = move |command: &str| {
        recorded.lock().unwrap().push(command.to_owned());

        match command {
            "nv show system version" | "command -v nv >/dev/null 2>&1" => {
                Err(RmsError::unauthenticated("NVOS credentials rejected"))
            }
            "onie-sysinfo -e" => Ok("AA:BB:CC:00:00:50\n".to_owned()),
            unexpected => panic!("unexpected operating-system probe: {unexpected}"),
        }
    };

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);
    switch.host_endpoint.as_mut().unwrap().endpoint.mac_address = "aa:bb:cc:00:00:50".to_owned();

    let onie_endpoint = endpoint_config(
        "192.0.2.50",
        "aa:bb:cc:00:00:50",
        22,
        "private-onie-user",
        "private-onie-password",
    );

    let writer = TestLogWriter::default();
    let log_writer = writer.clone();

    let subscriber = tracing_subscriber::registry().with(
        crate::logging::logfmt::layer().with_writer(Arc::new(move || Box::new(log_writer.clone()))),
    );

    let state = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .with_subscriber(subscriber)
        .await
        .expect("a positive ONIE marker should identify recovery mode");

    assert_eq!(state, NvosInstallationState::NotInstalled);

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec![
            "nv show system version".to_owned(),
            "command -v nv >/dev/null 2>&1".to_owned(),
            "onie-sysinfo -e".to_owned(),
        ]
    );

    let log = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();

    assert!(
        log.contains("ONIE identified on switch recovery endpoint"),
        "{log}"
    );

    assert!(log.contains("node=test-switch"), "{log}");
    assert!(log.contains("onie_host=192.0.2.50"), "{log}");
    assert!(log.contains("onie_port=22"), "{log}");
    assert!(!log.contains("private-onie-user"));
    assert!(!log.contains("private-onie-password"));
}

#[tokio::test]
async fn verify_nvos_installation_rejects_onie_without_an_expected_host_mac() {
    let mut switch =
        SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(|command| {
            panic!("ONIE must not be probed without an expected host MAC: {command}")
        });

    switch.host_endpoint = None;
    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let error = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .await
        .expect_err("recovery requires an expected host MAC");

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(error.message.contains("host endpoint MAC address"));
}

#[tokio::test]
async fn verify_nvos_installation_reports_both_probe_failures() {
    let mut switch =
        SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(|command| {
            match command {
                "nv show system version" | "command -v nv >/dev/null 2>&1" => {
                    Err(RmsError::unauthenticated("NVOS credentials rejected"))
                }
                "onie-sysinfo -e" => Err(RmsError::connection_refused("ONIE SSH unavailable")),
                unexpected => panic!("unexpected operating-system probe: {unexpected}"),
            }
        });

    switch.host_endpoint.as_mut().unwrap().endpoint.mac_address = "aa:bb:cc:00:00:50".to_owned();

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let error = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .await
        .expect_err("failure of both probes must leave the operating system unknown");

    assert_eq!(error.code, ErrorCode::ConnectionRefused);
    assert!(error.message.contains("NVOS credentials rejected"));
    assert!(error.message.contains("ONIE SSH unavailable"));
}

#[tokio::test]
async fn verify_nvos_installation_rejects_a_different_onie_management_mac() {
    let mut switch =
        SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(|command| {
            match command {
                "nv show system version" | "command -v nv >/dev/null 2>&1" => {
                    Err(RmsError::connection_refused("NVOS SSH unavailable"))
                }
                "onie-sysinfo -e" => Ok("aa:bb:cc:00:00:51\n".to_owned()),
                unexpected => panic!("unexpected operating-system probe: {unexpected}"),
            }
        });

    switch.host_endpoint.as_mut().unwrap().endpoint.mac_address = "aa:bb:cc:00:00:50".to_owned();

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let error = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .await
        .expect_err("a different switch must not be authorized for recovery");

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(error.message.contains("MAC does not match"));
    assert!(error.message.contains("refusing recovery"));
}

#[tokio::test]
async fn verify_nvos_installation_rejects_a_mismatched_supplied_onie_mac() {
    let mut switch =
        SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(|command| {
            match command {
                "nv show system version" | "command -v nv >/dev/null 2>&1" => {
                    Err(RmsError::connection_refused("NVOS SSH unavailable"))
                }
                unexpected => panic!("ONIE must not be probed with conflicting MACs: {unexpected}"),
            }
        });

    switch.host_endpoint.as_mut().unwrap().endpoint.mac_address = "aa:bb:cc:00:00:50".to_owned();

    let onie_endpoint = endpoint_config(
        "192.0.2.50",
        "aa:bb:cc:00:00:51",
        22,
        "onie",
        "onie-password",
    );

    let error = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .await
        .expect_err("conflicting endpoint MACs must fail before ONIE is probed");

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(error.message.contains("endpoint MAC does not match"));
}

#[tokio::test]
async fn verify_nvos_installation_rejects_a_missing_or_invalid_host_mac() {
    for host_mac in ["", "00:00:00:00:00:00", "01:00:5e:00:00:01", "not-a-mac"] {
        let mut switch =
            SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(|command| {
                match command {
                    "nv show system version" | "command -v nv >/dev/null 2>&1" => {
                        Err(RmsError::connection_refused("NVOS SSH unavailable"))
                    }
                    unexpected => {
                        panic!("ONIE must not be probed without a valid host MAC: {unexpected}")
                    }
                }
            });

        switch.host_endpoint.as_mut().unwrap().endpoint.mac_address = host_mac.to_owned();

        let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

        let error = switch
            .verify_nvos_installation(Some(&onie_endpoint))
            .await
            .expect_err("recovery requires a valid host MAC");

        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert!(error.message.contains("host endpoint MAC address"));
    }
}

#[tokio::test]
async fn verify_nvos_installation_rejects_an_invalid_onie_mac_output() {
    let mut switch =
        SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(|command| {
            match command {
                "nv show system version" | "command -v nv >/dev/null 2>&1" => {
                    Err(RmsError::connection_refused("NVOS SSH unavailable"))
                }
                "onie-sysinfo -e" => Ok("invalid-mac\n".to_owned()),
                unexpected => panic!("unexpected operating-system probe: {unexpected}"),
            }
        });

    switch.host_endpoint.as_mut().unwrap().endpoint.mac_address = "aa:bb:cc:00:00:50".to_owned();

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let error = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .await
        .expect_err("an invalid ONIE MAC must not authorize recovery");

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(error.message.contains("invalid management Ethernet MAC"));
}

#[tokio::test]
async fn verify_nvos_installation_rejects_an_onie_endpoint_without_credentials() {
    let commands = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&commands);

    let exec = move |command: &str| {
        recorded.lock().unwrap().push(command.to_owned());
        Err(RmsError::unauthenticated("NVOS credentials rejected"))
    };

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);

    let onie_endpoint = EndpointConfig::new(Endpoint {
        ip_address: "192.0.2.50".to_owned(),
        mac_address: String::new(),
        port: 22,
        host_name: None,
    });

    let error = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .await
        .expect_err("the ONIE probe requires explicit credentials");

    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert!(error.message.contains("username/password credentials"));

    assert_eq!(
        recorded_ssh_commands(&commands),
        vec![
            "nv show system version".to_owned(),
            "command -v nv >/dev/null 2>&1".to_owned(),
        ]
    );
}

#[tokio::test]
async fn verify_nvos_installation_rejects_a_non_ssh_onie_port() {
    let switch =
        SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(|command| {
            assert!(matches!(
                command,
                "nv show system version" | "command -v nv >/dev/null 2>&1"
            ));

            Err(RmsError::unauthenticated("NVOS credentials rejected"))
        });

    let onie_endpoint = endpoint_config("192.0.2.50", "", 2222, "onie", "onie-password");

    let error = switch
        .verify_nvos_installation(Some(&onie_endpoint))
        .await
        .expect_err("the ONIE SSH client only supports port 22");

    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert!(error.message.contains("must use SSH port 22"));
}

#[tokio::test]
async fn stage_nvos_recovery_image_uses_scp_with_the_onie_path() {
    let directory = tempfile::tempdir().expect("create installer directory");
    let local_path = directory.path().join("nvos-25.02.0.bin");

    std::fs::write(
        &local_path,
        vec![0_u8; config::MIN_FIRMWARE_FILE_SIZE as usize],
    )
    .expect("create installer image");

    let uploads = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&uploads);

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
        .with_host_credentials_for_test("admin", "requested-password")
        .with_scp_upload_for_test(move |local_path, remote_path| {
            recorded
                .lock()
                .unwrap()
                .push((local_path.to_owned(), remote_path.to_owned()));

            Ok(())
        })
        .with_ssh_exec_for_test(|command| {
            assert_eq!(command, "wc -c < '/tmp/nvos-25.02.0.bin'");
            Ok(config::MIN_FIRMWARE_FILE_SIZE.to_string())
        });

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    switch
        .stage_nvos_recovery_image(
            &onie_endpoint,
            local_path.to_str().expect("UTF-8 test path"),
            "nvos-25.02.0.bin",
            SshUploadOptions::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("the recovery image should be staged through SCP");

    assert_eq!(
        uploads.lock().unwrap().as_slice(),
        [(
            local_path.to_str().expect("UTF-8 test path").to_owned(),
            "/tmp/nvos-25.02.0.bin".to_owned(),
        )]
    );
}

#[tokio::test]
async fn stage_nvos_recovery_image_rejects_an_undersized_installer() {
    let directory = tempfile::tempdir().expect("create installer directory");
    let local_path = directory.path().join("nvos-small.bin");

    std::fs::write(&local_path, b"short").expect("create undersized installer");

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
        .with_host_credentials_for_test("admin", "requested-password")
        .with_scp_upload_for_test(|_, _| panic!("undersized installer must not be uploaded"));

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let error = switch
        .stage_nvos_recovery_image(
            &onie_endpoint,
            local_path.to_str().expect("UTF-8 test path"),
            "nvos-small.bin",
            SshUploadOptions::default(),
            &CancellationToken::new(),
        )
        .await
        .expect_err("undersized installer must be rejected");

    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert!(error.message.contains("too small"));
}

#[tokio::test]
async fn stage_nvos_recovery_image_rejects_a_truncated_remote_copy() {
    let directory = tempfile::tempdir().expect("create installer directory");
    let local_path = directory.path().join("nvos-25.02.0.bin");

    std::fs::write(
        &local_path,
        vec![0_u8; config::MIN_FIRMWARE_FILE_SIZE as usize],
    )
    .expect("create installer image");

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
        .with_host_credentials_for_test("admin", "requested-password")
        .with_scp_upload_for_test(|_, _| Ok(()))
        .with_ssh_exec_for_test(|command| {
            assert_eq!(command, "wc -c < '/tmp/nvos-25.02.0.bin'");
            Ok("512".to_owned())
        });

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let error = switch
        .stage_nvos_recovery_image(
            &onie_endpoint,
            local_path.to_str().expect("UTF-8 test path"),
            "nvos-25.02.0.bin",
            SshUploadOptions::default(),
            &CancellationToken::new(),
        )
        .await
        .expect_err("truncated ONIE copy must be rejected");

    assert_eq!(error.code, ErrorCode::FailedPrecondition);

    assert!(
        error
            .message
            .contains("expected 1024 bytes, found 512 bytes")
    );
}

#[tokio::test]
async fn install_staged_nvos_image_waits_until_the_nvos_probe_succeeds() {
    let commands = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&commands);
    let probe_count = Arc::new(AtomicUsize::new(0));
    let probes = Arc::clone(&probe_count);

    let mut switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
        .with_host_credentials_for_test("admin", "requested-password")
        .with_ssh_exec_for_test(move |command| {
            recorded.lock().unwrap().push(command.to_owned());

            if command.starts_with("rm -f '/tmp/rms-onie-nos-install.log'")
                || command.starts_with("if [ -f '/tmp/rms-onie-nos-install.status'")
            {
                return Ok(String::new());
            }

            assert_eq!(command, "command -v nv >/dev/null 2>&1");

            if probes.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(RmsError::connection_refused("NVOS SSH is still booting"))
            } else {
                Ok(String::new())
            }
        });

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    switch
        .install_staged_nvos_image_and_wait(
            &onie_endpoint,
            "nvos-25.02.0.bin",
            Duration::from_millis(100),
            &CancellationToken::new(),
        )
        .await
        .expect("NVOS recovery should complete after the NVOS probe succeeds");

    let commands = recorded_ssh_commands(&commands);

    assert_eq!(commands.len(), 6);
    assert!(commands[0].contains("onie-nos-install '/tmp/nvos-25.02.0.bin'"));
    assert_eq!(probe_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn install_staged_nvos_image_respects_the_recovery_timeout() {
    let mut switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
        .with_host_credentials_for_test("admin", "requested-password")
        .with_ssh_exec_for_test(|command| {
            if command.starts_with("rm -f '/tmp/rms-onie-nos-install.log'")
                || command.starts_with("if [ -f '/tmp/rms-onie-nos-install.status'")
            {
                Ok(String::new())
            } else if command.starts_with("tail -c ") {
                Ok("installer did not finish".to_owned())
            } else {
                Err(RmsError::connection_refused("NVOS SSH is still booting"))
            }
        });

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let error = tokio::time::timeout(
        Duration::from_secs(1),
        switch.install_staged_nvos_image_and_wait(
            &onie_endpoint,
            "nvos-25.02.0.bin",
            Duration::from_millis(5),
            &CancellationToken::new(),
        ),
    )
    .await
    .expect("configured ONIE recovery timeout should finish promptly")
    .expect_err("an unreachable NVOS endpoint must time out");

    assert_eq!(error.code, ErrorCode::Timeout);
    assert!(error.message.contains("timed out waiting for NVOS"));
    assert!(error.message.contains("installer did not finish"));
}

#[tokio::test]
async fn install_staged_nvos_image_reports_failed_status_with_redacted_log_tail() {
    let mut switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
        .with_host_credentials_for_test("admin", "requested-password")
        .with_ssh_exec_for_test(|command| {
            if command.starts_with("rm -f '/tmp/rms-onie-nos-install.log'") {
                Ok(String::new())
            } else if command.starts_with("if [ -f '/tmp/rms-onie-nos-install.status'") {
                Ok("1\n".to_owned())
            } else if command.starts_with("tail -c ") {
                Ok(format!(
                    "password=onie-password failed with requested-password token=unknown {}",
                    "diagnostic ".repeat(100)
                ))
            } else {
                Err(RmsError::connection_refused("NVOS SSH is still booting"))
            }
        });

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");

    let error = switch
        .install_staged_nvos_image_and_wait(
            &onie_endpoint,
            "nvos-25.02.0.bin",
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .expect_err("failed installer status must stop recovery early");

    assert_eq!(error.code, ErrorCode::Internal);
    assert!(error.message.contains("exited with status 1"));
    assert!(error.message.contains("password=XXXX"));
    assert!(error.message.contains("token=XXXX"));
    assert!(!error.message.contains("onie-password"));
    assert!(!error.message.contains("requested-password"));
    assert!(!error.message.contains("token=unknown"));
    assert!(error.message.chars().count() < 600);
}

#[tokio::test]
async fn stage_nvos_recovery_image_honors_preexisting_cancellation() {
    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9")
        .with_scp_upload_for_test(|_, _| panic!("cancelled recovery must not start SCP"));

    let onie_endpoint = endpoint_config("192.0.2.50", "", 22, "onie", "onie-password");
    let cancel = CancellationToken::new();

    cancel.cancel();

    let error = switch
        .stage_nvos_recovery_image(
            &onie_endpoint,
            "/var/lib/rms/nvos-25.02.0.bin",
            "nvos-25.02.0.bin",
            SshUploadOptions::default(),
            &cancel,
        )
        .await
        .expect_err("preexisting cancellation should stop ONIE staging");

    assert_eq!(error.code, ErrorCode::Cancelled);
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
fn is_retryable_nvue_transport_error_covers_transports() {
    assert!(is_retryable_nvue_transport_error(&RmsError::new(
        ErrorCode::ConnectionRefused,
        "connection refused"
    )));
    assert!(is_retryable_nvue_transport_error(&RmsError::timeout(
        "slow"
    )));
    assert!(is_retryable_nvue_transport_error(&RmsError::new(
        ErrorCode::Unavailable,
        "unavailable"
    )));
    assert!(is_retryable_nvue_transport_error(&RmsError::new(
        ErrorCode::DnsResolutionFailed,
        "dns"
    )));
    assert!(!is_retryable_nvue_transport_error(
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
        rejection: None,
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
        rejection: None,
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

#[test]
fn strict_action_poll_ignores_reboot_and_noop_hints() {
    use nvue::NvueActionPoll;

    let failed = |state: &str, message: &str| NvueActionPoll::Failed {
        state: state.to_owned(),
        message: message.to_owned(),
    };

    let cases = [
        (
            serde_json::json!({"state": "action_success"}),
            NvueActionPoll::Succeeded,
        ),
        (
            serde_json::json!({"state": "action_running", "status": "Reboot in progress"}),
            NvueActionPoll::Running,
        ),
        (
            serde_json::json!({"status": "action_success"}),
            NvueActionPoll::Running,
        ),
        (serde_json::json!({}), NvueActionPoll::Running),
        (
            serde_json::json!({"state": "action_error", "detail": "system will reboot"}),
            failed("action_error", "system will reboot"),
        ),
        (
            serde_json::json!({"state": "action_error", "issue": ["Nothing to uninstall"]}),
            failed("action_error", "Nothing to uninstall"),
        ),
        (
            serde_json::json!({"state": "action_failed", "status": "power cycle required"}),
            failed("action_failed", "power cycle required"),
        ),
    ];

    for (response, expected) in cases {
        assert_eq!(NvueActionPoll::strict(&response), expected, "{response}");
    }
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
async fn password_rotation_resumable_saves_when_endpoint_fails_and_requested_authenticates() {
    let server = MockServer::start().await;

    mount_system_probe(&server, TEST_AUTH, 401).await;
    mount_system_probe(&server, ROTATED_AUTH, 200).await;
    mount_save_applied_revision(&server, Some(ROTATED_AUTH)).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let outcome = switch
        .update_system_user_password_persisted_resumable("testusername", "rotated-password", None)
        .await
        .unwrap();

    assert_eq!(outcome, SwitchSystemPasswordUpdateOutcome::Resumed);

    assert!(
        switch
            .nvue_client()
            .unwrap()
            .credentials_match("testusername", "rotated-password")
    );
}

#[tokio::test]
async fn password_rotation_resumable_saves_with_provided_candidate_client_without_original_probe() {
    let endpoint_server = MockServer::start().await;
    let candidate_server = MockServer::start().await;

    mount_system_probe(&candidate_server, ROTATED_AUTH, 200).await;
    mount_save_applied_revision(&candidate_server, Some(ROTATED_AUTH)).await;

    let candidate = SwitchGb200Nvidia::for_test(&candidate_server.uri())
        .with_host_credentials_for_test("testusername", "test-password")
        .build_candidate_nvue_client("testusername", "rotated-password")
        .unwrap();

    let mut switch = SwitchGb200Nvidia::for_test(&endpoint_server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let outcome = switch
        .update_system_user_password_persisted_resumable(
            "testusername",
            "rotated-password",
            Some(candidate),
        )
        .await
        .unwrap();

    assert_eq!(outcome, SwitchSystemPasswordUpdateOutcome::Resumed);

    let endpoint_requests = endpoint_server
        .received_requests()
        .await
        .expect("wiremock should record requests");

    assert!(endpoint_requests.is_empty());
}

#[tokio::test]
async fn password_rotation_resumable_rejects_non_successful_provided_candidate_probe() {
    for (status, expected_code) in [
        (401, ErrorCode::Unauthenticated),
        (403, ErrorCode::Internal),
        (503, ErrorCode::Unavailable),
    ] {
        let endpoint_server = MockServer::start().await;
        let candidate_server = MockServer::start().await;

        mount_system_probe(&candidate_server, ROTATED_AUTH, status).await;

        let candidate = SwitchGb200Nvidia::for_test(&candidate_server.uri())
            .with_host_credentials_for_test("testusername", "test-password")
            .build_candidate_nvue_client("testusername", "rotated-password")
            .unwrap();

        let mut switch = SwitchGb200Nvidia::for_test(&endpoint_server.uri())
            .with_host_credentials_for_test("testusername", "test-password");

        let result = switch
            .update_system_user_password_persisted_resumable(
                "testusername",
                "rotated-password",
                Some(candidate),
            )
            .await;

        let Err(err) = result else {
            panic!("expected candidate probe status {status} to stop password rotation resume");
        };

        assert_eq!(err.code, expected_code);
        assert!(err.message.contains(&format!("returned {status}")));

        assert!(
            switch
                .nvue_client()
                .unwrap()
                .credentials_match("testusername", "test-password")
        );

        let candidate_requests = candidate_server
            .received_requests()
            .await
            .expect("wiremock should record requests");

        assert_eq!(candidate_requests.len(), 1);
    }
}

#[tokio::test]
async fn password_rotation_resumable_same_endpoint_password_saves_applied_config() {
    let server = MockServer::start().await;

    mount_system_probe(&server, TEST_AUTH, 200).await;
    mount_save_applied_revision(&server, Some(TEST_AUTH)).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let outcome = switch
        .update_system_user_password_persisted_resumable("testusername", "test-password", None)
        .await
        .unwrap();

    assert_eq!(outcome, SwitchSystemPasswordUpdateOutcome::Resumed);

    let requests = server
        .received_requests()
        .await
        .expect("wiremock should record requests");

    assert!(requests.iter().any(|request| {
        request.method == wiremock::http::Method::Patch
            && request.url.path() == "/nvue_v1/revision/applied"
    }));

    assert!(!requests.iter().any(|request| {
        request.method == wiremock::http::Method::Post && request.url.path() == "/nvue_v1/revision"
    }));
}

#[tokio::test]
async fn password_rotation_resumable_fails_when_endpoint_and_requested_are_rejected() {
    let server = MockServer::start().await;

    mount_system_probe(&server, TEST_AUTH, 401).await;
    mount_system_probe(&server, ROTATED_AUTH, 401).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let result = switch
        .update_system_user_password_persisted_resumable("testusername", "rotated-password", None)
        .await;

    let Err(err) = result else {
        panic!("expected resumable password auth failure");
    };

    assert_eq!(err.code, ErrorCode::Unauthenticated);

    assert!(
        err.message
            .contains("neither endpoint nor requested switch credentials authenticated")
    );
}

#[tokio::test]
async fn password_rotation_resumable_does_not_use_requested_password_for_different_endpoint_user() {
    let server = MockServer::start().await;

    mount_system_probe(&server, TEST_AUTH, 401).await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let result = switch
        .update_system_user_password_persisted_resumable("targetuser", "rotated-password", None)
        .await;

    let Err(err) = result else {
        panic!("expected endpoint auth failure");
    };

    assert_eq!(err.code, ErrorCode::Unauthenticated);

    assert!(
        !switch
            .nvue_client()
            .unwrap()
            .credentials_match("targetuser", "rotated-password")
    );
}

#[tokio::test]
async fn password_rotation_resumable_save_failure_redacts_requested_password() {
    let server = MockServer::start().await;

    mount_system_probe(&server, TEST_AUTH, 401).await;
    mount_system_probe(&server, ROTATED_AUTH, 200).await;

    mount_save_applied_revision_response(
        &server,
        Some(ROTATED_AUTH),
        ResponseTemplate::new(400).set_body_json(PasswordPolicyError {
            detail: "password rotated-password cannot be saved",
            status: 400,
            title: "Bad Request",
        }),
    )
    .await;

    let mut switch = SwitchGb200Nvidia::for_test(&server.uri())
        .with_host_credentials_for_test("testusername", "test-password");

    let result = switch
        .update_system_user_password_persisted_resumable("testusername", "rotated-password", None)
        .await;

    let Err(err) = result else {
        panic!("expected resumable save failure");
    };

    assert!(!err.message.contains("rotated-password"));
    assert!(err.message.contains("XXXX"));
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
async fn password_rotation_applied_and_saved_still_saves_applied_config() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "36", None).await;

    mount_stage_password_success(&server, "36", "targetuser", "rotated-password", None).await;

    mount_apply_revision(&server, "36", None).await;

    mount_revision_get(
        &server,
        "36",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState {
            state: "applied_and_saved",
        }),
    )
    .await;

    // Rotation saves unconditionally: the save doubles as the repair path for
    // an earlier attempt that applied but failed to save.
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
async fn nvue_finish_config_revision_skips_save_when_nvue_already_saved() {
    let server = MockServer::start().await;

    mount_apply_revision(&server, "4", None).await;

    mount_revision_get(
        &server,
        "4",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState {
            state: "applied_and_saved",
        }),
    )
    .await;

    // NVUE already persisted the revision, so RMS must not save it again.
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .and(body_json(RevisionUpdate::save_with_yes_prompt()))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    switch.nvue_start_config_revision("4").await.unwrap();
    switch.nvue_finish_config_revision("4").await.unwrap();
}

#[tokio::test]
async fn nvue_stage_config_patches_discards_candidate_on_patch_failure() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "42", None).await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/api"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "detail": "patch rejected"
        })))
        .expect(1)
        .named("PATCH /nvue_v1/system/api?rev failure")
        .mount(&server)
        .await;

    // The created candidate must be discarded when staging fails.
    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/revision/42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .named("DELETE /nvue_v1/revision/42 (discard candidate)")
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch
        .nvue_stage_config_patches(&[(
            "/nvue_v1/system/api",
            serde_json::json!({"certificate": "server-cert"}),
        )])
        .await
        .unwrap_err();

    assert!(
        err.message
            .contains("NVUE PATCH /nvue_v1/system/api?rev=42")
    );
}

#[tokio::test]
async fn nvue_stage_config_patches_preserves_staging_error_when_discard_fails() {
    let server = MockServer::start().await;

    mount_create_revision(&server, "42", None).await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/api"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "detail": "patch rejected"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Even when the discard attempt fails, the original staging error must be
    // surfaced (the discard is best-effort and only logged).
    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/revision/42"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let err = switch
        .nvue_stage_config_patches(&[(
            "/nvue_v1/system/api",
            serde_json::json!({"certificate": "server-cert"}),
        )])
        .await
        .unwrap_err();

    assert!(
        err.message
            .contains("NVUE PATCH /nvue_v1/system/api?rev=42")
    );
}

/// A secondary server 404 is not an empty address set. Treating only the
/// applied read that way created a revision and then failed the revision read.
#[tokio::test]
async fn secondary_server_404_fails_before_creating_a_revision()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let desired = ClusterNodeServerAddresses::from(["192.0.2.101".parse()?]);

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .respond_with(ResponseTemplate::new(200).set_body_json("42"))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let err = switch
        .reconcile_node_ips(InterfaceType::Secondary, &desired)
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::NotFound);

    Ok(())
}

#[tokio::test]
async fn matching_node_ips_skip_revision_mutation()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let desired = ClusterNodeServerAddresses::from(["192.0.2.101".parse()?]);
    let desired_payload = serde_json::json!({"192.0.2.101": {}});

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&desired_payload))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let changed = switch
        .reconcile_node_ips(InterfaceType::Primary, &desired)
        .await?;

    assert!(!changed);

    Ok(())
}

#[tokio::test]
async fn drifted_node_ips_use_delete_patch_apply_save_and_readback()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;

    let desired =
        ClusterNodeServerAddresses::from(["192.0.2.101".parse()?, "192.0.2.102".parse()?]);

    let previous_payload = serde_json::json!({"192.0.2.101": {}});

    let desired_payload = serde_json::json!({
        "192.0.2.101": {},
        "192.0.2.102": {},
    });

    let revision_current_payload = serde_json::json!({
        "192.0.2.101": {},
        "192.0.2.103": {},
    });

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&previous_payload))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&desired_payload))
        .expect(1)
        .mount(&server)
        .await;

    mount_create_revision(&server, "42", None).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&revision_current_payload))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/cluster/node/primary"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "42"))
        .and(body_json(&desired_payload))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody {}))
        .expect(1)
        .mount(&server)
        .await;

    mount_apply_revision(&server, "42", None).await;

    mount_revision_get(
        &server,
        "42",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    mount_save_applied_revision(&server, None).await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let changed = switch
        .reconcile_node_ips(InterfaceType::Primary, &desired)
        .await?;

    assert!(changed);

    Ok(())
}

#[tokio::test]
async fn disabling_cluster_clears_both_node_ip_interfaces_while_holding_operation_lock()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let configured_payload = serde_json::json!({"192.0.2.101": {}});
    let empty_payload = serde_json::json!({});

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured_payload))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&empty_payload))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured_payload))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&empty_payload))
        .expect(1)
        .mount(&server)
        .await;

    // Three revisions clear both node IP interfaces and then disable the
    // cluster through NVUE.
    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .and(body_json(RevisionCreate::default()))
        .respond_with(ResponseTemplate::new(200).set_body_json("42"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .and(body_json(RevisionCreate::default()))
        .respond_with(ResponseTemplate::new(200).set_body_json("43"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .and(body_json(RevisionCreate::default()))
        .respond_with(ResponseTemplate::new(200).set_body_json("44"))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured_payload))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/cluster/node/primary"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    mount_apply_revision(&server, "42", None).await;

    mount_revision_get(
        &server,
        "42",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary/server"))
        .and(query_param("rev", "43"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured_payload))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .and(query_param("rev", "43"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    mount_apply_revision(&server, "43", None).await;

    mount_revision_get(
        &server,
        "43",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    // Cluster-disable revision (`44`): stage -> apply -> wait.
    mount_stage_cluster_state(&server, "44", "disabled").await;
    mount_apply_revision(&server, "44", None).await;

    mount_revision_get(
        &server,
        "44",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    // All three revisions persist through `/revision/applied`.
    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .and(body_json(RevisionUpdate::save_with_yes_prompt()))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(3)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled"
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "disabled"
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "not ok",
            "reason": "starting"
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let (commands, exec) = record_ssh_commands();
    let switch = SwitchGb200Nvidia::for_test(&server.uri()).with_ssh_exec_for_test(exec);

    // The whole sequence runs under a single op_lock acquisition: the disable
    // step calls the `_unlocked` variant, so re-taking the lock (which would
    // deadlock) would hang this test.
    switch
        .clear_node_ips_and_disable_cluster(nvos::NvBridge::for_test(true))
        .await?;

    // Disabling the cluster no longer shells into the switch.
    assert!(recorded_ssh_commands(&commands).is_empty());

    let requests = server
        .received_requests()
        .await
        .expect("wiremock should record requests");

    let primary_delete_position = requests
        .iter()
        .position(|request| {
            request.method == wiremock::http::Method::Delete
                && request.url.path() == "/nvue_v1/cluster/node/primary"
        })
        .expect("primary node IP deletion should be requested");

    let secondary_delete_position = requests
        .iter()
        .position(|request| {
            request.method == wiremock::http::Method::Delete
                && request.url.path() == "/nvue_v1/cluster/node/secondary"
        })
        .expect("secondary node IP deletion should be requested");

    let state_read_position = requests
        .iter()
        .position(|request| {
            request.method == wiremock::http::Method::Get
                && request.url.path() == "/nvue_v1/cluster"
        })
        .expect("cluster state should be read");

    assert!(primary_delete_position < state_read_position);
    assert!(secondary_delete_position < state_read_position);

    Ok(())
}

#[tokio::test]
async fn disabling_cluster_skips_secondary_node_ips_when_nvbridge_is_unsupported()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let configured_payload = serde_json::json!({"192.0.2.101": {}});
    let empty_payload = serde_json::json!({});

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured_payload))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&empty_payload))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .and(body_json(RevisionCreate::default()))
        .respond_with(ResponseTemplate::new(200).set_body_json("42"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .and(body_json(RevisionCreate::default()))
        .respond_with(ResponseTemplate::new(200).set_body_json("43"))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured_payload))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/cluster/node/primary"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    mount_apply_revision(&server, "42", None).await;

    mount_revision_get(
        &server,
        "42",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/cluster"))
        .and(query_param("rev", "43"))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(1)
        .mount(&server)
        .await;

    mount_apply_revision(&server, "43", None).await;

    mount_revision_get(
        &server,
        "43",
        None,
        ResponseTemplate::new(200).set_body_json(RevisionState { state: "applied" }),
    )
    .await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .and(body_json(RevisionUpdate::save_with_yes_prompt()))
        .respond_with(ResponseTemplate::new(200).set_body_json(EmptyBody::default()))
        .expect(2)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled"
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "disabled"
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "not ok",
            "reason": "starting"
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary/server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured_payload))
        .expect(0)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    switch
        .clear_node_ips_and_disable_cluster(nvos::NvBridge::for_test(false))
        .await?;

    Ok(())
}

#[tokio::test]
async fn converged_node_ips_in_created_revision_discard_candidate()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;

    let desired =
        ClusterNodeServerAddresses::from(["192.0.2.101".parse()?, "192.0.2.102".parse()?]);

    let previous_payload = serde_json::json!({"192.0.2.101": {}});

    let desired_payload = serde_json::json!({
        "192.0.2.101": {},
        "192.0.2.102": {},
    });

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&previous_payload))
        .expect(1)
        .mount(&server)
        .await;

    mount_create_revision(&server, "42", None).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&desired_payload))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/revision/42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    let changed = switch
        .reconcile_node_ips(InterfaceType::Primary, &desired)
        .await?;

    assert!(!changed);

    Ok(())
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

// ── Config constants ────────────────────────────────────────────

#[test]
fn config_constant_values() {
    assert_eq!(config::MIN_FIRMWARE_FILE_SIZE, 1024);
    assert_eq!(config::SFTP_BUFFER_SIZE, 512 * 1024);
    assert_eq!(config::REVISION_POLL_INTERVAL_SECONDS, 1);
    assert_eq!(config::REVISION_APPLY_TIMEOUT_SECONDS, 60);
    assert_eq!(config::MAX_RETRY_ATTEMPTS, 3);
    assert_eq!(config::RETRY_WAIT_SECONDS, 30);
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

// ── NVOS readiness ──────────────────────────────────────────────

async fn mount_system(server: &MockServer, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

#[tokio::test]
async fn verify_nvos_ready_passes_when_ready_and_healthy() {
    let server = MockServer::start().await;
    mount_system(
        &server,
        serde_json::json!({
            "hostname": "switch",
            "status": "System is ready",
            "health": {"status": "OK"},
            "uptime": 11659
        }),
    )
    .await;
    let sw = SwitchGb200Nvidia::for_test(&server.uri());

    sw.verify_nvos_ready().await.unwrap();
}

#[tokio::test]
async fn verify_nvos_ready_fails_precondition_when_switch_reports_unhealthy() {
    let server = MockServer::start().await;
    mount_system(
        &server,
        serde_json::json!({ "status": "System is ready", "health": {"status": "Not OK"} }),
    )
    .await;
    let sw = SwitchGb200Nvidia::for_test(&server.uri());

    let error = sw.verify_nvos_ready().await.unwrap_err();

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(
        error.message.contains("health=Not OK"),
        "unexpected message: {}",
        error.message
    );
}

#[tokio::test]
async fn verify_nvos_ready_stops_the_poll_on_a_failed_boot() {
    let server = MockServer::start().await;
    mount_system(
        &server,
        serde_json::json!({ "status": "System is not ready (failed)", "health": {"status": "OK"} }),
    )
    .await;
    let sw = SwitchGb200Nvidia::for_test(&server.uri());

    let error = sw.verify_nvos_ready().await.unwrap_err();

    assert_eq!(error.code, ErrorCode::Aborted);
    assert!(error.message.contains("(failed)"), "{}", error.message);
}

/// NVOS remediates a fatal ASIC state itself, so the poll keeps waiting.
#[tokio::test]
async fn verify_nvos_ready_keeps_polling_on_fatal_health() {
    let server = MockServer::start().await;
    mount_system(
        &server,
        serde_json::json!({ "status": "System is ready", "health": {"status": "FATAL"} }),
    )
    .await;
    let sw = SwitchGb200Nvidia::for_test(&server.uri());

    let error = sw.verify_nvos_ready().await.unwrap_err();

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(error.message.contains("health=FATAL"), "{}", error.message);
}

/// A health word outside the known set fails the decode. Its code is one the
/// readiness poll retries, so a word a later NVOS adds waits out the budget.
#[tokio::test]
async fn verify_nvos_ready_reports_an_unknown_health_word_as_internal() {
    let server = MockServer::start().await;
    mount_system(
        &server,
        serde_json::json!({ "status": "System is ready", "health": {"status": "DEGRADED"} }),
    )
    .await;
    let sw = SwitchGb200Nvidia::for_test(&server.uri());

    let error = sw.verify_nvos_ready().await.unwrap_err();

    assert_eq!(error.code, ErrorCode::Internal);
}

#[tokio::test]
async fn verify_nvos_ready_keeps_transport_error_codes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let sw = SwitchGb200Nvidia::for_test(&server.uri());

    let error = sw.verify_nvos_ready().await.unwrap_err();

    assert_eq!(error.code, ErrorCode::Unavailable);
}

// ── Power state behavior ────────────────────────────────────────

#[tokio::test]
async fn get_power_state_uses_bmc_redfish_when_available() {
    let nvos = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&nvos)
        .await;

    let bmc = MockServer::start().await;
    mount_redfish_system(&bmc, "On").await;

    let sw = SwitchGb200Nvidia::for_test(&nvos.uri()).with_redfish_for_test(&bmc.uri());

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
        .and(path("/redfish/v1"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&bmc)
        .await;

    let sw = SwitchGb200Nvidia::for_test(&nvos.uri()).with_redfish_for_test(&bmc.uri());

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
    assert!(err.message.contains("BMC Redfish power details"));
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
        expected_inventory: None,
        recovery: None,
        runtime_config: None,
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
        expected_inventory: None,
        recovery: None,
        runtime_config: None,
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
        expected_inventory: None,
        recovery: None,
        runtime_config: None,
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
    assert!(err.message.contains("BMC Redfish power details"));
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
        expected_inventory: None,
        recovery: None,
        runtime_config: None,
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

// ── Switch host endpoint selection ───────────────────────────────────────

#[tokio::test]
async fn gb_switches_keep_nvue_inventory_and_power_cycle_without_a_bmc() {
    let host = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/firmware"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "BMC":{"actual-firmware":"88.1"}
        })))
        .expect(2)
        .mount(&host)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(2)
        .mount(&host)
        .await;
    Mock::given(method("POST"))
        .and(path("/nvue_v1/system"))
        .and(body_json(
            serde_json::json!({"@power-cycle":{"state":"start","parameters":{"force":true}}}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(2)
        .mount(&host)
        .await;
    for node_type in [NodeType::SwitchGb200Nvidia, NodeType::SwitchGb300Nvidia] {
        let mut switch = SwitchGb200Nvidia::for_test(&host.uri());
        switch.node_type = node_type;
        switch.bmc_endpoint = None;
        let endpoint = &mut switch.host_endpoint.as_mut().unwrap().endpoint;
        endpoint.ip_address = host.address().ip().to_string();
        endpoint.port = host.address().port();
        assert!(switch.redfish.is_none());
        let inventory = switch.get_firmware_inventory().await.unwrap();
        assert_eq!(inventory[0].version, "88.1");
        assert_eq!(switch.get_power_state().await.unwrap(), PowerState::On);
        switch
            .set_power_state(PowerOp::PowerCycle, PowerTargetType::System)
            .await
            .unwrap();
    }
}

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

#[tokio::test]
async fn bmc_aux_powercycle_uses_nv_redfish_reset_action() {
    let bmc = MockServer::start().await;
    mount_redfish_system(&bmc, "On").await;

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

    let sw = SwitchGb200Nvidia::for_test(&bmc.uri()).with_redfish_for_test(&bmc.uri());
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

#[tokio::test]
async fn is_already_staged_propagates_non_not_found_probe_errors() {
    // A first-upload (remote file absent) or an unreadable remote size map to
    // NotFound and are handled as "not staged" (see staged_from_remote_size
    // unit tests). A genuine failure such as a disconnected session
    // (FailedPrecondition) is different and must be surfaced, not silently
    // treated as "not staged" and followed by an upload attempt.
    let sw = SwitchGb200Nvidia::for_test("http://127.0.0.1:9");
    let ssh = crate::transport::ssh::SshClient::disconnected_for_test("10.0.0.1");

    let dir = tempfile::tempdir().expect("tempdir");
    let local_path = dir.path().join("fw.bin");
    std::fs::write(&local_path, b"firmware payload").expect("write local fw");

    let err = sw
        .is_already_staged(
            &ssh,
            local_path.to_str().expect("utf8 path"),
            "/host/nos-images/fw.bin",
        )
        .await
        .expect_err("a non-NotFound probe error must propagate");
    assert_eq!(
        err.code,
        crate::utilities::error::ErrorCode::FailedPrecondition
    );
}

fn sdn_state_report_ssh_exec(report_filename: &str, report_json: &str) -> SshCommandRecorder {
    let report_filename = report_filename.to_owned();
    let report_json = report_json.to_owned();
    let listing_calls = AtomicUsize::new(0);

    Box::new(move |command: &str| {
        if command.starts_with("nv action generate sdn state apps nmx-controller") {
            Ok(String::new())
        } else if command.contains("ls -t") {
            // The first listing runs before report generation; return no
            // prior report so the post-generation filename always looks
            // fresh, matching the common case in these fixtures.
            if listing_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(String::new())
            } else {
                Ok(report_filename.clone())
            }
        } else if command.starts_with("cat ") {
            Ok(report_json.clone())
        } else {
            panic!("unexpected SSH command in SDN state report test: {command}")
        }
    })
}

/// Uses RFC 5737 TEST-NET-2 addresses and placeholder identifiers rather than
/// any real chassis serial, hostname, or lab address.
fn sdn_state_report_json(second_connection_state: &str) -> String {
    format!(
        r#"{{
            "statusUpdates": [
                {{
                    "nvbdUuid": "test-switch-01-eth1_1",
                    "updateTimestamp": "1000000000000",
                    "connectionStatuses": [
                        {{
                            "nvbsUuid": "test-peer-01",
                            "nvbsAddress": "198.51.100.10:50000",
                            "connectionState": "NMX_NVB_CONNECTION_STATE_CONNECTED",
                            "kaState": "NMX_NVB_KA_STATE_OK",
                            "activeState": "NMX_NVB_ACTIVE_STATE_ACTIVE",
                            "roundtripLatency": 1000,
                            "nvbdAddress": "198.51.100.1:40000"
                        }},
                        {{
                            "nvbsUuid": "test-peer-02",
                            "nvbsAddress": "198.51.100.11:50000",
                            "connectionState": "{second_connection_state}",
                            "kaState": "NMX_NVB_KA_STATE_OK",
                            "activeState": "NMX_NVB_ACTIVE_STATE_ACTIVE",
                            "roundtripLatency": 1000,
                            "nvbdAddress": "198.51.100.1:40001"
                        }}
                    ]
                }}
            ]
        }}"#
    )
}

fn sdn_state_expected_peers(addresses: &[&str]) -> ClusterNodeServerAddresses {
    addresses
        .iter()
        .map(|address| address.parse().expect("valid IP literal in test"))
        .collect()
}

#[tokio::test]
async fn verify_sdn_fabric_state_succeeds_when_every_connection_is_healthy() {
    let report = sdn_state_report_json("NMX_NVB_CONNECTION_STATE_CONNECTED");

    let exec = sdn_state_report_ssh_exec(
        "nmx-controller_nv-bridge-state-report_20260223_154714",
        &report,
    );

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10", "198.51.100.11"]);

    switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect("every reported connection is healthy and every peer is present");
}

#[tokio::test]
async fn verify_sdn_fabric_state_ignores_active_state_when_connection_and_keepalive_are_healthy() {
    let report = r#"{
        "statusUpdates": [
            {
                "nvbdUuid": "test-switch-01-eth1_1",
                "connectionStatuses": [
                    {
                        "nvbsUuid": "test-peer-01",
                        "nvbsAddress": "198.51.100.10:50000",
                        "connectionState": "NMX_NVB_CONNECTION_STATE_CONNECTED",
                        "kaState": "NMX_NVB_KA_STATE_OK",
                        "activeState": "NMX_NVB_ACTIVE_STATE_ACTIVE"
                    },
                    {
                        "nvbsUuid": "test-peer-02",
                        "nvbsAddress": "198.51.100.11:50000",
                        "connectionState": "NMX_NVB_CONNECTION_STATE_CONNECTED",
                        "kaState": "NMX_NVB_KA_STATE_OK",
                        "activeState": "NMX_NVB_ACTIVE_STATE_INACTIVE"
                    },
                    {
                        "nvbsUuid": "test-peer-03",
                        "nvbsAddress": "198.51.100.12:50000",
                        "connectionState": "NMX_NVB_CONNECTION_STATE_CONNECTED",
                        "kaState": "NMX_NVB_KA_STATE_OK"
                    }
                ]
            }
        ]
    }"#;

    let exec = sdn_state_report_ssh_exec(
        "nmx-controller_nv-bridge-state-report_20260223_154714",
        report,
    );

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers =
        sdn_state_expected_peers(&["198.51.100.10", "198.51.100.11", "198.51.100.12"]);

    switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect("ACTIVE, INACTIVE, and omitted active states are role metadata, not health");
}

#[tokio::test]
async fn verify_sdn_fabric_state_fails_when_a_connection_is_unhealthy() {
    let report = sdn_state_report_json("NMX_NVB_CONNECTION_STATE_DISCONNECTED");

    let exec = sdn_state_report_ssh_exec(
        "nmx-controller_nv-bridge-state-report_20260223_154714",
        &report,
    );

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10", "198.51.100.11"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a disconnected fabric connection must fail verification");

    assert!(err.message.contains("unhealthy"), "{}", err.message);
}

#[tokio::test]
async fn verify_sdn_fabric_state_fails_when_a_configured_peer_is_missing_from_the_report() {
    let report = sdn_state_report_json("NMX_NVB_CONNECTION_STATE_CONNECTED");

    let exec = sdn_state_report_ssh_exec(
        "nmx-controller_nv-bridge-state-report_20260223_154714",
        &report,
    );

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);

    let expected_peers =
        sdn_state_expected_peers(&["198.51.100.10", "198.51.100.11", "198.51.100.200"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a configured peer absent from the report must fail verification");

    assert!(err.message.contains("198.51.100.200"), "{}", err.message);
}

#[tokio::test]
async fn verify_sdn_fabric_state_fails_when_report_is_not_valid_json() {
    let exec = sdn_state_report_ssh_exec(
        "nmx-controller_nv-bridge-state-report_20260223_154714",
        "not json",
    );

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a malformed report body must fail verification");

    assert!(
        err.message.contains("failed to parse SDN state report"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn verify_sdn_fabric_state_rejects_a_listing_entry_with_shell_metacharacters() {
    // No "cat " branch: a hostile filename reaching a downstream command
    // panics this test instead of silently succeeding.
    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(
        |command: &str| {
            if command.starts_with("nv action generate sdn state apps nmx-controller") {
                Ok(String::new())
            } else if command.contains("ls -t") {
                Ok("report; rm -rf /".to_owned())
            } else {
                panic!("a hostile listing entry must never reach a downstream command: {command}")
            }
        },
    );

    let expected_peers = sdn_state_expected_peers(&["198.51.100.10"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a listing entry with shell metacharacters must be rejected");

    assert!(
        err.message.contains("no SDN state report found"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn verify_sdn_fabric_state_rejects_a_listing_entry_with_path_traversal_tokens() {
    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(
        |command: &str| {
            if command.starts_with("nv action generate sdn state apps nmx-controller") {
                Ok(String::new())
            } else if command.contains("ls -t") {
                Ok("../report".to_owned())
            } else {
                panic!("a traversal listing entry must never reach a downstream command: {command}")
            }
        },
    );

    let expected_peers = sdn_state_expected_peers(&["198.51.100.10"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a listing entry with path traversal tokens must be rejected");

    assert!(
        err.message.contains("no SDN state report found"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn verify_sdn_fabric_state_fails_when_the_report_does_not_refresh() {
    let exec: SshCommandRecorder = Box::new(|command: &str| {
        if command.starts_with("nv action generate sdn state apps nmx-controller") {
            Ok(String::new())
        } else if command.contains("ls -t") {
            // Same filename before and after generation: NVOS accepted the
            // request but never wrote a new report.
            Ok("nmx-controller_nv-bridge-state-report_20260223_154714".to_owned())
        } else {
            panic!("a stale report must be rejected before being read: {command}")
        }
    });

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("an unchanged report filename must fail verification");

    assert!(err.message.contains("did not refresh"), "{}", err.message);
}

#[tokio::test]
async fn verify_sdn_fabric_state_treats_an_unparseable_peer_address_as_missing() {
    let report = r#"{
        "statusUpdates": [
            {
                "nvbdUuid": "test-switch-01-eth1_1",
                "connectionStatuses": [
                    {
                        "nvbsUuid": "test-peer-01",
                        "nvbsAddress": "198.51.100.10",
                        "connectionState": "NMX_NVB_CONNECTION_STATE_CONNECTED",
                        "kaState": "NMX_NVB_KA_STATE_OK",
                        "activeState": "NMX_NVB_ACTIVE_STATE_ACTIVE"
                    }
                ]
            }
        ]
    }"#;

    let exec = sdn_state_report_ssh_exec(
        "nmx-controller_nv-bridge-state-report_20260223_154714",
        report,
    );

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a peer address that fails to parse must not count as observed");

    assert!(err.message.contains("198.51.100.10"), "{}", err.message);
}

#[tokio::test]
async fn verify_sdn_fabric_state_propagates_a_transport_failure_on_the_baseline_listing() {
    // A channel-open failure on the pre-generation listing must not be
    // treated the same as "no prior report": swallowing it here could let a
    // stale report pass the freshness check if generation is never even
    // attempted. No baseline-listing error is tolerated; the missing-report
    // case is handled by the directory test in the command itself.
    let exec: SshCommandRecorder = Box::new(|command: &str| {
        if command.contains("ls -t") {
            Err(RmsError::new(ErrorCode::Unavailable, "SSH channel closed"))
        } else {
            panic!(
                "generation must not run after the baseline listing fails to transport: {command}"
            )
        }
    });

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a transport failure on the baseline listing must propagate");

    assert_eq!(err.code, ErrorCode::Unavailable);

    assert!(
        err.message.contains("SSH channel closed"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn verify_sdn_fabric_state_guards_the_baseline_listing_against_a_missing_directory() {
    // Confirmed against two independent real VR NVL72 switches (NVOS
    // 25.03.0566 and 25.03.0569): `ls` on a report directory that has never
    // been generated exits with status 2. The baseline listing therefore
    // tests for the directory first so that case stays a successful empty
    // listing instead of an error the code would have to tolerate by class.
    let report = sdn_state_report_json("NMX_NVB_CONNECTION_STATE_CONNECTED");
    let listing_calls = AtomicUsize::new(0);

    let exec: SshCommandRecorder = Box::new(move |command: &str| {
        if command.starts_with("nv action generate sdn state apps nmx-controller") {
            Ok(String::new())
        } else if command.contains("ls -t") {
            if listing_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                assert!(
                    command.contains("if [ -d ") && command.contains("; then ls -t "),
                    "the baseline listing must guard against a missing report directory \
                     rather than let `ls` fail: {command}"
                );

                Ok(String::new())
            } else {
                Ok("nmx-controller_nv-bridge-state-report_20260223_154714".to_owned())
            }
        } else if command.starts_with("cat ") {
            Ok(report.clone())
        } else {
            panic!("unexpected SSH command: {command}")
        }
    });

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10", "198.51.100.11"]);

    switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect("a switch with no prior report must still verify its fabric state");
}

#[tokio::test]
async fn verify_sdn_fabric_state_propagates_a_command_failure_on_the_baseline_listing() {
    // ErrorCode::Internal is not specific enough to tolerate: SshClient::exec
    // reports a command killed by a signal, a command with no exit status,
    // and a failed exec request under the same code as a non-zero `ls`.
    // Clearing previous_filename on one of those would let the freshness
    // check accept the stale report this fixture returns after generation.
    let listing_calls = AtomicUsize::new(0);

    let exec: SshCommandRecorder = Box::new(move |command: &str| {
        if command.contains("ls -t") {
            if listing_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(RmsError::new(
                    ErrorCode::Internal,
                    "SSH command \"...\" on host completed without exit status",
                ))
            } else {
                Ok("nmx-controller_nv-bridge-state-report_20260223_154714".to_owned())
            }
        } else {
            panic!("the baseline listing failure must stop verification: {command}")
        }
    });

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a command failure on the baseline listing must propagate");

    assert_eq!(err.code, ErrorCode::Internal);

    assert!(
        err.message.contains("without exit status"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn verify_sdn_fabric_state_fails_when_a_connection_reports_a_chassis_sn_mismatch() {
    // The mismatch is on the second connection, not the first: this rules
    // out an early-return or indexing bug that only a single-element
    // fixture would miss.
    let report = r#"{
        "statusUpdates": [
            {
                "nvbdUuid": "test-switch-01-eth1_1",
                "connectionStatuses": [
                    {
                        "nvbsUuid": "test-peer-01",
                        "nvbsAddress": "198.51.100.10:50000",
                        "connectionState": "NMX_NVB_CONNECTION_STATE_CONNECTED",
                        "kaState": "NMX_NVB_KA_STATE_OK",
                        "activeState": "NMX_NVB_ACTIVE_STATE_ACTIVE"
                    },
                    {
                        "nvbsUuid": "test-peer-02",
                        "nvbsAddress": "198.51.100.11:50000",
                        "connectionState": "NMX_NVB_CONNECTION_STATE_CONNECTED",
                        "kaState": "NMX_NVB_KA_STATE_OK",
                        "activeState": "NMX_NVB_ACTIVE_STATE_ACTIVE",
                        "chassisSnMismatch": true
                    }
                ]
            }
        ]
    }"#;

    let exec = sdn_state_report_ssh_exec(
        "nmx-controller_nv-bridge-state-report_20260223_154714",
        report,
    );

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10", "198.51.100.11"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("a live connection reporting a chassis serial number mismatch must fail");

    assert!(
        err.message.contains("test-peer-02")
            && err.message.contains("chassis serial number mismatch"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn verify_sdn_fabric_state_fails_when_a_state_key_is_omitted_from_the_report() {
    // Proto3 JSON marshaling omits a field entirely when it holds its
    // zero/default value; this simulates a connection whose connectionState
    // is genuinely NMX_NVB_CONNECTION_STATE_UNKNOWN (0) and therefore
    // absent from the JSON rather than an explicit key.
    let report = r#"{
        "statusUpdates": [
            {
                "nvbdUuid": "test-switch-01-eth1_1",
                "connectionStatuses": [
                    {
                        "nvbsUuid": "test-peer-01",
                        "nvbsAddress": "198.51.100.10:50000",
                        "kaState": "NMX_NVB_KA_STATE_OK",
                        "activeState": "NMX_NVB_ACTIVE_STATE_ACTIVE"
                    }
                ]
            }
        ]
    }"#;

    let exec = sdn_state_report_ssh_exec(
        "nmx-controller_nv-bridge-state-report_20260223_154714",
        report,
    );

    let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:9").with_ssh_exec_for_test(exec);
    let expected_peers = sdn_state_expected_peers(&["198.51.100.10"]);

    let err = switch
        .verify_sdn_fabric_state(&expected_peers)
        .await
        .expect_err("an omitted connectionState key must fail the connection, not the parse");

    assert!(
        err.message.contains("is unhealthy") && err.message.contains("connectionState="),
        "expected a targeted unhealthy-connection error, not a parse failure: {}",
        err.message
    );
}
