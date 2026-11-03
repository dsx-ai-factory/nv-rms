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
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct Fixture {
    server: MockServer,
    target: PowerShelfRFTarget,
    task: Arc<LiteonAllPsuTask>,
    reply: Arc<Mutex<(u16, String)>>,
    psus: Arc<Mutex<Vec<Value>>>,
    collection: Arc<Mutex<(u16, Value)>>,
    _temp: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let server = MockServer::start().await;
        let temp = tempfile::tempdir().unwrap();
        let file = temp
            .path()
            .join("SP_2183_1RD_Security_Bootloader_0707_20260814.D2DAppA.fwpkg");
        tokio::fs::write(&file, b"PSU package").await.unwrap();
        let mut access = BmcAccess::mock_with_base_url(server.uri(), "vr-liteon");
        access.model = "PF-1114-1RD".to_owned();
        let upload_target = PowerShelfRFTarget::new(access, None);
        upload_target.register_liteon_psu_all_task_for_repair(
            "initial",
            file.to_str().unwrap(),
            Instant::now(),
        );
        let task = upload_target.liteon_psu_all_task("initial").unwrap();
        // Match RMS: every status request creates a fresh, model-less target.
        let target =
            PowerShelfRFTarget::new(BmcAccess::mock_with_base_url(server.uri(), "vr-poll"), None);
        let reply = Arc::new(Mutex::new((404, String::new())));
        let task_reply = reply.clone();
        Mock::given(method("GET"))
            .and(path_regex(r"^/redfish/v1/TaskService/Tasks/.*$"))
            .respond_with(move |_: &wiremock::Request| {
                let (status, body) = task_reply.lock().unwrap().clone();
                ResponseTemplate::new(status).set_body_string(body)
            })
            .mount(&server)
            .await;
        let collection = Arc::new(Mutex::new((
            200,
            json!({"Members": [], "Members@odata.count": 0}),
        )));
        let collection_reply = collection.clone();
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks"))
            .respond_with(move |_: &wiremock::Request| {
                let (status, body) = collection_reply.lock().unwrap().clone();
                ResponseTemplate::new(status).set_body_json(body)
            })
            .mount(&server)
            .await;
        let psus = Arc::new(Mutex::new((1..=6).map(|_| json!({
            "Version": "0x6 / 0x6", "state": "Standby", "status": "OK", "updateProgress": 0
        })).collect()));
        Self {
            server,
            target,
            task,
            reply,
            psus,
            collection,
            _temp: temp,
        }
    }

    async fn mount_inventory(&self) {
        let members: Vec<Value> = (1..=6)
            .map(|id| {
                json!({
                    "@odata.id": format!("{LITEON_POWERUNITS_URI}/powerdevice{id}")
                })
            })
            .collect();
        Mock::given(method("GET"))
            .and(path(LITEON_POWERUNITS_URI))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Members": members})))
            .mount(&self.server)
            .await;
        for id in 1..=6 {
            let psus = self.psus.clone();
            Mock::given(method("GET"))
                .and(path(format!("{LITEON_POWERUNITS_URI}/powerdevice{id}")))
                .respond_with(move |_: &wiremock::Request| {
                    ResponseTemplate::new(200).set_body_json(psus.lock().unwrap()[id - 1].clone())
                })
                .mount(&self.server)
                .await;
        }
    }

    async fn mount_upload(&self, status: u16, response: Value, count: u64) {
        Mock::given(method("GET"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"HttpPushUri": "/redfish/v1/UpdateService/advertised"})),
            )
            .mount(&self.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/advertised"))
            .respond_with(ResponseTemplate::new(status).set_body_json(response))
            .expect(count)
            .mount(&self.server)
            .await;
        Mock::given(method("POST"))
            .and(path(LITEON_PSU_UPDATE_URI))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&self.server)
            .await;
    }

    async fn poll(&self) -> (bool, Value) {
        self.target.query_job_status("initial", None).await
    }

    /// Age the idle observation so the next validation may re-push.
    async fn settle(&self) {
        self.task.state.lock().await.idle_since = Some(Instant::now() - REPUSH_SETTLE_TIME);
    }

    async fn expire_deadline(&self) {
        self.task.state.lock().await.deadline = Instant::now();
    }

    async fn assert_no_uploads(&self) {
        assert!(self
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method != wiremock::http::Method::Post));
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.target.clear_liteon_psu_all_task_for_repair("initial");
    }
}

#[tokio::test]
async fn malformed_task_reads_fail_without_upload() {
    for (status, body) in [
        (400, "bad request"),
        (200, "not JSON"),
        (200, "[]"),
        (200, "{}"),
    ] {
        let f = Fixture::new().await;
        *f.reply.lock().unwrap() = (status, body.to_owned());
        let (ok, result) = f.poll().await;
        assert!(ok);
        assert_eq!(result["TaskState"], "Exception");
        assert_eq!(f.poll().await.1, result);
        f.assert_no_uploads().await;
    }
}

#[tokio::test]
async fn authentication_rejections_and_server_errors_are_outages_not_failures() {
    // A restarting PMC answered valid credentials with HTTP 401 for ~13 minutes.
    for status in [401, 403, 408, 429, 502, 503] {
        let f = Fixture::new().await;
        *f.reply.lock().unwrap() = (status, String::new());
        let (ok, result) = f.poll().await;
        assert!(ok, "HTTP {status}");
        assert_eq!(result["TaskState"], "Running", "HTTP {status}");
        assert!(f.task.state.lock().await.offline_since.is_some());
        assert!(f.task.state.lock().await.terminal.is_none());
        f.assert_no_uploads().await;
    }
}

#[tokio::test]
async fn outage_budget_covers_a_thirteen_minute_pmc_restart() {
    let f = Fixture::new().await;
    *f.reply.lock().unwrap() = (401, String::new());
    f.poll().await;
    f.task.state.lock().await.offline_since = Some(Instant::now() - Duration::from_secs(13 * 60));
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    *f.reply.lock().unwrap() = (404, String::new());
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    assert!(f.task.state.lock().await.offline_since.is_none());
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn offline_pmc_gets_a_bounded_budget_then_terminal_failure() {
    let f = Fixture::new().await;
    *f.reply.lock().unwrap() = (503, "{}".to_owned());
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    let since = f.task.state.lock().await.offline_since.unwrap();
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    assert_eq!(f.task.state.lock().await.offline_since, Some(since));
    f.task.state.lock().await.offline_since = Some(Instant::now() - PMC_OFFLINE_TIMEOUT);
    let (_, result) = f.poll().await;
    assert_eq!(result["TaskState"], "Exception");
    assert!(result.to_string().contains("did not recover"));
    assert_eq!(f.poll().await.1, result);
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn transport_failure_uses_outage_budget_without_uploading() {
    let f = Fixture::new().await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let target = PowerShelfRFTarget::new(
        BmcAccess::mock_with_base_url(format!("http://{address}"), "unreachable"),
        None,
    );
    let (ok, result) = target
        .poll_liteon_all_psu_recovery("initial", &f.task)
        .await;
    assert!(ok);
    assert_eq!(result["TaskState"], "Running");
    assert!(f.task.state.lock().await.offline_since.is_some());
    assert_eq!(f.task.state.lock().await.attempts, 0);
}

#[tokio::test]
async fn recovered_pmc_missing_tasks_idle_incomplete_psus_repush_and_follow_new_task() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 1).await;
    let deadline = f.task.state.lock().await.deadline;
    *f.reply.lock().unwrap() = (503, "{}".to_owned());
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    *f.reply.lock().unwrap() = (404, String::new());
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    assert!(f.task.state.lock().await.offline_since.is_none());
    f.settle().await;
    let (_, result) = f.poll().await;
    assert_eq!(result["repush_attempts"], 1);
    assert_eq!(result["monitored_task_id"], "replacement");
    assert_eq!(f.task.state.lock().await.deadline, deadline);
    *f.reply.lock().unwrap() = (
        200,
        json!({"TaskState": "Running", "PercentComplete": 40}).to_string(),
    );
    assert_eq!(f.poll().await.1["PercentComplete"], 40);
    assert_eq!(
        f.server
            .received_requests()
            .await
            .unwrap()
            .last()
            .unwrap()
            .url
            .path(),
        "/redfish/v1/TaskService/Tasks/replacement"
    );
    *f.reply.lock().unwrap() = (200, json!({"TaskState": "Completed"}).to_string());
    for psu in f.psus.lock().unwrap().iter_mut() {
        psu["Version"] = json!("0x7 / 0x7");
    }
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    assert_eq!(f.poll().await.1["TaskState"], "Completed");
}

#[tokio::test]
async fn missing_tasks_with_all_psus_at_target_completes_without_repush() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    for psu in f.psus.lock().unwrap().iter_mut() {
        psu["Version"] = json!("0x7 / 0x7");
    }
    f.poll().await;
    let (_, result) = f.poll().await;
    assert_eq!(result["TaskState"], "Completed");
    assert_eq!(f.poll().await.1, result);
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn active_psus_wait_until_idle_without_a_five_minute_flash_limit() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 1).await;
    f.psus.lock().unwrap()[0]["updateProgress"] = json!(40);
    for _ in 0..4 {
        assert_eq!(f.poll().await.1["TaskState"], "Running");
    }
    f.assert_no_uploads().await;
    f.psus.lock().unwrap()[0]["updateProgress"] = json!(0);
    f.poll().await;
    f.settle().await;
    assert_eq!(f.poll().await.1["repush_attempts"], 1);
}

#[tokio::test]
async fn repushes_are_limited_by_one_hour_not_count_or_version_progress() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 10).await;
    let deadline = f.task.state.lock().await.deadline;
    for attempt in 1..=10 {
        f.poll().await;
        f.settle().await;
        let (_, result) = f.poll().await;
        assert_eq!(result["TaskState"], "Running");
        assert_eq!(result["repush_attempts"], attempt);
        assert_eq!(f.task.state.lock().await.deadline, deadline);
    }
    f.expire_deadline().await;
    let (_, result) = f.poll().await;
    assert_eq!(result["TaskState"], "Exception");
    assert!(result.to_string().contains("one-hour"));
    assert_eq!(f.poll().await.1, result);
}

#[tokio::test]
async fn deadline_applies_to_active_task_and_psus_and_offline_pmc() {
    for phase in [Phase::PollTask, Phase::Validate] {
        let f = Fixture::new().await;
        f.task.state.lock().await.phase = phase;
        f.expire_deadline().await;
        assert_eq!(f.poll().await.1["TaskState"], "Exception");
        assert!(f.server.received_requests().await.unwrap().is_empty());
    }
}

async fn mount_listed_task(f: &Fixture, id: &str, body: Value) {
    Mock::given(method("GET"))
        .and(path(format!("/redfish/v1/TaskService/Tasks/{id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .with_priority(1)
        .mount(&f.server)
        .await;
}

#[tokio::test]
async fn retained_finished_tasks_do_not_block_a_repush() {
    // The PMC keeps finished tasks until it restarts: Tasks/0 (the PMC update)
    // is still listed while the PSU task is gone.
    for listing in [
        json!({"Members": [{"@odata.id": "/redfish/v1/TaskService/Tasks/0"}]}),
        json!({"Members": [{"@odata.id": "/redfish/v1/TaskService/Tasks/0", "TaskState": "Completed"}]}),
    ] {
        let f = Fixture::new().await;
        mount_listed_task(&f, "0", json!({"TaskState": "Completed"})).await;
        f.mount_inventory().await;
        f.mount_upload(202, json!({"Id": "replacement"}), 1).await;
        *f.collection.lock().unwrap() = (200, listing);
        f.poll().await;
        f.poll().await;
        f.settle().await;
        f.poll().await;
        let (_, result) = f.poll().await;
        assert_eq!(result["repush_attempts"], 1, "{result}");
        assert_eq!(result["monitored_task_id"], "replacement");
    }
}

#[tokio::test]
async fn unfinished_listed_task_blocks_and_is_named_when_the_deadline_passes() {
    let f = Fixture::new().await;
    mount_listed_task(&f, "7", json!({"TaskState": "Running"})).await;
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 0).await;
    *f.collection.lock().unwrap() = (
        200,
        json!({"Members": [
            {"@odata.id": "/redfish/v1/TaskService/Tasks/6", "TaskState": "Completed"},
            {"@odata.id": "/redfish/v1/TaskService/Tasks/7"}
        ]}),
    );
    let (_, result) = f.poll().await;
    assert_eq!(result["TaskState"], "Running");
    assert_eq!(result["blocked_by"], "7");
    f.expire_deadline().await;
    let (_, failure) = f.poll().await;
    assert_eq!(failure["TaskState"], "Exception");
    assert!(
        failure.to_string().contains("PMC task 7 was still active"),
        "{failure}"
    );
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn collection_must_be_confirmed_empty_including_pagination() {
    for body in [
        json!({"Members": [{"@odata.id": "/redfish/v1/TaskService/Tasks/other"}]}),
        json!({"Members": [], "Members@odata.count": 1}),
        json!({"Members": [], "Members@odata.nextLink": "/next"}),
    ] {
        let f = Fixture::new().await;
        mount_listed_task(&f, "other", json!({"TaskState": "Running"})).await;
        *f.collection.lock().unwrap() = (200, body);
        for _ in 0..3 {
            assert_eq!(f.poll().await.1["TaskState"], "Running");
        }
        f.assert_no_uploads().await;
    }
    let f = Fixture::new().await;
    *f.collection.lock().unwrap() = (200, json!({}));
    assert_eq!(f.poll().await.1["TaskState"], "Exception");
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn task_appearing_after_inventory_validation_blocks_repush() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 0).await;
    f.poll().await;
    f.settle().await;
    *f.collection.lock().unwrap() = (200, json!({"Members": [{"Id": "new-task"}]}));
    assert_eq!(f.poll().await.1["repush_attempts"], 0);
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn unreadable_psu_fields_wait_within_the_outage_budget_without_uploading() {
    for (field, value) in [
        ("Version", json!(null)),
        ("updateProgress", json!(null)),
        ("state", json!("")),
        ("status", json!("")),
    ] {
        let f = Fixture::new().await;
        f.mount_inventory().await;
        f.psus.lock().unwrap()[0][field] = value;
        f.poll().await;
        let (_, result) = f.poll().await;
        assert_eq!(result["TaskState"], "Running", "{field}");
        assert!(f.task.state.lock().await.offline_since.is_some(), "{field}");
        f.settle().await;
        assert_eq!(f.poll().await.1["TaskState"], "Running", "{field}");
        f.assert_no_uploads().await;

        // The budget is the bound: an unreadable PSU cannot wait forever.
        f.task.state.lock().await.offline_since = Some(Instant::now() - PMC_OFFLINE_TIMEOUT);
        assert_eq!(f.poll().await.1["TaskState"], "Exception", "{field}");
        f.assert_no_uploads().await;
    }
}

#[tokio::test]
async fn psu_that_becomes_readable_resumes_normal_recovery() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 1).await;
    let original = f.psus.lock().unwrap()[2].clone();
    f.psus.lock().unwrap()[2]["Version"] = json!(null);
    f.poll().await;
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    assert!(f.task.state.lock().await.offline_since.is_some());

    f.psus.lock().unwrap()[2] = original;
    f.poll().await;
    assert!(f.task.state.lock().await.offline_since.is_none());
    f.poll().await;
    f.settle().await;
    assert_eq!(f.poll().await.1["repush_attempts"], 1);
}

#[tokio::test]
async fn failed_psu_status_and_partial_inventory_never_authorize_repush() {
    for field in ["state", "status"] {
        let f = Fixture::new().await;
        f.mount_inventory().await;
        f.psus.lock().unwrap()[0][field] = json!("Critical");
        f.poll().await;
        assert_eq!(f.poll().await.1["TaskState"], "Exception", "{field}");
        f.assert_no_uploads().await;
    }
    let f = Fixture::new().await;
    Mock::given(method("GET"))
        .and(path(LITEON_POWERUNITS_URI))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Members": []})))
        .mount(&f.server)
        .await;
    f.poll().await;
    assert_eq!(f.poll().await.1["TaskState"], "Exception");
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn outage_during_validation_waits_and_then_retries_after_recovery() {
    let f = Fixture::new().await;
    f.poll().await;
    let guard = Mock::given(method("GET"))
        .and(path(LITEON_POWERUNITS_URI))
        .respond_with(ResponseTemplate::new(503))
        .mount_as_scoped(&f.server)
        .await;
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    assert!(f.task.state.lock().await.offline_since.is_some());
    f.assert_no_uploads().await;
    drop(guard);
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 1).await;
    // The outage restarts the settling period.
    assert_eq!(f.poll().await.1["repush_attempts"], 0);
    assert!(f.task.state.lock().await.offline_since.is_none());
    f.poll().await;
    f.settle().await;
    assert_eq!(f.poll().await.1["repush_attempts"], 1);
}

#[tokio::test]
async fn ambiguous_upload_is_never_replayed() {
    for (status, body) in [(503, json!({})), (202, json!({}))] {
        let f = Fixture::new().await;
        f.mount_inventory().await;
        f.mount_upload(status, body, 1).await;
        f.poll().await;
        f.settle().await;
        let (_, result) = f.poll().await;
        assert_eq!(result["TaskState"], "Exception");
        assert_eq!(result["repush_attempts"], 1);
        assert_eq!(f.poll().await.1, result);
    }
}

#[tokio::test]
async fn concurrent_poll_cannot_advance_the_same_recovery() {
    let f = Fixture::new().await;
    let _guard = f.task.state.lock().await;
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    assert!(f.server.received_requests().await.unwrap().is_empty());
}

async fn mount_shelf_identity(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Members": [
            {"@odata.id": "/redfish/v1/Chassis/Manager"},
            {"@odata.id": "/redfish/v1/Chassis/PSU1"},
            {"@odata.id": "/redfish/v1/Chassis/PowerShelf_0"}
        ]})))
        .mount(server)
        .await;
    for (id, model, count) in [
        ("Manager", "AST2600", 0),
        ("PSU1", "SP-2183-1RD", 0),
        ("PowerShelf_0", "PF-1114-1RD", 1),
    ] {
        Mock::given(method("GET"))
            .and(path(format!("/redfish/v1/Chassis/{id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Model": model})))
            .expect(count)
            .mount(server)
            .await;
    }
}

#[tokio::test]
async fn fresh_inventory_resolves_shelf_after_manager_and_psu_chassis() {
    let fixture = Fixture::new().await;
    mount_shelf_identity(&fixture.server).await;
    fixture.mount_inventory().await;
    let uri = "/redfish/v1/UpdateService/FirmwareInventory/PMC";
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"Members": [{"@odata.id": uri}]})),
        )
        .mount(&fixture.server)
        .await;
    Mock::given(method("GET"))
        .and(path(uri))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Version": "1.0.0"})))
        .mount(&fixture.server)
        .await;
    let (ok, errors, inventory) = fixture
        .target
        .get_firmware_inventory(crate::util::TraceFlags::default(), None, Some(""))
        .await;
    assert!(ok);
    assert_eq!(errors, 0);
    assert_eq!(inventory.len(), 7);
    assert_eq!(
        inventory[&format!("{LITEON_POWERUNITS_URI}/powerdevice1")]["Version"],
        "0x6 / 0x6"
    );
}

#[tokio::test]
async fn fresh_update_captures_shelf_identity_for_model_less_status_polls() {
    let mut fixture = Fixture::new().await;
    mount_shelf_identity(&fixture.server).await;
    fixture
        .mount_upload(202, json!({"Id": "new-upload"}), 1)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&fixture.server)
        .await;
    let args = CmdArgs {
        cmd: "update_fw".to_owned(),
        background: false,
        details: false,
        staged_update: false,
        staged_activate_update: false,
        quiet: true,
        special: None,
        oem_parameters: None,
    };
    let before_upload = Instant::now();
    let id = fixture
        .target
        .update_component(
            &args,
            "/redfish/v1/UpdateService/advertised",
            &fixture.task.update_file,
            30,
            None,
            true,
        )
        .await;
    assert_eq!(id.as_deref(), Some("new-upload"));
    let registered = fixture.target.liteon_psu_all_task("new-upload").unwrap();
    let deadline = registered.state.lock().await.deadline;
    assert!(deadline >= before_upload + LITEON_PSU_UPDATE_TIMEOUT);
    assert!(deadline <= Instant::now() + LITEON_PSU_UPDATE_TIMEOUT);
    assert_eq!(
        fixture
            .target
            .liteon_psu_all_task("new-upload")
            .unwrap()
            .model,
        "PF-1114-1RD"
    );
    let fresh = PowerShelfRFTarget::new(
        BmcAccess::mock_with_base_url(fixture.server.uri(), "fresh"),
        None,
    );
    assert_eq!(
        fresh.query_job_status("new-upload", None).await.1["TaskState"],
        "Running"
    );
    fixture
        .target
        .clear_liteon_psu_all_task_for_repair("new-upload");
}

#[tokio::test]
async fn model_lookup_rejects_missing_or_ambiguous_shelf_identity() {
    for members in [
        json!([]),
        json!([{"@odata.id": "/redfish/v1/Chassis/PSU1"}]),
        json!([{"@odata.id": "/redfish/v1/Chassis/powershelf"}, {"@odata.id": "/redfish/v1/Chassis/PowerShelf_0"}]),
    ] {
        let fixture = Fixture::new().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Members": members})))
            .mount(&fixture.server)
            .await;
        assert!(fixture.target.resolve_shelf_model().await.is_none());
        fixture.assert_no_uploads().await;
    }
}

fn psu_update_args() -> CmdArgs {
    CmdArgs {
        cmd: "update_fw".to_owned(),
        background: false,
        details: false,
        staged_update: false,
        staged_activate_update: false,
        quiet: true,
        special: None,
        oem_parameters: None,
    }
}

#[tokio::test]
async fn single_powershelf_chassis_resolves_the_shelf_model() {
    let fixture = Fixture::new().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Members": [{"@odata.id": "/redfish/v1/Chassis/powershelf"}],
            "Members@odata.count": 1
        })))
        .mount(&fixture.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/powershelf"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"Id": "powershelf", "Model": "PF-1333-7R", "PartNumber": "PF-1333-7R"}),
        ))
        .mount(&fixture.server)
        .await;
    assert_eq!(
        fixture.target.resolve_shelf_model().await.as_deref(),
        Some("PF-1333-7R")
    );
}

#[tokio::test]
async fn transient_chassis_read_failures_are_retried_and_persistent_ones_fail() {
    let shelf = json!({"Members": [{"@odata.id": "/redfish/v1/Chassis/powershelf"}]});
    let model = json!({"Id": "powershelf", "Model": "PF-1333-7R"});

    // The collection read fails once, the shelf read fails once, then both work.
    let f = Fixture::new().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&f.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(200).set_body_json(shelf.clone()))
        .mount(&f.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/powershelf"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&f.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/powershelf"))
        .respond_with(ResponseTemplate::new(200).set_body_json(model))
        .mount(&f.server)
        .await;
    assert_eq!(
        f.target.resolve_shelf_model().await.as_deref(),
        Some("PF-1333-7R")
    );

    // A read that keeps failing is attempted a bounded number of times.
    let f = Fixture::new().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(503))
        .expect(u64::from(SHELF_MODEL_ATTEMPTS))
        .mount(&f.server)
        .await;
    assert!(f.target.resolve_shelf_model().await.is_none());

    // A definitive answer (two shelves) is not retried.
    let f = Fixture::new().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Members": [
            {"@odata.id": "/redfish/v1/Chassis/powershelf"},
            {"@odata.id": "/redfish/v1/Chassis/powershelf2"}
        ]})))
        .expect(1)
        .mount(&f.server)
        .await;
    assert!(f.target.resolve_shelf_model().await.is_none());
}

#[tokio::test]
async fn unresolvable_shelf_identity_fails_a_psu_update_before_any_upload() {
    let shelf_uri = json!({"Members": [{"@odata.id": "/redfish/v1/Chassis/powershelf"}]});
    let two_shelves = json!({"Members": [
        {"@odata.id": "/redfish/v1/Chassis/powershelf"},
        {"@odata.id": "/redfish/v1/Chassis/powershelf2"}
    ]});
    // (chassis collection reply, shelf reply)
    let cases = [
        ((500, json!({})), None),
        ((200, json!({"Members": []})), None),
        ((200, two_shelves), None),
        ((200, shelf_uri.clone()), Some((500, json!({})))),
        ((200, shelf_uri.clone()), Some((200, json!({"Model": ""})))),
        ((200, shelf_uri), Some((200, json!({"Id": "powershelf"})))),
    ];
    for ((status, collection), shelf) in cases {
        let mut fixture = Fixture::new().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(status).set_body_json(collection))
            .mount(&fixture.server)
            .await;
        if let Some((status, body)) = shelf {
            Mock::given(method("GET"))
                .and(path("/redfish/v1/Chassis/powershelf"))
                .respond_with(ResponseTemplate::new(status).set_body_json(body))
                .mount(&fixture.server)
                .await;
        }
        let mut json_dict = json!({});
        let id = fixture
            .target
            .update_component(
                &psu_update_args(),
                "/redfish/v1/UpdateService/advertised",
                &fixture.task.update_file,
                30,
                Some(&mut json_dict),
                true,
            )
            .await;
        assert!(id.is_none());
        assert!(json_dict.to_string().contains("Cannot identify"));
        fixture.assert_no_uploads().await;
    }
}

#[tokio::test]
async fn update_component_discards_retained_recovery_entries_for_the_shelf() {
    let mut fixture = Fixture::new().await;
    mount_shelf_identity(&fixture.server).await;
    fixture.mount_upload(202, json!({"Id": "0"}), 1).await;
    Mock::given(method("PATCH"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&fixture.server)
        .await;
    // A finished earlier PSU job is retained under the task ID that a later
    // upload will receive again after a PMC restart.
    fixture.target.register_liteon_psu_all_task_for_repair(
        "0",
        &fixture.task.update_file,
        Instant::now(),
    );
    let stale = fixture.target.liteon_psu_all_task("0").unwrap();
    stale.state.lock().await.terminal = Some(json!({"TaskState": "Completed"}));
    assert!(fixture.target.liteon_psu_all_task("initial").is_some());

    let id = fixture
        .target
        .update_component(
            &psu_update_args(),
            "/redfish/v1/UpdateService/advertised",
            &fixture.task.update_file,
            30,
            None,
            true,
        )
        .await;
    assert_eq!(id.as_deref(), Some("0"));
    // The earlier entries are gone; "0" is the freshly registered upload.
    assert!(fixture.target.liteon_psu_all_task("initial").is_none());
    let current = fixture.target.liteon_psu_all_task("0").unwrap();
    assert!(!Arc::ptr_eq(&current, &stale));
    assert!(current.state.lock().await.terminal.is_none());
    fixture.target.clear_liteon_psu_all_task_for_repair("0");
}

#[tokio::test]
async fn interrupted_upload_is_not_replayed_on_the_next_poll() {
    let fixture = Fixture::new().await;
    fixture.mount_inventory().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"HttpPushUri": "/upload"})))
        .mount(&fixture.server)
        .await;
    let upload_started = Arc::new(tokio::sync::Notify::new());
    let notify = upload_started.clone();
    Mock::given(method("POST"))
        .and(path("/upload"))
        .respond_with(move |_: &wiremock::Request| {
            notify.notify_one();
            ResponseTemplate::new(202)
                .set_body_json(json!({"Id": "replacement"}))
                .set_delay(Duration::from_secs(10))
        })
        .expect(1)
        .mount(&fixture.server)
        .await;
    fixture.poll().await;
    fixture.settle().await;
    tokio::select! {
        _ = upload_started.notified() => {},
        result = fixture.poll() => panic!("upload unexpectedly finished: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("upload did not start"),
    }
    let (_, failure) = fixture.poll().await;
    assert_eq!(failure["TaskState"], "Exception");
    assert_eq!(failure["repush_attempts"], 1);
    assert_eq!(fixture.poll().await.1, failure);
}

#[tokio::test]
async fn slow_read_is_cut_off_by_remaining_job_deadline() {
    let f = Fixture::new().await;
    let _guard = Mock::given(method("GET"))
        .and(path("/redfish/v1/TaskService/Tasks/initial"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"TaskState": "Running"}))
                .set_delay(Duration::from_secs(2)),
        )
        .with_priority(1)
        .mount_as_scoped(&f.server)
        .await;
    f.task.state.lock().await.deadline = Instant::now() + Duration::from_millis(50);
    let (_, result) = f.poll().await;
    assert_eq!(result["TaskState"], "Exception");
    assert!(result.to_string().contains("one-hour"));
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn recovery_does_not_extend_job_deadline_and_new_outage_gets_its_own_budget() {
    let f = Fixture::new().await;
    let deadline = f.task.state.lock().await.deadline;
    *f.reply.lock().unwrap() = (503, "{}".to_owned());
    f.poll().await;
    f.task.state.lock().await.offline_since = Some(Instant::now() - Duration::from_secs(240));
    *f.reply.lock().unwrap() = (200, json!({"TaskState": "Running"}).to_string());
    f.poll().await;
    assert!(f.task.state.lock().await.offline_since.is_none());
    *f.reply.lock().unwrap() = (503, "{}".to_owned());
    f.poll().await;
    assert!(f.task.state.lock().await.offline_since.unwrap().elapsed() < Duration::from_secs(10));
    assert_eq!(f.task.state.lock().await.deadline, deadline);
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn outage_in_update_service_is_not_reset_by_readable_inventory() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&f.server)
        .await;
    f.poll().await;
    f.poll().await;
    let since = Instant::now() - Duration::from_secs(240);
    f.task.state.lock().await.offline_since = Some(since);
    assert_eq!(f.poll().await.1["TaskState"], "Running");
    assert_eq!(f.task.state.lock().await.offline_since, Some(since));
    f.task.state.lock().await.offline_since = Some(Instant::now() - PMC_OFFLINE_TIMEOUT);
    assert_eq!(f.poll().await.1["TaskState"], "Exception");
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn repush_waits_for_idle_incomplete_state_to_settle() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 0).await;
    f.poll().await;
    for _ in 0..3 {
        assert_eq!(f.poll().await.1["repush_attempts"], 0);
        assert_eq!(f.poll().await.1["TaskState"], "Running");
    }
    f.assert_no_uploads().await;
    let first = f.task.state.lock().await.idle_since.unwrap();
    f.poll().await;
    assert_eq!(f.task.state.lock().await.idle_since, Some(first));

    // Activity, an outage or another task each restart the settling period.
    f.psus.lock().unwrap()[0]["updateProgress"] = json!(10);
    f.poll().await;
    f.poll().await;
    assert!(f.task.state.lock().await.idle_since.is_none());
    f.psus.lock().unwrap()[0]["updateProgress"] = json!(0);
    f.poll().await;
    f.poll().await;
    assert!(f.task.state.lock().await.idle_since.is_some());
    *f.reply.lock().unwrap() = (401, String::new());
    f.poll().await;
    assert!(f.task.state.lock().await.idle_since.is_none());
    f.assert_no_uploads().await;
}

#[tokio::test]
async fn psu_version_change_while_idle_restarts_the_settle_period() {
    let f = Fixture::new().await;
    f.mount_inventory().await;
    f.mount_upload(202, json!({"Id": "replacement"}), 0).await;
    f.poll().await;
    f.poll().await;
    f.settle().await;
    // Every PSU is idle on the old version and the clock has fully aged. One
    // PSU now reports a different version: a flash just landed and the idle
    // report may be stale, so the next read must not upload.
    f.psus.lock().unwrap()[0]["Version"] = json!("0x6 / 0x7");
    f.poll().await; // back to validation
    let (_, result) = f.poll().await;
    assert_eq!(result["TaskState"], "Running");
    assert_eq!(result["repush_attempts"], 0);
    // The clock restarted at this read instead of staying aged.
    let restarted = f.task.state.lock().await.idle_since.unwrap();
    assert!(restarted.elapsed() < REPUSH_SETTLE_TIME);
    f.assert_no_uploads().await;

    // Unchanged reads keep waiting out the fresh period.
    f.poll().await;
    f.poll().await;
    assert_eq!(f.task.state.lock().await.idle_since, Some(restarted));
    f.assert_no_uploads().await;
}

#[test]
fn version_comparison_ignores_the_first_read_and_whitespace() {
    let read = |versions: &[&str]| {
        json!({"devices": versions.iter().enumerate().map(|(index, version)| json!({
            "device_id": (index + 1).to_string(), "version": version
        })).collect::<Vec<_>>()})
    };
    let old = read(&["0x6 / 0x6"; 6]);
    assert!(!psu_versions_changed(&json!({}), &old));
    assert!(!psu_versions_changed(&old, &old));
    assert!(!psu_versions_changed(&old, &read(&[" 0x6 / 0x6 "; 6])));
    let mut flipped = ["0x6 / 0x6"; 6];
    flipped[5] = "0x7 / 0x7";
    assert!(psu_versions_changed(&old, &read(&flipped)));
}

#[tokio::test]
async fn repush_upload_gets_its_own_budget_after_slow_reads() {
    let f = Fixture::new().await;
    let delay = Duration::from_millis(100);
    let members: Vec<Value> = (1..=6)
        .map(|id| json!({"@odata.id": format!("{LITEON_POWERUNITS_URI}/powerdevice{id}")}))
        .collect();
    // The nine reads of the re-push step take about 0.9s of the 2s test step
    // budget; the upload takes 1.5s. Together they exceed the step budget, but
    // the upload alone fits in its own.
    let slow = |body: Value| {
        ResponseTemplate::new(200)
            .set_body_json(body)
            .set_delay(delay)
    };
    for (uri, body) in [
        (
            LITEON_POWERUNITS_URI.to_owned(),
            json!({"Members": members}),
        ),
        (
            "/redfish/v1/UpdateService".to_owned(),
            json!({"HttpPushUri": "/redfish/v1/UpdateService/advertised"}),
        ),
        (
            "/redfish/v1/TaskService/Tasks".to_owned(),
            json!({"Members": [], "Members@odata.count": 0}),
        ),
    ] {
        Mock::given(method("GET"))
            .and(path(uri))
            .respond_with(slow(body))
            .with_priority(1)
            .mount(&f.server)
            .await;
    }
    for id in 1..=6 {
        let body = f.psus.lock().unwrap()[id - 1].clone();
        Mock::given(method("GET"))
            .and(path(format!("{LITEON_POWERUNITS_URI}/powerdevice{id}")))
            .respond_with(slow(body))
            .with_priority(1)
            .mount(&f.server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/redfish/v1/UpdateService/advertised"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(json!({"Id": "replacement"}))
                .set_delay(Duration::from_millis(1500)),
        )
        .expect(1)
        .mount(&f.server)
        .await;
    f.poll().await;
    f.poll().await;
    f.settle().await;
    f.poll().await;
    let (_, result) = f.poll().await;
    assert_eq!(result["TaskState"], "Running", "{result}");
    assert_eq!(result["repush_attempts"], 1);
    assert_eq!(result["monitored_task_id"], "replacement");
}

#[tokio::test]
async fn new_upload_discards_all_retained_recovery_entries_for_that_shelf() {
    let f = Fixture::new().await;
    let other = PowerShelfRFTarget::new(
        BmcAccess::mock_with_base_url("http://other-shelf.invalid", "other"),
        None,
    );
    f.target
        .register_liteon_psu_all_task_for_repair("0", "/tmp/a.fwpkg", Instant::now());
    f.target
        .register_liteon_psu_all_task_for_repair("1", "/tmp/b.fwpkg", Instant::now());
    other.register_liteon_psu_all_task_for_repair("0", "/tmp/c.fwpkg", Instant::now());
    assert!(f.target.liteon_psu_all_task("initial").is_some());

    f.target.clear_liteon_psu_all_tasks_for_shelf();

    for id in ["initial", "0", "1"] {
        assert!(f.target.liteon_psu_all_task(id).is_none(), "{id}");
    }
    assert!(other.liteon_psu_all_task("0").is_some());
    other.clear_liteon_psu_all_task_for_repair("0");
}

#[tokio::test]
async fn annotate_tags_objects_and_tolerates_non_objects() {
    let f = Fixture::new().await;
    let mut state = f.task.state.lock().await;
    state.task_id = "7".to_owned();
    state.attempts = 2;

    let tagged = state.annotate("initial", json!({"TaskState": "Running"}));
    assert_eq!(tagged["Id"], "initial");
    assert_eq!(tagged["monitored_task_id"], "7");
    assert_eq!(tagged["repush_attempts"], 2);
    assert_eq!(tagged["TaskState"], "Running");

    for value in [json!(null), json!([1]), json!("text")] {
        let untouched = state.annotate("initial", value.clone());
        if value.is_null() {
            assert!(untouched.is_null());
        } else {
            assert_eq!(untouched, value);
        }
    }
}
