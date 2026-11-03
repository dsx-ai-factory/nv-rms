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

//! Exercises the runner against a stubbed `nvssvt_server.py`, so these tests
//! are hermetic and fast: no NVSSVT install, no DUT, no subprocess.

use std::time::Duration;

use nvssvt::runner::{RunState, StartScanFields};
use nvssvt::{NvssvtConfig, NvssvtError, NvssvtRunner};
use serde_json::json;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn runner_for(server: &MockServer) -> NvssvtRunner {
    let config = NvssvtConfig::default().with_base_url(server.uri());
    NvssvtRunner::new(config).expect("build runner")
}

#[tokio::test]
async fn launch_posts_the_scan_fields_and_returns_the_run_handle() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/runs"))
        .and(body_partial_json(json!({
            "run_id": "rms-job-7",
            "bmc_ip": "10.0.0.5",
            "bmc_user": "admin",
            "quallevel": ["L1"],
            "config_override": "power_limit=500",
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "run_id": "rms-job-7",
            "pid": 4242,
            "log_dir": "/opt/nvssvt/api_runs/rms-job-7",
            "started_at": "2026-09-23T10:00:00+00:00",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let outcome = runner_for(&server)
        .launch(
            Some("rms-job-7"),
            StartScanFields {
                bmc_ip: Some("10.0.0.5".to_string()),
                bmc_user: Some("admin".to_string()),
                quallevel: vec!["L1".to_string()],
                config_override: Some("power_limit=500".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("launch should succeed");

    assert_eq!(outcome.run_id, "rms-job-7");
    assert_eq!(outcome.pid, 4242);
    assert_eq!(outcome.log_dir, "/opt/nvssvt/api_runs/rms-job-7");
    assert_eq!(outcome.started_at, "2026-09-23T10:00:00+00:00");
}

#[tokio::test]
async fn the_launcher_may_assign_its_own_run_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/runs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "run_id": "generated99",
            "pid": 4242,
            "log_dir": "/opt/nvssvt/api_runs/generated99",
            "started_at": "2026-09-23T10:00:00+00:00",
        })))
        .mount(&server)
        .await;

    let outcome = runner_for(&server)
        .launch(None, StartScanFields::default())
        .await
        .expect("launch should succeed");

    // The echoed id is authoritative, not the one we asked for (or didn't).
    assert_eq!(outcome.run_id, "generated99");
}

#[tokio::test]
async fn a_run_id_already_in_use_is_reported_as_already_exists() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/runs"))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_json(json!({"detail": "run_id 'rms-job-7' already exists"})),
        )
        .mount(&server)
        .await;

    let err = runner_for(&server)
        .launch(Some("rms-job-7"), StartScanFields::default())
        .await
        .expect_err("a duplicate run id should be an error");

    assert!(
        matches!(err, NvssvtError::RunAlreadyExists(_)),
        "got {err:?}"
    );
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::AlreadyExists);
}

#[tokio::test]
async fn status_maps_a_running_scan_with_its_progress() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/runs/abc123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "run_id": "abc123",
            "pid": 4242,
            "state": "running",
            "return_code": null,
            "started_at": "2026-09-23T10:00:00+00:00",
            "log_dir": "/opt/nvssvt/api_runs/abc123/NVSSVT_Logs_1",
            "overall_progress": 42.5,
            "duts": {"10.0.0.5": {"OVERALL_NVSSVT_PROGRESS": 42.5}},
            "summary": null,
            "error": null,
        })))
        .mount(&server)
        .await;

    let status = runner_for(&server)
        .status("abc123")
        .await
        .expect("status should succeed");

    assert_eq!(status.state, RunState::Running);
    assert_eq!(status.overall_progress, Some(42.5));
    assert_eq!(status.return_code, None);
    assert_eq!(status.duts["10.0.0.5"]["OVERALL_NVSSVT_PROGRESS"], 42.5);
}

#[tokio::test]
async fn status_carries_the_summary_report_once_the_scan_completes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/runs/abc123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "run_id": "abc123",
            "pid": 4242,
            "state": "completed",
            "return_code": 0,
            "started_at": "2026-09-23T10:00:00+00:00",
            "log_dir": "/opt/nvssvt/api_runs/abc123/NVSSVT_Logs_1",
            "overall_progress": 100.0,
            "duts": {},
            "summary": {"pass": 1, "fail": 0},
            "error": null,
        })))
        .mount(&server)
        .await;

    let status = runner_for(&server).status("abc123").await.expect("status");

    assert_eq!(status.state, RunState::Completed);
    assert_eq!(status.return_code, Some(0));
    assert_eq!(status.summary.expect("summary")["pass"], 1);
}

#[tokio::test]
async fn status_reports_a_failed_scan_with_its_error_text() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/runs/abc123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "run_id": "abc123",
            "pid": 4242,
            "state": "failed",
            "return_code": 1,
            "started_at": "2026-09-23T10:00:00+00:00",
            "log_dir": "/opt/nvssvt/api_runs/abc123",
            "overall_progress": null,
            "duts": {},
            "summary": null,
            "error": "simulated failure",
        })))
        .mount(&server)
        .await;

    let status = runner_for(&server).status("abc123").await.expect("status");

    assert_eq!(status.state, RunState::Failed);
    assert_eq!(status.error.as_deref(), Some("simulated failure"));
}

#[tokio::test]
async fn an_unknown_run_id_is_reported_as_not_found() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/runs/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"detail": "no such run"})))
        .mount(&server)
        .await;

    let err = runner_for(&server)
        .status("missing")
        .await
        .expect_err("an unknown run id should be an error");

    assert!(matches!(err, NvssvtError::RunNotFound(_)), "got {err:?}");
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn launcher_errors_are_surfaced_with_their_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/runs"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let err = runner_for(&server)
        .launch(None, StartScanFields::default())
        .await
        .expect_err("a 500 should be an error");

    assert!(err.to_string().contains("boom"), "got {err}");
    assert_eq!(tonic::Status::from(err).code(), tonic::Code::Internal);
}

#[tokio::test]
async fn an_unreachable_launcher_is_reported_as_unavailable() {
    // Reserve the port without listening so connections are refused and
    // concurrent mock servers cannot claim the port during the request.
    let socket = tokio::net::TcpSocket::new_v4().expect("create socket");
    socket.bind(([127, 0, 0, 1], 0).into()).expect("bind");
    let port = socket.local_addr().expect("addr").port();

    let config = NvssvtConfig::new("127.0.0.1", port).with_request_timeout(Duration::from_secs(2));

    let err = NvssvtRunner::new(config)
        .expect("build runner")
        .launch(None, StartScanFields::default())
        .await
        .expect_err("an unreachable launcher should be an error");

    drop(socket);

    assert!(matches!(err, NvssvtError::Transport(_)), "got {err:?}");

    assert_eq!(tonic::Status::from(err).code(), tonic::Code::Unavailable);
}
