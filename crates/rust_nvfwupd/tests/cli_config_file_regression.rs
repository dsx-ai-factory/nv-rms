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

use std::io::Cursor;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn run_nvfwupd(args: &[String], cwd: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_nvfwupd"))
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("failed to run nvfwupd test binary")
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yaml");
    std::fs::write(&path, contents).expect("write config file");
    path
}

fn write_test_tar(tar_path: &Path, manifest: &[u8], payload: &[u8]) {
    let tar_file = std::fs::File::create(tar_path).expect("create tar package");
    let mut builder = tar::Builder::new(tar_file);

    let mut manifest_header = tar::Header::new_gnu();
    manifest_header.set_size(manifest.len() as u64);
    manifest_header.set_mode(0o644);
    manifest_header.set_cksum();
    builder
        .append_data(&mut manifest_header, "MANIFEST", Cursor::new(manifest))
        .expect("append manifest");

    let mut payload_header = tar::Header::new_gnu();
    payload_header.set_size(payload.len() as u64);
    payload_header.set_mode(0o644);
    payload_header.set_cksum();
    builder
        .append_data(&mut payload_header, "firmware.bin", Cursor::new(payload))
        .expect("append payload");

    builder.finish().expect("finish tar package");
}

async fn mount_common_redfish_discovery(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Members": [{"@odata.id": "/redfish/v1/Chassis/DGX"}]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Members": [{"@odata.id": "/redfish/v1/Chassis/DGX"}]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/DGX"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Model": "GB200 NVL",
            "PartNumber": "PN-CONFIG",
            "SerialNumber": "SN-CONFIG"
        })))
        .mount(server)
        .await;
}

fn localhost_port(server: &MockServer) -> String {
    server
        .uri()
        .rsplit(':')
        .next()
        .expect("mock server port")
        .to_string()
}

fn run_tcp_probe_listener() -> (
    u16,
    Arc<AtomicBool>,
    Arc<AtomicBool>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local TCP probe");
    listener
        .set_nonblocking(true)
        .expect("set probe listener nonblocking");
    let port = listener.local_addr().expect("read listener address").port();
    let accepted = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let accepted_for_thread = Arc::clone(&accepted);
    let stop_for_thread = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !stop_for_thread.load(Ordering::SeqCst) && Instant::now() < deadline {
            match listener.accept() {
                Ok((stream, _)) => {
                    accepted_for_thread.store(true, Ordering::SeqCst);
                    drop(stream);
                    return;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return,
            }
        }
    });
    (port, accepted, stop, handle)
}

#[test]
fn update_fw_config_rejects_invalid_skip_preflight_type() {
    let tmp = tempfile::tempdir().unwrap();
    let config = write_config(
        tmp.path(),
        r#"
BMC_IP: 127.0.0.1
RF_USERNAME: test
RF_PASSWORD: secret
TargetPlatform: GB200
FWUpdateFilePath:
  - missing.fwpkg
SkipPreFlightChecks: 1
"#,
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "update_fw".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Improper format for SkipPreFlightChecks parameter"),
        "invalid SkipPreFlightChecks diagnostic missing: {stdout}"
    );
    assert!(
        !stdout.contains("missing.fwpkg"),
        "update continued far enough to validate packages: {stdout}"
    );
}

#[test]
fn update_fw_config_rejects_invalid_parallel_update_type() {
    let tmp = tempfile::tempdir().unwrap();
    let config = write_config(
        tmp.path(),
        r#"
ParallelUpdate: "true"
Targets:
  - BMC_IP: 127.0.0.1
    RF_USERNAME: test
    RF_PASSWORD: secret
    PACKAGE:
      - missing.fwpkg
"#,
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "update_fw".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Improper format for ParallelUpdate parameter"),
        "invalid ParallelUpdate diagnostic missing: {stdout}"
    );
    assert!(
        !stdout.contains("missing.fwpkg"),
        "update continued far enough to validate packages: {stdout}"
    );
}

#[test]
fn update_fw_config_rejects_malformed_multipart_options() {
    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("config_update.tar");
    write_test_tar(
        &package,
        b"purpose=PSU\nversion=1.0.0\n",
        b"config firmware",
    );
    let config = write_config(
        tmp.path(),
        &format!(
            r#"
BMC_IP: 192.0.2.10
RF_USERNAME: test
RF_PASSWORD: secret
TargetPlatform: GB200
FwUpdateMethod: MultipartHttpPushUri
FWUpdateFilePath:
  - {}
MultipartOptions: "ForceUpdate: true"
"#,
            package.display()
        ),
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "update_fw".to_string(),
            "-y".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Missing/Invalid configuration for MultipartOptions"),
        "invalid MultipartOptions diagnostic missing: {stdout}"
    );
    assert!(
        !stdout.contains("Unable to access BMC"),
        "update continued to target access after invalid config: {stdout}"
    );
}

#[test]
fn update_fw_config_rejects_malformed_update_parameters_targets() {
    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("config_update.tar");
    write_test_tar(
        &package,
        b"purpose=PSU\nversion=1.0.0\n",
        b"config firmware",
    );
    let config = write_config(
        tmp.path(),
        &format!(
            r#"
BMC_IP: 192.0.2.10
RF_USERNAME: test
RF_PASSWORD: secret
TargetPlatform: GB200
FwUpdateMethod: MultipartHttpPushUri
FWUpdateFilePath:
  - {}
MultipartOptions:
  ForceUpdate: true
UpdateParametersTargets:
  Target: /redfish/v1/Chassis/HGX_Chassis_0
"#,
            package.display()
        ),
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "update_fw".to_string(),
            "-y".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Missing/Invalid configuration for UpdateParametersTargets"),
        "invalid UpdateParametersTargets diagnostic missing: {stdout}"
    );
    assert!(
        !stdout.contains("Unable to access BMC"),
        "update continued to target access after invalid config: {stdout}"
    );
}

#[test]
fn update_fw_parallel_config_rejects_empty_package_payload() {
    let tmp = tempfile::tempdir().unwrap();
    let config = write_config(
        tmp.path(),
        r#"
ParallelUpdate: true
Targets:
  - BMC_IP: 127.0.0.1
    RF_USERNAME: test
    RF_PASSWORD: secret
    PACKAGE: []
"#,
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "update_fw".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("missing/invalid PACKAGE"),
        "invalid PACKAGE diagnostic missing: {stdout}"
    );
}

#[test]
fn update_fw_parallel_config_rejects_multiple_packages_per_target() {
    let tmp = tempfile::tempdir().unwrap();
    let first_package = tmp.path().join("first_update.tar");
    let second_package = tmp.path().join("second_update.tar");
    write_test_tar(
        &first_package,
        b"purpose=PSU\nversion=1.0.0\n",
        b"first config firmware",
    );
    write_test_tar(
        &second_package,
        b"purpose=PSU\nversion=2.0.0\n",
        b"second config firmware",
    );
    let config = write_config(
        tmp.path(),
        &format!(
            r#"
ParallelUpdate: true
Targets:
  - BMC_IP: 127.0.0.1
    RF_USERNAME: test
    RF_PASSWORD: secret
    PACKAGE:
      - {}
      - {}
"#,
            first_package.display(),
            second_package.display()
        ),
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "update_fw".to_string(),
            "-y".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Only one package per system is supported in parallel update."),
        "parallel multi-package diagnostic missing: {stdout}"
    );
    assert!(
        !stdout.contains("Unable to access BMC"),
        "update continued to target access after invalid parallel PACKAGE: {stdout}"
    );
}

#[test]
fn flint_update_config_uses_numeric_os_port() {
    let tmp = tempfile::tempdir().unwrap();
    let (port, accepted, stop, handle) = run_tcp_probe_listener();
    let config = write_config(
        tmp.path(),
        &format!(
            r#"
OS_IP: "127.0.0.1"
OS_USERNAME: "test"
OS_PASSWORD: "secret"
OS_PORT: {port}
BMC_IP: "192.0.2.10"
RF_USERNAME: "test"
RF_PASSWORD: "secret"
FlintDeviceType: "ConnectX"
"#
        ),
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "flint_update".to_string(),
            "-v".to_string(),
            "-t".to_string(),
            "1".to_string(),
        ],
        tmp.path(),
    );
    stop.store(true, Ordering::SeqCst);
    handle.join().expect("join TCP probe listener");

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        accepted.load(Ordering::SeqCst),
        "flint_update did not connect to numeric OS_PORT {port}; stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn flint_update_config_rejects_invalid_os_port_type() {
    let tmp = tempfile::tempdir().unwrap();
    let config = write_config(
        tmp.path(),
        r#"
OS_IP: "127.0.0.1"
OS_USERNAME: "test"
OS_PASSWORD: "secret"
OS_PORT:
  nested: value
BMC_IP: "192.0.2.10"
RF_USERNAME: "test"
RF_PASSWORD: "secret"
FlintDeviceType: "ConnectX"
"#,
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "flint_update".to_string(),
            "-v".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("OS_PORT must be a non-empty string or integer"),
        "invalid OS_PORT diagnostic missing: {stdout}"
    );
    assert!(
        !stdout.contains("Starting MST service"),
        "flint_update continued after invalid OS_PORT: {stdout}"
    );
}

#[test]
fn perform_factory_reset_config_rejects_invalid_bmc_reset_parameters() {
    let tmp = tempfile::tempdir().unwrap();
    let config = write_config(
        tmp.path(),
        r#"
BMC_IP: "192.0.2.10"
RF_USERNAME: "test"
RF_PASSWORD: "secret"
TargetPlatform: GB200
BMCResetParameters: ResetAll
"#,
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "perform_factory_reset".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Missing/Invalid configuration for BMCResetParameters"),
        "invalid BMCResetParameters diagnostic missing: {stdout}"
    );
    assert!(
        !stdout.contains("Unable to access BMC"),
        "factory reset continued to target access after invalid config: {stdout}"
    );
}

#[tokio::test]
async fn update_fw_single_target_config_forwards_numeric_port_multipart_options_and_oem_parameters()
{
    let server = MockServer::start().await;
    mount_common_redfish_discovery(&server).await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ServiceEnabled": true,
            "MultipartHttpPushUri": "/redfish/v1/UpdateService/update-multipart"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/redfish/v1/UpdateService/update-multipart"))
        .and(body_string_contains("\"ForceUpdate\":true"))
        .and(body_string_contains("/redfish/v1/Chassis/HGX_Chassis_0"))
        .and(body_string_contains("OemParameters"))
        .and(body_string_contains("\"ExampleOem\":\"enabled\""))
        .and(body_string_contains("config firmware"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "Id": "Task-ConfigUpdate"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("config_update.tar");
    write_test_tar(
        &package,
        b"purpose=PSU\nversion=1.0.0\n",
        b"config firmware",
    );
    let config = write_config(
        tmp.path(),
        &format!(
            r#"
BMC_IP: 127.0.0.1
RF_USERNAME: test
RF_PASSWORD: secret
TUNNEL_TCP_PORT: {}
ALLOW_HTTP: true
TargetPlatform: GB200
FwUpdateMethod: MultipartHttpPushUri
FWUpdateFilePath:
  - {}
MultipartOptions:
  ForceUpdate: true
UpdateParametersTargets:
  - /redfish/v1/Chassis/HGX_Chassis_0
OemParameters:
  ExampleOem: enabled
SkipPreFlightChecks: true
"#,
            localhost_port(&server),
            package.display()
        ),
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "update_fw".to_string(),
            "-y".to_string(),
            "-b".to_string(),
            "-j".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(0),
        "update_fw failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should be valid JSON");
    assert_eq!(parsed["Error Code"], 0);
    assert_eq!(parsed["Output"][0]["Id"], "Task-ConfigUpdate");
}

#[tokio::test]
async fn perform_factory_reset_config_forwards_bmc_reset_parameters() {
    let server = MockServer::start().await;
    mount_common_redfish_discovery(&server).await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Managers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Members": [{"@odata.id": "/redfish/v1/Managers/BMC_0"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(
            "/redfish/v1/Managers/BMC_0/Actions/Manager.ResetToDefaults",
        ))
        .and(body_string_contains("\"ResetToDefaultsType\":\"ResetAll\""))
        .and(body_string_contains("\"PreserveNetwork\":true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "TaskState": "Running"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let config = write_config(
        tmp.path(),
        &format!(
            r#"
BMC_IP: 127.0.0.1
RF_USERNAME: test
RF_PASSWORD: secret
TUNNEL_TCP_PORT: {}
ALLOW_HTTP: true
TargetPlatform: GB200
BMCResetParameters:
  ResetToDefaultsType: ResetAll
  PreserveNetwork: true
"#,
            localhost_port(&server)
        ),
    );

    let output = run_nvfwupd(
        &[
            "-c".to_string(),
            config.to_string_lossy().to_string(),
            "perform_factory_reset".to_string(),
        ],
        tmp.path(),
    );

    assert_eq!(
        output.status.code(),
        Some(0),
        "perform_factory_reset failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Factory Reset request successful"),
        "factory reset success message missing: {stdout}"
    );
}

#[tokio::test]
async fn update_fw_submission_failures_report_plain_text_and_identify_json_targets() {
    let large_body =
        json!({"error": {"Message": "busy ".repeat(2000), "password": "response-secret"}})
            .to_string();
    for parallel in [false, true] {
        for (status, body, category) in [
            (
                409,
                r#"{"error":{"Message":"busy","password":"response-secret"}}"#,
                "http_rejection",
            ),
            (
                202,
                "not-json password=response-secret",
                "json_decode_failure",
            ),
            (200, "{}", "missing_task_reference"),
            (202, "{}", "missing_task_reference"),
            (
                202,
                r#"{"Message":"Echoed target-secret"}"#,
                "missing_task_reference",
            ),
            (200, r#"{"Id":""}"#, "malformed_task_reference"),
            (202, r#"{"Id":""}"#, "malformed_task_reference"),
            (409, large_body.as_str(), "http_rejection"),
        ] {
            let server = MockServer::start().await;
            mount_common_redfish_discovery(&server).await;
            Mock::given(method("GET"))
                .and(path("/redfish/v1/UpdateService"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "ServiceEnabled": true,
                    "MultipartHttpPushUri": "/redfish/v1/UpdateService/update-multipart"
                })))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/redfish/v1/UpdateService/update-multipart"))
                .respond_with(
                    ResponseTemplate::new(status)
                        .insert_header("Retry-After", "30")
                        .set_body_string(body),
                )
                .expect(if parallel { 3 } else { 2 })
                .mount(&server)
                .await;

            let tmp = tempfile::tempdir().unwrap();
            let package = tmp.path().join("config_update.tar");
            write_test_tar(&package, b"purpose=PSU\nversion=1.0.0\n", b"firmware");
            let config = write_config(
                tmp.path(),
                &format!(
                    r#"
TargetPlatform: GB200
FwUpdateMethod: MultipartHttpPushUri
FWUpdateFilePath:
  - {package_path}
SkipPreFlightChecks: true
ParallelUpdate: {parallel}
Targets:
  - BMC_IP: 127.0.0.1
    RF_USERNAME: skipped-target
    PACKAGE: {package_path}
  - BMC_IP: 127.0.0.1
    RF_USERNAME: test
    RF_PASSWORD: target-secret
    TUNNEL_TCP_PORT: {port}
    ALLOW_HTTP: true
    PACKAGE: {package_path}
  - BMC_IP: 127.0.0.1
    RF_USERNAME: test
    RF_PASSWORD: target-secret
    TUNNEL_TCP_PORT: {port}
    ALLOW_HTTP: true
    PACKAGE: {package_path}
"#,
                    port = localhost_port(&server),
                    package_path = package.display()
                ),
            );
            let json_config = std::fs::read_to_string(&config).unwrap();
            // Serial JSON updates require --background and accept one target.
            // Parallel JSON updates support multiple targets with --background.
            if !parallel {
                std::fs::write(&config, json_config.rsplit_once("  - BMC_IP:").unwrap().0).unwrap();
            }
            let plain_config = tmp.path().join("plain-config.yaml");
            std::fs::write(
                &plain_config,
                json_config.rsplit_once("  - BMC_IP:").unwrap().0,
            )
            .unwrap();
            let plain_result = run_nvfwupd(
                &[
                    "-c".to_string(),
                    plain_config.to_string_lossy().to_string(),
                    "update_fw".to_string(),
                    "-y".to_string(),
                    "-b".to_string(),
                ],
                tmp.path(),
            );
            let plain_stdout = String::from_utf8(plain_result.stdout).unwrap();
            assert_eq!(plain_result.status.code(), Some(1), "{plain_stdout}");
            assert!(
                plain_stdout.contains("Firmware submission failed:"),
                "{plain_stdout}"
            );
            assert!(plain_stdout.contains(category), "{plain_stdout}");
            assert!(
                plain_stdout.contains(&format!("\"http_status\":{status}")),
                "{plain_stdout}"
            );
            assert!(!plain_stdout.contains("response-secret"), "{plain_stdout}");
            assert!(!plain_stdout.contains("target-secret"), "{plain_stdout}");
            if body == large_body {
                let legacy_error = plain_stdout
                    .lines()
                    .find(|line| line.starts_with("File upload failed with error "))
                    .expect("legacy upload error remains visible");
                assert!(legacy_error.contains("busy"), "{legacy_error}");
                assert!(
                    legacy_error.chars().count() <= 4096 + "File upload failed with error ".len()
                );
            }

            let result = run_nvfwupd(
                &[
                    "-c".to_string(),
                    config.to_string_lossy().to_string(),
                    "update_fw".to_string(),
                    "-y".to_string(),
                    "-b".to_string(),
                    "-j".to_string(),
                ],
                tmp.path(),
            );
            let stdout = String::from_utf8(result.stdout).unwrap();
            assert_eq!(
                result.status.code(),
                Some(1),
                "parallel={parallel} category={category}: {stdout}\n{}",
                String::from_utf8_lossy(&result.stderr)
            );
            // Parsing all of stdout also rejects extra plain-text errors or JSON objects.
            let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|err| {
                panic!("parallel={parallel} category={category}: {err}: {stdout}")
            });
            let object = parsed.as_object().unwrap();
            assert_eq!(object.len(), 3, "JSON envelope changed: {stdout}");
            assert_eq!(parsed["Error Code"], 1);
            let errors = parsed["Error"].as_array().expect("Error remains an array");
            assert!(
                !errors.is_empty(),
                "missing legacy error: parallel={parallel}, category={category}: {stdout}"
            );
            assert!(
                errors.iter().all(serde_json::Value::is_string),
                "Error entries must remain strings: {stdout}"
            );
            assert!(
                errors.iter().any(|error| error.as_str()
                    == Some("Failed to acquire task ID from firmware update response")),
                "legacy submission error changed: {stdout}"
            );
            let entries = parsed["Output"]
                .as_array()
                .expect("Output remains an array");
            let diagnostics: Vec<_> = entries
                .iter()
                .filter_map(|entry| entry.get("SubmissionDiagnostic"))
                .collect();
            assert_eq!(
                diagnostics.len(),
                if parallel { 2 } else { 1 },
                "diagnostics lost in parallel={parallel}: {stdout}"
            );
            for (target_index, diagnostic) in diagnostics.iter().enumerate() {
                assert_eq!(diagnostic["target_index"], target_index + 1);
                assert_eq!(diagnostic["category"], category);
                assert_eq!(diagnostic["http_status"], status);
                assert_eq!(diagnostic["headers"]["retry-after"], "30");
            }
            assert!(!stdout.contains("response-secret"), "{stdout}");
            assert!(!stdout.contains("target-secret"), "{stdout}");
        }
    }
}
