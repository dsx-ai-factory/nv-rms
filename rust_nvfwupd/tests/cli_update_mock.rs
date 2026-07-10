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

use std::io::Cursor;
use std::path::Path;
use std::process::{Command, Output};

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

async fn mount_powershelf_update_mock(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Members": [{"@odata.id": "/redfish/v1/Chassis/powershelf"}]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Members": [{"@odata.id": "/redfish/v1/Chassis/powershelf"}]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/powershelf"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Model": "PF-1333-13RD",
            "PartNumber": "PN-CLI",
            "SerialNumber": "SN-CLI"
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ServiceEnabled": true,
            "HttpPushUri": "/redfish/v1/UpdateService/upload"
        })))
        .mount(server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/redfish/v1/UpdateService"))
        .and(body_string_contains("Immediate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/redfish/v1/UpdateService/update"))
        .and(body_string_contains("cli firmware"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "Id": "Task-CliUpdate"
        })))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn update_fw_cli_runs_against_mock_bmc() {
    let server = MockServer::start().await;
    mount_powershelf_update_mock(&server).await;

    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("powershelf_cli.tar");
    write_test_tar(
        &package,
        b"purpose=PSU\nversion=2.0.0\nmodel=PowerShelf\n",
        b"cli firmware",
    );
    let port = server
        .uri()
        .rsplit(':')
        .next()
        .expect("mock server port")
        .to_string();

    let args = [
        "-t".to_string(),
        "ip=127.0.0.1".to_string(),
        "user=test".to_string(),
        "password=secret from cli".to_string(),
        format!("port={port}"),
        "servertype=powershelf".to_string(),
        "update_fw".to_string(),
        "-p".to_string(),
        package.to_string_lossy().to_string(),
        "-y".to_string(),
        "-b".to_string(),
        "-j".to_string(),
        "--skip_pre_flight_checks".to_string(),
    ];
    let output = run_nvfwupd(&args, tmp.path());

    assert_eq!(
        output.status.code(),
        Some(0),
        "update_fw failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let package_path = package.to_string_lossy();
    let normalized_stdout = stdout.as_ref().replace(package_path.as_ref(), "<PACKAGE>");
    assert_eq!(
        normalized_stdout,
        concat!(
            "{\n",
            "    \"Error\": [],\n",
            "    \"Error Code\": 0,\n",
            "    \"Output\": [\n",
            "        {\n",
            "            \"Id\": \"Task-CliUpdate\"\n",
            "        }\n",
            "    ]\n",
            "}\n",
        ),
        "stdout changed for CLI mock update path"
    );
}

#[tokio::test]
async fn update_fw_cli_verbose_writes_timestamped_log_file() {
    let server = MockServer::start().await;
    mount_powershelf_update_mock(&server).await;

    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("powershelf_cli.tar");
    let log_file = tmp.path().join("nvfwupd_cli.log");
    write_test_tar(
        &package,
        b"purpose=PSU\nversion=2.0.0\nmodel=PowerShelf\n",
        b"cli firmware",
    );
    let port = server
        .uri()
        .rsplit(':')
        .next()
        .expect("mock server port")
        .to_string();

    let args = [
        "-v".to_string(),
        log_file.to_string_lossy().to_string(),
        "-t".to_string(),
        "ip=127.0.0.1".to_string(),
        "user=test".to_string(),
        "password=secret from cli".to_string(),
        format!("port={port}"),
        "servertype=powershelf".to_string(),
        "update_fw".to_string(),
        "-p".to_string(),
        package.to_string_lossy().to_string(),
        "-y".to_string(),
        "-b".to_string(),
        "-j".to_string(),
        "--skip_pre_flight_checks".to_string(),
    ];
    let output = run_nvfwupd(&args, tmp.path());

    assert_eq!(
        output.status.code(),
        Some(0),
        "update_fw failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let log_contents = std::fs::read_to_string(&log_file).expect("read verbose log file");
    assert!(
        !log_contents.contains("secret"),
        "verbose log leaked the target password: {log_contents}"
    );
    assert!(
        !log_contents.contains("from cli"),
        "verbose log leaked the whitespace password suffix: {log_contents}"
    );
    assert!(
        log_contents.contains("password=XXXX"),
        "verbose log did not show a redacted password field: {log_contents}"
    );
    assert!(
        log_contents.contains("Cmd args:"),
        "verbose log did not contain CLI tracing output: {log_contents}"
    );
    let first_line = log_contents.lines().next().expect("log line");
    assert_eq!(first_line.as_bytes()[4], b'-');
    assert_eq!(first_line.as_bytes()[7], b'-');
    assert_eq!(first_line.as_bytes()[10], b' ');
    assert_eq!(first_line.as_bytes()[13], b':');
    assert_eq!(first_line.as_bytes()[16], b':');
}

#[tokio::test]
async fn update_fw_cli_verbose_without_path_uses_default_log_file() {
    let server = MockServer::start().await;
    mount_powershelf_update_mock(&server).await;

    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("powershelf_cli.tar");
    write_test_tar(
        &package,
        b"purpose=PSU\nversion=2.0.0\nmodel=PowerShelf\n",
        b"cli firmware",
    );
    let port = server
        .uri()
        .rsplit(':')
        .next()
        .expect("mock server port")
        .to_string();

    let args = [
        "-v".to_string(),
        "-t".to_string(),
        "ip=127.0.0.1".to_string(),
        "user=test".to_string(),
        "password=test".to_string(),
        format!("port={port}"),
        "servertype=powershelf".to_string(),
        "update_fw".to_string(),
        "-p".to_string(),
        package.to_string_lossy().to_string(),
        "-y".to_string(),
        "-b".to_string(),
        "-j".to_string(),
        "--skip_pre_flight_checks".to_string(),
    ];
    let output = run_nvfwupd(&args, tmp.path());

    assert_eq!(
        output.status.code(),
        Some(0),
        "update_fw failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let log_file = tmp.path().join("nvfwupd_log.txt");
    let log_contents = std::fs::read_to_string(&log_file).expect("read default verbose log file");
    assert!(
        log_contents.contains("Cmd args:"),
        "default verbose log did not contain CLI tracing output: {log_contents}"
    );
}
