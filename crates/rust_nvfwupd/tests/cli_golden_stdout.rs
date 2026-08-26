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
use std::path::Path;
use std::process::{Command, Output};

use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const DIVIDER: &str =
    "------------------------------------------------------------------------------------------------------------------------\n";
const MISSING_TARGET_STDOUT: &str =
    "Error: Required option -t/--target or -c/--config is missing.\nError Code: 1\n";

fn run_nvfwupd(args: &[String], cwd: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_nvfwupd"))
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("failed to run nvfwupd test binary")
}

fn assert_stdout(output: &Output, expected_status: i32, expected_stdout: &str) {
    assert_eq!(
        output.status.code(),
        Some(expected_status),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        expected_stdout,
        "stdout changed"
    );
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

fn powershelf_target_args(server: &MockServer) -> Vec<String> {
    let port = server
        .uri()
        .rsplit(':')
        .next()
        .expect("mock server port")
        .to_string();
    vec![
        "-t".to_string(),
        "ip=127.0.0.1".to_string(),
        "user=test".to_string(),
        "password=test".to_string(),
        format!("port={port}"),
        "servertype=powershelf".to_string(),
    ]
}

async fn mount_powershelf_chassis_mock(server: &MockServer) {
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
}

async fn mount_powershelf_update_mock(server: &MockServer) {
    mount_powershelf_chassis_mock(server).await;
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
        .and(body_string_contains("golden firmware"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "Id": "Task-GoldenUpdate"
        })))
        .expect(1)
        .mount(server)
        .await;
}

#[test]
fn golden_stdout_for_missing_target_command_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let cases: &[&[&str]] = &[
        &["show_version"],
        &["activate_fw", "-c", "RF_PWR_STATUS"],
        &["show_update_progress", "-i", "Task-Golden"],
        &["force_update", "status"],
        &["background_copy"],
        &["perform_factory_reset"],
        &["make_upd_targets"],
        &["flint_update", "-v"],
    ];

    for case in cases {
        let args: Vec<String> = case.iter().map(|arg| arg.to_string()).collect();
        let output = run_nvfwupd(&args, tmp.path());
        assert_stdout(&output, 1, MISSING_TARGET_STDOUT);
    }
}

#[test]
fn golden_stdout_for_package_only_command_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("not_pldm.tar");
    std::fs::write(&package, b"not a pldm package").unwrap();
    let package_arg = package.to_string_lossy().to_string();

    let show_pkg_args = vec![
        "show_pkg_content".to_string(),
        "-p".to_string(),
        package_arg.clone(),
    ];
    let show_pkg = run_nvfwupd(&show_pkg_args, tmp.path());
    let expected_show_pkg = "Error: incorrect package format.\n\
         Given input file <PACKAGE> is not a valid PLDM fwpkg\n\
         Error Code: 1\n";
    let normalized_show_pkg =
        String::from_utf8_lossy(&show_pkg.stdout).replace(&package_arg, "<PACKAGE>");
    assert_eq!(show_pkg.status.code(), Some(1));
    assert_eq!(normalized_show_pkg, expected_show_pkg);

    let unpack_args = vec!["unpack".to_string(), "-p".to_string(), package_arg.clone()];
    let unpack = run_nvfwupd(&unpack_args, tmp.path());
    let expected_unpack = format!("WARN: <PACKAGE> is not a PLDM package.\n{DIVIDER}");
    let normalized_unpack =
        String::from_utf8_lossy(&unpack.stdout).replace(&package_arg, "<PACKAGE>");
    assert_eq!(unpack.status.code(), Some(0));
    assert_eq!(normalized_unpack, expected_unpack);
}

#[test]
fn golden_stdout_for_flint_update_dispatch_path() {
    let tmp = tempfile::tempdir().unwrap();
    let args = vec![
        "-o".to_string(),
        "ip=127.0.0.1".to_string(),
        "user=test".to_string(),
        "password=test".to_string(),
        "port=1".to_string(),
        "flint_update".to_string(),
        "-v".to_string(),
        "-t".to_string(),
        "1".to_string(),
    ];
    let output = run_nvfwupd(&args, tmp.path());

    assert_stdout(
        &output,
        1,
        "Starting MST service...\n\
         Failed to start MST service. MST services may not be installed, or the OS may be unreachable. \
         Command execution error: SSH connect failed: Connection refused (os error 111)\n\
         Error Code: 1\n",
    );
}

#[tokio::test]
async fn golden_stdout_for_update_fw_mock_path() {
    let server = MockServer::start().await;
    mount_powershelf_update_mock(&server).await;

    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("powershelf_golden.tar");
    write_test_tar(
        &package,
        b"purpose=PSU\nversion=2.0.0\nmodel=PowerShelf\n",
        b"golden firmware",
    );

    let mut args = powershelf_target_args(&server);
    args.extend([
        "update_fw".to_string(),
        "-p".to_string(),
        package.to_string_lossy().to_string(),
        "-y".to_string(),
        "-b".to_string(),
        "-j".to_string(),
        "--skip_pre_flight_checks".to_string(),
    ]);
    let output = run_nvfwupd(&args, tmp.path());
    let package_arg = package.to_string_lossy();
    let normalized_stdout = String::from_utf8_lossy(&output.stdout)
        .as_ref()
        .replace(package_arg.as_ref(), "<PACKAGE>");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        normalized_stdout,
        concat!(
            "{\n",
            "    \"Error\": [],\n",
            "    \"Error Code\": 0,\n",
            "    \"Output\": [\n",
            "        {\n",
            "            \"Id\": \"Task-GoldenUpdate\"\n",
            "        }\n",
            "    ]\n",
            "}\n",
        )
    );
}

#[tokio::test]
async fn golden_stdout_for_show_version_mock_path() {
    let server = MockServer::start().await;
    mount_powershelf_chassis_mock(&server).await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService/FirmwareInventory"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Members": [{"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/BMC"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService/FirmwareInventory/BMC"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Version": "1.0.0"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let mut args = powershelf_target_args(&server);
    args.push("show_version".to_string());
    let output = run_nvfwupd(&args, tmp.path());

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!(
            "System Model: PF-1333-13RD\n\
             Part number: PN-CLI\n\
             Serial number: SN-CLI\n\
             Packages: N/A\n\
             Connection Status: Successful\n\n\
             Firmware Devices:\n\
             {:<40} {:<30}\n\
             {:<40} {:<30}\n\
             {:<40} {:<30}\n\
             {DIVIDER}\
             Error Code: 0\n",
            "AP Name", "Sys Version", "-------", "-----------", "BMC", "1.0.0"
        )
    );
}

#[tokio::test]
async fn golden_stdout_for_force_update_mock_path() {
    let server = MockServer::start().await;
    mount_powershelf_chassis_mock(&server).await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "HttpPushUriOptions": {
                "ForceUpdate": true
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let mut args = powershelf_target_args(&server);
    args.extend(["force_update".to_string(), "status".to_string()]);
    let output = run_nvfwupd(&args, tmp.path());

    assert_stdout(
        &output,
        0,
        &format!("BMC Connection Status: Successful\nForceUpdate is set to true\n{DIVIDER}"),
    );
}

#[tokio::test]
async fn golden_stdout_for_show_update_progress_mock_path() {
    let server = MockServer::start().await;
    mount_powershelf_chassis_mock(&server).await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/TaskService/Tasks/Task-Golden"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "StartTime": "2025-01-01T00:00:00Z",
            "TaskState": "Completed",
            "PercentComplete": 100,
            "TaskStatus": "OK"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let mut args = powershelf_target_args(&server);
    args.extend([
        "show_update_progress".to_string(),
        "-i".to_string(),
        "Task-Golden".to_string(),
    ]);
    let output = run_nvfwupd(&args, tmp.path());

    assert_stdout(
        &output,
        0,
        &format!(
            "  Task Info for Id: Task-Golden\n\
             {space2}{divider}\
             {space2}Task Info for Id: Task-Golden\n\
             {space1}StartTime: 2025-01-01T00:00:00Z\n\
             {space1}TaskState: Completed\n\
             {space1}PercentComplete: 100\n\
             {space1}TaskStatus: OK\n\
             {space1}Overall Task Status: {{\n\
             {space4}\"StartTime\": \"2025-01-01T00:00:00Z\",\n\
             {space4}\"TaskState\": \"Completed\",\n\
             {space4}\"PercentComplete\": 100,\n\
             {space4}\"TaskStatus\": \"OK\"\n\
             }}\n\
             {space2}Update is successful.\n\
             {divider}\
             Error Code: 0\n",
            space1 = " ",
            space2 = "  ",
            space4 = "    ",
            divider = DIVIDER
        ),
    );
}

#[tokio::test]
async fn golden_stdout_for_activate_fw_mock_path() {
    let server = MockServer::start().await;
    mount_powershelf_chassis_mock(&server).await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Managers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Members": [{"@odata.id": "/redfish/v1/Managers/BMC"}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Managers/BMC"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Actions": {
                "#Manager.Reset": {
                    "target": "/redfish/v1/Managers/BMC/Actions/Manager.Reset"
                }
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/redfish/v1/Managers/BMC/Actions/Manager.Reset"))
        .and(body_string_contains("GracefulRestart"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Reset": "Accepted"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let mut args = powershelf_target_args(&server);
    args.extend([
        "activate_fw".to_string(),
        "-c".to_string(),
        "RF_PWRSHELF_RESET".to_string(),
    ]);
    let output = run_nvfwupd(&args, tmp.path());

    assert_stdout(
        &output,
        0,
        &format!(
            "RF_PWRSHELF_RESET requested successfully.\n\
             PowerShelf Manager reset (GracefulRestart) initiated at /redfish/v1/Managers/BMC/Actions/Manager.Reset\n\
             Server response:\n\
             {{\n\
             {space4}\"Reset\": \"Accepted\"\n\
             }}\n\
             {DIVIDER}",
            space4 = "    "
        ),
    );
}

#[tokio::test]
async fn golden_stdout_for_background_copy_mock_path() {
    let server = MockServer::start().await;
    mount_powershelf_chassis_mock(&server).await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/UpdateService"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Actions": {
                "Oem": {
                    "#NvidiaUpdateService.CommitImage": {
                        "@Redfish.ActionInfo": "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.CommitImage/ActionInfo",
                        "target": "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.CommitImage"
                    }
                }
            }
        })))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.CommitImage/ActionInfo",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Parameters": [{
                "Name": "Targets",
                "AllowableValues": ["/redfish/v1/UpdateService/FirmwareInventory/BMC"]
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(
            "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.CommitImage",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Task": "BackgroundCopy"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let special = tmp.path().join("background_copy.json");
    std::fs::write(
        &special,
        r#"{"Targets":["/redfish/v1/UpdateService/FirmwareInventory/BMC"]}"#,
    )
    .unwrap();
    let mut args = powershelf_target_args(&server);
    args.extend([
        "background_copy".to_string(),
        "-s".to_string(),
        special.to_string_lossy().to_string(),
    ]);
    let output = run_nvfwupd(&args, tmp.path());

    assert_stdout(
        &output,
        0,
        &format!(
            "BMC Connection Status: Successful\n\
             \n\
             Querying UpdateService for all allowable targets...\n\
             Found ActionInfo URI: /redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.CommitImage/ActionInfo\n\
             Found 1 allowable target(s):\n\
             {space2}- /redfish/v1/UpdateService/FirmwareInventory/BMC\n\
             Background copy request successful\n\
             Task State:\n\
             {{\n\
             {space4}\"Task\": \"BackgroundCopy\"\n\
             }}\n\
             {DIVIDER}",
            space2 = "  ",
            space4 = "    "
        ),
    );
}

#[test]
fn json_background_update_connection_errors_are_json_only() {
    let tmp = tempfile::tempdir().unwrap();
    let package = tmp.path().join("package.tar");
    write_test_tar(&package, b"purpose=test\n", b"firmware");

    let args = vec![
        "-t".to_string(),
        "ip=127.0.0.1".to_string(),
        "user=test".to_string(),
        "password=secret".to_string(),
        "bmc_ca_cert=/tmp/test-ca.pem".to_string(),
        "verify_tls=false".to_string(),
        "update_fw".to_string(),
        "-p".to_string(),
        package.to_string_lossy().to_string(),
        "-j".to_string(),
        "-b".to_string(),
    ];
    let output = run_nvfwupd(&args, tmp.path());

    assert_eq!(
        output.status.code(),
        Some(1),
        "unexpected exit status: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should be valid JSON only");
    assert_eq!(parsed["Error Code"], 1);
    let errors = parsed["Error"]
        .as_array()
        .expect("Error should be an array");
    assert!(
        errors
            .iter()
            .any(|error| error.as_str().unwrap_or("").contains("bmc_ca_cert")),
        "expected bmc_ca_cert diagnostic in {stdout}"
    );
}

#[tokio::test]
async fn golden_stdout_for_powershelf_unsupported_mock_paths() {
    let tmp = tempfile::tempdir().unwrap();

    let factory_server = MockServer::start().await;
    mount_powershelf_chassis_mock(&factory_server).await;
    let mut factory_args = powershelf_target_args(&factory_server);
    factory_args.push("perform_factory_reset".to_string());
    let factory = run_nvfwupd(&factory_args, tmp.path());
    assert_stdout(
        &factory,
        1,
        "BMC Connection Status: Successful\n\
         Factory reset is not supported for PowerShelf\n\
         Error Code: 1\n",
    );

    let targets_server = MockServer::start().await;
    mount_powershelf_chassis_mock(&targets_server).await;
    let mut targets_args = powershelf_target_args(&targets_server);
    targets_args.push("make_upd_targets".to_string());
    let targets = run_nvfwupd(&targets_args, tmp.path());
    assert_stdout(
        &targets,
        0,
        "make_upd_targets is not supported for this platform\n",
    );
}
