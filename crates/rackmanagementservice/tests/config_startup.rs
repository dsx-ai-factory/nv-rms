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

//! Startup integration tests for the TOML configuration file.
//!
//! These exercise the real `rackmanagementservice` binary end-to-end: a valid
//! insecure config must let the gRPC server bind, while a missing or invalid
//! config must exit non-zero with a clear stderr message before any server is
//! started.

use std::io::Read;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Path to the compiled binary under test (provided by Cargo for integration tests).
const BIN: &str = env!("CARGO_BIN_EXE_rackmanagementservice");

fn write_config(dir: &std::path::Path, contents: &str) -> PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, contents).expect("write config");
    path
}

#[test]
fn missing_config_exits_with_clear_error() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("does-not-exist.toml");

    let output = Command::new(BIN)
        .arg("--config")
        .arg(&missing)
        .output()
        .expect("spawn binary");

    assert!(
        !output.status.success(),
        "process should exit non-zero for a missing config"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("configuration file not found"),
        "stderr should explain the missing file, got: {stderr}"
    );
}

#[test]
fn invalid_config_exits_with_clear_error() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_config(tmp.path(), "port = \"not-a-number\"\n");

    let output = Command::new(BIN)
        .arg("--config")
        .arg(&path)
        .output()
        .expect("spawn binary");

    assert!(
        !output.status.success(),
        "process should exit non-zero for an invalid config"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("configuration error"),
        "stderr should report a configuration error, got: {stderr}"
    );
}

#[test]
fn plaintext_trace_export_to_a_hostname_is_rejected_before_connecting() {
    for (protocol, path) in [("grpc", ""), ("http_protobuf", "/v1/traces")] {
        let collector = TcpListener::bind("127.0.0.1:0").expect("bind collector");
        collector.set_nonblocking(true).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let config = write_config(
            tmp.path(),
            &format!(
                "[tracing]\nendpoint = \"http://localhost:{}{path}\"\nprotocol = \"{protocol}\"\n",
                collector.local_addr().unwrap().port()
            ),
        );

        let output = Command::new(BIN)
            .arg("--config")
            .arg(&config)
            .env_remove("DATABASE_URL")
            .output()
            .expect("spawn binary");

        assert!(!output.status.success(), "{protocol}: plaintext must fail");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("scheme must be https"), "got: {stderr}");
        assert_eq!(
            collector
                .accept()
                .expect_err("must not contact a plaintext collector")
                .kind(),
            std::io::ErrorKind::WouldBlock,
        );
    }
}

#[test]
fn ambient_otel_exporter_settings_are_rejected_without_leaking_values() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_config(
        tmp.path(),
        "[tracing]\nendpoint = \"https://localhost:4317\"\n",
    );

    let variables = [
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_EXPORTER_OTLP_COMPRESSION",
        "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
    ];
    for variable in variables {
        let mut command = Command::new(BIN);
        command.arg("--config").arg(&path);
        for other in variables {
            command.env_remove(other);
        }
        let output = command
            .env(variable, "authorization=collector-secret")
            .output()
            .expect("spawn binary");

        assert!(!output.status.success(), "{variable} must be rejected");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&format!("{variable} is not supported")),
            "stderr should reject ambient OTLP configuration, got: {stderr}"
        );
        assert!(!stderr.contains("collector-secret"));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("collector-secret"));
    }
}

#[test]
fn insecure_config_without_env_gate_exits() {
    let tmp = tempfile::tempdir().unwrap();
    // `tls.insecure = true` alone must not be enough to bind plaintext: the
    // independent RMS_ALLOW_INSECURE=1 gate is required as well.
    let path = write_config(
        tmp.path(),
        "[tls]\ninsecure = true\n\n[switches]\ninsecure_switch = true\n",
    );

    let output = Command::new(BIN)
        .arg("--config")
        .arg(&path)
        // Ensure the gate is absent even if the test environment sets it.
        .env_remove("RMS_ALLOW_INSECURE")
        .output()
        .expect("spawn binary");

    assert!(
        !output.status.success(),
        "process should exit non-zero when the RMS_ALLOW_INSECURE gate is missing"
    );
    // The gate is checked after logging is initialized, so the diagnostic is
    // emitted via the tracing subscriber (stdout) rather than the pre-logging
    // stderr path used for config-parse errors.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        combined.contains("RMS_ALLOW_INSECURE"),
        "output should name the missing env gate, got: {combined}"
    );
}

#[test]
fn valid_insecure_config_binds_grpc_port() {
    assert_insecure_config_binds_grpc_port("");
}

#[test]
fn plaintext_trace_export_to_a_loopback_ip_starts_without_a_collector() {
    // Released at once, so no Collector listens on the endpoint's port.
    let collector_port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve collector port")
        .local_addr()
        .unwrap()
        .port();
    for (protocol, path) in [("grpc", ""), ("http_protobuf", "/v1/traces")] {
        assert_insecure_config_binds_grpc_port(&format!(
            "\n[tracing]\n\
             endpoint = \"http://127.0.0.1:{collector_port}{path}\"\n\
             protocol = \"{protocol}\"\n"
        ));
    }
}

/// Start RMS in dev mode with `extra_config` appended and assert that it binds its gRPC port.
fn assert_insecure_config_binds_grpc_port(extra_config: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let firmware_dir = tmp.path().join("firmware");

    // RMS binds OS-assigned ports directly; releasing test reservations before
    // spawning RMS lets concurrent processes claim those ports first.
    let config = format!(
        "port = 0\n\
         \n\
         [metrics]\n\
         port = 0\n\
         \n\
         [tls]\n\
         insecure = true\n\
         \n\
         [switches]\n\
         insecure_switch = true\n\
         \n\
         [workflows]\n\
         firmware_dir = {firmware:?}\n\
         {extra_config}",
        firmware = firmware_dir.to_string_lossy(),
    );

    let path = write_config(tmp.path(), &config);
    let stdout_path = tmp.path().join("stdout.log");
    let stdout = std::fs::File::create(&stdout_path).expect("create service log");

    let mut child = Command::new(BIN)
        .arg("--config")
        .arg(&path)
        .env_remove("DATABASE_URL")
        // Plaintext gRPC requires the independent env gate in addition to
        // `insecure = true`; supply it here for this dev-mode startup test.
        .env("RMS_ALLOW_INSECURE", "1")
        // Capture RMS logs in a file so startup failures retain diagnostics
        // without risking a full pipe buffer blocking the long-running child.
        .stdout(stdout)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn binary");

    // Poll the gRPC port until it accepts connections or we time out.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut bound = false;

    while Instant::now() < deadline {
        // If the process died early, stop waiting and surface its output.
        if let Some(status) = child.try_wait().expect("try_wait") {
            let mut stderr = String::new();
            if let Some(mut e) = child.stderr.take() {
                let _ = e.read_to_string(&mut stderr);
            }

            let stdout = std::fs::read_to_string(&stdout_path).expect("read service log");

            panic!("service exited early with {status}:\nstdout:\n{stdout}\nstderr:\n{stderr}");
        }

        // The startup record includes the port from the already-bound listener.
        let stdout = std::fs::read_to_string(&stdout_path).expect("read service log");

        let grpc_port = stdout
            .lines()
            .find(|line| line.contains("gRPC server starting WITHOUT TLS"))
            .and_then(|line| {
                line.split_whitespace()
                    .find_map(|field| field.strip_prefix("port="))
            })
            .and_then(|port| port.parse::<u16>().ok());

        if grpc_port.is_some_and(|port| std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()) {
            bound = true;
            break;
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    if bound {
        assert!(
            child.try_wait().expect("try_wait").is_none(),
            "service bound its gRPC port but exited immediately"
        );
    }

    // Always tear the child down before asserting so we never leak a process.
    let _ = child.kill();
    let _ = child.wait();

    let stdout = std::fs::read_to_string(&stdout_path).expect("read service log");

    assert!(
        bound,
        "service did not bind its gRPC port within the timeout:\n{stdout}"
    );
}
