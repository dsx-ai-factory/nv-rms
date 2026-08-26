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

/// Reserve two distinct OS-assigned localhost ports. Returns the still-open
/// listeners rather than just the port numbers: the caller should hold them
/// until immediately before the port numbers are handed to the process that
/// will actually bind them, so the time-of-check/time-of-use window in which
/// another process could steal either port stays as small as possible.
/// Binding both simultaneously guarantees the resulting ports differ.
fn two_free_ports() -> (TcpListener, TcpListener) {
    let a = TcpListener::bind("127.0.0.1:0").expect("bind a");
    let b = TcpListener::bind("127.0.0.1:0").expect("bind b");
    (a, b)
}

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
    // Unknown key is rejected by deny_unknown_fields.
    let path = write_config(tmp.path(), "definitely_not_a_real_key = 123\n");

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
    let tmp = tempfile::tempdir().unwrap();
    let firmware_dir = tmp.path().join("firmware");
    let (grpc_listener, metrics_listener) = two_free_ports();
    let grpc_port = grpc_listener.local_addr().unwrap().port();
    let metrics_port = metrics_listener.local_addr().unwrap().port();

    let config = format!(
        "port = {grpc_port}\n\
         \n\
         [metrics]\n\
         port = {metrics_port}\n\
         \n\
         [tls]\n\
         insecure = true\n\
         \n\
         [switches]\n\
         insecure_switch = true\n\
         \n\
         [workflows]\n\
         firmware_dir = {firmware:?}\n",
        firmware = firmware_dir.to_string_lossy(),
    );
    let path = write_config(tmp.path(), &config);

    // Hold both listeners open through config generation (file I/O of
    // unpredictable duration) and release them only immediately before
    // spawning the child that will rebind them, minimizing the window in
    // which a concurrent process could steal either port.
    drop(grpc_listener);
    drop(metrics_listener);

    let mut child = Command::new(BIN)
        .arg("--config")
        .arg(&path)
        .env_remove("DATABASE_URL")
        // Plaintext gRPC requires the independent env gate in addition to
        // `insecure = true`; supply it here for this dev-mode startup test.
        .env("RMS_ALLOW_INSECURE", "1")
        // This child runs until we kill it, and RMS logs to stdout. Discard
        // stdout rather than piping it: nothing reads it, so a full pipe buffer
        // could otherwise block the process. stderr stays piped for the
        // early-exit diagnostic below.
        .stdout(Stdio::null())
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
            panic!("service exited early with {status}: {stderr}");
        }
        if std::net::TcpStream::connect(("127.0.0.1", grpc_port)).is_ok() {
            bound = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    if bound {
        assert!(
            child.try_wait().expect("try_wait").is_none(),
            "service bound gRPC port {grpc_port} but exited immediately"
        );
    }

    // Always tear the child down before asserting so we never leak a process.
    let _ = child.kill();
    let _ = child.wait();

    assert!(
        bound,
        "service did not bind gRPC port {grpc_port} within the timeout"
    );
}
