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

use std::time::Duration;

use russh::ChannelMsg;
use secrecy::ExposeSecret;
use tokio_util::sync::CancellationToken;

use super::error::SshError;
use super::exec::{SSH_OUTPUT_CONTEXT_MAX_CHARS, SshExecOutput};
use super::logging::{
    SSH_LOG_COMMAND_MAX_CHARS, scrub_password_recovery_transcript, ssh_command_log_value,
};
use super::request::{ChannelRequestEvent, channel_request_event};
use super::{SshClient, SshEndpoint};
use crate::utilities::error::{ErrorCode, RmsError};

fn disconnected_client() -> SshClient {
    SshClient {
        host: "10.0.0.1".to_owned(),
        handle: None,
    }
}

#[test]
fn ssh_endpoint_stores_credentials() {
    let endpoint = SshEndpoint::new("10.0.0.1", "admin", "secret");

    assert_eq!(endpoint.host, "10.0.0.1");
    assert_eq!(endpoint.username, "admin");
    assert_eq!(endpoint.password.expose_secret(), "secret");
}

#[tokio::test]
async fn close_without_connect_succeeds() {
    let client = disconnected_client();

    client.close().await.unwrap();
}

#[test]
fn password_recovery_transcript_redacts_echoed_passwords() {
    let transcript = "\
New password: new pass 123
new pass 123
Retype new password:
new pass 123
password=other echoed secret
Retype new password:
password unchanged
authentication token manipulation error";

    let scrubbed = scrub_password_recovery_transcript(transcript, "new pass 123");

    assert!(!scrubbed.contains("new pass 123"));
    assert!(!scrubbed.contains("other echoed secret"));
    assert!(scrubbed.contains("New password: XXXX"));
    assert!(scrubbed.contains("password=XXXX"));
    assert!(scrubbed.contains("Retype new password:\nXXXX"));
    assert!(scrubbed.contains("Retype new password:\npassword unchanged"));
}

#[test]
fn ssh_command_log_value_redacts_and_caps_commands() {
    struct TestCase {
        name: &'static str,
        command: String,
        expected: String,
    }

    let cases = [
        TestCase {
            name: "plain command",
            command: "nv config apply --assume-yes".to_owned(),
            expected: "nv config apply --assume-yes".to_owned(),
        },
        TestCase {
            name: "password field",
            command: "nv set system password=super-secret".to_owned(),
            expected: "nv set system password=XXXX".to_owned(),
        },
        TestCase {
            name: "long password option",
            command: "tool --password super-secret --flag keep".to_owned(),
            expected: "tool --password XXXX --flag keep".to_owned(),
        },
        TestCase {
            name: "bare password argument",
            command: "password super-secret".to_owned(),
            expected: "password XXXX".to_owned(),
        },
        TestCase {
            name: "quoted password option",
            command: "tool --passwd 'super secret' --flag keep".to_owned(),
            expected: "tool --passwd XXXX --flag keep".to_owned(),
        },
        TestCase {
            name: "long command",
            command: format!("{}{}", "x".repeat(SSH_LOG_COMMAND_MAX_CHARS), "tail"),
            expected: format!("{}...", "x".repeat(SSH_LOG_COMMAND_MAX_CHARS)),
        },
    ];

    for case in cases {
        let actual = ssh_command_log_value(&case.command);
        assert_eq!(actual, case.expected, "{}", case.name);
    }
}

#[test]
fn channel_request_event_classifies_confirmation_messages() {
    struct TestCase {
        name: &'static str,
        message: Option<ChannelMsg>,
        expected: &'static str,
    }

    let cases = [
        TestCase {
            name: "request accepted",
            message: Some(ChannelMsg::Success),
            expected: "confirmed",
        },
        TestCase {
            name: "request rejected",
            message: Some(ChannelMsg::Failure),
            expected: "rejected",
        },
        TestCase {
            name: "channel closed",
            message: Some(ChannelMsg::Close),
            expected: "closed",
        },
        TestCase {
            name: "receiver closed",
            message: None,
            expected: "closed",
        },
        TestCase {
            name: "non-confirmation message",
            message: Some(ChannelMsg::ExitStatus { exit_status: 0 }),
            expected: "deferred",
        },
    ];

    for case in cases {
        let actual = match channel_request_event(case.message) {
            ChannelRequestEvent::Confirmed => "confirmed",
            ChannelRequestEvent::Rejected { details } => {
                assert!(!details.is_empty(), "{}", case.name);
                "rejected"
            }
            ChannelRequestEvent::Closed { details } => {
                assert!(!details.is_empty(), "{}", case.name);
                "closed"
            }
            ChannelRequestEvent::Deferred(_) => "deferred",
        };

        assert_eq!(actual, case.expected, "{}", case.name);
    }
}

#[tokio::test]
async fn exec_without_connect_returns_failed_precondition() {
    let client = disconnected_client();

    let err = client
        .exec("ls", SshClient::DEFAULT_TIMEOUT)
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
}

#[test]
fn exec_output_rejects_non_zero_exit_status_with_stderr() {
    let output = SshExecOutput {
        stdout: b"ignored stdout".to_vec(),
        stderr: b"nv action failed".to_vec(),
        exit_status: Some(1),
        exit_signal: None,
    };

    let err = output
        .into_stdout("10.0.0.1", "nv action import")
        .unwrap_err();

    let err = RmsError::from(err);

    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("10.0.0.1"));
    assert!(err.message.contains("exit status 1"));
    assert!(err.message.contains("nv action failed"));
}

#[test]
fn exec_output_rejects_signal_with_stderr_context() {
    let output = SshExecOutput {
        stdout: Vec::new(),
        stderr: b"terminated".to_vec(),
        exit_status: None,
        exit_signal: Some("KILL: killed".to_owned()),
    };

    let err = output.into_stdout("10.0.0.1", "reboot").unwrap_err();
    let err = RmsError::from(err);

    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("terminated by signal KILL: killed"));
    assert!(err.message.contains("stderr: terminated"));
}

#[test]
fn exec_output_rejects_missing_exit_status_with_stdout_context() {
    let output = SshExecOutput {
        stdout: b"partial output".to_vec(),
        stderr: Vec::new(),
        exit_status: None,
        exit_signal: None,
    };

    let err = output.into_stdout("10.0.0.1", "show version").unwrap_err();
    let err = RmsError::from(err);

    assert_eq!(err.code, ErrorCode::Internal);
    assert!(err.message.contains("completed without exit status"));
    assert!(err.message.contains("stdout: partial output"));
}

#[test]
fn exec_output_error_context_redacts_and_caps_remote_output() {
    let error_for_stderr = |stderr| {
        let output = SshExecOutput {
            stdout: Vec::new(),
            stderr,
            exit_status: Some(1),
            exit_signal: None,
        };

        output
            .into_stdout("10.0.0.1", "nv config apply")
            .map_err(RmsError::from)
            .unwrap_err()
    };

    let stderr = format!(
        "password=super-secret\n{}tail-marker",
        "x".repeat(SSH_OUTPUT_CONTEXT_MAX_CHARS + 50)
    );

    let err = error_for_stderr(stderr.into_bytes());

    assert!(err.message.contains("password=XXXX"));
    assert!(err.message.contains("..."));
    assert!(!err.message.contains("super-secret"));
    assert!(!err.message.contains("tail-marker"));

    let err = error_for_stderr(b"tool --password super-secret --flag keep".to_vec());

    assert!(err.message.contains("--password XXXX --flag keep"));
    assert!(!err.message.contains("super-secret"));
}

#[test]
fn exec_output_returns_stdout_for_zero_exit_status() {
    let output = SshExecOutput {
        stdout: b"ok\n".to_vec(),
        stderr: Vec::new(),
        exit_status: Some(0),
        exit_signal: None,
    };

    let stdout = output.into_stdout("10.0.0.1", "echo ok").unwrap();
    assert_eq!(stdout, "ok\n");
}

#[test]
fn ssh_error_conversion_preserves_rms_codes() {
    struct TestCase {
        error: SshError,
        expected: ErrorCode,
    }

    let cases = [
        TestCase {
            error: SshError::NotConnected,
            expected: ErrorCode::FailedPrecondition,
        },
        TestCase {
            error: SshError::Timeout {
                operation: "connect",
                host: "10.0.0.1".to_owned(),
                timeout_secs: 30,
            },
            expected: ErrorCode::Timeout,
        },
        TestCase {
            error: SshError::ConnectFailed {
                host: "10.0.0.1".to_owned(),
                details: "refused".to_owned(),
            },
            expected: ErrorCode::ConnectionRefused,
        },
        TestCase {
            error: SshError::AuthFailed {
                host: "10.0.0.1".to_owned(),
                details: "authentication failed".to_owned(),
            },
            expected: ErrorCode::InvalidArgument,
        },
        TestCase {
            error: SshError::Unavailable {
                operation: "command execution",
                details: "closed".to_owned(),
            },
            expected: ErrorCode::Unavailable,
        },
        TestCase {
            error: SshError::Cancelled {
                operation: "SFTP remote file write",
                host: "10.0.0.1".to_owned(),
            },
            expected: ErrorCode::Cancelled,
        },
        TestCase {
            error: SshError::RemoteFileNotFound {
                path: "/tmp/fw.bin".to_owned(),
                details: "missing".to_owned(),
            },
            expected: ErrorCode::NotFound,
        },
        TestCase {
            error: SshError::CommandMissingExitStatus {
                host: "10.0.0.1".to_owned(),
                command: "show version".to_owned(),
                output_context: String::new(),
            },
            expected: ErrorCode::Internal,
        },
    ];

    for case in cases {
        let err = RmsError::from(case.error);
        assert_eq!(err.code, case.expected);
    }
}

#[tokio::test]
async fn sftp_upload_without_connect_returns_failed_precondition() {
    let client = disconnected_client();

    let err = client
        .sftp_upload(
            "/tmp/fw.bin",
            "/remote/fw.bin",
            SshClient::UPLOAD_TIMEOUT,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
}

#[tokio::test]
async fn sftp_file_size_without_connect_returns_failed_precondition() {
    let client = disconnected_client();
    let err = client.sftp_file_size("/remote/fw.bin").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::FailedPrecondition);
}

#[tokio::test]
async fn complete_expired_password_change_without_connect_returns_failed_precondition() {
    let client = disconnected_client();

    let err = client
        .complete_expired_password_change("newpass", Duration::from_secs(1))
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
}

#[tokio::test]
async fn complete_expired_password_change_empty_password_rejected_precondition() {
    // Without a connected session, the client fails before validating the new
    // password. Keep this behavior stable for callers that check connection
    // state first.
    let client = disconnected_client();

    let err = client
        .complete_expired_password_change("", Duration::from_secs(1))
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::FailedPrecondition);
}
