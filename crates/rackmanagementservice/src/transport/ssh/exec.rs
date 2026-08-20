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

//! Remote command execution and output validation.
//!
//! This module treats remote command completion state as part of the SSH API:
//! missing exit status, signal termination, and non-zero exit status all become
//! failures even when stdout was captured successfully.

use std::time::Duration;

use russh::ChannelMsg;

use super::client::SshClient;
use super::error::{SshError, SshResult};
use super::logging::{scrub_sensitive_ssh_text, ssh_command_log_value};
use super::request::{ChannelRequestFailureCode, wait_for_channel_request_confirmation};
use crate::utilities::error::Result;

/// Maximum remote output included in returned error messages.
pub(super) const SSH_OUTPUT_CONTEXT_MAX_CHARS: usize = 512;

/// Captured output and completion state for one remote command.
///
/// `russh` delivers stdout, stderr, exit status, and signal notifications as
/// independent channel messages. This struct keeps those pieces together until
/// the command can be validated as a whole.
#[derive(Debug, Default)]
pub(super) struct SshExecOutput {
    /// Bytes received on the command's stdout stream.
    pub(super) stdout: Vec<u8>,

    /// Bytes received on the command's stderr stream.
    pub(super) stderr: Vec<u8>,

    /// Remote process exit status, if the server reported one.
    pub(super) exit_status: Option<u32>,

    /// Remote signal termination, if the server reported one.
    pub(super) exit_signal: Option<String>,
}

impl SshExecOutput {
    /// Validate completion state and return stdout for successful commands.
    pub(super) fn into_stdout(self, host: &str, command: &str) -> SshResult<String> {
        let command = ssh_command_log_value(command);

        if let Some(signal) = &self.exit_signal {
            tracing::warn!(
                host,
                command = &command,
                signal,
                stdout_bytes = self.stdout.len(),
                stderr_bytes = self.stderr.len(),
                "SSH command terminated by signal"
            );

            return Err(SshError::CommandTerminatedBySignal {
                host: host.to_owned(),
                command,
                signal: signal.to_owned(),
                output_context: ssh_output_context(&self.stdout, &self.stderr),
            });
        }

        let Some(exit_status) = self.exit_status else {
            tracing::warn!(
                host,
                command = &command,
                stdout_bytes = self.stdout.len(),
                stderr_bytes = self.stderr.len(),
                "SSH command completed without exit status"
            );

            return Err(SshError::CommandMissingExitStatus {
                host: host.to_owned(),
                command,
                output_context: ssh_output_context(&self.stdout, &self.stderr),
            });
        };

        if exit_status != 0 {
            tracing::warn!(
                host,
                command = &command,
                exit_status,
                stdout_bytes = self.stdout.len(),
                stderr_bytes = self.stderr.len(),
                "SSH command failed"
            );

            return Err(SshError::CommandFailed {
                host: host.to_owned(),
                command,
                exit_status,
                output_context: ssh_output_context(&self.stdout, &self.stderr),
            });
        }

        tracing::debug!(
            host,
            command = &command,
            exit_status,
            stdout_bytes = self.stdout.len(),
            stderr_bytes = self.stderr.len(),
            "SSH command completed"
        );

        Ok(String::from_utf8_lossy(&self.stdout).into_owned())
    }
}

impl SshClient {
    /// Execute a remote command and return its stdout output.
    ///
    /// Captures stdout and stderr. Returns an error if the remote command exits
    /// non-zero, terminates by signal, or does not report an exit status.
    /// Returns `Timeout` if the command doesn't complete within the deadline.
    ///
    /// # Errors
    ///
    /// Returns `FailedPrecondition` if the client is not connected, `Timeout`
    /// if execution exceeds `timeout`, `Unavailable` if the SSH channel cannot
    /// be opened, or `Internal` if the remote command fails or reports an
    /// invalid completion state.
    pub async fn exec(&self, command: &str, timeout: Duration) -> Result<String> {
        let handle = self.handle.as_ref().ok_or(SshError::NotConnected)?;

        let host = self.host.as_str();
        let command_log = ssh_command_log_value(command);
        let timeout_secs = timeout.as_secs();

        tracing::debug!(
            host,
            command = &command_log,
            timeout_secs,
            "SSH command starting"
        );

        let stdout = tokio::time::timeout(timeout, async {
            let mut channel = handle.channel_open_session().await.map_err(|e| {
                let error = e.to_string();
                tracing::warn!(
                    host,
                    command = &command_log,
                    error = &error,
                    "SSH channel open failed"
                );

                SshError::Unavailable {
                    operation: "command execution",
                    details: error,
                }
            })?;

            channel.exec(true, command).await.map_err(|e| {
                let error = e.to_string();
                tracing::warn!(
                    host,
                    command = &command_log,
                    error = &error,
                    "SSH exec request failed"
                );

                SshError::OperationFailed {
                    operation: "exec request",
                    details: error,
                }
            })?;

            let mut output = SshExecOutput::default();
            let deferred_messages = wait_for_channel_request_confirmation(
                &mut channel,
                "exec request",
                ChannelRequestFailureCode::OperationFailed,
            )
            .await?;

            for message in deferred_messages {
                if record_exec_channel_message(&mut output, message) {
                    break;
                }
            }

            loop {
                let Some(message) = channel.wait().await else {
                    break;
                };

                if record_exec_channel_message(&mut output, message) {
                    break;
                }
            }

            output.into_stdout(host, command)
        })
        .await
        .map_err(|_| {
            tracing::warn!(
                host,
                command = &command_log,
                timeout_secs,
                "SSH command timed out"
            );
            SshError::timeout("command execution", host, timeout)
        })??;

        Ok(stdout)
    }
}

/// Record one command channel message.
///
/// Returns `true` when command output collection should stop.
fn record_exec_channel_message(output: &mut SshExecOutput, message: ChannelMsg) -> bool {
    match message {
        ChannelMsg::Data { data } => {
            output.stdout.extend_from_slice(&data);
            false
        }
        ChannelMsg::ExtendedData { data, .. } => {
            output.stderr.extend_from_slice(&data);
            false
        }
        ChannelMsg::ExitStatus { exit_status } => {
            output.exit_status = Some(exit_status);
            false
        }
        ChannelMsg::ExitSignal {
            signal_name,
            error_message,
            ..
        } => {
            output.exit_signal = Some(format!("{signal_name:?}: {error_message}"));
            false
        }
        ChannelMsg::Close => true,
        _ => false,
    }
}

/// Build a short diagnostic suffix, preferring stderr over stdout.
fn ssh_output_context(stdout: &[u8], stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr);

    if let Some(stderr) = ssh_output_preview(&stderr) {
        return format!("; stderr: {stderr}");
    }

    let stdout = String::from_utf8_lossy(stdout);

    if let Some(stdout) = ssh_output_preview(&stdout) {
        return format!("; stdout: {stdout}");
    }

    String::new()
}

/// Redact and clip remote output before it is returned in an error message.
fn ssh_output_preview(output: &str) -> Option<String> {
    let output = output.trim();

    if output.is_empty() {
        return None;
    }

    // Remote output can be large or echo sensitive command arguments. Keep
    // returned errors useful for operators without turning them into raw logs.
    let redacted = scrub_sensitive_ssh_text(output, "");
    let single_line = redacted.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = single_line.chars();
    let clipped: String = chars.by_ref().take(SSH_OUTPUT_CONTEXT_MAX_CHARS).collect();

    if chars.next().is_some() {
        Some(format!("{clipped}..."))
    } else {
        Some(clipped)
    }
}
