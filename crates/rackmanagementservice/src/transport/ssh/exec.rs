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
        self.exec_command(command, timeout, false).await
    }

    /// Execute a command that reboots the host, permitting a missing reply or
    /// signal termination after the exec request is queued. Queueing does not
    /// prove acceptance; callers must independently verify completion, even
    /// when the request confirmation never arrives. Explicit request rejection
    /// and nonzero exit status still fail.
    pub(crate) async fn exec_for_reboot(&self, command: &str, timeout: Duration) -> Result<String> {
        self.exec_command(command, timeout, true).await
    }

    // Share request confirmation and output validation with ordinary commands,
    // whose strict completion contract remains unchanged.
    async fn exec_command(
        &self,
        command: &str,
        timeout: Duration,
        rebooting: bool,
    ) -> Result<String> {
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

        let mut request_queued = false;

        let outcome = tokio::time::timeout(timeout, async {
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

            request_queued = true;

            let mut output = SshExecOutput::default();
            let deferred_messages = wait_for_channel_request_confirmation(
                &mut channel,
                "exec request",
                if rebooting {
                    ChannelRequestFailureCode::RebootingCommand
                } else {
                    ChannelRequestFailureCode::OperationFailed
                },
            )
            .await?;

            let mut finished = false;

            for message in deferred_messages {
                finished = record_exec_channel_message(&mut output, message, rebooting);

                if finished {
                    break;
                }
            }

            while !finished {
                let Some(message) = channel.wait().await else {
                    break;
                };

                finished = record_exec_channel_message(&mut output, message, rebooting);
            }

            output.into_stdout(host, command)
        })
        .await;

        match outcome {
            Ok(Ok(stdout)) => Ok(stdout),
            Ok(Err(SshError::CommandMissingExitStatus { .. })) if rebooting && request_queued => {
                tracing::info!(
                    host,
                    "SSH reboot command connection closed; completion requires polling"
                );

                Ok(String::new())
            }
            Ok(Err(SshError::CommandTerminatedBySignal { signal, .. }))
                if rebooting && request_queued =>
            {
                tracing::info!(
                    host,
                    signal,
                    "SSH reboot command terminated by signal; completion requires polling"
                );

                Ok(String::new())
            }
            Err(_) if rebooting && request_queued => {
                tracing::info!(
                    host,
                    "SSH reboot command reply timed out; acceptance is uncertain and completion requires polling"
                );

                Ok(String::new())
            }
            Ok(Err(error)) => Err(error.into()),
            Err(_) => {
                tracing::warn!(
                    host,
                    command = &command_log,
                    timeout_secs,
                    "SSH command timed out"
                );

                Err(SshError::timeout("command execution", host, timeout).into())
            }
        }
    }
}

/// Record one command channel message.
///
/// Returns `true` when command output collection should stop.
fn record_exec_channel_message(
    output: &mut SshExecOutput,
    message: ChannelMsg,
    rebooting: bool,
) -> bool {
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
            rebooting && exit_status != 0
        }
        ChannelMsg::ExitSignal {
            signal_name,
            error_message,
            ..
        } => {
            output.exit_signal = Some(format!("{signal_name:?}: {error_message}"));
            rebooting
        }
        ChannelMsg::Close => true,
        _ => false,
    }
}

/// Build a short diagnostic suffix, preferring stderr over stdout.
pub(super) fn ssh_output_context(stdout: &[u8], stderr: &[u8]) -> String {
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
