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

//! Interactive expired-password recovery over an SSH PTY.
//!
//! Some switches force a password change during first login or after password
//! expiry. The regular command-exec path cannot drive that PAM prompt flow, so
//! this module opens a PTY shell and responds to the expected password prompts.

use std::time::Duration;

use russh::{Channel, ChannelMsg, client};

use super::client::{SshClient, SshHandler};
use super::error::{SshError, SshResult};
use super::logging::scrub_password_recovery_transcript;
use super::request::{ChannelRequestFailureCode, wait_for_channel_request_confirmation};
use crate::utilities::error::Result;

/// Prompt currently waiting for the replacement password.
#[derive(Clone, Copy)]
enum PasswordRecoveryPrompt {
    /// Initial `New password:` prompt.
    New,

    /// Confirmation prompt for the same password.
    Retype,
}

/// Result of reading one message from the password-recovery channel.
enum PasswordRecoveryChannelEvent {
    /// Prompt output was appended to the transcript.
    PromptOutput,

    /// The remote side closed the channel or stopped sending messages.
    Closed,

    /// Message did not affect the prompt state.
    Ignored,
}

/// Outcome after applying one password-recovery channel event.
enum PasswordRecoveryStep {
    /// Continue reading the prompt flow.
    Continue,

    /// The password-recovery flow has completed.
    Completed,
}

/// Local state for the interactive expired-password prompt flow.
///
/// The transcript is accumulated so prompt detection can handle fragmented SSH
/// channel messages. It is scrubbed before being returned in any error.
#[derive(Default)]
struct PasswordRecoveryState {
    /// Raw channel output accumulated for prompt detection.
    transcript: String,

    /// Whether the initial password prompt has been answered.
    answered_new_prompt: bool,

    /// Whether the confirmation password prompt has been answered.
    answered_retype_prompt: bool,
}

impl PasswordRecoveryState {
    /// Append prompt output to the transcript.
    fn append(&mut self, data: &[u8]) {
        self.transcript.push_str(&String::from_utf8_lossy(data));
    }

    /// Return a lower-case copy used for case-insensitive prompt matching.
    fn lower_transcript(&self) -> String {
        self.transcript.to_ascii_lowercase()
    }

    /// Determine whether the next password prompt is ready to answer.
    fn next_prompt(&self, lower: &str) -> Option<PasswordRecoveryPrompt> {
        if !self.answered_new_prompt && lower.contains("new password:") {
            return Some(PasswordRecoveryPrompt::New);
        }

        if self.answered_new_prompt
            && !self.answered_retype_prompt
            && (lower.contains("retype new password:")
                || lower.contains("re-enter new password:")
                || lower.contains("retype password:"))
        {
            return Some(PasswordRecoveryPrompt::Retype);
        }

        None
    }

    /// Record that a password prompt has been answered.
    fn mark_answered(&mut self, prompt: PasswordRecoveryPrompt) {
        match prompt {
            PasswordRecoveryPrompt::New => {
                self.answered_new_prompt = true;
            }
            PasswordRecoveryPrompt::Retype => {
                self.answered_retype_prompt = true;
            }
        }
    }

    /// Return the transcript with password material removed.
    fn scrubbed_transcript(&self, new_password: &str) -> String {
        scrub_password_recovery_transcript(&self.transcript, new_password)
    }
}

impl SshClient {
    /// Complete the forced first-login password change over an interactive SSH
    /// PTY session.
    ///
    /// Opens a shell channel, requests a PTY, then drives the NVOS
    /// expired-password recovery prompt flow:
    ///
    /// 1. Wait for "New password:" and send `new_password`.
    /// 2. Wait for "Retype new password:" or equivalent and send it again.
    /// 3. Treat a success marker, or EOF after the retype step, as completion.
    ///
    /// Returns an error on explicit failure messages such as "authentication
    /// token manipulation error" or on timeout.
    ///
    /// # Errors
    ///
    /// Returns `FailedPrecondition` if the client is not connected,
    /// `InvalidArgument` if `new_password` is empty, `Timeout` if the prompt
    /// flow does not finish within `timeout`, or `Internal`/`Unavailable` for
    /// SSH channel and prompt-flow failures.
    pub async fn complete_expired_password_change(
        &self,
        new_password: &str,
        timeout: Duration,
    ) -> Result<()> {
        let handle = self.handle.as_ref().ok_or(SshError::NotConnected)?;

        if new_password.is_empty() {
            return Err(SshError::EmptyPassword.into());
        }

        let host = self.host.as_str();
        let timeout_secs = timeout.as_secs();

        tracing::debug!(host, timeout_secs, "SSH password recovery starting");

        tokio::time::timeout(timeout, async {
            let (mut channel, deferred_messages) =
                open_password_recovery_channel(handle, host).await?;

            let mut state = PasswordRecoveryState::default();

            for message in deferred_messages {
                let event = password_recovery_event(message, &mut state);

                match handle_password_recovery_event(
                    event,
                    &mut channel,
                    host,
                    &mut state,
                    new_password,
                )
                .await?
                {
                    PasswordRecoveryStep::Continue => {}
                    PasswordRecoveryStep::Completed => return Ok::<(), SshError>(()),
                }
            }

            loop {
                let event = read_password_recovery_event(&mut channel, &mut state).await;

                match handle_password_recovery_event(
                    event,
                    &mut channel,
                    host,
                    &mut state,
                    new_password,
                )
                .await?
                {
                    PasswordRecoveryStep::Continue => {}
                    PasswordRecoveryStep::Completed => return Ok::<(), SshError>(()),
                }
            }
        })
        .await
        .map_err(|_| {
            tracing::warn!(host, timeout_secs, "SSH password recovery timed out");
            SshError::timeout("password recovery", host, timeout)
        })??;

        Ok(())
    }
}

/// Return whether the transcript contains a successful password-change marker.
fn has_password_recovery_success(lower: &str) -> bool {
    lower.contains("password updated successfully")
        || lower.contains("passwd updated successfully")
        || lower.contains("password changed")
}

/// Return whether the transcript contains a known password-change failure.
fn has_password_recovery_failure(lower: &str) -> bool {
    lower.contains("authentication token manipulation error")
        || lower.contains("bad password")
        || lower.contains("password unchanged")
        || lower.contains("failure setting password")
}

/// Apply one channel message to the password-recovery state machine.
fn password_recovery_event(
    message: ChannelMsg,
    state: &mut PasswordRecoveryState,
) -> PasswordRecoveryChannelEvent {
    match message {
        ChannelMsg::Data { ref data } | ChannelMsg::ExtendedData { ref data, .. } => {
            state.append(data);
            PasswordRecoveryChannelEvent::PromptOutput
        }
        ChannelMsg::Eof | ChannelMsg::Close => PasswordRecoveryChannelEvent::Closed,
        _ => PasswordRecoveryChannelEvent::Ignored,
    }
}

/// Read one channel message and update the prompt transcript when applicable.
async fn read_password_recovery_event(
    channel: &mut Channel<client::Msg>,
    state: &mut PasswordRecoveryState,
) -> PasswordRecoveryChannelEvent {
    match channel.wait().await {
        Some(message) => password_recovery_event(message, state),
        None => PasswordRecoveryChannelEvent::Closed,
    }
}

/// Apply prompt-flow policy after a channel event updates local state.
async fn handle_password_recovery_event(
    event: PasswordRecoveryChannelEvent,
    channel: &mut Channel<client::Msg>,
    host: &str,
    state: &mut PasswordRecoveryState,
    new_password: &str,
) -> SshResult<PasswordRecoveryStep> {
    match event {
        PasswordRecoveryChannelEvent::PromptOutput => {
            handle_password_recovery_prompt(channel, host, state, new_password).await
        }
        PasswordRecoveryChannelEvent::Closed => {
            handle_password_recovery_close(channel, host, state, new_password).await?;
            Ok(PasswordRecoveryStep::Completed)
        }
        PasswordRecoveryChannelEvent::Ignored => Ok(PasswordRecoveryStep::Continue),
    }
}

/// Respond to prompts or finish when the transcript reaches a terminal state.
async fn handle_password_recovery_prompt(
    channel: &mut Channel<client::Msg>,
    host: &str,
    state: &mut PasswordRecoveryState,
    new_password: &str,
) -> SshResult<PasswordRecoveryStep> {
    let lower = state.lower_transcript();

    if has_password_recovery_success(&lower) {
        tracing::debug!(host, "SSH password recovery completed");
        close_password_recovery_channel(channel).await;
        return Ok(PasswordRecoveryStep::Completed);
    }

    if has_password_recovery_failure(&lower) {
        close_password_recovery_channel(channel).await;

        let details = state.scrubbed_transcript(new_password);

        tracing::warn!(
            host,
            transcript_bytes = details.len(),
            "SSH password recovery failed"
        );

        return Err(SshError::PasswordRecoveryFailed { details });
    }

    if let Some(prompt) = state.next_prompt(&lower) {
        send_password_recovery_line(channel, host, new_password).await?;
        state.mark_answered(prompt);
    }

    Ok(PasswordRecoveryStep::Continue)
}

/// Complete or fail the recovery flow when the remote channel closes.
async fn handle_password_recovery_close(
    channel: &mut Channel<client::Msg>,
    host: &str,
    state: &PasswordRecoveryState,
    new_password: &str,
) -> SshResult<()> {
    if state.answered_retype_prompt {
        tracing::debug!(host, "SSH password recovery completed");
        close_password_recovery_channel(channel).await;
        return Ok(());
    }

    let transcript = state.scrubbed_transcript(new_password);

    tracing::warn!(
        host,
        answered_new_prompt = state.answered_new_prompt,
        answered_retype_prompt = state.answered_retype_prompt,
        transcript_bytes = transcript.len(),
        "SSH password recovery closed before completion"
    );

    Err(SshError::PasswordRecoveryFailed {
        details: format!("closed before completion; transcript: {transcript}"),
    })
}

/// Best-effort close for a password-recovery channel.
async fn close_password_recovery_channel(channel: &mut Channel<client::Msg>) {
    let _ = channel.eof().await;
    let _ = channel.close().await;
}

/// Open a PTY shell channel for the expired-password prompt flow.
async fn open_password_recovery_channel(
    handle: &client::Handle<SshHandler>,
    host: &str,
) -> SshResult<(Channel<client::Msg>, Vec<ChannelMsg>)> {
    let mut channel = handle.channel_open_session().await.map_err(|e| {
        let error = e.to_string();
        tracing::warn!(
            host,
            error = &error,
            "SSH password recovery channel open failed"
        );

        SshError::Unavailable {
            operation: "password recovery",
            details: error,
        }
    })?;

    // Request a PTY so the switch's PAM expired-password flow prompts.
    channel
        .request_pty(true, "xterm", 80, 24, 0, 0, &[])
        .await
        .map_err(|e| {
            let error = e.to_string();
            tracing::warn!(
                host,
                error = &error,
                "SSH password recovery PTY request failed"
            );

            SshError::OperationFailed {
                operation: "password recovery PTY request",
                details: error,
            }
        })?;

    let mut deferred_messages = wait_for_channel_request_confirmation(
        &mut channel,
        "password recovery PTY request",
        ChannelRequestFailureCode::OperationFailed,
    )
    .await?;

    channel.request_shell(true).await.map_err(|e| {
        let error = e.to_string();
        tracing::warn!(
            host,
            error = &error,
            "SSH password recovery shell request failed"
        );

        SshError::OperationFailed {
            operation: "password recovery shell request",
            details: error,
        }
    })?;

    deferred_messages.extend(
        wait_for_channel_request_confirmation(
            &mut channel,
            "password recovery shell request",
            ChannelRequestFailureCode::OperationFailed,
        )
        .await?,
    );

    Ok((channel, deferred_messages))
}

/// Send one password response line to the interactive prompt.
async fn send_password_recovery_line(
    channel: &mut Channel<client::Msg>,
    host: &str,
    new_password: &str,
) -> SshResult<()> {
    let line = format!("{new_password}\r");
    channel.data(line.as_bytes()).await.map_err(|e| {
        let error = e.to_string();
        tracing::warn!(host, error = &error, "SSH password recovery write failed");

        SshError::OperationFailed {
            operation: "password recovery write",
            details: error,
        }
    })
}
