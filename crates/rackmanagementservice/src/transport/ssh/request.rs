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

//! SSH channel request confirmation helpers.
//!
//! `russh` channel request methods only enqueue the request. When `want_reply`
//! is true, the remote `Success` or `Failure` arrives later as a channel
//! message, so callers must wait for that confirmation before treating the
//! request as accepted.

use russh::{Channel, ChannelMsg, client};

use super::error::{SshError, SshResult};

/// Error-code mapping to use when a channel request is rejected or closes early.
#[derive(Clone, Copy)]
pub(super) enum ChannelRequestFailureCode {
    /// Map the request failure to an operation failure.
    OperationFailed,

    /// Map the request failure to transport unavailability.
    Unavailable,
}

/// Parsed result of one channel message while waiting for request confirmation.
pub(super) enum ChannelRequestEvent {
    /// The remote accepted the channel request.
    Confirmed,

    /// The remote rejected the channel request.
    Rejected { details: String },

    /// The channel closed before request confirmation arrived.
    Closed { details: String },

    /// A non-confirmation message that the caller should process later.
    Deferred(ChannelMsg),
}

/// Wait for a channel request `Success` or `Failure` reply.
///
/// Returns non-confirmation messages that arrived before `Success` so the caller
/// can process them without losing channel output.
pub(super) async fn wait_for_channel_request_confirmation(
    channel: &mut Channel<client::Msg>,
    operation: &'static str,
    failure_code: ChannelRequestFailureCode,
) -> SshResult<Vec<ChannelMsg>> {
    let mut deferred_messages = Vec::new();

    loop {
        match channel_request_event(channel.wait().await) {
            ChannelRequestEvent::Confirmed => return Ok(deferred_messages),
            ChannelRequestEvent::Rejected { details } | ChannelRequestEvent::Closed { details } => {
                return Err(channel_request_error(failure_code, operation, details));
            }
            ChannelRequestEvent::Deferred(message) => {
                deferred_messages.push(message);
            }
        }
    }
}

/// Classify a channel message received while waiting for request confirmation.
pub(super) fn channel_request_event(message: Option<ChannelMsg>) -> ChannelRequestEvent {
    match message {
        Some(ChannelMsg::Success) => ChannelRequestEvent::Confirmed,
        Some(ChannelMsg::Failure) => ChannelRequestEvent::Rejected {
            details: "remote rejected request".to_owned(),
        },
        Some(ChannelMsg::OpenFailure(reason)) => ChannelRequestEvent::Rejected {
            details: format!("channel open failed: {reason:?}"),
        },
        Some(ChannelMsg::Eof) => ChannelRequestEvent::Closed {
            details: "channel reached EOF before request confirmation".to_owned(),
        },
        Some(ChannelMsg::Close) | None => ChannelRequestEvent::Closed {
            details: "channel closed before request confirmation".to_owned(),
        },
        Some(message) => ChannelRequestEvent::Deferred(message),
    }
}

/// Build an SSH error for a rejected or incomplete channel request.
fn channel_request_error(
    failure_code: ChannelRequestFailureCode,
    operation: &'static str,
    details: String,
) -> SshError {
    match failure_code {
        ChannelRequestFailureCode::OperationFailed => {
            SshError::OperationFailed { operation, details }
        }
        ChannelRequestFailureCode::Unavailable => SshError::Unavailable { operation, details },
    }
}
