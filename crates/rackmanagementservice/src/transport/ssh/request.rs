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
