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

//! Internal SSH transport errors and service error-code mapping.
//!
//! Public methods convert these variants to [`RmsError`] so callers keep the
//! repository-wide error type while this module preserves transport-specific
//! context until the API boundary.

use std::time::Duration;

use crate::utilities::error::{ErrorCode, RmsError};

/// Result type used inside the SSH transport module.
pub(super) type SshResult<T> = std::result::Result<T, SshError>;

/// Private SSH transport error.
///
/// Variants are intentionally operation-scoped instead of mirroring every
/// low-level `russh` or SFTP call. The mapping in [`SshError::code`] is the
/// external behavior exposed through [`RmsError`].
#[derive(Debug, thiserror::Error)]
pub(super) enum SshError {
    /// An operation required a previously authenticated SSH session.
    #[error("SSH session not connected")]
    NotConnected,

    /// A bounded SSH operation exceeded its caller-supplied timeout.
    #[error("SSH {operation} on {host} timed out after {timeout_secs}s")]
    Timeout {
        operation: &'static str,
        host: String,
        timeout_secs: u64,
    },

    /// Opening the TCP/SSH session failed before authentication.
    #[error("SSH connect to {host}: {details}")]
    ConnectFailed { host: String, details: String },

    /// Password authentication was rejected or failed.
    #[error("SSH auth to {host}: {details}")]
    AuthFailed { host: String, details: String },

    /// SSH setup failed in a way callers should treat as temporarily unavailable.
    #[error("SSH unavailable during {operation}: {details}")]
    Unavailable {
        operation: &'static str,
        details: String,
    },

    /// SSH operation failed after a session was already available.
    #[error("SSH {operation} failed: {details}")]
    OperationFailed {
        operation: &'static str,
        details: String,
    },

    /// The remote command reported signal termination.
    #[error("SSH command {command:?} on {host} terminated by signal {signal}{output_context}")]
    CommandTerminatedBySignal {
        host: String,
        command: String,
        signal: String,
        output_context: String,
    },

    /// The remote command closed without reporting an exit status.
    #[error("SSH command {command:?} on {host} completed without exit status{output_context}")]
    CommandMissingExitStatus {
        host: String,
        command: String,
        output_context: String,
    },

    /// The remote command returned a non-zero exit status.
    #[error(
        "SSH command {command:?} on {host} failed with exit status {exit_status}{output_context}"
    )]
    CommandFailed {
        host: String,
        command: String,
        exit_status: u32,
        output_context: String,
    },

    /// The local file needed for an SFTP upload could not be opened.
    #[error("cannot open local file {path}: {details}")]
    LocalFileOpenFailed { path: String, details: String },

    /// An SFTP operation failed after the local file and session were available.
    #[error("SFTP {operation} failed for {path}: {details}")]
    SftpOperationFailed {
        operation: &'static str,
        path: String,
        details: String,
    },

    /// The remote SFTP metadata query did not find the target path.
    #[error("remote file not found {path}: {details}")]
    RemoteFileNotFound { path: String, details: String },

    /// The operation was cancelled cooperatively (e.g. graceful server
    /// shutdown) before it completed.
    #[error("SSH {operation} on {host} cancelled")]
    Cancelled {
        operation: &'static str,
        host: String,
    },

    /// Password recovery was called with an empty replacement password.
    #[error("new password must not be empty")]
    EmptyPassword,

    /// The expired-password prompt flow failed before reaching a success state.
    #[error("SSH password recovery failed: {details}")]
    PasswordRecoveryFailed { details: String },
}

impl SshError {
    /// Build a timeout error using the same message shape across SSH operations.
    pub(super) fn timeout(operation: &'static str, host: &str, timeout: Duration) -> Self {
        Self::Timeout {
            operation,
            host: host.to_owned(),
            timeout_secs: timeout.as_secs(),
        }
    }

    /// Return the service error code exposed to callers after conversion.
    fn code(&self) -> ErrorCode {
        match self {
            Self::NotConnected => ErrorCode::FailedPrecondition,
            Self::Timeout { .. } => ErrorCode::Timeout,
            Self::ConnectFailed { .. } => ErrorCode::ConnectionRefused,
            Self::AuthFailed { .. } | Self::EmptyPassword => ErrorCode::InvalidArgument,
            Self::Unavailable { .. } => ErrorCode::Unavailable,
            Self::Cancelled { .. } => ErrorCode::Cancelled,
            Self::LocalFileOpenFailed { .. } | Self::RemoteFileNotFound { .. } => {
                ErrorCode::NotFound
            }
            Self::OperationFailed { .. }
            | Self::CommandTerminatedBySignal { .. }
            | Self::CommandMissingExitStatus { .. }
            | Self::CommandFailed { .. }
            | Self::SftpOperationFailed { .. }
            | Self::PasswordRecoveryFailed { .. } => ErrorCode::Internal,
        }
    }
}

impl From<SshError> for RmsError {
    fn from(error: SshError) -> Self {
        Self::new(error.code(), error.to_string())
    }
}
