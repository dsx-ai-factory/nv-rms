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

use std::fmt::Display;

/// Domain-neutral classification of why an async job ended. These codes are
/// generic (I/O, client/server failure, timeout, task failure, ...) rather
/// than firmware-specific. `conversions.rs` maps them to the generic
/// `pb::JobError` enum for `GetJobStatus` and to the legacy firmware-specific
/// enum for `GetFirmwareJobStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobError {
    Unspecified,
    Internal,
    Other,
    InvalidArgument,
    FailedPrecondition,
    ClientError,
    ServerError,
    Timeout,
    InvalidResponse,
    Unauthenticated,
    TargetNotFound,
    FileNotFound,
    UpdateInProgress,
}

impl Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Unspecified => "unspecified",
            Self::Internal => "internal",
            Self::Other => "other",
            Self::InvalidArgument => "invalid argument",
            Self::FailedPrecondition => "failed precondition",
            Self::ClientError => "client error",
            Self::ServerError => "server error",
            Self::Timeout => "timeout",
            Self::InvalidResponse => "invalid response",
            Self::Unauthenticated => "unauthenticated",
            Self::TargetNotFound => "target not found",
            Self::FileNotFound => "file not found",
            Self::UpdateInProgress => "update in progress",
        };
        f.write_str(text)
    }
}

impl std::error::Error for JobError {}

impl JobError {
    /// Lowercase snake-case label used by workflow metrics.
    pub const fn metrics_label(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Internal => "internal",
            Self::Other => "other",
            Self::InvalidArgument => "invalid_argument",
            Self::FailedPrecondition => "failed_precondition",
            Self::ClientError => "client_error",
            Self::ServerError => "server_error",
            Self::Timeout => "timeout",
            Self::InvalidResponse => "invalid_response",
            Self::Unauthenticated => "unauthenticated",
            Self::TargetNotFound => "target_not_found",
            Self::FileNotFound => "file_not_found",
            Self::UpdateInProgress => "update_in_progress",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_uses_human_readable_text() {
        assert_eq!(JobError::InvalidArgument.to_string(), "invalid argument");
        assert_eq!(
            JobError::FailedPrecondition.to_string(),
            "failed precondition"
        );
        assert_eq!(JobError::UpdateInProgress.to_string(), "update in progress");
    }

    #[test]
    fn metrics_labels_use_lowercase_snake_case() {
        assert_eq!(
            JobError::InvalidResponse.metrics_label(),
            "invalid_response"
        );
        assert_eq!(
            JobError::FailedPrecondition.metrics_label(),
            "failed_precondition"
        );
        assert_eq!(JobError::TargetNotFound.metrics_label(), "target_not_found");
    }
}
