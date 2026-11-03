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

use reqwest::StatusCode;
use tonic::Status;

/// Errors that can occur while launching or tracking an NVSSVT scan through
/// the launcher API.
#[derive(Debug, thiserror::Error)]
pub enum NvssvtError {
    #[error("failed to build the NVSSVT HTTP client: {0}")]
    BuildClient(#[source] reqwest::Error),

    #[error("failed to reach the NVSSVT launcher API: {}", source_chain(.0))]
    Transport(#[source] reqwest::Error),

    #[error("NVSSVT launcher API returned {status}: {body}")]
    Upstream { status: StatusCode, body: String },

    #[error("failed to decode the NVSSVT launcher API response: {}", source_chain(.0))]
    Decode(#[source] reqwest::Error),

    #[error("unknown NVSSVT run id: {0}")]
    RunNotFound(String),

    #[error("NVSSVT run id already in use: {0}")]
    RunAlreadyExists(String),
}

/// Renders `err` with its whole source chain. `reqwest::Error`'s own
/// `Display` stops at "error sending request for url (...)", which reads the
/// same whether the cause was a timeout, a refused connection, or a
/// server-closed keep-alive connection -- the distinction that matters when
/// diagnosing a failing poll, so it is spelled out here.
fn source_chain(err: &reqwest::Error) -> String {
    let mut chain = vec![err.to_string()];
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        chain.push(cause.to_string());
        source = cause.source();
    }
    chain.join(": ")
}

impl NvssvtError {
    /// Whether retrying the same call could plausibly succeed. A scan runs
    /// for a long time, so a poller should ride out a blip -- a sidecar
    /// restart, a dropped keep-alive connection -- rather than failing the
    /// scan over one bad response. A run the launcher has never heard of
    /// (or already has) will not appear or disappear on a retry.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) | Self::Upstream { .. } | Self::Decode(_) => true,
            Self::BuildClient(_) | Self::RunNotFound(_) | Self::RunAlreadyExists(_) => false,
        }
    }
}

impl From<NvssvtError> for Status {
    fn from(err: NvssvtError) -> Self {
        match err {
            NvssvtError::RunNotFound(id) => {
                Status::not_found(format!("unknown NVSSVT run id: {id}"))
            }
            NvssvtError::RunAlreadyExists(id) => {
                Status::already_exists(format!("NVSSVT run id already in use: {id}"))
            }
            // The launcher is a sidecar in this pod: unreachable means it is
            // starting, crashed, or gone, all of which a caller can sensibly
            // retry -- unlike the other variants, which need a fix first.
            err @ NvssvtError::Transport(_) => Status::unavailable(err.to_string()),
            other => Status::internal(other.to_string()),
        }
    }
}
