// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed preparation failures, retained until the job-status boundary.

use crate::orchestrator::job_lifecycle::JobError;

/// Retry recognized I/O failures and incomplete HTTP messages, including a
/// clean close before the response head. A generic body or connection error
/// can also mean malformed HTTP or a TLS/trust failure; those are permanent.
fn retryable_transport_error(error: &reqwest::Error) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(source) = cause {
        if source
            .downcast_ref::<hyper::Error>()
            .is_some_and(hyper::Error::is_incomplete_message)
        {
            return true;
        }
        if source
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| {
                matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::WouldBlock
                )
            })
        {
            return true;
        }
        cause = source.source();
    }
    false
}

/// Validation and permanent cache/integrity errors must not masquerade as
/// transient transport failures (or vice versa).
#[derive(Debug, thiserror::Error)]
pub(crate) enum ArtifactError {
    #[error("{0}")]
    InvalidArgument(String),
    #[error("{0}")]
    Cache(String),
    #[error("{0}")]
    RedirectPolicy(String),
    #[error("{context}: {source}")]
    Io {
        context: String,
        source: std::io::Error,
    },
    #[error("{context}: {source}")]
    Transport {
        code: JobError,
        context: String,
        source: reqwest::Error,
    },
    #[error("download failed with status {status}: {url}")]
    HttpStatus {
        status: reqwest::StatusCode,
        url: String,
    },
    #[error("{0}")]
    Integrity(String),
    #[error("artifact preparation cancelled")]
    Cancelled,
    #[error("{context}: {source}")]
    Context { context: String, source: Box<Self> },
    #[error("artifact preparation failed: {}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "))]
    Multiple(Vec<Self>),
}

impl ArtifactError {
    /// Preserve the transport cause without retaining a possibly secret URL.
    pub fn transport(context: impl Into<String>, source: reqwest::Error) -> Self {
        Self::from(source).context(context.into())
    }

    /// Attach an artifact's manifest path without losing its failure category.
    pub fn context(self, context: String) -> Self {
        Self::Context {
            context,
            source: Box::new(self),
        }
    }

    /// Map failures to existing job codes. A batch containing any permanent
    /// failure is permanent even when other artifacts failed transiently.
    pub fn job_error(&self) -> JobError {
        match self {
            Self::InvalidArgument(_) => JobError::InvalidArgument,
            Self::Cache(_) | Self::RedirectPolicy(_) | Self::Integrity(_) | Self::Cancelled => {
                JobError::FailedPrecondition
            }
            Self::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound => {
                JobError::FileNotFound
            }
            Self::Io { .. } => JobError::FailedPrecondition,
            Self::Transport { code, .. } => *code,
            Self::HttpStatus { status, .. } => match status.as_u16() {
                401 | 403 => JobError::Unauthenticated,
                404 => JobError::FileNotFound,
                408 => JobError::Timeout,
                429 | 500..=599 => JobError::ServerError,
                _ => JobError::ClientError,
            },
            Self::Context { source, .. } => source.job_error(),
            Self::Multiple(errors) => errors
                .iter()
                .map(Self::job_error)
                .find(|code| !matches!(code, JobError::ServerError | JobError::Timeout))
                .or_else(|| errors.first().map(Self::job_error))
                .unwrap_or(JobError::Internal),
        }
    }

    /// Whether the failure can be transient. A timeout may come from the OS
    /// before the overall deadline; the retry loop checks the remaining budget.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self.job_error(), JobError::ServerError | JobError::Timeout)
    }
}

/// Concatenate `error` and its sources so custom DNS/policy text is not lost
/// when reqwest Display is only `error sending request`.
fn format_error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = Vec::new();
    let mut current = Some(error);
    while let Some(err) = current {
        let text = err.to_string();
        if parts.last().is_none_or(|last: &String| *last != text) {
            parts.push(text);
        }
        current = err.source();
    }
    parts.join(": ")
}

impl From<reqwest::Error> for ArtifactError {
    fn from(source: reqwest::Error) -> Self {
        // Strip the URL before formatting the chain, not just from the stored
        // source: signed URL query parameters must not enter logs or job status.
        let source = source.without_url();
        let code = if source.is_timeout() {
            JobError::Timeout
        } else if retryable_transport_error(&source) {
            JobError::ServerError
        } else {
            JobError::ClientError
        };
        let chain = format_error_chain(&source);
        Self::Transport {
            code,
            context: format!("artifact transport failed: {chain}"),
            source,
        }
    }
}

impl From<std::io::Error> for ArtifactError {
    fn from(source: std::io::Error) -> Self {
        Self::Io {
            context: "artifact I/O failed".to_owned(),
            source,
        }
    }
}

impl From<&ArtifactError> for JobError {
    fn from(error: &ArtifactError) -> Self {
        error.job_error()
    }
}

#[cfg(test)]
mod tests;
