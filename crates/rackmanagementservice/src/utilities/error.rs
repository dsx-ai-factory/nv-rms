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

use std::fmt;

/// Structured error codes that the gRPC gateway maps to status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    Internal,
    NotFound,
    AlreadyExists,
    InvalidArgument,
    Timeout,
    FailedPrecondition,
    Unauthenticated,
    Unavailable,
    Cancelled,
    Unimplemented,
    ConnectionRefused,
    DnsResolutionFailed,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal => write!(f, "Internal"),
            Self::NotFound => write!(f, "NotFound"),
            Self::AlreadyExists => write!(f, "AlreadyExists"),
            Self::InvalidArgument => write!(f, "InvalidArgument"),
            Self::Timeout => write!(f, "Timeout"),
            Self::FailedPrecondition => write!(f, "FailedPrecondition"),
            Self::Unauthenticated => write!(f, "Unauthenticated"),
            Self::Unavailable => write!(f, "Unavailable"),
            Self::Cancelled => write!(f, "Cancelled"),
            Self::Unimplemented => write!(f, "Unimplemented"),
            Self::ConnectionRefused => write!(f, "ConnectionRefused"),
            Self::DnsResolutionFailed => write!(f, "DnsResolutionFailed"),
        }
    }
}

/// Lightweight error type carrying a code and message.
///
/// The gRPC gateway maps `code` to gRPC status codes. The `message` field
/// provides human-readable context. Error propagation uses the `?` operator.
#[derive(Debug, thiserror::Error)]
#[error("[{code}] {message}")]
pub struct RmsError {
    pub code: ErrorCode,
    pub message: String,
}

impl RmsError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::AlreadyExists, message)
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Timeout, message)
    }

    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::FailedPrecondition, message)
    }

    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthenticated, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unavailable, message)
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Cancelled, message)
    }

    pub fn unimplemented(operation: &str, node_type: impl fmt::Display) -> Self {
        Self::new(
            ErrorCode::Unimplemented,
            format!("{operation} not supported for {node_type} nodes"),
        )
    }

    pub fn connection_refused(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ConnectionRefused, message)
    }

    pub fn dns_resolution_failed(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::DnsResolutionFailed, message)
    }
}

impl From<nvue_client::ClientError> for RmsError {
    fn from(error: nvue_client::ClientError) -> Self {
        use nvue_client::ClientError;

        let code = match &error {
            ClientError::InvalidEndpoint(_) => ErrorCode::InvalidArgument,
            ClientError::ReadTlsMaterial { .. } | ClientError::InvalidTlsMaterial(_) => {
                ErrorCode::FailedPrecondition
            }
            ClientError::UnstableTlsMaterial { .. } => ErrorCode::Unavailable,
            ClientError::StalePreparedTransport | ClientError::ForeignPreparedTransport => {
                ErrorCode::FailedPrecondition
            }
            ClientError::ResolveHost { .. } | ClientError::NoResolvedAddress { .. } => {
                ErrorCode::DnsResolutionFailed
            }
            ClientError::Request { source, .. } if source.is_timeout() => ErrorCode::Timeout,
            ClientError::Request { source, .. } if source.is_connect() => {
                ErrorCode::ConnectionRefused
            }
            ClientError::Request { source, .. } if source.is_request() => ErrorCode::Unavailable,
            ClientError::HttpStatus { status: 401, .. } => ErrorCode::Unauthenticated,
            ClientError::HttpStatus { status: 404, .. } => ErrorCode::NotFound,
            ClientError::HttpStatus { status: 408, .. } => ErrorCode::Timeout,
            ClientError::HttpStatus { status: 409, .. } => ErrorCode::AlreadyExists,
            ClientError::HttpStatus { status: 503, .. } => ErrorCode::Unavailable,
            _ => ErrorCode::Internal,
        };

        Self::new(code, error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, RmsError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_includes_code_and_message() {
        let err = RmsError::not_found("node compute-01 not found");
        assert_eq!(err.to_string(), "[NotFound] node compute-01 not found");
    }

    #[test]
    fn error_code_equality() {
        let err = RmsError::internal("something broke");
        assert_eq!(err.code, ErrorCode::Internal);
    }

    #[test]
    fn unimplemented_formats_operation_and_type() {
        let err = RmsError::unimplemented("get_power_state", "powershelf");
        assert_eq!(
            err.to_string(),
            "[Unimplemented] get_power_state not supported for powershelf nodes"
        );
    }

    #[test]
    fn result_type_alias_works_with_question_mark() {
        fn fallible() -> Result<i32> {
            Err(RmsError::not_found("missing"))
        }

        fn caller() -> Result<i32> {
            let val = fallible()?;
            Ok(val + 1)
        }

        let result = caller();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, ErrorCode::NotFound);
    }
}
