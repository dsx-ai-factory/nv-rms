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

use std::path::PathBuf;

use thiserror::Error;

/// Shared Redfish client result type.
pub type Result<T> = std::result::Result<T, RedfishError>;

/// Error returned by shared Redfish operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RedfishError {
    #[error("{0}")]
    AlreadyExists(String),
    #[error("{0}")]
    ConnectionRefused(String),
    #[error("{0}")]
    DnsResolutionFailed(String),
    #[error("{0}")]
    FailedPrecondition(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    Internal(String),
    #[error("invalid Redfish endpoint {host}:{port}: {source}")]
    InvalidEndpoint {
        host: String,
        port: u16,
        #[source]
        source: url::ParseError,
    },
    #[error("{0}")]
    InvalidArgument(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Timeout(String),
    #[error("{0}")]
    Unauthenticated(String),
    #[error("{0}")]
    Unavailable(String),
    #[error("failed to create Redfish client: {source}")]
    CreateHttpClient {
        #[source]
        source: reqwest::Error,
    },
    #[error("cannot open firmware file {path}: {source}")]
    OpenFirmwareFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot read firmware file metadata {path}: {source}")]
    ReadFirmwareFileMetadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
