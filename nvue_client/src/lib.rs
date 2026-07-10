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

//! NVUE transport and focused API models shared by RMS and nvfwupd.

mod client;
mod number;
mod tls;
mod uri;
mod util;

pub mod action;
pub mod cluster;
pub mod platform;
pub mod revision;
pub mod system;

pub use client::{
    Client, ClientConfig, ClientCredentials, ClientEndpoint, ClientError, DEFAULT_TIMEOUT,
    NvueResponse, PreparedClientTls, SharedClient,
};
pub use number::JsonNumber;
pub use tls::{ClientTls, ClientTlsPaths};

/// Base server path for NVUE v1 endpoints.
pub const NVUE_V1_SERVER: &str = "/nvue_v1";
