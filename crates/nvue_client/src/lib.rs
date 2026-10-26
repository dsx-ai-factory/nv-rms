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
pub mod sdn;
pub mod system;

pub use client::{
    Client, ClientConfig, ClientCredentials, ClientEndpoint, ClientError, DEFAULT_TIMEOUT,
    NvueResponse, PreparedClientTls, SharedClient,
};
pub use number::JsonNumber;
pub use tls::{ClientTls, ClientTlsPaths};

/// Base server path for NVUE v1 endpoints.
pub const NVUE_V1_SERVER: &str = "/nvue_v1";
