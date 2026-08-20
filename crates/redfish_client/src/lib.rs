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

//! Shared nv-redfish client operations for RMS workspace consumers.

mod client;
mod error;
mod task;
mod types;

pub use client::RedfishClient;
pub use error::{RedfishError, Result};
pub use nv_redfish::bmc_http::BmcCredentials;
pub use nv_redfish::core::{DataStream, UploadReader};
pub use nv_redfish::resource::{PowerState as RedfishPowerState, ResetType};
pub use types::NvidiaMnnvlinkTopology;
