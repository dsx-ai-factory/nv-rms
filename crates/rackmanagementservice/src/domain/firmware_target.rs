// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Resolved firmware destinations, distinct from logical component identity.

use serde_json::{Map, Value};

use super::redfish_path::RedfishPath;

/// One executable upload destination, resolved before device I/O.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FirmwareDestination {
    /// An explicit validated Redfish resource.
    Redfish(RedfishPath),
    /// One multipart upload with all configured targets, including an empty list.
    Multipart(Vec<RedfishPath>),
    /// Let the concrete driver/package choose transport targets.
    DriverDefault,
}

/// Profile identity and destination retained alongside the logical component
/// and package in [`super::node::FirmwareTarget`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirmwareTargetResolution {
    /// Stable ID of the selected target within the node's startup profile.
    pub profile_target_id: String,
    /// Destination independent of the logical component used by vendor policy.
    pub destination: FirmwareDestination,
    /// Configured multipart UpdateParameters, preserved as a JSON object.
    pub update_parameters: Option<Map<String, Value>>,
    /// Configured multipart OemParameters; absence omits the multipart field.
    pub oem_parameters: Option<Map<String, Value>>,
}
