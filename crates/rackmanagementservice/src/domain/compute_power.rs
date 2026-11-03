// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Optional paths for existing compute power operations.

use super::redfish_path::RedfishPath;

/// Request-local power endpoint overrides; omitted paths retain existing discovery.
///
/// Paths configured through RMS are validated at profile loading. These settings
/// do not change reset payloads, sequencing, retry policy, or credentials.
///
/// # Examples
///
/// ```
/// # use rackmanagementservice::domain::compute_power::ComputePowerPaths;
/// let paths = ComputePowerPaths {
///     system_state_path: Some("/redfish/v1/Systems/Prototype".parse().unwrap()),
///     ..ComputePowerPaths::default()
/// };
/// assert!(paths.system_reset_path.is_none());
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ComputePowerPaths {
    /// Host power-state resource, also used for reset discovery when configured.
    pub system_state_path: Option<RedfishPath>,
    /// Host reset action used for power on, off, and restart.
    pub system_reset_path: Option<RedfishPath>,
    /// Host-BMC manager reset action for direct power requests.
    pub bmc_reset_path: Option<RedfishPath>,
    /// HMC manager reset action for direct power requests.
    pub hmc_reset_path: Option<RedfishPath>,
    /// NVIDIA auxiliary power-cycle action used during firmware activation.
    pub aux_power_cycle_path: Option<RedfishPath>,
    /// BMC readiness resource polled after auxiliary power cycling.
    pub bmc_ready_path: Option<RedfishPath>,
}
