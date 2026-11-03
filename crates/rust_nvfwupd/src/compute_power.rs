// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Optional paths for existing compute power operations.

use std::sync::Arc;

/// Request-local power endpoint overrides; omitted paths retain existing discovery.
///
/// Paths configured through RMS are validated at profile loading. These settings
/// do not change reset payloads, sequencing, retry policy, or credentials.
///
/// # Examples
///
/// ```
/// # use nvfwupd::compute_power::ComputePowerPaths;
/// let paths = ComputePowerPaths {
///     system_state_path: Some("/redfish/v1/Systems/Prototype".into()),
///     ..ComputePowerPaths::default()
/// };
/// assert!(paths.system_reset_path.is_none());
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ComputePowerPaths {
    /// Host power-state resource, also used for reset discovery when configured.
    pub system_state_path: Option<Arc<str>>,
    /// Host reset action used for power on, off, and restart.
    pub system_reset_path: Option<Arc<str>>,
    /// NVIDIA auxiliary power-cycle action used during firmware activation.
    pub aux_power_cycle_path: Option<Arc<str>>,
    /// BMC readiness resource polled after auxiliary power cycling.
    pub bmc_ready_path: Option<Arc<str>>,
}
