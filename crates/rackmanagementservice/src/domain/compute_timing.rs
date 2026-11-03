// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Timing policy for existing compute firmware workflows.

use std::time::Duration;

/// Timing values; orchestration, device commands, and recovery decisions stay in code.
/// Defaults preserve the existing compute workflow. Retry counts exclude the first attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ComputeFirmwareTiming {
    /// Maximum time to establish an NVFWUPD HTTP connection.
    pub connect_timeout: Duration,
    /// Timeout for a normal NVFWUPD Redfish request.
    pub request_timeout: Duration,
    /// Timeout for sending one firmware payload to the BMC.
    pub upload_timeout: Duration,
    /// Timeout for one NVFWUPD reachability or discovery GET.
    pub reachability_timeout: Duration,
    /// Timeout for each background-copy or write-protection query.
    pub precondition_query_timeout: Duration,
    /// Overall wait budget for device background copy before uploading firmware.
    pub background_copy_timeout: Duration,
    /// Delay between polls of device BackgroundCopyStatus.
    pub background_copy_poll_interval: Duration,
    /// Total attempts to discover inventory for write-protection checks.
    pub write_protect_inventory_attempts: u32,
    /// Delay between failed write-protection inventory reads.
    pub write_protect_inventory_retry_interval: Duration,
    /// Wait after host power-off before auxiliary power cycling.
    pub power_off_delay: Duration,
    /// Maximum complete AC-cycle attempts in the verified compute activation sequence.
    pub ac_cycle_attempts: u32,
    /// Wait after each auxiliary AC cycle in the verified activation sequence.
    pub ac_cycle_stabilization: Duration,
    /// Retries after the first failed power-off, auxiliary-cycle, or power-on command.
    pub activation_command_retries: u32,
    /// Delay between activation command retries.
    pub activation_command_retry_interval: Duration,
    /// Maximum BMC readiness checks after auxiliary power cycling.
    pub bmc_recovery_attempts: u32,
    /// Wait before each BMC readiness check.
    pub bmc_recovery_interval: Duration,
    /// Maximum power-state checks; verified activation uses this for both Off and On.
    pub power_on_attempts: u32,
    /// Wait before each host power-state check.
    pub power_on_interval: Duration,
    /// Minimum polling time before resending power-on while the host remains Off.
    pub power_on_resend_interval: Duration,
    /// Timeout for activation reads/reset actions and BMC recovery probes.
    pub recovery_probe_timeout: Duration,
    /// Extra expected-inventory reads after activation when inventory is not ready.
    pub inventory_recovery_retries: u32,
    /// Delay between internal expected-inventory recovery reads.
    pub inventory_recovery_retry_interval: Duration,
}

impl Default for ComputeFirmwareTiming {
    /// Preserve historical NVFWUPD budgets. The three 900-second transport values
    /// share the old request/upload default, but are independently configurable.
    /// A request's total deadline also bounds the time available for connection.
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(900),
            request_timeout: Duration::from_secs(900),
            upload_timeout: Duration::from_secs(900),
            reachability_timeout: Duration::from_secs(120),
            precondition_query_timeout: Duration::from_secs(30),
            background_copy_timeout: Duration::from_secs(900),
            background_copy_poll_interval: Duration::from_secs(10),
            write_protect_inventory_attempts: 3,
            write_protect_inventory_retry_interval: Duration::from_secs(30),
            power_off_delay: Duration::from_secs(20),
            ac_cycle_attempts: 2,
            ac_cycle_stabilization: Duration::from_secs(180),
            activation_command_retries: 10,
            activation_command_retry_interval: Duration::from_secs(60),
            bmc_recovery_attempts: 60,
            bmc_recovery_interval: Duration::from_secs(5),
            power_on_attempts: 60,
            power_on_interval: Duration::from_secs(5),
            power_on_resend_interval: Duration::from_secs(60),
            recovery_probe_timeout: Duration::from_secs(30),
            inventory_recovery_retries: 3,
            inventory_recovery_retry_interval: Duration::from_secs(120),
        }
    }
}
