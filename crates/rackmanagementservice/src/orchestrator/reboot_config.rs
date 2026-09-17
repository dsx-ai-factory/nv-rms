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

//! Server-side timing configuration for the rack cold reboot sequence.
//!
//! Each staged transition uses a *minimum-wait floor* (the hardware settle /
//! discharge time that must elapse regardless of what a probe reports) followed
//! by *polling* the real condition until it holds or a per-stage maximum is
//! reached. Defaults mirror the NVIDIA DGX GB200 cold-reboot runbook (3-5 min
//! capacitor discharge, ~2 min BMC/NVOS boot, ~5 min compute boot). Callers may
//! override any value per request via the `ExecuteColdRebootRequest` timeout
//! fields; an unset or zero field falls back to the default here.

use std::time::Duration;

use librms::protos::rack_manager as rm;

/// Prestage: minimum wait after issuing the compute graceful shutdown before
/// probing (0 — poll immediately; the OS shutdown is observed via the poll).
pub const PRESTAGE_SHUTDOWN_FLOOR_SECONDS: u32 = 0;
/// Prestage: maximum time to wait for the compute trays to gracefully power down
/// before the shelves are cut.
pub const PRESTAGE_SHUTDOWN_POLL_MAX_SECONDS: u32 = 300;
/// Steps 2-3: minimum wait after powering the shelves off before probing the
/// compute/switch BMCs for unreachability.
pub const DISCHARGE_FLOOR_SECONDS: u32 = 180;
/// Steps 2-3: maximum time to wait for every BMC to become unreachable.
pub const DISCHARGE_POLL_MAX_SECONDS: u32 = 600;
/// Steps 5-6: minimum wait after powering the shelves back on before probing.
pub const BMC_UP_FLOOR_SECONDS: u32 = 120;
/// Steps 5-6: maximum time to wait for every BMC to become reachable.
pub const BMC_UP_POLL_MAX_SECONDS: u32 = 600;
/// Steps 7-8: number of "nvue hello" attempts against each switch before failing.
pub const NVOS_HELLO_ATTEMPTS: u32 = 5;
/// Steps 7-8: base delay for the exponential backoff between "nvue hello" attempts
/// (doubles each attempt: 30s, 60s, 120s, 240s with the default base).
pub const NVOS_HELLO_BACKOFF_BASE_SECONDS: u32 = 30;
/// Steps 11-12: fixed wait for the compute OS to boot after power-on. The BMC
/// only reports host power, not OS readiness, so there is nothing to poll.
pub const COMPUTE_BOOT_WAIT_SECONDS: u32 = 300;
/// Poll cadence used inside every stage's poll loop.
pub const POLL_INTERVAL_SECONDS: u32 = 15;

/// Resolved floor + poll-max budget for a single gated stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageTiming {
    /// Unconditional minimum wait before the first probe.
    pub floor: Duration,
    /// Maximum total time spent polling the stage condition.
    pub poll_max: Duration,
}

/// All per-stage timing budgets for one cold reboot, resolved from a request
/// (per-field override) against the server defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdRebootTimings {
    /// Prestage: wait for the compute trays to graceful-shutdown before cutting power.
    pub prestage_shutdown: StageTiming,
    pub discharge: StageTiming,
    pub bmc_up: StageTiming,
    /// Steps 7-8: base delay for the exponential backoff between switch "nvue hello" attempts.
    pub nvos_hello_backoff: Duration,
    /// Steps 11-12: fixed wait for the compute OS to boot after power-on.
    pub compute_boot_wait: Duration,
    /// Poll cadence shared by every stage.
    pub poll_interval: Duration,
}

impl ColdRebootTimings {
    /// Resolves the effective timings from the request, applying the server
    /// default for every field left unset (or explicitly zero).
    pub fn from_proto(req: &rm::ExecuteColdRebootRequest) -> Self {
        Self {
            prestage_shutdown: StageTiming {
                floor: resolve(
                    req.prestage_shutdown_floor_seconds,
                    PRESTAGE_SHUTDOWN_FLOOR_SECONDS,
                ),
                poll_max: resolve(
                    req.prestage_shutdown_poll_max_seconds,
                    PRESTAGE_SHUTDOWN_POLL_MAX_SECONDS,
                ),
            },
            discharge: StageTiming {
                floor: resolve(req.discharge_floor_seconds, DISCHARGE_FLOOR_SECONDS),
                poll_max: resolve(req.discharge_poll_max_seconds, DISCHARGE_POLL_MAX_SECONDS),
            },
            bmc_up: StageTiming {
                floor: resolve(req.bmc_up_floor_seconds, BMC_UP_FLOOR_SECONDS),
                poll_max: resolve(req.bmc_up_poll_max_seconds, BMC_UP_POLL_MAX_SECONDS),
            },
            // Steps 7-8 use an nvue-hello retry with exponential backoff rather than
            // a floor+poll gate; the caller may override the base delay.
            nvos_hello_backoff: resolve(
                req.nvos_hello_backoff_seconds,
                NVOS_HELLO_BACKOFF_BASE_SECONDS,
            ),
            compute_boot_wait: resolve(req.compute_boot_wait_seconds, COMPUTE_BOOT_WAIT_SECONDS),
            poll_interval: resolve(req.poll_interval_seconds, POLL_INTERVAL_SECONDS),
        }
    }
}

/// Returns `value` as a `Duration` when it is set and non-zero, otherwise the
/// `default` (in seconds). A zero override is treated as "use the default"
/// because prost cannot distinguish a caller-supplied 0 from an intentional
/// disable, and a zero floor/poll-max is never meaningful for this sequence.
fn resolve(value: Option<u32>, default: u32) -> Duration {
    let seconds = match value {
        Some(n) if n > 0 => n,
        _ => default,
    };
    Duration::from_secs(u64::from(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_unset_uses_defaults() {
        let req = rm::ExecuteColdRebootRequest::default();
        let t = ColdRebootTimings::from_proto(&req);
        assert_eq!(t.prestage_shutdown.floor, Duration::from_secs(0));
        assert_eq!(t.prestage_shutdown.poll_max, Duration::from_secs(300));
        assert_eq!(t.discharge.floor, Duration::from_secs(180));
        assert_eq!(t.discharge.poll_max, Duration::from_secs(600));
        assert_eq!(t.bmc_up.floor, Duration::from_secs(120));
        assert_eq!(t.nvos_hello_backoff, Duration::from_secs(30));
        assert_eq!(t.compute_boot_wait, Duration::from_secs(300));
        assert_eq!(t.poll_interval, Duration::from_secs(15));
    }

    #[test]
    fn zero_is_treated_as_default() {
        let req = rm::ExecuteColdRebootRequest {
            discharge_floor_seconds: Some(0),
            poll_interval_seconds: Some(0),
            ..Default::default()
        };
        let t = ColdRebootTimings::from_proto(&req);
        assert_eq!(
            t.discharge.floor,
            Duration::from_secs(DISCHARGE_FLOOR_SECONDS as u64)
        );
        assert_eq!(
            t.poll_interval,
            Duration::from_secs(POLL_INTERVAL_SECONDS as u64)
        );
    }

    #[test]
    fn nonzero_overrides_are_applied() {
        let req = rm::ExecuteColdRebootRequest {
            prestage_shutdown_floor_seconds: Some(6),
            prestage_shutdown_poll_max_seconds: Some(10),
            discharge_floor_seconds: Some(1),
            discharge_poll_max_seconds: Some(2),
            bmc_up_floor_seconds: Some(3),
            bmc_up_poll_max_seconds: Some(4),
            nvos_hello_backoff_seconds: Some(5),
            compute_boot_wait_seconds: Some(7),
            poll_interval_seconds: Some(9),
            ..Default::default()
        };
        let t = ColdRebootTimings::from_proto(&req);
        assert_eq!(t.prestage_shutdown.floor, Duration::from_secs(6));
        assert_eq!(t.prestage_shutdown.poll_max, Duration::from_secs(10));
        assert_eq!(t.discharge.floor, Duration::from_secs(1));
        assert_eq!(t.discharge.poll_max, Duration::from_secs(2));
        assert_eq!(t.bmc_up.floor, Duration::from_secs(3));
        assert_eq!(t.bmc_up.poll_max, Duration::from_secs(4));
        assert_eq!(t.nvos_hello_backoff, Duration::from_secs(5));
        assert_eq!(t.compute_boot_wait, Duration::from_secs(7));
        assert_eq!(t.poll_interval, Duration::from_secs(9));
    }
}
