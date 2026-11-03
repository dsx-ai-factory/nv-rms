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

//! Deployment-tunable bounds for the long-running RMS workflow loops.
//!
//! Workflow loops poll remote hardware whose real-world timings vary by site:
//! a bench rack and a production hall disagree about how long a firmware flash
//! takes and how chatty the polling should be. Rather than compile those
//! numbers in, each loop reads its bounds from a small `Copy` options struct
//! grouped under [`WorkflowTimings`], which is built once at startup from the
//! `[workflows]` configuration section and handed to the gRPC service.
//!
//! Each options struct carries a [`Default`], but a default is not always the
//! value the loop shipped with. The firmware poll loop previously bounded its
//! timeout per node kind (switch 3h, compute 90m, powershelf 60m); the
//! configurable bound is uniform, so it takes the switch value and compute and
//! powershelf trays tolerate a wedged task for longer than before. The poll
//! interval was likewise raised from 5s to 30s to cut the log rate.

use std::time::Duration;

use super::RmsWorkflowConfig;
use crate::utilities::error::{Result, RmsError};

/// Bounds applied to a single in-flight firmware task poll loop.
///
/// One loop polls every compute Redfish flash task and every switch component
/// firmware task, so these bounds are deliberately generous enough to cover
/// the slowest component rather than tuned to any one of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FirmwareTaskPollOptions {
    /// Upper bound on how long to poll one firmware task before giving up.
    ///
    /// Firmware updates legitimately take tens of minutes on some components,
    /// so this is intentionally generous. Its job is to stop an indefinite
    /// loop when a task is stuck in `running` (device hang, network
    /// partition), not to enforce a per-component budget.
    pub timeout: Duration,

    /// Delay between successive polls of an in-flight firmware task.
    pub interval: Duration,

    /// How long to tolerate consecutive task-status read failures before
    /// surfacing the error. Reset by any successful read.
    pub transient_error_timeout: Duration,
}

impl FirmwareTaskPollOptions {
    /// Reject combinations that would make the loop poll at most once.
    pub fn validate(&self) -> Result<()> {
        if self.interval > self.timeout {
            return Err(RmsError::invalid_argument(
                "firmware task poll interval must be less than or equal to the firmware task timeout",
            ));
        }

        Ok(())
    }
}

impl Default for FirmwareTaskPollOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(3 * 60 * 60),
            interval: Duration::from_secs(30),
            transient_error_timeout: Duration::from_secs(15 * 60),
        }
    }
}

/// Bounds for the compute tray power-on at the start of a firmware update.
///
/// A compute tray firmware update opens by powering the tray on and waiting
/// for Redfish to report `PowerState == "On"`. The default timeout, one minute,
/// is the VR NVL72 bound for this power-on. GB200 and GB300 trays use it as
/// their default too.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ComputePowerOnOptions {
    /// How long to wait for the tray to report `On` after the power-on
    /// command.
    pub timeout: Duration,

    /// Delay between successive power-state reads while waiting.
    pub interval: Duration,
}

impl ComputePowerOnOptions {
    /// Reject a zero bound, an interval longer than the timeout, and a timeout
    /// the monotonic clock cannot add to now, so the deadline arithmetic in the
    /// step cannot overflow at run time.
    pub fn validate(&self) -> Result<()> {
        if self.timeout.is_zero() || self.interval.is_zero() {
            return Err(RmsError::invalid_argument(
                "compute power-on timeout and poll interval must be greater than zero",
            ));
        }
        if std::time::Instant::now()
            .checked_add(self.timeout)
            .is_none()
        {
            return Err(RmsError::invalid_argument(
                "compute power-on timeout must be representable by the monotonic clock",
            ));
        }
        if self.interval > self.timeout {
            return Err(RmsError::invalid_argument(
                "compute power-on poll interval must be less than or equal to the compute power-on timeout",
            ));
        }

        Ok(())
    }
}

impl Default for ComputePowerOnOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            // Matches nvfwupd's COMPUTE_ACTIVATION_POWER_STATE_POLL_INTERVAL_SECS,
            // the activation sequence's own power-state wait.
            interval: Duration::from_secs(5),
        }
    }
}

/// Every workflow-loop bound RMS reads from `[workflows]`, resolved once at
/// startup and shared by the gRPC handlers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkflowTimings {
    /// Delay between fabric configuration wait logs; independent of device polling and timeouts.
    pub fabric_config_wait_log_interval: Duration,

    /// Bounds for the shared firmware task poll loop.
    pub firmware_task_poll: FirmwareTaskPollOptions,

    /// Bounds for the compute tray power-on that opens a firmware update.
    pub compute_power_on: ComputePowerOnOptions,

    /// Independent observation budget for stored firmware/NVOS apply history.
    /// Expiry records completed nodes only; it does not cancel hardware jobs.
    pub apply_history_timeout: Duration,

    /// Wait after ONIE starts the installer for NVOS SSH to return.
    pub onie_recovery_timeout: Duration,

    /// Wait for NVUE to save recovered credentials after boot.
    pub recovered_credential_save_timeout: Duration,
}

impl Default for WorkflowTimings {
    fn default() -> Self {
        Self {
            fabric_config_wait_log_interval: Duration::from_secs(30),
            firmware_task_poll: FirmwareTaskPollOptions::default(),
            compute_power_on: ComputePowerOnOptions::default(),
            // Cover multi-stage updates separately from a single task's budget.
            // Sites running longer operations can increase this observation window.
            apply_history_timeout: Duration::from_hours(24),
            onie_recovery_timeout: Duration::from_secs(15 * 60),
            recovered_credential_save_timeout: Duration::from_secs(5 * 60),
        }
    }
}

impl WorkflowTimings {
    /// Validate every group. Called at startup so a bad combination fails the
    /// process rather than the first workflow that happens to use it.
    pub fn validate(&self) -> Result<()> {
        if self.apply_history_timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.apply_history_timeout)
                .is_none()
        {
            return Err(RmsError::invalid_argument(
                "apply history timeout must be non-zero and representable by the monotonic clock",
            ));
        }

        for (name, timeout) in [
            (
                "fabric configuration wait log interval",
                self.fabric_config_wait_log_interval,
            ),
            ("ONIE recovery", self.onie_recovery_timeout),
            (
                "recovered credential save",
                self.recovered_credential_save_timeout,
            ),
        ] {
            if timeout.is_zero() || std::time::Instant::now().checked_add(timeout).is_none() {
                return Err(RmsError::invalid_argument(format!(
                    "{name} timeout must be non-zero and representable by the monotonic clock"
                )));
            }
        }

        self.firmware_task_poll.validate()?;
        self.compute_power_on.validate()
    }
}

impl TryFrom<&RmsWorkflowConfig> for WorkflowTimings {
    type Error = RmsError;

    fn try_from(config: &RmsWorkflowConfig) -> Result<Self> {
        let timings = Self {
            fabric_config_wait_log_interval: Duration::from_secs(
                config.fabric_config_wait_log_interval_seconds.get(),
            ),
            apply_history_timeout: Duration::from_secs(config.apply_history_timeout_seconds.get()),
            onie_recovery_timeout: Duration::from_secs(config.onie_recovery_timeout_seconds.get()),
            recovered_credential_save_timeout: Duration::from_secs(
                config.recovered_credential_save_timeout_seconds.get(),
            ),
            firmware_task_poll: FirmwareTaskPollOptions {
                timeout: Duration::from_secs(config.firmware_task_timeout_seconds.get()),
                interval: Duration::from_secs(config.firmware_task_poll_interval_seconds.get()),
                transient_error_timeout: Duration::from_secs(
                    config.firmware_task_transient_error_timeout_seconds.get(),
                ),
            },
            compute_power_on: ComputePowerOnOptions {
                timeout: Duration::from_secs(config.compute_power_on_timeout_seconds.get()),
                interval: Duration::from_secs(config.compute_power_on_poll_interval_seconds.get()),
            },
        };

        timings.validate()?;

        Ok(timings)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    #[test]
    fn fabric_config_wait_log_interval_rejects_zero_and_clock_overflow() {
        for interval in [Duration::ZERO, Duration::MAX] {
            let timings = WorkflowTimings {
                fabric_config_wait_log_interval: interval,
                ..WorkflowTimings::default()
            };

            let error = timings.validate().unwrap_err();

            assert!(
                error
                    .message
                    .contains("fabric configuration wait log interval")
            );
        }
    }

    #[test]
    fn history_observation_timeout_is_independent_and_configurable() {
        let config = RmsWorkflowConfig {
            apply_history_timeout_seconds: NonZeroU64::new(172800).unwrap(),
            ..RmsWorkflowConfig::default()
        };
        let timings = WorkflowTimings::try_from(&config).unwrap();
        assert_eq!(timings.apply_history_timeout, Duration::from_hours(48));
        assert_eq!(
            timings.firmware_task_poll,
            FirmwareTaskPollOptions::default()
        );
        assert_eq!(
            WorkflowTimings::default().apply_history_timeout,
            Duration::from_hours(24)
        );
        let invalid = WorkflowTimings {
            apply_history_timeout: Duration::ZERO,
            ..WorkflowTimings::default()
        };
        assert!(invalid.validate().is_err());
        assert!(
            WorkflowTimings {
                apply_history_timeout: Duration::MAX,
                ..WorkflowTimings::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn firmware_task_poll_defaults_match_the_switch_constants_they_replaced() {
        let defaults = FirmwareTaskPollOptions::default();

        assert_eq!(defaults.timeout, Duration::from_secs(10_800));
        assert_eq!(defaults.transient_error_timeout, Duration::from_secs(900));
        // Deliberately not the 5s the poller shipped with: at 5s a 30-minute
        // flash emitted roughly 360 poll log lines.
        assert_eq!(defaults.interval, Duration::from_secs(30));
    }

    #[test]
    fn workflow_timings_default_matches_default_workflow_config() {
        assert_eq!(
            WorkflowTimings::try_from(&RmsWorkflowConfig::default())
                .expect("the default workflow config must be valid"),
            WorkflowTimings::default()
        );
    }

    #[test]
    fn recovery_timeouts_keep_defaults_and_accept_overrides() {
        let defaults = WorkflowTimings::default();

        assert_eq!(defaults.onie_recovery_timeout, Duration::from_secs(900));

        assert_eq!(
            defaults.recovered_credential_save_timeout,
            Duration::from_secs(300)
        );

        let config = RmsWorkflowConfig {
            onie_recovery_timeout_seconds: NonZeroU64::new(1200).unwrap(),
            recovered_credential_save_timeout_seconds: NonZeroU64::new(420).unwrap(),
            ..RmsWorkflowConfig::default()
        };

        let timings = WorkflowTimings::try_from(&config).unwrap();

        assert_eq!(timings.onie_recovery_timeout, Duration::from_secs(1200));

        assert_eq!(
            timings.recovered_credential_save_timeout,
            Duration::from_secs(420)
        );
    }

    #[test]
    fn recovery_timeouts_must_fit_the_monotonic_clock() {
        for timing in [
            WorkflowTimings {
                onie_recovery_timeout: Duration::MAX,
                ..WorkflowTimings::default()
            },
            WorkflowTimings {
                recovered_credential_save_timeout: Duration::MAX,
                ..WorkflowTimings::default()
            },
        ] {
            assert!(timing.validate().is_err());
        }
    }

    #[test]
    fn workflow_timings_conversion_rejects_an_invalid_config() {
        let config = RmsWorkflowConfig {
            firmware_task_timeout_seconds: NonZeroU64::new(30).unwrap(),
            firmware_task_poll_interval_seconds: NonZeroU64::new(31).unwrap(),
            ..RmsWorkflowConfig::default()
        };

        let error = WorkflowTimings::try_from(&config)
            .expect_err("an interval longer than the timeout must not convert");
        assert!(
            error.message.contains("firmware task poll interval"),
            "unexpected message: {}",
            error.message
        );
    }

    #[test]
    fn firmware_task_poll_rejects_interval_longer_than_timeout() {
        let options = FirmwareTaskPollOptions {
            timeout: Duration::from_secs(30),
            interval: Duration::from_secs(31),
            ..FirmwareTaskPollOptions::default()
        };

        let error = options
            .validate()
            .expect_err("interval longer than timeout must be rejected");
        assert!(
            error.message.contains("firmware task poll interval"),
            "unexpected message: {}",
            error.message
        );
    }

    #[test]
    fn firmware_task_poll_accepts_interval_equal_to_timeout() {
        let options = FirmwareTaskPollOptions {
            timeout: Duration::from_secs(30),
            interval: Duration::from_secs(30),
            ..FirmwareTaskPollOptions::default()
        };

        assert!(options.validate().is_ok());
    }

    #[test]
    fn compute_power_on_accepts_overrides_from_the_workflow_config() {
        let config = RmsWorkflowConfig {
            compute_power_on_timeout_seconds: NonZeroU64::new(90).unwrap(),
            compute_power_on_poll_interval_seconds: NonZeroU64::new(10).unwrap(),
            ..RmsWorkflowConfig::default()
        };

        let timings = WorkflowTimings::try_from(&config).unwrap();

        assert_eq!(
            timings.compute_power_on,
            ComputePowerOnOptions {
                timeout: Duration::from_secs(90),
                interval: Duration::from_secs(10),
            }
        );
    }

    #[test]
    fn compute_power_on_rejects_an_unrepresentable_timeout() {
        let options = ComputePowerOnOptions {
            timeout: Duration::MAX,
            interval: Duration::from_secs(5),
        };

        assert!(options.validate().is_err());
        assert!(
            WorkflowTimings::try_from(&RmsWorkflowConfig {
                compute_power_on_timeout_seconds: NonZeroU64::new(u64::MAX).unwrap(),
                ..RmsWorkflowConfig::default()
            })
            .is_err()
        );
    }

    #[test]
    fn compute_power_on_rejects_a_zero_bound() {
        for (timeout, interval) in [(0, 0), (0, 5), (60, 0)] {
            let options = ComputePowerOnOptions {
                timeout: Duration::from_secs(timeout),
                interval: Duration::from_secs(interval),
            };

            assert!(options.validate().is_err(), "{options:?} must be rejected");
        }
    }

    #[test]
    fn compute_power_on_rejects_interval_longer_than_timeout() {
        let config = RmsWorkflowConfig {
            compute_power_on_timeout_seconds: NonZeroU64::new(30).unwrap(),
            compute_power_on_poll_interval_seconds: NonZeroU64::new(31).unwrap(),
            ..RmsWorkflowConfig::default()
        };

        assert!(WorkflowTimings::try_from(&config).is_err());
        assert!(
            ComputePowerOnOptions {
                timeout: Duration::from_secs(30),
                interval: Duration::from_secs(30),
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn workflow_timings_validate_surfaces_group_errors() {
        let timings = WorkflowTimings {
            firmware_task_poll: FirmwareTaskPollOptions {
                timeout: Duration::from_secs(1),
                interval: Duration::from_secs(2),
                ..FirmwareTaskPollOptions::default()
            },
            ..WorkflowTimings::default()
        };

        assert!(timings.validate().is_err());
    }
}
