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

//! gNMI server state configuration and convergence polling.

use super::super::{SwitchGb200Nvidia, config};
use super::RETRY_WAIT_DELAY;
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{Result, RmsError};

use nvue_client::system::{GNMI_SERVER_ENDPOINT, GnmiServer, GnmiServerUpdate};
use serde_json::Value;

impl SwitchGb200Nvidia {
    fn desired_gnmi_state(enabled: bool) -> &'static str {
        if enabled { "enabled" } else { "disabled" }
    }

    /// Enables/disables the gNMI server and waits for NVUE to report convergence.
    ///
    /// The transition is staged, applied, and saved through the typed NVUE
    /// revision workflow. Two behaviors mirror [`Self::set_cluster_state`]:
    ///
    /// - It is a no-op when gNMI already reports the desired state: the
    ///   operational `GET /nvue_v1/system/gnmi-server` (no `rev`) is read first
    ///   and, on a match, no revision is created, applied, or saved.
    /// - Persistence happens only on a real config diff: an apply that NVUE
    ///   reports as "no config diff" is treated as success without a save.
    ///
    /// Only the gNMI server `state` is mutated; the gNMI transport/TLS authority
    /// configuration is unaffected.
    pub async fn gnmi_service(&self, enabled: bool) -> Result<Value> {
        let _op_guard = self.op_lock.lock().await;
        self.gnmi_service_unlocked(enabled).await
    }

    /// Reads the gNMI server's current `state` string from NVUE.
    ///
    /// Errors when NVUE does not report a state, so callers can distinguish an
    /// unreadable gNMI server from a known state value.
    async fn current_gnmi_state(&self) -> Result<String> {
        let value = self
            .nvue_http_get(GNMI_SERVER_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        let gnmi: GnmiServer = serde_json::from_value(value)
            .map_err(|e| RmsError::internal(format!("failed to parse gNMI server status: {e}")))?;

        gnmi.state
            .ok_or_else(|| RmsError::internal("unable to determine current gNMI server state"))
    }

    /// Changes gNMI server state while the caller holds `op_lock`.
    async fn gnmi_service_unlocked(&self, enabled: bool) -> Result<Value> {
        let desired_state = Self::desired_gnmi_state(enabled);
        tracing::info!(node = %self.id, enabled, desired_state, "gnmi_service");

        let current_state = self.current_gnmi_state().await?;

        tracing::info!(
            node = %self.id,
            enabled,
            current_state = %current_state,
            desired_state,
            "gNMI server current state observed"
        );

        // Skip the revision entirely when gNMI already holds the requested
        // state.
        if current_state == desired_state {
            tracing::info!(
                node = %self.id,
                enabled,
                current_state = %current_state,
                desired_state,
                "gNMI server state already set; skipping NVUE revision"
            );

            return Ok(serde_json::json!({"status": "success", "state": desired_state}));
        }

        self.stage_gnmi_state(desired_state).await?;

        // Verify the final operational state within a bounded number of polls
        // rather than resolving on any intermediate reading.
        self.wait_for_gnmi_state(desired_state).await
    }

    /// Stages, applies, and saves the desired gNMI state through a single NVUE
    /// configuration revision.
    ///
    /// Delegates to the shared revision workflow so revision failures and
    /// timeouts are surfaced as actionable RMS errors, an idempotent apply
    /// reported as "no config diff" is treated as success without an
    /// unnecessary save, and a candidate revision left by a failed stage is
    /// discarded rather than leaked on the switch.
    async fn stage_gnmi_state(&self, state: &str) -> Result<()> {
        let payload = serde_json::to_value(GnmiServerUpdate::new(state)).map_err(|e| {
            RmsError::internal(format!("failed to serialize gNMI server state update: {e}"))
        })?;

        self.nvue_apply_config_patches(&[(GNMI_SERVER_ENDPOINT, payload)])
            .await
    }

    async fn wait_for_gnmi_state(&self, expected: &str) -> Result<Value> {
        // Track the most recent NVUE read/parse failure so that, if no poll ever
        // observes the expected state, the error reflects the real last cause
        // (e.g. connectivity loss or an HTTP 5xx) instead of a generic timeout.
        // A successful-but-not-yet-converged read clears it: the true final
        // cause in that case is that the value never matched, not a read error.
        let mut last_read_error: Option<RmsError> = None;

        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            match self.current_gnmi_state().await {
                Ok(state) if state == expected => {
                    return Ok(serde_json::json!({"status": "success", "state": expected}));
                }
                Ok(_) => last_read_error = None,
                Err(error) => last_read_error = Some(error),
            }

            if attempt < config::MAX_RETRY_ATTEMPTS {
                tokio::time::sleep(RETRY_WAIT_DELAY).await;
            }
        }

        // Reads kept succeeding but the switch never converged to the requested
        // state within the bounded retry budget: a switch-side, retryable
        // condition, not an RMS-internal fault.
        Err(last_read_error.unwrap_or_else(|| {
            RmsError::unavailable(format!(
                "gNMI server state did not reach {expected} after retries"
            ))
        }))
    }
}
