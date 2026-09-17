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

//! Cluster state configuration and convergence polling.

use super::super::{SwitchGb200Nvidia, config};
use super::RETRY_WAIT_DELAY;
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{Result, RmsError};
use crate::utilities::insert_json_field;

use nvue_client::cluster::{CLUSTER_ENDPOINT, Cluster, ClusterState, ClusterUpdate};
use serde_json::Value;

impl SwitchGb200Nvidia {
    fn desired_cluster_state(enabled: bool) -> ClusterState {
        if enabled {
            ClusterState::Enabled
        } else {
            ClusterState::Disabled
        }
    }

    /// Enables/disables the cluster and waits for NVUE to report convergence.
    ///
    /// The transition is staged, applied, and saved through the typed NVUE
    /// revision workflow. Two behaviors differ from the former SSH
    /// implementation and callers should be aware of them:
    ///
    /// - It is a no-op when the cluster already reports the desired state: the
    ///   operational `GET /nvue_v1/cluster` (no `rev`) is read first and, on a
    ///   match, no revision is created, applied, or saved. The former code ran
    ///   `nv set` / `nv config apply` / `nv config save` unconditionally.
    /// - Persistence happens only on a real config diff: an apply that NVUE
    ///   reports as "no config diff" is treated as success without a save,
    ///   whereas the former `nv config save` persisted the running config on
    ///   every call.
    pub async fn set_cluster_state(&self, enabled: bool) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        self.set_cluster_state_unlocked(enabled).await
    }

    /// Reads the cluster's current `state` string from NVUE.
    ///
    /// Errors when NVUE does not report a state, so callers can distinguish an
    /// unreadable cluster from a known state value.
    async fn current_cluster_state(&self) -> Result<String> {
        let value = self
            .nvue_http_get(CLUSTER_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        let cluster: Cluster = serde_json::from_value(value)
            .map_err(|e| RmsError::internal(format!("failed to parse cluster status: {e}")))?;

        cluster
            .state
            .ok_or_else(|| RmsError::internal("unable to determine current cluster state"))
    }

    /// Changes cluster state while the caller holds `op_lock`.
    ///
    /// The state transition is staged, applied, and saved entirely through the
    /// typed NVUE revision workflow. RMS no longer shells into the switch to run
    /// `nv set cluster state` / `nv config apply` / `nv config save`, removing
    /// the dependency on nvCLI syntax, SSH availability, and CLI output parsing.
    pub(super) async fn set_cluster_state_unlocked(&self, enabled: bool) -> Result<()> {
        let state = Self::desired_cluster_state(enabled);
        let desired_state = state.as_str();
        tracing::info!(node = %self.id, enabled, desired_state, "set_cluster_state");

        let current_state = self.current_cluster_state().await?;

        tracing::info!(
            node = %self.id,
            enabled,
            current_state = %current_state,
            desired_state,
            "scale-up fabric current state observed"
        );

        // Preserve the no-op behavior: skip the revision entirely when the
        // cluster already holds the requested state. NVOS only ever reports the
        // cluster `state` as `enabled` or `disabled` (unset defaults to
        // `disabled`).
        if current_state == desired_state {
            tracing::info!(
                node = %self.id,
                enabled,
                current_state = %current_state,
                desired_state,
                "scale-up fabric state already set; skipping NVUE revision"
            );

            return Ok(());
        }

        self.stage_cluster_state(state).await?;

        // Verify the final operational state: wait until the cluster actually
        // reports the desired `enabled`/`disabled` value rather than resolving on
        // any intermediate reading.
        self.wait_for_cluster_state(desired_state).await.map(|_| ())
    }

    /// Stages, applies, and saves the desired cluster state through a single
    /// NVUE configuration revision.
    ///
    /// Delegates to the shared revision workflow so revision failures and
    /// timeouts are surfaced as actionable RMS errors, an idempotent apply
    /// reported as "no config diff" is treated as success without an
    /// unnecessary save, and a candidate revision left by a failed stage is
    /// discarded rather than leaked on the switch.
    async fn stage_cluster_state(&self, state: ClusterState) -> Result<()> {
        let payload = serde_json::to_value(ClusterUpdate::new(state.as_str())).map_err(|e| {
            RmsError::internal(format!("failed to serialize cluster state update: {e}"))
        })?;

        self.nvue_apply_config_patches(&[(CLUSTER_ENDPOINT, payload)])
            .await
    }

    async fn wait_for_cluster_state(&self, expected: &str) -> Result<Value> {
        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            if let Ok(cluster) = self
                .nvue_http_get(CLUSTER_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
                .await
                && let Some(state) = cluster.get("state").and_then(|v| v.as_str())
                && state == expected
            {
                return Ok(cluster);
            }

            if attempt < config::MAX_RETRY_ATTEMPTS {
                tokio::time::sleep(RETRY_WAIT_DELAY).await;
            }
        }

        Err(RmsError::internal(format!(
            "cluster state did not reach {expected} after retries"
        )))
    }

    pub async fn get_cluster_state(&self) -> Result<Value> {
        let mut result = self
            .nvue_http_get(CLUSTER_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        let state = result
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_owned();

        if state == "disabled" || state == "unknown" {
            insert_json_field(&mut result, "cluster_healthy", serde_json::json!(false));

            insert_json_field(
                &mut result,
                "warning",
                serde_json::json!("Cluster is disabled"),
            );

            insert_json_field(
                &mut result,
                "recommendation",
                serde_json::json!(
                    "Enable the scale-up fabric cluster before performing cluster operations"
                ),
            );
        } else {
            let nmxc_down = result
                .get("nmxc-conn")
                .and_then(|v| v.as_object())
                .and_then(|obj| obj.get("state"))
                .and_then(|v| v.as_str())
                .is_some_and(|s| s == "down" || s == "disabled");

            insert_json_field(
                &mut result,
                "cluster_healthy",
                serde_json::json!(!nmxc_down),
            );

            if nmxc_down {
                insert_json_field(
                    &mut result,
                    "warning",
                    serde_json::json!("nmxc-conn is down"),
                );
            }
        }

        Ok(result)
    }
}
