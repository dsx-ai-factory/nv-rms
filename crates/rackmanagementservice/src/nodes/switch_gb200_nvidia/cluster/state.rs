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

//! Cluster state configuration and convergence polling.

use super::super::{SwitchGb200Nvidia, config};
use super::RETRY_WAIT_DELAY;
use crate::transport::http_client::HttpClient;
use crate::transport::ssh_client::SshClient;
use crate::utilities::error::{Result, RmsError};
use crate::utilities::insert_json_field;

use nvue_client::cluster::ClusterState;
use serde_json::Value;

impl SwitchGb200Nvidia {
    fn desired_cluster_state(enabled: bool) -> ClusterState {
        if enabled {
            ClusterState::Enabled
        } else {
            ClusterState::Disabled
        }
    }

    pub async fn set_cluster_state(&self, enabled: bool) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        self.set_cluster_state_unlocked(enabled).await
    }

    /// Changes cluster state while the caller holds `op_lock`.
    pub(super) async fn set_cluster_state_unlocked(&self, enabled: bool) -> Result<()> {
        let state = Self::desired_cluster_state(enabled);
        let desired_state = state.as_str();
        tracing::info!(node = %self.id, enabled, desired_state, "set_cluster_state");

        {
            let cluster = self
                .nvue_http_get("/nvue_v1/cluster", HttpClient::DEFAULT_TIMEOUT)
                .await?;

            let current_state = cluster
                .get("state")
                .and_then(|value| value.as_str())
                .ok_or_else(|| RmsError::internal("unable to determine current cluster state"))?;

            tracing::info!(
                node = %self.id,
                enabled,
                current_state,
                desired_state,
                "scale-up fabric current state observed"
            );

            if current_state == desired_state || (enabled && current_state == "start") {
                tracing::info!(
                    node = %self.id,
                    enabled,
                    current_state,
                    desired_state,
                    "scale-up fabric state already set; skipping nv cli update"
                );

                return Ok(());
            }
        }

        self.run_cluster_state_nv_commands(desired_state).await?;

        self.wait_for_cluster_state(desired_state, enabled)
            .await
            .map(|_| ())
    }

    async fn run_cluster_state_nv_commands(&self, state: &str) -> Result<()> {
        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            exec(&format!("nv set cluster state {state}"))?;
            exec("nv config apply --assume-yes")?;
            exec("nv config save")?;
            return Ok(());
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        ssh.exec(
            &format!("nv set cluster state {state}"),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await?;

        ssh.exec("nv config apply --assume-yes", SshClient::DEFAULT_TIMEOUT)
            .await?;

        ssh.exec("nv config save", SshClient::DEFAULT_TIMEOUT)
            .await?;

        Ok(())
    }

    // Set cluster state via SSH then poll NVUE REST until it converges
    pub async fn update_cluster_config(&self, enabled: bool) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        let state = Self::desired_cluster_state(enabled);
        let desired_state = state.as_str();

        tracing::info!(node = %self.id, enabled, "update_cluster_config");
        self.run_cluster_state_nv_commands(desired_state).await?;

        self.wait_for_cluster_state(desired_state, enabled)
            .await
            .map(|_| ())
    }

    async fn wait_for_cluster_state(&self, expected: &str, allow_start: bool) -> Result<Value> {
        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            if let Ok(cluster) = self
                .nvue_http_get("/nvue_v1/cluster", HttpClient::DEFAULT_TIMEOUT)
                .await
                && let Some(state) = cluster.get("state").and_then(|v| v.as_str())
                && (state == expected || (allow_start && state == "start"))
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
            .nvue_http_get("/nvue_v1/cluster", HttpClient::DEFAULT_TIMEOUT)
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
                    "Enable cluster with update_cluster_config before performing cluster operations"
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
