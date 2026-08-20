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

//! NVUE node IP address reconciliation.

use nvue_client::ClientError;
use nvue_client::cluster::{ClusterNodeServerAddresses, InterfaceType, NMX_CONTROLLER_APP_NAME};
use nvue_client::revision::APPLIED_REVISION_ID;

use super::super::{SwitchGb200Nvidia, config};
use super::RETRY_WAIT_DELAY;
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{Result, RmsError};

impl SwitchGb200Nvidia {
    /// Reads the applied node IP addresses for one cluster interface.
    pub(crate) async fn get_node_ips(
        &self,
        interface_type: InterfaceType,
    ) -> Result<ClusterNodeServerAddresses> {
        let _op_guard = self.op_lock.lock().await;
        self.read_node_ips(interface_type).await
    }

    /// Replaces the applied node IP addresses when observed state differs.
    pub(crate) async fn reconcile_node_ips(
        &self,
        interface_type: InterfaceType,
        desired: &ClusterNodeServerAddresses,
    ) -> Result<bool> {
        let _op_guard = self.op_lock.lock().await;
        self.reconcile_node_ips_unlocked(interface_type, desired)
            .await
    }

    async fn reconcile_node_ips_unlocked(
        &self,
        interface_type: InterfaceType,
        desired: &ClusterNodeServerAddresses,
    ) -> Result<bool> {
        let current = self.read_node_ips(interface_type).await?;

        if current == *desired {
            tracing::info!(
                node = %self.id,
                interface_type = interface_type.as_str(),
                node_ip_count = desired.len(),
                "node IP addresses already match"
            );

            return Ok(false);
        }

        // Read the created revision instead of reusing the applied snapshot.
        // NVUE can change outside op_lock, so staging must use the revision's
        // actual baseline to avoid merging stale applied state.
        let revision_id = self.create_revision().await?;
        let nvue = self.nvue_client()?;

        let revision_current = nvue
            .get_cluster_node_servers(interface_type, &revision_id, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        if revision_current == *desired {
            self.discard_candidate_revision(&revision_id).await?;

            tracing::info!(
                node = %self.id,
                interface_type = interface_type.as_str(),
                node_ip_count = desired.len(),
                "node IP addresses already match; candidate revision discarded"
            );

            return Ok(false);
        }

        let staged = nvue
            .stage_cluster_node_servers(
                interface_type,
                &revision_id,
                &revision_current,
                desired,
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;

        debug_assert!(
            staged,
            "stage_cluster_node_servers returned false after revision-state equality check"
        );

        self.nvue_start_config_revision(&revision_id).await?;
        self.nvue_finish_config_revision(&revision_id).await?;

        let observed = self.read_node_ips(interface_type).await?;

        if observed != *desired {
            return Err(RmsError::failed_precondition(format!(
                "NVUE {} node IP addresses did not match requested state: requested \
                 {desired:?}, observed {observed:?}",
                interface_type.as_str()
            )));
        }

        tracing::info!(
            node = %self.id,
            interface_type = interface_type.as_str(),
            previous_node_ip_count = current.len(),
            node_ip_count = desired.len(),
            "node IP addresses configured"
        );

        Ok(true)
    }

    /// Clears primary node IPs, disables the cluster, and waits for shutdown.
    ///
    /// NVOS rejects cluster disable while cluster-node servers remain
    /// configured, so this method clears addresses before disabling the cluster.
    pub(crate) async fn clear_node_ips_and_disable_cluster(&self) -> Result<()> {
        // Keep both mutations and shutdown convergence in one operation so no
        // cluster-state or node-IP update can interleave with the transition.
        let _op_guard = self.op_lock.lock().await;

        self.reconcile_node_ips_unlocked(
            InterfaceType::Primary,
            &ClusterNodeServerAddresses::new(),
        )
        .await?;

        self.set_cluster_state_unlocked(false).await?;

        // The applied state can briefly report disabled before the controller
        // shutdown finishes. Wait for both before enabling another switch.
        let nvue = self.nvue_client()?;

        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            let cluster_disabled = match nvue.get_cluster(HttpClient::DEFAULT_TIMEOUT).await {
                Ok(cluster) => cluster.is_disabled(),
                Err(error) => {
                    tracing::debug!(
                        node = %self.id,
                        attempt,
                        %error,
                        "failed to read cluster state while waiting for shutdown"
                    );

                    false
                }
            };

            let controller_stopped = match nvue
                .get_cluster_app(NMX_CONTROLLER_APP_NAME, HttpClient::DEFAULT_TIMEOUT)
                .await
            {
                Ok(app) => app.is_stopped(),
                Err(ClientError::HttpStatus { status: 404, .. }) => true,
                Err(error) => {
                    tracing::debug!(
                        node = %self.id,
                        attempt,
                        %error,
                        "failed to read controller state while waiting for shutdown"
                    );

                    false
                }
            };

            tracing::debug!(
                node = %self.id,
                attempt,
                cluster_disabled,
                controller_stopped,
                "waiting for disabled cluster controller shutdown"
            );

            if cluster_disabled && controller_stopped {
                return Ok(());
            }

            if attempt < config::MAX_RETRY_ATTEMPTS {
                tokio::time::sleep(RETRY_WAIT_DELAY).await;
            }
        }

        Err(RmsError::failed_precondition(
            "cluster controller did not stop after disabling cluster",
        ))
    }

    async fn read_node_ips(
        &self,
        interface_type: InterfaceType,
    ) -> Result<ClusterNodeServerAddresses> {
        self.nvue_client()?
            .get_cluster_node_servers(
                interface_type,
                APPLIED_REVISION_ID,
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await
            .map_err(Into::into)
    }
}
