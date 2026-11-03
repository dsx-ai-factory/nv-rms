/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Activates independent controllers after the shared saved-link barrier.

use super::super::controller_targets::config_digest;
use super::observations;
use super::{ControllerContext, ManagedSwitch, PlannedFabric, Reconciliation};
use crate::api::grpc::scaleupfabricmanager_handlers::wait_for_nmx_controller_ready_for_config;
use crate::libnmxc::Nmxc;
use crate::libnmxc::managed_config::{ManagedConfig, config_matches, read_config};

use librms::protos::rack_manager as rm;
use nvue_client::cluster::NMX_CONTROLLER_APP_NAME;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc;

impl<'a> Reconciliation<'a> {
    /// Activates all groups concurrently after shared link verification.
    /// Drains active groups on failure, preserving independently verified successes.
    pub(super) async fn configure_groups(&mut self) -> Result<(), String> {
        let controller = self.controller;
        let plan = self.plan;
        let (progress_tx, mut progress_rx) = mpsc::channel(plan.fabrics.len().max(1));

        let mut pending = FuturesUnordered::new();
        let mut next_index = 0;
        let mut first_error = None;

        loop {
            while first_error.is_none()
                && !controller.job.cancellation_token().is_cancelled()
                && next_index < plan.fabrics.len()
            {
                let index = next_index;

                let fabric = plan
                    .fabrics
                    .get(index)
                    .ok_or_else(|| "resolved fabric index is invalid".to_owned())?;

                // Confirm active ownership before dispatching device I/O.
                if let Err(error) = self.publish() {
                    first_error.get_or_insert(error);
                    break;
                }

                let changed = self.changed_groups.contains(&index);
                let progress = progress_tx.clone();

                pending.push(async move {
                    let result = std::panic::AssertUnwindSafe(
                        controller.configure_group(fabric, changed, progress),
                    )
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(format!("fabric group {index} activation panicked")));

                    (index, result)
                });

                next_index += 1;
            }

            if pending.is_empty() {
                break;
            }

            tokio::select! {
                // Process progress before completion so ownership loss stops scheduling.
                biased;
                Some(()) = progress_rx.recv() => {
                    if let Err(error) = self.publish() {
                        first_error.get_or_insert(error);
                    }
                }
                Some((index, result)) = pending.next() => {
                    let result = result.and_then(|()| self.complete_group(index));

                    if let Err(error) = result {
                        first_error.get_or_insert(error);
                    }

                    if let Err(error) = self.publish() {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => controller.check_cancelled(),
        }
    }

    /// Records verified activation without changing sibling groups' ownership.
    pub(super) fn complete_group(&mut self, index: usize) -> Result<(), String> {
        let fabric = self
            .plan
            .fabrics
            .get(index)
            .ok_or_else(|| "resolved fabric index is invalid".to_owned())?;

        let switch = self.controller.primary(fabric)?;
        let changed = self.changed_groups.contains(&index);

        if changed {
            let scopes: Vec<_> = self
                .controller
                .switches
                .iter()
                .filter(|member| {
                    fabric
                        .assignment
                        .switch_node_ids
                        .contains(&member.node.node_id)
                })
                .map(ManagedSwitch::scopes)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();

            let scope_keys = crate::api::grpc::switch_scope::fabric_scope_keys(&scopes)
                .map_err(|_| "cannot encode controller admission aliases".to_owned())?;

            self.controller
                .service
                .fabric_operations
                .record_activation(
                    &self.operation.job_id,
                    switch.controller_identity(),
                    config_digest(fabric)?,
                    scope_keys,
                )
                .map_err(|_| "cannot record verified controller activation".to_owned())?;

            self.operation
                .pending_controllers
                .remove(&switch.controller_identity());
        }

        let primary_id = switch.node.node_id.clone();
        self.unverified_primaries.remove(&primary_id);

        Ok(())
    }

    /// Verifies the entire shared administrative target, including unassigned switches.
    pub(super) async fn verify_links(&self) -> Result<(), String> {
        let plan = self.plan;

        let reads = self.controller.switches.iter().map(|switch| async move {
            let target = plan
                .links
                .get(&switch.node.node_id)
                .ok_or_else(|| "switch has no managed link target".to_owned())?;

            let snapshot = switch
                .prepared
                .switch
                .get_scale_up_link_snapshot()
                .await
                .map_err(|error| {
                    format!(
                        "cannot verify saved links on switch '{}': {}",
                        switch.node.node_id, error.code
                    )
                })?;

            if !observations::links_match(&snapshot, target)? {
                return Err(format!(
                    "saved links do not match target on switch '{}'",
                    switch.node.node_id
                ));
            }

            Ok(())
        });

        observations::run_parallel(
            reads,
            Some(&self.controller.job.cancellation_token()),
            "saved link verification",
        )
        .await?;

        Ok(())
    }
}

impl<'a> ControllerContext<'a> {
    /// Starts a changed group and polls its stored configuration and live controller state.
    ///
    /// Unchanged groups are read without restarting. Success requires configured
    /// control-plane state, healthy controller software, and expected switch
    /// membership; GPU and operational link health are reported separately.
    pub(super) async fn configure_group(
        &self,
        fabric: &PlannedFabric,
        changed: bool,
        progress: mpsc::Sender<()>,
    ) -> Result<(), String> {
        let switch = self.primary(fabric)?;
        let config = ManagedConfig::try_from(fabric)?;

        if changed {
            self.start(switch).await?;
        }

        let deadline =
            tokio::time::Instant::now() + super::super::super::NMX_CONTROLLER_CONFIGURED_TIMEOUT;

        let mut last_observation = None;

        loop {
            self.check_cancelled()?;
            let mut client = self.connect(switch, true).await?;

            let current =
                read_config(client.as_mut(), &config, &self.service.nmx_gateway_id).await?;

            if !config_matches(&current, &config) {
                return Err(format!(
                    "managed configuration readback failed on primary '{}'",
                    switch.node.node_id
                ));
            }

            let observation = self.sample(fabric, client.as_mut()).await?;

            let state = (
                observation.control_plane_state,
                observation.health.controller,
                observation.membership_matches,
            );

            if last_observation != Some(state) {
                tracing::info!(
                    node_id = switch.node.node_id,
                    control_plane_state = ?observation.control_plane_state,
                    controller_health = observation.health.controller,
                    membership_matches = observation.membership_matches,
                    "fabric controller convergence observed"
                );

                last_observation = Some(state);
            }

            if is_controller_ready(&observation) && observation.membership_matches {
                return Ok(());
            }

            progress
                .send(())
                .await
                .map_err(|_| "fabric activation coordinator stopped".to_owned())?;

            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "primary '{}' did not reach configured state with the requested membership",
                    switch.node.node_id
                ));
            }

            tokio::time::sleep(super::super::super::NMX_CONTROLLER_CONFIGURED_POLL_DELAY).await;
        }
    }

    /// Opens the controller API using the existing V2 security and readiness owners.
    pub(super) async fn start(&self, switch: &ManagedSwitch) -> Result<(), String> {
        self.check_cancelled()?;
        switch
            .prepared
            .switch
            .set_cluster_state(true)
            .await
            .map_err(|error| {
                format!(
                    "cannot enable primary '{}': {}",
                    switch.node.node_id, error.code
                )
            })?;

        // Secure startup delegates exposure to the security owner, which first
        // validates TLS material. Insecure startup waits before clearing a binding.
        if self.service.switch_tls_roots.insecure_switch {
            switch
                .prepared
                .switch
                .enable_grpc_for_external_clients(NMX_CONTROLLER_APP_NAME, true)
                .await
                .map_err(|error| {
                    format!(
                        "cannot expose controller API on primary '{}': {}",
                        switch.node.node_id, error.code
                    )
                })?;

            wait_for_nmx_controller_ready_for_config(
                switch.prepared.switch.as_ref(),
                &switch.prepared.target_ip.to_string(),
            )
            .await
            .map_err(|error| {
                format!(
                    "controller API did not become ready on primary '{}': {}",
                    switch.node.node_id, error.code
                )
            })?;
        }

        self.service
            .enforce_primary_nmx_controller_security(
                switch.prepared.switch.as_ref(),
                &switch.prepared.target_ip.to_string(),
                &switch.prepared.tls_server_name,
                self.domain,
                self.job.cancellation_token(),
            )
            .await
            .map_err(|error| {
                format!(
                    "cannot enforce controller security on primary '{}': {}",
                    switch.node.node_id, error.code
                )
            })
    }

    /// Connects to NMX-C with the V2 TLS policy and completes Hello.
    /// Setting `retry_unavailable_hello` retries Hello on `Unavailable` responses.
    pub(super) async fn connect(
        &self,
        switch: &ManagedSwitch,
        retry_unavailable_hello: bool,
    ) -> Result<Box<dyn Nmxc>, String> {
        self.service
            .connect_nmx_controller(
                &switch.prepared.target_ip.to_string(),
                &switch.prepared.tls_server_name,
                self.domain,
                retry_unavailable_hello,
            )
            .await
            .map_err(|_| {
                format!(
                    "cannot read controller API on primary '{}'",
                    switch.node.node_id
                )
            })
    }

    /// Resolves an admitted primary by request identity, without selecting a replacement.
    pub(super) fn primary(&self, fabric: &PlannedFabric) -> Result<&'a ManagedSwitch, String> {
        self.switches
            .iter()
            .find(|switch| switch.node.node_id == fabric.assignment.primary_switch_node_id)
            .ok_or_else(|| "resolved primary has no discovered switch".to_owned())
    }

    /// Compares controller observations with the group's discovered physical members.
    pub(super) async fn sample(
        &self,
        fabric: &PlannedFabric,
        client: &mut dyn Nmxc,
    ) -> Result<observations::GroupObservation, String> {
        let switches: Vec<_> = self
            .switches
            .iter()
            .filter(|switch| {
                fabric
                    .assignment
                    .switch_node_ids
                    .contains(&switch.node.node_id)
            })
            .map(|switch| switch.placement.clone())
            .collect();

        let computes: Vec<_> = self
            .computes
            .iter()
            .filter(|compute| {
                fabric
                    .assignment
                    .compute_node_ids
                    .contains(&compute.node_id)
            })
            .cloned()
            .collect();

        observations::observe_group(client, &switches, &computes, &self.service.nmx_gateway_id)
            .await
    }

    /// Stops further workflow dispatch after cooperative cancellation is requested.
    pub(super) fn check_cancelled(&self) -> Result<(), String> {
        if self.job.cancellation_token().is_cancelled() {
            Err("managed reconciliation cancelled".to_owned())
        } else {
            Ok(())
        }
    }
}

/// Requires control-plane convergence and controller health, not data-plane health.
pub(super) fn is_controller_ready(observation: &observations::GroupObservation) -> bool {
    observation.control_plane_state == rm::ScaleUpFabricControlPlaneState::Configured
        && observation.health.controller == rm::ScaleUpFabricReportedStatus::Normal as i32
}
