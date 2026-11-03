/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stages static configuration and saved links before controller activation.

use std::collections::{BTreeMap, BTreeSet};

use super::{ManagedSwitch, Reconciliation};
use super::{observations, reconcile_links};
use crate::config::fabric_layout::FabricLinkState;
use crate::libnmxc::managed_config::{ManagedConfig, reconcile_config};

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

impl<'a> Reconciliation<'a> {
    /// Stops obsolete primaries and stages FM values in isolation before applying
    /// target link states.
    pub(super) async fn prepare(&mut self) -> Result<(), String> {
        for index in &self.changed_groups {
            let primary = self
                .plan
                .fabrics
                .get(*index)
                .map(|fabric| &fabric.assignment.primary_switch_node_id)
                .ok_or_else(|| "resolved fabric has no primary".to_owned())?;

            self.unverified_primaries.insert(primary.clone());
        }

        // Remove prior proof before any controller or FM mutation.
        let pending: Vec<_> = self
            .controller
            .switches
            .iter()
            .filter(|switch| self.unverified_primaries.contains(&switch.node.node_id))
            .map(ManagedSwitch::controller_identity)
            .collect();

        self.controller
            .service
            .fabric_operations
            .mark_pending(&self.operation.job_id, &pending)
            .map_err(|_| "cannot mark controller activation pending".to_owned())?;

        self.operation.pending_controllers.extend(pending);
        self.publish()?;

        for switch in self.controller.switches {
            self.controller.check_cancelled()?;

            if self.unverified_primaries.contains(&switch.node.node_id) {
                self.publish()?;

                switch
                    .prepared
                    .switch
                    .stop_cluster_controller()
                    .await
                    .map_err(|error| {
                        format!(
                            "cannot verify controller shutdown on switch '{}': {}",
                            switch.node.node_id, error.code
                        )
                    })?;
            }
        }

        self.normalize_static_configs().await?;
        self.prepare_links().await?;
        self.verify_links().await?;

        let selected: BTreeSet<_> = self
            .plan
            .fabrics
            .iter()
            .map(|fabric| &fabric.assignment.primary_switch_node_id)
            .collect();

        // Verified disabled non-primaries have no requested controller activation.
        for switch in self.controller.switches {
            if !selected.contains(&switch.node.node_id) {
                self.controller
                    .service
                    .fabric_operations
                    .clear_inactive(&self.operation.job_id, &switch.controller_identity())
                    .map_err(|_| "cannot clear disabled controller activation".to_owned())?;

                self.operation
                    .pending_controllers
                    .remove(&switch.controller_identity());
            }
        }

        self.unverified_primaries.retain(|id| selected.contains(id));

        self.publish()
    }

    /// Reconciles all switches concurrently and drains writes before recovery.
    pub(super) async fn prepare_links(&mut self) -> Result<(), String> {
        let controller = self.controller;
        let plan = self.plan;

        let mut switches = controller.switches.iter();
        let mut pending = FuturesUnordered::new();
        let mut first_error = None;

        loop {
            while first_error.is_none() && !controller.job.cancellation_token().is_cancelled() {
                let Some(switch) = switches.next() else {
                    break;
                };

                let Some(target) = plan.links.get(&switch.node.node_id) else {
                    first_error = Some("switch has no managed link target".to_owned());
                    break;
                };

                if let Err(error) = self.publish() {
                    first_error = Some(error);
                    break;
                }

                let unverified = self.unverified_primaries.contains(&switch.node.node_id);
                let startup_drift = self.startup_drift.contains(&switch.node.node_id);

                // Each future owns one switch's save, write, and readback sequence.
                pending.push(async move {
                    std::panic::AssertUnwindSafe(async {
                        if startup_drift && !unverified {
                            switch.save_enabled_cluster().await?;
                        }

                        reconcile_links(switch.prepared.switch.as_ref(), target).await?;

                        Ok(())
                    })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(format!(
                            "switch '{}' link reconciliation panicked",
                            switch.node.node_id
                        ))
                    })
                });
            }

            let Some(result) = pending.next().await else {
                break;
            };

            if let Err(error) = result {
                first_error.get_or_insert(error);
            }

            if let Err(error) = self.publish() {
                first_error.get_or_insert(error);
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        controller.check_cancelled()
    }

    /// Isolates every changed primary before staging FM values concurrently.
    /// All staging operations finish and controllers stop before target link states are applied.
    pub(super) async fn normalize_static_configs(&self) -> Result<(), String> {
        let controller = self.controller;
        let plan = self.plan;

        // Matching stored FM values need only the final activation.
        let isolations = self
            .changed_groups
            .iter()
            .filter(|index| !self.static_config_matches.contains(index))
            .map(|index| async move {
                controller.check_cancelled()?;

                let fabric = plan
                    .fabrics
                    .get(*index)
                    .ok_or_else(|| "resolved fabric index is invalid".to_owned())?;

                let switch = controller.primary(fabric)?;

                let isolated: BTreeMap<_, _> = plan
                    .links
                    .get(&switch.node.node_id)
                    .ok_or_else(|| "primary has no managed link target".to_owned())?
                    .keys()
                    .map(|id| (id.clone(), FabricLinkState::Down))
                    .collect();

                reconcile_links(switch.prepared.switch.as_ref(), &isolated).await?;

                Ok((fabric, switch))
            });

        let isolated = observations::run_parallel(
            isolations,
            Some(&controller.job.cancellation_token()),
            "static configuration isolation",
        )
        .await?;

        tracing::info!(
            primaries = isolated.len(),
            "static configuration isolation verified"
        );

        let staging = isolated.into_iter().map(|(fabric, switch)| async move {
            let config = ManagedConfig::try_from(fabric)?;

            controller.start(switch).await?;
            let mut client = controller.connect(switch, true).await?;

            reconcile_config(client.as_mut(), &config, &controller.service.nmx_gateway_id).await?;

            switch
                .prepared
                .switch
                .stop_cluster_controller()
                .await
                .map_err(|error| {
                    format!(
                        "cannot stop isolated controller on switch '{}': {}",
                        switch.node.node_id, error.message
                    )
                })?;

            Ok(())
        });

        observations::run_parallel(
            staging,
            Some(&controller.job.cancellation_token()),
            "static configuration staging",
        )
        .await?;

        tracing::info!("static configuration staging completed; isolated controllers stopped");

        Ok(())
    }
}
