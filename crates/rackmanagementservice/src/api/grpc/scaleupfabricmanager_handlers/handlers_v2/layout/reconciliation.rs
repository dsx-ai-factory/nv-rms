/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared saved-link preparation followed by independently verified controllers.
//!
//! Changed controllers stop before shared connectivity changes. Configuration
//! staging uses isolated primaries; all target links are saved and verified
//! before group activation. Failure preserves verified groups and releases
//! ownership after cleanup attempts rather than restoring an obsolete layout.

mod activation;
mod preparation;

use std::collections::{BTreeMap, BTreeSet};

use self::activation::is_controller_ready;
use super::controller_targets::config_digest;
use super::links::reconcile_links;
use super::observations::{self, ManagedSwitch};
use super::planner::{FabricPlan, ObservedPlacement, PlannedFabric};
use crate::api::grpc::server::RackManagerServiceImpl;
use crate::config::fabric_layout::FabricLinkState;
use crate::libnmxc::managed_config::{ManagedConfig, config_matches, read_config};
use crate::metrics::fabric_workflow::{Stage, StageTimer, observe};
use crate::orchestrator::fabric_operation::FabricOperation;
use crate::orchestrator::job_lifecycle::{JobError, JobFailure};
use crate::orchestrator::job_tracker::RmsJobHandle;

#[cfg(test)]
use librms::protos::rack_manager as rm;
#[cfg(test)]
use librms::protos::rack_manager_v2 as rm_v2;

use futures::FutureExt;

/// Immutable device context shared by independently polled fabric activations.
#[derive(Clone, Copy)]
struct ControllerContext<'a> {
    service: &'a RackManagerServiceImpl,
    job: &'a RmsJobHandle,
    switches: &'a [ManagedSwitch],
    computes: &'a [ObservedPlacement],
    domain: Option<&'a str>,
}

/// One admitted operation owns every switch through verification and cleanup.
struct Reconciliation<'a> {
    controller: ControllerContext<'a>,
    operation: FabricOperation,
    plan: &'a FabricPlan,

    /// Groups requiring activation because links, stored values, or proof differ.
    changed_groups: BTreeSet<usize>,

    /// Groups whose stored managed values already match and need no staging writes.
    static_config_matches: BTreeSet<usize>,

    /// Switches whose saved cluster state differs from their selected-primary role.
    startup_drift: BTreeSet<String>,

    /// Controllers that cannot yet be excluded from failure compensation.
    unverified_primaries: BTreeSet<String>,
}

impl RackManagerServiceImpl {
    /// Runs the owned workflow and completes the job only after verification.
    ///
    /// Failure drains started work and attempts controller cleanup before releasing
    /// fabric ownership and recording the terminal job outcome.
    /// A successful full-capacity target also captures single-fabric status;
    /// callers use live inspection for current layout observations. If full-rack
    /// status collection fails, the job fails and keeps the partial status in its result.
    pub(super) async fn run_managed_fabric_job(
        &self,
        job: RmsJobHandle,
        operation: FabricOperation,
        switches: Vec<ManagedSwitch>,
        computes: Vec<ObservedPlacement>,
        plan: FabricPlan,
        domain: Option<String>,
    ) {
        let cancellation = job.cancellation_token();
        let timer = StageTimer::start(Stage::Workflow);

        tracing::info!(
            switch_count = switches.len(),
            compute_count = computes.len(),
            fabric_count = plan.fabrics.len(),
            full_topology = plan.full_topology,
            "managed fabric reconciliation started"
        );

        let mut worker = Reconciliation {
            controller: ControllerContext {
                service: self,
                job: &job,
                switches: &switches,
                computes: &computes,
                domain: domain.as_deref(),
            },
            operation,
            plan: &plan,
            changed_groups: BTreeSet::new(),
            static_config_matches: BTreeSet::new(),
            startup_drift: BTreeSet::new(),
            unverified_primaries: BTreeSet::new(),
        };

        // Readback and completion share the worker's panic boundary so every
        // failure reaches cleanup before the job releases its reservations.
        let result = std::panic::AssertUnwindSafe(async {
            worker
                .run()
                .await
                .map_err(|error| JobFailure::new(JobError::Other, error))?;

            let result_json = if plan.full_topology {
                let status = observe(
                    Stage::Readback,
                    &cancellation,
                    self.workflow_timings.fabric_config_wait_log_interval,
                    self.read_scale_up_fabric_status(
                        &switches
                            .iter()
                            .map(|switch| switch.node.clone())
                            .collect::<Vec<_>>(),
                        domain.as_deref(),
                        &[],
                    ),
                )
                .await
                .map_err(|(message, partial_status)| {
                    let failure = JobFailure::new(JobError::Other, message);

                    // Include completed switch reads in the failed job's result.
                    match serde_json::to_string(&partial_status) {
                        Ok(result_json) => failure.with_result_json(result_json),
                        Err(error) => {
                            tracing::warn!(%error, "failed to serialize partial fabric status");

                            failure
                        }
                    }
                })?;

                serde_json::to_string(&status).map_err(|error| {
                    JobFailure::new(
                        JobError::Other,
                        format!("full-rack status encoding failed: {error}"),
                    )
                })?
            } else {
                String::new()
            };

            worker
                .publish()
                .map_err(|error| JobFailure::new(JobError::Other, error))?;

            self.fabric_operations
                .finish(&worker.operation.job_id)
                .map_err(|_| {
                    JobFailure::new(JobError::Other, "cannot record managed completion")
                })?;

            Ok(result_json)
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
            Err(JobFailure::new(
                JobError::Other,
                "managed reconciliation worker panicked",
            ))
        });

        let succeeded = match result {
            Ok(result_json) => {
                job.complete_verified("Fabric configuration verified", result_json);

                true
            }
            Err(mut failure) => {
                failure.message = worker.fail(failure.message).await;

                job.fail(failure);

                false
            }
        };

        timer.finish(succeeded, cancellation.is_cancelled());
    }
}

impl<'a> Reconciliation<'a> {
    /// Inspects, prepares, activates, and verifies while retaining every switch alias.
    async fn run(&mut self) -> Result<(), String> {
        let cancellation = self.controller.job.cancellation_token();

        for pending in &self.operation.pending_controllers {
            let switch = self
                .controller
                .switches
                .iter()
                .find(|switch| switch.controller_identity() == *pending)
                .ok_or_else(|| {
                    "pending controller activation has no discovered physical switch".to_owned()
                })?;

            self.unverified_primaries
                .insert(switch.node.node_id.clone());
        }

        self.publish()?;

        observe(
            Stage::Inspection,
            &cancellation,
            self.controller
                .service
                .workflow_timings
                .fabric_config_wait_log_interval,
            self.inspect(),
        )
        .await?;

        tracing::info!(
            changed_group_count = self.changed_groups.len(),
            matching_static_config_count = self.static_config_matches.len(),
            startup_drift_count = self.startup_drift.len(),
            "managed fabric drift classified"
        );

        self.controller.check_cancelled()?;

        observe(
            Stage::Preparation,
            &cancellation,
            self.controller
                .service
                .workflow_timings
                .fabric_config_wait_log_interval,
            self.prepare(),
        )
        .await?;

        self.publish()?;

        // Activation starts only after every saved shared-link target has been verified.
        observe(
            Stage::Activation,
            &cancellation,
            self.controller
                .service
                .workflow_timings
                .fabric_config_wait_log_interval,
            self.configure_groups(),
        )
        .await?;

        observe(
            Stage::Verification,
            &cancellation,
            self.controller
                .service
                .workflow_timings
                .fabric_config_wait_log_interval,
            async {
                self.verify_links().await?;

                let plan = self.plan;

                let reads = self.controller.switches.iter().map(|switch| async move {
                    let selected = plan.fabrics.iter().any(|fabric| {
                        fabric.assignment.primary_switch_node_id == switch.node.node_id
                    });

                    if switch.enabled().await? != selected
                        || switch.startup_enabled().await? != selected
                    {
                        return Err(format!(
                            "switch '{}' cluster readback does not match selected primaries",
                            switch.node.node_id
                        ));
                    }

                    Ok(())
                });

                observations::run_parallel(
                    reads,
                    Some(&self.controller.job.cancellation_token()),
                    "cluster verification",
                )
                .await?;

                self.publish()
            },
        )
        .await
    }

    /// Strict reads classify drift; failed reads never authorize corrective writes.
    async fn inspect(&mut self) -> Result<(), String> {
        let controller = self.controller;
        let plan = self.plan;

        let reads = controller.switches.iter().map(|switch| async move {
            controller.check_cancelled()?;

            let target = plan
                .links
                .get(&switch.node.node_id)
                .ok_or_else(|| "switch has no complete managed link target".to_owned())?;

            let snapshot = switch
                .prepared
                .switch
                .get_scale_up_link_snapshot()
                .await
                .map_err(|error| {
                    format!(
                        "cannot read saved links on switch '{}': {}",
                        switch.node.node_id, error.code
                    )
                })?;

            let matches = observations::links_match(&snapshot, target)?;

            // If any link setting differs, reconcile_links writes every requested
            // setting. For example, keeping link 1 Up while changing link 2 to
            // Down still writes Up for link 1. This flag identifies writes that
            // include Up; it does not imply a Down-to-Up transition. Preparation
            // verifies nonprimary controller shutdown before those writes.
            let link_changes_include_up =
                !matches && target.values().any(|state| *state == FabricLinkState::Up);

            let enabled = switch.enabled().await?;
            let startup_enabled = switch.startup_enabled().await?;

            Ok((matches, link_changes_include_up, enabled, startup_enabled))
        });

        let states = observations::run_parallel(
            reads,
            Some(&controller.job.cancellation_token()),
            "initial switch inspection",
        )
        .await?;

        for (switch, (matches, link_changes_include_up, enabled, startup_enabled)) in
            controller.switches.iter().zip(states)
        {
            controller.check_cancelled()?;

            let selected = self
                .plan
                .fabrics
                .iter()
                .any(|fabric| fabric.assignment.primary_switch_node_id == switch.node.node_id);

            if startup_enabled != selected {
                self.startup_drift.insert(switch.node.node_id.clone());
            }

            if !matches {
                self.changed_groups
                    .extend(
                        self.plan
                            .fabrics
                            .iter()
                            .enumerate()
                            .filter_map(|(index, fabric)| {
                                fabric
                                    .assignment
                                    .switch_node_ids
                                    .contains(&switch.node.node_id)
                                    .then_some(index)
                            }),
                    );
            }

            if let Some((index, fabric)) =
                self.plan.fabrics.iter().enumerate().find(|(_, fabric)| {
                    fabric.assignment.primary_switch_node_id == switch.node.node_id
                })
            {
                let pending = self
                    .operation
                    .pending_controllers
                    .contains(&switch.controller_identity());

                if enabled {
                    let config = ManagedConfig::try_from(fabric)?;
                    let mut client = self.controller.connect(switch, false).await?;

                    let current = read_config(
                        client.as_mut(),
                        &config,
                        &self.controller.service.nmx_gateway_id,
                    )
                    .await?;

                    if config_matches(&current, &config) {
                        self.static_config_matches.insert(index);
                    }

                    let observation = self.controller.sample(fabric, client.as_mut()).await?;

                    if !observation.membership_observed || !observation.controller_observed {
                        return Err(format!(
                            "controller or membership read unavailable on primary '{}': {}",
                            switch.node.node_id,
                            observation.errors.join("; ")
                        ));
                    }

                    // Matching files do not prove that the running controller
                    // consumed them. A process-local activation proof is also
                    // required; a service restart deliberately loses that proof.
                    let proven = self
                        .controller
                        .service
                        .fabric_operations
                        .has_activation_proof(
                            &switch.controller_identity(),
                            &config_digest(fabric)?,
                        )
                        .map_err(|_| "cannot read controller activation proof".to_owned())?;

                    if !proven
                        || !self.static_config_matches.contains(&index)
                        || pending
                        || !observation.membership_matches
                        || !is_controller_ready(&observation)
                    {
                        self.changed_groups.insert(index);
                    }
                } else {
                    self.changed_groups.insert(index);
                }
            } else if enabled || startup_enabled || link_changes_include_up {
                // Disabled cluster settings do not prove NMX-C has stopped.
                // Queue this controller for shutdown verification in preparation.
                self.unverified_primaries
                    .insert(switch.node.node_id.clone());
            }
        }

        self.publish()
    }

    /// Refreshes job progress and rejects workers that lost active ownership.
    fn publish(&self) -> Result<(), String> {
        if !self
            .controller
            .job
            .progress("Reconciling fabric configuration")
        {
            return Err("managed job no longer owns an active worker".to_owned());
        }

        Ok(())
    }

    /// Attempts cleanup and releases ownership even if cleanup fails or panics.
    /// Returns the original failure with any cleanup or release failure noted.
    async fn fail(&mut self, mut error: String) -> String {
        let timer = StageTimer::start(Stage::Recovery);

        let cleanup_failed = std::panic::AssertUnwindSafe(self.cleanup_failed_controllers())
            .catch_unwind()
            .await
            .unwrap_or_else(|_| {
                tracing::warn!("managed controller cleanup panicked");

                true
            });

        if cleanup_failed {
            error.push_str("; controller cleanup could not be verified");
        }

        let ownership_released = match self
            .controller
            .service
            .fabric_operations
            .release_failed(&self.operation.job_id)
        {
            Ok(()) => true,
            Err(release_error) => {
                tracing::warn!(error = %release_error, "cannot release failed fabric operation");
                error.push_str("; fabric ownership could not be released");

                false
            }
        };

        tracing::warn!(
            pending_controller_count = self.operation.pending_controllers.len(),
            manual_intervention_required = cleanup_failed || !ownership_released,
            ownership_released,
            error,
            "managed fabric recovery finished"
        );

        timer.finish(!cleanup_failed && ownership_released, false);

        error
    }

    /// Stops and isolates uncertain controllers without changing verified siblings.
    /// Returns whether any cleanup attempt or progress publication failed.
    async fn cleanup_failed_controllers(&mut self) -> bool {
        // Inspection alone does not authorize compensation. Pending activation is
        // recorded before writes, or carried from an earlier unresolved operation.
        self.unverified_primaries.retain(|id| {
            self.controller.switches.iter().any(|switch| {
                switch.node.node_id == *id
                    && self
                        .operation
                        .pending_controllers
                        .contains(&switch.controller_identity())
            })
        });

        let mut manual_intervention_required = false;

        for switch in self.controller.switches {
            if !self.unverified_primaries.contains(&switch.node.node_id) {
                continue;
            }

            if self.publish().is_err() {
                manual_intervention_required = true;
            }

            let stopped = switch
                .prepared
                .switch
                .stop_cluster_controller()
                .await
                .is_ok();

            let isolated: BTreeMap<_, _> = self
                .plan
                .links
                .get(&switch.node.node_id)
                .into_iter()
                .flat_map(|links| links.keys())
                .map(|id| (id.clone(), FabricLinkState::Down))
                .collect();

            let isolated = stopped
                && reconcile_links(switch.prepared.switch.as_ref(), &isolated)
                    .await
                    .is_ok();

            if !isolated {
                manual_intervention_required = true;
            }
        }

        if self.publish().is_err() {
            manual_intervention_required = true;
        }

        manual_intervention_required
    }
}

#[cfg(test)]
mod managed_worker_tests;

#[cfg(test)]
mod readiness_tests;
