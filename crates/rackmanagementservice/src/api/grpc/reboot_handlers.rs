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

//! `ExecuteColdReboot` handler and its staged orchestration worker.
//!
//! A cold reboot power-cycles a whole DGX GB200 rack: gracefully shut down the
//! compute trays over BMC, power OFF the power shelves, wait for the capacitor
//! discharge, power the shelves back ON, wait for the compute/switch tray BMCs
//! and switch NVOS to come up, then power ON the compute nodes. (Switch fabric
//! provisioning is out of scope — handled outside the cold reboot.) The RPC is
//! asynchronous: it
//! validates and reserves the target nodes, then returns a coordinator job ID.
//!
//! Progress is observed via `GetJobStatus` on that job. **While the reboot is
//! running**, `execution_state` is `RUNNING` and `state_description` reports the
//! current stage by its rack-reboot runbook step number(s) (e.g. `"Rack reboot
//! step 7-8: Waiting for switch NVOS readiness"`); the job framework has no
//! mid-run `result_json` update, so `result_json` is empty until the job is
//! terminal. **On completion or failure**, the full per-stage [`StageTimeline`]
//! (with timings) is written to `result_json`.
//!
//! Exclusion (see the module-level design): the coordinator holds the existing
//! registered rack power guard (for any registered target rack) plus a per-node
//! job-tracker reservation on every participating node, fencing out all tracked
//! operations and registered power RPCs. (`BatchSetPowerState` remains the
//! documented trusted-admin bypass: it consults neither the tracker nor the
//! guard.) The sequence is fail-fast with no automated rollback.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use serde_json::json;
use tokio::sync::OwnedMutexGuard;
use tokio_util::sync::CancellationToken;

use librms::protos::rack_manager as rm;

use crate::api::grpc::conversions::proto_node_type_to_domain;
use crate::api::grpc::power_handlers::build_ephemeral_power_node;
use crate::api::grpc::scaleupfabricmanager_handlers::EphemeralSwitch;
use crate::domain::node::{Node, NodeKind, PowerOp, PowerState, PowerTargetType};
use crate::nodes::NodeInstance;
use crate::orchestrator::job_lifecycle::{JobError, JobFailure};
use crate::orchestrator::job_tracker::{JobType, RmsJobHandle};
use crate::orchestrator::reboot_config::{ColdRebootTimings, NVOS_HELLO_ATTEMPTS, StageTiming};
use crate::orchestrator::stage_timeline::{Stage, StageTimeline};

use super::server::RackManagerServiceImpl;

/// A materialized power/probe target: its node id plus a drivable node handle.
struct NamedNode {
    id: String,
    node: Arc<NodeInstance>,
}

/// All handles a cold-reboot worker needs, materialized up front during
/// synchronous admission so any construction error fails fast before any node
/// is reserved or any power action is taken.
struct ColdRebootTargets {
    /// Power-shelf nodes: powered OFF (step 1) then back ON (step 4).
    shelves: Vec<NamedNode>,
    /// Compute-tray BMC handles: gracefully shut down and verified off
    /// (prestage), reachability-gated (steps 2-3 and 5-6), then powered ON
    /// (step 10).
    computes: Vec<NamedNode>,
    /// Switch-tray BMC handles: used only for reachability gating (steps 2-3
    /// and 5-6).
    switch_bmcs: Vec<NamedNode>,
    /// Switch-tray NVUE handles: used for NVOS readiness gating (steps 7-8).
    switch_nvue: Vec<EphemeralSwitch>,
}

impl ColdRebootTargets {
    /// Compute + switch BMC handles — the nodes whose BMC reachability gates
    /// steps 2-3 (discharged) and 5-6 (BMCs back up). Power shelves are
    /// excluded: they are what supplies power, and their management stays
    /// reachable on standby.
    fn bmc_probe_nodes(&self) -> impl Iterator<Item = &NamedNode> {
        self.computes.iter().chain(self.switch_bmcs.iter())
    }
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_execute_cold_reboot(
        &self,
        req: tonic::Request<rm::ExecuteColdRebootRequest>,
    ) -> std::result::Result<tonic::Response<rm::ExecuteColdRebootResponse>, tonic::Status> {
        let r = req.into_inner();
        let timings = ColdRebootTimings::from_proto(&r);

        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            ..Default::default()
        };

        // ── 1. Build the deduped device set from `nodes` ──
        // `domain` selects the NVL/mTLS material used to build the switch NVUE
        // clients for the NVOS-readiness probe (steps 7-8).
        let domain = r.domain.clone();
        let raw_nodes: Vec<rm::NodeInfo> = r.nodes.clone().map(|ns| ns.nodes).unwrap_or_default();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut devices: Vec<rm::NodeInfo> = Vec::new();
        for ni in raw_nodes {
            let key = (ni.rack_id.clone(), ni.node_id.clone());
            if seen.insert(key) {
                devices.push(ni);
            }
        }
        if devices.is_empty() {
            batch.message = "nodes is required and must contain at least one device".into();
            return Ok(cold_reboot_response(batch, 0, 0, 0));
        }
        let total_nodes = devices.len() as u32;
        let distinct_rack_ids: Vec<String> = {
            let mut racks: Vec<String> = devices
                .iter()
                .map(|d| d.rack_id.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            racks.sort();
            racks
        };
        let parent_rack_id = devices[0].rack_id.clone();

        tracing::info!(
            rack = %parent_rack_id,
            total_nodes,
            "cold reboot request received"
        );

        // ── Validation: a cold reboot targets a single physical rack ──
        if distinct_rack_ids.len() > 1 {
            let racks = distinct_rack_ids.join(", ");
            tracing::warn!(%racks, "cold reboot rejected: devices span multiple racks");
            batch.message = format!(
                "cold reboot must target a single rack, but devices span {} racks: {racks}",
                distinct_rack_ids.len()
            );
            return Ok(cold_reboot_response(batch, total_nodes, 0, total_nodes));
        }

        // Key list (rack_id, node_id) used for per-node reservation and rollback.
        let node_keys: Vec<(String, String)> = devices
            .iter()
            .map(|d| (d.rack_id.clone(), d.node_id.clone()))
            .collect();

        // ── 3. Classify + materialize every device (validation before reservation) ──
        let targets = match self
            .build_cold_reboot_targets(devices, domain, &mut batch)
            .await
        {
            Ok(targets) => targets,
            Err(()) => {
                batch.message = "one or more devices could not be prepared for cold reboot".into();
                return Ok(cold_reboot_response(batch, total_nodes, 0, total_nodes));
            }
        };

        // ── Validation: composition must look like a rack cold reboot ──
        if targets.shelves.is_empty() {
            tracing::warn!(
                rack = %parent_rack_id,
                "cold reboot rejected: no power shelf in device list"
            );
            batch.message =
                "cold reboot requires at least one power shelf in the device list".into();
            return Ok(cold_reboot_response(batch, total_nodes, 0, total_nodes));
        }
        if targets.computes.is_empty() {
            tracing::warn!(
                rack = %parent_rack_id,
                "cold reboot rejected: no compute node in device list"
            );
            batch.message =
                "cold reboot requires at least one compute node in the device list".into();
            return Ok(cold_reboot_response(batch, total_nodes, 0, total_nodes));
        }
        // Shelf power-off cycles the rack's switch trays whether or not they are
        // listed; without them the steps 2-6 BMC gates cannot observe them and
        // the steps 7-8 NVOS gate passes vacuously. Reject rather than skip the
        // switch-recovery verification of hardware this sequence power-cycles.
        if targets.switch_nvue.is_empty() {
            tracing::warn!(
                rack = %parent_rack_id,
                "cold reboot rejected: no switch in device list"
            );
            batch.message = "cold reboot requires at least one switch in the device list".into();
            return Ok(cold_reboot_response(batch, total_nodes, 0, total_nodes));
        }

        // Capture per-type counts for logging before `targets` moves into the worker.
        let shelf_count = targets.shelves.len() as u32;
        let compute_count = targets.computes.len() as u32;
        let switch_count = targets.switch_bmcs.len() as u32;

        // ── 4. Acquire the registered rack power guard for each registered rack ──
        // (Blocks SetPowerState / registered node CRUD.)
        let mut rack_guards: Vec<OwnedMutexGuard<()>> = Vec::new();
        for rack_id in &distinct_rack_ids {
            if let Some(rack) = self.rack_manager.find_rack(rack_id) {
                match rack.try_power_operation_guard() {
                    Ok(guard) => rack_guards.push(guard),
                    Err(_) => {
                        batch.message =
                            format!("rack {rack_id} is busy with another power operation");
                        return Ok(cold_reboot_response(batch, total_nodes, 0, total_nodes));
                    }
                }
            }
        }

        // ── 5. Reserve every participating node (atomic all-or-nothing) ──
        let mut reservation_ids: Vec<String> = Vec::new();
        let mut hold_pendings = Vec::new();
        for (rack_id, node_id) in &node_keys {
            match self.job_tracker.create_job_if_node_idle(
                rack_id,
                node_id,
                JobType::ColdRebootSequence,
            ) {
                Ok(pending) => {
                    reservation_ids.push(pending.id().to_string());
                    hold_pendings.push(pending);
                }
                Err(failure) => {
                    // Roll back every reservation already taken, then reject.
                    for id in &reservation_ids {
                        self.job_tracker.remove_queued_job(id);
                    }
                    tracing::warn!(
                        node = %node_id,
                        rack = %rack_id,
                        reason = %failure.message,
                        "cold reboot rejected: node busy"
                    );
                    batch.message = failure.message;
                    // rack_guards drop here, releasing them.
                    return Ok(cold_reboot_response(batch, total_nodes, 0, total_nodes));
                }
            }
        }

        // ── 6. Spawn each reservation as a hold that lives until released ──
        let release = CancellationToken::new();
        for pending in hold_pendings {
            let rel = release.clone();
            self.job_tracker
                .spawn_job(pending, move |job| async move {
                    rel.cancelled().await;
                    job.complete("cold reboot reservation released", "");
                })
                .detach();
        }

        // ── 7. Create the coordinator leaf and spawn the sequence worker ──
        // The coordinator is the externally visible workflow job (its ID is
        // returned to the caller). Its synthetic per-rack node id also makes it
        // a rack-level sentinel: a second cold reboot on the same rack is
        // rejected by the idle check even if the device lists are disjoint.
        let coordinator_node_id = format!("{parent_rack_id}:cold-reboot");
        let coordinator = match self.job_tracker.create_visible_workflow_job_if_node_idle(
            &parent_rack_id,
            &coordinator_node_id,
            JobType::ColdRebootSequence,
        ) {
            Ok(pending) => pending,
            Err(failure) => {
                // Could not create the coordinator: release the holds (which
                // drops their reservations) and reject.
                release.cancel();
                tracing::error!(
                    rack = %parent_rack_id,
                    reason = %failure.message,
                    "cold reboot rejected: could not create coordinator job"
                );
                batch.message = failure.message;
                return Ok(cold_reboot_response(batch, total_nodes, 0, total_nodes));
            }
        };
        let job_id = coordinator.id().to_string();

        let release_for_worker = release.clone();
        let rack_for_worker = parent_rack_id.clone();
        let handle = self
            .job_tracker
            .spawn_job(coordinator, move |job| async move {
                run_cold_reboot(
                    job,
                    rack_for_worker,
                    targets,
                    timings,
                    release_for_worker,
                    rack_guards,
                )
                .await;
            });

        // Panic-safety backstop: whatever happens to the worker (complete, fail,
        // or panic), release the reservation holds once it terminates.
        let release_for_joiner = release.clone();
        tokio::spawn(async move {
            let _ = handle.wait().await;
            release_for_joiner.cancel();
        });

        tracing::info!(
            job_id = %job_id,
            rack = %parent_rack_id,
            total_nodes,
            shelves = shelf_count,
            computes = compute_count,
            switches = switch_count,
            "cold reboot sequence started"
        );

        batch.job_id = job_id;
        batch.status = rm::ReturnCode::Success.into();
        batch.message = format!(
            "Cold reboot started for {total_nodes} nodes. Use GetJobStatus with response.job_id to \
             track per-stage progress."
        );
        Ok(cold_reboot_response(batch, total_nodes, total_nodes, 0))
    }

    /// Classifies each device by node type and materializes the handles the
    /// worker needs. Any device with an unknown type, or that cannot be built,
    /// is recorded as a per-node failure in `batch`; if *any* device fails, the
    /// whole admission is rejected (returns `Err`) so no partial reboot runs.
    async fn build_cold_reboot_targets(
        &self,
        devices: Vec<rm::NodeInfo>,
        domain: Option<String>,
        batch: &mut rm::NodeBatchResponse,
    ) -> std::result::Result<ColdRebootTargets, ()> {
        let mut shelves = Vec::new();
        let mut computes = Vec::new();
        let mut switch_bmcs = Vec::new();
        let mut switch_nvue = Vec::new();
        let mut had_error = false;

        let fail = |batch: &mut rm::NodeBatchResponse, node_id: &str, message: String| {
            tracing::error!(node = %node_id, error = %message, "cold reboot target rejected");
            batch.node_results.push(rm::NodeOperationResult {
                node_id: node_id.to_owned(),
                status: rm::ReturnCode::Failure.into(),
                error_message: message,
            });
        };

        for device in &devices {
            let node_id = device.node_id.clone();
            let kind = match proto_node_type_to_domain(device.r#type.unwrap_or(0)) {
                Some(node_type) => node_type.kind(),
                None => {
                    fail(batch, &node_id, "unknown or unsupported node type".into());
                    had_error = true;
                    continue;
                }
            };

            match kind {
                NodeKind::Powershelf => match build_ephemeral_power_node(device) {
                    Ok(node) => shelves.push(NamedNode { id: node_id, node }),
                    Err(e) => {
                        fail(batch, &node_id, e);
                        had_error = true;
                    }
                },
                NodeKind::Compute => match build_ephemeral_power_node(device) {
                    Ok(node) => computes.push(NamedNode { id: node_id, node }),
                    Err(e) => {
                        fail(batch, &node_id, e);
                        had_error = true;
                    }
                },
                NodeKind::Switch => {
                    // Switch trays need a BMC handle (reachability gating) and a
                    // separate NVUE handle (NVOS readiness gating). Require both.
                    let bmc = build_ephemeral_power_node(device);
                    let nvue = self.build_ephemeral_switch(device, domain.as_deref()).await;
                    match (bmc, nvue) {
                        (Ok(node), Ok(switch)) => {
                            switch_bmcs.push(NamedNode { id: node_id, node });
                            switch_nvue.push(switch);
                        }
                        (Err(e), _) => {
                            fail(batch, &node_id, e);
                            had_error = true;
                        }
                        (_, Err(e)) => {
                            fail(batch, &node_id, e.message);
                            had_error = true;
                        }
                    }
                }
            }
        }

        if had_error {
            return Err(());
        }

        Ok(ColdRebootTargets {
            shelves,
            computes,
            switch_bmcs,
            switch_nvue,
        })
    }
}

/// Builds the `ExecuteColdRebootResponse` envelope (wrapped in a tonic response)
/// with aggregate stats.
fn cold_reboot_response(
    mut batch: rm::NodeBatchResponse,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) -> tonic::Response<rm::ExecuteColdRebootResponse> {
    batch.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });
    tonic::Response::new(rm::ExecuteColdRebootResponse {
        response: Some(batch),
    })
}

/// The predeclared cold-reboot stage sequence, in worker execution order. The
/// descriptions double as the stage messages shown in the job's live
/// `state_description`, so `to_json()` shows the whole planned pipeline (with
/// these exact texts) from the first poll.
fn cold_reboot_stages() -> Vec<Stage> {
    [
        (
            "prestage_shutdown_compute",
            "Gracefully shutting down compute trays",
        ),
        (
            "prestage_verify_compute_down",
            "Waiting for compute trays to power down",
        ),
        ("power_off_shelves", "Powering off power shelves"),
        (
            "verify_discharged",
            "Waiting for capacitor discharge (tray BMCs unreachable)",
        ),
        ("power_on_shelves", "Powering on power shelves"),
        ("verify_bmc_up", "Waiting for tray BMCs to become reachable"),
        (
            "verify_nvos_ready",
            "Waiting for switch NVOS readiness (nvue hello)",
        ),
        ("power_on_compute", "Powering on compute nodes"),
        ("wait_compute_boot", "Waiting for compute OS boot"),
    ]
    .into_iter()
    .map(|(name, description)| Stage::new(name.to_owned(), description.to_owned()))
    .collect()
}

/// The staged cold-reboot worker. Owns the registered rack guards for the whole
/// sequence (`_rack_guards`, dropped on return) and releases the per-node
/// reservation holds via `release` when it finishes.
async fn run_cold_reboot(
    job: RmsJobHandle,
    rack: String,
    targets: ColdRebootTargets,
    timings: ColdRebootTimings,
    release: CancellationToken,
    _rack_guards: Vec<OwnedMutexGuard<()>>,
) {
    let mut timeline = StageTimeline::with_stages(job.id().to_string(), cold_reboot_stages())
        .with_node_rack(format!("{rack}:cold-reboot"), rack.clone());
    let outcome = execute_cold_reboot_stages(&job, &targets, &timings, &mut timeline).await;

    match outcome {
        Ok(()) => {
            tracing::info!(job_id = %job.id(), rack = %rack, "cold reboot sequence completed");
            job.complete("Cold reboot completed", timeline.to_json().to_string());
        }
        Err(message) => {
            tracing::error!(job_id = %job.id(), rack = %rack, error = %message, "cold reboot sequence failed");
            job.fail(
                JobFailure::new(JobError::Other, message)
                    .with_result_json(timeline.to_json().to_string()),
            );
        }
    }

    // Release the reservation holds now that the sequence is done. The admission
    // joiner also does this on worker panic; `cancel` is idempotent.
    release.cancel();
    // `_rack_guards` drops here, releasing the registered rack guards.
}

/// Runs the prestage (graceful compute shutdown) then the runbook steps
/// (1 through 11-12) in order, updating `timeline` and returning `Err(msg)` on the
/// first stage failure or cancellation (with the timeline already marked). No
/// further power actions are taken after a failure (fail-fast, no rollback).
async fn execute_cold_reboot_stages(
    job: &RmsJobHandle,
    targets: &ColdRebootTargets,
    timings: &ColdRebootTimings,
    timeline: &mut StageTimeline,
) -> std::result::Result<(), String> {
    // Stage numbers below reference the DGX GB200 rack-reboot runbook steps:
    // https://docs.nvidia.com/dgx/dgxgb200-user-guide/rack-reboot-sequence.html
    // Each verify-gate folds a runbook "wait N min" step together with its paired
    // "ping to verify" step, so one gate covers two consecutive runbook steps.

    // Pre-stage: gracefully bring the compute trays down over their BMC
    // BEFORE the shelves are cut, so the compute OS shuts down cleanly instead of
    // losing power abruptly when the busbar de-energizes. Wait for the trays to
    // reach OFF so Step 1 does not interrupt an in-progress shutdown.
    power_stage(
        job,
        timeline,
        "0",
        "prestage_shutdown_compute",
        "Gracefully shutting down compute trays",
        &targets.computes,
        PowerOp::GracefulShutdown,
    )
    .await?;

    gate(
        job,
        timeline,
        "0",
        "prestage_verify_compute_down",
        "Waiting for compute trays to power down",
        timings.prestage_shutdown,
        timings.poll_interval,
        Probe::ComputeDown,
        targets,
    )
    .await?;

    // Step 1: AC power off all power shelves.
    power_stage(
        job,
        timeline,
        "1",
        "power_off_shelves",
        "Powering off power shelves",
        &targets.shelves,
        PowerOp::Off,
    )
    .await?;

    // Steps 2-3: wait for capacitor discharge, then verify tray BMCs are unreachable.
    gate(
        job,
        timeline,
        "2-3",
        "verify_discharged",
        "Waiting for capacitor discharge (tray BMCs unreachable)",
        timings.discharge,
        timings.poll_interval,
        Probe::BmcUnreachable,
        targets,
    )
    .await?;

    // Step 4: AC power on all power shelves.
    power_stage(
        job,
        timeline,
        "4",
        "power_on_shelves",
        "Powering on power shelves",
        &targets.shelves,
        PowerOp::On,
    )
    .await?;

    // Steps 5-6: wait for tray BMCs to power up, then verify they are reachable.
    gate(
        job,
        timeline,
        "5-6",
        "verify_bmc_up",
        "Waiting for tray BMCs to become reachable",
        timings.bmc_up,
        timings.poll_interval,
        Probe::BmcReachable,
        targets,
    )
    .await?;

    // Steps 7-8: switch trays auto-boot to NVOS; verify readiness with an "nvue
    // hello" (GET /nvue_v1/system) on every switch, retried with exponential backoff.
    verify_nvos_ready(
        job,
        timeline,
        "7-8",
        &targets.switch_nvue,
        timings.nvos_hello_backoff,
    )
    .await?;

    // Runbook step 9 (switch tray provisioning / NMX-C start) is intentionally NOT
    // performed by RMS — fabric provisioning is persisted.

    // Step 10: power on all compute nodes via the compute tray BMC.
    power_stage(
        job,
        timeline,
        "10",
        "power_on_compute",
        "Powering on compute nodes",
        &targets.computes,
        PowerOp::On,
    )
    .await?;

    // Steps 11-12: wait a fixed interval for the compute OS to boot. The BMC power
    // state only reports host power, not OS readiness, and RMS has no host/OS boot
    // signal (no Redfish BootProgress in the typed client), so this is a plain wait.
    wait_stage(
        job,
        timeline,
        "11-12",
        "wait_compute_boot",
        "Waiting for compute OS boot",
        timings.compute_boot_wait,
    )
    .await?;

    // Step 13 (post-reboot host/GPU verification: persistenced, imex, links, fabric,
    // P2P topology) is intentionally NOT executed — those are host-OS/GPU checks RMS
    // has no path to (see decision 7). The sequence ends after compute power-on.
    Ok(())
}

/// Marks a stage running in the timeline AND the job's live `state_description`
/// ("Rack reboot step N: <message>", where N is the DGX GB200 rack-reboot runbook
/// step number(s) this stage implements — what a poller sees while the job runs),
/// logs the start, and returns the start instant for duration logging.
fn begin_stage(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    stage: &str,
    message: &str,
) -> Instant {
    // The worker drives the predeclared stage list strictly in order; a
    // mismatch here means `cold_reboot_stages` drifted from the call sites.
    debug_assert_eq!(timeline.current_name(), Some(stage));
    timeline.start_current();
    job.progress(format!("Rack reboot step {step}: {message}"));
    tracing::info!(job_id = %job.id(), stage, step, "cold reboot stage starting");
    Instant::now()
}

/// Marks a stage completed in the timeline and logs it with its duration.
fn complete_stage(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    stage: &str,
    message: &str,
    started: Instant,
    details: serde_json::Value,
) {
    let _ = message; // The stage description is predeclared in `cold_reboot_stages`.
    timeline.complete_current(details);
    tracing::info!(
        job_id = %job.id(),
        stage,
        step,
        duration_ms = started.elapsed().as_millis() as u64,
        "cold reboot stage completed"
    );
}

/// Marks a stage failed in the timeline (skipping the unreached stages), logs
/// it at error level, and returns the failure message so callers can
/// `return Err(fail_stage(...))`.
fn fail_stage(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    stage: &str,
    message: &str,
    details: serde_json::Value,
) -> String {
    timeline.fail_current(false, message, details);
    timeline.skip_remaining();
    tracing::error!(job_id = %job.id(), stage, step, error = %message, "cold reboot stage failed");
    message.to_owned()
}

/// Marks a stage cancelled in the timeline (skipping the unreached stages),
/// logs it at warn level, and returns the message.
fn cancel_stage(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    stage: &str,
) -> String {
    let msg = format!("cold reboot cancelled during {stage}");
    timeline.fail_current(true, &msg, json!({ "cancelled": true }));
    timeline.skip_remaining();
    tracing::warn!(job_id = %job.id(), stage, step, "cold reboot cancelled");
    msg
}

/// Applies one power operation to every node in `nodes` in parallel. Succeeds
/// only if all nodes succeed; otherwise records the failures and returns `Err`.
#[allow(clippy::too_many_arguments)]
async fn power_stage(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    stage: &str,
    message: &str,
    nodes: &[NamedNode],
    op: PowerOp,
) -> std::result::Result<(), String> {
    if let Some(err) = cancelled_before(job, timeline, step, stage) {
        return Err(err);
    }
    let started = begin_stage(job, timeline, step, stage, message);

    let results = join_all(nodes.iter().map(|n| async move {
        let outcome = n.node.set_power_state(op, PowerTargetType::System).await;
        (n.id.clone(), outcome)
    }))
    .await;

    let failures: Vec<String> = results
        .into_iter()
        .filter_map(|(id, res)| res.err().map(|e| format!("{id}: {}", e.message)))
        .collect();

    if failures.is_empty() {
        complete_stage(
            job,
            timeline,
            step,
            stage,
            message,
            started,
            json!({ "nodes": nodes.len() }),
        );
        Ok(())
    } else {
        let msg = format!("{stage} failed: {}", failures.join("; "));
        Err(fail_stage(
            job,
            timeline,
            step,
            stage,
            &msg,
            json!({ "failures": failures }),
        ))
    }
}

/// A stage gate condition. Kept as an enum (rather than an async closure, which
/// Rust does not stably support) so `gate` can poll a uniform predicate.
enum Probe {
    /// All compute/switch tray BMCs are unreachable (post shelf power-off).
    BmcUnreachable,
    /// All compute/switch tray BMCs are reachable (post shelf power-on).
    BmcReachable,
    /// Every compute BMC reports power state OFF (graceful shutdown complete).
    ComputeDown,
}

/// Minimum-wait floor followed by polling until `probe` holds or `timing.poll_max`
/// elapses. Honors cancellation throughout.
#[allow(clippy::too_many_arguments)]
async fn gate(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    stage: &str,
    message: &str,
    timing: StageTiming,
    poll_interval: Duration,
    probe: Probe,
    targets: &ColdRebootTargets,
) -> std::result::Result<(), String> {
    if let Some(err) = cancelled_before(job, timeline, step, stage) {
        return Err(err);
    }
    let started = begin_stage(job, timeline, step, stage, message);
    let cancel = job.cancellation_token();

    // Unconditional hardware settle / discharge floor.
    tracing::debug!(
        job_id = %job.id(),
        stage,
        step,
        floor_secs = timing.floor.as_secs(),
        "cold reboot stage floor wait"
    );
    sleep_cancellable(timing.floor, &cancel).await;
    if cancel.is_cancelled() {
        return Err(cancel_stage(job, timeline, step, stage));
    }

    let deadline = Instant::now() + timing.poll_max;
    loop {
        if probe_satisfied(&probe, targets).await {
            complete_stage(job, timeline, step, stage, message, started, json!({}));
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Err(cancel_stage(job, timeline, step, stage));
        }
        if Instant::now() >= deadline {
            let msg = format!("{stage} timed out after {}s", timing.poll_max.as_secs());
            return Err(fail_stage(
                job,
                timeline,
                step,
                stage,
                &msg,
                json!({ "timeout_seconds": timing.poll_max.as_secs() }),
            ));
        }
        tracing::debug!(
            job_id = %job.id(),
            stage,
            step,
            "cold reboot stage poll: condition not yet met"
        );
        // Cap the sleep at the remaining budget so a poll_interval larger than
        // what is left cannot push the stage past its declared poll_max; the
        // wake-up at the deadline still gets one final probe before failing.
        let remaining = deadline.saturating_duration_since(Instant::now());
        sleep_cancellable(poll_interval.min(remaining), &cancel).await;
    }
}

/// A fixed, cancellable wait with no probing — used where there is no readiness
/// signal to poll (e.g. waiting for the compute OS to boot, which the BMC cannot
/// report).
async fn wait_stage(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    stage: &str,
    message: &str,
    duration: Duration,
) -> std::result::Result<(), String> {
    if let Some(err) = cancelled_before(job, timeline, step, stage) {
        return Err(err);
    }
    let started = begin_stage(job, timeline, step, stage, message);
    let cancel = job.cancellation_token();
    sleep_cancellable(duration, &cancel).await;
    if cancel.is_cancelled() {
        return Err(cancel_stage(job, timeline, step, stage));
    }
    complete_stage(
        job,
        timeline,
        step,
        stage,
        message,
        started,
        json!({ "waited_seconds": duration.as_secs() }),
    );
    Ok(())
}

/// Steps 7-8: verify every switch tray's NVOS/NVUE is up via an "nvue hello"
/// (`GET /nvue_v1/system`). Tries up to `NVOS_HELLO_ATTEMPTS` times, backing off
/// exponentially from `backoff_base` (doubling each attempt) between tries. No
/// wait follows the final attempt. Honors cancellation throughout.
async fn verify_nvos_ready(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    switches: &[EphemeralSwitch],
    backoff_base: Duration,
) -> std::result::Result<(), String> {
    let stage = "verify_nvos_ready";
    let message = "Waiting for switch NVOS readiness (nvue hello)";
    if let Some(err) = cancelled_before(job, timeline, step, stage) {
        return Err(err);
    }
    let started = begin_stage(job, timeline, step, stage, message);
    let cancel = job.cancellation_token();

    let mut unresponsive: Vec<String> = Vec::new();
    for attempt in 0..NVOS_HELLO_ATTEMPTS {
        unresponsive = nvue_hello_failures(switches).await;
        if unresponsive.is_empty() {
            complete_stage(
                job,
                timeline,
                step,
                stage,
                message,
                started,
                json!({ "attempts": attempt + 1 }),
            );
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Err(cancel_stage(job, timeline, step, stage));
        }
        // Exponential backoff before the next attempt (none after the last).
        if attempt + 1 < NVOS_HELLO_ATTEMPTS {
            let delay = backoff_base * 2u32.pow(attempt);
            tracing::debug!(
                job_id = %job.id(),
                stage,
                step,
                attempt = attempt + 1,
                backoff_secs = delay.as_secs(),
                unresponsive = ?unresponsive,
                "nvue hello not ready on all switches; backing off"
            );
            sleep_cancellable(delay, &cancel).await;
            if cancel.is_cancelled() {
                return Err(cancel_stage(job, timeline, step, stage));
            }
        }
    }

    let msg = format!(
        "switch NVOS not ready: nvue hello failed after {NVOS_HELLO_ATTEMPTS} attempts; \
         unresponsive switches: {}",
        unresponsive.join(", ")
    );
    Err(fail_stage(
        job,
        timeline,
        step,
        stage,
        &msg,
        json!({
            "attempts": NVOS_HELLO_ATTEMPTS,
            "unresponsive_switches": unresponsive,
        }),
    ))
}

/// Probes every switch tray with an "nvue hello" and returns the node IDs of
/// the trays that did not answer (empty = all ready). Switches are probed
/// concurrently: each probe can cost a full connect timeout against a
/// still-booting switch, so a sequential sweep would multiply that by the
/// number of trays.
async fn nvue_hello_failures(switches: &[EphemeralSwitch]) -> Vec<String> {
    let results = join_all(switches.iter().map(|s| s.switch().nvue_hello())).await;
    switches
        .iter()
        .zip(results)
        .filter(|(_, result)| result.is_err())
        .map(|(s, _)| s.switch().id().to_owned())
        .collect()
}

/// Evaluates a gate condition against the current hardware state. All targets
/// are probed concurrently (they are distinct nodes, mirroring `power_stage`):
/// probing an unreachable BMC costs a full connect timeout, so a sequential
/// sweep would make one poll round cost `nodes × timeout`.
async fn probe_satisfied(probe: &Probe, targets: &ColdRebootTargets) -> bool {
    match probe {
        Probe::BmcUnreachable => join_all(targets.bmc_probe_nodes().map(|n| is_reachable(&n.node)))
            .await
            .into_iter()
            .all(|reachable| !reachable),
        Probe::BmcReachable => join_all(targets.bmc_probe_nodes().map(|n| is_reachable(&n.node)))
            .await
            .into_iter()
            .all(|reachable| reachable),
        Probe::ComputeDown => join_all(targets.computes.iter().map(|n| n.node.get_power_state()))
            .await
            .into_iter()
            .all(|state| matches!(state, Ok(PowerState::Off))),
    }
}

/// A BMC is "reachable" iff its power state probe returns a definite On/Off.
/// A transport error OR `Ok(Unknown)` both count as unreachable — this is the
/// uniform predicate required because switch `get_power_state` folds transport
/// failures into `Ok(Unknown)` rather than propagating `Err`.
async fn is_reachable(node: &NodeInstance) -> bool {
    matches!(
        node.get_power_state().await,
        Ok(PowerState::On) | Ok(PowerState::Off)
    )
}

/// Records a "cancelled before {stage}" failure in the timeline (warn) and
/// returns the message when the job has been cancelled, else `None`.
fn cancelled_before(
    job: &RmsJobHandle,
    timeline: &mut StageTimeline,
    step: &str,
    stage: &str,
) -> Option<String> {
    if job.cancellation_token().is_cancelled() {
        let msg = format!("cold reboot cancelled before {stage}");
        timeline.fail_current(true, &msg, json!({ "cancelled_before_start": true }));
        timeline.skip_remaining();
        tracing::warn!(job_id = %job.id(), stage, step, "cold reboot cancelled before stage start");
        Some(msg)
    } else {
        None
    }
}

/// Sleeps for `dur`, waking early if `cancel` fires.
async fn sleep_cancellable(dur: Duration, cancel: &CancellationToken) {
    tokio::select! {
        _ = tokio::time::sleep(dur) => {}
        _ = cancel.cancelled() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::super::server::SwitchTlsRoots;
    use crate::orchestrator::job_tracker::JobTracker;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use crate::transport::ssh::SftpUploadOptions;

    const RACK: &str = "rack-01";

    // Fake targets use 127.0.0.1 so that any power op a spawned worker attempts
    // fails fast with connection-refused rather than hanging on a connect
    // timeout. Admission (materialization) only constructs lazy clients, so it
    // succeeds without a live device.
    const FAKE_IP: &str = "127.0.0.1";

    fn test_service() -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::new()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            switch_tls_roots: SwitchTlsRoots {
                insecure_switch: true,
                ..SwitchTlsRoots::default()
            },
            sftp_upload_options: SftpUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        }
    }

    fn endpoint(ip: &str) -> rm::Endpoint {
        rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: ip.into(),
                // BMC endpoints require a MAC; harmless for host endpoints.
                mac_address: "00:11:22:33:44:55".into(),
                host_name: None,
            }),
            port: 443,
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "admin".into(),
                    password: "password".into(),
                })),
            }),
        }
    }

    fn node_info(node_id: &str, node_type: rm::NodeType) -> rm::NodeInfo {
        rm::NodeInfo {
            node_id: node_id.into(),
            rack_id: RACK.into(),
            r#type: Some(node_type as i32),
            bmc_endpoint: Some(endpoint(FAKE_IP)),
            host_endpoint: None,
            node_descriptor: None,
            additional_host_endpoints: Vec::new(),
        }
    }

    fn compute_node(node_id: &str) -> rm::NodeInfo {
        node_info(node_id, rm::NodeType::ComputeGb200Nvidia)
    }

    fn powershelf_node(node_id: &str) -> rm::NodeInfo {
        node_info(node_id, rm::NodeType::PowershelfGb200Liteon)
    }

    fn switch_node(node_id: &str) -> rm::NodeInfo {
        let mut ni = node_info(node_id, rm::NodeType::SwitchGb200Nvidia);
        // The switch host (NVUE) endpoint must be a non-loopback IP:
        // `validate_switch_target_host` rejects loopback/unspecified/link-local.
        // The BMC endpoint keeps 127.0.0.1 for fast connection-refused failures.
        ni.host_endpoint = Some(endpoint("10.0.0.11"));
        ni
    }

    fn reboot_request(nodes: Vec<rm::NodeInfo>) -> tonic::Request<rm::ExecuteColdRebootRequest> {
        // Tiny timeouts keep any spawned worker from lingering.
        tonic::Request::new(rm::ExecuteColdRebootRequest {
            nodes: Some(rm::NodeSet { nodes }),
            domain: None,
            prestage_shutdown_floor_seconds: Some(1),
            prestage_shutdown_poll_max_seconds: Some(1),
            discharge_floor_seconds: Some(1),
            discharge_poll_max_seconds: Some(1),
            bmc_up_floor_seconds: Some(1),
            bmc_up_poll_max_seconds: Some(1),
            nvos_hello_backoff_seconds: Some(1),
            compute_boot_wait_seconds: Some(1),
            poll_interval_seconds: Some(1),
        })
    }

    fn failure() -> i32 {
        rm::ReturnCode::Failure as i32
    }

    fn success() -> i32 {
        rm::ReturnCode::Success as i32
    }

    #[tokio::test]
    async fn rejects_unknown_node_type() {
        let svc = test_service();
        // A device with an unspecified/unknown type must be rejected, and the
        // whole admission fails fast without reserving anything.
        let mut bad = compute_node("x-01");
        bad.r#type = Some(0);
        let resp = svc
            .handle_execute_cold_reboot(reboot_request(vec![bad]))
            .await
            .unwrap()
            .into_inner();
        let batch = resp.response.unwrap();
        assert_eq!(batch.status, failure());
        assert!(batch.job_id.is_empty());
        assert!(
            batch
                .node_results
                .iter()
                .any(|n| n.node_id == "x-01" && n.status == failure()),
            "expected x-01 to be reported as failed: {:?}",
            batch.node_results
        );
    }

    #[tokio::test]
    async fn rejects_when_a_target_node_is_already_reserved() {
        let svc = test_service();
        // Simulate another tracked op (e.g. a firmware update) holding c-01.
        let _hold = svc
            .job_tracker
            .create_job_if_node_idle(RACK, "c-01", JobType::FirmwareUpdate)
            .expect("reserve c-01");

        // Include a shelf + compute + switch so composition validation passes and
        // the request reaches the reservation step (where the busy c-01 is caught).
        let resp = svc
            .handle_execute_cold_reboot(reboot_request(vec![
                powershelf_node("ps-01"),
                compute_node("c-01"),
                switch_node("sw-01"),
            ]))
            .await
            .unwrap()
            .into_inner();
        let batch = resp.response.unwrap();
        assert_eq!(batch.status, failure());
        assert!(batch.job_id.is_empty());
        assert!(
            batch.message.contains("in progress"),
            "expected a busy message: {}",
            batch.message
        );
    }

    #[tokio::test]
    async fn rejects_when_no_powershelf() {
        let svc = test_service();
        // compute only, no power shelf.
        let resp = svc
            .handle_execute_cold_reboot(reboot_request(vec![compute_node("c-01")]))
            .await
            .unwrap()
            .into_inner();
        let batch = resp.response.unwrap();
        assert_eq!(batch.status, failure());
        assert!(batch.job_id.is_empty());
        assert!(
            batch.message.contains("power shelf"),
            "expected a power-shelf message: {}",
            batch.message
        );
    }

    #[tokio::test]
    async fn rejects_when_no_compute() {
        let svc = test_service();
        // shelf only, no compute.
        let resp = svc
            .handle_execute_cold_reboot(reboot_request(vec![powershelf_node("ps-01")]))
            .await
            .unwrap()
            .into_inner();
        let batch = resp.response.unwrap();
        assert_eq!(batch.status, failure());
        assert!(batch.job_id.is_empty());
        assert!(
            batch.message.contains("compute"),
            "expected a compute message: {}",
            batch.message
        );
    }

    #[tokio::test]
    async fn rejects_when_no_switch() {
        let svc = test_service();
        // shelf + compute only: the NVOS gate would pass vacuously.
        let resp = svc
            .handle_execute_cold_reboot(reboot_request(vec![
                powershelf_node("ps-01"),
                compute_node("c-01"),
            ]))
            .await
            .unwrap()
            .into_inner();
        let batch = resp.response.unwrap();
        assert_eq!(batch.status, failure());
        assert!(batch.job_id.is_empty());
        assert!(
            batch.message.contains("switch"),
            "expected a switch message: {}",
            batch.message
        );
    }

    #[tokio::test]
    async fn rejects_mixed_racks() {
        let svc = test_service();
        // A device in a different rack than the rest → reject before any work.
        let mut ps_other_rack = powershelf_node("ps-99");
        ps_other_rack.rack_id = "rack-02".into();
        let resp = svc
            .handle_execute_cold_reboot(reboot_request(vec![ps_other_rack, compute_node("c-01")]))
            .await
            .unwrap()
            .into_inner();
        let batch = resp.response.unwrap();
        assert_eq!(batch.status, failure());
        assert!(batch.job_id.is_empty());
        assert!(
            batch.message.contains("single rack"),
            "expected a single-rack message: {}",
            batch.message
        );
    }

    #[tokio::test]
    async fn accepts_valid_request_and_returns_job_id() {
        let svc = test_service();
        let nodes = vec![
            powershelf_node("ps-01"),
            compute_node("c-01"),
            switch_node("sw-01"),
        ];
        let resp = svc
            .handle_execute_cold_reboot(reboot_request(nodes))
            .await
            .unwrap()
            .into_inner();
        let batch = resp.response.unwrap();
        assert_eq!(batch.status, success(), "message: {}", batch.message);
        assert!(!batch.job_id.is_empty());
        let stats = batch.stats.unwrap();
        // shelf + compute + switch = 3 devices.
        assert_eq!(stats.total_nodes, 3);
    }
}
