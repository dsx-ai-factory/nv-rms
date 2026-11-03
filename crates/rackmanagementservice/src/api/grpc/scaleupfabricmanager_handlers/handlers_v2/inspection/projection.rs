/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Builds the inspection response from collected status and read errors.

use std::collections::{BTreeSet, HashMap};

use super::{ComputeEvidence, FabricEvidence, ReadEvidence, SwitchEvidence, record_read_error};

use librms::protos::rack_manager as rm;

/// Attaches errors to the fabric containing the affected node or controller.
/// Errors for nodes with no identified fabric go in discovery_errors.
pub(super) fn project_observation(
    observation: &mut rm::InspectScaleUpFabricsResponse,
    mut fabrics: Vec<FabricEvidence>,
    switches: Vec<SwitchEvidence>,
    computes: Vec<ComputeEvidence>,
) {
    flag_ambiguous_discovery(&mut fabrics, &switches);

    observation.fabrics = fabrics
        .into_iter()
        .map(|fabric| {
            let mut observed = fabric.observation;

            let primary = observed.primary_switch_node_id.as_str();

            let own_reads = fabric
                .reads
                .iter()
                .map(|(resource, read)| (primary, resource.as_str(), read));

            let switch_reads = switches
                .iter()
                .filter(|switch| {
                    switch.node_id == primary || observed.switch_node_ids.contains(&switch.node_id)
                })
                .flat_map(|switch| {
                    switch
                        .reads
                        .iter()
                        .map(|(resource, read)| (switch.node_id.as_str(), resource.as_str(), read))
                });

            let compute_reads = computes
                .iter()
                .filter(|compute| observed.compute_node_ids.contains(&compute.node_id))
                .flat_map(|compute| {
                    compute
                        .reads
                        .iter()
                        .map(|(resource, read)| (compute.node_id.as_str(), resource.as_str(), read))
                });

            let reads: Vec<_> = own_reads.chain(switch_reads).chain(compute_reads).collect();

            let complete = reads
                .iter()
                .all(|(_, _, read)| read.collected && read.errors.is_empty());

            observed.inspection_errors = read_errors(reads.into_iter());
            observed.status = fabric_status(&observed, complete).into();

            observed
        })
        .collect();

    let associated: BTreeSet<_> = observation
        .fabrics
        .iter()
        .flat_map(|fabric| {
            fabric
                .switch_node_ids
                .iter()
                .chain(&fabric.compute_node_ids)
                .chain(std::iter::once(&fabric.primary_switch_node_id))
        })
        .collect();

    observation.discovery_errors = read_errors(
        switches
            .iter()
            .map(|node| (&node.node_id, &node.reads))
            .chain(computes.iter().map(|node| (&node.node_id, &node.reads)))
            .filter(|(id, _)| !associated.contains(id))
            .flat_map(|(id, reads)| {
                reads
                    .iter()
                    .map(move |(resource, read)| (id.as_str(), resource.as_str(), read))
            }),
    );
}

/// Returns ABNORMAL for a reported fault even when some reads are missing.
/// NORMAL requires a configured controller, normal components, and no collection errors.
fn fabric_status(
    observed: &rm::ScaleUpFabricInspectionObservation,
    complete: bool,
) -> rm::ScaleUpFabricReportedStatus {
    use rm::ScaleUpFabricControlPlaneState as ControlPlane;
    use rm::ScaleUpFabricReportedStatus as Health;

    let components = observed.component_status.as_ref().map(|health| {
        [
            health.controller,
            health.compute,
            health.switches,
            health.links,
        ]
    });

    if observed.control_plane_state == ControlPlane::Failed as i32
        || components.is_some_and(|states| states.contains(&(Health::Abnormal as i32)))
    {
        return Health::Abnormal;
    }

    if complete
        && !observed.switch_node_ids.is_empty()
        && observed.control_plane_state == ControlPlane::Configured as i32
        && components
            .is_some_and(|states| states.iter().all(|state| *state == Health::Normal as i32))
    {
        Health::Normal
    } else {
        Health::Unknown
    }
}

/// Reports shared node IDs as errors on each affected fabric, keeping both results.
/// An unreadable switch cluster state also leaves discovery incomplete because
/// that switch could have another enabled controller.
fn flag_ambiguous_discovery(fabrics: &mut [FabricEvidence], switches: &[SwitchEvidence]) {
    let mut owners = HashMap::<&str, usize>::new();

    for fabric in fabrics.iter() {
        for id in fabric
            .observation
            .switch_node_ids
            .iter()
            .chain(&fabric.observation.compute_node_ids)
        {
            *owners.entry(id).or_default() += 1;
        }
    }

    let shared: BTreeSet<String> = owners
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(id, _)| id.to_owned())
        .collect();

    for fabric in fabrics {
        let observed = &fabric.observation;

        if observed
            .switch_node_ids
            .iter()
            .chain(&observed.compute_node_ids)
            .any(|id| shared.contains(id))
        {
            record_read_error(
                &mut fabric.reads,
                "fabric-discovery",
                "live membership overlaps another enabled controller",
            );
        }

        if switches.iter().any(|switch| switch.enabled.is_none()) {
            record_read_error(
                &mut fabric.reads,
                "fabric-discovery",
                "inventory cluster state is incomplete; additional controllers may be present",
            );
        }
    }
}

/// Sorts and deduplicates errors by node ID and resource-prefixed message.
/// Includes unread resources even when no explicit error was recorded.
fn read_errors<'a>(
    reads: impl Iterator<Item = (&'a str, &'a str, &'a ReadEvidence)>,
) -> Vec<rm::ScaleUpFabricInspectionError> {
    let mut errors = BTreeSet::new();

    for (source, resource, read) in reads {
        if !read.collected && read.errors.is_empty() {
            errors.insert((
                source.to_owned(),
                format!("{resource}: resource was not collected"),
            ));
        }

        for error in &read.errors {
            errors.insert((source.to_owned(), format!("{resource}: {error}")));
        }
    }

    errors
        .into_iter()
        .map(|(node_id, message)| rm::ScaleUpFabricInspectionError { node_id, message })
        .collect()
}
