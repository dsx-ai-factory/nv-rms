/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Converts controller responses into fabric members and component status.

use super::membership::resolve_members;
use super::{ComputeEvidence, FabricEvidence, ReadEvidence, SwitchEvidence, record_read_error};
use crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2::observations::ObservedIdentity;
use crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2::observations::{
    FabricSamples, interpret_group, observation_identity,
};
use crate::libnmxc::nmxc_model::nmx_device_topo_info::Device;

use librms::protos::rack_manager as rm;

/// Creates a controller observation with all reads marked as not collected.
pub(super) fn controller_observation(primary_id: &str) -> FabricEvidence {
    FabricEvidence {
        observation: rm::ScaleUpFabricInspectionObservation {
            primary_switch_node_id: primary_id.into(),
            ..Default::default()
        },
        reads: [
            "controller-state",
            "topology",
            "switch-nodes",
            "compute-nodes",
        ]
        .into_iter()
        .map(|resource| {
            (
                resource.into(),
                ReadEvidence {
                    collected: false,
                    ..Default::default()
                },
            )
        })
        .collect(),
    }
}

/// Matches live devices to inventory nodes and records component status and read errors.
/// A reported hardware fault does not make an otherwise successful read fail.
pub(super) fn apply_samples<'a>(
    observed: &mut FabricEvidence,
    samples: &FabricSamples,
    switches: impl Iterator<Item = &'a SwitchEvidence>,
    computes: &[ComputeEvidence],
) {
    let switch_locations: Vec<_> = switches
        .filter_map(|switch| switch.location.as_ref())
        .collect();

    let compute_locations: Vec<_> = computes
        .iter()
        .filter_map(|compute| compute.location.as_ref())
        .collect();

    let placements = |ids: &[String], inventory: &[&ObservedIdentity]| {
        inventory
            .iter()
            .filter(|node| ids.contains(&node.node_id))
            .map(|node| (*node).clone())
            .collect::<Vec<_>>()
    };

    if let Ok(topology) = &samples.topology {
        observed
            .reads
            .entry("topology".into())
            .or_default()
            .collected = true;

        resolve_members(observed, topology, &switch_locations, &compute_locations);

        for device in &topology.device_topo_info {
            let (num_ports, ports) = match device.device.as_ref() {
                Some(Device::SwitchTopoInfo(device)) => (device.num_ports, &device.port_topo_info),
                Some(Device::GpuTopoInfo(device)) => (device.num_ports, &device.port_topo_info),
                None => continue,
            };

            if num_ports > ports.len() as u64 {
                record_read_error(
                    &mut observed.reads,
                    "topology",
                    "device lists fewer ports than its reported port count",
                );
            }
        }
    }

    // Unidentified zero-count rows are placeholders, not discovered members.
    for (resource, missing_identity) in [
        (
            "switch-nodes",
            samples.switch_nodes.as_ref().is_ok_and(|nodes| {
                nodes.node_info_list.iter().any(|node| {
                    node.num_switches > 0 && observation_identity(node.loc.as_ref()).is_none()
                })
            }),
        ),
        (
            "compute-nodes",
            samples.compute_nodes.as_ref().is_ok_and(|nodes| {
                nodes.node_info_list.iter().any(|node| {
                    node.num_gpus > 0 && observation_identity(node.loc.as_ref()).is_none()
                })
            }),
        ),
    ] {
        if missing_identity {
            record_read_error(
                &mut observed.reads,
                resource,
                "node reports devices but is missing a chassis serial number or a valid tray index",
            );
        }
    }

    let expected_switches = placements(&observed.observation.switch_node_ids, &switch_locations);
    let expected_computes = placements(&observed.observation.compute_node_ids, &compute_locations);

    let health = interpret_group(samples, &expected_switches, &expected_computes);

    observed.observation.control_plane_state = health.control_plane_state.into();
    observed.observation.component_status = Some(health.health);

    for (resource, error) in [
        ("controller-state", samples.domain.as_ref().err()),
        ("topology", samples.topology.as_ref().err()),
        ("switch-nodes", samples.switch_nodes.as_ref().err()),
        ("compute-nodes", samples.compute_nodes.as_ref().err()),
    ] {
        observed.reads.entry(resource.into()).or_default().collected = error.is_none();

        if let Some(error) = error {
            record_read_error(&mut observed.reads, resource, error);
        }
    }
}
