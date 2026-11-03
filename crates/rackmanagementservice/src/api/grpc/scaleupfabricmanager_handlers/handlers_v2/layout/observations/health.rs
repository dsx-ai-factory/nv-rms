/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Interprets controller readiness, membership, and data-plane health.

use super::super::super::observations::{self as shared, observation_identity};
use super::{FabricSamples, ObservedIdentity, ObservedPlacement};

use librms::protos::rack_manager as rm;

/// Independent controller readiness, data-plane health, and live membership.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) struct GroupObservation {
    /// Controller configuration state, independent of data-plane health.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) control_plane_state:
        rm::ScaleUpFabricControlPlaneState,

    /// Whether controller state and health were read successfully.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) controller_observed: bool,

    /// Read and membership errors retained independently of reported hardware health.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) errors: Vec<String>,

    /// Component health; failed reads produce unknown health.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) health:
        rm::ScaleUpFabricComponentStatus,

    /// Whether live topology was available for the membership comparison.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) membership_observed: bool,

    /// Whether switches match exactly and observed GPUs belong to supplied computes.
    /// Without compute inventory, GPU membership is not checked.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) membership_matches: bool,
}

/// Adds workflow read evidence and expected-membership checks to shared component status.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) fn interpret_group(
    samples: &FabricSamples,
    expected_switches: &[ObservedPlacement],
    expected_computes: &[ObservedPlacement],
) -> GroupObservation {
    let switches: Vec<_> = expected_switches
        .iter()
        .map(ObservedIdentity::from)
        .collect();

    let computes: Vec<_> = expected_computes
        .iter()
        .map(ObservedIdentity::from)
        .collect();

    let status = shared::interpret_group(samples, &switches, &computes);

    let mut errors: Vec<_> = [
        samples.domain.as_ref().err(),
        samples.topology.as_ref().err(),
        samples.switch_nodes.as_ref().err(),
        samples.compute_nodes.as_ref().err(),
    ]
    .into_iter()
    .flatten()
    .cloned()
    .collect();

    let membership_matches = samples.topology.as_ref().is_ok_and(|topology| {
        live_membership_matches(topology, expected_switches, expected_computes)
    });

    if samples.topology.is_ok() && !membership_matches {
        errors.push(
            "live topology membership does not match the resolved physical identities".into(),
        );
    }

    GroupObservation {
        control_plane_state: status.control_plane_state,
        health: status.health,
        controller_observed: samples.domain.is_ok(),
        errors,
        membership_observed: samples.topology.is_ok(),
        membership_matches,
    }
}

impl From<&ObservedPlacement> for ObservedIdentity {
    /// Retains physical identity for shared status interpretation; slot does not identify an ASIC.
    fn from(placement: &ObservedPlacement) -> Self {
        Self {
            node_id: placement.node_id.clone(),
            chassis_serial_number: placement.chassis_serial_number.clone(),
            tray_index: placement.position.tray_index,
        }
    }
}

/// Requires exact switch membership from live topology, not static node-list rows.
///
/// When compute inventory is supplied, each observed GPU must belong to that
/// inventory. Missing GPUs are allowed here so switch control planes can converge
/// before GPU software is installed; data-plane health still reports missing GPUs.
fn live_membership_matches(
    topology: &crate::libnmxc::nmxc_model::NmxFmTopologyInfo,
    expected_switches: &[ObservedPlacement],
    expected_computes: &[ObservedPlacement],
) -> bool {
    use crate::libnmxc::nmxc_model::nmx_device_topo_info::Device;

    let switches: std::collections::BTreeSet<_> = expected_switches
        .iter()
        .map(|node| {
            (
                node.chassis_serial_number.as_str(),
                u64::from(node.position.tray_index),
            )
        })
        .collect();

    let computes: std::collections::BTreeSet<_> = expected_computes
        .iter()
        .map(|node| {
            (
                node.chassis_serial_number.as_str(),
                u64::from(node.position.tray_index),
            )
        })
        .collect();

    let mut observed_switches = std::collections::BTreeSet::new();

    for device in &topology.device_topo_info {
        match device.device.as_ref() {
            Some(Device::SwitchTopoInfo(switch)) => {
                let Some(identity) = observation_identity(switch.loc.as_ref()) else {
                    return false;
                };

                if !switches.contains(&identity) {
                    return false;
                }

                observed_switches.insert(identity);
            }
            Some(Device::GpuTopoInfo(gpu)) if !computes.is_empty() => {
                let Some(identity) = observation_identity(gpu.loc.as_ref()) else {
                    return false;
                };

                if !computes.contains(&identity) {
                    return false;
                }
            }
            Some(Device::GpuTopoInfo(_)) => {}
            None => return false,
        }
    }

    switches == observed_switches
}

#[cfg(test)]
mod group_observation_tests;
