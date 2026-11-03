/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Converts NMX-C health and state values to the status fields reported by RMS.

use super::{FabricSamples, ObservedIdentity};
use crate::libnmxc::nmxc_model as nmx;

use librms::protos::rack_manager as rm;

/// Controller state and component status derived from NMX-C responses.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) struct GroupObservation {
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) control_plane_state:
        rm::ScaleUpFabricControlPlaneState,
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) health:
        rm::ScaleUpFabricComponentStatus,
}

/// Interprets controller and data-plane health without performing device I/O.
///
/// Matches node-list entries to live devices by physical identity. Failed reads
/// contribute UNKNOWN status; reported faults take precedence over UNKNOWN.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) fn interpret_group(
    samples: &FabricSamples,
    expected_switches: &[ObservedIdentity],
    expected_computes: &[ObservedIdentity],
) -> GroupObservation {
    use rm::ScaleUpFabricControlPlaneState as ControlPlane;
    use rm::ScaleUpFabricReportedStatus as Health;

    let (control_plane_state, controller) = match &samples.domain {
        Ok(domain) => {
            let state = match nmx::ControlPlaneState::try_from(domain.control_plane_state) {
                Ok(nmx::ControlPlaneState::NmxControlPlaneStateConfigured) => {
                    ControlPlane::Configured
                }
                Ok(
                    nmx::ControlPlaneState::NmxControlPlaneStateTimeout
                    | nmx::ControlPlaneState::NmxControlPlaneStateError,
                ) => ControlPlane::Failed,
                _ => ControlPlane::Unknown,
            };

            let controller = reported_health(
                domain.nmx_controller_health,
                nmx::NmxControllerHealth::Unknown,
                nmx::NmxControllerHealth::Healthy,
            );

            (state, controller)
        }
        Err(_) => (ControlPlane::Unknown, Health::Unknown),
    };

    let FabricSamples {
        topology,
        switch_nodes,
        compute_nodes,
        ..
    } = samples;

    let (compute, switches, links) = data_plane_health(
        topology.as_ref().ok(),
        switch_nodes.as_ref().ok(),
        compute_nodes.as_ref().ok(),
        expected_switches,
        expected_computes,
    );

    // Keep faults reported in node lists even when topology omits the affected node.
    // Healthy node-list entries alone are not enough to report NORMAL status.
    let (node_compute, node_switches, _) =
        health_without_topology(switch_nodes.as_ref().ok(), compute_nodes.as_ref().ok());

    let compute = if node_compute == Health::Abnormal {
        node_compute
    } else {
        compute
    };

    let switches = if node_switches == Health::Abnormal {
        node_switches
    } else {
        switches
    };

    GroupObservation {
        control_plane_state,
        health: rm::ScaleUpFabricComponentStatus {
            controller: controller as i32,
            compute: compute as i32,
            switches: switches as i32,
            links: links as i32,
        },
    }
}

/// Ignores locations with a blank chassis serial or an unset tray index.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) fn observation_identity(
    location: Option<&crate::libnmxc::nmxc_model::NmxLocationInfo>,
) -> Option<(&str, u64)> {
    let location = location?;

    if location.chassis_serial_number.trim().is_empty() || location.tray_index == u64::MAX {
        return None;
    }

    Some((location.chassis_serial_number.as_str(), location.tray_index))
}

/// Maps healthy to NORMAL, other known health states to ABNORMAL, and unknown to UNKNOWN.
fn reported_health<E>(raw: i32, unknown: E, healthy: E) -> rm::ScaleUpFabricReportedStatus
where
    E: TryFrom<i32> + PartialEq,
{
    use rm::ScaleUpFabricReportedStatus as Health;

    match E::try_from(raw) {
        Ok(state) if state == healthy => Health::Normal,
        Ok(state) if state != unknown => Health::Abnormal,
        _ => Health::Unknown,
    }
}

/// ABNORMAL takes precedence over UNKNOWN, then NORMAL. No observations means UNKNOWN.
fn aggregate_health(
    states: impl IntoIterator<Item = rm::ScaleUpFabricReportedStatus>,
) -> rm::ScaleUpFabricReportedStatus {
    use rm::ScaleUpFabricReportedStatus as Health;

    let mut aggregate = Health::Normal;
    let mut observed = false;

    for state in states {
        observed = true;

        aggregate = match (aggregate, state) {
            (Health::Abnormal, _) | (_, Health::Abnormal) => Health::Abnormal,
            (Health::Unknown, _) | (_, Health::Unknown) => Health::Unknown,
            _ => Health::Normal,
        };
    }

    if observed { aggregate } else { Health::Unknown }
}

/// Excludes administratively DOWN and FNM/HCA ports from link status.
fn operational_link_health(
    port: &crate::libnmxc::nmxc_model::NmxPortTopoInfo,
) -> Option<rm::ScaleUpFabricReportedStatus> {
    use rm::ScaleUpFabricReportedStatus as Health;

    let admin_state = nmx::AdminState::try_from(port.port_admin_state);

    if matches!(admin_state, Ok(nmx::AdminState::NmxAdminStateDown)) {
        return None;
    }

    match nmx::PortType::try_from(port.port_type) {
        Ok(nmx::PortType::NmxPortTypeFnm | nmx::PortType::NmxPortTypeHca) => return None,
        Ok(
            nmx::PortType::NmxPortTypeGpu
            | nmx::PortType::NmxPortTypeSwitchAccess
            | nmx::PortType::NmxPortTypeSwitchTrunk,
        ) => {}
        _ => return Some(Health::Unknown),
    }

    match admin_state {
        Ok(nmx::AdminState::NmxAdminStateDiag) => return Some(Health::Abnormal),
        Ok(nmx::AdminState::NmxAdminStateUp) => {}
        _ => return Some(Health::Unknown),
    }

    let physical = match nmx::PhysicalPortState::try_from(port.physical_state) {
        Ok(nmx::PhysicalPortState::NmxPortPhysStateLinkup) => Health::Normal,
        Ok(
            nmx::PhysicalPortState::NmxPortPhysStateSleep
            | nmx::PhysicalPortState::NmxPortPhysStatePolling
            | nmx::PhysicalPortState::NmxPortPhysStateDisabled
            | nmx::PhysicalPortState::NmxPortPhysStatePortconftrain
            | nmx::PhysicalPortState::NmxPortPhysStateLinkerrrecover
            | nmx::PhysicalPortState::NmxPortPhysStatePhytest,
        ) => Health::Abnormal,
        _ => Health::Unknown,
    };

    let logical = match nmx::LogicalLinkState::try_from(port.logical_state) {
        Ok(nmx::LogicalLinkState::NmxLogicalLinkStateActive) => Health::Normal,
        Ok(
            nmx::LogicalLinkState::NmxLogicalLinkStateDown
            | nmx::LogicalLinkState::NmxLogicalLinkStateInit
            | nmx::LogicalLinkState::NmxLogicalLinkStateArmed
            | nmx::LogicalLinkState::NmxLogicalLinkStateActDefer,
        ) => Health::Abnormal,
        _ => Health::Unknown,
    };

    Some(aggregate_health([physical, logical]))
}

/// Keeps reported node faults when topology is unavailable.
/// Node lists alone cannot establish NORMAL component or link status.
/// Positive-count rows retain faults even without a usable physical identity.
fn health_without_topology(
    switch_nodes: Option<&crate::libnmxc::nmxc_model::NmxGetSwitchNodeInfoListResponse>,
    compute_nodes: Option<&crate::libnmxc::nmxc_model::NmxGetComputeNodeInfoListResponse>,
) -> (
    rm::ScaleUpFabricReportedStatus,
    rm::ScaleUpFabricReportedStatus,
    rm::ScaleUpFabricReportedStatus,
) {
    use rm::ScaleUpFabricReportedStatus as Health;

    let switches = switch_nodes
        .into_iter()
        .flat_map(|nodes| &nodes.node_info_list)
        .filter(|node| node.num_switches > 0 || observation_identity(node.loc.as_ref()).is_some())
        .map(|node| {
            reported_health(
                node.node_health,
                nmx::SwitchNodeHealth::NmxSwitchNodeHealthUnknown,
                nmx::SwitchNodeHealth::NmxSwitchNodeHealthHealthy,
            )
        });

    let compute = compute_nodes
        .into_iter()
        .flat_map(|nodes| &nodes.node_info_list)
        .filter(|node| node.num_gpus > 0 || observation_identity(node.loc.as_ref()).is_some())
        .map(|node| {
            reported_health(
                node.node_health,
                nmx::ComputeNodeHealth::NmxComputeNodeHealthUnknown,
                nmx::ComputeNodeHealth::NmxComputeNodeHealthHealthy,
            )
        });

    (
        aggregate_health(compute.chain([Health::Unknown])),
        aggregate_health(switches.chain([Health::Unknown])),
        Health::Unknown,
    )
}

/// Combines device, node-list, and port status. Missing GPU identities contribute UNKNOWN.
fn data_plane_health(
    topology: Option<&crate::libnmxc::nmxc_model::NmxFmTopologyInfo>,
    switch_nodes: Option<&crate::libnmxc::nmxc_model::NmxGetSwitchNodeInfoListResponse>,
    compute_nodes: Option<&crate::libnmxc::nmxc_model::NmxGetComputeNodeInfoListResponse>,
    expected_switches: &[ObservedIdentity],
    expected_computes: &[ObservedIdentity],
) -> (
    rm::ScaleUpFabricReportedStatus,
    rm::ScaleUpFabricReportedStatus,
    rm::ScaleUpFabricReportedStatus,
) {
    use crate::libnmxc::nmxc_model::nmx_device_topo_info::Device;
    use rm::ScaleUpFabricReportedStatus as Health;

    let Some(topology) = topology else {
        return health_without_topology(switch_nodes, compute_nodes);
    };

    let mut compute = Vec::new();
    let mut switches = Vec::new();
    let mut links = Vec::new();
    let mut observed_switches = std::collections::BTreeSet::new();
    let mut observed_computes = std::collections::BTreeSet::new();

    if switch_nodes.is_none() {
        switches.push(Health::Unknown);
    }

    if compute_nodes.is_none() {
        compute.push(Health::Unknown);
    }

    for device in &topology.device_topo_info {
        let (num_ports, ports) = match device.device.as_ref() {
            Some(Device::SwitchTopoInfo(switch)) => {
                switches.push(reported_health(
                    switch.device_health,
                    nmx::SwitchHealth::NmxSwitchHealthUnknown,
                    nmx::SwitchHealth::NmxSwitchHealthHealthy,
                ));

                if let Some(identity) = observation_identity(switch.loc.as_ref()) {
                    observed_switches.insert(identity);
                }

                if let Some(nodes) = switch_nodes {
                    let identity = observation_identity(switch.loc.as_ref());

                    switches.push(aggregate_health(
                        nodes
                            .node_info_list
                            .iter()
                            .filter(|node| {
                                identity.is_some()
                                    && observation_identity(node.loc.as_ref()) == identity
                            })
                            .map(|node| {
                                reported_health(
                                    node.node_health,
                                    nmx::SwitchNodeHealth::NmxSwitchNodeHealthUnknown,
                                    nmx::SwitchNodeHealth::NmxSwitchNodeHealthHealthy,
                                )
                            }),
                    ));
                }

                (switch.num_ports, &switch.port_topo_info)
            }
            Some(Device::GpuTopoInfo(gpu)) => {
                compute.push(reported_health(
                    gpu.device_health,
                    nmx::GpuHealth::NmxGpuHealthUnknown,
                    nmx::GpuHealth::NmxGpuHealthHealthy,
                ));

                if let Some(identity) = observation_identity(gpu.loc.as_ref()) {
                    observed_computes.insert(identity);
                }

                if let Some(nodes) = compute_nodes {
                    let identity = observation_identity(gpu.loc.as_ref());

                    compute.push(aggregate_health(
                        nodes
                            .node_info_list
                            .iter()
                            .filter(|node| {
                                identity.is_some()
                                    && observation_identity(node.loc.as_ref()) == identity
                            })
                            .map(|node| {
                                reported_health(
                                    node.node_health,
                                    nmx::ComputeNodeHealth::NmxComputeNodeHealthUnknown,
                                    nmx::ComputeNodeHealth::NmxComputeNodeHealthHealthy,
                                )
                            }),
                    ));
                }

                (gpu.num_ports, &gpu.port_topo_info)
            }
            None => {
                compute.push(Health::Unknown);
                switches.push(Health::Unknown);
                links.push(Health::Unknown);
                continue;
            }
        };

        // Missing records cannot establish whether the absent ports are excluded.
        if num_ports > ports.len() as u64 {
            links.push(Health::Unknown);
        }

        links.extend(ports.iter().filter_map(operational_link_health));
    }

    if expected_switches.iter().any(|node| {
        !observed_switches.contains(&(
            node.chassis_serial_number.as_str(),
            u64::from(node.tray_index),
        ))
    }) {
        switches.push(Health::Unknown);
    }

    if observed_computes.is_empty()
        || expected_computes.iter().any(|node| {
            !observed_computes.contains(&(
                node.chassis_serial_number.as_str(),
                u64::from(node.tray_index),
            ))
        })
    {
        compute.push(Health::Unknown);
    }

    (
        aggregate_health(compute),
        aggregate_health(switches),
        aggregate_health(links),
    )
}

#[cfg(test)]
mod tests;
