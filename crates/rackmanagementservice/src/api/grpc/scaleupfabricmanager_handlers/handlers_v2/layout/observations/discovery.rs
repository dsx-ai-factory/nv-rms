/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Discovers physical switch and compute placement before planning.

use super::{ManagedSwitch, ObservedPlacement, run_parallel};
use crate::api::grpc::node_type_resolver::resolve_node_info;
use crate::api::grpc::server::RackManagerServiceImpl;
use crate::config::fabric_layout::FabricPosition;
use crate::domain::node::Node;
use crate::utilities::error::RmsError;

use librms::protos::rack_manager as rm;
use nvue_client::DEFAULT_TIMEOUT;

use serde::Deserialize;

// Coordinates must be present, nonnegative, and representable as u32.
#[derive(Deserialize)]
struct ComputePlacementReadback {
    chassis_sn: String,
    tray_index: u32,
    slot_number: u32,
}

impl RackManagerServiceImpl {
    /// Reads switch identity and placement concurrently, preserving inventory order.
    /// Missing physical identity or a managed-link adapter rejects discovery.
    pub(in super::super) async fn discover_layout_switches(
        &self,
        nodes: &[rm::NodeInfo],
        domain: Option<&str>,
    ) -> Result<Vec<ManagedSwitch>, String> {
        let reads = nodes.iter().map(|node| async move {
            let prepared = self
                .build_ephemeral_switch(node, domain)
                .await
                .map_err(|error| {
                    format!(
                        "cannot initialize switch '{}' for placement discovery: {}",
                        node.node_id, error.code
                    )
                })?;

            let client = prepared
                .switch
                .nvue_client()
                .ok_or_else(|| format!("switch '{}' has no NVUE discovery client", node.node_id))?;

            let location = client
                .get_chassis_location(DEFAULT_TIMEOUT)
                .await
                .map_err(|error| {
                    format!(
                        "cannot read switch '{}' placement: {}",
                        node.node_id,
                        RmsError::from(error).code
                    )
                })?;

            let chassis_serial_number = location
                .chassis_sn
                .filter(|serial| !serial.trim().is_empty())
                .ok_or_else(|| format!("switch '{}' reports no chassis identity", node.node_id))?;

            let tray_index = location
                .tray_index
                .as_deref()
                .and_then(|value| value.trim().parse().ok())
                .ok_or_else(|| format!("switch '{}' reports no valid tray index", node.node_id))?;

            let slot_number = location
                .slot_number
                .as_deref()
                .and_then(|value| value.trim().parse().ok())
                .ok_or_else(|| format!("switch '{}' reports no valid slot number", node.node_id))?;

            let node_type = resolve_node_info(node).map_err(|error| error.to_string())?;

            // Reject unsupported link adapters before admitting a managed job.
            if prepared.switch.as_switch_gb200().is_none() {
                return Err(format!(
                    "switch '{}' has no managed link adapter",
                    node.node_id
                ));
            }

            Ok(ManagedSwitch {
                node: node.clone(),
                prepared,
                placement: ObservedPlacement {
                    node_id: node.node_id.clone(),
                    node_type: node_type.as_str().to_owned(),
                    chassis_serial_number,
                    position: FabricPosition {
                        tray_index,
                        slot_number,
                    },
                    physical_topology: location.topology_id,
                },
            })
        });

        run_parallel(reads, None, "switch discovery").await
    }

    /// Reads compute placement through BMC adapters without compute-host changes.
    /// Caller node names correlate results but do not supply physical coordinates.
    pub(in super::super) async fn discover_layout_computes(
        &self,
        nodes: &[rm::NodeInfo],
    ) -> Result<Vec<ObservedPlacement>, String> {
        let reads = nodes.iter().map(|info| async move {
            let node = self
                .build_batch_ephemeral_node(info)
                .map_err(|_| {
                    format!(
                        "cannot initialize compute '{}' for BMC discovery",
                        info.node_id
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "compute '{}' has no placement discovery adapter",
                        info.node_id
                    )
                })?;

            let payload = node
                .get_device_info()
                .await
                .map_err(|error| {
                    format!(
                        "cannot read compute '{}' placement from BMC: {}",
                        info.node_id, error.code
                    )
                })?
                .ok_or_else(|| format!("compute '{}' reports no placement", info.node_id))?;

            let placement: ComputePlacementReadback =
                serde_json::from_value(payload).map_err(|_| {
                    format!(
                        "compute '{}' reports invalid physical placement",
                        info.node_id
                    )
                })?;

            if placement.chassis_sn.trim().is_empty() {
                return Err(format!(
                    "compute '{}' reports no chassis identity",
                    info.node_id
                ));
            }

            Ok(ObservedPlacement {
                node_id: info.node_id.clone(),
                node_type: node.node_type().as_str().to_owned(),
                chassis_serial_number: placement.chassis_sn,
                position: FabricPosition {
                    tray_index: placement.tray_index,
                    slot_number: placement.slot_number,
                },
                physical_topology: None,
            })
        });

        run_parallel(reads, None, "compute discovery").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_placement_requires_all_physical_identity_fields() {
        for field in ["chassis_sn", "tray_index", "slot_number"] {
            let mut payload = serde_json::json!({
                "chassis_sn": "observed-chassis", "tray_index": 2, "slot_number": 7,
            });

            payload.as_object_mut().unwrap().remove(field);

            let result = serde_json::from_value::<ComputePlacementReadback>(payload);

            assert!(result.is_err(), "missing {field} must fail deserialization");
        }
    }
}
