/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Matches devices in live topology to caller node IDs by chassis and tray.

use std::collections::BTreeSet;

use super::{FabricEvidence, ObservedIdentity, record_read_error};
use crate::libnmxc::nmxc_model as nmx;

/// Uses devices in GetTopologyInfo to identify members, not entries in the node lists.
/// Multiple GPUs in one compute tray produce one node ID. Missing or ambiguous
/// identities add errors without discarding members that were matched successfully.
pub(super) fn resolve_members(
    observed: &mut FabricEvidence,
    topology: &nmx::NmxFmTopologyInfo,
    switches: &[&ObservedIdentity],
    computes: &[&ObservedIdentity],
) {
    use nmx::nmx_device_topo_info::Device;

    let mut switch_ids = BTreeSet::new();
    let mut compute_ids = BTreeSet::new();

    for device in &topology.device_topo_info {
        let (location, inventory, ids) = match device.device.as_ref() {
            Some(Device::SwitchTopoInfo(device)) => {
                (device.loc.as_ref(), switches, &mut switch_ids)
            }
            Some(Device::GpuTopoInfo(device)) => (device.loc.as_ref(), computes, &mut compute_ids),
            None => {
                membership_error(observed, "live topology contains an unidentified device");
                continue;
            }
        };

        let Some(location) = location.filter(|location| {
            !location.chassis_serial_number.trim().is_empty() && location.tray_index != u64::MAX
        }) else {
            membership_error(observed, "live device has no usable physical identity");
            continue;
        };

        let mut matches = inventory.iter().filter(|node| {
            node.chassis_serial_number == location.chassis_serial_number
                && u64::from(node.tray_index) == location.tray_index
        });

        match (matches.next(), matches.next()) {
            (Some(node), None) => {
                ids.insert(node.node_id.clone());
            }
            _ => membership_error(
                observed,
                "live device physical identity does not resolve to exactly one inventory node",
            ),
        }
    }

    if !switch_ids.contains(&observed.observation.primary_switch_node_id) {
        membership_error(
            observed,
            "controller switch is absent from resolved live topology",
        );
    }

    observed.observation.switch_node_ids = switch_ids.into_iter().collect();
    observed.observation.compute_node_ids = compute_ids.into_iter().collect();
}

/// Records a matching error without marking the topology read as failed.
fn membership_error(observed: &mut FabricEvidence, message: &str) {
    record_read_error(&mut observed.reads, "topology", message);
}
