// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Request-local profile policy for the sanitized manifest planner.
//! Artifact catalogs remain platform-neutral; targets are resolved only at apply.

use super::{
    FirmwareTargetMapping, NodeType, get_firmware_component_mappings_for_node_type,
    get_firmware_flash_order,
};
use crate::config::hardware_profiles::{
    FirmwareTargetPlan, HardwareUpdatePlan, RequestTargetSelection,
};
use crate::nodes::compute_gb200_nvidia::flint_device_family;

/// Fields used to select a manifest component, plus its full profile target.
///
/// `mapping` identifies the source component and update destination. Keeping
/// `configured` lets the planner apply filename rules and construct update
/// requests without looking the profile target up again. It is `None` for
/// code-default mappings and in-band NIC updates. `order` sets the update sequence.
#[derive(Clone, Copy)]
pub(super) struct SelectedMapping<'a> {
    pub mapping: FirmwareTargetMapping<'a>,
    pub configured: Option<&'a FirmwareTargetPlan>,
    pub order: usize,
}

pub(super) fn mappings(
    node_type: NodeType,
    profile: Option<&HardwareUpdatePlan>,
) -> Vec<SelectedMapping<'_>> {
    let Some(profile) = profile else {
        let order = get_firmware_flash_order(node_type);
        return get_firmware_component_mappings_for_node_type(node_type)
            .into_iter()
            .map(|mapping| {
                let selector = if node_type.kind() == crate::domain::node::NodeKind::Powershelf {
                    mapping.target_key
                } else {
                    mapping.redfish_target
                };
                SelectedMapping {
                    order: order
                        .iter()
                        .position(|target| *target == selector)
                        .unwrap_or(usize::MAX),
                    mapping,
                    configured: None,
                }
            })
            .collect();
    };
    let mut mappings = Vec::new();
    for (order, target) in profile
        .stages()
        .iter()
        .flat_map(|stage| stage.targets())
        .enumerate()
    {
        for source in target.source_components() {
            let paths = target.request_targets();
            let single_upload_selector = match target.request_target_selection() {
                RequestTargetSelection::DriverDefault => Some(""),
                RequestTargetSelection::Multipart(paths) => Some(match paths.as_slice() {
                    [] => "",
                    [path] => path.as_str(),
                    _ => target.component_key(),
                }),
                RequestTargetSelection::Explicit(_) => None,
            };
            if let Some(selector) = single_upload_selector {
                mappings.push(SelectedMapping {
                    mapping: FirmwareTargetMapping {
                        source_component: source,
                        target_key: target.component_key(),
                        redfish_target: selector,
                        default_apply: target.default_apply(),
                    },
                    configured: Some(target),
                    order: order + get_firmware_flash_order(node_type).len(),
                });
            } else {
                for path in paths {
                    mappings.push(SelectedMapping {
                        mapping: FirmwareTargetMapping {
                            source_component: source,
                            target_key: target.component_key(),
                            redfish_target: path.as_str(),
                            default_apply: target.default_apply(),
                        },
                        configured: Some(target),
                        order: order + get_firmware_flash_order(node_type).len(),
                    });
                }
            }
        }
    }
    // The v1 schema configures Redfish updates. Preserve main's opt-in in-band
    // NIC and LinkX updates and their driver-owned activation boundaries.
    mappings.extend(
        get_firmware_component_mappings_for_node_type(node_type)
            .into_iter()
            .filter(|mapping| {
                flint_device_family(mapping.redfish_target).is_some()
                    || mapping.target_key == "LINKX"
            })
            .map(|mapping| SelectedMapping {
                mapping,
                configured: None,
                order: if mapping.target_key == "LINKX" {
                    usize::MAX
                } else {
                    get_firmware_flash_order(node_type)
                        .iter()
                        .position(|target| *target == mapping.redfish_target)
                        .unwrap_or(usize::MAX)
                },
            }),
    );
    mappings
}
