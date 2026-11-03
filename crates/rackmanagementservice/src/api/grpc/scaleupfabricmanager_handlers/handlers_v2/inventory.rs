/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared identity validation for caller-supplied fabric inventories.

use std::collections::HashSet;

use crate::api::grpc::node_type_resolver::resolve_node_info;
use crate::domain::node::NodeType;

use librms::protos::rack_manager as rm;

/// Validates every node ID and resolves its type without reading device endpoints.
///
/// Types follow the input order. Callers choose which roles they accept only
/// after validation, so filtering cannot hide duplicate IDs or unknown types.
pub(super) fn validate(nodes: &[rm::NodeInfo]) -> Result<Vec<NodeType>, tonic::Status> {
    let mut ids = HashSet::with_capacity(nodes.len());

    nodes
        .iter()
        .map(|node| {
            if node.node_id.trim().is_empty() || !ids.insert(node.node_id.as_str()) {
                return Err(tonic::Status::invalid_argument(
                    "inventory node IDs must be nonempty and unique",
                ));
            }

            resolve_node_info(node)
                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_typed_and_descriptor_nodes_in_input_order() {
        let nodes = vec![
            rm::NodeInfo {
                node_id: "switch-a".into(),
                r#type: Some(rm::NodeType::SwitchGb200Nvidia.into()),
                ..Default::default()
            },
            rm::NodeInfo {
                node_id: "compute-a".into(),
                node_descriptor: Some(rm::NodeDescriptor {
                    attributes: [
                        ("role".into(), "compute".into()),
                        ("vendor".into(), "nvidia".into()),
                        ("product_family".into(), "gb200".into()),
                    ]
                    .into(),
                }),
                ..Default::default()
            },
        ];

        let types = validate(&nodes).unwrap();

        assert_eq!(
            types,
            vec![NodeType::SwitchGb200Nvidia, NodeType::ComputeGb200Nvidia]
        );
    }

    #[test]
    fn rejects_invalid_ids_and_unresolved_types_before_filtering() {
        for (id, node_type, expected) in [
            (
                "",
                Some(rm::NodeType::ComputeGb200Nvidia.into()),
                "nonempty",
            ),
            (
                " ",
                Some(rm::NodeType::ComputeGb200Nvidia.into()),
                "nonempty",
            ),
            (
                "switch-a",
                Some(rm::NodeType::ComputeGb200Nvidia.into()),
                "unique",
            ),
            ("compute-a", Some(i32::MAX), "unknown node type"),
            ("compute-a", None, "descriptor is required"),
        ] {
            let nodes = vec![
                rm::NodeInfo {
                    node_id: "switch-a".into(),
                    r#type: Some(rm::NodeType::SwitchGb200Nvidia.into()),
                    ..Default::default()
                },
                rm::NodeInfo {
                    node_id: id.into(),
                    r#type: node_type,
                    ..Default::default()
                },
            ];

            let error = validate(&nodes).unwrap_err();

            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert!(error.message().contains(expected), "{error}");
        }
    }
}
