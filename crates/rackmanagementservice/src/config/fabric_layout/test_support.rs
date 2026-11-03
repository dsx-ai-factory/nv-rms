/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Synthetic catalog inputs shared by validation and planning tests.

use super::{FabricCatalogError, FabricLayoutCatalog};

/// Provides two physical switch and compute trays with single-tray grouping.
/// Tests override individual fields rather than duplicating the complete schema.
pub(crate) fn document() -> serde_json::Value {
    serde_json::json!({"profiles": [{
        "switch_node_types": ["switch_gb200_nvidia"],
        "compute_node_types": ["compute_gb200_nvidia"],
        "physical_topology": "synthetic physical topology",
        "compute_tray_count": 2, "switch_tray_count": 2,
        "full_link_states": {"acp1": "up", "fnm1": "down"},
        "fabric_rules": [{
            "compute_tray_count": 1, "switch_tray_count": 1,
            "grouping": {
                "compute": {"node": "compute", "coordinate": "tray_index", "divisor": 1, "multiplier": 1},
                "switch": {"node": "switch", "coordinate": "tray_index", "divisor": 1, "multiplier": 1}
            }
        }, {
            "compute_tray_count": 2, "switch_tray_count": 2,
        }],
        "link_calculation": {
            "default_state": "down",
            "rules": [{"prefix": "acp", "base": 1, "terms": [],
                "offsets": [0], "width": 1, "state": "up"}]
        }
    }]})
}

/// Serializes mutated fixture input through the catalog's JSON decoding boundary.
pub(crate) fn parse_document(
    document: &serde_json::Value,
) -> Result<FabricLayoutCatalog, FabricCatalogError> {
    FabricLayoutCatalog::parse(&serde_json::to_vec(document).unwrap())
}
