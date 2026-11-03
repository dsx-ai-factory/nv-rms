/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Builds layout-owned controller targets and activation digests.

use std::collections::BTreeMap;

use super::planner::PlannedFabric;
use crate::libnmxc::managed_config::ManagedConfig;
use crate::libnmxc::{NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY};

use sha2::{Digest, Sha256};

impl TryFrom<&PlannedFabric> for ManagedConfig {
    type Error = String;

    /// Validates the resolved topology, set values, and removals before controller I/O.
    fn try_from(fabric: &PlannedFabric) -> Result<Self, Self::Error> {
        Self::try_from(desired_config(fabric))
    }
}

/// Combines requested values, topology, and deployment-owned omission targets.
/// Unrelated stored settings are excluded from the desired target and digest.
fn desired_config(fabric: &PlannedFabric) -> BTreeMap<(String, String), Option<String>> {
    let mut desired: BTreeMap<_, _> = fabric
        .config
        .extra_static_configs
        .iter()
        .map(|entry| {
            (
                (entry.config_file_name.clone(), entry.key.clone()),
                Some(entry.value.clone()),
            )
        })
        .collect();

    desired.insert(
        (
            NMX_C_FM_CONFIG_FILE.to_owned(),
            NMX_C_TOPOLOGY_KEY.to_owned(),
        ),
        Some(fabric.config.topology_type.clone()),
    );

    desired.extend(
        fabric
            .absent_static_configs
            .iter()
            .map(|(file, key)| ((file.clone(), key.clone()), None)),
    );

    desired
}

/// Config proof excludes caller labels and credentials; every managed value participates.
pub(super) fn config_digest(config: &PlannedFabric) -> Result<String, String> {
    let desired = desired_config(config);
    let entries: Vec<_> = desired.iter().collect();

    let bytes = serde_json::to_vec(&entries)
        .map_err(|_| "cannot encode managed controller configuration".to_owned())?;

    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests;
