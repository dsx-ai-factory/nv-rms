// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Require one configured target per request component key.

use std::collections::{BTreeMap, BTreeSet};

use super::{FirmwareTargetPlan, FirmwareUpdateStage, HardwareProfileError, validation_error};

/// Require unique component keys across all stages, including optional targets.
pub(super) fn validate_component_key_uniqueness(
    profile_id: &str,
    stages: &[FirmwareUpdateStage],
) -> Result<(), HardwareProfileError> {
    let mut components = BTreeMap::<String, Vec<&FirmwareTargetPlan>>::new();
    for target in stages.iter().flat_map(|stage| stage.targets()) {
        components
            .entry(target.component_key().to_ascii_lowercase())
            .or_default()
            .push(target);
    }

    let mut conflicts = Vec::new();
    for (key, targets) in components {
        if targets.len() > 1 {
            let ids = targets
                .iter()
                .map(|target| target.id())
                .collect::<BTreeSet<_>>();
            // Manifest planning groups targets and default selection by component key,
            // so even disjoint filenames cannot make duplicate keys safe.
            conflicts.push(format!(
                "component_key {key:?} is assigned to multiple targets [{}]; component keys must be unique",
                ids.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    if conflicts.is_empty() {
        Ok(())
    } else {
        validation_error(format!(
            "profile {profile_id:?} has conflicting firmware target selectors: {}",
            conflicts.join("; ")
        ))
    }
}

#[cfg(test)]
mod tests;
