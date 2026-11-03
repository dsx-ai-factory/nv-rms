/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reconciles explicit administrative link targets and verifies persisted readback.

use std::collections::BTreeMap;

use super::observations;
use crate::config::fabric_layout::FabricLinkState;
use crate::nodes::SwitchScaleUpManagement;
use crate::nodes::switch_gb200_nvidia::ScaleUpLinkChange;

use nvue_client::interface::InterfaceLinkState;

/// Reconciles administrative UP/DOWN states for access (ACP) and fabric-management (FNM) links.
///
/// Skips writes when effective, applied, and startup state all match `target`.
/// Otherwise, applies and saves the requested states, then verifies all three
/// views by readback. Returns whether a write was needed.
///
/// Operational link health does not affect the administrative target; links may
/// remain down without attached endpoints.
/// The caller retains device ownership throughout the read, write, and readback.
/// Saving to startup also persists unrelated applied configuration.
pub(super) async fn reconcile_links(
    switch: &dyn SwitchScaleUpManagement,
    target: &BTreeMap<String, FabricLinkState>,
) -> Result<bool, String> {
    let snapshot = switch
        .get_scale_up_link_snapshot()
        .await
        .map_err(|_| format!("cannot read saved links on switch '{}'", switch.id()))?;

    if observations::links_match(&snapshot, target)? {
        return Ok(false);
    }

    let changes: Vec<_> = target
        .iter()
        .map(|(id, state)| ScaleUpLinkChange {
            interface_id: id.clone(),
            state: match state {
                FabricLinkState::Up => InterfaceLinkState::Up,
                FabricLinkState::Down => InterfaceLinkState::Down,
            },
        })
        .collect();

    switch
        .set_scale_up_link_config(&changes)
        .await
        .map_err(|_| format!("cannot reconcile saved links on switch '{}'", switch.id()))?;

    let snapshot = switch
        .get_scale_up_link_snapshot()
        .await
        .map_err(|_| format!("cannot verify saved links on switch '{}'", switch.id()))?;

    if !observations::links_match(&snapshot, target)? {
        return Err(format!(
            "saved-link write did not pass readback on switch '{}'",
            switch.id()
        ));
    }

    Ok(true)
}

#[cfg(test)]
mod tests;
