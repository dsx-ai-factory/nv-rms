/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Resolves caller intent using discovered positions and private calculation rules.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::fabric_layout::{
    FabricLayoutProfile, FabricLinkState, FabricPosition, FabricStaticConfigAction,
    validate_static_config_key, validate_static_config_value,
};

use librms::protos::{rack_manager as rm, rack_manager_v2 as rm_v2};

/// Candidate membership and link targets calculated from live inventory.
struct ResolvedPlacement {
    compute_positions: BTreeSet<FabricPosition>,
    switch_positions: BTreeSet<FabricPosition>,
    links: BTreeMap<FabricPosition, BTreeMap<String, FabricLinkState>>,
}

/// A failed search depends on the next request and occupied nodes, not assignment order.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct FailedPlacementState {
    next_request: usize,
    compute_positions: BTreeSet<FabricPosition>,
    switch_positions: BTreeSet<FabricPosition>,
}

/// Device location established by live discovery, never by caller labels.
#[derive(Clone, Debug)]
pub(super) struct ObservedPlacement {
    /// Caller node ID retained for assignments and response correlation.
    pub(super) node_id: String,

    /// Canonical RMS type used to select deployment rules.
    pub(super) node_type: String,

    /// Physical rack identity reported by the device's topology-location fields,
    /// not the individual node's serial number.
    pub(super) chassis_serial_number: String,

    /// Tray index and physical slot read during discovery.
    pub(super) position: FabricPosition,

    /// Switch-reported physical topology; compute discovery does not supply one.
    pub(super) physical_topology: Option<String>,
}

/// Resolved group and deployment-derived controller configuration.
pub(super) struct PlannedFabric {
    /// Resolved compute and switch membership, including the selected primary.
    pub(super) assignment: rm_v2::ScaleUpFabricMembers,

    /// Requested topology, group-specific owned targets, and unmanaged caller values.
    pub(super) config: rm_v2::ScaleUpFabricConfig,

    /// Owned file/key pairs that must be absent from the controller configuration.
    pub(super) absent_static_configs: BTreeSet<(String, String)>,
}

/// Complete target covering assigned and unassigned switches.
pub(super) struct FabricPlan {
    /// Disjoint assignments in the caller's fabric-specification order.
    pub(super) fabrics: Vec<PlannedFabric>,

    /// Complete controlled-interface targets keyed by caller switch ID.
    pub(super) links: BTreeMap<String, BTreeMap<String, FabricLinkState>>,

    /// One fabric spanning the selected physical topology's complete capacity.
    pub(super) full_topology: bool,
}

/// Resolves every membership and controller target before returning; failure has no side effects.
///
/// Candidate placements are ordered by discovered switch and compute positions.
/// The first supported disjoint assignment wins; an explicit primary constrains
/// its group's candidates. Unassigned switches receive administrative DOWN
/// targets. A single full-capacity specification uses the restoration baseline.
pub(super) fn plan_layout(
    profile: &FabricLayoutProfile,
    switches: &[ObservedPlacement],
    computes: &[ObservedPlacement],
    requested: &[rm_v2::ScaleUpFabricSpec],
    requested_config: &rm_v2::ScaleUpFabricConfig,
) -> Result<FabricPlan, String> {
    profile.validate()?;
    validate_inventory(profile, switches, computes)?;

    if let [fabric] = requested
        && fabric.compute_tray_count == profile.compute_tray_count
        && fabric.switch_tray_count == profile.switch_tray_count
    {
        return plan_full_rack(
            profile,
            switches,
            computes,
            requested_config,
            fabric.primary_switch_node_id.as_deref(),
        );
    }

    if requested.is_empty() || computes.is_empty() {
        return Err("smaller fabric layouts require compute inventory and nonempty fabrics".into());
    }

    let config = validate_managed_config(requested_config.clone())?;

    // Reject impossible totals before searching permutations of disjoint groups.
    // Subtract from inventory counts so oversized requests cannot overflow a sum.
    let mut remaining_computes = computes.len() as u64;
    let mut remaining_switches = switches.len() as u64;

    for fabric in requested {
        remaining_computes = remaining_computes
            .checked_sub(u64::from(fabric.compute_tray_count))
            .ok_or("requested fabric compute count exceeds supplied inventory")?;

        remaining_switches = remaining_switches
            .checked_sub(u64::from(fabric.switch_tray_count))
            .ok_or("requested fabric switch count exceeds supplied inventory")?;
    }

    let switch_positions: BTreeMap<_, _> = switches
        .iter()
        .map(|node| (node.node_id.as_str(), node.position))
        .collect();

    let placements = resolve_placements(profile, switches, computes, requested)?;

    let candidates = matching_placements(&placements, requested, &switch_positions);

    let mut selected = Vec::with_capacity(requested.len());

    if !select_placements(&candidates, &mut selected, &mut BTreeSet::new()) {
        return Err("requested fabric memberships have no supported disjoint placement".into());
    }

    let mut links: BTreeMap<_, _> = switches
        .iter()
        .map(|node| {
            (
                node.node_id.clone(),
                profile
                    .full_link_states
                    .keys()
                    .map(|id| (id.clone(), FabricLinkState::Down))
                    .collect(),
            )
        })
        .collect();

    let mut fabrics = Vec::with_capacity(requested.len());

    for (spec, placement) in requested.iter().zip(selected) {
        let (config, absent_static_configs) = resolve_controller_config(
            profile,
            &config,
            spec.compute_tray_count,
            spec.switch_tray_count,
        )?;

        let mut switch_members: Vec<_> = switches
            .iter()
            .filter(|node| placement.switch_positions.contains(&node.position))
            .collect();

        switch_members.sort_by_key(|node| node.position);

        let primary = spec
            .primary_switch_node_id
            .clone()
            .or_else(|| switch_members.first().map(|node| node.node_id.clone()))
            .ok_or_else(|| "resolved fabric has no primary candidate".to_owned())?;

        for (position, states) in &placement.links {
            let node = switch_members
                .iter()
                .find(|node| node.position == *position)
                .ok_or_else(|| "private link target has no discovered switch".to_owned())?;

            links.insert(node.node_id.clone(), states.clone());
        }

        let mut compute_members: Vec<_> = computes
            .iter()
            .filter(|node| placement.compute_positions.contains(&node.position))
            .collect();

        compute_members.sort_by_key(|node| node.position);

        fabrics.push(PlannedFabric {
            assignment: rm_v2::ScaleUpFabricMembers {
                compute_node_ids: compute_members
                    .into_iter()
                    .map(|node| node.node_id.clone())
                    .collect(),
                switch_node_ids: switch_members
                    .into_iter()
                    .map(|node| node.node_id.clone())
                    .collect(),
                primary_switch_node_id: primary,
            },
            config,
            absent_static_configs,
        });
    }

    Ok(FabricPlan {
        fabrics,
        links,
        full_topology: false,
    })
}

/// Expands supported group sizes into complete link targets from discovery.
fn resolve_placements(
    profile: &FabricLayoutProfile,
    switches: &[ObservedPlacement],
    computes: &[ObservedPlacement],
    requested: &[rm_v2::ScaleUpFabricSpec],
) -> Result<Vec<ResolvedPlacement>, String> {
    let compute_positions = computes.iter().map(|node| node.position).collect();
    let switch_positions = switches.iter().map(|node| node.position).collect();
    let interfaces = profile.full_link_states.keys().cloned().collect();

    let mut placements = Vec::new();

    for config in profile.fabric_rules.iter().filter(|rule| {
        requested.iter().any(|spec| {
            spec.compute_tray_count == rule.compute_tray_count
                && spec.switch_tray_count == rule.switch_tray_count
        })
    }) {
        // Full-capacity rules use the explicit restoration baseline, not grouping.
        let Some(grouping) = &config.grouping else {
            continue;
        };

        for group in grouping.resolve(&compute_positions, &switch_positions)? {
            if group.computes.len() as u64 != u64::from(config.compute_tray_count)
                || group.switches.len() as u64 != u64::from(config.switch_tray_count)
            {
                continue;
            }

            let links =
                profile
                    .link_calculation
                    .expand(&group.computes, &group.switches, &interfaces)?;

            placements.push(ResolvedPlacement {
                compute_positions: group.computes,
                switch_positions: group.switches,
                links,
            });
        }
    }

    placements.sort_by(|a, b| {
        a.switch_positions
            .cmp(&b.switch_positions)
            .then_with(|| a.compute_positions.cmp(&b.compute_positions))
    });

    Ok(placements)
}

/// Matches counts and primary preferences before disjoint selection.
fn matching_placements<'a>(
    placements: &'a [ResolvedPlacement],
    requested: &[rm_v2::ScaleUpFabricSpec],
    switch_positions: &BTreeMap<&str, FabricPosition>,
) -> Vec<Vec<&'a ResolvedPlacement>> {
    requested
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            placements
                .iter()
                .filter(|placement| {
                    if placement.compute_positions.len() as u64
                        != u64::from(spec.compute_tray_count)
                        || placement.switch_positions.len() as u64
                            != u64::from(spec.switch_tray_count)
                    {
                        return false;
                    }

                    if spec.primary_switch_node_id.as_deref().is_some_and(|id| {
                        switch_positions
                            .get(id)
                            .is_none_or(|position| !placement.switch_positions.contains(position))
                    }) {
                        return false;
                    }

                    // Exclude another fabric's requested primary before searching
                    // so earlier choices cannot consume that switch.
                    if requested.iter().enumerate().any(|(other_index, other)| {
                        other_index != index
                            && other.primary_switch_node_id.as_deref().is_some_and(|id| {
                                switch_positions.get(id).is_some_and(|position| {
                                    placement.switch_positions.contains(position)
                                })
                            })
                    }) {
                        return false;
                    }

                    true
                })
                .collect()
        })
        .collect()
}

/// Finds the first disjoint assignment in request order, skipping equivalent failed states.
/// The failure cache belongs to this candidate set and is shared only by its recursive search.
fn select_placements<'a>(
    candidates: &[Vec<&'a ResolvedPlacement>],
    selected: &mut Vec<&'a ResolvedPlacement>,
    failed: &mut BTreeSet<FailedPlacementState>,
) -> bool {
    let Some(options) = candidates.get(selected.len()) else {
        return true;
    };

    if candidates.iter().skip(selected.len()).any(Vec::is_empty) {
        return false;
    }

    let state = FailedPlacementState {
        next_request: selected.len(),
        compute_positions: selected
            .iter()
            .flat_map(|placement| placement.compute_positions.iter().copied())
            .collect(),
        switch_positions: selected
            .iter()
            .flat_map(|placement| placement.switch_positions.iter().copied())
            .collect(),
    };

    if failed.contains(&state) {
        return false;
    }

    for placement in options {
        if !placement
            .compute_positions
            .is_disjoint(&state.compute_positions)
            || !placement
                .switch_positions
                .is_disjoint(&state.switch_positions)
        {
            continue;
        }

        selected.push(placement);

        if select_placements(candidates, selected, failed) {
            return true;
        }

        selected.pop();
    }

    failed.insert(state);

    false
}

/// Requires all switches in one physical rack and a compute subset within capacity.
/// Nodes must report the same rack identity and have unique IDs and tray positions.
fn validate_inventory(
    profile: &FabricLayoutProfile,
    switches: &[ObservedPlacement],
    computes: &[ObservedPlacement],
) -> Result<(), String> {
    let serials: BTreeSet<_> = switches
        .iter()
        .chain(computes)
        .map(|node| node.chassis_serial_number.as_str())
        .collect();

    let ids: BTreeSet<_> = switches
        .iter()
        .chain(computes)
        .map(|node| node.node_id.as_str())
        .collect();

    if serials.len() != 1
        || serials.iter().any(|serial| serial.trim().is_empty())
        || ids.len() != switches.len() + computes.len()
        || ids.iter().any(|id| id.trim().is_empty())
        || switches.len() as u64 != u64::from(profile.switch_tray_count)
        || computes.len() as u64 > u64::from(profile.compute_tray_count)
    {
        return Err("observed inventory must contain every configured switch, fit within compute capacity, and have unique node IDs in one chassis".into());
    }

    let mut slots = BTreeSet::new();

    for nodes in [switches, computes] {
        let mut indices = BTreeSet::new();

        for node in nodes {
            if !indices.insert(node.position.tray_index) || !slots.insert(node.position.slot_number)
            {
                return Err(
                    "observed inventory contains duplicate tray indices or shared slots".into(),
                );
            }
        }
    }

    if switches.iter().any(|node| {
        !profile.switch_node_types.contains(&node.node_type)
            || node.physical_topology.as_deref() != Some(profile.physical_topology.as_str())
    }) || computes
        .iter()
        .any(|node| !profile.compute_node_types.contains(&node.node_type))
    {
        return Err(
            "observed hardware identity does not match the installed fabric profile".into(),
        );
    }

    Ok(())
}

/// Restores complete connectivity without requiring compute endpoints.
fn plan_full_rack(
    profile: &FabricLayoutProfile,
    switches: &[ObservedPlacement],
    computes: &[ObservedPlacement],
    requested_config: &rm_v2::ScaleUpFabricConfig,
    preferred_primary: Option<&str>,
) -> Result<FabricPlan, String> {
    if !computes.is_empty() && computes.len() as u64 != u64::from(profile.compute_tray_count) {
        return Err("full-topology compute inventory must be complete when supplied".into());
    }

    let config = validate_managed_config(requested_config.clone())?;

    let (config, absent_static_configs) = resolve_controller_config(
        profile,
        &config,
        profile.compute_tray_count,
        profile.switch_tray_count,
    )?;

    let mut ordered: Vec<_> = switches.iter().collect();
    ordered.sort_by_key(|node| node.position);

    let primary = match preferred_primary {
        Some(id) if ordered.iter().any(|node| node.node_id == id) => id.to_owned(),
        Some(_) => {
            return Err("preferred primary is absent from the supplied switch inventory".into());
        }
        None => ordered
            .first()
            .map(|node| node.node_id.clone())
            .ok_or_else(|| "full-inventory restoration has no switch".to_owned())?,
    };

    let links = ordered
        .iter()
        .map(|node| (node.node_id.clone(), profile.full_link_states.clone()))
        .collect();

    let mut ordered_computes: Vec<_> = computes.iter().collect();
    ordered_computes.sort_by_key(|node| node.position);

    Ok(FabricPlan {
        fabrics: vec![PlannedFabric {
            assignment: rm_v2::ScaleUpFabricMembers {
                compute_node_ids: ordered_computes
                    .into_iter()
                    .map(|node| node.node_id.clone())
                    .collect(),
                switch_node_ids: ordered
                    .into_iter()
                    .map(|node| node.node_id.clone())
                    .collect(),
                primary_switch_node_id: primary,
            },
            config,
            absent_static_configs,
        }],
        links,
        full_topology: true,
    })
}

/// Applies one exact rule, preserving unmanaged extras and rejecting owned-key conflicts.
fn resolve_controller_config(
    profile: &FabricLayoutProfile,
    config: &rm_v2::ScaleUpFabricConfig,
    compute_tray_count: u32,
    switch_tray_count: u32,
) -> Result<(rm_v2::ScaleUpFabricConfig, BTreeSet<(String, String)>), String> {
    let rule = profile
        .fabric_rules
        .iter()
        .find(|fabric| {
            fabric.compute_tray_count == compute_tray_count
                && fabric.switch_tray_count == switch_tray_count
        })
        .and_then(|fabric| {
            fabric
                .controller_rules
                .iter()
                .find(|rule| rule.topology_type == config.topology_type)
        })
        .ok_or_else(|| {
            "no controller rule matches the requested topology and fabric size".to_owned()
        })?;

    let mut values: BTreeMap<_, _> = config
        .extra_static_configs
        .iter()
        .map(|entry| {
            (
                (entry.config_file_name.clone(), entry.key.clone()),
                entry.value.clone(),
            )
        })
        .collect();

    let mut absent = BTreeSet::new();

    for target in &rule.static_configs {
        let key = (target.config_file_name.clone(), target.key.clone());

        match &target.action {
            FabricStaticConfigAction::Set { value } => {
                if values.get(&key).is_some_and(|supplied| supplied != value) {
                    return Err(
                        "caller static configuration conflicts with a controller set target".into(),
                    );
                }

                values.insert(key, value.clone());
            }
            FabricStaticConfigAction::Remove {} => {
                if values.contains_key(&key) {
                    return Err(
                        "caller static configuration conflicts with a controller removal target"
                            .into(),
                    );
                }

                absent.insert(key);
            }
        }
    }

    if values.len() > 1024 {
        return Err("resolved managed static config targets exceed 1024".into());
    }

    let config = rm_v2::ScaleUpFabricConfig {
        topology_type: config.topology_type.clone(),
        extra_static_configs: values
            .into_iter()
            .map(
                |((config_file_name, key), value)| rm::ScaleUpFabricStaticConfig {
                    config_file_name,
                    key,
                    value,
                },
            )
            .collect(),
    };

    Ok((config, absent))
}

/// Normalizes caller settings and checks the same literal rules used by the catalog.
fn validate_managed_config(
    config: rm_v2::ScaleUpFabricConfig,
) -> Result<rm_v2::ScaleUpFabricConfig, String> {
    let config = super::validate_scale_up_fabric_config(config)
        .map_err(|error| error.message().to_owned())?;

    validate_static_config_value(&config.topology_type)?;

    for entry in &config.extra_static_configs {
        validate_static_config_key(&entry.config_file_name, &entry.key)?;
        validate_static_config_value(&entry.value)?;
    }

    Ok(config)
}

#[cfg(test)]
mod tests;
