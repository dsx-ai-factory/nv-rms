/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Coordinate arithmetic for deployment-configured interface numbering.
//!
//! Rules have no executable expressions, recursion, device access, or platform
//! constants. Callers validate rule definitions, then expand
//! calculations against discovered positions without changing devices.

use std::collections::{BTreeMap, BTreeSet};

use super::{FabricLinkState, FabricPosition};
use crate::domain::node::NodeKind;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Coordinate {
    TrayIndex,
    SlotNumber,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IndexTerm {
    node: NodeKind,
    coordinate: Coordinate,
    divisor: u32,
    modulus: Option<u32>,
    remap: Option<BTreeMap<String, u32>>,
    multiplier: i64,
}

impl IndexTerm {
    /// Validates coordinate transforms for link numbering.
    fn validate(&self) -> Result<(), String> {
        if self.node == NodeKind::Powershelf {
            return Err("node: must be compute or switch".into());
        }

        if self.divisor == 0 {
            return Err("divisor: must be positive".into());
        }

        if self.modulus == Some(0) {
            return Err("modulus: must be positive".into());
        }

        if self.remap.as_ref().is_some_and(|map| {
            map.is_empty()
                || map.keys().any(|key| {
                    !key.parse::<u32>()
                        .is_ok_and(|number| number.to_string() == *key)
                })
        }) {
            return Err("remap: must be nonempty with canonical decimal u32 keys".into());
        }

        Ok(())
    }

    /// Applies division, remainder, optional private remapping, and scaling in order.
    fn evaluate(&self, compute: FabricPosition, switch: FabricPosition) -> Result<i64, String> {
        let position = match self.node {
            NodeKind::Compute => compute,
            NodeKind::Switch => switch,
            NodeKind::Powershelf => return Err("node: must be compute or switch".into()),
        };

        let coordinate = match self.coordinate {
            Coordinate::TrayIndex => position.tray_index,
            Coordinate::SlotNumber => position.slot_number,
        };

        let quotient = coordinate
            .checked_div(self.divisor)
            .ok_or_else(|| "divisor: must be positive".to_owned())?;

        let transformed = if let Some(modulus) = self.modulus {
            quotient
                .checked_rem(modulus)
                .ok_or_else(|| "modulus: must be positive".to_owned())?
        } else {
            quotient
        };

        let mapped = if let Some(remap) = &self.remap {
            *remap
                .get(&transformed.to_string())
                .ok_or_else(|| "remap: does not cover a supplied coordinate".to_owned())?
        } else {
            transformed
        };

        i64::from(mapped)
            .checked_mul(self.multiplier)
            .ok_or_else(|| "multiplier: arithmetic overflow".to_owned())
    }
}

/// Members joined by equal transformed coordinates during discovery planning.
pub(crate) struct DiscoveredGroup {
    pub(crate) computes: BTreeSet<FabricPosition>,
    pub(crate) switches: BTreeSet<FabricPosition>,
}

/// Coordinate transforms joining discovered compute and switch trays into groups.
///
/// A compute and switch belong to the same group when their transformed
/// coordinates are equal. Each transform uses only its own node role's position.
/// Groups missing either role are excluded; the planner checks requested counts.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FabricGrouping {
    /// Transform of a discovered compute tray index or slot number.
    compute: IndexTerm,

    /// Transform of a discovered switch tray index or slot number.
    switch: IndexTerm,
}

impl FabricGrouping {
    pub(super) fn validate(&self) -> Result<(), String> {
        self.compute.validate()?;
        self.switch.validate()?;

        if self.compute.node != NodeKind::Compute || self.switch.node != NodeKind::Switch {
            return Err(
                "grouping transforms must use coordinates of their declared node role".into(),
            );
        }

        Ok(())
    }

    /// Resolves complete coordinate groups without enumerating caller-independent positions.
    /// Missing remaps and arithmetic failures reject planning before device writes.
    pub(crate) fn resolve(
        &self,
        computes: &BTreeSet<FabricPosition>,
        switches: &BTreeSet<FabricPosition>,
    ) -> Result<Vec<DiscoveredGroup>, String> {
        self.validate()?;

        let mut compute_groups: BTreeMap<i64, BTreeSet<FabricPosition>> = BTreeMap::new();
        let mut switch_groups: BTreeMap<i64, BTreeSet<FabricPosition>> = BTreeMap::new();

        for position in computes {
            let group = self.compute.evaluate(*position, *position)?;
            compute_groups.entry(group).or_default().insert(*position);
        }

        for position in switches {
            let group = self.switch.evaluate(*position, *position)?;
            switch_groups.entry(group).or_default().insert(*position);
        }

        Ok(switch_groups
            .into_iter()
            .filter_map(|(group, switches)| {
                compute_groups
                    .remove(&group)
                    .map(|computes| DiscoveredGroup { computes, switches })
            })
            .collect())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LinkRule {
    prefix: String,
    base: i64,
    terms: Vec<IndexTerm>,
    offsets: Vec<i64>,
    width: u32,
    state: FabricLinkState,
}

impl LinkRule {
    /// Emits one numbered block per offset for a participating compute/switch pair.
    fn apply(
        &self,
        compute: FabricPosition,
        switch: FabricPosition,
        states: &mut BTreeMap<String, FabricLinkState>,
        assigned: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        let first = self
            .terms
            .iter()
            .enumerate()
            .try_fold(self.base, |sum, (index, term)| {
                let value = term
                    .evaluate(compute, switch)
                    .map_err(|error| format!("terms[{index}].{error}"))?;

                sum.checked_add(value).ok_or_else(|| {
                    format!("terms[{index}]: arithmetic overflow while adding to base")
                })
            })?;

        for (index, offset) in self.offsets.iter().enumerate() {
            let start = first
                .checked_add(*offset)
                .ok_or_else(|| format!("offsets[{index}]: arithmetic overflow"))?;

            for lane in 0..self.width {
                let number = start
                    .checked_add(i64::from(lane))
                    .ok_or_else(|| format!("offsets[{index}]: arithmetic overflow within block"))?;

                let number = u32::try_from(number)
                    .map_err(|_| format!("offsets[{index}]: interface number is out of range"))?;

                if number == 0 {
                    return Err(format!(
                        "offsets[{index}]: interface number must be positive"
                    ));
                }

                let id = format!("{}{number}", self.prefix);

                if !assigned.insert(id.clone()) {
                    return Err(format!(
                        "offsets[{index}]: assigns an interface more than once"
                    ));
                }

                let target = states.get_mut(&id).ok_or_else(|| {
                    format!("offsets[{index}]: targets an uncontrolled interface")
                })?;

                *target = self.state;
            }
        }

        Ok(())
    }
}

/// Deployment parameters for numbered-interface calculations.
///
/// For each participating compute/switch pair, rules emit
/// `prefix + (base + sum(transformed_coordinate * multiplier) + offset + lane)`.
/// Each term selects a compute or switch node and its tray index or slot number.
/// Coordinates are used without renumbering. Each term divides first,
/// optionally takes a remainder, then optionally applies a private remap table.
/// Every rule, block width, and numbering parameter comes from configuration.
///
/// # Examples
///
/// ```
/// use figment::Figment;
/// use figment::providers::{Format, Toml};
/// use rackmanagementservice::config::fabric_layout::FabricLinkCalculation;
///
/// let source = r#"
/// default_state = "down"
/// [[rules]]
/// prefix = "port"
/// base = 1
/// terms = []
/// offsets = [0]
/// width = 2
/// state = "up"
/// "#;
///
/// let calculation: FabricLinkCalculation = Figment::from(Toml::string(source)).extract()?;
/// calculation.validate()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FabricLinkCalculation {
    /// Explicit UP or DOWN state for controlled interfaces not emitted by a rule.
    default_state: FabricLinkState,

    /// Rules evaluated for each participating compute/switch pair.
    rules: Vec<LinkRule>,
}

impl FabricLinkCalculation {
    /// Checks rule structure independently of live device coordinates.
    ///
    /// # Errors
    ///
    /// Rejects empty rules or offsets, invalid prefixes, zero widths, zero
    /// divisors or moduli, unsupported node kinds, and empty or noncanonical remaps.
    pub fn validate(&self) -> Result<(), String> {
        if self.rules.is_empty() {
            return Err("rules: must be nonempty".into());
        }

        for (index, rule) in self.rules.iter().enumerate() {
            if rule.prefix.is_empty() || !rule.prefix.bytes().all(|byte| byte.is_ascii_lowercase())
            {
                return Err(format!(
                    "rules[{index}].prefix: must contain lowercase ASCII letters"
                ));
            }

            if rule.offsets.is_empty() {
                return Err(format!("rules[{index}].offsets: must be nonempty"));
            }

            if rule.width == 0 {
                return Err(format!("rules[{index}].width: must be positive"));
            }

            for (term_index, term) in rule.terms.iter().enumerate() {
                term.validate()
                    .map_err(|error| format!("rules[{index}].terms[{term_index}].{error}"))?;
            }
        }

        Ok(())
    }

    /// Expands discovered group members into complete interface targets.
    /// Missing remaps, overflow, duplicate assignments, or uncontrolled interface
    /// targets reject planning. Unemitted interfaces retain `default_state`.
    /// Duplicate assignments fail even when their target states are equal.
    /// A failure returns no partial targets and leaves caller inputs unchanged.
    /// The interface-name set defines ownership independently of target states.
    /// The caller controls inventory and rule sizes; this method imposes no work
    /// budget or count limits and performs no device I/O.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid rules, empty inventory or interfaces,
    /// or any expansion failure described above.
    pub fn expand(
        &self,
        computes: &BTreeSet<FabricPosition>,
        switches: &BTreeSet<FabricPosition>,
        interfaces: &BTreeSet<String>,
    ) -> Result<BTreeMap<FabricPosition, BTreeMap<String, FabricLinkState>>, String> {
        self.validate()?;

        if computes.is_empty() || switches.is_empty() || interfaces.is_empty() {
            return Err("link calculation inventory and interfaces must be nonempty".into());
        }

        switches
            .iter()
            .map(|position| {
                let states = self.expand_switch(computes, *position, interfaces)?;

                Ok((*position, states))
            })
            .collect()
    }

    /// Builds complete interface targets and rejects aliases across rules or computes.
    fn expand_switch(
        &self,
        computes: &BTreeSet<FabricPosition>,
        switch: FabricPosition,
        interfaces: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, FabricLinkState>, String> {
        let mut states = interfaces
            .iter()
            .map(|id| (id.clone(), self.default_state))
            .collect();

        let mut assigned = BTreeSet::new();

        for compute in computes {
            for (index, rule) in self.rules.iter().enumerate() {
                rule.apply(*compute, switch, &mut states, &mut assigned)
                    .map_err(|error| format!("rules[{index}].{error}"))?;
            }
        }

        Ok(states)
    }
}
