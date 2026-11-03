/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Deployment-supplied fabric sizing, grouping, and link calculation rules.
//!
//! Catalog construction validates definitions and rejects ambiguous selectors.
//! Callers supply discovered positions to calculate targets without device I/O.
//! Platform wiring parameters remain deployment data.

mod calculation;
mod controller;

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use crate::libnmxc::managed_config::validate_static_config_value;
pub(crate) use controller::validate_static_config_key;

pub use calculation::{FabricGrouping, FabricLinkCalculation};
pub use controller::{FabricControllerRule, FabricStaticConfigAction, FabricStaticConfigTarget};

use std::collections::{BTreeMap, BTreeSet};

use crate::domain::node::{NodeKind, NodeType};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAX_CATALOG_BYTES: u64 = 2 * 1024 * 1024;
const MAX_PROFILES: usize = 64;
const MAX_POSITIONS: usize = 256;
const MAX_OPTIONS: usize = 256;
const MAX_INTERFACES: usize = 1024;

/// Explicit target administrative state for a controlled interface.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FabricLinkState {
    /// Administratively enable the interface.
    Up,

    /// Administratively disable the interface.
    Down,
}

/// Physical tray coordinates supplied by device discovery.
///
/// # Examples
///
/// ```
/// use rackmanagementservice::config::fabric_layout::FabricPosition;
///
/// let position = FabricPosition { tray_index: 3, slot_number: 7 };
/// ```
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct FabricPosition {
    /// Tray index within a chassis.
    pub tray_index: u32,

    /// Slot number within a chassis.
    pub slot_number: u32,
}

/// Supported fabric size, coordinate grouping, and optional controller settings.
///
/// # Examples
///
/// ```
/// use rackmanagementservice::config::fabric_layout::FabricRule;
///
/// let rule = FabricRule {
///     compute_tray_count: 2,
///     switch_tray_count: 1,
///     grouping: None,
///     controller_rules: vec![],
/// };
/// // Grouping is omitted when the rule's counts equal the profile's physical capacity.
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FabricRule {
    /// Number of compute trays required in each resolved group.
    pub compute_tray_count: u32,

    /// Number of switch trays required in each resolved group.
    pub switch_tray_count: u32,

    /// Required for smaller fabrics; absent for the profile's full-capacity size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouping: Option<FabricGrouping>,

    /// Exact topology rules, each covering every static setting owned by this profile.
    /// May be omitted for link-only profiles. If any size declares controller rules,
    /// every size must declare at least one, with full-capacity rules for each topology.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub controller_rules: Vec<FabricControllerRule>,
}

/// Connectivity targets selected using observed hardware and topology identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FabricLayoutProfile {
    /// Stable RMS switch type names accepted by this profile.
    pub switch_node_types: BTreeSet<String>,

    /// Stable RMS compute type names accepted by this profile.
    pub compute_node_types: BTreeSet<String>,

    /// Exact physical topology identity reported by the switches.
    pub physical_topology: String,

    /// Maximum compute count accepted from the physical inventory.
    pub compute_tray_count: u32,

    /// Complete physical switch count required for every managed request.
    pub switch_tray_count: u32,

    /// Owned interface names and their full-inventory administrative targets.
    /// Layout calculation may emit only these names; unselected switches disable all of them.
    /// Configuration accepts individual `acp`/`fnm` names and inclusive ranges such as
    /// `acp1-4`. Loading expands ranges into individual names and rejects overlaps,
    /// even when their states match. Serialization writes individual names.
    #[serde(deserialize_with = "deserialize_full_link_states")]
    pub full_link_states: BTreeMap<String, FabricLinkState>,

    /// Supported sizes and grouping rules.
    pub fabric_rules: Vec<FabricRule>,

    /// Private coordinate rules expanded against discovered group members during planning.
    pub link_calculation: FabricLinkCalculation,
}

/// Expands interface ranges after configuration sources have merged their keys.
/// Rejects invalid names and overlaps rather than choosing a state for shared targets.
/// Checks the expanded interface count before allocating each range's targets.
fn deserialize_full_link_states<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, FabricLinkState>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let entries = BTreeMap::<String, FabricLinkState>::deserialize(deserializer)?;

    let parse_index = |text: &str| match text.parse::<u32>() {
        Ok(number) if number > 0 && number.to_string() == text => Ok(number),
        _ => Err(D::Error::custom(
            "interface indices must be canonical positive u32 integers",
        )),
    };

    let mut states = BTreeMap::new();

    for (expression, state) in entries {
        let (prefix, suffix) = expression
            .strip_prefix("acp")
            .map(|suffix| ("acp", suffix))
            .or_else(|| expression.strip_prefix("fnm").map(|suffix| ("fnm", suffix)))
            .ok_or_else(|| {
                D::Error::custom("controlled interfaces must use the acp or fnm prefix")
            })?;

        let (first, last) = suffix.split_once('-').unwrap_or((suffix, suffix));
        let first = parse_index(first)?;
        let last = parse_index(last)?;

        let width = last
            .checked_sub(first)
            .map(|distance| u64::from(distance) + 1)
            .ok_or_else(|| D::Error::custom("interface ranges must be ascending"))?;

        if width + states.len() as u64 > MAX_INTERFACES as u64 {
            return Err(D::Error::custom(format!(
                "expanded controlled interfaces exceed {MAX_INTERFACES}"
            )));
        }

        for number in first..=last {
            if states.insert(format!("{prefix}{number}"), state).is_some() {
                return Err(D::Error::custom("controlled interface entries overlap"));
            }
        }
    }

    Ok(states)
}

/// Deployment-owned placement and calculation rules under `workflows.fabric_layout`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FabricLayoutConfig {
    /// Supported hardware profiles whose rule definitions are validated at startup.
    pub profiles: Vec<FabricLayoutProfile>,
}

/// Immutable validated deployment catalog; an empty catalog selects no profiles.
#[derive(Clone, Debug, Default)]
pub struct FabricLayoutCatalog {
    profiles: Vec<FabricLayoutProfile>,
    identity: String,
}

/// Invalid deployment configuration.
#[derive(Debug, Error)]
pub enum FabricCatalogError {
    /// The catalog document is invalid.
    #[error("invalid fabric layout catalog: {0}")]
    Invalid(String),
}

impl FabricLayoutCatalog {
    /// Validates typed rule definitions before serving managed requests.
    ///
    /// The identity hashes only typed rules, so unrelated RMS settings and TOML
    /// formatting do not affect it.
    ///
    /// # Errors
    ///
    /// Rejects invalid profiles, ambiguous selectors, and invalid calculations.
    pub fn from_config(config: &FabricLayoutConfig) -> Result<Self, FabricCatalogError> {
        if config.profiles.is_empty() || config.profiles.len() > MAX_PROFILES {
            return Err(FabricCatalogError::Invalid(
                "profile count must be 1 through 64".into(),
            ));
        }

        let bytes = serde_json::to_vec(config)
            .map_err(|error| FabricCatalogError::Invalid(error.to_string()))?;

        if bytes.len() as u64 > MAX_CATALOG_BYTES {
            return Err(FabricCatalogError::Invalid("document exceeds 2 MiB".into()));
        }

        for (index, profile) in config.profiles.iter().enumerate() {
            profile.validate().map_err(|error| {
                FabricCatalogError::Invalid(format!("profiles[{index}]: {error}"))
            })?;
        }

        // Selection must be unique before device reads; overlapping type selectors
        // for one physical topology cannot be disambiguated by caller labels.
        for (index, profile) in config.profiles.iter().enumerate() {
            let ambiguous = config.profiles.iter().skip(index + 1).any(|other| {
                profile.physical_topology == other.physical_topology
                    && !profile
                        .switch_node_types
                        .is_disjoint(&other.switch_node_types)
            });

            if ambiguous {
                return Err(FabricCatalogError::Invalid(
                    "ambiguous profile selectors".into(),
                ));
            }
        }

        Ok(Self {
            profiles: config.profiles.clone(),
            identity: format!("{:x}", Sha256::digest(bytes)),
        })
    }

    #[cfg(test)]
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, FabricCatalogError> {
        let config = serde_json::from_slice(bytes)
            .map_err(|error| FabricCatalogError::Invalid(error.to_string()))?;

        Self::from_config(&config)
    }

    /// Digest of the typed deployment rules used to fence unresolved-operation retries.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Selects a profile only when all observed switch types match its topology.
    /// Configuration validation rejects overlapping selectors, so selection is unique.
    pub fn select(
        &self,
        physical_topology: &str,
        switch_types: &BTreeSet<String>,
    ) -> Option<&FabricLayoutProfile> {
        self.profiles.iter().find(|profile| {
            profile.physical_topology == physical_topology
                && !switch_types.is_empty()
                && switch_types.is_subset(&profile.switch_node_types)
        })
    }
}

impl FabricLayoutProfile {
    /// Checks profile identity, supported sizes, link rules, and controller ownership.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.physical_topology.trim().is_empty()
            || self.compute_tray_count == 0
            || self.switch_tray_count == 0
            || u64::from(self.compute_tray_count) + u64::from(self.switch_tray_count)
                > MAX_POSITIONS as u64
            || self.fabric_rules.is_empty()
            || self.fabric_rules.len() > MAX_OPTIONS
            || self.full_link_states.is_empty()
            || self.full_link_states.len() > MAX_INTERFACES
        {
            return Err("profile identity, inventory, fabric rules, and interfaces must be bounded and nonempty".into());
        }

        for (types, kind) in [
            (&self.switch_node_types, NodeKind::Switch),
            (&self.compute_node_types, NodeKind::Compute),
        ] {
            if types.is_empty()
                || types.iter().any(|name| {
                    NodeType::from_stable_name(name)
                        .is_none_or(|node_type| node_type.kind() != kind)
                })
            {
                return Err(
                    "profile node selectors must name canonical node types of the declared role"
                        .into(),
                );
            }
        }

        if self.full_link_states.keys().any(|id| {
            let suffix = id.strip_prefix("acp").or_else(|| id.strip_prefix("fnm"));

            !suffix.is_some_and(|suffix| {
                suffix
                    .parse::<u32>()
                    .is_ok_and(|number| number > 0 && number.to_string() == suffix)
            })
        }) {
            return Err("controlled interfaces must be expanded supported interface names".into());
        }

        self.link_calculation.validate()?;

        let mut sizes = BTreeSet::new();

        for placement in &self.fabric_rules {
            if placement.compute_tray_count == 0
                || placement.switch_tray_count == 0
                || placement.compute_tray_count > self.compute_tray_count
                || placement.switch_tray_count > self.switch_tray_count
                || !sizes.insert((placement.compute_tray_count, placement.switch_tray_count))
            {
                return Err(
                    "fabric sizes must be unique, positive, and fit within the physical inventory"
                        .into(),
                );
            }

            let full_capacity = placement.compute_tray_count == self.compute_tray_count
                && placement.switch_tray_count == self.switch_tray_count;

            match (&placement.grouping, full_capacity) {
                (Some(grouping), false) => grouping.validate()?,
                (None, true) => {}
                _ => return Err("grouping is required only for smaller fabric sizes".into()),
            }
        }

        if !sizes.contains(&(self.compute_tray_count, self.switch_tray_count)) {
            return Err("fabric rules must include the full physical inventory size".into());
        }

        self.validate_controller_rules()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod catalog_tests;
