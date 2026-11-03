/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Complete controller targets for exact deployment-defined topology and size pairs.

use std::collections::BTreeSet;

use super::{FabricLayoutProfile, MAX_OPTIONS};
use crate::libnmxc::managed_config::{validate_static_config_name, validate_static_config_value};
use crate::libnmxc::{NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY};

use serde::{Deserialize, Serialize};

const MAX_STATIC_CONFIG_KEYS: usize = 1024;

/// A literal value or explicit absence; a value of `"0"` remains a set action.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FabricStaticConfigAction {
    /// Write the supplied controller literal.
    Set {
        /// Nonempty unquoted value without comments, escaping, or control characters.
        value: String,
    },

    /// Remove the setting from the selected primary's configuration file.
    // Keep the struct variant so Serde rejects extra fields such as `value`.
    // An internally tagged unit variant ignores them despite `deny_unknown_fields`.
    Remove {},
}

/// One deployment-owned setting's target.
///
/// # Examples
///
/// ```
/// use rackmanagementservice::config::fabric_layout::{
///     FabricStaticConfigAction, FabricStaticConfigTarget,
/// };
///
/// let target = FabricStaticConfigTarget {
///     config_file_name: "fm_config".into(),
///     key: "SYNTHETIC_SETTING".into(),
///     action: FabricStaticConfigAction::Set { value: "0".into() },
/// };
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FabricStaticConfigTarget {
    /// Exact controller configuration file basename.
    pub config_file_name: String,

    /// Exact, case-sensitive identifier sent unchanged to the controller.
    pub key: String,

    /// Required value or removal for this setting.
    pub action: FabricStaticConfigAction,
}

/// Complete static targets selected by exact requested topology within a fabric-size rule.
///
/// # Examples
///
/// ```
/// use rackmanagementservice::config::fabric_layout::FabricControllerRule;
///
/// let rule = FabricControllerRule {
///     topology_type: "synthetic-topology".into(),
///     static_configs: vec![],
/// };
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FabricControllerRule {
    /// Exact `ScaleUpFabricConfig.topology_type` literal, without case folding or parsing.
    pub topology_type: String,

    /// Exactly one action for every profile-owned key, with no unowned keys.
    pub static_configs: Vec<FabricStaticConfigTarget>,
}

impl FabricLayoutProfile {
    /// Validates complete ownership and a restoration rule for every declared topology.
    pub(super) fn validate_controller_rules(&self) -> Result<(), String> {
        let rules: Vec<_> = self
            .fabric_rules
            .iter()
            .enumerate()
            .flat_map(|(fabric_index, fabric)| {
                fabric
                    .controller_rules
                    .iter()
                    .enumerate()
                    .map(move |(rule_index, rule)| (fabric_index, rule_index, fabric, rule))
            })
            .collect();

        // Link-only profiles do not declare controller ownership.
        if rules.is_empty() {
            return Ok(());
        }

        if let Some(index) = self
            .fabric_rules
            .iter()
            .position(|fabric| fabric.controller_rules.is_empty())
        {
            return Err(format!(
                "fabric_rules[{index}].controller_rules: at least one rule is required when the profile declares controller settings"
            ));
        }

        if rules.len() > MAX_OPTIONS {
            return Err("fabric_rules: total controller rule count exceeds 256".into());
        }

        // Ownership is inferred across all sizes and topologies. Every rule must
        // cover this union explicitly, including removal of settings no longer used.
        let managed_keys: BTreeSet<_> = rules
            .iter()
            .flat_map(|(_, _, _, rule)| {
                rule.static_configs
                    .iter()
                    .map(|target| (target.config_file_name.as_str(), target.key.clone()))
            })
            .collect();

        if managed_keys.len() > MAX_STATIC_CONFIG_KEYS {
            return Err("fabric_rules: managed static configuration key count exceeds 1024".into());
        }

        let full_size = (self.compute_tray_count, self.switch_tray_count);

        let mut selectors = BTreeSet::new();
        let mut topologies = BTreeSet::new();
        let mut restoration_topologies = BTreeSet::new();

        for (fabric_index, rule_index, fabric, rule) in rules {
            let location = format!("fabric_rules[{fabric_index}].controller_rules[{rule_index}]");

            validate_static_config_value(&rule.topology_type)
                .map_err(|error| format!("{location}.topology_type: {error}"))?;

            let size = (fabric.compute_tray_count, fabric.switch_tray_count);

            if !selectors.insert((rule.topology_type.as_str(), size)) {
                return Err(format!(
                    "{location}.topology_type: controller rule topology and size selectors must be unique"
                ));
            }

            topologies.insert(rule.topology_type.as_str());

            if size == full_size {
                restoration_topologies.insert(rule.topology_type.as_str());
            }

            let mut keys = BTreeSet::new();

            for (index, target) in rule.static_configs.iter().enumerate() {
                let target_location = format!("{location}.static_configs[{index}]");

                validate_static_config_key(&target.config_file_name, &target.key)
                    .map_err(|error| format!("{target_location}.{error}"))?;

                if let FabricStaticConfigAction::Set { value } = &target.action {
                    validate_static_config_value(value)
                        .map_err(|error| format!("{target_location}.action.value: {error}"))?;
                }

                if !keys.insert((target.config_file_name.as_str(), target.key.clone())) {
                    return Err(format!(
                        "{target_location}.key: controller targets must be unique within each file"
                    ));
                }
            }

            if keys != managed_keys {
                return Err(format!(
                    "{location}.static_configs: controller targets must cover exactly the managed static configuration keys"
                ));
            }
        }

        if topologies != restoration_topologies {
            return Err(
                "fabric_rules: every controller topology requires a full-capacity restoration rule"
                    .into(),
            );
        }

        Ok(())
    }
}

/// Validates target names and reserves topology for the dedicated request field.
pub(crate) fn validate_static_config_key(file: &str, key: &str) -> Result<(), String> {
    validate_static_config_name(file, key)?;

    if file.eq_ignore_ascii_case(NMX_C_FM_CONFIG_FILE)
        && key.eq_ignore_ascii_case(NMX_C_TOPOLOGY_KEY)
    {
        return Err(
            "key: managed static configuration topology must use the dedicated topology field"
                .into(),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests;
