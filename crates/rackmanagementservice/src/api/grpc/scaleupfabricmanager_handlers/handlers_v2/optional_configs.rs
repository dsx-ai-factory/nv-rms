/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

//! Extracts scoped NMX-C `fm_config` entries from V2 node descriptors.
//!
//! Generic normalization consumes descriptors as node-type metadata and may
//! replace or remove them. V2 temporarily extracts `fm_config:<key>` attributes
//! before normalization, then either restores them for job handoff or maps the
//! selected primary switch's values to static configuration.

use crate::libnmxc::NMX_C_FM_CONFIG_FILE;

use librms::protos::rack_manager as rm;

/// Prefix that scopes descriptor attributes to NMX-C `fm_config`.
const FM_CONFIG_ATTRIBUTE_PREFIX: &str = "fm_config:";

/// Configuration attributes held outside a descriptor during normalization.
///
/// Each tuple contains an NMX-C key and its value. The container owns these
/// values so callers can restore their scoped attributes for the asynchronous
/// job boundary or convert the selected primary's values into static
/// configuration.
#[derive(Default)]
pub(super) struct OptionalNodeConfigs(Vec<(String, String)>);

impl OptionalNodeConfigs {
    /// Removes every `fm_config:<key>` attribute from a descriptor.
    ///
    /// A missing descriptor or a descriptor without scoped attributes produces
    /// an empty container. Entries are captured in key order so conversion is
    /// deterministic even though descriptor attributes are stored in a map.
    pub(super) fn take_from(descriptor: Option<&mut rm::NodeDescriptor>) -> Self {
        let Some(descriptor) = descriptor else {
            return Self::default();
        };

        let mut configs = Vec::new();

        descriptor.attributes.retain(|attribute, value| {
            let Some(key) = attribute.strip_prefix(FM_CONFIG_ATTRIBUTE_PREFIX) else {
                return true;
            };

            configs.push((key.to_owned(), std::mem::take(value)));
            false
        });

        configs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Self(configs)
    }

    /// Restores captured attributes after generic descriptor normalization.
    ///
    /// Normalization may remove the original descriptor, so this creates one
    /// when needed. Consuming `self` prevents one captured value set from being
    /// restored more than once.
    pub(super) fn restore_to(self, descriptor: &mut Option<rm::NodeDescriptor>) {
        for (key, value) in self.0 {
            descriptor
                .get_or_insert_default()
                .attributes
                .insert(format!("{FM_CONFIG_ATTRIBUTE_PREFIX}{key}"), value);
        }
    }

    /// Converts captured attributes to NMX-C entries in key order.
    ///
    /// The returned entries are appended to the selected primary switch's
    /// requested static configuration and pass through normal duplicate-key
    /// validation before any switch mutation.
    pub(super) fn into_static_configs(self) -> Vec<rm::ScaleUpFabricStaticConfig> {
        self.0
            .into_iter()
            .map(|(key, value)| rm::ScaleUpFabricStaticConfig {
                config_file_name: NMX_C_FM_CONFIG_FILE.to_owned(),
                key,
                value,
            })
            .collect()
    }
}
