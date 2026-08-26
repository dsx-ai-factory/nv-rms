/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use librms::protos::rack_manager as pb;
use thiserror::Error;

use crate::api::grpc::conversions::{domain_node_type_to_proto, proto_node_type_to_domain};
use crate::domain::node::{NodeKind, NodeType as DomainNodeType, ProductFamily};

const KEY_ROLE: &str = "role";
const KEY_VENDOR: &str = "vendor";
const KEY_PRODUCT_FAMILY: &str = "product_family";
/// Optional descriptor key selecting a deployment-defined expected inventory.
pub const INVENTORY_PROFILE_ATTRIBUTE: &str = "inventory_profile";
const ROLE_COMPUTE: &str = "compute";
const ROLE_SWITCH: &str = "switch";
const ROLE_POWER_SHELF: &str = "power_shelf";

struct DescriptorRule {
    role: &'static str,
    vendor: &'static str,
    product_family: ProductFamily,
    node_type: DomainNodeType,
}

macro_rules! descriptor_rule {
    ($role:expr, $vendor:literal, $family:ident, $node_type:ident) => {
        DescriptorRule {
            role: $role,
            vendor: $vendor,
            product_family: ProductFamily::$family,
            node_type: DomainNodeType::$node_type,
        }
    };
}

// Rules use canonical outgoing role spellings. Lookup normalizes both sides.
const DESCRIPTOR_RULES: &[DescriptorRule] = &[
    descriptor_rule!(ROLE_COMPUTE, "nvidia", Gb200, ComputeGb200Nvidia),
    descriptor_rule!(ROLE_COMPUTE, "wiwynn", Gb200, ComputeGb200Wiwynn),
    descriptor_rule!(ROLE_COMPUTE, "nvidia", Gb300, ComputeGb300Nvidia),
    descriptor_rule!(ROLE_COMPUTE, "lenovo", Gb300, ComputeGb300Lenovo),
    descriptor_rule!(ROLE_COMPUTE, "supermicro", Gb300, ComputeGb300Supermicro),
    descriptor_rule!(ROLE_COMPUTE, "nvidia", Vrnvl72, ComputeVrnvl72Nvidia),
    descriptor_rule!(ROLE_SWITCH, "nvidia", Gb200, SwitchGb200Nvidia),
    descriptor_rule!(ROLE_SWITCH, "nvidia", Gb300, SwitchGb300Nvidia),
    descriptor_rule!(ROLE_SWITCH, "nvidia", Vrnvl72, SwitchVrnvl72Nvidia),
    descriptor_rule!(ROLE_POWER_SHELF, "liteon", Gb200, PowershelfGb200Liteon),
    descriptor_rule!(ROLE_POWER_SHELF, "delta", Gb200, PowershelfGb200Delta),
    descriptor_rule!(ROLE_POWER_SHELF, "liteon", Gb300, PowershelfGb300Liteon),
    descriptor_rule!(ROLE_POWER_SHELF, "delta", Gb300, PowershelfGb300Delta),
];

/// Error returned when a protobuf node selector cannot resolve to an RMS node type.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum NodeTypeResolutionError {
    /// The explicit protobuf enum value is not recognized by this RMS build.
    #[error("unknown node type value {0}")]
    UnknownNodeType(i32),

    /// A descriptor is required when the explicit node type is absent or unspecified.
    #[error("node descriptor is required when node type is unspecified")]
    MissingDescriptor,

    /// A required descriptor attribute is missing or empty.
    #[error("node descriptor is missing required attribute {0}")]
    MissingAttribute(&'static str),

    /// The descriptor contains an attribute that RMS does not accept.
    #[error("node descriptor attribute {0} is not supported")]
    UnsupportedAttribute(String),

    /// Descriptor attributes are validly shaped but not supported by this RMS build.
    #[error(
        "unsupported node descriptor role={role}, vendor={vendor}, product_family={product_family}"
    )]
    UnsupportedDescriptor {
        /// Descriptor role value after normalization.
        role: String,

        /// Descriptor vendor value after normalization.
        vendor: String,

        /// Descriptor product-family value after normalization.
        product_family: String,
    },

    /// Multiple descriptor entries resolve to the same internal node type.
    #[error("multiple node descriptors resolve to node type {0}")]
    DuplicateDescriptor(&'static str),
}

/// Normalizes one protobuf node selector in place for legacy handler dispatch.
pub fn normalize_node_info(
    node_info: &mut pb::NodeInfo,
) -> Result<DomainNodeType, NodeTypeResolutionError> {
    let inventory_profile = inventory_profile_from_descriptor(node_info.node_descriptor.as_ref());
    let node_type = resolve_node_type(node_info.r#type, node_info.node_descriptor.as_ref())?;
    let proto_type = domain_node_type_to_proto(node_type);
    node_info.r#type = Some(proto_type as i32);
    let mut descriptor = (proto_type == pb::NodeType::Unspecified)
        .then(|| domain_node_type_to_descriptor(node_type));
    if let Some(inventory_profile) = inventory_profile {
        descriptor
            .get_or_insert_default()
            .attributes
            .insert(INVENTORY_PROFILE_ATTRIBUTE.to_owned(), inventory_profile);
    }
    node_info.node_descriptor = descriptor;
    Ok(node_type)
}

/// Return the trimmed optional inventory profile from a node descriptor.
pub fn inventory_profile_from_descriptor(
    descriptor: Option<&pb::NodeDescriptor>,
) -> Option<String> {
    let value =
        descriptor.and_then(|descriptor| descriptor.attributes.get(INVENTORY_PROFILE_ATTRIBUTE))?;
    Some(value.trim().to_owned())
}

/// Resolves a node selector without rewriting its protobuf representation.
pub fn resolve_node_info(
    node_info: &pb::NodeInfo,
) -> Result<DomainNodeType, NodeTypeResolutionError> {
    resolve_node_type(node_info.r#type, node_info.node_descriptor.as_ref())
}

/// Normalizes every node selector in a request node set.
pub fn normalize_node_set(nodes: &mut Option<pb::NodeSet>) -> Result<(), NodeTypeResolutionError> {
    let Some(nodes) = nodes else {
        return Ok(());
    };

    for node in &mut nodes.nodes {
        normalize_node_info(node)?;
    }

    Ok(())
}

/// Normalizes a request-level node type selector in place.
pub fn normalize_node_type_selector(
    node_type: &mut i32,
    descriptor: &mut Option<pb::NodeDescriptor>,
) -> Result<DomainNodeType, NodeTypeResolutionError> {
    let resolved = resolve_node_type(Some(*node_type), descriptor.as_ref())?;
    let proto_type = domain_node_type_to_proto(resolved);
    *node_type = proto_type as i32;
    *descriptor =
        (proto_type == pb::NodeType::Unspecified).then(|| domain_node_type_to_descriptor(resolved));
    Ok(resolved)
}

/// Normalizes descriptor-keyed firmware target entries into enum-keyed entries.
pub fn normalize_firmware_target_selectors(
    firmware_targets: &mut HashMap<i32, pb::FirmwareTargetList>,
    descriptor_firmware_targets: &mut Vec<pb::NodeDescriptorFirmwareTargetList>,
) -> Result<(), NodeTypeResolutionError> {
    for node_type_key in firmware_targets.keys() {
        proto_node_type_to_domain(*node_type_key)
            .ok_or(NodeTypeResolutionError::UnknownNodeType(*node_type_key))?;
    }

    let mut descriptor_keys = HashSet::new();

    let mut retained_descriptor_targets = Vec::new();
    for mut descriptor_targets in std::mem::take(descriptor_firmware_targets) {
        let node_type = resolve_node_type(None, descriptor_targets.node_descriptor.as_ref())?;

        if !descriptor_keys.insert(node_type) {
            return Err(NodeTypeResolutionError::DuplicateDescriptor(
                node_type.as_str(),
            ));
        }

        let proto_type = domain_node_type_to_proto(node_type);
        if proto_type == pb::NodeType::Unspecified {
            descriptor_targets.node_descriptor = Some(domain_node_type_to_descriptor(node_type));
            descriptor_targets.firmware_targets.get_or_insert_default();
            retained_descriptor_targets.push(descriptor_targets);
        } else if let Entry::Vacant(entry) = firmware_targets.entry(proto_type as i32) {
            entry.insert(descriptor_targets.firmware_targets.unwrap_or_default());
        }
    }
    *descriptor_firmware_targets = retained_descriptor_targets;

    Ok(())
}

/// Normalizes descriptor-keyed firmware object filters into enum-keyed entries.
pub fn normalize_component_filter_selectors(
    component_filters: &mut HashMap<i32, pb::FirmwareObjectComponentFilter>,
    descriptor_component_filters: &mut Vec<pb::NodeDescriptorFirmwareObjectComponentFilter>,
) -> Result<(), NodeTypeResolutionError> {
    for node_type_key in component_filters.keys() {
        proto_node_type_to_domain(*node_type_key)
            .ok_or(NodeTypeResolutionError::UnknownNodeType(*node_type_key))?;
    }

    let mut descriptor_keys = HashSet::new();

    let mut retained_descriptor_filters = Vec::new();
    for mut descriptor_filter in std::mem::take(descriptor_component_filters) {
        let node_type = resolve_node_type(None, descriptor_filter.node_descriptor.as_ref())?;

        if !descriptor_keys.insert(node_type) {
            return Err(NodeTypeResolutionError::DuplicateDescriptor(
                node_type.as_str(),
            ));
        }

        let proto_type = domain_node_type_to_proto(node_type);
        if proto_type == pb::NodeType::Unspecified {
            descriptor_filter.node_descriptor = Some(domain_node_type_to_descriptor(node_type));
            descriptor_filter.component_filter.get_or_insert_default();
            retained_descriptor_filters.push(descriptor_filter);
        } else if let Entry::Vacant(entry) = component_filters.entry(proto_type as i32) {
            entry.insert(descriptor_filter.component_filter.unwrap_or_default());
        }
    }
    *descriptor_component_filters = retained_descriptor_filters;

    Ok(())
}

/// Resolves legacy and descriptor-keyed firmware targets to internal node types.
pub fn resolve_firmware_target_selectors<'a>(
    firmware_targets: &'a HashMap<i32, pb::FirmwareTargetList>,
    descriptor_firmware_targets: &'a [pb::NodeDescriptorFirmwareTargetList],
) -> Result<HashMap<DomainNodeType, &'a pb::FirmwareTargetList>, NodeTypeResolutionError> {
    let mut resolved = HashMap::new();
    for (node_type_key, targets) in firmware_targets {
        let node_type = proto_node_type_to_domain(*node_type_key)
            .ok_or(NodeTypeResolutionError::UnknownNodeType(*node_type_key))?;
        resolved.insert(node_type, targets);
    }

    let mut descriptor_types = HashSet::new();
    for descriptor_targets in descriptor_firmware_targets {
        let node_type = resolve_node_type(None, descriptor_targets.node_descriptor.as_ref())?;
        if !descriptor_types.insert(node_type) {
            return Err(NodeTypeResolutionError::DuplicateDescriptor(
                node_type.as_str(),
            ));
        }

        if let Some(targets) = descriptor_targets.firmware_targets.as_ref() {
            resolved.entry(node_type).or_insert(targets);
        }
    }

    Ok(resolved)
}

/// Resolves legacy and descriptor-keyed component filters to internal node types.
pub fn resolve_component_filter_selectors<'a>(
    component_filters: &'a HashMap<i32, pb::FirmwareObjectComponentFilter>,
    descriptor_component_filters: &'a [pb::NodeDescriptorFirmwareObjectComponentFilter],
) -> Result<HashMap<DomainNodeType, &'a pb::FirmwareObjectComponentFilter>, NodeTypeResolutionError>
{
    let mut resolved = HashMap::new();
    for (node_type_key, filter) in component_filters {
        let node_type = proto_node_type_to_domain(*node_type_key)
            .ok_or(NodeTypeResolutionError::UnknownNodeType(*node_type_key))?;
        resolved.insert(node_type, filter);
    }

    let mut descriptor_types = HashSet::new();
    for descriptor_filter in descriptor_component_filters {
        let node_type = resolve_node_type(None, descriptor_filter.node_descriptor.as_ref())?;
        if !descriptor_types.insert(node_type) {
            return Err(NodeTypeResolutionError::DuplicateDescriptor(
                node_type.as_str(),
            ));
        }

        if let Some(filter) = descriptor_filter.component_filter.as_ref() {
            resolved.entry(node_type).or_insert(filter);
        }
    }

    Ok(resolved)
}

/// Builds the canonical descriptor RMS returns for a supported internal node type.
pub fn domain_node_type_to_descriptor(node_type: DomainNodeType) -> pb::NodeDescriptor {
    let role = match node_type.kind() {
        NodeKind::Compute => ROLE_COMPUTE,
        NodeKind::Powershelf => ROLE_POWER_SHELF,
        NodeKind::Switch => ROLE_SWITCH,
    };

    pb::NodeDescriptor {
        attributes: HashMap::from([
            (KEY_ROLE.to_string(), role.to_string()),
            (
                KEY_VENDOR.to_string(),
                descriptor_vendor(node_type).to_string(),
            ),
            (
                KEY_PRODUCT_FAMILY.to_string(),
                product_family_name(node_type.product_family()).to_string(),
            ),
        ]),
    }
}

pub fn resolve_node_type(
    node_type: Option<i32>,
    descriptor: Option<&pb::NodeDescriptor>,
) -> Result<DomainNodeType, NodeTypeResolutionError> {
    if let Some(raw_type) = node_type {
        let proto_type = pb::NodeType::try_from(raw_type)
            .map_err(|_| NodeTypeResolutionError::UnknownNodeType(raw_type))?;

        if proto_type != pb::NodeType::Unspecified {
            return proto_node_type_to_domain(raw_type)
                .ok_or(NodeTypeResolutionError::UnknownNodeType(raw_type));
        }
    }

    resolve_node_descriptor(descriptor)
}

fn resolve_node_descriptor(
    descriptor: Option<&pb::NodeDescriptor>,
) -> Result<DomainNodeType, NodeTypeResolutionError> {
    let descriptor = descriptor.ok_or(NodeTypeResolutionError::MissingDescriptor)?;
    validate_descriptor_attributes(descriptor)?;

    let normalized_role =
        normalize_descriptor_value(required_descriptor_value(descriptor, KEY_ROLE)?);

    let normalized_vendor =
        normalize_descriptor_value(required_descriptor_value(descriptor, KEY_VENDOR)?);

    let product_family_value = required_descriptor_value(descriptor, KEY_PRODUCT_FAMILY)?;

    let product_family = parse_product_family(product_family_value).ok_or_else(|| {
        NodeTypeResolutionError::UnsupportedDescriptor {
            role: normalized_role.clone(),
            vendor: normalized_vendor.clone(),
            product_family: normalize_descriptor_value(product_family_value),
        }
    })?;

    DESCRIPTOR_RULES
        .iter()
        .find(|rule| {
            normalized_role == normalize_descriptor_value(rule.role)
                && product_family == rule.product_family
                && normalized_vendor == normalize_descriptor_value(rule.vendor)
        })
        .map(|rule| rule.node_type)
        .ok_or_else(|| NodeTypeResolutionError::UnsupportedDescriptor {
            role: normalized_role,
            vendor: normalized_vendor,
            product_family: product_family_name(product_family).to_string(),
        })
}

fn validate_descriptor_attributes(
    descriptor: &pb::NodeDescriptor,
) -> Result<(), NodeTypeResolutionError> {
    if let Some(key) = descriptor
        .attributes
        .keys()
        .filter(|key| !is_supported_descriptor_attribute(key))
        .min()
    {
        return Err(NodeTypeResolutionError::UnsupportedAttribute(key.clone()));
    }

    Ok(())
}

fn is_supported_descriptor_attribute(key: &str) -> bool {
    matches!(
        key,
        KEY_ROLE | KEY_VENDOR | KEY_PRODUCT_FAMILY | INVENTORY_PROFILE_ATTRIBUTE
    )
}

fn required_descriptor_value<'a>(
    descriptor: &'a pb::NodeDescriptor,
    key: &'static str,
) -> Result<&'a str, NodeTypeResolutionError> {
    descriptor
        .attributes
        .get(key)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(NodeTypeResolutionError::MissingAttribute(key))
}

fn parse_product_family(value: &str) -> Option<ProductFamily> {
    match normalize_descriptor_value(value).as_str() {
        "gb200" => Some(ProductFamily::Gb200),
        "gb300" => Some(ProductFamily::Gb300),
        "vrnvl72" => Some(ProductFamily::Vrnvl72),
        _ => None,
    }
}

fn product_family_name(product_family: ProductFamily) -> &'static str {
    match product_family {
        ProductFamily::Gb200 => "gb200",
        ProductFamily::Gb300 => "gb300",
        ProductFamily::Vrnvl72 => "vrnvl72",
    }
}

fn descriptor_vendor(node_type: DomainNodeType) -> &'static str {
    match node_type {
        DomainNodeType::ComputeGb200Nvidia
        | DomainNodeType::ComputeGb300Nvidia
        | DomainNodeType::SwitchGb200Nvidia
        | DomainNodeType::SwitchGb300Nvidia
        | DomainNodeType::ComputeVrnvl72Nvidia
        | DomainNodeType::SwitchVrnvl72Nvidia => "nvidia",
        DomainNodeType::PowershelfGb200Liteon | DomainNodeType::PowershelfGb300Liteon => "liteon",
        DomainNodeType::PowershelfGb200Delta | DomainNodeType::PowershelfGb300Delta => "delta",
        DomainNodeType::ComputeGb300Lenovo => "lenovo",
        DomainNodeType::ComputeGb300Supermicro => "supermicro",
        DomainNodeType::ComputeGb200Wiwynn => "wiwynn",
    }
}

fn normalize_descriptor_value(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '_'], "")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(role: &str, vendor: &str, product_family: &str) -> pb::NodeDescriptor {
        pb::NodeDescriptor {
            attributes: HashMap::from([
                (KEY_ROLE.to_string(), role.to_string()),
                (KEY_VENDOR.to_string(), vendor.to_string()),
                (KEY_PRODUCT_FAMILY.to_string(), product_family.to_string()),
            ]),
        }
    }

    #[test]
    fn node_descriptor_resolves_current_node_type_matrix() {
        let cases = [
            (
                descriptor("compute", "NVIDIA", "gb200"),
                DomainNodeType::ComputeGb200Nvidia,
            ),
            (
                descriptor("compute", "Wiwynn", "gb200"),
                DomainNodeType::ComputeGb200Wiwynn,
            ),
            (
                descriptor("compute", "NVIDIA", "gb300"),
                DomainNodeType::ComputeGb300Nvidia,
            ),
            (
                descriptor("compute", "Lenovo", "gb300"),
                DomainNodeType::ComputeGb300Lenovo,
            ),
            (
                descriptor("compute", "Supermicro", "gb300"),
                DomainNodeType::ComputeGb300Supermicro,
            ),
            (
                descriptor("compute", "NVIDIA", "vr_nvl72"),
                DomainNodeType::ComputeVrnvl72Nvidia,
            ),
            (
                descriptor("switch", "NVIDIA", "gb200"),
                DomainNodeType::SwitchGb200Nvidia,
            ),
            (
                descriptor("switch", "NVIDIA", "gb300"),
                DomainNodeType::SwitchGb300Nvidia,
            ),
            (
                descriptor("switch", "NVIDIA", "vrnvl72"),
                DomainNodeType::SwitchVrnvl72Nvidia,
            ),
            (
                descriptor("power_shelf", "Lite-On", "gb200"),
                DomainNodeType::PowershelfGb200Liteon,
            ),
            (
                descriptor("powershelf", "Delta", "gb200"),
                DomainNodeType::PowershelfGb200Delta,
            ),
            (
                descriptor("power_shelf", "LiteOn", "gb300"),
                DomainNodeType::PowershelfGb300Liteon,
            ),
            (
                descriptor("power_shelf", "Delta", "gb300"),
                DomainNodeType::PowershelfGb300Delta,
            ),
        ];

        for (descriptor, expected) in cases {
            let mut node = pb::NodeInfo {
                r#type: Some(pb::NodeType::Unspecified as i32),
                node_descriptor: Some(descriptor),
                ..Default::default()
            };

            assert_eq!(normalize_node_info(&mut node), Ok(expected));

            assert_eq!(
                node.r#type,
                Some(domain_node_type_to_proto(expected) as i32)
            );

            if domain_node_type_to_proto(expected) == pb::NodeType::Unspecified {
                assert_eq!(
                    node.node_descriptor,
                    Some(domain_node_type_to_descriptor(expected))
                );
            } else {
                assert!(node.node_descriptor.is_none());
            }

            let canonical = domain_node_type_to_descriptor(expected);
            assert_eq!(resolve_node_type(None, Some(&canonical)), Ok(expected));
        }
    }

    #[test]
    fn node_type_selector_resolves_descriptor_and_honors_enum_override() {
        let mut node_type = pb::NodeType::Unspecified as i32;
        let mut node_descriptor = Some(descriptor("compute", "NVIDIA", "gb200"));

        assert_eq!(
            normalize_node_type_selector(&mut node_type, &mut node_descriptor),
            Ok(DomainNodeType::ComputeGb200Nvidia)
        );

        let mut node_type = pb::NodeType::SwitchGb200Nvidia as i32;
        let mut node_descriptor = Some(descriptor("compute", "Lenovo", "gb300"));

        assert_eq!(
            normalize_node_type_selector(&mut node_type, &mut node_descriptor),
            Ok(DomainNodeType::SwitchGb200Nvidia)
        );

        assert_eq!(node_type, pb::NodeType::SwitchGb200Nvidia as i32);
        assert!(node_descriptor.is_none());
    }

    #[test]
    fn normalization_preserves_trimmed_inventory_profile() {
        let mut descriptor_only = pb::NodeInfo {
            r#type: Some(pb::NodeType::Unspecified as i32),
            node_descriptor: Some(descriptor("compute", "NVIDIA", "gb200")),
            ..Default::default()
        };
        descriptor_only
            .node_descriptor
            .as_mut()
            .unwrap()
            .attributes
            .insert(
                INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                " profile-a ".to_owned(),
            );

        assert_eq!(
            normalize_node_info(&mut descriptor_only),
            Ok(DomainNodeType::ComputeGb200Nvidia)
        );
        assert_eq!(
            descriptor_only
                .node_descriptor
                .as_ref()
                .unwrap()
                .attributes
                .get(INVENTORY_PROFILE_ATTRIBUTE)
                .map(String::as_str),
            Some("profile-a")
        );

        let mut explicit = pb::NodeInfo {
            r#type: Some(pb::NodeType::SwitchGb200Nvidia as i32),
            node_descriptor: Some(pb::NodeDescriptor {
                attributes: HashMap::from([(
                    INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                    "switch-profile".to_owned(),
                )]),
            }),
            ..Default::default()
        };

        assert_eq!(
            normalize_node_info(&mut explicit),
            Ok(DomainNodeType::SwitchGb200Nvidia)
        );
        assert_eq!(
            explicit.node_descriptor.unwrap().attributes,
            HashMap::from([(
                INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                "switch-profile".to_owned()
            )])
        );
    }

    #[test]
    fn normalization_preserves_empty_inventory_profile_for_per_node_validation() {
        let mut node = pb::NodeInfo {
            r#type: Some(pb::NodeType::ComputeGb200Nvidia as i32),
            node_descriptor: Some(pb::NodeDescriptor {
                attributes: HashMap::from([(
                    INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                    "  ".to_owned(),
                )]),
            }),
            ..Default::default()
        };

        assert_eq!(
            normalize_node_info(&mut node),
            Ok(DomainNodeType::ComputeGb200Nvidia)
        );
        assert_eq!(
            node.node_descriptor
                .unwrap()
                .attributes
                .get(INVENTORY_PROFILE_ATTRIBUTE),
            Some(&String::new())
        );
    }

    #[test]
    fn descriptor_rejects_missing_attributes_unknown_keys_and_unsupported_vendors() {
        let mut missing = descriptor("compute", "NVIDIA", "");
        missing.attributes.remove(KEY_PRODUCT_FAMILY);
        let mut unsupported = descriptor("compute", "NVIDIA", "gb200");

        unsupported.attributes.insert(
            "rack_hardware_type".to_string(),
            "dsx_gb200nvl_72x1".to_string(),
        );

        assert_eq!(
            resolve_node_type(None, Some(&missing)),
            Err(NodeTypeResolutionError::MissingAttribute(
                KEY_PRODUCT_FAMILY
            ))
        );

        assert_eq!(
            resolve_node_type(None, Some(&unsupported)),
            Err(NodeTypeResolutionError::UnsupportedAttribute(
                "rack_hardware_type".to_string()
            ))
        );

        assert_eq!(
            resolve_node_type(Some(pb::NodeType::Unspecified as i32), None),
            Err(NodeTypeResolutionError::MissingDescriptor)
        );

        for (role, vendor, product_family) in [
            ("compute", "nvidiacorp", "gb200"),
            ("power_shelf", "deltafoo", "gb200"),
        ] {
            let result = resolve_node_type(None, Some(&descriptor(role, vendor, product_family)));

            assert_eq!(
                result,
                Err(NodeTypeResolutionError::UnsupportedDescriptor {
                    role: normalize_descriptor_value(role),
                    vendor: normalize_descriptor_value(vendor),
                    product_family: product_family.to_owned(),
                }),
                "vendor {vendor}"
            );
        }
    }

    #[test]
    fn selector_maps_reject_unknown_legacy_node_type() {
        let unknown_node_type = 999;
        let expected = Err(NodeTypeResolutionError::UnknownNodeType(unknown_node_type));

        let mut firmware_targets =
            HashMap::from([(unknown_node_type, pb::FirmwareTargetList::default())]);

        assert_eq!(
            normalize_firmware_target_selectors(&mut firmware_targets, &mut Vec::new()),
            expected
        );

        let mut component_filters = HashMap::from([(
            unknown_node_type,
            pb::FirmwareObjectComponentFilter::default(),
        )]);

        assert_eq!(
            normalize_component_filter_selectors(&mut component_filters, &mut Vec::new()),
            expected
        );
    }

    #[test]
    fn descriptor_selector_maps_reject_duplicate_resolved_node_type() {
        let mut descriptor_targets = vec![
            pb::NodeDescriptorFirmwareTargetList {
                node_descriptor: Some(descriptor("compute", "NVIDIA", "gb200")),
                firmware_targets: Some(pb::FirmwareTargetList::default()),
            },
            pb::NodeDescriptorFirmwareTargetList {
                node_descriptor: Some(descriptor("compute", "n-vidia", "gb200")),
                firmware_targets: Some(pb::FirmwareTargetList::default()),
            },
        ];

        let result =
            normalize_firmware_target_selectors(&mut HashMap::new(), &mut descriptor_targets);

        assert_eq!(
            result,
            Err(NodeTypeResolutionError::DuplicateDescriptor(
                DomainNodeType::ComputeGb200Nvidia.as_str()
            ))
        );

        let mut descriptor_filters = vec![
            pb::NodeDescriptorFirmwareObjectComponentFilter {
                node_descriptor: Some(descriptor("switch", "NVIDIA", "gb300")),
                component_filter: Some(pb::FirmwareObjectComponentFilter::default()),
            },
            pb::NodeDescriptorFirmwareObjectComponentFilter {
                node_descriptor: Some(descriptor("switch", "nvidia", "gb-300")),
                component_filter: Some(pb::FirmwareObjectComponentFilter::default()),
            },
        ];

        let result =
            normalize_component_filter_selectors(&mut HashMap::new(), &mut descriptor_filters);

        assert_eq!(
            result,
            Err(NodeTypeResolutionError::DuplicateDescriptor(
                DomainNodeType::SwitchGb300Nvidia.as_str()
            ))
        );
    }

    #[test]
    fn descriptor_component_filters_normalize_with_legacy_precedence() {
        let legacy_key = pb::NodeType::ComputeGb200Nvidia as i32;

        let legacy_filter = pb::FirmwareObjectComponentFilter {
            components: vec!["BMC".to_owned()],
        };

        let descriptor_filter = pb::FirmwareObjectComponentFilter {
            components: vec!["HMC".to_owned()],
        };

        let mut component_filters = HashMap::from([(legacy_key, legacy_filter.clone())]);

        let mut descriptor_filters = vec![
            pb::NodeDescriptorFirmwareObjectComponentFilter {
                node_descriptor: Some(descriptor("compute", "NVIDIA", "gb200")),
                component_filter: Some(descriptor_filter.clone()),
            },
            pb::NodeDescriptorFirmwareObjectComponentFilter {
                node_descriptor: Some(descriptor("switch", "NVIDIA", "gb200")),
                component_filter: Some(descriptor_filter),
            },
        ];

        normalize_component_filter_selectors(&mut component_filters, &mut descriptor_filters)
            .unwrap();

        assert!(descriptor_filters.is_empty());
        assert_eq!(component_filters.get(&legacy_key), Some(&legacy_filter));

        assert_eq!(
            component_filters
                .get(&(pb::NodeType::SwitchGb200Nvidia as i32))
                .map(|filter| filter.components.as_slice()),
            Some(&["HMC".to_owned()][..])
        );
    }

    #[test]
    fn descriptor_only_wiwynn_selectors_remain_descriptor_keyed() {
        let mut firmware_targets = HashMap::new();
        let mut descriptor_targets = vec![pb::NodeDescriptorFirmwareTargetList {
            node_descriptor: Some(descriptor("compute", "Wiwynn", "gb200")),
            firmware_targets: Some(pb::FirmwareTargetList::default()),
        }];

        normalize_firmware_target_selectors(&mut firmware_targets, &mut descriptor_targets)
            .unwrap();

        assert!(firmware_targets.is_empty());
        assert_eq!(descriptor_targets.len(), 1);
        assert!(
            resolve_firmware_target_selectors(&firmware_targets, &descriptor_targets)
                .unwrap()
                .contains_key(&DomainNodeType::ComputeGb200Wiwynn)
        );

        let mut component_filters = HashMap::new();
        let mut descriptor_filters = vec![pb::NodeDescriptorFirmwareObjectComponentFilter {
            node_descriptor: Some(descriptor("compute", "Wiwynn", "gb200")),
            component_filter: Some(pb::FirmwareObjectComponentFilter::default()),
        }];

        normalize_component_filter_selectors(&mut component_filters, &mut descriptor_filters)
            .unwrap();

        assert!(component_filters.is_empty());
        assert_eq!(descriptor_filters.len(), 1);
        assert!(
            resolve_component_filter_selectors(&component_filters, &descriptor_filters)
                .unwrap()
                .contains_key(&DomainNodeType::ComputeGb200Wiwynn)
        );
    }
}
