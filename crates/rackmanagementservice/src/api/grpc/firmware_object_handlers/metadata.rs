// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Public metadata projection; platform identities stay separate all the way to
//! the wire. Missing URLs do not erase the component's inventory metadata.

use std::collections::{BTreeMap, BTreeSet};

use super::*;

#[cfg(test)]
pub(super) fn from_manifest(
    manifest: &FirmwareManifest,
) -> Result<rm::FirmwareObjectMetadata, String> {
    from_manifest_with_profiles(manifest, None)
}

pub(super) fn from_manifest_with_profiles(
    manifest: &FirmwareManifest,
    profiles: Option<&crate::config::hardware_profiles::HardwareProfileCatalog>,
) -> Result<rm::FirmwareObjectMetadata, String> {
    let boards = &manifest
        .single_milestone()
        .map_err(|e| e.to_string())?
        .board_skus;
    let channels: BTreeSet<_> = boards
        .iter()
        .flat_map(|board| &board.components.firmware)
        .filter_map(|component| plan::channel(manifest, component))
        .map(|channel| channel.trim().to_ascii_lowercase())
        .collect();
    let mut device_components = Vec::new();
    for (node_type, role) in firmware_node_type_lookup_keys() {
        let mut components: BTreeMap<String, rm::FirmwareObjectComponent> = BTreeMap::new();
        for channel in &channels {
            let selected = match plan::select_components_with_profile(
                manifest,
                node_type,
                channel,
                &FirmwareObjectComponentSelection::all(),
                profiles.and_then(|catalog| catalog.resolve_request(node_type)),
            ) {
                Ok(selected) => selected,
                Err(plan::PlanError::NoComponents(_)) => continue,
                Err(error) => return Err(error.to_string()),
            };
            for selected in selected {
                let source = selected.component;
                let component = components
                    .entry(selected.mapping.target_key.to_owned())
                    .or_insert_with(|| rm::FirmwareObjectComponent {
                        name: selected.mapping.target_key.to_owned(),
                        version: source.version.clone().unwrap_or_default(),
                        bundle: source.bundle.clone().unwrap_or_default(),
                        source_component: source.component.clone(),
                        subcomponents: source
                            .subcomponents
                            .iter()
                            .map(|sub| rm::FirmwareObjectSubcomponent {
                                name: sub.component.clone(),
                                version: sub.version.clone(),
                                sku_id: sub.sku_id.clone().unwrap_or_default(),
                            })
                            .collect(),
                        artifacts: Vec::new(),
                    });
                component.artifacts.extend(
                    source
                        .locations
                        .iter()
                        .filter(|location| {
                            plan::payload_location(manifest, source, location)
                                && plan::filename_allowed_with_profile(
                                    node_type,
                                    &selected.mapping,
                                    location,
                                    selected.configured,
                                )
                        })
                        .filter_map(|location| {
                            artifacts::advertised_filename(location).map(|filename| {
                                rm::FirmwareObjectArtifact {
                                    firmware_type: channel.clone(),
                                    filename,
                                    target: selected.mapping.redfish_target.to_owned(),
                                    bundle: source.bundle.clone().unwrap_or_default(),
                                }
                            })
                        }),
                );
            }
        }
        if !components.is_empty() {
            device_components.push(rm::FirmwareObjectDeviceComponents {
                node_type: domain_node_type_to_proto(node_type) as i32,
                node_descriptor: Some(domain_node_type_to_descriptor(node_type)),
                device_type: role.to_owned(),
                components: components.into_values().collect(),
            });
        }
    }
    let mut switch_system_images = Vec::new();
    for board in boards
        .iter()
        .filter(|board| lookup_key_from_board_type(&board.board_type) == Some(SWITCH_LOOKUP_KEY))
    {
        for component in board
            .components
            .software
            .iter()
            .filter(|component| component.component.eq_ignore_ascii_case("NVOS"))
        {
            // Describe advertised images, even if download/selection cannot yet
            // succeed. Applying an optional image requires its bytes, but must
            // not rewrite the manifest's prefetch policy in public metadata.
            for location in &component.locations {
                let Some(image_filename) = artifacts::advertised_filename(location)
                    .filter(|filename| filename.to_ascii_lowercase().ends_with(".bin"))
                else {
                    continue;
                };
                switch_system_images.push(rm::FirmwareObjectSwitchSystemImage {
                    software_type: component
                        .channel
                        .as_deref()
                        .unwrap_or_default()
                        .trim()
                        .to_ascii_lowercase(),
                    version: component.version.clone().unwrap_or_default(),
                    package_name: nonempty(location.package_name.as_deref())
                        .unwrap_or(&manifest.product_name)
                        .to_owned(),
                    image_filename,
                    required: location.required.unwrap_or(true),
                });
            }
        }
    }
    Ok(rm::FirmwareObjectMetadata {
        device_components,
        switch_system_images,
    })
}
