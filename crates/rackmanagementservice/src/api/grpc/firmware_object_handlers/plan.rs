// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Resolve a manifest for ONE concrete platform. No role-keyed target tables,
//! persistence objects, protobuf messages, or I/O belong in this layer.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

use super::artifacts::{Artifact, advertised_filename, has_download_location, manifest_revision};
use super::profile;
use super::{
    COMPUTE_LOOKUP_KEY, FirmwareManifest, FirmwareManifestExpectation,
    FirmwareObjectComponentSelection, FirmwareTargetMapping, NodeKind, NodeType,
    POWER_SHELF_LOOKUP_KEY, RECOVERY_BUNDLE_COMPONENT, SWITCH_LOOKUP_KEY,
    downloadable_nvos_location, firmware_component_allowed_for_node_type,
    firmware_payload_allowed_for_component, firmware_source_component_matches,
    firmware_versions_match, location_is_firmware_payload, location_is_recovery_payload,
    lookup_key_from_board_type, manifest, manifest_component_matches_target, nonempty,
    normalized_firmware_identity, select_nvos_location, selected_firmware_mapping_matches,
    supermicro_bmc_filename,
};
use crate::api::grpc::artifact_download::ArtifactError;
use crate::config::hardware_profiles::{FirmwareTargetPlan, HardwareUpdatePlan};
use crate::domain::node::{FirmwareTarget, RecoveryBundles};

/// Selection failures distinguish an absent platform from invalid/ambiguous
/// metadata, and cache resolution from malformed input.
#[derive(Debug, thiserror::Error)]
pub(super) enum PlanError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    NoComponents(String),
    #[error("{0}")]
    Cache(String),
}

impl From<String> for PlanError {
    fn from(message: String) -> Self {
        Self::Invalid(message)
    }
}

impl From<PlanError> for ArtifactError {
    fn from(error: PlanError) -> Self {
        match error {
            PlanError::Invalid(message) | PlanError::NoComponents(message) => {
                Self::InvalidArgument(message)
            }
            PlanError::Cache(message) => Self::Cache(message),
        }
    }
}

pub(super) struct SelectedComponent<'a> {
    board: usize,
    index: usize,
    pub(super) component: &'a manifest::Component,
    pub(super) mapping: FirmwareTargetMapping<'a>,
    pub(super) configured: Option<&'a FirmwareTargetPlan>,
    order: usize,
}

pub(super) struct FirmwarePlan {
    pub(super) node_type: NodeType,
    pub(super) targets: Vec<PlannedTarget>,
}

pub(super) struct PlannedTarget {
    firmware: FirmwareTarget,
    pub(super) artifact: Artifact,
    order: usize,
}

#[cfg(test)]
impl PlannedTarget {
    fn transport_target(&self) -> &str {
        crate::nodes::nvfwupd_adapter::firmware_target_selector(&self.firmware)
    }
}

fn board_matches(board_type: &str, node_type: NodeType) -> bool {
    // Preserve release catalogs that place vendor-specific powershelf payloads
    // under a compute board. Component mappings below still enforce node/vendor
    // eligibility; this does not allow flashing compute firmware on powershelves.
    matches!(
        (lookup_key_from_board_type(board_type), node_type.kind()),
        (
            Some(COMPUTE_LOOKUP_KEY),
            NodeKind::Compute | NodeKind::Powershelf
        ) | (Some(SWITCH_LOOKUP_KEY), NodeKind::Switch)
            | (Some(POWER_SHELF_LOOKUP_KEY), NodeKind::Powershelf)
    )
}

pub(super) fn channel<'a>(
    manifest: &FirmwareManifest,
    component: &'a manifest::Component,
) -> Option<&'a str> {
    nonempty(component.channel.as_deref()).or_else(|| {
        (manifest.product_name.eq_ignore_ascii_case("VR-NVL72")
            && matches!(
                component.component.as_str(),
                "HMC" | "BMC" | "BMC+CPLD+SMA+ERoT+SBIOS"
            ))
        .then_some("Prod")
    })
}

/// An empty request selects the manifest's sole component channel, not a
/// hard-coded production default. Location.Type is an artifact kind and is
/// deliberately excluded. Resolve across the whole release so different nodes
/// in one request cannot silently select different channels.
pub(super) fn resolve_channel(
    manifest: &FirmwareManifest,
    requested: &str,
) -> Result<String, PlanError> {
    let milestone = manifest.single_milestone().map_err(|e| e.to_string())?;
    resolve_channel_from_values(
        requested,
        milestone
            .board_skus
            .iter()
            .flat_map(|board| {
                board
                    .components
                    .firmware
                    .iter()
                    .chain(&board.components.software)
            })
            .filter_map(|component| channel(manifest, component)),
    )
}

fn resolve_channel_from_values<'a>(
    requested: &str,
    channels: impl IntoIterator<Item = &'a str>,
) -> Result<String, PlanError> {
    if !requested.trim().is_empty() {
        return Ok(requested.trim().to_ascii_lowercase());
    }
    let channels: BTreeSet<_> = channels
        .into_iter()
        .map(str::trim)
        .filter(|channel| !channel.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    if channels.len() > 1 {
        return Err(PlanError::Invalid(format!(
            "manifest contains multiple firmware types: {}; specify the requested type explicitly",
            channels.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }
    channels.into_iter().next().ok_or_else(|| {
        PlanError::Invalid(
            "cannot infer firmware type: manifest contains no component channels".to_owned(),
        )
    })
}

/// Metadata selection does not require downloads, but source precedence checks
/// payload eligibility. Multiple board candidates cannot be resolved by today's
/// API, so reject rather than overwrite.
/// Callers resolve the request channel once with `resolve_channel` before
/// selecting, planning firmware, projecting expectations, or selecting NVOS.
#[cfg(test)]
pub(super) fn select_components<'a>(
    manifest: &'a FirmwareManifest,
    node_type: NodeType,
    firmware_type: &str,
    selection: &FirmwareObjectComponentSelection,
) -> Result<Vec<SelectedComponent<'a>>, PlanError> {
    select_components_with_profile(manifest, node_type, firmware_type, selection, None)
}

pub(super) fn select_components_with_profile<'a>(
    manifest: &'a FirmwareManifest,
    node_type: NodeType,
    firmware_type: &str,
    selection: &FirmwareObjectComponentSelection,
    hardware_profile: Option<&'a HardwareUpdatePlan>,
) -> Result<Vec<SelectedComponent<'a>>, PlanError> {
    let mappings = profile::mappings(node_type, hardware_profile);
    let boards = &manifest
        .single_milestone()
        .map_err(|e| e.to_string())?
        .board_skus;
    // Split GB200/GB300 switch bundles provide EROT twice. Prefer the BIOS
    // bundle by manifest identity, not entry order, filename, or version. Check
    // the whole selected channel before filtering targets so EROT-only requests
    // download only the preferred bundle; metadata and comparisons agree.
    let split_switch = matches!(
        node_type,
        NodeType::SwitchGb200Nvidia | NodeType::SwitchGb300Nvidia
    );
    let prefer_switch_bios_erot = split_switch
        && boards
            .iter()
            .filter(|board| board_matches(&board.board_type, node_type))
            .flat_map(|board| &board.components.firmware)
            .any(|component| {
                component.component == "SBIOS+EROT"
                    && channel(manifest, component).is_some_and(|channel| {
                        channel.trim().eq_ignore_ascii_case(firmware_type.trim())
                    })
                    && component.locations.iter().any(|location| {
                        payload_location(manifest, component, location)
                            && has_download_location(location)
                            && advertised_filename(location).is_some()
                    })
            });
    let mut selected = Vec::new();
    let mut target_boards = HashMap::new();
    for (board_idx, board) in boards.iter().enumerate() {
        if !board_matches(&board.board_type, node_type) {
            continue;
        }
        for (index, component) in board.components.firmware.iter().enumerate() {
            for selected_mapping in &mappings {
                let mapping = &selected_mapping.mapping;
                if !firmware_source_component_matches(
                    &component.component,
                    mapping.source_component,
                ) || (hardware_profile.is_none()
                    && !firmware_component_allowed_for_node_type(node_type, mapping.target_key))
                    || !selected_mapping.configured.map_or_else(
                        || selection.matches_target_mapping(mapping),
                        |target| selection.matches_profile_target(target),
                    )
                {
                    continue;
                }
                // Check applicable components before channel filtering. Otherwise
                // a missing Type silently turns a complete request into a partial update.
                let channel = channel(manifest, component).ok_or_else(|| format!(
                    "Milestones[0].BoardSKUs[{board_idx}].Components.Firmware[{index}].Type is required for {} component {}",
                    node_type.as_str(), component.component,
                ))?;
                if !channel.trim().eq_ignore_ascii_case(firmware_type.trim()) {
                    continue;
                }
                if split_switch
                    && mapping.target_key == "EROT"
                    && ((prefer_switch_bios_erot && mapping.source_component == "BMC+FPGA+EROT")
                        || (!prefer_switch_bios_erot && mapping.source_component == "SBIOS+EROT"))
                {
                    continue;
                }
                if let Some(previous) = target_boards.insert(mapping.target_key, board_idx)
                    && previous != board_idx
                {
                    return Err(PlanError::Invalid(format!(
                        "ambiguous {} for {}: BoardSKUs[{previous}] and BoardSKUs[{board_idx}]; supply a manifest with one matching board",
                        mapping.target_key,
                        node_type.as_str()
                    )));
                }
                selected.push(SelectedComponent {
                    board: board_idx,
                    index,
                    component,
                    mapping: *mapping,
                    configured: selected_mapping.configured,
                    order: selected_mapping.order,
                });
            }
        }
    }
    for requested in &selection.components {
        if !selected.iter().any(|selected| {
            selected.configured.map_or_else(
                || {
                    selected_firmware_mapping_matches(
                        selected.mapping.source_component,
                        selected.mapping.target_key,
                        selected.mapping.redfish_target,
                        selected.mapping.default_apply,
                        std::slice::from_ref(requested),
                    )
                },
                |target| target.matches_component(requested),
            )
        }) {
            return Err(PlanError::Invalid(format!(
                "component {requested} has no matching manifest entry for {} ({firmware_type})",
                node_type.as_str()
            )));
        }
    }
    if selected.is_empty() {
        return Err(PlanError::NoComponents(format!(
            "no firmware components found for {} ({firmware_type})",
            node_type.as_str()
        )));
    }
    Ok(selected)
}

/// A recovery package. Routed before the payload filter so a recovery image
/// whose `Type` lies can never enter a flash plan.
pub(super) fn recovery_payload_location(location: &manifest::Location) -> bool {
    advertised_filename(location)
        .is_some_and(|name| location_is_recovery_payload(location.artifact_type.as_deref(), &name))
}

pub(super) fn payload_location(
    manifest: &FirmwareManifest,
    component: &manifest::Component,
    location: &manifest::Location,
) -> bool {
    if recovery_payload_location(location) {
        return false;
    }
    let inferred_vr_bmc = manifest.product_name.eq_ignore_ascii_case("VR-NVL72")
        && component.component.eq_ignore_ascii_case("BMC")
        && nonempty(location.artifact_type.as_deref()).is_none()
        && advertised_filename(location)
            .is_some_and(|name| name.to_ascii_lowercase().ends_with(".fwpkg"));
    (location_is_firmware_payload(location.artifact_type.as_deref()) || inferred_vr_bmc)
        && advertised_filename(location)
            .is_none_or(|name| firmware_payload_allowed_for_component(&component.component, &name))
}

pub(super) fn filename_allowed(
    node_type: NodeType,
    mapping: &FirmwareTargetMapping,
    location: &manifest::Location,
) -> bool {
    if node_type != NodeType::ComputeGb300Supermicro {
        return true;
    }
    let Some(filename) = advertised_filename(location) else {
        return false;
    };
    let filename = filename.to_ascii_lowercase();
    match mapping.target_key {
        "HMC" => filename.ends_with(".fwpkg") && filename.contains("nosbios"),
        "BIOS" => filename.ends_with(".bin") && filename.contains("bios"),
        "BMC" => supermicro_bmc_filename(&filename),
        _ => false,
    }
}

pub(super) fn filename_allowed_with_profile(
    node_type: NodeType,
    mapping: &FirmwareTargetMapping<'_>,
    location: &manifest::Location,
    configured: Option<&FirmwareTargetPlan>,
) -> bool {
    if let Some(target) = configured {
        return advertised_filename(location)
            .is_some_and(|filename| target.filename().matches(&filename));
    }
    filename_allowed(node_type, mapping, location)
}

fn location_context(board: usize, collection: &str, component: usize, location: usize) -> String {
    format!(
        "Milestones[0].BoardSKUs[{board}].Components.{collection}[{component}].Locations[{location}]"
    )
}

fn linkx_policy(
    version: Option<&str>,
    rollout: Option<&serde_json::Value>,
) -> Result<nvfwupd::flint::LinkxPolicy, String> {
    let policy: nvfwupd::flint::LinkxPolicy = serde_json::from_value(
        rollout
            .cloned()
            .ok_or("LINKX requires a LinkxRollout policy in the firmware manifest")?,
    )
    .map_err(|e| format!("invalid LinkxRollout policy: {e}"))?;
    policy.validate().map_err(|e| e.to_string())?;
    if version != Some(policy.target_version.as_str()) {
        return Err("LINKX Version must equal LinkxRollout.target_version".to_owned());
    }
    Ok(policy)
}

#[cfg(test)]
pub(super) fn firmware_plan(
    manifest: &FirmwareManifest,
    node_type: NodeType,
    firmware_type: &str,
    selection: &FirmwareObjectComponentSelection,
) -> Result<FirmwarePlan, PlanError> {
    firmware_plan_with_profile(manifest, node_type, firmware_type, selection, None)
}

pub(super) fn firmware_plan_with_profile(
    manifest: &FirmwareManifest,
    node_type: NodeType,
    firmware_type: &str,
    selection: &FirmwareObjectComponentSelection,
    hardware_profile: Option<&HardwareUpdatePlan>,
) -> Result<FirmwarePlan, PlanError> {
    let selected = select_components_with_profile(
        manifest,
        node_type,
        firmware_type,
        selection,
        hardware_profile,
    )?;
    let revision = manifest_revision(manifest)?;
    let mut targets_by_key: BTreeMap<&str, Vec<PlannedTarget>> = BTreeMap::new();
    for selected in selected {
        let linkx_policy = if selected.mapping.target_key == "LINKX" {
            Some(linkx_policy(
                selected.component.version.as_deref(),
                selected.component.linkx_rollout.as_ref(),
            )?)
        } else {
            None
        };
        let candidates = selected
            .component
            .locations
            .iter()
            .enumerate()
            .filter(|(_, location)| {
                payload_location(manifest, selected.component, location)
                    && filename_allowed_with_profile(
                        node_type,
                        &selected.mapping,
                        location,
                        selected.configured,
                    )
                    && has_download_location(location)
            });
        for (index, location) in candidates {
            let mut artifact = Artifact::from_location(
                &revision,
                location_context(selected.board, "Firmware", selected.index, index),
                location,
            )?;
            // A selected update requires its payload, even if prefetch was optional.
            artifact.required = true;
            let expected_version = if node_type == NodeType::ComputeGb300Supermicro
                && selected.mapping.target_key == "BMC"
            {
                Some(
                    nonempty(selected.component.version.as_deref())
                        .ok_or_else(|| {
                            "Supermicro GB300 BMC requires its manifest Version".to_owned()
                        })?
                        .to_owned(),
                )
            } else if node_type == NodeType::ComputeGb200Dell {
                nonempty(selected.component.version.as_deref()).map(str::to_owned)
            } else if node_type == NodeType::PowershelfVrnvl72Liteon
                && selected.mapping.target_key == "LiteOnPMC"
            {
                // The PMC restarts to activate its image and can lose the Redfish
                // task. The manifest version is the independent post-condition.
                nonempty(selected.component.version.as_deref()).map(str::to_owned)
            } else {
                linkx_policy
                    .as_ref()
                    .map(|policy| policy.target_version.clone())
            };
            let firmware = FirmwareTarget {
                component: selected.mapping.redfish_target.to_owned(),
                expected_version,
                linkx_policy: linkx_policy.clone(),
                ..FirmwareTarget::default()
            };
            let firmware = if let Some(configured) = selected.configured {
                configured
                    .resolve(&firmware)
                    .map_err(|error| PlanError::Invalid(error.message))?
            } else {
                firmware
            };
            if node_type == NodeType::ComputeGb200Dell {
                let candidate = FirmwareTarget {
                    firmware_file: artifact.filename.clone(),
                    ..firmware.clone()
                };
                crate::nodes::compute_gb200_dell::validate_firmware_targets(&[candidate])
                    .map_err(|error| PlanError::Invalid(error.message))?;
            }
            targets_by_key
                .entry(selected.mapping.target_key)
                .or_default()
                .push(PlannedTarget {
                    firmware,
                    artifact,
                    order: selected.order,
                });
        }
        // Alternate manifest sources can supply one compiled target.
        targets_by_key
            .entry(selected.mapping.target_key)
            .or_default();
    }
    let mut targets = Vec::new();
    for (key, entries) in targets_by_key {
        if entries.is_empty() {
            return Err(PlanError::Invalid(format!(
                "no eligible firmware payload for {} {key} ({firmware_type})",
                node_type.as_str()
            )));
        }
        if key == "LINKX" && entries.len() > 1 {
            return Err(PlanError::Invalid(
                "LINKX requires exactly one eligible image per node; ambiguous payloads are not selected automatically".to_owned(),
            ));
        }
        targets.extend(entries);
    }
    // Stable sort retains package order and the original compiled target order.
    targets.sort_by_key(|target| target.order);
    if node_type == NodeType::SwitchVrnvl72Nvidia
        && targets.len() == 5
        && targets
            .iter()
            .all(|target| target.artifact.id == targets[0].artifact.id)
        && ["bmc", "bios", "sma", "erot", "cpld1"].iter().all(|name| {
            targets
                .iter()
                .any(|target| &target.firmware.component == name)
        })
    {
        targets.truncate(1);
        targets[0].firmware.component.clear();
    }
    Ok(FirmwarePlan { node_type, targets })
}

impl FirmwarePlan {
    pub(super) fn resolve(
        &self,
        cache: &Path,
        ready: &HashSet<String>,
    ) -> Result<Vec<FirmwareTarget>, PlanError> {
        self.targets
            .iter()
            .map(|target| {
                if !ready.contains(&target.artifact.id) {
                    return Err(PlanError::Cache(format!(
                        "selected artifact {} is not available",
                        target.artifact.context
                    )));
                }
                let path = target
                    .artifact
                    .cache_path(cache)
                    .map_err(PlanError::Cache)?;
                if !path.is_file() {
                    return Err(PlanError::Cache(format!(
                        "prepared firmware file {} is missing",
                        path.display()
                    )));
                }
                let mut firmware = target.firmware.clone();
                firmware.firmware_file = path.to_string_lossy().into_owned();
                if self.node_type.kind() == NodeKind::Powershelf {
                    firmware.component.clear();
                }
                Ok(firmware)
            })
            .collect()
    }
}

#[cfg(test)]
pub(super) fn expectations(
    manifest: &FirmwareManifest,
    node_type: NodeType,
    firmware_type: &str,
    selection: &FirmwareObjectComponentSelection,
) -> Result<Vec<FirmwareManifestExpectation>, PlanError> {
    expectations_with_profile(manifest, node_type, firmware_type, selection, None)
}

pub(super) fn expectations_with_profile(
    manifest: &FirmwareManifest,
    node_type: NodeType,
    firmware_type: &str,
    selection: &FirmwareObjectComponentSelection,
    hardware_profile: Option<&HardwareUpdatePlan>,
) -> Result<Vec<FirmwareManifestExpectation>, PlanError> {
    let selected = select_components_with_profile(
        manifest,
        node_type,
        firmware_type,
        selection,
        hardware_profile,
    )?;
    let mut expectations: BTreeMap<(String, String, String), FirmwareManifestExpectation> =
        BTreeMap::new();
    for selected in selected {
        let component = selected.component;
        let mapping = selected.mapping;
        let subcomponents: Vec<_> = component
            .subcomponents
            .iter()
            .filter(|sub| !sub.version.trim().is_empty())
            .collect();
        let proposed = if subcomponents.is_empty() {
            nonempty(component.version.as_deref())
                .map(|version| {
                    vec![FirmwareManifestExpectation {
                        component: mapping.target_key.to_owned(),
                        version: version.trim().to_owned(),
                        target: mapping.redfish_target.to_owned(),
                        sku_id: String::new(),
                    }]
                })
                .unwrap_or_default()
        } else {
            subcomponents
                .into_iter()
                .filter(|sub| {
                    mapping
                        .target_key
                        .eq_ignore_ascii_case(&component.component)
                        || manifest_component_matches_target(&sub.component, mapping.target_key)
                })
                .map(|sub| FirmwareManifestExpectation {
                    component: sub.component.clone(),
                    version: sub.version.trim().to_owned(),
                    target: mapping.redfish_target.to_owned(),
                    sku_id: sub.sku_id.clone().unwrap_or_default(),
                })
                .collect()
        };
        let destinations: Vec<_> =
            match selected
                .configured
                .map(FirmwareTargetPlan::request_target_selection)
            {
                Some(crate::config::hardware_profiles::RequestTargetSelection::Multipart(
                    paths,
                )) if !paths.is_empty() => paths.iter().map(|path| path.as_str()).collect(),
                _ => vec![mapping.redfish_target],
            };
        for expectation in proposed.into_iter().flat_map(|expectation| {
            destinations
                .iter()
                .map(move |target| FirmwareManifestExpectation {
                    target: (*target).to_owned(),
                    ..expectation.clone()
                })
        }) {
            let key = (
                normalized_firmware_identity(&expectation.component),
                normalized_firmware_identity(&expectation.sku_id),
                normalized_firmware_identity(&expectation.target),
            );
            if let Some(existing) = expectations.get(&key)
                && !firmware_versions_match(&existing.version, &expectation.version)
            {
                return Err(PlanError::Invalid(format!(
                    "firmware manifest has conflicting versions for component {}: {} and {}",
                    expectation.component, existing.version, expectation.version
                )));
            }
            expectations.entry(key).or_insert(expectation);
        }
    }
    if expectations.is_empty() {
        return Err(PlanError::Invalid(format!(
            "no comparable firmware versions found in manifest for node type {} ({firmware_type})",
            node_type.as_str()
        )));
    }
    Ok(expectations.into_values().collect())
}

pub(super) fn switch_image(
    manifest: &FirmwareManifest,
    software_type: &str,
) -> Result<Artifact, PlanError> {
    let revision = manifest_revision(manifest)?;
    let mut images = Vec::new();
    for (board_idx, board) in manifest
        .single_milestone()
        .map_err(|e| e.to_string())?
        .board_skus
        .iter()
        .enumerate()
    {
        if lookup_key_from_board_type(&board.board_type) != Some(SWITCH_LOOKUP_KEY) {
            continue;
        }
        for (index, component) in board.components.software.iter().enumerate() {
            if !component.component.eq_ignore_ascii_case("NVOS")
                || !nonempty(component.channel.as_deref()).is_some_and(|channel| {
                    channel.trim().eq_ignore_ascii_case(software_type.trim())
                })
            {
                continue;
            }
            let context = format!("BoardSKUs[{board_idx}].Components.Software[{index}]");
            let location = select_nvos_location(&context, &component.locations)?;
            let location_idx = component
                .locations
                .iter()
                .position(|candidate| std::ptr::eq(candidate, location))
                .expect("selected location belongs to component");
            let mut artifact = Artifact::from_location(
                &revision,
                location_context(board_idx, "Software", index, location_idx),
                location,
            )?;
            artifact.required = true;
            images.push(artifact);
        }
    }
    if images.len() != 1 {
        return Err(PlanError::Invalid(format!(
            "expected one NVOS image for {software_type}; found {}",
            images.len()
        )));
    }
    Ok(images.remove(0))
}

/// Prefetch is intentionally platform-neutral: it stores payloads, never a union
/// of platform targets. Apply and comparison select one concrete platform later.
pub(super) fn catalog_artifacts(manifest: &FirmwareManifest) -> Result<Vec<Artifact>, PlanError> {
    let revision = manifest_revision(manifest)?;
    let mut artifacts = Vec::new();
    for (board_idx, board) in manifest
        .single_milestone()
        .map_err(|e| e.to_string())?
        .board_skus
        .iter()
        .enumerate()
    {
        if lookup_key_from_board_type(&board.board_type).is_none() {
            continue;
        }
        for (index, component) in board.components.firmware.iter().enumerate() {
            // Recovery packages ride the same prefetch: the compute OOB
            // recovery flow pushes them, and an object is not usable for
            // recovery until they are cached like every other payload.
            for (loc_idx, location) in component.locations.iter().enumerate() {
                if (payload_location(manifest, component, location)
                    || recovery_payload_location(location))
                    && has_download_location(location)
                {
                    artifacts.push(Artifact::from_location(
                        &revision,
                        location_context(board_idx, "Firmware", index, loc_idx),
                        location,
                    )?);
                }
            }
        }
        if lookup_key_from_board_type(&board.board_type) == Some(SWITCH_LOOKUP_KEY) {
            for (index, component) in board
                .components
                .software
                .iter()
                .enumerate()
                .filter(|(_, c)| c.component.eq_ignore_ascii_case("NVOS"))
            {
                // Prefetch all eligible images; uniqueness belongs to apply.
                for (loc_idx, location) in component
                    .locations
                    .iter()
                    .enumerate()
                    .filter(|(_, location)| downloadable_nvos_location(location))
                {
                    artifacts.push(Artifact::from_location(
                        &revision,
                        location_context(board_idx, "Software", index, loc_idx),
                        location,
                    )?);
                }
            }
        }
    }
    Ok(artifacts)
}

/// The two HMC packages the compute OOB recovery flow pushes, from the one
/// HMC component the plan would flash.
pub(super) struct RecoveryArtifacts {
    pub(super) regular: Artifact,
    pub(super) recovery: Artifact,
}

impl RecoveryArtifacts {
    /// The two cached files, once both are in the ready set and on disk.
    pub(super) fn resolve(
        &self,
        cache: &Path,
        ready: &HashSet<String>,
    ) -> Result<RecoveryBundles, PlanError> {
        let path = |label: &str, artifact: &Artifact| {
            if !ready.contains(&artifact.id) {
                return Err(PlanError::Cache(format!(
                    "{label} {} is not available",
                    artifact.context
                )));
            }
            let path = artifact.cache_path(cache).map_err(PlanError::Cache)?;
            if !path.is_file() {
                return Err(PlanError::Cache(format!(
                    "{label} {} is missing",
                    path.display()
                )));
            }
            Ok(path.to_string_lossy().into_owned())
        };
        Ok(RecoveryBundles {
            regular: path("regular HMC bundle", &self.regular)?,
            recovery: path("HMC recovery package", &self.recovery)?,
        })
    }
}

/// `None` when the manifest carries no recovery package for the platform.
/// The recovery flow names no channel: the production HMC package wins,
/// then the only channel present, and several non-production channels are
/// refused rather than guessed.
pub(super) fn recovery_artifacts(
    manifest: &FirmwareManifest,
    node_type: NodeType,
) -> Result<Option<RecoveryArtifacts>, PlanError> {
    let boards = &manifest
        .single_milestone()
        .map_err(|e| e.to_string())?
        .board_skus;
    let channels: BTreeSet<String> = boards
        .iter()
        .filter(|board| board_matches(&board.board_type, node_type))
        .flat_map(|board| &board.components.firmware)
        .filter(|component| {
            firmware_source_component_matches(&component.component, RECOVERY_BUNDLE_COMPONENT)
        })
        .filter_map(|component| channel(manifest, component))
        .map(|channel| channel.trim().to_ascii_lowercase())
        .collect();
    let channel = if channels.contains("prod") {
        "prod".to_owned()
    } else {
        let mut channels = channels.into_iter();
        match (channels.next(), channels.next()) {
            (None, _) => return Ok(None),
            (Some(only), None) => only,
            (Some(first), Some(second)) => {
                return Err(PlanError::Invalid(format!(
                    "recovery packages for {} span several channels ({first}, {second}, ...) and none is prod",
                    node_type.as_str()
                )));
            }
        }
    };
    // Only the HMC component is validated: an unrelated entry with no
    // channel must not fail a recovery whose inputs never touch it.
    let selected = match select_components_with_profile(
        manifest,
        node_type,
        &channel,
        &FirmwareObjectComponentSelection::components(vec![RECOVERY_BUNDLE_COMPONENT.to_owned()]),
        None,
    ) {
        Ok(selected) => selected,
        Err(PlanError::NoComponents(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut hmc = selected
        .into_iter()
        .filter(|selected| selected.mapping.target_key == RECOVERY_BUNDLE_COMPONENT);
    let Some(selected) = hmc.next() else {
        return Ok(None);
    };
    if hmc.next().is_some() {
        return Err(PlanError::Invalid(format!(
            "ambiguous HMC components for {} ({channel}) recovery",
            node_type.as_str()
        )));
    }
    let revision = manifest_revision(manifest)?;
    let mut regular = Vec::new();
    let mut recovery = Vec::new();
    for (index, location) in selected.component.locations.iter().enumerate() {
        if !has_download_location(location) {
            continue;
        }
        let context = location_context(selected.board, "Firmware", selected.index, index);
        if recovery_payload_location(location) {
            recovery.push(Artifact::from_location(&revision, context, location)?);
        } else if payload_location(manifest, selected.component, location)
            && filename_allowed(node_type, &selected.mapping, location)
        {
            regular.push(Artifact::from_location(&revision, context, location)?);
        }
    }
    let one = |mut artifacts: Vec<Artifact>, what: &str| match artifacts.len() {
        1 => Ok(Some(artifacts.remove(0))),
        0 => Ok(None),
        n => Err(PlanError::Invalid(format!(
            "ambiguous {what} for {} ({channel}) recovery: {n} candidates",
            node_type.as_str()
        ))),
    };
    let Some(mut recovery) = one(recovery, "HMC recovery package")? else {
        return Ok(None);
    };
    let Some(mut regular) = one(regular, "regular HMC bundle")? else {
        return Err(PlanError::Invalid(format!(
            "no eligible regular HMC bundle for {} ({channel}) recovery",
            node_type.as_str()
        )));
    };
    regular.required = true;
    recovery.required = true;
    Ok(Some(RecoveryArtifacts { regular, recovery }))
}

#[cfg(test)]
mod tests;
