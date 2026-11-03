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

//! Versioned hardware-update profiles.
//!
//! Profile YAML is parsed and validated once at startup. Firmware execution
//! consumes only the compiled [`HardwareUpdatePlan`], never the raw document.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use noyalib::compat::serde_yaml;
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::ComputePowerOnOptions;
use crate::domain::node::{
    NodeKind, NodeRuntimeConfig, NodeType, NvfwupdServerProfile, RedfishNvfwupdComputeConfig,
};

use crate::domain::redfish_path::RedfishPath;

#[cfg(test)]
mod parameter_tests;
mod selectors;

const SUPPORTED_SCHEMA_VERSION: u32 = 1;
const MAX_CONFIGURED_DURATION: Duration = Duration::from_secs(24 * 60 * 60);

include!(concat!(env!("OUT_DIR"), "/hardware_profiles.rs"));

/// Named activation implementation selected after a stage.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivationStrategy {
    /// Perform the complete compute-tray AC power-cycle sequence.
    FullComputePowerCycle,
}

/// Bounded polling policy compiled from a profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PollPolicy {
    task_timeout: Duration,
    interval: Duration,
    transient_error_budget: Duration,
}

impl PollPolicy {
    /// Maximum time to wait for a device task.
    pub fn task_timeout(self) -> Duration {
        self.task_timeout
    }

    /// Delay between task queries.
    pub fn interval(self) -> Duration {
        self.interval
    }

    /// Time during which transient task-query errors can be retried.
    pub fn transient_error_budget(self) -> Duration {
        self.transient_error_budget
    }
}

/// Post-update version verification policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerificationPolicy {
    enabled: bool,
    settle_delay: Duration,
    attempts: NonZeroU32,
    retry_interval: Duration,
}

impl VerificationPolicy {
    pub(crate) fn required(
        settle_delay: Duration,
        attempts: NonZeroU32,
        retry_interval: Duration,
    ) -> Self {
        Self {
            enabled: true,
            settle_delay,
            attempts,
            retry_interval,
        }
    }

    /// Whether the executor must verify installed versions.
    pub fn enabled(self) -> bool {
        self.enabled
    }

    /// Delay before the first inventory query.
    pub fn settle_delay(self) -> Duration {
        self.settle_delay
    }

    /// Maximum number of inventory queries.
    pub fn attempts(self) -> NonZeroU32 {
        self.attempts
    }

    /// Delay between inventory queries.
    pub fn retry_interval(self) -> Duration {
        self.retry_interval
    }
}

/// Optional filename predicates used to select a firmware artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilenameMatcher {
    suffix: Option<String>,
    contains: Option<String>,
    case_insensitive: bool,
}

impl FilenameMatcher {
    /// Required filename suffix, when configured.
    pub fn suffix(&self) -> Option<&str> {
        self.suffix.as_deref()
    }

    /// Required filename substring, when configured.
    pub fn contains(&self) -> Option<&str> {
        self.contains.as_deref()
    }

    /// Whether filename predicates use ASCII case-insensitive matching.
    pub fn case_insensitive(&self) -> bool {
        self.case_insensitive
    }

    /// Return whether a filename satisfies every configured predicate.
    /// With no predicates configured, any filename matches.
    pub fn matches(&self, filename: &str) -> bool {
        let normalized;
        let filename = if self.case_insensitive {
            normalized = filename.to_ascii_lowercase();
            &normalized
        } else {
            filename
        };
        self.suffix
            .as_deref()
            .is_none_or(|suffix| filename.ends_with(suffix))
            && self
                .contains
                .as_deref()
                .is_none_or(|needle| filename.contains(needle))
    }
}

/// How the firmware driver obtains the Redfish update target list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestTargetSelection {
    /// Send the validated same-origin paths declared by the profile.
    Explicit(Vec<RedfishPath>),
    /// One upload using the entire Targets or HttpPushUriTargets parameter.
    Multipart(Vec<RedfishPath>),
    /// Let the selected driver and firmware package determine the targets.
    DriverDefault,
}

/// One logical update target in a compiled stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirmwareTargetPlan {
    id: String,
    component_key: String,
    source_components: Vec<String>,
    filename: FilenameMatcher,
    request_target_selection: RequestTargetSelection,
    update_parameters: Option<Map<String, Value>>,
    oem_parameters: Option<Map<String, Value>>,
    default_apply: bool,
}

impl FirmwareTargetPlan {
    /// Resolve a selected component or retain a manifest-selected destination.
    /// Logical identity remains separate from the driver's transport selector.
    pub(crate) fn resolve(
        &self,
        target: &crate::domain::node::FirmwareTarget,
    ) -> crate::utilities::error::Result<crate::domain::node::FirmwareTarget> {
        use crate::domain::firmware_target::{FirmwareDestination, FirmwareTargetResolution};
        use crate::utilities::error::RmsError;
        if let Some(resolved) = &target.resolution {
            let allowed = match (&self.request_target_selection, &resolved.destination) {
                (RequestTargetSelection::Explicit(paths), FirmwareDestination::Redfish(path)) => {
                    paths.contains(path)
                }
                (
                    RequestTargetSelection::Multipart(expected),
                    FirmwareDestination::Multipart(actual),
                ) => expected == actual,
                (RequestTargetSelection::DriverDefault, FirmwareDestination::DriverDefault) => true,
                _ => false,
            };
            if resolved.profile_target_id != self.id
                || !allowed
                || resolved.update_parameters != self.update_parameters
                || resolved.oem_parameters != self.oem_parameters
            {
                return Err(RmsError::invalid_argument(format!(
                    "resolved firmware destination does not belong to profile target {}",
                    self.id
                )));
            }
            return Ok(target.clone());
        }
        let destination = match &self.request_target_selection {
            RequestTargetSelection::Explicit(paths) => {
                // Manifest fanout supplies a selected path after component selection.
                // Direct requests supply the component key and need one destination.
                let path = paths.iter().find(|path| path.eq_ignore_ascii_case(target.component.trim()))
                    .or(if let [path] = paths.as_slice() { Some(path) } else { None })
                    .ok_or_else(|| RmsError::invalid_argument(format!(
                        "hardware profile target {} has multiple request_targets; direct requests require a single destination", self.id)))?;
                FirmwareDestination::Redfish(path.clone())
            }
            RequestTargetSelection::DriverDefault => FirmwareDestination::DriverDefault,
            RequestTargetSelection::Multipart(paths) => {
                FirmwareDestination::Multipart(paths.clone())
            }
        };
        Ok(crate::domain::node::FirmwareTarget {
            component: self.component_key.clone(),
            resolution: Some(FirmwareTargetResolution {
                profile_target_id: self.id.clone(),
                destination,
                update_parameters: self.update_parameters.clone(),
                oem_parameters: self.oem_parameters.clone(),
            }),
            ..target.clone()
        })
    }

    /// Stable logical target identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Canonical component selector exposed by the RMS firmware API.
    pub fn component_key(&self) -> &str {
        &self.component_key
    }

    /// Match only the configured component key, ignoring ASCII case and outer whitespace.
    /// Filename selection is checked separately by the workflow planner.
    pub fn matches_component(&self, component: &str) -> bool {
        self.component_key.eq_ignore_ascii_case(component.trim())
    }

    /// Manifest component names that can supply this target.
    pub fn source_components(&self) -> &[String] {
        &self.source_components
    }

    /// Artifact filename predicates.
    pub fn filename(&self) -> &FilenameMatcher {
        &self.filename
    }

    /// How the driver obtains the Redfish update target list.
    pub fn request_target_selection(&self) -> &RequestTargetSelection {
        &self.request_target_selection
    }

    /// Explicit same-origin Redfish paths passed to the driver.
    ///
    /// This is empty for multipart or driver-default targeting.
    pub fn request_targets(&self) -> &[RedfishPath] {
        match &self.request_target_selection {
            RequestTargetSelection::Explicit(targets) => targets,
            RequestTargetSelection::Multipart(_) | RequestTargetSelection::DriverDefault => &[],
        }
    }

    /// Validated paths used by explicit uploads or a single multipart upload.
    pub fn destination_paths(&self) -> &[RedfishPath] {
        match &self.request_target_selection {
            RequestTargetSelection::Explicit(paths) | RequestTargetSelection::Multipart(paths) => {
                paths
            }
            RequestTargetSelection::DriverDefault => &[],
        }
    }

    /// Optional JSON object sent as multipart UpdateParameters.
    pub fn update_parameters(&self) -> Option<&Map<String, Value>> {
        self.update_parameters.as_ref()
    }

    /// Optional JSON object sent as multipart OemParameters.
    pub fn oem_parameters(&self) -> Option<&Map<String, Value>> {
        self.oem_parameters.as_ref()
    }

    /// Whether a request without component filters selects this target.
    pub fn default_apply(&self) -> bool {
        self.default_apply
    }
}

/// Ordered group of targets followed by an optional activation operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirmwareUpdateStage {
    number: u32,
    targets: Vec<FirmwareTargetPlan>,
    activate_after: bool,
}

impl FirmwareUpdateStage {
    /// Profile-defined stage number used for ordering.
    pub fn number(&self) -> u32 {
        self.number
    }

    /// Targets executed in document order within this stage.
    pub fn targets(&self) -> &[FirmwareTargetPlan] {
        &self.targets
    }

    /// Whether the configured activation strategy runs after the stage.
    pub fn activate_after(&self) -> bool {
        self.activate_after
    }
}

/// Fully validated, immutable firmware-update policy for one hardware type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HardwareUpdatePlan {
    schema_version: u32,
    profile_id: String,
    source_sha256: String,
    node_type: NodeType,
    request_matching_enabled: bool,
    runtime_config: NodeRuntimeConfig,
    polling: PollPolicy,
    power_on: Option<ComputePowerOnOptions>,
    verification: VerificationPolicy,
    stages: Vec<FirmwareUpdateStage>,
    activation: Option<ActivationStrategy>,
}

impl HardwareUpdatePlan {
    /// Parse, validate, and compile one YAML profile.
    ///
    /// # Errors
    ///
    /// Returns [`HardwareProfileError::Parse`] for malformed or structurally
    /// invalid YAML and [`HardwareProfileError::Validation`] for a cross-field
    /// invariant violation.
    pub fn from_yaml_str(source: &str) -> Result<Self, HardwareProfileError> {
        let document: HardwareProfileDocument =
            serde_yaml::from_str(source).map_err(HardwareProfileError::Parse)?;
        Self::compile(document, hex::encode(Sha256::digest(source.as_bytes())))
    }

    /// Schema version from which this plan was compiled.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Stable profile identifier.
    pub fn profile_id(&self) -> &str {
        &self.profile_id
    }

    /// Lowercase SHA-256 of the exact UTF-8 YAML bytes loaded at startup.
    ///
    /// Comments and whitespace affect this hash. It identifies the source file,
    /// not the effective runtime settings or the RMS implementation.
    pub fn source_sha256(&self) -> &str {
        &self.source_sha256
    }

    /// Canonical RMS node type selected by the profile.
    pub fn node_type(&self) -> NodeType {
        self.node_type
    }

    /// Whether incoming requests may select this profile by node type.
    pub fn request_matching_enabled(&self) -> bool {
        self.request_matching_enabled
    }

    /// Validated parameters for the code-selected node implementation.
    pub fn runtime_config(&self) -> &NodeRuntimeConfig {
        &self.runtime_config
    }

    /// Device-task polling policy.
    pub fn polling(&self) -> PollPolicy {
        self.polling
    }

    /// Bounds for the tray power-on that opens an update, the same type and
    /// rule as the service-wide `[workflows]` bounds. `None` leaves those in
    /// force for this profile's trays.
    pub fn power_on(&self) -> Option<ComputePowerOnOptions> {
        self.power_on
    }

    /// Installed-version verification policy.
    pub fn verification(&self) -> VerificationPolicy {
        self.verification
    }

    /// Ordered update stages.
    pub fn stages(&self) -> &[FirmwareUpdateStage] {
        &self.stages
    }

    /// Activation operation used by stages that request activation.
    pub fn activation(&self) -> Option<ActivationStrategy> {
        self.activation
    }

    fn compile(
        document: HardwareProfileDocument,
        source_sha256: String,
    ) -> Result<Self, HardwareProfileError> {
        if document.schema_version != SUPPORTED_SCHEMA_VERSION {
            return validation_error(format!(
                "unsupported schema_version {}; expected {SUPPORTED_SCHEMA_VERSION}",
                document.schema_version
            ));
        }

        let profile_id = validate_identifier("profile_id", document.profile_id)?;
        let node_type_name = document.matches.node_type.trim();
        let node_type = NodeType::from_stable_name(node_type_name).ok_or_else(|| {
            HardwareProfileError::Validation(format!(
                "unknown canonical matches.node_type {node_type_name:?}"
            ))
        })?;
        if node_type.kind() != NodeKind::Compute {
            return validation_error(format!(
                "hardware profile schema version 1 does not support {} nodes",
                node_type.kind()
            ));
        }
        let runtime_config = compile_runtime_config(document.driver)?;

        let polling = compile_polling(document.firmware.polling)?;
        let power_on = document
            .firmware
            .power_on
            .map(compile_power_on)
            .transpose()?;
        let verification = compile_verification(document.firmware.verification)?;
        let mut target_ids = BTreeSet::new();
        let mut grouped = BTreeMap::<u32, StageBuilder>::new();

        if document.firmware.targets.is_empty() {
            return validation_error("firmware.targets must contain at least one target");
        }

        for target in document.firmware.targets {
            if target.stage == 0 {
                return validation_error("firmware target stage must be greater than zero");
            }

            let target = compile_target(target)?;
            if !target_ids.insert(target.plan.id.clone()) {
                return validation_error(format!(
                    "duplicate firmware target id {:?}",
                    target.plan.id
                ));
            }

            let stage = grouped.entry(target.stage).or_insert_with(|| StageBuilder {
                activate_after: target.activate_after,
                targets: Vec::new(),
            });
            if stage.activate_after != target.activate_after {
                return validation_error(format!(
                    "all targets in stage {} must use the same activate_after_stage value",
                    target.stage
                ));
            }
            stage.targets.push(target.plan);
        }

        let stages = grouped
            .into_iter()
            .map(|(number, stage)| FirmwareUpdateStage {
                number,
                targets: stage.targets,
                activate_after: stage.activate_after,
            })
            .collect::<Vec<_>>();

        selectors::validate_component_key_uniqueness(&profile_id, &stages)?;

        let activation = document.firmware.activation.map(|value| value.strategy);
        if stages.iter().any(FirmwareUpdateStage::activate_after) && activation.is_none() {
            return validation_error(
                "firmware.activation is required when a stage activates after completion",
            );
        }

        Ok(Self {
            schema_version: document.schema_version,
            profile_id,
            source_sha256,
            node_type,
            request_matching_enabled: document.matches.enabled,
            runtime_config,
            polling,
            power_on,
            verification,
            stages,
            activation,
        })
    }
}

/// Immutable catalog snapshot indexed by profile ID and canonical node type.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HardwareProfileCatalog {
    by_profile_id: BTreeMap<String, Arc<HardwareUpdatePlan>>,
    by_node_type: HashMap<NodeType, Arc<HardwareUpdatePlan>>,
}

static BUILT_IN_CATALOG: LazyLock<HardwareProfileCatalog> = LazyLock::new(|| {
    HardwareProfileCatalog::built_in().expect("embedded hardware profiles must be valid")
});

/// Embedded defaults also supply vendor parameters when no external profile is selected.
pub(crate) fn built_in_profile(node_type: NodeType) -> Option<&'static HardwareUpdatePlan> {
    BUILT_IN_CATALOG.by_node_type(node_type)
}

/// Dell GB200 was introduced with profile-based planning and has no legacy
/// planner, so it always plans from a profile: an enabled override, else the
/// embedded one.
fn requires_embedded_profile(node_type: NodeType) -> bool {
    node_type == NodeType::ComputeGb200Dell
}

impl HardwareProfileCatalog {
    /// Compile the complete profiles distributed with this RMS binary.
    ///
    /// # Errors
    ///
    /// Returns an error if a shipped profile is invalid or if two shipped
    /// profiles claim the same ID or node type.
    pub fn built_in() -> Result<Self, HardwareProfileError> {
        let mut catalog = Self::default();
        for (_filename, source) in BUILT_IN_HARDWARE_PROFILES {
            catalog.insert(HardwareUpdatePlan::from_yaml_str(source)?)?;
        }
        Ok(catalog)
    }

    /// Load every `.yaml` and `.yml` profile directly under a directory.
    ///
    /// Files are processed in lexical path order so startup failures are
    /// deterministic. Other directory entries are ignored. An empty directory is
    /// valid: node types without an enabled profile use their code-defined defaults.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be read, a profile cannot be
    /// parsed or validated, or two profiles claim the same profile ID or node
    /// type.
    pub fn load_directory(path: impl AsRef<Path>) -> Result<Self, HardwareProfileError> {
        let path = path.as_ref();
        let entries = fs::read_dir(path).map_err(|source| HardwareProfileError::ReadDirectory {
            path: path.to_path_buf(),
            source,
        })?;
        let mut profile_paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| HardwareProfileError::ReadDirectory {
                path: path.to_path_buf(),
                source,
            })?;
            let profile_path = entry.path();
            if matches!(
                profile_path
                    .extension()
                    .and_then(|extension| extension.to_str()),
                Some("yaml" | "yml")
            ) {
                profile_paths.push(profile_path);
            }
        }
        profile_paths.sort();

        let mut catalog = Self::default();
        for profile_path in profile_paths {
            let source = fs::read_to_string(&profile_path).map_err(|source| {
                HardwareProfileError::ReadProfile {
                    path: profile_path.clone(),
                    source,
                }
            })?;
            let plan = HardwareUpdatePlan::from_yaml_str(&source).map_err(|source| {
                HardwareProfileError::InvalidProfileFile {
                    path: profile_path,
                    source: Box::new(source),
                }
            })?;
            catalog.insert(plan)?;
        }

        Ok(catalog)
    }

    /// Resolve a profile by its exact, case-sensitive profile ID.
    pub fn by_profile_id(&self, profile_id: &str) -> Option<&HardwareUpdatePlan> {
        self.by_profile_id.get(profile_id).map(Arc::as_ref)
    }

    /// Resolve the one profile registered for a canonical node type.
    pub fn by_node_type(&self, node_type: NodeType) -> Option<&HardwareUpdatePlan> {
        self.by_node_type.get(&node_type).map(Arc::as_ref)
    }

    /// Resolve and clone the immutable profile registered for a node type.
    pub fn shared_by_node_type(&self, node_type: NodeType) -> Option<Arc<HardwareUpdatePlan>> {
        self.by_node_type.get(&node_type).cloned()
    }

    /// Resolve an optional override for an incoming request node type.
    /// Missing or disabled profiles leave the concrete implementation's defaults
    /// active, except Dell GB200, which falls back to its embedded profile.
    pub fn resolve_request(&self, node_type: NodeType) -> Option<&HardwareUpdatePlan> {
        self.request_profile(node_type).map(Arc::as_ref)
    }

    /// Resolve and clone the active profile selected by a request node type.
    /// Dell GB200 falls back to its embedded profile when no override is enabled.
    pub fn shared_for_request(&self, node_type: NodeType) -> Option<Arc<HardwareUpdatePlan>> {
        self.request_profile(node_type).cloned()
    }

    /// The single place that decides which profile a request uses: the enabled
    /// catalog entry, else the embedded profile for node types that have no
    /// code-defined firmware planner.
    fn request_profile(&self, node_type: NodeType) -> Option<&Arc<HardwareUpdatePlan>> {
        self.by_node_type
            .get(&node_type)
            .filter(|profile| profile.request_matching_enabled())
            .or_else(|| {
                requires_embedded_profile(node_type)
                    .then(|| BUILT_IN_CATALOG.by_node_type.get(&node_type))
                    .flatten()
            })
    }

    /// Clone parameters to layer onto the concrete implementation for a request.
    pub fn runtime_config_for_request(&self, node_type: NodeType) -> Option<NodeRuntimeConfig> {
        self.resolve_request(node_type)
            .map(|profile| profile.runtime_config().clone())
    }

    /// Iterate through plans in profile-ID order.
    pub fn profiles(&self) -> impl Iterator<Item = &HardwareUpdatePlan> {
        self.by_profile_id.values().map(Arc::as_ref)
    }

    /// Number of compiled profiles in the catalog.
    pub fn len(&self) -> usize {
        self.by_profile_id.len()
    }

    /// Whether the catalog contains no profiles.
    pub fn is_empty(&self) -> bool {
        self.by_profile_id.is_empty()
    }

    fn insert(&mut self, plan: HardwareUpdatePlan) -> Result<(), HardwareProfileError> {
        if self.by_profile_id.contains_key(plan.profile_id()) {
            return validation_error(format!(
                "duplicate hardware profile id {:?}",
                plan.profile_id()
            ));
        }
        if let Some(existing) = self.by_node_type.get(&plan.node_type()) {
            return validation_error(format!(
                "node type {:?} is claimed by both profiles {:?} and {:?}",
                plan.node_type(),
                existing.profile_id(),
                plan.profile_id()
            ));
        }

        let plan = Arc::new(plan);
        self.by_node_type
            .insert(plan.node_type(), Arc::clone(&plan));
        self.by_profile_id
            .insert(plan.profile_id().to_owned(), plan);
        Ok(())
    }
}

/// Failure to parse, validate, or load a hardware profile.
#[derive(Debug, Error)]
pub enum HardwareProfileError {
    /// YAML syntax or shape did not match the versioned schema.
    #[error("invalid hardware profile YAML: {0}")]
    Parse(serde_yaml::Error),

    /// A cross-field or safety invariant failed.
    #[error("invalid hardware profile: {0}")]
    Validation(String),

    /// The configured profile directory could not be read.
    #[error("could not read hardware profile directory {path}: {source}")]
    ReadDirectory {
        /// Directory that could not be read.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },

    /// One profile file could not be read.
    #[error("could not read hardware profile {path}: {source}")]
    ReadProfile {
        /// File that could not be read.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },

    /// One profile file was parsed but was not valid.
    #[error("hardware profile {path} is invalid: {source}")]
    InvalidProfileFile {
        /// Invalid profile file.
        path: PathBuf,
        /// Parse or validation failure.
        source: Box<HardwareProfileError>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HardwareProfileDocument {
    schema_version: u32,
    profile_id: String,
    matches: RequestMatchDocument,
    driver: DriverDocument,
    firmware: FirmwareDocument,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverDocument {
    nvfwupd_server_profile: NvfwupdServerProfileDocument,
    redfish: RedfishComputeDocument,
    #[serde(default)]
    timing: ComputeTimingDocument,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NvfwupdServerProfileDocument {
    ComputeGb200,
    ComputeGb300,
    ComputeVrnvl72,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RedfishComputeDocument {
    computer_system_id: String,
    bmc_manager_id: String,
    hmc_manager_id: String,
    #[serde(default)]
    power: ComputePowerPathsDocument,
    device_info_timeout: String,
    #[serde(default)]
    connect_timeout: Option<String>,
    #[serde(default)]
    request_timeout: Option<String>,
    #[serde(default)]
    upload_timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestMatchDocument {
    node_type: String,
    #[serde(default = "default_true")]
    enabled: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FirmwareDocument {
    polling: PollingDocument,
    #[serde(default)]
    power_on: Option<PowerOnDocument>,
    verification: VerificationDocument,
    targets: Vec<TargetDocument>,
    activation: Option<ActivationDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PollingDocument {
    task_timeout: String,
    interval: String,
    transient_error_budget: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PowerOnDocument {
    timeout: String,
    interval: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerificationDocument {
    enabled: bool,
    settle_delay: String,
    attempts: u32,
    retry_interval: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetDocument {
    id: String,
    component_key: String,
    source_components: Vec<String>,
    #[serde(default)]
    filename: FilenameDocument,
    #[serde(default)]
    request_targets: Vec<String>,
    #[serde(default)]
    driver_default_targets: bool,
    #[serde(default)]
    update_parameters: Option<Map<String, Value>>,
    #[serde(default)]
    oem_parameters: Option<Map<String, Value>>,
    #[serde(default = "default_true")]
    default_apply: bool,
    stage: u32,
    activate_after_stage: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilenameDocument {
    suffix: Option<String>,
    contains: Option<String>,
    #[serde(default)]
    case_insensitive: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationDocument {
    strategy: ActivationStrategy,
}

struct CompiledTarget {
    stage: u32,
    activate_after: bool,
    plan: FirmwareTargetPlan,
}

struct StageBuilder {
    targets: Vec<FirmwareTargetPlan>,
    activate_after: bool,
}

const fn default_true() -> bool {
    true
}

fn compile_runtime_config(
    document: DriverDocument,
) -> Result<NodeRuntimeConfig, HardwareProfileError> {
    let DriverDocument {
        nvfwupd_server_profile,
        redfish,
        timing,
    } = document;
    let device_info_timeout = parse_duration(
        "driver.redfish.device_info_timeout",
        &redfish.device_info_timeout,
    )?;
    require_nonzero("driver.redfish.device_info_timeout", device_info_timeout)?;
    let runtime_config = RedfishNvfwupdComputeConfig {
        nvfwupd_server_profile: match nvfwupd_server_profile {
            NvfwupdServerProfileDocument::ComputeGb200 => NvfwupdServerProfile::ComputeGb200,
            NvfwupdServerProfileDocument::ComputeGb300 => NvfwupdServerProfile::ComputeGb300,
            NvfwupdServerProfileDocument::ComputeVrnvl72 => NvfwupdServerProfile::ComputeVrnvl72,
        },
        computer_system_id: validate_redfish_resource_id(
            "driver.redfish.computer_system_id",
            redfish.computer_system_id,
        )?
        .into(),
        bmc_manager_id: validate_redfish_resource_id(
            "driver.redfish.bmc_manager_id",
            redfish.bmc_manager_id,
        )?
        .into(),
        hmc_manager_id: validate_redfish_resource_id(
            "driver.redfish.hmc_manager_id",
            redfish.hmc_manager_id,
        )?
        .into(),
        device_info_timeout,
        redfish_connect_timeout: compile_timing_duration(
            "driver.redfish.connect_timeout",
            redfish.connect_timeout.as_deref(),
            Duration::from_secs(10),
            false,
        )?,
        redfish_request_timeout: compile_timing_duration(
            "driver.redfish.request_timeout",
            redfish.request_timeout.as_deref(),
            Duration::from_secs(30),
            false,
        )?,
        redfish_upload_timeout: compile_timing_duration(
            "driver.redfish.upload_timeout",
            redfish.upload_timeout.as_deref(),
            Duration::from_secs(600),
            false,
        )?,
        power_paths: redfish.power.compile()?,
        firmware_timing: timing.compile()?,
    };

    Ok(NodeRuntimeConfig::RedfishNvfwupdCompute(runtime_config))
}

/// Optional endpoint overrides; absent paths retain discovery.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ComputePowerPathsDocument {
    system_state_path: Option<String>,
    system_reset_path: Option<String>,
    bmc_reset_path: Option<String>,
    hmc_reset_path: Option<String>,
    aux_power_cycle_path: Option<String>,
    bmc_ready_path: Option<String>,
}

impl ComputePowerPathsDocument {
    fn compile(
        self,
    ) -> Result<crate::domain::compute_power::ComputePowerPaths, HardwareProfileError> {
        Ok(crate::domain::compute_power::ComputePowerPaths {
            system_state_path: compile_power_path(
                "driver.redfish.power.system_state_path",
                self.system_state_path,
            )?,
            system_reset_path: compile_power_path(
                "driver.redfish.power.system_reset_path",
                self.system_reset_path,
            )?,
            bmc_reset_path: compile_power_path(
                "driver.redfish.power.bmc_reset_path",
                self.bmc_reset_path,
            )?,
            hmc_reset_path: compile_power_path(
                "driver.redfish.power.hmc_reset_path",
                self.hmc_reset_path,
            )?,
            aux_power_cycle_path: compile_power_path(
                "driver.redfish.power.aux_power_cycle_path",
                self.aux_power_cycle_path,
            )?,
            bmc_ready_path: compile_power_path(
                "driver.redfish.power.bmc_ready_path",
                self.bmc_ready_path,
            )?,
        })
    }
}

fn compile_power_path(
    field: &str,
    value: Option<String>,
) -> Result<Option<RedfishPath>, HardwareProfileError> {
    value
        .map(|value| parse_redfish_path(field, &value))
        .transpose()
}

/// Optional sections preserve version-1 profiles that omit timing settings.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ComputeTimingDocument {
    transport: TransportTimingDocument,
    pre_update: PreUpdateTimingDocument,
    activation: ActivationTimingDocument,
    recovery: RecoveryTimingDocument,
    inventory_recovery: InventoryRecoveryTimingDocument,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TransportTimingDocument {
    connect_timeout: Option<String>,
    request_timeout: Option<String>,
    upload_timeout: Option<String>,
    reachability_timeout: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PreUpdateTimingDocument {
    query_timeout: Option<String>,
    background_copy_timeout: Option<String>,
    background_copy_poll_interval: Option<String>,
    write_protect_inventory_attempts: Option<u32>,
    write_protect_inventory_retry_interval: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ActivationTimingDocument {
    power_off_delay: Option<String>,
    ac_cycle_attempts: Option<u32>,
    ac_cycle_stabilization: Option<String>,
    command_retries: Option<u32>,
    command_retry_interval: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RecoveryTimingDocument {
    bmc_attempts: Option<u32>,
    bmc_interval: Option<String>,
    power_on_attempts: Option<u32>,
    power_on_interval: Option<String>,
    power_on_resend_interval: Option<String>,
    probe_timeout: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct InventoryRecoveryTimingDocument {
    retries: Option<u32>,
    retry_interval: Option<String>,
}

impl ComputeTimingDocument {
    fn compile(
        self,
    ) -> Result<crate::domain::compute_timing::ComputeFirmwareTiming, HardwareProfileError> {
        use crate::domain::compute_timing::ComputeFirmwareTiming;
        let defaults = ComputeFirmwareTiming::default();
        let timing = ComputeFirmwareTiming {
            connect_timeout: compile_timing_duration(
                "driver.timing.transport.connect_timeout",
                self.transport.connect_timeout.as_deref(),
                defaults.connect_timeout,
                false,
            )?,
            request_timeout: compile_timing_duration(
                "driver.timing.transport.request_timeout",
                self.transport.request_timeout.as_deref(),
                defaults.request_timeout,
                false,
            )?,
            upload_timeout: compile_timing_duration(
                "driver.timing.transport.upload_timeout",
                self.transport.upload_timeout.as_deref(),
                defaults.upload_timeout,
                false,
            )?,
            reachability_timeout: compile_timing_duration(
                "driver.timing.transport.reachability_timeout",
                self.transport.reachability_timeout.as_deref(),
                defaults.reachability_timeout,
                false,
            )?,
            precondition_query_timeout: compile_timing_duration(
                "driver.timing.pre_update.query_timeout",
                self.pre_update.query_timeout.as_deref(),
                defaults.precondition_query_timeout,
                false,
            )?,
            background_copy_timeout: compile_timing_duration(
                "driver.timing.pre_update.background_copy_timeout",
                self.pre_update.background_copy_timeout.as_deref(),
                defaults.background_copy_timeout,
                false,
            )?,
            background_copy_poll_interval: compile_timing_duration(
                "driver.timing.pre_update.background_copy_poll_interval",
                self.pre_update.background_copy_poll_interval.as_deref(),
                defaults.background_copy_poll_interval,
                false,
            )?,
            write_protect_inventory_attempts: compile_timing_count(
                "driver.timing.pre_update.write_protect_inventory_attempts",
                self.pre_update
                    .write_protect_inventory_attempts
                    .unwrap_or(defaults.write_protect_inventory_attempts),
                false,
            )?,
            write_protect_inventory_retry_interval: compile_timing_duration(
                "driver.timing.pre_update.write_protect_inventory_retry_interval",
                self.pre_update
                    .write_protect_inventory_retry_interval
                    .as_deref(),
                defaults.write_protect_inventory_retry_interval,
                false,
            )?,
            power_off_delay: compile_timing_duration(
                "driver.timing.activation.power_off_delay",
                self.activation.power_off_delay.as_deref(),
                defaults.power_off_delay,
                true,
            )?,
            ac_cycle_attempts: compile_timing_count(
                "driver.timing.activation.ac_cycle_attempts",
                self.activation
                    .ac_cycle_attempts
                    .unwrap_or(defaults.ac_cycle_attempts),
                false,
            )?,
            ac_cycle_stabilization: compile_timing_duration(
                "driver.timing.activation.ac_cycle_stabilization",
                self.activation.ac_cycle_stabilization.as_deref(),
                defaults.ac_cycle_stabilization,
                true,
            )?,
            activation_command_retries: compile_timing_count(
                "driver.timing.activation.command_retries",
                self.activation
                    .command_retries
                    .unwrap_or(defaults.activation_command_retries),
                true,
            )?,
            activation_command_retry_interval: compile_timing_duration(
                "driver.timing.activation.command_retry_interval",
                self.activation.command_retry_interval.as_deref(),
                defaults.activation_command_retry_interval,
                true,
            )?,
            bmc_recovery_attempts: compile_timing_count(
                "driver.timing.recovery.bmc_attempts",
                self.recovery
                    .bmc_attempts
                    .unwrap_or(defaults.bmc_recovery_attempts),
                false,
            )?,
            bmc_recovery_interval: compile_timing_duration(
                "driver.timing.recovery.bmc_interval",
                self.recovery.bmc_interval.as_deref(),
                defaults.bmc_recovery_interval,
                false,
            )?,
            power_on_attempts: compile_timing_count(
                "driver.timing.recovery.power_on_attempts",
                self.recovery
                    .power_on_attempts
                    .unwrap_or(defaults.power_on_attempts),
                false,
            )?,
            power_on_interval: compile_timing_duration(
                "driver.timing.recovery.power_on_interval",
                self.recovery.power_on_interval.as_deref(),
                defaults.power_on_interval,
                false,
            )?,
            power_on_resend_interval: compile_timing_duration(
                "driver.timing.recovery.power_on_resend_interval",
                self.recovery.power_on_resend_interval.as_deref(),
                defaults.power_on_resend_interval,
                false,
            )?,
            recovery_probe_timeout: compile_timing_duration(
                "driver.timing.recovery.probe_timeout",
                self.recovery.probe_timeout.as_deref(),
                defaults.recovery_probe_timeout,
                false,
            )?,
            inventory_recovery_retries: compile_timing_count(
                "driver.timing.inventory_recovery.retries",
                self.inventory_recovery
                    .retries
                    .unwrap_or(defaults.inventory_recovery_retries),
                true,
            )?,
            inventory_recovery_retry_interval: compile_timing_duration(
                "driver.timing.inventory_recovery.retry_interval",
                self.inventory_recovery.retry_interval.as_deref(),
                defaults.inventory_recovery_retry_interval,
                true,
            )?,
        };
        if timing.background_copy_poll_interval > timing.background_copy_timeout {
            return validation_error(
                "driver.timing.pre_update.background_copy_poll_interval must not exceed background_copy_timeout",
            );
        }
        if timing.power_on_resend_interval < timing.power_on_interval {
            return validation_error(
                "driver.timing.recovery.power_on_resend_interval must be at least power_on_interval",
            );
        }
        Ok(timing)
    }
}

fn compile_timing_duration(
    field: &str,
    value: Option<&str>,
    default: Duration,
    allow_zero: bool,
) -> Result<Duration, HardwareProfileError> {
    let duration = value
        .map(|value| parse_duration(field, value))
        .transpose()?
        .unwrap_or(default);
    if !allow_zero {
        require_nonzero(field, duration)?;
    }
    // NVFWUPD transports accept integer seconds. Never silently truncate milliseconds.
    if duration.subsec_nanos() != 0 {
        return validation_error(format!("{field} must use whole seconds"));
    }
    Ok(duration)
}

fn compile_timing_count(
    field: &str,
    value: u32,
    allow_zero: bool,
) -> Result<u32, HardwareProfileError> {
    if value > 1000 || (!allow_zero && value == 0) {
        return validation_error(format!(
            "{field} must be {}..=1000",
            if allow_zero { 0 } else { 1 }
        ));
    }
    Ok(value)
}

fn validate_redfish_resource_id(
    field: &str,
    value: String,
) -> Result<String, HardwareProfileError> {
    let value = value.trim();
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return validation_error(format!(
            "{field} must contain only ASCII letters, digits, '-', '_' or '.'"
        ));
    }
    Ok(value.to_owned())
}

fn compile_polling(document: PollingDocument) -> Result<PollPolicy, HardwareProfileError> {
    let task_timeout = parse_duration("firmware.polling.task_timeout", &document.task_timeout)?;
    let interval = parse_duration("firmware.polling.interval", &document.interval)?;
    let transient_error_budget = parse_duration(
        "firmware.polling.transient_error_budget",
        &document.transient_error_budget,
    )?;
    require_nonzero("firmware.polling.task_timeout", task_timeout)?;
    require_nonzero("firmware.polling.interval", interval)?;
    if interval > task_timeout {
        return validation_error("firmware.polling.interval must not exceed task_timeout");
    }
    if transient_error_budget > task_timeout {
        return validation_error(
            "firmware.polling.transient_error_budget must not exceed task_timeout",
        );
    }

    Ok(PollPolicy {
        task_timeout,
        interval,
        transient_error_budget,
    })
}

/// The block compiles into the service-wide options type and passes its one
/// validator, so the rule lives in one place for both sources.
fn compile_power_on(
    document: PowerOnDocument,
) -> Result<ComputePowerOnOptions, HardwareProfileError> {
    let timeout = parse_duration("firmware.power_on.timeout", &document.timeout)?;
    let interval = parse_duration("firmware.power_on.interval", &document.interval)?;
    let options = ComputePowerOnOptions { timeout, interval };
    options.validate().map_err(|error| {
        HardwareProfileError::Validation(format!("firmware.power_on: {}", error.message))
    })?;

    Ok(options)
}

fn compile_verification(
    document: VerificationDocument,
) -> Result<VerificationPolicy, HardwareProfileError> {
    let settle_delay =
        parse_duration("firmware.verification.settle_delay", &document.settle_delay)?;
    let retry_interval = parse_duration(
        "firmware.verification.retry_interval",
        &document.retry_interval,
    )?;
    let attempts = NonZeroU32::new(document.attempts).ok_or_else(|| {
        HardwareProfileError::Validation(
            "firmware.verification.attempts must be greater than zero".to_owned(),
        )
    })?;
    if attempts.get() > 1 {
        require_nonzero("firmware.verification.retry_interval", retry_interval)?;
    }

    Ok(VerificationPolicy {
        enabled: document.enabled,
        settle_delay,
        attempts,
        retry_interval,
    })
}

fn compile_target(document: TargetDocument) -> Result<CompiledTarget, HardwareProfileError> {
    let id = validate_identifier("firmware target id", document.id)?;
    let component_key = validate_component_key(&id, document.component_key)?;
    let source_components = validate_unique_nonempty_strings(
        &format!("target {id:?} source_components"),
        document.source_components,
    )?;
    if source_components.is_empty() {
        return validation_error(format!("target {id:?} source_components must not be empty"));
    }

    let mut suffix = validate_optional_matcher(&id, "suffix", document.filename.suffix)?;
    let mut contains = validate_optional_matcher(&id, "contains", document.filename.contains)?;
    if document.filename.case_insensitive {
        suffix = suffix.map(|value| value.to_ascii_lowercase());
        contains = contains.map(|value| value.to_ascii_lowercase());
    }

    let request_targets = validate_unique_nonempty_strings(
        &format!("target {id:?} request_targets"),
        document.request_targets,
    )?;
    let multipart_targets =
        compile_update_parameter_targets(&id, document.update_parameters.as_ref())?;
    let request_target_selection = match (
        document.driver_default_targets,
        request_targets.is_empty(),
        multipart_targets,
    ) {
        (false, true, Some(paths)) => RequestTargetSelection::Multipart(paths),
        (true, true, None) => RequestTargetSelection::DriverDefault,
        (false, false, None) => RequestTargetSelection::Explicit(
            request_targets
                .into_iter()
                .map(|path| parse_redfish_path(&id, &path))
                .collect::<Result<_, _>>()?,
        ),
        (false, true, None) => {
            return validation_error(format!(
                "target {id:?} must configure request_targets, driver_default_targets, or update_parameters.Targets/HttpPushUriTargets"
            ));
        }
        _ => {
            return validation_error(format!(
                "target {id:?} cannot combine request_targets, driver_default_targets, and update_parameters.Targets/HttpPushUriTargets"
            ));
        }
    };

    Ok(CompiledTarget {
        stage: document.stage,
        activate_after: document.activate_after_stage,
        plan: FirmwareTargetPlan {
            id,
            component_key,
            source_components,
            filename: FilenameMatcher {
                suffix,
                contains,
                case_insensitive: document.filename.case_insensitive,
            },
            request_target_selection,
            update_parameters: document.update_parameters,
            oem_parameters: document.oem_parameters,
            default_apply: document.default_apply,
        },
    })
}

fn compile_update_parameter_targets(
    id: &str,
    parameters: Option<&Map<String, Value>>,
) -> Result<Option<Vec<RedfishPath>>, HardwareProfileError> {
    let Some(parameters) = parameters else {
        return Ok(None);
    };
    for (key, valid) in [
        (
            "ForceUpdate",
            parameters.get("ForceUpdate").is_none_or(Value::is_boolean),
        ),
        (
            "ApplyTime",
            parameters.get("ApplyTime").is_none_or(Value::is_string),
        ),
    ] {
        if !valid {
            return validation_error(format!(
                "target {id:?} update_parameters.{key} has an invalid type"
            ));
        }
    }
    if parameters.contains_key("Targets") && parameters.contains_key("HttpPushUriTargets") {
        return validation_error(format!(
            "target {id:?} update_parameters cannot contain both Targets and HttpPushUriTargets"
        ));
    }
    let Some(value) = parameters
        .get("Targets")
        .or_else(|| parameters.get("HttpPushUriTargets"))
    else {
        return Ok(None);
    };
    let paths = value.as_array().ok_or_else(|| {
        HardwareProfileError::Validation(format!(
            "target {id:?} update parameter targets must be an array of Redfish paths"
        ))
    })?;
    let mut unique = BTreeSet::new();
    paths
        .iter()
        .map(|value| {
            let path = value.as_str().ok_or_else(|| {
                HardwareProfileError::Validation(format!(
                    "target {id:?} update parameter targets must contain strings"
                ))
            })?;
            let path = parse_redfish_path(id, path)?;
            if !unique.insert(path.as_str().to_ascii_lowercase()) {
                return validation_error(format!(
                    "target {id:?} update parameter targets must be unique"
                ));
            }
            Ok(path)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn validate_component_key(target_id: &str, value: String) -> Result<String, HardwareProfileError> {
    let value = value.trim();
    if value.is_empty() {
        return validation_error(format!(
            "target {target_id:?} component_key must not be empty"
        ));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return validation_error(format!(
            "target {target_id:?} component_key must contain only ASCII letters, digits, '-' or '_'"
        ));
    }
    Ok(value.to_owned())
}

fn parse_duration(field: &str, value: &str) -> Result<Duration, HardwareProfileError> {
    let value = value.trim();
    let (number, unit) = if let Some(number) = value.strip_suffix("ms") {
        (number, "ms")
    } else if let Some(number) = value.strip_suffix('s') {
        (number, "s")
    } else if let Some(number) = value.strip_suffix('m') {
        (number, "m")
    } else if let Some(number) = value.strip_suffix('h') {
        (number, "h")
    } else {
        return validation_error(format!(
            "{field} must use an integer duration with ms, s, m, or h suffix"
        ));
    };

    let number = number.parse::<u64>().map_err(|_| {
        HardwareProfileError::Validation(format!(
            "{field} must use an integer duration with ms, s, m, or h suffix"
        ))
    })?;
    let duration = match unit {
        "ms" => Duration::from_millis(number),
        "s" => Duration::from_secs(number),
        "m" => Duration::from_secs(number.checked_mul(60).ok_or_else(|| {
            HardwareProfileError::Validation(format!("{field} duration overflows"))
        })?),
        "h" => Duration::from_secs(number.checked_mul(60 * 60).ok_or_else(|| {
            HardwareProfileError::Validation(format!("{field} duration overflows"))
        })?),
        _ => unreachable!("duration unit was selected above"),
    };
    if duration > MAX_CONFIGURED_DURATION {
        return validation_error(format!("{field} must not exceed 24h"));
    }
    Ok(duration)
}

fn require_nonzero(field: &str, duration: Duration) -> Result<(), HardwareProfileError> {
    if duration.is_zero() {
        return validation_error(format!("{field} must be greater than zero"));
    }
    Ok(())
}

fn validate_identifier(field: &str, value: String) -> Result<String, HardwareProfileError> {
    let value = value.trim();
    if value.is_empty()
        || !value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
    {
        return validation_error(format!(
            "{field} must contain only lowercase ASCII letters, digits, and underscores"
        ));
    }
    Ok(value.to_owned())
}

fn validate_unique_nonempty_strings(
    field: &str,
    values: Vec<String>,
) -> Result<Vec<String>, HardwareProfileError> {
    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            return validation_error(format!("{field} contains an empty value"));
        }
        let key = value.to_ascii_lowercase();
        if !seen.insert(key) {
            return validation_error(format!("{field} contains duplicate value {value:?}"));
        }
        normalized.push(value.to_owned());
    }
    Ok(normalized)
}

fn validate_optional_matcher(
    target_id: &str,
    field: &str,
    value: Option<String>,
) -> Result<Option<String>, HardwareProfileError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return validation_error(format!(
            "target {target_id:?} filename.{field} must not be empty"
        ));
    }
    if value.contains(['/', '\\']) {
        return validation_error(format!(
            "target {target_id:?} filename.{field} must not contain a path separator"
        ));
    }
    Ok(Some(value.to_owned()))
}

fn parse_redfish_path(field: &str, value: &str) -> Result<RedfishPath, HardwareProfileError> {
    value
        .parse()
        .map_err(|error: crate::utilities::error::RmsError| {
            HardwareProfileError::Validation(format!("{field} path {value:?}: {}", error.message))
        })
}

fn validation_error<T>(message: impl Into<String>) -> Result<T, HardwareProfileError> {
    Err(HardwareProfileError::Validation(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUPERMICRO_PROFILE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hardware-profiles/v1/compute_gb300_supermicro.yaml"
    ));
    const NVIDIA_GB200_PROFILE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
    ));

    #[test]
    fn profile_hash_identifies_exact_startup_source_bytes() {
        let original = HardwareUpdatePlan::from_yaml_str(NVIDIA_GB200_PROFILE).unwrap();
        assert_eq!(
            original.source_sha256(),
            hex::encode(Sha256::digest(NVIDIA_GB200_PROFILE.as_bytes()))
        );
        let again = HardwareUpdatePlan::from_yaml_str(NVIDIA_GB200_PROFILE).unwrap();
        assert_eq!(original.source_sha256(), again.source_sha256());
        assert_eq!(original.source_sha256(), original.clone().source_sha256());

        for edited in [
            format!("{NVIDIA_GB200_PROFILE}\n# deployment comment\n"),
            NVIDIA_GB200_PROFILE.replace("HGX_Chassis_0", "HGX_Custom_0"),
        ] {
            let plan = HardwareUpdatePlan::from_yaml_str(&edited).unwrap();
            assert_eq!(plan.profile_id(), original.profile_id());
            assert_ne!(plan.source_sha256(), original.source_sha256());
            assert_eq!(
                plan.source_sha256(),
                hex::encode(Sha256::digest(edited.as_bytes()))
            );
        }
    }

    #[test]
    fn profile_hash_matches_embedded_and_external_sources() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("renamed.yaml");
        std::fs::write(&path, NVIDIA_GB200_PROFILE).unwrap();
        let external = HardwareProfileCatalog::load_directory(directory.path()).unwrap();
        let embedded = HardwareProfileCatalog::built_in().unwrap();
        let selected = external
            .resolve_request(NodeType::ComputeGb200Nvidia)
            .unwrap();
        assert_eq!(
            selected.source_sha256(),
            embedded
                .resolve_request(NodeType::ComputeGb200Nvidia)
                .unwrap()
                .source_sha256()
        );
    }

    #[test]
    fn startup_rejects_malformed_firmware_paths() {
        for path in [
            "/redfish/v1/%2e%2e/%2e%2e/other",
            r"/redfish/v1/Chassis/..\..\other",
            "/redfish/v1/Chassis/Bad Path",
        ] {
            let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
            yaml["firmware"]["targets"][0]["request_targets"] =
                serde_yaml::Value::Sequence(vec![serde_yaml::Value::from(path)]);
            let error = HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap())
                .unwrap_err();
            assert!(
                error.to_string().contains("plain same-origin"),
                "{path}: {error}"
            );
        }
    }

    #[test]
    fn omitted_empty_and_null_power_paths_preserve_discovery() {
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
        yaml["driver"]["redfish"]["power"]["bmc_ready_path"] = serde_yaml::Value::Null;
        let explicit =
            HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap()).unwrap();
        let NodeRuntimeConfig::RedfishNvfwupdCompute(runtime) = explicit.runtime_config();
        assert_eq!(
            runtime.power_paths,
            crate::domain::compute_power::ComputePowerPaths::default()
        );
        for empty_section in [false, true] {
            let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
            if empty_section {
                yaml["driver"]["redfish"]["power"] = serde_yaml::from_str("{}").unwrap();
            } else {
                yaml["driver"]["redfish"]
                    .as_mapping_mut()
                    .unwrap()
                    .remove("power");
            }
            let plan =
                HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap()).unwrap();
            assert_eq!(explicit.runtime_config(), plan.runtime_config());
        }
    }

    #[test]
    fn partial_power_override_keeps_other_paths_unset() {
        let document: ComputePowerPathsDocument =
            serde_yaml::from_str("system_reset_path: /redfish/v1/Oem/Reset").unwrap();
        assert_eq!(
            document.compile().unwrap(),
            crate::domain::compute_power::ComputePowerPaths {
                system_reset_path: Some("/redfish/v1/Oem/Reset".parse().unwrap()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn all_power_paths_compile_and_activation_paths_reach_nvfwupd() {
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
        yaml["driver"]["redfish"]["power"]["system_state_path"] =
            serde_yaml::Value::from("/redfish/v1/Oem/system_state_path");
        yaml["driver"]["redfish"]["power"]["system_reset_path"] =
            serde_yaml::Value::from("/redfish/v1/Oem/system_reset_path");
        yaml["driver"]["redfish"]["power"]["bmc_reset_path"] =
            serde_yaml::Value::from("/redfish/v1/Oem/bmc_reset_path");
        yaml["driver"]["redfish"]["power"]["hmc_reset_path"] =
            serde_yaml::Value::from("/redfish/v1/Oem/hmc_reset_path");
        yaml["driver"]["redfish"]["power"]["aux_power_cycle_path"] =
            serde_yaml::Value::from("/redfish/v1/Oem/aux_power_cycle_path");
        yaml["driver"]["redfish"]["power"]["bmc_ready_path"] =
            serde_yaml::Value::from("/redfish/v1/Oem/bmc_ready_path");
        let plan =
            HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap()).unwrap();
        let NodeRuntimeConfig::RedfishNvfwupdCompute(runtime) = plan.runtime_config();
        let paths = &runtime.power_paths;
        assert_eq!(
            paths.system_state_path.as_deref(),
            Some("/redfish/v1/Oem/system_state_path")
        );
        assert_eq!(
            paths.system_reset_path.as_deref(),
            Some("/redfish/v1/Oem/system_reset_path")
        );
        assert_eq!(
            paths.bmc_reset_path.as_deref(),
            Some("/redfish/v1/Oem/bmc_reset_path")
        );
        assert_eq!(
            paths.hmc_reset_path.as_deref(),
            Some("/redfish/v1/Oem/hmc_reset_path")
        );
        assert_eq!(
            paths.aux_power_cycle_path.as_deref(),
            Some("/redfish/v1/Oem/aux_power_cycle_path")
        );
        assert_eq!(
            paths.bmc_ready_path.as_deref(),
            Some("/redfish/v1/Oem/bmc_ready_path")
        );
        let nv_paths = nvfwupd::compute_power::ComputePowerPaths::from(paths);
        assert_eq!(
            nv_paths.system_state_path.as_deref(),
            paths.system_state_path.as_deref()
        );
        assert_eq!(
            nv_paths.system_reset_path.as_deref(),
            paths.system_reset_path.as_deref()
        );
        assert_eq!(
            nv_paths.aux_power_cycle_path.as_deref(),
            paths.aux_power_cycle_path.as_deref()
        );
        assert_eq!(
            nv_paths.bmc_ready_path.as_deref(),
            paths.bmc_ready_path.as_deref()
        );
    }

    #[test]
    fn unsafe_and_unknown_power_paths_are_rejected_at_startup() {
        for field in [
            "system_state_path",
            "system_reset_path",
            "bmc_reset_path",
            "hmc_reset_path",
            "aux_power_cycle_path",
            "bmc_ready_path",
        ] {
            for path in [
                "",
                "https://other.invalid/redfish/v1/Systems/1",
                "//other.invalid/reset",
                "/redfish/v1beta/Reset",
                "/admin",
                "/redfish/v1/../admin",
                "/redfish/v1/./Systems",
                "/redfish/v1//Systems",
                "/redfish/v1/%2e%2e/admin",
                "/redfish/v1/%2fadmin",
                "/redfish/v1/Systems?x=1",
                "/redfish/v1/Systems#fragment",
                "/redfish/v1/Systems\\\\Other",
                "/redfish/v1/Systems/ bad",
                " /redfish/v1/Systems",
                "/redfish/v1/Systems\n",
            ] {
                let mut yaml: serde_yaml::Value =
                    serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
                yaml["driver"]["redfish"]["power"][field] = serde_yaml::Value::from(path);
                let error =
                    HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap())
                        .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains(&format!("driver.redfish.power.{field}")),
                    "{path:?}: {error}"
                );
            }
        }
        let source = NVIDIA_GB200_PROFILE.replacen("system_reset_path:", "unknown_path:", 1);
        assert!(
            HardwareUpdatePlan::from_yaml_str(&source)
                .unwrap_err()
                .to_string()
                .contains("unknown_path")
        );
    }

    #[test]
    fn omitted_and_explicit_compute_timings_preserve_defaults() {
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
        let explicit = HardwareUpdatePlan::from_yaml_str(NVIDIA_GB200_PROFILE).unwrap();
        yaml["driver"].as_mapping_mut().unwrap().remove("timing");
        for name in ["connect_timeout", "request_timeout", "upload_timeout"] {
            yaml["driver"]["redfish"]
                .as_mapping_mut()
                .unwrap()
                .remove(name);
        }
        let omitted =
            HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap()).unwrap();
        assert_eq!(explicit.runtime_config(), omitted.runtime_config());
        let NodeRuntimeConfig::RedfishNvfwupdCompute(runtime) = explicit.runtime_config();
        assert_eq!(
            runtime.firmware_timing,
            crate::domain::compute_timing::ComputeFirmwareTiming::default()
        );
        assert_eq!(
            nvfwupd::compute_timing::ComputeFirmwareTiming::from(runtime.firmware_timing),
            nvfwupd::compute_timing::ComputeFirmwareTiming::default()
        );
    }

    #[test]
    fn every_compute_timing_reaches_the_nvfwupd_policy() {
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
        yaml["driver"]["timing"]["transport"]["connect_timeout"] = serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["transport"]["request_timeout"] = serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["transport"]["upload_timeout"] = serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["transport"]["reachability_timeout"] =
            serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["pre_update"]["query_timeout"] = serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["pre_update"]["background_copy_timeout"] =
            serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["pre_update"]["background_copy_poll_interval"] =
            serde_yaml::Value::from("5s");
        yaml["driver"]["timing"]["pre_update"]["write_protect_inventory_attempts"] =
            serde_yaml::Value::from(2);
        yaml["driver"]["timing"]["pre_update"]["write_protect_inventory_retry_interval"] =
            serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["activation"]["power_off_delay"] = serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["activation"]["ac_cycle_attempts"] = serde_yaml::Value::from(4);
        yaml["driver"]["timing"]["activation"]["ac_cycle_stabilization"] =
            serde_yaml::Value::from("23s");
        yaml["driver"]["timing"]["activation"]["command_retries"] = serde_yaml::Value::from(2);
        yaml["driver"]["timing"]["activation"]["command_retry_interval"] =
            serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["recovery"]["bmc_attempts"] = serde_yaml::Value::from(2);
        yaml["driver"]["timing"]["recovery"]["bmc_interval"] = serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["recovery"]["power_on_attempts"] = serde_yaml::Value::from(2);
        yaml["driver"]["timing"]["recovery"]["power_on_interval"] = serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["recovery"]["power_on_resend_interval"] =
            serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["recovery"]["probe_timeout"] = serde_yaml::Value::from("17s");
        yaml["driver"]["timing"]["inventory_recovery"]["retries"] = serde_yaml::Value::from(2);
        yaml["driver"]["timing"]["inventory_recovery"]["retry_interval"] =
            serde_yaml::Value::from("17s");
        for name in ["connect_timeout", "request_timeout", "upload_timeout"] {
            yaml["driver"]["redfish"][name] = serde_yaml::Value::from("19s");
        }
        let plan =
            HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap()).unwrap();
        let NodeRuntimeConfig::RedfishNvfwupdCompute(runtime) = plan.runtime_config();
        let timing = nvfwupd::compute_timing::ComputeFirmwareTiming::from(runtime.firmware_timing);
        assert_eq!(timing.connect_timeout, Duration::from_secs(17));
        assert_eq!(timing.request_timeout, Duration::from_secs(17));
        assert_eq!(timing.upload_timeout, Duration::from_secs(17));
        assert_eq!(timing.reachability_timeout, Duration::from_secs(17));
        assert_eq!(timing.precondition_query_timeout, Duration::from_secs(17));
        assert_eq!(timing.background_copy_timeout, Duration::from_secs(17));
        assert_eq!(timing.background_copy_poll_interval, Duration::from_secs(5));
        assert_eq!(timing.write_protect_inventory_attempts, 2);
        assert_eq!(
            timing.write_protect_inventory_retry_interval,
            Duration::from_secs(17)
        );
        assert_eq!(timing.power_off_delay, Duration::from_secs(17));
        assert_eq!(timing.ac_cycle_attempts, 4);
        assert_eq!(timing.ac_cycle_stabilization, Duration::from_secs(23));
        assert_eq!(timing.activation_command_retries, 2);
        assert_eq!(
            timing.activation_command_retry_interval,
            Duration::from_secs(17)
        );
        assert_eq!(timing.bmc_recovery_attempts, 2);
        assert_eq!(timing.bmc_recovery_interval, Duration::from_secs(17));
        assert_eq!(timing.power_on_attempts, 2);
        assert_eq!(timing.power_on_interval, Duration::from_secs(17));
        assert_eq!(timing.power_on_resend_interval, Duration::from_secs(17));
        assert_eq!(timing.recovery_probe_timeout, Duration::from_secs(17));
        assert_eq!(timing.inventory_recovery_retries, 2);
        assert_eq!(
            timing.inventory_recovery_retry_interval,
            Duration::from_secs(17)
        );
        assert_eq!(runtime.redfish_connect_timeout, Duration::from_secs(19));
        assert_eq!(runtime.redfish_request_timeout, Duration::from_secs(19));
        assert_eq!(runtime.redfish_upload_timeout, Duration::from_secs(19));
    }

    #[test]
    fn invalid_compute_timings_are_rejected_at_startup() {
        for (group, field, value) in [
            ("transport", "request_timeout", "0s"),
            ("transport", "upload_timeout", "1500ms"),
            ("transport", "connect_timeout", "25h"),
            ("activation", "ac_cycle_stabilization", "25h"),
            ("activation", "ac_cycle_stabilization", "1500ms"),
            ("pre_update", "background_copy_poll_interval", "16m"),
            ("recovery", "power_on_resend_interval", "1s"),
        ] {
            let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
            yaml["driver"]["timing"][group][field] = serde_yaml::Value::from(value);
            let error = HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap())
                .unwrap_err();
            let path = format!("driver.timing.{group}.{field}");
            assert!(error.to_string().contains(&path), "{path}: {error}");
        }
        for (group, field, value) in [
            ("recovery", "bmc_attempts", 0),
            ("activation", "command_retries", 1001),
            ("activation", "ac_cycle_attempts", 0),
            ("activation", "ac_cycle_attempts", 1001),
        ] {
            let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
            yaml["driver"]["timing"][group][field] = serde_yaml::Value::from(value);
            let error = HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap())
                .unwrap_err();
            let path = format!("driver.timing.{group}.{field}");
            assert!(error.to_string().contains(&path), "{path}: {error}");
        }
    }

    #[test]
    fn timing_sections_and_fields_can_be_omitted_independently() {
        use crate::domain::compute_timing::ComputeFirmwareTiming;

        for group in [
            "transport",
            "pre_update",
            "activation",
            "recovery",
            "inventory_recovery",
        ] {
            let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
            yaml["driver"]["timing"]
                .as_mapping_mut()
                .unwrap()
                .remove(group);
            let plan =
                HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap()).unwrap();
            let NodeRuntimeConfig::RedfishNvfwupdCompute(runtime) = plan.runtime_config();
            assert_eq!(
                runtime.firmware_timing,
                ComputeFirmwareTiming::default(),
                "{group}"
            );
        }

        let document: ComputeTimingDocument = serde_yaml::from_str(
            r"
transport:
  upload_timeout: 17s
pre_update:
  query_timeout: 18s
activation:
  power_off_delay: 19s
recovery:
  probe_timeout: 20s
inventory_recovery:
  retry_interval: 21s
",
        )
        .unwrap();
        assert_eq!(
            document.compile().unwrap(),
            ComputeFirmwareTiming {
                upload_timeout: Duration::from_secs(17),
                precondition_query_timeout: Duration::from_secs(18),
                power_off_delay: Duration::from_secs(19),
                recovery_probe_timeout: Duration::from_secs(20),
                inventory_recovery_retry_interval: Duration::from_secs(21),
                ..ComputeFirmwareTiming::default()
            }
        );
    }

    #[test]
    fn zero_retries_and_optional_delays_are_valid() {
        let document: ComputeTimingDocument = serde_yaml::from_str(
            r"
activation:
  command_retries: 0
  command_retry_interval: 0s
  power_off_delay: 0s
  ac_cycle_stabilization: 0s
inventory_recovery:
  retries: 0
  retry_interval: 0s
",
        )
        .unwrap();
        let timing = document.compile().unwrap();
        assert_eq!(timing.activation_command_retries, 0);
        assert_eq!(timing.activation_command_retry_interval, Duration::ZERO);
        assert_eq!(timing.inventory_recovery_retries, 0);
        assert_eq!(timing.inventory_recovery_retry_interval, Duration::ZERO);
        assert_eq!(timing.power_off_delay, Duration::ZERO);
        assert_eq!(timing.ac_cycle_stabilization, Duration::ZERO);
    }

    #[test]
    fn nvidia_gb200_profile_compiles_driver_default_bmc_targeting() {
        let plan =
            HardwareUpdatePlan::from_yaml_str(NVIDIA_GB200_PROFILE).expect("valid GB200 profile");

        assert_eq!(plan.profile_id(), "compute_gb200_nvidia");
        assert_eq!(plan.node_type(), NodeType::ComputeGb200Nvidia);
        assert!(plan.request_matching_enabled());
        let NodeRuntimeConfig::RedfishNvfwupdCompute(driver) = plan.runtime_config();
        assert_eq!(
            driver.nvfwupd_server_profile,
            NvfwupdServerProfile::ComputeGb200
        );
        assert_eq!(driver.computer_system_id.as_ref(), "System_0");
        assert_eq!(driver.bmc_manager_id.as_ref(), "BMC_0");
        assert_eq!(driver.hmc_manager_id.as_ref(), "HGX_BMC_0");
        assert_eq!(driver.device_info_timeout, Duration::from_secs(5 * 60));
        assert_eq!(plan.stages().len(), 1);
        assert_eq!(plan.stages()[0].targets().len(), 22);
        assert!(plan.stages()[0].activate_after());
        assert_eq!(plan.stages()[0].targets()[0].id(), "hmc");
        assert_eq!(plan.stages()[0].targets()[0].component_key(), "HMC");
        assert!(plan.stages()[0].targets()[0].default_apply());
        assert_eq!(
            plan.stages()[0].targets()[0]
                .request_targets()
                .iter()
                .map(RedfishPath::as_str)
                .collect::<Vec<_>>(),
            ["/redfish/v1/Chassis/HGX_Chassis_0"]
        );
        let bmc = plan.stages()[0].targets().last().expect("BMC target");
        assert_eq!(bmc.id(), "bmc");
        assert_eq!(
            bmc.request_target_selection(),
            &RequestTargetSelection::DriverDefault
        );
        assert!(bmc.default_apply());
        assert!(
            plan.stages()[0].targets()[1..21]
                .iter()
                .all(|target| !target.default_apply())
        );
    }

    #[test]
    fn omitted_or_empty_filename_selector_accepts_any_filename() {
        for source in [
            NVIDIA_GB200_PROFILE.to_owned(),
            NVIDIA_GB200_PROFILE.replacen("    - id: hmc", "    - id: hmc\n      filename: {}", 1),
        ] {
            let plan = HardwareUpdatePlan::from_yaml_str(&source).unwrap();
            for target in plan.stages()[0].targets() {
                assert_eq!(target.filename().suffix(), None);
                assert_eq!(target.filename().contains(), None);
                for filename in [
                    "firmware.fwpkg",
                    "firmware.bin",
                    "firmware",
                    "firmware.FWPKG",
                ] {
                    assert!(
                        target.filename().matches(filename),
                        "{}: {filename}",
                        target.id()
                    );
                }
            }
        }
    }

    #[test]
    fn explicit_filename_selector_still_requires_all_configured_predicates() {
        let plan = HardwareUpdatePlan::from_yaml_str(SUPERMICRO_PROFILE).unwrap();
        let matcher = plan.stages()[0].targets()[0].filename();
        assert!(matcher.matches("firmware-nosbios.fwpkg"));
        assert!(!matcher.matches("firmware-nosbios.bin"));
        assert!(!matcher.matches("firmware.fwpkg"));
    }

    #[test]
    fn supermicro_profile_compiles_to_ordered_typed_stages() {
        let plan = HardwareUpdatePlan::from_yaml_str(SUPERMICRO_PROFILE).expect("valid profile");

        assert_eq!(plan.schema_version(), 1);
        assert_eq!(plan.profile_id(), "compute_gb300_supermicro");
        assert_eq!(plan.node_type(), NodeType::ComputeGb300Supermicro);
        assert!(plan.request_matching_enabled());
        assert_eq!(
            plan.activation(),
            Some(ActivationStrategy::FullComputePowerCycle)
        );
        assert_eq!(
            plan.stages()
                .iter()
                .map(FirmwareUpdateStage::number)
                .collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert_eq!(plan.stages()[0].targets()[0].id(), "nosbios");
        assert!(plan.stages()[0].activate_after());
        assert_eq!(plan.stages()[1].targets()[0].id(), "bios");
        assert!(plan.stages()[1].activate_after());
        assert_eq!(plan.stages()[2].targets()[0].id(), "host_bmc");
        assert!(!plan.stages()[2].activate_after());
        assert_eq!(plan.polling().task_timeout(), Duration::from_secs(90 * 60));
        assert!(
            plan.stages()[2].targets()[0]
                .filename()
                .matches("host-obmc-1.2.3.bin")
        );
    }

    #[test]
    fn duplicate_target_id_is_rejected() {
        let source = SUPERMICRO_PROFILE.replacen("    - id: bios", "    - id: nosbios", 1);

        let error = HardwareUpdatePlan::from_yaml_str(&source).expect_err("duplicate must fail");

        assert!(error.to_string().contains("duplicate firmware target id"));
    }

    #[test]
    fn conflicting_activation_flags_in_one_stage_are_rejected() {
        let mut yaml: serde_yaml::Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
        yaml["firmware"]["targets"][1]["activate_after_stage"] = serde_yaml::Value::from(false);

        let error = HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap())
            .expect_err("one stage cannot have conflicting activation boundaries");

        assert!(
            error
                .to_string()
                .contains("must use the same activate_after_stage value"),
            "{error}"
        );
    }

    #[test]
    fn unsafe_request_target_is_rejected() {
        let source = SUPERMICRO_PROFILE.replacen(
            "/redfish/v1/Chassis/HGX_Chassis_0",
            "https://example.invalid/redfish/v1/Chassis/HGX_Chassis_0",
            1,
        );

        let error = HardwareUpdatePlan::from_yaml_str(&source).expect_err("URI must fail");

        assert!(error.to_string().contains("same-origin /redfish/v1 path"));
    }

    #[test]
    fn driver_default_and_explicit_targets_are_mutually_exclusive() {
        let source = NVIDIA_GB200_PROFILE.replacen(
            "      driver_default_targets: true",
            "      driver_default_targets: true\n      request_targets:\n        - /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0",
            1,
        );

        let error =
            HardwareUpdatePlan::from_yaml_str(&source).expect_err("ambiguous targeting must fail");

        assert!(error.to_string().contains("cannot combine"));
    }

    /// The shipped profile plus a `firmware.power_on` block with the given body.
    fn profile_with_power_on(body: &str) -> String {
        NVIDIA_GB200_PROFILE.replacen(
            "\nfirmware:\n  polling:\n",
            &format!("\nfirmware:\n  power_on:\n{body}  polling:\n"),
            1,
        )
    }

    #[test]
    fn shipped_profiles_carry_no_power_on_block() {
        for (filename, source) in BUILT_IN_HARDWARE_PROFILES {
            let plan = HardwareUpdatePlan::from_yaml_str(source).unwrap();
            assert_eq!(plan.power_on(), None, "{filename} carries a power_on block");
        }
    }

    #[test]
    fn power_on_block_compiles_to_its_bounds() {
        let plan = HardwareUpdatePlan::from_yaml_str(&profile_with_power_on(
            "    timeout: 2m\n    interval: 10s\n",
        ))
        .unwrap();

        let power_on = plan.power_on().expect("the block compiles");
        assert_eq!(power_on.timeout, Duration::from_secs(120));
        assert_eq!(power_on.interval, Duration::from_secs(10));
    }

    #[test]
    fn power_on_interval_cannot_exceed_timeout() {
        let error = HardwareUpdatePlan::from_yaml_str(&profile_with_power_on(
            "    timeout: 1s\n    interval: 5s\n",
        ))
        .expect_err("interval must fail");

        assert!(matches!(error, HardwareProfileError::Validation(_)));
    }

    #[test]
    fn power_on_block_rejects_a_zero_bound() {
        for body in [
            "    timeout: 0s\n    interval: 0s\n",
            "    timeout: 1m\n    interval: 0s\n",
        ] {
            let error = HardwareUpdatePlan::from_yaml_str(&profile_with_power_on(body))
                .expect_err("a zero bound must fail");

            assert!(matches!(error, HardwareProfileError::Validation(_)));
        }
    }

    #[test]
    fn power_on_block_rejects_unknown_fields() {
        let error = HardwareUpdatePlan::from_yaml_str(&profile_with_power_on(
            "    timeout: 1m\n    interval: 5s\n    unknown_field: true\n",
        ))
        .expect_err("unknown fields must be rejected");

        assert!(matches!(error, HardwareProfileError::Parse(_)));
    }

    #[test]
    fn polling_interval_cannot_exceed_timeout() {
        let source = SUPERMICRO_PROFILE.replacen("\n    interval: 5s", "\n    interval: 91m", 1);

        let error = HardwareUpdatePlan::from_yaml_str(&source).expect_err("interval must fail");

        assert!(error.to_string().contains("must not exceed task_timeout"));
    }

    #[test]
    fn schema_v1_rejects_a_non_compute_request_match() {
        let source = NVIDIA_GB200_PROFILE.replacen(
            "node_type: compute_gb200_nvidia",
            "node_type: switch_gb200_nvidia",
            1,
        );

        let error = HardwareUpdatePlan::from_yaml_str(&source)
            .expect_err("schema version 1 must reject switch selector");

        assert!(error.to_string().contains("does not support switch nodes"));
    }

    #[test]
    fn unknown_fields_are_rejected() {
        for section in [
            "",
            "/driver",
            "/driver/timing",
            "/driver/timing/transport",
            "/driver/timing/pre_update",
            "/driver/timing/activation",
            "/driver/timing/recovery",
            "/driver/timing/inventory_recovery",
            "/firmware",
            "/firmware/targets/0",
        ] {
            let mut yaml: Value = serde_yaml::from_str(NVIDIA_GB200_PROFILE).unwrap();
            yaml.pointer_mut(section).unwrap()["unknown_field"] = Value::Bool(true);
            let error = HardwareUpdatePlan::from_yaml_str(&serde_yaml::to_string(&yaml).unwrap())
                .expect_err("unknown fields must be rejected");
            assert!(matches!(error, HardwareProfileError::Parse(_)));
            assert!(
                error.to_string().contains("unknown field: unknown_field"),
                "{section}: {error}"
            );
        }
    }

    #[test]
    fn catalog_loads_yaml_files_and_ignores_other_files() {
        let directory = tempfile::tempdir().expect("temporary directory");
        fs::write(
            directory.path().join("profile.yaml"),
            SUPERMICRO_PROFILE.replacen("enabled: true", "enabled: false", 1),
        )
        .expect("write profile");
        fs::write(directory.path().join("README.md"), "not a profile").expect("write ignored file");

        let catalog =
            HardwareProfileCatalog::load_directory(directory.path()).expect("load profile catalog");

        assert_eq!(catalog.len(), 1);
        assert_eq!(
            catalog
                .by_node_type(NodeType::ComputeGb300Supermicro)
                .expect("profile by node type")
                .profile_id(),
            "compute_gb300_supermicro"
        );
        assert!(catalog.by_profile_id("compute_gb300_supermicro").is_some());
        assert!(
            catalog
                .resolve_request(NodeType::ComputeGb300Supermicro)
                .is_none()
        );
    }

    #[test]
    fn request_node_type_resolves_only_an_enabled_matching_profile() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("gb200.yaml"), NVIDIA_GB200_PROFILE).unwrap();
        fs::write(
            directory.path().join("supermicro.yaml"),
            SUPERMICRO_PROFILE.replacen("enabled: true", "enabled: false", 1),
        )
        .unwrap();
        let catalog = HardwareProfileCatalog::load_directory(directory.path()).unwrap();

        let profile = catalog
            .resolve_request(NodeType::ComputeGb200Nvidia)
            .expect("active GB200 request profile");

        assert_eq!(profile.profile_id(), "compute_gb200_nvidia");
        assert!(
            catalog
                .runtime_config_for_request(NodeType::ComputeGb200Nvidia)
                .is_some()
        );
        assert!(
            catalog
                .resolve_request(NodeType::ComputeGb300Supermicro)
                .is_none()
        );
        assert!(
            catalog
                .runtime_config_for_request(NodeType::ComputeGb300Supermicro)
                .is_none()
        );
    }

    #[test]
    fn built_in_profiles_cover_all_supported_compute_trays() {
        let catalog = HardwareProfileCatalog::built_in().unwrap();
        let compute_types = [
            NodeType::ComputeGb200Nvidia,
            NodeType::ComputeGb200Wiwynn,
            NodeType::ComputeGb200Dell,
            NodeType::ComputeGb300Nvidia,
            NodeType::ComputeGb300Lenovo,
            NodeType::ComputeGb300Supermicro,
            NodeType::ComputeVrnvl72Nvidia,
        ];
        assert_eq!(catalog.len(), compute_types.len());
        for node_type in compute_types {
            let profile = catalog
                .resolve_request(node_type)
                .unwrap_or_else(|| panic!("missing profile for {node_type:?}"));
            assert_eq!(profile.profile_id(), node_type.as_str());
            let NodeRuntimeConfig::RedfishNvfwupdCompute(runtime) = profile.runtime_config();
            assert_eq!(
                runtime.nvfwupd_server_profile,
                node_type.nvfwupd_server_profile()
            );
            assert_eq!(runtime.firmware_timing, Default::default());
            assert_eq!(
                runtime.power_paths,
                crate::domain::compute_power::ComputePowerPaths {
                    bmc_ready_path: Some("/redfish/v1".parse().unwrap()),
                    ..Default::default()
                }
            );
        }
        assert!(
            catalog
                .resolve_request(NodeType::SwitchGb200Nvidia)
                .is_none()
        );
        assert!(
            catalog
                .resolve_request(NodeType::PowershelfGb200Liteon)
                .is_none()
        );
    }

    #[test]
    fn filename_case_insensitivity_is_opt_in() {
        let source = SUPERMICRO_PROFILE.replace("contains: nosbios", "contains: NOSBIOS");
        let insensitive = HardwareUpdatePlan::from_yaml_str(&source).unwrap();
        assert!(
            insensitive.stages()[0].targets()[0]
                .filename()
                .matches("UPDATE_NOSBIOS.FWPKG")
        );
        assert!(
            insensitive.stages()[0].targets()[0]
                .filename()
                .matches("update_nosbios.fwpkg")
        );
        let sensitive =
            HardwareUpdatePlan::from_yaml_str(&source.replace(", case_insensitive: true", ""))
                .unwrap();
        assert!(
            !sensitive.stages()[0].targets()[0]
                .filename()
                .matches("update_nosbios.fwpkg")
        );
        assert!(
            sensitive.stages()[0].targets()[0]
                .filename()
                .matches("update_NOSBIOS.fwpkg")
        );
    }

    #[test]
    fn built_in_catalog_contains_every_build_discovered_profile() {
        let catalog = HardwareProfileCatalog::built_in().expect("built-in profiles");

        assert_eq!(catalog.len(), BUILT_IN_HARDWARE_PROFILES.len());
        assert!(
            BUILT_IN_HARDWARE_PROFILES
                .iter()
                .any(|(filename, _)| { *filename == "compute_gb200_nvidia.yaml" })
        );
    }
    #[test]
    fn dell_uses_embedded_defaults_and_honors_enabled_overrides() {
        let catalog = HardwareProfileCatalog::default();
        let node_type = NodeType::ComputeGb200Dell;
        assert_eq!(
            catalog.resolve_request(node_type).unwrap(),
            built_in_profile(node_type).unwrap()
        );
        assert!(catalog.shared_for_request(node_type).is_some());
        let source = include_str!("../../../../hardware-profiles/v1/compute_gb200_dell.yaml");
        let directory = tempfile::tempdir().unwrap();
        for enabled in [true, false] {
            std::fs::write(
                directory.path().join("dell.yaml"),
                source
                    .replace(
                        "profile_id: compute_gb200_dell",
                        "profile_id: operator_dell",
                    )
                    .replace("enabled: true", &format!("enabled: {enabled}")),
            )
            .unwrap();
            let catalog = HardwareProfileCatalog::load_directory(directory.path()).unwrap();
            let expected = if enabled {
                "operator_dell"
            } else {
                "compute_gb200_dell"
            };
            assert_eq!(
                catalog.resolve_request(node_type).unwrap().profile_id(),
                expected
            );
            assert_eq!(
                catalog.shared_for_request(node_type).unwrap().profile_id(),
                expected
            );
        }
    }
}
