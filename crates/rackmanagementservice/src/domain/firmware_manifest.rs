// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed input for the sanitized release JSON in `sample_firmware_manifest`.
//!
//! This module only reads metadata. It does not select payloads, infer hardware
//! targets, download files, or collapse boards into a role-keyed lookup table.
//! Missing download locations are valid metadata, not permission to skip an update.

use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use thiserror::Error;

/// One release catalog, retaining the JSON's board/component hierarchy and order.
///
/// # Examples
///
/// ```
/// use rackmanagementservice::domain::firmware_manifest::FirmwareManifest;
///
/// let manifest: FirmwareManifest = r#"{
///   "ProductName": "DGXGB200",
///   "Milestones": [{"Name": "1.3.6", "BoardSKUs": []}]
/// }"#.parse()?;
/// assert_eq!(manifest.single_milestone()?.name, "1.3.6");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FirmwareManifest {
    /// Product label from the manifest, not an RMS node type.
    #[serde(deserialize_with = "nonempty_string")]
    pub product_name: String,
    /// RMS firmware objects currently require exactly one milestone. This DTO
    /// also supports direct deserialization/construction, so consumers retain
    /// the cardinality check until a separate validated representation exists.
    pub milestones: Vec<Milestone>,
}

/// Release metadata and its ordered, distinct boards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Milestone {
    /// Release name; combined with the product name by the object API.
    #[serde(deserialize_with = "nonempty_string")]
    pub name: String,
    /// Informational lifecycle state; parsing does not require `Released`.
    pub state: Option<String>,
    /// Boards stay distinct even when they share a role or component names.
    #[serde(rename = "BoardSKUs")]
    pub board_skus: Vec<BoardSku>,
}

/// A board entry. Sanitized manifests need not contain a numeric SKU identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BoardSku {
    /// Optional vendor SKU, preserved without interpretation.
    #[serde(rename = "SKUID")]
    pub sku_id: Option<String>,
    /// Informational board name, when supplied.
    #[serde(default)]
    pub name: String,
    /// Board role, such as `Compute Node` or `Switch Tray`.
    #[serde(rename = "Type", deserialize_with = "nonempty_string")]
    pub board_type: String,
    /// Software and firmware remain separate collections.
    pub components: Components,
}

/// The two component collections in the sanitized format.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Components {
    /// Software, including NVOS and metadata-only host tools.
    #[serde(default)]
    pub software: Vec<Component>,
    /// Firmware, including bundles described only by subcomponent versions.
    #[serde(default)]
    pub firmware: Vec<Component>,
}

/// A component record shared by the software and firmware collections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Component {
    /// Optional LinkX rollout policy; validated only when that update is selected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linkx_rollout: Option<Value>,
    /// Manifest component name, not a device target or a synthetic lookup key.
    #[serde(deserialize_with = "nonempty_string")]
    pub component: String,
    /// Bundle version. An empty version is valid when subcomponents carry it.
    pub version: Option<String>,
    /// Release channel, such as `Prod`; no default channel is inferred here.
    #[serde(rename = "Type")]
    pub channel: Option<String>,
    /// Optional legacy bundle label still consumed by the update API.
    pub bundle: Option<String>,
    /// Advertised filenames, including documentation and recovery artifacts.
    #[serde(default)]
    pub file_names: Vec<String>,
    /// All locations, including null URLs and non-update artifacts.
    #[serde(default)]
    pub locations: Vec<Location>,
    /// Per-component expected versions, in manifest order.
    #[serde(rename = "SubComponents", default)]
    pub subcomponents: Vec<SubComponent>,
}

/// One advertised file and its optional source metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Location {
    /// Download URL or local path; null is common for sanitized metadata.
    pub location: Option<String>,
    /// Transport label; interpreted by the downloader, not the parser.
    pub location_type: Option<String>,
    /// Optional package label used by the switch image API.
    pub package_name: Option<String>,
    /// Artifact kind (`Firmware`, `Certificate`, `Firmware-Recovery`, etc.).
    #[serde(rename = "Type")]
    pub artifact_type: Option<String>,
    /// Advertised filename, independent of URL basename or query parameters.
    pub file_name: Option<String>,
    /// Optional checksum. A present value must be a SHA-256 hex string.
    #[serde(default, deserialize_with = "optional_sha256")]
    pub sha256: Option<String>,
    /// Optional update policy hint. Absence/null means required to existing callers.
    /// Preserve null versus explicit values in serialization: it contributes to
    /// persisted v1 manifest revisions and artifact identities.
    pub required: Option<bool>,
}

/// Expected firmware inventory metadata, independent of any download location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SubComponent {
    /// Component or component-instance name.
    #[serde(deserialize_with = "nonempty_string")]
    pub component: String,
    /// Expected version; an empty string means the sanitized source has no version.
    pub version: String,
    /// Optional vendor SKU used to disambiguate inventory entries.
    #[serde(rename = "SKUID")]
    pub sku_id: Option<String>,
    /// Informational subcomponent type; commonly null in sanitized manifests.
    #[serde(rename = "Type")]
    pub component_type: Option<String>,
}

/// A malformed field (with its JSON path), syntax error, or release-selection error.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// A field has the wrong shape, type, or value.
    #[error("invalid firmware manifest at {0}")]
    Field(#[from] serde_path_to_error::Error<serde_json::Error>),
    /// Trailing non-whitespace data follows a complete manifest.
    #[error("invalid firmware manifest: {0}")]
    TrailingData(#[source] serde_json::Error),
    /// The object API does not have a milestone selector.
    #[error("Milestones must contain exactly one milestone; found {0}")]
    MilestoneCount(usize),
}

impl FirmwareManifest {
    /// Return the one release supported by the firmware-object API.
    ///
    /// # Errors
    ///
    /// Returns [`ManifestError::MilestoneCount`] for zero or multiple releases.
    pub fn single_milestone(&self) -> Result<&Milestone, ManifestError> {
        match self.milestones.as_slice() {
            [milestone] => Ok(milestone),
            milestones => Err(ManifestError::MilestoneCount(milestones.len())),
        }
    }
}

impl FromStr for FirmwareManifest {
    type Err = ManifestError;

    fn from_str(json: &str) -> Result<Self, Self::Err> {
        let mut deserializer = serde_json::Deserializer::from_str(json);
        let manifest: Self = serde_path_to_error::deserialize(&mut deserializer)?;
        deserializer.end().map_err(ManifestError::TrailingData)?;
        manifest.single_milestone()?;
        Ok(manifest)
    }
}

impl TryFrom<&Value> for FirmwareManifest {
    type Error = ManifestError;

    fn try_from(json: &Value) -> Result<Self, Self::Error> {
        let manifest: Self = serde_path_to_error::deserialize(json)?;
        manifest.single_milestone()?;
        Ok(manifest)
    }
}

fn nonempty_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    if value.trim().is_empty() {
        return Err(de::Error::custom("expected a nonempty string"));
    }
    Ok(value)
}

fn optional_sha256<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|value| {
            let digest = value.trim();
            if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(de::Error::custom(
                    "invalid Sha256: expected 64 hex characters",
                ));
            }
            Ok(digest.to_ascii_lowercase())
        })
        .transpose()
}

#[cfg(test)]
mod tests;
