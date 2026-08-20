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

//! Types and endpoint helpers for the NVUE `/system` API area.

use std::collections::BTreeMap;

use crate::action::SimpleAction;
use crate::uri::{path_segment, query_value};
use crate::{JsonNumber, NVUE_V1_SERVER};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Serialize};

/// NVOS partition 1 identifier.
pub const NVOS_PARTITION_1_ID: &str = "partition1";

/// NVOS partition 2 identifier.
pub const NVOS_PARTITION_2_ID: &str = "partition2";

/// Full endpoint for `GET /system`.
pub const SYSTEM_ENDPOINT: &str = "/nvue_v1/system";

/// Full endpoint for `POST /system/factory-default`.
pub const SYSTEM_FACTORY_DEFAULT_ENDPOINT: &str = "/nvue_v1/system/factory-default";

/// Full endpoint for `GET /system/image`.
pub const SYSTEM_IMAGE_ENDPOINT: &str = "/nvue_v1/system/image";

/// Full endpoint for `GET /system/image/files`.
pub const SYSTEM_IMAGE_FILES_ENDPOINT: &str = "/nvue_v1/system/image/files";

/// Full endpoint for `GET/PATCH /system/gnmi-server`.
pub const GNMI_SERVER_ENDPOINT: &str = "/nvue_v1/system/gnmi-server";

/// Request body for a forced full NVOS factory-default reset.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SystemFactoryDefaultResetRequest {
    #[serde(rename = "@reset")]
    reset: SimpleAction<FactoryDefaultResetParameters>,
}

impl SystemFactoryDefaultResetRequest {
    /// Creates the fixed request accepted by `POST /nvue_v1/system/factory-default`.
    pub fn start() -> Self {
        Self {
            reset: SimpleAction::start(FactoryDefaultResetParameters { force: true }),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct FactoryDefaultResetParameters {
    force: bool,
}

/// Builds the endpoint for `/system/image/files/{image}`.
pub fn system_image_file_endpoint(image: &str) -> String {
    format!(
        "{NVUE_V1_SERVER}/system/image/files/{}",
        path_segment(image)
    )
}

/// Builds the endpoint for `/system/aaa/user/{user-id}`.
pub fn user_endpoint(user_id: &str) -> String {
    format!("{NVUE_V1_SERVER}/system/aaa/user/{}", path_segment(user_id))
}

/// Builds `/system/aaa/user/{user-id}?rev={revision-id}` for password changes.
pub fn user_endpoint_for_revision(user_id: &str, revision_id: &str) -> String {
    format!(
        "{}?rev={}",
        user_endpoint(user_id),
        query_value(revision_id)
    )
}

/// Builds `/system/gnmi-server?rev={revision-id}` for gNMI configuration.
pub fn gnmi_server_endpoint_for_revision(revision_id: &str) -> String {
    format!("{GNMI_SERVER_ENDPOINT}?rev={}", query_value(revision_id))
}

/// Request body for `POST /system` power-cycle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemPowerCycleRequest {
    /// Power-cycle action.
    #[serde(rename = "@power-cycle")]
    pub power_cycle: SimpleAction<PowerCycleParameters>,
}

impl SystemPowerCycleRequest {
    /// Create a power-cycle request.
    pub fn new(force: bool) -> Self {
        Self {
            power_cycle: SimpleAction::start(PowerCycleParameters {
                force: Some(force),
                immediate: None,
            }),
        }
    }
}

/// Parameters for system power-cycle.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PowerCycleParameters {
    /// Force the power-cycle action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,

    /// Request immediate action, when supported by the switch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub immediate: Option<bool>,
}

/// Request body for `POST /system/image` fetch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemImageFetchRequest {
    /// Fetch action.
    #[serde(rename = "@fetch")]
    pub fetch: SimpleAction<SystemImageFetchParameters>,
}

impl SystemImageFetchRequest {
    /// Create a system-image fetch request.
    pub fn new(remote_url: impl Into<String>) -> Self {
        Self {
            fetch: SimpleAction::start(SystemImageFetchParameters {
                remote_url: remote_url.into(),
            }),
        }
    }
}

/// Parameters for system-image fetch.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemImageFetchParameters {
    /// URL passed to NVUE as `remote-url`.
    #[serde(rename = "remote-url")]
    pub remote_url: String,
}

/// Request body for `POST /system/image/files/{image}` install.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemImageInstallRequest {
    /// Install action.
    #[serde(rename = "@install")]
    pub install: SimpleAction<SystemImageInstallParameters>,
}

impl SystemImageInstallRequest {
    /// Create a system-image install request.
    pub fn new(force: bool) -> Self {
        Self {
            install: SimpleAction::start(SystemImageInstallParameters {
                force: Some(force),
                ..Default::default()
            }),
        }
    }

    /// Create a compatibility install request that also carries `image-file`.
    pub fn with_image_file(force: bool, image_file: impl Into<String>) -> Self {
        Self {
            install: SimpleAction::start(SystemImageInstallParameters {
                force: Some(force),
                image_file: Some(image_file.into()),
                ..Default::default()
            }),
        }
    }
}

/// Parameters for system-image install.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemImageInstallParameters {
    /// Force the install action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,

    /// Reboot behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reboot: Option<bool>,

    /// ISSU behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issu: Option<bool>,

    /// Skip SM behavior.
    #[serde(default, rename = "skip-sm", skip_serializing_if = "Option::is_none")]
    pub skip_sm: Option<bool>,

    /// Compatibility field currently accepted by switch responses.
    #[serde(
        default,
        rename = "image-file",
        skip_serializing_if = "Option::is_none"
    )]
    pub image_file: Option<String>,
}

/// Request body for `POST /system/image` uninstall.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemImageUninstallRequest {
    /// Uninstall action.
    #[serde(rename = "@uninstall")]
    pub uninstall: SimpleAction<SystemImageUninstallParameters>,
}

impl SystemImageUninstallRequest {
    /// Create a system-image uninstall request.
    pub fn new(force: bool) -> Self {
        Self {
            uninstall: SimpleAction::start(SystemImageUninstallParameters { force: Some(force) }),
        }
    }
}

/// Parameters for system-image uninstall.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemImageUninstallParameters {
    /// Force the uninstall action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
}

/// Raw view of `/system/image` needed by RMS.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct SystemImage {
    /// Partition 1 image information.
    #[serde(default)]
    pub partition1: Option<SystemImageBuild>,

    /// Partition 2 image information.
    #[serde(default)]
    pub partition2: Option<SystemImageBuild>,

    /// Current partition or build ID.
    #[serde(default)]
    pub current: Option<String>,

    /// Next partition or build ID.
    #[serde(default)]
    pub next: Option<String>,
}

impl SystemImage {
    /// Return the normalized state used by install-then-wait flows.
    pub fn normalized_state(&self) -> SystemImageState {
        let mut state = SystemImageState {
            partition1_build_id: self
                .partition1
                .as_ref()
                .and_then(SystemImageBuild::build_id)
                .unwrap_or_default()
                .to_owned(),
            partition2_build_id: self
                .partition2
                .as_ref()
                .and_then(SystemImageBuild::build_id)
                .unwrap_or_default()
                .to_owned(),
            current: self.current.clone().unwrap_or_default(),
            next: self.next.clone().unwrap_or_default(),
            ..Default::default()
        };

        state.current_partition = normalize_partition_id(&state.current);
        state.next_partition = normalize_partition_id(&state.next);
        state.current_build_id = resolve_build_id(&state, &state.current, &state.current_partition);
        state.next_build_id = resolve_build_id(&state, &state.next, &state.next_partition);

        if state.current_partition.is_empty() {
            state.current_partition = find_partition_id(&state, &state.current_build_id);
        }

        if state.next_partition.is_empty() {
            state.next_partition = find_partition_id(&state, &state.next_build_id);
        }

        state
    }
}

/// A system-image partition build-id value.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SystemImageBuild {
    /// Plain build-id string.
    Text(String),

    /// Object carrying `build-id`.
    Object {
        /// Build ID.
        #[serde(default, rename = "build-id")]
        build_id: Option<String>,
    },
}

impl SystemImageBuild {
    /// Return the build ID.
    pub fn build_id(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            Self::Object { build_id } => build_id.as_deref(),
        }
    }
}

/// Normalized system-image state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SystemImageState {
    /// Raw current value.
    pub current: String,
    /// Raw next value.
    pub next: String,
    /// Normalized current partition.
    pub current_partition: String,
    /// Normalized next partition.
    pub next_partition: String,
    /// Resolved current build ID.
    pub current_build_id: String,
    /// Resolved next build ID.
    pub next_build_id: String,
    /// Partition 1 build ID.
    pub partition1_build_id: String,
    /// Partition 2 build ID.
    pub partition2_build_id: String,
}

/// Recursive system-image file listing.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum SystemImageListing {
    /// Null listing value.
    Null(()),

    /// String value.
    Text(String),

    /// List value.
    List(Vec<SystemImageListing>),

    /// Object value.
    Object(BTreeMap<String, SystemImageListing>),

    /// Boolean value.
    Bool(bool),

    /// Numeric value.
    Number(JsonNumber),
}

impl SystemImageListing {
    /// Return true when `image_filename` appears anywhere in the listing.
    pub fn contains_file(&self, image_filename: &str) -> bool {
        match self {
            Self::Text(value) => value == image_filename,
            Self::List(values) => values
                .iter()
                .any(|value| value.contains_file(image_filename)),
            Self::Object(values) => values
                .iter()
                .any(|(key, value)| key == image_filename || value.contains_file(image_filename)),
            Self::Null(()) | Self::Bool(_) | Self::Number(_) => false,
        }
    }
}

/// Payload for `PATCH /system/aaa/user/{user-id}` password changes.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserPasswordPatch {
    /// Base64-encoded password value sent to NVUE.
    pub password: String,
}

impl std::fmt::Debug for UserPasswordPatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserPasswordPatch")
            .field("password", &"<redacted>")
            .finish()
    }
}

impl UserPasswordPatch {
    /// Create a password patch payload from a plaintext password.
    pub fn new(password: impl Into<String>) -> Self {
        let password = password.into();

        Self {
            password: BASE64_STANDARD.encode(password.as_bytes()),
        }
    }
}

/// Minimal gNMI server view.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct GnmiServer {
    /// gNMI server state.
    #[serde(default)]
    pub state: Option<String>,
}

fn normalize_partition_id(value: &str) -> String {
    match value {
        "1" | NVOS_PARTITION_1_ID => NVOS_PARTITION_1_ID.to_owned(),
        "2" | NVOS_PARTITION_2_ID => NVOS_PARTITION_2_ID.to_owned(),
        _ => String::new(),
    }
}

fn find_partition_id(state: &SystemImageState, build_id: &str) -> String {
    if build_id.is_empty() {
        return String::new();
    }

    if !state.partition1_build_id.is_empty() && state.partition1_build_id == build_id {
        return NVOS_PARTITION_1_ID.to_owned();
    }

    if !state.partition2_build_id.is_empty() && state.partition2_build_id == build_id {
        return NVOS_PARTITION_2_ID.to_owned();
    }

    String::new()
}

fn resolve_build_id(state: &SystemImageState, value: &str, partition_id: &str) -> String {
    if partition_id == NVOS_PARTITION_1_ID || value == NVOS_PARTITION_1_ID {
        return state.partition1_build_id.clone();
    }
    if partition_id == NVOS_PARTITION_2_ID || value == NVOS_PARTITION_2_ID {
        return state.partition2_build_id.clone();
    }
    value.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_factory_reset_request_has_expected_wire_shape() {
        assert_eq!(
            serde_json::to_value(SystemFactoryDefaultResetRequest::start()).unwrap(),
            serde_json::json!({
                "@reset": {
                    "state": "start",
                    "parameters": { "force": true }
                }
            })
        );
    }

    #[test]
    fn endpoints_use_nvue_v1_server() {
        assert_eq!(SYSTEM_ENDPOINT, "/nvue_v1/system");

        assert_eq!(
            SYSTEM_FACTORY_DEFAULT_ENDPOINT,
            "/nvue_v1/system/factory-default"
        );

        assert_eq!(SYSTEM_IMAGE_ENDPOINT, "/nvue_v1/system/image");

        assert_eq!(
            system_image_file_endpoint("nvos.bin"),
            "/nvue_v1/system/image/files/nvos.bin"
        );
        assert_eq!(user_endpoint("admin"), "/nvue_v1/system/aaa/user/admin");
        assert_eq!(
            user_endpoint_for_revision("admin", "pending"),
            "/nvue_v1/system/aaa/user/admin?rev=pending"
        );
        assert_eq!(
            gnmi_server_endpoint_for_revision("pending"),
            "/nvue_v1/system/gnmi-server?rev=pending"
        );
        assert_eq!(
            system_image_file_endpoint("nvos image.bin"),
            "/nvue_v1/system/image/files/nvos%20image.bin"
        );
        assert_eq!(
            user_endpoint_for_revision("admin/root", "pending&base=applied"),
            "/nvue_v1/system/aaa/user/admin%2Froot?rev=pending%26base%3Dapplied"
        );
    }

    #[test]
    fn action_payloads_match_nvue_action_keys() {
        let power_cycle = serde_json::to_string(&SystemPowerCycleRequest::new(true)).unwrap();

        let fetch =
            serde_json::to_string(&SystemImageFetchRequest::new("file:///tmp/a.bin")).unwrap();

        let install =
            serde_json::to_string(&SystemImageInstallRequest::with_image_file(true, "a.bin"))
                .unwrap();

        let uninstall = serde_json::to_string(&SystemImageUninstallRequest::new(true)).unwrap();

        assert!(power_cycle.contains(r#""@power-cycle""#));
        assert!(fetch.contains(r#""@fetch""#));
        assert!(fetch.contains(r#""remote-url":"file:///tmp/a.bin""#));
        assert!(install.contains(r#""@install""#));
        assert!(install.contains(r#""image-file":"a.bin""#));
        assert!(uninstall.contains(r#""@uninstall""#));
    }

    #[test]
    fn system_image_state_normalizes_partition_shapes() {
        let image: SystemImage = serde_json::from_str(
            r#"{
                "partition1": {"build-id": "nvos-1.2.3"},
                "partition2": "nvos-2.3.4",
                "current": "1",
                "next": "nvos-2.3.4"
            }"#,
        )
        .unwrap();

        let state = image.normalized_state();

        assert_eq!(state.current_partition, NVOS_PARTITION_1_ID);
        assert_eq!(state.next_partition, NVOS_PARTITION_2_ID);
        assert_eq!(state.current_build_id, "nvos-1.2.3");
        assert_eq!(state.next_build_id, "nvos-2.3.4");
    }

    #[test]
    fn listing_contains_file_recursively() {
        let listing: SystemImageListing = serde_json::from_str(
            r#"{
                "partition1": ["old.bin"],
                "partition2": {"nvos.bin": {"size": 18446744073709551615}},
                "metadata": {"ratio": 1.5}
            }"#,
        )
        .unwrap();

        assert!(listing.contains_file("nvos.bin"));
        assert!(listing.contains_file("old.bin"));
        assert!(!listing.contains_file("missing.bin"));
    }

    #[test]
    fn user_password_patch_uses_password_field() {
        let payload = serde_json::to_string(&UserPasswordPatch::new("plain-password")).unwrap();

        assert_eq!(payload, r#"{"password":"cGxhaW4tcGFzc3dvcmQ="}"#);
    }

    #[test]
    fn user_password_patch_debug_redacts_password() {
        let payload = UserPasswordPatch::new("plain-password");
        let debug = format!("{payload:?}");

        assert!(!debug.contains("plain-password"));
        assert!(!debug.contains(&payload.password));
        assert!(debug.contains("<redacted>"));
    }
}
