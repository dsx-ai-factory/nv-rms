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

//! Types and endpoint helpers for the NVUE `/system` API area.

use std::collections::BTreeMap;

use crate::action::SimpleAction;
use crate::uri::{path_segment, query_value};
use crate::util::deserialize_optional_lenient_string;
use crate::{JsonNumber, NVUE_V1_SERVER};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

/// Full endpoint for `GET/PATCH /system/api`.
pub const SYSTEM_API_ENDPOINT: &str = "/nvue_v1/system/api";

/// Full endpoint for `GET/PATCH /system/api/mtls`.
pub const SYSTEM_API_MTLS_ENDPOINT: &str = "/nvue_v1/system/api/mtls";

/// Full endpoint for `GET/PATCH /system/gnmi-server/mtls`.
pub const GNMI_SERVER_MTLS_ENDPOINT: &str = "/nvue_v1/system/gnmi-server/mtls";

pub const SPDM_ENDPOINT: &str = "/nvue_v1/system/security/spdm";

/// Operational NVUE SPDM component inventory.
pub const SPDM_OPERATIONAL_ENDPOINT: &str = "/nvue_v1/system/security/spdm?rev=operational";

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

/// Request body for NVUE SPDM measurement generation.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SpdmGenerateRequest {
    #[serde(rename = "@generate")]
    generate: SimpleAction<SpdmGenerateParameters>,
}

impl SpdmGenerateRequest {
    /// Create a fresh SPDM measurement request using a hex-encoded nonce.
    pub fn new(nonce: impl Into<String>) -> Self {
        Self {
            generate: SimpleAction::start(SpdmGenerateParameters {
                nonce: nonce.into(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SpdmGenerateParameters {
    nonce: String,
}

/// Endpoint for one SPDM component.
pub fn spdm_component_endpoint(component_id: &str) -> String {
    format!("{SPDM_ENDPOINT}/{}", path_segment(component_id))
}

/// Endpoint for one SPDM component's measurements.
pub fn spdm_measurements_endpoint(component_id: &str) -> String {
    format!("{}/measurements", spdm_component_endpoint(component_id))
}

/// Endpoint for one SPDM component's certificate chain.
pub fn spdm_certificates_endpoint(component_id: &str) -> String {
    format!("{}/certificates", spdm_component_endpoint(component_id))
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

/// NVUE system-image reboot policy, encoded as `"yes"` or `"no"`, not a boolean.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SystemImageReboot {
    Yes,
    No,
}

/// Parameters for system-image install.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SystemImageInstallParameters {
    /// Force the install action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,

    /// Reboot behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reboot: Option<SystemImageReboot>,

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
    /// Non-force uninstall. Automatic cleanup uses this.
    pub fn new() -> Self {
        Self {
            uninstall: SimpleAction::start(SystemImageUninstallParameters { force: Some(false) }),
        }
    }

    /// Force uninstall. Callers must have explicit intent; RMS image
    /// cleanup never uses this.
    pub fn forced() -> Self {
        Self {
            uninstall: SimpleAction::start(SystemImageUninstallParameters { force: Some(true) }),
        }
    }
}

impl Default for SystemImageUninstallRequest {
    fn default() -> Self {
        Self::new()
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

/// Request body for `PATCH /system/gnmi-server`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GnmiServerUpdate {
    /// gNMI server state (`enabled`/`disabled`).
    pub state: String,
}

impl GnmiServerUpdate {
    /// Create a gNMI server state update payload.
    pub fn new(state: impl Into<String>) -> Self {
        Self {
            state: state.into(),
        }
    }
}

/// Returns the first of `direct`, `operational`, `applied` that is present, matching
/// NVUE's revision-aware response layering.
fn resolve_layered_field<'a>(
    direct: Option<&'a str>,
    operational: Option<&'a str>,
    applied: Option<&'a str>,
) -> Option<&'a str> {
    direct.or(operational).or(applied)
}

/// The `status` line of `GET /system`. NVOS writes one of four constants,
/// and `Failed` is terminal: the boot will not recover on its own. The line
/// is free text on the wire, so a line outside the four, such as one a later
/// NVOS adds, is kept as the switch wrote it and treated as not ready.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub enum SystemStatus {
    /// `System is initializing`: services are still coming up.
    Initializing,
    /// `System is ready`.
    Ready,
    /// `System is not ready`.
    NotReady,
    /// `System is not ready (failed)`: the boot failed, waiting will not
    /// change it.
    Failed,
    /// A line outside the four NVOS constants, as the switch wrote it.
    Unknown(String),
}

impl SystemStatus {
    /// The line NVOS writes while services start.
    pub const INITIALIZING: &'static str = "System is initializing";
    /// The line NVOS writes once startup has finished.
    pub const READY: &'static str = "System is ready";
    /// The line NVOS writes for a system that is up but not ready.
    pub const NOT_READY: &'static str = "System is not ready";
    /// The line NVOS writes when the boot failed.
    pub const FAILED: &'static str = "System is not ready (failed)";

    /// The NVOS line, for messages.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Initializing => Self::INITIALIZING,
            Self::Ready => Self::READY,
            Self::NotReady => Self::NOT_READY,
            Self::Failed => Self::FAILED,
            Self::Unknown(line) => line,
        }
    }
}

impl From<String> for SystemStatus {
    fn from(line: String) -> Self {
        match line.as_str() {
            Self::INITIALIZING => Self::Initializing,
            Self::READY => Self::Ready,
            Self::NOT_READY => Self::NotReady,
            Self::FAILED => Self::Failed,
            _ => Self::Unknown(line),
        }
    }
}

impl std::fmt::Display for SystemStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The health value of `GET /system`, the same closed set on every NVOS
/// generation. A word outside the set fails the decode with serde's
/// "unknown variant" and the expected list, so a vocabulary change on the
/// switch shows up as a decode error, not as a switch that never becomes
/// ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SystemHealth {
    /// `OK`.
    #[serde(rename = "OK")]
    Ok,
    /// `Not OK`: a component reports a problem.
    #[serde(rename = "Not OK")]
    NotOk,
    /// `FATAL`.
    #[serde(rename = "FATAL")]
    Fatal,
    /// `N/A`: reported for a short while after boot, before the health
    /// monitor has run. The reason the readiness check polls.
    #[serde(rename = "N/A")]
    NotAvailable,
}

impl SystemHealth {
    /// Returns the NVUE string spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::NotOk => "Not OK",
            Self::Fatal => "FATAL",
            Self::NotAvailable => "N/A",
        }
    }
}

impl std::fmt::Display for SystemHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Readiness and health of a switch, decoded from `GET /system`.
///
/// Two NVOS generations spell the health field differently; both decode
/// into this type through the private `SystemRootJson`. The rest of the body is
/// ignored. Decode failures are serde's: a body without `status`, a body
/// with neither health spelling (`missing field`), or a health word
/// outside [`SystemHealth`] (`unknown variant`). Such a body is not a
/// switch saying "not ready" but a resource of another shape or version.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "SystemRootJson")]
pub struct SystemRoot {
    /// Boot progress.
    pub status: SystemStatus,
    /// Overall health.
    pub health: SystemHealth,
}

impl SystemRoot {
    /// Startup finished and no health issue is reported.
    pub fn is_ready(&self) -> bool {
        self.status == SystemStatus::Ready && self.health == SystemHealth::Ok
    }

    /// The switch reports a boot that failed. Waiting will not change it,
    /// so a caller polling for readiness stops here. Health is not part of
    /// it: NVOS remediates `FATAL` on its own (a service restart, a monitor
    /// window, one reboot) and the value returns to `OK` when that works.
    pub fn is_failed(&self) -> bool {
        self.status == SystemStatus::Failed
    }
}

impl std::fmt::Display for SystemRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "status={}, health={}", self.status, self.health)
    }
}

/// Wire shape of `GET /system`: the fields the readiness check reads, in
/// both spellings NVOS has used.
///
/// NVOS 25.02.4300 (the Common OM alignment) turned the flat
/// `health-status` string into a `health` object. Before it, GB200 NVL72
/// 1.3.x ships NVOS 25.02.25xx and answers, trimmed to these fields:
///
/// ```json
/// { "status": "System is ready", "health-status": "OK" }
/// ```
///
/// From it on, GB200 2.0, GB300 1.0.x, and VR NVL72 0.6 (NVOS 25.02.43xx
/// and 25.03.x) answer:
///
/// ```json
/// { "status": "System is ready", "health": { "status": "OK" } }
/// ```
///
/// An explicit struct instead of serde attributes on [`SystemRoot`]: the
/// two spellings differ in value type, string against object, which no
/// `alias` covers, and the fact that two shapes exist deserves a named
/// place rather than an attribute. Exactly one of the two health fields
/// is present on a real switch; [`SystemRoot::try_from`] picks it.
#[derive(Deserialize)]
struct SystemRootJson {
    status: SystemStatus,

    /// NVOS 25.02.4300 and later.
    #[serde(default)]
    health: Option<SystemHealthJson>,

    /// NVOS before 25.02.4300.
    #[serde(default, rename = "health-status")]
    health_status: Option<SystemHealth>,
}

/// The `health` object of the current spelling.
#[derive(Deserialize)]
struct SystemHealthJson {
    status: SystemHealth,
}

impl TryFrom<SystemRootJson> for SystemRoot {
    type Error = serde::de::value::Error;

    fn try_from(json: SystemRootJson) -> Result<Self, Self::Error> {
        let health = json
            .health
            .map(|health| health.status)
            .or(json.health_status)
            .ok_or_else(|| serde::de::Error::missing_field("health"))?;
        Ok(Self {
            status: json.status,
            health,
        })
    }
}

/// NVUE API certificate configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct SystemApi {
    /// Configured entity certificate ID.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_lenient_string"
    )]
    pub certificate: Option<String>,

    /// Operational values returned by revision-aware NVUE responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operational: Option<SystemApiFields>,

    /// Applied values returned by revision-aware NVUE responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    applied: Option<SystemApiFields>,
}

impl SystemApi {
    /// Returns the configured entity certificate ID.
    pub fn certificate(&self) -> Option<&str> {
        resolve_layered_field(
            self.certificate.as_deref(),
            self.operational
                .as_ref()
                .and_then(|fields| fields.certificate.as_deref()),
            self.applied
                .as_ref()
                .and_then(|fields| fields.certificate.as_deref()),
        )
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
struct SystemApiFields {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_lenient_string"
    )]
    certificate: Option<String>,
}

/// mTLS CA configuration shared by NVUE API and gNMI server endpoints.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct MtlsConfiguration {
    /// Configured CA certificate ID.
    #[serde(
        default,
        rename = "ca-certificate",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_lenient_string"
    )]
    pub ca_certificate: Option<String>,

    /// Operational values returned by revision-aware NVUE responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operational: Option<MtlsConfigurationFields>,

    /// Applied values returned by revision-aware NVUE responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    applied: Option<MtlsConfigurationFields>,

    /// Additional NVUE fields retained when forwarding the status response.
    #[serde(flatten)]
    additional: BTreeMap<String, Value>,
}

impl MtlsConfiguration {
    /// Returns the configured CA certificate ID.
    pub fn ca_certificate(&self) -> Option<&str> {
        resolve_layered_field(
            self.ca_certificate.as_deref(),
            self.operational
                .as_ref()
                .and_then(|fields| fields.ca_certificate.as_deref()),
            self.applied
                .as_ref()
                .and_then(|fields| fields.ca_certificate.as_deref()),
        )
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
struct MtlsConfigurationFields {
    #[serde(
        default,
        rename = "ca-certificate",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_lenient_string"
    )]
    ca_certificate: Option<String>,

    #[serde(flatten)]
    additional: BTreeMap<String, Value>,
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

    fn system_from(body: serde_json::Value) -> SystemRoot {
        serde_json::from_value(body).unwrap()
    }

    #[test]
    fn system_decodes_both_wire_shapes() {
        let current = system_from(serde_json::json!({
            "hostname": "switch",
            "status": "System is ready",
            "health": {"status": "OK"},
            "uptime": 11659
        }));
        assert!(current.is_ready());

        let legacy = system_from(serde_json::json!({
            "build": "NVOS Debian GNU/Linux 11 (bullseye)",
            "status": "System is ready",
            "health-status": "OK",
            "uptime": 2620098
        }));
        assert_eq!(legacy, current);
    }

    #[test]
    fn system_reduces_status_and_keeps_health() {
        let booting = system_from(serde_json::json!({
            "status": "System is booting",
            "health": {"status": "N/A"}
        }));
        assert_eq!(
            booting,
            SystemRoot {
                status: SystemStatus::Unknown("System is booting".to_owned()),
                health: SystemHealth::NotAvailable,
            }
        );
        assert!(!booting.is_ready());
        assert!(
            !booting.is_failed(),
            "an unknown line is not ready, not failed"
        );
        assert_eq!(booting.to_string(), "status=System is booting, health=N/A");

        let unhealthy = system_from(serde_json::json!({
            "status": "System is ready",
            "health-status": "Not OK"
        }));
        assert!(!unhealthy.is_ready());
        assert_eq!(
            unhealthy.to_string(),
            "status=System is ready, health=Not OK"
        );
    }

    #[test]
    fn system_names_the_four_nvos_status_lines() {
        let lines = [
            ("System is initializing", SystemStatus::Initializing),
            ("System is ready", SystemStatus::Ready),
            ("System is not ready", SystemStatus::NotReady),
            ("System is not ready (failed)", SystemStatus::Failed),
        ];
        for (line, expected) in lines {
            let system = system_from(serde_json::json!({
                "status": line,
                "health": {"status": "OK"}
            }));
            assert_eq!(system.status, expected, "{line}");
            assert_eq!(system.status.as_str(), line);
        }
    }

    #[test]
    fn system_is_failed_only_on_a_failed_boot() {
        let failed_boot = system_from(serde_json::json!({
            "status": "System is not ready (failed)",
            "health": {"status": "OK"}
        }));
        assert!(failed_boot.is_failed());
        assert!(!failed_boot.is_ready());

        let fatal = system_from(serde_json::json!({
            "status": "System is ready",
            "health-status": "FATAL"
        }));
        assert!(!fatal.is_failed(), "NVOS remediates FATAL; the poll waits");
        assert!(!fatal.is_ready());

        let not_ready = system_from(serde_json::json!({
            "status": "System is not ready",
            "health": {"status": "Not OK"}
        }));
        assert!(!not_ready.is_failed(), "not ready and Not OK are retried");
    }

    #[test]
    fn system_rejects_an_unknown_health_word() {
        let error = serde_json::from_value::<SystemRoot>(
            serde_json::json!({ "status": "System is ready", "health": {"status": "Degraded"} }),
        )
        .expect_err("an unknown health word must not decode");
        assert!(
            error.to_string().contains("unknown variant `Degraded`"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn system_rejects_a_body_without_readiness_fields() {
        let error =
            serde_json::from_value::<SystemRoot>(serde_json::json!({ "health": {"status": "OK"} }))
                .expect_err("a body without status must not decode");
        assert!(
            error.to_string().contains("missing field `status`"),
            "unexpected error: {error}"
        );

        let error = serde_json::from_value::<SystemRoot>(
            serde_json::json!({ "status": "System is ready" }),
        )
        .expect_err("a body without health must not decode");
        assert!(
            error.to_string().contains("missing field `health`"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn endpoints_use_nvue_v1_server() {
        assert_eq!(SYSTEM_ENDPOINT, "/nvue_v1/system");
        assert_eq!(SPDM_ENDPOINT, "/nvue_v1/system/security/spdm");

        assert_eq!(
            spdm_component_endpoint("ERoT/BMC"),
            "/nvue_v1/system/security/spdm/ERoT%2FBMC"
        );

        assert_eq!(
            spdm_measurements_endpoint("ERoT_BMC_0"),
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/measurements"
        );

        assert_eq!(
            spdm_certificates_endpoint("ERoT_BMC_0"),
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/certificates"
        );

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
    fn service_certificate_configuration_reads_direct_and_layered_values() {
        let direct: SystemApi = serde_json::from_str(r#"{"certificate":"entity-direct"}"#).unwrap();

        let layered: SystemApi = serde_json::from_str(
            r#"{"operational":{"certificate":"entity-operational"},
                "applied":{"certificate":"entity-applied"}}"#,
        )
        .unwrap();

        let mtls: MtlsConfiguration = serde_json::from_str(
            r#"{"status":"enabled","applied":{"ca-certificate":"ca-applied",
                "state":"active"}}"#,
        )
        .unwrap();

        let mtls_status = serde_json::to_value(&mtls).unwrap();

        assert_eq!(direct.certificate(), Some("entity-direct"));
        assert_eq!(layered.certificate(), Some("entity-operational"));
        assert_eq!(mtls.ca_certificate(), Some("ca-applied"));
        assert_eq!(mtls_status["status"], "enabled");
        assert_eq!(mtls_status["applied"]["state"], "active");
    }

    #[test]
    fn wrong_typed_certificate_fields_decode_as_absent() {
        let system_api: SystemApi =
            serde_json::from_str(r#"{"certificate":true,"operational":{"certificate":false}}"#)
                .unwrap();

        let mtls: MtlsConfiguration =
            serde_json::from_str(r#"{"ca-certificate":["not","a","string"]}"#).unwrap();

        assert_eq!(system_api.certificate(), None);
        assert_eq!(mtls.ca_certificate(), None);
    }

    #[test]
    fn system_image_reboot_policy_uses_nvue_strings() {
        let mut request = SystemImageInstallRequest::with_image_file(true, "nvos.bin");
        assert!(
            serde_json::to_value(&request).unwrap()["@install"]["parameters"]
                .get("reboot")
                .is_none()
        );
        for (policy, wire_value) in [
            (SystemImageReboot::No, "no"),
            (SystemImageReboot::Yes, "yes"),
        ] {
            request.install.parameters.as_mut().unwrap().reboot = Some(policy);
            let value = serde_json::to_value(&request).unwrap();
            assert_eq!(value["@install"]["parameters"]["reboot"], wire_value);
            assert_eq!(
                serde_json::from_value::<SystemImageInstallRequest>(value).unwrap(),
                request
            );
        }
    }

    #[test]
    fn action_payloads_match_nvue_action_keys() {
        let power_cycle = serde_json::to_string(&SystemPowerCycleRequest::new(true)).unwrap();

        let fetch =
            serde_json::to_string(&SystemImageFetchRequest::new("file:///tmp/a.bin")).unwrap();

        let install =
            serde_json::to_string(&SystemImageInstallRequest::with_image_file(true, "a.bin"))
                .unwrap();

        let uninstall = serde_json::to_string(&SystemImageUninstallRequest::new()).unwrap();
        let uninstall_forced =
            serde_json::to_string(&SystemImageUninstallRequest::forced()).unwrap();

        let spdm_generate = serde_json::to_string(&SpdmGenerateRequest::new("deadbeef")).unwrap();

        assert!(power_cycle.contains(r#""@power-cycle""#));
        assert!(fetch.contains(r#""@fetch""#));
        assert!(fetch.contains(r#""remote-url":"file:///tmp/a.bin""#));
        assert!(install.contains(r#""@install""#));
        assert!(install.contains(r#""image-file":"a.bin""#));
        assert!(uninstall.contains(r#""@uninstall""#));
        assert!(uninstall.contains(r#""force":false"#));
        assert!(!uninstall.contains(r#""force":true"#));
        assert!(uninstall_forced.contains(r#""force":true"#));

        assert_eq!(
            spdm_generate,
            r#"{"@generate":{"state":"start","parameters":{"nonce":"deadbeef"}}}"#
        );
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
