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

//! NVOS mTLS certificate installation for switch services.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use nvswitch_cli::{
    BenignReason, ClusterApp, CommandFlow, FileMode, NvConfigOperation, RemotePath, StepStatus,
    StepSubject,
};
use nvue_client::system::{GNMI_SERVER_ENDPOINT, GNMI_SERVER_MTLS_ENDPOINT, MtlsConfiguration};
use serde::Serialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::libnmxc::NmxcTlsConfig;
use crate::nodes::nvos::GnmiClientCa;
use crate::transport::http_client::HttpClient;
use crate::transport::ssh_client::SshClient;
use crate::utilities::error::{Result, RmsError};
use librms::protos::rack_manager as rm;

#[cfg(not(test))]
use super::super::config;
use super::super::{SwitchGb200Nvidia, is_valid_identifier};

const REMOTE_TLS_ROOT: &str = "/home/admin/certs";
pub(super) const REMOTE_RUN_ID_FORMAT: &str = "%Y-%m-%d_%H-%M-%S_%3f";
pub(super) const NVUE_CA_CERTIFICATE_COLLECTION: &str = "/nvue_v1/system/security/ca-certificate";
pub(super) const NVUE_ENTITY_CERTIFICATE_COLLECTION: &str = "/nvue_v1/system/security/certificate";
pub(super) const NVUE_SYSTEM_API_PATH: &str = "/nvue_v1/system/api";
pub(super) const NVUE_SYSTEM_API_MTLS_PATH: &str = "/nvue_v1/system/api/mtls";
const MATERIAL_STABILITY_ATTEMPTS: usize = 3;
#[cfg(not(test))]
const MTLS_UNSET_COMMAND_SETTLE_DELAY: Duration = Duration::from_millis(300);
#[cfg(test)]
const MTLS_UNSET_COMMAND_SETTLE_DELAY: Duration = Duration::ZERO;
#[cfg(test)]
const CLUSTER_MANAGER_ACTION_SETTLE_DELAY: Duration = Duration::ZERO;
#[cfg(not(test))]
const CLUSTER_MANAGER_ACTION_SETTLE_DELAY: Duration =
    Duration::from_secs(config::CLUSTER_MANAGER_ACTION_SETTLE_SECONDS);

/// Staging directory mode: owner rwx, group/other traverse-only (no list).
/// NVUE `file://` import reads staged PEMs via `nvued`, which may not run as
/// `admin`; mode 0700 blocks traversal for other users even when files are 0644.
// rwxr-x--x (0o751): owner full, group read+search, other search only.
pub const SWITCH_CERTIFICATE_REMOTE_DIR_PERMISSIONS: FileMode = FileMode::builder()
    .owner_read()
    .owner_write()
    .owner_execute()
    .group_read()
    .group_execute()
    .other_execute()
    .build();
// rw-r--r-- (0o644): owner read+write, group/other read.
pub const SWITCH_CERTIFICATE_PUBLIC_FILE_PERMISSIONS: FileMode = FileMode::builder()
    .owner_read()
    .owner_write()
    .group_read()
    .other_read()
    .build();
// r-------- (0o400): owner read only (private key).
pub const SWITCH_CERTIFICATE_PRIVATE_KEY_PERMISSIONS: FileMode =
    FileMode::builder().owner_read().build();

/// Remote TLS file copied to the switch before NVUE import.
pub(crate) struct MtlsRemoteFileSpec<'a> {
    pub remote_name: &'static str,
    pub local_path: &'a std::path::Path,
    pub permissions: FileMode,
}

/// Outcome of a single SFTP copy step in the switch certificate workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MtlsSftpCopyOutcome {
    pub remote_path: String,
    pub bytes: u64,
}

pub(crate) fn mtls_remote_file_specs(
    material: &SwitchMtlsMaterialPaths,
) -> [MtlsRemoteFileSpec<'_>; 3] {
    [
        MtlsRemoteFileSpec {
            remote_name: "ca.pem",
            local_path: material.ca_cert_path.as_path(),
            permissions: SWITCH_CERTIFICATE_PUBLIC_FILE_PERMISSIONS,
        },
        MtlsRemoteFileSpec {
            remote_name: "client.pem",
            local_path: material.entity_cert_path.as_path(),
            permissions: SWITCH_CERTIFICATE_PUBLIC_FILE_PERMISSIONS,
        },
        MtlsRemoteFileSpec {
            remote_name: "client.key",
            local_path: material.entity_key_path.as_path(),
            permissions: SWITCH_CERTIFICATE_PRIVATE_KEY_PERMISSIONS,
        },
    ]
}

pub(crate) fn sftp_copy_stage_name(remote_file_name: &str) -> String {
    format!("sftp_copy_{}", remote_file_name.replace('.', "_"))
}

fn readable_remote_run_id() -> String {
    Utc::now().format(REMOTE_RUN_ID_FORMAT).to_string()
}

fn nvue_ca_certificate_endpoint(cert_id: &str) -> String {
    format!("{NVUE_CA_CERTIFICATE_COLLECTION}/{cert_id}")
}

fn nvue_entity_certificate_endpoint(cert_id: &str) -> String {
    format!("{NVUE_ENTITY_CERTIFICATE_COLLECTION}/{cert_id}")
}

fn nvue_cluster_app_manager_endpoint(app_name: &str) -> String {
    format!("/nvue_v1/cluster/apps/{app_name}/manager")
}

pub(super) fn nvue_cluster_app_manager_sub_endpoint(app_name: &str, field: &str) -> String {
    format!("{}/{}", nvue_cluster_app_manager_endpoint(app_name), field)
}

#[derive(Serialize)]
struct ImportCaCertificateParameters<'a> {
    uri: &'a str,
}

#[derive(Serialize)]
struct ImportEntityCertificateParameters<'a> {
    #[serde(rename = "uri-public-key")]
    public_key_uri: &'a str,

    #[serde(rename = "uri-private-key")]
    private_key_uri: &'a str,
}

#[derive(Serialize)]
struct ClusterAppManagerCertificateParameters<'a> {
    #[serde(rename = "cert-id")]
    certificate_id: &'a str,
}

#[derive(Serialize)]
struct ClusterAppManagerCaCertificateParameters<'a> {
    #[serde(rename = "cacert-id")]
    ca_certificate_id: &'a str,
}

#[derive(Serialize)]
struct CertificateBinding<'a> {
    certificate: &'a str,
}

#[derive(Serialize)]
struct CaCertificateBinding<'a> {
    #[serde(rename = "ca-certificate")]
    ca_certificate: &'a str,
}

/// Build the ordered command flow that clears mTLS for `services`.
///
/// Command text, step ordering, the gNMI command-form fallback, and which
/// failures are benign all live in [`CommandFlow::unset_mtls`].
fn mtls_unset_flow(services: &[SwitchMtlsService]) -> Result<CommandFlow> {
    let services: Vec<_> = services
        .iter()
        .map(|service| service.cli_service())
        .collect();

    Ok(CommandFlow::unset_mtls(&services)?.with_settle_delay(MTLS_UNSET_COMMAND_SETTLE_DELAY))
}

pub(crate) fn extract_string_field<'a>(response: &'a Value, field: &str) -> Option<&'a str> {
    response
        .get(field)
        .and_then(Value::as_str)
        .or_else(|| {
            response
                .get("operational")
                .and_then(|v| v.get(field))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            response
                .get("applied")
                .and_then(|v| v.get(field))
                .and_then(Value::as_str)
        })
}

fn certificate_installed_services(response: &Value) -> Vec<String> {
    let installed = response
        .get("installed")
        .or_else(|| response.get("[installed]"));

    match installed {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
        Some(Value::Object(values)) => values.keys().cloned().collect(),
        Some(Value::String(value)) if !value.is_empty() => vec![value.clone()],
        _ => Vec::new(),
    }
}

fn verify_certificate_not_expired(response: &Value, certificate_id: &str) -> Result<DateTime<Utc>> {
    let valid_to = extract_string_field(response, "valid-to").ok_or_else(|| {
        RmsError::failed_precondition(format!(
            "switch certificate {certificate_id} response missing valid-to"
        ))
    })?;
    let valid_to = DateTime::parse_from_rfc3339(valid_to)
        .map_err(|e| {
            RmsError::failed_precondition(format!(
                "switch certificate {certificate_id} valid-to is not RFC3339: {e}"
            ))
        })?
        .with_timezone(&Utc);

    if valid_to <= Utc::now() {
        return Err(RmsError::failed_precondition(format!(
            "switch certificate {certificate_id} expired at {valid_to}"
        )));
    }

    Ok(valid_to)
}

pub(crate) fn verify_expected_field(
    actual: Option<&str>,
    field: &str,
    expected: &str,
    context: &str,
) -> Result<()> {
    let actual = actual.ok_or_else(|| {
        RmsError::failed_precondition(format!("{context} response missing {field}"))
    })?;
    if actual != expected {
        return Err(RmsError::failed_precondition(format!(
            "{context} expected {field}={expected}, got {actual}"
        )));
    }
    Ok(())
}

/// Completed-job value when gNMI verification read a bound client CA.
pub(crate) const GNMI_CLIENT_AUTHENTICATION_MTLS: &str = "mtls";

/// Completed-job value when the image has no gNMI client-CA resource.
///
/// Bind installed the server certificate on `/system/gnmi-server` and did not
/// configure client authentication.
pub(crate) const GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY: &str =
    "server-certificate-only";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SwitchMtlsService {
    NvueApi,
    ScaleUpFabricTelemetry,
    ScaleUpFabricManager,
    ScaleUpFabricTelemetryInterface,
}

impl SwitchMtlsService {
    pub(crate) const ALL: [Self; 4] = [
        Self::NvueApi,
        Self::ScaleUpFabricTelemetry,
        Self::ScaleUpFabricManager,
        Self::ScaleUpFabricTelemetryInterface,
    ];

    pub fn from_proto(value: rm::SwitchService) -> Option<Self> {
        match value {
            rm::SwitchService::Unspecified => None,
            rm::SwitchService::NvueApi => Some(Self::NvueApi),
            rm::SwitchService::ScaleUpFabricTelemetry => Some(Self::ScaleUpFabricTelemetry),
            rm::SwitchService::ScaleUpFabricManager => Some(Self::ScaleUpFabricManager),
            rm::SwitchService::ScaleUpFabricTelemetryInterface => {
                Some(Self::ScaleUpFabricTelemetryInterface)
            }
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NvueApi => "nvue-api",
            Self::ScaleUpFabricTelemetry => "scale-up-fabric-telemetry",
            Self::ScaleUpFabricManager => "scale-up-fabric-manager",
            Self::ScaleUpFabricTelemetryInterface => "scale-up-fabric-telemetry-interface",
        }
    }

    pub(crate) fn cluster_app_name(self) -> Option<&'static str> {
        match self {
            Self::ScaleUpFabricManager => Some(ClusterApp::NMX_CONTROLLER),
            Self::ScaleUpFabricTelemetry => Some(ClusterApp::NMX_TELEMETRY),
            Self::NvueApi | Self::ScaleUpFabricTelemetryInterface => None,
        }
    }

    /// The NVOS object whose mTLS binding this service is configured through.
    fn cli_service(self) -> nvswitch_cli::MtlsService {
        match self {
            Self::NvueApi => nvswitch_cli::MtlsService::NvueApi,
            Self::ScaleUpFabricTelemetryInterface => nvswitch_cli::MtlsService::GnmiServer,
            Self::ScaleUpFabricManager => {
                nvswitch_cli::MtlsService::ClusterApp(ClusterApp::nmx_controller())
            }
            Self::ScaleUpFabricTelemetry => {
                nvswitch_cli::MtlsService::ClusterApp(ClusterApp::nmx_telemetry())
            }
        }
    }

    pub(crate) fn installed_certificate_service_name(self) -> Option<&'static str> {
        match self {
            Self::NvueApi => Some("nvue-rest-api"),
            Self::ScaleUpFabricTelemetryInterface => Some("gnmi-server"),
            Self::ScaleUpFabricManager | Self::ScaleUpFabricTelemetry => None,
        }
    }
}

/// What happened to one service (or one persist operation) during an mTLS
/// unset run.
///
/// Reported in place of the `nv` command lines the switch was asked to run:
/// callers need to know which services converged, not how NVOS was driven.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MtlsUnsetStatus {
    /// The binding was cleared or the persist step's desired state already holds.
    Cleared,

    /// The cluster is disabled, so the app could not have been running mTLS.
    /// The desired state already held.
    AlreadyDisabled,

    /// This NVOS build has no mTLS schema for the service, so it was already
    /// insecure. The desired state already held.
    Unsupported,

    /// The change failed and aborted the run.
    Failed,

    /// Never attempted: an earlier stage aborted the run, or the run finished
    /// early because the remaining work became unnecessary.
    NotAttempted,
}

impl MtlsUnsetStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cleared => "cleared",
            Self::AlreadyDisabled => "already_disabled",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
            Self::NotAttempted => "not_attempted",
        }
    }

    fn from_step_status(status: &StepStatus) -> Self {
        match status {
            StepStatus::Succeeded { .. } => Self::Cleared,
            StepStatus::Benign {
                reason: BenignReason::ClusterNotEnabled,
            } => Self::AlreadyDisabled,
            StepStatus::Benign {
                reason: BenignReason::GnmiMtlsSchemaAbsent,
            } => Self::Unsupported,
            StepStatus::Benign {
                reason: BenignReason::ConfigAlreadyApplied,
            } => Self::Cleared,
            StepStatus::Failed { .. } => Self::Failed,
            StepStatus::NotAttempted => Self::NotAttempted,
        }
    }
}

/// The stage of the unset run a status belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MtlsUnsetStage {
    /// Clearing a service's mTLS binding.
    UnsetBinding,

    /// Applying the pending configuration.
    ConfigApply,

    /// Persisting the applied configuration.
    ConfigSave,
}

impl MtlsUnsetStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UnsetBinding => "unset_binding",
            Self::ConfigApply => "config_apply",
            Self::ConfigSave => "config_save",
        }
    }

    fn from_persist_operation(operation: NvConfigOperation) -> Self {
        match operation {
            NvConfigOperation::Save => Self::ConfigSave,
            // Detach never appears in the unset flow; treating it as an apply
            // keeps this total without inventing a stage the flow cannot reach.
            NvConfigOperation::Apply
            | NvConfigOperation::ApplyAssumeYes
            | NvConfigOperation::Detach => Self::ConfigApply,
        }
    }
}

/// One service's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MtlsServiceOutcome {
    pub service: SwitchMtlsService,
    pub status: MtlsUnsetStatus,
}

/// One persist operation's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MtlsPersistOutcome {
    pub stage: MtlsUnsetStage,
    pub status: MtlsUnsetStatus,
}

/// Where an unset run stopped.
///
/// `service` is `None` when a persist step failed, since apply and save are
/// not attributable to any single service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MtlsUnsetFailureSite {
    pub service: Option<SwitchMtlsService>,
    pub stage: MtlsUnsetStage,
    pub message: String,
}

impl MtlsUnsetFailureSite {
    /// A description of where the run stopped, used as the error's message.
    ///
    /// `message` is the transport's own text with the command already removed
    /// by [`SwitchCliTransport::run`](super::super::cli::SwitchCliTransport),
    /// so this adds the service and stage without reintroducing command text.
    fn describe(&self) -> String {
        match self.service {
            Some(service) => format!(
                "failed to unset mTLS for {} at stage {}: {}",
                service.as_str(),
                self.stage.as_str(),
                self.message
            ),
            None => format!(
                "failed to unset mTLS at stage {}: {}",
                self.stage.as_str(),
                self.message
            ),
        }
    }
}

/// Per-service and per-persist outcomes for one mTLS unset run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MtlsUnsetReport {
    pub services: Vec<MtlsServiceOutcome>,
    pub persist: Vec<MtlsPersistOutcome>,
    pub failure: Option<MtlsUnsetFailureSite>,

    /// The run was cancelled part-way through. The statuses above still
    /// describe what the switch was actually asked to do.
    pub cancelled: bool,
}

impl MtlsUnsetReport {
    /// A report where every requested service and both persist operations
    /// share `status`.
    fn uniform(services: &[SwitchMtlsService], status: MtlsUnsetStatus) -> Self {
        Self {
            services: services
                .iter()
                .map(|service| MtlsServiceOutcome {
                    service: *service,
                    status,
                })
                .collect(),
            persist: [MtlsUnsetStage::ConfigApply, MtlsUnsetStage::ConfigSave]
                .into_iter()
                .map(|stage| MtlsPersistOutcome { stage, status })
                .collect(),
            failure: None,
            cancelled: false,
        }
    }

    /// A report for a run that never started, so every requested service and
    /// both persist operations read as `not_attempted` rather than absent.
    pub(crate) fn not_attempted(services: &[SwitchMtlsService]) -> Self {
        Self::uniform(services, MtlsUnsetStatus::NotAttempted)
    }

    /// A fully successful run, for tests covering how a `completed` result is
    /// rendered rather than how one is arrived at.
    #[cfg(test)]
    pub(crate) fn all_cleared(services: &[SwitchMtlsService]) -> Self {
        Self::uniform(services, MtlsUnsetStatus::Cleared)
    }

    /// Translate a flow report into service-level outcomes.
    ///
    /// `requested` is the caller's service list; each flow step's
    /// [`StepSubject`] is matched back through [`SwitchMtlsService::cli_service`],
    /// which is injective over the four supported services, so the mapping is
    /// unambiguous. A step whose subject does not resolve is skipped rather
    /// than guessed at.
    fn from_flow(report: &nvswitch_cli::FlowReport, requested: &[SwitchMtlsService]) -> Self {
        let mut built = Self {
            cancelled: report.interrupted(),
            ..Self::default()
        };

        for step in report.steps() {
            let status = MtlsUnsetStatus::from_step_status(step.status());

            let (service, stage) = match step.subject() {
                Some(StepSubject::Mtls(cli_service)) => {
                    let Some(service) = requested
                        .iter()
                        .copied()
                        .find(|service| service.cli_service() == *cli_service)
                    else {
                        continue;
                    };

                    built.services.push(MtlsServiceOutcome { service, status });

                    (Some(service), MtlsUnsetStage::UnsetBinding)
                }
                Some(StepSubject::Persist(operation)) => {
                    let stage = MtlsUnsetStage::from_persist_operation(*operation);

                    built.persist.push(MtlsPersistOutcome { stage, status });

                    (None, stage)
                }
                None => continue,
            };

            if let StepStatus::Failed { message } = step.status() {
                built.failure.get_or_insert(MtlsUnsetFailureSite {
                    service,
                    stage,
                    message: message.clone(),
                });
            }
        }

        built
    }
}

/// A failed unset run, carrying the partial report alongside the error.
///
/// Keeping them together is the point: the error alone cannot say which
/// services had already been cleared before the run stopped.
#[derive(Debug)]
pub(crate) struct MtlsUnsetError {
    pub error: RmsError,
    pub report: MtlsUnsetReport,
}

impl From<MtlsUnsetError> for RmsError {
    fn from(error: MtlsUnsetError) -> Self {
        error.error
    }
}

fn sanitize_domain_for_cert_id(domain: &str) -> String {
    domain
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn cert_ids_for_domain_with_suffix(domain: &str, suffix: &str) -> (String, String) {
    let sanitized = sanitize_domain_for_cert_id(domain);
    (
        format!("rms-{sanitized}-ca-{suffix}"),
        format!("rms-{sanitized}-cert-{suffix}"),
    )
}

/// Returns unique NVOS CA and entity certificate IDs for a TLS domain.
///
/// A UTC timestamp suffix is appended so repeated installs do not collide with
/// certificates already present on the switch.
pub fn cert_ids_for_domain(domain: &str) -> (String, String) {
    cert_ids_for_domain_with_suffix(domain, &readable_remote_run_id())
}

async fn read_material_bundle(paths: [&Path; 3]) -> Result<[Vec<u8>; 3]> {
    async fn read(path: &Path) -> Result<Vec<u8>> {
        tokio::fs::read(path).await.map_err(|error| {
            RmsError::invalid_argument(format!(
                "failed to read switch TLS material {}: {error}",
                path.display()
            ))
        })
    }

    Ok([
        read(paths[0]).await?,
        read(paths[1]).await?,
        read(paths[2]).await?,
    ])
}

async fn snapshot_material_bundle(
    source_paths: [&Path; 3],
) -> Result<(Arc<tempfile::TempDir>, [PathBuf; 3])> {
    for _ in 0..MATERIAL_STABILITY_ATTEMPTS {
        let first = read_material_bundle(source_paths).await?;
        tokio::task::yield_now().await;
        let second = read_material_bundle(source_paths).await?;

        if first != second {
            continue;
        }

        let snapshot = Arc::new(tempfile::tempdir().map_err(|error| {
            RmsError::internal(format!("failed to create TLS material snapshot: {error}"))
        })?);
        let paths = ["ca.pem", "client.pem", "client.key"].map(|name| snapshot.path().join(name));

        for (path, bytes) in paths.iter().zip(first) {
            tokio::fs::write(path, bytes).await.map_err(|error| {
                RmsError::internal(format!(
                    "failed to write TLS material snapshot {}: {error}",
                    path.display()
                ))
            })?;
        }

        return Ok((snapshot, paths));
    }

    Err(RmsError::unavailable(
        "TLS material changed while it was being loaded",
    ))
}

#[derive(Clone)]
pub(crate) struct StableNmxcTlsConfig {
    config: NmxcTlsConfig,
    _snapshot: Arc<tempfile::TempDir>,
}

impl StableNmxcTlsConfig {
    pub(crate) async fn load(config: NmxcTlsConfig) -> Result<Self> {
        let paths = [
            config.ca_cert_path.as_deref(),
            config.client_cert_path.as_deref(),
            config.client_key_path.as_deref(),
        ];
        let [Some(ca), Some(cert), Some(key)] = paths else {
            return Err(RmsError::failed_precondition(
                "NMX/gNMI client mTLS requires CA, client certificate, and client key paths",
            ));
        };

        let (snapshot, paths) = snapshot_material_bundle([ca, cert, key]).await?;

        Ok(Self {
            config: NmxcTlsConfig {
                ca_cert_path: Some(paths[0].clone()),
                client_cert_path: Some(paths[1].clone()),
                client_key_path: Some(paths[2].clone()),
                authority: config.authority,
            },
            _snapshot: snapshot,
        })
    }

    pub(crate) fn as_config(&self) -> &NmxcTlsConfig {
        &self.config
    }

    pub(crate) fn set_authority_if_none(&mut self, authority: &str) {
        if self.config.authority.is_none() {
            self.config.authority = Some(authority.to_owned());
        }
    }
}

#[derive(Clone)]
pub struct SwitchMtlsMaterialPaths {
    pub domain: String,
    pub ca_cert_path: PathBuf,
    pub entity_cert_path: PathBuf,
    pub entity_key_path: PathBuf,
    remote_run_id: String,
    ca_cert_id: String,
    entity_cert_id: String,
    _snapshot: Option<Arc<tempfile::TempDir>>,
}

impl SwitchMtlsMaterialPaths {
    fn new(
        domain: String,
        ca_cert_path: PathBuf,
        entity_cert_path: PathBuf,
        entity_key_path: PathBuf,
    ) -> Self {
        let remote_run_id = readable_remote_run_id();
        let (ca_cert_id, entity_cert_id) = cert_ids_for_domain_with_suffix(&domain, &remote_run_id);
        Self {
            domain,
            ca_cert_path,
            entity_cert_path,
            entity_key_path,
            remote_run_id,
            ca_cert_id,
            entity_cert_id,
            _snapshot: None,
        }
    }

    pub async fn load_stable(
        domain: String,
        ca_cert_path: PathBuf,
        entity_cert_path: PathBuf,
        entity_key_path: PathBuf,
    ) -> Result<Self> {
        let (snapshot, paths) = snapshot_material_bundle([
            ca_cert_path.as_path(),
            entity_cert_path.as_path(),
            entity_key_path.as_path(),
        ])
        .await?;

        let mut material = Self::new(domain, paths[0].clone(), paths[1].clone(), paths[2].clone());

        material._snapshot = Some(snapshot);

        Ok(material)
    }

    pub fn remote_dir(&self) -> String {
        format!("{REMOTE_TLS_ROOT}/{}", self.remote_run_id)
    }

    pub fn ca_cert_id(&self) -> String {
        self.ca_cert_id.clone()
    }

    pub fn entity_cert_id(&self) -> String {
        self.entity_cert_id.clone()
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(
            "fabric-a".into(),
            PathBuf::from("/tmp/ca.pem"),
            PathBuf::from("/tmp/client.pem"),
            PathBuf::from("/tmp/client.key"),
        )
    }

    /// Test material whose IDs carry `imported_at`, so a run of installs gets
    /// distinct, ordered IDs however fast it runs.
    #[cfg(test)]
    pub(crate) fn for_test_at(imported_at: chrono::NaiveDateTime) -> Self {
        let remote_run_id = imported_at.format(REMOTE_RUN_ID_FORMAT).to_string();
        let (ca_cert_id, entity_cert_id) =
            cert_ids_for_domain_with_suffix("fabric-a", &remote_run_id);

        Self {
            remote_run_id,
            ca_cert_id,
            entity_cert_id,
            ..Self::for_test()
        }
    }
}

impl SwitchGb200Nvidia {
    /// Returns the `(ca_id, cert_id)` bound to the NVUE API, which
    /// `ConfigureSwitchCertificate` must have configured first.
    async fn nvue_api_material_ids(&self) -> Result<(String, String)> {
        let api = self
            .nvue_client()?
            .get_system_api(HttpClient::DEFAULT_TIMEOUT)
            .await?;

        let cert_id = api
            .certificate()
            .filter(|value| !value.is_empty() && *value != "self-signed")
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "NVUE API certificate is not configured; run ConfigureSwitchCertificate first",
                )
            })?
            .to_string();

        let mtls = self.get_nvue_api_mtls_configuration().await?;

        let ca_id = mtls
            .ca_certificate()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "NVUE API mTLS CA certificate is not configured; run \
                     ConfigureSwitchCertificate first",
                )
            })?
            .to_string();

        Ok((ca_id, cert_id))
    }

    /// Reuses the active NVUE API certificate objects for NMX Controller mTLS.
    ///
    /// The certificate RPC provisions and verifies NVUE before scale-up
    /// reconciliation selects a primary. Reusing that binding avoids importing
    /// duplicate certificate objects during each primary rotation.
    pub(crate) async fn bind_nmx_controller_to_nvue_api_material(&self) -> Result<()> {
        let (ca_id, cert_id) = self.nvue_api_material_ids().await?;

        self.bind_cluster_app_mtls("nmx-controller", &ca_id, &cert_id)
            .await?;

        self.verify_cluster_app_mtls("nmx-controller", &ca_id, &cert_id)
            .await?;

        Ok(())
    }

    /// Reports whether NMX Controller mTLS is bound to the certificate objects
    /// the NVUE API currently uses.
    ///
    /// Certificate rotation refreshes the NVUE binding first, so a mismatch
    /// means NMX Controller still serves the superseded certificate even while
    /// that certificate keeps validating.
    pub(crate) async fn nmx_controller_binding_is_current(&self) -> Result<bool> {
        let (ca_id, cert_id) = self.nvue_api_material_ids().await?;

        let certificate = self
            .get_cluster_app_manager_leaf("nmx-controller", "certificate")
            .await?;
        let ca_certificate = self
            .get_cluster_app_manager_leaf("nmx-controller", "ca-certificate")
            .await?;
        let encryption = self
            .get_cluster_app_manager_leaf("nmx-controller", "encryption")
            .await?;

        let bound_cert = extract_string_field(&certificate, "certificate");
        let bound_ca = extract_string_field(&ca_certificate, "ca-certificate");
        let bound_encryption = extract_string_field(&encryption, "encryption");
        let is_current = bound_cert == Some(cert_id.as_str())
            && bound_ca == Some(ca_id.as_str())
            && bound_encryption == Some("mtls");

        tracing::debug!(
            node = %self.id(),
            nvue_certificate = %cert_id,
            nvue_ca_certificate = %ca_id,
            nmx_certificate = bound_cert.unwrap_or("(unset)"),
            nmx_ca_certificate = bound_ca.unwrap_or("(unset)"),
            nmx_encryption = bound_encryption.unwrap_or("(unset)"),
            is_current,
            "compared NMX Controller mTLS binding with NVUE API material"
        );

        Ok(is_current)
    }

    /// Clear mTLS for `services` and persist the result.
    ///
    /// Returns a per-service report: which bindings were cleared, which were
    /// already in the desired state, and which were never attempted. The
    /// `nv` command lines behind those changes stay in this node's tracing
    /// output and never reach the report.
    ///
    /// `cancel` is honoured between commands, never during one, and a
    /// cancelled run comes back as `Ok` with
    /// [`MtlsUnsetReport::cancelled`] set. Stopping is not the same as doing
    /// nothing: `nv action restore` takes effect on the switch immediately,
    /// so the report describes the work already issued rather than writing
    /// the whole run off as unattempted.
    ///
    /// # Errors
    ///
    /// A failed run returns [`MtlsUnsetError`], which carries the same report
    /// covering the steps that did run, so a caller can still say which
    /// services took effect before the run stopped.
    pub(crate) async fn unset_mtls_services(
        &self,
        services: &[SwitchMtlsService],
        cancel: &CancellationToken,
    ) -> std::result::Result<MtlsUnsetReport, MtlsUnsetError> {
        let invalid = |error: RmsError| MtlsUnsetError {
            error,
            report: MtlsUnsetReport::not_attempted(services),
        };

        if services.is_empty() {
            return Err(invalid(RmsError::invalid_argument(
                "services must contain at least one SwitchService",
            )));
        }

        let flow = mtls_unset_flow(services).map_err(invalid)?;

        tracing::info!(
            node = %self.id(),
            ?services,
            command_count = flow.commands().len(),
            "unsetting switch service mTLS mode via SSH"
        );

        let cli = self.nv_cli().await.map_err(invalid)?;

        match cli.run_flow_while(&flow, || !cancel.is_cancelled()).await {
            Ok(report) => Ok(MtlsUnsetReport::from_flow(&report, services)),
            Err(failure) => {
                let report = MtlsUnsetReport::from_flow(&failure.report, services);

                // Prefer the attributed description: it names the service and
                // stage the run stopped on, which the raw transport error
                // cannot. Either way the transport's own classification is
                // preserved, and either way the command text was already
                // stripped at the transport boundary.
                let error = match &report.failure {
                    Some(site) => RmsError::new(failure.failure.code, site.describe()),
                    None => failure.failure,
                };

                Err(MtlsUnsetError { error, report })
            }
        }
    }

    pub(crate) async fn prepare_mtls_remote_dirs(
        &self,
        material: &SwitchMtlsMaterialPaths,
    ) -> Result<()> {
        let remote_dir = material.remote_dir();
        tracing::debug!(
            node = %self.id(),
            remote_dir = %remote_dir,
            tls_root = REMOTE_TLS_ROOT,
            "prepare_mtls_remote_dirs stage starting"
        );
        tracing::info!(
            node = %self.id(),
            remote_dir = %remote_dir,
            "preparing remote TLS directories on switch"
        );

        #[cfg(test)]
        if self.ssh_exec_for_test.is_some() {
            tracing::debug!(node = %self.id(), "prepare_mtls_remote_dirs skipped in test mode");
            return Ok(());
        }

        let cli = self.nv_cli().await?;
        tracing::debug!(
            node = %self.id(),
            remote_dir = %remote_dir,
            "SSH session established for remote TLS directory preparation"
        );

        // Create the root explicitly before the per-run directory so it gets
        // the restricted mode outright, rather than whatever mode the platform
        // gives a parent it creates implicitly.
        for str_path in [REMOTE_TLS_ROOT, remote_dir.as_str()] {
            let path = RemotePath::new(str_path)?;

            cli.create_directory(&path, SWITCH_CERTIFICATE_REMOTE_DIR_PERMISSIONS)
                .await?;

            tracing::debug!(
                node = %self.id(),
                path = %str_path,
                "created remote TLS directory"
            );
        }

        tracing::info!(
            node = %self.id(),
            remote_dir = %remote_dir,
            "remote TLS directories prepared on switch"
        );
        Ok(())
    }

    /// Remove the per-run staging directory created by
    /// [`Self::prepare_mtls_remote_dirs`].
    ///
    /// The staged files under `remote_dir` exist only to feed the NVUE
    /// `@import` `file://` URIs in [`Self::import_mtls_material`]. Once the
    /// switch has imported them into its own security store, the raw copies --
    /// including the client private key -- are redundant, so this removes the
    /// whole per-run directory. `remote_dir` is a timestamped path this code
    /// constructs, never caller input, so recursive removal is safe here.
    pub(crate) async fn cleanup_mtls_remote_dir(
        &self,
        material: &SwitchMtlsMaterialPaths,
    ) -> Result<()> {
        let remote_dir = material.remote_dir();

        // Unlike `prepare_mtls_remote_dirs` and `sftp_copy_mtls_file` -- which
        // open a real SSH/SFTP session and so must be skipped under the test
        // stub -- this path issues a single command through `nv_cli`, whose test
        // transport records it. There is deliberately no `#[cfg(test)]`
        // short-circuit here: letting the removal run keeps the "the client key
        // never outlives the job" guarantee exercised in tests rather than
        // silently no-opped.
        //
        // `nv_cli()` opens a fresh SSH session rather than reusing an earlier
        // one. That matches this workflow's per-stage reconnect design and is
        // the deliberate choice on the failure path in particular: cleanup runs
        // even when a prior stage errored, so a previously-opened session may be
        // in an unknown state. The extra connect is immaterial next to the
        // job's NVUE HTTP round-trips.
        let path = RemotePath::new(remote_dir.as_str())?;
        self.nv_cli().await?.remove_directory(&path).await?;

        tracing::info!(
            node = %self.id(),
            remote_dir = %remote_dir,
            "removed staged TLS material from switch"
        );
        Ok(())
    }

    /// Best-effort variant of [`Self::cleanup_mtls_remote_dir`]: logs and
    /// swallows any error so it can be called on both the success and failure
    /// paths of the certificate job without changing the job's outcome.
    ///
    /// Cleanup is a hygiene step -- a failure here leaves recoverable scratch
    /// state on the switch, so it must never turn a successful install into a
    /// failure, nor mask the original error on a failing path.
    pub(crate) async fn cleanup_mtls_remote_dir_best_effort(
        &self,
        material: &SwitchMtlsMaterialPaths,
    ) {
        if let Err(e) = self.cleanup_mtls_remote_dir(material).await {
            tracing::warn!(
                node = %self.id(),
                remote_dir = %material.remote_dir(),
                error = %e.message,
                "failed to remove staged TLS material; leaving it for manual cleanup"
            );
        }
    }

    pub(crate) async fn sftp_copy_mtls_file(
        &self,
        material: &SwitchMtlsMaterialPaths,
        remote_file_name: &str,
        cancel: CancellationToken,
    ) -> Result<MtlsSftpCopyOutcome> {
        let spec = mtls_remote_file_specs(material)
            .into_iter()
            .find(|file| file.remote_name == remote_file_name)
            .ok_or_else(|| {
                RmsError::invalid_argument(format!(
                    "unknown TLS remote file name: {remote_file_name}"
                ))
            })?;

        let remote_dir = material.remote_dir();
        let remote_path = format!("{remote_dir}/{}", spec.remote_name);
        if cancel.is_cancelled() {
            return Err(RmsError::cancelled(format!(
                "SFTP copy cancelled before uploading {remote_path}"
            )));
        }
        tracing::debug!(
            node = %self.id(),
            remote_file = spec.remote_name,
            local_path = %spec.local_path.display(),
            remote_path = %remote_path,
            permissions = %spec.permissions,
            "sftp_copy_mtls_file stage starting"
        );

        #[cfg(test)]
        if self.ssh_exec_for_test.is_some() {
            tracing::debug!(
                node = %self.id(),
                remote_path = %remote_path,
                "sftp_copy_mtls_file skipped in test mode"
            );
            return Ok(MtlsSftpCopyOutcome {
                remote_path,
                bytes: 0,
            });
        }

        let local_meta = tokio::fs::metadata(spec.local_path).await.map_err(|e| {
            RmsError::invalid_argument(format!(
                "TLS material file {} not found: {e}",
                spec.local_path.display()
            ))
        })?;
        tracing::debug!(
            node = %self.id(),
            remote_path = %remote_path,
            local_bytes = local_meta.len(),
            "resolved local TLS material metadata for SFTP copy"
        );

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        tracing::info!(
            node = %self.id(),
            local_path = %spec.local_path.display(),
            remote_path = %remote_path,
            bytes = local_meta.len(),
            "copying TLS material file to switch via SFTP"
        );

        ssh.sftp_upload(
            spec.local_path.to_string_lossy().as_ref(),
            &remote_path,
            SshClient::UPLOAD_TIMEOUT,
            cancel,
        )
        .await?;
        tracing::debug!(
            node = %self.id(),
            remote_path = %remote_path,
            bytes = local_meta.len(),
            "SFTP upload completed; verifying remote file"
        );

        let cli = self.nv_cli_over(ssh)?;
        let verified_path = RemotePath::new(remote_path.clone())?;

        cli.confirm_file(&verified_path, "SFTP upload").await?;
        tracing::debug!(
            node = %self.id(),
            remote_path = %remote_path,
            "remote TLS file presence verified after SFTP copy"
        );

        cli.set_permissions(&verified_path, spec.permissions)
            .await?;
        tracing::debug!(
            node = %self.id(),
            remote_path = %remote_path,
            mode = %spec.permissions,
            "set remote TLS file permissions after SFTP copy"
        );

        tracing::info!(
            node = %self.id(),
            remote_path = %remote_path,
            bytes = local_meta.len(),
            "TLS material file copied to switch via SFTP"
        );

        Ok(MtlsSftpCopyOutcome {
            remote_path,
            bytes: local_meta.len(),
        })
    }

    pub(crate) async fn import_mtls_material(
        &self,
        material: &SwitchMtlsMaterialPaths,
    ) -> Result<()> {
        if self.mtls_skip_nvue_for_test() {
            tracing::debug!(node = %self.id(), "import_mtls_material skipped in test mode");
            return Ok(());
        }

        let remote_dir = material.remote_dir();
        let ca_uri = format!("file://{remote_dir}/ca.pem");
        let cert_uri = format!("file://{remote_dir}/client.pem");
        let key_uri = format!("file://{remote_dir}/client.key");
        let ca_cert_id = material.ca_cert_id();
        let entity_cert_id = material.entity_cert_id();
        tracing::debug!(
            node = %self.id(),
            remote_dir = %remote_dir,
            ca_cert_id = %ca_cert_id,
            entity_cert_id = %entity_cert_id,
            ca_uri = %ca_uri,
            cert_uri = %cert_uri,
            key_uri = %key_uri,
            "import_mtls_material stage starting"
        );

        tracing::info!(
            node = %self.id(),
            remote_dir = %remote_dir,
            ca_cert_id = %ca_cert_id,
            entity_cert_id = %entity_cert_id,
            "importing TLS material into NVOS security store via NVUE API"
        );

        let ca_endpoint = nvue_ca_certificate_endpoint(&ca_cert_id);
        tracing::debug!(
            node = %self.id(),
            endpoint = %ca_endpoint,
            uri = %ca_uri,
            "importing CA certificate via NVUE API"
        );

        self.nvue_start_action(
            &ca_endpoint,
            "@import",
            ImportCaCertificateParameters { uri: &ca_uri },
        )
        .await?;

        let entity_endpoint = nvue_entity_certificate_endpoint(&entity_cert_id);
        tracing::debug!(
            node = %self.id(),
            endpoint = %entity_endpoint,
            cert_uri = %cert_uri,
            key_uri = %key_uri,
            "importing entity certificate via NVUE API"
        );
        self.nvue_start_action(
            &entity_endpoint,
            "@import",
            ImportEntityCertificateParameters {
                public_key_uri: &cert_uri,
                private_key_uri: &key_uri,
            },
        )
        .await?;

        tracing::info!(
            node = %self.id(),
            ca_cert_id = %ca_cert_id,
            entity_cert_id = %entity_cert_id,
            "imported TLS material into NVOS security store"
        );

        Ok(())
    }

    pub(crate) async fn bind_mtls_service(
        &self,
        material: &SwitchMtlsMaterialPaths,
        service: SwitchMtlsService,
    ) -> Result<()> {
        if self.mtls_skip_nvue_for_test() {
            tracing::debug!(
                node = %self.id(),
                ?service,
                "bind_mtls_service skipped in test mode"
            );
            return Ok(());
        }

        let ca_id = material.ca_cert_id();
        let cert_id = material.entity_cert_id();
        tracing::debug!(
            node = %self.id(),
            ?service,
            ca_id = %ca_id,
            cert_id = %cert_id,
            domain = %material.domain,
            "bind_mtls_service stage starting"
        );

        match service {
            SwitchMtlsService::NvueApi => {
                tracing::debug!(node = %self.id(), service = "nvue-api", "binding NVUE API mTLS");
                let revision_id = self.stage_nvue_mtls_binding(material).await?;

                self.nvue_start_config_revision(&revision_id).await?;
                self.nvue_finish_config_revision(&revision_id).await?;
            }
            SwitchMtlsService::ScaleUpFabricTelemetryInterface => {
                let capabilities = self.initialize_nvue_capabilities().await?;
                if capabilities.is_supported() {
                    tracing::debug!(
                        node = %self.id(),
                        service = "gnmi-server",
                        "binding gNMI server certificate and client CA"
                    );
                } else {
                    tracing::debug!(
                        node = %self.id(),
                        service = "gnmi-server",
                        "binding gNMI server certificate without a client CA"
                    );
                }
                capabilities.bind_gnmi(self, material).await?;
            }
            SwitchMtlsService::ScaleUpFabricTelemetry => {
                tracing::debug!(node = %self.id(), app_name = "nmx-telemetry", "binding cluster app mTLS");
                self.bind_cluster_app_mtls("nmx-telemetry", &ca_id, &cert_id)
                    .await?;
            }
            SwitchMtlsService::ScaleUpFabricManager => {
                tracing::debug!(node = %self.id(), app_name = "nmx-controller", "binding cluster app mTLS");
                self.bind_cluster_app_mtls("nmx-controller", &ca_id, &cert_id)
                    .await?;
            }
        }

        tracing::debug!(
            node = %self.id(),
            ?service,
            ca_id = %ca_id,
            cert_id = %cert_id,
            "bind_mtls_service stage completed"
        );
        Ok(())
    }

    pub(crate) async fn stage_nvue_mtls_binding(
        &self,
        material: &SwitchMtlsMaterialPaths,
    ) -> Result<String> {
        let certificate_id = material.entity_cert_id();
        let ca_certificate_id = material.ca_cert_id();

        let certificate = serde_json::to_value(CertificateBinding {
            certificate: &certificate_id,
        })
        .map_err(|error| {
            RmsError::internal(format!(
                "failed to serialize NVUE API certificate binding: {error}"
            ))
        })?;

        let ca_certificate = serde_json::to_value(CaCertificateBinding {
            ca_certificate: &ca_certificate_id,
        })
        .map_err(|error| {
            RmsError::internal(format!(
                "failed to serialize NVUE API CA certificate binding: {error}"
            ))
        })?;

        self.nvue_stage_config_patches(&[
            (NVUE_SYSTEM_API_PATH, certificate),
            (NVUE_SYSTEM_API_MTLS_PATH, ca_certificate),
        ])
        .await
    }

    pub(crate) async fn verify_mtls_services(
        &self,
        material: &SwitchMtlsMaterialPaths,
        services: &[SwitchMtlsService],
    ) -> Result<Value> {
        let ca_id = material.ca_cert_id();
        let cert_id = material.entity_cert_id();
        let certificate = self
            .get_switch_security_certificate_object(&cert_id)
            .await?;
        let valid_to = verify_certificate_not_expired(&certificate, &cert_id)?;
        let ca_certificate = self
            .get_switch_security_ca_certificate_object(&ca_id)
            .await?;
        let installed_services = certificate_installed_services(&certificate);

        for service in services {
            if let Some(installed_service) = service.installed_certificate_service_name()
                && !installed_services
                    .iter()
                    .any(|actual| actual == installed_service)
            {
                return Err(RmsError::failed_precondition(format!(
                    "switch certificate {cert_id} is not installed for {installed_service}"
                )));
            }
        }

        let nvue_mtls = if services.contains(&SwitchMtlsService::NvueApi) {
            let configuration = self.get_nvue_api_mtls_configuration().await?;

            verify_expected_field(
                configuration.ca_certificate(),
                "ca-certificate",
                &ca_id,
                "NVUE API mTLS",
            )?;

            Some(configuration)
        } else {
            None
        };

        let (gnmi_server, gnmi_mtls, gnmi_client_authentication) =
            if services.contains(&SwitchMtlsService::ScaleUpFabricTelemetryInterface) {
                let (configuration, mtls) = self
                    .initialize_nvue_capabilities()
                    .await?
                    .verify_gnmi(self, material)
                    .await?;
                let authentication = match &mtls {
                    Some(_) => GNMI_CLIENT_AUTHENTICATION_MTLS,
                    None => GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY,
                };
                (Some(configuration), mtls, Some(authentication))
            } else {
                (None, None, None)
            };

        let mut cluster_apps = serde_json::Map::new();
        for service in services {
            if let Some(app_name) = service.cluster_app_name() {
                let status = self
                    .verify_cluster_app_mtls(app_name, &ca_id, &cert_id)
                    .await?;
                cluster_apps.insert(app_name.to_owned(), status);
            }
        }

        Ok(json!({
            "entity_certificate_id": cert_id,
            "ca_certificate_id": ca_id,
            "valid_to": valid_to.to_rfc3339(),
            "installed_services": installed_services,
            "ca_certificate": ca_certificate,
            "nvue_mtls": nvue_mtls,
            "gnmi_server": gnmi_server,
            "gnmi_mtls": gnmi_mtls,
            "gnmi_client_authentication": gnmi_client_authentication,
            "cluster_apps": cluster_apps,
        }))
    }

    async fn verify_cluster_app_mtls(
        &self,
        app_name: &str,
        ca_id: &str,
        cert_id: &str,
    ) -> Result<Value> {
        let certificate = self
            .get_cluster_app_manager_leaf(app_name, "certificate")
            .await?;

        verify_expected_field(
            extract_string_field(&certificate, "certificate"),
            "certificate",
            cert_id,
            &format!("cluster app {app_name} manager certificate"),
        )?;

        let ca_certificate = self
            .get_cluster_app_manager_leaf(app_name, "ca-certificate")
            .await?;

        verify_expected_field(
            extract_string_field(&ca_certificate, "ca-certificate"),
            "ca-certificate",
            ca_id,
            &format!("cluster app {app_name} manager CA certificate"),
        )?;

        let encryption = self
            .get_cluster_app_manager_leaf(app_name, "encryption")
            .await?;

        verify_expected_field(
            extract_string_field(&encryption, "encryption"),
            "encryption",
            "mtls",
            &format!("cluster app {app_name} manager encryption"),
        )?;

        Ok(json!({
            "certificate": certificate,
            "ca_certificate": ca_certificate,
            "encryption": encryption,
        }))
    }

    async fn bind_cluster_app_mtls(
        &self,
        app_name: &str,
        ca_id: &str,
        cert_id: &str,
    ) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app name: {app_name}"
            )));
        }
        if !is_valid_identifier(ca_id) || !is_valid_identifier(cert_id) {
            return Err(RmsError::invalid_argument(
                "certificate identifiers contain invalid characters",
            ));
        }

        // NVOS requires cluster + manager to be enabled before cert/CA/encryption updates.
        self.set_cluster_state(true).await?;
        self.enable_grpc_for_external_clients(app_name, true)
            .await?;

        // Enabling the manager only converges its NVUE state. NVOS applies each
        // field update below by calling back into the app itself, so a manager
        // that is enabled while its app is still starting rejects the first
        // update with "CallBack to nmx app manager failed". Wait for the app
        // before the first update; the per-field settle delay further down only
        // spaces the updates that follow it.
        self.require_cluster_app_manager_action_ready(app_name)
            .await?;

        let cert_bindings = [
            (
                "certificate",
                serde_json::to_value(ClusterAppManagerCertificateParameters {
                    certificate_id: cert_id,
                })
                .map_err(|error| {
                    RmsError::internal(format!(
                        "failed to serialize cluster app certificate binding: {error}"
                    ))
                })?,
            ),
            (
                "ca-certificate",
                serde_json::to_value(ClusterAppManagerCaCertificateParameters {
                    ca_certificate_id: ca_id,
                })
                .map_err(|error| {
                    RmsError::internal(format!(
                        "failed to serialize cluster app CA certificate binding: {error}"
                    ))
                })?,
            ),
        ];

        for (field, parameters) in cert_bindings {
            let endpoint = nvue_cluster_app_manager_sub_endpoint(app_name, field);
            tracing::debug!(
                node = %self.id(),
                app_name,
                field,
                endpoint = %endpoint,
                "binding cluster app manager mTLS field via NVUE API"
            );
            self.nvue_start_action(&endpoint, "@update", parameters)
                .await?;

            tokio::time::sleep(CLUSTER_MANAGER_ACTION_SETTLE_DELAY).await;
        }

        // Encryption manager binding prefers the typed NVUE `@update` action
        // (the `encryption` segment binds its value from a `mode` parameter in
        // NVOS `cue_cluster_v1`, so the parameter name is `mode`, not the
        // segment name), but retains an explicit SSH nvCLI backstop for this
        // step: cert installation is a bootstrap where the NVUE channel can be
        // unavailable/mid-transition, and SSH is an independent credential
        // path. See `bind_cluster_app_manager_encryption_mtls`.
        tracing::debug!(
            node = %self.id(),
            app_name,
            field = "encryption",
            "binding cluster app manager encryption (NVUE with SSH backstop)"
        );
        self.bind_cluster_app_manager_encryption_mtls(app_name)
            .await?;

        tracing::debug!(
            node = %self.id(),
            app_name,
            ca_id,
            cert_id,
            "cluster app manager mTLS binding applied via NVUE API"
        );

        tracing::info!(
            node = %self.id(),
            app_name,
            ca_id,
            cert_id,
            "bound cluster app manager to mTLS"
        );
        Ok(())
    }
}

impl GnmiClientCa {
    /// Bind gNMI certificates according to the discovered impl.
    ///
    /// Unsupported client-CA is only the discovery probe
    /// (`GET /system/gnmi-server/mtls` returning 404). That path commits the
    /// entity certificate alone. When the probe says the resource exists, the
    /// entity certificate and client CA share one revision. A CA patch failure
    /// discards that revision before apply, so the switch does not keep a new
    /// entity certificate beside the previous client CA.
    pub(crate) async fn bind_gnmi(
        self,
        switch: &SwitchGb200Nvidia,
        material: &SwitchMtlsMaterialPaths,
    ) -> Result<()> {
        let cert_id = material.entity_cert_id();
        let ca_id = material.ca_cert_id();

        let certificate = serde_json::to_value(CertificateBinding {
            certificate: &cert_id,
        })
        .map_err(|error| {
            RmsError::internal(format!(
                "failed to serialize gNMI certificate binding: {error}"
            ))
        })?;

        if !self.is_supported() {
            tracing::warn!(
                node = %switch.id(),
                "gNMI client-CA resource is absent; binding the server certificate only"
            );
            return switch
                .nvue_apply_config_patches(&[(GNMI_SERVER_ENDPOINT, certificate)])
                .await;
        }

        let ca_certificate = serde_json::to_value(CaCertificateBinding {
            ca_certificate: &ca_id,
        })
        .map_err(|error| {
            RmsError::internal(format!(
                "failed to serialize gNMI CA certificate binding: {error}"
            ))
        })?;

        switch
            .nvue_apply_config_patches(&[
                (GNMI_SERVER_ENDPOINT, certificate),
                (GNMI_SERVER_MTLS_ENDPOINT, ca_certificate),
            ])
            .await
    }

    /// Verify gNMI bind. `gnmi_mtls` is `null` when client-CA is unsupported,
    /// and the verification object records `gnmi_client_authentication` as
    /// `server-certificate-only`. When the probe said the resource exists, a
    /// missing mTLS body fails verification and a bound CA records `mtls`.
    pub(crate) async fn verify_gnmi(
        self,
        switch: &SwitchGb200Nvidia,
        material: &SwitchMtlsMaterialPaths,
    ) -> Result<(Value, Option<MtlsConfiguration>)> {
        let cert_id = material.entity_cert_id();
        let ca_id = material.ca_cert_id();
        let configuration = switch.get_gnmi_server_configuration().await?;

        verify_expected_field(
            extract_string_field(&configuration, "certificate"),
            "certificate",
            &cert_id,
            "gNMI server",
        )?;

        if !self.is_supported() {
            return Ok((configuration, None));
        }

        let mtls = switch.get_gnmi_server_mtls_if_present().await?;
        let Some(mtls_configuration) = mtls else {
            return Err(RmsError::failed_precondition(
                "gNMI client-CA resource disappeared before verification: GET /nvue_v1/system/gnmi-server/mtls returned 404",
            ));
        };

        verify_expected_field(
            mtls_configuration.ca_certificate(),
            "ca-certificate",
            &ca_id,
            "gNMI server mTLS",
        )?;

        Ok((configuration, Some(mtls_configuration)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::super::config;

    const GNMI_MTLS_UNSUPPORTED_ERROR: &str =
        "Error: 'mtls' is not one of ['state', 'certificate']";

    const GNMI_MTLS_CERTIFICATE_UNSET_COMMAND: &str =
        "nv unset system gnmi-server mtls certificate";
    const GNMI_MTLS_CA_CERTIFICATE_UNSET_COMMAND: &str =
        "nv unset system gnmi-server mtls ca-certificate";

    type RecordedCommands = Arc<std::sync::Mutex<Vec<String>>>;

    /// A token for tests that are not about cancellation.
    fn no_cancel() -> CancellationToken {
        CancellationToken::new()
    }

    fn switch_for_gnmi_unset_test(
        primary_error: &'static str,
        fallback_error: Option<&'static str>,
    ) -> (SwitchGb200Nvidia, RecordedCommands) {
        let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&commands);

        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(
            move |command| {
                recorded.lock().unwrap().push(command.to_owned());

                match command {
                    GNMI_MTLS_CERTIFICATE_UNSET_COMMAND => Err(RmsError::internal(primary_error)),
                    GNMI_MTLS_CA_CERTIFICATE_UNSET_COMMAND => fallback_error
                        .map(RmsError::internal)
                        .map_or_else(|| Ok(String::new()), Err),
                    _ => Ok(String::new()),
                }
            },
        );

        (switch, commands)
    }

    /// A switch whose SSH transport records every command and fails exactly
    /// the ones in `failing`.
    fn recording_switch(
        failing: &'static [(&'static str, &'static str)],
    ) -> (SwitchGb200Nvidia, RecordedCommands) {
        let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&commands);

        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(
            move |command| {
                recorded.lock().unwrap().push(command.to_owned());

                failing
                    .iter()
                    .find(|(failing, _)| *failing == command)
                    .map_or_else(
                        || Ok(String::new()),
                        |(_, error)| Err(RmsError::internal(*error)),
                    )
            },
        );

        (switch, commands)
    }

    fn assert_recorded_commands(recorded: &RecordedCommands, expected: &[&str]) {
        assert_eq!(*recorded.lock().unwrap(), expected);
    }

    /// `(service name, status)` for each service in `report`.
    fn service_statuses(report: &MtlsUnsetReport) -> Vec<(&'static str, &'static str)> {
        report
            .services
            .iter()
            .map(|outcome| (outcome.service.as_str(), outcome.status.as_str()))
            .collect()
    }

    /// `(stage, status)` for each persist operation in `report`.
    fn persist_statuses(report: &MtlsUnsetReport) -> Vec<(&'static str, &'static str)> {
        report
            .persist
            .iter()
            .map(|outcome| (outcome.stage.as_str(), outcome.status.as_str()))
            .collect()
    }

    /// The persist pair every fully-executed flow ends with.
    const PERSIST_COMPLETED: [(&str, &str); 2] =
        [("config_apply", "cleared"), ("config_save", "cleared")];

    const PERSIST_NOT_ATTEMPTED: [(&str, &str); 2] = [
        ("config_apply", "not_attempted"),
        ("config_save", "not_attempted"),
    ];

    #[test]
    fn sftp_copy_stage_name_sanitizes_dots() {
        assert_eq!(sftp_copy_stage_name("client.pem"), "sftp_copy_client_pem");
    }

    #[tokio::test]
    async fn sftp_copy_mtls_file_stops_when_already_cancelled() {
        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:1")
            .with_ssh_exec_for_test(|_| Ok(String::new()));
        let material = SwitchMtlsMaterialPaths::new(
            "fabric-a".into(),
            PathBuf::from("/tmp/ca.pem"),
            PathBuf::from("/tmp/client.pem"),
            PathBuf::from("/tmp/client.key"),
        );
        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = switch
            .sftp_copy_mtls_file(&material, "ca.pem", cancel)
            .await
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::Cancelled);
    }

    /// The whole point of cleanup is "the client key never outlives the job",
    /// so pin that it actually issues the `rm -rf` of the scratch dir rather
    /// than no-opping under the test transport.
    #[tokio::test]
    async fn cleanup_mtls_remote_dir_removes_the_staged_directory() {
        let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&commands);
        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(
            move |command| {
                recorded.lock().unwrap().push(command.to_owned());
                Ok(String::new())
            },
        );
        let material = SwitchMtlsMaterialPaths::new(
            "fabric-a".into(),
            PathBuf::from("/tmp/ca.pem"),
            PathBuf::from("/tmp/client.pem"),
            PathBuf::from("/tmp/client.key"),
        );

        switch.cleanup_mtls_remote_dir(&material).await.unwrap();

        assert_eq!(
            *commands.lock().unwrap(),
            vec![format!("rm -rf '{}'", material.remote_dir())],
        );
    }

    /// The best-effort variant runs on both the success and failure paths of
    /// the certificate job, so it must still attempt the removal but must never
    /// propagate or panic when the switch refuses it.
    #[tokio::test]
    async fn cleanup_mtls_remote_dir_best_effort_attempts_removal_and_swallows_errors() {
        let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&commands);
        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(
            move |command| {
                recorded.lock().unwrap().push(command.to_owned());
                Err(RmsError::internal("switch refused the removal"))
            },
        );
        let material = SwitchMtlsMaterialPaths::new(
            "fabric-a".into(),
            PathBuf::from("/tmp/ca.pem"),
            PathBuf::from("/tmp/client.pem"),
            PathBuf::from("/tmp/client.key"),
        );

        switch.cleanup_mtls_remote_dir_best_effort(&material).await;

        assert_eq!(
            *commands.lock().unwrap(),
            vec![format!("rm -rf '{}'", material.remote_dir())],
        );
    }

    #[test]
    fn mtls_remote_file_specs_lists_all_tls_files() {
        let material = SwitchMtlsMaterialPaths::new(
            "fabric-a".into(),
            std::path::PathBuf::from("/tmp/ca.pem"),
            std::path::PathBuf::from("/tmp/client.pem"),
            std::path::PathBuf::from("/tmp/client.key"),
        );
        let names: Vec<&str> = mtls_remote_file_specs(&material)
            .iter()
            .map(|spec| spec.remote_name)
            .collect();
        assert_eq!(names, vec!["ca.pem", "client.pem", "client.key"]);
    }

    #[tokio::test]
    async fn nmx_controller_binding_validates_api_certificate_before_reading_mtls() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(NVUE_SYSTEM_API_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"certificate": "self-signed"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(NVUE_SYSTEM_API_MTLS_PATH))
            .respond_with(ResponseTemplate::new(503))
            .expect(0)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let error = switch
            .bind_nmx_controller_to_nvue_api_material()
            .await
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::FailedPrecondition);

        assert!(
            error
                .message
                .contains("run ConfigureSwitchCertificate first")
        );
    }

    async fn mount_nmx_controller_binding(
        server: &MockServer,
        nvue_cert: &str,
        nvue_ca: &str,
        nmx_cert: &str,
        nmx_ca: &str,
        nmx_encryption: &str,
    ) {
        let responses = [
            (
                NVUE_SYSTEM_API_PATH.to_string(),
                json!({"certificate": nvue_cert}),
            ),
            (
                NVUE_SYSTEM_API_MTLS_PATH.to_string(),
                json!({"ca-certificate": nvue_ca}),
            ),
            (
                "/nvue_v1/cluster/apps/nmx-controller/manager/certificate".to_string(),
                json!({"certificate": nmx_cert}),
            ),
            (
                "/nvue_v1/cluster/apps/nmx-controller/manager/ca-certificate".to_string(),
                json!({"ca-certificate": nmx_ca}),
            ),
            (
                "/nvue_v1/cluster/apps/nmx-controller/manager/encryption".to_string(),
                json!({"encryption": nmx_encryption}),
            ),
        ];

        for (endpoint, body) in responses {
            Mock::given(method("GET"))
                .and(path(endpoint))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(server)
                .await;
        }
    }

    #[tokio::test]
    async fn nmx_controller_binding_is_current_when_ids_match_nvue_api() {
        let server = MockServer::start().await;
        mount_nmx_controller_binding(&server, "cert-2", "ca-1", "cert-2", "ca-1", "mtls").await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        assert!(switch.nmx_controller_binding_is_current().await.unwrap());
    }

    #[tokio::test]
    async fn nmx_controller_binding_is_stale_when_nvue_certificate_was_rotated() {
        let server = MockServer::start().await;
        mount_nmx_controller_binding(&server, "cert-2", "ca-1", "cert-1", "ca-1", "mtls").await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        assert!(!switch.nmx_controller_binding_is_current().await.unwrap());
    }

    #[tokio::test]
    async fn nmx_controller_binding_is_stale_when_encryption_is_not_mtls() {
        let server = MockServer::start().await;
        mount_nmx_controller_binding(&server, "cert-2", "ca-1", "cert-2", "ca-1", "none").await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        assert!(!switch.nmx_controller_binding_is_current().await.unwrap());
    }

    #[tokio::test]
    async fn nmx_controller_binding_is_stale_when_only_nvue_ca_was_rotated() {
        let server = MockServer::start().await;
        mount_nmx_controller_binding(&server, "cert-2", "ca-2", "cert-2", "ca-1", "mtls").await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        assert!(!switch.nmx_controller_binding_is_current().await.unwrap());
    }

    #[tokio::test]
    async fn nmx_controller_binding_is_unreadable_when_ca_leaf_returns_404() {
        let server = MockServer::start().await;
        mount_nmx_controller_binding(&server, "cert-2", "ca-1", "cert-2", "ca-1", "mtls").await;

        Mock::given(method("GET"))
            .and(path(
                "/nvue_v1/cluster/apps/nmx-controller/manager/ca-certificate",
            ))
            .respond_with(ResponseTemplate::new(404))
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let error = switch
            .nmx_controller_binding_is_current()
            .await
            .unwrap_err();

        assert!(error.message.contains("404"));
    }

    #[tokio::test]
    async fn nmx_controller_binding_is_stale_when_certificate_field_is_missing() {
        let server = MockServer::start().await;
        mount_nmx_controller_binding(&server, "cert-2", "ca-1", "cert-2", "ca-1", "mtls").await;

        Mock::given(method("GET"))
            .and(path(
                "/nvue_v1/cluster/apps/nmx-controller/manager/certificate",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        assert!(!switch.nmx_controller_binding_is_current().await.unwrap());
    }

    #[tokio::test]
    async fn stable_material_detects_source_changes() {
        let directory = tempfile::tempdir().unwrap();
        let paths = ["ca.pem", "client.pem", "client.key"].map(|name| directory.path().join(name));

        for path in &paths {
            tokio::fs::write(path, b"original").await.unwrap();
        }

        let material = SwitchMtlsMaterialPaths::load_stable(
            "fabric-a".into(),
            paths[0].clone(),
            paths[1].clone(),
            paths[2].clone(),
        )
        .await
        .unwrap();
        let client_tls = StableNmxcTlsConfig::load(NmxcTlsConfig {
            ca_cert_path: Some(paths[0].clone()),
            client_cert_path: Some(paths[1].clone()),
            client_key_path: Some(paths[2].clone()),
            authority: Some("switch.example.com".into()),
        })
        .await
        .unwrap();

        tokio::fs::write(&paths[2], b"rotated").await.unwrap();

        assert_eq!(
            tokio::fs::read(&material.entity_key_path).await.unwrap(),
            b"original"
        );
        assert_eq!(
            tokio::fs::read(client_tls.as_config().client_key_path.as_ref().unwrap())
                .await
                .unwrap(),
            b"original"
        );
    }

    #[test]
    fn remote_tls_directories_are_created_with_restricted_permissions() {
        assert_eq!(SWITCH_CERTIFICATE_REMOTE_DIR_PERMISSIONS.to_string(), "751");
        assert_eq!(
            SWITCH_CERTIFICATE_PUBLIC_FILE_PERMISSIONS.to_string(),
            "644"
        );
        assert_eq!(
            SWITCH_CERTIFICATE_PRIVATE_KEY_PERMISSIONS.to_string(),
            "400"
        );
    }

    #[test]
    fn cert_ids_for_domain_sanitizes_labels_and_appends_suffix() {
        let suffix = "2025-06-24_12-30-45_123";
        let (ca, cert) = cert_ids_for_domain_with_suffix("switch-a.example.com", suffix);
        assert_eq!(ca, "rms-switch-a-example-com-ca-2025-06-24_12-30-45_123");
        assert_eq!(
            cert,
            "rms-switch-a-example-com-cert-2025-06-24_12-30-45_123"
        );
    }

    #[test]
    fn cert_ids_for_domain_appends_timestamp_suffix() {
        let (ca, cert) = cert_ids_for_domain("site-wide");
        assert!(ca.starts_with("rms-site-wide-ca-"));
        assert!(cert.starts_with("rms-site-wide-cert-"));
        let suffix = ca.strip_prefix("rms-site-wide-ca-").expect("ca prefix");
        assert_eq!(cert, format!("rms-site-wide-cert-{suffix}"));
        chrono::NaiveDateTime::parse_from_str(suffix, REMOTE_RUN_ID_FORMAT)
            .expect("cert id suffix should be a readable UTC timestamp");
    }

    #[test]
    fn material_cert_ids_are_stable_for_one_instance() {
        let material = SwitchMtlsMaterialPaths::new(
            "switch.example.com".into(),
            PathBuf::from("/tmp/ca.pem"),
            PathBuf::from("/tmp/client.pem"),
            PathBuf::from("/tmp/client.key"),
        );
        assert_eq!(material.ca_cert_id(), material.ca_cert_id());
        assert_eq!(material.entity_cert_id(), material.entity_cert_id());
        assert!(
            material
                .ca_cert_id()
                .ends_with(&material.remote_dir().rsplit('/').next().unwrap())
        );
    }

    #[test]
    fn remote_dir_uses_uri_safe_timestamp_instead_of_nvlink_domain() {
        let material = SwitchMtlsMaterialPaths::new(
            "fabric #?%".into(),
            PathBuf::from("/tmp/ca.pem"),
            PathBuf::from("/tmp/client.pem"),
            PathBuf::from("/tmp/client.key"),
        );

        let remote_dir = material.remote_dir();
        assert!(remote_dir.starts_with(&format!("{REMOTE_TLS_ROOT}/")));
        assert!(!remote_dir.contains(&material.domain));

        let run_id = remote_dir.rsplit('/').next().expect("run id segment");
        chrono::NaiveDateTime::parse_from_str(run_id, REMOTE_RUN_ID_FORMAT)
            .expect("run id should be a readable UTC timestamp");
        assert_eq!(material.remote_dir(), remote_dir);
    }

    /// Mounts the reads that let `bind_cluster_app_mtls` reach its manager field
    /// updates: an already-enabled cluster, so the cluster revision is skipped,
    /// and an already-enabled manager. The manager `@update` action is mounted
    /// separately, so a test that does not mount it also asserts that no enable
    /// action was attempted.
    async fn mount_enabled_cluster_and_manager(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "state": "enabled",
                "nmxc-conn": { "state": "up" }
            })))
            .named("GET /nvue_v1/cluster")
            .mount(server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "enabled" })))
            .named("GET /nvue_v1/cluster/apps/nmx-controller/manager")
            .mount(server)
            .await;
    }

    async fn request_paths(server: &MockServer) -> Vec<String> {
        server
            .received_requests()
            .await
            .expect("wiremock requests")
            .into_iter()
            .map(|request| request.url.path().to_owned())
            .collect()
    }

    /// Guards the ready path: an app that reports itself ready receives every
    /// manager field update, in order, and needs no manager enable action
    /// because its manager state has already converged.
    #[tokio::test]
    async fn bind_cluster_app_mtls_submits_every_manager_field_update_for_a_ready_app() {
        let server = MockServer::start().await;

        mount_enabled_cluster_and_manager(&server).await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "ok",
                "additional-info": "CONTROL_PLANE_STATE_CONFIGURED",
                "manager": { "state": "enabled" }
            })))
            .named("ready GET /nvue_v1/cluster/apps/nmx-controller")
            .mount(&server)
            .await;

        for field in ["certificate", "ca-certificate", "encryption"] {
            Mock::given(method("POST"))
                .and(path(format!(
                    "/nvue_v1/cluster/apps/nmx-controller/manager/{field}"
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
                .named("POST @update /nvue_v1/cluster/apps/nmx-controller/manager/<field>")
                .mount(&server)
                .await;
        }

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        switch
            .bind_cluster_app_mtls("nmx-controller", "rms-ca", "rms-cert")
            .await
            .unwrap();

        let submitted_field_updates: Vec<_> = request_paths(&server)
            .await
            .into_iter()
            .filter_map(|entry| {
                entry
                    .strip_prefix("/nvue_v1/cluster/apps/nmx-controller/manager/")
                    .map(str::to_owned)
            })
            .collect();

        assert_eq!(
            submitted_field_updates,
            ["certificate", "ca-certificate", "encryption"]
        );
    }

    /// The reported scenario: the manager state converges while the app behind
    /// it is still starting. The enable path spends its own budget and returns
    /// anyway, so only the readiness gate keeps the first certificate update
    /// from reaching an app that cannot yet service the callback.
    #[tokio::test]
    async fn bind_cluster_app_mtls_waits_for_a_starting_app_before_the_first_certificate_update() {
        let server = MockServer::start().await;

        mount_enabled_cluster_and_manager(&server).await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .named("POST @update /nvue_v1/cluster/apps/nmx-controller/manager")
            .mount(&server)
            .await;

        // `enable_grpc_for_external_clients` reads the app status once per
        // pre-action attempt and once more while converging the manager state,
        // so it consumes this many reads before the readiness gate runs.
        let reads_consumed_by_enable = u64::from(config::MAX_RETRY_ATTEMPTS + 1) + 1;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "not ok",
                "additional-info": "CONTROL_PLANE_STATE_UNDEFINED",
                "manager": { "state": "enabled" }
            })))
            .up_to_n_times(reads_consumed_by_enable)
            .named("starting GET /nvue_v1/cluster/apps/nmx-controller")
            .mount(&server)
            .await;

        // Only the readiness gate reads this far. Without it the certificate
        // update is submitted on the starting response and this never serves.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "ok",
                "additional-info": "CONTROL_PLANE_STATE_CONFIGURED",
                "manager": { "state": "enabled" }
            })))
            .expect(1)
            .named("ready GET /nvue_v1/cluster/apps/nmx-controller")
            .mount(&server)
            .await;

        for field in ["certificate", "ca-certificate", "encryption"] {
            Mock::given(method("POST"))
                .and(path(format!(
                    "/nvue_v1/cluster/apps/nmx-controller/manager/{field}"
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
                .named("POST @update /nvue_v1/cluster/apps/nmx-controller/manager/<field>")
                .mount(&server)
                .await;
        }

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        switch
            .bind_cluster_app_mtls("nmx-controller", "rms-ca", "rms-cert")
            .await
            .unwrap();

        let paths = request_paths(&server).await;

        let first_certificate_update = paths
            .iter()
            .position(|entry| entry == "/nvue_v1/cluster/apps/nmx-controller/manager/certificate")
            .expect("certificate update was submitted");

        let app_reads_before_update = paths
            .iter()
            .take(first_certificate_update)
            .filter(|entry| *entry == "/nvue_v1/cluster/apps/nmx-controller")
            .count();

        assert!(
            u64::try_from(app_reads_before_update).unwrap() > reads_consumed_by_enable,
            "expected the readiness gate to read app status after the enable path, \
             saw {app_reads_before_update} read(s) before the certificate update in {paths:?}"
        );
    }

    /// An app that never becomes ready must fail the bind on the bounded
    /// readiness wait, without submitting a certificate update that NVOS cannot
    /// deliver.
    #[tokio::test]
    async fn bind_cluster_app_mtls_submits_no_certificate_update_when_the_app_stays_unready() {
        let server = MockServer::start().await;

        mount_enabled_cluster_and_manager(&server).await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .named("POST @update /nvue_v1/cluster/apps/nmx-controller/manager")
            .mount(&server)
            .await;

        // The manager state converges while the app behind it is still starting:
        // NVOS reports the app health check as failing and no control-plane
        // state it recognizes, so it cannot deliver a configuration callback.
        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "not ok",
                "additional-info": "CONTROL_PLANE_STATE_UNDEFINED",
                "manager": { "state": "enabled" }
            })))
            .named("starting GET /nvue_v1/cluster/apps/nmx-controller")
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let error = switch
            .bind_cluster_app_mtls("nmx-controller", "rms-ca", "rms-cert")
            .await
            .unwrap_err();

        assert_eq!(
            error.message,
            "cluster app nmx-controller did not become ready for manager configuration"
        );

        assert!(
            !request_paths(&server)
                .await
                .contains(&"/nvue_v1/cluster/apps/nmx-controller/manager/certificate".to_owned())
        );
    }

    #[test]
    fn switch_mtls_service_maps_proto_values() {
        assert_eq!(
            SwitchMtlsService::from_proto(rm::SwitchService::NvueApi),
            Some(SwitchMtlsService::NvueApi)
        );
        assert_eq!(
            SwitchMtlsService::from_proto(rm::SwitchService::Unspecified),
            None
        );
    }

    #[test]
    fn expected_field_distinguishes_missing_and_mismatched_values() {
        let missing =
            verify_expected_field(None, "ca-certificate", "rms-ca", "NVUE API mTLS").unwrap_err();

        let mismatch = verify_expected_field(
            Some("other-ca"),
            "ca-certificate",
            "rms-ca",
            "NVUE API mTLS",
        )
        .unwrap_err();

        assert_eq!(
            missing.message,
            "NVUE API mTLS response missing ca-certificate"
        );

        assert_eq!(
            mismatch.message,
            "NVUE API mTLS expected ca-certificate=rms-ca, got other-ca"
        );
    }

    #[test]
    fn nvue_certificate_endpoints_use_security_paths() {
        assert_eq!(
            nvue_ca_certificate_endpoint("rms-example-ca"),
            "/nvue_v1/system/security/ca-certificate/rms-example-ca"
        );
        assert_eq!(
            nvue_entity_certificate_endpoint("rms-example-cert"),
            "/nvue_v1/system/security/certificate/rms-example-cert"
        );
        assert_eq!(
            nvue_cluster_app_manager_sub_endpoint("nmx-controller", "certificate"),
            "/nvue_v1/cluster/apps/nmx-controller/manager/certificate"
        );
        assert_eq!(
            nvue_cluster_app_manager_sub_endpoint("nmx-controller", "ca-certificate"),
            "/nvue_v1/cluster/apps/nmx-controller/manager/ca-certificate"
        );

        assert_eq!(
            nvue_client::system::GNMI_SERVER_MTLS_ENDPOINT,
            "/nvue_v1/system/gnmi-server/mtls"
        );

        assert_eq!(
            nvue_cluster_app_manager_sub_endpoint("nmx-controller", "encryption"),
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption"
        );
    }

    #[test]
    fn nvue_import_parameters_match_certificate_management_api() {
        let ca = serde_json::to_value(ImportCaCertificateParameters {
            uri: "file:///home/admin/certs/ca.pem",
        })
        .unwrap();

        assert_eq!(ca["uri"], "file:///home/admin/certs/ca.pem");

        let entity = serde_json::to_value(ImportEntityCertificateParameters {
            public_key_uri: "file:///home/admin/certs/client.pem",
            private_key_uri: "file:///home/admin/certs/client.key",
        })
        .unwrap();

        assert_eq!(
            entity["uri-public-key"],
            "file:///home/admin/certs/client.pem"
        );
        assert_eq!(
            entity["uri-private-key"],
            "file:///home/admin/certs/client.key"
        );
    }

    #[test]
    fn nvue_cluster_app_manager_update_requests_mtls_binding() {
        let cert = serde_json::to_value(ClusterAppManagerCertificateParameters {
            certificate_id: "rms-example-cert",
        })
        .unwrap();

        assert_eq!(cert["cert-id"], "rms-example-cert");

        let ca = serde_json::to_value(ClusterAppManagerCaCertificateParameters {
            ca_certificate_id: "rms-example-ca",
        })
        .unwrap();

        assert_eq!(ca["cacert-id"], "rms-example-ca");
    }

    /// Guards the RMS-side mapping from the proto service enum to the NVOS
    /// object each service's mTLS binding lives on. The commands themselves are
    /// covered by `nvswitch_cli`.
    #[test]
    fn mtls_unset_flow_maps_every_service_to_its_nvos_object() -> Result<()> {
        let commands = mtls_unset_flow(&[
            SwitchMtlsService::NvueApi,
            SwitchMtlsService::ScaleUpFabricManager,
            SwitchMtlsService::ScaleUpFabricTelemetry,
            SwitchMtlsService::ScaleUpFabricTelemetryInterface,
        ])?
        .commands();

        assert_eq!(
            commands,
            vec![
                "nv unset system api mtls ca-certificate",
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv action restore cluster apps nmx-telemetry manager encryption",
                "nv unset system gnmi-server mtls certificate",
                "nv config apply --assume-yes",
                "nv config save",
            ]
        );

        Ok(())
    }

    /// Every supported service must appear in the report under its own RMS
    /// name, so a caller can tell the two cluster apps apart even though both
    /// go through `MtlsService::ClusterApp`.
    #[tokio::test]
    async fn unset_mtls_services_reports_every_requested_service() {
        let (switch, commands) = recording_switch(&[]);
        let services = [
            SwitchMtlsService::NvueApi,
            SwitchMtlsService::ScaleUpFabricManager,
            SwitchMtlsService::ScaleUpFabricTelemetry,
            SwitchMtlsService::ScaleUpFabricTelemetryInterface,
        ];

        let report = switch
            .unset_mtls_services(&services, &no_cancel())
            .await
            .unwrap();

        assert_eq!(
            service_statuses(&report),
            vec![
                ("nvue-api", "cleared"),
                ("scale-up-fabric-manager", "cleared"),
                ("scale-up-fabric-telemetry", "cleared"),
                ("scale-up-fabric-telemetry-interface", "cleared"),
            ]
        );
        assert_eq!(persist_statuses(&report), PERSIST_COMPLETED);
        assert_eq!(report.failure, None);

        // The report replaces the command list, but the commands themselves
        // must be unchanged; the transport recording is where that is pinned.
        assert_recorded_commands(
            &commands,
            &[
                "nv unset system api mtls ca-certificate",
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv action restore cluster apps nmx-telemetry manager encryption",
                GNMI_MTLS_CERTIFICATE_UNSET_COMMAND,
                "nv config apply --assume-yes",
                "nv config save",
            ],
        );
    }

    #[tokio::test]
    async fn unset_mtls_services_ignores_disabled_cluster_restore_error() {
        let (switch, commands) = recording_switch(&[(
            "nv action restore cluster apps nmx-controller manager encryption",
            "Action failed with the following issue: cluster is not enabled",
        )]);

        let report = switch
            .unset_mtls_services(&[SwitchMtlsService::ScaleUpFabricManager], &no_cancel())
            .await
            .unwrap();

        assert_eq!(
            service_statuses(&report),
            vec![("scale-up-fabric-manager", "already_disabled")]
        );
        assert_eq!(persist_statuses(&report), PERSIST_COMPLETED);

        assert_recorded_commands(
            &commands,
            &[
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv config apply --assume-yes",
                "nv config save",
            ],
        );
    }

    #[tokio::test]
    async fn unset_mtls_services_saves_and_reports_already_applied_configuration() {
        let (switch, commands) = recording_switch(&[(
            "nv config apply --assume-yes",
            "Revision 'applied' has already been applied.",
        )]);

        let report = switch
            .unset_mtls_services(&[SwitchMtlsService::NvueApi], &no_cancel())
            .await
            .unwrap();

        assert_eq!(service_statuses(&report), vec![("nvue-api", "cleared")]);
        assert_eq!(persist_statuses(&report), PERSIST_COMPLETED);
        assert_eq!(report.failure, None);
        assert!(!report.cancelled);
        assert_recorded_commands(
            &commands,
            &[
                "nv unset system api mtls ca-certificate",
                "nv config apply --assume-yes",
                "nv config save",
            ],
        );
    }

    #[tokio::test]
    async fn unset_mtls_services_retries_gnmi_with_ca_certificate_syntax() {
        let (switch, commands) = switch_for_gnmi_unset_test("unknown field certificate", None);

        let report = switch
            .unset_mtls_services(
                &[SwitchMtlsService::ScaleUpFabricTelemetryInterface],
                &no_cancel(),
            )
            .await
            .unwrap();

        // The alternate spelling is an NVOS detail, so it changes which
        // command runs but not the service's reported status.
        assert_eq!(
            service_statuses(&report),
            vec![("scale-up-fabric-telemetry-interface", "cleared")]
        );

        assert_recorded_commands(
            &commands,
            &[
                GNMI_MTLS_CERTIFICATE_UNSET_COMMAND,
                GNMI_MTLS_CA_CERTIFICATE_UNSET_COMMAND,
                "nv config apply --assume-yes",
                "nv config save",
            ],
        );
    }

    #[tokio::test]
    async fn unset_mtls_services_handles_unsupported_gnmi_alone_and_with_other_services() {
        // gNMI alone: an absent schema leaves nothing to apply, so the flow
        // completes early and both persist steps stay unattempted.
        let (switch, commands) = switch_for_gnmi_unset_test(
            GNMI_MTLS_UNSUPPORTED_ERROR,
            Some(GNMI_MTLS_UNSUPPORTED_ERROR),
        );

        let report = switch
            .unset_mtls_services(
                &[SwitchMtlsService::ScaleUpFabricTelemetryInterface],
                &no_cancel(),
            )
            .await
            .unwrap();

        assert_eq!(
            service_statuses(&report),
            vec![("scale-up-fabric-telemetry-interface", "unsupported")]
        );
        assert_eq!(persist_statuses(&report), PERSIST_NOT_ATTEMPTED);
        assert_recorded_commands(
            &commands,
            &[
                GNMI_MTLS_CERTIFICATE_UNSET_COMMAND,
                GNMI_MTLS_CA_CERTIFICATE_UNSET_COMMAND,
            ],
        );

        // Alongside another service there is still a change to persist.
        let (switch, commands) = switch_for_gnmi_unset_test(
            GNMI_MTLS_UNSUPPORTED_ERROR,
            Some(GNMI_MTLS_UNSUPPORTED_ERROR),
        );

        let report = switch
            .unset_mtls_services(
                &[
                    SwitchMtlsService::NvueApi,
                    SwitchMtlsService::ScaleUpFabricTelemetryInterface,
                ],
                &no_cancel(),
            )
            .await
            .unwrap();

        assert_eq!(
            service_statuses(&report),
            vec![
                ("nvue-api", "cleared"),
                ("scale-up-fabric-telemetry-interface", "unsupported"),
            ]
        );
        assert_eq!(persist_statuses(&report), PERSIST_COMPLETED);
        assert_recorded_commands(
            &commands,
            &[
                "nv unset system api mtls ca-certificate",
                GNMI_MTLS_CERTIFICATE_UNSET_COMMAND,
                GNMI_MTLS_CA_CERTIFICATE_UNSET_COMMAND,
                "nv config apply --assume-yes",
                "nv config save",
            ],
        );
    }

    #[tokio::test]
    async fn unset_mtls_services_stops_when_both_gnmi_command_forms_fail() {
        let (switch, commands) =
            switch_for_gnmi_unset_test(GNMI_MTLS_UNSUPPORTED_ERROR, Some("permission denied"));

        let failure = switch
            .unset_mtls_services(
                &[SwitchMtlsService::ScaleUpFabricTelemetryInterface],
                &no_cancel(),
            )
            .await
            .unwrap_err();

        for fragment in [
            "scale-up-fabric-telemetry-interface",
            "unset_binding",
            "'mtls' is not one of",
            "permission denied",
        ] {
            assert!(
                failure.error.message.contains(fragment),
                "expected {fragment:?} in {:?}",
                failure.error.message
            );
        }

        assert_recorded_commands(
            &commands,
            &[
                GNMI_MTLS_CERTIFICATE_UNSET_COMMAND,
                GNMI_MTLS_CA_CERTIFICATE_UNSET_COMMAND,
            ],
        );
    }

    /// Graceful shutdown cancels in-flight jobs, and the switch keeps
    /// whatever the run already issued. `nv action restore` takes effect
    /// immediately, so a cancelled run that had already restored a cluster
    /// app must report it as cleared rather than as never attempted.
    #[tokio::test]
    async fn unset_mtls_services_reports_what_ran_when_cancelled_mid_flow() {
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&commands);

        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(
            move |command| {
                recorded.lock().unwrap().push(command.to_owned());
                trigger.cancel();

                Ok(String::new())
            },
        );

        let report = switch
            .unset_mtls_services(
                &[
                    SwitchMtlsService::ScaleUpFabricManager,
                    SwitchMtlsService::NvueApi,
                ],
                &cancel,
            )
            .await
            .unwrap();

        assert!(report.cancelled);
        assert_eq!(
            service_statuses(&report),
            vec![
                ("scale-up-fabric-manager", "cleared"),
                ("nvue-api", "not_attempted"),
            ]
        );

        // Neither persist step ran, which is how a client learns the change
        // reached the switch but was never committed.
        assert_eq!(persist_statuses(&report), PERSIST_NOT_ATTEMPTED);

        // A cancelled run is not a failed one; there is no site to attribute.
        assert_eq!(report.failure, None);

        assert_recorded_commands(
            &commands,
            &["nv action restore cluster apps nmx-controller manager encryption"],
        );
    }

    #[tokio::test]
    async fn unset_mtls_services_cancelled_before_it_starts_issues_nothing() {
        let (switch, commands) = recording_switch(&[]);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let report = switch
            .unset_mtls_services(&[SwitchMtlsService::NvueApi], &cancel)
            .await
            .unwrap();

        assert!(report.cancelled);
        assert_eq!(
            service_statuses(&report),
            vec![("nvue-api", "not_attempted")]
        );
        assert_recorded_commands(&commands, &[]);
    }

    /// A run nobody cancelled must not look cancelled.
    #[tokio::test]
    async fn unset_mtls_services_is_not_marked_cancelled_on_a_clean_run() {
        let (switch, _commands) = recording_switch(&[]);

        let report = switch
            .unset_mtls_services(&[SwitchMtlsService::NvueApi], &no_cancel())
            .await
            .unwrap();

        assert!(!report.cancelled);
    }

    /// A failure mid-run must still say which services had already been
    /// cleared and which never ran: that is the whole point of carrying the
    /// report on the error path.
    #[tokio::test]
    async fn unset_mtls_services_attributes_a_partial_failure_to_its_service_and_stage() {
        let (switch, commands) = recording_switch(&[(
            "nv action restore cluster apps nmx-telemetry manager encryption",
            "permission denied",
        )]);

        let failure = switch
            .unset_mtls_services(
                &[
                    SwitchMtlsService::NvueApi,
                    SwitchMtlsService::ScaleUpFabricTelemetry,
                    SwitchMtlsService::ScaleUpFabricTelemetryInterface,
                ],
                &no_cancel(),
            )
            .await
            .unwrap_err();

        assert_eq!(
            service_statuses(&failure.report),
            vec![
                ("nvue-api", "cleared"),
                ("scale-up-fabric-telemetry", "failed"),
                ("scale-up-fabric-telemetry-interface", "not_attempted"),
            ]
        );
        assert_eq!(persist_statuses(&failure.report), PERSIST_NOT_ATTEMPTED);

        let site = failure.report.failure.expect("attributed failure site");
        assert_eq!(
            site.service,
            Some(SwitchMtlsService::ScaleUpFabricTelemetry)
        );
        assert_eq!(site.stage, MtlsUnsetStage::UnsetBinding);
        assert_eq!(site.message, "permission denied");

        assert_eq!(
            failure.error.message,
            "failed to unset mTLS for scale-up-fabric-telemetry at stage unset_binding: \
             permission denied"
        );

        assert_recorded_commands(
            &commands,
            &[
                "nv unset system api mtls ca-certificate",
                "nv action restore cluster apps nmx-telemetry manager encryption",
            ],
        );
    }

    /// A persist step is not attributable to any single service, so the
    /// failure site names the stage and leaves `service` unset.
    #[tokio::test]
    async fn unset_mtls_services_attributes_a_persist_failure_to_its_stage_only() {
        let (switch, _commands) = recording_switch(&[("nv config save", "disk full")]);

        let failure = switch
            .unset_mtls_services(&[SwitchMtlsService::NvueApi], &no_cancel())
            .await
            .unwrap_err();

        assert_eq!(
            service_statuses(&failure.report),
            vec![("nvue-api", "cleared")]
        );
        assert_eq!(
            persist_statuses(&failure.report),
            [("config_apply", "cleared"), ("config_save", "failed")]
        );

        let site = failure.report.failure.expect("attributed failure site");
        assert_eq!(site.service, None);
        assert_eq!(site.stage, MtlsUnsetStage::ConfigSave);

        assert_eq!(
            failure.error.message,
            "failed to unset mTLS at stage config_save: disk full"
        );
    }

    /// The message a client sees names the service, never the `nv` command
    /// that failed; command text belongs in tracing only.
    #[tokio::test]
    async fn unset_mtls_services_keeps_command_text_out_of_the_error_message() {
        let (switch, _commands) = recording_switch(&[(
            "nv unset system api mtls ca-certificate",
            "permission denied",
        )]);

        let failure = switch
            .unset_mtls_services(&[SwitchMtlsService::NvueApi], &no_cancel())
            .await
            .unwrap_err();

        assert!(!failure.error.message.contains("nv unset"));
        assert!(!failure.error.message.contains("nv config"));
        assert!(failure.error.message.contains("nvue-api"));
        assert!(failure.error.message.contains("permission denied"));
    }

    #[tokio::test]
    async fn unset_mtls_services_does_not_ignore_a_real_cluster_restore_failure() {
        let (switch, _commands) = recording_switch(&[(
            "nv action restore cluster apps nmx-controller manager encryption",
            "permission denied",
        )]);

        let failure = switch
            .unset_mtls_services(&[SwitchMtlsService::ScaleUpFabricManager], &no_cancel())
            .await
            .unwrap_err();

        assert!(failure.error.message.contains("permission denied"));
        assert_eq!(
            service_statuses(&failure.report),
            vec![("scale-up-fabric-manager", "failed")]
        );
    }

    /// The NVUE API unset carries no benign-failure policy, so a
    /// "cluster is not enabled" message must not excuse it.
    #[tokio::test]
    async fn unset_mtls_services_only_tolerates_a_disabled_cluster_on_cluster_apps() {
        let (switch, _commands) = recording_switch(&[(
            "nv unset system api mtls ca-certificate",
            "cluster is not enabled",
        )]);

        let failure = switch
            .unset_mtls_services(&[SwitchMtlsService::NvueApi], &no_cancel())
            .await
            .unwrap_err();

        assert!(failure.error.message.contains("cluster is not enabled"));
        assert_eq!(
            service_statuses(&failure.report),
            vec![("nvue-api", "failed")]
        );
    }

    #[tokio::test]
    async fn unset_mtls_services_requires_at_least_one_service() {
        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1")
            .with_ssh_exec_for_test(|_| Ok(String::new()));

        let failure = switch
            .unset_mtls_services(&[], &no_cancel())
            .await
            .unwrap_err();

        assert_eq!(failure.error.code, ErrorCode::InvalidArgument);
        assert!(failure.report.services.is_empty());
    }

    /// `From<MtlsUnsetError>` is what lets callers that do not want the report
    /// keep using `?`; it must preserve the attributed error verbatim.
    #[tokio::test]
    async fn mtls_unset_error_converts_to_its_inner_rms_error() {
        let (switch, _commands) = recording_switch(&[(
            "nv unset system api mtls ca-certificate",
            "permission denied",
        )]);

        let failure = switch
            .unset_mtls_services(&[SwitchMtlsService::NvueApi], &no_cancel())
            .await
            .unwrap_err();
        let expected = failure.error.message.clone();
        let error = RmsError::from(failure);

        assert_eq!(error.message, expected);
        assert_eq!(error.code, ErrorCode::Internal);
    }

    #[test]
    fn is_nvue_retryable_error_detects_connectivity_failures() {
        use crate::utilities::error::ErrorCode;

        let tls_err = RmsError::internal("POST /nvue_v1/revision: tls handshake failure");

        assert!(SwitchGb200Nvidia::is_nvue_retryable_error(&tls_err));

        let forbidden_err = RmsError::new(
            ErrorCode::Internal,
            "HTTP POST /nvue_v1/revision returned 403",
        );

        assert!(SwitchGb200Nvidia::is_nvue_retryable_error(&forbidden_err));

        let unauthenticated_err = RmsError::new(
            ErrorCode::Unauthenticated,
            "HTTP GET /nvue_v1/system returned 401",
        );

        assert!(SwitchGb200Nvidia::is_nvue_retryable_error(
            &unauthenticated_err
        ));

        let connect_err = RmsError::new(ErrorCode::ConnectionRefused, "connect refused");

        assert!(SwitchGb200Nvidia::is_nvue_retryable_error(&connect_err));

        let logic_err = RmsError::internal("NVUE action job-1 failed with state action_error");

        assert!(!SwitchGb200Nvidia::is_nvue_retryable_error(&logic_err));
    }
}
