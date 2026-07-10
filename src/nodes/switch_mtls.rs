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

//! NVOS mTLS certificate installation for switch services.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::libnmxc::NmxcTlsConfig;
use crate::transport::ssh_client::SshClient;
use crate::utilities::error::{Result, RmsError};
use librms::protos::rack_manager as rm;

use super::switch_gb200_nvidia::{SwitchGb200Nvidia, config, is_valid_identifier, shell_quote};

const REMOTE_TLS_ROOT: &str = "/home/admin/certs";
const REMOTE_RUN_ID_FORMAT: &str = "%Y-%m-%d_%H-%M-%S_%3f";
const NVUE_CA_CERTIFICATE_COLLECTION: &str = "/nvue_v1/system/security/ca-certificate";
const NVUE_ENTITY_CERTIFICATE_COLLECTION: &str = "/nvue_v1/system/security/certificate";
const NVUE_SYSTEM_API_PATH: &str = "/nvue_v1/system/api";
const NVUE_SYSTEM_API_MTLS_PATH: &str = "/nvue_v1/system/api/mtls";
const NVUE_GNMI_SERVER_PATH: &str = "/nvue_v1/system/gnmi-server";
const NVUE_GNMI_SERVER_MTLS_PATH: &str = "/nvue_v1/system/gnmi-server/mtls";
const MATERIAL_STABILITY_ATTEMPTS: usize = 3;
const MTLS_UNSET_COMMAND_SETTLE_DELAY: Duration = Duration::from_millis(300);
const CLUSTER_NOT_ENABLED_ERROR: &str = "cluster is not enabled";

/// Staging directory mode: owner rwx, group/other traverse-only (no list).
/// NVUE `file://` import reads staged PEMs via `nvued`, which may not run as
/// `admin`; mode 0700 blocks traversal for other users even when files are 0644.
pub const SWITCH_CERTIFICATE_REMOTE_DIR_PERMISSIONS: u32 = 0o751;
pub const SWITCH_CERTIFICATE_PUBLIC_FILE_PERMISSIONS: u32 = 0o644;
pub const SWITCH_CERTIFICATE_PRIVATE_KEY_PERMISSIONS: u32 = 0o400;

/// Remote TLS file copied to the switch before NVUE import.
pub(crate) struct MtlsRemoteFileSpec<'a> {
    pub remote_name: &'static str,
    pub local_path: &'a std::path::Path,
    pub permissions: u32,
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

fn shell_chmod_mode(mode: u32) -> String {
    format!("{mode:o}")
}

fn remote_install_dir_command(path: &str) -> String {
    format!(
        "install -d -m {} {}",
        shell_chmod_mode(SWITCH_CERTIFICATE_REMOTE_DIR_PERMISSIONS),
        shell_quote(path),
    )
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

fn nvue_cluster_app_manager_sub_endpoint(app_name: &str, field: &str) -> String {
    format!("{}/{}", nvue_cluster_app_manager_endpoint(app_name), field)
}

fn nvue_import_ca_certificate_parameters(uri: &str) -> Value {
    json!({ "uri": uri })
}

fn nvue_import_entity_certificate_parameters(cert_uri: &str, key_uri: &str) -> Value {
    json!({
        "uri-public-key": cert_uri,
        "uri-private-key": key_uri,
    })
}

fn nvue_cluster_app_manager_certificate_parameters(cert_id: &str) -> Value {
    json!({ "cert-id": cert_id })
}

fn nvue_cluster_app_manager_ca_certificate_parameters(ca_id: &str) -> Value {
    json!({ "cacert-id": ca_id })
}

fn nvue_cluster_app_manager_encryption_parameters() -> Value {
    json!({ "encryption": "mtls" })
}

fn mtls_unset_command(service: SwitchMtlsService) -> &'static str {
    match service {
        SwitchMtlsService::NvueApi => "nv unset system api mtls ca-certificate",
        SwitchMtlsService::ScaleUpFabricTelemetryInterface => {
            "nv unset system gnmi-server mtls certificate"
        }
        SwitchMtlsService::ScaleUpFabricTelemetry => {
            "nv action restore cluster apps nmx-telemetry manager encryption"
        }
        SwitchMtlsService::ScaleUpFabricManager => {
            "nv action restore cluster apps nmx-controller manager encryption"
        }
    }
}

fn mtls_unset_commands(services: &[SwitchMtlsService]) -> Vec<String> {
    services
        .iter()
        .map(|service| mtls_unset_command(*service).to_owned())
        .chain([
            "nv config apply --assume-yes".to_owned(),
            "nv config save".to_owned(),
        ])
        .collect()
}

fn is_cluster_app_manager_encryption_restore(command: &str) -> bool {
    command.starts_with("nv action restore cluster apps ")
        && command.ends_with(" manager encryption")
}

fn should_ignore_mtls_unset_error(command: &str, error: &RmsError) -> bool {
    // Restoring cluster app manager encryption is unnecessary when the cluster
    // is already disabled. NVOS reports that as an action failure, but the
    // desired insecure-mode end state is already reached.
    is_cluster_app_manager_encryption_restore(command)
        && error.message.contains(CLUSTER_NOT_ENABLED_ERROR)
}

fn extract_string_field<'a>(response: &'a Value, field: &str) -> Option<&'a str> {
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

fn verify_expected_field(
    response: &Value,
    field: &str,
    expected: &str,
    context: &str,
) -> Result<()> {
    let actual = extract_string_field(response, field).ok_or_else(|| {
        RmsError::failed_precondition(format!("{context} response missing {field}"))
    })?;
    if actual != expected {
        return Err(RmsError::failed_precondition(format!(
            "{context} expected {field}={expected}, got {actual}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SwitchMtlsService {
    NvueApi,
    ScaleUpFabricTelemetry,
    ScaleUpFabricManager,
    ScaleUpFabricTelemetryInterface,
}

impl SwitchMtlsService {
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
            Self::ScaleUpFabricManager => Some("nmx-controller"),
            Self::ScaleUpFabricTelemetry => Some("nmx-telemetry"),
            Self::NvueApi | Self::ScaleUpFabricTelemetryInterface => None,
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
}

impl SwitchGb200Nvidia {
    pub(crate) async fn unset_mtls_services(
        &self,
        services: &[SwitchMtlsService],
    ) -> Result<Vec<String>> {
        if services.is_empty() {
            return Err(RmsError::invalid_argument(
                "services must contain at least one SwitchService",
            ));
        }

        let commands = mtls_unset_commands(services);

        tracing::info!(
            node = %self.id(),
            ?services,
            command_count = commands.len(),
            "unsetting switch service mTLS mode via SSH"
        );

        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            for command in &commands {
                if let Err(error) = exec(command) {
                    if should_ignore_mtls_unset_error(command, &error) {
                        tracing::info!(
                            node = %self.id(),
                            command,
                            error = %error.message,
                            "ignoring mTLS unset error because cluster is already disabled"
                        );

                        continue;
                    }

                    return Err(error);
                }
            }

            return Ok(commands);
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        for (index, command) in commands.iter().enumerate() {
            if let Err(error) = ssh.exec(command, SshClient::DEFAULT_TIMEOUT).await {
                if should_ignore_mtls_unset_error(command, &error) {
                    tracing::info!(
                        node = %self.id(),
                        command,
                        error = %error.message,
                        "ignoring mTLS unset error because cluster is already disabled"
                    );
                } else {
                    return Err(error);
                }
            }

            if index + 1 < commands.len() {
                // NVUE applies and persists config asynchronously under the
                // hood; keep SSH command sequencing conservative without adding
                // material delay to the overall job.
                tokio::time::sleep(MTLS_UNSET_COMMAND_SETTLE_DELAY).await;
            }
        }

        Ok(commands)
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

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;
        tracing::debug!(
            node = %self.id(),
            remote_dir = %remote_dir,
            "SSH session established for remote TLS directory preparation"
        );
        ssh.exec(
            &remote_install_dir_command(REMOTE_TLS_ROOT),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await?;
        tracing::debug!(
            node = %self.id(),
            path = REMOTE_TLS_ROOT,
            "created remote TLS root directory"
        );
        ssh.exec(
            &remote_install_dir_command(&remote_dir),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await?;
        tracing::debug!(
            node = %self.id(),
            path = %remote_dir,
            "created remote TLS run directory"
        );

        tracing::info!(
            node = %self.id(),
            remote_dir = %remote_dir,
            "remote TLS directories prepared on switch"
        );
        Ok(())
    }

    pub(crate) async fn sftp_copy_mtls_file(
        &self,
        material: &SwitchMtlsMaterialPaths,
        remote_file_name: &str,
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
        tracing::debug!(
            node = %self.id(),
            remote_file = spec.remote_name,
            local_path = %spec.local_path.display(),
            remote_path = %remote_path,
            permissions = spec.permissions,
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
        )
        .await?;
        tracing::debug!(
            node = %self.id(),
            remote_path = %remote_path,
            bytes = local_meta.len(),
            "SFTP upload completed; verifying remote file"
        );

        let verify_cmd = format!("test -f {} && echo ok", shell_quote(&remote_path));
        let verify_out = ssh.exec(&verify_cmd, SshClient::DEFAULT_TIMEOUT).await?;
        if !verify_out.contains("ok") {
            return Err(RmsError::internal(format!(
                "SFTP upload verification failed for {remote_path}"
            )));
        }
        tracing::debug!(
            node = %self.id(),
            remote_path = %remote_path,
            verify_output = %verify_out.trim(),
            "remote TLS file presence verified after SFTP copy"
        );

        let chmod_cmd = format!(
            "chmod {} {}",
            shell_chmod_mode(spec.permissions),
            shell_quote(&remote_path),
        );
        ssh.exec(&chmod_cmd, SshClient::DEFAULT_TIMEOUT).await?;
        tracing::debug!(
            node = %self.id(),
            remote_path = %remote_path,
            mode = %shell_chmod_mode(spec.permissions),
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
            nvue_import_ca_certificate_parameters(&ca_uri),
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
            nvue_import_entity_certificate_parameters(&cert_uri, &key_uri),
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
                tracing::debug!(node = %self.id(), service = "gnmi-server", "binding gNMI server mTLS");
                self.nvue_apply_config_patches(&[
                    (NVUE_GNMI_SERVER_PATH, json!({ "certificate": cert_id })),
                    (
                        NVUE_GNMI_SERVER_MTLS_PATH,
                        json!({ "ca-certificate": ca_id }),
                    ),
                ])
                .await?;
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
        self.nvue_stage_config_patches(&[
            (
                NVUE_SYSTEM_API_PATH,
                json!({ "certificate": material.entity_cert_id() }),
            ),
            (
                NVUE_SYSTEM_API_MTLS_PATH,
                json!({ "ca-certificate": material.ca_cert_id() }),
            ),
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
            if let Some(installed_service) = service.installed_certificate_service_name() {
                if !installed_services
                    .iter()
                    .any(|actual| actual == installed_service)
                {
                    return Err(RmsError::failed_precondition(format!(
                        "switch certificate {cert_id} is not installed for {installed_service}"
                    )));
                }
            }
        }

        let nvue_mtls = if services.contains(&SwitchMtlsService::NvueApi) {
            let configuration = self.get_nvue_api_mtls_configuration().await?;

            verify_expected_field(&configuration, "ca-certificate", &ca_id, "NVUE API mTLS")?;

            Some(configuration)
        } else {
            None
        };

        let gnmi_mtls = if services.contains(&SwitchMtlsService::ScaleUpFabricTelemetryInterface) {
            let configuration = self.get_gnmi_server_mtls_configuration().await?;

            verify_expected_field(&configuration, "ca-certificate", &ca_id, "gNMI server mTLS")?;

            Some(configuration)
        } else {
            None
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
            "gnmi_mtls": gnmi_mtls,
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
            &certificate,
            "certificate",
            cert_id,
            &format!("cluster app {app_name} manager certificate"),
        )?;

        let ca_certificate = self
            .get_cluster_app_manager_leaf(app_name, "ca-certificate")
            .await?;
        verify_expected_field(
            &ca_certificate,
            "ca-certificate",
            ca_id,
            &format!("cluster app {app_name} manager CA certificate"),
        )?;

        let encryption = self
            .get_cluster_app_manager_leaf(app_name, "encryption")
            .await?;
        verify_expected_field(
            &encryption,
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

        let cert_bindings = [
            (
                "certificate",
                nvue_cluster_app_manager_certificate_parameters(cert_id),
            ),
            (
                "ca-certificate",
                nvue_cluster_app_manager_ca_certificate_parameters(ca_id),
            ),
        ];
        let settle_delay = Duration::from_secs(config::CLUSTER_MANAGER_ACTION_SETTLE_SECONDS);

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
            tokio::time::sleep(settle_delay).await;
        }

        let encryption_field = "encryption";
        let encryption_endpoint = nvue_cluster_app_manager_sub_endpoint(app_name, encryption_field);
        tracing::debug!(
            node = %self.id(),
            app_name,
            field = encryption_field,
            endpoint = %encryption_endpoint,
            "binding cluster app manager encryption via NVUE API"
        );
        match self
            .nvue_start_action(
                &encryption_endpoint,
                "@update",
                nvue_cluster_app_manager_encryption_parameters(),
            )
            .await
        {
            Ok(()) => {}
            Err(e) => {
                tracing::warn!(
                    node = %self.id(),
                    app_name,
                    error = %e.message,
                    "NVUE REST encryption update failed; retrying via NVOS CLI"
                );
                self.run_cluster_app_manager_field_action(app_name, encryption_field, "mtls")
                    .await?;
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sftp_copy_stage_name_sanitizes_dots() {
        assert_eq!(sftp_copy_stage_name("client.pem"), "sftp_copy_client_pem");
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
    fn remote_install_dir_command_creates_with_restricted_permissions() {
        let cmd = remote_install_dir_command(REMOTE_TLS_ROOT);
        assert!(cmd.contains("install -d -m 751"));
        assert!(cmd.contains(REMOTE_TLS_ROOT));
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
            NVUE_GNMI_SERVER_MTLS_PATH,
            "/nvue_v1/system/gnmi-server/mtls"
        );

        assert_eq!(
            nvue_cluster_app_manager_sub_endpoint("nmx-controller", "encryption"),
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption"
        );
    }

    #[test]
    fn nvue_import_parameters_match_certificate_management_api() {
        let ca = nvue_import_ca_certificate_parameters("file:///home/admin/certs/ca.pem");
        assert_eq!(ca["uri"], "file:///home/admin/certs/ca.pem");

        let entity = nvue_import_entity_certificate_parameters(
            "file:///home/admin/certs/client.pem",
            "file:///home/admin/certs/client.key",
        );
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
        let cert = nvue_cluster_app_manager_certificate_parameters("rms-example-cert");
        assert_eq!(cert["cert-id"], "rms-example-cert");

        let ca = nvue_cluster_app_manager_ca_certificate_parameters("rms-example-ca");
        assert_eq!(ca["cacert-id"], "rms-example-ca");

        let encryption = nvue_cluster_app_manager_encryption_parameters();
        assert_eq!(encryption["encryption"], "mtls");
    }

    #[test]
    fn mtls_unset_commands_remove_service_mtls_and_save_config() {
        let commands = mtls_unset_commands(&[
            SwitchMtlsService::NvueApi,
            SwitchMtlsService::ScaleUpFabricManager,
            SwitchMtlsService::ScaleUpFabricTelemetryInterface,
        ]);

        assert_eq!(
            commands,
            vec![
                "nv unset system api mtls ca-certificate",
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv unset system gnmi-server mtls certificate",
                "nv config apply --assume-yes",
                "nv config save",
            ]
        );
    }

    #[tokio::test]
    async fn unset_mtls_services_ignores_disabled_cluster_restore_error() {
        let switch =
            SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(|command| {
                if is_cluster_app_manager_encryption_restore(command) {
                    return Err(RmsError::internal(
                        "Action failed with the following issue: cluster is not enabled",
                    ));
                }

                Ok(String::new())
            });

        let commands = switch
            .unset_mtls_services(&[SwitchMtlsService::ScaleUpFabricManager])
            .await
            .unwrap();

        assert_eq!(
            commands,
            vec![
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv config apply --assume-yes",
                "nv config save",
            ]
        );
    }

    #[test]
    fn should_ignore_mtls_unset_error_only_for_cluster_disabled_restore() {
        let benign = RmsError::internal("cluster is not enabled");
        let fatal = RmsError::internal("permission denied");

        assert!(should_ignore_mtls_unset_error(
            "nv action restore cluster apps nmx-controller manager encryption",
            &benign
        ));

        assert!(!should_ignore_mtls_unset_error(
            "nv action restore cluster apps nmx-controller manager encryption",
            &fatal
        ));

        assert!(!should_ignore_mtls_unset_error(
            "nv unset system api mtls ca-certificate",
            &benign
        ));
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
