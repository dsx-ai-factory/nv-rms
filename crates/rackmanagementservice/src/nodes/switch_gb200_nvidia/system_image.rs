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

//! System-image state and workflows for NVIDIA GB200 switches.

use std::path::Path;
use std::time::Duration;

use super::validation::is_valid_firmware_filename;
use super::{SwitchGb200Nvidia, config, extract_job_id};
use crate::api::grpc::switch_certificate_handlers::SwitchCertificateInstallNode;
use crate::domain::node::{FirmwareTaskStatus, Node};
use crate::domain::rack::EndpointConfig;
use crate::nodes::SwitchSystemImageNode;
use crate::observability::tracing::record_phase_result;
use crate::transport::http_client::HttpClient;
use crate::transport::ssh_client::{SshClient, SshEndpoint, SshUploadOptions};
use crate::utilities::error::{ErrorCode, Result, RmsError};

use async_trait::async_trait;
use macaddr::MacAddr6;
use nvswitch_cli::RemotePath;
use nvue_client::system::{
    SystemImageFetchRequest, SystemImageInstallRequest, SystemImageReboot,
    SystemImageUninstallRequest,
};
use regex::Regex;
use secrecy::ExposeSecret;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub const NVOS_PARTITION_1_ID: &str = "partition1";
pub const NVOS_PARTITION_2_ID: &str = "partition2";

const NVOS_INSTALLATION_PROBE_COMMAND: &str = "command -v nv >/dev/null 2>&1";
const NVOS_INSTALLATION_REST_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const NVOS_VERSION_PROBE_COMMAND: &str = "nv show system version";
const NVOS_VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const ONIE_INSTALLATION_PROBE_COMMAND: &str = "onie-sysinfo -e";
const ONIE_RECOVERY_IMAGE_DIRECTORY: &str = "/tmp";
const ONIE_INSTALL_LOG: &str = "/tmp/rms-onie-nos-install.log";
const ONIE_INSTALL_STATUS: &str = "/tmp/rms-onie-nos-install.status";
const ONIE_INSTALL_LOG_TAIL_BYTES: usize = 1024;
const ONIE_INSTALL_DIAGNOSTIC_MAX_CHARS: usize = 512;
const ONIE_INSTALL_DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(not(test))]
const NVOS_RECOVERY_POLL_INTERVAL: Duration = Duration::from_secs(10);
#[cfg(test)]
const NVOS_RECOVERY_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Result of probing the operating system installed on a switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NvosInstallationState {
    /// NVOS is installed and the normal NVUE workflow may proceed.
    Installed,

    /// NVOS is not installed and the authorized ONIE recovery path is needed.
    NotInstalled,
}

fn parse_management_mac(value: &str) -> Option<MacAddr6> {
    let mac = value.trim().parse::<MacAddr6>().ok()?;
    (!mac.is_nil() && mac.is_unicast()).then_some(mac)
}

// ── System image state & poll heuristics ──────────────────────────────

/// Normalized NVUE system image state.
///
/// NVUE shapes the `/nvue_v1/system/image` response slightly differently across
/// switch software versions. `normalize_system_image_state` reduces that to a
/// stable representation that callers (install-then-wait flow) can compare
/// directly against a target build-id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SwitchSystemImageState {
    pub current: String,
    pub next: String,
    pub current_partition: String,
    pub next_partition: String,
    pub current_build_id: String,
    pub next_build_id: String,
    pub partition1_build_id: String,
    pub partition2_build_id: String,
}

// Parse a "build-id" style value - may be a plain string or an object with a
// string `build-id` field. Returns the empty string when neither shape matches.
fn extract_system_image_build_id(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.to_owned();
    }
    if let Some(obj) = value.as_object()
        && let Some(s) = obj.get("build-id").and_then(|v| v.as_str())
    {
        return s.to_owned();
    }
    String::new()
}

fn normalize_system_image_partition_id(value: &str) -> String {
    match value {
        "1" | NVOS_PARTITION_1_ID => NVOS_PARTITION_1_ID.to_owned(),
        "2" | NVOS_PARTITION_2_ID => NVOS_PARTITION_2_ID.to_owned(),
        _ => String::new(),
    }
}

fn find_system_image_partition_id(state: &SwitchSystemImageState, build_id: &str) -> String {
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

fn resolve_system_image_build_id(
    state: &SwitchSystemImageState,
    value: &str,
    partition_id: &str,
) -> String {
    if partition_id == NVOS_PARTITION_1_ID || value == NVOS_PARTITION_1_ID {
        return state.partition1_build_id.clone();
    }
    if partition_id == NVOS_PARTITION_2_ID || value == NVOS_PARTITION_2_ID {
        return state.partition2_build_id.clone();
    }
    value.to_owned()
}

pub fn normalize_system_image_state(raw: &Value) -> SwitchSystemImageState {
    let mut state = SwitchSystemImageState::default();
    if let Some(p1) = raw.get(NVOS_PARTITION_1_ID) {
        state.partition1_build_id = extract_system_image_build_id(p1);
    }
    if let Some(p2) = raw.get(NVOS_PARTITION_2_ID) {
        state.partition2_build_id = extract_system_image_build_id(p2);
    }
    state.current = raw
        .get("current")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    state.next = raw
        .get("next")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    state.current_partition = normalize_system_image_partition_id(&state.current);
    state.next_partition = normalize_system_image_partition_id(&state.next);
    state.current_build_id =
        resolve_system_image_build_id(&state, &state.current, &state.current_partition);
    state.next_build_id = resolve_system_image_build_id(&state, &state.next, &state.next_partition);
    if state.current_partition.is_empty() {
        state.current_partition = find_system_image_partition_id(&state, &state.current_build_id);
    }
    if state.next_partition.is_empty() {
        state.next_partition = find_system_image_partition_id(&state, &state.next_build_id);
    }
    state
}

/// Decision for automatic unused-partition cleanup during
/// `UpdateSwitchSystemImage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnusedPartitionCleanup {
    /// The opposite partition has no image. Cleanup is a successful no-op.
    NoUnusedImage,
    /// An image is present but it is the current image, boot-next, or the
    /// incoming target. Cleanup skips uninstall.
    SkipProtected {
        partition: &'static str,
        build_id: String,
        reason: &'static str,
    },
    /// Safe to uninstall without `force: true`.
    Uninstall {
        partition: &'static str,
        build_id: String,
    },
}

/// Plan unused-partition cleanup from normalized image state.
///
/// Fails closed when the current image, boot-next, or target build cannot
/// be established. Never selects force uninstall.
pub fn plan_unused_partition_cleanup(
    state: &SwitchSystemImageState,
    target_build_id: &str,
) -> Result<UnusedPartitionCleanup> {
    if target_build_id.is_empty() {
        return Err(RmsError::failed_precondition(
            "system image target build-id is empty; refusing unused-partition cleanup",
        ));
    }
    if state.current_partition.is_empty() || state.current_build_id.is_empty() {
        return Err(RmsError::failed_precondition(
            "current system image partition or build-id is unknown; refusing unused-partition cleanup",
        ));
    }

    let (unused_build_id, unused_partition) = match state.current_partition.as_str() {
        NVOS_PARTITION_2_ID => (state.partition1_build_id.as_str(), NVOS_PARTITION_1_ID),
        NVOS_PARTITION_1_ID => (state.partition2_build_id.as_str(), NVOS_PARTITION_2_ID),
        other => {
            return Err(RmsError::failed_precondition(format!(
                "unrecognized current partition ID: {other}"
            )));
        }
    };

    if unused_build_id.is_empty() {
        Ok(UnusedPartitionCleanup::NoUnusedImage)
    } else if state.next_build_id.is_empty() && state.next_partition.is_empty() {
        Err(RmsError::failed_precondition(format!(
            "unused {unused_partition} image {unused_build_id} is present but boot-next is unknown; refusing unused-partition cleanup"
        )))
    } else if unused_build_id == state.current_build_id {
        Ok(UnusedPartitionCleanup::SkipProtected {
            partition: unused_partition,
            build_id: unused_build_id.to_owned(),
            reason: "current",
        })
    } else if unused_build_id == state.next_build_id || state.next_partition == unused_partition {
        Ok(UnusedPartitionCleanup::SkipProtected {
            partition: unused_partition,
            build_id: unused_build_id.to_owned(),
            reason: "boot-next",
        })
    } else if unused_build_id == target_build_id {
        Ok(UnusedPartitionCleanup::SkipProtected {
            partition: unused_partition,
            build_id: unused_build_id.to_owned(),
            reason: "target-build",
        })
    } else {
        Ok(UnusedPartitionCleanup::Uninstall {
            partition: unused_partition,
            build_id: unused_build_id.to_owned(),
        })
    }
}

/// Walk an NVUE system-image listing and return true if `image_filename`
/// appears anywhere in the structure (as an array entry, an object key,
/// or a nested value).
pub fn system_image_listing_contains(value: &Value, image_filename: &str) -> bool {
    match value {
        Value::String(s) => s == image_filename,
        Value::Array(arr) => arr
            .iter()
            .any(|v| system_image_listing_contains(v, image_filename)),
        Value::Object(obj) => {
            if obj.contains_key(image_filename) {
                return true;
            }
            obj.iter().any(|(k, v)| {
                k == image_filename || system_image_listing_contains(v, image_filename)
            })
        }
        _ => false,
    }
}

/// Infer the NVOS build-id (`nvos-X.Y.Z`) from a system-image filename.
/// The filename stem must contain exactly one `X.Y.Z` pattern.
pub fn infer_target_build_id(image_filename: &str) -> Result<String> {
    let stem = Path::new(image_filename)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    // Numeric semver triple. Bounded by word boundaries so "1.2.3.4" is
    // still matched as "1.2.3" (first match wins when unique).
    let re = Regex::new(r"(\d+\.\d+\.\d+)").expect("regex compiles");

    let mut version: Option<String> = None;
    for caps in re.captures_iter(stem) {
        let candidate = caps
            .get(1)
            .map(|m| m.as_str().to_owned())
            .unwrap_or_default();
        match &version {
            None => version = Some(candidate),
            Some(existing) if existing != &candidate => {
                return Err(RmsError::invalid_argument(format!(
                    "image filename '{image_filename}' contains multiple version patterns, cannot infer target build-id"
                )));
            }
            _ => {}
        }
    }

    match version {
        Some(v) => Ok(format!("nvos-{v}")),
        None => Err(RmsError::invalid_argument(format!(
            "unable to infer NVOS build-id from image filename '{image_filename}'"
        ))),
    }
}

/// True when an HTTP call failed with 401 Unauthorized.
pub fn is_http_unauthorized_error(err: &RmsError) -> bool {
    err.code == ErrorCode::Unauthenticated
}

/// Transport-level NVUE errors that are worth retrying while the switch is
/// temporarily unavailable.
pub fn is_retryable_nvue_transport_error(err: &RmsError) -> bool {
    matches!(
        err.code,
        ErrorCode::ConnectionRefused
            | ErrorCode::Timeout
            | ErrorCode::Unavailable
            | ErrorCode::DnsResolutionFailed
    )
}

pub fn is_retryable_steady_state_error(err: &RmsError) -> bool {
    is_retryable_nvue_transport_error(err)
        || err
            .message
            .contains("HTTP GET /nvue_v1/system/image returned ")
}

pub fn is_retryable_install_poll_error(err: &RmsError) -> bool {
    is_retryable_nvue_transport_error(err) || err.message.contains("HTTP GET /nvue_v1/action/")
}

pub(super) fn contains_case_insensitive(text: &str, needle: &str) -> bool {
    text.to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

pub fn is_uninstall_poll_success(status: &FirmwareTaskStatus) -> bool {
    status.state == "action_success"
}

fn is_install_progress_text(text: &str) -> bool {
    text.to_ascii_lowercase().contains("installing image")
}

fn is_install_reboot_transition_text(text: &str) -> bool {
    contains_case_insensitive(text, "disconnecting from nvos")
        || contains_case_insensitive(text, "offline during reboot")
        || contains_case_insensitive(text, "system is offline")
        || contains_case_insensitive(text, "performing reboot cold")
}

/// An `action_success` state, or the `action_error + percent==100` handoff
/// that NVUE sometimes reports in place of action_success for reboot-driven
/// installs.
pub fn is_install_poll_success(status: &FirmwareTaskStatus) -> bool {
    status.state == "action_success" || (status.state == "action_error" && status.percent == 100)
}

pub fn is_install_progress_state(status: &FirmwareTaskStatus) -> bool {
    is_install_progress_text(&status.status) || is_install_progress_text(&status.message)
}

pub fn is_install_reboot_transition_state(status: &FirmwareTaskStatus) -> bool {
    is_install_reboot_transition_text(&status.status)
        || is_install_reboot_transition_text(&status.message)
}

/// `action_error` with percent==100 and "Installing image" context is NVUE's
/// way of handing off from the install phase to the reboot phase. Once we see
/// this, further polling of the install job is pointless - move to the
/// steady-state wait.
pub fn is_install_handoff_to_reboot(status: &FirmwareTaskStatus) -> bool {
    status.state == "action_error"
        && status.completed
        && status.percent == 100
        && is_install_progress_state(status)
}

pub fn describe_install_poll(status: &FirmwareTaskStatus) -> String {
    let mut out = format!("state={}", status.state);
    if !status.status.is_empty() {
        out.push_str(&format!(", status={}", status.status));
    }
    if !status.message.is_empty() {
        out.push_str(&format!(", message={}", status.message));
    }
    out.push_str(&format!(", completed={}", status.completed));
    out.push_str(&format!(", percent={}", status.percent));
    out
}

#[async_trait]
impl SwitchSystemImageNode for SwitchGb200Nvidia {
    fn require_certificate_install_node(&self) -> Result<&dyn SwitchCertificateInstallNode> {
        Ok(self)
    }

    async fn verify_nvos_installation(
        &self,
        onie_endpoint: Option<&EndpointConfig>,
    ) -> Result<NvosInstallationState> {
        let _op_guard = self.op_lock.lock().await;
        self.verify_nvos_installation(onie_endpoint).await
    }

    async fn stage_nvos_recovery_image(
        &self,
        onie_endpoint: &EndpointConfig,
        local_file_path: &str,
        image_filename: &str,
        upload_options: SshUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        self.stage_nvos_recovery_image(
            onie_endpoint,
            local_file_path,
            image_filename,
            upload_options,
            cancel,
        )
        .await
    }

    async fn install_staged_nvos_image_and_wait(
        &mut self,
        onie_endpoint: &EndpointConfig,
        image_filename: &str,
        recovery_timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<()> {
        self.install_staged_nvos_image_and_wait(
            onie_endpoint,
            image_filename,
            recovery_timeout,
            cancel,
        )
        .await
    }

    async fn save_current_applied_config(&self) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        self.save_current_applied_config().await
    }

    fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
        self.optional_nvue_client()
    }

    fn system_image_requires_bmc_activation(&self) -> bool {
        false
    }

    async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
        let _op_guard = self.op_lock.lock().await;
        self.get_normalized_system_image_state().await
    }

    async fn ensure_nvos_asic_firmware_config(&self) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        SwitchGb200Nvidia::ensure_nvos_asic_firmware_config(self).await
    }

    async fn poll_firmware_task(
        &self,
        task_id: &str,
        staged_install: bool,
    ) -> Result<crate::domain::node::FirmwareTaskStatus> {
        if staged_install {
            let _op_guard = self.op_lock.lock().await;
            self.poll_staged_system_image_task(task_id).await
        } else {
            Node::poll_firmware_task(self, task_id).await
        }
    }

    async fn uninstall_system_image(&self) -> Result<String> {
        let _op_guard = self.op_lock.lock().await;
        self.uninstall_system_image().await
    }

    async fn list_system_images(&self) -> Result<serde_json::Value> {
        let _op_guard = self.op_lock.lock().await;
        self.list_system_images().await
    }

    async fn push_system_image_file(
        &self,
        local_file_path: &str,
        image_filename: &str,
        sftp_upload_options: SshUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;

        self.push_system_image_file_with_options(
            local_file_path,
            image_filename,
            sftp_upload_options,
            cancel,
        )
        .await
    }

    async fn install_system_image(&self, image_filename: &str) -> Result<String> {
        let _op_guard = self.op_lock.lock().await;
        self.install_system_image(image_filename).await
    }

    async fn activate_system_image_via_bmc(&self, cancel: &CancellationToken) -> Result<()> {
        cancel
            .run_until_cancelled(Node::bmc_aux_powercycle(self))
            .await
            .ok_or_else(|| RmsError::cancelled("NVOS BMC activation cancelled"))?
    }

    async fn recover_admin_password_after_boot(&mut self, target_password: &str) -> Result<()> {
        self.recover_admin_password_after_boot(target_password)
            .await
    }

    async fn verify_system_ready(&self) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        self.verify_nvos_ready().await
    }
}

impl SwitchGb200Nvidia {
    fn onie_recovery_image_path(image_filename: &str) -> Result<String> {
        if !is_valid_firmware_filename(image_filename) {
            return Err(RmsError::invalid_argument(
                "system image filename is invalid \
                 (only alphanumeric, '-', '_', '.' allowed, no hidden files)",
            ));
        }

        Ok(format!("{ONIE_RECOVERY_IMAGE_DIRECTORY}/{image_filename}"))
    }

    /// Validate the NVOS admin credentials used after ONIE installs the image.
    /// ONIE authenticates with the separate recovery endpoint credentials.
    fn validate_recovered_nvos_admin_credentials(&self) -> Result<()> {
        let (target_username, target_password) = self.require_host_credentials()?;

        if target_username != "admin" {
            return Err(RmsError::failed_precondition(
                "ONIE recovery requires the configured NVOS username to be admin",
            ));
        }

        if target_password.expose_secret().is_empty() {
            return Err(RmsError::invalid_argument(
                "ONIE recovery requires a non-empty configured NVOS password",
            ));
        }

        Ok(())
    }

    async fn connect_onie(endpoint: &EndpointConfig) -> Result<SshClient> {
        SshClient::connect(
            Self::onie_ssh_endpoint(endpoint)?,
            SshClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    async fn execute_onie_command(
        &self,
        endpoint: &EndpointConfig,
        command: &str,
        cancel: &CancellationToken,
    ) -> Result<String> {
        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            return exec(command);
        }

        let client = Self::connect_onie(endpoint).await?;

        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(RmsError::cancelled(
                "NVOS recovery cancelled while executing an ONIE command",
            )),
            result = client.exec(command, SshClient::DEFAULT_TIMEOUT) => result,
        };

        let _ = client.close().await;

        result
    }

    /// Confirm that SCP left the complete installer on the ONIE filesystem.
    async fn confirm_onie_recovery_image(
        &self,
        endpoint: &EndpointConfig,
        remote_path: &str,
        expected_size: u64,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let command = format!("wc -c < '{remote_path}'");

        let output = self
            .execute_onie_command(endpoint, &command, cancel)
            .await
            .map_err(|error| {
                RmsError::new(
                    error.code,
                    format!(
                        "unable to confirm the staged ONIE installer: {}",
                        error.message
                    ),
                )
            })?;

        let actual_size = output.trim().parse::<u64>().map_err(|_| {
            RmsError::failed_precondition("ONIE returned an invalid staged installer size")
        })?;

        if actual_size != expected_size {
            return Err(RmsError::failed_precondition(format!(
                "staged ONIE installer size mismatch: expected {expected_size} bytes, found {actual_size} bytes"
            )));
        }

        Ok(())
    }

    /// Return a bounded, redacted installer diagnostic when ONIE remains reachable.
    async fn onie_installer_log_tail(
        &self,
        endpoint: &EndpointConfig,
        cancel: &CancellationToken,
    ) -> Option<String> {
        let command = format!("tail -c {ONIE_INSTALL_LOG_TAIL_BYTES} '{ONIE_INSTALL_LOG}'");

        let output = tokio::time::timeout(
            ONIE_INSTALL_DIAGNOSTIC_TIMEOUT,
            self.execute_onie_command(endpoint, &command, cancel),
        )
        .await
        .ok()?
        .ok()?;

        let target_password = self.require_host_credentials().ok()?.1.expose_secret();

        let onie_password = endpoint
            .credentials
            .as_ref()
            .and_then(|credentials| credentials.user_pass_secret())?
            .1
            .expose_secret();

        // Redact both known credentials before applying the shared field
        // redactor. Collapse control characters so errors remain one line.
        let redacted = output.replace(target_password, common::DEFAULT_REPLACEMENT);

        let redacted = if onie_password.is_empty() {
            redacted
        } else {
            redacted.replace(onie_password, common::DEFAULT_REPLACEMENT)
        };

        let redacted = redacted
            .split_whitespace()
            .map(common::redaction::redact_secret_key_value_arg)
            .collect::<Vec<_>>()
            .join(" ");

        let redacted = common::redaction::redact_secret_fields_bounded(
            &redacted,
            ONIE_INSTALL_DIAGNOSTIC_MAX_CHARS,
        );

        let printable: String = redacted
            .chars()
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect();

        let diagnostic = printable.split_whitespace().collect::<Vec<_>>().join(" ");

        (!diagnostic.is_empty()).then_some(diagnostic)
    }

    async fn probe_ssh_command(&self, endpoint: SshEndpoint, command: &str) -> Result<String> {
        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            return exec(command);
        }

        let client = SshClient::connect(endpoint, SshClient::DEFAULT_TIMEOUT).await?;
        let result = client.exec(command, SshClient::DEFAULT_TIMEOUT).await;

        // The probe result is authoritative. Dropping the client still
        // releases the session if the explicit disconnect fails.
        let _ = client.close().await;

        result
    }

    fn onie_ssh_endpoint(endpoint: &EndpointConfig) -> Result<SshEndpoint> {
        if endpoint.endpoint.ip_address.is_empty() {
            return Err(RmsError::invalid_argument(
                "ONIE recovery endpoint is missing an IP address",
            ));
        }

        if endpoint.endpoint.port != 22 {
            return Err(RmsError::invalid_argument(format!(
                "ONIE recovery endpoint must use SSH port 22, got {}",
                endpoint.endpoint.port
            )));
        }

        let (username, password) = endpoint
            .credentials
            .as_ref()
            .and_then(|credentials| credentials.user_pass_secret())
            .ok_or_else(|| {
                RmsError::invalid_argument(
                    "ONIE recovery endpoint requires username/password credentials",
                )
            })?;

        Ok(SshEndpoint::new(
            &endpoint.endpoint.ip_address,
            username,
            password.expose_secret(),
        ))
    }

    /// Determine whether the switch is running NVOS or ONIE.
    ///
    /// A valid NVUE system response identifies NVOS without requiring a ready
    /// or healthy system. Any REST failure, including TLS or authentication
    /// rejection, permits SSH detection only when an ONIE endpoint was supplied
    /// to authorize recovery. SSH first runs `nv show system version` with a
    /// five-second budget. If that query fails, a binary-presence check still
    /// identifies NVOS without requiring `nvued`. Only failure of both NVOS
    /// checks permits probing ONIE with its own credentials.
    ///
    /// # Errors
    ///
    /// Returns the REST probe error when ONIE recovery is disabled. When recovery
    /// is enabled, returns the SSH detection error if neither endpoint verifies
    /// the installed operating system or the ONIE identity checks fail.
    pub async fn verify_nvos_installation(
        &self,
        onie_endpoint: Option<&EndpointConfig>,
    ) -> Result<NvosInstallationState> {
        let rest_probe = async {
            self.nvue_client()?
                .get_system(NVOS_INSTALLATION_REST_PROBE_TIMEOUT)
                .await
                .map_err(RmsError::from)
        }
        .await;

        let Err(error) = rest_probe else {
            return Ok(NvosInstallationState::Installed);
        };

        let Some(onie_endpoint) = onie_endpoint else {
            return Err(error);
        };

        tracing::debug!(
            node = %self.id,
            "NVUE installation probe failed; using authorized SSH detection"
        );

        let version_probe = tokio::time::timeout(NVOS_VERSION_PROBE_TIMEOUT, async {
            self.probe_ssh_command(self.ssh_endpoint()?, NVOS_VERSION_PROBE_COMMAND)
                .await
        })
        .await;

        if matches!(version_probe, Ok(Ok(_))) {
            return Ok(NvosInstallationState::Installed);
        }

        tracing::debug!(
            node = %self.id,
            "NVOS version query failed; checking for the nv command without requiring NVUE"
        );

        self.verify_nvos_installation_via_ssh(Some(onie_endpoint))
            .await
    }

    /// Detect NVOS over SSH, optionally checking ONIE after a failed NVOS probe.
    /// Also used after ONIE installation while NVUE and credentials recover.
    async fn verify_nvos_installation_via_ssh(
        &self,
        onie_endpoint: Option<&EndpointConfig>,
    ) -> Result<NvosInstallationState> {
        // A missing primary configuration and a failed NVOS probe both permit
        // checking the independently configured ONIE endpoint.
        let nvos_error = match self.ssh_endpoint() {
            Ok(endpoint) => match self
                .probe_ssh_command(endpoint, NVOS_INSTALLATION_PROBE_COMMAND)
                .await
            {
                Ok(_) => return Ok(NvosInstallationState::Installed),
                Err(error) => error,
            },
            Err(error) => error,
        };

        let Some(onie_endpoint) = onie_endpoint else {
            return Err(nvos_error);
        };

        let onie_ssh_endpoint = Self::onie_ssh_endpoint(onie_endpoint)?;

        let expected_mac = self
            .host_endpoint
            .as_ref()
            .and_then(|endpoint| parse_management_mac(&endpoint.endpoint.mac_address))
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "ONIE recovery requires a valid, nonzero host endpoint MAC address",
                )
            })?;

        if !onie_endpoint.endpoint.mac_address.is_empty() {
            let supplied_mac = parse_management_mac(&onie_endpoint.endpoint.mac_address)
                .ok_or_else(|| {
                    RmsError::failed_precondition(
                        "ONIE recovery endpoint has an invalid MAC address",
                    )
                })?;

            if supplied_mac != expected_mac {
                return Err(RmsError::failed_precondition(
                    "ONIE recovery endpoint MAC does not match the switch host MAC",
                ));
            }
        }

        match self
            .probe_ssh_command(onie_ssh_endpoint, ONIE_INSTALLATION_PROBE_COMMAND)
            .await
        {
            Ok(observed_mac) => {
                let observed_mac = parse_management_mac(&observed_mac).ok_or_else(|| {
                    RmsError::failed_precondition(
                        "ONIE returned an invalid management Ethernet MAC address",
                    )
                })?;

                if observed_mac != expected_mac {
                    return Err(RmsError::failed_precondition(
                        "ONIE management MAC does not match the switch host MAC; refusing recovery",
                    ));
                }

                tracing::info!(
                    node = %self.id,
                    onie_host = %onie_endpoint.endpoint.ip_address,
                    onie_port = onie_endpoint.endpoint.port,
                    "ONIE identified on switch recovery endpoint"
                );

                Ok(NvosInstallationState::NotInstalled)
            }
            Err(onie_error) => Err(RmsError::new(
                onie_error.code,
                format!(
                    "unable to verify the installed switch operating system; \
                     NVOS probe failed: {}; ONIE probe failed: {}",
                    nvos_error.message, onie_error.message
                ),
            )),
        }
    }

    /// Copy an NVOS installer from the RMS host to the authorized ONIE host.
    ///
    /// The staged path is deterministic (`/tmp/<image_filename>`) so the
    /// install phase can reconnect independently after this SCP session is
    /// closed. The local image must meet the minimum size, and the remote
    /// byte count must match before installation begins.
    pub async fn stage_nvos_recovery_image(
        &self,
        onie_endpoint: &EndpointConfig,
        local_file_path: &str,
        image_filename: &str,
        upload_options: SshUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()> {
        if cancel.is_cancelled() {
            return Err(RmsError::cancelled(
                "NVOS recovery cancelled before ONIE image staging",
            ));
        }

        self.validate_recovered_nvos_admin_credentials()?;

        let remote_path = Self::onie_recovery_image_path(image_filename)?;

        let local_size = tokio::fs::metadata(local_file_path)
            .await
            .map_err(|error| {
                RmsError::invalid_argument(format!(
                    "ONIE recovery image file {local_file_path} not found: {error}"
                ))
            })?
            .len();

        if local_size < config::MIN_FIRMWARE_FILE_SIZE {
            return Err(RmsError::invalid_argument(format!(
                "ONIE recovery image file too small (minimum {} bytes)",
                config::MIN_FIRMWARE_FILE_SIZE
            )));
        }

        #[cfg(test)]
        if let Some(upload) = &self.scp_upload_for_test {
            upload(local_file_path, &remote_path)?;

            return self
                .confirm_onie_recovery_image(onie_endpoint, &remote_path, local_size, cancel)
                .await;
        }

        let client = Self::connect_onie(onie_endpoint).await?;

        let result = client
            .scp_upload_with_options(
                local_file_path,
                &remote_path,
                upload_options,
                cancel.clone(),
            )
            .await;

        // The SCP result is authoritative. Dropping the client still releases
        // the session if the explicit disconnect fails.
        let _ = client.close().await;

        result?;

        self.confirm_onie_recovery_image(onie_endpoint, &remote_path, local_size, cancel)
            .await
    }

    /// Start the staged ONIE installer, replace the factory-default `admin`
    /// password when required, and verify the normal NVOS SSH endpoint.
    /// A reported installer failure ends the wait early with a bounded,
    /// redacted log tail; NVOS SSH remains the success criterion after reboot.
    pub async fn install_staged_nvos_image_and_wait(
        &mut self,
        onie_endpoint: &EndpointConfig,
        image_filename: &str,
        recovery_timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<()> {
        self.validate_recovered_nvos_admin_credentials()?;

        let target_password = self
            .require_host_credentials()?
            .1
            .expose_secret()
            .to_owned();

        let remote_path = Self::onie_recovery_image_path(image_filename)?;

        let install_command = format!(
            "rm -f '{ONIE_INSTALL_LOG}' '{ONIE_INSTALL_STATUS}' || exit 1; \
             (onie-nos-install '{remote_path}' >'{ONIE_INSTALL_LOG}' 2>&1; \
             printf '%s\\n' \"$?\" >'{ONIE_INSTALL_STATUS}') \
             </dev/null >/dev/null 2>&1 &"
        );

        if cancel.is_cancelled() {
            return Err(RmsError::cancelled(
                "NVOS recovery cancelled before ONIE install trigger",
            ));
        }

        self.execute_onie_command(onie_endpoint, &install_command, cancel)
            .await?;

        tracing::info!(node = %self.id, "ONIE installer started");

        let deadline = tokio::time::Instant::now()
            .checked_add(recovery_timeout)
            .ok_or_else(|| RmsError::invalid_argument("ONIE recovery timeout is too large"))?;

        let last_probe_error = loop {
            if cancel.is_cancelled() {
                return Err(RmsError::cancelled(
                    "NVOS recovery cancelled while waiting for NVOS SSH",
                ));
            }

            let probe_error = match self.verify_nvos_installation_via_ssh(None).await {
                Ok(NvosInstallationState::Installed) => {
                    tracing::info!(node = %self.id, "ONIE installation completed and NVOS verified");
                    return Ok(());
                }
                Ok(NvosInstallationState::NotInstalled) => {
                    "NVOS probe did not identify NVOS".to_owned()
                }
                Err(error) if error.code == ErrorCode::Unauthenticated => {
                    // A fresh NVOS install exposes the expired `admin/admin`
                    // account before the configured host credentials work.
                    // Only enter that interactive flow after SSH has rejected
                    // the configured credential; transport failures still mean
                    // the new operating system may be booting.
                    match self
                        .recover_admin_password_after_boot(&target_password)
                        .await
                    {
                        Ok(()) => {
                            "factory-default admin password changed; waiting for configured NVOS verification"
                                .to_owned()
                        }
                        Err(recovery_error)
                            if matches!(
                                recovery_error.code,
                                ErrorCode::ConnectionRefused
                                    | ErrorCode::Timeout
                                    | ErrorCode::Unauthenticated
                                    | ErrorCode::Unavailable
                            ) =>
                        {
                            format!(
                                "configured credentials were rejected and factory-default admin recovery is not ready: {}",
                                recovery_error.message
                            )
                        }
                        Err(recovery_error) => return Err(recovery_error),
                    }
                }
                Err(error) => error.message,
            };

            let status_command =
                format!("if [ -f '{ONIE_INSTALL_STATUS}' ]; then cat '{ONIE_INSTALL_STATUS}'; fi");

            let status_result = tokio::time::timeout(
                ONIE_INSTALL_DIAGNOSTIC_TIMEOUT,
                self.execute_onie_command(onie_endpoint, &status_command, cancel),
            )
            .await;

            match status_result {
                Ok(Ok(status)) if !status.trim().is_empty() => {
                    let status = status.trim().parse::<u8>().map_err(|_| {
                        RmsError::failed_precondition(
                            "ONIE installer returned an invalid exit status",
                        )
                    })?;

                    if status != 0 {
                        let log_tail = self.onie_installer_log_tail(onie_endpoint, cancel).await;

                        let detail = log_tail
                            .map(|tail| format!("; installer log tail: {tail}"))
                            .unwrap_or_default();

                        tracing::error!(node = %self.id, status, "ONIE installer failed");

                        return Err(RmsError::internal(format!(
                            "ONIE installer exited with status {status}{detail}"
                        )));
                    }
                }
                Ok(Err(error)) if error.code == ErrorCode::Cancelled => return Err(error),
                _ => {} // ONIE may be rebooting; the NVOS probe remains authoritative.
            }

            if tokio::time::Instant::now() >= deadline {
                break probe_error;
            }

            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err(RmsError::cancelled(
                        "NVOS recovery cancelled while waiting for NVOS SSH",
                    ));
                }
                _ = tokio::time::sleep(NVOS_RECOVERY_POLL_INTERVAL) => {}
            }
        };

        let log_tail = self.onie_installer_log_tail(onie_endpoint, cancel).await;

        let detail = log_tail
            .map(|tail| format!("; installer log tail: {tail}"))
            .unwrap_or_default();

        tracing::warn!(node = %self.id, "ONIE installation timed out before NVOS verification");

        Err(RmsError::timeout(format!(
            "timed out waiting for NVOS after ONIE recovery: {last_probe_error}{detail}"
        )))
    }

    pub async fn fetch_system_image(&self, remote_url: &str) -> Result<String> {
        let payload =
            serde_json::to_value(SystemImageFetchRequest::new(remote_url)).map_err(|error| {
                RmsError::internal(format!(
                    "failed to serialize system image fetch request: {error}"
                ))
            })?;

        let resp = self
            .nvue_http_post(
                "/nvue_v1/system/image",
                &payload,
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;
        extract_job_id(&resp)
    }

    pub async fn install_system_image(&self, image_filename: &str) -> Result<String> {
        self.install_system_image_with_reboot(image_filename, None)
            .await
    }

    pub(in crate::nodes) async fn install_system_image_with_reboot(
        &self,
        image_filename: &str,
        reboot: Option<SystemImageReboot>,
    ) -> Result<String> {
        if !is_valid_firmware_filename(image_filename) {
            return Err(RmsError::invalid_argument(
                "invalid image filename \
                 (only alphanumeric, '-', '_', '.' allowed, no hidden files)",
            ));
        }

        let mut request = SystemImageInstallRequest::with_image_file(true, image_filename);
        request.install.parameters.get_or_insert_default().reboot = reboot;
        let payload = serde_json::to_value(request).map_err(|error| {
            RmsError::internal(format!(
                "failed to serialize system image install request: {error}"
            ))
        })?;

        let endpoint = format!("/nvue_v1/system/image/files/{image_filename}");
        let resp = self
            .nvue_http_post(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        extract_job_id(&resp)
    }

    pub async fn list_system_images(&self) -> Result<Value> {
        self.nvue_http_get("/nvue_v1/system/image/files", HttpClient::DEFAULT_TIMEOUT)
            .await
    }

    /// Poll a no-reboot install without the legacy reboot/no-op success heuristics.
    pub(crate) async fn poll_staged_system_image_task(
        &self,
        task_id: &str,
    ) -> Result<FirmwareTaskStatus> {
        let endpoint = format!("/nvue_v1/action/{task_id}");
        let response = self
            .nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        let mut action: nvue_client::action::ActionStatus = serde_json::from_value(response)
            .map_err(|error| {
                RmsError::internal(format!("invalid NVOS install task response: {error}"))
            })?;
        if action.state.is_none() {
            action.state = action.status.clone();
        }
        let status = action.poll_status();
        Ok(FirmwareTaskStatus {
            state: status.state,
            completed: status.completed,
            percent: status.percent,
            status: status.status,
            message: status.message,
            rejection: None,
        })
    }

    /// Uninstalls an unused system image without `force`. Returns the NVOS
    /// action job ID. Callers must have already established that the image
    /// is not current, boot-next, or the incoming target.
    pub async fn uninstall_system_image(&self) -> Result<String> {
        // NVUE infers the unused image from current system image state.
        let payload =
            serde_json::to_value(SystemImageUninstallRequest::new()).map_err(|error| {
                RmsError::internal(format!(
                    "failed to serialize system image uninstall request: {error}"
                ))
            })?;

        let resp = self
            .nvue_http_post(
                "/nvue_v1/system/image",
                &payload,
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;
        extract_job_id(&resp)
    }

    /// Raw `/nvue_v1/system/image` view — current/next boot selection and the
    /// images present in each partition, in whatever shape the switch reports.
    pub async fn get_system_image_state(&self) -> Result<Value> {
        self.nvue_http_get("/nvue_v1/system/image", HttpClient::DEFAULT_TIMEOUT)
            .await
    }

    /// `get_system_image_state()` normalized to the stable
    /// `SwitchSystemImageState` representation. See `normalize_system_image_state`.
    pub async fn get_normalized_system_image_state(&self) -> Result<SwitchSystemImageState> {
        let raw = self.get_system_image_state().await?;
        Ok(normalize_system_image_state(&raw))
    }

    /// Resolve which partition currently contains `build_id`. Returns
    /// `Ok(None)` if neither partition has a matching build-id.
    pub async fn find_system_image_partition(&self, build_id: &str) -> Result<Option<String>> {
        if build_id.is_empty() {
            return Err(RmsError::invalid_argument(
                "system image build-id must not be empty",
            ));
        }
        let state = self.get_normalized_system_image_state().await?;
        let partition_id = find_system_image_partition_id(&state, build_id);
        if partition_id.is_empty() {
            Ok(None)
        } else {
            Ok(Some(partition_id))
        }
    }

    /// SFTP a local image file to `/host/nos-images/<image_filename>`,
    /// skipping the transfer when the same file already exists remotely with
    /// a matching size. Verifies the uploaded file is present afterward.
    pub async fn push_system_image_file(
        &self,
        local_path: &str,
        image_filename: &str,
        cancel: &CancellationToken,
    ) -> Result<()> {
        self.push_system_image_file_with_options(
            local_path,
            image_filename,
            SshUploadOptions::default(),
            cancel,
        )
        .await
    }

    pub async fn is_already_staged(
        &self,
        ssh: &SshClient,
        local_path: &str,
        remote_path: &str,
    ) -> Result<bool> {
        let local_meta = std::fs::metadata(local_path).map_err(|e| {
            RmsError::invalid_argument(format!("system image file {local_path} not found: {e}"))
        })?;
        // The staging check is an optimization to skip re-uploading an
        // identical file. A NotFound probe means the remote file does not exist
        // yet (first-time upload), so treat it as "not staged" and fall through
        // to the upload rather than surfacing the error. `sftp_file_size` also
        // returns 0 when the server omits the size, which flows through as a
        // size mismatch and re-upload. Other probe errors (e.g. connectivity)
        // propagate so real problems are not masked.
        match ssh.sftp_file_size(remote_path).await {
            Ok(remote_size) => Ok(local_meta.len() == remote_size),
            Err(e) if e.code == ErrorCode::NotFound => {
                tracing::debug!(
                    node = %self.id,
                    remote_path,
                    error = %e,
                    "remote file not found during staging check; treating as not staged"
                );
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// SFTP a local image file with explicit upload timeout tunables.
    ///
    /// Emits node-scoped staging logs here while lower-level SFTP progress logs
    /// rely on the caller's tracing span for job correlation.
    pub async fn push_system_image_file_with_options(
        &self,
        local_path: &str,
        image_filename: &str,
        sftp_upload_options: SshUploadOptions,
        cancel: &CancellationToken,
    ) -> Result<()> {
        if !is_valid_firmware_filename(image_filename) {
            return Err(RmsError::invalid_argument(
                "system image filename is invalid \
                 (only alphanumeric, '-', '_', '.' allowed, no hidden files)",
            ));
        }
        let local_meta = std::fs::metadata(local_path).map_err(|e| {
            RmsError::invalid_argument(format!("system image file {local_path} not found: {e}"))
        })?;
        if local_meta.len() < config::MIN_FIRMWARE_FILE_SIZE {
            return Err(RmsError::invalid_argument(format!(
                "system image file too small (minimum {} bytes)",
                config::MIN_FIRMWARE_FILE_SIZE
            )));
        }

        let remote_path = format!("/host/nos-images/{image_filename}");
        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        if self
            .is_already_staged(&ssh, local_path, &remote_path)
            .await?
        {
            tracing::info!(
                 node = %self.id,
                   remote_path,
                   "system image already staged on switch, skipping SFTP upload"
            );
            return Ok(());
        }

        tracing::info!(
            node = %self.id,
            remote_path,
            upload_timeout_secs = sftp_upload_options.overall_timeout.as_secs(),
            step_timeout_secs = sftp_upload_options.step_timeout.as_secs(),
            buffer_size_bytes = config::SFTP_BUFFER_SIZE,
            "uploading system image via SFTP"
        );

        system_image_sftp_upload(&ssh, local_path, &remote_path, sftp_upload_options, cancel)
            .await?;

        self.nv_cli_over(ssh)?
            .confirm_file(&RemotePath::new(remote_path)?, "SFTP upload")
            .await
    }
}

async fn system_image_sftp_upload(
    ssh: &SshClient,
    local_path: &str,
    remote_path: &str,
    options: SshUploadOptions,
    cancel: &CancellationToken,
) -> Result<()> {
    use tracing::Instrument;

    let span = tracing::info_span!("switch_system_image_sftp_upload");
    let result = ssh
        .sftp_upload_with_options(local_path, remote_path, options, cancel.clone())
        .instrument(span.clone())
        .await;
    record_phase_result(&span, &result, false);
    result
}

#[cfg(test)]
mod phase_tests {
    use super::*;
    use opentelemetry::trace::{Status, TracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider};
    use tracing::Instrument;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    #[tokio::test]
    async fn sftp_phase_records_transport_failure_before_closing() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("system-image-upload-test"))
                .with_filter(tracing_subscriber::filter::filter_fn(
                    crate::observability::tracing::should_export,
                )),
        );
        let result = async {
            let ssh = SshClient::disconnected_for_test("private-upload-host");
            system_image_sftp_upload(
                &ssh,
                "/private-upload-path",
                "/private-remote-path",
                SshUploadOptions::default(),
                &CancellationToken::new(),
            )
            .instrument(tracing::info_span!(parent: None, "request"))
            .await
        }
        .with_subscriber(subscriber)
        .await;
        assert_eq!(result.unwrap_err().code, ErrorCode::FailedPrecondition);
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 2);
        let upload = spans
            .iter()
            .find(|span| span.name == "switch_system_image_sftp_upload")
            .unwrap();
        assert_eq!(upload.status, Status::error(""));
        let category = upload
            .attributes
            .iter()
            .find(|attr| attr.key.as_str() == "error.type")
            .unwrap();
        assert_eq!(
            category.value.as_str(),
            ErrorCode::FailedPrecondition.to_string()
        );
        let parent = spans.iter().find(|span| span.name == "request").unwrap();
        assert_eq!(parent.status, Status::Unset);
        assert_eq!(upload.parent_span_id, parent.span_context.span_id());
        assert_eq!(
            upload.span_context.trace_id(),
            parent.span_context.trace_id()
        );
        assert_ne!(upload.span_context.span_id(), parent.span_context.span_id());
        assert!(!format!("{spans:?}").contains("private-upload"));
        assert!(!format!("{spans:?}").contains("private-remote"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;

    fn dual_image_state() -> SwitchSystemImageState {
        SwitchSystemImageState {
            current: NVOS_PARTITION_1_ID.to_owned(),
            next: NVOS_PARTITION_1_ID.to_owned(),
            current_partition: NVOS_PARTITION_1_ID.to_owned(),
            next_partition: NVOS_PARTITION_1_ID.to_owned(),
            current_build_id: "nvos-1.0.0".to_owned(),
            next_build_id: "nvos-1.0.0".to_owned(),
            partition1_build_id: "nvos-1.0.0".to_owned(),
            partition2_build_id: "nvos-0.9.0".to_owned(),
        }
    }

    #[test]
    fn plan_skips_when_unused_partition_is_empty() {
        let mut state = dual_image_state();
        state.partition2_build_id.clear();
        assert_eq!(
            plan_unused_partition_cleanup(&state, "nvos-2.0.0").unwrap(),
            UnusedPartitionCleanup::NoUnusedImage
        );
    }

    #[test]
    fn plan_uninstalls_unrelated_unused_image() {
        assert_eq!(
            plan_unused_partition_cleanup(&dual_image_state(), "nvos-2.0.0").unwrap(),
            UnusedPartitionCleanup::Uninstall {
                partition: NVOS_PARTITION_2_ID,
                build_id: "nvos-0.9.0".to_owned(),
            }
        );
    }

    #[test]
    fn plan_preserves_current_image_on_unused_partition() {
        let mut state = dual_image_state();
        state.partition2_build_id = state.current_build_id.clone();
        let plan = plan_unused_partition_cleanup(&state, "nvos-2.0.0").unwrap();
        match plan {
            UnusedPartitionCleanup::SkipProtected {
                reason, build_id, ..
            } => {
                assert_eq!(reason, "current");
                assert_eq!(build_id, "nvos-1.0.0");
            }
            other => panic!("expected SkipProtected, got {other:?}"),
        }
    }

    #[test]
    fn plan_preserves_boot_next_image() {
        let mut state = dual_image_state();
        state.next = NVOS_PARTITION_2_ID.to_owned();
        state.next_partition = NVOS_PARTITION_2_ID.to_owned();
        state.next_build_id = "nvos-0.9.0".to_owned();
        let plan = plan_unused_partition_cleanup(&state, "nvos-2.0.0").unwrap();
        match plan {
            UnusedPartitionCleanup::SkipProtected { reason, .. } => {
                assert_eq!(reason, "boot-next");
            }
            other => panic!("expected SkipProtected, got {other:?}"),
        }
    }

    #[test]
    fn plan_preserves_incoming_target_build() {
        let plan = plan_unused_partition_cleanup(&dual_image_state(), "nvos-0.9.0").unwrap();
        match plan {
            UnusedPartitionCleanup::SkipProtected {
                reason, build_id, ..
            } => {
                assert_eq!(reason, "target-build");
                assert_eq!(build_id, "nvos-0.9.0");
            }
            other => panic!("expected SkipProtected, got {other:?}"),
        }
    }

    #[test]
    fn plan_fails_closed_when_target_build_id_is_empty() {
        let err = plan_unused_partition_cleanup(&dual_image_state(), "").unwrap_err();
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("target build-id is empty"));
    }

    #[test]
    fn plan_fails_closed_when_current_image_is_unknown() {
        let mut state = dual_image_state();
        state.current_partition.clear();
        let err = plan_unused_partition_cleanup(&state, "nvos-2.0.0").unwrap_err();
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("current system image"));

        let mut state = dual_image_state();
        state.current_build_id.clear();
        let err = plan_unused_partition_cleanup(&state, "nvos-2.0.0").unwrap_err();
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
    }

    #[test]
    fn plan_fails_closed_when_unused_image_present_but_boot_next_unknown() {
        let mut state = dual_image_state();
        state.next.clear();
        state.next_partition.clear();
        state.next_build_id.clear();
        let err = plan_unused_partition_cleanup(&state, "nvos-2.0.0").unwrap_err();
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("boot-next is unknown"));
    }
}
