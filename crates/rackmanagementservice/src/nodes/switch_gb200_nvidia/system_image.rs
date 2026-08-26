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

use super::validation::is_valid_firmware_filename;
use super::{SwitchGb200Nvidia, config, extract_job_id, shell_quote};
use crate::domain::node::FirmwareTaskStatus;
use crate::transport::http_client::HttpClient;
use crate::transport::ssh_client::{SftpUploadOptions, SshClient};
use crate::utilities::error::{ErrorCode, Result, RmsError};

use regex::Regex;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub const NVOS_PARTITION_1_ID: &str = "partition1";
pub const NVOS_PARTITION_2_ID: &str = "partition2";

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

impl SwitchGb200Nvidia {
    pub async fn fetch_system_image(&self, remote_url: &str) -> Result<String> {
        let payload = serde_json::json!({
            "@fetch": {
                "state": "start",
                "parameters": {"remote-url": remote_url}
            }
        });
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
        if !is_valid_firmware_filename(image_filename) {
            return Err(RmsError::invalid_argument(
                "invalid image filename \
                 (only alphanumeric, '-', '_', '.' allowed, no hidden files)",
            ));
        }

        let payload = serde_json::json!({
            "@install": {
                "state": "start",
                "parameters": {"force": true, "image-file": image_filename}
            }
        });
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

    /// Removes old unused images that are not current or boot-next.
    /// Returns the NVOS action job ID.
    pub async fn uninstall_system_image(&self) -> Result<String> {
        // No need to specify image-id, NVUE will infer it from the current system image state
        let payload = serde_json::json!({
            "@uninstall": {
                "state": "start",
                "parameters": {"force": true}
            }
        });
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
            SftpUploadOptions::default(),
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
        sftp_upload_options: SftpUploadOptions,
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

        use tracing::Instrument;

        ssh.sftp_upload_with_options(
            local_path,
            &remote_path,
            sftp_upload_options,
            cancel.clone(),
        )
        .instrument(tracing::info_span!("switch_system_image_sftp_upload"))
        .await?;

        let verify = ssh
            .exec(
                &format!("test -f {} && echo ok", shell_quote(&remote_path)),
                SshClient::DEFAULT_TIMEOUT,
            )
            .await?;
        if !verify.contains("ok") {
            return Err(RmsError::internal(format!(
                "SFTP upload verification failed for {remote_path}"
            )));
        }
        Ok(())
    }
}
