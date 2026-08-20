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

//! Firmware inventory, staging, and NVFWUPD workflows for NVIDIA GB200 switches.

#[cfg(test)]
use regex::Regex;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::validation::{
    firmware_inventory_endpoint_component, is_valid_component, is_valid_firmware_filename,
    is_valid_firmware_inventory_component,
};
use super::{SwitchGb200Nvidia, config, shell_quote};
use crate::domain::node::FirmwareTaskStatus;
#[cfg(test)]
use crate::domain::node::FirmwareType;
use crate::nodes::nvfwupd_adapter;
use crate::transport::http_client::HttpClient;
use crate::transport::ssh_client::SshClient;
use crate::utilities::error::{Result, RmsError};

#[cfg(test)]
pub(super) fn classify_firmware_type_switch(name: &str) -> FirmwareType {
    let lower = name.to_ascii_lowercase();

    if lower.contains("bmc") {
        FirmwareType::BMC
    } else if lower.contains("bios") {
        FirmwareType::BIOS
    } else if lower.contains("fpga") {
        FirmwareType::FPGA
    } else if lower.contains("cpld") {
        FirmwareType::CPLD
    } else {
        FirmwareType::Unknown
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(super) struct FwpkgCpldSubcomponent {
    pub(super) cpld_name: String,
    pub(super) component_id: String,
    pub(super) revision: String,
    pub(super) parent_device_name: String,
    pub(super) component_index: usize,
}

#[cfg(test)]
pub(super) fn map_package_device_name(component: &str) -> Option<&'static str> {
    match component.to_ascii_lowercase().as_str() {
        "bmc" => Some("BMC"),
        "fpga" => Some("SMR"),
        "erot" => Some("EROT"),
        "bios" => Some("SBIOS"),
        "transceiver" => Some("TRANSCEIVER"),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn extract_switch_version_string(firmware: &Value) -> String {
    for field in &["actual-firmware", "version", "Version", "sys_version"] {
        if let Some(v) = firmware.get(*field).and_then(Value::as_str) {
            return v.to_owned();
        }
    }
    "unknown".to_owned()
}

#[cfg(test)]
pub(super) fn build_cpld_package_version(sub: &FwpkgCpldSubcomponent) -> String {
    format!("CPLD{}_{}", sub.component_id, sub.revision)
}

#[cfg(test)]
pub(super) fn strip_leading_zeros(value: &str) -> String {
    match value.find(|c: char| c != '0') {
        Some(pos) => value[pos..].to_owned(),
        None => "0".to_owned(),
    }
}

#[cfg(test)]
pub(super) fn normalize_cpld_version_string(version: &str) -> String {
    if version.is_empty() {
        return String::new();
    }

    let upper = version.to_ascii_uppercase();

    let mut collapsed = String::with_capacity(upper.len());
    let mut prev_separator = false;

    for c in upper.chars() {
        if c.is_ascii_alphanumeric() {
            collapsed.push(c);

            prev_separator = false;
        } else if !prev_separator {
            collapsed.push('_');

            prev_separator = true;
        }
    }

    let re = Regex::new(r"(?:^|_)(?:CPLD_*)?([0-9]+)_*REV_?([0-9]+)(?:_|$)").expect("valid regex");

    if let Some(caps) = re.captures(&collapsed) {
        format!(
            "CPLD{}_REV{}",
            strip_leading_zeros(&caps[1]),
            strip_leading_zeros(&caps[2])
        )
    } else {
        collapsed
    }
}

// ── Reboot hints / job ID extraction ────────────────────────────────

pub(super) fn contains_reboot_hint(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("power cycle") || lower.contains("reboot") || lower.contains("system is offline")
}

/// True when an NVUE action message indicates a no-op operation, like an uninstall action that has no effect.
/// These should be treated as success and not errors.
pub(super) fn indicates_noop(message: &str) -> bool {
    message.contains("Nothing to uninstall")
}

// Extract a human-readable error from an NVUE action `issue` array.
// Each issue entry may be a string, an object with `message`, or an object
// with nested `data.msg`. Multiple entries are joined with "; ". Returns an
// empty string if no usable message is found.
pub(super) fn extract_action_issue_message(response: &Value) -> String {
    let Some(issues) = response.get("issue").and_then(|v| v.as_array()) else {
        return String::new();
    };

    let mut parts: Vec<String> = Vec::with_capacity(issues.len());

    for issue in issues {
        let part = if let Some(s) = issue.as_str() {
            s.to_owned()
        } else if issue.is_object() {
            if let Some(msg) = issue.get("message").and_then(|v| v.as_str()) {
                msg.to_owned()
            } else if let Some(msg) = issue
                .get("data")
                .and_then(|d| d.get("msg"))
                .and_then(|v| v.as_str())
            {
                msg.to_owned()
            } else {
                continue;
            }
        } else {
            continue;
        };

        if !part.is_empty() {
            parts.push(part);
        }
    }

    parts.join("; ")
}

impl SwitchGb200Nvidia {
    pub(super) fn nvfwupd_target_config(&self) -> Result<nvfwupd::workflow::TargetConfig> {
        let Some(host_endpoint) = self.host_endpoint.as_ref() else {
            return Err(RmsError::failed_precondition(
                "switch host endpoint is required for NVUE/NVOS operations",
            ));
        };

        let Some(credentials) = host_endpoint.credentials.as_ref() else {
            return Err(RmsError::failed_precondition(
                "switch host credentials are required for NVUE/NVOS operations",
            ));
        };

        Ok(nvfwupd_adapter::to_target_config_with_secret(
            self.node_type,
            &host_endpoint.endpoint.ip_address,
            host_endpoint.endpoint.port,
            &credentials.username,
            &credentials.password,
            !host_endpoint.dangerously_accept_invalid_certs,
        ))
    }

    pub(super) fn nvfwupd_workflow(&self) -> Result<nvfwupd::workflow_api::WorkflowContext> {
        Ok(nvfwupd::workflow_api::WorkflowContext::with_nvue_client(
            self.nvue_client()?.clone(),
        ))
    }

    pub async fn list_firmware(&self, component: &str, list_files: bool) -> Result<Value> {
        if !is_valid_firmware_inventory_component(component) {
            return Err(RmsError::invalid_argument(format!(
                "invalid firmware component '{component}' \
                 (valid: bmc, fpga, erot, cpld, bios, transceiver, cpld<N>)"
            )));
        }

        let mut endpoint = format!(
            "/nvue_v1/platform/firmware/{}",
            firmware_inventory_endpoint_component(component, list_files)
        );

        if list_files {
            endpoint.push_str("/files");
        }

        self.nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await
    }

    pub async fn push_firmware_file(
        &self,
        local_path: &str,
        component: &str,
        filename: &str,
    ) -> Result<()> {
        if !is_valid_firmware_filename(filename) {
            return Err(RmsError::invalid_argument(
                "firmware filename is invalid \
                 (only alphanumeric, '-', '_', '.' allowed, no hidden files)",
            ));
        }

        if component.is_empty() || !is_valid_component(component) {
            return Err(RmsError::invalid_argument(format!(
                "firmware component '{component}' is invalid \
                 (valid: bmc, fpga, erot, cpld, bios, transceiver)"
            )));
        }

        let lower = component.to_ascii_lowercase();
        let remote_path = format!("/host/fw-images/{lower}/{filename}");
        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        // Skip upload if remote file already matches local size
        if self
            .is_already_staged(&ssh, local_path, &remote_path)
            .await?
        {
            tracing::info!(
                node_id = %self.id,
                component = %lower,
                "firmware file already staged on switch, skipping SFTP upload"
            );

            return Ok(());
        }

        tracing::info!(
            node_id = %self.id,
            component = %lower,
            upload_timeout_secs = SshClient::UPLOAD_TIMEOUT.as_secs(),
            buffer_size_bytes = config::SFTP_BUFFER_SIZE,
            "uploading firmware file via SFTP"
        );

        ssh.sftp_upload(
            local_path,
            &remote_path,
            SshClient::UPLOAD_TIMEOUT,
            CancellationToken::new(),
        )
        .await?;

        let verify_cmd = format!("test -f {} && echo ok", shell_quote(&remote_path));
        let verify_out = ssh.exec(&verify_cmd, SshClient::DEFAULT_TIMEOUT).await?;

        if !verify_out.contains("ok") {
            return Err(RmsError::internal(format!(
                "SFTP upload verification failed for {remote_path}"
            )));
        }

        Ok(())
    }

    pub(super) async fn poll_nvue_action_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let endpoint = format!("/nvue_v1/action/{task_id}");

        let resp = self
            .nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        Self::parse_firmware_task_response(&resp)
    }

    // ── Private inner methods (called with lock already held) ───────

    // Interpret NVUE action state machine: action_success/action_error/
    // action_running/action_failed, with reboot-hint detection.
    fn parse_firmware_task_response(resp: &Value) -> Result<FirmwareTaskStatus> {
        let state = resp
            .get("state")
            .and_then(|v| v.as_str())
            .or_else(|| resp.get("status").and_then(|v| v.as_str()))
            .unwrap_or_default()
            .to_owned();

        let status = resp
            .get("detail")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();

        let mut st = FirmwareTaskStatus {
            state: state.clone(),
            status,
            ..Default::default()
        };

        if state == "action_success" {
            st.completed = true;
            st.percent = 100;
        } else if state == "action_error" {
            let detail = resp
                .get("detail")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();

            let status_field = resp
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();

            let issue_message = extract_action_issue_message(resp);
            st.completed = true;

            if !issue_message.is_empty() {
                st.message = issue_message;

                if indicates_noop(&st.message) {
                    st.completed = true;
                    st.percent = 100;
                    st.state = "action_success".to_owned();
                }
            } else if contains_reboot_hint(&detail) || contains_reboot_hint(&status_field) {
                // NVUE reports "action_error" for a reboot; treat as success
                st.percent = 100;
            } else if !detail.is_empty() {
                st.message = detail;
            } else {
                st.message = status_field;
            }
        } else if state == "action_running" {
            let status_field = resp
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();

            if contains_reboot_hint(&status_field) {
                st.completed = true;
                st.percent = 100;
                st.state = "action_success".to_owned();
            }
        } else if state == "action_failed" {
            st.completed = true;
            let issue_message = extract_action_issue_message(resp);

            if !issue_message.is_empty() {
                st.message = issue_message;
            } else {
                st.message = resp
                    .get("detail")
                    .and_then(|v| v.as_str())
                    .unwrap_or("firmware job failed")
                    .to_owned();
            }
        }

        Ok(st)
    }
}
