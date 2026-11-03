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

//! Structured host-side Flint firmware workflows.
//!
//! The process CLI and hosted callers share the NIC workflow, which validates
//! the complete device/image PSID plan before it starts flashing.
//!
//! The LinkX workflow uses the same Flint/SSH transport to update VRNVL72 compute
//! OSFP modules. It validates connector health and flashes sequentially with
//! immediate module activation, without a tray reset. The wiring and commands
//! follow cobalt-spods' guarded compute LinkX workflow
//! (`network_validation/manage_compute_linkx_firmware.py`, 5cdb173).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::os_access::OsAccess;
use crate::ssh_options::{SSH_HOST_KEY_MODE_ARG, SSH_KNOWN_HOSTS_ARG};
use crate::workflow::{
    FirmwareUpdateOutcome, FirmwareVersionCheckSummary, FlintDeviceFamily,
    FlintExpectedDeviceCounts, FlintFirmwareTarget, FlintFirmwareUpdateRequest,
    FlintFirmwareVersionCheckRequest, HostTargetConfig, NvFwUpdError, Result, SshHostKeyMode,
    UpdateSummary,
};

const QUERY_TIMEOUT_SECS: u64 = 60;
const TRANSFER_TIMEOUT_SECS: u64 = 300;
const FLASH_TIMEOUT_MULTIPLIER: u64 = 20;
const FLASH_TIMEOUT_SECS: u64 = FLASH_TIMEOUT_MULTIPLIER * QUERY_TIMEOUT_SECS;
const REMOTE_TEMP_PREFIX: &str = "/tmp/nvfwupd-flint.";
const HOST_OS_BOOT_TIMEOUT_SECS: u64 = 6 * 60;
const HOST_OS_PROBE_TIMEOUT_SECS: u64 = 30;
#[cfg(not(test))]
const HOST_OS_BOOT_ATTEMPTS: u32 = 25;
#[cfg(test)]
const HOST_OS_BOOT_ATTEMPTS: u32 = 3;
#[cfg(not(test))]
const HOST_OS_BOOT_POLL_INTERVAL: Duration = Duration::from_secs(15);
#[cfg(test)]
const HOST_OS_BOOT_POLL_INTERVAL: Duration = Duration::ZERO;
const HOST_OS_READY_COMMAND: &str = "if command -v systemctl >/dev/null 2>&1; then \
state=$(systemctl is-system-running 2>/dev/null || true); \
case \"$state\" in running|degraded) exit 0 ;; \
*) printf 'system-state:%s\\n' \"$state\"; exit 1 ;; esac; \
else true; fi";
const FLINT_ALREADY_UPDATED_PENDING_RESET: &str =
    "the firmware image was already updated on flash, pending reset";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

#[async_trait]
trait FlintHostAccess: Clone + Send + Sync + 'static {
    fn target(&self) -> &str;

    async fn execute(&self, command: &str, timeout_secs: u64, use_sudo: bool) -> CommandOutput;

    async fn upload(&self, local_path: &str, remote_path: &str) -> CommandOutput;
}

#[async_trait]
pub(crate) trait FlintDeviceDiscoveryRecovery: Send {
    async fn recover(&mut self) -> Result<()>;
}

#[async_trait]
impl FlintHostAccess for OsAccess {
    fn target(&self) -> &str {
        &self.ip
    }

    async fn execute(&self, command: &str, timeout_secs: u64, use_sudo: bool) -> CommandOutput {
        let (success, stdout, stderr) = self.execute_command(command, timeout_secs, use_sudo).await;
        CommandOutput {
            success,
            stdout,
            stderr,
        }
    }

    async fn upload(&self, local_path: &str, remote_path: &str) -> CommandOutput {
        let (success, stdout, stderr) = self
            .transfer_file(local_path, Some(remote_path), TRANSFER_TIMEOUT_SECS)
            .await;
        CommandOutput {
            success,
            stdout,
            stderr,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FlintDevice {
    family: FlintDeviceFamily,
    path: String,
    psid: String,
    version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FlintImage {
    family: FlintDeviceFamily,
    local_path: String,
    remote_path: String,
    psid: String,
    version: String,
}

#[derive(Debug, Clone)]
struct PlannedUpdate {
    device: FlintDevice,
    image: FlintImage,
}

/// Update host-visible adapter firmware using Flint.
///
/// # Errors
///
/// Returns a structured error when SSH, required tools, device discovery,
/// image validation, file transfer, or a Flint flash fails.
pub async fn update_firmware(
    host: HostTargetConfig,
    request: FlintFirmwareUpdateRequest,
) -> Result<FirmwareUpdateOutcome> {
    let access = os_access_from_config(&host)?;
    update_firmware_with_access(access, request).await
}

pub(crate) async fn update_firmware_with_discovery_recovery(
    host: HostTargetConfig,
    request: FlintFirmwareUpdateRequest,
    recovery: &mut (dyn FlintDeviceDiscoveryRecovery + Send),
) -> Result<FirmwareUpdateOutcome> {
    let access = os_access_from_config(&host)?;
    update_firmware_with_access_and_recovery(access, request, Some(recovery)).await
}

/// Verify host-visible adapter firmware against Flint image metadata.
///
/// # Errors
///
/// Returns a structured error when the host cannot be queried or the supplied
/// image set cannot be mapped safely to the discovered devices.
pub async fn verify_firmware_versions(
    host: HostTargetConfig,
    request: FlintFirmwareVersionCheckRequest,
) -> Result<FirmwareVersionCheckSummary> {
    let access = os_access_from_config(&host)?;
    verify_firmware_versions_with_access(access, request).await
}

/// Wait for the host OS to finish booting and accept SSH commands.
pub(crate) async fn wait_for_host_os_ready(
    host: HostTargetConfig,
    cancellation: Option<CancellationToken>,
) -> Result<u32> {
    let access = os_access_from_config(&host)?;
    wait_for_host_os_ready_with_access(&access, cancellation.as_ref()).await
}

async fn wait_for_host_os_ready_with_access<A: FlintHostAccess>(
    access: &A,
    cancellation: Option<&CancellationToken>,
) -> Result<u32> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(HOST_OS_BOOT_TIMEOUT_SECS);
    for attempt in 1..=HOST_OS_BOOT_ATTEMPTS {
        check_cancelled(cancellation, "wait for GB200 host OS boot")?;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let probe_timeout_secs = remaining.as_secs().min(HOST_OS_PROBE_TIMEOUT_SECS);
        if probe_timeout_secs == 0 {
            break;
        }
        let output = access
            .execute(HOST_OS_READY_COMMAND, probe_timeout_secs, false)
            .await;
        if output.success {
            return Ok(attempt);
        }

        let system_is_booting = output.stdout.trim().starts_with("system-state:");
        let error = command_error(
            access.target(),
            "host OS readiness probe",
            output,
            false,
            probe_timeout_secs,
        );
        let retryable = system_is_booting
            || matches!(
                &error,
                NvFwUpdError::HostUnreachable { .. } | NvFwUpdError::Timeout { .. }
            );
        if !retryable {
            return Err(error);
        }
        if attempt < HOST_OS_BOOT_ATTEMPTS {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let poll_interval = HOST_OS_BOOT_POLL_INTERVAL.min(remaining);
            if poll_interval.is_zero() && !cfg!(test) {
                break;
            }
            if let Some(cancellation) = cancellation {
                tokio::select! {
                    () = tokio::time::sleep(poll_interval) => {}
                    () = cancellation.cancelled() => {
                        return Err(NvFwUpdError::Cancelled {
                            operation: "wait for GB200 host OS boot",
                        });
                    }
                }
            } else {
                tokio::time::sleep(poll_interval).await;
            }
        }
    }

    Err(NvFwUpdError::Timeout {
        operation: "wait for GB200 host OS boot",
        seconds: HOST_OS_BOOT_TIMEOUT_SECS,
    })
}

async fn update_firmware_with_access<A: FlintHostAccess>(
    access: A,
    request: FlintFirmwareUpdateRequest,
) -> Result<FirmwareUpdateOutcome> {
    update_firmware_with_access_and_recovery(access, request, None).await
}

async fn update_firmware_with_access_and_recovery<A: FlintHostAccess>(
    access: A,
    request: FlintFirmwareUpdateRequest,
    discovery_recovery: Option<&mut (dyn FlintDeviceDiscoveryRecovery + Send)>,
) -> Result<FirmwareUpdateOutcome> {
    let query_timeout_secs = request
        .timeout_secs
        .filter(|timeout| *timeout > 0)
        .unwrap_or(QUERY_TIMEOUT_SECS);
    let flash_timeout_secs = query_timeout_secs.saturating_mul(FLASH_TIMEOUT_MULTIPLIER);
    validate_targets(&request.targets)?;
    check_cancelled(request.cancellation.as_ref(), "Flint update setup")?;
    validate_host_tools(&access, query_timeout_secs).await?;
    check_cancelled(request.cancellation.as_ref(), "Flint device discovery")?;
    let devices = discover_devices_with_recovery(
        &access,
        &request.targets,
        &request.expected_device_counts,
        query_timeout_secs,
        request.cancellation.as_ref(),
        discovery_recovery,
    )
    .await?;
    check_cancelled(request.cancellation.as_ref(), "Flint temporary staging")?;
    let remote_dir = create_remote_temp_dir(&access, query_timeout_secs).await?;
    let result = async {
        check_cancelled(request.cancellation.as_ref(), "Flint image staging")?;
        let images =
            stage_and_query_images(&access, &remote_dir, &request.targets, query_timeout_secs)
                .await?;
        check_cancelled(request.cancellation.as_ref(), "Flint update planning")?;
        let plan = build_update_plan(&request.targets, devices, images)?;
        let pending: Vec<_> = plan
            .into_iter()
            .filter(|entry| request.force_update || entry.device.version != entry.image.version)
            .collect();

        if pending.is_empty() {
            return Ok(FirmwareUpdateOutcome::Skipped {
                reason: "all discovered Flint devices already report the requested versions"
                    .to_owned(),
            });
        }

        check_cancelled(request.cancellation.as_ref(), "Flint flashes")?;
        let (completed, first_failures) =
            run_flash_batch(access.clone(), pending, flash_timeout_secs).await?;
        let retry_cancelled = !first_failures.is_empty()
            && request
                .cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled);
        let (mut completed, failures) = if first_failures.is_empty() {
            (completed, Vec::new())
        } else if retry_cancelled {
            (completed, first_failures)
        } else {
            let retry_updates = first_failures
                .into_iter()
                .map(|failure| failure.update)
                .collect();
            let (retried, failures) =
                run_flash_batch(access.clone(), retry_updates, flash_timeout_secs).await?;
            let mut completed = completed;
            completed.extend(retried);
            (completed, failures)
        };

        completed.sort_by(|left, right| left.device.path.cmp(&right.device.path));
        if !failures.is_empty() {
            let details = failures
                .iter()
                .map(|failure| {
                    format!(
                        "{} ({}): {}",
                        failure.update.device.path,
                        failure.update.device.family.component(),
                        failure.error
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            let failure_context = if retry_cancelled {
                "retry skipped because cancellation was requested"
            } else {
                "failed after one retry"
            };
            return Err(NvFwUpdError::TaskFailed {
                task_id: None,
                message: format!("Flint flash {failure_context}: {details}"),
            });
        }

        Ok(FirmwareUpdateOutcome::Completed(UpdateSummary {
            message: format!("Flint firmware updated on {} device(s)", completed.len()),
            task_ids: Vec::new(),
            details: json!({
                "status": "completed",
                "force_update": request.force_update,
                "devices": completed.iter().map(update_result_json).collect::<Vec<_>>(),
            }),
        }))
    }
    .await;
    cleanup_remote_temp_dir(&access, &remote_dir, query_timeout_secs).await;
    result
}

async fn discover_devices_with_recovery<A: FlintHostAccess>(
    access: &A,
    targets: &[FlintFirmwareTarget],
    expected_device_counts: &FlintExpectedDeviceCounts,
    query_timeout_secs: u64,
    cancellation: Option<&CancellationToken>,
    discovery_recovery: Option<&mut (dyn FlintDeviceDiscoveryRecovery + Send)>,
) -> Result<Vec<FlintDevice>> {
    let first_error = match start_mst_and_discover_devices(
        access,
        targets,
        expected_device_counts,
        query_timeout_secs,
    )
    .await
    {
        Ok(devices) => return Ok(devices),
        Err(error) => error,
    };
    let Some(recovery) = discovery_recovery else {
        return Err(first_error);
    };
    if !flint_discovery_recovery_allowed(&first_error) {
        return Err(first_error);
    }

    tracing::warn!(
        error = %first_error,
        "Flint device enumeration failed; retrying once after GB200 auxiliary AC-cycle recovery"
    );
    check_cancelled(cancellation, "Flint device discovery recovery")?;
    recovery.recover().await?;
    check_cancelled(cancellation, "Flint device discovery retry")?;
    start_mst_and_discover_devices(access, targets, expected_device_counts, query_timeout_secs)
        .await
}

fn flint_discovery_recovery_allowed(error: &NvFwUpdError) -> bool {
    matches!(
        error,
        NvFwUpdError::HostUnreachable { .. }
            | NvFwUpdError::IncompatibleFirmware { .. }
            | NvFwUpdError::TaskFailed { .. }
            | NvFwUpdError::Timeout { .. }
            | NvFwUpdError::InvalidResponse { .. }
    )
}

async fn verify_firmware_versions_with_access<A: FlintHostAccess>(
    access: A,
    request: FlintFirmwareVersionCheckRequest,
) -> Result<FirmwareVersionCheckSummary> {
    validate_targets(&request.targets)?;
    check_cancelled(
        request.cancellation.as_ref(),
        "Flint version verification setup",
    )?;
    prepare_host(&access, QUERY_TIMEOUT_SECS).await?;
    check_cancelled(
        request.cancellation.as_ref(),
        "Flint version verification staging",
    )?;
    let remote_dir = create_remote_temp_dir(&access, QUERY_TIMEOUT_SECS).await?;
    let result = async {
        check_cancelled(
            request.cancellation.as_ref(),
            "Flint version verification discovery",
        )?;
        let devices = discover_and_query_devices(
            &access,
            &request.targets,
            &request.expected_device_counts,
            QUERY_TIMEOUT_SECS,
        )
        .await?;
        check_cancelled(
            request.cancellation.as_ref(),
            "Flint version verification image staging",
        )?;
        let images =
            stage_and_query_images(&access, &remote_dir, &request.targets, QUERY_TIMEOUT_SECS)
                .await?;
        check_cancelled(
            request.cancellation.as_ref(),
            "Flint version verification comparison",
        )?;
        let plan = build_update_plan(&request.targets, devices, images)?;
        let components: Vec<Value> = plan
            .iter()
            .map(|entry| {
                let matched = entry.device.version == entry.image.version;
                json!({
                    "name": entry.device.path,
                    "component": entry.device.family.component(),
                    "psid": entry.device.psid,
                    "system_version": entry.device.version,
                    "package_version": entry.image.version,
                    "firmware_file": entry.image.local_path,
                    "status": if matched { "matched" } else { "mismatched" },
                })
            })
            .collect();
        let mismatches: Vec<_> = components
            .iter()
            .filter(|component| component["status"] == "mismatched")
            .cloned()
            .collect();
        Ok(FirmwareVersionCheckSummary {
            matched: mismatches.is_empty(),
            details: json!({
                "status": if mismatches.is_empty() { "matched" } else { "mismatch" },
                "components": components,
                "mismatches": mismatches,
            }),
        })
    }
    .await;
    cleanup_remote_temp_dir(&access, &remote_dir, QUERY_TIMEOUT_SECS).await;
    result
}

fn os_access_from_config(config: &HostTargetConfig) -> Result<OsAccess> {
    let mut args = HashMap::from([
        ("ip".to_owned(), config.ip.clone()),
        ("user".to_owned(), config.username.clone()),
        ("password".to_owned(), config.password.clone()),
        ("port".to_owned(), config.port.unwrap_or(22).to_string()),
        (
            SSH_HOST_KEY_MODE_ARG.to_owned(),
            config.ssh_host_key_mode.as_target_arg().to_owned(),
        ),
    ]);
    if let Some(known_hosts) = config.ssh_known_hosts.as_ref() {
        args.insert(SSH_KNOWN_HOSTS_ARG.to_owned(), known_hosts.clone());
    }
    let access = OsAccess::from_arg_dict(&args);
    let (valid, message) = access.is_valid();
    if valid {
        Ok(access)
    } else {
        Err(NvFwUpdError::InvalidResponse {
            context: "host target configuration",
            message,
        })
    }
}

fn validate_targets(targets: &[FlintFirmwareTarget]) -> Result<()> {
    if targets.is_empty() {
        return Err(NvFwUpdError::InvalidResponse {
            context: "Flint update request",
            message: "at least one device-family target is required".to_owned(),
        });
    }
    let mut families = BTreeSet::new();
    let mut basenames = BTreeSet::new();
    for target in targets {
        if !families.insert(target.family) {
            return Err(NvFwUpdError::InvalidResponse {
                context: "Flint update request",
                message: format!("duplicate target for {}", target.family.component()),
            });
        }
        if target.firmware_files.is_empty() {
            return Err(NvFwUpdError::InvalidResponse {
                context: "Flint update request",
                message: format!("{} has no firmware images", target.family.component()),
            });
        }
        for path in &target.firmware_files {
            let basename = safe_firmware_basename(path)?;
            if !basenames.insert(basename.clone()) {
                return Err(NvFwUpdError::IncompatibleFirmware {
                    component: target.family.component().to_owned(),
                    message: format!("duplicate remote image basename {basename}"),
                });
            }
        }
    }
    Ok(())
}

async fn prepare_host<A: FlintHostAccess>(access: &A, query_timeout_secs: u64) -> Result<()> {
    validate_host_tools(access, query_timeout_secs).await?;
    start_mst(access, query_timeout_secs).await
}

async fn validate_host_tools<A: FlintHostAccess>(
    access: &A,
    query_timeout_secs: u64,
) -> Result<()> {
    let tools = access
        .execute(
            "command -v mst && command -v flint",
            query_timeout_secs,
            false,
        )
        .await;
    if !tools.success {
        return Err(command_error(
            access.target(),
            "mst/flint",
            tools,
            true,
            query_timeout_secs,
        ));
    }
    Ok(())
}

async fn start_mst<A: FlintHostAccess>(access: &A, query_timeout_secs: u64) -> Result<()> {
    let mst = access.execute("mst start", query_timeout_secs, true).await;
    if !mst.success {
        return Err(command_error(
            access.target(),
            "mst",
            mst,
            false,
            query_timeout_secs,
        ));
    }
    Ok(())
}

async fn start_mst_and_discover_devices<A: FlintHostAccess>(
    access: &A,
    targets: &[FlintFirmwareTarget],
    expected_device_counts: &FlintExpectedDeviceCounts,
    query_timeout_secs: u64,
) -> Result<Vec<FlintDevice>> {
    start_mst(access, query_timeout_secs).await?;
    discover_and_query_devices(access, targets, expected_device_counts, query_timeout_secs).await
}

async fn create_remote_temp_dir<A: FlintHostAccess>(
    access: &A,
    query_timeout_secs: u64,
) -> Result<String> {
    let output = access
        .execute(
            "mktemp -d /tmp/nvfwupd-flint.XXXXXX",
            query_timeout_secs,
            false,
        )
        .await;
    if !output.success {
        return Err(command_error(
            access.target(),
            "mktemp",
            output,
            false,
            query_timeout_secs,
        ));
    }
    let path = output.stdout.trim();
    if !path.starts_with(REMOTE_TEMP_PREFIX)
        || path[REMOTE_TEMP_PREFIX.len()..].is_empty()
        || path.contains(char::is_whitespace)
    {
        return Err(NvFwUpdError::InvalidResponse {
            context: "remote temporary directory",
            message: "mktemp returned an unexpected path".to_owned(),
        });
    }
    Ok(path.to_owned())
}

async fn cleanup_remote_temp_dir<A: FlintHostAccess>(
    access: &A,
    remote_dir: &str,
    query_timeout_secs: u64,
) {
    if !remote_dir.starts_with(REMOTE_TEMP_PREFIX) {
        tracing::warn!(
            remote_dir,
            "refusing to clean unexpected Flint temporary path"
        );
        return;
    }
    let command = format!("rm -rf -- {}", shell_quote(remote_dir));
    let output = access.execute(&command, query_timeout_secs, false).await;
    if !output.success {
        tracing::warn!(
            target = access.target(),
            error = %sanitized_command_message(&output),
            "failed to clean Flint temporary directory"
        );
    }
}

async fn discover_and_query_devices<A: FlintHostAccess>(
    access: &A,
    targets: &[FlintFirmwareTarget],
    expected_device_counts: &FlintExpectedDeviceCounts,
    query_timeout_secs: u64,
) -> Result<Vec<FlintDevice>> {
    let status = access
        .execute("mst status -v", query_timeout_secs, true)
        .await;
    if !status.success {
        return Err(command_error(
            access.target(),
            "mst",
            status,
            false,
            query_timeout_secs,
        ));
    }
    let discovered = parse_mst_status(&status.stdout)?;
    validate_expected_device_counts(expected_device_counts, &discovered)?;
    let requested: BTreeSet<_> = targets.iter().map(|target| target.family).collect();
    let mut selected: Vec<_> = discovered
        .into_iter()
        .filter(|(family, _)| requested.contains(family))
        .collect();
    selected.sort_by(|left, right| left.1.cmp(&right.1));
    validate_discovered_families(&requested, &selected)?;

    let mut devices = Vec::with_capacity(selected.len());
    for (family, path) in selected {
        let command = format!("flint -d {} --yes query", shell_quote(&path));
        let output = access.execute(&command, query_timeout_secs, true).await;
        if !output.success {
            return Err(command_error(
                access.target(),
                "flint",
                output,
                false,
                query_timeout_secs,
            ));
        }
        let (version, psid) = parse_flint_query(&output.stdout);
        devices.push(FlintDevice {
            family,
            path,
            psid: psid.ok_or_else(|| NvFwUpdError::InvalidResponse {
                context: "Flint device query",
                message: "device query did not report a PSID".to_owned(),
            })?,
            version: version.ok_or_else(|| NvFwUpdError::InvalidResponse {
                context: "Flint device query",
                message: "device query did not report FW Version".to_owned(),
            })?,
        });
    }
    Ok(devices)
}

fn validate_expected_device_counts(
    expected: &FlintExpectedDeviceCounts,
    discovered: &[(FlintDeviceFamily, String)],
) -> Result<()> {
    let mut actual = BTreeMap::<FlintDeviceFamily, usize>::new();
    for (family, _) in discovered {
        *actual.entry(*family).or_default() += 1;
    }

    let mismatches: Vec<_> = expected
        .iter()
        .filter_map(|(family, expected_count)| {
            let actual_count = actual.get(family).copied().unwrap_or_default();
            (actual_count != *expected_count).then(|| {
                format!(
                    "{} expected {}, found {}",
                    family.component(),
                    expected_count,
                    actual_count
                )
            })
        })
        .collect();
    if mismatches.is_empty() {
        return Ok(());
    }

    Err(NvFwUpdError::IncompatibleFirmware {
        component: "Flint device inventory".to_owned(),
        message: format!(
            "mst status physical-device counts do not match the expected inventory profile: {}",
            mismatches.join("; ")
        ),
    })
}

fn validate_discovered_families(
    requested: &BTreeSet<FlintDeviceFamily>,
    discovered: &[(FlintDeviceFamily, String)],
) -> Result<()> {
    let present: BTreeSet<_> = discovered.iter().map(|(family, _)| *family).collect();
    let both_connectx_requested = requested.contains(&FlintDeviceFamily::ConnectX7)
        && requested.contains(&FlintDeviceFamily::ConnectX8);
    for family in requested {
        if present.contains(family) {
            continue;
        }
        let absent_connectx_alternative = both_connectx_requested
            && matches!(
                family,
                FlintDeviceFamily::ConnectX7 | FlintDeviceFamily::ConnectX8
            )
            && present.iter().any(|present_family| {
                matches!(
                    present_family,
                    FlintDeviceFamily::ConnectX7 | FlintDeviceFamily::ConnectX8
                )
            });
        if !absent_connectx_alternative {
            return Err(NvFwUpdError::IncompatibleFirmware {
                component: family.component().to_owned(),
                message: format!(
                    "mst status reported no physical {} devices",
                    family.mst_device_type()
                ),
            });
        }
    }
    if present.is_empty() {
        return Err(NvFwUpdError::IncompatibleFirmware {
            component: "Flint".to_owned(),
            message: "mst status reported no requested physical devices".to_owned(),
        });
    }
    Ok(())
}

async fn stage_and_query_images<A: FlintHostAccess>(
    access: &A,
    remote_dir: &str,
    targets: &[FlintFirmwareTarget],
    query_timeout_secs: u64,
) -> Result<Vec<FlintImage>> {
    let mut images = Vec::new();
    for target in targets {
        for local_path in &target.firmware_files {
            let basename = safe_firmware_basename(local_path)?;
            let remote_path = format!("{remote_dir}/{basename}");
            let upload = access.upload(local_path, &remote_path).await;
            if !upload.success {
                return Err(command_error(
                    access.target(),
                    "SFTP upload",
                    upload,
                    false,
                    TRANSFER_TIMEOUT_SECS,
                ));
            }
            let command = format!("flint -i {} query", shell_quote(&remote_path));
            let output = access.execute(&command, query_timeout_secs, true).await;
            if !output.success {
                return Err(command_error(
                    access.target(),
                    "flint",
                    output,
                    false,
                    query_timeout_secs,
                ));
            }
            let (version, psid) = parse_flint_query(&output.stdout);
            images.push(FlintImage {
                family: target.family,
                local_path: local_path.clone(),
                remote_path,
                psid: psid.ok_or_else(|| NvFwUpdError::InvalidResponse {
                    context: "Flint image query",
                    message: format!("image {basename} did not report a PSID"),
                })?,
                version: version.ok_or_else(|| NvFwUpdError::InvalidResponse {
                    context: "Flint image query",
                    message: format!("image {basename} did not report FW Version"),
                })?,
            });
        }
    }
    select_equivalent_images(images)
}

fn select_equivalent_images(images: Vec<FlintImage>) -> Result<Vec<FlintImage>> {
    let mut by_identity: BTreeMap<(FlintDeviceFamily, String), Vec<FlintImage>> = BTreeMap::new();
    for image in images {
        by_identity
            .entry((image.family, image.psid.clone()))
            .or_default()
            .push(image);
    }
    let mut selected = Vec::with_capacity(by_identity.len());
    for ((family, psid), mut candidates) in by_identity {
        let versions: BTreeSet<_> = candidates
            .iter()
            .map(|image| image.version.as_str())
            .collect();
        if versions.len() != 1 {
            return Err(NvFwUpdError::IncompatibleFirmware {
                component: family.component().to_owned(),
                message: format!("PSID {psid} has candidate images with different versions"),
            });
        }
        candidates.sort_by(|left, right| {
            image_preference(&left.local_path)
                .cmp(&image_preference(&right.local_path))
                .then_with(|| left.local_path.cmp(&right.local_path))
        });
        if candidates.len() > 1
            && image_preference(&candidates[0].local_path)
                == image_preference(&candidates[1].local_path)
        {
            return Err(NvFwUpdError::IncompatibleFirmware {
                component: family.component().to_owned(),
                message: format!(
                    "PSID {psid} has multiple equally preferred images for version {}",
                    candidates[0].version
                ),
            });
        }
        if let Some(image) = candidates.into_iter().next() {
            selected.push(image);
        }
    }
    Ok(selected)
}

fn build_update_plan(
    targets: &[FlintFirmwareTarget],
    devices: Vec<FlintDevice>,
    images: Vec<FlintImage>,
) -> Result<Vec<PlannedUpdate>> {
    let requested: BTreeSet<_> = targets.iter().map(|target| target.family).collect();
    let mut image_by_identity = BTreeMap::new();
    for image in images {
        image_by_identity.insert((image.family, image.psid.clone()), image);
    }
    let mut plan = Vec::with_capacity(devices.len());
    for device in devices {
        let key = (device.family, device.psid.clone());
        let image = image_by_identity.get(&key).cloned().ok_or_else(|| {
            NvFwUpdError::IncompatibleFirmware {
                component: device.family.component().to_owned(),
                message: format!(
                    "no candidate image matches device {} PSID {}",
                    device.path, device.psid
                ),
            }
        })?;
        plan.push(PlannedUpdate { device, image });
    }
    if plan.is_empty() && !requested.is_empty() {
        return Err(NvFwUpdError::IncompatibleFirmware {
            component: "Flint".to_owned(),
            message: "no requested device could be mapped to a firmware image".to_owned(),
        });
    }
    Ok(plan)
}

#[derive(Debug)]
struct FlashFailure {
    update: PlannedUpdate,
    error: String,
}

async fn run_flash_batch<A: FlintHostAccess>(
    access: A,
    updates: Vec<PlannedUpdate>,
    flash_timeout_secs: u64,
) -> Result<(Vec<PlannedUpdate>, Vec<FlashFailure>)> {
    let mut tasks = JoinSet::new();
    for update in updates {
        let access = access.clone();
        tasks.spawn(async move {
            let command = format!(
                "flint -d {} --yes -i {} b",
                shell_quote(&update.device.path),
                shell_quote(&update.image.remote_path)
            );
            let output = access.execute(&command, flash_timeout_secs, true).await;
            if output.success || flint_flash_is_pending_reset(&output) {
                if !output.success {
                    tracing::info!(
                        device = update.device.path,
                        "Flint reports firmware already staged and pending reset; treating flash as complete"
                    );
                }
                Ok(update)
            } else {
                Err(FlashFailure {
                    update,
                    error: sanitized_command_message(&output),
                })
            }
        });
    }
    let mut completed = Vec::new();
    let mut failed = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(update)) => completed.push(update),
            Ok(Err(failure)) => {
                tracing::warn!(
                    device = failure.update.device.path,
                    error = failure.error,
                    "Flint flash attempt failed"
                );
                failed.push(failure);
            }
            Err(error) => {
                return Err(NvFwUpdError::TaskFailed {
                    task_id: None,
                    message: format!("Flint worker task failed: {error}"),
                });
            }
        }
    }
    Ok((completed, failed))
}

fn flint_flash_is_pending_reset(output: &CommandOutput) -> bool {
    !output.success
        && (output
            .stdout
            .to_ascii_lowercase()
            .contains(FLINT_ALREADY_UPDATED_PENDING_RESET)
            || output
                .stderr
                .to_ascii_lowercase()
                .contains(FLINT_ALREADY_UPDATED_PENDING_RESET))
}

fn update_result_json(update: &PlannedUpdate) -> Value {
    json!({
        "component": update.device.family.component(),
        "device": update.device.path,
        "psid": update.device.psid,
        "previous_version": update.device.version,
        "expected_version": update.image.version,
        "firmware_file": update.image.local_path,
    })
}

fn safe_firmware_basename(path: &str) -> Result<String> {
    let basename = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| NvFwUpdError::PackageParse {
            path: path.to_owned(),
            message: "firmware image requires a valid UTF-8 basename".to_owned(),
        })?;
    if !basename
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
    {
        return Err(NvFwUpdError::PackageParse {
            path: path.to_owned(),
            message: "firmware image basename contains unsupported characters".to_owned(),
        });
    }
    let lower = basename.to_ascii_lowercase();
    if !(lower.ends_with(".bin") || lower.ends_with(".pldm")) {
        return Err(NvFwUpdError::PackageParse {
            path: path.to_owned(),
            message: "Flint firmware image must end in .bin or .pldm".to_owned(),
        });
    }
    Ok(basename.to_owned())
}

fn image_preference(path: &str) -> u8 {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".signed.reduced.bin") {
        0
    } else if lower.ends_with(".signed.pldm") {
        1
    } else if lower.ends_with(".bin") {
        2
    } else {
        3
    }
}

fn command_error(
    target: &str,
    tool: &str,
    output: CommandOutput,
    tool_precondition: bool,
    timeout_secs: u64,
) -> NvFwUpdError {
    let raw_message = if output.stderr.trim().is_empty() {
        output.stdout.trim()
    } else {
        output.stderr.trim()
    };
    let lower = raw_message.to_ascii_lowercase();
    let message = sanitized_command_message(&output);
    if lower.contains("authentication")
        || lower.contains("permission denied") && lower.contains("password")
    {
        NvFwUpdError::AuthFailed {
            target: target.to_owned(),
            message,
        }
    } else if lower.contains("sudo")
        && (lower.contains("password is required")
            || lower.contains("not in the sudoers")
            || lower.contains("not allowed")
            || lower.contains("permission denied"))
    {
        NvFwUpdError::HostSudoUnavailable { message }
    } else if lower.contains("timed out") || lower.contains("timeout") {
        NvFwUpdError::Timeout {
            operation: "host Flint command",
            seconds: timeout_secs,
        }
    } else if lower.contains("connection refused")
        || lower.contains("no route to host")
        || lower.contains("connection error")
    {
        NvFwUpdError::HostUnreachable {
            target: target.to_owned(),
            message,
        }
    } else if tool_precondition {
        NvFwUpdError::HostToolUnavailable {
            tool: tool.to_owned(),
            message,
        }
    } else {
        NvFwUpdError::TaskFailed {
            task_id: None,
            message: format!("host command {tool} failed: {message}"),
        }
    }
}

fn sanitized_command_message(output: &CommandOutput) -> String {
    let message = if output.stderr.trim().is_empty() {
        output.stdout.trim()
    } else {
        output.stderr.trim()
    };
    crate::utils::Util::sanitize_log(message)
}

fn check_cancelled(token: Option<&CancellationToken>, operation: &'static str) -> Result<()> {
    if token.is_some_and(CancellationToken::is_cancelled) {
        Err(NvFwUpdError::Cancelled { operation })
    } else {
        Ok(())
    }
}

/// Parse physical MST devices and classify supported adapter families.
fn parse_mst_status(output: &str) -> Result<Vec<(FlintDeviceFamily, String)>> {
    output
        .lines()
        .find(|line| line.contains("DEVICE_TYPE") && line.contains("MST"))
        .ok_or_else(|| NvFwUpdError::InvalidResponse {
            context: "mst status",
            message: "output is missing the DEVICE_TYPE/MST header".to_owned(),
        })?;
    let mut devices = Vec::new();
    for line in output.lines() {
        if line.contains("Inband devices:") {
            break;
        }
        let Some(path_start) = line.find("/dev/mst/") else {
            continue;
        };
        let device_type = line[..path_start].trim();
        let Some(path) = line[path_start..].split_whitespace().next() else {
            continue;
        };
        if is_virtual_function_path(path) {
            continue;
        }
        if let Some(family) = family_from_mst_device_type(device_type) {
            devices.push((family, path.to_owned()));
        }
    }
    Ok(devices)
}

fn family_from_mst_device_type(device_type: &str) -> Option<FlintDeviceFamily> {
    let normalized: String = device_type
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    if normalized.starts_with("connectx7") {
        Some(FlintDeviceFamily::ConnectX7)
    } else if normalized.starts_with("connectx8") {
        Some(FlintDeviceFamily::ConnectX8)
    } else if normalized.starts_with("bluefield3") {
        Some(FlintDeviceFamily::BlueField3)
    } else {
        None
    }
}

fn is_virtual_function_path(path: &str) -> bool {
    path.rsplit('.').next().is_some_and(|tail| {
        tail.len() == 1 && tail.chars().all(|character| character.is_ascii_digit())
    })
}

fn parse_flint_query(output: &str) -> (Option<String>, Option<String>) {
    let mut version = None;
    let mut psid = None;
    for line in output.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        match key.trim() {
            "FW Version" => version = Some(value.trim().to_owned()),
            "PSID" => psid = Some(value.trim().to_owned()),
            _ => {}
        }
    }
    (version, psid)
}

pub(crate) fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

const LINKX_RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);
#[cfg(not(test))]
const LINKX_POLL_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(test)]
const LINKX_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Physical connector selection for the supported VR CX9 wiring layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LinkxConnector {
    C1,
    C3,
    C5,
    C7,
}

impl LinkxConnector {
    pub const ALL: [Self; 4] = [Self::C1, Self::C3, Self::C5, Self::C7];

    /// Exact physical programming device; C5/C7 ordering is intentional.
    fn device(self) -> String {
        let index = match self {
            Self::C1 => 1,
            Self::C3 => 3,
            Self::C5 => 7,
            Self::C7 => 5,
        };
        format!("/dev/mst/mt4133_pciconf{index}")
    }

    fn module_paths(self) -> [String; 2] {
        let index = match self {
            Self::C1 => 0,
            Self::C3 => 2,
            Self::C5 => 6,
            Self::C7 => 4,
        };
        [index, index + 1].map(|i| format!("/dev/mst/mt4133_pciconf{i}.3"))
    }

    fn interfaces(self) -> Vec<String> {
        let (rails, start) = match self {
            Self::C1 => ([0, 1], 0),
            Self::C3 => ([1, 0], 4),
            Self::C5 => ([2, 3], 0),
            Self::C7 => ([3, 2], 4),
        };
        rails
            .into_iter()
            .flat_map(|rail| (start..start + 4).map(move |port| format!("eth_r{rail}_p{port}")))
            .collect()
    }
}

/// Release-specific safety policy, supplied with the proposed firmware image.
/// There is deliberately no default approved starting version or bypass mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkxPolicy {
    pub current_version: String,
    pub target_version: String,
    /// SHA-256 of the exact firmware payload, verified locally and after transfer.
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct LinkxFirmwareTarget {
    pub connectors: Vec<LinkxConnector>,
    pub firmware_file: String,
    pub policy: LinkxPolicy,
}

#[derive(Debug, Clone)]
pub struct LinkxUpdateRequest {
    pub targets: Vec<LinkxFirmwareTarget>,
    pub cancellation: Option<CancellationToken>,
}

/// Firmware state is separate from link health: TARGET does not imply links up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LinkxVersionState {
    Target,
    NotTarget,
    MixedVersion,
    PartialRead,
    Unreadable,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkxEvidence {
    /// Last observed values; a timed-out inspection can retain earlier observations.
    pub versions: [Option<String>; 2],
    pub version_state: LinkxVersionState,
    pub links_up: usize,
    pub module_health_ok: bool,
    pub query_errors: Vec<LinkxQueryError>,
    /// Partial or retained observations must not be used to declare recovery complete.
    pub inspection_timed_out: bool,
}

/// A failed inspection command, including its device/interface path and sanitized cause.
#[derive(Debug, Clone, Serialize)]
pub struct LinkxQueryError {
    pub command: String,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkxDisposition {
    NotAttempted,
    Skipped,
    Completed,
    FlashFailed,
    PostcheckFailed,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkxConnectorResult {
    pub connector: LinkxConnector,
    pub status: LinkxDisposition,
    pub before: LinkxEvidence,
    pub after: Option<LinkxEvidence>,
    pub error: Option<String>,
}

/// Once a flash starts, return evidence rather than a bare cancellation error.
/// A failed command may have changed firmware; it must never be blindly retried.
#[derive(Debug, Clone, Serialize)]
pub struct LinkxUpdateReport {
    pub completed: bool,
    pub cancellation_requested: bool,
    pub connectors: Vec<LinkxConnectorResult>,
}

fn linkx_invalid(message: impl Into<String>) -> NvFwUpdError {
    NvFwUpdError::IncompatibleFirmware {
        component: "LINKX".to_owned(),
        message: message.into(),
    }
}

impl LinkxPolicy {
    pub fn validate(&self) -> Result<()> {
        let version_ok = |v: &str| {
            !v.is_empty()
                && v != "00.00.0000"
                && v.split('.').count() >= 3
                && v.split('.')
                    .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
        };
        if !version_ok(&self.current_version) || !version_ok(&self.target_version) {
            return Err(linkx_invalid(
                "LinkX policy requires explicit numeric current_version and target_version",
            ));
        }
        if self.sha256.len() != 64 || !self.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(linkx_invalid("LinkX policy requires a 64-digit sha256"));
        }
        Ok(())
    }
}

/// Audit both module paths without flashing, starting MST, or cycling power.
/// Unreadable firmware and link health are reported independently in the evidence.
pub async fn inspect_linkx_firmware(
    host: HostTargetConfig,
    connectors: &[LinkxConnector],
    target_version: &str,
) -> Result<Vec<(LinkxConnector, LinkxEvidence)>> {
    if host.ssh_host_key_mode == SshHostKeyMode::Disabled {
        return Err(linkx_invalid(
            "LinkX requires SSH host-key verification (TOFU or Strict)",
        ));
    }
    let access = os_access_from_config(&host)?;
    run_linkx_query(&access, "command -v mlxlink", false).await?;
    let mut evidence = Vec::new();
    for &connector in connectors {
        let result = inspect_linkx_connector(&access, connector, target_version).await;
        evidence.push((connector, result));
    }
    Ok(evidence)
}

/// Update selected VRNVL72 connectors sequentially after whole-request preflight.
///
/// # Errors
/// Fails before flashing on invalid policy, missing tools, inaccessible SSH,
/// checksum/type/version mismatch, or an unhealthy selected connector.
/// After the first flash starts, inspect the returned report's `completed` flag.
pub async fn update_linkx_firmware(
    host: HostTargetConfig,
    request: LinkxUpdateRequest,
) -> Result<LinkxUpdateReport> {
    if host.ssh_host_key_mode == SshHostKeyMode::Disabled {
        return Err(linkx_invalid(
            "LinkX requires SSH host-key verification (TOFU or Strict)",
        ));
    }
    let access = os_access_from_config(&host)?;
    update_linkx_with_access(&access, request, LINKX_RECOVERY_TIMEOUT).await
}

async fn run_linkx_query<A: FlintHostAccess>(
    access: &A,
    command: &str,
    sudo: bool,
) -> Result<String> {
    let output = access.execute(command, QUERY_TIMEOUT_SECS, sudo).await;
    if !output.success {
        return Err(command_error(
            access.target(),
            "LinkX query",
            output,
            false,
            QUERY_TIMEOUT_SECS,
        ));
    }
    Ok(output.stdout)
}

async fn verify_local_linkx_image(target: &LinkxFirmwareTarget) -> Result<()> {
    target.policy.validate()?;
    let mut file = tokio::fs::File::open(&target.firmware_file)
        .await
        .map_err(|e| linkx_invalid(format!("cannot open LinkX image: {e}")))?;
    let mut hash = Sha256::new();
    let mut buf = [0_u8; 65536];
    loop {
        let size = file
            .read(&mut buf)
            .await
            .map_err(|e| linkx_invalid(format!("cannot read LinkX image: {e}")))?;
        if size == 0 {
            break;
        }
        hash.update(&buf[..size]);
    }
    if !format!("{:x}", hash.finalize()).eq_ignore_ascii_case(&target.policy.sha256) {
        return Err(linkx_invalid(
            "local LinkX image sha256 does not match the rollout policy",
        ));
    }
    Ok(())
}

async fn update_linkx_with_access<A: FlintHostAccess>(
    access: &A,
    request: LinkxUpdateRequest,
    recovery_timeout: Duration,
) -> Result<LinkxUpdateReport> {
    check_cancelled(request.cancellation.as_ref(), "LinkX preflight")?;
    let mut selected = BTreeSet::new();
    if request.targets.is_empty() {
        return Err(linkx_invalid("select at least one LinkX connector"));
    }
    for target in &request.targets {
        if target.connectors.is_empty() {
            return Err(linkx_invalid(
                "select at least one LinkX connector per image",
            ));
        }
        for connector in &target.connectors {
            if !selected.insert(*connector) {
                return Err(linkx_invalid("overlapping LinkX connector selections"));
            }
        }
        verify_local_linkx_image(target).await?;
    }
    check_cancelled(request.cancellation.as_ref(), "LinkX host preparation")?;
    // An earlier OOB stage may have just powered the tray on. PowerState=On
    // does not mean SSH/the OS is ready; only wait here, never cycle the tray.
    wait_for_host_os_ready_with_access(access, request.cancellation.as_ref())
        .await
        .map_err(|error| match error {
            // Keep the existing workflow's diagnostics unchanged; only LinkX
            // needs a host label that does not name GB200.
            NvFwUpdError::Timeout {
                operation: "wait for GB200 host OS boot",
                seconds,
            } => NvFwUpdError::Timeout {
                operation: "wait for host OS boot",
                seconds,
            },
            NvFwUpdError::Cancelled {
                operation: "wait for GB200 host OS boot",
            } => NvFwUpdError::Cancelled {
                operation: "wait for host OS boot",
            },
            other => other,
        })?;
    check_cancelled(request.cancellation.as_ref(), "LinkX host preparation")?;
    let tools = access
        .execute(
            "command -v mst && command -v flint && command -v mlxlink && command -v sha256sum",
            QUERY_TIMEOUT_SECS,
            false,
        )
        .await;
    if !tools.success {
        return Err(command_error(
            access.target(),
            "mst/flint/mlxlink/sha256sum",
            tools,
            true,
            QUERY_TIMEOUT_SECS,
        ));
    }
    run_linkx_query(access, "mst start", true).await?;
    let remote_dir = create_remote_temp_dir(access, QUERY_TIMEOUT_SECS).await?;
    // This stricter staging policy is LinkX-only. Do not change the shared
    // helper's path acceptance for existing Flint CLI/library callers.
    if !remote_dir[REMOTE_TEMP_PREFIX.len()..]
        .chars()
        .all(|c| c.is_ascii_alphanumeric())
    {
        return Err(NvFwUpdError::InvalidResponse {
            context: "remote temporary directory",
            message: "mktemp returned an unexpected path".to_owned(),
        });
    }
    let result = staged_linkx_update(access, &request, &remote_dir, recovery_timeout).await;
    // Always clean on success, preflight failure, cancellation, or partial update.
    cleanup_remote_temp_dir(access, &remote_dir, QUERY_TIMEOUT_SECS).await;
    result
}

struct PlannedLinkxConnector {
    connector: LinkxConnector,
    remote_image: String,
    target_version: String,
    before: LinkxEvidence,
}

async fn staged_linkx_update<A: FlintHostAccess>(
    access: &A,
    request: &LinkxUpdateRequest,
    remote_dir: &str,
    recovery_timeout: Duration,
) -> Result<LinkxUpdateReport> {
    let mut plan = Vec::new();
    for (index, target) in request.targets.iter().enumerate() {
        check_cancelled(request.cancellation.as_ref(), "LinkX image preflight")?;
        let remote_image = format!("{remote_dir}/image-{index}.bin");
        let upload = access.upload(&target.firmware_file, &remote_image).await;
        if !upload.success {
            return Err(command_error(
                access.target(),
                "LinkX image upload",
                upload,
                false,
                300,
            ));
        }
        let digest = run_linkx_query(
            access,
            &format!("sha256sum -- {}", shell_quote(&remote_image)),
            false,
        )
        .await?;
        if !digest
            .split_whitespace()
            .next()
            .is_some_and(|hash| hash.eq_ignore_ascii_case(&target.policy.sha256))
        {
            return Err(linkx_invalid(
                "remote LinkX image sha256 does not match the rollout policy",
            ));
        }
        let image = run_linkx_query(
            access,
            &format!("flint -i {} q", shell_quote(&remote_image)),
            true,
        )
        .await?;
        if linkx_field(&image, "Image type").as_deref() != Some("LINKX")
            || linkx_field(&image, "FW Version").as_deref()
                != Some(target.policy.target_version.as_str())
        {
            return Err(linkx_invalid(
                "Flint image type/version must match LINKX and the approved target version",
            ));
        }
        for &connector in &target.connectors {
            check_cancelled(request.cancellation.as_ref(), "LinkX connector preflight")?;
            for path in [connector.device()]
                .into_iter()
                .chain(connector.module_paths())
            {
                run_linkx_query(access, &format!("test -e {}", shell_quote(&path)), false).await?;
            }
            let before =
                inspect_linkx_connector(access, connector, &target.policy.target_version).await;
            if before.inspection_timed_out || !before.query_errors.is_empty() {
                return Err(NvFwUpdError::TaskFailed {
                    task_id: None,
                    message: format!(
                        "LinkX {connector:?} preflight inspection failed: {}",
                        json!(before)
                    ),
                });
            }
            if before.version_state != LinkxVersionState::Target {
                let component = run_linkx_query(
                    access,
                    &format!(
                        "flint -d {} --linkx --downstream_device_ids 1 q",
                        shell_quote(&connector.device())
                    ),
                    true,
                )
                .await?;
                validate_linkx_preflight(&component, &before, &target.policy)?;
            }
            plan.push(PlannedLinkxConnector {
                connector,
                remote_image: remote_image.clone(),
                target_version: target.policy.target_version.clone(),
                before,
            });
        }
    }
    plan.sort_by_key(|p| p.connector);
    check_cancelled(request.cancellation.as_ref(), "LinkX before flashing")?;
    let mut report = LinkxUpdateReport {
        completed: false,
        cancellation_requested: false,
        connectors: plan
            .iter()
            .map(|p| LinkxConnectorResult {
                connector: p.connector,
                status: if p.before.version_state == LinkxVersionState::Target {
                    LinkxDisposition::Skipped
                } else {
                    LinkxDisposition::NotAttempted
                },
                before: p.before.clone(),
                after: None,
                error: None,
            })
            .collect(),
    };
    let mut flash_started = false;
    for (p, result) in plan.iter().zip(&mut report.connectors) {
        if result.status == LinkxDisposition::Skipped {
            continue;
        }
        if request
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            if !flash_started {
                return Err(NvFwUpdError::Cancelled {
                    operation: "LinkX before flashing",
                });
            }
            break;
        }
        flash_started = true;
        // Deliberately no cancellation select or automatic retry around a flash.
        let command = format!("flint -d {} --linkx --downstream_device_id_start_index 1 --num_of_downstream_devices 1 --download_transfer --activate -i {} b", shell_quote(&p.connector.device()), shell_quote(&p.remote_image));
        let output = access.execute(&command, FLASH_TIMEOUT_SECS, true).await;
        if !output.success {
            result.status = LinkxDisposition::FlashFailed;
            result.error = Some(
                command_error(
                    access.target(),
                    "LinkX flash (firmware may have changed)",
                    output,
                    false,
                    FLASH_TIMEOUT_SECS,
                )
                .to_string(),
            );
            result.after =
                Some(inspect_linkx_connector(access, p.connector, &p.target_version).await);
            break;
        }
        let after =
            wait_for_linkx_recovery(access, p.connector, &p.target_version, recovery_timeout).await;
        let healthy = !after.inspection_timed_out
            && after.query_errors.is_empty()
            && after.version_state == LinkxVersionState::Target
            && after.links_up == 8
            && after.module_health_ok;
        result.after = Some(after);
        result.status = if healthy {
            LinkxDisposition::Completed
        } else {
            LinkxDisposition::PostcheckFailed
        };
        if !healthy {
            result.error = Some("LinkX flash finished but dual-path version/module health/8-link recovery was not confirmed; no further connectors flashed".to_owned());
            break;
        }
    }
    report.completed = report.connectors.iter().all(|r| {
        matches!(
            r.status,
            LinkxDisposition::Completed | LinkxDisposition::Skipped
        )
    });
    report.cancellation_requested = request
        .cancellation
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled);
    Ok(report)
}

/// Read a unique key; duplicated/missing fields fail closed. Strip MFT ANSI colors.
fn linkx_field(text: &str, key: &str) -> Option<String> {
    let ansi = regex::Regex::new(r"\x1b\[[0-9;]*m").expect("static ANSI pattern");
    let plain = ansi.replace_all(text, "");
    let mut values = plain.lines().filter_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == key).then(|| value.trim().to_owned())
    });
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

fn validate_linkx_preflight(
    component: &str,
    evidence: &LinkxEvidence,
    policy: &LinkxPolicy,
) -> Result<()> {
    let approved = ["FW A Version", "FW B Version"]
        .iter()
        .any(|key| linkx_field(component, key).as_deref() == Some(policy.current_version.as_str()));
    let idle = component.lines().any(|line| {
        line.replace(':', " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            == ["Component", "Update", "State", "IDLE"]
    });
    if !approved
        || !idle
        || !evidence.module_health_ok
        || evidence.links_up != 8
        || matches!(
            evidence.version_state,
            LinkxVersionState::PartialRead | LinkxVersionState::Unreadable
        )
    {
        return Err(linkx_invalid("LinkX preflight requires approved current firmware, IDLE component, two readable healthy Ready module paths, and all eight links up"));
    }
    Ok(())
}

fn linkx_version_state(versions: &[Option<String>; 2], target: &str) -> LinkxVersionState {
    match (&versions[0], &versions[1]) {
        (Some(a), Some(b)) if a == target && b == target => LinkxVersionState::Target,
        (Some(a), Some(b)) if a == b => LinkxVersionState::NotTarget,
        (Some(_), Some(_)) => LinkxVersionState::MixedVersion,
        (None, None) => LinkxVersionState::Unreadable,
        _ => LinkxVersionState::PartialRead,
    }
}

async fn inspect_linkx_connector<A: FlintHostAccess>(
    access: &A,
    connector: LinkxConnector,
    target: &str,
) -> LinkxEvidence {
    inspect_linkx_connector_with_deadline(
        access,
        connector,
        target,
        tokio::time::Instant::now() + LINKX_RECOVERY_TIMEOUT,
        &mut LinkxInspection::default(),
    )
    .await
}

/// Updated after each query, outside the timed future, so a deadline never
/// erases observations from completed queries or the preceding recovery probe.
#[derive(Default)]
struct LinkxInspection {
    versions: [Option<String>; 2],
    module_health: [bool; 2],
    links: [bool; 8],
    query_errors: Vec<LinkxQueryError>,
    pending_command: Option<String>,
}

impl LinkxInspection {
    fn record_error(&mut self, command: String, error: String) {
        tracing::warn!(command, error, "LinkX inspection query failed");
        self.query_errors.push(LinkxQueryError { command, error });
    }

    async fn query<A: FlintHostAccess>(
        &mut self,
        access: &A,
        command: String,
        sudo: bool,
    ) -> Option<String> {
        self.pending_command = Some(command.clone());
        let result = run_linkx_query(access, &command, sudo).await;
        self.pending_command = None;
        match result {
            Ok(text) => Some(text),
            Err(error) => {
                self.record_error(command, error.to_string());
                None
            }
        }
    }
}

async fn collect_linkx_connector_evidence<A: FlintHostAccess>(
    access: &A,
    connector: LinkxConnector,
    inspection: &mut LinkxInspection,
) {
    for (index, path) in connector.module_paths().iter().enumerate() {
        let text = inspection
            .query(access, format!("mlxlink -d {} -m", shell_quote(path)), true)
            .await;
        inspection.versions[index] = text
            .as_deref()
            .and_then(|text| linkx_field(text, "FW Version"))
            .filter(|v| v != "N/A" && !v.is_empty() && v != "00.00.0000");
        inspection.module_health[index] = text.as_deref().is_some_and(|text| {
            linkx_field(text, "Module FW Fault").as_deref() == Some("0")
                && linkx_field(text, "DataPath FW Fault").as_deref() == Some("0")
                && linkx_field(text, "Module State").as_deref() == Some("Ready state")
        });
    }
    for (index, interface) in connector.interfaces().iter().enumerate() {
        inspection.links[index] = inspection
            .query(
                access,
                format!("cat /sys/class/net/{interface}/operstate"),
                false,
            )
            .await
            .is_some_and(|state| state.trim() == "up");
    }
}

async fn inspect_linkx_connector_with_deadline<A: FlintHostAccess>(
    access: &A,
    connector: LinkxConnector,
    target: &str,
    deadline: tokio::time::Instant,
    inspection: &mut LinkxInspection,
) -> LinkxEvidence {
    inspection.query_errors.clear();
    let inspection_timed_out = tokio::time::timeout_at(
        deadline,
        collect_linkx_connector_evidence(access, connector, inspection),
    )
    .await
    .is_err();
    if inspection_timed_out {
        let command = inspection.pending_command.take().unwrap_or_default();
        inspection.record_error(
            command,
            "LinkX inspection deadline exceeded; completed observations retained".to_owned(),
        );
    }
    LinkxEvidence {
        version_state: linkx_version_state(&inspection.versions, target),
        versions: inspection.versions.clone(),
        links_up: inspection.links.iter().filter(|&&up| up).count(),
        module_health_ok: inspection.module_health.iter().all(|&healthy| healthy),
        query_errors: inspection.query_errors.clone(),
        inspection_timed_out,
    }
}

async fn wait_for_linkx_recovery<A: FlintHostAccess>(
    access: &A,
    connector: LinkxConnector,
    target: &str,
    timeout: Duration,
) -> LinkxEvidence {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut inspection = LinkxInspection::default();
    loop {
        // Bound the whole inspection too: ten sequential SSH queries must not
        // extend the advertised recovery window by ten command timeouts.
        let evidence = inspect_linkx_connector_with_deadline(
            access,
            connector,
            target,
            deadline,
            &mut inspection,
        )
        .await;
        if evidence.inspection_timed_out
            || (evidence.query_errors.is_empty()
                && evidence.version_state == LinkxVersionState::Target
                && evidence.links_up == 8
                && evidence.module_health_ok)
            || tokio::time::Instant::now() + LINKX_POLL_INTERVAL >= deadline
        {
            return evidence;
        }
        tokio::time::sleep(LINKX_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    mod linkx {
        use super::*;
        use async_trait::async_trait;
        use std::collections::VecDeque;
        use std::sync::{Arc, Mutex};

        const OLD: &str = "44.40.2019";
        const NEW: &str = "44.40.2035";
        const IMAGE: &[u8] = b"test LinkX payload";

        #[derive(Default)]
        struct State {
            commands: Vec<String>,
            flashed: BTreeSet<usize>,
            fault_device: Option<usize>,
            flash_failure: bool,
            degraded_after_flash: bool,
            current: bool,
            wrong_image: bool,
            missing_tools: bool,
            wrong_hash: bool,
            temp_dir_output: Option<String>,
            cancel_on_flash: Option<CancellationToken>,
            boot_probes: VecDeque<CommandOutput>,
            cancel_on_boot_probe: Option<CancellationToken>,
            query_failures: Vec<(String, String)>,
            query_failures_after_flash: bool,
            stalled_query_after_flash: Option<String>,
            successful_queries_before_stall: usize,
        }

        #[derive(Clone, Default)]
        struct Mock(Arc<Mutex<State>>);

        fn ok(stdout: impl Into<String>) -> CommandOutput {
            CommandOutput {
                success: true,
                stdout: stdout.into(),
                stderr: String::new(),
            }
        }

        fn digest() -> String {
            format!("{:x}", Sha256::digest(IMAGE))
        }

        #[async_trait]
        impl FlintHostAccess for Mock {
            fn target(&self) -> &str {
                "test-host"
            }

            async fn execute(&self, command: &str, _timeout: u64, _sudo: bool) -> CommandOutput {
                let stall = {
                    let mut state = self.0.lock().unwrap();
                    state.commands.push(command.to_owned());
                    if !state.flashed.is_empty()
                        && state.stalled_query_after_flash.as_deref() == Some(command)
                    {
                        if state.successful_queries_before_stall == 0 {
                            true
                        } else {
                            state.successful_queries_before_stall -= 1;
                            false
                        }
                    } else {
                        false
                    }
                };
                if stall {
                    return std::future::pending().await;
                }
                let mut state = self.0.lock().unwrap();
                if command == HOST_OS_READY_COMMAND {
                    if let Some(cancel) = &state.cancel_on_boot_probe {
                        cancel.cancel();
                    }
                    return state.boot_probes.pop_front().unwrap_or_else(|| ok(""));
                }
                if !state.query_failures_after_flash || !state.flashed.is_empty() {
                    if let Some((_, error)) = state
                        .query_failures
                        .iter()
                        .find(|(query, _)| query == command)
                    {
                        return failure(error);
                    }
                }
                if command.starts_with("command -v") && state.missing_tools {
                    return CommandOutput {
                        success: false,
                        stdout: String::new(),
                        stderr: "flint not found".to_owned(),
                    };
                }
                if command.starts_with("mktemp") {
                    return ok(state
                        .temp_dir_output
                        .as_deref()
                        .unwrap_or("/tmp/nvfwupd-flint.ABC123\n"));
                }
                if command.starts_with("sha256sum") {
                    return ok(if state.wrong_hash {
                        "0".repeat(64)
                    } else {
                        digest()
                    });
                }
                if command.starts_with("flint -i") {
                    return ok(format!(
                        "Image type: {}\nFW Version: {NEW}\n",
                        if state.wrong_image { "FS4" } else { "LINKX" }
                    ));
                }
                let index = command
                    .split("pciconf")
                    .nth(1)
                    .and_then(|s| s.chars().next())
                    .and_then(|c| c.to_digit(10))
                    .map(|i| i as usize);
                if command.contains("--download_transfer") {
                    assert!(command.contains("--activate"));
                    assert!(command.contains(
                        "--downstream_device_id_start_index 1 --num_of_downstream_devices 1"
                    ));
                    state.flashed.insert(index.unwrap());
                    if let Some(cancel) = &state.cancel_on_flash {
                        cancel.cancel();
                    }
                    return CommandOutput {
                        success: !state.flash_failure,
                        stdout: String::new(),
                        stderr: if state.flash_failure {
                            "flash failed".to_owned()
                        } else {
                            String::new()
                        },
                    };
                }
                if command.contains("--downstream_device_ids 1 q") {
                    return ok(format!("FW A Version : {OLD}\nFW B Version : 00.00.0000\nComponent Update State IDLE\n"));
                }
                if command.starts_with("mlxlink") {
                    let index = index.unwrap();
                    let programmed = state.current || state.flashed.contains(&(index | 1));
                    let fault = state.fault_device == Some(index);
                    return ok(format!("FW Version : {}\nModule FW Fault : {}\nDataPath FW Fault : 0\nModule State : Ready state\n", if programmed { NEW } else { OLD }, if fault { 1 } else { 0 }));
                }
                if command.starts_with("cat /sys") {
                    return ok(if state.degraded_after_flash && !state.flashed.is_empty() {
                        "down"
                    } else {
                        "up"
                    });
                }
                ok("")
            }

            async fn upload(&self, _: &str, remote: &str) -> CommandOutput {
                self.0
                    .lock()
                    .unwrap()
                    .commands
                    .push(format!("upload {remote}"));
                ok("")
            }
        }

        fn request(
            file: &tempfile::NamedTempFile,
            connectors: Vec<LinkxConnector>,
        ) -> LinkxUpdateRequest {
            LinkxUpdateRequest {
                targets: vec![LinkxFirmwareTarget {
                    connectors,
                    firmware_file: file.path().to_str().unwrap().to_owned(),
                    policy: LinkxPolicy {
                        current_version: OLD.to_owned(),
                        target_version: NEW.to_owned(),
                        sha256: digest(),
                    },
                }],
                cancellation: None,
            }
        }

        fn image() -> tempfile::NamedTempFile {
            use std::io::Write;
            let mut file = tempfile::NamedTempFile::new().unwrap();
            file.write_all(IMAGE).unwrap();
            file
        }

        #[tokio::test]
        async fn linkx_rejects_unexpected_temp_paths_before_upload_or_flash() {
            let file = image();
            for path in [
                "/tmp/nvfwupd-flint.ABC-123",
                "/tmp/nvfwupd-flint.ABC_123",
                "/tmp/nvfwupd-flint.ABC/../other",
                "/tmp/nvfwupd-flint.ABC 123",
                "/tmp/nvfwupd-flint.",
                "/tmp/other.ABC123",
            ] {
                let mock = Mock::default();
                mock.0.lock().unwrap().temp_dir_output = Some(path.to_owned());
                let error = update_linkx_with_access(
                    &mock,
                    request(&file, vec![LinkxConnector::C1]),
                    Duration::ZERO,
                )
                .await
                .unwrap_err();
                assert!(matches!(
                    error,
                    NvFwUpdError::InvalidResponse {
                        context: "remote temporary directory",
                        ..
                    }
                ));
                let state = mock.0.lock().unwrap();
                assert!(state.flashed.is_empty());
                assert!(!state.commands.iter().any(|command| {
                    command.starts_with("upload ") || command.starts_with("rm ")
                }));
            }
        }

        #[test]
        fn connector_mapping_matches_reference_and_covers_32_links_once() {
            let mut interfaces = BTreeSet::new();
            for (connector, device) in LinkxConnector::ALL.into_iter().zip([1, 3, 7, 5]) {
                assert_eq!(
                    connector.device(),
                    format!("/dev/mst/mt4133_pciconf{device}")
                );
                assert_eq!(
                    connector.module_paths(),
                    [
                        format!("/dev/mst/mt4133_pciconf{}.3", device - 1),
                        format!("/dev/mst/mt4133_pciconf{device}.3")
                    ]
                );
                assert_eq!(connector.interfaces().len(), 8);
                interfaces.extend(connector.interfaces());
            }
            assert_eq!(interfaces.len(), 32);
            assert_eq!(LinkxConnector::C5.interfaces()[0], "eth_r2_p0");
            assert_eq!(LinkxConnector::C7.interfaces()[0], "eth_r3_p4");
        }

        #[test]
        fn versions_require_two_independent_reads() {
            assert_eq!(
                linkx_version_state(&[Some(NEW.into()), Some(NEW.into())], NEW),
                LinkxVersionState::Target
            );
            assert_eq!(
                linkx_version_state(&[Some(OLD.into()), Some(OLD.into())], NEW),
                LinkxVersionState::NotTarget
            );
            assert_eq!(
                linkx_version_state(&[Some(OLD.into()), Some(NEW.into())], NEW),
                LinkxVersionState::MixedVersion
            );
            assert_eq!(
                linkx_version_state(&[Some(NEW.into()), None], NEW),
                LinkxVersionState::PartialRead
            );
            assert_eq!(
                linkx_version_state(&[None, None], NEW),
                LinkxVersionState::Unreadable
            );
            assert_eq!(
                linkx_field("\x1b[32mFW Version : 1.2.3\x1b[0m", "FW Version"),
                Some("1.2.3".into())
            );
            assert_eq!(
                linkx_field("FW Version : 1.2.3\nFW Version : 1.2.3", "FW Version"),
                None
            );
        }

        #[test]
        fn preflight_rejects_unapproved_version_busy_faulted_and_unreadable_modules() {
            let policy = LinkxPolicy {
                current_version: OLD.into(),
                target_version: NEW.into(),
                sha256: digest(),
            };
            let component = format!(
                "FW A Version : {OLD}\nFW B Version : 00.00.0000\nComponent Update State IDLE"
            );
            let healthy = LinkxEvidence {
                versions: [Some(OLD.into()), Some(OLD.into())],
                version_state: LinkxVersionState::NotTarget,
                links_up: 8,
                module_health_ok: true,
                query_errors: Vec::new(),
                inspection_timed_out: false,
            };
            assert!(validate_linkx_preflight(&component, &healthy, &policy).is_ok());
            assert!(validate_linkx_preflight(
                &component.replace(OLD, "44.39.1000"),
                &healthy,
                &policy
            )
            .is_err());
            assert!(validate_linkx_preflight(
                &component.replace("IDLE", "BUSY"),
                &healthy,
                &policy
            )
            .is_err());
            assert!(validate_linkx_preflight(
                &component,
                &LinkxEvidence {
                    module_health_ok: false,
                    ..healthy.clone()
                },
                &policy
            )
            .is_err());
            assert!(validate_linkx_preflight(
                &component,
                &LinkxEvidence {
                    links_up: 7,
                    ..healthy.clone()
                },
                &policy
            )
            .is_err());
            assert!(validate_linkx_preflight(
                &component,
                &LinkxEvidence {
                    version_state: LinkxVersionState::PartialRead,
                    ..healthy
                },
                &policy
            )
            .is_err());
        }

        #[tokio::test]
        async fn overlapping_connectors_and_invalid_policy_fail_before_remote_io() {
            let file = image();
            let mock = Mock::default();
            let req = request(&file, vec![LinkxConnector::C1, LinkxConnector::C1]);
            assert!(update_linkx_with_access(&mock, req, Duration::ZERO)
                .await
                .is_err());
            let mut req = request(&file, vec![LinkxConnector::C1]);
            req.targets[0].policy.current_version.clear();
            assert!(update_linkx_with_access(&mock, req, Duration::ZERO)
                .await
                .is_err());
            assert!(mock.0.lock().unwrap().commands.is_empty());
        }

        #[tokio::test]
        async fn flashes_sequentially_after_all_preflight_and_cleans() {
            let file = image();
            let mock = Mock::default();
            let report = update_linkx_with_access(
                &mock,
                request(&file, LinkxConnector::ALL.to_vec()),
                Duration::from_millis(10),
            )
            .await
            .unwrap();
            assert!(report.completed);
            assert!(report
                .connectors
                .iter()
                .all(|r| r.status == LinkxDisposition::Completed));
            let state = mock.0.lock().unwrap();
            let flash_start = state
                .commands
                .iter()
                .position(|c| c.contains("--download_transfer"))
                .unwrap();
            assert_eq!(
                state.commands[..flash_start]
                    .iter()
                    .filter(|c| c.contains("--downstream_device_ids 1 q"))
                    .count(),
                4
            );
            let flashes: Vec<_> = state
                .commands
                .iter()
                .filter(|c| c.contains("--download_transfer"))
                .collect();
            for (cmd, device) in flashes.iter().zip([1, 3, 7, 5]) {
                assert!(cmd.contains(&format!("pciconf{device}'")));
            }
            assert!(state
                .commands
                .last()
                .unwrap()
                .starts_with("rm -rf -- '/tmp/nvfwupd-flint."));
        }

        #[tokio::test]
        async fn last_connector_preflight_failure_prevents_every_flash() {
            let file = image();
            let mock = Mock::default();
            mock.0.lock().unwrap().fault_device = Some(4);
            assert!(update_linkx_with_access(
                &mock,
                request(&file, LinkxConnector::ALL.to_vec()),
                Duration::from_millis(5)
            )
            .await
            .is_err());
            let state = mock.0.lock().unwrap();
            assert!(state.flashed.is_empty());
            assert!(state.commands.last().unwrap().starts_with("rm -rf"));
        }

        #[tokio::test]
        async fn current_connectors_skip_and_missing_tools_fail_before_staging() {
            let file = image();
            let mock = Mock::default();
            mock.0.lock().unwrap().current = true;
            let report = update_linkx_with_access(
                &mock,
                request(&file, LinkxConnector::ALL.to_vec()),
                Duration::from_millis(5),
            )
            .await
            .unwrap();
            assert!(report.completed);
            assert!(report
                .connectors
                .iter()
                .all(|r| r.status == LinkxDisposition::Skipped));
            assert!(mock.0.lock().unwrap().flashed.is_empty());
            let mock = Mock::default();
            mock.0.lock().unwrap().missing_tools = true;
            assert!(matches!(
                update_linkx_with_access(
                    &mock,
                    request(&file, vec![LinkxConnector::C1]),
                    Duration::ZERO
                )
                .await,
                Err(NvFwUpdError::HostToolUnavailable { .. })
            ));
            assert!(!mock
                .0
                .lock()
                .unwrap()
                .commands
                .iter()
                .any(|c| c.starts_with("mktemp")));
        }

        #[tokio::test]
        async fn waits_for_ssh_and_os_after_oob_before_linkx_host_preparation() {
            let file = image();
            let mock = Mock::default();
            mock.0.lock().unwrap().boot_probes = VecDeque::from([
                failure("connection refused"),
                CommandOutput {
                    success: false,
                    stdout: "system-state:starting".into(),
                    stderr: String::new(),
                },
                ok(""),
            ]);
            let report = update_linkx_with_access(
                &mock,
                request(&file, vec![LinkxConnector::C1]),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

            assert!(report.completed);
            let state = mock.0.lock().unwrap();
            assert_eq!(&state.commands[..3], &[HOST_OS_READY_COMMAND; 3]);
            assert!(state.commands[3].starts_with("command -v mst"));
            assert_eq!(state.flashed.len(), 1);
        }

        #[tokio::test]
        async fn failed_or_cancelled_os_wait_never_starts_mst_staging_or_flash() {
            let file = image();
            for mode in ["timeout", "authentication", "cancelled"] {
                let mock = Mock::default();
                let token = CancellationToken::new();
                let mut req = request(&file, vec![LinkxConnector::C1]);
                req.cancellation = Some(token.clone());
                {
                    let mut state = mock.0.lock().unwrap();
                    match mode {
                        "timeout" => {
                            state.boot_probes = (0..HOST_OS_BOOT_ATTEMPTS)
                                .map(|_| failure("connection refused"))
                                .collect()
                        }
                        "authentication" => state
                            .boot_probes
                            .push_back(failure("authentication failed")),
                        "cancelled" => state.cancel_on_boot_probe = Some(token),
                        _ => unreachable!(),
                    }
                }
                let error = update_linkx_with_access(&mock, req, Duration::ZERO)
                    .await
                    .unwrap_err();
                match mode {
                    "timeout" => assert!(matches!(
                        error,
                        NvFwUpdError::Timeout {
                            operation: "wait for host OS boot",
                            seconds: HOST_OS_BOOT_TIMEOUT_SECS,
                        }
                    )),
                    "authentication" => assert!(matches!(error, NvFwUpdError::AuthFailed { .. })),
                    "cancelled" => assert!(matches!(error, NvFwUpdError::Cancelled { .. })),
                    _ => unreachable!(),
                }
                let state = mock.0.lock().unwrap();
                assert!(state
                    .commands
                    .iter()
                    .all(|command| command == HOST_OS_READY_COMMAND));
                assert_eq!(
                    state.commands.len(),
                    if mode == "timeout" {
                        HOST_OS_BOOT_ATTEMPTS as usize
                    } else {
                        1
                    }
                );
                assert!(state.flashed.is_empty());
            }
        }

        #[tokio::test]
        async fn query_errors_preserve_command_and_cause_and_prevent_flashing() {
            let file = image();
            let failures = [
                (
                    "mlxlink -d '/dev/mst/mt4133_pciconf0.3' -m",
                    "mlxlink: device query failed",
                ),
                (
                    "cat /sys/class/net/eth_r0_p0/operstate",
                    "connection refused",
                ),
            ];
            for (command, cause) in failures {
                let mock = Mock::default();
                mock.0
                    .lock()
                    .unwrap()
                    .query_failures
                    .push((command.into(), cause.into()));
                let evidence = inspect_linkx_connector(&mock, LinkxConnector::C1, NEW).await;
                assert_eq!(evidence.query_errors.len(), 1);
                assert_eq!(evidence.query_errors[0].command, command);
                assert!(evidence.query_errors[0].error.contains(cause));
                assert!(!evidence.inspection_timed_out);
                let error = update_linkx_with_access(
                    &mock,
                    request(&file, vec![LinkxConnector::C1]),
                    Duration::ZERO,
                )
                .await
                .unwrap_err();
                assert!(matches!(&error, NvFwUpdError::TaskFailed { .. }));
                assert!(error.to_string().contains(command));
                assert!(error.to_string().contains(cause));
                assert!(error.to_string().contains("C1 preflight"));
                let state = mock.0.lock().unwrap();
                assert!(state.flashed.is_empty());
                assert!(state.commands.last().unwrap().starts_with("rm -rf"));
            }
        }

        #[tokio::test]
        async fn postcheck_query_error_retains_partial_update_report_and_stops_next_connector() {
            let file = image();
            let mock = Mock::default();
            let command = "cat /sys/class/net/eth_r0_p0/operstate";
            {
                let mut state = mock.0.lock().unwrap();
                state
                    .query_failures
                    .push((command.into(), "connection refused".into()));
                state.query_failures_after_flash = true;
            }
            let report = update_linkx_with_access(
                &mock,
                request(&file, LinkxConnector::ALL.to_vec()),
                Duration::from_millis(10),
            )
            .await
            .unwrap();
            assert!(!report.completed);
            assert_eq!(
                report.connectors[0].status,
                LinkxDisposition::PostcheckFailed
            );
            let after = report.connectors[0].after.as_ref().unwrap();
            assert_eq!(after.version_state, LinkxVersionState::Target);
            assert_eq!(after.links_up, 7);
            assert!(after.query_errors.iter().any(
                |error| error.command == command && error.error.contains("connection refused")
            ));
            assert_eq!(report.connectors[1].status, LinkxDisposition::NotAttempted);
            assert_eq!(mock.0.lock().unwrap().flashed.len(), 1);
        }

        #[tokio::test]
        async fn postcheck_deadline_retains_versions_and_links_from_completed_queries() {
            let file = image();
            let mock = Mock::default();
            let command = "cat /sys/class/net/eth_r1_p3/operstate";
            mock.0.lock().unwrap().stalled_query_after_flash = Some(command.into());
            let report = update_linkx_with_access(
                &mock,
                request(&file, LinkxConnector::ALL.to_vec()),
                Duration::from_millis(250),
            )
            .await
            .unwrap();

            assert!(!report.completed);
            assert_eq!(
                report.connectors[0].status,
                LinkxDisposition::PostcheckFailed
            );
            let after = report.connectors[0].after.as_ref().unwrap();
            assert_eq!(after.versions, [Some(NEW.into()), Some(NEW.into())]);
            assert_eq!(after.version_state, LinkxVersionState::Target);
            assert_eq!(after.links_up, 7);
            assert!(after.inspection_timed_out);
            assert_eq!(after.query_errors[0].command, command);
            assert!(after.query_errors[0].error.contains("deadline exceeded"));
            assert_eq!(report.connectors[1].status, LinkxDisposition::NotAttempted);
            assert_eq!(mock.0.lock().unwrap().flashed.len(), 1);
        }

        #[tokio::test(start_paused = true)]
        async fn postcheck_deadline_retains_previous_probe_without_claiming_success() {
            let file = image();
            let mock = Mock::default();
            let command = "mlxlink -d '/dev/mst/mt4133_pciconf0.3' -m";

            {
                let mut state = mock.0.lock().unwrap();
                state.stalled_query_after_flash = Some(command.into());
                state.successful_queries_before_stall = 1;
                state.query_failures_after_flash = true;
                state.query_failures.push((
                    "cat /sys/class/net/eth_r0_p0/operstate".into(),
                    "connection refused".into(),
                ));
            }

            // The first recovery probe sees both versions and seven links.
            // The next probe times out before returning any new observations.
            // Paused time keeps runner scheduling from exhausting the deadline
            // before the second probe starts; Tokio advances it while waiting.
            let report = update_linkx_with_access(
                &mock,
                request(&file, LinkxConnector::ALL.to_vec()),
                Duration::from_millis(250),
            )
            .await
            .unwrap();

            assert!(!report.completed);
            assert_eq!(
                report.connectors[0].status,
                LinkxDisposition::PostcheckFailed
            );

            let evidence = report.connectors[0].after.as_ref().unwrap();

            assert_eq!(evidence.version_state, LinkxVersionState::Target);
            assert_eq!(evidence.links_up, 7);
            assert!(evidence.module_health_ok);
            assert!(evidence.inspection_timed_out);
            assert_eq!(evidence.query_errors[0].command, command);
            assert_eq!(report.connectors[1].status, LinkxDisposition::NotAttempted);
        }

        #[tokio::test]
        async fn failures_stop_without_retry_and_preserve_target_but_links_down() {
            let file = image();
            for failed_flash in [true, false] {
                let mock = Mock::default();
                {
                    let mut state = mock.0.lock().unwrap();
                    state.flash_failure = failed_flash;
                    state.degraded_after_flash = !failed_flash;
                }
                let report = update_linkx_with_access(
                    &mock,
                    request(&file, LinkxConnector::ALL.to_vec()),
                    Duration::from_millis(5),
                )
                .await
                .unwrap();
                assert!(!report.completed);
                assert_eq!(
                    report.connectors[0].status,
                    if failed_flash {
                        LinkxDisposition::FlashFailed
                    } else {
                        LinkxDisposition::PostcheckFailed
                    }
                );
                assert_eq!(
                    report.connectors[0].after.as_ref().unwrap().version_state,
                    LinkxVersionState::Target
                );
                assert_eq!(report.connectors[1].status, LinkxDisposition::NotAttempted);
                assert_eq!(mock.0.lock().unwrap().flashed.len(), 1);
            }
        }

        #[tokio::test]
        async fn cancellation_before_flash_is_error_after_flash_retains_evidence() {
            let file = image();
            let mock = Mock::default();
            let token = CancellationToken::new();
            let mut req = request(&file, LinkxConnector::ALL.to_vec());
            req.cancellation = Some(token.clone());
            token.cancel();
            assert!(matches!(
                update_linkx_with_access(&mock, req, Duration::ZERO).await,
                Err(NvFwUpdError::Cancelled { .. })
            ));
            assert!(mock.0.lock().unwrap().commands.is_empty());
            let token = CancellationToken::new();
            mock.0.lock().unwrap().cancel_on_flash = Some(token.clone());
            let mut req = request(&file, LinkxConnector::ALL.to_vec());
            req.cancellation = Some(token);
            let report = update_linkx_with_access(&mock, req, Duration::from_millis(10))
                .await
                .unwrap();
            assert!(report.cancellation_requested);
            assert!(!report.completed);
            assert_eq!(report.connectors[0].status, LinkxDisposition::Completed);
            assert_eq!(report.connectors[1].status, LinkxDisposition::NotAttempted);
        }

        #[tokio::test]
        async fn image_type_and_checksums_fail_closed_and_cleanup() {
            let file = image();
            for remote_hash in [true, false] {
                let mock = Mock::default();
                {
                    let mut state = mock.0.lock().unwrap();
                    state.wrong_hash = remote_hash;
                    state.wrong_image = !remote_hash;
                }
                assert!(update_linkx_with_access(
                    &mock,
                    request(&file, vec![LinkxConnector::C1]),
                    Duration::ZERO
                )
                .await
                .is_err());
                let state = mock.0.lock().unwrap();
                assert!(state.flashed.is_empty());
                assert!(state.commands.last().unwrap().starts_with("rm -rf"));
            }
            let mock = Mock::default();
            let mut req = request(&file, vec![LinkxConnector::C1]);
            req.targets[0].policy.sha256 = "0".repeat(64);
            assert!(update_linkx_with_access(&mock, req, Duration::ZERO)
                .await
                .is_err());
            assert!(mock.0.lock().unwrap().commands.is_empty());
        }
    }
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    fn mst_output() -> &'static str {
        "DEVICE_TYPE             MST                           PCI       RDMA\n\
         ConnectX7(rev:0)        /dev/mst/mt4129_pciconf0     01:00.0\n\
         ConnectX7(rev:0)        /dev/mst/mt4129_pciconf0.1   01:00.1\n\
         ConnectX8(rev:0)        /dev/mst/mt4133_pciconf1     02:00.0\n\
         BlueField3(rev:1)       /dev/mst/mt41692_pciconf0    03:00.0\n\
         Inband devices:\n\
         ConnectX7(rev:0)        /dev/mst/mt4129_pciconf9     09:00.0\n"
    }

    #[test]
    fn mst_parser_classifies_supported_physical_devices() {
        let devices = parse_mst_status(mst_output()).expect("valid MST output");

        assert_eq!(
            devices,
            vec![
                (
                    FlintDeviceFamily::ConnectX7,
                    "/dev/mst/mt4129_pciconf0".to_owned()
                ),
                (
                    FlintDeviceFamily::ConnectX8,
                    "/dev/mst/mt4133_pciconf1".to_owned()
                ),
                (
                    FlintDeviceFamily::BlueField3,
                    "/dev/mst/mt41692_pciconf0".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn mst_parser_rejects_malformed_output() {
        let error = parse_mst_status("no devices").expect_err("header is required");

        assert!(matches!(
            error,
            NvFwUpdError::InvalidResponse {
                context: "mst status",
                ..
            }
        ));
    }

    #[test]
    fn expected_device_counts_support_variable_physical_topologies() {
        let discovered = vec![
            (
                FlintDeviceFamily::ConnectX7,
                "/dev/mst/mt4129_pciconf0".to_owned(),
            ),
            (
                FlintDeviceFamily::ConnectX7,
                "/dev/mst/mt4129_pciconf1".to_owned(),
            ),
            (
                FlintDeviceFamily::BlueField3,
                "/dev/mst/mt41692_pciconf0".to_owned(),
            ),
        ];
        let expected = BTreeMap::from([
            (FlintDeviceFamily::ConnectX7, 2),
            (FlintDeviceFamily::ConnectX8, 0),
            (FlintDeviceFamily::BlueField3, 1),
        ]);

        validate_expected_device_counts(&expected, &discovered)
            .expect("profile counts should not assume four ConnectX devices");
    }

    #[test]
    fn expected_device_count_mismatch_reports_each_family() {
        let discovered = vec![(
            FlintDeviceFamily::ConnectX7,
            "/dev/mst/mt4129_pciconf0".to_owned(),
        )];
        let expected = BTreeMap::from([
            (FlintDeviceFamily::ConnectX7, 2),
            (FlintDeviceFamily::ConnectX8, 0),
            (FlintDeviceFamily::BlueField3, 1),
        ]);

        let error = validate_expected_device_counts(&expected, &discovered)
            .expect_err("missing physical devices must reject discovery");

        assert!(matches!(error, NvFwUpdError::IncompatibleFirmware { .. }));
        assert!(error.to_string().contains("CX7 expected 2, found 1"));
        assert!(error.to_string().contains("BF3_NIC expected 1, found 0"));
        assert!(!error.to_string().contains("CX8"));
    }

    #[test]
    fn equivalent_images_prefer_reduced_binary() {
        let images = vec![
            image("/tmp/fw.signed.pldm", "PSID-A", "1.2.3"),
            image("/tmp/fw.signed.reduced.bin", "PSID-A", "1.2.3"),
        ];

        let selected = select_equivalent_images(images).expect("equivalent images are valid");

        assert_eq!(selected.len(), 1);
        assert!(selected[0].local_path.ends_with(".signed.reduced.bin"));
    }

    #[test]
    fn equivalent_images_with_different_versions_are_rejected() {
        let images = vec![
            image("/tmp/old.bin", "PSID-A", "1.0"),
            image("/tmp/new.bin", "PSID-A", "2.0"),
        ];

        let error = select_equivalent_images(images).expect_err("versions are ambiguous");

        assert!(matches!(error, NvFwUpdError::IncompatibleFirmware { .. }));
    }

    #[test]
    fn equally_preferred_images_are_rejected_as_ambiguous() {
        let images = vec![
            image("/tmp/fw-a.bin", "PSID-A", "2.0"),
            image("/tmp/fw-b.bin", "PSID-A", "2.0"),
        ];

        let error = select_equivalent_images(images).expect_err("same-rank images are ambiguous");

        assert!(matches!(error, NvFwUpdError::IncompatibleFirmware { .. }));
    }

    #[test]
    fn update_plan_rejects_device_without_matching_psid() {
        let device = FlintDevice {
            family: FlintDeviceFamily::ConnectX7,
            path: "/dev/mst/cx7".to_owned(),
            psid: "PSID-B".to_owned(),
            version: "1.0".to_owned(),
        };

        let error = build_update_plan(
            &[target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            vec![device],
            vec![image("/tmp/fw.bin", "PSID-A", "2.0")],
        )
        .expect_err("unmatched PSID must fail before flashing");

        assert!(matches!(error, NvFwUpdError::IncompatibleFirmware { .. }));
    }

    #[derive(Clone)]
    struct FakeAccess {
        commands: Arc<Mutex<Vec<String>>>,
        command_timeouts: Arc<Mutex<Vec<u64>>>,
        responses: Arc<Mutex<VecDeque<CommandOutput>>>,
        cancel_on_flash: Option<CancellationToken>,
    }

    impl FakeAccess {
        fn new(responses: Vec<CommandOutput>) -> Self {
            Self {
                commands: Arc::new(Mutex::new(Vec::new())),
                command_timeouts: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(responses.into())),
                cancel_on_flash: None,
            }
        }

        fn cancelling_on_flash(mut self, cancellation: CancellationToken) -> Self {
            self.cancel_on_flash = Some(cancellation);
            self
        }
    }

    #[async_trait]
    impl FlintHostAccess for FakeAccess {
        fn target(&self) -> &str {
            "127.0.0.1"
        }

        async fn execute(
            &self,
            command: &str,
            timeout_secs: u64,
            _use_sudo: bool,
        ) -> CommandOutput {
            self.commands
                .lock()
                .expect("commands lock")
                .push(command.to_owned());
            self.command_timeouts
                .lock()
                .expect("command timeouts lock")
                .push(timeout_secs);
            if command.ends_with(" b") {
                if let Some(cancellation) = self.cancel_on_flash.as_ref() {
                    cancellation.cancel();
                }
            }
            self.responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .unwrap_or_else(|| success(""))
        }

        async fn upload(&self, local_path: &str, remote_path: &str) -> CommandOutput {
            self.commands
                .lock()
                .expect("commands lock")
                .push(format!("upload {local_path} {remote_path}"));
            success("")
        }
    }

    #[tokio::test]
    async fn temp_directory_validation_preserves_existing_flint_cli_behavior() {
        for (path, accepted) in [
            ("/tmp/nvfwupd-flint.ABC123\n", true),
            ("/tmp/nvfwupd-flint.ABC-123", true),
            ("/tmp/nvfwupd-flint.ABC_123", true),
            ("/tmp/nvfwupd-flint.ABC 123", false),
            ("/tmp/nvfwupd-flint.ABC\n123", false),
            ("/tmp/nvfwupd-flint.", false),
            ("/tmp/other.ABC123", false),
        ] {
            let access = FakeAccess::new(vec![success(path)]);
            let result = create_remote_temp_dir(&access, QUERY_TIMEOUT_SECS).await;
            if accepted {
                assert_eq!(result.unwrap(), path.trim());
            } else {
                assert!(matches!(
                    result,
                    Err(NvFwUpdError::InvalidResponse {
                        context: "remote temporary directory",
                        ..
                    })
                ));
            }
        }
    }

    struct FakeDiscoveryRecovery {
        commands: Arc<Mutex<Vec<String>>>,
        recoveries: usize,
    }

    #[async_trait]
    impl FlintDeviceDiscoveryRecovery for FakeDiscoveryRecovery {
        async fn recover(&mut self) -> Result<()> {
            assert!(
                !self
                    .commands
                    .lock()
                    .expect("commands lock")
                    .iter()
                    .any(|command| command.ends_with(" b")),
                "device-discovery recovery must run before any Flint flash"
            );
            self.recoveries += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn device_discovery_retries_once_after_recovery_before_flashing() {
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            success("mst started"),
            failure("enumeration unavailable"),
            success("mst restarted"),
            success(mst_output()),
            success("FW Version: 1.0\nPSID: PSID-A\n"),
            success("/tmp/nvfwupd-flint.ABC123\n"),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            success("flashed"),
            success(""),
        ]);
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: None,
            cancellation: None,
            expected_device_counts: Default::default(),
        };
        let mut recovery = FakeDiscoveryRecovery {
            commands: access.commands.clone(),
            recoveries: 0,
        };

        let outcome =
            update_firmware_with_access_and_recovery(access.clone(), request, Some(&mut recovery))
                .await
                .expect("second device-discovery attempt should allow the update");

        assert!(matches!(outcome, FirmwareUpdateOutcome::Completed(_)));
        assert_eq!(recovery.recoveries, 1);
        let commands = access.commands.lock().expect("commands lock");
        assert_eq!(
            commands
                .iter()
                .filter(|command| command.as_str() == "mst status -v")
                .count(),
            2
        );
        assert_eq!(
            commands
                .iter()
                .filter(|command| command.ends_with(" b"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn expected_count_mismatch_retries_once_before_flashing() {
        let two_cx7 = "DEVICE_TYPE             MST                           PCI       RDMA\n\
                       ConnectX7(rev:0)        /dev/mst/mt4129_pciconf0     01:00.0\n\
                       ConnectX7(rev:0)        /dev/mst/mt4129_pciconf1     02:00.0\n";
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            success("mst started"),
            success(mst_output()),
            success("mst restarted"),
            success(two_cx7),
            success("FW Version: 1.0\nPSID: PSID-A\n"),
            success("FW Version: 1.0\nPSID: PSID-A\n"),
            success("/tmp/nvfwupd-flint.ABC123\n"),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            success("flashed"),
            success("flashed"),
            success(""),
        ]);
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: None,
            cancellation: None,
            expected_device_counts: BTreeMap::from([(FlintDeviceFamily::ConnectX7, 2)]),
        };
        let mut recovery = FakeDiscoveryRecovery {
            commands: access.commands.clone(),
            recoveries: 0,
        };

        let outcome =
            update_firmware_with_access_and_recovery(access.clone(), request, Some(&mut recovery))
                .await
                .expect("matching inventory after recovery should allow both flashes");

        assert!(matches!(outcome, FirmwareUpdateOutcome::Completed(_)));
        assert_eq!(recovery.recoveries, 1);
        assert_eq!(
            access
                .commands
                .lock()
                .expect("commands lock")
                .iter()
                .filter(|command| command.ends_with(" b"))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn second_device_discovery_failure_is_not_recovered_again() {
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            failure("first mst start failure"),
            failure("second mst start failure"),
        ]);
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: None,
            cancellation: None,
            expected_device_counts: Default::default(),
        };
        let mut recovery = FakeDiscoveryRecovery {
            commands: access.commands.clone(),
            recoveries: 0,
        };

        let error =
            update_firmware_with_access_and_recovery(access.clone(), request, Some(&mut recovery))
                .await
                .expect_err("the second device-discovery failure must end the workflow");

        assert!(matches!(error, NvFwUpdError::TaskFailed { .. }));
        assert_eq!(recovery.recoveries, 1);
        assert!(!access
            .commands
            .lock()
            .expect("commands lock")
            .iter()
            .any(|command| command.ends_with(" b")));
    }

    #[tokio::test]
    async fn device_discovery_authentication_failure_does_not_power_cycle() {
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            success("mst started"),
            success(mst_output()),
            failure("authentication failed"),
        ]);
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: None,
            cancellation: None,
            expected_device_counts: Default::default(),
        };
        let mut recovery = FakeDiscoveryRecovery {
            commands: access.commands.clone(),
            recoveries: 0,
        };

        let error = update_firmware_with_access_and_recovery(access, request, Some(&mut recovery))
            .await
            .expect_err("invalid credentials cannot be fixed by a power cycle");

        assert!(matches!(error, NvFwUpdError::AuthFailed { .. }));
        assert_eq!(recovery.recoveries, 0);
    }

    #[tokio::test]
    async fn flash_batch_retries_only_failed_devices() {
        let access = FakeAccess::new(vec![failure("first failed"), success("flashed")]);
        let update = planned_update("/dev/mst/cx7", "1.0", "2.0");

        let (_, failed) = run_flash_batch(access.clone(), vec![update], FLASH_TIMEOUT_SECS)
            .await
            .unwrap();
        assert_eq!(failed.len(), 1);
        let retry_updates = failed.into_iter().map(|failure| failure.update).collect();
        let (completed, failed) =
            run_flash_batch(access.clone(), retry_updates, FLASH_TIMEOUT_SECS)
                .await
                .unwrap();

        assert_eq!(completed.len(), 1);
        assert!(failed.is_empty());
        assert_eq!(access.commands.lock().expect("commands lock").len(), 2);
    }

    #[tokio::test]
    async fn persistent_flash_failure_is_reported_after_retry() {
        let access = FakeAccess::new(vec![failure("first failed"), failure("retry failed")]);
        let update = planned_update("/dev/mst/cx7", "1.0", "2.0");

        let (_, failed) = run_flash_batch(access.clone(), vec![update], FLASH_TIMEOUT_SECS)
            .await
            .unwrap();
        let retry_updates = failed.into_iter().map(|failure| failure.update).collect();
        let (_, failed) = run_flash_batch(access.clone(), retry_updates, FLASH_TIMEOUT_SECS)
            .await
            .unwrap();

        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].error, "retry failed");
        assert_eq!(access.commands.lock().expect("commands lock").len(), 2);
    }

    #[tokio::test]
    async fn flash_batch_accepts_firmware_already_pending_reset() {
        let access = FakeAccess::new(vec![failure(
            "-E- Burning FS4 image failed: The firmware image was already updated on flash, pending reset.",
        )]);
        let update = planned_update("/dev/mst/cx7", "1.0", "2.0");

        let (completed, failed) = run_flash_batch(access.clone(), vec![update], FLASH_TIMEOUT_SECS)
            .await
            .unwrap();

        assert_eq!(completed.len(), 1);
        assert!(failed.is_empty());
        assert_eq!(access.commands.lock().expect("commands lock").len(), 1);
    }

    #[test]
    fn pending_reset_match_is_narrow_and_checks_both_output_streams() {
        assert!(flint_flash_is_pending_reset(&failure(
            "The firmware image was already updated on flash, pending reset"
        )));
        assert!(flint_flash_is_pending_reset(&CommandOutput {
            success: false,
            stdout: "THE FIRMWARE IMAGE WAS ALREADY UPDATED ON FLASH, PENDING RESET".to_owned(),
            stderr: String::new(),
        }));
        assert!(!flint_flash_is_pending_reset(&failure(
            "firmware image already updated"
        )));
        assert!(!flint_flash_is_pending_reset(&failure(
            "flash failed before pending reset"
        )));
    }

    #[tokio::test]
    async fn already_current_workflow_cleans_remote_staging() {
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            success("mst started"),
            success(mst_output()),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            success("/tmp/nvfwupd-flint.ABC123\n"),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            success(""),
        ]);
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: None,
            cancellation: None,
            expected_device_counts: Default::default(),
        };

        let outcome = update_firmware_with_access(access.clone(), request)
            .await
            .expect("already-current workflow should succeed");

        assert!(matches!(outcome, FirmwareUpdateOutcome::Skipped { .. }));
        let commands = access.commands.lock().expect("commands lock");
        assert!(commands
            .last()
            .is_some_and(|command| command == "rm -rf -- '/tmp/nvfwupd-flint.ABC123'"));
        assert!(!commands.iter().any(|command| command.ends_with(" b")));
    }

    #[tokio::test]
    async fn force_update_flashes_an_already_current_device() {
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            success("mst started"),
            success(mst_output()),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            success("/tmp/nvfwupd-flint.ABC123\n"),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            success("flashed"),
            success(""),
        ]);
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: true,
            timeout_secs: None,
            cancellation: None,
            expected_device_counts: Default::default(),
        };

        let outcome = update_firmware_with_access(access.clone(), request)
            .await
            .expect("force update should flash an already-current device");

        assert!(matches!(outcome, FirmwareUpdateOutcome::Completed(_)));
        assert!(access
            .commands
            .lock()
            .expect("commands lock")
            .iter()
            .any(|command| command.ends_with(" b")));
    }

    #[tokio::test]
    async fn update_workflow_uses_requested_command_timeout() {
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            success("mst started"),
            success(mst_output()),
            success("FW Version: 1.0\nPSID: PSID-A\n"),
            success("/tmp/nvfwupd-flint.ABC123\n"),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            success("flashed"),
            success(""),
        ]);
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: Some(7),
            cancellation: None,
            expected_device_counts: Default::default(),
        };

        let outcome = update_firmware_with_access(access.clone(), request)
            .await
            .expect("workflow should succeed");

        assert!(matches!(outcome, FirmwareUpdateOutcome::Completed(_)));
        assert_eq!(
            *access
                .command_timeouts
                .lock()
                .expect("command timeouts lock"),
            vec![7, 7, 7, 7, 7, 7, 140, 7]
        );
    }

    #[tokio::test]
    async fn cancellation_after_successful_flash_returns_completed() {
        let cancellation = CancellationToken::new();
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            success("mst started"),
            success(mst_output()),
            success("FW Version: 1.0\nPSID: PSID-A\n"),
            success("/tmp/nvfwupd-flint.ABC123\n"),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            success("flashed"),
            success(""),
        ])
        .cancelling_on_flash(cancellation.clone());
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: None,
            cancellation: Some(cancellation),
            expected_device_counts: Default::default(),
        };

        let outcome = update_firmware_with_access(access, request)
            .await
            .expect("completed flash must return its deterministic result");

        assert!(matches!(outcome, FirmwareUpdateOutcome::Completed(_)));
    }

    #[tokio::test]
    async fn cancellation_after_failed_flash_skips_retry_and_reports_failure() {
        let cancellation = CancellationToken::new();
        let access = FakeAccess::new(vec![
            success("/usr/bin/mst\n/usr/bin/flint\n"),
            success("mst started"),
            success(mst_output()),
            success("FW Version: 1.0\nPSID: PSID-A\n"),
            success("/tmp/nvfwupd-flint.ABC123\n"),
            success("FW Version: 2.0\nPSID: PSID-A\n"),
            failure("flash failed"),
            success(""),
        ])
        .cancelling_on_flash(cancellation.clone());
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: None,
            cancellation: Some(cancellation),
            expected_device_counts: Default::default(),
        };

        let error = update_firmware_with_access(access.clone(), request)
            .await
            .expect_err("failed flash must return a deterministic failure");

        let NvFwUpdError::TaskFailed { message, .. } = error else {
            panic!("expected Flint flash task failure");
        };
        assert!(message.starts_with("Flint flash "));
        assert!(!message.starts_with("Flint burn "));
        assert_eq!(
            access
                .commands
                .lock()
                .expect("commands lock")
                .iter()
                .filter(|command| command.ends_with(" b"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn cancellation_before_setup_runs_no_host_commands() {
        let access = FakeAccess::new(Vec::new());
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let request = FlintFirmwareUpdateRequest {
            targets: vec![target(FlintDeviceFamily::ConnectX7, &["/tmp/fw.bin"])],
            force_update: false,
            timeout_secs: None,
            cancellation: Some(cancellation),
            expected_device_counts: Default::default(),
        };

        let error = update_firmware_with_access(access.clone(), request)
            .await
            .expect_err("pre-cancelled workflow should stop before setup");

        assert!(matches!(error, NvFwUpdError::Cancelled { .. }));
        assert!(access.commands.lock().expect("commands lock").is_empty());
    }

    #[test]
    fn command_errors_classify_sudo_and_timeouts() {
        let _sanitize_guard = crate::utils::SanitizeTestGuard::new();
        crate::utils::Util::set_sanitize_config(Some(HashMap::from([(
            "password".to_owned(),
            "password is required".to_owned(),
        )])));

        let sudo = command_error(
            "host",
            "mst",
            failure("sudo: a password is required"),
            true,
            QUERY_TIMEOUT_SECS,
        );
        assert!(matches!(sudo, NvFwUpdError::HostSudoUnavailable { .. }));

        let timeout = command_error("host", "flint", failure("command timed out"), false, 45);
        assert!(matches!(timeout, NvFwUpdError::Timeout { seconds: 45, .. }));
    }

    #[test]
    fn host_target_debug_redacts_password() {
        let config = HostTargetConfig {
            ip: "127.0.0.1".to_owned(),
            username: "root".to_owned(),
            password: "secret".to_owned(),
            port: None,
            ssh_known_hosts: None,
            ssh_host_key_mode: crate::workflow::SshHostKeyMode::Disabled,
        };

        assert!(!format!("{config:?}").contains("secret"));
    }

    #[test]
    fn host_target_defaults_to_ssh_port_22() {
        let config = HostTargetConfig {
            ip: "127.0.0.1".to_owned(),
            username: "root".to_owned(),
            password: "secret".to_owned(),
            port: None,
            ssh_known_hosts: None,
            ssh_host_key_mode: crate::workflow::SshHostKeyMode::Disabled,
        };

        let access = os_access_from_config(&config).expect("host config should be valid");

        assert_eq!(access.port, 22);
    }

    #[test]
    fn supported_family_aliases_are_canonicalized() {
        assert_eq!(
            FlintDeviceFamily::from_component("ConnectX-7"),
            Some(FlintDeviceFamily::ConnectX7)
        );
        assert_eq!(
            FlintDeviceFamily::from_component("CX8"),
            Some(FlintDeviceFamily::ConnectX8)
        );
        assert_eq!(
            FlintDeviceFamily::from_component("BF3"),
            Some(FlintDeviceFamily::BlueField3)
        );
    }

    #[tokio::test]
    async fn missing_host_tools_are_reported_as_a_precondition() {
        let access = FakeAccess::new(vec![failure("flint: command not found")]);

        let error = prepare_host(&access, QUERY_TIMEOUT_SECS)
            .await
            .expect_err("missing Flint must fail preflight");

        assert!(matches!(error, NvFwUpdError::HostToolUnavailable { .. }));
    }

    #[tokio::test]
    async fn host_os_boot_wait_retries_ssh_and_systemd_startup() {
        let access = FakeAccess::new(vec![
            failure("OS connection error: connection refused"),
            CommandOutput {
                success: false,
                stdout: "system-state:starting\n".to_owned(),
                stderr: String::new(),
            },
            success(""),
        ]);

        let attempts = wait_for_host_os_ready_with_access(&access, None)
            .await
            .expect("host should become ready on the final probe");

        assert_eq!(attempts, 3);
        assert_eq!(access.commands.lock().expect("commands lock").len(), 3);
    }

    #[tokio::test]
    async fn host_os_boot_wait_does_not_retry_authentication_failures() {
        let access = FakeAccess::new(vec![failure("authentication failed")]);

        let error = wait_for_host_os_ready_with_access(&access, None)
            .await
            .expect_err("invalid credentials must fail without an AC-cycle retry");

        assert!(matches!(error, NvFwUpdError::AuthFailed { .. }));
        assert_eq!(access.commands.lock().expect("commands lock").len(), 1);
    }

    #[tokio::test]
    async fn host_os_boot_wait_uses_the_sop_timeout() {
        let access = FakeAccess::new(vec![
            failure("OS connection error: connection refused"),
            failure("OS connection error: connection refused"),
            failure("OS connection error: connection refused"),
        ]);

        let error = wait_for_host_os_ready_with_access(&access, None)
            .await
            .expect_err("host that never boots must exhaust the SOP window");

        assert!(matches!(
            error,
            NvFwUpdError::Timeout {
                operation: "wait for GB200 host OS boot",
                seconds: HOST_OS_BOOT_TIMEOUT_SECS,
            }
        ));
    }

    fn image(path: &str, psid: &str, version: &str) -> FlintImage {
        FlintImage {
            family: FlintDeviceFamily::ConnectX7,
            local_path: path.to_owned(),
            remote_path: format!(
                "/remote/{}",
                Path::new(path).file_name().unwrap().to_string_lossy()
            ),
            psid: psid.to_owned(),
            version: version.to_owned(),
        }
    }

    fn target(family: FlintDeviceFamily, files: &[&str]) -> FlintFirmwareTarget {
        FlintFirmwareTarget {
            family,
            firmware_files: files.iter().map(|file| (*file).to_owned()).collect(),
        }
    }

    fn planned_update(path: &str, current: &str, expected: &str) -> PlannedUpdate {
        PlannedUpdate {
            device: FlintDevice {
                family: FlintDeviceFamily::ConnectX7,
                path: path.to_owned(),
                psid: "PSID-A".to_owned(),
                version: current.to_owned(),
            },
            image: image("/tmp/fw.bin", "PSID-A", expected),
        }
    }

    fn success(stdout: impl Into<String>) -> CommandOutput {
        CommandOutput {
            success: true,
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }

    fn failure(stderr: impl Into<String>) -> CommandOutput {
        CommandOutput {
            success: false,
            stdout: String::new(),
            stderr: stderr.into(),
        }
    }
}
