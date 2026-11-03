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

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::domain::firmware_target::FirmwareDestination;

use async_trait::async_trait;

use crate::domain::node::*;
use crate::domain::rack::NodeConfig;
use crate::nodes::compute_gb200_nvidia::NvidiaGb200Compute;
use crate::transport::http_client::{HttpClient, REDFISH_V1_ROOT};
use crate::transport::redfish_client::NvidiaMnnvlinkTopology;
use crate::utilities::error::{Result, RmsError};

/// Dell GB200 compute node.
///
/// Dell GB200 with the AMI host BMC and NVIDIA HMC.
///
/// The hardware profile supplies upload parameters and stage order. This
/// adapter handles raw host-BMC version evidence and the AMI multipart contract;
/// inventory, power, task polling and activation use the shared GB200 driver.
pub struct DellGb200Compute {
    inner: NvidiaGb200Compute,
    bmc_http: HttpClient,
    pending_update: Mutex<Option<PendingUpdate>>,
    unreadable_task_grace: Duration,
}

struct PendingUpdate {
    task_id: Option<String>,
    original_force: bool,
    terminal: bool,
    /// Consecutive cleanup attempts that could not read the device task.
    unreadable_polls: u8,
    /// When the current run of failed task reads began.
    unreadable_since: Option<Instant>,
}

/// Cleanup treats a device task as terminal only after this many consecutive
/// failed status reads spanning at least [`UNREADABLE_TASK_GRACE`]. A task that
/// stays unreadable (for example after the BMC restarts and drops it) must not
/// block later updates on a long-lived node, but quick retries while the BMC
/// reboots must not restore the setting under a live task.
const MAX_UNREADABLE_TASK_POLLS: u8 = 3;
/// Matches the profile's default transient task-poll error budget.
const UNREADABLE_TASK_GRACE: Duration = Duration::from_secs(15 * 60);

const UPDATE_SERVICE: &str = "/redfish/v1/UpdateService";
const FORCE_UPDATE: &str = "/HttpPushUriOptions/ForceUpdate";
const PRESERVE_CONFIGURATION: &str = "/Oem/AMIUpdateService/PreserveConfiguration";
// AMI links IPMI and Network preservation; requesting opposite values conflicts.
const PRESERVE_ACCESS: &[&str] = &["Authentication", "IPMI", "Network", "REDFISH"];

impl DellGb200Compute {
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::ComputeGb200Dell {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        let mut config = config.clone();
        if config.runtime_config.is_none() {
            config.runtime_config =
                crate::config::hardware_profiles::HardwareProfileCatalog::default()
                    .runtime_config_for_request(config.node_type);
        }
        let inner = NvidiaGb200Compute::from_config(&config, rack_id)?;
        let endpoint = config.bmc_endpoint.as_ref().ok_or_else(|| {
            RmsError::invalid_argument("bmc_endpoint is required for Dell GB200 compute nodes")
        })?;
        let bmc_http = HttpClient::new_with_endpoint_credentials(
            &endpoint.endpoint.ip_address,
            endpoint.endpoint.port,
            endpoint.credentials.clone(),
            endpoint.dangerously_accept_invalid_certs,
            true,
        )?
        .require_path_prefix(REDFISH_V1_ROOT);
        Ok(Self {
            inner,
            bmc_http,
            pending_update: Mutex::new(None),
            unreadable_task_grace: UNREADABLE_TASK_GRACE,
        })
    }

    pub async fn get_mnnvlink_topology(&self) -> Result<NvidiaMnnvlinkTopology> {
        self.inner.get_mnnvlink_topology().await
    }

    async fn read_force_update(&self) -> Result<bool> {
        self.bmc_http
            .get(UPDATE_SERVICE, HttpClient::DEFAULT_TIMEOUT)
            .await?
            .pointer(FORCE_UPDATE)
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "Dell firmware upload requires boolean HttpPushUriOptions.ForceUpdate",
                )
            })
    }

    async fn set_force_update(&self, force: bool) -> Result<()> {
        self.bmc_http
            .patch_with_if_match(
                UPDATE_SERVICE,
                &json!({"HttpPushUriOptions": {"ForceUpdate": force}}),
                "*",
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;
        if self.read_force_update().await? != force {
            return Err(RmsError::failed_precondition(format!(
                "Dell firmware upload stopped: HttpPushUriOptions.ForceUpdate did not verify as {force}"
            )));
        }
        Ok(())
    }

    /// Called with pending_update locked, including after preparation errors.
    /// Keep the saved value on failure so cleanup can be retried.
    async fn restore_force_update(&self, pending: &mut Option<PendingUpdate>) -> Result<()> {
        let Some(update) = pending.as_mut() else {
            return Ok(());
        };
        let restore = async {
            if let Some(task_id) = &update.task_id
                && !update.terminal
            {
                match self.inner.poll_firmware_task(task_id).await {
                    Ok(status) => {
                        update.unreadable_polls = 0;
                        update.unreadable_since = None;
                        update.terminal = status.completed;
                        if !update.terminal {
                            return Err(RmsError::failed_precondition(format!(
                                "task {task_id} is still active; leaving ForceUpdate unchanged until it finishes"
                            )));
                        }
                    }
                    Err(error) => {
                        update.unreadable_polls = update.unreadable_polls.saturating_add(1);
                        let since = *update.unreadable_since.get_or_insert_with(Instant::now);
                        if update.unreadable_polls < MAX_UNREADABLE_TASK_POLLS
                            || since.elapsed() < self.unreadable_task_grace
                        {
                            return Err(error);
                        }
                        tracing::warn!(
                            node = self.id(),
                            task_id,
                            attempts = update.unreadable_polls,
                            error = %error.message,
                            "Dell firmware task stayed unreadable; treating it as terminal and restoring ForceUpdate"
                        );
                        update.terminal = true;
                    }
                }
            }
            if self.read_force_update().await? != update.original_force {
                self.set_force_update(update.original_force).await?;
            }
            Ok(())
        }
        .await;
        restore.map_err(|error: RmsError| {
            RmsError::new(
                error.code,
                format!(
                    "Dell ForceUpdate cleanup pending (restore {}): {}",
                    update.original_force, error.message
                ),
            )
        })?;
        *pending = None;
        Ok(())
    }

    async fn preserve_bmc_access(&self) -> Result<()> {
        let service = self
            .bmc_http
            .get(UPDATE_SERVICE, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        let mut preserve = service
            .pointer(PRESERVE_CONFIGURATION)
            .and_then(Value::as_object)
            .cloned()
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "Dell BMC upload requires AMI PreserveConfiguration settings",
                )
            })?;
        for key in PRESERVE_ACCESS {
            if !preserve.get(*key).is_some_and(Value::is_boolean) {
                return Err(RmsError::failed_precondition(format!(
                    "Dell BMC upload requires boolean PreserveConfiguration.{key}"
                )));
            }
        }
        if PRESERVE_ACCESS
            .iter()
            .all(|key| preserve[*key] == Value::Bool(true))
        {
            return Ok(());
        }
        for key in PRESERVE_ACCESS {
            preserve.insert((*key).to_owned(), Value::Bool(true));
        }
        self.bmc_http
            .patch_with_if_match(
                UPDATE_SERVICE,
                &json!({"Oem": {"AMIUpdateService": {
                    "@odata.type": "#AMIUpdateService.v1_0_0.AMIUpdateService",
                    "PreserveConfiguration": preserve,
                }}}),
                "*",
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;
        let verified = self
            .bmc_http
            .get(UPDATE_SERVICE, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        for key in PRESERVE_ACCESS {
            if verified
                .pointer(PRESERVE_CONFIGURATION)
                .and_then(|preserve| preserve.get(*key))
                .and_then(Value::as_bool)
                != Some(true)
            {
                return Err(RmsError::failed_precondition(format!(
                    "Dell BMC upload stopped: PreserveConfiguration.{key} did not verify as true"
                )));
            }
        }
        tracing::info!(
            node = self.id(),
            "verified Dell BMC access configuration preservation"
        );
        Ok(())
    }

    async fn version_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let inventory = self.inner.get_firmware_inventory().await?;
        if let Some(policy) = self.expected_inventory_policy() {
            let present: Vec<_> = inventory
                .iter()
                .map(|entry| {
                    entry
                        .target
                        .rsplit('/')
                        .next()
                        .unwrap_or(&entry.name)
                        .to_owned()
                })
                .collect();
            let missing = nvfwupd::expected_inventory::missing_expected_ap_names_from_present(
                &policy.ap_names,
                &present,
            );
            if !missing.is_empty() {
                return Err(RmsError::failed_precondition(format!(
                    "Dell GB200 expected firmware inventory is missing: {}",
                    missing.join(", ")
                )));
            }
        }
        Ok(inventory)
    }
}

#[async_trait]
impl FirmwareTaskCleanup for DellGb200Compute {
    async fn finish(&self, task_id: &str) -> Result<()> {
        let mut pending = self.pending_update.lock().await;
        if pending
            .as_ref()
            .and_then(|update| update.task_id.as_deref())
            != Some(task_id)
        {
            return Ok(());
        }
        self.restore_force_update(&mut pending).await
    }
}

#[async_trait]
impl Node for DellGb200Compute {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::ComputeGb200Dell
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::ComputeGb200Dell.as_str().to_owned(),
        );
        info
    }

    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        self.inner.expected_inventory_policy()
    }

    fn firmware_task_cleanup(&self) -> Option<&dyn FirmwareTaskCleanup> {
        Some(self)
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        self.inner.get_power_state().await
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        self.inner.set_power_state(op, target).await
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        self.inner.get_firmware_inventory().await
    }

    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        validate_firmware_targets(targets)?;
        let (manifest_targets, packages): (Vec<_>, Vec<_>) = targets
            .iter()
            .cloned()
            .partition(|target| is_host_bmc(target) || uses_manifest_inventory_version(target));
        let mut checks = Vec::new();
        if !manifest_targets.is_empty() {
            let inventory = self.version_inventory().await?;
            checks.push(inventory_version_check(&manifest_targets, &inventory)?);
        }
        if !packages.is_empty() {
            let mut check = self
                .inner
                .verify_firmware_package_versions(&packages)
                .await?;
            // NVFWUPD permits opaque compute images and can return matched=true
            // with no evidence. Dell activation must not report those as verified.
            check.matched &= check.details["status"] == "matched"
                && check.details["package_parse_errors"]
                    .as_array()
                    .is_some_and(Vec::is_empty);
            for target in &packages {
                if let Some(resolution) = &target.resolution {
                    let paths = match &resolution.destination {
                        FirmwareDestination::Redfish(path) => vec![path],
                        FirmwareDestination::Multipart(paths) => paths.iter().collect(),
                        FirmwareDestination::DriverDefault => Vec::new(),
                    };
                    for path in paths {
                        check.matched &=
                            check.details["components"]
                                .as_array()
                                .is_some_and(|components| {
                                    components.iter().any(|component| {
                                        component["inventory_path"] == path.as_str()
                                            && component["status"] == "matched"
                                    })
                                });
                    }
                }
            }
            checks.push(check);
        }
        let matched = !checks.is_empty() && checks.iter().all(|check| check.matched);
        Ok(FirmwareVersionCheckSummary {
            matched,
            details: json!({
                "status": if matched { "matched" } else { "mismatch" },
                "checks": checks.into_iter().map(|check| check.details).collect::<Vec<_>>(),
            }),
        })
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        validate_firmware_targets(std::slice::from_ref(target))?;
        let target = require_resolution(target)?;
        if options
            .cancellation
            .as_ref()
            .is_some_and(|token| token.is_cancelled())
        {
            return Err(RmsError::cancelled("Dell GB200 firmware update cancelled"));
        }
        let force_parameter = target
            .resolution
            .as_ref()
            .and_then(|resolution| resolution.update_parameters.as_ref())
            .map(|parameters| parameters.get("ForceUpdate"))
            .unwrap_or_else(|| {
                options
                    .special_json
                    .as_ref()
                    .and_then(|parameters| parameters.get("ForceUpdate"))
            });
        let reflash = force_parameter
            .and_then(Value::as_bool)
            .unwrap_or(force_update);
        // Serialize changes to the device-wide setting across uploads and task
        // cleanup. A prior task must finish before another upload can change it.
        let mut pending = self.pending_update.lock().await;
        self.restore_force_update(&mut pending).await?;
        if !reflash && is_host_bmc(&target) {
            let inventory = self.version_inventory().await?;
            if inventory_version_check(std::slice::from_ref(&target), &inventory)?.matched {
                return Ok(FirmwareUpdateOutcome::Skipped {
                    reason: "host BMC already has the requested firmware version".to_owned(),
                });
            }
        }
        // AMI uses UpdateService's persistent option for forced downgrades.
        // Its default multipart body must still omit ForceUpdate. Explicit
        // profile parameters remain unchanged in the multipart request.
        let cancellation = options.cancellation.clone();
        let outcome = self
            .inner
            .update_firmware_with_preparation(&target, false, options, async {
                let read_settings = async {
                    if is_host_bmc(&target) {
                        self.preserve_bmc_access().await?;
                    }
                    self.read_force_update().await
                };
                let original_force = if let Some(token) = &cancellation {
                    token
                        .run_until_cancelled(read_settings)
                        .await
                        .ok_or_else(|| {
                            RmsError::cancelled("Dell firmware preparation cancelled")
                        })??
                } else {
                    read_settings.await?
                };
                // Finish the bounded PATCH/read-back even if cancellation fires,
                // then roll it back before returning. Dropping an in-flight PATCH
                // could let a late device-side write race with that rollback.
                *pending = Some(PendingUpdate {
                    task_id: None,
                    original_force,
                    terminal: false,
                    unreadable_polls: 0,
                    unreadable_since: None,
                });
                if original_force != reflash {
                    self.set_force_update(reflash).await?;
                }
                if cancellation
                    .as_ref()
                    .is_some_and(|token| token.is_cancelled())
                {
                    return Err(RmsError::cancelled("Dell firmware preparation cancelled"));
                }
                Ok(())
            })
            .await;
        if let Ok(FirmwareUpdateOutcome::Started(handle)) = &outcome {
            if let Some(update) = pending.as_mut() {
                update.task_id = Some(handle.task_id.clone());
            }
            return outcome;
        }
        match (outcome, self.restore_force_update(&mut pending).await) {
            (outcome, Ok(())) => outcome,
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(cleanup_error)) => Err(RmsError::new(
                error.code,
                format!("{}; {}", error.message, cleanup_error.message),
            )),
        }
    }

    async fn start_firmware_upload(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
    ) -> Result<String> {
        match self
            .update_firmware(target, force_update, FirmwareUpdateOptions::default())
            .await?
        {
            FirmwareUpdateOutcome::Started(handle) => Ok(handle.task_id),
            FirmwareUpdateOutcome::Skipped { reason } => Err(RmsError::already_exists(reason)),
            FirmwareUpdateOutcome::Completed(summary) => Err(RmsError::internal(format!(
                "Dell GB200 firmware update completed without a task ID: {}",
                summary.message
            ))),
        }
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let mut pending = self.pending_update.lock().await;
        let status = self.inner.poll_firmware_task(task_id).await?;
        if status.completed
            && let Some(update) = pending.as_mut()
            && update.task_id.as_deref() == Some(task_id)
        {
            update.terminal = true;
            // finish() retries and reports a failed restore; the task result stays authoritative.
            if let Err(error) = self.restore_force_update(&mut pending).await {
                tracing::warn!(
                    node = self.id(),
                    task_id,
                    error = %error.message,
                    "Dell ForceUpdate restore failed after task completion; cleanup will retry"
                );
            }
        }
        Ok(status)
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        self.inner.activate_firmware_with(request).await
    }

    async fn activate_firmware(&self) -> Result<()> {
        self.inner.activate_firmware().await
    }
}

const HOST_BMC_INVENTORY: &str = "/redfish/v1/UpdateService/FirmwareInventory/BMC";

fn is_host_bmc(target: &FirmwareTarget) -> bool {
    Path::new(&target.firmware_file)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("ima"))
        || target
            .resolution
            .as_ref()
            .and_then(|resolution| resolution.oem_parameters.as_ref())
            .and_then(|parameters| parameters.get("ImageType"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|image_type| image_type.eq_ignore_ascii_case("BMC"))
}

/// A manifest version can directly verify explicit firmware-inventory targets
/// (not a chassis or package-wide destination with several component versions).
fn uses_manifest_inventory_version(target: &FirmwareTarget) -> bool {
    if target
        .expected_version
        .as_deref()
        .is_none_or(|version| version.trim().is_empty())
    {
        return false;
    }
    match target
        .resolution
        .as_ref()
        .map(|resolution| &resolution.destination)
    {
        Some(FirmwareDestination::Redfish(path)) => path.as_str().contains("/FirmwareInventory/"),
        Some(FirmwareDestination::Multipart(paths)) => {
            !paths.is_empty()
                && paths
                    .iter()
                    .all(|path| path.as_str().contains("/FirmwareInventory/"))
        }
        _ => false,
    }
}

/// A `BmcFwV<12 digits>.ima` filename encodes YY.MM.DDHHMMSS. Do not guess
/// versions from other filenames; manifests can supply an explicit Version.
fn filename_bmc_version(filename: &str) -> Option<String> {
    let name = Path::new(filename)
        .file_name()?
        .to_str()?
        .to_ascii_lowercase();
    let stem = name.strip_suffix(".ima")?;
    let (_, version) = stem.rsplit_once("bmcfwv")?;
    (version.len() == 12 && version.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| format!("{}.{}.{}", &version[..2], &version[2..4], &version[4..]))
}

fn expected_bmc_version(target: &FirmwareTarget) -> Result<String> {
    let declared = target
        .expected_version
        .as_deref()
        .map(str::trim)
        .filter(|version| !version.is_empty());
    let filename = filename_bmc_version(&target.firmware_file);
    match (declared, filename) {
        (Some(declared), Some(filename)) if declared != filename => {
            Err(RmsError::invalid_argument(format!(
                "BMC manifest Version {declared} conflicts with filename version {filename}"
            )))
        }
        (Some(declared), _) => Ok(declared.to_owned()),
        (None, Some(filename)) => Ok(filename),
        (None, None) => Err(RmsError::invalid_argument(
            "Dell GB200 raw BMC firmware requires a manifest Version or a BmcFwV<12 digits>.ima filename",
        )),
    }
}

/// Check version evidence before accepting a job, without performing device I/O.
pub(crate) fn validate_firmware_targets(targets: &[FirmwareTarget]) -> Result<()> {
    for target in targets {
        if crate::nodes::compute_gb200_nvidia::flint_family_for_target(target).is_some()
            || ["BF3_BFB", "LINKX"]
                .iter()
                .any(|component| target.component.eq_ignore_ascii_case(component))
        {
            return Err(RmsError::unimplemented(
                "in-band firmware update",
                NodeType::ComputeGb200Dell,
            ));
        }
        if is_host_bmc(target) {
            expected_bmc_version(target)?;
        }
    }
    Ok(())
}

/// Request handlers resolve targets against the active hardware profile, which
/// is an enabled operator override or the embedded Dell profile. The node does
/// not choose a profile itself, so an override can never be bypassed here.
fn require_resolution(target: &FirmwareTarget) -> Result<FirmwareTarget> {
    if target.resolution.is_none() {
        return Err(RmsError::failed_precondition(format!(
            "Dell GB200 firmware target {:?} was not resolved by a hardware profile",
            target.component
        )));
    }
    Ok(target.clone())
}

fn inventory_version_check(
    targets: &[FirmwareTarget],
    inventory: &[FirmwareInfo],
) -> Result<FirmwareVersionCheckSummary> {
    // When a stage contains several BMC images, the last image supplies the
    // final expected version at each inventory path; all images still upload.
    let mut expected = BTreeMap::new();
    for target in targets {
        let version = if is_host_bmc(target) {
            expected_bmc_version(target)?
        } else {
            target
                .expected_version
                .as_deref()
                .filter(|version| !version.trim().is_empty())
                .ok_or_else(|| RmsError::invalid_argument("missing manifest inventory version"))?
                .trim()
                .to_owned()
        };
        let paths = match target
            .resolution
            .as_ref()
            .map(|resolution| &resolution.destination)
        {
            Some(FirmwareDestination::Redfish(path)) => vec![path.as_str()],
            Some(FirmwareDestination::Multipart(paths)) if !paths.is_empty() => {
                paths.iter().map(|path| path.as_str()).collect()
            }
            _ => vec![HOST_BMC_INVENTORY],
        };
        for path in paths {
            expected.insert(path, version.clone());
        }
    }
    let components: Vec<_> = expected
        .into_iter()
        .map(|(path, version)| {
            let installed = inventory
                .iter()
                .find(|component| component.target == path)
                .map(|component| component.version.trim());
            let matched = installed == Some(version.as_str());
            json!({
                "name": path.rsplit('/').next().unwrap_or("BMC"),
                "inventory_path": path,
                "package_version": version,
                "system_version": installed,
                "status": if matched { "matched" } else { "mismatched" },
            })
        })
        .collect();
    let matched = !components.is_empty()
        && components
            .iter()
            .all(|component| component["status"] == "matched");
    Ok(FirmwareVersionCheckSummary {
        matched,
        details: json!({
            "status": if matched { "matched" } else { "mismatch" },
            "verification": "dell_manifest_or_filename_version",
            "components": components,
        }),
    })
}

#[cfg(test)]
mod tests;
