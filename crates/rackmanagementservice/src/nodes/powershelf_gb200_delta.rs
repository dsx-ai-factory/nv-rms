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

use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::domain::node::{
    ExpectedInventoryPolicy, FirmwareActivationMode, FirmwareActivationRequest,
    FirmwareActivationSummary, FirmwareInfo, FirmwareTarget, FirmwareTaskStatus,
    FirmwareUpdateOptions, FirmwareUpdateOutcome, Node, NodeType, PowerOp, PowerState,
    PowerTargetType,
};
use crate::domain::rack::{EndpointConfig, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::nodes::redfish_action_info::{
    ActionInfoResource, ChassisResetRequest, EmptyActionRequest, ResetType, reset_allowable_values,
    to_action_payload,
};
use crate::transport::redfish_client::{
    ODataLink, RedfishClient, ResourceCollection, redfish_client_from_powershelf_endpoint,
};
use crate::utilities::error::{Result, RmsError};

const DELTA_CHASSIS_COLLECTION_ENDPOINT: &str = "/redfish/v1/Chassis";
const DELTA_POWER_SHELVES_COLLECTION_ENDPOINT: &str = "/redfish/v1/PowerEquipment/PowerShelves";
const DELTA_TURN_ON_ACTION: &str = "#PowerShelf.TurnOnPSUs";
const DELTA_TURN_OFF_ACTION: &str = "#PowerShelf.TurnOffPSUs";
const DELTA_CHASSIS_RESET_ACTION: &str = "#Chassis.Reset";

/// Delta GB200 powershelf node.
///
/// Delta GB200 and GB300 powershelves share this Redfish behavior. Power state
/// is exposed through Delta OEM PSU properties. Power control depends on
/// `DeltaPowerControl`: GB200/GB300 shelves use Delta OEM actions under
/// `/redfish/v1/PowerEquipment/PowerShelves`, while VR NVL72 shelves use the
/// standard `#Chassis.Reset` action. Firmware workflows use the shared NVFWUPD
/// PowerShelf server type.
pub struct PowershelfGb200Delta {
    id: String,
    rack_id: String,
    node_type: NodeType,
    bmc_endpoint: EndpointConfig,
    expected_inventory: Option<ExpectedInventoryPolicy>,
    redfish: RedfishClient,
    power_control: DeltaPowerControl,

    // Serializes full node operations across shared Redfish/NVFWUPD clients.
    // The guard is held across awaits so BMC operations cannot interleave.
    op_lock: tokio::sync::Mutex<()>,

    chassis_url: tokio::sync::OnceCell<String>,
    power_shelf_url: tokio::sync::OnceCell<String>,
    reset_chassis_url: tokio::sync::OnceCell<String>,
}

/// Redfish power-control interface a Delta shelf exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeltaPowerControl {
    /// Delta OEM `#PowerShelf.TurnOnPSUs` / `#PowerShelf.TurnOffPSUs` actions
    /// on a `/redfish/v1/PowerEquipment/PowerShelves` member (GB200/GB300).
    OemPsuActions,
    /// Standard `#Chassis.Reset` on the Delta shelf chassis, with `ResetType`
    /// `On` or `ForceOff` (VR NVL72 shelves managed by an NVIDIA PMC).
    ChassisReset,
}

/// Power transition a Delta shelf can perform. Delta shelves have no graceful
/// power off, so every off request becomes `ForceOff`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeltaPowerIntent {
    On,
    ForceOff,
}

impl DeltaPowerIntent {
    /// Returns the Delta OEM action key for this intent.
    fn action_key(self) -> &'static str {
        match self {
            Self::On => DELTA_TURN_ON_ACTION,
            Self::ForceOff => DELTA_TURN_OFF_ACTION,
        }
    }
}

/// Maps an intent to its `#Chassis.Reset` `ResetType`.
impl From<DeltaPowerIntent> for ResetType {
    fn from(intent: DeltaPowerIntent) -> Self {
        match intent {
            DeltaPowerIntent::On => Self::On,
            DeltaPowerIntent::ForceOff => Self::ForceOff,
        }
    }
}

impl PowershelfGb200Delta {
    /// Builds a Delta GB200 shelf node that uses the OEM PSU power actions.
    ///
    /// # Errors
    ///
    /// Returns `Unimplemented` if `config.node_type` is not
    /// `PowershelfGb200Delta`, `InvalidArgument` if no BMC endpoint is
    /// configured, or any error from building the Redfish client.
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::PowershelfGb200Delta {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }
        let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
            return Err(RmsError::invalid_argument(
                "bmc_endpoint is required for powershelf nodes",
            ));
        };
        let redfish = redfish_client_from_powershelf_endpoint(bmc_endpoint)?;

        Ok(Self {
            id: config.id.clone(),
            rack_id: rack_id.to_owned(),
            node_type: config.node_type,
            bmc_endpoint: bmc_endpoint.clone(),
            expected_inventory: config.expected_inventory.clone(),
            redfish,
            power_control: DeltaPowerControl::OemPsuActions,
            op_lock: tokio::sync::Mutex::new(()),
            chassis_url: tokio::sync::OnceCell::new(),
            power_shelf_url: tokio::sync::OnceCell::new(),
            reset_chassis_url: tokio::sync::OnceCell::new(),
        })
    }

    /// Selects the Redfish power-control interface. Defaults to
    /// [`DeltaPowerControl::OemPsuActions`].
    pub(crate) fn with_power_control(mut self, power_control: DeltaPowerControl) -> Self {
        self.power_control = power_control;
        self
    }

    /// Returns the URL of the chassis that advertises `#Chassis.Reset`,
    /// discovering it on first use. A failed discovery is not cached.
    async fn resolve_reset_chassis_url(&self, redfish: &RedfishClient) -> Result<&str> {
        self.reset_chassis_url
            .get_or_try_init(|| discover_delta_reset_chassis_url(redfish))
            .await
            .map(String::as_str)
    }

    /// Returns the URL of the shelf chassis used for power-state reads,
    /// discovering it on first use. A failed discovery is not cached.
    async fn resolve_chassis_url(&self, redfish: &RedfishClient) -> Result<&str> {
        self.chassis_url
            .get_or_try_init(|| discover_delta_chassis_url(redfish))
            .await
            .map(String::as_str)
    }

    /// Returns the URL of the `PowerShelves` member that carries the OEM PSU
    /// actions, discovering it on first use. A failed discovery is not cached.
    async fn resolve_power_shelf_url(&self, redfish: &RedfishClient) -> Result<&str> {
        self.power_shelf_url
            .get_or_try_init(|| discover_delta_power_shelf_url(redfish))
            .await
            .map(String::as_str)
    }

    /// Points the node at a test Redfish server and clears the cached
    /// resource URLs.
    #[cfg(test)]
    pub(crate) fn replace_redfish_for_test(&mut self, base_url: &str) {
        self.redfish = crate::transport::redfish_client::redfish_client_for_test(base_url);
        self.chassis_url = tokio::sync::OnceCell::new();
        self.power_shelf_url = tokio::sync::OnceCell::new();
        self.reset_chassis_url = tokio::sync::OnceCell::new();
    }

    /// Returns the NVFWUPD target for this shelf's BMC. TLS verification is on
    /// unless the endpoint sets `dangerously_accept_invalid_certs`.
    fn nvfwupd_target_config(&self) -> nvfwupd::workflow::TargetConfig {
        nvfwupd_adapter::to_target_config_with_credentials(
            self.node_type,
            &self.bmc_endpoint.endpoint.ip_address,
            self.bmc_endpoint.endpoint.port,
            self.bmc_endpoint.credentials.as_ref(),
            !self.bmc_endpoint.dangerously_accept_invalid_certs,
        )
    }
}

#[async_trait]
impl Node for PowershelfGb200Delta {
    fn id(&self) -> &str {
        &self.id
    }

    fn rack_id(&self) -> &str {
        &self.rack_id
    }

    fn node_type(&self) -> NodeType {
        self.node_type
    }

    fn get_info(&self) -> HashMap<String, String> {
        HashMap::from([
            ("type".to_owned(), self.node_type.as_str().to_owned()),
            (
                "host".to_owned(),
                self.bmc_endpoint.endpoint.ip_address.clone(),
            ),
            (
                "port".to_owned(),
                self.bmc_endpoint.endpoint.port.to_string(),
            ),
            (
                "macAddress".to_owned(),
                self.bmc_endpoint.endpoint.mac_address.clone(),
            ),
        ])
    }

    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        self.expected_inventory.as_ref()
    }

    /// Aggregates the shelf's PSU power states.
    ///
    /// Returns `Off` if any PSU reports off, `On` only if every PSU reports
    /// on, and `Unknown` otherwise, including when no PSU can be read.
    /// Individual PSU read failures are logged, not returned.
    async fn get_power_state(&self) -> Result<PowerState> {
        let _op_guard = self.op_lock.lock().await;
        let redfish = &self.redfish;

        tracing::debug!(node = %self.id(), "get_power_state");
        let chassis_url = self.resolve_chassis_url(redfish).await?;
        let chassis: DeltaResource = redfish_get(redfish, chassis_url).await?;
        let psu_states = fetch_delta_psu_power_states(redfish, &chassis).await;
        let state = aggregate_delta_psu_power_states(&psu_states);
        tracing::debug!(
            node = %self.id(),
            ?state,
            psu_count = psu_states.len(),
            "get_power_state: aggregated from Delta PowerSupplies"
        );
        Ok(state)
    }

    /// Powers the shelf on or off through the configured `DeltaPowerControl`.
    ///
    /// `On` and `ForceOn` power on. `Off`, `GracefulShutdown`, and `ForceOff`
    /// all perform a hard power off, and the graceful requests log a warning.
    /// `target` is ignored.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for any other operation, and fails if the
    /// shelf does not advertise the required action.
    async fn set_power_state(&self, op: PowerOp, _target: PowerTargetType) -> Result<()> {
        tracing::info!(node = %self.id(), ?op, "set_power_state");
        let intent = match op {
            PowerOp::On | PowerOp::ForceOn => DeltaPowerIntent::On,
            // Delta shelves have no graceful power off, only TurnOffPSUs or a
            // ForceOff reset. RMS serves graceful requests with that action so
            // callers never see the hardware gap.
            PowerOp::Off | PowerOp::GracefulShutdown | PowerOp::ForceOff => {
                DeltaPowerIntent::ForceOff
            }
            _ => {
                return Err(RmsError::invalid_argument(format!(
                    "{op:?} is not supported for powershelf power control"
                )));
            }
        };

        let _op_guard = self.op_lock.lock().await;
        let redfish = &self.redfish;
        let action = match self.power_control {
            DeltaPowerControl::OemPsuActions => {
                let power_shelf_url = self.resolve_power_shelf_url(redfish).await?;
                invoke_delta_power_action(redfish, power_shelf_url, intent).await?;
                intent.action_key()
            }
            DeltaPowerControl::ChassisReset => {
                let chassis_url = self.resolve_reset_chassis_url(redfish).await?;
                invoke_delta_chassis_reset(redfish, chassis_url, intent).await?;
                DELTA_CHASSIS_RESET_ACTION
            }
        };
        if matches!(op, PowerOp::Off | PowerOp::GracefulShutdown) {
            tracing::warn!(
                node = %self.id(),
                ?op,
                action,
                "Delta powershelf has no graceful power off; served the request as ForceOff"
            );
        }
        Ok(())
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let _op_guard = self.op_lock.lock().await;

        let inventory = nvfwupd::workflow_api::get_firmware_inventory(self.nvfwupd_target_config())
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(inventory
            .into_iter()
            .map(nvfwupd_adapter::to_firmware_info)
            .collect())
    }

    /// Updates shelf firmware through NVFWUPD.
    ///
    /// Only the target's firmware file and expected version are sent; its
    /// component, resolution, and LinkX policy are cleared. The node's
    /// expected-inventory AP names, if any, are passed through.
    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let _op_guard = self.op_lock.lock().await;

        let target = FirmwareTarget {
            component: String::new(),
            firmware_file: target.firmware_file.clone(),
            expected_version: target.expected_version.clone(),
            resolution: None,
            linkx_policy: None,
        };
        let request = nvfwupd_adapter::to_update_request(&target, force_update, options);
        let expected_inventory = self
            .expected_inventory_policy()
            .map(|policy| policy.ap_names.as_ref().to_vec());
        let outcome = nvfwupd::workflow_api::update_firmware_with_expected_inventory(
            self.nvfwupd_target_config(),
            request,
            expected_inventory,
        )
        .await
        .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_outcome(outcome))
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let _op_guard = self.op_lock.lock().await;

        let status = nvfwupd::workflow_api::get_task_status(self.nvfwupd_target_config(), task_id)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::to_task_status(status))
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        let _op_guard = self.op_lock.lock().await;

        let request = nvfwupd_adapter::to_activation_request(request);
        let summary =
            nvfwupd::workflow_api::activate_firmware(self.nvfwupd_target_config(), request)
                .await
                .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_activation_summary(summary))
    }

    /// Activates staged firmware with a non-forced power shelf reset.
    async fn activate_firmware(&self) -> Result<()> {
        self.activate_firmware_with(FirmwareActivationRequest {
            mode: FirmwareActivationMode::PowerShelfReset { force: false },
            cancellation: None,
        })
        .await
        .map(|_| ())
    }
}

/// Finds the Delta shelf chassis used for power-state reads.
///
/// Prefers a chassis with a `PowerSubsystem` and a Delta marker. Otherwise
/// falls back to the first chassis with a `PowerSubsystem`.
async fn discover_delta_chassis_url(redfish_client: &RedfishClient) -> Result<String> {
    discover_member_url(
        redfish_client,
        DELTA_CHASSIS_COLLECTION_ENDPOINT,
        chassis_resource_looks_like_delta_powershelf,
        chassis_has_power_subsystem,
        "no Delta powershelf chassis found in /redfish/v1/Chassis",
        "failed to inspect chassis member while discovering Delta powershelf chassis",
    )
    .await
}

/// Finds the `PowerShelves` member that carries the Delta OEM PSU actions.
///
/// Prefers a member that also looks like a Delta power shelf. Otherwise falls
/// back to the first member with origin-relative TurnOn and TurnOff targets.
async fn discover_delta_power_shelf_url(redfish_client: &RedfishClient) -> Result<String> {
    discover_member_url(
        redfish_client,
        DELTA_POWER_SHELVES_COLLECTION_ENDPOINT,
        power_shelf_resource_looks_like_delta,
        power_shelf_has_delta_actions,
        "no Delta powershelf found in /redfish/v1/PowerEquipment/PowerShelves",
        "failed to inspect PowerShelf member while discovering Delta powershelf actions",
    )
    .await
}

/// Finds the Delta shelf chassis that advertises `#Chassis.Reset`.
///
/// There is deliberately no looser fallback: posting a reset to a chassis that
/// is not the Delta shelf would power the wrong device.
async fn discover_delta_reset_chassis_url(redfish_client: &RedfishClient) -> Result<String> {
    discover_member_url(
        redfish_client,
        DELTA_CHASSIS_COLLECTION_ENDPOINT,
        chassis_resource_has_delta_reset,
        |_| false,
        "no Delta powershelf chassis with #Chassis.Reset found in /redfish/v1/Chassis",
        "failed to inspect chassis member while discovering Delta powershelf #Chassis.Reset",
    )
    .await
}

/// Fields RMS reads from a Delta chassis or `PowerShelves` member during
/// discovery and power control.
#[derive(Debug, Clone, Default, Deserialize)]
struct DeltaResource {
    #[serde(rename = "EquipmentType")]
    equipment_type: Option<String>,
    #[serde(rename = "Name")]
    name: Option<String>,
    #[serde(rename = "Manufacturer")]
    manufacturer: Option<String>,
    #[serde(rename = "PowerSubsystem")]
    power_subsystem: Option<ODataLink>,
    // Kept untyped so a null or unexpected entry cannot fail decoding of the
    // whole resource; only the VR path reads `#Chassis.Reset` out of it.
    #[serde(rename = "Actions", default)]
    actions: Option<Map<String, Value>>,
    #[serde(rename = "Oem", default)]
    oem: DeltaOem,
}

/// A resource's `Oem` object: the `deltaenergysystems` section, plus every
/// other vendor key so their names can be checked for a Delta marker.
#[derive(Debug, Clone, Default, Deserialize)]
struct DeltaOem {
    #[serde(rename = "deltaenergysystems")]
    delta: Option<DeltaEnergyOem>,
    #[serde(flatten)]
    others: Map<String, Value>,
}

/// The `Oem.deltaenergysystems` section: OEM PSU actions on a shelf, or the
/// OEM power value on a PSU.
#[derive(Debug, Clone, Default, Deserialize)]
struct DeltaEnergyOem {
    #[serde(rename = "Actions", default)]
    actions: HashMap<String, DeltaAction>,
    #[serde(rename = "Power")]
    power: Option<Value>,
}

/// One advertised Redfish action: its POST target and optional ActionInfo link.
#[derive(Debug, Clone, Default, Deserialize)]
struct DeltaAction {
    target: Option<String>,
    #[serde(rename = "@Redfish.ActionInfo")]
    action_info: Option<String>,
}

/// A chassis `PowerSubsystem`, read only for its `PowerSupplies` link.
#[derive(Debug, Clone, Default, Deserialize)]
struct PowerSubsystemResource {
    #[serde(rename = "PowerSupplies")]
    power_supplies: Option<ODataLink>,
}

/// One PSU, read for its OEM power value and its standard `PowerState`.
#[derive(Debug, Clone, Default, Deserialize)]
struct PowerSupplyResource {
    #[serde(rename = "PowerState")]
    power_state: Option<Value>,
    #[serde(rename = "Oem", default)]
    oem: DeltaOem,
}

/// Fetches and decodes the Redfish resource at `path`.
async fn redfish_get<T>(redfish: &RedfishClient, path: &str) -> Result<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    redfish.get_resource(path).await.map_err(RmsError::from)
}

/// Resolves a single member URL out of a Redfish collection.
///
/// Returns the first member satisfying `is_match`; failing that, the first
/// member satisfying the looser `is_fallback` predicate.
///
/// FMEA note (multi-shelf ambiguity): this deliberately takes the *first*
/// match and does not detect the case where more than one member matches. That
/// is safe under the current 1-BMC-per-shelf topology (each Delta shelf is
/// fronted by its own controller, whose collection contains exactly one shelf).
/// It would become ambiguous only if an aggregating Redfish endpoint ever
/// fronted multiple shelves; because the result is memoized in a `OnceCell` for
/// the node's lifetime (see `resolve_chassis_url` / `resolve_power_shelf_url`),
/// a wrong first choice would also persist until the process restarts. Left as
/// first-match-wins for now; hardening (fail-loud on multiple matches) is
/// tracked separately as it is riskier than it looks.
async fn discover_member_url<F, G>(
    redfish: &RedfishClient,
    collection_endpoint: &str,
    is_match: F,
    is_fallback: G,
    not_found_msg: &str,
    inspect_error_msg: &str,
) -> Result<String>
where
    F: Fn(&DeltaResource) -> bool,
    G: Fn(&DeltaResource) -> bool,
{
    let collection: ResourceCollection = redfish_get(redfish, collection_endpoint).await?;
    let mut fallback = None;

    for member in &collection.members {
        let Some(uri) = member.nonempty() else {
            continue;
        };
        match redfish_get::<DeltaResource>(redfish, uri).await {
            Ok(resource) if is_match(&resource) => {
                return Ok(uri.to_owned());
            }
            Ok(resource) if is_fallback(&resource) && fallback.is_none() => {
                fallback = Some(uri.to_owned());
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    uri,
                    error = %e.message,
                    "{inspect_error_msg}"
                );
            }
        }
    }

    fallback.ok_or_else(|| RmsError::internal(not_found_msg))
}

/// Returns `true` if the chassis has a `PowerSubsystem` and a Delta marker.
fn chassis_resource_looks_like_delta_powershelf(chassis: &DeltaResource) -> bool {
    chassis_has_power_subsystem(chassis) && has_delta_marker(chassis)
}

/// Fallback predicate for chassis discovery: "any chassis that exposes a
/// PowerSubsystem".
///
/// FMEA note (loose fallback): this is intentionally broad and is only reached
/// when no chassis carries the Delta OEM marker (see
/// `chassis_resource_looks_like_delta_powershelf`). Because a compute or switch
/// chassis can also expose a `PowerSubsystem`, on a controller that ever
/// enumerated more than one chassis this could fall back onto a non-shelf
/// chassis. That does not occur under the current 1-BMC-per-shelf topology
/// (the controller's collection contains only its own shelf). A stricter
/// fallback (e.g. also requiring `ChassisType == "Shelf"`) was considered but
/// deferred: it risks failing discovery on shelves whose firmware does not set
/// that field, so it should be validated against real hardware first.
fn chassis_has_power_subsystem(chassis: &DeltaResource) -> bool {
    chassis
        .power_subsystem
        .as_ref()
        .and_then(ODataLink::nonempty)
        .is_some()
}

/// Returns `true` if the member has a Delta marker, identifies as a power
/// shelf, and has usable TurnOn and TurnOff targets.
fn power_shelf_resource_looks_like_delta(power_shelf: &DeltaResource) -> bool {
    has_delta_marker(power_shelf)
        && resource_looks_like_power_shelf(power_shelf)
        && power_shelf_has_delta_actions(power_shelf)
}

/// Returns `true` if `EquipmentType` contains `powershelf` or `Name` contains
/// `power shelf`, ignoring ASCII case.
fn resource_looks_like_power_shelf(resource: &DeltaResource) -> bool {
    string_field_contains(resource.equipment_type.as_deref(), "powershelf")
        || string_field_contains(resource.name.as_deref(), "power shelf")
}

/// Returns `true` if `Manufacturer` contains `delta`, `Oem` has a
/// `deltaenergysystems` section, or another `Oem` key contains `delta`.
/// The `Manufacturer` and other-key checks ignore ASCII case.
fn has_delta_marker(resource: &DeltaResource) -> bool {
    string_field_contains(resource.manufacturer.as_deref(), "delta")
        || resource.oem.delta.is_some()
        || resource
            .oem
            .others
            .keys()
            .any(|key| key.to_ascii_lowercase().contains("delta"))
}

/// Returns `true` if `value` is present and its ASCII-lowercased form contains
/// `needle`. `needle` must already be lowercase.
fn string_field_contains(value: Option<&str>, needle: &str) -> bool {
    value.is_some_and(|value| value.to_ascii_lowercase().contains(needle))
}

/// Returns `true` if both OEM TurnOn and TurnOff actions have origin-relative
/// targets.
fn power_shelf_has_delta_actions(power_shelf: &DeltaResource) -> bool {
    delta_power_action_target(power_shelf, DELTA_TURN_ON_ACTION)
        .is_some_and(|target| is_bmc_relative_action_target(&target))
        && delta_power_action_target(power_shelf, DELTA_TURN_OFF_ACTION)
            .is_some_and(|target| is_bmc_relative_action_target(&target))
}

/// Returns `true` if the chassis has a Delta marker and a `#Chassis.Reset`
/// action with an origin-relative target.
fn chassis_resource_has_delta_reset(chassis: &DeltaResource) -> bool {
    has_delta_marker(chassis)
        && chassis_reset_action(chassis)
            .as_ref()
            .and_then(chassis_reset_target)
            .is_some_and(is_bmc_relative_action_target)
}

/// Decodes the Delta chassis reset action.
///
/// If the reset action is not present, silently returns `None`.
/// If the action is present but cannot be decoded, logs an error and returns `None`.
fn chassis_reset_action(chassis: &DeltaResource) -> Option<DeltaAction> {
    let action = chassis.actions.as_ref()?.get(DELTA_CHASSIS_RESET_ACTION)?;
    match serde_json::from_value(action.clone()) {
        Ok(action) => Some(action),
        Err(e) => {
            tracing::error!(
                action = DELTA_CHASSIS_RESET_ACTION,
                error = %e,
                "failed to decode Delta chassis reset action"
            );
            None
        }
    }
}

/// Returns the action's `target`, or `None` if it is missing or empty.
fn chassis_reset_target(action: &DeltaAction) -> Option<&str> {
    action.target.as_deref().filter(|target| !target.is_empty())
}

/// Same origin-relative rules as `RedfishClient::post_action`: skip members
/// whose advertised action targets would be rejected at POST time.
fn is_bmc_relative_action_target(uri: &str) -> bool {
    let uri = uri.trim();
    !uri.is_empty() && uri.starts_with('/') && !uri.starts_with("//") && !uri.contains("://")
}

/// Re-reads the `PowerShelves` member and posts the intent's OEM action with an
/// empty body.
///
/// # Errors
///
/// Fails if the member no longer advertises the action's target.
async fn invoke_delta_power_action(
    redfish: &RedfishClient,
    power_shelf_url: &str,
    intent: DeltaPowerIntent,
) -> Result<()> {
    let power_shelf: DeltaResource = redfish_get(redfish, power_shelf_url).await?;
    let target = delta_power_action_target(&power_shelf, intent.action_key()).ok_or_else(|| {
        RmsError::internal(format!(
            "Delta powershelf Actions missing {} target",
            intent.action_key()
        ))
    })?;
    let payload = to_action_payload(&EmptyActionRequest {})?;
    redfish
        .post_action(&target, &payload)
        .await
        .map_err(RmsError::from)?;
    Ok(())
}

/// Posts `#Chassis.Reset` with the intent's `ResetType`.
///
/// When the action's ActionInfo lists `ResetType` allowable values, a reset
/// type outside them is refused without sending anything.
async fn invoke_delta_chassis_reset(
    redfish: &RedfishClient,
    chassis_url: &str,
    intent: DeltaPowerIntent,
) -> Result<()> {
    let chassis: DeltaResource = redfish_get(redfish, chassis_url).await?;
    let action = chassis_reset_action(&chassis).ok_or_else(|| {
        RmsError::internal(format!(
            "Delta powershelf chassis Actions missing {DELTA_CHASSIS_RESET_ACTION}"
        ))
    })?;
    let target = chassis_reset_target(&action).ok_or_else(|| {
        RmsError::internal(format!(
            "Delta powershelf chassis Actions missing {DELTA_CHASSIS_RESET_ACTION} target"
        ))
    })?;
    let reset_type = ResetType::from(intent);

    if let Some(action_info_url) = action.action_info.as_deref().filter(|url| !url.is_empty()) {
        let action_info: ActionInfoResource = redfish_get(redfish, action_info_url).await?;
        if let Some(allowed) = reset_allowable_values(&action_info)
            && !reset_type.is_in(&allowed)
        {
            return Err(RmsError::failed_precondition(format!(
                "{DELTA_CHASSIS_RESET_ACTION} ResetType {:?} is not in \
                 AllowableValues {allowed:?}; refusing to send an unsupported reset",
                reset_type.as_str()
            )));
        }
    }

    let payload = to_action_payload(&ChassisResetRequest { reset_type })?;
    tracing::debug!(
        action = DELTA_CHASSIS_RESET_ACTION,
        target,
        payload = %payload,
        "invoke_delta_chassis_reset"
    );
    Ok(redfish.post_action(target, &payload).await?)
}

/// Returns the target of the named `Oem.deltaenergysystems` action, or `None`
/// if it is missing or empty.
fn delta_power_action_target(power_shelf: &DeltaResource, action: &str) -> Option<String> {
    power_shelf
        .oem
        .delta
        .as_ref()?
        .actions
        .get(action)?
        .target
        .as_deref()
        .filter(|target| !target.is_empty())
        .map(str::to_owned)
}

/// Walks the chassis `PowerSubsystem` to its PSUs and decodes each one's power
/// state.
///
/// Returns one entry per `PowerSupplies` member, in order. A member that
/// cannot be read or decoded is `None`. Returns an empty vector if a link is
/// missing or the subsystem or collection cannot be fetched. Failures are
/// logged, not returned.
async fn fetch_delta_psu_power_states(
    redfish: &RedfishClient,
    chassis: &DeltaResource,
) -> Vec<Option<bool>> {
    let Some(subsystem_url) = chassis
        .power_subsystem
        .as_ref()
        .and_then(ODataLink::nonempty)
        .map(str::to_owned)
    else {
        tracing::warn!("Delta powershelf chassis exposes no PowerSubsystem link");
        return Vec::new();
    };

    let subsystem: PowerSubsystemResource = match redfish_get(redfish, &subsystem_url).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                url = %subsystem_url,
                error = %e.message,
                "failed to fetch Delta PowerSubsystem"
            );
            return Vec::new();
        }
    };

    let Some(supplies_url) = subsystem
        .power_supplies
        .as_ref()
        .and_then(ODataLink::nonempty)
        .map(str::to_owned)
    else {
        tracing::warn!(url = %subsystem_url, "Delta PowerSubsystem has no PowerSupplies link");
        return Vec::new();
    };

    let collection: ResourceCollection = match redfish_get(redfish, &supplies_url).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                url = %supplies_url,
                error = %e.message,
                "failed to fetch Delta PowerSupplies collection"
            );
            return Vec::new();
        }
    };

    let mut out = Vec::with_capacity(collection.members.len());
    for member in &collection.members {
        let Some(uri) = member.nonempty() else {
            out.push(None);
            continue;
        };
        match redfish_get::<PowerSupplyResource>(redfish, uri).await {
            Ok(psu) => out.push(decode_delta_psu_power_state(&psu)),
            Err(e) => {
                tracing::warn!(uri, error = %e.message, "failed to fetch Delta PowerSupply");
                out.push(None);
            }
        }
    }
    out
}

/// Decodes a PSU's power state, preferring the Delta OEM `Power` value over
/// the standard `PowerState`. Returns `None` if neither decodes.
fn decode_delta_psu_power_state(psu: &PowerSupplyResource) -> Option<bool> {
    decode_power_value(
        psu.oem
            .delta
            .as_ref()
            .and_then(|delta| delta.power.as_ref()),
    )
    .or_else(|| decode_power_value(psu.power_state.as_ref()))
}

/// Decodes a boolean, or the strings `on`/`true` and `off`/`false` in any
/// ASCII case. Returns `None` for anything else.
fn decode_power_value(field: Option<&Value>) -> Option<bool> {
    match field? {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "on" | "true" => Some(true),
            "off" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Combines PSU states into a shelf state.
///
/// Returns `Off` if any PSU is off, `On` if every PSU is on, and `Unknown` if
/// the list is empty or any PSU state is unknown and none is off.
fn aggregate_delta_psu_power_states(states: &[Option<bool>]) -> PowerState {
    if states.is_empty() {
        return PowerState::Unknown;
    }
    let mut all_on = true;
    for state in states {
        match state {
            Some(false) => return PowerState::Off,
            Some(true) => {}
            None => all_on = false,
        }
    }
    if all_on {
        PowerState::On
    } else {
        PowerState::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
    use crate::utilities::error::ErrorCode;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(node_type: NodeType) -> NodeConfig {
        NodeConfig {
            id: "ps-01".into(),
            node_type,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.5".into(),
                    mac_address: "aa:bb:cc:dd:ee:ff".into(),
                    port: 8443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "pass")),
                true,
            )),
            host_endpoint: None,
            expected_inventory: None,
            recovery: None,
            runtime_config: None,
        }
    }

    fn test_node(base_url: &str) -> PowershelfGb200Delta {
        let config = test_config(NodeType::PowershelfGb200Delta);
        let mut powershelf = PowershelfGb200Delta::from_config(&config, "rack-01").unwrap();
        powershelf.replace_redfish_for_test(base_url);
        powershelf
    }

    fn parse<T: DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).expect("test fixture JSON")
    }

    fn delta_psu_json(power: Value) -> Value {
        serde_json::json!({
            "Oem": {
                "deltaenergysystems": {
                    "Power": power
                }
            }
        })
    }

    fn delta_psu(power: Value) -> PowerSupplyResource {
        parse(delta_psu_json(power))
    }

    async fn mount_delta_power_state_tree(server: &MockServer, psu_powers: &[bool]) {
        Mock::given(method("GET"))
            .and(path(DELTA_CHASSIS_COLLECTION_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [
                    { "@odata.id": "/redfish/v1/Chassis/chassis" }
                ]
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "@odata.id": "/redfish/v1/Chassis/chassis",
                "Id": "chassis",
                "Manufacturer": "DELTA",
                "Model": "810",
                "PowerSubsystem": {
                    "@odata.id": "/redfish/v1/Chassis/chassis/PowerSubsystem"
                },
                "Oem": {
                    "deltaenergysystems": {}
                }
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/chassis/PowerSubsystem"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "PowerSupplies": {
                    "@odata.id": "/redfish/v1/Chassis/chassis/PowerSubsystem/PowerSupplies"
                }
            })))
            .mount(server)
            .await;

        let members: Vec<Value> = (1..=psu_powers.len())
            .map(|idx| {
                serde_json::json!({
                    "@odata.id": format!(
                        "/redfish/v1/Chassis/chassis/PowerSubsystem/PowerSupplies/{idx}"
                    )
                })
            })
            .collect();
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/chassis/PowerSubsystem/PowerSupplies",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": members
            })))
            .mount(server)
            .await;

        for (offset, power) in psu_powers.iter().enumerate() {
            let idx = offset + 1;
            let psu_path =
                format!("/redfish/v1/Chassis/chassis/PowerSubsystem/PowerSupplies/{idx}");
            Mock::given(method("GET"))
                .and(path(psu_path))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(delta_psu_json(Value::Bool(*power))),
                )
                .mount(server)
                .await;
        }
    }

    async fn mount_delta_power_shelf_tree(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path(DELTA_POWER_SHELVES_COLLECTION_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [
                    { "@odata.id": "/redfish/v1/PowerEquipment/PowerShelves/1" }
                ]
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/PowerEquipment/PowerShelves/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "@odata.id": "/redfish/v1/PowerEquipment/PowerShelves/1",
                "EquipmentType": "PowerShelf",
                "Manufacturer": "DELTA",
                "Oem": {
                    "deltaenergysystems": {
                        "Actions": {
                            "#PowerShelf.TurnOnPSUs": {
                                "target": "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOnPSUs"
                            },
                            "#PowerShelf.TurnOffPSUs": {
                                "target": "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOffPSUs"
                            }
                        }
                    }
                }
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn discover_member_url_prefers_match_and_skips_failed_members() {
        const COLLECTION_ENDPOINT: &str = "/redfish/v1/TestMembers";
        const FALLBACK_ENDPOINT: &str = "/redfish/v1/TestMembers/fallback";
        const FAILED_ENDPOINT: &str = "/redfish/v1/TestMembers/failed";
        const MATCH_ENDPOINT: &str = "/redfish/v1/TestMembers/match";

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(COLLECTION_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [
                    { "@odata.id": FALLBACK_ENDPOINT },
                    { "@odata.id": FAILED_ENDPOINT },
                    { "@odata.id": MATCH_ENDPOINT }
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(FALLBACK_ENDPOINT))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "Name": "fallback" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(FAILED_ENDPOINT))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(MATCH_ENDPOINT))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "Name": "match" })),
            )
            .mount(&server)
            .await;
        let redfish = crate::transport::redfish_client::redfish_client_for_test(&server.uri());

        let discovered = discover_member_url(
            &redfish,
            COLLECTION_ENDPOINT,
            |resource| resource.name.as_deref() == Some("match"),
            |resource| resource.name.as_deref() == Some("fallback"),
            "test member not found",
            "failed to inspect test member",
        )
        .await
        .unwrap();

        assert_eq!(discovered, MATCH_ENDPOINT);
    }

    #[tokio::test]
    async fn discover_member_url_returns_fallback_without_match() {
        const COLLECTION_ENDPOINT: &str = "/redfish/v1/TestMembers";
        const FALLBACK_ENDPOINT: &str = "/redfish/v1/TestMembers/fallback";

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(COLLECTION_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{ "@odata.id": FALLBACK_ENDPOINT }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(FALLBACK_ENDPOINT))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "Name": "fallback" })),
            )
            .mount(&server)
            .await;
        let redfish = crate::transport::redfish_client::redfish_client_for_test(&server.uri());

        let discovered = discover_member_url(
            &redfish,
            COLLECTION_ENDPOINT,
            |_| false,
            |resource| resource.name.as_deref() == Some("fallback"),
            "test member not found",
            "failed to inspect test member",
        )
        .await
        .unwrap();

        assert_eq!(discovered, FALLBACK_ENDPOINT);
    }

    fn delta_shelf_with_action_targets(on: &str, off: &str) -> DeltaResource {
        parse(serde_json::json!({
            "Oem": {
                "deltaenergysystems": {
                    "Actions": {
                        "#PowerShelf.TurnOnPSUs": { "target": on },
                        "#PowerShelf.TurnOffPSUs": { "target": off }
                    }
                }
            }
        }))
    }

    #[test]
    fn power_shelf_has_delta_actions_requires_origin_relative_targets() {
        let relative = "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOnPSUs";
        let off = "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOffPSUs";

        assert!(power_shelf_has_delta_actions(
            &delta_shelf_with_action_targets(relative, off)
        ));
        assert!(!power_shelf_has_delta_actions(
            &delta_shelf_with_action_targets("https://evil.example/TurnOn", off)
        ));
        assert!(!power_shelf_has_delta_actions(
            &delta_shelf_with_action_targets(relative, "//evil.example/TurnOff")
        ));
        assert!(!power_shelf_has_delta_actions(
            &delta_shelf_with_action_targets("PowerShelf.TurnOnPSUs", off)
        ));
    }

    #[test]
    fn delta_chassis_decodes_null_action_shapes() {
        for actions in [
            Value::Null,
            serde_json::json!({ "Oem": null }),
            serde_json::json!({ "#Chassis.Reset": null }),
        ] {
            let chassis: DeltaResource = parse(serde_json::json!({
                "Manufacturer": "DELTA",
                "PowerSubsystem": { "@odata.id": "/redfish/v1/Chassis/chassis/PowerSubsystem" },
                "Actions": actions,
            }));

            assert!(
                chassis_resource_looks_like_delta_powershelf(&chassis),
                "{actions}"
            );
            assert!(!chassis_resource_has_delta_reset(&chassis), "{actions}");
        }
    }

    #[tokio::test]
    async fn discover_member_url_skips_power_shelf_with_absolute_action_targets() {
        const COLLECTION_ENDPOINT: &str = "/redfish/v1/PowerEquipment/PowerShelves";
        const ABSOLUTE_ENDPOINT: &str = "/redfish/v1/PowerEquipment/PowerShelves/absolute";
        const RELATIVE_ENDPOINT: &str = "/redfish/v1/PowerEquipment/PowerShelves/relative";
        let on = "/redfish/v1/PowerEquipment/PowerShelves/relative/Actions/TurnOn";
        let off = "/redfish/v1/PowerEquipment/PowerShelves/relative/Actions/TurnOff";

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(COLLECTION_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [
                    { "@odata.id": ABSOLUTE_ENDPOINT },
                    { "@odata.id": RELATIVE_ENDPOINT }
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(ABSOLUTE_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Oem": {
                    "deltaenergysystems": {
                        "Actions": {
                            "#PowerShelf.TurnOnPSUs": {
                                "target": "https://evil.example/TurnOn"
                            },
                            "#PowerShelf.TurnOffPSUs": {
                                "target": "https://evil.example/TurnOff"
                            }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(RELATIVE_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Oem": {
                    "deltaenergysystems": {
                        "Actions": {
                            "#PowerShelf.TurnOnPSUs": { "target": on },
                            "#PowerShelf.TurnOffPSUs": { "target": off }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;
        let redfish = crate::transport::redfish_client::redfish_client_for_test(&server.uri());

        let discovered = discover_member_url(
            &redfish,
            COLLECTION_ENDPOINT,
            power_shelf_has_delta_actions,
            |_| false,
            "no actionable Delta powershelf found",
            "failed to inspect test member",
        )
        .await
        .unwrap();

        assert_eq!(discovered, RELATIVE_ENDPOINT);
    }

    #[test]
    fn from_config_reports_delta_type() {
        let ps = PowershelfGb200Delta::from_config(
            &test_config(NodeType::PowershelfGb200Delta),
            "rack-01",
        )
        .unwrap();

        assert_eq!(ps.node_type(), NodeType::PowershelfGb200Delta);
        assert_eq!(
            ps.get_info()["type"],
            NodeType::PowershelfGb200Delta.as_str()
        );
    }

    #[test]
    fn replace_redfish_for_test_clears_cached_resource_urls() {
        let mut powershelf = PowershelfGb200Delta::from_config(
            &test_config(NodeType::PowershelfGb200Delta),
            "rack-01",
        )
        .unwrap();
        powershelf
            .chassis_url
            .set("/redfish/v1/Chassis/old".to_owned())
            .unwrap();
        powershelf
            .power_shelf_url
            .set("/redfish/v1/PowerEquipment/PowerShelves/old".to_owned())
            .unwrap();

        powershelf.replace_redfish_for_test("http://127.0.0.1:12345");

        assert!(powershelf.chassis_url.get().is_none());
        assert!(powershelf.power_shelf_url.get().is_none());
    }

    #[test]
    fn nvfwupd_target_config_uses_powershelf_server_type() {
        let ps = PowershelfGb200Delta::from_config(
            &test_config(NodeType::PowershelfGb200Delta),
            "rack-01",
        )
        .unwrap();
        let config = ps.nvfwupd_target_config();

        assert_eq!(config.ip, "10.0.0.5");
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "pass");
        assert_eq!(config.port, Some(8443));
        assert_eq!(
            config.server_type,
            nvfwupd::workflow::ServerType::PowerShelf
        );
        assert!(!config.verify_tls);
    }

    #[test]
    fn decode_delta_psu_power_state_accepts_oem_bool() {
        assert_eq!(
            decode_delta_psu_power_state(&delta_psu(Value::Bool(true))),
            Some(true)
        );
        assert_eq!(
            decode_delta_psu_power_state(&delta_psu(Value::Bool(false))),
            Some(false)
        );
    }

    #[test]
    fn decode_delta_psu_power_state_returns_none_for_invalid_oem_state() {
        assert_eq!(
            decode_delta_psu_power_state(&delta_psu(serde_json::json!("PoweringOn"))),
            None
        );
        assert_eq!(
            decode_delta_psu_power_state(&parse(serde_json::json!({}))),
            None
        );
    }

    #[test]
    fn decode_delta_psu_power_state_falls_back_to_top_level_power_state() {
        assert_eq!(
            decode_delta_psu_power_state(&parse(serde_json::json!({"PowerState": "On"}))),
            Some(true)
        );
        assert_eq!(
            decode_delta_psu_power_state(&parse(serde_json::json!({"PowerState": "Off"}))),
            Some(false)
        );
    }

    #[tokio::test]
    async fn get_power_state_supports_gb200_delta_oem_tree() {
        let server = MockServer::start().await;
        mount_delta_power_state_tree(&server, &[true, true, true, true, true, true]).await;
        let powershelf = test_node(&server.uri());

        assert_eq!(powershelf.get_power_state().await.unwrap(), PowerState::On);
    }

    #[tokio::test]
    async fn get_power_state_returns_off_when_any_delta_psu_is_off() {
        let server = MockServer::start().await;
        mount_delta_power_state_tree(&server, &[true, true, false, true, true, true]).await;
        let powershelf = test_node(&server.uri());

        assert_eq!(powershelf.get_power_state().await.unwrap(), PowerState::Off);
    }

    #[tokio::test]
    async fn set_power_state_posts_delta_turn_on_actions() {
        let server = MockServer::start().await;
        mount_delta_power_shelf_tree(&server).await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOnPSUs",
            ))
            .and(body_json(serde_json::json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(2)
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        powershelf
            .set_power_state(PowerOp::On, PowerTargetType::System)
            .await
            .unwrap();
        powershelf
            .set_power_state(PowerOp::ForceOn, PowerTargetType::System)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn set_power_state_posts_delta_turn_off_actions() {
        let server = MockServer::start().await;
        mount_delta_power_shelf_tree(&server).await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOffPSUs",
            ))
            .and(body_json(serde_json::json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(3)
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        // Graceful requests are served as ForceOff because Delta has no
        // graceful power off.
        for op in [PowerOp::Off, PowerOp::GracefulShutdown, PowerOp::ForceOff] {
            powershelf
                .set_power_state(op, PowerTargetType::System)
                .await
                .unwrap_or_else(|err| panic!("{op:?}: {err}"));
        }
    }

    #[tokio::test]
    async fn set_power_state_rejects_unsupported_operations() {
        let ps = PowershelfGb200Delta::from_config(
            &test_config(NodeType::PowershelfGb200Delta),
            "rack-01",
        )
        .unwrap();

        for op in [
            PowerOp::PowerCycle,
            PowerOp::GracefulRestart,
            PowerOp::ForceRestart,
            PowerOp::Nmi,
        ] {
            let err = ps
                .set_power_state(op, PowerTargetType::System)
                .await
                .unwrap_err();

            assert_eq!(err.code, ErrorCode::InvalidArgument, "{op:?}");
            assert!(err.message.contains("not supported for powershelf"));
        }
    }
}
