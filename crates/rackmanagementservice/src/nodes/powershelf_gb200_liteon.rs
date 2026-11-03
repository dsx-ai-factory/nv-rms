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
use serde_json::Value;

use crate::domain::node::*;
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::nodes::redfish_action_info::{
    ActionInfoResource, ChassisResetRequest, EmptyActionRequest, ResetType, reset_allowable_values,
    to_action_payload,
};
use crate::transport::redfish_client::{
    ODataLink, RedfishClient, ResourceCollection, redfish_client_from_host,
    redfish_client_from_powershelf_endpoint,
};
use crate::utilities::error::{Result, RmsError};

/// Concrete node for LiteOn GB200 powershelf (PDB) BMCs (Redfish).
///
/// Power:     GET /redfish/v1/Chassis/PowerShelf_0;
///            POST /redfish/v1/Chassis/powershelf/Actions/Chassis.{ForceOff,On}
///
/// Firmware inventory/update/activation is delegated to NVFWUPD. Redfish
/// helpers remain for power and legacy task-id paths.
pub struct PowershelfGb200Liteon {
    id: String,
    rack_id: String,
    node_type: NodeType,
    bmc_endpoint: EndpointConfig,
    expected_inventory: Option<ExpectedInventoryPolicy>,
    redfish: RedfishClient,

    // Serializes full node operations across shared Redfish/NVFWUPD clients.
    // The guard is held across awaits so BMC operations cannot interleave.
    op_lock: tokio::sync::Mutex<()>,

    /// Cached chassis resource URL. Discovered lazily on first use by
    /// enumerating `/redfish/v1/Chassis`.
    chassis_url: tokio::sync::OnceCell<String>,
}

const RMS_CHASSIS_COLLECTION_ENDPOINT: &str = "/redfish/v1/Chassis";
const RMS_CHASSIS_RESET_ACTION: &str = "#Chassis.Reset";

/// High-level power intent the powershelf supports.
///
/// `Off` is a graceful shutdown request. Some shelves (for example the VR NVL72
/// LiteOn PF-1114-1RD) expose no graceful off at all, only `ForceOff`. RMS hides
/// that hardware gap from callers: when the chassis advertises no graceful
/// path, `Off` is carried out as `ForceOff`. The fallback is capability-based
/// only. A graceful action the chassis does advertise but that fails at
/// runtime returns its error and is never retried as a hard cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChassisIntent {
    On,
    /// Graceful shutdown (`#Chassis.Off` / `ResetType: GracefulShutdown`).
    Off,
    /// Immediate hard power cut (`#Chassis.ForceOff` / `ResetType: ForceOff`).
    ForceOff,
}

impl ChassisIntent {
    /// Preferred vendor-specific action key (e.g. `#Chassis.ForceOff`). Tried
    /// first; absence triggers the `#Chassis.Reset` fallback.
    fn direct_action_key(&self) -> &'static str {
        match self {
            ChassisIntent::On => "#Chassis.On",
            ChassisIntent::Off => "#Chassis.Off",
            ChassisIntent::ForceOff => "#Chassis.ForceOff",
        }
    }
}

/// Maps an intent to the `ResetType` used with `#Chassis.Reset`.
impl From<ChassisIntent> for ResetType {
    fn from(intent: ChassisIntent) -> Self {
        match intent {
            ChassisIntent::On => Self::On,
            ChassisIntent::Off => Self::GracefulShutdown,
            ChassisIntent::ForceOff => Self::ForceOff,
        }
    }
}

/// Returns `Unimplemented` unless `node_type` is `PowershelfGb200Liteon`.
fn validate_powershelf_gb200_liteon_node_type(node_type: NodeType) -> Result<()> {
    if node_type != NodeType::PowershelfGb200Liteon {
        return Err(RmsError::unimplemented("create_node", node_type.as_str()));
    }
    Ok(())
}

impl PowershelfGb200Liteon {
    /// Builds a LiteOn GB200 shelf node from explicit BMC connection details.
    ///
    /// The node has no expected-inventory policy.
    ///
    /// # Errors
    ///
    /// Returns any error from building the Redfish client.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        rack_id: String,
        host: String,
        port: u16,
        username: &str,
        password: &str,
        mac_address: String,
        dangerously_accept_invalid_certs: bool,
    ) -> Result<Self> {
        let redfish = redfish_client_from_host(
            &host,
            port,
            username,
            password,
            dangerously_accept_invalid_certs,
            true,
        )?;
        Ok(Self {
            id,
            rack_id,
            node_type: NodeType::PowershelfGb200Liteon,
            bmc_endpoint: EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: host,
                    mac_address,
                    port,
                    host_name: None,
                },
                Some(EndpointCredentials::new(username, password)),
                dangerously_accept_invalid_certs,
            ),
            expected_inventory: None,
            redfish,
            op_lock: tokio::sync::Mutex::new(()),
            chassis_url: tokio::sync::OnceCell::new(),
        })
    }

    /// Builds a LiteOn GB200 shelf node from its node configuration.
    ///
    /// # Errors
    ///
    /// Returns `Unimplemented` if `config.node_type` is not
    /// `PowershelfGb200Liteon`, `InvalidArgument` if no BMC endpoint is
    /// configured, or any error from building the Redfish client.
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        validate_powershelf_gb200_liteon_node_type(config.node_type)?;
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
            op_lock: tokio::sync::Mutex::new(()),
            chassis_url: tokio::sync::OnceCell::new(),
        })
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

    /// Lazily resolves and caches the powershelf chassis URL by enumerating
    /// `/redfish/v1/Chassis` and picking the member whose Id starts with
    /// `powershelf` (case-insensitive). This handles both LiteOn
    /// (`/Chassis/powershelf`) and NVIDIA PMC (`/Chassis/PowerShelf_0`).
    /// A failed discovery is not cached.
    async fn resolve_chassis_url(&self, redfish: &RedfishClient) -> Result<&str> {
        self.chassis_url
            .get_or_try_init(|| discover_chassis_url(redfish))
            .await
            .map(String::as_str)
    }

    /// Points the node at a test Redfish server and clears the cached chassis
    /// URL.
    #[cfg(test)]
    pub(crate) fn replace_redfish_for_test(&mut self, base_url: &str) {
        self.redfish = crate::transport::redfish_client::redfish_client_for_test(base_url);
        self.chassis_url = tokio::sync::OnceCell::new();
    }
}

#[async_trait]
impl Node for PowershelfGb200Liteon {
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

    /// Powershelf has no single chassis-wide PowerState we can trust. We model
    /// the shelf as `Off` as soon as any PSU reports `PowerState: "Off"` and
    /// `On` only when every PSU reports `On`. Any single failed PSU is treated
    /// as the shelf being off, so callers see the worst-case state rather than
    /// masking a degraded shelf behind a healthy peer.
    async fn get_power_state(&self) -> Result<PowerState> {
        let _op_guard = self.op_lock.lock().await;
        let redfish = &self.redfish;

        tracing::debug!(node = %self.id, "get_power_state");
        let chassis_url = self.resolve_chassis_url(redfish).await?;
        let chassis: ChassisResource = redfish_get(redfish, chassis_url).await?;
        let psu_states = fetch_psu_power_states(redfish, &chassis).await;
        let state = aggregate_psu_power_states(&psu_states);
        tracing::debug!(
            node = %self.id,
            ?state,
            psu_count = psu_states.len(),
            "get_power_state: aggregated from PowerSupplies"
        );
        Ok(state)
    }

    /// Powers the shelf on or off through its chassis actions.
    ///
    /// `On` and `ForceOn` power on, and `ForceOff` cuts power. `Off` and
    /// `GracefulShutdown` use a graceful path when the chassis advertises one,
    /// and otherwise fall back to `ForceOff`. `target` is ignored.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for `PowerCycle`, `GracefulRestart`,
    /// `ForceRestart`, and `Nmi`, and fails if the chassis advertises no usable
    /// action for the request.
    async fn set_power_state(&self, op: PowerOp, _target: PowerTargetType) -> Result<()> {
        tracing::info!(node = %self.id, ?op, "set_power_state");
        let intent = match op {
            PowerOp::On | PowerOp::ForceOn => ChassisIntent::On,
            // Graceful requests prefer a graceful path. When the shelf
            // advertises none, invoke_chassis_action serves them as ForceOff.
            PowerOp::Off | PowerOp::GracefulShutdown => ChassisIntent::Off,
            PowerOp::ForceOff => ChassisIntent::ForceOff,
            PowerOp::PowerCycle
            | PowerOp::GracefulRestart
            | PowerOp::ForceRestart
            | PowerOp::Nmi => {
                return Err(RmsError::invalid_argument(format!(
                    "{op:?} is not supported for powershelf power control"
                )));
            }
        };

        let _op_guard = self.op_lock.lock().await;
        let redfish = &self.redfish;
        let url = self.resolve_chassis_url(redfish).await?;
        let request = PowerRequest { node: &self.id, op };
        let outcome = invoke_chassis_action(redfish, url, intent, request).await?;
        // The hard-cut warning is logged by `try_invoke_intent` before the
        // fallback is sent, so it exists even if the BMC rejects it. This
        // records that the BMC accepted it.
        if let ChassisActionOutcome::FellBackToForceOff { action } = outcome {
            tracing::info!(
                node = %self.id,
                ?op,
                action,
                "powershelf accepted the ForceOff fallback for a graceful power off"
            );
        }
        Ok(())
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let _op_guard = self.op_lock.lock().await;

        tracing::debug!(node = %self.id, "get_firmware_inventory");
        let inventory = nvfwupd::workflow_api::get_firmware_inventory(self.nvfwupd_target_config())
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        let inventory: Vec<FirmwareInfo> = inventory
            .into_iter()
            .map(nvfwupd_adapter::to_firmware_info)
            .collect();
        tracing::info!(node = %self.id, count = inventory.len(), "get_firmware_inventory");
        Ok(inventory)
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

        tracing::info!(
            node = %self.id,
            component = "",
            file = %target.firmware_file,
            "update_firmware via NVFWUPD"
        );
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

        tracing::debug!(node = %self.id, task_id, "poll_firmware_task via NVFWUPD");
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

        tracing::info!(node = %self.id, "activate_firmware via NVFWUPD");
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

// ── Helper functions ──

/// Discovered Redfish action links for a single action entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActionLinks {
    target: String,
    action_info: String,
}

/// Fields RMS reads from the shelf chassis: its advertised actions and its
/// `PowerSubsystem` link.
#[derive(Debug, Clone, Default, Deserialize)]
struct ChassisResource {
    #[serde(rename = "Actions", default)]
    actions: HashMap<String, ChassisAction>,
    #[serde(rename = "PowerSubsystem")]
    power_subsystem: Option<ODataLink>,
}

/// One advertised chassis action: its POST target and optional ActionInfo link.
#[derive(Debug, Clone, Default, Deserialize)]
struct ChassisAction {
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

/// One PSU, read only for its `PowerState`.
#[derive(Debug, Clone, Default, Deserialize)]
struct PowerSupplyResource {
    #[serde(rename = "PowerState")]
    power_state: Option<Value>,
}

/// Fetches and decodes the Redfish resource at `path`.
async fn redfish_get<T>(redfish: &RedfishClient, path: &str) -> Result<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    redfish.get_resource(path).await.map_err(RmsError::from)
}

/// How [`invoke_chassis_action`] satisfied a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChassisActionOutcome {
    /// The requested intent was posted.
    AsRequested,
    /// A graceful `Off` was served as `ForceOff` because the chassis
    /// advertises no graceful path. `action` is the chassis action posted.
    FellBackToForceOff { action: &'static str },
}

/// The power request being served. Carried down to the point where a graceful
/// `Off` is turned into a `ForceOff`, so that decision is logged with the node
/// and operation before anything is sent.
#[derive(Debug, Clone, Copy)]
struct PowerRequest<'a> {
    node: &'a str,
    op: PowerOp,
}

/// Warns that RMS is about to hard-cut a shelf that was asked to power off
/// gracefully. `action` is the chassis action about to be posted.
fn warn_hard_cut_fallback(request: PowerRequest<'_>, action: &str) {
    tracing::warn!(
        node = request.node,
        op = ?request.op,
        action,
        "powershelf advertises no graceful power off; serving the request as ForceOff"
    );
}

/// Result of trying to post one intent against the chassis.
enum IntentAttempt {
    /// The action was posted and accepted by the BMC.
    Posted { action: &'static str },
    /// The chassis does not advertise a usable path for the intent. Nothing
    /// was sent. Carries the error to return if no fallback applies.
    Unavailable(RmsError),
}

/// Posts `intent`, serving a graceful `Off` as `ForceOff` when the chassis
/// advertises no graceful path.
///
/// Fallback is capability-based only. A graceful action that is advertised but
/// fails (HTTP error, malformed action entry, unreadable ActionInfo) is
/// returned as an error and is never retried as a hard cut.
///
/// `request` identifies the caller's request. When a graceful `Off` becomes a
/// `ForceOff`, a warning naming it is logged before the `ForceOff` is sent.
async fn invoke_chassis_action(
    redfish: &RedfishClient,
    chassis_url: &str,
    intent: ChassisIntent,
    request: PowerRequest<'_>,
) -> Result<ChassisActionOutcome> {
    let chassis: ChassisResource = redfish_get(redfish, chassis_url).await?;

    let graceful_error = match try_invoke_intent(redfish, &chassis, intent, None).await? {
        IntentAttempt::Posted { .. } => return Ok(ChassisActionOutcome::AsRequested),
        IntentAttempt::Unavailable(error) => error,
    };
    if intent != ChassisIntent::Off {
        return Err(graceful_error);
    }

    let fallback = try_invoke_intent(redfish, &chassis, ChassisIntent::ForceOff, Some(request))
        .await
        .map_err(|error| {
            RmsError::new(
                error.code,
                format!(
                    "powershelf advertises no graceful power off; the ForceOff fallback failed: {}",
                    error.message
                ),
            )
        })?;
    match fallback {
        IntentAttempt::Posted { action } => Ok(ChassisActionOutcome::FellBackToForceOff { action }),
        IntentAttempt::Unavailable(force_error) => {
            let mut advertised: Vec<&str> = chassis.actions.keys().map(String::as_str).collect();
            advertised.sort_unstable();
            Err(RmsError::failed_precondition(format!(
                "powershelf advertises neither a graceful power off nor a ForceOff \
                 (graceful: {}; ForceOff: {}); advertised chassis actions: {advertised:?}",
                graceful_error.message, force_error.message
            )))
        }
    }
}

/// Posts one intent, preferring its direct chassis action and falling back to
/// `#Chassis.Reset` with the matching `ResetType`.
///
/// Returns [`IntentAttempt::Unavailable`] without sending anything when the
/// chassis advertises no usable path for the intent.
///
/// `hard_cut_for` is `Some` only when `intent` is a `ForceOff` standing in for
/// a graceful request. The hard-cut warning is logged just before the POST, so
/// it is present even when the BMC then rejects the action.
async fn try_invoke_intent(
    redfish: &RedfishClient,
    chassis: &ChassisResource,
    intent: ChassisIntent,
    hard_cut_for: Option<PowerRequest<'_>>,
) -> Result<IntentAttempt> {
    if let Some(links) = find_action_links(chassis, intent.direct_action_key())? {
        let payload = if links.action_info.is_empty() {
            to_action_payload(&EmptyActionRequest {})?
        } else {
            let info: ActionInfoResource = redfish_get(redfish, &links.action_info).await?;
            build_action_payload(&info)
        };
        tracing::debug!(
            action = intent.direct_action_key(),
            target = %links.target,
            payload = %payload,
            "invoke_chassis_action: direct action"
        );
        if let Some(request) = hard_cut_for {
            warn_hard_cut_fallback(request, intent.direct_action_key());
        }
        redfish
            .post_action(&links.target, &payload)
            .await
            .map_err(RmsError::from)?;
        return Ok(IntentAttempt::Posted {
            action: intent.direct_action_key(),
        });
    }

    let Some(reset) = find_action_links(chassis, RMS_CHASSIS_RESET_ACTION)? else {
        return Ok(IntentAttempt::Unavailable(RmsError::internal(format!(
            "powershelf chassis Actions expose neither {} nor {RMS_CHASSIS_RESET_ACTION}",
            intent.direct_action_key()
        ))));
    };

    // When the BMC advertises AllowableValues for ResetType, honor them: sending
    // a reset the device says it does not support risks leaving the shelf in an
    // indeterminate state. Treat it as unavailable rather than "attempt anyway".
    // If no AllowableValues are advertised we cannot verify, so we let the BMC
    // decide.
    let reset_type = ResetType::from(intent);
    if !reset.action_info.is_empty() {
        let info: ActionInfoResource = redfish_get(redfish, &reset.action_info).await?;
        if let Some(allowed) = reset_allowable_values(&info)
            && !reset_type.is_in(&allowed)
        {
            return Ok(IntentAttempt::Unavailable(RmsError::failed_precondition(
                format!(
                    "#Chassis.Reset ResetType {:?} is not in AllowableValues {:?}; \
                     refusing to send an unsupported reset",
                    reset_type.as_str(),
                    allowed
                ),
            )));
        }
    }

    let payload = to_action_payload(&ChassisResetRequest { reset_type })?;
    tracing::debug!(
        action = RMS_CHASSIS_RESET_ACTION,
        target = %reset.target,
        payload = %payload,
        "invoke_chassis_action: #Chassis.Reset fallback"
    );
    if let Some(request) = hard_cut_for {
        warn_hard_cut_fallback(request, RMS_CHASSIS_RESET_ACTION);
    }
    redfish
        .post_action(&reset.target, &payload)
        .await
        .map_err(RmsError::from)?;
    Ok(IntentAttempt::Posted {
        action: RMS_CHASSIS_RESET_ACTION,
    })
}

/// Finds the shelf chassis in `/redfish/v1/Chassis`: the first member whose
/// last path segment starts with `powershelf`, ignoring ASCII case.
///
/// Logs a warning if more than one member matches.
///
/// # Errors
///
/// Returns `Internal` if no member matches.
async fn discover_chassis_url(redfish: &RedfishClient) -> Result<String> {
    let collection: ResourceCollection =
        redfish_get(redfish, RMS_CHASSIS_COLLECTION_ENDPOINT).await?;
    let matches: Vec<&str> = collection
        .members
        .iter()
        .filter_map(|member| member.nonempty())
        .filter(|uri| {
            uri.rsplit('/')
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .starts_with("powershelf")
        })
        .collect();

    let Some(first) = matches.first() else {
        return Err(RmsError::internal(format!(
            "no powershelf chassis found in {RMS_CHASSIS_COLLECTION_ENDPOINT}"
        )));
    };

    // Each shelf is expected to be fronted by its own BMC, so this collection
    // should contain exactly one powershelf chassis. If an aggregating Redfish
    // endpoint ever exposes more than one we cannot tell which physical shelf
    // the caller means; log the ambiguity and proceed with the first match
    // rather than failing.
    if matches.len() > 1 {
        tracing::warn!(
            endpoint = RMS_CHASSIS_COLLECTION_ENDPOINT,
            candidates = ?matches,
            chosen = %first,
            "multiple powershelf chassis found; using the first match"
        );
    }

    Ok((*first).to_owned())
}

/// Returns `Ok(None)` when the action key is absent (caller can fall back),
/// `Ok(Some(_))` when well-formed, `Err(_)` when present but malformed.
fn find_action_links(chassis: &ChassisResource, action: &str) -> Result<Option<ActionLinks>> {
    let Some(act) = chassis.actions.get(action) else {
        return Ok(None);
    };

    let target = act.target.as_deref().unwrap_or_default();
    if target.is_empty() {
        return Err(RmsError::internal(format!(
            "missing {action} target in powershelf chassis Actions"
        )));
    }

    let action_info = act.action_info.clone().unwrap_or_default();

    Ok(Some(ActionLinks {
        target: target.to_owned(),
        action_info,
    }))
}

/// Selects each parameter's `DefaultValue`, else the first `AllowableValues`
/// entry. Parameters with neither are omitted so the BMC's defaults stand.
fn build_action_payload(action_info: &ActionInfoResource) -> Value {
    let mut obj = serde_json::Map::new();

    for parameter in &action_info.parameters {
        let Some(name) = parameter.name.as_deref() else {
            continue;
        };
        if let Some(default) = parameter.default_value.as_ref() {
            obj.insert(name.to_owned(), default.clone());
            continue;
        }
        if let Some(first) = parameter
            .allowable_values
            .as_ref()
            .and_then(|values| values.first())
        {
            obj.insert(name.to_owned(), first.clone());
        }
    }
    Value::Object(obj)
}

/// Walks a powershelf chassis JSON down to its PSU members and returns the
/// `PowerState` each PSU reports, decoded into a tri-state: `Some(true)` =
/// energized, `Some(false)` = off, `None` = field absent, unrecognized, or
/// unreadable. The returned vector has one entry per collection member, in
/// order. Unreadable members must be recorded as `None` so they cannot be
/// dropped from the shelf-level aggregate.
async fn fetch_psu_power_states(
    redfish: &RedfishClient,
    chassis: &ChassisResource,
) -> Vec<Option<bool>> {
    let Some(subsystem_url) = power_supplies_url(chassis) else {
        tracing::warn!("powershelf chassis exposes no PowerSubsystem.PowerSupplies link");
        return Vec::new();
    };

    let subsystem: PowerSubsystemResource = match redfish_get(redfish, subsystem_url).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                url = %subsystem_url,
                error = %e.message,
                "failed to fetch PowerSubsystem"
            );
            return Vec::new();
        }
    };

    let Some(supplies) = subsystem
        .power_supplies
        .as_ref()
        .and_then(ODataLink::nonempty)
        .map(str::to_owned)
    else {
        tracing::warn!(
            url = %subsystem_url,
            "PowerSubsystem has no PowerSupplies link"
        );
        return Vec::new();
    };

    let collection: ResourceCollection = match redfish_get(redfish, &supplies).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                url = %supplies,
                error = %e.message,
                "failed to fetch PowerSupplies collection"
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
            Ok(psu) => out.push(decode_psu_power_state(psu.power_state.as_ref())),
            Err(e) => {
                tracing::warn!(uri, error = %e.message, "failed to fetch PowerSupply");
                out.push(None);
            }
        }
    }
    out
}

/// Decode a PSU `PowerState` field into a tri-state. Real hardware is
/// inconsistent about both type and spelling:
/// - spec-compliant BMCs (e.g. NVIDIA PMC) report the Redfish enum string
///   `"On"` / `"Off"`,
/// - LiteOn power shelves report a JSON boolean (`true` / `false`),
/// - some firmware revisions stringify that boolean as `"true"` / `"false"`.
///
/// All three carry the same semantics on the wire, so the client accepts any
/// of them (string comparison is case-insensitive). Anything else — `null`, a
/// number, or an unrecognized enum value like `"PoweringOn"` — degrades to
/// `None` and contributes no signal to the shelf-level aggregate.
fn decode_psu_power_state(field: Option<&Value>) -> Option<bool> {
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

/// Returns the chassis `PowerSubsystem` link, the first hop toward its PSUs,
/// or `None` if it is missing or empty.
fn power_supplies_url(chassis: &ChassisResource) -> Option<&str> {
    chassis
        .power_subsystem
        .as_ref()
        .and_then(ODataLink::nonempty)
}

/// Collapses per-PSU `PowerState` readings into a single shelf-level state:
/// - any PSU reading `Some(false)` → `Off` (a single dead PSU drops the shelf),
/// - else if every PSU reports `Some(true)` → `On`,
/// - else (empty, missing readings, or mixed known/unknown) → `Unknown`.
fn aggregate_psu_power_states(states: &[Option<bool>]) -> PowerState {
    if states.is_empty() {
        return PowerState::Unknown;
    }
    let mut all_on = true;
    for s in states {
        match s {
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
    use crate::transport::redfish_client::redfish_client_for_test;
    use crate::utilities::error::ErrorCode;
    use std::sync::{Arc, Mutex};
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn node_type_is_powershelf_gb200_liteon() {
        let ps = PowershelfGb200Liteon::new(
            "ps-01".into(),
            "rack-01".into(),
            "10.0.0.1".into(),
            443,
            "",
            "",
            String::new(),
            false,
        )
        .unwrap();
        assert_eq!(ps.node_type(), NodeType::PowershelfGb200Liteon);
    }

    #[test]
    fn from_config_rejects_powershelf_gb200_delta_node_type() {
        let config = NodeConfig {
            id: "ps-01".into(),
            node_type: NodeType::PowershelfGb200Delta,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.5".into(),
                    mac_address: String::new(),
                    port: 443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "pass")),
                false,
            )),
            host_endpoint: None,
            expected_inventory: None,
            recovery: None,
            runtime_config: None,
        };

        let result = PowershelfGb200Liteon::from_config(&config, "rack-01");

        assert!(matches!(
            result,
            Err(err) if err.code == crate::utilities::error::ErrorCode::Unimplemented
        ));
    }

    #[test]
    fn get_info_no_host_mac_ip() {
        let ps = PowershelfGb200Liteon::new(
            "ps-01".into(),
            "rack-01".into(),
            "10.0.0.5".into(),
            443,
            "admin",
            "pass",
            "aa:bb:cc:dd:ee:ff".into(),
            false,
        )
        .unwrap();

        let info = ps.get_info();
        assert_eq!(info["type"], "powershelf_gb200_liteon");
        assert_eq!(info["host"], "10.0.0.5");
        assert_eq!(info["macAddress"], "aa:bb:cc:dd:ee:ff");
        assert!(!info.contains_key("hostMac_0"));
        assert!(!info.contains_key("hostIp_0"));
    }

    #[test]
    fn nvfwupd_target_config_uses_bmc_credentials_and_default_tls() {
        let ps = PowershelfGb200Liteon::new(
            "ps-01".into(),
            "rack-01".into(),
            "10.0.0.7".into(),
            8443,
            "admin",
            "secret",
            String::new(),
            true,
        )
        .unwrap();

        let config = ps.nvfwupd_target_config();
        assert_eq!(config.ip, "10.0.0.7");
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "secret");
        assert_eq!(config.port, Some(8443));
        assert_eq!(
            config.server_type,
            nvfwupd::workflow::ServerType::PowerShelf
        );
        assert!(!config.verify_tls);
        assert!(!format!("{config:?}").contains("secret"));
    }

    fn parse<T: DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).expect("test fixture JSON")
    }

    fn sample_chassis() -> ChassisResource {
        parse(serde_json::json!({
            "@odata.id": "/redfish/v1/Chassis/powershelf",
            "Actions": {
                "#Chassis.ForceOff": {
                    "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/ForceOffActionInfo",
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
                },
                "#Chassis.On": {
                    "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/OnActionInfo",
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.On"
                }
            }
        }))
    }

    #[test]
    fn chassis_intent_maps_to_direct_key_and_reset_type() {
        assert_eq!(ChassisIntent::On.direct_action_key(), "#Chassis.On");
        assert_eq!(ResetType::from(ChassisIntent::On), ResetType::On);
        assert_eq!(ChassisIntent::Off.direct_action_key(), "#Chassis.Off");
        assert_eq!(
            ResetType::from(ChassisIntent::Off),
            ResetType::GracefulShutdown
        );
        assert_eq!(
            ChassisIntent::ForceOff.direct_action_key(),
            "#Chassis.ForceOff"
        );
        assert_eq!(
            ResetType::from(ChassisIntent::ForceOff),
            ResetType::ForceOff
        );
    }

    #[test]
    fn graceful_off_and_force_off_map_to_distinct_intents() {
        // The two intents stay distinct and each keeps its own direct action.
        // Whether a graceful Off is served as ForceOff is decided at run time
        // in `invoke_chassis_action`, from what the chassis advertises.
        assert_ne!(ChassisIntent::Off, ChassisIntent::ForceOff);
        assert_eq!(
            ChassisIntent::Off.direct_action_key(),
            "#Chassis.Off",
            "graceful Off must use its own action, not ForceOff"
        );
    }

    #[test]
    fn find_action_links_reads_target_and_action_info() {
        let chassis = sample_chassis();

        let force_off = find_action_links(&chassis, ChassisIntent::ForceOff.direct_action_key())
            .unwrap()
            .unwrap();
        assert_eq!(
            force_off.target,
            "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
        );
        assert_eq!(
            force_off.action_info,
            "/redfish/v1/Chassis/powershelf/ForceOffActionInfo"
        );

        let on = find_action_links(&chassis, ChassisIntent::On.direct_action_key())
            .unwrap()
            .unwrap();
        assert_eq!(
            on.target,
            "/redfish/v1/Chassis/powershelf/Actions/Chassis.On"
        );
        assert_eq!(
            on.action_info,
            "/redfish/v1/Chassis/powershelf/OnActionInfo"
        );
    }

    #[test]
    fn find_action_links_missing_action_info_is_ok() {
        let chassis = parse(serde_json::json!({
            "Actions": {
                "#Chassis.ForceOff": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
                }
            }
        }));
        let links = find_action_links(&chassis, ChassisIntent::ForceOff.direct_action_key())
            .unwrap()
            .unwrap();
        assert_eq!(
            links.target,
            "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
        );
        assert!(links.action_info.is_empty());
    }

    #[test]
    fn find_action_links_returns_none_when_absent() {
        let chassis = parse(serde_json::json!({
            "Actions": { "#Chassis.Reset": { "target": "/r" } }
        }));
        assert!(
            find_action_links(&chassis, ChassisIntent::ForceOff.direct_action_key())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn find_action_links_propagates_malformed_entries() {
        let chassis = parse(serde_json::json!({
            "Actions": { "#Chassis.ForceOff": { "@Redfish.ActionInfo": "/x" } }
        }));
        let err =
            find_action_links(&chassis, ChassisIntent::ForceOff.direct_action_key()).unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("target"));
    }

    fn pick_powershelf_uri(collection: &ResourceCollection) -> Option<String> {
        collection.members.iter().find_map(|member| {
            let uri = member.nonempty()?;
            if uri
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .starts_with("powershelf")
            {
                Some(uri.to_owned())
            } else {
                None
            }
        })
    }

    #[test]
    fn discover_chassis_url_picks_pmc_capital_variant() {
        let coll: ResourceCollection = parse(serde_json::json!({
            "Members": [
                { "@odata.id": "/redfish/v1/Chassis/PowerShelf_0" },
                { "@odata.id": "/redfish/v1/Chassis/ERoT_BMC_0" }
            ]
        }));
        assert_eq!(
            pick_powershelf_uri(&coll).as_deref(),
            Some("/redfish/v1/Chassis/PowerShelf_0")
        );
    }

    #[test]
    fn discover_chassis_url_picks_liteon_lowercase_variant() {
        let coll: ResourceCollection = parse(serde_json::json!({
            "Members": [
                { "@odata.id": "/redfish/v1/Chassis/ERoT_BMC_0" },
                { "@odata.id": "/redfish/v1/Chassis/powershelf" }
            ]
        }));
        assert_eq!(
            pick_powershelf_uri(&coll).as_deref(),
            Some("/redfish/v1/Chassis/powershelf")
        );
    }

    #[test]
    fn discover_chassis_url_skips_unrelated_chassis() {
        let coll: ResourceCollection = parse(serde_json::json!({
            "Members": [
                { "@odata.id": "/redfish/v1/Chassis/PowerSubsystem" },
                { "@odata.id": "/redfish/v1/Chassis/ERoT_BMC_0" }
            ]
        }));
        assert_eq!(pick_powershelf_uri(&coll), None);
    }

    #[test]
    fn build_action_payload_uses_first_allowable_value() {
        let info: ActionInfoResource = parse(serde_json::json!({
            "Parameters": [
                {
                    "Name": "ForceOffType",
                    "Required": true,
                    "DataType": "String",
                    "AllowableValues": ["ForceOff"]
                }
            ]
        }));
        assert_eq!(
            build_action_payload(&info),
            serde_json::json!({"ForceOffType": "ForceOff"})
        );
    }

    #[test]
    fn build_action_payload_prefers_default_value() {
        let info: ActionInfoResource = parse(serde_json::json!({
            "Parameters": [
                {
                    "Name": "OnType",
                    "DataType": "String",
                    "DefaultValue": "On",
                    "AllowableValues": ["On", "Soft"]
                }
            ]
        }));
        assert_eq!(
            build_action_payload(&info),
            serde_json::json!({"OnType": "On"})
        );
    }

    #[test]
    fn build_action_payload_skips_unconstrained_parameters() {
        let info: ActionInfoResource = parse(serde_json::json!({
            "Parameters": [
                { "Name": "Optional", "DataType": "String" },
                {
                    "Name": "Required",
                    "DataType": "String",
                    "AllowableValues": ["X"]
                }
            ]
        }));
        assert_eq!(
            build_action_payload(&info),
            serde_json::json!({"Required": "X"})
        );
    }

    #[test]
    fn build_action_payload_no_parameters_is_empty_object() {
        assert_eq!(
            build_action_payload(&parse(serde_json::json!({}))),
            serde_json::json!({})
        );
    }

    #[tokio::test]
    async fn set_power_state_rejects_unsupported_ops() {
        let ps = PowershelfGb200Liteon::new(
            "ps-01".into(),
            "rack-01".into(),
            "10.0.0.1".into(),
            443,
            "",
            "",
            String::new(),
            false,
        )
        .unwrap();

        // Restart-style operations stay unsupported on a power shelf. Graceful
        // power-down (`Off`, `GracefulShutdown`) is supported and falls back to
        // ForceOff when the shelf advertises no graceful path.
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
            assert!(
                err.message.contains("not supported for powershelf"),
                "{op:?}: {}",
                err.message
            );
        }
    }

    #[test]
    fn aggregate_returns_off_when_any_psu_is_off() {
        let states = [Some(true), Some(false), Some(true)];
        assert_eq!(aggregate_psu_power_states(&states), PowerState::Off);
    }

    #[test]
    fn aggregate_returns_off_when_all_psus_report_off() {
        let states = [Some(false), Some(false), Some(false)];
        assert_eq!(aggregate_psu_power_states(&states), PowerState::Off);
    }

    #[test]
    fn aggregate_returns_on_when_all_psus_are_on() {
        let states = [Some(true), Some(true), Some(true)];
        assert_eq!(aggregate_psu_power_states(&states), PowerState::On);
    }

    #[test]
    fn aggregate_returns_on_when_single_psu_is_on() {
        let states = [Some(true)];
        assert_eq!(aggregate_psu_power_states(&states), PowerState::On);
    }

    #[test]
    fn aggregate_returns_unknown_when_no_psus_reported() {
        assert_eq!(aggregate_psu_power_states(&[]), PowerState::Unknown);
    }

    #[test]
    fn aggregate_returns_unknown_when_all_psu_readings_missing() {
        let states = [None, None];
        assert_eq!(aggregate_psu_power_states(&states), PowerState::Unknown);
    }

    #[test]
    fn aggregate_returns_unknown_when_on_reading_is_partial() {
        // We can only commit to `On` when every PSU confirms it; a missing
        // reading next to a known `On` leaves the shelf in an unknown state.
        let states = [Some(true), None];
        assert_eq!(aggregate_psu_power_states(&states), PowerState::Unknown);
    }

    #[test]
    fn aggregate_ignores_missing_but_keeps_off_signal() {
        // One PSU's PowerState couldn't be decoded and another reports Off;
        // the aggregate trusts the known Off reading rather than degrading to
        // Unknown.
        let states = [None, Some(false)];
        assert_eq!(aggregate_psu_power_states(&states), PowerState::Off);
    }

    #[test]
    fn decode_psu_power_state_accepts_liteon_bool() {
        // LiteOn powershelf BMCs report PowerState as a JSON boolean rather
        // than the Redfish enum string.
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!(false))),
            Some(false)
        );
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!(true))),
            Some(true)
        );
    }

    #[test]
    fn decode_psu_power_state_accepts_spec_string() {
        // NVIDIA PMC and other spec-compliant BMCs use the Redfish enum.
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("On"))),
            Some(true)
        );
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("Off"))),
            Some(false)
        );
    }

    #[test]
    fn decode_psu_power_state_accepts_stringified_bool() {
        // Some firmware stringifies the LiteOn boolean.
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("true"))),
            Some(true)
        );
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("false"))),
            Some(false)
        );
    }

    #[test]
    fn decode_psu_power_state_is_case_insensitive_for_strings() {
        // Tolerate spelling drift across vendors and firmware revisions.
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("ON"))),
            Some(true)
        );
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("off"))),
            Some(false)
        );
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("True"))),
            Some(true)
        );
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("FALSE"))),
            Some(false)
        );
    }

    #[test]
    fn decode_psu_power_state_returns_none_for_unknown_shapes() {
        assert_eq!(decode_psu_power_state(None), None);
        assert_eq!(decode_psu_power_state(Some(&serde_json::Value::Null)), None);
        assert_eq!(
            decode_psu_power_state(Some(&serde_json::json!("PoweringOn"))),
            None
        );
        assert_eq!(decode_psu_power_state(Some(&serde_json::json!(1))), None);
        assert_eq!(decode_psu_power_state(Some(&serde_json::json!(0))), None);
        assert_eq!(decode_psu_power_state(Some(&serde_json::json!([]))), None);
        assert_eq!(decode_psu_power_state(Some(&serde_json::json!({}))), None);
    }

    #[test]
    fn power_supplies_url_extracts_subsystem_link() {
        let chassis = parse(serde_json::json!({
            "PowerSubsystem": {
                "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem"
            }
        }));
        let links = power_supplies_url(&chassis).expect("link present");
        assert_eq!(links, "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem");
    }

    #[test]
    fn power_supplies_url_returns_none_when_missing() {
        assert!(power_supplies_url(&parse(serde_json::json!({}))).is_none());
        assert!(
            power_supplies_url(&parse(serde_json::json!({
                "PowerSubsystem": {"@odata.id": ""}
            })))
            .is_none()
        );
    }

    const LITEON_TEST_SUBSYSTEM_PATH: &str = "/redfish/v1/Chassis/powershelf/PowerSubsystem";
    const LITEON_TEST_SUPPLIES_PATH: &str =
        "/redfish/v1/Chassis/powershelf/PowerSubsystem/PowerSupplies";

    /// Minimal chassis whose `PowerSubsystem` link points at the tree mounted
    /// by [`mount_liteon_supplies_collection`].
    fn liteon_test_chassis() -> ChassisResource {
        parse(serde_json::json!({
            "@odata.id": "/redfish/v1/Chassis/powershelf",
            "PowerSubsystem": { "@odata.id": LITEON_TEST_SUBSYSTEM_PATH }
        }))
    }

    /// Mounts `PowerSubsystem` → `PowerSupplies` so a test can drive
    /// `fetch_psu_power_states` with an arbitrary `Members` array.
    async fn mount_liteon_supplies_collection(server: &MockServer, members: Value) {
        Mock::given(method("GET"))
            .and(path(LITEON_TEST_SUBSYSTEM_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "PowerSupplies": { "@odata.id": LITEON_TEST_SUPPLIES_PATH }
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(LITEON_TEST_SUPPLIES_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "Members": members })),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn fetch_psu_power_states_yields_one_entry_per_member_none_on_failure() {
        const PSU_ON: &str = "/redfish/v1/Chassis/powershelf/PowerSubsystem/PowerSupplies/on";
        const PSU_OFF: &str = "/redfish/v1/Chassis/powershelf/PowerSubsystem/PowerSupplies/off";
        const PSU_UNREACHABLE: &str =
            "/redfish/v1/Chassis/powershelf/PowerSubsystem/PowerSupplies/unreachable";

        let server = MockServer::start().await;
        mount_liteon_supplies_collection(
            &server,
            serde_json::json!([
                { "@odata.id": PSU_ON },
                { "@odata.id": PSU_OFF },
                { "@odata.id": PSU_UNREACHABLE },
                { "Name": "no-odata-id" },
                { "@odata.id": "" },
            ]),
        )
        .await;
        Mock::given(method("GET"))
            .and(path(PSU_ON))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "PowerState": "On" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(PSU_OFF))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "PowerState": "Off" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(PSU_UNREACHABLE))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let redfish = redfish_client_for_test(&server.uri());
        let states = fetch_psu_power_states(&redfish, &liteon_test_chassis()).await;

        // One positional entry per member. Regression guard: unreadable PSUs
        // must not be dropped (which would let the readable ones aggregate to
        // a falsely-confident state).
        assert_eq!(states, vec![Some(true), Some(false), None, None, None]);
        assert_eq!(
            aggregate_psu_power_states(&states),
            PowerState::Off,
            "a Some(false) still forces Off even alongside unreadable PSUs"
        );
    }

    #[tokio::test]
    async fn fetch_psu_power_states_unreadable_psu_downgrades_on_to_unknown() {
        const PSU_ON: &str = "/redfish/v1/Chassis/powershelf/PowerSubsystem/PowerSupplies/on";
        const PSU_UNREACHABLE: &str =
            "/redfish/v1/Chassis/powershelf/PowerSubsystem/PowerSupplies/unreachable";

        let server = MockServer::start().await;
        mount_liteon_supplies_collection(
            &server,
            serde_json::json!([
                { "@odata.id": PSU_ON },
                { "@odata.id": PSU_UNREACHABLE },
            ]),
        )
        .await;
        Mock::given(method("GET"))
            .and(path(PSU_ON))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "PowerState": "On" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(PSU_UNREACHABLE))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let redfish = redfish_client_for_test(&server.uri());
        let states = fetch_psu_power_states(&redfish, &liteon_test_chassis()).await;

        assert_eq!(states, vec![Some(true), None]);
        assert_eq!(
            aggregate_psu_power_states(&states),
            PowerState::Unknown,
            "an unreadable PSU must prevent a false On aggregate"
        );
    }

    #[tokio::test]
    async fn fetch_psu_power_states_is_empty_without_power_subsystem() {
        let server = MockServer::start().await;
        let redfish = redfish_client_for_test(&server.uri());
        let chassis = parse(serde_json::json!({
            "@odata.id": "/redfish/v1/Chassis/powershelf"
        }));
        let states = fetch_psu_power_states(&redfish, &chassis).await;

        assert!(states.is_empty());
        assert_eq!(aggregate_psu_power_states(&states), PowerState::Unknown);
    }

    #[tokio::test]
    async fn fetch_psu_power_states_is_empty_without_power_supplies_link() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(LITEON_TEST_SUBSYSTEM_PATH))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "Id": "PowerSubsystem" })),
            )
            .mount(&server)
            .await;

        let redfish = redfish_client_for_test(&server.uri());
        let states = fetch_psu_power_states(&redfish, &liteon_test_chassis()).await;

        assert!(states.is_empty());
        assert_eq!(aggregate_psu_power_states(&states), PowerState::Unknown);
    }

    fn test_node(base_url: &str) -> PowershelfGb200Liteon {
        let mut ps = PowershelfGb200Liteon::new(
            "ps-01".into(),
            "rack-01".into(),
            "10.0.0.5".into(),
            443,
            "admin",
            "pass",
            "aa:bb:cc:dd:ee:ff".into(),
            true,
        )
        .unwrap();
        ps.replace_redfish_for_test(base_url);
        ps
    }

    async fn mount_liteon_chassis_discovery(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path(RMS_CHASSIS_COLLECTION_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [
                    { "@odata.id": "/redfish/v1/Chassis/powershelf" }
                ]
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/powershelf"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "@odata.id": "/redfish/v1/Chassis/powershelf",
                "Id": "powershelf",
                "PowerSubsystem": { "@odata.id": LITEON_TEST_SUBSYSTEM_PATH }
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn get_power_state_reports_unknown_when_a_liteon_psu_is_unreadable() {
        const PSU_ON: &str = "/redfish/v1/Chassis/powershelf/PowerSubsystem/PowerSupplies/1";
        const PSU_UNREACHABLE: &str =
            "/redfish/v1/Chassis/powershelf/PowerSubsystem/PowerSupplies/2";

        let server = MockServer::start().await;
        mount_liteon_chassis_discovery(&server).await;
        mount_liteon_supplies_collection(
            &server,
            serde_json::json!([
                { "@odata.id": PSU_ON },
                { "@odata.id": PSU_UNREACHABLE },
            ]),
        )
        .await;
        Mock::given(method("GET"))
            .and(path(PSU_ON))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "PowerState": "On" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(PSU_UNREACHABLE))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let powershelf = test_node(&server.uri());

        // End-to-end through get_power_state: one readable "on" PSU alongside
        // one unreadable PSU must report Unknown, not On.
        assert_eq!(
            powershelf.get_power_state().await.unwrap(),
            PowerState::Unknown
        );
    }

    const CHASSIS_PATH: &str = "/redfish/v1/Chassis/powershelf";

    fn test_request() -> PowerRequest<'static> {
        PowerRequest {
            node: "ps-01",
            op: PowerOp::Off,
        }
    }

    /// Collects log output written while a [`capture_logs`] guard is alive.
    #[derive(Clone, Default)]
    struct LogCapture(Arc<Mutex<Vec<u8>>>);

    impl LogCapture {
        fn text(&self) -> String {
            let guard = self.0.lock().unwrap();
            String::from_utf8_lossy(&guard).into_owned()
        }
    }

    impl std::io::Write for LogCapture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.0.lock().unwrap();
            std::io::Write::write(&mut *guard, buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Captures this thread's log events until the returned guard is dropped.
    /// `#[tokio::test]` runs on one thread, so events from awaited code land
    /// here.
    fn capture_logs() -> (LogCapture, tracing::subscriber::DefaultGuard) {
        let capture = LogCapture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();
        (capture, guard)
    }

    /// The text of the hard-cut warning logged when a graceful request is
    /// served as `ForceOff`.
    const HARD_CUT_WARNING: &str = "serving the request as ForceOff";

    async fn mount_chassis(server: &MockServer, actions: Value) {
        Mock::given(method("GET"))
            .and(path(CHASSIS_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "Actions": actions })),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn invoke_chassis_action_uses_direct_off_action_for_graceful_off() {
        let server = MockServer::start().await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.Off": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Off" },
                "#Chassis.ForceOff": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff" }
            }),
        )
        .await;
        // The graceful action must be posted, never the ForceOff one.
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Off"))
            .and(body_json(serde_json::json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        let outcome =
            invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
                .await
                .unwrap();
        assert_eq!(outcome, ChassisActionOutcome::AsRequested);
    }

    /// Mounts a `#Chassis.Reset`-only chassis whose `ResetActionInfo` advertises
    /// `allowable` reset types.
    async fn mount_reset_only_chassis(server: &MockServer, allowable: Value) {
        mount_chassis(
            server,
            serde_json::json!({
                "#Chassis.Reset": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset",
                    "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/ResetActionInfo"
                }
            }),
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/powershelf/ResetActionInfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Parameters": [
                    { "Name": "ResetType", "AllowableValues": allowable }
                ]
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn invoke_chassis_action_graceful_off_falls_back_to_reset_force_off() {
        let server = MockServer::start().await;
        // Reset advertises On/ForceOff but NOT GracefulShutdown, and there is no
        // direct #Chassis.Off or #Chassis.ForceOff action.
        mount_reset_only_chassis(&server, serde_json::json!(["On", "ForceOff"])).await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .and(body_json(serde_json::json!({ "ResetType": "ForceOff" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        let outcome =
            invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
                .await
                .unwrap();

        assert_eq!(
            outcome,
            ChassisActionOutcome::FellBackToForceOff {
                action: RMS_CHASSIS_RESET_ACTION
            }
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_failed_force_off_fallback_reports_the_fallback() {
        let server = MockServer::start().await;
        mount_reset_only_chassis(&server, serde_json::json!(["On", "ForceOff"])).await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .and(body_json(serde_json::json!({ "ResetType": "ForceOff" })))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        let err = invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap_err();

        assert!(
            err.message.contains("ForceOff fallback failed"),
            "message: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_force_off_rejects_reset_type_not_in_allowable_values() {
        let server = MockServer::start().await;
        mount_reset_only_chassis(&server, serde_json::json!(["On", "GracefulShutdown"])).await;
        // The reset must NOT be sent when the device says it is unsupported.
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        // Only graceful requests fall back; an explicit ForceOff is never
        // rewritten into something else.
        let err = invoke_chassis_action(
            &redfish,
            CHASSIS_PATH,
            ChassisIntent::ForceOff,
            test_request(),
        )
        .await
        .unwrap_err();

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        // The AllowableValues rejection itself is returned. The graceful
        // fallback error ("neither a graceful power off nor a ForceOff") would
        // also mention AllowableValues, so rule it out.
        assert!(
            err.message.contains("AllowableValues")
                && !err.message.contains("neither a graceful power off"),
            "message: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_graceful_off_fails_when_no_off_path_is_advertised() {
        let server = MockServer::start().await;
        mount_reset_only_chassis(&server, serde_json::json!(["On"])).await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        let err = invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(
            err.message
                .contains("neither a graceful power off nor a ForceOff")
                && err.message.contains("#Chassis.Reset"),
            "message: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_graceful_off_fails_when_chassis_has_no_actions() {
        let server = MockServer::start().await;
        mount_chassis(&server, serde_json::json!({})).await;
        let redfish = redfish_client_for_test(&server.uri());

        let err = invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
    }

    #[tokio::test]
    async fn invoke_chassis_action_advertised_graceful_failure_is_not_retried_as_force_off() {
        let server = MockServer::start().await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.Off": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Off" },
                "#Chassis.ForceOff": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff" }
            }),
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Off"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        // A runtime failure of an advertised graceful action must never become
        // a hard power cut.
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn invoke_chassis_action_unreadable_action_info_for_graceful_action_is_not_retried_as_force_off()
     {
        let server = MockServer::start().await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.Off": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Off",
                    "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/OffActionInfo"
                },
                "#Chassis.ForceOff": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff" }
            }),
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/powershelf/OffActionInfo"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        // The graceful action is advertised but its ActionInfo cannot be read,
        // so nothing is sent and the failure is not turned into a hard cut.
        for action in ["Chassis.Off", "Chassis.ForceOff"] {
            Mock::given(method("POST"))
                .and(path(format!(
                    "/redfish/v1/Chassis/powershelf/Actions/{action}"
                )))
                .respond_with(ResponseTemplate::new(200))
                .expect(0)
                .mount(&server)
                .await;
        }
        let redfish = redfish_client_for_test(&server.uri());

        invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn invoke_chassis_action_unreadable_reset_action_info_is_not_retried_as_force_off() {
        let server = MockServer::start().await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.Reset": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset",
                    "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/ResetActionInfo"
                }
            }),
        )
        .await;
        // Read exactly once: a second read would mean the failed graceful
        // attempt was retried as a ForceOff fallback.
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/powershelf/ResetActionInfo"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn invoke_chassis_action_on_is_never_served_as_force_off_through_reset() {
        let server = MockServer::start().await;
        // The shelf can be reset to ForceOff but not turned on.
        mount_reset_only_chassis(&server, serde_json::json!(["ForceOff"])).await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        let err = invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::On, test_request())
            .await
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(
            err.message.contains("\"On\"") && !err.message.contains("ForceOff fallback"),
            "message: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_on_is_never_served_as_direct_force_off() {
        let server = MockServer::start().await;
        // No way to turn the shelf on is advertised, only a direct ForceOff.
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.ForceOff": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff" }
            }),
        )
        .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        let err = invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::On, test_request())
            .await
            .unwrap_err();

        assert!(
            err.message.contains("#Chassis.On"),
            "message: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_fallback_warns_before_posting_reset_force_off() {
        let server = MockServer::start().await;
        mount_reset_only_chassis(&server, serde_json::json!(["On", "ForceOff"])).await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .and(body_json(serde_json::json!({ "ResetType": "ForceOff" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());
        let (logs, _guard) = capture_logs();

        invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap();

        let logs = logs.text();
        assert!(
            logs.contains("WARN")
                && logs.contains(HARD_CUT_WARNING)
                && logs.contains("ps-01")
                && logs.contains("op=Off")
                && logs.contains("#Chassis.Reset"),
            "logs: {logs}"
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_fallback_warns_with_the_direct_force_off_action() {
        let server = MockServer::start().await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.ForceOff": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff" }
            }),
        )
        .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());
        let (logs, _guard) = capture_logs();

        invoke_chassis_action(
            &redfish,
            CHASSIS_PATH,
            ChassisIntent::Off,
            PowerRequest {
                node: "ps-02",
                op: PowerOp::GracefulShutdown,
            },
        )
        .await
        .unwrap();

        let logs = logs.text();
        assert!(
            logs.contains("WARN")
                && logs.contains(HARD_CUT_WARNING)
                && logs.contains("ps-02")
                && logs.contains("op=GracefulShutdown")
                && logs.contains("#Chassis.ForceOff"),
            "logs: {logs}"
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_fallback_warns_even_when_the_force_off_is_rejected() {
        let server = MockServer::start().await;
        mount_reset_only_chassis(&server, serde_json::json!(["On", "ForceOff"])).await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());
        let (logs, _guard) = capture_logs();

        invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap_err();

        let logs = logs.text();
        assert!(
            logs.contains("WARN") && logs.contains(HARD_CUT_WARNING),
            "logs: {logs}"
        );
    }

    #[tokio::test]
    async fn invoke_chassis_action_does_not_warn_when_nothing_is_hard_cut() {
        // A graceful action that is advertised and used.
        let server = MockServer::start().await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.Off": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Off" },
                "#Chassis.ForceOff": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff" }
            }),
        )
        .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());
        let (graceful_logs, graceful_guard) = capture_logs();
        invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap();
        drop(graceful_guard);

        // An explicit ForceOff is the caller's choice, not a fallback.
        let (force_logs, force_guard) = capture_logs();
        invoke_chassis_action(
            &redfish,
            CHASSIS_PATH,
            ChassisIntent::ForceOff,
            PowerRequest {
                node: "ps-01",
                op: PowerOp::ForceOff,
            },
        )
        .await
        .unwrap();
        drop(force_guard);

        // No graceful path and no ForceOff either: nothing is sent.
        let empty = MockServer::start().await;
        mount_reset_only_chassis(&empty, serde_json::json!(["On"])).await;
        let redfish = redfish_client_for_test(&empty.uri());
        let (none_logs, _none_guard) = capture_logs();
        invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap_err();

        for (case, logs) in [
            ("graceful action used", graceful_logs.text()),
            ("explicit ForceOff", force_logs.text()),
            ("no usable path", none_logs.text()),
        ] {
            assert!(!logs.contains(HARD_CUT_WARNING), "{case}: {logs}");
        }
    }

    #[tokio::test]
    async fn invoke_chassis_action_force_off_intent_skips_graceful_paths() {
        let server = MockServer::start().await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.Off": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Off" },
                "#Chassis.ForceOff": { "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff" }
            }),
        )
        .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Off"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        let outcome = invoke_chassis_action(
            &redfish,
            CHASSIS_PATH,
            ChassisIntent::ForceOff,
            test_request(),
        )
        .await
        .unwrap();

        assert_eq!(outcome, ChassisActionOutcome::AsRequested);
    }

    #[tokio::test]
    async fn invoke_chassis_action_reset_fallback_allows_advertised_reset_type() {
        let server = MockServer::start().await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.Reset": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset",
                    "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/ResetActionInfo"
                }
            }),
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/powershelf/ResetActionInfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Parameters": [
                    { "Name": "ResetType", "AllowableValues": ["On", "GracefulShutdown", "ForceOff"] }
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .and(body_json(
                serde_json::json!({ "ResetType": "GracefulShutdown" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
            .await
            .unwrap();
    }

    /// Chassis `Actions` exactly as reported by the VR NVL72 LiteOn shelf
    /// (PF-1114-1RD): `On`, `ForceOff`, and `Reset`, with no `#Chassis.Off`.
    fn pf_1114_1rd_chassis_actions() -> Value {
        serde_json::json!({
            "#Chassis.ForceOff": {
                "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
                "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/ForceOffActionInfo"
            },
            "#Chassis.On": {
                "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.On",
                "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/OnActionInfo"
            },
            "#Chassis.Reset": {
                "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset",
                "@Redfish.ActionInfo": "/redfish/v1/Chassis/powershelf/ResetActionInfo"
            }
        })
    }

    /// Mounts the `ActionInfo` resources exactly as captured from a VR NVL72
    /// LiteOn shelf (PF-1114-1RD): `ResetType` allows only `Reset`, and
    /// `ForceOff` takes a required `ForceOffType` of `ForceOff`.
    async fn mount_pf_1114_1rd_action_infos(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/powershelf/ResetActionInfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "ResetActionInfo",
                "Parameters": [{
                    "AllowableValues": ["Reset"],
                    "DataType": "String",
                    "Name": "ResetType",
                    "Required": true
                }]
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/powershelf/ForceOffActionInfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "ForceOffActionInfo",
                "Parameters": [{
                    "AllowableValues": ["ForceOff"],
                    "DataType": "String",
                    "Name": "ForceOffType",
                    "Required": true
                }]
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn vr_liteon_shelf_graceful_off_is_served_by_direct_force_off_action() {
        let server = MockServer::start().await;
        mount_chassis(&server, pf_1114_1rd_chassis_actions()).await;
        mount_pf_1114_1rd_action_infos(&server).await;
        // The shelf has no #Chassis.Off and its ResetType allows only `Reset`,
        // so a graceful request must never reach #Chassis.Reset.
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        // Both the graceful fallback and an explicit ForceOff post the payload
        // the shelf's ForceOffActionInfo requires.
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .and(body_json(serde_json::json!({ "ForceOffType": "ForceOff" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(2)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        let graceful =
            invoke_chassis_action(&redfish, CHASSIS_PATH, ChassisIntent::Off, test_request())
                .await
                .unwrap();
        let forced = invoke_chassis_action(
            &redfish,
            CHASSIS_PATH,
            ChassisIntent::ForceOff,
            test_request(),
        )
        .await
        .unwrap();

        assert_eq!(
            graceful,
            ChassisActionOutcome::FellBackToForceOff {
                action: "#Chassis.ForceOff"
            }
        );
        assert_eq!(forced, ChassisActionOutcome::AsRequested);
    }

    #[tokio::test]
    async fn vr_liteon_shelf_set_power_state_serves_graceful_requests_as_force_off() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RMS_CHASSIS_COLLECTION_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{ "@odata.id": CHASSIS_PATH }]
            })))
            .mount(&server)
            .await;
        mount_chassis(&server, pf_1114_1rd_chassis_actions()).await;
        mount_pf_1114_1rd_action_infos(&server).await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .and(body_json(serde_json::json!({ "ForceOffType": "ForceOff" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(3)
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        for op in [PowerOp::Off, PowerOp::GracefulShutdown, PowerOp::ForceOff] {
            powershelf
                .set_power_state(op, PowerTargetType::System)
                .await
                .unwrap_or_else(|err| panic!("{op:?}: {err}"));
        }
    }

    #[tokio::test]
    async fn set_power_state_serves_graceful_requests_as_force_off_on_force_only_shelf() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RMS_CHASSIS_COLLECTION_ENDPOINT))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{ "@odata.id": CHASSIS_PATH }]
            })))
            .mount(&server)
            .await;
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.ForceOff": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
                },
                "#Chassis.On": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.On"
                }
            }),
        )
        .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(3)
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        for op in [PowerOp::Off, PowerOp::GracefulShutdown, PowerOp::ForceOff] {
            powershelf
                .set_power_state(op, PowerTargetType::System)
                .await
                .unwrap_or_else(|err| panic!("{op:?}: {err}"));
        }
    }

    #[tokio::test]
    async fn invoke_chassis_action_reset_fallback_proceeds_without_allowable_values() {
        let server = MockServer::start().await;
        // Reset action with no ActionInfo: capabilities are unknown, so we can't
        // verify and must let the BMC decide.
        mount_chassis(
            &server,
            serde_json::json!({
                "#Chassis.Reset": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"
                }
            }),
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .and(body_json(serde_json::json!({ "ResetType": "ForceOff" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let redfish = redfish_client_for_test(&server.uri());

        invoke_chassis_action(
            &redfish,
            CHASSIS_PATH,
            ChassisIntent::ForceOff,
            test_request(),
        )
        .await
        .unwrap();
    }
}
