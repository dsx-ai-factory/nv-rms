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
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

use crate::domain::node::*;
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::transport::http_client::HttpClient;
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
    http: HttpClient,

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
/// `Off` and `ForceOff` are deliberately distinct: `Off` is a graceful
/// shutdown request and must never be silently upgraded to a hard cut, which
/// would abruptly drop power to downstream compute trays.
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

    /// Value of the `ResetType` parameter used with `#Chassis.Reset`.
    fn redfish_reset_type(&self) -> &'static str {
        match self {
            ChassisIntent::On => "On",
            ChassisIntent::Off => "GracefulShutdown",
            ChassisIntent::ForceOff => "ForceOff",
        }
    }
}

fn validate_powershelf_gb200_liteon_node_type(node_type: NodeType) -> Result<()> {
    if node_type != NodeType::PowershelfGb200Liteon {
        return Err(RmsError::unimplemented("create_node", node_type.as_str()));
    }
    Ok(())
}

impl PowershelfGb200Liteon {
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
        let http = HttpClient::new(
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
            http,
            op_lock: tokio::sync::Mutex::new(()),
            chassis_url: tokio::sync::OnceCell::new(),
        })
    }

    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        validate_powershelf_gb200_liteon_node_type(config.node_type)?;
        let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
            return Err(RmsError::invalid_argument(
                "bmc_endpoint is required for powershelf nodes",
            ));
        };
        let credentials = bmc_endpoint.credentials.as_ref();
        let username = credentials
            .map(|credentials| credentials.username.as_str())
            .unwrap_or_default();
        let password = credentials
            .map(|credentials| credentials.password.expose_secret())
            .unwrap_or_default();

        let http = HttpClient::new(
            &bmc_endpoint.endpoint.ip_address,
            bmc_endpoint.endpoint.port,
            username,
            password,
            bmc_endpoint.dangerously_accept_invalid_certs,
            true,
        )?;
        Ok(Self {
            id: config.id.clone(),
            rack_id: rack_id.to_owned(),
            node_type: config.node_type,
            bmc_endpoint: bmc_endpoint.clone(),
            expected_inventory: config.expected_inventory.clone(),
            http,
            op_lock: tokio::sync::Mutex::new(()),
            chassis_url: tokio::sync::OnceCell::new(),
        })
    }

    fn nvfwupd_target_config(&self) -> nvfwupd::workflow::TargetConfig {
        let credentials = self.bmc_endpoint.credentials.as_ref();
        let username = credentials
            .map(|credentials| credentials.username.as_str())
            .unwrap_or_default();
        let default_password = SecretString::default();
        let password = credentials
            .map(|credentials| &credentials.password)
            .unwrap_or(&default_password);

        nvfwupd_adapter::to_target_config_with_secret(
            self.node_type,
            &self.bmc_endpoint.endpoint.ip_address,
            self.bmc_endpoint.endpoint.port,
            username,
            password,
            !self.bmc_endpoint.dangerously_accept_invalid_certs,
        )
    }

    /// RMS powershelf ForceOff action.
    /// Lazily resolves and caches the powershelf chassis URL by enumerating
    /// `/redfish/v1/Chassis` and picking the member whose Id starts with
    /// `powershelf` (case-insensitive). This handles both LiteOn
    /// (`/Chassis/powershelf`) and NVIDIA PMC (`/Chassis/PowerShelf_0`).
    async fn resolve_chassis_url(&self, http: &HttpClient) -> Result<&str> {
        self.chassis_url
            .get_or_try_init(|| discover_chassis_url(http))
            .await
            .map(String::as_str)
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
        let http = &self.http;

        tracing::debug!(node = %self.id, "get_power_state");
        let chassis_url = self.resolve_chassis_url(http).await?;
        let chassis = http.get(chassis_url, HttpClient::DEFAULT_TIMEOUT).await?;
        let psu_states = fetch_psu_power_states(http, &chassis).await;
        let state = aggregate_psu_power_states(&psu_states);
        tracing::debug!(
            node = %self.id,
            ?state,
            psu_count = psu_states.len(),
            "get_power_state: aggregated from PowerSupplies"
        );
        Ok(state)
    }

    async fn set_power_state(&self, op: PowerOp, _target: PowerTargetType) -> Result<()> {
        tracing::info!(node = %self.id, ?op, "set_power_state");
        let intent = match op {
            PowerOp::On | PowerOp::ForceOn => ChassisIntent::On,
            // A graceful Off must stay graceful: it maps to a graceful-shutdown
            // intent, never a hard ForceOff. If the shelf can't honor a
            // graceful shutdown, invoke_chassis_action fails rather than
            // silently cutting power.
            PowerOp::Off => ChassisIntent::Off,
            PowerOp::ForceOff => ChassisIntent::ForceOff,
            PowerOp::PowerCycle
            | PowerOp::GracefulShutdown
            | PowerOp::GracefulRestart
            | PowerOp::ForceRestart
            | PowerOp::Nmi => {
                return Err(RmsError::invalid_argument(format!(
                    "{op:?} is not supported for powershelf power control"
                )));
            }
        };

        let _op_guard = self.op_lock.lock().await;
        let http = &self.http;
        let url = self.resolve_chassis_url(http).await?;
        invoke_chassis_action(http, url, intent).await
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

async fn invoke_chassis_action(
    http: &HttpClient,
    chassis_url: &str,
    intent: ChassisIntent,
) -> Result<()> {
    let chassis = http.get(chassis_url, HttpClient::DEFAULT_TIMEOUT).await?;

    if let Some(links) = find_action_links(&chassis, intent.direct_action_key())? {
        let payload = if links.action_info.is_empty() {
            Value::Object(serde_json::Map::new())
        } else {
            let info = http
                .get(&links.action_info, HttpClient::DEFAULT_TIMEOUT)
                .await?;
            build_action_payload(&info)
        };
        tracing::debug!(
            action = intent.direct_action_key(),
            target = %links.target,
            payload = %payload,
            "invoke_chassis_action: direct action"
        );
        http.post(&links.target, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        return Ok(());
    }

    let reset = find_action_links(&chassis, RMS_CHASSIS_RESET_ACTION)?.ok_or_else(|| {
        RmsError::internal(format!(
            "powershelf chassis Actions expose neither {} nor {RMS_CHASSIS_RESET_ACTION}",
            intent.direct_action_key()
        ))
    })?;

    // When the BMC advertises AllowableValues for ResetType, honor them: sending
    // a reset the device says it does not support risks leaving the shelf in an
    // indeterminate state. Reject rather than "attempt anyway". If no
    // AllowableValues are advertised we cannot verify, so we let the BMC decide.
    if !reset.action_info.is_empty() {
        let info = http
            .get(&reset.action_info, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        if let Some(allowed) = reset_allowable_values(&info)
            && !allowed.iter().any(|v| v == intent.redfish_reset_type())
        {
            return Err(RmsError::failed_precondition(format!(
                "#Chassis.Reset ResetType {:?} is not in AllowableValues {:?}; \
                     refusing to send an unsupported reset",
                intent.redfish_reset_type(),
                allowed
            )));
        }
    }

    let payload = serde_json::json!({"ResetType": intent.redfish_reset_type()});
    tracing::debug!(
        action = RMS_CHASSIS_RESET_ACTION,
        target = %reset.target,
        payload = %payload,
        "invoke_chassis_action: #Chassis.Reset fallback"
    );
    http.post(&reset.target, &payload, HttpClient::DEFAULT_TIMEOUT)
        .await?;
    Ok(())
}

async fn discover_chassis_url(http: &HttpClient) -> Result<String> {
    let collection = http
        .get(RMS_CHASSIS_COLLECTION_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
        .await?;
    let Some(members) = collection.get("Members").and_then(|m| m.as_array()) else {
        return Err(RmsError::internal(format!(
            "{RMS_CHASSIS_COLLECTION_ENDPOINT} response missing Members array"
        )));
    };

    let matches: Vec<&str> = members
        .iter()
        .filter_map(|m| m.get("@odata.id").and_then(|v| v.as_str()))
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
fn find_action_links(chassis: &Value, action: &str) -> Result<Option<ActionLinks>> {
    let Some(act) = chassis.get("Actions").and_then(|a| a.get(action)) else {
        return Ok(None);
    };

    let target = act
        .get("target")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if target.is_empty() {
        return Err(RmsError::internal(format!(
            "missing {action} target in powershelf chassis Actions"
        )));
    }

    let action_info = act
        .get("@Redfish.ActionInfo")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();

    Ok(Some(ActionLinks {
        target: target.to_owned(),
        action_info,
    }))
}

/// Returns the `ResetType` parameter's `AllowableValues` from a `#Chassis.Reset`
/// ActionInfo, or `None` if the parameter or its allowables aren't advertised.
fn reset_allowable_values(action_info: &Value) -> Option<Vec<String>> {
    let params = action_info.get("Parameters").and_then(|p| p.as_array())?;
    let reset_param = params
        .iter()
        .find(|p| p.get("Name").and_then(|n| n.as_str()) == Some("ResetType"))?;
    let allowed = reset_param
        .get("AllowableValues")
        .and_then(|v| v.as_array())?;
    Some(
        allowed
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
    )
}

/// Selects each parameter's `DefaultValue`, else the first `AllowableValues`
/// entry. Parameters with neither are omitted so the BMC's defaults stand.
fn build_action_payload(action_info: &Value) -> Value {
    let mut obj = serde_json::Map::new();
    let Some(params) = action_info.get("Parameters").and_then(|p| p.as_array()) else {
        return Value::Object(obj);
    };

    for p in params {
        let Some(name) = p.get("Name").and_then(|v| v.as_str()) else {
            continue;
        };
        if let Some(default) = p.get("DefaultValue") {
            obj.insert(name.to_owned(), default.clone());
            continue;
        }
        if let Some(first) = p
            .get("AllowableValues")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
        {
            obj.insert(name.to_owned(), first.clone());
        }
    }
    Value::Object(obj)
}

/// Walks a powershelf chassis JSON down to its PSU members and returns the
/// `PowerState` each PSU reports, decoded into a tri-state: `Some(true)` =
/// energized, `Some(false)` = off, `None` = field absent or unrecognized.
/// Members whose URL is missing or whose resource fetch fails are skipped — a
/// missing PSU contributes no signal — so the returned vector has one entry
/// per PSU we successfully read, in collection order.
async fn fetch_psu_power_states(http: &HttpClient, chassis: &Value) -> Vec<Option<bool>> {
    let Some(supplies_url) = power_supplies_url(chassis) else {
        tracing::warn!("powershelf chassis exposes no PowerSubsystem.PowerSupplies link");
        return Vec::new();
    };

    let subsystem = match http
        .get(&supplies_url.subsystem, HttpClient::DEFAULT_TIMEOUT)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                url = %supplies_url.subsystem,
                error = %e.message,
                "failed to fetch PowerSubsystem"
            );
            return Vec::new();
        }
    };

    let supplies = match subsystem
        .pointer("/PowerSupplies/@odata.id")
        .and_then(|v| v.as_str())
    {
        Some(uri) if !uri.is_empty() => uri.to_owned(),
        _ => {
            tracing::warn!(
                url = %supplies_url.subsystem,
                "PowerSubsystem has no PowerSupplies link"
            );
            return Vec::new();
        }
    };

    let collection = match http.get(&supplies, HttpClient::DEFAULT_TIMEOUT).await {
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

    let Some(members) = collection.get("Members").and_then(|m| m.as_array()) else {
        return Vec::new();
    };

    let mut out = Vec::with_capacity(members.len());
    for member in members {
        let Some(uri) = member.get("@odata.id").and_then(|v| v.as_str()) else {
            continue;
        };
        if uri.is_empty() {
            continue;
        }
        match http.get(uri, HttpClient::DEFAULT_TIMEOUT).await {
            Ok(psu) => out.push(decode_psu_power_state(psu.get("PowerState"))),
            Err(e) => {
                tracing::warn!(uri, error = %e.message, "failed to fetch PowerSupply");
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

/// Internal: link to the chassis-level PowerSubsystem. Wrapped so the call site
/// reads less noisily.
struct SuppliesLinks {
    subsystem: String,
}

fn power_supplies_url(chassis: &Value) -> Option<SuppliesLinks> {
    let subsystem = chassis
        .pointer("/PowerSubsystem/@odata.id")
        .and_then(|v| v.as_str())?;
    if subsystem.is_empty() {
        return None;
    }
    Some(SuppliesLinks {
        subsystem: subsystem.to_owned(),
    })
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
    use crate::utilities::error::ErrorCode;
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

    fn sample_chassis() -> Value {
        serde_json::json!({
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
        })
    }

    #[test]
    fn chassis_intent_maps_to_direct_key_and_reset_type() {
        assert_eq!(ChassisIntent::On.direct_action_key(), "#Chassis.On");
        assert_eq!(ChassisIntent::On.redfish_reset_type(), "On");
        assert_eq!(ChassisIntent::Off.direct_action_key(), "#Chassis.Off");
        assert_eq!(ChassisIntent::Off.redfish_reset_type(), "GracefulShutdown");
        assert_eq!(
            ChassisIntent::ForceOff.direct_action_key(),
            "#Chassis.ForceOff"
        );
        assert_eq!(ChassisIntent::ForceOff.redfish_reset_type(), "ForceOff");
    }

    #[test]
    fn graceful_off_and_force_off_map_to_distinct_intents() {
        // The whole point of the fix: a graceful Off must not collapse into a
        // hard ForceOff.
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
        let chassis = serde_json::json!({
            "Actions": {
                "#Chassis.ForceOff": {
                    "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
                }
            }
        });
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
        let chassis = serde_json::json!({
            "Actions": { "#Chassis.Reset": { "target": "/r" } }
        });
        assert!(
            find_action_links(&chassis, ChassisIntent::ForceOff.direct_action_key())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn find_action_links_propagates_malformed_entries() {
        let chassis = serde_json::json!({
            "Actions": { "#Chassis.ForceOff": { "@Redfish.ActionInfo": "/x" } }
        });
        let err =
            find_action_links(&chassis, ChassisIntent::ForceOff.direct_action_key()).unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("target"));
    }

    fn pick_powershelf_uri(collection: &Value) -> Option<String> {
        collection
            .get("Members")
            .and_then(|m| m.as_array())
            .and_then(|members| {
                members.iter().find_map(|m| {
                    let uri = m.get("@odata.id").and_then(|v| v.as_str())?;
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
            })
    }

    #[test]
    fn discover_chassis_url_picks_pmc_capital_variant() {
        let coll = serde_json::json!({
            "Members": [
                { "@odata.id": "/redfish/v1/Chassis/PowerShelf_0" },
                { "@odata.id": "/redfish/v1/Chassis/ERoT_BMC_0" }
            ]
        });
        assert_eq!(
            pick_powershelf_uri(&coll).as_deref(),
            Some("/redfish/v1/Chassis/PowerShelf_0")
        );
    }

    #[test]
    fn discover_chassis_url_picks_liteon_lowercase_variant() {
        let coll = serde_json::json!({
            "Members": [
                { "@odata.id": "/redfish/v1/Chassis/ERoT_BMC_0" },
                { "@odata.id": "/redfish/v1/Chassis/powershelf" }
            ]
        });
        assert_eq!(
            pick_powershelf_uri(&coll).as_deref(),
            Some("/redfish/v1/Chassis/powershelf")
        );
    }

    #[test]
    fn discover_chassis_url_skips_unrelated_chassis() {
        let coll = serde_json::json!({
            "Members": [
                { "@odata.id": "/redfish/v1/Chassis/PowerSubsystem" },
                { "@odata.id": "/redfish/v1/Chassis/ERoT_BMC_0" }
            ]
        });
        assert_eq!(pick_powershelf_uri(&coll), None);
    }

    #[test]
    fn reset_allowable_values_returns_advertised_reset_types() {
        let info = serde_json::json!({
            "Parameters": [
                {
                    "Name": "ResetType",
                    "AllowableValues": ["On", "ForceOff", "GracefulShutdown"]
                }
            ]
        });
        assert_eq!(
            reset_allowable_values(&info),
            Some(vec![
                "On".into(),
                "ForceOff".into(),
                "GracefulShutdown".into()
            ])
        );
    }

    #[test]
    fn reset_allowable_values_none_when_param_missing() {
        assert_eq!(reset_allowable_values(&serde_json::json!({})), None);
        assert_eq!(
            reset_allowable_values(&serde_json::json!({ "Parameters": [] })),
            None
        );
        let wrong_name = serde_json::json!({
            "Parameters": [{ "Name": "OnType", "AllowableValues": ["On"] }]
        });
        assert_eq!(reset_allowable_values(&wrong_name), None);
    }

    #[test]
    fn build_action_payload_uses_first_allowable_value() {
        let info = serde_json::json!({
            "Parameters": [
                {
                    "Name": "ForceOffType",
                    "Required": true,
                    "DataType": "String",
                    "AllowableValues": ["ForceOff"]
                }
            ]
        });
        assert_eq!(
            build_action_payload(&info),
            serde_json::json!({"ForceOffType": "ForceOff"})
        );
    }

    #[test]
    fn build_action_payload_prefers_default_value() {
        let info = serde_json::json!({
            "Parameters": [
                {
                    "Name": "OnType",
                    "DataType": "String",
                    "DefaultValue": "On",
                    "AllowableValues": ["On", "Soft"]
                }
            ]
        });
        assert_eq!(
            build_action_payload(&info),
            serde_json::json!({"OnType": "On"})
        );
    }

    #[test]
    fn build_action_payload_skips_unconstrained_parameters() {
        let info = serde_json::json!({
            "Parameters": [
                { "Name": "Optional", "DataType": "String" },
                {
                    "Name": "Required",
                    "DataType": "String",
                    "AllowableValues": ["X"]
                }
            ]
        });
        assert_eq!(
            build_action_payload(&info),
            serde_json::json!({"Required": "X"})
        );
    }

    #[test]
    fn build_action_payload_no_parameters_is_empty_object() {
        assert_eq!(
            build_action_payload(&serde_json::json!({})),
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

        // GracefulShutdown stays unsupported on a power shelf (no OS to shut
        // down); a graceful power-down is expressed via PowerOp::Off instead.
        for op in [
            PowerOp::PowerCycle,
            PowerOp::GracefulShutdown,
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
        let chassis = serde_json::json!({
            "PowerSubsystem": {
                "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem"
            }
        });
        let links = power_supplies_url(&chassis).expect("link present");
        assert_eq!(
            links.subsystem,
            "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem"
        );
    }

    #[test]
    fn power_supplies_url_returns_none_when_missing() {
        assert!(power_supplies_url(&serde_json::json!({})).is_none());
        assert!(
            power_supplies_url(&serde_json::json!({"PowerSubsystem": {"@odata.id": ""}})).is_none()
        );
    }

    const CHASSIS_PATH: &str = "/redfish/v1/Chassis/powershelf";

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
        let http = HttpClient::for_test(&server.uri());

        invoke_chassis_action(&http, CHASSIS_PATH, ChassisIntent::Off)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn invoke_chassis_action_rejects_reset_type_not_in_allowable_values() {
        let server = MockServer::start().await;
        // Only a #Chassis.Reset action, advertising On/ForceOff but NOT
        // GracefulShutdown.
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
                    { "Name": "ResetType", "AllowableValues": ["On", "ForceOff"] }
                ]
            })))
            .mount(&server)
            .await;
        // The reset must NOT be sent when the device says it is unsupported.
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Chassis/powershelf/Actions/Chassis.Reset"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let http = HttpClient::for_test(&server.uri());

        let err = invoke_chassis_action(&http, CHASSIS_PATH, ChassisIntent::Off)
            .await
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(
            err.message.contains("AllowableValues"),
            "message: {}",
            err.message
        );
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
        let http = HttpClient::for_test(&server.uri());

        invoke_chassis_action(&http, CHASSIS_PATH, ChassisIntent::Off)
            .await
            .unwrap();
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
        let http = HttpClient::for_test(&server.uri());

        invoke_chassis_action(&http, CHASSIS_PATH, ChassisIntent::ForceOff)
            .await
            .unwrap();
    }
}
