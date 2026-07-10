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

use std::collections::HashMap;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

use crate::domain::node::{
    FirmwareActivationMode, FirmwareActivationRequest, FirmwareActivationSummary, FirmwareInfo,
    FirmwareTarget, FirmwareTaskStatus, FirmwareUpdateOptions, FirmwareUpdateOutcome, Node,
    NodeType, PowerOp, PowerState, PowerTargetType,
};
use crate::domain::rack::{EndpointConfig, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{Result, RmsError};

const DELTA_CHASSIS_COLLECTION_ENDPOINT: &str = "/redfish/v1/Chassis";
const DELTA_POWER_SHELVES_COLLECTION_ENDPOINT: &str = "/redfish/v1/PowerEquipment/PowerShelves";
const DELTA_OEM_KEY: &str = "deltaenergysystems";
const DELTA_TURN_ON_ACTION: &str = "#PowerShelf.TurnOnPSUs";
const DELTA_TURN_OFF_ACTION: &str = "#PowerShelf.TurnOffPSUs";

/// Delta GB200 powershelf node.
///
/// Delta GB200 and GB300 powershelves share this Redfish behavior. Power state
/// is exposed through Delta OEM PSU properties, while power control is exposed
/// under `/redfish/v1/PowerEquipment/PowerShelves`. Firmware workflows use the
/// shared NVFWUPD PowerShelf server type.
pub struct PowershelfGb200Delta {
    id: String,
    rack_id: String,
    node_type: NodeType,
    bmc_endpoint: EndpointConfig,
    http: tokio::sync::Mutex<HttpClient>,
    chassis_url: tokio::sync::OnceCell<String>,
    power_shelf_url: tokio::sync::OnceCell<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeltaPowerIntent {
    On,
    ForceOff,
}

impl DeltaPowerIntent {
    fn action_key(self) -> &'static str {
        match self {
            Self::On => DELTA_TURN_ON_ACTION,
            Self::ForceOff => DELTA_TURN_OFF_ACTION,
        }
    }
}

impl PowershelfGb200Delta {
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
            http: tokio::sync::Mutex::new(http),
            chassis_url: tokio::sync::OnceCell::new(),
            power_shelf_url: tokio::sync::OnceCell::new(),
        })
    }

    async fn resolve_chassis_url(&self, http: &HttpClient) -> Result<&str> {
        self.chassis_url
            .get_or_try_init(|| discover_delta_chassis_url(http))
            .await
            .map(String::as_str)
    }

    async fn resolve_power_shelf_url(&self, http: &HttpClient) -> Result<&str> {
        self.power_shelf_url
            .get_or_try_init(|| discover_delta_power_shelf_url(http))
            .await
            .map(String::as_str)
    }

    #[cfg(test)]
    pub(crate) fn replace_http_for_test(&mut self, base_url: &str) {
        self.http = tokio::sync::Mutex::new(HttpClient::for_test(base_url));
        self.chassis_url = tokio::sync::OnceCell::new();
        self.power_shelf_url = tokio::sync::OnceCell::new();
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

    async fn get_power_state(&self) -> Result<PowerState> {
        let http = self.http.lock().await;
        tracing::debug!(node = %self.id(), "get_power_state");
        let chassis_url = self.resolve_chassis_url(&http).await?;
        let chassis = http.get(chassis_url, HttpClient::DEFAULT_TIMEOUT).await?;
        let psu_states = fetch_delta_psu_power_states(&http, &chassis).await;
        let state = aggregate_delta_psu_power_states(&psu_states);
        tracing::debug!(
            node = %self.id(),
            ?state,
            psu_count = psu_states.len(),
            "get_power_state: aggregated from Delta PowerSupplies"
        );
        Ok(state)
    }

    async fn set_power_state(&self, op: PowerOp, _target: PowerTargetType) -> Result<()> {
        tracing::info!(node = %self.id(), ?op, "set_power_state");
        let intent = match op {
            PowerOp::On | PowerOp::ForceOn => DeltaPowerIntent::On,
            PowerOp::Off | PowerOp::ForceOff => DeltaPowerIntent::ForceOff,
            _ => {
                return Err(RmsError::invalid_argument(format!(
                    "{op:?} is not supported for powershelf power control"
                )));
            }
        };

        let http = self.http.lock().await;
        let power_shelf_url = self.resolve_power_shelf_url(&http).await?;
        invoke_delta_power_action(&http, power_shelf_url, intent).await
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let inventory = nvfwupd::workflow_api::get_firmware_inventory(self.nvfwupd_target_config())
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(inventory
            .into_iter()
            .map(nvfwupd_adapter::to_firmware_info)
            .collect())
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let target = FirmwareTarget {
            component: String::new(),
            firmware_file: target.firmware_file.clone(),
        };
        let request = nvfwupd_adapter::to_update_request(&target, force_update, options);
        let outcome = nvfwupd::workflow_api::update_firmware(self.nvfwupd_target_config(), request)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_outcome(outcome))
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let status = nvfwupd::workflow_api::get_task_status(self.nvfwupd_target_config(), task_id)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::to_task_status(status))
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
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

async fn discover_delta_chassis_url(http: &HttpClient) -> Result<String> {
    discover_member_url(
        http,
        DELTA_CHASSIS_COLLECTION_ENDPOINT,
        chassis_resource_looks_like_delta_powershelf,
        chassis_has_power_subsystem,
        "no Delta powershelf chassis found in /redfish/v1/Chassis",
        "failed to inspect chassis member while discovering Delta powershelf chassis",
    )
    .await
}

async fn discover_delta_power_shelf_url(http: &HttpClient) -> Result<String> {
    discover_member_url(
        http,
        DELTA_POWER_SHELVES_COLLECTION_ENDPOINT,
        power_shelf_resource_looks_like_delta,
        power_shelf_has_delta_actions,
        "no Delta powershelf found in /redfish/v1/PowerEquipment/PowerShelves",
        "failed to inspect PowerShelf member while discovering Delta powershelf actions",
    )
    .await
}

async fn discover_member_url<F, G>(
    http: &HttpClient,
    collection_endpoint: &str,
    is_match: F,
    is_fallback: G,
    not_found_msg: &str,
    inspect_error_msg: &str,
) -> Result<String>
where
    F: Fn(&Value) -> bool,
    G: Fn(&Value) -> bool,
{
    let collection = http
        .get(collection_endpoint, HttpClient::DEFAULT_TIMEOUT)
        .await?;
    let members = members_array(&collection, collection_endpoint)?;
    let mut fallback = None;

    for member in members {
        let Some(uri) = member.get("@odata.id").and_then(|v| v.as_str()) else {
            continue;
        };
        if uri.is_empty() {
            continue;
        }
        match http.get(uri, HttpClient::DEFAULT_TIMEOUT).await {
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

fn members_array<'a>(collection: &'a Value, endpoint: &str) -> Result<&'a Vec<Value>> {
    collection
        .get("Members")
        .and_then(|m| m.as_array())
        .ok_or_else(|| RmsError::internal(format!("{endpoint} response missing Members array")))
}

fn chassis_resource_looks_like_delta_powershelf(chassis: &Value) -> bool {
    chassis_has_power_subsystem(chassis) && has_delta_marker(chassis)
}

fn chassis_has_power_subsystem(chassis: &Value) -> bool {
    chassis
        .pointer("/PowerSubsystem/@odata.id")
        .and_then(|v| v.as_str())
        .is_some_and(|uri| !uri.is_empty())
}

fn power_shelf_resource_looks_like_delta(power_shelf: &Value) -> bool {
    has_delta_marker(power_shelf)
        && resource_looks_like_power_shelf(power_shelf)
        && power_shelf_has_delta_actions(power_shelf)
}

fn resource_looks_like_power_shelf(resource: &Value) -> bool {
    string_field_contains(resource, "EquipmentType", "powershelf")
        || string_field_contains(resource, "Name", "power shelf")
}

fn has_delta_marker(resource: &Value) -> bool {
    string_field_contains(resource, "Manufacturer", "delta")
        || resource.pointer("/Oem/deltaenergysystems").is_some()
        || resource
            .get("Oem")
            .and_then(|oem| oem.as_object())
            .is_some_and(|oem| {
                oem.keys()
                    .any(|key| key.to_ascii_lowercase().contains("delta"))
            })
}

fn string_field_contains(resource: &Value, key: &str, needle: &str) -> bool {
    resource
        .get(key)
        .and_then(|value| value.as_str())
        .is_some_and(|value| value.to_ascii_lowercase().contains(needle))
}

fn power_shelf_has_delta_actions(power_shelf: &Value) -> bool {
    delta_power_action_target(power_shelf, DELTA_TURN_ON_ACTION).is_some()
        && delta_power_action_target(power_shelf, DELTA_TURN_OFF_ACTION).is_some()
}

async fn invoke_delta_power_action(
    http: &HttpClient,
    power_shelf_url: &str,
    intent: DeltaPowerIntent,
) -> Result<()> {
    let power_shelf = http
        .get(power_shelf_url, HttpClient::DEFAULT_TIMEOUT)
        .await?;
    let target = delta_power_action_target(&power_shelf, intent.action_key()).ok_or_else(|| {
        RmsError::internal(format!(
            "Delta powershelf Actions missing {} target",
            intent.action_key()
        ))
    })?;
    http.post(
        &target,
        &Value::Object(serde_json::Map::new()),
        HttpClient::DEFAULT_TIMEOUT,
    )
    .await?;
    Ok(())
}

fn delta_power_action_target(power_shelf: &Value, action: &str) -> Option<String> {
    power_shelf
        .get("Oem")
        .and_then(|oem| oem.get(DELTA_OEM_KEY))
        .and_then(|delta| delta.get("Actions"))
        .and_then(|actions| actions.get(action))
        .and_then(|action| action.get("target"))
        .and_then(|target| target.as_str())
        .filter(|target| !target.is_empty())
        .map(str::to_owned)
}

async fn fetch_delta_psu_power_states(http: &HttpClient, chassis: &Value) -> Vec<Option<bool>> {
    let Some(subsystem_url) = chassis
        .pointer("/PowerSubsystem/@odata.id")
        .and_then(|v| v.as_str())
        .filter(|uri| !uri.is_empty())
    else {
        tracing::warn!("Delta powershelf chassis exposes no PowerSubsystem link");
        return Vec::new();
    };

    let subsystem = match http.get(subsystem_url, HttpClient::DEFAULT_TIMEOUT).await {
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
        .pointer("/PowerSupplies/@odata.id")
        .and_then(|v| v.as_str())
        .filter(|uri| !uri.is_empty())
    else {
        tracing::warn!(url = %subsystem_url, "Delta PowerSubsystem has no PowerSupplies link");
        return Vec::new();
    };

    let collection = match http.get(supplies_url, HttpClient::DEFAULT_TIMEOUT).await {
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
            Ok(psu) => out.push(decode_delta_psu_power_state(&psu)),
            Err(e) => {
                tracing::warn!(uri, error = %e.message, "failed to fetch Delta PowerSupply");
            }
        }
    }
    out
}

fn decode_delta_psu_power_state(psu: &Value) -> Option<bool> {
    decode_power_value(psu.pointer("/Oem/deltaenergysystems/Power"))
        .or_else(|| decode_power_value(psu.get("PowerState")))
}

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
        }
    }

    fn test_node(base_url: &str) -> PowershelfGb200Delta {
        let config = test_config(NodeType::PowershelfGb200Delta);
        let mut powershelf = PowershelfGb200Delta::from_config(&config, "rack-01").unwrap();
        powershelf.replace_http_for_test(base_url);
        powershelf
    }

    fn delta_psu(power: Value) -> Value {
        serde_json::json!({
            "Oem": {
                "deltaenergysystems": {
                    "Power": power
                }
            }
        })
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
                    ResponseTemplate::new(200).set_body_json(delta_psu(Value::Bool(*power))),
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
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "kind": "fallback" })),
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
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "kind": "match" })),
            )
            .mount(&server)
            .await;
        let http = HttpClient::for_test(&server.uri());

        let discovered = discover_member_url(
            &http,
            COLLECTION_ENDPOINT,
            |resource| resource.get("kind").and_then(Value::as_str) == Some("match"),
            |resource| resource.get("kind").and_then(Value::as_str) == Some("fallback"),
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
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "kind": "fallback" })),
            )
            .mount(&server)
            .await;
        let http = HttpClient::for_test(&server.uri());

        let discovered = discover_member_url(
            &http,
            COLLECTION_ENDPOINT,
            |_| false,
            |resource| resource.get("kind").and_then(Value::as_str) == Some("fallback"),
            "test member not found",
            "failed to inspect test member",
        )
        .await
        .unwrap();

        assert_eq!(discovered, FALLBACK_ENDPOINT);
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
    fn replace_http_for_test_clears_cached_resource_urls() {
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

        powershelf.replace_http_for_test("http://127.0.0.1:12345");

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
        assert_eq!(decode_delta_psu_power_state(&serde_json::json!({})), None);
    }

    #[test]
    fn decode_delta_psu_power_state_falls_back_to_top_level_power_state() {
        assert_eq!(
            decode_delta_psu_power_state(&serde_json::json!({"PowerState": "On"})),
            Some(true)
        );
        assert_eq!(
            decode_delta_psu_power_state(&serde_json::json!({"PowerState": "Off"})),
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
            .expect(2)
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        powershelf
            .set_power_state(PowerOp::Off, PowerTargetType::System)
            .await
            .unwrap();
        powershelf
            .set_power_state(PowerOp::ForceOff, PowerTargetType::System)
            .await
            .unwrap();
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
            assert!(err.message.contains("not supported for powershelf"));
        }
    }
}
