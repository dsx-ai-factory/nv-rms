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

use crate::domain::node::{
    ExpectedInventoryPolicy, FirmwareActivationRequest, FirmwareActivationSummary, FirmwareInfo,
    FirmwareTarget, FirmwareTaskStatus, FirmwareUpdateOptions, FirmwareUpdateOutcome, Node,
    NodeType, PowerOp, PowerState, PowerTargetType,
};
use crate::domain::rack::NodeConfig;
use crate::nodes::powershelf_gb200_delta::PowershelfGb200Delta;
use crate::utilities::error::{Result, RmsError};

/// Delta GB300 powershelf node.
///
/// GB200 and GB300 Delta powershelves expose the same Delta OEM Redfish tree.
/// This wrapper keeps the concrete GB300 node identity while delegating power
/// and firmware behavior to the shared GB200 Delta implementation.
pub struct PowershelfGb300Delta {
    inner: PowershelfGb200Delta,
}

impl PowershelfGb300Delta {
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::PowershelfGb300Delta {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        let mut gb200_config = config.clone();
        gb200_config.node_type = NodeType::PowershelfGb200Delta;
        PowershelfGb200Delta::from_config(&gb200_config, rack_id).map(|inner| Self { inner })
    }
}

#[async_trait]
impl Node for PowershelfGb300Delta {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::PowershelfGb300Delta
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::PowershelfGb300Delta.as_str().to_owned(),
        );
        info
    }

    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        self.inner.expected_inventory_policy()
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

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        self.inner
            .update_firmware(target, force_update, options)
            .await
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        self.inner.poll_firmware_task(task_id).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config() -> NodeConfig {
        NodeConfig {
            id: "ps-01".into(),
            node_type: NodeType::PowershelfGb300Delta,
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
        }
    }

    fn test_node(base_url: &str) -> PowershelfGb300Delta {
        let mut powershelf = PowershelfGb300Delta::from_config(&test_config(), "rack-01").unwrap();
        powershelf.inner.replace_http_for_test(base_url);
        powershelf
    }

    #[test]
    fn from_config_reports_gb300_delta_type() {
        let powershelf = PowershelfGb300Delta::from_config(&test_config(), "rack-01").unwrap();

        assert_eq!(powershelf.node_type(), NodeType::PowershelfGb300Delta);
        assert_eq!(
            powershelf.get_info()["type"],
            NodeType::PowershelfGb300Delta.as_str()
        );
        assert_eq!(powershelf.get_info()["host"], "10.0.0.5");
    }

    #[tokio::test]
    async fn get_power_state_delegates_to_shared_delta_tree() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{ "@odata.id": "/redfish/v1/Chassis/chassis" }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Manufacturer": "DELTA",
                "PowerSubsystem": {
                    "@odata.id": "/redfish/v1/Chassis/chassis/PowerSubsystem"
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/chassis/PowerSubsystem"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "PowerSupplies": {
                    "@odata.id": "/redfish/v1/Chassis/chassis/PowerSubsystem/PowerSupplies"
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/chassis/PowerSubsystem/PowerSupplies",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/Chassis/chassis/PowerSubsystem/PowerSupplies/1"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/chassis/PowerSubsystem/PowerSupplies/1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Oem": { "deltaenergysystems": { "Power": true } }
            })))
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        assert_eq!(powershelf.get_power_state().await.unwrap(), PowerState::On);
    }

    #[tokio::test]
    async fn set_power_state_delegates_to_shared_delta_action() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/PowerEquipment/PowerShelves"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/PowerEquipment/PowerShelves/1"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/PowerEquipment/PowerShelves/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
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
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOffPSUs",
            ))
            .and(body_json(serde_json::json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        powershelf
            .set_power_state(PowerOp::Off, PowerTargetType::System)
            .await
            .unwrap();
    }
}
