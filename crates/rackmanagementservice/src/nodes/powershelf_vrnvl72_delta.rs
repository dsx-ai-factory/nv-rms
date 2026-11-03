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
use crate::nodes::powershelf_gb200_delta::{DeltaPowerControl, PowershelfGb200Delta};
use crate::utilities::error::{Result, RmsError};

/// Delta VR NVL72 powershelf node (AHE54V2037A, managed by an NVIDIA PMC).
///
/// This wrapper keeps the concrete VR NVL72 node identity while delegating
/// power reads and firmware to the shared Delta implementation. The shelf's
/// `/redfish/v1/PowerEquipment/PowerShelves` member has no Delta OEM PSU
/// actions; power control instead uses `#Chassis.Reset` on the shelf chassis
/// (for example `/redfish/v1/Chassis/PowerShelf_0`) with `ResetType` `On` or
/// `ForceOff`.
pub struct PowershelfVrnvl72Delta {
    inner: PowershelfGb200Delta,
}

impl PowershelfVrnvl72Delta {
    /// Builds a Delta VR NVL72 shelf node backed by a GB200 Delta node that
    /// uses `#Chassis.Reset` power control.
    ///
    /// # Errors
    ///
    /// Returns `Unimplemented` if `config.node_type` is not
    /// `PowershelfVrnvl72Delta`, or any error from
    /// [`PowershelfGb200Delta::from_config`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rackmanagementservice::domain::node::{Node, NodeType};
    /// use rackmanagementservice::domain::rack::NodeConfig;
    /// use rackmanagementservice::nodes::powershelf_vrnvl72_delta::PowershelfVrnvl72Delta;
    ///
    /// let config = NodeConfig::new("ps-0", NodeType::PowershelfVrnvl72Delta, "192.0.2.10");
    /// let shelf = PowershelfVrnvl72Delta::from_config(&config, "rack-1")?;
    /// assert_eq!(shelf.node_type(), NodeType::PowershelfVrnvl72Delta);
    ///
    /// let other = NodeConfig::new("ps-1", NodeType::PowershelfGb200Delta, "192.0.2.11");
    /// assert!(PowershelfVrnvl72Delta::from_config(&other, "rack-1").is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::PowershelfVrnvl72Delta {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        let mut gb200_config = config.clone();
        gb200_config.node_type = NodeType::PowershelfGb200Delta;
        PowershelfGb200Delta::from_config(&gb200_config, rack_id).map(|inner| Self {
            inner: inner.with_power_control(DeltaPowerControl::ChassisReset),
        })
    }
}

#[async_trait]
impl Node for PowershelfVrnvl72Delta {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::PowershelfVrnvl72Delta
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::PowershelfVrnvl72Delta.as_str().to_owned(),
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
    use crate::utilities::error::ErrorCode;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config_for(node_type: NodeType) -> NodeConfig {
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

    fn test_config() -> NodeConfig {
        test_config_for(NodeType::PowershelfVrnvl72Delta)
    }

    fn test_node(base_url: &str) -> PowershelfVrnvl72Delta {
        let mut powershelf =
            PowershelfVrnvl72Delta::from_config(&test_config(), "rack-01").unwrap();
        powershelf.inner.replace_redfish_for_test(base_url);
        powershelf
    }

    #[test]
    fn from_config_reports_vrnvl72_delta_type() {
        let powershelf = PowershelfVrnvl72Delta::from_config(&test_config(), "rack-01").unwrap();

        assert_eq!(powershelf.node_type(), NodeType::PowershelfVrnvl72Delta);
        assert_eq!(powershelf.id(), "ps-01");
        assert_eq!(powershelf.rack_id(), "rack-01");
        assert_eq!(
            powershelf.get_info()["type"],
            NodeType::PowershelfVrnvl72Delta.as_str()
        );
        assert_eq!(powershelf.get_info()["host"], "10.0.0.5");
        assert_eq!(powershelf.get_info()["port"], "8443");
    }

    #[test]
    fn from_config_rejects_mismatched_node_types() {
        for node_type in [
            NodeType::PowershelfGb200Delta,
            NodeType::PowershelfGb300Delta,
            NodeType::PowershelfVrnvl72Liteon,
        ] {
            let err = PowershelfVrnvl72Delta::from_config(&test_config_for(node_type), "rack-01")
                .err()
                .expect("mismatched node type must be rejected");

            assert_eq!(err.code, ErrorCode::Unimplemented, "{node_type}");
        }
    }

    #[test]
    fn from_config_requires_bmc_endpoint() {
        let mut config = test_config();
        config.bmc_endpoint = None;

        let err = PowershelfVrnvl72Delta::from_config(&config, "rack-01")
            .err()
            .expect("missing BMC endpoint must be rejected");

        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn get_power_state_delegates_to_shared_delta_tree() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{ "@odata.id": "/redfish/v1/Chassis/PowerShelf_0" }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/PowerShelf_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Manufacturer": "DELTA",
                "PowerSubsystem": {
                    "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem"
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "PowerSupplies": {
                    "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem/PowerSupplies"
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem/PowerSupplies",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{
                    "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem/PowerSupplies/1"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem/PowerSupplies/1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Oem": { "deltaenergysystems": { "Power": true } }
            })))
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        assert_eq!(powershelf.get_power_state().await.unwrap(), PowerState::On);
    }

    const SHELF_CHASSIS: &str = "/redfish/v1/Chassis/PowerShelf_0";
    const SHELF_RESET_TARGET: &str = "/redfish/v1/Chassis/PowerShelf_0/Actions/Chassis.Reset";
    const SHELF_RESET_ACTION_INFO: &str = "/redfish/v1/Chassis/PowerShelf_0/ResetActionInfo";

    /// Mounts the shelf chassis and `ResetActionInfo` captured from an
    /// AHE54V2037A shelf (PMC firmware 3.3.8; serial number omitted), plus a
    /// hypothetical non-Delta chassis that also advertises `#Chassis.Reset`.
    /// The captured `/redfish/v1/Chassis` lists only `PowerShelf_0`; the extra
    /// member checks that discovery never resets a non-Delta chassis.
    async fn mount_chassis_reset_tree(server: &MockServer, allowable: serde_json::Value) {
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [
                    { "@odata.id": "/redfish/v1/Chassis/PMC_0" },
                    { "@odata.id": SHELF_CHASSIS }
                ]
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/PMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id": "PMC_0",
                "Manufacturer": "NVIDIA",
                "Actions": {
                    "#Chassis.Reset": {
                        "target": "/redfish/v1/Chassis/PMC_0/Actions/Chassis.Reset"
                    }
                }
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(SHELF_CHASSIS))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "@odata.id": SHELF_CHASSIS,
                "@odata.type": "#Chassis.v1_26_0.Chassis",
                "Actions": {
                    "#Chassis.Reset": {
                        "@Redfish.ActionInfo": SHELF_RESET_ACTION_INFO,
                        "target": SHELF_RESET_TARGET
                    }
                },
                "ChassisType": "RackMount",
                "Id": "PowerShelf_0",
                "Links": {
                    "ManagedBy": [{ "@odata.id": "/redfish/v1/Managers/PMC_0" }],
                    "PowerDistribution": {
                        "@odata.id": "/redfish/v1/PowerEquipment/PowerShelves/1"
                    }
                },
                "Manufacturer": "DELTA",
                "MaxPowerWatts": 110000,
                "Model": "AHE54V2037A",
                "Name": "PowerShelf_0",
                "Oem": {
                    "deltaenergysystems": {
                        "ACPowerRampRate": {
                            "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/Oem/deltaenergysystems/ACPowerRampRate"
                        },
                        "AllSensors": {
                            "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/Oem/deltaenergysystems/AllSensors"
                        },
                        "PowerBrake": {
                            "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/Oem/deltaenergysystems/PowerBrake"
                        }
                    }
                },
                "PowerState": "On",
                "PowerSubsystem": {
                    "@odata.id": "/redfish/v1/Chassis/PowerShelf_0/PowerSubsystem"
                },
                "Status": { "Conditions": [], "Health": "OK", "HealthRollup": "OK", "State": "Enabled" }
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(SHELF_RESET_ACTION_INFO))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "@odata.id": SHELF_RESET_ACTION_INFO,
                "@odata.type": "#ActionInfo.v1_1_2.ActionInfo",
                "Id": "ResetActionInfo",
                "Name": "Reset Action Info",
                "Parameters": [{
                    "AllowableValues": allowable,
                    "DataType": "String",
                    "Name": "ResetType",
                    "Required": true
                }]
            })))
            .mount(server)
            .await;
    }

    fn reported_allowable_values() -> serde_json::Value {
        serde_json::json!(["On", "ForceOn", "ForceOff"])
    }

    async fn expect_reset(server: &MockServer, reset_type: &str, times: u64) {
        Mock::given(method("POST"))
            .and(path(SHELF_RESET_TARGET))
            .and(body_json(serde_json::json!({ "ResetType": reset_type })))
            .respond_with(ResponseTemplate::new(204))
            .expect(times)
            .mount(server)
            .await;
    }

    /// Any POST to the shelf's PowerShelves tree or the PMC chassis is wrong.
    async fn forbid_other_power_posts(server: &MockServer) {
        for target in [
            "/redfish/v1/Chassis/PMC_0/Actions/Chassis.Reset",
            "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOnPSUs",
            "/redfish/v1/PowerEquipment/PowerShelves/1/Oem/deltaenergysystems/Actions/PowerShelf.TurnOffPSUs",
        ] {
            Mock::given(method("POST"))
                .and(path(target))
                .respond_with(ResponseTemplate::new(204))
                .expect(0)
                .mount(server)
                .await;
        }
    }

    #[tokio::test]
    async fn set_power_state_on_posts_chassis_reset_on() {
        for op in [PowerOp::On, PowerOp::ForceOn] {
            let server = MockServer::start().await;
            mount_chassis_reset_tree(&server, reported_allowable_values()).await;
            expect_reset(&server, "On", 1).await;
            forbid_other_power_posts(&server).await;
            let powershelf = test_node(&server.uri());

            powershelf
                .set_power_state(op, PowerTargetType::System)
                .await
                .unwrap_or_else(|err| panic!("{op:?}: {}", err.message));
        }
    }

    #[tokio::test]
    async fn set_power_state_off_posts_chassis_reset_force_off() {
        for op in [PowerOp::ForceOff, PowerOp::Off, PowerOp::GracefulShutdown] {
            let server = MockServer::start().await;
            mount_chassis_reset_tree(&server, reported_allowable_values()).await;
            expect_reset(&server, "ForceOff", 1).await;
            forbid_other_power_posts(&server).await;
            let powershelf = test_node(&server.uri());

            powershelf
                .set_power_state(op, PowerTargetType::System)
                .await
                .unwrap_or_else(|err| panic!("{op:?}: {}", err.message));
        }
    }

    #[tokio::test]
    async fn set_power_state_does_not_need_power_shelves_oem_actions() {
        let server = MockServer::start().await;
        mount_chassis_reset_tree(&server, reported_allowable_values()).await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/PowerEquipment/PowerShelves"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{ "@odata.id": "/redfish/v1/PowerEquipment/PowerShelves/1" }]
            })))
            .expect(0)
            .mount(&server)
            .await;
        expect_reset(&server, "ForceOff", 1).await;
        let powershelf = test_node(&server.uri());

        powershelf
            .set_power_state(PowerOp::ForceOff, PowerTargetType::System)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn set_power_state_refuses_reset_type_outside_allowable_values() {
        let server = MockServer::start().await;
        mount_chassis_reset_tree(&server, serde_json::json!(["On"])).await;
        Mock::given(method("POST"))
            .and(path(SHELF_RESET_TARGET))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        let err = powershelf
            .set_power_state(PowerOp::ForceOff, PowerTargetType::System)
            .await
            .expect_err("ForceOff is not allowed by ResetActionInfo");

        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("ForceOff"), "{}", err.message);
    }

    #[tokio::test]
    async fn set_power_state_posts_reset_without_action_info() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{ "@odata.id": SHELF_CHASSIS }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SHELF_CHASSIS))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Manufacturer": "DELTA",
                "Actions": { "#Chassis.Reset": { "target": SHELF_RESET_TARGET } }
            })))
            .mount(&server)
            .await;
        expect_reset(&server, "On", 1).await;
        let powershelf = test_node(&server.uri());

        powershelf
            .set_power_state(PowerOp::On, PowerTargetType::System)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn set_power_state_fails_without_delta_chassis_reset() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [
                    { "@odata.id": "/redfish/v1/Chassis/PMC_0" },
                    { "@odata.id": SHELF_CHASSIS }
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/PMC_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Manufacturer": "NVIDIA",
                "Actions": {
                    "#Chassis.Reset": {
                        "target": "/redfish/v1/Chassis/PMC_0/Actions/Chassis.Reset"
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(SHELF_CHASSIS))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Manufacturer": "DELTA",
                "Actions": {}
            })))
            .mount(&server)
            .await;
        forbid_other_power_posts(&server).await;
        let powershelf = test_node(&server.uri());

        let err = powershelf
            .set_power_state(PowerOp::On, PowerTargetType::System)
            .await
            .expect_err("no Delta chassis advertises #Chassis.Reset");

        assert!(
            err.message
                .contains("no Delta powershelf chassis with #Chassis.Reset"),
            "{}",
            err.message
        );
    }

    #[tokio::test]
    async fn set_power_state_rejects_unsupported_operations() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&server)
            .await;
        let powershelf = test_node(&server.uri());

        for op in [
            PowerOp::PowerCycle,
            PowerOp::GracefulRestart,
            PowerOp::ForceRestart,
            PowerOp::Nmi,
        ] {
            let err = powershelf
                .set_power_state(op, PowerTargetType::System)
                .await
                .expect_err("unsupported op must be rejected");
            assert_eq!(err.code, ErrorCode::InvalidArgument, "{op:?}");
        }
    }
}
