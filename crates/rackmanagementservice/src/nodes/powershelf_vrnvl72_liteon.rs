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
use crate::nodes::powershelf_gb200_liteon::PowershelfGb200Liteon;
use crate::utilities::error::{Result, RmsError};

/// LiteOn VR NVL72 powershelf node (PF-1114-1RD).
///
/// This shelf exposes standard DMTF `PowerSubsystem/PowerSupplies` resources
/// and LiteOn OEM inventory at the fixed
/// `/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits` URI. Redfish
/// discovery omits links to the OEM tree, but the resources exist. Direct OEM
/// PSU firmware updates do not take effect. PMC and all-PSU updates use
/// `UpdateService.HttpPushUri`. The chassis has no `#Chassis.Off` action. The
/// shared GB200 LiteOn implementation handles inventory and power I/O while
/// this wrapper preserves the concrete node type.
pub struct PowershelfVrnvl72Liteon {
    inner: PowershelfGb200Liteon,
}

impl PowershelfVrnvl72Liteon {
    /// Builds a LiteOn VR NVL72 shelf node backed by a GB200 LiteOn node.
    ///
    /// # Errors
    ///
    /// Returns `Unimplemented` if `config.node_type` is not
    /// `PowershelfVrnvl72Liteon`, or any error from
    /// [`PowershelfGb200Liteon::from_config`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rackmanagementservice::domain::node::{Node, NodeType};
    /// use rackmanagementservice::domain::rack::NodeConfig;
    /// use rackmanagementservice::nodes::powershelf_vrnvl72_liteon::PowershelfVrnvl72Liteon;
    ///
    /// let config = NodeConfig::new("ps-0", NodeType::PowershelfVrnvl72Liteon, "192.0.2.10");
    /// let shelf = PowershelfVrnvl72Liteon::from_config(&config, "rack-1")?;
    /// assert_eq!(shelf.node_type(), NodeType::PowershelfVrnvl72Liteon);
    ///
    /// let other = NodeConfig::new("ps-1", NodeType::PowershelfGb200Liteon, "192.0.2.11");
    /// assert!(PowershelfVrnvl72Liteon::from_config(&other, "rack-1").is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::PowershelfVrnvl72Liteon {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        let mut gb200_config = config.clone();
        gb200_config.node_type = NodeType::PowershelfGb200Liteon;
        PowershelfGb200Liteon::from_config(&gb200_config, rack_id).map(|inner| Self { inner })
    }
}

#[async_trait]
impl Node for PowershelfVrnvl72Liteon {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::PowershelfVrnvl72Liteon
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::PowershelfVrnvl72Liteon.as_str().to_owned(),
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
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(node_type: NodeType) -> NodeConfig {
        NodeConfig {
            id: "ps-01".into(),
            node_type,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.5".into(),
                    mac_address: String::new(),
                    port: 443,
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

    #[test]
    fn from_config_reports_vrnvl72_type() {
        let ps =
            PowershelfVrnvl72Liteon::from_config(&config(NodeType::PowershelfVrnvl72Liteon), "r-1")
                .unwrap();

        assert_eq!(ps.node_type(), NodeType::PowershelfVrnvl72Liteon);
        assert_eq!(
            ps.get_info()["type"],
            NodeType::PowershelfVrnvl72Liteon.as_str()
        );
    }

    #[test]
    fn from_config_rejects_mismatched_node_type() {
        let err =
            PowershelfVrnvl72Liteon::from_config(&config(NodeType::PowershelfGb200Liteon), "r-1")
                .err()
                .expect("mismatched node type must be rejected");

        assert_eq!(err.code, ErrorCode::Unimplemented);
    }

    #[tokio::test]
    async fn set_power_state_serves_graceful_requests_as_force_off() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Members": [{ "@odata.id": "/redfish/v1/Chassis/powershelf" }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/Chassis/powershelf"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Actions": {
                    "#Chassis.ForceOff": {
                        "target": "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
                    }
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(2)
            .mount(&server)
            .await;
        let mut ps =
            PowershelfVrnvl72Liteon::from_config(&config(NodeType::PowershelfVrnvl72Liteon), "r-1")
                .unwrap();
        ps.inner.replace_redfish_for_test(&server.uri());

        for op in [PowerOp::Off, PowerOp::GracefulShutdown] {
            ps.set_power_state(op, PowerTargetType::System)
                .await
                .unwrap_or_else(|err| panic!("{op:?}: {err}"));
        }
    }
}
