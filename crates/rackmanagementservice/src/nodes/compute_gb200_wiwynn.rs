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

use crate::domain::node::*;
use crate::domain::rack::NodeConfig;
use crate::nodes::compute_gb200_nvidia::NvidiaGb200Compute;
use crate::transport::redfish_client::NvidiaMnnvlinkTopology;
use crate::utilities::error::{Result, RmsError};

/// Wiwynn GB200 compute node.
///
/// Wiwynn GB200 uses the reference GB200 runtime behavior. Its descriptor-selected
/// internal identity allows firmware-object apply to preserve both BMC images
/// supplied by the firmware manifest without changing NVIDIA GB200 policy or the protobuf enum.
pub struct WiwynnGb200Compute {
    inner: NvidiaGb200Compute,
}

impl WiwynnGb200Compute {
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::ComputeGb200Wiwynn {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        NvidiaGb200Compute::from_config(config, rack_id).map(|inner| Self { inner })
    }

    pub async fn get_mnnvlink_topology(&self) -> Result<NvidiaMnnvlinkTopology> {
        self.inner.get_mnnvlink_topology().await
    }
}

#[async_trait]
impl Node for WiwynnGb200Compute {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::ComputeGb200Wiwynn
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::ComputeGb200Wiwynn.as_str().to_owned(),
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

    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        self.inner.verify_firmware_package_versions(targets).await
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

    async fn start_firmware_upload(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
    ) -> Result<String> {
        self.inner.start_firmware_upload(target, force_update).await
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

    fn config(node_type: NodeType) -> NodeConfig {
        NodeConfig {
            id: "c-01".into(),
            node_type,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.1".into(),
                    mac_address: "aa:bb:cc:dd:ee:ff".into(),
                    port: 8443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "secret")),
                true,
            )),
            host_endpoint: None,
            expected_inventory: None,
        }
    }

    #[test]
    fn from_config_reports_wiwynn_type() {
        let compute =
            WiwynnGb200Compute::from_config(&config(NodeType::ComputeGb200Wiwynn), "rack-01")
                .unwrap();

        assert_eq!(compute.node_type(), NodeType::ComputeGb200Wiwynn);
        assert_eq!(
            compute.get_info()["type"],
            NodeType::ComputeGb200Wiwynn.as_str()
        );
        assert_eq!(compute.get_info()["host"], "10.0.0.1");
        assert_eq!(compute.get_info()["port"], "8443");
    }

    #[test]
    fn from_config_rejects_other_node_types() {
        let error =
            WiwynnGb200Compute::from_config(&config(NodeType::ComputeGb200Nvidia), "rack-01")
                .err()
                .expect("NVIDIA GB200 config should be rejected");

        assert_eq!(
            error.message,
            "create_node not supported for compute_gb200_nvidia nodes"
        );
    }
}
