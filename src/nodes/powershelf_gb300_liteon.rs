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

use crate::domain::node::{
    FirmwareActivationRequest, FirmwareActivationSummary, FirmwareInfo, FirmwareTarget,
    FirmwareTaskStatus, FirmwareUpdateOptions, FirmwareUpdateOutcome, Node, NodeType, PowerOp,
    PowerState, PowerTargetType,
};
use crate::domain::rack::NodeConfig;
use crate::nodes::powershelf_gb200_liteon::PowershelfGb200Liteon;
use crate::utilities::error::{Result, RmsError};

/// LiteOn GB300 powershelf node.
///
/// GB300 LiteOn powershelves currently use the GB200 LiteOn Redfish and
/// NVFWUPD behavior. This wrapper keeps the concrete node type distinct while
/// delegating I/O to the GB200 implementation.
pub struct PowershelfGb300Liteon {
    inner: PowershelfGb200Liteon,
}

impl PowershelfGb300Liteon {
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::PowershelfGb300Liteon {
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
impl Node for PowershelfGb300Liteon {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::PowershelfGb300Liteon
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::PowershelfGb300Liteon.as_str().to_owned(),
        );
        info
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

    #[test]
    fn from_config_reports_gb300_type() {
        let config = NodeConfig {
            id: "ps-01".into(),
            node_type: NodeType::PowershelfGb300Liteon,
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
        };

        let ps = PowershelfGb300Liteon::from_config(&config, "rack-01").unwrap();

        assert_eq!(ps.node_type(), NodeType::PowershelfGb300Liteon);
        assert_eq!(
            ps.get_info()["type"],
            NodeType::PowershelfGb300Liteon.as_str()
        );
    }
}
