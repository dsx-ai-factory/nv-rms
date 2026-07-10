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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::domain::node::{
    FirmwareActivationRequest, FirmwareActivationSummary, FirmwareInfo, FirmwareTarget,
    FirmwareTaskStatus, FirmwareUpdateOptions, FirmwareUpdateOutcome, FirmwareVersionCheckSummary,
    Node, NodeKind, NodeType, PowerOp, PowerState, PowerTargetType,
};
use crate::domain::rack::NodeConfig;
use crate::nodes::compute_gb200_nvidia::NvidiaGb200Compute;
use crate::nodes::compute_gb300_lenovo::LenovoGb300Compute;
use crate::nodes::compute_gb300_nvidia::NvidiaGb300Compute;
use crate::nodes::powershelf_gb200_delta::PowershelfGb200Delta;
use crate::nodes::powershelf_gb200_liteon::PowershelfGb200Liteon;
use crate::nodes::powershelf_gb300_delta::PowershelfGb300Delta;
use crate::nodes::powershelf_gb300_liteon::PowershelfGb300Liteon;
use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;
use crate::nodes::switch_gb300_nvidia::SwitchGb300Nvidia;
use crate::utilities::error::{Result, RmsError};

macro_rules! match_node_instance {
    ($self:expr, $node:ident => $body:expr) => {
        match $self {
            NodeInstance::ComputeGb200Nvidia($node) => $body,
            NodeInstance::PowershelfGb200Liteon($node) => $body,
            NodeInstance::SwitchGb200Nvidia($node) => $body,
            NodeInstance::PowershelfGb200Delta($node) => $body,
            NodeInstance::ComputeGb300Nvidia($node) => $body,
            NodeInstance::SwitchGb300Nvidia($node) => $body,
            NodeInstance::PowershelfGb300Liteon($node) => $body,
            NodeInstance::PowershelfGb300Delta($node) => $body,
            NodeInstance::ComputeGb300Lenovo($node) => $body,
            #[cfg(test)]
            NodeInstance::Test($node) => $body,
        }
    };
}

/// Switch firmware capability used by inventory-backed switch firmware RPCs.
#[async_trait]
pub(crate) trait SwitchFirmwareManagement: Node {
    async fn list_firmware(&self, component: &str, list_files: bool) -> Result<Value>;
    async fn push_firmware_file(
        &self,
        local_file_path: &str,
        component: &str,
        filename: &str,
    ) -> Result<()>;
    async fn poll_job_blocking(
        &self,
        job_id: &str,
        timeout: Duration,
        interval: Duration,
    ) -> Result<bool>;
    async fn list_system_images(&self) -> Result<Value>;
}

/// Switch scale-up fabric capability used by request-scoped ScaleUp RPCs.
#[async_trait]
pub(crate) trait SwitchScaleUpManagement: Node {
    fn as_switch_gb200(&self) -> Option<&SwitchGb200Nvidia> {
        None
    }

    async fn set_cluster_state(&self, enabled: bool) -> Result<()>;
    async fn get_cluster_state(&self) -> Result<Value>;
    async fn get_cluster_apps_status(&self, app_name: &str) -> Result<Value>;
    async fn enable_grpc_for_external_clients(
        &self,
        app_name: &str,
        enabled: bool,
    ) -> Result<Value>;
    async fn gnmi_service(&self, enabled: bool) -> Result<Value>;
    async fn check_grpc_status(&self, app_name: &str, target_switch: &str) -> Result<Value>;
    async fn restart_cluster_app(&self, app_name: &str) -> Result<()>;
}

#[async_trait]
trait SwitchDeviceInfo: Node {
    async fn get_chassis_location_info(&self) -> Result<Value>;
}

/// Closed set of node implementations supported by RMS.
///
/// `NodeType` is the identity source, and this enum is the corresponding
/// concrete runtime value. Keeping inventory nodes as this closed enum makes
/// per-node dispatch exhaustive instead of relying on trait-object node storage
/// plus downcasts.
pub enum NodeInstance {
    ComputeGb200Nvidia(Box<NvidiaGb200Compute>),
    PowershelfGb200Liteon(Box<PowershelfGb200Liteon>),
    SwitchGb200Nvidia(Box<SwitchGb200Nvidia>),
    PowershelfGb200Delta(Box<PowershelfGb200Delta>),
    ComputeGb300Nvidia(Box<NvidiaGb300Compute>),
    SwitchGb300Nvidia(Box<SwitchGb300Nvidia>),
    PowershelfGb300Liteon(Box<PowershelfGb300Liteon>),
    PowershelfGb300Delta(Box<PowershelfGb300Delta>),
    ComputeGb300Lenovo(Box<LenovoGb300Compute>),
    #[cfg(test)]
    Test(Box<dyn Node>),
}

impl NodeInstance {
    /// Wrap a test node so rack/handler tests can exercise orchestration
    /// behavior without constructing a real hardware client.
    #[cfg(test)]
    pub(crate) fn from_test_node(node: impl Node + 'static) -> Self {
        Self::Test(Box::new(node))
    }

    /// Build the concrete node implementation selected by `config.node_type`.
    ///
    /// This is the only place that maps the canonical [`NodeType`] identity to
    /// a runtime node value. New concrete node types should be added here and
    /// to the [`NodeInstance`] enum together so dispatch stays exhaustive.
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        match config.node_type {
            NodeType::ComputeGb200Nvidia => NvidiaGb200Compute::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::ComputeGb200Nvidia),
            NodeType::PowershelfGb200Liteon => PowershelfGb200Liteon::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::PowershelfGb200Liteon),
            NodeType::SwitchGb200Nvidia => SwitchGb200Nvidia::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::SwitchGb200Nvidia),
            NodeType::PowershelfGb200Delta => PowershelfGb200Delta::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::PowershelfGb200Delta),
            NodeType::ComputeGb300Nvidia => NvidiaGb300Compute::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::ComputeGb300Nvidia),
            NodeType::SwitchGb300Nvidia => SwitchGb300Nvidia::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::SwitchGb300Nvidia),
            NodeType::PowershelfGb300Liteon => PowershelfGb300Liteon::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::PowershelfGb300Liteon),
            NodeType::PowershelfGb300Delta => PowershelfGb300Delta::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::PowershelfGb300Delta),
            NodeType::ComputeGb300Lenovo => LenovoGb300Compute::from_config(config, rack_id)
                .map(Box::new)
                .map(Self::ComputeGb300Lenovo),
        }
    }

    /// Build a shareable node instance for rack inventory or request-local use.
    pub fn create(config: &NodeConfig, rack_id: &str) -> Result<Arc<Self>> {
        Self::from_config(config, rack_id).map(Arc::new)
    }

    /// Build switch scale-up fabric behavior from a switch node config.
    pub(crate) fn create_switch_scale_up(
        config: &NodeConfig,
        rack_id: &str,
    ) -> Result<Box<dyn SwitchScaleUpManagement>> {
        let node_type = config.node_type;
        Self::from_switch_config(config, rack_id)?
            .into_switch_scale_up()
            .ok_or_else(|| {
                RmsError::invalid_argument(format!("node type {node_type} is not a switch"))
            })
    }

    /// Build switch password-management behavior from a switch node config.
    pub(crate) fn create_switch_password(
        config: &NodeConfig,
        rack_id: &str,
    ) -> Result<Box<SwitchGb200Nvidia>> {
        let node_type = config.node_type;
        match Self::from_switch_config(config, rack_id)? {
            Self::SwitchGb200Nvidia(node) | Self::SwitchGb300Nvidia(node) => Ok(node),
            _ => Err(RmsError::invalid_argument(format!(
                "node type {node_type} is not a switch"
            ))),
        }
    }

    /// Return switch firmware behavior when this instance is a switch.
    pub(crate) fn as_switch_firmware(&self) -> Option<&dyn SwitchFirmwareManagement> {
        match self {
            Self::SwitchGb200Nvidia(node) => Some(node.as_ref()),
            Self::SwitchGb300Nvidia(node) => Some(node.as_ref()),
            #[cfg(test)]
            Self::Test(_) => None,
            _ => None,
        }
    }

    /// Fetch device-info payloads for node types that expose one.
    ///
    /// Compute nodes return MNNVL topology data, switches return chassis
    /// location data, and powershelves return `Ok(None)` because they do not
    /// currently expose this RPC surface.
    pub async fn get_device_info(&self) -> Result<Option<Value>> {
        match self {
            Self::ComputeGb200Nvidia(node) => node.get_mnnvlink_topology().await.map(Some),
            Self::ComputeGb300Nvidia(node) => node.get_mnnvlink_topology().await.map(Some),
            Self::ComputeGb300Lenovo(node) => node.get_mnnvlink_topology().await.map(Some),
            Self::SwitchGb200Nvidia(node) | Self::SwitchGb300Nvidia(node) => {
                let value = SwitchDeviceInfo::get_chassis_location_info(node.as_ref()).await?;

                Ok(Some(value))
            }
            Self::PowershelfGb200Liteon(_)
            | Self::PowershelfGb200Delta(_)
            | Self::PowershelfGb300Liteon(_)
            | Self::PowershelfGb300Delta(_) => Ok(None),
            #[cfg(test)]
            Self::Test(_) => Ok(None),
        }
    }

    /// Return the shared NVUE client for a host-backed switch.
    pub(crate) fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
        match self {
            Self::SwitchGb200Nvidia(node) | Self::SwitchGb300Nvidia(node) => {
                node.optional_nvue_client()
            }
            _ => None,
        }
    }

    fn from_switch_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        let node_type = config.node_type;
        if node_type.kind() != NodeKind::Switch {
            return Err(RmsError::invalid_argument(format!(
                "node type {node_type} is not a switch"
            )));
        }

        Self::from_config(config, rack_id)
    }

    fn into_switch_scale_up(self) -> Option<Box<dyn SwitchScaleUpManagement>> {
        match self {
            Self::SwitchGb200Nvidia(node) => Some(node),
            Self::SwitchGb300Nvidia(node) => Some(node),
            #[cfg(test)]
            Self::Test(_) => None,
            _ => None,
        }
    }
}

#[async_trait]
impl SwitchFirmwareManagement for SwitchGb200Nvidia {
    async fn list_firmware(&self, component: &str, list_files: bool) -> Result<Value> {
        SwitchGb200Nvidia::list_firmware(self, component, list_files).await
    }

    async fn push_firmware_file(
        &self,
        local_file_path: &str,
        component: &str,
        filename: &str,
    ) -> Result<()> {
        SwitchGb200Nvidia::push_firmware_file(self, local_file_path, component, filename).await
    }

    async fn poll_job_blocking(
        &self,
        job_id: &str,
        timeout: Duration,
        interval: Duration,
    ) -> Result<bool> {
        SwitchGb200Nvidia::poll_job_blocking(self, job_id, timeout, interval).await
    }

    async fn list_system_images(&self) -> Result<Value> {
        SwitchGb200Nvidia::list_system_images(self).await
    }
}

#[async_trait]
impl SwitchScaleUpManagement for SwitchGb200Nvidia {
    fn as_switch_gb200(&self) -> Option<&SwitchGb200Nvidia> {
        Some(self)
    }

    async fn set_cluster_state(&self, enabled: bool) -> Result<()> {
        SwitchGb200Nvidia::set_cluster_state(self, enabled).await
    }

    async fn get_cluster_state(&self) -> Result<Value> {
        SwitchGb200Nvidia::get_cluster_state(self).await
    }

    async fn get_cluster_apps_status(&self, app_name: &str) -> Result<Value> {
        SwitchGb200Nvidia::get_cluster_apps_status(self, app_name).await
    }

    async fn enable_grpc_for_external_clients(
        &self,
        app_name: &str,
        enabled: bool,
    ) -> Result<Value> {
        SwitchGb200Nvidia::enable_grpc_for_external_clients(self, app_name, enabled).await
    }

    async fn gnmi_service(&self, enabled: bool) -> Result<Value> {
        SwitchGb200Nvidia::gnmi_service(self, enabled).await
    }

    async fn check_grpc_status(&self, app_name: &str, target_switch: &str) -> Result<Value> {
        SwitchGb200Nvidia::check_grpc_status(self, app_name, target_switch).await
    }

    async fn restart_cluster_app(&self, app_name: &str) -> Result<()> {
        SwitchGb200Nvidia::restart_cluster_app(self, app_name).await
    }
}

#[async_trait]
impl SwitchDeviceInfo for SwitchGb200Nvidia {
    async fn get_chassis_location_info(&self) -> Result<Value> {
        SwitchGb200Nvidia::get_chassis_location_info(self).await
    }
}

#[async_trait]
impl Node for NodeInstance {
    fn id(&self) -> &str {
        match_node_instance!(self, node => node.id())
    }

    fn rack_id(&self) -> &str {
        match_node_instance!(self, node => node.rack_id())
    }

    fn node_type(&self) -> NodeType {
        match_node_instance!(self, node => node.node_type())
    }

    fn get_info(&self) -> std::collections::HashMap<String, String> {
        match_node_instance!(self, node => node.get_info())
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        match_node_instance!(self, node => node.get_power_state().await)
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        match_node_instance!(self, node => node.set_power_state(op, target).await)
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        match_node_instance!(self, node => node.get_firmware_inventory().await)
    }

    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        match_node_instance!(self, node => node.verify_firmware_package_versions(targets).await)
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        match_node_instance!(self, node => node.update_firmware(target, force_update, options).await)
    }

    async fn start_firmware_upload(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
    ) -> Result<String> {
        match_node_instance!(self, node => node.start_firmware_upload(target, force_update).await)
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        match_node_instance!(self, node => node.poll_firmware_task(task_id).await)
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        match_node_instance!(self, node => node.activate_firmware_with(request).await)
    }

    async fn activate_firmware(&self) -> Result<()> {
        match_node_instance!(self, node => node.activate_firmware().await)
    }

    fn supports_bmc_aux_powercycle(&self) -> bool {
        match_node_instance!(self, node => node.supports_bmc_aux_powercycle())
    }

    async fn bmc_aux_powercycle(&self) -> Result<()> {
        match_node_instance!(self, node => node.bmc_aux_powercycle().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct BmcPowercycleTestNode {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Node for BmcPowercycleTestNode {
        fn id(&self) -> &str {
            "test-node"
        }

        fn rack_id(&self) -> &str {
            "test-rack"
        }

        fn node_type(&self) -> NodeType {
            NodeType::SwitchGb200Nvidia
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn supports_bmc_aux_powercycle(&self) -> bool {
            true
        }

        async fn bmc_aux_powercycle(&self) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn endpoint_config(ip_address: &str, mac_address: &str) -> EndpointConfig {
        EndpointConfig::with_credentials(
            Endpoint {
                ip_address: ip_address.to_owned(),
                mac_address: mac_address.to_owned(),
                port: 443,
                host_name: None,
            },
            Some(EndpointCredentials::new("admin", "password")),
            false,
        )
    }

    #[test]
    fn create_switch_scale_up_accepts_gb300_switch_type() {
        let config = NodeConfig {
            id: "sw-01".to_owned(),
            node_type: NodeType::SwitchGb300Nvidia,
            bmc_endpoint: None,
            host_endpoint: Some(endpoint_config("10.0.0.11", "11:22:33:44:55:66")),
        };

        let switch = NodeInstance::create_switch_scale_up(&config, "rack-01").unwrap();

        assert_eq!(switch.node_type(), NodeType::SwitchGb300Nvidia);
    }

    #[test]
    fn create_switch_scale_up_rejects_non_switch_node_type() {
        let config = NodeConfig {
            id: "c-01".to_owned(),
            node_type: NodeType::ComputeGb200Nvidia,
            bmc_endpoint: Some(endpoint_config("10.0.0.10", "aa:bb:cc:dd:ee:ff")),
            host_endpoint: None,
        };

        let Err(err) = NodeInstance::create_switch_scale_up(&config, "rack-01") else {
            panic!("expected non-switch config to be rejected");
        };

        assert!(err.message.contains("is not a switch"));
    }

    #[tokio::test]
    async fn forwards_bmc_aux_powercycle_capability_to_inner_node() {
        let calls = Arc::new(AtomicUsize::new(0));
        let node = NodeInstance::from_test_node(BmcPowercycleTestNode {
            calls: Arc::clone(&calls),
        });

        assert!(node.supports_bmc_aux_powercycle());
        node.bmc_aux_powercycle().await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
