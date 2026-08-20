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
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::utilities::error::{ErrorCode, Result, RmsError};

// ── Power ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerState {
    Unknown,
    On,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerOp {
    On,
    Off,
    ForceOn,
    ForceOff,
    PowerCycle,
    GracefulShutdown,
    GracefulRestart,
    ForceRestart,
    Nmi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PowerTargetType {
    #[default]
    System,
    BMC,
    HMC,
}

// ── Firmware ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FirmwareType {
    #[default]
    Unknown,
    BMC,
    BIOS,
    CPLD,
    FPGA,
    NIC,
    HBA,
    HMC,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirmwareInfo {
    pub name: String,
    pub version: String,
    pub firmware_type: FirmwareType,
    pub updateable: bool,
    pub target: String,
    pub health: String,
    pub sku: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirmwareTarget {
    pub component: String,
    pub firmware_file: String,
    pub expected_version: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FirmwareTaskStatus {
    pub completed: bool,
    pub percent: i32,
    pub state: String,
    pub status: String,
    pub message: String,
}

/// Canonical RMS node type.
///
/// This is the source of truth for stable node type names and the derived
/// routing metadata used by racks, endpoint selection, and NVFWUPD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeType {
    ComputeGb200Nvidia,
    ComputeGb200Wiwynn,
    PowershelfGb200Liteon,
    SwitchGb200Nvidia,
    PowershelfGb200Delta,
    ComputeGb300Nvidia,
    SwitchGb300Nvidia,
    PowershelfGb300Liteon,
    PowershelfGb300Delta,
    ComputeGb300Lenovo,
    ComputeGb300Supermicro,
    ComputeVrnvl72Nvidia,
    SwitchVrnvl72Nvidia,
}

/// Broad device kind used for policies that only care about node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeKind {
    Compute,
    Powershelf,
    Switch,
}

/// Product family that determines which rack type a node belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProductFamily {
    Gb200,
    Gb300,
    Vrnvl72,
}

/// NVFWUPD server-type profile for a node.
///
/// This is separate from [`NodeKind`] and [`ProductFamily`] because the
/// external NVFWUPD API groups some RMS node types together differently than
/// inventory and rack routing do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NvfwupdServerProfile {
    ComputeGb200,
    ComputeGb300,
    ComputeVrnvl72,
    Powershelf,
    SwitchGb200,
    SwitchGb300,
    SwitchVrnvl72,
}

impl NodeType {
    /// Stable protobuf/config string for this node type.
    ///
    /// Prefer `Display` in formatting contexts. This method remains
    /// available because callers need a `const` string in tables and static
    /// metadata.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ComputeGb200Nvidia => "compute_gb200_nvidia",
            Self::ComputeGb200Wiwynn => "compute_gb200_wiwynn",
            Self::PowershelfGb200Liteon => "powershelf_gb200_liteon",
            Self::SwitchGb200Nvidia => "switch_gb200_nvidia",
            Self::PowershelfGb200Delta => "powershelf_gb200_delta",
            Self::ComputeGb300Nvidia => "compute_gb300_nvidia",
            Self::SwitchGb300Nvidia => "switch_gb300_nvidia",
            Self::PowershelfGb300Liteon => "powershelf_gb300_liteon",
            Self::PowershelfGb300Delta => "powershelf_gb300_delta",
            Self::ComputeGb300Lenovo => "compute_gb300_lenovo",
            Self::ComputeGb300Supermicro => "compute_gb300_supermicro",
            Self::ComputeVrnvl72Nvidia => "compute_vrnvl72_nvidia",
            Self::SwitchVrnvl72Nvidia => "switch_vrnvl72_nvidia",
        }
    }

    /// Broad device kind for endpoint and credential policy.
    pub const fn kind(self) -> NodeKind {
        match self {
            Self::ComputeGb200Nvidia
            | Self::ComputeGb200Wiwynn
            | Self::ComputeGb300Nvidia
            | Self::ComputeGb300Lenovo
            | Self::ComputeGb300Supermicro
            | Self::ComputeVrnvl72Nvidia => NodeKind::Compute,
            Self::PowershelfGb200Liteon
            | Self::PowershelfGb200Delta
            | Self::PowershelfGb300Liteon
            | Self::PowershelfGb300Delta => NodeKind::Powershelf,
            Self::SwitchGb200Nvidia | Self::SwitchGb300Nvidia | Self::SwitchVrnvl72Nvidia => {
                NodeKind::Switch
            }
        }
    }

    /// Product family that owns this node type.
    pub const fn product_family(self) -> ProductFamily {
        match self {
            Self::ComputeGb200Nvidia
            | Self::ComputeGb200Wiwynn
            | Self::PowershelfGb200Liteon
            | Self::SwitchGb200Nvidia
            | Self::PowershelfGb200Delta => ProductFamily::Gb200,
            Self::ComputeGb300Nvidia
            | Self::SwitchGb300Nvidia
            | Self::PowershelfGb300Liteon
            | Self::PowershelfGb300Delta
            | Self::ComputeGb300Lenovo
            | Self::ComputeGb300Supermicro => ProductFamily::Gb300,
            Self::ComputeVrnvl72Nvidia | Self::SwitchVrnvl72Nvidia => ProductFamily::Vrnvl72,
        }
    }

    /// Whether management workflows for this node type use the host endpoint.
    ///
    /// Switch NVUE/NVOS workflows authenticate through the host endpoint;
    /// compute and powershelf Redfish workflows use BMC credentials.
    pub const fn uses_host_management_endpoint(self) -> bool {
        matches!(self.kind(), NodeKind::Switch)
    }

    /// NVFWUPD server-type profile for firmware workflows.
    pub const fn nvfwupd_server_profile(self) -> NvfwupdServerProfile {
        match self {
            Self::ComputeGb200Nvidia | Self::ComputeGb200Wiwynn => {
                NvfwupdServerProfile::ComputeGb200
            }
            Self::ComputeGb300Nvidia | Self::ComputeGb300Lenovo | Self::ComputeGb300Supermicro => {
                NvfwupdServerProfile::ComputeGb300
            }
            Self::ComputeVrnvl72Nvidia => NvfwupdServerProfile::ComputeVrnvl72,
            Self::PowershelfGb200Liteon
            | Self::PowershelfGb200Delta
            | Self::PowershelfGb300Liteon
            | Self::PowershelfGb300Delta => NvfwupdServerProfile::Powershelf,
            Self::SwitchGb200Nvidia => NvfwupdServerProfile::SwitchGb200,
            Self::SwitchGb300Nvidia => NvfwupdServerProfile::SwitchGb300,
            Self::SwitchVrnvl72Nvidia => NvfwupdServerProfile::SwitchVrnvl72,
        }
    }
}

impl ProductFamily {
    /// Stable rack type string used by inventory and rack creation APIs.
    pub const fn rack_type(self) -> &'static str {
        match self {
            Self::Gb200 => "NVL_GB200",
            Self::Gb300 => "NVL_GB300",
            Self::Vrnvl72 => "NVL_VRNVL72",
        }
    }

    /// Human-readable rack model string returned in rack metadata.
    pub const fn model(self) -> &'static str {
        match self {
            Self::Gb200 => "GB200 NVL",
            Self::Gb300 => "GB300 NVL",
            Self::Vrnvl72 => "VR NVL72",
        }
    }
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for NodeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compute => f.write_str("compute"),
            Self::Powershelf => f.write_str("powershelf"),
            Self::Switch => f.write_str("switch"),
        }
    }
}

/// Expected NVFWUPD AP inventory selected by opaque deployment metadata.
///
/// The profile name is returned through inventory APIs while the AP names are
/// passed to NVFWUPD. Both values are immutable and shared by all node
/// instances that select the same configured profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedInventoryPolicy {
    pub profile: Arc<str>,
    pub ap_names: Arc<[String]>,
}

#[derive(Debug, Clone, Default)]
pub struct FirmwareUpdateOptions {
    pub special_json: Option<Value>,
    pub oem_parameters: Option<Value>,
    pub liteon_device_id: Option<String>,
    pub apply_time: Option<String>,
    pub cancellation: Option<CancellationToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirmwareTaskHandle {
    pub task_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FirmwareUpdateSummary {
    pub message: String,
    pub task_ids: Vec<String>,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FirmwareUpdateOutcome {
    Started(FirmwareTaskHandle),
    Completed(FirmwareUpdateSummary),
    Skipped { reason: String },
}

#[derive(Debug, Clone)]
pub struct FirmwareActivationRequest {
    pub mode: FirmwareActivationMode,
    pub cancellation: Option<CancellationToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FirmwareActivationMode {
    SingleCommand(FirmwareActivationCommand),
    FullGb200Compute,
    SwitchPowerCycle,
    PowerShelfReset { force: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirmwareActivationCommand {
    RfPowerOn,
    RfPowerOff,
    RfPowerCycle,
    RfAuxPowerCycle,
    RfPowerStatus,
    RfPowerShelfReset,
    RfPowerShelfResetForce,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FirmwareActivationSummary {
    pub message: String,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FirmwareVersionCheckSummary {
    pub matched: bool,
    pub details: Value,
}

// ── Node Trait ──

/// Trait for all node implementations.
///
/// Each concrete node type implements protocol-specific I/O behind this
/// interface. Nodes are pure async I/O — they have no knowledge of job
/// tracking, scheduling, or the rack hierarchy.
///
/// Methods with default implementations return `Unimplemented` errors,
/// allowing node types to opt in to only the operations they support
/// (e.g., Powershelf does not support `activate_firmware`).
///
#[async_trait]
pub trait Node: Send + Sync {
    // ── Identity (sync, returns stored data) ──

    fn id(&self) -> &str;
    fn rack_id(&self) -> &str;
    fn node_type(&self) -> NodeType;
    fn get_info(&self) -> HashMap<String, String>;
    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        None
    }

    // ── Power (async, performs I/O) ──

    async fn get_power_state(&self) -> Result<PowerState> {
        Err(RmsError::unimplemented("get_power_state", self.node_type()))
    }

    async fn set_power_state(&self, _op: PowerOp, _target: PowerTargetType) -> Result<()> {
        Err(RmsError::unimplemented("set_power_state", self.node_type()))
    }

    // ── Firmware (async, performs I/O) ──

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        Err(RmsError::unimplemented(
            "get_firmware_inventory",
            self.node_type(),
        ))
    }

    async fn verify_firmware_package_versions(
        &self,
        _targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        Err(RmsError::unimplemented(
            "verify_firmware_package_versions",
            self.node_type(),
        ))
    }

    /// Primary firmware update entrypoint for RMS orchestration.
    ///
    /// The default implementation preserves the legacy task-id based contract
    /// until each concrete node is moved to the NVFWUPD facade in Phase 5c-5e.
    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        _options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        match self.start_firmware_upload(target, force_update).await {
            Ok(task_id) => Ok(FirmwareUpdateOutcome::Started(FirmwareTaskHandle {
                task_id,
            })),
            Err(e) if e.code == ErrorCode::AlreadyExists => {
                Ok(FirmwareUpdateOutcome::Skipped { reason: e.message })
            }
            Err(e) => Err(e),
        }
    }

    /// Legacy task-id based firmware update entrypoint.
    ///
    /// New RMS firmware code should call `update_firmware`; this method stays
    /// available while concrete node implementations migrate to NVFWUPD.
    async fn start_firmware_upload(
        &self,
        _target: &FirmwareTarget,
        _force_update: bool,
    ) -> Result<String> {
        Err(RmsError::unimplemented(
            "start_firmware_upload",
            self.node_type(),
        ))
    }

    async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
        Err(RmsError::unimplemented(
            "poll_firmware_task",
            self.node_type(),
        ))
    }

    /// Primary firmware activation entrypoint for RMS orchestration.
    ///
    /// The default implementation preserves the legacy no-argument activation
    /// method while the concrete node implementations migrate to NVFWUPD.
    async fn activate_firmware_with(
        &self,
        _request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        self.activate_firmware().await?;
        Ok(FirmwareActivationSummary {
            message: "Firmware activation completed".to_owned(),
            details: Value::Null,
        })
    }

    /// Legacy firmware activation entrypoint.
    ///
    /// New RMS firmware code should call `activate_firmware_with`; this method
    /// stays available while concrete node implementations migrate to NVFWUPD.
    async fn activate_firmware(&self) -> Result<()> {
        Err(RmsError::unimplemented(
            "activate_firmware",
            self.node_type(),
        ))
    }

    /// Returns `true` when this node supports [`Node::bmc_aux_powercycle`].
    ///
    /// Callers must check this before attempting recovery via BMC power-cycle;
    /// nodes that return `false` here will return `Unimplemented` from
    /// `bmc_aux_powercycle`, which would mask the original error.
    fn supports_bmc_aux_powercycle(&self) -> bool {
        false
    }

    /// Power-cycles the host through an auxilliary method (e.g. BMC using the Redfish API).
    ///
    /// Nodes that have a BMC endpoint configured should implement this method.
    async fn bmc_aux_powercycle(&self) -> Result<()> {
        Err(RmsError::unimplemented(
            "bmc_aux_powercycle",
            self.node_type().as_str(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;

    struct StubNode;
    struct LegacySkipNode;

    #[async_trait]
    impl Node for StubNode {
        fn id(&self) -> &str {
            "stub-01"
        }
        fn rack_id(&self) -> &str {
            "rack-01"
        }
        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb200Nvidia
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::from([("type".into(), "stub".into())])
        }
    }

    #[async_trait]
    impl Node for LegacySkipNode {
        fn id(&self) -> &str {
            "skip-01"
        }
        fn rack_id(&self) -> &str {
            "rack-01"
        }
        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb200Nvidia
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        async fn start_firmware_upload(
            &self,
            _target: &FirmwareTarget,
            _force_update: bool,
        ) -> Result<String> {
            Err(RmsError::already_exists("already up to date"))
        }
    }

    #[test]
    fn power_target_type_defaults_to_system() {
        assert_eq!(PowerTargetType::default(), PowerTargetType::System);
    }

    #[test]
    fn firmware_type_defaults_to_unknown() {
        assert_eq!(FirmwareType::default(), FirmwareType::Unknown);
    }

    #[test]
    fn firmware_task_status_defaults_to_incomplete() {
        let status = FirmwareTaskStatus::default();
        assert!(!status.completed);
        assert_eq!(status.percent, 0);
    }

    #[test]
    fn node_type_metadata_is_canonical() {
        let cases = [
            (
                NodeType::ComputeGb200Nvidia,
                "compute_gb200_nvidia",
                NodeKind::Compute,
                ProductFamily::Gb200,
                NvfwupdServerProfile::ComputeGb200,
                false,
            ),
            (
                NodeType::ComputeGb200Wiwynn,
                "compute_gb200_wiwynn",
                NodeKind::Compute,
                ProductFamily::Gb200,
                NvfwupdServerProfile::ComputeGb200,
                false,
            ),
            (
                NodeType::PowershelfGb200Liteon,
                "powershelf_gb200_liteon",
                NodeKind::Powershelf,
                ProductFamily::Gb200,
                NvfwupdServerProfile::Powershelf,
                false,
            ),
            (
                NodeType::SwitchGb200Nvidia,
                "switch_gb200_nvidia",
                NodeKind::Switch,
                ProductFamily::Gb200,
                NvfwupdServerProfile::SwitchGb200,
                true,
            ),
            (
                NodeType::PowershelfGb200Delta,
                "powershelf_gb200_delta",
                NodeKind::Powershelf,
                ProductFamily::Gb200,
                NvfwupdServerProfile::Powershelf,
                false,
            ),
            (
                NodeType::ComputeGb300Nvidia,
                "compute_gb300_nvidia",
                NodeKind::Compute,
                ProductFamily::Gb300,
                NvfwupdServerProfile::ComputeGb300,
                false,
            ),
            (
                NodeType::SwitchGb300Nvidia,
                "switch_gb300_nvidia",
                NodeKind::Switch,
                ProductFamily::Gb300,
                NvfwupdServerProfile::SwitchGb300,
                true,
            ),
            (
                NodeType::PowershelfGb300Liteon,
                "powershelf_gb300_liteon",
                NodeKind::Powershelf,
                ProductFamily::Gb300,
                NvfwupdServerProfile::Powershelf,
                false,
            ),
            (
                NodeType::PowershelfGb300Delta,
                "powershelf_gb300_delta",
                NodeKind::Powershelf,
                ProductFamily::Gb300,
                NvfwupdServerProfile::Powershelf,
                false,
            ),
            (
                NodeType::ComputeGb300Lenovo,
                "compute_gb300_lenovo",
                NodeKind::Compute,
                ProductFamily::Gb300,
                NvfwupdServerProfile::ComputeGb300,
                false,
            ),
            (
                NodeType::ComputeVrnvl72Nvidia,
                "compute_vrnvl72_nvidia",
                NodeKind::Compute,
                ProductFamily::Vrnvl72,
                NvfwupdServerProfile::ComputeVrnvl72,
                false,
            ),
            (
                NodeType::SwitchVrnvl72Nvidia,
                "switch_vrnvl72_nvidia",
                NodeKind::Switch,
                ProductFamily::Vrnvl72,
                NvfwupdServerProfile::SwitchVrnvl72,
                true,
            ),
        ];

        for (node_type, name, kind, product_family, nvfwupd_profile, uses_host) in cases {
            assert_eq!(node_type.as_str(), name);
            assert_eq!(node_type.to_string(), name);
            assert_eq!(node_type.kind(), kind);
            let kind_name = match kind {
                NodeKind::Compute => "compute",
                NodeKind::Powershelf => "powershelf",
                NodeKind::Switch => "switch",
            };
            assert_eq!(kind.to_string(), kind_name);
            assert_eq!(node_type.product_family(), product_family);
            assert_eq!(node_type.nvfwupd_server_profile(), nvfwupd_profile);
            assert_eq!(node_type.uses_host_management_endpoint(), uses_host);
        }
    }

    #[tokio::test]
    async fn default_power_state_returns_unimplemented() {
        let node = StubNode;
        let err = node.get_power_state().await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Unimplemented);
        let node_type = NodeType::ComputeGb200Nvidia.to_string();
        assert!(err.message.contains(&node_type));
    }

    #[tokio::test]
    async fn default_firmware_methods_return_unimplemented() {
        let node = StubNode;
        assert!(node.get_firmware_inventory().await.is_err());
        assert!(
            node.update_firmware(
                &FirmwareTarget {
                    component: "bmc".into(),
                    firmware_file: "fw.bin".into(),
                    expected_version: None,
                },
                false,
                FirmwareUpdateOptions::default(),
            )
            .await
            .is_err()
        );
        assert!(
            node.start_firmware_upload(
                &FirmwareTarget {
                    component: "bmc".into(),
                    firmware_file: "fw.bin".into(),
                    expected_version: None,
                },
                false,
            )
            .await
            .is_err()
        );
        assert!(node.poll_firmware_task("task-1").await.is_err());
        assert!(
            node.activate_firmware_with(FirmwareActivationRequest {
                mode: FirmwareActivationMode::FullGb200Compute,
                cancellation: None,
            })
            .await
            .is_err()
        );
        assert!(node.activate_firmware().await.is_err());
        assert!(node.bmc_aux_powercycle().await.is_err());
    }

    #[tokio::test]
    async fn default_update_maps_legacy_already_exists_to_skipped() {
        let outcome = LegacySkipNode
            .update_firmware(
                &FirmwareTarget {
                    component: "bmc".into(),
                    firmware_file: "fw.bin".into(),
                    expected_version: None,
                },
                false,
                FirmwareUpdateOptions::default(),
            )
            .await
            .expect("legacy already-exists should become skipped");

        assert_eq!(
            outcome,
            FirmwareUpdateOutcome::Skipped {
                reason: "already up to date".to_owned()
            }
        );
    }
}
