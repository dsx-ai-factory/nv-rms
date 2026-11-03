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
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::utilities::error::{ErrorCode, Result, RmsError};

// ── Device-info read timeouts ──
//
// Backstops for a single node's device-info read. Each device client
// (NVUE/Redfish/SSH) already applies its own per-request timeout; these bound
// the *aggregate* client I/O of one read so a client that fails to time out on
// its own cannot stall an inventory fan-out.
//
// They are meant to be applied *around only the client I/O*, i.e. inside the
// device-info read after the node's `op_lock` has been acquired (see
// `NvidiaGb200Compute::get_mnnvlink_topology` and the `SwitchDeviceInfo`
// `get_chassis_location_info` impl). Charging the `op_lock` wait against the
// deadline would let ordinary contention with an unrelated node operation
// (e.g. a firmware upload that holds the lock for minutes) turn a healthy node
// into a spurious timeout; keeping the lock wait outside preserves the base
// "queue behind the lock, then read" behavior.

/// Switch device-info goes through NVUE and is a small, bounded number of
/// requests, so a tight ceiling clears a healthy-but-slow switch while letting a
/// genuinely stuck one fail fast.
pub(crate) const SWITCH_DEVICE_INFO_FETCH_TIMEOUT: Duration = Duration::from_secs(90);

/// Compute device-info walks a Redfish GET sequence whose length is proportional
/// to the systems × processors the BMC advertises — RMS cannot bound that count.
/// This is therefore an **availability backstop, not a hard worst-case bound**:
/// a BMC advertising many processors, or a degraded one that parks nearly every
/// GET at its 30s per-request timeout, can still reach it (a single system with
/// ~6 processors already sums to ~300s, i.e. right at the boundary). It is sized
/// to clear a healthy single-system node with a handful of processors so the
/// common slow-but-responsive case surfaces its own specific Redfish error; a
/// pathologically large or badly degraded topology is the case it deliberately
/// preempts with the generic deadline error.
pub(crate) const COMPUTE_DEVICE_INFO_FETCH_TIMEOUT: Duration = Duration::from_secs(300);

/// Per-node-kind device-info I/O backstop, or `None` for a kind that performs
/// no device-info I/O and therefore needs no bound.
///
/// Powershelves read from a BMC/PMC for power and firmware, but expose **no
/// device-info payload**: `NodeInstance::get_device_info` returns `Ok(None)`
/// synchronously for every powershelf variant (see `nodes/instance.rs`), so
/// there is nothing to time. Returning `None` lets callers skip the timeout
/// wrapper entirely rather than arming a ceiling that could never fire. Compute
/// (Redfish GET walk) and switch (NVUE) reads do real client I/O and get the
/// per-kind ceiling.
pub(crate) fn device_info_fetch_timeout(node_type: NodeType) -> Option<Duration> {
    match node_type.kind() {
        NodeKind::Compute => Some(COMPUTE_DEVICE_INFO_FETCH_TIMEOUT),
        NodeKind::Switch => Some(SWITCH_DEVICE_INFO_FETCH_TIMEOUT),
        NodeKind::Powershelf => None,
    }
}

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
#[allow(clippy::upper_case_acronyms)] // Hardware acronyms (BMC/HMC) read better in ALLCAPS.
pub enum PowerTargetType {
    #[default]
    System,
    BMC,
    HMC,
}

// ── Firmware ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(clippy::upper_case_acronyms)] // Hardware acronyms (BMC/BIOS/…) read better in ALLCAPS.
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
    pub state: String,
    pub sku: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FirmwareTarget {
    /// Logical component identity; profile resolution must not replace it with a path.
    pub component: String,
    pub firmware_file: String,
    pub expected_version: Option<String>,
    /// Present once a profile target is selected. Legacy/code-default requests
    /// retain their existing component interpretation when this is absent.
    pub resolution: Option<super::firmware_target::FirmwareTargetResolution>,
    /// Private manifest rollout policy; never inferred from a filename.
    pub linkx_policy: Option<nvfwupd::flint::LinkxPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FirmwareTaskStatus {
    pub completed: bool,
    pub percent: i32,
    pub state: String,
    pub status: String,
    pub message: String,
    pub rejection: Option<FirmwareTaskRejection>,
}

/// A terminal firmware task rejection established from the structured response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirmwareTaskRejection {
    /// Every reported failure is an identical image; no component reports a completed update or pending activation.
    IdenticalImage,
}

/// Canonical RMS node type.
///
/// This is the source of truth for stable node type names and the derived
/// routing metadata used by racks, endpoint selection, and NVFWUPD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeType {
    ComputeGb200Nvidia,
    ComputeGb200Wiwynn,
    ComputeGb200Dell,
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
    PowershelfVrnvl72Liteon,
    PowershelfVrnvl72Delta,
}

/// Broad device kind used for policies that only care about node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
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

/// Profile-derived parameters for a concrete Redfish/NVFWUPD compute node.
///
/// These values are compiled from a hardware profile at startup and passed to
/// the implementation selected by [`NodeType`]. They configure existing
/// behavior; they do not select or replace the concrete node implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedfishNvfwupdComputeConfig {
    /// NVFWUPD protocol family used to interpret packages and workflows.
    pub nvfwupd_server_profile: NvfwupdServerProfile,
    /// Redfish `ComputerSystem` resource ID used for host power operations.
    pub computer_system_id: Arc<str>,
    /// Redfish `Manager` resource ID used for host-BMC power operations.
    pub bmc_manager_id: Arc<str>,
    /// Redfish `Manager` resource ID used for HMC power operations.
    pub hmc_manager_id: Arc<str>,
    /// Aggregate timeout for one MNNVLink topology read.
    pub device_info_timeout: Duration,
    /// Connection establishment limit for the shared Redfish client.
    pub redfish_connect_timeout: Duration,
    /// Per-request limit for shared Redfish reads and power operations.
    pub redfish_request_timeout: Duration,
    /// Per-upload limit for the direct Redfish upload operation.
    pub redfish_upload_timeout: Duration,
    /// Optional paths for direct power operations and firmware activation.
    pub power_paths: super::compute_power::ComputePowerPaths,
    /// Low-level compute firmware workflow timings.
    pub firmware_timing: super::compute_timing::ComputeFirmwareTiming,
}

/// Validated runtime parameters supplied to a concrete node implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRuntimeConfig {
    /// Parameters understood by concrete Redfish/NVFWUPD compute implementations.
    RedfishNvfwupdCompute(RedfishNvfwupdComputeConfig),
}

impl NodeType {
    /// Resolve a canonical node type from its stable configuration name.
    ///
    /// Matching is exact and case-sensitive so configuration does not create
    /// aliases for hardware identity.
    pub fn from_stable_name(value: &str) -> Option<Self> {
        match value {
            "compute_gb200_nvidia" => Some(Self::ComputeGb200Nvidia),
            "compute_gb200_wiwynn" => Some(Self::ComputeGb200Wiwynn),
            "compute_gb200_dell" => Some(Self::ComputeGb200Dell),
            "powershelf_gb200_liteon" => Some(Self::PowershelfGb200Liteon),
            "switch_gb200_nvidia" => Some(Self::SwitchGb200Nvidia),
            "powershelf_gb200_delta" => Some(Self::PowershelfGb200Delta),
            "compute_gb300_nvidia" => Some(Self::ComputeGb300Nvidia),
            "switch_gb300_nvidia" => Some(Self::SwitchGb300Nvidia),
            "powershelf_gb300_liteon" => Some(Self::PowershelfGb300Liteon),
            "powershelf_gb300_delta" => Some(Self::PowershelfGb300Delta),
            "compute_gb300_lenovo" => Some(Self::ComputeGb300Lenovo),
            "compute_gb300_supermicro" => Some(Self::ComputeGb300Supermicro),
            "compute_vrnvl72_nvidia" => Some(Self::ComputeVrnvl72Nvidia),
            "switch_vrnvl72_nvidia" => Some(Self::SwitchVrnvl72Nvidia),
            "powershelf_vrnvl72_liteon" => Some(Self::PowershelfVrnvl72Liteon),
            "powershelf_vrnvl72_delta" => Some(Self::PowershelfVrnvl72Delta),
            _ => None,
        }
    }

    /// Stable protobuf/config string for this node type.
    ///
    /// Prefer `Display` in formatting contexts. This method remains
    /// available because callers need a `const` string in tables and static
    /// metadata.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ComputeGb200Nvidia => "compute_gb200_nvidia",
            Self::ComputeGb200Wiwynn => "compute_gb200_wiwynn",
            Self::ComputeGb200Dell => "compute_gb200_dell",
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
            Self::PowershelfVrnvl72Liteon => "powershelf_vrnvl72_liteon",
            Self::PowershelfVrnvl72Delta => "powershelf_vrnvl72_delta",
        }
    }

    /// Broad device kind for endpoint and credential policy.
    pub const fn kind(self) -> NodeKind {
        match self {
            Self::ComputeGb200Nvidia
            | Self::ComputeGb200Wiwynn
            | Self::ComputeGb200Dell
            | Self::ComputeGb300Nvidia
            | Self::ComputeGb300Lenovo
            | Self::ComputeGb300Supermicro
            | Self::ComputeVrnvl72Nvidia => NodeKind::Compute,
            Self::PowershelfGb200Liteon
            | Self::PowershelfGb200Delta
            | Self::PowershelfGb300Liteon
            | Self::PowershelfGb300Delta
            | Self::PowershelfVrnvl72Liteon
            | Self::PowershelfVrnvl72Delta => NodeKind::Powershelf,
            Self::SwitchGb200Nvidia | Self::SwitchGb300Nvidia | Self::SwitchVrnvl72Nvidia => {
                NodeKind::Switch
            }
        }
    }

    /// Whether this node type supports host-side Flint adapter updates.
    pub(crate) const fn supports_flint_inband_firmware(self) -> bool {
        matches!(self, Self::ComputeGb200Nvidia | Self::ComputeGb200Wiwynn)
    }

    /// Node types with a compute OOB recovery flow: the only ones a recovery
    /// profile may bind to. The GB200 constructor is shared by wrappers that
    /// have no flow of their own, so the check happens by type, not by
    /// constructor.
    pub const fn has_recovery_flow(self) -> bool {
        matches!(self, Self::ComputeGb200Nvidia)
    }

    /// Product family that owns this node type.
    pub const fn product_family(self) -> ProductFamily {
        match self {
            Self::ComputeGb200Nvidia
            | Self::ComputeGb200Wiwynn
            | Self::ComputeGb200Dell
            | Self::PowershelfGb200Liteon
            | Self::SwitchGb200Nvidia
            | Self::PowershelfGb200Delta => ProductFamily::Gb200,
            Self::ComputeGb300Nvidia
            | Self::SwitchGb300Nvidia
            | Self::PowershelfGb300Liteon
            | Self::PowershelfGb300Delta
            | Self::ComputeGb300Lenovo
            | Self::ComputeGb300Supermicro => ProductFamily::Gb300,
            Self::ComputeVrnvl72Nvidia
            | Self::SwitchVrnvl72Nvidia
            | Self::PowershelfVrnvl72Liteon
            | Self::PowershelfVrnvl72Delta => ProductFamily::Vrnvl72,
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
            Self::ComputeGb200Nvidia | Self::ComputeGb200Wiwynn | Self::ComputeGb200Dell => {
                NvfwupdServerProfile::ComputeGb200
            }
            Self::ComputeGb300Nvidia | Self::ComputeGb300Lenovo | Self::ComputeGb300Supermicro => {
                NvfwupdServerProfile::ComputeGb300
            }
            Self::ComputeVrnvl72Nvidia => NvfwupdServerProfile::ComputeVrnvl72,
            Self::PowershelfGb200Liteon
            | Self::PowershelfGb200Delta
            | Self::PowershelfGb300Liteon
            | Self::PowershelfGb300Delta
            | Self::PowershelfVrnvl72Liteon
            | Self::PowershelfVrnvl72Delta => NvfwupdServerProfile::Powershelf,
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

/// Expected firmware inventory selected by opaque deployment metadata.
///
/// The profile name is returned through inventory APIs. AP names are passed to
/// NVFWUPD, while physical adapter counts are passed to Flint workflows. All
/// values are immutable and shared by nodes selecting the configured profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedInventoryPolicy {
    pub profile: Arc<str>,
    pub ap_names: Arc<[String]>,
    pub flint_device_counts: Arc<nvfwupd::workflow::FlintExpectedDeviceCounts>,
}

/// One recovery profile as written in the config, opaque to generic RMS.
/// A node type turns it into its own roster with [`RecoveryProfile::parse`]
/// when it binds to the profile at add_nodes; nothing else reads inside.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct RecoveryProfile(serde_json::Map<String, serde_json::Value>);

impl RecoveryProfile {
    /// Parse the table into a node type's roster. `T` decides which fields
    /// exist; with `deny_unknown_fields` a field meant for another platform
    /// fails here, before the profile can reach a job.
    pub fn parse<T: serde::de::DeserializeOwned>(&self) -> serde_json::Result<T> {
        serde_json::from_value(serde_json::Value::Object(self.0.clone()))
    }
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

/// Optional cleanup for nodes that temporarily change device-wide settings.
///
/// The caller invokes this after monitoring a started task, including on poll
/// errors and cancellation. Implementations must check whether it is safe to
/// restore settings: stopping RMS polling does not stop the device's task.
#[async_trait]
pub trait FirmwareTaskCleanup: Send + Sync {
    async fn finish(&self, task_id: &str) -> Result<()>;
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
    PowerOn,
    PowerOff,
    PowerCycle,
    AuxPowerCycle,
    PowerStatus,
    PowerShelfReset,
    PowerShelfResetForce,
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

/// Bundle files for the recovery pushes. The resolver supplies both or
/// errors. Which SOT artifact serves each node is still open (the
/// bundle-selection question).
#[derive(Debug, Clone)]
pub struct RecoveryBundles {
    /// The regular HMC PLDM bundle. One push+cycle round recovers one
    /// reachable FPGA.
    pub regular: String,
    /// The HMC recovery bundle. One push recovers every CPU ERoT and GPU.
    pub recovery: String,
}

// ── Node Trait ──

/// The firmware recovery capability of a node type. Three tiers share one
/// trait boundary: the job layer reads the inventory, asks `detect`, and hands
/// a non-empty result to `recover` with the bundles it resolved, and it never
/// learns the signature, the watched names, or the steps. The node type owns
/// both methods. Behind them sits a procedure module that sees the tray only
/// through the primitives it declares. A node reaches this capability through
/// [`Node::recovery`]: `None` there means no recovery flow, or no profile bound,
/// and detection stays off.
#[async_trait]
pub trait Recoverable: Send + Sync {
    /// The watched components this inventory shows in recovery state. Empty
    /// means the flow does not apply to this reading.
    fn detect(&self, inventory: &[FirmwareInfo]) -> Vec<String>;

    /// Run the recovery procedure for `in_recovery`, the names `detect`
    /// returned, with the bundle files the job resolved. `Ok` means every
    /// touched component reads healthy again and the update may proceed.
    /// `cancel` is read between stages and while a push task is awaited,
    /// never inside a push or a power cycle: those leave the tray
    /// half-activated when interrupted. A fired token ends the flow with a
    /// `Cancelled` error at the next such point.
    async fn recover(
        &self,
        in_recovery: &[String],
        bundles: RecoveryBundles,
        cancel: &CancellationToken,
    ) -> Result<()>;
}

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
/// A method added here with a default body is silently absent on the five
/// node types that wrap `NvidiaGb200Compute`, since they forward methods
/// one by one. Every new default needs a decision per wrapper.
#[async_trait]
pub trait Node: Send + Sync {
    // ── Identity (sync, returns stored data) ──

    fn id(&self) -> &str;
    fn rack_id(&self) -> &str;
    fn node_type(&self) -> NodeType;
    fn get_info(&self) -> HashMap<String, String>;
    /// The recovery capability, when this node type has a flow and a
    /// profile is bound. `None` keeps detection off (see [`Recoverable`]).
    fn recovery(&self) -> Option<&dyn Recoverable> {
        None
    }

    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        None
    }

    /// Only adapters that change persistent settings during upload opt in.
    /// The shared NVIDIA driver and its other wrappers need no such cleanup.
    fn firmware_task_cleanup(&self) -> Option<&dyn FirmwareTaskCleanup> {
        None
    }

    // ── Power (async, performs I/O) ──

    async fn get_power_state(&self) -> Result<PowerState> {
        Err(RmsError::unimplemented("get_power_state", self.node_type()))
    }

    async fn set_power_state(&self, _op: PowerOp, _target: PowerTargetType) -> Result<()> {
        Err(RmsError::unimplemented("set_power_state", self.node_type()))
    }

    // ── Device info (async, performs I/O) ──

    /// Fetch a device-info payload (e.g. tray/chassis location) when the node
    /// type exposes one.
    ///
    /// Production dispatch lives entirely on the inherent
    /// [`crate::nodes::NodeInstance::get_device_info`], which the handlers call
    /// directly; nothing in the crate reaches device info through this trait
    /// method. It exists only so `dyn Node` test mocks can supply device-info
    /// behavior, and is therefore `#[cfg(test)]`-gated: keeping the
    /// always-`Ok(None)` default off the production trait surface prevents a
    /// future trait-object/generic caller from silently getting an empty result
    /// that disagrees with the inherent method (it becomes a compile error
    /// instead).
    #[cfg(test)]
    async fn get_device_info(&self) -> Result<Option<Value>> {
        Ok(None)
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

    /// Update one logical firmware group.
    ///
    /// Most node types update one package at a time. Nodes with host-side
    /// PSID-specific images can override this method to validate and apply all
    /// candidate images as one atomic planning unit.
    async fn update_firmware_group(
        &self,
        targets: &[FirmwareTarget],
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let [target] = targets else {
            return Err(RmsError::invalid_argument(
                "firmware update group must contain exactly one target for this node type",
            ));
        };
        self.update_firmware(target, force_update, options).await
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

    /// Sends one request to the node's operating-system management API and
    /// returns `Ok(())` when it answers, the transport error otherwise.
    ///
    /// A switch power-cycled through its BMC can pass the version check while
    /// its OS still boots, so the readiness poll waits on this first. Nodes
    /// without such an API have nothing to wait for and return `Ok(())`.
    async fn verify_management_api_reachable(&self) -> Result<()> {
        Ok(())
    }

    /// Confirms the node's operating system reports itself booted and healthy
    /// after an activation.
    ///
    /// Nodes without such a signal have nothing to verify and return `Ok(())`.
    /// Nodes that have one fail with `FailedPrecondition` while the signal
    /// says not ready, and with `Aborted` when it reports a state that waiting
    /// will not change, such as a failed boot. A caller that polls stops on
    /// `Aborted` and on `InvalidArgument` (no usable management endpoint) and
    /// retries the other errors, because a booting node can answer with any
    /// of them before it answers ready.
    async fn verify_system_ready(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;

    struct StubNode;
    struct LegacySkipNode;

    #[test]
    fn node_kind_uses_snake_case_serialization() {
        for (kind, name) in [
            (NodeKind::Compute, "compute"),
            (NodeKind::Powershelf, "powershelf"),
            (NodeKind::Switch, "switch"),
        ] {
            let value = serde_json::Value::String(name.to_owned());

            assert_eq!(serde_json::to_value(kind).unwrap(), value);
            assert_eq!(serde_json::from_value::<NodeKind>(value).unwrap(), kind);
        }
    }

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
                NodeType::ComputeGb200Dell,
                "compute_gb200_dell",
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
                NodeType::ComputeGb300Supermicro,
                "compute_gb300_supermicro",
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
            (
                NodeType::PowershelfVrnvl72Liteon,
                "powershelf_vrnvl72_liteon",
                NodeKind::Powershelf,
                ProductFamily::Vrnvl72,
                NvfwupdServerProfile::Powershelf,
                false,
            ),
            (
                NodeType::PowershelfVrnvl72Delta,
                "powershelf_vrnvl72_delta",
                NodeKind::Powershelf,
                ProductFamily::Vrnvl72,
                NvfwupdServerProfile::Powershelf,
                false,
            ),
        ];

        for (node_type, name, kind, product_family, nvfwupd_profile, uses_host) in cases {
            assert_eq!(NodeType::from_stable_name(name), Some(node_type));
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
        assert_eq!(NodeType::from_stable_name("unknown_node"), None);
    }

    #[test]
    fn flint_inband_firmware_capability_is_limited_to_supported_gb200_compute_types() {
        assert!(NodeType::ComputeGb200Nvidia.supports_flint_inband_firmware());
        assert!(NodeType::ComputeGb200Wiwynn.supports_flint_inband_firmware());
        assert!(!NodeType::SwitchGb200Nvidia.supports_flint_inband_firmware());
        assert!(!NodeType::ComputeGb300Nvidia.supports_flint_inband_firmware());
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
                    resolution: None,
                    linkx_policy: None,
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
                    resolution: None,
                    linkx_policy: None,
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
                    resolution: None,
                    linkx_policy: None,
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
