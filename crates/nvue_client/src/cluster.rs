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

//! Types for the NVUE `/cluster` API area.

use std::collections::{BTreeMap, HashSet};
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::NVUE_V1_SERVER;
use crate::action::SimpleAction;
use crate::uri::{path_segment, query_value};

/// NMX factory reset in-progress reason.
pub const NMX_FACTORY_RESET_IN_PROGRESS_REASON: &str = "FACTORY RESET IN PROGRESS";

/// NMX controller app name in cluster app responses.
pub const NMX_CONTROLLER_APP_NAME: &str = "nmx-controller";

/// Full endpoint used by the existing RMS HTTP client for `GET /cluster`.
pub const CLUSTER_ENDPOINT: &str = "/nvue_v1/cluster";

/// Deduplicated cluster-node server addresses.
pub type ClusterNodeServerAddresses = HashSet<IpAddr>;

pub(crate) type ClusterNodeServerMap = BTreeMap<IpAddr, ClusterNodeServer>;

/// Cluster enablement state values exposed by NVUE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    /// Cluster is enabled.
    Enabled,

    /// Cluster is disabled.
    Disabled,
}

/// Cluster management interface type whose server addresses NVUE configures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InterfaceType {
    /// Primary cluster interface, currently mapped to switch `eth0`.
    Primary,

    /// Secondary cluster interface, reserved for an explicit `eth1` address source.
    Secondary,
}

impl InterfaceType {
    /// Returns the NVUE path spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
        }
    }
}

impl ClusterState {
    /// Returns the NVUE string spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

impl TryFrom<&str> for ClusterState {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            _ => Err("unknown cluster state"),
        }
    }
}

/// `nmxc-conn` state values exposed by NVUE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NmxcConnectionState {
    /// NMX-C connection is up.
    Up,

    /// NMX-C connection is down.
    Down,

    /// NMX-C connection is not applicable.
    NotApplicable,
}

impl NmxcConnectionState {
    /// Returns the NVUE string spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
            Self::NotApplicable => "n/a",
        }
    }
}

impl TryFrom<&str> for NmxcConnectionState {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "up" => Ok(Self::Up),
            "down" => Ok(Self::Down),
            "n/a" => Ok(Self::NotApplicable),
            _ => Err("unknown NMX-C connection state"),
        }
    }
}

/// Cluster app status values used by RMS manager-action logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterAppStatus {
    /// App reports healthy.
    Ok,

    /// App reports unhealthy.
    NotOk,

    /// App reports stopped.
    Stopped,
}

impl ClusterAppStatus {
    /// Returns the NVUE string spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotOk => "not ok",
            Self::Stopped => "stopped",
        }
    }
}

impl TryFrom<&str> for ClusterAppStatus {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "ok" => Ok(Self::Ok),
            "not ok" => Ok(Self::NotOk),
            "stopped" => Ok(Self::Stopped),
            _ => Err("unknown cluster app status"),
        }
    }
}

/// Cluster app manager state values used by RMS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterAppManagerState {
    /// App manager is enabled.
    Enabled,

    /// App manager is disabled.
    Disabled,

    /// App manager start action is in progress.
    Start,

    /// App manager is active.
    Active,
}

impl ClusterAppManagerState {
    /// Returns the NVUE string spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Start => "start",
            Self::Active => "active",
        }
    }
}

impl TryFrom<&str> for ClusterAppManagerState {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            "start" => Ok(Self::Start),
            "active" => Ok(Self::Active),
            _ => Err("unknown cluster app manager state"),
        }
    }
}

/// NMX control plane states used by RMS readiness checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NmxControlPlaneState {
    /// NMX control plane is configured.
    Configured,

    /// NMX control plane is unconfigured.
    Unconfigured,

    /// NMX control plane is stopped.
    Stopped,

    /// Legacy stopped spelling observed in app responses.
    LegacyStopped,
}

impl NmxControlPlaneState {
    /// Returns the NVUE string spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configured => "CONTROL_PLANE_STATE_CONFIGURED",
            Self::Unconfigured => "CONTROL_PLANE_STATE_UNCONFIGURED",
            Self::Stopped => "CONTROL_PLANE_STATE_STOPPED",
            Self::LegacyStopped => "STOPPED",
        }
    }

    /// Returns true for states where cluster app manager actions are accepted.
    pub const fn is_ready_for_manager_action(self) -> bool {
        matches!(self, Self::Configured | Self::Unconfigured)
    }

    /// Returns true for stopped spellings observed in app runtime state.
    pub const fn is_stopped(self) -> bool {
        matches!(self, Self::Stopped | Self::LegacyStopped)
    }
}

impl TryFrom<&str> for NmxControlPlaneState {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "CONTROL_PLANE_STATE_CONFIGURED" => Ok(Self::Configured),
            "CONTROL_PLANE_STATE_UNCONFIGURED" => Ok(Self::Unconfigured),
            "CONTROL_PLANE_STATE_STOPPED" => Ok(Self::Stopped),
            "STOPPED" => Ok(Self::LegacyStopped),
            _ => Err("unknown NMX control plane state"),
        }
    }
}

/// Builds `/cluster?rev={revision-id}` for configuration updates.
pub fn cluster_endpoint_for_revision(revision_id: &str) -> String {
    format!("{CLUSTER_ENDPOINT}?rev={}", query_value(revision_id))
}

/// Builds the endpoint for one cluster interface's server-address collection.
pub fn cluster_node_server_endpoint(interface_type: InterfaceType) -> String {
    format!(
        "{NVUE_V1_SERVER}/cluster/node/{}/server",
        interface_type.as_str()
    )
}

/// Builds the endpoint for one cluster interface.
pub fn cluster_node_endpoint(interface_type: InterfaceType) -> String {
    format!("{NVUE_V1_SERVER}/cluster/node/{}", interface_type.as_str())
}

/// Builds the revision-scoped endpoint for a cluster-interface update.
pub fn cluster_node_endpoint_for_revision(
    interface_type: InterfaceType,
    revision_id: &str,
) -> String {
    format!(
        "{}?rev={}",
        cluster_node_endpoint(interface_type),
        query_value(revision_id)
    )
}

/// Builds the revision-scoped endpoint for a server-address update.
pub fn cluster_node_server_endpoint_for_revision(
    interface_type: InterfaceType,
    revision_id: &str,
) -> String {
    format!(
        "{}?rev={}",
        cluster_node_server_endpoint(interface_type),
        query_value(revision_id)
    )
}

/// Builds the existing RMS HTTP client endpoint for `GET /cluster/apps/{app-name}`.
pub fn app_endpoint(app_name: &str) -> String {
    format!("{NVUE_V1_SERVER}/cluster/apps/{}", path_segment(app_name))
}

/// Builds the existing RMS HTTP client endpoint for `/cluster/apps/{app-name}/manager`.
pub fn app_manager_endpoint(app_name: &str) -> String {
    format!(
        "{NVUE_V1_SERVER}/cluster/apps/{}/manager",
        path_segment(app_name)
    )
}

/// Request body for `PATCH /cluster`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterUpdate {
    /// Cluster state.
    pub state: String,
}

/// Empty per-address value required by the NVUE cluster-node server wire schema.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ClusterNodeServer {}

impl ClusterUpdate {
    /// Create a cluster state update payload.
    pub fn new(state: impl Into<String>) -> Self {
        Self {
            state: state.into(),
        }
    }
}

/// Request body for `POST /cluster/apps/{app-name}` `@start`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterAppStartRequest {
    /// Start action.
    #[serde(rename = "@start")]
    pub start: SimpleAction<NoActionParameters>,
}

impl ClusterAppStartRequest {
    /// Create a start request.
    pub fn new() -> Self {
        Self {
            start: SimpleAction::start_without_parameters(),
        }
    }
}

impl Default for ClusterAppStartRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Request body for `POST /cluster/apps/{app-name}` `@stop`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterAppStopRequest {
    /// Stop action.
    #[serde(rename = "@stop")]
    pub stop: SimpleAction<NoActionParameters>,
}

impl ClusterAppStopRequest {
    /// Create a stop request.
    pub fn new() -> Self {
        Self {
            stop: SimpleAction::start_without_parameters(),
        }
    }
}

impl Default for ClusterAppStopRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Empty action parameter marker.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NoActionParameters {}

/// Request body for `POST /cluster/apps/{app-name}/manager` `@update`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterAppManagerUpdateRequest {
    /// Update action.
    #[serde(rename = "@update")]
    pub update: ClusterAppManagerUpdateAction,
}

impl ClusterAppManagerUpdateRequest {
    /// Create a manager-state update request.
    pub fn new(state: impl Into<String>) -> Self {
        Self {
            update: ClusterAppManagerUpdateAction::new(state),
        }
    }
}

/// Action body for cluster app manager `@update`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterAppManagerUpdateAction {
    /// Update parameters.
    pub parameters: ClusterAppManagerUpdateParameters,
}

impl ClusterAppManagerUpdateAction {
    /// Create an update action body.
    pub fn new(state: impl Into<String>) -> Self {
        Self {
            parameters: ClusterAppManagerUpdateParameters {
                state: state.into(),
            },
        }
    }
}

/// Parameters for cluster app manager `@update`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterAppManagerUpdateParameters {
    /// Desired manager state.
    pub state: String,
}

/// Minimal view of `schema-cluster-cluster` needed before manager actions.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Cluster {
    /// Cluster enablement state.
    #[serde(default)]
    pub state: Option<String>,

    /// gRPC connection state to NMX-C APP-GW.
    #[serde(default, rename = "nmxc-conn")]
    pub nmxc_conn: Option<NmxcConnection>,
}

impl Cluster {
    /// Returns true when NVUE reports the cluster disabled.
    pub fn is_disabled(&self) -> bool {
        self.state
            .as_deref()
            .is_some_and(|state| ClusterState::try_from(state) == Ok(ClusterState::Disabled))
    }

    /// Returns true when the cluster can accept cluster app manager actions.
    pub fn is_ready_for_app_manager_action(&self) -> bool {
        self.state
            .as_deref()
            .is_some_and(|state| ClusterState::try_from(state) == Ok(ClusterState::Enabled))
            && self.nmxc_conn.as_ref().is_some_and(NmxcConnection::is_up)
    }
}

/// NVUE `nmxc-conn` is documented as a string, but some responses nest `state`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum NmxcConnection {
    /// OpenAPI shape: `"up"`, `"down"`, or `"n/a"`.
    Text(String),

    /// Compatibility shape observed from switch responses.
    Object {
        /// Nested connection state.
        state: Option<String>,
    },
}

impl NmxcConnection {
    /// Returns the connection state as a borrowed string.
    pub fn state(&self) -> Option<&str> {
        match self {
            Self::Text(state) => Some(state),
            Self::Object { state } => state.as_deref(),
        }
    }

    /// Returns true when the NMX-C connection is up.
    pub fn is_up(&self) -> bool {
        self.state().is_some_and(|state| {
            NmxcConnectionState::try_from(state) == Ok(NmxcConnectionState::Up)
        })
    }
}

/// Minimal view of `schema-cluster-cluster-app` needed around manager actions.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ClusterApp {
    /// App health/status string.
    #[serde(default)]
    pub status: Option<String>,

    /// App status reason.
    #[serde(default)]
    pub reason: Option<String>,

    /// Additional app state. The OpenAPI field is typoed as `addition-info`.
    #[serde(default, rename = "addition-info", alias = "additional-info")]
    pub addition_info: Option<String>,

    /// Cluster app manager state.
    #[serde(default)]
    pub manager: Option<ClusterAppManager>,
}

impl ClusterApp {
    /// Returns the typed NMX control-plane state when NVUE reports a known value.
    pub fn control_plane_state(&self) -> Option<NmxControlPlaneState> {
        self.addition_info
            .as_deref()
            .and_then(|state| NmxControlPlaneState::try_from(state.trim()).ok())
    }

    /// Returns true only when the NMX control plane reports configured.
    pub fn is_control_plane_configured(&self) -> bool {
        self.control_plane_state() == Some(NmxControlPlaneState::Configured)
    }

    /// Returns true when NVUE reports the app stopped.
    pub fn is_stopped(&self) -> bool {
        // An explicitly stopped app can retain `status: not ok` and
        // `addition-info: CONTROL_PLANE_STATE_UNDEFINED`.
        self.status
            .as_deref()
            .is_some_and(|status| status.eq_ignore_ascii_case(ClusterAppStatus::Stopped.as_str()))
            || self.reason.as_deref() == Some("stopped by user")
            || self
                .control_plane_state()
                .is_some_and(NmxControlPlaneState::is_stopped)
    }

    /// Returns true when this app can accept a manager action.
    pub fn is_ready_for_manager_action(&self) -> bool {
        self.manager_state().is_some()
            && (self.status.as_deref().is_some_and(|status| {
                ClusterAppStatus::try_from(status) == Ok(ClusterAppStatus::Ok)
            }) || self
                .control_plane_state()
                .is_some_and(NmxControlPlaneState::is_ready_for_manager_action))
    }

    /// Returns the nested app manager state.
    pub fn manager_state(&self) -> Option<&str> {
        self.manager.as_ref().and_then(|manager| manager.state())
    }

    /// Returns true once the app manager reached the requested state.
    pub fn manager_reached_state(&self, desired: &str) -> bool {
        self.manager_state().is_some_and(|state| {
            state == desired
                || (desired == ClusterAppManagerState::Enabled.as_str()
                    && ClusterAppManagerState::try_from(state)
                        == Ok(ClusterAppManagerState::Active))
        })
    }
}

/// Minimal view of `schema-cluster-cluster-app-mgr`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ClusterAppManager {
    /// App manager port state.
    #[serde(default)]
    pub state: Option<String>,
}

impl ClusterAppManager {
    /// Returns the app manager state.
    pub fn state(&self) -> Option<&str> {
        self.state.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_cluster(input: &str) -> Cluster {
        serde_json::from_str(input).expect("cluster fixture should deserialize")
    }

    fn decode_cluster_app(input: &str) -> ClusterApp {
        serde_json::from_str(input).expect("cluster app fixture should deserialize")
    }

    #[test]
    fn endpoint_helpers_include_nvue_v1_server() {
        assert_eq!(CLUSTER_ENDPOINT, "/nvue_v1/cluster");
        assert_eq!(
            cluster_endpoint_for_revision("pending"),
            "/nvue_v1/cluster?rev=pending"
        );
        assert_eq!(
            app_endpoint("nmx-controller"),
            "/nvue_v1/cluster/apps/nmx-controller"
        );
        assert_eq!(
            app_manager_endpoint("nmx-controller"),
            "/nvue_v1/cluster/apps/nmx-controller/manager"
        );
        assert_eq!(
            cluster_endpoint_for_revision("pending&base=applied"),
            "/nvue_v1/cluster?rev=pending%26base%3Dapplied"
        );

        assert_eq!(
            cluster_node_server_endpoint(InterfaceType::Primary),
            "/nvue_v1/cluster/node/primary/server"
        );

        assert_eq!(
            cluster_node_endpoint_for_revision(InterfaceType::Primary, "pending&base=applied"),
            "/nvue_v1/cluster/node/primary?rev=pending%26base%3Dapplied"
        );

        assert_eq!(
            cluster_node_server_endpoint_for_revision(
                InterfaceType::Secondary,
                "pending&base=applied",
            ),
            "/nvue_v1/cluster/node/secondary/server?rev=pending%26base%3Dapplied"
        );

        assert_eq!(
            app_endpoint("nmx/controller"),
            "/nvue_v1/cluster/apps/nmx%2Fcontroller"
        );
    }

    #[test]
    fn action_payloads_use_nvue_action_keys() {
        let cluster =
            serde_json::to_string(&ClusterUpdate::new(ClusterState::Enabled.as_str())).unwrap();

        let start = serde_json::to_string(&ClusterAppStartRequest::new()).unwrap();
        let stop = serde_json::to_string(&ClusterAppStopRequest::new()).unwrap();

        let manager = serde_json::to_string(&ClusterAppManagerUpdateRequest::new(
            ClusterAppManagerState::Enabled.as_str(),
        ))
        .unwrap();

        assert_eq!(cluster, r#"{"state":"enabled"}"#);
        assert!(start.contains(r#""@start""#));
        assert!(stop.contains(r#""@stop""#));
        assert_eq!(manager, r#"{"@update":{"parameters":{"state":"enabled"}}}"#);
    }

    #[test]
    fn cluster_state_enums_parse_nvue_strings() {
        assert_eq!(ClusterState::try_from("enabled"), Ok(ClusterState::Enabled));

        assert_eq!(
            NmxcConnectionState::try_from("up"),
            Ok(NmxcConnectionState::Up)
        );

        assert_eq!(ClusterAppStatus::try_from("ok"), Ok(ClusterAppStatus::Ok));

        assert_eq!(
            ClusterAppManagerState::try_from("active"),
            Ok(ClusterAppManagerState::Active)
        );

        assert_eq!(
            NmxControlPlaneState::try_from("CONTROL_PLANE_STATE_UNCONFIGURED"),
            Ok(NmxControlPlaneState::Unconfigured)
        );

        assert!(ClusterState::try_from("unknown").is_err());
    }

    #[test]
    fn cluster_ready_for_app_manager_action_reads_nvue_shapes() {
        struct TestCase {
            name: &'static str,
            cluster: Cluster,
            expected: bool,
        }

        let cases = [
            TestCase {
                name: "enabled with openapi nmxc state",
                cluster: decode_cluster(r#"{"state":"enabled","nmxc-conn":"up"}"#),
                expected: true,
            },
            TestCase {
                name: "enabled with nested nmxc state",
                cluster: decode_cluster(r#"{"state":"enabled","nmxc-conn":{"state":"up"}}"#),
                expected: true,
            },
            TestCase {
                name: "cluster start is not ready",
                cluster: decode_cluster(r#"{"state":"start","nmxc-conn":{"state":"up"}}"#),
                expected: false,
            },
            TestCase {
                name: "nmxc connection down is not ready",
                cluster: decode_cluster(r#"{"state":"enabled","nmxc-conn":"down"}"#),
                expected: false,
            },
            TestCase {
                name: "nmxc connection n/a is not ready",
                cluster: decode_cluster(r#"{"state":"enabled","nmxc-conn":"n/a"}"#),
                expected: false,
            },
        ];

        for case in cases {
            assert_eq!(
                case.cluster.is_ready_for_app_manager_action(),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn cluster_and_app_shutdown_states_are_typed() {
        let cluster = decode_cluster(r#"{"state":"disabled"}"#);

        assert!(cluster.is_disabled());

        for app in [
            decode_cluster_app(r#"{"status":"stopped"}"#),
            decode_cluster_app(
                r#"{
                    "status":"not ok",
                    "reason":"stopped by user",
                    "addition-info":"CONTROL_PLANE_STATE_UNDEFINED"
                }"#,
            ),
            decode_cluster_app(r#"{"addition-info":"CONTROL_PLANE_STATE_STOPPED"}"#),
        ] {
            assert!(app.is_stopped());
        }

        assert!(!decode_cluster_app(r#"{"status":"not ok","reason":"starting"}"#).is_stopped());
    }

    #[test]
    fn app_ready_for_manager_action_reads_nvue_shapes() {
        struct TestCase {
            name: &'static str,
            app_status: ClusterApp,
            expected: bool,
        }

        let cases = [
            TestCase {
                name: "ok status with manager state",
                app_status: decode_cluster_app(
                    r#"{
                        "status": "ok",
                        "addition-info": "CONTROL_PLANE_STATE_CONFIGURED",
                        "manager": {"state": "disabled"}
                    }"#,
                ),
                expected: true,
            },
            TestCase {
                name: "legacy additional info field with manager state",
                app_status: decode_cluster_app(
                    r#"{
                        "status": "not ok",
                        "additional-info": "CONTROL_PLANE_STATE_UNCONFIGURED",
                        "manager": {"state": "disabled"}
                    }"#,
                ),
                expected: true,
            },
            TestCase {
                name: "starting app is not ready",
                app_status: decode_cluster_app(
                    r#"{
                        "status": "not ok",
                        "addition-info": "STARTING",
                        "manager": {"state": "disabled"}
                    }"#,
                ),
                expected: false,
            },
            TestCase {
                name: "control plane state without manager is not ready",
                app_status: decode_cluster_app(
                    r#"{
                        "status": "not ok",
                        "addition-info": "CONTROL_PLANE_STATE_UNCONFIGURED"
                    }"#,
                ),
                expected: false,
            },
        ];

        for case in cases {
            assert_eq!(
                case.app_status.is_ready_for_manager_action(),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn manager_reached_state_accepts_active_as_enabled_only_for_enable() {
        for state in ["enabled", "active"] {
            let app = ClusterApp {
                status: None,
                reason: None,
                addition_info: None,
                manager: Some(ClusterAppManager {
                    state: Some(state.to_owned()),
                }),
            };

            assert!(app.manager_reached_state("enabled"), "{state}");
        }

        let starting = ClusterApp {
            status: None,
            reason: None,
            addition_info: None,
            manager: Some(ClusterAppManager {
                state: Some("start".to_owned()),
            }),
        };
        let disabled = ClusterApp {
            status: None,
            reason: None,
            addition_info: None,
            manager: Some(ClusterAppManager {
                state: Some("disabled".to_owned()),
            }),
        };

        assert!(!starting.manager_reached_state("disabled"));
        assert!(!starting.manager_reached_state("enabled"));
        assert!(disabled.manager_reached_state("disabled"));
    }

    #[test]
    fn control_plane_configured_requires_exact_typed_state() {
        for field in ["addition-info", "additional-info"] {
            let configured = decode_cluster_app(&format!(
                r#"{{"{field}":"CONTROL_PLANE_STATE_CONFIGURED"}}"#
            ));

            assert!(configured.is_control_plane_configured(), "{field}");
        }

        for state in [
            "CONTROL_PLANE_STATE_UNCONFIGURED",
            "CONTROL_PLANE_STATE_OFFLINE",
            "CONTROL_PLANE_STATE_STANDBY",
            "CONTROL_PLANE_STATE_STARTING",
        ] {
            let transitional = decode_cluster_app(&format!(r#"{{"addition-info":"{state}"}}"#));

            assert!(
                !transitional.is_control_plane_configured(),
                "{state} must not satisfy configured convergence"
            );
        }
    }
}
