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

use crate::NVUE_V1_SERVER;
use crate::action::SimpleAction;
use crate::uri::{path_segment, query_value};

use serde::{Deserialize, Serialize};

/// Cluster state value for enabled clusters.
pub const CLUSTER_STATE_ENABLED: &str = "enabled";

/// `nmxc-conn` value for an up connection.
pub const NMXC_CONN_UP: &str = "up";

/// Cluster app status value for healthy apps.
pub const APP_STATUS_OK: &str = "ok";

/// Cluster app manager state value for enabled managers.
pub const APP_MANAGER_STATE_ENABLED: &str = "enabled";

/// Cluster app manager active state observed in switch responses.
pub const APP_MANAGER_STATE_ACTIVE: &str = "active";

/// Control plane states treated as manager-action-ready.
pub const NMX_CONTROL_PLANE_READY_STATES: &[&str] = &[
    "CONTROL_PLANE_STATE_CONFIGURED",
    "CONTROL_PLANE_STATE_UNCONFIGURED",
];

/// Full endpoint used by the existing RMS HTTP client for `GET /cluster`.
pub const CLUSTER_ENDPOINT: &str = "/nvue_v1/cluster";

/// Builds `/cluster?rev={revision-id}` for configuration updates.
pub fn cluster_endpoint_for_revision(revision_id: &str) -> String {
    format!("{CLUSTER_ENDPOINT}?rev={}", query_value(revision_id))
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
    /// Returns true when the cluster can accept cluster app manager actions.
    pub fn is_ready_for_app_manager_action(&self) -> bool {
        self.state.as_deref() == Some(CLUSTER_STATE_ENABLED)
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
        self.state() == Some(NMXC_CONN_UP)
    }
}

/// Minimal view of `schema-cluster-cluster-app` needed around manager actions.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ClusterApp {
    /// App health/status string.
    #[serde(default)]
    pub status: Option<String>,

    /// Additional app state. The OpenAPI field is typoed as `addition-info`.
    #[serde(default, rename = "addition-info", alias = "additional-info")]
    pub addition_info: Option<String>,

    /// Cluster app manager state.
    #[serde(default)]
    pub manager: Option<ClusterAppManager>,
}

impl ClusterApp {
    /// Returns true when this app can accept a manager action.
    pub fn is_ready_for_manager_action(&self) -> bool {
        self.manager_state().is_some()
            && (self.status.as_deref() == Some(APP_STATUS_OK)
                || self
                    .addition_info
                    .as_deref()
                    .is_some_and(|state| NMX_CONTROL_PLANE_READY_STATES.contains(&state)))
    }

    /// Returns the nested app manager state.
    pub fn manager_state(&self) -> Option<&str> {
        self.manager.as_ref().and_then(|manager| manager.state())
    }

    /// Returns true once the app manager reached the requested state.
    pub fn manager_reached_state(&self, desired: &str) -> bool {
        self.manager_state().is_some_and(|state| {
            state == desired
                || (desired == APP_MANAGER_STATE_ENABLED && state == APP_MANAGER_STATE_ACTIVE)
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
            app_endpoint("nmx/controller"),
            "/nvue_v1/cluster/apps/nmx%2Fcontroller"
        );
    }

    #[test]
    fn action_payloads_use_nvue_action_keys() {
        let cluster = serde_json::to_string(&ClusterUpdate::new(CLUSTER_STATE_ENABLED)).unwrap();
        let start = serde_json::to_string(&ClusterAppStartRequest::new()).unwrap();
        let stop = serde_json::to_string(&ClusterAppStopRequest::new()).unwrap();
        let manager = serde_json::to_string(&ClusterAppManagerUpdateRequest::new(
            APP_MANAGER_STATE_ENABLED,
        ))
        .unwrap();

        assert_eq!(cluster, r#"{"state":"enabled"}"#);
        assert!(start.contains(r#""@start""#));
        assert!(stop.contains(r#""@stop""#));
        assert_eq!(manager, r#"{"@update":{"parameters":{"state":"enabled"}}}"#);
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
                addition_info: None,
                manager: Some(ClusterAppManager {
                    state: Some(state.to_owned()),
                }),
            };

            assert!(app.manager_reached_state("enabled"), "{state}");
        }

        let starting = ClusterApp {
            status: None,
            addition_info: None,
            manager: Some(ClusterAppManager {
                state: Some("start".to_owned()),
            }),
        };
        let disabled = ClusterApp {
            status: None,
            addition_info: None,
            manager: Some(ClusterAppManager {
                state: Some("disabled".to_owned()),
            }),
        };

        assert!(!starting.manager_reached_state("disabled"));
        assert!(!starting.manager_reached_state("enabled"));
        assert!(disabled.manager_reached_state("disabled"));
    }
}
