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

//! Cluster application state, manager actions, and gRPC/gNMI health workflows.

use std::time::Duration;

use super::super::validation::is_valid_identifier;
use super::super::{SwitchGb200Nvidia, config};
use super::RETRY_WAIT_DELAY;
use crate::transport::http_client::HttpClient;
use crate::transport::ssh_client::SshClient;
use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::insert_json_field;

use nvue_client::cluster::{
    ClusterAppManagerState, ClusterAppStartRequest, ClusterAppStatus, ClusterAppStopRequest,
    ClusterState, NmxControlPlaneState, NmxcConnectionState, app_endpoint,
};
use serde_json::Value;

async fn test_port_connectivity(
    hostname: &str,
    target_port: u16,
    timeout: Duration,
) -> Result<bool> {
    let probe = HttpClient::new(hostname, target_port, "", "", false, false)?;

    match probe.get("/", timeout).await {
        Ok(_) => Ok(true),
        Err(e) if e.code == ErrorCode::ConnectionRefused || e.code == ErrorCode::Timeout => {
            Ok(false)
        }
        // For gRPC ports, a non-HTTP response still means the port is
        // reachable — only ConnectionRefused/Timeout mean truly unreachable.
        Err(_) => Ok(true),
    }
}

// Keep these predicates narrow while this module still reads dynamic JSON.
// NVUE spellings live in nvue_client; RMS owns only the manager-action policy.
fn nvue_state_field(value: Option<&Value>) -> Option<&str> {
    match value {
        Some(Value::String(state)) => Some(state),
        Some(Value::Object(fields)) => fields.get("state").and_then(Value::as_str),
        _ => None,
    }
}

fn cluster_ready_for_app_manager_action(cluster: &Value) -> bool {
    let state = cluster.get("state").and_then(Value::as_str);
    let nmxc_conn = nvue_state_field(cluster.get("nmxc-conn"));

    state.is_some_and(|state| ClusterState::try_from(state) == Ok(ClusterState::Enabled))
        && nmxc_conn.is_some_and(|state| {
            NmxcConnectionState::try_from(state) == Ok(NmxcConnectionState::Up)
        })
}

fn cluster_enabled_for_app_manager_action(cluster: &Value) -> bool {
    cluster
        .get("state")
        .and_then(Value::as_str)
        .is_some_and(|state| ClusterState::try_from(state) == Ok(ClusterState::Enabled))
}

fn app_manager_state(app_status: &Value) -> Option<&str> {
    app_status
        .get("manager")
        .and_then(|manager| manager.get("state"))
        .and_then(Value::as_str)
}

pub(super) fn app_control_plane_state(app_status: &Value) -> Option<&str> {
    ["additional-info", "addition-info"]
        .iter()
        .find_map(|field| app_status.get(*field).and_then(Value::as_str))
}

fn app_ready_for_manager_action(app_status: &Value) -> bool {
    let status = app_status.get("status").and_then(Value::as_str);
    let control_plane_state = app_control_plane_state(app_status);

    status.is_some_and(|status| ClusterAppStatus::try_from(status) == Ok(ClusterAppStatus::Ok))
        || control_plane_state
            .and_then(|state| NmxControlPlaneState::try_from(state).ok())
            .is_some_and(NmxControlPlaneState::is_ready_for_manager_action)
}

fn app_waiting_for_manager_action(app_status: &Value) -> bool {
    !app_ready_for_manager_action(app_status)
        && (app_status.get("status").and_then(Value::as_str).is_some()
            || app_control_plane_state(app_status).is_some())
}

fn app_stopped_for_manager_action(app_status: &Value) -> bool {
    app_status
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| {
            ClusterAppStatus::try_from(status) == Ok(ClusterAppStatus::Stopped)
                || status.eq_ignore_ascii_case(ClusterAppStatus::Stopped.as_str())
        })
        || app_control_plane_state(app_status)
            .and_then(|state| NmxControlPlaneState::try_from(state).ok())
            .is_some_and(NmxControlPlaneState::is_stopped)
}

fn app_manager_reached_state(app_status: &Value, desired: &str) -> bool {
    app_manager_state(app_status).is_some_and(|state| {
        state == desired
            || (desired == ClusterAppManagerState::Enabled.as_str()
                && matches!(
                    ClusterAppManagerState::try_from(state),
                    Ok(ClusterAppManagerState::Start | ClusterAppManagerState::Active)
                ))
    })
}

pub fn grpc_port_for_app(app_name: &str) -> u16 {
    match app_name {
        "nmx-telemetry" => config::GRPC_PORT_NMX_TELEMETRY,
        "nmx-controller" => config::GRPC_PORT_NMX_CONTROLLER,
        _ => config::GRPC_PORT_NMX_CONTROLLER,
    }
}

impl SwitchGb200Nvidia {
    pub(crate) async fn get_cluster_app_manager_leaf(
        &self,
        app_name: &str,
        leaf: &str,
    ) -> Result<Value> {
        if !is_valid_identifier(app_name) || !is_valid_identifier(leaf) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app manager path: {app_name}/{leaf}"
            )));
        }

        self.nvue_http_get(
            &format!("/nvue_v1/cluster/apps/{app_name}/manager/{leaf}"),
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub async fn get_cluster_apps_status(&self, app_name: &str) -> Result<Value> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        match self
            .nvue_http_get(
                &format!("/nvue_v1/cluster/apps/{app_name}"),
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await
        {
            Ok(mut result) => {
                let status = result
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_owned();

                insert_json_field(&mut result, "installed", serde_json::json!(true));

                insert_json_field(
                    &mut result,
                    "health_status",
                    serde_json::json!(if status == "ok" { "green" } else { "red" }),
                );

                let status_description = if status == "ok" {
                    serde_json::json!(format!("{app_name} is running and healthy"))
                } else {
                    serde_json::json!(format!("{app_name} status: {status}"))
                };

                insert_json_field(&mut result, "status_description", status_description);

                Ok(result)
            }
            Err(e) if e.code == ErrorCode::NotFound => Ok(serde_json::json!({
                "installed": false,
                "health_status": "unknown",
                "status_description": format!("App {app_name} is not installed"),
            })),
            Err(e) => Err(e),
        }
    }

    // Idempotent: checks current gRPC state before toggling via SSH
    pub async fn enable_grpc_for_external_clients(
        &self,
        app_name: &str,
        enabled: bool,
    ) -> Result<Value> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        let desired = if enabled { "enabled" } else { "disabled" };

        let target_switch = self.require_host_endpoint()?.ip_address.clone();

        {
            tracing::info!(node = %self.id, app_name, enabled, "enable_grpc_for_external_clients");

            // Short-circuit if already in desired state.
            if let Ok(pre) = self.check_grpc_status(app_name, &target_switch).await
                && let Some(summary) = pre.get("summary")
            {
                let already_enabled = summary
                    .get("grpc_enabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

                let port_reachable = summary
                    .get("port_reachable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

                if enabled && already_enabled && port_reachable {
                    tracing::info!(node = %self.id, "gRPC already enabled, skipping");
                    return Ok(pre);
                }

                if !enabled && !already_enabled {
                    tracing::info!(node = %self.id, "gRPC already disabled, skipping");
                    return Ok(pre);
                }
            }
        }

        if enabled {
            match self
                .wait_for_cluster_app_manager_action_ready(app_name)
                .await?
            {
                Some(status) if app_manager_reached_state(&status, desired) => return Ok(status),
                Some(_) => {}
                None => {
                    tracing::warn!(
                        node = %self.id,
                        app_name,
                        "cluster app manager-action readiness was not observed; attempting action"
                    );
                }
            }
        }

        self.run_cluster_app_manager_action(app_name, desired)
            .await?;

        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            let status = self.get_cluster_apps_status(app_name).await;

            if let Ok(status) = status
                && app_manager_reached_state(&status, desired)
            {
                return Ok(status);
            }

            if attempt < config::MAX_RETRY_ATTEMPTS {
                tokio::time::sleep(RETRY_WAIT_DELAY).await;
            }
        }

        Err(RmsError::internal(format!(
            "gRPC manager for {app_name} did not reach {desired}"
        )))
    }

    async fn wait_for_cluster_app_manager_action_ready(
        &self,
        app_name: &str,
    ) -> Result<Option<Value>> {
        // NVOS can report the cluster enabled before the app manager callback
        // accepts manager actions. Wait only while NVUE reports an explicit
        // transitional state; when readiness is not observable, let the SSH
        // action remain the source of truth.
        let mut start_requested = false;

        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            let should_wait = match self
                .nvue_http_get("/nvue_v1/cluster", HttpClient::DEFAULT_TIMEOUT)
                .await
            {
                Ok(cluster) if !cluster_enabled_for_app_manager_action(&cluster) => true,
                Ok(cluster) => {
                    let cluster_ready = cluster_ready_for_app_manager_action(&cluster);

                    let nmxc_conn_pending =
                        nvue_state_field(cluster.get("nmxc-conn")).is_some_and(|state| {
                            NmxcConnectionState::try_from(state) != Ok(NmxcConnectionState::Up)
                        });

                    match self.get_cluster_apps_status(app_name).await {
                        Ok(app_status)
                            if cluster_ready && app_ready_for_manager_action(&app_status) =>
                        {
                            return Ok(Some(app_status));
                        }
                        Ok(app_status) if app_stopped_for_manager_action(&app_status) => {
                            if !start_requested {
                                tracing::info!(
                                    node = %self.id,
                                    app_name,
                                    "starting stopped cluster app before manager action"
                                );

                                self.start_cluster_app(app_name).await?;

                                start_requested = true;
                            }

                            true
                        }
                        Ok(app_status) => {
                            nmxc_conn_pending || app_waiting_for_manager_action(&app_status)
                        }
                        Err(error) => {
                            tracing::debug!(
                                node = %self.id,
                                app_name,
                                error = %error.message,
                                "cluster app readiness was not observable before manager action"
                            );

                            nmxc_conn_pending
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        node = %self.id,
                        app_name,
                        error = %error.message,
                        "cluster readiness was not observable before manager action"
                    );

                    false
                }
            };

            if !should_wait {
                return Ok(None);
            }

            if attempt < config::MAX_RETRY_ATTEMPTS {
                tokio::time::sleep(RETRY_WAIT_DELAY).await;
            }
        }

        if start_requested {
            Err(RmsError::internal(format!(
                "cluster app {app_name} did not become ready after start"
            )))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn ensure_cluster_app_manager_action_ready(
        &self,
        app_name: &str,
    ) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        if self
            .wait_for_cluster_app_manager_action_ready(app_name)
            .await?
            .is_none()
        {
            tracing::warn!(
                node = %self.id,
                app_name,
                "cluster app manager-action readiness was not observed; attempting action"
            );
        }

        Ok(())
    }

    async fn start_cluster_app(&self, app_name: &str) -> Result<()> {
        let cmd = format!("nv action start cluster apps {app_name}");

        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            exec(&cmd)?;
            return Ok(());
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;
        ssh.exec(&cmd, SshClient::DEFAULT_TIMEOUT).await?;

        Ok(())
    }

    async fn run_cluster_app_manager_nv_action(&self, app_name: &str, args: &str) -> Result<()> {
        let cmd = format!("nv action update cluster apps {app_name} manager {args}");

        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            exec(&cmd)?;
            return Ok(());
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;
        ssh.exec(&cmd, SshClient::DEFAULT_TIMEOUT).await?;

        Ok(())
    }

    async fn run_cluster_app_manager_action(&self, app_name: &str, desired: &str) -> Result<()> {
        self.run_cluster_app_manager_nv_action(app_name, desired)
            .await
    }

    /// Runs `nv action update cluster apps <app> manager <field> <value>` over SSH.
    pub(crate) async fn run_cluster_app_manager_field_action(
        &self,
        app_name: &str,
        field: &str,
        value: &str,
    ) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app name: {app_name}"
            )));
        }

        if field.is_empty() || value.is_empty() {
            return Err(RmsError::invalid_argument(
                "cluster app manager field and value must be non-empty",
            ));
        }

        self.run_cluster_app_manager_nv_action(app_name, &format!("{field} {value}"))
            .await
    }

    // Enable/disable gNMI via SSH, then poll until state converges
    pub async fn gnmi_service(&self, enabled: bool) -> Result<Value> {
        let state = if enabled { "enabled" } else { "disabled" };

        tracing::info!(node = %self.id, enabled, "gnmi_service");

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        ssh.exec(
            &format!("nv set system gnmi-server state {state}"),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await?;

        ssh.exec("nv config apply --assume-yes", SshClient::DEFAULT_TIMEOUT)
            .await?;

        ssh.exec("nv config save", SshClient::DEFAULT_TIMEOUT)
            .await?;

        tokio::time::sleep(Duration::from_secs(config::GNMI_CONFIG_WAIT_SECONDS)).await;

        let delays = [
            config::GNMI_RETRY_DELAY_1_SECONDS,
            config::GNMI_RETRY_DELAY_2_SECONDS,
            config::GNMI_RETRY_DELAY_2_SECONDS,
        ];

        for delay in delays.iter().take(config::MAX_RETRY_ATTEMPTS as usize) {
            if let Ok(ssh) =
                SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await
                && let Ok(output) = ssh
                    .exec(
                        "nv show system gnmi-server -o json",
                        SshClient::DEFAULT_TIMEOUT,
                    )
                    .await
                && let Ok(j) = serde_json::from_str::<Value>(&output)
                && j.get("state").and_then(|v| v.as_str()) == Some(state)
            {
                return Ok(serde_json::json!({"status": "success", "state": state}));
            }

            tokio::time::sleep(Duration::from_secs(*delay)).await;
        }

        Err(RmsError::internal(format!(
            "gNMI service did not reach state {state} after retries"
        )))
    }

    pub async fn restart_cluster_app(&self, app_name: &str) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        let endpoint = app_endpoint(app_name);

        let stop_payload = serde_json::to_value(ClusterAppStopRequest::new()).map_err(|e| {
            RmsError::internal(format!("failed to serialize cluster app stop action: {e}"))
        })?;

        let start_payload = serde_json::to_value(ClusterAppStartRequest::new()).map_err(|e| {
            RmsError::internal(format!("failed to serialize cluster app start action: {e}"))
        })?;

        // Each leg POSTs the NVUE action and, when the switch returns an action
        // job, waits for it to complete (or fail/exhaust) via
        // nvue_run_action_payload rather than sleeping a fixed interval.
        tracing::info!(node = %self.id, app_name, "restart_cluster_app: stop via NVUE");
        self.nvue_run_action_payload(&endpoint, "@stop", stop_payload)
            .await?;

        tracing::info!(node = %self.id, app_name, "restart_cluster_app: start via NVUE");
        self.nvue_run_action_payload(&endpoint, "@start", start_payload)
            .await?;

        Ok(())
    }

    pub async fn check_grpc_status(&self, app_name: &str, target_switch: &str) -> Result<Value> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid app name: {app_name}"
            )));
        }

        if target_switch.is_empty() {
            return Err(RmsError::invalid_argument(
                "target switch name cannot be empty",
            ));
        }

        let grpc_port = grpc_port_for_app(app_name);
        let endpoint = format!("/nvue_v1/cluster/apps/{app_name}/manager");

        let manager_resp = self
            .nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await;

        let (grpc_enabled, grpc_status) = match manager_resp {
            Ok(resp) => {
                let s = resp
                    .get("state")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();

                let enabled = matches!(
                    ClusterAppManagerState::try_from(s),
                    Ok(ClusterAppManagerState::Start
                        | ClusterAppManagerState::Enabled
                        | ClusterAppManagerState::Active)
                );

                (enabled, resp)
            }
            Err(_) => (false, Value::Null),
        };

        let port_reachable =
            test_port_connectivity(target_switch, grpc_port, Duration::from_secs(10))
                .await
                .unwrap_or(false);

        if port_reachable {
            tracing::info!("gRPC port {} is reachable", grpc_port);
        } else {
            tracing::info!("gRPC port {} is not reachable", grpc_port);
        }

        Ok(serde_json::json!({
            "app_name": app_name,
            "target_switch": target_switch,
            "grpc_status": grpc_status,
            "port_connectivity": {
                "reachable": port_reachable,
                "port": grpc_port,
            },
            "summary": {
                "grpc_enabled": grpc_enabled,
                "port_reachable": port_reachable,
                "ready_for_external_clients": grpc_enabled && port_reachable,
            },
        }))
    }
}
