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

use nvue_client::DEFAULT_TIMEOUT as NVUE_DEFAULT_TIMEOUT;
use nvue_client::cluster::{
    Cluster, ClusterApp, ClusterAppManagerState, ClusterAppManagerUpdateRequest,
    ClusterAppStartRequest, ClusterAppStatus, ClusterAppStopRequest, ClusterState, NmxcConnection,
    NmxcConnectionState, app_endpoint, app_manager_endpoint,
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

fn cluster_enabled_for_app_manager_action(cluster: &Cluster) -> bool {
    cluster
        .state
        .as_deref()
        .is_some_and(|state| ClusterState::try_from(state) == Ok(ClusterState::Enabled))
}

fn app_ready_for_manager_action(app: &ClusterApp) -> bool {
    // Do not require `manager.state` here. If runtime state is ready but the
    // manager field is absent, the caller should attempt the SSH action instead
    // of waiting for a field that NVUE may not expose yet.
    app.status.as_deref() == Some("ok")
        || app
            .control_plane_state()
            .is_some_and(|state| state.is_ready_for_manager_action())
}

fn app_waiting_for_manager_action(app: &ClusterApp) -> bool {
    !app_ready_for_manager_action(app) && (app.status.is_some() || app.addition_info.is_some())
}

fn app_stopped_for_manager_action(app: &ClusterApp) -> bool {
    app.status
        .as_deref()
        .is_some_and(|status| status.eq_ignore_ascii_case(ClusterAppStatus::Stopped.as_str()))
        || app
            .control_plane_state()
            .is_some_and(|state| state.is_stopped())
}

fn app_manager_reached_state(app: &ClusterApp, desired: &str) -> bool {
    app.manager_state().is_some_and(|state| {
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

/// Extract the HTTP status code embedded in an NVUE error message.
///
/// NVUE HTTP failures surface as `... returned <status>` (see
/// `nvue_client::ClientError::HttpStatus`), and the action wrapper preserves
/// that text, so the numeric status is recoverable from the message even
/// though the wrapper flattens the error code to `Internal`.
fn nvue_http_status_from_message(message: &str) -> Option<u16> {
    const MARKER: &str = "returned ";
    let start = message.find(MARKER)? + MARKER.len();
    let digits: String = message[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

/// True when a failed NVUE manager-encryption `@update` is a definitive
/// client-side rejection that must surface rather than be masked by the SSH
/// cert-install backstop.
///
/// The backstop exists only for the mTLS-bootstrap window where the NVUE
/// channel itself is unavailable or mid-transition -- connectivity loss, a TLS
/// handshake failure, or a 401/403 while RMS's outbound client identity is not
/// yet trusted -- plus transient server-side (5xx) errors. A non-transient
/// HTTP 4xx (e.g. 400 malformed body, 404 unknown app, 409 encryption already
/// set) or a failed action job means NVUE itself rejected the request; the SSH
/// nvCLI form carries the same semantics and could paper over the real API
/// problem, so those surface directly instead of triggering the fallback.
fn is_nonretryable_encryption_bind_error(err: &RmsError) -> bool {
    // Typed client-side rejections raised before or independent of an HTTP
    // status (e.g. argument validation in the action helper itself).
    if matches!(
        err.code,
        ErrorCode::InvalidArgument
            | ErrorCode::NotFound
            | ErrorCode::AlreadyExists
            | ErrorCode::FailedPrecondition
    ) {
        return true;
    }

    let message = err.message.to_ascii_lowercase();

    // A failed NVUE action job is a definitive rejection, not a transport blip.
    if message.contains("failed with state") {
        return true;
    }

    // Non-transient client HTTP 4xx must surface. 401/403 (RMS not yet trusted
    // during the mTLS transition) and 408 (request timeout) stay eligible for
    // the backstop, as do 5xx server errors and connectivity failures.
    match nvue_http_status_from_message(&message) {
        Some(status) => (400..500).contains(&status) && !matches!(status, 401 | 403 | 408),
        None => false,
    }
}

impl SwitchGb200Nvidia {
    /// Builds the typed readiness view while preserving the augmented status response.
    ///
    /// `ClusterApp` intentionally ignores RMS reporting fields such as `installed`.
    /// A not-found sentinel therefore follows the unknown-readiness fallback, while
    /// callers still receive the original response containing `installed: false`.
    async fn read_cluster_app_status(&self, app_name: &str) -> Result<(ClusterApp, Value)> {
        let response = self.get_cluster_apps_status(app_name).await?;

        let app = serde_json::from_value(response.clone()).map_err(|error| {
            RmsError::internal(format!(
                "invalid NVUE cluster application response for '{app_name}': {error}"
            ))
        })?;

        Ok((app, response))
    }

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

    // Idempotent: checks current gRPC state before toggling via the NVUE
    // cluster-app manager `@update` action.
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
                Some((status, response)) if app_manager_reached_state(&status, desired) => {
                    return Ok(response);
                }
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
            if let Ok((status, response)) = self.read_cluster_app_status(app_name).await
                && app_manager_reached_state(&status, desired)
            {
                return Ok(response);
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
    ) -> Result<Option<(ClusterApp, Value)>> {
        // NVOS can report the cluster enabled before the app manager callback
        // accepts manager actions. Wait only while NVUE reports an explicit
        // transitional state; when readiness is not observable, let the NVUE
        // manager action proceed and surface any error itself.
        let mut start_requested = false;

        for attempt in 0..=config::MAX_RETRY_ATTEMPTS {
            let should_wait = match self
                .nvue_client()?
                .get_cluster(NVUE_DEFAULT_TIMEOUT)
                .await
                .map_err(RmsError::from)
            {
                Ok(cluster) if !cluster_enabled_for_app_manager_action(&cluster) => true,
                Ok(cluster) => {
                    let cluster_ready = cluster.is_ready_for_app_manager_action();

                    let nmxc_conn_pending = cluster
                        .nmxc_conn
                        .as_ref()
                        .and_then(NmxcConnection::state)
                        .is_some_and(|state| {
                            NmxcConnectionState::try_from(state) != Ok(NmxcConnectionState::Up)
                        });

                    match self.read_cluster_app_status(app_name).await {
                        Ok((app_status, response))
                            if cluster_ready && app_ready_for_manager_action(&app_status) =>
                        {
                            return Ok(Some((app_status, response)));
                        }
                        Ok((app_status, _)) if app_stopped_for_manager_action(&app_status) => {
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
                        Ok((app_status, _)) => {
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

    /// Starts a stopped cluster app through the NVUE `@start` action
    /// (`POST /cluster/apps/<app>`), replacing the former
    /// `nv action start cluster apps <app>` SSH command. The action job, when
    /// returned, is polled to completion with a bounded timeout.
    async fn start_cluster_app(&self, app_name: &str) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app name: {app_name}"
            )));
        }

        let endpoint = app_endpoint(app_name);
        let payload = serde_json::to_value(ClusterAppStartRequest::new()).map_err(|e| {
            RmsError::internal(format!("failed to serialize cluster app start action: {e}"))
        })?;

        self.nvue_run_action_payload(&endpoint, "@start", payload)
            .await
    }

    /// Sets the cluster-app manager state (`enabled`/`disabled`) through the
    /// NVUE `@update` action (`POST /cluster/apps/<app>/manager`), replacing the
    /// former `nv action update cluster apps <app> manager <state>` SSH command.
    async fn run_cluster_app_manager_action(&self, app_name: &str, desired: &str) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app name: {app_name}"
            )));
        }

        let endpoint = app_manager_endpoint(app_name);
        let payload =
            serde_json::to_value(ClusterAppManagerUpdateRequest::new(desired)).map_err(|e| {
                RmsError::internal(format!(
                    "failed to serialize cluster app manager update: {e}"
                ))
            })?;

        self.nvue_run_action_payload(&endpoint, "@update", payload)
            .await
    }

    /// Updates a single cluster-app manager field through the NVUE `@update`
    /// action (`POST /cluster/apps/<app>/manager/<field>`), replacing the former
    /// `nv action update cluster apps <app> manager <field> <value>` SSH command.
    ///
    /// `field` is the NVUE path segment under `/manager`; `param_key` is the
    /// NVUE action parameter name that carries `value`. They are usually the
    /// same, but not always: in NVOS `cue_cluster_v1` the `encryption` segment
    /// binds its value from a `mode` parameter (the action token is `<mode>`),
    /// so the parameter name is passed explicitly rather than assumed equal to
    /// the segment.
    pub(crate) async fn run_cluster_app_manager_field_action(
        &self,
        app_name: &str,
        field: &str,
        param_key: &str,
        value: &str,
    ) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app name: {app_name}"
            )));
        }

        // `field` is a URL path segment and `param_key` is a JSON key, so both
        // must be restricted identifiers; `value` only needs to be present.
        if !is_valid_identifier(field) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app manager field: {field}"
            )));
        }

        if !is_valid_identifier(param_key) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app manager parameter: {param_key}"
            )));
        }

        if value.is_empty() {
            return Err(RmsError::invalid_argument(
                "cluster app manager field value must be non-empty",
            ));
        }

        let endpoint = format!("{}/{field}", app_manager_endpoint(app_name));
        let parameters = serde_json::json!({ param_key: value });

        self.nvue_start_action(&endpoint, "@update", parameters)
            .await
    }

    /// Binds the cluster app manager encryption mode to mTLS, preferring the
    /// typed NVUE `@update` action and falling back to SSH nvCLI on failure.
    ///
    /// This is the manager-encryption step of cert installation. During mTLS
    /// bootstrap the NVUE channel can be unavailable or mid-transition (RMS's
    /// outbound client identity only joins the verified transport once the
    /// switch enables mTLS), so an SSH backstop is retained specifically for
    /// this step. The fallback is explicit, logged, and self-contained in
    /// [`Self::run_cluster_app_manager_encryption_ssh_fallback`] so it can be
    /// removed on its own once NVOS guarantees the NVUE manager-encryption
    /// action is available throughout the cert-install transition.
    pub(crate) async fn bind_cluster_app_manager_encryption_mtls(
        &self,
        app_name: &str,
    ) -> Result<()> {
        match self
            .run_cluster_app_manager_field_action(app_name, "encryption", "mode", "mtls")
            .await
        {
            Ok(()) => Ok(()),
            // A definitive NVUE rejection (non-transient client 4xx or a failed
            // action job) is surfaced directly: the SSH backstop shares the same
            // semantics and would otherwise mask a real NVUE API problem.
            Err(error) if is_nonretryable_encryption_bind_error(&error) => {
                tracing::warn!(
                    node = %self.id,
                    app_name,
                    error = %error.message,
                    "NVUE encryption bind rejected by switch; surfacing error without SSH backstop"
                );

                Err(error)
            }
            // Transport/transient failure (connectivity loss, TLS handshake, an
            // untrusted 401/403 during the mTLS transition, or a 5xx): fall back
            // to the SSH nvCLI backstop, which does not depend on the NVUE
            // channel that may still be mid-transition.
            Err(error) => {
                tracing::warn!(
                    node = %self.id,
                    app_name,
                    error = %error.message,
                    "NVUE encryption bind failed (transport/transient); retrying via SSH nvCLI (cert-install backstop)"
                );

                self.run_cluster_app_manager_encryption_ssh_fallback(app_name, "mtls")
                    .await
            }
        }
    }

    /// SSH backstop for the manager-encryption bind used during cert
    /// installation, reproducing the pre-NVUE nvCLI command
    /// `nv action update cluster apps <app> manager encryption <mode>`.
    ///
    /// Kept isolated and separately removable; see
    /// [`Self::bind_cluster_app_manager_encryption_mtls`] for why the SSH path
    /// is retained only for this step.
    async fn run_cluster_app_manager_encryption_ssh_fallback(
        &self,
        app_name: &str,
        mode: &str,
    ) -> Result<()> {
        if !is_valid_identifier(app_name) {
            return Err(RmsError::invalid_argument(format!(
                "invalid cluster app name: {app_name}"
            )));
        }

        if !is_valid_identifier(mode) {
            return Err(RmsError::invalid_argument(format!(
                "invalid encryption mode: {mode}"
            )));
        }

        let cmd = format!("nv action update cluster apps {app_name} manager encryption {mode}");

        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            exec(&cmd)?;
            return Ok(());
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;
        ssh.exec(&cmd, SshClient::DEFAULT_TIMEOUT).await?;

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::{is_nonretryable_encryption_bind_error, nvue_http_status_from_message};
    use crate::utilities::error::{ErrorCode, RmsError};

    #[test]
    fn nvue_http_status_from_message_extracts_status() {
        assert_eq!(
            nvue_http_status_from_message(
                "nvue post /manager/encryption @update failed: http post /manager/encryption returned 409"
            ),
            Some(409)
        );
        assert_eq!(
            nvue_http_status_from_message("returned 400: bad body"),
            Some(400)
        );
        assert_eq!(nvue_http_status_from_message("connection refused"), None);
        assert_eq!(nvue_http_status_from_message("returned soon"), None);
    }

    #[test]
    fn client_4xx_rejections_surface() {
        for status in [400u16, 405, 409, 422, 499] {
            let err = RmsError::internal(format!(
                "NVUE POST /x @update failed: HTTP POST /x returned {status}"
            ));
            assert!(
                is_nonretryable_encryption_bind_error(&err),
                "status {status} should surface"
            );
        }
    }

    #[test]
    fn typed_client_codes_surface() {
        for code in [
            ErrorCode::InvalidArgument,
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::FailedPrecondition,
        ] {
            assert!(is_nonretryable_encryption_bind_error(&RmsError::new(
                code, "rejected"
            )));
        }
    }

    #[test]
    fn failed_action_job_surfaces() {
        let err = RmsError::internal("NVUE action enc-1 failed with state action_failed");
        assert!(is_nonretryable_encryption_bind_error(&err));
    }

    #[test]
    fn transient_and_transport_errors_keep_the_backstop() {
        // Auth-during-transition and request timeout HTTP statuses.
        for status in [401u16, 403, 408] {
            let err = RmsError::internal(format!(
                "NVUE POST /x @update failed: HTTP POST /x returned {status}"
            ));
            assert!(
                !is_nonretryable_encryption_bind_error(&err),
                "status {status} should keep the SSH backstop"
            );
        }

        // Server-side 5xx errors.
        for status in [500u16, 502, 503] {
            let err = RmsError::internal(format!(
                "NVUE POST /x @update failed: HTTP POST /x returned {status}"
            ));
            assert!(
                !is_nonretryable_encryption_bind_error(&err),
                "status {status} should keep the SSH backstop"
            );
        }

        // Connectivity / TLS transport failures (no HTTP status in the message).
        for message in [
            "NVUE POST /x @update failed: post /x: error trying to connect: connection refused",
            "NVUE POST /x @update failed: post /x: tls handshake eof",
        ] {
            assert!(!is_nonretryable_encryption_bind_error(&RmsError::internal(
                message
            )));
        }

        // Typed transport codes.
        for code in [
            ErrorCode::Timeout,
            ErrorCode::Unavailable,
            ErrorCode::ConnectionRefused,
            ErrorCode::DnsResolutionFailed,
            ErrorCode::Unauthenticated,
        ] {
            assert!(!is_nonretryable_encryption_bind_error(&RmsError::new(
                code,
                "transient"
            )));
        }
    }
}
