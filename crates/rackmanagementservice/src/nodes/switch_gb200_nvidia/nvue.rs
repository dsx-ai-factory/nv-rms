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

//! NVUE client, HTTP transport, revision, and action helpers for NVIDIA GB200 switches.

use std::time::Duration;

use super::{
    SwitchGb200Nvidia, config, is_http_unauthorized_error, is_retryable_nvue_transport_error,
};
use crate::domain::rack::EndpointConfig;
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{ErrorCode, Result, RmsError};

use nvue_client::revision::{
    REVISION_ENDPOINT, RevisionApplyStatus, RevisionCreate, RevisionIdResponse, RevisionResponse,
    RevisionUpdate, applied_revision_endpoint, revision_endpoint,
};
use nvue_client::{
    Client as NvueClient, ClientConfig as NvueConnectConfig, ClientCredentials as NvueCredentials,
    ClientEndpoint as NvueEndpoint, DEFAULT_TIMEOUT as NVUE_DEFAULT_TIMEOUT,
    SharedClient as SharedNvueClient,
};
use serde_json::Value;

pub(super) fn build_nvue_connect_config(
    host_endpoint: &EndpointConfig,
    username: &str,
    password: &str,
) -> Result<NvueConnectConfig> {
    Ok(NvueConnectConfig {
        endpoint: NvueEndpoint::new(
            &host_endpoint.endpoint.ip_address,
            host_endpoint.endpoint.port,
            host_endpoint.endpoint.host_name.as_deref(),
        )?,
        credentials: NvueCredentials::new(username, password),
        dangerously_accept_invalid_certs: host_endpoint.dangerously_accept_invalid_certs,
    })
}

pub(super) fn build_nvue_api(config: NvueConnectConfig) -> Result<SharedNvueClient> {
    NvueClient::new(config).map_err(Into::into)
}

fn rms_http_status_code(status: u16) -> ErrorCode {
    match status {
        401 => ErrorCode::Unauthenticated,
        404 => ErrorCode::NotFound,
        408 => ErrorCode::Timeout,
        409 => ErrorCode::AlreadyExists,
        503 => ErrorCode::Unavailable,
        _ => ErrorCode::Internal,
    }
}

pub(super) fn extract_job_id(response: &Value) -> Result<String> {
    nvue_client::action::extract_action_id(response).map_err(Into::into)
}

fn revision_apply_timeout(
    revision_id: &str,
    last_observed_state: Option<String>,
    last_retryable_error: Option<RmsError>,
) -> RmsError {
    let mut message = format!(
        "timed out waiting for revision {revision_id} to reach applied state or report no config diff"
    );

    if let Some(state) = last_observed_state {
        message.push_str("; last observed ");
        message.push_str(&state);
    }

    if let Some(error) = last_retryable_error {
        message.push_str("; last retryable error: ");
        message.push_str(&error.message);
    }

    RmsError::timeout(message)
}

fn revision_pending_state(state_prefix: Option<&str>, state: &str) -> Option<String> {
    if state.is_empty() {
        return None;
    }

    match state_prefix {
        Some(prefix) => Some(format!("{prefix} state={state}")),
        None => Some(format!("state={state}")),
    }
}

fn revision_poll_decision(
    response: &RevisionResponse,
    revision_id: &str,
    pending_state_prefix: Option<&str>,
) -> Result<RevisionPollDecision> {
    match response.apply_status(revision_id) {
        RevisionApplyStatus::Applied => Ok(RevisionPollDecision::Complete {
            no_config_diff: false,
        }),
        RevisionApplyStatus::NoConfigDiff => Ok(RevisionPollDecision::Complete {
            no_config_diff: true,
        }),
        RevisionApplyStatus::Failed { state, issue } => Err(revision_apply_failure(
            revision_id,
            state.as_str(),
            issue.as_str(),
        )),
        RevisionApplyStatus::Pending { state } => Ok(RevisionPollDecision::Pending {
            observed_state: revision_pending_state(pending_state_prefix, &state),
        }),
    }
}

fn revision_apply_failure(revision_id: &str, state: &str, issue: &str) -> RmsError {
    let message = if issue.is_empty() {
        format!("revision {revision_id} apply failed (state={state})")
    } else {
        format!("revision {revision_id} apply failed (state={state}): {issue}")
    };

    RmsError::failed_precondition(message)
}

fn is_candidate_revision_poll_error(err: &RmsError) -> bool {
    // Candidate credentials may return 401 while NVUE is still applying an
    // active-user password change. Keep that retryable only for candidate
    // polling; current-credential polling treats 401 as an auth failure.
    is_http_unauthorized_error(err) || is_retryable_nvue_transport_error(err)
}

enum RevisionPollDecision {
    Complete { no_config_diff: bool },
    Pending { observed_state: Option<String> },
}

enum CandidateRevisionDiagnostic {
    Continue {
        observed_state: Option<String>,
        retryable_error: Option<RmsError>,
    },
}

impl SwitchGb200Nvidia {
    pub(crate) fn nvue_client(&self) -> Result<&SharedNvueClient> {
        self.nvue.as_ref().ok_or_else(|| {
            RmsError::invalid_argument("host_endpoint is required for NVUE operations")
        })
    }

    pub(crate) fn optional_nvue_client(&self) -> Option<&SharedNvueClient> {
        self.nvue.as_ref()
    }

    pub(crate) fn validate_nvue_client_endpoint(&self, nvue: &SharedNvueClient) -> Result<()> {
        let host_endpoint = self.require_host_endpoint()?;
        let client_endpoint = nvue.endpoint();

        let expected_ip = host_endpoint
            .ip_address
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .map_err(|error| {
                RmsError::invalid_argument(format!(
                    "invalid switch management IP {}: {error}",
                    host_endpoint.ip_address
                ))
            })?;

        let actual_ip = client_endpoint
            .connect_host
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .map_err(|error| {
                RmsError::invalid_argument(format!(
                    "invalid NVUE client management IP {}: {error}",
                    client_endpoint.connect_host
                ))
            })?;

        if expected_ip != actual_ip || host_endpoint.port != client_endpoint.port {
            return Err(RmsError::invalid_argument(format!(
                "NVUE client {}:{} does not match switch management endpoint {}:{}",
                client_endpoint.connect_host,
                client_endpoint.port,
                host_endpoint.ip_address,
                host_endpoint.port
            )));
        }

        Ok(())
    }

    pub(crate) fn with_nvue_client(mut self, nvue: SharedNvueClient) -> Result<Self> {
        self.validate_nvue_client_endpoint(&nvue)?;

        self.nvue = Some(nvue);

        Ok(self)
    }

    #[cfg(test)]
    pub(crate) async fn configure_nvue_client_tls(
        &self,
        tls: Option<nvue_client::ClientTls>,
    ) -> Result<bool> {
        let client = self.nvue_client()?;

        match tls {
            Some(tls) => client
                .prepare_client_tls_if_changed(tls, None)
                .await
                .map(|prepared| prepared.is_some())
                .map_err(Into::into),
            None => client.configure_server_name(None).await.map_err(Into::into),
        }
    }

    pub(super) async fn nvue_http_get(&self, endpoint: &str, timeout: Duration) -> Result<Value> {
        Ok(self.nvue_client()?.get_json(endpoint, timeout).await?)
    }

    pub(super) async fn nvue_http_post(
        &self,
        endpoint: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<Value> {
        Ok(self
            .nvue_client()?
            .post_json(endpoint, payload, timeout)
            .await?)
    }

    pub(super) async fn nvue_http_patch(
        &self,
        endpoint: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<Value> {
        Ok(self
            .nvue_client()?
            .patch_json(endpoint, payload, timeout)
            .await?)
    }

    pub(super) async fn nvue_http_patch_with_error_body(
        &self,
        endpoint: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<()> {
        Self::nvue_client_patch_with_error_body(self.nvue_client()?, endpoint, payload, timeout)
            .await
    }

    pub(super) async fn nvue_client_patch_with_error_body(
        nvue: &SharedNvueClient,
        endpoint: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<()> {
        let response = nvue.patch(endpoint, payload, timeout).await?;

        if response.is_success() {
            return Ok(());
        }

        let message = if response.body.trim().is_empty() {
            format!("HTTP PATCH {endpoint} returned {}", response.status)
        } else {
            format!(
                "HTTP PATCH {endpoint} returned {}: {}",
                response.status, response.body
            )
        };

        Err(RmsError::new(
            rms_http_status_code(response.status),
            message,
        ))
    }

    pub(super) async fn create_revision(&self) -> Result<String> {
        let payload = RevisionCreate::default();

        let payload = serde_json::to_value(&payload).map_err(|e| {
            RmsError::internal(format!("failed to serialize revision create request: {e}"))
        })?;

        let response = self
            .nvue_http_post(REVISION_ENDPOINT, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .map_err(|e| {
                RmsError::new(e.code, format!("failed to create revision: {}", e.message))
            })?;

        let response = serde_json::from_value::<RevisionIdResponse>(response).map_err(|e| {
            RmsError::internal(format!("failed to parse revision create response: {e}"))
        })?;

        response
            .revision_id()
            .ok_or_else(|| RmsError::internal("no revision id in NVUE response"))
    }

    /// Discards a candidate revision created by RMS before apply starts.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or NVUE rejects the deletion.
    pub(super) async fn discard_candidate_revision(&self, revision_id: &str) -> Result<()> {
        let endpoint = revision_endpoint(revision_id);

        let response = self
            .nvue_client()?
            .delete(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        if response.is_success() {
            return Ok(());
        }

        let message = if response.body.trim().is_empty() {
            format!("HTTP DELETE {endpoint} returned {}", response.status)
        } else {
            format!(
                "HTTP DELETE {endpoint} returned {}: {}",
                response.status, response.body
            )
        };

        Err(RmsError::new(
            rms_http_status_code(response.status),
            message,
        ))
    }

    pub(crate) async fn nvue_start_config_revision(&self, revision_id: &str) -> Result<()> {
        let payload = RevisionUpdate::apply_with_yes_prompt();

        let payload = serde_json::to_value(&payload).map_err(|e| {
            RmsError::internal(format!("failed to serialize revision apply request: {e}"))
        })?;

        let endpoint = revision_endpoint(revision_id);

        self.nvue_http_patch_with_error_body(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .map_err(|e| {
                RmsError::new(
                    e.code,
                    format!("failed to apply revision {revision_id}: {}", e.message),
                )
            })?;
        Ok(())
    }

    pub(super) async fn wait_for_revision_applied_or_no_config_diff(
        &self,
        revision_id: &str,
        timeout: Duration,
        candidate_nvue: Option<&SharedNvueClient>,
    ) -> Result<bool> {
        if let Some(candidate_nvue) = candidate_nvue {
            return self
                .wait_for_revision_with_candidate_credentials(revision_id, timeout, candidate_nvue)
                .await;
        }

        self.wait_for_revision_with_current_credentials(revision_id, timeout)
            .await
    }

    async fn wait_for_revision_with_current_credentials(
        &self,
        revision_id: &str,
        timeout: Duration,
    ) -> Result<bool> {
        let deadline = std::time::Instant::now() + timeout;
        let mut last_retryable_error = None;
        let mut last_observed_state = None;

        tracing::info!(
            node = %self.id,
            revision = %revision_id,
            "waiting for NVUE revision apply with current credentials"
        );

        while std::time::Instant::now() < deadline {
            match self
                .poll_revision_with_current_credentials(revision_id)
                .await
            {
                Ok(response) => {
                    let decision = revision_poll_decision(&response, revision_id, None)?;
                    if let Some(no_config_diff) = self.note_revision_poll_decision(
                        revision_id,
                        decision,
                        &mut last_observed_state,
                    ) {
                        return Ok(no_config_diff);
                    }
                }
                Err(e) if is_retryable_nvue_transport_error(&e) => {
                    tracing::warn!(
                        node = %self.id,
                        revision = %revision_id,
                        error = %e.message,
                        "revision poll transient error; retrying"
                    );

                    last_retryable_error = Some(e);
                }
                Err(e) => return Err(e),
            }

            tokio::time::sleep(Duration::from_secs(config::REVISION_POLL_INTERVAL_SECONDS)).await;
        }

        Err(revision_apply_timeout(
            revision_id,
            last_observed_state,
            last_retryable_error,
        ))
    }

    async fn wait_for_revision_with_candidate_credentials(
        &self,
        revision_id: &str,
        timeout: Duration,
        candidate_nvue: &SharedNvueClient,
    ) -> Result<bool> {
        let deadline = std::time::Instant::now() + timeout;
        let mut last_retryable_error = None;
        let mut last_observed_state = None;
        let mut reported_current_credential_diagnostics = false;

        tracing::info!(
            node = %self.id,
            revision = %revision_id,
            "waiting for password revision apply with candidate credentials"
        );

        while std::time::Instant::now() < deadline {
            match Self::poll_revision_with_nvue(candidate_nvue, revision_id).await {
                Ok(response) => {
                    let decision = revision_poll_decision(
                        &response,
                        revision_id,
                        Some("candidate credentials"),
                    )?;

                    if let Some(no_config_diff) = self.note_revision_poll_decision(
                        revision_id,
                        decision,
                        &mut last_observed_state,
                    ) {
                        return Ok(no_config_diff);
                    }
                }
                Err(e) if is_candidate_revision_poll_error(&e) => {
                    tracing::debug!(
                        node = %self.id,
                        revision = %revision_id,
                        error = %e.message,
                        "revision poll with candidate credentials not ready; checking current credentials for terminal apply diagnostics"
                    );

                    last_retryable_error = Some(e);

                    if !reported_current_credential_diagnostics {
                        tracing::info!(
                            node = %self.id,
                            revision = %revision_id,
                            "candidate credentials not ready; checking current credentials for revision diagnostics"
                        );

                        reported_current_credential_diagnostics = true;
                    }

                    match self
                        .poll_current_credentials_for_candidate_diagnostics(revision_id)
                        .await?
                    {
                        CandidateRevisionDiagnostic::Continue {
                            observed_state,
                            retryable_error,
                        } => {
                            if let Some(state) = observed_state {
                                last_observed_state = Some(state);
                            }

                            if let Some(error) = retryable_error {
                                last_retryable_error = Some(error);
                            }
                        }
                    }
                }
                Err(e) => return Err(e),
            }

            tokio::time::sleep(Duration::from_secs(config::REVISION_POLL_INTERVAL_SECONDS)).await;
        }

        Err(revision_apply_timeout(
            revision_id,
            last_observed_state,
            last_retryable_error,
        ))
    }

    fn note_revision_poll_decision(
        &self,
        revision_id: &str,
        decision: RevisionPollDecision,
        last_observed_state: &mut Option<String>,
    ) -> Option<bool> {
        match decision {
            RevisionPollDecision::Complete { no_config_diff } => {
                if no_config_diff {
                    tracing::info!(
                        node = %self.id,
                        revision = %revision_id,
                        "revision apply reported no config diff"
                    );
                } else {
                    tracing::info!(node = %self.id, revision = %revision_id, "revision reached applied state");
                }

                Some(no_config_diff)
            }
            RevisionPollDecision::Pending { observed_state } => {
                if let Some(state) = observed_state {
                    *last_observed_state = Some(state);
                }

                None
            }
        }
    }

    async fn poll_current_credentials_for_candidate_diagnostics(
        &self,
        revision_id: &str,
    ) -> Result<CandidateRevisionDiagnostic> {
        match self
            .poll_revision_with_current_credentials(revision_id)
            .await
        {
            Ok(response) => {
                self.current_credential_candidate_diagnostic_from_response(&response, revision_id)
            }
            Err(e) if is_candidate_revision_poll_error(&e) => {
                // Once NVUE accepts an active-user password apply, old
                // credentials can stop working before candidate credentials are
                // accepted for revision polling. Treat old-credential 401 as
                // handoff noise only in this candidate path; current-only
                // revision polling remains fail-fast on 401.
                tracing::debug!(
                    node = %self.id,
                    revision = %revision_id,
                    error = %e.message,
                    "revision poll with current credentials also not ready during credential handoff; retrying"
                );

                Ok(CandidateRevisionDiagnostic::Continue {
                    observed_state: None,
                    retryable_error: Some(e),
                })
            }
            Err(e) => Err(e),
        }
    }

    fn current_credential_candidate_diagnostic_from_response(
        &self,
        response: &RevisionResponse,
        revision_id: &str,
    ) -> Result<CandidateRevisionDiagnostic> {
        match response.apply_status(revision_id) {
            RevisionApplyStatus::Applied => {
                // Do not accept `applied` from the old credentials for an
                // active-user password change. Rotation is complete only after
                // the new credentials can read the applied revision. Old
                // credentials are retained here to expose terminal NVUE
                // diagnostics when candidate auth is rejected.
                Ok(CandidateRevisionDiagnostic::Continue {
                    observed_state: Some("current credentials state=applied".to_owned()),
                    retryable_error: None,
                })
            }
            RevisionApplyStatus::NoConfigDiff => {
                tracing::info!(
                    node = %self.id,
                    revision = %revision_id,
                    "current credentials reported no config diff while candidate credentials are not ready; retrying candidate credentials"
                );

                Ok(CandidateRevisionDiagnostic::Continue {
                    observed_state: Some("current credentials no config diff".to_owned()),
                    retryable_error: None,
                })
            }
            RevisionApplyStatus::Failed { state, issue } => Err(revision_apply_failure(
                revision_id,
                state.as_str(),
                issue.as_str(),
            )),
            RevisionApplyStatus::Pending { state } => Ok(CandidateRevisionDiagnostic::Continue {
                observed_state: revision_pending_state(Some("current credentials"), &state),
                retryable_error: None,
            }),
        }
    }

    async fn poll_revision_with_current_credentials(
        &self,
        revision_id: &str,
    ) -> Result<RevisionResponse> {
        let endpoint = revision_endpoint(revision_id);
        let response = self
            .nvue_http_get(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        serde_json::from_value(response).map_err(|e| {
            RmsError::internal(format!(
                "failed to parse revision {revision_id} response: {e}"
            ))
        })
    }

    async fn poll_revision_with_nvue(
        nvue: &SharedNvueClient,
        revision_id: &str,
    ) -> Result<RevisionResponse> {
        let endpoint = revision_endpoint(revision_id);
        let response = nvue
            .get_json(&endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        serde_json::from_value(response).map_err(|e| {
            RmsError::internal(format!(
                "failed to parse revision {revision_id} response: {e}"
            ))
        })
    }

    /// Save the currently applied NVUE config to startup without a revision ID.
    ///
    /// Resume and no-diff paths use this when the running config is already the
    /// desired state and there may be no concrete revision left to save.
    pub(crate) async fn save_current_applied_config(&self) -> Result<()> {
        Self::save_current_applied_config_with_client(self.nvue_client()?).await
    }

    pub(super) async fn save_current_applied_config_with_client(
        nvue: &SharedNvueClient,
    ) -> Result<()> {
        // Password rotation reaches this point even after an idempotent
        // no-diff apply result. Saving `/revision/applied` persists the current
        // running config, which lets retries repair an earlier apply-succeeded
        // but save-failed attempt.
        let payload = Self::revision_save_payload()?;
        let endpoint = applied_revision_endpoint();
        Self::nvue_client_patch_with_error_body(
            nvue,
            &endpoint,
            &payload,
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub(super) async fn save_applied_revision(&self, revision_id: &str) -> Result<()> {
        let primary = self.save_current_applied_config().await;

        if primary.is_ok() {
            return Ok(());
        }

        // Some NVUE builds accept save on the concrete revision endpoint rather
        // than `/revision/applied`; keep both NVUE-only paths before failing.
        let payload = Self::revision_save_payload()?;
        let endpoint = revision_endpoint(revision_id);
        let fallback = self
            .nvue_http_patch_with_error_body(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await;

        match (primary, fallback) {
            (Err(primary_err), Err(fallback_err)) => {
                let code = match (primary_err.code, fallback_err.code) {
                    (ErrorCode::Unauthenticated, _) | (_, ErrorCode::Unauthenticated) => {
                        ErrorCode::Unauthenticated
                    }
                    (ErrorCode::NotFound, fallback_code) => fallback_code,
                    (primary_code, _) => primary_code,
                };

                Err(RmsError::new(
                    code,
                    format!(
                        "failed to save configuration after apply: /revision/applied: {}; /revision/{revision_id}: {}",
                        primary_err.message, fallback_err.message
                    ),
                ))
            }
            _ => Ok(()),
        }
    }

    fn revision_save_payload() -> Result<Value> {
        serde_json::to_value(RevisionUpdate::save_with_yes_prompt()).map_err(|e| {
            RmsError::internal(format!("failed to serialize revision save request: {e}"))
        })
    }

    pub(crate) async fn wait_for_nvue_action_completion(&self, action_id: &str) -> Result<()> {
        let timeout = Duration::from_secs(config::REVISION_APPLY_TIMEOUT_SECONDS);
        let deadline = std::time::Instant::now() + timeout;
        let mut last_retryable_error = None;

        while std::time::Instant::now() < deadline {
            // The action POST already succeeded, so retry only this read-only
            // status poll when NVUE briefly drops its HTTPS connection.
            match self.poll_nvue_action_task(action_id).await {
                Ok(status) if status.completed => {
                    if status.state == "action_success" || status.percent == 100 {
                        return Ok(());
                    }
                    let message = if status.message.is_empty() {
                        format!("NVUE action {action_id} failed with state {}", status.state)
                    } else {
                        status.message
                    };
                    return Err(RmsError::internal(message));
                }
                Ok(_) => {}
                Err(error) if is_retryable_nvue_transport_error(&error) => {
                    tracing::warn!(
                        node = %self.id,
                        action = %action_id,
                        error = %error.message,
                        "NVUE action poll transient error; retrying"
                    );
                    last_retryable_error = Some(error);
                }
                Err(error) => return Err(error),
            }

            tokio::time::sleep(Duration::from_secs(config::REVISION_POLL_INTERVAL_SECONDS)).await;
        }

        let mut message = format!("timed out waiting for NVUE action {action_id} to complete");
        if let Some(error) = last_retryable_error {
            message.push_str("; last retryable error: ");
            message.push_str(&error.message);
        }

        Err(RmsError::timeout(message))
    }

    pub(crate) async fn nvue_start_action(
        &self,
        endpoint: &str,
        action: &str,
        parameters: Value,
    ) -> Result<()> {
        let payload = serde_json::json!({
            action: {
                "state": "start",
                "parameters": parameters,
            }
        });

        self.nvue_run_action_payload(endpoint, action, payload)
            .await
    }

    pub(super) async fn nvue_run_action_payload(
        &self,
        endpoint: &str,
        action_name: &str,
        payload: Value,
    ) -> Result<()> {
        let response = self
            .nvue_http_post(endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .map_err(|e| {
                RmsError::internal(format!(
                    "NVUE POST {endpoint} {action_name} failed: {}",
                    e.message
                ))
            })?;

        if let Ok(job_id) = extract_job_id(&response) {
            self.wait_for_nvue_action_completion(&job_id).await?;
        }

        Ok(())
    }

    pub(crate) async fn nvue_apply_config_patches(&self, patches: &[(&str, Value)]) -> Result<()> {
        if patches.is_empty() {
            return Ok(());
        }

        let revision_id = self.nvue_stage_config_patches(patches).await?;
        self.nvue_start_config_revision(&revision_id).await?;
        self.nvue_finish_config_revision(&revision_id).await
    }

    pub(crate) async fn nvue_stage_config_patches(
        &self,
        patches: &[(&str, Value)],
    ) -> Result<String> {
        if patches.is_empty() {
            return Err(RmsError::invalid_argument(
                "at least one NVUE configuration patch is required",
            ));
        }

        let revision_id = self.create_revision().await?;
        for (path, payload) in patches {
            let endpoint = format!("{path}?rev={revision_id}");
            self.nvue_http_patch(&endpoint, payload, HttpClient::DEFAULT_TIMEOUT)
                .await
                .map_err(|e| {
                    RmsError::internal(format!("NVUE PATCH {endpoint} failed: {}", e.message))
                })?;
        }

        Ok(revision_id)
    }

    pub(crate) async fn nvue_finish_config_revision(&self, revision_id: &str) -> Result<()> {
        let no_config_diff = self
            .wait_for_revision_applied_or_no_config_diff(
                revision_id,
                Duration::from_secs(config::REVISION_APPLY_TIMEOUT_SECONDS),
                None,
            )
            .await?;
        if !no_config_diff {
            self.save_applied_revision(revision_id).await?;
        }
        Ok(())
    }

    async fn nvue_get_system_hello(&self) -> Result<()> {
        self.nvue_http_get("/nvue_v1/system", NVUE_DEFAULT_TIMEOUT)
            .await
            .map(|_| ())
    }

    pub(crate) async fn verify_nvue_api_hello(&self) -> Result<()> {
        self.nvue_get_system_hello().await
    }

    /// True when a short NVUE connectivity retry may succeed.
    pub(crate) fn is_nvue_retryable_error(err: &RmsError) -> bool {
        if matches!(
            err.code,
            ErrorCode::Timeout
                | ErrorCode::Unavailable
                | ErrorCode::ConnectionRefused
                | ErrorCode::DnsResolutionFailed
                | ErrorCode::Unauthenticated
        ) {
            return true;
        }

        if err.code != ErrorCode::Internal {
            return false;
        }

        let message = err.message.to_ascii_lowercase();

        [
            "tls",
            "certificate",
            "connection",
            "connect",
            "http 401",
            "http 403",
            "returned 401",
            "returned 403",
        ]
        .iter()
        .any(|needle| message.contains(needle))
    }

    pub async fn get_chassis_location_info(&self) -> Result<Value> {
        let resp = self
            .nvue_http_get(
                "/nvue_v1/platform/chassis-location",
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await?;

        Ok(serde_json::json!({
            "switch_info": {
                "chassis_sn": resp.get("chassis-sn").and_then(|v| v.as_str()).unwrap_or_default(),
                "slot_number": resp.get("slot-number").and_then(|v| v.as_str()).unwrap_or_default(),
                "topology_id": resp.get("topology-id").and_then(|v| v.as_str()).unwrap_or_default(),
                "tray_index": resp.get("tray-index").and_then(|v| v.as_str()).unwrap_or_default(),
            }
        }))
    }
}
