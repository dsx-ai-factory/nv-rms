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

//! Password rotation and post-boot password recovery workflows for NVIDIA GB200 switches.

use std::time::Duration;

use super::validation::is_valid_system_username;
use super::{SwitchGb200Nvidia, config};
use crate::domain::rack::EndpointCredentials;
use crate::transport::http_client::HttpClient;
use crate::transport::ssh_client::{SshClient, SshEndpoint};
use crate::utilities::error::{ErrorCode, Result, RmsError};

use nvue_client::system::{SYSTEM_ENDPOINT, UserPasswordPatch, user_endpoint_for_revision};
use nvue_client::{ClientCredentials as NvueCredentials, SharedClient as SharedNvueClient};
use secrecy::{ExposeSecret, SecretString};

const EXPIRED_PASSWORD_CHANGE_TIMEOUT: Duration = Duration::from_secs(90);
const RECOVERED_PASSWORD_VALIDATION_TIMEOUT: Duration = Duration::from_secs(60);
const RECOVERED_PASSWORD_VALIDATION_INTERVAL: Duration = Duration::from_secs(5);

/// Result of a resumable switch password rotation.
///
/// The RPC worker serializes this as a secret-free phase marker so callers can
/// distinguish a newly applied revision from a resumed partial success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SwitchSystemPasswordUpdateOutcome {
    /// Current credentials authenticated and RMS ran the normal update flow.
    ///
    /// `revision_id` is absent only when the requested password already
    /// authenticated and NVUE reported no configuration diff.
    Updated { revision_id: Option<String> },

    /// RMS resumed an interrupted credential transition and saved the running
    /// config.
    Resumed,

    /// RMS replaced the factory-default `admin` password through the mandatory
    /// SSH prompt and validated the requested password through SSH.
    RecoveredFactoryDefault,
}

impl SwitchSystemPasswordUpdateOutcome {
    pub(crate) fn phase(&self) -> &'static str {
        match self {
            Self::Updated { .. } => "password_update_persisted",
            Self::Resumed => "applied_config_persisted",
            Self::RecoveredFactoryDefault => "factory_default_admin_password_recovered",
        }
    }

    pub(crate) fn revision_id(&self) -> Option<&str> {
        match self {
            Self::Updated { revision_id } => revision_id.as_deref(),
            Self::Resumed | Self::RecoveredFactoryDefault => None,
        }
    }
}

// ── Password update helpers ─────────────────────────────────────────

fn redact_password_update_error(
    message: &str,
    new_password: &str,
    encoded_password: &str,
) -> String {
    // NVUE policy errors can echo the rejected password value. Scrub both the
    // caller's plaintext and the base64 wire value before reporting upstream.
    let mut redacted = message.to_owned();

    for secret in [new_password, encoded_password] {
        if !secret.is_empty() {
            redacted = redacted.replace(secret, "XXXX");
        }
    }

    common::redaction::redact_secret_fields(&redacted)
}

fn redact_password_update_rms_error(
    err: RmsError,
    new_password: &str,
    encoded_password: &str,
) -> RmsError {
    RmsError::new(
        err.code,
        redact_password_update_error(&err.message, new_password, encoded_password),
    )
}

impl SwitchGb200Nvidia {
    // ── Switch System user security operations ──────────────────────────────

    /// Update an NVOS user's password and persist it across reboots.
    ///
    /// Flow:
    /// 1. Verify current credentials and return when the active user already
    ///    has the requested password.
    /// 2. Create an NVUE revision.
    /// 3. Stage the password change on that revision.
    /// 4. Apply the revision.
    /// 5. For active-user rotation, poll with candidate credentials while
    ///    retaining old credentials for terminal diagnostics; temporary 401s
    ///    are retryable only in this candidate-credential handoff path.
    /// 6. Treat NVUE no-config-diff as apply success for idempotency.
    /// 7. Commit in-memory credentials after candidate credentials prove the
    ///    requested active-user password is usable.
    /// 8. Save the applied NVUE config, including after no-config-diff, so
    ///    retries can complete reboot persistence after an earlier save failure.
    pub async fn update_system_user_password_persisted(
        &mut self,
        user_id: &str,
        new_password: &str,
    ) -> Result<()> {
        self.update_system_user_password_persisted_with_result(user_id, new_password)
            .await
            .map(|_| ())
    }

    async fn update_system_user_password_persisted_with_result(
        &mut self,
        user_id: &str,
        new_password: &str,
    ) -> Result<Option<String>> {
        if user_id.is_empty() {
            return Err(RmsError::invalid_argument("username must not be empty"));
        }

        if new_password.is_empty() {
            return Err(RmsError::invalid_argument("password must not be empty"));
        }

        let (active_auth_username, requested_password_matches_active_credentials) = {
            let (current_username, current_password) = self.require_host_credentials()?;
            let matches_active_user = user_id == current_username;
            (
                matches_active_user.then(|| current_username.to_owned()),
                matches_active_user && current_password.expose_secret() == new_password,
            )
        };

        if requested_password_matches_active_credentials {
            // A no-op must still prove that the configured endpoint credentials
            // pass NVUE authentication. Some NVUE builds can return other
            // non-2xx statuses from `/system`; stale credentials are the 401
            // case we must reject before reporting success.
            self.verify_current_nvue_credentials().await?;

            tracing::info!(
                node = %self.id,
                user = %user_id,
                "switch password rotation skipped because requested password already authenticates"
            );

            return Ok(None);
        }

        let revision_id = self.create_revision().await?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            revision = %revision_id,
            "created NVUE revision for switch password rotation"
        );

        let encoded_password = self
            .patch_system_user_password(&revision_id, user_id, new_password)
            .await?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            revision = %revision_id,
            "staged switch password update in NVUE revision"
        );

        self.nvue_start_config_revision(&revision_id)
            .await
            .map_err(|e| redact_password_update_rms_error(e, new_password, &encoded_password))?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            revision = %revision_id,
            "started NVUE revision apply for switch password rotation"
        );

        // Active-user rotation has a short ambiguity window: a successful apply
        // may require the new password, but an invalid or failed apply may
        // still only be visible with the old password. Use a candidate client
        // for polling, and keep the main client on old credentials until the
        // revision is confirmed applied or NVUE reports an idempotent no-diff.
        let candidate_nvue = active_auth_username
            .as_deref()
            .map(|username| self.build_candidate_nvue_client(username, new_password))
            .transpose()?;

        let outcome = self
            .wait_for_revision_applied_or_no_config_diff(
                &revision_id,
                Duration::from_secs(config::REVISION_APPLY_TIMEOUT_SECONDS),
                candidate_nvue.as_ref(),
            )
            .await
            .map_err(|e| redact_password_update_rms_error(e, new_password, &encoded_password))?;

        if !outcome.needs_save() {
            // Rotation still saves below: the save doubles as the repair path
            // for an earlier attempt that applied but failed to save.
            tracing::info!(
                node = %self.id,
                user = %user_id,
                revision = %revision_id,
                ?outcome,
                "switch password revision needed no save from apply; continuing to save applied config"
            );
        }

        if active_auth_username.is_some() {
            // Candidate polling only completes after the requested password can
            // read the revision. Refresh before save because active-user save
            // must use the new credentials even when apply reports no diff.
            tracing::info!(
                node = %self.id,
                user = %user_id,
                revision = %revision_id,
                "refreshing RMS NVUE credentials after active-user password rotation"
            );

            self.refresh_http_credentials(new_password).await?;
        }

        self.save_applied_revision(&revision_id)
            .await
            .map_err(|e| redact_password_update_rms_error(e, new_password, &encoded_password))?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            revision = %revision_id,
            "saved applied switch password revision for reboot persistence"
        );

        Ok(Some(revision_id))
    }

    /// Update an NVOS user's password or resume an earlier active-user rotation
    /// that was applied but not saved.
    ///
    /// Endpoint credentials drive normal password updates. When the target and
    /// endpoint users match, the requested password can authenticate a resume
    /// attempt. After authentication succeeds, RMS saves the applied config and
    /// refreshes the endpoint credentials.
    ///
    /// `candidate_nvue` lets the RPC worker provide an initialized TLS client
    /// after endpoint authentication fails. When it is `None`, this method
    /// builds an equivalent client from the current NVUE configuration after
    /// the same failure. A supplied candidate is ignored when the target and
    /// endpoint users differ.
    pub(crate) async fn update_system_user_password_persisted_resumable(
        &mut self,
        user_id: &str,
        password: &str,
        candidate_nvue: Option<SharedNvueClient>,
    ) -> Result<SwitchSystemPasswordUpdateOutcome> {
        if user_id.is_empty() {
            return Err(RmsError::invalid_argument("username must not be empty"));
        }

        if password.is_empty() {
            return Err(RmsError::invalid_argument("password must not be empty"));
        }

        let (target_is_endpoint_user, requested_password_matches_endpoint) = {
            let (username, current_password) = self.require_host_credentials()?;

            (
                username == user_id,
                username == user_id && current_password.expose_secret() == password,
            )
        };

        if let Some(candidate_nvue) = candidate_nvue
            && target_is_endpoint_user
        {
            // Probe the supplied candidate before touching the original
            // client. In the mTLS resume flow, endpoint-credential rejection
            // can leave the original transport unable to make another request.
            return self
                .resume_system_user_password_with_candidate(user_id, password, &candidate_nvue)
                .await;
        }

        match self.verify_current_nvue_credentials().await {
            Ok(()) if !requested_password_matches_endpoint => {
                let revision_id = self
                    .update_system_user_password_persisted_with_result(user_id, password)
                    .await?;

                return Ok(SwitchSystemPasswordUpdateOutcome::Updated { revision_id });
            }
            Ok(()) => {
                // Requested credentials already work. Save below so a retry can
                // repair an earlier apply-succeeded but save-failed attempt.
            }
            Err(e) if e.code == ErrorCode::Unauthenticated && target_is_endpoint_user => {
                // A 401 for endpoint credentials can mean the prior attempt
                // already changed the active endpoint user's password before
                // RMS failed to save the applied NVUE config. Probe the requested
                // password only to confirm that resume state; this branch sends
                // no password patch or revision apply.
                let candidate_nvue = self.build_candidate_nvue_client(user_id, password)?;
                return self
                    .resume_system_user_password_with_candidate(user_id, password, &candidate_nvue)
                    .await;
            }
            Err(e) => return Err(e),
        }

        self.save_current_applied_config()
            .await
            .map_err(|e| redact_password_update_rms_error(e, password, ""))?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            "saved applied config after confirming requested switch credentials"
        );

        Ok(SwitchSystemPasswordUpdateOutcome::Resumed)
    }

    async fn resume_system_user_password_with_candidate(
        &mut self,
        user_id: &str,
        password: &str,
        candidate_nvue: &SharedNvueClient,
    ) -> Result<SwitchSystemPasswordUpdateOutcome> {
        let status = Self::probe_nvue_credentials(
            candidate_nvue,
            "requested switch credentials before resuming password rotation",
        )
        .await?;

        if !(200..300).contains(&status) {
            if status == 401 && user_id == "admin" && self.require_host_credentials()?.0 == "admin"
            {
                self.recover_admin_password_after_boot(password).await?;

                return Ok(SwitchSystemPasswordUpdateOutcome::RecoveredFactoryDefault);
            }

            let code = match status {
                401 => ErrorCode::Unauthenticated,
                408 => ErrorCode::Timeout,
                503 => ErrorCode::Unavailable,
                _ => {
                    tracing::warn!(
                        node = %self.id,
                        status,
                        "requested switch credential probe returned unexpected HTTP status"
                    );

                    ErrorCode::Internal
                }
            };

            let message = if status == 401 {
                format!(
                    "neither endpoint nor requested switch credentials authenticated for password rotation: HTTP GET {SYSTEM_ENDPOINT} returned 401"
                )
            } else {
                format!(
                    "failed to verify requested switch credentials before resuming password rotation: HTTP GET {SYSTEM_ENDPOINT} returned {status}"
                )
            };

            return Err(RmsError::new(code, message));
        }

        Self::save_current_applied_config_with_client(candidate_nvue)
            .await
            .map_err(|e| redact_password_update_rms_error(e, password, ""))?;

        self.refresh_http_credentials(password).await?;

        tracing::info!(
            node = %self.id,
            user = %user_id,
            "saved applied config after confirming requested switch credentials"
        );

        Ok(SwitchSystemPasswordUpdateOutcome::Resumed)
    }

    async fn verify_current_nvue_credentials(&self) -> Result<()> {
        let status = Self::probe_nvue_credentials(
            self.nvue_client()?,
            "current switch credentials before skipping password rotation",
        )
        .await?;

        if status == 401 {
            return Err(RmsError::unauthenticated(format!(
                "failed to verify current switch credentials before skipping password rotation: HTTP GET {SYSTEM_ENDPOINT} returned 401"
            )));
        }

        Ok(())
    }

    async fn probe_nvue_credentials(client: &SharedNvueClient, context: &str) -> Result<u16> {
        match client
            .probe(SYSTEM_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
            .await
        {
            Ok(status) => Ok(status),
            Err(e) => {
                let e = RmsError::from(e);

                Err(RmsError::new(
                    e.code,
                    format!("failed to verify {context}: {}", e.message),
                ))
            }
        }
    }

    async fn patch_system_user_password(
        &self,
        revision_id: &str,
        user_id: &str,
        new_password: &str,
    ) -> Result<String> {
        if !is_valid_system_username(user_id) {
            return Err(RmsError::invalid_argument(format!(
                "invalid user_id '{user_id}': only ASCII letters and digits are allowed"
            )));
        }

        let payload = UserPasswordPatch::new(new_password);
        let encoded_password = payload.password.clone();

        let payload = serde_json::to_value(&payload).map_err(|e| {
            RmsError::internal(format!("failed to serialize password update for NVUE: {e}"))
        })?;

        let endpoint = user_endpoint_for_revision(user_id, revision_id);

        self.nvue_http_patch_with_error_body(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .map_err(|e| {
                let message =
                    redact_password_update_error(&e.message, new_password, &encoded_password);

                RmsError::new(
                    e.code,
                    format!("failed to stage password update for user '{user_id}': {message}"),
                )
            })?;

        Ok(encoded_password)
    }

    /// Build an alternate-credential NVUE client for resumable rotation.
    pub(crate) fn build_candidate_nvue_client(
        &self,
        username: &str,
        password: &str,
    ) -> Result<SharedNvueClient> {
        Ok(self
            .nvue_client()?
            .clone_with_credentials(NvueCredentials::new(username, password)))
    }

    /// Selects credentials for physical admission without changing switch state.
    ///
    /// Rejected endpoint credentials permit a requested-password probe only for
    /// the same user. A returned alternate client is used for identity discovery
    /// and passed to the resumable update; persistence still follows admission.
    pub(crate) async fn authenticate_system_user_password_update(
        &self,
        user_id: &str,
        password: &str,
    ) -> Result<Option<SharedNvueClient>> {
        let Err(error) = self.verify_current_nvue_credentials().await else {
            return Ok(None);
        };

        if error.code != ErrorCode::Unauthenticated {
            return Err(error);
        }

        let (endpoint_username, _) = self.require_host_credentials()?;

        if endpoint_username != user_id {
            return Err(error);
        }

        let candidate = self.build_candidate_nvue_client(user_id, password)?;

        let status = Self::probe_nvue_credentials(
            &candidate,
            "requested switch credentials before mutation admission",
        )
        .await?;

        if status == 401 {
            return Err(RmsError::unauthenticated(
                "neither endpoint nor requested switch credentials authenticated for password rotation",
            ));
        }

        if !(200..300).contains(&status) {
            let code = match status {
                408 => ErrorCode::Timeout,
                503 => ErrorCode::Unavailable,
                _ => ErrorCode::Internal,
            };

            return Err(RmsError::new(
                code,
                format!(
                    "requested switch credential probe returned HTTP {status} before mutation admission"
                ),
            ));
        }

        Ok(Some(candidate))
    }

    async fn refresh_http_credentials(&mut self, new_password: &str) -> Result<()> {
        let username = self.require_host_credentials()?.0.to_owned();

        // Update the NVUE client first: if this fails, return before mutating
        // in-memory credentials so they never diverge from what the client uses.
        self.nvue_client()?
            .update_credentials(NvueCredentials::new(&username, new_password))
            .await?;

        let Some(EndpointCredentials::UserPass { password, .. }) = self
            .host_endpoint
            .as_mut()
            .and_then(|endpoint| endpoint.credentials.as_mut())
        else {
            return Err(RmsError::failed_precondition(
                "switch host credentials are required for NVUE/NVOS operations",
            ));
        };

        *password = SecretString::from(new_password);

        tracing::debug!(
            node = %self.id,
            user = %username,
            "refreshed in-memory NVUE credentials after password rotation"
        );

        Ok(())
    }

    /// Recover the `admin` password after a reboot that put the switch into
    /// the forced expired-password change flow. SSHes in with the default
    /// `admin`/`admin` credentials, completes the expired-password change
    /// with `new_password`, then validates a fresh SSH login with the new
    /// credentials and refreshes this Switch's internal HTTP client and host
    /// endpoint credentials.
    ///
    /// Precondition: this Switch's host endpoint user must be `admin`.
    pub async fn recover_admin_password_after_boot(&mut self, new_password: &str) -> Result<()> {
        let host_username = self.require_host_credentials()?.0.to_owned();

        if host_username != "admin" {
            return Err(RmsError::failed_precondition(
                "post-boot admin password recovery requires the switch user to be admin",
            ));
        }

        if new_password.is_empty() {
            return Err(RmsError::invalid_argument("new password must not be empty"));
        }

        #[cfg(test)]
        if let Some(recover) = &self.ssh_password_recovery_for_test {
            recover("admin", new_password)?;
            self.refresh_http_credentials(new_password).await?;

            return Ok(());
        }

        let host_ip_address = self.require_host_endpoint()?.ip_address.clone();

        tracing::info!(node = %self.id, "attempting expired admin recovery over SSH");

        let recovery = SshClient::connect(
            SshEndpoint::new(&host_ip_address, "admin", "admin"),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await?;

        tracing::info!(node = %self.id, "connected with default admin credentials");

        recovery
            .complete_expired_password_change(new_password, EXPIRED_PASSWORD_CHANGE_TIMEOUT)
            .await?;

        tracing::info!(node = %self.id, "completed SSH password change; validating restored login");

        let deadline = std::time::Instant::now() + RECOVERED_PASSWORD_VALIDATION_TIMEOUT;

        loop {
            let err = match SshClient::connect(
                SshEndpoint::new(&host_ip_address, &host_username, new_password),
                RECOVERED_PASSWORD_VALIDATION_INTERVAL,
            )
            .await
            {
                Ok(_) => {
                    self.refresh_http_credentials(new_password).await?;
                    tracing::info!(node = %self.id, "refreshed live credentials after SSH recovery");
                    return Ok(());
                }
                Err(e) => {
                    tracing::info!(
                        node = %self.id,
                        error = %e.message,
                        "restored SSH validation attempt failed"
                    );

                    e
                }
            };

            if std::time::Instant::now() >= deadline {
                return Err(RmsError::internal(format!(
                    "SSH password recovery completed but restored SSH login validation failed: {}",
                    err.message
                )));
            }

            tokio::time::sleep(RECOVERED_PASSWORD_VALIDATION_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use wiremock::matchers::{basic_auth, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn password_admission_probes_are_read_only_and_restrict_candidate_credentials() {
        for (target_user, candidate_status, expected_code) in [
            ("operator", 200, None),
            ("operator", 401, Some(ErrorCode::Unauthenticated)),
            ("operator", 408, Some(ErrorCode::Timeout)),
            ("operator", 503, Some(ErrorCode::Unavailable)),
            ("operator", 418, Some(ErrorCode::Internal)),
            ("other-user", 200, Some(ErrorCode::Unauthenticated)),
        ] {
            let nvue = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/system"))
                .and(basic_auth("operator", "current-password"))
                .respond_with(ResponseTemplate::new(401))
                .expect(1)
                .mount(&nvue)
                .await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/system"))
                .and(basic_auth(target_user, "next-password"))
                .respond_with(ResponseTemplate::new(candidate_status))
                .expect(u64::from(target_user == "operator"))
                .mount(&nvue)
                .await;

            let switch = SwitchGb200Nvidia::for_test(&nvue.uri())
                .with_host_credentials_for_test("operator", "current-password");

            let result = switch
                .authenticate_system_user_password_update(target_user, "next-password")
                .await;

            if let Some(code) = expected_code {
                assert_eq!(result.err().unwrap().code, code);
            } else {
                assert!(result.unwrap().is_some());
            }

            let requests = nvue.received_requests().await.unwrap();

            assert!(
                requests
                    .iter()
                    .all(|request| request.method == wiremock::http::Method::Get)
            );

            assert_eq!(
                switch.require_host_credentials().unwrap().1.expose_secret(),
                "current-password"
            );

            nvue.verify().await;
        }
    }

    #[tokio::test]
    async fn refresh_http_credentials_does_not_mutate_password_when_nvue_client_missing() {
        let mut switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:1");
        switch.nvue = None;

        let result = switch.refresh_http_credentials("new-password").await;

        assert!(
            result.is_err(),
            "refresh should fail before touching in-memory credentials"
        );

        let password = switch
            .host_endpoint
            .as_ref()
            .and_then(|endpoint| endpoint.credentials.as_ref())
            .and_then(EndpointCredentials::user_pass)
            .map(|(_, password)| password.to_owned())
            .expect("host endpoint credentials should still be present");

        assert_eq!(
            password, "test-password",
            "in-memory credentials must not change when the NVUE update never ran"
        );
    }
}
