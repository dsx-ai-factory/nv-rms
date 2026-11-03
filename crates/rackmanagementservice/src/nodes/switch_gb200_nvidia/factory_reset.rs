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

//! Full NVOS factory-default reset submission and readiness detection.

use std::time::Duration;

use super::{SwitchGb200Nvidia, config};
use crate::transport::ssh_client::{SshClient, SshEndpoint};
use crate::utilities::error::{Result, RmsError};

use nvswitch_cli::{NvCommand, SwitchCommand};
use nvue_client::system::{SYSTEM_FACTORY_DEFAULT_ENDPOINT, SystemFactoryDefaultResetRequest};
use nvue_client::{ClientError, DEFAULT_TIMEOUT, PreparedClientTls, SharedClient};

/// Whether a failed reset can safely use a different submission transport.
#[derive(Debug)]
pub(crate) enum FactoryResetError {
    /// NVUE connection or TLS setup failed before the reset could be sent.
    Connection(RmsError),

    /// Authentication, application, or request validation rejected the reset.
    Rejected(RmsError),

    /// The request may have reached NVUE; completion must be checked first.
    Ambiguous(RmsError),
}

impl SwitchGb200Nvidia {
    /// Builds an NVUE client with the factory-default credentials.
    ///
    /// This client is used only after configured credentials are rejected. It
    /// lets a retried batch converge when an earlier attempt already reset the
    /// switch and left RMS with stale credentials.
    pub(crate) fn default_nvue_client(&self) -> Result<SharedClient> {
        self.build_candidate_nvue_client("admin", "admin")
    }

    /// Probes whether the switch accepts factory-default SSH credentials.
    ///
    /// Connection and authentication failures mean the default login is not
    /// currently ready. A successful connection is closed without opening a
    /// shell, so RMS does not answer the first-login password prompt.
    pub(crate) async fn probe_factory_default_login(&self) -> Result<bool> {
        #[cfg(test)]
        if let Some(probe) = &self.factory_default_probe_for_test {
            return probe();
        }

        let host = &self.require_host_endpoint()?.ip_address;

        match SshClient::connect(
            SshEndpoint::new(host.as_str(), "admin", "admin"),
            SshClient::DEFAULT_TIMEOUT,
        )
        .await
        {
            Ok(client) => {
                // Authentication proves readiness; a disconnect error cannot
                // make the switch offline again.
                let _ = client.close().await;
                Ok(true)
            }
            Err(error) => {
                tracing::debug!(
                    node = %self.id,
                    host = %host,
                    error_code = ?error.code,
                    error = %error.message,
                    "factory-reset SSH readiness probe failed"
                );

                Ok(false)
            }
        }
    }

    /// Waits for the reset switch to accept factory-default SSH credentials.
    ///
    /// Successful authentication is the readiness signal. RMS closes the
    /// connection without opening a shell, so it neither answers the forced
    /// first-login password prompt nor replaces the default password. Transient
    /// connection and authentication failures are retried until the job timeout.
    pub(crate) async fn wait_for_factory_default(&self) -> Result<()> {
        let host = self.require_host_endpoint()?.ip_address.clone();
        let poll_interval = Duration::from_secs(config::DEFAULT_POLL_INTERVAL_SECONDS);

        let wait = async {
            tokio::time::sleep(poll_interval).await;

            loop {
                if self.probe_factory_default_login().await? {
                    return Ok(());
                }

                tokio::time::sleep(poll_interval).await;
            }
        };

        tokio::time::timeout(
            Duration::from_secs(config::DEFAULT_JOB_TIMEOUT_SECONDS),
            wait,
        )
        .await
        .map_err(|_| {
            RmsError::timeout(format!(
                "timed out waiting for switch SSH connectivity at {host}"
            ))
        })?
    }

    /// Submits a forced full NVOS reset through the typed NVUE action endpoint.
    ///
    /// A prepared TLS transport is probed before submission. Setup failures
    /// permit an authorized connection fallback; uncertain action responses
    /// require completion polling without another reset.
    /// Success means NVUE accepted the action; it does not mean the reset or
    /// reboot completed. [`Self::wait_for_factory_default`] provides that
    /// completion condition.
    pub(crate) async fn submit_factory_default_reset(
        &self,
        nvue: &SharedClient,
        prepared: Option<&PreparedClientTls>,
    ) -> std::result::Result<(), FactoryResetError> {
        if let Some(prepared) = prepared {
            // This read-only probe cannot submit a reset. Transport failures
            // are safe to fall back from; HTTP failures remain rejections.
            nvue.activate_prepared_client_tls(prepared, "/nvue_v1/system", DEFAULT_TIMEOUT)
                .await
                .map_err(|error| {
                    if matches!(error, ClientError::Request { .. }) {
                        FactoryResetError::Connection(error.into())
                    } else {
                        FactoryResetError::Rejected(error.into())
                    }
                })?;
        }

        nvue.post_json(
            SYSTEM_FACTORY_DEFAULT_ENDPOINT,
            &SystemFactoryDefaultResetRequest::start(),
            DEFAULT_TIMEOUT,
        )
        .await
        .map_err(|error| {
            let connection_failed = matches!(
                &error,
                ClientError::Request { source, .. } if source.is_connect()
            ) || error.is_server_certificate_validation_error();

            if connection_failed {
                return FactoryResetError::Connection(error.into());
            }

            if matches!(
                error,
                ClientError::Request { .. }
                    | ClientError::InvalidJson { .. }
                    | ClientError::ResponseTooLarge { .. }
            ) {
                return FactoryResetError::Ambiguous(error.into());
            }

            FactoryResetError::Rejected(error.into())
        })?;

        Ok(())
    }

    /// Submit a full reset over SSH using configured `admin` credentials.
    ///
    /// The command uses `force` to suppress confirmation. A session
    /// closing during reboot is followed by the usual default-login polling;
    /// it does not authorize another reset or a first-login password change.
    pub(crate) async fn submit_factory_default_reset_ssh(&self) -> Result<()> {
        let (username, _) = self.require_host_credentials()?;

        if username != "admin" {
            return Err(RmsError::failed_precondition(
                "NVOS factory reset over SSH requires the admin account",
            ));
        }

        let cli = self.nv_cli().await?;

        cli.run(SwitchCommand::Nv(NvCommand::factory_reset()))
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn ssh_factory_reset_rejects_non_admin_accounts_before_submission() {
        for username in ["root", "operator"] {
            let mut switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:1")
                .with_host_credentials_for_test(username, "password");

            switch.ssh_exec_for_test = Some(std::sync::Arc::new(|_| {
                panic!("non-admin reset must not submit an SSH command");
            }));

            let error = switch
                .submit_factory_default_reset_ssh()
                .await
                .expect_err("non-admin reset must fail");

            assert_eq!(
                error.code,
                crate::utilities::error::ErrorCode::FailedPrecondition
            );

            assert_eq!(
                error.message,
                "NVOS factory reset over SSH requires the admin account"
            );
        }
    }

    #[tokio::test]
    async fn reset_submission_uses_the_typed_nvue_request() {
        let server = MockServer::start().await;

        let body = serde_json::json!({
            "@reset": {
                "state": "start",
                "parameters": { "force": true }
            }
        });

        Mock::given(method("POST"))
            .and(path(SYSTEM_FACTORY_DEFAULT_ENDPOINT))
            .and(header(
                "authorization",
                "Basic dGVzdC11c2VybmFtZTp0ZXN0LXBhc3N3b3Jk",
            ))
            .and(body_json(&body))
            .respond_with(ResponseTemplate::new(201).set_body_json("reset-1"))
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        switch
            .submit_factory_default_reset(switch.nvue_client().unwrap(), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reset_submission_reports_authentication_failure() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(SYSTEM_FACTORY_DEFAULT_ENDPOINT))
            .and(header(
                "authorization",
                "Basic dGVzdC11c2VybmFtZTp0ZXN0LXBhc3N3b3Jk",
            ))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let error = switch
            .submit_factory_default_reset(switch.nvue_client().unwrap(), None)
            .await
            .unwrap_err();

        let FactoryResetError::Rejected(error) = error else {
            panic!("authentication rejection must not authorize SSH fallback");
        };

        assert_eq!(
            error.code,
            crate::utilities::error::ErrorCode::Unauthenticated
        );
    }
}
