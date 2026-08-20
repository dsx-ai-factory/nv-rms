/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 */

//! Full NVOS factory-default reset submission and readiness detection.

use std::time::Duration;

use nvue_client::system::{SYSTEM_FACTORY_DEFAULT_ENDPOINT, SystemFactoryDefaultResetRequest};
use nvue_client::{DEFAULT_TIMEOUT, SharedClient};

use super::{SwitchGb200Nvidia, config};
use crate::transport::ssh_client::{SshClient, SshEndpoint};
use crate::utilities::error::{Result, RmsError};

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
    /// Success means NVUE accepted the action; it does not mean the reset or
    /// reboot completed. [`Self::wait_for_factory_default`] provides that
    /// completion condition.
    pub(crate) async fn submit_factory_default_reset(&self, nvue: &SharedClient) -> Result<()> {
        nvue.post_json(
            SYSTEM_FACTORY_DEFAULT_ENDPOINT,
            &SystemFactoryDefaultResetRequest::start(),
            DEFAULT_TIMEOUT,
        )
        .await
        .map_err(RmsError::from)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

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
            .submit_factory_default_reset(switch.nvue_client().unwrap())
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
            .submit_factory_default_reset(switch.nvue_client().unwrap())
            .await
            .unwrap_err();

        assert_eq!(
            error.code,
            crate::utilities::error::ErrorCode::Unauthenticated
        );
    }
}
