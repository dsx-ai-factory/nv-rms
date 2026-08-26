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

//! Connection lifecycle and authentication for the SSH transport.

use std::sync::Arc;
use std::time::Duration;

use russh::client::AuthResult;
use russh::keys::PublicKeyOrCertificate;
use russh::{Disconnect, client};
use secrecy::{ExposeSecret, SecretString};

use super::error::SshError;
use crate::utilities::error::{Result, RmsError};

/// Fixed reusable buffer size for firmware-sized SFTP uploads.
pub const SFTP_UPLOAD_BUFFER_SIZE_BYTES: usize = 512 * 1024;

/// Minimal SSH client handler that accepts all host keys.
///
/// Host key verification is skipped because these clients connect to BMCs and
/// switches on the management network where the surrounding inventory model
/// already identifies the target.
pub(super) struct SshHandler;

impl client::Handler for SshHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKeyOrCertificate,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Async SSH/SFTP client for communicating with network switches.
///
/// Created per operation. The switch node builds a connected client with
/// [`SshClient::connect`] when it needs to execute a remote command or transfer
/// firmware via SFTP, then uses [`SshClient::exec`],
/// [`SshClient::sftp_upload`], or [`SshClient::sftp_file_size`].
///
/// Like `HttpClient`, this is not thread-safe on its own. Concrete node types
/// wrap access in a `tokio::sync::Mutex` when sharing a client is required.
pub struct SshClient {
    /// Target host or IP address used for the SSH session.
    pub(super) host: String,

    /// Active authenticated SSH session, if [`SshClient::connect`] succeeded.
    pub(super) handle: Option<client::Handle<SshHandler>>,
}

/// Timeout tunables for switch NVOS image SFTP uploads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SftpUploadOptions {
    /// Maximum wall-clock time allowed for the whole upload.
    pub overall_timeout: Duration,

    /// Maximum time allowed for one SFTP setup/read/write/flush step.
    pub step_timeout: Duration,
}

impl SftpUploadOptions {
    pub fn new(overall_timeout: Duration, step_timeout: Duration) -> Result<Self> {
        let options = Self {
            overall_timeout,
            step_timeout,
        };

        options.validate()?;

        Ok(options)
    }

    pub fn validate(&self) -> Result<()> {
        if self.overall_timeout.is_zero() {
            return Err(RmsError::invalid_argument(
                "SFTP upload timeout must be greater than 0 seconds",
            ));
        }

        if self.step_timeout.is_zero() {
            return Err(RmsError::invalid_argument(
                "SFTP step timeout must be greater than 0 seconds",
            ));
        }

        if self.step_timeout > self.overall_timeout {
            return Err(RmsError::invalid_argument(
                "SFTP step timeout must be less than or equal to upload timeout",
            ));
        }

        Ok(())
    }
}

impl Default for SftpUploadOptions {
    fn default() -> Self {
        Self {
            overall_timeout: Duration::from_secs(3600),
            step_timeout: Duration::from_secs(30),
        }
    }
}

/// Host and credentials used to open an SSH session.
#[derive(Debug, Clone)]
pub struct SshEndpoint {
    /// Target host or IP address used for the SSH session.
    pub(super) host: String,

    /// Username used for password authentication.
    pub(super) username: String,

    /// Password kept out of debug output and structured logs.
    pub(super) password: SecretString,
}

impl SshEndpoint {
    /// Create SSH connection settings for a target host and credentials.
    pub fn new(
        host: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            username: username.into(),
            password: SecretString::from(password.into()),
        }
    }
}

impl SshClient {
    /// Default timeout for short SSH operations such as connect and exec.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Default overall timeout for firmware-sized SFTP uploads.
    pub const UPLOAD_TIMEOUT: Duration = Duration::from_secs(1800);

    /// Test-only: build a client with no active session, so remote operations
    /// (`exec`, `sftp_upload`, `sftp_file_size`, ...) fail as if disconnected.
    #[cfg(test)]
    pub(crate) fn disconnected_for_test(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            handle: None,
        }
    }

    /// Open an SSH connection and return an authenticated client.
    ///
    /// The client connects to port 22 on the endpoint host. On success, later
    /// calls to [`SshClient::exec`], [`SshClient::sftp_upload`], and
    /// [`SshClient::sftp_file_size`] use the established session.
    ///
    /// # Errors
    ///
    /// Returns `Timeout` if connection or authentication exceeds `timeout`,
    /// `ConnectionRefused` if the SSH connection cannot be opened, or
    /// `InvalidArgument` if authentication fails.
    pub async fn connect(endpoint: SshEndpoint, timeout: Duration) -> Result<Self> {
        let SshEndpoint {
            host,
            username,
            password,
        } = endpoint;

        let host_ref = host.as_str();
        let username_ref = username.as_str();
        let timeout_secs = timeout.as_secs();

        tracing::debug!(
            host = host_ref,
            username = username_ref,
            timeout_secs,
            "SSH connect starting"
        );

        let config = client::Config {
            inactivity_timeout: Some(timeout),
            ..Default::default()
        };

        let mut session = tokio::time::timeout(
            timeout,
            client::connect(Arc::new(config), (host_ref, 22), SshHandler),
        )
        .await
        .map_err(|_| {
            tracing::warn!(
                host = host_ref,
                username = username_ref,
                timeout_secs,
                "SSH connect timed out"
            );
            SshError::timeout("connect", host_ref, timeout)
        })?
        .map_err(|e| {
            let error = e.to_string();

            tracing::warn!(
                host = host_ref,
                username = username_ref,
                error = &error,
                "SSH connect failed"
            );

            SshError::ConnectFailed {
                host: host.clone(),
                details: error,
            }
        })?;

        let authenticated = tokio::time::timeout(
            timeout,
            session.authenticate_password(username_ref, password.expose_secret()),
        )
        .await
        .map_err(|_| {
            tracing::warn!(
                host = host_ref,
                username = username_ref,
                timeout_secs,
                "SSH auth timed out"
            );
            SshError::timeout("auth", host_ref, timeout)
        })?
        .map_err(|e| {
            let error = e.to_string();
            tracing::warn!(
                host = host_ref,
                username = username_ref,
                error = &error,
                "SSH auth failed"
            );

            SshError::AuthFailed {
                host: host.clone(),
                details: error,
            }
        })?;

        match authenticated {
            AuthResult::Success => {
                tracing::debug!(
                    host = host_ref,
                    username = username_ref,
                    "SSH auth succeeded"
                );

                Ok(Self {
                    host,
                    handle: Some(session),
                })
            }
            AuthResult::Failure { .. } => {
                tracing::warn!(
                    host = host_ref,
                    username = username_ref,
                    "SSH auth rejected credentials"
                );

                Err(SshError::AuthFailed {
                    host,
                    details: "authentication failed".to_owned(),
                }
                .into())
            }
        }
    }

    /// Disconnect the active SSH session.
    ///
    /// Calling this method on a disconnected client is a no-op. The client is
    /// consumed so it cannot be reused after the session is closed.
    ///
    /// If this method is not called, dropping the client still drops the
    /// underlying `russh` handle and reclaims the session resources. Use
    /// `close` when callers need to send an explicit SSH disconnect message and
    /// observe disconnect errors.
    ///
    /// # Errors
    ///
    /// Returns `Internal` if the remote disconnect request fails.
    pub async fn close(self) -> Result<()> {
        let Self { host, handle } = self;
        let Some(handle) = handle else {
            return Ok(());
        };

        let host = host.as_str();

        handle
            .disconnect(Disconnect::ByApplication, "", "en")
            .await
            .map_err(|e| {
                let error = e.to_string();
                tracing::warn!(host, error = &error, "SSH disconnect failed");

                SshError::OperationFailed {
                    operation: "disconnect",
                    details: error,
                }
            })?;

        tracing::debug!(host, "SSH session closed");

        Ok(())
    }
}
