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

//! NVOS CLI handler wiring for switch targets.

use std::time::Duration;

use nvswitch_cli::{
    CommandFailure, HandlerConfig, NvSwitchCliHandler, NvSwitchTransport, SwitchCommand,
};

use crate::ssh_transport::{self, SshHostKeyPolicy};
use crate::utils::Util as NvUtils;

pub(crate) type SwitchCliHandler = NvSwitchCliHandler<SwitchCliTransport>;

#[derive(Debug, Clone)]
pub(crate) struct SwitchCliError {
    message: String,
}

impl std::fmt::Display for SwitchCliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl CommandFailure for SwitchCliError {
    fn message(&self) -> &str {
        &self.message
    }

    fn with_message(&self, message: String) -> Self {
        Self { message }
    }

    fn from_message(message: String) -> Self {
        Self { message }
    }
}

/// Runs NVOS CLI commands over `nvfwupd`'s SSH transport.
pub(crate) struct SwitchCliTransport {
    host: String,
    port: u16,
    user: String,
    password: String,
    host_key_policy: SshHostKeyPolicy,
}

impl SwitchCliTransport {
    /// Default SSH port for switch management access.
    pub(crate) const SSH_PORT: u16 = 22;

    pub(crate) fn new(
        host: &str,
        user: &str,
        password: &str,
        host_key_policy: SshHostKeyPolicy,
    ) -> Self {
        Self {
            host: host.replace(['[', ']'], ""),
            port: Self::SSH_PORT,
            user: user.to_string(),
            password: password.to_string(),
            host_key_policy,
        }
    }

    /// Build a handler that runs each command with `timeout_secs`.
    pub(crate) fn into_handler(self, timeout_secs: u64) -> SwitchCliHandler {
        SwitchCliHandler::with_config(
            self,
            HandlerConfig {
                command_timeout: Duration::from_secs(timeout_secs),
            },
        )
    }
}

impl NvSwitchTransport for SwitchCliTransport {
    type Failure = SwitchCliError;

    fn host(&self) -> &str {
        &self.host
    }

    async fn run(
        &self,
        command: &SwitchCommand,
        timeout: Duration,
    ) -> std::result::Result<String, Self::Failure> {
        let output = ssh_transport::execute_command_async(
            &self.host,
            self.port,
            &self.user,
            &self.password,
            timeout.as_secs(),
            &command.render(),
            None,
            self.host_key_policy.clone(),
        )
        .await
        .map_err(|error| SwitchCliError {
            // Transport errors can echo the target address and password, so
            // they are sanitized before they reach a caller's message.
            message: NvUtils::sanitize_log(&error),
        })?;

        if output.success {
            return Ok(output.stdout);
        }

        Err(SwitchCliError {
            message: NvUtils::sanitize_log(&format!(
                "stdout='{}', stderr='{}'",
                output.stdout.trim(),
                output.stderr.trim()
            )),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nvswitch_cli::{RemotePath, ShellCommand};

    fn transport(host: &str) -> SwitchCliTransport {
        SwitchCliTransport::new(
            host,
            "admin",
            "admin",
            SshHostKeyPolicy::insecure_accept_any(),
        )
    }

    #[test]
    fn ipv6_brackets_are_stripped_from_the_host() {
        assert_eq!(transport("[fd00::1]").host(), "fd00::1");
        assert_eq!(transport("10.0.0.1").host(), "10.0.0.1");
    }

    #[tokio::test]
    async fn commands_reach_the_ssh_transport_as_rendered_text() {
        let mock_host = "mock-switch-cli-render";
        ssh_transport::install_mock_for_host(mock_host);

        let handler = transport(mock_host).into_handler(30);
        let path = RemotePath::new("/host/fw-images/bmc/").unwrap();

        handler.create_directory_tree(&path).await.unwrap();

        let ssh = ssh_transport::take_mock_snapshot(mock_host).unwrap();

        assert_eq!(
            ssh.exec_calls
                .iter()
                .map(|call| call.command.as_str())
                .collect::<Vec<_>>(),
            vec!["mkdir -p '/host/fw-images/bmc/'"]
        );
    }

    /// Only the always-on secret redaction is asserted here. The IP-address
    /// pass depends on `Util`'s process-wide sanitize toggle, which other tests
    /// in this crate mutate; `bmc_access` covers that end to end.
    #[tokio::test]
    async fn transport_errors_drop_secret_material() {
        let mock_host = "mock-switch-cli-transport-error";
        ssh_transport::install_mock_exec_error_for_host(
            mock_host,
            "connect failed for ip=192.0.2.105 password=plain_secret",
        );

        let handler = transport(mock_host).into_handler(30);
        let path = RemotePath::new("/host/nos-images/nvos.bin").unwrap();

        let error = handler
            .run(ShellCommand::RemoveFile { path })
            .await
            .expect_err("mock transport should fail");

        assert!(!error.message().contains("plain_secret"));
        assert!(error.message().contains("connect failed"));

        let _ = ssh_transport::take_mock_snapshot(mock_host);
    }

    #[tokio::test]
    async fn a_rejected_command_still_reaches_the_transport() {
        let mock_host = "mock-switch-cli-command-rejected";
        ssh_transport::install_mock_exec_error_for_host(mock_host, "boom");

        let handler = transport(mock_host).into_handler(30);
        let path = RemotePath::new("/host/nos-images/nvos.bin").unwrap();

        handler
            .run(ShellCommand::RemoveFile { path })
            .await
            .expect_err("mock transport should fail");

        let ssh = ssh_transport::take_mock_snapshot(mock_host).unwrap();

        assert_eq!(
            ssh.exec_calls
                .iter()
                .map(|call| call.command.as_str())
                .collect::<Vec<_>>(),
            vec!["rm -f '/host/nos-images/nvos.bin'"]
        );
    }

    #[test]
    fn rewrapping_a_failure_replaces_only_the_message() {
        let error = SwitchCliError {
            message: "connection reset".to_string(),
        };

        assert_eq!(
            error.with_message("composed".to_string()).message(),
            "composed"
        );
        assert_eq!(
            SwitchCliError::from_message("composed".to_string()).message(),
            "composed"
        );
    }
}
