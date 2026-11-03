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

//! NVOS CLI handler wiring for NVIDIA GB200 switches.
//!
//! Every `nv` CLI command and shell helper RMS runs on a switch goes through
//! the shared [`nvswitch_cli`] crate, which owns the command text, step
//! ordering, and failure classification. This module supplies the
//! two things that crate deliberately does not own: RMS's SSH session and
//! RMS's error type.

use std::time::Duration;

use super::SwitchGb200Nvidia;
use crate::transport::ssh::strip_ssh_command_text;
use crate::transport::ssh_client::SshClient;
use crate::utilities::error::{Result, RmsError};

use nvswitch_cli::{
    CommandFailure, HandlerConfig, InvalidCommand, NvCommand, NvSwitchCliHandler,
    NvSwitchTransport, SwitchCommand,
};

/// The NVOS CLI handler bound to RMS's SSH transport.
pub(crate) type SwitchCliHandler = NvSwitchCliHandler<SwitchCliTransport>;

impl CommandFailure for RmsError {
    fn message(&self) -> &str {
        &self.message
    }

    fn with_message(&self, message: String) -> Self {
        Self::new(self.code, message)
    }

    fn from_message(message: String) -> Self {
        Self::internal(message)
    }
}

/// An argument could not be rendered into a command line.
impl From<InvalidCommand> for RmsError {
    fn from(error: InvalidCommand) -> Self {
        Self::invalid_argument(error.to_string())
    }
}

/// Runs NVOS CLI commands over RMS's SSH client.
pub(crate) enum SwitchCliTransport {
    /// A connected SSH session.
    Ssh {
        /// Switch host, kept alongside the client for log fields.
        host: String,

        /// Authenticated session used for every command.
        client: SshClient,
    },

    /// Test double that records commands instead of opening a session.
    #[cfg(test)]
    Test {
        /// Switch host reported in log fields.
        host: String,

        /// Closure standing in for remote execution.
        exec: std::sync::Arc<super::SshExecForTest>,
    },
}

impl NvSwitchTransport for SwitchCliTransport {
    type Failure = RmsError;

    fn host(&self) -> &str {
        match self {
            Self::Ssh { host, .. } => host,
            #[cfg(test)]
            Self::Test { host, .. } => host,
        }
    }

    async fn run(&self, command: &SwitchCommand, timeout: Duration) -> Result<String> {
        let rendered = command.render();

        let outcome = match self {
            // A full reset may close SSH before the command reports its exit
            // status; the reset job verifies completion through login polling.
            Self::Ssh { client, .. }
                if command == &SwitchCommand::Nv(NvCommand::factory_reset()) =>
            {
                client.exec_for_reboot(&rendered, timeout).await
            }
            Self::Ssh { client, .. } => client.exec(&rendered, timeout).await,
            #[cfg(test)]
            Self::Test { exec, .. } => exec(&rendered),
        };

        // The transport names the command it ran in its error text. That text
        // travels up through flow reports into job results, so strip the
        // command here: which `nv` line RMS chose is an NVOS implementation
        // detail that belongs in this node's tracing, not in a client payload.
        // The exit status and the switch's own stdout/stderr are kept.
        outcome.map_err(|error| {
            RmsError::new(
                error.code,
                strip_ssh_command_text(&error.message, &rendered),
            )
        })
    }
}

impl SwitchGb200Nvidia {
    /// Open an NVOS CLI handler for this switch.
    ///
    /// Each handler owns one SSH session, so callers that run several commands
    /// should build one handler and reuse it rather than reconnecting per
    /// command.
    ///
    /// # Errors
    ///
    /// Returns `FailedPrecondition` when the switch has no host endpoint or
    /// credentials, or the connection errors from
    /// [`SshClient::connect`](crate::transport::ssh_client::SshClient::connect).
    pub(crate) async fn nv_cli(&self) -> Result<SwitchCliHandler> {
        let host = self.require_host_endpoint()?.ip_address.clone();

        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            return Ok(SwitchCliHandler::with_config(
                SwitchCliTransport::Test {
                    host,
                    exec: std::sync::Arc::clone(exec),
                },
                Self::nv_cli_config(),
            ));
        }

        let client = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        Ok(SwitchCliHandler::with_config(
            SwitchCliTransport::Ssh { host, client },
            Self::nv_cli_config(),
        ))
    }

    /// Wrap an already-connected SSH session in an NVOS CLI handler.
    pub(crate) fn nv_cli_over(&self, client: SshClient) -> Result<SwitchCliHandler> {
        let host = self.require_host_endpoint()?.ip_address.clone();

        Ok(SwitchCliHandler::with_config(
            SwitchCliTransport::Ssh { host, client },
            Self::nv_cli_config(),
        ))
    }

    /// Timeout applied to every NVOS CLI command.
    fn nv_cli_config() -> HandlerConfig {
        HandlerConfig {
            command_timeout: SshClient::DEFAULT_TIMEOUT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;

    #[test]
    fn rewrapping_a_failure_preserves_its_error_code() {
        let error = RmsError::new(ErrorCode::Unavailable, "original");
        let rewrapped = error.with_message("composed".to_owned());

        assert_eq!(rewrapped.code, ErrorCode::Unavailable);
        assert_eq!(rewrapped.message, "composed");
    }

    /// Failures the handler detects itself - a command that exits zero without
    /// its expected confirmation - have no switch-side code to carry over.
    #[test]
    fn handler_detected_failures_map_to_internal() {
        let error = <RmsError as CommandFailure>::from_message("no confirmation".to_owned());

        assert_eq!(error.code, ErrorCode::Internal);
    }

    #[test]
    fn argument_rejections_map_to_invalid_argument() {
        let Err(path_err) = nvswitch_cli::RemotePath::new("/tmp/a\nrm -rf /") else {
            panic!("Should be error")
        };
        let error = RmsError::from(path_err);

        assert_eq!(error.code, ErrorCode::InvalidArgument);
        assert!(error.message.contains("line break"));
    }
}
