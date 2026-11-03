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

//! Typed NVOS `nv` CLI commands and ordered flows for NVIDIA switches,
//! shared by RMS and `nvfwupd`.
//!
//! Switch CLI commands used to be assembled as string literals and `format!`
//! calls at each call site. A misspelled verb, a transposed path segment, or a
//! `nv config save` that drifted ahead of the `nv config apply` it was meant to
//! persist all produced working code that silently did the wrong thing on a
//! switch. This crate makes those mistakes unrepresentable:
//!
//! - **Commands are values.** [`NvCommand`] and [`ShellCommand`] name the
//!   operation; this crate renders the command line. Arguments flow through
//!   validated types ([`NvPath`], [`RemotePath`], [`ClusterApp`],
//!   [`FileMode`]) that reject anything needing quoting or capable of splitting
//!   a command line.
//! - **Order is a value too.** A [`CommandFlow`] fixes the sequence once, in a
//!   named constructor, instead of leaving it implicit in whichever function
//!   happens to run the commands.
//! - **Failure policy travels with the step.** NVOS reports "already in the
//!   desired state" and "this build has no such schema" as ordinary command
//!   failures. [`BenignFailure`] classifies them next to the command they
//!   belong to rather than in an `if` several call frames away.
//! - **One execution path.** [`NvSwitchCliHandler`] owns timeouts, logging,
//!   and output checking for every command.
//!
//! # Bring your own SSH
//!
//! This crate does not open SSH sessions. RMS and `nvfwupd` each maintain a
//! `russh` stack with features the other does not need, so the seam is the
//! [`NvSwitchTransport`] trait: implement it over an existing session, and
//! implement [`CommandFailure`] over the error type you already propagate.
//! Keeping the error type on the caller's side means RMS's gRPC status codes
//! survive a round trip through here.
//!
//! # Examples
//!
//! ```
//! # use std::time::Duration;
//! # use nvswitch_cli::{CommandFailure, NvSwitchTransport};
//! # #[derive(Debug)]
//! # struct Error(String);
//! # impl CommandFailure for Error {
//! #     fn message(&self) -> &str { &self.0 }
//! #     fn with_message(&self, message: String) -> Self { Self(message) }
//! #     fn from_message(message: String) -> Self { Self(message) }
//! # }
//! # struct Ssh;
//! # impl From<nvswitch_cli::InvalidCommand> for Error {
//! #     fn from(error: nvswitch_cli::InvalidCommand) -> Self { Self(error.to_string()) }
//! # }
//! # impl NvSwitchTransport for Ssh {
//! #     type Failure = Error;
//! #     fn host(&self) -> &str { "10.0.0.1" }
//! #     async fn run(
//! #         &self,
//! #         _command: &nvswitch_cli::SwitchCommand,
//! #         _timeout: Duration,
//! #     ) -> Result<String, Error> {
//! #         Ok(String::new())
//! #     }
//! # }
//! # async fn example(ssh: Ssh) -> Result<(), Error> {
//! use nvswitch_cli::{
//!     ClusterApp, CommandFlow, MtlsService, NvSwitchCliHandler, StepStatus, StepSubject,
//! };
//!
//! let handler = NvSwitchCliHandler::new(ssh);
//!
//! // Clear mTLS on two services, then apply and save - in that order, always.
//! let report = handler
//!     .run_flow(&CommandFlow::unset_mtls(&[
//!         MtlsService::NvueApi,
//!         MtlsService::ClusterApp(ClusterApp::nmx_controller()),
//!     ])?)
//!     .await
//!     .map_err(|failure| failure.failure)?;
//!
//! // Steps are identified by subject, not by command text.
//! let steps = report.steps();
//! assert_eq!(
//!     steps[0].subject(),
//!     Some(&StepSubject::Mtls(MtlsService::NvueApi)),
//! );
//! assert_eq!(
//!     steps[0].status(),
//!     &StepStatus::Succeeded { used_fallback: false },
//! );
//! assert_eq!(steps[3].subject().map(StepSubject::label), Some("config-save"));
//! # Ok(())
//! # }
//! ```

mod command;
mod error;
mod flow;
mod handler;
mod transport;

pub use command::{
    ClusterApp, FileMode, FileModeBuilder, ManagerEncryption, MtlsService, NvActionVerb, NvCommand,
    NvConfigOperation, NvPath, RemotePath, ShellCommand, SwitchCommand,
};
pub use error::{InvalidCommand, Result};
pub use flow::{BenignFailure, BenignOutcome, BenignReason, CommandFlow, FlowStep, StepSubject};
pub use handler::{
    FlowFailure, FlowReport, HandlerConfig, NvSwitchCliHandler, StepReport, StepStatus,
};
pub use transport::{CommandFailure, NvSwitchTransport};
