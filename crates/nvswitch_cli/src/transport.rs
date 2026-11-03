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

//! The seam between this crate's command layer and a caller's SSH stack.
//!
//! This crate owns *what* runs on a switch and *in what order*; it does not own
//! the SSH session. Callers implement [`NvSwitchTransport`] over whatever
//! connection they already maintain, and [`CommandFailure`] over whatever error
//! type they already propagate. Keeping the error type on the caller's side is
//! deliberate: RMS maps failures to gRPC status codes, and routing those
//! through an intermediate enum here would flatten codes this crate has no way
//! to reconstruct.

use std::future::Future;
use std::time::Duration;

use crate::command::SwitchCommand;

/// A caller-owned execution failure that this crate can classify and re-wrap.
///
/// Implementations are expected to be cheap to clone-by-message: the handler
/// composes new failures from existing ones (for example when a command and its
/// fallback both fail) and must preserve the original classification while
/// replacing the message.
///
/// # Examples
///
/// ```
/// use nvswitch_cli::CommandFailure;
///
/// #[derive(Debug)]
/// struct MyError {
///     message: String,
/// }
///
/// impl CommandFailure for MyError {
///     fn message(&self) -> &str {
///         &self.message
///     }
///
///     fn with_message(&self, message: String) -> Self {
///         Self { message }
///     }
///
///     fn from_message(message: String) -> Self {
///         Self { message }
///     }
/// }
/// ```
pub trait CommandFailure: std::fmt::Debug + Sized {
    /// Human-readable failure detail.
    ///
    /// This is the text matched against
    /// [`BenignFailure`](crate::BenignFailure) fragments, so it should be the
    /// switch's own message rather than a decorated or prefixed form.
    fn message(&self) -> &str;

    /// Rebuild this failure with a new message, keeping its classification.
    fn with_message(&self, message: String) -> Self;

    /// Build a failure that did not originate from the transport.
    ///
    /// Used for conditions this crate detects itself, such as a command that
    /// exited successfully but did not produce its expected confirmation.
    fn from_message(message: String) -> Self;
}

/// Runs one validated command on a switch.
///
/// Implementations call [`SwitchCommand::render`] to get the command line to
/// execute, and must send that text verbatim: they must not append, quote, or
/// otherwise rewrite it, because quoting already happened during rendering and
/// doing it twice would corrupt paths containing spaces.
///
/// Receiving the typed command rather than a bare string lets an implementation
/// inspect what it is about to run - to pick a timeout per command kind, to
/// route `nv` commands differently from POSIX helpers, or to log
/// [`SwitchCommand::render_masked`] instead of the raw text.
///
/// A successful result is the command's standard output. A non-zero exit
/// status, a signal, or a missing exit status must all be reported as failures.
pub trait NvSwitchTransport {
    /// The caller's error type.
    type Failure: CommandFailure;

    /// Host or address this transport is bound to, used as a log field.
    fn host(&self) -> &str;

    /// Execute `command`, returning its standard output.
    fn run(
        &self,
        command: &SwitchCommand,
        timeout: Duration,
    ) -> impl Future<Output = std::result::Result<String, Self::Failure>> + Send;
}
