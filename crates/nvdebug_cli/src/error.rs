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
//! Errors from preparing and running nvdebug. A non-zero exit from nvdebug is
//! not an error here; it is the status inside a [`crate::Collection`].
//!
//! The variants are the decisions a caller acts on. An executable that
//! cannot be used is [`Error::Unavailable`]: it could not be looked up or
//! started, it is not a regular file, or its `--version` failed.
//! [`Error::Failed`] is a failure no caller acts on: a task on tokio's
//! blocking pool that panicked or that a shutting-down runtime dropped,
//! waiting on a started nvdebug, the DUT file and its directory, or the
//! output directory's lock. A failure leaves this kind for its own the day a
//! caller acts on it.
//!
//! Both carry a [`Detail`]: the action and its object as text, and the error
//! that said why, kept as it came, when there is one. A reason that is not
//! an error, an exit status or a "not a regular file", is part of the text.
//! The text names the executable's path, so a typo in the configured value
//! shows, and a path only this crate picks: the DUT file and its directory.
//!
//! [`Error`]'s Display is the action alone, and [`std::error::Error::source`]
//! gives the cause. A plain `{error}` drops the cause: print the chain, as
//! eyre's `{:#}` or a `tracing` field recorded as `&dyn Error` do.

use std::error::Error as StdError;
use std::path::Path;
use std::time::Duration;

/// A cause of any error type. The box makes it `'static`, which
/// `downcast_ref` needs. `Send` and `Sync` let it cross await points and
/// threads, and convert into eyre or anyhow with `?`.
type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A value the caller passed, or a path it named, that the crate refuses.
    /// `what` names the value; `why` says what the crate expected.
    #[error("invalid {what}: {why}")]
    Invalid { what: &'static str, why: String },

    #[error("nvdebug did not finish within {0:?}")]
    Timeout(Duration),

    #[error("cancelled")]
    Cancelled,

    /// The executable cannot be used.
    #[error(transparent)]
    Unavailable(Detail),

    /// A failure no caller acts on.
    #[error(transparent)]
    Failed(Detail),
}

/// What failed and on what, and the error that said why, when there is one.
/// The fields are private: a caller reads the text through Display, and the
/// cause through `source()`.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Detail {
    message: String,
    #[source]
    cause: Option<BoxError>,
}

// TODO: a one-line view for people, the action and then its cause, as in
// `error during log collection: write failed /tmp/x - Permission denied`.
// Display cannot carry the cause, since `source()` already does and a chain
// printer would show it twice. It lands with the first consumer that prints
// these errors to people, when the crate is wired into RMS. Until then the
// probe and the logs print the source chain.

/// Constructors, one set per kind.
impl Error {
    // Set of Error::Invalid helpers

    pub(crate) fn new_invalid(what: &'static str, why: impl Into<String>) -> Self {
        Self::Invalid {
            what,
            why: why.into(),
        }
    }

    /// The same, suitable for map_err() calls. The cause is dropped, so `why`
    /// has to say all of it. Note that we don't care about what E actually
    /// is, thus no need to have any bounds applied to it.
    pub(crate) fn invalid<E>(what: &'static str, why: &'static str) -> impl FnOnce(E) -> Self {
        move |_e: E| Self::new_invalid(what, why)
    }

    //--------------------------------------------------------------------------

    // Set of Error::Unavailable helpers

    /// The whole reason is in `message`: there is no error behind it.
    pub(crate) fn new_unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable(Detail::new(message, None))
    }

    /// For map_err() calls: `message` names the action and its object, and
    /// the error is kept as the cause.
    pub(crate) fn unavailable<E>(message: impl Into<String>) -> impl FnOnce(E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        move |cause| Self::Unavailable(Detail::new(message, Some(Box::new(cause))))
    }

    //--------------------------------------------------------------------------

    // Set of Error::Failed helpers

    /// For map_err() calls: `message` names the action and its object, and
    /// the error is kept as the cause.
    pub(crate) fn failed<E>(message: impl Into<String>) -> impl FnOnce(E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        move |cause| Self::Failed(Detail::new(message, Some(Box::new(cause))))
    }

    /// The same on a path: `what` failed on `path`. The text is built only
    /// when there is an error to build it for.
    pub(crate) fn failed_path<E>(what: &'static str, path: &Path) -> impl FnOnce(E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        move |cause| Self::failed(format!("{what} {}", path.display()))(cause)
    }
}

impl Detail {
    fn new(message: impl Into<String>, cause: Option<BoxError>) -> Self {
        Self {
            message: message.into(),
            cause,
        }
    }
}
