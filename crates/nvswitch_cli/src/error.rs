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

//! Errors raised while building commands.
/// A command could not be built because an argument was not representable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidCommand {
    #[error("invalid {kind} {value:?}: expected a non-empty [A-Za-z0-9_-] identifier")]
    Identifier {
        /// What the identifier was meant to name, for example `cluster app`.
        kind: &'static str,
        /// The rejected value.
        value: String,
    },

    #[error("invalid remote path {value:?}: {reason}")]
    RemotePath {
        /// The rejected value.
        value: String,

        /// Why the path cannot be rendered safely.
        reason: &'static str,
    },

    #[error("NVOS path must contain at least one segment")]
    EmptyPath,
}

/// Result alias for command construction.
pub type Result<T> = std::result::Result<T, InvalidCommand>;
