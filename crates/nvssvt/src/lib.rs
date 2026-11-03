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

//! Launches and tracks NVSSVT scans through NVSSVT's own launcher API
//! (`nvssvt_server.py`).
//!
//! [`runner::NvssvtRunner`] posts a scan to the launcher and polls it until
//! the scan finishes, at which point the parsed summary report is available
//! too. NVSSVT runs as a second container in the same pod as the caller, so
//! the default target is `http://127.0.0.1:8000` and no configuration is
//! required; host and port are overridable for development against an
//! instance running elsewhere.
//!
//! The launcher owns the NVSSVT subprocess and its log directory -- `pid`
//! and `log_dir` on [`runner::LaunchOutcome`]/[`runner::ScanStatus`] describe
//! the launcher's filesystem, not this process's. This crate is kept free of
//! any gRPC service implementation or job-tracking policy, so it can be
//! embedded into any server without pulling in the rest of this workspace.
//! The gRPC surface (`RackManagerV2.StartSystemValidation`, in `librms`) and
//! job-tracking policy live in the caller (`rackmanagementservice`), which
//! drives [`runner::NvssvtRunner`] from its own async job framework.

mod client;
pub mod config;
pub mod error;
pub mod runner;

pub use config::{DEFAULT_HOST, DEFAULT_PORT, DEFAULT_REQUEST_TIMEOUT, NvssvtConfig};
pub use error::NvssvtError;
pub use runner::NvssvtRunner;
