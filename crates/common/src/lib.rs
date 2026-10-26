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

//! Shared, dependency-light common primitives for the RMS workspace.
//!
//! Each concern lives in its own module so callers import exactly what they
//! need rather than pulling in an unrelated crate for one helper:
//!
//! - [`redaction`] masks password/token-like key/value fields in free text and
//!   JSON so credentials never reach logs, errors, or diagnostics.
//! - [`log_sanitize`] provides a configurable literal-string / IP-address log
//!   sanitizer.
//! - [`net`] resolves device-supplied request endpoints against a client's own
//!   authority, fencing out SSRF / confused-deputy host swaps and path
//!   traversal.
//!
//! The modules are intentionally decoupled from one another; import them
//! separately (e.g. `use common::redaction;` or `use common::net;`).

pub mod log_sanitize;
pub mod net;
pub mod redaction;

/// Token substituted for redacted secret material across every helper here.
pub const DEFAULT_REPLACEMENT: &str = "XXXX";
