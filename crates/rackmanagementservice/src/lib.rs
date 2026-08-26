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

// Crate root — declares all modules so they are accessible from both the binary and tests.

pub mod api;
pub mod config;
pub mod domain;
pub mod libgnmi;
pub mod libnmxc;
pub mod logging;
pub mod metrics;
pub mod nodes;
pub mod orchestrator;
pub mod persistence;
pub mod racks;
pub mod transport;
pub mod utilities;

// Not `#[cfg(test)]`-gated: this crate's own `bin` target (`src/main.rs`) depends
// on this lib as an ordinary (non-test) dependency, so a `cfg(test)` gate here
// would vanish for the binary's own unit tests even though `cfg(test)` is active
// for *that* crate. Exposing it unconditionally lets `src/main.rs` reuse it too.
pub mod test_env;
