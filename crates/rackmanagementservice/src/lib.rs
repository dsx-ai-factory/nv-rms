/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
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
