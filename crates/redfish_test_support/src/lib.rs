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

//! Test-only Redfish simulator used by RMS integration tests and benchmarks.
//!
//! The public API is [`RedfishSimulator`]. It starts one or more HTTPS Redfish
//! endpoints from bundled fixture archives and applies simulation parameters
//! such as firmware task delay and failure rate.

pub mod fixtures;
mod redfish_mockup_server;
pub mod simulator;

pub use simulator::{RedfishFixture, RedfishSimulator, RedfishSimulatorBuilder};
