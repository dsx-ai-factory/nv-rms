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

//! Cluster state, application management, and SDN reset workflows.

mod app;
mod node;
mod sdn;
mod state;

#[cfg(test)]
pub(super) use self::sdn::SDN_FACTORY_RESET_UNCONFIGURED_CONFIRMATION_POLLS;

pub use self::app::grpc_port_for_app;

use std::time::Duration;

#[cfg(not(test))]
use super::config;

#[cfg(test)]
const RETRY_WAIT_DELAY: Duration = Duration::ZERO;
#[cfg(not(test))]
const RETRY_WAIT_DELAY: Duration = Duration::from_secs(config::RETRY_WAIT_SECONDS);
