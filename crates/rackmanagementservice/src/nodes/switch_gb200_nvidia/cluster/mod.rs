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
