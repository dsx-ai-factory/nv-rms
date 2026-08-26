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

//! Power-operation mapping for NVIDIA GB200 switch nodes.

use crate::domain::node::PowerOp;
use crate::transport::redfish_client::ResetType as RedfishResetType;
use crate::utilities::error::{Result, RmsError};

pub(super) fn switch_redfish_reset_type(op: PowerOp) -> Result<RedfishResetType> {
    match op {
        PowerOp::On => Ok(RedfishResetType::On),
        PowerOp::Off => Ok(RedfishResetType::GracefulShutdown),
        PowerOp::ForceOn => Ok(RedfishResetType::ForceOn),
        PowerOp::ForceOff => Ok(RedfishResetType::ForceOff),
        PowerOp::PowerCycle => Ok(RedfishResetType::PowerCycle),
        PowerOp::GracefulShutdown => Ok(RedfishResetType::GracefulShutdown),
        PowerOp::GracefulRestart => Ok(RedfishResetType::GracefulRestart),
        PowerOp::ForceRestart => Ok(RedfishResetType::ForceRestart),
        PowerOp::Nmi => Err(RmsError::invalid_argument(
            "switch nodes do not support NMI reset",
        )),
    }
}
