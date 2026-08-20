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
