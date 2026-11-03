/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reads NMX-C responses and converts them to fabric status.

mod health;
mod sampling;

pub(super) use health::{interpret_group, observation_identity};
pub(super) use sampling::{FabricSamples, sample_group};

/// Chassis and tray identity used to match live devices to caller node IDs.
#[derive(Clone)]
pub(super) struct ObservedIdentity {
    pub(super) node_id: String,
    pub(super) chassis_serial_number: String,
    pub(super) tray_index: u32,
}
