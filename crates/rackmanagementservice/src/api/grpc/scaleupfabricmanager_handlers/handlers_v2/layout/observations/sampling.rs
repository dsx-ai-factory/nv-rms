/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Checks expected identities before collecting controller readiness evidence.

use std::collections::BTreeSet;

use super::{GroupObservation, ObservedPlacement, interpret_group, sample_group};
use crate::libnmxc::Nmxc;

/// Validates expected membership before sampling reconciliation health.
pub(in super::super) async fn observe_group(
    client: &mut dyn Nmxc,
    expected_switches: &[ObservedPlacement],
    expected_computes: &[ObservedPlacement],
    gateway_id: &str,
) -> Result<GroupObservation, String> {
    if expected_switches.is_empty()
        || expected_switches
            .iter()
            .chain(expected_computes)
            .any(|node| node.chassis_serial_number.trim().is_empty())
    {
        return Err("group observation requires at least one switch and known physical identities for all switches and computes".into());
    }

    // Tray indices identify nodes within each kind; computes and switches may
    // share an index. Caller labels do not distinguish duplicate hardware.
    for nodes in [expected_switches, expected_computes] {
        let mut identities = BTreeSet::new();

        if nodes.iter().any(|node| {
            !identities.insert((
                node.chassis_serial_number.as_str(),
                node.position.tray_index,
            ))
        }) {
            return Err("group observation contains duplicate expected physical identities".into());
        }
    }

    let samples = sample_group(client, gateway_id, None).await;

    Ok(interpret_group(
        &samples,
        expected_switches,
        expected_computes,
    ))
}
