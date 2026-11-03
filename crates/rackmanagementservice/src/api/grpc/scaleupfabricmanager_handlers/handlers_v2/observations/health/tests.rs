/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared fixtures for regression tests.

mod health;
mod protocol;
mod sampling;

use std::collections::VecDeque;
use std::time::Duration;

use super::super::sample_group;
use super::super::sampling::sample_resource;
use super::*;
use crate::libnmxc::test_support::{FakeNmxc, fabric_responses, scripted_response_mut};
use crate::libnmxc::{NmxcError, nmxc_model as nmx};

use tokio::time::Instant;

fn expected(tray_index: u32) -> ObservedIdentity {
    ObservedIdentity {
        node_id: format!("node-{tray_index}"),
        chassis_serial_number: "rack-A01".into(),
        tray_index,
    }
}

fn healthy_client() -> FakeNmxc {
    let switch_loc = nmx::NmxLocationInfo {
        chassis_serial_number: "rack-A01".into(),
        tray_index: 5,
        location: None,
    };

    let compute_loc = nmx::NmxLocationInfo {
        tray_index: 1,
        ..switch_loc.clone()
    };

    let port = nmx::NmxPortTopoInfo {
        port_type: 2,
        physical_state: 5,
        logical_state: 4,
        port_admin_state: 1,
        ..Default::default()
    };

    let responses = fabric_responses(
        switch_loc,
        &[100, 101],
        1,
        vec![nmx::NmxGpuTopoInfo {
            loc: Some(compute_loc),
            device_uid: 200,
            device_health: 1,
            port_topo_info: vec![port],
            ..Default::default()
        }],
        FakeNmxc::success_return_code().server_header,
    );

    FakeNmxc {
        domain_state_results: VecDeque::from([Ok(responses.domain)]),
        topology_results: VecDeque::from([Ok(responses.topology)]),
        switch_node_results: VecDeque::from([Ok(responses.switches)]),
        compute_node_results: VecDeque::from([Ok(responses.computes)]),
        ..Default::default()
    }
}

/// Exercises the shared sampler and interpreter without the mutation workflow.
async fn observe_samples(
    client: &mut dyn crate::libnmxc::Nmxc,
    switches: &[ObservedIdentity],
    computes: &[ObservedIdentity],
    gateway: &str,
) -> GroupObservation {
    let samples = sample_group(client, gateway, None).await;
    interpret_group(&samples, switches, computes)
}
