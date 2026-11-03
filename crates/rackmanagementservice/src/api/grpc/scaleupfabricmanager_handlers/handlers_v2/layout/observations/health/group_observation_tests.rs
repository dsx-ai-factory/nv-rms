/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared fixtures for regression tests.

mod membership;

use std::collections::VecDeque;

use super::super::sampling::observe_group;
use super::*;
use crate::config::fabric_layout::FabricPosition;
use crate::libnmxc::nmxc_model as nmx;
use crate::libnmxc::test_support::{FakeNmxc, fabric_responses, scripted_response_mut};

fn expected(tray_index: u32) -> ObservedPlacement {
    ObservedPlacement {
        node_id: format!("node-{tray_index}"),
        node_type: String::new(),
        chassis_serial_number: "rack-A01".into(),
        position: FabricPosition {
            tray_index,
            slot_number: 0,
        },
        physical_topology: None,
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
