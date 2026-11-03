/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Controller and data-plane health regression tests.

use super::*;

#[tokio::test]
async fn partial_topology_retains_identified_node_failures_without_trusting_placeholders() {
    use rm::ScaleUpFabricReportedStatus as Health;

    for (raw, expected_health) in [
        (1, Health::Unknown),
        (2, Health::Abnormal),
        (0, Health::Unknown),
        (i32::MAX, Health::Unknown),
    ] {
        let mut client = healthy_client();

        scripted_response_mut(&mut client.topology_results)
            .device_topo_info
            .clear();

        let switches = scripted_response_mut(&mut client.switch_node_results);

        switches.node_info_list[0].node_health = raw;

        switches.node_info_list.push(nmx::NmxSwitchNodeInfo {
            node_health: 2,
            ..Default::default()
        });

        let computes = scripted_response_mut(&mut client.compute_node_results);

        computes.node_info_list[0].node_health = raw;

        computes.node_info_list.push(nmx::NmxComputeNodeInfo {
            node_health: 2,
            ..Default::default()
        });

        let observed =
            observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

        assert_eq!(
            observed.health.compute, expected_health as i32,
            "compute health {raw}"
        );

        assert_eq!(
            observed.health.switches, expected_health as i32,
            "switch health {raw}"
        );

        assert_eq!(observed.health.links, Health::Unknown as i32);
    }
}

#[tokio::test]
async fn omitted_unhealthy_node_rows_override_other_healthy_topology_devices() {
    use rm::ScaleUpFabricReportedStatus as Health;

    let mut client = healthy_client();

    let omitted_location = nmx::NmxLocationInfo {
        chassis_serial_number: "rack-A01".into(),
        tray_index: 9,
        ..Default::default()
    };

    scripted_response_mut(&mut client.compute_node_results)
        .node_info_list
        .push(nmx::NmxComputeNodeInfo {
            loc: Some(omitted_location.clone()),
            node_health: 2,
            ..Default::default()
        });

    scripted_response_mut(&mut client.switch_node_results)
        .node_info_list
        .push(nmx::NmxSwitchNodeInfo {
            loc: Some(omitted_location),
            node_health: 2,
            ..Default::default()
        });

    let observed = observe_samples(&mut client, &[expected(5)], &[], "gateway").await;

    assert_eq!(observed.health.compute, Health::Abnormal as i32);
    assert_eq!(observed.health.switches, Health::Abnormal as i32);
    assert_eq!(observed.health.links, Health::Normal as i32);
}

#[tokio::test]
async fn configured_controller_does_not_hide_unhealthy_gpu_switch_and_links() {
    let mut client = healthy_client();

    let topology = scripted_response_mut(&mut client.topology_results);

    for device in &mut topology.device_topo_info {
        match device.device.as_mut().unwrap() {
            nmx::nmx_device_topo_info::Device::SwitchTopoInfo(switch) => {
                switch.device_health = 2;
                switch.port_topo_info.first_mut().unwrap().logical_state = 1;
            }
            nmx::nmx_device_topo_info::Device::GpuTopoInfo(gpu) => gpu.device_health = 3,
        }
    }

    let observation = observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

    assert_eq!(
        observation.control_plane_state,
        rm::ScaleUpFabricControlPlaneState::Configured
    );

    assert_eq!(
        observation.health.controller,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );

    assert_eq!(
        observation.health.compute,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );

    assert_eq!(
        observation.health.switches,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );

    assert_eq!(
        observation.health.links,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );
}

#[tokio::test]
async fn configured_domain_with_unhealthy_controller_is_not_healthy_readiness() {
    let mut client = healthy_client();

    scripted_response_mut(&mut client.domain_state_results).nmx_controller_health = 3;

    let observation = observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

    assert_eq!(
        observation.control_plane_state,
        rm::ScaleUpFabricControlPlaneState::Configured
    );

    assert_eq!(
        observation.health.controller,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );
}

#[tokio::test]
async fn unknown_raw_states_stay_unknown_and_unhealthy_overrides_unknown() {
    let mut client = healthy_client();

    let domain = scripted_response_mut(&mut client.domain_state_results);

    domain.control_plane_state = 999;
    domain.nmx_controller_health = 999;

    let topology = scripted_response_mut(&mut client.topology_results);

    for device in &mut topology.device_topo_info {
        let ports = match device.device.as_mut().unwrap() {
            nmx::nmx_device_topo_info::Device::SwitchTopoInfo(switch) => {
                switch.device_health = 999;
                &mut switch.port_topo_info
            }
            nmx::nmx_device_topo_info::Device::GpuTopoInfo(gpu) => {
                gpu.device_health = 999;
                &mut gpu.port_topo_info
            }
        };

        ports.first_mut().unwrap().physical_state = 999;
    }

    let observation = observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

    assert_eq!(
        observation.control_plane_state,
        rm::ScaleUpFabricControlPlaneState::Unknown
    );

    assert_eq!(
        observation.health.controller,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.compute,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.switches,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.links,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        aggregate_health([
            rm::ScaleUpFabricReportedStatus::Unknown,
            rm::ScaleUpFabricReportedStatus::Abnormal
        ]),
        rm::ScaleUpFabricReportedStatus::Abnormal
    );
}

#[tokio::test]
async fn failed_reads_leave_component_status_unknown() {
    let mut client = FakeNmxc::default();

    let observation = observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

    assert_eq!(
        observation.control_plane_state,
        rm::ScaleUpFabricControlPlaneState::Unknown
    );

    assert_eq!(
        observation.health.controller,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.compute,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.switches,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.links,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );
}

#[tokio::test]
async fn missing_topology_preserves_node_failures_without_reporting_healthy_links() {
    use rm::ScaleUpFabricReportedStatus as Health;

    for (raw, expected_health) in [
        (Some(1), Health::Unknown),
        (Some(2), Health::Abnormal),
        (Some(0), Health::Unknown),
        (Some(i32::MAX), Health::Unknown),
        (None, Health::Unknown),
    ] {
        let mut client = healthy_client();

        client.topology_results.clear();

        if let Some(raw) = raw {
            let switches = scripted_response_mut(&mut client.switch_node_results);

            let computes = scripted_response_mut(&mut client.compute_node_results);

            let mut switch = switches.node_info_list[0].clone();
            let mut compute = computes.node_info_list[0].clone();

            switch.node_health = raw;
            compute.node_health = raw;
            switches.node_info_list.push(switch);
            computes.node_info_list.push(compute);

            // A failure without a physical identity is not an observed node.
            switches.node_info_list.push(nmx::NmxSwitchNodeInfo {
                node_health: 2,
                ..Default::default()
            });

            computes.node_info_list.push(nmx::NmxComputeNodeInfo {
                node_health: 2,
                ..Default::default()
            });
        } else {
            client.switch_node_results.clear();
            client.compute_node_results.clear();
        }

        let observation =
            observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

        assert_eq!(observation.health.compute, expected_health as i32);
        assert_eq!(observation.health.switches, expected_health as i32);
        assert_eq!(observation.health.links, Health::Unknown as i32);
    }
}

#[tokio::test]
async fn node_health_sources_remain_independent_from_healthy_device_topology() {
    let mut client = healthy_client();

    scripted_response_mut(&mut client.switch_node_results)
        .node_info_list
        .first_mut()
        .unwrap()
        .node_health = 2;

    scripted_response_mut(&mut client.compute_node_results)
        .node_info_list
        .first_mut()
        .unwrap()
        .node_health = 3;

    let observation = observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

    assert_eq!(
        observation.health.controller,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );

    assert_eq!(
        observation.health.compute,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );

    assert_eq!(
        observation.health.switches,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );

    assert_eq!(
        observation.health.links,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );
}

#[tokio::test]
async fn missing_node_health_reads_are_unavailable_and_unhealthy_devices_take_precedence() {
    for unhealthy in [false, true] {
        let mut client = healthy_client();

        client.switch_node_results.clear();
        client.compute_node_results.clear();

        let topology = scripted_response_mut(&mut client.topology_results);

        let device_health = if unhealthy { 3 } else { 1 };

        for device in &mut topology.device_topo_info {
            match device.device.as_mut().unwrap() {
                nmx::nmx_device_topo_info::Device::SwitchTopoInfo(switch) => {
                    switch.device_health = device_health
                }
                nmx::nmx_device_topo_info::Device::GpuTopoInfo(gpu) => {
                    gpu.device_health = device_health
                }
            }
        }

        let observation =
            observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

        let expected = if unhealthy {
            rm::ScaleUpFabricReportedStatus::Abnormal
        } else {
            rm::ScaleUpFabricReportedStatus::Unknown
        };

        assert_eq!(observation.health.compute, expected as i32);
        assert_eq!(observation.health.switches, expected as i32);

        assert_eq!(
            observation.health.links,
            rm::ScaleUpFabricReportedStatus::Normal as i32
        );
    }
}

#[tokio::test]
async fn domain_failure_does_not_replace_independent_available_device_health() {
    let mut client = healthy_client();

    client.domain_state_results.clear();

    let observation = observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

    assert_eq!(
        observation.control_plane_state,
        rm::ScaleUpFabricControlPlaneState::Unknown
    );

    assert_eq!(
        observation.health.controller,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.compute,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );

    assert_eq!(
        observation.health.switches,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );

    assert_eq!(
        observation.health.links,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );
}

#[tokio::test]
async fn every_node_health_row_at_one_live_physical_position_is_aggregated() {
    let mut client = healthy_client();

    let nodes = &mut scripted_response_mut(&mut client.compute_node_results).node_info_list;

    let mut unhealthy = nodes.first().unwrap().clone();

    unhealthy.node_health = 3;
    nodes.push(unhealthy);

    let observation = observe_samples(&mut client, &[expected(5)], &[expected(1)], "gateway").await;

    assert_eq!(
        observation.health.compute,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );
}
