/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Physical membership and incomplete-discovery regression tests.

use super::*;

#[tokio::test]
async fn invalid_expected_membership_performs_no_hardware_reads() {
    let mut blank = expected(1);
    blank.chassis_serial_number = " ".into();

    for (switches, computes) in [
        (vec![], vec![]),
        (vec![blank.clone()], vec![]),
        (vec![expected(5)], vec![blank]),
    ] {
        let mut client = healthy_client();

        let error = observe_group(&mut client, &switches, &computes, "gateway")
            .await
            .err()
            .expect("unknown physical membership must be rejected");

        assert_eq!(
            error,
            "group observation requires at least one switch and known physical identities for all switches and computes"
        );

        assert!(client.domain_state_calls.is_empty());
        assert!(client.topology_calls.is_empty());
        assert!(client.switch_node_calls.is_empty());
        assert!(client.compute_node_calls.is_empty());
    }
}

#[tokio::test]
async fn duplicate_expected_identities_perform_no_hardware_reads() {
    for duplicate_switch in [true, false] {
        let mut switches = vec![expected(5)];
        let mut computes = vec![expected(1)];

        let nodes = if duplicate_switch {
            &mut switches
        } else {
            &mut computes
        };

        let mut alias = nodes[0].clone();
        alias.node_id = "another-label".into();
        nodes.push(alias);

        let mut client = healthy_client();

        let error = observe_group(&mut client, &switches, &computes, "gateway")
            .await
            .err()
            .expect("duplicate identities must fail before sampling");

        assert_eq!(
            error,
            "group observation contains duplicate expected physical identities"
        );

        assert!(client.domain_state_calls.is_empty());
        assert!(client.topology_calls.is_empty());
        assert!(client.switch_node_calls.is_empty());
        assert!(client.compute_node_calls.is_empty());
    }
}

#[tokio::test]
async fn readiness_and_healthy_membership_accept_multiple_asics_at_one_switch_position() {
    let mut client = healthy_client();

    let observation = observe_group(&mut client, &[expected(5)], &[expected(1)], "gateway")
        .await
        .unwrap();

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

    assert!(observation.membership_observed);
    assert!(observation.membership_matches);
    assert_eq!(client.domain_state_calls, ["gateway"]);
    assert_eq!(client.topology_calls, ["gateway"]);
    assert_eq!(client.switch_node_calls, ["gateway"]);
    assert_eq!(client.compute_node_calls, ["gateway"]);
}

#[tokio::test]
async fn offline_node_rows_do_not_establish_membership_or_healthy_switches() {
    let placeholder = nmx::NmxSwitchNodeInfo {
        loc: Some(nmx::NmxLocationInfo {
            chassis_serial_number: String::new(),
            tray_index: u64::MAX,
            location: None,
        }),
        num_switches: 2,
        node_health: 1,
    };

    let mut client = healthy_client();

    scripted_response_mut(&mut client.switch_node_results).node_info_list = vec![placeholder; 9];

    scripted_response_mut(&mut client.topology_results)
        .device_topo_info
        .retain(|device| {
            matches!(
                device.device,
                Some(nmx::nmx_device_topo_info::Device::SwitchTopoInfo(_))
            )
        });

    let observation = observe_group(&mut client, &[expected(5)], &[expected(1)], "gateway")
        .await
        .unwrap();

    assert!(observation.membership_matches);

    assert_eq!(
        observation.control_plane_state,
        rm::ScaleUpFabricControlPlaneState::Configured
    );

    assert_eq!(
        observation.health.controller,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );

    assert_eq!(
        observation.health.switches,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.compute,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );
}

#[tokio::test]
async fn missing_or_foreign_or_unknown_live_switch_positions_do_not_match() {
    for location in [
        None,
        Some(nmx::NmxLocationInfo {
            chassis_serial_number: "foreign".into(),
            tray_index: 5,
            location: None,
        }),
        Some(nmx::NmxLocationInfo {
            chassis_serial_number: "rack-A01".into(),
            tray_index: u64::MAX,
            location: None,
        }),
    ] {
        let mut client = healthy_client();

        let topology = scripted_response_mut(&mut client.topology_results);

        for device in &mut topology.device_topo_info {
            if let Some(nmx::nmx_device_topo_info::Device::SwitchTopoInfo(switch)) =
                &mut device.device
            {
                switch.loc = location.clone();
            }
        }

        let observation = observe_group(&mut client, &[expected(5)], &[expected(1)], "gateway")
            .await
            .unwrap();

        assert!(observation.membership_observed);
        assert!(!observation.membership_matches);
        assert!(observation.errors.join("; ").contains("membership"));
    }

    let mut client = healthy_client();

    scripted_response_mut(&mut client.topology_results)
        .device_topo_info
        .clear();

    let observation = observe_group(&mut client, &[expected(5)], &[expected(1)], "gateway")
        .await
        .unwrap();

    assert!(!observation.membership_matches);
}

#[tokio::test]
async fn foreign_or_unknown_gpu_locations_fail_only_with_expected_compute_membership() {
    for location in [
        None,
        Some(nmx::NmxLocationInfo {
            chassis_serial_number: "rack-A01".into(),
            tray_index: 99,
            location: None,
        }),
    ] {
        for computes in [vec![expected(1)], Vec::new()] {
            let mut client = healthy_client();

            let topology = scripted_response_mut(&mut client.topology_results);

            let gpu = topology
                .device_topo_info
                .iter_mut()
                .find_map(|device| match device.device.as_mut() {
                    Some(nmx::nmx_device_topo_info::Device::GpuTopoInfo(gpu)) => Some(gpu),
                    _ => None,
                })
                .unwrap();

            gpu.loc = location.clone();

            let observation = observe_group(&mut client, &[expected(5)], &computes, "gateway")
                .await
                .unwrap();

            assert_eq!(observation.membership_matches, computes.is_empty());
        }
    }
}

#[tokio::test]
async fn incomplete_gpu_discovery_is_unknown_without_using_unverified_slot_ids() {
    let mut client = healthy_client();

    let topology = scripted_response_mut(&mut client.topology_results);

    let gpu = topology
        .device_topo_info
        .iter_mut()
        .find_map(|device| match device.device.as_mut() {
            Some(nmx::nmx_device_topo_info::Device::GpuTopoInfo(gpu)) => Some(gpu),
            _ => None,
        })
        .unwrap();

    gpu.loc.as_mut().unwrap().location = Some(nmx::NmxLocation {
        slot_id: 777,
        ..Default::default()
    });

    let mut first = expected(1);

    first.position.slot_number = 9;

    let observation = observe_group(
        &mut client,
        &[expected(5)],
        &[first, expected(2)],
        "gateway",
    )
    .await
    .unwrap();

    assert!(observation.membership_matches);

    assert_eq!(
        observation.health.compute,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observation.health.controller,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );
}

#[tokio::test]
async fn failed_reads_report_unavailable_without_defaulting_to_unknown() {
    let mut client = FakeNmxc::default();

    let observation = observe_group(&mut client, &[expected(5)], &[expected(1)], "gateway")
        .await
        .unwrap();

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

    assert!(!observation.membership_observed);
    assert!(!observation.membership_matches);
    assert!(!observation.controller_observed);
    assert!(observation.errors.join("; ").contains("controller"));
    assert!(observation.errors.join("; ").contains("topology"));
    assert!(observation.errors.join("; ").contains("compute"));
    assert!(observation.errors.join("; ").contains("switch"));
}
