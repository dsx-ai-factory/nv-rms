/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Protocol health and operational port-state regression tests.

use super::*;

#[test]
fn protocol_health_and_control_plane_values_preserve_wire_meanings() {
    use rm::ScaleUpFabricControlPlaneState as ControlPlane;
    use rm::ScaleUpFabricReportedStatus as Health;

    let cases = [
        (-1, ControlPlane::Unknown, [Health::Unknown; 5]),
        (0, ControlPlane::Unknown, [Health::Unknown; 5]),
        (1, ControlPlane::Unknown, [Health::Normal; 5]),
        (2, ControlPlane::Unknown, [Health::Abnormal; 5]),
        (3, ControlPlane::Configured, [Health::Abnormal; 5]),
        (
            4,
            ControlPlane::Failed,
            [
                Health::Abnormal,
                Health::Unknown,
                Health::Unknown,
                Health::Unknown,
                Health::Abnormal,
            ],
        ),
        (5, ControlPlane::Failed, [Health::Unknown; 5]),
        (6, ControlPlane::Unknown, [Health::Unknown; 5]),
        (7, ControlPlane::Unknown, [Health::Unknown; 5]),
        (i32::MIN, ControlPlane::Unknown, [Health::Unknown; 5]),
        (i32::MAX, ControlPlane::Unknown, [Health::Unknown; 5]),
    ];

    for (raw, expected_control_plane, expected_health) in cases {
        let samples = FabricSamples {
            domain: Ok(nmx::NmxDomainStateInfo {
                control_plane_state: raw,
                nmx_controller_health: raw,
                ..Default::default()
            }),
            topology: Ok(Default::default()),
            switch_nodes: Ok(Default::default()),
            compute_nodes: Ok(Default::default()),
        };

        let observation = interpret_group(&samples, &[], &[]);

        let actual_health = [
            rm::ScaleUpFabricReportedStatus::try_from(observation.health.controller).unwrap(),
            reported_health(
                raw,
                nmx::ComputeNodeHealth::NmxComputeNodeHealthUnknown,
                nmx::ComputeNodeHealth::NmxComputeNodeHealthHealthy,
            ),
            reported_health(
                raw,
                nmx::SwitchNodeHealth::NmxSwitchNodeHealthUnknown,
                nmx::SwitchNodeHealth::NmxSwitchNodeHealthHealthy,
            ),
            reported_health(
                raw,
                nmx::SwitchHealth::NmxSwitchHealthUnknown,
                nmx::SwitchHealth::NmxSwitchHealthHealthy,
            ),
            reported_health(
                raw,
                nmx::GpuHealth::NmxGpuHealthUnknown,
                nmx::GpuHealth::NmxGpuHealthHealthy,
            ),
        ];

        assert_eq!(
            observation.control_plane_state, expected_control_plane,
            "raw={raw}"
        );

        assert_eq!(actual_health, expected_health, "raw={raw}");
    }
}

#[test]
fn protocol_port_states_preserve_health_and_exclusions() {
    use rm::ScaleUpFabricReportedStatus as Health;

    let cases = [
        (1, 1, 5, 4, Some(Health::Normal)),
        (2, 1, 5, 4, Some(Health::Normal)),
        (3, 1, 5, 4, Some(Health::Normal)),
        (4, 1, 5, 4, None),
        (5, 1, 5, 4, None),
        (0, 1, 5, 4, Some(Health::Unknown)),
        (6, 1, 5, 4, Some(Health::Unknown)),
        (2, 0, 5, 4, Some(Health::Unknown)),
        (2, 2, 1, 1, None),
        (0, 2, 0, 0, None),
        (i32::MAX, 2, 0, 0, None),
        (2, 3, 5, 4, Some(Health::Abnormal)),
        (2, 4, 5, 4, Some(Health::Unknown)),
        (2, 1, 0, 4, Some(Health::Unknown)),
        (2, 1, 1, 4, Some(Health::Abnormal)),
        (2, 1, 2, 4, Some(Health::Abnormal)),
        (2, 1, 3, 4, Some(Health::Abnormal)),
        (2, 1, 4, 4, Some(Health::Abnormal)),
        (2, 1, 6, 4, Some(Health::Abnormal)),
        (2, 1, 7, 4, Some(Health::Abnormal)),
        (2, 1, 8, 4, Some(Health::Unknown)),
        (2, 1, 5, 0, Some(Health::Unknown)),
        (2, 1, 5, 1, Some(Health::Abnormal)),
        (2, 1, 5, 2, Some(Health::Abnormal)),
        (2, 1, 5, 3, Some(Health::Abnormal)),
        (2, 1, 5, 5, Some(Health::Abnormal)),
        (2, 1, 5, 6, Some(Health::Unknown)),
        (2, 1, -1, 4, Some(Health::Unknown)),
        (2, 1, 5, -1, Some(Health::Unknown)),
        (2, 1, 0, 1, Some(Health::Abnormal)),
    ];

    for (port_type, port_admin_state, physical_state, logical_state, expected) in cases {
        let port = nmx::NmxPortTopoInfo {
            port_type,
            port_admin_state,
            physical_state,
            logical_state,
            ..Default::default()
        };

        assert_eq!(operational_link_health(&port), expected, "port={port:?}");
    }
}

#[test]
fn operational_links_exclude_intentional_down_and_preserve_unknown_states() {
    let mut port = nmx::NmxPortTopoInfo {
        port_type: 2,
        port_admin_state: 2,
        physical_state: 1,
        logical_state: 1,
        ..Default::default()
    };

    assert_eq!(operational_link_health(&port), None);

    port.port_admin_state = 999;
    port.physical_state = 5;
    port.logical_state = 4;

    assert_eq!(
        operational_link_health(&port),
        Some(rm::ScaleUpFabricReportedStatus::Unknown)
    );

    port.port_admin_state = 1;
    port.physical_state = 999;
    port.logical_state = 1;

    assert_eq!(
        operational_link_health(&port),
        Some(rm::ScaleUpFabricReportedStatus::Abnormal)
    );

    port.port_type = 4;

    assert_eq!(operational_link_health(&port), None);
}
