/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Live inspection regression tests.

use std::collections::VecDeque;

use super::*;
use crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2::observations::FabricSamples;
use crate::libnmxc::nmxc_model as nmx;
use crate::libnmxc::test_support::{FakeNmxc, fabric_responses};

use tokio::time::Instant;

fn inspection_request() -> rm::InspectScaleUpFabricsRequest {
    rm::InspectScaleUpFabricsRequest {
        nodes: Some(rm::NodeSet {
            nodes: [
                ("s1", rm::NodeType::SwitchGb200Nvidia),
                ("c1", rm::NodeType::ComputeGb200Nvidia),
                ("s2", rm::NodeType::SwitchGb200Nvidia),
                ("c2", rm::NodeType::ComputeGb200Nvidia),
            ]
            .into_iter()
            .map(|(node_id, node_type)| rm::NodeInfo {
                node_id: node_id.into(),
                r#type: Some(node_type.into()),
                ..Default::default()
            })
            .collect(),
        }),
        ..Default::default()
    }
}

#[test]
fn projection_preserves_members_and_source_errors() {
    let switches = vec![switch_observation("s1", Some(true), 5)];
    let mut compute = compute_observation("c1", 1);

    compute.reads.insert(
        "identity".into(),
        ReadEvidence {
            collected: false,
            errors: vec!["read failed".into()],
        },
    );

    let mut fabric = controller_observation("s1");

    apply_samples(
        &mut fabric,
        &hardware_samples("rack-A", 5, 2, &[("rack-A", 1)]),
        switches.iter(),
        std::slice::from_ref(&compute),
    );

    let mut compact = rm::InspectScaleUpFabricsResponse::default();
    projection::project_observation(&mut compact, vec![fabric], switches, vec![compute]);

    let observed = &compact.fabrics[0];

    assert_eq!(observed.switch_node_ids, ["s1"]);
    assert_eq!(observed.compute_node_ids, ["c1"]);
    assert_eq!(observed.primary_switch_node_id, "s1");

    assert_eq!(
        observed.status,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );

    assert!(
        observed
            .inspection_errors
            .contains(&rm::ScaleUpFabricInspectionError {
                node_id: "c1".into(),
                message: "identity: read failed".into(),
            })
    );
}

fn compute_observation(node_id: &str, tray: u32) -> ComputeEvidence {
    ComputeEvidence {
        node_id: node_id.into(),
        location: switch_observation(node_id, None, tray).location,
        reads: [(
            "identity".into(),
            ReadEvidence {
                collected: true,
                ..Default::default()
            },
        )]
        .into(),
    }
}

fn switch_observation(node_id: &str, enabled: Option<bool>, tray: u32) -> SwitchEvidence {
    SwitchEvidence {
        node_id: node_id.into(),
        enabled,
        reads: [(
            "cluster".into(),
            ReadEvidence {
                collected: enabled.is_some(),
                ..Default::default()
            },
        )]
        .into(),
        location: Some(ObservedIdentity {
            node_id: node_id.into(),
            chassis_serial_number: "rack-A".into(),
            tray_index: tray,
        }),
    }
}

fn hardware_samples(
    serial: &str,
    tray: u64,
    health: i32,
    computes: &[(&str, u64)],
) -> FabricSamples {
    let location = nmx::NmxLocationInfo {
        chassis_serial_number: serial.into(),
        tray_index: tray,
        location: None,
    };

    let gpus = computes
        .iter()
        .map(|(serial, tray)| nmx::NmxGpuTopoInfo {
            loc: Some(nmx::NmxLocationInfo {
                chassis_serial_number: (*serial).into(),
                tray_index: *tray,
                location: None,
            }),
            device_health: 1,
            ..Default::default()
        })
        .collect();

    let responses = fabric_responses(location, &[42], health, gpus, None);

    FabricSamples {
        domain: Ok(responses.domain),
        topology: Ok(responses.topology),
        switch_nodes: Ok(responses.switches),
        compute_nodes: Ok(responses.computes),
    }
}

#[test]
fn unidentified_node_faults_require_positive_counts_and_keep_resolved_members() {
    use rm::ScaleUpFabricReportedStatus as Status;

    for (is_switch, resource) in [(true, "switch-nodes"), (false, "compute-nodes")] {
        for count in [0, 1] {
            let mut samples = hardware_samples("rack-A", 5, 1, &[("rack-A", 1)]);

            if is_switch {
                samples.switch_nodes.as_mut().unwrap().node_info_list.push(
                    nmx::NmxSwitchNodeInfo {
                        num_switches: count,
                        node_health: nmx::SwitchNodeHealth::NmxSwitchNodeHealthUnhealthy as i32,
                        ..Default::default()
                    },
                );
            } else {
                samples.compute_nodes.as_mut().unwrap().node_info_list.push(
                    nmx::NmxComputeNodeInfo {
                        num_gpus: count,
                        node_health: nmx::ComputeNodeHealth::NmxComputeNodeHealthUnhealthy as i32,
                        ..Default::default()
                    },
                );
            }

            let switches = vec![switch_observation("s1", Some(true), 5)];
            let computes = vec![compute_observation("c1", 1)];
            let mut fabric = controller_observation("s1");

            apply_samples(&mut fabric, &samples, switches.iter(), &computes);

            let mut response = rm::InspectScaleUpFabricsResponse::default();
            projection::project_observation(&mut response, vec![fabric], switches, computes);

            let observed = &response.fabrics[0];
            let components = observed.component_status.as_ref().unwrap();

            let expected = if count == 0 {
                Status::Normal
            } else {
                Status::Abnormal
            } as i32;

            assert_eq!(observed.status, expected);

            assert_eq!(
                if is_switch {
                    components.switches
                } else {
                    components.compute
                },
                expected
            );

            assert_eq!(observed.switch_node_ids, ["s1"]);
            assert_eq!(observed.compute_node_ids, ["c1"]);

            assert_eq!(
                observed.control_plane_state,
                rm::ScaleUpFabricControlPlaneState::Configured as i32
            );

            assert_eq!(observed.inspection_errors.len(), usize::from(count != 0));

            assert!(
                observed
                    .inspection_errors
                    .iter()
                    .all(|error| error.node_id == "s1" && error.message.starts_with(resource))
            );

            assert!(response.discovery_errors.is_empty());
        }
    }
}

#[test]
fn port_shortfalls_report_unknown_without_hiding_observed_link_faults() {
    use nmx::nmx_device_topo_info::Device;
    use rm::ScaleUpFabricReportedStatus as Status;

    let cases = [
        (0, 1, false),
        (1, 1, false),
        (2, 1, false),
        (1, 0, false),
        (2, 1, true),
        (1, 0, true),
    ];

    for (is_switch, (count, listed, fault)) in [false, true]
        .into_iter()
        .flat_map(|is_switch| cases.map(|case| (is_switch, case)))
    {
        let mut samples = hardware_samples("rack-A", 5, 1, &[("rack-A", 1)]);
        let topology = samples.topology.as_mut().unwrap();

        for device in &mut topology.device_topo_info {
            let ports = match device.device.as_mut().unwrap() {
                Device::SwitchTopoInfo(device) => &mut device.port_topo_info,
                Device::GpuTopoInfo(device) => &mut device.port_topo_info,
            };

            *ports = vec![nmx::NmxPortTopoInfo {
                port_type: nmx::PortType::NmxPortTypeGpu as i32,
                port_admin_state: nmx::AdminState::NmxAdminStateUp as i32,
                physical_state: nmx::PhysicalPortState::NmxPortPhysStateLinkup as i32,
                logical_state: if fault {
                    nmx::LogicalLinkState::NmxLogicalLinkStateDown
                } else {
                    nmx::LogicalLinkState::NmxLogicalLinkStateActive
                } as i32,
                ..Default::default()
            }];
        }

        let (num_ports, ports) = topology
            .device_topo_info
            .iter_mut()
            .find_map(|entry| match entry.device.as_mut().unwrap() {
                Device::SwitchTopoInfo(device) if is_switch => {
                    Some((&mut device.num_ports, &mut device.port_topo_info))
                }
                Device::GpuTopoInfo(device) if !is_switch => {
                    Some((&mut device.num_ports, &mut device.port_topo_info))
                }
                _ => None,
            })
            .unwrap();

        *num_ports = count;
        ports.truncate(listed);

        let switches = vec![switch_observation("s1", Some(true), 5)];
        let computes = vec![compute_observation("c1", 1)];
        let mut fabric = controller_observation("s1");

        apply_samples(&mut fabric, &samples, switches.iter(), &computes);

        let mut response = rm::InspectScaleUpFabricsResponse::default();
        projection::project_observation(&mut response, vec![fabric], switches, computes);

        let observed = &response.fabrics[0];
        let shortfall = count > listed as u64;

        let expected = if fault {
            Status::Abnormal
        } else if shortfall {
            Status::Unknown
        } else {
            Status::Normal
        } as i32;

        assert_eq!(observed.status, expected);
        assert_eq!(observed.component_status.as_ref().unwrap().links, expected);
        assert_eq!(observed.switch_node_ids, ["s1"]);
        assert_eq!(observed.compute_node_ids, ["c1"]);

        assert_eq!(
            observed.control_plane_state,
            rm::ScaleUpFabricControlPlaneState::Configured as i32
        );

        assert_eq!(observed.inspection_errors.len(), usize::from(shortfall));

        assert!(
            observed
                .inspection_errors
                .iter()
                .all(|error| error.node_id == "s1" && error.message.starts_with("topology:"))
        );

        assert!(response.discovery_errors.is_empty());
    }
}

#[test]
fn read_results_distinguish_successful_empty_values_from_failed_reads() {
    let mut reads = HashMap::new();

    let empty = collect_read(
        Ok(Ok::<_, &str>(Vec::<String>::new())),
        "switch-nodes",
        &mut reads,
    );

    let failed = collect_read(
        Ok(Err::<Vec<String>, _>("private transport diagnostic")),
        "topology",
        &mut reads,
    );

    assert_eq!(empty, Some(Vec::new()));
    assert_eq!(failed, None);

    assert!(reads["switch-nodes"].collected);
    assert!(reads["switch-nodes"].errors.is_empty());

    assert!(!reads["topology"].collected);
    assert_eq!(reads["topology"].errors, ["resource read failed"]);
    assert!(!reads.contains_key("controller-state"));
}

#[test]
fn validation_rejects_missing_switches_and_invalid_inventory() {
    type Mutation = fn(&mut rm::InspectScaleUpFabricsRequest);

    let cases: [(Mutation, &str); 4] = [
        (
            |r| r.nodes = None,
            "inspection requires at least one switch",
        ),
        (
            |r| r.nodes.as_mut().unwrap().nodes[1].node_id = "s1".into(),
            "inventory node IDs must be nonempty and unique",
        ),
        (
            |r| r.nodes.as_mut().unwrap().nodes[0].node_id = " ".into(),
            "inventory node IDs must be nonempty and unique",
        ),
        (
            |r| r.nodes.as_mut().unwrap().nodes[2].node_id = "s1".into(),
            "inventory node IDs must be nonempty and unique",
        ),
    ];

    for (mutate, expected) in cases {
        let mut request = inspection_request();
        mutate(&mut request);
        assert_eq!(validate_inspection(&request).unwrap_err(), expected);
    }
}

#[test]
fn compute_inventory_is_optional() {
    let mut request = inspection_request();

    assert!(validate_inspection(&request).is_ok());
    request
        .nodes
        .as_mut()
        .unwrap()
        .nodes
        .retain(|node| node.r#type == Some(rm::NodeType::SwitchGb200Nvidia.into()));

    assert!(validate_inspection(&request).is_ok());
}

#[test]
fn mixed_inventory_preserves_order_within_each_role() {
    let request = inspection_request();

    let (switches, computes) = validate_inspection(&request).unwrap();

    assert_eq!(
        switches
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<Vec<_>>(),
        ["s1", "s2"]
    );

    assert_eq!(
        computes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<Vec<_>>(),
        ["c1", "c2"]
    );
}

#[test]
fn inspection_rejects_non_fabric_roles() {
    let mut request = inspection_request();
    request.nodes.as_mut().unwrap().nodes[1].r#type =
        Some(rm::NodeType::PowershelfGb200Liteon.into());

    assert_eq!(
        validate_inspection(&request).unwrap_err(),
        "inspection accepts only switch and compute nodes"
    );
}

#[test]
fn health_requires_complete_disjoint_controller_observations() {
    use rm::ScaleUpFabricReportedStatus as Health;

    for (overlap, unknown_switch, healthy) in [
        (false, false, true),
        (true, false, false),
        (false, true, false),
    ] {
        let mut switches = vec![
            switch_observation("s1", Some(true), 5),
            switch_observation("s2", Some(true), 6),
        ];

        if unknown_switch {
            switches.push(switch_observation("s3", None, 7));
        }

        let computes = vec![compute_observation("c1", 1), compute_observation("c2", 2)];
        let mut fabrics = Vec::new();
        for (primary, switch_tray, compute_tray) in
            [("s1", 5, 1), ("s2", 6, if overlap { 1 } else { 2 })]
        {
            let mut fabric = controller_observation(primary);

            apply_samples(
                &mut fabric,
                &hardware_samples("rack-A", switch_tray, 1, &[("rack-A", compute_tray)]),
                switches.iter(),
                &computes,
            );

            fabrics.push(fabric);
        }

        let mut observation = rm::InspectScaleUpFabricsResponse::default();
        projection::project_observation(&mut observation, fabrics, switches, computes);
        assert_eq!(observation.fabrics.len(), 2);

        for fabric in observation.fabrics {
            assert_eq!(
                fabric.status,
                if healthy {
                    Health::Normal
                } else {
                    Health::Unknown
                } as i32
            );

            assert_eq!(fabric.inspection_errors.is_empty(), healthy);
        }
    }
}

#[test]
fn unassociated_node_errors_do_not_erase_live_health() {
    let switches = vec![switch_observation("s1", Some(true), 5)];
    let mut computes = vec![compute_observation("c1", 1), compute_observation("c2", 2)];
    computes[1].location = None;

    record_read_error(&mut computes[1].reads, "identity", "resource read failed");

    let mut fabric = controller_observation("s1");

    apply_samples(
        &mut fabric,
        &hardware_samples("rack-A", 5, 1, &[("rack-A", 1)]),
        switches.iter(),
        &computes,
    );

    let mut observation = rm::InspectScaleUpFabricsResponse::default();
    projection::project_observation(&mut observation, vec![fabric], switches, computes);

    let fabric = &observation.fabrics[0];

    assert_eq!(
        fabric.status,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );

    assert_eq!(fabric.compute_node_ids, ["c1"]);
    assert!(fabric.inspection_errors.is_empty());

    assert_eq!(
        observation.discovery_errors,
        [rm::ScaleUpFabricInspectionError {
            node_id: "c2".into(),
            message: "identity: resource read failed".into(),
        }]
    );
}

#[test]
fn successful_unhealthy_samples_have_no_collection_issues() {
    let switches = vec![switch_observation("s1", Some(true), 5)];
    let computes = vec![compute_observation("c1", 1)];
    let mut fabric = controller_observation("s1");

    apply_samples(
        &mut fabric,
        &hardware_samples("rack-A", 5, 2, &[("rack-A", 1)]),
        switches.iter(),
        &computes,
    );

    let mut observation = rm::InspectScaleUpFabricsResponse::default();
    projection::project_observation(&mut observation, vec![fabric], switches, computes);

    assert_eq!(
        observation.fabrics[0].status,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );

    assert!(observation.fabrics[0].inspection_errors.is_empty());
    assert!(observation.discovery_errors.is_empty());
}

#[test]
fn failed_discovery_does_not_look_like_successful_empty_discovery() {
    for enabled in [Some(false), None] {
        let switches = vec![switch_observation("s1", enabled, 5)];
        let mut observation = rm::InspectScaleUpFabricsResponse::default();

        projection::project_observation(&mut observation, vec![], switches, vec![]);

        assert!(observation.fabrics.is_empty());
        assert_eq!(observation.discovery_errors.is_empty(), enabled.is_some());

        if enabled.is_none() {
            assert_eq!(observation.discovery_errors[0].node_id, "s1");
            assert!(observation.discovery_errors[0].message.contains("cluster"));
        }
    }
}

#[test]
fn unresolved_physical_identities_retain_partial_membership_and_errors() {
    let switch = switch_observation("s1", Some(true), 5);
    for (serial, tray, resolved) in [
        ("rack-A", 5, true),
        ("rack-B", 5, false),
        ("", u64::MAX, false),
    ] {
        let samples = hardware_samples(serial, tray, 1, &[]);
        let mut observed = controller_observation("s1");

        apply_samples(&mut observed, &samples, std::iter::once(&switch), &[]);
        assert_eq!(!observed.observation.switch_node_ids.is_empty(), resolved);
        assert_eq!(observed.reads["topology"].errors.is_empty(), resolved);

        assert!(observed.reads["topology"].collected);
    }
}

#[test]
fn configured_does_not_imply_healthy_when_evidence_is_missing() {
    use rm::ScaleUpFabricReportedStatus as Health;

    for missing in [
        "compute-inventory",
        "gpu-topology",
        "topology",
        "controller",
        "device-kind",
    ] {
        let switches = vec![switch_observation("s1", Some(true), 5)];

        let mut computes = vec![compute_observation("c1", 1)];
        let mut samples = hardware_samples("rack-A", 5, 1, &[("rack-A", 1)]);

        match missing {
            "compute-inventory" => computes.clear(),
            "gpu-topology" => samples = hardware_samples("rack-A", 5, 1, &[]),
            "topology" => samples.topology = Err("topology unavailable".into()),
            "controller" => samples.domain = Err("controller unavailable".into()),
            "device-kind" => samples
                .topology
                .as_mut()
                .unwrap()
                .device_topo_info
                .push(nmx::NmxDeviceTopoInfo { device: None }),
            _ => unreachable!(),
        }

        let mut fabric = controller_observation("s1");

        apply_samples(&mut fabric, &samples, switches.iter(), &computes);

        let mut observation = rm::InspectScaleUpFabricsResponse::default();

        projection::project_observation(&mut observation, vec![fabric], switches, computes);

        assert_eq!(
            observation.fabrics[0].status,
            Health::Unknown as i32,
            "{missing}"
        );
    }
}

#[test]
fn compute_ids_are_observed_deduplicated_and_never_inferred_from_inventory() {
    let switch = switch_observation("s1", Some(true), 5);
    let computes = [compute_observation("c1", 1), compute_observation("c2", 2)];
    for (devices, expected, complete) in [
        (vec![("rack-A", 1), ("rack-A", 1)], vec!["c1"], true),
        (vec![], vec![], true),
        (vec![("rack-A", 1), ("rack-A", 9)], vec!["c1"], false),
        (vec![("", u64::MAX)], vec![], false),
    ] {
        let mut observed = controller_observation("s1");

        apply_samples(
            &mut observed,
            &hardware_samples("rack-A", 5, 1, &devices),
            std::iter::once(&switch),
            &computes,
        );

        assert_eq!(observed.observation.compute_node_ids, expected);
        assert_eq!(observed.reads["topology"].errors.is_empty(), complete);
    }

    let mut observed = controller_observation("s1");

    apply_samples(
        &mut observed,
        &hardware_samples("rack-A", 5, 1, &[("rack-A", 1)]),
        std::iter::once(&switch),
        &[],
    );

    assert!(observed.observation.compute_node_ids.is_empty());
    assert!(!observed.reads["topology"].errors.is_empty());
}

#[test]
fn duplicate_physical_identities_are_not_assigned_arbitrarily() {
    let switches = [
        switch_observation("s1", Some(true), 5),
        switch_observation("alias", Some(false), 5),
    ];

    let mut observed = controller_observation("s1");

    apply_samples(
        &mut observed,
        &hardware_samples("rack-A", 5, 1, &[]),
        switches.iter(),
        &[],
    );

    assert!(observed.observation.switch_node_ids.is_empty());
    assert!(!observed.reads["topology"].errors.is_empty());
}

#[test]
fn contradictory_node_reports_affect_health_without_changing_membership() {
    let switch = switch_observation("s1", Some(true), 5);

    let mut samples = hardware_samples("rack-A", 5, 1, &[]);
    let nodes = &mut samples.switch_nodes.as_mut().unwrap().node_info_list;
    let mut contradictory = nodes[0].clone();
    contradictory.node_health = 3;
    nodes.push(contradictory);

    let mut observed = controller_observation("s1");

    apply_samples(&mut observed, &samples, std::iter::once(&switch), &[]);

    assert_eq!(observed.observation.switch_node_ids, ["s1"]);

    assert_eq!(
        observed
            .observation
            .component_status
            .as_ref()
            .unwrap()
            .switches,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );
}

#[test]
fn unknown_controller_and_link_states_remain_observable() {
    let switch = switch_observation("s1", Some(true), 5);

    let mut samples = hardware_samples("rack-A", 5, 1, &[("rack-A", 1)]);
    samples.domain.as_mut().unwrap().control_plane_state = 901;
    samples.domain.as_mut().unwrap().nmx_controller_health = 902;

    let Some(nmx::nmx_device_topo_info::Device::SwitchTopoInfo(switch_topology)) =
        samples.topology.as_mut().unwrap().device_topo_info[0]
            .device
            .as_mut()
    else {
        panic!("fixture requires switch topology");
    };

    switch_topology.port_topo_info[0].logical_state = 903;

    let mut observed = controller_observation("s1");

    apply_samples(&mut observed, &samples, std::iter::once(&switch), &[]);

    assert_eq!(
        observed.observation.control_plane_state,
        rm::ScaleUpFabricControlPlaneState::Unknown as i32
    );

    assert_eq!(
        observed
            .observation
            .component_status
            .as_ref()
            .unwrap()
            .controller,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert_eq!(
        observed
            .observation
            .component_status
            .as_ref()
            .unwrap()
            .links,
        rm::ScaleUpFabricReportedStatus::Unknown as i32
    );

    assert!(observed.reads["controller-state"].collected);
}

#[test]
fn failed_controller_reads_retain_successful_membership_and_link_samples() {
    let switch = switch_observation("s1", Some(true), 5);

    let mut samples = hardware_samples("rack-A", 5, 1, &[]);
    samples.domain = Err("controller state and health read unavailable".into());
    samples.switch_nodes = Err("switch health read unavailable".into());

    let mut observed = controller_observation("s1");

    apply_samples(&mut observed, &samples, std::iter::once(&switch), &[]);

    assert!(!observed.reads["controller-state"].collected);

    assert!(observed.reads["topology"].collected);

    assert!(!observed.reads["switch-nodes"].collected);

    assert!(observed.reads["compute-nodes"].collected);

    assert_eq!(observed.observation.switch_node_ids, ["s1"]);

    assert_eq!(
        observed
            .observation
            .component_status
            .as_ref()
            .unwrap()
            .links,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );

    assert_eq!(
        observed
            .reads
            .values()
            .map(|read| read.errors.len())
            .sum::<usize>(),
        2
    );

    let mut projected = rm::InspectScaleUpFabricsResponse::default();

    projection::project_observation(&mut projected, vec![observed], vec![switch], vec![]);

    assert_eq!(
        projected.fabrics[0].inspection_errors,
        [
            rm::ScaleUpFabricInspectionError {
                node_id: "s1".into(),
                message: "controller-state: controller state and health read unavailable".into(),
            },
            rm::ScaleUpFabricInspectionError {
                node_id: "s1".into(),
                message: "switch-nodes: switch health read unavailable".into(),
            },
        ]
    );
}

#[tokio::test]
async fn live_samples_establish_health_without_static_configuration() {
    let expected = hardware_samples("rack-A", 5, 1, &[("rack-A", 1)]);

    let mut client = FakeNmxc {
        domain_state_results: VecDeque::from([Ok(expected.domain.unwrap())]),
        topology_results: VecDeque::from([Ok(expected.topology.unwrap())]),
        switch_node_results: VecDeque::from([Ok(expected.switch_nodes.unwrap())]),
        compute_node_results: VecDeque::from([Ok(expected.compute_nodes.unwrap())]),
        ..Default::default()
    };

    let samples = sample_group(&mut client, "gateway", None).await;
    let switches = vec![switch_observation("s1", Some(true), 5)];
    let computes = vec![compute_observation("c1", 1)];
    let mut fabric = controller_observation("s1");

    apply_samples(&mut fabric, &samples, switches.iter(), &computes);

    let mut observation = rm::InspectScaleUpFabricsResponse::default();
    projection::project_observation(&mut observation, vec![fabric], switches, computes);

    assert_eq!(
        observation.fabrics[0].status,
        rm::ScaleUpFabricReportedStatus::Normal as i32
    );

    assert!(observation.fabrics[0].inspection_errors.is_empty());
    assert!(observation.discovery_errors.is_empty());
    assert!(client.get_static_config_file_calls.is_empty());
    assert!(client.set_calls.is_empty());
    assert!(client.set_file_calls.is_empty());
}

#[tokio::test(start_paused = true)]
async fn deadline_uses_each_grpc_unit_and_never_exceeds_server_budget() {
    let cases = [
        ("1H", Duration::from_secs(60)),
        ("1M", Duration::from_secs(54)),
        ("2S", Duration::from_millis(1750)),
        ("300m", Duration::from_millis(50)),
        ("400000u", Duration::from_millis(150)),
        ("3m", Duration::ZERO),
        ("4u", Duration::ZERO),
        ("5n", Duration::ZERO),
        ("250m", Duration::ZERO),
        ("251m", Duration::from_millis(1)),
        ("50000000n", Duration::ZERO),
        ("0S", Duration::ZERO),
        ("99999999H", Duration::from_secs(60)),
    ];

    for (value, budget) in cases {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("grpc-timeout", value.parse().unwrap());

        let result = inspection_deadline(&metadata);

        if budget.is_zero() {
            assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
            continue;
        }

        let deadline = result.unwrap();

        assert_eq!(deadline.duration_since(Instant::now()), budget, "{value}");
    }

    let deadline = inspection_deadline(&tonic::metadata::MetadataMap::new()).unwrap();

    assert_eq!(deadline.duration_since(Instant::now()), INSPECTION_TIMEOUT);
}

#[test]
fn malformed_ascii_and_multibyte_deadlines_return_errors_without_panicking() {
    let cases: &[&[u8]] = &[
        b"",
        b"S",
        b"1",
        b"1x",
        b"123456789S",
        b"-1S",
        b"+1S",
        b"1.1S",
        b" 1S",
        &[0xc3, 0xa9],
        &[b'1', 0xc3, 0xa9],
    ];

    for value in cases {
        let mut metadata = tonic::metadata::MetadataMap::new();

        metadata.insert(
            "grpc-timeout",
            tonic::metadata::MetadataValue::try_from(*value).unwrap(),
        );

        let result = inspection_deadline(&metadata);

        assert_eq!(
            result.unwrap_err().code(),
            tonic::Code::InvalidArgument,
            "{value:?}"
        );
    }
}
