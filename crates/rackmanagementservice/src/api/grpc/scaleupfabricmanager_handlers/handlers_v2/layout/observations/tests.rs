/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Tests for concurrent discovery and saved link-state verification.

use super::*;

use crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2::reconciliation_tests::{
    WiremockHttpsProxy, insecure_fabric_service, non_loopback_local_ip, vrnvl72_switch,
};

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn switch_discovery_and_state_reads_validate_responses() {
    for (tray, slot, valid) in [
        ("3", Some("7"), true),
        (" 3 ", Some(" 7 "), true),
        ("3", None, false),
        ("-1", Some("7"), false),
        ("3", Some("invalid"), false),
    ] {
        let server = MockServer::start().await;
        let switch_ip = non_loopback_local_ip();
        let https = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
        let service = insecure_fabric_service();
        let node = vrnvl72_switch("caller-label", switch_ip, https.port);

        let location = Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/chassis-location"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "chassis-sn": "observed-chassis",
                "tray-index": tray,
                "slot-number": slot,
                "topology-id": "synthetic-topology",
            })))
            .expect(1)
            .mount_as_scoped(&server)
            .await;

        let result = service
            .discover_layout_switches(std::slice::from_ref(&node), None)
            .await;

        drop(location);

        if valid {
            let switches = result.unwrap();
            let placement = &switches[0].placement;

            assert_eq!(placement.node_id, "caller-label");
            assert_eq!(placement.chassis_serial_number, "observed-chassis");
            assert_eq!(placement.position.tray_index, 3);
            assert_eq!(placement.position.slot_number, 7);

            assert_eq!(
                placement.physical_topology.as_deref(),
                Some("synthetic-topology")
            );

            let cluster = Mock::given(method("GET"))
                .and(path("/nvue_v1/cluster"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "state": "unknown",
                })))
                .expect(1)
                .mount_as_scoped(&server)
                .await;

            let error = switches[0].enabled().await.unwrap_err();

            assert!(error.contains("unknown cluster state"));

            drop(cluster);

            for endpoint in [
                "/nvue_v1/platform/chassis-location",
                "/nvue_v1/cluster",
                "/nvue_v1/",
            ] {
                Mock::given(method("GET"))
                    .and(path(endpoint))
                    .respond_with(
                        ResponseTemplate::new(401).set_body_string("private response details"),
                    )
                    .expect(1)
                    .mount(&server)
                    .await;
            }

            let error = service
                .discover_layout_switches(&[node], None)
                .await
                .err()
                .expect("unauthorized discovery must fail");

            assert_eq!(
                error,
                "cannot read switch 'caller-label' placement: Unauthenticated"
            );

            assert_eq!(
                switches[0].enabled().await.unwrap_err(),
                "cannot read switch 'caller-label' cluster state: Unauthenticated"
            );

            assert_eq!(
                switches[0].startup_enabled().await.unwrap_err(),
                "cannot read switch 'caller-label' saved cluster state: Unauthenticated"
            );
        } else {
            let error = result
                .err()
                .expect("invalid coordinates must reject discovery");

            assert!(error.contains("no valid"));
        }

        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| { request.method == wiremock::http::Method::Get })
        );
    }
}

#[tokio::test]
async fn compute_discovery_validates_bmc_coordinates() {
    for (tray, slot, valid) in [
        (2, 7, true),
        (-1, 7, false),
        (2, -1, false),
        (i64::from(u32::MAX) + 1, 7, false),
        (2, i64::from(u32::MAX) + 1, false),
    ] {
        let server = MockServer::start().await;
        let ip = non_loopback_local_ip();
        let https = WiremockHttpsProxy::start(ip, *server.address()).await;
        let service = insecure_fabric_service();
        let mut node = vrnvl72_switch("compute-1", ip, https.port);
        node.r#type = Some(rm::NodeType::ComputeGb200Nvidia as i32);
        node.bmc_endpoint = node.host_endpoint.take();
        node.bmc_endpoint
            .as_mut()
            .unwrap()
            .interface
            .as_mut()
            .unwrap()
            .mac_address = "02:00:00:00:00:01".into();

        for (endpoint, mut body) in [
            (
                "/redfish/v1",
                serde_json::json!({"Id": "RootService", "Name": "Root Service", "Links": {}, "Systems": {"@odata.id": "/redfish/v1/Systems"}}),
            ),
            (
                "/redfish/v1/Systems",
                serde_json::json!({"@odata.type": "#ComputerSystemCollection.ComputerSystemCollection", "Name": "Systems", "Members": [{"@odata.id": "/redfish/v1/Systems/1"}]}),
            ),
            (
                "/redfish/v1/Systems/1",
                serde_json::json!({"Id": "1", "Name": "Compute", "Processors": {"@odata.id": "/redfish/v1/Systems/1/Processors"}}),
            ),
            (
                "/redfish/v1/Systems/1/Processors",
                serde_json::json!({"@odata.type": "#ProcessorCollection.ProcessorCollection", "Name": "Processors", "Members": [{"@odata.id": "/redfish/v1/Systems/1/Processors/GPU1"}]}),
            ),
            (
                "/redfish/v1/Systems/1/Processors/GPU1",
                serde_json::json!({"Id": "GPU1", "Name": "GPU", "Oem": {"Nvidia": {"MNNVLinkTopology": {"ChassisSerialNumber": "observed-chassis", "TraySlotIndex": tray, "TraySlotNumber": slot}}}}),
            ),
        ] {
            body["@odata.id"] = endpoint.into();

            Mock::given(method("GET"))
                .and(path(endpoint))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        }

        let result = service
            .discover_layout_computes(std::slice::from_ref(&node))
            .await;

        if valid {
            let placements = result.unwrap();

            assert_eq!(placements.len(), 1);
            assert_eq!(placements[0].node_id, "compute-1");
            assert_eq!(placements[0].chassis_serial_number, "observed-chassis");
            assert_eq!(placements[0].position.tray_index, 2);
            assert_eq!(placements[0].position.slot_number, 7);
        } else {
            assert_eq!(
                result.unwrap_err(),
                "compute 'compute-1' reports invalid physical placement"
            );

            continue;
        }

        server.reset().await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_string("private response details"))
            .mount(&server)
            .await;

        let error = service.discover_layout_computes(&[node]).await.unwrap_err();

        assert_eq!(
            error,
            "cannot read compute 'compute-1' placement from BMC: NotFound"
        );
    }
}

#[tokio::test]
async fn parallel_device_batch_overlaps_all_reads_and_preserves_input_order() {
    let barrier = tokio::sync::Barrier::new(9);

    let operations = (0..9).map(|index| {
        let barrier = &barrier;

        async move {
            barrier.wait().await;
            tokio::time::sleep(std::time::Duration::from_millis((8 - index) * 2)).await;

            Ok(index)
        }
    });

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        run_parallel(operations, None, "test reads"),
    )
    .await
    .expect("all 9 reads must overlap");

    assert_eq!(result.unwrap(), (0..9).collect::<Vec<_>>());
}

#[tokio::test]
async fn parallel_device_failure_panic_and_cancellation_drain_started_operations() {
    for scenario in ["failure", "panic", "cancellation"] {
        let barrier = tokio::sync::Barrier::new(9);
        let completed = std::sync::atomic::AtomicUsize::new(0);
        let cancellation = CancellationToken::new();

        let operations = (0..9).map(|index| {
            let barrier = &barrier;
            let completed = &completed;
            let cancellation = &cancellation;

            async move {
                barrier.wait().await;

                if index == 0 {
                    match scenario {
                        "failure" => return Err("injected failure".to_owned()),
                        "panic" => panic!("injected panic"),
                        _ => cancellation.cancel(),
                    }
                }

                tokio::task::yield_now().await;
                completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                Ok(())
            }
        });

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run_parallel(operations, Some(&cancellation), "test operations"),
        )
        .await
        .expect("started operations must drain");

        assert!(result.is_err(), "{scenario}");

        assert_eq!(
            completed.load(std::sync::atomic::Ordering::SeqCst),
            if scenario == "cancellation" { 9 } else { 8 }
        );
    }
}

#[tokio::test]
async fn failed_or_cancelled_device_phase_does_not_start_queued_operations() {
    for cancelled_before_start in [false, true] {
        let cancellation = CancellationToken::new();
        let started = std::sync::atomic::AtomicUsize::new(0);

        if cancelled_before_start {
            cancellation.cancel();
        }

        let operations = (0..9).map(|_| async {
            started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            Err::<(), _>("injected immediate failure".to_owned())
        });

        let result = run_parallel(operations, Some(&cancellation), "test operations").await;

        assert!(result.is_err());

        assert_eq!(
            started.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(!cancelled_before_start)
        );
    }
}

#[test]
fn saved_state_must_match_even_when_effective_state_matches() {
    let up: InterfaceCollection =
        serde_json::from_value(serde_json::json!({"acp1": {"link": {"state": {"up": {}}}}}))
            .unwrap();

    let snapshot = ScaleUpLinkSnapshot {
        effective: up.clone(),
        applied: up,
        startup: BTreeMap::new(),
    };

    let target = BTreeMap::from([("acp1".into(), FabricLinkState::Up)]);

    assert!(!links_match(&snapshot, &target).unwrap());
}

#[test]
fn unknown_and_ambiguous_states_fail_closed() {
    let unknown: InterfaceCollection = serde_json::from_value(
        serde_json::json!({"acp1": {"link": {"state": {"up": {}, "down": {}}}}}),
    )
    .unwrap();

    let snapshot = ScaleUpLinkSnapshot {
        effective: unknown.clone(),
        applied: unknown.clone(),
        startup: unknown,
    };

    let target = BTreeMap::from([("acp1".into(), FabricLinkState::Down)]);

    assert!(links_match(&snapshot, &target).is_err());
}

#[test]
fn inherited_effective_state_requires_explicit_applied_and_startup_targets() {
    let up: InterfaceCollection =
        serde_json::from_value(serde_json::json!({"acp1": {"link": {"state": {"up": {}}}}}))
            .unwrap();

    let mut snapshot = ScaleUpLinkSnapshot {
        effective: up.clone(),
        applied: BTreeMap::new(),
        startup: BTreeMap::new(),
    };

    let target = BTreeMap::from([("acp1".into(), FabricLinkState::Up)]);

    assert!(!links_match(&snapshot, &target).unwrap());

    snapshot.applied = up.clone();
    snapshot.startup = up;

    assert!(links_match(&snapshot, &target).unwrap());
}
