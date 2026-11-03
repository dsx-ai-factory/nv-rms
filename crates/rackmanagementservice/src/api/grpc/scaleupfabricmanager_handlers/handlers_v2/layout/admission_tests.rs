/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Installed-catalog admission, selected members, and worker dispatch.

use std::sync::Arc;
use std::time::Duration;

use super::super::reconciliation_tests::{
    WiremockHttpsProxy, insecure_fabric_service, non_loopback_local_ip, vrnvl72_switch,
};
use super::*;
use crate::api::grpc::switch_scope::fabric_scope_keys;
use crate::config::fabric_layout::{FabricLayoutCatalog, FabricLayoutConfig};
use crate::orchestrator::job_lifecycle::JobState;
use crate::orchestrator::job_tracker::JobTracker;

use librms::protos::rack_manager_v2::rack_manager_v2_server::RackManagerV2;

use wiremock::http::Method;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn admission_fixture(
    discovery_reads: u64,
    worker_reads: u64,
) -> (
    RackManagerServiceImpl,
    rm_v2::ConfigureScaleUpFabricManagerRequest,
    MockServer,
    WiremockHttpsProxy,
) {
    let server = MockServer::start().await;
    let ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(ip, *server.address()).await;
    let mut service = insecure_fabric_service();

    let config: FabricLayoutConfig = serde_json::from_value(serde_json::json!({
        "profiles": [{
            "switch_node_types": ["switch_gb200_nvidia"],
            "compute_node_types": ["compute_gb200_nvidia"],
            "physical_topology": "synthetic",
            "compute_tray_count": 1, "switch_tray_count": 1,
            "full_link_states": {"acp1": "up"},
            "fabric_rules": [{
                "compute_tray_count": 1, "switch_tray_count": 1,
                "controller_rules": [{"topology_type": "synthetic", "static_configs": []}]
            }],
            "link_calculation": {"default_state": "down", "rules": [{
                "prefix": "acp", "base": 1, "terms": [], "offsets": [0],
                "width": 1, "state": "up"
            }]}
        }]
    }))
    .unwrap();

    service.fabric_layout_catalog = Arc::new(FabricLayoutCatalog::from_config(&config).unwrap());

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chassis-sn": "synthetic", "slot-number": "4",
            "tray-index": "3", "topology-id": "synthetic"
        })))
        .expect(discovery_reads)
        .mount(&server)
        .await;

    // Fail the first worker observation, after discovery and admission complete.
    Mock::given(method("GET"))
        .and(path("/nvue_v1/interface"))
        .respond_with(ResponseTemplate::new(503))
        .expect(worker_reads)
        .mount(&server)
        .await;

    let mut node = vrnvl72_switch("switch-a", ip, proxy.port);
    node.r#type = None;

    node.node_descriptor = Some(rm::NodeDescriptor {
        attributes: [
            ("role".into(), "switch".into()),
            ("vendor".into(), "NVIDIA".into()),
            ("product_family".into(), "GB200".into()),
        ]
        .into(),
    });

    let request = rm_v2::ConfigureScaleUpFabricManagerRequest {
        nodes: Some(rm::NodeSet { nodes: vec![node] }),
        config: Some(rm_v2::ScaleUpFabricConfig {
            topology_type: "synthetic".into(),
            ..Default::default()
        }),
        layout: Some(rm_v2::ScaleUpFabricLayout {
            fabrics: vec![rm_v2::ScaleUpFabricSpec {
                compute_tray_count: 1,
                switch_tray_count: 1,
                ..Default::default()
            }],
        }),
        ..Default::default()
    };

    (service, request, server, proxy)
}

#[tokio::test]
async fn layout_admission_rejects_active_ownership_capacity_and_excess_computes() {
    let (mut service, request, server, _proxy) = admission_fixture(3, 0).await;
    let registry = &service.fabric_operations;

    let scope = JobScope::PhysicalSwitch {
        chassis_serial_number: "synthetic".into(),
        tray_index: 3,
    };

    let scope_keys = fabric_scope_keys(std::slice::from_ref(&scope)).unwrap();

    let owner = service
        .job_tracker
        .create_scoped_job(
            "rack-01",
            "",
            JobType::ConfigureScaleUpFabricManagerV2,
            vec![scope],
        )
        .unwrap();

    registry
        .admit(
            FabricOperationIntent {
                scope_keys: scope_keys.clone(),
                rule_id: service.fabric_layout_catalog.identity().to_owned(),
            },
            owner.id().as_ref(),
            None,
        )
        .unwrap();

    let error = RackManagerV2::configure_scale_up_fabric_manager(
        &service,
        tonic::Request::new(request.clone()),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    assert_eq!(
        service.job_tracker.registry().non_terminal_job_ids(),
        [owner.id().clone()]
    );

    assert_eq!(
        registry.find_overlapping(&scope_keys).unwrap()[0].job_id,
        owner.id().as_ref()
    );

    registry.finish(owner.id().as_ref()).unwrap();

    owner.fail(JobFailure::new(JobError::Other, "test owner finished"));

    service.job_tracker = Arc::new(JobTracker::builder().max_tracked_jobs(0).build().unwrap());

    let error = RackManagerV2::configure_scale_up_fabric_manager(
        &service,
        tonic::Request::new(request.clone()),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Internal);

    assert!(registry.find_overlapping(&scope_keys).unwrap().is_empty());

    let mut request = request;
    request
        .nodes
        .as_mut()
        .unwrap()
        .nodes
        .extend(["compute-a", "compute-b"].map(|id| rm::NodeInfo {
            node_id: id.into(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            ..Default::default()
        }));

    let error =
        RackManagerV2::configure_scale_up_fabric_manager(&service, tonic::Request::new(request))
            .await
            .unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    server.verify().await;
}

#[tokio::test]
async fn installed_catalog_admits_selected_fabric_and_retries_failed_job() {
    let (service, request, server, _proxy) = admission_fixture(2, 2).await;

    let scope = JobScope::PhysicalSwitch {
        chassis_serial_number: "synthetic".into(),
        tray_index: 3,
    };

    let scope_keys = fabric_scope_keys(std::slice::from_ref(&scope)).unwrap();
    let registry = &service.fabric_operations;

    let mut job_ids = BTreeSet::new();

    for attempt in 0..2 {
        let response = RackManagerV2::configure_scale_up_fabric_manager(
            &service,
            tonic::Request::new(request.clone()),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(job_ids.insert(response.job_id.clone()));

        assert_eq!(response.selected_fabrics.len(), 1);

        let selected = &response.selected_fabrics[0];

        assert!(selected.compute_node_ids.is_empty());
        assert_eq!(selected.switch_node_ids, ["switch-a"]);
        assert_eq!(selected.primary_switch_node_id, "switch-a");

        // The current-thread runtime has not polled the spawned worker yet.
        let overlapping = service.job_tracker.create_scoped_job(
            "rack-01",
            "other",
            JobType::ConfigureScaleUpFabricManagerV2,
            vec![scope.clone()],
        );

        assert!(overlapping.is_err());

        let operation = registry.find_overlapping(&scope_keys).unwrap().remove(0);

        let terminal = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let job = service.job_tracker.get_job(&response.job_id).unwrap();

                if job.state.is_terminal() {
                    return job.state;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert_eq!(terminal, JobState::Failed);

        let unresolved = registry.find_overlapping(&scope_keys).unwrap();

        assert!(unresolved.is_empty());

        registry.invalidate_and_require_clear(&scope_keys).unwrap();

        if attempt == 0 {
            // Recover a terminal owner whose pending controller set is empty.
            registry
                .admit(operation.intent, &response.job_id, None)
                .unwrap();
        }
    }

    server.verify().await;

    let requests = server.received_requests().await.unwrap();

    assert!(requests.iter().all(|request| request.method == Method::Get));
}
