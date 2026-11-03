/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Regression cases for recovery.

use super::*;

#[tokio::test]
async fn link_read_errors_report_category_without_device_response() {
    let fixture = WorkerFixture::new().await;

    for code in [ErrorCode::Unauthenticated, ErrorCode::Timeout] {
        fixture.states[0].lock().unwrap().snapshot_error = Some(code);
        let (job, operation) = fixture.admit();
        let mut worker = fixture.worker(&job, operation);

        for error in [
            worker.inspect().await.unwrap_err(),
            worker.verify_links().await.unwrap_err(),
        ] {
            assert!(error.contains(&code.to_string()));
            assert!(error.contains("switch-a"));
            assert!(!error.contains("private device response"));
        }

        fixture
            .service
            .fabric_operations
            .finish(&worker.operation.job_id)
            .unwrap();

        job.fail(JobFailure::new(JobError::Other, "inspection failed"));
    }
}

#[tokio::test]
async fn full_rack_readback_failure_keeps_partial_status_and_verified_primary() {
    use crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2::reconciliation_tests::{
        WiremockHttpsProxy, non_loopback_local_ip, vrnvl72_switch,
    };

    let mut fixture = WorkerFixture::new().await;
    fixture.plan.full_topology = true;
    let ip = non_loopback_local_ip();
    let mut proxies = Vec::new();

    for (switch, server) in fixture.switches.iter_mut().zip(&fixture._servers) {
        let proxy = WiremockHttpsProxy::start(ip, *server.address()).await;
        switch.node.host_endpoint =
            vrnvl72_switch(&switch.node.node_id, ip, proxy.port).host_endpoint;

        proxies.push(proxy);
    }

    let reads = std::sync::atomic::AtomicUsize::new(0);

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(move |_: &wiremock::Request| {
            // Only the final status collection observes a second enabled primary.
            let read = reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": if read < 2 { "disabled" } else { "enabled" }
            }))
        })
        .with_priority(1)
        .expect(3)
        .mount(&fixture._servers[1])
        .await;

    let (job, operation) = fixture.admit();
    fixture.prove_primary(&operation);
    let job_id = job.id().to_string();
    let primary = fixture.switches[0].controller_identity();
    let digest = config_digest(&fixture.plan.fabrics[0]).unwrap();
    let switches = std::mem::take(&mut fixture.switches);

    let plan = std::mem::replace(
        &mut fixture.plan,
        FabricPlan {
            fabrics: Vec::new(),
            links: BTreeMap::new(),
            full_topology: false,
        },
    );

    tokio::time::timeout(
        Duration::from_secs(10),
        fixture
            .service
            .run_managed_fabric_job(job, operation, switches, Vec::new(), plan, None),
    )
    .await
    .unwrap();

    let result = fixture.service.job_tracker.get_job(&job_id).unwrap();

    assert_eq!(
        result.state,
        crate::orchestrator::job_lifecycle::JobState::Failed
    );

    assert!(
        result
            .error_message
            .contains("multiple switches report scale-up fabric enabled")
    );

    let status: rm::ScaleUpFabricStatus = serde_json::from_str(&result.result_json).unwrap();

    assert_eq!(status.switches.len(), 2);
    assert!(status.switches.iter().all(|switch| switch.enabled));
    assert!(fixture.states[0].lock().unwrap().enabled);

    assert!(
        fixture
            .service
            .fabric_operations
            .has_activation_proof(&primary, &digest)
            .unwrap()
    );

    assert!(
        !fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(event, Event::Stop(_)))
    );
}

#[tokio::test]
async fn proved_enabled_primary_repairs_saved_disable_without_restart() {
    let fixture = WorkerFixture::new().await;
    fixture.states[0].lock().unwrap().saved_enabled = false;
    let (job, operation) = fixture.admit();
    fixture.prove_primary(&operation);
    let mut worker = fixture.worker(&job, operation);

    let result = tokio::time::timeout(Duration::from_secs(10), worker.run())
        .await
        .unwrap();

    assert_eq!(result, Ok(()));
    assert!(fixture.states[0].lock().unwrap().saved_enabled);

    assert!(
        fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .all(|event| matches!(event, Event::Snapshot(_, _, _) | Event::Save(_)))
    );

    assert_eq!(
        fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, Event::Save(_)))
            .count(),
        1
    );
}

#[tokio::test]
async fn nonselected_saved_enable_is_stopped_even_when_effective_disabled() {
    let fixture = WorkerFixture::new().await;
    fixture.states[1].lock().unwrap().saved_enabled = true;
    let (job, operation) = fixture.admit();
    fixture.prove_primary(&operation);
    let mut worker = fixture.worker(&job, operation);

    let result = tokio::time::timeout(Duration::from_secs(10), worker.run())
        .await
        .unwrap();

    assert_eq!(result, Ok(()));
    assert!(!fixture.states[1].lock().unwrap().saved_enabled);

    assert_eq!(
        fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, Event::Stop(id) if id == "switch-b"))
            .count(),
        1
    );

    assert!(fixture.events.lock().unwrap().iter().all(|event| !matches!(
        event,
        Event::Start(_) | Event::Restart(_) | Event::WriteConfig
    )));

    assert!(worker.operation.pending_controllers.is_empty());
}

#[tokio::test]
async fn failed_inspection_does_not_compensate_an_observed_nonselected_controller() {
    let mut fixture = WorkerFixture::new().await;

    fixture.plan.fabrics[0].assignment.switch_node_ids = vec!["switch-b".to_owned()];
    fixture.plan.fabrics[0].assignment.primary_switch_node_id = "switch-b".to_owned();
    fixture.states[1].lock().unwrap().saved_read_error = Some("unavailable");

    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    let error = tokio::time::timeout(Duration::from_secs(10), worker.run())
        .await
        .unwrap()
        .unwrap_err();

    worker.fail(error).await;

    assert!(fixture.states[0].lock().unwrap().enabled);
    assert!(fixture.states[0].lock().unwrap().saved_enabled);

    assert!(worker.operation.pending_controllers.is_empty());

    assert!(
        fixture
            .service
            .fabric_operations
            .find_overlapping(&worker.operation.intent.scope_keys)
            .unwrap()
            .is_empty()
    );

    fixture
        .service
        .fabric_operations
        .invalidate_and_require_clear(&worker.operation.intent.scope_keys)
        .unwrap();

    assert!(
        fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .all(|event| matches!(event, Event::Snapshot(_, _, _)))
    );
}

#[tokio::test]
async fn failure_after_shared_link_preparation_releases_ownership_without_pending_controllers() {
    let fixture = WorkerFixture::new().await;
    fixture.states[1].lock().unwrap().saved_link_up = false;
    let (job, operation) = fixture.admit();
    fixture.prove_primary(&operation);
    let mut worker = fixture.worker(&job, operation);

    tokio::time::timeout(Duration::from_secs(10), worker.run())
        .await
        .unwrap()
        .unwrap();

    assert!(worker.operation.pending_controllers.is_empty());

    assert!(
        fixture
            .events
            .lock()
            .unwrap()
            .contains(&Event::WriteLinks("switch-b".to_owned()))
    );

    worker.fail("final readback unavailable".to_owned()).await;

    fixture
        .service
        .fabric_operations
        .invalidate_and_require_clear(&worker.operation.intent.scope_keys)
        .unwrap();
}

#[tokio::test]
async fn cleanup_holds_reservations_until_attempts_finish() {
    let fixture = WorkerFixture::new().await;
    let (job, mut operation) = fixture.admit();
    let pending = fixture.switches[0].controller_identity();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    fixture
        .service
        .fabric_operations
        .mark_pending(&operation.job_id, std::slice::from_ref(&pending))
        .unwrap();

    operation.pending_controllers.insert(pending);
    fixture.states[0].lock().unwrap().stop_gate = Some(Arc::clone(&gate));
    job.cancellation_token().cancel();

    let mut worker = fixture.worker(&job, operation);

    let error = worker.run().await.unwrap_err();
    let scopes = fixture.switches[0].scopes().unwrap();
    let scope_keys = worker.operation.intent.scope_keys.clone();

    let blocked = async {
        while !fixture
            .events
            .lock()
            .unwrap()
            .contains(&Event::Stop("switch-a".to_owned()))
        {
            tokio::task::yield_now().await;
        }

        assert!(
            fixture
                .service
                .job_tracker
                .reserve_mutation(scopes.clone())
                .is_err()
        );

        assert!(
            fixture
                .service
                .fabric_operations
                .invalidate_and_require_clear(&scope_keys)
                .is_err()
        );

        gate.add_permits(1);
    };

    let (error, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(worker.fail(error), blocked)
    })
    .await
    .unwrap();

    fixture
        .service
        .fabric_operations
        .invalidate_and_require_clear(&scope_keys)
        .unwrap();

    assert!(
        fixture
            .service
            .job_tracker
            .reserve_mutation(scopes.clone())
            .is_err()
    );

    drop(worker);
    job.fail(JobFailure::new(JobError::Other, error));

    let _reservation = fixture
        .service
        .job_tracker
        .reserve_mutation(scopes)
        .unwrap();

    assert!(fixture.states[1].lock().unwrap().saved_link_up);
}

#[tokio::test]
async fn cancellation_before_inspection_preserves_unstarted_scopes() {
    let fixture = WorkerFixture::new().await;
    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    job.cancellation_token().cancel();

    let error = worker.run().await.unwrap_err();
    worker.fail(error).await;

    assert!(fixture.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failure_cleanup_releases_ownership_and_preserves_verified_siblings() {
    let mut fixture = WorkerFixture::new().await;

    fixture.plan.fabrics.push(PlannedFabric {
        assignment: rm_v2::ScaleUpFabricMembers {
            switch_node_ids: vec!["switch-b".to_owned()],
            primary_switch_node_id: "switch-b".to_owned(),
            ..Default::default()
        },
        config: rm_v2::ScaleUpFabricConfig {
            topology_type: "layout-b".to_owned(),
            extra_static_configs: Vec::new(),
        },
        absent_static_configs: BTreeSet::new(),
    });

    for (cancel, stop_error, panic_on_link_write) in [
        (false, None, false),
        (true, None, false),
        (false, Some(ErrorCode::Timeout), false),
        (false, Some(ErrorCode::Internal), false),
        (false, None, true),
    ] {
        fixture.events.lock().unwrap().clear();

        {
            let mut state = fixture.states[1].lock().unwrap();
            state.link_up = true;
            state.saved_link_up = true;
            state.stop_error = stop_error;
            state.panic_on_link_write = panic_on_link_write;
        }

        let (job, operation) = fixture.admit();
        fixture.prove_primary(&operation);
        let pending = fixture.switches[1].controller_identity();
        fixture
            .service
            .fabric_operations
            .mark_pending(&operation.job_id, std::slice::from_ref(&pending))
            .unwrap();

        let mut worker = fixture.worker(&job, operation);
        worker.operation.pending_controllers.insert(pending.clone());
        worker.unverified_primaries.insert("switch-b".to_owned());

        if cancel {
            job.cancellation_token().cancel();
        }

        let error = worker.fail("group verification failed".to_owned()).await;

        assert!(fixture.events.lock().unwrap().iter().all(
            |event| !matches!(event, Event::Stop(id) | Event::WriteLinks(id) if id == "switch-a")
        ));

        if stop_error.is_some() || panic_on_link_write {
            assert!(error.contains("cleanup"));
        } else {
            assert!(!fixture.states[1].lock().unwrap().saved_link_up);
        }

        assert!(
            fixture
                .service
                .fabric_operations
                .has_activation_proof(
                    &fixture.switches[0].controller_identity(),
                    &config_digest(&fixture.plan.fabrics[0]).unwrap(),
                )
                .unwrap()
        );

        fixture
            .service
            .fabric_operations
            .invalidate_and_require_clear(&worker.operation.intent.scope_keys)
            .unwrap();

        job.fail(JobFailure::new(JobError::Other, error));
    }
}

#[tokio::test]
async fn retry_exhaustion_and_readback_failure_allow_later_mutation() {
    for full_readback in [false, true] {
        let mut fixture = WorkerFixture::new().await;
        let mut readback_proxies = Vec::new();

        if full_readback {
            use crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2::reconciliation_tests::{
                WiremockHttpsProxy, non_loopback_local_ip, vrnvl72_switch,
            };

            fixture.plan.full_topology = true;
            let ip = non_loopback_local_ip();

            for (switch, server) in fixture.switches.iter_mut().zip(&fixture._servers) {
                let proxy = WiremockHttpsProxy::start(ip, *server.address()).await;
                switch.node.host_endpoint =
                    vrnvl72_switch(&switch.node.node_id, ip, proxy.port).host_endpoint;

                readback_proxies.push(proxy);
            }

            let reads = std::sync::atomic::AtomicUsize::new(0);

            Mock::given(method("GET"))
                .and(path("/nvue_v1/cluster"))
                .respond_with(move |_: &wiremock::Request| {
                    // The third cluster read is the final full-rack status collection.
                    let read = reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "state": if read < 2 { "disabled" } else { "enabled" }
                    }))
                })
                .with_priority(1)
                .expect(3)
                .mount(&fixture._servers[1])
                .await;
        } else {
            fixture.states[0].lock().unwrap().enabled = false;
            *fixture.hello_failures.lock().unwrap() =
                crate::api::grpc::scaleupfabricmanager_handlers::NMX_CONTROLLER_READY_POLL_ATTEMPTS
                    + 1;
        }

        let (job, operation) = fixture.admit();

        if full_readback {
            fixture.prove_primary(&operation);
        }

        let job_id = job.id().to_string();
        let scope_keys = operation.intent.scope_keys.clone();
        let node = fixture.switches[0].node.clone();

        let client = fixture.switches[0]
            .prepared
            .switch
            .nvue_client()
            .unwrap()
            .clone();

        let switches = std::mem::take(&mut fixture.switches);

        let plan = std::mem::replace(
            &mut fixture.plan,
            FabricPlan {
                fabrics: Vec::new(),
                links: BTreeMap::new(),
                full_topology: false,
            },
        );

        tokio::time::timeout(
            Duration::from_secs(10),
            fixture.service.run_managed_fabric_job(
                job,
                operation,
                switches,
                Vec::new(),
                plan,
                None,
            ),
        )
        .await
        .unwrap();

        assert_eq!(
            fixture.service.job_tracker.get_job(&job_id).unwrap().state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );

        assert_eq!(*fixture.hello_failures.lock().unwrap(), 0);

        assert!(
            fixture
                .service
                .fabric_operations
                .find_overlapping(&scope_keys)
                .unwrap()
                .is_empty()
        );

        let _reservation = fixture
            .service
            .reserve_switch_mutation(Some(&client), &node)
            .await
            .unwrap();
    }
}
