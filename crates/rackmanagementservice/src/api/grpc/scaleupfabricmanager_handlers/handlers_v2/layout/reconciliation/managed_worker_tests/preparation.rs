/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Regression cases for preparation.

use super::*;

#[tokio::test]
async fn disabled_nonprimary_requires_shutdown_before_enabling_links() {
    for (link_up, target, stop_error, should_stop) in [
        (false, FabricLinkState::Up, None, true),
        (false, FabricLinkState::Up, Some(ErrorCode::Timeout), true),
        (true, FabricLinkState::Up, None, false),
        (true, FabricLinkState::Down, None, false),
    ] {
        let mut fixture = WorkerFixture::new().await;
        fixture.plan.fabrics[0]
            .assignment
            .switch_node_ids
            .push("switch-b".to_owned());

        fixture.plan.links.insert(
            "switch-b".to_owned(),
            BTreeMap::from([("access1".to_owned(), target)]),
        );

        {
            let mut state = fixture.states[1].lock().unwrap();
            state.link_up = link_up;
            state.saved_link_up = link_up;
            state.stop_error = stop_error;
        }

        let (job, operation) = fixture.admit();
        let mut worker = fixture.worker(&job, operation);

        worker.inspect().await.unwrap();

        let result = worker.prepare().await;

        assert_eq!(result.is_err(), stop_error.is_some());

        let events = fixture.events.lock().unwrap();

        let stop = events
            .iter()
            .position(|event| *event == Event::Stop("switch-b".to_owned()));

        assert_eq!(stop.is_some(), should_stop);

        if stop_error.is_some() {
            assert!(
                !events
                    .iter()
                    .any(|event| { matches!(event, Event::WriteLinks(_) | Event::Start(_)) })
            );
        } else if should_stop {
            let write = events
                .iter()
                .position(|event| *event == Event::WriteLinks("switch-b".to_owned()))
                .unwrap();

            assert!(stop.unwrap() < write);
            assert!(fixture.states[1].lock().unwrap().saved_link_up);
        }
    }
}

#[tokio::test]
async fn missing_client_tls_does_not_expose_external_grpc() {
    let mut fixture = WorkerFixture::new().await;
    fixture.service.switch_tls_roots.insecure_switch = false;
    let (job, operation) = fixture.admit();
    let worker = fixture.worker(&job, operation);

    let result = worker.controller.start(&fixture.switches[0]).await;

    assert!(result.is_err());

    assert!(
        !fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(event, Event::ExposeController(_)))
    );
}

#[tokio::test]
async fn parallel_static_staging_isolates_all_primaries_and_stops_before_target_links() {
    // The controller already has the target values, but disabled primaries
    // still require isolated starts to establish authoritative readback.
    let fixture = WorkerFixture::parallel_groups(&["layout-a"; 5]).await;
    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    worker.changed_groups.extend(0..5);

    let result = tokio::time::timeout(Duration::from_secs(10), worker.prepare())
        .await
        .expect("all 5 isolated controller starts must overlap");

    assert_eq!(result, Ok(()));

    assert_eq!(
        *fixture.config_content.lock().unwrap(),
        format!("{NMX_C_TOPOLOGY_KEY}=layout-a\n")
    );

    assert!(fixture.states.iter().all(|state| {
        let state = state.lock().unwrap();
        !state.enabled && !state.saved_enabled && state.link_up && state.saved_link_up
    }));

    let events = fixture.events.lock().unwrap();

    let first_start = events
        .iter()
        .position(|event| matches!(event, Event::Start(_)))
        .unwrap();

    let last_stop = events
        .iter()
        .rposition(|event| matches!(event, Event::Stop(_)))
        .unwrap();

    let first_target_write = events
        .iter()
        .enumerate()
        .find_map(|(index, event)| {
            (index > first_start && matches!(event, Event::WriteLinks(_))).then_some(index)
        })
        .unwrap();

    assert!(last_stop < first_target_write);

    for switch in &fixture.switches {
        let isolation = events
            .iter()
            .position(|event| *event == Event::Snapshot(switch.node.node_id.clone(), false, false))
            .unwrap();

        assert!(isolation < first_start);
    }

    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Event::Start(_)))
            .count(),
        5
    );
}

#[tokio::test]
async fn parallel_static_staging_failure_drains_siblings_without_enabling_target_links() {
    let fixture = WorkerFixture::parallel_groups(&["layout-a"; 5]).await;
    fixture.states[0].lock().unwrap().panic_on_start = true;

    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    worker.changed_groups.extend(0..5);

    let result = tokio::time::timeout(Duration::from_secs(10), worker.prepare())
        .await
        .expect("sibling static staging must drain after a panic");

    assert!(result.is_err());

    assert!(fixture.states.iter().all(|state| {
        let state = state.lock().unwrap();
        !state.link_up && !state.saved_link_up
    }));

    assert!(
        fixture.states[1..]
            .iter()
            .all(|state| !state.lock().unwrap().enabled)
    );

    worker.fail(result.unwrap_err()).await;

    assert!(
        fixture
            .states
            .iter()
            .all(|state| !state.lock().unwrap().enabled)
    );

    assert_eq!(worker.operation.pending_controllers.len(), 5);
}

#[tokio::test]
async fn parallel_link_preparation_overlaps_all_switches_and_finishes_before_activation() {
    let (fixture, gate) = WorkerFixture::blocked_link_writes().await;
    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    let release_writes = async {
        loop {
            let events = fixture.events.lock().unwrap().clone();

            let started = events
                .iter()
                .filter(|event| matches!(event, Event::WriteLinks(_)))
                .count();

            if started == 9 {
                assert!(!events.iter().any(|event| matches!(event, Event::Start(_))));
                gate.add_permits(9);
                break;
            }

            tokio::task::yield_now().await;
        }
    };

    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(worker.run(), release_writes)
    })
    .await
    .expect("all nine link writes must overlap before controller activation");

    assert_eq!(result, Ok(()));
    let events = fixture.events.lock().unwrap();

    let activation = events
        .iter()
        .position(|event| matches!(event, Event::Start(_)))
        .unwrap();

    let mut active = 0;
    let mut peak = 0;
    let mut completed = 0;

    for (index, event) in events.iter().enumerate() {
        if matches!(event, Event::WriteLinks(_)) {
            active += 1;
            peak = peak.max(active);
        }

        if matches!(event, Event::LinkWriteFinished(_)) {
            active -= 1;
            completed += 1;
            assert!(index < activation);
        }
    }

    assert_eq!(active, 0);

    assert_eq!(peak, 9);

    assert_eq!(completed, 9);

    assert!(
        fixture
            .states
            .iter()
            .all(|state| state.lock().unwrap().saved_link_up)
    );
}

#[tokio::test]
async fn parallel_link_errors_and_panics_drain_all_dispatched_writes() {
    for panic_on_write in [false, true] {
        let (fixture, gate) = WorkerFixture::blocked_link_writes().await;
        fixture.states[0].lock().unwrap().reject_saved_link_write = true;
        fixture.states[0].lock().unwrap().panic_on_link_write = panic_on_write;
        let (job, operation) = fixture.admit();
        let mut worker = fixture.worker(&job, operation);

        let release_writes = async {
            loop {
                let started = fixture
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|event| matches!(event, Event::WriteLinks(_)))
                    .count();

                if started == 9 {
                    gate.add_permits(9);
                    break;
                }

                tokio::task::yield_now().await;
            }
        };

        let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(worker.prepare_links(), release_writes)
        })
        .await
        .expect("dispatched writes must finish after a sibling fails");

        assert!(result.is_err());

        assert!(
            fixture.states[1..]
                .iter()
                .all(|state| state.lock().unwrap().saved_link_up)
        );

        assert!(!fixture.states[0].lock().unwrap().saved_link_up);
    }
}

#[tokio::test]
async fn cancellation_drains_all_started_parallel_link_writes() {
    let (fixture, gate) = WorkerFixture::blocked_link_writes().await;
    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    let cancel_after_dispatch = async {
        loop {
            let started = fixture
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, Event::WriteLinks(_)))
                .count();

            if started == 9 {
                job.cancellation_token().cancel();
                gate.add_permits(9);
                break;
            }

            tokio::task::yield_now().await;
        }
    };

    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(worker.prepare_links(), cancel_after_dispatch)
    })
    .await
    .expect("dispatched writes must finish after cancellation");

    assert!(result.is_err());

    assert!(
        fixture
            .states
            .iter()
            .all(|state| state.lock().unwrap().saved_link_up)
    );
}
