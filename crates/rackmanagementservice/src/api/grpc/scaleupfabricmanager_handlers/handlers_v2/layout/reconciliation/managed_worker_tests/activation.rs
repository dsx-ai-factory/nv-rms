/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Regression cases for activation.

use super::*;

#[tokio::test]
async fn parallel_activation_preserves_verified_sibling_after_failure_and_panic() {
    let fixture = WorkerFixture::parallel_groups(&["layout-a", "layout-b", "layout-c"]).await;
    fixture.states[1].lock().unwrap().panic_on_start = true;
    let (job, operation) = fixture.admit();

    let mut worker = fixture.worker(&job, operation);
    worker.changed_groups.extend(0..3);
    worker.static_config_matches.extend(0..3);

    // Exercise the real preparation barrier without the isolated bootstrap.
    worker.prepare().await.unwrap();

    let error = tokio::time::timeout(Duration::from_secs(10), worker.configure_groups())
        .await
        .expect("all three activations must overlap at the start barrier")
        .unwrap_err();

    assert_eq!(worker.operation.pending_controllers.len(), 2);

    assert!(
        !worker
            .operation
            .pending_controllers
            .contains(&fixture.switches[0].controller_identity())
    );

    let starts = fixture
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, Event::Start(_)))
        .count();

    assert_eq!(starts, 3);

    worker.fail(error).await;

    assert!(fixture.states[0].lock().unwrap().enabled);
    assert!(!fixture.states[1].lock().unwrap().enabled);
    assert!(!fixture.states[2].lock().unwrap().enabled);

    assert_eq!(worker.operation.pending_controllers.len(), 2);
}

#[tokio::test]
async fn parallel_activation_overlaps_all_groups_and_drains_failures() {
    let fixture = WorkerFixture::parallel_groups(&["layout-b"; 9]).await;
    let (job, operation) = fixture.admit();

    let mut worker = fixture.worker(&job, operation);
    worker.changed_groups.extend(0..9);
    worker.static_config_matches.extend(0..9);
    worker.prepare().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(10), worker.configure_groups())
        .await
        .expect("all nine activations must overlap at the start barrier");

    assert!(result.is_err());

    let starts = fixture
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, Event::Start(_)))
        .count();

    assert_eq!(starts, 9);

    assert_eq!(worker.operation.pending_controllers.len(), 9);
}

#[tokio::test]
async fn terminal_job_drains_started_activations_before_recovery() {
    let fixture = WorkerFixture::parallel_groups(&["layout-b"; 2]).await;

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    fixture.states[1].lock().unwrap().start_gate = Some(Arc::clone(&gate));

    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    worker.changed_groups.extend(0..2);
    worker.static_config_matches.extend(0..2);
    worker.prepare().await.unwrap();

    let fail_after_dispatch = async {
        loop {
            let starts = fixture
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, Event::Start(_)))
                .count();

            if starts == 2 {
                fixture
                    .service
                    .job_tracker
                    .registry()
                    .fail_non_terminal_for_shutdown(std::slice::from_ref(job.id()));

                assert!(
                    fixture
                        .service
                        .job_tracker
                        .registry()
                        .get(job.id())
                        .unwrap()
                        .state
                        .is_terminal()
                );

                break;
            }

            tokio::task::yield_now().await;
        }
    };

    let activations = worker.configure_groups();
    tokio::pin!(activations);

    let (waiting, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            tokio::time::timeout(Duration::from_secs(2), &mut activations),
            fail_after_dispatch
        )
    })
    .await
    .expect("all activations must start");

    assert!(
        waiting.is_err(),
        "terminal publication must not drop an active sibling"
    );

    gate.add_permits(1);

    assert!(
        tokio::time::timeout(Duration::from_secs(10), &mut activations)
            .await
            .expect("released activation must drain")
            .is_err()
    );

    assert!(
        fixture
            .events
            .lock()
            .unwrap()
            .contains(&Event::StartFinished("switch-b".to_owned()))
    );
}

#[tokio::test]
async fn cancellation_drains_all_started_parallel_activations() {
    let fixture = WorkerFixture::parallel_groups(&["layout-b"; 5]).await;
    let (job, operation) = fixture.admit();

    let mut worker = fixture.worker(&job, operation);
    worker.changed_groups.extend(0..5);
    worker.static_config_matches.extend(0..5);
    worker.prepare().await.unwrap();

    let cancel_after_activation = async {
        loop {
            let starts = fixture
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, Event::Start(_)))
                .count();

            if starts == 5 {
                job.cancellation_token().cancel();
                break;
            }

            tokio::task::yield_now().await;
        }
    };

    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(worker.configure_groups(), cancel_after_activation)
    })
    .await
    .expect("active groups must drain after cancellation");

    assert!(result.is_err());

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
async fn final_activation_follows_every_shared_saved_link_verification() {
    let fixture = WorkerFixture::new().await;
    fixture.states[0].lock().unwrap().link_up = false;
    fixture.states[0].lock().unwrap().saved_link_up = false;
    fixture.states[1].lock().unwrap().link_up = false;
    fixture.states[1].lock().unwrap().saved_link_up = false;
    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    let result = tokio::time::timeout(Duration::from_secs(10), worker.run())
        .await
        .unwrap();

    assert_eq!(result, Ok(()));
    let events = fixture.events.lock().unwrap();

    let activation = events
        .iter()
        .position(|event| *event == Event::Start("switch-a".to_owned()))
        .unwrap();

    for id in ["switch-a", "switch-b"] {
        let write = events
            .iter()
            .position(|event| *event == Event::WriteLinks(id.to_owned()))
            .unwrap();

        let verification = events
            .iter()
            .enumerate()
            .find_map(|(index, event)| {
                (index > write && *event == Event::Snapshot(id.to_owned(), true, true))
                    .then_some(index)
            })
            .unwrap();

        assert!(
            verification < activation,
            "saved target on {id} was not verified before activation"
        );
    }

    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Event::Start(_)))
            .count(),
        1
    );

    assert!(
        events
            .iter()
            .all(|event| !matches!(event, Event::WriteConfig))
    );
}

#[tokio::test]
async fn rejected_saved_target_prevents_final_activation() {
    let mut fixture = WorkerFixture::new().await;
    fixture.add_switch("switch-c", 9, false).await;

    fixture.plan.links.insert(
        "switch-c".to_owned(),
        BTreeMap::from([("access1".to_owned(), FabricLinkState::Up)]),
    );

    fixture.states[1].lock().unwrap().link_up = false;
    fixture.states[1].lock().unwrap().saved_link_up = false;
    fixture.states[1].lock().unwrap().reject_saved_link_write = true;
    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    let result = tokio::time::timeout(Duration::from_secs(10), worker.run())
        .await
        .unwrap();

    assert!(result.is_err());

    worker.fail(result.unwrap_err()).await;

    assert!(
        fixture
            .events
            .lock()
            .unwrap()
            .contains(&Event::WriteLinks("switch-b".to_owned()))
    );

    assert!(
        fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .all(|event| !matches!(event, Event::Start(_)))
    );
}

#[tokio::test]
async fn failed_file_removal_retains_pending_activation_and_isolates_primary() {
    let mut fixture = WorkerFixture::new().await;
    fixture.plan.fabrics[0]
        .absent_static_configs
        .insert((NMX_C_FM_CONFIG_FILE.to_owned(), "owned".to_owned()));

    *fixture.config_content.lock().unwrap() =
        format!("{NMX_C_TOPOLOGY_KEY}=layout-a\nowned=old\nunowned=preserved\n");

    let (job, operation) = fixture.admit();
    let mut worker = fixture.worker(&job, operation);

    let result = tokio::time::timeout(Duration::from_secs(10), worker.run())
        .await
        .unwrap();

    assert!(result.is_err());
    worker.fail(result.unwrap_err()).await;

    assert!(!fixture.states[0].lock().unwrap().enabled);
    assert!(!fixture.states[0].lock().unwrap().saved_enabled);

    assert!(
        worker
            .operation
            .pending_controllers
            .contains(&fixture.switches[0].controller_identity())
    );

    assert_eq!(
        fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, Event::WriteConfig))
            .count(),
        1
    );

    assert_eq!(
        fixture
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, Event::Start(_)))
            .count(),
        1
    );
}

#[tokio::test]
async fn proved_matching_repeats_ignore_unassigned_switch_mutations() {
    let fixture = WorkerFixture::new().await;

    for repeat in 0..2 {
        let (job, operation) = fixture.admit();

        if repeat == 0 {
            fixture.prove_primary(&operation);
        }

        let mut worker = fixture.worker(&job, operation);

        let result = tokio::time::timeout(Duration::from_secs(10), worker.run())
            .await
            .unwrap();

        assert_eq!(result, Ok(()));

        fixture
            .service
            .fabric_operations
            .finish(&worker.operation.job_id)
            .unwrap();

        job.complete("verified", "");

        let scopes = fixture.switches[1].scopes().unwrap();

        let _reservation = fixture
            .service
            .job_tracker
            .reserve_mutation(scopes.clone())
            .unwrap();

        crate::api::grpc::switch_scope::require_no_fabric_operation(
            fixture.service.fabric_operations.as_ref(),
            &scopes,
        )
        .unwrap();
    }

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
async fn failed_or_unknown_saved_cluster_read_performs_no_writes() {
    let fixture = WorkerFixture::new().await;

    for error in ["unavailable", "unknown"] {
        fixture.states[0].lock().unwrap().saved_read_error = Some(error);
        let (job, operation) = fixture.admit();
        let mut worker = fixture.worker(&job, operation);

        let result = tokio::time::timeout(Duration::from_secs(10), worker.run())
            .await
            .unwrap();

        assert!(result.is_err());

        assert!(
            fixture
                .events
                .lock()
                .unwrap()
                .iter()
                .all(|event| matches!(event, Event::Snapshot(_, _, _)))
        );

        fixture
            .service
            .fabric_operations
            .finish(&worker.operation.job_id)
            .unwrap();

        job.fail(JobFailure::new(JobError::Other, "saved read unavailable"));
    }
}
