/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Exercises link reconciliation through the switch's NVUE adapter.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;
use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;

use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Models independently applied and persisted interface state for repeat calls.
async fn fixture(
    applied_up: bool,
    startup_up: bool,
    persist: bool,
) -> (MockServer, SwitchGb200Nvidia, Arc<AtomicUsize>) {
    let server = MockServer::start().await;
    let applied = Arc::new(AtomicBool::new(applied_up));
    let startup = Arc::new(AtomicBool::new(startup_up));
    let writes = Arc::new(AtomicUsize::new(0));

    for (route, revision, state) in [
        ("/nvue_v1/interface", "applied", Arc::clone(&applied)),
        ("/nvue_v1/", "applied", Arc::clone(&applied)),
        ("/nvue_v1/", "startup", Arc::clone(&startup)),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .and(query_param("rev", revision))
            .respond_with(move |_: &wiremock::Request| {
                let state = if state.load(Ordering::SeqCst) {
                    "up"
                } else {
                    "down"
                };

                let interfaces = serde_json::json!({"acp1": {"link": {"state": {(state): {}}}}});

                let body = if route == "/nvue_v1/" {
                    serde_json::json!({"interface": interfaces})
                } else {
                    interfaces
                };

                ResponseTemplate::new(200).set_body_json(body)
            })
            .mount(&server)
            .await;
    }

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .respond_with(ResponseTemplate::new(200).set_body_json("42"))
        .mount(&server)
        .await;

    for route in [
        "/nvue_v1/interface",
        "/nvue_v1/revision/42",
        "/nvue_v1/revision/applied",
    ] {
        let applied = Arc::clone(&applied);
        let startup = Arc::clone(&startup);
        let writes = Arc::clone(&writes);

        Mock::given(method("PATCH"))
            .and(path(route))
            .respond_with(move |_: &wiremock::Request| {
                writes.fetch_add(1, Ordering::SeqCst);

                if route == "/nvue_v1/revision/42" {
                    applied.store(true, Ordering::SeqCst);
                }

                if persist && route != "/nvue_v1/interface" {
                    startup.store(true, Ordering::SeqCst);
                }

                ResponseTemplate::new(200).set_body_json(serde_json::json!({}))
            })
            .mount(&server)
            .await;
    }

    Mock::given(method("GET"))
        .and(path("/nvue_v1/revision/42"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"42": {"state": "applied_and_saved"}})),
        )
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    (server, switch, writes)
}

#[tokio::test]
async fn link_reconciliation_repairs_drift_and_checks_persistence() {
    for (applied, saved, persist) in [
        (true, true, true),
        (false, false, true),
        (true, false, true),
        (false, false, false),
    ] {
        let (_server, switch, writes) = fixture(applied, saved, persist).await;
        let target = BTreeMap::from([("acp1".to_owned(), FabricLinkState::Up)]);

        let result = reconcile_links(&switch, &target).await;

        if !persist {
            assert!(result.is_err());
            assert!(writes.load(Ordering::SeqCst) > 0);
            continue;
        }

        assert_eq!(result, Ok(!applied || !saved));

        let before = writes.load(Ordering::SeqCst);
        let repeated = reconcile_links(&switch, &target).await;

        assert_eq!(repeated, Ok(false));
        assert_eq!(writes.load(Ordering::SeqCst), before);

        if applied && saved {
            assert_eq!(before, 0);
        } else {
            assert!(before > 0);
        }
    }
}

#[tokio::test]
async fn failed_link_observation_does_not_authorize_writes() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/interface"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let switch = SwitchGb200Nvidia::for_test(&server.uri());
    let target = BTreeMap::from([("acp1".to_owned(), FabricLinkState::Up)]);

    let result = reconcile_links(&switch, &target).await;

    assert!(result.is_err());

    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == wiremock::http::Method::Get)
    );
}
