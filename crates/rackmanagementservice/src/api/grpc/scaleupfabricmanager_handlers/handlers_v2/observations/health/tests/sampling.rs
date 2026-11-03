/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Independent sampling and read-deadline regression tests.

use super::*;

#[tokio::test]
async fn samples_preserve_independent_successes_and_sanitize_each_read_failure() {
    for failure in 0..4 {
        let mut client = healthy_client();

        let error = crate::libnmxc::NmxcError::Status(tonic::Status::unavailable(
            "transport to https://user:secret@controller failed",
        ));

        match failure {
            0 => client.domain_state_results = VecDeque::from([Err(error)]),
            1 => client.topology_results = VecDeque::from([Err(error)]),
            2 => client.switch_node_results = VecDeque::from([Err(error)]),
            3 => client.compute_node_results = VecDeque::from([Err(error)]),
            _ => unreachable!(),
        }

        let samples = sample_group(&mut client, "gateway", None).await;

        let results = [
            samples.domain.as_ref().map(|_| ()),
            samples.topology.as_ref().map(|_| ()),
            samples.switch_nodes.as_ref().map(|_| ()),
            samples.compute_nodes.as_ref().map(|_| ()),
        ];

        let expected_errors = [
            "controller state and health read unavailable",
            "device topology and operational link read unavailable",
            "switch health read unavailable",
            "compute health read unavailable",
        ];

        for (resource, result) in results.into_iter().enumerate() {
            if resource == failure {
                assert_eq!(result.unwrap_err(), expected_errors[resource]);
            } else {
                assert!(result.is_ok(), "resource {resource} failed unexpectedly");
            }
        }

        assert_eq!(client.domain_state_calls, ["gateway"]);
        assert_eq!(client.topology_calls, ["gateway"]);
        assert_eq!(client.switch_node_calls, ["gateway"]);
        assert_eq!(client.compute_node_calls, ["gateway"]);
        assert!(client.set_calls.is_empty());
        assert!(client.set_file_calls.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn resource_deadline_preserves_completed_reads_and_bounds_pending_reads() {
    let deadline = Some(Instant::now() + Duration::from_secs(1));

    let first = sample_resource(async { Ok(7u32) }, deadline, "first unavailable").await;

    let second = sample_resource(
        std::future::pending::<Result<u32, NmxcError>>(),
        deadline,
        "second unavailable",
    )
    .await;

    let third = sample_resource(async { Ok(9u32) }, deadline, "third unavailable").await;

    assert_eq!(first.unwrap(), 7);
    assert_eq!(second.unwrap_err(), "second unavailable");
    assert_eq!(third.unwrap_err(), "third unavailable");
}
