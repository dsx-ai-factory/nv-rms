/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reads controller state, topology, and node status, keeping each result separately.

use std::future::Future;

use crate::libnmxc::{Nmxc, NmxcError, nmxc_model as nmx};

use tokio::time::Instant;

/// NMX-C responses or a read error for each requested resource.
///
/// Successful responses are kept when another read fails. Reads run in sequence
/// and can reflect different points in time.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) struct FabricSamples {
    /// Controller configuration state and controller health.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) domain:
        Result<nmx::NmxDomainStateInfo, String>,

    /// Device identities, topology, and operational port states.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) topology:
        Result<nmx::NmxFmTopologyInfo, String>,

    /// Switch-node identities and reported health.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) switch_nodes:
        Result<nmx::NmxGetSwitchNodeInfoListResponse, String>,

    /// Compute-node identities and reported health.
    pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) compute_nodes:
        Result<nmx::NmxGetComputeNodeInfoListResponse, String>,
}

/// Reads the 4 NMX-C resources in sequence without changing device configuration.
///
/// When supplied, the deadline applies to the whole sequence. Completed responses
/// are kept. Raw transport errors are omitted because they may contain credentials.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) async fn sample_group(
    client: &mut dyn Nmxc,
    gateway_id: &str,
    deadline: Option<Instant>,
) -> FabricSamples {
    let domain = sample_resource(
        client.get_domain_state_info(gateway_id),
        deadline,
        "controller state and health read unavailable",
    )
    .await;

    let topology = sample_resource(
        client.get_topology_info(gateway_id),
        deadline,
        "device topology and operational link read unavailable",
    )
    .await;

    let switch_nodes = sample_resource(
        client.get_switch_node_info_list(gateway_id),
        deadline,
        "switch health read unavailable",
    )
    .await;

    let compute_nodes = sample_resource(
        client.get_compute_node_info_list(gateway_id),
        deadline,
        "compute health read unavailable",
    )
    .await;

    FabricSamples {
        domain,
        topology,
        switch_nodes,
        compute_nodes,
    }
}

/// Applies an optional deadline and replaces read errors with the supplied message.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) async fn sample_resource<T>(
    read: impl Future<Output = Result<T, NmxcError>>,
    deadline: Option<Instant>,
    unavailable: &str,
) -> Result<T, String> {
    let result = if let Some(deadline) = deadline {
        if deadline <= Instant::now() {
            return Err(unavailable.to_owned());
        }

        tokio::time::timeout_at(deadline, read)
            .await
            .map_err(|_| unavailable.to_owned())?
    } else {
        read.await
    };

    result.map_err(|_| unavailable.to_owned())
}
