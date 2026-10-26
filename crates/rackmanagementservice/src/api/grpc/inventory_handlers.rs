/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::Arc;

use futures::StreamExt;
use librms::protos::rack_manager as rm;
use serde_json::Value;

use super::server::{RackManagerServiceImpl, find_rack};
use crate::api::grpc::conversions::{domain_node_type_to_proto, flatten_node_info};
use crate::api::grpc::node_type_resolver::{
    INVENTORY_PROFILE_ATTRIBUTE, domain_node_type_to_descriptor, resolve_node_info,
    resolve_node_type,
};
use crate::domain::node::{Node, NodeKind, device_info_fetch_timeout};
use crate::domain::rack::{NodeConfig, RACK_POWER_BUSY_MESSAGE};
use crate::nodes::NodeInstance;

/// Maximum number of node device-info reads performed concurrently during a
/// fan-out. Bounds how many switch/BMC sessions RMS opens at once for a large
/// rack while still turning a sequential sum-of-latencies into a bounded
/// max-of-latencies.
const DEVICE_INFO_FETCH_CONCURRENCY: usize = 16;

impl RackManagerServiceImpl {
    pub(crate) async fn handle_list_node_inventory(
        &self,
        _req: tonic::Request<rm::ListNodeInventoryRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListNodeInventoryResponse>, tonic::Status> {
        let mut resp = rm::ListNodeInventoryResponse { nodes: Vec::new() };

        for rack in self.rack_manager.list_racks() {
            for node in rack.list_nodes() {
                let info = node.get_info();
                let mut ni = rm::NodeInventoryInfo {
                    node_id: node.id().to_owned(),
                    rack_id: node.rack_id().to_owned(),
                    r#type: domain_node_type_to_proto(node.node_type()).into(),
                    node_descriptor: Some(domain_node_type_to_descriptor(node.node_type())),
                    ip_address: info.get("host").cloned().unwrap_or_default(),
                    port: info.get("port").and_then(|p| p.parse().ok()).unwrap_or(443),
                    mac_address: info.get("macAddress").cloned().unwrap_or_default(),
                    ..Default::default()
                };
                if let Some(policy) = node.expected_inventory_policy() {
                    ni.node_descriptor
                        .get_or_insert_default()
                        .attributes
                        .insert(
                            INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                            policy.profile.to_string(),
                        );
                }
                for i in 0.. {
                    match info.get(&format!("hostMac_{i}")) {
                        Some(v) => ni.host_mac_addresses.push(v.clone()),
                        None => break,
                    }
                }
                for i in 0.. {
                    match info.get(&format!("hostIp_{i}")) {
                        Some(v) => ni.host_ip_addresses.push(v.clone()),
                        None => break,
                    }
                }
                resp.nodes.push(ni);
            }
        }
        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_create_nodes(
        &self,
        req: tonic::Request<rm::CreateNodesRequest>,
    ) -> std::result::Result<tonic::Response<rm::CreateNodesResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut response = rm::OperationResponse {
            status: rm::ReturnCode::Success.into(),
            message: String::new(),
        };
        let mut success = 0u32;
        let mut failed = 0u32;
        let mut messages = Vec::new();
        let nodes = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        let total_nodes = nodes.len() as u32;

        if total_nodes == 0 {
            response.status = rm::ReturnCode::Failure.into();
            response.message = "no nodes to create".into();
            return Ok(tonic::Response::new(rm::CreateNodesResponse {
                response: Some(response),
                stats: Some(rm::NodeOperationStats {
                    total_nodes,
                    successful_nodes: 0,
                    failed_nodes: 0,
                }),
            }));
        }

        // Process each node: auto-create rack if needed, validate, add
        for ni in &nodes {
            let node_type = match resolve_node_info(ni) {
                Ok(node_type) => node_type,
                Err(error) => {
                    tracing::error!(node = %ni.node_id, "unknown node type");
                    messages.push(format!("{}: {error}", ni.node_id));
                    failed += 1;
                    continue;
                }
            };

            let expected_inventory = match self.expected_inventory_policy_for_node(ni) {
                Ok(policy) => policy,
                Err(e) => {
                    tracing::error!(
                        node = ni.node_id,
                        rack = ni.rack_id,
                        error = e.message,
                        "invalid expected inventory profile"
                    );
                    messages.push(format!("{}: {}", ni.node_id, e.message));
                    failed += 1;
                    continue;
                }
            };

            let rack_type = node_type.product_family().rack_type();

            let flat = match flatten_node_info(ni) {
                Ok(flat) => flat,
                Err(e) => {
                    let message = e.message;
                    tracing::error!(
                        node = ni.node_id,
                        rack = ni.rack_id,
                        error = message,
                        "invalid endpoint credentials"
                    );

                    messages.push(format!("{}: {}", ni.node_id, message));
                    failed += 1;
                    continue;
                }
            };

            // Inventory is stateful RMS-owned configuration, so every
            // registered node needs a BMC endpoint. RMS uses it as the stable
            // management endpoint for inventory and Redfish power operations.
            let bmc_endpoint = match flat.bmc_endpoint() {
                Ok(endpoint) => self.bmc_endpoint_with_rms_tls_policy(endpoint),
                Err(e) => {
                    let message = e.message;
                    tracing::error!(
                        node = ni.node_id,
                        rack = ni.rack_id,
                        error = message,
                        "invalid BMC endpoint"
                    );

                    messages.push(format!("{}: {}", ni.node_id, message));
                    failed += 1;
                    continue;
                }
            };
            // Host endpoints are optional at registration time. Switches can
            // be created BMC-only to match legacy inventory behavior; host
            // workflows validate host_endpoint when a later RPC needs
            // NVUE/NVOS or in-band firmware access.
            let host_endpoint_result = if node_type.kind() == NodeKind::Switch {
                flat.optional_switch_host_management_endpoint()
            } else if node_type.supports_flint_inband_firmware() {
                flat.optional_compute_host_ssh_endpoint()
            } else {
                flat.optional_host_endpoint()
            };
            let host_endpoint = match host_endpoint_result {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    let message = e.message;
                    tracing::error!(
                        node = ni.node_id,
                        rack = ni.rack_id,
                        error = message,
                        "invalid host endpoint"
                    );

                    messages.push(format!("{}: {}", ni.node_id, message));
                    failed += 1;
                    continue;
                }
            };

            let config = NodeConfig {
                id: ni.node_id.clone(),
                node_type,
                bmc_endpoint: Some(bmc_endpoint),
                host_endpoint,
                expected_inventory,
            };

            // Create the node and add it to its rack as one atomic step,
            // creating the rack if it does not exist yet. The rack is built and
            // persisted only once a node has been added successfully, and
            // concurrent CreateNodes calls for the same new rack converge on a
            // single instance.
            let add_result =
                self.rack_manager
                    .with_rack_for_node(&ni.rack_id, rack_type, |target| {
                        let node = target.create_node(&config)?;
                        target.add_node(&config.id, node)
                    });
            if let Err(e) = add_result {
                tracing::error!(node = %ni.node_id, rack = %ni.rack_id, error = %e.message, "failed to add node to rack");
                messages.push(format!("{}: {}", ni.node_id, e.message));
                failed += 1;
                continue;
            }
            success += 1;
        }

        messages.push(format!("{success} added, {failed} failed"));
        response.status = if failed == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();
        response.message = messages.join("\n");
        Ok(tonic::Response::new(rm::CreateNodesResponse {
            response: Some(response),
            stats: Some(rm::NodeOperationStats {
                total_nodes,
                successful_nodes: success,
                failed_nodes: failed,
            }),
        }))
    }

    pub(crate) async fn handle_update_node(
        &self,
        _req: tonic::Request<rm::UpdateNodeRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateNodeResponse>, tonic::Status> {
        Ok(tonic::Response::new(rm::UpdateNodeResponse {
            response: Some(rm::OperationResponse {
                status: rm::ReturnCode::Failure.into(),
                message: "UpdateNode not yet implemented".into(),
            }),
        }))
    }

    pub(crate) async fn handle_delete_node(
        &self,
        req: tonic::Request<rm::DeleteNodeRequest>,
    ) -> std::result::Result<tonic::Response<rm::DeleteNodeResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut response = rm::OperationResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                response.message = format!("rack not found: {}", r.rack_id);
                return Ok(tonic::Response::new(rm::DeleteNodeResponse {
                    response: Some(response),
                }));
            }
        };

        // Registered power operations resolve their node from the current
        // inventory. Block deletes while one is active so it cannot lose its
        // node mid-operation.
        let Ok(_power_guard) = rack.try_power_operation_guard() else {
            tracing::warn!(
                node = r.node_id,
                rack = r.rack_id,
                "{}",
                RACK_POWER_BUSY_MESSAGE
            );
            response.message = RACK_POWER_BUSY_MESSAGE.to_owned();
            return Ok(tonic::Response::new(rm::DeleteNodeResponse {
                response: Some(response),
            }));
        };

        match rack.remove_node(&r.node_id) {
            Ok(()) => {
                tracing::info!(node = %r.node_id, rack = %r.rack_id, "node removed");
                response.status = rm::ReturnCode::Success.into();
                response.message = format!("node {} removed", r.node_id);
            }
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e.message, "failed to remove node");
                response.message = e.message;
            }
        }
        Ok(tonic::Response::new(rm::DeleteNodeResponse {
            response: Some(response),
        }))
    }

    pub(crate) async fn handle_list_racks(
        &self,
        _req: tonic::Request<rm::ListRacksRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListRacksResponse>, tonic::Status> {
        let ids = self.rack_manager.list_rack_ids();
        Ok(tonic::Response::new(rm::ListRacksResponse {
            rack_ids: ids,
        }))
    }

    // ── Device info (tray + chassis location) ──

    pub(crate) async fn handle_get_node_device_info(
        &self,
        req: tonic::Request<rm::GetNodeDeviceInfoRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetNodeDeviceInfoResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::GetNodeDeviceInfoResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            device_info: None,
        };

        if r.rack_id.is_empty() || r.node_id.is_empty() {
            resp.message = "rack_id and node_id are required".into();
            return Ok(tonic::Response::new(resp));
        }

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(rk) => rk,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.message = format!("rack not found: {}", r.rack_id);
                return Ok(tonic::Response::new(resp));
            }
        };

        let node = match rack.find_node(&r.node_id) {
            Some(n) => n,
            None => {
                resp.message = format!("node not found: {}", r.node_id);
                return Ok(tonic::Response::new(resp));
            }
        };

        // Reuse the shared deadline-bounded read (client init + fetch + shaping)
        // so the single-node and batch paths stay in lockstep: one per-kind
        // timeout, one copy of the init/fetch/populate logic, and identical
        // error / no-device-info semantics.
        match self
            .read_node_device_info_with_deadline(node.id(), node.as_ref())
            .await
        {
            Ok(Some(info)) => {
                resp.device_info = Some(info);
                resp.status = rm::ReturnCode::Success.into();
                resp.message = "Device info retrieved successfully".into();
            }
            Ok(None) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.message = "Device info is not available for this node type".into();
            }
            Err(message) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %message, "device-info read failed");
                resp.message = message;
            }
        }

        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_list_node_device_info_by_node_type(
        &self,
        req: tonic::Request<rm::ListNodeDeviceInfoByNodeTypeRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListNodeDeviceInfoByNodeTypeResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut result = rm::ListNodeDeviceInfoByNodeTypeResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_device_details: Vec::new(),
            stats: Some(rm::NodeOperationStats {
                total_nodes: 0,
                successful_nodes: 0,
                failed_nodes: 0,
            }),
        };

        if r.rack_id.is_empty() {
            result.message = "rack_id is required".into();
            return Ok(tonic::Response::new(result));
        }

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(rk) => rk,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                result.message = format!("rack not found: {}", r.rack_id);
                return Ok(tonic::Response::new(result));
            }
        };

        let target_type = match resolve_node_type(Some(r.node_type), r.node_descriptor.as_ref()) {
            Ok(node_type) => node_type,
            Err(error) => {
                result.message = error.to_string();
                return Ok(tonic::Response::new(result));
            }
        };
        let targets: Vec<_> = rack
            .list_nodes()
            .into_iter()
            .filter(|node| node.node_type() == target_type)
            .collect();
        let total_nodes = targets.len() as u32;

        // Fan out the per-node device-info reads concurrently with a bounded
        // degree and a per-node deadline. One unreachable node then costs its
        // own timeout ceiling rather than stalling every node behind it, and a
        // large rack's latency becomes the slowest node instead of the sum.
        let outcomes: Vec<NodeDeviceInfoOutcome> =
            futures::stream::iter(targets.into_iter().enumerate().map(
                |(index, node)| async move {
                    let node_id = node.id().to_owned();
                    let result = self
                        .read_node_device_info_with_deadline(&node_id, node.as_ref())
                        .await;
                    (index, node_id, result)
                },
            ))
            .buffer_unordered(DEVICE_INFO_FETCH_CONCURRENCY)
            .collect()
            .await;

        let (details, errors) = partition_device_info_outcomes(outcomes);
        result.node_device_details = details;

        if errors.is_empty() {
            result.status = rm::ReturnCode::Success.into();
            result.message = "Device info retrieved successfully".into();
        } else {
            let joined = errors.join("; ");
            tracing::error!(rack = %r.rack_id, errors = %joined, "ListNodeDeviceInfoByNodeType completed with errors");
            result.message = format!("Completed with errors: {joined}");
        }
        let failed_nodes = errors.len() as u32;
        result.stats = Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes: total_nodes.saturating_sub(failed_nodes),
            failed_nodes,
        });
        Ok(tonic::Response::new(result))
    }

    pub(crate) async fn handle_batch_get_node_device_info(
        &self,
        req: tonic::Request<rm::BatchGetNodeDeviceInfoRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchGetNodeDeviceInfoResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut result = rm::BatchGetNodeDeviceInfoResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_device_details: Vec::new(),
            stats: Some(rm::NodeOperationStats {
                total_nodes: 0,
                successful_nodes: 0,
                failed_nodes: 0,
            }),
        };

        let nodes: Vec<rm::NodeInfo> = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        let total_nodes = nodes.len() as u32;
        if nodes.is_empty() {
            result.message = "No nodes specified in request".into();
            return Ok(tonic::Response::new(result));
        }

        // Phase 1: cheap synchronous validation + ephemeral node construction
        // (no network I/O). Failures become error entries keyed by request
        // index so the final output preserves request order.
        let mut outcomes: Vec<NodeDeviceInfoOutcome> = Vec::new();
        let mut ready: Vec<(usize, String, Arc<NodeInstance>)> = Vec::new();
        for (index, node_info) in nodes.iter().enumerate() {
            match self.build_batch_ephemeral_node(node_info) {
                Ok(Some(node)) => ready.push((index, node_info.node_id.clone(), node)),
                // Node type exposes no device info: record the "no device info"
                // outcome directly (counted as a success, no details emitted).
                Ok(None) => outcomes.push((index, node_info.node_id.clone(), Ok(None))),
                Err(message) => outcomes.push((index, node_info.node_id.clone(), Err(message))),
            }
        }

        // Phase 2: fan out the device-info reads (NVUE init + fetch)
        // concurrently with a bounded degree and a per-node deadline, so one
        // unreachable node cannot serialize the whole batch behind its timeout.
        let fetched: Vec<NodeDeviceInfoOutcome> =
            futures::stream::iter(ready.into_iter().map(|(index, node_id, node)| async move {
                let result = self
                    .read_node_device_info_with_deadline(&node_id, node.as_ref())
                    .await;
                (index, node_id, result)
            }))
            .buffer_unordered(DEVICE_INFO_FETCH_CONCURRENCY)
            .collect()
            .await;
        outcomes.extend(fetched);

        let (details, errors) = partition_device_info_outcomes(outcomes);
        result.node_device_details = details;

        if errors.is_empty() {
            result.status = rm::ReturnCode::Success.into();
            result.message = "Device info retrieved successfully".into();
        } else {
            let joined = errors.join("; ");
            tracing::error!(errors = %joined, "BatchGetNodeDeviceInfo completed with errors");
            result.message = format!("Completed with errors: {joined}");
            // If at least some nodes succeeded, still return FAILURE overall
            // but include the successful results
        }
        let failed_nodes = errors.len() as u32;
        result.stats = Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes: total_nodes.saturating_sub(failed_nodes),
            failed_nodes,
        });
        Ok(tonic::Response::new(result))
    }

    /// Read a single node's device info under a bounded deadline: configure the
    /// NVUE client, fetch device info, and shape it into a `NodeDeviceInfo`.
    ///
    /// Returns `Ok(Some(info))` on success, `Ok(None)` when the node type does
    /// not expose device info (skipped silently by callers), and `Err(message)`
    /// (without the `node_id=` prefix, which the caller adds) for an init/fetch
    /// failure, a populate failure, or a deadline overrun.
    ///
    /// The per-node-kind [`device_info_fetch_timeout`] backstop bounds only the
    /// client I/O, split across the two phases that actually perform it:
    ///
    /// - NVUE client setup, bounded here. It is not serialized by the node
    ///   `op_lock`, so bounding it cannot turn lock contention into a spurious
    ///   timeout, and it catches a stuck TLS/setup before it can stall a fan-out.
    /// - The device read itself, which takes the `op_lock` and then bounds its
    ///   own I/O internally (see `NvidiaGb200Compute::get_mnnvlink_topology` and
    ///   the `SwitchDeviceInfo::get_chassis_location_info` impl). Deliberately
    ///   *not* wrapped here: doing so would charge the `op_lock` wait against the
    ///   deadline, so an unrelated long-running operation on the same node (e.g.
    ///   a firmware upload) would spuriously fail an otherwise healthy read
    ///   instead of queuing behind it.
    ///
    /// A kind with no device-info I/O (powershelf) returns `None` from
    /// [`device_info_fetch_timeout`]; setup is then awaited unwrapped, since its
    /// `NodeInstance::get_device_info` is a synchronous `Ok(None)` and its NVUE
    /// client is absent, so there is nothing that could hang to bound.
    async fn read_node_device_info_with_deadline(
        &self,
        node_id: &str,
        node: &NodeInstance,
    ) -> std::result::Result<Option<rm::NodeDeviceInfo>, String> {
        let setup = self.initialize_nvue_client(node.nvue_client(), None);
        match device_info_fetch_timeout(node.node_type()) {
            Some(setup_timeout) => match tokio::time::timeout(setup_timeout, setup).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error.message),
                Err(_elapsed) => {
                    return Err(format!(
                        "device-info client setup exceeded {}s deadline",
                        setup_timeout.as_secs()
                    ));
                }
            },
            None => setup.await.map_err(|error| error.message)?,
        }

        let device_info = node
            .get_device_info()
            .await
            .map_err(|error| error.message)?;

        let Some(device_info) = device_info else {
            return Ok(None);
        };

        let mut info = rm::NodeDeviceInfo {
            node_id: node_id.to_owned(),
            ..Default::default()
        };
        populate_node_device_info(&device_info, &mut info)?;
        Ok(Some(info))
    }

    /// Validate a batch `NodeInfo` and build the ephemeral node used for a
    /// device-info read. This is the synchronous, network-free half of the
    /// batch handler.
    ///
    /// Returns `Ok(Some(node))` for a node type that exposes device info,
    /// `Ok(None)` for a node type that has none (mirrors
    /// [`NodeInstance::get_device_info`], so those are reported as "no device
    /// info" rather than a failure), and `Err(message)` (bare; the caller
    /// prefixes `node_id=`) on a validation failure. Kept separate so the async
    /// device-info reads can be fanned out concurrently.
    fn build_batch_ephemeral_node(
        &self,
        node_info: &rm::NodeInfo,
    ) -> std::result::Result<Option<Arc<NodeInstance>>, String> {
        let node_type = resolve_node_info(node_info).map_err(|error| error.to_string())?;

        // Power shelves expose no device info (every powershelf variant returns
        // `Ok(None)` from `NodeInstance::get_device_info`), so short-circuit to
        // the "no device info" outcome without demanding credentials/endpoints
        // or constructing an ephemeral node. This matches the single-node and
        // list-by-type handlers (which read the same `Ok(None)` from an already
        // registered node) and keeps every powershelf kind consistent: none is
        // failed for missing/malformed creds when there is nothing to read.
        if node_type.kind() == NodeKind::Powershelf {
            return Ok(None);
        }

        let flat = flatten_node_info(node_info).map_err(|e| e.message)?;

        if flat.creds_for_node_type(node_type).is_none() {
            let source = if node_type.kind() == NodeKind::Switch {
                "host"
            } else {
                "BMC"
            };
            return Err(format!("missing {source} credentials"));
        }

        // Switch device-info reads use host/NVUE. Keep BMC optional here so
        // host-only stateless switch reads work; compute and powershelf still
        // require BMC for Redfish.
        let is_switch = node_type.kind() == NodeKind::Switch;
        let bmc_endpoint = if is_switch {
            self.optional_bmc_endpoint_with_rms_tls_policy(
                flat.optional_bmc_endpoint().map_err(|e| e.message)?,
            )
        } else {
            self.optional_bmc_endpoint_with_rms_tls_policy(
                flat.bmc_endpoint().map(Some).map_err(|e| e.message)?,
            )
        };

        let host_endpoint = if is_switch {
            flat.switch_host_management_endpoint()
                .map(Some)
                .map_err(|e| e.message)?
        } else if node_type.supports_flint_inband_firmware() {
            flat.optional_compute_host_ssh_endpoint()
                .map_err(|e| e.message)?
        } else {
            flat.optional_host_endpoint().map_err(|e| e.message)?
        };

        let config = NodeConfig {
            id: node_info.node_id.clone(),
            node_type,
            bmc_endpoint,
            host_endpoint,
            expected_inventory: None,
        };

        NodeInstance::create(&config, node_info.rack_id.as_str())
            .map(Some)
            .map_err(|e| e.message)
    }
}

/// Per-node outcome of a device-info fan-out, keyed by the caller's original
/// index so results can be reassembled deterministically after a concurrent
/// (out-of-order) fan-out. `Ok(None)` marks a node type without device info.
type NodeDeviceInfoOutcome = (
    usize,
    String,
    std::result::Result<Option<rm::NodeDeviceInfo>, String>,
);

/// Split indexed per-node outcomes into ordered device-info details and error
/// strings. Both are returned in ascending-index order, so a concurrent fan-out
/// still yields deterministic, request-order output. `Ok(None)` outcomes are
/// node types without device info and are dropped (the caller counts them as
/// successes). Error entries are prefixed with `node_id=<id>:`.
fn partition_device_info_outcomes(
    mut outcomes: Vec<NodeDeviceInfoOutcome>,
) -> (Vec<rm::NodeDeviceInfo>, Vec<String>) {
    outcomes.sort_by_key(|(index, _, _)| *index);

    let mut details = Vec::new();
    let mut errors = Vec::new();
    for (_, node_id, outcome) in outcomes {
        match outcome {
            Ok(Some(info)) => details.push(info),
            Ok(None) => {}
            Err(message) => errors.push(format!("node_id={node_id}: {message}")),
        }
    }
    (details, errors)
}

// Populate `NodeDeviceInfo` optional numeric fields from a JSON payload shaped like
// either `{chassis_sn, slot_number, tray_index}` (compute) or
// `{switch_info: {chassis_sn, slot_number, tray_index, ...}}` (switch).
// Numeric string values are parsed as integers; empty strings are treated as
// "absent". Real topology fields may be absent, alphanumeric, or larger than
// the current proto can represent, so unrepresentable optional values are
// omitted instead of failing the entire device-info response.
fn populate_node_device_info(
    source: &Value,
    target: &mut rm::NodeDeviceInfo,
) -> std::result::Result<(), String> {
    let payload = source.get("switch_info").unwrap_or(source);

    target.chassis_sn = extract_optional_i64(payload, "chassis_sn");
    target.slot_number = extract_optional_u32(payload, "slot_number");
    target.tray_index = extract_optional_u32(payload, "tray_index");
    Ok(())
}

fn extract_optional_i64(payload: &Value, field: &str) -> Option<i64> {
    match payload.get(field)? {
        Value::Number(n) => n.as_i64(),
        Value::String(s) if !s.is_empty() => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn extract_optional_u32(payload: &Value, field: &str) -> Option<u32> {
    extract_optional_i64(payload, field).and_then(|v| u32::try_from(v).ok())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
    use crate::domain::node::{
        COMPUTE_DEVICE_INFO_FETCH_TIMEOUT, NodeType, SWITCH_DEVICE_INFO_FETCH_TIMEOUT,
    };
    use crate::libnmxc::TlsMaterialStore;
    use crate::orchestrator::job_tracker::JobTracker;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;

    fn test_service() -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::new()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            switch_tls_roots: SwitchTlsRoots::default(),
            sftp_upload_options: crate::transport::ssh::SftpUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        }
    }

    #[tokio::test]
    async fn create_nodes_rejects_empty_request() {
        let service = test_service();

        let response = service
            .handle_create_nodes(tonic::Request::new(rm::CreateNodesRequest { nodes: None }))
            .await
            .expect("handler should not error")
            .into_inner();

        let op = response.response.expect("operation response present");
        assert_eq!(op.status, rm::ReturnCode::Failure as i32);
        assert_eq!(op.message, "no nodes to create");

        let stats = response.stats.expect("stats present");
        assert_eq!(stats.total_nodes, 0);
        assert_eq!(stats.successful_nodes, 0);
        assert_eq!(stats.failed_nodes, 0);

        assert!(service.rack_manager.list_racks().is_empty());
    }

    #[tokio::test]
    async fn create_nodes_rejects_empty_node_list() {
        let service = test_service();

        let response = service
            .handle_create_nodes(tonic::Request::new(rm::CreateNodesRequest {
                nodes: Some(rm::NodeSet { nodes: Vec::new() }),
            }))
            .await
            .expect("handler should not error")
            .into_inner();

        let op = response.response.expect("operation response present");
        assert_eq!(op.status, rm::ReturnCode::Failure as i32);
        assert_eq!(op.message, "no nodes to create");

        let stats = response.stats.expect("stats present");
        assert_eq!(stats.total_nodes, 0);
        assert_eq!(stats.successful_nodes, 0);
        assert_eq!(stats.failed_nodes, 0);

        assert!(service.rack_manager.list_racks().is_empty());
    }

    #[tokio::test]
    async fn create_nodes_does_not_initialize_nvue_tls() {
        let tls_root = tempfile::tempdir().unwrap();
        let mut service = test_service();

        service.switch_tls_roots = SwitchTlsRoots {
            client_tls: Some(TlsMaterialStore::new(tls_root.path())),
            default_domain: Some("site-wide".into()),
            ..SwitchTlsRoots::default()
        };

        let endpoint = rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: "192.0.2.1".into(),
                mac_address: "00:11:22:33:44:55".into(),
                host_name: Some("switch.example.com".into()),
            }),
            port: 443,
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "admin".into(),
                    password: "password".into(),
                })),
            }),
        };

        let node = rm::NodeInfo {
            node_id: "switch-1".into(),
            rack_id: "rack-1".into(),
            r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
            bmc_endpoint: Some(endpoint.clone()),
            host_endpoint: Some(endpoint),
            ..Default::default()
        };

        let response = service
            .handle_create_nodes(tonic::Request::new(rm::CreateNodesRequest {
                nodes: Some(rm::NodeSet { nodes: vec![node] }),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.stats.unwrap().successful_nodes, 1);
        assert!(service.rack_manager.find_rack("rack-1").is_some());
    }

    #[tokio::test]
    async fn create_and_list_node_round_trips_inventory_profile() {
        let profiles: crate::config::ExpectedInventoryProfiles = serde_json::from_value(json!({
            "gb200-compute-variant-a": ["FW_BMC_0", "HGX_FW_GPU_0"]
        }))
        .unwrap();
        let mut service = test_service();
        service.expected_inventory_catalog =
            crate::config::ExpectedInventoryCatalog::from(&profiles);
        let endpoint = rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: "192.0.2.10".to_owned(),
                mac_address: "00:11:22:33:44:55".to_owned(),
                host_name: None,
            }),
            port: 443,
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "admin".to_owned(),
                    password: "password".to_owned(),
                })),
            }),
        };
        let node = rm::NodeInfo {
            node_id: "compute-1".to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            node_descriptor: Some(rm::NodeDescriptor {
                attributes: HashMap::from([(
                    INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                    " gb200-compute-variant-a ".to_owned(),
                )]),
            }),
            bmc_endpoint: Some(endpoint),
            ..Default::default()
        };

        let created = service
            .handle_create_nodes(tonic::Request::new(rm::CreateNodesRequest {
                nodes: Some(rm::NodeSet { nodes: vec![node] }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(created.stats.unwrap().successful_nodes, 1);

        let inventory = service
            .handle_list_node_inventory(
                tonic::Request::new(rm::ListNodeInventoryRequest::default()),
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(inventory.nodes.len(), 1);
        let descriptor = inventory.nodes[0].node_descriptor.as_ref().unwrap();
        assert_eq!(
            descriptor
                .attributes
                .get(INVENTORY_PROFILE_ATTRIBUTE)
                .map(String::as_str),
            Some("gb200-compute-variant-a")
        );
        assert_eq!(
            descriptor.attributes.get("role").map(String::as_str),
            Some("compute")
        );
    }

    #[tokio::test]
    async fn create_node_rejects_unresolved_inventory_profile_without_registering_rack() {
        for (profile, expected_error) in [
            ("unknown-profile", "unknown inventory_profile"),
            ("  ", "inventory_profile must not be empty"),
        ] {
            let service = test_service();
            let node = rm::NodeInfo {
                node_id: "compute-1".to_owned(),
                rack_id: "rack-1".to_owned(),
                r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                node_descriptor: Some(rm::NodeDescriptor {
                    attributes: HashMap::from([(
                        INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                        profile.to_owned(),
                    )]),
                }),
                ..Default::default()
            };

            let response = service
                .handle_create_nodes(tonic::Request::new(rm::CreateNodesRequest {
                    nodes: Some(rm::NodeSet { nodes: vec![node] }),
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(response.stats.unwrap().failed_nodes, 1);
            assert!(response.response.unwrap().message.contains(expected_error));
            assert!(service.rack_manager.list_racks().is_empty());
        }
    }

    #[test]
    fn populate_node_device_info_preserves_numeric_chassis_serials() {
        let source = json!({
            "chassis_sn": "1784124020070",
            "slot_number": 69,
            "tray_index": 103,
        });
        let mut target = rm::NodeDeviceInfo {
            node_id: "node-1".to_owned(),
            ..Default::default()
        };

        populate_node_device_info(&source, &mut target).unwrap();

        assert_eq!(target.chassis_sn, Some(1_784_124_020_070));
        assert_eq!(target.slot_number, Some(69));
        assert_eq!(target.tray_index, Some(103));
    }

    #[test]
    fn populate_node_device_info_tolerates_alphanumeric_chassis_serials() {
        let source = json!({
            "chassis_sn": "MT2504601BF4",
            "slot_number": 69,
            "tray_index": 103,
        });
        let mut target = rm::NodeDeviceInfo {
            node_id: "node-1".to_owned(),
            ..Default::default()
        };

        populate_node_device_info(&source, &mut target).unwrap();

        assert_eq!(target.chassis_sn, None);
        assert_eq!(target.slot_number, Some(69));
        assert_eq!(target.tray_index, Some(103));
    }

    #[test]
    fn populate_node_device_info_tolerates_unrepresentable_optional_numbers() {
        let source = json!({
            "chassis_sn": 999999999999999999_i64,
            "slot_number": 999999999999_i64,
            "tray_index": "not-a-number",
        });
        let mut target = rm::NodeDeviceInfo {
            node_id: "node-1".to_owned(),
            ..Default::default()
        };

        populate_node_device_info(&source, &mut target).unwrap();

        assert_eq!(target.chassis_sn, Some(999999999999999999_i64));
        assert_eq!(target.slot_number, None);
        assert_eq!(target.tray_index, None);
    }

    /// Device-info behavior a [`DeviceInfoMockNode`] should exhibit when the
    /// fan-out reads it.
    enum DeviceInfoBehavior {
        /// Return this payload as the node's device info.
        Value(Value),
        /// Node type without device info (`Ok(None)`).
        None,
        /// Fail the fetch with this message.
        Error(String),
        /// Sleep far past the deadline so the read must be timed out. Under a
        /// paused clock this resolves via virtual time rather than wall time.
        Hang,
    }

    /// A [`Node`] whose only interesting behavior is [`Node::get_device_info`],
    /// letting the inventory fan-out be exercised without any real device
    /// client. Wrapped via [`NodeInstance::from_test_node`], which delegates
    /// `get_device_info` to this impl.
    struct DeviceInfoMockNode {
        id: String,
        node_type: NodeType,
        behavior: DeviceInfoBehavior,
    }

    #[async_trait::async_trait]
    impl Node for DeviceInfoMockNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn rack_id(&self) -> &str {
            "rack-1"
        }

        fn node_type(&self) -> NodeType {
            self.node_type
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        async fn get_device_info(&self) -> crate::utilities::error::Result<Option<Value>> {
            match &self.behavior {
                DeviceInfoBehavior::Value(value) => Ok(Some(value.clone())),
                DeviceInfoBehavior::None => Ok(None),
                DeviceInfoBehavior::Error(message) => {
                    Err(crate::utilities::error::RmsError::internal(message.clone()))
                }
                DeviceInfoBehavior::Hang => {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                    Ok(None)
                }
            }
        }
    }

    /// Register a rack of mock device-info nodes so the inventory handlers can
    /// be driven end-to-end without real device clients.
    fn add_mock_nodes(
        service: &RackManagerServiceImpl,
        rack_id: &str,
        nodes: Vec<(&str, NodeType, DeviceInfoBehavior)>,
    ) {
        let rack_type = NodeType::SwitchGb200Nvidia.product_family().rack_type();
        service
            .rack_manager
            .with_rack_for_node(rack_id, rack_type, |rack| {
                for (id, node_type, behavior) in nodes {
                    let mock = DeviceInfoMockNode {
                        id: id.to_owned(),
                        node_type,
                        behavior,
                    };
                    rack.add_node(id, Arc::new(NodeInstance::from_test_node(mock)))?;
                }
                Ok(())
            })
            .expect("mock rack + nodes should be created");
    }

    fn by_type_request(
        rack_id: &str,
        node_type: rm::NodeType,
    ) -> rm::ListNodeDeviceInfoByNodeTypeRequest {
        rm::ListNodeDeviceInfoByNodeTypeRequest {
            rack_id: rack_id.to_owned(),
            node_type: node_type as i32,
            ..Default::default()
        }
    }

    #[test]
    fn partition_device_info_outcomes_orders_by_index_and_buckets_outcomes() {
        // Deliberately out of index order to prove the sort restores order.
        let outcomes: Vec<NodeDeviceInfoOutcome> = vec![
            (
                2,
                "c".to_owned(),
                Ok(Some(rm::NodeDeviceInfo {
                    node_id: "c".to_owned(),
                    chassis_sn: Some(3),
                    ..Default::default()
                })),
            ),
            (0, "a".to_owned(), Err("boom".to_owned())),
            (1, "b".to_owned(), Ok(None)),
            (
                3,
                "d".to_owned(),
                Ok(Some(rm::NodeDeviceInfo {
                    node_id: "d".to_owned(),
                    chassis_sn: Some(4),
                    ..Default::default()
                })),
            ),
        ];

        let (details, errors) = partition_device_info_outcomes(outcomes);

        // `Ok(None)` is dropped; errors carry the `node_id=` prefix.
        assert_eq!(errors, vec!["node_id=a: boom".to_owned()]);
        // Details preserve ascending-index order regardless of input order.
        assert_eq!(
            details
                .iter()
                .map(|d| d.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert_eq!(details[0].chassis_sn, Some(3));
        assert_eq!(details[1].chassis_sn, Some(4));
    }

    #[tokio::test]
    async fn list_by_type_fans_out_and_aggregates_success_none_and_error() {
        let service = test_service();
        add_mock_nodes(
            &service,
            "rack-1",
            vec![
                (
                    "sw-ok",
                    NodeType::SwitchGb200Nvidia,
                    DeviceInfoBehavior::Value(json!({
                        "switch_info": { "chassis_sn": 11, "slot_number": 1, "tray_index": 2 }
                    })),
                ),
                (
                    "sw-none",
                    NodeType::SwitchGb200Nvidia,
                    DeviceInfoBehavior::None,
                ),
                (
                    "sw-err",
                    NodeType::SwitchGb200Nvidia,
                    DeviceInfoBehavior::Error("nvue unreachable".to_owned()),
                ),
                // Different node type: must be filtered out and never fetched.
                (
                    "compute-x",
                    NodeType::ComputeGb200Nvidia,
                    DeviceInfoBehavior::Value(json!({ "chassis_sn": 99 })),
                ),
            ],
        );

        let resp = service
            .handle_list_node_device_info_by_node_type(tonic::Request::new(by_type_request(
                "rack-1",
                rm::NodeType::SwitchGb200Nvidia,
            )))
            .await
            .unwrap()
            .into_inner();

        let stats = resp.stats.expect("stats present");
        assert_eq!(stats.total_nodes, 3);
        assert_eq!(stats.failed_nodes, 1);
        assert_eq!(stats.successful_nodes, 2);

        assert_eq!(resp.node_device_details.len(), 1);
        assert_eq!(resp.node_device_details[0].node_id, "sw-ok");
        assert_eq!(resp.node_device_details[0].chassis_sn, Some(11));
        assert_eq!(resp.status, rm::ReturnCode::Failure as i32);
        assert!(
            resp.message.contains("node_id=sw-err: nvue unreachable"),
            "unexpected message: {}",
            resp.message
        );
    }

    #[tokio::test(start_paused = true)]
    async fn list_by_type_times_out_slow_node_without_stalling_others() {
        let service = test_service();
        add_mock_nodes(
            &service,
            "rack-1",
            vec![
                (
                    "sw-fast",
                    NodeType::SwitchGb200Nvidia,
                    DeviceInfoBehavior::Value(json!({ "chassis_sn": 7 })),
                ),
                (
                    "sw-hang",
                    NodeType::SwitchGb200Nvidia,
                    DeviceInfoBehavior::Hang,
                ),
            ],
        );

        // With a paused clock the deadline fires in virtual time, so the hung
        // node cannot serialize the fast node behind it or stall the test.
        let resp = service
            .handle_list_node_device_info_by_node_type(tonic::Request::new(by_type_request(
                "rack-1",
                rm::NodeType::SwitchGb200Nvidia,
            )))
            .await
            .unwrap()
            .into_inner();

        let stats = resp.stats.expect("stats present");
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.failed_nodes, 1);
        assert_eq!(stats.successful_nodes, 1);

        assert_eq!(resp.node_device_details.len(), 1);
        assert_eq!(resp.node_device_details[0].node_id, "sw-fast");
        assert!(
            resp.message.contains("node_id=sw-hang")
                && resp.message.contains("exceeded 90s deadline"),
            "unexpected message: {}",
            resp.message
        );
    }

    fn powershelf_node_info(node_id: &str, node_type: rm::NodeType) -> rm::NodeInfo {
        rm::NodeInfo {
            node_id: node_id.to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(node_type as i32),
            ..Default::default()
        }
    }

    #[test]
    fn build_batch_ephemeral_node_short_circuits_powershelves_to_no_device_info() {
        let service = test_service();

        // Every powershelf kind exposes no device info, so all resolve to
        // `Ok(None)` up front — no credentials/endpoints required and no node
        // built. This notably includes the Liteon variants, which previously
        // still had to pass credential validation to build an ephemeral node.
        for node_type in [
            rm::NodeType::PowershelfGb200Liteon,
            rm::NodeType::PowershelfGb200Delta,
            rm::NodeType::PowershelfGb300Liteon,
            rm::NodeType::PowershelfGb300Delta,
        ] {
            let outcome = service
                .build_batch_ephemeral_node(&powershelf_node_info("psu", node_type))
                .expect("powershelf should not error");
            assert!(
                outcome.is_none(),
                "{node_type:?} should short-circuit to no-device-info without creds"
            );
        }

        // Other node kinds still run validation: a compute node missing BMC
        // credentials is still rejected rather than silently short-circuited.
        let compute = rm::NodeInfo {
            node_id: "compute-1".to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            ..Default::default()
        };
        assert!(
            service.build_batch_ephemeral_node(&compute).is_err(),
            "non-powershelf node kinds must still be validated"
        );
    }

    #[tokio::test]
    async fn batch_device_info_reports_powershelves_as_no_device_info() {
        let service = test_service();

        // Mix Liteon and Delta variants, all without credentials, to prove every
        // powershelf kind is reported uniformly as success/no-details.
        let resp = service
            .handle_batch_get_node_device_info(tonic::Request::new(
                rm::BatchGetNodeDeviceInfoRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![
                            powershelf_node_info("psu-200l", rm::NodeType::PowershelfGb200Liteon),
                            powershelf_node_info("psu-200d", rm::NodeType::PowershelfGb200Delta),
                            powershelf_node_info("psu-300l", rm::NodeType::PowershelfGb300Liteon),
                            powershelf_node_info("psu-300d", rm::NodeType::PowershelfGb300Delta),
                        ],
                    }),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        // Mirrors NodeInstance::get_device_info and the single-node handler:
        // no device info means success with no details, not a failed node.
        let stats = resp.stats.expect("stats present");
        assert_eq!(stats.total_nodes, 4);
        assert_eq!(stats.failed_nodes, 0);
        assert_eq!(stats.successful_nodes, 4);
        assert!(resp.node_device_details.is_empty());
        assert_eq!(resp.status, rm::ReturnCode::Success as i32);
    }

    #[test]
    fn device_info_fetch_timeout_is_keyed_by_node_kind() {
        // Compute device info walks a multi-GET Redfish sequence, so its
        // backstop must sit above the switch (NVUE) ceiling to avoid preempting
        // a slow-but-responsive BMC mid-discovery.
        assert_eq!(
            device_info_fetch_timeout(NodeType::ComputeGb200Nvidia),
            Some(COMPUTE_DEVICE_INFO_FETCH_TIMEOUT)
        );
        assert_eq!(
            device_info_fetch_timeout(NodeType::SwitchGb200Nvidia),
            Some(SWITCH_DEVICE_INFO_FETCH_TIMEOUT)
        );
        // Powershelves expose no device info (get_device_info is a synchronous
        // Ok(None)), so there is nothing to bound and no timeout is armed.
        assert_eq!(
            device_info_fetch_timeout(NodeType::PowershelfGb200Delta),
            None
        );
        assert!(COMPUTE_DEVICE_INFO_FETCH_TIMEOUT > SWITCH_DEVICE_INFO_FETCH_TIMEOUT);
    }
}
