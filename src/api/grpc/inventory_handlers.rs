/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

use serde_json::Value;

use super::server::{RackManagerServiceImpl, find_rack};
use crate::api::grpc::conversions::{
    domain_node_type_to_proto, flatten_node_info, proto_node_type_to_domain,
};
use crate::domain::node::{Node, NodeKind, NodeType};
use crate::domain::rack::{NodeConfig, PowerOnStep, RACK_POWER_BUSY_MESSAGE};
use crate::nodes::NodeInstance;
use librms::protos::rack_manager as rm;

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
                    ip_address: info.get("host").cloned().unwrap_or_default(),
                    port: info.get("port").and_then(|p| p.parse().ok()).unwrap_or(443),
                    mac_address: info.get("macAddress").cloned().unwrap_or_default(),
                    ..Default::default()
                };
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
            let node_type = match proto_node_type_to_domain(ni.r#type.unwrap_or(0)) {
                Some(t) => t,
                None => {
                    tracing::error!(node = %ni.node_id, "unknown node type");
                    messages.push(format!("{}: unknown type", ni.node_id));
                    failed += 1;
                    continue;
                }
            };

            let rack_type = node_type.product_family().rack_type();
            let rack = match self
                .rack_manager
                .find_or_create_rack(&ni.rack_id, rack_type)
            {
                Ok(rack) => rack,
                Err(e) => {
                    tracing::error!(node = %ni.node_id, rack = %ni.rack_id, error = %e.message, "failed to find/create rack");
                    messages.push(format!("{}: {}", ni.node_id, e.message));
                    failed += 1;
                    continue;
                }
            };

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
                Ok(endpoint) => endpoint,
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
            // NVUE/NVOS access.
            let host_endpoint_result = if node_type.kind() == NodeKind::Switch {
                flat.optional_switch_host_management_endpoint()
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
            };

            let node = match rack.create_node(&config) {
                Ok(n) => n,
                Err(e) => {
                    tracing::error!(node = %ni.node_id, rack = %ni.rack_id, error = %e.message, "failed to create node");
                    messages.push(format!("{}: {}", ni.node_id, e.message));
                    failed += 1;
                    continue;
                }
            };

            // Add the node and register the rack as one atomic step. If a
            // concurrent CreateNodes call already registered this rack, the node
            // is added to that instance and our factory rack is discarded; the
            // rack is persisted only once a node has been added successfully.
            let add_result = self
                .rack_manager
                .register_rack_with(&rack, rack_type, |target| target.add_node(&config.id, node));
            if let Err(e) = add_result {
                tracing::error!(node = %ni.node_id, rack = %ni.rack_id, error = %e.message, "failed to add node");
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

        // A rack power sequence depends on the current inventory. Block deletes
        // while the sequence is active so a later step cannot lose its node.
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

    pub(crate) async fn handle_get_rack_power_on_sequence(
        &self,
        req: tonic::Request<rm::GetRackPowerOnSequenceRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetRackPowerOnSequenceResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut resp = rm::GetRackPowerOnSequenceResponse {
            status: rm::ReturnCode::Failure.into(),
            power_on_order: Vec::new(),
            is_valid: false,
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                return Ok(tonic::Response::new(resp));
            }
        };
        let order = rack.get_power_on_order();

        for step in &order {
            resp.power_on_order.push(rm::PowerOnOrderItem {
                node_id: step.node_id.clone(),
                completion_check: Some(rm::CompletionCheck {
                    enabled: step.completion_check,
                    timeout_seconds: step.timeout_seconds,
                }),
            });
        }
        resp.status = rm::ReturnCode::Success.into();
        resp.is_valid = !order.is_empty();
        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_set_rack_power_on_sequence(
        &self,
        req: tonic::Request<rm::SetRackPowerOnSequenceRequest>,
    ) -> std::result::Result<tonic::Response<rm::SetRackPowerOnSequenceResponse>, tonic::Status>
    {
        let r = req.into_inner();

        let rack_id = r.rack_id;
        let power_on_order = r.power_on_order;

        let mut response = rm::OperationResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %rack_id, "rack not found");
                response.message = "rack not found".into();
                return Ok(tonic::Response::new(rm::SetRackPowerOnSequenceResponse {
                    response: Some(response),
                }));
            }
        };

        // The stored order is the contract used by inventory-backed rack power.
        // Reject changes while a sequence is running so the accepted order does
        // not diverge from the active hardware operation.
        let Ok(_power_guard) = rack.try_power_operation_guard() else {
            tracing::warn!(rack = rack_id, "{}", RACK_POWER_BUSY_MESSAGE);
            response.message = RACK_POWER_BUSY_MESSAGE.to_owned();
            return Ok(tonic::Response::new(rm::SetRackPowerOnSequenceResponse {
                response: Some(response),
            }));
        };

        let steps: Vec<PowerOnStep> = power_on_order
            .into_iter()
            .enumerate()
            .map(|(i, item)| {
                let completion_check = item.completion_check;
                PowerOnStep {
                    order_index: i as i32,
                    node_id: item.node_id,
                    completion_check: completion_check.as_ref().is_some_and(|check| check.enabled),
                    timeout_seconds: completion_check
                        .as_ref()
                        .map_or(0, |check| check.timeout_seconds),
                }
            })
            .collect();

        let count = steps.len();

        if let Err(e) = rack.set_power_on_order(steps) {
            tracing::warn!(rack = %rack_id, error = %e.message, "power-on order rejected");
            response.message = e.message;

            return Ok(tonic::Response::new(rm::SetRackPowerOnSequenceResponse {
                response: Some(response),
            }));
        }

        tracing::info!(rack = %rack_id, count, "power-on order set");

        response.status = rm::ReturnCode::Success.into();
        response.message = format!("power-on order set with {count} steps");

        Ok(tonic::Response::new(rm::SetRackPowerOnSequenceResponse {
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

        if let Err(error) = self.initialize_nvue_client(node.nvue_client(), None).await {
            tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %error.message, "failed to configure NVUE client");
            resp.message = error.message;
            return Ok(tonic::Response::new(resp));
        }

        let device_info = match node.get_device_info().await {
            Ok(Some(v)) => v,
            Ok(None) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.message = "Device info is not available for this node type".into();
                return Ok(tonic::Response::new(resp));
            }
            Err(e) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %e.message, "get_device_info failed");
                resp.message = e.message;
                return Ok(tonic::Response::new(resp));
            }
        };

        let mut info = rm::NodeDeviceInfo {
            node_id: node.id().to_owned(),
            ..Default::default()
        };
        if let Err(msg) = populate_node_device_info(&device_info, &mut info) {
            resp.message = msg;
            return Ok(tonic::Response::new(resp));
        }

        resp.device_info = Some(info);
        resp.status = rm::ReturnCode::Success.into();
        resp.message = "Device info retrieved successfully".into();
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

        let target_type = match proto_node_type_to_domain(r.node_type) {
            Some(s) => s,
            None => {
                result.message = format!("invalid or unrecognized node_type: {}", r.node_type);
                return Ok(tonic::Response::new(result));
            }
        };
        let mut errors: Vec<String> = Vec::new();
        let mut total_nodes = 0u32;

        for node in rack.list_nodes() {
            if node.node_type() != target_type {
                continue;
            }
            total_nodes += 1;

            if let Err(error) = self.initialize_nvue_client(node.nvue_client(), None).await {
                errors.push(format!("node_id={}: {}", node.id(), error.message));
                continue;
            }

            let device_info = match node.get_device_info().await {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => {
                    errors.push(format!("node_id={}: {}", node.id(), e.message));
                    continue;
                }
            };

            let mut info = rm::NodeDeviceInfo {
                node_id: node.id().to_owned(),
                ..Default::default()
            };
            if let Err(msg) = populate_node_device_info(&device_info, &mut info) {
                errors.push(format!("node_id={}: {}", node.id(), msg));
                continue;
            }
            result.node_device_details.push(info);
        }

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

        let mut errors: Vec<String> = Vec::new();
        for node_info in nodes {
            let node_type_key = node_info.r#type.unwrap_or(0);
            let Some(node_type) = proto_node_type_to_domain(node_type_key) else {
                errors.push(format!(
                    "node_id={}: unknown or missing node type",
                    node_info.node_id
                ));
                continue;
            };

            let flat = match flatten_node_info(&node_info) {
                Ok(flat) => flat,
                Err(e) => {
                    push_node_error(&mut errors, &node_info.node_id, e.message);
                    continue;
                }
            };

            if flat.creds_for_node_type(node_type).is_none() {
                let source = if node_type.kind() == NodeKind::Switch {
                    "host"
                } else {
                    "BMC"
                };
                errors.push(format!(
                    "node_id={}: missing {source} credentials",
                    node_info.node_id
                ));
                continue;
            }

            // Switch device-info reads use host/NVUE. Keep BMC optional here so
            // host-only stateless switch reads work; compute and powershelf still
            // require BMC for Redfish.
            let is_switch = node_type.kind() == NodeKind::Switch;
            let bmc_endpoint = if is_switch {
                match flat.optional_bmc_endpoint() {
                    Ok(endpoint) => endpoint,
                    Err(e) => {
                        push_node_error(&mut errors, &node_info.node_id, e.message);
                        continue;
                    }
                }
            } else {
                match flat.bmc_endpoint().map(Some) {
                    Ok(endpoint) => endpoint,
                    Err(e) => {
                        push_node_error(&mut errors, &node_info.node_id, e.message);
                        continue;
                    }
                }
            };

            let host_endpoint = if is_switch {
                match flat.switch_host_management_endpoint().map(Some) {
                    Ok(endpoint) => endpoint,
                    Err(e) => {
                        push_node_error(&mut errors, &node_info.node_id, e.message);
                        continue;
                    }
                }
            } else {
                match flat.optional_host_endpoint() {
                    Ok(endpoint) => endpoint,
                    Err(e) => {
                        push_node_error(&mut errors, &node_info.node_id, e.message);
                        continue;
                    }
                }
            };

            let config = NodeConfig {
                id: node_info.node_id.clone(),
                node_type,
                bmc_endpoint,
                host_endpoint,
            };
            if matches!(
                node_type,
                NodeType::PowershelfGb200Delta | NodeType::PowershelfGb300Delta
            ) {
                errors.push(format!(
                    "node_id={}: build_ephemeral_node not supported for {} nodes",
                    node_info.node_id, node_type
                ));
                continue;
            }

            let node = match NodeInstance::create(&config, node_info.rack_id.as_str()) {
                Ok(node) => node,
                Err(e) => {
                    push_node_error(&mut errors, &node_info.node_id, e.message);
                    continue;
                }
            };

            if let Err(e) = self.initialize_nvue_client(node.nvue_client(), None).await {
                push_node_error(&mut errors, &node_info.node_id, e.message);
                continue;
            }

            let device_info = match node.get_device_info().await {
                Ok(device_info) => device_info,
                Err(e) => {
                    push_node_error(&mut errors, &node_info.node_id, e.message);
                    continue;
                }
            };
            let Some(device_info) = device_info else {
                // Node type doesn't expose device info; skip silently (matches
                // ListNodeDeviceInfoByNodeType behavior).
                continue;
            };

            let mut info = rm::NodeDeviceInfo {
                node_id: node_info.node_id.clone(),
                ..Default::default()
            };
            if let Err(msg) = populate_node_device_info(&device_info, &mut info) {
                errors.push(format!("node_id={}: {msg}", node_info.node_id));
                continue;
            }
            result.node_device_details.push(info);
        }

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

fn push_node_error(errors: &mut Vec<String>, node_id: &str, message: impl std::fmt::Display) {
    errors.push(format!("node_id={node_id}: {message}"));
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
    use std::sync::Arc;
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
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
            dangerously_accept_invalid_certs: false,
        };

        let node = rm::NodeInfo {
            node_id: "switch-1".into(),
            rack_id: "rack-1".into(),
            r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
            bmc_endpoint: Some(endpoint.clone()),
            host_endpoint: Some(endpoint),
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
}
