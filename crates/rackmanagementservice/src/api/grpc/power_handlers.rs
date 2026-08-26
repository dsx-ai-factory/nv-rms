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

use secrecy::ExposeSecret;

use crate::api::grpc::conversions::{flatten_node_info, power_state_to_string};
use crate::api::grpc::node_type_resolver::resolve_node_info;
use crate::domain::node::{Node, NodeKind, PowerOp, PowerTargetType};
use crate::domain::rack::{
    Endpoint, EndpointConfig, EndpointCredentials, NodeConfig, RACK_POWER_BUSY_MESSAGE,
};
use crate::nodes::NodeInstance;
use librms::protos::rack_manager as rm;

use super::server::{RackManagerServiceImpl, find_node, find_rack};

impl RackManagerServiceImpl {
    pub(crate) async fn handle_set_power_state(
        &self,
        req: tonic::Request<rm::SetPowerStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::SetPowerStateResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::SetPowerStateResponse {
            status: rm::ReturnCode::Failure.into(),
        };

        let op = rm::PowerOperation::try_from(r.operation)
            .ok()
            .and_then(|op| PowerOp::try_from(op).ok())
            .ok_or_else(|| {
                tonic::Status::invalid_argument(format!("invalid operation: {}", r.operation))
            })?;

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                return Ok(tonic::Response::new(resp));
            }
        };

        // Inventory-backed single-node power must not overlap another
        // registered power mutation in the same rack; otherwise callers can
        // both see success while the BMC applies whichever command arrives last.
        let Ok(_power_guard) = rack.try_power_operation_guard() else {
            tracing::warn!(
                rack = r.rack_id,
                node = r.node_id,
                "{}",
                RACK_POWER_BUSY_MESSAGE
            );
            return Ok(tonic::Response::new(resp));
        };

        let node = match find_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(_) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, "node not found");
                return Ok(tonic::Response::new(resp));
            }
        };

        if let Err(error) = self
            .initialize_power_nvue_client_if_needed(node.as_ref(), Some(op))
            .await
        {
            tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %error.message, "failed to configure NVUE client");
            return Ok(tonic::Response::new(resp));
        }

        match node.set_power_state(op, PowerTargetType::System).await {
            Ok(()) => {
                tracing::info!(node = %r.node_id, rack = %r.rack_id, ?op, "power state set");
                resp.status = rm::ReturnCode::Success.into();
            }
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e.message, "SetPowerState failed")
            }
        }
        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_batch_set_power_state(
        &self,
        req: tonic::Request<rm::BatchSetPowerStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchSetPowerStateResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::new(),
            job_id: String::new(),
            stats: None,
        };

        let nodes: Vec<rm::NodeInfo> = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        let total_nodes = nodes.len() as u32;
        let mut successful_nodes = 0;
        let mut failed_nodes = 0;
        if nodes.is_empty() {
            batch.message = "No nodes specified in request".into();
            return Ok(tonic::Response::new(batch_set_power_state_response(
                batch,
                total_nodes,
                successful_nodes,
                failed_nodes,
            )));
        }

        let op = rm::PowerOperation::try_from(r.operation)
            .ok()
            .and_then(|op| PowerOp::try_from(op).ok())
            .ok_or_else(|| {
                tonic::Status::invalid_argument(format!("invalid operation: {}", r.operation))
            })?;

        for node_info in nodes {
            let node_id = node_info.node_id.clone();

            //
            // NOTE:
            //
            // Batch power is the standalone trusted-admin direct endpoint path.
            // It intentionally does not look up inventory or join rack power
            // serialization; inventory-backed callers use SetPowerState or
            // SequenceRackPower instead.
            //

            let node = match build_ephemeral_power_node(&node_info) {
                Ok(node) => node,
                Err(e) => {
                    tracing::error!(
                        node = %node_id,
                        rack = %node_info.rack_id,
                        error = %e,
                        "failed to construct unregistered node"
                    );
                    failed_nodes += 1;
                    batch
                        .node_results
                        .push(node_power_result(node_id, rm::ReturnCode::Failure, e));
                    continue;
                }
            };

            match node.set_power_state(op, PowerTargetType::System).await {
                Ok(()) => {
                    tracing::info!(
                        node = %node_id,
                        rack = %node_info.rack_id,
                        node_type = %node.node_type(),
                        ?op,
                        "unregistered node power operation completed"
                    );
                    successful_nodes += 1;
                    batch.node_results.push(node_power_result(
                        node_id,
                        rm::ReturnCode::Success,
                        String::new(),
                    ));
                }
                Err(e) => {
                    let error_message = e.message;
                    tracing::error!(
                        node = %node_id,
                        rack = %node_info.rack_id,
                        node_type = %node.node_type(),
                        error = %error_message,
                        "unregistered node power operation failed"
                    );
                    failed_nodes += 1;
                    batch.node_results.push(node_power_result(
                        node_id,
                        rm::ReturnCode::Failure,
                        error_message,
                    ));
                }
            }
        }

        if failed_nodes == 0 {
            batch.status = rm::ReturnCode::Success.into();
            batch.message = format!(
                "Power operation completed for {}/{} nodes",
                successful_nodes, total_nodes
            );
        } else {
            batch.message = format!(
                "Power operation completed with {} successes and {} failures out of {} nodes",
                successful_nodes, failed_nodes, total_nodes
            );
        }

        Ok(tonic::Response::new(batch_set_power_state_response(
            batch,
            total_nodes,
            successful_nodes,
            failed_nodes,
        )))
    }

    pub(crate) async fn handle_get_power_state(
        &self,
        req: tonic::Request<rm::GetPowerStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::GetPowerStateResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::GetPowerStateResponse {
            status: rm::ReturnCode::Failure.into(),
            node_id: r.node_id.clone(),
            rack_id: r.rack_id.clone(),
            pstate: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(_) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, "node not found");
                return Ok(tonic::Response::new(resp));
            }
        };

        if let Err(error) = self
            .initialize_power_nvue_client_if_needed(node.as_ref(), None)
            .await
        {
            tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %error.message, "failed to configure NVUE client");
            return Ok(tonic::Response::new(resp));
        }

        match node.get_power_state().await {
            Ok(state) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.pstate = power_state_to_string(state).to_owned();
            }
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e.message, "GetPowerState failed")
            }
        }
        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_batch_get_power_state(
        &self,
        req: tonic::Request<rm::BatchGetPowerStateRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchGetPowerStateResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::new(),
            job_id: String::new(),
            stats: None,
        };
        let mut node_power_states: Vec<rm::NodePowerState> = Vec::new();

        let nodes: Vec<rm::NodeInfo> = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        let total_nodes = nodes.len() as u32;
        let mut successful_nodes = 0;
        let mut failed_nodes = 0;
        if nodes.is_empty() {
            batch.message = "No nodes specified in request".into();
            return Ok(tonic::Response::new(batch_get_power_state_response(
                batch,
                node_power_states,
                total_nodes,
                successful_nodes,
                failed_nodes,
            )));
        }

        for node_info in nodes {
            let node_id = node_info.node_id.clone();
            let node = match build_ephemeral_power_node(&node_info) {
                Ok(node) => node,
                Err(e) => {
                    tracing::error!(
                        node = %node_id,
                        rack = %node_info.rack_id,
                        error = %e,
                        "failed to construct unregistered node"
                    );
                    failed_nodes += 1;
                    batch
                        .node_results
                        .push(node_power_result(node_id, rm::ReturnCode::Failure, e));
                    continue;
                }
            };

            match node.get_power_state().await {
                Ok(state) => {
                    let pstate = power_state_to_string(state).to_owned();
                    tracing::info!(
                        node = %node_id,
                        rack = %node_info.rack_id,
                        node_type = %node.node_type(),
                        %pstate,
                        "unregistered node power state queried"
                    );
                    successful_nodes += 1;
                    batch.node_results.push(node_power_result(
                        node_id.clone(),
                        rm::ReturnCode::Success,
                        String::new(),
                    ));
                    node_power_states.push(rm::NodePowerState { node_id, pstate });
                }
                Err(e) => {
                    let error_message = e.message;
                    tracing::error!(
                        node = %node_id,
                        rack = %node_info.rack_id,
                        node_type = %node.node_type(),
                        error = %error_message,
                        "unregistered node power state query failed"
                    );
                    failed_nodes += 1;
                    batch.node_results.push(node_power_result(
                        node_id,
                        rm::ReturnCode::Failure,
                        error_message,
                    ));
                }
            }
        }

        if failed_nodes == 0 {
            batch.status = rm::ReturnCode::Success.into();
            batch.message = format!(
                "Power state queried for {}/{} nodes",
                successful_nodes, total_nodes
            );
        } else {
            batch.message = format!(
                "Power state query completed with {} successes and {} failures out of {} nodes",
                successful_nodes, failed_nodes, total_nodes
            );
        }

        Ok(tonic::Response::new(batch_get_power_state_response(
            batch,
            node_power_states,
            total_nodes,
            successful_nodes,
            failed_nodes,
        )))
    }

    pub(crate) async fn handle_sequence_rack_power(
        &self,
        req: tonic::Request<rm::SequenceRackPowerRequest>,
    ) -> std::result::Result<tonic::Response<rm::SequenceRackPowerResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::SequenceRackPowerResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };

        // Hold the rack power lock for the full inventory-backed sequence so
        // registered single-node power and inventory mutations cannot interleave
        // with the ordered Redfish calls. Standalone batch power is a separate
        // trusted-admin mode and is not mixed with inventory-backed operation.
        let Ok(_power_guard) = rack.try_power_operation_guard() else {
            tracing::warn!(rack = r.rack_id, "{}", RACK_POWER_BUSY_MESSAGE);
            resp.message = RACK_POWER_BUSY_MESSAGE.to_owned();
            return Ok(tonic::Response::new(resp));
        };

        let order = rack.get_power_on_order();
        if order.is_empty() {
            tracing::error!(rack = %r.rack_id, "power-on order not set");
            resp.message = "power-on order not set".into();
            return Ok(tonic::Response::new(resp));
        }

        let op = rm::RackPowerOperation::try_from(r.operation)
            .ok()
            .and_then(|op| PowerOp::try_from(op).ok())
            .ok_or_else(|| {
                tonic::Status::invalid_argument(format!("invalid operation: {}", r.operation))
            })?;

        // Execute power op on each node in the configured sequence order
        let mut all_ok = true;
        for step in &order {
            if let Some(node) = rack.find_node(&step.node_id) {
                if let Err(error) = self
                    .initialize_power_nvue_client_if_needed(node.as_ref(), Some(op))
                    .await
                {
                    tracing::error!(
                        node = %step.node_id,
                        rack = %r.rack_id,
                        error = %error.message,
                        "failed to configure NVUE client"
                    );
                    all_ok = false;
                    continue;
                }

                if let Err(e) = node.set_power_state(op, PowerTargetType::System).await {
                    tracing::error!(
                        node = %step.node_id,
                        rack = %r.rack_id,
                        error = %e.message,
                        "failed to set power state for node"
                    );
                    all_ok = false;
                }
            } else {
                tracing::error!(
                    node = %step.node_id,
                    rack = %r.rack_id,
                    "node not found in power sequence"
                );
                all_ok = false;
            }
        }

        if all_ok {
            tracing::info!(rack = %r.rack_id, ?op, "rack power operation completed");
            resp.status = rm::ReturnCode::Success.into();
            resp.message = "rack power operation completed".into();
        } else {
            resp.status = rm::ReturnCode::Failure.into();
            resp.message = "some nodes failed".into();
        }
        Ok(tonic::Response::new(resp))
    }

    async fn initialize_power_nvue_client_if_needed(
        &self,
        node: &NodeInstance,
        op: Option<PowerOp>,
    ) -> crate::utilities::error::Result<()> {
        let Some(client) = node.nvue_client() else {
            return Ok(());
        };

        if node.supports_bmc_aux_powercycle() {
            return Ok(());
        }

        if op.is_some_and(|op| op != PowerOp::PowerCycle) {
            return Ok(());
        }

        self.initialize_nvue_client(Some(client), None).await
    }
}

fn build_ephemeral_power_node(
    node_info: &rm::NodeInfo,
) -> std::result::Result<Arc<NodeInstance>, String> {
    let node_type = resolve_node_info(node_info).map_err(|error| error.to_string())?;

    let flat = flatten_node_info(node_info).map_err(|e| e.message)?;

    if node_type.kind() == NodeKind::Switch {
        // Switch power is a BMC/Redfish workflow. Even if the caller supplied a
        // host_endpoint for NVUE, non-PowerCycle operations need BMC details so
        // RMS does not send Redfish power commands to the host endpoint.
        let Some(bmc) = flat.bmc.as_ref() else {
            return Err(format!(
                "Missing BMC IP address for node {}",
                node_info.node_id
            ));
        };
        if bmc.ip_address.is_empty() {
            return Err(format!(
                "Missing BMC IP address for node {}",
                node_info.node_id
            ));
        }
        if bmc.username.is_empty() || bmc.password.expose_secret().is_empty() {
            return Err(format!(
                "Missing BMC credentials for node {}",
                node_info.node_id
            ));
        }
        let port = flat.resolve_bmc_port().map_err(|e| e.message)?;
        let config = NodeConfig {
            id: node_info.node_id.clone(),
            node_type,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: bmc.ip_address.clone(),
                    mac_address: bmc.mac_address.clone(),
                    port,
                    host_name: None,
                },
                Some(EndpointCredentials::new(
                    bmc.username.as_str(),
                    bmc.password.expose_secret(),
                )),
                true,
            )),
            host_endpoint: None,
            expected_inventory: None,
        };

        return NodeInstance::create(&config, &node_info.rack_id).map_err(|e| e.message);
    }

    if flat.creds_for_node_type(node_type).is_none() {
        return Err(format!(
            "Missing BMC credentials for node {}",
            node_info.node_id
        ));
    }

    let config = NodeConfig {
        id: node_info.node_id.clone(),
        node_type,
        bmc_endpoint: Some({
            let mut endpoint = flat.bmc_endpoint().map_err(|e| e.message)?;
            // BMC/Redfish endpoints use HTTPS with Basic auth in current
            // deployments, but RMS cannot validate BMC certificates. Keep this
            // hardcoded here rather than honoring a client-supplied endpoint field.
            endpoint.dangerously_accept_invalid_certs = true;
            endpoint
        }),
        host_endpoint: flat.optional_host_endpoint().map_err(|e| e.message)?,
        expected_inventory: None,
    };

    NodeInstance::create(&config, &node_info.rack_id).map_err(|e| e.message)
}

fn node_power_result(
    node_id: String,
    status: rm::ReturnCode,
    error_message: String,
) -> rm::NodeOperationResult {
    rm::NodeOperationResult {
        node_id,
        status: status.into(),
        error_message,
    }
}

fn batch_set_power_state_response(
    mut batch: rm::NodeBatchResponse,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) -> rm::BatchSetPowerStateResponse {
    batch.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });

    rm::BatchSetPowerStateResponse {
        response: Some(batch),
    }
}

fn batch_get_power_state_response(
    mut batch: rm::NodeBatchResponse,
    node_power_states: Vec<rm::NodePowerState>,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) -> rm::BatchGetPowerStateResponse {
    batch.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });

    rm::BatchGetPowerStateResponse {
        response: Some(batch),
        node_power_states,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};

    use super::super::server::SwitchTlsRoots;
    use crate::domain::node::{Node, NodeType, PowerState, ProductFamily};
    use crate::domain::rack::PowerOnStep;
    use crate::orchestrator::job_tracker::JobTracker;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use crate::racks::ManagedRack;
    use crate::utilities::error::Result;

    const RACK_ID: &str = "rack-01";
    const NODE_ID: &str = "c-01";

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    struct BlockingPowerState {
        entered: AsyncMutex<Option<oneshot::Sender<()>>>,
        release: Notify,
        calls: AtomicUsize,
    }

    struct BlockingPowerNode {
        state: Arc<BlockingPowerState>,
    }

    #[async_trait]
    impl Node for BlockingPowerNode {
        fn id(&self) -> &str {
            NODE_ID
        }

        fn rack_id(&self) -> &str {
            RACK_ID
        }

        fn node_type(&self) -> NodeType {
            NodeType::ComputeGb200Nvidia
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        async fn get_power_state(&self) -> Result<PowerState> {
            Ok(PowerState::On)
        }

        async fn set_power_state(&self, _op: PowerOp, _target: PowerTargetType) -> Result<()> {
            self.state.calls.fetch_add(1, Ordering::SeqCst);
            let entered = self.state.entered.lock().await.take();
            if let Some(entered) = entered {
                let _ = entered.send(());
            }

            self.state.release.notified().await;
            Ok(())
        }
    }

    fn test_service(rack_manager: Arc<RackManager>) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager,
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

    fn rack_manager_with_rack() -> TestResult<(Arc<RackManager>, Arc<ManagedRack>)> {
        let rack_manager = Arc::new(RackManager::new());
        rack_manager.create_rack(RACK_ID, ProductFamily::Gb200.rack_type())?;
        let Some(rack) = rack_manager.find_rack(RACK_ID) else {
            return Err("rack not found".into());
        };
        Ok((rack_manager, rack))
    }

    fn add_blocking_node(
        rack: &Arc<ManagedRack>,
    ) -> TestResult<(oneshot::Receiver<()>, Arc<BlockingPowerState>)> {
        let (entered_tx, entered) = oneshot::channel();
        let state = Arc::new(BlockingPowerState {
            entered: AsyncMutex::new(Some(entered_tx)),
            release: Notify::new(),
            calls: AtomicUsize::new(0),
        });
        let node = Arc::new(NodeInstance::from_test_node(BlockingPowerNode {
            state: state.clone(),
        }));

        rack.add_node(NODE_ID, node)?;
        Ok((entered, state))
    }

    fn set_power_request() -> tonic::Request<rm::SetPowerStateRequest> {
        tonic::Request::new(rm::SetPowerStateRequest {
            rack_id: RACK_ID.to_owned(),
            node_id: NODE_ID.to_owned(),
            operation: rm::PowerOperation::On as i32,
        })
    }

    fn sequence_power_request() -> tonic::Request<rm::SequenceRackPowerRequest> {
        tonic::Request::new(rm::SequenceRackPowerRequest {
            operation: rm::RackPowerOperation::On as i32,
            rack_id: RACK_ID.to_owned(),
        })
    }

    #[tokio::test]
    async fn set_power_state_rejects_concurrent_request_for_same_rack() -> TestResult {
        let (rack_manager, rack) = rack_manager_with_rack()?;
        let (entered, state) = add_blocking_node(&rack)?;
        let service = test_service(rack_manager);

        let first_service = service.clone();
        let first = tokio::spawn(async move {
            first_service
                .handle_set_power_state(set_power_request())
                .await
        });

        entered.await?;

        let second = service
            .handle_set_power_state(set_power_request())
            .await?
            .into_inner();

        assert_eq!(second.status, rm::ReturnCode::Failure as i32);
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);

        state.release.notify_waiters();
        let first = first.await??.into_inner();

        assert_eq!(first.status, rm::ReturnCode::Success as i32);
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn sequence_rack_power_rejects_while_set_power_state_in_progress() -> TestResult {
        let (rack_manager, rack) = rack_manager_with_rack()?;
        let (entered, state) = add_blocking_node(&rack)?;
        rack.set_power_on_order(vec![PowerOnStep::new(0, NODE_ID)])?;
        let service = test_service(rack_manager);

        let power_service = service.clone();
        let power = tokio::spawn(async move {
            power_service
                .handle_set_power_state(set_power_request())
                .await
        });

        entered.await?;

        let sequence = service
            .handle_sequence_rack_power(sequence_power_request())
            .await?
            .into_inner();

        assert_eq!(sequence.status, rm::ReturnCode::Failure as i32);
        assert_eq!(sequence.message, RACK_POWER_BUSY_MESSAGE);
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);

        state.release.notify_waiters();
        let power = power.await??.into_inner();

        assert_eq!(power.status, rm::ReturnCode::Success as i32);
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn sequence_rack_power_rejects_delete_while_sequence_in_progress() -> TestResult {
        let (rack_manager, rack) = rack_manager_with_rack()?;
        let (entered, state) = add_blocking_node(&rack)?;
        rack.set_power_on_order(vec![PowerOnStep::new(0, NODE_ID)])?;
        let service = test_service(rack_manager);

        let sequence_service = service.clone();
        let sequence = tokio::spawn(async move {
            sequence_service
                .handle_sequence_rack_power(sequence_power_request())
                .await
        });

        entered.await?;

        let delete = service
            .handle_delete_node(tonic::Request::new(rm::DeleteNodeRequest {
                node_id: NODE_ID.to_owned(),
                rack_id: RACK_ID.to_owned(),
            }))
            .await?
            .into_inner();

        let delete_response = delete.response.ok_or("missing delete response")?;

        assert_eq!(delete_response.status, rm::ReturnCode::Failure as i32);
        assert_eq!(delete_response.message, RACK_POWER_BUSY_MESSAGE);
        assert!(rack.find_node(NODE_ID).is_some());
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);

        state.release.notify_waiters();
        let sequence = sequence.await??.into_inner();

        assert_eq!(sequence.status, rm::ReturnCode::Success as i32);
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
