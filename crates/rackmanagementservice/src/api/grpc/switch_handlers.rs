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

use std::path::Path;
use std::sync::Arc;

use super::server::{RackManagerServiceImpl, find_rack};
use crate::api::grpc::conversions::{
    batch_targets, failed_node_results, proto_node_type_to_domain,
};
use crate::api::grpc::firmware_handlers::{build_ephemeral_node, resolve_firmware_file};
use crate::domain::node::{Node, NodeKind, NodeType};
use crate::nodes::switch_gb200_nvidia::FactoryResetError;
use crate::nodes::switch_gb200_nvidia::config::MIN_FIRMWARE_FILE_SIZE;
use crate::nodes::{NodeInstance, SwitchFirmwareManagement};
use crate::orchestrator::job_lifecycle::{JobError, JobFailure};
use crate::orchestrator::job_tracker::{JobType, RmsJobHandle};
use crate::racks::ManagedRack;
use crate::utilities::error::{ErrorCode, RmsError};

use librms::protos::rack_manager as rm;

fn find_switch_node(
    rack: &ManagedRack,
    node_id: &str,
) -> std::result::Result<Arc<NodeInstance>, String> {
    let node = rack
        .find_node(node_id)
        .ok_or_else(|| format!("node {node_id} not found"))?;
    if node.as_switch_firmware().is_none() {
        return Err(format!("node {node_id} is not a switch"));
    }
    Ok(node)
}

async fn initialized_switch_firmware<'a>(
    service: &RackManagerServiceImpl,
    node: &'a NodeInstance,
    node_id: &str,
) -> std::result::Result<&'a dyn SwitchFirmwareManagement, String> {
    service
        .initialize_nvue_client(node.nvue_client(), None)
        .await
        .map_err(|error| error.message)?;

    node.as_switch_firmware()
        .ok_or_else(|| format!("node {node_id} is not a switch"))
}

fn switch_fw_component_to_string(component_type: i32) -> String {
    match rm::SwitchFirmwareComponentType::try_from(component_type) {
        Ok(rm::SwitchFirmwareComponentType::Bmc) => "BMC".into(),
        Ok(rm::SwitchFirmwareComponentType::Fpga) => "FPGA".into(),
        Ok(rm::SwitchFirmwareComponentType::Erot) => "EROT".into(),
        Ok(rm::SwitchFirmwareComponentType::Cpld) => "CPLD".into(),
        Ok(rm::SwitchFirmwareComponentType::Bios) => "BIOS".into(),
        Ok(rm::SwitchFirmwareComponentType::Transceiver) => "TRANSCEIVER".into(),
        _ => "UNKNOWN".into(),
    }
}

fn switch_fw_inventory_component_to_string(component_type: i32) -> String {
    match rm::SwitchFirmwareComponentType::try_from(component_type) {
        Ok(rm::SwitchFirmwareComponentType::Cpld) => "CPLD1".into(),
        _ => switch_fw_component_to_string(component_type),
    }
}

fn build_factory_reset_target(
    device: &rm::NodeInfo,
    insecure_bmc: bool,
) -> std::result::Result<Arc<NodeInstance>, String> {
    if device.node_id.is_empty() || device.rack_id.is_empty() {
        return Err("device node_id and rack_id are required".into());
    }

    let node_type = proto_node_type_to_domain(device.r#type.unwrap_or_default())
        .filter(|node_type| node_type.kind() == NodeKind::Switch)
        .ok_or_else(|| "device node type must be a switch".to_owned())?;

    #[cfg(test)]
    if let Ok(node) = tests::FACTORY_RESET_TARGET.try_with(Arc::clone) {
        return Ok(node);
    }

    build_ephemeral_node(device, node_type, None, None, insecure_bmc).map_err(|error| error.message)
}

fn reserve_factory_reset_jobs(
    service: &RackManagerServiceImpl,
    parent_id: &str,
    devices: Vec<rm::NodeInfo>,
    response: &mut rm::NodeBatchResponse,
) -> Vec<(RmsJobHandle, Arc<NodeInstance>)> {
    let insecure_bmc = service.insecure_bmc;
    let (reserved, rejected) = service.job_tracker.create_batch_jobs(
        parent_id,
        JobType::SwitchFactoryDefaultReset,
        batch_targets(devices),
        |device| build_factory_reset_target(device, insecure_bmc),
    );

    response.node_results.extend(failed_node_results(rejected));

    // This response reports admitted nodes through `node_results`, not a
    // per-node job list.
    reserved
        .into_iter()
        .map(|(device, pending, node)| {
            response.node_results.push(rm::NodeOperationResult {
                node_id: device.node_id,
                status: rm::ReturnCode::Success.into(),
                error_message: String::new(),
            });

            (pending, node)
        })
        .collect()
}

/// Prepare verified NVUE access before sending the destructive action.
async fn submit_factory_reset(
    service: &RackManagerServiceImpl,
    node: &NodeInstance,
    client: &nvue_client::SharedClient,
    domain: Option<&str>,
) -> std::result::Result<(), FactoryResetError> {
    let prepared = if service.switch_tls_roots.insecure_switch {
        client
            .configure_insecure_https()
            .await
            .map_err(|error| FactoryResetError::Connection(error.into()))?;

        None
    } else {
        let endpoint = client.endpoint();

        let server_name = service
            .switch_tls_roots
            .tls_server_name_for_endpoint(&endpoint.host, domain)
            .map_err(|error| FactoryResetError::Connection(RmsError::failed_precondition(error)))?;

        let tls = service
            .switch_tls_roots
            .resolve_required_nvue_client_tls(domain)
            .map_err(|error| FactoryResetError::Connection(RmsError::failed_precondition(error)))?;

        let prepared = client
            .prepare_verified_client_tls(tls, Some(&server_name))
            .await
            .map_err(|error| FactoryResetError::Connection(error.into()))?;

        Some(prepared)
    };

    node.require_nvidia_switch()
        .map_err(FactoryResetError::Rejected)?
        .submit_factory_default_reset(client, prepared.as_ref())
        .await
}

/// Keep the configured/default NVUE credential retry separate from the
/// operator-controlled SSH connection fallback. Uncertain NVUE submissions
/// never enter the SSH submission path.
async fn submit_factory_reset_with_fallback(
    service: &RackManagerServiceImpl,
    node: &NodeInstance,
    domain: Option<&str>,
    allow_ssh_fallback: bool,
) -> std::result::Result<(), FactoryResetError> {
    let configured = node.nvue_client().cloned().ok_or_else(|| {
        FactoryResetError::Rejected(RmsError::invalid_argument(
            "switch host endpoint is required",
        ))
    })?;

    let switch = node
        .require_nvidia_switch()
        .map_err(FactoryResetError::Rejected)?;

    let nvue_submission = match submit_factory_reset(service, node, &configured, domain).await {
        Err(FactoryResetError::Rejected(error))
            if error.code == ErrorCode::Unauthenticated
                && !configured.credentials_match("admin", "admin") =>
        {
            let default = switch
                .default_nvue_client()
                .map_err(FactoryResetError::Rejected)?;

            submit_factory_reset(service, node, &default, domain).await
        }
        outcome => outcome,
    };

    let submission = match nvue_submission {
        Err(FactoryResetError::Connection(error)) if allow_ssh_fallback => {
            tracing::warn!(node = %node.id(), error = %error,
                "NVUE reset connection failed before submission; attempting authorized SSH fallback");

            switch.submit_factory_default_reset_ssh().await
                .map_err(|ssh_error| FactoryResetError::Rejected(RmsError::new(
                    ssh_error.code,
                    format!("SSH factory-reset submission failed: {ssh_error}; NVUE connection failed: {error}"),
                )))
        }
        Err(FactoryResetError::Connection(mut error)) => {
            error
                .message
                .push_str("; SSH reset submission is disabled (allow_ssh_fallback=false)");

            Err(FactoryResetError::Connection(error))
        }
        outcome => outcome,
    };

    // Either transport can reject stale configured credentials after an
    // earlier reset. Probe the default login without submitting another reset.
    match submission {
        Err(FactoryResetError::Rejected(error)) if error.code == ErrorCode::Unauthenticated => {
            if switch
                .probe_factory_default_login()
                .await
                .map_err(FactoryResetError::Rejected)?
            {
                Ok(())
            } else {
                Err(FactoryResetError::Rejected(error))
            }
        }
        outcome => outcome,
    }
}

async fn run_factory_reset_job(
    job: RmsJobHandle,
    service: &RackManagerServiceImpl,
    node: &NodeInstance,
    domain: Option<&str>,
    allow_ssh_fallback: bool,
) {
    job.progress("Resetting switch to factory defaults");

    // Recovery may start with rejected credentials, so reserve configured
    // endpoints without requiring a physical-identity read.
    let admission =
        crate::api::grpc::switch_scope::configured_switch_scopes(node, node.nvue_client())
            .map_err(|error| JobFailure::new(JobError::Internal, error.message))
            .and_then(|scopes| service.admit_known_switch_job_scopes(&job, scopes));

    if let Err(error) = admission {
        job.fail(error);
        return;
    }

    let submission =
        submit_factory_reset_with_fallback(service, node, domain, allow_ssh_fallback).await;

    let uncertainty = match submission {
        Ok(()) => None,
        Err(FactoryResetError::Ambiguous(error)) => {
            job.progress(
                "NVUE reset outcome is uncertain; checking completion without resubmitting",
            );

            Some(error)
        }
        Err(FactoryResetError::Connection(error) | FactoryResetError::Rejected(error)) => {
            job.fail(JobFailure::new(JobError::Other, error.message));
            return;
        }
    };

    let result = async {
        node.require_nvidia_switch()?
            .wait_for_factory_default()
            .await
    }
    .await;

    match result {
        Ok(()) => job.complete_verified("Switch factory reset completed", ""),
        Err(mut error) => {
            if let Some(submission_error) = uncertainty {
                error.message = format!(
                    "NVUE reset may have been accepted: {submission_error}; completion was not confirmed: {error}; no additional reset was submitted"
                );
            }

            job.fail(JobFailure::new(JobError::Other, error.message));
        }
    }
}

fn factory_reset_failure(
    mut response: rm::NodeBatchResponse,
    total_nodes: u32,
    message: &str,
) -> tonic::Response<rm::BatchResetSwitchFactoryDefaultResponse> {
    response.status = rm::ReturnCode::Failure.into();
    response.message = message.to_owned();

    response.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes: 0,
        failed_nodes: total_nodes,
    });

    tonic::Response::new(rm::BatchResetSwitchFactoryDefaultResponse {
        response: Some(response),
    })
}

impl RackManagerServiceImpl {
    /// Admits one factory-reset child job per valid switch under a parent job.
    ///
    /// Per-node success in the RPC response means the child job was admitted,
    /// not that the reset completed. Callers poll the parent job through
    /// `GetJobStatus`; a child completes only after factory-default SSH
    /// authentication succeeds.
    pub(crate) async fn handle_batch_reset_switch_factory_default(
        &self,
        req: tonic::Request<rm::BatchResetSwitchFactoryDefaultRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::BatchResetSwitchFactoryDefaultResponse>,
        tonic::Status,
    > {
        let request = req.into_inner();
        let devices = request.nodes.map(|nodes| nodes.nodes).unwrap_or_default();
        let total_nodes = devices.len() as u32;
        let mut response = rm::NodeBatchResponse::default();

        if devices.is_empty() {
            return Ok(factory_reset_failure(
                response,
                total_nodes,
                "nodes is required",
            ));
        }

        let domain = request.domain;
        let allow_ssh_fallback = request.allow_ssh_fallback;

        let parent_rack_id = devices
            .first()
            .map(|device| device.rack_id.clone())
            .unwrap_or_default();

        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(&parent_rack_id, JobType::SwitchFactoryDefaultReset)
        else {
            let message = "failed to create switch factory-reset job";

            response
                .node_results
                .extend(devices.into_iter().map(|device| rm::NodeOperationResult {
                    node_id: device.node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message: message.to_owned(),
                }));

            return Ok(factory_reset_failure(response, total_nodes, message));
        };

        response.job_id = parent_id.clone();

        // Reserve every node before spawning. This keeps duplicate targets and
        // concurrent destructive operations excluded for the complete batch.
        let queued_jobs = reserve_factory_reset_jobs(self, &parent_id, devices, &mut response);

        if queued_jobs.is_empty() {
            response.message = "no switch factory-reset jobs created".into();
            self.job_tracker
                .mark_failed_message(&parent_id, &response.message);
        } else {
            for (pending, node) in queued_jobs {
                let service = self.clone();
                let domain = domain.clone();

                self.job_tracker
                    .spawn_job(pending, move |job| async move {
                        run_factory_reset_job(
                            job,
                            &service,
                            &node,
                            domain.as_deref(),
                            allow_ssh_fallback,
                        )
                        .await;
                    })
                    .detach();
            }
        }

        let successful_nodes = response
            .node_results
            .iter()
            .filter(|result| result.status == rm::ReturnCode::Success as i32)
            .count() as u32;

        let failed_nodes = total_nodes - successful_nodes;

        response.status = if failed_nodes == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();

        if successful_nodes != 0 {
            response.message = format!("Created {successful_nodes} switch factory-reset jobs");
        }

        response.stats = Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes,
            failed_nodes,
        });

        Ok(tonic::Response::new(
            rm::BatchResetSwitchFactoryDefaultResponse {
                response: Some(response),
            },
        ))
    }

    pub(crate) async fn handle_list_switch_firmware(
        &self,
        req: tonic::Request<rm::ListSwitchFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListSwitchFirmwareResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::ListSwitchFirmwareResponse {
            status: rm::ReturnCode::Failure.into(),
            result_json: String::new(),
            error_message: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.error_message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_switch_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let switch = if node.node_type() == NodeType::SwitchVrnvl72Nvidia {
            node.as_switch_firmware()
                .ok_or_else(|| format!("node {} is not a switch", r.node_id))
        } else {
            initialized_switch_firmware(self, node.as_ref(), &r.node_id).await
        };
        let switch = match switch {
            Ok(s) => s,
            Err(error) => {
                resp.error_message = error;
                return Ok(tonic::Response::new(resp));
            }
        };

        if !is_valid_component_type(r.component_type) {
            tracing::error!(rack = %r.rack_id, node = %r.node_id, "invalid component type: UNKNOWN is not allowed");
            resp.error_message = "invalid component type: UNKNOWN is not allowed".into();
            return Ok(tonic::Response::new(resp));
        }

        let component = switch_fw_inventory_component_to_string(r.component_type);

        match switch.list_firmware(&component, false).await {
            Ok(json) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.result_json = json.to_string();
            }
            Err(e) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %e.message, "list_firmware failed");
                resp.error_message = e.message;
            }
        }

        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_push_switch_firmware(
        &self,
        req: tonic::Request<rm::PushSwitchFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::PushSwitchFirmwareResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::PushSwitchFirmwareResponse {
            status: rm::ReturnCode::Failure.into(),
            result_json: String::new(),
            error_message: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.error_message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_switch_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let switch = match initialized_switch_firmware(self, node.as_ref(), &r.node_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };

        if !is_valid_component_type(r.component_type) {
            tracing::error!(rack = %r.rack_id, node = %r.node_id, "invalid component type: UNKNOWN is not allowed");
            resp.error_message = "invalid component type: UNKNOWN is not allowed".into();
            return Ok(tonic::Response::new(resp));
        }
        if !is_valid_filename(&r.filename) {
            tracing::error!(rack = %r.rack_id, node = %r.node_id, filename = %r.filename, "invalid firmware filename");
            resp.error_message = "invalid firmware filename".into();
            return Ok(tonic::Response::new(resp));
        }
        let local_file_path = match resolve_firmware_file(&r.local_file_path, &self.firmware_dir) {
            Ok(path) => path,
            Err(e) => {
                tracing::error!(
                    rack = %r.rack_id,
                    node = %r.node_id,
                    path = %r.local_file_path,
                    error = %e,
                    "invalid firmware file path"
                );
                resp.error_message = format!("invalid firmware file path: {e}");
                return Ok(tonic::Response::new(resp));
            }
        };

        if !file_exists_and_min_size(&local_file_path, MIN_FIRMWARE_FILE_SIZE) {
            tracing::error!(
                rack = %r.rack_id,
                node = %r.node_id,
                path = %local_file_path.display(),
                "firmware file not found or too small"
            );
            resp.error_message = format!(
                "firmware file not found or too small: {}",
                local_file_path.display()
            );
            return Ok(tonic::Response::new(resp));
        }
        let local_file_path = local_file_path.to_string_lossy().into_owned();

        let component = switch_fw_component_to_string(r.component_type);
        match switch
            .push_firmware_file(&local_file_path, &component, &r.filename)
            .await
        {
            Ok(()) => {
                resp.status = rm::ReturnCode::Success.into();
            }
            Err(e) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %e.message, "push_firmware_file failed");
                resp.error_message = e.message;
            }
        }
        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_list_switch_system_images(
        &self,
        req: tonic::Request<rm::ListSwitchSystemImagesRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListSwitchSystemImagesResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut resp = rm::ListSwitchSystemImagesResponse {
            status: rm::ReturnCode::Failure.into(),
            images_json: String::new(),
            error_message: String::new(),
        };
        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.error_message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_switch_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let switch = match initialized_switch_firmware(self, node.as_ref(), &r.node_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };

        match switch.list_system_images().await {
            Ok(json) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.images_json = json.to_string();
            }
            Err(e) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %e.message, "list_system_images failed");
                resp.error_message = e.message;
            }
        }

        Ok(tonic::Response::new(resp))
    }
}

// ── Validation helpers ──

fn is_valid_component_type(component_type: i32) -> bool {
    let Ok(component_type) = rm::SwitchFirmwareComponentType::try_from(component_type) else {
        return false;
    };

    !matches!(
        component_type,
        rm::SwitchFirmwareComponentType::Unspecified | rm::SwitchFirmwareComponentType::Unknown
    )
}

fn is_valid_filename(name: &str) -> bool {
    if name.is_empty() || name.starts_with('.') {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

fn file_exists_and_min_size(path: &Path, min_size: u64) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.len() >= min_size)
        .unwrap_or(false)
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
    use crate::api::grpc::switch_scope::{configured_switch_scopes, fabric_scope_keys};
    use crate::orchestrator::fabric_operation::{ControllerIdentity, FabricOperationIntent};
    use crate::orchestrator::job_lifecycle::{JobScope, JobState};
    use crate::orchestrator::job_tracker::JobTracker;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use crate::transport::ssh::SshUploadOptions;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    tokio::task_local! {
        pub(super) static FACTORY_RESET_TARGET: Arc<NodeInstance>;
    }

    /// Serves a local HTTP mock through the HTTPS transport used by switch workers.
    pub(crate) struct WiremockHttpsProxy {
        /// Local port accepting TLS connections.
        pub(crate) port: u16,

        certificate_pem: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl WiremockHttpsProxy {
        /// Starts a TLS listener forwarding requests to the supplied mock server.
        pub(crate) async fn start(upstream: SocketAddr) -> Self {
            use rcgen::generate_simple_self_signed;
            use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
            use tokio::io::copy_bidirectional;
            use tokio::net::{TcpListener, TcpStream};
            use tokio_rustls::TlsAcceptor;

            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

            let cert = generate_simple_self_signed(vec![
                "switch.example.com".into(),
                "localhost".into(),
                "127.0.0.1".into(),
            ])
            .expect("generate NVUE test certificate");

            let certificate_pem = cert.cert.pem();
            let cert_der = CertificateDer::from(cert.cert.der().to_vec());
            let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

            let mut tls_config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert_der], key_der.into())
                .expect("build NVUE test TLS config");

            tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];

            let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind NVUE HTTPS proxy");

            let port = listener.local_addr().expect("read proxy address").port();

            let task = tokio::spawn(async move {
                while let Ok((downstream, _)) = listener.accept().await {
                    let tls_acceptor = tls_acceptor.clone();

                    tokio::spawn(async move {
                        let Ok(mut downstream) = tls_acceptor.accept(downstream).await else {
                            return;
                        };

                        let Ok(mut upstream) = TcpStream::connect(upstream).await else {
                            return;
                        };

                        copy_bidirectional(&mut downstream, &mut upstream)
                            .await
                            .ok();
                    });
                }
            });

            Self {
                port,
                certificate_pem,
                task,
            }
        }
    }

    impl Drop for WiremockHttpsProxy {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn test_service(job_tracker: Arc<JobTracker>) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker,
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            artifact_require_sha256: false,
            insecure_bmc: true,
            artifact_download_policy:
                crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
            switch_tls_roots: SwitchTlsRoots {
                insecure_switch: true,
                ..SwitchTlsRoots::default()
            },
            sftp_upload_options: SshUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
            workflow_timings: crate::config::WorkflowTimings::default(),
            recovery_profiles: crate::config::RecoveryProfiles::default(),
            fabric_layout_catalog: std::sync::Arc::new(
                crate::config::fabric_layout::FabricLayoutCatalog::default(),
            ),
            hardware_profiles: Arc::new(
                crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
            ),
            fabric_operations: std::sync::Arc::new(Default::default()),
        }
    }

    fn factory_reset_node_with_port(rack_id: &str, node_id: &str, port: u32) -> rm::NodeInfo {
        rm::NodeInfo {
            rack_id: rack_id.to_owned(),
            node_id: node_id.to_owned(),
            r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
            host_endpoint: Some(rm::Endpoint {
                interface: Some(rm::NetworkInterface {
                    ip_address: "127.0.0.1".to_owned(),
                    mac_address: String::new(),
                    host_name: Some("switch.example.com".to_owned()),
                }),
                port,
                credentials: Some(rm::Credentials {
                    auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                        username: "admin".to_owned(),
                        password: "current-password".to_owned(),
                    })),
                }),
            }),
            ..Default::default()
        }
    }

    fn factory_reset_node(rack_id: &str, node_id: &str) -> rm::NodeInfo {
        factory_reset_node_with_port(rack_id, node_id, 443)
    }

    /// Builds a configured switch using the local NVUE HTTPS proxy.
    pub(crate) async fn factory_reset_test_target(
        server: &MockServer,
    ) -> (WiremockHttpsProxy, NodeInstance) {
        let proxy = WiremockHttpsProxy::start(*server.address()).await;
        let node = build_factory_reset_target(
            &factory_reset_node_with_port("rack-01", "switch-01", proxy.port.into()),
            true,
        )
        .unwrap();
        let node = Arc::into_inner(node).expect("factory reset target has one owner");

        (proxy, node)
    }

    // Real TLS material lets secure-mode tests reach the activation probe,
    // rather than stopping at missing-file validation or using insecure HTTPS.
    async fn factory_reset_secure_test_target(
        server: &MockServer,
        wrong_ca: bool,
    ) -> (
        WiremockHttpsProxy,
        NodeInstance,
        tempfile::TempDir,
        RackManagerServiceImpl,
    ) {
        let proxy = WiremockHttpsProxy::start(*server.address()).await;

        let node = build_factory_reset_target(
            &factory_reset_node_with_port("rack-01", "switch-01", proxy.port.into()),
            false,
        )
        .unwrap();

        let node = Arc::into_inner(node).expect("factory reset target has one owner");
        let root = tempfile::tempdir().unwrap();
        let domain = root.path().join("fabric-a");

        tokio::fs::create_dir(&domain).await.unwrap();

        let client = rcgen::generate_simple_self_signed(vec!["rms-client".to_owned()]).unwrap();

        let ca = if wrong_ca {
            client.cert.pem()
        } else {
            proxy.certificate_pem.clone()
        };

        for (name, material) in [
            ("ca.pem", ca),
            ("client.pem", client.cert.pem()),
            ("client.key", client.key_pair.serialize_pem()),
        ] {
            tokio::fs::write(domain.join(name), material).await.unwrap();
        }

        let mut service = test_service(Arc::new(JobTracker::new()));

        service.switch_tls_roots = SwitchTlsRoots {
            insecure_switch: false,
            client_tls: Some(crate::libnmxc::TlsMaterialStore::new(root.path())),
            default_domain: Some("fabric-a".to_owned()),
            ..SwitchTlsRoots::default()
        };

        (proxy, node, root, service)
    }

    #[tokio::test]
    async fn factory_reset_verified_tls_probe_precedes_submission_and_rejects_http_errors() {
        for probe_status in [200, 403] {
            let server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/system"))
                .respond_with(
                    ResponseTemplate::new(probe_status).set_body_json(serde_json::json!({})),
                )
                .expect(1)
                .mount(&server)
                .await;

            Mock::given(method("POST"))
                .and(path(nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT))
                .respond_with(ResponseTemplate::new(201).set_body_json("reset-1"))
                .expect(if probe_status == 200 { 1 } else { 0 })
                .mount(&server)
                .await;

            let (_proxy, mut node, _root, service) =
                factory_reset_secure_test_target(&server, false).await;

            let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
                panic!("expected NVIDIA switch");
            };

            switch.ssh_exec_for_test = Some(Arc::new(|_| {
                panic!("verified NVUE success and HTTP rejection must not submit over SSH")
            }));

            let result = submit_factory_reset_with_fallback(&service, &node, None, true).await;

            if probe_status == 200 {
                assert!(result.is_ok(), "{result:?}");
            } else {
                assert!(
                    matches!(result, Err(FactoryResetError::Rejected(_))),
                    "{result:?}"
                );
            }

            let requests = server.received_requests().await.unwrap();

            assert_eq!(requests[0].method.to_string(), "GET");
            assert_eq!(requests[0].url.path(), "/nvue_v1/system");

            if probe_status == 200 {
                assert_eq!(requests[1].method.to_string(), "POST");

                assert_eq!(
                    requests[1].url.path(),
                    nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT
                );
            }
        }
    }

    #[tokio::test]
    async fn factory_reset_wrong_ca_probe_requires_opt_in_and_never_submits_an_nvue_reset() {
        for allow_ssh_fallback in [false, true] {
            let server = MockServer::start().await;

            let (_proxy, mut node, _root, service) =
                factory_reset_secure_test_target(&server, true).await;

            let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
                panic!("expected NVIDIA switch");
            };

            let commands = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&commands);

            switch.ssh_exec_for_test = Some(Arc::new(move |command| {
                recorded.lock().unwrap().push(command.to_owned());
                Ok(String::new())
            }));

            let result =
                submit_factory_reset_with_fallback(&service, &node, None, allow_ssh_fallback).await;

            if allow_ssh_fallback {
                assert!(result.is_ok(), "{result:?}");

                assert_eq!(
                    *commands.lock().unwrap(),
                    ["nv action reset system factory-default force"]
                );
            } else {
                assert!(
                    matches!(result, Err(FactoryResetError::Connection(_))),
                    "{result:?}"
                );

                assert!(commands.lock().unwrap().is_empty());
            }

            assert!(server.received_requests().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn factory_reset_ssh_authentication_failure_checks_already_reset_without_resubmitting() {
        for (ssh_code, already_reset) in [
            (ErrorCode::Unauthenticated, true),
            (ErrorCode::Unauthenticated, false),
            (ErrorCode::Internal, true),
        ] {
            let server = MockServer::start().await;
            let (proxy, mut node) = factory_reset_test_target(&server).await;

            drop(proxy);

            let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
                panic!("expected NVIDIA switch");
            };

            let submissions = Arc::new(Mutex::new(0));
            let recorded = Arc::clone(&submissions);

            switch.ssh_exec_for_test = Some(Arc::new(move |_| {
                *recorded.lock().unwrap() += 1;
                Err(RmsError::new(
                    ssh_code,
                    "configured SSH credentials rejected",
                ))
            }));

            let probes = Arc::new(Mutex::new(0));
            let recorded = Arc::clone(&probes);

            switch.factory_default_probe_for_test = Some(Arc::new(move || {
                *recorded.lock().unwrap() += 1;
                Ok(already_reset)
            }));

            let service = test_service(Arc::new(JobTracker::new()));

            let result = submit_factory_reset_with_fallback(&service, &node, None, true).await;

            assert_eq!(
                result.is_ok(),
                ssh_code == ErrorCode::Unauthenticated && already_reset,
                "{result:?}"
            );

            assert_eq!(*submissions.lock().unwrap(), 1);

            assert_eq!(
                *probes.lock().unwrap(),
                usize::from(ssh_code == ErrorCode::Unauthenticated)
            );

            if let Err(FactoryResetError::Rejected(error)) = result {
                assert_eq!(error.code, ssh_code);

                assert!(
                    error
                        .message
                        .contains("SSH factory-reset submission failed")
                );
            }

            assert!(server.received_requests().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn factory_reset_nvue_success_never_submits_over_ssh() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT))
            .respond_with(ResponseTemplate::new(201).set_body_json("reset-1"))
            .expect(1)
            .mount(&server)
            .await;

        let (_proxy, mut node) = factory_reset_test_target(&server).await;

        let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
            panic!("expected NVIDIA switch");
        };

        switch.ssh_exec_for_test = Some(Arc::new(|_| {
            panic!("NVUE success must not submit over SSH")
        }));

        let service = test_service(Arc::new(JobTracker::new()));

        submit_factory_reset_with_fallback(&service, &node, None, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn factory_reset_connection_failure_requires_ssh_opt_in() {
        for allow_ssh_fallback in [false, true] {
            let server = MockServer::start().await;
            let (proxy, mut node) = factory_reset_test_target(&server).await;

            drop(proxy);

            let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
                panic!("expected NVIDIA switch");
            };

            let commands = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&commands);

            switch.ssh_exec_for_test = Some(Arc::new(move |command| {
                recorded.lock().unwrap().push(command.to_owned());
                Ok(String::new())
            }));

            let service = test_service(Arc::new(JobTracker::new()));

            let result =
                submit_factory_reset_with_fallback(&service, &node, None, allow_ssh_fallback).await;

            assert_eq!(result.is_ok(), allow_ssh_fallback, "{result:?}");

            assert_eq!(
                commands.lock().unwrap().len(),
                usize::from(allow_ssh_fallback)
            );

            if allow_ssh_fallback {
                assert_eq!(
                    commands.lock().unwrap()[0],
                    "nv action reset system factory-default force"
                );
            }

            assert!(server.received_requests().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn factory_reset_missing_or_unusable_tls_does_not_block_authorized_ssh() {
        for (invalid_material, allow_ssh_fallback) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let server = MockServer::start().await;
            let (_proxy, mut node) = factory_reset_test_target(&server).await;
            let root = tempfile::tempdir().unwrap();
            let mut service = test_service(Arc::new(JobTracker::new()));
            service.switch_tls_roots.insecure_switch = false;

            if invalid_material {
                let domain = root.path().join("fabric-a");
                tokio::fs::create_dir(&domain).await.unwrap();

                for name in ["ca.pem", "client.pem", "client.key"] {
                    tokio::fs::write(domain.join(name), "invalid PEM")
                        .await
                        .unwrap();
                }

                service.switch_tls_roots.client_tls =
                    Some(crate::libnmxc::TlsMaterialStore::new(root.path()));

                service.switch_tls_roots.default_domain = Some("fabric-a".to_owned());
            }

            let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
                panic!("expected NVIDIA switch");
            };

            let commands = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&commands);

            switch.ssh_exec_for_test = Some(Arc::new(move |command| {
                recorded.lock().unwrap().push(command.to_owned());
                Ok(String::new())
            }));

            let result =
                submit_factory_reset_with_fallback(&service, &node, None, allow_ssh_fallback).await;

            assert_eq!(result.is_ok(), allow_ssh_fallback, "{result:?}");

            assert_eq!(
                commands.lock().unwrap().len(),
                usize::from(allow_ssh_fallback)
            );

            assert!(server.received_requests().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn factory_reset_ssh_submission_failure_fails_the_job() {
        let server = MockServer::start().await;
        let (_proxy, mut node) = factory_reset_test_target(&server).await;

        let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
            panic!("expected NVIDIA switch");
        };

        switch.ssh_exec_for_test = Some(Arc::new(|_| Err(RmsError::internal("permission denied"))));

        switch.factory_default_probe_for_test = Some(Arc::new(|| {
            panic!("failed SSH submission must not start polling")
        }));

        let tracker = Arc::new(JobTracker::new());

        let job = tracker
            .create_job_if_node_idle("rack-01", "switch-01", JobType::SwitchFactoryDefaultReset)
            .unwrap();

        let job_id = job.id().to_string();
        let mut service = test_service(Arc::clone(&tracker));
        service.switch_tls_roots.insecure_switch = false;

        run_factory_reset_job(job, &service, &node, None, true).await;

        let status = tracker.get_job(&job_id).unwrap();

        assert_eq!(status.state, JobState::Failed);

        assert!(
            status
                .error_message
                .contains("SSH factory-reset submission failed")
        );

        assert!(status.error_message.contains("permission denied"));
    }

    #[tokio::test]
    async fn factory_reset_ambiguous_nvue_submission_checks_completion_without_duplicate_reset() {
        for (allow_ssh_fallback, ready) in [(false, true), (true, true), (true, false)] {
            let server = MockServer::start().await;

            // The reset reached NVUE, but its successful response is unusable.
            Mock::given(method("POST"))
                .and(path(nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT))
                .respond_with(ResponseTemplate::new(201).set_body_string("truncated response"))
                .expect(1)
                .mount(&server)
                .await;

            let (_proxy, mut node) = factory_reset_test_target(&server).await;

            let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
                panic!("expected NVIDIA switch");
            };

            switch.ssh_exec_for_test = Some(Arc::new(|_| {
                panic!("uncertain NVUE submission must not submit over SSH")
            }));

            let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let recorded = Arc::clone(&probes);

            switch.factory_default_probe_for_test = Some(Arc::new(move || {
                recorded.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                if ready {
                    Ok(true)
                } else {
                    Err(RmsError::timeout("reset completion was not observed"))
                }
            }));

            let tracker = Arc::new(JobTracker::new());

            let job = tracker
                .create_job_if_node_idle("rack-01", "switch-01", JobType::SwitchFactoryDefaultReset)
                .unwrap();

            let job_id = job.id().to_string();
            let service = test_service(Arc::clone(&tracker));

            run_factory_reset_job(job, &service, &node, None, allow_ssh_fallback).await;

            let status = tracker.get_job(&job_id).unwrap();

            assert_eq!(
                status.state,
                if ready {
                    JobState::Completed
                } else {
                    JobState::Failed
                }
            );

            if !ready {
                assert!(
                    status
                        .error_message
                        .contains("NVUE reset may have been accepted")
                );

                assert!(
                    status
                        .error_message
                        .contains("no additional reset was submitted")
                );
            }

            assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(server.received_requests().await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn factory_reset_nvue_disconnect_after_sending_checks_completion_without_resubmitting() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = WiremockHttpsProxy::start(listener.local_addr().unwrap()).await;

        let target = build_factory_reset_target(
            &factory_reset_node_with_port("rack-01", "switch-01", proxy.port.into()),
            true,
        )
        .unwrap();

        let mut node = Arc::into_inner(target).unwrap();

        let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
            panic!("expected NVIDIA switch");
        };

        switch.ssh_exec_for_test = Some(Arc::new(|_| {
            panic!("disconnect after submission must not submit over SSH")
        }));

        switch.factory_default_probe_for_test = Some(Arc::new(|| Ok(true)));
        let tracker = Arc::new(JobTracker::new());

        let job = tracker
            .create_job_if_node_idle("rack-01", "switch-01", JobType::SwitchFactoryDefaultReset)
            .unwrap();

        let job_id = job.id().to_string();
        let service = test_service(Arc::clone(&tracker));

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = vec![0; 8192];
            let size = socket.read(&mut received).await.unwrap();

            assert!(String::from_utf8_lossy(&received[..size]).contains("POST /nvue_v1/system/"));

            // A successful status followed by a truncated body simulates NVUE
            // disappearing during reboot after receiving the reset request.
            socket
                .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 999\r\n\r\n")
                .await
                .unwrap();
        });

        run_factory_reset_job(job, &service, &node, None, true).await;
        server.await.unwrap();

        assert_eq!(tracker.get_job(&job_id).unwrap().state, JobState::Completed);
    }

    #[tokio::test]
    async fn factory_reset_authentication_and_application_rejections_do_not_authorize_ssh() {
        for status in [401, 403, 408, 503] {
            let server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path(nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT))
                .respond_with(ResponseTemplate::new(status))
                .expect(if status == 401 { 2 } else { 1 })
                .mount(&server)
                .await;

            let (_proxy, mut node) = factory_reset_test_target(&server).await;

            let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
                panic!("expected NVIDIA switch");
            };

            switch.ssh_exec_for_test =
                Some(Arc::new(|_| panic!("rejection must not authorize SSH")));

            switch.factory_default_probe_for_test = Some(Arc::new(|| Ok(false)));
            let service = test_service(Arc::new(JobTracker::new()));

            let result = submit_factory_reset_with_fallback(&service, &node, None, true).await;

            assert!(
                matches!(result, Err(FactoryResetError::Rejected(_))),
                "{result:?}"
            );
        }
    }

    #[tokio::test]
    async fn factory_reset_retries_once_with_default_credentials_after_authentication_failure() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT))
            .and(header(
                "authorization",
                "Basic YWRtaW46Y3VycmVudC1wYXNzd29yZA==",
            ))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT))
            .and(header("authorization", "Basic YWRtaW46YWRtaW4="))
            .respond_with(ResponseTemplate::new(201).set_body_json("reset-1"))
            .expect(1)
            .mount(&server)
            .await;

        let (_proxy, mut node) = factory_reset_test_target(&server).await;

        let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
            panic!("expected NVIDIA switch");
        };

        switch.ssh_exec_for_test = Some(Arc::new(|_| {
            panic!("authentication/application errors must not authorize SSH")
        }));

        let service = test_service(Arc::new(JobTracker::new()));

        submit_factory_reset_with_fallback(&service, &node, None, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn factory_reset_does_not_retry_default_credentials_after_non_authentication_failure() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT))
            .and(header(
                "authorization",
                "Basic YWRtaW46Y3VycmVudC1wYXNzd29yZA==",
            ))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path(nvue_client::system::SYSTEM_FACTORY_DEFAULT_ENDPOINT))
            .and(header("authorization", "Basic YWRtaW46YWRtaW4="))
            .respond_with(ResponseTemplate::new(201).set_body_json("reset-1"))
            .expect(0)
            .mount(&server)
            .await;

        let (_proxy, mut node) = factory_reset_test_target(&server).await;

        let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
            panic!("expected NVIDIA switch");
        };

        switch.ssh_exec_for_test = Some(Arc::new(|_| {
            panic!("authentication/application errors must not authorize SSH")
        }));

        let service = test_service(Arc::new(JobTracker::new()));

        assert!(
            submit_factory_reset_with_fallback(&service, &node, None, true)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn factory_reset_batch_propagates_ssh_fallback_and_submits_once() {
        let server = MockServer::start().await;
        let (proxy, mut node) = factory_reset_test_target(&server).await;
        let device = factory_reset_node_with_port("rack-01", "switch-01", proxy.port.into());

        drop(proxy);

        let NodeInstance::SwitchGb200Nvidia(switch) = &mut node else {
            panic!("expected NVIDIA switch");
        };

        let commands = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&commands);

        switch.ssh_exec_for_test = Some(Arc::new(move |command| {
            recorded.lock().unwrap().push(command.to_owned());
            Ok(String::new())
        }));

        switch.factory_default_probe_for_test = Some(Arc::new(|| Ok(true)));

        let tracker = Arc::new(JobTracker::new());
        let service = test_service(Arc::clone(&tracker));

        // Scope the transport doubles to target construction in this batch.
        let response = FACTORY_RESET_TARGET
            .scope(
                Arc::new(node),
                service.handle_batch_reset_switch_factory_default(tonic::Request::new(
                    rm::BatchResetSwitchFactoryDefaultRequest {
                        nodes: Some(rm::NodeSet {
                            nodes: vec![device],
                        }),
                        domain: None,
                        allow_ssh_fallback: true,
                    },
                )),
            )
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();

        assert_eq!(response.status, rm::ReturnCode::Success as i32);
        assert_eq!(response.node_results.len(), 1);

        assert_eq!(
            response.node_results[0].status,
            rm::ReturnCode::Success as i32
        );

        let parent = tracker.get_job(&response.job_id).unwrap();

        assert_eq!(parent.child_job_ids.len(), 1);

        assert!(
            tracker
                .registry()
                .wait_for_terminal_jobs(
                    &[parent.child_job_ids[0].as_str().into()],
                    Some(Duration::from_secs(15)),
                )
                .await
        );

        assert_eq!(
            tracker.get_job(&response.job_id).unwrap().state,
            JobState::Completed
        );

        assert_eq!(
            *commands.lock().unwrap(),
            ["nv action reset system factory-default force"]
        );

        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn factory_reset_rejects_active_alias_and_unresolved_operation_before_submission() {
        for (unresolved, ssh_only) in [(false, false), (true, false), (false, true)] {
            let server = MockServer::start().await;

            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(500))
                .expect(0)
                .mount(&server)
                .await;

            let (_proxy, node) = factory_reset_test_target(&server).await;
            let tracker = Arc::new(JobTracker::new());
            let service = test_service(Arc::clone(&tracker));
            let scopes = configured_switch_scopes(&node, node.nvue_client()).unwrap();

            let endpoint = scopes
                .iter()
                .find(|scope| matches!(scope, JobScope::Endpoint { port, .. } if !ssh_only || *port == 22))
                .unwrap()
                .clone();

            let owner = tracker
                .create_scoped_job(
                    "managed-rack",
                    "managed-switch",
                    JobType::ConfigureScaleUpFabricManagerV2,
                    vec![endpoint],
                )
                .unwrap();

            if unresolved {
                service
                    .fabric_operations
                    .admit(
                        FabricOperationIntent {
                            scope_keys: fabric_scope_keys(&scopes).unwrap(),
                            rule_id: "rule".into(),
                        },
                        owner.id().as_ref(),
                        None,
                    )
                    .unwrap();

                owner.fail(JobFailure::new(JobError::Other, "unresolved"));
            }

            let job = tracker
                .create_job_if_node_idle(
                    node.rack_id(),
                    node.id(),
                    JobType::SwitchFactoryDefaultReset,
                )
                .unwrap();

            let job_id = job.id().to_string();

            run_factory_reset_job(job, &service, &node, None, false).await;

            assert!(server.received_requests().await.unwrap().is_empty());
            assert_eq!(tracker.get_job(&job_id).unwrap().state, JobState::Failed);

            assert_eq!(
                tracker.get_job(&job_id).unwrap().error_code,
                JobError::UpdateInProgress
            );
        }
    }

    #[tokio::test]
    async fn factory_reset_invalidates_activation_and_holds_aliases_through_credential_fallback() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(header(
                "authorization",
                "Basic YWRtaW46Y3VycmVudC1wYXNzd29yZA==",
            ))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(header("authorization", "Basic YWRtaW46YWRtaW4="))
            .respond_with(ResponseTemplate::new(500).set_delay(Duration::from_secs(1)))
            .expect(1)
            .mount(&server)
            .await;

        let (_proxy, node) = factory_reset_test_target(&server).await;
        let node = Arc::new(node);
        let tracker = Arc::new(JobTracker::new());
        let service = Arc::new(test_service(Arc::clone(&tracker)));
        let scopes = configured_switch_scopes(node.as_ref(), node.nvue_client()).unwrap();
        let scope_keys = fabric_scope_keys(&scopes).unwrap();

        let endpoint_scopes: Vec<_> = scopes
            .into_iter()
            .filter(|scope| matches!(scope, JobScope::Endpoint { .. }))
            .collect();

        let identity = ControllerIdentity {
            chassis_serial_number: "chassis".into(),
            tray_index: 1,
        };

        service
            .fabric_operations
            .admit(
                FabricOperationIntent {
                    scope_keys: scope_keys.clone(),
                    rule_id: "rule".into(),
                },
                "configured-job",
                None,
            )
            .unwrap();

        service
            .fabric_operations
            .mark_pending("configured-job", std::slice::from_ref(&identity))
            .unwrap();

        service
            .fabric_operations
            .record_activation(
                "configured-job",
                identity.clone(),
                "activation".into(),
                scope_keys,
            )
            .unwrap();

        service.fabric_operations.finish("configured-job").unwrap();

        assert!(
            service
                .fabric_operations
                .has_activation_proof(&identity, "activation")
                .unwrap()
        );

        let job = tracker
            .create_job_if_node_idle(
                node.rack_id(),
                node.id(),
                JobType::SwitchFactoryDefaultReset,
            )
            .unwrap();

        let job_id = job.id().to_string();
        let worker_service = Arc::clone(&service);

        let worker = tokio::spawn(async move {
            run_factory_reset_job(job, &worker_service, &node, None, false).await;
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            while server.received_requests().await.unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(
            !service
                .fabric_operations
                .has_activation_proof(&identity, "activation")
                .unwrap()
        );

        assert!(tracker.reserve_mutation(endpoint_scopes.clone()).is_err());

        worker.await.unwrap();

        assert_eq!(tracker.get_job(&job_id).unwrap().state, JobState::Failed);
        assert!(tracker.reserve_mutation(endpoint_scopes).is_ok());
    }

    #[tokio::test]
    async fn factory_reset_reports_each_target_when_parent_admission_fails() {
        let tracker = Arc::new(JobTracker::with_retention_limits(
            Duration::from_secs(60),
            0,
        ));

        let service = test_service(tracker);

        let response = service
            .handle_batch_reset_switch_factory_default(tonic::Request::new(
                rm::BatchResetSwitchFactoryDefaultRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![factory_reset_node("rack-01", "switch-01")],
                    }),
                    domain: None,
                    allow_ssh_fallback: false,
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();

        assert_eq!(response.node_results.len(), 1);

        assert_eq!(
            response.node_results[0].status,
            rm::ReturnCode::Failure as i32
        );

        assert!(
            response.node_results[0]
                .error_message
                .contains("failed to create")
        );
    }

    #[tokio::test]
    async fn factory_reset_marks_parent_failed_when_target_is_busy() {
        let tracker = Arc::new(JobTracker::new());

        let _busy = tracker
            .create_job_if_node_idle("rack-01", "switch-01", JobType::SwitchSystemImageUpdate)
            .unwrap();

        let service = test_service(Arc::clone(&tracker));

        let response = service
            .handle_batch_reset_switch_factory_default(tonic::Request::new(
                rm::BatchResetSwitchFactoryDefaultRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![factory_reset_node("rack-01", "switch-01")],
                    }),
                    domain: None,
                    allow_ssh_fallback: false,
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();

        assert_eq!(response.status, rm::ReturnCode::Failure as i32);
        assert_eq!(response.stats.unwrap().failed_nodes, 1);

        assert_eq!(
            tracker.get_job(&response.job_id).unwrap().state,
            JobState::Failed
        );
    }

    #[tokio::test]
    async fn factory_reset_admits_duplicate_target_only_once() {
        let tracker = Arc::new(JobTracker::new());
        let service = test_service(tracker);

        let node = factory_reset_node("rack-01", "switch-01");

        let response = service
            .handle_batch_reset_switch_factory_default(tonic::Request::new(
                rm::BatchResetSwitchFactoryDefaultRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![node.clone(), node],
                    }),
                    domain: None,
                    allow_ssh_fallback: false,
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();

        let successful = response
            .node_results
            .iter()
            .filter(|result| result.status == rm::ReturnCode::Success as i32)
            .count();

        assert_eq!(successful, 1);

        // The duplicate is reported, not silently dropped, and it names the
        // reason so a caller can tell it apart from a busy or invalid node.
        let refused: Vec<_> = response
            .node_results
            .iter()
            .filter(|result| result.status == rm::ReturnCode::Failure as i32)
            .collect();
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].node_id, "switch-01");
        assert_eq!(
            refused[0].error_message,
            "duplicate target rack_id/node_id: rack-01/switch-01"
        );
        assert_eq!(response.stats.unwrap().failed_nodes, 1);

        // One child only: a second concurrent factory reset on the same switch
        // would be a destructive double-mutation.
        let parent = service
            .job_tracker
            .get_job(&response.job_id)
            .expect("the batch parent job should be tracked");
        assert_eq!(parent.child_job_ids.len(), 1);
    }
}
