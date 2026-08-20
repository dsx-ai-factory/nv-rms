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

use std::path::Path;
use std::sync::Arc;

use super::server::{RackManagerServiceImpl, find_rack};
use crate::api::grpc::conversions::proto_node_type_to_domain;
use crate::api::grpc::firmware_handlers::{build_ephemeral_node, resolve_firmware_file};
use crate::domain::node::NodeKind;
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
) -> std::result::Result<Arc<NodeInstance>, String> {
    if device.node_id.is_empty() || device.rack_id.is_empty() {
        return Err("device node_id and rack_id are required".into());
    }

    let node_type = proto_node_type_to_domain(device.r#type.unwrap_or_default())
        .filter(|node_type| node_type.kind() == NodeKind::Switch)
        .ok_or_else(|| "device node type must be a switch".to_owned())?;

    build_ephemeral_node(device, node_type, None).map_err(|error| error.message)
}

fn reserve_factory_reset_jobs(
    service: &RackManagerServiceImpl,
    parent_id: &str,
    targets: Vec<(rm::NodeInfo, Arc<NodeInstance>)>,
    response: &mut rm::NodeBatchResponse,
) -> Vec<(RmsJobHandle, Arc<NodeInstance>)> {
    let mut queued_jobs = Vec::new();

    for (device, node) in targets {
        let pending = match service.job_tracker.create_child_job_if_node_idle(
            parent_id,
            &device.rack_id,
            &device.node_id,
            JobType::SwitchFactoryDefaultReset,
        ) {
            Ok(pending) => pending,
            Err(failure) => {
                response.node_results.push(rm::NodeOperationResult {
                    node_id: device.node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message: failure.message,
                });

                continue;
            }
        };

        response.node_results.push(rm::NodeOperationResult {
            node_id: device.node_id,
            status: rm::ReturnCode::Success.into(),
            error_message: String::new(),
        });

        queued_jobs.push((pending, node));
    }

    queued_jobs
}

async fn submit_factory_reset(
    service: &RackManagerServiceImpl,
    node: &NodeInstance,
    client: &nvue_client::SharedClient,
    domain: Option<&str>,
) -> crate::utilities::error::Result<()> {
    service.initialize_nvue_client(Some(client), domain).await?;
    node.require_nvidia_switch()?
        .submit_factory_default_reset(client)
        .await
}

/// Submits with configured credentials, then retries once with `admin/admin`
/// only when the configured credentials are rejected. If the final submission
/// is also rejected, one successful default SSH login may confirm that an
/// earlier attempt already reset the switch.
async fn submit_factory_reset_with_fallback(
    service: &RackManagerServiceImpl,
    node: &NodeInstance,
    domain: Option<&str>,
) -> crate::utilities::error::Result<()> {
    let configured = node
        .nvue_client()
        .cloned()
        .ok_or_else(|| RmsError::invalid_argument("switch host endpoint is required"))?;

    let result = match submit_factory_reset(service, node, &configured, domain).await {
        Ok(()) => return Ok(()),
        Err(error)
            if error.code == ErrorCode::Unauthenticated
                && !configured.credentials_match("admin", "admin") =>
        {
            let default = node.require_nvidia_switch()?.default_nvue_client()?;

            submit_factory_reset(service, node, &default, domain).await
        }
        Err(error) => Err(error),
    };

    match result {
        Ok(()) => Ok(()),
        Err(error) if error.code == ErrorCode::Unauthenticated => {
            if node
                .require_nvidia_switch()?
                .probe_factory_default_login()
                .await?
            {
                Ok(())
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

async fn run_factory_reset_job(
    job: RmsJobHandle,
    service: &RackManagerServiceImpl,
    node: &NodeInstance,
    domain: Option<&str>,
) {
    job.progress("Resetting switch to factory defaults");

    let result = async {
        submit_factory_reset_with_fallback(service, node, domain).await?;

        node.require_nvidia_switch()?
            .wait_for_factory_default()
            .await
    }
    .await;

    match result {
        Ok(()) => job.complete("Switch factory reset completed", ""),
        Err(error) => job.fail(JobFailure::new(JobError::Other, error.message)),
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

        let mut targets = Vec::new();
        let domain = request.domain;

        for device in devices {
            match build_factory_reset_target(&device) {
                Ok(node) => targets.push((device, node)),
                Err(error_message) => {
                    response.node_results.push(rm::NodeOperationResult {
                        node_id: device.node_id,
                        status: rm::ReturnCode::Failure.into(),
                        error_message,
                    });
                }
            }
        }

        let Some((first, _)) = targets.first() else {
            return Ok(factory_reset_failure(
                response,
                total_nodes,
                "no switch factory-reset jobs created",
            ));
        };

        let parent_rack_id = first.rack_id.clone();

        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(&parent_rack_id, JobType::SwitchFactoryDefaultReset)
        else {
            let message = "failed to create switch factory-reset job";

            response
                .node_results
                .extend(
                    targets
                        .into_iter()
                        .map(|(device, _)| rm::NodeOperationResult {
                            node_id: device.node_id,
                            status: rm::ReturnCode::Failure.into(),
                            error_message: message.to_owned(),
                        }),
                );

            return Ok(factory_reset_failure(response, total_nodes, message));
        };

        response.job_id = parent_id.clone();

        // Reserve every node before spawning. This keeps duplicate targets and
        // concurrent destructive operations excluded for the complete batch.
        let queued_jobs = reserve_factory_reset_jobs(self, &parent_id, targets, &mut response);

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
                        run_factory_reset_job(job, &service, &node, domain.as_deref()).await;
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
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
    use crate::orchestrator::job_lifecycle::JobState;
    use crate::orchestrator::job_tracker::JobTracker;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use crate::transport::ssh::SftpUploadOptions;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct WiremockHttpsProxy {
        port: u16,
        task: tokio::task::JoinHandle<()>,
    }

    impl WiremockHttpsProxy {
        async fn start(upstream: SocketAddr) -> Self {
            use rcgen::generate_simple_self_signed;
            use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
            use tokio::io::copy_bidirectional;
            use tokio::net::{TcpListener, TcpStream};
            use tokio_rustls::TlsAcceptor;

            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

            let cert = generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
                .expect("generate NVUE test certificate");

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

            Self { port, task }
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
            switch_tls_roots: SwitchTlsRoots {
                insecure_switch: true,
                ..SwitchTlsRoots::default()
            },
            sftp_upload_options: SftpUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
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

    async fn factory_reset_test_target(server: &MockServer) -> (WiremockHttpsProxy, NodeInstance) {
        let proxy = WiremockHttpsProxy::start(*server.address()).await;
        let node = build_factory_reset_target(&factory_reset_node_with_port(
            "rack-01",
            "switch-01",
            proxy.port.into(),
        ))
        .unwrap();
        let node = Arc::into_inner(node).expect("factory reset target has one owner");

        (proxy, node)
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

        let (_proxy, node) = factory_reset_test_target(&server).await;
        let service = test_service(Arc::new(JobTracker::new()));

        submit_factory_reset_with_fallback(&service, &node, None)
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

        let (_proxy, node) = factory_reset_test_target(&server).await;
        let service = test_service(Arc::new(JobTracker::new()));

        assert!(
            submit_factory_reset_with_fallback(&service, &node, None)
                .await
                .is_err()
        );
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
        assert_eq!(response.stats.unwrap().failed_nodes, 1);
    }
}
