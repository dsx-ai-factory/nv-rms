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

//! gRPC end-to-end tests.
//!
//! Starts the full gRPC server in-process and exercises RPC flows using
//! the generated tonic client. Tests cover the handler + orchestrator +
//! rack + node creation layers.
//!
//! ## Redfish mockup tests
//!
//! Tests that exercise real Redfish paths use a per-test in-process Rust
//! simulator that serves static JSON from fixture data directories
//! over HTTPS (self-signed TLS via rcgen). Each test gets its own mockup
//! on OS-assigned ports to avoid port conflicts and shared state issues.
//! The Redfish simulator simulates firmware update tasks with configurable
//! delay and failure rate.

use std::collections::HashMap;
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use librms::protos::rack_manager;
use librms::protos::rack_manager::rack_manager_client::RackManagerClient;
use librms::protos::rack_manager_v2;
use librms::protos::rack_manager_v2::rack_manager_v2_client::RackManagerV2Client;
use rackmanagementservice::api::grpc::server::{GrpcServer, SwitchTlsRoots, TlsMode};
use rackmanagementservice::config::{
    ExpectedInventoryCatalog, ExpectedInventoryProfiles, FirmwareTaskPollOptions, WorkflowTimings,
};
use rackmanagementservice::libnmxc::TlsMaterialStore;
use rackmanagementservice::metrics;
use rackmanagementservice::orchestrator::job_tracker::JobTracker;
use rackmanagementservice::orchestrator::rack_manager::RackManager;
use rackmanagementservice::persistence::Backends;
use redfish_test_support::RedfishSimulator;
use tempfile::TempDir;

use rack_manager::*;

#[path = "grpc_e2e/hardware_profile_startup.rs"]
mod hardware_profile_startup;

// ApplyFirmwareObject always activates; the GB200 SOP includes a three-minute
// stabilization plus recovery and post-activation inventory verification.
const GB200_FIRMWARE_OBJECT_APPLY_TIMEOUT: Duration = Duration::from_secs(6 * 60);

// ══════════════════════════════════════════════════════════════════════════════
//  Redfish simulator lifecycle (in-process Rust server)
// ══════════════════════════════════════════════════════════════════════════════

fn write_synthetic_compute_fwpkg(path: &Path) {
    write_synthetic_fwpkg(path, b"BMC", b"GB200Nvl-25.02-9");
}

fn write_synthetic_fwpkg(path: &Path, device_version: &[u8], component_version: &[u8]) {
    const EXPECTED_UUID: [u8; 16] = [
        0xf0, 0x18, 0x87, 0x8c, 0xcb, 0x7d, 0x49, 0x43, 0x98, 0x00, 0xa0, 0x2f, 0x05, 0x9a, 0xca,
        0x02,
    ];

    let version = b"GB200-P4978_TEST";
    let bitmap_bit_length = 8u16;
    let mut pkg = Vec::new();

    pkg.extend_from_slice(&EXPECTED_UUID);
    pkg.push(1);
    pkg.extend_from_slice(&0u16.to_le_bytes());
    pkg.extend_from_slice(&[0u8; 13]);
    pkg.extend_from_slice(&bitmap_bit_length.to_le_bytes());
    pkg.push(1);
    pkg.push(version.len() as u8);
    pkg.extend_from_slice(version);

    pkg.push(1);
    pkg.extend_from_slice(&0u16.to_le_bytes());
    pkg.push(0);
    pkg.extend_from_slice(&0u32.to_le_bytes());
    pkg.push(1);
    pkg.push(device_version.len() as u8);
    pkg.extend_from_slice(&0u16.to_le_bytes());
    let mut bitmap = vec![0u8; (bitmap_bit_length as usize).div_ceil(8)];
    bitmap[0] = 0x01;
    pkg.extend_from_slice(&bitmap);
    pkg.extend_from_slice(device_version);

    pkg.extend_from_slice(&1u16.to_le_bytes());
    pkg.extend_from_slice(&0x000Au16.to_le_bytes());
    pkg.extend_from_slice(&0x0001u16.to_le_bytes());
    pkg.extend_from_slice(&0u32.to_le_bytes());
    pkg.extend_from_slice(&0u16.to_le_bytes());
    pkg.extend_from_slice(&0x01u16.to_le_bytes());
    let payload_offset = pkg.len() as u32 + 4 + 4 + 1 + 1 + component_version.len() as u32;
    pkg.extend_from_slice(&payload_offset.to_le_bytes());
    pkg.extend_from_slice(&4u32.to_le_bytes());
    pkg.push(1);
    pkg.push(component_version.len() as u8);
    pkg.extend_from_slice(component_version);
    pkg.extend_from_slice(b"FWFW");

    std::fs::write(path, pkg).expect("write synthetic fwpkg");
}

fn write_synthetic_powershelf_tar(path: &Path) {
    let tar_file = std::fs::File::create(path).expect("create synthetic powershelf tar");
    let mut builder = tar::Builder::new(tar_file);

    let manifest = b"purpose=PSU\nversion=1.2.3\nmodel=PowerShelf\n";
    let mut manifest_header = tar::Header::new_gnu();
    manifest_header.set_size(manifest.len() as u64);
    manifest_header.set_mode(0o644);
    manifest_header.set_cksum();
    builder
        .append_data(&mut manifest_header, "MANIFEST", Cursor::new(manifest))
        .expect("append powershelf manifest");

    let payload = b"synthetic powershelf firmware";
    let mut payload_header = tar::Header::new_gnu();
    payload_header.set_size(payload.len() as u64);
    payload_header.set_mode(0o644);
    payload_header.set_cksum();
    builder
        .append_data(&mut payload_header, "firmware.bin", Cursor::new(payload))
        .expect("append powershelf payload");

    builder.finish().expect("finish synthetic powershelf tar");
}

/// Start a per-test mockup server with compute and powershelf on OS-assigned ports.
///
/// Each test gets its own `RedfishSimulator` instance with fresh ports and
/// independent task state, so tests can run concurrently without interfering
/// with each other, and each test's tokio runtime can tear down cleanly without
/// cancelling another test's listener tasks.
///
/// The expensive part — extracting the tar.gz archives and parsing all JSON —
/// is paid only once per test-binary run. `RedfishSimulator::start()` checks a
/// process-wide `LazyLock` cache registry: on the first call it loads the
/// archive from disk and stores an `Arc` under the path key; every subsequent
/// call across all tests clones that `Arc` cheaply.
async fn start_mockup() -> RedfishSimulator {
    RedfishSimulator::builder()
        .add_gb200_compute(0)
        .add_powershelf(0)
        .start()
        .await
}

const COMPUTE_HOST: &str = "127.0.0.1";
const POWERSHELF_HOST: &str = "127.0.0.1";

fn operation_response(response: &Option<OperationResponse>) -> &OperationResponse {
    response.as_ref().expect("operation response must be set")
}

fn assert_stats(
    stats: &Option<NodeOperationStats>,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) {
    let Some(stats) = stats.as_ref() else {
        panic!("node operation stats must be set");
    };

    assert_eq!(stats.total_nodes, total_nodes);
    assert_eq!(stats.successful_nodes, successful_nodes);
    assert_eq!(stats.failed_nodes, failed_nodes);
}

fn batch_response(response: &Option<NodeBatchResponse>) -> &NodeBatchResponse {
    let Some(response) = response.as_ref() else {
        panic!("batch response must be set");
    };

    assert!(response.stats.is_some(), "node operation stats must be set");

    response
}

fn assert_batch_stats(
    response: &NodeBatchResponse,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
) {
    assert_stats(&response.stats, total_nodes, successful_nodes, failed_nodes);
}

async fn start_server_with_manager(rm: Arc<RackManager>) -> (E2eServer, E2eClient) {
    start_server_with_manager_and_catalog(rm, ExpectedInventoryCatalog::default()).await
}

async fn start_server_with_manager_and_catalog(
    rm: Arc<RackManager>,
    expected_inventory_catalog: ExpectedInventoryCatalog,
) -> (E2eServer, E2eClient) {
    start_server_with_manager_catalog_and_switch_tls_roots(
        rm,
        expected_inventory_catalog,
        SwitchTlsRoots {
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        },
    )
    .await
}

async fn start_server_with_manager_catalog_and_switch_tls_roots(
    rm: Arc<RackManager>,
    expected_inventory_catalog: ExpectedInventoryCatalog,
    switch_tls_roots: SwitchTlsRoots,
) -> (E2eServer, E2eClient) {
    let firmware_dir = TempDir::new().expect("create firmware TempDir");
    let jt = Arc::new(JobTracker::new());

    let mut grpc_server = GrpcServer::new(0, rm, jt, Backends::memory())
        .with_firmware_dir(firmware_dir.path())
        .with_expected_inventory_catalog(expected_inventory_catalog)
        .with_workflow_timings(WorkflowTimings {
            // The mock BMCs complete a task in milliseconds, so the production
            // 30s poll interval would push every firmware job past the
            // `wait_for_job_terminal` deadline.
            firmware_task_poll: FirmwareTaskPollOptions {
                interval: Duration::from_millis(10),
                ..FirmwareTaskPollOptions::default()
            },
            ..WorkflowTimings::default()
        })
        .with_insecure_listener()
        .with_switch_tls_roots(switch_tls_roots);

    let (incoming, port) = grpc_server.bind_localhost_ephemeral_port().unwrap();

    let _metrics_registry = metrics::init(&metrics::MetricsInfo {
        grpc_port: port,
        firmware_dir: firmware_dir.path().to_string_lossy().into_owned(),
        persistence_type: "memory".to_string(),
        tls_mode: TlsMode::Insecure,
    })
    .unwrap();

    grpc_server
        .start_with_bound_incoming(incoming)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let addr = format!("http://127.0.0.1:{port}");
    let rack_manager = RackManagerClient::connect(addr.clone()).await.unwrap();
    let rack_manager_v2 = RackManagerV2Client::connect(addr).await.unwrap();

    (
        E2eServer {
            server: grpc_server,
            firmware_dir,
        },
        E2eClient {
            rack_manager,
            rack_manager_v2,
        },
    )
}

async fn start_server() -> (E2eServer, E2eClient) {
    start_server_with_manager(Arc::new(RackManager::new())).await
}

async fn start_server_with_switch_tls_roots(
    switch_tls_roots: SwitchTlsRoots,
) -> (E2eServer, E2eClient) {
    start_server_with_manager_catalog_and_switch_tls_roots(
        Arc::new(RackManager::new()),
        ExpectedInventoryCatalog::default(),
        switch_tls_roots,
    )
    .await
}

/// A throwaway CA used to issue a server certificate for [`WiremockHttpsProxy`]
/// and a client certificate for `TlsMaterialStore`, so the attestation
/// happy-path test can exercise real certificate verification rather than
/// the self-signed cert other NVUE tests use.
struct NvueTestCa {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

impl NvueTestCa {
    fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];

        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Self { cert, key }
    }

    fn issue(&self, sans: Vec<String>) -> (rcgen::Certificate, rcgen::KeyPair) {
        let params = rcgen::CertificateParams::new(sans).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
        (cert, key)
    }
}

/// Writes `TlsMaterialStore`-shaped NVUE client TLS material under
/// `root/domain/` and returns a CA-issued server certificate for the proxy
/// fronting the mock NVUE server.
fn write_nvue_client_tls_material(
    root: &std::path::Path,
    domain: &str,
    ca: &NvueTestCa,
) -> (rcgen::Certificate, rcgen::KeyPair) {
    let (server_cert, server_key) = ca.issue(vec!["localhost".to_owned(), "127.0.0.1".to_owned()]);
    let (client_cert, client_key) = ca.issue(vec!["rms-attestation-test-client".to_owned()]);

    let domain_dir = root.join(domain);
    std::fs::create_dir_all(&domain_dir).unwrap();
    std::fs::write(domain_dir.join("ca.pem"), ca.cert.pem()).unwrap();
    std::fs::write(domain_dir.join("client.pem"), client_cert.pem()).unwrap();
    std::fs::write(domain_dir.join("client.key"), client_key.serialize_pem()).unwrap();

    (server_cert, server_key)
}

struct E2eClient {
    rack_manager: RackManagerClient<tonic::transport::Channel>,
    rack_manager_v2: RackManagerV2Client<tonic::transport::Channel>,
}

impl std::ops::Deref for E2eClient {
    type Target = RackManagerClient<tonic::transport::Channel>;

    fn deref(&self) -> &Self::Target {
        &self.rack_manager
    }
}

impl std::ops::DerefMut for E2eClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rack_manager
    }
}

/// Test server that owns its firmware temp directory so both are cleaned up
/// together when the server goes out of scope.
struct E2eServer {
    server: GrpcServer,
    firmware_dir: TempDir,
}

impl E2eServer {
    fn stop(&mut self) {
        self.server.stop_without_awaiting_terminal();
    }

    fn make_fake_firmware_file(&self, name: &str) -> PathBuf {
        let path = self.firmware_dir.path().join(name);
        if let Err(e) = std::fs::write(&path, b"fake firmware payload") {
            panic!("write firmware fixture {}: {e}", path.display());
        }
        canonicalize_firmware_path(&path)
    }

    fn make_synthetic_compute_fwpkg(&self, name: &str) -> PathBuf {
        let path = self.firmware_dir.path().join(name);
        write_synthetic_compute_fwpkg(&path);
        canonicalize_firmware_path(&path)
    }

    fn make_synthetic_powershelf_tar(&self, name: &str) -> PathBuf {
        let path = self.firmware_dir.path().join(name);
        write_synthetic_powershelf_tar(&path);
        canonicalize_firmware_path(&path)
    }
}

impl Drop for E2eServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn canonicalize_firmware_path(path: &Path) -> PathBuf {
    let Ok(canonical) = path.canonicalize() else {
        panic!("canonicalize firmware fixture {}", path.display());
    };
    canonical
}

fn bmc_endpoint_with_auth(ip: &str, mac: &str, port: u16, user: &str, pass: &str) -> Endpoint {
    Endpoint {
        interface: Some(NetworkInterface {
            ip_address: ip.into(),
            mac_address: mac.into(),
            host_name: None,
        }),
        port: u32::from(port),
        credentials: Some(Credentials {
            auth: Some(credentials::Auth::UserPass(UsernamePassword {
                username: user.into(),
                password: pass.into(),
            })),
        }),
    }
}

fn bmc_endpoint(ip: &str, mac: &str, port: u16) -> Endpoint {
    bmc_endpoint_with_auth(ip, mac, port, "admin", "password")
}

fn host_endpoint(ip_macs: &[(&str, &str)]) -> Endpoint {
    host_endpoint_with_name(ip_macs, None)
}

#[allow(dead_code)]
fn host_endpoint_with_name(ip_macs: &[(&str, &str)], host_name: Option<&str>) -> Endpoint {
    let (ip, mac) = ip_macs.first().copied().unwrap_or_default();

    Endpoint {
        interface: Some(NetworkInterface {
            ip_address: ip.into(),
            mac_address: mac.into(),
            host_name: host_name.map(str::to_owned),
        }),
        port: 0,
        credentials: None,
    }
}

/// Build a NodeInfo matching the pre-submodule-bump flat-field test usage.
/// Use this as a drop-in replacement for inline `NodeInfo { ip_address, ... }`.
#[allow(clippy::too_many_arguments)]
fn mk_node_info(
    node_id: &str,
    rack_id: &str,
    ip: &str,
    mac: &str,
    port: u32,
    username: Option<&str>,
    password: Option<&str>,
    node_type: NodeType,
    host_mac_addresses: Vec<String>,
    host_ip_addresses: Vec<String>,
) -> NodeInfo {
    let user = username.unwrap_or("");
    let pass = password.unwrap_or("");
    let host_pairs: Vec<(String, String)> = host_ip_addresses
        .iter()
        .zip(
            host_mac_addresses
                .iter()
                .chain(std::iter::repeat(&String::new())),
        )
        .map(|(ip, mac)| (ip.clone(), mac.clone()))
        .collect();
    let host_refs: Vec<(&str, &str)> = host_pairs
        .iter()
        .map(|(ip, mac)| (ip.as_str(), mac.as_str()))
        .collect();
    NodeInfo {
        node_id: node_id.into(),
        rack_id: rack_id.into(),
        r#type: Some(node_type as i32),
        bmc_endpoint: Some(bmc_endpoint_with_auth(ip, mac, port as u16, user, pass)),
        host_endpoint: if host_refs.is_empty() {
            None
        } else {
            Some(host_endpoint(&host_refs))
        },
        ..Default::default()
    }
}

fn compute_node(id: &str, rack_id: &str, port: u16) -> NodeInfo {
    NodeInfo {
        node_id: id.into(),
        rack_id: rack_id.into(),
        r#type: Some(NodeType::ComputeGb200Nvidia as i32),
        bmc_endpoint: Some(bmc_endpoint(COMPUTE_HOST, "aa:bb:cc:dd:ee:01", port)),
        host_endpoint: None,
        ..Default::default()
    }
}

fn node_descriptor(role: &str, vendor: &str, product_family: &str) -> NodeDescriptor {
    NodeDescriptor {
        attributes: HashMap::from([
            ("role".to_string(), role.to_string()),
            ("vendor".to_string(), vendor.to_string()),
            ("product_family".to_string(), product_family.to_string()),
        ]),
    }
}

fn vrnvl72_compute_node(id: &str, rack_id: &str, port: u16) -> NodeInfo {
    let mut node = compute_node(id, rack_id, port);
    node.r#type = Some(NodeType::ComputeVrnvl72Nvidia as i32);
    node
}

fn powershelf_node(id: &str, rack_id: &str, port: u16) -> NodeInfo {
    NodeInfo {
        node_id: id.into(),
        rack_id: rack_id.into(),
        r#type: Some(NodeType::PowershelfGb200Liteon as i32),
        bmc_endpoint: Some(bmc_endpoint(POWERSHELF_HOST, "aa:bb:cc:dd:ee:02", port)),
        host_endpoint: None,
        ..Default::default()
    }
}

fn switch_power_node(id: &str, rack_id: &str, port: u16) -> NodeInfo {
    switch_power_node_with_host_credentials(id, rack_id, port, "host-admin", "host-password")
}

fn switch_power_node_with_host_credentials(
    id: &str,
    rack_id: &str,
    port: u16,
    host_user: &str,
    host_pass: &str,
) -> NodeInfo {
    NodeInfo {
        node_id: id.into(),
        rack_id: rack_id.into(),
        r#type: Some(NodeType::SwitchGb200Nvidia as i32),
        bmc_endpoint: Some(bmc_endpoint(COMPUTE_HOST, "aa:bb:cc:dd:ee:03", port)),
        host_endpoint: Some(Endpoint {
            interface: Some(NetworkInterface {
                ip_address: COMPUTE_HOST.into(),
                mac_address: "aa:bb:cc:dd:ee:13".into(),
                host_name: None,
            }),
            port: 0,
            credentials: Some(Credentials {
                auth: Some(credentials::Auth::UserPass(UsernamePassword {
                    username: host_user.into(),
                    password: host_pass.into(),
                })),
            }),
        }),
        ..Default::default()
    }
}

async fn add_nodes(
    client: &mut RackManagerClient<tonic::transport::Channel>,
    nodes: Vec<NodeInfo>,
) {
    let total_nodes = nodes.len() as u32;
    let resp = client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet { nodes }),
        })
        .await
        .unwrap()
        .into_inner();
    let response = operation_response(&resp.response);
    assert_eq!(
        response.status,
        ReturnCode::Success as i32,
        "CreateNodes failed: {}",
        response.message
    );
    assert_stats(&resp.stats, total_nodes, total_nodes, 0);
}

/// Poll a firmware job every 2s until it reaches a terminal state or 30s timeout.
async fn poll_job_until_terminal(
    client: &mut RackManagerClient<tonic::transport::Channel>,
    job_id: &str,
) -> GetFirmwareJobStatusResponse {
    poll_job_until_terminal_within(client, job_id, Duration::from_secs(30)).await
}

async fn poll_job_until_terminal_within(
    client: &mut RackManagerClient<tonic::transport::Channel>,
    job_id: &str,
    timeout: Duration,
) -> GetFirmwareJobStatusResponse {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let job = client
            .get_firmware_job_status(GetFirmwareJobStatusRequest {
                job_id: job_id.into(),
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            job.status,
            ReturnCode::Success as i32,
            "job poll failed: {}",
            job.error_message
        );

        let state = job.job_state;
        if state == FirmwareJobState::Completed as i32 || state == FirmwareJobState::Failed as i32 {
            return job;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "firmware job {job_id} did not reach terminal state within {}s (last state: {state})",
            timeout.as_secs()
        );
    }
}

/// Poll a generic parent job until it reaches a terminal state.
async fn poll_generic_job_until_terminal(
    client: &mut RackManagerClient<tonic::transport::Channel>,
    job_id: &str,
) -> GetJobStatusResponse {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = client
                .get_job_status(GetJobStatusRequest {
                    job_id: job_id.to_owned(),
                    include_child_job_states: true,
                })
                .await
                .unwrap()
                .into_inner();

            let parent = status
                .job_states
                .first()
                .expect("generic job status must include the requested parent");

            if matches!(
                JobExecutionState::try_from(parent.execution_state),
                Ok(JobExecutionState::Completed | JobExecutionState::Failed)
            ) {
                return status;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("generic job {job_id} did not reach a terminal state within 10s"))
}

// ── Basic connectivity ──

#[tokio::test]
async fn version_returns_ok() {
    let (mut server, mut client) = start_server().await;
    let resp = client
        .get_version(GetVersionRequest {})
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.version.is_empty());
    server.stop();
}

// ── Rack CRUD ──

#[tokio::test]
async fn list_racks_empty() {
    let (mut server, mut client) = start_server().await;
    let resp = client
        .list_racks(ListRacksRequest {})
        .await
        .unwrap()
        .into_inner();

    assert!(resp.rack_ids.is_empty());
    server.stop();
}

// ── CreateNodes creates rack + node ──

#[tokio::test]
async fn create_nodes_creates_rack_and_node() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet {
                nodes: vec![mk_node_info(
                    "compute-01",
                    "rack-01",
                    "10.0.0.1",
                    "aa:bb:cc:dd:ee:ff",
                    443,
                    Some("admin"),
                    Some("pass"),
                    NodeType::ComputeGb200Nvidia,
                    vec![],
                    vec![],
                )],
            }),
        })
        .await
        .unwrap()
        .into_inner();

    let response = operation_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);
    assert!(response.message.contains("1 added"));
    assert_stats(&resp.stats, 1, 1, 0);

    let racks = client
        .list_racks(ListRacksRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(racks.rack_ids.len(), 1);
    assert!(racks.rack_ids.contains(&"rack-01".to_owned()));

    server.stop();
}

#[tokio::test]
async fn create_nodes_accepts_bmc_only_switch() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device_bmc_only(
                    "switch-01",
                    "rack-01",
                    "10.0.0.11",
                    "aa:bb:cc:00:00:11",
                )],
            }),
        })
        .await
        .unwrap()
        .into_inner();

    let response = operation_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);
    assert!(response.message.contains("1 added"));
    assert_stats(&resp.stats, 1, 1, 0);

    server.stop();
}

// ── ListNodeInventory ──

#[tokio::test]
async fn list_node_inventory_returns_added_nodes() {
    let (mut server, mut client) = start_server().await;

    client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet {
                nodes: vec![
                    mk_node_info(
                        "c-01",
                        "rack-01",
                        "10.0.0.1",
                        "aa:bb:cc:00:00:01",
                        443,
                        Some("admin"),
                        Some("pass"),
                        NodeType::ComputeGb200Nvidia,
                        vec![],
                        vec![],
                    ),
                    switch_device("s-01", "rack-01", "10.0.0.2", "aa:bb:cc:00:00:02"),
                ],
            }),
        })
        .await
        .unwrap();

    let inventory = client
        .list_node_inventory(ListNodeInventoryRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(inventory.nodes.len(), 2);

    let ids: Vec<&str> = inventory.nodes.iter().map(|n| n.node_id.as_str()).collect();
    assert!(ids.contains(&"c-01"));
    assert!(ids.contains(&"s-01"));

    let Some(switch) = inventory.nodes.iter().find(|n| n.node_id == "s-01") else {
        panic!("missing switch inventory entry");
    };

    assert_eq!(switch.ip_address, "10.0.0.2");
    assert_eq!(switch.port, 443);

    server.stop();
}

#[tokio::test]
async fn vrnvl72_compute_node_round_trips_inventory_type() {
    let (mut server, mut client) = start_server().await;
    let node = mk_node_info(
        "vr-compute-01",
        "vr-rack-01",
        "10.0.0.10",
        "aa:bb:cc:00:00:10",
        443,
        Some("admin"),
        Some("pass"),
        NodeType::ComputeVrnvl72Nvidia,
        vec![],
        vec![],
    );

    add_nodes(&mut client, vec![node]).await;

    let inventory = client
        .list_node_inventory(ListNodeInventoryRequest {})
        .await
        .unwrap()
        .into_inner();

    let node = inventory
        .nodes
        .iter()
        .find(|node| node.node_id == "vr-compute-01")
        .unwrap();
    assert_eq!(node.rack_id, "vr-rack-01");
    assert_eq!(node.r#type, NodeType::ComputeVrnvl72Nvidia as i32);

    server.stop();
}

#[tokio::test]
async fn descriptor_only_vrnvl72_delta_powershelf_round_trips_inventory_descriptor() {
    let (mut server, mut client) = start_server().await;
    let mut node = mk_node_info(
        "vr-ps-delta-01",
        "vr-rack-01",
        "10.0.0.20",
        "aa:bb:cc:00:00:20",
        443,
        Some("admin"),
        Some("pass"),
        NodeType::Unspecified,
        vec![],
        vec![],
    );
    node.r#type = None;
    node.node_descriptor = Some(node_descriptor("Power Shelf", "Delta", "VR-NVL72"));

    add_nodes(&mut client, vec![node]).await;

    let inventory = client
        .list_node_inventory(ListNodeInventoryRequest {})
        .await
        .unwrap()
        .into_inner();

    let node = inventory
        .nodes
        .iter()
        .find(|node| node.node_id == "vr-ps-delta-01")
        .expect("descriptor-only VR Delta shelf should be present in inventory");
    assert_eq!(node.rack_id, "vr-rack-01");
    assert_eq!(node.r#type, NodeType::Unspecified as i32);
    assert_eq!(
        node.node_descriptor.as_ref(),
        Some(&node_descriptor("power_shelf", "delta", "vrnvl72"))
    );

    server.stop();
}

#[tokio::test]
async fn vrnvl72_delta_powershelf_power_uses_chassis_reset() {
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const RESET_TARGET: &str = "/redfish/v1/Chassis/PowerShelf_0/Actions/Chassis.Reset";

    // AHE54V2037A shelves on an NVIDIA PMC expose no Delta OEM PSU actions,
    // so power control must not depend on /redfish/v1/PowerEquipment.
    let bmc = MockServer::start().await;
    let bmc_https = WiremockHttpsProxy::start(*bmc.address()).await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/PowerEquipment/PowerShelves"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&bmc)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Members": [{ "@odata.id": "/redfish/v1/Chassis/PowerShelf_0" }]
        })))
        .mount(&bmc)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/PowerShelf_0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Id": "PowerShelf_0",
            "Manufacturer": "DELTA",
            "PowerState": "On",
            "Actions": {
                "#Chassis.Reset": {
                    "@Redfish.ActionInfo": "/redfish/v1/Chassis/PowerShelf_0/ResetActionInfo",
                    "target": RESET_TARGET
                }
            }
        })))
        .mount(&bmc)
        .await;
    Mock::given(method("GET"))
        .and(path("/redfish/v1/Chassis/PowerShelf_0/ResetActionInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "Parameters": [{
                "Name": "ResetType",
                "Required": true,
                "DataType": "String",
                "AllowableValues": ["On", "ForceOn", "ForceOff"]
            }]
        })))
        .mount(&bmc)
        .await;
    Mock::given(method("POST"))
        .and(path(RESET_TARGET))
        .and(body_json(serde_json::json!({ "ResetType": "ForceOff" })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&bmc)
        .await;
    Mock::given(method("POST"))
        .and(path(RESET_TARGET))
        .and(body_json(serde_json::json!({ "ResetType": "On" })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&bmc)
        .await;

    let (mut server, mut client) = start_server().await;
    let mut node = NodeInfo {
        node_id: "vr-ps-delta-01".into(),
        rack_id: "vr-rack-01".into(),
        r#type: None,
        bmc_endpoint: Some(bmc_endpoint(
            "127.0.0.1",
            "aa:bb:cc:00:00:20",
            bmc_https.port(),
        )),
        ..Default::default()
    };
    node.node_descriptor = Some(node_descriptor("power_shelf", "delta", "vrnvl72"));

    // Inline mode: no registration.
    let inline = client
        .batch_set_power_state(BatchSetPowerStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![node.clone()],
            }),
            operation: PowerOperation::ForceOff as i32,
        })
        .await
        .unwrap()
        .into_inner();
    let response = batch_response(&inline.response);
    assert_eq!(
        response.status,
        ReturnCode::Success as i32,
        "{:?}",
        response.node_results
    );

    // Registered mode.
    add_nodes(&mut client, vec![node]).await;
    let registered = client
        .set_power_state(SetPowerStateRequest {
            node_id: "vr-ps-delta-01".into(),
            operation: PowerOperation::On as i32,
            rack_id: "vr-rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(registered.status, ReturnCode::Success as i32);

    server.stop();
}

#[tokio::test]
async fn descriptor_only_nodes_support_inventory_and_power() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let mut nvidia = compute_node("c-01", "rack-01", mockup.ports()[0]);
    nvidia.r#type = None;
    nvidia.node_descriptor = Some(node_descriptor("compute", "NVIDIA", "gb200"));

    let mut wiwynn = compute_node("c-02", "rack-01", mockup.ports()[0]);
    wiwynn.r#type = None;
    wiwynn.node_descriptor = Some(node_descriptor("compute", "Wiwynn", "gb200"));

    let mut dell = compute_node("c-03", "rack-01", mockup.ports()[0]);
    dell.r#type = None;
    dell.node_descriptor = Some(node_descriptor("compute", "Dell", "gb200"));
    add_nodes(
        &mut client,
        vec![nvidia.clone(), wiwynn.clone(), dell.clone()],
    )
    .await;

    let inventory = client
        .list_node_inventory(ListNodeInventoryRequest {})
        .await
        .unwrap()
        .into_inner();

    let nvidia_inventory = inventory
        .nodes
        .iter()
        .find(|node| node.node_id == "c-01")
        .expect("descriptor-only node should be present in inventory");

    assert_eq!(nvidia_inventory.r#type, NodeType::ComputeGb200Nvidia as i32);

    assert_eq!(
        nvidia_inventory.node_descriptor.as_ref(),
        Some(&node_descriptor("compute", "nvidia", "gb200"))
    );

    let wiwynn_inventory = inventory
        .nodes
        .iter()
        .find(|node| node.node_id == "c-02")
        .expect("Wiwynn descriptor-only node should be present in inventory");

    assert_eq!(wiwynn_inventory.r#type, NodeType::Unspecified as i32);
    assert_eq!(
        wiwynn_inventory.node_descriptor.as_ref(),
        Some(&node_descriptor("compute", "wiwynn", "gb200"))
    );

    let dell_inventory = inventory
        .nodes
        .iter()
        .find(|node| node.node_id == "c-03")
        .unwrap();
    assert_eq!(dell_inventory.r#type, NodeType::Unspecified as i32);
    assert_eq!(
        dell_inventory.node_descriptor.as_ref(),
        Some(&node_descriptor("compute", "dell", "gb200"))
    );

    let power = client
        .batch_get_power_state(BatchGetPowerStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![nvidia, wiwynn, dell],
            }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        batch_response(&power.response).status,
        ReturnCode::Success as i32
    );

    server.stop();
}

#[tokio::test]
async fn inventory_profile_survives_grpc_normalization_and_inventory_round_trip() {
    let profiles: ExpectedInventoryProfiles = serde_json::from_value(serde_json::json!({
        "gb200-compute-variant-a": ["FW_BMC_0", "HGX_FW_GPU_0"]
    }))
    .unwrap();
    let catalog = ExpectedInventoryCatalog::from(&profiles);
    let (mut server, mut client) =
        start_server_with_manager_and_catalog(Arc::new(RackManager::new()), catalog).await;
    let mut node = mk_node_info(
        "c-01",
        "rack-01",
        "10.0.0.1",
        "aa:bb:cc:dd:ee:ff",
        443,
        Some("admin"),
        Some("pass"),
        NodeType::ComputeGb200Nvidia,
        vec![],
        vec![],
    );
    node.node_descriptor = Some(NodeDescriptor {
        attributes: HashMap::from([(
            "inventory_profile".to_owned(),
            " gb200-compute-variant-a ".to_owned(),
        )]),
    });

    add_nodes(&mut client, vec![node]).await;
    let inventory = client
        .list_node_inventory(ListNodeInventoryRequest {})
        .await
        .unwrap()
        .into_inner();
    let descriptor = inventory.nodes[0].node_descriptor.as_ref().unwrap();

    assert_eq!(
        descriptor
            .attributes
            .get("inventory_profile")
            .map(String::as_str),
        Some("gb200-compute-variant-a")
    );
    assert_eq!(
        descriptor.attributes.get("role").map(String::as_str),
        Some("compute")
    );
    assert_eq!(
        descriptor
            .attributes
            .get("product_family")
            .map(String::as_str),
        Some("gb200")
    );

    server.stop();
}

// ── DeleteNode ──

#[tokio::test]
async fn delete_node_removes_from_inventory() {
    let (mut server, mut client) = start_server().await;

    client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet {
                nodes: vec![mk_node_info(
                    "c-01",
                    "rack-01",
                    "10.0.0.1",
                    "aa:bb:cc:dd:ee:ff",
                    443,
                    Some("admin"),
                    Some("pass"),
                    NodeType::ComputeGb200Nvidia,
                    vec![],
                    vec![],
                )],
            }),
        })
        .await
        .unwrap();

    let resp = client
        .delete_node(DeleteNodeRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let response = operation_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);

    let inventory = client
        .list_node_inventory(ListNodeInventoryRequest {})
        .await
        .unwrap()
        .into_inner();

    assert!(inventory.nodes.is_empty());
    server.stop();
}

// ── Firmware job status ──

#[tokio::test]
async fn get_firmware_job_status_not_found() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .get_firmware_job_status(GetFirmwareJobStatusRequest {
            job_id: "nonexistent-job".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(resp.error_message.contains("not found"));

    server.stop();
}

// ── RackManager error paths ──

#[tokio::test]
async fn create_nodes_missing_credentials_fails() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet {
                nodes: vec![mk_node_info(
                    "c-01",
                    "rack-01",
                    "10.0.0.1",
                    "aa:bb:cc:dd:ee:ff",
                    443,
                    None,
                    None,
                    NodeType::ComputeGb200Nvidia,
                    vec![],
                    vec![],
                )],
            }),
        })
        .await
        .unwrap()
        .into_inner();

    let response = operation_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(response.message.contains("missing credentials"));

    server.stop();
}

#[tokio::test]
async fn create_nodes_unspecified_type_without_descriptor_fails() {
    let (mut server, mut client) = start_server().await;

    let result = client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet {
                nodes: vec![mk_node_info(
                    "c-01",
                    "rack-01",
                    "10.0.0.1",
                    "aa:bb:cc:dd:ee:ff",
                    443,
                    Some("admin"),
                    Some("pass"),
                    NodeType::Unspecified,
                    vec![],
                    vec![],
                )],
            }),
        })
        .await;

    let status = result.expect_err("CreateNodes should reject missing node descriptor");

    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    assert!(
        status
            .message()
            .contains("node descriptor is required when node type is unspecified")
    );

    server.stop();
}

#[tokio::test]
async fn remove_nonexistent_node_fails() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .delete_node(DeleteNodeRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let response = operation_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);

    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  Redfish simulator tests (each test starts its own simulator on random ports)
// ══════════════════════════════════════════════════════════════════════════════

// ── Compute power state ──

#[tokio::test]
async fn compute_get_power_state() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;

    let resp = client
        .get_power_state(GetPowerStateRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Success as i32);
    assert_eq!(resp.pstate, "ON");
    assert_eq!(resp.node_id, "c-01");
    server.stop();
}

// ── Switch power state ──

#[tokio::test]
async fn switch_get_power_state_uses_bmc_when_nvos_credentials_are_invalid() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![switch_power_node_with_host_credentials(
            "sw-01",
            "rack-01",
            mockup.ports()[0],
            "bad_nvos_user",
            "bad_nvos_pass",
        )],
    )
    .await;

    let resp = client
        .get_power_state(GetPowerStateRequest {
            node_id: "sw-01".into(),
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Success as i32);
    assert_eq!(resp.pstate, "ON");
    server.stop();
}

#[tokio::test]
async fn compute_set_power_state_off_on() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;

    let off_resp = client
        .set_power_state(SetPowerStateRequest {
            node_id: "c-01".into(),
            operation: PowerOperation::Off as i32,
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(off_resp.status, ReturnCode::Success as i32);

    let on_resp = client
        .set_power_state(SetPowerStateRequest {
            node_id: "c-01".into(),
            operation: PowerOperation::On as i32,
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(on_resp.status, ReturnCode::Success as i32);

    server.stop();
}

// ── Compute firmware inventory ──

#[tokio::test]
async fn compute_get_node_firmware_inventory() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;

    let resp = client
        .get_node_firmware_inventory(GetNodeFirmwareInventoryRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Success as i32);
    assert!(
        !resp.firmware_list.is_empty(),
        "expected firmware entries from mockup"
    );

    let names: Vec<&str> = resp.firmware_list.iter().map(|f| f.name.as_str()).collect();
    assert!(names.contains(&"FW_BMC_0"), "missing FW_BMC_0 in {names:?}");
    assert!(
        names.contains(&"HGX_FW_BMC_0"),
        "missing HGX_FW_BMC_0 in {names:?}"
    );
    assert!(names.contains(&"UEFI"), "missing UEFI in {names:?}");

    server.stop();
}

#[tokio::test]
async fn batch_get_firmware_inventory_supports_unregistered_nodes_and_partial_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let mut invalid = compute_node("invalid-01", "rack-01", mockup.ports()[0]);
    invalid.bmc_endpoint = None;

    let resp = client
        .batch_get_firmware_inventory(BatchGetFirmwareInventoryRequest {
            nodes: Some(NodeSet {
                nodes: vec![compute_node("c-01", "rack-01", mockup.ports()[0]), invalid],
            }),
        })
        .await?
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert_batch_stats(response, 2, 1, 1);
    assert_eq!(response.node_results.len(), 2);
    assert_eq!(response.node_results[0].node_id, "c-01");
    assert_eq!(response.node_results[0].status, ReturnCode::Success as i32);
    assert_eq!(response.node_results[1].node_id, "invalid-01");
    assert_eq!(response.node_results[1].status, ReturnCode::Failure as i32);
    assert_eq!(resp.nodes.len(), 1);
    assert_eq!(resp.nodes[0].node_id, "c-01");
    let names: Vec<&str> = resp.nodes[0]
        .firmware_list
        .iter()
        .map(|firmware| firmware.name.as_str())
        .collect();
    assert!(names.contains(&"FW_BMC_0"), "missing FW_BMC_0 in {names:?}");

    server.stop();
    Ok(())
}

#[tokio::test]
async fn batch_get_firmware_inventory_rejects_empty_node_set()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_get_firmware_inventory(BatchGetFirmwareInventoryRequest { nodes: None })
        .await?
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert_eq!(response.message, "No nodes specified in request");
    assert_batch_stats(response, 0, 0, 0);
    assert!(response.node_results.is_empty());
    assert!(resp.nodes.is_empty());

    server.stop();
    Ok(())
}

#[tokio::test]
async fn compare_firmware_object_infers_type_and_reports_version_drift_as_data()
-> Result<(), Box<dyn std::error::Error>> {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let device = compute_node("c-01", "rack-01", mockup.ports()[0]);
    let config_json = firmware_object_release_catalog_json(
        "e2e-compare-fw",
        serde_json::json!([{
            "SKUID": "699-24764-0001-TS3",
            "Name": "GB200-Compute",
            "Type": "Compute Tray",
            "Components": {
                "Firmware": [{
                    "Component": "BMC",
                    "Version": "proposed-version",
                    "Type": "Prod",
                    "Locations": [{
                        "Location": "https://example.test/compute-bmc.fwpkg",
                        "LocationType": "HTTPS",
                        "Type": "Firmware"
                    }]
                }]
            }
        }]),
    );

    let resp = client
        .compare_firmware_object(CompareFirmwareObjectRequest {
            config_json,
            firmware_type: String::new(),
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            component_filters: HashMap::from([(
                NodeType::ComputeGb200Nvidia as i32,
                FirmwareObjectComponentFilter {
                    components: vec!["BMC".to_owned()],
                },
            )]),
            ..Default::default()
        })
        .await?
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);
    assert_batch_stats(response, 1, 1, 0);
    assert_eq!(resp.object_id, "e2e-compare-fw_test");
    assert!(!resp.matches_manifest);
    assert_eq!(resp.nodes.len(), 1);
    assert!(!resp.nodes[0].matches_manifest);
    assert_eq!(resp.nodes[0].components.len(), 1);
    let comparison = &resp.nodes[0].components[0];
    assert_eq!(comparison.component, "BMC");
    assert_eq!(comparison.installed_component, "FW_BMC_0");
    assert_eq!(comparison.proposed_version, "proposed-version");
    assert_eq!(
        comparison.status,
        FirmwareComparisonStatus::Mismatched as i32
    );

    server.stop();
    Ok(())
}

#[tokio::test]
async fn vrnvl72_inventory_and_manifest_comparison_do_not_require_nvue()
-> Result<(), Box<dyn std::error::Error>> {
    let bmc = wiremock::MockServer::start().await;
    let bmc_https = WiremockHttpsProxy::start(*bmc.address()).await;
    let host = wiremock::MockServer::start().await;
    let host_https = WiremockHttpsProxy::start(*host.address()).await;
    mount_switch_bmc_inventory(&bmc, &[("MGX_FW_BMC_0", "88.1")]).await;
    let (mut server, mut client) = start_server().await;
    let mut device = switch_password_device("sw-01", "rack-01", host_https.port());
    device.r#type = Some(NodeType::SwitchVrnvl72Nvidia as i32);
    device.bmc_endpoint = Some(bmc_endpoint(
        "127.0.0.1",
        "aa:bb:cc:00:20:01",
        bmc_https.port(),
    ));

    // Even a registered switch with an unusable NVUE endpoint must remain readable.
    add_nodes(&mut client, vec![device.clone()]).await;
    let node_inventory = client
        .get_node_firmware_inventory(GetNodeFirmwareInventoryRequest {
            rack_id: "rack-01".to_owned(),
            node_id: "sw-01".to_owned(),
        })
        .await?
        .into_inner();
    assert_eq!(node_inventory.status, ReturnCode::Success as i32);
    assert_eq!(node_inventory.firmware_list[0].name, "MGX_FW_BMC_0");
    let rack_inventory = client
        .get_rack_firmware_inventory(GetRackFirmwareInventoryRequest {
            rack_id: "rack-01".to_owned(),
        })
        .await?
        .into_inner();
    assert_eq!(rack_inventory.status, ReturnCode::Success as i32);
    assert_eq!(rack_inventory.nodes.len(), 1);

    {
        let node_type = NodeType::SwitchVrnvl72Nvidia;
        let manifest_component = "BMC+CPLD+SMA+ERoT+SBIOS";
        device.r#type = Some(node_type as i32);
        device.host_endpoint = None;
        let inventory = client
            .batch_get_firmware_inventory(BatchGetFirmwareInventoryRequest {
                nodes: Some(NodeSet {
                    nodes: vec![device.clone()],
                }),
            })
            .await?
            .into_inner();
        assert_eq!(
            batch_response(&inventory.response).status,
            ReturnCode::Success as i32
        );
        assert_eq!(inventory.nodes[0].firmware_list[0].version, "88.1");

        for (version, status) in [
            ("88.1", FirmwareComparisonStatus::Matched),
            ("88.2", FirmwareComparisonStatus::Mismatched),
        ] {
            let comparison = client.compare_firmware_object(CompareFirmwareObjectRequest {
                config_json: firmware_object_release_catalog_json("switch-compare", serde_json::json!([{
                    "Name":"Switch", "Type":"Switch Tray", "Components":{"Firmware":[{
                        "Component":manifest_component, "Type":"Prod",
                        "SubComponents":[{"Component":"BMC", "Version":version}],
                        "Locations":[{"Location":"https://example.test/switch.fwpkg", "LocationType":"HTTPS", "Type":"Firmware"}]
                    }]}
                }])),
                component_filters: HashMap::from([(node_type as i32, FirmwareObjectComponentFilter { components:vec!["BMC".to_owned()] })]),
                nodes: Some(NodeSet { nodes: vec![device.clone()] }),
                ..Default::default()
            }).await?.into_inner();
            assert_eq!(
                batch_response(&comparison.response).status,
                ReturnCode::Success as i32,
                "{comparison:?}"
            );
            assert_eq!(
                comparison.nodes[0].components[0].installed_component,
                "MGX_FW_BMC_0"
            );
            assert_eq!(comparison.nodes[0].components[0].status, status as i32);
            assert_eq!(
                comparison.matches_manifest,
                status == FirmwareComparisonStatus::Matched
            );
        }
    }
    assert!(host.received_requests().await.unwrap().is_empty());
    server.stop();
    Ok(())
}

// ── Compute rack-level firmware inventory ──

#[tokio::test]
async fn gb_switch_inventory_and_manifest_comparison_keep_using_nvue() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let host = MockServer::start().await;
    let https = WiremockHttpsProxy::start(*host.address()).await;
    let bmc = MockServer::start().await;
    let bmc_https = WiremockHttpsProxy::start(*bmc.address()).await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/firmware"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "BMC":{"actual-firmware":"88.1"}
        })))
        .expect(4)
        .mount(&host)
        .await;
    let (mut server, mut client) = start_server().await;
    for node_type in [NodeType::SwitchGb200Nvidia, NodeType::SwitchGb300Nvidia] {
        let mut device = switch_password_device("gb-switch", "rack-01", https.port());
        device.r#type = Some(node_type as i32);
        device.bmc_endpoint = Some(bmc_endpoint(
            "127.0.0.1",
            "aa:bb:cc:00:20:01",
            bmc_https.port(),
        ));
        let response = client
            .batch_get_firmware_inventory(BatchGetFirmwareInventoryRequest {
                nodes: Some(NodeSet {
                    nodes: vec![device.clone()],
                }),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            batch_response(&response.response).status,
            ReturnCode::Success as i32
        );
        assert_eq!(response.nodes[0].firmware_list[0].version, "88.1");
        let comparison = client.compare_firmware_object(CompareFirmwareObjectRequest {
            config_json: firmware_object_release_catalog_json("gb-compare", serde_json::json!([{
                "Name":"Switch", "Type":"Switch Tray", "Components":{"Firmware":[{
                    "Component":"BMC+FPGA+EROT", "Type":"Prod",
                    "SubComponents":[{"Component":"BMC", "Version":"88.1"}],
                    "Locations":[{"Location":"https://example.test/switch.fwpkg", "LocationType":"HTTPS", "Type":"Firmware"}]
                }]}
            }])),
            component_filters:HashMap::from([(node_type as i32, FirmwareObjectComponentFilter { components:vec!["BMC".to_owned()] })]),
            nodes:Some(NodeSet { nodes:vec![device.clone()] }),
            ..Default::default()
        }).await.unwrap().into_inner();
        assert_eq!(
            batch_response(&comparison.response).status,
            ReturnCode::Success as i32,
            "{comparison:?}"
        );
        assert!(comparison.matches_manifest);
        device.host_endpoint = None;
        let response = client
            .batch_get_firmware_inventory(BatchGetFirmwareInventoryRequest {
                nodes: Some(NodeSet {
                    nodes: vec![device],
                }),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            batch_response(&response.response).status,
            ReturnCode::Failure as i32
        );
    }
    assert!(bmc.received_requests().await.unwrap().is_empty());
    server.stop();
}

#[tokio::test]
async fn compute_get_rack_firmware_inventory() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;

    let resp = client
        .get_rack_firmware_inventory(GetRackFirmwareInventoryRequest {
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Success as i32);
    assert_eq!(resp.nodes.len(), 1);
    assert!(!resp.nodes[0].firmware_list.is_empty());

    server.stop();
}

// ── Powershelf power state ──

#[tokio::test]
async fn powershelf_get_power_state() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![powershelf_node("ps-01", "rack-01", mockup.ports()[1])],
    )
    .await;

    let resp = client
        .get_power_state(GetPowerStateRequest {
            node_id: "ps-01".into(),
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Success as i32);
    assert_eq!(resp.pstate, "ON");
    server.stop();
}

#[tokio::test]
async fn powershelf_set_power_state() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![powershelf_node("ps-01", "rack-01", mockup.ports()[1])],
    )
    .await;

    // The powershelf maps ForceOn to On (no separate hardware surface). Graceful
    // requests (Off, GracefulShutdown) use a graceful path when the shelf
    // advertises one (the mockup advertises ResetType GracefulShutdown) and fall
    // back to ForceOff otherwise.
    for op in [
        PowerOperation::ForceOff,
        PowerOperation::On,
        PowerOperation::Off,
        PowerOperation::GracefulShutdown,
        PowerOperation::ForceOn,
    ] {
        let resp = client
            .set_power_state(SetPowerStateRequest {
                node_id: "ps-01".into(),
                operation: op as i32,
                rack_id: "rack-01".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.status,
            ReturnCode::Success as i32,
            "power op {op:?} failed"
        );
    }

    for op in [
        PowerOperation::Reset,
        PowerOperation::GracefulRestart,
        PowerOperation::ForceRestart,
    ] {
        let resp = client
            .set_power_state(SetPowerStateRequest {
                node_id: "ps-01".into(),
                operation: op as i32,
                rack_id: "rack-01".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.status,
            ReturnCode::Failure as i32,
            "power op {op:?} should be unsupported on powershelf"
        );
    }

    server.stop();
}

#[tokio::test]
async fn powershelf_batch_set_power_state() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    for op in [PowerOperation::ForceOff, PowerOperation::On] {
        let resp = client
            .batch_set_power_state(BatchSetPowerStateRequest {
                nodes: Some(NodeSet {
                    nodes: vec![powershelf_node("ps-01", "rack-01", mockup.ports()[1])],
                }),
                operation: op as i32,
            })
            .await
            .unwrap()
            .into_inner();

        let response = batch_response(&resp.response);
        assert_eq!(
            response.status,
            ReturnCode::Success as i32,
            "power op {op:?} failed"
        );
        assert_batch_stats(response, 1, 1, 0);
        assert_eq!(response.node_results.len(), 1);
        assert_eq!(response.node_results[0].node_id, "ps-01");
        assert_eq!(response.node_results[0].status, ReturnCode::Success as i32);
    }

    server.stop();
}

#[tokio::test]
async fn batch_set_power_state_supports_all_node_types_for_power_on() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_set_power_state(BatchSetPowerStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![
                    compute_node("c-01", "rack-01", mockup.ports()[0]),
                    powershelf_node("ps-01", "rack-01", mockup.ports()[1]),
                    switch_power_node("sw-01", "rack-01", mockup.ports()[0]),
                ],
            }),
            operation: PowerOperation::On as i32,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);
    assert_batch_stats(response, 3, 3, 0);
    assert_eq!(response.node_results.len(), 3);

    server.stop();
}

#[tokio::test]
async fn switch_batch_set_power_state_supports_redfish_reset_types() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    for op in [
        PowerOperation::ForceOff,
        PowerOperation::On,
        PowerOperation::Reset,
        PowerOperation::ForceOn,
        PowerOperation::GracefulShutdown,
        PowerOperation::GracefulRestart,
        PowerOperation::ForceRestart,
    ] {
        let resp = client
            .batch_set_power_state(BatchSetPowerStateRequest {
                nodes: Some(NodeSet {
                    nodes: vec![switch_power_node("sw-01", "rack-01", mockup.ports()[0])],
                }),
                operation: op as i32,
            })
            .await
            .unwrap()
            .into_inner();

        let response = batch_response(&resp.response);
        assert_eq!(
            response.status,
            ReturnCode::Success as i32,
            "switch power op {op:?} failed: {}",
            response.message
        );
        assert_batch_stats(response, 1, 1, 0);
    }

    server.stop();
}

#[tokio::test]
async fn switch_batch_get_power_state_uses_bmc_redfish() -> Result<(), Box<dyn std::error::Error>> {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_get_power_state(BatchGetPowerStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_power_node("sw-01", "rack-01", mockup.ports()[0])],
            }),
        })
        .await?
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(
        response.status,
        ReturnCode::Success as i32,
        "switch power state failed: {}",
        response.message
    );
    assert_batch_stats(response, 1, 1, 0);
    assert_eq!(resp.node_power_states.len(), 1);
    assert_eq!(resp.node_power_states[0].node_id, "sw-01");
    assert_eq!(resp.node_power_states[0].pstate, "ON");

    server.stop();
    Ok(())
}

// ── Powershelf firmware inventory ──

#[tokio::test]
async fn powershelf_get_node_firmware_inventory() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![powershelf_node("ps-01", "rack-01", mockup.ports()[1])],
    )
    .await;

    let resp = client
        .get_node_firmware_inventory(GetNodeFirmwareInventoryRequest {
            node_id: "ps-01".into(),
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Success as i32);
    assert!(
        !resp.firmware_list.is_empty(),
        "expected firmware from PMC mockup"
    );

    server.stop();
}

// ── Powershelf firmware upload ──

#[tokio::test]
async fn powershelf_firmware_upload_async() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![powershelf_node("ps-01", "rack-01", mockup.ports()[1])],
    )
    .await;

    let tmp = server.make_synthetic_powershelf_tar("rms_test_ps_fw.tar");

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "ps-01".into(),
            rack_id: "rack-01".into(),
            filename: tmp.to_str().unwrap().into(),
            target: String::new(),
            activate: true,
            force_update: false,
            firmware_targets: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        ReturnCode::Success as i32,
        "async FW update should accept: {}",
        resp.message
    );
    assert!(!resp.job_id.is_empty());

    let job = poll_job_until_terminal(&mut client, &resp.job_id).await;
    assert_eq!(
        job.job_state,
        FirmwareJobState::Completed as i32,
        "job should complete (failure_rate=0)"
    );
    let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
    assert_eq!(result["hardware_config_source"], "code_defaults");
    assert!(result.get("hardware_profile").is_none());
    assert!(result.get("hardware_profile_sha256").is_none());
    assert_eq!(
        result["rms_build"]["git_sha"],
        serde_json::json!(option_env!("VERGEN_GIT_SHA"))
    );

    server.stop();
}

#[tokio::test]
async fn powershelf_firmware_upload_file_not_found() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![powershelf_node("ps-01", "rack-01", mockup.ports()[1])],
    )
    .await;

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "ps-01".into(),
            rack_id: "rack-01".into(),
            filename: "/nonexistent/firmware.bin".into(),
            target: String::new(),
            activate: false,
            force_update: false,
            firmware_targets: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(resp.message.contains("not found"));

    server.stop();
}

#[tokio::test]
async fn powershelf_firmware_upload_empty_filename() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![powershelf_node("ps-01", "rack-01", mockup.ports()[1])],
    )
    .await;

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "ps-01".into(),
            rack_id: "rack-01".into(),
            filename: String::new(),
            target: String::new(),
            activate: false,
            force_update: false,
            firmware_targets: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);

    server.stop();
}

// ── Compute firmware upload ──

#[tokio::test]
async fn compute_firmware_upload_async() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;

    let tmp = server.make_synthetic_compute_fwpkg("rms_test_compute_fw.fwpkg");

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
            filename: tmp.to_str().unwrap().into(),
            target: "BMC".into(),
            activate: false,
            force_update: false,
            firmware_targets: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        ReturnCode::Success as i32,
        "async FW update should accept: {}",
        resp.message
    );
    assert!(!resp.job_id.is_empty());

    let job = poll_job_until_terminal(&mut client, &resp.job_id).await;
    assert_eq!(
        job.job_state,
        FirmwareJobState::Completed as i32,
        "job should complete (failure_rate=0)"
    );
    let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
    let profile =
        rackmanagementservice::config::hardware_profiles::HardwareUpdatePlan::from_yaml_str(
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../hardware-profiles/v1/compute_gb200_nvidia.yaml"
            )),
        )
        .unwrap();
    assert_eq!(result["hardware_config_source"], "profile");
    assert_eq!(result["hardware_profile_sha256"], profile.source_sha256());
    assert_eq!(result["hardware_profile_schema_version"], 1);
    assert_eq!(result["hardware_profile"], profile.profile_id());
    assert_eq!(
        result["rms_build"]["git_sha"],
        serde_json::json!(option_env!("VERGEN_GIT_SHA"))
    );

    server.stop();
}

#[tokio::test]
async fn compute_firmware_upload_file_not_found() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
            filename: "/nonexistent/firmware.bin".into(),
            target: String::new(),
            activate: false,
            force_update: false,
            firmware_targets: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(resp.message.contains("not found"));

    server.stop();
}

#[tokio::test]
async fn compute_inband_firmware_requires_activation() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;
    let image = server.make_synthetic_compute_fwpkg("cx7-test.bin");

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
            filename: String::new(),
            target: String::new(),
            activate: false,
            force_update: false,
            firmware_targets: vec![FirmwareTarget {
                target: "CX7".to_owned(),
                filename: image.to_string_lossy().into_owned(),
            }],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(resp.message.contains("require activation"));
    assert!(resp.job_id.is_empty());

    server.stop();
}

#[tokio::test]
async fn compute_firmware_rejects_mixed_inband_and_oob_targets() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;
    let image = server.make_synthetic_compute_fwpkg("mixed-fw.bin");
    let filename = image.to_string_lossy().into_owned();

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
            filename: String::new(),
            target: String::new(),
            activate: true,
            force_update: false,
            firmware_targets: vec![
                FirmwareTarget {
                    target: "CX7".to_owned(),
                    filename: filename.clone(),
                },
                FirmwareTarget {
                    target: "BMC".to_owned(),
                    filename,
                },
            ],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(resp.message.contains("cannot be mixed"));
    assert!(resp.job_id.is_empty());

    server.stop();
}

#[tokio::test]
async fn compute_firmware_upload_guaranteed_failure() {
    // Dedicated simulator instance with 100% failure rate, isolated from other tests.
    let fail_mockup = RedfishSimulator::builder()
        .with_task_delay(0.01)
        .always_fail_firmware_updates()
        .add_gb200_compute(0)
        .start()
        .await;
    let fail_port = fail_mockup.ports()[0];

    let (mut server, mut client) = start_server().await;

    let node = mk_node_info(
        "c-fail",
        "rack-fail",
        COMPUTE_HOST,
        "aa:bb:cc:dd:ee:03",
        u32::from(fail_port),
        Some("admin"),
        Some("password"),
        NodeType::ComputeGb200Nvidia,
        vec![],
        vec![],
    );
    add_nodes(&mut client, vec![node]).await;

    let tmp = server.make_synthetic_compute_fwpkg("rms_test_compute_fw_fail.fwpkg");

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "c-fail".into(),
            rack_id: "rack-fail".into(),
            filename: tmp.to_str().unwrap().into(),
            target: String::new(),
            activate: false,
            force_update: false,
            firmware_targets: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Success as i32);

    let job = poll_job_until_terminal(&mut client, &resp.job_id).await;
    assert_eq!(
        job.job_state,
        FirmwareJobState::Failed as i32,
        "job should fail (failure_rate=1.0)"
    );

    fail_mockup.stop();
    server.stop();
}

// ── Mixed rack: compute + powershelf ──

#[tokio::test]
async fn mixed_rack_inventory_and_power() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![
            compute_node("c-01", "rack-01", mockup.ports()[0]),
            powershelf_node("ps-01", "rack-01", mockup.ports()[1]),
        ],
    )
    .await;

    let inventory = client
        .list_node_inventory(ListNodeInventoryRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(inventory.nodes.len(), 2);

    let rack_fw = client
        .get_rack_firmware_inventory(GetRackFirmwareInventoryRequest {
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rack_fw.status, ReturnCode::Success as i32);
    assert_eq!(rack_fw.nodes.len(), 2);
    for node_fw in &rack_fw.nodes {
        assert!(!node_fw.firmware_list.is_empty());
    }

    for (node_id, expected_state) in [("c-01", "ON"), ("ps-01", "ON")] {
        let resp = client
            .get_power_state(GetPowerStateRequest {
                node_id: node_id.into(),
                rack_id: "rack-01".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.pstate, expected_state, "power state for {node_id}");
    }

    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  GetNodeDeviceInfo / ListNodeDeviceInfoByNodeType — compute tray info
// ══════════════════════════════════════════════════════════════════════════════

// MNNVLinkTopology in the mockup under
// `.../Processors/GPU_0/index.json` contains:
//   ChassisSerialNumber = "1784124020070"
//   TraySlotNumber      = 69
//   TraySlotIndex       = 103
// These are the values get_mnnvlink_topology should surface.
const COMPUTE_CHASSIS_SN: i64 = 1_784_124_020_070;
const COMPUTE_SLOT_NUMBER: u32 = 69;
const COMPUTE_TRAY_INDEX: u32 = 103;

#[tokio::test]
async fn get_node_device_info_populates_compute_tray_fields() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;

    let resp = client
        .get_node_device_info(GetNodeDeviceInfoRequest {
            rack_id: "rack-01".into(),
            node_id: "c-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        ReturnCode::Success as i32,
        "expected success, got {}: {}",
        resp.status,
        resp.message,
    );
    let info = resp
        .device_info
        .expect("device_info must be populated for compute");
    assert_eq!(info.node_id, "c-01");
    assert_eq!(info.chassis_sn, Some(COMPUTE_CHASSIS_SN));
    assert_eq!(info.slot_number, Some(COMPUTE_SLOT_NUMBER));
    assert_eq!(info.tray_index, Some(COMPUTE_TRAY_INDEX));

    server.stop();
}

#[tokio::test]
async fn get_node_device_info_populates_vrnvl72_compute_tray_fields() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![vrnvl72_compute_node(
            "vr-c-01",
            "vr-rack-01",
            mockup.ports()[0],
        )],
    )
    .await;

    let resp = client
        .get_node_device_info(GetNodeDeviceInfoRequest {
            rack_id: "vr-rack-01".into(),
            node_id: "vr-c-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        ReturnCode::Success as i32,
        "expected success, got {}: {}",
        resp.status,
        resp.message,
    );
    let info = resp
        .device_info
        .expect("device_info must be populated for VRNVL72 compute");
    assert_eq!(info.node_id, "vr-c-01");
    assert_eq!(info.chassis_sn, Some(COMPUTE_CHASSIS_SN));
    assert_eq!(info.slot_number, Some(COMPUTE_SLOT_NUMBER));
    assert_eq!(info.tray_index, Some(COMPUTE_TRAY_INDEX));

    server.stop();
}

#[tokio::test]
async fn list_node_device_info_by_node_type_populates_compute_tray_fields() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![
            compute_node("c-01", "rack-01", mockup.ports()[0]),
            compute_node("c-02", "rack-01", mockup.ports()[0]),
        ],
    )
    .await;

    let resp = client
        .list_node_device_info_by_node_type(ListNodeDeviceInfoByNodeTypeRequest {
            rack_id: "rack-01".into(),
            node_type: NodeType::ComputeGb200Nvidia as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        ReturnCode::Success as i32,
        "expected success, got {}: {}",
        resp.status,
        resp.message,
    );
    assert_eq!(resp.node_device_details.len(), 2);
    assert_stats(&resp.stats, 2, 2, 0);

    let mut ids: Vec<&str> = resp
        .node_device_details
        .iter()
        .map(|n| n.node_id.as_str())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["c-01", "c-02"]);

    for info in &resp.node_device_details {
        assert_eq!(info.chassis_sn, Some(COMPUTE_CHASSIS_SN));
        assert_eq!(info.slot_number, Some(COMPUTE_SLOT_NUMBER));
        assert_eq!(info.tray_index, Some(COMPUTE_TRAY_INDEX));
    }

    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  BatchUpdateFirmware
// ══════════════════════════════════════════════════════════════════════════════

fn firmware_targets(
    node_type: NodeType,
    pairs: &[(&str, &str)],
) -> HashMap<i32, FirmwareTargetList> {
    HashMap::from([(
        node_type as i32,
        FirmwareTargetList {
            targets: pairs
                .iter()
                .map(|(tgt, file)| FirmwareTarget {
                    target: (*tgt).to_owned(),
                    filename: (*file).to_owned(),
                })
                .collect(),
        },
    )])
}

#[tokio::test]
async fn batch_update_firmware_rejects_empty_nodes() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_update_firmware(BatchUpdateFirmwareRequest {
            nodes: Some(NodeSet { nodes: vec![] }),
            firmware_targets: HashMap::new(),
            activate: false,
            force_update: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(
        response.message.to_lowercase().contains("no nodes"),
        "expected 'no nodes' in error, got: {}",
        response.message
    );
    assert_batch_stats(response, 0, 0, 0);
    assert!(response.job_id.is_empty());
    assert!(resp.jobs.is_empty());

    server.stop();
}

#[tokio::test]
async fn batch_update_firmware_rejects_empty_targets() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let device = compute_node("c-01", "rack-01", mockup.ports()[0]);
    let resp = client
        .batch_update_firmware(BatchUpdateFirmwareRequest {
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            firmware_targets: HashMap::new(),
            activate: false,
            force_update: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(
        response
            .message
            .to_lowercase()
            .contains("no firmware targets"),
        "expected 'no firmware targets' in error, got: {}",
        response.message
    );
    assert!(response.job_id.is_empty());

    server.stop();
}

#[tokio::test]
async fn batch_update_firmware_creates_jobs_for_compute_nodes() {
    let mockup = RedfishSimulator::builder()
        .add_gb200_compute(0)
        .add_gb200_compute(0)
        .start()
        .await;
    let (mut server, mut client) = start_server().await;

    // Synthetic PLDM package fixture that both devices reference. NVFWUPD
    // parses the package before upload, so this guards more than job creation.
    let tmp = server.make_synthetic_compute_fwpkg("rms_test_device_list_fw.fwpkg");

    let devices = vec![
        compute_node("c-01", "rack-01", mockup.ports()[0]),
        compute_node("c-02", "rack-01", mockup.ports()[1]),
    ];

    let targets = firmware_targets(
        NodeType::ComputeGb200Nvidia,
        &[("HMC", tmp.to_str().unwrap())],
    );

    let resp = client
        .batch_update_firmware(BatchUpdateFirmwareRequest {
            nodes: Some(NodeSet { nodes: devices }),
            firmware_targets: targets,
            activate: false,
            force_update: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(
        response.status,
        ReturnCode::Success as i32,
        "expected success, got: {}",
        response.message,
    );
    assert_batch_stats(response, 2, 2, 0);
    assert!(!response.job_id.is_empty(), "parent job_id must be set");
    assert_eq!(resp.jobs.len(), 2);

    let mut node_ids: Vec<&str> = resp.jobs.iter().map(|j| j.node_id.as_str()).collect();
    node_ids.sort();
    assert_eq!(node_ids, vec!["c-01", "c-02"]);
    for nj in &resp.jobs {
        assert!(!nj.job_id.is_empty());
    }

    let parent_job = poll_job_until_terminal(&mut client, &response.job_id).await;
    assert_eq!(
        parent_job.job_state,
        FirmwareJobState::Completed as i32,
        "device-list parent job should complete successfully: {}",
        parent_job.error_message
    );

    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  Firmware objects
// ══════════════════════════════════════════════════════════════════════════════

fn firmware_object_release_catalog_json(
    product_name: &str,
    board_skus: serde_json::Value,
) -> String {
    serde_json::json!({
        "ProductName": product_name,
        "Milestones": [{
            "Name": "test",
            "BoardSKUs": board_skus,
        }]
    })
    .to_string()
}

fn apply_firmware_object_http_config_json(artifact_url: &str) -> String {
    firmware_object_release_catalog_json(
        "e2e-http-fw",
        serde_json::json!([{
            "SKUID": "699-24764-0001-TS3",
            "Name": "GB200-Compute",
            "Type": "Compute Tray",
            "Components": {
                "Firmware": [{
                    "Component": "BMC",
                    "Type": "Prod",
                    "Locations": [{
                        "Name": "Production_Package",
                        "Location": artifact_url,
                        "LocationType": "HTTPS",
                        "Type": "Firmware"
                    }]
                }]
            }
        }]),
    )
}

#[tokio::test]
async fn firmware_object_prefetch_accepts_placeholders_and_multiple_nvos_images() {
    let (mut server, mut client) = start_server().await;
    let source = server.make_synthetic_compute_fwpkg("catalog.fwpkg");
    let config_json = firmware_object_release_catalog_json(
        "e2e-prefetch",
        serde_json::json!([
            {"Type": "Compute Tray", "Components": {"Firmware": [{
                "Component": "BMC", "Type": "Prod", "Locations": [
                    {"Location": source, "LocationType": "file", "Type": "Firmware"},
                    {"FileName": "placeholder.fwpkg", "Location": null, "Type": "Firmware"}
                ]
            }]}},
            {"Type": "Switch Tray", "Components": {"Software": [{
                "Component": "NVOS", "Type": "Prod", "Locations": [
                    {"FileName": "notes.bin", "Location": null, "Required": false},
                    {"FileName": "nvos-amd64-25.02.4440.bin", "Location": source, "LocationType": "file"},
                    {"FileName": "nvos-amd64-25.02.4441.bin", "Location": source, "LocationType": "file"}
                ]
            }]}}
        ]),
    );
    let object = client
        .add_firmware_object(AddFirmwareObjectRequest {
            config_json,
            hardware_type: "gb200".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .object
        .unwrap();
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = client
                .get_firmware_object(GetFirmwareObjectRequest {
                    id: object.id.clone(),
                })
                .await
                .unwrap()
                .into_inner()
                .object
                .unwrap();
            if current.available {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("metadata placeholders and NVOS ambiguity must not fail prefetch");
    let metadata = ready.metadata.unwrap();
    assert_eq!(metadata.switch_system_images.len(), 3);
    assert!(!metadata.switch_system_images[0].required);
    server.stop();
}

#[tokio::test]
async fn firmware_object_metadata_recognizes_liteon_hyphen_and_space_names() {
    let (mut server, mut client) = start_server().await;
    // Catalog/metadata test only: these synthetic bytes are never sent to hardware.
    let source = server.make_synthetic_compute_fwpkg("powershelf-catalog.fwpkg");
    for (index, separator) in [" ", "-"].into_iter().enumerate() {
        let firmware: Vec<_> = ["PMC", "PSU"].into_iter().map(|part| serde_json::json!({
            "Component": format!("LiteOn{separator}{part}"), "Version": "1.0", "Type": "Prod",
            "Locations": [{"Location": source, "LocationType": "file", "Type": "Firmware",
                "FileName": format!("liteon-{part}.fwpkg")}]
        })).collect();
        let object = client.add_firmware_object(AddFirmwareObjectRequest {
            config_json: firmware_object_release_catalog_json(&format!("liteon-alias-{index}"),
                serde_json::json!([{"Type": "Power Shelf", "Components": {"Firmware": firmware}}])),
            hardware_type: "gb200".to_owned(),
            ..Default::default()
        }).await.unwrap().into_inner().object.unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let current = client
                    .get_firmware_object(GetFirmwareObjectRequest {
                        id: object.id.clone(),
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .object
                    .unwrap();
                if current.available {
                    break current;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("LiteOn catalog preparation must complete");
        let devices = ready.metadata.unwrap().device_components;
        assert_eq!(
            devices.len(),
            3,
            "GB200 and GB300 LiteOn plus the VR LiteOn shelf, no Delta or compute platforms"
        );
        for device in devices {
            assert_eq!(device.device_type, "Power Shelf");
            let names = device
                .components
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(names, ["LiteOnPMC", "LiteOnPSU"]);
            let parts = ["PMC", "PSU"];
            for (component, part) in device.components.iter().zip(parts) {
                assert_eq!(
                    component.source_component,
                    format!("LiteOn{separator}{part}")
                );
                assert_eq!(component.artifacts.len(), 1);
            }
        }
    }
    server.stop();
}

#[tokio::test]
async fn apply_firmware_object_rejects_missing_channel_before_download() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let artifacts = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"must not download"))
        .expect(0)
        .mount(&artifacts)
        .await;
    let config_json = firmware_object_release_catalog_json(
        "missing-channel",
        serde_json::json!([{
            "Type": "Compute Tray",
            "Components": {"Firmware": [
                {"Component": "BMC", "Type": "Prod", "Locations": [{
                    "Location": format!("{}/bmc.fwpkg", artifacts.uri()),
                    "LocationType": "HTTPS", "Type": "Firmware"
                }]},
                {"Component": "HMC", "Locations": [{
                    "Location": format!("{}/hmc.fwpkg", artifacts.uri()),
                    "LocationType": "HTTPS", "Type": "Firmware"
                }]}
            ]}
        }]),
    );
    let (mut server, mut client) = start_server().await;
    let response = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".into(),
            config_json,
            firmware_type: String::new(),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet {
                nodes: vec![compute_node("c-01", "rack-01", 1)],
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.jobs.len(), 1);
    let child = poll_job_until_terminal_within(
        &mut client,
        &response.jobs[0].job_id,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(child.job_state, FirmwareJobState::Failed as i32);
    assert!(
        child.error_message.contains("Firmware[1].Type"),
        "{}",
        child.error_message
    );
    assert!(
        child.error_message.contains("HMC"),
        "{}",
        child.error_message
    );
    artifacts.verify().await;
    server.stop();
}

#[tokio::test]
async fn apply_firmware_object_defaults_type_and_downloads_without_access_token() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let artifact_path = "/compute-bmc.fwpkg";
    let temp = TempDir::new().expect("create temp dir for fwpkg fixture");
    let fwpkg_path = temp.path().join("compute-bmc.fwpkg");
    write_synthetic_compute_fwpkg(&fwpkg_path);
    let fwpkg_bytes = std::fs::read(&fwpkg_path).expect("read synthetic compute fwpkg");

    let http_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(artifact_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(fwpkg_bytes.clone()))
        .mount(&http_server)
        .await;

    let artifact_url = format!("{}{}", http_server.uri(), artifact_path);
    let config_json = apply_firmware_object_http_config_json(&artifact_url);

    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let device = compute_node("c-01", "rack-01", mockup.ports()[0]);
    add_nodes(&mut client, vec![device.clone()]).await;

    let resp = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".into(),
            config_json,
            access_token: None,
            firmware_type: String::new(),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            force_update: false,
            component_filters: HashMap::from([(
                NodeType::ComputeGb200Nvidia as i32,
                FirmwareObjectComponentFilter {
                    components: vec!["BMC".into()],
                },
            )]),
            ..Default::default()
        })
        .await
        .expect("apply_firmware_object RPC should succeed")
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(
        response.status,
        ReturnCode::Success as i32,
        "apply_firmware_object should accept missing access_token: {}",
        response.message
    );
    assert_batch_stats(response, 1, 1, 0);
    assert!(!response.job_id.is_empty(), "parent job_id must be set");
    assert_eq!(resp.jobs.len(), 1);
    assert!(!resp.jobs[0].job_id.is_empty());

    // ApplyFirmwareObject enables activation and the post-activation version
    // check waits before reading inventory in the integration-test build.
    let parent_job = poll_job_until_terminal_within(
        &mut client,
        &response.job_id,
        GB200_FIRMWARE_OBJECT_APPLY_TIMEOUT,
    )
    .await;
    assert_eq!(
        parent_job.job_state,
        FirmwareJobState::Completed as i32,
        "apply_firmware_object parent job should complete: {}",
        parent_job.error_message
    );
    assert!(
        !parent_job
            .error_message
            .to_lowercase()
            .contains("access_token"),
        "job should not fail due to missing access_token: {}",
        parent_job.error_message
    );

    let requests = http_server
        .received_requests()
        .await
        .expect("wiremock should record artifact download requests");
    assert!(
        requests
            .iter()
            .any(|request| request.method.to_string() == "GET"),
        "artifact download should issue GET to wiremock"
    );
    for request in &requests {
        for name in request.headers.keys() {
            assert!(
                !name.as_str().eq_ignore_ascii_case("x-jfrog-art-api"),
                "HTTP artifact download must not send Artifactory auth header"
            );
        }
    }

    server.stop();
}

#[tokio::test]
async fn apply_firmware_object_retries_a_transient_artifact_failure() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let artifact_path = "/compute-bmc.fwpkg";
    let temp = TempDir::new().expect("create temp dir for fwpkg fixture");
    let fwpkg_path = temp.path().join("compute-bmc.fwpkg");
    write_synthetic_compute_fwpkg(&fwpkg_path);
    let fwpkg_bytes = std::fs::read(&fwpkg_path).expect("read synthetic compute fwpkg");

    let http_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(artifact_path))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .expect(1)
        .mount(&http_server)
        .await;
    Mock::given(method("GET"))
        .and(path(artifact_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(fwpkg_bytes))
        .expect(1)
        .mount(&http_server)
        .await;

    let artifact_url = format!("{}{}", http_server.uri(), artifact_path);
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let device = compute_node("c-01", "rack-01", mockup.ports()[0]);
    add_nodes(&mut client, vec![device.clone()]).await;

    let resp = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".into(),
            config_json: apply_firmware_object_http_config_json(&artifact_url),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            component_filters: HashMap::from([(
                NodeType::ComputeGb200Nvidia as i32,
                FirmwareObjectComponentFilter {
                    components: vec!["BMC".into()],
                },
            )]),
            ..Default::default()
        })
        .await
        .expect("apply_firmware_object RPC should succeed")
        .into_inner();
    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);

    let parent_job = poll_job_until_terminal_within(
        &mut client,
        &response.job_id,
        GB200_FIRMWARE_OBJECT_APPLY_TIMEOUT,
    )
    .await;
    assert_eq!(
        parent_job.job_state,
        FirmwareJobState::Completed as i32,
        "one 503 must not fail the apply: {}",
        parent_job.error_message
    );
    http_server.verify().await;
    server.stop();
}

#[tokio::test]
async fn apply_firmware_object_stops_before_the_device_when_downloads_keep_failing() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Three attempts, then the job fails during artifact preparation.
    let artifacts = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .expect(3)
        .mount(&artifacts)
        .await;
    // Any connection to this port is a device contact, whatever the protocol.
    let bmc = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind BMC listener");
    let bmc_port = bmc.local_addr().expect("BMC address").port();

    let artifact_url = format!("{}/compute-bmc.fwpkg", artifacts.uri());
    let (mut server, mut client) = start_server().await;
    let response = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".into(),
            config_json: apply_firmware_object_http_config_json(&artifact_url),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet {
                nodes: vec![compute_node("c-01", "rack-01", bmc_port)],
            }),
            component_filters: HashMap::from([(
                NodeType::ComputeGb200Nvidia as i32,
                FirmwareObjectComponentFilter {
                    components: vec!["BMC".into()],
                },
            )]),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.jobs.len(), 1);

    let child = poll_job_until_terminal_within(
        &mut client,
        &response.jobs[0].job_id,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(child.job_state, FirmwareJobState::Failed as i32);
    assert_eq!(child.error_code, FirmwareUpdateError::ServerError as i32);
    artifacts.verify().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), bmc.accept())
            .await
            .is_err(),
        "a failed preparation must not reach the device"
    );
    server.stop();
}

fn apply_firmware_object_artifactory_config_json(artifact_url: &str) -> String {
    firmware_object_release_catalog_json(
        "e2e-artifactory-fw",
        serde_json::json!([{
            "SKUID": "699-24764-0001-TS3",
            "Name": "GB200-Compute",
            "Type": "Compute Tray",
            "Components": {
                "Firmware": [{
                    "Component": "BMC",
                    "Type": "Prod",
                    "Locations": [{
                        "Name": "Production_Package",
                        "Location": artifact_url,
                        "LocationType": "artifactory",
                        "Type": "Firmware"
                    }]
                }]
            }
        }]),
    )
}

fn apply_firmware_object_file_config_json(artifact_path: &str) -> String {
    firmware_object_release_catalog_json(
        "e2e-file-fw",
        serde_json::json!([{
            "SKUID": "699-24764-0001-TS3",
            "Name": "GB200-Compute",
            "Type": "Compute Tray",
            "Components": {
                "Firmware": [{
                    "Component": "BMC",
                    "Type": "Prod",
                    "Locations": [{
                        "Name": "Production_Package",
                        "Location": artifact_path,
                        "LocationType": "file",
                        "Type": "Firmware"
                    }]
                }]
            }
        }]),
    )
}

fn apply_switch_system_image_http_config_json(artifact_url: &str) -> String {
    firmware_object_release_catalog_json(
        "e2e-switch-http",
        serde_json::json!([{
            "Name": "Sample Switch",
            "Type": "Switch Tray",
            "Components": {
                "Software": [{
                    "Component": "NVOS",
                    "Version": "25.02.4440",
                    "Type": "prod",
                    "Locations": [{
                        "Location": artifact_url,
                        "LocationType": "http",
                        "PackageName": "GB200NVL72_NVOS",
                    }]
                }]
            }
        }]),
    )
}

#[tokio::test]
async fn apply_firmware_object_rejects_authenticated_http_without_transmitting_credentials() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let artifact_path = "/compute-bmc.fwpkg";
    let http_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(artifact_path))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&http_server)
        .await;

    let artifact_url = format!("{}{}", http_server.uri(), artifact_path);
    let config_json = apply_firmware_object_artifactory_config_json(&artifact_url);

    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let device = compute_node("c-01", "rack-01", mockup.ports()[0]);
    add_nodes(&mut client, vec![device.clone()]).await;

    let resp = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".into(),
            config_json,
            access_token: Some("secret-token".into()),
            firmware_type: "prod".into(),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            force_update: false,
            component_filters: HashMap::from([(
                NodeType::ComputeGb200Nvidia as i32,
                FirmwareObjectComponentFilter {
                    components: vec!["BMC".into()],
                },
            )]),
            ..Default::default()
        })
        .await
        .expect("apply_firmware_object RPC should succeed")
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);

    let parent_job = poll_job_until_terminal_within(
        &mut client,
        &response.job_id,
        GB200_FIRMWARE_OBJECT_APPLY_TIMEOUT,
    )
    .await;
    assert_eq!(
        parent_job.job_state,
        FirmwareJobState::Failed as i32,
        "authenticated HTTP apply must fail: {}",
        parent_job.error_message
    );

    assert!(http_server.received_requests().await.unwrap().is_empty());
    let child =
        poll_job_until_terminal_within(&mut client, &resp.jobs[0].job_id, Duration::from_secs(10))
            .await;
    assert!(child.error_message.contains("require HTTPS"));
    assert_eq!(child.error_code, FirmwareUpdateError::ClientFailure as i32);
    let generic = client
        .get_job_status(GetJobStatusRequest {
            job_id: child.job_id,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        generic.job_states[0].error_code,
        rack_manager::JobError::InvalidArgument as i32
    );
    server.stop();
}

#[tokio::test]
async fn signed_artifact_redirect_failure_redacts_job_errors_and_logs() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use wiremock::matchers::path;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TEST_NAME: &str = "signed_artifact_redirect_failure_redacts_job_errors_and_logs";

    // Isolate tracing's process-wide callsite cache from parallel gRPC tests.
    if std::env::var("RMS_ISOLATED_TRACING_TEST").as_deref() != Ok(TEST_NAME) {
        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env("RMS_ISOLATED_TRACING_TEST", TEST_NAME)
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(
            output.status.success() && stdout.contains("test result: ok. 1 passed; 0 failed;"),
            "{stdout}\n{stderr}"
        );

        return;
    }

    let logs = tempfile::NamedTempFile::new().unwrap();

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter("rackmanagementservice=debug")
        .with_ansi(false)
        .without_time()
        .with_writer(Arc::new(logs.reopen().unwrap()))
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

    let signed_url = format!(
        "http://{}/fw.fwpkg?token=SECRET&X-Amz-Signature=SIGNATURE",
        listener.local_addr().unwrap()
    );

    let broken_server = tokio::spawn(async move {
        // Retry an interrupted body, then stop on malformed HTTP. Both URLs
        // and the retained first failure must stay redacted in status and logs.
        for response in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\npartial"
                .as_slice(),
            b"HTTP/1.1 invalid\r\n\r\n".as_slice(),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();

            while !request.ends_with(b"\r\n\r\n") {
                assert_ne!(socket.read_buf(&mut request).await.unwrap(), 0);
            }

            socket.write_all(response).await.unwrap();
        }
    });

    let redirect = MockServer::start().await;
    Mock::given(path("/fw.fwpkg"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", signed_url.as_str()))
        .expect(2)
        .mount(&redirect)
        .await;

    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let node = compute_node("c-01", "rack-01", mockup.ports()[0]);

    add_nodes(&mut client, vec![node.clone()]).await;

    let response = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".to_owned(),
            config_json: apply_firmware_object_artifactory_config_json(&format!(
                "{}/fw.fwpkg?token=INITIAL_SECRET",
                redirect.uri()
            )),
            firmware_type: "prod".to_owned(),
            hardware_type: "gb200".to_owned(),
            nodes: Some(NodeSet { nodes: vec![node] }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let status =
        poll_generic_job_until_terminal(&mut client, &batch_response(&response.response).job_id)
            .await;

    broken_server.await.unwrap();

    assert_eq!(
        batch_response(&response.response).status,
        ReturnCode::Success as i32
    );

    assert!(
        status.job_states.len() >= 2,
        "parent and child must be returned"
    );

    for job in &status.job_states {
        assert_eq!(job.execution_state, JobExecutionState::Failed as i32);

        for secret in ["INITIAL_SECRET", "SECRET", "SIGNATURE"] {
            assert!(!job.error_message.contains(secret), "{}", job.error_message);
        }
    }

    let child = status
        .job_states
        .iter()
        .find(|job| job.job_id == response.jobs[0].job_id)
        .expect("GetJobStatus must include the firmware child");

    assert_eq!(child.error_code, rack_manager::JobError::ClientError as i32);

    assert!(
        child
            .error_message
            .contains("artifact request failed for http://")
    );

    assert!(child.error_message.contains("artifact transport failed"));
    assert!(
        child
            .error_message
            .contains("after 2 attempts (first failure:")
    );
    assert!(
        child
            .error_message
            .contains("failed to read artifact response body")
    );

    let output = std::fs::read_to_string(logs.path()).unwrap();

    assert!(output.contains("WARN"), "{output}");
    assert!(output.contains("retrying"), "{output}");
    assert!(output.contains("ERROR"), "{output}");
    assert!(output.contains("required artifact unavailable"), "{output}");
    assert!(output.contains("artifact transport failed"), "{output}");

    for secret in ["INITIAL_SECRET", "SECRET", "SIGNATURE"] {
        assert!(!output.contains(secret), "{output}");
    }

    server.stop();
}

#[tokio::test]
async fn apply_firmware_object_downloads_local_file_artifact() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let device = compute_node("c-01", "rack-01", mockup.ports()[0]);
    add_nodes(&mut client, vec![device.clone()]).await;

    let fwpkg_path = server.make_synthetic_compute_fwpkg("compute-bmc.fwpkg");
    let config_json = apply_firmware_object_file_config_json(fwpkg_path.to_str().unwrap());

    let resp = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".into(),
            config_json,
            access_token: None,
            firmware_type: "prod".into(),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            force_update: false,
            component_filters: HashMap::from([(
                NodeType::ComputeGb200Nvidia as i32,
                FirmwareObjectComponentFilter {
                    components: vec!["BMC".into()],
                },
            )]),
            ..Default::default()
        })
        .await
        .expect("apply_firmware_object RPC should succeed")
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);

    let parent_job = poll_job_until_terminal_within(
        &mut client,
        &response.job_id,
        GB200_FIRMWARE_OBJECT_APPLY_TIMEOUT,
    )
    .await;
    assert_eq!(
        parent_job.job_state,
        FirmwareJobState::Completed as i32,
        "local file download apply should complete: {}",
        parent_job.error_message
    );

    server.stop();
}

#[tokio::test]
async fn apply_firmware_object_applies_all_component_locations_in_order() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let device = compute_node("c-01", "rack-01", mockup.ports()[0]);
    add_nodes(&mut client, vec![device.clone()]).await;
    let filenames = ["bmc-3.fwpkg", "bmc-1.fwpkg", "bmc-2.fwpkg"];
    let locations: Vec<_> = filenames
        .iter()
        .map(|filename| {
            let path = server.make_synthetic_compute_fwpkg(filename);
            serde_json::json!({"Location": path, "LocationType": "file", "Type": "Firmware"})
        })
        .collect();
    let config_json = firmware_object_release_catalog_json(
        "e2e-all-locations",
        serde_json::json!([{
            "Type": "Compute Tray", "Components": {"Firmware": [{
                "Component": "BMC", "Type": "Prod", "Locations": locations
            }]}
        }]),
    );

    let response = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".to_owned(),
            config_json,
            firmware_type: "prod".to_owned(),
            hardware_type: "gb200".to_owned(),
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            force_update: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        batch_response(&response.response).status,
        ReturnCode::Success as i32
    );
    let node_job = poll_job_until_terminal_within(
        &mut client,
        &response.jobs[0].job_id,
        GB200_FIRMWARE_OBJECT_APPLY_TIMEOUT,
    )
    .await;

    assert_eq!(
        node_job.job_state,
        FirmwareJobState::Completed as i32,
        "{}",
        node_job.error_message
    );
    let result: serde_json::Value = serde_json::from_str(&node_job.result_json).unwrap();
    assert_eq!(result["targets_completed"], 3);
    assert_eq!(result["activated"], true);
    let targets = result["targets"].as_array().unwrap();
    assert_eq!(targets.len(), filenames.len());
    for (target, filename) in targets.iter().zip(filenames) {
        assert!(
            target["firmware_file"]
                .as_str()
                .unwrap()
                .ends_with(filename)
        );
        assert_eq!(target["status"], "completed");
    }
    server.stop();
}

#[tokio::test]
async fn apply_firmware_object_download_failure_marks_job_failed() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let artifact_path = "/missing.fwpkg";
    let http_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(artifact_path))
        .respond_with(ResponseTemplate::new(404))
        .mount(&http_server)
        .await;

    let artifact_url = format!("{}{}", http_server.uri(), artifact_path);
    let config_json = apply_firmware_object_http_config_json(&artifact_url);

    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    let device = compute_node("c-01", "rack-01", mockup.ports()[0]);
    add_nodes(&mut client, vec![device.clone()]).await;

    let resp = client
        .apply_firmware_object(ApplyFirmwareObjectRequest {
            rack_id: "rack-01".into(),
            config_json,
            access_token: None,
            firmware_type: "prod".into(),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            force_update: false,
            component_filters: HashMap::from([(
                NodeType::ComputeGb200Nvidia as i32,
                FirmwareObjectComponentFilter {
                    components: vec!["BMC".into()],
                },
            )]),
            ..Default::default()
        })
        .await
        .expect("apply_firmware_object RPC should succeed")
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);

    let child_job_id = resp.jobs[0].job_id.clone();
    let child_job =
        poll_job_until_terminal_within(&mut client, &child_job_id, Duration::from_secs(30)).await;
    assert_eq!(
        child_job.job_state,
        FirmwareJobState::Failed as i32,
        "missing artifact should fail the child job"
    );
    assert_eq!(
        child_job.error_code,
        FirmwareUpdateError::FileNotFound as i32
    );
    assert!(
        child_job.error_message.to_lowercase().contains("download")
            || child_job.error_message.contains("404")
            || child_job.error_message.contains("failed"),
        "child job error should mention download failure: {}",
        child_job.error_message
    );

    server.stop();
}

#[tokio::test]
async fn apply_switch_system_image_defaults_type_downloads_and_propagates_recovery_flag() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let artifact_path = "/nvos-amd64-25.02.4440.bin";
    let http_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(artifact_path))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"synthetic switch image"))
        .mount(&http_server)
        .await;

    let artifact_url = format!("{}{}", http_server.uri(), artifact_path);
    let config_json = apply_switch_system_image_http_config_json(&artifact_url);

    let (mut server, mut client) = start_server().await;

    let mut device = with_onie_recovery_endpoint(
        switch_device("sw-01", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:11"),
        "127.0.0.1",
    );

    device.host_endpoint.as_mut().unwrap().port = 1;
    let devices = vec![device];

    let resp = client
        .apply_switch_system_image(ApplySwitchSystemImageRequest {
            rack_id: "rack-01".into(),
            config_json,
            access_token: None,
            software_type: String::new(),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet { nodes: devices }),
            allow_onie_recovery: true,
        })
        .await
        .expect("apply_switch_system_image RPC should succeed")
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);
    assert_eq!(resp.jobs.len(), 1);
    let child_job_id = resp.jobs[0].job_id.clone();

    let mut terminal_status = None;

    for _ in 0..150 {
        let status = client
            .get_switch_system_image_job_status(GetSwitchSystemImageJobStatusRequest {
                job_id: child_job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        if status.state == "completed" || status.state == "failed" {
            terminal_status = Some(status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let terminal_status = terminal_status.expect("switch image job should reach terminal state");

    let result: serde_json::Value = serde_json::from_str(&terminal_status.result_json)
        .expect("switch image job result_json should be valid JSON");

    assert_eq!(result["allow_onie_recovery"], true);
    assert_eq!(result["onie_recovery_endpoint_configured"], true);

    let requests = http_server
        .received_requests()
        .await
        .expect("wiremock should record switch image download");
    assert!(
        requests
            .iter()
            .any(|request| request.method.to_string() == "GET"),
        "switch image apply should download artifact over HTTP"
    );

    server.stop();
}

#[tokio::test]
async fn batch_update_firmware_missing_type_reports_per_node_failure() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let tmp = server.make_fake_firmware_file("rms_test_device_list_fw_missing_type.bin");

    // Provide a compute device but only switch-type firmware targets -> the
    // compute device has no firmware targets for its type and should be
    // reported as a skipped/failed device in the response, not kill the RPC.
    let devices = vec![compute_node("c-01", "rack-01", mockup.ports()[0])];

    let targets = firmware_targets(
        NodeType::SwitchGb200Nvidia,
        &[("SW_0", tmp.to_str().unwrap())],
    );

    let resp = client
        .batch_update_firmware(BatchUpdateFirmwareRequest {
            nodes: Some(NodeSet { nodes: devices }),
            firmware_targets: targets,
            activate: false,
            force_update: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    // No child jobs should be created (no matching targets), so the RPC must
    // report FAILURE with a pollable failed parent and a non-empty per-node
    // results list explaining why.
    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert_batch_stats(response, 1, 0, 1);
    assert!(!response.job_id.is_empty());
    let parent_job = poll_generic_job_until_terminal(&mut client, &response.job_id).await;
    assert_eq!(
        parent_job.job_states[0].execution_state,
        JobExecutionState::Failed as i32
    );
    assert!(resp.jobs.is_empty());
    assert_eq!(response.node_results.len(), 1);
    assert_eq!(response.node_results[0].node_id, "c-01");

    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  UpdateSwitchSystemImage / GetSwitchSystemImageJobStatus
// ══════════════════════════════════════════════════════════════════════════════

/// Build a NodeInfo for a switch with BOTH bmc_endpoint and host_endpoint
/// populated. Switches authenticate NVUE/NVOS via host_endpoint credentials
/// so tests that exercise UpdateSwitchSystemImage or
/// BatchUpdateFirmware must provide host creds.
fn switch_device(id: &str, rack_id: &str, ip: &str, mac: &str) -> NodeInfo {
    NodeInfo {
        node_id: id.into(),
        rack_id: rack_id.into(),
        r#type: Some(NodeType::SwitchGb200Nvidia as i32),
        bmc_endpoint: Some(bmc_endpoint(ip, mac, 443)),
        host_endpoint: Some(Endpoint {
            interface: Some(NetworkInterface {
                ip_address: ip.into(),
                mac_address: mac.into(),
                host_name: Some(format!("{id}.switch.example.com")),
            }),
            port: 22,
            credentials: Some(Credentials {
                auth: Some(credentials::Auth::UserPass(UsernamePassword {
                    username: "admin".into(),
                    password: "password".into(),
                })),
            }),
        }),
        ..Default::default()
    }
}

fn with_onie_recovery_endpoint(mut node: NodeInfo, ip_address: &str) -> NodeInfo {
    node.additional_host_endpoints = vec![Endpoint {
        interface: Some(NetworkInterface {
            ip_address: ip_address.to_owned(),
            ..Default::default()
        }),
        port: 22,
        credentials: Some(Credentials {
            auth: Some(credentials::Auth::UserPass(UsernamePassword {
                username: "onie".to_owned(),
                password: "recovery-password".to_owned(),
            })),
        }),
    }];

    node
}

fn switch_password_device(id: &str, rack_id: &str, port: u16) -> NodeInfo {
    NodeInfo {
        node_id: id.into(),
        rack_id: rack_id.into(),
        r#type: Some(NodeType::SwitchGb200Nvidia as i32),
        bmc_endpoint: None,
        host_endpoint: Some(Endpoint {
            interface: Some(NetworkInterface {
                ip_address: "127.0.0.1".into(),
                mac_address: "aa:bb:cc:00:10:01".into(),
                host_name: Some(format!("{id}.switch.example.com")),
            }),
            port: u32::from(port),
            credentials: Some(Credentials {
                auth: Some(credentials::Auth::UserPass(UsernamePassword {
                    username: "admin".into(),
                    password: "current-password".into(),
                })),
            }),
        }),
        ..Default::default()
    }
}

async fn mount_switch_bmc_inventory(server: &wiremock::MockServer, entries: &[(&str, &str)]) {
    use wiremock::matchers::{basic_auth, method, path};
    use wiremock::{Mock, ResponseTemplate};

    let root = "/redfish/v1/UpdateService/FirmwareInventory";
    let members: Vec<_> = entries
        .iter()
        .map(|(id, _)| serde_json::json!({"@odata.id":format!("{root}/{id}")}))
        .collect();
    Mock::given(method("GET"))
        .and(path(root))
        .and(basic_auth("admin", "password"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Members":members})),
        )
        .mount(server)
        .await;
    for (id, version) in entries {
        Mock::given(method("GET"))
            .and(path(format!("{root}/{id}")))
            .and(basic_auth("admin", "password"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Id":id, "Version":version, "Updateable":true
            })))
            .mount(server)
            .await;
    }
}

/// TLS terminator for HTTP-only wiremock servers.
///
/// Production NVUE clients always use HTTPS. This proxy preserves that
/// transport path while forwarding decrypted HTTP to wiremock for request
/// matching and response fixtures.
struct WiremockHttpsProxy {
    port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl WiremockHttpsProxy {
    /// Fronts `upstream` with a throwaway self-signed certificate. Callers
    /// that need `insecure_switch: false` (real certificate verification)
    /// must use [`Self::start_with_certificate`] with a certificate chaining
    /// to a CA the test's `TlsMaterialStore` trusts instead.
    async fn start(upstream: SocketAddr) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
            .expect("generate NVUE test certificate");

        Self::start_with_certificate(upstream, cert.cert, cert.key_pair, None).await
    }

    async fn start_with_certificate(
        upstream: SocketAddr,
        cert: rcgen::Certificate,
        key_pair: rcgen::KeyPair,
        client_ca: Option<&rcgen::Certificate>,
    ) -> Self {
        use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
        use tokio::io::copy_bidirectional;
        use tokio::net::{TcpListener, TcpStream};
        use tokio_rustls::TlsAcceptor;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let cert_der = CertificateDer::from(cert.der().to_vec());
        let key_der = PrivatePkcs8KeyDer::from(key_pair.serialize_der());

        let tls_builder = rustls::ServerConfig::builder();

        let tls_builder = match client_ca {
            Some(client_ca) => {
                let mut roots = rustls::RootCertStore::empty();
                roots
                    .add(CertificateDer::from(client_ca.der().to_vec()))
                    .expect("add NVUE client CA certificate");

                let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .expect("build NVUE client certificate verifier");

                tls_builder.with_client_cert_verifier(verifier)
            }
            None => tls_builder.with_no_client_auth(),
        };

        let mut tls_config = tls_builder
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

                    let _ = copy_bidirectional(&mut downstream, &mut upstream).await;
                });
            }
        });

        Self { port, task }
    }

    fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for WiremockHttpsProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Switch NodeInfo with bmc_endpoint creds but NO host_endpoint, used to
/// exercise the "missing host credentials" rejection in BatchUpdateFirmware
/// and UpdateSwitchSystemImage.
fn switch_device_bmc_only(id: &str, rack_id: &str, ip: &str, mac: &str) -> NodeInfo {
    NodeInfo {
        node_id: id.into(),
        rack_id: rack_id.into(),
        r#type: Some(NodeType::SwitchGb200Nvidia as i32),
        bmc_endpoint: Some(bmc_endpoint(ip, mac, 443)),
        host_endpoint: None,
        ..Default::default()
    }
}

fn non_loopback_local_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).ok()?;
    socket
        .connect(SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 80)))
        .ok()?;

    let IpAddr::V4(ip) = socket.local_addr().ok()?.ip() else {
        return None;
    };
    if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() || ip.is_link_local() {
        return None;
    }

    Some(IpAddr::V4(ip))
}

#[tokio::test]
async fn push_switch_firmware_rejects_existing_file_outside_firmware_dir() {
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![switch_device(
            "sw-01",
            "rack-01",
            "10.0.0.11",
            "aa:bb:cc:00:00:11",
        )],
    )
    .await;
    let outside_dir = TempDir::new().expect("create outside firmware TempDir");
    let image = outside_dir.path().join("bmc-1.2.3.bin");
    std::fs::write(&image, vec![b'x'; 2048]).expect("write outside switch firmware");

    let resp = client
        .push_switch_firmware(PushSwitchFirmwareRequest {
            rack_id: "rack-01".into(),
            node_id: "sw-01".into(),
            component_type: SwitchFirmwareComponentType::Bmc as i32,
            filename: "bmc-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);

    assert!(
        resp.error_message.contains("outside firmware directory"),
        "expected firmware-dir containment error, got: {}",
        resp.error_message
    );

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_rejects_empty_nodes() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: vec![] }),
            image_filename: "nvos.bin".into(),
            local_file_path: "/tmp/nvos.bin".into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(
        response.message.to_lowercase().contains("no nodes"),
        "expected 'no nodes' in error, got: {}",
        response.message
    );
    assert!(response.job_id.is_empty());
    assert!(resp.jobs.is_empty());

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_rejects_empty_filename() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device(
                    "sw-01",
                    "rack-01",
                    "10.0.0.1",
                    "aa:bb:cc:00:00:11",
                )],
            }),
            image_filename: String::new(),
            local_file_path: "/tmp/nvos.bin".into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(
        response.message.to_lowercase().contains("image_filename"),
        "expected 'image_filename' in error, got: {}",
        response.message
    );

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_rejects_empty_local_path() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device(
                    "sw-01",
                    "rack-01",
                    "10.0.0.1",
                    "aa:bb:cc:00:00:11",
                )],
            }),
            image_filename: "nvos.bin".into(),
            local_file_path: String::new(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(
        response.message.to_lowercase().contains("local_file_path"),
        "expected 'local_file_path' in error, got: {}",
        response.message
    );

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_rejects_nonexistent_local_file() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device(
                    "sw-01",
                    "rack-01",
                    "10.0.0.11",
                    "aa:bb:cc:00:00:11",
                )],
            }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: "/tmp/this_file_definitely_does_not_exist_12345.bin".into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(
        response
            .message
            .to_lowercase()
            .contains("firmware file not found"),
        "expected file-not-found error, got: {}",
        response.message
    );

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_rejects_existing_file_outside_firmware_dir() {
    let (mut server, mut client) = start_server().await;
    let outside_dir = TempDir::new().expect("create outside image TempDir");
    let image = outside_dir.path().join("nvos-1.2.3.bin");
    std::fs::write(&image, b"fake nvos image payload").expect("write outside image");

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device(
                    "sw-01",
                    "rack-01",
                    "10.0.0.11",
                    "aa:bb:cc:00:00:11",
                )],
            }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(
        response.message.contains("outside firmware directory"),
        "expected firmware-dir containment error, got: {}",
        response.message
    );
    assert!(response.job_id.is_empty());
    assert!(resp.jobs.is_empty());

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_rejects_filename_without_version() {
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_no_version.bin");

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device(
                    "sw-01",
                    "rack-01",
                    "10.0.0.11",
                    "aa:bb:cc:00:00:11",
                )],
            }),
            image_filename: "nvos-latest.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(
        response.message.contains("unable to infer"),
        "expected build-id inference error, got: {}",
        response.message
    );

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_skips_non_switch_nodes() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_skip.bin");
    let devices = vec![compute_node("c-01", "rack-01", mockup.ports()[0])];

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: devices }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert_batch_stats(response, 1, 0, 1);
    assert!(!response.job_id.is_empty());
    let parent_job = poll_generic_job_until_terminal(&mut client, &response.job_id).await;
    assert_eq!(
        parent_job.job_states[0].execution_state,
        JobExecutionState::Failed as i32
    );
    assert!(resp.jobs.is_empty());
    assert_eq!(response.node_results.len(), 1);
    assert_eq!(response.node_results[0].node_id, "c-01");
    assert!(
        response.node_results[0]
            .error_message
            .contains("not a switch")
    );

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_creates_jobs_for_switches() {
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_create.bin");

    let mut devices = vec![
        switch_device("sw-01", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:11"),
        switch_device("sw-02", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:12"),
    ];

    for device in &mut devices {
        device.host_endpoint.as_mut().unwrap().port = 1;
    }

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: devices }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(
        response.status,
        ReturnCode::Success as i32,
        "expected success (all switches), got {}: {}",
        response.status,
        response.message
    );
    assert_batch_stats(response, 2, 2, 0);
    assert!(!response.job_id.is_empty(), "parent job_id must be set");
    assert_eq!(resp.jobs.len(), 2);

    let mut ids: Vec<&str> = resp.jobs.iter().map(|job| job.node_id.as_str()).collect();
    ids.sort();

    assert_eq!(ids, vec!["sw-01", "sw-02"]);

    for job in &resp.jobs {
        assert!(!job.job_id.is_empty());
    }

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_accepts_nvue_without_ssh_detection() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    for allow_onie_recovery in [false, true] {
        let (mut server, mut client) = start_server().await;
        let nvue = MockServer::start().await;
        let https = WiremockHttpsProxy::start(*nvue.address()).await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "System is ready", "health-status": "OK"
            })))
            .expect(1)
            .mount(&nvue)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system/image"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "current": "nvos-1.2.3", "next": "nvos-1.2.3"
            })))
            .expect(1)
            .mount(&nvue)
            .await;

        let image = server.make_fake_firmware_file("rms_test_nvue_image_probe.bin");
        let device = switch_password_device("nvue-probe", "rack-01", https.port());

        let device = if allow_onie_recovery {
            with_onie_recovery_endpoint(device, "192.0.2.50")
        } else {
            device
        };

        let response = client
            .update_switch_system_image(UpdateSwitchSystemImageRequest {
                nodes: Some(NodeSet {
                    nodes: vec![device],
                }),
                image_filename: "nvos-1.2.3.bin".to_owned(),
                local_file_path: image.to_str().unwrap().to_owned(),
                allow_onie_recovery,
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.jobs.len(), 1);

        let child = poll_job_until_terminal(&mut client, &response.jobs[0].job_id).await;

        assert_eq!(
            child.job_state,
            FirmwareJobState::Completed as i32,
            "{}",
            child.error_message
        );

        let status = client
            .get_switch_system_image_job_status(GetSwitchSystemImageJobStatusRequest {
                job_id: response.jobs[0].job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        let result: serde_json::Value = serde_json::from_str(&status.result_json).unwrap();

        assert_eq!(result["status"], "already_up_to_date");
        assert_eq!(result["allow_onie_recovery"], allow_onie_recovery);

        server.stop();
    }
}

#[tokio::test]
async fn update_switch_system_image_tracks_an_unreachable_switch_setup_failure() {
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_create.bin");
    let mut device = switch_device("sw-01", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:11");

    device.host_endpoint.as_mut().unwrap().port = 1;

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);

    assert_eq!(response.status, ReturnCode::Success as i32);
    assert_batch_stats(response, 1, 1, 0);

    assert!(!response.job_id.is_empty(), "parent job_id must be set");
    assert_eq!(resp.jobs.len(), 1);
    assert!(response.node_results.is_empty());

    let child =
        poll_job_until_terminal_within(&mut client, &resp.jobs[0].job_id, Duration::from_secs(30))
            .await;

    assert_eq!(child.job_state, FirmwareJobState::Failed as i32);

    assert!(
        child.error_message.contains("/nvue_v1/system"),
        "NVOS verification should preserve the REST error when recovery is disabled: {}",
        child.error_message
    );

    let parent_job = poll_generic_job_until_terminal(&mut client, &response.job_id).await;

    assert_eq!(
        parent_job.job_states[0].execution_state,
        JobExecutionState::Failed as i32
    );

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_admits_one_job_per_switch() {
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_dup.bin");

    let mut device = switch_device("sw-01", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:11");

    device.host_endpoint.as_mut().unwrap().port = 1;

    // A repeated switch must not get two concurrent image installs: the second
    // occurrence is refused and reported, not silently dropped.
    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet {
                nodes: vec![device.clone(), device],
            }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert_batch_stats(response, 2, 1, 1);
    assert_eq!(resp.jobs.len(), 1);
    assert_eq!(resp.jobs[0].node_id, "sw-01");

    let rejected: Vec<&str> = response
        .node_results
        .iter()
        .filter(|result| result.status == ReturnCode::Failure as i32)
        .map(|result| result.node_id.as_str())
        .collect();
    assert_eq!(rejected, ["sw-01"]);

    server.stop();
}

#[tokio::test]
async fn batch_disable_switch_mtls_rejects_empty_services_over_grpc() {
    let (mut server, mut client) = start_server().await;

    let error = client
        .batch_disable_switch_mtls(BatchDisableSwitchMtlsRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device(
                    "sw-01",
                    "rack-01",
                    "10.0.0.11",
                    "aa:bb:cc:00:00:11",
                )],
            }),
            services: Vec::new(),
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    assert!(
        error
            .message()
            .contains("services must contain at least one SwitchService")
    );

    server.stop();
}

#[tokio::test]
async fn update_switch_system_password_completes_parent_and_child_jobs() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;
    let nvue_https = WiremockHttpsProxy::start(*nvue.address()).await;

    let responses = [
        (
            "GET",
            "/nvue_v1/platform/chassis-location",
            serde_json::json!({"chassis-sn": "password-test-chassis", "tray-index": "1"}),
        ),
        ("GET", "/nvue_v1/system", serde_json::json!({})),
        ("POST", "/nvue_v1/revision", serde_json::json!("55")),
        (
            "PATCH",
            "/nvue_v1/system/aaa/user/admin",
            serde_json::json!({}),
        ),
        ("PATCH", "/nvue_v1/revision/55", serde_json::json!({})),
        (
            "GET",
            "/nvue_v1/revision/55",
            serde_json::json!({ "state": "applied" }),
        ),
        ("PATCH", "/nvue_v1/revision/applied", serde_json::json!({})),
    ];

    for (request_method, request_path, body) in responses {
        Mock::given(method(request_method))
            .and(path(request_path))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&nvue)
            .await;
    }

    let (mut server, mut client) = start_server().await;

    let devices = vec![switch_password_device(
        "sw-01",
        "rack-01",
        nvue_https.port(),
    )];

    let resp = client
        .update_switch_system_password(UpdateSwitchSystemPasswordRequest {
            nodes: Some(NodeSet { nodes: devices }),
            username: "admin".into(),
            password: "next-password".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);

    assert_eq!(response.status, ReturnCode::Success as i32);
    assert_batch_stats(response, 1, 1, 0);

    assert_eq!(response.node_results.len(), 1);
    assert_eq!(response.node_results[0].node_id, "sw-01");
    assert_eq!(response.node_results[0].status, ReturnCode::Success as i32);
    assert!(response.node_results[0].error_message.is_empty());
    assert!(!response.job_id.is_empty());

    let status = poll_generic_job_until_terminal(&mut client, &response.job_id).await;

    assert_eq!(status.job_states.len(), 2);

    assert_eq!(status.job_states[0].job_id, response.job_id);

    assert_eq!(
        status.job_states[0].execution_state,
        JobExecutionState::Completed as i32
    );

    assert_eq!(status.job_states[0].child_job_ids.len(), 1);
    assert!(!status.job_states[0].child_job_ids[0].is_empty());

    let child_job_id = &status.job_states[0].child_job_ids[0];

    assert_eq!(status.job_states[1].job_id, *child_job_id);
    assert_eq!(status.job_states[1].node_id.as_deref(), Some("sw-01"));

    assert_eq!(
        status.job_states[1].execution_state,
        JobExecutionState::Completed as i32
    );

    assert!(status.job_states[1].error_message.is_empty());

    let result: serde_json::Value = serde_json::from_str(&status.job_states[1].result_json)
        .expect("password update result must be valid JSON");

    assert_eq!(result["status"], "completed");
    assert_eq!(result["phase"], "password_update_persisted");
    assert_eq!(result["revision_id"], "55");

    server.stop();
}

#[tokio::test]
async fn switch_spdm_attestation_rpc_rejects_invalid_nonce_size() {
    let (mut server, mut client) = start_server().await;

    let error = client
        .batch_collect_switch_spdm_attestation_evidence(
            BatchCollectSwitchSpdmAttestationEvidenceRequest {
                targets: vec![NodeInfo {
                    rack_id: "rack-01".to_owned(),
                    node_id: "sw-01".to_owned(),
                    r#type: Some(NodeType::SwitchGb200Nvidia.into()),
                    ..Default::default()
                }],
                nonce: vec![1; 31],
                domain: None,
            },
        )
        .await
        .expect_err("invalid nonce size must be rejected");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("exactly 32 bytes"));

    server.stop();
}

#[tokio::test]
async fn switch_spdm_attestation_rpc_rejects_duplicate_physical_target() {
    let (mut server, mut client) = start_server().await;

    let error = client
        .batch_collect_switch_spdm_attestation_evidence(
            BatchCollectSwitchSpdmAttestationEvidenceRequest {
                targets: vec![
                    switch_device("sw-01", "rack-01", "10.0.0.11", "aa:bb:cc:00:00:11"),
                    switch_device("sw-alias", "rack-02", "10.0.0.11", "aa:bb:cc:00:00:11"),
                ],
                nonce: vec![1; 32],
                domain: None,
            },
        )
        .await
        .expect_err("duplicate physical switch must be rejected");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("duplicate physical switch"));

    server.stop();
}

#[tokio::test]
async fn switch_spdm_attestation_rpc_collects_evidence_over_insecure_nvue_https() {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;
    let nvue_https = WiremockHttpsProxy::start(*nvue.address()).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/security/spdm"))
        .and(query_param("rev", "operational"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ERoT_BMC_0": {}
        })))
        .mount(&nvue)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/system/security/spdm/ERoT_BMC_0"))
        .respond_with(ResponseTemplate::new(201).set_body_json("spdm-bmc"))
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/action/spdm-bmc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "action_success"
        })))
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/measurements",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"signed_measurements": "value"})),
        )
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/certificates",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"certificate_chain": "value"})),
        )
        .mount(&nvue)
        .await;

    // insecure_switch is an operator opt-out of switch mTLS, not a reason to
    // block attestation: the RPC must run over unverified NVUE HTTPS exactly
    // like every other switch RPC does under this flag.
    let (mut server, mut client) = start_server().await;

    let response = client
        .batch_collect_switch_spdm_attestation_evidence(
            BatchCollectSwitchSpdmAttestationEvidenceRequest {
                targets: vec![switch_password_device(
                    "sw-01",
                    "rack-01",
                    nvue_https.port(),
                )],
                nonce: vec![0xab; 32],
                domain: None,
            },
        )
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.status, ReturnCode::Success as i32);
    assert_eq!(response.results.len(), 1);

    let target = &response.results[0];

    assert_eq!(target.status, ReturnCode::Success as i32);
    assert_eq!(target.components.len(), 1);
    assert_eq!(target.components[0].status, ReturnCode::Success as i32);

    server.stop();
}

#[tokio::test]
async fn switch_spdm_attestation_rpc_collects_evidence_over_verified_nvue_https() {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;

    // The non-insecure NVUE client activation path probes readiness with a
    // GET before any SPDM calls.
    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system/security/spdm"))
        .and(query_param("rev", "operational"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ERoT_BMC_0": {}
        })))
        .mount(&nvue)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/system/security/spdm/ERoT_BMC_0"))
        .respond_with(ResponseTemplate::new(201).set_body_json("spdm-bmc"))
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/action/spdm-bmc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "action_success"
        })))
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/measurements",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"signed_measurements": "value"})),
        )
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/nvue_v1/system/security/spdm/ERoT_BMC_0/certificates",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"certificate_chain": "value"})),
        )
        .mount(&nvue)
        .await;

    let tls_root = TempDir::new().expect("create TLS material TempDir");
    let ca = NvueTestCa::new();

    let (server_cert, server_key) =
        write_nvue_client_tls_material(tls_root.path(), "test-domain", &ca);

    let nvue_https = WiremockHttpsProxy::start_with_certificate(
        *nvue.address(),
        server_cert,
        server_key,
        Some(&ca.cert),
    )
    .await;

    let (mut server, mut client) = start_server_with_switch_tls_roots(SwitchTlsRoots {
        client_tls: Some(TlsMaterialStore::new(tls_root.path())),
        dns_domain: Some("localhost".to_owned()),
        insecure_switch: false,
        ..SwitchTlsRoots::default()
    })
    .await;

    let response = client
        .batch_collect_switch_spdm_attestation_evidence(
            BatchCollectSwitchSpdmAttestationEvidenceRequest {
                targets: vec![switch_password_device(
                    "sw-01",
                    "rack-01",
                    nvue_https.port(),
                )],
                nonce: vec![0xab; 32],
                domain: Some("test-domain".to_owned()),
            },
        )
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.status, ReturnCode::Success as i32);
    assert_eq!(response.results.len(), 1);

    let target = &response.results[0];

    assert_eq!(target.status, ReturnCode::Success as i32);
    assert_eq!(target.node_id, "sw-01");
    assert!(target.collection_timestamp.is_some());
    assert_eq!(target.components.len(), 1);
    assert_eq!(target.components[0].component_id, "ERoT_BMC_0");
    assert_eq!(target.components[0].status, ReturnCode::Success as i32);
    assert!(target.components[0].evidence.is_some());

    server.stop();
}

#[tokio::test]
async fn switch_spdm_attestation_rpc_rejects_untrusted_nvue_server_certificate() {
    use wiremock::MockServer;

    // No SPDM mocks are needed: the TLS handshake must fail before any HTTP
    // request reaches wiremock.
    let nvue = MockServer::start().await;

    let tls_root = TempDir::new().expect("create TLS material TempDir");
    let ca = NvueTestCa::new();

    write_nvue_client_tls_material(tls_root.path(), "test-domain", &ca);

    // The proxy's server certificate is signed by a CA the TlsMaterialStore
    // above does not trust: this proves certificate verification is actually
    // load-bearing on this path, not a no-op that the happy-path test alone
    // couldn't distinguish from a broken "accept any cert" transport.
    let untrusted_ca = NvueTestCa::new();

    let (untrusted_server_cert, untrusted_server_key) =
        untrusted_ca.issue(vec!["localhost".to_owned(), "127.0.0.1".to_owned()]);

    let nvue_https = WiremockHttpsProxy::start_with_certificate(
        *nvue.address(),
        untrusted_server_cert,
        untrusted_server_key,
        Some(&ca.cert),
    )
    .await;

    let (mut server, mut client) = start_server_with_switch_tls_roots(SwitchTlsRoots {
        client_tls: Some(TlsMaterialStore::new(tls_root.path())),
        dns_domain: Some("localhost".to_owned()),
        insecure_switch: false,
        ..SwitchTlsRoots::default()
    })
    .await;

    let response = client
        .batch_collect_switch_spdm_attestation_evidence(
            BatchCollectSwitchSpdmAttestationEvidenceRequest {
                targets: vec![switch_password_device(
                    "sw-01",
                    "rack-01",
                    nvue_https.port(),
                )],
                nonce: vec![0xab; 32],
                domain: Some("test-domain".to_owned()),
            },
        )
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].status, ReturnCode::Failure as i32);

    assert!(
        response.results[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("certificate"),
        "expected a certificate-specific error, got: {:?}",
        response.results[0].error_message
    );

    server.stop();
}

#[tokio::test]
async fn get_switch_system_image_job_status_returns_known_job() {
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_status.bin");

    let mut device = switch_device("sw-01", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:11");

    device.host_endpoint.as_mut().unwrap().port = 1;
    let devices = vec![device];

    let create_resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: devices }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(create_resp.jobs.len(), 1);
    let child_job_id = create_resp.jobs[0].job_id.clone();
    let parent_job_id = batch_response(&create_resp.response).job_id.clone();

    let status = client
        .get_switch_system_image_job_status(GetSwitchSystemImageJobStatusRequest {
            job_id: child_job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(status.status, ReturnCode::Success as i32);
    assert_eq!(status.job_id, child_job_id);
    assert_eq!(status.rack_id, "rack-01");
    assert_eq!(status.node_id, "sw-01");
    assert!(!status.state.is_empty());

    let parent_status = client
        .get_switch_system_image_job_status(GetSwitchSystemImageJobStatusRequest {
            job_id: parent_job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(parent_status.status, ReturnCode::Success as i32);
    assert_eq!(parent_status.job_id, parent_job_id);
    assert_eq!(parent_status.rack_id, "rack-01");
    assert!(parent_status.node_id.is_empty(), "parent has no node_id");

    server.stop();
}

#[tokio::test]
async fn get_switch_system_image_job_status_unknown_job_fails_gracefully() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .get_switch_system_image_job_status(GetSwitchSystemImageJobStatusRequest {
            job_id: "does-not-exist".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(resp.message.contains("not found"));

    server.stop();
}

#[tokio::test]
async fn vrnvl72_system_image_rpc_rejects_missing_bmc_before_switch_io() {
    let host = wiremock::MockServer::start().await;
    let host_https = WiremockHttpsProxy::start(*host.address()).await;
    let (mut server, mut client) = start_server().await;
    let image = server.make_fake_firmware_file("rms_test_vr_nvos.bin");
    let mut device = switch_password_device("vr-sw-01", "rack-01", host_https.port());
    device.r#type = Some(NodeType::SwitchVrnvl72Nvidia as i32);

    for bmc in [
        None,
        Some(Endpoint {
            credentials: None,
            ..bmc_endpoint("127.0.0.1", "aa:bb:cc:00:20:01", 443)
        }),
    ] {
        device.bmc_endpoint = bmc;
        let response = client
            .update_switch_system_image(UpdateSwitchSystemImageRequest {
                nodes: Some(NodeSet {
                    nodes: vec![device.clone()],
                }),
                image_filename: "nvos-1.2.3.bin".into(),
                local_file_path: image.to_str().unwrap().into(),
                allow_onie_recovery: false,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(response.jobs.is_empty());
        let batch = batch_response(&response.response);
        assert_eq!(batch.status, ReturnCode::Failure as i32);
        assert_eq!(batch.node_results.len(), 1);
        assert!(
            batch.node_results[0]
                .error_message
                .contains("BMC endpoint and credentials")
        );
        assert!(host.received_requests().await.unwrap().is_empty());
    }
    server.stop();
}

#[tokio::test]
async fn switch_system_image_job_result_json_includes_timing_and_recovery_authorization() {
    // An unreachable loopback switch fails the real SSH installation probe.
    // The terminal job must still preserve the result-json observability contract.
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_timing.bin");

    let mut device = with_onie_recovery_endpoint(
        switch_device("sw-01", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:11"),
        "127.0.0.1",
    );

    device.host_endpoint.as_mut().unwrap().port = 1;
    let devices = vec![device];

    let create = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: devices }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(create.jobs.len(), 1);
    let child_job_id = create.jobs[0].job_id.clone();

    let mut status = None;

    for _ in 0..150 {
        let current = client
            .get_switch_system_image_job_status(GetSwitchSystemImageJobStatusRequest {
                job_id: child_job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        if current.state == "completed" || current.state == "failed" {
            status = Some(current);
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let status = status.expect("job should reach a terminal state within 30s");
    assert_eq!(
        status.state, "failed",
        "expected the unreachable switch to fail the SSH installation probe"
    );
    assert!(
        !status.result_json.is_empty(),
        "result_json must be populated even on failure"
    );

    let result: serde_json::Value =
        serde_json::from_str(&status.result_json).expect("result_json must be valid JSON");

    assert_eq!(result["allow_onie_recovery"], true);
    assert_eq!(result["onie_recovery_endpoint_configured"], true);

    let timing = result
        .get("timing_summary")
        .expect("timing_summary must be present in result_json");

    assert!(timing.is_object(), "timing_summary must be an object");
    assert!(timing.get("started_at_ms").is_some());
    assert!(timing.get("total_duration_ms").is_some());

    let stages = timing
        .get("stages")
        .and_then(|value| value.as_array())
        .expect("timing_summary.stages must be an array");

    assert!(!stages.is_empty(), "expected at least one stage entry");

    let probe = stages
        .iter()
        .find(|stage| stage["name"] == "verify_nvos_installation")
        .expect("verify_nvos_installation stage must be present");

    assert_eq!(probe["status"].as_str().unwrap(), "failed");
    assert!(probe["duration_ms"].as_i64().unwrap() >= 0);

    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  gRPC client --force plumbing
//
//  End-to-end integration tests exercising the force_update field via the
//  tonic client (update_firmware + batch_update_firmware).
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn update_firmware_with_force_update_true_succeeds() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;
    add_nodes(
        &mut client,
        vec![compute_node("c-01", "rack-01", mockup.ports()[0])],
    )
    .await;

    let tmp = server.make_synthetic_compute_fwpkg("rms_test_force_update_fw.fwpkg");

    let resp = client
        .update_firmware(UpdateFirmwareRequest {
            node_id: "c-01".into(),
            rack_id: "rack-01".into(),
            filename: tmp.to_str().unwrap().into(),
            target: "BMC".into(),
            activate: false,
            firmware_targets: vec![],
            force_update: true,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        ReturnCode::Success as i32,
        "async FW update with force=true should be accepted: {}",
        resp.message
    );
    assert!(!resp.job_id.is_empty());

    let job = poll_job_until_terminal(&mut client, &resp.job_id).await;
    assert_eq!(
        job.job_state,
        FirmwareJobState::Completed as i32,
        "job should complete with force=true"
    );

    server.stop();
}

#[tokio::test]
async fn batch_update_firmware_with_force_update_true_creates_jobs() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let tmp = server.make_synthetic_compute_fwpkg("rms_test_force_update_devlist.fwpkg");

    let devices = vec![compute_node("c-01", "rack-01", mockup.ports()[0])];
    let targets = firmware_targets(
        NodeType::ComputeGb200Nvidia,
        &[("HMC", tmp.to_str().unwrap())],
    );

    let resp = client
        .batch_update_firmware(BatchUpdateFirmwareRequest {
            nodes: Some(NodeSet { nodes: devices }),
            firmware_targets: targets,
            activate: false,
            force_update: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(
        response.status,
        ReturnCode::Success as i32,
        "node-list FW update with force=true should succeed: {}",
        response.message,
    );
    assert_batch_stats(response, 1, 1, 0);
    assert_eq!(resp.jobs.len(), 1);
    assert!(!response.job_id.is_empty());

    let parent_job = poll_job_until_terminal(&mut client, &response.job_id).await;
    assert_eq!(
        parent_job.job_state,
        FirmwareJobState::Completed as i32,
        "force-update device-list parent job should complete successfully: {}",
        parent_job.error_message
    );

    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  Switch uses host credentials for NVOS
//
//  Switches authenticate to NVUE / SSH via their host_endpoint credentials
//  (not the BMC). A switch that provides only bmc_endpoint credentials should
//  be rejected as missing host credentials; a switch with only host_endpoint
//  credentials should be accepted.
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn switch_firmware_preflight_skips_matching_packages_over_grpc() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;
    let https = WiremockHttpsProxy::start(*nvue.address()).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chassis-sn": "preflight-test-chassis", "tray-index": "1"
        })))
        .expect(2)
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/firmware"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "BMC": {"actual-firmware": "GB200Nvl-25.02-9"},
            "BIOS": {"actual-firmware": "unrelated-version"}
        })))
        .expect(2)
        .mount(&nvue)
        .await;

    let (mut server, mut client) = start_server().await;
    // This fixture is a PLDM package with one BMC record. Its component version
    // is read from the payload, independent of the local filename.
    let file = server.make_synthetic_compute_fwpkg("switch-preflight.fwpkg");
    let device = switch_password_device("preflight-switch", "rack-01", https.port());

    for firmware_object in [false, true] {
        let (parent_id, child_id) = if firmware_object {
            let response = client
                .apply_firmware_object(ApplyFirmwareObjectRequest {
                    rack_id: "rack-01".to_owned(),
                    config_json: firmware_object_release_catalog_json(
                        "switch-preflight",
                        serde_json::json!([{
                            "Name": "GB200-Switch",
                            "Type": "Switch Tray",
                            "Components": {"Firmware": [{
                                "Component": "BMC+FPGA+EROT",
                                "Type": "Prod",
                                "Locations": [{
                                    "Name": "Production_Package",
                                    "Location": file.to_str().unwrap(),
                                    "LocationType": "file",
                                    "Type": "Firmware"
                                }]
                            }]}
                        }]),
                    ),
                    hardware_type: "gb200".to_owned(),
                    firmware_type: "prod".to_owned(),
                    component_filters: HashMap::from([(
                        NodeType::SwitchGb200Nvidia as i32,
                        FirmwareObjectComponentFilter {
                            components: vec!["BMC".to_owned()],
                        },
                    )]),
                    nodes: Some(NodeSet {
                        nodes: vec![device.clone()],
                    }),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();

            let batch = batch_response(&response.response);

            assert_eq!(batch.status, ReturnCode::Success as i32);

            (batch.job_id.clone(), response.jobs[0].job_id.clone())
        } else {
            let response = client
                .batch_update_firmware(BatchUpdateFirmwareRequest {
                    nodes: Some(NodeSet {
                        nodes: vec![device.clone()],
                    }),
                    firmware_targets: firmware_targets(
                        NodeType::SwitchGb200Nvidia,
                        &[("BMC", file.to_str().unwrap())],
                    ),
                    activate: true,
                    force_update: false,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();

            let batch = batch_response(&response.response);

            assert_eq!(batch.status, ReturnCode::Success as i32);

            (batch.job_id.clone(), response.jobs[0].job_id.clone())
        };

        let child =
            poll_job_until_terminal_within(&mut client, &child_id, Duration::from_secs(10)).await;

        assert_eq!(
            child.job_state,
            FirmwareJobState::Completed as i32,
            "{}",
            child.error_message
        );

        let result: serde_json::Value = serde_json::from_str(&child.result_json).unwrap();

        assert_eq!(result["status"], "already_up_to_date");
        assert_eq!(result["targets_skipped"], 1);
        assert_eq!(result["activated"], false);

        assert_eq!(
            result["pre_update_version_check"]["components"][0]["package_version"],
            "GB200Nvl-25.02-9"
        );

        let parent =
            poll_job_until_terminal_within(&mut client, &parent_id, Duration::from_secs(10)).await;

        assert_eq!(
            parent.job_state,
            FirmwareJobState::Completed as i32,
            "{}",
            parent.error_message
        );
    }

    let requests = nvue.received_requests().await.unwrap();

    assert!(
        requests
            .iter()
            .all(|request| request.method == wiremock::http::Method::Get)
    );

    server.stop();
}

#[tokio::test]
async fn switch_erot_apply_skips_using_bios_bundle_version_over_grpc() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;
    let https = WiremockHttpsProxy::start(*nvue.address()).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chassis-sn": "erot-test-chassis", "tray-index": "1"
        })))
        .expect(2)
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/firmware"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "EROT-BMC": {"actual-firmware": "1.5"},
            "EROT-ASIC1": {"actual-firmware": "1.5"}
        })))
        .expect(2)
        .mount(&nvue)
        .await;

    let (mut server, mut client) = start_server().await;
    let old_file = server.firmware_dir.path().join("alpha.fwpkg");
    let preferred_file = server.firmware_dir.path().join("beta.fwpkg");

    write_synthetic_fwpkg(&old_file, b"ERoT", b"2.0");
    write_synthetic_fwpkg(&preferred_file, b"ERoT", b"1.5");

    for reverse in [false, true] {
        let mut components = vec![
            serde_json::json!({
                "Component": "BMC+FPGA+EROT", "Type": "Prod",
                "Locations": [{"Location": old_file, "LocationType": "file", "Type": "Firmware"}]
            }),
            serde_json::json!({
                "Component": "SBIOS+EROT", "Type": "Prod",
                "Locations": [{"Location": preferred_file, "LocationType": "file", "Type": "Firmware"}]
            }),
        ];

        if reverse {
            components.reverse();
        }

        let response = client
            .apply_firmware_object(ApplyFirmwareObjectRequest {
                rack_id: "rack-01".to_owned(),
                config_json: firmware_object_release_catalog_json(
                    "erot-precedence",
                    serde_json::json!([{
                        "Name": "Switch", "Type": "Switch Tray",
                        "Components": {"Firmware": components}
                    }]),
                ),
                hardware_type: "gb200".to_owned(),
                firmware_type: "prod".to_owned(),
                component_filters: HashMap::from([(
                    NodeType::SwitchGb200Nvidia as i32,
                    FirmwareObjectComponentFilter {
                        components: vec!["EROT".to_owned()],
                    },
                )]),
                nodes: Some(NodeSet {
                    nodes: vec![switch_password_device(
                        "erot-switch",
                        "rack-01",
                        https.port(),
                    )],
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            batch_response(&response.response).status,
            ReturnCode::Success as i32
        );

        let child = poll_job_until_terminal_within(
            &mut client,
            &response.jobs[0].job_id,
            Duration::from_secs(10),
        )
        .await;

        assert_eq!(
            child.job_state,
            FirmwareJobState::Completed as i32,
            "{}",
            child.error_message
        );

        let result: serde_json::Value = serde_json::from_str(&child.result_json).unwrap();

        assert_eq!(result["status"], "already_up_to_date");
        assert_eq!(result["targets_skipped"], 1);
        assert_eq!(result["activated"], false);

        assert!(
            result["targets"][0]["firmware_file"]
                .as_str()
                .unwrap()
                .ends_with("/beta.fwpkg")
        );

        let compared = result["pre_update_version_check"]["components"]
            .as_array()
            .unwrap();

        assert_eq!(compared.len(), 2);

        assert!(
            compared
                .iter()
                .all(|entry| entry["package_version"] == "1.5")
        );

        let parent = poll_job_until_terminal_within(
            &mut client,
            &batch_response(&response.response).job_id,
            Duration::from_secs(10),
        )
        .await;

        assert_eq!(parent.job_state, FirmwareJobState::Completed as i32);
    }

    assert!(
        nvue.received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| { request.method == wiremock::http::Method::Get })
    );

    server.stop();
}

#[tokio::test]
async fn batch_update_firmware_rejects_switch_with_bmc_only_credentials() {
    let (mut server, mut client) = start_server().await;

    let tmp = server.make_fake_firmware_file("rms_test_host_creds_switch_bmc_only.bin");

    let devices = vec![switch_device_bmc_only(
        "sw-01",
        "rack-01",
        "10.0.0.11",
        "aa:bb:cc:00:00:11",
    )];

    let targets = firmware_targets(
        NodeType::SwitchGb200Nvidia,
        &[("SW_0", tmp.to_str().unwrap())],
    );

    let resp = client
        .batch_update_firmware(BatchUpdateFirmwareRequest {
            nodes: Some(NodeSet { nodes: devices }),
            firmware_targets: targets,
            activate: false,
            force_update: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert_batch_stats(response, 1, 0, 1);
    assert!(!response.job_id.is_empty());
    let parent_job = poll_generic_job_until_terminal(&mut client, &response.job_id).await;
    assert_eq!(
        parent_job.job_states[0].execution_state,
        JobExecutionState::Failed as i32
    );
    assert!(resp.jobs.is_empty());
    assert_eq!(response.node_results.len(), 1);
    assert_eq!(response.node_results[0].node_id, "sw-01");
    assert!(
        response.node_results[0]
            .error_message
            .to_lowercase()
            .contains("host credentials"),
        "expected 'host credentials' in per-node error, got: {}",
        response.node_results[0].error_message,
    );

    server.stop();
}

#[tokio::test]
async fn batch_update_firmware_accepts_switch_with_host_only_credentials() {
    use wiremock::matchers::{basic_auth, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;
    let https = WiremockHttpsProxy::start(*nvue.address()).await;

    let discovery = Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .and(basic_auth("host_user", "host_pass"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chassis-sn": "host-credentials-test-chassis", "tray-index": "1"
        })))
        .expect(1)
        .mount_as_scoped(&nvue)
        .await;

    let (mut server, mut client) = start_server().await;

    let tmp = server.make_fake_firmware_file("rms_test_host_creds_switch_host_only.bin");

    // NVUE authenticates with host credentials without a BMC endpoint.
    let device = NodeInfo {
        node_id: "sw-01".into(),
        rack_id: "rack-01".into(),
        r#type: Some(NodeType::SwitchGb200Nvidia as i32),
        bmc_endpoint: None,
        host_endpoint: Some(Endpoint {
            interface: Some(NetworkInterface {
                ip_address: "127.0.0.1".into(),
                mac_address: "aa:bb:cc:00:00:11".into(),
                host_name: None,
            }),
            port: u32::from(https.port()),
            credentials: Some(Credentials {
                auth: Some(credentials::Auth::UserPass(UsernamePassword {
                    username: "host_user".into(),
                    password: "host_pass".into(),
                })),
            }),
        }),
        ..Default::default()
    };

    let targets = firmware_targets(
        NodeType::SwitchGb200Nvidia,
        &[("SW_0", tmp.to_str().unwrap())],
    );

    let resp = client
        .batch_update_firmware(BatchUpdateFirmwareRequest {
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            firmware_targets: targets,
            activate: false,
            force_update: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);

    assert_eq!(
        response.status,
        ReturnCode::Success as i32,
        "switch with only host creds should be accepted, got: {}",
        response.message
    );

    assert_batch_stats(response, 1, 1, 0);

    assert_eq!(resp.jobs.len(), 1);
    assert_eq!(resp.jobs[0].node_id, "sw-01");

    // RPC acceptance does not wait for the worker's authenticated discovery.
    tokio::time::timeout(Duration::from_secs(5), discovery.wait_until_satisfied())
        .await
        .expect("the worker must discover the switch before fixture teardown");

    server.stop();
}

#[tokio::test]
async fn update_switch_system_image_rejects_switch_with_bmc_only_credentials() {
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_bmc_only.bin");
    let devices = vec![switch_device_bmc_only(
        "sw-01",
        "rack-01",
        "10.0.0.11",
        "aa:bb:cc:00:00:11",
    )];

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: devices }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
            allow_onie_recovery: false,
        })
        .await
        .unwrap()
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert_batch_stats(response, 1, 0, 1);
    assert!(!response.job_id.is_empty());
    let parent_job = poll_generic_job_until_terminal(&mut client, &response.job_id).await;
    assert_eq!(
        parent_job.job_states[0].execution_state,
        JobExecutionState::Failed as i32
    );
    assert_eq!(response.node_results.len(), 1);
    assert!(
        response.node_results[0]
            .error_message
            .to_lowercase()
            .contains("host credentials"),
        "expected 'host credentials' in error, got: {}",
        response.node_results[0].error_message,
    );

    let _ = std::fs::remove_file(&image);
    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  ScaleUpFabricManager RPCs
//
//  ScaleUp RPCs take a caller-supplied NodeInfo (no inventory lookup).
//  These tests cover validation + empty-nodes handling; full
//  ScaleUp flows (NMX gRPC / NVUE cluster state) require a real switch and
//  are exercised via deployment tests, not unit / integration tests.
// ══════════════════════════════════════════════════════════════════════════════

fn switch_newnodeinfo_host_only(id: &str, ip: &str, mac: &str) -> NodeInfo {
    switch_newnodeinfo_host_only_with_port(id, ip, mac, 22)
}

fn switch_newnodeinfo_host_only_with_port(id: &str, ip: &str, mac: &str, port: u32) -> NodeInfo {
    NodeInfo {
        node_id: id.into(),
        rack_id: "rack-01".into(),
        r#type: Some(NodeType::SwitchGb200Nvidia as i32),
        bmc_endpoint: None,
        host_endpoint: Some(Endpoint {
            interface: Some(NetworkInterface {
                ip_address: ip.into(),
                mac_address: mac.into(),
                host_name: Some(format!("{id}.switch.example.com")),
            }),
            port,
            credentials: Some(Credentials {
                auth: Some(credentials::Auth::UserPass(UsernamePassword {
                    username: "admin".into(),
                    password: "password".into(),
                })),
            }),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn configure_scale_up_fabric_manager_rejects_missing_topology_type() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .configure_scale_up_fabric_manager(ConfigureScaleUpFabricManagerRequest {
            node: Some(switch_newnodeinfo_host_only(
                "sw-01",
                "10.0.0.11",
                "aa:bb:cc:00:00:11",
            )),
            topology_type: String::new(),
            domain: None,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);

    assert!(
        resp.message.to_lowercase().contains("topology_type"),
        "expected 'topology_type' in error, got: {}",
        resp.message
    );

    server.stop();
}

#[tokio::test]
async fn configure_scale_up_fabric_manager_rejects_missing_device() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .configure_scale_up_fabric_manager(ConfigureScaleUpFabricManagerRequest {
            node: None,
            topology_type: "gb200_nvl72r2_c2g4_topology".into(),
            domain: None,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);

    assert!(
        resp.message.to_lowercase().contains("device is required"),
        "expected 'device is required' in error, got: {}",
        resp.message
    );

    server.stop();
}

#[tokio::test]
async fn configure_scale_up_fabric_manager_rejects_switch_without_host_credentials() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .configure_scale_up_fabric_manager(ConfigureScaleUpFabricManagerRequest {
            node: Some(switch_device_bmc_only(
                "sw-01",
                "rack-01",
                "10.0.0.11",
                "aa:bb:cc:00:00:11",
            )),
            topology_type: "gb200_nvl72r2_c2g4_topology".into(),
            domain: None,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);

    assert!(
        resp.message.to_lowercase().contains("host credentials"),
        "expected 'host credentials' in error, got: {}",
        resp.message
    );

    server.stop();
}

#[tokio::test]
async fn rack_manager_v2_configure_scale_up_fabric_manager_rejects_invalid_requests() {
    struct TestCase {
        name: &'static str,
        request: rack_manager_v2::ConfigureScaleUpFabricManagerRequest,
        expected_message: &'static str,
    }

    let (mut server, mut client) = start_server().await;

    let valid_request = rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
        nodes: Some(NodeSet {
            nodes: vec![switch_newnodeinfo_host_only(
                "sw-01",
                "10.0.0.11",
                "aa:bb:cc:00:00:11",
            )],
        }),
        config: Some(rack_manager_v2::ScaleUpFabricConfig {
            topology_type: "gb200_nvl72r2_c2g4_topology".into(),
            extra_static_configs: vec![],
        }),
        ..Default::default()
    };

    let cases = [
        TestCase {
            name: "missing topology type",
            request: rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
                config: Some(rack_manager_v2::ScaleUpFabricConfig::default()),
                ..valid_request.clone()
            },
            expected_message: "topology_type",
        },
        TestCase {
            name: "missing nodes",
            request: rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
                nodes: None,
                ..valid_request.clone()
            },
            expected_message: "nodes",
        },
        TestCase {
            name: "missing config",
            request: rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
                config: None,
                ..valid_request.clone()
            },
            expected_message: "config",
        },
        TestCase {
            name: "missing host credentials",
            request: rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
                nodes: Some(NodeSet {
                    nodes: vec![switch_device_bmc_only(
                        "sw-01",
                        "rack-01",
                        "10.0.0.11",
                        "aa:bb:cc:00:00:11",
                    )],
                }),
                ..valid_request.clone()
            },
            expected_message: "host credentials",
        },
        TestCase {
            name: "duplicate host endpoint IP address",
            request: rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
                nodes: Some(NodeSet {
                    nodes: vec![
                        switch_newnodeinfo_host_only("sw-01", "10.0.0.11", "aa:bb:cc:00:00:11"),
                        switch_newnodeinfo_host_only("sw-02", "10.0.0.11", "aa:bb:cc:00:00:12"),
                    ],
                }),
                ..valid_request.clone()
            },
            expected_message: "duplicate host endpoint ip address",
        },
        TestCase {
            name: "duplicate ID across switch and compute roles",
            request: rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
                nodes: Some(NodeSet {
                    nodes: vec![
                        switch_newnodeinfo_host_only("shared-id", "10.0.0.11", "aa:bb:cc:00:00:11"),
                        NodeInfo {
                            node_id: "shared-id".into(),
                            r#type: Some(NodeType::ComputeGb200Nvidia.into()),
                            ..Default::default()
                        },
                    ],
                }),
                ..valid_request.clone()
            },
            expected_message: "nonempty and unique",
        },
        TestCase {
            name: "inventory contains no switches",
            request: rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
                nodes: Some(NodeSet {
                    nodes: vec![NodeInfo {
                        node_id: "compute-only".into(),
                        r#type: Some(NodeType::ComputeGb200Nvidia.into()),
                        ..Default::default()
                    }],
                }),
                ..valid_request.clone()
            },
            expected_message: "must contain at least one switch",
        },
    ];

    for case in cases {
        let err = client
            .rack_manager_v2
            .configure_scale_up_fabric_manager(case.request)
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{}", case.name);

        assert!(
            err.message().to_lowercase().contains(case.expected_message),
            "{}: expected '{}' in error, got: {}",
            case.name,
            case.expected_message,
            err.message()
        );
    }

    server.stop();
}

async fn mount_disabled_fabric_inspection_fixture(nvue: &wiremock::MockServer) {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chassis-sn": "inspection-chassis", "tray-index": "1", "slot-number": "2"
        })))
        .mount(nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"state":"disabled"})),
        )
        .mount(nvue)
        .await;
}

/// Exposes the NVUE fixture on a non-loopback address accepted by switch validation.
async fn switch_inspection_proxy(
    upstream: SocketAddr,
) -> (IpAddr, WiremockHttpsProxy, WiremockHttpsProxy) {
    use tokio::io::copy_bidirectional;
    use tokio::net::{TcpListener, TcpStream};

    let switch_ip =
        non_loopback_local_ip().expect("test requires a non-loopback local IPv4 address");

    let https = WiremockHttpsProxy::start(upstream).await;

    let listener = TcpListener::bind(SocketAddr::new(switch_ip, 0))
        .await
        .unwrap();

    let port = listener.local_addr().unwrap().port();
    let upstream_port = https.port();

    let task = tokio::spawn(async move {
        while let Ok((mut downstream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut upstream = TcpStream::connect((Ipv4Addr::LOCALHOST, upstream_port))
                    .await
                    .unwrap();

                let _ = copy_bidirectional(&mut downstream, &mut upstream).await;
            });
        }
    });

    (switch_ip, https, WiremockHttpsProxy { port, task })
}

#[tokio::test]
async fn fabric_inspection_reads_all_switches_without_jobs_or_mutations() {
    use librms::protos::rack_manager::InspectScaleUpFabricsRequest;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;
    let (switch_ip, _https, management_proxy) = switch_inspection_proxy(*nvue.address()).await;
    let port = management_proxy.port;

    mount_disabled_fabric_inspection_fixture(&nvue).await;

    let nodes = ["switch-a", "switch-b", "unassigned"].map(|id| {
        let mut node = switch_device(id, "rack-01", &switch_ip.to_string(), "aa:bb:cc:00:00:11");
        let endpoint = node.host_endpoint.as_mut().unwrap();

        endpoint.port = u32::from(port);
        endpoint.interface.as_mut().unwrap().host_name = None;

        node
    });

    let request = InspectScaleUpFabricsRequest {
        nodes: Some(NodeSet {
            nodes: nodes.into(),
        }),
        ..Default::default()
    };

    let (mut server, mut client) = start_server().await;

    let response = client
        .inspect_scale_up_fabrics(request.clone())
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.status, ReturnCode::Success as i32, "{response:?}");

    // Disabled controllers do not create fabric observations.
    assert!(response.fabrics.is_empty());
    assert!(response.discovery_errors.is_empty());

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(503).set_body_string("private upstream diagnostic"))
        .with_priority(1)
        .mount(&nvue)
        .await;

    let partial = client
        .inspect_scale_up_fabrics(request.clone())
        .await
        .unwrap()
        .into_inner();

    assert_eq!(partial.status, ReturnCode::Failure as i32);

    assert!(partial.fabrics.is_empty());

    assert_eq!(
        partial
            .discovery_errors
            .iter()
            .map(|issue| issue.node_id.as_str())
            .collect::<Vec<_>>(),
        ["switch-a", "switch-b", "unassigned"]
    );

    assert!(
        partial
            .discovery_errors
            .iter()
            .all(|issue| issue.message == "identity: resource read failed")
    );

    let requests = nvue.received_requests().await.unwrap();

    assert!(!requests.is_empty());

    assert!(
        requests
            .iter()
            .all(|request| request.method == wiremock::http::Method::Get
                && matches!(
                    request.url.path(),
                    "/nvue_v1/cluster" | "/nvue_v1/platform/chassis-location"
                ))
    );

    server.stop();
}

#[tokio::test]
async fn fabric_inspection_returns_partial_results_before_the_transport_deadline() {
    use wiremock::matchers::basic_auth;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;
    let (switch_ip, _https, management_proxy) = switch_inspection_proxy(*nvue.address()).await;

    mount_disabled_fabric_inspection_fixture(&nvue).await;

    Mock::given(basic_auth("stalled", "test-password"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
        .with_priority(1)
        .mount(&nvue)
        .await;

    // Four stalled switches precede the responsive switch. Every switch must
    // start collection without waiting for an earlier switch to finish.
    let nodes = (0..5)
        .map(|index| {
            let mut node = switch_device(
                &format!("switch-{index}"),
                "rack-01",
                &switch_ip.to_string(),
                "aa:bb:cc:00:00:11",
            );

            let endpoint = node.host_endpoint.as_mut().unwrap();

            endpoint.port = u32::from(management_proxy.port);
            endpoint.interface.as_mut().unwrap().host_name = None;

            if index < 4 {
                endpoint.credentials = Some(Credentials {
                    auth: Some(credentials::Auth::UserPass(UsernamePassword {
                        username: "stalled".into(),
                        password: "test-password".into(),
                    })),
                });
            }

            node
        })
        .collect();

    let mut request = tonic::Request::new(InspectScaleUpFabricsRequest {
        nodes: Some(NodeSet { nodes }),
        ..Default::default()
    });

    request.set_timeout(Duration::from_secs(10));

    let (mut server, mut client) = start_server().await;

    let response = client
        .inspect_scale_up_fabrics(request)
        .await
        .expect("partial inspection results must arrive before transport cancellation")
        .into_inner();

    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(response.fabrics.is_empty());
    assert_eq!(response.discovery_errors.len(), 8);

    for (index, error) in response.discovery_errors.iter().enumerate() {
        assert_eq!(error.node_id, format!("switch-{}", index / 2));
        assert!(error.message.contains("collection deadline exceeded"));
    }

    assert!(
        !response
            .discovery_errors
            .iter()
            .any(|error| error.node_id == "switch-4")
    );

    server.stop();
}

#[tokio::test]
async fn fabric_inspection_validation_returns_collection_errors() {
    let (mut server, mut client) = start_server().await;

    let response = client
        .inspect_scale_up_fabrics(
            librms::protos::rack_manager::InspectScaleUpFabricsRequest::default(),
        )
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(response.fabrics.is_empty());
    assert_eq!(response.discovery_errors.len(), 1);
    assert!(response.discovery_errors[0].node_id.is_empty());

    assert_eq!(response.discovery_errors[0].message, response.error_message);

    assert_eq!(
        response.error_message,
        "inspection requires at least one switch"
    );

    let mut node = switch_newnodeinfo_host_only("sw-01", "10.0.0.11", "aa:bb:cc:00:00:11");
    node.r#type = None;
    node.node_descriptor = Some(node_descriptor("switch", "NVIDIA", ""));

    // Inspection returns descriptor errors in discovery_errors rather than as gRPC errors.
    let response = client
        .inspect_scale_up_fabrics(librms::protos::rack_manager::InspectScaleUpFabricsRequest {
            nodes: Some(NodeSet { nodes: vec![node] }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.status, ReturnCode::Failure as i32);
    assert!(response.fabrics.is_empty());
    assert_eq!(response.discovery_errors.len(), 1);
    assert!(response.discovery_errors[0].node_id.is_empty());
    assert_eq!(response.discovery_errors[0].message, response.error_message);
    assert!(response.error_message.contains("product_family"));

    server.stop();
}

#[tokio::test]
async fn fabric_inspection_preserves_switch_validation_errors() {
    let (mut server, mut client) = start_server().await;

    for (case, expected) in [
        ("credentials", "missing host credentials"),
        ("endpoint", "missing host endpoint ip_address"),
        ("target", "switch target host is not allowed"),
    ] {
        let mut node = switch_device("switch-01", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:11");
        let endpoint = node.host_endpoint.as_mut().unwrap();

        endpoint.credentials = Some(Credentials {
            auth: Some(credentials::Auth::UserPass(UsernamePassword {
                username: "inspection-private-user".into(),
                password: "inspection-private-password".into(),
            })),
        });

        match case {
            "credentials" => endpoint.credentials = None,
            "endpoint" => endpoint.interface.as_mut().unwrap().ip_address.clear(),
            _ => {}
        }

        let response = client
            .inspect_scale_up_fabrics(InspectScaleUpFabricsRequest {
                nodes: Some(NodeSet { nodes: vec![node] }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.status, ReturnCode::Failure as i32);
        assert_eq!(response.discovery_errors.len(), 3);

        let error = response
            .discovery_errors
            .iter()
            .find(|error| error.message.starts_with("switch-connection:"))
            .expect("switch validation must identify the input error");

        assert_eq!(error.node_id, "switch-01");
        assert!(error.message.contains(expected), "{response:?}");

        let text = format!("{response:?}");

        assert!(!text.contains("inspection-private-user"));
        assert!(!text.contains("inspection-private-password"));
    }

    server.stop();
}

#[tokio::test]
async fn fabric_inspection_preserves_compute_validation_errors() {
    let (mut server, mut client) = start_server().await;

    for (missing_credentials, expected_message) in [
        (true, "identity: missing BMC credentials"),
        (false, "identity: missing BMC endpoint mac_address"),
    ] {
        let mut compute = compute_node("compute-01", "rack-01", 443);

        let mut endpoint = bmc_endpoint_with_auth(
            COMPUTE_HOST,
            "",
            443,
            "inspection-private-user",
            "inspection-private-password",
        );

        if missing_credentials {
            endpoint.credentials = None;
        }

        compute.bmc_endpoint = Some(endpoint);

        // An endpoint-free switch fails locally, so neither node contacts hardware.
        let switch = NodeInfo {
            node_id: "switch-01".into(),
            rack_id: "rack-01".into(),
            r#type: Some(NodeType::SwitchGb200Nvidia as i32),
            ..Default::default()
        };

        let response = client
            .inspect_scale_up_fabrics(librms::protos::rack_manager::InspectScaleUpFabricsRequest {
                nodes: Some(NodeSet {
                    nodes: vec![switch, compute],
                }),
                ..Default::default()
            })
            .await
            .expect("inspection must return collection errors in the response")
            .into_inner();

        assert_eq!(response.status, ReturnCode::Failure as i32);

        let error = response
            .discovery_errors
            .iter()
            .find(|error| error.node_id == "compute-01")
            .expect("compute validation failure must identify the compute node");

        assert_eq!(error.message, expected_message);

        let response_text = format!("{response:?}");
        assert!(!response_text.contains("inspection-private-user"));
        assert!(!response_text.contains("inspection-private-password"));
    }

    server.stop();
}

#[tokio::test]
async fn get_scale_up_fabric_status_normalizes_node_descriptors() {
    let (mut server, mut client) = start_server().await;
    let mut node = switch_newnodeinfo_host_only("sw-01", "10.0.0.11", "aa:bb:cc:00:00:11");
    node.r#type = None;
    node.node_descriptor = Some(node_descriptor("switch", "NVIDIA", ""));

    let error = client
        .get_scale_up_fabric_status(GetScaleUpFabricStatusRequest {
            nodes: Some(NodeSet { nodes: vec![node] }),
            domain: None,
        })
        .await
        .expect_err("invalid descriptor should be rejected at RPC boundary");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("product_family"));

    server.stop();
}

/// Both V2 paths normalize descriptors; valid layouts require an installed catalog.
#[tokio::test]
async fn fabric_layout_validates_inventory_before_requiring_deployment_rules() {
    let (mut server, mut client) = start_server().await;

    let nodes = Some(NodeSet {
        nodes: vec![NodeInfo {
            node_id: "switch-a".to_owned(),
            node_descriptor: Some(node_descriptor("switch", "NVIDIA", "")),
            ..Default::default()
        }],
    });

    let layout_result = client
        .rack_manager_v2
        .configure_scale_up_fabric_manager(rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
            nodes: nodes.clone(),
            config: Some(rack_manager_v2::ScaleUpFabricConfig {
                topology_type: "synthetic".to_owned(),
                ..Default::default()
            }),
            layout: Some(rack_manager_v2::ScaleUpFabricLayout {
                fabrics: vec![rack_manager_v2::ScaleUpFabricSpec {
                    compute_tray_count: 2,
                    switch_tray_count: 1,
                    primary_switch_node_id: None,
                }],
            }),
            ..Default::default()
        })
        .await;

    let layout_error = layout_result.expect_err("layout configuration must be refused");

    assert_eq!(layout_error.code(), tonic::Code::InvalidArgument);
    assert!(layout_error.message().contains("product_family"));

    let no_layout_result = client
        .rack_manager_v2
        .configure_scale_up_fabric_manager(rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
            nodes,
            ..Default::default()
        })
        .await;

    let no_layout_error = no_layout_result.expect_err("invalid descriptor must be rejected");

    assert_eq!(no_layout_error.code(), tonic::Code::InvalidArgument);
    assert!(no_layout_error.message().contains("product_family"));

    let configured = client
        .rack_manager_v2
        .configure_scale_up_fabric_manager(rack_manager_v2::ConfigureScaleUpFabricManagerRequest {
            nodes: Some(NodeSet {
                nodes: vec![NodeInfo {
                    node_id: "switch-a".to_owned(),
                    r#type: Some(NodeType::SwitchGb200Nvidia as i32),
                    ..Default::default()
                }],
            }),
            config: Some(rack_manager_v2::ScaleUpFabricConfig {
                topology_type: "synthetic".to_owned(),
                ..Default::default()
            }),
            layout: Some(rack_manager_v2::ScaleUpFabricLayout {
                fabrics: vec![rack_manager_v2::ScaleUpFabricSpec {
                    compute_tray_count: 1,
                    switch_tray_count: 1,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        })
        .await
        .expect_err("layout configuration requires deployment rules");

    assert_eq!(configured.code(), tonic::Code::Unimplemented);

    server.stop();
}

#[tokio::test]
async fn batch_get_scale_up_fabric_service_status_empty_nodes_returns_empty_map() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_get_scale_up_fabric_service_status(BatchGetScaleUpFabricServiceStatusRequest {
            nodes: Some(NodeSet { nodes: vec![] }),
        })
        .await
        .unwrap()
        .into_inner();

    // Empty request -> empty map, reported as Failure (no nodes successfully queried).
    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(resp.service_statuses.is_empty());
    assert_stats(&resp.stats, 0, 0, 0);

    server.stop();
}

#[tokio::test]
async fn batch_set_scale_up_fabric_state_empty_nodes_returns_failure_response() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_set_scale_up_fabric_state(BatchSetScaleUpFabricStateRequest {
            nodes: Some(NodeSet { nodes: vec![] }),
            enabled: true,
        })
        .await
        .unwrap()
        .into_inner();

    let batch = batch_response(&resp.response);
    assert_eq!(batch.status, ReturnCode::Failure as i32);
    assert_batch_stats(batch, 0, 0, 0);
    assert!(batch.node_results.is_empty());
    assert!(batch.job_id.is_empty());
    assert!(
        batch.message.to_lowercase().contains("nodes is required"),
        "got: {}",
        batch.message
    );

    server.stop();
}

#[tokio::test]
async fn batch_reset_switch_factory_default_rejects_empty_nodes() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_reset_switch_factory_default(BatchResetSwitchFactoryDefaultRequest {
            nodes: Some(NodeSet { nodes: vec![] }),
            domain: None,
            allow_ssh_fallback: false,
        })
        .await
        .unwrap()
        .into_inner();

    let batch = batch_response(&resp.response);

    assert_eq!(batch.status, ReturnCode::Failure as i32);
    assert_batch_stats(batch, 0, 0, 0);

    assert!(batch.node_results.is_empty());
    assert!(batch.job_id.is_empty());
    assert!(batch.message.contains("nodes is required"));

    server.stop();
}

#[tokio::test]
async fn batch_set_scale_up_fabric_state_rejects_switch_without_host_credentials() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_set_scale_up_fabric_state(BatchSetScaleUpFabricStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device_bmc_only(
                    "sw-01",
                    "rack-01",
                    "10.0.0.11",
                    "aa:bb:cc:00:00:11",
                )],
            }),
            enabled: true,
        })
        .await
        .unwrap()
        .into_inner();

    let batch = batch_response(&resp.response);
    assert_eq!(batch.status, ReturnCode::Failure as i32);
    assert_batch_stats(batch, 1, 0, 1);
    assert!(batch.job_id.is_empty());
    assert_eq!(batch.node_results.len(), 1);
    assert_eq!(batch.node_results[0].node_id, "sw-01");
    assert_eq!(batch.node_results[0].status, ReturnCode::Failure as i32);
    assert!(
        batch.node_results[0]
            .error_message
            .to_lowercase()
            .contains("host credentials"),
        "got: {}",
        batch.node_results[0].error_message
    );

    server.stop();
}

#[tokio::test]
async fn batch_set_scale_up_fabric_state_single_switch_failure_makes_batch_failure() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_set_scale_up_fabric_state(BatchSetScaleUpFabricStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_device_bmc_only(
                    "sw-01",
                    "rack-01",
                    "10.0.0.11",
                    "aa:bb:cc:00:00:11",
                )],
            }),
            enabled: true,
        })
        .await
        .unwrap()
        .into_inner();

    let batch = batch_response(&resp.response);
    assert_eq!(batch.status, ReturnCode::Failure as i32);
    assert_batch_stats(batch, 1, 0, 1);
    assert!(
        batch.message.to_lowercase().contains("enabled failed"),
        "got: {}",
        batch.message
    );

    server.stop();
}

#[tokio::test]
async fn set_scale_up_fabric_state_preserves_redacted_http_error_body() {
    use tokio::io::copy_bidirectional;
    use tokio::net::{TcpListener, TcpStream};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let switch_ip =
        non_loopback_local_ip().expect("test requires a non-loopback local IPv4 address");

    let nvue = MockServer::start().await;
    let https = WiremockHttpsProxy::start(*nvue.address()).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chassis-sn": "error-sanitization-test-chassis", "tray-index": "1"
        })))
        .expect(1)
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": "not found", "path": "/nvue_v1/cluster", "token": "device-secret"
        })))
        .expect(1)
        .mount(&nvue)
        .await;

    // The RPC rejects loopback management targets; forward local TLS traffic
    // from an allowed management address to the loopback HTTPS fixture.
    let listener = TcpListener::bind(SocketAddr::new(switch_ip, 0))
        .await
        .expect("bind local management proxy");

    let port = listener
        .local_addr()
        .expect("read management proxy address")
        .port();

    let upstream_port = https.port();

    let task = tokio::spawn(async move {
        while let Ok((mut downstream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut upstream = TcpStream::connect((Ipv4Addr::LOCALHOST, upstream_port))
                    .await
                    .expect("connect local HTTPS fixture");

                let _ = copy_bidirectional(&mut downstream, &mut upstream).await;
            });
        }
    });

    let management_proxy = WiremockHttpsProxy { port, task };
    let (mut server, mut client) = start_server().await;
    let switch_ip = switch_ip.to_string();

    let mut device = switch_device("sw-01", "rack-01", &switch_ip, "aa:bb:cc:00:00:11");

    if let Some(iface) = device
        .host_endpoint
        .as_mut()
        .and_then(|endpoint| endpoint.interface.as_mut())
    {
        // Mock listens on the management IP; omit DNS name so NVUE uses ip_address.
        iface.host_name = None;
    }

    if let Some(endpoint) = device.host_endpoint.as_mut() {
        endpoint.port = u32::from(management_proxy.port());
    }

    let resp = client
        .batch_set_scale_up_fabric_state(BatchSetScaleUpFabricStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            enabled: true,
        })
        .await
        .unwrap()
        .into_inner();

    let batch = batch_response(&resp.response);

    assert_eq!(batch.status, ReturnCode::Failure as i32);
    assert_batch_stats(batch, 1, 0, 1);

    assert_eq!(batch.node_results.len(), 1);

    assert_eq!(
        batch.node_results[0].error_message,
        r#"HTTP GET /nvue_v1/cluster returned 404: {"error":"not found","path":"/nvue_v1/cluster","token":"XXXX"}"#
    );

    assert!(
        !batch.node_results[0]
            .error_message
            .contains("device-secret")
    );

    server.stop();
}

#[tokio::test]
async fn set_scale_up_fabric_state_rejects_unsafe_host_endpoint() {
    let (mut server, mut client) = start_server().await;
    let device = switch_device("sw-01", "rack-01", "127.0.0.1", "aa:bb:cc:00:00:11");

    let resp = client
        .batch_set_scale_up_fabric_state(BatchSetScaleUpFabricStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![device],
            }),
            enabled: true,
        })
        .await
        .unwrap()
        .into_inner();

    let batch = batch_response(&resp.response);
    assert_eq!(batch.status, ReturnCode::Failure as i32);
    assert_batch_stats(batch, 1, 0, 1);
    assert_eq!(batch.node_results.len(), 1);
    assert_eq!(
        batch.node_results[0].error_message,
        "switch target host is not allowed: 127.0.0.1"
    );

    server.stop();
}

#[tokio::test]
async fn password_rotation_retry_uses_requested_credentials_for_physical_admission() {
    use wiremock::matchers::{basic_auth, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let nvue = MockServer::start().await;
    let https = WiremockHttpsProxy::start(*nvue.address()).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .and(basic_auth("admin", "current-password"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/system"))
        .and(basic_auth("admin", "next-password"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(2)
        .mount(&nvue)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .and(basic_auth("admin", "next-password"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"chassis-sn":"retry-chassis","tray-index":"1"})),
        )
        .expect(1)
        .mount(&nvue)
        .await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .and(basic_auth("admin", "next-password"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&nvue)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&nvue)
        .await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/system/aaa/user/admin"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&nvue)
        .await;

    let (mut server, mut client) = start_server().await;

    let response = client
        .update_switch_system_password(UpdateSwitchSystemPasswordRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_password_device(
                    "retry-switch",
                    "rack-01",
                    https.port(),
                )],
            }),
            username: "admin".into(),
            password: "next-password".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let batch = batch_response(&response.response);
    let status = poll_generic_job_until_terminal(&mut client, &batch.job_id).await;

    assert_eq!(status.job_states.len(), 2);

    assert!(
        status
            .job_states
            .iter()
            .all(|job| job.execution_state == JobExecutionState::Completed as i32),
        "{status:?}"
    );

    let result: serde_json::Value =
        serde_json::from_str(&status.job_states[1].result_json).unwrap();

    assert_eq!(result["phase"], "applied_config_persisted");

    nvue.verify().await;

    server.stop();
}

#[tokio::test]
async fn get_scale_up_fabric_state_rejects_missing_device() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .get_scale_up_fabric_state(GetScaleUpFabricStateRequest { node: None })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(
        resp.error_message
            .to_lowercase()
            .contains("device is required"),
        "got: {}",
        resp.error_message
    );

    server.stop();
}

#[tokio::test]
async fn set_scale_up_fabric_telemetry_interface_state_rejects_missing_device() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .set_scale_up_fabric_telemetry_interface_state(
            SetScaleUpFabricTelemetryInterfaceStateRequest {
                node: None,
                enable: true,
            },
        )
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(
        resp.error_message
            .to_lowercase()
            .contains("device is required"),
        "got: {}",
        resp.error_message
    );

    server.stop();
}

// ══════════════════════════════════════════════════════════════════════════════
//  BatchGetNodeDeviceInfo - caller-supplied node list
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn batch_get_node_device_info_rejects_empty_nodes() {
    let (mut server, mut client) = start_server().await;

    let resp = client
        .batch_get_node_device_info(BatchGetNodeDeviceInfoRequest {
            nodes: Some(NodeSet { nodes: vec![] }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(
        resp.message.to_lowercase().contains("no nodes"),
        "got: {}",
        resp.message
    );
    assert!(resp.node_device_details.is_empty());
    assert_stats(&resp.stats, 0, 0, 0);

    server.stop();
}

#[tokio::test]
async fn batch_get_node_device_info_populates_compute_tray_fields() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let devices = vec![
        compute_node("c-01", "rack-01", mockup.ports()[0]),
        compute_node("c-02", "rack-01", mockup.ports()[0]),
    ];

    let resp = client
        .batch_get_node_device_info(BatchGetNodeDeviceInfoRequest {
            nodes: Some(NodeSet { nodes: devices }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        ReturnCode::Success as i32,
        "expected success, got {}: {}",
        resp.status,
        resp.message
    );
    assert_eq!(resp.node_device_details.len(), 2);
    assert_stats(&resp.stats, 2, 2, 0);

    let mut ids: Vec<&str> = resp
        .node_device_details
        .iter()
        .map(|n| n.node_id.as_str())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["c-01", "c-02"]);

    for info in &resp.node_device_details {
        assert_eq!(info.chassis_sn, Some(COMPUTE_CHASSIS_SN));
        assert_eq!(info.slot_number, Some(COMPUTE_SLOT_NUMBER));
        assert_eq!(info.tray_index, Some(COMPUTE_TRAY_INDEX));
    }

    server.stop();
}

#[tokio::test]
async fn batch_get_node_device_info_aggregates_per_node_failures() {
    // A bogus BMC port means the HTTP fetch will fail, which surfaces as a
    // per-node error without killing the whole RPC. The response is reported
    // as FAILURE overall with a "Completed with errors" summary.
    let (mut server, mut client) = start_server().await;

    let devices = vec![compute_node("c-bad", "rack-01", 1)];

    let resp = client
        .batch_get_node_device_info(BatchGetNodeDeviceInfoRequest {
            nodes: Some(NodeSet { nodes: devices }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(
        resp.message
            .to_lowercase()
            .contains("completed with errors")
            || resp.message.to_lowercase().contains("error"),
        "got: {}",
        resp.message
    );
    assert!(resp.node_device_details.is_empty());
    assert_stats(&resp.stats, 1, 0, 1);

    server.stop();
}

#[tokio::test]
async fn batch_get_node_device_info_assigns_results_across_phase_boundary() {
    // The batch read is two-phase: nodes with no device info (powershelves)
    // resolve synchronously in phase 1, while nodes that build a real instance
    // (computes) are fanned out in phase 2 and reassembled by original index.
    // Positioning the powershelf *between* the computes interleaves the two
    // phases' index spaces, so this pins that phase-2 results land on the
    // correct node and the stats account for the phase-1 success too — a path
    // the powershelf-only test leaves unexercised (its `ready` set is empty).
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let devices = vec![
        compute_node("c-01", "rack-01", mockup.ports()[0]),
        powershelf_node("ps-mid", "rack-01", mockup.ports()[1]),
        compute_node("c-02", "rack-01", mockup.ports()[0]),
    ];

    let resp = client
        .batch_get_node_device_info(BatchGetNodeDeviceInfoRequest {
            nodes: Some(NodeSet { nodes: devices }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status,
        ReturnCode::Success as i32,
        "expected success, got {}: {}",
        resp.status,
        resp.message
    );
    // Powershelf is a success with no detail row; only the two computes have
    // device info, so 3 total / 3 successful / 0 failed and 2 detail rows.
    assert_stats(&resp.stats, 3, 3, 0);
    assert_eq!(resp.node_device_details.len(), 2);

    let mut ids: Vec<&str> = resp
        .node_device_details
        .iter()
        .map(|n| n.node_id.as_str())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["c-01", "c-02"],
        "phase-2 results must be attributed to the compute nodes, not shifted by the phase-1 powershelf"
    );

    for info in &resp.node_device_details {
        assert_eq!(info.chassis_sn, Some(COMPUTE_CHASSIS_SN));
        assert_eq!(info.slot_number, Some(COMPUTE_SLOT_NUMBER));
        assert_eq!(info.tray_index, Some(COMPUTE_TRAY_INDEX));
    }

    server.stop();
}

#[tokio::test]
async fn batch_get_node_device_info_reports_delta_powershelf_as_no_device_info_stateless() {
    let (mut server, mut client) = start_server().await;

    // Powershelves expose no device info, so a stateless batch read short-
    // circuits to "no device info" up front: a success with no details and no
    // credential/endpoint validation, consistent with the single-node and
    // list-by-type handlers and every powershelf kind.
    let devices = vec![mk_node_info(
        "ps-delta",
        "rack-01",
        POWERSHELF_HOST,
        "aa:bb:cc:dd:ee:03",
        443,
        Some("admin"),
        Some("password"),
        NodeType::PowershelfGb200Delta,
        vec![],
        vec![],
    )];

    let resp = client
        .batch_get_node_device_info(BatchGetNodeDeviceInfoRequest {
            nodes: Some(NodeSet { nodes: devices }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, ReturnCode::Success as i32);
    assert!(resp.node_device_details.is_empty());
    assert_stats(&resp.stats, 1, 1, 0);

    server.stop();
}
