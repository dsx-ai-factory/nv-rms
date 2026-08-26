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
use rackmanagementservice::config::{ExpectedInventoryCatalog, ExpectedInventoryProfiles};
use rackmanagementservice::metrics;
use rackmanagementservice::orchestrator::job_tracker::JobTracker;
use rackmanagementservice::orchestrator::rack_manager::RackManager;
use rackmanagementservice::persistence::Backends;
use redfish_test_support::RedfishSimulator;
use tempfile::TempDir;

use rack_manager::*;

// ══════════════════════════════════════════════════════════════════════════════
//  Redfish simulator lifecycle (in-process Rust server)
// ══════════════════════════════════════════════════════════════════════════════

fn write_synthetic_compute_fwpkg(path: &Path) {
    const EXPECTED_UUID: [u8; 16] = [
        0xf0, 0x18, 0x87, 0x8c, 0xcb, 0x7d, 0x49, 0x43, 0x98, 0x00, 0xa0, 0x2f, 0x05, 0x9a, 0xca,
        0x02,
    ];

    let version = b"GB200-P4978_TEST";
    let device_version = b"BMC";
    let component_version = b"GB200Nvl-25.02-9";
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

    std::fs::write(path, pkg).expect("write synthetic compute fwpkg");
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
    let firmware_dir = TempDir::new().expect("create firmware TempDir");
    let jt = Arc::new(JobTracker::new());

    let mut grpc_server = GrpcServer::new(0, rm, jt, Backends::memory())
        .with_firmware_dir(firmware_dir.path())
        .with_expected_inventory_catalog(expected_inventory_catalog)
        .with_insecure_listener()
        .with_switch_tls_roots(SwitchTlsRoots {
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        });
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
async fn descriptor_only_nodes_support_inventory_and_power() {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let mut nvidia = compute_node("c-01", "rack-01", mockup.ports()[0]);
    nvidia.r#type = None;
    nvidia.node_descriptor = Some(node_descriptor("compute", "NVIDIA", "gb200"));

    let mut wiwynn = compute_node("c-02", "rack-01", mockup.ports()[0]);
    wiwynn.r#type = None;
    wiwynn.node_descriptor = Some(node_descriptor("compute", "Wiwynn", "gb200"));

    add_nodes(&mut client, vec![nvidia.clone(), wiwynn.clone()]).await;

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

    let power = client
        .batch_get_power_state(BatchGetPowerStateRequest {
            nodes: Some(NodeSet {
                nodes: vec![nvidia, wiwynn],
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

// ── Power-on sequence ──

#[tokio::test]
async fn set_and_get_power_on_sequence() {
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

    let set_resp = client
        .set_rack_power_on_sequence(SetRackPowerOnSequenceRequest {
            rack_id: "rack-01".into(),
            power_on_order: vec![PowerOnOrderItem {
                node_id: "c-01".into(),
                completion_check: Some(CompletionCheck {
                    enabled: true,
                    timeout_seconds: 120,
                }),
            }],
        })
        .await
        .unwrap()
        .into_inner();

    let set_response = operation_response(&set_resp.response);
    assert_eq!(set_response.status, ReturnCode::Success as i32);

    let get_resp = client
        .get_rack_power_on_sequence(GetRackPowerOnSequenceRequest {
            rack_id: "rack-01".into(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(get_resp.status, ReturnCode::Success as i32);
    assert!(get_resp.is_valid);
    assert_eq!(get_resp.power_on_order.len(), 1);
    assert_eq!(get_resp.power_on_order[0].node_id, "c-01");

    let check = get_resp.power_on_order[0]
        .completion_check
        .as_ref()
        .unwrap();
    assert!(check.enabled);
    assert_eq!(check.timeout_seconds, 120);

    server.stop();
}

#[tokio::test]
async fn sequence_rack_power_off_uses_registered_switch_bmc_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let mockup = start_mockup().await;
    let (mut server, mut client) = start_server().await;

    let create_resp = client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet {
                nodes: vec![switch_power_node("sw-01", "rack-01", mockup.ports()[0])],
            }),
        })
        .await?
        .into_inner();

    assert_eq!(
        create_resp
            .response
            .as_ref()
            .map(|response| response.status),
        Some(ReturnCode::Success as i32)
    );

    let set_resp = client
        .set_rack_power_on_sequence(SetRackPowerOnSequenceRequest {
            rack_id: "rack-01".into(),
            power_on_order: vec![PowerOnOrderItem {
                node_id: "sw-01".into(),
                completion_check: None,
            }],
        })
        .await?
        .into_inner();

    assert_eq!(
        set_resp.response.as_ref().map(|response| response.status),
        Some(ReturnCode::Success as i32)
    );

    let power_resp = client
        .sequence_rack_power(SequenceRackPowerRequest {
            operation: RackPowerOperation::Off as i32,
            rack_id: "rack-01".into(),
        })
        .await?
        .into_inner();

    assert_eq!(power_resp.status, ReturnCode::Success as i32);

    server.stop();
    Ok(())
}

#[tokio::test]
async fn set_power_on_sequence_rejects_invalid_orders() -> Result<(), Box<dyn std::error::Error>> {
    let (mut server, mut client) = start_server().await;

    let create_resp = client
        .create_nodes(CreateNodesRequest {
            nodes: Some(NodeSet {
                nodes: vec![compute_node("c-01", "rack-01", 443)],
            }),
        })
        .await?
        .into_inner();

    assert_eq!(
        create_resp
            .response
            .as_ref()
            .map(|response| response.status),
        Some(ReturnCode::Success as i32)
    );

    let order_item = |node_id: &str| PowerOnOrderItem {
        node_id: node_id.into(),
        completion_check: None,
    };

    let set_resp = client
        .set_rack_power_on_sequence(SetRackPowerOnSequenceRequest {
            rack_id: "rack-01".into(),
            power_on_order: vec![order_item("c-01")],
        })
        .await?
        .into_inner();

    assert_eq!(
        set_resp.response.as_ref().map(|response| response.status),
        Some(ReturnCode::Success as i32)
    );

    let cases = [
        (
            "unknown node",
            vec![order_item("missing-node")],
            "unknown node",
        ),
        ("empty node", vec![order_item("")], "node_id is required"),
        (
            "duplicated nodes",
            vec![order_item("c-01"), order_item("c-01")],
            "duplicate node",
        ),
        ("empty sequence", Vec::new(), "power-on order is required"),
    ];

    for (case_name, power_on_order, expected_message) in cases {
        let set_resp = client
            .set_rack_power_on_sequence(SetRackPowerOnSequenceRequest {
                rack_id: "rack-01".into(),
                power_on_order,
            })
            .await?
            .into_inner();

        assert_eq!(
            set_resp.response.as_ref().map(|response| response.status),
            Some(ReturnCode::Failure as i32),
            "{case_name}"
        );
        assert!(
            set_resp
                .response
                .as_ref()
                .is_some_and(|response| response.message.contains(expected_message)),
            "{case_name}"
        );

        let get_resp = client
            .get_rack_power_on_sequence(GetRackPowerOnSequenceRequest {
                rack_id: "rack-01".into(),
            })
            .await?
            .into_inner();

        assert_eq!(get_resp.status, ReturnCode::Success as i32, "{case_name}");
        assert!(get_resp.is_valid, "{case_name}");
        assert_eq!(get_resp.power_on_order.len(), 1, "{case_name}");
        assert_eq!(get_resp.power_on_order[0].node_id, "c-01", "{case_name}");
    }

    server.stop();

    Ok(())
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

// ── Compute rack-level firmware inventory ──

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

    // The powershelf maps ForceOn to On (no separate hardware surface), but a
    // graceful Off stays graceful (ResetType GracefulShutdown) and is never
    // silently upgraded to a hard ForceOff. All four are honored by a shelf
    // that advertises the corresponding ResetType.
    for op in [
        PowerOperation::ForceOff,
        PowerOperation::On,
        PowerOperation::Off,
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
        PowerOperation::GracefulShutdown,
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
            target: String::new(),
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
async fn compute_firmware_upload_guaranteed_failure() {
    // Dedicated simulator instance with 100% failure rate, isolated from other tests.
    let fail_port = portpicker::pick_unused_port().expect("no free port") as u16;
    let fail_mockup = RedfishSimulator::builder()
        .with_task_delay(0.01)
        .always_fail_firmware_updates()
        .add_gb200_compute(fail_port)
        .start()
        .await;

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
        &[("HGX_0", tmp.to_str().unwrap())],
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
async fn apply_firmware_object_downloads_without_access_token() {
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
    let parent_job =
        poll_job_until_terminal_within(&mut client, &response.job_id, Duration::from_secs(120))
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
async fn apply_firmware_object_downloads_from_artifactory_with_access_token() {
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let artifact_path = "/compute-bmc.fwpkg";
    let temp = TempDir::new().expect("create temp dir for fwpkg fixture");
    let fwpkg_path = temp.path().join("compute-bmc.fwpkg");
    write_synthetic_compute_fwpkg(&fwpkg_path);
    let fwpkg_bytes = std::fs::read(&fwpkg_path).expect("read synthetic compute fwpkg");

    let http_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(artifact_path))
        .and(header("X-JFrog-Art-Api", "secret-token"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(fwpkg_bytes))
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

    let parent_job =
        poll_job_until_terminal_within(&mut client, &response.job_id, Duration::from_secs(120))
            .await;
    assert_eq!(
        parent_job.job_state,
        FirmwareJobState::Completed as i32,
        "artifactory download apply should complete: {}",
        parent_job.error_message
    );

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

    let parent_job =
        poll_job_until_terminal_within(&mut client, &response.job_id, Duration::from_secs(120))
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
async fn apply_switch_system_image_downloads_remote_artifact() {
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
    let devices = vec![switch_device(
        "sw-01",
        "rack-01",
        "127.0.0.1",
        "aa:bb:cc:00:00:11",
    )];

    let resp = client
        .apply_switch_system_image(ApplySwitchSystemImageRequest {
            rack_id: "rack-01".into(),
            config_json,
            access_token: None,
            software_type: "prod".into(),
            hardware_type: "gb200".into(),
            nodes: Some(NodeSet { nodes: devices }),
        })
        .await
        .expect("apply_switch_system_image RPC should succeed")
        .into_inner();

    let response = batch_response(&resp.response);
    assert_eq!(response.status, ReturnCode::Success as i32);
    assert_eq!(resp.jobs.len(), 1);
    let child_job_id = resp.jobs[0].job_id.clone();

    let mut saw_terminal = false;
    for _ in 0..150 {
        let status = client
            .get_switch_system_image_job_status(GetSwitchSystemImageJobStatusRequest {
                job_id: child_job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        if status.state == "completed" || status.state == "failed" {
            saw_terminal = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(saw_terminal, "switch image job should reach terminal state");

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
    let devices = vec![
        switch_device("sw-01", "rack-01", "10.0.0.11", "aa:bb:cc:00:00:11"),
        switch_device("sw-02", "rack-01", "10.0.0.12", "aa:bb:cc:00:00:12"),
    ];

    let resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: devices }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
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

    let mut ids: Vec<&str> = resp.jobs.iter().map(|j| j.node_id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["sw-01", "sw-02"]);
    for nj in &resp.jobs {
        assert!(!nj.job_id.is_empty());
    }

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
async fn get_switch_system_image_job_status_returns_known_job() {
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_status.bin");
    let devices = vec![switch_device(
        "sw-01",
        "rack-01",
        "10.0.0.11",
        "aa:bb:cc:00:00:11",
    )];

    let create_resp = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: devices }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
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
async fn switch_system_image_job_result_json_includes_timing_summary() {
    // Failure path (the switch-OS background task reaches the inspect_state
    // stage, fails there because the bogus IP can't be reached, and writes
    // the stage timeline into `result_json`). This asserts the observability
    // contract: callers can read `timing_summary.stages[*].{name,status,duration_ms}`
    // out of GetSwitchSystemImageJobStatus.result_json for any completed job.
    let (mut server, mut client) = start_server().await;

    let image = server.make_fake_firmware_file("rms_test_switch_sys_image_timing.bin");
    let devices = vec![switch_device(
        "sw-01",
        "rack-01",
        "127.0.0.1", // local host endpoint will refuse quickly
        "aa:bb:cc:00:00:11",
    )];

    let create = client
        .update_switch_system_image(UpdateSwitchSystemImageRequest {
            nodes: Some(NodeSet { nodes: devices }),
            image_filename: "nvos-1.2.3.bin".into(),
            local_file_path: image.to_str().unwrap().into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(create.jobs.len(), 1);
    let child_job_id = create.jobs[0].job_id.clone();

    // Poll GetSwitchSystemImageJobStatus until the child job leaves running/queued
    // (up to ~30s). Failure path is quick since the BMC/NVUE connection refuses.
    let mut status = None;
    for _ in 0..150 {
        let s = client
            .get_switch_system_image_job_status(GetSwitchSystemImageJobStatusRequest {
                job_id: child_job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        if s.state == "completed" || s.state == "failed" {
            status = Some(s);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let status = status.expect("job should reach a terminal state within 30s");
    assert_eq!(
        status.state, "failed",
        "expected the bogus-IP job to fail in inspect_state"
    );
    assert!(
        !status.result_json.is_empty(),
        "result_json must be populated even on failure"
    );

    let result: serde_json::Value =
        serde_json::from_str(&status.result_json).expect("result_json must be valid JSON");
    let timing = result
        .get("timing_summary")
        .expect("timing_summary must be present in result_json");
    assert!(timing.is_object(), "timing_summary must be an object");
    assert!(timing.get("started_at_ms").is_some());
    assert!(timing.get("total_duration_ms").is_some());

    let stages = timing
        .get("stages")
        .and_then(|v| v.as_array())
        .expect("timing_summary.stages must be an array");
    // The first stage (inspect_state) must be present and marked failed.
    assert!(!stages.is_empty(), "expected at least one stage entry");
    let first = &stages[0];
    assert_eq!(first["name"].as_str().unwrap(), "inspect_state");
    assert_eq!(first["status"].as_str().unwrap(), "failed");
    assert!(first["duration_ms"].as_i64().unwrap() >= 0);

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
            target: String::new(),
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
        &[("HGX_0", tmp.to_str().unwrap())],
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
    let (mut server, mut client) = start_server().await;

    let tmp = server.make_fake_firmware_file("rms_test_host_creds_switch_host_only.bin");

    // Switch with ONLY host_endpoint credentials (no bmc_endpoint). Under the
    // new host-creds contract this is the canonical case for switch NVOS.
    let device = NodeInfo {
        node_id: "sw-01".into(),
        rack_id: "rack-01".into(),
        r#type: Some(NodeType::SwitchGb200Nvidia as i32),
        bmc_endpoint: None,
        host_endpoint: Some(Endpoint {
            interface: Some(NetworkInterface {
                ip_address: "10.0.0.11".into(),
                mac_address: "aa:bb:cc:00:00:11".into(),
                host_name: None,
            }),
            port: 22,
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
async fn set_scale_up_fabric_state_does_not_echo_http_error_body() {
    let switch_ip =
        non_loopback_local_ip().expect("test requires a non-loopback local IPv4 address");
    let mockup = RedfishSimulator::builder()
        .bind_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .add_gb200_compute(0)
        .start()
        .await;

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
        endpoint.port = u32::from(mockup.ports()[0]);
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
        "HTTP GET /nvue_v1/cluster returned 404"
    );
    assert!(!batch.node_results[0].error_message.contains("not found"));

    mockup.stop();
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
async fn batch_get_node_device_info_rejects_delta_powershelf_stateless() {
    let (mut server, mut client) = start_server().await;

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

    assert_eq!(resp.status, ReturnCode::Failure as i32);
    assert!(
        resp.message
            .contains("build_ephemeral_node not supported for powershelf_gb200_delta nodes"),
        "got: {}",
        resp.message
    );
    assert!(resp.node_device_details.is_empty());
    assert_stats(&resp.stats, 1, 0, 1);

    server.stop();
}
