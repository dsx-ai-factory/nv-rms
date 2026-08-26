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

//! Scale benchmarks — measures Rust service performance at 100–10k node scale
//! using an in-process Redfish simulator. Large benchmark cases share a
//! using an in-process Redfish simulator. Each node is backed by its own
//! dedicated OS-assigned listener; large benchmark cases therefore require an
//! elevated file-descriptor limit (see `ulimit` note below).
//!
//! Environment note: Several large scale benchmarks require an elevated ulimit
//! to avoid hitting process file descriptor limits. This can be set with:
//!
//! ```bash
//! ulimit -n 100000
//! ```

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use librms::protos::rack_manager;
use librms::protos::rack_manager::rack_manager_client::RackManagerClient;
use rack_manager::*;
use rackmanagementservice::api::grpc::server::{GrpcServer, TlsMode};
use rackmanagementservice::metrics::{self, MetricsInfo};
use rackmanagementservice::orchestrator::job_tracker::JobTracker;
use rackmanagementservice::orchestrator::rack_manager::RackManager;
use rackmanagementservice::persistence::Backends;
use redfish_test_support::{RedfishFixture, RedfishSimulator};

const COMPUTE_HOST: &str = "127.0.0.1";

/// Job-tracker capacity for benchmarks that never create tracked jobs
/// (node registration, power-state, firmware inventory). Mirrors the
/// production default so those paths exercise the real configuration.
const DEFAULT_BENCH_MAX_TRACKED_JOBS: usize = 10_000;

/// Effectively unbounded job-tracker capacity for the firmware-update
/// benchmark, whose reused server accumulates job records across every
/// criterion iteration.
const UNBOUNDED_BENCH_MAX_TRACKED_JOBS: usize = usize::MAX;

fn write_synthetic_compute_fwpkg(path: &Path) {
    const EXPECTED_UUID: [u8; 16] = [
        0xf0, 0x18, 0x87, 0x8c, 0xcb, 0x7d, 0x49, 0x43, 0x98, 0x00, 0xa0, 0x2f, 0x05, 0x9a, 0xca,
        0x02,
    ];

    let version = b"GB200-P4978_BENCH";
    let device_version = b"BMC";
    let component_version = b"1.0.0";
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

// ── Shared helpers ──

// Build a compute node pointing to a simulator BMC port.
fn make_node(id: usize, port: u16) -> NodeInfo {
    let mac = format!(
        "aa:bb:cc:{:02x}:{:02x}:{:02x}",
        id >> 16,
        (id >> 8) & 0xff,
        id & 0xff
    );
    NodeInfo {
        node_id: format!("c-{id:05}"),
        rack_id: "rack-bench".into(),
        r#type: Some(NodeType::ComputeGb200Nvidia as i32),
        bmc_endpoint: Some(Endpoint {
            interface: Some(NetworkInterface {
                ip_address: COMPUTE_HOST.into(),
                mac_address: mac,
                host_name: None,
            }),
            port: u32::from(port),
            credentials: Some(Credentials {
                auth: Some(credentials::Auth::UserPass(UsernamePassword {
                    username: "admin".into(),
                    password: "password".into(),
                })),
            }),
        }),
        host_endpoint: None,
        node_descriptor: None,
    }
}

// Spin up a fresh RMS gRPC server and connect a client to it.
//
// `max_tracked_jobs` bounds the job tracker. The firmware-update benchmark
// reuses one server across every criterion iteration, and terminal jobs are
// retained (24h TTL) rather than reaped mid-run, so job records accumulate
// across iterations. A small cap (the 10k production default) is exhausted
// well before the benchmark finishes, so callers that create jobs pass a much
// larger bound.
async fn start_grpc_server(
    max_tracked_jobs: usize,
) -> (GrpcServer, RackManagerClient<tonic::transport::Channel>) {
    let _metrics_registry = metrics::init(&MetricsInfo {
        grpc_port: 0,
        firmware_dir: String::new(),
        persistence_type: "memory".to_string(),
        tls_mode: TlsMode::Insecure,
    })
    .unwrap();
    let rm = Arc::new(RackManager::new());
    let jt = Arc::new(
        JobTracker::builder()
            .max_tracked_jobs(max_tracked_jobs)
            .build()
            .unwrap(),
    );
    let port = portpicker::pick_unused_port().expect("no free port");

    let tmp_fw_dir = std::env::temp_dir();
    let mut server = GrpcServer::new(port, rm, jt, Backends::memory())
        .with_firmware_dir(tmp_fw_dir)
        .with_insecure_listener();
    server.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let addr = format!("http://127.0.0.1:{port}");
    let client = RackManagerClient::connect(addr).await.unwrap();
    (server, client)
}

// Start the Redfish simulator with N logical node ports.
async fn start_mockup(n: usize) -> RedfishSimulator {
    start_mockup_with_task_delay(n, 1.0).await
}

async fn start_mockup_with_task_delay(n: usize, task_delay_seconds: f32) -> RedfishSimulator {
    // Firmware updates need one dedicated listener per node: several logical
    // nodes sharing a simulated BMC would collide on its single TaskService,
    // and the preflight check would reject the concurrent updates as
    // "already running".
    RedfishSimulator::builder()
        .with_task_delay(task_delay_seconds)
        .never_fail_firmware_updates()
        .add_auto_ports(n, RedfishFixture::Gb200Compute)
        .start()
        .await
}

// Register N nodes via gRPC in batches of 500, using actual mockup ports
async fn add_n_nodes(
    client: &mut RackManagerClient<tonic::transport::Channel>,
    n: usize,
    ports: &[u16],
) {
    let batch_size = 500;
    for start in (0..n).step_by(batch_size) {
        let end = (start + batch_size).min(n);
        let nodes: Vec<_> = (start..end).map(|i| make_node(i, ports[i])).collect();
        let resp = client
            .create_nodes(CreateNodesRequest {
                nodes: Some(NodeSet { nodes }),
            })
            .await
            .unwrap()
            .into_inner();
        let response = resp.response.expect("CreateNodesResponse missing response");
        assert_eq!(
            response.status,
            ReturnCode::Success as i32,
            "CreateNodes failed: {}",
            response.message
        );
    }
}

// ── Benchmark 1: Node registration throughput ──
// Measures: gRPC overhead + rack/node creation in memory (no Redfish calls)
// Production scenario: initial rack provisioning
// No mockup needed — CreateNodes stores node config but never contacts the BMC.

fn bench_add_nodes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("add_nodes");
    group.sample_size(10);

    for n in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async {
                // Each iteration: fresh server → register all nodes → tear down
                let (mut server, mut client) =
                    start_grpc_server(DEFAULT_BENCH_MAX_TRACKED_JOBS).await;
                let dummy_ports: Vec<u16> = (0..n).map(|_| 443).collect();
                add_n_nodes(&mut client, n, &dummy_ports).await;
                server.stop_without_awaiting_terminal();
            });
        });
    }

    group.finish();
}

// ── Benchmark 2: Concurrent GetPowerState ──
// Measures: N parallel gRPC calls, each triggering an HTTPS GET to a BMC endpoint
// Production scenario: monitoring dashboard polling power state of all nodes

fn bench_concurrent_power_state(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_power_state");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for n in [100, 1_000, 10_000] {
        // Setup: start mockup + gRPC server, pre-register all nodes (not measured)
        let (mut server, client, mockup) = rt.block_on(async {
            let mockup = start_mockup(n).await;
            let (server, mut client) = start_grpc_server(DEFAULT_BENCH_MAX_TRACKED_JOBS).await;
            add_n_nodes(&mut client, n, mockup.ports()).await;
            (server, client, mockup)
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                async move {
                    // Fire N concurrent GetPowerState RPCs (one per node)
                    let handles: Vec<_> = (0..n)
                        .map(|i| {
                            let mut c = client.clone();
                            tokio::spawn(async move {
                                // Each call: gRPC → handler → HTTPS GET to BMC → parse JSON →
                                // return
                                c.get_power_state(GetPowerStateRequest {
                                    node_id: format!("c-{i:05}"),
                                    rack_id: "rack-bench".into(),
                                })
                                .await
                                .unwrap()
                                .into_inner()
                            })
                        })
                        .collect();
                    // Wait for all N power state queries to complete
                    futures::future::join_all(handles).await
                }
            });
        });

        rt.block_on(async {
            server.stop_without_awaiting_terminal();
            mockup.stop();
        });
    }

    group.finish();
}

// ── Benchmark 3: Concurrent GetNodeFirmwareInventory ──
// Measures: N parallel gRPC calls, each triggering ~20 sequential HTTPS GETs per node
// Production scenario: pre-update audit of firmware versions across the fleet

fn bench_concurrent_firmware_inventory(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_firmware_inventory");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    for n in [100, 1_000, 10_000] {
        // Setup: start mockup + gRPC server, pre-register all nodes (not measured)
        let (mut server, client, mockup) = rt.block_on(async {
            let mockup = start_mockup(n).await;
            let (server, mut client) = start_grpc_server(DEFAULT_BENCH_MAX_TRACKED_JOBS).await;
            add_n_nodes(&mut client, n, mockup.ports()).await;
            (server, client, mockup)
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                async move {
                    // Fire N concurrent firmware inventory queries
                    let handles: Vec<_> = (0..n)
                        .map(|i| {
                            let mut c = client.clone();
                            tokio::spawn(async move {
                                // Each call: gRPC → handler → GET collection + GET each member →
                                // aggregate
                                c.get_node_firmware_inventory(GetNodeFirmwareInventoryRequest {
                                    node_id: format!("c-{i:05}"),
                                    rack_id: "rack-bench".into(),
                                })
                                .await
                                .unwrap()
                                .into_inner()
                            })
                        })
                        .collect();
                    // Wait for all N inventory queries to complete
                    futures::future::join_all(handles).await
                }
            });
        });

        rt.block_on(async {
            server.stop_without_awaiting_terminal();
            mockup.stop();
        });
    }

    group.finish();
}

// ── Benchmark 4: Parallel firmware updates ──
// Measures: async job lifecycle — N concurrent uploads + task polling + status tracking
// Production scenario: fleet-wide firmware push ("update BMC on all 100 nodes")

fn bench_parallel_firmware_updates(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel_firmware_updates");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    // Create a minimal PLDM package so the NVFWUPD-backed update path measures
    // a successful parse/upload flow instead of the fast parser-failure path.
    let tmp = std::env::temp_dir().join("rms_bench_fw.fwpkg");
    write_synthetic_compute_fwpkg(&tmp);
    let fw_path = tmp.to_str().unwrap().to_owned();

    for n in [100, 1_000, 10_000] {
        // Setup: start mockup with fast task completion (0.1s), pre-register nodes
        let (mut server, client, mockup) = rt.block_on(async {
            let mockup = start_mockup_with_task_delay(n, 0.1).await;
            let (server, mut client) = start_grpc_server(UNBOUNDED_BENCH_MAX_TRACKED_JOBS).await;
            add_n_nodes(&mut client, n, mockup.ports()).await;
            (server, client, mockup)
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let fw = fw_path.clone();
            b.to_async(&rt).iter(|| {
                let mut client = client.clone();
                let fw = fw.clone();
                async move {
                    // Phase 1: fire N concurrent async firmware update requests
                    let handles: Vec<_> = (0..n)
                        .map(|i| {
                            let mut c = client.clone();
                            let fw = fw.clone();
                            tokio::spawn(async move {
                                // Returns immediately with a job ID; actual upload runs in
                                // background
                                c.update_firmware(UpdateFirmwareRequest {
                                    node_id: format!("c-{i:05}"),
                                    rack_id: "rack-bench".into(),
                                    filename: fw,
                                    target: String::new(),
                                    activate: false,
                                    force_update: false,
                                    firmware_targets: vec![],
                                })
                                .await
                                .unwrap()
                                .into_inner()
                            })
                        })
                        .collect();
                    // Collect all job IDs from the async responses
                    let responses = futures::future::join_all(handles).await;
                    let job_ids: Vec<String> = responses
                        .into_iter()
                        .map(|r| {
                            let r = r.unwrap();
                            assert_eq!(
                                r.status,
                                ReturnCode::Success as i32,
                                "UpdateNodeFirmwareAsync failed: code={} msg={}",
                                r.error_code,
                                r.message
                            );
                            assert!(!r.job_id.is_empty(), "empty job_id with success status");
                            r.job_id
                        })
                        .collect();

                    // Phase 2: poll all jobs until they reach terminal state
                    // (background tasks: upload → poll BMC task → mark complete)
                    loop {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        let mut all_done = true;
                        for jid in &job_ids {
                            let job = client
                                .get_firmware_job_status(GetFirmwareJobStatusRequest {
                                    job_id: jid.clone(),
                                })
                                .await
                                .unwrap()
                                .into_inner();
                            // Check if job reached a terminal state (completed or failed)
                            let terminal = job.job_state == FirmwareJobState::Completed as i32
                                || job.job_state == FirmwareJobState::Failed as i32;
                            if !terminal {
                                all_done = false;
                                break;
                            } else {
                                assert!(
                                    job.job_state == FirmwareJobState::Completed as i32,
                                    "Failed firmware update for job {jid} with error: {}",
                                    job.error_message
                                );
                            }
                        }
                        if all_done {
                            break;
                        }
                    }
                }
            });
        });

        rt.block_on(async {
            server.stop_without_awaiting_terminal();
            mockup.stop();
        });
    }

    let _ = std::fs::remove_file(&tmp);
    group.finish();
}

criterion_group!(
    benches,
    bench_add_nodes,
    bench_concurrent_power_state,
    bench_concurrent_firmware_inventory,
    bench_parallel_firmware_updates,
);
criterion_main!(benches);
