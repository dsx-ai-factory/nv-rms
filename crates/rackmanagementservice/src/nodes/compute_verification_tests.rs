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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::NodeInstance;
use crate::config::hardware_profiles::HardwareProfileCatalog;
use crate::domain::node::{
    ExpectedInventoryPolicy, FirmwareTarget, Node, NodeRuntimeConfig, NodeType,
};
use crate::domain::rack::{EndpointCredentials, NodeConfig};

const INVENTORY: &str = "/redfish/v1/UpdateService/FirmwareInventory";
const COMPUTE_TYPES: [NodeType; 7] = [
    NodeType::ComputeGb200Nvidia,
    NodeType::ComputeGb200Wiwynn,
    NodeType::ComputeGb200Dell,
    NodeType::ComputeGb300Nvidia,
    NodeType::ComputeGb300Lenovo,
    NodeType::ComputeGb300Supermicro,
    NodeType::ComputeVrnvl72Nvidia,
];

/// Terminate TLS for real node requests, forwarding HTTP to wiremock.
pub(super) struct HttpsInventory {
    pub(super) server: MockServer,
    pub(super) port: u16,
    proxy: JoinHandle<()>,
}

impl HttpsInventory {
    pub(super) async fn start() -> Self {
        let server = MockServer::start().await;
        let upstream = *server.address();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let proxy = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let acceptor = acceptor.clone();
                        connections.spawn(async move {
                            let mut stream = acceptor.accept(stream).await.unwrap();
                            let mut upstream = TcpStream::connect(upstream).await.unwrap();
                            let _ = tokio::io::copy_bidirectional(&mut stream, &mut upstream).await;
                        });
                    }
                    Some(result) = connections.join_next() => result.unwrap(),
                }
            }
        });
        Self {
            server,
            port,
            proxy,
        }
    }
}

impl Drop for HttpsInventory {
    fn drop(&mut self) {
        self.proxy.abort();
    }
}

fn compute_node(node_type: NodeType, port: u16, with_policy: bool) -> NodeInstance {
    let catalog = HardwareProfileCatalog::built_in().unwrap();
    let mut runtime = catalog.runtime_config_for_request(node_type).unwrap();
    let NodeRuntimeConfig::RedfishNvfwupdCompute(compute) = &mut runtime;
    compute.firmware_timing.inventory_recovery_retries = 1;
    compute.firmware_timing.inventory_recovery_retry_interval = Duration::ZERO;
    let mut config = NodeConfig::new("compute", node_type, "127.0.0.1");
    let endpoint = config.bmc_endpoint.as_mut().unwrap();
    endpoint.endpoint.port = port;
    endpoint.dangerously_accept_invalid_certs = true;
    endpoint.credentials = Some(EndpointCredentials::UserPass {
        username: "admin".into(),
        password: "test-password".into(),
    });
    config.runtime_config = Some(runtime);
    config.expected_inventory = with_policy.then(|| ExpectedInventoryPolicy {
        profile: "operator-expectations".into(),
        ap_names: vec!["FW_TEST".into(), "FW_REQUIRED".into()].into(),
        flint_device_counts: Arc::default(),
    });
    NodeInstance::from_config(&config, "rack").unwrap()
}

/// Minimal PLDM package containing version 1.0 for FW_TEST, independent of hardware.
fn firmware_package() -> tempfile::NamedTempFile {
    let mut bytes = vec![
        0xF0, 0x18, 0x87, 0x8C, 0xCB, 0x7D, 0x49, 0x43, 0x98, 0x00, 0xA0, 0x2F, 0x05, 0x9A, 0xCA,
        0x02,
    ];
    bytes.push(1); // Header revision.
    bytes.extend_from_slice(&0u16.to_le_bytes()); // Header size (unused by parser).
    bytes.extend_from_slice(&[0; 13]); // Release date.
    bytes.extend_from_slice(&8u16.to_le_bytes()); // Component bitmap bit length.
    bytes.extend_from_slice(&[1, 4]); // ASCII package version and length.
    bytes.extend_from_slice(b"TEST");
    bytes.push(1); // Device record count.
    bytes.extend_from_slice(&0u16.to_le_bytes()); // Record length.
    bytes.push(0); // Descriptor count.
    bytes.extend_from_slice(&0u32.to_le_bytes()); // Update flags.
    bytes.extend_from_slice(&[1, 7]); // ASCII AP name and length.
    bytes.extend_from_slice(&0u16.to_le_bytes()); // Package data length.
    bytes.push(1); // Select component zero.
    bytes.extend_from_slice(b"FW_TEST");
    bytes.extend_from_slice(&1u16.to_le_bytes()); // Component count.
    let payload_offset = bytes.len() as u32 + 22 + 3 + 4;
    bytes.extend_from_slice(&0x000Au16.to_le_bytes()); // Firmware classification.
    bytes.extend_from_slice(&1u16.to_le_bytes()); // Component identifier.
    bytes.extend_from_slice(&0u32.to_le_bytes()); // Comparison stamp.
    bytes.extend_from_slice(&0u16.to_le_bytes()); // Options.
    bytes.extend_from_slice(&1u16.to_le_bytes()); // Activation method.
    bytes.extend_from_slice(&payload_offset.to_le_bytes());
    bytes.extend_from_slice(&4u32.to_le_bytes()); // Payload length.
    bytes.extend_from_slice(&[1, 3]);
    bytes.extend_from_slice(b"1.0");
    bytes.extend_from_slice(&0u32.to_le_bytes()); // Header checksum.
    bytes.extend_from_slice(b"TEST");
    let file = tempfile::Builder::new()
        .suffix(".fwpkg")
        .tempfile()
        .unwrap();
    std::fs::write(file.path(), bytes).unwrap();
    file
}

#[tokio::test]
async fn all_compute_wrappers_enforce_expected_inventory_and_retry_recovery() {
    let package = firmware_package();
    let targets = [FirmwareTarget {
        component: "FW_TEST".into(),
        firmware_file: package.path().to_string_lossy().into_owned(),
        ..FirmwareTarget::default()
    }];
    for node_type in COMPUTE_TYPES {
        // The package and installed FW_TEST versions match in every case. Only
        // the independently configured FW_REQUIRED inventory entry is delayed.
        for (with_policy, recovers, expected_reads) in
            [(true, false, 2), (true, true, 3), (false, false, 1)]
        {
            let mock = HttpsInventory::start().await;
            let reads = Arc::new(AtomicUsize::new(0));
            Mock::given(method("GET"))
                .and(path(INVENTORY))
                .respond_with(move |_: &wiremock::Request| {
                    let mut members = vec![json!({"@odata.id": format!("{INVENTORY}/FW_TEST")})];
                    if reads.fetch_add(1, Ordering::SeqCst) > 0 && recovers {
                        members.push(json!({"@odata.id": format!("{INVENTORY}/FW_REQUIRED")}));
                    }
                    ResponseTemplate::new(200).set_body_json(json!({"Members": members}))
                })
                .expect(expected_reads)
                .mount(&mock.server)
                .await;
            for name in ["FW_TEST", "FW_REQUIRED"] {
                Mock::given(method("GET"))
                    .and(path(format!("{INVENTORY}/{name}")))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "@odata.id": format!("{INVENTORY}/{name}"),
                        "Id": name, "Version": "1.0", "Updateable": true,
                    })))
                    .mount(&mock.server)
                    .await;
            }
            let node = compute_node(node_type, mock.port, with_policy);
            let result = node.verify_firmware_package_versions(&targets).await;
            if with_policy && !recovers {
                let error =
                    result.expect_err("missing operator-required inventory must fail verification");
                assert!(
                    error.to_string().contains("FW_REQUIRED"),
                    "{node_type:?}: {error}"
                );
            } else {
                let summary = result.unwrap_or_else(|error| panic!("{node_type:?}: {error}"));
                assert!(summary.matched, "{node_type:?}: {summary:?}");
                assert_ne!(summary.details["status"], "unverified");
            }
            mock.server.verify().await;
        }
    }
}
