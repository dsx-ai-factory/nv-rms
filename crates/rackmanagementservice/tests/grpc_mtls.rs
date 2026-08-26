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

//! End-to-end mTLS tests for the gRPC server.
//!
//! Spawns `GrpcServer` with a client CA configured and exercises the full
//! stack against it with a tonic client: rustls TLS 1.3 handshake, client-cert
//! verification against the server's configured CA, ALPN to h2, and a real
//! RPC round-trip.

use std::sync::Arc;
use std::time::Duration;

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tempfile::TempDir;
use tonic::transport::{Certificate as TonicCertificate, ClientTlsConfig, Endpoint, Identity};

use librms::protos::rack_manager::rack_manager_client::RackManagerClient;
use librms::protos::rack_manager::{self as rm};
use rackmanagementservice::api::grpc::server::{GrpcServer, TlsConfig, TlsMode};
use rackmanagementservice::metrics;
use rackmanagementservice::orchestrator::job_tracker::JobTracker;
use rackmanagementservice::orchestrator::rack_manager::RackManager;
use rackmanagementservice::persistence::Backends;

// ── Crypto provider (tests don't go through `main()`, so install it here) ──

fn ensure_crypto_provider() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

// ── Certificate generation ──

/// A generated CA plus helpers to issue leaf certs under it.
struct TestCa {
    cert: Certificate,
    key: KeyPair,
}

impl TestCa {
    fn new(name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        Self { cert, key }
    }

    fn issue(
        &self,
        sans: Vec<String>,
        cn: &str,
        eku: ExtendedKeyUsagePurpose,
    ) -> (Certificate, KeyPair) {
        let mut params = CertificateParams::new(sans).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.extended_key_usages = vec![eku];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.cert, &self.key).unwrap();
        (cert, key)
    }

    fn ca_pem(&self) -> String {
        self.cert.pem()
    }
}

/// Paths + PEMs for a fully-populated mTLS trust domain, rooted at a tempdir.
struct MtlsFixture {
    _tmp: TempDir,
    server_cert_path: std::path::PathBuf,
    server_key_path: std::path::PathBuf,
    ca_path: std::path::PathBuf,
    ca_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
    bad_client_cert_pem: String,
    bad_client_key_pem: String,
}

fn build_fixture() -> MtlsFixture {
    let tmp = tempfile::tempdir().unwrap();
    let ca = TestCa::new("rms-test-ca");

    // Server cert: must cover localhost + 127.0.0.1 for hostname verification.
    let (server_cert, server_key) = ca.issue(
        vec!["localhost".into(), "127.0.0.1".into()],
        "rms-test-server",
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    // Client cert: signed by the same CA.
    let (client_cert, client_key) = ca.issue(
        vec!["rms-test-client".into()],
        "rms-test-client",
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    // Bad client: signed by a DIFFERENT CA that the server does not trust.
    let bad_ca = TestCa::new("rms-bad-ca");
    let (bad_client_cert, bad_client_key) = bad_ca.issue(
        vec!["rms-bad-client".into()],
        "rms-bad-client",
        ExtendedKeyUsagePurpose::ClientAuth,
    );

    let server_cert_path = tmp.path().join("server.pem");
    let server_key_path = tmp.path().join("server.key");
    let ca_path = tmp.path().join("ca.pem");
    std::fs::write(&server_cert_path, server_cert.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
    std::fs::write(&ca_path, ca.ca_pem()).unwrap();

    MtlsFixture {
        _tmp: tmp,
        server_cert_path,
        server_key_path,
        ca_path,
        ca_pem: ca.ca_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
        bad_client_cert_pem: bad_client_cert.pem(),
        bad_client_key_pem: bad_client_key.serialize_pem(),
    }
}

// ── Server lifecycle ──

async fn start_mtls_server(fx: &MtlsFixture) -> (GrpcServer, u16) {
    ensure_crypto_provider();

    let rm = Arc::new(RackManager::new());
    let jt = Arc::new(JobTracker::new());
    let port = portpicker::pick_unused_port().expect("no free port");
    let _metrics_registry = metrics::init(&metrics::MetricsInfo {
        grpc_port: port,
        firmware_dir: "firmware".to_string(),
        persistence_type: "memory".to_string(),
        tls_mode: TlsMode::Mtls,
    })
    .unwrap();

    let tls = TlsConfig::resolve(
        fx.server_cert_path.to_str(),
        fx.server_key_path.to_str(),
        fx.ca_path.to_str(),
        None,
    )
    .expect("failed to resolve server TLS material")
    .expect("mTLS fixture always provides cert + key");

    let mut server = GrpcServer::new(port, rm, jt, Backends::memory()).with_tls(tls);
    server.start().await.unwrap();

    // Give the listener a beat to actually bind.
    tokio::time::sleep(Duration::from_millis(150)).await;

    (server, port)
}

fn endpoint(port: u16) -> Endpoint {
    Endpoint::from_shared(format!("https://localhost:{port}"))
        .unwrap()
        .timeout(Duration::from_secs(3))
        .connect_timeout(Duration::from_secs(3))
}

fn client_tls_with_identity(ca_pem: &str, cert_pem: &str, key_pem: &str) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .ca_certificate(TonicCertificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name("localhost")
}

// ── Tests ──

/// Happy path: correct CA + correct client identity -> RPC succeeds.
#[tokio::test]
async fn mtls_happy_path_rpc_succeeds() {
    let fx = build_fixture();
    let (mut server, port) = start_mtls_server(&fx).await;

    let tls = client_tls_with_identity(&fx.ca_pem, &fx.client_cert_pem, &fx.client_key_pem);
    let channel = endpoint(port)
        .tls_config(tls)
        .expect("client tls config")
        .connect()
        .await
        .expect("client should be able to connect with correct certs");

    let mut client = RackManagerClient::new(channel);
    let resp = client
        .list_racks(tonic::Request::new(rm::ListRacksRequest {}))
        .await
        .expect("list_racks RPC should succeed over mTLS");

    // Empty RackManager → empty list, but a clean response at all proves the
    // encrypted round-trip worked end to end.
    assert!(resp.into_inner().rack_ids.is_empty());

    server.stop_without_awaiting_terminal();
}

/// Tonic's `Endpoint::connect()` is lazy — the handshake is deferred until the
/// first RPC. So for the rejection cases we need to actually send a request
/// and observe *that* failing. If `connect()` errors we accept that too, since
/// some TLS errors surface there.
async fn expect_rpc_rejected(endpoint: Endpoint, scenario: &str) {
    let channel = match endpoint.connect().await {
        Ok(c) => c,
        Err(_) => return, // handshake already refused at connect time
    };

    let mut client = RackManagerClient::new(channel);
    let res = client
        .list_racks(tonic::Request::new(rm::ListRacksRequest {}))
        .await;

    assert!(
        res.is_err(),
        "expected mTLS to reject {scenario}, but the RPC succeeded: {res:?}"
    );
}

/// Client trusts the server CA but presents no client identity.
/// Server demands a client cert, so the handshake fails.
#[tokio::test]
async fn mtls_rejects_client_without_identity() {
    let fx = build_fixture();
    let (mut server, port) = start_mtls_server(&fx).await;

    let tls = ClientTlsConfig::new()
        .ca_certificate(TonicCertificate::from_pem(&fx.ca_pem))
        .domain_name("localhost");
    let ep = endpoint(port).tls_config(tls).expect("client tls config");

    expect_rpc_rejected(ep, "a client with no certificate").await;

    server.stop_without_awaiting_terminal();
}

/// Client presents a client cert signed by an unrelated CA the server
/// doesn't trust. Server rejects the chain during the handshake.
#[tokio::test]
async fn mtls_rejects_client_cert_signed_by_unknown_ca() {
    let fx = build_fixture();
    let (mut server, port) = start_mtls_server(&fx).await;

    let tls = client_tls_with_identity(&fx.ca_pem, &fx.bad_client_cert_pem, &fx.bad_client_key_pem);
    let ep = endpoint(port).tls_config(tls).expect("client tls config");

    expect_rpc_rejected(ep, "a client cert signed by an untrusted CA").await;

    server.stop_without_awaiting_terminal();
}
