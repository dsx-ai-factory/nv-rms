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

//! Transport-level tests for scale-up fabric reconciliation.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;
use crate::api::grpc::server::SwitchTlsRoots;
use crate::libnmxc::{nmxc_model, test_support::FakeNmxc};
use crate::nodes::nvos::NvBridge;
use crate::orchestrator::job_lifecycle::JobState;
use crate::orchestrator::job_tracker::JobTracker;
use crate::orchestrator::rack_manager::RackManager;
use crate::persistence::Backends;

#[derive(Clone)]
struct TestNmxController {
    static_config: nmxc_model::NmxGetStaticConfigResponse,
    hello_calls: Arc<AtomicUsize>,
}

impl<B> tonic::codegen::Service<http::Request<B>> for TestNmxController
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        match request.uri().path() {
            "/nmx_c.NMX_Controller/Hello" => {
                struct Hello {
                    hello_calls: Arc<AtomicUsize>,
                }

                impl tonic::server::UnaryService<nmxc_model::NmxHelloRequest> for Hello {
                    type Response = nmxc_model::NmxHelloResponse;
                    type Future =
                        tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

                    fn call(
                        &mut self,
                        _request: tonic::Request<nmxc_model::NmxHelloRequest>,
                    ) -> Self::Future {
                        self.hello_calls.fetch_add(1, Ordering::SeqCst);
                        let server_header = FakeNmxc::success_return_code().server_header;

                        Box::pin(async move {
                            Ok(tonic::Response::new(nmxc_model::NmxHelloResponse {
                                server_header,
                            }))
                        })
                    }
                }

                let hello = Hello {
                    hello_calls: self.hello_calls.clone(),
                };
                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);

                    Ok(grpc.unary(hello, request).await)
                })
            }
            "/nmx_c.NMX_Controller/GetDomainStateInfo" => {
                struct GetDomainStateInfo;

                impl tonic::server::UnaryService<nmxc_model::NmxGetDomainStateInfoRequest> for GetDomainStateInfo {
                    type Response = nmxc_model::NmxDomainStateInfo;
                    type Future =
                        tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

                    fn call(
                        &mut self,
                        _request: tonic::Request<nmxc_model::NmxGetDomainStateInfoRequest>,
                    ) -> Self::Future {
                        Box::pin(async {
                            Ok(tonic::Response::new(nmxc_model::NmxDomainStateInfo {
                                server_header: FakeNmxc::success_return_code().server_header,
                                control_plane_state: 3,
                                nmx_controller_health: 3,
                                ..Default::default()
                            }))
                        })
                    }
                }

                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);

                    Ok(grpc.unary(GetDomainStateInfo, request).await)
                })
            }
            "/nmx_c.NMX_Controller/GetStaticConfig" => {
                struct GetStaticConfig {
                    response: nmxc_model::NmxGetStaticConfigResponse,
                }

                impl tonic::server::UnaryService<nmxc_model::NmxGetStaticConfigRequest> for GetStaticConfig {
                    type Response = nmxc_model::NmxGetStaticConfigResponse;
                    type Future =
                        tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

                    fn call(
                        &mut self,
                        _request: tonic::Request<nmxc_model::NmxGetStaticConfigRequest>,
                    ) -> Self::Future {
                        let response = self.response.clone();
                        Box::pin(async move { Ok(tonic::Response::new(response)) })
                    }
                }

                let response = self.static_config.clone();

                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    let method = GetStaticConfig { response };

                    Ok(grpc.unary(method, request).await)
                })
            }
            _ => Box::pin(async move {
                let mut response = http::Response::new(tonic::body::Body::default());

                response.headers_mut().insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );

                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );

                Ok(response)
            }),
        }
    }
}

impl tonic::server::NamedService for TestNmxController {
    const NAME: &'static str = "nmx_c.NMX_Controller";
}

// Production NMX-C clients require port 9370. Serialize fixture owners within
// this test process; the selected local address must also be free externally.
static TEST_NMX_CONTROLLER_PORT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct TestNmxControllerServer {
    task: tokio::task::JoinHandle<()>,
    hello_calls: Arc<AtomicUsize>,
    _port_guard: tokio::sync::MutexGuard<'static, ()>,
}

impl TestNmxControllerServer {
    async fn start(bind_ip: IpAddr, topology_type: &str) -> Self {
        let port_guard = TEST_NMX_CONTROLLER_PORT_LOCK.lock().await;

        let static_config = FakeNmxc::static_config_files_response(&[
            (
                NMX_C_FM_CONFIG_FILE,
                &format!("{NMX_C_TOPOLOGY_KEY}={topology_type}\n"),
            ),
            (NMX_C_SM_CONFIG_FILE, ""),
        ]);

        let incoming = bind_test_nmx_controller(bind_ip).await;

        let hello_calls = Arc::new(AtomicUsize::new(0));
        let service = TestNmxController {
            static_config,
            hello_calls: hello_calls.clone(),
        };
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .expect("run test NMX Controller");
        });

        Self {
            task,
            hello_calls,
            _port_guard: port_guard,
        }
    }

    async fn start_mtls_with_first_connect_failure(
        bind_ip: IpAddr,
        material_root: &std::path::Path,
    ) -> (Self, Arc<AtomicUsize>) {
        use futures::StreamExt;
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, KeyUsagePurpose};

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let client_key = KeyPair::generate().unwrap();

        let client = CertificateParams::new(vec!["rms-client".into()])
            .unwrap()
            .signed_by(&client_key, &ca, &ca_key)
            .unwrap();

        let server_key = KeyPair::generate().unwrap();

        let server_cert = CertificateParams::new(vec!["nmx-controller.example.com".into()])
            .unwrap()
            .signed_by(&server_key, &ca, &ca_key)
            .unwrap();

        let domain_dir = material_root.join("fabric-a");
        tokio::fs::create_dir(&domain_dir).await.unwrap();

        for (name, pem) in [
            ("ca.pem", ca.pem()),
            ("client.pem", client.pem()),
            ("client.key", client_key.serialize_pem()),
        ] {
            tokio::fs::write(domain_dir.join(name), pem).await.unwrap();
        }

        let port_guard = TEST_NMX_CONTROLLER_PORT_LOCK.lock().await;
        let tls_attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = Arc::clone(&tls_attempts);

        let incoming = bind_test_nmx_controller(bind_ip)
            .await
            .filter_map(move |stream| {
                let attempts = Arc::clone(&observed_attempts);

                async move {
                    if let Ok(socket) = &stream {
                        let mut first_byte = [0];

                        // Readiness uses plaintext HTTP. Fail the first TLS ClientHello
                        // after TCP succeeds, then let the retry complete mTLS.
                        if socket.peek(&mut first_byte).await.ok() == Some(1)
                            && first_byte[0] == 0x16
                            && attempts.fetch_add(1, Ordering::SeqCst) == 0
                        {
                            return None;
                        }
                    }

                    Some(stream)
                }
            });

        let hello_calls = Arc::new(AtomicUsize::new(0));

        let controller = TestNmxController {
            static_config: FakeNmxc::static_config_files_response(&[]),
            hello_calls: Arc::clone(&hello_calls),
        };

        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .tls_config(
                    tonic::transport::ServerTlsConfig::new()
                        .identity(tonic::transport::Identity::from_pem(
                            server_cert.pem(),
                            server_key.serialize_pem(),
                        ))
                        .client_ca_root(tonic::transport::Certificate::from_pem(ca.pem())),
                )
                .unwrap()
                .add_service(controller)
                .serve_with_incoming(Box::pin(incoming))
                .await
                .expect("run mTLS test NMX Controller");
        });

        let server = Self {
            task,
            hello_calls,
            _port_guard: port_guard,
        };

        (server, tls_attempts)
    }
}

async fn bind_test_nmx_controller(bind_ip: IpAddr) -> tonic::transport::server::TcpIncoming {
    let addr = SocketAddr::from((bind_ip, config::GRPC_PORT_NMX_CONTROLLER));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);

    loop {
        match tonic::transport::server::TcpIncoming::bind(addr) {
            Ok(incoming) => return incoming,
            Err(error)
                if error.kind() == std::io::ErrorKind::AddrInUse
                    && std::time::Instant::now() < deadline =>
            {
                // JoinHandle::abort() does not close the previous listener until
                // the runtime polls it; yield so a prior fixture can release 9370.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("bind test NMX Controller: {error:?}"),
        }
    }
}

impl Drop for TestNmxControllerServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) struct WiremockHttpsProxy {
    pub(super) port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl WiremockHttpsProxy {
    pub(super) async fn start(bind_ip: IpAddr, upstream: SocketAddr) -> Self {
        use rcgen::generate_simple_self_signed;
        use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
        use tokio::io::copy_bidirectional;
        use tokio::net::{TcpListener, TcpStream};
        use tokio_rustls::TlsAcceptor;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let cert = generate_simple_self_signed(vec!["localhost".into()])
            .expect("generate NVUE test certificate");

        let cert_der = CertificateDer::from(cert.cert.der().to_vec());
        let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

        let mut tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der.into())
            .expect("build NVUE test TLS config");

        tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];

        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

        let listener = TcpListener::bind(SocketAddr::from((bind_ip, 0)))
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

pub(super) fn insecure_fabric_service() -> RackManagerServiceImpl {
    RackManagerServiceImpl {
        rack_manager: Arc::new(RackManager::new()),
        job_tracker: Arc::new(JobTracker::new()),
        firmware_download_cancellations: Arc::new(std::sync::Mutex::new(HashMap::new())),
        backends: Backends::memory(),
        firmware_dir: std::path::PathBuf::from("firmware"),
        artifact_require_sha256: false,
        insecure_bmc: true,
        artifact_download_policy:
            crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
        switch_tls_roots: SwitchTlsRoots {
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        },
        sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
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

pub(super) fn vrnvl72_switch(node_id: &str, switch_ip: IpAddr, port: u16) -> rm::NodeInfo {
    rm::NodeInfo {
        node_id: node_id.into(),
        rack_id: "rack-01".into(),
        r#type: Some(rm::NodeType::SwitchVrnvl72Nvidia as i32),
        host_endpoint: Some(rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: switch_ip.to_string(),
                mac_address: String::new(),
                host_name: None,
            }),
            port: u32::from(port),
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "admin".into(),
                    password: "password".into(),
                })),
            }),
        }),
        ..Default::default()
    }
}

fn with_secondary_address(mut node: rm::NodeInfo, address: &str) -> rm::NodeInfo {
    node.additional_host_endpoints.push(rm::Endpoint {
        interface: Some(rm::NetworkInterface {
            ip_address: address.into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    node
}

pub(super) fn non_loopback_local_ip() -> IpAddr {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
        .expect("bind local address probe");

    socket
        .connect(SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 80)))
        .expect("select local interface");

    let ip = socket.local_addr().expect("read local interface").ip();

    assert!(!super::super::is_disallowed_switch_target_ip(ip));

    ip
}

/// Slow discovery must not suppress a responsive controller's reported failure.
#[tokio::test]
async fn inspection_retains_controller_samples_when_discovery_reaches_deadline() {
    use wiremock::matchers::any;

    let switch_ip = non_loopback_local_ip();
    let _controller = TestNmxControllerServer::start(switch_ip, "test-topology").await;
    let service = insecure_fabric_service();

    for stalled in ["compute", "other-switch", "primary-identity"] {
        let nvue = MockServer::start().await;
        let slow = MockServer::start().await;
        let nvue_proxy = WiremockHttpsProxy::start(switch_ip, *nvue.address()).await;
        let slow_proxy = WiremockHttpsProxy::start(switch_ip, *slow.address()).await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"state": "enabled"})),
            )
            .mount(&nvue)
            .await;

        let identity = ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chassis-sn": "inspection-chassis", "tray-index": "1", "slot-number": "2"
        }));

        let identity = if stalled == "primary-identity" {
            identity.set_delay(Duration::from_secs(20))
        } else {
            identity
        };

        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/chassis-location"))
            .respond_with(identity)
            .mount(&nvue)
            .await;

        Mock::given(any())
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(20)))
            .mount(&slow)
            .await;

        let mut nodes = vec![vrnvl72_switch("primary", switch_ip, nvue_proxy.port)];

        if stalled != "primary-identity" {
            let mut node = vrnvl72_switch(stalled, switch_ip, slow_proxy.port);

            if stalled == "compute" {
                node.r#type = Some(rm::NodeType::ComputeGb200Nvidia.into());
                node.bmc_endpoint = node.host_endpoint.take();

                node.bmc_endpoint
                    .as_mut()
                    .unwrap()
                    .interface
                    .as_mut()
                    .unwrap()
                    .mac_address = "02:00:00:00:00:01".into();
            }

            nodes.push(node);
        }

        let mut request = tonic::Request::new(rm::InspectScaleUpFabricsRequest {
            nodes: Some(rm::NodeSet { nodes }),
            ..Default::default()
        });

        // Allow TLS and controller reads to finish under coverage instrumentation.
        // The 20-second mock delays must still exceed the shared deadline.
        request.set_timeout(Duration::from_secs(10));

        let response = tokio::time::timeout(
            Duration::from_secs(15),
            service.handle_inspect_scale_up_fabrics(request),
        )
        .await
        .expect("inspection must obey the shared deadline")
        .unwrap()
        .into_inner();

        assert_eq!(response.fabrics.len(), 1, "{stalled}: {response:?}");

        let fabric = &response.fabrics[0];

        assert_eq!(fabric.primary_switch_node_id, "primary");

        assert_eq!(
            fabric.control_plane_state,
            rm::ScaleUpFabricControlPlaneState::Configured as i32,
            "{stalled}: {response:?}"
        );

        assert_eq!(
            fabric.status,
            rm::ScaleUpFabricReportedStatus::Abnormal as i32
        );

        assert_eq!(response.status, rm::ReturnCode::Failure as i32);

        let errors: Vec<_> = response
            .discovery_errors
            .iter()
            .chain(&fabric.inspection_errors)
            .collect();

        assert!(
            errors
                .iter()
                .any(|error| error.message.contains("deadline")),
            "{stalled}: {response:?}"
        );

        let servers = if stalled == "primary-identity" {
            vec![&nvue]
        } else {
            vec![&nvue, &slow]
        };

        for server in servers {
            let requests = server.received_requests().await.unwrap();

            assert!(
                !requests.is_empty(),
                "{stalled}: discovery was not attempted"
            );

            assert!(
                requests
                    .iter()
                    .all(|request| request.method == wiremock::http::Method::Get)
            );
        }
    }
}

#[tokio::test]
async fn secure_primary_with_current_binding_retries_tls_connect_failure_without_rebinding() {
    use crate::libnmxc::TlsMaterialStore;
    use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;

    let server = MockServer::start().await;
    for (endpoint, body) in [
        (
            "/nvue_v1/system/api",
            serde_json::json!({"certificate": "cert-2"}),
        ),
        (
            "/nvue_v1/system/api/mtls",
            serde_json::json!({"ca-certificate": "ca-1"}),
        ),
        (
            "/nvue_v1/cluster/apps/nmx-controller/manager/certificate",
            serde_json::json!({"certificate": "cert-2"}),
        ),
        (
            "/nvue_v1/cluster/apps/nmx-controller/manager/ca-certificate",
            serde_json::json!({"ca-certificate": "ca-1"}),
        ),
        (
            "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
            serde_json::json!({"encryption": "mtls"}),
        ),
        ("/nvue_v1/cluster", serde_json::json!({"state": "enabled"})),
        (
            "/nvue_v1/cluster/apps/nmx-controller",
            serde_json::json!({"status": "ok", "manager": {"state": "enabled"}}),
        ),
        (
            "/nvue_v1/cluster/apps/nmx-controller/manager",
            serde_json::json!({"state": "enabled"}),
        ),
    ] {
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
    }

    let material = tempfile::tempdir().unwrap();
    let bind_ip = non_loopback_local_ip();

    let (controller_server, tls_attempts) =
        TestNmxControllerServer::start_mtls_with_first_connect_failure(bind_ip, material.path())
            .await;

    let mut service = insecure_fabric_service();

    service.switch_tls_roots = SwitchTlsRoots {
        client_tls: Some(TlsMaterialStore::new(material.path())),
        ..SwitchTlsRoots::default()
    };

    let switch = SwitchGb200Nvidia::for_test(&server.uri());

    tokio::time::timeout(
        Duration::from_secs(30),
        service.enforce_primary_nmx_controller_security(
            &switch,
            &bind_ip.to_string(),
            "nmx-controller.example.com",
            Some("fabric-a"),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("secure primary Hello retries must be bounded")
    .unwrap();

    assert_eq!(tls_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(controller_server.hello_calls.load(Ordering::SeqCst), 1);

    let requests = server.received_requests().await.unwrap();

    assert!(
        requests
            .iter()
            .all(|request| request.method == wiremock::http::Method::Get)
    );

    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path()
                == "/nvue_v1/cluster/apps/nmx-controller/manager/ca-certificate")
            .count(),
        1
    );
}

#[tokio::test]
async fn matching_starting_primary_waits_for_control_plane_convergence() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "start",
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "not ok",
            "reason": "GFM: UNCONFIGURED",
            "addition-info": "CONTROL_PLANE_STATE_UNCONFIGURED",
            "manager": {
                "state": "enabled",
                "encryption": "disabled",
            },
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "ok",
            "additional-info": "CONTROL_PLANE_STATE_CONFIGURED",
            "manager": {
                "state": "enabled",
                "encryption": "disabled",
            },
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
        })))
        .mount(&server)
        .await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let _nmx_controller = TestNmxControllerServer::start(switch_ip, "topology-a").await;

    let service = RackManagerServiceImpl {
        rack_manager: Arc::new(RackManager::new()),
        job_tracker: Arc::new(JobTracker::new()),
        firmware_download_cancellations: Arc::new(std::sync::Mutex::new(HashMap::new())),
        backends: Backends::memory(),
        firmware_dir: std::path::PathBuf::from("firmware"),
        artifact_require_sha256: false,
        insecure_bmc: true,
        artifact_download_policy:
            crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
        switch_tls_roots: SwitchTlsRoots {
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        },
        sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
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
    };

    let primary = rm::NodeInfo {
        node_id: "sw-01".into(),
        rack_id: "rack-01".into(),
        r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
        host_endpoint: Some(rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: switch_ip.to_string(),
                mac_address: String::new(),
                host_name: None,
            }),
            port: u32::from(proxy.port),
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "admin".into(),
                    password: "password".into(),
                })),
            }),
        }),
        ..Default::default()
    };

    let config = rm_v2::ScaleUpFabricConfig {
        topology_type: "topology-a".into(),
        extra_static_configs: Vec::new(),
    };

    let reconciliation = tokio::spawn(async move {
        service
            .reconcile_scale_up_fabric_primary(&primary, &config, None, false)
            .await
    });

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let first_convergence_read_seen = server
                .received_requests()
                .await
                .expect("wiremock should record requests")
                .iter()
                .any(|request| request.url.path() == "/nvue_v1/cluster/apps/nmx-controller");

            if first_convergence_read_seen {
                break;
            }

            assert!(
                !reconciliation.is_finished(),
                "matching reconciliation returned before checking NMX Controller convergence"
            );

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("matching reconciliation should check NMX Controller convergence");

    assert!(
        !reconciliation.is_finished(),
        "unconfigured state must keep matching reconciliation pending"
    );

    tokio::time::timeout(Duration::from_secs(10), reconciliation)
        .await
        .expect("matching reconciliation should finish after configured state")
        .expect("reconciliation task should complete")
        .expect("configured state should complete matching reconciliation");

    let requests = server
        .received_requests()
        .await
        .expect("wiremock should record requests");

    assert!(
        !requests
            .iter()
            .any(|request| request.method.to_string() == "POST"),
        "matching configuration must not restart NMX Controller"
    );
}

/// Verifies insecure V2 reconciliation unsets mTLS before plaintext Hello.
#[tokio::test]
async fn insecure_switch_unsets_enabled_manager_encryption_before_hello() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "ok",
            "additional-info": "CONTROL_PLANE_STATE_CONFIGURED",
            "manager": {
                "state": "enabled",
                "encryption": "mtls",
            },
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
        })))
        .mount(&server)
        .await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let nmx_controller = TestNmxControllerServer::start(switch_ip, "topology-a").await;

    let service = RackManagerServiceImpl {
        rack_manager: Arc::new(RackManager::new()),
        job_tracker: Arc::new(JobTracker::new()),
        firmware_download_cancellations: Arc::new(std::sync::Mutex::new(HashMap::new())),
        backends: Backends::memory(),
        firmware_dir: std::path::PathBuf::from("firmware"),
        artifact_require_sha256: false,
        insecure_bmc: true,
        artifact_download_policy:
            crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
        switch_tls_roots: SwitchTlsRoots {
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        },
        sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
        nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
        expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        workflow_timings: crate::config::WorkflowTimings::default(),
        recovery_profiles: crate::config::RecoveryProfiles::default(),
        fabric_layout_catalog: std::sync::Arc::new(
            crate::config::fabric_layout::FabricLayoutCatalog::default(),
        ),
        hardware_profiles: std::sync::Arc::new(
            crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
        ),
        fabric_operations: std::sync::Arc::new(Default::default()),
    };

    let primary = rm::NodeInfo {
        node_id: "sw-01".into(),
        rack_id: "rack-01".into(),
        r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
        host_endpoint: Some(rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: switch_ip.to_string(),
                mac_address: String::new(),
                host_name: None,
            }),
            port: u32::from(proxy.port),
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: "admin".into(),
                    password: "password".into(),
                })),
            }),
        }),
        ..Default::default()
    };

    let config = rm_v2::ScaleUpFabricConfig {
        topology_type: "topology-a".into(),
        extra_static_configs: Vec::new(),
    };

    let error = service
        .reconcile_scale_up_fabric_primary(&primary, &config, None, false)
        .await
        .expect_err("enabled manager encryption must be unset before plaintext Hello");

    assert!(error.contains("failed to disable NMX-C mTLS"), "{error}");
    assert_eq!(
        nmx_controller.hello_calls.load(Ordering::SeqCst),
        0,
        "plaintext Hello must not run while manager encryption is still mtls"
    );
}

/// Verifies insecure V2 reconciliation opens a closed manager port on an
/// enabled primary instead of waiting for the port to open.
#[tokio::test]
async fn insecure_switch_opens_closed_manager_port_on_enabled_primary() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "ok",
            "additional-info": "CONTROL_PLANE_STATE_UNCONFIGURED",
            "manager": {
                "state": "disabled",
                "encryption": "disabled",
            },
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "disabled",
            "encryption": "disabled",
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller/manager"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "title": "manager update rejected",
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Keep 9370 closed, as on a switch whose manager port is disabled.
    let _nmx_controller_port = TEST_NMX_CONTROLLER_PORT_LOCK.lock().await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let service = insecure_fabric_service();
    let primary = rm::NodeInfo {
        r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
        ..vrnvl72_switch("sw-01", switch_ip, proxy.port)
    };

    let config = rm_v2::ScaleUpFabricConfig {
        topology_type: "topology-a".into(),
        extra_static_configs: Vec::new(),
    };

    let error = service
        .reconcile_scale_up_fabric_primary(&primary, &config, None, false)
        .await
        .expect_err("the mocked manager port update is rejected");

    assert!(error.contains("failed to enable gRPC"), "{error}");
}

/// Verifies the legacy fabric disable clears VR NVL72 node IPs that V2 left
/// configured before it disables the cluster.
#[tokio::test]
async fn legacy_disable_clears_vrnvl72_node_ips_before_cluster_disable() {
    let server = MockServer::start().await;
    let configured = serde_json::json!({"192.0.2.101": {}});

    mount_nvbridge_probe(&server, 404).await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .respond_with(ResponseTemplate::new(200).set_body_json("42"))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/nvue_v1/revision"))
        .respond_with(ResponseTemplate::new(200).set_body_json("43"))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&configured))
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/nvue_v1/cluster/node/primary"))
        .and(query_param("rev", "42"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/cluster"))
        .and(query_param("rev", "43"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    for revision in ["42", "43"] {
        Mock::given(method("PATCH"))
            .and(path(format!("/nvue_v1/revision/{revision}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/nvue_v1/revision/{revision}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"state": "applied"})),
            )
            .mount(&server)
            .await;
    }

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "disabled",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/apps/nmx-controller"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let service = insecure_fabric_service();

    let result = super::super::batch_set_scale_up_fabric_state_for_device(
        &service,
        vrnvl72_switch("sw-01", switch_ip, proxy.port),
        false,
    )
    .await;

    assert_eq!(
        result.status,
        rm::ReturnCode::Success as i32,
        "{}",
        result.error_message
    );

    let requests = server.received_requests().await.expect("recorded requests");
    let position = |expected_method: &str, expected_path: &str| {
        requests.iter().position(|request| {
            request.method.to_string() == expected_method && request.url.path() == expected_path
        })
    };

    let node_ip_delete = position("DELETE", "/nvue_v1/cluster/node/primary")
        .expect("primary node IPs must be cleared");
    let cluster_disable =
        position("PATCH", "/nvue_v1/cluster").expect("the cluster must be disabled");

    assert!(
        node_ip_delete < cluster_disable,
        "node IPs must be cleared before the cluster disable is staged"
    );
}

async fn mount_nvbridge_probe(server: &MockServer, status: u16) {
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .respond_with(ResponseTemplate::new(status).set_body_json(serde_json::json!({})))
        .mount(server)
        .await;
}

/// Requesting secondary addresses fails at session init when NVUE has no
/// `/cluster/node/secondary`, before the primary address read.
#[tokio::test]
async fn prepare_node_ip_snapshot_rejects_required_secondary_without_nvbridge() {
    let server = MockServer::start().await;
    mount_nvbridge_probe(&server, 404).await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let service = insecure_fabric_service();
    let node = with_secondary_address(vrnvl72_switch("sw-01", switch_ip, proxy.port), "10.1.3.101");

    let error = match prepare_node_ip_snapshot(&service, &node, None, true, true).await {
        Err(error) => error,
        Ok(_) => panic!("required secondary addresses need NVBridge"),
    };

    assert!(
        error.contains("NVBridge secondary cluster-node interface is not available"),
        "{error}"
    );
    assert!(error.contains("404"), "{error}");

    let requests = server.received_requests().await.expect("recorded requests");
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/nvue_v1/cluster/node/primary/server"),
        "primary address read must not run after the NVBridge gate rejects the image"
    );
}

/// A non-selected switch still asks to include secondary state, but an
/// unsupported image must not GET `/cluster/node/secondary/server`.
#[tokio::test]
async fn prepare_node_ip_snapshot_skips_secondary_read_without_nvbridge() {
    let server = MockServer::start().await;
    mount_nvbridge_probe(&server, 404).await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let service = insecure_fabric_service();
    let node = vrnvl72_switch("sw-02", switch_ip, proxy.port);

    let snapshot = prepare_node_ip_snapshot(&service, &node, None, true, false)
        .await
        .expect("unsupported NVBridge is allowed when secondary addresses are not required");

    assert!(snapshot.secondary_node_ips.is_none());
    assert_eq!(snapshot.capabilities, Some(NvBridge::Unsupported));

    let requests = server.received_requests().await.expect("recorded requests");
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/nvue_v1/cluster/node/secondary/server"),
        "secondary address read must stay behind the NVBridge gate"
    );
}

/// A selected switch with no secondary addresses does not probe NVBridge.
/// A 503 on that GET must not fail preparation.
#[tokio::test]
async fn prepare_node_ip_snapshot_skips_nvbridge_probe_without_secondary_addresses() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/secondary"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({})))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster/node/primary/server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let service = insecure_fabric_service();
    let node = vrnvl72_switch("sw-01", switch_ip, proxy.port);

    let snapshot = prepare_node_ip_snapshot(&service, &node, None, false, false)
        .await
        .expect("a primary-only snapshot must not probe the secondary collection");

    assert!(snapshot.capabilities.is_none());
    assert!(snapshot.secondary_node_ips.is_none());
}

/// Invalid inventories fail admission before switch endpoints are read.
#[tokio::test]
async fn v2_rejects_invalid_inventory_before_switch_endpoint_validation() {
    let service = insecure_fabric_service();

    for (nodes, expected) in [
        (
            vec![rm::NodeInfo {
                node_id: "compute-only".into(),
                r#type: Some(rm::NodeType::ComputeGb200Nvidia.into()),
                ..Default::default()
            }],
            "must contain at least one switch",
        ),
        (
            vec![
                rm::NodeInfo {
                    node_id: "duplicate".into(),
                    r#type: Some(rm::NodeType::SwitchGb200Nvidia.into()),
                    ..Default::default()
                },
                rm::NodeInfo {
                    node_id: "duplicate".into(),
                    r#type: Some(rm::NodeType::ComputeGb200Nvidia.into()),
                    ..Default::default()
                },
            ],
            "nonempty and unique",
        ),
    ] {
        let error = service
            .handle_configure_scale_up_fabric_manager_v2(tonic::Request::new(
                rm_v2::ConfigureScaleUpFabricManagerRequest {
                    nodes: Some(rm::NodeSet { nodes }),
                    ..Default::default()
                },
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains(expected), "{error}");
    }
}

/// Known non-switch nodes need no endpoints and are excluded from the job.
/// The V2 job passes `require_secondary_interface` for the selected switch.
/// Dropping that argument, or the session-init gate, continues into the
/// primary address read.
#[tokio::test]
async fn v2_job_filters_non_switch_nodes_and_rejects_unsupported_secondary_addresses() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "chassis-sn": "chassis-01",
            "tray-index": "1",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "disabled",
        })))
        .mount(&server)
        .await;
    mount_nvbridge_probe(&server, 404).await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let service = insecure_fabric_service();
    let node = with_secondary_address(vrnvl72_switch("sw-01", switch_ip, proxy.port), "10.1.3.101");

    let response = service
        .handle_configure_scale_up_fabric_manager_v2(tonic::Request::new(
            rm_v2::ConfigureScaleUpFabricManagerRequest {
                nodes: Some(rm::NodeSet {
                    nodes: vec![
                        rm::NodeInfo {
                            node_id: "compute-ignored".into(),
                            r#type: Some(rm::NodeType::ComputeGb200Nvidia.into()),
                            ..Default::default()
                        },
                        node,
                        rm::NodeInfo {
                            node_id: "power-ignored".into(),
                            r#type: Some(rm::NodeType::PowershelfGb200Liteon.into()),
                            ..Default::default()
                        },
                    ],
                }),
                primary_switch_node_id: Some("sw-01".into()),
                config: Some(rm_v2::ScaleUpFabricConfig {
                    topology_type: "topology-a".into(),
                    extra_static_configs: Vec::new(),
                }),
                ..Default::default()
            },
        ))
        .await
        .expect("job admission");

    let job_id = response.into_inner().job_id;
    let job = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(job) = service.job_tracker.get_job(&job_id)
                && job.state.is_terminal()
            {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("V2 job should finish");

    assert_eq!(job.state, JobState::Failed);
    assert!(
        job.error_message
            .contains("NVBridge secondary cluster-node interface is not available"),
        "{}",
        job.error_message
    );

    let requests = server.received_requests().await.expect("recorded requests");
    assert!(
        requests
            .iter()
            .any(|request| request.url.path() == "/nvue_v1/cluster/node/secondary"),
        "the job must probe NVBridge before failing"
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/nvue_v1/cluster/node/primary/server"),
        "a missing require_secondary_interface argument would read primary addresses first"
    );
}

#[tokio::test]
async fn fabric_scope_collection_reads_concurrently_and_retains_ordered_aliases() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let switch_ip = non_loopback_local_ip();
    let service = insecure_fabric_service();

    let mut servers = tokio::task::JoinSet::new();
    let mut proxies = Vec::new();
    let mut nodes = Vec::new();
    let mut expected = Vec::new();

    for tray_index in 0..2 {
        let barrier = Arc::clone(&barrier);

        let app = axum::Router::new().route(
            "/nvue_v1/platform/chassis-location",
            axum::routing::get(move || {
                let barrier = Arc::clone(&barrier);

                async move {
                    // Neither response can finish until both reads have started.
                    barrier.wait().await;

                    axum::Json(serde_json::json!({
                        "chassis-sn": "test-chassis",
                        "tray-index": tray_index.to_string(),
                    }))
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();

        let upstream = listener.local_addr().unwrap();

        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });

        let proxy = WiremockHttpsProxy::start(switch_ip, upstream).await;
        let node = vrnvl72_switch(&format!("switch-{tray_index}"), switch_ip, proxy.port);

        let logical = JobScope::LogicalNode {
            rack_id: node.rack_id.clone(),
            node_id: node.node_id.clone(),
        };

        let endpoint = JobScope::endpoint(switch_ip.to_string(), proxy.port);

        expected.extend([
            logical.clone(),
            endpoint.clone(),
            JobScope::endpoint(switch_ip.to_string(), 22),
            logical,
            JobScope::PhysicalSwitch {
                chassis_serial_number: "test-chassis".into(),
                tray_index,
            },
            endpoint,
        ]);

        nodes.push(node);
        proxies.push(proxy);
    }

    let scopes = tokio::time::timeout(
        Duration::from_secs(5),
        service.collect_scale_up_fabric_scopes(&nodes, None),
    )
    .await
    .expect("identity reads must overlap to release the barrier")
    .unwrap();

    assert_eq!(scopes, expected);

    servers.shutdown().await;
}

#[tokio::test]
async fn fabric_scope_collection_returns_errors_in_request_order() {
    let service = insecure_fabric_service();
    let invalid_host = vrnvl72_switch("invalid-host", Ipv4Addr::LOCALHOST.into(), 443);
    let mut invalid_port = invalid_host.clone();
    invalid_port.node_id = "invalid-port".into();
    invalid_port.host_endpoint.as_mut().unwrap().port = u32::MAX;

    for (nodes, expected) in [
        (
            [invalid_host.clone(), invalid_port.clone()],
            tonic::Code::FailedPrecondition,
        ),
        ([invalid_port, invalid_host], tonic::Code::InvalidArgument),
    ] {
        let error = service
            .collect_scale_up_fabric_scopes(&nodes, None)
            .await
            .unwrap_err();

        assert_eq!(error.code(), expected);
    }
}

#[tokio::test]
async fn v2_configuration_excludes_overlapping_endpoints_without_chassis_metadata() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let switch_ip = non_loopback_local_ip();
    let nvue_proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let bmc_proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let metrics = prometheus::Registry::new();
    let mut service = insecure_fabric_service();
    service.job_tracker = Arc::new(JobTracker::builder().metrics(&metrics).build().unwrap());

    let mut node = vrnvl72_switch("fabric-switch", switch_ip, nvue_proxy.port);
    let mut bmc = node.host_endpoint.clone().unwrap();
    bmc.port = u32::from(bmc_proxy.port);
    bmc.interface.as_mut().unwrap().mac_address = "02:00:00:00:00:01".into();

    node.bmc_endpoint = Some(bmc.clone());

    let alias_node = rm::NodeInfo {
        rack_id: "alias-rack".into(),
        node_id: "alias-switch".into(),
        r#type: node.r#type,
        bmc_endpoint: Some(bmc),
        ..Default::default()
    };

    let request = rm_v2::ConfigureScaleUpFabricManagerRequest {
        nodes: Some(rm::NodeSet { nodes: vec![node] }),
        config: Some(rm_v2::ScaleUpFabricConfig {
            topology_type: "topology-a".into(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let alias_scopes = crate::api::grpc::switch_scope::request_switch_scopes(&alias_node).unwrap();

    let reservation = service
        .job_tracker
        .reserve_mutation(alias_scopes.clone())
        .unwrap();

    let error = service
        .handle_configure_scale_up_fabric_manager_v2(tonic::Request::new(request.clone()))
        .await
        .expect_err("V2 configuration must not overlap reserved endpoint aliases");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("occupied"));

    drop(reservation);

    let intent = crate::orchestrator::fabric_operation::FabricOperationIntent {
        scope_keys: crate::api::grpc::switch_scope::fabric_scope_keys(&alias_scopes).unwrap(),
        rule_id: "test-rule".into(),
    };

    service
        .fabric_operations
        .admit(intent, "owner", None)
        .unwrap();

    let error = service
        .handle_configure_scale_up_fabric_manager_v2(tonic::Request::new(request.clone()))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let recorded = prometheus::TextEncoder::new()
        .encode_to_string(&metrics.gather())
        .unwrap();

    assert!(
        recorded.contains("error_code=\"update_in_progress\""),
        "{recorded}"
    );

    service.fabric_operations.finish("owner").unwrap();

    let response = service
        .handle_configure_scale_up_fabric_manager_v2(tonic::Request::new(request))
        .await
        .expect("configuration is admitted after endpoint aliases are released")
        .into_inner();

    // The current-thread runtime has not polled the configuration worker yet.
    // Configuration owns the supplied endpoint aliases before any device writes.
    let failure = service
        .job_tracker
        .reserve_mutation(alias_scopes)
        .err()
        .expect("configuration must retain endpoint reservations");

    assert!(
        failure.message.contains(&response.job_id),
        "{}",
        failure.message
    );

    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == wiremock::http::Method::Get)
    );

    service
        .job_tracker
        .shutdown_and_await_terminal(Duration::from_secs(2))
        .await;

    assert!(
        service
            .job_tracker
            .get_job(&response.job_id)
            .unwrap()
            .state
            .is_terminal()
    );
}

#[tokio::test]
async fn sdn_reset_excludes_supplied_bmc_and_ssh_aliases_without_chassis_metadata() {
    let server = MockServer::start().await;
    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;

    for port in [443_u16, 22] {
        let service = insecure_fabric_service();

        let mut node = vrnvl72_switch("reset-switch", switch_ip, proxy.port);

        let mut endpoint = node.host_endpoint.clone().unwrap();

        endpoint.interface.as_mut().unwrap().ip_address = "192.0.2.20".into();

        endpoint.port = u32::from(port);

        if port == 443 {
            node.bmc_endpoint = Some(endpoint);
        } else {
            node.additional_host_endpoints.push(endpoint);
        }

        let _owner = service
            .job_tracker
            .reserve_mutation(vec![JobScope::endpoint("192.0.2.20", port)])
            .unwrap();

        let response = service
            .handle_batch_reset_switch_sdn_factory_default(tonic::Request::new(
                rm::BatchResetSwitchSdnFactoryDefaultRequest {
                    nodes: Some(rm::NodeSet { nodes: vec![node] }),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .response
            .unwrap();

        let parent = service.job_tracker.get_job(&response.job_id).unwrap();
        let child_id = parent.child_job_ids.first().unwrap();

        service
            .job_tracker
            .registry()
            .wait_for_terminal_outcomes(&[child_id.clone().into()], Some(Duration::from_secs(5)))
            .await;

        let child = service.job_tracker.get_job(child_id).unwrap();

        assert_eq!(child.state, JobState::Failed);

        assert!(
            child.error_message.contains("occupied"),
            "{}",
            child.error_message
        );
    }
}

#[tokio::test]
async fn v2_reserves_secondary_switch_through_reconciliation_and_rollback() {
    let IpAddr::V4(switch_ip) = non_loopback_local_ip() else {
        panic!("the local address probe must return IPv4");
    };

    let service = insecure_fabric_service();
    let (phase_tx, mut phase_rx) = tokio::sync::mpsc::channel(2);
    let resume = Arc::new(tokio::sync::Semaphore::new(0));

    let mut servers = tokio::task::JoinSet::new();
    let mut proxies = Vec::new();
    let mut nodes = Vec::new();

    // Use separate listeners and ports on IPv4 and mapped IPv6 so the public
    // handler sees distinct inventory addresses without requiring 2 host NICs.
    for (index, ip) in [switch_ip.into(), switch_ip.to_ipv6_mapped().into()]
        .into_iter()
        .enumerate()
    {
        let reads = Arc::new(AtomicUsize::new(0));
        let phase_tx = phase_tx.clone();
        let resume = Arc::clone(&resume);

        let app = axum::Router::new()
            .route("/nvue_v1/platform/chassis-location", axum::routing::get(move || async move {
                axum::Json(serde_json::json!({"chassis-sn": "test-chassis", "tray-index": index.to_string()}))
            }))
            .route("/nvue_v1/cluster", axum::routing::get(move || {
                let read = reads.fetch_add(1, Ordering::SeqCst);
                let phase_tx = phase_tx.clone();
                let resume = Arc::clone(&resume);

                async move {
                    if index == 1 && read > 0 {
                        // Read 0 snapshots the enabled cluster. Read 1 is the
                        // disable preflight; failing it forces rollback (read 2).
                        phase_tx.send(read).await.unwrap();
                        resume.acquire().await.unwrap().forget();
                    }

                    let status = if index == 1 && read == 1 {
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        axum::http::StatusCode::OK
                    };

                    (status, axum::Json(serde_json::json!({"state": "enabled"})))
                }
            }));

        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();

        let upstream = listener.local_addr().unwrap();

        servers.spawn(async move { axum::serve(listener, app).await.unwrap() });

        let proxy = WiremockHttpsProxy::start(ip, upstream).await;
        let mut node = vrnvl72_switch(&format!("switch-{index}"), ip, proxy.port);
        node.r#type = Some(rm::NodeType::SwitchGb200Nvidia.into());
        nodes.push(node);
        proxies.push(proxy);
    }

    let secondary = nodes.last().unwrap();

    let mut secondary_scopes =
        crate::api::grpc::switch_scope::request_switch_scopes(secondary).unwrap();

    secondary_scopes.push(JobScope::PhysicalSwitch {
        chassis_serial_number: "test-chassis".into(),
        tray_index: 1,
    });

    let response = service
        .handle_configure_scale_up_fabric_manager_v2(tonic::Request::new(
            rm_v2::ConfigureScaleUpFabricManagerRequest {
                nodes: Some(rm::NodeSet { nodes }),
                primary_switch_node_id: Some("switch-0".into()),
                config: Some(rm_v2::ScaleUpFabricConfig {
                    topology_type: "topology-a".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();

    for expected_read in [1, 2] {
        let read = tokio::time::timeout(Duration::from_secs(10), phase_rx.recv())
            .await
            .expect("worker must reach reconciliation and rollback")
            .unwrap();

        assert_eq!(read, expected_read);

        for scope in &secondary_scopes {
            let failure = service
                .job_tracker
                .reserve_mutation(vec![scope.clone()])
                .err()
                .expect("secondary aliases must remain reserved");

            assert!(failure.message.contains(&response.job_id));
        }

        resume.add_permits(1);
    }

    let job = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let job = service.job_tracker.get_job(&response.job_id).unwrap();

            if job.state.is_terminal() {
                break job;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rollback must finish");

    assert_eq!(job.state, JobState::Failed);

    assert!(
        job.error_message
            .contains("failed to disable scale-up fabric on switch 'switch-1'")
    );

    assert!(!job.error_message.contains("rollback incomplete"));

    assert!(
        service
            .job_tracker
            .reserve_mutation(secondary_scopes)
            .is_ok()
    );

    servers.shutdown().await;
}

#[tokio::test]
async fn direct_fabric_mutation_retains_endpoints_without_chassis_metadata() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform/chassis-location"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    let switch_ip = non_loopback_local_ip();
    let nvue_proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let bmc_proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let service = insecure_fabric_service();

    let mut node = vrnvl72_switch("fabric-switch", switch_ip, nvue_proxy.port);
    let mut bmc = node.host_endpoint.clone().unwrap();
    bmc.port = u32::from(bmc_proxy.port);
    bmc.interface.as_mut().unwrap().mac_address = "02:00:00:00:00:01".into();

    node.bmc_endpoint = Some(bmc.clone());

    let prepared = service.build_ephemeral_switch(&node, None).await.unwrap();

    let reservation = service
        .reserve_switch_mutation(prepared.switch().nvue_client(), &node)
        .await
        .unwrap();

    let alias_node = rm::NodeInfo {
        rack_id: "alias-rack".into(),
        node_id: "alias-switch".into(),
        r#type: node.r#type,
        bmc_endpoint: Some(bmc),
        ..Default::default()
    };

    let alias_scopes = crate::api::grpc::switch_scope::request_switch_scopes(&alias_node).unwrap();

    let failure = service
        .job_tracker
        .reserve_mutation(alias_scopes.clone())
        .err()
        .expect("direct mutation must retain endpoint reservations");

    assert!(failure.message.contains("occupied"), "{}", failure.message);

    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == wiremock::http::Method::Get)
    );

    drop(reservation);

    assert!(service.job_tracker.reserve_mutation(alias_scopes).is_ok());
}

#[tokio::test]
async fn switch_admission_without_client_preserves_request_and_job_aliases() {
    let service = insecure_fabric_service();
    let node = vrnvl72_switch("switch-a", "192.0.2.10".parse().unwrap(), 443);
    let scopes = crate::api::grpc::switch_scope::request_switch_scopes(&node).unwrap();

    let reservation = service.reserve_switch_mutation(None, &node).await.unwrap();

    for scope in &scopes {
        assert!(
            service
                .job_tracker
                .reserve_mutation(vec![scope.clone()])
                .is_err()
        );
    }

    drop(reservation);

    assert!(service.job_tracker.reserve_mutation(scopes).is_ok());

    let job = service
        .job_tracker
        .create_visible_workflow_job_if_node_idle(
            &node.rack_id,
            "job-owner",
            JobType::ConfigureScaleUpFabricManagerV2,
        )
        .unwrap();

    service
        .admit_switch_job_scopes(&job, None, &node.rack_id, &node.node_id, Vec::new())
        .await
        .unwrap();

    let logical = vec![JobScope::LogicalNode {
        rack_id: node.rack_id,
        node_id: node.node_id,
    }];

    assert!(
        service
            .job_tracker
            .reserve_mutation(logical.clone())
            .is_err()
    );

    job.complete("done", "");

    assert!(service.job_tracker.reserve_mutation(logical).is_ok());
}

#[tokio::test]
async fn switch_reservation_allows_cluster_reads_but_blocks_telemetry_mutations() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let switch_ip = non_loopback_local_ip();
    let proxy = WiremockHttpsProxy::start(switch_ip, *server.address()).await;
    let service = insecure_fabric_service();
    let node = vrnvl72_switch("fabric-switch", switch_ip, proxy.port);
    let scopes = crate::api::grpc::switch_scope::request_switch_scopes(&node).unwrap();
    let _reservation = service.job_tracker.reserve_mutation(scopes).unwrap();

    let state = service
        .handle_get_scale_up_fabric_state(tonic::Request::new(rm::GetScaleUpFabricStateRequest {
            node: Some(node.clone()),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        state.status,
        rm::ReturnCode::Success as i32,
        "{}",
        state.error_message
    );

    assert!(state.state_json.contains("enabled"));

    let mutation = service
        .handle_set_scale_up_fabric_telemetry_interface_state(tonic::Request::new(
            rm::SetScaleUpFabricTelemetryInterfaceStateRequest {
                node: Some(node),
                enable: true,
            },
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(mutation.status, rm::ReturnCode::Failure as i32);
    assert!(mutation.error_message.contains("occupied"));

    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| { request.method == wiremock::http::Method::Get })
    );
}
