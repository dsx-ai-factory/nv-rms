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

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;
use crate::api::grpc::server::SwitchTlsRoots;
use crate::libnmxc::{nmxc_model, test_support::FakeNmxc};
use crate::orchestrator::job_tracker::JobTracker;
use crate::orchestrator::rack_manager::RackManager;
use crate::persistence::Backends;

#[derive(Clone)]
struct TestNmxController {
    static_config: nmxc_model::NmxGetStaticConfigResponse,
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
                struct Hello;

                impl tonic::server::UnaryService<nmxc_model::NmxHelloRequest> for Hello {
                    type Response = nmxc_model::NmxHelloResponse;
                    type Future =
                        tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

                    fn call(
                        &mut self,
                        _request: tonic::Request<nmxc_model::NmxHelloRequest>,
                    ) -> Self::Future {
                        let server_header = FakeNmxc::success_return_code().server_header;

                        Box::pin(async move {
                            Ok(tonic::Response::new(nmxc_model::NmxHelloResponse {
                                server_header,
                            }))
                        })
                    }
                }

                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);

                    Ok(grpc.unary(Hello, request).await)
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

        let incoming = tonic::transport::server::TcpIncoming::bind(SocketAddr::from((
            bind_ip,
            config::GRPC_PORT_NMX_CONTROLLER,
        )))
        .expect("bind test NMX Controller");

        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TestNmxController { static_config })
                .serve_with_incoming(incoming)
                .await
                .expect("run test NMX Controller");
        });

        Self {
            task,
            _port_guard: port_guard,
        }
    }
}

impl Drop for TestNmxControllerServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct WiremockHttpsProxy {
    port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl WiremockHttpsProxy {
    async fn start(bind_ip: IpAddr, upstream: SocketAddr) -> Self {
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

fn non_loopback_local_ip() -> IpAddr {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
        .expect("bind local address probe");

    socket
        .connect(SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 80)))
        .expect("select local interface");

    let ip = socket.local_addr().expect("read local interface").ip();

    assert!(!super::super::is_disallowed_switch_target_ip(ip));

    ip
}

#[tokio::test]
async fn matching_primary_waits_for_control_plane_convergence() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/nvue_v1/cluster"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "state": "enabled",
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
        })))
        .expect(1)
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
        switch_tls_roots: SwitchTlsRoots {
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        },
        sftp_upload_options: crate::transport::ssh::SftpUploadOptions::default(),
        nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
        expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
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
