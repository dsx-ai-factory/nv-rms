/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared fixtures for regression tests.

mod activation;
mod preparation;
mod recovery;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;
use crate::api::grpc::scaleupfabricmanager_handlers::EphemeralSwitch;
use crate::api::grpc::server::SwitchTlsRoots;
use crate::config::fabric_layout::FabricPosition;
use crate::domain::node::{Node, NodeType};
use crate::libnmxc::test_support::{FakeNmxc, fabric_responses};
use crate::libnmxc::{NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY, nmxc_model};
use crate::nodes::switch_gb200_nvidia::ScaleUpLinkChange;
use crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;
use crate::nodes::{ScaleUpLinkSnapshot, SwitchScaleUpManagement};
use crate::orchestrator::fabric_operation::FabricOperationIntent;
use crate::orchestrator::job_tracker::{JobTracker, JobType};
use crate::orchestrator::rack_manager::RackManager;
use crate::persistence::Backends;
use crate::utilities::error::{ErrorCode, Result as RmsResult, RmsError};

use nvue_client::interface::InterfaceCollection;
use nvue_client::interface::InterfaceLinkState;

use serde_json::Value;
use tokio_stream::wrappers::TcpListenerStream;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// The production controller port is fixed. These local fixture owners serialize.
static CONTROLLER_PORT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Snapshot(String, bool, bool),
    WriteLinks(String),
    LinkWriteFinished(String),
    Start(String),
    StartFinished(String),
    ExposeController(String),
    Stop(String),
    Restart(String),
    Save(String),
    WriteConfig,
}

struct SwitchState {
    enabled: bool,
    saved_enabled: bool,
    saved_read_error: Option<&'static str>,
    snapshot_error: Option<ErrorCode>,
    link_up: bool,
    saved_link_up: bool,
    reject_saved_link_write: bool,
    link_write_gate: Option<Arc<tokio::sync::Semaphore>>,
    panic_on_link_write: bool,
    stop_gate: Option<Arc<tokio::sync::Semaphore>>,
    stop_error: Option<ErrorCode>,
    start_barrier: Option<Arc<tokio::sync::Barrier>>,
    start_gate: Option<Arc<tokio::sync::Semaphore>>,
    panic_on_start: bool,
}

struct RecordingSwitch {
    id: String,
    client: nvue_client::SharedClient,
    backing: SwitchGb200Nvidia,
    state: Arc<Mutex<SwitchState>>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl Node for RecordingSwitch {
    fn id(&self) -> &str {
        &self.id
    }

    fn rack_id(&self) -> &str {
        "rack-test"
    }

    fn node_type(&self) -> NodeType {
        NodeType::SwitchGb200Nvidia
    }

    fn get_info(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

#[async_trait::async_trait]
impl SwitchScaleUpManagement for RecordingSwitch {
    fn nvue_client(&self) -> Option<&nvue_client::SharedClient> {
        Some(&self.client)
    }

    fn as_switch_gb200(&self) -> Option<&SwitchGb200Nvidia> {
        Some(&self.backing)
    }

    async fn set_cluster_state(&self, enabled: bool) -> RmsResult<()> {
        self.events.lock().unwrap().push(if enabled {
            Event::Start(self.id.clone())
        } else {
            Event::Stop(self.id.clone())
        });

        let (barrier, gate, panic_on_start) = {
            let mut state = self.state.lock().unwrap();
            state.enabled = enabled;
            state.saved_enabled = enabled;
            (
                state.start_barrier.clone(),
                state.start_gate.clone(),
                state.panic_on_start,
            )
        };

        if enabled {
            if let Some(barrier) = barrier {
                barrier.wait().await;
            }

            if let Some(gate) = gate {
                gate.acquire().await.unwrap().forget();
                self.events
                    .lock()
                    .unwrap()
                    .push(Event::StartFinished(self.id.clone()));
            }

            assert!(!panic_on_start, "injected activation panic");
        }

        Ok(())
    }

    async fn stop_cluster_controller(&self) -> RmsResult<()> {
        self.set_cluster_state(false).await?;

        let gate = self.state.lock().unwrap().stop_gate.clone();

        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }

        if let Some(error) = self.state.lock().unwrap().stop_error {
            return Err(RmsError::new(error, "injected shutdown failure"));
        }

        Ok(())
    }

    async fn get_cluster_state(&self) -> RmsResult<Value> {
        let enabled = self.state.lock().unwrap().enabled;
        Ok(serde_json::json!({"state": if enabled { "enabled" } else { "disabled" }}))
    }

    async fn get_scale_up_link_snapshot(&self) -> RmsResult<ScaleUpLinkSnapshot> {
        let state = self.state.lock().unwrap();

        if let Some(code) = state.snapshot_error {
            return Err(RmsError::new(code, "private device response"));
        }

        self.events.lock().unwrap().push(Event::Snapshot(
            self.id.clone(),
            state.link_up,
            state.saved_link_up,
        ));

        let effective: InterfaceCollection = serde_json::from_value(serde_json::json!({
            "access1": {"link": {"state": if state.link_up { serde_json::json!({"up": {}}) } else { serde_json::json!({"down": {}}) }}}
        })).unwrap();

        let startup = serde_json::from_value(serde_json::json!({
            "access1": {"link": {"state": if state.saved_link_up { serde_json::json!({"up": {}}) } else { serde_json::json!({"down": {}}) }}}
        })).unwrap();

        Ok(ScaleUpLinkSnapshot {
            applied: effective.clone(),
            effective,
            startup,
        })
    }

    async fn set_scale_up_link_config(&self, changes: &[ScaleUpLinkChange]) -> RmsResult<()> {
        self.events
            .lock()
            .unwrap()
            .push(Event::WriteLinks(self.id.clone()));

        let (gate, panic_on_write) = {
            let state = self.state.lock().unwrap();
            (state.link_write_gate.clone(), state.panic_on_link_write)
        };

        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }

        assert!(!panic_on_write, "injected link write panic");

        {
            let mut state = self.state.lock().unwrap();
            state.link_up = changes
                .iter()
                .all(|change| change.state == InterfaceLinkState::Up);

            if !state.reject_saved_link_write {
                state.saved_link_up = state.link_up;
            }
        }

        self.events
            .lock()
            .unwrap()
            .push(Event::LinkWriteFinished(self.id.clone()));

        Ok(())
    }

    async fn nvue_hello(&self) -> RmsResult<()> {
        Ok(())
    }

    async fn get_cluster_apps_status(&self, _app_name: &str) -> RmsResult<Value> {
        Ok(
            serde_json::json!({"status": "ok", "manager": {"state": "enabled", "encryption": "disabled"}}),
        )
    }

    async fn enable_grpc_for_external_clients(
        &self,
        _app_name: &str,
        _enabled: bool,
    ) -> RmsResult<Value> {
        self.events
            .lock()
            .unwrap()
            .push(Event::ExposeController(self.id.clone()));

        Ok(serde_json::json!({}))
    }

    async fn gnmi_service(&self, _enabled: bool) -> RmsResult<Value> {
        Err(RmsError::internal("unexpected service mutation"))
    }

    async fn check_grpc_status(&self, _app_name: &str, _target_switch: &str) -> RmsResult<Value> {
        Ok(serde_json::json!({"summary": {"ready_for_external_clients": true}}))
    }

    async fn restart_cluster_app(&self, _app_name: &str) -> RmsResult<()> {
        self.events
            .lock()
            .unwrap()
            .push(Event::Restart(self.id.clone()));

        Ok(())
    }

    async fn reset_sdn_factory_default(&self) -> RmsResult<String> {
        Err(RmsError::internal("unexpected factory reset"))
    }
}

struct LocalHttpsProxy {
    port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl LocalHttpsProxy {
    async fn start(upstream: SocketAddr) -> Self {
        use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
        use tokio::io::copy_bidirectional;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();

        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(cert.cert.der().to_vec())],
                PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
            )
            .unwrap();

        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));

        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();

        let port = listener.local_addr().unwrap().port();

        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();

            while let Ok((stream, _)) = listener.accept().await {
                let acceptor = acceptor.clone();

                connections.spawn(async move {
                    let Ok(mut downstream) = acceptor.accept(stream).await else {
                        return;
                    };

                    let Ok(mut upstream) = tokio::net::TcpStream::connect(upstream).await else {
                        return;
                    };

                    let _ = copy_bidirectional(&mut downstream, &mut upstream).await;
                });
            }
        });

        Self { port, task }
    }
}

impl Drop for LocalHttpsProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireRequest {}

struct WireReply<T>(T);

impl<T: prost::Message + Clone + Send + Sync + 'static> tonic::server::UnaryService<WireRequest>
    for WireReply<T>
{
    type Response = T;
    type Future = tonic::codegen::BoxFuture<tonic::Response<T>, tonic::Status>;

    fn call(&mut self, _request: tonic::Request<WireRequest>) -> Self::Future {
        let response = self.0.clone();
        Box::pin(async move { Ok(tonic::Response::new(response)) })
    }
}

#[derive(Clone)]
struct LocalController {
    events: Arc<Mutex<Vec<Event>>>,
    config_content: Arc<Mutex<String>>,
    hello_failures: Arc<Mutex<usize>>,
}

impl<B> tonic::codegen::Service<http::Request<B>> for LocalController
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
        macro_rules! reply {
            ($response:expr) => {{
                let response = $response;
                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);

                    Ok(grpc.unary(WireReply(response), request).await)
                })
            }};
        }

        let header = nmxc_model::NmxServerHeader {
            return_code: 1,
            ..Default::default()
        };

        let location = nmxc_model::NmxLocationInfo {
            chassis_serial_number: "rack-test-A01".to_owned(),
            tray_index: 5,
            location: None,
        };

        let hardware = fabric_responses(location, &[0], 1, Vec::new(), Some(header.clone()));

        match request.uri().path() {
            "/nmx_c.NMX_Controller/Hello" => {
                let mut failures = self.hello_failures.lock().unwrap();

                if *failures > 0 {
                    *failures -= 1;

                    return Box::pin(async {
                        Ok(tonic::Status::unavailable("controller starting").into_http())
                    });
                }

                reply!(nmxc_model::NmxHelloResponse {
                    server_header: Some(header)
                })
            }
            "/nmx_c.NMX_Controller/GetStaticConfig" => {
                reply!(FakeNmxc::static_config_files_response(&[(
                    NMX_C_FM_CONFIG_FILE,
                    &self.config_content.lock().unwrap()
                )]))
            }
            "/nmx_c.NMX_Controller/SetStaticConfig" => {
                self.events.lock().unwrap().push(Event::WriteConfig);
                reply!(nmxc_model::NmxReturnCode {
                    server_header: Some(header)
                })
            }
            "/nmx_c.NMX_Controller/GetDomainStateInfo" => {
                reply!(hardware.domain)
            }
            "/nmx_c.NMX_Controller/GetTopologyInfo" => reply!(hardware.topology),
            "/nmx_c.NMX_Controller/GetSwitchNodeInfoList" => {
                reply!(hardware.switches)
            }
            "/nmx_c.NMX_Controller/GetComputeNodeInfoList" => {
                reply!(hardware.computes)
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

impl tonic::server::NamedService for LocalController {
    const NAME: &'static str = "nmx_c.NMX_Controller";
}

struct WorkerFixture {
    service: RackManagerServiceImpl,
    switches: Vec<ManagedSwitch>,
    plan: FabricPlan,
    states: Vec<Arc<Mutex<SwitchState>>>,
    events: Arc<Mutex<Vec<Event>>>,
    config_content: Arc<Mutex<String>>,
    hello_failures: Arc<Mutex<usize>>,
    _servers: Vec<MockServer>,
    _proxies: Vec<LocalHttpsProxy>,
    controller: tokio::task::JoinHandle<()>,
    _port_guard: tokio::sync::MutexGuard<'static, ()>,
}

impl WorkerFixture {
    async fn new() -> Self {
        let port_guard = CONTROLLER_PORT.lock().await;
        let events = Arc::new(Mutex::new(Vec::new()));
        let config_content = Arc::new(Mutex::new(format!("{NMX_C_TOPOLOGY_KEY}=layout-a\n")));
        let hello_failures = Arc::new(Mutex::new(0));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

        // Aborted owners release the previous listener when their runtime polls cleanup.
        let listener = loop {
            match tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 9370)).await {
                Ok(listener) => break listener,
                Err(error)
                    if error.kind() == std::io::ErrorKind::AddrInUse
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("cannot bind local controller fixture: {error}"),
            }
        };

        let controller_service = LocalController {
            events: Arc::clone(&events),
            config_content: Arc::clone(&config_content),
            hello_failures: Arc::clone(&hello_failures),
        };

        let controller = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(controller_service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let service = RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::new()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: "firmware".into(),
            artifact_require_sha256: false,
            insecure_bmc: true,
            artifact_download_policy: Default::default(),
            switch_tls_roots: SwitchTlsRoots {
                insecure_switch: true,
                ..Default::default()
            },
            sftp_upload_options: Default::default(),
            nmx_gateway_id: "gateway-test".to_owned(),
            expected_inventory_catalog: Default::default(),
            workflow_timings: Default::default(),
            recovery_profiles: Default::default(),
            hardware_profiles: Arc::new(
                crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
            ),
            fabric_operations: Arc::new(Default::default()),
            fabric_layout_catalog: Arc::new(Default::default()),
        };

        let plan = FabricPlan {
            fabrics: vec![PlannedFabric {
                assignment: rm_v2::ScaleUpFabricMembers {
                    switch_node_ids: vec!["switch-a".to_owned()],
                    primary_switch_node_id: "switch-a".to_owned(),
                    ..Default::default()
                },
                config: rm_v2::ScaleUpFabricConfig {
                    topology_type: "layout-a".to_owned(),
                    extra_static_configs: Vec::new(),
                },
                absent_static_configs: BTreeSet::new(),
            }],
            links: ["switch-a", "switch-b"]
                .into_iter()
                .map(|id| {
                    (
                        id.to_owned(),
                        BTreeMap::from([("access1".to_owned(), FabricLinkState::Up)]),
                    )
                })
                .collect(),
            full_topology: false,
        };

        let mut fixture = Self {
            service,
            switches: Vec::new(),
            plan,
            states: Vec::new(),
            events,
            config_content,
            hello_failures,
            _servers: Vec::new(),
            _proxies: Vec::new(),
            controller,
            _port_guard: port_guard,
        };

        for (id, tray, selected) in [("switch-a", 5, true), ("switch-b", 8, false)] {
            fixture.add_switch(id, tray, selected).await;
        }

        fixture
    }

    async fn add_switch(&mut self, id: &str, tray: u32, selected: bool) {
        let server = MockServer::start().await;

        let state = Arc::new(Mutex::new(SwitchState {
            enabled: selected,
            saved_enabled: selected,
            saved_read_error: None,
            snapshot_error: None,
            link_up: true,
            saved_link_up: true,
            reject_saved_link_write: false,
            link_write_gate: None,
            panic_on_link_write: false,
            stop_gate: None,
            stop_error: None,
            start_barrier: None,
            start_gate: None,
            panic_on_start: false,
        }));

        mount_cluster_responses(&server, &state, &self.events, id).await;

        let client = nvue_client::Client::new(nvue_client::ClientConfig {
            endpoint: nvue_client::ClientEndpoint::http(
                server.address().ip().to_string(),
                server.address().port(),
            ),
            credentials: nvue_client::ClientCredentials::new("test", "test"),
            dangerously_accept_invalid_certs: false,
        })
        .unwrap();

        let proxy = LocalHttpsProxy::start(*server.address()).await;

        let backing = SwitchGb200Nvidia::new(
            id.to_owned(),
            "rack-test".to_owned(),
            "127.0.0.1".to_owned(),
            proxy.port,
            "test",
            "test",
            String::new(),
            String::new(),
            "127.0.0.1".to_owned(),
            String::new(),
            true,
        )
        .unwrap();

        self.switches.push(ManagedSwitch {
            node: rm::NodeInfo {
                node_id: id.to_owned(),
                rack_id: "rack-test".to_owned(),
                r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
                ..Default::default()
            },
            prepared: EphemeralSwitch {
                switch: Box::new(RecordingSwitch {
                    id: id.to_owned(),
                    client,
                    backing,
                    state: Arc::clone(&state),
                    events: Arc::clone(&self.events),
                }),
                target_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                tls_server_name: "localhost".to_owned(),
            },
            placement: ObservedPlacement {
                node_id: id.to_owned(),
                node_type: "switch_gb200_nvidia".to_owned(),
                chassis_serial_number: "rack-test-A01".to_owned(),
                position: FabricPosition {
                    tray_index: tray,
                    slot_number: 0,
                },
                physical_topology: None,
            },
        });

        self.states.push(state);
        self._servers.push(server);
        self._proxies.push(proxy);
    }

    /// Blocks all 9 switch writes so tests can observe concurrent dispatch.
    async fn blocked_link_writes() -> (Self, Arc<tokio::sync::Semaphore>) {
        let mut fixture = Self::new().await;

        for index in 2..9 {
            let id = format!("switch-{index}");
            fixture.add_switch(&id, index as u32 + 9, false).await;

            fixture.plan.links.insert(
                id,
                BTreeMap::from([("access1".to_owned(), FabricLinkState::Up)]),
            );
        }

        let gate = Arc::new(tokio::sync::Semaphore::new(0));

        for state in &fixture.states {
            let mut state = state.lock().unwrap();
            state.link_up = false;
            state.saved_link_up = false;
            state.link_write_gate = Some(Arc::clone(&gate));
        }

        (fixture, gate)
    }

    /// Creates groups whose controller starts wait at a shared barrier.
    async fn parallel_groups(topologies: &[&str]) -> Self {
        let mut fixture = Self::new().await;

        for index in 2..topologies.len() {
            fixture
                .add_switch(&format!("switch-{index}"), 9 + index as u32, false)
                .await;
        }

        fixture.plan.fabrics.clear();

        let barrier = Arc::new(tokio::sync::Barrier::new(topologies.len()));

        for (index, topology) in topologies.iter().enumerate() {
            let id = fixture.switches[index].node.node_id.clone();
            fixture.states[index].lock().unwrap().start_barrier = Some(Arc::clone(&barrier));

            fixture.plan.links.insert(
                id.clone(),
                BTreeMap::from([("access1".to_owned(), FabricLinkState::Up)]),
            );

            fixture.plan.fabrics.push(PlannedFabric {
                assignment: rm_v2::ScaleUpFabricMembers {
                    switch_node_ids: vec![id.clone()],
                    primary_switch_node_id: id,
                    ..Default::default()
                },
                config: rm_v2::ScaleUpFabricConfig {
                    topology_type: (*topology).to_owned(),
                    extra_static_configs: Vec::new(),
                },
                absent_static_configs: BTreeSet::new(),
            });
        }

        fixture
    }

    fn admit(&self) -> (RmsJobHandle, FabricOperation) {
        let scopes = self
            .switches
            .iter()
            .flat_map(|switch| switch.scopes().unwrap())
            .collect::<Vec<_>>();

        let scope_keys = crate::api::grpc::switch_scope::fabric_scope_keys(&scopes).unwrap();

        let job = self
            .service
            .job_tracker
            .create_scoped_job(
                "rack-test",
                "worker",
                JobType::ConfigureScaleUpFabricManagerV2,
                scopes,
            )
            .unwrap();

        let operation = self
            .service
            .fabric_operations
            .admit(
                FabricOperationIntent {
                    scope_keys,
                    rule_id: "rule-test".to_owned(),
                },
                job.id().as_ref(),
                None,
            )
            .unwrap();

        (job, operation)
    }

    fn prove_primary(&self, operation: &FabricOperation) {
        let primary = &self.switches[0];

        let scopes =
            crate::api::grpc::switch_scope::fabric_scope_keys(&primary.scopes().unwrap()).unwrap();

        self.service
            .fabric_operations
            .mark_pending(&operation.job_id, &[primary.controller_identity()])
            .unwrap();

        self.service
            .fabric_operations
            .record_activation(
                &operation.job_id,
                primary.controller_identity(),
                config_digest(&self.plan.fabrics[0]).unwrap(),
                scopes,
            )
            .unwrap();
    }

    fn worker<'a>(
        &'a self,
        job: &'a RmsJobHandle,
        operation: FabricOperation,
    ) -> Reconciliation<'a> {
        Reconciliation {
            controller: ControllerContext {
                service: &self.service,
                job,
                switches: &self.switches,
                computes: &[],
                domain: None,
            },
            operation,
            plan: &self.plan,
            changed_groups: BTreeSet::new(),
            static_config_matches: BTreeSet::new(),
            startup_drift: BTreeSet::new(),
            unverified_primaries: BTreeSet::new(),
        }
    }
}

impl Drop for WorkerFixture {
    fn drop(&mut self) {
        self.controller.abort();
    }
}

async fn mount_cluster_responses(
    server: &MockServer,
    state: &Arc<Mutex<SwitchState>>,
    events: &Arc<Mutex<Vec<Event>>>,
    id: &str,
) {
    let read_state = Arc::clone(state);

    Mock::given(method("GET"))
        .and(|request: &wiremock::Request| {
            matches!(request.url.path(), "/nvue_v1/cluster" | "/nvue_v1/")
        })
        .respond_with(move |request: &wiremock::Request| {
            let state = read_state.lock().unwrap();
            let root = request.url.path() == "/nvue_v1/";

            let saved = request
                .url
                .query_pairs()
                .any(|(key, value)| key == "rev" && value == "startup");

            if saved && let Some(error) = state.saved_read_error {
                return if error == "unavailable" {
                    ResponseTemplate::new(503)
                } else {
                    ResponseTemplate::new(200).set_body_json(if root {
                        serde_json::json!({"cluster": {"state": error}})
                    } else {
                        serde_json::json!({"state": error})
                    })
                };
            }

            let enabled = if saved {
                state.saved_enabled
            } else {
                state.enabled
            };

            let state = if enabled { "enabled" } else { "disabled" };

            ResponseTemplate::new(200).set_body_json(if root {
                serde_json::json!({"cluster": {"state": state}})
            } else {
                serde_json::json!({"state": state})
            })
        })
        .mount(server)
        .await;

    Mock::given(method("GET")).and(path("/nvue_v1/cluster/apps/nmx-controller")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok", "manager": {"state": "enabled", "encryption": "disabled"}}))).mount(server).await;

    let save_state = Arc::clone(state);
    let save_events = Arc::clone(events);
    let save_id = id.to_owned();

    Mock::given(method("PATCH"))
        .and(path("/nvue_v1/revision/applied"))
        .respond_with(move |_request: &wiremock::Request| {
            let mut state = save_state.lock().unwrap();
            state.saved_enabled = state.enabled;
            save_events
                .lock()
                .unwrap()
                .push(Event::Save(save_id.clone()));

            ResponseTemplate::new(200).set_body_json(serde_json::json!({}))
        })
        .mount(server)
        .await;
}
