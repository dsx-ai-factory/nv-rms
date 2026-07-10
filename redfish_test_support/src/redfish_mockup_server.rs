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

//! Rust-native Redfish mockup server for tests and benchmarks.
//!
//! Serves static Redfish JSON from mockup data directories over HTTPS
//! (self-signed TLS via rcgen + tokio-rustls). Simulates firmware update tasks
//! with configurable delay and failure rate.
//!
//! ## Architecture
//!
//! Each port gets its own axum router with its own JSON cache loaded from
//! the assigned mockup directory. This prevents path collisions when serving
//! different device types (e.g., compute vs. powershelf) on different ports,
//! since both directories contain overlapping Redfish paths like
//! `/redfish/v1/UpdateService/FirmwareInventory`.
//!
//! Simulator configuration is shared across all ports via `SharedState`, so
//! `set_delay()` and `set_failure_rate()` apply globally. Firmware tasks are
//! tracked per-port, so each simulated BMC only sees its own `TaskService/Tasks`
//! (mirroring real hardware and avoiding cross-node preflight false positives).
//!
//! ## Usage
//!
//! ```rust,ignore
//! // E2e tests: different mockup data per port
//! let server = MockRedfishServer::builder()
//!     .add_endpoint(5000, "mockup/compute")
//!     .add_endpoint(5001, "mockup/powershelf")
//!     .start().await;
//!
//! // Benchmarks: same mockup data on many ports (one per simulated BMC)
//! let server = MockRedfishServer::builder()
//!     .add_port_range(16000..26000, "mockup/compute")
//!     .start().await;
//!
//! server.set_delay(0.1);
//! server.set_failure_rate(0.0);
//! server.stop();
//! ```

use std::collections::HashMap;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::PrivatePkcs8KeyDer;
use serde::Deserialize;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

type JsonArchive = HashMap<String, serde_json::Value>;
type SharedJsonArchive = Arc<JsonArchive>;
type ArchiveCache = Mutex<HashMap<String, SharedJsonArchive>>;

// ── Configuration and task simulation ──

/// Runtime-configurable parameters for firmware task simulation.
/// Applied globally across all ports.
#[derive(Debug, Clone)]
struct SimConfig {
    delay_seconds: f32,
    failure_rate: f32,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            delay_seconds: 1.0,
            failure_rate: 0.0,
        }
    }
}

/// A simulated Redfish firmware update task.
/// Transitions from Running -> Completed/Exception after `delay` seconds.
struct SimTask {
    created: Instant,
    delay: f32,
    failed: bool,
}

#[derive(Debug, Deserialize)]
struct ConfigUpdate {
    delay_seconds: Option<f32>,
    failure_rate: Option<f32>,
}

type ResponseResult<T> = Result<T, Box<Response>>;

// ── Server state ──

/// State shared across all ports: simulator configuration only.
struct SharedState {
    config: RwLock<SimConfig>,
}

/// Per-port state. Each port references a JSON cache for its assigned mockup
/// directory. Ports sharing the same directory share the same Arc (no cloning).
/// This prevents path collisions between different device types (e.g., compute
/// vs. powershelf) while being memory-efficient for benchmarks with 10k+ ports.
///
/// Firmware tasks are tracked per-port so that each simulated BMC only sees its
/// own tasks (mirroring real hardware). A single global task map would let one
/// node's `/TaskService/Tasks` scan observe every other node's tasks, which both
/// grows unbounded and causes false-positive "update already running" preflight
/// hits at high concurrency.
struct PortState {
    json_cache: SharedJsonArchive,
    shared: Arc<SharedState>,
    tasks: RwLock<HashMap<String, SimTask>>,
    next_task_id: AtomicU64,
    resource_overrides: RwLock<JsonArchive>,
    unavailable_until: RwLock<Option<Instant>>,
}

// ── Public API ──

/// Handle to a running mockup server. Stops all listeners on drop.
/// Use `ports()` to get the actual bound ports (OS-assigned when port 0 is used).
pub struct MockRedfishServer {
    shutdown_tx: watch::Sender<()>,
    shared: Arc<SharedState>,
    bound_ports: Vec<u16>,
}

pub struct MockRedfishServerBuilder {
    endpoints: Vec<(u16, String)>,
    port_ranges: Vec<(Range<u16>, String)>,
    bind_ip: IpAddr,
}

impl MockRedfishServerBuilder {
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
            port_ranges: Vec::new(),
            bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        }
    }

    /// Bind listeners to the given IP address.
    pub fn bind_ip(mut self, bind_ip: IpAddr) -> Self {
        self.bind_ip = bind_ip;
        self
    }

    /// Add an endpoint serving JSON from the given mockup directory.
    /// Use port 0 to let the OS assign an available port (recommended for tests).
    /// The actual port can be read from `MockRedfishServer::ports()` after start.
    pub fn add_endpoint(mut self, port: u16, mockup_archive: &str) -> Self {
        self.endpoints.push((port, mockup_archive.to_owned()));
        self
    }

    /// Add a range of ports all serving JSON from the same mockup directory.
    /// Used by benchmarks for per-node BMC isolation (one port per node).
    pub fn add_port_range(mut self, range: Range<u16>, mockup_archive: &str) -> Self {
        self.port_ranges.push((range, mockup_archive.to_owned()));
        self
    }

    /// Add N OS-assigned ports all serving JSON from the same mockup directory.
    /// Like `add_port_range` but avoids hardcoded port numbers and TIME_WAIT conflicts.
    pub fn add_auto_ports(mut self, count: usize, mockup_archive: &str) -> Self {
        for _ in 0..count {
            self.endpoints.push((0, mockup_archive.to_owned()));
        }
        self
    }

    /// Start the server. Binds all configured ports with TLS and returns
    /// a handle for runtime configuration and shutdown.
    pub async fn start(self) -> MockRedfishServer {
        const FILE_EXT: &str = ".tar.gz";

        // Process-wide cache of parsed JSON maps, keyed by archive path.
        //
        // Loading and JSON-parsing a tar.gz archive is the dominant cost when
        // starting a mockup server. Tests that use `#[tokio::test]` each get
        // their own tokio runtime, so sharing a single `MockRedfishServer`
        // across tests is not safe — the server's listener tasks are tied to
        // the runtime that spawned them and are cancelled when that runtime
        // drops at test teardown. Each test therefore starts its own server
        // (fresh ports, independent task state), but the read-only JSON data
        // that all those servers serve is identical.
        //
        // `ARCHIVE_CACHE` solves this: the first `start()` call for a given
        // archive path reads the file and parses all JSON, storing an
        // `Arc<HashMap>` under the path key. Every subsequent call clones the
        // `Arc` without touching the file system. `Mutex` guards the map during insertion; after
        // that all access is immutable through the `Arc`, so the hot path (reading an
        // already-populated cache) is a single lock acquire, an `Arc::clone`, and an unlock.
        static ARCHIVE_CACHE: LazyLock<ArchiveCache> = LazyLock::new(|| Mutex::new(HashMap::new()));

        let shared = Arc::new(SharedState {
            config: RwLock::new(SimConfig::default()),
        });

        // Resolve each unique archive path to a shared Arc, loading from disk
        // only on the first encounter.
        let mut dir_caches: HashMap<String, SharedJsonArchive> = HashMap::new();

        let unique_dirs = self
            .endpoints
            .iter()
            .map(|(_, d)| d)
            .chain(self.port_ranges.iter().map(|(_, d)| d));

        for dir in unique_dirs {
            if dir_caches.contains_key(dir) {
                continue;
            }

            let arc = {
                let mut global = ARCHIVE_CACHE.lock().unwrap();

                if let Some(existing) = global.get(dir) {
                    existing.clone()
                } else {
                    let mut cache = HashMap::new();
                    load_mockup_data(dir, FILE_EXT, &mut cache);
                    inject_default_psu_power_state(&mut cache);

                    let arc = Arc::new(cache);
                    global.insert(dir.clone(), arc.clone());

                    arc
                }
            };

            dir_caches.insert(dir.clone(), arc);
        }

        // Flatten all (port, directory) pairs
        let mut port_dirs: Vec<(u16, String)> = Vec::new();
        for (port, dir) in &self.endpoints {
            port_dirs.push((*port, dir.clone()));
        }

        for (range, dir) in &self.port_ranges {
            for port in range.clone() {
                port_dirs.push((port, dir.clone()));
            }
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let tls_acceptor = make_tls_acceptor();

        // Bind all listeners first (port 0 = OS-assigned), collect actual ports
        let mut bound_ports = Vec::new();
        let mut listeners = Vec::new();

        for (requested_port, dir) in &port_dirs {
            let addr = SocketAddr::from((self.bind_ip, *requested_port));
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .unwrap_or_else(|e| panic!("mockup server: failed to bind {addr}: {e}"));

            let actual_port = listener.local_addr().unwrap().port();

            bound_ports.push(actual_port);
            listeners.push((listener, dir.clone()));
        }

        // Spawn a TLS accept loop for each bound listener
        let empty_cache = Arc::new(HashMap::new());

        for (listener, dir) in listeners {
            let json_cache = dir_caches
                .get(&dir)
                .cloned()
                .unwrap_or_else(|| empty_cache.clone());

            let state = Arc::new(PortState {
                json_cache,
                shared: shared.clone(),
                tasks: RwLock::new(HashMap::new()),
                next_task_id: AtomicU64::new(1),
                resource_overrides: RwLock::new(HashMap::new()),
                unavailable_until: RwLock::new(None),
            });

            let tls_acceptor = tls_acceptor.clone();
            let mut rx = shutdown_rx.clone();

            tokio::spawn(async move {
                let app = build_router(state);
                loop {
                    tokio::select! {
                        result = listener.accept() => {
                            let (stream, _) = match result {
                                Ok(s) => s,
                                Err(_) => continue,
                            };
                            let tls = tls_acceptor.clone();
                            let app = app.clone();
                            tokio::spawn(async move {
                                let Ok(tls_stream) = tls.accept(stream).await else { return };
                                let io = hyper_util::rt::TokioIo::new(tls_stream);
                                let svc = hyper_util::service::TowerToHyperService::new(app);
                                let _ = hyper::server::conn::http1::Builder::new()
                                    .serve_connection(io, svc)
                                    .await;
                            });
                        }
                        _ = rx.changed() => break,
                    }
                }
            });
        }

        MockRedfishServer {
            shutdown_tx,
            shared,
            bound_ports,
        }
    }
}

impl MockRedfishServer {
    pub fn builder() -> MockRedfishServerBuilder {
        MockRedfishServerBuilder::new()
    }

    /// Returns the actual bound ports in the same order as `add_endpoint` / `add_port_range` calls.
    pub fn ports(&self) -> &[u16] {
        &self.bound_ports
    }

    /// Set the simulated firmware task completion delay (seconds).
    pub fn set_delay(&self, seconds: f32) {
        assert_valid_task_delay(seconds);
        self.shared.config.write().unwrap().delay_seconds = seconds;
    }

    /// Set the firmware task failure probability (0.0 = always succeed, 1.0 = always fail).
    pub fn set_failure_rate(&self, rate: f32) {
        assert_valid_failure_rate(rate);
        self.shared.config.write().unwrap().failure_rate = rate;
    }

    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

impl Drop for MockRedfishServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

// ── TLS ──

/// Generate a self-signed certificate and build a TLS acceptor.
/// Shared across all ports (same cert, independent TLS sessions per connection).
fn make_tls_acceptor() -> TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert = generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
        .expect("cert generation");

    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der.into())
        .expect("TLS config");

    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];

    TlsAcceptor::from(Arc::new(server_config))
}

// ── Mockup data loading ──

/// Read every `index.json` from `mockup_archive` into the cache.
/// Maps each file's parent directory (relative to the archive's top-level dir)
/// to a URL path.
/// E.g., entry `nvidia-pmc/redfish/v1/Chassis_0/index.json` → `/redfish/v1/Chassis_0`
fn load_mockup_data(mockup_archive: &str, file_ext: &str, cache: &mut JsonArchive) {
    let Ok(file) = std::fs::File::open(mockup_archive) else {
        eprintln!("mockup server: archive not found: {mockup_archive}");
        return;
    };

    // The top-level directory inside the archive matches the last path component
    // of mockup_archive (e.g. "nvidia-pmc" from ".../mockup/nvidia-pmc").
    let prefix = std::path::Path::new(mockup_archive.trim_end_matches(file_ext))
        .file_name()
        .map(std::path::Path::new)
        .unwrap_or_else(|| std::path::Path::new(""));

    let gz = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(gz);

    let entries = match archive.entries() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("mockup server: failed to read archive {mockup_archive}: {e}");
            return;
        }
    };

    for entry in entries {
        let Ok(mut entry) = entry else {
            continue;
        };

        let Ok(path) = entry.path() else {
            continue;
        };

        if path.file_name().and_then(|n| n.to_str()) != Some("index.json") {
            continue;
        }

        let rel = path.strip_prefix(prefix).unwrap_or(&path);
        let parent = match rel.parent() {
            Some(p) => p,
            None => continue,
        };

        // Convert archive path to URL path
        let url_path = format!("/{}", parent.to_string_lossy());
        let url_path = url_path.replace('\\', "/");

        let url_path = if url_path == "/" {
            "/".to_owned()
        } else {
            url_path.trim_end_matches('/').to_owned()
        };

        let mut contents = String::new();
        if entry.read_to_string(&mut contents).is_err() {
            continue;
        }

        if let Ok(value) = serde_json::from_str(&contents) {
            cache.insert(url_path, value);
        } else {
            cache.insert(url_path, serde_json::Value::String(contents));
        }
    }
}

// ── Axum router and handlers ──

fn build_router(state: Arc<PortState>) -> Router {
    Router::new()
        .route(
            "/bmc-sim/config",
            get(handle_config_get).post(handle_config_post),
        )
        .fallback(handle_fallback)
        .with_state(state)
}

const FIRMWARE_UPLOAD_PATHS: &[&str] = &[
    "/redfish/v1/UpdateService/update-multipart",
    "/redfish/v1/UpdateService/update",
];
const TASK_COLLECTION_PATH: &str = "/redfish/v1/TaskService/Tasks";
const TASK_PATH_PREFIX: &str = "/redfish/v1/TaskService/Tasks/";

/// GET /bmc-sim/config — return current delay and failure rate.
async fn handle_config_get(State(state): State<Arc<PortState>>) -> Response {
    let config = state.shared.config.read().unwrap();

    config_response(&config)
}

/// POST /bmc-sim/config — update delay and/or failure rate at runtime.
async fn handle_config_post(State(state): State<Arc<PortState>>, body: String) -> Response {
    let updates = match serde_json::from_str::<ConfigUpdate>(&body) {
        Ok(updates) => updates,
        Err(_) => return bad_request("request body must be valid simulator config"),
    };

    if let Some(delay_seconds) = updates.delay_seconds
        && !is_valid_task_delay(delay_seconds)
    {
        return bad_request("delay_seconds must be non-negative and finite");
    }

    if let Some(failure_rate) = updates.failure_rate
        && !is_valid_failure_rate(failure_rate)
    {
        return bad_request("failure_rate must be in [0.0, 1.0]");
    }

    let mut config = state.shared.config.write().unwrap();
    if let Some(delay_seconds) = updates.delay_seconds {
        config.delay_seconds = delay_seconds;
    }

    if let Some(failure_rate) = updates.failure_rate {
        config.failure_rate = failure_rate;
    }

    config_response(&config)
}

fn config_response(config: &SimConfig) -> Response {
    json_response(
        StatusCode::OK,
        serde_json::json!({
            "delay_seconds": config.delay_seconds,
            "failure_rate": config.failure_rate,
        }),
    )
}

fn assert_valid_task_delay(seconds: f32) {
    assert!(
        is_valid_task_delay(seconds),
        "task delay must be non-negative and finite"
    );
}

fn assert_valid_failure_rate(rate: f32) {
    assert!(
        is_valid_failure_rate(rate),
        "firmware failure rate must be in [0.0, 1.0]"
    );
}

fn is_valid_task_delay(seconds: f32) -> bool {
    seconds.is_finite() && seconds >= 0.0
}

fn is_valid_failure_rate(rate: f32) -> bool {
    rate.is_finite() && (0.0..=1.0).contains(&rate)
}

/// Catch-all handler for all Redfish paths. Routes to:
/// 1. Firmware upload interception (POST/PUT to UpdateService)
/// 2. Task polling (GET TaskService/Tasks/{id})
/// 3. Static JSON from the per-port cache (GET/PATCH/POST)
/// 4. Explicit POST/PATCH action validation for known src Redfish calls
/// 5. 404 for unknown GET paths
async fn handle_fallback(State(state): State<Arc<PortState>>, req: Request<Body>) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let path_trimmed = path.trim_end_matches('/').to_owned();

    if method == Method::GET && path_trimmed == "/redfish/v1" && is_temporarily_unavailable(&state)
    {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error": "BMC temporarily unavailable"}),
        );
    }

    if method == Method::GET && path_trimmed == TASK_COLLECTION_PATH {
        return handle_task_collection(&state);
    }

    if method == Method::GET && path_trimmed.starts_with(TASK_PATH_PREFIX) {
        let task_id = path_trimmed[TASK_PATH_PREFIX.len()..].trim_matches('/');
        if !task_id.is_empty()
            && let Some(response) = handle_task_poll(&state, task_id)
        {
            return response;
        }
    }

    if (method == Method::POST || method == Method::PUT)
        && FIRMWARE_UPLOAD_PATHS.iter().any(|p| path_trimmed == *p)
    {
        let headers = req.headers().clone();
        let body = req.into_body();
        let _ = axum::body::to_bytes(body, 64 * 1024 * 1024).await;

        return handle_firmware_upload(&state, &method, &path_trimmed, &headers);
    }

    if method == Method::POST || method == Method::PATCH {
        let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
            .await
            .unwrap_or_default();

        return handle_action_request(&state, &method, &path_trimmed, &body);
    }

    let lookup = if path_trimmed.is_empty() {
        "/"
    } else {
        &path_trimmed
    };

    if let Some(json) = lookup_resource(&state, lookup) {
        json_response(StatusCode::OK, json)
    } else {
        json_response(
            StatusCode::NOT_FOUND,
            serde_json::json!({"error": "not found", "path": path}),
        )
    }
}

fn lookup_resource(state: &PortState, path: &str) -> Option<serde_json::Value> {
    if let Some(json) = state.resource_overrides.read().unwrap().get(path) {
        return Some(json.clone());
    }

    state.json_cache.get(path).cloned()
}

fn is_temporarily_unavailable(state: &PortState) -> bool {
    let now = Instant::now();

    {
        let unavailable_until = state.unavailable_until.read().unwrap();
        match *unavailable_until {
            Some(until) if until > now => return true,
            Some(_) => {}
            None => return false,
        }
    }

    *state.unavailable_until.write().unwrap() = None;
    false
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    response_from_json(status, body.to_string())
}

fn response_from_json(status: StatusCode, body: String) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap_or_else(|_| Response::new(Body::from("{}")))
}

fn empty_success() -> Response {
    json_response(StatusCode::OK, serde_json::json!({}))
}

fn bad_request(message: &str) -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"error": message}),
    )
}

fn parse_json_body(body: &[u8]) -> ResponseResult<serde_json::Value> {
    serde_json::from_slice(body)
        .map_err(|_| Box::new(bad_request("request body must be valid JSON")))
}

fn handle_action_request(state: &PortState, method: &Method, path: &str, body: &[u8]) -> Response {
    if method == Method::POST && path.ends_with("/Actions/ComputerSystem.Reset") {
        return handle_computer_system_reset(state, body);
    }

    if method == Method::POST && path.ends_with("/Actions/Manager.Reset") {
        return handle_manager_reset(state, body);
    }

    if method == Method::POST && path.ends_with("/Actions/Oem/NvidiaChassis.AuxPowerReset") {
        return handle_aux_power_reset(state, body);
    }

    if method == Method::POST && path == "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff" {
        return handle_powershelf_force_off(state, body);
    }

    if method == Method::POST && path == "/redfish/v1/Chassis/powershelf/Actions/Chassis.On" {
        return handle_powershelf_on(state, body);
    }

    if method == Method::POST && path.ends_with("/Actions/Chassis.Reset") {
        return handle_chassis_reset(state, path, body);
    }

    if method == Method::POST
        && (path.ends_with(
            "/Oem/Nvidia/WorkloadPowerProfile/Actions/NvidiaWorkloadPower.EnableProfiles",
        ) || path.ends_with(
            "/Oem/Nvidia/WorkloadPowerProfile/Actions/NvidiaWorkloadPower.DisableProfiles",
        ))
    {
        return handle_workload_profile_action(body);
    }

    if method == Method::PATCH && path == "/redfish/v1/UpdateService" {
        return handle_update_service_patch(body);
    }

    if method == Method::PATCH && path.ends_with("/EnvironmentMetrics") {
        return handle_environment_metrics_patch(state, path, body);
    }

    json_response(
        StatusCode::NOT_FOUND,
        serde_json::json!({"error": "unsupported Redfish action", "path": path}),
    )
}

fn handle_computer_system_reset(state: &PortState, body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    let Some(reset_type) = data.get("ResetType").and_then(|v| v.as_str()) else {
        return bad_request("ResetType is required");
    };

    if !is_allowed_reset_type(reset_type) {
        return bad_request("ResetType is not supported by mock");
    }

    let power_state = match reset_type {
        "On" | "ForceOn" | "PowerCycle" | "GracefulRestart" | "ForceRestart" => Some("On"),
        "GracefulShutdown" | "ForceOff" => Some("Off"),
        _ => None,
    };

    if let Some(power_state) = power_state {
        set_power_state(state, "/redfish/v1/Systems/System_0", power_state);
    }

    empty_success()
}

fn handle_manager_reset(state: &PortState, body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    let Some(reset_type) = data.get("ResetType").and_then(|v| v.as_str()) else {
        return bad_request("ResetType is required");
    };

    if !is_allowed_reset_type(reset_type) {
        return bad_request("ResetType is not supported by mock");
    }

    if reset_type_triggers_controller_restart(reset_type) {
        *state.unavailable_until.write().unwrap() =
            Some(Instant::now() + std::time::Duration::from_secs(2));
    }

    empty_success()
}

fn handle_aux_power_reset(state: &PortState, body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    if data.get("ResetType").and_then(|v| v.as_str()) != Some("AuxPowerCycle") {
        return bad_request("ResetType must be AuxPowerCycle");
    }

    set_power_state(state, "/redfish/v1/Systems/System_0", "Off");
    *state.unavailable_until.write().unwrap() =
        Some(Instant::now() + std::time::Duration::from_millis(100));

    empty_success()
}

fn handle_powershelf_force_off(state: &PortState, body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    if data.get("ForceOffType").and_then(|v| v.as_str()) != Some("ForceOff") {
        return bad_request("ForceOffType must be ForceOff");
    }

    set_chassis_and_psu_power_state(state, "/redfish/v1/Chassis/PowerShelf_0", "Off");
    empty_success()
}

fn handle_powershelf_on(state: &PortState, body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    if data.get("OnType").and_then(|v| v.as_str()) != Some("On") {
        return bad_request("OnType must be On");
    }

    set_chassis_and_psu_power_state(state, "/redfish/v1/Chassis/PowerShelf_0", "On");
    empty_success()
}

/// Generic `#Chassis.Reset` handler used by chassis whose Redfish surface only
/// exposes the standard action (e.g. NVIDIA PMC powershelf). Side-effects the
/// chassis path inferred from the action URL so subsequent `PowerState` reads
/// observe the change.
fn handle_chassis_reset(state: &PortState, path: &str, body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    let Some(reset_type) = data.get("ResetType").and_then(|v| v.as_str()) else {
        return bad_request("ResetType is required");
    };

    if !is_allowed_reset_type(reset_type) {
        return bad_request("ResetType is not supported by mock");
    }

    let power_state = match reset_type {
        "On" | "ForceOn" | "PowerCycle" | "GracefulRestart" | "ForceRestart" => Some("On"),
        "GracefulShutdown" | "ForceOff" => Some("Off"),
        _ => None,
    };

    if let Some(power_state) = power_state {
        let chassis_path = path.trim_end_matches("/Actions/Chassis.Reset");
        // A chassis with a PowerSubsystem.PowerSupplies collection (i.e. a
        // powershelf) needs the PSU-level state to track the chassis-level
        // state, since clients now compute the shelf state from PSUs.
        if has_power_supplies(state, chassis_path) {
            set_chassis_and_psu_power_state(state, chassis_path, power_state);
        } else {
            set_power_state(state, chassis_path, power_state);
        }
    }

    empty_success()
}

fn handle_workload_profile_action(body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    let profile_mask = data
        .get("ProfileMask")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    if profile_mask.is_empty() {
        return bad_request("ProfileMask is required");
    }

    empty_success()
}

fn handle_update_service_patch(body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    let Some(apply_time) = data
        .pointer("/HttpPushUriOptions/HttpPushUriApplyTime/ApplyTime")
        .and_then(|v| v.as_str())
    else {
        return bad_request("HttpPushUriApplyTime.ApplyTime is required");
    };

    if !matches!(apply_time, "Immediate" | "OnReset") {
        return bad_request("ApplyTime is not supported by mock");
    }

    empty_success()
}

fn handle_environment_metrics_patch(state: &PortState, path: &str, body: &[u8]) -> Response {
    let data = match parse_json_body(body) {
        Ok(data) => data,
        Err(response) => return *response,
    };

    let Some(setpoint) = data
        .pointer("/PowerLimitWatts/SetPoint")
        .and_then(|v| v.as_i64())
    else {
        return bad_request("PowerLimitWatts.SetPoint is required");
    };

    if let Some(resource) = lookup_resource(state, path) {
        let min = resource
            .pointer("/PowerLimitWatts/AllowableMin")
            .and_then(|v| v.as_i64());
        let max = resource
            .pointer("/PowerLimitWatts/AllowableMax")
            .and_then(|v| v.as_i64());

        if min.is_some_and(|m| setpoint < m) || max.is_some_and(|m| setpoint > m) {
            return bad_request("PowerLimitWatts.SetPoint is outside allowable range");
        }
    }

    set_metric_setpoint(state, path, setpoint);
    empty_success()
}

fn is_allowed_reset_type(reset_type: &str) -> bool {
    matches!(
        reset_type,
        "On" | "ForceOn"
            | "GracefulShutdown"
            | "ForceOff"
            | "PowerCycle"
            | "GracefulRestart"
            | "ForceRestart"
            | "Nmi"
    )
}

fn reset_type_triggers_controller_restart(reset_type: &str) -> bool {
    matches!(
        reset_type,
        "PowerCycle" | "GracefulRestart" | "ForceRestart"
    )
}

fn set_power_state(state: &PortState, path: &str, power_state: &str) {
    let mut resource =
        lookup_resource(state, path).unwrap_or_else(|| serde_json::json!({"@odata.id": path}));

    if let Some(obj) = resource.as_object_mut() {
        obj.insert(
            "PowerState".to_owned(),
            serde_json::Value::String(power_state.to_owned()),
        );
    }

    state
        .resource_overrides
        .write()
        .unwrap()
        .insert(path.to_owned(), resource);
}

/// Update the chassis `PowerState` and the `PowerState` on every PSU listed
/// under its `PowerSubsystem.PowerSupplies` collection. Used for powershelf
/// chassis so clients that derive shelf state from PSU state observe the
/// expected value after a power action.
fn set_chassis_and_psu_power_state(state: &PortState, chassis_path: &str, power_state: &str) {
    set_power_state(state, chassis_path, power_state);
    for psu_path in psu_paths_under_chassis(state, chassis_path) {
        set_power_state(state, &psu_path, power_state);
    }
}

/// True when the chassis at `chassis_path` exposes a `PowerSubsystem` link
/// whose `PowerSupplies` collection has at least one member in the static
/// mockup. Used to decide whether a generic `#Chassis.Reset` should cascade.
fn has_power_supplies(state: &PortState, chassis_path: &str) -> bool {
    !psu_paths_under_chassis(state, chassis_path).is_empty()
}

/// Resolve `chassis -> PowerSubsystem -> PowerSupplies -> members` against the
/// static cache and return the member paths. The cache holds the original
/// (unmodified) PowerSubsystem and collection documents, so this resolution is
/// stable regardless of how many overrides have been applied to PSU bodies.
fn psu_paths_under_chassis(state: &PortState, chassis_path: &str) -> Vec<String> {
    let Some(chassis) = lookup_resource(state, chassis_path) else {
        return Vec::new();
    };
    let Some(subsystem_path) = chassis
        .pointer("/PowerSubsystem/@odata.id")
        .and_then(|v| v.as_str())
    else {
        return Vec::new();
    };

    let Some(subsystem) = lookup_resource(state, subsystem_path) else {
        return Vec::new();
    };
    let Some(supplies_path) = subsystem
        .pointer("/PowerSupplies/@odata.id")
        .and_then(|v| v.as_str())
    else {
        return Vec::new();
    };

    let Some(collection) = lookup_resource(state, supplies_path) else {
        return Vec::new();
    };
    let Some(members) = collection.get("Members").and_then(|m| m.as_array()) else {
        return Vec::new();
    };

    members
        .iter()
        .filter_map(|m| {
            m.get("@odata.id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        })
        .collect()
}

/// Mockup-time fixup: real powershelf hardware reports `PowerState: "On"` on
/// each PSU when the rail is energized, but the static mockup JSON omits the
/// field. Inject the default so clients that compute shelf state from PSU
/// state see the expected `On` immediately after a fresh load.
fn inject_default_psu_power_state(cache: &mut JsonArchive) {
    for value in cache.values_mut() {
        let Some(obj) = value.as_object_mut() else {
            continue;
        };
        let is_psu = obj
            .get("@odata.type")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.starts_with("#PowerSupply."));
        if is_psu && !obj.contains_key("PowerState") {
            obj.insert(
                "PowerState".to_owned(),
                serde_json::Value::String("On".to_owned()),
            );
        }
    }
}

fn set_metric_setpoint(state: &PortState, path: &str, setpoint: i64) {
    let mut resource =
        lookup_resource(state, path).unwrap_or_else(|| serde_json::json!({"@odata.id": path}));

    if let Some(obj) = resource.as_object_mut()
        && let Some(power_limit) = obj
            .get_mut("PowerLimitWatts")
            .and_then(|v| v.as_object_mut())
    {
        power_limit.insert(
            "SetPoint".to_owned(),
            serde_json::Value::Number(setpoint.into()),
        );
    }

    state
        .resource_overrides
        .write()
        .unwrap()
        .insert(path.to_owned(), resource);
}

fn validate_upload_request(method: &Method, path: &str, headers: &HeaderMap) -> ResponseResult<()> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    if path == "/redfish/v1/UpdateService/update-multipart" {
        if method != Method::POST {
            return Err(Box::new(json_response(
                StatusCode::METHOD_NOT_ALLOWED,
                serde_json::json!({"error": "multipart update requires POST"}),
            )));
        }

        if !content_type.starts_with("multipart/form-data") {
            return Err(Box::new(json_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                serde_json::json!({"error": "multipart update requires multipart/form-data"}),
            )));
        }

        return Ok(());
    }

    if path == "/redfish/v1/UpdateService/update" {
        if method != Method::POST && method != Method::PUT {
            return Err(Box::new(json_response(
                StatusCode::METHOD_NOT_ALLOWED,
                serde_json::json!({"error": "raw update requires POST or PUT"}),
            )));
        }

        if content_type != "application/octet-stream" {
            return Err(Box::new(json_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                serde_json::json!({"error": "raw update requires application/octet-stream"}),
            )));
        }

        return Ok(());
    }

    Err(Box::new(json_response(
        StatusCode::NOT_FOUND,
        serde_json::json!({"error": "unsupported firmware upload path", "path": path}),
    )))
}

/// Simulate a firmware upload: create a task that completes after the configured
/// delay, with failure determined by the configured failure_rate at upload time.
/// Returns 202 Accepted with a Location header pointing to the task.
fn handle_firmware_upload(
    state: &PortState,
    method: &Method,
    path: &str,
    headers: &HeaderMap,
) -> Response {
    if let Err(response) = validate_upload_request(method, path, headers) {
        return *response;
    }

    let config = state.shared.config.read().unwrap();
    let delay = config.delay_seconds;
    let failure_rate = config.failure_rate;
    drop(config);

    let task_sequence = state.next_task_id.fetch_add(1, Ordering::Relaxed);
    let task_id = task_sequence.to_string();
    let failed = rand_failure(failure_rate, task_sequence);

    state.tasks.write().unwrap().insert(
        task_id.clone(),
        SimTask {
            created: Instant::now(),
            delay,
            failed,
        },
    );

    let body = serde_json::json!({
        "@odata.id": format!("/redfish/v1/TaskService/Tasks/{task_id}"),
    });

    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            header::LOCATION,
            format!("/redfish/v1/TaskService/Tasks/{task_id}"),
        )
        .body(Body::from(body.to_string()))
        .unwrap_or_else(|_| Response::new(Body::from("{}")))
}

fn handle_task_collection(state: &PortState) -> Response {
    let tasks = state.tasks.read().unwrap();
    if tasks.is_empty()
        && let Some(static_body) = lookup_resource(state, TASK_COLLECTION_PATH)
    {
        return json_response(StatusCode::OK, static_body);
    }

    let members: Vec<serde_json::Value> = tasks
        .keys()
        .map(|id| {
            serde_json::json!({
                "@odata.id": format!("/redfish/v1/TaskService/Tasks/{id}"),
            })
        })
        .collect();

    json_response(
        StatusCode::OK,
        serde_json::json!({
            "@odata.id": TASK_COLLECTION_PATH,
            "Members": members,
            "Members@odata.count": tasks.len(),
        }),
    )
}

/// Return Redfish Task JSON. State transitions based on wall clock time:
/// - elapsed < delay -> Running (with percent estimate)
/// - elapsed >= delay && !failed -> Completed (200)
/// - elapsed >= delay && failed -> Exception (200)
fn handle_task_poll(state: &PortState, task_id: &str) -> Option<Response> {
    let tasks = state.tasks.read().unwrap();
    let task = tasks.get(task_id)?;

    let elapsed = task.created.elapsed().as_secs_f32();
    let (task_state, percent, status_code) = if elapsed < task.delay {
        let pct = ((elapsed / task.delay) * 100.0).min(99.0) as u32;
        ("Running", pct, StatusCode::OK)
    } else if task.failed {
        ("Exception", 0u32, StatusCode::OK)
    } else {
        ("Completed", 100u32, StatusCode::OK)
    };

    let task_status = if task_state == "Exception" {
        "Critical"
    } else {
        "OK"
    };
    let message = match task_state {
        "Running" => "Firmware update in progress",
        "Completed" => "Firmware update completed successfully",
        _ => "Firmware update failed",
    };

    let body = serde_json::json!({
        "@odata.id": format!("/redfish/v1/TaskService/Tasks/{task_id}"),
        "@odata.type": "#Task.v1_4_3.Task",
        "Id": task_id,
        "TaskState": task_state,
        "TaskStatus": task_status,
        "PercentComplete": percent,
        "StartTime": "2026-01-01T00:00:00Z",
        "EndTime": if task_state == "Running" {
            serde_json::Value::Null
        } else {
            serde_json::Value::String("2026-01-01T00:00:01Z".to_owned())
        },
        "Messages": [{ "Message": message }],
    });

    Some(json_response(status_code, body))
}

fn rand_failure(failure_rate: f32, sequence: u64) -> bool {
    if failure_rate <= 0.0 {
        return false;
    }

    if failure_rate >= 1.0 {
        return true;
    }

    let r = mix_u64(sequence) as f32 / u64::MAX as f32;
    r < failure_rate
}

fn mix_u64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
