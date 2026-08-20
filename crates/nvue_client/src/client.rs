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

use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, RequestBuilder};
use rustls::pki_types::ServerName;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

use crate::ClientTls;
use crate::cluster::{
    CLUSTER_ENDPOINT, Cluster, ClusterApp, ClusterNodeServer, ClusterNodeServerAddresses,
    ClusterNodeServerMap, InterfaceType, app_endpoint, cluster_node_endpoint_for_revision,
    cluster_node_server_endpoint_for_revision,
};
use crate::tls::{TlsSnapshot, load_stable_snapshot};

const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Plain-HTTP NVUE requests open a fresh connection instead of reusing pooled
/// keep-alive connections. Control-plane HTTP servers (NVOS/NVUE, and the
/// wiremock servers used in tests) routinely close idle connections; reusing a
/// half-closed pooled connection surfaces as a spurious `error sending request`
/// transient failure — a notable source of test flakiness under a loaded
/// runtime. The extra TCP connect per plain request is negligible, and there is
/// no TLS handshake to amortize on this path (the client-TLS transports keep
/// their pools).
const PLAIN_POOL_MAX_IDLE_PER_HOST: usize = 0;

/// Default timeout for ordinary NVUE control-plane requests.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared NVUE client handle.
pub type SharedClient = Arc<Client>;

/// URL scheme used for NVUE requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportScheme {
    /// Plain HTTP for explicit deployments and test servers.
    Http,

    /// HTTPS with server authentication.
    Https,
}

impl TransportScheme {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

// Certificate installation can require verification even while the active
// bootstrap transport accepts an untrusted server certificate.
#[derive(Clone, Copy)]
enum ServerVerification {
    Configured,
    Required,
}

/// NVUE URL authority and TCP connection target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientEndpoint {
    /// Host placed in the URL and used for TLS SNI.
    pub host: String,

    /// Host or IP address used for the TCP connection.
    pub connect_host: String,

    /// NVUE service port.
    pub port: u16,

    scheme: TransportScheme,
}

impl ClientEndpoint {
    /// Build an endpoint from a management address and optional DNS hostname.
    pub fn new(ip_address: &str, port: u16, host_name: Option<&str>) -> Result<Self, ClientError> {
        normalize_host(ip_address)
            .parse::<IpAddr>()
            .map_err(|error| {
                ClientError::InvalidEndpoint(format!(
                    "switch management IP {ip_address} is invalid: {error}"
                ))
            })?;

        let host =
            normalize_host(Self::nvue_http_host_from_endpoint(ip_address, host_name)?).to_owned();

        let endpoint = Self {
            host,
            connect_host: normalize_host(ip_address).to_owned(),
            port,
            scheme: TransportScheme::Https,
        };

        endpoint.validate()?;

        Ok(endpoint)
    }

    /// Create an HTTPS endpoint.
    pub fn https(tls_host: impl Into<String>, connect_host: impl Into<String>, port: u16) -> Self {
        Self {
            host: tls_host.into(),
            connect_host: connect_host.into(),
            port,
            scheme: TransportScheme::Https,
        }
    }

    /// Create a plain HTTP endpoint.
    pub fn http(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();

        Self {
            host: host.clone(),
            connect_host: host,
            port,
            scheme: TransportScheme::Http,
        }
    }

    /// Parse an HTTP(S) base URL containing only a scheme and authority.
    pub fn from_base_url(base_url: &str) -> Result<Self, ClientError> {
        let url = reqwest::Url::parse(base_url).map_err(|error| {
            ClientError::InvalidEndpoint(format!("invalid NVUE base URL {base_url}: {error}"))
        })?;

        if !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ClientError::InvalidEndpoint(format!(
                "NVUE base URL must contain only an authority: {base_url}"
            )));
        }

        let host = url.host_str().ok_or_else(|| {
            ClientError::InvalidEndpoint(format!("NVUE base URL has no host: {base_url}"))
        })?;

        let port = url.port_or_known_default().ok_or_else(|| {
            ClientError::InvalidEndpoint(format!("NVUE base URL has no port: {base_url}"))
        })?;

        match url.scheme() {
            "http" => Ok(Self::http(host, port)),
            "https" => Ok(Self::https(host, host, port)),
            scheme => Err(ClientError::InvalidEndpoint(format!(
                "unsupported NVUE transport scheme {scheme}"
            ))),
        }
    }

    /// Select the URL/TLS host from a required management IP and optional hostname.
    pub fn nvue_http_host_from_endpoint<'a>(
        ip_address: &'a str,
        host_name: Option<&'a str>,
    ) -> Result<&'a str, ClientError> {
        if ip_address.trim().is_empty() {
            return Err(ClientError::InvalidEndpoint(
                "switch management IP is required for NVUE operations".into(),
            ));
        }

        Ok(host_name
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .unwrap_or(ip_address))
    }

    fn validate(&self) -> Result<(), ClientError> {
        if self.host.trim().is_empty() {
            return Err(ClientError::InvalidEndpoint(
                "NVUE TLS/URL host is empty".into(),
            ));
        }

        if self.connect_host.trim().is_empty() {
            return Err(ClientError::InvalidEndpoint(
                "NVUE connection host is empty".into(),
            ));
        }

        validate_server_name(&self.host)?;

        if normalize_host(&self.host) != normalize_host(&self.connect_host) {
            let connect_ip = normalize_host(&self.connect_host)
                .parse::<IpAddr>()
                .map_err(|error| {
                    ClientError::InvalidEndpoint(format!(
                        "NVUE connection host {} must be a management IP address when TLS authority differs: {error}",
                        self.connect_host
                    ))
                })?;

            if let Ok(authority_ip) = normalize_host(&self.host).parse::<IpAddr>()
                && authority_ip != connect_ip
            {
                return Err(ClientError::InvalidEndpoint(format!(
                    "NVUE IP authority {} does not match management IP {}",
                    self.host, self.connect_host
                )));
            }
        }

        if self.port == 0 {
            return Err(ClientError::InvalidEndpoint(
                "NVUE service port must not be zero".into(),
            ));
        }

        Ok(())
    }

    fn base_url(&self, scheme: TransportScheme, host: &str) -> String {
        format!(
            "{}://{}:{}",
            scheme.as_str(),
            format_url_host(host),
            self.port
        )
    }
}

/// NVUE basic-auth credentials.
#[derive(Clone)]
pub struct ClientCredentials {
    /// HTTP basic-auth username.
    pub username: String,

    /// HTTP basic-auth password, redacted by its `Debug` implementation.
    pub password: SecretString,
}

impl ClientCredentials {
    /// Create credentials from a username and password.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: SecretString::from(password.into()),
        }
    }

    /// Return the configured username.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Return the configured password.
    pub fn password(&self) -> &str {
        self.password.expose_secret()
    }
}

impl fmt::Debug for ClientCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Connection settings for one NVUE client.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// URL and TCP target.
    pub endpoint: ClientEndpoint,

    /// HTTP basic-auth credentials.
    pub credentials: ClientCredentials,

    /// Disable server certificate validation when explicitly requested.
    pub dangerously_accept_invalid_certs: bool,
}

/// Raw NVUE response retained for callers that need legacy response details.
#[derive(Clone, Debug)]
pub struct NvueResponse {
    /// HTTP status code.
    pub status: u16,

    /// Parsed JSON value, or `null` when the body is not valid JSON.
    pub value: Value,

    /// Response body decoded lossily as UTF-8.
    pub body: String,

    json_error: Option<String>,
}

impl NvueResponse {
    /// Return whether the status is in the HTTP success range.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Validate a successful JSON response and return its parsed value.
    pub fn into_json(self, method: &'static str, path: &str) -> Result<Value, ClientError> {
        if !self.is_success() {
            return Err(ClientError::HttpStatus {
                method,
                path: path.to_owned(),
                status: self.status,
                body: self.body,
            });
        }

        if let Some(message) = self.json_error {
            return Err(ClientError::InvalidJson {
                method,
                path: path.to_owned(),
                message,
            });
        }

        Ok(self.value)
    }
}

/// Error returned by NVUE transport and response handling.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Endpoint configuration is empty or invalid.
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),

    /// An NVUE request payload could not be encoded.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// NVUE response did not contain a required API value.
    #[error("{0}")]
    InvalidResponse(String),

    /// The TCP connection target could not be resolved.
    #[error("failed to resolve NVUE host {host}:{port}: {source}")]
    ResolveHost {
        /// Host that failed to resolve.
        host: String,

        /// Port passed to the resolver.
        port: u16,

        /// Resolver failure.
        #[source]
        source: std::io::Error,
    },

    /// Name resolution succeeded without yielding an address.
    #[error("no addresses resolved for NVUE host {host}:{port}")]
    NoResolvedAddress {
        /// Host passed to the resolver.
        host: String,

        /// Port passed to the resolver.
        port: u16,
    },

    /// A TLS PEM file could not be read.
    #[error("failed to read NVUE TLS {description} {path}: {source}")]
    ReadTlsMaterial {
        /// PEM role being read.
        description: &'static str,

        /// PEM path being read.
        path: PathBuf,

        /// Filesystem failure.
        #[source]
        source: std::io::Error,
    },

    /// The PEM files changed throughout the bounded stable-read window.
    #[error(
        "NVUE client TLS PEM files did not produce two matching snapshots after {attempts} attempts"
    )]
    UnstableTlsMaterial {
        /// Number of comparisons attempted.
        attempts: usize,
    },

    /// TLS material could not be parsed or applied.
    #[error("invalid NVUE TLS material: {0}")]
    InvalidTlsMaterial(String),

    /// The active transport changed after a client-TLS candidate was prepared.
    #[error("active NVUE transport changed before prepared client TLS was activated")]
    StalePreparedTransport,

    /// A prepared transport was passed to a different NVUE client.
    #[error("prepared client TLS belongs to a different NVUE client")]
    ForeignPreparedTransport,

    /// A request or response-body read failed.
    #[error("{operation}: {source}")]
    Request {
        /// Request operation being performed.
        operation: String,

        /// Reqwest failure.
        #[source]
        source: reqwest::Error,
    },

    /// The response exceeded the fixed control-plane body limit.
    #[error("NVUE response for {method} {path} exceeded {limit} bytes")]
    ResponseTooLarge {
        /// HTTP method.
        method: &'static str,

        /// NVUE request path.
        path: String,

        /// Maximum accepted body size.
        limit: usize,
    },

    /// A JSON convenience method received a non-success response.
    #[error("HTTP {method} {path} returned {status}")]
    HttpStatus {
        /// HTTP method.
        method: &'static str,

        /// NVUE request path.
        path: String,

        /// HTTP status code.
        status: u16,

        /// Raw response body.
        body: String,
    },

    /// A successful response did not contain valid JSON.
    #[error("invalid JSON in NVUE {method} {path} response: {message}")]
    InvalidJson {
        /// HTTP method.
        method: &'static str,

        /// NVUE request path.
        path: String,

        /// JSON parser message.
        message: String,
    },
}

impl ClientError {
    /// Create an API-level error for a malformed NVUE response.
    pub fn invalid_response(message: impl Into<String>) -> Self {
        Self::InvalidResponse(message.into())
    }

    /// Returns `true` if the request failed while validating the server certificate.
    pub fn is_server_certificate_validation_error(&self) -> bool {
        let Self::Request { source, .. } = self else {
            return false;
        };

        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(source);

        while let Some(error) = current {
            if matches!(
                error.downcast_ref::<rustls::Error>(),
                Some(rustls::Error::InvalidCertificate(_))
            ) {
                return true;
            }

            current = error
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::get_ref)
                .map(|source| source as &(dyn std::error::Error + 'static))
                .or_else(|| error.source());
        }

        false
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClientGeneration {
    Plain,
    ClientTls {
        fingerprint: [u8; 32],
        server_name: String,

        // Include server-certificate verification in the generation identity.
        // Activation accepts a stale candidate only when its TLS material,
        // server name, and verification policy match the active transport;
        // otherwise verified and certificate-bypassing transports could be
        // mistaken for the same generation.
        dangerously_accept_invalid_certs: bool,
    },
}

struct ClientState {
    transport: Arc<Transport>,
    generation: ClientGeneration,
    revision: u64,
    config: ClientConfig,
    tls: Option<ClientTls>,
    scheme: TransportScheme,
}

/// Fully built NVUE client-TLS transport awaiting a successful read-only probe.
pub struct PreparedClientTls {
    owner: Arc<()>,
    based_on_revision: u64,
    transport: Arc<Transport>,
    generation: ClientGeneration,
    config: ClientConfig,
    tls: ClientTls,
    material_fingerprint: [u8; 32],
}

impl PreparedClientTls {
    /// Fingerprint of the CA, client certificate, and private key snapshot.
    pub fn material_fingerprint(&self) -> [u8; 32] {
        self.material_fingerprint
    }
}

#[derive(Clone, Copy)]
enum ResponseMode {
    Bounded,
    ImportedLegacy,
}

struct Transport {
    client: reqwest::Client,
    base_url: String,
    credentials: ClientCredentials,
    send_empty_basic_auth: bool,
    response_mode: ResponseMode,
}

impl Transport {
    async fn send<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        payload: Option<&T>,
        if_match: Option<&str>,
        timeout: Duration,
    ) -> Result<NvueResponse, ClientError> {
        let method_name = method_name(&method);
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.base_url))
            .header(CONTENT_TYPE, "application/json")
            .timeout(timeout);

        if self.send_empty_basic_auth || !self.credentials.username.is_empty() {
            request = request.basic_auth(
                &self.credentials.username,
                Some(self.credentials.password.expose_secret()),
            );
        }

        if let Some(payload) = payload {
            request = request.json(payload);
        }

        if let Some(if_match) = if_match {
            request = request.header("If-Match", if_match);
        }

        let response = send_request(request, method_name, path).await?;

        read_response(response, method_name, path, self.response_mode).await
    }

    async fn probe(&self, path: &str, timeout: Duration) -> Result<u16, ClientError> {
        let mut request = self
            .client
            .get(format!("{}{path}", self.base_url))
            .header(CONTENT_TYPE, "application/json")
            .timeout(timeout);

        if self.send_empty_basic_auth || !self.credentials.username.is_empty() {
            request = request.basic_auth(
                &self.credentials.username,
                Some(self.credentials.password.expose_secret()),
            );
        }

        Ok(send_request(request, "GET", path).await?.status().as_u16())
    }
}

/// Stable NVUE handle with atomically replaceable immutable transports.
pub struct Client {
    owner: Arc<()>,
    state: StdMutex<ClientState>,

    /// Serializes complete replacement construction, never ordinary requests.
    reconfigure: AsyncMutex<()>,
}

impl Client {
    /// Build a plain NVUE client synchronously.
    pub fn new(config: ClientConfig) -> Result<SharedClient, ClientError> {
        config.endpoint.validate()?;

        let scheme = config.endpoint.scheme;
        let transport = build_plain_transport(&config, scheme)?;

        Ok(Arc::new(Self {
            owner: Arc::new(()),
            state: StdMutex::new(ClientState {
                transport: Arc::new(transport),
                generation: ClientGeneration::Plain,
                revision: 0,
                config,
                tls: None,
                scheme,
            }),
            reconfigure: AsyncMutex::new(()),
        }))
    }

    /// Wrap an existing reqwest transport while retaining NVUE endpoint and
    /// credential metadata. This preserves caller-specific trust roots and
    /// proxy settings and always sends basic auth like reqwest-based legacy
    /// callers; later transport reconfiguration uses `config`.
    pub fn from_reqwest(
        config: ClientConfig,
        client: reqwest::Client,
    ) -> Result<SharedClient, ClientError> {
        config.endpoint.validate()?;

        if normalize_host(&config.endpoint.host) != normalize_host(&config.endpoint.connect_host) {
            return Err(ClientError::InvalidEndpoint(
                "imported reqwest clients require matching NVUE URL and connection hosts".into(),
            ));
        }

        let scheme = config.endpoint.scheme;

        let transport = Transport {
            client,
            base_url: config.endpoint.base_url(scheme, &config.endpoint.host),
            credentials: config.credentials.clone(),
            send_empty_basic_auth: true,
            response_mode: ResponseMode::ImportedLegacy,
        };

        Ok(Arc::new(Self {
            owner: Arc::new(()),
            state: StdMutex::new(ClientState {
                transport: Arc::new(transport),
                generation: ClientGeneration::Plain,
                revision: 0,
                config,
                tls: None,
                scheme,
            }),
            reconfigure: AsyncMutex::new(()),
        }))
    }

    /// Build an NVUE client and configure its initial transport before returning it.
    pub async fn connect(
        config: ClientConfig,
        tls: Option<ClientTls>,
    ) -> Result<SharedClient, ClientError> {
        config.endpoint.validate()?;

        let scheme = config.endpoint.scheme;
        let (transport, generation) = build_transport(&config, tls.as_ref(), scheme).await?;

        Ok(Arc::new(Self {
            owner: Arc::new(()),
            state: StdMutex::new(ClientState {
                transport: Arc::new(transport),
                generation,
                revision: 0,
                config,
                tls,
                scheme,
            }),
            reconfigure: AsyncMutex::new(()),
        }))
    }

    /// Send a GET and retain status, parsed JSON, and raw body.
    pub async fn get(&self, path: &str, timeout: Duration) -> Result<NvueResponse, ClientError> {
        self.send::<Value>(Method::GET, path, None, None, timeout)
            .await
    }

    /// Send a GET and return once response headers arrive, without reading the body.
    pub async fn probe(&self, path: &str, timeout: Duration) -> Result<u16, ClientError> {
        self.transport_snapshot().probe(path, timeout).await
    }

    /// Send a POST and retain status, parsed JSON, and raw body.
    pub async fn post<T: Serialize + ?Sized>(
        &self,
        path: &str,
        payload: &T,
        timeout: Duration,
    ) -> Result<NvueResponse, ClientError> {
        self.send(Method::POST, path, Some(payload), None, timeout)
            .await
    }

    /// Send a PATCH and retain status, parsed JSON, and raw body.
    pub async fn patch(
        &self,
        path: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<NvueResponse, ClientError> {
        self.send(Method::PATCH, path, Some(payload), None, timeout)
            .await
    }

    /// Send a DELETE and retain status, parsed JSON, and raw body.
    pub async fn delete(&self, path: &str, timeout: Duration) -> Result<NvueResponse, ClientError> {
        self.send::<Value>(Method::DELETE, path, None, None, timeout)
            .await
    }

    /// Send a GET and require a successful JSON response.
    pub async fn get_json(&self, path: &str, timeout: Duration) -> Result<Value, ClientError> {
        self.get(path, timeout).await?.into_json("GET", path)
    }

    /// Reads and decodes the NVUE cluster state.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the successful response does
    /// not match the NVUE cluster schema.
    pub async fn get_cluster(&self, timeout: Duration) -> Result<Cluster, ClientError> {
        let response = self.get_json(CLUSTER_ENDPOINT, timeout).await?;

        serde_json::from_value(response).map_err(|error| {
            ClientError::invalid_response(format!("invalid NVUE cluster response: {error}"))
        })
    }

    /// Reads and decodes one NVUE cluster application.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the successful response does
    /// not match the NVUE cluster application schema.
    pub async fn get_cluster_app(
        &self,
        app_name: &str,
        timeout: Duration,
    ) -> Result<ClusterApp, ClientError> {
        let endpoint = app_endpoint(app_name);

        let response = self.get_json(&endpoint, timeout).await?;

        serde_json::from_value(response).map_err(|error| {
            ClientError::invalid_response(format!(
                "invalid NVUE cluster application response for '{app_name}': {error}"
            ))
        })
    }

    /// Reads one revision's configured server addresses for a cluster interface.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the successful response does
    /// not match the NVUE cluster-node server schema.
    pub async fn get_cluster_node_servers(
        &self,
        interface_type: InterfaceType,
        revision_id: &str,
        timeout: Duration,
    ) -> Result<ClusterNodeServerAddresses, ClientError> {
        let endpoint = cluster_node_server_endpoint_for_revision(interface_type, revision_id);

        let response = self.get_json(&endpoint, timeout).await?;

        let servers: ClusterNodeServerMap = serde_json::from_value(response).map_err(|error| {
            ClientError::invalid_response(format!(
                "invalid NVUE {} cluster-node server response: {error}",
                interface_type.as_str()
            ))
        })?;

        Ok(servers.into_keys().collect())
    }

    /// Stages cluster-node server addresses on an existing NVUE revision.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization, transport, or NVUE validation fails.
    async fn patch_cluster_node_servers(
        &self,
        interface_type: InterfaceType,
        revision_id: &str,
        servers: &ClusterNodeServerAddresses,
        timeout: Duration,
    ) -> Result<(), ClientError> {
        let endpoint = cluster_node_server_endpoint_for_revision(interface_type, revision_id);

        let servers: ClusterNodeServerMap = servers
            .iter()
            .copied()
            .map(|address| (address, ClusterNodeServer {}))
            .collect();

        let payload = serde_json::to_value(servers).map_err(|error| {
            ClientError::InvalidRequest(format!(
                "failed to serialize NVUE {} cluster-node servers: {error}",
                interface_type.as_str()
            ))
        })?;

        self.patch_json(&endpoint, &payload, timeout).await?;

        Ok(())
    }

    /// Stages an exact cluster-node server collection on an existing revision.
    ///
    /// NVUE collection PATCH operations merge keys. When the desired state only
    /// adds addresses, this patches just those additions. The collection is
    /// cleared by unsetting the interface only when exact reconciliation must
    /// remove stale keys. Unsetting also avoids leaving an empty interface that
    /// prevents disabling the cluster.
    /// An exact match returns `false` without issuing an NVUE write.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization, transport, or NVUE validation fails.
    pub async fn stage_cluster_node_servers(
        &self,
        interface_type: InterfaceType,
        revision_id: &str,
        current: &ClusterNodeServerAddresses,
        desired: &ClusterNodeServerAddresses,
        timeout: Duration,
    ) -> Result<bool, ClientError> {
        if current == desired {
            return Ok(false);
        }

        if current.is_subset(desired) {
            let additions: ClusterNodeServerAddresses =
                desired.difference(current).copied().collect();

            self.patch_cluster_node_servers(interface_type, revision_id, &additions, timeout)
                .await?;
        } else {
            let endpoint = cluster_node_endpoint_for_revision(interface_type, revision_id);
            let response = self.delete(&endpoint, timeout).await?;

            if !response.is_success() {
                return Err(ClientError::HttpStatus {
                    method: "DELETE",
                    path: endpoint,
                    status: response.status,
                    body: response.body,
                });
            }

            if !desired.is_empty() {
                self.patch_cluster_node_servers(interface_type, revision_id, desired, timeout)
                    .await?;
            }
        }

        Ok(true)
    }

    /// Send a POST and require a successful JSON response.
    pub async fn post_json<T: Serialize + ?Sized>(
        &self,
        path: &str,
        payload: &T,
        timeout: Duration,
    ) -> Result<Value, ClientError> {
        self.post(path, payload, timeout)
            .await?
            .into_json("POST", path)
    }

    /// Send a PATCH and require a successful JSON response.
    pub async fn patch_json(
        &self,
        path: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<Value, ClientError> {
        self.patch(path, payload, timeout)
            .await?
            .into_json("PATCH", path)
    }

    /// Send a conditional PATCH and require a successful JSON response.
    pub async fn patch_json_with_if_match(
        &self,
        path: &str,
        payload: &Value,
        if_match: &str,
        timeout: Duration,
    ) -> Result<Value, ClientError> {
        self.send(Method::PATCH, path, Some(payload), Some(if_match), timeout)
            .await?
            .into_json("PATCH", path)
    }

    /// Change the URL/TLS authority while the client still uses plain HTTPS.
    /// Runtime client-TLS changes must use the prepare-and-activate workflow.
    pub async fn configure_server_name(
        &self,
        server_name: Option<&str>,
    ) -> Result<bool, ClientError> {
        let _guard = self.reconfigure.lock().await;
        let (mut config, scheme) = {
            let state = self.state.lock().unwrap();

            if state.tls.is_some() {
                return Err(ClientError::InvalidEndpoint(
                    "changing an active client-TLS transport requires prepare_client_tls".into(),
                ));
            }

            (state.config.clone(), state.scheme)
        };

        if let Some(server_name) = server_name {
            validate_server_name(server_name)?;

            config.endpoint.host = normalize_host(server_name).to_owned();
            config.endpoint.validate()?;
        }

        {
            let state = self.state.lock().unwrap();

            if state.generation == ClientGeneration::Plain
                && normalize_host(&state.config.endpoint.host)
                    == normalize_host(&config.endpoint.host)
            {
                return Ok(false);
            }
        }

        let transport = build_plain_transport(&config, scheme)?;
        let mut state = self.state.lock().unwrap();

        state.transport = Arc::new(transport);
        state.generation = ClientGeneration::Plain;
        state.revision = state.revision.wrapping_add(1);
        state.config = config;

        Ok(true)
    }

    /// Force plain HTTPS without client TLS and without server certificate
    /// verification.
    pub async fn configure_insecure_https(&self) -> Result<bool, ClientError> {
        let _guard = self.reconfigure.lock().await;
        let (mut config, scheme) = {
            let state = self.state.lock().unwrap();

            (state.config.clone(), state.scheme)
        };

        if scheme != TransportScheme::Https {
            return Err(ClientError::InvalidEndpoint(
                "insecure NVUE switch mode requires an HTTPS endpoint".into(),
            ));
        }

        config.dangerously_accept_invalid_certs = true;

        {
            let state = self.state.lock().unwrap();

            if state.generation == ClientGeneration::Plain
                && state.tls.is_none()
                && normalize_host(&state.config.endpoint.host)
                    == normalize_host(&config.endpoint.host)
                && state.config.dangerously_accept_invalid_certs
            {
                return Ok(false);
            }
        }

        let transport = build_plain_transport(&config, scheme)?;
        let mut state = self.state.lock().unwrap();

        state.transport = Arc::new(transport);
        state.generation = ClientGeneration::Plain;
        state.revision = state.revision.wrapping_add(1);
        state.config = config;
        state.tls = None;

        Ok(true)
    }

    /// Build a fresh client-TLS candidate without changing the active transport.
    pub async fn prepare_client_tls(
        &self,
        tls: ClientTls,
        server_name: Option<&str>,
    ) -> Result<PreparedClientTls, ClientError> {
        let prepared = self
            .prepare_client_tls_candidate(tls, server_name, false, ServerVerification::Configured)
            .await?;

        Ok(prepared.expect("forced client-TLS preparation must produce a candidate"))
    }

    /// Build a client-TLS candidate that requires a valid server certificate.
    ///
    /// The active transport remains unchanged, including any unverified
    /// transport used to bootstrap certificate installation.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint or TLS material is invalid, or when
    /// the verified transport cannot be built.
    pub async fn prepare_verified_client_tls(
        &self,
        tls: ClientTls,
        server_name: Option<&str>,
    ) -> Result<PreparedClientTls, ClientError> {
        let prepared = self
            .prepare_client_tls_candidate(tls, server_name, false, ServerVerification::Required)
            .await?;

        Ok(prepared.expect("forced client-TLS preparation must produce a candidate"))
    }

    /// Build a client-TLS candidate only when material or TLS authority changed.
    pub async fn prepare_client_tls_if_changed(
        &self,
        tls: ClientTls,
        server_name: Option<&str>,
    ) -> Result<Option<PreparedClientTls>, ClientError> {
        self.prepare_client_tls_candidate(tls, server_name, true, ServerVerification::Configured)
            .await
    }

    async fn prepare_client_tls_candidate(
        &self,
        tls: ClientTls,
        server_name: Option<&str>,
        skip_if_unchanged: bool,
        server_verification: ServerVerification,
    ) -> Result<Option<PreparedClientTls>, ClientError> {
        let _guard = self.reconfigure.lock().await;
        let (based_on_revision, mut config, scheme) = {
            let state = self.state.lock().unwrap();

            (state.revision, state.config.clone(), state.scheme)
        };

        if let Some(server_name) = server_name {
            validate_server_name(server_name)?;

            config.endpoint.host = normalize_host(server_name).to_owned();
            config.endpoint.validate()?;
        }

        if matches!(server_verification, ServerVerification::Required) {
            config.dangerously_accept_invalid_certs = false;
        }

        let snapshot = load_stable_snapshot(&tls).await?;
        let server_name = effective_server_name(&config)?;
        let generation = ClientGeneration::ClientTls {
            fingerprint: snapshot.fingerprint,
            server_name: server_name.clone(),
            dangerously_accept_invalid_certs: config.dangerously_accept_invalid_certs,
        };

        if skip_if_unchanged {
            let state = self.state.lock().unwrap();

            if state.generation == generation && state.tls.as_ref() == Some(&tls) {
                return Ok(None);
            }
        }

        let transport =
            build_client_tls_transport(&config, &server_name, &snapshot, scheme).await?;

        Ok(Some(PreparedClientTls {
            owner: Arc::clone(&self.owner),
            based_on_revision,
            transport: Arc::new(transport),
            generation,
            config,
            tls,
            material_fingerprint: snapshot.fingerprint,
        }))
    }

    /// Verify a prepared client-TLS transport with a read-only JSON request,
    /// then atomically make it active.
    ///
    /// A stale candidate succeeds only when its exact TLS generation is already
    /// active. Other stale candidates are rejected. Failed or cancelled probes
    /// leave the active transport intact.
    pub async fn activate_prepared_client_tls(
        &self,
        prepared: &PreparedClientTls,
        path: &str,
        timeout: Duration,
    ) -> Result<Value, ClientError> {
        let _guard = self.reconfigure.lock().await;

        if !Arc::ptr_eq(&self.owner, &prepared.owner) {
            return Err(ClientError::ForeignPreparedTransport);
        }

        let active_transport = {
            let state = self.state.lock().unwrap();

            if state.revision != prepared.based_on_revision {
                if state.generation == prepared.generation
                    && state.tls.as_ref() == Some(&prepared.tls)
                {
                    Some(Arc::clone(&state.transport))
                } else {
                    return Err(ClientError::StalePreparedTransport);
                }
            } else {
                None
            }
        };

        let response = active_transport
            .as_ref()
            .unwrap_or(&prepared.transport)
            .send::<Value>(Method::GET, path, None, None, timeout)
            .await?
            .into_json("GET", path)?;

        if active_transport.is_some() {
            return Ok(response);
        }

        let mut state = self.state.lock().unwrap();

        state.transport = Arc::clone(&prepared.transport);
        state.generation = prepared.generation.clone();
        state.revision = state.revision.wrapping_add(1);
        state.config = prepared.config.clone();
        state.tls = Some(prepared.tls.clone());

        Ok(response)
    }

    /// Rebuild the active transport with new basic-auth credentials.
    pub async fn update_credentials(
        &self,
        credentials: ClientCredentials,
    ) -> Result<(), ClientError> {
        let _guard = self.reconfigure.lock().await;
        let mut state = self.state.lock().unwrap();

        let transport = Transport {
            client: state.transport.client.clone(),
            base_url: state.transport.base_url.clone(),
            credentials: credentials.clone(),
            send_empty_basic_auth: state.transport.send_empty_basic_auth,
            response_mode: state.transport.response_mode,
        };

        state.transport = Arc::new(transport);
        state.revision = state.revision.wrapping_add(1);
        state.config.credentials = credentials;

        Ok(())
    }

    /// Clone this client with different Basic Auth credentials.
    ///
    /// Password rotation uses this to probe candidate credentials while keeping
    /// the active client on the old credentials until the revision is confirmed.
    /// The clone preserves the current transport generation, TLS material, and
    /// reqwest transport settings, but later reconfiguration is isolated from
    /// the original client.
    pub fn clone_with_credentials(&self, credentials: ClientCredentials) -> SharedClient {
        let state = self.state.lock().unwrap();
        let mut config = state.config.clone();

        config.credentials = credentials.clone();

        let transport = Transport {
            client: state.transport.client.clone(),
            base_url: state.transport.base_url.clone(),
            credentials,
            send_empty_basic_auth: state.transport.send_empty_basic_auth,
            response_mode: state.transport.response_mode,
        };

        Arc::new(Self {
            owner: Arc::new(()),
            state: StdMutex::new(ClientState {
                transport: Arc::new(transport),
                generation: state.generation.clone(),
                revision: state.revision,
                config,
                tls: state.tls.clone(),
                scheme: state.scheme,
            }),
            reconfigure: AsyncMutex::new(()),
        })
    }

    /// Return a snapshot of the configured NVUE endpoint.
    pub fn endpoint(&self) -> ClientEndpoint {
        self.state.lock().unwrap().config.endpoint.clone()
    }

    /// Return whether the active generation presents a client certificate.
    pub fn uses_client_tls(&self) -> bool {
        matches!(
            self.state.lock().unwrap().generation,
            ClientGeneration::ClientTls { .. }
        )
    }

    /// Return whether the active client uses the supplied basic-auth credentials.
    pub fn credentials_match(&self, username: &str, password: &str) -> bool {
        let state = self.state.lock().unwrap();

        state.config.credentials.username == username
            && state.config.credentials.password.expose_secret() == password
    }

    async fn send<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        payload: Option<&T>,
        if_match: Option<&str>,
        timeout: Duration,
    ) -> Result<NvueResponse, ClientError> {
        self.transport_snapshot()
            .send(method, path, payload, if_match, timeout)
            .await
    }

    fn transport_snapshot(&self) -> Arc<Transport> {
        Arc::clone(&self.state.lock().unwrap().transport)
    }

    #[cfg(test)]
    fn transport_snapshot_for_test(&self) -> Arc<Transport> {
        self.transport_snapshot()
    }
}

async fn build_transport(
    config: &ClientConfig,
    tls: Option<&ClientTls>,
    scheme: TransportScheme,
) -> Result<(Transport, ClientGeneration), ClientError> {
    if let Some(tls) = tls {
        let snapshot = load_stable_snapshot(tls).await?;
        let server_name = effective_server_name(config)?;

        let generation = ClientGeneration::ClientTls {
            fingerprint: snapshot.fingerprint,
            server_name: server_name.clone(),
            dangerously_accept_invalid_certs: config.dangerously_accept_invalid_certs,
        };

        let transport = build_client_tls_transport(config, &server_name, &snapshot, scheme).await?;

        return Ok((transport, generation));
    }

    let transport = build_plain_transport(config, scheme)?;

    Ok((transport, ClientGeneration::Plain))
}

fn build_plain_transport(
    config: &ClientConfig,
    scheme: TransportScheme,
) -> Result<Transport, ClientError> {
    let client = if normalize_host(&config.endpoint.host)
        != normalize_host(&config.endpoint.connect_host)
    {
        // A per-endpoint DNS override is baked into the client, so it cannot be shared.
        let socket_addr = parse_socket_addr(&config.endpoint.connect_host, config.endpoint.port)?;
        disable_proxies(reqwest::Client::builder())
            .danger_accept_invalid_certs(config.dangerously_accept_invalid_certs)
            .connect_timeout(CONNECT_TIMEOUT)
            .pool_max_idle_per_host(PLAIN_POOL_MAX_IDLE_PER_HOST)
            .resolve(normalize_host(&config.endpoint.host), socket_addr)
            .build()
            .map_err(|source| ClientError::Request {
                operation: "failed to create NVUE HTTP client".into(),
                source,
            })?
    } else {
        // No per-endpoint resolve: reuse a shared client (a cheap Arc clone) instead of
        // rebuilding the reqwest/TLS stack for every switch. reqwest keys its connection
        // pool by host, so one client safely serves every switch.
        shared_plain_client(config.dangerously_accept_invalid_certs)?
    };

    Ok(Transport {
        client,
        base_url: config.endpoint.base_url(scheme, &config.endpoint.host),
        credentials: config.credentials.clone(),
        send_empty_basic_auth: false,
        response_mode: ResponseMode::Bounded,
    })
}

/// Return a process-wide plain (non-client-TLS) reqwest client, built once per
/// certificate-verification mode and cloned thereafter. Building a reqwest client
/// initializes the TLS stack, which is expensive to repeat for every switch.
fn shared_plain_client(accept_invalid_certs: bool) -> Result<reqwest::Client, ClientError> {
    static ACCEPTING: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    static VERIFYING: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

    let cell = if accept_invalid_certs {
        &ACCEPTING
    } else {
        &VERIFYING
    };

    if let Some(client) = cell.get() {
        return Ok(client.clone());
    }

    let client = disable_proxies(reqwest::Client::builder())
        .danger_accept_invalid_certs(accept_invalid_certs)
        .connect_timeout(CONNECT_TIMEOUT)
        .pool_max_idle_per_host(PLAIN_POOL_MAX_IDLE_PER_HOST)
        .build()
        .map_err(|source| ClientError::Request {
            operation: "failed to create NVUE HTTP client".into(),
            source,
        })?;

    Ok(cell.get_or_init(|| client).clone())
}

async fn build_client_tls_transport(
    config: &ClientConfig,
    server_name: &str,
    snapshot: &TlsSnapshot,
    scheme: TransportScheme,
) -> Result<Transport, ClientError> {
    if scheme != TransportScheme::Https {
        return Err(ClientError::InvalidEndpoint(
            "NVUE client TLS requires an HTTPS endpoint".into(),
        ));
    }

    let identity = reqwest::Identity::from_pem(&snapshot.identity_pem)
        .map_err(|error| ClientError::InvalidTlsMaterial(error.to_string()))?;

    let ca_cert = reqwest::Certificate::from_pem(&snapshot.ca_cert_pem)
        .map_err(|error| ClientError::InvalidTlsMaterial(error.to_string()))?;

    let socket_addr =
        resolve_socket_addr(&config.endpoint.connect_host, config.endpoint.port).await?;

    let client = disable_proxies(reqwest::Client::builder())
        .danger_accept_invalid_certs(config.dangerously_accept_invalid_certs)
        .connect_timeout(CONNECT_TIMEOUT)
        .identity(identity)
        .add_root_certificate(ca_cert)
        .resolve(server_name, socket_addr)
        .build()
        .map_err(|source| ClientError::Request {
            operation: "failed to create NVUE client-TLS HTTP client".into(),
            source,
        })?;

    Ok(Transport {
        client,
        base_url: config.endpoint.base_url(scheme, server_name),
        credentials: config.credentials.clone(),
        send_empty_basic_auth: false,
        response_mode: ResponseMode::Bounded,
    })
}

/// Keep switch control-plane traffic on the validated management path; a proxy
/// would bypass the DNS-to-management-IP override configured with `resolve`.
fn disable_proxies(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    builder.no_proxy()
}

fn effective_server_name(config: &ClientConfig) -> Result<String, ClientError> {
    validate_server_name(&config.endpoint.host)?;
    Ok(normalize_host(&config.endpoint.host).to_owned())
}

fn parse_socket_addr(host: &str, port: u16) -> Result<SocketAddr, ClientError> {
    let host = normalize_host(host);

    let ip = host.parse::<IpAddr>().map_err(|error| {
        ClientError::InvalidEndpoint(format!(
            "NVUE connection host {host} must be an IP address when it differs from the URL host: {error}"
        ))
    })?;

    Ok(SocketAddr::new(ip, port))
}

async fn resolve_socket_addr(host: &str, port: u16) -> Result<SocketAddr, ClientError> {
    let host = normalize_host(host);

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }

    let mut addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|source| ClientError::ResolveHost {
            host: host.to_owned(),
            port,
            source,
        })?;

    addresses
        .next()
        .ok_or_else(|| ClientError::NoResolvedAddress {
            host: host.to_owned(),
            port,
        })
}

fn normalize_host(host: &str) -> &str {
    host.trim()
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or_else(|| host.trim())
}

fn validate_server_name(name: &str) -> Result<(), ClientError> {
    let name = normalize_host(name);

    ServerName::try_from(name.to_owned())
        .map(|_| ())
        .map_err(|error| {
            ClientError::InvalidEndpoint(format!("invalid NVUE URL/TLS host: {error}"))
        })
}

fn format_url_host(host: &str) -> String {
    let host = normalize_host(host);

    if host.parse::<Ipv6Addr>().is_ok() {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

fn method_name(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PATCH => "PATCH",
        Method::DELETE => "DELETE",
        _ => "HTTP",
    }
}

async fn send_request(
    request: RequestBuilder,
    method: &'static str,
    path: &str,
) -> Result<reqwest::Response, ClientError> {
    request.send().await.map_err(|source| ClientError::Request {
        operation: format!("NVUE {method} {path}"),
        source,
    })
}

async fn read_response(
    mut response: reqwest::Response,
    method: &'static str,
    path: &str,
    mode: ResponseMode,
) -> Result<NvueResponse, ClientError> {
    let status = response.status().as_u16();

    if matches!(mode, ResponseMode::ImportedLegacy) {
        let bytes = if method == "GET" && status != 200 {
            Vec::new()
        } else {
            response.bytes().await.unwrap_or_default().to_vec()
        };

        return Ok(response_from_bytes(status, bytes));
    }

    if response
        .content_length()
        .is_some_and(|length| length > DEFAULT_MAX_RESPONSE_BYTES as u64)
    {
        return Err(ClientError::ResponseTooLarge {
            method,
            path: path.to_owned(),
            limit: DEFAULT_MAX_RESPONSE_BYTES,
        });
    }

    let mut bytes = Vec::new();

    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| ClientError::Request {
            operation: format!("read NVUE {method} {path} response"),
            source,
        })?
    {
        if bytes.len().saturating_add(chunk.len()) > DEFAULT_MAX_RESPONSE_BYTES {
            return Err(ClientError::ResponseTooLarge {
                method,
                path: path.to_owned(),
                limit: DEFAULT_MAX_RESPONSE_BYTES,
            });
        }

        bytes.extend_from_slice(&chunk);
    }

    Ok(response_from_bytes(status, bytes))
}

fn response_from_bytes(status: u16, bytes: Vec<u8>) -> NvueResponse {
    let body = String::from_utf8_lossy(&bytes).into_owned();

    let (value, json_error) = if bytes.is_empty() || status == 204 {
        (Value::Object(serde_json::Map::new()), None)
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(value) => (value, None),
            Err(error) => (Value::Null, Some(error.to_string())),
        }
    };

    NvueResponse {
        status,
        value,
        body,
        json_error,
    }
}

#[cfg(test)]
mod tests {
    use std::{io, path::Path};

    use rcgen::generate_simple_self_signed;
    use reqwest::header::{HeaderMap, HeaderValue};
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn test_config(host: &str, port: u16) -> ClientConfig {
        ClientConfig {
            endpoint: ClientEndpoint {
                host: host.to_owned(),
                connect_host: host.to_owned(),
                port,
                scheme: TransportScheme::Https,
            },
            credentials: ClientCredentials::new("admin", "pass"),
            dangerously_accept_invalid_certs: true,
        }
    }

    fn test_http_config(host: &str, port: u16) -> ClientConfig {
        let mut config = test_config(host, port);

        config.endpoint = ClientEndpoint::http(host, port);

        config
    }

    async fn write_test_tls_material(dir: &Path) -> ClientTls {
        let certificate = generate_simple_self_signed(vec!["switch.example.com".into()]).unwrap();
        let ca_path = dir.join("ca.pem");
        let cert_path = dir.join("client.pem");
        let key_path = dir.join("client.key");

        tokio::fs::write(&ca_path, certificate.cert.pem())
            .await
            .unwrap();

        tokio::fs::write(&cert_path, certificate.cert.pem())
            .await
            .unwrap();

        tokio::fs::write(&key_path, certificate.key_pair.serialize_pem())
            .await
            .unwrap();

        ClientTls::new(ca_path, cert_path, key_path)
    }

    async fn test_tls_client() -> (tempfile::TempDir, MockServer, ClientTls, SharedClient) {
        let directory = tempfile::tempdir().unwrap();
        let server = MockServer::start().await;
        let tls = write_test_tls_material(directory.path()).await;

        let client = Client::connect(
            test_config("127.0.0.1", server.address().port()),
            Some(tls.clone()),
        )
        .await
        .unwrap();

        (directory, server, tls, client)
    }

    #[tokio::test]
    async fn raw_post_preserves_status_json_and_body() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/action"))
            .and(body_json(serde_json::json!({"run": true})))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_body_raw(r#"{"action-id":"42"}"#, "application/json"),
            )
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config).unwrap();

        let response = client
            .post(
                "/nvue_v1/action",
                &serde_json::json!({"run": true}),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(response.status, 202);
        assert_eq!(response.value["action-id"], "42");
        assert_eq!(response.body, r#"{"action-id":"42"}"#);
    }

    #[tokio::test]
    async fn raw_patch_preserves_error_status_and_body() {
        let server = MockServer::start().await;

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/system/aaa/user/admin"))
            .and(body_json(serde_json::json!({"password": "encoded"})))
            .respond_with(ResponseTemplate::new(400).set_body_string("password policy failed"))
            .expect(1)
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config).unwrap();
        let response = client
            .patch(
                "/nvue_v1/system/aaa/user/admin",
                &serde_json::json!({"password": "encoded"}),
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(response.status, 400);
        assert_eq!(response.body, "password policy failed");
    }

    #[tokio::test]
    async fn cluster_app_get_decodes_typed_nvue_response() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/apps/nmx-controller"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "not ok",
                "reason": "GFM: UNCONFIGURED",
                "additional-info": "CONTROL_PLANE_STATE_UNCONFIGURED",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config).unwrap();

        let app = client
            .get_cluster_app("nmx-controller", Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(app.status.as_deref(), Some("not ok"));
        assert_eq!(app.reason.as_deref(), Some("GFM: UNCONFIGURED"));

        assert_eq!(
            app.control_plane_state(),
            Some(crate::cluster::NmxControlPlaneState::Unconfigured)
        );
    }

    #[tokio::test]
    async fn cluster_get_decodes_typed_nvue_response() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "disabled",
                "nmxc-conn": "down",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config).unwrap();

        let cluster = client.get_cluster(Duration::from_secs(1)).await.unwrap();

        assert!(cluster.is_disabled());
    }

    #[tokio::test]
    async fn cluster_node_server_methods_use_typed_revision_scoped_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        let address = "192.0.2.101".parse::<IpAddr>()?;
        let servers = ClusterNodeServerAddresses::from([address]);

        let previous = ClusterNodeServerAddresses::from(["192.0.2.100".parse::<IpAddr>()?]);

        let expanded =
            ClusterNodeServerAddresses::from([address, "192.0.2.102".parse::<IpAddr>()?]);

        let servers_payload = serde_json::json!({"192.0.2.101": {}});
        let addition_payload = serde_json::json!({"192.0.2.102": {}});

        Mock::given(method("GET"))
            .and(path("/nvue_v1/cluster/node/primary/server"))
            .and(query_param("rev", "applied"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&servers_payload))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/nvue_v1/cluster/node/primary"))
            .and(query_param("rev", "42"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/nvue_v1/cluster/node/primary"))
            .and(query_param("rev", "44"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/cluster/node/primary/server"))
            .and(query_param("rev", "42"))
            .and(body_json(&servers_payload))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/cluster/node/primary/server"))
            .and(query_param("rev", "43"))
            .and(body_json(&addition_payload))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config)?;

        let observed = client
            .get_cluster_node_servers(InterfaceType::Primary, "applied", Duration::from_secs(1))
            .await?;

        client
            .stage_cluster_node_servers(
                InterfaceType::Primary,
                "42",
                &previous,
                &servers,
                Duration::from_secs(1),
            )
            .await?;

        client
            .stage_cluster_node_servers(
                InterfaceType::Primary,
                "43",
                &servers,
                &expanded,
                Duration::from_secs(1),
            )
            .await?;

        client
            .stage_cluster_node_servers(
                InterfaceType::Primary,
                "44",
                &servers,
                &ClusterNodeServerAddresses::default(),
                Duration::from_secs(1),
            )
            .await?;

        let staged = client
            .stage_cluster_node_servers(
                InterfaceType::Primary,
                "45",
                &servers,
                &servers,
                Duration::from_secs(1),
            )
            .await?;

        assert_eq!(observed, servers);
        assert!(!staged);

        Ok(())
    }

    #[tokio::test]
    async fn imported_reqwest_transport_is_used() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform"))
            .and(header("x-imported-transport", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let mut headers = HeaderMap::new();
        headers.insert("x-imported-transport", HeaderValue::from_static("true"));

        let transport = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::from_reqwest(config, transport).unwrap();

        let response = client
            .get("/nvue_v1/platform", Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(response.status, 200);
    }

    #[test]
    fn imported_reqwest_transport_rejects_split_endpoint() {
        let mut config = test_config("192.0.2.44", 443);
        config.endpoint = ClientEndpoint::https("switch.example.com", "192.0.2.44", 443);

        assert!(matches!(
            Client::from_reqwest(config, reqwest::Client::new()),
            Err(ClientError::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn probe_treats_any_http_response_as_reachable() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/firmware"))
            .respond_with(ResponseTemplate::new(503).set_body_string("restarting"))
            .expect(1)
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config).unwrap();

        let status = client
            .probe("/nvue_v1/platform/firmware", Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(status, 503);
    }

    #[tokio::test]
    async fn json_helper_rejects_http_error_without_replay() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/action"))
            .respond_with(ResponseTemplate::new(503).set_body_string("not ready"))
            .expect(1)
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config).unwrap();

        let error = client
            .post_json(
                "/nvue_v1/action",
                &serde_json::json!({}),
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();

        let message = error.to_string();

        assert!(matches!(error, ClientError::HttpStatus { status: 503, .. }));
        assert_eq!(message, "HTTP POST /nvue_v1/action returned 503");
    }

    #[test]
    fn endpoint_keeps_management_ip_separate_from_valid_tls_authority() {
        for (ip, host_name, expected_host) in [
            (
                "192.0.2.44",
                Some("switch.example.com"),
                "switch.example.com",
            ),
            ("192.0.2.44", None, "192.0.2.44"),
            ("fd00::44", None, "fd00::44"),
        ] {
            let endpoint = ClientEndpoint::new(ip, 443, host_name).unwrap();

            assert_eq!(endpoint.host, expected_host);
            assert_eq!(endpoint.connect_host, ip);
        }

        for (ip, host_name, port) in [
            ("not-an-ip", Some("switch.example.com"), 443),
            ("192.0.2.44", Some("user@evil.example"), 443),
            ("192.0.2.44", Some("switch.example.com/path"), 443),
            ("192.0.2.44", Some("198.51.100.7"), 443),
            ("192.0.2.44", Some("switch.example.com"), 0),
        ] {
            assert!(ClientEndpoint::new(ip, port, host_name).is_err());
        }

        assert!(ClientEndpoint::from_base_url("https://switch.example.com").is_ok());
    }

    #[tokio::test]
    async fn plain_https_can_apply_dns_authority_without_changing_connect_ip() {
        let client = Client::new(test_config("192.0.2.44", 443)).unwrap();

        assert!(
            client
                .configure_server_name(Some("switch.example.com"))
                .await
                .unwrap()
        );

        let endpoint = client.endpoint();

        assert_eq!(endpoint.host, "switch.example.com");
        assert_eq!(endpoint.connect_host, "192.0.2.44");
    }

    #[tokio::test]
    async fn insecure_https_replaces_active_client_tls_without_changing_endpoint() {
        let (_directory, _server, _tls, client) = test_tls_client().await;

        assert!(client.uses_client_tls());

        let changed = client.configure_insecure_https().await.unwrap();

        let endpoint = client.endpoint();

        assert!(changed);
        assert!(!client.uses_client_tls());
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.connect_host, "127.0.0.1");

        let unchanged = client.configure_insecure_https().await.unwrap();

        assert!(!unchanged);
    }

    #[tokio::test]
    async fn verified_tls_candidate_restores_server_certificate_validation() {
        let (_directory, _server, tls, client) = test_tls_client().await;

        client.configure_insecure_https().await.unwrap();

        let prepared = client.prepare_verified_client_tls(tls, None).await.unwrap();

        let active_accepts_invalid_certs = client
            .state
            .lock()
            .unwrap()
            .config
            .dangerously_accept_invalid_certs;

        assert!(active_accepts_invalid_certs);
        assert!(!prepared.config.dangerously_accept_invalid_certs);
        assert!(!client.uses_client_tls());
    }

    #[tokio::test]
    async fn cloned_client_transport_changes_do_not_affect_original() {
        let (_directory, _server, _tls, client) = test_tls_client().await;

        let cloned =
            client.clone_with_credentials(ClientCredentials::new("admin", "replacement-pass"));

        assert!(client.uses_client_tls());
        assert!(cloned.uses_client_tls());

        cloned.configure_insecure_https().await.unwrap();

        assert!(client.uses_client_tls());
        assert!(!cloned.uses_client_tls());
    }

    #[test]
    fn http_status_is_not_a_server_certificate_validation_error() {
        let error = ClientError::HttpStatus {
            method: "GET",
            path: "/nvue_v1/system".into(),
            status: 401,
            body: String::new(),
        };

        assert!(!error.is_server_certificate_validation_error());
    }

    #[tokio::test]
    async fn wrapped_rustls_invalid_certificate_is_server_certificate_validation_error() {
        let certificate_error =
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding);

        let io_error = io::Error::other(certificate_error);

        let body = reqwest::Body::wrap_stream(futures::stream::once(async {
            Err::<Vec<u8>, _>(io_error)
        }));

        let server = MockServer::start().await;
        let request = reqwest::Client::new().post(server.uri()).body(body);

        let error = send_request(request, "POST", "/nvue_v1/system")
            .await
            .unwrap_err();

        assert!(matches!(&error, ClientError::Request { .. }));
        assert!(error.is_server_certificate_validation_error());
    }

    #[tokio::test]
    async fn internal_transport_builder_disables_proxies() {
        let target = MockServer::start().await;
        let proxy = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/direct"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&target)
            .await;

        let client = disable_proxies(
            reqwest::Client::builder().proxy(reqwest::Proxy::all(proxy.uri()).unwrap()),
        )
        .build()
        .unwrap();

        assert!(
            client
                .get(format!("{}/direct", target.uri()))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }

    #[tokio::test]
    async fn preparing_rotated_tls_does_not_replace_active_transport() {
        let (directory, _server, tls, client) = test_tls_client().await;
        let before = client.transport_snapshot_for_test();

        assert!(
            client
                .prepare_client_tls_if_changed(tls.clone(), None)
                .await
                .unwrap()
                .is_none()
        );

        write_test_tls_material(directory.path()).await;

        let _prepared = client.prepare_client_tls(tls, None).await.unwrap();

        assert!(Arc::ptr_eq(&before, &client.transport_snapshot_for_test()));
    }

    #[tokio::test]
    async fn failed_stale_or_foreign_prepared_tls_never_replaces_active_transport() {
        let (directory, _server, tls, client) = test_tls_client().await;
        let original = client.transport_snapshot_for_test();
        let failed_candidate = client
            .prepare_client_tls(tls.clone(), Some("switch.example.com"))
            .await
            .unwrap();

        assert!(
            client
                .activate_prepared_client_tls(
                    &failed_candidate,
                    "/nvue_v1/system",
                    Duration::from_millis(10),
                )
                .await
                .is_err()
        );

        assert!(Arc::ptr_eq(
            &original,
            &client.transport_snapshot_for_test()
        ));

        let foreign = Client::new(test_config("127.0.0.1", 443)).unwrap();
        let error = foreign
            .activate_prepared_client_tls(
                &failed_candidate,
                "/nvue_v1/system",
                Duration::from_millis(10),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, ClientError::ForeignPreparedTransport));

        write_test_tls_material(directory.path()).await;
        let stale_candidate = client.prepare_client_tls(tls, None).await.unwrap();

        client
            .update_credentials(ClientCredentials::new("admin", "new-pass"))
            .await
            .unwrap();

        assert!(client.credentials_match("admin", "new-pass"));
        let current = client.transport_snapshot_for_test();
        let error = client
            .activate_prepared_client_tls(
                &stale_candidate,
                "/nvue_v1/system",
                Duration::from_millis(10),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, ClientError::StalePreparedTransport));
        assert!(Arc::ptr_eq(&current, &client.transport_snapshot_for_test()));
    }

    #[tokio::test]
    async fn clone_with_credentials_uses_replacement_basic_auth() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system"))
            .and(header("Authorization", "Basic YWRtaW46bmV3LXBhc3M="))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config).unwrap();
        let clone = client.clone_with_credentials(ClientCredentials::new("admin", "new-pass"));
        let original_transport = client.transport_snapshot_for_test();
        let clone_transport = clone.transport_snapshot_for_test();

        assert!(clone.credentials_match("admin", "new-pass"));
        assert_eq!(original_transport.base_url, clone_transport.base_url);

        clone
            .get_json("/nvue_v1/system", Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn activation_commits_transport_and_accepts_matching_stale_candidate() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(2)
            .mount(&server)
            .await;

        let config = test_http_config("127.0.0.1", server.address().port());
        let client = Client::new(config.clone()).unwrap();
        let fingerprint = [7; 32];

        let prepared = PreparedClientTls {
            owner: Arc::clone(&client.owner),
            based_on_revision: 0,
            transport: Arc::new(build_plain_transport(&config, TransportScheme::Http).unwrap()),
            generation: ClientGeneration::ClientTls {
                fingerprint,
                server_name: "127.0.0.1".into(),
                dangerously_accept_invalid_certs: true,
            },
            config,
            tls: ClientTls::new("ca.pem".into(), "client.pem".into(), "client.key".into()),
            material_fingerprint: fingerprint,
        };

        client
            .activate_prepared_client_tls(&prepared, "/nvue_v1/system", Duration::from_secs(1))
            .await
            .unwrap();
        client
            .activate_prepared_client_tls(&prepared, "/nvue_v1/system", Duration::from_secs(1))
            .await
            .unwrap();

        assert!(client.uses_client_tls());
    }
}
