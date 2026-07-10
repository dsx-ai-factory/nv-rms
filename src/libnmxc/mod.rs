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

mod nmxc_api;
mod response;
mod switch_tls_store;
#[cfg(test)]
pub(crate) mod test_support;

use std::path::PathBuf;
use std::time::Duration;

use http::Uri;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

use crate::libnmxc::nmxc_api::NmxcApi;

pub use switch_tls_store::{
    TlsMaterialStore, normalize_domain, normalize_tls_server_name, validate_domain,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default `gateway_id` sent on NMX-C gRPC requests from RMS.
pub const DEFAULT_NMX_C_GATEWAY_ID: &str = "rack-manager-grpc-client";
pub const NMX_C_GATEWAY_ID_ENV: &str = "RMS_NMX_GATEWAY_ID";
pub const NMX_C_FM_CONFIG_FILE: &str = "fm_config";
pub const NMX_C_TOPOLOGY_KEY: &str = "MNNVL_TOPOLOGY";

/// Generated protobuf types from `switch_client.proto`.
pub mod nmxc_model {
    #![allow(clippy::all, unused_qualifications, non_snake_case)]
    pub use crate::api::grpc::proto::switch_client::*;
}

pub use response::find_static_config_value;

#[derive(thiserror::Error, Debug)]
pub enum NmxcError {
    #[error("Invalid endpoint URL: {0}")]
    InvalidEndpoint(String),

    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status: {0}")]
    Status(#[from] tonic::Status),

    #[error("NMX-C {operation} response missing server_header")]
    MissingServerHeader { operation: &'static str },

    #[error("NMX-C {operation} returned application level status code {return_code}")]
    NmxReturnCode {
        return_code: i32,
        operation: &'static str,
    },
}

impl NmxcError {
    /// Creates an error for an invalid or missing response from the server.
    pub fn invalid_response(msg: impl Into<String>) -> Self {
        Self::Status(tonic::Status::unknown(msg.into()))
    }

    /// NMX-C `server_header.return_code` when this error is [`NmxcError::NmxReturnCode`].
    pub fn nmx_return_code(&self) -> Option<i32> {
        match self {
            Self::NmxReturnCode { return_code, .. } => Some(*return_code),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// Base URI for the NMX-C gRPC service, for example `https://10.0.0.2:9370`.
    pub uri: Uri,
}

impl Endpoint {
    pub fn new(url: impl AsRef<str>) -> Result<Self, NmxcError> {
        let url = url.as_ref();
        let uri = url
            .parse::<Uri>()
            .map_err(|e| NmxcError::InvalidEndpoint(format!("{url}: {e}")))?;
        Ok(Self { uri })
    }
}

/// Optional client mTLS paths for HTTPS connections to NMX-C.
///
/// When both `client_cert_path` and `client_key_path` are set, the client presents a certificate
/// for mutual TLS. `ca_cert_path` adds an extra CA bundle for verifying the server.
///
/// `authority` sets the TLS server name. If unset, the host portion of the gRPC endpoint URL is
/// used.
///
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NmxcTlsConfig {
    pub ca_cert_path: Option<PathBuf>,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
    pub authority: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NmxcClientPoolBuilder {
    pub timeout: Duration,
}

impl Default for NmxcClientPoolBuilder {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl NmxcClientPoolBuilder {
    pub fn build(self) -> Result<NmxcClientPool, NmxcError> {
        Ok(NmxcClientPool {
            timeout: self.timeout,
        })
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Clone, Debug)]
pub struct NmxcClientPool {
    timeout: Duration,
}

impl NmxcClientPool {
    pub fn builder() -> NmxcClientPoolBuilder {
        NmxcClientPoolBuilder::default()
    }

    pub async fn create_client(
        &self,
        endpoint: Endpoint,
        tls: Option<&NmxcTlsConfig>,
    ) -> Result<Box<dyn Nmxc>, NmxcError> {
        let channel = self.create_channel(endpoint, tls).await?;
        Ok(Box::new(NmxcApi::new(channel)))
    }

    pub(crate) async fn create_channel(
        &self,
        endpoint: Endpoint,
        tls: Option<&NmxcTlsConfig>,
    ) -> Result<Channel, NmxcError> {
        self.connect(&endpoint, tls).await
    }

    async fn build_https_tls_config(
        &self,
        uri: &Uri,
        tls: &NmxcTlsConfig,
    ) -> Result<ClientTlsConfig, NmxcError> {
        let mut config = ClientTlsConfig::new();

        if let Some(ref path) = tls.ca_cert_path {
            let pem = tokio::fs::read(path).await.map_err(|e| {
                NmxcError::InvalidEndpoint(format!("read gRPC TLS CA cert {}: {e}", path.display()))
            })?;
            config = config.ca_certificate(Certificate::from_pem(pem));
        }

        match (&tls.client_cert_path, &tls.client_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let cert = tokio::fs::read(cert_path).await.map_err(|e| {
                    NmxcError::InvalidEndpoint(format!(
                        "read gRPC TLS client cert {}: {e}",
                        cert_path.display()
                    ))
                })?;
                let key = tokio::fs::read(key_path).await.map_err(|e| {
                    NmxcError::InvalidEndpoint(format!(
                        "read gRPC TLS client key {}: {e}",
                        key_path.display()
                    ))
                })?;
                config = config.identity(Identity::from_pem(cert, key));
            }
            (None, None) => {}
            _ => {
                return Err(NmxcError::InvalidEndpoint(
                    "gRPC TLS client cert path and key path must both be set for mTLS".to_owned(),
                ));
            }
        }

        let domain = tls
            .authority
            .clone()
            .or_else(|| uri.host().map(str::to_owned))
            .filter(|s| !s.is_empty());
        if let Some(domain) = domain {
            config = config.domain_name(domain);
        }

        Ok(config)
    }

    async fn connect(
        &self,
        endpoint: &Endpoint,
        tls: Option<&NmxcTlsConfig>,
    ) -> Result<Channel, NmxcError> {
        let uri = &endpoint.uri;
        let use_tls = uri
            .scheme_str()
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https"));

        let tls_authority = tls.and_then(|config| config.authority.as_deref());

        tracing::info!(
            uri = %uri,
            use_tls,
            tls_authority = tls_authority.unwrap_or("(endpoint host)"),
            "connecting to gRPC service"
        );

        let channel = if use_tls {
            let endpoint_builder = tonic::transport::Endpoint::from_shared(uri.to_string())
                .map_err(|e| NmxcError::InvalidEndpoint(e.to_string()))?
                .connect_timeout(self.timeout);

            let tls_config = match tls {
                Some(tls) => self.build_https_tls_config(uri, tls).await?,
                None => ClientTlsConfig::new(),
            };

            endpoint_builder
                .tls_config(tls_config)
                .map_err(|e| NmxcError::InvalidEndpoint(e.to_string()))?
                .connect()
                .await
                .map_err(|e| {
                    tracing::warn!(
                        uri = %uri,
                        tls_authority = tls_authority.unwrap_or("(endpoint host)"),
                        error = %e,
                        "gRPC transport connect failed"
                    );

                    e
                })?
        } else {
            tonic::transport::Channel::from_shared(uri.to_string())
                .map_err(|e| NmxcError::InvalidEndpoint(e.to_string()))?
                .connect_timeout(self.timeout)
                .connect()
                .await
                .map_err(|e| {
                    tracing::warn!(uri = %uri, error = %e, "gRPC transport connect failed");
                    e
                })?
        };

        tracing::info!(uri = %uri, "connected to gRPC service");
        Ok(channel)
    }
}

/// Abstraction over [`NmxcClientPool`] and test doubles.
#[async_trait::async_trait]
pub trait NmxcPool: Send + Sync + 'static {
    async fn create_client(
        &self,
        endpoint: Endpoint,
        tls: Option<&NmxcTlsConfig>,
    ) -> Result<Box<dyn Nmxc>, NmxcError>;
}

#[async_trait::async_trait]
impl NmxcPool for NmxcClientPool {
    async fn create_client(
        &self,
        endpoint: Endpoint,
        tls: Option<&NmxcTlsConfig>,
    ) -> Result<Box<dyn Nmxc>, NmxcError> {
        NmxcClientPool::create_client(self, endpoint, tls).await
    }
}

#[async_trait::async_trait]
pub trait Nmxc: Send + Sync + 'static {
    /// Perform a Hello handshake with the NMX-C controller.
    async fn hello(&mut self, gateway_id: &str) -> Result<nmxc_model::NmxHelloResponse, NmxcError>;

    async fn get_static_config(
        &mut self,
        config_file_name: &str,
        key: &str,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxGetStaticConfigResponse, NmxcError>;

    async fn set_static_config(
        &mut self,
        config_file_name: &str,
        key: &str,
        value: &str,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxReturnCode, NmxcError>;
}

pub fn gateway_id_from_env() -> String {
    std::env::var(NMX_C_GATEWAY_ID_ENV)
        .ok()
        .filter(|gateway_id| !gateway_id.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_NMX_C_GATEWAY_ID.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_new_accepts_valid_uri() {
        let endpoint = Endpoint::new("https://10.0.0.2:9370").unwrap();

        assert_eq!(endpoint.uri.scheme_str(), Some("https"));
        assert_eq!(endpoint.uri.host(), Some("10.0.0.2"));
    }

    #[test]
    fn endpoint_new_rejects_invalid_uri() {
        let err = Endpoint::new("http://[::1").unwrap_err();

        assert!(matches!(err, NmxcError::InvalidEndpoint(_)));
    }

    #[test]
    fn nmx_return_code_returns_code_for_return_code_errors() {
        let err = NmxcError::NmxReturnCode {
            return_code: 3,
            operation: "SetStaticConfig",
        };

        assert_eq!(err.nmx_return_code(), Some(3));
    }
}
