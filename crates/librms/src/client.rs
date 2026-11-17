/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-Apache-2.0
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use eyre::Result;
use hyper::body::Incoming;
use hyper_timeout::TimeoutConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ConfigBuilder, DigitallySignedStruct, SignatureScheme, WantsVerifier};
use tonic::body::Body;
use tonic::transport::Uri;
use tower::ServiceExt;
use tower::util::BoxCloneService;
use tryhard::backoff_strategies::FixedBackoff;
use tryhard::{NoOnRetry, RetryFutureConfig};

pub use crate::client_config::RmsClientConfig;
use crate::protos::rack_manager::rack_manager_client::RackManagerClient;
use crate::protos::rack_manager_v2::rack_manager_v2_client::RackManagerV2Client;
use crate::{ConfigurationError, RmsTlsClientError};

/// Cloneable RMS HTTP transport, exposed for caller-supplied layers.
pub type TransportService = BoxCloneService<
    hyper::Request<Body>,
    hyper::Response<Incoming>,
    hyper_util::client::legacy::Error,
>;

/// Caller-owned transport layer, reapplied whenever a connection is rebuilt.
/// Implementations must preserve the HTTP/2 request/response and error contract.
pub trait RmsTransportLayer: Send + Sync + std::fmt::Debug {
    /// Wraps the shared transport before either V1 or V2 tonic client is constructed.
    fn layer(&self, transport: TransportService) -> TransportService;
}

pub type RackManagerClientT = RackManagerClient<TransportService>;

/// Transport-backed client type exposed for RackManagerV2 connection providers.
pub type RackManagerV2ClientT = RackManagerV2Client<TransportService>;

pub type RmsTlsClientResult<T> = std::result::Result<T, RmsTlsClientError>;
pub type RmsHttpsClientResult<T> = std::result::Result<T, RmsTlsClientError>;

#[derive(Debug)]
pub struct DummyTlsVerifier {
    print_warning: bool,
}

impl Default for DummyTlsVerifier {
    fn default() -> Self {
        Self::new()
    }
}

impl DummyTlsVerifier {
    #[cfg(not(test))]
    pub fn new() -> Self {
        Self {
            // Warnings are suppressed if this is running in a unit-test
            print_warning: std::env::var_os("CARGO_MANIFEST_DIR").is_none(),
        }
    }

    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            // Warnings are suppressed if this is running in a unit-test
            print_warning: false,
        }
    }
}

pub const DEFAULT_DOMAIN: &str = "localhost.localdomain";

impl ServerCertVerifier for DummyTlsVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.print_warning {
            eprintln!(
                "IGNORING SERVER CERT, Please ensure that I am removed to actually validate TLS."
            );
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        if self.print_warning {
            eprintln!(
                "IGNORING SERVER CERT, Please ensure that I am removed to actually validate TLS."
            );
        }
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        if self.print_warning {
            eprintln!(
                "IGNORING SERVER CERT, Please ensure that I am removed to actually validate TLS."
            );
        }
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

// RetryConfig is intended to be a generic
// set of parameters used for defining retries.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub retries: u32,
    pub interval: Duration,
}

impl Default for RetryConfig {
    // default returns the default retry configuration,
    // which is 10 second intervals up to 60 times.
    fn default() -> Self {
        Self {
            retries: 60,
            interval: Duration::from_secs(10),
        }
    }
}

// RmsApiConfig holds configuration used to connect
// to a given RMS API URL, including the client
// configuration itself, as well as retry config.
#[derive(Debug, Clone, Copy)]
pub struct RmsApiConfig<'a> {
    pub url: &'a str,
    pub client_config: &'a RmsClientConfig,
    pub retry_config: RetryConfig,
}

impl<'a> RmsApiConfig<'a> {
    // new creates a new RmsApiConfig, for the given
    // RMS API URL and RmsClientConfig, with
    // a default retry configuration.
    pub fn new(url: &'a str, client_config: &'a RmsClientConfig) -> Self {
        Self {
            url,
            client_config,
            retry_config: RetryConfig::default(),
        }
    }

    // with_retry_config allows a caller to set their
    // own RetryConfig beyond the default.
    pub fn with_retry_config(self, retry_config: RetryConfig) -> Self {
        Self {
            retry_config,
            ..self
        }
    }

    fn retry_config(&self) -> RetryFutureConfig<FixedBackoff, NoOnRetry> {
        RetryFutureConfig::new(self.retry_config.retries).fixed_backoff(self.retry_config.interval)
    }
}

fn rustls_client_builder() -> ConfigBuilder<ClientConfig, WantsVerifier> {
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        // unwrap safety: the error only comes if the configured protocol versions are
        // invalid, which should never happen with the safe defaults.
        .unwrap()
}

#[derive(Clone, Debug)]
pub struct RmsTlsClient<'a> {
    rms_client_config: &'a RmsClientConfig,
}

impl<'a> RmsTlsClient<'a> {
    pub fn new(rms_client_config: &'a RmsClientConfig) -> Self {
        Self { rms_client_config }
    }

    fn build_transport<S: AsRef<str>>(
        &self,
        url: S,
    ) -> RmsTlsClientResult<(TransportService, Uri)> {
        let uri = Uri::from_str(url.as_ref()).map_err(|e| ConfigurationError::InvalidUri {
            uri_string: url.as_ref().to_string(),
            error: e,
        })?;

        // check for certs if the uri given is HTTPS
        // then check if enforce_(m)tls is set
        let config = if let Some(scheme) = uri.scheme()
            && scheme == &tonic::codegen::http::uri::Scheme::HTTPS
        {
            // since its https, look for certs
            match self.rms_client_config.read_root_ca() {
                Ok(root_ca) => match self.rms_client_config.read_client_cert() {
                    Ok((client_cert, client_key)) => {
                        match rustls_client_builder()
                            .with_root_certificates(root_ca)
                            .with_client_auth_cert(client_cert, client_key)
                        {
                            Ok(x) => x,
                            Err(e) => {
                                if self.rms_client_config.enforce_tls {
                                    return Err(RmsTlsClientError::RustTLS(e));
                                }
                                tracing::warn!(
                                    "Error {e} setting up TLS configuration, skipping TLS"
                                );
                                rustls_client_builder()
                                    .dangerous()
                                    .with_custom_certificate_verifier(std::sync::Arc::new(
                                        DummyTlsVerifier::new(),
                                    ))
                                    .with_no_client_auth()
                            }
                        }
                    }
                    Err(e) => {
                        // no mtls
                        if self.rms_client_config.enforce_tls {
                            return Err(e);
                        }
                        rustls_client_builder()
                            .with_root_certificates(root_ca)
                            .with_no_client_auth()
                    }
                },
                Err(e) => {
                    // no tls
                    if self.rms_client_config.enforce_tls {
                        return Err(e);
                    }
                    rustls_client_builder()
                        .dangerous()
                        .with_custom_certificate_verifier(std::sync::Arc::new(
                            DummyTlsVerifier::new(),
                        ))
                        .with_no_client_auth()
                }
            }
        } else {
            // url was HTTP
            if self.rms_client_config.enforce_tls {
                return Err(RmsTlsClientError::Configuration(
                    ConfigurationError::InvalidHTTPURL {
                        url: url.as_ref().to_string(),
                    },
                ));
            }
            rustls_client_builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(DummyTlsVerifier::new()))
                .with_no_client_auth()
        };

        let mut https_connector = TimeoutConnector::new(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(config)
                .https_or_http()
                .enable_http2()
                .build(),
        );

        https_connector.set_connect_timeout(self.rms_client_config.connect_timeout);

        let hyper_client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .http2_only(true)
            // Send a PING frame every this
            .http2_keep_alive_interval(Some(Duration::from_secs(10)))
            // The server will have this much time to respond with a PONG
            .http2_keep_alive_timeout(Duration::from_secs(15))
            // Send PING even when no active http2 streams
            .http2_keep_alive_while_idle(true)
            // How many connections will be kept open, per host.
            .pool_max_idle_per_host(2)
            .timer(TokioTimer::new())
            .pool_timer(TokioTimer::new())
            .build(https_connector)
            .boxed_clone();

        let transport = match &self.rms_client_config.transport_layer {
            Some(layer) => layer.layer(hyper_client),
            None => hyper_client,
        };
        Ok((transport, uri))
    }

    /// Builds a new Client for the Rack Manager API which uses a HTTPS/TLS connector
    /// and appropriate certificates for connecting to the Rack Manager service.
    pub fn build_rms_client<S: AsRef<str>>(
        &self,
        url: S,
    ) -> RmsTlsClientResult<RackManagerClientT> {
        let (transport, uri) = self.build_transport(url)?;
        let mut rms_client = RackManagerClient::with_origin(transport, uri);

        if let Some(max_decoding_message_size) = self.rms_client_config.max_decoding_message_size {
            rms_client = rms_client.max_decoding_message_size(max_decoding_message_size);
        }

        Ok(rms_client)
    }

    /// Builds a new Client for the RackManagerV2 service.
    pub fn build_rms_client_v2<S: AsRef<str>>(
        &self,
        url: S,
    ) -> RmsTlsClientResult<RackManagerV2ClientT> {
        let (transport, uri) = self.build_transport(url)?;
        let mut client_v2 = RackManagerV2Client::with_origin(transport, uri);

        if let Some(max_decoding_message_size) = self.rms_client_config.max_decoding_message_size {
            client_v2 = client_v2.max_decoding_message_size(max_decoding_message_size);
        }

        Ok(client_v2)
    }

    /// Builds a client and retries failed `GetVersion` readiness probes.
    /// Each probe uses `client_config.connect_timeout`, including connection establishment.
    /// `None` leaves probes without a deadline.
    pub async fn retry_build_rms(
        api_config: &RmsApiConfig<'a>,
    ) -> RmsTlsClientResult<RackManagerClientT> {
        // In the retrying function, if the RmsTlsClient just fails to even build, return _that_
        // error early by putting it in the Ok(Err(e)) variant, so that tryhard doesn't keep
        // retrying a configuration error.
        let result: Result<Result<RackManagerClientT, RmsTlsClientError>, RmsTlsClientError> =
            tryhard::retry_fn(|| async move {
                let mut client = match RmsTlsClient::new(api_config.client_config)
                    .build_rms_client(api_config.url)
                {
                    Ok(client) => client,
                    // Don't let tryhard retry this, just push the error into the Ok variant
                    Err(e) => return Ok(Err(e)),
                };

                let probe = client.get_version(tonic::Request::new(
                    crate::protos::rack_manager::GetVersionRequest {},
                ));

                let readiness = match api_config.client_config.connect_timeout {
                    Some(timeout) => {
                        tokio::time::timeout(timeout, probe)
                            .await
                            .unwrap_or_else(|_| {
                                Err(tonic::Status::deadline_exceeded(
                                    "RMS readiness probe timed out",
                                ))
                            })
                    }
                    None => probe.await,
                };

                readiness
                    .inspect_err(|err| {
                        tracing::error!(
                            "error connecting client to rms (url: {}), will retry: {}",
                            api_config.url,
                            err
                        );
                    })
                    .map_err(|e| RmsTlsClientError::Connection(e.to_string()))?;

                // ok, ok
                Ok(Ok(client))
            })
            .with_config(api_config.retry_config())
            .await
            .inspect_err(|err| {
                tracing::error!(
                    "error connecting client to rack manager api (url: {}, attempts: {}): {}",
                    api_config.url,
                    api_config.retry_config.retries,
                    err
                );
            });

        result.flatten()
    }
}
