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

use std::path::Path;
use std::time::Duration;

use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::{ConfigurationError, RmsTlsClientError};

const SPIFFE_CERT: &str = "/var/run/secrets/spiffe.io/tls.crt";
const SPIFFE_KEY: &str = "/var/run/secrets/spiffe.io/tls.key";
const SPIFFE_CA: &str = "/var/run/secrets/spiffe.io/ca.crt";

#[derive(thiserror::Error, Debug)]
pub enum ClientConfigError {
    #[error("Unable to parse url: {0}")]
    UrlParseError(String),
}

#[derive(Clone, Debug)]
pub struct ClientCert {
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Clone, Debug, Default)]
pub struct RmsClientConfig {
    /// Optional caller-owned HTTP transport layer, disabled by default.
    pub transport_layer: Option<std::sync::Arc<dyn crate::client::RmsTransportLayer>>,
    /// Optional decoded unary RPC observer, disabled by default. Shared by cloned clients.
    pub rpc_observer: Option<std::sync::Arc<dyn tonic_client_wrapper::RpcObserver>>,
    pub root_ca_path: Option<String>,
    pub client_cert: Option<ClientCert>,
    pub enforce_tls: bool,
    pub max_decoding_message_size: Option<usize>,

    /// Bounds connection establishment, including TLS, and each `GetVersion` readiness
    /// attempt. Does not limit socket reads, writes, or application RPC execution.
    /// `None` disables both limits. `new` uses ten seconds; `Default` uses `None`.
    pub connect_timeout: Option<Duration>,

    pub connect_retries_max: Option<u32>,
    pub connect_retries_interval: Option<Duration>,
}

/// look for a fallback if user did not specify
/// generally a crate should not be doing this.
pub fn rms_client_cert_info(
    client_cert: Option<String>,
    client_key: Option<String>,
) -> Option<ClientCert> {
    // First from args
    if let Some(client_cert) = client_cert {
        {
            let client_key = client_key?;
            return Some(ClientCert {
                cert_path: client_cert,
                key_path: client_key,
            });
        }
    }

    // this is the location for most k8s pods
    if Path::new(SPIFFE_CERT).exists() && Path::new(SPIFFE_KEY).exists() {
        return Some(ClientCert {
            cert_path: SPIFFE_CERT.to_string(),
            key_path: SPIFFE_KEY.to_string(),
        });
    }

    // RMS client cert is optional - if not found, return None instead of panicking
    None
}

/// look for a fallback if user did not specify
/// generally a crate should not be doing this.
pub fn rms_root_ca_info(rms_root_ca_path: Option<String>) -> Option<String> {
    if let Some(x) = rms_root_ca_path
        && Path::new(x.as_str()).exists()
    {
        return Some(x);
    }

    // this is the location for most k8s pods
    if Path::new(SPIFFE_CA).exists() {
        return Some(SPIFFE_CA.to_string());
    }

    None
}

impl RmsClientConfig {
    pub fn new(
        root_ca_path: Option<String>,
        client_cert: Option<String>,
        client_key: Option<String>,
        enforce_tls: bool,
    ) -> Self {
        let client_cert = rms_client_cert_info(client_cert, client_key);
        let root_ca_path = rms_root_ca_info(root_ca_path);

        let disabled = client_cert.is_none() || root_ca_path.is_none();
        let can_enforce_tls = if enforce_tls && disabled {
            tracing::warn!(
                "TLS enforcing set but certs not provided, TLS enforcement will be disabled."
            );
            false
        } else {
            // disabled is false, use passed in setting
            enforce_tls
        };
        let max_decoding_message_size = std::env::var("TONIC_MAX_DECODING_MESSAGE_SIZE")
            .ok()
            .and_then(|ms| ms.parse::<usize>().ok());

        Self {
            transport_layer: None,
            rpc_observer: None,
            root_ca_path,
            client_cert,
            enforce_tls: can_enforce_tls,
            max_decoding_message_size,
            connect_timeout: Some(Duration::from_secs(10)),
            connect_retries_max: Some(3),
            connect_retries_interval: Some(Duration::from_secs(20)),
        }
    }

    pub fn read_client_cert(
        &self,
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), RmsTlsClientError> {
        if let Some(client_cert) = self.client_cert.as_ref() {
            let certs = {
                let fd = std::fs::File::open(&client_cert.cert_path).map_err(|e| {
                    RmsTlsClientError::Configuration(ConfigurationError::CouldNotReadClientCert {
                        path: client_cert.cert_path.clone(),
                        error: e,
                    })
                })?;
                let mut buf = std::io::BufReader::new(&fd);

                let mut errors = vec![];

                let valid_certificates = rustls_pemfile::certs(&mut buf)
                    .filter_map(|result| match result {
                        Ok(v) => Some(Ok(v)),
                        Err(err) => match err.kind() {
                            std::io::ErrorKind::InvalidData => {
                                errors.push(err);
                                None
                            }
                            _ => Some(Err(err)),
                        },
                    })
                    .collect::<eyre::Result<Vec<_>, _>>()
                    .unwrap_or_else(|err| {
                        errors.push(err);
                        vec![]
                    });

                if !errors.is_empty() {
                    if valid_certificates.is_empty() {
                        return Err(RmsTlsClientError::Configuration(
                            ConfigurationError::InvalidClientCert {
                                error: errors.iter().map(|x| x.to_string()).collect(),
                            },
                        ));
                    }
                    tracing::warn!( certs = ?errors, "Found error parsing one or more certificates");
                }

                valid_certificates
            };

            let key = {
                let fd = std::fs::File::open(&client_cert.key_path).map_err(|e| {
                    RmsTlsClientError::Configuration(ConfigurationError::CouldNotReadClientKey {
                        path: client_cert.key_path.clone(),
                        error: e,
                    })
                })?;
                let mut buf = std::io::BufReader::new(&fd);

                use rustls_pemfile::Item;

                match rustls_pemfile::read_one(&mut buf) {
                    Ok(Some(item)) => match item {
                        Item::Pkcs1Key(key) => Some(key.into()),
                        Item::Pkcs8Key(key) => Some(key.into()),
                        Item::Sec1Key(key) => Some(key.into()),
                        _ => None,
                    },
                    Err(e) => {
                        return Err(RmsTlsClientError::Configuration(
                            ConfigurationError::InvalidClientKey {
                                error: e.to_string(),
                            },
                        ));
                    }
                    _ => None,
                }
            };

            let key = match key {
                Some(key) => key,
                None => {
                    // tracing::error!("Rustls error: no keys?");
                    return Err(RmsTlsClientError::Configuration(
                        ConfigurationError::InvalidClientKey {
                            error: "No Client keys available".to_string(),
                        },
                    ));
                }
            };

            Ok((certs, key))
        } else {
            Err(RmsTlsClientError::Configuration(
                ConfigurationError::InvalidClientCert {
                    error: "No Client cert specified or available".to_string(),
                },
            ))
        }
    }

    pub fn read_root_ca(&self) -> Result<RootCertStore, RmsTlsClientError> {
        if let Some(root_ca_path) = &self.root_ca_path {
            let mut roots = RootCertStore::empty();
            let fd = std::fs::File::open(root_ca_path).map_err(|e| {
                RmsTlsClientError::Configuration(ConfigurationError::CouldNotReadRootCa {
                    path: root_ca_path.to_string(),
                    error: e,
                })
            })?;
            let mut buf = std::io::BufReader::new(&fd);
            let mut errors = vec![];

            roots.add_parsable_certificates(rustls_pemfile::certs(&mut buf).filter_map(|result| {
                match result {
                    Ok(cert) => Some(cert),
                    Err(e) => {
                        errors.push(e);
                        None
                    }
                }
            }));
            if roots.is_empty() {
                return Err(RmsTlsClientError::Configuration(
                    ConfigurationError::InvalidRootCa {
                        error: errors.iter().map(|e| e.to_string()).collect(),
                    },
                ));
            }
            Ok(roots)
        } else {
            Err(RmsTlsClientError::Configuration(
                ConfigurationError::InvalidRootCa {
                    error: "Root CA path not specified".to_string(),
                },
            ))
        }
    }
}
