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

mod auth;
mod gnmi_api;
#[cfg(test)]
pub(crate) mod test_support;
mod tls;

use std::time::Duration;

pub use crate::utilities::url::grpc_target_uri;
pub use auth::is_capabilities_connect_error;
pub use gnmi_api::GnmiApi;
pub use tls::{GnmiTlsConfig, tls_config_from_paths};

pub use crate::libnmxc::Endpoint;

use crate::libnmxc::{NmxcClientPool, NmxcError};

/// Default gNMI gRPC port on NVOS switches.
pub const DEFAULT_GNMI_PORT: u16 = 9339;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Generated protobuf types from `gnmi.proto`.
pub mod gnmi_model {
    #![allow(clippy::all, unused_qualifications, non_snake_case)]
    pub use crate::api::grpc::proto::gnmi::*;
}

pub use crate::libnmxc::{TlsMaterialStore, normalize_domain, validate_domain};

#[derive(thiserror::Error, Debug)]
pub enum GnmiError {
    #[error(transparent)]
    Nmxc(#[from] NmxcError),

    #[error("invalid gNMI {field} metadata: {message}")]
    InvalidMetadata {
        field: &'static str,
        message: String,
    },

    #[error("gRPC status: {0}")]
    Status(#[from] tonic::Status),
}

impl GnmiError {
    pub fn invalid_response(msg: impl Into<String>) -> Self {
        Self::Nmxc(NmxcError::invalid_response(msg))
    }
}

impl From<GnmiError> for crate::utilities::error::RmsError {
    fn from(err: GnmiError) -> Self {
        match err {
            GnmiError::Nmxc(err) => Self::internal(err.to_string()),
            GnmiError::InvalidMetadata { field, message } => {
                Self::invalid_argument(format!("invalid gNMI {field} metadata: {message}"))
            }
            GnmiError::Status(status) => Self::internal(format!("gRPC status: {status}")),
        }
    }
}

/// Basic-auth credentials carried in gNMI gRPC metadata.
#[derive(Clone, Debug)]
pub struct GnmiCredentials {
    pub username: String,
    pub password: String,
}

impl GnmiCredentials {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

/// Build the HTTPS gRPC target URI for gNMI Capabilities on a switch host.
pub fn capabilities_target_uri(switch_host: &str) -> String {
    grpc_target_uri(switch_host, DEFAULT_GNMI_PORT, true)
}

pub fn capabilities_connect_error_message(
    target_uri: &str,
    tls_authority: &str,
    source: impl std::fmt::Display,
) -> String {
    format!(
        "gNMI Capabilities connect failed for {target_uri} (authority={tls_authority}): {source}"
    )
}

pub fn capabilities_rpc_error_message(
    target_uri: &str,
    tls_authority: &str,
    source: impl std::fmt::Display,
) -> String {
    format!("gNMI Capabilities failed for {target_uri} (authority={tls_authority}): {source}")
}

#[derive(Clone, Debug)]
pub struct GnmiClientPoolBuilder {
    pub timeout: Duration,
}

impl Default for GnmiClientPoolBuilder {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl GnmiClientPoolBuilder {
    pub fn build(self) -> Result<GnmiClientPool, GnmiError> {
        Ok(GnmiClientPool {
            inner: NmxcClientPool::builder().timeout(self.timeout).build()?,
        })
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Clone, Debug)]
pub struct GnmiClientPool {
    inner: NmxcClientPool,
}

impl GnmiClientPool {
    pub fn builder() -> GnmiClientPoolBuilder {
        GnmiClientPoolBuilder::default()
    }

    pub async fn create_client(
        &self,
        endpoint: Endpoint,
        tls: Option<&GnmiTlsConfig>,
        credentials: GnmiCredentials,
    ) -> Result<Box<dyn Gnmi>, GnmiError> {
        let channel = self.inner.create_channel(endpoint, tls).await?;
        Ok(Box::new(GnmiApi::new(channel, credentials)))
    }
}

/// Abstraction over [`GnmiClientPool`] and test doubles.
#[async_trait::async_trait]
pub trait GnmiPool: Send + Sync + 'static {
    async fn create_client(
        &self,
        endpoint: Endpoint,
        tls: Option<&GnmiTlsConfig>,
        credentials: GnmiCredentials,
    ) -> Result<Box<dyn Gnmi>, GnmiError>;
}

#[async_trait::async_trait]
impl GnmiPool for GnmiClientPool {
    async fn create_client(
        &self,
        endpoint: Endpoint,
        tls: Option<&GnmiTlsConfig>,
        credentials: GnmiCredentials,
    ) -> Result<Box<dyn Gnmi>, GnmiError> {
        GnmiClientPool::create_client(self, endpoint, tls, credentials).await
    }
}

/// gNMI client operations used by RMS switch certificate workflows.
#[async_trait::async_trait]
pub trait Gnmi: Send + Sync + 'static {
    /// `Capabilities` connectivity probe over mTLS with basic-auth metadata.
    async fn capabilities(&mut self) -> Result<gnmi_model::CapabilityResponse, GnmiError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_target_uri_uses_gnmi_port() {
        assert_eq!(
            capabilities_target_uri("switch.example.com"),
            "https://switch.example.com:9339"
        );
    }

    #[test]
    fn capabilities_target_uri_formats_ipv6_literal() {
        assert_eq!(
            capabilities_target_uri("2001:db8::1"),
            "https://[2001:db8::1]:9339"
        );
    }
}
