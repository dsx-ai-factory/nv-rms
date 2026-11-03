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

use std::collections::HashMap;
use std::sync::Arc;

use secrecy::{ExposeSecret, SecretString};

use super::node::{ExpectedInventoryPolicy, Node, NodeRuntimeConfig, NodeType, RecoveryProfile};
use crate::utilities::error::Result;

pub(crate) const RACK_POWER_BUSY_MESSAGE: &str = "rack power operation in progress";

// ── Node Config ──

/// Network endpoint used to communicate with a node management interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub ip_address: String,
    pub mac_address: String,
    pub port: u16,
    /// DNS hostname for HTTPS/mTLS management APIs (for example NVUE) when set;
    /// NVUE falls back to `ip_address`. SSH/SFTP always use `ip_address`.
    pub host_name: Option<String>,
}

/// Credentials used for endpoint authentication.
#[derive(Debug, Clone)]
pub enum EndpointCredentials {
    /// Username/password authentication.
    UserPass {
        username: String,
        password: SecretString,
    },

    /// Redfish session-token authentication for BMC endpoints.
    BmcSessionToken(SecretString),
}

impl PartialEq for EndpointCredentials {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::UserPass {
                    username: left_username,
                    password: left_password,
                },
                Self::UserPass {
                    username: right_username,
                    password: right_password,
                },
            ) => {
                left_username == right_username
                    && left_password.expose_secret() == right_password.expose_secret()
            }
            (Self::BmcSessionToken(left), Self::BmcSessionToken(right)) => {
                left.expose_secret() == right.expose_secret()
            }
            (Self::UserPass { .. }, Self::BmcSessionToken(_))
            | (Self::BmcSessionToken(_), Self::UserPass { .. }) => false,
        }
    }
}

impl Eq for EndpointCredentials {}

impl EndpointCredentials {
    /// Create endpoint credentials from a username and password.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::UserPass {
            username: username.into(),
            password: SecretString::from(password.into()),
        }
    }

    /// Create BMC credentials that authenticate Redfish requests with a session token.
    pub fn from_session_token(token: impl Into<String>) -> Self {
        Self::BmcSessionToken(SecretString::from(token.into()))
    }

    /// Return credential mode name for credential-safe logs.
    pub fn auth_type(&self) -> &'static str {
        match self {
            Self::UserPass { .. } => "user_pass",
            Self::BmcSessionToken(_) => "session_token",
        }
    }

    /// Return whether the credential carries complete, usable authentication material.
    pub fn has_auth_material(&self) -> bool {
        match self {
            Self::UserPass { username, password } => {
                !username.is_empty() && !password.expose_secret().is_empty()
            }
            Self::BmcSessionToken(token) => !token.expose_secret().is_empty(),
        }
    }

    /// Return username/password credentials, excluding BMC session tokens.
    pub fn user_pass(&self) -> Option<(&str, &str)> {
        let Self::UserPass { username, password } = self else {
            return None;
        };

        (!username.is_empty() && !password.expose_secret().is_empty())
            .then_some((username, password.expose_secret()))
    }

    /// Return username/password credentials without exposing the password.
    pub(crate) fn user_pass_secret(&self) -> Option<(&str, &SecretString)> {
        let Self::UserPass { username, password } = self else {
            return None;
        };

        (!username.is_empty() && !password.expose_secret().is_empty())
            .then_some((username.as_str(), password))
    }
}

/// Endpoint plus the auth settings needed to communicate with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointConfig {
    pub endpoint: Endpoint,
    pub credentials: Option<EndpointCredentials>,
    pub dangerously_accept_invalid_certs: bool,
}

impl EndpointConfig {
    /// Create an endpoint config without credentials.
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            credentials: None,
            dangerously_accept_invalid_certs: false,
        }
    }

    /// Create an endpoint config with optional credentials.
    pub fn with_credentials(
        endpoint: Endpoint,
        credentials: Option<EndpointCredentials>,
        dangerously_accept_invalid_certs: bool,
    ) -> Self {
        Self {
            endpoint,
            credentials,
            dangerously_accept_invalid_certs,
        }
    }
}

/// Configuration for creating a new node, populated from gRPC request fields.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: String,
    pub node_type: NodeType,
    pub bmc_endpoint: Option<EndpointConfig>,
    pub host_endpoint: Option<EndpointConfig>,
    pub expected_inventory: Option<ExpectedInventoryPolicy>,
    /// The recovery profile bound to this node, still the opaque table. The
    /// node type parses it in `from_config`; `None` leaves detection off.
    pub recovery: Option<RecoveryProfile>,
    /// Optional profile-derived parameters for the concrete implementation.
    /// Production request parsing populates these from the immutable startup
    /// hardware-profile catalog; node-type dispatch remains code-defined.
    pub runtime_config: Option<NodeRuntimeConfig>,
}

impl NodeConfig {
    /// Create a node config with a default BMC endpoint port.
    pub fn new(id: impl Into<String>, node_type: NodeType, host: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            node_type,
            bmc_endpoint: Some(EndpointConfig::new(Endpoint {
                ip_address: host.into(),
                mac_address: String::new(),
                port: 443,
                host_name: None,
            })),
            host_endpoint: None,
            expected_inventory: None,
            recovery: None,
            runtime_config: None,
        }
    }
}

// ── Rack Trait ──

/// Trait for all rack types (e.g., NvlGb200Rack, NvlGb300Rack).
///
/// Each concrete rack type owns a map of node values and provides a node
/// factory that creates the appropriate concrete node from config. Racks handle
/// node CRUD. All methods are synchronous — node map operations are quick
/// in-memory work protected by internal locks.
pub trait Rack: Send + Sync {
    /// Node handle stored by this rack implementation.
    type Node: Node + 'static;

    // ── Identity ──

    fn id(&self) -> &str;
    fn rack_type(&self) -> &'static str;
    fn get_info(&self) -> HashMap<String, String>;

    // ── Node Factory ──

    fn create_node(&self, config: &NodeConfig) -> Result<Arc<Self::Node>>;

    // ── Node Management ──

    fn add_node(&self, node_id: &str, node: Arc<Self::Node>) -> Result<()>;
    fn remove_node(&self, node_id: &str) -> Result<()>;
    fn find_node(&self, node_id: &str) -> Option<Arc<Self::Node>>;
    fn list_nodes(&self) -> Vec<Arc<Self::Node>>;

    /// Tries to acquire the guard that serializes registered power operations
    /// with inventory mutations on this rack.
    fn try_power_operation_guard(
        &self,
    ) -> std::result::Result<tokio::sync::OwnedMutexGuard<()>, tokio::sync::TryLockError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_config_new_sets_defaults() {
        let config = NodeConfig::new("compute-01", NodeType::ComputeGb200Nvidia, "10.0.0.1");
        assert_eq!(config.id, "compute-01");
        assert_eq!(config.node_type, NodeType::ComputeGb200Nvidia);
        let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
            panic!("expected BMC endpoint");
        };

        assert_eq!(bmc_endpoint.endpoint.port, 443);
        assert!(config.host_endpoint.is_none());
        assert!(!bmc_endpoint.dangerously_accept_invalid_certs);
        assert!(bmc_endpoint.credentials.is_none());
    }

    #[test]
    fn endpoint_credentials_debug_redacts_password() {
        let credentials = EndpointCredentials::new("admin", "secret");

        let debug = format!("{credentials:?}");

        assert!(debug.contains("admin"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn endpoint_session_token_is_redacted_and_not_exposed_as_user_pass() {
        let credentials = EndpointCredentials::from_session_token("session-secret");

        let debug = format!("{credentials:?}");

        assert_eq!(credentials.auth_type(), "session_token");
        assert!(credentials.has_auth_material());
        assert!(credentials.user_pass().is_none());
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("session-secret"));
    }
}
