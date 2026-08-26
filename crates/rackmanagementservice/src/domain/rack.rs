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

use crate::domain::node::{ExpectedInventoryPolicy, Node, NodeType};
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

/// Username/password pair used for endpoint authentication.
#[derive(Debug, Clone)]
pub struct EndpointCredentials {
    pub username: String,
    pub password: SecretString,
}

impl PartialEq for EndpointCredentials {
    fn eq(&self, other: &Self) -> bool {
        self.username == other.username
            && self.password.expose_secret() == other.password.expose_secret()
    }
}

impl Eq for EndpointCredentials {}

impl EndpointCredentials {
    /// Create endpoint credentials from a username and password.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: SecretString::from(password.into()),
        }
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
        }
    }
}

// ── Power-On Order ──

/// A single step in a rack's power-on sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PowerOnStep {
    pub order_index: i32,
    pub node_id: String,
    pub completion_check: bool,
    pub timeout_seconds: u32,
}

impl PowerOnStep {
    pub fn new(order_index: i32, node_id: impl Into<String>) -> Self {
        Self {
            order_index,
            node_id: node_id.into(),
            completion_check: false,
            timeout_seconds: 300,
        }
    }
}

// ── Rack Trait ──

/// Trait for all rack types (e.g., NvlGb200Rack, NvlGb300Rack).
///
/// Each concrete rack type owns a map of node values and provides a node
/// factory that creates the appropriate concrete node from config. Racks handle
/// node CRUD and power-on sequencing. All methods are synchronous — node map
/// operations are quick in-memory work protected by internal locks.
///
/// Adding/removing nodes invalidates the power-on order (cleared by concrete
/// implementations in `add_node`/`remove_node`).
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

    /// Tries to acquire the guard that serializes inventory-backed rack power mutations.
    fn try_power_operation_guard(
        &self,
    ) -> std::result::Result<tokio::sync::OwnedMutexGuard<()>, tokio::sync::TryLockError>;

    // ── Power-On Order ──

    /// Validates and stores the power-on order against current rack inventory.
    fn set_power_on_order(&self, order: Vec<PowerOnStep>) -> Result<()>;
    fn get_power_on_order(&self) -> Vec<PowerOnStep>;
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
    fn power_on_step_new_sets_defaults() {
        let step = PowerOnStep::new(0, "pshelf-01");
        assert_eq!(step.order_index, 0);
        assert_eq!(step.node_id, "pshelf-01");
        assert!(!step.completion_check);
        assert_eq!(step.timeout_seconds, 300);
    }
}
