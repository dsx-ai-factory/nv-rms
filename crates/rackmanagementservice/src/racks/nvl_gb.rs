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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use secrecy::ExposeSecret;

use crate::domain::node::{Node, NodeKind, NodeType, ProductFamily};
use crate::domain::rack::{NodeConfig, PowerOnStep, Rack};
use crate::nodes::NodeInstance;
use crate::utilities::error::{Result, RmsError};

/// Shared concrete rack implementation for supported NVL configurations.
pub struct NvlGb200Rack {
    id: String,
    rack_type: &'static str,
    model: &'static str,

    // Serializes inventory-backed power mutations for this rack. Handlers may
    // hold it across awaited Redfish calls to keep rack sequences and registered
    // direct power RPCs from interleaving.
    power_operation_lock: Arc<tokio::sync::Mutex<()>>,

    // Inventory and power_on_order are one locked state because the order is
    // only valid for the inventory snapshot it was checked against.
    state: RwLock<RackState>,
}

pub struct NvlGb300Rack {
    inner: NvlGb200Rack,
}

pub struct NvlVrnvl72Rack {
    inner: NvlGb200Rack,
}

struct RackState {
    nodes: HashMap<String, Arc<NodeInstance>>,
    power_on_order: Vec<PowerOnStep>,
}

impl NvlGb200Rack {
    pub fn new(id: String) -> Self {
        Self::new_with_profile(
            id,
            ProductFamily::Gb200.rack_type(),
            ProductFamily::Gb200.model(),
        )
    }

    fn new_with_profile(id: String, rack_type: &'static str, model: &'static str) -> Self {
        Self {
            id,
            rack_type,
            model,
            power_operation_lock: Arc::new(tokio::sync::Mutex::new(())),
            state: RwLock::new(RackState {
                nodes: HashMap::new(),
                power_on_order: Vec::new(),
            }),
        }
    }

    fn supports_node_type(&self, node_type: NodeType) -> bool {
        self.rack_type == node_type.product_family().rack_type()
    }
}

impl NvlGb300Rack {
    pub fn new(id: String) -> Self {
        Self {
            inner: NvlGb200Rack::new_with_profile(
                id,
                ProductFamily::Gb300.rack_type(),
                ProductFamily::Gb300.model(),
            ),
        }
    }
}

impl NvlVrnvl72Rack {
    pub fn new(id: String) -> Self {
        Self {
            inner: NvlGb200Rack::new_with_profile(
                id,
                ProductFamily::Vrnvl72.rack_type(),
                ProductFamily::Vrnvl72.model(),
            ),
        }
    }
}

impl Rack for NvlGb200Rack {
    type Node = NodeInstance;

    fn id(&self) -> &str {
        &self.id
    }

    fn rack_type(&self) -> &'static str {
        self.rack_type
    }

    fn get_info(&self) -> HashMap<String, String> {
        let state = self.state.read().unwrap();
        HashMap::from([
            ("model".to_owned(), self.model.to_owned()),
            ("nodeCount".to_owned(), state.nodes.len().to_string()),
        ])
    }

    fn create_node(&self, config: &NodeConfig) -> Result<Arc<NodeInstance>> {
        let node_type = config.node_type;
        if !self.supports_node_type(node_type) {
            return Err(RmsError::invalid_argument(format!(
                "rack type {} does not support node type {}",
                self.rack_type, node_type
            )));
        }

        validate_node_config(config)?;
        validate_credentials(config, node_type.kind())?;

        let node = NodeInstance::create(config, &self.id)?;
        tracing::info!(
            node = %config.id,
            rack = %self.id,
            node_type = %node_type,
            "created node"
        );
        Ok(node)
    }

    fn add_node(&self, node_id: &str, node: Arc<NodeInstance>) -> Result<()> {
        if node_id.is_empty() {
            return Err(RmsError::invalid_argument("node ID cannot be empty"));
        }

        let canonical_node_id = node.id();
        if canonical_node_id != node_id {
            return Err(RmsError::invalid_argument(format!(
                "node ID mismatch: requested {node_id}, node id {canonical_node_id}"
            )));
        }

        let mut state = self.state.write().unwrap();
        if state.nodes.contains_key(node_id) {
            return Err(RmsError::already_exists(format!(
                "node {node_id} already exists in rack {}",
                self.id
            )));
        }

        state.nodes.insert(node_id.to_owned(), node);

        if !state.power_on_order.is_empty() {
            state.power_on_order.clear();
            tracing::info!(
                rack = %self.id,
                "power-on order invalidated due to node addition"
            );
        }

        Ok(())
    }

    fn remove_node(&self, node_id: &str) -> Result<()> {
        let mut state = self.state.write().unwrap();
        if state.nodes.remove(node_id).is_none() {
            return Err(RmsError::not_found(format!(
                "node {node_id} not found in rack {}",
                self.id
            )));
        }

        if !state.power_on_order.is_empty() {
            state.power_on_order.clear();
            tracing::info!(
                rack = %self.id,
                "power-on order invalidated due to node removal"
            );
        }

        Ok(())
    }

    fn find_node(&self, node_id: &str) -> Option<Arc<NodeInstance>> {
        let state = self.state.read().unwrap();
        state.nodes.get(node_id).cloned()
    }

    fn list_nodes(&self) -> Vec<Arc<NodeInstance>> {
        let state = self.state.read().unwrap();
        state.nodes.values().cloned().collect()
    }

    fn try_power_operation_guard(
        &self,
    ) -> std::result::Result<tokio::sync::OwnedMutexGuard<()>, tokio::sync::TryLockError> {
        self.power_operation_lock.clone().try_lock_owned()
    }

    fn set_power_on_order(&self, order: Vec<PowerOnStep>) -> Result<()> {
        // Inventory and order share this lock because a power-on order is only
        // executable relative to the same inventory snapshot. Holding the write
        // lock through validation and assignment prevents add/remove from
        // changing nodes between the check and the persisted update.
        let mut state = self.state.write().unwrap();
        validate_power_on_order(&state.nodes, &order)?;
        state.power_on_order = order;

        Ok(())
    }

    fn get_power_on_order(&self) -> Vec<PowerOnStep> {
        let state = self.state.read().unwrap();
        state.power_on_order.clone()
    }
}

impl Rack for NvlGb300Rack {
    type Node = NodeInstance;

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_type(&self) -> &'static str {
        self.inner.rack_type()
    }

    fn get_info(&self) -> HashMap<String, String> {
        self.inner.get_info()
    }

    fn create_node(&self, config: &NodeConfig) -> Result<Arc<NodeInstance>> {
        self.inner.create_node(config)
    }

    fn add_node(&self, node_id: &str, node: Arc<NodeInstance>) -> Result<()> {
        self.inner.add_node(node_id, node)
    }

    fn remove_node(&self, node_id: &str) -> Result<()> {
        self.inner.remove_node(node_id)
    }

    fn find_node(&self, node_id: &str) -> Option<Arc<NodeInstance>> {
        self.inner.find_node(node_id)
    }

    fn list_nodes(&self) -> Vec<Arc<NodeInstance>> {
        self.inner.list_nodes()
    }

    fn try_power_operation_guard(
        &self,
    ) -> std::result::Result<tokio::sync::OwnedMutexGuard<()>, tokio::sync::TryLockError> {
        self.inner.try_power_operation_guard()
    }

    fn set_power_on_order(&self, order: Vec<PowerOnStep>) -> Result<()> {
        self.inner.set_power_on_order(order)
    }

    fn get_power_on_order(&self) -> Vec<PowerOnStep> {
        self.inner.get_power_on_order()
    }
}

impl Rack for NvlVrnvl72Rack {
    type Node = NodeInstance;

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_type(&self) -> &'static str {
        self.inner.rack_type()
    }

    fn get_info(&self) -> HashMap<String, String> {
        self.inner.get_info()
    }

    fn create_node(&self, config: &NodeConfig) -> Result<Arc<NodeInstance>> {
        self.inner.create_node(config)
    }

    fn add_node(&self, node_id: &str, node: Arc<NodeInstance>) -> Result<()> {
        self.inner.add_node(node_id, node)
    }

    fn remove_node(&self, node_id: &str) -> Result<()> {
        self.inner.remove_node(node_id)
    }

    fn find_node(&self, node_id: &str) -> Option<Arc<NodeInstance>> {
        self.inner.find_node(node_id)
    }

    fn list_nodes(&self) -> Vec<Arc<NodeInstance>> {
        self.inner.list_nodes()
    }

    fn try_power_operation_guard(
        &self,
    ) -> std::result::Result<tokio::sync::OwnedMutexGuard<()>, tokio::sync::TryLockError> {
        self.inner.try_power_operation_guard()
    }

    fn set_power_on_order(&self, order: Vec<PowerOnStep>) -> Result<()> {
        self.inner.set_power_on_order(order)
    }

    fn get_power_on_order(&self) -> Vec<PowerOnStep> {
        self.inner.get_power_on_order()
    }
}

/// Verifies that a submitted power-on order is executable for the rack inventory.
fn validate_power_on_order(
    nodes: &HashMap<String, Arc<NodeInstance>>,
    order: &[PowerOnStep],
) -> Result<()> {
    if order.is_empty() {
        return Err(RmsError::invalid_argument("power-on order is required"));
    }

    let mut node_ids = HashSet::with_capacity(order.len());
    for step in order {
        let node_id = &step.node_id;

        if node_id.is_empty() {
            return Err(RmsError::invalid_argument(
                "node_id is required in power-on order",
            ));
        }

        if !node_ids.insert(node_id.as_str()) {
            return Err(RmsError::invalid_argument(format!(
                "duplicate node in power-on order: {node_id}"
            )));
        }

        if !nodes.contains_key(node_id) {
            return Err(RmsError::invalid_argument(format!(
                "unknown node in power-on order: {node_id}"
            )));
        }
    }

    Ok(())
}

fn validate_node_config(config: &NodeConfig) -> Result<()> {
    // Registered rack inventory always keeps a BMC endpoint as the stable
    // management address. Direct/stateless RPC paths may build host-only
    // switches, but inventory nodes still need BMC data for Redfish workflows.
    if config.id.is_empty() {
        return Err(RmsError::invalid_argument("missing node ID"));
    }

    let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
        return Err(RmsError::invalid_argument(format!(
            "missing BMC endpoint for node {}",
            config.id
        )));
    };

    if bmc_endpoint.endpoint.ip_address.is_empty() {
        return Err(RmsError::invalid_argument(format!(
            "missing host for node {}",
            config.id
        )));
    }

    if bmc_endpoint.endpoint.mac_address.is_empty() {
        return Err(RmsError::invalid_argument(format!(
            "missing macAddress for node {}",
            config.id
        )));
    }
    Ok(())
}

fn validate_credentials(config: &NodeConfig, node_kind: NodeKind) -> Result<()> {
    let credentials = config
        .bmc_endpoint
        .as_ref()
        .and_then(|endpoint| endpoint.credentials.as_ref());

    let username = credentials
        .map(|credentials| credentials.username.as_str())
        .unwrap_or_default();
    let password = credentials
        .map(|credentials| credentials.password.expose_secret())
        .unwrap_or_default();

    if username.is_empty() || password.is_empty() {
        return Err(RmsError::invalid_argument(format!(
            "create_{node_kind}: missing credentials for {} (provide username/password)",
            config.id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, Rack};

    fn test_config(id: &str, node_type: NodeType) -> NodeConfig {
        let mut config = NodeConfig {
            id: id.to_owned(),
            node_type,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.1".to_owned(),
                    mac_address: "aa:bb:cc:dd:ee:ff".to_owned(),
                    port: 443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "pass")),
                false,
            )),
            host_endpoint: None,
            expected_inventory: None,
        };
        if node_type.kind() == NodeKind::Switch {
            config.host_endpoint = Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.2".to_owned(),
                    mac_address: "11:22:33:44:55:66".to_owned(),
                    port: 443,
                    host_name: Some(format!("{id}.switch.test.local")),
                },
                Some(EndpointCredentials::new("admin", "pass")),
                false,
            ));
        }

        config
    }

    #[test]
    fn create_node_compute() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .unwrap();
        assert_eq!(node.node_type(), NodeType::ComputeGb200Nvidia);
        assert_eq!(node.id(), "c-01");
        assert_eq!(node.rack_id(), "rack-01");
    }

    #[test]
    fn create_node_compute_gb200_wiwynn() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Wiwynn))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::ComputeGb200Wiwynn);
        assert_eq!(node.get_info()["type"], "compute_gb200_wiwynn");
    }

    #[test]
    fn create_node_gb300_compute() {
        let rack = NvlGb300Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb300Nvidia))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::ComputeGb300Nvidia);
        assert_eq!(node.id(), "c-01");
        assert_eq!(node.rack_id(), "rack-01");
    }

    #[test]
    fn create_node_compute_gb300_lenovo() {
        let rack = NvlGb300Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb300Lenovo))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::ComputeGb300Lenovo);
        assert_eq!(node.get_info()["type"], "compute_gb300_lenovo");
    }

    #[test]
    fn create_node_compute_gb300_supermicro() {
        let rack = NvlGb300Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb300Supermicro))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::ComputeGb300Supermicro);
        assert_eq!(node.get_info()["type"], "compute_gb300_supermicro");
    }

    #[test]
    fn create_node_vrnvl72_compute() {
        let rack = NvlVrnvl72Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeVrnvl72Nvidia))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::ComputeVrnvl72Nvidia);
        assert_eq!(node.get_info()["type"], "compute_vrnvl72_nvidia");
    }

    #[test]
    fn create_node_vrnvl72_switch() {
        let rack = NvlVrnvl72Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("s-01", NodeType::SwitchVrnvl72Nvidia))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::SwitchVrnvl72Nvidia);
        assert_eq!(node.get_info()["type"], "switch_vrnvl72_nvidia");
    }

    #[test]
    fn create_node_switch() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("s-01", NodeType::SwitchGb200Nvidia))
            .unwrap();
        assert_eq!(node.node_type(), NodeType::SwitchGb200Nvidia);
    }

    #[test]
    fn create_node_gb300_switch() {
        let rack = NvlGb300Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("s-01", NodeType::SwitchGb300Nvidia))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::SwitchGb300Nvidia);
        assert_eq!(node.get_info()["type"], "switch_gb300_nvidia");
    }

    #[test]
    fn create_node_switch_accepts_bmc_only_config() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let mut config = test_config("s-01", NodeType::SwitchGb200Nvidia);
        config.host_endpoint = None;

        let node = rack.create_node(&config).unwrap();

        assert_eq!(node.node_type(), NodeType::SwitchGb200Nvidia);
    }

    #[test]
    fn create_node_switch_accepts_host_endpoint_without_host_credentials() -> Result<()> {
        let rack = NvlGb200Rack::new("rack-01".into());
        let mut config = test_config("s-01", NodeType::SwitchGb200Nvidia);
        let Some(host_endpoint) = config.host_endpoint.as_mut() else {
            panic!("expected host endpoint");
        };

        host_endpoint.credentials = None;

        let node = rack.create_node(&config)?;

        assert_eq!(node.node_type(), NodeType::SwitchGb200Nvidia);

        Ok(())
    }

    #[test]
    fn create_node_powershelf() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("p-01", NodeType::PowershelfGb200Liteon))
            .unwrap();
        assert_eq!(node.node_type(), NodeType::PowershelfGb200Liteon);
    }

    #[test]
    fn create_node_powershelf_gb300_liteon() {
        let rack = NvlGb300Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("p-01", NodeType::PowershelfGb300Liteon))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::PowershelfGb300Liteon);
        assert_eq!(node.get_info()["type"], "powershelf_gb300_liteon");
    }

    #[test]
    fn create_node_powershelf_gb200_delta() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("p-01", NodeType::PowershelfGb200Delta))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::PowershelfGb200Delta);
        assert_eq!(node.get_info()["type"], "powershelf_gb200_delta");
    }

    #[test]
    fn create_node_powershelf_gb300_delta() {
        let rack = NvlGb300Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("p-01", NodeType::PowershelfGb300Delta))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::PowershelfGb300Delta);
        assert_eq!(node.get_info()["type"], "powershelf_gb300_delta");
    }

    #[test]
    fn create_node_missing_id() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let mut config = test_config("", NodeType::ComputeGb200Nvidia);
        config.id = String::new();
        let err = rack.create_node(&config).err().unwrap();
        assert!(err.message.contains("missing node ID"));
    }

    #[test]
    fn create_node_missing_host() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let mut config = test_config("c-01", NodeType::ComputeGb200Nvidia);
        if let Some(endpoint) = config.bmc_endpoint.as_mut() {
            endpoint.endpoint.ip_address = String::new();
        }

        let err = rack.create_node(&config).err().unwrap();
        assert!(err.message.contains("missing host"));
    }

    #[test]
    fn create_node_missing_credentials() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let mut config = test_config("c-01", NodeType::ComputeGb200Nvidia);
        if let Some(endpoint) = config.bmc_endpoint.as_mut() {
            endpoint.credentials = None;
        }

        let err = rack.create_node(&config).err().unwrap();
        assert!(err.message.contains("missing credentials"));
    }

    #[test]
    fn add_and_find_node() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .unwrap();
        rack.add_node("c-01", node).unwrap();

        let found = rack.find_node("c-01");
        assert!(found.is_some());
        assert_eq!(found.unwrap().id(), "c-01");
    }

    #[test]
    fn add_duplicate_fails() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node1 = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .unwrap();
        let node2 = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .unwrap();
        rack.add_node("c-01", node1).unwrap();
        let err = rack.add_node("c-01", node2).unwrap_err();
        assert!(err.message.contains("already exists"));
    }

    #[test]
    fn add_node_empty_id_fails() -> Result<()> {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack.create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))?;

        let Err(err) = rack.add_node("", node) else {
            panic!("empty node ID was accepted");
        };

        assert!(err.message.contains("cannot be empty"));
        assert!(rack.list_nodes().is_empty());

        Ok(())
    }

    #[test]
    fn add_node_mismatched_id_fails() -> Result<()> {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack.create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))?;

        let Err(err) = rack.add_node("wrong-id", node) else {
            panic!("mismatched node ID was accepted");
        };

        assert!(err.message.contains("requested wrong-id"));
        assert!(err.message.contains("node id c-01"));
        assert!(rack.list_nodes().is_empty());

        Ok(())
    }

    #[test]
    fn remove_node_succeeds() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .unwrap();
        rack.add_node("c-01", node).unwrap();
        rack.remove_node("c-01").unwrap();
        assert!(rack.find_node("c-01").is_none());
    }

    #[test]
    fn remove_nonexistent_fails() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let err = rack.remove_node("c-01").unwrap_err();
        assert!(err.message.contains("not found"));
    }

    #[test]
    fn list_nodes_returns_all() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let c = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .unwrap();
        let s = rack
            .create_node(&test_config("s-01", NodeType::SwitchGb200Nvidia))
            .unwrap();
        rack.add_node("c-01", c).unwrap();
        rack.add_node("s-01", s).unwrap();

        let nodes = rack.list_nodes();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn get_info_shows_model_and_count() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .unwrap();
        rack.add_node("c-01", node).unwrap();

        let info = rack.get_info();
        assert_eq!(info["model"], "GB200 NVL");
        assert_eq!(info["nodeCount"], "1");
    }

    #[test]
    fn power_on_order_crud() -> Result<()> {
        let rack = NvlGb200Rack::new("rack-01".into());
        assert!(rack.get_power_on_order().is_empty());

        let p = rack.create_node(&test_config("p-01", NodeType::PowershelfGb200Liteon))?;
        let s = rack.create_node(&test_config("s-01", NodeType::SwitchGb200Nvidia))?;
        let c = rack.create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))?;
        rack.add_node("p-01", p)?;
        rack.add_node("s-01", s)?;
        rack.add_node("c-01", c)?;

        let order = vec![
            PowerOnStep::new(0, "p-01"),
            PowerOnStep::new(1, "s-01"),
            PowerOnStep::new(2, "c-01"),
        ];
        rack.set_power_on_order(order)?;
        assert_eq!(rack.get_power_on_order().len(), 3);

        Ok(())
    }

    #[test]
    fn set_power_on_order_rejects_invalid_order_without_mutation() -> Result<()> {
        let rack = NvlGb200Rack::new("rack-01".into());
        let c = rack.create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))?;
        rack.add_node("c-01", c)?;
        rack.set_power_on_order(vec![PowerOnStep::new(0, "c-01")])?;

        let invalid_orders = [
            (vec![PowerOnStep::new(0, "missing-node")], "unknown node"),
            (vec![PowerOnStep::new(0, "")], "node_id is required"),
            (
                vec![PowerOnStep::new(0, "c-01"), PowerOnStep::new(1, "c-01")],
                "duplicate node",
            ),
            (Vec::new(), "power-on order is required"),
        ];

        for (order, expected_message) in invalid_orders {
            let Err(err) = rack.set_power_on_order(order) else {
                panic!("invalid power-on order was accepted");
            };
            assert!(err.message.contains(expected_message));

            let saved = rack.get_power_on_order();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].node_id, "c-01");
        }

        Ok(())
    }

    #[test]
    fn add_node_invalidates_power_on_order() -> Result<()> {
        let rack = NvlGb200Rack::new("rack-01".into());
        let c = rack.create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))?;
        rack.add_node("c-01", c)?;

        rack.set_power_on_order(vec![PowerOnStep::new(0, "c-01")])?;
        assert_eq!(rack.get_power_on_order().len(), 1);

        let s = rack.create_node(&test_config("s-01", NodeType::SwitchGb200Nvidia))?;
        rack.add_node("s-01", s)?;
        assert!(rack.get_power_on_order().is_empty());

        Ok(())
    }

    #[test]
    fn remove_node_invalidates_power_on_order() -> Result<()> {
        let rack = NvlGb200Rack::new("rack-01".into());
        let c = rack.create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))?;
        rack.add_node("c-01", c)?;

        rack.set_power_on_order(vec![PowerOnStep::new(0, "c-01")])?;
        rack.remove_node("c-01")?;
        assert!(rack.get_power_on_order().is_empty());

        Ok(())
    }

    #[test]
    fn product_family_type_strings() {
        assert_eq!(ProductFamily::Gb200.rack_type(), "NVL_GB200");
        assert_eq!(ProductFamily::Gb300.rack_type(), "NVL_GB300");
        assert_eq!(ProductFamily::Vrnvl72.rack_type(), "NVL_VRNVL72");
    }

    #[test]
    fn gb200_rack_rejects_gb300_node_type() {
        let rack = NvlGb200Rack::new("rack-01".into());
        let err = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb300Nvidia))
            .err()
            .unwrap();

        assert!(err.message.contains("rack type NVL_GB200"));
        assert!(err.message.contains("compute_gb300_nvidia"));
    }

    #[test]
    fn gb300_rack_rejects_gb200_node_type() {
        let rack = NvlGb300Rack::new("rack-01".into());
        let err = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .err()
            .unwrap();

        assert!(err.message.contains("rack type NVL_GB300"));
        assert!(err.message.contains("compute_gb200_nvidia"));
    }

    #[test]
    fn vrnvl72_rack_rejects_gb200_node_type() {
        let rack = NvlVrnvl72Rack::new("rack-01".into());
        let err = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb200Nvidia))
            .err()
            .unwrap();

        assert!(err.message.contains("rack type NVL_VRNVL72"));
        assert!(err.message.contains("compute_gb200_nvidia"));
    }

    #[test]
    fn gb300_rack_uses_gb300_model_with_shared_node_factory() {
        let rack = NvlGb300Rack::new("rack-01".into());
        let node = rack
            .create_node(&test_config("c-01", NodeType::ComputeGb300Nvidia))
            .unwrap();

        assert_eq!(node.node_type(), NodeType::ComputeGb300Nvidia);
        assert_eq!(rack.get_info()["model"], "GB300 NVL");
    }
}
