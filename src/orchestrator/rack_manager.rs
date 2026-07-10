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

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use crate::domain::node::ProductFamily;
use crate::racks::{
    ManagedRack,
    nvl_gb::{NvlGb200Rack, NvlGb300Rack},
};
use crate::utilities::error::{Result, RmsError};

/// Central orchestrator — the single entry point for all business logic.
///
/// Owns the rack map. Gateways traverse the hierarchy directly:
///
///   let rack = rm.find_rack(rack_id)?;
///   let node = rack.find_node(node_id)?;
///   let ps = node.get_power_state().await?;
///
/// RackManager handles rack-level CRUD. All node operations go through
/// the Rack and Node interfaces directly.
pub struct RackManager {
    racks: RwLock<HashMap<String, Arc<ManagedRack>>>,
}

impl RackManager {
    pub fn new() -> Self {
        Self {
            racks: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_rack(&self, rack_id: &str, rack_type: &str) -> Result<()> {
        if rack_id.is_empty() {
            return Err(RmsError::invalid_argument("rack ID cannot be empty"));
        }
        // Build the rack before locking so no fallible work runs under the guard.
        let rack = create_rack_by_type(rack_id, rack_type)?;
        let mut racks = self.racks.write().unwrap_or_else(PoisonError::into_inner);
        if racks.contains_key(rack_id) {
            return Err(RmsError::already_exists(format!(
                "rack {rack_id} already exists"
            )));
        }
        racks.insert(rack_id.to_owned(), rack);
        tracing::info!(rack_id, rack_type, "rack created");
        Ok(())
    }

    pub fn remove_rack(&self, rack_id: &str) -> Result<()> {
        let mut racks = self.racks.write().unwrap_or_else(PoisonError::into_inner);
        if racks.remove(rack_id).is_none() {
            return Err(RmsError::not_found(format!("rack {rack_id} not found")));
        }
        tracing::info!(rack_id, "rack removed");
        Ok(())
    }

    pub fn find_rack(&self, rack_id: &str) -> Option<Arc<ManagedRack>> {
        let racks = self.racks.read().unwrap_or_else(PoisonError::into_inner);
        racks.get(rack_id).cloned()
    }

    pub fn list_racks(&self) -> Vec<Arc<ManagedRack>> {
        let racks = self.racks.read().unwrap_or_else(PoisonError::into_inner);
        racks.values().cloned().collect()
    }

    /// List all rack IDs.
    pub fn list_rack_ids(&self) -> Vec<String> {
        let racks = self.racks.read().unwrap_or_else(PoisonError::into_inner);
        racks.keys().cloned().collect()
    }

    /// Find a rack, or build a fresh (unregistered) one if it doesn't exist yet
    /// (used by AddNode). Newly built racks are registered atomically with the
    /// first node via [`register_rack_with`](Self::register_rack_with).
    pub fn find_or_create_rack(&self, rack_id: &str, rack_type: &str) -> Result<Arc<ManagedRack>> {
        {
            let racks = self.racks.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(rack) = racks.get(rack_id) {
                ensure_rack_type_matches(rack_id, rack.as_ref(), rack_type)?;
                return Ok(rack.clone());
            }
        }
        create_rack_by_type(rack_id, rack_type)
    }

    /// Run `add` against the rack registered under `rack`'s ID, registering
    /// `rack` itself when none exists yet, atomically under the registry write
    /// lock.
    ///
    /// `add` receives the canonical rack: an already-registered instance when
    /// one exists, otherwise `rack`. `rack` is inserted into the registry only
    /// after `add` succeeds, so a failed `add` never leaves an empty rack, and
    /// concurrent creators of the same new rack converge on a single registered
    /// instance instead of mutating soon-to-be-discarded copies.
    pub fn register_rack_with<T>(
        &self,
        rack: &Arc<ManagedRack>,
        rack_type: &str,
        add: impl FnOnce(&Arc<ManagedRack>) -> Result<T>,
    ) -> Result<T> {
        let mut racks = self.racks.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = racks.get(rack.id()) {
            ensure_rack_type_matches(rack.id(), existing.as_ref(), rack_type)?;
            return add(existing);
        }

        let value = add(rack)?;
        racks.insert(rack.id().to_owned(), Arc::clone(rack));
        tracing::info!(rack_id = rack.id(), rack_type, "rack registered");
        Ok(value)
    }
}

impl Default for RackManager {
    fn default() -> Self {
        Self::new()
    }
}

fn create_rack_by_type(rack_id: &str, rack_type: &str) -> Result<Arc<ManagedRack>> {
    if rack_type == ProductFamily::Gb200.rack_type() {
        Ok(Arc::new(NvlGb200Rack::new(rack_id.to_owned())))
    } else if rack_type == ProductFamily::Gb300.rack_type() {
        Ok(Arc::new(NvlGb300Rack::new(rack_id.to_owned())))
    } else {
        Err(RmsError::invalid_argument(format!(
            "unsupported rack type '{rack_type}'"
        )))
    }
}

fn ensure_rack_type_matches(rack_id: &str, rack: &ManagedRack, requested_type: &str) -> Result<()> {
    let existing_type = rack.rack_type();
    if existing_type != requested_type {
        return Err(RmsError::invalid_argument(format!(
            "rack {rack_id} already exists as type {existing_type} but was requested as {requested_type}"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_find_rack() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();

        let rack = rm.find_rack("rack-01");
        assert!(rack.is_some());
        assert_eq!(rack.unwrap().id(), "rack-01");
    }

    #[test]
    fn create_and_find_gb300_rack() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb300.rack_type())
            .unwrap();

        let rack = rm.find_rack("rack-01");
        assert!(rack.is_some());
        let rack = rack.unwrap();
        assert_eq!(rack.id(), "rack-01");
        assert_eq!(rack.get_info()["model"], "GB300 NVL");
    }

    #[test]
    fn create_duplicate_fails() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        let err = rm
            .create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap_err();
        assert!(err.message.contains("already exists"));
    }

    #[test]
    fn create_empty_id_fails() {
        let rm = RackManager::new();
        let err = rm
            .create_rack("", ProductFamily::Gb200.rack_type())
            .unwrap_err();
        assert!(err.message.contains("cannot be empty"));
    }

    #[test]
    fn create_unsupported_type_fails() {
        let rm = RackManager::new();
        let err = rm.create_rack("rack-01", "UNKNOWN").unwrap_err();
        assert!(err.message.contains("unsupported rack type"));
    }

    #[test]
    fn remove_rack() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        rm.remove_rack("rack-01").unwrap();
        assert!(rm.find_rack("rack-01").is_none());
    }

    #[test]
    fn remove_nonexistent_fails() {
        let rm = RackManager::new();
        let err = rm.remove_rack("rack-01").unwrap_err();
        assert!(err.message.contains("not found"));
    }

    #[test]
    fn list_racks() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        rm.create_rack("rack-02", ProductFamily::Gb200.rack_type())
            .unwrap();
        assert_eq!(rm.list_racks().len(), 2);
    }

    #[test]
    fn list_rack_ids() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        rm.create_rack("rack-02", ProductFamily::Gb200.rack_type())
            .unwrap();

        let mut ids = rm.list_rack_ids();
        ids.sort();
        assert_eq!(ids, vec!["rack-01", "rack-02"]);
    }

    #[test]
    fn find_nonexistent_returns_none() {
        let rm = RackManager::new();
        assert!(rm.find_rack("rack-01").is_none());
    }

    #[test]
    fn find_or_create_creates_new() {
        let rm = RackManager::new();
        let rack = rm
            .find_or_create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        assert_eq!(rack.id(), "rack-01");
        // Newly created racks are not registered until the caller persists them.
        assert_eq!(rm.list_racks().len(), 0);

        let registered = rm
            .register_rack_with(&rack, ProductFamily::Gb200.rack_type(), |target| {
                Ok(target.clone())
            })
            .unwrap();
        assert_eq!(registered.id(), "rack-01");
        assert_eq!(rm.list_racks().len(), 1);
    }

    #[test]
    fn register_rack_with_skips_insert_on_add_failure() {
        let rm = RackManager::new();
        let rack = rm
            .find_or_create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();

        let err = rm
            .register_rack_with(&rack, ProductFamily::Gb200.rack_type(), |_| -> Result<()> {
                Err(RmsError::invalid_argument("boom"))
            })
            .err()
            .unwrap();

        assert_eq!(err.message, "boom");
        // A failed add must not persist the rack.
        assert_eq!(rm.list_racks().len(), 0);
    }

    #[test]
    fn find_or_create_returns_existing() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        let rack = rm
            .find_or_create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        assert_eq!(rack.id(), "rack-01");
        assert_eq!(rm.list_racks().len(), 1);
    }

    #[test]
    fn register_rack_keeps_existing() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        let original = rm.find_rack("rack-01").unwrap();

        let rebuilt = rm
            .find_or_create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        let registered = rm
            .register_rack_with(&rebuilt, ProductFamily::Gb200.rack_type(), |target| {
                Ok(target.clone())
            })
            .unwrap();

        assert_eq!(rm.list_racks().len(), 1);
        // The already-registered rack wins; register_rack_with adds to it.
        assert!(Arc::ptr_eq(&original, &registered));
        assert!(Arc::ptr_eq(&original, &rm.find_rack("rack-01").unwrap()));
    }

    #[test]
    fn find_or_create_rejects_existing_rack_type_mismatch() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();

        let err = rm
            .find_or_create_rack("rack-01", ProductFamily::Gb300.rack_type())
            .err()
            .unwrap();

        assert!(err.message.contains("already exists as type NVL_GB200"));
        assert!(err.message.contains("requested as NVL_GB300"));
        assert_eq!(rm.list_racks().len(), 1);
    }

    #[test]
    fn concurrent_reads() {
        let rm = Arc::new(RackManager::new());
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let rm = rm.clone();
                std::thread::spawn(move || {
                    let rack = rm.find_rack("rack-01");
                    assert!(rack.is_some());
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn default_trait() {
        let rm = RackManager::default();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        assert!(rm.find_rack("rack-01").is_some());
    }

    #[test]
    fn recovers_from_poisoned_lock() {
        let rm = Arc::new(RackManager::new());
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();

        // Poison the write lock by panicking under the guard.
        let rm_panic = rm.clone();
        let rack = rm_panic.find_rack("rack-01").unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rm_panic.register_rack_with(
                &rack,
                ProductFamily::Gb200.rack_type(),
                |_| -> Result<()> {
                    panic!("boom while holding the write lock");
                },
            )
        }));
        assert!(result.is_err());

        // Lock is poisoned, but reads and writes still succeed.
        assert!(rm.find_rack("rack-01").is_some());
        rm.create_rack("rack-02", ProductFamily::Gb200.rack_type())
            .unwrap();
        assert_eq!(rm.list_racks().len(), 2);
    }

    #[test]
    fn find_or_create_concurrent_race() {
        let rm = Arc::new(RackManager::new());

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let rm = rm.clone();
                std::thread::spawn(move || {
                    let rack = rm
                        .find_or_create_rack("rack-01", ProductFamily::Gb200.rack_type())
                        .unwrap();
                    rm.register_rack_with(&rack, ProductFamily::Gb200.rack_type(), |target| {
                        Ok(target.clone())
                    })
                    .unwrap()
                })
            })
            .collect();

        let registered: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Every concurrent creator must converge on the single registered rack,
        // so nodes added through any returned handle land in the same instance.
        assert_eq!(rm.list_racks().len(), 1);
        let canonical = rm.find_rack("rack-01").unwrap();
        for rack in &registered {
            assert!(Arc::ptr_eq(rack, &canonical));
        }
    }
}
