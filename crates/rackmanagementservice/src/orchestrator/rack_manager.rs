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
use std::collections::hash_map::Entry;
use std::sync::{Arc, PoisonError, RwLock};

use crate::domain::node::ProductFamily;
use crate::racks::{
    ManagedRack,
    nvl_gb::{NvlGb200Rack, NvlGb300Rack, NvlVrnvl72Rack},
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
        // Validate the type before locking (the only fallible step); the rack
        // itself is constructed lazily below, only when we actually insert it.
        let kind = RackKind::parse(rack_type)?;
        let mut racks = self.racks.write().unwrap_or_else(PoisonError::into_inner);
        match racks.entry(rack_id.to_owned()) {
            Entry::Occupied(_) => Err(RmsError::already_exists(format!(
                "rack {rack_id} already exists"
            ))),
            Entry::Vacant(slot) => {
                slot.insert(kind.build(rack_id));
                tracing::info!(rack_id, rack_type, "rack created");
                Ok(())
            }
        }
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

    /// Look up (or lazily create) the rack for `rack_id` and run `add` against
    /// it, atomically under the registry write lock. Used by AddNode.
    ///
    /// `add` receives the canonical rack: the already-registered instance when
    /// one exists, otherwise a freshly built rack. A newly built rack is
    /// inserted only after `add` succeeds, so a failed `add` never leaves an
    /// empty rack behind.
    ///
    /// Unlike a build-then-register split, the rack is constructed lazily
    /// inside the write lock and only on the vacant branch, so no instance is
    /// ever built speculatively and discarded on a race -- concurrent creators
    /// of the same new `rack_id` all converge on the single instance built by
    /// the winner.
    ///
    /// # Lock sequence diagrams
    ///
    /// See `src/orchestrator/README.md` for Mermaid lock sequence diagrams
    /// covering the vacant, occupied, and failed-`add` paths and the
    /// concurrent-creator race that this method serializes.
    pub fn with_rack_for_node<T>(
        &self,
        rack_id: &str,
        rack_type: &str,
        add: impl FnOnce(&Arc<ManagedRack>) -> Result<T>,
    ) -> Result<T> {
        if rack_id.is_empty() {
            return Err(RmsError::invalid_argument("rack ID cannot be empty"));
        }
        // Validate the type before touching the map (the only fallible step
        // that does not need the lock).
        let kind = RackKind::parse(rack_type)?;
        let mut racks = self.racks.write().unwrap_or_else(PoisonError::into_inner);
        match racks.entry(rack_id.to_owned()) {
            Entry::Occupied(existing) => {
                ensure_rack_type_matches(rack_id, existing.get().as_ref(), rack_type)?;
                add(existing.get())
            }
            Entry::Vacant(slot) => {
                let rack = kind.build(rack_id);
                let value = add(&rack)?;
                slot.insert(rack);
                tracing::info!(rack_id, rack_type, "rack registered");
                Ok(value)
            }
        }
    }
}

impl Default for RackManager {
    fn default() -> Self {
        Self::new()
    }
}

/// A validated rack type.
///
/// Splitting parsing from construction is what lets the registry build racks
/// lazily under its write lock: [`RackKind::parse`] is the only fallible step
/// and runs before the lock, while [`RackKind::build`] is infallible and runs
/// only on the insert path. Because construction no longer happens
/// speculatively (no instance is ever built and then discarded on a race),
/// rack constructors are not required to be side-effect-free for correctness --
/// though keeping them to pure in-memory initialization remains good practice.
#[derive(Clone, Copy)]
enum RackKind {
    Gb200,
    Gb300,
    Vrnvl72,
}

impl RackKind {
    /// Validate a rack-type string. Cheap, allocation-free, side-effect-free.
    fn parse(rack_type: &str) -> Result<Self> {
        if rack_type == ProductFamily::Gb200.rack_type() {
            Ok(Self::Gb200)
        } else if rack_type == ProductFamily::Gb300.rack_type() {
            Ok(Self::Gb300)
        } else if rack_type == ProductFamily::Vrnvl72.rack_type() {
            Ok(Self::Vrnvl72)
        } else {
            Err(RmsError::invalid_argument(format!(
                "unsupported rack type '{rack_type}'"
            )))
        }
    }

    /// Construct the rack instance. Infallible once the type is validated.
    fn build(self, rack_id: &str) -> Arc<ManagedRack> {
        match self {
            Self::Gb200 => Arc::new(NvlGb200Rack::new(rack_id.to_owned())),
            Self::Gb300 => Arc::new(NvlGb300Rack::new(rack_id.to_owned())),
            Self::Vrnvl72 => Arc::new(NvlVrnvl72Rack::new(rack_id.to_owned())),
        }
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
    fn create_and_find_vrnvl72_rack() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Vrnvl72.rack_type())
            .unwrap();

        let rack = rm.find_rack("rack-01").unwrap();
        assert_eq!(rack.rack_type(), "NVL_VRNVL72");
        assert_eq!(rack.get_info()["model"], "VR NVL72");
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
    fn with_rack_for_node_creates_and_registers() {
        let rm = RackManager::new();
        let registered = rm
            .with_rack_for_node("rack-01", ProductFamily::Gb200.rack_type(), |target| {
                Ok(target.clone())
            })
            .unwrap();
        assert_eq!(registered.id(), "rack-01");
        // A successful add registers the freshly built rack.
        assert_eq!(rm.list_racks().len(), 1);
        assert!(Arc::ptr_eq(&registered, &rm.find_rack("rack-01").unwrap()));
    }

    #[test]
    fn with_rack_for_node_skips_insert_on_add_failure() {
        let rm = RackManager::new();
        let err = rm
            .with_rack_for_node(
                "rack-01",
                ProductFamily::Gb200.rack_type(),
                |_| -> Result<()> { Err(RmsError::invalid_argument("boom")) },
            )
            .err()
            .unwrap();

        assert_eq!(err.message, "boom");
        // A failed add must not persist the rack.
        assert_eq!(rm.list_racks().len(), 0);
    }

    #[test]
    fn with_rack_for_node_rejects_empty_id() {
        let rm = RackManager::new();
        let err = rm
            .with_rack_for_node("", ProductFamily::Gb200.rack_type(), |target| {
                Ok(target.clone())
            })
            .err()
            .unwrap();
        assert!(err.message.contains("cannot be empty"));
        assert_eq!(rm.list_racks().len(), 0);
    }

    #[test]
    fn with_rack_for_node_rejects_unsupported_type() {
        let rm = RackManager::new();
        let err = rm
            .with_rack_for_node("rack-01", "UNKNOWN", |target| Ok(target.clone()))
            .err()
            .unwrap();
        assert!(err.message.contains("unsupported rack type"));
        assert_eq!(rm.list_racks().len(), 0);
    }

    #[test]
    fn with_rack_for_node_uses_existing() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();
        let original = rm.find_rack("rack-01").unwrap();

        // The already-registered rack is reused, not rebuilt.
        let used = rm
            .with_rack_for_node("rack-01", ProductFamily::Gb200.rack_type(), |target| {
                Ok(target.clone())
            })
            .unwrap();

        assert_eq!(rm.list_racks().len(), 1);
        assert!(Arc::ptr_eq(&original, &used));
        assert!(Arc::ptr_eq(&original, &rm.find_rack("rack-01").unwrap()));
    }

    #[test]
    fn with_rack_for_node_rejects_existing_rack_type_mismatch() {
        let rm = RackManager::new();
        rm.create_rack("rack-01", ProductFamily::Gb200.rack_type())
            .unwrap();

        let err = rm
            .with_rack_for_node("rack-01", ProductFamily::Gb300.rack_type(), |target| {
                Ok(target.clone())
            })
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
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rm_panic.with_rack_for_node(
                "rack-01",
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
    fn with_rack_for_node_concurrent_race() {
        let rm = Arc::new(RackManager::new());

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let rm = rm.clone();
                std::thread::spawn(move || {
                    rm.with_rack_for_node("rack-01", ProductFamily::Gb200.rack_type(), |target| {
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
