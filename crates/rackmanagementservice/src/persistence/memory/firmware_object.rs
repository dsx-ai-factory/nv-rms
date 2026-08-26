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

//! In-memory implementation of [`FirmwareObjectStore`]. Mirrors the Postgres
//! impl's semantics: `set_default` is atomic (achieved by holding the
//! catalog write lock across the clear+set), `list_apply_history` joins
//! against the catalog at read time so deletions are reflected in the
//! `firmware_available` flag.

use std::collections::HashMap;
use std::sync::{LockResult, RwLock};

use async_trait::async_trait;
use chrono::Utc;

use crate::persistence::firmware_object::{
    FirmwareObject, FirmwareObjectApplyHistoryRecord, FirmwareObjectSearchFilter,
    FirmwareObjectStore, RackHardwareType,
};
use crate::utilities::error::{Result, RmsError};

/// Recover the guard from a possibly-poisoned lock instead of panicking.
///
/// Every critical section in this store only mutates an in-memory
/// `HashMap`/`Vec` and cannot itself panic, so a poisoned lock means some
/// *other* thread panicked while holding the guard -- not that the
/// protected data was left inconsistent. Propagating that as a fresh panic
/// on every subsequent `.unwrap()` would cascade one unrelated failure into
/// aborting all later firmware requests until restart. Taking the guard via
/// `into_inner` keeps the store usable. Mirrors the poison-recovery pattern
/// already used for the mutexes in the gRPC handlers and `rack_manager`.
fn recover<G>(result: LockResult<G>) -> G {
    result.unwrap_or_else(|poisoned| {
        tracing::warn!("memory firmware object store lock poisoned; recovering guard");
        poisoned.into_inner()
    })
}

/// Stored apply-history record without the derived `firmware_available`
/// flag. The flag is computed at read time by looking the firmware up in
/// the catalog (mirrors the Postgres LEFT JOIN).
#[derive(Debug, Clone)]
struct StoredApply {
    object_id: String,
    rack_id: String,
    firmware_type: String,
    rack_hardware_type: RackHardwareType,
    node_ids: Vec<String>,
    applied_at: chrono::DateTime<Utc>,
}

#[derive(Default)]
pub struct MemoryFirmwareObjectStore {
    catalog: RwLock<HashMap<String, FirmwareObject>>,
    history: RwLock<Vec<StoredApply>>,
}

impl MemoryFirmwareObjectStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FirmwareObjectStore for MemoryFirmwareObjectStore {
    async fn create(
        &self,
        id: &str,
        hw_type: RackHardwareType,
        config: serde_json::Value,
        parsed_components: Option<serde_json::Value>,
    ) -> Result<FirmwareObject> {
        let mut catalog = recover(self.catalog.write());
        if catalog.contains_key(id) {
            return Err(RmsError::already_exists(format!(
                "firmware object {id} already exists"
            )));
        }
        let now = Utc::now();
        let fw = FirmwareObject {
            id: id.to_owned(),
            rack_hardware_type: hw_type,
            available: false,
            is_default: false,
            config,
            parsed_components,
            created: now,
            updated: now,
        };
        catalog.insert(id.to_owned(), fw.clone());
        Ok(fw)
    }

    async fn find_by_id(&self, id: &str) -> Result<FirmwareObject> {
        let catalog = recover(self.catalog.read());
        catalog
            .get(id)
            .cloned()
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))
    }

    async fn list(&self, filter: FirmwareObjectSearchFilter) -> Result<Vec<FirmwareObject>> {
        let catalog = recover(self.catalog.read());
        let mut rows: Vec<FirmwareObject> = catalog
            .values()
            .filter(|fw| !filter.only_available || fw.available)
            .filter(|fw| {
                filter
                    .rack_hardware_type
                    .as_ref()
                    .is_none_or(|hw| &fw.rack_hardware_type == hw)
            })
            .cloned()
            .collect();
        // Newest first, matching Postgres `ORDER BY created DESC`.
        rows.sort_by_key(|fw| std::cmp::Reverse(fw.created));
        Ok(rows)
    }

    async fn update_config(&self, id: &str, config: serde_json::Value) -> Result<FirmwareObject> {
        let mut catalog = recover(self.catalog.write());
        let fw = catalog
            .get_mut(id)
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))?;
        fw.config = config;
        fw.updated = Utc::now();
        Ok(fw.clone())
    }

    async fn set_available(&self, id: &str, available: bool) -> Result<FirmwareObject> {
        let mut catalog = recover(self.catalog.write());
        let fw = catalog
            .get_mut(id)
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))?;
        fw.available = available;
        fw.updated = Utc::now();
        Ok(fw.clone())
    }

    async fn set_parsed_components_available(
        &self,
        id: &str,
        parsed_components: serde_json::Value,
    ) -> Result<FirmwareObject> {
        let mut catalog = recover(self.catalog.write());
        let fw = catalog
            .get_mut(id)
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))?;
        fw.parsed_components = Some(parsed_components);
        fw.available = true;
        fw.updated = Utc::now();
        Ok(fw.clone())
    }

    async fn set_default(&self, id: &str) -> Result<FirmwareObject> {
        // Hold the write lock across the entire clear+set sequence -- this
        // is what gives us the same atomicity as the Postgres transaction.
        let mut catalog = recover(self.catalog.write());
        let target_hw = catalog
            .get(id)
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))?
            .rack_hardware_type
            .clone();

        let now = Utc::now();
        for fw in catalog.values_mut() {
            if fw.id != id && fw.rack_hardware_type == target_hw && fw.is_default {
                fw.is_default = false;
                fw.updated = now;
            }
        }
        // The entry was confirmed to exist above and we hold the only write
        // lock, so this lookup should succeed; surface a NotFound if the
        // invariant is ever broken.
        let fw = catalog
            .get_mut(id)
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))?;
        fw.is_default = true;
        fw.updated = now;
        Ok(fw.clone())
    }

    async fn has_default(&self, hw: &RackHardwareType) -> Result<bool> {
        let catalog = recover(self.catalog.read());
        Ok(catalog
            .values()
            .any(|fw| &fw.rack_hardware_type == hw && fw.is_default))
    }

    async fn find_default_by_hw_type(&self, hw: &RackHardwareType) -> Result<FirmwareObject> {
        let catalog = recover(self.catalog.read());
        // Match Postgres's ORDER BY created DESC LIMIT 1 -- if multiple
        // rows are somehow default for the same hw type, pick newest.
        catalog
            .values()
            .filter(|fw| &fw.rack_hardware_type == hw && fw.is_default)
            .max_by_key(|fw| fw.created)
            .cloned()
            .ok_or_else(|| {
                RmsError::not_found(format!("default firmware object for {hw} not found"))
            })
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let mut catalog = recover(self.catalog.write());
        catalog
            .remove(id)
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))?;
        Ok(())
    }

    async fn record_apply(
        &self,
        object_id: &str,
        rack_id: &str,
        firmware_type: &str,
        hw_type: RackHardwareType,
        node_ids: &[String],
    ) -> Result<()> {
        let mut history = recover(self.history.write());
        history.push(StoredApply {
            object_id: object_id.to_owned(),
            rack_id: rack_id.to_owned(),
            firmware_type: firmware_type.to_owned(),
            rack_hardware_type: hw_type,
            node_ids: node_ids.to_vec(),
            applied_at: Utc::now(),
        });
        Ok(())
    }

    async fn list_apply_history(
        &self,
        object_id: Option<&str>,
        rack_ids: &[String],
    ) -> Result<Vec<FirmwareObjectApplyHistoryRecord>> {
        let history = recover(self.history.read());
        let catalog = recover(self.catalog.read());

        let mut rows: Vec<FirmwareObjectApplyHistoryRecord> = history
            .iter()
            .filter(|h| object_id.is_none_or(|id| h.object_id == id))
            .filter(|h| rack_ids.is_empty() || rack_ids.iter().any(|r| r == &h.rack_id))
            .map(|h| {
                // Matches by id string, mirroring the Postgres LEFT JOIN. Ids
                // are reusable after delete (see the `FirmwareObjectStore`
                // ID-reuse caveat), so this cannot tell a re-created bundle
                // apart from the original.
                let firmware_available = catalog
                    .get(&h.object_id)
                    .map(|fw| fw.available)
                    .unwrap_or(false);
                FirmwareObjectApplyHistoryRecord {
                    object_id: h.object_id.clone(),
                    rack_id: h.rack_id.clone(),
                    firmware_type: h.firmware_type.clone(),
                    rack_hardware_type: h.rack_hardware_type.clone(),
                    node_ids: h.node_ids.clone(),
                    applied_at: h.applied_at,
                    firmware_available,
                }
            })
            .collect();
        rows.sort_by_key(|r| std::cmp::Reverse(r.applied_at));
        Ok(rows)
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;

    /// A thread panicking while holding a write guard poisons the lock. The
    /// store must keep serving reads and writes afterwards (via `recover`)
    /// rather than cascading a panic into every later firmware operation.
    #[tokio::test]
    async fn recovers_from_poisoned_lock() {
        let store = Arc::new(MemoryFirmwareObjectStore::new());

        let poisoner = Arc::clone(&store);
        let handle = std::thread::spawn(move || {
            let _guard = poisoner.catalog.write().unwrap();
            panic!("intentional panic to poison the catalog lock");
        });
        assert!(
            handle.join().is_err(),
            "the poisoning thread should have panicked"
        );
        assert!(
            store.catalog.is_poisoned(),
            "the catalog lock should be poisoned after the panic"
        );

        // Writes still succeed despite the poisoned lock.
        let created = store
            .create("fw-1", RackHardwareType::any(), json!({"Id": "fw-1"}), None)
            .await
            .expect("create should succeed on a poisoned-but-recovered lock");
        assert_eq!(created.id, "fw-1");

        // Reads still succeed too.
        let found = store
            .find_by_id("fw-1")
            .await
            .expect("find_by_id should succeed on a poisoned-but-recovered lock");
        assert_eq!(found.id, "fw-1");
    }

    /// Companion to `recovers_from_poisoned_lock` covering the history lock:
    /// `record_apply` (history only) and `list_apply_history` (which acquires
    /// both the history and catalog locks) must keep working after a panic
    /// poisons the history guard.
    #[tokio::test]
    async fn recovers_from_poisoned_history_lock() {
        let store = Arc::new(MemoryFirmwareObjectStore::new());

        let poisoner = Arc::clone(&store);
        let handle = std::thread::spawn(move || {
            let _guard = poisoner.history.write().unwrap();
            panic!("intentional panic to poison the history lock");
        });
        assert!(
            handle.join().is_err(),
            "the poisoning thread should have panicked"
        );
        assert!(
            store.history.is_poisoned(),
            "the history lock should be poisoned after the panic"
        );

        // Writing history still succeeds despite the poisoned lock.
        store
            .record_apply(
                "fw-1",
                "rack-1",
                "bmc",
                RackHardwareType::any(),
                &["node-1".to_owned()],
            )
            .await
            .expect("record_apply should succeed on a poisoned-but-recovered lock");

        // Reading history (which also acquires the catalog lock) still works.
        let history = store
            .list_apply_history(None, &[])
            .await
            .expect("list_apply_history should succeed on a poisoned-but-recovered lock");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].object_id, "fw-1");
        assert_eq!(history[0].rack_id, "rack-1");
    }
}
