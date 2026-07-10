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

//! In-memory implementation of [`FirmwareObjectStore`]. Mirrors the Postgres
//! impl's semantics: `set_default` is atomic (achieved by holding the
//! catalog write lock across the clear+set), `list_apply_history` joins
//! against the catalog at read time so deletions are reflected in the
//! `firmware_available` flag.

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use chrono::Utc;

use crate::persistence::firmware_object::{
    FirmwareObject, FirmwareObjectApplyHistoryRecord, FirmwareObjectSearchFilter,
    FirmwareObjectStore, RackHardwareType,
};
use crate::utilities::error::{Result, RmsError};

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
        let mut catalog = self.catalog.write().unwrap();
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
        let catalog = self.catalog.read().unwrap();
        catalog
            .get(id)
            .cloned()
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))
    }

    async fn list(&self, filter: FirmwareObjectSearchFilter) -> Result<Vec<FirmwareObject>> {
        let catalog = self.catalog.read().unwrap();
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
        let mut catalog = self.catalog.write().unwrap();
        let fw = catalog
            .get_mut(id)
            .ok_or_else(|| RmsError::not_found(format!("firmware object {id} not found")))?;
        fw.config = config;
        fw.updated = Utc::now();
        Ok(fw.clone())
    }

    async fn set_available(&self, id: &str, available: bool) -> Result<FirmwareObject> {
        let mut catalog = self.catalog.write().unwrap();
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
        let mut catalog = self.catalog.write().unwrap();
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
        let mut catalog = self.catalog.write().unwrap();
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
        let catalog = self.catalog.read().unwrap();
        Ok(catalog
            .values()
            .any(|fw| &fw.rack_hardware_type == hw && fw.is_default))
    }

    async fn find_default_by_hw_type(&self, hw: &RackHardwareType) -> Result<FirmwareObject> {
        let catalog = self.catalog.read().unwrap();
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
        let mut catalog = self.catalog.write().unwrap();
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
        let mut history = self.history.write().unwrap();
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
        let history = self.history.read().unwrap();
        let catalog = self.catalog.read().unwrap();

        let mut rows: Vec<FirmwareObjectApplyHistoryRecord> = history
            .iter()
            .filter(|h| object_id.is_none_or(|id| h.object_id == id))
            .filter(|h| rack_ids.is_empty() || rack_ids.iter().any(|r| r == &h.rack_id))
            .map(|h| {
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
