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

//! Persistence contract for firmware object catalog and apply history.
//!
//! Holds the domain types (`FirmwareObject`, `RackHardwareType`, etc.) and the
//! `FirmwareObjectStore` trait. Two implementations live alongside this file in
//! `super::memory` and `super::postgres`. RMS code never touches `sqlx`
//! directly -- it only operates through this trait.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::utilities::error::Result;

/// Identifier for a class of rack hardware (e.g. "gb200-nvl", "any").
///
/// Newtype around `String` with `sqlx::Type` derived to TEXT so it binds and
/// reads from the typed `rack_hardware_type` column directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(transparent)]
pub struct RackHardwareType(pub String);

impl RackHardwareType {
    /// Catch-all hardware type used when a firmware bundle isn't specific to
    /// a single hardware class.
    pub fn any() -> Self {
        Self("any".to_owned())
    }
}

impl std::fmt::Display for RackHardwareType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for RackHardwareType {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// A managed firmware bundle in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct FirmwareObject {
    pub id: String,
    pub rack_hardware_type: RackHardwareType,
    pub available: bool,
    pub is_default: bool,
    pub config: serde_json::Value,
    pub parsed_components: Option<serde_json::Value>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}

/// Filter for `FirmwareObjectStore::list`. All fields combine with AND.
#[derive(Debug, Clone, Default)]
pub struct FirmwareObjectSearchFilter {
    pub only_available: bool,
    pub rack_hardware_type: Option<RackHardwareType>,
}

/// One row of apply history, joined with the catalog so callers see whether
/// the firmware that was applied is still in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirmwareObjectApplyHistoryRecord {
    pub object_id: String,
    pub rack_id: String,
    pub firmware_type: String,
    pub rack_hardware_type: RackHardwareType,
    pub node_ids: Vec<String>,
    pub applied_at: DateTime<Utc>,
    /// True if `object_id` still exists in the catalog at read time.
    ///
    /// ID-reuse caveat: `object_id` is a caller/config-derived string that
    /// can be reused after a delete, and history stores no content
    /// fingerprint. A `true` here therefore means "*some* bundle with this
    /// id exists now", not necessarily the same physical bundle that was
    /// applied. Audit consumers must treat `object_id` as non-unique across
    /// time.
    pub firmware_available: bool,
}

/// Trait for persistence of firmware objects + apply history.
///
/// Method signatures take and return only domain types -- no `sqlx`,
/// `PgConnection`, or SQL strings leak through. Both the in-memory and
/// Postgres implementations honor the same semantics:
///
/// - `set_default` is atomic: it clears any previous default for the same
///   `rack_hardware_type` and marks the target row default in one logical
///   step. Postgres uses a transaction; Memory holds a write lock.
/// - `list_apply_history` joins history rows against the catalog so the
///   `firmware_available` flag reflects the catalog at read time, not at
///   record time. Deleting a firmware does not delete its history.
///
/// ID-reuse caveat (audit consumers): a firmware `id` is a caller/config
/// derived string (not a surrogate key) and can be reused after a delete.
/// Apply-history references the catalog only by that id string and records
/// no content fingerprint, so history for a deleted id silently
/// re-associates with a later, different bundle that reuses the same id.
/// Treat `object_id` as non-unique across time.
#[async_trait]
pub trait FirmwareObjectStore: Send + Sync {
    /// Insert a new firmware bundle. Defaults: `available=false`,
    /// `is_default=false`, `created`/`updated` to NOW.
    async fn create(
        &self,
        id: &str,
        hw_type: RackHardwareType,
        config: serde_json::Value,
        parsed_components: Option<serde_json::Value>,
    ) -> Result<FirmwareObject>;

    /// Look up a firmware bundle by id; `RmsError::NotFound` if missing.
    async fn find_by_id(&self, id: &str) -> Result<FirmwareObject>;

    /// List bundles matching `filter`, ordered by `created` DESC.
    async fn list(&self, filter: FirmwareObjectSearchFilter) -> Result<Vec<FirmwareObject>>;

    /// Replace the `config` JSON for a bundle and bump `updated`.
    async fn update_config(&self, id: &str, config: serde_json::Value) -> Result<FirmwareObject>;

    /// Toggle the `available` flag for a bundle.
    async fn set_available(&self, id: &str, available: bool) -> Result<FirmwareObject>;

    /// Replace parsed component metadata and mark the bundle available in one
    /// logical update after artifact download completes.
    async fn set_parsed_components_available(
        &self,
        id: &str,
        parsed_components: serde_json::Value,
    ) -> Result<FirmwareObject>;

    /// Mark `id` as the default bundle for its `rack_hardware_type`. Clears
    /// any previously-default bundle of the same hardware type. Atomic.
    async fn set_default(&self, id: &str) -> Result<FirmwareObject>;

    /// Whether any bundle is currently default for `hw`.
    async fn has_default(&self, hw: &RackHardwareType) -> Result<bool>;

    /// Find the default bundle for `hw`; `RmsError::NotFound` if none.
    async fn find_default_by_hw_type(&self, hw: &RackHardwareType) -> Result<FirmwareObject>;

    /// Remove a firmware bundle from the catalog. Apply-history rows that
    /// reference it are kept; their `firmware_available` flag will read
    /// false from then on.
    async fn delete(&self, id: &str) -> Result<()>;

    /// Append a new apply-history row. Does not require the firmware to
    /// exist in the catalog. Only `object_id` is stored to reference the
    /// applied bundle -- no content fingerprint -- so a later bundle that
    /// reuses the same id becomes indistinguishable from this one in the
    /// audit trail (see the trait-level ID-reuse caveat).
    async fn record_apply(
        &self,
        object_id: &str,
        rack_id: &str,
        firmware_type: &str,
        hw_type: RackHardwareType,
        node_ids: &[String],
    ) -> Result<()>;

    /// List apply-history rows newest-first, optionally filtered by
    /// `object_id` and/or `rack_ids`. The `firmware_available` field on
    /// each row reflects whether the firmware still exists in the catalog.
    /// Because ids are reusable (see the trait-level ID-reuse caveat),
    /// `firmware_available` and the `object_id` filter match by id string
    /// and cannot distinguish a re-created bundle from the original.
    async fn list_apply_history(
        &self,
        object_id: Option<&str>,
        rack_ids: &[String],
    ) -> Result<Vec<FirmwareObjectApplyHistoryRecord>>;

    /// Return a string representation of the store for use as a label.
    fn name(&self) -> &'static str;
}
