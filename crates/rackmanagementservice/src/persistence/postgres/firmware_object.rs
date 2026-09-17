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

//! Postgres implementation of [`FirmwareObjectStore`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sqlx::QueryBuilder;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{FromRow, Row};

use crate::persistence::firmware_object::{
    FirmwareObject, FirmwareObjectApplyHistoryRecord, FirmwareObjectSearchFilter,
    FirmwareObjectStore, RackHardwareType,
};
use crate::persistence::postgres::error::DatabaseError;
use crate::utilities::error::Result;

pub struct PostgresFirmwareObjectStore {
    pool: PgPool,
}

impl PostgresFirmwareObjectStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// First key of the two-int `pg_advisory_xact_lock` used to serialize default
/// firmware selection. Acts as a private namespace so the per-hardware-type
/// lock cannot collide with advisory locks taken elsewhere. Value spells
/// "RMFD" (RMS FirmwareDefault) in ASCII.
const FIRMWARE_DEFAULT_LOCK_NAMESPACE: i32 = 0x524d_4644;

/// Serialize every default-bundle change for one hardware type by taking a
/// transaction-scoped advisory lock keyed on it. Concurrent `set_default` /
/// `set_default_if_none` calls for the same hardware type then queue behind
/// one another instead of racing on the partial unique index, so the loser
/// serializes cleanly rather than failing with a duplicate-key error. The lock
/// is released automatically at COMMIT/ROLLBACK.
async fn lock_default_slot(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    hw: &RackHardwareType,
) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(FIRMWARE_DEFAULT_LOCK_NAMESPACE)
        .bind(hw)
        .execute(&mut **tx)
        .await
        .map_err(|e| DatabaseError::query("acquire default-slot advisory lock", e))?;
    Ok(())
}

/// Same as [`lock_default_slot`], but resolves the hardware type from a
/// firmware `id` inside the lock statement. This lets callers that do not
/// already hold the hardware type take the advisory lock without a separate
/// unlocked read first, while still acquiring it before touching any row (the
/// lock order set by `set_default`). A non-existent `id` makes the subquery
/// NULL; `pg_advisory_xact_lock` is STRICT and acquires no lock in that case,
/// so the caller's following row read is responsible for reporting not-found.
async fn lock_default_slot_for_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: &str,
) -> Result<()> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock($1, hashtext( \
             (SELECT rack_hardware_type FROM rack_firmware WHERE id = $2)))",
    )
    .bind(FIRMWARE_DEFAULT_LOCK_NAMESPACE)
    .bind(id)
    .execute(&mut **tx)
    .await
    .map_err(|e| DatabaseError::query("acquire default-slot advisory lock", e))?;
    Ok(())
}

// ── Apply-history row used only for the LEFT JOIN with availability ──

#[derive(Debug, Clone, FromRow)]
struct DbApplyHistory {
    #[allow(dead_code)]
    id: i64,
    object_id: String,
    rack_id: String,
    firmware_type: String,
    rack_hardware_type: RackHardwareType,
    node_ids: Vec<String>,
    applied_at: DateTime<Utc>,
}

struct DbApplyHistoryWithAvailability {
    history: DbApplyHistory,
    firmware_available: bool,
}

impl<'r> FromRow<'r, PgRow> for DbApplyHistoryWithAvailability {
    fn from_row(row: &'r PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            history: DbApplyHistory::from_row(row)?,
            firmware_available: row.try_get("firmware_available")?,
        })
    }
}

impl From<DbApplyHistoryWithAvailability> for FirmwareObjectApplyHistoryRecord {
    fn from(row: DbApplyHistoryWithAvailability) -> Self {
        Self {
            object_id: row.history.object_id,
            rack_id: row.history.rack_id,
            firmware_type: row.history.firmware_type,
            rack_hardware_type: row.history.rack_hardware_type,
            node_ids: row.history.node_ids,
            applied_at: row.history.applied_at,
            firmware_available: row.firmware_available,
        }
    }
}

#[async_trait]
impl FirmwareObjectStore for PostgresFirmwareObjectStore {
    async fn create(
        &self,
        id: &str,
        hw_type: RackHardwareType,
        config: serde_json::Value,
        parsed_components: Option<serde_json::Value>,
    ) -> Result<FirmwareObject> {
        let q = "INSERT INTO rack_firmware (id, rack_hardware_type, config, parsed_components) \
                 VALUES ($1, $2, $3::jsonb, $4::jsonb) RETURNING *";
        let row: FirmwareObject = sqlx::query_as(q)
            .bind(id)
            .bind(hw_type)
            .bind(Json(config))
            .bind(parsed_components.map(Json))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::already_exists_or_query("firmware object", id, e))?;
        Ok(row)
    }

    async fn find_by_id(&self, id: &str) -> Result<FirmwareObject> {
        let q = "SELECT * FROM rack_firmware WHERE id = $1";
        let row: FirmwareObject = sqlx::query_as(q)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::not_found_or_query("firmware object", id, e))?;
        Ok(row)
    }

    async fn list(&self, filter: FirmwareObjectSearchFilter) -> Result<Vec<FirmwareObject>> {
        // Injection-safety invariant: only ever `push()` constant SQL
        // fragments. Every caller-supplied value MUST go through
        // `push_bind`/`bind` so it is sent as a parameter, never
        // interpolated into the query text.
        let mut qb = QueryBuilder::new("SELECT * FROM rack_firmware WHERE TRUE");

        if filter.only_available {
            qb.push(" AND available = true");
        }
        if let Some(hw) = filter.rack_hardware_type {
            qb.push(" AND rack_hardware_type = ");
            qb.push_bind(hw);
        }
        qb.push(" ORDER BY created DESC");

        let rows: Vec<FirmwareObject> = qb
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::query("firmware object list", e))?;
        Ok(rows)
    }

    async fn update_config(&self, id: &str, config: serde_json::Value) -> Result<FirmwareObject> {
        let q = "UPDATE rack_firmware \
                 SET config = $2::jsonb, updated = NOW() \
                 WHERE id = $1 \
                 RETURNING *";
        let row: FirmwareObject = sqlx::query_as(q)
            .bind(id)
            .bind(Json(config))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::not_found_or_query("firmware object", id, e))?;
        Ok(row)
    }

    async fn set_available(&self, id: &str, available: bool) -> Result<FirmwareObject> {
        let q = "UPDATE rack_firmware \
                 SET available = $2, updated = NOW() \
                 WHERE id = $1 \
                 RETURNING *";
        let row: FirmwareObject = sqlx::query_as(q)
            .bind(id)
            .bind(available)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::not_found_or_query("firmware object", id, e))?;
        Ok(row)
    }

    async fn set_parsed_components_available(
        &self,
        id: &str,
        parsed_components: serde_json::Value,
    ) -> Result<FirmwareObject> {
        let q = "UPDATE rack_firmware \
                 SET parsed_components = $2::jsonb, available = true, updated = NOW() \
                 WHERE id = $1 \
                 RETURNING *";
        let row: FirmwareObject = sqlx::query_as(q)
            .bind(id)
            .bind(Json(parsed_components))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::not_found_or_query("firmware object", id, e))?;
        Ok(row)
    }

    async fn set_default(&self, id: &str) -> Result<FirmwareObject> {
        // Atomic: clear any previous default for this hardware type, then mark
        // the target row default. Wrapping both writes in a transaction makes
        // them appear as one logical step to other readers/writers.
        let mut tx = self.pool.begin().await.map_err(DatabaseError::from)?;

        // Take the per-hardware-type advisory lock BEFORE any FOR UPDATE row
        // lock. Read the hardware type first without locking so it can key the
        // lock. Locking the slot ahead of the row gives a single lock order
        // (advisory -> rows) and avoids a deadlock: otherwise one call could
        // hold the target row lock while waiting on the advisory lock, while
        // the advisory holder's "clear previous default" waits on that row.
        let (hw,): (RackHardwareType,) =
            sqlx::query_as("SELECT rack_hardware_type FROM rack_firmware WHERE id = $1")
                .bind(id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| DatabaseError::not_found_or_query("firmware object", id, e))?;
        lock_default_slot(&mut tx, &hw).await?;

        // Now that the slot is ours, lock the target row (also serializes a
        // concurrent set_default for the same id) and confirm it still exists.
        let fw: FirmwareObject =
            sqlx::query_as("SELECT * FROM rack_firmware WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| DatabaseError::not_found_or_query("firmware object", id, e))?;

        sqlx::query(
            "UPDATE rack_firmware \
             SET is_default = false, updated = NOW() \
             WHERE rack_hardware_type = $1 AND is_default = true",
        )
        .bind(&fw.rack_hardware_type)
        .execute(&mut *tx)
        .await
        .map_err(|e| DatabaseError::query("clear previous default", e))?;

        let updated: FirmwareObject = sqlx::query_as(
            "UPDATE rack_firmware \
             SET is_default = true, updated = NOW() \
             WHERE id = $1 \
             RETURNING *",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| DatabaseError::query("set default", e))?;

        tx.commit().await.map_err(DatabaseError::from)?;
        Ok(updated)
    }

    async fn set_default_if_none(&self, id: &str) -> Result<FirmwareObject> {
        let mut tx = self.pool.begin().await.map_err(DatabaseError::from)?;

        // Serialize default selection for this hardware type on the advisory
        // lock alone. That lock -- not a row lock -- is what protects the
        // partial unique index (rack_firmware_default_idx) against concurrent
        // default-setters: a `FOR UPDATE` on existing rows would not stop a
        // *new* row being inserted and made default, so the row lock adds no
        // safety here. The hardware-type read is folded into the lock
        // statement, so there is no separate unlocked query and the advisory
        // lock is still taken before any row is touched (see set_default).
        lock_default_slot_for_id(&mut tx, id).await?;

        // Plain read -- no row lock needed now that the advisory lock
        // serializes default changes for this hardware type. Supplies the
        // current row for the no-op return path and the hardware type for the
        // guard below, and reports not-found when the id does not exist.
        let fw: FirmwareObject = sqlx::query_as("SELECT * FROM rack_firmware WHERE id = $1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| DatabaseError::not_found_or_query("firmware object", id, e))?;

        // The NOT EXISTS guard makes this a no-op whenever any row of this
        // hardware type is already default (including this one), so an existing
        // operator-chosen default is never clobbered by an add.
        let updated: Option<FirmwareObject> = sqlx::query_as(
            "UPDATE rack_firmware \
             SET is_default = true, updated = NOW() \
             WHERE id = $1 \
               AND NOT EXISTS ( \
                   SELECT 1 FROM rack_firmware \
                   WHERE rack_hardware_type = $2 AND is_default = true \
               ) \
             RETURNING *",
        )
        .bind(id)
        .bind(&fw.rack_hardware_type)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| DatabaseError::query("set default if none", e))?;

        tx.commit().await.map_err(DatabaseError::from)?;

        // `None` means the guard tripped: another row of this hardware type is
        // already the default, so this call intentionally changed nothing.
        // Return the requested object's current (unchanged) state -- callers
        // want the state of `id`, and the result is idempotent when this row
        // already holds the default.
        Ok(updated.unwrap_or(fw))
    }

    async fn has_default(&self, hw: &RackHardwareType) -> Result<bool> {
        let q = "SELECT EXISTS( \
                     SELECT 1 FROM rack_firmware \
                     WHERE rack_hardware_type = $1 AND is_default = true \
                 )";
        let (exists,): (bool,) = sqlx::query_as(q)
            .bind(hw)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::query(q, e))?;
        Ok(exists)
    }

    async fn find_default_by_hw_type(&self, hw: &RackHardwareType) -> Result<FirmwareObject> {
        let q = "SELECT * FROM rack_firmware \
                 WHERE rack_hardware_type = $1 AND is_default = true \
                 ORDER BY created DESC LIMIT 1";
        let row: FirmwareObject = sqlx::query_as(q)
            .bind(hw)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                DatabaseError::not_found_or_query("default firmware object", hw.to_string(), e)
            })?;
        Ok(row)
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let q = "DELETE FROM rack_firmware WHERE id = $1 RETURNING id";
        let _: (String,) = sqlx::query_as(q)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DatabaseError::not_found_or_query("firmware object", id, e))?;
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
        let q = "INSERT INTO rack_firmware_apply_history \
                     (object_id, rack_id, firmware_type, rack_hardware_type, node_ids) \
                 VALUES ($1, $2, $3, $4, $5)";
        sqlx::query(q)
            .bind(object_id)
            .bind(rack_id)
            .bind(firmware_type)
            .bind(hw_type)
            .bind(node_ids)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::query(q, e))?;
        Ok(())
    }

    async fn list_apply_history(
        &self,
        object_id: Option<&str>,
        rack_ids: &[String],
    ) -> Result<Vec<FirmwareObjectApplyHistoryRecord>> {
        // LEFT JOIN against the firmware object table to compute firmware_available at
        // read time. History rows whose firmware was deleted come back with
        // firmware_available = false rather than vanishing.
        //
        // Injection-safety invariant: only ever `push()` constant SQL
        // fragments. Every caller-supplied value MUST go through
        // `push_bind`/`bind` so it is sent as a parameter, never
        // interpolated into the query text.
        let mut qb = QueryBuilder::new(
            "SELECT h.id, h.object_id, h.rack_id, h.firmware_type, \
                    h.rack_hardware_type, h.node_ids, h.applied_at, \
                    COALESCE(rf.available, false) AS firmware_available \
             FROM rack_firmware_apply_history h \
             LEFT JOIN rack_firmware rf ON rf.id = h.object_id \
             WHERE TRUE",
        );

        if let Some(id) = object_id {
            qb.push(" AND h.object_id = ");
            qb.push_bind(id.to_owned());
        }
        if !rack_ids.is_empty() {
            qb.push(" AND h.rack_id = ANY(");
            qb.push_bind(rack_ids.to_vec());
            qb.push(")");
        }
        qb.push(" ORDER BY h.applied_at DESC");

        let rows: Vec<DbApplyHistoryWithAvailability> = qb
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DatabaseError::query("list_apply_history", e))?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    fn name(&self) -> &'static str {
        "postgres"
    }
}
