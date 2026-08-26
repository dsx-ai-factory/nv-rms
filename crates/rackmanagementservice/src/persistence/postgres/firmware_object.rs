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

        // Look up the target's hardware type. Locking the row keeps a
        // concurrent set_default for the same row from racing us.
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
