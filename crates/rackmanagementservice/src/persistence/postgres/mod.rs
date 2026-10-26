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

//! Postgres-backed implementations of the persistence traits.
//!
//! Connection-pool construction and migrations live here. The per-domain
//! impls (e.g. [`PostgresFirmwareObjectStore`]) live in sibling modules.
//! Migrations live in the `migrations/` directory next to this file so
//! everything Postgres-specific is colocated.
//!
//! Do NOT edit an already-applied migration file (even comments):
//! [`run_bootstrap`] runs `sqlx::migrate!`, which checksums each migration's
//! bytes and refuses to start ("migration was previously applied but has
//! been modified") when the compiled-in bytes differ from what a database
//! already recorded. Schema notes/caveats therefore belong in source docs
//! like this one, never inside the `.sql` files.
//!
//! ID-reuse caveat (audit consumers): `rack_firmware.id` is a
//! caller/config-derived string (ProductName_MilestoneName), not a
//! surrogate key, and can be reused after a delete. Apply-history
//! references the catalog only by that id string and stores no content
//! fingerprint, so history for a deleted id silently re-associates with a
//! later, different bundle that reuses the same id (and its computed
//! `firmware_available` flips back to true). Treat `object_id` as
//! non-unique across time. See [`crate::persistence::firmware_object`] for
//! the full contract.
//!
//! [`PostgresFirmwareObjectStore`]: firmware_object::PostgresFirmwareObjectStore

pub mod error;
pub mod firmware_object;

pub use error::DatabaseError;
pub use firmware_object::PostgresFirmwareObjectStore;

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::utilities::error::Result;

/// How long `connect` will wait when trying to obtain its first connection
/// before giving up. sqlx's default is 30s which is fine for a healthy DB
/// but slows test iteration to a crawl when the URL points somewhere
/// closed. 5s is plenty for any reachable database and fails fast
/// otherwise.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Build a Postgres connection pool.
///
/// `max_connections` caps the pool size; pick something proportional to
/// your concurrent workload. Returns `RmsError::Internal` (wrapping
/// `DatabaseError::Pool`) if the connection cannot be established within
/// `CONNECT_TIMEOUT`.
pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(CONNECT_TIMEOUT)
        .connect(url)
        .await
        .map_err(DatabaseError::from)?;
    Ok(pool)
}

/// Run all embedded bootstrap and migrate operations against the pool.
/// Idempotent -- operations already applied are skipped.
///
/// Bootstrap operations are embedded into the binary at compile time via
/// `sqlx::migrate!()`, so production deployments don't need the
/// `migrations/` directory present at runtime.
pub async fn run_bootstrap(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("src/persistence/postgres/migrations")
        .run(pool)
        .await
        .map_err(DatabaseError::from)?;
    Ok(())
}
