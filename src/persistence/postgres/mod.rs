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

//! Postgres-backed implementations of the persistence traits.
//!
//! Connection-pool construction and migrations live here. The per-domain
//! impls (e.g. [`PostgresFirmwareObjectStore`]) live in sibling modules.
//! Migrations live in the `migrations/` directory next to this file so
//! everything Postgres-specific is colocated.
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
