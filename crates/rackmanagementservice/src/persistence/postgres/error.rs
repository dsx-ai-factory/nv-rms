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

//! Postgres-specific errors and their mapping into the service-wide
//! `RmsError`. Used only inside `super` -- callers see `RmsError`.

use crate::utilities::error::{ErrorCode, RmsError};

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("connection pool error: {source}")]
    Pool {
        #[from]
        source: sqlx::Error,
    },

    #[error("migration error: {source}")]
    Migrate {
        #[from]
        source: sqlx::migrate::MigrateError,
    },

    #[error("query failed ({sql}): {source}")]
    Query { sql: String, source: sqlx::Error },

    #[error("{kind} not found: {id}")]
    NotFound { kind: &'static str, id: String },

    #[error("{kind} already exists: {id}")]
    AlreadyExists { kind: &'static str, id: String },
}

impl DatabaseError {
    /// Build a `Query` variant from a SQL string and the underlying
    /// `sqlx::Error`. Stored as the SQL text so logs make it clear what
    /// failed without leaking bound parameters.
    pub fn query(sql: impl Into<String>, source: sqlx::Error) -> Self {
        Self::Query {
            sql: sql.into(),
            source,
        }
    }

    /// Map a `fetch_one` / `fetch_optional` failure: `RowNotFound` becomes
    /// `NotFound`, anything else becomes a `Query` error.
    pub fn not_found_or_query(
        kind: &'static str,
        id: impl Into<String>,
        source: sqlx::Error,
    ) -> Self {
        match source {
            sqlx::Error::RowNotFound => Self::NotFound {
                kind,
                id: id.into(),
            },
            other => Self::Query {
                sql: kind.to_owned(),
                source: other,
            },
        }
    }

    /// Map a unique-constraint violation to `AlreadyExists`. Postgres uses
    /// SQLSTATE `23505` for unique violations.
    pub fn already_exists_or_query(
        kind: &'static str,
        id: impl Into<String>,
        source: sqlx::Error,
    ) -> Self {
        if matches!(
            source.as_database_error().and_then(|e| e.code()),
            Some(code) if code == "23505"
        ) {
            Self::AlreadyExists {
                kind,
                id: id.into(),
            }
        } else {
            Self::Query {
                sql: kind.to_owned(),
                source,
            }
        }
    }
}

impl From<DatabaseError> for RmsError {
    fn from(err: DatabaseError) -> Self {
        let code = match &err {
            DatabaseError::NotFound { .. } => ErrorCode::NotFound,
            DatabaseError::AlreadyExists { .. } => ErrorCode::AlreadyExists,
            DatabaseError::Pool { .. }
            | DatabaseError::Migrate { .. }
            | DatabaseError::Query { .. } => ErrorCode::Internal,
        };
        RmsError::new(code, err.to_string())
    }
}

#[cfg(test)]
mod tests {
    //! Most error mapping is covered by integration tests in
    //! `tests/persistence.rs`:
    //!
    //! - `NotFound` mapping (and the `not_found_or_query` IF branch) is
    //!   exercised by `find_by_id_missing_returns_not_found`.
    //! - `AlreadyExists` mapping (and the `already_exists_or_query` IF
    //!   branch) is exercised by `create_duplicate_fails`.
    //! - `Pool` mapping is exercised by `connect_with_bad_url_returns_internal_error`.
    //!
    //! The unit tests below cover only the branches that integration tests
    //! can't easily trigger.
    use super::*;

    #[test]
    fn query_failure_maps_to_internal() {
        // Only test that exercises the `Query` variant + the
        // `DatabaseError::query()` constructor. Integration tests can't
        // easily produce a Query error against a healthy database.
        let db_err = DatabaseError::query("SELECT 1", sqlx::Error::RowNotFound);
        let rms: RmsError = db_err.into();
        assert_eq!(rms.code, ErrorCode::Internal);
        assert!(rms.message.contains("SELECT 1"));
    }

    #[test]
    fn not_found_or_query_passes_through_other_errors() {
        // ELSE branch: a non-RowNotFound error must surface as Query, not
        // NotFound. Integration tests can't trigger this without injecting
        // a fault into a healthy connection.
        let other = sqlx::Error::Protocol("oops".to_owned());
        let db_err = DatabaseError::not_found_or_query("firmware object", "fw-001", other);
        assert!(matches!(db_err, DatabaseError::Query { .. }));
    }

    #[test]
    fn already_exists_or_query_passes_through_non_unique_violation() {
        // ELSE branch: anything that isn't a unique-violation must
        // surface as Query, not AlreadyExists. Integration tests only
        // ever trigger the IF branch (real unique-violation).
        let other = sqlx::Error::Protocol("not a unique violation".to_owned());
        let db_err = DatabaseError::already_exists_or_query("firmware object", "fw-001", other);
        assert!(matches!(db_err, DatabaseError::Query { .. }));
    }
}
