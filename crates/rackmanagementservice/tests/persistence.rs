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

//! Cross-backend behavioral tests for the persistence layer.
//!
//! Each scenario is written once as `async fn<S: FirmwareObjectStore>(store: &S)`
//! and run twice: once against `MemoryFirmwareObjectStore` (always runs) and
//! once against `PostgresFirmwareObjectStore` via `#[sqlx::test]` (skipped if
//! `DATABASE_URL` is not set). The matrix lives in one place so adding a
//! new behavior automatically covers both impls.

use rackmanagementservice::persistence::firmware_object::{
    FirmwareObjectSearchFilter, FirmwareObjectStore, RackHardwareType,
};
use rackmanagementservice::persistence::memory::MemoryFirmwareObjectStore;
use rackmanagementservice::persistence::postgres::PostgresFirmwareObjectStore;
use rackmanagementservice::persistence::postgres::{connect, run_bootstrap};
use rackmanagementservice::utilities::error::ErrorCode;
use serde_json::json;

// ══════════════════════════════════════════════════════════════════════════
//  Behavioral scenarios -- generic over FirmwareObjectStore
// ══════════════════════════════════════════════════════════════════════════

async fn create_then_find_by_id_round_trip<S: FirmwareObjectStore>(store: &S) {
    let fw = store
        .create(
            "fw-001",
            RackHardwareType::any(),
            json!({"Id": "fw-001"}),
            None,
        )
        .await
        .expect("create");

    let found = store.find_by_id(&fw.id).await.expect("find");
    assert_eq!(found.id, "fw-001");
    assert_eq!(found.rack_hardware_type, RackHardwareType::any());
    assert!(!found.is_default);
    assert!(!found.available);
    assert_eq!(found.config, json!({"Id": "fw-001"}));
    assert!(found.parsed_components.is_none());
}

async fn create_duplicate_fails<S: FirmwareObjectStore>(store: &S) {
    store
        .create("fw-dup", RackHardwareType::any(), json!({}), None)
        .await
        .expect("first create");

    let err = store
        .create("fw-dup", RackHardwareType::any(), json!({}), None)
        .await
        .expect_err("duplicate must fail");
    assert_eq!(err.code, ErrorCode::AlreadyExists);
}

async fn find_by_id_missing_returns_not_found<S: FirmwareObjectStore>(store: &S) {
    let err = store
        .find_by_id("nope")
        .await
        .expect_err("missing must fail");
    assert_eq!(err.code, ErrorCode::NotFound);
}

async fn create_defaults_to_not_default<S: FirmwareObjectStore>(store: &S) {
    let fw = store
        .create(
            "fw-010",
            RackHardwareType::any(),
            json!({"Id": "fw-010"}),
            None,
        )
        .await
        .expect("create");
    assert!(!fw.is_default);
    assert!(!fw.available);
}

async fn apply_history_record_and_list<S: FirmwareObjectStore>(store: &S) {
    // Create + mark available so the firmware_available flag is true on read.
    store
        .create(
            "fw-001",
            RackHardwareType::any(),
            json!({"Id": "fw-001"}),
            None,
        )
        .await
        .unwrap();
    store.set_available("fw-001", true).await.unwrap();

    store
        .record_apply(
            "fw-001",
            "rack-a",
            "prod",
            RackHardwareType::any(),
            &["node-a".to_owned()],
        )
        .await
        .unwrap();
    // Sleep so the second record's `applied_at` is strictly later than the
    // first; otherwise both NOW()/Utc::now() values can land in the same
    // microsecond on a fast machine and the sort order below becomes
    // implementation-defined.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    store
        .record_apply(
            "fw-001",
            "rack-b",
            "dev",
            RackHardwareType::any(),
            &["node-b".to_owned(), "node-c".to_owned()],
        )
        .await
        .unwrap();

    // Newest first.
    let all = store.list_apply_history(None, &[]).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].rack_id, "rack-b");
    assert_eq!(all[1].rack_id, "rack-a");
    assert_eq!(all[0].node_ids, vec!["node-b", "node-c"]);
    assert_eq!(all[1].node_ids, vec!["node-a"]);
    assert!(all[0].firmware_available);
    assert!(all[1].firmware_available);

    // Filter by object_id.
    let filtered = store.list_apply_history(Some("fw-001"), &[]).await.unwrap();
    assert_eq!(filtered.len(), 2);

    // Filter by missing object_id.
    let empty = store
        .list_apply_history(Some("fw-ghost"), &[])
        .await
        .unwrap();
    assert!(empty.is_empty());

    // Filter by rack_id.
    let by_rack = store
        .list_apply_history(None, &["rack-a".to_owned()])
        .await
        .unwrap();
    assert_eq!(by_rack.len(), 1);
    assert_eq!(by_rack[0].rack_id, "rack-a");

    // Combined filter.
    let combined = store
        .list_apply_history(Some("fw-001"), &["rack-b".to_owned()])
        .await
        .unwrap();
    assert_eq!(combined.len(), 1);
    assert_eq!(combined[0].rack_id, "rack-b");
}

async fn apply_history_firmware_available_reflects_deletion<S: FirmwareObjectStore>(store: &S) {
    store
        .create(
            "fw-002",
            RackHardwareType::any(),
            json!({"Id": "fw-002"}),
            None,
        )
        .await
        .unwrap();
    store.set_available("fw-002", true).await.unwrap();

    store
        .record_apply(
            "fw-002",
            "rack-a",
            "prod",
            RackHardwareType::any(),
            &["node-a".to_owned()],
        )
        .await
        .unwrap();

    let before = store.list_apply_history(Some("fw-002"), &[]).await.unwrap();
    assert_eq!(before.len(), 1);
    assert!(before[0].firmware_available);

    store.delete("fw-002").await.unwrap();

    let after = store.list_apply_history(Some("fw-002"), &[]).await.unwrap();
    assert_eq!(after.len(), 1);
    assert!(!after[0].firmware_available);
}

async fn apply_history_unavailable_firmware<S: FirmwareObjectStore>(store: &S) {
    // Record history for an object_id that was never created in the catalog.
    store
        .record_apply(
            "fw-ghost",
            "rack-a",
            "prod",
            RackHardwareType::any(),
            &["node-a".to_owned()],
        )
        .await
        .unwrap();

    let history = store.list_apply_history(None, &[]).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].object_id, "fw-ghost");
    assert_eq!(history[0].node_ids, vec!["node-a"]);
    assert!(!history[0].firmware_available);
}

async fn has_default_returns_false_when_none_set<S: FirmwareObjectStore>(store: &S) {
    store
        .create(
            "fw-011",
            RackHardwareType::any(),
            json!({"Id": "fw-011"}),
            None,
        )
        .await
        .unwrap();
    assert!(!store.has_default(&RackHardwareType::any()).await.unwrap());
}

async fn set_default_and_has_default<S: FirmwareObjectStore>(store: &S) {
    store
        .create(
            "fw-012",
            RackHardwareType::any(),
            json!({"Id": "fw-012"}),
            None,
        )
        .await
        .unwrap();

    let fw = store.set_default("fw-012").await.unwrap();
    assert!(fw.is_default);
    assert!(store.has_default(&RackHardwareType::any()).await.unwrap());

    let default_fw = store
        .find_default_by_hw_type(&RackHardwareType::any())
        .await
        .unwrap();
    assert_eq!(default_fw.id, "fw-012");
    assert!(default_fw.is_default);
}

async fn set_default_clears_previous_default<S: FirmwareObjectStore>(store: &S) {
    let hw = RackHardwareType::from("test-type");

    store
        .create("fw-a", hw.clone(), json!({"Id": "fw-a"}), None)
        .await
        .unwrap();
    store
        .create("fw-b", hw.clone(), json!({"Id": "fw-b"}), None)
        .await
        .unwrap();

    store.set_default("fw-a").await.unwrap();
    let a = store.find_by_id("fw-a").await.unwrap();
    assert!(a.is_default);

    store.set_default("fw-b").await.unwrap();
    let a = store.find_by_id("fw-a").await.unwrap();
    let b = store.find_by_id("fw-b").await.unwrap();
    assert!(!a.is_default);
    assert!(b.is_default);
}

async fn set_default_does_not_affect_other_hardware_types<S: FirmwareObjectStore>(store: &S) {
    let type_a = RackHardwareType::from("type-a");
    let type_b = RackHardwareType::from("type-b");

    store
        .create("fw-x", type_a.clone(), json!({"Id": "fw-x"}), None)
        .await
        .unwrap();
    store
        .create("fw-y", type_b.clone(), json!({"Id": "fw-y"}), None)
        .await
        .unwrap();

    store.set_default("fw-x").await.unwrap();
    store.set_default("fw-y").await.unwrap();

    let x = store.find_by_id("fw-x").await.unwrap();
    let y = store.find_by_id("fw-y").await.unwrap();
    assert!(x.is_default);
    assert!(y.is_default);
}

async fn find_default_by_hw_type_not_found<S: FirmwareObjectStore>(store: &S) {
    let hw = RackHardwareType::from("type-z");
    store
        .create("fw-z", hw.clone(), json!({"Id": "fw-z"}), None)
        .await
        .unwrap();
    // fw-z exists but isn't default.
    let err = store
        .find_default_by_hw_type(&hw)
        .await
        .expect_err("no default exists");
    assert_eq!(err.code, ErrorCode::NotFound);
}

async fn list_filters_by_only_available_and_hw_type<S: FirmwareObjectStore>(store: &S) {
    let hw_a = RackHardwareType::from("hw-a");
    let hw_b = RackHardwareType::from("hw-b");

    store
        .create("fw-1", hw_a.clone(), json!({}), None)
        .await
        .unwrap();
    store.set_available("fw-1", true).await.unwrap();

    store
        .create("fw-2", hw_a.clone(), json!({}), None)
        .await
        .unwrap();
    // fw-2 left unavailable

    store
        .create("fw-3", hw_b.clone(), json!({}), None)
        .await
        .unwrap();
    store.set_available("fw-3", true).await.unwrap();

    // Unfiltered: all three.
    let all = store
        .list(FirmwareObjectSearchFilter::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    // only_available: fw-1, fw-3.
    let only_avail = store
        .list(FirmwareObjectSearchFilter {
            only_available: true,
            rack_hardware_type: None,
        })
        .await
        .unwrap();
    assert_eq!(only_avail.len(), 2);
    let ids: std::collections::HashSet<_> = only_avail.iter().map(|f| f.id.clone()).collect();
    assert!(ids.contains("fw-1"));
    assert!(ids.contains("fw-3"));

    // hw_a only: fw-1, fw-2.
    let hw_a_only = store
        .list(FirmwareObjectSearchFilter {
            only_available: false,
            rack_hardware_type: Some(hw_a.clone()),
        })
        .await
        .unwrap();
    assert_eq!(hw_a_only.len(), 2);

    // hw_a AND only_available: just fw-1.
    let combined = store
        .list(FirmwareObjectSearchFilter {
            only_available: true,
            rack_hardware_type: Some(hw_a),
        })
        .await
        .unwrap();
    assert_eq!(combined.len(), 1);
    assert_eq!(combined[0].id, "fw-1");
}

async fn update_config_bumps_updated_timestamp<S: FirmwareObjectStore>(store: &S) {
    let original = store
        .create("fw-100", RackHardwareType::any(), json!({"v": 1}), None)
        .await
        .unwrap();

    // Sleep briefly so the updated timestamp can advance past created.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let updated = store
        .update_config("fw-100", json!({"v": 2}))
        .await
        .unwrap();

    assert_eq!(updated.config, json!({"v": 2}));
    assert!(updated.updated > original.updated);
}

async fn set_available_round_trip<S: FirmwareObjectStore>(store: &S) {
    store
        .create("fw-200", RackHardwareType::any(), json!({}), None)
        .await
        .unwrap();

    let fw = store.set_available("fw-200", true).await.unwrap();
    assert!(fw.available);

    let fw = store.set_available("fw-200", false).await.unwrap();
    assert!(!fw.available);
}

async fn set_parsed_components_available_updates_lookup<S: FirmwareObjectStore>(store: &S) {
    store
        .create(
            "fw-201",
            RackHardwareType::any(),
            json!({"Id": "fw-201"}),
            None,
        )
        .await
        .unwrap();

    let lookup = json!({
        "devices": {
            "Compute Node": {
                "BMC_prod": {
                    "filename": "bmc.fwpkg",
                    "target": "",
                    "component": "BMC",
                    "bundle": "BMC",
                    "firmware_type": "prod",
                    "version": "1.0.0",
                    "subcomponents": []
                }
            }
        }
    });
    let fw = store
        .set_parsed_components_available("fw-201", lookup.clone())
        .await
        .unwrap();

    assert!(fw.available);
    assert_eq!(fw.parsed_components.as_ref(), Some(&lookup));

    let found = store.find_by_id("fw-201").await.unwrap();
    assert!(found.available);
    assert_eq!(found.parsed_components.as_ref(), Some(&lookup));
}

async fn delete_removes_row<S: FirmwareObjectStore>(store: &S) {
    store
        .create("fw-300", RackHardwareType::any(), json!({}), None)
        .await
        .unwrap();
    store.delete("fw-300").await.unwrap();

    let err = store.find_by_id("fw-300").await.expect_err("deleted");
    assert_eq!(err.code, ErrorCode::NotFound);

    // Deleting again is also NotFound.
    let err = store.delete("fw-300").await.expect_err("already gone");
    assert_eq!(err.code, ErrorCode::NotFound);
}

// ══════════════════════════════════════════════════════════════════════════
//  Memory backend wrappers (always run)
// ══════════════════════════════════════════════════════════════════════════

macro_rules! memory_test {
    ($name:ident) => {
        #[tokio::test]
        async fn $name() {
            let store = MemoryFirmwareObjectStore::new();
            super::$name(&store).await;
        }
    };
}

mod memory {
    use super::*;

    memory_test!(create_then_find_by_id_round_trip);
    memory_test!(create_duplicate_fails);
    memory_test!(find_by_id_missing_returns_not_found);
    memory_test!(create_defaults_to_not_default);
    memory_test!(apply_history_record_and_list);
    memory_test!(apply_history_firmware_available_reflects_deletion);
    memory_test!(apply_history_unavailable_firmware);
    memory_test!(has_default_returns_false_when_none_set);
    memory_test!(set_default_and_has_default);
    memory_test!(set_default_clears_previous_default);
    memory_test!(set_default_does_not_affect_other_hardware_types);
    memory_test!(find_default_by_hw_type_not_found);
    memory_test!(list_filters_by_only_available_and_hw_type);
    memory_test!(update_config_bumps_updated_timestamp);
    memory_test!(set_available_round_trip);
    memory_test!(set_parsed_components_available_updates_lookup);
    memory_test!(delete_removes_row);
}

// ══════════════════════════════════════════════════════════════════════════
//  Postgres backend wrappers (run when DATABASE_URL is set; #[sqlx::test]
//  provisions an isolated DB per test and runs the migrations)
// ══════════════════════════════════════════════════════════════════════════

macro_rules! postgres_test {
    ($name:ident) => {
        #[sqlx::test(migrations = "src/persistence/postgres/migrations")]
        async fn $name(pool: sqlx::PgPool) {
            let store = PostgresFirmwareObjectStore::new(pool);
            super::$name(&store).await;
        }
    };
}

mod postgres {
    use super::*;

    /// Exercises the `connect` and `run_migrations` helpers in
    /// `persistence::postgres`. `#[sqlx::test]` provisions its own pool
    /// and runs migrations directly, bypassing these wrappers, so this
    /// test calls them explicitly. Skips when `DATABASE_URL` is unset.
    #[tokio::test]
    async fn connect_and_run_migrations_succeed() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            return;
        };
        let pool = connect(&url, 5).await.expect("connect");
        // Idempotent -- migrations already applied are skipped.
        run_bootstrap(&pool).await.expect("run_migrations");
    }

    /// Bad URL -> `RmsError` with `Internal` code. Points at port 1 on
    /// localhost so the connect call fails fast with "connection refused"
    /// rather than waiting on DNS or a connect timeout.
    #[tokio::test]
    async fn connect_with_bad_url_returns_internal_error() {
        let err = connect("postgres://postgres:postgres@127.0.0.1:1/rms_test", 1)
            .await
            .expect_err("bad url should fail");
        assert_eq!(err.code, ErrorCode::Internal);
    }

    /// Regression: a failed `connect` must never surface the DSN password in
    /// the resulting `RmsError` message (it is logged verbatim at startup).
    /// Uses a distinctive password so any leak is unambiguous.
    #[tokio::test]
    async fn connect_error_message_does_not_leak_dsn_password() {
        const SECRET: &str = "sup3rsecretPASSWORD";
        let url = format!("postgres://rms_user:{SECRET}@127.0.0.1:1/rms_test");

        let err = connect(&url, 1).await.expect_err("bad url should fail");

        assert!(
            !err.message.contains(SECRET),
            "connect error message leaked the DSN password: {}",
            err.message
        );
    }

    postgres_test!(create_then_find_by_id_round_trip);
    postgres_test!(create_duplicate_fails);
    postgres_test!(find_by_id_missing_returns_not_found);
    postgres_test!(create_defaults_to_not_default);
    postgres_test!(apply_history_record_and_list);
    postgres_test!(apply_history_firmware_available_reflects_deletion);
    postgres_test!(apply_history_unavailable_firmware);
    postgres_test!(has_default_returns_false_when_none_set);
    postgres_test!(set_default_and_has_default);
    postgres_test!(set_default_clears_previous_default);
    postgres_test!(set_default_does_not_affect_other_hardware_types);
    postgres_test!(find_default_by_hw_type_not_found);
    postgres_test!(list_filters_by_only_available_and_hw_type);
    postgres_test!(update_config_bumps_updated_timestamp);
    postgres_test!(set_available_round_trip);
    postgres_test!(set_parsed_components_available_updates_lookup);
    postgres_test!(delete_removes_row);
}
