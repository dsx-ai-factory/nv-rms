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

//! Persistence layer.
//!
//! One trait per domain (`FirmwareObjectStore`, future `InventoryStore`, ...).
//! Two backend implementations live under [`memory`] and [`postgres`].
//!
//! Callers depend only on the trait objects bundled in [`Backends`]; they
//! never see `sqlx`, `PgPool`, or SQL. To swap backends, build a different
//! `Backends` at startup -- nothing else changes.

pub mod firmware_object;
pub mod memory;
pub mod postgres;

pub use firmware_object::{
    FirmwareObject, FirmwareObjectApplyHistoryRecord, FirmwareObjectSearchFilter,
    FirmwareObjectStore, RackHardwareType,
};

use std::sync::Arc;

/// Aggregate of every persistence trait the service depends on.
///
/// Adding a new persistence domain means adding a new field; call sites
/// take a single `&Backends` instead of N trait-object parameters.
///
/// Tests and dev runs without a configured database use [`Backends::memory`]
/// to build an all-in-memory variant. Production wiring (in `main.rs`)
/// builds the Postgres variant from a shared `PgPool`.
#[derive(Clone)]
pub struct Backends {
    pub firmware_objects: Arc<dyn FirmwareObjectStore>,
    // Future:
    // pub inventory: Arc<dyn InventoryStore>,
}

impl Backends {
    /// Build a `Backends` whose every store is the in-memory implementation.
    /// Used by tests, benchmarks, and dev runs without a configured
    /// `DATABASE_URL`. Adding a new store means adding it here too.
    pub fn memory() -> Self {
        Self {
            firmware_objects: Arc::new(memory::MemoryFirmwareObjectStore::new()),
        }
    }
}
