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

//! In-memory implementations of the persistence traits.
//!
//! Used by tests (and would be used by dev runs without a configured DB).
//! Mirrors the Postgres semantics so the same behavioral test scenarios
//! pass against either backend.

pub mod firmware_object;

pub use firmware_object::MemoryFirmwareObjectStore;
