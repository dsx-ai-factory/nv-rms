-- SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
-- SPDX-License-Identifier: LicenseRef-NvidiaProprietary
--
-- NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
-- property and proprietary rights in and to this material, related
-- documentation and any modifications thereto. Any use, reproduction,
-- disclosure or distribution of this material and related documentation
-- without an express license agreement from NVIDIA CORPORATION or
-- its affiliates is strictly prohibited.
--
--
-- Catalog of managed firmware bundles plus an apply-history table.
--
-- The two tables are intentionally not joined by foreign key: history rows
-- outlive the firmware they reference (deleting a bundle keeps the audit
-- trail intact). The `firmware_available` flag returned by reads is
-- computed at query time via LEFT JOIN.

CREATE TABLE IF NOT EXISTS rack_firmware (
    id                 TEXT        PRIMARY KEY,
    rack_hardware_type TEXT        NOT NULL,
    available          BOOLEAN     NOT NULL DEFAULT false,
    is_default         BOOLEAN     NOT NULL DEFAULT false,
    config             JSONB       NOT NULL,
    parsed_components  JSONB,
    created            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS rack_firmware_hw_type_idx
    ON rack_firmware (rack_hardware_type);

-- Speeds up has_default / find_default_by_hw_type. Partial index because
-- only one row per hardware type can be default at a time.
CREATE UNIQUE INDEX IF NOT EXISTS rack_firmware_default_idx
    ON rack_firmware (rack_hardware_type)
    WHERE is_default;

CREATE TABLE IF NOT EXISTS rack_firmware_apply_history (
    id                 BIGSERIAL   PRIMARY KEY,
    object_id          TEXT        NOT NULL,
    rack_id            TEXT        NOT NULL,
    firmware_type      TEXT        NOT NULL,
    rack_hardware_type TEXT        NOT NULL,
    node_ids           TEXT[]      NOT NULL DEFAULT '{}',
    applied_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS rack_firmware_apply_history_object_id_idx
    ON rack_firmware_apply_history (object_id);

CREATE INDEX IF NOT EXISTS rack_firmware_apply_history_rack_id_idx
    ON rack_firmware_apply_history (rack_id);

CREATE INDEX IF NOT EXISTS rack_firmware_apply_history_applied_at_idx
    ON rack_firmware_apply_history (applied_at DESC);
