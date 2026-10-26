-- SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
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
