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

pub mod error;
pub mod url;

/// Insert a field into a JSON object value.
///
/// Non-object values are left unchanged; callers use this for values they
/// construct as objects before adding optional fields.
pub(crate) fn insert_json_field(
    output: &mut serde_json::Value,
    key: &str,
    value: serde_json::Value,
) {
    if let Some(fields) = output.as_object_mut() {
        fields.insert(key.to_string(), value);
    }
}
