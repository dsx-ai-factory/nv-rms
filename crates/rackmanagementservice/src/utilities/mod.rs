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
