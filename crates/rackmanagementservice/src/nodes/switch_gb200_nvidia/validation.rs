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

//! Validation and shell quoting for NVIDIA GB200 switch operations.
//!
//! Validates user-supplied identifiers, filenames, usernames, and component
//! names, and safely quotes command arguments before transport calls.

use super::config;

pub(crate) fn shell_quote(s: &str) -> String {
    let mut quoted = String::with_capacity(s.len() + 2);
    quoted.push('\'');

    for c in s.chars() {
        if c == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(c);
        }
    }

    quoted.push('\'');
    quoted
}

pub(crate) fn is_valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

pub(super) fn is_valid_system_username(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric())
}

pub(super) fn is_valid_firmware_filename(name: &str) -> bool {
    if name.is_empty() || name.starts_with('.') {
        return false;
    }

    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

pub(super) fn is_valid_component(component: &str) -> bool {
    let lower = component.to_ascii_lowercase();
    config::VALID_COMPONENTS.iter().any(|&v| lower == v)
}

pub(super) fn is_valid_firmware_inventory_component(component: &str) -> bool {
    if is_valid_component(component) {
        return true;
    }

    let lower = component.to_ascii_lowercase();

    if lower.len() <= 4 || !lower.starts_with("cpld") {
        return false;
    }

    lower
        .strip_prefix("cpld")
        .is_some_and(|suffix| suffix.chars().all(|c| c.is_ascii_digit()))
}

/// NVUE exposes CPLD version inventory via subcomponents like CPLD1,
/// while uploaded files remain under the aggregate CPLD namespace.
pub(super) fn firmware_inventory_endpoint_component(component: &str, list_files: bool) -> String {
    let lower = component.to_ascii_lowercase();

    if !list_files && lower == "cpld" {
        return "CPLD1".to_owned();
    }

    component.to_owned()
}
