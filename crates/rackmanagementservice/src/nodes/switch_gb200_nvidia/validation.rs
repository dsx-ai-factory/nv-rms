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
