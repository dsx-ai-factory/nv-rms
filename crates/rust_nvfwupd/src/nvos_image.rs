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

//! Helpers for treating NVOS switch images as firmware inventory entries.

use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::{json, Value};

/// AP name used for NVOS in `show_version` tables.
pub(crate) const NVOS_AP_NAME: &str = "NVOS";

/// NVUE endpoint that describes current/next NVOS system images.
pub(crate) const SYSTEM_IMAGE_URI: &str = "/nvue_v1/system/image";

/// NVUE endpoint that lists image files available on the switch.
pub(crate) const SYSTEM_IMAGE_FILES_URI: &str = "/nvue_v1/system/image/files";

/// Switch filesystem directory where NVOS images are staged.
pub(crate) const NVOS_UPLOAD_DIR: &str = "/host/nos-images/";

static NVOS_VERSION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(\d+\.\d+\.\d+(?:-\d+)?)").expect("valid regex"));
const FILE_LIST_MAX_DEPTH: usize = 32;

/// Return whether a package path looks like an NVOS switch image.
pub(crate) fn is_nvos_image_path(path: &str) -> bool {
    let Some(file_name) = Path::new(path).file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let lower = file_name.to_ascii_lowercase();
    lower.ends_with(".bin") && lower.contains("nvos")
}

/// Return a safe remote filename for an NVOS image path.
pub(crate) fn safe_image_file_name(path: &str) -> Result<String, String> {
    let file_name = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("NVOS image path {path} does not contain a valid file name"))?;

    if !is_nvos_image_path(file_name) {
        return Err(format!(
            "NVOS image {file_name} must be a .bin file whose name contains 'nvos'"
        ));
    }

    if file_name.starts_with('.')
        || file_name
            .chars()
            .any(|ch| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.'))
    {
        return Err(format!(
            "NVOS image filename {file_name} is invalid; only alphanumeric, '-', '_', and '.' are allowed"
        ));
    }

    Ok(file_name.to_string())
}

/// Derive the NVOS build-id shown in package comparisons from an image path.
pub(crate) fn package_version_from_image_path(path: &str) -> Option<String> {
    if !is_nvos_image_path(path) {
        return None;
    }

    let file_name = Path::new(path).file_name()?.to_str()?;
    if let Some(captures) = NVOS_VERSION_RE.captures(file_name) {
        return captures
            .get(1)
            .map(|version| format!("nvos-{}", version.as_str()));
    }

    Path::new(file_name)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.trim().is_empty())
        .map(str::to_string)
}

/// Return the NVOS package version represented by the provided recipes.
pub(crate) fn package_version_from_recipes(recipes: Option<&Vec<String>>) -> Option<String> {
    recipes
        .into_iter()
        .flatten()
        .find_map(|recipe| package_version_from_image_path(recipe))
}

/// Return package display names contributed by NVOS image recipes.
pub(crate) fn package_versions_from_recipes(recipes: Option<&Vec<String>>) -> Vec<String> {
    recipes
        .into_iter()
        .flatten()
        .filter_map(|recipe| package_version_from_image_path(recipe))
        .collect()
}

/// Return true when an inventory AP name is the synthetic NVOS row.
pub(crate) fn is_nvos_ap_name(ap_name: &str) -> bool {
    ap_name.trim().eq_ignore_ascii_case(NVOS_AP_NAME)
}

fn build_id_field(value: &Value) -> Option<&str> {
    match value {
        Value::String(text) => Some(text.as_str()),
        Value::Object(object) => object
            .get("build-id")
            .or_else(|| object.get("build_id"))
            .or_else(|| object.get("buildId"))
            .and_then(Value::as_str),
        _ => None,
    }
}

fn partition_build_id(image: &Value, partition: &str) -> Option<String> {
    image
        .get(partition)
        .and_then(build_id_field)
        .map(str::to_string)
}

fn selected_build_id(image: &Value, selector: &str) -> Option<String> {
    let selector = selector.trim();
    match selector {
        "1" | "partition1" => partition_build_id(image, "partition1"),
        "2" | "partition2" => partition_build_id(image, "partition2"),
        value if value.starts_with("nvos-") => Some(value.to_string()),
        value if !value.is_empty() => {
            let p1 = partition_build_id(image, "partition1");
            let p2 = partition_build_id(image, "partition2");
            p1.into_iter()
                .chain(p2)
                .find(|build_id| build_id.eq_ignore_ascii_case(value))
        }
        _ => None,
    }
}

/// Return the current NVOS build-id from `/nvue_v1/system/image`.
pub(crate) fn current_build_id(image: &Value) -> Option<String> {
    image
        .get("current")
        .and_then(Value::as_str)
        .and_then(|selector| selected_build_id(image, selector))
        .or_else(|| partition_build_id(image, "current"))
}

/// Return the next-boot NVOS build-id from `/nvue_v1/system/image`.
pub(crate) fn next_build_id(image: &Value) -> Option<String> {
    image
        .get("next")
        .and_then(Value::as_str)
        .and_then(|selector| selected_build_id(image, selector))
        .or_else(|| partition_build_id(image, "next"))
}

/// Convert `/nvue_v1/system/image` into a synthetic FirmwareInventory entry.
pub(crate) fn inventory_entry_from_system_image(image: &Value) -> Option<Value> {
    let current = current_build_id(image)?;
    let mut entry = json!({
        "Id": NVOS_AP_NAME,
        "Version": current,
        "Updateable": true,
    });

    if let Some(next) = next_build_id(image) {
        entry["NextBootVersion"] = json!(next);
    }

    Some(entry)
}

/// Return true when a nested NVUE file-list response contains `file_name`.
pub(crate) fn response_contains_file_name(value: &Value, file_name: &str) -> bool {
    response_contains_file_name_at_depth(value, file_name, 0)
}

fn response_contains_file_name_at_depth(value: &Value, file_name: &str, depth: usize) -> bool {
    if depth > FILE_LIST_MAX_DEPTH {
        return false;
    }

    match value {
        Value::String(value) => value == file_name,
        Value::Array(values) => values
            .iter()
            .any(|value| response_contains_file_name_at_depth(value, file_name, depth + 1)),
        Value::Object(values) => values.iter().any(|(key, value)| {
            key == file_name || response_contains_file_name_at_depth(value, file_name, depth + 1)
        }),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nvos_image_version_is_derived_from_common_file_names() {
        assert_eq!(
            package_version_from_image_path("/tmp/nvos-amd64-25.02.4440.bin").as_deref(),
            Some("nvos-25.02.4440")
        );
        assert_eq!(
            package_version_from_image_path("nvos-amd64-25.01.2434-013.bin").as_deref(),
            Some("nvos-25.01.2434-013")
        );
    }

    #[test]
    fn nvos_image_detection_rejects_non_nvos_bins() {
        assert!(!is_nvos_image_path("transceiver-fw.bin"));
        assert!(!is_nvos_image_path("nvos.txt"));
        assert!(is_nvos_image_path("new-nvos-image.bin"));
    }

    #[test]
    fn system_image_inventory_entry_uses_current_partition_build_id() {
        let image = json!({
            "current": "1",
            "next": "partition2",
            "partition1": {"build-id": "nvos-25.02.4440"},
            "partition2": "nvos-25.02.5555"
        });

        let entry = inventory_entry_from_system_image(&image).unwrap();

        assert_eq!(entry["Id"], NVOS_AP_NAME);
        assert_eq!(entry["Version"], "nvos-25.02.4440");
        assert_eq!(entry["NextBootVersion"], "nvos-25.02.5555");
    }

    #[test]
    fn nested_file_listing_checks_keys_and_values() {
        let listing = json!({
            "partition1": ["old.bin"],
            "partition2": {"nvos-amd64-25.02.4440.bin": {"size": 1234}}
        });

        assert!(response_contains_file_name(
            &listing,
            "nvos-amd64-25.02.4440.bin"
        ));
        assert!(response_contains_file_name(&listing, "old.bin"));
        assert!(!response_contains_file_name(&listing, "missing.bin"));
    }

    #[test]
    fn file_listing_search_is_depth_limited() {
        let image_name = "nvos-amd64-25.02.4440.bin";
        let mut within_limit = json!(image_name);
        for _ in 0..FILE_LIST_MAX_DEPTH {
            within_limit = json!({"next": within_limit});
        }

        let mut beyond_limit = json!(image_name);
        for _ in 0..=FILE_LIST_MAX_DEPTH {
            beyond_limit = json!({"next": beyond_limit});
        }

        assert!(response_contains_file_name(&within_limit, image_name));
        assert!(!response_contains_file_name(&beyond_limit, image_name));
    }
}
