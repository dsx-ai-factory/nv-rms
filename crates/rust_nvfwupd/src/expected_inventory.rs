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

//! Expected FirmwareInventory helpers shared by the CLI and workflow API.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

/// Parse a JSON value into a caller-provided expected AP inventory list.
///
/// The preferred shape is a JSON array of strings. For convenience this also
/// accepts an object with one of the common list keys used by orchestration
/// systems.
pub fn parse_expected_inventory_value(value: &Value) -> Result<Vec<String>, String> {
    let list = if let Some(array) = value.as_array() {
        array
    } else if let Some(object) = value.as_object() {
        [
            "expected_inventory",
            "expectedInventory",
            "ap_names",
            "apNames",
            "APNames",
        ]
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_array))
        .ok_or_else(|| {
            "expected inventory JSON object must contain an AP-name array field".to_string()
        })?
    } else {
        return Err("expected inventory JSON must be an array of AP names".to_string());
    };

    let mut expected = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, item) in list.iter().enumerate() {
        let Some(ap_name) = item.as_str() else {
            return Err(format!(
                "expected inventory entry at index {index} must be a string"
            ));
        };
        let ap_name = ap_name.trim();
        let normalized = normalize_ap_name(ap_name);
        if !normalized.is_empty() && seen.insert(normalized) {
            expected.push(ap_name.to_string());
        }
    }

    if expected.is_empty() {
        return Err("expected inventory must contain at least one non-empty AP name".to_string());
    }

    Ok(expected)
}

/// Read and parse an expected-inventory JSON file.
pub async fn read_expected_inventory_file(path: &str) -> Result<Vec<String>, String> {
    let body = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| format!("Unable to read expected inventory file {path}: {e}"))?;
    let value = serde_json::from_str::<Value>(&body)
        .map_err(|e| format!("Unable to parse expected inventory file {path}: {e}"))?;
    parse_expected_inventory_value(&value)
}

/// Return AP names from `expected` that are absent from `inventory`.
///
/// Matching is case-insensitive and compares against inventory map keys, URI
/// leaves, `Id`, `Name`, and `@odata.id` URI leaves.
pub fn missing_expected_ap_names(
    expected: &[String],
    inventory: &Map<String, Value>,
) -> Vec<String> {
    let present = normalized_inventory_aliases(inventory);
    expected
        .iter()
        .filter(|ap_name| !present.contains(&normalize_ap_name(ap_name)))
        .cloned()
        .collect()
}

/// Return AP names from `expected` that are absent from collection AP names.
pub fn missing_expected_ap_names_from_present(
    expected: &[String],
    present_ap_names: &[String],
) -> Vec<String> {
    let present = present_ap_names
        .iter()
        .map(|ap_name| normalize_ap_name(ap_name))
        .filter(|ap_name| !ap_name.is_empty())
        .collect::<BTreeSet<_>>();

    expected
        .iter()
        .filter(|ap_name| !present.contains(&normalize_ap_name(ap_name)))
        .cloned()
        .collect()
}

/// Return AP names advertised by a FirmwareInventory collection response.
pub fn present_ap_names_from_collection(collection: &Value) -> Vec<String> {
    let mut names = BTreeMap::new();

    if let Some(members) = collection.get("Members").and_then(Value::as_array) {
        for member in members {
            if let Some(odata_id) = string_field(member, "@odata.id") {
                insert_display_ap_name(&mut names, path_leaf(odata_id));
            }
        }
    }

    for firmware_device in collection
        .get("Oem")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|oem| oem.values())
        .filter_map(Value::as_object)
        .filter_map(|vendor_data| vendor_data.get("FirmwareInventory"))
        .filter_map(Value::as_array)
        .flatten()
    {
        if let Some(data_source_uri) = string_field(firmware_device, "DataSourceUri") {
            insert_display_ap_name(&mut names, path_leaf(data_source_uri));
        }
        if let Some(name) = string_field(firmware_device, "Name") {
            insert_display_ap_name(&mut names, name);
        }
    }

    names.into_values().collect()
}

/// Return a deterministic, human-readable list of AP names present in inventory.
pub fn present_ap_names(inventory: &Map<String, Value>) -> Vec<String> {
    let mut names = BTreeMap::new();
    for (path, details) in inventory {
        insert_display_ap_name(&mut names, path_leaf(path));
        if let Some(id) = string_field(details, "Id") {
            insert_display_ap_name(&mut names, id);
        }
        if let Some(odata_id) = string_field(details, "@odata.id") {
            insert_display_ap_name(&mut names, path_leaf(odata_id));
        }
    }
    names.into_values().collect()
}

/// Build a concise validation error for missing expected inventory entries.
pub fn missing_expected_inventory_message(
    phase: &str,
    missing: &[String],
    present: &[String],
) -> String {
    let present_text = if present.is_empty() {
        "none".to_string()
    } else {
        present.join(", ")
    };
    format!(
        "{phase}: expected firmware inventory APs were not present: {}. Present APs: {present_text}",
        missing.join(", ")
    )
}

fn normalized_inventory_aliases(inventory: &Map<String, Value>) -> BTreeSet<String> {
    let mut aliases = BTreeSet::new();
    for (path, details) in inventory {
        insert_aliases(&mut aliases, path);
        if let Some(id) = string_field(details, "Id") {
            insert_aliases(&mut aliases, id);
        }
        if let Some(name) = string_field(details, "Name") {
            insert_aliases(&mut aliases, name);
        }
        if let Some(odata_id) = string_field(details, "@odata.id") {
            insert_aliases(&mut aliases, odata_id);
        }
    }
    aliases
}

fn insert_aliases(aliases: &mut BTreeSet<String>, raw: &str) {
    let normalized = normalize_ap_name(raw);
    if !normalized.is_empty() {
        aliases.insert(normalized);
    }

    let leaf = normalize_ap_name(path_leaf(raw));
    if !leaf.is_empty() {
        aliases.insert(leaf);
    }
}

fn insert_display_ap_name(aliases: &mut BTreeMap<String, String>, raw: &str) {
    let trimmed = raw.trim().trim_end_matches('/');
    let normalized = normalize_ap_name(trimmed);
    if !normalized.is_empty() {
        aliases
            .entry(normalized)
            .or_insert_with(|| trimmed.to_string());
    }
}

fn normalize_ap_name(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_ascii_lowercase()
}

fn path_leaf(raw: &str) -> &str {
    raw.trim()
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(raw)
}

fn string_field<'a>(details: &'a Value, field: &str) -> Option<&'a str> {
    details.get(field).and_then(Value::as_str).map(str::trim)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_array_and_known_object_shapes() {
        assert_eq!(
            parse_expected_inventory_value(&json!(["FW_BMC_0", "hgx_fw_gpu_0"])).unwrap(),
            vec!["FW_BMC_0", "hgx_fw_gpu_0"]
        );
        assert_eq!(
            parse_expected_inventory_value(&json!({"expected_inventory": ["FW_BMC_0"]})).unwrap(),
            vec!["FW_BMC_0"]
        );
        assert_eq!(
            parse_expected_inventory_value(&json!({"apNames": ["FW_ERoT_BMC_0"]})).unwrap(),
            vec!["FW_ERoT_BMC_0"]
        );
    }

    #[test]
    fn rejects_non_string_entries() {
        let err = parse_expected_inventory_value(&json!(["FW_BMC_0", 7])).unwrap_err();
        assert!(err.contains("index 1"));
    }

    #[test]
    fn rejects_blank_only_entries() {
        let err = parse_expected_inventory_value(&json!(["", "   "])).unwrap_err();
        assert!(err.contains("at least one non-empty AP name"));
    }

    #[test]
    fn deduplicates_case_insensitively_while_preserving_first_spelling() {
        assert_eq!(
            parse_expected_inventory_value(&json!(["FW_BMC_0", "fw_bmc_0", "HGX_FW_GPU_0"]))
                .unwrap(),
            vec!["FW_BMC_0", "HGX_FW_GPU_0"]
        );
    }

    #[test]
    fn matches_inventory_case_insensitively_against_paths_and_ids() {
        let inventory = Map::from_iter([
            (
                "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0".to_string(),
                json!({"Id": "FW_BMC_0", "Name": "Software Inventory"}),
            ),
            (
                "HGX_FW_GPU_0".to_string(),
                json!({"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"}),
            ),
        ]);

        let missing = missing_expected_ap_names(
            &[
                "fw_bmc_0".to_string(),
                "HGX_FW_GPU_0".to_string(),
                "FW_ERoT_BMC_0".to_string(),
            ],
            &inventory,
        );

        assert_eq!(missing, vec!["FW_ERoT_BMC_0"]);
    }

    #[test]
    fn matches_present_ap_names_case_insensitively() {
        let missing = missing_expected_ap_names_from_present(
            &[
                "fw_bmc_0".to_string(),
                "HGX_FW_GPU_0".to_string(),
                "FW_ERoT_BMC_0".to_string(),
            ],
            &["FW_BMC_0".to_string(), "hgx_fw_gpu_0".to_string()],
        );

        assert_eq!(missing, vec!["FW_ERoT_BMC_0"]);
    }

    #[test]
    fn present_ap_names_from_collection_reports_member_leaves() {
        let collection = json!({
            "Members": [
                {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0"},
                {"@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0/"}
            ],
            "Name": "Software Inventory Collection"
        });

        assert_eq!(
            present_ap_names_from_collection(&collection),
            vec!["FW_BMC_0", "HGX_FW_GPU_0"]
        );
    }

    #[test]
    fn present_ap_names_from_collection_includes_embedded_oem_inventory() {
        let collection = json!({
            "Oem": {
                "Vendor": {
                    "FirmwareInventory": [
                        {
                            "DataSourceUri": "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0",
                            "Name": "FW_BMC_ALIAS"
                        }
                    ]
                }
            }
        });

        assert_eq!(
            present_ap_names_from_collection(&collection),
            vec!["FW_BMC_0", "FW_BMC_ALIAS"]
        );
    }

    #[test]
    fn present_ap_names_reports_ap_ids_without_full_uris_or_names() {
        let inventory = Map::from_iter([(
            "/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0".to_string(),
            json!({
                "Id": "HostBMC_0",
                "Name": "Host BMC",
                "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/FW_HOST_BMC_0",
            }),
        )]);

        let present = present_ap_names(&inventory);
        assert!(present.contains(&"FW_BMC_0".to_string()));
        assert!(present.contains(&"HostBMC_0".to_string()));
        assert!(present.contains(&"FW_HOST_BMC_0".to_string()));
        assert!(!present.contains(&"Host BMC".to_string()));
        assert!(
            !present.contains(&"/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0".to_string())
        );
        assert!(!present
            .contains(&"/redfish/v1/UpdateService/FirmwareInventory/FW_HOST_BMC_0".to_string()));
    }
}
