/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Synthetic controller targets exercise catalog validation without device access.

use super::*;
use crate::config::fabric_layout::test_support::{document, parse_document};
use crate::config::fabric_layout::{FabricLayoutCatalog, FabricLayoutConfig};

use figment::Figment;
use figment::providers::{Format, Toml};

/// Extends the shared link-only fixture with a set target and its restoration action.
fn controller_document() -> serde_json::Value {
    let mut document = document();

    for (index, fabric) in document["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        let action = if index == 0 {
            serde_json::json!({"type": "set", "value": "0"})
        } else {
            serde_json::json!({"type": "remove"})
        };

        fabric["controller_rules"] = serde_json::json!([{
            "topology_type": "synthetic-topology",
            "static_configs": [{"config_file_name": "fm_config", "key": "SETTING", "action": action}]
        }]);
    }

    document
}

#[test]
fn link_only_catalog_preserves_serialization_and_identity() {
    let original = document();
    let baseline = parse_document(&original).unwrap();

    let mut explicit_empty = original.clone();

    for fabric in explicit_empty["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
    {
        fabric["controller_rules"] = serde_json::json!([]);
    }

    let config: FabricLayoutConfig = serde_json::from_value(explicit_empty).unwrap();
    let catalog = FabricLayoutCatalog::from_config(&config).unwrap();
    let encoded = serde_json::to_value(config).unwrap();

    assert_eq!(catalog.identity(), baseline.identity());

    for fabric in encoded["profiles"][0]["fabric_rules"].as_array().unwrap() {
        assert!(fabric.get("controller_rules").is_none());
    }
}

#[test]
fn controller_targets_preserve_zero_removal_and_catalog_identity_on_round_trip() {
    let document = controller_document();
    let config: FabricLayoutConfig = serde_json::from_value(document.clone()).unwrap();
    let catalog = FabricLayoutCatalog::from_config(&config).unwrap();

    let profile = catalog
        .select(
            "synthetic physical topology",
            &["switch_gb200_nvidia".into()].into(),
        )
        .unwrap();

    assert_eq!(
        profile.fabric_rules[0].controller_rules[0].static_configs[0].action,
        FabricStaticConfigAction::Set { value: "0".into() }
    );

    assert_eq!(
        profile.fabric_rules[1].controller_rules[0].static_configs[0].action,
        FabricStaticConfigAction::Remove {}
    );

    let encoded = serde_json::to_value(config).unwrap();

    assert_eq!(
        parse_document(&encoded).unwrap().identity(),
        catalog.identity()
    );

    // Controller values and actions affect the catalog identity used for retries.
    for (path, value) in [
        (
            "/profiles/0/fabric_rules/0/controller_rules/0/static_configs/0/action/value",
            serde_json::json!("1"),
        ),
        (
            "/profiles/0/fabric_rules/0/controller_rules/0/static_configs/0/action",
            serde_json::json!({"type": "remove"}),
        ),
    ] {
        let mut changed = document.clone();
        *changed.pointer_mut(path).unwrap() = value;

        assert_ne!(
            parse_document(&changed).unwrap().identity(),
            catalog.identity()
        );
    }

    assert_ne!(
        parse_document(&self::document()).unwrap().identity(),
        catalog.identity()
    );
}

#[test]
fn controller_rules_load_from_toml_and_reject_unknown_fields() {
    let source = r#"
topology_type = "synthetic-topology"
static_configs = [
  { config_file_name = "fm_config", key = "SETTING", action = { type = "set", value = "0" } },
  { config_file_name = "other.cfg", key = "setting", action = { type = "remove" } }
]
"#;
    let rule: FabricControllerRule = Figment::from(Toml::string(source)).extract().unwrap();
    let mut document = document();

    for fabric in document["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
    {
        fabric["controller_rules"] = serde_json::json!([rule]);
    }

    let catalog = parse_document(&document).unwrap();

    let profile = catalog
        .select(
            "synthetic physical topology",
            &["switch_gb200_nvidia".into()].into(),
        )
        .unwrap();

    assert_eq!(
        profile.fabric_rules[0].controller_rules[0]
            .static_configs
            .len(),
        2
    );

    assert_eq!(
        rule.static_configs[0].action,
        FabricStaticConfigAction::Set { value: "0".into() }
    );

    assert_eq!(
        rule.static_configs[1].action,
        FabricStaticConfigAction::Remove {}
    );

    for (path, value) in [
        ("/topology_type", serde_json::json!(null)),
        (
            "/static_configs/0/action",
            serde_json::json!({"type": "set"}),
        ),
        (
            "/static_configs/0/action",
            serde_json::json!({"type": "remove", "value": "0"}),
        ),
        (
            "/static_configs/0/action",
            serde_json::json!({"type": "default"}),
        ),
        (
            "/static_configs/0",
            serde_json::json!({"config_file_name": "fm_config", "key": "SETTING", "action": {"type": "remove"}, "unexpected": true}),
        ),
    ] {
        let mut invalid = serde_json::to_value(&rule).unwrap();
        *invalid.pointer_mut(path).unwrap() = value;

        assert!(
            serde_json::from_value::<FabricControllerRule>(invalid).is_err(),
            "accepted {path}"
        );
    }

    let invalid = format!("{source}\nunexpected = true");

    assert!(
        Figment::from(Toml::string(&invalid))
            .extract::<FabricControllerRule>()
            .is_err()
    );
}

#[test]
fn malformed_keys_and_literals_fail_catalog_validation_without_echoing_values() {
    for (file, key) in [
        ("", "SETTING"),
        (".", "SETTING"),
        ("..", "SETTING"),
        (".config", "SETTING"),
        ("../fm_config", "SETTING"),
        ("file name", "SETTING"),
        ("fm_config", ""),
        ("fm_config", "1SETTING"),
        ("fm_config", "SETTING\n"),
        ("fm_config", "SETTING=1"),
        ("fm_config", "MNNVL_TOPOLOGY"),
        ("FM_CONFIG", "mnnvl_topology"),
    ] {
        let mut document = controller_document();

        // Keep ownership consistent so only identifier validation can reject the target.
        for fabric in document["profiles"][0]["fabric_rules"]
            .as_array_mut()
            .unwrap()
        {
            let target = &mut fabric["controller_rules"][0]["static_configs"][0];
            target["config_file_name"] = serde_json::json!(file);
            target["key"] = serde_json::json!(key);
        }

        let error = parse_document(&document).unwrap_err().to_string();

        let expected = if !file.eq_ignore_ascii_case("fm_config") {
            "config_file_name: managed static configuration requires a file basename"
        } else if key.eq_ignore_ascii_case("MNNVL_TOPOLOGY") {
            "key: managed static configuration topology must use the dedicated topology field"
        } else {
            "key: managed static configuration requires an identifier key"
        };

        assert_eq!(
            error,
            format!(
                "invalid fabric layout catalog: profiles[0]: fabric_rules[0].controller_rules[0].static_configs[0].{expected}"
            )
        );
    }

    for field in ["/topology_type", "/static_configs/0/action/value"] {
        for value in [
            "",
            " padded ",
            "value\nINJECTED=1",
            "value\t",
            "value#comment",
            "value;comment",
            "'value'",
            "\"value\"",
            "value\\escape",
        ] {
            let mut document = controller_document();
            *document["profiles"][0]["fabric_rules"][0]["controller_rules"][0]
                .pointer_mut(field)
                .unwrap() = serde_json::json!(value);

            let error = parse_document(&document).unwrap_err().to_string();

            let field = field
                .trim_start_matches('/')
                .replace("/0/", "[0].")
                .replace('/', ".");

            assert!(
                error.starts_with(&format!(
                    "invalid fabric layout catalog: profiles[0]: fabric_rules[0].controller_rules[0].{field}: "
                )),
                "{error}"
            );

            assert!(error.contains("nonempty unquoted literal lines"), "{error}");
            assert!(!error.contains("INJECTED"));
        }
    }
}

#[test]
fn controller_rules_require_complete_unique_targets_and_size_selectors() {
    let baseline = controller_document();
    let rule = baseline["profiles"][0]["fabric_rules"][0]["controller_rules"][0].clone();
    let target = rule["static_configs"][0].clone();

    for (path, value, expected) in [
        (
            "/controller_rules",
            serde_json::json!([]),
            "fabric_rules[0].controller_rules: at least one rule is required",
        ),
        (
            "/controller_rules",
            serde_json::json!([rule.clone(), rule]),
            "selectors must be unique",
        ),
        (
            "/controller_rules/0/static_configs",
            serde_json::json!([]),
            "cover exactly",
        ),
        (
            "/controller_rules/0/static_configs/0/key",
            serde_json::json!("ANOTHER_SETTING"),
            "cover exactly",
        ),
        (
            "/controller_rules/0/static_configs",
            serde_json::json!(vec![target.clone(); MAX_STATIC_CONFIG_KEYS + 1]),
            "unique within each file",
        ),
        (
            "/controller_rules/0/static_configs",
            serde_json::json!([target.clone(), target]),
            "unique within each file",
        ),
        (
            "/controller_rules/0/static_configs",
            serde_json::json!([
                {"config_file_name": "fm_config", "key": "SETTING", "action": {"type": "remove"}},
                {"config_file_name": "fm_config", "key": "setting", "action": {"type": "remove"}}
            ]),
            "cover exactly",
        ),
    ] {
        let mut invalid = baseline.clone();
        *invalid["profiles"][0]["fabric_rules"][0]
            .pointer_mut(path)
            .unwrap() = value;

        let error = parse_document(&invalid).unwrap_err().to_string();

        assert!(error.contains(expected), "{path}: {error}");
    }

    let mut missing = baseline;
    missing["profiles"][0]["fabric_rules"][0]
        .as_object_mut()
        .unwrap()
        .remove("controller_rules");

    assert!(
        parse_document(&missing)
            .unwrap_err()
            .to_string()
            .contains("fabric_rules[0].controller_rules: at least one rule is required")
    );
}

#[test]
fn each_topology_requires_its_own_full_capacity_restoration_rule() {
    let mut document = controller_document();
    let mut other = document["profiles"][0]["fabric_rules"][0]["controller_rules"][0].clone();
    other["topology_type"] = serde_json::json!("SYNTHETIC-TOPOLOGY");
    document["profiles"][0]["fabric_rules"][0]["controller_rules"]
        .as_array_mut()
        .unwrap()
        .push(other.clone());

    let error = parse_document(&document).unwrap_err();

    assert!(error.to_string().contains("full-capacity restoration rule"));

    document["profiles"][0]["fabric_rules"][1]["controller_rules"]
        .as_array_mut()
        .unwrap()
        .push(other);

    assert!(parse_document(&document).is_ok());
}

#[test]
fn controller_ownership_distinguishes_key_casing_across_all_sizes() {
    let mut document = controller_document();

    for fabric in document["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
    {
        fabric["controller_rules"][0]["static_configs"] = serde_json::json!([
            {"config_file_name": "sm_config", "key": "log_flags",
             "action": {"type": "set", "value": "7"}},
            {"config_file_name": "sm_config", "key": "LOG_FLAGS",
             "action": {"type": "remove"}}
        ]);
    }

    let catalog = parse_document(&document);

    assert!(catalog.is_ok(), "{catalog:?}");

    document["profiles"][0]["fabric_rules"][1]["controller_rules"][0]["static_configs"]
        .as_array_mut()
        .unwrap()
        .pop();

    let error = parse_document(&document).unwrap_err();

    assert!(error.to_string().contains("cover exactly"), "{error}");
}

#[test]
fn controller_rules_allow_no_owned_keys() {
    let mut document = controller_document();

    for fabric in document["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
    {
        fabric["controller_rules"][0]["static_configs"] = serde_json::json!([]);
    }

    assert!(parse_document(&document).is_ok());
}

#[test]
fn controller_rules_accept_the_rule_limit_and_reject_one_more() {
    let mut document = controller_document();

    // Each requested topology has both a smaller-size and a restoration rule.
    for fabric in document["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
    {
        let template = fabric["controller_rules"][0].clone();

        fabric["controller_rules"] = (0..MAX_OPTIONS / 2)
            .map(|index| {
                let mut rule = template.clone();
                rule["topology_type"] = serde_json::json!(format!("synthetic-{index}"));

                rule
            })
            .collect();
    }

    assert!(parse_document(&document).is_ok());

    // A full-capacity-only topology is otherwise valid and adds exactly one rule.
    let mut extra = document["profiles"][0]["fabric_rules"][1]["controller_rules"][0].clone();
    extra["topology_type"] = serde_json::json!("synthetic-extra");
    document["profiles"][0]["fabric_rules"][1]["controller_rules"]
        .as_array_mut()
        .unwrap()
        .push(extra);

    let error = parse_document(&document).unwrap_err().to_string();

    assert_eq!(
        error,
        "invalid fabric layout catalog: profiles[0]: fabric_rules: total controller rule count exceeds 256"
    );
}

#[test]
fn controller_rules_accept_the_key_limit_and_reject_one_more() {
    let mut document = controller_document();

    for fabric in document["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
    {
        fabric["controller_rules"][0]["static_configs"] = (0..MAX_STATIC_CONFIG_KEYS)
            .map(|index| {
                serde_json::json!({
                    "config_file_name": "fm_config", "key": format!("SETTING_{index}"),
                    "action": {"type": "remove"}
                })
            })
            .collect();
    }

    assert!(parse_document(&document).is_ok());

    // Keep ownership complete so only the key-count limit rejects the catalog.
    for fabric in document["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
    {
        fabric["controller_rules"][0]["static_configs"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "config_file_name": "fm_config", "key": "EXTRA_SETTING",
                "action": {"type": "remove"}
            }));
    }

    let error = parse_document(&document).unwrap_err().to_string();

    assert_eq!(
        error,
        "invalid fabric layout catalog: profiles[0]: fabric_rules: managed static configuration key count exceeds 1024"
    );
}
