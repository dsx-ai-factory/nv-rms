/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Catalog validation and coordinate-calculation regression tests.

use super::test_support::{document, parse_document};
use super::*;

use figment::Figment;
use figment::providers::{Format, Serialized, Toml};

#[test]
fn full_link_ranges_preserve_catalog_identity_targets_and_serialization() {
    let expanded = calculated_document();

    let mut ranged = expanded.clone();

    ranged["profiles"][0]["full_link_states"] = serde_json::json!({
        "acp9-10": "up", "acp29-30": "up", "fnm1-1": "up"
    });

    let expanded_catalog = parse_document(&expanded).unwrap();
    let ranged_catalog = parse_document(&ranged).unwrap();
    let ranged_config: FabricLayoutConfig = serde_json::from_value(ranged.clone()).unwrap();

    assert_eq!(expanded_catalog.identity(), ranged_catalog.identity());
    assert_eq!(expand_document(expanded.clone()), expand_document(ranged));

    assert_eq!(
        serde_json::to_value(&ranged_config.profiles[0].full_link_states).unwrap(),
        expanded["profiles"][0]["full_link_states"]
    );

    let roundtrip: FabricLayoutConfig =
        serde_json::from_slice(&serde_json::to_vec(&ranged_config).unwrap()).unwrap();

    assert_eq!(
        FabricLayoutCatalog::from_config(&roundtrip)
            .unwrap()
            .identity(),
        ranged_catalog.identity()
    );
}

#[test]
fn full_link_ranges_expand_after_figment_merges_raw_keys() {
    let mut profile = document()["profiles"][0].clone();
    profile.as_object_mut().unwrap().remove("full_link_states");

    let source = Figment::from(Serialized::defaults(profile)).merge(Toml::string(
        r#"full_link_states = { "acp1-2" = "up", acp4 = "down", "fnm1-2" = "up" }"#,
    ));

    let merged: FabricLayoutProfile = source
        .clone()
        .merge(Toml::string(r#"full_link_states = { "acp1-2" = "down" }"#))
        .extract()
        .unwrap();

    assert_eq!(
        merged.full_link_states,
        BTreeMap::from([
            ("acp1".into(), FabricLinkState::Down),
            ("acp2".into(), FabricLinkState::Down),
            ("acp4".into(), FabricLinkState::Down),
            ("fnm1".into(), FabricLinkState::Up),
            ("fnm2".into(), FabricLinkState::Up),
        ])
    );

    for state in ["up", "down"] {
        let conflicting = source
            .clone()
            .merge(Toml::string(&format!(
                "full_link_states = {{ acp2 = \"{state}\" }}"
            )))
            .extract::<FabricLayoutProfile>();

        assert!(conflicting.is_err(), "overlap accepted for {state}");
    }
}

#[test]
fn full_link_ranges_reject_invalid_indices_syntax_and_overlaps() {
    for expression in [
        "acp0",
        "acp01",
        "acp+1",
        "acp1-0",
        "acp1-02",
        "acp2-1",
        "acp1-",
        "acp-2",
        "acp1-2-3",
        "acp1-acp2",
        "acp1,2",
        "acp*",
        "acp1 -2",
        "acp1- 2",
        " acp1",
        "acp1 ",
        "ACP1",
        "eth1",
        "acp",
        "acp4294967296",
        "acp1-4294967296",
    ] {
        let mut input = document();
        input["profiles"][0]["full_link_states"] = serde_json::json!({expression: "up"});

        assert!(parse_document(&input).is_err(), "accepted {expression}");
    }

    for targets in [
        serde_json::json!({"acp1-3": "up", "acp2-4": "up"}),
        serde_json::json!({"fnm1-3": "up", "fnm2": "down"}),
        serde_json::json!({"acp1": "up", "acp1-1": "up"}),
    ] {
        let mut input = document();
        input["profiles"][0]["full_link_states"] = targets;

        assert!(parse_document(&input).is_err());
    }
}

#[test]
fn full_link_ranges_enforce_the_expanded_limit_before_allocation() {
    for (targets, valid) in [
        (serde_json::json!({"acp1-1024": "up"}), true),
        (serde_json::json!({"acp4294967295-4294967295": "up"}), true),
        (serde_json::json!({"acp1-1025": "up"}), false),
        (serde_json::json!({"acp1-1024": "up", "fnm1": "up"}), false),
        (serde_json::json!({"acp1-4294967295": "up"}), false),
    ] {
        let mut input = document();
        input["profiles"][0]["full_link_states"] = targets;

        assert_eq!(parse_document(&input).is_ok(), valid, "{input}");
    }
}

fn calculated_document() -> serde_json::Value {
    let mut document = document();
    let profile = &mut document["profiles"][0];

    profile["full_link_states"] = serde_json::json!({
        "acp9": "up", "acp10": "up",
        "acp29": "up", "acp30": "up", "fnm1": "up"
    });

    profile["link_calculation"] = serde_json::json!({
        "default_state": "down",
        "rules": [{"prefix": "acp", "base": 1, "offsets": [0, 20], "width": 2, "state": "up",
            "terms": [
                {"node": "compute", "coordinate": "tray_index", "divisor": 1,
                    "remap": {"3": 3, "4": 4}, "multiplier": 2},
                {"node": "switch", "coordinate": "tray_index", "divisor": 1, "multiplier": 1}
            ]}]
    });

    document
}

// Use deliberately nonzero indices and separate slots to exercise live-coordinate evaluation.
fn observed_positions() -> (BTreeSet<FabricPosition>, BTreeSet<FabricPosition>) {
    (
        BTreeSet::from([FabricPosition {
            tray_index: 3,
            slot_number: 7,
        }]),
        BTreeSet::from([FabricPosition {
            tray_index: 2,
            slot_number: 9,
        }]),
    )
}

fn expand_document(
    document: serde_json::Value,
) -> Result<BTreeMap<FabricPosition, BTreeMap<String, FabricLinkState>>, String> {
    let catalog = parse_document(&document).map_err(|error| error.to_string())?;

    let profile = &catalog.profiles[0];
    let (computes, switches) = observed_positions();

    profile.link_calculation.expand(
        &computes,
        &switches,
        &profile.full_link_states.keys().cloned().collect(),
    )
}

#[test]
fn complete_rules_load_and_select_by_observed_identity() {
    let catalog = parse_document(&document()).unwrap();

    let types = BTreeSet::from(["switch_gb200_nvidia".to_owned()]);

    assert!(
        catalog
            .select("synthetic physical topology", &types)
            .is_some()
    );

    assert!(catalog.select("different topology", &types).is_none());
    assert_eq!(catalog.identity().len(), 64);
}

#[test]
fn calculated_links_expand_private_parameters_into_complete_targets() {
    let targets = expand_document(calculated_document()).unwrap();

    assert_eq!(
        targets[&FabricPosition {
            tray_index: 2,
            slot_number: 9
        }],
        BTreeMap::from([
            ("acp9".into(), FabricLinkState::Up),
            ("acp10".into(), FabricLinkState::Up),
            ("acp29".into(), FabricLinkState::Up),
            ("acp30".into(), FabricLinkState::Up),
            ("fnm1".into(), FabricLinkState::Down),
        ])
    );

    assert_eq!(targets.len(), 1);
}

#[test]
fn counts_group_roles_and_supported_sizes_are_validated() {
    for (path, value) in [
        ("/profiles/0/compute_tray_count", serde_json::json!(0)),
        ("/profiles/0/switch_tray_count", serde_json::json!(256)),
        (
            "/profiles/0/fabric_rules/0/compute_tray_count",
            serde_json::json!(3),
        ),
        (
            "/profiles/0/fabric_rules/0/switch_tray_count",
            serde_json::json!(0),
        ),
        (
            "/profiles/0/fabric_rules/0/grouping/compute/node",
            serde_json::json!("switch"),
        ),
        (
            "/profiles/0/fabric_rules/0/grouping/switch/node",
            serde_json::json!("compute"),
        ),
        (
            "/profiles/0/fabric_rules/0/grouping/compute/divisor",
            serde_json::json!(0),
        ),
    ] {
        let mut document = document();
        *document.pointer_mut(path).unwrap() = value;

        assert!(
            parse_document(&document).is_err(),
            "accepted invalid grouping or size at {path}"
        );
    }

    let mut document = document();
    let duplicate = document["profiles"][0]["fabric_rules"][0].clone();
    document["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);

    assert!(parse_document(&document).is_err());
}

#[test]
fn implicit_or_missing_link_targets_fail_catalog_loading() {
    for (path, key) in [
        ("/profiles/0/link_calculation", "default_state"),
        ("/profiles/0/link_calculation/rules/0", "state"),
    ] {
        for value in [Some(serde_json::json!("default")), None] {
            let mut document = document();
            let target = document.pointer_mut(path).unwrap().as_object_mut().unwrap();

            if let Some(value) = value {
                target.insert(key.to_owned(), value);
            } else {
                target.remove(key);
            }

            assert!(
                parse_document(&document).is_err(),
                "accepted an implicit or missing target at {path}/{key}"
            );
        }
    }
}

#[test]
fn incomplete_links_and_unknown_fields_fail_loading() {
    let mut document = document();
    document["profiles"][0]["full_link_states"] = serde_json::json!({});

    assert!(parse_document(&document).is_err());

    document = self::document();
    document["profiles"][0]["formula"] = serde_json::json!("executable input");

    assert!(parse_document(&document).is_err());
}

#[test]
fn baseline_keys_define_ownership_and_calculation_cannot_emit_unowned_links() {
    let mut document = document();
    document["profiles"][0]["full_link_states"] = serde_json::json!({"fnm1": "down"});

    // A baseline does not require a redundant interface list. Calculation still
    // rejects an emitted name outside that baseline before any switch writes.
    assert!(parse_document(&document).is_ok());

    assert!(
        expand_document(document)
            .unwrap_err()
            .contains("uncontrolled")
    );

    let mut invalid = self::document();
    invalid["profiles"][0]["full_link_states"]["acp1"] = serde_json::json!("default");

    assert!(parse_document(&invalid).is_err());
    invalid["profiles"][0]
        .as_object_mut()
        .unwrap()
        .remove("full_link_states");

    assert!(parse_document(&invalid).is_err());
}

#[test]
fn conflicting_profile_selectors_fail_loading() {
    let mut document = document();
    let duplicate = document["profiles"][0].clone();
    document["profiles"].as_array_mut().unwrap().push(duplicate);

    assert!(parse_document(&document).is_err());
}

#[test]
fn typed_catalog_rejects_oversized_configuration() {
    let mut config: FabricLayoutConfig = serde_json::from_value(document()).unwrap();
    config.profiles[0].physical_topology = "x".repeat(MAX_CATALOG_BYTES as usize);

    let error = FabricLayoutCatalog::from_config(&config).unwrap_err();

    assert!(error.to_string().contains("document exceeds 2 MiB"));
}

#[test]
fn grouping_is_required_only_for_smaller_sizes() {
    for (index, grouping) in [
        (0, serde_json::Value::Null),
        (
            1,
            document()["profiles"][0]["fabric_rules"][0]["grouping"].clone(),
        ),
    ] {
        let mut invalid = document();
        invalid["profiles"][0]["fabric_rules"][index]["grouping"] = grouping;
        let error = parse_document(&invalid).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("grouping is required only for smaller")
        );
    }
}

#[test]
fn grouping_joins_equal_coordinates_and_excludes_incomplete_groups() {
    let catalog = parse_document(&document()).unwrap();

    let grouping = catalog.profiles[0].fabric_rules[0]
        .grouping
        .as_ref()
        .unwrap();

    let position = |tray_index| FabricPosition {
        tray_index,
        slot_number: tray_index + 10,
    };

    let computes = BTreeSet::from([position(1), position(2)]);
    let switches = BTreeSet::from([position(2), position(3)]);

    let groups = grouping.resolve(&computes, &switches).unwrap();

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].computes, BTreeSet::from([position(2)]));
    assert_eq!(groups[0].switches, BTreeSet::from([position(2)]));
}

#[test]
fn grouping_rejects_unmapped_coordinates() {
    let mut input = document();
    input["profiles"][0]["fabric_rules"][0]["grouping"]["compute"]["remap"] =
        serde_json::json!({"0": 0});

    let catalog = parse_document(&input).unwrap();

    let grouping = catalog.profiles[0].fabric_rules[0]
        .grouping
        .as_ref()
        .unwrap();

    let positions = BTreeSet::from([FabricPosition {
        tray_index: 1,
        slot_number: 2,
    }]);

    let error = grouping.resolve(&positions, &positions).err().unwrap();

    assert!(error.contains("remap"), "{error}");
}

#[test]
fn catalog_requires_full_capacity_rule_and_identity_tracks_link_targets() {
    let mut input = document();
    let baseline = parse_document(&input).unwrap();
    input["profiles"][0]["full_link_states"]["fnm1"] = serde_json::json!("up");

    let changed = parse_document(&input).unwrap();

    assert_ne!(baseline.identity(), changed.identity());

    input["profiles"][0]["fabric_rules"]
        .as_array_mut()
        .unwrap()
        .pop();

    let error = parse_document(&input).unwrap_err();

    assert!(error.to_string().contains("full physical inventory size"));
}
