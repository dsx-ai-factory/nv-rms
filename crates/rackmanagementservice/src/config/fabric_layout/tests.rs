/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Synthetic mapping rules exercise calculation without platform wiring data.

use std::collections::{BTreeMap, BTreeSet};

use super::*;

use figment::Figment;
use figment::providers::{Format, Toml};
use serde_json::{Value, json};

const CONFIG: &str = r#"
default_state = "down"
[[rules]]
prefix = "port"
base = 1
offsets = [0, 20]
width = 2
state = "up"
terms = [
  { node = "compute", coordinate = "tray_index", divisor = 1, multiplier = 2, remap = { "3" = 3, "4" = 4 } },
  { node = "switch", coordinate = "tray_index", divisor = 1, multiplier = 1 },
]
"#;

fn calculation() -> FabricLinkCalculation {
    Figment::from(Toml::string(CONFIG)).extract().unwrap()
}

// Nonzero indices and different slots expose incorrect coordinate selection.
fn inputs() -> (
    BTreeSet<FabricPosition>,
    BTreeSet<FabricPosition>,
    BTreeSet<String>,
) {
    (
        [FabricPosition {
            tray_index: 3,
            slot_number: 7,
        }]
        .into(),
        [FabricPosition {
            tray_index: 2,
            slot_number: 9,
        }]
        .into(),
        ["port9", "port10", "port29", "port30", "aux1"]
            .map(str::to_owned)
            .into(),
    )
}

#[test]
fn toml_rules_expand_into_complete_targets() {
    let calculation = calculation();
    let (computes, switches, interfaces) = inputs();

    let targets = calculation
        .expand(&computes, &switches, &interfaces)
        .unwrap();

    assert_eq!(targets.len(), 1);

    assert_eq!(
        targets.values().next().unwrap(),
        &BTreeMap::from([
            ("port9".into(), FabricLinkState::Up),
            ("port10".into(), FabricLinkState::Up),
            ("port29".into(), FabricLinkState::Up),
            ("port30".into(), FabricLinkState::Up),
            ("aux1".into(), FabricLinkState::Down),
        ])
    );
}

#[test]
fn slot_transforms_divide_then_remap_before_signed_scaling() {
    let mut document = serde_json::to_value(calculation()).unwrap();
    document["rules"][0]["base"] = json!(2);

    document["rules"][0]["terms"] = json!([
        {"node": "compute", "coordinate": "slot_number", "divisor": 2, "modulus": 2,
            "remap": {"1": 4}, "multiplier": 2},
        {"node": "switch", "coordinate": "slot_number", "divisor": 3, "modulus": 2,
            "remap": {"1": 1}, "multiplier": -1}
    ]);

    let calculation: FabricLinkCalculation = serde_json::from_value(document).unwrap();
    let (computes, switches, interfaces) = inputs();

    let targets = calculation
        .expand(&computes, &switches, &interfaces)
        .unwrap();

    let states = targets.values().next().unwrap();

    assert_eq!(states["port9"], FabricLinkState::Up);
    assert_eq!(states["port30"], FabricLinkState::Up);
    assert_eq!(states["aux1"], FabricLinkState::Down);
}

#[test]
fn multiple_compute_and_switch_coordinates_have_independent_targets() {
    let (mut computes, mut switches, mut interfaces) = inputs();

    computes.insert(FabricPosition {
        tray_index: 4,
        slot_number: 8,
    });

    switches.insert(FabricPosition {
        tray_index: 4,
        slot_number: 10,
    });

    for number in [11, 12, 13, 14, 31, 32, 33, 34] {
        interfaces.insert(format!("port{number}"));
    }

    let targets = calculation()
        .expand(&computes, &switches, &interfaces)
        .unwrap();

    assert_eq!(targets.len(), 2);

    for (switch, states) in targets {
        let enabled: BTreeSet<_> = states
            .iter()
            .filter(|(_, state)| **state == FabricLinkState::Up)
            .map(|(id, _)| id.clone())
            .collect();

        let expected = match switch.tray_index {
            2 => [9, 10, 11, 12, 29, 30, 31, 32],
            4 => [11, 12, 13, 14, 31, 32, 33, 34],
            other => panic!("unexpected switch tray {other}"),
        };

        assert_eq!(
            enabled,
            expected.map(|number| format!("port{number}")).into()
        );

        assert_eq!(states.len(), interfaces.len());

        assert_eq!(states["aux1"], FabricLinkState::Down);
    }
}

#[test]
fn invalid_rule_definitions_are_rejected_before_expansion() {
    for (path, value) in [
        ("/rules", json!([])),
        ("/rules/0/prefix", json!("")),
        ("/rules/0/prefix", json!("port1")),
        ("/rules/0/prefix", json!("PORT")),
        ("/rules/0/width", json!(0)),
        ("/rules/0/offsets", json!([])),
        ("/rules/0/terms/0/node", json!("powershelf")),
        ("/rules/0/terms/0/divisor", json!(0)),
        ("/rules/0/terms/0/modulus", json!(0)),
        ("/rules/0/terms/0/remap", json!({})),
        ("/rules/0/terms/0/remap", json!({"03":3})),
        ("/rules/0/terms/0/remap", json!({"-1":3})),
    ] {
        let mut document = serde_json::to_value(calculation()).unwrap();
        *document.pointer_mut(path).unwrap() = value;

        let calculation: FabricLinkCalculation = serde_json::from_value(document).unwrap();

        let error = calculation.validate().unwrap_err();

        let field = path
            .trim_start_matches('/')
            .replace("/0", "[0]")
            .replace('/', ".");

        assert!(error.starts_with(&format!("{field}:")), "{path}: {error}");
    }
}

#[test]
fn invalid_fields_states_and_selectors_fail_deserialization() {
    for (path, key, value) in [
        ("", "unexpected", json!(true)),
        ("/rules/0", "unexpected", json!(true)),
        ("/rules/0/terms/0", "unexpected", json!(true)),
        ("", "default_state", json!("default")),
        ("/rules/0", "state", json!("diag")),
        ("/rules/0/terms/0", "coordinate", json!("unknown")),
        ("/rules/0/terms/0", "node", json!("unknown")),
        (
            "/rules/0/terms/0",
            "coordinate",
            json!("compute_tray_index"),
        ),
        (
            "/rules/0/terms/0",
            "coordinate",
            json!("compute_slot_number"),
        ),
        ("/rules/0/terms/0", "coordinate", json!("switch_tray_index")),
        (
            "/rules/0/terms/0",
            "coordinate",
            json!("switch_slot_number"),
        ),
    ] {
        let mut document = serde_json::to_value(calculation()).unwrap();
        document.pointer_mut(path).unwrap()[key] = value;

        assert!(serde_json::from_value::<FabricLinkCalculation>(document).is_err());
    }

    for (path, key) in [
        ("", "default_state"),
        ("/rules/0", "state"),
        ("/rules/0/terms/0", "node"),
        ("/rules/0/terms/0", "coordinate"),
    ] {
        let mut document = serde_json::to_value(calculation()).unwrap();
        document
            .pointer_mut(path)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);

        assert!(serde_json::from_value::<FabricLinkCalculation>(document).is_err());
    }
}

#[test]
fn expansion_rejects_overflow_missing_remaps_and_invalid_assignments() {
    for (path, value, expected) in [
        (
            "/rules/0/terms/0/node",
            json!("powershelf"),
            "node: must be compute or switch",
        ),
        ("/rules/0/terms/0/multiplier", json!(i64::MAX), "overflow"),
        ("/rules/0/base", json!(i64::MAX), "overflow"),
        ("/rules/0/base", json!(-100), "out of range"),
        ("/rules/0/base", json!(-8), "must be positive"),
        ("/rules/0/base", json!(u32::MAX), "out of range"),
        ("/rules/0/offsets", json!([0, 0]), "more than once"),
        ("/rules/0/offsets", json!([0, 1]), "more than once"),
        ("/rules/0/offsets", json!([0, 100]), "uncontrolled"),
        ("/rules/0/terms/0/remap", json!({"4":4}), "does not cover"),
    ] {
        let mut document = serde_json::to_value(calculation()).unwrap();
        *document.pointer_mut(path).unwrap() = value;

        let calculation: FabricLinkCalculation = serde_json::from_value(document).unwrap();
        let (computes, switches, interfaces) = inputs();

        let error = calculation
            .expand(&computes, &switches, &interfaces)
            .unwrap_err();

        assert!(error.contains(expected), "{path}: {error}");
        assert!(error.starts_with("rules[0]"), "{path}: {error}");
    }
}

#[test]
fn diagnostics_locate_later_rules_and_terms_without_printing_values() {
    let mut document = serde_json::to_value(calculation()).unwrap();
    let mut rule = document["rules"][0].clone();
    rule["terms"][1]["divisor"] = json!(0);
    document["rules"].as_array_mut().unwrap().push(rule);

    let invalid: FabricLinkCalculation = serde_json::from_value(document.clone()).unwrap();

    assert_eq!(
        invalid.validate().unwrap_err(),
        "rules[1].terms[1].divisor: must be positive"
    );

    document["rules"][1]["terms"][1]["divisor"] = json!(1);
    document["rules"][1]["terms"][1]["remap"] = json!({"123456789": 987654321});
    document["rules"][1]["terms"][0]["multiplier"] = json!(0);
    document["rules"][1]["base"] = json!(13);

    let invalid: FabricLinkCalculation = serde_json::from_value(document).unwrap();
    let (computes, switches, interfaces) = inputs();

    let error = invalid
        .expand(&computes, &switches, &interfaces)
        .unwrap_err();

    assert_eq!(
        error,
        "rules[1].terms[1].remap: does not cover a supplied coordinate"
    );

    assert!(!error.contains("123456789"));
    assert!(!error.contains("987654321"));
}

#[test]
fn duplicate_rules_and_compute_assignments_fail() {
    let document = serde_json::to_value(calculation()).unwrap();
    let rule = document["rules"][0].clone();

    let duplicated_calculation: FabricLinkCalculation = serde_json::from_value(json!({
        "default_state":"down", "rules":[rule.clone(), rule]
    }))
    .unwrap();

    let (mut computes, switches, interfaces) = inputs();

    let error = duplicated_calculation
        .expand(&computes, &switches, &interfaces)
        .unwrap_err();

    assert!(error.contains("more than once"));

    computes.insert(FabricPosition {
        tray_index: 3,
        slot_number: 8,
    });

    let error = calculation()
        .expand(&computes, &switches, &interfaces)
        .unwrap_err();

    assert!(error.contains("more than once"));
}

#[test]
fn empty_compute_switch_or_interface_inputs_are_rejected() {
    let calculation = calculation();
    let (computes, switches, interfaces) = inputs();

    for (compute_input, switch_input, interface_input) in [
        (BTreeSet::new(), switches.clone(), interfaces.clone()),
        (computes.clone(), BTreeSet::new(), interfaces.clone()),
        (computes, switches, BTreeSet::new()),
    ] {
        let result = calculation.expand(&compute_input, &switch_input, &interface_input);

        assert!(result.is_err());
    }
}

#[test]
fn serialization_preserves_calculated_targets() {
    let original = calculation();
    let document: Value = serde_json::to_value(&original).unwrap();

    let decoded: FabricLinkCalculation = serde_json::from_value(document).unwrap();
    let (computes, switches, interfaces) = inputs();

    let expected = original.expand(&computes, &switches, &interfaces).unwrap();

    let actual = decoded.expand(&computes, &switches, &interfaces).unwrap();

    assert_eq!(actual, expected);

    assert_eq!(
        actual.values().next().unwrap()["port10"],
        FabricLinkState::Up
    );
}

#[test]
fn constant_rules_override_an_up_default() {
    let document = json!({
        "default_state": "up",
        "rules": [{"prefix":"port", "base":9, "terms":[], "offsets":[0,20],
            "width":2, "state":"down"}]
    });

    let calculation: FabricLinkCalculation = serde_json::from_value(document).unwrap();
    let (computes, switches, interfaces) = inputs();

    let targets = calculation
        .expand(&computes, &switches, &interfaces)
        .unwrap();

    let states = targets.values().next().unwrap();

    assert_eq!(states["aux1"], FabricLinkState::Up);
    assert_eq!(states["port9"], FabricLinkState::Down);
    assert_eq!(states["port30"], FabricLinkState::Down);
}

#[test]
fn block_width_is_supplied_by_rules() {
    let document = json!({
        "default_state": "down",
        "rules": [{"prefix":"port", "base":1, "terms":[], "offsets":[0],
            "width":1025, "state":"up"}]
    });

    let calculation: FabricLinkCalculation = serde_json::from_value(document).unwrap();
    let (computes, switches, _) = inputs();

    let interfaces = (1..=1025).map(|number| format!("port{number}")).collect();

    let targets = calculation
        .expand(&computes, &switches, &interfaces)
        .unwrap();

    let states = targets.values().next().unwrap();

    assert_eq!(states.len(), 1025);
    assert!(states.values().all(|state| *state == FabricLinkState::Up));
}

#[test]
fn configured_rule_sizes_are_accepted() {
    let prefix = "p".repeat(33);

    let remap: BTreeMap<_, _> = (0..257)
        .map(|number| (number.to_string(), number))
        .collect();

    let term = json!({
        "node":"compute", "coordinate":"tray_index", "divisor":1, "multiplier":0, "remap":remap
    });

    let offsets: Vec<_> = (0..33).collect();

    let rules: Vec<_> = (0..33)
        .map(|index| {
            json!({
                "prefix":prefix, "base":1 + index * 33, "terms":vec![term.clone(); 9],
                "offsets":offsets, "width":1, "state":"up"
            })
        })
        .collect();

    let calculation: FabricLinkCalculation = serde_json::from_value(json!({
        "default_state":"down", "rules":rules
    }))
    .unwrap();

    let (computes, switches, _) = inputs();

    let interfaces = (1..=1089)
        .map(|number| format!("{prefix}{number}"))
        .collect();

    let targets = calculation
        .expand(&computes, &switches, &interfaces)
        .unwrap();

    let states = targets.values().next().unwrap();

    assert_eq!(states.len(), 1089);
    assert!(states.values().all(|state| *state == FabricLinkState::Up));
}

#[test]
fn inventory_and_interface_sizes_are_supplied_by_the_caller() {
    let calculation: FabricLinkCalculation = serde_json::from_value(json!({
        "default_state":"down",
        "rules":[{"prefix":"port", "base":1, "offsets":[0], "width":1, "state":"up",
            "terms":[{"node":"compute", "coordinate":"tray_index", "divisor":1, "multiplier":1}]}]
    }))
    .unwrap();

    let (_, switches, _) = inputs();

    let computes = (0..257)
        .map(|tray_index| FabricPosition {
            tray_index,
            slot_number: 0,
        })
        .collect();

    let interfaces = (1..=1025).map(|number| format!("port{number}")).collect();

    let targets = calculation
        .expand(&computes, &switches, &interfaces)
        .unwrap();

    let states = targets.values().next().unwrap();

    assert_eq!(states.len(), 1025);

    assert_eq!(
        states
            .values()
            .filter(|state| **state == FabricLinkState::Up)
            .count(),
        257
    );

    assert_eq!(states["port257"], FabricLinkState::Up);
    assert_eq!(states["port258"], FabricLinkState::Down);
}
