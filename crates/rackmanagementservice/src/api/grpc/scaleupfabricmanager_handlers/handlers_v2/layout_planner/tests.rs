/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Synthetic inventory, placement, and controller-target planning checks.

use super::*;
use crate::config::fabric_layout::{FabricControllerRule, FabricStaticConfigTarget, test_support};

const SMALL: rm_v2::ScaleUpFabricSpec = rm_v2::ScaleUpFabricSpec {
    compute_tray_count: 1,
    switch_tray_count: 1,
    primary_switch_node_id: None,
};

#[test]
fn mixed_sizes_have_disjoint_members_and_size_specific_controller_values() {
    let (mut profile, mut switches, mut computes, config) = fixture();
    profile.compute_tray_count = 3;
    profile.switch_tray_count = 3;
    profile.fabric_rules[1].compute_tray_count = 3;
    profile.fabric_rules[1].switch_tray_count = 3;

    for (nodes, slot_number) in [(&mut switches, 23), (&mut computes, 3)] {
        let mut node = nodes[0].clone();
        node.node_id = node.node_id.replace('0', "2");

        node.position = FabricPosition {
            tray_index: 2,
            slot_number,
        };

        nodes.push(node);
    }

    let mut paired = profile.fabric_rules[0].clone();
    paired.compute_tray_count = 2;

    paired.grouping = serde_json::from_value(serde_json::json!({
        "compute": {"node": "compute", "coordinate": "tray_index", "divisor": 2, "multiplier": 1},
        "switch": {"node": "switch", "coordinate": "tray_index", "divisor": 1, "remap": {"0": 1, "1": 0, "2": 2}, "multiplier": 1}
    })).unwrap();

    profile.fabric_rules.push(paired);

    for (rule, value) in profile
        .fabric_rules
        .iter_mut()
        .zip(["single", "full", "paired"])
    {
        rule.controller_rules[0].static_configs = vec![FabricStaticConfigTarget {
            config_file_name: "fm_config".into(),
            key: "SYNTHETIC_SETTING".into(),
            action: FabricStaticConfigAction::Set {
                value: value.into(),
            },
        }];
    }

    profile
        .full_link_states
        .insert("acp2".into(), FabricLinkState::Up);

    profile.link_calculation = serde_json::from_value(serde_json::json!({
        "default_state": "down", "rules": [{"prefix": "acp", "base": 1,
        "terms": [{"node": "compute", "coordinate": "tray_index", "divisor": 1, "modulus": 2, "multiplier": 1}],
        "offsets": [0], "width": 1, "state": "up"}]
    })).unwrap();

    let requests = [
        SMALL,
        rm_v2::ScaleUpFabricSpec {
            compute_tray_count: 2,
            ..SMALL
        },
    ];

    let plan = plan_layout(&profile, &switches, &computes, &requests, &config).unwrap();

    assert_eq!(
        plan.fabrics[1].assignment.compute_node_ids,
        ["compute-0", "compute-1"]
    );

    assert_eq!(plan.fabrics[1].assignment.switch_node_ids, ["switch-1"]);
    assert_eq!(plan.fabrics[0].assignment.compute_node_ids, ["compute-2"]);
    assert_eq!(plan.fabrics[0].assignment.switch_node_ids, ["switch-2"]);

    assert_eq!(
        plan.fabrics[1].config.extra_static_configs[0].value,
        "paired"
    );

    assert_eq!(
        plan.fabrics[0].config.extra_static_configs[0].value,
        "single"
    );

    assert_eq!(plan.links["switch-1"]["acp2"], FabricLinkState::Up);
    assert_eq!(plan.links["switch-0"]["acp1"], FabricLinkState::Down);

    // A remap in an unrequested size must not prevent a supported request.
    computes[1].position.tray_index = 5;
    switches[1].position.tray_index = 5;

    let plan = plan_layout(&profile, &switches, &computes, &[SMALL], &config).unwrap();

    assert_eq!(plan.fabrics[0].assignment.switch_node_ids, ["switch-0"]);
}

#[test]
fn placement_search_checks_all_candidates_and_rejects_impossible_membership() {
    let placement = ResolvedPlacement {
        compute_positions: [FabricPosition {
            tray_index: 0,
            slot_number: 1,
        }]
        .into(),
        switch_positions: [FabricPosition {
            tray_index: 0,
            slot_number: 2,
        }]
        .into(),
        links: BTreeMap::new(),
    };

    let alternative = ResolvedPlacement {
        compute_positions: [FabricPosition {
            tray_index: 1,
            slot_number: 3,
        }]
        .into(),
        switch_positions: [FabricPosition {
            tray_index: 1,
            slot_number: 4,
        }]
        .into(),
        links: BTreeMap::new(),
    };

    // Overlapping candidates must not prevent reaching a later valid choice.
    let mut options = vec![&placement; 4097];
    options.push(&alternative);

    let mut selected = Vec::new();

    let possible = select_placements(
        &[vec![&placement], options],
        &mut selected,
        &mut BTreeSet::new(),
    );

    assert!(possible);
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].switch_positions, placement.switch_positions);
    assert_eq!(selected[1].switch_positions, alternative.switch_positions);

    selected.clear();

    let mut failed = BTreeSet::new();

    let possible = select_placements(
        &vec![vec![&placement, &alternative]; 3],
        &mut selected,
        &mut failed,
    );

    assert!(!possible);
    assert!(selected.is_empty());
    // Only the four occupied-node subsets are retained, not their permutations.
    assert_eq!(failed.len(), 4);

    failed.clear();

    let possible = select_placements(
        &[vec![&placement, &alternative], vec![]],
        &mut selected,
        &mut failed,
    );

    assert!(!possible);
    assert!(selected.is_empty());
    assert!(failed.is_empty());
}

/// Provides a validated synthetic profile and matching discovery results.
fn fixture() -> (
    FabricLayoutProfile,
    Vec<ObservedPlacement>,
    Vec<ObservedPlacement>,
    rm_v2::ScaleUpFabricConfig,
) {
    let nodes = |kind: &str, first_slot: u32| {
        (0..2)
            .map(|index| ObservedPlacement {
                node_id: format!("{kind}-{index}"),
                node_type: format!("{kind}_gb200_nvidia"),
                chassis_serial_number: "synthetic-chassis".into(),
                position: FabricPosition {
                    tray_index: index,
                    slot_number: first_slot + index,
                },
                physical_topology: (kind == "switch").then(|| "synthetic physical topology".into()),
            })
            .collect()
    };

    let mut document = test_support::document();

    let mut profile: FabricLayoutProfile =
        serde_json::from_value(document["profiles"][0].take()).unwrap();

    for rule in &mut profile.fabric_rules {
        rule.controller_rules = vec![FabricControllerRule {
            topology_type: "synthetic-fabric".into(),
            static_configs: vec![],
        }];
    }

    profile.validate().unwrap();

    (
        profile,
        nodes("switch", 21),
        nodes("compute", 1),
        rm_v2::ScaleUpFabricConfig {
            topology_type: "synthetic-fabric".into(),
            extra_static_configs: vec![],
        },
    )
}

#[test]
fn automatic_selection_is_stable_and_honors_a_later_primary_preference() {
    let (profile, mut switches, mut computes, config) = fixture();

    let requests = [
        SMALL.clone(),
        rm_v2::ScaleUpFabricSpec {
            primary_switch_node_id: Some("switch-0".into()),
            ..SMALL
        },
    ];

    let plan = plan_layout(&profile, &switches, &computes, &requests, &config).unwrap();

    assert_eq!(plan.fabrics[0].assignment.compute_node_ids, ["compute-1"]);

    assert_eq!(
        plan.fabrics[0].assignment.primary_switch_node_id,
        "switch-1"
    );

    assert_eq!(plan.fabrics[1].assignment.switch_node_ids, ["switch-0"]);
    assert!(!plan.full_topology);

    switches.reverse();
    computes.reverse();

    let reordered = plan_layout(&profile, &switches, &computes, &requests, &config).unwrap();

    assert_eq!(reordered.links, plan.links);

    for (actual, expected) in reordered.fabrics.iter().zip(&plan.fabrics) {
        assert_eq!(actual.assignment, expected.assignment);
        assert_eq!(actual.config, expected.config);
    }
}

#[test]
fn nine_groups_do_not_exhaust_search_before_a_late_primary_preference() {
    let (mut profile, mut switches, mut computes, config) = fixture();
    profile.compute_tray_count = 9;
    profile.switch_tray_count = 9;
    profile.fabric_rules[1].compute_tray_count = 9;
    profile.fabric_rules[1].switch_tray_count = 9;

    for nodes in [&mut switches, &mut computes] {
        let first = nodes[0].clone();

        nodes.extend((2..9).map(|index| ObservedPlacement {
            node_id: first.node_id.replace('0', &index.to_string()),
            position: FabricPosition {
                tray_index: index,
                slot_number: first.position.slot_number + index,
            },
            ..first.clone()
        }));
    }

    let mut requests = vec![SMALL; 9];
    requests[8].primary_switch_node_id = Some("switch-0".into());

    let plan = plan_layout(&profile, &switches, &computes, &requests, &config).unwrap();

    assert_eq!(plan.fabrics.len(), 9);

    assert_eq!(
        plan.fabrics[0].assignment.primary_switch_node_id,
        "switch-1"
    );

    assert_eq!(
        plan.fabrics[8].assignment.primary_switch_node_id,
        "switch-0"
    );

    // An over-capacity request must fail without enumerating all permutations.
    let requests = vec![SMALL; 10];

    let error = plan_layout(&profile, &switches, &computes, &requests, &config)
        .err()
        .unwrap();

    assert_eq!(
        error,
        "requested fabric compute count exceeds supplied inventory"
    );

    // Totals fit inventory, but not every request has a usable placement.
    for missing_group in [false, true] {
        let mut inventory = computes.clone();
        let mut requests = vec![SMALL; 9];

        if missing_group {
            inventory[8].position.tray_index = 9;
        } else {
            requests[8].primary_switch_node_id = Some("missing".into());
        }

        let error = plan_layout(&profile, &switches, &inventory, &requests, &config)
            .err()
            .unwrap();

        assert_eq!(
            error,
            "requested fabric memberships have no supported disjoint placement"
        );
    }
}

#[test]
fn aggregate_counts_are_checked_against_supplied_inventory() {
    let (profile, switches, computes, config) = fixture();

    for (compute_count, switch_count, supplied_computes, expected) in [
        (1, 1, 1, "compute"),
        (1, 2, 2, "switch"),
        (u32::MAX, 1, 2, "compute"),
        (1, u32::MAX, 2, "switch"),
    ] {
        let fabric = rm_v2::ScaleUpFabricSpec {
            compute_tray_count: compute_count,
            switch_tray_count: switch_count,
            primary_switch_node_id: None,
        };

        let requested = vec![fabric; 2];

        let error = plan_layout(
            &profile,
            &switches,
            &computes[..supplied_computes],
            &requested,
            &config,
        )
        .err()
        .unwrap();

        assert_eq!(
            error,
            format!("requested fabric {expected} count exceeds supplied inventory")
        );
    }
}

#[test]
fn compute_subset_uses_discovered_coordinates_and_disables_unassigned_switches() {
    let (profile, mut switches, mut computes, config) = fixture();

    for node in switches.iter_mut().chain(&mut computes) {
        node.position.tray_index += 8;
        node.position.slot_number += 100;
    }

    let plan = plan_layout(&profile, &switches, &computes[..1], &[SMALL], &config).unwrap();

    assert_eq!(plan.fabrics.len(), 1);
    assert_eq!(plan.fabrics[0].assignment.compute_node_ids, ["compute-0"]);
    assert_eq!(plan.fabrics[0].assignment.switch_node_ids, ["switch-0"]);
    assert_eq!(plan.links["switch-0"]["acp1"], FabricLinkState::Up);
    assert_eq!(plan.links["switch-0"]["fnm1"], FabricLinkState::Down);

    assert_eq!(
        plan.links["switch-1"],
        BTreeMap::from([
            ("acp1".into(), FabricLinkState::Down),
            ("fnm1".into(), FabricLinkState::Down),
        ])
    );
}

#[test]
fn invalid_inventory_fails_before_selection() {
    let (profile, switches, computes, config) = fixture();

    for case in [
        "missing switch",
        "extra compute",
        "duplicate ID",
        "empty ID",
        "empty serial",
        "foreign chassis",
        "duplicate tray",
        "shared slot",
        "switch type",
        "compute type",
        "topology",
    ] {
        let mut changed_switches = switches.clone();
        let mut changed_computes = computes.clone();

        match case {
            "missing switch" => {
                changed_switches.pop();
            }
            "extra compute" => changed_computes.push(computes[0].clone()),
            "duplicate ID" => changed_computes[0].node_id = switches[0].node_id.clone(),
            "empty ID" => changed_switches[0].node_id = " ".into(),
            "empty serial" => {
                for node in changed_switches.iter_mut().chain(&mut changed_computes) {
                    node.chassis_serial_number = " ".into();
                }
            }
            "foreign chassis" => changed_computes[0].chassis_serial_number = "other".into(),
            "duplicate tray" => changed_switches[1].position.tray_index = 0,
            "shared slot" => {
                changed_switches[0].position.slot_number = computes[0].position.slot_number
            }
            "switch type" => changed_switches[0].node_type = computes[0].node_type.clone(),
            "compute type" => changed_computes[0].node_type = switches[0].node_type.clone(),
            "topology" => changed_switches[0].physical_topology = None,
            _ => unreachable!(),
        }

        let error = plan_layout(
            &profile,
            &changed_switches,
            &changed_computes,
            &[SMALL],
            &config,
        )
        .err()
        .unwrap();

        assert!(
            error.contains("inventory") || error.contains("identity"),
            "{case}: {error}"
        );
    }
}

#[test]
fn invalid_requests_do_not_return_partial_plans() {
    let (profile, switches, computes, config) = fixture();

    let preferred = rm_v2::ScaleUpFabricSpec {
        primary_switch_node_id: Some("switch-0".into()),
        ..SMALL
    };

    for requests in [
        vec![],
        vec![rm_v2::ScaleUpFabricSpec::default()],
        vec![SMALL; 3],
        vec![rm_v2::ScaleUpFabricSpec {
            compute_tray_count: 3,
            ..SMALL
        }],
        vec![rm_v2::ScaleUpFabricSpec {
            primary_switch_node_id: Some("missing".into()),
            ..SMALL
        }],
        vec![preferred; 2],
    ] {
        let result = plan_layout(&profile, &switches, &computes, &requests, &config);

        assert!(result.is_err(), "{requests:?}");
    }

    for subset in [&computes[..0], &computes[..1]] {
        let result = plan_layout(&profile, &switches, subset, &[SMALL, SMALL], &config);

        assert!(result.is_err());
    }
}

#[test]
fn ranged_baselines_preserve_partial_and_full_layout_targets() {
    let (mut expanded, switches, computes, config) = fixture();
    expanded
        .full_link_states
        .insert("acp2".into(), FabricLinkState::Up);

    let mut input = serde_json::to_value(&expanded).unwrap();
    input["full_link_states"] = serde_json::json!({"acp1-2": "up", "fnm1": "down"});
    let ranged: FabricLayoutProfile = serde_json::from_value(input).unwrap();

    for spec in [
        SMALL,
        rm_v2::ScaleUpFabricSpec {
            compute_tray_count: 2,
            switch_tray_count: 2,
            primary_switch_node_id: None,
        },
    ] {
        let requested = [spec];
        let expected = plan_layout(&expanded, &switches, &computes, &requested, &config).unwrap();
        let actual = plan_layout(&ranged, &switches, &computes, &requested, &config).unwrap();

        assert_eq!(actual.links, expected.links);
        assert_eq!(actual.fabrics[0].assignment, expected.fabrics[0].assignment);
        assert_eq!(actual.full_topology, expected.full_topology);

        if actual.full_topology {
            assert!(
                actual
                    .links
                    .values()
                    .all(|states| states == &expanded.full_link_states)
            );
        } else {
            assert!(
                actual.links["switch-1"]
                    .values()
                    .all(|state| *state == FabricLinkState::Down)
            );

            assert_eq!(actual.links["switch-0"]["acp1"], FabricLinkState::Up);
            assert_eq!(actual.links["switch-0"]["acp2"], FabricLinkState::Down);
        }
    }
}

#[test]
fn full_capacity_restores_baseline_and_returns_optional_compute_inventory() {
    let (profile, mut switches, mut computes, config) = fixture();
    switches.reverse();
    computes.reverse();

    for (preference, expected) in [(None, "switch-0"), (Some("switch-1"), "switch-1")] {
        let requested = [rm_v2::ScaleUpFabricSpec {
            compute_tray_count: 2,
            switch_tray_count: 2,
            primary_switch_node_id: preference.map(str::to_owned),
        }];

        for inventory in [&computes[..0], &computes[..]] {
            let plan = plan_layout(&profile, &switches, inventory, &requested, &config).unwrap();

            assert!(plan.full_topology);
            assert_eq!(plan.fabrics.len(), 1);
            assert_eq!(plan.fabrics[0].assignment.primary_switch_node_id, expected);

            assert_eq!(
                plan.fabrics[0].assignment.switch_node_ids,
                ["switch-0", "switch-1"]
            );

            assert_eq!(
                plan.fabrics[0].assignment.compute_node_ids.len(),
                inventory.len()
            );

            assert!(
                plan.links
                    .values()
                    .all(|states| states == &profile.full_link_states)
            );
        }
    }

    let requested = [rm_v2::ScaleUpFabricSpec {
        compute_tray_count: 2,
        switch_tray_count: 2,
        primary_switch_node_id: None,
    }];

    let error = plan_layout(&profile, &switches, &computes[..1], &requested, &config)
        .err()
        .unwrap();

    assert!(error.contains("must be complete"));
}

#[test]
fn full_capacity_rejects_missing_switches_or_unknown_primary() {
    let (profile, switches, _, config) = fixture();

    for (inventory, preference) in [(&switches[..1], None), (&switches[..], Some("missing"))] {
        let requested = [rm_v2::ScaleUpFabricSpec {
            compute_tray_count: 2,
            switch_tray_count: 2,
            primary_switch_node_id: preference.map(str::to_owned),
        }];

        let result = plan_layout(&profile, inventory, &[], &requested, &config);

        assert!(result.is_err());
    }
}

#[test]
fn missing_remaps_and_invalid_link_targets_fail_planning() {
    let (profile, switches, computes, config) = fixture();

    for case in ["remap", "uncontrolled"] {
        let mut changed = profile.clone();

        if case == "remap" {
            changed.fabric_rules[0].grouping = serde_json::from_value(serde_json::json!({
                "compute": {"node": "compute", "coordinate": "tray_index", "divisor": 1, "multiplier": 1},
                "switch": {"node": "switch", "coordinate": "tray_index", "divisor": 1, "remap": {"0": 0}, "multiplier": 1}
            })).unwrap();
        } else {
            changed.link_calculation = serde_json::from_value(serde_json::json!({
                "default_state": "down", "rules": [{"prefix": "acp", "base": 2,
                "terms": [], "offsets": [0], "width": 1, "state": "up"}]
            }))
            .unwrap();
        }

        let error = plan_layout(&changed, &switches, &computes, &[SMALL], &config)
            .err()
            .unwrap();

        assert!(error.contains(case), "{error}");
    }
}

#[test]
fn controller_rules_set_remove_and_restore_without_changing_unmanaged_settings() {
    let (mut profile, switches, computes, mut config) = fixture();

    for (rule, action) in profile.fabric_rules.iter_mut().zip([
        FabricStaticConfigAction::Remove {},
        FabricStaticConfigAction::Set {
            value: "restored".into(),
        },
    ]) {
        rule.controller_rules[0].static_configs = vec![
            FabricStaticConfigTarget {
                config_file_name: "fm_config".into(),
                key: "synthetic_zero".into(),
                action: FabricStaticConfigAction::Set { value: "0".into() },
            },
            FabricStaticConfigTarget {
                config_file_name: "fm_config".into(),
                key: "synthetic_removed".into(),
                action,
            },
        ];
    }

    config
        .extra_static_configs
        .push(rm::ScaleUpFabricStaticConfig {
            config_file_name: "other_file".into(),
            key: "UNMANAGED".into(),
            value: "caller-value".into(),
        });

    let layout = plan_layout(&profile, &switches, &computes, &[SMALL, SMALL], &config).unwrap();

    let full = [rm_v2::ScaleUpFabricSpec {
        compute_tray_count: 2,
        switch_tray_count: 2,
        primary_switch_node_id: None,
    }];

    let restored = plan_layout(&profile, &switches, &[], &full, &config).unwrap();

    for fabric in &layout.fabrics {
        assert_eq!(fabric.config.extra_static_configs[0].key, "synthetic_zero");
        assert_eq!(fabric.config.extra_static_configs[0].value, "0");

        assert_eq!(
            fabric.config.extra_static_configs[1],
            config.extra_static_configs[0]
        );

        assert_eq!(
            fabric.absent_static_configs,
            BTreeSet::from([("fm_config".into(), "synthetic_removed".into())])
        );
    }

    assert!(restored.fabrics[0].absent_static_configs.is_empty());

    assert_eq!(
        restored.fabrics[0].config.extra_static_configs[0].value,
        "restored"
    );

    for (key, value, accepted) in [
        ("synthetic_zero", "0", true),
        ("synthetic_zero", "1", false),
        ("synthetic_removed", "0", false),
    ] {
        let mut supplied = config.clone();
        supplied
            .extra_static_configs
            .push(rm::ScaleUpFabricStaticConfig {
                config_file_name: "fm_config".into(),
                key: key.into(),
                value: value.into(),
            });

        let result = plan_layout(&profile, &switches, &computes, &[SMALL], &supplied);

        assert_eq!(result.is_ok(), accepted, "{key}");

        if let Ok(plan) = result {
            assert_eq!(plan.fabrics[0].config, layout.fabrics[0].config);
        }
    }
}

#[test]
fn topology_matching_is_exact_and_link_only_profiles_cannot_configure_controllers() {
    let (mut profile, switches, computes, config) = fixture();

    for topology in ["SYNTHETIC-FABRIC", "synthetic-fabric-extra", "unlisted"] {
        let requested = rm_v2::ScaleUpFabricConfig {
            topology_type: topology.into(),
            ..config.clone()
        };

        let error = plan_layout(&profile, &switches, &computes, &[SMALL], &requested)
            .err()
            .unwrap();

        assert!(error.contains("no controller rule"), "{error}");
    }

    for rule in &mut profile.fabric_rules {
        rule.controller_rules.clear();
    }

    let error = plan_layout(&profile, &switches, &computes, &[SMALL], &config)
        .err()
        .unwrap();

    assert!(error.contains("no controller rule"));
}

#[test]
fn caller_static_configs_are_validated_before_building_a_plan() {
    let (profile, switches, computes, config) = fixture();

    for (file, key, value) in [
        ("../file", "KEY", "1"),
        ("fm_config", "1KEY", "1"),
        ("fm_config", "KEY", "line\nline"),
        ("fm_config", "KEY", "1#comment"),
        ("fm_config", "mnnvl_topology", "other"),
        ("fm_config", "KEY", ""),
    ] {
        let mut requested = config.clone();
        requested
            .extra_static_configs
            .push(rm::ScaleUpFabricStaticConfig {
                config_file_name: file.into(),
                key: key.into(),
                value: value.into(),
            });

        let result = plan_layout(&profile, &switches, &computes, &[SMALL], &requested);

        assert!(result.is_err(), "{file}/{key}");
    }

    let mut requested = config.clone();
    requested.extra_static_configs = ["key", " key "]
        .map(|key| rm::ScaleUpFabricStaticConfig {
            config_file_name: "fm_config".into(),
            key: key.into(),
            value: "1".into(),
        })
        .into();

    let error = plan_layout(&profile, &switches, &computes, &[SMALL], &requested)
        .err()
        .unwrap();

    assert!(error.contains("duplicate"));

    requested.extra_static_configs = [("key", "lower"), ("KEY", "upper")]
        .map(|(key, value)| rm::ScaleUpFabricStaticConfig {
            config_file_name: "other.cfg".into(),
            key: key.into(),
            value: value.into(),
        })
        .into();

    let plan = plan_layout(&profile, &switches, &computes, &[SMALL], &requested).unwrap();

    let values: BTreeMap<_, _> = plan.fabrics[0]
        .config
        .extra_static_configs
        .iter()
        .map(|entry| (entry.key.as_str(), entry.value.as_str()))
        .collect();

    assert_eq!(values.get("key"), Some(&"lower"));
    assert_eq!(values.get("KEY"), Some(&"upper"));
}
