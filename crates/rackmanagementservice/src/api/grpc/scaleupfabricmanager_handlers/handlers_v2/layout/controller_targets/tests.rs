/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Static configuration and controller-readiness regression tests.

use std::collections::{BTreeSet, VecDeque};

use super::super::planner::ObservedPlacement;
use super::*;
use crate::config::fabric_layout::FabricPosition;
use crate::libnmxc::NmxcError;
use crate::libnmxc::managed_config::reconcile_config;
use crate::libnmxc::test_support::FakeNmxc;

use librms::protos::rack_manager as rm;
use librms::protos::rack_manager_v2 as rm_v2;

/// One owned setting for activation-digest comparisons.
fn fixture_config() -> PlannedFabric {
    PlannedFabric {
        assignment: Default::default(),
        absent_static_configs: BTreeSet::new(),
        config: rm_v2::ScaleUpFabricConfig {
            topology_type: "layout-a".to_owned(),
            extra_static_configs: vec![rm::ScaleUpFabricStaticConfig {
                config_file_name: "managed.cfg".to_owned(),
                key: "owned".to_owned(),
                value: "expected".to_owned(),
            }],
        },
    }
}

#[tokio::test]
async fn planned_sets_and_removals_preserve_key_casing_and_repeat_without_writes() {
    let profile = serde_json::from_value(serde_json::json!({
        "switch_node_types": ["switch_gb200_nvidia"],
        "compute_node_types": ["compute_gb200_nvidia"],
        "physical_topology": "synthetic",
        "compute_tray_count": 1, "switch_tray_count": 1,
        "full_link_states": {"acp1": "up"},
        "fabric_rules": [{
            "compute_tray_count": 1, "switch_tray_count": 1,
            "controller_rules": [{"topology_type": "layout-a",
            "static_configs": [
                {"config_file_name": "managed.cfg", "key": "owned",
                 "action": {"type": "remove"}},
                {"config_file_name": "sm_config", "key": "log_flags",
                 "action": {"type": "set", "value": "7"}}
            ]}]
        }],
        "link_calculation": {"default_state": "down", "rules": [{
            "prefix": "acp", "base": 1, "terms": [], "offsets": [0], "width": 1, "state": "up"
        }]}
    }))
    .unwrap();

    let observed = ObservedPlacement {
        node_id: "switch-a".into(),
        node_type: "switch_gb200_nvidia".into(),
        chassis_serial_number: "synthetic".into(),
        position: FabricPosition {
            tray_index: 0,
            slot_number: 1,
        },
        physical_topology: Some("synthetic".into()),
    };

    let request = rm_v2::ScaleUpFabricConfig {
        topology_type: "layout-a".into(),
        extra_static_configs: vec![rm::ScaleUpFabricStaticConfig {
            config_file_name: "sm_config".into(),
            key: "log_flags".into(),
            value: "7".into(),
        }],
    };

    let fabrics = [rm_v2::ScaleUpFabricSpec {
        compute_tray_count: 1,
        switch_tray_count: 1,
        ..Default::default()
    }];

    let plan =
        super::super::planner::plan_layout(&profile, &[observed], &[], &fabrics, &request).unwrap();

    let config = ManagedConfig::try_from(&plan.fabrics[0]).unwrap();

    let fresh = "# retained\r\nowned = 'old'\r\nOWNED=keep\r\nunowned = new\r\n";
    let expected = "# retained\r\nOWNED=keep\r\nunowned = new\r\n";
    let topology = "MNNVL_TOPOLOGY=layout-a\n";

    let before = FakeNmxc::static_config_files_response(&[
        (NMX_C_FM_CONFIG_FILE, topology),
        ("managed.cfg", fresh),
        ("sm_config", "log_flags 3\nLOG_FLAGS 9\n"),
    ]);

    let after = FakeNmxc::static_config_files_response(&[
        (NMX_C_FM_CONFIG_FILE, topology),
        ("managed.cfg", expected),
        ("sm_config", "log_flags 3\nLOG_FLAGS 9\n"),
    ]);

    let configured = FakeNmxc::static_config_files_response(&[
        (NMX_C_FM_CONFIG_FILE, topology),
        ("managed.cfg", expected),
        ("sm_config", "log_flags 7\nLOG_FLAGS 9\n"),
    ]);

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([
            Ok(before.clone()),
            Ok(before),
            Ok(after),
            Ok(configured.clone()),
            Ok(configured.clone()),
            Ok(configured.clone()),
            Ok(configured),
        ]),
        set_results: VecDeque::from([Err(NmxcError::Status(tonic::Status::deadline_exceeded(
            "acknowledgement unavailable",
        )))]),
        ..Default::default()
    };

    let result = reconcile_config(&mut client, &config, "gateway").await;
    let repeated = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(result, Ok(true));
    assert_eq!(repeated, Ok(false));

    assert_eq!(
        client.set_calls,
        [(
            "sm_config".to_owned(),
            "log_flags".to_owned(),
            "7".to_owned(),
            "gateway".to_owned()
        )]
    );

    assert_eq!(
        client.set_file_calls,
        [(
            "managed.cfg".to_owned(),
            expected.to_owned(),
            "gateway".to_owned()
        )]
    );

    assert!(client.get_static_config_results.is_empty());
}

#[test]
fn activation_digest_distinguishes_absence_and_present_values() {
    let mut config = fixture_config();
    let present = config_digest(&config).unwrap();

    config.config.extra_static_configs.clear();
    let unowned = config_digest(&config).unwrap();
    config
        .absent_static_configs
        .insert(("managed.cfg".to_owned(), "owned".to_owned()));

    let absent = config_digest(&config).unwrap();

    assert_ne!(present, absent);
    assert_ne!(unowned, absent);

    config.absent_static_configs.clear();
    config
        .absent_static_configs
        .insert(("managed.cfg".to_owned(), "OWNED".to_owned()));

    assert_ne!(config_digest(&config).unwrap(), absent);
}
