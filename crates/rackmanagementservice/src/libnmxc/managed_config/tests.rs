/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Readback, preservation, and idempotency checks for managed settings.

use std::collections::VecDeque;

use super::*;
use crate::libnmxc::test_support::FakeNmxc;
use crate::libnmxc::{NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY, NmxcError};

/// Two files let tests distinguish incomplete readback from an absent key.
fn fixture_config() -> ManagedConfig {
    ManagedConfig::try_from(BTreeMap::from([
        (("engine.cfg".into(), "MODE".into()), Some("enabled".into())),
        (
            ("managed.cfg".into(), "owned".into()),
            Some("expected".into()),
        ),
    ]))
    .unwrap()
}

fn complete_response(owned_value: &str) -> nmxc_model::NmxGetStaticConfigResponse {
    let topology = "MODE=enabled\n".to_owned();
    let managed = format!("owned={owned_value}\nunowned=preserved\n");

    FakeNmxc::static_config_files_response(&[("engine.cfg", &topology), ("managed.cfg", &managed)])
}

#[tokio::test]
async fn matching_configuration_repeat_performs_no_writes() {
    let config = fixture_config();
    let response = complete_response("expected");

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([
            Ok(response.clone()),
            Ok(response.clone()),
            Ok(response.clone()),
            Ok(response),
        ]),
        ..Default::default()
    };

    let first = reconcile_config(&mut client, &config, "gateway").await;
    let repeated = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(first, Ok(false));
    assert_eq!(repeated, Ok(false));
    assert!(client.set_calls.is_empty());
    assert!(client.get_static_config_results.is_empty());
}

#[tokio::test]
async fn unavailable_initial_configuration_read_performs_no_writes() {
    let config = fixture_config();

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([Err(NmxcError::Status(
            tonic::Status::unavailable("read failed"),
        ))]),
        ..Default::default()
    };

    let result = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(
        result,
        Err("managed configuration read unavailable".to_owned())
    );

    assert!(client.set_calls.is_empty());
}

#[tokio::test]
async fn incomplete_initial_configuration_read_performs_no_writes() {
    let config = fixture_config();

    for response in [
        FakeNmxc::empty_static_config_response(),
        FakeNmxc::static_config_files_response(&[("managed.cfg", "owned=old")]),
        FakeNmxc::static_config_files_response(&[
            ("engine.cfg", ""),
            ("managed.cfg", "owned=old"),
            ("managed.cfg", "owned=old"),
        ]),
    ] {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([Ok(response)]),
            ..Default::default()
        };

        let result = reconcile_config(&mut client, &config, "gateway").await;

        assert!(result.is_err());
        assert!(client.set_calls.is_empty());
    }
}

#[tokio::test]
async fn timed_out_configuration_write_requires_authoritative_readback() {
    let config = fixture_config();

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([
            Ok(complete_response("old")),
            Ok(complete_response("expected")),
            Ok(complete_response("expected")),
        ]),
        set_results: VecDeque::from([Err(NmxcError::Status(tonic::Status::deadline_exceeded(
            "write acknowledgement unavailable",
        )))]),
        ..Default::default()
    };

    let result = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(result, Ok(true));

    assert_eq!(
        client.set_calls,
        [(
            "managed.cfg".to_owned(),
            "owned".to_owned(),
            "expected".to_owned(),
            "gateway".to_owned()
        )]
    );
}

#[tokio::test]
async fn rejected_configuration_readback_stops_the_batch() {
    let config = fixture_config();

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([
            Ok(complete_response("old")),
            Ok(complete_response("old")),
        ]),
        ..Default::default()
    };

    let result = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(
        result,
        Err("managed configuration write did not pass readback".to_owned())
    );

    assert_eq!(client.set_calls.len(), 1);
    assert!(client.get_static_config_results.is_empty());
}

#[test]
fn complete_file_absence_is_drift_but_duplicate_managed_entries_are_ambiguous() {
    let config = fixture_config();
    let desired = &config.0;

    let missing = FakeNmxc::static_config_files_response(&[
        ("engine.cfg", ""),
        ("managed.cfg", "unowned=preserved"),
    ]);

    let current = complete_config(&missing, desired).unwrap();

    assert!(current.values.values().all(Option::is_none));
    assert!(!config_matches(&current, &config));

    let topology = "MODE=enabled\n".to_owned();

    let duplicate = FakeNmxc::static_config_files_response(&[
        ("engine.cfg", &topology),
        ("managed.cfg", "owned=expected\nowned=old\n"),
    ]);

    assert_eq!(
        complete_config(&duplicate, desired),
        Err("controller returned an ambiguous managed configuration entry".to_owned())
    );
}

#[tokio::test]
async fn already_absent_configuration_repeat_performs_no_writes() {
    let mut config = fixture_config();
    config
        .0
        .insert(("managed.cfg".into(), "owned".into()), None);

    let response = FakeNmxc::static_config_files_response(&[
        ("engine.cfg", "MODE=enabled\n"),
        ("managed.cfg", "unowned=preserved\n"),
    ]);

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([
            Ok(response.clone()),
            Ok(response.clone()),
            Ok(response.clone()),
            Ok(response),
        ]),
        ..Default::default()
    };

    assert_eq!(
        reconcile_config(&mut client, &config, "gateway").await,
        Ok(false)
    );

    assert_eq!(
        reconcile_config(&mut client, &config, "gateway").await,
        Ok(false)
    );

    assert!(client.set_calls.is_empty());
    assert!(client.set_file_calls.is_empty());
    assert!(client.get_static_config_results.is_empty());
}

#[tokio::test]
async fn managed_key_removals_reconcile_and_repeat_without_writes() {
    let mut config = fixture_config();
    config
        .0
        .insert(("managed.cfg".into(), "owned".into()), None);

    let fresh =
        "# retained\r\nowned = 'old'\r\nOWNED=keep\r\nunowned = new\r\n; retained\nTAIL=\u{3b1}";

    let expected = "# retained\r\nOWNED=keep\r\nunowned = new\r\n; retained\nTAIL=\u{3b1}";
    let topology = "MODE=enabled\n";

    let before =
        FakeNmxc::static_config_files_response(&[("engine.cfg", topology), ("managed.cfg", fresh)]);

    let after = FakeNmxc::static_config_files_response(&[
        ("engine.cfg", topology),
        ("managed.cfg", expected),
    ]);

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([
            Ok(complete_response("old")),
            Ok(before),
            Ok(after.clone()),
            Ok(after.clone()),
            Ok(after.clone()),
            Ok(after),
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
    assert!(client.set_calls.is_empty());

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

#[tokio::test]
async fn file_readback_accepts_reordered_records_without_changing_record_text() {
    let mut config = fixture_config();
    config
        .0
        .insert(("managed.cfg".into(), "owned".into()), None);

    let original = "owned=old\n# retained\nunowned=preserved\n\nTAIL='Keep This'\n";
    let expected = "# retained\nunowned=preserved\n\nTAIL='Keep This'\n";
    let reordered = "TAIL='Keep This'\n\nunowned=preserved\n# retained\n";
    let topology = "MODE=enabled\n";

    let before = FakeNmxc::static_config_files_response(&[
        ("engine.cfg", topology),
        ("managed.cfg", original),
    ]);

    let after = FakeNmxc::static_config_files_response(&[
        ("engine.cfg", topology),
        ("managed.cfg", reordered),
    ]);

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([
            Ok(before.clone()),
            Ok(before),
            Ok(after.clone()),
            Ok(after),
        ]),
        set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
        ..Default::default()
    };

    let result = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(result, Ok(true));
    assert!(client.set_calls.is_empty());

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

#[tokio::test]
async fn ambiguous_or_unsupported_configuration_performs_no_writes() {
    let config = fixture_config();

    for content in [
        "owned=expected\nowned=old\n",
        "owned=old\\\n continuation\n",
        "owned='unterminated\n",
        "owned=old # ambiguous\n",
        "unowned=escaped\\\n continuation\nowned=old\n",
        "[section]\nowned=old\n",
    ] {
        let response = FakeNmxc::static_config_files_response(&[
            ("engine.cfg", "MODE=enabled\n"),
            ("managed.cfg", content),
        ]);

        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([Ok(response)]),
            ..Default::default()
        };

        assert!(
            reconcile_config(&mut client, &config, "gateway")
                .await
                .is_err()
        );

        assert!(client.set_calls.is_empty());
        assert!(client.set_file_calls.is_empty());
    }
}

#[tokio::test]
async fn file_readback_rejects_changed_records_and_stale_owned_keys() {
    let mut config = fixture_config();
    config
        .0
        .insert(("managed.cfg".into(), "owned".into()), None);

    for content in [
        "",
        "unowned=changed\n",
        "UNOWNED=preserved\n",
        " unowned=preserved\n",
        "unowned='preserved'\n",
        "unowned=preserved\n\n",
        "unowned=preserved\n# added\n",
        "unowned=preserved\nextra=value\n",
        "owned=old\nunowned=preserved\n",
    ] {
        let mut client = FakeNmxc {
            get_static_config_results: VecDeque::from([
                Ok(complete_response("old")),
                Ok(complete_response("old")),
                Ok(FakeNmxc::static_config_files_response(&[
                    ("engine.cfg", "MODE=enabled\n"),
                    ("managed.cfg", content),
                ])),
            ]),
            set_results: VecDeque::from([Ok(FakeNmxc::success_return_code())]),
            ..Default::default()
        };

        let result = reconcile_config(&mut client, &config, "gateway").await;

        assert_eq!(
            result,
            Err("managed configuration file write did not pass preservation readback".to_owned()),
            "returned content: {content:?}"
        );

        assert_eq!(client.set_file_calls.len(), 1);
        assert!(client.set_calls.is_empty());
    }
}

#[tokio::test]
async fn separate_file_deletions_preserve_each_fresh_snapshot() {
    let mut config = fixture_config();
    config
        .0
        .insert(("managed.cfg".into(), "owned".into()), None);

    config.0.insert(("second.cfg".into(), "owned".into()), None);

    let mut responses = VecDeque::new();

    for (first, second) in [
        ("owned=old\nunowned=first\n", "owned=old\nunowned=second\n"),
        ("owned=old\nunowned=first\n", "owned=old\nunowned=second\n"),
        ("unowned=first\n", "owned=old\nunowned=second\n"),
        ("unowned=first\n", "owned=old\nunowned=fresh\n"),
        ("unowned=first\n", "unowned=fresh\n"),
        ("unowned=first\n", "unowned=fresh\n"),
    ] {
        responses.push_back(Ok(FakeNmxc::static_config_files_response(&[
            ("engine.cfg", "MODE=enabled\n"),
            ("managed.cfg", first),
            ("second.cfg", second),
        ])));
    }

    let mut client = FakeNmxc {
        get_static_config_results: responses,
        ..Default::default()
    };

    let result = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(result, Ok(true));

    assert_eq!(
        client.set_file_calls,
        [
            (
                "managed.cfg".to_owned(),
                "unowned=first\n".to_owned(),
                "gateway".to_owned()
            ),
            (
                "second.cfg".to_owned(),
                "unowned=fresh\n".to_owned(),
                "gateway".to_owned()
            ),
        ]
    );

    assert!(client.set_calls.is_empty());
    assert!(client.get_static_config_results.is_empty());
}

#[test]
fn file_replacement_preserves_retained_records_without_adding_values() {
    let config = ManagedConfig::try_from(BTreeMap::from([
        (("engine.cfg".into(), "MODE".into()), Some("enabled".into())),
        (("engine.cfg".into(), "owned".into()), None),
    ]))
    .unwrap();

    let replacement = replacement_config_file(
        "engine.cfg",
        "# keep\nowned=old\nunowned=preserved",
        &config.0,
    )
    .unwrap();

    assert_eq!(replacement, "# keep\nunowned=preserved");
}

#[tokio::test]
async fn mixed_removals_and_sets_preserve_syntax_until_key_value_updates() {
    let config = ManagedConfig::try_from(BTreeMap::from([
        (("engine.cfg".into(), "obsolete".into()), None),
        (("engine.cfg".into(), "changed".into()), Some("new".into())),
        (
            ("engine.cfg".into(), "missing".into()),
            Some("added".into()),
        ),
    ]))
    .unwrap();

    for separator in [" ", "="] {
        let retained = format!("# keep\r\nchanged{separator}old\r\nunowned{separator}preserved");
        let initial = format!("obsolete{separator}old\r\n{retained}");
        let updated = retained.replace("old", "new");
        let complete = format!("{updated}\r\nmissing{separator}added\r\n");

        let mut client = FakeNmxc {
            get_static_config_results: [
                &initial, &initial, &retained, &updated, &complete, &complete, &complete, &complete,
            ]
            .into_iter()
            .map(|content| {
                Ok(FakeNmxc::static_config_files_response(&[(
                    "engine.cfg",
                    content,
                )]))
            })
            .collect(),
            ..Default::default()
        };

        let first = reconcile_config(&mut client, &config, "gateway").await;
        let repeated = reconcile_config(&mut client, &config, "gateway").await;

        assert_eq!(first, Ok(true));
        assert_eq!(repeated, Ok(false));

        assert_eq!(
            client.set_file_calls,
            [("engine.cfg".to_owned(), retained, "gateway".to_owned())]
        );

        assert_eq!(
            client.set_calls,
            [("changed", "new"), ("missing", "added")].map(|(key, value)| (
                "engine.cfg".to_owned(),
                key.to_owned(),
                value.to_owned(),
                "gateway".to_owned(),
            ))
        );

        assert!(client.get_static_config_results.is_empty());
    }
}

#[test]
fn equals_inside_a_value_is_not_the_key_separator() {
    let config = ManagedConfig::try_from(BTreeMap::from([(
        ("engine.cfg".into(), "owned".into()),
        Some("a=b".into()),
    )]))
    .unwrap();

    for separator in ["=", " ", " = ", "\t"] {
        let content = format!("owned{separator}a=b\n");
        let response = FakeNmxc::static_config_files_response(&[("engine.cfg", &content)]);

        let snapshot = complete_config(&response, &config.0).unwrap();

        assert!(config_matches(&snapshot, &config));
    }
}

#[test]
fn target_validation_preserves_name_and_literal_policy() {
    for (file, key, value) in [
        ("engine.cfg", "lower_case", Some("0")),
        ("Engine-1.cfg", "UPPER_CASE", Some("two words")),
        ("engine.cfg", "_key2", Some("a=b")),
        (NMX_C_FM_CONFIG_FILE, NMX_C_TOPOLOGY_KEY, Some("topology")),
        ("engine.cfg", "absent", None),
    ] {
        let entries = BTreeMap::from([((file.into(), key.into()), value.map(str::to_owned))]);
        let config = ManagedConfig::try_from(entries.clone()).unwrap();

        assert_eq!(config.0, entries);
    }

    for (file, key, value) in [
        ("", "KEY", Some("value")),
        ("../engine.cfg", "KEY", Some("value")),
        (".", "KEY", None),
        ("..", "KEY", None),
        (".engine.cfg", "KEY", None),
        ("dir\\engine.cfg", "KEY", None),
        ("engine cfg", "KEY", None),
        ("engine.cfg", "", None),
        ("engine.cfg", "1KEY", None),
        ("engine.cfg", "KEY\nOTHER", None),
        ("engine.cfg", "KEY", Some("")),
        ("engine.cfg", "KEY", Some(" value")),
        ("engine.cfg", "KEY", Some("value ")),
        ("engine.cfg", "KEY", Some("value\nINJECTED=1")),
        ("engine.cfg", "KEY", Some("value\t")),
        ("engine.cfg", "KEY", Some("'quoted'")),
        ("engine.cfg", "KEY", Some("\"quoted\"")),
        ("engine.cfg", "KEY", Some("escaped\\")),
        ("engine.cfg", "KEY", Some("value # comment")),
        ("engine.cfg", "KEY", Some("value;comment")),
    ] {
        let entries = BTreeMap::from([((file.into(), key.into()), value.map(str::to_owned))]);

        assert!(ManagedConfig::try_from(entries).is_err());
    }
}

#[tokio::test]
async fn reconciliation_preserves_sm_and_fm_key_casing() {
    for (file, key, other_key) in [
        ("sm_config", "log_flags", "LOG_FLAGS"),
        ("fm_config", "LOG_LEVEL", "log_level"),
    ] {
        let config = ManagedConfig::try_from(BTreeMap::from([
            ((file.into(), key.into()), Some("7".into())),
            ((file.into(), other_key.into()), Some("3".into())),
        ]))
        .unwrap();

        let initial = format!("{key} 1\n{other_key} 3\n");
        let updated = format!("{key} 7\n{other_key} 3\n");

        let mut client = FakeNmxc {
            get_static_config_results: [&initial, &updated, &updated, &updated, &updated]
                .into_iter()
                .map(|content| Ok(FakeNmxc::static_config_files_response(&[(file, content)])))
                .collect(),
            ..Default::default()
        };

        let first = reconcile_config(&mut client, &config, "gateway").await;
        let repeated = reconcile_config(&mut client, &config, "gateway").await;

        assert_eq!(first, Ok(true));
        assert_eq!(repeated, Ok(false));

        assert_eq!(
            client.set_calls,
            [(
                file.to_owned(),
                key.to_owned(),
                "7".to_owned(),
                "gateway".to_owned()
            )]
        );

        assert!(client.set_file_calls.is_empty());
        assert!(client.get_static_config_results.is_empty());
    }
}

#[tokio::test]
async fn readback_with_different_key_casing_does_not_satisfy_a_target() {
    let config = ManagedConfig::try_from(BTreeMap::from([(
        ("sm_config".into(), "log_flags".into()),
        Some("7".into()),
    )]))
    .unwrap();

    let response = FakeNmxc::static_config_files_response(&[("sm_config", "LOG_FLAGS 7\n")]);

    let mut client = FakeNmxc {
        get_static_config_results: VecDeque::from([Ok(response.clone()), Ok(response)]),
        ..Default::default()
    };

    let result = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(
        result,
        Err("managed configuration write did not pass readback".into())
    );

    assert_eq!(
        client.set_calls,
        [(
            "sm_config".into(),
            "log_flags".into(),
            "7".into(),
            "gateway".into()
        )]
    );
}

#[tokio::test]
async fn empty_targets_do_not_read_or_write_the_controller() {
    let config = ManagedConfig::try_from(BTreeMap::new()).unwrap();
    let mut client = FakeNmxc::default();

    let result = reconcile_config(&mut client, &config, "gateway").await;

    assert_eq!(result, Ok(false));
    assert!(client.get_static_config_file_calls.is_empty());
    assert!(client.set_calls.is_empty());
    assert!(client.set_file_calls.is_empty());
}

#[test]
fn unrequested_absence_is_not_evidence_that_a_different_target_matches() {
    let config = fixture_config();
    let snapshot = complete_config(&complete_response("expected"), &config.0).unwrap();

    let different = ManagedConfig::try_from(BTreeMap::from([(
        ("managed.cfg".into(), "UNREAD".into()),
        None,
    )]))
    .unwrap();

    assert!(config_matches(&snapshot, &config));
    assert!(!config_matches(&snapshot, &different));
}
