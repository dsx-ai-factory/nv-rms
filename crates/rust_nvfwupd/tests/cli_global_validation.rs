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

use std::process::{Command, Output};

fn run_nvfwupd(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_nvfwupd"))
        .args(args)
        .output()
        .expect("failed to run nvfwupd test binary")
}

#[test]
fn unknown_globals_are_rejected_before_early_commands() {
    let cases: &[&[&str]] = &[
        &["-z", "version"],
        &["-x", "version"],
        &["-z", "help"],
        &["-z"],
    ];

    for args in cases {
        let output = run_nvfwupd(args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "args {:?} unexpectedly succeeded: stdout={}, stderr={}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("nvfwupd: error: unrecognized arguments:"),
            "args {:?} did not report argparse-style unknown globals: {}",
            args,
            stderr
        );
    }
}

#[test]
fn top_level_help_aliases_use_help_path() {
    for args in [&["-h"][..], &["--help"][..]] {
        let output = run_nvfwupd(args);
        assert_eq!(
            output.status.code(),
            Some(1),
            "args {:?} did not use help-on-error path: stdout={}, stderr={}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stdout.contains("Usage:"));
        assert!(!stderr.contains("unrecognized arguments"));
    }
}

#[test]
fn early_commands_reject_trailing_unknown_flags() {
    for args in [&["version", "-z"][..], &["help", "-z"][..]] {
        let output = run_nvfwupd(args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "args {:?} unexpectedly succeeded: stdout={}, stderr={}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("nvfwupd: error: unrecognized arguments: -z"),
            "args {:?} did not reject trailing unknown flag: {}",
            args,
            stderr
        );
    }
}

#[test]
fn top_level_version_alias_still_bypasses_command_schema() {
    let output = run_nvfwupd(&["--version"]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "--version failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("nvfwupd version"));
}
