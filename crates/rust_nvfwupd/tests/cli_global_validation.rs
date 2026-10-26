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
