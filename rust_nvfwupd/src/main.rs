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

//! CLI provides interface to update firmware components
//! through BMC using Redfish protocol

use std::process;

use nvfwupd::cli_format::{rolling_file_layer, stdout_layer};
use nvfwupd::cli_schema::CLISchema;
use nvfwupd::set_exit_requested;
use nvfwupd::updcommand::{dispatch_command, validate_global_options, FwUpdCmdHelp};
use nvfwupd::util::{BailAction, Util};
use nvfwupd::version;
use tracing_subscriber::{prelude::*, util::SubscriberInitExt};

fn get_arguments(schema: &CLISchema) -> (String, Vec<String>, Vec<String>) {
    let args: Vec<String> = std::env::args().collect();
    let exec_name = args[0].rsplit('/').next().unwrap_or(&args[0]).to_string();
    let mut global_options = Vec::new();

    // Keep top-level version aliases out of global-option parsing while still
    // validating globals before schema commands such as `version` and `help`.
    if args.len() > 1 && (args[1] == "-V" || args[1] == "--version") {
        return (exec_name, global_options, vec![args[1].clone()]);
    }

    let cmd_list = schema.get_command_list();

    let mut index = 1;
    for arg in args.iter().skip(1) {
        if cmd_list.contains(arg) {
            break;
        }
        global_options.push(arg.clone());
        index += 1;
    }

    let cmd_args = args[index..].to_vec();

    (exec_name, global_options, cmd_args)
}

fn check_duplicate_item(items: &[String]) -> bool {
    let mut seen = std::collections::HashSet::new();
    for item in items {
        if !seen.insert(item) {
            return true;
        }
    }
    false
}

fn verbose_log_file(global_options: &[String]) -> Option<String> {
    let mut iter = global_options.iter();
    while let Some(opt) = iter.next() {
        if opt == "-v" || opt == "--verbose" {
            return match iter.next() {
                Some(path) if !path.starts_with('-') => Some(path.clone()),
                _ => Some("nvfwupd_log.txt".to_string()),
            };
        }
    }
    None
}

fn validate_verbose_log_file(log_file: &str) {
    if log_file.ends_with('/')
        || std::fs::metadata(log_file)
            .map(|metadata| metadata.is_dir())
            .unwrap_or(false)
    {
        Util::bail_nvfwupd(
            1,
            &format!("Invalid log file path. {} is a directory.", log_file),
            BailAction::Exit,
            None,
        );
    }
}

fn init_cli_tracing(global_options: &[String]) {
    if let Some(log_file) = verbose_log_file(global_options) {
        validate_verbose_log_file(&log_file);
        let file_layer = rolling_file_layer(&log_file);
        let subscriber = tracing_subscriber::registry()
            .with(stdout_layer())
            .with(file_layer);
        let _ = subscriber.try_init();
    } else {
        let subscriber = tracing_subscriber::registry().with(stdout_layer());
        let _ = subscriber.try_init();
    }
}

#[tokio::main]
async fn main() {
    // Set up Ctrl+C handler
    tokio::spawn(async {
        let mut exit_requested = false;
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                break;
            }
            if exit_requested {
                eprintln!("\nForce exiting...");
                process::exit(1);
            }
            exit_requested = true;
            set_exit_requested(true);
            eprintln!("\nCtrl-C was pressed. Press again to force exit.");
        }
    });

    // Load the CLI schema from the compile-time-embedded YAML so the binary
    // is fully standalone at runtime — matches Python's pyinstaller build
    // where `cli_schema.yaml` is bundled into the single-file executable.
    // This is a hard dependency: the source file `src/cli_schema.yaml` must
    // exist at build time or `cargo build` fails with a clear error.
    let mut schema = CLISchema::new();
    if let Err(e) = schema.load_embedded_schema() {
        Util::bail_nvfwupd(
            1,
            &format!("Error open CLI schema: {}", e),
            BailAction::Exit,
            None,
        );
    }

    let (exec_name, global_options, cmd_args) = get_arguments(&schema);

    if check_duplicate_item(&global_options) {
        Util::bail_nvfwupd(
            1,
            "Error: nvfwupd tool expects any global config input exactly one time.",
            BailAction::Exit,
            None,
        );
    }

    let top_level_help = cmd_args.is_empty()
        && !global_options.is_empty()
        && global_options
            .iter()
            .all(|arg| arg == "-h" || arg == "--help");

    if cmd_args.is_empty() && top_level_help {
        FwUpdCmdHelp::print_usage(&exec_name, "Error: Missing command argument", &schema);
        println!("Error Code: 1");
        process::exit(1);
    }

    validate_global_options(&schema, &global_options);

    if cmd_args.is_empty() {
        FwUpdCmdHelp::print_usage(&exec_name, "Error: Missing command argument", &schema);
        // Python parity: emit "Error Code: 1" trailer on help-on-error exits
        // (no-args, unknown-cmd, -h, --help all take this path).
        println!("Error Code: 1");
        process::exit(1);
    } else if cmd_args[0] == "-V" || cmd_args[0] == "--version" {
        println!("nvfwupd version {}", version::NVFWUPD_CLI_VERSION);
        process::exit(0);
    }

    let cmd_record = schema.get_command_schema(&cmd_args[0]);
    if cmd_record.is_none() {
        FwUpdCmdHelp::print_usage(&exec_name, "Error: Invalid command", &schema);
        println!("Error Code: 1");
        process::exit(1);
    }

    init_cli_tracing(&global_options);

    // Pass cmd_args without the command name itself (cmd_args[0])
    dispatch_command(
        &schema,
        &exec_name,
        &global_options,
        &cmd_args[1..],
        cmd_record.unwrap(),
    )
    .await;
}
