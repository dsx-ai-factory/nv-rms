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

//! Main command dispatch module for nvfwupd CLI.
//!
//! Implements the firmware update command pipeline: argument validation,
//! BMC target resolution, platform identification, and execution of
//! individual CLI commands (show_version, update_fw, activate, etc.).
//!
//! The [`dispatch_command`] function is the entry point called from `main`
//! after CLI schema parsing. It maps the command class name to the
//! appropriate `FwUpdCmd*` subtype and invokes `run_command()`.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Write as IoWrite};
use std::path::Path;
use std::time::{Duration, Instant};

use regex::Regex;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::task::JoinSet;

fn json_pretty_4space(value: &Value) -> String {
    let buf = Vec::new();
    let fmt = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(buf, fmt);
    value.serialize(&mut ser).unwrap_or(());
    String::from_utf8(ser.into_inner()).unwrap_or_default()
}

use crate::bmc_access::BmcAccess;
use crate::check_exit_requested;
use crate::cli_schema::{CLISchema, CommandSchema};
use crate::config_parser::ConfigParser;
// ConfigRFTarget is used indirectly via the platform resolution path
use crate::dgx_rftarget::DGXRFTarget;
use crate::expected_inventory;
use crate::gb200_rftarget::GB200RFTarget;
use crate::gb200_switch_rftarget::GB200SwitchRFTarget;
use crate::gh200_rftarget::GH200RFTarget;
use crate::gh_rftarget::GHRFTarget;
use crate::hgxb100_rftarget::{HGXB100RFTarget, HGXRUBINRFTarget};
use crate::input_params::{InputParams, TaskId, WorkerResult};
use crate::ipmitool_api::{IpmiCommandError, IpmiToolActivation, IPMI_CMD_DICT};
use crate::os_access::{self, OsAccess};
use crate::pldm::{self, FirmwarePkg, PLDM};
use crate::powershelf_rftarget::PowerShelfRFTarget;
use crate::rf_target::{
    CmdArgs as RFCmdArgs, PkgParser, RFTarget, UpdatePreconditionMode, SERVER_TYPE_CLASS_DICT,
    TARGET_CLASS_DICT, TARGET_TYPE_CONFIG_DICT,
};
use crate::ssh_options::{
    ssh_host_key_mode_string_value_supported, SSH_HOST_KEY_MODE_ARG, SSH_HOST_KEY_MODE_DISABLED,
    SSH_HOST_KEY_MODE_STRICT, SSH_HOST_KEY_MODE_TOFU, SSH_KNOWN_HOSTS_ARG,
};
use crate::util::{BailAction, TraceFlags, Util};
use crate::utils::Util as NvUtils;
use crate::version;

fn redfish_fallback_for_ipmi_command(command: &str) -> &str {
    match command {
        "PWR_STATUS" => "RF_PWR_STATUS",
        "PWR_OFF" => "RF_PWR_OFF",
        "PWR_ON" => "RF_PWR_ON",
        "PWR_CYCLE" => "RF_PWR_CYCLE",
        _ => command,
    }
}

fn target_input_option_supported(key: &str) -> bool {
    matches!(
        key,
        "ip" | "port"
            | "user"
            | "password"
            | "servertype"
            | "verify_tls"
            | "bmc_ca_cert"
            | SSH_KNOWN_HOSTS_ARG
            | SSH_HOST_KEY_MODE_ARG
    )
}

fn bool_string_value_supported(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "0" | "false" | "no" | "off"
    )
}

fn verify_tls_string_value_supported(value: &str) -> bool {
    bool_string_value_supported(value)
}

// ---------------------------------------------------------------------------
// FirmwarePkgAdapter -- bridges FirmwarePkg to PkgParser
// ---------------------------------------------------------------------------

/// Adapter that wraps a `Box<dyn FirmwarePkg>` so it can be used where
/// `&mut dyn PkgParser` is expected.
struct FirmwarePkgAdapter<'a> {
    inner: &'a mut dyn FirmwarePkg,
}

#[async_trait::async_trait]
impl<'a> PkgParser for FirmwarePkgAdapter<'a> {
    async fn parse_pkg(&mut self, pkg_path: &str) -> (bool, String) {
        self.inner.parse_pkg(pkg_path, None).await
    }

    async fn get_unpack_file_dict(&mut self, pkg_path: &str) {
        self.inner.prepare_unpack_file_dict(pkg_path).await;
    }

    fn unpack_file_ap_dict(&self) -> &std::collections::HashMap<String, Vec<String>> {
        self.inner.unpack_file_ap_dict()
    }

    fn apname_version_dict_json(&self) -> serde_json::Value {
        serde_json::json!(self.inner.apname_version_dict())
    }

    fn pldm_raw_dict(&self) -> serde_json::Value {
        self.inner.pldm_raw_dict()
    }
}

// ---------------------------------------------------------------------------
// dispatch_command
// ---------------------------------------------------------------------------

/// Main command dispatch entry point.
pub async fn dispatch_command(
    schema: &CLISchema,
    exec_name: &str,
    global_options: &[String],
    cmd_args: &[String],
    cmd_record: &CommandSchema,
) {
    let class_name = &cmd_record.class_name;

    let base = FwUpdCmdBase::new(
        schema,
        exec_name.to_string(),
        cmd_record.name.clone(),
        cmd_record.clone(),
        global_options.to_vec(),
        cmd_args.to_vec(),
    )
    .await;

    match class_name.as_str() {
        "ToolVersion" => FwUpdCmdToolVersion::new(base).run_command().await,
        "Help" => FwUpdCmdHelp::new(base).run_command().await,
        "ShowRecipe" => FwUpdCmdShowRecipe::new(base).run_command().await,
        "UnpackPLDM" => FwUpdCmdUnpackPLDM::new(base).run_command().await,
        "ShowVersion" => FwUpdCmdShowVersion::new(base).run_command().await,
        "ForceUpdate" => FwUpdCmdForceUpdate::new(base).run_command().await,
        "UpdateFirmware" => FwUpdCmdUpdateFirmware::new(base).run_command().await,
        "ActivateFirmware" => FwUpdCmdActivateFirmware::new(base).run_command().await,
        "ShowUpdateProgress" => FwUpdCmdShowUpdateProgress::new(base).run_command().await,
        "PerformFactoryReset" => FwUpdCmdPerformFactoryReset::new(base).run_command().await,
        "BackgroundCopy" => FwUpdCmdBackgroundCopy::new(base).run_command().await,
        "FlintUpdate" => FwUpdCmdFlintUpdate::new(base).run_command().await,
        "CreateUpdateTargets" => FwUpdCmdCreateUpdateTargets::new(base).run_command().await,
        _ => {
            eprintln!("Error: Unknown command class '{}'", class_name);
            std::process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------
// ParsedArgs -- schema-aware argument parser
// ---------------------------------------------------------------------------

/// Schema-aware parsed arguments.
///
/// Unlike a simple HashMap, this understands store_true actions, nargs, etc.
#[derive(Clone)]
pub struct ParsedArgs {
    bools: HashMap<String, bool>,
    strings: HashMap<String, String>,
    lists: HashMap<String, Vec<String>>,
    positionals: Vec<String>,
}

impl fmt::Debug for ParsedArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let strings: HashMap<&str, String> = self
            .strings
            .iter()
            .map(|(key, value)| {
                (
                    key.as_str(),
                    NvUtils::redact_secret_value_for_key(key, value),
                )
            })
            .collect();
        let lists: HashMap<&str, Vec<String>> = self
            .lists
            .iter()
            .map(|(key, values)| {
                (
                    key.as_str(),
                    values
                        .iter()
                        .map(|value| redact_arg_value(key, value))
                        .collect(),
                )
            })
            .collect();
        let positionals: Vec<String> = self
            .positionals
            .iter()
            .map(|value| NvUtils::redact_secret_fields(value))
            .collect();

        f.debug_struct("ParsedArgs")
            .field("bools", &self.bools)
            .field("strings", &strings)
            .field("lists", &lists)
            .field("positionals", &positionals)
            .finish()
    }
}

impl ParsedArgs {
    pub fn new() -> Self {
        Self {
            bools: HashMap::new(),
            strings: HashMap::new(),
            lists: HashMap::new(),
            positionals: Vec::new(),
        }
    }

    /// Get a boolean flag (store_true actions like --json, --yes, --background).
    pub fn get_bool(&self, name: &str) -> bool {
        *self.bools.get(name).unwrap_or(&false)
    }

    /// Get a string value (store actions like --timeout).
    pub fn get_string(&self, name: &str) -> Option<&str> {
        self.strings.get(name).map(|s| s.as_str())
    }

    /// Get a list of values (nargs="+" options like --package).
    pub fn get_list(&self, name: &str) -> &[String] {
        self.lists.get(name).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get the first positional argument.
    pub fn get_positional(&self, index: usize) -> Option<&str> {
        self.positionals.get(index).map(|s| s.as_str())
    }
}

fn redact_arg_vec_map(args: &HashMap<String, Vec<String>>) -> HashMap<String, Vec<String>> {
    args.iter()
        .map(|(key, values)| {
            (
                key.clone(),
                values
                    .iter()
                    .map(|value| redact_arg_value(key, value))
                    .collect(),
            )
        })
        .collect()
}

fn redact_arg_value(key: &str, value: &str) -> String {
    if key == "target" || key == "os_target" {
        NvUtils::redact_secret_key_value_arg(value)
    } else {
        NvUtils::redact_secret_value_for_key(key, value)
    }
}

/// Parse command arguments based on the command schema options.
fn parse_cmd_args(args: &[String], cmd_schema: &CommandSchema) -> ParsedArgs {
    let mut parsed = ParsedArgs::new();

    // Build lookup tables from schema
    let mut short_to_long: HashMap<String, String> = HashMap::new();
    let mut action_map: HashMap<String, String> = HashMap::new();
    let mut nargs_map: HashMap<String, String> = HashMap::new();

    // Schema options with no Short key act as positional arguments
    // (e.g. force_upd_action for "enable|disable|status").
    let mut positional_opts: Vec<String> = Vec::new();

    for opt in &cmd_schema.options {
        let long = opt.long.clone();
        // Python parity: options without a real Short key are treated as
        // POSITIONAL arguments by argparse (e.g. force_update's
        // force_upd_action with Short: "--"). They must NOT match the
        // `--<long>` form; if a user types `--force_upd_action check`,
        // argparse treats `--force_upd_action` as unrecognized. So skip
        // adding such opts to action_map — the parser's unknown-flag
        // branch will then emit the right argparse error.
        let is_positional = match opt.short {
            Some(ref s) if !s.is_empty() && s != "--" => false,
            _ => opt.action != "store_true",
        };
        if !is_positional {
            action_map.insert(long.clone(), opt.action.clone());
            if let Some(ref nargs) = opt.nargs {
                nargs_map.insert(long.clone(), nargs.clone());
            }
        }
        if let Some(ref short) = opt.short {
            if !short.is_empty() && short != "--" {
                short_to_long.insert(short.clone(), long.clone());
            }
        }
        if is_positional {
            positional_opts.push(long);
        }
    }

    // Collect unknown flag+value pairs so we can emit a Python-argparse-style
    // "unrecognized arguments" error at the end, matching Python's behavior
    // exactly (and preventing the previous silent-accept behavior that led
    // Rust's show_version to ignore --timeout 0 instead of rejecting it).
    let mut unrecognized: Vec<String> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];

        if arg.starts_with("--") {
            let key = arg.trim_start_matches("--").to_string();
            if let Some(action) = action_map.get(&key) {
                if action == "store_true" {
                    parsed.bools.insert(key, true);
                } else {
                    // Determine if nargs is "+" (list)
                    let is_list = nargs_map.get(&key).map(|n| n == "+").unwrap_or(false);
                    if is_list {
                        let mut vals = Vec::new();
                        i += 1;
                        while i < args.len() && !args[i].starts_with('-') {
                            vals.push(args[i].clone());
                            i += 1;
                        }
                        parsed.lists.insert(key, vals);
                        continue; // skip the i += 1 at bottom
                    } else {
                        i += 1;
                        if i < args.len() {
                            parsed.strings.insert(key, args[i].clone());
                        }
                    }
                }
            } else {
                // Unknown long option — collect for end-of-parse reporting.
                // Also consume a following non-flag token as its value so we
                // match Python's "unrecognized arguments: --foo 0" grouping.
                unrecognized.push(arg.clone());
                if i + 1 < args.len() && !args[i + 1].starts_with('-') {
                    unrecognized.push(args[i + 1].clone());
                    i += 1;
                }
            }
        } else if arg.starts_with('-') && arg.len() == 2 {
            let short_key = arg[1..2].to_string();
            if let Some(long_key) = short_to_long.get(&short_key) {
                let long_key = long_key.clone();
                if let Some(action) = action_map.get(&long_key) {
                    if action == "store_true" {
                        parsed.bools.insert(long_key, true);
                    } else {
                        let is_list = nargs_map.get(&long_key).map(|n| n == "+").unwrap_or(false);
                        if is_list {
                            let mut vals = Vec::new();
                            i += 1;
                            while i < args.len() && !args[i].starts_with('-') {
                                vals.push(args[i].clone());
                                i += 1;
                            }
                            parsed.lists.insert(long_key, vals);
                            continue;
                        } else {
                            i += 1;
                            if i < args.len() {
                                parsed.strings.insert(long_key, args[i].clone());
                            }
                        }
                    }
                } else {
                    i += 1;
                    if i < args.len() && !args[i].starts_with('-') {
                        parsed.strings.insert(long_key, args[i].clone());
                    }
                }
            } else {
                // Unknown short option — collect for rejection.
                unrecognized.push(arg.clone());
                if i + 1 < args.len() && !args[i + 1].starts_with('-') {
                    unrecognized.push(args[i + 1].clone());
                    i += 1;
                }
            }
        } else {
            // Positional argument — map to schema options with no Short key
            if !positional_opts.is_empty() {
                let opt_name = positional_opts.remove(0);
                parsed.strings.insert(opt_name, arg.clone());
            }
            parsed.positionals.push(arg.clone());
        }
        i += 1;
    }

    if !unrecognized.is_empty() {
        // Python argparse exits 2 with:
        //   usage: <command> <usage-string>
        //   nvfwupd: error: unrecognized arguments: <tokens...>
        let usage = if cmd_schema.usage.is_empty() {
            cmd_schema.name.clone()
        } else {
            cmd_schema.usage.clone()
        };
        eprintln!("usage: {}", usage);
        eprintln!(
            "nvfwupd: error: unrecognized arguments: {}",
            unrecognized.join(" ")
        );
        std::process::exit(2);
    }

    parsed
}

/// Parse global options from the raw option strings.
fn parse_global_options(opts: &[String], schema: &CLISchema) -> HashMap<String, Vec<String>> {
    let mut result: HashMap<String, Vec<String>> = HashMap::new();

    // Build short-to-long lookup from schema
    let mut short_to_long: HashMap<String, String> = HashMap::new();
    let global_opts = schema.get_global_options();
    for (_name, opt) in &global_opts {
        if !opt.short.is_empty() && !opt.long.is_empty() {
            short_to_long.insert(opt.short.clone(), opt.long.clone());
        }
    }
    // Valid long names (used to detect unrecognized globals like `-j` used
    // as a global option when it's a per-command option).
    let valid_longs: std::collections::HashSet<String> = global_opts
        .iter()
        .map(|(_n, o)| o.long.clone())
        .filter(|s| !s.is_empty())
        .collect();

    let mut unrecognized: Vec<String> = Vec::new();
    let mut current_key: Option<String> = None;
    for opt in opts {
        if opt.starts_with("--") {
            let key = opt.trim_start_matches("--").to_string();
            if !valid_longs.contains(&key) {
                unrecognized.push(opt.clone());
                current_key = None;
                continue;
            }
            current_key = Some(key.clone());
            result.entry(key).or_default();
        } else if opt.starts_with('-') && opt.len() == 2 {
            let short = opt[1..2].to_string();
            match short_to_long.get(&short).cloned() {
                Some(long) => {
                    current_key = Some(long.clone());
                    result.entry(long).or_default();
                }
                None => {
                    // Python parity: a bare short like `-j` that isn't a
                    // global option in the schema (it's per-command) is
                    // reported as an "unrecognized arguments" argparse error.
                    unrecognized.push(opt.clone());
                    current_key = None;
                }
            }
        } else if let Some(ref key) = current_key {
            result.entry(key.clone()).or_default().push(opt.clone());
        }
    }

    if !unrecognized.is_empty() {
        let exec_name = std::env::current_exe()
            .ok()
            .and_then(|p| p.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "nvfwupd".to_string());
        eprintln!("usage: {} [-t BMC-TARGET] [-v] < command >", exec_name);
        eprintln!(
            "nvfwupd: error: unrecognized arguments: {}",
            unrecognized.join(" ")
        );
        std::process::exit(2);
    }

    result
}

/// Validate top-level global options before command-specific fast paths run.
///
/// Commands such as `version` and `help` intentionally do very little work, but
/// they still need argparse-style rejection for unknown globals.
pub fn validate_global_options(schema: &CLISchema, opts: &[String]) {
    let _ = parse_global_options(opts, schema);
}

// ---------------------------------------------------------------------------
// FwUpdCmdBase -- shared state for all command implementations
// ---------------------------------------------------------------------------

/// Base struct holding state shared by all firmware update commands.
pub struct FwUpdCmdBase<'a> {
    pub schema: &'a CLISchema,
    pub exec_name: String,
    pub cmd_name: String,
    pub cmd_schema: CommandSchema,
    pub global_options: Vec<String>,
    pub args: Vec<String>,
    pub trace: TraceFlags,
    pub config_parser: Option<ConfigParser>,
    pub g_verbose: bool,
}

impl<'a> FwUpdCmdBase<'a> {
    pub async fn new(
        schema: &'a CLISchema,
        exec_name: String,
        cmd_name: String,
        cmd_schema: CommandSchema,
        global_options: Vec<String>,
        args: Vec<String>,
    ) -> Self {
        let g_verbose = global_options.iter().any(|o| o == "-v" || o == "--verbose");
        let json_mode = args.iter().any(|o| o == "-j" || o == "--json");
        let trace = TraceFlags::new(g_verbose, json_mode);

        // Parse config file if -c option is present
        let mut config_parser = if global_options.iter().any(|o| o == "-c" || o == "--config") {
            let mut config_path = None;
            let mut found_c = false;
            for opt in &global_options {
                if found_c {
                    config_path = Some(opt.clone());
                    break;
                }
                if opt == "-c" || opt == "--config" {
                    found_c = true;
                }
            }
            // Match Python behavior: if the user passed `-c` but the file is
            // missing or unparseable, emit a specific error rather than
            // falling through to "Required option -t/--target or -c/--config
            // is missing." (which wrongly suggests -c wasn't provided).
            // `BailAction::Exit` calls process::exit, so the unreachable!()
            // calls below are dead code from the runtime's perspective but
            // give the compiler a divergent type to satisfy the Option<_>
            // branch arms.
            match config_path {
                Some(path) => {
                    let mut cp = ConfigParser::new(path.clone());
                    match cp.parse_config_data().await {
                        Ok(_) => Some(cp),
                        Err(e) if e == format!("Config file {} does not exist.", path) => {
                            Util::bail_nvfwupd(1, &e, BailAction::Exit, None);
                            unreachable!("bail_nvfwupd with Exit terminates");
                        }
                        Err(_e) => {
                            // Match Python's exact message (including the
                            // missing space after the period — that's how
                            // the original Python string is written).
                            Util::bail_nvfwupd(
                                1,
                                &format!(
                                    "Unable to parse config file {}.Please provide valid YAML config",
                                    path
                                ),
                                BailAction::Exit,
                                None,
                            );
                            unreachable!("bail_nvfwupd with Exit terminates");
                        }
                    }
                }
                None => None,
            }
        } else {
            None
        };

        // Honor SANITIZE_LOG config option.
        // Python: if the config only has SANITIZE_LOG, treat as no-config
        //         AND apply the sanitize flag globally.
        if let Some(ref cp) = config_parser {
            if let Some(ref cfg) = cp.config_dict {
                if let Some(sanitize_val) = cfg.get("SANITIZE_LOG").and_then(|v| v.as_bool()) {
                    NvUtils::set_is_sanitize(sanitize_val);
                    // If SANITIZE_LOG is the only key, drop the config_parser
                    // so the tool behaves as if no config file was given.
                    if let Some(obj) = cfg.as_object() {
                        if obj.len() == 1 {
                            config_parser = None;
                        }
                    }
                }
            }
        }

        Self {
            schema,
            exec_name,
            cmd_name,
            cmd_schema,
            global_options,
            args,
            trace,
            config_parser,
            g_verbose,
        }
    }

    /// Parse command arguments using the schema-aware parser.
    pub fn parse_cmd_args(&self) -> ParsedArgs {
        parse_cmd_args(&self.args, &self.cmd_schema)
    }

    /// Parse global options into a HashMap.
    pub fn parse_global_options(&self) -> HashMap<String, Vec<String>> {
        parse_global_options(&self.global_options, self.schema)
    }

    /// Validate global and command options.
    /// Returns `(global_args, cmd_args)`.
    pub fn validate_cmd(
        &self,
        json_input: Option<&Value>,
    ) -> (HashMap<String, Vec<String>>, ParsedArgs) {
        let global_args = self.parse_global_options();

        // Python parity: argparse requires that `-t/--target` (nargs='+')
        // receives at least one value; a bare `--target` with no following
        // tokens exits 2 with "expected at least one argument".
        if let Some(target_vals) = global_args.get("target") {
            if target_vals.is_empty() {
                eprintln!("usage: {} [-t BMC-TARGET] [-v] < command >", self.exec_name);
                eprintln!("nvfwupd: error: argument -t/--target: expected at least one argument");
                std::process::exit(2);
            }
        }

        if self.config_parser.is_none() {
            let has_target = global_args.contains_key("target");
            let has_os_target = global_args.contains_key("os_target");
            if !has_target && !has_os_target {
                // Exit immediately on missing required global option so the
                // caller does not continue into validate_target_json (which
                // would print a second, redundant error) or the command
                // handler (which expects validated inputs). Matches Python
                // argparse behavior of terminating on first required-arg error.
                Util::bail_nvfwupd(
                    1,
                    "Error: Required option -t/--target or -c/--config is missing.",
                    BailAction::Exit,
                    json_input,
                );
            }
        }

        let cmd_args = self.parse_cmd_args();

        if self.g_verbose {
            let global_args = redact_arg_vec_map(&global_args);
            tracing::debug!("Global args: {:?}", global_args);
            tracing::debug!("Cmd args: {:?}", cmd_args);
        }

        (global_args, cmd_args)
    }

    /// Validate recipe file paths.
    pub async fn validate_recipes(
        &self,
        recipes: Option<Vec<String>>,
        json_dict: Option<&Value>,
    ) -> Option<Vec<String>> {
        let mut recipes = recipes;

        // Python parity (I7): when a CLI `-t/--target` is explicitly given
        // alongside `-c/--config`, the config file's PACKAGE / FWUpdateFilePath
        // entries are ignored. Packages only come from the CLI `-p/--package`
        // in that case. When the `-t` target lacks a package, `Packages: N/A`
        // is the expected output.
        let has_cli_target = self
            .global_options
            .iter()
            .any(|o| o == "-t" || o == "--target");

        if recipes.is_none() && !has_cli_target {
            if let Some(ref cp) = self.config_parser {
                if let Some(ref config) = cp.config_dict {
                    let parallel = config
                        .get("ParallelUpdate")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if parallel {
                        let mut pkgs = Vec::new();
                        if let Some(targets) = config.get("Targets").and_then(|v| v.as_array()) {
                            for target in targets {
                                if let Some(pkg) = target.get("PACKAGE") {
                                    if let Some(arr) = pkg.as_array() {
                                        for p in arr {
                                            if let Some(s) = p.as_str() {
                                                pkgs.push(s.to_string());
                                            }
                                        }
                                    } else if let Some(s) = pkg.as_str() {
                                        pkgs.push(s.to_string());
                                    }
                                }
                            }
                        }
                        if !pkgs.is_empty() {
                            recipes = Some(pkgs);
                        }
                    } else if let Some(paths) = config.get("FWUpdateFilePath") {
                        if let Some(arr) = paths.as_array() {
                            let pkgs: Vec<String> = arr
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                            if !pkgs.is_empty() {
                                recipes = Some(pkgs);
                            }
                        } else if let Some(s) = paths.as_str() {
                            // Python parity: Python's `for each in recipes:`
                            // iterates characters when FWUpdateFilePath is a
                            // string instead of a list. This is a Python
                            // duck-typing quirk rather than intentional
                            // behavior, but it produces an observable error
                            // ("firmware file <first-missing-char> not found")
                            // that users can grep for — replicate it to keep
                            // Rust's output byte-for-byte identical.
                            let chars: Vec<String> = s.chars().map(|c| c.to_string()).collect();
                            if !chars.is_empty() {
                                recipes = Some(chars);
                            }
                        }
                    }
                }
            }
        }

        if let Some(ref recipe_list) = recipes {
            for each in recipe_list {
                if !path_exists(each).await {
                    // Exit on first missing firmware file so that subsequent
                    // code doesn't try to parse / open / walk a non-existent
                    // package. Matches Python behavior.
                    Util::bail_nvfwupd(
                        1,
                        &format!(
                            "Error: {}: firmware file {} not found",
                            self.exec_name, each
                        ),
                        BailAction::Exit,
                        json_dict,
                    );
                }
            }
        }

        recipes
    }

    /// Validate target JSON or config file and produce a list of target argument lists.
    pub async fn validate_target_json(
        &self,
        global_args: &HashMap<String, Vec<String>>,
        json_dict: Option<&Value>,
    ) -> Vec<Vec<String>> {
        let target_args = global_args.get("target");

        // If no BMC targets are provided, check for config or error
        if target_args.is_none() && self.config_parser.is_none() {
            if global_args.contains_key("os_target") {
                Util::bail_nvfwupd(
                    1,
                    "Error: This command requires BMC targets (-t/--target \
                     option). OS targets (-o/--os_target) are not supported \
                     for this command.",
                    BailAction::Exit,
                    json_dict,
                );
            } else {
                Util::bail_nvfwupd(
                    1,
                    "Error: This command requires BMC targets (-t/--target option).",
                    BailAction::Exit,
                    json_dict,
                );
            }
            return Vec::new();
        }

        if let Some(args) = target_args {
            if args.len() > 0 && args.iter().any(|a| a.contains('=')) {
                // Inline key=value target: ip=X user=Y password=Z
                let mut given_opts = Vec::new();
                let mut ip_addr: Option<String> = None;

                // Extract IP address
                for input_opt in args {
                    let parts: Vec<&str> = input_opt.splitn(2, '=').collect();
                    if parts.len() == 2 && parts[0].to_lowercase() == "ip" {
                        ip_addr = Some(parts[1].trim_matches(|c| c == '[' || c == ']').to_string());
                        break;
                    }
                }

                if ip_addr.is_none() {
                    Util::bail_nvfwupd(
                        1,
                        "Error: Missing target IP in command input",
                        BailAction::Exit,
                        json_dict,
                    );
                    return Vec::new();
                }

                // Validate all keys
                for input_opt in args {
                    let parts: Vec<&str> = input_opt.splitn(2, '=').collect();
                    if parts.len() == 2 {
                        let key = parts[0].to_lowercase();
                        if !target_input_option_supported(&key) {
                            Util::bail_nvfwupd(
                                1,
                                &format!(
                                    "Error: Incorrect input option {}",
                                    NvUtils::sanitize_log(input_opt)
                                ),
                                BailAction::Exit,
                                json_dict,
                            );
                        }
                        given_opts.push(key);
                    }
                }

                if !given_opts.contains(&"ip".to_string()) {
                    Util::bail_nvfwupd(
                        1,
                        "Error: Missing target IP in command input",
                        BailAction::Exit,
                        json_dict,
                    );
                    return Vec::new();
                }

                if !given_opts.contains(&"port".to_string())
                    && (!given_opts.contains(&"user".to_string())
                        || !given_opts.contains(&"password".to_string()))
                {
                    Util::bail_nvfwupd(
                        1,
                        "Error: Missing target credentials user/password in command input",
                        BailAction::Exit,
                        json_dict,
                    );
                    return Vec::new();
                }

                // Resolve DNS
                let resolved_ip = resolve_ip(ip_addr.as_deref().unwrap_or("")).await;
                let mut resolved_args: Vec<String> = Vec::new();
                for input_opt in args {
                    if input_opt.starts_with("ip=") {
                        resolved_args.push(format!("ip={}", resolved_ip));
                    } else {
                        resolved_args.push(input_opt.clone());
                    }
                }

                return vec![resolved_args];
            }

            // Check for deprecated targets=file.json
            if args.len() == 1 {
                let first = &args[0];
                let parts: Vec<&str> = first.splitn(2, '=').collect();
                if parts.len() == 2 && parts[0].to_lowercase() == "targets" {
                    Util::bail_nvfwupd(
                        1,
                        "Error: targets json file input support has been deprecated, \
                         please move to using config.yaml for the same multi-target inputs.",
                        BailAction::Exit,
                        json_dict,
                    );
                    return Vec::new();
                }
                // Single item that looks like an option key
                if parts[0].to_lowercase() != "ip" {
                    if target_input_option_supported(&parts[0].to_lowercase()) {
                        Util::bail_nvfwupd(
                            1,
                            "Error: Incomplete input value for option -t/--target",
                            BailAction::Exit,
                            json_dict,
                        );
                    } else {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "Error: Incorrect input option {}",
                                NvUtils::sanitize_log(first)
                            ),
                            BailAction::Exit,
                            json_dict,
                        );
                    }
                    return Vec::new();
                }
            }
        }

        // Config file targets
        if let Some(ref cp) = self.config_parser {
            return self.make_target_list(&cp.targets, json_dict).await;
        }

        Vec::new()
    }

    /// Format a serde_json `Value` like Python's `str(dict)` / repr for
    /// use in error messages. Python output uses single quotes with a
    /// space after ':' and ',' — matching that gives us byte-exact parity
    /// on per-target validation error strings (I2).
    fn format_value_python_repr(v: &Value) -> String {
        match v {
            Value::Null => "None".to_string(),
            Value::Bool(b) => {
                if *b {
                    "True".to_string()
                } else {
                    "False".to_string()
                }
            }
            Value::Number(n) => n.to_string(),
            Value::String(s) => format!("'{}'", s),
            Value::Array(a) => {
                let items: Vec<String> = a.iter().map(Self::format_value_python_repr).collect();
                format!("[{}]", items.join(", "))
            }
            Value::Object(o) => {
                let items: Vec<String> = o
                    .iter()
                    .map(|(k, val)| format!("'{}': {}", k, Self::format_value_python_repr(val)))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }

    /// Build target list from config parser targets.
    async fn make_target_list(
        &self,
        targets: &[Value],
        json_dict: Option<&Value>,
    ) -> Vec<Vec<String>> {
        let mut targets_list = Vec::new();

        let is_config = self.config_parser.is_some();
        let ip_key = if is_config { "BMC_IP" } else { "ip" };
        let user_key = if is_config { "RF_USERNAME" } else { "user" };
        let pass_key = if is_config { "RF_PASSWORD" } else { "password" };
        let port_key = if is_config { "TUNNEL_TCP_PORT" } else { "port" };
        let type_key = if is_config {
            "TARGET_PLATFORM"
        } else {
            "servertype"
        };

        if targets.is_empty() {
            // Exit immediately to match Python behavior. Previously DoNothing
            // left us with an empty target list that downstream code happily
            // iterated (zero iterations), and the tool exited 0 with just
            // "Error: target list is empty" printed — missing the trailing
            // "Error Code: 1" and reporting success to callers.
            Util::bail_nvfwupd(
                1,
                "Error: target list is empty",
                BailAction::Exit,
                json_dict,
            );
            return targets_list;
        }

        for target in targets {
            let obj = match target.as_object() {
                Some(o) => o,
                None => {
                    Util::bail_nvfwupd(
                        1,
                        &format!(
                            "Error: {} is not a valid object",
                            NvUtils::sanitize_log(&Self::format_value_python_repr(target))
                        ),
                        BailAction::DoNothing,
                        json_dict,
                    );
                    continue;
                }
            };

            let ip_addr = obj.get(ip_key).and_then(|v| v.as_str()).unwrap_or("");
            let user = obj.get(user_key).and_then(|v| v.as_str()).unwrap_or("");
            let password = obj.get(pass_key).and_then(|v| v.as_str()).unwrap_or("");
            let port = obj.get(port_key).and_then(|v| v.as_str()).unwrap_or("");
            let platform_type = obj.get(type_key).and_then(|v| v.as_str());

            // Validate required keys
            let is_parallel_update = is_config
                && self.cmd_name == "update_fw"
                && self
                    .config_parser
                    .as_ref()
                    .and_then(|cp| cp.config_dict.as_ref())
                    .and_then(|d| d.get("ParallelUpdate"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

            if is_parallel_update {
                if ip_addr.is_empty()
                    || user.is_empty()
                    || password.is_empty()
                    || !obj.contains_key("PACKAGE")
                {
                    Util::bail_nvfwupd(
                        1,
                        &format!(
                            "Error: {} object has missing/invalid keys  ",
                            NvUtils::sanitize_log(&Self::format_value_python_repr(target))
                        ),
                        BailAction::DoNothing,
                        json_dict,
                    );
                    continue;
                }
            } else if ip_addr.is_empty() || user.is_empty() || password.is_empty() {
                Util::bail_nvfwupd(
                    1,
                    &format!(
                        "Error: {} object has missing/invalid keys  ",
                        NvUtils::sanitize_log(&Self::format_value_python_repr(target))
                    ),
                    BailAction::DoNothing,
                    json_dict,
                );
                continue;
            }

            // Resolve DNS
            let resolved_ip = resolve_ip(ip_addr).await;

            let mut ns = vec![
                format!("ip={}", resolved_ip),
                format!("user={}", user),
                format!("password={}", password),
            ];
            if !port.is_empty() {
                ns.push(format!("port={}", port));
            }
            if let Some(pt) = platform_type {
                ns.push(format!("servertype={}", pt.to_lowercase()));
            }

            // Handle package
            if let Some(pkg) = obj.get("PACKAGE") {
                if let Some(arr) = pkg.as_array() {
                    let pkg_str: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
                    ns.push(format!("package={}", pkg_str.join(" ")));
                } else if let Some(s) = pkg.as_str() {
                    ns.push(format!("package={}", s));
                }
            }

            // Handle optional parameters
            if let Some(tp) = obj.get("UPDATE_PARAMETERS_TARGETS") {
                ns.push(format!("UpdateParametersTargets={}", tp));
            }
            if let Some(oem) = obj.get("OEM_PARAMETERS") {
                ns.push(format!("OemParameters={}", oem));
            }
            if let Some(verify_tls) = obj.get("VERIFY_TLS") {
                match verify_tls {
                    Value::Bool(value) => ns.push(format!("verify_tls={}", value)),
                    Value::String(value) if verify_tls_string_value_supported(value) => {
                        ns.push(format!("verify_tls={}", value))
                    }
                    _ => {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "Error: {} object has invalid VERIFY_TLS value. \
                                      VERIFY_TLS must be a boolean or one of \
                                      1/0, true/false, yes/no, or on/off",
                                NvUtils::sanitize_log(&Self::format_value_python_repr(target))
                            ),
                            BailAction::DoNothing,
                            json_dict,
                        );
                        continue;
                    }
                }
            }
            if let Some(ca_cert) = obj.get("BMC_CA_CERT") {
                match ca_cert.as_str() {
                    Some(path) if !path.trim().is_empty() => {
                        ns.push(format!("bmc_ca_cert={}", path))
                    }
                    _ => {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "Error: {} object has invalid BMC_CA_CERT value. \
                                      BMC_CA_CERT must be a non-empty string",
                                NvUtils::sanitize_log(&Self::format_value_python_repr(target))
                            ),
                            BailAction::DoNothing,
                            json_dict,
                        );
                        continue;
                    }
                }
            }
            if let Some(known_hosts) = obj.get("SSH_KNOWN_HOSTS") {
                match known_hosts.as_str() {
                    Some(path) if !path.trim().is_empty() => {
                        ns.push(format!("{SSH_KNOWN_HOSTS_ARG}={}", path))
                    }
                    _ => {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "Error: {} object has invalid SSH_KNOWN_HOSTS value. \
                                      SSH_KNOWN_HOSTS must be a non-empty string",
                                NvUtils::sanitize_log(&Self::format_value_python_repr(target))
                            ),
                            BailAction::DoNothing,
                            json_dict,
                        );
                        continue;
                    }
                }
            }
            if let Some(mode) = obj.get("SSH_HOST_KEY_MODE") {
                match mode.as_str() {
                    Some(value) if ssh_host_key_mode_string_value_supported(value) => {
                        ns.push(format!("{SSH_HOST_KEY_MODE_ARG}={}", value))
                    }
                    _ => {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "Error: {} object has invalid SSH_HOST_KEY_MODE value. \
                                      SSH_HOST_KEY_MODE must be one of disabled, tofu, or strict",
                                NvUtils::sanitize_log(&Self::format_value_python_repr(target))
                            ),
                            BailAction::DoNothing,
                            json_dict,
                        );
                        continue;
                    }
                }
            }
            if let Some(sn) = obj.get("SYSTEM_NAME").and_then(|v| v.as_str()) {
                ns.push(format!("systemname={}", sn));
            }
            if let Some(ud) = obj.get("UPDATE_DELAY") {
                // Python validates UPDATE_DELAY as integer >= 0.
                // Reject floats, strings, and negative values.
                match ud.as_i64() {
                    Some(n) if n >= 0 => {
                        ns.push(format!("UpdateDelay={}", n));
                    }
                    _ => {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "Error: {} object has invalid UPDATE_DELAY value. \
                                      UPDATE_DELAY must be a non-negative integer",
                                NvUtils::sanitize_log(&Self::format_value_python_repr(target))
                            ),
                            BailAction::DoNothing,
                            json_dict,
                        );
                        continue;
                    }
                }
            }

            // Add TargetPlatform from config if set
            if let Some(ref cp) = self.config_parser {
                if let Some(ref config) = cp.config_dict {
                    if let Some(update_config) =
                        config.get("TargetPlatform").and_then(|v| v.as_str())
                    {
                        ns.push(format!("servertype={}", update_config.to_lowercase()));
                    }
                }
            }

            targets_list.push(ns);
        }

        // If every target in the config was rejected by the per-target
        // DoNothing bails above, `targets_list` is empty. Python exits
        // with status 1 in this case rather than silently continuing to
        // downstream code with zero work. Match that.
        //
        // Note: per-target errors stay as DoNothing so that multi-target
        // configs with a mix of valid and invalid entries still process
        // the valid ones — only the "no valid targets at all" case exits.
        if targets_list.is_empty() {
            Util::bail_nvfwupd(1, "", BailAction::Exit, json_dict);
        }

        targets_list
    }

    /// Validate OS target parameters.
    pub fn validate_os_target_json(
        &self,
        global_args: &HashMap<String, Vec<String>>,
        json_dict: Option<&Value>,
    ) -> Vec<String> {
        let os_args = global_args.get("os_target");

        if os_args.is_none() || os_args.map(|a| a.is_empty()).unwrap_or(true) {
            // Check config file
            if let Some(ref cp) = self.config_parser {
                if let Some(ref config) = cp.config_dict {
                    let os_ip = config.get("OS_IP").and_then(|v| v.as_str());
                    let os_user = config.get("OS_USERNAME").and_then(|v| v.as_str());
                    let os_pass = config.get("OS_PASSWORD").and_then(|v| v.as_str());
                    let os_port = config
                        .get("OS_PORT")
                        .and_then(|v| v.as_str())
                        .unwrap_or("22");

                    if let (Some(ip), Some(user), Some(pass)) = (os_ip, os_user, os_pass) {
                        let mut os_target = vec![
                            format!("ip={}", ip),
                            format!("user={}", user),
                            format!("password={}", pass),
                            format!("port={}", os_port),
                        ];
                        if let Some(known_hosts) = config.get("OS_SSH_KNOWN_HOSTS") {
                            match known_hosts.as_str() {
                                Some(path) if !path.trim().is_empty() => {
                                    os_target.push(format!("{SSH_KNOWN_HOSTS_ARG}={}", path))
                                }
                                _ => {
                                    Util::bail_nvfwupd(
                                        1,
                                        "Error: OS_SSH_KNOWN_HOSTS must be a non-empty string",
                                        BailAction::DoNothing,
                                        json_dict,
                                    );
                                    return Vec::new();
                                }
                            }
                        }
                        if let Some(mode) = config.get("OS_SSH_HOST_KEY_MODE") {
                            match mode.as_str() {
                                Some(value) if ssh_host_key_mode_string_value_supported(value) => {
                                    os_target.push(format!("{SSH_HOST_KEY_MODE_ARG}={}", value))
                                }
                                _ => {
                                    Util::bail_nvfwupd(
                                        1,
                                        "Error: OS_SSH_HOST_KEY_MODE must be one of disabled, tofu, or strict",
                                        BailAction::DoNothing,
                                        json_dict,
                                    );
                                    return Vec::new();
                                }
                            }
                        }

                        return os_target;
                    }
                }
            }
            Util::bail_nvfwupd(
                1,
                "Error: OS target is required. Use -o/--os_target option.",
                BailAction::DoNothing,
                json_dict,
            );
            return Vec::new();
        }

        let args = os_args.unwrap();

        // Validate key=value pairs
        let mut given_opts = Vec::new();
        for input_opt in args {
            let parts: Vec<&str> = input_opt.splitn(2, '=').collect();
            if parts.len() == 2 {
                let key = parts[0].to_lowercase();
                if ![
                    "ip",
                    "port",
                    "user",
                    "password",
                    "servertype",
                    SSH_KNOWN_HOSTS_ARG,
                    SSH_HOST_KEY_MODE_ARG,
                ]
                .contains(&key.as_str())
                {
                    Util::bail_nvfwupd(
                        1,
                        &format!(
                            "Error: Incorrect OS target input option {}",
                            NvUtils::sanitize_log(input_opt)
                        ),
                        BailAction::DoNothing,
                        json_dict,
                    );
                }
                given_opts.push(key);
            }
        }

        if !given_opts.contains(&"ip".to_string()) {
            Util::bail_nvfwupd(
                1,
                "Error: Missing OS target IP in command input",
                BailAction::DoNothing,
                json_dict,
            );
            return Vec::new();
        }
        if !given_opts.contains(&"port".to_string())
            && (!given_opts.contains(&"user".to_string())
                || !given_opts.contains(&"password".to_string()))
        {
            Util::bail_nvfwupd(
                1,
                "Error: Missing OS target credentials user/password in command input",
                BailAction::DoNothing,
                json_dict,
            );
            return Vec::new();
        }

        args.clone()
    }

    /// Identify the target platform and return the RFTarget class name.
    pub fn match_platform(target_platform: &str) -> Option<&'static str> {
        if target_platform.trim().is_empty() {
            return None;
        }
        for (key, val) in TARGET_CLASS_DICT.iter() {
            if key.contains(target_platform) || target_platform.contains(key) {
                return Some(val);
            }
        }
        None
    }

    /// Create an RFTarget based on platform identification.
    ///
    /// Returns a boxed trait object suitable for polymorphic firmware operations.
    pub fn init_platform(
        &self,
        bmc_access: BmcAccess,
        platform_type: Option<&str>,
        json_dict: Option<&Value>,
        parallel_update: bool,
    ) -> Option<Box<dyn RFTarget + Send + Sync>> {
        // Parallel Update has platform_types individually defined
        // Config single targets expected to use individual TargetPlatform
        if let Some(pt) = platform_type {
            if (parallel_update) || (self.config_parser.is_none()) {
                let target_class = SERVER_TYPE_CLASS_DICT.get(pt.to_lowercase().as_str());
                if target_class.is_none() {
                    // Use Exit so the non-parallel path aborts immediately
                    // with "Error Code: 1" instead of falling through to
                    // print a partial firmware table. In parallel mode,
                    // bail_nvfwupd_threadsafe with Exit + parallel_update
                    // returns early without calling process::exit, so the
                    // worker still unwinds cleanly via the `return None`
                    // below and the aggregate error handling in the caller
                    // reports status correctly.
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        "Invalid Server Type.",
                        BailAction::Exit,
                        json_dict,
                        parallel_update,
                    );
                    return None;
                }
                return Some(create_rf_target(target_class.unwrap(), bmc_access, None));
            }
        }

        // Config parser path - resolve the underlying platform target directly
        if let Some(ref cp) = self.config_parser {
            if let Some(ref config) = cp.config_dict {
                let update_config = config
                    .get("TargetPlatform")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_lowercase());

                if let Some(ref uc) = update_config {
                    // Reject an unsupported TargetPlatform up-front to match
                    // Python behavior. Previously an invalid value (e.g.
                    // "foobar") silently fell through to BMC-model auto-
                    // detection and returned a valid target, causing Rust
                    // to run commands against the wrong platform class with
                    // exit code 0.
                    if !NvUtils::SUPPORTED_PLATFORMS.contains(&uc.as_str()) {
                        Util::bail_nvfwupd_threadsafe(
                            1,
                            &format!("TargetPlatform {} is not supported", uc),
                            BailAction::Exit,
                            json_dict,
                            parallel_update,
                        );
                        return None;
                    }
                    match bmc_access.access_type {
                        crate::bmc_access::AccessType::Login
                        | crate::bmc_access::AccessType::PortForward
                        | crate::bmc_access::AccessType::NVSwitch => {}
                    }

                    // Look up the class name from SERVER_TYPE_CLASS_DICT
                    if let Some(class_name) = SERVER_TYPE_CLASS_DICT.get(uc.as_str()) {
                        return Some(create_rf_target(
                            class_name,
                            bmc_access,
                            Some(config.clone()),
                        ));
                    }
                }

                // Fallback: use BMC model to determine platform
                let model = bmc_access.model.to_lowercase();
                let mut target_platform = model.as_str();
                if target_platform.contains("hgx") {
                    target_platform = "hgx";
                }
                if let Some(class_name) = TARGET_CLASS_DICT.get(target_platform) {
                    return Some(create_rf_target(
                        class_name,
                        bmc_access,
                        Some(config.clone()),
                    ));
                }
                if let Some(class_name) = Self::match_platform(target_platform) {
                    return Some(create_rf_target(
                        class_name,
                        bmc_access,
                        Some(config.clone()),
                    ));
                }

                Util::bail_nvfwupd_threadsafe(
                    1,
                    &format!("Platform {} not supported", target_platform),
                    BailAction::DoNothing,
                    json_dict,
                    parallel_update,
                );
                return None;
            }
        }

        // Check DUT_MAP environment variable
        if let Ok(dut_map) = std::env::var("NVFWUPD_DUT_MAP") {
            if dut_map.contains(':') {
                let parts: Vec<&str> = dut_map.splitn(2, ':').collect();
                let stack = parts[1];
                if let Some(target_class) = TARGET_TYPE_CONFIG_DICT.get(stack) {
                    return Some(create_rf_target(target_class, bmc_access, None));
                } else {
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        &format!("Invalid configuration {}. Unknown type {}.", dut_map, stack),
                        BailAction::DoNothing,
                        json_dict,
                        parallel_update,
                    );
                    return None;
                }
            }
        }

        // Auto-detect from BMC model
        let mut target_platform = bmc_access.model.to_lowercase();
        if target_platform.contains("hgx") {
            target_platform = "hgx".to_string();
        }
        let mut target_class = TARGET_CLASS_DICT.get(target_platform.as_str()).copied();
        if target_class.is_none() {
            target_class = Self::match_platform(&target_platform);
            if target_class.is_none() {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &format!("Platform {} not supported", target_platform),
                    BailAction::DoNothing,
                    json_dict,
                    parallel_update,
                );
                return None;
            }
        }

        Some(create_rf_target(target_class.unwrap(), bmc_access, None))
    }

    /// Raise a process exit if the user has requested an exit via signal.
    #[allow(dead_code)]
    pub fn check_exit_and_raise(&self) {
        if check_exit_requested() {
            eprintln!("User requested exit");
            std::process::exit(1);
        }
    }

    /// Log the effective ParallelThreadCount (for verbose mode).
    /// Matches Python's `_log_parallel_thread_count`.
    pub fn log_parallel_thread_count(&self, print_json: Option<&Value>) {
        let default: usize = 10;
        let log_file_only = print_json.is_some();
        let count = self
            .config_parser
            .as_ref()
            .and_then(|cp| cp.config_dict.as_ref())
            .and_then(|d| d.get("ParallelThreadCount"));
        let effective = self.get_parallel_max_workers(None);
        let msg = match count {
            Some(_) => format!("Using configured ParallelThreadCount={}", effective),
            None => format!("Using default ParallelThreadCount={}", default),
        };
        if log_file_only {
            tracing::info!(indent = 0, log_only = true, "{msg}");
        } else {
            tracing::debug!(
                cli_verbose = self.trace.cli_verbose,
                json_mode = self.trace.json_mode,
                "{msg}"
            );
        }
    }

    /// Return the configured max parallel workers (default 10, min 1).
    pub fn get_parallel_max_workers(&self, json_output: Option<&Value>) -> usize {
        const DEFAULT_MAX_WORKERS: usize = 10;

        let count = self
            .config_parser
            .as_ref()
            .and_then(|cp| cp.config_dict.as_ref())
            .and_then(|d| d.get("ParallelThreadCount"));

        match count {
            Some(val) => {
                if let Some(n) = val.as_u64() {
                    if n < 1 {
                        Util::bail_nvfwupd(
                            1,
                            "ParallelThreadCount must be a positive integer",
                            BailAction::DoNothing,
                            json_output,
                        );
                        DEFAULT_MAX_WORKERS
                    } else {
                        n as usize
                    }
                } else if let Some(n) = val.as_i64() {
                    if n < 1 {
                        Util::bail_nvfwupd(
                            1,
                            "ParallelThreadCount must be a positive integer",
                            BailAction::DoNothing,
                            json_output,
                        );
                        DEFAULT_MAX_WORKERS
                    } else {
                        n as usize
                    }
                } else {
                    Util::bail_nvfwupd(
                        1,
                        "ParallelThreadCount must be an integer",
                        BailAction::DoNothing,
                        json_output,
                    );
                    DEFAULT_MAX_WORKERS
                }
            }
            None => DEFAULT_MAX_WORKERS,
        }
    }

    /// Build per-target `InputParams` from the config target key=value lists.
    ///
    /// Each target's `Vec<String>` has entries like `ip=X`, `package=Y`, etc.
    /// produced by `make_target_list`.
    pub fn create_input_params_list(
        target_ips: &[Vec<String>],
        default_recipe: &[String],
    ) -> Vec<InputParams> {
        let mut params_list = Vec::new();

        for target_args in target_ips {
            let arg_dict = parse_target_args(target_args);

            let ip = arg_dict.get("ip").cloned().unwrap_or_default();

            let package_str = arg_dict.get("package").cloned();
            let package_list: Vec<String> = if let Some(ref pkg) = package_str {
                if pkg.contains(' ') {
                    pkg.split_whitespace().map(|s| s.to_string()).collect()
                } else {
                    vec![pkg.clone()]
                }
            } else {
                default_recipe.to_vec()
            };

            let special = arg_dict.get("UpdateParametersTargets").cloned();
            let oem_parameters = arg_dict.get("OemParameters").cloned();
            let system_name = arg_dict.get("systemname").cloned();
            let update_delay: u64 = arg_dict
                .get("UpdateDelay")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            params_list.push(InputParams {
                target_args: target_args.clone(),
                ip,
                package_list,
                special,
                oem_parameters,
                system_name,
                update_delay,
            });
        }

        params_list
    }
}

/// Factory function to create an RFTarget from a class name string.
fn create_rf_target(
    class_name: &str,
    bmc_access: BmcAccess,
    config_dict: Option<Value>,
) -> Box<dyn RFTarget + Send + Sync> {
    match class_name {
        "GHRFTarget" => Box::new(GHRFTarget::new(bmc_access, config_dict)),
        "DGX_RFTarget" => Box::new(DGXRFTarget::new(bmc_access, config_dict)),
        "GH200RFTarget" => Box::new(GH200RFTarget::new(bmc_access, config_dict)),
        "GB200RFTarget" => Box::new(GB200RFTarget::new(bmc_access, config_dict)),
        "GB200SwitchRFTarget" => Box::new(GB200SwitchRFTarget::new(bmc_access, config_dict)),
        "HGXB100RFTarget" => Box::new(HGXB100RFTarget::new(bmc_access, config_dict)),
        "HGXRUBINRFTarget" => Box::new(HGXRUBINRFTarget::new(bmc_access, config_dict)),
        "PowerShelfRFTarget" => Box::new(PowerShelfRFTarget::new(bmc_access, config_dict)),
        _ => {
            Util::bail_nvfwupd(
                1,
                &format!("Error: Unknown target class: {}", class_name),
                BailAction::Exit,
                None,
            );
            unreachable!()
        }
    }
}

fn init_platform_from_config(
    bmc_access: BmcAccess,
    platform_type: Option<&str>,
    config_dict: Option<&Value>,
    json_dict: Option<&Value>,
    parallel_update: bool,
) -> Option<Box<dyn RFTarget + Send + Sync>> {
    if let Some(pt) = platform_type {
        if parallel_update || config_dict.is_none() {
            let target_class = SERVER_TYPE_CLASS_DICT.get(pt.to_lowercase().as_str());
            if target_class.is_none() {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    "Invalid Server Type.",
                    BailAction::Exit,
                    json_dict,
                    parallel_update,
                );
                return None;
            }
            return Some(create_rf_target(target_class.unwrap(), bmc_access, None));
        }
    }

    if let Some(config) = config_dict {
        let update_config = config
            .get("TargetPlatform")
            .and_then(|v| v.as_str())
            .map(|s| s.to_lowercase());

        if let Some(ref uc) = update_config {
            if !NvUtils::SUPPORTED_PLATFORMS.contains(&uc.as_str()) {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &format!("TargetPlatform {} is not supported", uc),
                    BailAction::Exit,
                    json_dict,
                    parallel_update,
                );
                return None;
            }

            if let Some(class_name) = SERVER_TYPE_CLASS_DICT.get(uc.as_str()) {
                return Some(create_rf_target(
                    class_name,
                    bmc_access,
                    Some(config.clone()),
                ));
            }
        }

        let model = bmc_access.model.to_lowercase();
        let mut target_platform = model.as_str();
        if target_platform.contains("hgx") {
            target_platform = "hgx";
        }
        if let Some(class_name) = TARGET_CLASS_DICT.get(target_platform) {
            return Some(create_rf_target(
                class_name,
                bmc_access,
                Some(config.clone()),
            ));
        }
        if let Some(class_name) = FwUpdCmdBase::match_platform(target_platform) {
            return Some(create_rf_target(
                class_name,
                bmc_access,
                Some(config.clone()),
            ));
        }

        Util::bail_nvfwupd_threadsafe(
            1,
            &format!("Platform {} not supported", target_platform),
            BailAction::DoNothing,
            json_dict,
            parallel_update,
        );
        return None;
    }

    if let Ok(dut_map) = std::env::var("NVFWUPD_DUT_MAP") {
        if dut_map.contains(':') {
            let parts: Vec<&str> = dut_map.splitn(2, ':').collect();
            let stack = parts[1];
            if let Some(target_class) = TARGET_TYPE_CONFIG_DICT.get(stack) {
                return Some(create_rf_target(target_class, bmc_access, None));
            } else {
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &format!("Invalid configuration {}. Unknown type {}.", dut_map, stack),
                    BailAction::DoNothing,
                    json_dict,
                    parallel_update,
                );
                return None;
            }
        }
    }

    let mut target_platform = bmc_access.model.to_lowercase();
    if target_platform.contains("hgx") {
        target_platform = "hgx".to_string();
    }
    let mut target_class = TARGET_CLASS_DICT.get(target_platform.as_str()).copied();
    if target_class.is_none() {
        target_class = FwUpdCmdBase::match_platform(&target_platform);
        if target_class.is_none() {
            Util::bail_nvfwupd_threadsafe(
                1,
                &format!("Platform {} not supported", target_platform),
                BailAction::DoNothing,
                json_dict,
                parallel_update,
            );
            return None;
        }
    }

    Some(create_rf_target(target_class.unwrap(), bmc_access, None))
}

/// Resolve a hostname or IP to an IP address string.
///
/// Matches Python's `socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)` —
/// Tokio's `lookup_host` resolves for stream sockets (TCP). We prefer IPv4
/// addresses first when available (matching Python's typical ordering).
/// On DNS failure, bails with an error (matching Python's socket.gaierror).
async fn resolve_ip(host: &str) -> String {
    // Strip brackets for IPv6
    let clean = host.trim_matches(|c| c == '[' || c == ']');
    match tokio::net::lookup_host((clean, 0u16)).await {
        Ok(addrs) => {
            // Prefer IPv4 first to match Python's typical getaddrinfo ordering
            // when both A and AAAA records exist.
            let addrs_vec: Vec<_> = addrs.collect();
            if let Some(v4) = addrs_vec.iter().find(|a| a.is_ipv4()) {
                return v4.ip().to_string();
            }
            if let Some(first) = addrs_vec.first() {
                return first.ip().to_string();
            }
            Util::bail_nvfwupd(
                1,
                &format!("Error: Unable to resolve hostname: {}", host),
                BailAction::DoNothing,
                None,
            );
            clean.to_string()
        }
        Err(e) => {
            Util::bail_nvfwupd(
                1,
                &format!("Error: Unable to resolve hostname {}: {}", host, e),
                BailAction::DoNothing,
                None,
            );
            clean.to_string()
        }
    }
}

async fn path_exists(path: &str) -> bool {
    tokio::fs::metadata(path).await.is_ok()
}

async fn path_is_file(path: &str) -> bool {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.is_file())
        .unwrap_or(false)
}

/// Parse target arg list into a HashMap of key=value pairs.
fn parse_target_args(target_args: &[String]) -> HashMap<String, String> {
    let mut arg_dict = HashMap::new();
    for each in target_args {
        let tokens: Vec<&str> = each.splitn(2, '=').collect();
        if tokens.len() == 2 {
            arg_dict.insert(tokens[0].to_string(), tokens[1].to_string());
        }
    }
    arg_dict
}

fn append_json_error_lines(json_dict: &mut Value, message: &str) {
    let Some(errors) = json_dict.get_mut("Error").and_then(|v| v.as_array_mut()) else {
        return;
    };

    for line in NvUtils::sanitize_log(message).lines() {
        let line = line.trim();
        if !line.is_empty() {
            errors.push(Value::String(line.to_string()));
        }
    }
}

async fn validate_expected_inventory_for_cli(
    rf_target: &(dyn RFTarget + Send + Sync),
    expected_inventory: Option<&[String]>,
    trace: TraceFlags,
    json_output: Option<&mut Value>,
) -> Result<(), String> {
    let Some(expected_inventory) = expected_inventory.filter(|expected| !expected.is_empty())
    else {
        return Ok(());
    };

    let (ok, err_code, present) = rf_target
        .get_expected_inventory_ap_names(trace, json_output)
        .await;
    if !ok || err_code != 0 {
        return Err(format!(
            "pre-update expected inventory check: failed to retrieve firmware inventory collection, error code {err_code}"
        ));
    }

    let missing =
        expected_inventory::missing_expected_ap_names_from_present(expected_inventory, &present);
    if missing.is_empty() {
        return Ok(());
    }

    Err(expected_inventory::missing_expected_inventory_message(
        "pre-update expected inventory check",
        &missing,
        &present,
    ))
}

// ---------------------------------------------------------------------------
// Parallel update worker
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct UpdateWorkerContext {
    trace: TraceFlags,
    config_dict: Option<Value>,
    g_verbose: bool,
}

/// Per-target worker for parallel firmware update.
///
/// Creates its own `BmcAccess` + `RFTarget`, calls `start_update_monitor`
/// with `parallel_update=true` (which collects task IDs without entering
/// the foreground monitoring loop), and returns a `WorkerResult`.
#[allow(clippy::too_many_arguments)]
async fn update_fw_worker(
    context: UpdateWorkerContext,
    params: InputParams,
    cmd_args_template: RFCmdArgs,
    json_mode: bool,
    time_out: u64,
    skip_pre_flight_checks: bool,
    expected_inventory: Option<Vec<String>>,
) -> WorkerResult {
    let ip_display = NvUtils::sanitize_log(&params.ip);
    let mut json_dict: Option<Value> = if json_mode {
        Some(json!({ "Error": [], "Error Code": 0, "Output": [] }))
    } else {
        None
    };
    let mut err_status: i32 = 0;

    if !json_mode {
        println!("Updating ip address: {}", ip_display);
    }

    // Connect to BMC
    let bmc_result =
        BmcAccess::get_bmc_access(&params.target_args, context.trace, json_dict.as_mut()).await;
    let (bmc_access, platform_type) = match bmc_result {
        Ok((access, pt)) => (access, pt),
        Err(msg) => {
            Util::bail_nvfwupd_threadsafe(
                1,
                &format!("Unable to access BMC {}", ip_display),
                BailAction::DoNothing,
                json_dict.as_ref(),
                true,
            );
            if let Some(ref mut jd) = json_dict {
                append_json_error_lines(jd, &msg);
                append_json_error_lines(jd, &format!("Unable to access BMC {}", ip_display));
            }
            return WorkerResult {
                input: params.clone(),
                task_id_list: Vec::new(),
                rf_target: None,
                json_dict,
                err_status: 1,
                is_powershelf: false,
            };
        }
    };

    // Validate recipe file types
    for recipe in &params.package_list {
        if !recipe.ends_with("fwpkg") && !recipe.ends_with("tar") {
            Util::bail_nvfwupd_threadsafe(
                1,
                "Invalid Firmware Package selected.",
                BailAction::PrintDivider,
                json_dict.as_ref(),
                true,
            );
            if let Some(ref mut jd) = json_dict {
                if let Some(arr) = jd.get_mut("Error").and_then(|v| v.as_array_mut()) {
                    arr.push(Value::String(
                        "Invalid Firmware Package selected.".to_string(),
                    ));
                }
            }
            return WorkerResult {
                input: params.clone(),
                task_id_list: Vec::new(),
                rf_target: None,
                json_dict,
                err_status: 1,
                is_powershelf: false,
            };
        }
    }

    // Initialize platform
    let mut rf_target = match init_platform_from_config(
        bmc_access,
        platform_type.as_deref(),
        context.config_dict.as_ref(),
        json_dict.as_ref(),
        true,
    ) {
        Some(t) => t,
        None => {
            err_status = 1;
            return WorkerResult {
                input: params.clone(),
                task_id_list: Vec::new(),
                rf_target: None,
                json_dict,
                err_status,
                is_powershelf: false,
            };
        }
    };

    let is_powershelf = rf_target.class_name() == "PowerShelfRFTarget";

    if let Err(message) = validate_expected_inventory_for_cli(
        rf_target.as_ref(),
        expected_inventory.as_deref(),
        context.trace,
        json_dict.as_mut(),
    )
    .await
    {
        Util::bail_nvfwupd_threadsafe(1, &message, BailAction::DoNothing, json_dict.as_ref(), true);
        if let Some(ref mut jd) = json_dict {
            append_json_error_lines(jd, &message);
        }
        return WorkerResult {
            input: params.clone(),
            task_id_list: Vec::new(),
            rf_target: None,
            json_dict,
            err_status: 1,
            is_powershelf,
        };
    }

    if !json_mode {
        println!("FW package: {:?}", params.package_list);
    }

    // Build per-worker cmd_args with overrides from config
    let mut worker_cmd_args = cmd_args_template.clone();
    if worker_cmd_args.special.is_none() {
        if let Some(ref s) = params.special {
            worker_cmd_args.special = Some(vec![s.clone()]);
        }
    }
    if worker_cmd_args.oem_parameters.is_none() {
        if let Some(ref o) = params.oem_parameters {
            worker_cmd_args.oem_parameters = Some(vec![o.clone()]);
        }
    }

    // Create per-target package parser
    let mut pkg_parser = pldm::get_pkg_parser(
        &params.package_list[0],
        context.g_verbose,
        false,
        json_dict.as_ref(),
    )
    .await;

    // Parse packages
    for recipe in &params.package_list {
        let (ok, msg) = pkg_parser.parse_pkg(recipe, None).await;
        if !ok {
            Util::bail_nvfwupd_threadsafe(
                1,
                &format!("WARN: {} is not a valid package. {}", recipe, msg),
                BailAction::DoNothing,
                json_dict.as_ref(),
                true,
            );
        }
    }

    // Run update with parallel_update=true (collects task IDs only)
    let mut adapter = FirmwarePkgAdapter {
        inner: pkg_parser.as_mut(),
    };
    let (status, task_id_list_raw) = rf_target
        .start_update_monitor(
            &params.package_list,
            &mut adapter,
            &worker_cmd_args,
            time_out,
            true, // parallel_update
            json_dict.as_mut(),
            params.update_delay,
            skip_pre_flight_checks,
            Some(UpdatePreconditionMode::SingleShot),
        )
        .await;

    err_status |= status;
    pkg_parser.remove_files().await;

    // Wrap raw task ID strings into TaskId objects
    let task_id_list: Vec<TaskId> = task_id_list_raw
        .into_iter()
        .map(|id| TaskId::new(id, None, None))
        .collect();

    // Annotate JSON output with system_name if present
    if let Some(ref sn) = params.system_name {
        if let Some(ref mut jd) = json_dict {
            if let Some(outputs) = jd.get_mut("Output").and_then(|v| v.as_array_mut()) {
                for item in outputs.iter_mut() {
                    if let Some(obj) = item.as_object_mut() {
                        obj.insert("system_name".to_string(), Value::String(sn.clone()));
                    }
                }
            }
        }
    }

    WorkerResult {
        input: params.clone(),
        task_id_list,
        rf_target: Some(rf_target),
        json_dict,
        err_status,
        is_powershelf,
    }
}

fn input_ordered_worker_results(
    mut indexed_worker_results: Vec<(usize, WorkerResult)>,
) -> Vec<WorkerResult> {
    indexed_worker_results.sort_by_key(|(index, _)| *index);
    indexed_worker_results
        .into_iter()
        .map(|(_, result)| result)
        .collect()
}

// ===========================================================================
// FwUpdCmdToolVersion
// ===========================================================================

pub struct FwUpdCmdToolVersion<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdToolVersion<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    #[allow(dead_code)]
    pub fn print_version() {
        println!("nvfwupd version {}", version::NVFWUPD_CLI_VERSION);
    }

    pub async fn run_command(&self) {
        let _ = self.base.parse_cmd_args();
        println!(
            "{} version {}",
            self.base.exec_name,
            version::NVFWUPD_CLI_VERSION
        );
    }
}

// ===========================================================================
// FwUpdCmdHelp
// ===========================================================================

pub struct FwUpdCmdHelp<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdHelp<'a> {
    const HELP_WRAP_WIDTH: usize = 100;

    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    fn print_wrapped_line(indent: &str, text: &str) {
        let mut line = String::new();
        for word in text.split_whitespace() {
            let next_len = if line.is_empty() {
                word.len()
            } else {
                line.len() + 1 + word.len()
            };

            if !line.is_empty() && indent.len() + next_len > Self::HELP_WRAP_WIDTH {
                println!("{indent}{line}");
                line.clear();
            }

            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }

        if !line.is_empty() {
            println!("{indent}{line}");
        }
    }

    fn print_target_option_notes() {
        const SERVER_TYPES: &str = "DGX, DGXRUBIN, HGX, MGX, GH200, HGXB100, HGXB300, HGXRUBIN, GB200, GB300, VRNVL72, GB200Switch, GB300Switch, VRNVL72Switch, Powershelf";
        const NOTES: &[&str] = &[
            "Target options: port=<port num for port forwarding>, servertype=<Type of server>.",
            "OS target options: port=<SSH port>, servertype=<Type of server>.",
        ];

        println!("Target option notes:");
        for note in NOTES {
            Self::print_wrapped_line("    ", note);
            println!();
        }
        Self::print_wrapped_line(
            "    ",
            &format!("Supported servertype values: {SERVER_TYPES}."),
        );
        println!();
    }

    fn print_global_options(schema: &CLISchema) {
        println!("Global options:");
        if let Some(ref data) = schema.schema_data {
            for opt in &data.global_options.options {
                for (_name, entry) in opt {
                    println!(
                        "    -{} --{} {}",
                        entry.short,
                        entry.long,
                        entry.arg.as_deref().unwrap_or("")
                    );
                    Self::print_wrapped_line("           ", &entry.description);
                    println!();
                }
            }
        }
        Self::print_target_option_notes();
    }

    pub fn print_usage(exec_name: &str, text: &str, schema: &CLISchema) {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let args_str = args.join(" ");
        if !args_str.is_empty() {
            println!("{}: {}", text, args_str);
        } else {
            println!("{}", text);
        }
        println!();

        println!("{} version {}", exec_name, version::NVFWUPD_CLI_VERSION);
        println!();
        println!("Usage: {} [ global options ] <command>", exec_name);
        println!();

        Self::print_global_options(schema);

        println!("Commands:");
        if let Some(ref data) = schema.schema_data {
            // Commands not requiring global options.
            // Python parity: command lines are indented with 5 spaces and
            // option lines with 9 spaces (Rust previously used 4 and 8).
            for cmd in &data.commands {
                if !cmd.require_global_option {
                    let options = schema.get_command_options(&cmd.name);
                    if !options.is_empty() {
                        println!("     {} [ options... ]", cmd.name);
                    } else {
                        println!("     {:<10} {:<40}", cmd.name, cmd.description);
                    }
                    for each_option in &options {
                        let option_str = format!(
                            "-{}  --{}",
                            each_option.short.as_deref().unwrap_or(""),
                            each_option.long
                        );
                        println!(
                            "         {:<20} {:<60}",
                            option_str, each_option.description
                        );
                    }
                    println!();
                }
            }
            // Commands requiring global options
            for cmd in &data.commands {
                if cmd.require_global_option {
                    if cmd.name == "perform_factory_reset" || cmd.name == "install_license" {
                        println!("     <Global options...> {}", cmd.name);
                    } else {
                        println!("     <Global options...> {} [ options... ]", cmd.name);
                    }
                    let options = schema.get_command_options(&cmd.name);
                    for each_option in &options {
                        let option_str =
                            if cmd.name == "force_update" && each_option.short.is_none() {
                                "enable|disable|status".to_string()
                            } else {
                                format!(
                                    "-{}  --{}",
                                    each_option.short.as_deref().unwrap_or(""),
                                    each_option.long
                                )
                            };
                        let mut description = each_option.description.clone();
                        if cmd.name == "activate_fw" && each_option.short.as_deref() == Some("c") {
                            // Python repr of a list uses single quotes, not
                            // Rust's Debug format which uses double quotes.
                            let cmds_list = FwUpdCmdActivateFirmware::SUPPORTED_COMMANDS
                                .iter()
                                .map(|s| format!("'{}'", s))
                                .collect::<Vec<_>>()
                                .join(", ");
                            description = format!(
                                "{}. List of supported commands [{}]",
                                description, cmds_list
                            );
                        }
                        println!("         {:<28} {:<60}", option_str, description);
                    }
                    println!();
                }
            }
        }
    }

    pub async fn run_command(&self) {
        let _ = self.base.parse_cmd_args();
        println!(
            "{} version {}",
            self.base.exec_name,
            version::NVFWUPD_CLI_VERSION
        );
        println!();
        println!(
            "Usage: {} [ global options ] <command>",
            self.base.exec_name
        );
        println!();

        Self::print_global_options(&self.base.schema);

        println!("Commands:");
        if let Some(ref data) = self.base.schema.schema_data {
            // Python parity: 5-space indent for command lines, 9-space for
            // option lines (Rust previously used 4 / 8).
            for cmd in &data.commands {
                if !cmd.require_global_option {
                    let options = self.base.schema.get_command_options(&cmd.name);
                    if !options.is_empty() {
                        println!("     {} [ options... ]", cmd.name);
                    } else {
                        println!("     {:<10} {:<40}", cmd.name, cmd.description);
                    }
                    for each_option in &options {
                        let option_str = format!(
                            "-{}  --{}",
                            each_option.short.as_deref().unwrap_or(""),
                            each_option.long
                        );
                        println!(
                            "         {:<20} {:<60}",
                            option_str, each_option.description
                        );
                    }
                    println!();
                }
            }
            for cmd in &data.commands {
                if cmd.require_global_option {
                    if cmd.name == "perform_factory_reset" || cmd.name == "install_license" {
                        println!("     <Global options...> {}", cmd.name);
                    } else {
                        println!("     <Global options...> {} [ options... ]", cmd.name);
                    }
                    let options = self.base.schema.get_command_options(&cmd.name);
                    for each_option in &options {
                        let option_str =
                            if cmd.name == "force_update" && each_option.short.is_none() {
                                "enable|disable|status".to_string()
                            } else {
                                format!(
                                    "-{}  --{}",
                                    each_option.short.as_deref().unwrap_or(""),
                                    each_option.long
                                )
                            };
                        let mut description = each_option.description.clone();
                        if cmd.name == "activate_fw" && each_option.short.as_deref() == Some("c") {
                            // Python repr of a list uses single quotes, not
                            // Rust's Debug format which uses double quotes.
                            let cmds_list = FwUpdCmdActivateFirmware::SUPPORTED_COMMANDS
                                .iter()
                                .map(|s| format!("'{}'", s))
                                .collect::<Vec<_>>()
                                .join(", ");
                            description = format!(
                                "{}. List of supported commands [{}]",
                                description, cmds_list
                            );
                        }
                        println!("         {:<28} {:<60}", option_str, description);
                    }
                    println!();
                }
            }
        }
    }
}

// ===========================================================================
// FwUpdCmdShowRecipe
// ===========================================================================

pub struct FwUpdCmdShowRecipe<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdShowRecipe<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let cmd_args = self.base.parse_cmd_args();
        let recipe_list = cmd_args.get_list("package").to_vec();

        if recipe_list.is_empty() {
            // Python parity: `-p/--package` is marked Required: true in
            // cli_schema.yaml and argparse emits a usage/error pair with
            // exit code 2 when it's missing.
            eprintln!("usage: show_pkg_content -p/--package [fwpkg]");
            eprintln!("nvfwupd: error: the following arguments are required: -p/--package");
            std::process::exit(2);
        }

        self.base
            .validate_recipes(Some(recipe_list.clone()), None)
            .await;

        for pkg_file in &recipe_list {
            let mut pkg_parser =
                pldm::get_pkg_parser(pkg_file, self.base.g_verbose, false, None).await;

            // Check if this is a PLDM parser
            if !pkg_file.ends_with(".fwpkg") {
                // Python emits: "Error: incorrect package format." then
                // "Given input file <path> is not a valid PLDM fwpkg" and
                // bails with Error Code: 1.
                println!("Error: incorrect package format.");
                println!("Given input file {} is not a valid PLDM fwpkg", pkg_file);
                Util::bail_nvfwupd(1, "", BailAction::Exit, None);
                continue;
            }

            let (status, msg) = pkg_parser.parse_pkg(pkg_file, None).await;
            if !status {
                println!("WARN: {} is not a valid PLDM package. Ignoring", pkg_file);
                println!("{}", msg);
                Util::bail_nvfwupd(1, "", BailAction::PrintDivider, None);
                continue;
            }

            pkg_parser.print_package_content(pkg_file);

            if recipe_list.len() == 1 {
                std::process::exit(0);
            } else {
                Util::bail_nvfwupd(0, "", BailAction::PrintDivider, None);
            }
        }
    }
}

// ===========================================================================
// FwUpdCmdUnpackPLDM
// ===========================================================================

pub struct FwUpdCmdUnpackPLDM<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdUnpackPLDM<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let cmd_args = self.base.parse_cmd_args();
        let recipe_list = cmd_args.get_list("package").to_vec();
        let outdir = cmd_args.get_string("outdir").unwrap_or(".");

        if recipe_list.is_empty() {
            // Python parity: `-p/--package` is marked Required: true in
            // cli_schema.yaml; argparse emits usage/error + exit 2.
            eprintln!(
                "usage: unpack -o/--outdir [dirpath to save unpacked files] -p/--package [fwpkg]"
            );
            eprintln!("nvfwupd: error: the following arguments are required: -p/--package");
            std::process::exit(2);
        }

        self.base
            .validate_recipes(Some(recipe_list.clone()), None)
            .await;

        for pkg_file in &recipe_list {
            if !pkg_file.ends_with(".fwpkg") {
                println!("WARN: {} is not a PLDM package.", pkg_file);
                Util::bail_nvfwupd(1, "", BailAction::PrintDivider, None);
                continue;
            }

            // PLDM-specific unpack
            let mut pldm = PLDM::new();
            pldm.verbose = self.base.g_verbose;
            let (status, _, msg) = pldm.unpack_pkg(pkg_file, outdir, true).await;
            if !status {
                println!("WARN: {} is not a valid PLDM package. Ignoring", pkg_file);
                println!("{}", msg);
                Util::bail_nvfwupd(1, "", BailAction::PrintDivider, None);
                continue;
            }

            pldm.print_package(pkg_file);
            Util::bail_nvfwupd(0, "", BailAction::PrintDivider, None);
        }
    }
}

// ===========================================================================
// FwUpdCmdShowVersion
// ===========================================================================

pub struct FwUpdCmdShowVersion<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdShowVersion<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    fn package_set_has_hgx_package(pkg_parser: &dyn FirmwarePkg) -> bool {
        pkg_parser
            .apname_version_dict()
            .keys()
            .any(|pkg_name| pkg_name.to_ascii_uppercase().contains("HGX"))
    }

    fn is_hgx_inventory_ap(ap_name: &str) -> bool {
        ap_name.starts_with("hgx_")
    }

    fn is_dgx_target(rf_target: &dyn RFTarget) -> bool {
        rf_target.class_name() == "DGX_RFTarget"
    }

    fn skip_hgx_matching_for_dgx_only_package(
        rf_target: &dyn RFTarget,
        ap_name: &str,
        pkg_parser: &dyn FirmwarePkg,
    ) -> bool {
        Self::is_dgx_target(rf_target)
            && Self::is_hgx_inventory_ap(ap_name)
            && !Self::package_set_has_hgx_package(pkg_parser)
    }

    pub async fn run_command(&mut self) {
        let (global_args, cmd_args) = self.base.validate_cmd(None);

        let json_mode = cmd_args.get_bool("json");
        let show_staged = cmd_args.get_bool("staged");
        let show_software_inventory = cmd_args.get_bool("inventory");

        let json_error_return: Option<Value> = if json_mode {
            Some(json!({"Error": [], "Error Code": 0, "Output": []}))
        } else {
            None
        };

        let list_of_target_ips = self
            .base
            .validate_target_json(&global_args, json_error_return.as_ref())
            .await;

        let recipe_args = cmd_args.get_list("package").to_vec();
        let recipe_list = self
            .base
            .validate_recipes(
                if recipe_args.is_empty() {
                    None
                } else {
                    Some(recipe_args)
                },
                json_error_return.as_ref(),
            )
            .await;

        // Determine if parallel update is set
        let parallel_update = self
            .base
            .config_parser
            .as_ref()
            .and_then(|cp| cp.config_dict.as_ref())
            .and_then(|d| d.get("ParallelUpdate"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut pkg_parser: Option<Box<dyn FirmwarePkg>> = None;

        // For non-parallel, create a single parser from the first package
        if let Some(ref rl) = recipe_list {
            if !rl.is_empty() && !parallel_update {
                let mut parser = pldm::get_pkg_parser(
                    &rl[0],
                    self.base.g_verbose,
                    false,
                    json_error_return.as_ref(),
                )
                .await;
                for pkg_file in rl {
                    let (status, msg) = parser.parse_pkg(pkg_file, None).await;
                    if !status {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "WARN: {} is not a valid package. Ignoring, {}",
                                pkg_file, msg
                            ),
                            BailAction::PrintDivider,
                            json_error_return.as_ref(),
                        );
                        continue;
                    }
                }
                parser.remove_files().await;
                pkg_parser = Some(parser);
            }
        }

        let mut all_inv_status: i32 = 0;

        if parallel_update {
            // =============================================================
            // Parallel show_version path
            // =============================================================
            let input_params_list = FwUpdCmdBase::create_input_params_list(
                &list_of_target_ips,
                recipe_list.as_ref().map(|v| v.as_slice()).unwrap_or(&[]),
            );

            let max_workers = self
                .base
                .get_parallel_max_workers(json_error_return.as_ref());

            // Worker result: (inv_err, json_output, worker_json_dict, system_name, package_list)
            type ShowVersionResult = (i32, Value, Option<Value>, Option<String>, Vec<String>);
            let mut results: Vec<(usize, ShowVersionResult)> = Vec::new();

            for (chunk_index, chunk) in input_params_list.chunks(max_workers).enumerate() {
                let mut join_set: JoinSet<(usize, ShowVersionResult)> = JoinSet::new();

                for (offset, params) in chunk.iter().cloned().enumerate() {
                    let result_index = chunk_index * max_workers + offset;
                    let worker_schema = self.base.schema.clone();
                    let exec_name = self.base.exec_name.clone();
                    let cmd_name = self.base.cmd_name.clone();
                    let cmd_schema = self.base.cmd_schema.clone();
                    let global_options = self.base.global_options.clone();
                    let args = self.base.args.clone();
                    let trace = self.base.trace;
                    let config_parser = self.base.config_parser.clone();
                    let g_verbose = self.base.g_verbose;

                    join_set.spawn(async move {
                        if check_exit_requested() {
                            return (result_index, (1, json!({}), None, None, Vec::new()));
                        }

                        let worker_base = FwUpdCmdBase {
                            schema: &worker_schema,
                            exec_name,
                            cmd_name,
                            cmd_schema,
                            global_options,
                            args,
                            trace,
                            config_parser,
                            g_verbose,
                        };

                        // Per-worker JSON dict (deep copy) for error accumulation
                        let worker_json_dict: Option<Value> = if json_mode {
                            Some(json!({"Error": [], "Error Code": 0, "Output": []}))
                        } else {
                            None
                        };

                        let mut target_pkg_parser: Option<Box<dyn FirmwarePkg>> = None;
                        if !params.package_list.is_empty() {
                            let mut parser = pldm::get_pkg_parser(
                                &params.package_list[0],
                                g_verbose,
                                false,
                                worker_json_dict.as_ref(),
                            )
                            .await;
                            for pkg_file in &params.package_list {
                                let (status, msg) = parser.parse_pkg(pkg_file, None).await;
                                if !status {
                                    Util::bail_nvfwupd_threadsafe(
                                        1,
                                        &format!(
                                            "WARN: {} is not a valid package. {}",
                                            pkg_file, msg
                                        ),
                                        BailAction::PrintDivider,
                                        worker_json_dict.as_ref(),
                                        true,
                                    );
                                }
                            }
                            parser.remove_files().await;
                            target_pkg_parser = Some(parser);
                        }

                        let (inv_err, json_output) = FwUpdCmdShowVersion::get_output_json_static(
                            &worker_base,
                            &params.target_args,
                            target_pkg_parser.as_ref(),
                            if params.package_list.is_empty() {
                                None
                            } else {
                                Some(&params.package_list)
                            },
                            show_staged,
                            show_software_inventory,
                            worker_json_dict.as_ref(),
                            true,
                        )
                        .await;

                        let result = (
                            inv_err,
                            json_output,
                            worker_json_dict,
                            params.system_name,
                            params.package_list,
                        );
                        (result_index, result)
                    });
                }

                while let Some(join_result) = join_set.join_next().await {
                    match join_result {
                        Ok(result) => results.push(result),
                        Err(err) => {
                            Util::bail_nvfwupd(
                                1,
                                &format!("Parallel show_version worker failed: {}", err),
                                BailAction::PrintDivider,
                                json_error_return.as_ref(),
                            );
                            all_inv_status = 1;
                        }
                    }
                }
            }

            results.sort_by_key(|(index, _)| *index);

            // Merge results
            let mut parallel_json_output: Vec<Value> = Vec::new();

            for (_index, (inv_err, mut json_output, worker_json_dict, system_name, package_list)) in
                results
            {
                all_inv_status |= inv_err;

                if json_mode {
                    // If json_output is empty or missing key fields, use the
                    // worker's json_dict (which may contain error info) instead.
                    let is_empty = json_output
                        .as_object()
                        .map(|o| o.is_empty())
                        .unwrap_or(true);
                    let has_model = json_output.get("System Model").is_some();
                    let effective_output = if (is_empty || !has_model) && worker_json_dict.is_some()
                    {
                        let mut wjd = worker_json_dict.unwrap();
                        if let Some(ref sn) = system_name {
                            wjd["System Name"] = json!(sn);
                        }
                        wjd
                    } else {
                        if let Some(ref sn) = system_name {
                            json_output["System Name"] = json!(sn);
                        }
                        json_output
                    };
                    parallel_json_output.push(effective_output);
                } else {
                    if let Some(ref sn) = system_name {
                        println!("Displaying version info for {}", sn);
                    }
                    let rl = if package_list.is_empty() {
                        None
                    } else {
                        Some(&package_list)
                    };
                    self.print_output_json(
                        &json_output,
                        rl,
                        json_mode,
                        show_staged,
                        all_inv_status,
                        None,
                        false,
                    );
                }
            }

            if json_mode {
                let arr = json!(parallel_json_output);
                self.print_output_json(
                    &arr,
                    None,
                    json_mode,
                    show_staged,
                    all_inv_status,
                    None,
                    true,
                );
                std::process::exit(all_inv_status);
            } else {
                // Parity with Python: emit trailing "Error Code: N" line
                // and exit with the aggregate inventory status.
                Util::bail_nvfwupd(all_inv_status, "", BailAction::Exit, None);
            }
        } else {
            // =============================================================
            // Serial show_version path
            // =============================================================
            for target_args in &list_of_target_ips {
                let (inv_err, json_output) = self
                    .get_output_json(
                        target_args,
                        pkg_parser.as_ref(),
                        recipe_list.as_ref(),
                        show_staged,
                        show_software_inventory,
                        json_error_return.as_ref(),
                        false,
                    )
                    .await;

                all_inv_status |= inv_err;
                self.print_output_json(
                    &json_output,
                    recipe_list.as_ref(),
                    json_mode,
                    show_staged,
                    all_inv_status,
                    json_error_return.as_ref(),
                    false,
                );
            }

            if json_mode {
                // JSON mode: Error Code is already embedded in the printed
                // JSON dict via print_output_json. Exit with the aggregate
                // status so callers (and CI) observe the right exit code,
                // mirroring the parallel_json_output path above.
                std::process::exit(all_inv_status);
            } else {
                // Parity with Python: emit trailing "Error Code: N" line
                // and exit with the aggregate inventory status.
                Util::bail_nvfwupd(all_inv_status, "", BailAction::Exit, None);
            }
        }
    }

    /// Static version callable from parallel threads without borrowing `self`.
    async fn get_output_json_static(
        base: &FwUpdCmdBase<'_>,
        target_args: &[String],
        pkg_parser: Option<&Box<dyn FirmwarePkg>>,
        recipe_list: Option<&Vec<String>>,
        show_staged: bool,
        show_software_inventory: bool,
        json_dict: Option<&Value>,
        parallel_operation: bool,
    ) -> (i32, Value) {
        let mut json_output = json!({});
        let bmc_ip_raw = target_args
            .first()
            .map(|s| s.as_str())
            .unwrap_or("unknown")
            .to_string();
        let bmc_ip = NvUtils::sanitize_log(&bmc_ip_raw);

        let bmc_result = BmcAccess::get_bmc_access(target_args, base.trace, None).await;
        let (bmc_access, platform_type) = match bmc_result {
            Ok((access, pt)) => (access, pt),
            Err(msg) => {
                json_output["Connection Status"] = json!("Failed");
                // Re-sanitize bmc_ip: get_bmc_access populates the global
                // sanitize_config from target_args on entry, but the initial
                // sanitize above ran before that population so the redaction
                // was a no-op. Re-applying now that the filter is loaded
                // produces the Python-matching "ip=XXXX" form.
                let bmc_ip = NvUtils::sanitize_log(&bmc_ip_raw);
                append_json_error_lines(&mut json_output, &msg);
                append_json_error_lines(
                    &mut json_output,
                    &format!("Error : Unable to access BMC {}", bmc_ip),
                );
                Util::bail_nvfwupd_threadsafe(
                    1,
                    &format!("Error : Unable to access BMC {}", bmc_ip),
                    BailAction::DoNothing,
                    json_dict,
                    parallel_operation,
                );
                return (1, json_output);
            }
        };

        json_output["Connection Status"] = json!("Successful");

        if let Some(ref rl) = recipe_list {
            for recipe in rl.iter() {
                if !recipe.ends_with("fwpkg") && !recipe.ends_with("tar") {
                    Util::bail_nvfwupd_threadsafe(
                        1,
                        "Invalid Firmware Package selected.",
                        BailAction::Exit,
                        json_dict,
                        parallel_operation,
                    );
                }
            }
        }

        let (status, inv_err, mut inv_dict) = bmc_access
            .get_firmware_inventory(base.trace, None, Some(&bmc_access.model))
            .await;
        if !status {
            Util::bail_nvfwupd_threadsafe(
                1,
                "Error : Failed to retrieve firmware inventory from BMC",
                BailAction::PrintDivider,
                json_dict,
                parallel_operation,
            );
            return (1, json_output);
        }

        // Merge software inventory when --inventory flag is set
        if show_software_inventory {
            let sw_inv = bmc_access.get_software_inventory(base.trace, None).await;
            inv_dict.extend(sw_inv);
        }

        let pkg_names: Value = if let Some(ref parser) = pkg_parser {
            let keys: Vec<String> = parser.apname_version_dict().keys().cloned().collect();
            json!(keys)
        } else {
            json!("N/A")
        };

        json_output["System Model"] = json!(bmc_access.model);
        json_output["Part number"] = json!(bmc_access.partnumber);
        json_output["Serial number"] = json!(bmc_access.serialnumber);
        json_output["Packages"] = pkg_names;
        json_output["System IP"] = json!(bmc_access.ip);
        json_output["Firmware Devices"] = json!([]);

        let rf_target = base.init_platform(
            bmc_access,
            platform_type.as_deref(),
            json_dict,
            parallel_operation,
        );

        let rf_target: Box<dyn RFTarget + Send + Sync> = match rf_target {
            Some(t) => t,
            None => return (1, json_output),
        };

        for (dev_url, val) in &inv_dict {
            let mut firmware_dev = json!({});
            let ap_inv_name = if !dev_url.contains("OSFP") {
                dev_url.rsplit('/').next().unwrap_or(dev_url).to_string()
            } else {
                dev_url.clone()
            };

            firmware_dev["AP Name"] = json!(ap_inv_name);
            let ap_name = ap_inv_name.to_lowercase();

            let sys_version = val
                .get("Version")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            firmware_dev["Sys Version"] = json!(sys_version);

            if show_staged {
                let staged_version = val
                    .pointer("/Oem/Nvidia/InactiveFirmwareSlot/FirmwareState")
                    .and_then(|v| v.as_str())
                    .and_then(|state| {
                        if state == "Staged" {
                            val.pointer("/Oem/Nvidia/InactiveFirmwareSlot/Version")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| "N/A".to_string());
                firmware_dev["Staged Version"] = json!(staged_version);
            }

            if let Some(ref rl) = recipe_list {
                if !rl.is_empty() {
                    let mut pkg_version = "N/A".to_string();
                    let mut up_to_date = "Yes".to_string();
                    let mut compare_pkg_version = true;

                    if let Some(ref parser) = pkg_parser {
                        if Self::skip_hgx_matching_for_dgx_only_package(
                            rf_target.as_ref(),
                            &ap_name,
                            parser.as_ref(),
                        ) {
                            up_to_date = "N/A".to_string();
                            compare_pkg_version = false;
                        } else if rf_target.is_fungible_component(&ap_name) {
                            pkg_version = "unknown".to_string();
                            if let Some(identifier) =
                                rf_target.get_identifier_from_chassis(dev_url).await
                            {
                                pkg_version = rf_target
                                    .get_version_sku(
                                        &identifier.to_lowercase(),
                                        &json!(parser.apname_version_dict()),
                                        &ap_name,
                                    )
                                    .unwrap_or_else(|| "N/A".to_string());
                            }
                        } else {
                            pkg_version = rf_target
                                .get_component_version(
                                    &json!(parser.apname_version_dict()),
                                    &ap_name,
                                    None,
                                )
                                .await
                                .unwrap_or_else(|| "N/A".to_string());
                        }
                    }

                    if compare_pkg_version
                        && !rf_target.firmware_version_up_to_date(&pkg_version, &sys_version)
                    {
                        up_to_date = "No".to_string();
                    }
                    firmware_dev["Pkg Version"] = json!(pkg_version);
                    firmware_dev["Up-To-Date"] = json!(up_to_date);
                }
            }

            if let Some(devices) = json_output["Firmware Devices"].as_array_mut() {
                devices.push(firmware_dev);
            }
        }

        (inv_err, json_output)
    }

    async fn get_output_json(
        &self,
        target_args: &[String],
        pkg_parser: Option<&Box<dyn FirmwarePkg>>,
        recipe_list: Option<&Vec<String>>,
        show_staged: bool,
        show_software_inventory: bool,
        json_dict: Option<&Value>,
        parallel_operation: bool,
    ) -> (i32, Value) {
        Self::get_output_json_static(
            &self.base,
            target_args,
            pkg_parser,
            recipe_list,
            show_staged,
            show_software_inventory,
            json_dict,
            parallel_operation,
        )
        .await
    }

    fn print_output_json(
        &self,
        json_output: &Value,
        recipe_list: Option<&Vec<String>>,
        json_mode: bool,
        show_staged: bool,
        all_inv_status: i32,
        json_error_return: Option<&Value>,
        parallel_operation: bool,
    ) {
        if parallel_operation && json_mode {
            println!("{}", json_pretty_4space(json_output));
            return;
        }

        if json_mode {
            if let Some(jer) = json_error_return {
                if all_inv_status != 0
                    && jer
                        .get("Error")
                        .and_then(|e| e.as_array())
                        .map(|a| !a.is_empty())
                        .unwrap_or(false)
                {
                    let mut output = jer.clone();
                    if json_output.get("System Model").is_some()
                        && json_output.get("Part number").is_some()
                    {
                        if let Some(arr) = output.get_mut("Output").and_then(|o| o.as_array_mut()) {
                            arr.push(json_output.clone());
                        }
                    }
                    println!("{}", json_pretty_4space(&output));
                    return;
                }
            }
            let mut output = json_output.clone();
            output["Error Code"] = json!(all_inv_status);
            println!("{}", json_pretty_4space(&output));
        } else {
            println!(
                "System Model: {}",
                json_output
                    .get("System Model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("N/A")
            );
            println!(
                "Part number: {}",
                json_output
                    .get("Part number")
                    .and_then(|v| v.as_str())
                    .unwrap_or("N/A")
            );
            println!(
                "Serial number: {}",
                json_output
                    .get("Serial number")
                    .and_then(|v| v.as_str())
                    .unwrap_or("N/A")
            );
            let packages_display = match json_output.get("Packages") {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Array(arr)) => {
                    // Parity with Python: mirror Python's `repr()` output for
                    // a list of strings, which uses single quotes around each
                    // item — e.g. ['a', 'b'] — instead of Rust's Debug format
                    // (`{:?}`) which yields double quotes: ["a", "b"].
                    let items: Vec<String> = arr
                        .iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| format!("'{}'", s))
                        .collect();
                    format!("[{}]", items.join(", "))
                }
                Some(other) => other.to_string(),
                None => "N/A".to_string(),
            };
            println!("Packages: {}", packages_display);
            println!(
                "Connection Status: {}",
                json_output
                    .get("Connection Status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Failed")
            );
            println!();

            let fw_devices = json_output
                .get("Firmware Devices")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            println!("Firmware Devices:");
            let has_recipes = recipe_list.map(|rl| !rl.is_empty()).unwrap_or(false);

            if has_recipes {
                if show_staged {
                    println!(
                        "{:<40} {:<30} {:<30} {:<30} {:<10}",
                        "AP Name", "Sys Version", "Staged Version", "Pkg Version", "Up-To-Date"
                    );
                    println!(
                        "{:<40} {:<30} {:<30} {:<30} {:<10}",
                        "-------", "-----------", "--------------", "-----------", "----------"
                    );
                    for fw in &fw_devices {
                        println!(
                            "{:<40} {:<30} {:<30} {:<30} {:<10}",
                            fw.get("AP Name").and_then(|v| v.as_str()).unwrap_or("N/A"),
                            fw.get("Sys Version")
                                .and_then(|v| v.as_str())
                                .unwrap_or("N/A"),
                            fw.get("Staged Version")
                                .and_then(|v| v.as_str())
                                .unwrap_or("N/A"),
                            fw.get("Pkg Version")
                                .and_then(|v| v.as_str())
                                .unwrap_or("N/A"),
                            fw.get("Up-To-Date")
                                .and_then(|v| v.as_str())
                                .unwrap_or("N/A"),
                        );
                    }
                } else {
                    println!(
                        "{:<40} {:<30} {:<30} {:<10}",
                        "AP Name", "Sys Version", "Pkg Version", "Up-To-Date"
                    );
                    println!(
                        "{:<40} {:<30} {:<30} {:<10}",
                        "-------", "-----------", "-----------", "----------"
                    );
                    for fw in &fw_devices {
                        println!(
                            "{:<40} {:<30} {:<30} {:<10}",
                            fw.get("AP Name").and_then(|v| v.as_str()).unwrap_or("N/A"),
                            fw.get("Sys Version")
                                .and_then(|v| v.as_str())
                                .unwrap_or("N/A"),
                            fw.get("Pkg Version")
                                .and_then(|v| v.as_str())
                                .unwrap_or("N/A"),
                            fw.get("Up-To-Date")
                                .and_then(|v| v.as_str())
                                .unwrap_or("N/A"),
                        );
                    }
                }
            } else if show_staged {
                println!(
                    "{:<40} {:<30} {:<30}",
                    "AP Name", "Sys Version", "Staged Version"
                );
                println!(
                    "{:<40} {:<30} {:<30}",
                    "-------", "-----------", "--------------"
                );
                for fw in &fw_devices {
                    println!(
                        "{:<40} {:<30} {:<30}",
                        fw.get("AP Name").and_then(|v| v.as_str()).unwrap_or("N/A"),
                        fw.get("Sys Version")
                            .and_then(|v| v.as_str())
                            .unwrap_or("N/A"),
                        fw.get("Staged Version")
                            .and_then(|v| v.as_str())
                            .unwrap_or("N/A"),
                    );
                }
            } else {
                println!("{:<40} {:<30}", "AP Name", "Sys Version");
                println!("{:<40} {:<30}", "-------", "-----------");
                for fw in &fw_devices {
                    println!(
                        "{:<40} {:<30}",
                        fw.get("AP Name").and_then(|v| v.as_str()).unwrap_or("N/A"),
                        fw.get("Sys Version")
                            .and_then(|v| v.as_str())
                            .unwrap_or("N/A"),
                    );
                }
            }
            println!("{}", "-".repeat(120));
        }
    }
}

// ===========================================================================
// FwUpdCmdForceUpdate
// ===========================================================================

pub struct FwUpdCmdForceUpdate<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdForceUpdate<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let (global_args, cmd_args) = self.base.validate_cmd(None);
        let list_of_target_ips = self.base.validate_target_json(&global_args, None).await;

        let json_mode = cmd_args.get_bool("json");
        let mut json_output: Option<Value> = if json_mode {
            Some(json!({"Error": [], "Error Code": 0, "Output": []}))
        } else {
            None
        };

        // Get the force update action — stored as "force_upd_action" by the
        // schema-aware parser, or as a positional fallback.
        let action = cmd_args
            .get_string("force_upd_action")
            .or_else(|| cmd_args.get_positional(0))
            .unwrap_or("status")
            .to_lowercase();

        for target_args in &list_of_target_ips {
            let bmc_ip =
                NvUtils::sanitize_log(target_args.first().map(|s| s.as_str()).unwrap_or("unknown"));

            let bmc_result = BmcAccess::get_bmc_access(target_args, self.base.trace, None).await;
            let bmc_access = match bmc_result {
                Ok((access, _)) => access,
                Err(_) => {
                    if !json_mode {
                        println!("BMC Connection Status: Failed");
                    }
                    Util::bail_nvfwupd(
                        1,
                        &format!("Unable to access BMC {}", bmc_ip),
                        BailAction::PrintDivider,
                        json_output.as_ref(),
                    );
                    continue;
                }
            };

            if !json_mode {
                println!("BMC Connection Status: Successful");
            }

            // GET UpdateService
            let (status, task_dict) = bmc_access
                .dispatch_request("GET", "/redfish/v1/UpdateService", None, None)
                .await;
            if !status {
                Util::bail_nvfwupd(
                    1,
                    "Failed to get UpdateService data from the target.",
                    BailAction::PrintDivider,
                    json_output.as_ref(),
                );
                continue;
            }

            // Check if ForceUpdate is supported
            let force_upd = task_dict
                .pointer("/HttpPushUriOptions/ForceUpdate")
                .and_then(|v| v.as_bool());

            if force_upd.is_none() {
                // Python parity (C16): in JSON mode, append the error to the
                // JSON Error array and set Error Code: 1. In non-JSON mode,
                // print the plain message + divider. Previously
                // BailAction::PrintDivider in JSON mode was a silent no-op,
                // leaving Error: [], Error Code: 0.
                let msg = "The force_update command is not supported for this platform.";
                if json_mode {
                    if let Some(ref mut jo) = json_output {
                        if let Some(arr) = jo.get_mut("Error").and_then(|v| v.as_array_mut()) {
                            arr.push(Value::String(msg.to_string()));
                        }
                        jo["Error Code"] = json!(1);
                    }
                } else {
                    println!("{}", msg);
                    println!("{}", "-".repeat(120));
                }
                continue;
            }

            match action.as_str() {
                "status" => {
                    if json_mode {
                        if let Some(ref mut jo) = json_output {
                            if let Some(arr) = jo.get_mut("Output").and_then(|o| o.as_array_mut()) {
                                arr.push(task_dict.clone());
                            }
                        }
                    } else {
                        println!("ForceUpdate is set to {}", force_upd.unwrap());
                        println!("{}", "-".repeat(120));
                    }
                }
                "enable" | "disable" => {
                    let force_upd_val = action == "enable";
                    let force_updset =
                        json!({"HttpPushUriOptions": {"ForceUpdate": force_upd_val}});

                    let (status, err_dict) = bmc_access
                        .dispatch_request(
                            "PATCH",
                            "/redfish/v1/UpdateService",
                            Some(&force_updset),
                            None,
                        )
                        .await;
                    if !status {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "Patch command for UpdateService data failed err: {}",
                                NvUtils::redact_secret_json_value(&err_dict)
                            ),
                            BailAction::PrintDivider,
                            json_output.as_ref(),
                        );
                        continue;
                    }

                    if json_mode {
                        if let Some(ref mut jo) = json_output {
                            if let Some(arr) = jo.get_mut("Output").and_then(|o| o.as_array_mut()) {
                                arr.push(json!({"ForceUpdate": force_upd_val}));
                            }
                        }
                    } else {
                        println!(
                            "ForceUpdate flag was successfully set {} on the system.",
                            force_upd_val
                        );
                        println!("{}", "-".repeat(120));
                    }
                }
                _ => {
                    Util::bail_nvfwupd(
                        1,
                        "Incorrect input option to force_update command.",
                        BailAction::PrintDivider,
                        json_output.as_ref(),
                    );
                    continue;
                }
            }
        }

        if let Some(ref jo) = json_output {
            println!("{}", json_pretty_4space(jo));
        }
    }
}

// ===========================================================================
// FwUpdCmdUpdateFirmware
// ===========================================================================

pub struct FwUpdCmdUpdateFirmware<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdUpdateFirmware<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let (global_args, cmd_args) = self.base.validate_cmd(None);

        let json_mode = cmd_args.get_bool("json");
        let background = cmd_args.get_bool("background");
        let yes = cmd_args.get_bool("yes");
        let details = cmd_args.get_bool("details");
        let staged_update = cmd_args.get_bool("staged_update");
        let staged_activate_update = cmd_args.get_bool("staged_activate_update");
        let mut skip_pre_flight_checks = cmd_args.get_bool("skip_pre_flight_checks");
        let timeout_str = cmd_args.get_string("timeout");
        let expected_inventory_file = cmd_args
            .get_string("expected_inventory")
            .filter(|path| !path.is_empty())
            .map(str::to_string);

        let mut json_output: Option<Value> = if json_mode {
            Some(json!({"Error": [], "Error Code": 0, "Output": []}))
        } else {
            None
        };

        // Merge SkipPreFlightChecks from config file when CLI flag is not set
        if !skip_pre_flight_checks {
            if let Some(ref cp) = self.base.config_parser {
                if let Some(ref config) = cp.config_dict {
                    match config.get("SkipPreFlightChecks") {
                        Some(Value::Bool(b)) => skip_pre_flight_checks = *b,
                        Some(_) => {
                            Util::bail_nvfwupd(
                                1,
                                "Error: Improper format for SkipPreFlightChecks parameter. \
                                 Expected boolean (true/false).",
                                BailAction::DoNothing,
                                json_output.as_ref(),
                            );
                        }
                        None => {}
                    }
                }
            }
        }

        // Validate required arguments BEFORE option-combination checks.
        // Python validates required args (-p/--package) before argument
        // combinations like -j/-b compatibility. Doing this in the wrong
        // order produced "JSON update is not supported without --background
        // option." for `update_fw -j` when Python said
        // "Missing command option -p/--package".
        //
        // Note: this is an argparse-equivalent error. Python emits plain
        // text even in -j/--json mode because argparse fires before the
        // application considers -j. Pass None for the JSON dict so we
        // match the plain-text output format.
        let recipe_args = cmd_args.get_list("package").to_vec();
        if recipe_args.is_empty() && self.base.config_parser.is_none() {
            Util::bail_nvfwupd(
                1,
                "Missing command option -p/--package",
                BailAction::Exit,
                None,
            );
            return;
        }

        // Validate option combinations (after required-arg checks).
        if details && json_mode {
            Util::bail_nvfwupd(
                1,
                "Table update progress is not supported with json option.",
                BailAction::Exit,
                json_output.as_ref(),
            );
        }

        if json_mode && !background {
            Util::bail_nvfwupd(
                1,
                "JSON update is not supported without --background option.",
                BailAction::Exit,
                json_output.as_ref(),
            );
        }

        if staged_update && staged_activate_update {
            // Safety: exit immediately on conflicting flags. Previously
            // DoNothing let the command fall through and actually run the
            // firmware update despite the error message, launching a task
            // the user explicitly asked to cancel via conflicting flags.
            Util::bail_nvfwupd(
                1,
                "Stage only option is not supported alongside stage and activate option",
                BailAction::Exit,
                json_output.as_ref(),
            );
            return;
        }

        let list_of_target_ips = self
            .base
            .validate_target_json(&global_args, json_output.as_ref())
            .await;

        // Determine parallel update
        let parallel_update = self
            .base
            .config_parser
            .as_ref()
            .and_then(|cp| cp.config_dict.as_ref())
            .and_then(|d| d.get("ParallelUpdate"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Validate background with multiple targets
        if background && list_of_target_ips.len() > 1 && !parallel_update {
            // Bail out with exit so we never proceed with an invalid
            // background + multi-target combination. Previously this used
            // DoNothing, which recorded the error but continued execution.
            Util::bail_nvfwupd(
                1,
                "Multi-target update is not supported with --background option.",
                BailAction::Exit,
                json_output.as_ref(),
            );
            return;
        }

        let recipe_list = self
            .base
            .validate_recipes(
                if recipe_args.is_empty() {
                    None
                } else {
                    Some(recipe_args)
                },
                json_output.as_ref(),
            )
            .await;

        if recipe_list.is_none() || recipe_list.as_ref().map(|r| r.is_empty()).unwrap_or(true) {
            // Exit here to match Python behavior. Previously DoNothing was
            // used, which fell through to the unwrap() below and panicked
            // with "called Option::unwrap() on a None value" when -p/--package
            // was omitted on update_fw.
            Util::bail_nvfwupd(
                1,
                "Error: No valid packages input for fw_update",
                BailAction::Exit,
                json_output.as_ref(),
            );
            return;
        }
        let recipe_list = recipe_list.unwrap();

        let expected_inventory = if let Some(expected_inventory_file) =
            expected_inventory_file.as_deref()
        {
            match expected_inventory::read_expected_inventory_file(expected_inventory_file).await {
                Ok(expected_inventory) => Some(expected_inventory),
                Err(message) => {
                    Util::bail_nvfwupd(1, &message, BailAction::Exit, json_output.as_ref());
                    return;
                }
            }
        } else {
            None
        };

        // Create package parser
        let mut pkg_parser = pldm::get_pkg_parser(
            &recipe_list[0],
            self.base.g_verbose,
            false,
            json_output.as_ref(),
        )
        .await;

        // Validate special file (nargs='+', stored as list)
        let special_list = cmd_args.get_list("special");
        let special: Option<Vec<String>> = if special_list.is_empty() {
            None
        } else {
            let first = &special_list[0];
            if serde_json::from_str::<Value>(first).is_err() && !path_is_file(first).await {
                Util::bail_nvfwupd(
                    1,
                    &format!("Special command json file doesn't exist: {}", first),
                    BailAction::PrintDivider,
                    json_output.as_ref(),
                );
                return;
            }
            Some(special_list.to_vec())
        };

        // Validate OEM parameters file (nargs='+', stored as list)
        let oem_list = cmd_args.get_list("oem_parameters");
        let oem_parameters: Option<Vec<String>> = if oem_list.is_empty() {
            None
        } else {
            let first = &oem_list[0];
            if serde_json::from_str::<Value>(first).is_err() && !path_is_file(first).await {
                Util::bail_nvfwupd(
                    1,
                    &format!("Oem Parameters json file doesn't exist: {}", first),
                    BailAction::PrintDivider,
                    json_output.as_ref(),
                );
                return;
            }
            Some(oem_list.to_vec())
        };

        let mut all_update_status: i32 = 0;

        // Timeout
        let time_out: u64 = timeout_str
            .and_then(|s| s.parse().ok())
            .filter(|&t: &u64| t != 0)
            .unwrap_or(900);

        let rf_cmd_args = RFCmdArgs {
            cmd: String::new(),
            background,
            details,
            staged_update,
            staged_activate_update,
            quiet: false,
            special: special.clone(),
            oem_parameters: oem_parameters.clone(),
        };

        if parallel_update {
            // =============================================================
            // Parallel update path
            // =============================================================
            let input_params_list =
                FwUpdCmdBase::create_input_params_list(&list_of_target_ips, &recipe_list);

            if input_params_list.is_empty() {
                Util::bail_nvfwupd(
                    1,
                    "No valid targets for parallel update",
                    BailAction::Exit,
                    json_output.as_ref(),
                );
                return;
            }

            let max_workers = self.base.get_parallel_max_workers(json_output.as_ref());

            // Spawn worker tasks and process in batches to cap concurrency.
            let mut indexed_worker_results: Vec<(usize, WorkerResult)> = Vec::new();
            let worker_context = UpdateWorkerContext {
                trace: self.base.trace,
                config_dict: self
                    .base
                    .config_parser
                    .as_ref()
                    .and_then(|cp| cp.config_dict.clone()),
                g_verbose: self.base.g_verbose,
            };

            for (chunk_index, chunk) in input_params_list.chunks(max_workers).enumerate() {
                let mut join_set = JoinSet::new();

                for (offset, params) in chunk.iter().cloned().enumerate() {
                    let result_index = chunk_index * max_workers + offset;
                    let worker_context = worker_context.clone();
                    let rf_cmd_args = rf_cmd_args.clone();
                    let expected_inventory = expected_inventory.clone();
                    join_set.spawn(async move {
                        (
                            result_index,
                            update_fw_worker(
                                worker_context,
                                params,
                                rf_cmd_args,
                                json_mode,
                                time_out,
                                skip_pre_flight_checks,
                                expected_inventory,
                            )
                            .await,
                        )
                    });
                }

                while let Some(joined) = join_set.join_next().await {
                    match joined {
                        Ok(result) => indexed_worker_results.push(result),
                        Err(err) => {
                            all_update_status = 1;
                            if let Some(ref mut output) = json_output {
                                if let Some(errors) =
                                    output.get_mut("Error").and_then(|v| v.as_array_mut())
                                {
                                    errors.push(Value::String(format!(
                                        "Parallel update worker failed: {}",
                                        err
                                    )));
                                }
                            }
                        }
                    }
                }
            }

            let worker_results = input_ordered_worker_results(indexed_worker_results);

            // Merge per-worker JSON into the main json_output
            if json_mode {
                for wr in &worker_results {
                    if let Some(ref worker_json) = wr.json_dict {
                        if let Some(ref mut main_json) = json_output {
                            if let Some(outputs) =
                                worker_json.get("Output").and_then(|v| v.as_array())
                            {
                                if let Some(main_out) =
                                    main_json.get_mut("Output").and_then(|v| v.as_array_mut())
                                {
                                    main_out.extend(outputs.iter().cloned());
                                }
                            }
                            if let Some(errors) =
                                worker_json.get("Error").and_then(|v| v.as_array())
                            {
                                if let Some(main_err) =
                                    main_json.get_mut("Error").and_then(|v| v.as_array_mut())
                                {
                                    main_err.extend(errors.iter().cloned());
                                }
                            }
                        }
                    }
                }
                if let Some(ref main_json) = json_output {
                    if let Some(errors) = main_json.get("Error").and_then(|v| v.as_array()) {
                        if !errors.is_empty() {
                            all_update_status = 1;
                        }
                    }
                }
            }

            // Post-process: filter out targets with no tasks or bad task IDs
            let mut active_results: Vec<WorkerResult> = Vec::new();
            for mut wr in worker_results {
                if !wr.task_id_list.is_empty() && wr.rf_target.is_some() {
                    // Remove task IDs that are negative integers (bad IDs)
                    wr.task_id_list
                        .retain(|tid| match tid.task_id.parse::<i64>() {
                            Ok(n) if n < 0 => false,
                            _ => true,
                        });
                    if !wr.task_id_list.is_empty() {
                        active_results.push(wr);
                    } else {
                        all_update_status = 1;
                    }
                } else if wr.is_powershelf {
                    if !json_mode {
                        println!(
                            "Firmware update initiated for IP: {}",
                            NvUtils::sanitize_log(&wr.input.ip)
                        );
                        if let Some(ref sn) = wr.input.system_name {
                            println!("System: {}", sn);
                        }
                        println!(
                            "Powershelf firmware update initiated and will activate on its own."
                        );
                        println!("{}", "-".repeat(120));
                    }
                } else {
                    all_update_status |= wr.err_status;
                    if wr.err_status == 0 {
                        all_update_status = 1;
                    }
                }
            }

            // Task success/failure state sets
            let task_success_states: &[&str] = &["completed", "action_success"];
            let task_failure_states: &[&str] = &[
                "cancelled",
                "cancelling",
                "exception",
                "interrupted",
                "killed",
                "stopping",
                "suspended",
                "error",
            ];

            // Helper: query task statuses for all active workers in parallel.
            async fn query_all_tasks_parallel(results: &mut [WorkerResult]) {
                let mut join_set = JoinSet::new();

                for (idx, wr) in results.iter_mut().enumerate() {
                    let Some(rf_target) = wr.rf_target.take() else {
                        continue;
                    };
                    let task_ids: Vec<String> = wr
                        .task_id_list
                        .iter()
                        .map(|task| task.task_id.clone())
                        .collect();

                    join_set.spawn(async move {
                        let mut statuses = Vec::with_capacity(task_ids.len());
                        for task_id in task_ids {
                            let (status, response) =
                                rf_target.query_job_status(&task_id, None).await;
                            statuses.push((status, response));
                        }
                        (idx, rf_target, statuses)
                    });
                }

                while let Some(joined) = join_set.join_next().await {
                    let Ok((idx, rf_target, statuses)) = joined else {
                        continue;
                    };
                    if let Some(wr) = results.get_mut(idx) {
                        for (task, (status, response)) in wr.task_id_list.iter_mut().zip(statuses) {
                            task.status = Some(status);
                            task.response_dict = Some(response);
                        }
                        wr.rf_target = Some(rf_target);
                    }
                }
            }

            // Helper: print task statuses, remove terminal tasks, return
            // updated error status.
            fn print_and_prune_tasks(
                results: &mut Vec<WorkerResult>,
                json_mode: bool,
                task_success_states: &[&str],
                task_failure_states: &[&str],
                mut all_update_status: i32,
            ) -> i32 {
                for wr in results.iter_mut() {
                    if !wr.task_id_list.is_empty() && !json_mode {
                        println!(
                            "Printing Task status for IP: {}",
                            NvUtils::sanitize_log(&wr.input.ip)
                        );
                        if let Some(ref sn) = wr.input.system_name {
                            println!("Printing Task status for system: {}", sn);
                        }
                    }

                    let rf = wr.rf_target.as_ref().unwrap();
                    let mut keep = Vec::new();
                    for task in wr.task_id_list.drain(..) {
                        let resp = task.response_dict.as_ref().cloned().unwrap_or(json!({}));
                        let status = task.status.unwrap_or(false);
                        let (ret_code, job_state) =
                            rf.print_job_status(&task.task_id, &resp, status, None);

                        let state_str = job_state.as_deref().unwrap_or("");
                        let is_terminal = state_str.is_empty()
                            || task_success_states.contains(&state_str)
                            || task_failure_states.contains(&state_str)
                            || ret_code != 0;

                        if is_terminal {
                            if state_str.is_empty()
                                || task_failure_states.contains(&state_str)
                                || ret_code != 0
                            {
                                all_update_status = 1;
                            }
                        } else {
                            keep.push(task);
                        }
                    }
                    wr.task_id_list = keep;
                }
                all_update_status
            }

            // Initial status query and print (non-JSON, non-background)
            if !json_mode && !active_results.is_empty() {
                query_all_tasks_parallel(&mut active_results).await;
                all_update_status = print_and_prune_tasks(
                    &mut active_results,
                    json_mode,
                    task_success_states,
                    task_failure_states,
                    all_update_status,
                );
            }

            // If running in background mode, exit now
            if background {
                Util::bail_nvfwupd(
                    all_update_status,
                    "",
                    BailAction::Exit,
                    json_output.as_ref(),
                );
                return;
            }

            // Ongoing polling loop
            loop {
                if check_exit_requested() {
                    break;
                }

                active_results.retain(|wr| !wr.task_id_list.is_empty());
                if active_results.is_empty() {
                    break;
                }

                query_all_tasks_parallel(&mut active_results).await;
                all_update_status = print_and_prune_tasks(
                    &mut active_results,
                    json_mode,
                    task_success_states,
                    task_failure_states,
                    all_update_status,
                );

                // Sleep 20 seconds in 1-second intervals with exit checking
                for _ in 0..20 {
                    if check_exit_requested() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }

            Util::bail_nvfwupd(
                all_update_status,
                "",
                BailAction::Exit,
                json_output.as_ref(),
            );
        } else {
            // =============================================================
            // Serial (single-target) update path
            // =============================================================
            for target_args in &list_of_target_ips {
                let bmc_ip = NvUtils::sanitize_log(
                    target_args.first().map(|s| s.as_str()).unwrap_or("unknown"),
                );

                if !json_mode {
                    println!("Updating ip address: {}", bmc_ip);
                }

                let bmc_result =
                    BmcAccess::get_bmc_access(target_args, self.base.trace, None).await;
                let (bmc_access, platform_type) = match bmc_result {
                    Ok((access, pt)) => (access, pt),
                    Err(msg) => {
                        Util::bail_nvfwupd(
                            1,
                            &format!("Unable to access BMC {}", bmc_ip),
                            BailAction::PrintDivider,
                            json_output.as_ref(),
                        );
                        if let Some(ref mut output) = json_output {
                            append_json_error_lines(output, &msg);
                            append_json_error_lines(
                                output,
                                &format!("Unable to access BMC {}", bmc_ip),
                            );
                        }
                        all_update_status = 1;
                        continue;
                    }
                };

                // Validate recipe file types
                for recipe in &recipe_list {
                    if !recipe.ends_with("fwpkg") && !recipe.ends_with("tar") {
                        Util::bail_nvfwupd(
                            1,
                            "Invalid Firmware Package selected.",
                            BailAction::Exit,
                            json_output.as_ref(),
                        );
                    }
                }

                let mut rf_target = match self.base.init_platform(
                    bmc_access,
                    platform_type.as_deref(),
                    json_output.as_ref(),
                    false,
                ) {
                    Some(t) => t,
                    None => {
                        all_update_status = 1;
                        continue;
                    }
                };

                if !json_mode {
                    println!("FW package: {:?}", recipe_list);
                }

                if let Err(message) = validate_expected_inventory_for_cli(
                    rf_target.as_ref(),
                    expected_inventory.as_deref(),
                    self.base.trace,
                    json_output.as_mut(),
                )
                .await
                {
                    Util::bail_nvfwupd(1, &message, BailAction::PrintDivider, json_output.as_ref());
                    if let Some(ref mut output) = json_output {
                        append_json_error_lines(output, &message);
                    }
                    all_update_status = 1;
                    continue;
                }

                // Prompt for confirmation (serial only)
                if !yes && !json_mode {
                    print!("Ok to proceed with firmware update? <Y/N>\n");
                    io::stdout().flush().ok();
                    let mut answer = String::new();
                    match io::stdin().read_line(&mut answer) {
                        Ok(_) => {
                            if answer.trim().to_lowercase() != "y" {
                                Util::bail_nvfwupd(
                                    0,
                                    "Exiting firmware update process..!",
                                    BailAction::PrintDivider,
                                    None,
                                );
                                continue;
                            }
                        }
                        Err(_) => {
                            all_update_status = 1;
                            Util::bail_nvfwupd(
                                1,
                                "Exiting firmware update process..!",
                                BailAction::PrintDivider,
                                None,
                            );
                            continue;
                        }
                    }
                }

                // Parse packages for this target
                for recipe in &recipe_list {
                    let (ok, msg) = pkg_parser.parse_pkg(recipe, None).await;
                    if !ok {
                        Util::bail_nvfwupd(
                            1,
                            &format!("WARN: {} is not a valid package. {}", recipe, msg),
                            BailAction::PrintDivider,
                            json_output.as_ref(),
                        );
                        continue;
                    }
                }

                let mut adapter = FirmwarePkgAdapter {
                    inner: pkg_parser.as_mut(),
                };
                let (status, _task_id_list) = rf_target
                    .start_update_monitor(
                        &recipe_list,
                        &mut adapter,
                        &rf_cmd_args,
                        time_out,
                        false,
                        json_output.as_mut(),
                        0,
                        skip_pre_flight_checks,
                        Some(UpdatePreconditionMode::SingleShot),
                    )
                    .await;

                all_update_status |= status;
                pkg_parser.remove_files().await;
                if !json_mode {
                    println!("{}", "-".repeat(120));
                }
            }

            Util::bail_nvfwupd(
                all_update_status,
                "",
                BailAction::Exit,
                json_output.as_ref(),
            );
        }
    }
}

// ===========================================================================
// FwUpdCmdActivateFirmware
// ===========================================================================

pub struct FwUpdCmdActivateFirmware<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdActivateFirmware<'a> {
    pub const SUPPORTED_COMMANDS: &'static [&'static str] = &[
        "PWR_STATUS",
        "PWR_OFF",
        "PWR_ON",
        "PWR_CYCLE",
        "RESET_COLD",
        "RESET_WARM",
        "NVUE_PWR_CYCLE",
        "RF_AUX_PWR_CYCLE",
        "RF_PWR_ON",
        "RF_PWR_OFF",
        "RF_PWR_CYCLE",
        "RF_PWR_STATUS",
        "RF_PWRSHELF_RESET",
        "RF_PWRSHELF_RESET_FORCE",
    ];

    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let (global_args, cmd_args) = self.base.validate_cmd(None);

        // Safety: `-c/--cmd` is marked Required: true in cli_schema.yaml.
        // Previously we fell back to `PWR_CYCLE` via `unwrap_or`, which
        // silently executed a power-cycle IPMI command on every target
        // when the user omitted `-c`. Enforce the required argument the
        // same way Python argparse does before touching any BMC.
        let command = match cmd_args.get_string("cmd") {
            Some(c) if !c.is_empty() => c.to_string(),
            _ => {
                eprintln!("usage: activate_fw [-y] [-c]");
                eprintln!("nvfwupd: error: the following arguments are required: -c/--cmd");
                std::process::exit(2);
            }
        };

        let list_of_target_ips = self.base.validate_target_json(&global_args, None).await;
        let mut err_code = 0;

        for target_args in &list_of_target_ips {
            let bmc_result = BmcAccess::get_bmc_access(target_args, self.base.trace, None).await;
            let (bmc_access, platform_type) = match bmc_result {
                Ok((access, pt)) => (access, pt),
                Err(_) => {
                    Util::bail_nvfwupd(
                        1,
                        "Unable to access target",
                        BailAction::PrintDivider,
                        None,
                    );
                    err_code = 1;
                    continue;
                }
            };

            // Check if this is an IPMI command
            if IPMI_CMD_DICT.get(command.as_str()).is_some() {
                let mut ipmi = IpmiToolActivation::new();
                ipmi.setup_ipmi_command(target_args);
                match ipmi.run_ipmi_command(&command).await {
                    Ok(_) => {}
                    Err(IpmiCommandError::IpmiNotAvailable(_)) => {
                        let rf_command = redfish_fallback_for_ipmi_command(&command);
                        let Some(mut rf_target) = self.base.init_platform(
                            bmc_access,
                            platform_type.as_deref(),
                            None,
                            false,
                        ) else {
                            err_code = 1;
                            continue;
                        };

                        let rf_cmd_args = RFCmdArgs {
                            cmd: rf_command.to_string(),
                            background: false,
                            details: false,
                            staged_update: false,
                            staged_activate_update: false,
                            quiet: false,
                            special: None,
                            oem_parameters: None,
                        };
                        if rf_target.run_oob_activation(&rf_cmd_args).await != 0 {
                            err_code = 1;
                        }
                    }
                    Err(_) => {
                        err_code = 1;
                    }
                }
            } else {
                let mut rf_target =
                    match self
                        .base
                        .init_platform(bmc_access, platform_type.as_deref(), None, false)
                    {
                        Some(t) => t,
                        None => {
                            err_code = 1;
                            continue;
                        }
                    };

                let rf_cmd_args = RFCmdArgs {
                    cmd: command.clone(),
                    background: false,
                    details: false,
                    staged_update: false,
                    staged_activate_update: false,
                    quiet: false,
                    special: None,
                    oem_parameters: None,
                };
                if rf_target.run_oob_activation(&rf_cmd_args).await != 0 {
                    err_code = 1;
                }
            }
            println!("{}", "-".repeat(120));
        }

        if err_code != 0 {
            Util::bail_nvfwupd(
                1,
                "Activation command completed with errors",
                BailAction::Exit,
                None,
            );
        }
    }
}

// ===========================================================================
// FwUpdCmdShowUpdateProgress
// ===========================================================================

pub struct FwUpdCmdShowUpdateProgress<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdShowUpdateProgress<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let (global_args, cmd_args) = self.base.validate_cmd(None);

        let json_mode = cmd_args.get_bool("json");
        let mut json_output: Option<Value> = if json_mode {
            Some(json!({"Error": [], "Error Code": 0, "Output": []}))
        } else {
            None
        };

        let task_ids = cmd_args.get_list("id").to_vec();

        // Parity with Python argparse: -i/--id is marked Required: true in
        // cli_schema.yaml. Python argparse enforces this before any other
        // processing (including -j/--json) — so even in JSON mode, Python
        // prints the plain-text usage/error pair rather than structured
        // JSON. Mirror that behavior here instead of silently succeeding.
        if task_ids.is_empty() {
            eprintln!("usage: show_update_progress [-j] -i/--id Task Ids [RECIPE ...]");
            eprintln!("nvfwupd: error: the following arguments are required: -i/--id");
            std::process::exit(2);
        }

        let list_of_target_ips = self
            .base
            .validate_target_json(&global_args, json_output.as_ref())
            .await;

        let mut error_status: i32 = 0;

        if list_of_target_ips.len() == 1 {
            let target_args = &list_of_target_ips[0];
            let bmc_ip =
                NvUtils::sanitize_log(target_args.first().map(|s| s.as_str()).unwrap_or("unknown"));

            let bmc_result = BmcAccess::get_bmc_access(target_args, self.base.trace, None).await;
            let (bmc_access, platform_type) = match bmc_result {
                Ok((access, pt)) => (access, pt),
                Err(_) => {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Unable to access system {}", bmc_ip),
                        BailAction::Exit,
                        json_output.as_ref(),
                    );
                    return;
                }
            };

            let rf_target = match self.base.init_platform(
                bmc_access,
                platform_type.as_deref(),
                json_output.as_ref(),
                false,
            ) {
                Some(t) => t,
                None => return,
            };

            for task_id in &task_ids {
                error_status |= rf_target
                    .process_job_status(task_id, json_output.as_mut())
                    .await;
                if !json_mode {
                    println!("{}", "-".repeat(120));
                }
            }
        } else {
            Util::bail_nvfwupd(
                1,
                "Multiple targets not supported with show_update_progress command.",
                BailAction::Exit,
                json_output.as_ref(),
            );
        }

        Util::bail_nvfwupd(error_status, "", BailAction::Exit, json_output.as_ref());
    }
}

// ===========================================================================
// FwUpdCmdPerformFactoryReset
// ===========================================================================

pub struct FwUpdCmdPerformFactoryReset<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdPerformFactoryReset<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let (global_args, _cmd_args) = self.base.validate_cmd(None);
        let list_of_target_ips = self.base.validate_target_json(&global_args, None).await;

        for target_args in &list_of_target_ips {
            let bmc_ip =
                NvUtils::sanitize_log(target_args.first().map(|s| s.as_str()).unwrap_or("unknown"));

            let bmc_result = BmcAccess::get_bmc_access(target_args, self.base.trace, None).await;
            let (bmc_access, platform_type) = match bmc_result {
                Ok((access, pt)) => (access, pt),
                Err(_) => {
                    println!("BMC Connection Status: Failed");
                    Util::bail_nvfwupd(
                        1,
                        &format!("Unable to access BMC {}", bmc_ip),
                        BailAction::PrintDivider,
                        None,
                    );
                    continue;
                }
            };

            println!("BMC Connection Status: Successful");

            let mut rf_target =
                match self
                    .base
                    .init_platform(bmc_access, platform_type.as_deref(), None, false)
                {
                    Some(t) => t,
                    None => continue,
                };

            let (status, response_dict) = rf_target.factory_reset(None).await;
            if !status {
                Util::bail_nvfwupd(
                    1,
                    "Factory Reset request failed!",
                    BailAction::PrintDivider,
                    None,
                );
                continue;
            }
            println!("Factory Reset request successful");
            println!("Task State:");
            println!("{}", NvUtils::redacted_json_pretty_4space(&response_dict));
            println!("{}", "-".repeat(120));
        }
    }
}

// ===========================================================================
// FwUpdCmdBackgroundCopy
// ===========================================================================

pub struct FwUpdCmdBackgroundCopy<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdBackgroundCopy<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let (global_args, cmd_args) = self.base.validate_cmd(None);

        // Validate special file (required)
        let special = cmd_args
            .get_list("special")
            .first()
            .cloned()
            .or_else(|| cmd_args.get_string("special").map(|s| s.to_string()));
        match &special {
            Some(sp) => {
                if !path_is_file(sp).await {
                    Util::bail_nvfwupd(
                        1,
                        &format!("Special command json file doesn't exist: {}", sp),
                        BailAction::Exit,
                        None,
                    );
                    return;
                }
            }
            None => {
                Util::bail_nvfwupd(
                    1,
                    "Special command json file is required",
                    BailAction::Exit,
                    None,
                );
                return;
            }
        }
        let special_file = special.unwrap();

        let interactive = cmd_args.get_bool("interactive");
        let timeout_val: u64 = cmd_args
            .get_string("timeout")
            .and_then(|s| s.parse().ok())
            .filter(|&t: &u64| t > 0)
            .unwrap_or(600);
        let poll_interval: u64 = cmd_args
            .get_string("poll_interval")
            .and_then(|s| s.parse().ok())
            .filter(|&t: &u64| t > 0)
            .unwrap_or(20);

        let list_of_target_ips = self.base.validate_target_json(&global_args, None).await;
        let mut err_code = 0;

        for target_args in &list_of_target_ips {
            let bmc_ip =
                NvUtils::sanitize_log(target_args.first().map(|s| s.as_str()).unwrap_or("unknown"));

            let bmc_result = BmcAccess::get_bmc_access(target_args, self.base.trace, None).await;
            let (bmc_access, platform_type) = match bmc_result {
                Ok((access, pt)) => (access, pt),
                Err(_) => {
                    println!("BMC Connection Status: Failed");
                    Util::bail_nvfwupd(
                        1,
                        &format!("Unable to access BMC {}", bmc_ip),
                        BailAction::PrintDivider,
                        None,
                    );
                    err_code = 1;
                    continue;
                }
            };

            println!("BMC Connection Status: Successful");

            // Validate interactive mode targets
            let mut interactive_targets: Vec<String> = Vec::new();
            if interactive {
                let json_params: Value = match tokio::fs::read_to_string(&special_file).await {
                    Ok(contents) => match serde_json::from_str(&contents) {
                        Ok(v) => v,
                        Err(e) => {
                            Util::bail_nvfwupd(
                                1,
                                &format!("Error reading targets from JSON file: {}", e),
                                BailAction::Exit,
                                None,
                            );
                            return;
                        }
                    },
                    Err(e) => {
                        Util::bail_nvfwupd(
                            1,
                            &format!("Error reading targets from JSON file: {}", e),
                            BailAction::Exit,
                            None,
                        );
                        return;
                    }
                };

                interactive_targets = json_params
                    .get("Targets")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();

                if interactive_targets.is_empty() {
                    println!("Empty Targets list detected - querying for all allowable targets");
                    interactive_targets = self.get_all_allowable_targets(&bmc_access).await;
                    if interactive_targets.is_empty() {
                        Util::bail_nvfwupd(
                            1,
                            "No allowable targets found for background copy",
                            BailAction::Exit,
                            None,
                        );
                        return;
                    }
                }

                // Validate targets support interactive background copy
                for target in &interactive_targets {
                    let mut status = false;
                    let mut response_dict = json!({});
                    for _attempt in 0..3 {
                        let (s, r) = bmc_access.dispatch_request("GET", target, None, None).await;
                        status = s;
                        response_dict = r;
                        if status {
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }

                    if !status {
                        Util::bail_nvfwupd(
                            1,
                            &format!("Failed to query URI {} after 3 attempts", target),
                            BailAction::Exit,
                            None,
                        );
                        return;
                    }

                    if response_dict
                        .pointer("/Oem/Nvidia/ActiveFirmwareSlot")
                        .is_none()
                        || response_dict
                            .pointer("/Oem/Nvidia/InactiveFirmwareSlot")
                            .is_none()
                    {
                        Util::bail_nvfwupd(
                            1,
                            &format!(
                                "Target {} does not support Interactive Background Copy",
                                target
                            ),
                            BailAction::Exit,
                            None,
                        );
                        return;
                    }
                }
            }

            let user_targets: Vec<String> = match tokio::fs::read_to_string(&special_file).await {
                Ok(contents) => serde_json::from_str::<Value>(&contents)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("Targets")
                            .and_then(|v| v.as_array())
                            .map(|targets| {
                                targets
                                    .iter()
                                    .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                                    .collect::<Vec<String>>()
                            })
                    })
                    .unwrap_or_default(),
                Err(_) => Vec::new(),
            };

            if !user_targets.is_empty() {
                let allowable_targets = self.get_all_allowable_targets(&bmc_access).await;
                if allowable_targets.is_empty() {
                    Util::bail_nvfwupd(
                        1,
                        "No allowable targets found for background copy on this system. Background copy may not be supported on this platform.",
                        BailAction::PrintDivider,
                        None,
                    );
                    err_code = 1;
                    continue;
                }

                let invalid_targets: Vec<String> = user_targets
                    .iter()
                    .filter(|target| !allowable_targets.contains(*target))
                    .cloned()
                    .collect();
                if !invalid_targets.is_empty() {
                    Util::bail_nvfwupd(
                        1,
                        &format!(
                            "The following targets are not valid for background copy: {:?}\nAllowable targets for this system: {:?}",
                            invalid_targets, allowable_targets
                        ),
                        BailAction::PrintDivider,
                        None,
                    );
                    err_code = 1;
                    continue;
                }
            }

            if interactive {
                let mut rf_target = match self.base.init_platform(
                    bmc_access.clone(),
                    platform_type.as_deref(),
                    None,
                    false,
                ) {
                    Some(t) => t,
                    None => {
                        err_code = 1;
                        continue;
                    }
                };

                let (status, response_dict) = rf_target.background_copy(&special_file).await;
                if !status {
                    Util::bail_nvfwupd(
                        1,
                        "Background copy request failed!",
                        BailAction::PrintDivider,
                        None,
                    );
                    err_code = 1;
                    continue;
                }
                println!("Background copy request successful");
                println!("Task State:");
                println!("{}", NvUtils::redacted_json_pretty_4space(&response_dict));
                println!("{}", "-".repeat(120));

                // Poll firmware versions
                if !self
                    .poll_firmware_versions(
                        &bmc_access,
                        &interactive_targets,
                        poll_interval,
                        timeout_val,
                    )
                    .await
                {
                    err_code = 1;
                }
            } else {
                // Non-interactive background copy
                let mut rf_target =
                    match self
                        .base
                        .init_platform(bmc_access, platform_type.as_deref(), None, false)
                    {
                        Some(t) => t,
                        None => {
                            err_code = 1;
                            continue;
                        }
                    };

                let (status, response_dict) = rf_target.background_copy(&special_file).await;
                if !status {
                    Util::bail_nvfwupd(
                        1,
                        "Background copy request failed!",
                        BailAction::PrintDivider,
                        None,
                    );
                    err_code = 1;
                    continue;
                }
                println!("Background copy request successful");
                println!("Task State:");
                println!("{}", NvUtils::redacted_json_pretty_4space(&response_dict));
                println!("{}", "-".repeat(120));
            }
        }

        if err_code != 0 {
            Util::bail_nvfwupd(
                1,
                "Background copy completed with errors",
                BailAction::Exit,
                None,
            );
        }
    }

    async fn get_all_allowable_targets(&self, bmc_access: &BmcAccess) -> Vec<String> {
        println!("\nQuerying UpdateService for all allowable targets...");

        let (status, update_service_dict) = bmc_access
            .dispatch_request("GET", "/redfish/v1/UpdateService", None, None)
            .await;
        if !status {
            println!("Error: Failed to query /redfish/v1/UpdateService");
            return Vec::new();
        }

        let action_info_uri = match update_service_dict
            .pointer("/Actions/Oem/#NvidiaUpdateService.CommitImage/@Redfish.ActionInfo")
            .and_then(|v| v.as_str())
        {
            Some(uri) => {
                println!("Found ActionInfo URI: {}", uri);
                uri.to_string()
            }
            None => {
                println!(
                    "Error: Could not find CommitImage ActionInfo URI in UpdateService response"
                );
                return Vec::new();
            }
        };

        let (status, action_info_dict) = bmc_access
            .dispatch_request("GET", &action_info_uri, None, None)
            .await;
        if !status {
            println!("Error: Failed to query {}", action_info_uri);
            return Vec::new();
        }

        if let Some(parameters) = action_info_dict
            .get("Parameters")
            .and_then(|v| v.as_array())
        {
            for param in parameters {
                if param.get("Name").and_then(|v| v.as_str()) == Some("Targets") {
                    let allowable: Vec<String> = param
                        .get("AllowableValues")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();

                    println!("Found {} allowable target(s):", allowable.len());
                    for t in &allowable {
                        println!("  - {}", t);
                    }
                    return allowable;
                }
            }
        }

        println!("Error: Could not find 'Targets' parameter in ActionInfo");
        Vec::new()
    }

    async fn poll_firmware_versions(
        &self,
        bmc_access: &BmcAccess,
        targets: &[String],
        poll_interval: u64,
        max_wait_time: u64,
    ) -> bool {
        println!("\nInteractive mode: Polling firmware slots until versions match...");
        println!("Poll interval: {} seconds", poll_interval);
        println!("Maximum wait time: {} seconds", max_wait_time);
        println!("{}", "-".repeat(120));

        let mut target_status: HashMap<String, (bool, String, String)> = HashMap::new();
        for target in targets {
            target_status.insert(target.clone(), (false, String::new(), String::new()));
        }

        let start = Instant::now();
        let mut all_matched = false;

        while !all_matched && start.elapsed().as_secs() < max_wait_time {
            println!("\nPolling firmware slot versions...");

            all_matched = true;
            for (target, (matched, active_ver, inactive_ver)) in target_status.iter_mut() {
                if *matched {
                    continue;
                }

                let (status, response_dict) =
                    bmc_access.dispatch_request("GET", target, None, None).await;
                if !status {
                    println!("  {}: Failed to query URI", target);
                    all_matched = false;
                    continue;
                }

                let oem_nvidia = response_dict
                    .pointer("/Oem/Nvidia")
                    .unwrap_or(&json!({}))
                    .clone();

                let active_version = oem_nvidia
                    .pointer("/ActiveFirmwareSlot/Version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown")
                    .to_string();
                let inactive_version = oem_nvidia
                    .pointer("/InactiveFirmwareSlot/Version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown")
                    .to_string();

                *active_ver = active_version.clone();
                *inactive_ver = inactive_version.clone();

                if active_version == inactive_version && active_version != "Unknown" {
                    *matched = true;
                    println!("  {}: MATCHED - Version: {}", target, active_version);
                } else {
                    all_matched = false;
                    println!(
                        "  {}: Active: {}, Inactive: {} (waiting...)",
                        target, active_version, inactive_version
                    );
                }
            }

            if !all_matched {
                println!("\nWaiting {} seconds before next poll...", poll_interval);
                tokio::time::sleep(Duration::from_secs(poll_interval)).await;
            }
        }

        println!("\n{}", "=".repeat(120));
        if all_matched {
            println!("SUCCESS: All firmware slots have matching versions!");
            println!("{}", "=".repeat(120));
            println!("\nFinal Status:");
            for (target, (_, active_ver, _)) in &target_status {
                println!("  {}", target);
                println!("    Version: {}", active_ver);
            }
        } else {
            let elapsed = start.elapsed().as_secs_f64();
            println!(
                "TIMEOUT: Maximum wait time of {} seconds exceeded (elapsed: {:.1}s)",
                max_wait_time, elapsed
            );
            println!("{}", "=".repeat(120));
            println!("\nFinal Status:");
            for (target, (matched, active_ver, inactive_ver)) in &target_status {
                let status_str = if *matched { "MATCHED" } else { "NOT MATCHED" };
                println!("  {} - {}", target, status_str);
                println!("    Active Version: {}", active_ver);
                println!("    Inactive Version: {}", inactive_ver);
            }
        }
        println!("{}", "-".repeat(120));
        all_matched
    }
}

// ===========================================================================
// FwUpdCmdFlintUpdate
// ===========================================================================

pub struct FwUpdCmdFlintUpdate<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdFlintUpdate<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let (global_args, cmd_args) = self.base.validate_cmd(None);

        let json_mode = cmd_args.get_bool("json");
        let version_query = cmd_args.get_bool("version");
        let mut json_output: Option<Value> = if json_mode {
            Some(json!({"Error": [], "Error Code": 0, "Output": []}))
        } else {
            None
        };

        // Validate that either image or version query is provided
        let image_args = cmd_args.get_list("image").to_vec();
        if image_args.is_empty() && !version_query && self.base.config_parser.is_none() {
            Util::bail_nvfwupd(
                1,
                "Missing command option -i/--image or -v/--version",
                BailAction::DoNothing,
                json_output.as_ref(),
            );
        }

        // Timeout handling
        let query_timeout: u64 = cmd_args
            .get_string("timeout")
            .and_then(|s| s.parse().ok())
            .filter(|&t: &u64| t > 0)
            .unwrap_or(60);
        let flash_timeout: u64 = query_timeout * 20;

        // Validate OS target
        let os_target_args = self
            .base
            .validate_os_target_json(&global_args, json_output.as_ref());
        if os_target_args.is_empty() {
            return;
        }

        // Create OS access
        let os_access_result = os_access::get_os_access(&os_target_args, json_output.as_ref());
        let (os_access, _arg_dict) = match os_access_result {
            Ok(v) => v,
            Err(msg) => {
                Util::bail_nvfwupd(
                    1,
                    &format!("Failed to establish OS connection: {}", msg),
                    BailAction::Exit,
                    json_output.as_ref(),
                );
                return;
            }
        };

        // Python parity: emit the "Starting MST service..." banner BEFORE
        // attempting any OS command, regardless of reachability state.
        // Python doesn't do a pre-reachability check — it just runs
        // `mst start` and lets the SSH failure surface as the mst error.
        // Previously Rust ran `is_reachable()` first which produced a
        // different one-line error ("OS is not reachable: TCP connect
        // failed...") and skipped the "Starting MST service..." banner,
        // also silently exiting 0 due to BailAction::DoNothing.
        if !json_mode {
            println!("Starting MST service...");
        }
        let (success, _output, error) = os_access
            .execute_command("mst start", query_timeout, true)
            .await;
        if !success {
            Util::bail_nvfwupd(
                1,
                &format!(
                    "Failed to start MST service. MST services may not be installed, \
                     or the OS may be unreachable. {}",
                    error
                ),
                BailAction::Exit,
                json_output.as_ref(),
            );
            return;
        }

        let device_type = cmd_args.get_string("device_type").map(|s| s.to_string());

        // Version query workflow
        if version_query {
            if !json_mode {
                println!("Querying firmware version...");
            }

            if !json_mode {
                if let Some(ref dt) = device_type {
                    println!("Querying firmware version for {} devices...", dt);
                } else {
                    println!("Querying firmware version for all MST devices...");
                }
            }

            // Get device info
            let (success, output, error) = os_access
                .execute_command("mst status -v", query_timeout, true)
                .await;
            if !success {
                Util::bail_nvfwupd(
                    1,
                    &format!("Failed to get MST status: {}", error),
                    BailAction::DoNothing,
                    json_output.as_ref(),
                );
                return;
            }

            // Parse MST status output. Python's query path (`get_device_info`
            // at updcommand.py:3324) does NOT filter SR-IOV VFs — it returns
            // all devices including VFs (`.1`, `.2`, `.3`). Only the flash
            // path (`get_device_paths` at 3247, used by `flint_flash_devices`)
            // applies the VF filter. Match that split: leave query output
            // unfiltered here, filter in the flash path only.
            let device_info = self.parse_mst_status(&output, device_type.as_deref());

            if device_info.is_empty() {
                let msg = if let Some(ref dt) = device_type {
                    format!("No MST devices found for {}", dt)
                } else {
                    "No MST devices found".to_string()
                };
                Util::bail_nvfwupd(1, &msg, BailAction::Exit, json_output.as_ref());
                return;
            }

            // Query image versions if provided
            let mut image_files = image_args.clone();
            if image_files.is_empty() {
                if let Some(ref cp) = self.base.config_parser {
                    if let Some(ref config) = cp.config_dict {
                        if let Some(imgs) = config.get("FlintImageFilePath") {
                            if let Some(arr) = imgs.as_array() {
                                image_files = arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect();
                            } else if let Some(s) = imgs.as_str() {
                                image_files = vec![s.to_string()];
                            }
                        }
                    }
                }
            }

            let mut image_versions: Vec<Value> = Vec::new();
            for image_file in &image_files {
                if tokio::fs::metadata(image_file).await.is_err() {
                    image_versions
                        .push(json!({"image": image_file, "error": "Image file not found"}));
                    continue;
                }

                let (success, _msg, error) = os_access.transfer_file(image_file, None, 300).await;
                if !success {
                    image_versions.push(json!({"image": image_file, "error": format!("Failed to transfer image file: {}", error)}));
                    continue;
                }

                let remote_name = Path::new(image_file)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();

                let flint_cmd = format!("flint -i {} query", shell_quote(&remote_name));
                let (success, output, _error) = os_access
                    .execute_command(&flint_cmd, query_timeout, true)
                    .await;

                if success {
                    let (fw_ver, psid) = parse_flint_query(&output);
                    if let Some(v) = fw_ver {
                        let mut entry = json!({"image": image_file, "fw_version": v});
                        if let Some(p) = psid {
                            entry["psid"] = json!(p);
                        }
                        image_versions.push(entry);
                    } else {
                        image_versions.push(json!({"image": image_file, "error": "FW Version not found in image output"}));
                    }
                } else {
                    image_versions.push(json!({"image": image_file, "error": format!("Image query failed: {}", _error)}));
                }
            }

            // Query each device
            let mut version_results: Vec<Value> = Vec::new();
            for device in &device_info {
                let device_path = device
                    .get("device_path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let dev_type = device
                    .get("device_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown");

                if !device_path.starts_with("/dev/mst/") {
                    version_results.push(json!({
                        "device": device_path,
                        "device_type": dev_type,
                        "fw_version": "Invalid device path"
                    }));
                    continue;
                }

                let flint_cmd = format!("flint -d {} --yes query", shell_quote(device_path));
                let (success, output, error) = os_access
                    .execute_command(&flint_cmd, query_timeout, true)
                    .await;

                if success {
                    let (fw_ver, psid) = parse_flint_query(&output);
                    let mut entry = json!({
                        "device": device_path,
                        "device_type": dev_type,
                        "fw_version": fw_ver.unwrap_or_else(|| "FW Version not found in output".to_string()),
                    });
                    if let Some(p) = psid {
                        entry["psid"] = json!(p);
                    }
                    version_results.push(entry);
                } else {
                    let error_msg = if error.trim().is_empty() {
                        "Query failed".to_string()
                    } else {
                        format!("Query failed: {}", error.trim())
                    };
                    version_results.push(json!({
                        "device": device_path,
                        "device_type": dev_type,
                        "fw_version": error_msg
                    }));
                }
            }

            // Output
            if let Some(ref mut jo) = json_output {
                jo["Output"] = json!({ "devices": version_results, "images": image_versions });
                println!("{}", json_pretty_4space(jo));
            } else {
                // Image firmware versions
                if !image_versions.is_empty() {
                    println!("\nImage Firmware Versions:");
                    println!("{:<40} {:<20} {}", "Image", "FW Version", "PSID");
                    println!("{}", "-".repeat(80));
                    for iv in &image_versions {
                        let image = iv.get("image").and_then(|v| v.as_str()).unwrap_or("");
                        let fw_ver = iv
                            .get("fw_version")
                            .and_then(|v| v.as_str())
                            .or_else(|| iv.get("error").and_then(|v| v.as_str()))
                            .unwrap_or("N/A");
                        let psid = iv.get("psid").and_then(|v| v.as_str()).unwrap_or("");
                        println!("{:<40} {:<20} {}", image, fw_ver, psid);
                    }
                    println!();
                }

                // Device firmware versions
                println!("Device Firmware Versions:");
                println!(
                    "{:<20} {:<30} {:<20} {}",
                    "Type", "Device", "FW Version", "PSID"
                );
                println!("{}", "-".repeat(80));
                for result in &version_results {
                    let dev_type = result
                        .get("device_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let device = result.get("device").and_then(|v| v.as_str()).unwrap_or("");
                    let fw_ver = result
                        .get("fw_version")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let psid = result.get("psid").and_then(|v| v.as_str()).unwrap_or("");
                    println!("{:<20} {:<30} {:<20} {}", dev_type, device, fw_ver, psid);
                }
                println!();
            }
            return;
        }

        // Firmware update workflow
        let mut image_files = image_args;
        if image_files.is_empty() {
            if let Some(ref cp) = self.base.config_parser {
                if let Some(ref config) = cp.config_dict {
                    if let Some(imgs) = config.get("FlintImageFilePath") {
                        if let Some(arr) = imgs.as_array() {
                            image_files = arr
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                        } else if let Some(s) = imgs.as_str() {
                            image_files = vec![s.to_string()];
                        }
                    }
                }
            }
        }

        if image_files.is_empty() {
            Util::bail_nvfwupd(
                1,
                "Error: No image file specified for flint update",
                BailAction::DoNothing,
                json_output.as_ref(),
            );
            return;
        }

        let mut results: Vec<Value> = Vec::new();
        for image_file in &image_files {
            if tokio::fs::metadata(image_file).await.is_err() {
                if let Some(ref mut jo) = json_output {
                    if let Some(arr) = jo.get_mut("Error").and_then(|e| e.as_array_mut()) {
                        arr.push(json!(format!("Image file not found: {}", image_file)));
                    }
                } else {
                    println!("Error: Image file not found: {}", image_file);
                }
                continue;
            }

            if !json_mode {
                println!("Processing firmware image: {}", image_file);
            }

            let dt = device_type.clone().or_else(|| {
                self.base
                    .config_parser
                    .as_ref()
                    .and_then(|cp| cp.config_dict.as_ref())
                    .and_then(|d| d.get("FlintDeviceType"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });

            if dt.is_none() {
                Util::bail_nvfwupd(
                    1,
                    "Device type is not specified",
                    BailAction::DoNothing,
                    json_output.as_ref(),
                );
                continue;
            }
            let device_name = dt.unwrap();

            let (flash_success, flash_error) = self
                .flint_flash(
                    &os_access,
                    &device_name,
                    image_file,
                    true,
                    query_timeout,
                    flash_timeout,
                )
                .await;

            let result = json!({
                "command": format!("flint_flash {} {}", device_name, image_file),
                "success": flash_success,
                "output": if flash_success { "Firmware flashing completed successfully" } else { "Firmware flashing failed" }
            });

            if !flash_success {
                if let Some(ref mut jo) = json_output {
                    if let Some(arr) = jo.get_mut("Error").and_then(|e| e.as_array_mut()) {
                        arr.push(json!(format!("Firmware flashing failed: {}", flash_error)));
                    }
                    jo["Error Code"] = json!(1);
                }
            }
            results.push(result);
        }

        // Output results
        if let Some(ref mut jo) = json_output {
            if let Some(arr) = jo.get_mut("Output").and_then(|o| o.as_array_mut()) {
                arr.extend(results.clone());
            }
            let has_errors = results
                .iter()
                .any(|r| r.get("success") == Some(&json!(false)));
            if has_errors {
                jo["Error Code"] = json!(1);
            }
            println!("{}", json_pretty_4space(jo));
            if has_errors {
                std::process::exit(1);
            }
        } else {
            println!("Command execution completed:");
            for result in &results {
                println!(
                    "Command: {}",
                    result.get("command").and_then(|v| v.as_str()).unwrap_or("")
                );
                println!(
                    "Status: {}",
                    if result.get("success") == Some(&json!(true)) {
                        "Success"
                    } else {
                        "Failed"
                    }
                );
                if let Some(output) = result.get("output").and_then(|v| v.as_str()) {
                    println!("Output: {}", output);
                }
                println!("{}", "-".repeat(50));
            }
            let has_errors = results
                .iter()
                .any(|r| r.get("success") == Some(&json!(false)));
            if has_errors {
                Util::bail_nvfwupd(
                    1,
                    "Some commands failed during execution",
                    BailAction::Exit,
                    None,
                );
            }
        }
    }

    /// Parse MST status output into device info.
    fn parse_mst_status(&self, output: &str, device_type: Option<&str>) -> Vec<Value> {
        let mut device_info = Vec::new();
        let lines: Vec<&str> = output.lines().collect();

        let mut device_type_pos: Option<usize> = None;
        let mut mst_pos: Option<usize> = None;
        let mut pci_pos: Option<usize> = None;

        for line in &lines {
            if line.contains("DEVICE_TYPE") && line.contains("MST") {
                device_type_pos = Some(line.find("DEVICE_TYPE").unwrap_or(0));
                mst_pos = Some(line.find("MST").unwrap_or(0));
                pci_pos = line.find("PCI");
                break;
            }
        }

        let (dt_pos, m_pos) = match (device_type_pos, mst_pos) {
            (Some(d), Some(m)) => (d, m),
            _ => return device_info,
        };
        let m_end = pci_pos.unwrap_or(usize::MAX);

        let mut past_inband = false;
        for line in &lines {
            if line.contains("Inband devices:") {
                past_inband = true;
            }
            if past_inband {
                break;
            }

            if line.contains("/dev/mst/") {
                let dev_type = if dt_pos < line.len() {
                    let end = std::cmp::min(m_pos, line.len());
                    line[dt_pos..end].trim().to_string()
                } else {
                    String::new()
                };

                let device_path = if m_pos < line.len() {
                    let end = std::cmp::min(m_end, line.len());
                    line[m_pos..end].trim().to_string()
                } else {
                    String::new()
                };

                if !dev_type.is_empty() && !device_path.is_empty() {
                    if let Some(dt_filter) = device_type {
                        if !dev_type.to_lowercase().contains(&dt_filter.to_lowercase()) {
                            continue;
                        }
                    }
                    device_info.push(json!({
                        "device_path": device_path,
                        "device_type": dev_type
                    }));
                }
            }
        }

        device_info
    }

    async fn flint_flash(
        &self,
        os_access: &OsAccess,
        named_device: &str,
        file_name: &str,
        use_sudo: bool,
        query_timeout: u64,
        flash_timeout: u64,
    ) -> (bool, String) {
        // Transfer firmware file
        tracing::info!(
            indent = 0,
            json_mode = self.base.trace.json_mode,
            "Transferring firmware file {} to remote system...",
            file_name
        );
        let (success, _output, error) = os_access.transfer_file(file_name, None, 300).await;
        if !success {
            tracing::info!(
                indent = 0,
                json_mode = self.base.trace.json_mode,
                "Failed to transfer firmware file: {}",
                error
            );
            return (false, format!("File transfer failed: {}", error));
        }

        let remote_name = Path::new(file_name)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        // Validate filename
        let safe_re = Regex::new(r"^[a-zA-Z0-9._-]+$").unwrap();
        if !safe_re.is_match(&remote_name) {
            return (
                false,
                "Invalid filename: contains unsafe characters".to_string(),
            );
        }

        // Get device paths
        let (success, output, error) = os_access
            .execute_command("mst status -v", query_timeout, use_sudo)
            .await;
        if !success {
            return (false, format!("Device discovery failed: {}", error));
        }

        let device_info = self.parse_mst_status(&output, Some(named_device));
        // Python parity: filter out SR-IOV virtual functions. `mst status -v`
        // lists both Physical Functions (e.g. `/dev/mst/mt4133_pciconf9`) and
        // Virtual Functions (e.g. `/dev/mst/mt4133_pciconf9.1`, `.2`, `.3`).
        // Flashing a VF fails because flint only supports PFs, and attempting
        // it produces misleading "Flash failed" noise. Python's
        // `get_device_paths` (updcommand.py:3298-3301) strips any path ending
        // with `.0`-`.9`; mirror that here.
        let device_paths: Vec<String> = device_info
            .iter()
            .filter_map(|d| {
                d.get("device_path")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .filter(|p| {
                // Keep only if the trailing token after the last '.' is NOT
                // a single digit (0-9). Physical functions have no suffix
                // (e.g. `mt4133_pciconf9`), so rsplit('.').next() returns
                // the whole name — which is not a digit — and is kept.
                match p.rsplit('.').next() {
                    Some(tail) => {
                        !(tail.len() == 1
                            && tail
                                .chars()
                                .next()
                                .map(|c| c.is_ascii_digit())
                                .unwrap_or(false))
                    }
                    None => true,
                }
            })
            .collect();

        if device_paths.is_empty() {
            return (false, format!("No MST devices found for {}", named_device));
        }

        // Query image PSID
        let escaped_name = shell_quote(&remote_name);
        let image_query_cmd = format!("flint -i {} query", escaped_name);
        let (success, output, _error) = os_access
            .execute_command(&image_query_cmd, query_timeout, use_sudo)
            .await;
        let image_psid = if success {
            parse_flint_query_psid(&output)
        } else {
            None
        };

        // Flash each device
        let mut all_verified = true;
        let mut flash_attempted_count = 0;
        let mut error_details = Vec::new();

        for device_path in &device_paths {
            if !device_path.starts_with("/dev/mst/") {
                error_details.push(format!("Invalid device path: {}", device_path));
                all_verified = false;
                continue;
            }

            let escaped_device = shell_quote(device_path);

            // Query device PSID
            let device_query_cmd = format!("flint -d {} --yes query", escaped_device);
            let (success, output, _error) = os_access
                .execute_command(&device_query_cmd, query_timeout, use_sudo)
                .await;
            let device_psid = if success {
                parse_flint_query_psid(&output)
            } else {
                None
            };

            if device_psid.is_none() {
                tracing::info!(
                    indent = 0,
                    json_mode = self.base.trace.json_mode,
                    "Skipping device {}: device PSID could not be determined",
                    device_path
                );
                continue;
            }

            if let Some(ref ip) = image_psid {
                if Some(ip.as_str()) != device_psid.as_deref() {
                    tracing::info!(
                        indent = 0,
                        json_mode = self.base.trace.json_mode,
                        "Skipping device {}: PSID mismatch (image: {}, device: {})",
                        device_path,
                        ip,
                        device_psid.as_deref().unwrap_or("N/A")
                    );
                    continue;
                }
            }

            flash_attempted_count += 1;
            let flash_cmd = format!("flint -d {} --yes -i {} b", escaped_device, escaped_name);
            tracing::info!(
                indent = 0,
                json_mode = self.base.trace.json_mode,
                "Executing flash command: {}",
                flash_cmd
            );

            let (success, output, error) = os_access
                .execute_command(&flash_cmd, flash_timeout, use_sudo)
                .await;
            if !success {
                tracing::info!(
                    indent = 0,
                    json_mode = self.base.trace.json_mode,
                    "Flash failed for device {}: {}",
                    device_path,
                    error
                );
                error_details.push(format!("Device {}: {}", device_path, error));
                all_verified = false;
            } else {
                tracing::info!(
                    indent = 0,
                    json_mode = self.base.trace.json_mode,
                    "Flash successful for device {}: {}",
                    device_path,
                    output
                );
            }
        }

        if !device_paths.is_empty() && flash_attempted_count == 0 {
            all_verified = false;
            error_details.push(
                "No devices flashed: all devices were skipped (PSID mismatch) \
                 or had invalid paths."
                    .to_string(),
            );
        }

        if all_verified {
            (true, String::new())
        } else {
            (false, error_details.join("; "))
        }
    }
}

// ===========================================================================
// FwUpdCmdCreateUpdateTargets
// ===========================================================================

pub struct FwUpdCmdCreateUpdateTargets<'a> {
    base: FwUpdCmdBase<'a>,
}

impl<'a> FwUpdCmdCreateUpdateTargets<'a> {
    pub fn new(base: FwUpdCmdBase<'a>) -> Self {
        Self { base }
    }

    pub async fn run_command(&mut self) {
        let (global_args, cmd_args) = self.base.validate_cmd(None);
        let list_of_target_ips = self.base.validate_target_json(&global_args, None).await;

        if list_of_target_ips.len() == 1 {
            let target_args = &list_of_target_ips[0];
            let bmc_result = BmcAccess::get_bmc_access(target_args, self.base.trace, None).await;
            let (bmc_access, platform_type) = match bmc_result {
                Ok((access, pt)) => (access, pt),
                Err(_) => {
                    let bmc_ip = NvUtils::sanitize_log(
                        target_args.first().map(|s| s.as_str()).unwrap_or("unknown"),
                    );
                    Util::bail_nvfwupd(
                        1,
                        &format!("Unable to access system {}", bmc_ip),
                        BailAction::Exit,
                        None,
                    );
                    return;
                }
            };

            let rf_target =
                match self
                    .base
                    .init_platform(bmc_access, platform_type.as_deref(), None, false)
                {
                    Some(t) => t,
                    None => return,
                };

            let dir_path = cmd_args.get_string("outdir").unwrap_or(".");

            rf_target.make_update_target_json(dir_path).await;
        } else {
            Util::bail_nvfwupd(
                1,
                "Multiple targets not supported with make_upd_targets command.",
                BailAction::Exit,
                None,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Simple shell quoting (wraps in single quotes, escaping existing single quotes).
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Parse flint query output for FW Version and PSID.
fn parse_flint_query(output: &str) -> (Option<String>, Option<String>) {
    let mut fw_version = None;
    let mut psid = None;
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("FW Version:") {
            fw_version = Some(line.split(':').nth(1).unwrap_or("").trim().to_string());
        } else if trimmed.starts_with("PSID:") {
            psid = Some(line.split(':').nth(1).unwrap_or("").trim().to_string());
        }
    }
    (fw_version, psid)
}

/// Parse flint query output for just PSID.
fn parse_flint_query_psid(output: &str) -> Option<String> {
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("PSID:") {
            return Some(line.split(':').nth(1).unwrap_or("").trim().to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    struct MockFirmwarePkg {
        apname_version_dict: HashMap<String, IndexMap<String, Vec<String>>>,
    }

    impl MockFirmwarePkg {
        fn with_packages(package_names: &[&str]) -> Self {
            let apname_version_dict = package_names
                .iter()
                .map(|name| ((*name).to_string(), IndexMap::new()))
                .collect();
            Self {
                apname_version_dict,
            }
        }
    }

    #[async_trait::async_trait]
    impl FirmwarePkg for MockFirmwarePkg {
        async fn parse_pkg(
            &mut self,
            _package_name: &str,
            _json_dict: Option<&mut Value>,
        ) -> (bool, String) {
            (true, String::new())
        }

        async fn remove_files(&mut self) {}

        fn apname_version_dict(&self) -> &HashMap<String, IndexMap<String, Vec<String>>> {
            &self.apname_version_dict
        }

        fn print_package_content(&self, _package_name: &str) {}
    }

    #[test]
    fn test_match_platform_known() {
        let result = FwUpdCmdBase::match_platform("dgx");
        assert!(result.is_some());
    }

    #[test]
    fn test_match_platform_liteon_powershelf_model() {
        let result = FwUpdCmdBase::match_platform("pf-1333-7r");
        assert_eq!(result, Some("PowerShelfRFTarget"));
    }

    #[test]
    fn test_match_platform_flex_powershelf_model() {
        let result = FwUpdCmdBase::match_platform("nvd-p-0000adt00");
        assert_eq!(result, Some("PowerShelfRFTarget"));
    }

    #[test]
    fn test_match_platform_dgx_rubin() {
        assert_eq!(
            FwUpdCmdBase::match_platform("dgxrubin"),
            Some("DGX_RFTarget")
        );
        assert_eq!(
            FwUpdCmdBase::match_platform("dgx rubin"),
            Some("DGX_RFTarget")
        );
    }

    #[test]
    fn test_match_platform_n5400_switch_model() {
        let result = FwUpdCmdBase::match_platform("n5400_ld");
        assert_eq!(result, Some("GB200SwitchRFTarget"));
    }

    #[test]
    fn show_version_hgx_inventory_skip_only_applies_to_dgx_only_packages() {
        let target = DGXRFTarget::new(BmcAccess::default_stub(), None);
        let dgx_only_pkg = MockFirmwarePkg::with_packages(&["DGX-Rubin-NVL8_0006"]);
        let mixed_pkg =
            MockFirmwarePkg::with_packages(&["DGX-Rubin-NVL8_0006", "HGX-Rubin-NVL8_0001"]);

        assert!(FwUpdCmdShowVersion::skip_hgx_matching_for_dgx_only_package(
            &target,
            "hgx_fw_gpu_0",
            &dgx_only_pkg
        ));
        assert!(
            !FwUpdCmdShowVersion::skip_hgx_matching_for_dgx_only_package(
                &target,
                "fw_bmc_0",
                &dgx_only_pkg
            )
        );
        assert!(
            !FwUpdCmdShowVersion::skip_hgx_matching_for_dgx_only_package(
                &target,
                "hgx_fw_gpu_0",
                &mixed_pkg
            )
        );

        let non_dgx_target = HGXB100RFTarget::new(BmcAccess::default_stub(), None);
        assert!(
            !FwUpdCmdShowVersion::skip_hgx_matching_for_dgx_only_package(
                &non_dgx_target,
                "hgx_fw_gpu_0",
                &dgx_only_pkg
            )
        );
    }

    #[test]
    fn test_match_platform_unknown() {
        let result = FwUpdCmdBase::match_platform("totally_unknown_platform_xyz");
        assert!(result.is_none());
    }

    #[test]
    fn test_match_platform_empty() {
        let result = FwUpdCmdBase::match_platform("");
        assert!(result.is_none());
    }

    #[test]
    fn test_parsed_args_debug_redacts_passwords() {
        let mut parsed = ParsedArgs::new();
        parsed.lists.insert(
            "target".to_string(),
            vec!["password=plain secret suffix".to_string()],
        );
        parsed
            .strings
            .insert("RF_PASSWORD".to_string(), "other_secret".to_string());
        parsed
            .positionals
            .push("BMC_PASSWORD: positional_secret".to_string());

        let debug = format!("{parsed:?}");
        assert!(!debug.contains("plain"));
        assert!(!debug.contains("suffix"));
        assert!(!debug.contains("other_secret"));
        assert!(!debug.contains("positional_secret"));
        assert!(debug.contains("XXXX"));
    }

    #[test]
    fn test_parse_flint_query() {
        let output = "FW Version:   20.43.1000\nPSID:         MT_0000000123\n";
        let (fw, psid) = parse_flint_query(output);
        assert_eq!(fw, Some("20.43.1000".to_string()));
        assert_eq!(psid, Some("MT_0000000123".to_string()));
    }

    #[test]
    fn test_parse_flint_query_empty() {
        let (fw, psid) = parse_flint_query("");
        assert!(fw.is_none());
        assert!(psid.is_none());
    }

    #[test]
    fn test_shell_quote() {
        assert_eq!(shell_quote("hello"), "'hello'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn test_redfish_fallback_for_ipmi_command() {
        assert_eq!(
            redfish_fallback_for_ipmi_command("PWR_STATUS"),
            "RF_PWR_STATUS"
        );
        assert_eq!(redfish_fallback_for_ipmi_command("PWR_OFF"), "RF_PWR_OFF");
        assert_eq!(redfish_fallback_for_ipmi_command("PWR_ON"), "RF_PWR_ON");
        assert_eq!(
            redfish_fallback_for_ipmi_command("PWR_CYCLE"),
            "RF_PWR_CYCLE"
        );
        assert_eq!(
            redfish_fallback_for_ipmi_command("RESET_COLD"),
            "RESET_COLD"
        );
    }

    #[test]
    fn target_input_option_support_includes_tls_and_ssh_options() {
        for key in [
            "ip",
            "port",
            "user",
            "password",
            "servertype",
            "verify_tls",
            "bmc_ca_cert",
            "ssh_known_hosts",
            "ssh_host_key_mode",
        ] {
            assert!(
                target_input_option_supported(key),
                "{key} should be accepted"
            );
        }

        for key in ["ca_cert", "tls_ca", "verify_ssl", "known_hosts", "unknown"] {
            assert!(
                !target_input_option_supported(key),
                "{key} should not be accepted"
            );
        }
    }

    #[test]
    fn verify_tls_string_value_support_matches_bmc_access_boolean_parser() {
        for value in ["1", "true", "yes", "on", "0", "false", "no", "off"] {
            assert!(
                verify_tls_string_value_supported(value),
                "{value} should be accepted"
            );
        }

        for value in ["", " ", "maybe", "2", "enabled"] {
            assert!(
                !verify_tls_string_value_supported(value),
                "{value:?} should be rejected"
            );
        }
    }

    #[test]
    fn ssh_host_key_mode_values_match_bmc_access_parser() {
        for value in [
            SSH_HOST_KEY_MODE_DISABLED,
            SSH_HOST_KEY_MODE_TOFU,
            SSH_HOST_KEY_MODE_STRICT,
        ] {
            assert!(
                ssh_host_key_mode_string_value_supported(value),
                "{value} should be accepted"
            );
        }

        for value in ["", " ", "maybe", "verify", "off"] {
            assert!(
                !ssh_host_key_mode_string_value_supported(value),
                "{value:?} should be rejected"
            );
        }
    }

    #[tokio::test]
    async fn force_update_argv_actions_parse_as_command_args() {
        struct TestCase {
            name: &'static str,
            action: &'static str,
        }

        let cases = [
            TestCase {
                name: "enable",
                action: "enable",
            },
            TestCase {
                name: "disable",
                action: "disable",
            },
            TestCase {
                name: "status",
                action: "status",
            },
        ];

        let mut schema = CLISchema::new();
        schema
            .load_embedded_schema()
            .expect("embedded CLI schema should parse");
        let command = schema
            .get_command_schema("force_update")
            .expect("force_update schema should exist")
            .clone();

        for case in cases {
            let base = FwUpdCmdBase::new(
                &schema,
                "nvfwupd".to_string(),
                "force_update".to_string(),
                command.clone(),
                Vec::new(),
                vec![case.action.to_string()],
            )
            .await;

            let parsed = base.parse_cmd_args();

            assert_eq!(
                parsed.get_string("force_upd_action"),
                Some(case.action),
                "{} should parse as the named force update action",
                case.name
            );
            assert_eq!(
                parsed.get_positional(0),
                Some(case.action),
                "{} should remain available as the first positional argv value",
                case.name
            );
            assert_eq!(
                parsed.get_positional(1),
                None,
                "{} should consume exactly one positional argv value",
                case.name
            );
            assert_eq!(parsed.strings.len(), 1, "{}", case.name);
            assert!(parsed.bools.is_empty(), "{}", case.name);
            assert!(parsed.lists.is_empty(), "{}", case.name);
            assert_eq!(parsed.positionals.len(), 1, "{}", case.name);
        }
    }

    #[test]
    fn test_input_ordered_worker_results_restores_config_order() {
        fn worker_result(ip: &str) -> WorkerResult {
            WorkerResult {
                input: InputParams {
                    target_args: vec![format!("ip={}", ip)],
                    ip: ip.to_string(),
                    package_list: Vec::new(),
                    special: None,
                    oem_parameters: None,
                    system_name: None,
                    update_delay: 0,
                },
                task_id_list: Vec::new(),
                rf_target: None,
                json_dict: None,
                err_status: 0,
                is_powershelf: false,
            }
        }

        let ordered = input_ordered_worker_results(vec![
            (2, worker_result("192.0.2.3")),
            (0, worker_result("192.0.2.1")),
            (1, worker_result("192.0.2.2")),
        ]);

        let ips: Vec<&str> = ordered.iter().map(|wr| wr.input.ip.as_str()).collect();
        assert_eq!(ips, vec!["192.0.2.1", "192.0.2.2", "192.0.2.3"]);
    }

    #[test]
    fn test_parsed_args() {
        let mut pa = ParsedArgs::new();
        pa.bools.insert("json".to_string(), true);
        pa.strings.insert("timeout".to_string(), "900".to_string());
        pa.lists.insert(
            "package".to_string(),
            vec!["a.fwpkg".to_string(), "b.fwpkg".to_string()],
        );
        pa.positionals.push("enable".to_string());

        assert!(pa.get_bool("json"));
        assert!(!pa.get_bool("nonexistent"));
        assert_eq!(pa.get_string("timeout"), Some("900"));
        assert_eq!(pa.get_list("package"), &["a.fwpkg", "b.fwpkg"]);
        assert_eq!(pa.get_positional(0), Some("enable"));
    }

    #[tokio::test]
    async fn test_resolve_ip_passthrough() {
        // Non-resolvable should pass through
        let result = resolve_ip("192.168.1.1").await;
        assert!(!result.is_empty());
    }
}
