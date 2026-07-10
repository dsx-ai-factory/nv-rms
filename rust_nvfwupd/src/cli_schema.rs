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

//! CLI schema loader and accessor.
//!
//! Loads the `cli_schema.yaml` file that defines CLI commands and global
//! options, and provides methods to query command definitions, global
//! options, and per-command options.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use noyalib::compat::serde_yaml;
use serde::{Deserialize, Deserializer};

// ---------------------------------------------------------------------------
// YAML‑mapped structs
// ---------------------------------------------------------------------------

/// Top-level schema loaded from `cli_schema.yaml`.
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaFile {
    #[serde(rename = "GlobalOptions")]
    pub global_options: GlobalOptionsBlock,

    #[serde(rename = "Commands")]
    pub commands: Vec<CommandSchema>,
}

/// The `GlobalOptions` block in the YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct GlobalOptionsBlock {
    #[serde(rename = "Groups", default)]
    pub groups: Vec<String>,

    #[serde(rename = "Usage", default)]
    pub usage: String,

    #[serde(rename = "Options", default)]
    pub options: Vec<HashMap<String, GlobalOption>>,
}

/// A single global option entry (values inside the option‑name map).
#[derive(Debug, Clone, Deserialize)]
pub struct GlobalOption {
    #[serde(rename = "Description", default)]
    pub description: String,

    #[serde(rename = "Short", default)]
    pub short: String,

    #[serde(rename = "Long", default)]
    pub long: String,

    #[serde(rename = "Arg", default)]
    pub arg: Option<String>,

    /// Arity token from the schema.
    ///
    /// This stays string-shaped because valid CLI schema values include
    /// argparse tokens such as `+` and `*`, as well as exact counts such as
    /// `1`.
    #[serde(
        rename = "Nargs",
        default,
        deserialize_with = "deserialize_optional_string_scalar"
    )]
    pub nargs: Option<String>,

    #[serde(rename = "Action", default)]
    pub action: String,

    #[serde(rename = "Required", default)]
    pub required: Option<bool>,

    #[serde(rename = "Validator", default)]
    pub validator: Option<String>,
}

/// A command definition inside the `Commands` list.
#[derive(Debug, Clone, Deserialize)]
pub struct CommandSchema {
    #[serde(rename = "Name")]
    pub name: String,

    #[serde(rename = "Class")]
    pub class_name: String,

    #[serde(rename = "RequireGlobalOption", default)]
    pub require_global_option: bool,

    #[serde(rename = "Description", default)]
    pub description: String,

    #[serde(rename = "Usage", default)]
    pub usage: String,

    #[serde(rename = "Options", default)]
    pub options: Vec<CommandOption>,
}

/// A per-command option entry.
#[derive(Debug, Clone, Deserialize)]
pub struct CommandOption {
    #[serde(rename = "Short", default)]
    pub short: Option<String>,

    #[serde(rename = "Long")]
    pub long: String,

    #[serde(rename = "Action", default)]
    pub action: String,

    /// Arity token from the schema.
    ///
    /// This stays string-shaped because valid CLI schema values include
    /// argparse tokens such as `+` and `*`, as well as exact counts such as
    /// `1`.
    #[serde(
        rename = "Nargs",
        default,
        deserialize_with = "deserialize_optional_string_scalar"
    )]
    pub nargs: Option<String>,

    #[serde(rename = "Description", default)]
    pub description: String,

    #[serde(rename = "Required", default)]
    pub required: Option<bool>,

    #[serde(rename = "Validator", default)]
    pub validator: Option<String>,
}

// The schema file may express exact arity as an unquoted number, while the
// command parser consumes all arity values as string tokens. Normalize string
// and integer YAML scalars into that internal representation.
#[derive(Deserialize)]
#[serde(untagged)]
enum StringScalar {
    String(String),
    Unsigned(u64),
    Signed(i64),
}

impl StringScalar {
    fn into_string(self) -> String {
        match self {
            StringScalar::String(value) => value,
            StringScalar::Unsigned(value) => value.to_string(),
            StringScalar::Signed(value) => value.to_string(),
        }
    }
}

fn deserialize_optional_string_scalar<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<StringScalar>::deserialize(deserializer)?;
    Ok(value.map(StringScalar::into_string))
}

// ---------------------------------------------------------------------------
// CLISchema — runtime wrapper
// ---------------------------------------------------------------------------

/// Runtime handle for the parsed CLI schema.
///
/// Mirrors the Python `CLISchema` class: call [`CLISchema::new`] to create an
/// empty instance, then [`CLISchema::load_schema`] to populate it from the
/// YAML file.
#[derive(Debug, Clone)]
pub struct CLISchema {
    pub schema_data: Option<SchemaFile>,
}

/// The CLI schema YAML embedded at compile time.
///
/// Matches Python's pyinstaller-built binary where `cli_schema.yaml` is
/// bundled into the single-file executable. Keeping this as `include_str!`
/// (rather than loading from disk at startup) means:
///   1. The shipped binary is fully standalone — no external file required
///      at runtime.
///   2. It is impossible to run with a schema that doesn't match the
///      compiled code.
///   3. Cargo will fail the build if `src/cli_schema.yaml` is missing,
///      turning a runtime surprise into a compile-time error.
///
/// The YAML source lives at `src/cli_schema.yaml` inside this crate. When
/// updating the schema, edit that file and rebuild. The Python source copy
/// at `flm/src/nvfwupd/cli_schema.yaml` is kept in sync manually (the two
/// files should always be byte-identical).
const EMBEDDED_SCHEMA_YAML: &str = include_str!("cli_schema.yaml");

impl CLISchema {
    /// Create an empty (unloaded) schema.
    pub fn new() -> Self {
        CLISchema { schema_data: None }
    }

    /// Load the CLI schema from the compile-time-embedded YAML.
    ///
    /// This is the preferred loader for production use — no disk I/O,
    /// no dependency on an external file, and no way for the schema to
    /// get out of sync with the built binary.
    pub fn load_embedded_schema(&mut self) -> Result<(), String> {
        let schema: SchemaFile = serde_yaml::from_str(EMBEDDED_SCHEMA_YAML)
            .map_err(|e| format!("Failed to parse embedded schema YAML: {}", e))?;
        self.schema_data = Some(schema);
        Ok(())
    }

    /// Load and parse the CLI schema from a YAML file at `path`.
    ///
    /// Retained for tests and development workflows that want to exercise
    /// an alternative schema without rebuilding. Production code should
    /// use [`CLISchema::load_embedded_schema`] instead.
    pub fn load_schema(&mut self, path: &str) -> Result<(), String> {
        let contents = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read schema file '{}': {}", path, e))?;
        let schema: SchemaFile = serde_yaml::from_str(&contents)
            .map_err(|e| format!("Failed to parse schema YAML '{}': {}", path, e))?;
        self.schema_data = Some(schema);
        Ok(())
    }

    // -- accessors ----------------------------------------------------------

    /// Return the list of command names defined in the schema.
    pub fn get_command_list(&self) -> Vec<String> {
        match &self.schema_data {
            Some(data) => data.commands.iter().map(|c| c.name.clone()).collect(),
            None => Vec::new(),
        }
    }

    /// Return global options as a map of *option‑name* → [`GlobalOption`].
    ///
    /// The YAML stores global options as a list of single‑entry maps
    /// (e.g. `- Target: { ... }`). This flattens them into one
    /// `HashMap<String, GlobalOption>`.
    pub fn get_global_options(&self) -> HashMap<String, GlobalOption> {
        let mut options_dict = HashMap::new();
        if let Some(data) = &self.schema_data {
            for entry in &data.global_options.options {
                for (name, opt) in entry {
                    options_dict.insert(name.clone(), opt.clone());
                }
            }
        }
        options_dict
    }

    /// Return the option list for a given command name.
    ///
    /// Returns an empty `Vec` when the command has no options or the command
    /// name is not found (matching the Python behaviour).
    pub fn get_command_options(&self, cmd: &str) -> Vec<CommandOption> {
        match self.get_command_schema(cmd) {
            Some(schema) => schema.options.clone(),
            None => Vec::new(),
        }
    }

    /// Look up the full [`CommandSchema`] for a command by name.
    pub fn get_command_schema(&self, cmd_name: &str) -> Option<&CommandSchema> {
        self.schema_data
            .as_ref()?
            .commands
            .iter()
            .find(|c| c.name == cmd_name)
    }

    /// Return the global‑options usage string from the schema.
    pub fn get_global_usage(&self) -> &str {
        match &self.schema_data {
            Some(data) => &data.global_options.usage,
            None => "",
        }
    }
}

// ---------------------------------------------------------------------------
// Standalone helper
// ---------------------------------------------------------------------------

/// Return the path to `cli_schema.yaml` located next to the running
/// executable.
///
/// The schema file is expected to live in the same directory as the binary
/// (mirroring the Python convention of resolving relative to the script).
pub fn get_schema_path() -> String {
    let filename = "cli_schema.yaml";

    // 1. Check next to the binary (installed / release layout)
    if let Ok(exe) = std::env::current_exe() {
        let mut p = exe.clone();
        p.pop();
        p.push(filename);
        if p.exists() {
            return p.to_string_lossy().into_owned();
        }
    }

    // 2. Check the original Python nvfwupd source directory (sibling of rust_nvfwupd)
    if let Ok(exe) = std::env::current_exe() {
        // exe is e.g. .../rust_nvfwupd/target/release/nvfwupd
        // walk up until we find the nvfwupd/ sibling that has cli_schema.yaml
        let mut dir = exe.clone();
        for _ in 0..6 {
            dir.pop();
            let candidate = dir.join("nvfwupd").join(filename);
            if candidate.exists() {
                return candidate.to_string_lossy().into_owned();
            }
        }
    }

    // 3. Check current working directory
    let cwd = PathBuf::from(filename);
    if cwd.exists() {
        return cwd.to_string_lossy().into_owned();
    }

    // 4. Fallback: return the binary-relative path (will produce a clear error)
    let mut fallback = std::env::current_exe()
        .map(|p| {
            let mut d = p;
            d.pop();
            d
        })
        .unwrap_or_else(|_| PathBuf::from("."));
    fallback.push(filename);
    fallback.to_string_lossy().into_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn sample_yaml() -> &'static str {
        r#"
GlobalOptions:
  Groups:
    - Target
  Usage: "[-t BMC-TARGET] [-v] < command >"
  Options:
    - Target:
        Description: BMC target
        Short: t
        Long: target
        Arg: "<target>"
        Nargs: "+"
        Action: store
        Validator: validate_target
    - Verbosity:
        Description: Increase verbosity
        Short: v
        Long: verbose
        Action: store
        Nargs: "*"
Commands:
  - Name: show_version
    Class: ShowVersion
    RequireGlobalOption: true
    Description: Show firmware versions.
    Usage: "show_version -p FWPKG"
    Options:
      - Description: PLDM firmware package
        Short: p
        Long: package
        Nargs: "+"
        Action: store
        Required: false
      - Description: show output in JSON
        Short: j
        Long: json
        Action: store_true
        Required: false
  - Name: help
    Class: Help
    RequireGlobalOption: false
    Description: Show tool help.
"#
    }

    fn load_sample() -> CLISchema {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(sample_yaml().as_bytes()).unwrap();
        let path = tmp.path().to_string_lossy().to_string();
        let mut schema = CLISchema::new();
        schema.load_schema(&path).unwrap();
        schema
    }

    #[test]
    fn test_get_command_list() {
        let schema = load_sample();
        let cmds = schema.get_command_list();
        assert_eq!(cmds, vec!["show_version", "help"]);
    }

    #[test]
    fn test_get_global_options() {
        let schema = load_sample();
        let opts = schema.get_global_options();
        assert!(opts.contains_key("Target"));
        assert!(opts.contains_key("Verbosity"));
        assert_eq!(opts["Target"].short, "t");
        assert_eq!(opts["Target"].long, "target");
    }

    #[test]
    fn test_get_command_options() {
        let schema = load_sample();
        let opts = schema.get_command_options("show_version");
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[0].long, "package");
        assert_eq!(opts[1].long, "json");
    }

    #[test]
    fn test_get_command_options_empty() {
        let schema = load_sample();
        let opts = schema.get_command_options("help");
        assert!(opts.is_empty());
    }

    #[test]
    fn test_get_command_schema() {
        let schema = load_sample();
        let cmd = schema.get_command_schema("show_version").unwrap();
        assert_eq!(cmd.class_name, "ShowVersion");
        assert!(cmd.require_global_option);
    }

    #[test]
    fn test_get_command_schema_not_found() {
        let schema = load_sample();
        assert!(schema.get_command_schema("nonexistent").is_none());
    }

    #[test]
    fn test_load_embedded_schema() {
        let mut schema = CLISchema::new();
        schema
            .load_embedded_schema()
            .expect("embedded schema should parse");

        assert!(schema.get_global_options().contains_key("Target"));
        assert!(schema.get_command_schema("update_fw").is_some());

        let force_update_options = schema.get_command_options("force_update");
        let force_update_action = force_update_options
            .iter()
            .find(|option| option.long == "force_upd_action")
            .expect("force_update action option should exist");
        assert_eq!(force_update_action.nargs.as_deref(), Some("1"));
    }

    #[test]
    fn test_empty_schema() {
        let schema = CLISchema::new();
        assert!(schema.get_command_list().is_empty());
        assert!(schema.get_global_options().is_empty());
        assert!(schema.get_command_schema("any").is_none());
    }

    #[test]
    fn test_load_schema_bad_path() {
        let mut schema = CLISchema::new();
        assert!(schema.load_schema("/nonexistent/path.yaml").is_err());
    }
}
