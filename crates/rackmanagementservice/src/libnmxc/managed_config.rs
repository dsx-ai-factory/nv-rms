/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reconciles caller-owned settings while preserving unrelated file contents.
//!
//! Callers provide desired values and removals through `ManagedConfig`.
//! Reconciliation reads before writing and verifies complete-file replacements.
//! Reservations, device isolation, and activation belong to the caller.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use super::{Nmxc, nmxc_model};
use crate::utilities::filename::validate_non_hidden_basename;

/// Desired values or required absence, indexed by exact file name and key.
type ConfigEntries = BTreeMap<(String, String), Option<String>>;

/// Validated settings that RMS owns for one controller.
///
/// Construct with `ManagedConfig::try_from(entries)`, where each map entry
/// contains a file basename, a key, and either a literal value or `None` for
/// removal. For example, `(("engine.cfg", "MODE"), Some("enabled"))` requests
/// a value, while `(("engine.cfg", "OLD_MODE"), None)` requests absence.
/// Files use RMS's non-hidden basename policy and are case-sensitive.
/// Keys are case-sensitive and sent unchanged to the controller.
/// Values are preserved without trimming.
///
/// The caller chooses ownership, serializes access to the controller, and
/// controls controller isolation and restart. This type does not discover
/// devices, select settings, or acquire reservations.
pub(crate) struct ManagedConfig(ConfigEntries);

impl TryFrom<ConfigEntries> for ManagedConfig {
    type Error = String;

    /// Validates every target before allowing controller I/O.
    ///
    /// # Errors
    ///
    /// Rejects invalid basenames or keys and values with empty
    /// content, surrounding whitespace, quotes, escaping, comments, or controls.
    fn try_from(entries: ConfigEntries) -> Result<Self, Self::Error> {
        for ((file, key), value) in &entries {
            validate_static_config_name(file, key).map_err(|_| {
                "managed configuration requires a file basename and an identifier key".to_owned()
            })?;

            if let Some(value) = value {
                validate_static_config_value(value).map_err(|_| {
                    "managed configuration requires nonempty unquoted literal values".to_owned()
                })?;
            }
        }

        Ok(Self(entries))
    }
}

/// Complete file records and the values or absence established for requested keys.
///
/// Obtain with `read_config(client, target, gateway).await` and compare with
/// `config_matches(&snapshot, target)`. Unrequested keys have no evidence of
/// absence; comparisons against those keys return false.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ManagedConfigSnapshot {
    files: BTreeMap<String, String>,
    values: ConfigEntries,
}

/// An active physical directive whose byte range can be replaced without reformatting.
struct ManagedDirective {
    range: Range<usize>,
    key: String,
    value: String,
}

/// Validates target names without normalizing case or reserving topology keys.
/// Catalog callers add their own ownership and reserved-key checks.
pub(crate) fn validate_static_config_name(file: &str, key: &str) -> Result<(), String> {
    if validate_non_hidden_basename(file).is_err() {
        return Err(
            "config_file_name: managed static configuration requires a file basename".into(),
        );
    }

    if key.is_empty()
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || key.as_bytes().first().is_some_and(u8::is_ascii_digit)
    {
        return Err("key: managed static configuration requires an identifier key".into());
    }

    Ok(())
}

/// Requires an unquoted target literal without escaping, comments, or controls.
/// Catalog loading and controller writes use the same syntax rules.
pub(crate) fn validate_static_config_value(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.trim() != value
        || value.chars().any(char::is_control)
        || value.contains(['\\', '\'', '"', '#', ';'])
    {
        return Err("managed static configuration values must be nonempty unquoted literal lines without surrounding whitespace, escaping, comments, or control characters".into());
    }

    Ok(())
}

/// Accepts unambiguous single-line directives; unsupported continuations fail closed.
/// Raw unowned values are retained, and only owned values require literal interpretation.
fn managed_directives(
    file: &str,
    content: &str,
    desired: &ConfigEntries,
) -> Result<Vec<ManagedDirective>, String> {
    let mut directives = Vec::new();
    let mut seen = BTreeSet::new();
    let mut offset = 0;

    for physical_line in content.split_inclusive('\n') {
        let range = offset..offset + physical_line.len();
        offset = range.end;
        let line = physical_line.trim();

        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }

        if line.bytes().rev().take_while(|byte| *byte == b'\\').count() % 2 != 0 {
            return Err("managed configuration contains an unsupported continuation".to_owned());
        }

        // Only the separator after the key defines the assignment. A value
        // in a whitespace-delimited record may itself contain '='.
        let separator = line
            .find(|character: char| character == '=' || character.is_whitespace())
            .ok_or_else(|| "managed configuration contains an ambiguous directive".to_owned())?;

        let (key, value) = line.split_at(separator);
        let value = value.trim_start();
        let value = value.strip_prefix('=').unwrap_or(value);
        let key = key.trim();

        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
        {
            return Err("managed configuration contains an unsupported key".to_owned());
        }

        let key = key.to_owned();

        if !seen.insert(key.clone()) {
            return Err("controller returned an ambiguous managed configuration entry".to_owned());
        }

        let mut value = value.trim();

        if desired.contains_key(&(file.to_owned(), key.clone())) {
            if value.starts_with(['\'', '"']) {
                let quote = value
                    .chars()
                    .next()
                    .ok_or_else(|| "managed configuration value is empty".to_owned())?;

                value = value
                    .strip_prefix(quote)
                    .and_then(|value| value.strip_suffix(quote))
                    .ok_or_else(|| {
                        "managed configuration contains an ambiguous quoted value".to_owned()
                    })?;
            }

            if value.contains(['\\', '\'', '"', '#', ';']) || value.chars().any(char::is_control) {
                return Err("managed configuration contains an unsupported owned value".to_owned());
            }
        }

        directives.push(ManagedDirective {
            range,
            key,
            value: value.to_owned(),
        });
    }

    Ok(directives)
}

/// Requires every requested file exactly once and rejects ambiguous directives.
fn complete_config(
    response: &nmxc_model::NmxGetStaticConfigResponse,
    desired: &ConfigEntries,
) -> Result<ManagedConfigSnapshot, String> {
    let files = response
        .static_config
        .as_ref()
        .and_then(|config| config.config_file_contents.as_ref())
        .ok_or_else(|| {
            "controller did not return complete requested configuration files".to_owned()
        })?;

    let required: BTreeSet<_> = desired.keys().map(|(file, _)| file.as_str()).collect();
    let mut returned = BTreeSet::new();

    for file in &files.config_file_content {
        if !returned.insert(file.config_file_name.as_str()) {
            return Err("controller returned duplicate configuration files".to_owned());
        }
    }

    if !required.is_subset(&returned) {
        return Err("controller omitted a requested configuration file".to_owned());
    }

    let mut current = ManagedConfigSnapshot {
        files: BTreeMap::new(),
        values: desired.keys().map(|key| (key.clone(), None)).collect(),
    };

    for file in &files.config_file_content {
        if !required.contains(file.config_file_name.as_str()) {
            continue;
        }

        for entry in managed_directives(&file.config_file_name, &file.config_file_content, desired)?
        {
            let key = (file.config_file_name.clone(), entry.key);

            if desired.contains_key(&key) {
                current.values.insert(key, Some(entry.value));
            }
        }

        current.files.insert(
            file.config_file_name.clone(),
            file.config_file_content.clone(),
        );
    }

    Ok(current)
}

/// Reads complete files so key absence can be distinguished from missing readback.
/// An empty target returns an empty snapshot without controller I/O.
///
/// # Errors
///
/// Returns sanitized errors for failed reads, missing or duplicate files,
/// duplicate directives, or syntax that cannot be interpreted safely.
pub(crate) async fn read_config(
    client: &mut dyn Nmxc,
    config: &ManagedConfig,
    gateway: &str,
) -> Result<ManagedConfigSnapshot, String> {
    let desired = &config.0;

    if desired.is_empty() {
        return Ok(ManagedConfigSnapshot {
            files: BTreeMap::new(),
            values: BTreeMap::new(),
        });
    }

    let files: BTreeSet<_> = desired.keys().map(|(file, _)| file.as_str()).collect();

    let response = client
        .get_static_config_files(&files.into_iter().collect::<Vec<_>>(), gateway)
        .await
        .map_err(|_| "managed configuration read unavailable".to_owned())?;

    complete_config(&response, desired)
}

/// Compares requested keys; `None` requires observed absence, not missing readback.
pub(crate) fn config_matches(current: &ManagedConfigSnapshot, config: &ManagedConfig) -> bool {
    config
        .0
        .iter()
        .all(|(key, value)| current.values.get(key) == Some(value))
}

/// Removes requested records and preserves every other byte from a fresh read.
/// Changed and missing values use the key/value API after file replacement.
fn replacement_config_file(
    file: &str,
    content: &str,
    desired: &ConfigEntries,
) -> Result<String, String> {
    let directives = managed_directives(file, content, desired)?;

    let mut output = String::new();
    let mut cursor = 0;

    for directive in directives {
        output.push_str(
            content
                .get(cursor..directive.range.start)
                .ok_or_else(|| "managed configuration record range is invalid".to_owned())?,
        );

        cursor = directive.range.end;
        let target = desired.get(&(file.to_owned(), directive.key.clone()));

        if target.is_some_and(Option::is_none) {
            continue;
        }

        output.push_str(
            content
                .get(directive.range)
                .ok_or_else(|| "managed configuration record range is invalid".to_owned())?,
        );
    }

    output.push_str(
        content
            .get(cursor..)
            .ok_or_else(|| "managed configuration record range is invalid".to_owned())?,
    );

    Ok(output)
}

/// Reconciles managed values and removals, skipping targets that already match.
///
/// Removal uses a fresh complete-file read and verifies unrelated record text
/// and multiplicity after writing; record order may change on the controller.
/// Set acknowledgements are not mutation evidence: successful readback decides
/// whether a write succeeded, including writes whose acknowledgement failed.
/// Returns whether any write was attempted, not whether the device reports a diff.
/// An empty target succeeds without controller I/O. Errors do not roll back
/// completed writes; callers must treat an interrupted or failed call as
/// potentially mutated and read the controller again before recovery.
///
/// # Errors
///
/// Returns sanitized errors for unavailable or ambiguous reads, mismatched
/// target values, or file replacements that fail preservation readback.
pub(crate) async fn reconcile_config(
    client: &mut dyn Nmxc,
    config: &ManagedConfig,
    gateway: &str,
) -> Result<bool, String> {
    let desired = &config.0;

    let mut current = read_config(client, config, gateway).await?;
    let mut changed = false;

    let deletions: BTreeSet<_> = desired
        .iter()
        .filter(|(key, value)| {
            value.is_none() && current.values.get(*key).is_some_and(Option::is_some)
        })
        .map(|((file, _), _)| file.clone())
        .collect();

    for file in deletions {
        current = read_config(client, config, gateway).await?;

        let content = current
            .files
            .get(&file)
            .ok_or_else(|| "controller omitted a requested configuration file".to_owned())?;

        let replacement = replacement_config_file(&file, content, desired)?;

        if replacement == *content {
            continue;
        }

        changed = true;

        if let Err(error) = client
            .set_static_config_file(&file, &replacement, gateway)
            .await
        {
            let (error_class, protocol_code) = match error {
                crate::libnmxc::NmxcError::Status(status) => {
                    ("grpc_status", Some(status.code() as i32))
                }
                crate::libnmxc::NmxcError::NmxReturnCode { return_code, .. } => {
                    ("nmx_status", Some(return_code))
                }
                crate::libnmxc::NmxcError::MissingServerHeader { .. } => ("missing_header", None),
                crate::libnmxc::NmxcError::Transport(_) => ("transport", None),
                crate::libnmxc::NmxcError::InvalidEndpoint(_) => ("invalid_endpoint", None),
            };

            tracing::warn!(
                error_class,
                protocol_code,
                "NMX configuration file write failed; checking authoritative readback"
            );
        }

        current = read_config(client, config, gateway).await?;

        let returned = current
            .files
            .get(&file)
            .ok_or_else(|| "controller omitted a requested configuration file".to_owned())?;

        // NMX may serialize the same directives in a different order. Preserve
        // each record's text and multiplicity while comparing complete files.
        let mut expected_records: Vec<_> = replacement.lines().collect();
        let mut returned_records: Vec<_> = returned.lines().collect();

        expected_records.sort_unstable();
        returned_records.sort_unstable();

        if returned_records != expected_records {
            return Err(
                "managed configuration file write did not pass preservation readback".to_owned(),
            );
        }
    }

    for ((file, key), value) in desired {
        let Some(value) = value else {
            continue;
        };

        if current
            .values
            .get(&(file.clone(), key.clone()))
            .and_then(Option::as_ref)
            == Some(value)
        {
            continue;
        }

        changed = true;
        client
            .set_static_config(file, key, value, gateway)
            .await
            .ok();

        current = read_config(client, config, gateway).await?;

        if current
            .values
            .get(&(file.clone(), key.clone()))
            .and_then(Option::as_ref)
            != Some(value)
        {
            return Err("managed configuration write did not pass readback".to_owned());
        }
    }

    if !config_matches(&read_config(client, config, gateway).await?, config) {
        return Err("managed configuration batch readback failed".to_owned());
    }

    Ok(changed)
}

#[cfg(test)]
mod tests;
