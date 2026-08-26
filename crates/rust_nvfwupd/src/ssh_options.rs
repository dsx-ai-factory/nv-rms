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

//! Shared SSH/SFTP host-key option parsing.

use std::collections::HashMap;

use crate::ssh_transport;

pub(crate) const SSH_KNOWN_HOSTS_ARG: &str = "ssh_known_hosts";
pub(crate) const SSH_HOST_KEY_MODE_ARG: &str = "ssh_host_key_mode";
pub(crate) const SSH_HOST_KEY_MODE_DISABLED: &str = "disabled";
pub(crate) const SSH_HOST_KEY_MODE_TOFU: &str = "tofu";
pub(crate) const SSH_HOST_KEY_MODE_STRICT: &str = "strict";

/// Parsed SSH/SFTP host-key verification options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SshHostKeyOptions {
    pub(crate) known_hosts: Option<String>,
    pub(crate) mode: String,
}

/// Return whether a user-facing host-key mode string is supported.
pub(crate) fn ssh_host_key_mode_string_value_supported(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        SSH_HOST_KEY_MODE_DISABLED | SSH_HOST_KEY_MODE_TOFU | SSH_HOST_KEY_MODE_STRICT
    )
}

fn parse_ssh_host_key_mode(
    mode_arg: Option<&str>,
    known_hosts_present: bool,
) -> Result<String, String> {
    match mode_arg {
        None if known_hosts_present => Ok(SSH_HOST_KEY_MODE_TOFU.to_string()),
        // SSH host-key verification is wired and ready in NVFWUPD; default
        // enablement is pending final customer discussions.
        None => Ok(SSH_HOST_KEY_MODE_DISABLED.to_string()),
        Some(value) => {
            let value = value.trim();
            if ssh_host_key_mode_string_value_supported(value) {
                Ok(value.to_ascii_lowercase())
            } else {
                Err(format!(
                    "{SSH_HOST_KEY_MODE_ARG} must be one of {SSH_HOST_KEY_MODE_DISABLED}, {SSH_HOST_KEY_MODE_TOFU}, or {SSH_HOST_KEY_MODE_STRICT}"
                ))
            }
        }
    }
}

/// Parse shared SSH/SFTP host-key options from a target argument dictionary.
pub(crate) fn parse_ssh_options(
    arg_dict: &HashMap<String, String>,
) -> Result<SshHostKeyOptions, String> {
    let known_hosts = arg_dict
        .get(SSH_KNOWN_HOSTS_ARG)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if arg_dict
        .get(SSH_KNOWN_HOSTS_ARG)
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(format!("{SSH_KNOWN_HOSTS_ARG} must be a non-empty path"));
    }

    let explicit_mode = arg_dict.get(SSH_HOST_KEY_MODE_ARG).map(String::as_str);
    let mode = parse_ssh_host_key_mode(explicit_mode, known_hosts.is_some())?;
    if mode == SSH_HOST_KEY_MODE_DISABLED && known_hosts.is_some() {
        return Err(format!(
            "{SSH_KNOWN_HOSTS_ARG} cannot be used when {SSH_HOST_KEY_MODE_ARG}={SSH_HOST_KEY_MODE_DISABLED}; \
             remove {SSH_KNOWN_HOSTS_ARG} or use {SSH_HOST_KEY_MODE_ARG}={SSH_HOST_KEY_MODE_TOFU} or {SSH_HOST_KEY_MODE_STRICT}"
        ));
    }

    Ok(SshHostKeyOptions { known_hosts, mode })
}

/// Build a transport policy from parsed SSH/SFTP host-key options.
pub(crate) fn host_key_policy(
    known_hosts: Option<String>,
    mode: &str,
) -> ssh_transport::SshHostKeyPolicy {
    match mode.trim().to_ascii_lowercase().as_str() {
        SSH_HOST_KEY_MODE_DISABLED => ssh_transport::SshHostKeyPolicy::insecure_accept_any(),
        SSH_HOST_KEY_MODE_STRICT => ssh_transport::SshHostKeyPolicy::strict(known_hosts),
        _ => ssh_transport::SshHostKeyPolicy::trust_on_first_use(known_hosts),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_ssh_host_key_mode_defaults_to_disabled() {
        let options = parse_ssh_options(&HashMap::new()).expect("omitted mode should default");

        assert_eq!(options.mode, SSH_HOST_KEY_MODE_DISABLED);
    }

    #[test]
    fn known_hosts_without_mode_opts_into_tofu() {
        let mut args = HashMap::new();
        args.insert(
            SSH_KNOWN_HOSTS_ARG.to_string(),
            "/tmp/known_hosts".to_string(),
        );

        let options = parse_ssh_options(&args).expect("known_hosts should opt into verification");

        assert_eq!(options.known_hosts.as_deref(), Some("/tmp/known_hosts"));
        assert_eq!(options.mode, SSH_HOST_KEY_MODE_TOFU);
    }

    #[test]
    fn explicit_empty_ssh_host_key_mode_is_rejected() {
        let mut args = HashMap::new();
        args.insert(SSH_HOST_KEY_MODE_ARG.to_string(), "  ".to_string());

        let err = parse_ssh_options(&args).expect_err("blank mode should be invalid");

        assert!(err.contains(SSH_HOST_KEY_MODE_ARG));
        assert!(err.contains(SSH_HOST_KEY_MODE_TOFU));
    }
}
