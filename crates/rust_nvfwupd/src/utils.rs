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

//! Utils module for nvfwupd
//!
//! Provides log sanitization and utility functions for the nvfwupd tool.
//!
//! The generic secret-redaction and log-sanitization primitives now live in
//! the shared [`common`] crate. This module keeps nvfwupd's public API stable
//! (re-exporting the sanitizer types and delegating the redaction helpers on
//! [`Util`]) while retaining the nvfwupd-specific policy: the CLI sanitize
//! config, global toggle, credential field list, and platform helpers.

use serde_json::Value;
use std::collections::HashMap;
use std::process::Command;
use std::sync::RwLock;

use tracing::error;

// Re-exported from the shared `common` crate so existing
// `nvfwupd::utils::{LogSanitizer, BuiltInLogSanitizers}` import paths keep
// working.
pub use common::log_sanitize::{BuiltInLogSanitizers, LogSanitizer};

// ---------------------------------------------------------------------------
// Global sanitization state
// ---------------------------------------------------------------------------

/// Global flag controlling whether log sanitization is enabled.
static IS_SANITIZE: RwLock<bool> = RwLock::new(true);

/// Global sanitization config (key-value pairs parsed from CLI arguments).
static SANITIZE_CONFIG: std::sync::LazyLock<RwLock<Option<HashMap<String, String>>>> =
    std::sync::LazyLock::new(|| RwLock::new(None));

// ---------------------------------------------------------------------------
// Util
// ---------------------------------------------------------------------------

/// Collection of static utility methods mirroring the Python `Util` class.
pub struct Util;

impl Util {
    /// Platforms supported by nvfwupd.
    pub const SUPPORTED_PLATFORMS: &'static [&'static str] = &[
        "gh",
        "dgx",
        "dgxrubin",
        "hgx",
        "gh200",
        "gb200",
        "gb300",
        "hgxb100",
        "hgxb300",
        "hgxrubin",
        "mgx",
        "gb200switch",
        "gb300switch",
        "powershelf",
        "vrnvl72switch",
        "vrnvl72",
    ];

    /// Credential fields checked when building the sanitizer filter list.
    const CRED_FIELDS: &'static [&'static str] = &[
        "BMC_IP",
        "BMC_USERNAME",
        "BMC_PASSWORD",
        "RF_USERNAME",
        "RF_PASSWORD",
        "RF_USER",
        "RF_PASS",
        "HOST_IP",
        "HOST_USERNAME",
        "HOST_PASSWORD",
        "RF_User",
        "RF_Pass",
        "BMC_SSH_USERNAME",
        "BMC_SSH_PASSWORD",
        "HMC_IP",
        "username",
        "user",
        "password",
        "passwd",
        "pwd",
        "pass",
        "ip",
    ];

    // -- Global state accessors ---------------------------------------------

    /// Returns the current value of the global sanitize flag.
    pub fn is_sanitize() -> bool {
        *IS_SANITIZE.read().expect("IS_SANITIZE lock poisoned")
    }

    /// Sets the global sanitize flag.
    pub fn set_is_sanitize(value: bool) {
        *IS_SANITIZE.write().expect("IS_SANITIZE lock poisoned") = value;
    }

    /// Returns a clone of the current global sanitize config.
    pub fn sanitize_config() -> Option<HashMap<String, String>> {
        SANITIZE_CONFIG
            .read()
            .expect("SANITIZE_CONFIG lock poisoned")
            .clone()
    }

    /// Replaces the global sanitize config.
    pub fn set_sanitize_config(config: Option<HashMap<String, String>>) {
        *SANITIZE_CONFIG
            .write()
            .expect("SANITIZE_CONFIG lock poisoned") = config;
    }

    // -- Static helpers -----------------------------------------------------

    /// Returns the default log configuration list.
    pub fn default_log_config() -> Vec<String> {
        vec![
            "ip=xxx".to_string(),
            "user=xxx".to_string(),
            "password=xxx".to_string(),
        ]
    }

    /// Pings the given host once and returns `true` if the ping succeeds.
    pub fn ping_to_check_system(host: &str) -> bool {
        if host.starts_with('-') {
            return false;
        }

        Command::new("ping")
            .args(["-c", "1", host])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    /// Return a trimmed same-origin request path when a Redfish/NVUE URI is safe
    /// to append to the already-selected target base URL.
    ///
    /// This intentionally accepts only path-style values such as
    /// `/redfish/v1/UpdateService/update-multipart`. Absolute URLs,
    /// authority-style paths (`//host/path`), empty values, and control
    /// characters are rejected so device/config supplied URIs cannot redirect
    /// authenticated requests away from the selected target.
    pub fn same_origin_request_path(uri: &str) -> Option<&str> {
        if uri.chars().any(char::is_control) {
            return None;
        }

        let uri = uri.trim();
        if uri.is_empty() || !uri.starts_with('/') || uri.starts_with("//") {
            return None;
        }

        Some(uri)
    }

    /// Return whether `uri` is a safe same-origin request path.
    pub fn valid_same_origin_request_path(uri: &str) -> bool {
        Self::same_origin_request_path(uri).is_some()
    }

    /// Parses a `"key=value"` config list into a [`HashMap`].
    ///
    /// Returns `None` if `config` is empty or `None`.
    /// On parse error, logs an error and returns `None`.
    ///
    /// When `json_mode` is `true` the error message indicates JSON output
    /// context (mirrors the Python behaviour of `bail_nvfwupd` with a JSON
    /// output dict).
    pub fn get_log_sanitize_config(
        config: Option<&[String]>,
        json_mode: bool,
    ) -> Option<HashMap<String, String>> {
        let config = config?;
        if config.is_empty() {
            return None;
        }

        let mut result = HashMap::new();
        for item in config {
            match item.split_once('=') {
                Some((key, value)) => {
                    result.insert(key.to_string(), value.to_string());
                }
                None => {
                    let item = Self::redact_secret_fields(item);
                    if json_mode {
                        error!("Invalid command argument for '{}'. (JSON mode)", item);
                    } else {
                        error!("Invalid command argument for '{}'.", item);
                    }
                    return None;
                }
            }
        }
        Some(result)
    }

    /// Builds a [`LogSanitizer`] from the given config map.
    ///
    /// If `enabled` is `false`, returns a no-op sanitizer that passes strings
    /// through unchanged. When `log_config` is `None`, only the built-in
    /// default IP address redaction is applied.
    pub fn get_sanitizer(
        log_config: Option<&HashMap<String, String>>,
        enabled: bool,
    ) -> LogSanitizer {
        if !enabled {
            return LogSanitizer::new(Some(&[]), None, Some(&[]));
        }

        let log_config = match log_config {
            Some(c) => c,
            None => return LogSanitizer::new(Some(&[]), None, None),
        };

        let filter_list: Vec<String> = Self::CRED_FIELDS
            .iter()
            .filter_map(|&field| log_config.get(field).filter(|v| !v.is_empty()).cloned())
            .collect();

        LogSanitizer::new(Some(&filter_list), None, None)
    }

    /// Returns true when `field` is a secret-like key that should never be logged.
    ///
    /// Delegates to the shared [`common::redaction`] primitive.
    pub fn is_secret_field_name(field: &str) -> bool {
        common::redaction::is_secret_field_name(field)
    }

    /// Redacts password-like key/value fields independent of SANITIZE_LOG.
    ///
    /// Delegates to the shared [`common::redaction`] primitive.
    pub fn redact_secret_fields(data: &str) -> String {
        common::redaction::redact_secret_fields(data)
    }

    /// Redacts one CLI-style `key=value` argument without relying on whitespace delimiters.
    ///
    /// Delegates to the shared [`common::redaction`] primitive.
    pub fn redact_secret_key_value_arg(arg: &str) -> String {
        common::redaction::redact_secret_key_value_arg(arg)
    }

    /// Redacts a value when its owning key is password-like, otherwise scans the value.
    ///
    /// Delegates to the shared [`common::redaction`] primitive.
    pub fn redact_secret_value_for_key(key: &str, value: &str) -> String {
        common::redaction::redact_secret_value_for_key(key, value)
    }

    /// Recursively redacts password-like keys from a JSON value.
    ///
    /// Delegates to the shared [`common::redaction`] primitive.
    pub fn redact_secret_json_value(value: &Value) -> Value {
        common::redaction::redact_secret_json_value(value)
    }

    /// Pretty-prints JSON after recursively redacting password-like fields.
    ///
    /// Delegates to the shared [`common::redaction`] primitive.
    pub fn redacted_json_pretty_4space(value: &Value) -> String {
        common::redaction::redacted_json_pretty_4space(value)
    }

    /// Sanitizes `data` using the current global sanitization config.
    ///
    /// Password-like key/value fields are always redacted. When sanitization is
    /// disabled (via [`Util::set_is_sanitize`]), only this mandatory credential
    /// redaction is applied.
    pub fn sanitize_log(data: &str) -> String {
        let data = Self::redact_secret_fields(data);
        if !Self::is_sanitize() {
            return data;
        }
        let config = Self::sanitize_config();
        let sanitizer = Self::get_sanitizer(config.as_ref(), true);
        sanitizer.sanitize(&data)
    }

    /// Checks whether `target_access` names a supported platform and the
    /// `platform_type` is a valid Redfish access mode.
    ///
    /// Returns `Some(message)` with an error description when the platform
    /// is recognised but the access type is not Redfish-compatible, or
    /// `None` otherwise.
    pub fn target_platform_supported(
        target_access: Option<&str>,
        platform_type: &str,
    ) -> Option<String> {
        let access = target_access?.to_lowercase();

        if Self::SUPPORTED_PLATFORMS.contains(&access.as_str()) {
            if platform_type != "BMCLoginAccess" && platform_type != "BMCPortForwardAccess" {
                return Some(format!(
                    "Configured target platform is {} but target does not support Redfish service.",
                    access
                ));
            }
        }

        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) static SANITIZE_TEST_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

/// Serializes tests that depend on global sanitize state (`IS_SANITIZE`, `SANITIZE_CONFIG`).
#[cfg(test)]
pub(crate) struct SanitizeTestGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    prev_is_sanitize: bool,
    prev_sanitize_config: Option<HashMap<String, String>>,
}

#[cfg(test)]
impl SanitizeTestGuard {
    pub(crate) fn new() -> Self {
        let lock = SANITIZE_TEST_LOCK.lock().unwrap();
        let prev_is_sanitize = Util::is_sanitize();
        let prev_sanitize_config = Util::sanitize_config();
        Util::set_is_sanitize(true);
        Util::set_sanitize_config(None);
        Self {
            _lock: lock,
            prev_is_sanitize,
            prev_sanitize_config,
        }
    }
}

#[cfg(test)]
impl Drop for SanitizeTestGuard {
    fn drop(&mut self) {
        Util::set_is_sanitize(self.prev_is_sanitize);
        Util::set_sanitize_config(self.prev_sanitize_config.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: the generic `LogSanitizer` and `redact_secret_*` primitives are
    // owned and unit-tested by the shared `utils` crate. The tests below cover
    // the nvfwupd-specific policy layered on top (CLI sanitize config, the
    // global toggle, `sanitize_log`, and platform helpers); they still exercise
    // the delegation end-to-end via `Util`/the re-exported `LogSanitizer`.

    #[test]
    fn test_default_log_config() {
        let cfg = Util::default_log_config();
        assert_eq!(cfg.len(), 3);
        assert!(cfg.contains(&"ip=xxx".to_string()));
    }

    #[test]
    fn ping_to_check_system_rejects_option_shaped_hosts() {
        assert!(!Util::ping_to_check_system("-n"));
        assert!(!Util::ping_to_check_system("--help"));
    }

    #[test]
    fn same_origin_request_path_rejects_off_origin_values() {
        assert_eq!(
            Util::same_origin_request_path(" /redfish/v1/UpdateService "),
            Some("/redfish/v1/UpdateService")
        );

        for value in [
            "",
            "https://attacker.example/upload",
            "http://attacker.example/upload",
            "//attacker.example/upload",
            "redfish/v1/UpdateService",
            "/redfish/v1/UpdateService\nInjected: yes",
            "/redfish/v1/UpdateService\u{7}",
        ] {
            assert!(Util::same_origin_request_path(value).is_none());
        }
    }

    #[test]
    fn test_get_log_sanitize_config_valid() {
        let items = vec![
            "ip=10.0.0.1".to_string(),
            "user=admin".to_string(),
            "password=secret".to_string(),
        ];
        let result = Util::get_log_sanitize_config(Some(&items), false);
        assert!(result.is_some());
        let map = result.unwrap();
        assert_eq!(map.get("ip").unwrap(), "10.0.0.1");
        assert_eq!(map.get("user").unwrap(), "admin");
        assert_eq!(map.get("password").unwrap(), "secret");
    }

    #[test]
    fn test_get_log_sanitize_config_invalid() {
        let items = vec!["no_equals_sign".to_string()];
        let result = Util::get_log_sanitize_config(Some(&items), false);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_log_sanitize_config_none() {
        let result = Util::get_log_sanitize_config(None, false);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_sanitizer_disabled() {
        let config = HashMap::from([("ip".to_string(), "10.0.0.1".to_string())]);
        let sanitizer = Util::get_sanitizer(Some(&config), false);
        // Disabled sanitizer should pass through.
        assert_eq!(sanitizer.sanitize("10.0.0.1"), "10.0.0.1");
    }

    #[test]
    fn test_get_sanitizer_defaults_to_ip_redaction_without_config() {
        let sanitizer = Util::get_sanitizer(None, true);
        assert_eq!(
            sanitizer.sanitize("Updating ip address: ip=10.0.0.1"),
            "Updating ip address: ip=XXXX"
        );
    }

    #[test]
    fn test_get_sanitizer_filters_cred_fields() {
        let config = HashMap::from([
            ("ip".to_string(), "10.0.0.1".to_string()),
            ("user".to_string(), "admin".to_string()),
            ("password".to_string(), "s3cret".to_string()),
        ]);
        let sanitizer = Util::get_sanitizer(Some(&config), true);
        let result = sanitizer.sanitize("Logging in as admin with s3cret to 10.0.0.1");
        assert!(!result.contains("admin"));
        assert!(!result.contains("s3cret"));
        assert!(!result.contains("10.0.0.1"));
    }

    #[test]
    fn test_sanitize_log_always_redacts_password_fields() {
        let _guard = SanitizeTestGuard::new();
        Util::set_is_sanitize(false);
        let redacted = Util::sanitize_log("target password=plain_secret");

        assert!(!redacted.contains("plain_secret"));
        assert!(redacted.contains("password=XXXX"));
    }

    #[test]
    fn test_target_platform_supported_valid() {
        // Supported platform with Redfish access should return None.
        let result = Util::target_platform_supported(Some("gh"), "BMCLoginAccess");
        assert!(result.is_none());

        let result = Util::target_platform_supported(Some("dgxrubin"), "BMCLoginAccess");
        assert!(result.is_none());
    }

    #[test]
    fn test_target_platform_supported_no_redfish() {
        let result = Util::target_platform_supported(Some("GH"), "SomeOtherAccess");
        assert!(result.is_some());
        assert!(result.unwrap().contains("does not support Redfish"));
    }

    #[test]
    fn test_target_platform_supported_unknown() {
        let result = Util::target_platform_supported(Some("unknown_platform"), "BMCLoginAccess");
        assert!(result.is_none());
    }

    #[test]
    fn test_target_platform_supported_none() {
        let result = Util::target_platform_supported(None, "BMCLoginAccess");
        assert!(result.is_none());
    }

    #[test]
    fn test_supported_platforms_count() {
        assert_eq!(Util::SUPPORTED_PLATFORMS.len(), 16);
    }
}
