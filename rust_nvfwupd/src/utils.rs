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

//! Utils module for nvfwupd
//!
//! Provides log sanitization and utility functions for the nvfwupd tool.

use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::process::Command;
use std::sync::{LazyLock, RwLock};

use log::error;

// ---------------------------------------------------------------------------
// Built-in log sanitizer patterns
// ---------------------------------------------------------------------------

/// Supported built-in log sanitizer regex identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltInLogSanitizers {
    Ipv4,
    Ipv6,
}

impl BuiltInLogSanitizers {
    /// Returns the regex pattern string for this built-in sanitizer.
    pub fn pattern(&self) -> &'static str {
        match self {
            BuiltInLogSanitizers::Ipv4 => {
                r"((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])"
            }
            BuiltInLogSanitizers::Ipv6 => {
                concat!(
                    r"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|",
                    r"([0-9a-fA-F]{1,4}:){1,7}:|",
                    r"([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|",
                    r"([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|",
                    r"([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|",
                    r"([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|",
                    r"([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|",
                    r"[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|",
                    r":((:[0-9a-fA-F]{1,4}){1,7}|:)|",
                    r"fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|",
                    r"::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}",
                    r"(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|",
                    r"([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}",
                    r"(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))"
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LogSanitizer
// ---------------------------------------------------------------------------

/// Sanitizes log output by replacing sensitive strings (IPs, credentials)
/// with a configurable replacement string (default `"XXXX"`).
///
/// Accepts an explicit list of literal strings to filter as well as built-in
/// regex patterns for IPv4 / IPv6 addresses. All patterns are compiled into
/// a single [`Regex`] for efficient matching.
pub struct LogSanitizer {
    compiled: Option<Regex>,
    replacement: String,
}

impl LogSanitizer {
    /// Default replacement token used when sensitive data is found.
    pub const DEFAULT_REPLACEMENT: &'static str = "XXXX";

    /// Creates a new `LogSanitizer`.
    ///
    /// # Arguments
    /// * `string_list`        - Literal strings to filter (will be regex-escaped
    ///                          and wrapped in word-boundary assertions).
    /// * `replacement_string` - Token that replaces every match (default `"XXXX"`).
    /// * `additional_regex`   - Slice of [`BuiltInLogSanitizers`] whose patterns
    ///                          will be appended to the compiled regex.
    pub fn new(
        string_list: Option<&[String]>,
        replacement_string: Option<&str>,
        additional_regex: Option<&[BuiltInLogSanitizers]>,
    ) -> Self {
        let replacement = replacement_string
            .unwrap_or(Self::DEFAULT_REPLACEMENT)
            .to_string();

        let mut patterns: Vec<String> = Vec::new();

        // Escape literal strings and wrap with word-boundary-like lookarounds.
        // Python used (?<!\w) and (?!\w) which are equivalent to \b on word chars.
        if let Some(list) = string_list {
            for s in list {
                if !s.is_empty() {
                    let escaped = regex::escape(s);
                    patterns.push(format!(r"(?-u:\b){}(?-u:\b)", escaped));
                }
            }
        }

        // Append built-in regex patterns.
        let defaults = [BuiltInLogSanitizers::Ipv4, BuiltInLogSanitizers::Ipv6];
        let builtins = additional_regex.unwrap_or(&defaults);
        for sanitizer in builtins {
            patterns.push(sanitizer.pattern().to_string());
        }

        let compiled = if patterns.is_empty() {
            None
        } else {
            let joined = patterns.join("|");
            match Regex::new(&joined) {
                Ok(re) => Some(re),
                Err(e) => {
                    error!("Failed to compile log sanitizer regex: {}", e);
                    None
                }
            }
        };

        Self {
            compiled,
            replacement,
        }
    }

    /// Returns a no-op sanitizer that passes strings through unchanged.
    pub fn noop() -> Self {
        Self {
            compiled: None,
            replacement: Self::DEFAULT_REPLACEMENT.to_string(),
        }
    }

    /// Sanitizes `input` by replacing all matches with the replacement string.
    pub fn sanitize(&self, input: &str) -> String {
        match &self.compiled {
            Some(re) => re
                .replace_all(input, self.replacement.as_str())
                .into_owned(),
            None => input.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Global sanitization state
// ---------------------------------------------------------------------------

/// Global flag controlling whether log sanitization is enabled.
static IS_SANITIZE: RwLock<bool> = RwLock::new(true);

/// Global sanitization config (key-value pairs parsed from CLI arguments).
static SANITIZE_CONFIG: std::sync::LazyLock<RwLock<Option<HashMap<String, String>>>> =
    std::sync::LazyLock::new(|| RwLock::new(None));

static SECRET_DOUBLE_QUOTED_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>["']?(?P<key>[A-Za-z0-9_.-]*(?:password|passwd|pwd|pass)[A-Za-z0-9_.-]*)["']?\s*(?:=|:)\s*)
        "(?P<value>[^"]*)"
        "#,
    )
    .expect("valid double-quoted secret field regex")
});

static SECRET_SINGLE_QUOTED_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>["']?(?P<key>[A-Za-z0-9_.-]*(?:password|passwd|pwd|pass)[A-Za-z0-9_.-]*)["']?\s*(?:=|:)\s*)
        '(?P<value>[^']*)'
        "#,
    )
    .expect("valid single-quoted secret field regex")
});

static SECRET_BARE_EQUALS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>["']?(?P<key>[A-Za-z0-9_.-]*(?:password|passwd|pwd|pass)[A-Za-z0-9_.-]*)["']?\s*=\s*)
        (?P<value>[^"',}\]\r\n]*?)
        (?P<suffix>["']|\s+[A-Za-z0-9_.-]+\s*=|\s*[,}\]]|$)
        "#,
    )
    .expect("valid bare equals secret field regex")
});

static SECRET_BARE_COLON_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>["']?(?P<key>[A-Za-z0-9_.-]*(?:password|passwd|pwd|pass)[A-Za-z0-9_.-]*)["']?\s*:\s*)
        (?P<value>[^,\r\n}\])]+)
        "#,
    )
    .expect("valid bare colon secret field regex")
});

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
        "mgx-nvl",
        "gb200switch",
        "gb300switch",
        "powershelf",
        "vrswitch",
        "vrnvl72switch",
        "vr",
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
    /// If `enabled` is `false` or `log_config` is `None`, returns a no-op
    /// sanitizer that passes strings through unchanged.
    pub fn get_sanitizer(
        log_config: Option<&HashMap<String, String>>,
        enabled: bool,
    ) -> LogSanitizer {
        if !enabled {
            return LogSanitizer::new(Some(&[]), None, Some(&[]));
        }

        let log_config = match log_config {
            Some(c) => c,
            None => return LogSanitizer::new(Some(&[]), None, Some(&[])),
        };

        let filter_list: Vec<String> = Self::CRED_FIELDS
            .iter()
            .filter_map(|&field| log_config.get(field).filter(|v| !v.is_empty()).cloned())
            .collect();

        LogSanitizer::new(Some(&filter_list), None, None)
    }

    /// Returns true when `field` is a password-like key that should never be logged.
    pub fn is_secret_field_name(field: &str) -> bool {
        let normalized = field
            .trim_matches(|c: char| c == '"' || c == '\'' || c.is_ascii_whitespace())
            .replace(['-', '.'], "_")
            .to_ascii_lowercase();

        normalized == "password"
            || normalized == "passwd"
            || normalized == "pwd"
            || normalized == "pass"
            || normalized.ends_with("_password")
            || normalized.ends_with("_passwd")
            || normalized.ends_with("_pwd")
            || normalized.ends_with("_pass")
            || normalized.contains("password")
    }

    fn redact_secret_replacement(caps: &regex::Captures<'_>, quoted: bool) -> String {
        let Some(key) = caps.name("key").map(|m| m.as_str()) else {
            return caps
                .get(0)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();
        };

        if !Self::is_secret_field_name(key) {
            return caps
                .get(0)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();
        }

        let prefix = caps.name("prefix").map(|m| m.as_str()).unwrap_or_default();
        let suffix = caps.name("suffix").map(|m| m.as_str()).unwrap_or_default();
        if quoted {
            format!(r#"{prefix}"{}"{suffix}"#, LogSanitizer::DEFAULT_REPLACEMENT)
        } else {
            format!("{prefix}{}{suffix}", LogSanitizer::DEFAULT_REPLACEMENT)
        }
    }

    /// Redacts password-like key/value fields independent of SANITIZE_LOG.
    pub fn redact_secret_fields(data: &str) -> String {
        let data = SECRET_DOUBLE_QUOTED_RE
            .replace_all(data, |caps: &regex::Captures<'_>| {
                Self::redact_secret_replacement(caps, true)
            })
            .into_owned();
        let data = SECRET_SINGLE_QUOTED_RE
            .replace_all(&data, |caps: &regex::Captures<'_>| {
                Self::redact_secret_replacement(caps, true)
            })
            .into_owned();
        let data = SECRET_BARE_EQUALS_RE
            .replace_all(&data, |caps: &regex::Captures<'_>| {
                Self::redact_secret_replacement(caps, false)
            })
            .into_owned();
        SECRET_BARE_COLON_RE
            .replace_all(&data, |caps: &regex::Captures<'_>| {
                Self::redact_secret_replacement(caps, false)
            })
            .into_owned()
    }

    /// Redacts one CLI-style `key=value` argument without relying on whitespace delimiters.
    pub fn redact_secret_key_value_arg(arg: &str) -> String {
        let Some((key, _value)) = arg.split_once('=') else {
            return Self::redact_secret_fields(arg);
        };

        if Self::is_secret_field_name(key) {
            format!("{key}={}", LogSanitizer::DEFAULT_REPLACEMENT)
        } else {
            Self::redact_secret_fields(arg)
        }
    }

    /// Redacts a value when its owning key is password-like, otherwise scans the value.
    pub fn redact_secret_value_for_key(key: &str, value: &str) -> String {
        if Self::is_secret_field_name(key) {
            LogSanitizer::DEFAULT_REPLACEMENT.to_string()
        } else {
            Self::redact_secret_fields(value)
        }
    }

    /// Recursively redacts password-like keys from a JSON value.
    pub fn redact_secret_json_value(value: &Value) -> Value {
        match value {
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(key, value)| {
                        let redacted = if Self::is_secret_field_name(key) {
                            Value::String(LogSanitizer::DEFAULT_REPLACEMENT.to_string())
                        } else {
                            Self::redact_secret_json_value(value)
                        };
                        (key.clone(), redacted)
                    })
                    .collect(),
            ),
            Value::Array(values) => {
                Value::Array(values.iter().map(Self::redact_secret_json_value).collect())
            }
            Value::String(value) => Value::String(Self::redact_secret_fields(value)),
            _ => value.clone(),
        }
    }

    /// Pretty-prints JSON after recursively redacting password-like fields.
    pub fn redacted_json_pretty_4space(value: &Value) -> String {
        let redacted = Self::redact_secret_json_value(value);
        let buf = Vec::new();
        let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
        let mut ser = serde_json::Serializer::with_formatter(buf, formatter);
        redacted.serialize(&mut ser).unwrap_or(());
        String::from_utf8(ser.into_inner()).unwrap_or_default()
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

    #[test]
    fn test_sanitize_ipv4() {
        let sanitizer = LogSanitizer::new(None, None, Some(&[BuiltInLogSanitizers::Ipv4]));
        let result = sanitizer.sanitize("Connecting to 192.168.1.100 on port 443");
        assert_eq!(result, "Connecting to XXXX on port 443");
    }

    #[test]
    fn test_sanitize_literal_strings() {
        let secrets = vec!["admin".to_string(), "s3cret".to_string()];
        let sanitizer = LogSanitizer::new(Some(&secrets), None, Some(&[]));
        let result = sanitizer.sanitize("user=admin pass=s3cret");
        assert_eq!(result, "user=XXXX pass=XXXX");
    }

    #[test]
    fn test_noop_sanitizer() {
        let sanitizer = LogSanitizer::noop();
        let input = "unchanged 192.168.1.1 data";
        assert_eq!(sanitizer.sanitize(input), input);
    }

    #[test]
    fn test_custom_replacement() {
        let sanitizer = LogSanitizer::new(
            None,
            Some("[REDACTED]"),
            Some(&[BuiltInLogSanitizers::Ipv4]),
        );
        let result = sanitizer.sanitize("host 10.0.0.1 is up");
        assert_eq!(result, "host [REDACTED] is up");
    }

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
    fn test_redact_secret_fields_masks_password_like_keys() {
        let secret = "plain_secret";
        let cases = [
            format!("password={secret}"),
            format!("password={secret} with spaces"),
            format!("RF_PASSWORD: {secret}"),
            format!(r#"{{"RF_PASSWORD": "{secret}"}}"#),
            format!("{{'RF_Pass': '{secret}'}}"),
            format!(r#"{{"target": ["ip=192.0.2.1", "password={secret}"]}}"#),
            format!(r#"{{"target": ["ip=192.0.2.1", "password={secret} with spaces"]}}"#),
        ];

        for case in cases {
            let redacted = Util::redact_secret_fields(&case);
            assert!(!redacted.contains(secret), "{redacted}");
            assert!(!redacted.contains("with spaces"), "{redacted}");
            assert!(redacted.contains(LogSanitizer::DEFAULT_REPLACEMENT));
        }

        assert_eq!(Util::redact_secret_fields("compass=north"), "compass=north");
    }

    #[test]
    fn test_redact_secret_key_value_arg_masks_whitespace_password_value() {
        let redacted = Util::redact_secret_key_value_arg("password=alpha beta gamma");
        assert_eq!(redacted, "password=XXXX");

        let redacted = Util::redact_secret_key_value_arg("user=admin");
        assert_eq!(redacted, "user=admin");
    }

    #[test]
    fn test_redact_secret_json_value_masks_nested_passwords() {
        let value = serde_json::json!({
            "Targets": [
                {
                    "BMC_IP": "192.0.2.1",
                    "RF_USERNAME": "admin",
                    "RF_PASSWORD": "plain_secret",
                    "Nested": {
                        "bmc_ssh_password": "other_secret"
                    }
                }
            ]
        });

        let redacted = Util::redact_secret_json_value(&value);
        let rendered = redacted.to_string();
        assert!(!rendered.contains("plain_secret"));
        assert!(!rendered.contains("other_secret"));
        assert!(rendered.contains(LogSanitizer::DEFAULT_REPLACEMENT));
    }

    #[test]
    fn test_redacted_json_pretty_4space_masks_response_fields() {
        let value = serde_json::json!({
            "Messages": [
                {
                    "Message": "BMC echoed password=raw response secret",
                    "MessageArgs": ["password=arg secret"]
                }
            ],
            "Oem": {
                "Password": "nested secret"
            }
        });

        let rendered = Util::redacted_json_pretty_4space(&value);

        assert!(!rendered.contains("raw response secret"));
        assert!(!rendered.contains("arg secret"));
        assert!(!rendered.contains("nested secret"));
        assert!(rendered.contains(LogSanitizer::DEFAULT_REPLACEMENT));
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
        assert_eq!(Util::SUPPORTED_PLATFORMS.len(), 19);
    }
}
