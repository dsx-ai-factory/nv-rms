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

//! Secret redaction primitives.
//!
//! These helpers mask password/token-like key/value fields in free text and
//! JSON so credentials never reach logs, error messages, or diagnostics.
//!
//! They are deliberately free functions with no global state: redaction of
//! password-like fields is a mandatory, always-on transform, independent of
//! any per-process "sanitize logs" toggle a caller may layer on top.

use std::sync::LazyLock;

use regex::Regex;
use serde::Serialize;
use serde_json::Value;

use crate::DEFAULT_REPLACEMENT;

static SECRET_DOUBLE_QUOTED_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>["']?(?P<key>[A-Za-z0-9_.-]*(?:password|passwd|pwd|pass|token|secret|credential|api[_-]?key|authorization|cookie|session)[A-Za-z0-9_.-]*)["']?\s*(?:=|:)\s*)
        "(?P<value>(?:\\.|[^"\\])*)"
        "#,
    )
    .expect("valid double-quoted secret field regex")
});

static SECRET_SINGLE_QUOTED_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>["']?(?P<key>[A-Za-z0-9_.-]*(?:password|passwd|pwd|pass|token|secret|credential|api[_-]?key|authorization|cookie|session)[A-Za-z0-9_.-]*)["']?\s*(?:=|:)\s*)
        '(?P<value>(?:\\.|[^'\\])*)'
        "#,
    )
    .expect("valid single-quoted secret field regex")
});

static SECRET_BARE_EQUALS_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>["']?(?P<key>[A-Za-z0-9_.-]*(?:password|passwd|pwd|pass|token|secret|credential|api[_-]?key|authorization|cookie|session)[A-Za-z0-9_.-]*)["']?\s*=\s*)
        (?P<value>[^"',}\]\r\n]*?)
        (?P<suffix>["']|\s+[A-Za-z0-9_.-]+\s*=|\s*[,}\]]|$)
        "#,
    )
    .expect("valid bare equals secret field regex")
});

static SECRET_BARE_COLON_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>["']?(?P<key>[A-Za-z0-9_.-]*(?:password|passwd|pwd|pass|token|secret|credential|api[_-]?key|authorization|cookie|session)[A-Za-z0-9_.-]*)["']?\s*:\s*)
        (?P<value>[^,\r\n}\])]+)
        "#,
    )
    .expect("valid bare colon secret field regex")
});

/// Returns true when `field` is a secret-like key that should never be logged.
pub fn is_secret_field_name(field: &str) -> bool {
    let normalized = field
        .trim_matches(|c: char| c == '"' || c == '\'' || c.is_ascii_whitespace())
        .replace(['-', '.'], "_")
        .to_ascii_lowercase();

    const EXACT_SECRET_FIELDS: &[&str] = &[
        "password",
        "passwd",
        "pwd",
        "pass",
        "token",
        "secret",
        "credential",
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "set_cookie",
        "session",
    ];
    const SECRET_SUFFIXES: &[&str] = &[
        "token",
        "secret",
        "credential",
        "_password",
        "_passwd",
        "_pwd",
        "_pass",
        "_token",
        "_secret",
        "_credential",
        "_api_key",
        "_apikey",
        "_authorization",
        "_cookie",
        "_session",
    ];

    EXACT_SECRET_FIELDS.contains(&normalized.as_str())
        || SECRET_SUFFIXES
            .iter()
            .any(|suffix| normalized.ends_with(suffix))
        || normalized.contains("password")
        || normalized.contains("api_key")
        || normalized.contains("apikey")
}

fn redact_secret_replacement(caps: &regex::Captures<'_>, quoted: bool) -> String {
    let Some(key) = caps.name("key").map(|m| m.as_str()) else {
        return caps
            .get(0)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
    };

    if !is_secret_field_name(key) {
        return caps
            .get(0)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
    }

    let prefix = caps.name("prefix").map(|m| m.as_str()).unwrap_or_default();
    let suffix = caps.name("suffix").map(|m| m.as_str()).unwrap_or_default();
    if quoted {
        format!(r#"{prefix}"{}"{suffix}"#, DEFAULT_REPLACEMENT)
    } else {
        format!("{prefix}{}{suffix}", DEFAULT_REPLACEMENT)
    }
}

/// Redacts password-like key/value fields independent of any sanitize toggle.
pub fn redact_secret_fields(data: &str) -> String {
    let data = SECRET_DOUBLE_QUOTED_RE
        .replace_all(data, |caps: &regex::Captures<'_>| {
            redact_secret_replacement(caps, true)
        })
        .into_owned();
    let data = SECRET_SINGLE_QUOTED_RE
        .replace_all(&data, |caps: &regex::Captures<'_>| {
            redact_secret_replacement(caps, true)
        })
        .into_owned();
    let data = SECRET_BARE_EQUALS_RE
        .replace_all(&data, |caps: &regex::Captures<'_>| {
            redact_secret_replacement(caps, false)
        })
        .into_owned();
    SECRET_BARE_COLON_RE
        .replace_all(&data, |caps: &regex::Captures<'_>| {
            redact_secret_replacement(caps, false)
        })
        .into_owned()
}

/// Redacts one CLI-style `key=value` argument without relying on whitespace delimiters.
pub fn redact_secret_key_value_arg(arg: &str) -> String {
    let Some((key, _value)) = arg.split_once('=') else {
        return redact_secret_fields(arg);
    };

    if is_secret_field_name(key) {
        format!("{key}={}", DEFAULT_REPLACEMENT)
    } else {
        redact_secret_fields(arg)
    }
}

/// Redacts a value when its owning key is password-like, otherwise scans the value.
pub fn redact_secret_value_for_key(key: &str, value: &str) -> String {
    if is_secret_field_name(key) {
        DEFAULT_REPLACEMENT.to_string()
    } else {
        redact_secret_fields(value)
    }
}

/// Recursively redacts password-like keys from a JSON value.
pub fn redact_secret_json_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let redacted = if is_secret_field_name(key) {
                        Value::String(DEFAULT_REPLACEMENT.to_string())
                    } else {
                        redact_secret_json_value(value)
                    };
                    (key.clone(), redacted)
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact_secret_json_value).collect()),
        Value::String(value) => Value::String(redact_secret_fields(value)),
        _ => value.clone(),
    }
}

/// Pretty-prints JSON after recursively redacting password-like fields.
pub fn redacted_json_pretty_4space(value: &Value) -> String {
    let redacted = redact_secret_json_value(value);
    let buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(buf, formatter);
    redacted.serialize(&mut ser).unwrap_or(());
    String::from_utf8(ser.into_inner()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_password_like_keys() {
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
            let redacted = redact_secret_fields(&case);
            assert!(!redacted.contains(secret), "{redacted}");
            assert!(!redacted.contains("with spaces"), "{redacted}");
            assert!(redacted.contains(DEFAULT_REPLACEMENT));
        }

        assert_eq!(redact_secret_fields("compass=north"), "compass=north");
    }

    #[test]
    fn masks_token_like_keys() {
        let secret = "token_secret_value";
        let cases = [
            format!("AccessToken={secret}"),
            format!("X-Auth-Token: {secret}"),
            format!("api_key={secret}"),
            format!("authorization=\"Bearer {secret}\""),
            format!("cookie='{secret}'"),
            format!("session={secret}"),
            format!(r#"{{"client_secret": "{secret}"}}"#),
        ];

        for case in cases {
            let redacted = redact_secret_fields(&case);
            assert!(!redacted.contains(secret), "{redacted}");
            assert!(redacted.contains(DEFAULT_REPLACEMENT));
        }

        assert_eq!(
            redact_secret_fields("sessional_metric=allowed"),
            "sessional_metric=allowed"
        );
    }

    #[test]
    fn masks_escaped_quotes_within_quoted_values() {
        // Regression: the quoted-value groups previously stopped at the first
        // delimiter, so an escaped quote inside the value truncated the match
        // and leaked the tail (e.g. the `cd` in `ab\"cd`).

        // Double-quoted value containing an escaped double quote.
        let redacted = redact_secret_fields(r#"{"password":"ab\"cd"}"#);
        assert!(!redacted.contains("ab"), "{redacted}");
        assert!(!redacted.contains("cd"), "{redacted}");
        assert!(redacted.contains(DEFAULT_REPLACEMENT));

        // Single-quoted value containing an escaped single quote.
        let redacted = redact_secret_fields(r#"password='ab\'cd'"#);
        assert!(!redacted.contains("ab"), "{redacted}");
        assert!(!redacted.contains("cd"), "{redacted}");
        assert!(redacted.contains(DEFAULT_REPLACEMENT));

        // A trailing escaped backslash before the closing quote must not confuse
        // the delimiter tracking.
        let redacted = redact_secret_fields(r#"{"token":"ab\\"}"#);
        assert!(!redacted.contains("\"ab"), "{redacted}");
        assert!(redacted.contains(DEFAULT_REPLACEMENT));
    }

    #[test]
    fn is_secret_field_name_matches_expected_keys() {
        for key in [
            "password",
            "RF_PASSWORD",
            "bmc_ssh_password",
            "AccessToken",
            "X-Auth-Token",
            "api_key",
            "apiKey",
            "authorization",
            "Set-Cookie",
            "session",
            "client_secret",
        ] {
            assert!(is_secret_field_name(key), "expected secret key: {key}");
        }

        for key in ["user", "ip", "compass", "sessional_metric", "description"] {
            assert!(!is_secret_field_name(key), "unexpected secret key: {key}");
        }
    }

    #[test]
    fn key_value_arg_masks_whitespace_password_value() {
        assert_eq!(
            redact_secret_key_value_arg("password=alpha beta gamma"),
            "password=XXXX"
        );
        assert_eq!(redact_secret_key_value_arg("user=admin"), "user=admin");
    }

    #[test]
    fn value_for_key_masks_only_secret_keys() {
        assert_eq!(redact_secret_value_for_key("password", "hunter2"), "XXXX");
        // Non-secret key still scans the value for embedded secret fields.
        assert_eq!(
            redact_secret_value_for_key("detail", "echoed password=hunter2"),
            "echoed password=XXXX"
        );
        assert_eq!(
            redact_secret_value_for_key("detail", "no secrets here"),
            "no secrets here"
        );
    }

    #[test]
    fn json_value_masks_nested_passwords() {
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

        let rendered = redact_secret_json_value(&value).to_string();
        assert!(!rendered.contains("plain_secret"));
        assert!(!rendered.contains("other_secret"));
        assert!(rendered.contains(DEFAULT_REPLACEMENT));
    }

    #[test]
    fn json_value_masks_token_like_keys() {
        let value = serde_json::json!({
            "AccessToken": "token_secret_value",
            "api_key": "api_secret_value",
            "authorization": "Bearer auth_secret_value",
            "Set-Cookie": "cookie_secret_value",
            "nested": {
                "session": "session_secret_value"
            }
        });

        let rendered = redact_secret_json_value(&value).to_string();

        for secret in [
            "token_secret_value",
            "api_secret_value",
            "auth_secret_value",
            "cookie_secret_value",
            "session_secret_value",
        ] {
            assert!(!rendered.contains(secret), "{rendered}");
        }
        assert!(rendered.contains(DEFAULT_REPLACEMENT));
    }

    #[test]
    fn pretty_4space_masks_response_fields() {
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

        let rendered = redacted_json_pretty_4space(&value);

        assert!(!rendered.contains("raw response secret"));
        assert!(!rendered.contains("arg secret"));
        assert!(!rendered.contains("nested secret"));
        assert!(rendered.contains(DEFAULT_REPLACEMENT));
        // 4-space pretty formatting is preserved.
        assert!(rendered.contains("\n    \"Messages\""));
    }
}
