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

//! Redaction and bounded string helpers for SSH structured logs.
//!
//! Remote command text and interactive transcripts can contain passwords.
//! Helpers in this module keep logs and returned diagnostics useful without
//! preserving raw secret material.

use std::borrow::Cow;
use std::sync::LazyLock;

use regex::Regex;

const REDACTED_SECRET: &str = "XXXX";

/// Maximum command text emitted as a structured log field.
pub(super) const SSH_LOG_COMMAND_MAX_CHARS: usize = 160;

static PASSWORD_KEY_VALUE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?ix)
        (?P<prefix>\b(?:new[\t\x20]+password|retype[\t\x20]+(?:new[\t\x20]+)?password|re-enter[\t\x20]+new[\t\x20]+password|password|passwd|pwd)\b[\t\x20]*[:=][\t\x20]*)
        (?P<value>[^\r\n]+)
        ",
    )
    .expect("valid password key-value redaction regex")
});

static PASSWORD_ARGUMENT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?ix)
        (?P<prefix>(?:^|[[:space:]])(?:--?)?(?:password|passwd|pwd)\b[[:space:]]+)
        (?P<value>"[^"\r\n]*"|'[^'\r\n]*'|[^[:space:]\r\n]+)
        "#,
    )
    .expect("valid password argument redaction regex")
});

/// Return a redacted and clipped command string suitable for structured logs.
pub(super) fn ssh_command_log_value(command: &str) -> String {
    let redacted = scrub_sensitive_ssh_text(command, "");
    let mut chars = redacted.chars();
    let clipped: String = chars.by_ref().take(SSH_LOG_COMMAND_MAX_CHARS).collect();

    if chars.next().is_some() {
        format!("{clipped}...")
    } else {
        clipped
    }
}

/// Redact password prompt values and echoed replacement passwords.
///
/// This intentionally does not redact space-delimited `password <word>` forms
/// because recovery transcripts also contain status phrases such as "password
/// unchanged".
pub(super) fn scrub_password_recovery_transcript(transcript: &str, new_password: &str) -> String {
    scrub_password_key_values(transcript, new_password).into_owned()
}

/// Redact password prompt values, common command arguments, and echoed
/// replacement passwords from command text or command output.
///
/// `new_password` is optional because the same redaction pass is used for raw
/// command strings where the attempted password is not known separately.
pub(super) fn scrub_sensitive_ssh_text(text: &str, new_password: &str) -> String {
    let without_key_values = scrub_password_key_values(text, new_password);

    PASSWORD_ARGUMENT_RE
        .replace_all(without_key_values.as_ref(), redact_password_match)
        .into_owned()
}

/// Redact password-like key/value text and any separately known password.
fn scrub_password_key_values<'text>(text: &'text str, new_password: &str) -> Cow<'text, str> {
    let without_attempted_password = if new_password.is_empty() {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(text.replace(new_password, REDACTED_SECRET))
    };

    match without_attempted_password {
        Cow::Borrowed(text) => PASSWORD_KEY_VALUE_RE.replace_all(text, redact_password_match),
        Cow::Owned(text) => Cow::Owned(
            PASSWORD_KEY_VALUE_RE
                .replace_all(&text, redact_password_match)
                .into_owned(),
        ),
    }
}

/// Preserve the matched password label while replacing its value.
fn redact_password_match(caps: &regex::Captures<'_>) -> Cow<'static, str> {
    if let Some(prefix) = caps.name("prefix") {
        Cow::Owned(format!("{}{}", prefix.as_str(), REDACTED_SECRET))
    } else {
        Cow::Borrowed(REDACTED_SECRET)
    }
}
