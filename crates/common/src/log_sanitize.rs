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

//! Configurable log sanitizer for literal strings and IP addresses.
//!
//! A [`LogSanitizer`] compiles an optional list of literal secrets together
//! with built-in IPv4/IPv6 patterns into a single [`Regex`] and replaces every
//! match with a replacement token. It carries no global state, so callers own
//! the policy for what to filter.

use regex::{NoExpand, Regex};
use tracing::error;

use crate::DEFAULT_REPLACEMENT;

/// Whether `c` is an ASCII word character (`[0-9A-Za-z_]`), matching the
/// character class used by the non-Unicode `\b` boundary `(?-u:\b)`.
fn is_ascii_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

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
    pub const DEFAULT_REPLACEMENT: &'static str = DEFAULT_REPLACEMENT;

    /// Creates a new `LogSanitizer`.
    ///
    /// # Arguments
    /// * `string_list` - Literal strings to filter (regex-escaped and wrapped in
    ///   word-boundary assertions).
    /// * `replacement_string` - Token that replaces every match (default `"XXXX"`).
    /// * `additional_regex` - Slice of [`BuiltInLogSanitizers`] whose patterns are
    ///   appended to the compiled regex.
    pub fn new(
        string_list: Option<&[String]>,
        replacement_string: Option<&str>,
        additional_regex: Option<&[BuiltInLogSanitizers]>,
    ) -> Self {
        let replacement = replacement_string
            .unwrap_or(Self::DEFAULT_REPLACEMENT)
            .to_string();

        let mut patterns: Vec<String> = Vec::new();

        // Escape literal strings and guard them so a secret is not matched as a
        // substring of a larger word. Python used the lookarounds `(?<!\w)` /
        // `(?!\w)`; Rust's regex engine has no lookaround, so we approximate with
        // `\b` (ASCII, matching `[0-9A-Za-z_]`). A `\b` is only emitted on a side
        // whose adjacent secret character is itself a word character: placing `\b`
        // next to a punctuation edge (e.g. `hunter2!` or `!hunter2`) can never
        // match and would silently leak that secret.
        if let Some(list) = string_list {
            // Order literals by descending length so that when two secrets
            // overlap (e.g. "hunter2" and "hunter2!"), the longer alternative
            // appears first in the combined regex. Rust's regex uses
            // leftmost-first (preference-order) alternation, so a shorter prefix
            // placed first would win and leak the remainder of the longer
            // secret. The secondary lexical order just keeps the output stable.
            let mut ordered: Vec<&String> = list.iter().filter(|s| !s.is_empty()).collect();
            ordered.sort_by(|a, b| {
                b.len()
                    .cmp(&a.len())
                    .then_with(|| a.as_str().cmp(b.as_str()))
            });

            for s in ordered {
                let escaped = regex::escape(s);
                let mut pattern = String::new();
                if s.chars().next().is_some_and(is_ascii_word_char) {
                    pattern.push_str(r"(?-u:\b)");
                }
                pattern.push_str(&escaped);
                if s.chars().next_back().is_some_and(is_ascii_word_char) {
                    pattern.push_str(r"(?-u:\b)");
                }
                patterns.push(pattern);
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
            // `NoExpand` treats the replacement as literal text: without it a
            // replacement containing `$0`/`$1`/`${name}` would be interpreted as
            // a capture-expansion string and re-insert the matched secret.
            Some(re) => re
                .replace_all(input, NoExpand(self.replacement.as_str()))
                .into_owned(),
            None => input.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_ipv4() {
        let sanitizer = LogSanitizer::new(None, None, Some(&[BuiltInLogSanitizers::Ipv4]));
        let result = sanitizer.sanitize("Connecting to 192.168.1.100 on port 443");
        assert_eq!(result, "Connecting to XXXX on port 443");
    }

    #[test]
    fn sanitize_literal_strings() {
        let secrets = vec!["admin".to_string(), "s3cret".to_string()];
        let sanitizer = LogSanitizer::new(Some(&secrets), None, Some(&[]));
        let result = sanitizer.sanitize("user=admin pass=s3cret");
        assert_eq!(result, "user=XXXX pass=XXXX");
    }

    #[test]
    fn literal_secrets_with_edge_punctuation_are_redacted() {
        // Regression: a `\b` on both sides never matches when the secret starts
        // or ends with punctuation, silently leaking the value.
        let secrets = vec![
            "hunter2!".to_string(), // trailing punctuation
            "!hunter2".to_string(), // leading punctuation
            "!secret!".to_string(), // both edges punctuation
        ];
        let sanitizer = LogSanitizer::new(Some(&secrets), None, Some(&[]));

        assert_eq!(sanitizer.sanitize("pass hunter2! now"), "pass XXXX now");
        assert_eq!(sanitizer.sanitize("pw=!hunter2 ok"), "pw=XXXX ok");
        assert_eq!(sanitizer.sanitize("x !secret! y"), "x XXXX y");
    }

    #[test]
    fn overlapping_literal_secrets_prefer_longer_match() {
        // Regression: Rust's regex alternation is leftmost-first, so without
        // ordering literals by descending length the shorter "hunter2" wins and
        // leaks the trailing "!" of "hunter2!". The list is given shorter-first
        // (the problematic order) to prove ordering fixes it regardless of how
        // the caller supplies the values.
        let secrets = vec!["hunter2".to_string(), "hunter2!".to_string()];
        let sanitizer = LogSanitizer::new(Some(&secrets), None, Some(&[]));

        assert_eq!(sanitizer.sanitize("pass hunter2! now"), "pass XXXX now");
        // The bare shorter secret is still redacted on its own.
        assert_eq!(sanitizer.sanitize("pass hunter2 now"), "pass XXXX now");
    }

    #[test]
    fn literal_secret_word_edges_still_avoid_substring_matches() {
        // The boundary guard is preserved on word-character edges so a secret is
        // not redacted when it is part of a larger token.
        let secrets = vec!["admin".to_string()];
        let sanitizer = LogSanitizer::new(Some(&secrets), None, Some(&[]));

        assert_eq!(sanitizer.sanitize("user=admin"), "user=XXXX");
        assert_eq!(sanitizer.sanitize("administrator"), "administrator");
        assert_eq!(sanitizer.sanitize("badminton"), "badminton");
    }

    #[test]
    fn noop_sanitizer_passes_through() {
        let sanitizer = LogSanitizer::noop();
        let input = "unchanged 192.168.1.1 data";
        assert_eq!(sanitizer.sanitize(input), input);
    }

    #[test]
    fn empty_filter_list_and_no_builtins_is_pass_through() {
        // Mirrors the "disabled" path callers rely on: no literals, no
        // built-in IP patterns leaves the input untouched.
        let sanitizer = LogSanitizer::new(Some(&[]), None, Some(&[]));
        assert_eq!(sanitizer.sanitize("10.0.0.1 admin"), "10.0.0.1 admin");
    }

    #[test]
    fn default_builtins_redact_ipv4() {
        // additional_regex = None selects the IPv4 + IPv6 defaults; this is the
        // path `get_sanitizer(None, true)`-style callers rely on for address
        // redaction.
        let sanitizer = LogSanitizer::new(None, None, None);
        assert_eq!(sanitizer.sanitize("host 10.0.0.1 up"), "host XXXX up");
    }

    #[test]
    fn custom_replacement_token() {
        let sanitizer = LogSanitizer::new(
            None,
            Some("[REDACTED]"),
            Some(&[BuiltInLogSanitizers::Ipv4]),
        );
        let result = sanitizer.sanitize("host 10.0.0.1 is up");
        assert_eq!(result, "host [REDACTED] is up");
    }

    #[test]
    fn replacement_with_capture_syntax_stays_literal() {
        // Regression: a replacement containing regex expansion syntax such as
        // `$0` must be emitted literally, not expanded into the matched text --
        // otherwise the "redacted" output would re-leak the secret it matched.
        let sanitizer = LogSanitizer::new(None, Some("$0"), Some(&[BuiltInLogSanitizers::Ipv4]));
        let result = sanitizer.sanitize("host 10.0.0.1 up");
        assert_eq!(result, "host $0 up");
    }
}
