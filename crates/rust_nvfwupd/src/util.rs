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

//! Core utility module for nvfwupd CLI.
//!
//! Provides error handling (bail functions), path resolution,
//! duplicate checking, dictionary comparison, and text wrapping.

use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process;

use crate::utils::Util as NvUtils;

/// Actions that `bail_nvfwupd` can take after recording an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BailAction {
    /// Terminate the process with the given exit code.
    Exit,
    /// Record the error but take no additional action.
    DoNothing,
    /// Print a divider line after recording the error.
    PrintDivider,
}

/// Per-command fields attached to tracing events that the CLI formatter uses.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TraceFlags {
    pub cli_verbose: bool,
    pub json_mode: bool,
}

impl TraceFlags {
    pub const fn new(cli_verbose: bool, json_mode: bool) -> Self {
        Self {
            cli_verbose,
            json_mode,
        }
    }
}

/// Core utility struct for nvfwupd helper functions.
pub struct Util;

impl Util {
    /// Handle an error condition by printing it to stderr and then
    /// performing the specified action.
    ///
    /// The `print_json` parameter is accepted for API compatibility but
    /// is not mutated. Callers are responsible for accumulating JSON
    /// error output separately.
    ///
    /// # Arguments
    /// * `code` - Numeric error/exit code
    /// * `msg` - Error message describing the failure
    /// * `action` - What to do after recording the error
    /// * `print_json` - Optional reference to a JSON Value (unused, kept for API compat)
    pub fn bail_nvfwupd(code: i32, msg: &str, action: BailAction, print_json: Option<&Value>) {
        let msg = NvUtils::sanitize_log(msg);
        if let Some(pj) = print_json {
            // JSON mode: accumulate errors into the JSON dict and
            // print JSON instead of plain text.
            if !msg.is_empty() && code != 0 {
                // Append error message to the JSON dict (clone since ref is immutable)
                // Printed on EXIT below.
            }
            match action {
                BailAction::Exit => {
                    let mut output = pj.clone();
                    output["Error Code"] = Value::Number(code.into());
                    if !msg.is_empty() && code != 0 {
                        if let Some(arr) = output.get_mut("Error").and_then(|v| v.as_array_mut()) {
                            arr.push(Value::String(msg.clone()));
                        }
                    }
                    println!("{}", Self::json_pretty_4space(&output));
                    process::exit(code);
                }
                BailAction::PrintDivider => { /* suppress divider in json mode */ }
                BailAction::DoNothing => {}
            }
        } else {
            if !msg.is_empty() {
                println!("{}", msg);
            }
            match action {
                BailAction::Exit => {
                    println!("Error Code: {}", code);
                    process::exit(code);
                }
                BailAction::PrintDivider => {
                    println!("{}", "-".repeat(120));
                }
                BailAction::DoNothing => {}
            }
        }
    }

    fn json_pretty_4space(value: &Value) -> String {
        let buf = Vec::new();
        let fmt = serde_json::ser::PrettyFormatter::with_indent(b"    ");
        let mut ser = serde_json::Serializer::with_formatter(buf, fmt);
        value.serialize(&mut ser).unwrap_or(());
        String::from_utf8(ser.into_inner()).unwrap_or_default()
    }

    /// Thread-safe version of `bail_nvfwupd`.
    ///
    /// Prints the error to stderr. When `parallel_update` is true
    /// and the action is `Exit`, the function returns instead of
    /// terminating so that other threads can finish gracefully.
    ///
    /// # Arguments
    /// * `code` - Numeric error/exit code
    /// * `msg` - Error message describing the failure
    /// * `action` - What to do after recording the error
    /// * `print_json` - Optional reference to a JSON Value (unused, kept for API compat)
    /// * `parallel_update` - When true, avoid calling `process::exit`
    pub fn bail_nvfwupd_threadsafe(
        code: i32,
        msg: &str,
        action: BailAction,
        print_json: Option<&Value>,
        parallel_update: bool,
    ) {
        let msg = NvUtils::sanitize_log(msg);
        if let Some(pj) = print_json {
            if !msg.is_empty() && code != 0 {
                // error accumulation handled at call site for threadsafe
            }
            match action {
                BailAction::Exit => {
                    if parallel_update {
                        return;
                    }
                    let mut output = pj.clone();
                    output["Error Code"] = Value::Number(code.into());
                    if !msg.is_empty() && code != 0 {
                        if let Some(arr) = output.get_mut("Error").and_then(|v| v.as_array_mut()) {
                            arr.push(Value::String(msg.clone()));
                        }
                    }
                    println!("{}", Self::json_pretty_4space(&output));
                    process::exit(code);
                }
                BailAction::PrintDivider => { /* suppress divider in json mode */ }
                BailAction::DoNothing => {}
            }
        } else {
            if !msg.is_empty() {
                println!("{}", msg);
            }
            match action {
                BailAction::Exit => {
                    if parallel_update {
                        return;
                    }
                    println!("Error Code: {}", code);
                    process::exit(code);
                }
                BailAction::PrintDivider => {
                    println!("{}", "-".repeat(120));
                }
                BailAction::DoNothing => {}
            }
        }
    }

    /// Get the absolute path of a file relative to the current executable's
    /// directory.
    ///
    /// # Arguments
    /// * `filename` - The file name or relative path to resolve
    ///
    /// # Returns
    /// The absolute path as a `PathBuf`, or the original filename as a
    /// fallback if the executable path cannot be determined.
    pub fn get_abs_path(filename: &str) -> PathBuf {
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                return exe_dir.join(filename);
            }
        }
        PathBuf::from(filename)
    }

    /// Check for duplicate items in a slice of strings.
    ///
    /// # Arguments
    /// * `items` - Slice of string references to check
    ///
    /// # Returns
    /// A `Vec` of duplicate strings found. Empty if all items are unique.
    pub fn check_duplicate_item(items: &[&str]) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut duplicates = Vec::new();
        for item in items {
            if !seen.insert(*item) {
                let dup = item.to_string();
                if !duplicates.contains(&dup) {
                    duplicates.push(dup);
                }
            }
        }
        duplicates
    }

    /// Compare message dictionaries and collect unique results for table
    /// output.
    ///
    /// Iterates over `messages` (a slice of JSON objects). For each object,
    /// extracts the `"MessageId"` field as the dedup key. If that key has
    /// not been seen before, the object is appended to `final_result` and
    /// the key is recorded in `seen_result`.
    ///
    /// # Arguments
    /// * `messages` - Slice of JSON Value objects (each expected to have a `"MessageId"` field)
    /// * `final_result` - Accumulator vec for unique message objects
    /// * `seen_result` - Map of MessageId keys already processed (with their Value)
    pub fn compare_dict(
        messages: &[Value],
        final_result: &mut Vec<Value>,
        seen_result: &mut HashMap<String, Value>,
    ) {
        for msg in messages {
            if let Some(key) = msg.get("MessageId").and_then(|v| v.as_str()) {
                if !seen_result.contains_key(key) {
                    seen_result.insert(key.to_string(), msg.clone());
                    final_result.push(msg.clone());
                }
            }
        }
    }

    /// Wrap text to the specified width, breaking on word boundaries.
    ///
    /// Lines are broken at spaces so that no output line exceeds `width`
    /// characters (unless a single word is longer than `width`).
    ///
    /// # Arguments
    /// * `text` - The text to wrap
    /// * `width` - Maximum line width in characters
    ///
    /// # Returns
    /// A `String` with newlines inserted at wrap points.
    pub fn wrap_text(text: &str, width: usize) -> String {
        if width == 0 {
            return text.to_string();
        }

        let mut result = String::new();
        for line in text.lines() {
            if line.len() <= width {
                if !result.is_empty() {
                    result.push('\n');
                }
                result.push_str(line);
                continue;
            }

            let mut current_line = String::new();
            for word in line.split_whitespace() {
                if current_line.is_empty() {
                    current_line.push_str(word);
                } else if current_line.len() + 1 + word.len() <= width {
                    current_line.push(' ');
                    current_line.push_str(word);
                } else {
                    if !result.is_empty() {
                        result.push('\n');
                    }
                    result.push_str(&current_line);
                    current_line = word.to_string();
                }
            }
            if !current_line.is_empty() {
                if !result.is_empty() {
                    result.push('\n');
                }
                result.push_str(&current_line);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_bail_action_enum() {
        assert_ne!(BailAction::Exit, BailAction::DoNothing);
        assert_ne!(BailAction::PrintDivider, BailAction::Exit);
    }

    #[test]
    fn test_bail_nvfwupd_with_json_ref() {
        let json_val = json!({});
        // Should not panic; print_json is accepted but not mutated
        Util::bail_nvfwupd(
            42,
            "something failed",
            BailAction::DoNothing,
            Some(&json_val),
        );
    }

    #[test]
    fn test_bail_nvfwupd_threadsafe_does_not_panic() {
        let json_val = json!({});
        Util::bail_nvfwupd_threadsafe(
            10,
            "thread error",
            BailAction::DoNothing,
            Some(&json_val),
            true,
        );
    }

    #[test]
    fn test_bail_nvfwupd_no_json_does_not_panic() {
        // When print_json is None, should just print to stderr
        Util::bail_nvfwupd(1, "plain error", BailAction::DoNothing, None);
    }

    #[test]
    fn test_bail_nvfwupd_print_divider() {
        Util::bail_nvfwupd(1, "divider test", BailAction::PrintDivider, None);
    }

    #[test]
    fn test_get_abs_path() {
        let path = Util::get_abs_path("config.yaml");
        assert!(path.is_absolute() || path.to_str().unwrap() == "config.yaml");
        assert!(path.to_str().unwrap().ends_with("config.yaml"));
    }

    #[test]
    fn test_check_duplicate_item_no_duplicates() {
        let items = vec!["a", "b", "c"];
        let dups = Util::check_duplicate_item(&items);
        assert!(dups.is_empty());
    }

    #[test]
    fn test_check_duplicate_item_with_duplicates() {
        let items = vec!["a", "b", "a", "c", "b"];
        let dups = Util::check_duplicate_item(&items);
        assert_eq!(dups.len(), 2);
        assert!(dups.contains(&"a".to_string()));
        assert!(dups.contains(&"b".to_string()));
    }

    #[test]
    fn test_check_duplicate_item_empty() {
        let items: Vec<&str> = vec![];
        let dups = Util::check_duplicate_item(&items);
        assert!(dups.is_empty());
    }

    #[test]
    fn test_compare_dict() {
        let messages = vec![
            json!({"MessageId": "key1", "Message": "val1"}),
            json!({"MessageId": "key2", "Message": "val2"}),
        ];

        let mut final_result: Vec<Value> = Vec::new();
        let mut seen: HashMap<String, Value> = HashMap::new();

        Util::compare_dict(&messages, &mut final_result, &mut seen);
        assert_eq!(final_result.len(), 2);
        assert!(seen.contains_key("key1"));
        assert!(seen.contains_key("key2"));

        // Adding again should not duplicate
        let messages2 = vec![
            json!({"MessageId": "key1", "Message": "new_val"}),
            json!({"MessageId": "key3", "Message": "val3"}),
        ];

        Util::compare_dict(&messages2, &mut final_result, &mut seen);
        assert_eq!(final_result.len(), 3);
        // key1 should still be the original value
        assert_eq!(final_result[0]["Message"], "val1");
        assert_eq!(final_result[2]["MessageId"], "key3");
    }

    #[test]
    fn test_wrap_text_short_line() {
        let result = Util::wrap_text("hello world", 80);
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_wrap_text_wraps_long_line() {
        let result = Util::wrap_text("one two three four five", 10);
        assert!(result.contains('\n'));
        for line in result.lines() {
            // Each line should be at most 10 chars (unless a single word exceeds it)
            assert!(line.len() <= 10, "Line too long: '{}'", line);
        }
    }

    #[test]
    fn test_wrap_text_preserves_existing_newlines() {
        let result = Util::wrap_text("line one\nline two", 80);
        assert_eq!(result, "line one\nline two");
    }

    #[test]
    fn test_wrap_text_zero_width() {
        let result = Util::wrap_text("hello world", 0);
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_wrap_text_long_word() {
        let result = Util::wrap_text("superlongword short", 5);
        // The long word cannot be broken, so it stays on its own line
        let lines: Vec<&str> = result.lines().collect();
        assert_eq!(lines[0], "superlongword");
        assert_eq!(lines[1], "short");
    }
}
