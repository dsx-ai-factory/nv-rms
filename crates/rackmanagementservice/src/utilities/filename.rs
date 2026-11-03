/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared filename validation for RMS configuration and firmware operations.

/// The reason a name does not satisfy RMS's non-hidden basename policy.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BasenameError {
    /// The name is empty.
    Empty,

    /// The name contains a character outside ASCII letters, digits, `_`, `-`, and `.`.
    UnsupportedCharacters,

    /// The name starts with a dot, including `.` and `..`.
    Hidden,
}

/// Accepts a nonempty, non-hidden basename containing ASCII letters, digits,
/// `_`, `-`, and `.`. For example, `engine.cfg` is accepted and `.engine` is not.
/// This is an RMS input policy, not a check of filesystem or device support.
///
/// # Errors
///
/// Reports an empty name, unsupported characters, or a leading dot, in that order.
pub(crate) fn validate_non_hidden_basename(name: &str) -> Result<(), BasenameError> {
    if name.is_empty() {
        return Err(BasenameError::Empty);
    }

    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
    {
        return Err(BasenameError::UnsupportedCharacters);
    }

    if name.starts_with('.') {
        return Err(BasenameError::Hidden);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basename_validation_preserves_error_precedence() {
        for (name, expected) in [
            ("engine.cfg", Ok(())),
            ("FW_1-2.bin", Ok(())),
            ("-config", Ok(())),
            ("", Err(BasenameError::Empty)),
            (".", Err(BasenameError::Hidden)),
            ("..", Err(BasenameError::Hidden)),
            (".config", Err(BasenameError::Hidden)),
            ("../config", Err(BasenameError::UnsupportedCharacters)),
            ("dir\\config", Err(BasenameError::UnsupportedCharacters)),
            ("config name", Err(BasenameError::UnsupportedCharacters)),
            ("config\n", Err(BasenameError::UnsupportedCharacters)),
            ("config\0", Err(BasenameError::UnsupportedCharacters)),
            ("\u{e9}.cfg", Err(BasenameError::UnsupportedCharacters)),
        ] {
            assert_eq!(validate_non_hidden_basename(name), expected, "{name:?}");
        }
    }
}
