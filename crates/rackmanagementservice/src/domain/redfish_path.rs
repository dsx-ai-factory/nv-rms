// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Plain, same-origin Redfish resource and action paths accepted by profiles.

use std::{fmt, ops::Deref, str::FromStr, sync::Arc};

/// A validated path, without URL escapes, queries, fragments or traversal.
///
/// # Examples
/// ```
/// use rackmanagementservice::domain::redfish_path::RedfishPath;
/// let path: RedfishPath = "/redfish/v1/Systems/System_0".parse().unwrap();
/// assert_eq!(path.as_str(), "/redfish/v1/Systems/System_0");
/// assert!("/redfish/v1/%2e%2e/other".parse::<RedfishPath>().is_err());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RedfishPath(Arc<str>);

impl RedfishPath {
    /// Borrow the validated path at a transport boundary.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for RedfishPath {
    type Err = crate::utilities::error::RmsError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !(value == "/redfish/v1" || value.starts_with("/redfish/v1/"))
            || value.contains("//")
            || value
                .split('/')
                .any(|segment| matches!(segment, "." | ".."))
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'_' | b'-' | b'.'))
        {
            return Err(crate::utilities::error::RmsError::invalid_argument(
                "expected a plain same-origin /redfish/v1 path without escapes, whitespace, query, fragment, or traversal",
            ));
        }
        Ok(Self(value.into()))
    }
}

impl Deref for RedfishPath {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RedfishPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_escaped_and_malformed_paths() {
        for path in [
            "/redfish/v1/%2e%2e/%2e%2e/other",
            "/redfish/v1/Chassis/..\\..\\other",
            "/redfish/v1/Chassis/Bad Path",
            "/redfish/v1/Chassis/../other",
            "/redfish/v1/Chassis/./other",
            "/redfish/v1//Systems",
            "/redfish/v1/Systems?x=1",
            "/redfish/v1/Systems#x",
            "https://other/redfish/v1",
            "/redfish/v10/Systems",
        ] {
            assert!(path.parse::<RedfishPath>().is_err(), "{path:?}");
        }
    }
}
