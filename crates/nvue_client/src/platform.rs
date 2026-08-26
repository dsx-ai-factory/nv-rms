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

//! Types and endpoint helpers for the NVUE `/platform` API area.

use std::collections::BTreeMap;

use crate::NVUE_V1_SERVER;
use crate::action::SimpleAction;
use crate::uri::path_segment;

use serde::{Deserialize, Serialize};

/// Full endpoint for `GET /platform`.
pub const PLATFORM_ENDPOINT: &str = "/nvue_v1/platform";
/// Full endpoint for `GET /platform/chassis-location`.
pub const CHASSIS_LOCATION_ENDPOINT: &str = "/nvue_v1/platform/chassis-location";
/// Full endpoint for `GET /platform/firmware`.
pub const FIRMWARE_ENDPOINT: &str = "/nvue_v1/platform/firmware";

/// Compatibility endpoint observed on switch firmware workflows.
///
/// NVOS API 25.02.4300 does not expose this as a `POST` operation. Keep it
/// separate from OpenAPI-derived operations so callers can treat it as an
/// observed compatibility path.
pub const COMPAT_FIRMWARE_FETCH_ENDPOINT: &str = FIRMWARE_ENDPOINT;

/// Builds the endpoint for `/platform/firmware/{platform-component-id}`.
pub fn firmware_component_endpoint(platform_component_id: &str) -> String {
    format!(
        "{NVUE_V1_SERVER}/platform/firmware/{}",
        path_segment(platform_component_id)
    )
}

/// Builds the endpoint for `/platform/firmware/{platform-component-id}/files`.
pub fn firmware_component_files_endpoint(platform_component_id: &str) -> String {
    format!(
        "{NVUE_V1_SERVER}/platform/firmware/{}/files",
        path_segment(platform_component_id)
    )
}

/// Builds the endpoint for `/platform/firmware/{platform-component-id}/files/{file-name}`.
pub fn firmware_component_file_endpoint(platform_component_id: &str, file_name: &str) -> String {
    format!(
        "{NVUE_V1_SERVER}/platform/firmware/{}/files/{}",
        path_segment(platform_component_id),
        path_segment(file_name)
    )
}

/// Builds the observed compatibility endpoint `/platform/firmware/files/{file-name}`.
///
/// NVOS API 25.02.4300 does not expose this path; nvfwupd currently uses it
/// for VRNVL72 parallel switch updates.
pub fn compat_firmware_file_endpoint(file_name: &str) -> String {
    format!("{FIRMWARE_ENDPOINT}/files/{}", path_segment(file_name))
}

/// Platform identity fields used by nvfwupd.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Platform {
    /// Product name.
    #[serde(default, rename = "product-name")]
    pub product_name: Option<String>,

    /// System type fallback used by VR systems.
    #[serde(default, rename = "system-type")]
    pub system_type: Option<String>,

    /// Part number.
    #[serde(default, rename = "part-number")]
    pub part_number: Option<String>,

    /// Serial number.
    #[serde(default, rename = "serial-number")]
    pub serial_number: Option<String>,
}

impl Platform {
    /// Return the product name, falling back to `system-type`.
    pub fn model(&self) -> Option<&str> {
        self.product_name
            .as_deref()
            .and_then(meaningful_platform_string)
            .or_else(|| {
                self.system_type
                    .as_deref()
                    .and_then(meaningful_platform_string)
            })
    }
}

fn meaningful_platform_string(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() || value == "N/A" {
        None
    } else {
        Some(value)
    }
}

/// Chassis location fields used by RMS inventory.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChassisLocation {
    /// Chassis serial number.
    #[serde(default, rename = "chassis-sn")]
    pub chassis_sn: Option<String>,

    /// Slot number.
    #[serde(default, rename = "slot-number")]
    pub slot_number: Option<String>,

    /// Topology ID.
    #[serde(default, rename = "topology-id")]
    pub topology_id: Option<String>,

    /// Tray index.
    #[serde(default, rename = "tray-index")]
    pub tray_index: Option<String>,
}

/// Platform firmware inventory collection.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct PlatformFirmware {
    /// Firmware components keyed by platform component ID.
    pub components: BTreeMap<String, PlatformFirmwareComponent>,
}

impl PlatformFirmware {
    /// Return an iterator over component IDs and component data.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &PlatformFirmwareComponent)> {
        self.components.iter()
    }
}

/// Platform firmware component data used by RMS and nvfwupd inventory.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlatformFirmwareComponent {
    /// Actual firmware version.
    #[serde(default, rename = "actual-firmware")]
    pub actual_firmware: Option<String>,

    /// Lowercase version field.
    #[serde(default)]
    pub version: Option<String>,

    /// Redfish-style uppercase version field observed by wrappers.
    #[serde(default, rename = "Version")]
    pub display_version: Option<String>,

    /// Nested status object.
    #[serde(default, rename = "Status")]
    pub status: Option<PlatformFirmwareStatus>,

    /// Flat health field.
    #[serde(default, rename = "Health")]
    pub health: Option<String>,
}

impl PlatformFirmwareComponent {
    /// Return the best available version string.
    pub fn version(&self) -> Option<&str> {
        self.actual_firmware
            .as_deref()
            .or(self.version.as_deref())
            .or(self.display_version.as_deref())
    }

    /// Return the best available health string.
    pub fn health(&self) -> Option<&str> {
        self.status
            .as_ref()
            .and_then(|status| status.health.as_deref())
            .or(self.health.as_deref())
    }
}

/// Platform firmware health status.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlatformFirmwareStatus {
    /// Health field.
    #[serde(default, rename = "Health")]
    pub health: Option<String>,
}

/// Request body for `POST /platform/firmware/{component}/files/{file-name}` install.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlatformFirmwareInstallRequest {
    /// Install action.
    #[serde(rename = "@install")]
    pub install: SimpleAction<PlatformFirmwareInstallParameters>,
}

impl PlatformFirmwareInstallRequest {
    /// Create a platform firmware install request.
    pub fn new(force: bool) -> Self {
        Self {
            install: SimpleAction::start(PlatformFirmwareInstallParameters {
                force: Some(force),
                ..Default::default()
            }),
        }
    }
}

/// Platform firmware install parameters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlatformFirmwareInstallParameters {
    /// Force the install action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,

    /// Skip reboot behavior.
    #[serde(
        default,
        rename = "skip-reboot",
        skip_serializing_if = "Option::is_none"
    )]
    pub skip_reboot: Option<bool>,

    /// Skip version check behavior.
    #[serde(
        default,
        rename = "skip-version-check",
        skip_serializing_if = "Option::is_none"
    )]
    pub skip_version_check: Option<bool>,
}

/// Compatibility request body for observed `POST /platform/firmware` fetch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlatformFirmwareFetchRequest {
    /// Fetch action.
    #[serde(rename = "@fetch")]
    pub fetch: SimpleAction<PlatformFirmwareFetchParameters>,
}

impl PlatformFirmwareFetchRequest {
    /// Create a platform firmware fetch request.
    pub fn new(remote_url: impl Into<String>) -> Self {
        Self {
            fetch: SimpleAction::start(PlatformFirmwareFetchParameters {
                remote_url: remote_url.into(),
            }),
        }
    }
}

/// Platform firmware fetch parameters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlatformFirmwareFetchParameters {
    /// Remote URL used by compatibility fetch workflows.
    #[serde(rename = "remote-url")]
    pub remote_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_use_nvue_v1_server() {
        assert_eq!(PLATFORM_ENDPOINT, "/nvue_v1/platform");
        assert_eq!(
            CHASSIS_LOCATION_ENDPOINT,
            "/nvue_v1/platform/chassis-location"
        );
        assert_eq!(FIRMWARE_ENDPOINT, "/nvue_v1/platform/firmware");
        assert_eq!(
            firmware_component_endpoint("BMC"),
            "/nvue_v1/platform/firmware/BMC"
        );
        assert_eq!(
            firmware_component_files_endpoint("BMC"),
            "/nvue_v1/platform/firmware/BMC/files"
        );
        assert_eq!(
            firmware_component_file_endpoint("BMC", "switch_bundle.fwpkg"),
            "/nvue_v1/platform/firmware/BMC/files/switch_bundle.fwpkg"
        );
        assert_eq!(
            compat_firmware_file_endpoint("switch_bundle.fwpkg"),
            "/nvue_v1/platform/firmware/files/switch_bundle.fwpkg"
        );
        assert_eq!(
            firmware_component_file_endpoint("BMC/1", "switch bundle.fwpkg"),
            "/nvue_v1/platform/firmware/BMC%2F1/files/switch%20bundle.fwpkg"
        );
    }

    #[test]
    fn platform_model_falls_back_to_system_type() {
        struct TestCase {
            name: &'static str,
            input: &'static str,
            expected: Option<&'static str>,
        }

        let cases = [
            TestCase {
                name: "missing product",
                input: r#"{"system-type":"VRNVL72"}"#,
                expected: Some("VRNVL72"),
            },
            TestCase {
                name: "n/a product",
                input: r#"{"product-name":"N/A","system-type":"VRNVL72"}"#,
                expected: Some("VRNVL72"),
            },
            TestCase {
                name: "product wins",
                input: r#"{"product-name":"DGX","system-type":"VRNVL72"}"#,
                expected: Some("DGX"),
            },
            TestCase {
                name: "empty values",
                input: r#"{"product-name":" ","system-type":""}"#,
                expected: None,
            },
        ];

        for case in cases {
            let platform: Platform = serde_json::from_str(case.input).unwrap();
            assert_eq!(platform.model(), case.expected, "{}", case.name);
        }
    }

    #[test]
    fn firmware_inventory_reads_version_and_health_shapes() {
        let inventory: PlatformFirmware = serde_json::from_str(
            r#"{
                "BMC": {
                    "actual-firmware": "1.0",
                    "Status": {"Health": "OK"}
                },
                "CPLD1": {
                    "Version": "2.0",
                    "Health": "Warning"
                }
            }"#,
        )
        .unwrap();

        assert_eq!(inventory.components["BMC"].version(), Some("1.0"));
        assert_eq!(inventory.components["BMC"].health(), Some("OK"));
        assert_eq!(inventory.components["CPLD1"].version(), Some("2.0"));
        assert_eq!(inventory.components["CPLD1"].health(), Some("Warning"));
    }

    #[test]
    fn install_and_fetch_payloads_use_action_keys() {
        let install = serde_json::to_string(&PlatformFirmwareInstallRequest::new(false)).unwrap();
        let fetch =
            serde_json::to_string(&PlatformFirmwareFetchRequest::new("file:///tmp/a.fwpkg"))
                .unwrap();

        assert!(install.contains(r#""@install""#));
        assert!(install.contains(r#""force":false"#));
        assert!(fetch.contains(r#""@fetch""#));
        assert!(fetch.contains(r#""remote-url":"file:///tmp/a.fwpkg""#));
    }
}
