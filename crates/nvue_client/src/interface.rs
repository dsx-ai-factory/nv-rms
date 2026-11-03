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

//! Types for the NVUE `/interface` API area.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// NVUE interface collection endpoint.
pub const INTERFACE_ENDPOINT: &str = "/nvue_v1/interface";

/// Explicit applied interface configuration from the NVUE root configuration.
pub const INTERFACE_APPLIED_CONFIG_ENDPOINT: &str = "/nvue_v1/?rev=applied&filled=false";

/// Explicit startup interface configuration from the NVUE root configuration.
pub const INTERFACE_STARTUP_CONFIG_ENDPOINT: &str = "/nvue_v1/?rev=startup&filled=false";

/// Effective applied interface configuration with schema defaults.
pub const INTERFACE_APPLIED_ENDPOINT: &str = "/nvue_v1/interface?rev=applied&filled=true";

/// Administrative interface link state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InterfaceLinkState {
    /// Link is administratively up.
    Up,

    /// Link is administratively down.
    Down,
}

impl InterfaceLinkState {
    /// Returns the NVUE state key.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

/// Complete interface collection returned by NVUE.
pub type InterfaceCollection = BTreeMap<String, InterfaceConfig>;

/// Interface configuration fields used by RMS.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct InterfaceConfig {
    /// Link configuration, when NVUE reports it.
    #[serde(default)]
    pub link: InterfaceLinkConfig,
}

/// Link configuration fields used by RMS.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct InterfaceLinkConfig {
    /// NVUE encodes state as an object keyed by `up` or `down`.
    #[serde(default)]
    pub state: Option<BTreeMap<String, InterfaceLinkStateMarker>>,
}

/// Empty object stored below an NVUE state key.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InterfaceLinkStateMarker {}

impl InterfaceLinkConfig {
    /// Returns DOWN when present, otherwise UP, or `None` for an absent or unknown state.
    /// This accessor does not reject contradictory keys; strict consumers validate the map.
    pub fn administrative_state(&self) -> Option<InterfaceLinkState> {
        let state = self.state.as_ref()?;

        if state.contains_key("down") {
            Some(InterfaceLinkState::Down)
        } else if state.contains_key("up") {
            Some(InterfaceLinkState::Up)
        } else {
            None
        }
    }
}

/// Builds one NVUE interface entry with an explicit administrative state.
/// Operational fields are omitted so the patch changes only administrative state.
pub fn interface_link_patch(state: InterfaceLinkState) -> InterfaceConfig {
    let state = Some(BTreeMap::from([(
        state.as_str().to_owned(),
        InterfaceLinkStateMarker {},
    )]));

    InterfaceConfig {
        link: InterfaceLinkConfig { state },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_patch_serializes_explicit_administrative_states() {
        let down = serde_json::to_value(interface_link_patch(InterfaceLinkState::Down)).unwrap();
        let up = serde_json::to_value(interface_link_patch(InterfaceLinkState::Up)).unwrap();

        assert_eq!(down, serde_json::json!({"link": {"state": {"down": {}}}}));
        assert_eq!(up, serde_json::json!({"link": {"state": {"up": {}}}}));
    }

    #[test]
    fn link_config_decodes_administrative_state() {
        let interfaces: InterfaceCollection = serde_json::from_value(serde_json::json!({
            "acp1": {"link": {"state": {"up": {}}, "physical-state": "PortConfigurationTraining"}},
            "fnm1": {"link": {"state": {"down": {}}}}
        }))
        .unwrap();

        assert_eq!(
            interfaces["acp1"].link.administrative_state(),
            Some(InterfaceLinkState::Up)
        );

        assert_eq!(
            interfaces["fnm1"].link.administrative_state(),
            Some(InterfaceLinkState::Down)
        );
    }
}
