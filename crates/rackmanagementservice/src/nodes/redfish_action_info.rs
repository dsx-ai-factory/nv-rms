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

//! Redfish action payloads and `ActionInfo` decoding shared by power shelf
//! drivers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::utilities::error::{Result, RmsError};

/// Redfish `ResetType` values RMS sends with `#Chassis.Reset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum ResetType {
    On,
    ForceOff,
    GracefulShutdown,
}

impl ResetType {
    /// Returns the Redfish name, which is also the serialized value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::On => "On",
            Self::ForceOff => "ForceOff",
            Self::GracefulShutdown => "GracefulShutdown",
        }
    }

    /// Whether this value appears in an advertised `AllowableValues` list.
    pub(crate) fn is_in(self, allowed: &[String]) -> bool {
        allowed.iter().any(|value| value == self.as_str())
    }
}

/// Request body for `#Chassis.Reset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct ChassisResetRequest {
    #[serde(rename = "ResetType")]
    pub(crate) reset_type: ResetType,
}

/// Request body for actions that take no parameters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub(crate) struct EmptyActionRequest {}

/// Encodes a typed action request as the JSON body `RedfishClient::post_action`
/// expects.
pub(crate) fn to_action_payload<T: Serialize>(request: &T) -> Result<Value> {
    serde_json::to_value(request).map_err(|err| {
        RmsError::internal(format!("failed to encode Redfish action payload: {err}"))
    })
}

/// A Redfish `ActionInfo` resource, read for the parameters an action accepts.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ActionInfoResource {
    #[serde(rename = "Parameters", default)]
    pub(crate) parameters: Vec<ActionParameter>,
}

/// One `ActionInfo` parameter: its name, default, and allowed values.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ActionParameter {
    #[serde(rename = "Name")]
    pub(crate) name: Option<String>,
    #[serde(rename = "DefaultValue")]
    pub(crate) default_value: Option<Value>,
    #[serde(rename = "AllowableValues")]
    pub(crate) allowable_values: Option<Vec<Value>>,
}

/// Returns the `ResetType` parameter's `AllowableValues` from a `#Chassis.Reset`
/// ActionInfo, or `None` if the parameter or its allowables aren't advertised.
pub(crate) fn reset_allowable_values(action_info: &ActionInfoResource) -> Option<Vec<String>> {
    let reset_param = action_info
        .parameters
        .iter()
        .find(|parameter| parameter.name.as_deref() == Some("ResetType"))?;
    let allowed: Vec<String> = reset_param
        .allowable_values
        .as_ref()?
        .iter()
        .filter_map(|value| value.as_str().map(str::to_owned))
        .collect();
    if allowed.is_empty() {
        None
    } else {
        Some(allowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: Value) -> ActionInfoResource {
        serde_json::from_value(value).expect("valid ActionInfo")
    }

    #[test]
    fn reset_type_serializes_as_its_redfish_name() {
        for reset_type in [
            ResetType::On,
            ResetType::ForceOff,
            ResetType::GracefulShutdown,
        ] {
            assert_eq!(
                serde_json::to_value(reset_type).unwrap(),
                Value::String(reset_type.as_str().to_owned())
            );
        }
    }

    #[test]
    fn reset_type_is_in_matches_exact_allowable_value() {
        let allowed = vec!["On".to_owned(), "ForceOff".to_owned()];
        assert!(ResetType::On.is_in(&allowed));
        assert!(ResetType::ForceOff.is_in(&allowed));
        assert!(!ResetType::GracefulShutdown.is_in(&allowed));
        assert!(!ResetType::On.is_in(&["on".to_owned()]));
    }

    #[test]
    fn action_payloads_encode_redfish_bodies() {
        assert_eq!(
            to_action_payload(&ChassisResetRequest {
                reset_type: ResetType::ForceOff
            })
            .unwrap(),
            serde_json::json!({ "ResetType": "ForceOff" })
        );
        assert_eq!(
            to_action_payload(&EmptyActionRequest {}).unwrap(),
            serde_json::json!({})
        );
    }

    #[test]
    fn reset_allowable_values_returns_advertised_reset_types() {
        let info = parse(serde_json::json!({
            "Parameters": [
                {
                    "Name": "ResetType",
                    "AllowableValues": ["On", "ForceOff", "GracefulShutdown"]
                }
            ]
        }));
        assert_eq!(
            reset_allowable_values(&info),
            Some(vec![
                "On".into(),
                "ForceOff".into(),
                "GracefulShutdown".into()
            ])
        );
    }

    #[test]
    fn reset_allowable_values_none_when_param_missing() {
        assert_eq!(reset_allowable_values(&parse(serde_json::json!({}))), None);
        assert_eq!(
            reset_allowable_values(&parse(serde_json::json!({ "Parameters": [] }))),
            None
        );
        let wrong_name = parse(serde_json::json!({
            "Parameters": [{ "Name": "OnType", "AllowableValues": ["On"] }]
        }));
        assert_eq!(reset_allowable_values(&wrong_name), None);
        let empty = parse(serde_json::json!({
            "Parameters": [{ "Name": "ResetType", "AllowableValues": [] }]
        }));
        assert_eq!(reset_allowable_values(&empty), None);
        let non_strings = parse(serde_json::json!({
            "Parameters": [{ "Name": "ResetType", "AllowableValues": [1, true, {}] }]
        }));
        assert_eq!(reset_allowable_values(&non_strings), None);
    }
}
