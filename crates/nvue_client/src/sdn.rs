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

//! Types for the NVUE `/sdn` API area.

use crate::action::SimpleAction;

use serde::Serialize;

/// Endpoint for `POST /sdn/factory-default`.
pub const FACTORY_DEFAULT_ENDPOINT: &str = "/nvue_v1/sdn/factory-default";

/// Request body for `POST /sdn/factory-default` `@reset`.
#[derive(Debug, Clone, Serialize)]
pub struct SdnFactoryDefaultResetRequest {
    /// Reset action.
    #[serde(rename = "@reset")]
    pub reset: SimpleAction<SdnFactoryDefaultResetParameters>,
}

impl SdnFactoryDefaultResetRequest {
    /// Create an SDN factory-default reset request.
    pub fn new() -> Self {
        Self {
            reset: SimpleAction::start(SdnFactoryDefaultResetParameters { force: true }),
        }
    }
}

impl Default for SdnFactoryDefaultResetRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for SDN factory-default reset.
#[derive(Debug, Clone, Serialize)]
pub struct SdnFactoryDefaultResetParameters {
    /// Force reset even when SDN has existing configuration.
    pub force: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_sdn_factory_default_request_serializes_expected_action() -> serde_json::Result<()> {
        let payload = serde_json::to_string(&SdnFactoryDefaultResetRequest::new())?;

        assert_eq!(
            payload,
            r#"{"@reset":{"state":"start","parameters":{"force":true}}}"#
        );

        Ok(())
    }
}
