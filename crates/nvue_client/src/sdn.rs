/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
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
