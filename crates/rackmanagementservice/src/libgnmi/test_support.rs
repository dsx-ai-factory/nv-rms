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

use std::collections::VecDeque;
use std::sync::Mutex;

use crate::libgnmi::{Gnmi, GnmiError, gnmi_model};

#[derive(Default)]
pub(crate) struct FakeGnmi {
    pub(crate) capabilities_results:
        Mutex<VecDeque<std::result::Result<gnmi_model::CapabilityResponse, GnmiError>>>,
    pub(crate) capabilities_calls: Mutex<u32>,
}

#[async_trait::async_trait]
impl Gnmi for FakeGnmi {
    async fn capabilities(
        &mut self,
    ) -> std::result::Result<gnmi_model::CapabilityResponse, GnmiError> {
        *self
            .capabilities_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner()) += 1;
        self.capabilities_results
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
            .unwrap_or_else(|| {
                Err(GnmiError::invalid_response(
                    "unexpected gNMI Capabilities call in fake client",
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_gnmi_returns_queued_capabilities_response() {
        let mut fake = FakeGnmi {
            capabilities_results: Mutex::new(VecDeque::from([Ok(
                gnmi_model::CapabilityResponse {},
            )])),
            ..FakeGnmi::default()
        };

        fake.capabilities().await.unwrap();
        assert_eq!(
            *fake.capabilities_calls.lock().unwrap(),
            1,
            "Capabilities call should be recorded"
        );
    }
}
