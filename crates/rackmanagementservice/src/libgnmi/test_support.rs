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
