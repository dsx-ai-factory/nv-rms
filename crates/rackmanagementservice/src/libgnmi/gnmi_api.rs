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

use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;

use crate::libgnmi::gnmi_model::{CapabilityRequest, g_nmi_client::GNmiClient};
use crate::libgnmi::{Gnmi, GnmiCredentials, GnmiError};

pub struct GnmiApi {
    client: GNmiClient<Channel>,
    credentials: GnmiCredentials,
}

impl GnmiApi {
    pub fn new(channel: Channel, credentials: GnmiCredentials) -> Self {
        Self {
            client: GNmiClient::new(channel),
            credentials,
        }
    }

    fn metadata_value(field: &'static str, value: &str) -> Result<MetadataValue<Ascii>, GnmiError> {
        MetadataValue::try_from(value).map_err(|err| GnmiError::InvalidMetadata {
            field,
            message: err.to_string(),
        })
    }

    fn capabilities_request(&self) -> Result<tonic::Request<CapabilityRequest>, GnmiError> {
        let mut request = tonic::Request::new(CapabilityRequest {});
        request.metadata_mut().insert(
            "username",
            Self::metadata_value("username", &self.credentials.username)?,
        );
        request.metadata_mut().insert(
            "password",
            Self::metadata_value("password", &self.credentials.password)?,
        );
        Ok(request)
    }
}

#[async_trait::async_trait]
impl Gnmi for GnmiApi {
    async fn capabilities(
        &mut self,
    ) -> Result<crate::libgnmi::gnmi_model::CapabilityResponse, GnmiError> {
        let request = self.capabilities_request()?;
        Ok(self.client.capabilities(request).await?.into_inner())
    }
}
