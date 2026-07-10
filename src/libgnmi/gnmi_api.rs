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
