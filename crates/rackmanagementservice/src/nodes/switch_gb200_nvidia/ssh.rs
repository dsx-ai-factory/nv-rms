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

//! SSH endpoint construction for NVIDIA GB200 switch operations.

use secrecy::ExposeSecret;

use super::SwitchGb200Nvidia;
use crate::transport::ssh_client::SshEndpoint;
use crate::utilities::error::Result;

impl SwitchGb200Nvidia {
    pub(crate) fn ssh_endpoint(&self) -> Result<SshEndpoint> {
        let host_endpoint = self.require_host_endpoint()?;
        let credentials = self.require_host_credentials()?;

        Ok(SshEndpoint::new(
            &host_endpoint.ip_address,
            &credentials.username,
            credentials.password.expose_secret(),
        ))
    }
}
