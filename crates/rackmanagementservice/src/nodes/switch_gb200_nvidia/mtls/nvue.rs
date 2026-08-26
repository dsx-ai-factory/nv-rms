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

//! NVUE mTLS certificate and service configuration queries for NVIDIA GB200 switches.

use super::super::SwitchGb200Nvidia;
use super::super::validation::is_valid_identifier;
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{Result, RmsError};

use serde_json::Value;

impl SwitchGb200Nvidia {
    #[cfg(test)]
    pub(crate) fn mtls_skip_nvue_for_test(&self) -> bool {
        self.ssh_exec_for_test.is_some()
    }

    #[cfg(not(test))]
    pub(crate) fn mtls_skip_nvue_for_test(&self) -> bool {
        false
    }

    pub(crate) async fn get_switch_security_certificate_object(
        &self,
        certificate_id: &str,
    ) -> Result<Value> {
        if !is_valid_identifier(certificate_id) {
            return Err(RmsError::invalid_argument(format!(
                "invalid switch certificate id: {certificate_id}"
            )));
        }

        self.nvue_http_get(
            &format!("/nvue_v1/system/security/certificate/{certificate_id}"),
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn get_switch_security_ca_certificate_object(
        &self,
        ca_certificate_id: &str,
    ) -> Result<Value> {
        if !is_valid_identifier(ca_certificate_id) {
            return Err(RmsError::invalid_argument(format!(
                "invalid switch CA certificate id: {ca_certificate_id}"
            )));
        }

        self.nvue_http_get(
            &format!("/nvue_v1/system/security/ca-certificate/{ca_certificate_id}"),
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn get_gnmi_server_mtls_configuration(&self) -> Result<Value> {
        self.nvue_http_get(
            "/nvue_v1/system/gnmi-server/mtls",
            HttpClient::DEFAULT_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn get_nvue_api_mtls_configuration(&self) -> Result<Value> {
        self.nvue_http_get("/nvue_v1/system/api/mtls", HttpClient::DEFAULT_TIMEOUT)
            .await
    }
}
