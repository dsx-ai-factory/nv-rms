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
