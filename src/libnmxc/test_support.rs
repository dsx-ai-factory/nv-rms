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

use crate::libnmxc::{Nmxc, NmxcError, nmxc_model};

#[derive(Default)]
pub(crate) struct FakeNmxc {
    pub(crate) get_static_config_results:
        VecDeque<std::result::Result<nmxc_model::NmxGetStaticConfigResponse, NmxcError>>,
    pub(crate) set_results: VecDeque<std::result::Result<nmxc_model::NmxReturnCode, NmxcError>>,
    pub(crate) set_calls: Vec<(String, String, String, String)>,
}

impl FakeNmxc {
    fn success_header() -> nmxc_model::NmxServerHeader {
        nmxc_model::NmxServerHeader {
            domain_uuid: String::new(),
            app_uuid: String::new(),
            app_ver: String::new(),
            return_code: nmxc_model::StReturnCode::NmxStSuccess as i32,
        }
    }

    pub(crate) fn success_return_code() -> nmxc_model::NmxReturnCode {
        nmxc_model::NmxReturnCode {
            server_header: Some(Self::success_header()),
        }
    }

    pub(crate) fn static_config_response(
        config_file_name: &str,
        key: &str,
        value: &str,
    ) -> nmxc_model::NmxGetStaticConfigResponse {
        nmxc_model::NmxGetStaticConfigResponse {
            server_header: Some(Self::success_header()),
            static_config: Some(nmxc_model::NmxStaticConfig {
                context: None,
                config_key_vals: Some(nmxc_model::NmxConfigKeyVals {
                    config_key_val: vec![nmxc_model::NmxConfigKeyVal {
                        config_file_name: config_file_name.to_owned(),
                        key: key.to_owned(),
                        value: value.to_owned(),
                    }],
                }),
            }),
            config_key_vals: None,
        }
    }

    pub(crate) fn empty_static_config_response() -> nmxc_model::NmxGetStaticConfigResponse {
        nmxc_model::NmxGetStaticConfigResponse {
            server_header: Some(Self::success_header()),
            static_config: None,
            config_key_vals: None,
        }
    }
}

#[async_trait::async_trait]
impl Nmxc for FakeNmxc {
    async fn hello(
        &mut self,
        _gateway_id: &str,
    ) -> std::result::Result<nmxc_model::NmxHelloResponse, NmxcError> {
        Ok(nmxc_model::NmxHelloResponse {
            server_header: Some(nmxc_model::NmxServerHeader {
                domain_uuid: String::new(),
                app_uuid: String::new(),
                app_ver: String::new(),
                return_code: nmxc_model::StReturnCode::NmxStSuccess as i32,
            }),
        })
    }

    async fn get_static_config(
        &mut self,
        _config_file_name: &str,
        _key: &str,
        _gateway_id: &str,
    ) -> std::result::Result<nmxc_model::NmxGetStaticConfigResponse, NmxcError> {
        self.get_static_config_results
            .pop_front()
            .unwrap_or_else(|| {
                Err(NmxcError::invalid_response(
                    "unexpected get_static_config call in fake NMX-C client",
                ))
            })
    }

    async fn set_static_config(
        &mut self,
        config_file_name: &str,
        key: &str,
        value: &str,
        gateway_id: &str,
    ) -> std::result::Result<nmxc_model::NmxReturnCode, NmxcError> {
        self.set_calls.push((
            config_file_name.to_owned(),
            key.to_owned(),
            value.to_owned(),
            gateway_id.to_owned(),
        ));
        self.set_results.pop_front().unwrap_or_else(|| {
            Err(NmxcError::invalid_response(
                "unexpected set_static_config call in fake NMX-C client",
            ))
        })
    }
}
