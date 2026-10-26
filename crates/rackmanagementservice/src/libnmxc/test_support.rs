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

use crate::libnmxc::{Nmxc, NmxcError, nmxc_model};

#[derive(Default)]
pub(crate) struct FakeNmxc {
    /// Scripted Hello results consumed in order.
    pub(crate) hello_results:
        VecDeque<std::result::Result<nmxc_model::NmxHelloResponse, NmxcError>>,

    /// Number of Hello calls made through this fake.
    pub(crate) hello_calls: usize,

    pub(crate) get_static_config_results:
        VecDeque<std::result::Result<nmxc_model::NmxGetStaticConfigResponse, NmxcError>>,
    pub(crate) get_static_config_file_calls: Vec<Vec<String>>,
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

    /// Returns the default successful Hello response.
    pub(crate) fn hello_response() -> nmxc_model::NmxHelloResponse {
        nmxc_model::NmxHelloResponse {
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
                config_file_contents: None,
            }),
            config_key_vals: None,
        }
    }

    pub(crate) fn static_config_files_response(
        files: &[(&str, &str)],
    ) -> nmxc_model::NmxGetStaticConfigResponse {
        nmxc_model::NmxGetStaticConfigResponse {
            server_header: Some(Self::success_header()),
            static_config: Some(nmxc_model::NmxStaticConfig {
                context: None,
                config_key_vals: None,
                config_file_contents: Some(nmxc_model::NmxConfigFileContents {
                    config_file_content: files
                        .iter()
                        .map(|(name, content)| nmxc_model::NmxConfigFileContent {
                            config_file_name: (*name).to_owned(),
                            config_file_content: (*content).to_owned(),
                        })
                        .collect(),
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
        self.hello_calls += 1;

        self.hello_results
            .pop_front()
            .unwrap_or_else(|| Ok(Self::hello_response()))
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

    async fn get_static_config_files(
        &mut self,
        config_file_names: &[&str],
        _gateway_id: &str,
    ) -> std::result::Result<nmxc_model::NmxGetStaticConfigResponse, NmxcError> {
        self.get_static_config_file_calls.push(
            config_file_names
                .iter()
                .map(|config_file_name| (*config_file_name).to_owned())
                .collect(),
        );

        self.get_static_config_results
            .pop_front()
            .unwrap_or_else(|| {
                Err(NmxcError::invalid_response(
                    "unexpected get_static_config_files call in fake NMX-C client",
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
