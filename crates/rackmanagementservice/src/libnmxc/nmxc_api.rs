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

use std::time::Duration;

use prost::Message;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::Channel;

use crate::libnmxc::response::check_server_header_success;
use crate::libnmxc::{Nmxc, NmxcError, nmxc_model};

const HELLO_PATH: &str = "/nmx_c.NMX_Controller/Hello";
const GET_STATIC_CONFIG_PATH: &str = "/nmx_c.NMX_Controller/GetStaticConfig";
const SET_STATIC_CONFIG_PATH: &str = "/nmx_c.NMX_Controller/SetStaticConfig";

pub struct NmxcApi {
    channel: Channel,
    /// Per-RPC deadline. Bounds `ready()` + the unary call so a server that
    /// accepts the connection but then hangs can't block a call indefinitely.
    timeout: Duration,
}

impl NmxcApi {
    pub fn new(channel: Channel, timeout: Duration) -> Self {
        Self { channel, timeout }
    }

    async fn unary<Req, Resp>(
        &self,
        path: &'static str,
        req: Req,
    ) -> Result<tonic::Response<Resp>, NmxcError>
    where
        Req: Message + Default + Send + 'static,
        Resp: Message + Default + Send + 'static,
    {
        let codec = tonic_prost::ProstCodec::<Req, Resp>::default();
        let path_and_query = PathAndQuery::from_static(path);
        let mut grpc_client = tonic::client::Grpc::new(self.channel.clone());
        let call = async {
            grpc_client.ready().await?;
            let resp = grpc_client
                .unary(tonic::Request::new(req), path_and_query, codec)
                .await?;
            Ok::<_, NmxcError>(resp)
        };

        tokio::time::timeout(self.timeout, call)
            .await
            .map_err(|_| {
                tracing::warn!(
                    path,
                    timeout_secs = self.timeout.as_secs(),
                    "NMX-C RPC timed out"
                );
                NmxcError::Status(tonic::Status::deadline_exceeded(format!(
                    "NMX-C RPC {path} timed out after {} seconds",
                    self.timeout.as_secs()
                )))
            })?
    }
}

#[async_trait::async_trait]
impl Nmxc for NmxcApi {
    async fn hello(&mut self, gateway_id: &str) -> Result<nmxc_model::NmxHelloResponse, NmxcError> {
        let req = nmxc_model::NmxHelloRequest {
            gateway_id: gateway_id.to_owned(),
            major_version: nmxc_model::ProtoMsgMajorVersion::ProtoMsgMajorVersion as i32,
            minor_version: nmxc_model::ProtoMsgMinorVersion::ProtoMsgMinorVersion as i32,
        };
        let response: nmxc_model::NmxHelloResponse =
            self.unary(HELLO_PATH, req).await?.into_inner();
        check_server_header_success(response.server_header.as_ref(), "Hello")?;
        Ok(response)
    }

    async fn get_static_config(
        &mut self,
        config_file_name: &str,
        key: &str,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxGetStaticConfigResponse, NmxcError> {
        let req = nmxc_model::NmxGetStaticConfigRequest {
            context: None,
            config_keys: Some(nmxc_model::NmxConfigKeys {
                config_key: vec![nmxc_model::NmxConfigKey {
                    config_file_name: config_file_name.to_owned(),
                    key: key.to_owned(),
                }],
            }),
            config_files: None,
            gateway_id: gateway_id.to_owned(),
        };

        let response: nmxc_model::NmxGetStaticConfigResponse =
            self.unary(GET_STATIC_CONFIG_PATH, req).await?.into_inner();

        check_server_header_success(response.server_header.as_ref(), "GetStaticConfig")?;
        Ok(response)
    }

    async fn get_static_config_files(
        &mut self,
        config_file_names: &[&str],
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxGetStaticConfigResponse, NmxcError> {
        let req = nmxc_model::NmxGetStaticConfigRequest {
            context: None,
            config_keys: None,
            config_files: Some(nmxc_model::NmxConfigFiles {
                config_file: config_file_names
                    .iter()
                    .map(|config_file_name| nmxc_model::NmxConfigFile {
                        config_file_name: (*config_file_name).to_owned(),
                    })
                    .collect(),
            }),
            gateway_id: gateway_id.to_owned(),
        };
        let response: nmxc_model::NmxGetStaticConfigResponse =
            self.unary(GET_STATIC_CONFIG_PATH, req).await?.into_inner();
        check_server_header_success(response.server_header.as_ref(), "GetStaticConfig")?;
        Ok(response)
    }

    async fn set_static_config(
        &mut self,
        config_file_name: &str,
        key: &str,
        value: &str,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxReturnCode, NmxcError> {
        let req = nmxc_model::NmxSetStaticConfigRequest {
            gateway_id: gateway_id.to_owned(),
            static_config: Some(nmxc_model::NmxStaticConfig {
                context: Some(nmxc_model::NmxContext {
                    context: String::new(),
                }),
                config_key_vals: Some(nmxc_model::NmxConfigKeyVals {
                    config_key_val: vec![nmxc_model::NmxConfigKeyVal {
                        config_file_name: config_file_name.to_owned(),
                        key: key.to_owned(),
                        value: value.to_owned(),
                    }],
                }),
                config_file_contents: None,
            }),
        };
        let response: nmxc_model::NmxReturnCode =
            self.unary(SET_STATIC_CONFIG_PATH, req).await?.into_inner();
        check_server_header_success(response.server_header.as_ref(), "SetStaticConfig")?;
        Ok(response)
    }
}
