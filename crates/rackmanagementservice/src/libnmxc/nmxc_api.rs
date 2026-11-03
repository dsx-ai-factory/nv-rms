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

use std::time::Duration;

use crate::libnmxc::response::check_server_header_success;
use crate::libnmxc::{Nmxc, NmxcError, nmxc_model};

use prost::Message;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::Channel;

const HELLO_PATH: &str = "/nmx_c.NMX_Controller/Hello";
const GET_STATIC_CONFIG_PATH: &str = "/nmx_c.NMX_Controller/GetStaticConfig";
const SET_STATIC_CONFIG_PATH: &str = "/nmx_c.NMX_Controller/SetStaticConfig";
const GET_DOMAIN_STATE_INFO_PATH: &str = "/nmx_c.NMX_Controller/GetDomainStateInfo";
const GET_TOPOLOGY_INFO_PATH: &str = "/nmx_c.NMX_Controller/GetTopologyInfo";
const GET_SWITCH_NODE_INFO_LIST_PATH: &str = "/nmx_c.NMX_Controller/GetSwitchNodeInfoList";
const GET_COMPUTE_NODE_INFO_LIST_PATH: &str = "/nmx_c.NMX_Controller/GetComputeNodeInfoList";

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
    async fn get_domain_state_info(
        &mut self,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxDomainStateInfo, NmxcError> {
        let request = nmxc_model::NmxGetDomainStateInfoRequest {
            context: None,
            gateway_id: gateway_id.to_owned(),
        };

        let response: nmxc_model::NmxDomainStateInfo = self
            .unary(GET_DOMAIN_STATE_INFO_PATH, request)
            .await?
            .into_inner();

        check_server_header_success(response.server_header.as_ref(), "GetDomainStateInfo")?;
        Ok(response)
    }

    async fn get_topology_info(
        &mut self,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxFmTopologyInfo, NmxcError> {
        let request = nmxc_model::NmxGetTopologyInfoRequest {
            context: None,
            gateway_id: gateway_id.to_owned(),
        };

        let response: nmxc_model::NmxFmTopologyInfo = self
            .unary(GET_TOPOLOGY_INFO_PATH, request)
            .await?
            .into_inner();

        check_server_header_success(response.server_header.as_ref(), "GetTopologyInfo")?;
        Ok(response)
    }

    async fn get_switch_node_info_list(
        &mut self,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxGetSwitchNodeInfoListResponse, NmxcError> {
        let request = nmxc_model::NmxGetSwitchNodeInfoListRequest {
            context: None,
            loc_list: Vec::new(),
            gateway_id: gateway_id.to_owned(),
        };

        let response: nmxc_model::NmxGetSwitchNodeInfoListResponse = self
            .unary(GET_SWITCH_NODE_INFO_LIST_PATH, request)
            .await?
            .into_inner();

        check_server_header_success(response.server_header.as_ref(), "GetSwitchNodeInfoList")?;
        Ok(response)
    }

    async fn get_compute_node_info_list(
        &mut self,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxGetComputeNodeInfoListResponse, NmxcError> {
        let request = nmxc_model::NmxGetComputeNodeInfoListRequest {
            context: None,
            loc_list: Vec::new(),
            gateway_id: gateway_id.to_owned(),
        };

        let response: nmxc_model::NmxGetComputeNodeInfoListResponse = self
            .unary(GET_COMPUTE_NODE_INFO_LIST_PATH, request)
            .await?
            .into_inner();

        check_server_header_success(response.server_header.as_ref(), "GetComputeNodeInfoList")?;
        Ok(response)
    }

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

    async fn set_static_config_file(
        &mut self,
        config_file_name: &str,
        content: &str,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxReturnCode, NmxcError> {
        let request = nmxc_model::NmxSetStaticConfigRequest {
            gateway_id: gateway_id.to_owned(),
            static_config: Some(nmxc_model::NmxStaticConfig {
                context: Some(nmxc_model::NmxContext {
                    context: String::new(),
                }),
                config_key_vals: None,
                config_file_contents: Some(nmxc_model::NmxConfigFileContents {
                    config_file_content: vec![nmxc_model::NmxConfigFileContent {
                        config_file_name: config_file_name.to_owned(),
                        config_file_content: content.to_owned(),
                    }],
                }),
            }),
        };

        let response: nmxc_model::NmxReturnCode = self
            .unary(SET_STATIC_CONFIG_PATH, request)
            .await?
            .into_inner();

        check_server_header_success(response.server_header.as_ref(), "SetStaticConfig")?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    use tokio_stream::wrappers::TcpListenerStream;

    // Domain requests carry the gateway at tag 2; other observation requests
    // use tag 3. Static-config writes use tag 1 and a message payload at tag 2.
    #[derive(Clone, PartialEq, Message)]
    struct ObservationRequest {
        #[prost(string, tag = "1")]
        file_gateway: String,

        #[prost(bytes = "vec", tag = "2")]
        domain_gateway: Vec<u8>,
        #[prost(string, tag = "3")]
        gateway: String,
    }

    // This independent decoder verifies the file-content field's wire tag.
    #[derive(Clone, PartialEq, Message)]
    struct WrittenStaticConfig {
        #[prost(message, optional, tag = "1")]
        context: Option<nmxc_model::NmxContext>,

        #[prost(bytes = "vec", optional, tag = "2")]
        key_values: Option<Vec<u8>>,

        #[prost(message, optional, tag = "3")]
        file_contents: Option<nmxc_model::NmxConfigFileContents>,
    }

    #[derive(Clone)]
    struct ObservationService {
        header: Option<nmxc_model::NmxServerHeader>,
        calls: Arc<Mutex<Vec<(String, String)>>>,
        file_configs: Option<Arc<Mutex<Vec<WrittenStaticConfig>>>>,
    }

    impl tonic::server::UnaryService<ObservationRequest> for ObservationService {
        type Response = nmxc_model::NmxReturnCode;
        type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

        fn call(&mut self, request: tonic::Request<ObservationRequest>) -> Self::Future {
            let request = request.into_inner();

            let gateway = if let Some(file_configs) = &self.file_configs {
                let config = WrittenStaticConfig::decode(request.domain_gateway.as_slice())
                    .expect("decode observed static configuration");

                file_configs.lock().unwrap().push(config);
                request.file_gateway
            } else if request.domain_gateway.is_empty() {
                request.gateway
            } else {
                String::from_utf8(request.domain_gateway).unwrap()
            };

            self.calls.lock().unwrap().last_mut().unwrap().1 = gateway;

            let response = nmxc_model::NmxReturnCode {
                server_header: self.header.clone(),
            };

            Box::pin(async move { Ok(tonic::Response::new(response)) })
        }
    }

    impl<B> tonic::codegen::Service<http::Request<B>> for ObservationService
    where
        B: tonic::codegen::Body + Send + 'static,
        B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<B>) -> Self::Future {
            self.calls
                .lock()
                .unwrap()
                .push((request.uri().path().to_owned(), String::new()));

            let service = self.clone();

            Box::pin(async move {
                let codec = tonic_prost::ProstCodec::default();
                let mut grpc = tonic::server::Grpc::new(codec);

                Ok(grpc.unary(service, request).await)
            })
        }
    }

    impl tonic::server::NamedService for ObservationService {
        const NAME: &'static str = "nmx_c.NMX_Controller";
    }

    #[tokio::test]
    async fn observation_rpcs_use_official_paths_and_reject_unsuccessful_headers() {
        for return_code in [Some(1), Some(503), None] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let calls = Arc::new(Mutex::new(Vec::new()));

            let service = ObservationService {
                header: return_code.map(|return_code| nmxc_model::NmxServerHeader {
                    return_code,
                    ..Default::default()
                }),
                calls: calls.clone(),
                file_configs: None,
            };

            let cancellation = tokio_util::sync::CancellationToken::new();
            let shutdown = cancellation.clone();

            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        shutdown.cancelled(),
                    )
                    .await
                    .unwrap();
            });

            let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                .unwrap()
                .connect()
                .await
                .unwrap();

            let mut client = NmxcApi::new(channel, Duration::from_secs(2));

            let results = [
                client
                    .get_domain_state_info("gateway")
                    .await
                    .map(|response| {
                        assert_eq!(response.control_plane_state, 0);
                        assert_eq!(response.nmx_controller_health, 0);
                    }),
                client.get_topology_info("gateway").await.map(|response| {
                    assert!(response.device_topo_info.is_empty());
                }),
                client
                    .get_switch_node_info_list("gateway")
                    .await
                    .map(|response| {
                        assert!(response.node_info_list.is_empty());
                    }),
                client
                    .get_compute_node_info_list("gateway")
                    .await
                    .map(|response| {
                        assert!(response.node_info_list.is_empty());
                    }),
            ];

            for result in results {
                match return_code {
                    Some(1) => result.unwrap(),
                    Some(code) => assert_eq!(result.unwrap_err().nmx_return_code(), Some(code)),
                    None => assert!(matches!(result, Err(NmxcError::MissingServerHeader { .. }))),
                }
            }

            let expected = [
                "/nmx_c.NMX_Controller/GetDomainStateInfo",
                "/nmx_c.NMX_Controller/GetTopologyInfo",
                "/nmx_c.NMX_Controller/GetSwitchNodeInfoList",
                "/nmx_c.NMX_Controller/GetComputeNodeInfoList",
            ]
            .map(|path| (path.to_owned(), "gateway".to_owned()));

            assert_eq!(*calls.lock().unwrap(), expected);

            cancellation.cancel();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn set_static_config_file_preserves_wire_content_and_validates_acknowledgement() {
        for content in [
            "",
            "# Retained settings\r\nCUSTOM = keep\nCUSTOM_UNICODE = \u{03bb}\n",
        ] {
            for return_code in [Some(1), Some(3), None] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let calls = Arc::new(Mutex::new(Vec::new()));
                let file_configs = Arc::new(Mutex::new(Vec::new()));

                let header = return_code.map(|return_code| nmxc_model::NmxServerHeader {
                    domain_uuid: "domain".to_owned(),
                    app_uuid: "controller".to_owned(),
                    app_ver: "version".to_owned(),
                    return_code,
                });

                let service = ObservationService {
                    header: header.clone(),
                    calls: calls.clone(),
                    file_configs: Some(file_configs.clone()),
                };

                let cancellation = tokio_util::sync::CancellationToken::new();
                let _shutdown_guard = cancellation.clone().drop_guard();
                let shutdown = cancellation.clone();

                let server = tokio::spawn(async move {
                    tonic::transport::Server::builder()
                        .add_service(service)
                        .serve_with_incoming_shutdown(
                            TcpListenerStream::new(listener),
                            shutdown.cancelled(),
                        )
                        .await
                        .unwrap();
                });

                let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();

                let mut client = NmxcApi::new(channel, Duration::from_secs(2));

                let result = client
                    .set_static_config_file("fm_config", content, "file-gateway")
                    .await;

                match return_code {
                    Some(1) => assert_eq!(result.unwrap().server_header, header),
                    Some(code) => assert!(matches!(
                        result,
                        Err(NmxcError::NmxReturnCode {
                            return_code,
                            operation: "SetStaticConfig"
                        }) if return_code == code
                    )),
                    None => assert!(matches!(
                        result,
                        Err(NmxcError::MissingServerHeader {
                            operation: "SetStaticConfig"
                        })
                    )),
                }

                assert_eq!(
                    *calls.lock().unwrap(),
                    [(
                        "/nmx_c.NMX_Controller/SetStaticConfig".to_owned(),
                        "file-gateway".to_owned()
                    )]
                );

                assert_eq!(
                    *file_configs.lock().unwrap(),
                    [WrittenStaticConfig {
                        context: Some(nmxc_model::NmxContext {
                            context: String::new()
                        }),
                        key_values: None,
                        file_contents: Some(nmxc_model::NmxConfigFileContents {
                            config_file_content: vec![nmxc_model::NmxConfigFileContent {
                                config_file_name: "fm_config".to_owned(),
                                config_file_content: content.to_owned()
                            }]
                        })
                    }]
                );

                cancellation.cancel();
                server.await.unwrap();
            }
        }
    }
}
