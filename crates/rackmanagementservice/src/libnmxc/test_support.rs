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

/// Test responses for one switch tray and the supplied GPU devices.
pub(crate) struct FabricResponses {
    /// Configured control plane with healthy controller status.
    pub(crate) domain: nmxc_model::NmxDomainStateInfo,

    /// ASIC and GPU topology, preserving caller-supplied device identifiers and ports.
    pub(crate) topology: nmxc_model::NmxFmTopologyInfo,

    /// Switch tray health and ASIC count.
    pub(crate) switches: nmxc_model::NmxGetSwitchNodeInfoListResponse,

    /// One compute row per supplied GPU device.
    pub(crate) computes: nmxc_model::NmxGetComputeNodeInfoListResponse,
}

#[derive(Default)]
pub(crate) struct FakeNmxc {
    /// Scripted Hello results consumed in order.
    pub(crate) hello_results:
        VecDeque<std::result::Result<nmxc_model::NmxHelloResponse, NmxcError>>,

    /// gateway_id argument recorded for each Hello call made through this fake.
    pub(crate) hello_calls: Vec<String>,

    pub(crate) get_static_config_results:
        VecDeque<std::result::Result<nmxc_model::NmxGetStaticConfigResponse, NmxcError>>,
    pub(crate) get_static_config_file_calls: Vec<Vec<String>>,
    pub(crate) set_results: VecDeque<std::result::Result<nmxc_model::NmxReturnCode, NmxcError>>,
    pub(crate) set_calls: Vec<(String, String, String, String)>,
    pub(crate) set_file_calls: Vec<(String, String, String)>,
    pub(crate) domain_state_results: VecDeque<Result<nmxc_model::NmxDomainStateInfo, NmxcError>>,
    pub(crate) topology_results: VecDeque<Result<nmxc_model::NmxFmTopologyInfo, NmxcError>>,
    pub(crate) switch_node_results:
        VecDeque<Result<nmxc_model::NmxGetSwitchNodeInfoListResponse, NmxcError>>,
    pub(crate) compute_node_results:
        VecDeque<Result<nmxc_model::NmxGetComputeNodeInfoListResponse, NmxcError>>,
    pub(crate) domain_state_calls: Vec<String>,
    pub(crate) topology_calls: Vec<String>,
    pub(crate) switch_node_calls: Vec<String>,
    pub(crate) compute_node_calls: Vec<String>,
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
    async fn get_domain_state_info(
        &mut self,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxDomainStateInfo, NmxcError> {
        self.domain_state_calls.push(gateway_id.to_owned());

        self.domain_state_results.pop_front().unwrap_or_else(|| {
            Err(NmxcError::Status(tonic::Status::unimplemented(
                "GetDomainStateInfo was not scripted",
            )))
        })
    }

    async fn get_topology_info(
        &mut self,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxFmTopologyInfo, NmxcError> {
        self.topology_calls.push(gateway_id.to_owned());

        self.topology_results.pop_front().unwrap_or_else(|| {
            Err(NmxcError::Status(tonic::Status::unimplemented(
                "GetTopologyInfo was not scripted",
            )))
        })
    }

    async fn get_switch_node_info_list(
        &mut self,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxGetSwitchNodeInfoListResponse, NmxcError> {
        self.switch_node_calls.push(gateway_id.to_owned());

        self.switch_node_results.pop_front().unwrap_or_else(|| {
            Err(NmxcError::Status(tonic::Status::unimplemented(
                "GetSwitchNodeInfoList was not scripted",
            )))
        })
    }

    async fn get_compute_node_info_list(
        &mut self,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxGetComputeNodeInfoListResponse, NmxcError> {
        self.compute_node_calls.push(gateway_id.to_owned());

        self.compute_node_results.pop_front().unwrap_or_else(|| {
            Err(NmxcError::Status(tonic::Status::unimplemented(
                "GetComputeNodeInfoList was not scripted",
            )))
        })
    }

    async fn hello(
        &mut self,
        gateway_id: &str,
    ) -> std::result::Result<nmxc_model::NmxHelloResponse, NmxcError> {
        self.hello_calls.push(gateway_id.to_owned());

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

    async fn set_static_config_file(
        &mut self,
        config_file_name: &str,
        content: &str,
        gateway_id: &str,
    ) -> Result<nmxc_model::NmxReturnCode, NmxcError> {
        self.set_file_calls.push((
            config_file_name.to_owned(),
            content.to_owned(),
            gateway_id.to_owned(),
        ));

        self.set_results.pop_front().unwrap_or_else(|| {
            Err(NmxcError::invalid_response(
                "unexpected set_static_config_file call in fake NMX-C client",
            ))
        })
    }
}

/// Builds test responses with one enabled, active port per switch ASIC.
/// Uses the supplied GPU ports and response header unchanged.
pub(crate) fn fabric_responses(
    location: nmxc_model::NmxLocationInfo,
    switch_ids: &[u64],
    switch_health: i32,
    gpus: Vec<nmxc_model::NmxGpuTopoInfo>,
    header: Option<nmxc_model::NmxServerHeader>,
) -> FabricResponses {
    let switches = switch_ids
        .iter()
        .map(|device_uid| nmxc_model::NmxDeviceTopoInfo {
            device: Some(nmxc_model::nmx_device_topo_info::Device::SwitchTopoInfo(
                nmxc_model::NmxSwitchTopoInfo {
                    loc: Some(location.clone()),
                    device_uid: *device_uid,
                    device_health: switch_health,
                    port_topo_info: vec![nmxc_model::NmxPortTopoInfo {
                        port_type: 2,
                        physical_state: 5,
                        logical_state: 4,
                        port_admin_state: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
        });

    let compute_rows = gpus
        .iter()
        .map(|gpu| nmxc_model::NmxComputeNodeInfo {
            loc: gpu.loc.clone(),
            node_health: gpu.device_health,
            num_gpus: 1,
        })
        .collect();

    let devices = switches
        .chain(gpus.into_iter().map(|gpu| nmxc_model::NmxDeviceTopoInfo {
            device: Some(nmxc_model::nmx_device_topo_info::Device::GpuTopoInfo(gpu)),
        }))
        .collect();

    FabricResponses {
        domain: nmxc_model::NmxDomainStateInfo {
            server_header: header.clone(),
            control_plane_state: 3,
            nmx_controller_health: 1,
            ..Default::default()
        },
        topology: nmxc_model::NmxFmTopologyInfo {
            server_header: header.clone(),
            device_topo_info: devices,
            ..Default::default()
        },
        switches: nmxc_model::NmxGetSwitchNodeInfoListResponse {
            server_header: header.clone(),
            node_info_list: vec![nmxc_model::NmxSwitchNodeInfo {
                loc: Some(location),
                num_switches: switch_ids.len() as u32,
                node_health: switch_health,
            }],
            ..Default::default()
        },
        computes: nmxc_model::NmxGetComputeNodeInfoListResponse {
            server_header: header,
            node_info_list: compute_rows,
            ..Default::default()
        },
    }
}

/// Returns the first queued response for a test to edit without removing it.
/// Panics if the queue is empty or the first response is an error.
pub(crate) fn scripted_response_mut<T>(results: &mut VecDeque<Result<T, NmxcError>>) -> &mut T {
    results.front_mut().unwrap().as_mut().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unscripted_file_write_fails_and_records_arguments() {
        let mut client = FakeNmxc::default();

        let error = client
            .set_static_config_file("fm_config", "SETTING = value\n", "gateway")
            .await
            .unwrap_err();

        let NmxcError::Status(status) = error else {
            panic!("expected an unscripted-call status");
        };

        assert_eq!(status.code(), tonic::Code::Unknown);

        assert_eq!(
            status.message(),
            "unexpected set_static_config_file call in fake NMX-C client"
        );

        assert_eq!(
            client.set_file_calls,
            [(
                "fm_config".to_owned(),
                "SETTING = value\n".to_owned(),
                "gateway".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn file_writes_consume_shared_set_results_in_order() {
        let success = FakeNmxc::success_return_code();

        let mut client = FakeNmxc {
            set_results: VecDeque::from([
                Ok(success.clone()),
                Err(NmxcError::Status(tonic::Status::failed_precondition(
                    "scripted write failure",
                ))),
                Ok(success.clone()),
            ]),
            ..Default::default()
        };

        let legacy_result = client
            .set_static_config("fm_config", "SETTING", "old", "gateway")
            .await
            .unwrap();

        assert_eq!(legacy_result, success);

        let error = client
            .set_static_config_file("fm_config", "SETTING = first\n", "gateway")
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            NmxcError::Status(status)
                if status.code() == tonic::Code::FailedPrecondition
                    && status.message() == "scripted write failure"
        ));

        let file_result = client
            .set_static_config_file("sm_config", "SETTING = second\n", "gateway")
            .await
            .unwrap();

        assert_eq!(file_result, success);
        assert!(client.set_results.is_empty());

        assert_eq!(
            client.set_calls,
            [(
                "fm_config".to_owned(),
                "SETTING".to_owned(),
                "old".to_owned(),
                "gateway".to_owned()
            )]
        );

        assert_eq!(
            client.set_file_calls,
            [
                (
                    "fm_config".to_owned(),
                    "SETTING = first\n".to_owned(),
                    "gateway".to_owned()
                ),
                (
                    "sm_config".to_owned(),
                    "SETTING = second\n".to_owned(),
                    "gateway".to_owned()
                )
            ]
        );
    }

    #[tokio::test]
    async fn unscripted_observations_fail_without_fabricating_health() {
        let mut client = FakeNmxc::default();

        let errors = [
            client.get_domain_state_info("gateway").await.unwrap_err(),
            client.get_topology_info("gateway").await.unwrap_err(),
            client
                .get_switch_node_info_list("gateway")
                .await
                .unwrap_err(),
            client
                .get_compute_node_info_list("gateway")
                .await
                .unwrap_err(),
        ];

        for error in errors {
            assert!(
                matches!(error, NmxcError::Status(status) if status.code() == tonic::Code::Unimplemented)
            );
        }

        assert_eq!(client.domain_state_calls, ["gateway"]);
        assert_eq!(client.topology_calls, ["gateway"]);
        assert_eq!(client.switch_node_calls, ["gateway"]);
        assert_eq!(client.compute_node_calls, ["gateway"]);
    }

    #[tokio::test]
    async fn scripted_observations_preserve_independent_health_and_membership() {
        let domain = nmxc_model::NmxDomainStateInfo {
            server_header: Some(FakeNmxc::success_header()),
            control_plane_state: 3,
            nmx_controller_health: 3,
            ..Default::default()
        };

        let switch_nodes = nmxc_model::NmxGetSwitchNodeInfoListResponse {
            server_header: Some(FakeNmxc::success_header()),
            node_info_list: vec![nmxc_model::NmxSwitchNodeInfo {
                loc: Some(nmxc_model::NmxLocationInfo {
                    chassis_serial_number: "rack-A01".to_owned(),
                    tray_index: 0,
                    location: None,
                }),
                num_switches: 2,
                node_health: 0,
            }],
            ..Default::default()
        };

        let topology = nmxc_model::NmxFmTopologyInfo {
            server_header: Some(FakeNmxc::success_header()),
            ..Default::default()
        };

        let compute_nodes = nmxc_model::NmxGetComputeNodeInfoListResponse {
            server_header: Some(FakeNmxc::success_header()),
            ..Default::default()
        };

        let mut client = FakeNmxc {
            domain_state_results: VecDeque::from([Ok(domain.clone())]),
            topology_results: VecDeque::from([Ok(topology.clone())]),
            switch_node_results: VecDeque::from([Ok(switch_nodes.clone())]),
            compute_node_results: VecDeque::from([Ok(compute_nodes.clone())]),
            ..Default::default()
        };

        assert_eq!(
            client.get_domain_state_info("gateway").await.unwrap(),
            domain
        );

        assert_eq!(client.get_topology_info("gateway").await.unwrap(), topology);

        assert_eq!(
            client.get_switch_node_info_list("gateway").await.unwrap(),
            switch_nodes
        );

        assert_eq!(
            client.get_compute_node_info_list("gateway").await.unwrap(),
            compute_nodes
        );
    }
}
