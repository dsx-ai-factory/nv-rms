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

use crate::libnmxc::{NmxcError, nmxc_model};

pub(crate) fn check_server_header_success(
    header: Option<&nmxc_model::NmxServerHeader>,
    operation: &'static str,
) -> Result<(), NmxcError> {
    let Some(header) = header else {
        return Err(NmxcError::MissingServerHeader { operation });
    };
    if header.return_code == nmxc_model::StReturnCode::NmxStSuccess as i32 {
        Ok(())
    } else {
        Err(NmxcError::NmxReturnCode {
            return_code: header.return_code,
            operation,
        })
    }
}

pub fn find_static_config_value(
    response: &nmxc_model::NmxGetStaticConfigResponse,
    config_file_name: &str,
    key: &str,
) -> Option<String> {
    let nested_value = response
        .static_config
        .as_ref()
        .and_then(|static_config| static_config.config_key_vals.as_ref())
        .and_then(|vals| {
            vals.config_key_val
                .iter()
                .find(|kv| kv.config_file_name == config_file_name && kv.key == key)
        })
        .map(|kv| kv.value.clone());

    nested_value.or_else(|| {
        response.config_key_vals.as_ref().and_then(|vals| {
            vals.config_key_val
                .iter()
                .find(|kv| kv.config_file_name == config_file_name && kv.key == key)
                .map(|kv| kv.value.clone())
        })
    })
}

pub(crate) fn static_config_file_values(
    response: &nmxc_model::NmxGetStaticConfigResponse,
) -> Vec<nmxc_model::NmxConfigKeyVal> {
    response
        .static_config
        .as_ref()
        .and_then(|static_config| static_config.config_file_contents.as_ref())
        .into_iter()
        .flat_map(|contents| &contents.config_file_content)
        .flat_map(parse_static_config_file)
        .collect()
}

fn parse_static_config_file(
    config_file: &nmxc_model::NmxConfigFileContent,
) -> Vec<nmxc_model::NmxConfigKeyVal> {
    config_file
        .config_file_content
        .lines()
        .filter_map(|line| {
            let line = line.trim();

            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                return None;
            }

            let (key, value) = line.split_once('=').or_else(|| {
                let separator = line.find(char::is_whitespace)?;
                Some(line.split_at(separator))
            })?;

            let key = key.trim();

            if key.is_empty() {
                return None;
            }

            Some(nmxc_model::NmxConfigKeyVal {
                config_file_name: config_file.config_file_name.clone(),
                key: key.to_owned(),
                value: value.trim().to_owned(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use prost::Message;

    #[test]
    fn domain_wire_preserves_configured_state_and_independent_unhealthy_controller() {
        let wire = [0x0a, 0x02, 0x20, 0x01, 0x18, 0x03, 0x30, 0x03];
        let domain = nmxc_model::NmxDomainStateInfo::decode(wire.as_slice()).unwrap();

        assert!(
            check_server_header_success(domain.server_header.as_ref(), "GetDomainStateInfo")
                .is_ok()
        );

        assert_eq!(domain.control_plane_state, 3);
        assert_eq!(domain.nmx_controller_health, 3);
        assert_eq!(domain.encode_to_vec(), wire);
    }

    #[test]
    fn domain_wire_preserves_unknown_enum_values_and_unknown_return_codes() {
        let wire = [0x18, 0x8d, 0x07, 0x30, 0x8e, 0x07];
        let domain = nmxc_model::NmxDomainStateInfo::decode(wire.as_slice()).unwrap();

        let header = nmxc_model::NmxServerHeader {
            return_code: 503,
            ..Default::default()
        };

        let error = check_server_header_success(Some(&header), "GetDomainStateInfo").unwrap_err();

        assert_eq!(domain.control_plane_state, 909);
        assert_eq!(domain.nmx_controller_health, 910);
        assert_eq!(domain.encode_to_vec(), wire);
        assert_eq!(error.nmx_return_code(), Some(503));
    }

    #[test]
    fn membership_wire_preserves_alphanumeric_chassis_and_zero_position() {
        let location_wire = [
            0x12, 0x08, b'r', b'a', b'c', b'k', b'-', b'A', b'0', b'1', 0x20, 0x00, 0x32, 0x04,
            0x10, 0x00, 0x18, 0x07,
        ];

        let mut node_wire = vec![0x0a, location_wire.len() as u8];

        node_wire.extend_from_slice(&location_wire);
        node_wire.extend_from_slice(&[0x10, 0x02, 0x18, 0x8d, 0x07]);

        let mut list_wire = vec![0x1a, node_wire.len() as u8];

        list_wire.extend_from_slice(&node_wire);

        let switches =
            nmxc_model::NmxGetSwitchNodeInfoListResponse::decode(list_wire.as_slice()).unwrap();

        let computes =
            nmxc_model::NmxGetComputeNodeInfoListResponse::decode(list_wire.as_slice()).unwrap();

        let switch = switches.node_info_list.first().unwrap();
        let location = switch.loc.as_ref().unwrap();

        assert_eq!(location.chassis_serial_number, "rack-A01");
        assert_eq!(location.tray_index, 0);
        assert_eq!(location.location.as_ref().unwrap().slot_id, 0);
        assert_eq!(location.location.as_ref().unwrap().host_id, 7);
        assert_eq!(switch.num_switches, 2);
        assert_eq!(switch.node_health, 909);
        assert_eq!(computes.node_info_list.first().unwrap().num_gpus, 2);
        assert_eq!(computes.node_info_list.first().unwrap().node_health, 909);
    }

    #[test]
    fn topology_wire_preserves_device_health_and_port_states_at_official_tags() {
        let port_wire = [
            0x08, 0x02, 0x18, 0x07, 0x20, 0x0a, 0x28, 0x03, 0x30, 0x05, 0x38, 0x04, 0xf8, 0x01,
            0x01,
        ];

        let mut switch_wire = vec![
            0x18,
            0x09,
            0x28,
            0x01,
            0x30,
            0x0b,
            0x58,
            0x02,
            0x62,
            port_wire.len() as u8,
        ];

        switch_wire.extend_from_slice(&port_wire);

        let mut device_wire = vec![0x0a, switch_wire.len() as u8];

        device_wire.extend_from_slice(&switch_wire);

        let mut topology_wire = vec![0x1a, device_wire.len() as u8];

        topology_wire.extend_from_slice(&device_wire);

        let topology = nmxc_model::NmxFmTopologyInfo::decode(topology_wire.as_slice()).unwrap();

        let device = topology
            .device_topo_info
            .first()
            .unwrap()
            .device
            .as_ref()
            .unwrap();

        let nmxc_model::nmx_device_topo_info::Device::SwitchTopoInfo(switch) = device else {
            panic!("expected a switch topology observation");
        };

        let port = switch.port_topo_info.first().unwrap();

        assert_eq!(switch.device_uid, 9);
        assert_eq!(switch.system_uid, 11);
        assert_eq!(switch.device_health, 2);
        assert_eq!(switch.num_ports, 1);
        assert_eq!(port.port_type, 2);
        assert_eq!(port.port_num, 7);
        assert_eq!(port.peer_port_device_uid, 10);
        assert_eq!(port.peer_port_num, 3);
        assert_eq!(port.physical_state, 5);
        assert_eq!(port.logical_state, 4);
        assert_eq!(port.port_admin_state, 1);
        assert_eq!(topology.encode_to_vec(), topology_wire);
    }

    #[test]
    fn gpu_topology_wire_preserves_independent_health_and_unknown_link_states() {
        let wire = [
            0x1a, 0x0b, 0x12, 0x09, 0x68, 0x03, 0x72, 0x05, 0x30, 0x08, 0x38, 0x8d, 0x07,
        ];

        let topology = nmxc_model::NmxFmTopologyInfo::decode(wire.as_slice()).unwrap();

        let device = topology
            .device_topo_info
            .first()
            .unwrap()
            .device
            .as_ref()
            .unwrap();

        let nmxc_model::nmx_device_topo_info::Device::GpuTopoInfo(gpu) = device else {
            panic!("expected a GPU topology observation");
        };

        let port = gpu.port_topo_info.first().unwrap();

        assert_eq!(gpu.device_health, 3);
        assert_eq!(port.physical_state, 8);
        assert_eq!(port.logical_state, 909);
        assert_eq!(topology.encode_to_vec(), wire);
    }

    fn success_header() -> nmxc_model::NmxServerHeader {
        nmxc_model::NmxServerHeader {
            domain_uuid: String::new(),
            app_uuid: String::new(),
            app_ver: String::new(),
            return_code: nmxc_model::StReturnCode::NmxStSuccess as i32,
        }
    }

    #[test]
    fn check_server_header_success_accepts_success_code() {
        assert!(check_server_header_success(Some(&success_header()), "Hello").is_ok());
    }

    #[test]
    fn check_server_header_success_rejects_missing_header() {
        let err = check_server_header_success(None, "Hello").unwrap_err();
        assert!(matches!(
            err,
            NmxcError::MissingServerHeader { operation: "Hello" }
        ));
    }

    #[test]
    fn check_server_header_success_rejects_error_code() {
        let header = nmxc_model::NmxServerHeader {
            return_code: nmxc_model::StReturnCode::NmxStGenericError as i32,
            ..success_header()
        };
        let err = check_server_header_success(Some(&header), "SetStaticConfig").unwrap_err();
        assert_eq!(err.nmx_return_code(), Some(3));
    }

    #[test]
    fn find_static_config_value_reads_nested_static_config_first() {
        let response = nmxc_model::NmxGetStaticConfigResponse {
            server_header: Some(success_header()),
            static_config: Some(nmxc_model::NmxStaticConfig {
                context: None,
                config_key_vals: Some(nmxc_model::NmxConfigKeyVals {
                    config_key_val: vec![nmxc_model::NmxConfigKeyVal {
                        config_file_name: "fm_config".into(),
                        key: "MNNVL_TOPOLOGY".into(),
                        value: "nested".into(),
                    }],
                }),
                config_file_contents: None,
            }),
            config_key_vals: Some(nmxc_model::NmxConfigKeyVals {
                config_key_val: vec![nmxc_model::NmxConfigKeyVal {
                    config_file_name: "fm_config".into(),
                    key: "MNNVL_TOPOLOGY".into(),
                    value: "direct".into(),
                }],
            }),
        };

        assert_eq!(
            find_static_config_value(&response, "fm_config", "MNNVL_TOPOLOGY").as_deref(),
            Some("nested")
        );
    }

    #[test]
    fn find_static_config_value_falls_back_to_direct_values() {
        let response = nmxc_model::NmxGetStaticConfigResponse {
            server_header: Some(success_header()),
            static_config: None,
            config_key_vals: Some(nmxc_model::NmxConfigKeyVals {
                config_key_val: vec![nmxc_model::NmxConfigKeyVal {
                    config_file_name: "fm_config".into(),
                    key: "MNNVL_TOPOLOGY".into(),
                    value: "direct".into(),
                }],
            }),
        };

        assert_eq!(
            find_static_config_value(&response, "fm_config", "MNNVL_TOPOLOGY").as_deref(),
            Some("direct")
        );
    }

    #[test]
    fn static_config_file_values_parses_both_file_formats() {
        let response = nmxc_model::NmxGetStaticConfigResponse {
            server_header: Some(success_header()),
            static_config: Some(nmxc_model::NmxStaticConfig {
                context: None,
                config_key_vals: None,
                config_file_contents: Some(nmxc_model::NmxConfigFileContents {
                    config_file_content: vec![
                        nmxc_model::NmxConfigFileContent {
                            config_file_name: "fm_config".into(),
                            config_file_content: "# comment\nFIRST=one\nEMPTY=\n".into(),
                        },
                        nmxc_model::NmxConfigFileContent {
                            config_file_name: "sm_config".into(),
                            config_file_content: "; comment\nsecond two words\n".into(),
                        },
                    ],
                }),
            }),
            config_key_vals: None,
        };

        let values = static_config_file_values(&response);

        assert_eq!(values.len(), 3);
        assert_eq!(values[0].config_file_name, "fm_config");
        assert_eq!(values[0].key, "FIRST");
        assert_eq!(values[0].value, "one");
        assert_eq!(values[1].key, "EMPTY");
        assert!(values[1].value.is_empty());
        assert_eq!(values[2].config_file_name, "sm_config");
        assert_eq!(values[2].key, "second");
        assert_eq!(values[2].value, "two words");
    }
}
