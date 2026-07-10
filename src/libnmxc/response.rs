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

#[cfg(test)]
mod tests {
    use super::*;

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
}
