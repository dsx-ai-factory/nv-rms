/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-Apache-2.0
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[cfg(feature = "client")]
mod client_api;

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub mod client_config;
pub mod protos;
#[cfg(feature = "serde")]
pub mod timestamp_serde {
    use chrono::{DateTime, Utc};
    use prost_types::Timestamp;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<Timestamp>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(timestamp) => {
                let dt = DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
                    .ok_or_else(|| {
                        serde::ser::Error::custom("invalid google.protobuf.Timestamp")
                    })?;
                serializer.serialize_some(&dt.to_rfc3339())
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Timestamp>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|value| {
                let dt = DateTime::parse_from_rfc3339(&value)
                    .map_err(D::Error::custom)?
                    .with_timezone(&Utc);
                Ok(Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                })
            })
            .transpose()
    }
}

#[cfg(feature = "client")]
pub(crate) use client::{RackManagerClientT, RackManagerV2ClientT};

#[cfg(feature = "client")]
pub use client_api::*;

/// Unary RPC observation contracts for consumers supplying auditing policy.
#[cfg(feature = "client")]
pub use tonic_client_wrapper::{RpcObservation, RpcObserver, RpcObserverError};

/// Descriptor set for decoding RMS V1 and V2 messages without client transport dependencies.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/rms.bin"));

#[cfg(test)]
mod proto_model_tests {
    use prost::Message as _;

    use super::protos::rack_manager::{
        ApplyStoredSwitchSystemImageRequest, ApplySwitchSystemImageRequest,
        BatchGetFirmwareInventoryRequest, BatchResetSwitchFactoryDefaultRequest,
        CompareFirmwareObjectRequest, Endpoint, FirmwareComparisonStatus, NetworkInterface,
        NodeInfo, NodeSet, NodeType, UpdateSwitchSystemImageRequest,
    };

    #[test]
    fn vrnvl72_node_types_have_stable_wire_values_and_names() {
        assert_eq!(NodeType::ComputeVrnvl72Nvidia as i32, 10);
        assert_eq!(NodeType::SwitchVrnvl72Nvidia as i32, 11);

        assert_eq!(
            NodeType::ComputeVrnvl72Nvidia.as_str_name(),
            "COMPUTE_VRNVL72_NVIDIA"
        );
        assert_eq!(
            NodeType::SwitchVrnvl72Nvidia.as_str_name(),
            "SWITCH_VRNVL72_NVIDIA"
        );

        assert_eq!(
            NodeType::from_str_name("COMPUTE_VRNVL72_NVIDIA"),
            Some(NodeType::ComputeVrnvl72Nvidia)
        );
        assert_eq!(
            NodeType::from_str_name("SWITCH_VRNVL72_NVIDIA"),
            Some(NodeType::SwitchVrnvl72Nvidia)
        );
    }

    #[test]
    fn additional_host_endpoints_use_protobuf_field_seven() {
        let node = NodeInfo {
            additional_host_endpoints: vec![Endpoint {
                interface: Some(NetworkInterface {
                    ip_address: "192.0.2.11".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(
            node.encode_to_vec(),
            [
                0x3a, 0x0e, 0x0a, 0x0c, 0x0a, 0x0a, b'1', b'9', b'2', b'.', b'0', b'.', b'2', b'.',
                b'1', b'1',
            ]
        );
    }

    #[test]
    fn node_info_without_additional_host_endpoints_decodes_with_an_empty_list()
    -> Result<(), prost::DecodeError> {
        let node = NodeInfo::decode([0x0a, 0x05, b's', b'w', b'-', b'0', b'1'].as_slice())?;

        assert_eq!(node.node_id, "sw-01");
        assert!(node.additional_host_endpoints.is_empty());

        Ok(())
    }

    #[test]
    fn batch_firmware_inventory_request_uses_node_set_field_two() -> Result<(), prost::DecodeError>
    {
        let request = BatchGetFirmwareInventoryRequest {
            nodes: Some(NodeSet {
                nodes: vec![NodeInfo {
                    node_id: "compute-01".to_owned(),
                    ..Default::default()
                }],
            }),
        };

        let encoded = request.encode_to_vec();
        assert_eq!(encoded.first(), Some(&0x12));
        assert_eq!(
            BatchGetFirmwareInventoryRequest::decode(encoded.as_slice())?,
            request
        );

        Ok(())
    }

    #[test]
    fn switch_system_image_requests_encode_allow_onie_recovery_as_plain_bool() {
        let stored_request = ApplyStoredSwitchSystemImageRequest {
            allow_onie_recovery: true,
            ..Default::default()
        };

        let direct_request = ApplySwitchSystemImageRequest {
            allow_onie_recovery: true,
            ..Default::default()
        };

        let update_request = UpdateSwitchSystemImageRequest {
            allow_onie_recovery: true,
            ..Default::default()
        };

        // A set flag is encoded under the field's tag (7, 8 and 5 respectively).
        assert_eq!(stored_request.encode_to_vec(), [0x38, 0x01]);
        assert_eq!(direct_request.encode_to_vec(), [0x40, 0x01]);
        assert_eq!(update_request.encode_to_vec(), [0x28, 0x01]);

        // The field is a plain proto3 bool: false is the default and is not encoded, so an
        // explicit false is indistinguishable from an unset value on the wire.
        assert!(!ApplyStoredSwitchSystemImageRequest::default().allow_onie_recovery);
        assert!(!ApplySwitchSystemImageRequest::default().allow_onie_recovery);
        assert!(!UpdateSwitchSystemImageRequest::default().allow_onie_recovery);
        assert!(
            ApplyStoredSwitchSystemImageRequest {
                allow_onie_recovery: false,
                ..Default::default()
            }
            .encode_to_vec()
            .is_empty()
        );

        assert!(
            ApplyStoredSwitchSystemImageRequest::decode(stored_request.encode_to_vec().as_slice())
                .unwrap()
                .allow_onie_recovery
        );
    }

    #[test]
    fn factory_reset_ssh_fallback_defaults_to_disabled() -> Result<(), prost::DecodeError> {
        let request = BatchResetSwitchFactoryDefaultRequest::decode([].as_slice())?;

        assert!(!request.allow_ssh_fallback);
        assert!(request.encode_to_vec().is_empty());

        Ok(())
    }

    #[test]
    fn firmware_manifest_comparison_api_has_stable_wire_values() {
        assert_eq!(FirmwareComparisonStatus::Matched as i32, 1);
        assert_eq!(FirmwareComparisonStatus::Mismatched as i32, 2);
        assert_eq!(FirmwareComparisonStatus::Missing as i32, 3);
        assert_eq!(FirmwareComparisonStatus::Unverified as i32, 4);

        let request = CompareFirmwareObjectRequest {
            config_json: "{}".to_owned(),
            firmware_type: "prod".to_owned(),
            nodes: Some(NodeSet::default()),
            ..Default::default()
        };

        assert_eq!(request.config_json, "{}");
        assert_eq!(request.firmware_type, "prod");
        assert!(request.nodes.is_some());
        assert_eq!(request.encode_to_vec().first(), Some(&0x0a));
    }
}

#[cfg(all(test, feature = "client"))]
mod client_feature_tests {
    use super::*;

    fn assert_public_type<T: ?Sized>() -> &'static str {
        std::any::type_name::<T>()
    }

    #[test]
    fn client_feature_exposes_expected_public_api_paths() {
        let public_api_types = [
            assert_public_type::<RackManagerApi>(),
            assert_public_type::<RmsClientPool>(),
            assert_public_type::<dyn RmsApi>(),
            assert_public_type::<client::RmsTlsClient<'static>>(),
            assert_public_type::<client_config::RmsClientConfig>(),
            assert_public_type::<
                protos::rack_manager::rack_manager_client::RackManagerClient<
                    tonic::transport::Channel,
                >,
            >(),
            assert_public_type::<protos::rack_manager_client::RackManagerApiClient>(),
            assert_public_type::<
                protos::rack_manager_v2::rack_manager_v2_client::RackManagerV2Client<
                    tonic::transport::Channel,
                >,
            >(),
            assert_public_type::<protos::rack_manager_v2_client::RackManagerV2ApiClient>(),
        ];

        assert_eq!(public_api_types.len(), 9);
    }
}

#[cfg(all(test, feature = "server"))]
mod server_feature_tests {
    use super::*;

    #[test]
    fn server_feature_exposes_generated_server_type() {
        let server_type = std::any::type_name::<
            protos::rack_manager::rack_manager_server::RackManagerServer<()>,
        >();

        let server_v2_type = std::any::type_name::<
            protos::rack_manager_v2::rack_manager_v2_server::RackManagerV2Server<()>,
        >();

        assert!(server_type.ends_with("RackManagerServer<()>"));
        assert!(server_v2_type.ends_with("RackManagerV2Server<()>"));
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests {
    use super::protos::{rack_manager as rms, rack_manager_v2 as rms_v2};

    #[test]
    fn factory_reset_ssh_fallback_defaults_to_disabled_in_json() -> Result<(), serde_json::Error> {
        let request: rms::BatchResetSwitchFactoryDefaultRequest = serde_json::from_str("{}")?;

        assert!(!request.allow_ssh_fallback);

        Ok(())
    }

    /// Compile-time trait bound check. The function body is empty and optimized away;
    /// the compiler just verifies T satisfies Serialize + DeserializeOwned. If the
    /// type_attribute in build.rs stops covering a type, the call site fails to compile.
    fn assert_serde<T: serde::Serialize + serde::de::DeserializeOwned>() {}

    /// Verifies that the single package-level type_attribute(".rack_manager", ...) in
    /// build.rs correctly applies serde derives to all proto-generated types. Covers
    /// the structurally distinct categories: plain messages, the oneof enum nested inside
    /// Credentials (which previously needed separate handling), top-level enums,
    /// request/response pairs, and timestamp-backed responses.
    #[test]
    fn proto_types_implement_serde() {
        // Plain message
        assert_serde::<rms::Credentials>();
        assert_serde::<rms::ComponentInventoryInfo>();
        assert_serde::<rms::NodeDescriptor>();
        assert_serde::<rms::NodeDescriptorFirmwareTargetList>();
        assert_serde::<rms::NodeDescriptorFirmwareObjectComponentFilter>();

        // Oneof enum - the case that previously required special handling in build.rs
        assert_serde::<rms::credentials::Auth>();

        // Top-level enums
        assert_serde::<rms::NodeType>();
        assert_serde::<rms::PowerOperation>();
        assert_serde::<rms::SwitchService>();

        // Representative request/response pair
        assert_serde::<rms::SetPowerStateRequest>();
        assert_serde::<rms::SetPowerStateResponse>();
        assert_serde::<rms::BatchSetScaleUpFabricStateRequest>();
        assert_serde::<rms::BatchSetScaleUpFabricStateResponse>();
        assert_serde::<rms::BatchResetSwitchSdnFactoryDefaultRequest>();
        assert_serde::<rms::BatchResetSwitchSdnFactoryDefaultResponse>();
        assert_serde::<rms::BatchResetSwitchFactoryDefaultRequest>();
        assert_serde::<rms::BatchResetSwitchFactoryDefaultResponse>();
        assert_serde::<rms::JobStatus>();
        assert_serde::<rms::GetJobStatusRequest>();
        assert_serde::<rms::GetJobStatusResponse>();
        assert_serde::<rms::ConfigureSwitchCertificateRequest>();
        assert_serde::<rms::ConfigureSwitchCertificateResponse>();
        assert_serde::<rms::ConfigureSwitchCertificateJobInfo>();
        assert_serde::<rms::BatchDisableSwitchMtlsRequest>();
        assert_serde::<rms::BatchDisableSwitchMtlsResponse>();
        assert_serde::<rms::GetConfigureSwitchCertificateJobStatusRequest>();
        assert_serde::<rms::GetConfigureSwitchCertificateJobStatusResponse>();

        // Scale-up fabric status models exposed by RackManager.
        assert_serde::<rms::GetScaleUpFabricStatusRequest>();
        assert_serde::<rms::GetScaleUpFabricStatusResponse>();
        assert_serde::<rms::ScaleUpFabricStaticConfig>();
        assert_serde::<rms::ScaleUpFabricStatus>();
        assert_serde::<rms::ScaleUpFabricSwitchStatus>();

        // RackManagerV2 request, response, and desired configuration model.
        assert_serde::<rms_v2::ConfigureScaleUpFabricManagerRequest>();
        assert_serde::<rms_v2::ConfigureScaleUpFabricManagerResponse>();
        assert_serde::<rms_v2::ScaleUpFabricConfig>();
        assert_serde::<rms_v2::StartSystemValidationRequest>();
        assert_serde::<rms_v2::StartSystemValidationResponse>();

        // Timestamp-backed responses
        assert_serde::<rms::GetFirmwareJobStatusResponse>();
        assert_serde::<rms::GetSwitchSystemImageJobStatusResponse>();
        assert_serde::<rms::BatchGetFirmwareInventoryRequest>();
        assert_serde::<rms::BatchGetFirmwareInventoryResponse>();
    }
}
