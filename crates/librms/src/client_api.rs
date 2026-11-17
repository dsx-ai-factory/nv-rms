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

use std::sync::Arc;
use std::time::SystemTime;
use std::{fs, io};

use crate::client::{
    RackManagerClientT, RackManagerV2ClientT, RetryConfig, RmsApiConfig, RmsTlsClient,
};
use crate::client_config::RmsClientConfig;
use crate::protos::rack_manager as rms;
use crate::protos::rack_manager_client::RackManagerApiClient;
use crate::protos::rack_manager_v2 as rms_v2;
use crate::protos::rack_manager_v2_client::RackManagerV2ApiClient;

use chrono::{DateTime, Utc};
use tonic::Status;

#[derive(thiserror::Error, Debug)]
pub enum RmsTlsClientError {
    #[error("ConnectError error: {0}")]
    Connection(String),
    #[error("Configuration error: {0}")]
    Configuration(#[from] ConfigurationError),
    #[error("Rust TLS error: {0}")]
    RustTLS(#[from] rustls::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigurationError {
    #[error("Invalid URI {uri_string}: {error}")]
    InvalidUri {
        uri_string: String,
        error: hyper::http::uri::InvalidUri,
    },
    #[error("Could not read Root CA cert at {path}: {error}")]
    CouldNotReadRootCa { path: String, error: io::Error },
    #[error("Could not read Client cert at {path}: {error}")]
    CouldNotReadClientCert { path: String, error: io::Error },
    #[error("Could not read Client key at {path}: {error}")]
    CouldNotReadClientKey { path: String, error: io::Error },
    #[error("Invalid Client cert: {error}")]
    InvalidClientCert { error: String },
    #[error("Invalid Client key: {error}")]
    InvalidClientKey { error: String },
    #[error("Invalid Root CA: {error}")]
    InvalidRootCa { error: String },
    #[error("Invalid HTTP URL with TLS enforced: {url}")]
    InvalidHTTPURL { url: String },
}

impl From<RmsTlsClientError> for tonic::Status {
    fn from(value: RmsTlsClientError) -> Self {
        tonic::Status::unavailable(value.to_string())
    }
}

impl RackManagerApiClient {
    pub fn new(rms_config: &RmsApiConfig<'_>) -> Self {
        Self::build(RmsTlsConnectionProvider {
            url: rms_config.url.to_string(),
            client_config: rms_config.client_config.clone(),
            retry_config: rms_config.retry_config,
        })
    }
}

impl RackManagerV2ApiClient {
    /// Creates a lazy wrapper client for RackManagerV2 RPCs.
    pub fn new(rms_config: &RmsApiConfig<'_>) -> Self {
        Self::build(RmsTlsConnectionProviderV2 {
            url: rms_config.url.to_string(),
            client_config: rms_config.client_config.clone(),
            retry_config: rms_config.retry_config,
        })
    }
}

// TODO: Add more error types for better error handling.
#[derive(thiserror::Error, Debug)]
pub enum RackManagerError {
    #[error("The connection or API call to the Rack Manager server returned {0}")]
    ApiInvocationError(#[from] tonic::Status),
    #[error("TLS client error: {0}")]
    TlsError(#[from] RmsTlsClientError),
}

#[derive(Clone)]
pub struct RmsClientPool {
    pub client: RackManagerApi,
}

impl RmsClientPool {
    pub fn new(rms_api_config: &RmsApiConfig<'_>) -> Self {
        let client = RackManagerApi::new(rms_api_config);
        Self { client }
    }
}

#[async_trait::async_trait]
pub trait RackManagerClientPool: Send + Sync + 'static {
    async fn create_client(&self) -> Arc<dyn RmsApi>;
}

#[async_trait::async_trait]
impl RackManagerClientPool for RmsClientPool {
    async fn create_client(&self) -> Arc<dyn RmsApi> {
        Arc::new(self.client.clone())
    }
}

#[derive(Clone, Debug)]
pub struct RackManagerApi {
    pub client: RackManagerApiClient,
    #[allow(unused)]
    pub config: RmsClientConfig,
    #[allow(unused)]
    pub api_url: String,
}

impl RackManagerApi {
    /// create a rack manager client that can be used in the api server
    pub fn new(rms_api_config: &RmsApiConfig<'_>) -> Self {
        let client = RackManagerApiClient::new(rms_api_config);

        Self {
            client,
            config: rms_api_config.client_config.clone(),
            api_url: rms_api_config.url.to_string(),
        }
    }

    /// Issues one unary RackManagerV2 RPC with the configured `RpcObserver`
    /// (`RmsClientConfig::rpc_observer`) attached.
    ///
    /// Every direct V2 call made by `RackManagerApi` must go through this helper so that it
    /// is observed consistently. It preserves the shared V1 readiness/retry policy by
    /// establishing the V1 connection first, builds a V2 client, and reports
    /// `RackManagerError::TlsError` to the observer as `Unavailable`.
    ///
    /// `method`, `request_type` and `response_type` are the protobuf names reported to the
    /// observer (for example `"rack_manager_v2.StartSystemValidationRequest"`). `call`
    /// receives an owned V2 client and the request, and performs the generated client call.
    async fn observed_v2_unary<Req, Resp, Call, Fut>(
        &self,
        method: &'static str,
        request_type: &'static str,
        response_type: &'static str,
        request: Req,
        call: Call,
    ) -> Result<Resp, RackManagerError>
    where
        Req: prost::Message,
        Resp: prost::Message,
        Call: FnOnce(RackManagerV2ClientT, tonic::Request<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<Resp>, Status>>,
    {
        let observation = tonic_client_wrapper::start_rpc(
            self.config.rpc_observer.as_deref(),
            method,
            request_type,
            &request,
        );
        tonic_client_wrapper::finish_rpc(
            observation,
            response_type,
            async {
                let _ = self.client.connection().await?;
                let client_v2 =
                    RmsTlsClient::new(&self.config).build_rms_client_v2(&self.api_url)?;
                Ok(call(client_v2, tonic::Request::new(request))
                    .await?
                    .into_inner())
            },
            |error: &RackManagerError| match error {
                RackManagerError::ApiInvocationError(status) => status.code(),
                RackManagerError::TlsError(_) => tonic::Code::Unavailable,
            },
        )
        .await
    }
}

// declare the functions
#[allow(clippy::too_many_arguments, dead_code)]
#[async_trait::async_trait]
pub trait RmsApi: Send + Sync + 'static {
    async fn set_power_state(
        &self,
        cmd: rms::SetPowerStateRequest,
    ) -> Result<rms::SetPowerStateResponse, RackManagerError>;

    async fn batch_set_power_state(
        &self,
        cmd: rms::BatchSetPowerStateRequest,
    ) -> Result<rms::BatchSetPowerStateResponse, RackManagerError>;

    async fn get_power_state(
        &self,
        cmd: rms::GetPowerStateRequest,
    ) -> Result<rms::GetPowerStateResponse, RackManagerError>;

    async fn batch_get_power_state(
        &self,
        cmd: rms::BatchGetPowerStateRequest,
    ) -> Result<rms::BatchGetPowerStateResponse, RackManagerError>;

    async fn list_node_inventory(&self)
    -> Result<rms::ListNodeInventoryResponse, RackManagerError>;

    async fn create_nodes(
        &self,
        cmd: rms::CreateNodesRequest,
    ) -> Result<rms::CreateNodesResponse, RackManagerError>;

    async fn update_node(
        &self,
        cmd: rms::UpdateNodeRequest,
    ) -> Result<rms::UpdateNodeResponse, RackManagerError>;

    async fn delete_node(
        &self,
        cmd: rms::DeleteNodeRequest,
    ) -> Result<rms::DeleteNodeResponse, RackManagerError>;

    async fn list_racks(&self) -> Result<rms::ListRacksResponse, RackManagerError>;

    async fn get_node_device_info(
        &self,
        cmd: rms::GetNodeDeviceInfoRequest,
    ) -> Result<rms::GetNodeDeviceInfoResponse, RackManagerError>;

    async fn list_node_device_info_by_node_type(
        &self,
        cmd: rms::ListNodeDeviceInfoByNodeTypeRequest,
    ) -> Result<rms::ListNodeDeviceInfoByNodeTypeResponse, RackManagerError>;

    async fn batch_get_node_device_info(
        &self,
        cmd: rms::BatchGetNodeDeviceInfoRequest,
    ) -> Result<rms::BatchGetNodeDeviceInfoResponse, RackManagerError>;

    async fn get_node_firmware_inventory(
        &self,
        cmd: rms::GetNodeFirmwareInventoryRequest,
    ) -> Result<rms::GetNodeFirmwareInventoryResponse, RackManagerError>;

    async fn batch_get_firmware_inventory(
        &self,
        cmd: rms::BatchGetFirmwareInventoryRequest,
    ) -> Result<rms::BatchGetFirmwareInventoryResponse, RackManagerError>;

    async fn compare_firmware_object(
        &self,
        cmd: rms::CompareFirmwareObjectRequest,
    ) -> Result<rms::CompareFirmwareObjectResponse, RackManagerError>;

    async fn get_rack_firmware_inventory(
        &self,
        cmd: rms::GetRackFirmwareInventoryRequest,
    ) -> Result<rms::GetRackFirmwareInventoryResponse, RackManagerError>;

    async fn update_firmware(
        &self,
        cmd: rms::UpdateFirmwareRequest,
    ) -> Result<rms::UpdateFirmwareResponse, RackManagerError>;

    async fn batch_update_firmware_by_node_type(
        &self,
        cmd: rms::BatchUpdateFirmwareByNodeTypeRequest,
    ) -> Result<rms::BatchUpdateFirmwareByNodeTypeResponse, RackManagerError>;

    async fn batch_update_firmware(
        &self,
        cmd: rms::BatchUpdateFirmwareRequest,
    ) -> Result<rms::BatchUpdateFirmwareResponse, RackManagerError>;

    async fn update_switch_system_image(
        &self,
        cmd: rms::UpdateSwitchSystemImageRequest,
    ) -> Result<rms::UpdateSwitchSystemImageResponse, RackManagerError>;

    async fn add_firmware_object(
        &self,
        cmd: rms::AddFirmwareObjectRequest,
    ) -> Result<rms::AddFirmwareObjectResponse, RackManagerError>;

    async fn get_firmware_object(
        &self,
        cmd: rms::GetFirmwareObjectRequest,
    ) -> Result<rms::GetFirmwareObjectResponse, RackManagerError>;

    async fn list_firmware_objects(
        &self,
        cmd: rms::ListFirmwareObjectsRequest,
    ) -> Result<rms::ListFirmwareObjectsResponse, RackManagerError>;

    async fn delete_firmware_object(
        &self,
        cmd: rms::DeleteFirmwareObjectRequest,
    ) -> Result<rms::DeleteFirmwareObjectResponse, RackManagerError>;

    async fn set_default_firmware_object(
        &self,
        cmd: rms::SetDefaultFirmwareObjectRequest,
    ) -> Result<rms::SetDefaultFirmwareObjectResponse, RackManagerError>;

    async fn apply_stored_firmware_object(
        &self,
        cmd: rms::ApplyStoredFirmwareObjectRequest,
    ) -> Result<rms::ApplyStoredFirmwareObjectResponse, RackManagerError>;

    async fn apply_firmware_object(
        &self,
        cmd: rms::ApplyFirmwareObjectRequest,
    ) -> Result<rms::ApplyFirmwareObjectResponse, RackManagerError>;

    async fn apply_switch_system_image(
        &self,
        cmd: rms::ApplySwitchSystemImageRequest,
    ) -> Result<rms::ApplySwitchSystemImageResponse, RackManagerError>;

    async fn apply_stored_switch_system_image(
        &self,
        cmd: rms::ApplyStoredSwitchSystemImageRequest,
    ) -> Result<rms::ApplyStoredSwitchSystemImageResponse, RackManagerError>;

    async fn get_firmware_object_history(
        &self,
        cmd: rms::GetFirmwareObjectHistoryRequest,
    ) -> Result<rms::GetFirmwareObjectHistoryResponse, RackManagerError>;

    async fn list_switch_firmware(
        &self,
        cmd: rms::ListSwitchFirmwareRequest,
    ) -> Result<rms::ListSwitchFirmwareResponse, RackManagerError>;

    async fn push_switch_firmware(
        &self,
        cmd: rms::PushSwitchFirmwareRequest,
    ) -> Result<rms::PushSwitchFirmwareResponse, RackManagerError>;

    /// Submits asynchronous NVOS factory-default resets for the given switches.
    ///
    /// Set `cmd.allow_ssh_fallback` to permit reset submission over SSH when the
    /// reset cannot be submitted through NVUE.
    /// The fallback is disabled by default.
    ///
    /// Implementors may omit this method; the default returns `Unimplemented`.
    async fn batch_reset_switch_factory_default(
        &self,
        _cmd: rms::BatchResetSwitchFactoryDefaultRequest,
    ) -> Result<rms::BatchResetSwitchFactoryDefaultResponse, RackManagerError> {
        Err(tonic::Status::unimplemented(
            "BatchResetSwitchFactoryDefault is not implemented by this RmsApi",
        )
        .into())
    }

    /// Submits V2 scale-up fabric reconciliation configuration.
    ///
    /// Implementors may omit this method; the default returns `Unimplemented`.
    async fn configure_scale_up_fabric_manager_v2(
        &self,
        _cmd: rms_v2::ConfigureScaleUpFabricManagerRequest,
    ) -> Result<rms_v2::ConfigureScaleUpFabricManagerResponse, RackManagerError> {
        Err(tonic::Status::unimplemented("RackManagerV2 is not implemented by this RmsApi").into())
    }

    /// Launches an NVSSVT system validation scan as an asynchronous job.
    ///
    /// Implementors may omit this method; the default returns `Unimplemented`.
    async fn start_system_validation(
        &self,
        _cmd: rms_v2::StartSystemValidationRequest,
    ) -> Result<rms_v2::StartSystemValidationResponse, RackManagerError> {
        Err(tonic::Status::unimplemented("RackManagerV2 is not implemented by this RmsApi").into())
    }

    async fn configure_scale_up_fabric_manager(
        &self,
        cmd: rms::ConfigureScaleUpFabricManagerRequest,
    ) -> Result<rms::ConfigureScaleUpFabricManagerResponse, RackManagerError>;

    /// Retrieves the observed scale-up fabric state.
    ///
    /// Implementors may omit this method; the default returns `Unimplemented`.
    async fn get_scale_up_fabric_status(
        &self,
        _cmd: rms::GetScaleUpFabricStatusRequest,
    ) -> Result<rms::GetScaleUpFabricStatusResponse, RackManagerError> {
        Err(tonic::Status::unimplemented(
            "GetScaleUpFabricStatus is not implemented by this RmsApi",
        )
        .into())
    }

    /// Reads live fabric members, controller state, and sampled component status.
    ///
    /// Collection failures retain successful observations. A normal component
    /// status does not establish end-to-end connectivity or bandwidth.
    /// Implementors may omit this method; the default returns `Unimplemented`.
    async fn inspect_scale_up_fabrics(
        &self,
        _cmd: rms::InspectScaleUpFabricsRequest,
    ) -> Result<rms::InspectScaleUpFabricsResponse, RackManagerError> {
        Err(
            tonic::Status::unimplemented("InspectScaleUpFabrics is not implemented by this RmsApi")
                .into(),
        )
    }

    async fn batch_reset_switch_sdn_factory_default(
        &self,
        cmd: rms::BatchResetSwitchSdnFactoryDefaultRequest,
    ) -> Result<rms::BatchResetSwitchSdnFactoryDefaultResponse, RackManagerError>;

    async fn batch_set_scale_up_fabric_state(
        &self,
        cmd: rms::BatchSetScaleUpFabricStateRequest,
    ) -> Result<rms::BatchSetScaleUpFabricStateResponse, RackManagerError>;

    async fn batch_get_scale_up_fabric_service_status(
        &self,
        cmd: rms::BatchGetScaleUpFabricServiceStatusRequest,
    ) -> Result<rms::BatchGetScaleUpFabricServiceStatusResponse, RackManagerError>;

    async fn get_scale_up_fabric_state(
        &self,
        cmd: rms::GetScaleUpFabricStateRequest,
    ) -> Result<rms::GetScaleUpFabricStateResponse, RackManagerError>;

    async fn set_scale_up_fabric_telemetry_interface_state(
        &self,
        cmd: rms::SetScaleUpFabricTelemetryInterfaceStateRequest,
    ) -> Result<rms::SetScaleUpFabricTelemetryInterfaceStateResponse, RackManagerError>;

    async fn configure_switch_certificate(
        &self,
        cmd: rms::ConfigureSwitchCertificateRequest,
    ) -> Result<rms::ConfigureSwitchCertificateResponse, RackManagerError>;

    async fn batch_disable_switch_mtls(
        &self,
        cmd: rms::BatchDisableSwitchMtlsRequest,
    ) -> Result<rms::BatchDisableSwitchMtlsResponse, RackManagerError>;

    async fn get_configure_switch_certificate_job_status(
        &self,
        cmd: rms::GetConfigureSwitchCertificateJobStatusRequest,
    ) -> Result<rms::GetConfigureSwitchCertificateJobStatusResponse, RackManagerError>;

    async fn list_switch_system_images(
        &self,
        cmd: rms::ListSwitchSystemImagesRequest,
    ) -> Result<rms::ListSwitchSystemImagesResponse, RackManagerError>;

    async fn get_switch_system_image_job_status(
        &self,
        cmd: rms::GetSwitchSystemImageJobStatusRequest,
    ) -> Result<rms::GetSwitchSystemImageJobStatusResponse, RackManagerError>;

    async fn update_switch_system_password(
        &self,
        cmd: rms::UpdateSwitchSystemPasswordRequest,
    ) -> Result<rms::UpdateSwitchSystemPasswordResponse, RackManagerError>;

    async fn batch_collect_switch_spdm_attestation_evidence(
        &self,
        _cmd: rms::BatchCollectSwitchSpdmAttestationEvidenceRequest,
    ) -> Result<rms::BatchCollectSwitchSpdmAttestationEvidenceResponse, RackManagerError> {
        Err(tonic::Status::unimplemented(
            "switch SPDM attestation evidence collection is not implemented",
        )
        .into())
    }

    async fn get_version(&self) -> Result<rms::GetVersionResponse, RackManagerError>;

    async fn get_firmware_job_status(
        &self,
        cmd: rms::GetFirmwareJobStatusRequest,
    ) -> Result<rms::GetFirmwareJobStatusResponse, RackManagerError>;

    async fn get_job_status(
        &self,
        cmd: rms::GetJobStatusRequest,
    ) -> Result<rms::GetJobStatusResponse, RackManagerError>;
}

#[async_trait::async_trait]
impl RmsApi for RackManagerApi {
    async fn set_power_state(
        &self,
        cmd: rms::SetPowerStateRequest,
    ) -> Result<rms::SetPowerStateResponse, RackManagerError> {
        Ok(self.client.set_power_state(cmd).await?)
    }

    async fn batch_set_power_state(
        &self,
        cmd: rms::BatchSetPowerStateRequest,
    ) -> Result<rms::BatchSetPowerStateResponse, RackManagerError> {
        Ok(self.client.batch_set_power_state(cmd).await?)
    }

    async fn get_power_state(
        &self,
        cmd: rms::GetPowerStateRequest,
    ) -> Result<rms::GetPowerStateResponse, RackManagerError> {
        Ok(self.client.get_power_state(cmd).await?)
    }

    async fn batch_get_power_state(
        &self,
        cmd: rms::BatchGetPowerStateRequest,
    ) -> Result<rms::BatchGetPowerStateResponse, RackManagerError> {
        Ok(self.client.batch_get_power_state(cmd).await?)
    }

    async fn list_node_inventory(
        &self,
    ) -> Result<rms::ListNodeInventoryResponse, RackManagerError> {
        Ok(self.client.list_node_inventory().await?)
    }

    async fn create_nodes(
        &self,
        cmd: rms::CreateNodesRequest,
    ) -> Result<rms::CreateNodesResponse, RackManagerError> {
        Ok(self.client.create_nodes(cmd).await?)
    }

    async fn update_node(
        &self,
        cmd: rms::UpdateNodeRequest,
    ) -> Result<rms::UpdateNodeResponse, RackManagerError> {
        Ok(self.client.update_node(cmd).await?)
    }

    async fn delete_node(
        &self,
        cmd: rms::DeleteNodeRequest,
    ) -> Result<rms::DeleteNodeResponse, RackManagerError> {
        Ok(self.client.delete_node(cmd).await?)
    }

    async fn list_racks(&self) -> Result<rms::ListRacksResponse, RackManagerError> {
        Ok(self.client.list_racks().await?)
    }

    async fn get_node_device_info(
        &self,
        cmd: rms::GetNodeDeviceInfoRequest,
    ) -> Result<rms::GetNodeDeviceInfoResponse, RackManagerError> {
        Ok(self.client.get_node_device_info(cmd).await?)
    }

    async fn list_node_device_info_by_node_type(
        &self,
        cmd: rms::ListNodeDeviceInfoByNodeTypeRequest,
    ) -> Result<rms::ListNodeDeviceInfoByNodeTypeResponse, RackManagerError> {
        Ok(self.client.list_node_device_info_by_node_type(cmd).await?)
    }

    async fn batch_get_node_device_info(
        &self,
        cmd: rms::BatchGetNodeDeviceInfoRequest,
    ) -> Result<rms::BatchGetNodeDeviceInfoResponse, RackManagerError> {
        Ok(self.client.batch_get_node_device_info(cmd).await?)
    }

    async fn get_node_firmware_inventory(
        &self,
        cmd: rms::GetNodeFirmwareInventoryRequest,
    ) -> Result<rms::GetNodeFirmwareInventoryResponse, RackManagerError> {
        Ok(self.client.get_node_firmware_inventory(cmd).await?)
    }

    async fn batch_get_firmware_inventory(
        &self,
        cmd: rms::BatchGetFirmwareInventoryRequest,
    ) -> Result<rms::BatchGetFirmwareInventoryResponse, RackManagerError> {
        Ok(self.client.batch_get_firmware_inventory(cmd).await?)
    }

    async fn compare_firmware_object(
        &self,
        cmd: rms::CompareFirmwareObjectRequest,
    ) -> Result<rms::CompareFirmwareObjectResponse, RackManagerError> {
        Ok(self.client.compare_firmware_object(cmd).await?)
    }

    async fn get_rack_firmware_inventory(
        &self,
        cmd: rms::GetRackFirmwareInventoryRequest,
    ) -> Result<rms::GetRackFirmwareInventoryResponse, RackManagerError> {
        Ok(self.client.get_rack_firmware_inventory(cmd).await?)
    }

    async fn update_firmware(
        &self,
        cmd: rms::UpdateFirmwareRequest,
    ) -> Result<rms::UpdateFirmwareResponse, RackManagerError> {
        Ok(self.client.update_firmware(cmd).await?)
    }

    async fn batch_update_firmware_by_node_type(
        &self,
        cmd: rms::BatchUpdateFirmwareByNodeTypeRequest,
    ) -> Result<rms::BatchUpdateFirmwareByNodeTypeResponse, RackManagerError> {
        Ok(self.client.batch_update_firmware_by_node_type(cmd).await?)
    }

    async fn batch_update_firmware(
        &self,
        cmd: rms::BatchUpdateFirmwareRequest,
    ) -> Result<rms::BatchUpdateFirmwareResponse, RackManagerError> {
        Ok(self.client.batch_update_firmware(cmd).await?)
    }

    async fn update_switch_system_image(
        &self,
        cmd: rms::UpdateSwitchSystemImageRequest,
    ) -> Result<rms::UpdateSwitchSystemImageResponse, RackManagerError> {
        Ok(self.client.update_switch_system_image(cmd).await?)
    }

    async fn add_firmware_object(
        &self,
        cmd: rms::AddFirmwareObjectRequest,
    ) -> Result<rms::AddFirmwareObjectResponse, RackManagerError> {
        Ok(self.client.add_firmware_object(cmd).await?)
    }

    async fn get_firmware_object(
        &self,
        cmd: rms::GetFirmwareObjectRequest,
    ) -> Result<rms::GetFirmwareObjectResponse, RackManagerError> {
        Ok(self.client.get_firmware_object(cmd).await?)
    }

    async fn list_firmware_objects(
        &self,
        cmd: rms::ListFirmwareObjectsRequest,
    ) -> Result<rms::ListFirmwareObjectsResponse, RackManagerError> {
        Ok(self.client.list_firmware_objects(cmd).await?)
    }

    async fn delete_firmware_object(
        &self,
        cmd: rms::DeleteFirmwareObjectRequest,
    ) -> Result<rms::DeleteFirmwareObjectResponse, RackManagerError> {
        Ok(self.client.delete_firmware_object(cmd).await?)
    }

    async fn set_default_firmware_object(
        &self,
        cmd: rms::SetDefaultFirmwareObjectRequest,
    ) -> Result<rms::SetDefaultFirmwareObjectResponse, RackManagerError> {
        Ok(self.client.set_default_firmware_object(cmd).await?)
    }

    async fn apply_stored_firmware_object(
        &self,
        cmd: rms::ApplyStoredFirmwareObjectRequest,
    ) -> Result<rms::ApplyStoredFirmwareObjectResponse, RackManagerError> {
        Ok(self.client.apply_stored_firmware_object(cmd).await?)
    }

    async fn apply_firmware_object(
        &self,
        cmd: rms::ApplyFirmwareObjectRequest,
    ) -> Result<rms::ApplyFirmwareObjectResponse, RackManagerError> {
        Ok(self.client.apply_firmware_object(cmd).await?)
    }

    async fn apply_switch_system_image(
        &self,
        cmd: rms::ApplySwitchSystemImageRequest,
    ) -> Result<rms::ApplySwitchSystemImageResponse, RackManagerError> {
        Ok(self.client.apply_switch_system_image(cmd).await?)
    }

    async fn apply_stored_switch_system_image(
        &self,
        cmd: rms::ApplyStoredSwitchSystemImageRequest,
    ) -> Result<rms::ApplyStoredSwitchSystemImageResponse, RackManagerError> {
        Ok(self.client.apply_stored_switch_system_image(cmd).await?)
    }

    async fn get_firmware_object_history(
        &self,
        cmd: rms::GetFirmwareObjectHistoryRequest,
    ) -> Result<rms::GetFirmwareObjectHistoryResponse, RackManagerError> {
        Ok(self.client.get_firmware_object_history(cmd).await?)
    }

    async fn list_switch_firmware(
        &self,
        cmd: rms::ListSwitchFirmwareRequest,
    ) -> Result<rms::ListSwitchFirmwareResponse, RackManagerError> {
        Ok(self.client.list_switch_firmware(cmd).await?)
    }

    async fn push_switch_firmware(
        &self,
        cmd: rms::PushSwitchFirmwareRequest,
    ) -> Result<rms::PushSwitchFirmwareResponse, RackManagerError> {
        Ok(self.client.push_switch_firmware(cmd).await?)
    }

    async fn batch_reset_switch_factory_default(
        &self,
        cmd: rms::BatchResetSwitchFactoryDefaultRequest,
    ) -> Result<rms::BatchResetSwitchFactoryDefaultResponse, RackManagerError> {
        Ok(self.client.batch_reset_switch_factory_default(cmd).await?)
    }

    async fn configure_scale_up_fabric_manager_v2(
        &self,
        cmd: rms_v2::ConfigureScaleUpFabricManagerRequest,
    ) -> Result<rms_v2::ConfigureScaleUpFabricManagerResponse, RackManagerError> {
        self.observed_v2_unary(
            "ConfigureScaleUpFabricManager",
            "rack_manager_v2.ConfigureScaleUpFabricManagerRequest",
            "rack_manager_v2.ConfigureScaleUpFabricManagerResponse",
            cmd,
            |mut client, request| async move {
                client.configure_scale_up_fabric_manager(request).await
            },
        )
        .await
    }

    async fn start_system_validation(
        &self,
        cmd: rms_v2::StartSystemValidationRequest,
    ) -> Result<rms_v2::StartSystemValidationResponse, RackManagerError> {
        self.observed_v2_unary(
            "StartSystemValidation",
            "rack_manager_v2.StartSystemValidationRequest",
            "rack_manager_v2.StartSystemValidationResponse",
            cmd,
            |mut client, request| async move { client.start_system_validation(request).await },
        )
        .await
    }

    async fn configure_scale_up_fabric_manager(
        &self,
        cmd: rms::ConfigureScaleUpFabricManagerRequest,
    ) -> Result<rms::ConfigureScaleUpFabricManagerResponse, RackManagerError> {
        Ok(self.client.configure_scale_up_fabric_manager(cmd).await?)
    }

    async fn get_scale_up_fabric_status(
        &self,
        cmd: rms::GetScaleUpFabricStatusRequest,
    ) -> Result<rms::GetScaleUpFabricStatusResponse, RackManagerError> {
        Ok(self.client.get_scale_up_fabric_status(cmd).await?)
    }

    async fn inspect_scale_up_fabrics(
        &self,
        cmd: rms::InspectScaleUpFabricsRequest,
    ) -> Result<rms::InspectScaleUpFabricsResponse, RackManagerError> {
        Ok(self.client.inspect_scale_up_fabrics(cmd).await?)
    }

    async fn batch_reset_switch_sdn_factory_default(
        &self,
        cmd: rms::BatchResetSwitchSdnFactoryDefaultRequest,
    ) -> Result<rms::BatchResetSwitchSdnFactoryDefaultResponse, RackManagerError> {
        Ok(self
            .client
            .batch_reset_switch_sdn_factory_default(cmd)
            .await?)
    }

    async fn batch_set_scale_up_fabric_state(
        &self,
        cmd: rms::BatchSetScaleUpFabricStateRequest,
    ) -> Result<rms::BatchSetScaleUpFabricStateResponse, RackManagerError> {
        Ok(self.client.batch_set_scale_up_fabric_state(cmd).await?)
    }

    async fn batch_get_scale_up_fabric_service_status(
        &self,
        cmd: rms::BatchGetScaleUpFabricServiceStatusRequest,
    ) -> Result<rms::BatchGetScaleUpFabricServiceStatusResponse, RackManagerError> {
        Ok(self
            .client
            .batch_get_scale_up_fabric_service_status(cmd)
            .await?)
    }

    async fn get_scale_up_fabric_state(
        &self,
        cmd: rms::GetScaleUpFabricStateRequest,
    ) -> Result<rms::GetScaleUpFabricStateResponse, RackManagerError> {
        Ok(self.client.get_scale_up_fabric_state(cmd).await?)
    }

    async fn set_scale_up_fabric_telemetry_interface_state(
        &self,
        cmd: rms::SetScaleUpFabricTelemetryInterfaceStateRequest,
    ) -> Result<rms::SetScaleUpFabricTelemetryInterfaceStateResponse, RackManagerError> {
        Ok(self
            .client
            .set_scale_up_fabric_telemetry_interface_state(cmd)
            .await?)
    }

    async fn configure_switch_certificate(
        &self,
        cmd: rms::ConfigureSwitchCertificateRequest,
    ) -> Result<rms::ConfigureSwitchCertificateResponse, RackManagerError> {
        Ok(self.client.configure_switch_certificate(cmd).await?)
    }

    async fn batch_disable_switch_mtls(
        &self,
        cmd: rms::BatchDisableSwitchMtlsRequest,
    ) -> Result<rms::BatchDisableSwitchMtlsResponse, RackManagerError> {
        Ok(self.client.batch_disable_switch_mtls(cmd).await?)
    }

    async fn get_configure_switch_certificate_job_status(
        &self,
        cmd: rms::GetConfigureSwitchCertificateJobStatusRequest,
    ) -> Result<rms::GetConfigureSwitchCertificateJobStatusResponse, RackManagerError> {
        Ok(self
            .client
            .get_configure_switch_certificate_job_status(cmd)
            .await?)
    }

    async fn list_switch_system_images(
        &self,
        cmd: rms::ListSwitchSystemImagesRequest,
    ) -> Result<rms::ListSwitchSystemImagesResponse, RackManagerError> {
        Ok(self.client.list_switch_system_images(cmd).await?)
    }

    async fn get_switch_system_image_job_status(
        &self,
        cmd: rms::GetSwitchSystemImageJobStatusRequest,
    ) -> Result<rms::GetSwitchSystemImageJobStatusResponse, RackManagerError> {
        Ok(self.client.get_switch_system_image_job_status(cmd).await?)
    }

    async fn update_switch_system_password(
        &self,
        cmd: rms::UpdateSwitchSystemPasswordRequest,
    ) -> Result<rms::UpdateSwitchSystemPasswordResponse, RackManagerError> {
        Ok(self.client.update_switch_system_password(cmd).await?)
    }

    async fn batch_collect_switch_spdm_attestation_evidence(
        &self,
        cmd: rms::BatchCollectSwitchSpdmAttestationEvidenceRequest,
    ) -> Result<rms::BatchCollectSwitchSpdmAttestationEvidenceResponse, RackManagerError> {
        Ok(self
            .client
            .batch_collect_switch_spdm_attestation_evidence(cmd)
            .await?)
    }

    async fn get_version(&self) -> Result<rms::GetVersionResponse, RackManagerError> {
        Ok(self.client.get_version().await?)
    }

    async fn get_firmware_job_status(
        &self,
        cmd: rms::GetFirmwareJobStatusRequest,
    ) -> Result<rms::GetFirmwareJobStatusResponse, RackManagerError> {
        Ok(self.client.get_firmware_job_status(cmd).await?)
    }

    async fn get_job_status(
        &self,
        cmd: rms::GetJobStatusRequest,
    ) -> Result<rms::GetJobStatusResponse, RackManagerError> {
        Ok(self.client.get_job_status(cmd).await?)
    }
}

#[derive(Debug)]
pub struct RmsTlsConnectionProvider {
    pub url: String,
    pub client_config: RmsClientConfig,
    pub retry_config: RetryConfig,
}

#[async_trait::async_trait]
impl tonic_client_wrapper::ConnectionProvider<RackManagerClientT> for RmsTlsConnectionProvider {
    async fn provide_connection(&self) -> Result<RackManagerClientT, Status> {
        let api_config =
            RmsApiConfig::new(&self.url, &self.client_config).with_retry_config(self.retry_config);
        RmsTlsClient::retry_build_rms(&api_config)
            .await
            .map_err(Into::into)
    }

    async fn connection_is_stale(&self, last_connected: SystemTime) -> bool {
        if let Some(ref client_cert) = self.client_config.client_cert {
            if let Ok(mtime) = fs::metadata(&client_cert.cert_path).and_then(|m| m.modified()) {
                if mtime > last_connected {
                    let old_cert_date = DateTime::<Utc>::from(last_connected);
                    let new_cert_date = DateTime::<Utc>::from(mtime);

                    tracing::info!(
                        cert_path = &client_cert.cert_path,
                        %old_cert_date,
                        %new_cert_date,
                        "RmsApiClient: Reconnecting to pick up newer client certificate"
                    );

                    true
                } else {
                    false
                }
            } else if let Ok(mtime) = fs::metadata(&client_cert.key_path).and_then(|m| m.modified())
            {
                // Just in case the cert and key are created some amount of time apart and we
                // last constructed a client with the new cert but the old key...
                if mtime > last_connected {
                    let old_key_date = DateTime::<Utc>::from(last_connected);
                    let new_key_date = DateTime::<Utc>::from(mtime);

                    tracing::info!(
                        key_path = &client_cert.key_path,
                        %old_key_date,
                        %new_key_date,
                        "RmsApiClient: Reconnecting to pick up newer client key"
                    );

                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    fn rpc_observer(&self) -> Option<&dyn tonic_client_wrapper::RpcObserver> {
        self.client_config.rpc_observer.as_deref()
    }

    fn connection_url(&self) -> &str {
        self.url.as_str()
    }
}

/// Supplies V2 clients after the shared RMS endpoint passes a `GetVersion` readiness check.
#[derive(Debug)]
pub struct RmsTlsConnectionProviderV2 {
    /// RMS endpoint used by the V2 client.
    pub url: String,

    /// TLS configuration used by the V2 client.
    pub client_config: RmsClientConfig,

    /// Retry policy applied while establishing a connection.
    pub retry_config: RetryConfig,
}

#[async_trait::async_trait]
impl tonic_client_wrapper::ConnectionProvider<RackManagerV2ClientT> for RmsTlsConnectionProviderV2 {
    async fn provide_connection(&self) -> Result<RackManagerV2ClientT, Status> {
        let api_config =
            RmsApiConfig::new(&self.url, &self.client_config).with_retry_config(self.retry_config);
        RmsTlsClient::retry_build_rms(&api_config)
            .await
            .map_err(Status::from)?;
        RmsTlsClient::new(&self.client_config)
            .build_rms_client_v2(&self.url)
            .map_err(Into::into)
    }

    async fn connection_is_stale(&self, last_connected: SystemTime) -> bool {
        if let Some(ref client_cert) = self.client_config.client_cert {
            if let Ok(mtime) = fs::metadata(&client_cert.cert_path).and_then(|m| m.modified()) {
                if mtime > last_connected {
                    let old_cert_date = DateTime::<Utc>::from(last_connected);
                    let new_cert_date = DateTime::<Utc>::from(mtime);

                    tracing::info!(
                        cert_path = &client_cert.cert_path,
                        %old_cert_date,
                        %new_cert_date,
                        "RmsV2ApiClient: Reconnecting to pick up newer client certificate"
                    );

                    true
                } else {
                    false
                }
            } else if let Ok(mtime) = fs::metadata(&client_cert.key_path).and_then(|m| m.modified())
            {
                // Just in case the cert and key are created some amount of time apart and we
                // last constructed a client with the new cert but the old key...
                if mtime > last_connected {
                    let old_key_date = DateTime::<Utc>::from(last_connected);
                    let new_key_date = DateTime::<Utc>::from(mtime);

                    tracing::info!(
                        key_path = &client_cert.key_path,
                        %old_key_date,
                        %new_key_date,
                        "RmsV2ApiClient: Reconnecting to pick up newer client key"
                    );

                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    fn rpc_observer(&self) -> Option<&dyn tonic_client_wrapper::RpcObserver> {
        self.client_config.rpc_observer.as_deref()
    }

    fn connection_url(&self) -> &str {
        self.url.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tonic_client_wrapper::ConnectionProvider as _;

    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum ApiVersion {
        V1,
        V2,
    }

    async fn failed_connection_attempts(
        api_version: ApiVersion,
        retry_config: RetryConfig,
    ) -> (usize, Duration) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("test listener should have an address")
        );
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = Arc::clone(&attempts);
        let accept_task = tokio::spawn(async move {
            loop {
                let (connection, _) = listener
                    .accept()
                    .await
                    .expect("test listener should accept connections");
                observed_attempts.fetch_add(1, Ordering::Relaxed);
                drop(connection);
            }
        });

        let client_config = RmsClientConfig::new(None, None, None, false);
        let started_at = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            match api_version {
                ApiVersion::V1 => RmsTlsConnectionProvider {
                    url,
                    client_config,
                    retry_config,
                }
                .provide_connection()
                .await
                .map(|_| ()),
                ApiVersion::V2 => RmsTlsConnectionProviderV2 {
                    url,
                    client_config,
                    retry_config,
                }
                .provide_connection()
                .await
                .map(|_| ()),
            }
        })
        .await
        .expect("failed connection should return without hanging");
        let elapsed = started_at.elapsed();
        assert!(result.is_err());

        accept_task.abort();
        assert!(
            accept_task
                .await
                .expect_err("accept task should be cancelled")
                .is_cancelled()
        );
        (attempts.load(Ordering::Relaxed), elapsed)
    }

    #[tokio::test]
    async fn zero_retries_means_one_connection_attempt() {
        for api_version in [ApiVersion::V1, ApiVersion::V2] {
            let (attempts, _) = failed_connection_attempts(
                api_version,
                RetryConfig {
                    retries: 0,
                    interval: Duration::from_secs(1),
                },
            )
            .await;
            assert_eq!(
                attempts, 1,
                "{api_version:?} made more than one connection attempt"
            );
        }
    }

    #[tokio::test]
    async fn retries_after_the_configured_interval() {
        let interval = Duration::from_millis(100);

        for api_version in [ApiVersion::V1, ApiVersion::V2] {
            let (attempts, elapsed) = failed_connection_attempts(
                api_version,
                RetryConfig {
                    retries: 1,
                    interval,
                },
            )
            .await;
            assert_eq!(attempts, 2, "{api_version:?} made the wrong attempt count");
            assert!(
                elapsed >= interval,
                "{api_version:?} retried after {elapsed:?}, before the {interval:?} interval"
            );
        }
    }
}
