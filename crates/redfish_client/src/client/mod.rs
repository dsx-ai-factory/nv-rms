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

use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use nv_redfish::Bmc as _;
use nv_redfish::ServiceRoot;
use nv_redfish::bmc_http::reqwest::{BmcError, Client, ClientParams};
use nv_redfish::bmc_http::{BmcCredentials, CacheSettings, HttpBmc};
use nv_redfish::core::{DataStream, EntityTypeRef, NavProperty, ODataId, UploadReader};
use nv_redfish::resource::{PowerState as RedfishPowerState, ResetType};
use nv_redfish::schema::computer_system::{
    ComputerSystem as ComputerSystemSchema, ComputerSystemResetAction,
};
use nv_redfish::schema::computer_system_collection::ComputerSystemCollection as ComputerSystemCollectionSchema;
use nv_redfish::schema::manager::{Manager as ManagerSchema, ManagerResetAction};
use nv_redfish::schema::manager_collection::ManagerCollection as ManagerCollectionSchema;
use nv_redfish::schema::processor::Processor as ProcessorSchema;
use nv_redfish::schema::processor_collection::ProcessorCollection as ProcessorCollectionSchema;
use nv_redfish::update_service::MultipartUpdateParameters;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::compat::TokioAsyncReadCompatExt as _;
use url::Url;

use crate::error::{RedfishError, Result};
use crate::task::{RedfishTaskResponse, task_id_from_modification_response};
use crate::types::NvidiaMnnvlinkTopology;

type NvRedfishBmc = HttpBmc<Client>;
type NvRedfishServiceRoot = ServiceRoot<NvRedfishBmc>;

static HTTP_CLIENT_REJECTING_INVALID_CERTS: OnceLock<Mutex<Option<Client>>> = OnceLock::new();
static HTTP_CLIENT_ACCEPTING_INVALID_CERTS: OnceLock<Mutex<Option<Client>>> = OnceLock::new();

/// Shared nv-redfish client holder.
///
/// Operation serialization belongs above this client, for example on the node.
/// Constructing this type is cheap: only the underlying reqwest HTTP transport
/// is cached, while endpoint credentials and ServiceRoot state remain attached
/// to each Redfish client instance.
#[derive(Clone)]
pub struct RedfishClient {
    bmc: Arc<NvRedfishBmc>,

    // ServiceRoot discovery is a BMC request. Keep it per RedfishClient
    // instance while still disabling nv-redfish's general HTTP resource cache.
    root: Arc<AsyncMutex<Option<Arc<NvRedfishServiceRoot>>>>,
}

impl RedfishClient {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

    /// Default timeout for Redfish multipart firmware upload requests.
    pub const DEFAULT_UPLOAD_TIMEOUT: Duration = Duration::from_secs(600);

    /// Build a Redfish client from explicit connection data.
    pub fn new(
        host: &str,
        port: u16,
        credentials: BmcCredentials,
        dangerously_accept_invalid_certs: bool,
        https: bool,
    ) -> Result<Self> {
        let endpoint_url = redfish_endpoint_url(host, port, https)?;

        let bmc = HttpBmc::new(
            cached_http_client(dangerously_accept_invalid_certs)?,
            endpoint_url,
            credentials,
            CacheSettings::with_capacity(0),
        );

        Ok(Self {
            bmc: Arc::new(bmc),
            root: Arc::new(AsyncMutex::new(None)),
        })
    }

    /// Clear cached Redfish service discovery data.
    ///
    /// Call this after a known BMC restart or Redfish service restart. Normal
    /// operations also refresh ServiceRoot once after stale collection links
    /// return 404 or 503.
    pub async fn invalidate_cache(&self) {
        *self.root.lock().await = None;
    }

    /// Read a ComputerSystem power state through nv-redfish resource navigation.
    pub async fn computer_system_power_state(
        &self,
        system_id: &str,
    ) -> Result<Option<RedfishPowerState>> {
        let system = self.computer_system_schema(system_id).await?;
        Ok(system.power_state.flatten())
    }

    /// Reset a ComputerSystem using the resource-provided action target.
    pub async fn reset_computer_system(
        &self,
        system_id: &str,
        reset_type: ResetType,
    ) -> Result<()> {
        let system = self.computer_system_schema(system_id).await?;
        let reset = system
            .actions
            .as_ref()
            .and_then(|actions| actions.reset.as_ref())
            .ok_or_else(|| {
                RedfishError::FailedPrecondition(format!(
                    "ComputerSystem {system_id} Reset action is not available"
                ))
            })?;

        let result = reset
            .run(
                self.bmc.as_ref(),
                &ComputerSystemResetAction {
                    reset_type: Some(reset_type),
                },
            )
            .await;

        map_action_result(format!("POST {} via nv-redfish", reset.target), result)
    }

    /// Reset a Manager using the resource-provided action target.
    pub async fn reset_manager(&self, manager_id: &str, reset_type: ResetType) -> Result<()> {
        let manager = self.manager_schema(manager_id).await?;
        let reset = manager
            .actions
            .as_ref()
            .and_then(|actions| actions.reset.as_ref())
            .ok_or_else(|| {
                RedfishError::FailedPrecondition(format!(
                    "Manager {manager_id} Reset action is not available"
                ))
            })?;

        let result = reset
            .run(
                self.bmc.as_ref(),
                &ManagerResetAction {
                    reset_type: Some(reset_type),
                },
            )
            .await;

        self.invalidate_cache().await;

        map_action_result(format!("POST {} via nv-redfish", reset.target), result)
    }

    /// Read NVIDIA MNNVLink topology from a ComputerSystem processor OEM block.
    ///
    /// nv-redfish owns resource navigation. Its schema does not type this OEM
    /// object, so only `Oem.Nvidia.MNNVLinkTopology` is decoded locally.
    pub async fn computer_system_processor_oem_nvidia_mnnvlink_topology(
        &self,
        system_id: &str,
        processor_id: &str,
    ) -> Result<NvidiaMnnvlinkTopology> {
        let processor = self
            .computer_system_processor_schema(system_id, processor_id)
            .await?;

        nvidia_mnnvlink_topology_from_processor(
            &processor,
            &format!("ComputerSystem {system_id} Processor {processor_id}"),
        )?
        .ok_or_else(|| {
            RedfishError::NotFound(format!(
                "MNNVLinkTopology not found in ComputerSystem {system_id} Processor {processor_id}"
            ))
        })
    }

    /// Discover NVIDIA MNNVLink topology from the ComputerSystem processor graph.
    ///
    /// This follows the BMC-advertised Systems and Processors links and returns
    /// the first processor that exposes `Oem.Nvidia.MNNVLinkTopology`.
    pub async fn nvidia_mnnvlink_topology(&self) -> Result<NvidiaMnnvlinkTopology> {
        let systems = self.computer_system_collection_schema().await?;
        let mut first_discovery_error = None;

        for system_ref in &systems.members {
            let system_id = system_ref.id().clone();

            let system = match system_ref.get(self.bmc.as_ref()).await {
                Ok(system) => system,
                Err(error) => {
                    first_discovery_error.get_or_insert_with(|| {
                        map_redfish_error(&format!("GET {system_id} via nv-redfish"), &error)
                    });

                    continue;
                }
            };

            let Some(processors_ref) = system.processors.as_ref() else {
                continue;
            };

            let processors: Arc<ProcessorCollectionSchema> =
                match processors_ref.get(self.bmc.as_ref()).await {
                    Ok(processors) => processors,
                    Err(error) => {
                        first_discovery_error.get_or_insert_with(|| {
                            map_redfish_error(
                                &format!("GET {} via nv-redfish", processors_ref.id()),
                                &error,
                            )
                        });

                        continue;
                    }
                };

            for processor_ref in &processors.members {
                let processor_id = processor_ref.id().clone();

                let processor = match processor_ref.get(self.bmc.as_ref()).await {
                    Ok(processor) => processor,
                    Err(error) => {
                        first_discovery_error.get_or_insert_with(|| {
                            map_redfish_error(&format!("GET {processor_id} via nv-redfish"), &error)
                        });

                        continue;
                    }
                };

                if let Some(topology) = nvidia_mnnvlink_topology_from_processor(
                    &processor,
                    &format!("ComputerSystem {system_id} Processor {processor_id}"),
                )? {
                    return Ok(topology);
                }
            }
        }

        if let Some(error) = first_discovery_error {
            return Err(error);
        }

        Err(RedfishError::NotFound(
            "MNNVLinkTopology not found in any ComputerSystem Processor".to_owned(),
        ))
    }

    /// Upload a firmware stream through Redfish UpdateService multipart update.
    ///
    /// Returns the TaskService task ID from the Redfish response.
    ///
    /// TaskService monitor URIs are normalized to the task ID because RMS
    /// polling still uses the existing NVFWUPD task-status API.
    pub async fn multipart_update_firmware<R>(
        &self,
        update_stream: DataStream<R>,
        targets: Vec<String>,
        force_update: bool,
    ) -> Result<String>
    where
        R: UploadReader,
    {
        self.multipart_update_firmware_with_timeout(
            update_stream,
            targets,
            force_update,
            Self::DEFAULT_UPLOAD_TIMEOUT,
        )
        .await
    }

    /// Upload a firmware stream with a caller-provided multipart request timeout.
    pub async fn multipart_update_firmware_with_timeout<R>(
        &self,
        update_stream: DataStream<R>,
        targets: Vec<String>,
        force_update: bool,
        upload_timeout: Duration,
    ) -> Result<String>
    where
        R: UploadReader,
    {
        if upload_timeout.is_zero() {
            return Err(RedfishError::InvalidArgument(
                "multipart firmware upload timeout must be greater than zero".to_owned(),
            ));
        }

        let parameters = MultipartUpdateParameters::builder()
            .with_force_update(force_update)
            .with_targets(targets)
            .build();

        let root = self.root().await?;

        let update_service = match root
            .update_service()
            .await
            .map_err(|error| map_nv_redfish_error("GET UpdateService via nv-redfish", error))
        {
            Err(error) if should_refresh_service_root(&error) => {
                let root = self.refresh_root(&root).await?;

                root.update_service().await.map_err(|error| {
                    map_nv_redfish_error("GET UpdateService via nv-redfish", error)
                })?
            }
            result => result?,
        }
        .ok_or_else(|| RedfishError::NotFound("Redfish UpdateService not found".to_owned()))?;

        let response = update_service
            .multipart_update_from_reader::<_, _, RedfishTaskResponse>(
                &parameters,
                update_stream,
                upload_timeout,
            )
            .await
            .map_err(|error| {
                map_nv_redfish_error("POST UpdateService multipart update via nv-redfish", error)
            })?;

        task_id_from_modification_response(response).ok_or_else(|| {
            RedfishError::Internal(
                "no task ID in Redfish UpdateService multipart response".to_owned(),
            )
        })
    }

    /// Upload firmware from a local file path.
    pub async fn multipart_update_firmware_from_path(
        &self,
        firmware_path: impl AsRef<Path>,
        targets: Vec<String>,
        force_update: bool,
    ) -> Result<String> {
        self.multipart_update_firmware_from_path_with_timeout(
            firmware_path,
            targets,
            force_update,
            Self::DEFAULT_UPLOAD_TIMEOUT,
        )
        .await
    }

    /// Upload firmware from a local file path with a caller-provided multipart request timeout.
    pub async fn multipart_update_firmware_from_path_with_timeout(
        &self,
        firmware_path: impl AsRef<Path>,
        targets: Vec<String>,
        force_update: bool,
        upload_timeout: Duration,
    ) -> Result<String> {
        if upload_timeout.is_zero() {
            return Err(RedfishError::InvalidArgument(
                "multipart firmware upload timeout must be greater than zero".to_owned(),
            ));
        }

        let firmware_path = firmware_path.as_ref().to_path_buf();
        let file_name = firmware_path
            .file_name()
            .map(|file_name| file_name.to_string_lossy().into_owned())
            .filter(|file_name| !file_name.is_empty())
            .ok_or_else(|| {
                RedfishError::InvalidArgument(format!(
                    "firmware path {} does not include a file name",
                    firmware_path.display()
                ))
            })?;

        let file = tokio::fs::File::open(&firmware_path)
            .await
            .map_err(|source| RedfishError::OpenFirmwareFile {
                path: firmware_path.clone(),
                source,
            })?;

        let content_length = file
            .metadata()
            .await
            .map_err(|source| RedfishError::ReadFirmwareFileMetadata {
                path: firmware_path.clone(),
                source,
            })?
            .len();

        let update_stream =
            DataStream::new(file_name, file.compat()).with_content_length(content_length);

        self.multipart_update_firmware_with_timeout(
            update_stream,
            targets,
            force_update,
            upload_timeout,
        )
        .await
    }

    async fn computer_system_schema(&self, system_id: &str) -> Result<Arc<ComputerSystemSchema>> {
        let conventional_path = ODataId::from(format!("/redfish/v1/Systems/{system_id}"));

        let direct_error = match self.resource_schema_at(&conventional_path).await {
            Ok(system) if computer_system_schema_matches(&system, system_id) => return Ok(system),
            Ok(_) => None,
            Err(error) => Some(error),
        };

        let collection = self.computer_system_collection_schema().await?;
        match self
            .resource_schema_from_collection(
                "ComputerSystem",
                system_id,
                &collection.members,
                computer_system_schema_matches,
            )
            .await
        {
            Ok(system) => Ok(system),
            Err(error) if matches!(&error, RedfishError::NotFound(_)) => {
                Err(direct_error.unwrap_or(error))
            }
            Err(error) => Err(error),
        }
    }

    async fn computer_system_collection_schema(
        &self,
    ) -> Result<Arc<ComputerSystemCollectionSchema>> {
        let root = self.root().await?;

        match self
            .computer_system_collection_schema_from_root(root.as_ref())
            .await
        {
            Err(error) if should_refresh_service_root(&error) => {
                let root = self.refresh_root(&root).await?;

                self.computer_system_collection_schema_from_root(root.as_ref())
                    .await
            }
            result => result,
        }
    }

    async fn computer_system_collection_schema_from_root(
        &self,
        root: &NvRedfishServiceRoot,
    ) -> Result<Arc<ComputerSystemCollectionSchema>> {
        match root.root.systems.as_ref() {
            Some(systems_ref) => systems_ref.get(self.bmc.as_ref()).await.map_err(|error| {
                map_redfish_error(&format!("GET {} via nv-redfish", systems_ref.id()), &error)
            }),
            None => {
                self.resource_schema_at(&ODataId::from("/redfish/v1/Systems".to_owned()))
                    .await
            }
        }
    }

    async fn manager_schema(&self, manager_id: &str) -> Result<Arc<ManagerSchema>> {
        let conventional_path = ODataId::from(format!("/redfish/v1/Managers/{manager_id}"));

        let direct_error = match self.resource_schema_at(&conventional_path).await {
            Ok(manager) if manager_schema_matches(&manager, manager_id) => return Ok(manager),
            Ok(_) => None,
            Err(error) => Some(error),
        };

        let collection = self.manager_collection_schema().await?;
        match self
            .resource_schema_from_collection(
                "Manager",
                manager_id,
                &collection.members,
                manager_schema_matches,
            )
            .await
        {
            Ok(manager) => Ok(manager),
            Err(error) if matches!(&error, RedfishError::NotFound(_)) => {
                Err(direct_error.unwrap_or(error))
            }
            Err(error) => Err(error),
        }
    }

    async fn manager_collection_schema(&self) -> Result<Arc<ManagerCollectionSchema>> {
        let root = self.root().await?;

        match self
            .manager_collection_schema_from_root(root.as_ref())
            .await
        {
            Err(error) if should_refresh_service_root(&error) => {
                let root = self.refresh_root(&root).await?;

                self.manager_collection_schema_from_root(root.as_ref())
                    .await
            }
            result => result,
        }
    }

    async fn manager_collection_schema_from_root(
        &self,
        root: &NvRedfishServiceRoot,
    ) -> Result<Arc<ManagerCollectionSchema>> {
        match root.root.managers.as_ref() {
            Some(managers_ref) => managers_ref.get(self.bmc.as_ref()).await.map_err(|error| {
                map_redfish_error(&format!("GET {} via nv-redfish", managers_ref.id()), &error)
            }),
            None => {
                self.resource_schema_at(&ODataId::from("/redfish/v1/Managers".to_owned()))
                    .await
            }
        }
    }

    async fn computer_system_processor_schema(
        &self,
        system_id: &str,
        processor_id: &str,
    ) -> Result<Arc<ProcessorSchema>> {
        let system = self.computer_system_schema(system_id).await?;

        let processors_ref = system.processors.as_ref().ok_or_else(|| {
            RedfishError::NotFound(format!(
                "Processor collection not found for ComputerSystem {system_id}"
            ))
        })?;

        let processors: Arc<ProcessorCollectionSchema> = processors_ref
            .get(self.bmc.as_ref())
            .await
            .map_err(|error| {
                map_redfish_error(
                    &format!("GET {} via nv-redfish", processors_ref.id()),
                    &error,
                )
            })?;

        self.resource_schema_from_collection(
            &format!("ComputerSystem {system_id} Processor"),
            processor_id,
            &processors.members,
            processor_schema_matches,
        )
        .await
    }

    async fn resource_schema_at<T>(&self, path: &ODataId) -> Result<Arc<T>>
    where
        T: EntityTypeRef + for<'de> Deserialize<'de> + 'static,
    {
        self.bmc
            .get(path)
            .await
            .map_err(|error| map_redfish_error(&format!("GET {path} via nv-redfish"), &error))
    }

    async fn resource_schema_from_collection<T>(
        &self,
        resource_type: &str,
        resource_id: &str,
        members: &[NavProperty<T>],
        matches_resource: fn(&T, &str) -> bool,
    ) -> Result<Arc<T>>
    where
        T: EntityTypeRef + for<'de> Deserialize<'de> + 'static,
    {
        let mut target_error = None;

        for member in members {
            let member_id = member.id().clone();
            let result = member.get(self.bmc.as_ref()).await;

            match result {
                Ok(resource) if matches_resource(&resource, resource_id) => return Ok(resource),
                Ok(_) => {}
                Err(error) if member_id.last_segment() == Some(resource_id) => {
                    let context = format!("GET {member_id} via nv-redfish");
                    target_error.get_or_insert_with(|| map_redfish_error(&context, &error));
                }
                Err(_) => {}
            }
        }

        if let Some(error) = target_error {
            return Err(error);
        }

        Err(RedfishError::NotFound(format!(
            "{resource_type} {resource_id} not found"
        )))
    }

    async fn root(&self) -> Result<Arc<NvRedfishServiceRoot>> {
        let mut cached_root = self.root.lock().await;

        if let Some(root) = cached_root.as_ref() {
            return Ok(Arc::clone(root));
        }

        let root = self.fetch_root().await?;
        *cached_root = Some(Arc::clone(&root));

        Ok(root)
    }

    async fn refresh_root(
        &self,
        stale_root: &Arc<NvRedfishServiceRoot>,
    ) -> Result<Arc<NvRedfishServiceRoot>> {
        let mut cached_root = self.root.lock().await;

        if let Some(root) = cached_root.as_ref()
            && !Arc::ptr_eq(root, stale_root)
        {
            return Ok(Arc::clone(root));
        }

        let root = self.fetch_root().await?;
        *cached_root = Some(Arc::clone(&root));

        Ok(root)
    }

    async fn fetch_root(&self) -> Result<Arc<NvRedfishServiceRoot>> {
        ServiceRoot::new(Arc::clone(&self.bmc))
            .await
            .map(Arc::new)
            .map_err(|error| map_nv_redfish_error("GET /redfish/v1 via nv-redfish", error))
    }
}

fn cached_http_client(dangerously_accept_invalid_certs: bool) -> Result<Client> {
    let cache = if dangerously_accept_invalid_certs {
        &HTTP_CLIENT_ACCEPTING_INVALID_CERTS
    } else {
        &HTTP_CLIENT_REJECTING_INVALID_CERTS
    };

    let mut cache = cache.get_or_init(|| Mutex::new(None)).lock().map_err(|_| {
        RedfishError::Internal("Redfish HTTP client cache lock poisoned".to_owned())
    })?;

    if let Some(client) = cache.as_ref() {
        return Ok(client.clone());
    }

    let client = Client::with_params(client_params(dangerously_accept_invalid_certs))
        .map_err(|source| RedfishError::CreateHttpClient { source })?;

    *cache = Some(client.clone());

    Ok(client)
}

fn client_params(dangerously_accept_invalid_certs: bool) -> ClientParams {
    ClientParams::new()
        .timeout(RedfishClient::REQUEST_TIMEOUT)
        .connect_timeout(RedfishClient::CONNECT_TIMEOUT)
        .accept_invalid_certs(dangerously_accept_invalid_certs)
}

fn redfish_endpoint_url(host: &str, port: u16, https: bool) -> Result<Url> {
    let scheme = if https { "https" } else { "http" };

    let authority = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };

    Url::parse(&format!("{scheme}://{authority}:{port}")).map_err(|source| {
        RedfishError::InvalidEndpoint {
            host: host.to_owned(),
            port,
            source,
        }
    })
}

fn computer_system_schema_matches(system: &ComputerSystemSchema, expected_id: &str) -> bool {
    system.base.id == expected_id || system.odata_id().last_segment() == Some(expected_id)
}

fn manager_schema_matches(manager: &ManagerSchema, expected_id: &str) -> bool {
    manager.base.id == expected_id || manager.odata_id().last_segment() == Some(expected_id)
}

fn processor_schema_matches(processor: &ProcessorSchema, expected_id: &str) -> bool {
    processor.base.id == expected_id || processor.odata_id().last_segment() == Some(expected_id)
}

fn nvidia_mnnvlink_topology_from_processor(
    processor: &ProcessorSchema,
    context: &str,
) -> Result<Option<NvidiaMnnvlinkTopology>> {
    let Some(topology) = processor
        .base
        .base
        .oem
        .as_ref()
        .and_then(|oem| oem.additional_properties.get("Nvidia"))
        .and_then(|nvidia| nvidia.get("MNNVLinkTopology"))
        .cloned()
    else {
        return Ok(None);
    };

    serde_json::from_value(topology).map(Some).map_err(|error| {
        RedfishError::InvalidArgument(format!("MNNVLinkTopology invalid in {context}: {error}"))
    })
}

fn map_action_result<T>(context: String, result: std::result::Result<T, BmcError>) -> Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(error) => Err(map_redfish_error(&context, &error)),
    }
}

fn should_refresh_service_root(error: &RedfishError) -> bool {
    matches!(
        error,
        RedfishError::NotFound(_) | RedfishError::Unavailable(_)
    )
}

fn map_redfish_error(context: &str, error: &BmcError) -> RedfishError {
    match error {
        BmcError::ReqwestError(error) => {
            let message = format!("{context}: {error}");

            if error.is_timeout() {
                RedfishError::Timeout(message)
            } else if error.is_connect() {
                let reqwest_message = error.to_string().to_ascii_lowercase();

                if reqwest_message.contains("dns") || reqwest_message.contains("resolve") {
                    RedfishError::DnsResolutionFailed(message)
                } else {
                    RedfishError::ConnectionRefused(message)
                }
            } else if error.is_request() {
                RedfishError::Unavailable(message)
            } else {
                RedfishError::Internal(message)
            }
        }
        BmcError::InvalidResponse { status, .. } => {
            let message = format!("{context}: HTTP {}", status.as_u16());

            match status.as_u16() {
                401 => RedfishError::Unauthenticated(message),
                403 => RedfishError::Forbidden(message),
                404 => RedfishError::NotFound(message),
                408 => RedfishError::Timeout(message),
                409 => RedfishError::AlreadyExists(message),
                502 => RedfishError::Unavailable(message),
                503 => RedfishError::Unavailable(message),
                504 => RedfishError::Unavailable(message),
                _ => RedfishError::Internal(message),
            }
        }
        BmcError::InvalidRequest(message) => {
            RedfishError::InvalidArgument(format!("{context}: {message}"))
        }
        _ => RedfishError::Internal(format!("{context}: {error}")),
    }
}

fn map_nv_redfish_error(context: &str, error: nv_redfish::Error<NvRedfishBmc>) -> RedfishError {
    match error {
        nv_redfish::Error::Bmc(error) => map_redfish_error(context, &error),
        nv_redfish::Error::ActionNotAvailable => {
            RedfishError::FailedPrecondition(format!("{context}: action is not available"))
        }
        nv_redfish::Error::UpdateServiceMultipartHttpPushUriNotAvailable => {
            RedfishError::FailedPrecondition(format!(
                "{context}: UpdateService MultipartHttpPushUri is not available"
            ))
        }
        nv_redfish::Error::Json(error) => RedfishError::Internal(format!("{context}: {error}")),
    }
}

#[cfg(test)]
mod tests;
