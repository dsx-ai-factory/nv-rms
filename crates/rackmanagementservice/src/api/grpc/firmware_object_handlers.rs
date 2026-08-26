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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use super::server::RackManagerServiceImpl;
use crate::api::grpc::artifact_download::{
    build_artifact_http_client, download_single_file, is_valid_sha256_hex,
};
use crate::api::grpc::conversions::{
    domain_node_type_to_proto, flatten_node_info, proto_node_type_to_domain,
    proto_node_type_to_string, timestamp_from_datetime,
};
use crate::api::grpc::firmware_artifact_paths::{
    ArtifactPathError, artifact_cache_path, filename_from_location, redact_location_for_logging,
};
use crate::api::grpc::firmware_handlers::{
    FirmwareTargetExpectedVersions, build_ephemeral_node,
    build_firmware_targets_from_list_with_expected_versions, spawn_firmware_update_job,
};
use crate::api::grpc::node_type_resolver::{
    domain_node_type_to_descriptor, resolve_component_filter_selectors,
    resolve_firmware_target_selectors, resolve_node_info,
};
use crate::api::grpc::switch_image_handlers::{
    create_switch_system_image_node, run_switch_system_image_job,
};
use crate::domain::node::{ExpectedInventoryPolicy, NodeKind, NodeType};
use crate::domain::rack::NodeConfig;
use crate::nodes::compute_gb300_supermicro::{
    SUPERMICRO_BIOS_TARGET, SUPERMICRO_HGX_TARGET, supermicro_bmc_filename,
};
use crate::nodes::switch_gb200_nvidia::infer_target_build_id;
use crate::orchestrator::job_lifecycle::{
    CleanupPlan, JobDomain, JobError, JobFailure, JobId, JobRegistry, JobSpec,
};
use crate::orchestrator::job_tracker::{JobType, RmsJobHandle, job_failure};
use crate::persistence::{
    FirmwareObject, FirmwareObjectSearchFilter, FirmwareObjectStore, RackHardwareType,
};
use crate::utilities::error::RmsError;
use librms::protos::rack_manager as rm;

const FIRMWARE_OBJECT_CACHE_SUBDIR: &str = "firmware_objects";
const EPHEMERAL_FIRMWARE_OBJECT_CACHE_SUBDIR: &str = "firmware_objects_ephemeral";
const COMPUTE_LOOKUP_KEY: &str = "Compute Node";
const SWITCH_LOOKUP_KEY: &str = "Switch Tray";
const POWER_SHELF_LOOKUP_KEY: &str = "Power Shelf";
const VRNVL72_COMPUTE_LOOKUP_KEY: &str = "VRNVL72 Compute Node";
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ParsedFirmwareComponents {
    #[serde(default)]
    board_skus: Vec<BoardSkuFirmware>,
    #[serde(default)]
    switch_system_images: Vec<SwitchSystemImageArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BoardSkuFirmware {
    sku_id: String,
    name: String,
    sku_type: String,
    firmware_components: Vec<FirmwareComponent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirmwareComponent {
    component: String,
    bundle: Option<String>,
    version: Option<String>,
    component_type: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    context: String,
    locations: Vec<FirmwareLocation>,
    subcomponents: Vec<FirmwareSubComponent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirmwareSubComponent {
    component: String,
    version: String,
    skuid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirmwareLocation {
    location: String,
    location_type: String,
    firmware_type: Option<String>,
    /// Optional SHA-256 hex digest advertised by the firmware manifest and verified after download.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    context: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SwitchSystemImageArtifact {
    device_type: String,
    component: String,
    version: String,
    firmware_type: String,
    package_name: String,
    location: String,
    location_type: String,
    required: bool,
    image_filename: String,
    /// Optional SHA-256 hex digest advertised by the firmware manifest and verified after download.
    sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirmwareLookupTable {
    #[serde(default)]
    devices: HashMap<String, HashMap<String, FirmwareLookupEntry>>,
    #[serde(default)]
    switch_system_images: HashMap<String, HashMap<String, SwitchSystemImageLookupEntry>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirmwareLookupEntry {
    filename: String,
    target: String,
    component: String,
    bundle: String,
    firmware_type: String,
    version: Option<String>,
    subcomponents: Vec<FirmwareSubComponent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SwitchSystemImageLookupEntry {
    component: String,
    package_name: String,
    version: String,
    image_filename: String,
    location_type: String,
    firmware_type: String,
    required: bool,
}

#[derive(Debug, Clone)]
struct DownloadArtifact {
    url: String,
    location_type: String,
    component: String,
    bundle: Option<String>,
    required: bool,
    relative_cache_dir: Option<String>,
    sha256: Option<String>,
}

/// Identifying details for a single firmware artifact download, kept outside the
/// spawned task's future so it survives success, failure, cancellation, and
/// panics/aborts (which only surface a `JoinError`, not the task's own state).
#[derive(Debug, Clone, Default)]
struct DownloadContext {
    component: String,
    bundle: Option<String>,
    location_type: String,
    url: String,
    required: bool,
}

impl From<&DownloadArtifact> for DownloadContext {
    fn from(artifact: &DownloadArtifact) -> Self {
        Self {
            component: artifact.component.clone(),
            bundle: artifact.bundle.clone(),
            location_type: artifact.location_type.clone(),
            url: artifact.url.clone(),
            required: artifact.required,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeviceType {
    ComputeGb200Nvidia,
    ComputeGb300Nvidia,
    ComputeVrnvl72Nvidia,
    SwitchGb200Nvidia,
    SwitchGb300Nvidia,
    SwitchVrnvl72Nvidia,
    PowerShelf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirmwareObjectArtifactScope {
    FirmwareTargets,
    SwitchSystemImages,
    All,
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_add_firmware_object(
        &self,
        req: Request<rm::AddFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::AddFirmwareObjectResponse>, Status> {
        let r = req.into_inner();
        let config: Value = serde_json::from_str(&r.config_json)
            .map_err(|e| Status::invalid_argument(format!("invalid config_json: {e}")))?;
        let id = firmware_object_id_from_config(&config).map_err(Status::invalid_argument)?;

        if r.hardware_type.is_empty() {
            return Err(Status::invalid_argument("hardware_type is required"));
        }

        let parsed =
            parse_firmware_object_config_value(&id, &config).map_err(Status::invalid_argument)?;
        validate_firmware_object_sha256(&parsed).map_err(Status::invalid_argument)?;

        let lookup_table = build_firmware_lookup_table(&parsed);
        let parsed_value =
            if lookup_table.devices.is_empty() && lookup_table.switch_system_images.is_empty() {
                None
            } else {
                Some(serde_json::to_value(&lookup_table).map_err(|e| {
                    Status::internal(format!("failed to serialize parsed components: {e}"))
                })?)
            };

        let hw_type = RackHardwareType(r.hardware_type);
        let mut firmware = self
            .backends
            .firmware_objects
            .create(&id, hw_type.clone(), config, parsed_value)
            .await
            .map_err(status_from_rms_error)?;

        let should_set_default = r.set_default
            || !self
                .backends
                .firmware_objects
                .has_default(&hw_type)
                .await
                .map_err(status_from_rms_error)?;
        if should_set_default {
            firmware = self
                .backends
                .firmware_objects
                .set_default(&id)
                .await
                .map_err(status_from_rms_error)?;
        }

        if !parsed.board_skus.is_empty() || !parsed.switch_system_images.is_empty() {
            let cancel = CancellationToken::new();

            self.firmware_download_cancellations
                .lock()
                .unwrap_or_else(|error| {
                    tracing::warn!("firmware_download_cancellations lock poisoned; recovering");
                    error.into_inner()
                })
                .insert(id.clone(), cancel.clone());

            spawn_firmware_download_task(
                id.clone(),
                parsed,
                optional_access_token(r.access_token),
                self.firmware_dir.clone(),
                self.backends.firmware_objects.clone(),
                cancel,
                self.firmware_download_cancellations.clone(),
            );
        }

        Ok(Response::new(rm::AddFirmwareObjectResponse {
            object: Some(firmware_object_to_proto(firmware)),
        }))
    }

    pub(crate) async fn handle_get_firmware_object(
        &self,
        req: Request<rm::GetFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::GetFirmwareObjectResponse>, Status> {
        let r = req.into_inner();
        let firmware = self
            .backends
            .firmware_objects
            .find_by_id(&r.id)
            .await
            .map_err(status_from_rms_error)?;
        Ok(Response::new(rm::GetFirmwareObjectResponse {
            object: Some(firmware_object_to_proto(firmware)),
        }))
    }

    pub(crate) async fn handle_list_firmware_objects(
        &self,
        req: Request<rm::ListFirmwareObjectsRequest>,
    ) -> std::result::Result<Response<rm::ListFirmwareObjectsResponse>, Status> {
        let r = req.into_inner();
        let filter = FirmwareObjectSearchFilter {
            only_available: r.only_available,
            rack_hardware_type: (!r.hardware_type.is_empty())
                .then_some(RackHardwareType(r.hardware_type)),
        };
        let objects = self
            .backends
            .firmware_objects
            .list(filter)
            .await
            .map_err(status_from_rms_error)?
            .into_iter()
            .map(firmware_object_to_proto)
            .collect();
        Ok(Response::new(rm::ListFirmwareObjectsResponse { objects }))
    }

    pub(crate) async fn handle_delete_firmware_object(
        &self,
        req: Request<rm::DeleteFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::DeleteFirmwareObjectResponse>, Status> {
        let r = req.into_inner();
        validate_firmware_object_id(&r.id).map_err(Status::invalid_argument)?;
        cancel_firmware_download(&self.firmware_download_cancellations, &r.id);
        self.backends
            .firmware_objects
            .delete(&r.id)
            .await
            .map_err(status_from_rms_error)?;

        let cache_dir = firmware_object_cache_dir(&self.firmware_dir, &r.id);
        if let Err(e) = tokio::fs::remove_dir_all(&cache_dir).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                object_id = %r.id,
                path = %cache_dir.display(),
                error = %e,
                "failed to remove firmware object cache directory"
            );
        }

        Ok(Response::new(rm::DeleteFirmwareObjectResponse {
            response: Some(rm::OperationResponse {
                status: rm::ReturnCode::Success.into(),
                message: format!("deleted firmware object {}", r.id),
            }),
        }))
    }

    pub(crate) async fn handle_set_default_firmware_object(
        &self,
        req: Request<rm::SetDefaultFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::SetDefaultFirmwareObjectResponse>, Status> {
        let r = req.into_inner();
        let firmware = self
            .backends
            .firmware_objects
            .set_default(&r.object_id)
            .await
            .map_err(status_from_rms_error)?;
        Ok(Response::new(rm::SetDefaultFirmwareObjectResponse {
            object: Some(firmware_object_to_proto(firmware)),
        }))
    }

    pub(crate) async fn handle_apply_stored_firmware_object(
        &self,
        req: Request<rm::ApplyStoredFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::ApplyStoredFirmwareObjectResponse>, Status> {
        let r = req.into_inner();
        let firmware = self
            .resolve_requested_firmware(&r.object_id, &r.hardware_type)
            .await?;
        let node_ids = request_node_ids(r.nodes.as_ref());
        let mut node_jobs = Vec::new();
        let response = if !firmware.available {
            batch_failure(format!("firmware object {} is not available", firmware.id))
        } else {
            match self.build_update_firmware_request(&r, &firmware) {
                Ok(update_request) => {
                    let expected_versions =
                        match firmware_target_expected_versions_from_firmware_manifest(
                            &firmware,
                            &update_request,
                        ) {
                            Ok(expected_versions) => expected_versions,
                            Err(error) => {
                                return Ok(Response::new(rm::ApplyStoredFirmwareObjectResponse {
                                    response: Some(batch_failure(error)),
                                    object_id: firmware.id,
                                    jobs: node_jobs,
                                }));
                            }
                        };
                    let update_response = self
                        .handle_batch_update_firmware_with_expected_versions(
                            Request::new(update_request),
                            expected_versions,
                        )
                        .await?
                        .into_inner();
                    node_jobs = update_response.jobs;
                    let response = update_response.response.unwrap_or_else(|| {
                        batch_failure("BatchUpdateFirmware returned an empty response".to_owned())
                    });
                    self.record_apply_if_success(ApplyHistoryParams {
                        response: &response,
                        object_id: &firmware.id,
                        rack_id: &r.rack_id,
                        update_type: &r.firmware_type,
                        hardware_type: firmware.rack_hardware_type.clone(),
                        node_ids: &node_ids,
                        operation: "firmware_object_apply",
                    })
                    .await;
                    response
                }
                Err(e) => batch_failure(e),
            }
        };

        Ok(Response::new(rm::ApplyStoredFirmwareObjectResponse {
            response: Some(response),
            object_id: firmware.id,
            jobs: node_jobs,
        }))
    }

    pub(crate) async fn handle_apply_firmware_object(
        &self,
        req: Request<rm::ApplyFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::ApplyFirmwareObjectResponse>, Status> {
        let r = req.into_inner();
        if r.hardware_type.is_empty() {
            return Err(Status::invalid_argument("hardware_type is required"));
        }

        let devices = request_devices(r.nodes.as_ref());
        if devices.is_empty() {
            let response = batch_failure("No devices specified in request".to_owned());
            return Ok(Response::new(rm::ApplyFirmwareObjectResponse {
                response: Some(response),
                object_id: best_effort_firmware_object_id(&r.config_json),
                jobs: Vec::new(),
            }));
        }

        let object_id = best_effort_firmware_object_id(&r.config_json);
        let Some(parent_job_id) = self
            .job_tracker
            .create_parent_job(&r.rack_id, JobType::FirmwareUpdate)
        else {
            let response =
                batch_failure("failed to create parent firmware object apply job".to_owned());
            return Ok(Response::new(rm::ApplyFirmwareObjectResponse {
                response: Some(response),
                object_id,
                jobs: Vec::new(),
            }));
        };
        let precreated = self.precreate_firmware_jobs(parent_job_id, devices);
        if precreated.jobs.is_empty() {
            let response = rm::NodeBatchResponse {
                status: rm::ReturnCode::Failure.into(),
                message: "No firmware object apply jobs created".to_owned(),
                node_results: precreated.node_results,
                job_id: precreated.parent_job_id.clone(),
                stats: Some(rm::NodeOperationStats {
                    total_nodes: precreated.total_nodes,
                    successful_nodes: 0,
                    failed_nodes: precreated.failed_nodes,
                }),
            };

            return Ok(Response::new(rm::ApplyFirmwareObjectResponse {
                response: Some(response),
                object_id,
                jobs: Vec::new(),
            }));
        }

        let response = accepted_batch_with_stats(
            precreated.parent_job_id.clone(),
            precreated.total_nodes,
            precreated.jobs.len() as u32,
            precreated.failed_nodes,
            precreated.node_results,
            "Created firmware object apply jobs. Stage 0 downloads firmware artifacts; use GetFirmwareJobStatus with response.job_id or node job IDs to track progress.",
        );

        let node_jobs = precreated.node_jobs;
        self.spawn_apply_firmware_object_job(r, precreated.jobs);

        Ok(Response::new(rm::ApplyFirmwareObjectResponse {
            response: Some(response),
            object_id,
            jobs: node_jobs,
        }))
    }

    pub(crate) async fn handle_apply_stored_switch_system_image(
        &self,
        req: Request<rm::ApplyStoredSwitchSystemImageRequest>,
    ) -> std::result::Result<Response<rm::ApplyStoredSwitchSystemImageResponse>, Status> {
        let r = req.into_inner();
        let firmware = self
            .resolve_requested_firmware(&r.object_id, &r.hardware_type)
            .await?;
        let node_ids = request_node_ids(r.nodes.as_ref());

        if !firmware.available {
            let response =
                batch_failure(format!("firmware object {} is not available", firmware.id));
            return Ok(Response::new(rm::ApplyStoredSwitchSystemImageResponse {
                response: Some(response),
                object_id: firmware.id,
                image_filename: String::new(),
                jobs: Vec::new(),
            }));
        }

        let image = match resolve_switch_system_image(&firmware, &r.software_type) {
            Ok(image) => image,
            Err(e) => {
                let response = batch_failure(e);
                return Ok(Response::new(rm::ApplyStoredSwitchSystemImageResponse {
                    response: Some(response),
                    object_id: firmware.id,
                    image_filename: String::new(),
                    jobs: Vec::new(),
                }));
            }
        };

        let cache_dir = firmware_object_cache_dir(&self.firmware_dir, &firmware.id);

        let local_file_path = match switch_system_image_cache_path(&cache_dir, &image) {
            Ok(path) => path.to_string_lossy().into_owned(),
            Err(e) => {
                let response = batch_failure(e.to_string());
                return Ok(Response::new(rm::ApplyStoredSwitchSystemImageResponse {
                    response: Some(response),
                    object_id: firmware.id,
                    image_filename: String::new(),
                    jobs: Vec::new(),
                }));
            }
        };

        let update_response = self
            .handle_update_switch_system_image(Request::new(rm::UpdateSwitchSystemImageRequest {
                nodes: r.nodes,
                image_filename: image.image_filename.clone(),
                local_file_path,
            }))
            .await?
            .into_inner();

        let response = update_response.response.unwrap_or_else(|| {
            batch_failure("UpdateSwitchSystemImage returned an empty response".to_owned())
        });

        self.record_apply_if_success(ApplyHistoryParams {
            response: &response,
            object_id: &firmware.id,
            rack_id: &r.rack_id,
            update_type: &r.software_type,
            hardware_type: firmware.rack_hardware_type.clone(),
            node_ids: &node_ids,
            operation: "switch_system_image_apply",
        })
        .await;

        Ok(Response::new(rm::ApplyStoredSwitchSystemImageResponse {
            response: Some(response),
            object_id: firmware.id,
            image_filename: image.image_filename,
            jobs: update_response.jobs,
        }))
    }

    pub(crate) async fn handle_apply_switch_system_image(
        &self,
        req: Request<rm::ApplySwitchSystemImageRequest>,
    ) -> std::result::Result<Response<rm::ApplySwitchSystemImageResponse>, Status> {
        let r = req.into_inner();
        if r.hardware_type.is_empty() {
            return Err(Status::invalid_argument("hardware_type is required"));
        }

        let devices = request_devices(r.nodes.as_ref());
        if devices.is_empty() {
            let response = batch_failure("No devices specified in request".to_owned());
            return Ok(Response::new(rm::ApplySwitchSystemImageResponse {
                response: Some(response),
                object_id: best_effort_firmware_object_id(&r.config_json),
                image_filename: String::new(),
                jobs: Vec::new(),
            }));
        }

        let object_id = best_effort_firmware_object_id(&r.config_json);
        let Some(parent_job_id) = self
            .job_tracker
            .create_parent_job(&r.rack_id, JobType::SwitchSystemImageUpdate)
        else {
            let response =
                batch_failure("failed to create parent switch system image apply job".to_owned());
            return Ok(Response::new(rm::ApplySwitchSystemImageResponse {
                response: Some(response),
                object_id,
                image_filename: String::new(),
                jobs: Vec::new(),
            }));
        };
        let (parent_job_id, precreated_jobs, node_jobs) =
            self.precreate_switch_image_jobs(parent_job_id, devices);

        // Every per-device job was rejected (e.g. all nodes busy or the tracker
        // is at capacity); fail the batch instead of launching stage-0 work for
        // an empty job set and reporting it as accepted.
        if precreated_jobs.is_empty() {
            let mut response =
                batch_failure("No switch system image apply jobs created".to_owned());
            response.job_id = parent_job_id.clone();
            return Ok(Response::new(rm::ApplySwitchSystemImageResponse {
                response: Some(response),
                object_id,
                image_filename: String::new(),
                jobs: Vec::new(),
            }));
        }

        self.spawn_apply_switch_system_image_job(r, precreated_jobs);

        let response = accepted_batch(
            parent_job_id,
            node_jobs.len() as u32,
            "Created switch system image apply jobs. Stage 0 downloads the image artifact; use GetSwitchSystemImageJobStatus with response.job_id or node job IDs to track progress.",
        );

        Ok(Response::new(rm::ApplySwitchSystemImageResponse {
            response: Some(response),
            object_id,
            image_filename: String::new(),
            jobs: node_jobs,
        }))
    }

    pub(crate) async fn handle_get_firmware_object_history(
        &self,
        req: Request<rm::GetFirmwareObjectHistoryRequest>,
    ) -> std::result::Result<Response<rm::GetFirmwareObjectHistoryResponse>, Status> {
        let r = req.into_inner();
        let object_id = (!r.object_id.is_empty()).then_some(r.object_id.as_str());
        let records = self
            .backends
            .firmware_objects
            .list_apply_history(object_id, &r.rack_ids)
            .await
            .map_err(status_from_rms_error)?
            .into_iter()
            .map(|record| rm::FirmwareObjectHistoryRecord {
                object_id: record.object_id,
                rack_id: record.rack_id,
                firmware_type: record.firmware_type,
                applied_at: Some(timestamp_from_datetime(record.applied_at)),
                firmware_available: record.firmware_available,
                hardware_type: record.rack_hardware_type.0,
                node_ids: record.node_ids,
            })
            .collect();
        Ok(Response::new(rm::GetFirmwareObjectHistoryResponse {
            records,
        }))
    }

    async fn resolve_requested_firmware(
        &self,
        object_id: &str,
        hardware_type: &str,
    ) -> std::result::Result<FirmwareObject, Status> {
        if object_id.is_empty() {
            if hardware_type.is_empty() {
                return Err(Status::invalid_argument(
                    "hardware_type is required when object_id is empty",
                ));
            }
            return self
                .backends
                .firmware_objects
                .find_default_by_hw_type(&RackHardwareType(hardware_type.to_owned()))
                .await
                .map_err(status_from_rms_error);
        }

        let firmware = self
            .backends
            .firmware_objects
            .find_by_id(object_id)
            .await
            .map_err(status_from_rms_error)?;
        if !hardware_type.is_empty()
            && firmware.rack_hardware_type.0 != hardware_type
            && firmware.rack_hardware_type != RackHardwareType::any()
        {
            return Err(Status::failed_precondition(format!(
                "firmware object {} hardware type {} does not match request hardware type {}",
                firmware.id, firmware.rack_hardware_type, hardware_type
            )));
        }
        Ok(firmware)
    }

    async fn record_apply_if_success(&self, params: ApplyHistoryParams<'_>) {
        if !node_batch_succeeded(params.response) {
            return;
        }
        let _ = self
            .backends
            .firmware_objects
            .record_apply(
                params.object_id,
                params.rack_id,
                params.update_type,
                params.hardware_type,
                params.node_ids,
            )
            .await
            .map_err(|e| {
                tracing::warn!(
                    operation = params.operation,
                    error = %e.message,
                    "failed to record firmware object apply history"
                )
            });
    }

    fn precreate_firmware_jobs(
        &self,
        parent_job_id: String,
        devices: Vec<rm::NodeInfo>,
    ) -> PrecreatedFirmwareJobs {
        let mut precreated_jobs = Vec::with_capacity(devices.len());
        let mut node_jobs = Vec::with_capacity(devices.len());
        let mut node_results = Vec::new();
        let total_nodes = devices.len() as u32;
        let mut failed_nodes = 0u32;

        for device in devices {
            let expected_inventory = match self.expected_inventory_policy_for_node(&device) {
                Ok(policy) => policy,
                Err(error) => {
                    node_results.push(rm::NodeOperationResult {
                        node_id: device.node_id,
                        status: rm::ReturnCode::Failure.into(),
                        error_message: error.message,
                    });
                    failed_nodes += 1;
                    continue;
                }
            };
            let pending = match self.job_tracker.create_child_job_if_node_idle(
                &parent_job_id,
                &device.rack_id,
                &device.node_id,
                JobType::FirmwareUpdate,
            ) {
                Ok(pending) => pending,
                Err(failure) => {
                    tracing::warn!(
                        node = device.node_id,
                        rack = device.rack_id,
                        message = %failure.message,
                        "firmware object apply job rejected"
                    );

                    node_results.push(rm::NodeOperationResult {
                        node_id: device.node_id,
                        status: rm::ReturnCode::Failure.into(),
                        error_message: failure.message,
                    });

                    failed_nodes += 1;
                    continue;
                }
            };

            let job_id = pending.id().to_string();
            node_jobs.push(rm::NodeFirmwareJobInfo {
                node_id: device.node_id.clone(),
                job_id,
            });
            precreated_jobs.push(PrecreatedFirmwareJob {
                device,
                pending,
                expected_inventory,
            });
        }

        if precreated_jobs.is_empty() {
            self.job_tracker
                .mark_failed_message(&parent_job_id, "No firmware object apply jobs created");
        }

        PrecreatedFirmwareJobs {
            parent_job_id,
            jobs: precreated_jobs,
            node_jobs,
            node_results,
            total_nodes,
            failed_nodes,
        }
    }

    fn precreate_switch_image_jobs(
        &self,
        parent_job_id: String,
        devices: Vec<rm::NodeInfo>,
    ) -> (
        String,
        Vec<PrecreatedSwitchImageJob>,
        Vec<rm::SwitchSystemImageUpdateJobInfo>,
    ) {
        let mut precreated_jobs = Vec::with_capacity(devices.len());
        let mut node_jobs = Vec::with_capacity(devices.len());

        for device in devices {
            let pending = match self.job_tracker.create_child_job(
                &parent_job_id,
                &device.rack_id,
                &device.node_id,
                JobType::SwitchSystemImageUpdate,
            ) {
                Ok(pending) => pending,
                Err(failure) => {
                    tracing::warn!(
                        node = device.node_id,
                        rack = device.rack_id,
                        message = %failure.message,
                        "switch image apply job rejected"
                    );
                    continue;
                }
            };
            let job_id = pending.id().to_string();
            node_jobs.push(rm::SwitchSystemImageUpdateJobInfo {
                node_id: device.node_id.clone(),
                job_id,
            });
            precreated_jobs.push(PrecreatedSwitchImageJob { device, pending });
        }

        if precreated_jobs.is_empty() {
            self.job_tracker
                .mark_failed_message(&parent_job_id, "No switch system image apply jobs created");
        }

        (parent_job_id, precreated_jobs, node_jobs)
    }

    fn spawn_apply_firmware_object_job(
        &self,
        request: rm::ApplyFirmwareObjectRequest,
        jobs: Vec<PrecreatedFirmwareJob>,
    ) {
        let service = self.clone();
        tokio::spawn(async move {
            service.run_apply_firmware_object_job(request, jobs).await;
        });
    }

    async fn run_apply_firmware_object_job(
        self,
        request: rm::ApplyFirmwareObjectRequest,
        jobs: Vec<PrecreatedFirmwareJob>,
    ) {
        mark_firmware_jobs_running(
            &self.job_tracker,
            &jobs,
            "Stage 0: parsing firmware object JSON",
        );
        let mut parsed_config = match parse_ephemeral_firmware_object_config(
            &request.config_json,
            &request.hardware_type,
            FirmwareObjectArtifactScope::FirmwareTargets,
        ) {
            Ok(parsed_config) => parsed_config,
            Err(e) => {
                mark_firmware_jobs_failed(&self.job_tracker, jobs, JobError::InvalidArgument, &e);
                return;
            }
        };

        if let Err(e) = filter_parsed_components_for_apply_with_descriptors(
            &mut parsed_config.parsed,
            request.nodes.as_ref(),
            &request.firmware_type,
            &request.component_filters,
            &request.node_descriptor_component_filters,
            &[],
        ) {
            mark_firmware_jobs_failed(&self.job_tracker, jobs, JobError::InvalidArgument, &e);
            return;
        }
        if let Err(e) = validate_required_download_locations(&parsed_config.parsed) {
            mark_firmware_jobs_failed(&self.job_tracker, jobs, JobError::InvalidArgument, &e);
            return;
        }

        mark_firmware_jobs_running(
            &self.job_tracker,
            &jobs,
            "Stage 0: downloading firmware object artifacts",
        );
        let cache = self.ephemeral_firmware_cache(&parsed_config.id);
        let firmware = match self
            .build_ephemeral_firmware_object_from_parsed(
                parsed_config,
                optional_access_token(request.access_token.clone()),
                &cache.object_id,
            )
            .await
        {
            Ok(firmware) => firmware,
            Err(e) => {
                let child_job_ids = firmware_job_ids(&jobs);
                mark_firmware_jobs_failed(&self.job_tracker, jobs, JobError::ClientError, &e);
                spawn_ephemeral_object_cache_cleanup(
                    self.job_tracker.registry(),
                    child_job_ids,
                    cache.dir,
                );
                return;
            }
        };

        mark_firmware_jobs_running(
            &self.job_tracker,
            &jobs,
            "Resolving firmware object targets",
        );
        let update_request = match self.build_update_firmware_request_from_json(
            &request,
            &firmware,
            &cache.object_id,
        ) {
            Ok(update_request) => update_request,
            Err(e) => {
                let child_job_ids = firmware_job_ids(&jobs);
                mark_firmware_jobs_failed(&self.job_tracker, jobs, JobError::InvalidResponse, &e);
                spawn_ephemeral_object_cache_cleanup(
                    self.job_tracker.registry(),
                    child_job_ids,
                    cache.dir,
                );
                return;
            }
        };

        let expected_versions = match firmware_target_expected_versions_from_firmware_manifest(
            &firmware,
            &update_request,
        ) {
            Ok(expected_versions) => expected_versions,
            Err(e) => {
                let child_job_ids = firmware_job_ids(&jobs);
                mark_firmware_jobs_failed(&self.job_tracker, jobs, JobError::InvalidResponse, &e);
                spawn_ephemeral_object_cache_cleanup(
                    self.job_tracker.registry(),
                    child_job_ids,
                    cache.dir,
                );
                return;
            }
        };

        let child_job_ids = firmware_job_ids(&jobs);
        self.dispatch_precreated_firmware_jobs(update_request, jobs, expected_versions)
            .await;
        spawn_ephemeral_object_cache_cleanup(self.job_tracker.registry(), child_job_ids, cache.dir);
    }

    fn spawn_apply_switch_system_image_job(
        &self,
        request: rm::ApplySwitchSystemImageRequest,
        jobs: Vec<PrecreatedSwitchImageJob>,
    ) {
        let service = self.clone();
        tokio::spawn(async move {
            service
                .run_apply_switch_system_image_job(request, jobs)
                .await;
        });
    }

    async fn run_apply_switch_system_image_job(
        self,
        request: rm::ApplySwitchSystemImageRequest,
        jobs: Vec<PrecreatedSwitchImageJob>,
    ) {
        mark_switch_jobs_running(
            &self.job_tracker,
            &jobs,
            "Stage 0: parsing firmware object JSON",
        );
        let parsed_config = match parse_ephemeral_firmware_object_config(
            &request.config_json,
            &request.hardware_type,
            FirmwareObjectArtifactScope::SwitchSystemImages,
        ) {
            Ok(parsed_config) => parsed_config,
            Err(e) => {
                mark_switch_jobs_failed(&self.job_tracker, jobs, &e);
                return;
            }
        };

        mark_switch_jobs_running(
            &self.job_tracker,
            &jobs,
            "Stage 0: downloading switch image artifact",
        );
        let cache = self.ephemeral_firmware_cache(&parsed_config.id);
        let firmware = match self
            .build_ephemeral_firmware_object_from_parsed(
                parsed_config,
                optional_access_token(request.access_token.clone()),
                &cache.object_id,
            )
            .await
        {
            Ok(firmware) => firmware,
            Err(e) => {
                let child_job_ids = switch_image_job_ids(&jobs);
                mark_switch_jobs_failed(&self.job_tracker, jobs, &e);
                spawn_ephemeral_object_cache_cleanup(
                    self.job_tracker.registry(),
                    child_job_ids,
                    cache.dir,
                );
                return;
            }
        };

        mark_switch_jobs_running(&self.job_tracker, &jobs, "Resolving switch system image");
        let image = match resolve_switch_system_image(&firmware, &request.software_type) {
            Ok(image) => image,
            Err(e) => {
                let child_job_ids = switch_image_job_ids(&jobs);
                mark_switch_jobs_failed(&self.job_tracker, jobs, &e);
                spawn_ephemeral_object_cache_cleanup(
                    self.job_tracker.registry(),
                    child_job_ids,
                    cache.dir,
                );
                return;
            }
        };

        let local_file_path = match switch_system_image_cache_path(&cache.dir, &image) {
            Ok(path) => path.to_string_lossy().into_owned(),
            Err(e) => {
                let message = e.to_string();
                let child_job_ids = switch_image_job_ids(&jobs);
                mark_switch_jobs_failed(&self.job_tracker, jobs, &message);
                spawn_ephemeral_object_cache_cleanup(
                    self.job_tracker.registry(),
                    child_job_ids,
                    cache.dir,
                );
                return;
            }
        };

        let child_job_ids = switch_image_job_ids(&jobs);
        self.dispatch_precreated_switch_image_jobs(jobs, image.image_filename, local_file_path)
            .await;
        spawn_ephemeral_object_cache_cleanup(self.job_tracker.registry(), child_job_ids, cache.dir);
    }

    async fn dispatch_precreated_firmware_jobs(
        &self,
        r: rm::BatchUpdateFirmwareRequest,
        jobs: Vec<PrecreatedFirmwareJob>,
        expected_versions: FirmwareTargetExpectedVersions,
    ) {
        if r.firmware_targets.is_empty() && r.node_descriptor_firmware_targets.is_empty() {
            mark_firmware_jobs_failed(
                &self.job_tracker,
                jobs,
                JobError::InvalidResponse,
                "No firmware targets specified in request",
            );
            return;
        }

        let target_lists = match resolve_firmware_target_selectors(
            &r.firmware_targets,
            &r.node_descriptor_firmware_targets,
        ) {
            Ok(target_lists) => target_lists,
            Err(error) => {
                mark_firmware_jobs_failed(
                    &self.job_tracker,
                    jobs,
                    JobError::FileNotFound,
                    &format!("Failed to resolve firmware targets: {error}"),
                );
                return;
            }
        };

        let mut resolved: HashMap<NodeType, Vec<crate::domain::node::FirmwareTarget>> =
            HashMap::new();
        for (node_type, target_list) in target_lists {
            match build_firmware_targets_from_list_with_expected_versions(
                node_type,
                &target_list.targets,
                &self.firmware_dir,
                expected_versions.get(&node_type),
            ) {
                Ok(targets) if targets.is_empty() => {
                    mark_firmware_jobs_failed(
                        &self.job_tracker,
                        jobs,
                        JobError::FileNotFound,
                        &format!(
                            "Failed to resolve firmware targets for node type {}: empty list",
                            node_type.as_str()
                        ),
                    );
                    return;
                }
                Ok(targets) => {
                    resolved.insert(node_type, targets);
                }
                Err(e) => {
                    mark_firmware_jobs_failed(
                        &self.job_tracker,
                        jobs,
                        JobError::FileNotFound,
                        &format!(
                            "Failed to resolve firmware targets for node type {}: {e}",
                            node_type.as_str()
                        ),
                    );
                    return;
                }
            }
        }

        let mut refresh_child_id = None;
        for job in jobs {
            let Ok(node_type) = resolve_node_info(&job.device) else {
                fail_precreated_child(
                    job.pending,
                    job_failure(JobError::TargetNotFound, "Unknown or missing node type"),
                    &mut refresh_child_id,
                );
                continue;
            };

            let Some(targets) = resolved.get(&node_type).cloned() else {
                fail_precreated_child(
                    job.pending,
                    job_failure(
                        JobError::TargetNotFound,
                        format!(
                            "No firmware targets provided for node type {}",
                            node_type.as_str()
                        ),
                    ),
                    &mut refresh_child_id,
                );
                continue;
            };

            let flat = match flatten_node_info(&job.device) {
                Ok(flat) => flat,
                Err(e) => {
                    let message = e.message;
                    tracing::error!(
                        job_id = %job.pending.id(),
                        node = job.device.node_id,
                        node_type = node_type.as_str(),
                        error = message,
                        "invalid endpoint credentials"
                    );

                    fail_precreated_child(
                        job.pending,
                        job_failure(JobError::InvalidArgument, message),
                        &mut refresh_child_id,
                    );

                    continue;
                }
            };

            if flat.creds_for_node_type(node_type).is_none() {
                let source = if node_type.kind() == NodeKind::Switch {
                    "host"
                } else {
                    "BMC"
                };
                fail_precreated_child(
                    job.pending,
                    job_failure(
                        JobError::InvalidArgument,
                        format!(
                            "Missing {source} credentials for device {}",
                            job.device.node_id
                        ),
                    ),
                    &mut refresh_child_id,
                );
                continue;
            }

            let node = match build_ephemeral_node(
                &job.device,
                node_type,
                job.expected_inventory.clone(),
            ) {
                Ok(node) => node,
                Err(e) => {
                    let message = e.message;
                    fail_precreated_child(
                        job.pending,
                        job_failure(
                            JobError::ClientError,
                            format!(
                                "Failed to construct node for type {}: {message}",
                                node_type.as_str()
                            ),
                        ),
                        &mut refresh_child_id,
                    );

                    continue;
                }
            };

            if let Err(e) = self.initialize_nvue_client(node.nvue_client(), None).await {
                fail_precreated_child(
                    job.pending,
                    job_failure(JobError::ClientError, e.message),
                    &mut refresh_child_id,
                );

                continue;
            }

            spawn_firmware_update_job(
                self.job_tracker.clone(),
                job.pending,
                node,
                targets,
                r.activate,
                r.force_update,
            );
        }
        refresh_parent_after_failures(&self.job_tracker, refresh_child_id);
    }

    async fn dispatch_precreated_switch_image_jobs(
        &self,
        jobs: Vec<PrecreatedSwitchImageJob>,
        image_filename: String,
        local_file_path: String,
    ) {
        if image_filename.is_empty() {
            mark_switch_jobs_failed(&self.job_tracker, jobs, "image_filename is required");
            return;
        }
        if !Path::new(&local_file_path).is_file() {
            mark_switch_jobs_failed(
                &self.job_tracker,
                jobs,
                &format!("local_file_path does not exist or is not a file: {local_file_path}"),
            );
            return;
        }
        let target_build_id = match infer_target_build_id(&image_filename) {
            Ok(target_build_id) => target_build_id,
            Err(e) => {
                mark_switch_jobs_failed(&self.job_tracker, jobs, &e.message);
                return;
            }
        };

        let mut refresh_child_id = None;
        for job in jobs {
            let node_type_key = job.device.r#type.unwrap_or(0);
            let Some(node_type) = proto_node_type_to_domain(node_type_key) else {
                fail_precreated_child(
                    job.pending,
                    switch_job_failure(format!(
                        "device {} is not a switch (type={})",
                        job.device.node_id,
                        proto_node_type_to_string(node_type_key).unwrap_or("unknown")
                    )),
                    &mut refresh_child_id,
                );
                continue;
            };
            if node_type.kind() != NodeKind::Switch {
                fail_precreated_child(
                    job.pending,
                    switch_job_failure(format!(
                        "device {} is not a switch (type={})",
                        job.device.node_id,
                        proto_node_type_to_string(node_type_key).unwrap_or("unknown")
                    )),
                    &mut refresh_child_id,
                );
                continue;
            }

            let flat = match flatten_node_info(&job.device) {
                Ok(flat) => flat,
                Err(e) => {
                    let message = e.message;
                    tracing::error!(
                        job_id = %job.pending.id(),
                        node = job.device.node_id,
                        rack = job.device.rack_id,
                        error = message,
                        "invalid endpoint credentials"
                    );

                    fail_precreated_child(
                        job.pending,
                        switch_job_failure(format!(
                            "Invalid credentials for switch {}: {}",
                            job.device.node_id, message
                        )),
                        &mut refresh_child_id,
                    );

                    continue;
                }
            };

            let Some((user, pass)) = flat.creds_for_node_type(node_type) else {
                fail_precreated_child(
                    job.pending,
                    switch_job_failure(format!(
                        "Missing host credentials for switch {}",
                        job.device.node_id
                    )),
                    &mut refresh_child_id,
                );
                continue;
            };
            let user = user.to_owned();
            let pass = pass.to_owned();

            // Applying a switch system image is a direct host workflow. The
            // host endpoint is required for NVUE/SSH, while a BMC endpoint is
            // optional and only preserved if the caller supplied valid data.
            // Malformed BMC endpoint configuration is dropped with a warning
            // rather than failing the job.
            let bmc_endpoint = match flat.optional_bmc_endpoint() {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    tracing::warn!(
                        job_id = %job.pending.id(),
                        node = job.device.node_id,
                        rack = job.device.rack_id,
                        error = e.message,
                        "ignoring malformed BMC endpoint for switch image apply"
                    );
                    None
                }
            };
            let host_endpoint = match flat.switch_host_management_endpoint() {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    let message = e.message;
                    fail_precreated_child(
                        job.pending,
                        switch_job_failure(format!(
                            "Invalid host endpoint for switch {}: {}",
                            job.device.node_id, message
                        )),
                        &mut refresh_child_id,
                    );

                    continue;
                }
            };
            let config = NodeConfig {
                id: job.device.node_id.clone(),
                node_type,
                bmc_endpoint,
                host_endpoint: Some(host_endpoint),
                expected_inventory: None,
            };
            let rack_id = job.device.rack_id.clone();
            let node_id = job.device.node_id.clone();

            let switch_result = create_switch_system_image_node(&config, &rack_id);
            let switch = match switch_result {
                Ok(switch) => switch,
                Err(e) => {
                    fail_precreated_child(
                        job.pending,
                        switch_job_failure(format!("Failed to construct switch: {}", e.message)),
                        &mut refresh_child_id,
                    );
                    continue;
                }
            };

            if let Err(e) = self
                .initialize_nvue_client(switch.nvue_client(), None)
                .await
            {
                fail_precreated_child(
                    job.pending,
                    switch_job_failure(e.message),
                    &mut refresh_child_id,
                );

                continue;
            }

            let switch = Arc::new(tokio::sync::Mutex::new(switch));

            let pending = job.pending;
            let cancel = pending.cancellation_token();

            let job_tracker = self.job_tracker.clone();
            let image_filename = image_filename.clone();
            let local_file_path = local_file_path.clone();
            let target_build_id = target_build_id.clone();
            let switch_username = user;
            let switch_password = pass;
            let sftp_upload_options = self.sftp_upload_options;

            // The switch workflow seals its own job through the tracked handle.
            // The tracked supervisor adds panic/abort handling around it.
            job_tracker
                .spawn_job(pending, move |job| async move {
                    run_switch_system_image_job(
                        job,
                        switch,
                        &image_filename,
                        &local_file_path,
                        &target_build_id,
                        &switch_username,
                        &switch_password,
                        &rack_id,
                        &node_id,
                        sftp_upload_options,
                        &cancel,
                    )
                    .await;
                })
                .detach();
        }
        refresh_parent_after_failures(&self.job_tracker, refresh_child_id);
    }

    async fn build_ephemeral_firmware_object_from_parsed(
        &self,
        parsed_config: EphemeralFirmwareObjectConfig,
        access_token: Option<String>,
        cache_object_id: &str,
    ) -> std::result::Result<FirmwareObject, String> {
        validate_required_download_locations(&parsed_config.parsed)?;
        let cancel = CancellationToken::new();
        let lookup_table = download_firmware_artifacts(
            cache_object_id,
            &parsed_config.parsed,
            access_token,
            &self.firmware_dir,
            EPHEMERAL_FIRMWARE_OBJECT_CACHE_SUBDIR,
            &cancel,
        )
        .await
        .map_err(|e| format!("failed to download artifacts: {e}"))?;
        let parsed_components = Some(
            serde_json::to_value(lookup_table)
                .map_err(|e| format!("failed to serialize parsed components: {e}"))?,
        );
        let now = Utc::now();

        Ok(FirmwareObject {
            id: parsed_config.id,
            rack_hardware_type: RackHardwareType(parsed_config.hardware_type),
            available: true,
            is_default: false,
            config: parsed_config.config,
            parsed_components,
            created: now,
            updated: now,
        })
    }

    fn ephemeral_firmware_cache(&self, object_id: &str) -> EphemeralFirmwareCache {
        let object_id = ephemeral_cache_object_id(object_id);
        let dir = firmware_object_cache_dir_for_subdir(
            &self.firmware_dir,
            EPHEMERAL_FIRMWARE_OBJECT_CACHE_SUBDIR,
            &object_id,
        );
        EphemeralFirmwareCache { object_id, dir }
    }

    // Keep accepting deprecated global component filters for existing clients.
    #[allow(deprecated)]
    fn build_update_firmware_request(
        &self,
        r: &rm::ApplyStoredFirmwareObjectRequest,
        firmware: &FirmwareObject,
    ) -> std::result::Result<rm::BatchUpdateFirmwareRequest, String> {
        self.build_update_firmware_request_inner(FirmwareObjectApplyParams {
            nodes: r.nodes.clone(),
            firmware_type: &r.firmware_type,
            force_update: r.force_update,
            component_filters: &r.component_filters,
            descriptor_component_filters: &r.node_descriptor_component_filters,
            global_components: &r.components,
            firmware,
            cache_object_id: &firmware.id,
            cache_subdir: FIRMWARE_OBJECT_CACHE_SUBDIR,
        })
    }

    fn build_update_firmware_request_from_json(
        &self,
        r: &rm::ApplyFirmwareObjectRequest,
        firmware: &FirmwareObject,
        cache_object_id: &str,
    ) -> std::result::Result<rm::BatchUpdateFirmwareRequest, String> {
        self.build_update_firmware_request_inner(FirmwareObjectApplyParams {
            nodes: r.nodes.clone(),
            firmware_type: &r.firmware_type,
            force_update: r.force_update,
            component_filters: &r.component_filters,
            descriptor_component_filters: &r.node_descriptor_component_filters,
            global_components: &[],
            firmware,
            cache_object_id,
            cache_subdir: EPHEMERAL_FIRMWARE_OBJECT_CACHE_SUBDIR,
        })
    }

    fn build_update_firmware_request_inner(
        &self,
        params: FirmwareObjectApplyParams<'_>,
    ) -> std::result::Result<rm::BatchUpdateFirmwareRequest, String> {
        let nodes = params.nodes.ok_or_else(|| "nodes is required".to_owned())?;
        if nodes.nodes.is_empty() {
            return Err("nodes must include at least one device".to_owned());
        }

        let mut present_types = HashSet::new();
        for device in &nodes.nodes {
            let node_type = resolve_node_info(device).map_err(|error| error.to_string())?;
            present_types.insert(node_type);
        }

        let component_filters = resolve_component_filter_selectors(
            params.component_filters,
            params.descriptor_component_filters,
        )
        .map_err(|error| error.to_string())?;

        let parsed = parsed_lookup_table(params.firmware)?;
        let mut firmware_targets = HashMap::new();
        let mut descriptor_firmware_targets = Vec::new();
        for (node_type, lookup_key) in firmware_node_type_lookup_keys() {
            if !present_types.contains(&node_type) {
                continue;
            }
            let selection = match selection_for_node_type(
                &component_filters,
                params.global_components,
                node_type,
            )? {
                Some(selection) => selection,
                None => continue,
            };
            let targets = build_firmware_targets(
                &parsed,
                node_type,
                lookup_key,
                params.firmware_type,
                params.cache_object_id,
                params.cache_subdir,
                &selection,
            )?;
            let target_list = rm::FirmwareTargetList { targets };
            let proto_type = domain_node_type_to_proto(node_type);
            if proto_type == rm::NodeType::Unspecified {
                descriptor_firmware_targets.push(rm::NodeDescriptorFirmwareTargetList {
                    node_descriptor: Some(domain_node_type_to_descriptor(node_type)),
                    firmware_targets: Some(target_list),
                });
            } else {
                firmware_targets.insert(proto_type as i32, target_list);
            }
        }

        if firmware_targets.is_empty() && descriptor_firmware_targets.is_empty() {
            let types: Vec<_> = present_types
                .iter()
                .map(|node_type| node_type.as_str())
                .collect();
            return Err(format!(
                "no firmware object targets found for supplied node types: {}",
                types.join(", ")
            ));
        }

        Ok(rm::BatchUpdateFirmwareRequest {
            nodes: Some(nodes),
            firmware_targets,
            node_descriptor_firmware_targets: descriptor_firmware_targets,
            activate: true,
            force_update: params.force_update,
        })
    }
}

struct FirmwareObjectApplyParams<'a> {
    nodes: Option<rm::NodeSet>,
    firmware_type: &'a str,
    force_update: bool,
    component_filters: &'a HashMap<i32, rm::FirmwareObjectComponentFilter>,
    descriptor_component_filters: &'a [rm::NodeDescriptorFirmwareObjectComponentFilter],
    global_components: &'a [String],
    firmware: &'a FirmwareObject,
    cache_object_id: &'a str,
    cache_subdir: &'a str,
}

struct ApplyHistoryParams<'a> {
    response: &'a rm::NodeBatchResponse,
    object_id: &'a str,
    rack_id: &'a str,
    update_type: &'a str,
    hardware_type: RackHardwareType,
    node_ids: &'a [String],
    operation: &'static str,
}

struct PrecreatedFirmwareJobs {
    parent_job_id: String,
    jobs: Vec<PrecreatedFirmwareJob>,
    node_jobs: Vec<rm::NodeFirmwareJobInfo>,
    node_results: Vec<rm::NodeOperationResult>,
    total_nodes: u32,
    failed_nodes: u32,
}

struct PrecreatedFirmwareJob {
    device: rm::NodeInfo,
    pending: RmsJobHandle,
    expected_inventory: Option<ExpectedInventoryPolicy>,
}

struct PrecreatedSwitchImageJob {
    device: rm::NodeInfo,
    pending: RmsJobHandle,
}

struct EphemeralFirmwareObjectConfig {
    id: String,
    hardware_type: String,
    config: Value,
    parsed: ParsedFirmwareComponents,
}

struct EphemeralFirmwareCache {
    object_id: String,
    dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FirmwareTargetMapping {
    source_component: &'static str,
    target_key: &'static str,
    redfish_target: &'static str,
    default_apply: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct FirmwareObjectComponentSelection {
    mode: FirmwareObjectComponentSelectionMode,
    components: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum FirmwareObjectComponentSelectionMode {
    #[default]
    DefaultOnly,
    All,
    Components,
}

impl FirmwareObjectComponentSelection {
    fn default_only() -> Self {
        Self {
            mode: FirmwareObjectComponentSelectionMode::DefaultOnly,
            components: Vec::new(),
        }
    }

    fn all() -> Self {
        Self {
            mode: FirmwareObjectComponentSelectionMode::All,
            components: Vec::new(),
        }
    }

    fn components(components: Vec<String>) -> Self {
        Self {
            mode: FirmwareObjectComponentSelectionMode::Components,
            components,
        }
    }

    fn matches_mapping(
        &self,
        component: &FirmwareComponent,
        mapping: &FirmwareTargetMapping,
    ) -> bool {
        if component.component != mapping.source_component {
            return false;
        }

        self.matches_target_mapping(mapping)
    }

    fn matches_target_mapping(&self, mapping: &FirmwareTargetMapping) -> bool {
        match self.mode {
            FirmwareObjectComponentSelectionMode::DefaultOnly => mapping.default_apply,
            FirmwareObjectComponentSelectionMode::All => true,
            FirmwareObjectComponentSelectionMode::Components => selected_firmware_mapping_matches(
                mapping.source_component,
                mapping.target_key,
                mapping.redfish_target,
                mapping.default_apply,
                &self.components,
            ),
        }
    }
}

fn firmware_node_type_lookup_keys() -> [(NodeType, &'static str); 13] {
    [
        (NodeType::ComputeGb200Nvidia, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb200Wiwynn, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb300Nvidia, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb300Lenovo, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb300Supermicro, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeVrnvl72Nvidia, COMPUTE_LOOKUP_KEY),
        (NodeType::PowershelfGb200Liteon, POWER_SHELF_LOOKUP_KEY),
        (NodeType::PowershelfGb200Delta, POWER_SHELF_LOOKUP_KEY),
        (NodeType::PowershelfGb300Liteon, POWER_SHELF_LOOKUP_KEY),
        (NodeType::PowershelfGb300Delta, POWER_SHELF_LOOKUP_KEY),
        (NodeType::SwitchGb200Nvidia, SWITCH_LOOKUP_KEY),
        (NodeType::SwitchGb300Nvidia, SWITCH_LOOKUP_KEY),
        (NodeType::SwitchVrnvl72Nvidia, SWITCH_LOOKUP_KEY),
    ]
}

fn selection_for_node_type(
    component_filters: &HashMap<NodeType, &rm::FirmwareObjectComponentFilter>,
    global_components: &[String],
    node_type: NodeType,
) -> std::result::Result<Option<FirmwareObjectComponentSelection>, String> {
    if component_filters.is_empty() {
        return Ok(Some(if global_components.is_empty() {
            FirmwareObjectComponentSelection::default_only()
        } else {
            FirmwareObjectComponentSelection::components(global_components.to_vec())
        }));
    }

    component_filters
        .get(&node_type)
        .map(|filter| component_filter_selection(node_type, filter))
        .transpose()
}

fn component_filter_selection(
    _node_type: NodeType,
    filter: &rm::FirmwareObjectComponentFilter,
) -> std::result::Result<FirmwareObjectComponentSelection, String> {
    // Keep node_type in this boundary for future validation of per-node target keys.
    // Component filters currently accept source component names and target keys, so the
    // actual narrowing remains lookup-table driven until those inputs are made stricter.
    Ok(if filter.components.is_empty() {
        FirmwareObjectComponentSelection::all()
    } else {
        FirmwareObjectComponentSelection::components(filter.components.clone())
    })
}

// Narrows raw firmware manifest data for one-shot apply before artifact download. Board Type
// chooses the device role; the request node type chooses the concrete mapping.
#[cfg(test)]
fn filter_parsed_components_for_apply(
    parsed: &mut ParsedFirmwareComponents,
    nodes: Option<&rm::NodeSet>,
    firmware_type: &str,
    component_filters: &HashMap<i32, rm::FirmwareObjectComponentFilter>,
    global_components: &[String],
) -> std::result::Result<(), String> {
    filter_parsed_components_for_apply_with_descriptors(
        parsed,
        nodes,
        firmware_type,
        component_filters,
        &[],
        global_components,
    )
}

fn filter_parsed_components_for_apply_with_descriptors(
    parsed: &mut ParsedFirmwareComponents,
    nodes: Option<&rm::NodeSet>,
    firmware_type: &str,
    component_filters: &HashMap<i32, rm::FirmwareObjectComponentFilter>,
    descriptor_component_filters: &[rm::NodeDescriptorFirmwareObjectComponentFilter],
    global_components: &[String],
) -> std::result::Result<(), String> {
    let component_filters =
        resolve_component_filter_selectors(component_filters, descriptor_component_filters)
            .map_err(|error| error.to_string())?;

    // One-shot apply downloads only the requested firmware manifest components.
    let selection =
        firmware_download_selection_resolved(nodes, &component_filters, global_components)?;
    if selection.nodes_by_lookup_key.is_empty() {
        parsed.board_skus.clear();
        parsed.switch_system_images.clear();
        return Ok(());
    }

    let wanted_type = firmware_type.to_ascii_lowercase();
    parsed.board_skus.retain_mut(|board_sku| {
        let lookup_keys = lookup_keys_for_board_sku(board_sku);

        board_sku.firmware_components.retain_mut(|component| {
            let matching_mappings =
                matching_download_mappings(component, &lookup_keys, &selection.nodes_by_lookup_key);
            if matching_mappings.is_empty() {
                return false;
            }

            if let Some(component_type) = component
                .component_type
                .as_deref()
                .filter(|component_type| !component_type.trim().is_empty())
                && !component_type.eq_ignore_ascii_case(&wanted_type)
            {
                return false;
            }

            let retain_all_payloads = matching_mappings.iter().any(|(node_type, mapping)| {
                firmware_download_mapping_requires_all_payloads(*node_type, mapping)
            });

            retain_downloadable_locations(component, retain_all_payloads)
        });

        !board_sku.firmware_components.is_empty()
    });

    parsed.switch_system_images.clear();
    Ok(())
}

#[derive(Debug, Default)]
struct FirmwareDownloadSelection {
    nodes_by_lookup_key: HashMap<&'static str, Vec<FirmwareNodeSelection>>,
}

#[derive(Debug)]
struct FirmwareNodeSelection {
    node_type: NodeType,
    components: FirmwareObjectComponentSelection,
}

fn firmware_download_selection_resolved(
    nodes: Option<&rm::NodeSet>,
    component_filters: &HashMap<NodeType, &rm::FirmwareObjectComponentFilter>,
    global_components: &[String],
) -> std::result::Result<FirmwareDownloadSelection, String> {
    let mut present_types = HashSet::new();
    if let Some(nodes) = nodes {
        for node in &nodes.nodes {
            let node_type = resolve_node_info(node).map_err(|error| error.to_string())?;
            present_types.insert(node_type);
        }
    }

    let mut selection = FirmwareDownloadSelection::default();
    for (node_type, lookup_key) in firmware_node_type_lookup_keys() {
        if !present_types.contains(&node_type) {
            continue;
        }
        let Some(component_selection) =
            selection_for_node_type(component_filters, global_components, node_type)?
        else {
            continue;
        };
        selection
            .nodes_by_lookup_key
            .entry(lookup_key)
            .or_default()
            .push(FirmwareNodeSelection {
                node_type,
                components: component_selection,
            });
    }

    Ok(selection)
}

fn lookup_key_from_board_type(board_type: &str) -> Option<&'static str> {
    let board_type = board_type.trim();
    if board_type.eq_ignore_ascii_case("Compute Tray")
        || board_type.eq_ignore_ascii_case(COMPUTE_LOOKUP_KEY)
    {
        Some(COMPUTE_LOOKUP_KEY)
    } else if board_type.eq_ignore_ascii_case(SWITCH_LOOKUP_KEY) {
        Some(SWITCH_LOOKUP_KEY)
    } else if board_type.eq_ignore_ascii_case("PowerShelf")
        || board_type.eq_ignore_ascii_case(POWER_SHELF_LOOKUP_KEY)
    {
        Some(POWER_SHELF_LOOKUP_KEY)
    } else {
        None
    }
}

fn lookup_keys_for_board_sku(board_sku: &BoardSkuFirmware) -> Vec<&'static str> {
    let Some(primary) = lookup_key_from_board_type(&board_sku.sku_type) else {
        return Vec::new();
    };

    let mut lookup_keys = vec![primary];
    // Some release catalogs carry powershelf payloads under a compute board.
    // Keep recognizing those source components without binding the compute
    // board to a particular product family.
    if primary == COMPUTE_LOOKUP_KEY {
        lookup_keys.push(POWER_SHELF_LOOKUP_KEY);
    }
    lookup_keys
}

fn matching_download_mappings(
    component: &FirmwareComponent,
    lookup_keys: &[&'static str],
    selections: &HashMap<&'static str, Vec<FirmwareNodeSelection>>,
) -> Vec<(NodeType, FirmwareTargetMapping)> {
    let mut matches = Vec::new();
    for lookup_key in lookup_keys {
        let Some(node_selections) = selections.get(lookup_key) else {
            continue;
        };
        for node_selection in node_selections {
            for mapping in get_firmware_component_mappings_for_node_type(node_selection.node_type) {
                if !firmware_component_allowed_for_node_type(
                    node_selection.node_type,
                    mapping.target_key,
                ) || !node_selection
                    .components
                    .matches_mapping(component, &mapping)
                {
                    continue;
                }
                if !matches.contains(&(node_selection.node_type, mapping)) {
                    matches.push((node_selection.node_type, mapping));
                }
            }
        }
    }
    matches
}

fn selected_firmware_mapping_matches(
    source_component: &str,
    lookup_key: &str,
    target: &str,
    allow_source_component_match: bool,
    selected: &[String],
) -> bool {
    let mut candidates = vec![lookup_key.to_ascii_lowercase(), target.to_ascii_lowercase()];
    if allow_source_component_match {
        candidates.push(source_component.to_ascii_lowercase());
    }
    selected.iter().any(|wanted| {
        let wanted = wanted.to_ascii_lowercase();
        candidates.iter().any(|candidate| candidate == &wanted)
    })
}

fn retain_downloadable_locations(component: &mut FirmwareComponent, retain_all: bool) -> bool {
    let mut locations = Vec::new();
    for location in &component.locations {
        let is_payload = location_is_firmware_payload(location.firmware_type.as_deref());
        let is_missing_required_firmware = location
            .firmware_type
            .as_deref()
            .is_some_and(|ty| ty.eq_ignore_ascii_case("Firmware"))
            && location.location.trim().is_empty();
        if is_payload
            && (is_missing_required_firmware || filename_from_location(&location.location).is_ok())
        {
            locations.push(location.clone());
            if !retain_all {
                break;
            }
        }
    }

    // Non-opt-in paths keep existing behavior: first payload only.
    if locations.is_empty() {
        component.locations.clear();
        return false;
    }

    component.locations = locations;
    true
}

fn parse_ephemeral_firmware_object_config(
    config_json: &str,
    hardware_type: &str,
    scope: FirmwareObjectArtifactScope,
) -> std::result::Result<EphemeralFirmwareObjectConfig, String> {
    if hardware_type.is_empty() {
        return Err("hardware_type is required".to_owned());
    }
    let config: Value =
        serde_json::from_str(config_json).map_err(|e| format!("invalid config_json: {e}"))?;
    let id = firmware_object_id_from_config(&config)?;
    let parsed = parse_firmware_object_config_value_with_scope(&id, &config, scope)?;
    Ok(EphemeralFirmwareObjectConfig {
        id,
        hardware_type: hardware_type.to_owned(),
        config,
        parsed,
    })
}

fn best_effort_firmware_object_id(config_json: &str) -> String {
    serde_json::from_str::<Value>(config_json)
        .ok()
        .and_then(|config| firmware_object_id_from_config(&config).ok())
        .unwrap_or_default()
}

fn ephemeral_cache_object_id(object_id: &str) -> String {
    format!("{object_id}-{}", uuid::Uuid::new_v4())
}

fn firmware_object_id_from_config(config: &Value) -> std::result::Result<String, String> {
    let product_name = required_json_string(config, "ProductName", "config_json")?;
    let (milestone_idx, milestone) = selected_milestone(config)?;
    let milestone_name =
        required_json_string(milestone, "Name", &format!("Milestones[{milestone_idx}]"))?;
    let id = format!("{product_name}_{milestone_name}");
    validate_firmware_object_id(&id).map_err(str::to_owned)?;
    Ok(id)
}

fn parse_firmware_object_config_value(
    object_id: &str,
    config: &Value,
) -> std::result::Result<ParsedFirmwareComponents, String> {
    parse_firmware_object_config_value_with_scope(
        object_id,
        config,
        FirmwareObjectArtifactScope::All,
    )
}

fn parse_firmware_object_config_value_with_scope(
    object_id: &str,
    config: &Value,
    scope: FirmwareObjectArtifactScope,
) -> std::result::Result<ParsedFirmwareComponents, String> {
    let mut parsed = match scope {
        // Ephemeral apply: fail closed so a malformed release catalog is a visible
        // error rather than silent warn + empty targets downstream.
        FirmwareObjectArtifactScope::FirmwareTargets | FirmwareObjectArtifactScope::All => {
            parse_firmware_object_config(config).map_err(|e| {
                tracing::error!(
                    object_id,
                    error = %e,
                    "failed to parse firmware release catalog"
                );
                format!("invalid firmware release catalog in config_json: {e}")
            })?
        }
        FirmwareObjectArtifactScope::SwitchSystemImages => ParsedFirmwareComponents::default(),
    };

    if matches!(
        scope,
        FirmwareObjectArtifactScope::SwitchSystemImages | FirmwareObjectArtifactScope::All
    ) {
        parsed.switch_system_images = collect_switch_system_images(config)
            .map_err(|e| format!("invalid switch system image data: {e}"))?;
    }
    if matches!(scope, FirmwareObjectArtifactScope::All) {
        validate_required_download_locations(&parsed)?;
    }
    Ok(parsed)
}

fn request_devices(nodes: Option<&rm::NodeSet>) -> Vec<rm::NodeInfo> {
    nodes.map(|nodes| nodes.nodes.clone()).unwrap_or_default()
}

fn request_node_ids(nodes: Option<&rm::NodeSet>) -> Vec<String> {
    nodes
        .map(|nodes| {
            nodes
                .nodes
                .iter()
                .map(|device| device.node_id.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn firmware_job_ids(jobs: &[PrecreatedFirmwareJob]) -> Vec<String> {
    jobs.iter()
        .map(|job| job.pending.id().to_string())
        .collect()
}

fn switch_image_job_ids(jobs: &[PrecreatedSwitchImageJob]) -> Vec<String> {
    jobs.iter()
        .map(|job| job.pending.id().to_string())
        .collect()
}

fn node_batch_succeeded(response: &rm::NodeBatchResponse) -> bool {
    response.status == rm::ReturnCode::Success as i32
}

fn cancel_firmware_download(
    cancellations: &Arc<Mutex<HashMap<String, CancellationToken>>>,
    object_id: &str,
) {
    // The map holds no cross-operation invariants, so a poisoned lock is safe
    // to recover from; unwrapping instead would leave firmware object
    // management permanently broken after any panic under the lock.
    let cancel = cancellations
        .lock()
        .unwrap_or_else(|error| {
            tracing::warn!("firmware_download_cancellations lock poisoned; recovering");
            error.into_inner()
        })
        .remove(object_id);

    if let Some(cancel) = cancel {
        cancel.cancel();
    }
}

fn mark_firmware_jobs_running(
    tracker: &Arc<crate::orchestrator::job_tracker::JobTracker>,
    jobs: &[PrecreatedFirmwareJob],
    description: &str,
) {
    for job in jobs {
        tracker.mark_running(job.pending.id().as_ref(), description);
    }
}

fn mark_firmware_jobs_failed(
    tracker: &Arc<crate::orchestrator::job_tracker::JobTracker>,
    jobs: Vec<PrecreatedFirmwareJob>,
    error_code: JobError,
    message: &str,
) {
    let mut refresh_child_id = None;
    for job in jobs {
        fail_precreated_child(
            job.pending,
            job_failure(error_code, message),
            &mut refresh_child_id,
        );
    }
    refresh_parent_after_failures(tracker, refresh_child_id);
}

fn mark_switch_jobs_running(
    tracker: &Arc<crate::orchestrator::job_tracker::JobTracker>,
    jobs: &[PrecreatedSwitchImageJob],
    message: &str,
) {
    for job in jobs {
        tracker.mark_running(job.pending.id().as_ref(), message);
    }
}

fn mark_switch_jobs_failed(
    tracker: &Arc<crate::orchestrator::job_tracker::JobTracker>,
    jobs: Vec<PrecreatedSwitchImageJob>,
    message: &str,
) {
    let mut refresh_child_id = None;
    for job in jobs {
        fail_precreated_child(
            job.pending,
            switch_job_failure(message),
            &mut refresh_child_id,
        );
    }
    refresh_parent_after_failures(tracker, refresh_child_id);
}

fn fail_precreated_child(
    job: RmsJobHandle,
    failure: JobFailure,
    refresh_child_id: &mut Option<JobId>,
) {
    let child_id = job.id().clone();
    let span = job.span();
    let error_message = failure.message.clone();
    job.fail(failure);
    let _entered = span.enter();
    tracing::error!(error_message, "job failed");
    refresh_child_id.get_or_insert(child_id);
}

fn switch_job_failure(message: impl Into<String>) -> JobFailure {
    JobFailure::new(JobError::Other, message).with_result_json(String::new())
}

fn refresh_parent_after_failures(
    tracker: &Arc<crate::orchestrator::job_tracker::JobTracker>,
    refresh_child_id: Option<JobId>,
) {
    if let Some(child_id) = refresh_child_id {
        tracker.refresh_parent_for_child(&child_id);
    }
}

// Firmware and switch system image batches share the ephemeral object cache,
// and removing it after the child jobs finish is a generic `job_lifecycle`
// concern: wait for the IDs to reach a terminal state, then drop the dir. It
// only needs the `JobRegistry` the jobs live in, not the firmware-flavored
// `JobTracker` surface, so it stays generic over the job domain.
//
// Cleanup is hosted on a job supervisor (`spawn_job_with_cleanup`).
// The supervisor runs the plan in its own supervised task, so a panic while
// waiting for child jobs or removing the directory is caught and logged within
// the job span instead of silently aborting the task and leaking the cache
// directory.
fn spawn_ephemeral_object_cache_cleanup<D: JobDomain>(
    registry: Arc<JobRegistry<D>>,
    child_job_ids: Vec<String>,
    cache_dir: PathBuf,
) {
    let cleanup_plan = ephemeral_object_cache_cleanup_plan(child_job_ids, cache_dir);
    let pending = match registry.create_job(
        JobSpec::new(
            "",
            "ephemeral-object-cache-cleanup",
            "Cleaning up ephemeral object cache",
            tracing::Span::current(),
        ),
        None,
        false,
    ) {
        Ok(pending) => pending,
        Err(failure) => {
            // The registry is at capacity, so fall back to a best-effort
            // detached cleanup rather than leaking the cache directory.
            tracing::warn!(
                error = %failure.message,
                "job registry at capacity; running ephemeral object cache cleanup without a tracked supervisor"
            );
            tokio::spawn(async move {
                cleanup_plan.run(registry).await;
            });
            return;
        }
    };

    registry
        .spawn_job_with_cleanup(pending, cleanup_plan, |job| async move {
            job.complete("Completed", "{}");
        })
        .detach();
}

fn ephemeral_object_cache_cleanup_plan(
    child_job_ids: Vec<String>,
    cache_dir: PathBuf,
) -> CleanupPlan {
    const CLEANUP_TIMEOUT: Duration = Duration::from_hours(1);
    let child_job_ids = child_job_ids.into_iter().map(JobId::from).collect();
    CleanupPlan::remove_dir_after_jobs_terminate(child_job_ids, cache_dir, Some(CLEANUP_TIMEOUT))
}

#[cfg(test)]
async fn cleanup_ephemeral_object_cache_after_jobs<D: JobDomain>(
    registry: Arc<JobRegistry<D>>,
    child_job_ids: Vec<String>,
    cache_dir: PathBuf,
) {
    ephemeral_object_cache_cleanup_plan(child_job_ids, cache_dir)
        .run(registry)
        .await;
}

fn validate_firmware_object_id(id: &str) -> std::result::Result<(), &'static str> {
    let safe_single_component = Path::new(id)
        .components()
        .all(|component| matches!(component, Component::Normal(_)));
    if !id.is_empty() && safe_single_component && !id.contains('/') && !id.contains('\\') {
        Ok(())
    } else {
        Err("firmware object Id must be a single path-safe component")
    }
}

fn firmware_object_metadata(
    object_id: &str,
    parsed_components: Option<&Value>,
) -> Option<rm::FirmwareObjectMetadata> {
    let parsed_components = parsed_components?;

    let lookup_table: FirmwareLookupTable = match serde_json::from_value(parsed_components.clone())
    {
        Ok(lookup_table) => lookup_table,
        Err(e) => {
            tracing::warn!(
                object_id,
                error = %e,
                "failed to parse firmware object metadata"
            );
            return None;
        }
    };

    Some(metadata_from_lookup_table(&lookup_table))
}

fn metadata_from_lookup_table(lookup_table: &FirmwareLookupTable) -> rm::FirmwareObjectMetadata {
    let mut device_components = Vec::new();
    let mut devices: Vec<_> = lookup_table.devices.iter().collect();
    devices.sort_by_key(|(device_type, _)| device_type.as_str());

    for (device_type, entries) in devices {
        let public_device_type = if device_type == VRNVL72_COMPUTE_LOOKUP_KEY {
            COMPUTE_LOOKUP_KEY
        } else {
            device_type.as_str()
        };
        for node_type in node_types_for_device_type(device_type) {
            let mut components_by_name: BTreeMap<String, rm::FirmwareObjectComponent> =
                BTreeMap::new();
            let mut sorted_entries: Vec<_> = entries.iter().collect();
            sorted_entries.sort_by_key(|(component_key, _)| component_key.as_str());
            let mut seen_single_package_targets_by_firmware_type: HashMap<String, HashSet<String>> =
                HashMap::new();
            for (component_key, entry) in sorted_entries {
                if firmware_lookup_entry_mapping_for_node_type(*node_type, component_key, entry)
                    .is_none()
                {
                    continue;
                }
                let component_name = normalized_component_name(component_key);
                let seen_single_package_targets = seen_single_package_targets_by_firmware_type
                    .entry(entry.firmware_type.to_ascii_lowercase())
                    .or_default();
                if !firmware_package_allowed_by_multiplicity_policy(
                    *node_type,
                    component_key,
                    seen_single_package_targets,
                ) {
                    continue;
                }
                let component = components_by_name
                    .entry(component_name.clone())
                    .or_insert_with(|| rm::FirmwareObjectComponent {
                        name: component_name,
                        version: entry.version.clone().unwrap_or_default(),
                        bundle: entry.bundle.clone(),
                        source_component: entry.component.clone(),
                        subcomponents: entry
                            .subcomponents
                            .iter()
                            .map(|subcomponent| rm::FirmwareObjectSubcomponent {
                                name: subcomponent.component.clone(),
                                version: subcomponent.version.clone(),
                                sku_id: subcomponent.skuid.clone().unwrap_or_default(),
                            })
                            .collect(),
                        artifacts: Vec::new(),
                    });
                component.artifacts.push(rm::FirmwareObjectArtifact {
                    firmware_type: entry.firmware_type.clone(),
                    filename: entry.filename.clone(),
                    target: entry.target.clone(),
                    bundle: entry.bundle.clone(),
                });
            }
            let components: Vec<_> = components_by_name.into_values().collect();
            if components.is_empty() {
                continue;
            }
            device_components.push(rm::FirmwareObjectDeviceComponents {
                node_type: domain_node_type_to_proto(*node_type) as i32,
                node_descriptor: Some(domain_node_type_to_descriptor(*node_type)),
                device_type: public_device_type.to_owned(),
                components,
            });
        }
    }

    let mut switch_system_images = Vec::new();
    let mut image_groups: Vec<_> = lookup_table.switch_system_images.iter().collect();
    image_groups.sort_by_key(|(device_type, _)| device_type.as_str());
    for (_, images) in image_groups {
        let mut image_entries: Vec<_> = images.values().collect();
        image_entries.sort_by_key(|image| {
            (
                image.firmware_type.as_str(),
                image.version.as_str(),
                image.image_filename.as_str(),
            )
        });
        switch_system_images.extend(image_entries.into_iter().map(|image| {
            rm::FirmwareObjectSwitchSystemImage {
                software_type: image.firmware_type.clone(),
                version: image.version.clone(),
                package_name: image.package_name.clone(),
                image_filename: image.image_filename.clone(),
                required: image.required,
            }
        }));
    }

    rm::FirmwareObjectMetadata {
        device_components,
        switch_system_images,
    }
}

fn normalized_component_name(component_key: &str) -> String {
    target_key_from_component_key(component_key)
}

fn target_key_from_component_key(component_key: &str) -> String {
    for target_key in known_firmware_target_keys() {
        let Some(suffix) = component_key.strip_prefix(target_key) else {
            continue;
        };
        if suffix.is_empty() || suffix.starts_with('_') {
            return (*target_key).to_owned();
        }
    }

    component_key
        .split_once('_')
        .map(|(key, _)| key)
        .unwrap_or(component_key)
        .to_owned()
}

fn known_firmware_target_keys() -> &'static [&'static str] {
    &[
        "PCIE_SWITCH_CONFIG_0",
        "INFOROM_GPU_0",
        "INFOROM_GPU_1",
        "INFOROM_GPU_2",
        "INFOROM_GPU_3",
        "EROT_FPGA_0",
        "EROT_FPGA_1",
        "EROT_BMC_0",
        "EROT_CPU_0",
        "EROT_CPU_1",
        "HGX_BMC_0",
        "DeltaPMC",
        "DeltaPSU",
        "LiteOnPMC",
        "LiteOnPSU",
        "FPGA_0",
        "FPGA_1",
        "CPLD_0",
        "CPU_0",
        "CPU_1",
        "GPU_0",
        "GPU_1",
        "GPU_2",
        "GPU_3",
        "BMC",
        "HMC",
        "SMA",
        "FPGA",
        "EROT",
        "CPLD",
        "BIOS",
    ]
}

fn node_types_for_device_type(device_type: &str) -> &'static [NodeType] {
    match device_type {
        COMPUTE_LOOKUP_KEY => &[
            NodeType::ComputeGb200Nvidia,
            NodeType::ComputeGb200Wiwynn,
            NodeType::ComputeGb300Nvidia,
            NodeType::ComputeGb300Lenovo,
            NodeType::ComputeVrnvl72Nvidia,
        ],
        VRNVL72_COMPUTE_LOOKUP_KEY => &[NodeType::ComputeVrnvl72Nvidia],
        POWER_SHELF_LOOKUP_KEY => &[
            NodeType::PowershelfGb200Liteon,
            NodeType::PowershelfGb200Delta,
            NodeType::PowershelfGb300Liteon,
            NodeType::PowershelfGb300Delta,
        ],
        SWITCH_LOOKUP_KEY => &[
            NodeType::SwitchGb200Nvidia,
            NodeType::SwitchGb300Nvidia,
            NodeType::SwitchVrnvl72Nvidia,
        ],
        _ => &[],
    }
}

fn firmware_object_to_proto(firmware: FirmwareObject) -> rm::FirmwareObject {
    let metadata = firmware_object_metadata(&firmware.id, firmware.parsed_components.as_ref());
    rm::FirmwareObject {
        id: firmware.id,
        config_json: firmware.config.to_string(),
        available: firmware.available,
        created: Some(timestamp_from_datetime(firmware.created)),
        updated: Some(timestamp_from_datetime(firmware.updated)),
        metadata,
        hardware_type: firmware.rack_hardware_type.0,
        is_default: firmware.is_default,
    }
}

fn status_from_rms_error(error: RmsError) -> Status {
    match error.code {
        crate::utilities::error::ErrorCode::NotFound => Status::not_found(error.message),
        crate::utilities::error::ErrorCode::AlreadyExists => Status::already_exists(error.message),
        crate::utilities::error::ErrorCode::InvalidArgument => {
            Status::invalid_argument(error.message)
        }
        crate::utilities::error::ErrorCode::FailedPrecondition => {
            Status::failed_precondition(error.message)
        }
        crate::utilities::error::ErrorCode::Unauthenticated => {
            Status::unauthenticated(error.message)
        }
        _ => Status::internal(error.message),
    }
}

fn batch_failure(message: String) -> rm::NodeBatchResponse {
    rm::NodeBatchResponse {
        status: rm::ReturnCode::Failure.into(),
        message,
        node_results: Vec::new(),
        job_id: String::new(),
        stats: Some(rm::NodeOperationStats {
            total_nodes: 0,
            successful_nodes: 0,
            failed_nodes: 0,
        }),
    }
}

fn accepted_batch(job_id: String, total_nodes: u32, message: &str) -> rm::NodeBatchResponse {
    accepted_batch_with_stats(job_id, total_nodes, 0, 0, Vec::new(), message)
}

fn accepted_batch_with_stats(
    job_id: String,
    total_nodes: u32,
    successful_nodes: u32,
    failed_nodes: u32,
    node_results: Vec<rm::NodeOperationResult>,
    message: &str,
) -> rm::NodeBatchResponse {
    rm::NodeBatchResponse {
        status: if failed_nodes == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into(),
        message: message.to_owned(),
        node_results,
        job_id,
        stats: Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes,
            failed_nodes,
        }),
    }
}

struct BoardSkuSource<'a> {
    board_skus: &'a [Value],
    context: String,
    product_name: String,
}

fn board_sku_source(config: &Value) -> std::result::Result<BoardSkuSource<'_>, String> {
    let product_name = required_json_string(config, "ProductName", "config_json")?;
    let (milestone_idx, milestone) = selected_milestone(config)?;
    let board_skus = milestone
        .get("BoardSKUs")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Milestones[{milestone_idx}] must contain 'BoardSKUs' array"))?;
    Ok(BoardSkuSource {
        board_skus,
        context: format!("Milestones[{milestone_idx}].BoardSKUs"),
        product_name,
    })
}

fn selected_milestone(config: &Value) -> std::result::Result<(usize, &Value), String> {
    let milestones = config
        .get("Milestones")
        .and_then(Value::as_array)
        .ok_or_else(|| "JSON must contain 'Milestones' array".to_owned())?;

    match milestones.len() {
        1 => Ok((0, &milestones[0])),
        0 => Err("Milestones must contain exactly one milestone; found 0".to_owned()),
        count => Err(format!(
            "Milestones must contain exactly one milestone; found {count}"
        )),
    }
}

fn parse_firmware_object_config(
    config: &Value,
) -> std::result::Result<ParsedFirmwareComponents, String> {
    let source = board_sku_source(config)?;
    let board_skus = source.board_skus;
    let mut parsed_board_skus = Vec::with_capacity(board_skus.len());

    for (board_idx, board_sku) in board_skus.iter().enumerate() {
        let sku_id = json_string(board_sku, "SKUID");
        let name = json_string(board_sku, "Name");
        let sku_type = json_string(board_sku, "Type");
        // The sanitized VR-NVL72 firmware manifest omits the firmware-channel Type on the
        // update packages that RMS consumes.
        let infer_vrnvl72_defaults = source.product_name.eq_ignore_ascii_case("VR-NVL72")
            && matches!(
                lookup_key_from_board_type(&sku_type),
                Some(COMPUTE_LOOKUP_KEY | SWITCH_LOOKUP_KEY)
            );
        let firmware_components = board_sku
            .get("Components")
            .and_then(|components| components.get("Firmware"))
            .and_then(Value::as_array)
            .map(|firmware_list| {
                firmware_list
                    .iter()
                    .enumerate()
                    .map(|(firmware_idx, firmware)| {
                        let context = format!(
                            "{}[{board_idx}].Components.Firmware[{firmware_idx}]",
                            source.context
                        );
                        parse_firmware_component(firmware, &context, infer_vrnvl72_defaults)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        parsed_board_skus.push(BoardSkuFirmware {
            sku_id,
            name,
            sku_type,
            firmware_components,
        });
    }

    Ok(ParsedFirmwareComponents {
        board_skus: parsed_board_skus,
        switch_system_images: Vec::new(),
    })
}

fn parse_firmware_component(
    firmware: &Value,
    context: &str,
    infer_vrnvl72_defaults: bool,
) -> FirmwareComponent {
    let component = json_string(firmware, "Component");
    let locations = firmware
        .get("Locations")
        .and_then(Value::as_array)
        .map(|locations| {
            locations
                .iter()
                .enumerate()
                .filter_map(|location| {
                    let (location_idx, location) = location;
                    let firmware_type = optional_json_string(location, "Type")
                        .filter(|value| !value.trim().is_empty())
                        .or_else(|| {
                            let filename = json_string(location, "FileName");
                            // This manifest shape has one update package plus a
                            // non-payload CoRIM; only infer the .fwpkg as firmware.
                            (infer_vrnvl72_defaults
                                && component.eq_ignore_ascii_case("BMC")
                                && filename.to_ascii_lowercase().ends_with(".fwpkg"))
                            .then(|| "Firmware".to_owned())
                        });
                    location_is_firmware_payload(firmware_type.as_deref()).then(|| {
                        let location_value = json_string(location, "Location");
                        if location_value.trim().is_empty()
                            && firmware_type
                                .as_deref()
                                .is_some_and(|ty| ty.eq_ignore_ascii_case("Firmware"))
                        {
                            tracing::debug!(
                                component = %json_string(firmware, "Component"),
                                context = %format!("{context}.Locations[{location_idx}]"),
                                "firmware payload location is empty"
                            );
                        }
                        FirmwareLocation {
                            location: location_value,
                            location_type: json_string(location, "LocationType"),
                            firmware_type,
                            sha256: optional_sha256(location),
                            context: format!("{context}.Locations[{location_idx}]"),
                        }
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let subcomponents = firmware
        .get("SubComponents")
        .and_then(Value::as_array)
        .map(|subcomponents| {
            subcomponents
                .iter()
                .filter_map(|subcomponent| {
                    let component = json_string(subcomponent, "Component");
                    let version = json_string(subcomponent, "Version");
                    (!component.is_empty() && !version.is_empty()).then(|| FirmwareSubComponent {
                        component,
                        version,
                        skuid: optional_json_string(subcomponent, "SKUID"),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    FirmwareComponent {
        component: component.clone(),
        bundle: optional_json_string(firmware, "Bundle"),
        version: optional_json_string(firmware, "Version"),
        component_type: optional_json_string(firmware, "Type")
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                (infer_vrnvl72_defaults
                    && (component.eq_ignore_ascii_case("HMC")
                        || component.eq_ignore_ascii_case("BMC")
                        || component.eq_ignore_ascii_case("BMC+CPLD+SMA+ERoT+SBIOS")))
                .then(|| "Prod".to_owned())
            }),
        context: context.to_owned(),
        locations,
        subcomponents,
    }
}

fn collect_switch_system_images(
    config: &Value,
) -> std::result::Result<Vec<SwitchSystemImageArtifact>, String> {
    let mut parsed = Vec::new();
    let mut seen_keys = HashSet::new();
    let source = board_sku_source(config)?;
    collect_switch_system_images_from_board_skus(
        source.board_skus,
        &source.context,
        &source.product_name,
        &mut parsed,
        &mut seen_keys,
    )?;

    Ok(parsed)
}

fn collect_switch_system_images_from_board_skus(
    board_skus: &[Value],
    board_skus_context: &str,
    product_name: &str,
    parsed: &mut Vec<SwitchSystemImageArtifact>,
    seen_keys: &mut HashSet<(String, String, String)>,
) -> std::result::Result<(), String> {
    for (board_idx, board_sku) in board_skus.iter().enumerate() {
        if json_string(board_sku, "Type") != "Switch Tray" {
            continue;
        }
        let board_context = format!("{board_skus_context}[{board_idx}]");
        let board_name = json_string(board_sku, "Name");
        let Some(software) = board_sku
            .get("Components")
            .and_then(|components| components.get("Software"))
            .and_then(Value::as_array)
        else {
            continue;
        };

        let board_package_name = software
            .iter()
            .filter(|entry| json_string(entry, "Component") == "NVOS")
            .flat_map(|entry| {
                entry
                    .get("Locations")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .map(|location| json_string(location, "PackageName"))
            .find(|package_name| !package_name.is_empty());

        for (software_idx, entry) in software.iter().enumerate() {
            if json_string(entry, "Component") != "NVOS" {
                continue;
            }
            let context = format!("{board_context}.Components.Software[{software_idx}]");
            let version = required_json_string(entry, "Version", &context)?;
            let firmware_type = required_json_string(entry, "Type", &context)?.to_lowercase();
            let Some(locations) = entry.get("Locations").and_then(Value::as_array) else {
                return Err(format!("{context}.Locations is required"));
            };
            let selected = select_nvos_location(&context, locations)?;
            let package_name = json_string(selected, "PackageName");
            let package_name = if package_name.is_empty() {
                board_package_name
                    .clone()
                    .or_else(|| (!product_name.is_empty()).then(|| product_name.to_owned()))
                    .or_else(|| (!board_name.is_empty()).then_some(board_name.clone()))
                    .ok_or_else(|| {
                        format!("{context}.Locations selected PackageName is required")
                    })?
            } else {
                package_name
            };
            let location = json_string(selected, "Location");
            let location_type = required_json_string(selected, "LocationType", &context)?;
            let key = (
                "Switch Tray".to_owned(),
                "NVOS".to_owned(),
                firmware_type.clone(),
            );
            if seen_keys.insert(key) {
                parsed.push(SwitchSystemImageArtifact {
                    device_type: "Switch Tray".to_owned(),
                    component: "NVOS".to_owned(),
                    version,
                    firmware_type,
                    package_name,
                    image_filename: filename_from_location(&location).map_err(|e| e.to_string())?,
                    location,
                    location_type,
                    required: selected
                        .get("Required")
                        .and_then(Value::as_bool)
                        .unwrap_or(true),
                    sha256: optional_sha256(selected),
                });
            }
        }
    }
    Ok(())
}

fn select_nvos_location<'a>(
    context: &str,
    locations: &'a [Value],
) -> std::result::Result<&'a Value, String> {
    let mut candidates = Vec::new();
    for (idx, location) in locations.iter().enumerate() {
        let location_value = json_string(location, "Location");
        if location_value.is_empty() {
            continue;
        }
        let Ok(image_filename) = filename_from_location(&location_value)
            .inspect_err(|e| tracing::debug!(location_value, error = %e, "failed to extract filename from location")) else {
            continue;
        };
        if !image_filename.to_ascii_lowercase().ends_with(".bin") {
            continue;
        }
        let name_upper = json_string(location, "Name").to_ascii_uppercase();
        let filename_upper = image_filename.to_ascii_uppercase();
        let location_upper = location_value.to_ascii_uppercase();
        let mut score = 0;
        if name_upper.contains("NVOS") || filename_upper.contains("NVOS") {
            score += 2;
        }
        if name_upper.contains("AMD64")
            || filename_upper.contains("AMD64")
            || location_upper.contains("/AMD64/")
        {
            score += 1;
        }
        candidates.push((idx, score));
    }
    let best_score = candidates
        .iter()
        .map(|(_, score)| *score)
        .max()
        .ok_or_else(|| format!("{context}.Locations must include an NVOS .bin image location"))?;
    let best: Vec<_> = candidates
        .iter()
        .copied()
        .filter(|(_, score)| *score == best_score)
        .collect();
    // `best_score` is selected from `candidates`, so `best` is non-empty here.
    // The slice pattern requires a single winner; ties make the NVOS location ambiguous.
    let [(best_index, _)] = best.as_slice() else {
        return Err(format!(
            "{context}.Locations contains multiple NVOS .bin image candidates"
        ));
    };
    locations
        .get(*best_index)
        .ok_or_else(|| format!("{context}.Locations candidate index is out of bounds"))
}

fn spawn_firmware_download_task(
    object_id: String,
    parsed_components: ParsedFirmwareComponents,
    access_token: Option<String>,
    firmware_dir: PathBuf,
    store: Arc<dyn FirmwareObjectStore>,
    cancel: CancellationToken,
    cancellations: Arc<Mutex<HashMap<String, CancellationToken>>>,
) {
    tokio::spawn(async move {
        if let Err(e) = download_firmware_files(
            &object_id,
            &parsed_components,
            access_token,
            &firmware_dir,
            store.as_ref(),
            &cancel,
        )
        .await
        {
            tracing::error!(
                object_id = %object_id,
                error = %e,
                "failed to download firmware object artifacts"
            );
        }

        cancellations
            .lock()
            .unwrap_or_else(|error| {
                tracing::warn!("firmware_download_cancellations lock poisoned; recovering");
                error.into_inner()
            })
            .remove(&object_id);
    });
}

fn optional_access_token(access_token: Option<String>) -> Option<String> {
    access_token.filter(|token| !token.trim().is_empty())
}

async fn download_firmware_files(
    object_id: &str,
    parsed_components: &ParsedFirmwareComponents,
    access_token: Option<String>,
    firmware_dir: &Path,
    store: &dyn FirmwareObjectStore,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let lookup_table = download_firmware_artifacts(
        object_id,
        parsed_components,
        access_token,
        firmware_dir,
        FIRMWARE_OBJECT_CACHE_SUBDIR,
        cancel,
    )
    .await?;
    let lookup_json = serde_json::to_value(lookup_table)
        .map_err(|e| format!("failed to serialize lookup table: {e}"))?;
    store
        .set_parsed_components_available(object_id, lookup_json)
        .await
        .map_err(|e| e.message)?;
    Ok(())
}

/// Maximum number of individual artifact failures listed by name in the
/// aggregate error returned by [`download_firmware_artifacts`]. Batches can
/// include dozens of artifacts; the per-artifact WARN logs already carry the
/// full detail for every failure, so the aggregate is capped to stay readable.
const MAX_LISTED_DOWNLOAD_FAILURES: usize = 5;

/// Builds a bounded, human-readable summary of required artifact download
/// failures (component + url + underlying error) so the aggregate error is
/// self-contained without requiring a search through the paired WARN logs.
fn summarize_download_failures(failures: &[(DownloadContext, String)]) -> String {
    let mut details: Vec<String> = failures
        .iter()
        .take(MAX_LISTED_DOWNLOAD_FAILURES)
        .map(|(context, error)| {
            format!(
                "{} ({}): {error}",
                context.component,
                redact_location_for_logging(&context.url)
            )
        })
        .collect();
    if failures.len() > MAX_LISTED_DOWNLOAD_FAILURES {
        details.push(format!(
            "...and {} more",
            failures.len() - MAX_LISTED_DOWNLOAD_FAILURES
        ));
    }
    format!(
        "{} required firmware artifact downloads failed: {}",
        failures.len(),
        details.join("; ")
    )
}

async fn download_firmware_artifacts(
    object_id: &str,
    parsed_components: &ParsedFirmwareComponents,
    access_token: Option<String>,
    firmware_dir: &Path,
    cache_subdir: &str,
    cancel: &CancellationToken,
) -> std::result::Result<FirmwareLookupTable, String> {
    if cancel.is_cancelled() {
        return Err(format!(
            "download cancelled: firmware object {object_id} was deleted before any artifacts were downloaded"
        ));
    }
    let firmware_cache_dir =
        firmware_object_cache_dir_for_subdir(firmware_dir, cache_subdir, object_id);
    tokio::fs::create_dir_all(&firmware_cache_dir)
        .await
        .map_err(|e| format!("failed to create cache directory: {e}"))?;

    let http_client = build_artifact_http_client()?;
    let mut task_set: JoinSet<std::result::Result<(), String>> = JoinSet::new();
    // Task identity is looked up by `tokio::task::Id` because a `JoinError`
    // (panic/abort) only carries the task id, not any state from the task's
    // own future, so we can't attach artifact details to those failures
    // without tracking them separately here.
    let mut contexts: HashMap<tokio::task::Id, DownloadContext> = HashMap::new();
    for artifact in build_download_artifacts(parsed_components) {
        let context = DownloadContext::from(&artifact);
        let handle = spawn_download_artifact(
            &mut task_set,
            object_id.to_owned(),
            artifact,
            access_token.clone(),
            firmware_cache_dir.clone(),
            cancel.clone(),
            http_client.clone(),
        );
        contexts.insert(handle.id(), context);
    }

    // Keep track of any failures that occur across the tasks.
    let mut failures: Vec<(DownloadContext, String)> = Vec::new();
    while let Some(joined) = task_set.join_next_with_id().await {
        match joined {
            Ok((id, Ok(()))) => {
                contexts.remove(&id);
            }
            Ok((id, Err(e))) => match contexts.remove(&id) {
                Some(context) => {
                    tracing::warn!(
                        object_id,
                        component = %context.component,
                        bundle = ?context.bundle,
                        location_type = %context.location_type,
                        url = %redact_location_for_logging(&context.url),
                        error = %e,
                        "firmware object artifact download failed"
                    );
                    if context.required {
                        failures.push((context, e));
                    }
                }
                None => {
                    tracing::warn!(
                        object_id,
                        error = %e,
                        "firmware object artifact download failed: context not found"
                    );
                }
            },
            Err(join_error) => match contexts.remove(&join_error.id()) {
                Some(context) => {
                    tracing::warn!(
                        object_id,
                        component = %context.component,
                        bundle = ?context.bundle,
                        location_type = %context.location_type,
                        url = %redact_location_for_logging(&context.url),
                        error = %join_error,
                        "firmware object download task failed"
                    );
                    if context.required {
                        let error = join_error.to_string();
                        failures.push((context, error));
                    }
                }
                None => {
                    tracing::warn!(
                        object_id,
                        error = %join_error,
                        "firmware object download task failed: context not found"
                    );
                }
            },
        }
    }

    // Take list of failures/contexts and return a summarized error string.
    if !failures.is_empty() {
        return Err(summarize_download_failures(&failures));
    }
    Ok(build_firmware_lookup_table(parsed_components))
}

/// Builds and returns a list of DownloadArtifacts from a ParsedFirmwareComponents object.
fn build_download_artifacts(parsed_components: &ParsedFirmwareComponents) -> Vec<DownloadArtifact> {
    let mut artifacts = Vec::new();
    for board_sku in &parsed_components.board_skus {
        for firmware_component in &board_sku.firmware_components {
            for location in &firmware_component.locations {
                artifacts.push(DownloadArtifact {
                    url: location.location.clone(),
                    location_type: location.location_type.clone(),
                    component: firmware_component.component.clone(),
                    bundle: firmware_component.bundle.clone(),
                    required: true,
                    relative_cache_dir: Some(firmware_type_cache_subdir(
                        firmware_component
                            .component_type
                            .as_deref()
                            .unwrap_or("prod"),
                    )),
                    sha256: location.sha256.clone(),
                });
            }
        }
    }
    for system_image in &parsed_components.switch_system_images {
        artifacts.push(DownloadArtifact {
            url: system_image.location.clone(),
            location_type: system_image.location_type.clone(),
            component: system_image.component.clone(),
            bundle: Some(system_image.package_name.clone()),
            required: system_image.required,
            relative_cache_dir: Some(firmware_type_cache_subdir(&system_image.firmware_type)),
            sha256: system_image.sha256.clone(),
        });
    }
    artifacts
}

fn spawn_download_artifact(
    task_set: &mut JoinSet<std::result::Result<(), String>>,
    object_id: String,
    artifact: DownloadArtifact,
    access_token: Option<String>,
    dest_dir: PathBuf,
    cancel: CancellationToken,
    http_client: reqwest::Client,
) -> tokio::task::AbortHandle {
    task_set.spawn(async move {
        let dest_dir = artifact
            .relative_cache_dir
            .as_deref()
            .map(|relative_dir| dest_dir.join(relative_dir))
            .unwrap_or(dest_dir);
        let download = download_single_file(
            &artifact.url,
            &artifact.location_type,
            &artifact.component,
            artifact.bundle.as_deref(),
            access_token.as_deref(),
            artifact.sha256.as_deref(),
            dest_dir,
            &http_client,
        );
        tokio::select! {
            _ = cancel.cancelled() => Err(format!(
                "download cancelled: firmware object {object_id} was deleted"
            )),
            result = download => result,
        }
    })
}

fn firmware_object_cache_dir(firmware_dir: &Path, object_id: &str) -> PathBuf {
    firmware_object_cache_dir_for_subdir(firmware_dir, FIRMWARE_OBJECT_CACHE_SUBDIR, object_id)
}

fn firmware_object_cache_dir_for_subdir(
    firmware_dir: &Path,
    cache_subdir: &str,
    object_id: &str,
) -> PathBuf {
    firmware_dir.join(cache_subdir).join(object_id)
}

fn build_firmware_lookup_table(
    parsed_components: &ParsedFirmwareComponents,
) -> FirmwareLookupTable {
    // Parsed firmware manifest artifacts are stored by the sanitized board Type role. The
    // concrete node type is intentionally resolved later, during apply.
    let mut lookup = FirmwareLookupTable {
        devices: HashMap::new(),
        switch_system_images: HashMap::new(),
    };

    for board_sku in &parsed_components.board_skus {
        for lookup_key in lookup_keys_for_board_sku(board_sku) {
            let mappings = firmware_mappings_for_lookup_key(lookup_key);
            let device_components = lookup.devices.entry(lookup_key.to_owned()).or_default();

            for firmware_component in &board_sku.firmware_components {
                let bundle = firmware_component.bundle.clone().unwrap_or_default();
                let fw_type = firmware_component
                    .component_type
                    .as_deref()
                    .unwrap_or("prod")
                    .to_lowercase();

                for mapping in &mappings {
                    if firmware_component.component == mapping.source_component {
                        insert_lookup_entry(
                            device_components,
                            mapping.target_key,
                            mapping.redfish_target,
                            firmware_component,
                            &bundle,
                            &fw_type,
                        );
                    }
                }
            }
        }
    }
    lookup.devices.retain(|_, entries| !entries.is_empty());

    for system_image in &parsed_components.switch_system_images {
        let typed_key = format!("{}_{}", system_image.component, system_image.firmware_type);
        lookup
            .switch_system_images
            .entry(system_image.device_type.clone())
            .or_default()
            .insert(
                typed_key,
                SwitchSystemImageLookupEntry {
                    component: system_image.component.clone(),
                    package_name: system_image.package_name.clone(),
                    version: system_image.version.clone(),
                    image_filename: system_image.image_filename.clone(),
                    location_type: system_image.location_type.clone(),
                    firmware_type: system_image.firmware_type.clone(),
                    required: system_image.required,
                },
            );
    }

    lookup
}

fn firmware_mappings_for_lookup_key(lookup_key: &str) -> Vec<FirmwareTargetMapping> {
    let mut mappings = Vec::new();
    for (node_type, candidate_lookup_key) in firmware_node_type_lookup_keys() {
        if candidate_lookup_key != lookup_key {
            continue;
        }
        for mapping in get_firmware_component_mappings_for_node_type(node_type) {
            if !mappings.contains(&mapping) {
                mappings.push(mapping);
            }
        }
    }
    mappings
}

fn insert_lookup_entry(
    device_components: &mut HashMap<String, FirmwareLookupEntry>,
    lookup_key: &str,
    target: &str,
    firmware_component: &FirmwareComponent,
    bundle: &str,
    fw_type: &str,
) {
    // Multiple payloads for one target get stable numbered lookup keys.
    let mut payload_index = 0usize;
    for location in &firmware_component.locations {
        if location_is_firmware_payload(location.firmware_type.as_deref())
            && let Ok(filename) = filename_from_location(&location.location)
        {
            payload_index += 1;
            device_components.insert(
                firmware_lookup_entry_key(lookup_key, fw_type, payload_index),
                FirmwareLookupEntry {
                    filename: cache_relative_filename(fw_type, &filename),
                    target: target.to_owned(),
                    component: firmware_component.component.clone(),
                    bundle: bundle.to_owned(),
                    firmware_type: fw_type.to_owned(),
                    version: firmware_component.version.clone(),
                    subcomponents: firmware_component.subcomponents.clone(),
                },
            );
        }
    }
}

fn firmware_lookup_entry_key(lookup_key: &str, fw_type: &str, payload_index: usize) -> String {
    // Always number entries so ordering does not depend on an unsuffixed key.
    format!("{lookup_key}_{fw_type}_{payload_index:04}")
}

fn build_firmware_targets(
    lookup_table: &FirmwareLookupTable,
    node_type: NodeType,
    lookup_key: &str,
    firmware_type: &str,
    object_id: &str,
    cache_subdir: &str,
    selection: &FirmwareObjectComponentSelection,
) -> std::result::Result<Vec<rm::FirmwareTarget>, String> {
    let mut firmware_components = find_firmware_components_for_device(
        lookup_table,
        node_type,
        lookup_key,
        firmware_type,
        selection,
    );
    let flash_order = get_firmware_flash_order(node_type);
    firmware_components.sort_by_key(|(component_key, _, target)| {
        (
            firmware_flash_order_index(lookup_key, component_key, target, flash_order),
            component_key.clone(),
        )
    });
    let firmware_components =
        apply_firmware_target_multiplicity_policy(node_type, firmware_components);
    let firmware_components = collapse_parallel_package_targets(node_type, firmware_components);

    if firmware_components.is_empty() {
        return Err(format!(
            "no matching firmware found in config for {lookup_key} ({firmware_type})"
        ));
    }

    Ok(firmware_components
        .into_iter()
        .map(|(_, filename, target)| rm::FirmwareTarget {
            target: firmware_target_for_apply_request(node_type, target),
            filename: format!("{cache_subdir}/{object_id}/{filename}"),
        })
        .collect())
}

fn firmware_target_for_apply_request(node_type: NodeType, target: String) -> String {
    if node_type.kind() == NodeKind::Powershelf {
        String::new()
    } else {
        target
    }
}

fn firmware_flash_order_index(
    lookup_key: &str,
    component_key: &str,
    target: &str,
    flash_order: &[&str],
) -> usize {
    // PowerShelf packages do not need a Redfish target; order them by firmware manifest key.
    let component_name = normalized_component_name(component_key);
    let order_key = if lookup_key == POWER_SHELF_LOOKUP_KEY {
        component_name.as_str()
    } else {
        target
    };
    flash_order
        .iter()
        .position(|candidate| *candidate == order_key)
        .unwrap_or(usize::MAX)
}

fn apply_firmware_target_multiplicity_policy(
    node_type: NodeType,
    firmware_components: Vec<(String, String, String)>,
) -> Vec<(String, String, String)> {
    // Default behavior is one package per target key unless policy opts in.
    let mut seen_single_package_targets = HashSet::new();
    firmware_components
        .into_iter()
        .filter(|(component_key, _, _)| {
            firmware_package_allowed_by_multiplicity_policy(
                node_type,
                component_key,
                &mut seen_single_package_targets,
            )
        })
        .collect()
}

fn firmware_package_allowed_by_multiplicity_policy(
    node_type: NodeType,
    component_key: &str,
    seen_single_package_targets: &mut HashSet<String>,
) -> bool {
    let target_key = normalized_component_name(component_key);
    firmware_target_allows_multiple_packages(node_type, &target_key)
        || seen_single_package_targets.insert(target_key)
}

fn collapse_parallel_package_targets(
    node_type: NodeType,
    firmware_components: Vec<(String, String, String)>,
) -> Vec<(String, String, String)> {
    const VRNVL72_SWITCH_PARALLEL_TARGETS: [&str; 5] = ["bmc", "bios", "sma", "erot", "cpld1"];

    if node_type != NodeType::SwitchVrnvl72Nvidia {
        return firmware_components;
    }

    let filenames: HashSet<_> = firmware_components
        .iter()
        .map(|(_, filename, _)| filename.as_str())
        .collect();
    let targets: HashSet<_> = firmware_components
        .iter()
        .map(|(_, _, target)| target.to_ascii_lowercase())
        .collect();
    let required: HashSet<_> = VRNVL72_SWITCH_PARALLEL_TARGETS
        .iter()
        .map(|target| (*target).to_owned())
        .collect();

    if filenames.len() != 1 || targets != required {
        return firmware_components;
    }

    let filename = firmware_components
        .first()
        .map(|(_, filename, _)| filename.clone())
        .unwrap_or_default();
    vec![("PACKAGE".to_owned(), filename, String::new())]
}

fn find_firmware_components_for_device(
    lookup_table: &FirmwareLookupTable,
    node_type: NodeType,
    hardware_type: &str,
    firmware_type: &str,
    selection: &FirmwareObjectComponentSelection,
) -> Vec<(String, String, String)> {
    let wanted_type = firmware_type.to_lowercase();
    let mut results = Vec::new();
    let device_components = lookup_table.devices.get(hardware_type).or_else(|| {
        (node_type == NodeType::ComputeVrnvl72Nvidia && hardware_type == COMPUTE_LOOKUP_KEY)
            .then(|| lookup_table.devices.get(VRNVL72_COMPUTE_LOOKUP_KEY))
            .flatten()
    });
    if let Some(device_components) = device_components {
        for (component_key, entry) in device_components {
            if entry.firmware_type.to_lowercase() != wanted_type {
                continue;
            }
            if firmware_lookup_entry_mapping_for_node_type(node_type, component_key, entry)
                .is_none()
                || !firmware_entry_allowed_for_node_type(node_type, component_key, entry)
                || !component_matches(node_type, component_key, entry, selection)
            {
                continue;
            }
            results.push((
                component_key.clone(),
                entry.filename.clone(),
                entry.target.clone(),
            ));
        }
    }
    results
}

fn firmware_entry_allowed_for_node_type(
    node_type: NodeType,
    component_key: &str,
    entry: &FirmwareLookupEntry,
) -> bool {
    if node_type != NodeType::ComputeGb300Supermicro {
        return true;
    }

    let target_key = target_key_from_component_key(component_key);
    let filename = Path::new(&entry.filename)
        .file_name()
        .and_then(|filename| filename.to_str())
        .unwrap_or(&entry.filename)
        .to_ascii_lowercase();
    match target_key.as_str() {
        "HMC" => filename.ends_with(".fwpkg") && filename.contains("nosbios"),
        "BIOS" => filename.ends_with(".bin") && filename.contains("bios"),
        "BMC" => supermicro_bmc_filename(&entry.filename),
        _ => false,
    }
}

fn component_matches(
    node_type: NodeType,
    component_key: &str,
    entry: &FirmwareLookupEntry,
    selection: &FirmwareObjectComponentSelection,
) -> bool {
    let default_mapping =
        firmware_lookup_entry_mapping_for_node_type(node_type, component_key, entry);
    match selection.mode {
        FirmwareObjectComponentSelectionMode::DefaultOnly => {
            return default_mapping.is_some_and(|mapping| mapping.default_apply);
        }
        FirmwareObjectComponentSelectionMode::All => return true,
        FirmwareObjectComponentSelectionMode::Components => {}
    }

    let normalized_key = target_key_from_component_key(component_key).to_lowercase();
    let mut candidates = vec![normalized_key, entry.target.to_lowercase()];
    if default_mapping.is_some_and(|mapping| mapping.default_apply) {
        candidates.push(entry.component.to_lowercase());
    }
    selection.components.iter().any(|wanted| {
        let wanted = wanted.to_ascii_lowercase();
        candidates.iter().any(|candidate| candidate == &wanted)
    })
}

fn resolve_switch_system_image(
    firmware: &FirmwareObject,
    firmware_type: &str,
) -> std::result::Result<SwitchSystemImageLookupEntry, String> {
    let lookup_table = parsed_lookup_table(firmware)?;
    let wanted_type = firmware_type.to_lowercase();
    lookup_table
        .switch_system_images
        .get("Switch Tray")
        .and_then(|images| {
            images.values().find(|entry| {
                entry.component == "NVOS" && entry.firmware_type.to_lowercase() == wanted_type
            })
        })
        .cloned()
        .ok_or_else(|| {
            format!(
                "firmware object {} has no switch system image for firmware type {}",
                firmware.id, firmware_type
            )
        })
}

fn parsed_lookup_table(
    firmware: &FirmwareObject,
) -> std::result::Result<FirmwareLookupTable, String> {
    let parsed = firmware
        .parsed_components
        .clone()
        .ok_or_else(|| format!("firmware object {} has no parsed components", firmware.id))?;
    serde_json::from_value(parsed).map_err(|e| {
        format!(
            "failed to parse firmware object {} lookup table: {e}",
            firmware.id
        )
    })
}

fn firmware_target_expected_versions_from_firmware_manifest(
    firmware: &FirmwareObject,
    request: &rm::BatchUpdateFirmwareRequest,
) -> std::result::Result<FirmwareTargetExpectedVersions, String> {
    let target_lists = resolve_firmware_target_selectors(
        &request.firmware_targets,
        &request.node_descriptor_firmware_targets,
    )
    .map_err(|error| format!("failed to resolve firmware targets: {error}"))?;
    let Some(target_list) = target_lists.get(&NodeType::ComputeGb300Supermicro) else {
        return Ok(HashMap::new());
    };

    let lookup = parsed_lookup_table(firmware)?;
    let bmc_entries = lookup
        .devices
        .get(COMPUTE_LOOKUP_KEY)
        .into_iter()
        .flat_map(HashMap::values)
        .filter(|entry| entry.component.eq_ignore_ascii_case("BMC") && entry.target.is_empty())
        .collect::<Vec<_>>();
    let mut versions = HashMap::new();
    for target in &target_list.targets {
        if !target.target.is_empty() {
            continue;
        }

        let mut matching_entries = bmc_entries
            .iter()
            .copied()
            .filter(|entry| target.filename.ends_with(&entry.filename));
        let Some(entry) = matching_entries.next() else {
            return Err(format!(
                "Supermicro GB300 BMC target {} has no matching firmware manifest lookup entry",
                target.filename
            ));
        };
        if matching_entries.next().is_some() {
            return Err(format!(
                "Supermicro GB300 BMC target {} matches multiple firmware manifest lookup entries",
                target.filename
            ));
        }
        let version = entry
            .version
            .as_deref()
            .filter(|version| !version.trim().is_empty())
            .ok_or_else(|| {
                format!(
                    "Supermicro GB300 BMC target {} is missing its firmware manifest version",
                    target.filename
                )
            })?;
        versions.insert(target.filename.clone(), version.to_owned());
    }

    if versions.is_empty() {
        Ok(HashMap::new())
    } else {
        Ok(HashMap::from([(
            NodeType::ComputeGb300Supermicro,
            versions,
        )]))
    }
}

fn validate_required_download_locations(
    parsed_components: &ParsedFirmwareComponents,
) -> std::result::Result<(), String> {
    for board_sku in &parsed_components.board_skus {
        for component in &board_sku.firmware_components {
            let board_name = if board_sku.name.is_empty() {
                "unknown board"
            } else {
                board_sku.name.as_str()
            };
            if !component.locations.is_empty()
                && component
                    .component_type
                    .as_deref()
                    .is_none_or(|component_type| component_type.trim().is_empty())
            {
                let context = context_suffix(&component.context);
                return Err(format!(
                    "firmware component '{}' for board '{}'{} is missing Type",
                    component.component, board_name, context
                ));
            }

            for location in &component.locations {
                if !location
                    .firmware_type
                    .as_deref()
                    .is_some_and(|ty| ty.eq_ignore_ascii_case("Firmware"))
                {
                    continue;
                }
                if location.location.trim().is_empty() {
                    let context = context_suffix(&location.context);
                    return Err(format!(
                        "firmware component '{}' for board '{}'{} has Type \"Firmware\" but missing Location",
                        component.component, board_name, context
                    ));
                }
            }
        }
    }

    for image in &parsed_components.switch_system_images {
        if image.location.trim().is_empty() {
            return Err(format!(
                "switch system image '{}' ({}) is missing Location",
                image.component, image.firmware_type
            ));
        }
    }

    Ok(())
}

fn context_suffix(context: &str) -> String {
    if context.is_empty() {
        String::new()
    } else {
        format!(" at {context}")
    }
}

fn location_is_firmware_payload(location_type: Option<&str>) -> bool {
    matches!(
        location_type,
        Some(location_type)
            if location_type.eq_ignore_ascii_case("Firmware")
                || location_type.eq_ignore_ascii_case("Binary")
    )
}

fn device_type_for_node_type(node_type: NodeType) -> Option<DeviceType> {
    match node_type {
        NodeType::ComputeGb200Nvidia | NodeType::ComputeGb200Wiwynn => {
            Some(DeviceType::ComputeGb200Nvidia)
        }
        NodeType::ComputeGb300Nvidia
        | NodeType::ComputeGb300Lenovo
        | NodeType::ComputeGb300Supermicro => Some(DeviceType::ComputeGb300Nvidia),
        NodeType::ComputeVrnvl72Nvidia => Some(DeviceType::ComputeVrnvl72Nvidia),
        NodeType::SwitchGb200Nvidia => Some(DeviceType::SwitchGb200Nvidia),
        NodeType::SwitchGb300Nvidia => Some(DeviceType::SwitchGb300Nvidia),
        NodeType::SwitchVrnvl72Nvidia => Some(DeviceType::SwitchVrnvl72Nvidia),
        NodeType::PowershelfGb200Liteon
        | NodeType::PowershelfGb200Delta
        | NodeType::PowershelfGb300Liteon
        | NodeType::PowershelfGb300Delta => Some(DeviceType::PowerShelf),
    }
}

fn firmware_target_allows_multiple_packages(node_type: NodeType, target_key: &str) -> bool {
    (matches!(node_type, NodeType::ComputeGb300Lenovo) && target_key.eq_ignore_ascii_case("HMC"))
        || (matches!(node_type, NodeType::ComputeGb200Wiwynn)
            && target_key.eq_ignore_ascii_case("BMC"))
}

fn firmware_download_mapping_requires_all_payloads(
    node_type: NodeType,
    mapping: &FirmwareTargetMapping,
) -> bool {
    // Supermicro manifests can place nosbios and BIOS under the same HMC
    // component. Keep all source locations through download, then select one
    // compatible payload for each logical target during apply.
    node_type == NodeType::ComputeGb300Supermicro
        || firmware_target_allows_multiple_packages(node_type, mapping.target_key)
}

fn firmware_component_allowed_for_node_type(node_type: NodeType, target_key: &str) -> bool {
    let target_key = target_key_from_component_key(target_key);
    if device_type_for_node_type(node_type).is_none() {
        return false;
    }
    match node_type {
        NodeType::ComputeGb300Lenovo => {
            return matches!(target_key.as_str(), "BMC" | "HMC");
        }
        NodeType::ComputeGb300Supermicro => {
            return matches!(target_key.as_str(), "BMC" | "HMC" | "BIOS");
        }
        NodeType::PowershelfGb200Liteon | NodeType::PowershelfGb300Liteon => {
            return matches!(target_key.as_str(), "LiteOnPMC" | "LiteOnPSU");
        }
        NodeType::PowershelfGb200Delta | NodeType::PowershelfGb300Delta => {
            return matches!(target_key.as_str(), "DeltaPMC" | "DeltaPSU");
        }
        _ => {}
    }
    get_firmware_component_mappings_for_node_type(node_type)
        .iter()
        .any(|mapping| mapping.target_key == target_key)
}

fn get_firmware_component_mappings_for_node_type(
    node_type: NodeType,
) -> Vec<FirmwareTargetMapping> {
    if node_type == NodeType::ComputeGb300Supermicro {
        return vec![
            FirmwareTargetMapping {
                source_component: "HMC",
                target_key: "HMC",
                redfish_target: SUPERMICRO_HGX_TARGET,
                default_apply: true,
            },
            FirmwareTargetMapping {
                source_component: "HMC",
                target_key: "BIOS",
                redfish_target: SUPERMICRO_BIOS_TARGET,
                default_apply: true,
            },
            FirmwareTargetMapping {
                source_component: "BIOS",
                target_key: "BIOS",
                redfish_target: SUPERMICRO_BIOS_TARGET,
                default_apply: true,
            },
            FirmwareTargetMapping {
                source_component: "BMC",
                target_key: "BMC",
                redfish_target: "",
                default_apply: true,
            },
        ];
    }

    device_type_for_node_type(node_type)
        .map(|device_type| get_firmware_component_mappings_for_device_type(&device_type))
        .unwrap_or_default()
}

#[cfg(test)]
fn mapped_firmware_device_types() -> Vec<DeviceType> {
    let mut device_types = Vec::new();
    for (node_type, _) in firmware_node_type_lookup_keys() {
        let Some(device_type) = device_type_for_node_type(node_type) else {
            continue;
        };
        if !device_types.contains(&device_type) {
            device_types.push(device_type);
        }
    }
    device_types
}

fn get_firmware_component_mappings_for_device_type(
    device_type: &DeviceType,
) -> Vec<FirmwareTargetMapping> {
    match device_type {
        DeviceType::ComputeVrnvl72Nvidia => vec![
            FirmwareTargetMapping {
                source_component: "HMC",
                target_key: "HMC",
                redfish_target: "/redfish/v1/Chassis/HGX_Chassis_0",
                default_apply: true,
            },
            FirmwareTargetMapping {
                source_component: "BMC",
                target_key: "BMC",
                redfish_target: "",
                default_apply: true,
            },
        ],
        DeviceType::ComputeGb200Nvidia | DeviceType::ComputeGb300Nvidia => {
            vec![
                FirmwareTargetMapping {
                    source_component: "HMC",
                    target_key: "HMC",
                    redfish_target: "/redfish/v1/Chassis/HGX_Chassis_0",
                    default_apply: true,
                },
                FirmwareTargetMapping {
                    source_component: "BMC",
                    target_key: "BMC",
                    redfish_target: "",
                    default_apply: true,
                },
                hgx_firmware_mapping(
                    "CPU_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
                ),
                hgx_firmware_mapping(
                    "CPU_1",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1",
                ),
                hgx_firmware_mapping(
                    "GPU_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
                ),
                hgx_firmware_mapping(
                    "GPU_1",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_1",
                ),
                hgx_firmware_mapping(
                    "GPU_2",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_2",
                ),
                hgx_firmware_mapping(
                    "GPU_3",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_3",
                ),
                hgx_firmware_mapping(
                    "HGX_BMC_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_BMC_0",
                ),
                hgx_firmware_mapping(
                    "FPGA_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_FPGA_0",
                ),
                hgx_firmware_mapping(
                    "FPGA_1",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_FPGA_1",
                ),
                hgx_firmware_mapping(
                    "CPLD_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPLD_0",
                ),
                hgx_firmware_mapping(
                    "EROT_BMC_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_BMC_0",
                ),
                hgx_firmware_mapping(
                    "EROT_CPU_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_CPU_0",
                ),
                hgx_firmware_mapping(
                    "EROT_CPU_1",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_CPU_1",
                ),
                hgx_firmware_mapping(
                    "EROT_FPGA_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_FPGA_0",
                ),
                hgx_firmware_mapping(
                    "EROT_FPGA_1",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_FPGA_1",
                ),
                hgx_firmware_mapping(
                    "INFOROM_GPU_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_InfoROM_GPU_0",
                ),
                hgx_firmware_mapping(
                    "INFOROM_GPU_1",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_InfoROM_GPU_1",
                ),
                hgx_firmware_mapping(
                    "INFOROM_GPU_2",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_InfoROM_GPU_2",
                ),
                hgx_firmware_mapping(
                    "INFOROM_GPU_3",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_InfoROM_GPU_3",
                ),
                hgx_firmware_mapping(
                    "PCIE_SWITCH_CONFIG_0",
                    "/redfish/v1/UpdateService/FirmwareInventory/HGX_PCIeSwitchConfig_0",
                ),
            ]
        }
        DeviceType::SwitchGb200Nvidia | DeviceType::SwitchGb300Nvidia => vec![
            switch_firmware_mapping("BMC+FPGA+EROT", "BMC", "bmc"),
            switch_firmware_mapping("BMC+FPGA+EROT", "FPGA", "fpga"),
            switch_firmware_mapping("BMC+FPGA+EROT", "EROT", "erot"),
            switch_firmware_mapping("CPLD", "CPLD", "cpld"),
            switch_firmware_mapping("SBIOS+EROT", "BIOS", "bios"),
        ],
        DeviceType::SwitchVrnvl72Nvidia => vec![
            switch_firmware_mapping("BMC+CPLD+SMA+ERoT+SBIOS", "BMC", "bmc"),
            switch_firmware_mapping("BMC+CPLD+SMA+ERoT+SBIOS", "BIOS", "bios"),
            switch_firmware_mapping("BMC+CPLD+SMA+ERoT+SBIOS", "SMA", "sma"),
            switch_firmware_mapping("BMC+CPLD+SMA+ERoT+SBIOS", "EROT", "erot"),
            switch_firmware_mapping("BMC+CPLD+SMA+ERoT+SBIOS", "CPLD", "cpld1"),
        ],
        DeviceType::PowerShelf => {
            vec![
                powershelf_firmware_mapping("Delta-PMC", "DeltaPMC"),
                powershelf_firmware_mapping("Delta-PSU", "DeltaPSU"),
                powershelf_firmware_mapping("LiteOn PSU", "LiteOnPSU"),
                powershelf_firmware_mapping("LiteOn PMC", "LiteOnPMC"),
            ]
        }
    }
}

fn hgx_firmware_mapping(
    target_key: &'static str,
    redfish_target: &'static str,
) -> FirmwareTargetMapping {
    FirmwareTargetMapping {
        source_component: "HMC",
        target_key,
        redfish_target,
        default_apply: false,
    }
}

fn switch_firmware_mapping(
    source_component: &'static str,
    target_key: &'static str,
    redfish_target: &'static str,
) -> FirmwareTargetMapping {
    FirmwareTargetMapping {
        source_component,
        target_key,
        redfish_target,
        default_apply: true,
    }
}

fn powershelf_firmware_mapping(
    source_component: &'static str,
    target_key: &'static str,
) -> FirmwareTargetMapping {
    FirmwareTargetMapping {
        source_component,
        target_key,
        redfish_target: "",
        default_apply: true,
    }
}

fn firmware_lookup_entry_mapping_for_node_type(
    node_type: NodeType,
    component_key: &str,
    entry: &FirmwareLookupEntry,
) -> Option<FirmwareTargetMapping> {
    let target_key = target_key_from_component_key(component_key);
    if !firmware_component_allowed_for_node_type(node_type, &target_key) {
        return None;
    }
    get_firmware_component_mappings_for_node_type(node_type)
        .into_iter()
        .find(|mapping| {
            mapping.target_key == target_key
                && mapping.source_component == entry.component
                && mapping.redfish_target == entry.target
        })
}

fn get_firmware_flash_order(node_type: NodeType) -> &'static [&'static str] {
    match node_type {
        NodeType::ComputeGb300Supermicro => &[SUPERMICRO_HGX_TARGET, SUPERMICRO_BIOS_TARGET, ""],
        NodeType::SwitchVrnvl72Nvidia => &["bmc", "bios", "sma", "erot", "cpld1"],
        NodeType::SwitchGb200Nvidia | NodeType::SwitchGb300Nvidia => {
            &["bmc", "fpga", "erot", "cpld", "bios"]
        }
        NodeType::ComputeGb200Nvidia
        | NodeType::ComputeGb200Wiwynn
        | NodeType::ComputeGb300Nvidia
        | NodeType::ComputeGb300Lenovo
        | NodeType::ComputeVrnvl72Nvidia => &[
            "/redfish/v1/Chassis/HGX_Chassis_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_1",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_2",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_3",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_BMC_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_FPGA_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_FPGA_1",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPLD_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_BMC_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_CPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_CPU_1",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_FPGA_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_ERoT_FPGA_1",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_InfoROM_GPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_InfoROM_GPU_1",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_InfoROM_GPU_2",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_InfoROM_GPU_3",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_PCIeSwitchConfig_0",
            "",
        ],
        NodeType::PowershelfGb200Liteon
        | NodeType::PowershelfGb200Delta
        | NodeType::PowershelfGb300Liteon
        | NodeType::PowershelfGb300Delta => &["DeltaPMC", "LiteOnPMC", "DeltaPSU", "LiteOnPSU"],
    }
}

fn firmware_type_cache_subdir(firmware_type: &str) -> String {
    let normalized = firmware_type.trim().to_lowercase();
    let normalized = if normalized.is_empty() {
        "prod"
    } else {
        normalized.as_str()
    };
    let mut sanitized: String = normalized
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "prod".to_owned()
    } else {
        if sanitized.starts_with('.') {
            sanitized.replace_range(..1, "_");
        }

        sanitized
    }
}

fn cache_relative_filename(firmware_type: &str, filename: &str) -> String {
    format!("{}/{}", firmware_type_cache_subdir(firmware_type), filename)
}

fn switch_system_image_cache_path(
    cache_dir: &Path,
    image: &SwitchSystemImageLookupEntry,
) -> std::result::Result<PathBuf, ArtifactPathError> {
    artifact_cache_path(
        cache_dir,
        &firmware_type_cache_subdir(&image.firmware_type),
        &image.image_filename,
    )
}

fn required_json_string(
    value: &Value,
    field: &str,
    context: &str,
) -> std::result::Result<String, String> {
    let value = json_string(value, field);
    if value.is_empty() {
        return Err(format!("{context}.{field} is required"));
    }
    Ok(value)
}

fn optional_json_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn json_string(value: &Value, field: &str) -> String {
    optional_json_string(value, field).unwrap_or_default()
}

/// Reads and normalizes an optional firmware manifest `Sha256` digest (trimmed, lowercased).
/// Format validation is deferred to [`validate_firmware_object_sha256`] so a
/// malformed digest surfaces as a single actionable error.
fn optional_sha256(value: &Value) -> Option<String> {
    optional_json_string(value, "Sha256")
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
}

/// Rejects any firmware manifest `Sha256` digest that is not a 64-char hex string,
/// so a typo fails the add up front rather than as an opaque download error.
fn validate_firmware_object_sha256(
    parsed: &ParsedFirmwareComponents,
) -> std::result::Result<(), String> {
    for board_sku in &parsed.board_skus {
        for component in &board_sku.firmware_components {
            for location in &component.locations {
                if let Some(sha256) = location.sha256.as_deref()
                    && !is_valid_sha256_hex(sha256)
                {
                    return Err(format!(
                        "invalid Sha256 for component {} ({}): expected 64 hex characters",
                        component.component, location.context
                    ));
                }
            }
        }
    }
    for image in &parsed.switch_system_images {
        if let Some(sha256) = image.sha256.as_deref()
            && !is_valid_sha256_hex(sha256)
        {
            return Err(format!(
                "invalid Sha256 for switch system image {}: expected 64 hex characters",
                image.package_name
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::super::server::SwitchTlsRoots;
    use super::*;
    use crate::orchestrator::job_lifecycle::{JobLifecycleState, JobSpec};
    use crate::orchestrator::job_tracker::{JobTracker, RmsJobDomain};
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;

    fn test_service() -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::new()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            switch_tls_roots: SwitchTlsRoots::default(),
            sftp_upload_options: crate::transport::ssh::SftpUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        }
    }

    #[test]
    fn firmware_flash_order_is_selected_by_node_type() {
        assert_eq!(
            get_firmware_flash_order(NodeType::SwitchVrnvl72Nvidia),
            &["bmc", "bios", "sma", "erot", "cpld1"]
        );
        assert_eq!(
            get_firmware_flash_order(NodeType::SwitchGb200Nvidia),
            &["bmc", "fpga", "erot", "cpld", "bios"]
        );
        assert_eq!(
            get_firmware_flash_order(NodeType::ComputeVrnvl72Nvidia),
            get_firmware_flash_order(NodeType::ComputeGb200Nvidia)
        );
        assert_eq!(
            get_firmware_flash_order(NodeType::PowershelfGb300Delta),
            &["DeltaPMC", "LiteOnPMC", "DeltaPSU", "LiteOnPSU"]
        );
    }

    fn release_catalog(board_skus: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "ProductName": "TestProduct",
            "Milestones": [{
                "Name": "test-release",
                "State": "Released",
                "BoardSKUs": board_skus
            }]
        })
    }

    fn firmware_object_with_lookup(lookup: FirmwareLookupTable) -> FirmwareObject {
        FirmwareObject {
            id: "fw-1".to_owned(),
            rack_hardware_type: RackHardwareType("gb200".to_owned()),
            available: true,
            is_default: false,
            config: release_catalog(serde_json::json!([])),
            parsed_components: Some(serde_json::to_value(lookup).unwrap()),
            created: Utc::now(),
            updated: Utc::now(),
        }
    }

    fn duplicate_hmc_lookup_table() -> FirmwareLookupTable {
        let hmc_target = "/redfish/v1/Chassis/HGX_Chassis_0";
        let mut compute_entries = HashMap::new();
        compute_entries.insert(
            "HMC_prod_0001".to_owned(),
            FirmwareLookupEntry {
                filename: "prod/hmc.fwpkg".to_owned(),
                target: hmc_target.to_owned(),
                component: "HMC".to_owned(),
                bundle: "hmc-bundle".to_owned(),
                firmware_type: "prod".to_owned(),
                version: Some("1.2.3".to_owned()),
                subcomponents: Vec::new(),
            },
        );
        compute_entries.insert(
            "HMC_prod_0002".to_owned(),
            FirmwareLookupEntry {
                filename: "prod/partner-sbios.fwpkg".to_owned(),
                target: hmc_target.to_owned(),
                component: "HMC".to_owned(),
                bundle: "hmc-bundle".to_owned(),
                firmware_type: "prod".to_owned(),
                version: Some("1.2.3".to_owned()),
                subcomponents: Vec::new(),
            },
        );
        FirmwareLookupTable {
            devices: HashMap::from([("Compute Node".to_owned(), compute_entries)]),
            switch_system_images: HashMap::new(),
        }
    }

    fn hmc_lookup_table_from_firmware_manifest() -> FirmwareLookupTable {
        build_firmware_lookup_table(&ParsedFirmwareComponents {
            board_skus: vec![BoardSkuFirmware {
                sku_id: "test-compute-sku".to_owned(),
                name: "compute".to_owned(),
                sku_type: "Compute Node".to_owned(),
                firmware_components: vec![FirmwareComponent {
                    component: "HMC".to_owned(),
                    bundle: Some("hmc-bundle".to_owned()),
                    version: Some("1.2.3".to_owned()),
                    component_type: Some("prod".to_owned()),
                    context: String::new(),
                    locations: vec![FirmwareLocation {
                        location: "https://example.test/hmc.fwpkg".to_owned(),
                        location_type: "HTTPS".to_owned(),
                        firmware_type: Some("Firmware".to_owned()),
                        sha256: None,
                        context: String::new(),
                    }],
                    subcomponents: Vec::new(),
                }],
            }],
            switch_system_images: Vec::new(),
        })
    }

    fn supermicro_gb300_lookup_table_from_firmware_manifest() -> FirmwareLookupTable {
        build_firmware_lookup_table(&ParsedFirmwareComponents {
            board_skus: vec![BoardSkuFirmware {
                sku_id: "test-supermicro-compute-sku".to_owned(),
                name: "Test Supermicro compute".to_owned(),
                sku_type: "Compute Node".to_owned(),
                firmware_components: vec![
                    FirmwareComponent {
                        component: "HMC".to_owned(),
                        bundle: Some("supermicro-gb300".to_owned()),
                        version: Some("1.0.6".to_owned()),
                        component_type: Some("prod".to_owned()),
                        context: String::new(),
                        locations: vec![
                            FirmwareLocation {
                                location: "https://example.test/test-bios-image.bin".to_owned(),
                                location_type: "HTTPS".to_owned(),
                                firmware_type: Some("Firmware".to_owned()),
                                sha256: None,
                                context: String::new(),
                            },
                            FirmwareLocation {
                                location: "https://example.test/compute-nosbios.fwpkg".to_owned(),
                                location_type: "HTTPS".to_owned(),
                                firmware_type: Some("Firmware".to_owned()),
                                sha256: None,
                                context: String::new(),
                            },
                        ],
                        subcomponents: Vec::new(),
                    },
                    FirmwareComponent {
                        component: "BMC".to_owned(),
                        bundle: Some("supermicro-gb300".to_owned()),
                        version: Some("70.02.01.05".to_owned()),
                        component_type: Some("prod".to_owned()),
                        context: String::new(),
                        locations: vec![FirmwareLocation {
                            location: "https://example.test/NvOBMC_test-image.bin".to_owned(),
                            location_type: "HTTPS".to_owned(),
                            firmware_type: Some("Firmware".to_owned()),
                            sha256: None,
                            context: String::new(),
                        }],
                        subcomponents: Vec::new(),
                    },
                ],
            }],
            switch_system_images: Vec::new(),
        })
    }

    fn duplicate_bmc_lookup_table_from_firmware_manifest() -> FirmwareLookupTable {
        build_firmware_lookup_table(&ParsedFirmwareComponents {
            board_skus: vec![BoardSkuFirmware {
                sku_id: "test-compute-sku".to_owned(),
                name: "compute".to_owned(),
                sku_type: "Compute Node".to_owned(),
                firmware_components: vec![FirmwareComponent {
                    component: "BMC".to_owned(),
                    bundle: Some("wiwynn-bmc-bundle".to_owned()),
                    version: Some("1.2.3".to_owned()),
                    component_type: Some("prod".to_owned()),
                    context: String::new(),
                    locations: vec![
                        FirmwareLocation {
                            location: "https://example.test/bmc-primary.fwpkg".to_owned(),
                            location_type: "HTTPS".to_owned(),
                            firmware_type: Some("Firmware".to_owned()),
                            sha256: None,
                            context: String::new(),
                        },
                        FirmwareLocation {
                            location: "https://example.test/bmc-secondary.fwpkg".to_owned(),
                            location_type: "HTTPS".to_owned(),
                            firmware_type: Some("Firmware".to_owned()),
                            sha256: None,
                            context: String::new(),
                        },
                    ],
                    subcomponents: Vec::new(),
                }],
            }],
            switch_system_images: Vec::new(),
        })
    }

    fn mixed_powershelf_lookup_table() -> FirmwareLookupTable {
        let power_shelf_entries = HashMap::from([
            (
                "DeltaPMC_prod_0001".to_owned(),
                FirmwareLookupEntry {
                    filename: "prod/delta-pmc.fwpkg".to_owned(),
                    target: String::new(),
                    component: "Delta-PMC".to_owned(),
                    bundle: String::new(),
                    firmware_type: "prod".to_owned(),
                    version: Some("3.2.4".to_owned()),
                    subcomponents: Vec::new(),
                },
            ),
            (
                "DeltaPSU_prod_0001".to_owned(),
                FirmwareLookupEntry {
                    filename: "prod/delta-psu.tar".to_owned(),
                    target: String::new(),
                    component: "Delta-PSU".to_owned(),
                    bundle: String::new(),
                    firmware_type: "prod".to_owned(),
                    version: Some("0104".to_owned()),
                    subcomponents: Vec::new(),
                },
            ),
            (
                "LiteOnPMC_prod_0001".to_owned(),
                FirmwareLookupEntry {
                    filename: "prod/liteon-pmc.tar".to_owned(),
                    target: String::new(),
                    component: "LiteOn PMC".to_owned(),
                    bundle: String::new(),
                    firmware_type: "prod".to_owned(),
                    version: Some("1.3.10".to_owned()),
                    subcomponents: Vec::new(),
                },
            ),
            (
                "LiteOnPSU_prod_0001".to_owned(),
                FirmwareLookupEntry {
                    filename: "prod/liteon-psu.tar".to_owned(),
                    target: String::new(),
                    component: "LiteOn PSU".to_owned(),
                    bundle: String::new(),
                    firmware_type: "prod".to_owned(),
                    version: Some("0101".to_owned()),
                    subcomponents: Vec::new(),
                },
            ),
        ]);

        FirmwareLookupTable {
            devices: HashMap::from([("Power Shelf".to_owned(), power_shelf_entries)]),
            switch_system_images: HashMap::new(),
        }
    }

    #[test]
    fn firmware_object_id_must_be_single_safe_path_component() {
        assert!(validate_firmware_object_id("fw-1").is_ok());
        assert!(validate_firmware_object_id("").is_err());
        assert!(validate_firmware_object_id("..").is_err());
        assert!(validate_firmware_object_id("../evil").is_err());
        assert!(validate_firmware_object_id("nested/evil").is_err());
        assert!(validate_firmware_object_id("nested\\evil").is_err());
    }

    #[test]
    fn accepted_batch_reports_queued_nodes_without_completed_successes() {
        let response = accepted_batch("job-1".to_owned(), 3, "queued");
        let Some(stats) = response.stats.as_ref() else {
            panic!("accepted batch should include stats");
        };

        assert_eq!(stats.total_nodes, 3);
        assert_eq!(stats.successful_nodes, 0);
        assert_eq!(stats.failed_nodes, 0);
    }

    #[tokio::test]
    async fn apply_firmware_object_stats_count_created_and_rejected_jobs() {
        let service = test_service();
        let mut nodes = Vec::new();
        let mut active_jobs = Vec::new();
        for idx in 0..3 {
            let node_id = format!("node-active-{idx}");
            let active = service
                .job_tracker
                .create_job("rack-1", &node_id, JobType::FirmwareUpdate)
                .unwrap();
            active.progress("running");
            active_jobs.push(active);

            nodes.push(rm::NodeInfo {
                node_id,
                rack_id: "rack-1".to_owned(),
                r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                ..Default::default()
            });
        }

        for idx in 0..7 {
            nodes.push(rm::NodeInfo {
                node_id: format!("node-created-{idx}"),
                rack_id: "rack-1".to_owned(),
                r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                ..Default::default()
            });
        }

        let request = rm::ApplyFirmwareObjectRequest {
            rack_id: "rack-1".to_owned(),
            config_json: "not-json".to_owned(),
            access_token: Some("token".to_owned()),
            firmware_type: "prod".to_owned(),
            hardware_type: "gb200".to_owned(),
            nodes: Some(rm::NodeSet { nodes }),
            ..Default::default()
        };

        let Ok(response) = service
            .handle_apply_firmware_object(Request::new(request))
            .await
        else {
            panic!("apply firmware object should return a response");
        };

        let response = response.into_inner();
        let Some(batch) = response.response.as_ref() else {
            panic!("batch response should be present");
        };

        let Some(stats) = batch.stats.as_ref() else {
            panic!("stats should be present");
        };

        assert_eq!(stats.total_nodes, 10);
        assert_eq!(stats.successful_nodes, 7);
        assert_eq!(stats.failed_nodes, 3);
        assert_eq!(response.jobs.len(), 7);
        assert_eq!(batch.node_results.len(), 3);
    }

    #[tokio::test]
    async fn apply_firmware_object_reports_parent_creation_failure_when_tracker_full() {
        let service = RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::builder().max_tracked_jobs(1).build().unwrap()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            switch_tls_roots: SwitchTlsRoots::default(),
            sftp_upload_options: crate::transport::ssh::SftpUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        };

        let occupant = service
            .job_tracker
            .create_job("rack-1", "occupant", JobType::FirmwareUpdate)
            .unwrap();
        occupant.progress("running");

        let request = rm::ApplyFirmwareObjectRequest {
            rack_id: "rack-1".to_owned(),
            config_json: "not-json".to_owned(),
            access_token: Some("token".to_owned()),
            firmware_type: "prod".to_owned(),
            hardware_type: "gb200".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "node-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };

        let response = service
            .handle_apply_firmware_object(Request::new(request))
            .await
            .expect("handler should return a response")
            .into_inner();

        assert!(response.jobs.is_empty());
        let batch = response.response.expect("batch response should be present");
        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert!(batch.job_id.is_empty());
        assert_eq!(
            batch.message,
            "failed to create parent firmware object apply job"
        );
    }

    #[tokio::test]
    async fn apply_switch_system_image_reports_parent_creation_failure_when_tracker_full() {
        let service = RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::builder().max_tracked_jobs(1).build().unwrap()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            switch_tls_roots: SwitchTlsRoots::default(),
            sftp_upload_options: crate::transport::ssh::SftpUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
        };

        let occupant = service
            .job_tracker
            .create_job("rack-1", "occupant", JobType::SwitchSystemImageUpdate)
            .unwrap();
        occupant.progress("running");

        let request = rm::ApplySwitchSystemImageRequest {
            rack_id: "rack-1".to_owned(),
            config_json: "not-json".to_owned(),
            hardware_type: "gb200".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "sw-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };

        let response = service
            .handle_apply_switch_system_image(Request::new(request))
            .await
            .expect("handler should return a response")
            .into_inner();

        assert!(response.jobs.is_empty());
        let batch = response.response.expect("batch response should be present");
        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert!(batch.job_id.is_empty());
        assert_eq!(
            batch.message,
            "failed to create parent switch system image apply job"
        );
    }

    #[test]
    fn switch_image_parse_skips_documentation_locations_with_percent_encoded_filenames() {
        // Documentation URLs (PDFs) with percent-encoded spaces/parens appear in the
        // same Locations array as the NVOS .bin in real firmware object JSON files.
        // select_nvos_location must skip them rather than propagating an error so that
        // the valid .bin location is still selected.
        let config: serde_json::Value = serde_json::from_str(include_str!(
            "test_fixtures/nvos_percent_encoded_locations.json"
        ))
        .expect("fixture should be valid JSON");

        let parsed = parse_firmware_object_config_value("fw-1", &config)
            .expect("percent-encoded documentation URLs should not block switch image selection");

        assert_eq!(parsed.switch_system_images.len(), 1);
        let image = &parsed.switch_system_images[0];
        assert_eq!(image.firmware_type, "prod");
        assert_eq!(image.image_filename, "nvos-amd64-25.02.2553.bin");
    }

    #[test]
    fn firmware_target_from_json_parse_skips_switch_system_image_metadata() {
        let config = release_catalog(serde_json::json!([{
            "Type": "Switch Tray",
            "Components": {
                "Software": [{
                    "Component": "NVOS",
                    "Type": "Prod",
                    "Version": "25.02.2351",
                    "Locations": [
                        {
                            "Name": "NVOS Artifact",
                            "Location": "https://example.test/release/25.02.2441/amd64/prod/nvos-amd64-25.02.2441.bin",
                            "LocationType": "HTTPS",
                            "Version": "25.02.2441"
                        },
                        {
                            "Name": "NVOS Artifact",
                            "Location": "https://example.test/release/25.02.2351/amd64/prod/nvos-amd64-25.02.2351.bin",
                            "LocationType": "HTTPS",
                            "Version": "25.02.2351"
                        }
                    ]
                }]
            }
        }]));

        let parsed = parse_firmware_object_config_value_with_scope(
            "fw-1",
            &config,
            FirmwareObjectArtifactScope::FirmwareTargets,
        )
        .expect("firmware target parse should ignore ambiguous switch image metadata");
        assert!(parsed.switch_system_images.is_empty());

        let err = parse_firmware_object_config_value_with_scope(
            "fw-1",
            &config,
            FirmwareObjectArtifactScope::SwitchSystemImages,
        )
        .expect_err("switch image parse should still validate switch image metadata");
        assert!(err.contains("multiple NVOS .bin image candidates"));
    }

    #[test]
    fn firmware_object_id_derives_from_single_milestone_regardless_of_state() {
        let config = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [{
                "Name": "1.3.5-GA",
                "State": "Draft",
                "BoardSKUs": []
            }]
        });

        let id = firmware_object_id_from_config(&config).expect("new-format ID should derive");

        assert_eq!(id, "GB200NVL_72x1_1.3.5-GA");
    }

    #[test]
    fn milestone_selection_rejects_empty_or_multiple_milestones() {
        let empty_milestones = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": []
        });
        let err = firmware_object_id_from_config(&empty_milestones)
            .expect_err("new-format JSON without a milestone should fail");
        assert!(err.contains("found 0"));

        let multiple_milestones = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [
                {
                    "Name": "1.3.5-GA",
                    "State": "Released",
                    "BoardSKUs": []
                },
                {
                    "Name": "1.3.6-GA",
                    "State": "Released",
                    "BoardSKUs": []
                }
            ]
        });
        let err = firmware_object_id_from_config(&multiple_milestones)
            .expect_err("new-format JSON with multiple milestones should fail");
        assert!(err.contains("found 2"));
    }

    #[test]
    fn legacy_top_level_id_and_board_skus_are_rejected() {
        let legacy = serde_json::json!({
            "Id": "fw-1",
            "BoardSKUs": []
        });

        let err = firmware_object_id_from_config(&legacy)
            .expect_err("legacy ID-only JSON should not derive an object ID");
        assert!(err.contains("ProductName"));

        let legacy_with_product_name = serde_json::json!({
            "ProductName": "LegacyProduct",
            "BoardSKUs": []
        });
        let err = parse_firmware_object_config(&legacy_with_product_name)
            .expect_err("top-level BoardSKUs should not parse");
        assert!(err.contains("Milestones"));
    }

    #[test]
    fn new_format_gb200_release_catalog_parses_firmware_and_switch_image() {
        let config = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [{
                "Name": "1.3.5-GA",
                "State": "Released",
                "BoardSKUs": [
                    {
                        "Name": "Sample Switch",
                        "Type": "Switch Tray",
                        "Components": {
                            "Software": [{
                                "Component": "NVOS",
                                "Type": "Prod",
                                "Version": "25.02.2553",
                                "Locations": [{
                                    "Location": "https://example.test/release/25.02.2553/amd64/prod/nvos-amd64-25.02.2553.bin",
                                    "LocationType": "HTTPS",
                                    "PackageName": "",
                                    "Type": "Misc",
                                    "FileName": "nvos-amd64-25.02.2553.bin"
                                }]
                            }],
                            "Firmware": [{
                                "Component": "BMC+FPGA+EROT",
                                "Type": "Prod",
                                "Version": "test-switch-firmware-version",
                                "Locations": [{
                                    "Location": "https://example.test/switch-bmc.fwpkg",
                                    "LocationType": "HTTPS",
                                    "Type": "Firmware"
                                }]
                            }]
                        }
                    },
                    {
                        "Name": "Sample Compute",
                        "Type": "Compute Node",
                        "Components": {
                            "Firmware": [{
                                "Component": "HMC",
                                "Type": "Prod",
                                "Version": "",
                                "Locations": [
                                    {
                                        "Location": "https://example.test/compute-sbios.zip",
                                        "LocationType": "HTTPS",
                                        "PackageName": "",
                                        "Type": "Firmware",
                                        "FileName": "compute-sbios.zip"
                                    },
                                    {
                                        "Location": "https://example.test/compute-sbios.corim",
                                        "LocationType": "HTTPS",
                                        "PackageName": "",
                                        "Type": "Certificate",
                                        "FileName": "compute-sbios.corim"
                                    }
                                ],
                                "SubComponents": [{
                                    "Component": "SBIOS",
                                    "Version": "02.04.14"
                                }]
                            }]
                        }
                    }
                ]
            }]
        });

        let parsed = parse_firmware_object_config_value("GB200NVL_72x1_1.3.5-GA", &config)
            .expect("new-format release catalog should parse");

        assert_eq!(parsed.board_skus.len(), 2);
        assert_eq!(
            lookup_key_from_board_type(&parsed.board_skus[0].sku_type),
            Some(SWITCH_LOOKUP_KEY)
        );
        assert_eq!(
            lookup_key_from_board_type(&parsed.board_skus[1].sku_type),
            Some(COMPUTE_LOOKUP_KEY)
        );
        assert_eq!(parsed.board_skus[1].firmware_components[0].component, "HMC");
        assert_eq!(
            parsed.board_skus[1].firmware_components[0]
                .component_type
                .as_deref(),
            Some("Prod")
        );

        let lookup = build_firmware_lookup_table(&parsed);
        let compute = lookup
            .devices
            .get("Compute Node")
            .expect("compute lookup should exist");
        assert_eq!(compute["HMC_prod_0001"].filename, "prod/compute-sbios.zip");
        assert_eq!(parsed.switch_system_images[0].package_name, "GB200NVL_72x1");
        assert_eq!(parsed.switch_system_images[0].firmware_type, "prod");
        assert_eq!(
            parsed.switch_system_images[0].image_filename,
            "nvos-amd64-25.02.2553.bin"
        );
    }

    #[test]
    fn add_object_parser_rejects_needed_firmware_location_missing_location() {
        let config = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [{
                "Name": "1.3.5-GA",
                "State": "Released",
                "BoardSKUs": [{
                    "Name": "Sample Board",
                    "Type": "Compute Node",
                    "Components": {
                        "Firmware": [{
                            "Component": "HMC",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "",
                                "LocationType": "",
                                "Type": "Firmware",
                                "FileName": "compute-sbios.zip"
                            }]
                        }]
                    }
                }]
            }]
        });

        let err = parse_firmware_object_config_value("GB200NVL_72x1_1.3.5-GA", &config)
            .expect_err("add-object parser should reject missing firmware Location");

        assert!(err.contains("HMC"));
        assert!(err.contains("Type \"Firmware\""));
        assert!(err.contains("missing Location"));
    }

    #[test]
    fn add_object_parser_rejects_needed_firmware_component_missing_type() {
        let config = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [{
                "Name": "1.3.5-GA",
                "State": "Released",
                "BoardSKUs": [{
                    "Name": "Sample Compute",
                    "Type": "Compute Node",
                    "Components": {
                        "Firmware": [{
                            "Component": "HMC",
                            "Locations": [{
                                "Location": "https://example.test/compute-sbios.zip",
                                "LocationType": "HTTPS",
                                "Type": "Firmware",
                                "FileName": "compute-sbios.zip"
                            }]
                        }]
                    }
                }]
            }]
        });

        let err = parse_firmware_object_config_value("GB200NVL_72x1_1.3.5-GA", &config)
            .expect_err("add-object parser should reject missing firmware component Type");

        assert!(err.contains("HMC"));
        assert!(err.contains("missing Type"));
    }

    #[test]
    fn apply_validation_rejects_missing_location_only_for_selected_component() {
        let config = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [{
                "Name": "1.3.5-GA",
                "State": "Released",
                "BoardSKUs": [{
                    "Name": "Sample Compute",
                    "Type": "Compute Node",
                    "Components": {
                        "Firmware": [
                            {
                                "Component": "BMC",
                                "Type": "Prod",
                                "Locations": [{
                                    "Location": "https://example.test/bmc.fwpkg",
                                    "LocationType": "HTTPS",
                                    "Type": "Firmware"
                                }]
                            },
                            {
                                "Component": "HMC",
                                "Type": "Prod",
                                "Locations": [{
                                    "Location": "",
                                    "LocationType": "",
                                    "Type": "Firmware",
                                    "FileName": "compute-sbios.zip"
                                }]
                            }
                        ]
                    }
                }]
            }]
        });
        let nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "compute-1".to_owned(),
                rack_id: "rack-1".to_owned(),
                r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                ..Default::default()
            }],
        };

        let mut parsed = parse_firmware_object_config_value_with_scope(
            "GB200NVL_72x1_1.3.5-GA",
            &config,
            FirmwareObjectArtifactScope::FirmwareTargets,
        )
        .expect("firmware target parse should defer missing-location validation");
        filter_parsed_components_for_apply(
            &mut parsed,
            Some(&nodes),
            "prod",
            &HashMap::from([(
                rm::NodeType::ComputeGb200Nvidia as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["BMC".to_owned()],
                },
            )]),
            &[],
        )
        .expect("BMC selection should succeed");
        validate_required_download_locations(&parsed)
            .expect("unselected missing HMC location should be ignored");

        let mut parsed = parse_firmware_object_config_value_with_scope(
            "GB200NVL_72x1_1.3.5-GA",
            &config,
            FirmwareObjectArtifactScope::FirmwareTargets,
        )
        .expect("firmware target parse should defer missing-location validation");
        filter_parsed_components_for_apply(
            &mut parsed,
            Some(&nodes),
            "prod",
            &HashMap::from([(
                rm::NodeType::ComputeGb200Nvidia as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["HMC".to_owned()],
                },
            )]),
            &[],
        )
        .expect("HMC selection should succeed");
        let err = validate_required_download_locations(&parsed)
            .expect_err("selected missing HMC location should fail");
        assert!(err.contains("HMC"));
        assert!(err.contains("missing Location"));
    }

    #[test]
    fn apply_validation_rejects_missing_type_only_for_selected_component() {
        let config = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [{
                "Name": "1.3.5-GA",
                "State": "Released",
                "BoardSKUs": [{
                    "Name": "Sample Compute",
                    "Type": "Compute Node",
                    "Components": {
                        "Firmware": [
                            {
                                "Component": "BMC",
                                "Type": "Prod",
                                "Locations": [{
                                    "Location": "https://example.test/bmc.fwpkg",
                                    "LocationType": "HTTPS",
                                    "Type": "Firmware"
                                }]
                            },
                            {
                                "Component": "HMC",
                                "Locations": [{
                                    "Location": "https://example.test/hmc.fwpkg",
                                    "LocationType": "HTTPS",
                                    "Type": "Firmware"
                                }]
                            }
                        ]
                    }
                }]
            }]
        });
        let nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "compute-1".to_owned(),
                rack_id: "rack-1".to_owned(),
                r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                ..Default::default()
            }],
        };

        let mut parsed = parse_firmware_object_config_value_with_scope(
            "GB200NVL_72x1_1.3.5-GA",
            &config,
            FirmwareObjectArtifactScope::FirmwareTargets,
        )
        .expect("firmware target parse should defer missing-type validation");
        filter_parsed_components_for_apply(
            &mut parsed,
            Some(&nodes),
            "prod",
            &HashMap::from([(
                rm::NodeType::ComputeGb200Nvidia as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["BMC".to_owned()],
                },
            )]),
            &[],
        )
        .expect("BMC selection should succeed");
        validate_required_download_locations(&parsed)
            .expect("unselected missing HMC type should be ignored");

        let mut parsed = parse_firmware_object_config_value_with_scope(
            "GB200NVL_72x1_1.3.5-GA",
            &config,
            FirmwareObjectArtifactScope::FirmwareTargets,
        )
        .expect("firmware target parse should defer missing-type validation");
        filter_parsed_components_for_apply(
            &mut parsed,
            Some(&nodes),
            "prod",
            &HashMap::from([(
                rm::NodeType::ComputeGb200Nvidia as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["HMC".to_owned()],
                },
            )]),
            &[],
        )
        .expect("HMC selection should succeed");
        let err = validate_required_download_locations(&parsed)
            .expect_err("selected missing HMC type should fail");
        assert!(err.contains("HMC"));
        assert!(err.contains("missing Type"));
    }

    #[test]
    fn new_format_switch_image_requires_real_bin_location_when_needed() {
        let config = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [{
                "Name": "1.3.5-GA",
                "State": "Released",
                "BoardSKUs": [{
                    "Name": "Sample_Switch",
                    "Type": "Switch Tray",
                    "Components": {
                        "Software": [{
                            "Component": "NVOS",
                            "Type": "Prod",
                            "Version": "25.02.2553",
                            "Locations": [{
                                "Location": "",
                                "LocationType": "",
                                "PackageName": "",
                                "Type": "Misc",
                                "FileName": "nvos-amd64-25.02.2553.bin"
                            }]
                        }]
                    }
                }]
            }]
        });

        let err = parse_firmware_object_config_value_with_scope(
            "GB200NVL_72x1_1.3.5-GA",
            &config,
            FirmwareObjectArtifactScope::SwitchSystemImages,
        )
        .expect_err("switch image parse should require a real image URL");

        assert!(err.contains("NVOS .bin image location"));
    }

    #[test]
    fn new_format_switch_image_requires_type_when_needed() {
        let config = serde_json::json!({
            "ProductName": "GB200NVL_72x1",
            "Milestones": [{
                "Name": "1.3.5-GA",
                "State": "Released",
                "BoardSKUs": [{
                    "Name": "Sample_Switch",
                    "Type": "Switch Tray",
                    "Components": {
                        "Software": [{
                            "Component": "NVOS",
                            "Version": "25.02.2553",
                            "Locations": [{
                                "Location": "https://example.test/release/25.02.2553/amd64/prod/nvos-amd64-25.02.2553.bin",
                                "LocationType": "HTTPS",
                                "PackageName": "",
                                "Type": "Misc",
                                "FileName": "nvos-amd64-25.02.2553.bin"
                            }]
                        }]
                    }
                }]
            }]
        });

        let err = parse_firmware_object_config_value_with_scope(
            "GB200NVL_72x1_1.3.5-GA",
            &config,
            FirmwareObjectArtifactScope::SwitchSystemImages,
        )
        .expect_err("switch image parse should require software Type");

        assert!(err.contains("Type is required"));
    }

    #[test]
    fn precreate_firmware_jobs_rejects_active_node_job() {
        let service = test_service();
        let active = service
            .job_tracker
            .create_job("rack-1", "node-1", JobType::FirmwareUpdate)
            .unwrap();
        active.progress("running");
        let device = rm::NodeInfo {
            node_id: "node-1".to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            ..Default::default()
        };

        let parent_job_id = service
            .job_tracker
            .create_parent_job("rack-1", JobType::FirmwareUpdate)
            .expect("parent job should be created");
        let precreated = service.precreate_firmware_jobs(parent_job_id, vec![device]);

        assert!(!precreated.parent_job_id.is_empty());
        assert!(precreated.jobs.is_empty());
        assert!(precreated.node_jobs.is_empty());
        assert_eq!(precreated.node_results.len(), 1);
        assert_eq!(precreated.failed_nodes, 1);
        assert!(
            precreated.node_results[0]
                .error_message
                .contains(active.id().as_ref())
        );
        assert_eq!(
            service
                .job_tracker
                .get_job(&precreated.parent_job_id)
                .unwrap()
                .state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
    }

    #[test]
    fn precreate_firmware_jobs_rejects_unresolved_profiles_without_child_jobs() {
        let service = test_service();
        let unknown_device = rm::NodeInfo {
            node_id: "node-1".to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            node_descriptor: Some(rm::NodeDescriptor {
                attributes: HashMap::from([(
                    crate::api::grpc::node_type_resolver::INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                    "unknown-profile".to_owned(),
                )]),
            }),
            ..Default::default()
        };
        let empty_device = rm::NodeInfo {
            node_id: "node-2".to_owned(),
            rack_id: "rack-1".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            node_descriptor: Some(rm::NodeDescriptor {
                attributes: HashMap::from([(
                    crate::api::grpc::node_type_resolver::INVENTORY_PROFILE_ATTRIBUTE.to_owned(),
                    " ".to_owned(),
                )]),
            }),
            ..Default::default()
        };

        let parent_job_id = service
            .job_tracker
            .create_parent_job("rack-1", JobType::FirmwareUpdate)
            .expect("parent job should be created");
        let precreated =
            service.precreate_firmware_jobs(parent_job_id, vec![unknown_device, empty_device]);

        assert!(!precreated.parent_job_id.is_empty());
        assert!(precreated.jobs.is_empty());
        assert!(precreated.node_jobs.is_empty());
        assert_eq!(precreated.failed_nodes, 2);
        assert_eq!(precreated.node_results.len(), 2);
        assert!(
            precreated.node_results[0]
                .error_message
                .contains("unknown inventory_profile")
        );
        assert!(
            precreated.node_results[1]
                .error_message
                .contains("inventory_profile must not be empty")
        );
        assert_eq!(
            service
                .job_tracker
                .get_job(&precreated.parent_job_id)
                .unwrap()
                .state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
    }

    #[test]
    fn failing_precreated_firmware_jobs_seals_handles_and_refreshes_parent() {
        let service = test_service();
        let parent_job_id = service
            .job_tracker
            .create_parent_job("rack-1", JobType::FirmwareUpdate)
            .expect("parent job should be created");
        let jobs: Vec<PrecreatedFirmwareJob> = ["node-1", "node-2"]
            .into_iter()
            .map(|node_id| PrecreatedFirmwareJob {
                device: rm::NodeInfo {
                    rack_id: "rack-1".to_owned(),
                    node_id: node_id.to_owned(),
                    ..Default::default()
                },
                pending: service
                    .job_tracker
                    .create_child_job(&parent_job_id, "rack-1", node_id, JobType::FirmwareUpdate)
                    .expect("child job should be created"),
                expected_inventory: None,
            })
            .collect();
        let child_job_ids = firmware_job_ids(&jobs);

        mark_firmware_jobs_failed(
            &service.job_tracker,
            jobs,
            JobError::InvalidArgument,
            "invalid firmware object",
        );

        let parent = service
            .job_tracker
            .registry()
            .get(&JobId::from(parent_job_id.as_str()))
            .expect("parent job should exist");
        assert!(matches!(parent.state, JobLifecycleState::Failed { .. }));
        for child_job_id in child_job_ids {
            let child = service
                .job_tracker
                .registry()
                .get(&JobId::from(child_job_id))
                .expect("child job should exist");
            let JobLifecycleState::Failed { message, .. } = child.state else {
                panic!("child job should be failed");
            };
            assert_eq!(message, "invalid firmware object");
        }
    }

    #[test]
    fn failing_precreated_switch_jobs_seals_handles_and_refreshes_parent() {
        let service = test_service();
        let parent_job_id = service
            .job_tracker
            .create_parent_job("rack-1", JobType::SwitchSystemImageUpdate)
            .expect("parent job should be created");
        let jobs: Vec<PrecreatedSwitchImageJob> = ["switch-1", "switch-2"]
            .into_iter()
            .map(|node_id| PrecreatedSwitchImageJob {
                device: rm::NodeInfo {
                    rack_id: "rack-1".to_owned(),
                    node_id: node_id.to_owned(),
                    ..Default::default()
                },
                pending: service
                    .job_tracker
                    .create_child_job(
                        &parent_job_id,
                        "rack-1",
                        node_id,
                        JobType::SwitchSystemImageUpdate,
                    )
                    .expect("child job should be created"),
            })
            .collect();
        let child_job_ids = switch_image_job_ids(&jobs);

        mark_switch_jobs_failed(&service.job_tracker, jobs, "invalid switch image");

        let parent = service
            .job_tracker
            .registry()
            .get(&JobId::from(parent_job_id.as_str()))
            .expect("parent job should exist");
        assert!(matches!(parent.state, JobLifecycleState::Failed { .. }));
        for child_job_id in child_job_ids {
            let child = service
                .job_tracker
                .registry()
                .get(&JobId::from(child_job_id))
                .expect("child job should exist");
            let JobLifecycleState::Failed { message, .. } = child.state else {
                panic!("child job should be failed");
            };
            assert_eq!(message, "invalid switch image");
        }
    }

    // The ephemeral object cache cleanup is a generic job_lifecycle operation
    // shared by firmware and switch batches, so exercise it directly against a
    // bare JobRegistry rather than standing up a full JobTracker.
    #[tokio::test]
    async fn cleanup_removes_ephemeral_cache_after_jobs_terminal() {
        let registry = Arc::new(JobRegistry::<RmsJobDomain>::new(Duration::from_secs(3600)));
        let pending = registry
            .create_leaf(JobSpec::new(
                "rack-1",
                "switch-1",
                "Queued",
                tracing::Span::none(),
            ))
            .unwrap();
        let job_id = pending.id().to_string();

        let temp = tempfile::tempdir().unwrap();
        let cache_dir = temp.path().join("firmware_objects_ephemeral").join("fw-1");
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(cache_dir.join("nvos.bin"), b"image")
            .await
            .unwrap();

        let cleanup = tokio::spawn(cleanup_ephemeral_object_cache_after_jobs(
            registry.clone(),
            vec![job_id.clone()],
            cache_dir.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(cache_dir.exists());

        pending.complete("done", "");
        tokio::time::timeout(Duration::from_secs(1), cleanup)
            .await
            .unwrap()
            .unwrap();
        assert!(!cache_dir.exists());
    }

    #[tokio::test]
    async fn cleanup_treats_missing_job_as_terminal() {
        let registry = Arc::new(JobRegistry::<RmsJobDomain>::new(Duration::from_secs(3600)));
        let temp = tempfile::tempdir().unwrap();
        let cache_dir = temp.path().join("firmware_objects_ephemeral").join("fw-1");
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();

        cleanup_ephemeral_object_cache_after_jobs(
            registry,
            vec!["missing-job".to_owned()],
            cache_dir.clone(),
        )
        .await;

        assert!(!cache_dir.exists());
    }

    // The detached tracked supervisor must still remove the cache directory once
    // the child jobs reach a terminal state, even though the caller never awaits
    // the join handle.
    #[tokio::test]
    async fn spawn_cleanup_removes_cache_after_child_terminal() {
        let registry = Arc::new(JobRegistry::<RmsJobDomain>::new(Duration::from_secs(3600)));
        let pending = registry
            .create_leaf(JobSpec::new(
                "rack-1",
                "switch-1",
                "Queued",
                tracing::Span::none(),
            ))
            .unwrap();
        let job_id = pending.id().to_string();

        let temp = tempfile::tempdir().unwrap();
        let cache_dir = temp.path().join("firmware_objects_ephemeral").join("fw-1");
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();

        spawn_ephemeral_object_cache_cleanup(
            registry.clone(),
            vec![job_id.clone()],
            cache_dir.clone(),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(cache_dir.exists());

        pending.complete("done", "");
        wait_until_removed(&cache_dir, Duration::from_secs(1)).await;
        assert!(!cache_dir.exists());
    }

    async fn wait_until_removed(path: &Path, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while path.exists() {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn download_artifacts_uses_firmware_type_cache_dirs() {
        let parsed = ParsedFirmwareComponents {
            board_skus: vec![BoardSkuFirmware {
                sku_id: "test-compute-sku".to_owned(),
                name: "compute".to_owned(),
                sku_type: "Compute Node".to_owned(),
                firmware_components: vec![FirmwareComponent {
                    component: "BMC".to_owned(),
                    bundle: Some("compute-bmc-bundle".to_owned()),
                    version: Some("1.2.3".to_owned()),
                    component_type: Some("Prod".to_owned()),
                    context: String::new(),
                    locations: vec![FirmwareLocation {
                        location: "https://example.test/compute-bmc.fwpkg".to_owned(),
                        location_type: "artifactory".to_owned(),
                        firmware_type: Some("Firmware".to_owned()),
                        sha256: None,
                        context: String::new(),
                    }],
                    subcomponents: Vec::new(),
                }],
            }],
            switch_system_images: vec![
                SwitchSystemImageArtifact {
                    device_type: "Switch Tray".to_owned(),
                    component: "NVOS".to_owned(),
                    version: "25.02.4440".to_owned(),
                    firmware_type: "prod".to_owned(),
                    package_name: "GB200NVL72_NVOS".to_owned(),
                    location: "https://example.test/amd64/prod/nvos-amd64-25.02.4440.bin"
                        .to_owned(),
                    location_type: "artifactory".to_owned(),
                    required: true,
                    image_filename: "nvos-amd64-25.02.4440.bin".to_owned(),
                    sha256: None,
                },
                SwitchSystemImageArtifact {
                    device_type: "Switch Tray".to_owned(),
                    component: "NVOS".to_owned(),
                    version: "25.02.4440".to_owned(),
                    firmware_type: "dev".to_owned(),
                    package_name: "GB200NVL72_NVOS".to_owned(),
                    location: "https://example.test/amd64/dev/nvos-amd64-25.02.4440.bin".to_owned(),
                    location_type: "artifactory".to_owned(),
                    required: true,
                    image_filename: "nvos-amd64-25.02.4440.bin".to_owned(),
                    sha256: None,
                },
            ],
        };

        let artifacts = build_download_artifacts(&parsed);

        assert_eq!(artifacts.len(), 3);
        assert_eq!(artifacts[0].relative_cache_dir.as_deref(), Some("prod"));
        assert_eq!(artifacts[1].relative_cache_dir.as_deref(), Some("prod"));
        assert_eq!(artifacts[2].relative_cache_dir.as_deref(), Some("dev"));
        assert_eq!(
            filename_from_location(&artifacts[1].url).unwrap(),
            filename_from_location(&artifacts[2].url).unwrap()
        );
    }

    #[test]
    fn switch_system_image_cache_path_uses_typed_cache_dir()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let image = SwitchSystemImageLookupEntry {
            component: "NVOS".to_owned(),
            package_name: "GB200NVL72_NVOS".to_owned(),
            version: "25.02.4440".to_owned(),
            image_filename: "nvos-amd64-25.02.4440.bin".to_owned(),
            location_type: "artifactory".to_owned(),
            firmware_type: "dev".to_owned(),
            required: true,
        };

        let typed_path = temp.path().join("dev").join(&image.image_filename);

        assert_eq!(
            switch_system_image_cache_path(temp.path(), &image)?,
            typed_path
        );

        Ok(())
    }

    #[test]
    fn firmware_type_cache_subdir_is_safe_for_download_and_apply()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let image_filename = "nvos-amd64-25.02.4440.bin".to_owned();
        let parsed = ParsedFirmwareComponents {
            board_skus: Vec::new(),
            switch_system_images: vec![SwitchSystemImageArtifact {
                device_type: "Switch Tray".to_owned(),
                component: "NVOS".to_owned(),
                version: "25.02.4440".to_owned(),
                firmware_type: ".dev".to_owned(),
                package_name: "GB200NVL72_NVOS".to_owned(),
                location: format!("https://example.test/amd64/dev/{image_filename}"),
                location_type: "artifactory".to_owned(),
                required: true,
                image_filename: image_filename.clone(),
                sha256: None,
            }],
        };

        let artifacts = build_download_artifacts(&parsed);
        let temp = tempfile::tempdir()?;
        let image = SwitchSystemImageLookupEntry {
            component: "NVOS".to_owned(),
            package_name: "GB200NVL72_NVOS".to_owned(),
            version: "25.02.4440".to_owned(),
            image_filename,
            location_type: "artifactory".to_owned(),
            firmware_type: ".dev".to_owned(),
            required: true,
        };

        assert_eq!(artifacts[0].relative_cache_dir.as_deref(), Some("_dev"));
        assert_eq!(
            switch_system_image_cache_path(temp.path(), &image)?,
            temp.path().join("_dev").join(&image.image_filename)
        );

        Ok(())
    }

    #[test]
    fn switch_system_image_cache_path_rejects_unsafe_filename() {
        let image = SwitchSystemImageLookupEntry {
            component: "NVOS".to_owned(),
            package_name: "GB200NVL72_NVOS".to_owned(),
            version: "25.02.4440".to_owned(),
            image_filename: "../nvos-amd64-25.02.4440.bin".to_owned(),
            location_type: "artifactory".to_owned(),
            firmware_type: "dev".to_owned(),
            required: true,
        };

        assert_eq!(
            switch_system_image_cache_path(Path::new("/cache"), &image),
            Err(ArtifactPathError::UnsafeCharacters {
                filename: "../nvos-amd64-25.02.4440.bin".to_owned()
            })
        );
    }

    #[test]
    fn firmware_object_proto_exposes_typed_metadata() {
        let mut power_shelf_entries = HashMap::new();
        power_shelf_entries.insert(
            "DeltaPSU_prod".to_owned(),
            FirmwareLookupEntry {
                filename: "delta-psu.tar".to_owned(),
                target: String::new(),
                component: "Delta-PSU".to_owned(),
                bundle: "delta-powershelf-bundle".to_owned(),
                firmware_type: "prod".to_owned(),
                version: Some("1.2.3".to_owned()),
                subcomponents: vec![FirmwareSubComponent {
                    component: "PSU".to_owned(),
                    version: "4.5.6".to_owned(),
                    skuid: Some("sku-1".to_owned()),
                }],
            },
        );
        power_shelf_entries.insert(
            "LiteOnPSU_prod".to_owned(),
            FirmwareLookupEntry {
                filename: "liteon-psu.tar".to_owned(),
                target: String::new(),
                component: "LiteOn PSU".to_owned(),
                bundle: "liteon-powershelf-bundle".to_owned(),
                firmware_type: "prod".to_owned(),
                version: Some("0101".to_owned()),
                subcomponents: Vec::new(),
            },
        );
        let lookup = FirmwareLookupTable {
            devices: HashMap::from([("Power Shelf".to_owned(), power_shelf_entries)]),
            switch_system_images: HashMap::from([(
                "Switch Tray".to_owned(),
                HashMap::from([(
                    "NVOS_prod".to_owned(),
                    SwitchSystemImageLookupEntry {
                        component: "NVOS".to_owned(),
                        package_name: "nvos.pkg".to_owned(),
                        version: "25.01".to_owned(),
                        image_filename: "nvos.img".to_owned(),
                        location_type: "artifactory".to_owned(),
                        firmware_type: "prod".to_owned(),
                        required: true,
                    },
                )]),
            )]),
        };
        let proto = firmware_object_to_proto(firmware_object_with_lookup(lookup));
        let metadata = proto.metadata.expect("metadata should be populated");

        assert!(proto.created.is_some());
        assert!(proto.updated.is_some());
        assert_eq!(metadata.device_components.len(), 4);
        let node_types: HashSet<_> = metadata
            .device_components
            .iter()
            .map(|entry| entry.node_type)
            .collect();
        assert_eq!(
            node_types,
            HashSet::from([
                rm::NodeType::PowershelfGb200Liteon as i32,
                rm::NodeType::PowershelfGb200Delta as i32,
                rm::NodeType::PowershelfGb300Liteon as i32,
                rm::NodeType::PowershelfGb300Delta as i32,
            ])
        );

        let liteon_metadata = metadata
            .device_components
            .iter()
            .find(|entry| entry.node_type == rm::NodeType::PowershelfGb300Liteon as i32)
            .expect("GB300 LiteOn powershelf metadata should be present");
        assert_eq!(liteon_metadata.components.len(), 1);
        let component = &liteon_metadata.components[0];
        assert_eq!(component.name, "LiteOnPSU");
        assert_eq!(component.source_component, "LiteOn PSU");
        assert_eq!(component.artifacts[0].filename, "liteon-psu.tar");

        let delta_metadata = metadata
            .device_components
            .iter()
            .find(|entry| entry.node_type == rm::NodeType::PowershelfGb300Delta as i32)
            .expect("GB300 Delta powershelf metadata should be present");
        assert_eq!(delta_metadata.components.len(), 1);
        let component = &delta_metadata.components[0];
        assert_eq!(component.name, "DeltaPSU");
        assert_eq!(component.source_component, "Delta-PSU");
        assert_eq!(component.artifacts[0].filename, "delta-psu.tar");
        assert_eq!(component.artifacts[0].bundle, "delta-powershelf-bundle");
        assert_eq!(component.subcomponents[0].sku_id, "sku-1");
        assert_eq!(metadata.switch_system_images.len(), 1);
        assert_eq!(metadata.switch_system_images[0].image_filename, "nvos.img");
        assert!(metadata.switch_system_images[0].required);
    }

    #[test]
    fn apply_request_builds_delta_powershelf_target_from_component_filter() {
        let mut power_shelf_entries = HashMap::new();
        power_shelf_entries.insert(
            "DeltaPSU_prod".to_owned(),
            FirmwareLookupEntry {
                filename: "delta-psu.tar".to_owned(),
                target: String::new(),
                component: "Delta-PSU".to_owned(),
                bundle: "delta-powershelf-bundle".to_owned(),
                firmware_type: "prod".to_owned(),
                version: Some("1.2.3".to_owned()),
                subcomponents: Vec::new(),
            },
        );
        let lookup = FirmwareLookupTable {
            devices: HashMap::from([("Power Shelf".to_owned(), power_shelf_entries)]),
            switch_system_images: HashMap::new(),
        };
        let firmware = firmware_object_with_lookup(lookup);
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "ps-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::PowershelfGb200Delta as i32),
                    ..Default::default()
                }],
            }),
            component_filters: HashMap::from([(
                rm::NodeType::PowershelfGb200Delta as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["Delta-PSU".to_owned()],
                },
            )]),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::PowershelfGb200Delta as i32))
            .expect("powershelf targets should be present");

        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].target, "");
        assert_eq!(
            targets.targets[0].filename,
            "firmware_objects/fw-1/delta-psu.tar"
        );
    }

    #[test]
    fn generic_powershelffw_is_not_assigned_to_vendor_powershelf_metadata() {
        let mut power_shelf_entries = HashMap::new();
        power_shelf_entries.insert(
            "PowerShelfFW_prod".to_owned(),
            FirmwareLookupEntry {
                filename: "powershelf.bin".to_owned(),
                target: String::new(),
                component: "Power Shelf FW".to_owned(),
                bundle: "generic-powershelf-bundle".to_owned(),
                firmware_type: "prod".to_owned(),
                version: Some("1.2.3".to_owned()),
                subcomponents: Vec::new(),
            },
        );
        let lookup = FirmwareLookupTable {
            devices: HashMap::from([("Power Shelf".to_owned(), power_shelf_entries)]),
            switch_system_images: HashMap::new(),
        };

        let proto = firmware_object_to_proto(firmware_object_with_lookup(lookup));
        let metadata = proto.metadata.expect("metadata should be populated");

        assert!(metadata.device_components.is_empty());
    }

    #[test]
    fn lookup_table_maps_compute_bmc_to_empty_target() {
        let parsed = ParsedFirmwareComponents {
            board_skus: vec![BoardSkuFirmware {
                sku_id: "test-compute-sku".to_owned(),
                name: "compute".to_owned(),
                sku_type: "Compute Node".to_owned(),
                firmware_components: vec![FirmwareComponent {
                    component: "BMC".to_owned(),
                    bundle: Some("compute-bmc-bundle".to_owned()),
                    version: Some("1.2.3".to_owned()),
                    component_type: Some("prod".to_owned()),
                    context: String::new(),
                    locations: vec![FirmwareLocation {
                        location: "https://example.test/compute-bmc.fwpkg".to_owned(),
                        location_type: "artifactory".to_owned(),
                        firmware_type: Some("Firmware".to_owned()),
                        sha256: None,
                        context: String::new(),
                    }],
                    subcomponents: Vec::new(),
                }],
            }],
            switch_system_images: Vec::new(),
        };

        let lookup = build_firmware_lookup_table(&parsed);
        let entry = lookup.devices["Compute Node"]
            .get("BMC_prod_0001")
            .expect("compute BMC entry should exist");

        assert_eq!(entry.filename, "prod/compute-bmc.fwpkg");
        assert_eq!(entry.target, "");
    }

    #[test]
    fn lookup_table_preserves_multiple_firmware_payloads_per_target_key() {
        let parsed = ParsedFirmwareComponents {
            board_skus: vec![BoardSkuFirmware {
                sku_id: "test-compute-sku".to_owned(),
                name: "test-compute-board".to_owned(),
                sku_type: "Compute Node".to_owned(),
                firmware_components: vec![FirmwareComponent {
                    component: "HMC".to_owned(),
                    bundle: Some("hmc-bundle".to_owned()),
                    version: Some("1.2.3".to_owned()),
                    component_type: Some("Prod".to_owned()),
                    context: String::new(),
                    locations: vec![
                        FirmwareLocation {
                            location: "https://example.test/hmc.fwpkg".to_owned(),
                            location_type: "HTTPS".to_owned(),
                            firmware_type: Some("Firmware".to_owned()),
                            sha256: None,
                            context: String::new(),
                        },
                        FirmwareLocation {
                            location: "https://example.test/partner-sbios.fwpkg".to_owned(),
                            location_type: "HTTPS".to_owned(),
                            firmware_type: Some("Firmware".to_owned()),
                            sha256: None,
                            context: String::new(),
                        },
                        FirmwareLocation {
                            location: "https://example.test/hmc.corim".to_owned(),
                            location_type: "HTTPS".to_owned(),
                            firmware_type: Some("Certificate".to_owned()),
                            sha256: None,
                            context: String::new(),
                        },
                    ],
                    subcomponents: Vec::new(),
                }],
            }],
            switch_system_images: Vec::new(),
        };

        let lookup = build_firmware_lookup_table(&parsed);
        let compute = lookup
            .devices
            .get("Compute Node")
            .expect("compute entries should be present");

        assert_eq!(compute["HMC_prod_0001"].filename, "prod/hmc.fwpkg");
        assert_eq!(
            compute["HMC_prod_0002"].filename,
            "prod/partner-sbios.fwpkg"
        );
        assert_eq!(
            compute["HMC_prod_0002"].target,
            "/redfish/v1/Chassis/HGX_Chassis_0"
        );
        assert_eq!(
            compute["CPU_0_prod_0001"].target,
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0"
        );
        assert_eq!(
            compute["CPU_0_prod_0002"].filename,
            "prod/partner-sbios.fwpkg"
        );
    }

    #[test]
    fn metadata_exposes_fine_grained_targets() {
        let proto = firmware_object_to_proto(firmware_object_with_lookup(
            hmc_lookup_table_from_firmware_manifest(),
        ));
        let metadata = proto.metadata.expect("metadata should be populated");
        let compute_metadata = metadata
            .device_components
            .iter()
            .find(|entry| entry.node_type == rm::NodeType::ComputeGb200Nvidia as i32)
            .expect("GB200 compute metadata should be present");
        let cpu = compute_metadata
            .components
            .iter()
            .find(|component| component.name == "CPU_0")
            .expect("CPU_0 metadata should be present");

        assert_eq!(
            cpu.artifacts[0].target,
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0"
        );
    }

    #[test]
    fn apply_filter_prunes_ephemeral_downloads_to_requested_component() {
        let config = release_catalog(serde_json::json!([
            {
                "SKUID": "test-compute-sku",
                "Name": "test-compute-board",
                "Type": "Compute Tray",
                "Components": {
                    "Firmware": [
                        {
                            "Component": "BMC",
                            "Type": "Prod",
                            "Locations": [
                                {
                                    "Name": "Production_Package",
                                    "Location": "https://example.test/compute-bmc.fwpkg",
                                    "LocationType": "HTTPS",
                                    "Type": "Firmware"
                                },
                                {
                                    "Name": "Recovery_Package",
                                    "Location": "https://example.test/compute-bmc-recovery.fwpkg",
                                    "LocationType": "HTTPS",
                                    "Type": "Firmware"
                                }
                            ]
                        },
                        {
                            "Component": "HMC",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/compute-hmc.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "Firmware"
                            }]
                        },
                        {
                            "Component": "NMX-T",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/nmx-t.tgz",
                                "LocationType": "HTTPS",
                                "Type": "Binary"
                            }]
                        }
                    ]
                }
            },
            {
                "SKUID": "test-switch-sku",
                "Name": "Sample Switch",
                "Type": "Switch Tray",
                "Components": {
                    "Firmware": [{
                        "Component": "BMC+FPGA+EROT",
                        "Type": "Prod",
                        "Locations": [{
                            "Location": "https://example.test/switch-firmware.fwpkg",
                            "LocationType": "HTTPS",
                            "Type": "Firmware"
                        }]
                    }]
                }
            }
        ]));
        let mut parsed = parse_firmware_object_config(&config).unwrap();
        let nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "compute-tray-01".to_owned(),
                rack_id: "rack-01".to_owned(),
                r#type: Some(rm::NodeType::ComputeGb300Nvidia as i32),
                ..Default::default()
            }],
        };
        let component_filters = HashMap::from([(
            rm::NodeType::ComputeGb300Nvidia as i32,
            rm::FirmwareObjectComponentFilter {
                components: vec!["BMC".to_owned()],
            },
        )]);

        filter_parsed_components_for_apply(
            &mut parsed,
            Some(&nodes),
            "prod",
            &component_filters,
            &[],
        )
        .unwrap();

        assert_eq!(parsed.board_skus.len(), 1);
        assert_eq!(parsed.board_skus[0].firmware_components.len(), 1);
        assert_eq!(parsed.board_skus[0].firmware_components[0].component, "BMC");
        assert_eq!(
            parsed.board_skus[0].firmware_components[0].locations.len(),
            1
        );

        let artifacts = build_download_artifacts(&parsed);
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].url, "https://example.test/compute-bmc.fwpkg");

        let lookup = build_firmware_lookup_table(&parsed);
        let compute = lookup
            .devices
            .get("Compute Node")
            .expect("compute lookup should exist");
        assert_eq!(compute.len(), 1);
        assert_eq!(compute["BMC_prod_0001"].target, "");
        assert_eq!(compute["BMC_prod_0001"].filename, "prod/compute-bmc.fwpkg");
    }

    #[test]
    fn apply_filter_keeps_all_lenovo_hmc_payloads_without_component_filter() {
        let config = release_catalog(serde_json::json!([{
            "SKUID": "test-compute-sku",
            "Name": "test-compute-board",
            "Type": "Compute Tray",
            "Components": {
                "Firmware": [{
                    "Component": "HMC",
                    "Type": "Prod",
                    "Locations": [
                        {
                            "Location": "https://example.test/hmc.fwpkg",
                            "LocationType": "HTTPS",
                            "Type": "Firmware"
                        },
                        {
                            "Location": "https://example.test/partner-sbios.fwpkg",
                            "LocationType": "HTTPS",
                            "Type": "Firmware"
                        },
                        {
                            "Location": "https://example.test/hmc.corim",
                            "LocationType": "HTTPS",
                            "Type": "Certificate"
                        }
                    ]
                }]
            }
        }]));
        let mut parsed = parse_firmware_object_config(&config).unwrap();
        let nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "compute-tray-01".to_owned(),
                rack_id: "rack-01".to_owned(),
                r#type: Some(rm::NodeType::ComputeGb300Lenovo as i32),
                ..Default::default()
            }],
        };
        let component_filters = HashMap::new();

        filter_parsed_components_for_apply(
            &mut parsed,
            Some(&nodes),
            "prod",
            &component_filters,
            &[],
        )
        .unwrap();

        assert_eq!(parsed.board_skus.len(), 1);
        assert_eq!(parsed.board_skus[0].firmware_components.len(), 1);
        let hmc = &parsed.board_skus[0].firmware_components[0];
        assert_eq!(hmc.component, "HMC");
        assert_eq!(hmc.locations.len(), 2);

        let artifacts = build_download_artifacts(&parsed);
        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[0].url, "https://example.test/hmc.fwpkg");
        assert_eq!(artifacts[1].url, "https://example.test/partner-sbios.fwpkg");

        let lookup = build_firmware_lookup_table(&parsed);
        let compute = lookup
            .devices
            .get("Compute Node")
            .expect("compute lookup should exist");
        assert!(compute.contains_key("HMC_prod_0001"));
        assert!(compute.contains_key("HMC_prod_0002"));
    }

    #[test]
    fn apply_filter_keeps_supermicro_nosbios_bios_and_bmc_payloads() {
        let config = release_catalog(serde_json::json!([{
            "SKUID": "test-supermicro-compute-sku",
            "Name": "Test Supermicro compute",
            "Type": "Compute Tray",
            "Components": {
                "Firmware": [
                    {
                        "Component": "HMC",
                        "Type": "Prod",
                        "Locations": [
                            {
                                "Location": "https://example.test/test-bios-image.bin",
                                "LocationType": "HTTPS",
                                "Type": "Firmware"
                            },
                            {
                                "Location": "https://example.test/compute-nosbios.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "Firmware"
                            }
                        ]
                    },
                    {
                        "Component": "BMC",
                        "Type": "Prod",
                        "Locations": [{
                            "Location": "https://example.test/NvOBMC_test-image.bin",
                            "LocationType": "HTTPS",
                            "Type": "Firmware"
                        }]
                    }
                ]
            }
        }]));
        let mut parsed = parse_firmware_object_config(&config).unwrap();
        let node_type = NodeType::ComputeGb300Supermicro;
        let nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "compute-tray-01".to_owned(),
                rack_id: "rack-01".to_owned(),
                r#type: Some(rm::NodeType::Unspecified as i32),
                node_descriptor: Some(domain_node_type_to_descriptor(node_type)),
                ..Default::default()
            }],
        };

        filter_parsed_components_for_apply(&mut parsed, Some(&nodes), "prod", &HashMap::new(), &[])
            .unwrap();

        assert_eq!(parsed.board_skus[0].firmware_components.len(), 2);
        assert_eq!(
            parsed.board_skus[0].firmware_components[0].locations.len(),
            2
        );
        let artifacts = build_download_artifacts(&parsed);
        assert_eq!(artifacts.len(), 3);
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.url.contains("nosbios"))
        );
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.url.contains("bios-image"))
        );
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.url.contains("NvOBMC"))
        );
    }

    #[test]
    fn apply_filter_keeps_both_wiwynn_bmc_payloads_from_firmware_manifest() {
        let config = release_catalog(serde_json::json!([{
            "SKUID": "test-compute-sku",
            "Name": "compute",
            "Type": "Compute Tray",
            "Components": {
                "Firmware": [{
                    "Component": "BMC",
                    "Type": "Prod",
                    "Locations": [
                        {
                            "Location": "https://example.test/bmc-primary.fwpkg",
                            "LocationType": "HTTPS",
                            "Type": "Firmware"
                        },
                        {
                            "Location": "https://example.test/bmc-secondary.fwpkg",
                            "LocationType": "HTTPS",
                            "Type": "Firmware"
                        }
                    ]
                }]
            }
        }]));
        let mut parsed = parse_firmware_object_config(&config).unwrap();
        let nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "compute-tray-01".to_owned(),
                rack_id: "rack-01".to_owned(),
                r#type: Some(rm::NodeType::Unspecified as i32),
                node_descriptor: Some(domain_node_type_to_descriptor(NodeType::ComputeGb200Wiwynn)),
                ..Default::default()
            }],
        };

        filter_parsed_components_for_apply(&mut parsed, Some(&nodes), "prod", &HashMap::new(), &[])
            .unwrap();

        let bmc = &parsed.board_skus[0].firmware_components[0];
        assert_eq!(bmc.component, "BMC");
        assert_eq!(bmc.locations.len(), 2);

        let artifacts = build_download_artifacts(&parsed);
        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[0].url, "https://example.test/bmc-primary.fwpkg");
        assert_eq!(artifacts[1].url, "https://example.test/bmc-secondary.fwpkg");

        let lookup = build_firmware_lookup_table(&parsed);
        let compute = &lookup.devices[COMPUTE_LOOKUP_KEY];
        assert!(compute.contains_key("BMC_prod_0001"));
        assert!(compute.contains_key("BMC_prod_0002"));
    }

    #[test]
    fn metadata_exposes_multiple_bmc_artifacts_only_for_wiwynn() {
        let metadata =
            metadata_from_lookup_table(&duplicate_bmc_lookup_table_from_firmware_manifest());

        let nvidia = metadata
            .device_components
            .iter()
            .find(|entry| {
                entry.node_descriptor.as_ref()
                    == Some(&domain_node_type_to_descriptor(
                        NodeType::ComputeGb200Nvidia,
                    ))
            })
            .unwrap();
        let nvidia_bmc = nvidia
            .components
            .iter()
            .find(|component| component.name == "BMC")
            .unwrap();

        assert_eq!(nvidia_bmc.artifacts.len(), 1);
        assert_eq!(nvidia_bmc.artifacts[0].filename, "prod/bmc-primary.fwpkg");

        let wiwynn = metadata
            .device_components
            .iter()
            .find(|entry| {
                entry.node_descriptor.as_ref()
                    == Some(&domain_node_type_to_descriptor(
                        NodeType::ComputeGb200Wiwynn,
                    ))
            })
            .unwrap();
        let wiwynn_bmc = wiwynn
            .components
            .iter()
            .find(|component| component.name == "BMC")
            .unwrap();

        assert_eq!(wiwynn_bmc.artifacts.len(), 2);
        assert_eq!(wiwynn_bmc.artifacts[0].filename, "prod/bmc-primary.fwpkg");
        assert_eq!(wiwynn_bmc.artifacts[1].filename, "prod/bmc-secondary.fwpkg");
    }

    #[test]
    fn apply_filter_keeps_only_liteon_downloads_for_liteon_powershelf() {
        let config = release_catalog(serde_json::json!([{
            "SKUID": "test-powershelf-sku",
            "Name": "PowerShelf",
            "Type": "PowerShelf",
            "Components": {
                "Firmware": [
                    {
                        "Component": "Delta-PMC",
                        "Type": "Prod",
                        "Locations": [{
                            "Location": "https://example.test/delta-pmc.fwpkg",
                            "LocationType": "HTTPS",
                            "Type": "Binary"
                        }]
                    },
                    {
                        "Component": "Delta-PSU",
                        "Type": "Prod",
                        "Locations": [{
                            "Location": "https://example.test/delta-psu.tar",
                            "LocationType": "HTTPS",
                            "Type": "Binary"
                        }]
                    },
                    {
                        "Component": "LiteOn PSU",
                        "Type": "Prod",
                        "Locations": [{
                            "Location": "https://example.test/liteon-psu.tar",
                            "LocationType": "HTTPS",
                            "Type": "Binary"
                        }]
                    },
                    {
                        "Component": "LiteOn PMC",
                        "Type": "Prod",
                        "Locations": [{
                            "Location": "https://example.test/liteon-pmc.tar",
                            "LocationType": "HTTPS",
                            "Type": "Binary"
                        }]
                    }
                ]
            }
        }]));
        let mut parsed = parse_firmware_object_config(&config).unwrap();
        let nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                node_id: "ps-1".to_owned(),
                rack_id: "rack-1".to_owned(),
                r#type: Some(rm::NodeType::PowershelfGb300Liteon as i32),
                ..Default::default()
            }],
        };

        filter_parsed_components_for_apply(&mut parsed, Some(&nodes), "prod", &HashMap::new(), &[])
            .unwrap();

        assert_eq!(parsed.board_skus.len(), 1);
        let component_names: Vec<_> = parsed.board_skus[0]
            .firmware_components
            .iter()
            .map(|component| component.component.as_str())
            .collect();
        assert_eq!(component_names, vec!["LiteOn PSU", "LiteOn PMC"]);

        let artifacts = build_download_artifacts(&parsed);
        let urls: Vec<_> = artifacts
            .iter()
            .map(|artifact| artifact.url.as_str())
            .collect();
        assert_eq!(
            urls,
            vec![
                "https://example.test/liteon-psu.tar",
                "https://example.test/liteon-pmc.tar"
            ]
        );

        let lookup = build_firmware_lookup_table(&parsed);
        let powershelf = lookup
            .devices
            .get("Power Shelf")
            .expect("powershelf lookup should exist");
        assert!(powershelf.contains_key("LiteOnPSU_prod_0001"));
        assert!(powershelf.contains_key("LiteOnPMC_prod_0001"));
        assert!(!powershelf.contains_key("DeltaPMC_prod_0001"));
        assert!(!powershelf.contains_key("DeltaPSU_prod_0001"));
    }

    #[test]
    fn lookup_table_maps_gb300_firmware_manifest_device_shapes() {
        let config = release_catalog(serde_json::json!([
            {
                "SKUID": "test-compute-sku-a,test-compute-sku-b,test-compute-sku-c",
                "Name": "test-compute-board",
                "Type": "Compute Tray",
                "Components": {
                    "Firmware": [
                        {
                            "Component": "BMC",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/compute-bmc.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "Firmware"
                            }]
                        },
                        {
                            "Component": "HMC",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/compute-hmc.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "Firmware"
                            }]
                        }
                    ]
                }
            },
            {
                "SKUID": "test-switch-sku",
                "Name": "Sample Switch",
                "Type": "Switch Tray",
                "Components": {
                    "Firmware": [
                        {
                            "Component": "BMC+FPGA+EROT",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/switch-firmware.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "Firmware"
                            }]
                        },
                        {
                            "Component": "CPLD",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/switch-cpld.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "Firmware"
                            }]
                        }
                    ]
                }
            },
            {
                "SKUID": "test-powershelf-sku",
                "Name": "PowerShelf",
                "Type": "PowerShelf",
                "Components": {
                    "Firmware": [
                        {
                            "Component": "Delta-PMC",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/delta-pmc-firmware.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "Binary"
                            }]
                        },
                        {
                            "Component": "Delta-PSU",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/delta-psu-firmware.tar",
                                "LocationType": "HTTPS",
                                "Type": "Binary"
                            }]
                        },
                        {
                            "Component": "LiteOn PSU",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/liteon-psu-firmware.tar",
                                "LocationType": "HTTPS",
                                "Type": "Binary"
                            }]
                        },
                        {
                            "Component": "LiteOn PMC",
                            "Type": "Prod",
                            "Locations": [{
                                "Location": "https://example.test/liteon-pmc-firmware.tar",
                                "LocationType": "HTTPS",
                                "Type": "Binary"
                            }]
                        }
                    ]
                }
            }
        ]));
        let parsed = parse_firmware_object_config(&config).unwrap();

        assert_eq!(
            lookup_key_from_board_type(&parsed.board_skus[0].sku_type),
            Some(COMPUTE_LOOKUP_KEY)
        );
        assert_eq!(
            lookup_key_from_board_type(&parsed.board_skus[1].sku_type),
            Some(SWITCH_LOOKUP_KEY)
        );
        assert_eq!(
            lookup_key_from_board_type(&parsed.board_skus[2].sku_type),
            Some(POWER_SHELF_LOOKUP_KEY)
        );

        let lookup = build_firmware_lookup_table(&parsed);
        let compute = lookup
            .devices
            .get("Compute Node")
            .expect("GB300 compute entries should be present");
        assert_eq!(compute["BMC_prod_0001"].filename, "prod/compute-bmc.fwpkg");
        assert_eq!(compute["BMC_prod_0001"].target, "");
        assert_eq!(
            compute["HMC_prod_0001"].target,
            "/redfish/v1/Chassis/HGX_Chassis_0"
        );
        assert_eq!(
            compute["CPU_0_prod_0001"].target,
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0"
        );

        let switch = lookup
            .devices
            .get("Switch Tray")
            .expect("GB300 switch entries should be present");
        assert_eq!(switch["BMC_prod_0001"].target, "bmc");
        assert_eq!(switch["FPGA_prod_0001"].target, "fpga");
        assert_eq!(switch["EROT_prod_0001"].target, "erot");
        assert_eq!(switch["CPLD_prod_0001"].target, "cpld");

        let powershelf = lookup
            .devices
            .get("Power Shelf")
            .expect("GB300 powershelf entries should be present");
        assert_eq!(powershelf["DeltaPMC_prod_0001"].target, "");
        assert_eq!(
            powershelf["DeltaPMC_prod_0001"].filename,
            "prod/delta-pmc-firmware.fwpkg"
        );
        assert_eq!(powershelf["DeltaPSU_prod_0001"].target, "");
        assert_eq!(
            powershelf["DeltaPSU_prod_0001"].filename,
            "prod/delta-psu-firmware.tar"
        );
        assert_eq!(powershelf["LiteOnPSU_prod_0001"].target, "");
        assert_eq!(powershelf["LiteOnPMC_prod_0001"].target, "");
    }

    #[test]
    fn mapped_device_types_include_supported_variants() {
        let device_types = mapped_firmware_device_types();

        assert!(device_types.contains(&DeviceType::ComputeGb200Nvidia));
        assert!(device_types.contains(&DeviceType::ComputeGb300Nvidia));
        assert!(device_types.contains(&DeviceType::ComputeVrnvl72Nvidia));
        assert!(device_types.contains(&DeviceType::SwitchGb200Nvidia));
        assert!(device_types.contains(&DeviceType::SwitchGb300Nvidia));
        assert!(device_types.contains(&DeviceType::SwitchVrnvl72Nvidia));
        assert!(device_types.contains(&DeviceType::PowerShelf));
        assert_eq!(
            device_type_for_node_type(NodeType::SwitchVrnvl72Nvidia),
            Some(DeviceType::SwitchVrnvl72Nvidia)
        );
    }

    #[test]
    fn vrnvl72_manifest_without_component_type_or_skuid_uses_compute_tray_role() {
        let config = serde_json::json!({
            "ProductName": "VR-NVL72",
            "Milestones": [{
                "Name": "test-release",
                "BoardSKUs": [{
                    "Name": "test-compute-board",
                    "Type": "Compute Tray",
                    "Components": {
                        "Firmware": [{
                            "Component": "HMC",
                            "Locations": [{
                                "Location": "https://example.test/compute-hmc.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "Firmware",
                                "FileName": "compute-hmc.fwpkg"
                            }]
                        }, {
                            "Component": "BMC",
                            "Type": "",
                            "Locations": [{
                                "Location": "https://example.test/compute-bmc.fwpkg",
                                "LocationType": "HTTPS",
                                "Type": "",
                                "FileName": "compute-bmc.fwpkg"
                            }, {
                                "Location": "",
                                "LocationType": "",
                                "Type": "",
                                "FileName": "compute-bmc.corim"
                            }]
                        }]
                    }
                }]
            }]
        });
        let parsed = parse_firmware_object_config(&config).unwrap();

        validate_required_download_locations(&parsed).unwrap();

        let lookup = build_firmware_lookup_table(&parsed);
        let compute = lookup.devices.get(COMPUTE_LOOKUP_KEY).unwrap();
        assert_eq!(
            parsed.board_skus[0].firmware_components[0]
                .component_type
                .as_deref(),
            Some("Prod")
        );
        assert_eq!(
            parsed.board_skus[0].firmware_components[1]
                .component_type
                .as_deref(),
            Some("Prod")
        );
        assert_eq!(
            compute["HMC_prod_0001"].target,
            "/redfish/v1/Chassis/HGX_Chassis_0"
        );
        assert_eq!(compute["BMC_prod_0001"].target, "");

        let mappings =
            get_firmware_component_mappings_for_device_type(&DeviceType::ComputeVrnvl72Nvidia);
        assert_eq!(mappings.len(), 2);
        assert!(mappings.iter().any(|mapping| mapping.target_key == "HMC"));
        assert!(mappings.iter().any(|mapping| mapping.target_key == "BMC"));

        let metadata = metadata_from_lookup_table(&lookup);
        assert!(
            metadata
                .device_components
                .iter()
                .any(|entry| entry.node_type == rm::NodeType::ComputeVrnvl72Nvidia as i32)
        );
        assert!(
            metadata
                .device_components
                .iter()
                .all(|entry| entry.device_type == COMPUTE_LOOKUP_KEY)
        );

        let vr_targets = build_firmware_targets(
            &lookup,
            NodeType::ComputeVrnvl72Nvidia,
            COMPUTE_LOOKUP_KEY,
            "prod",
            "vr-fw",
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &FirmwareObjectComponentSelection::default_only(),
        )
        .unwrap();
        assert_eq!(vr_targets.len(), 2);
    }

    #[test]
    fn switch_tray_type_routes_vr_bundle_by_apply_node_type() {
        let config = serde_json::json!({
            "ProductName": "VR-NVL72",
            "Milestones": [{
                "Name": "0.6-build4",
                "BoardSKUs": [{
                    "Name": "reference-switch-board",
                    "Type": "Switch Tray",
                    "Components": {
                        "Firmware": [{
                            "Component": "BMC+CPLD+SMA+ERoT+SBIOS",
                            "Locations": [{
                                "Location": "/firmware/test-switch-bundle.fwpkg",
                                "LocationType": "FILE",
                                "Type": "Firmware",
                                "FileName": "test-switch-bundle.fwpkg"
                            }]
                        }]
                    }
                }]
            }]
        });
        let parsed = parse_firmware_object_config(&config).unwrap();
        validate_required_download_locations(&parsed).unwrap();
        assert_eq!(
            parsed.board_skus[0].firmware_components[0]
                .component_type
                .as_deref(),
            Some("Prod")
        );

        let vr_nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                r#type: Some(rm::NodeType::SwitchVrnvl72Nvidia as i32),
                ..Default::default()
            }],
        };
        let mut vr_downloads = parsed.clone();
        filter_parsed_components_for_apply(
            &mut vr_downloads,
            Some(&vr_nodes),
            "prod",
            &HashMap::new(),
            &[],
        )
        .unwrap();
        assert_eq!(vr_downloads.board_skus.len(), 1);
        assert_eq!(vr_downloads.board_skus[0].firmware_components.len(), 1);

        let gb_nodes = rm::NodeSet {
            nodes: vec![rm::NodeInfo {
                r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
                ..Default::default()
            }],
        };
        let mut gb_downloads = parsed.clone();
        filter_parsed_components_for_apply(
            &mut gb_downloads,
            Some(&gb_nodes),
            "prod",
            &HashMap::new(),
            &[],
        )
        .unwrap();
        assert!(gb_downloads.board_skus.is_empty());

        let lookup = build_firmware_lookup_table(&parsed);
        let switch = lookup.devices.get(SWITCH_LOOKUP_KEY).unwrap();
        assert_eq!(switch.len(), 5);
        assert_eq!(switch["BMC_prod_0001"].target, "bmc");
        assert_eq!(switch["BIOS_prod_0001"].target, "bios");
        assert_eq!(switch["SMA_prod_0001"].target, "sma");
        assert_eq!(switch["EROT_prod_0001"].target, "erot");
        assert_eq!(switch["CPLD_prod_0001"].target, "cpld1");

        let package_targets = build_firmware_targets(
            &lookup,
            NodeType::SwitchVrnvl72Nvidia,
            SWITCH_LOOKUP_KEY,
            "prod",
            "vr-fw",
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &FirmwareObjectComponentSelection::default_only(),
        )
        .unwrap();
        assert_eq!(package_targets.len(), 1);
        assert_eq!(package_targets[0].target, "");
        assert!(
            package_targets[0]
                .filename
                .ends_with("/prod/test-switch-bundle.fwpkg")
        );

        let bmc_targets = build_firmware_targets(
            &lookup,
            NodeType::SwitchVrnvl72Nvidia,
            SWITCH_LOOKUP_KEY,
            "prod",
            "vr-fw",
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &FirmwareObjectComponentSelection::components(vec!["BMC".to_owned()]),
        )
        .unwrap();
        assert_eq!(bmc_targets.len(), 1);
        assert_eq!(bmc_targets[0].target, "bmc");

        assert!(
            build_firmware_targets(
                &lookup,
                NodeType::SwitchGb200Nvidia,
                SWITCH_LOOKUP_KEY,
                "prod",
                "vr-fw",
                FIRMWARE_OBJECT_CACHE_SUBDIR,
                &FirmwareObjectComponentSelection::default_only(),
            )
            .is_err()
        );
    }

    #[test]
    fn apply_request_builds_gb300_powershelf_targets_from_component_filter() {
        let mut power_shelf_entries = HashMap::new();
        power_shelf_entries.insert(
            "DeltaPMC_prod".to_owned(),
            FirmwareLookupEntry {
                filename: "prod/delta-pmc-firmware.fwpkg".to_owned(),
                target: String::new(),
                component: "Delta-PMC".to_owned(),
                bundle: String::new(),
                firmware_type: "prod".to_owned(),
                version: Some("3.2.4".to_owned()),
                subcomponents: Vec::new(),
            },
        );
        power_shelf_entries.insert(
            "DeltaPSU_prod".to_owned(),
            FirmwareLookupEntry {
                filename: "prod/delta-psu-firmware.tar".to_owned(),
                target: String::new(),
                component: "Delta-PSU".to_owned(),
                bundle: String::new(),
                firmware_type: "prod".to_owned(),
                version: Some("0104".to_owned()),
                subcomponents: Vec::new(),
            },
        );
        let lookup = FirmwareLookupTable {
            devices: HashMap::from([("Power Shelf".to_owned(), power_shelf_entries)]),
            switch_system_images: HashMap::new(),
        };
        let firmware = firmware_object_with_lookup(lookup);
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "ps-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::PowershelfGb300Delta as i32),
                    ..Default::default()
                }],
            }),
            component_filters: HashMap::from([(
                rm::NodeType::PowershelfGb300Delta as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["Delta-PSU".to_owned(), "Delta-PMC".to_owned()],
                },
            )]),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::PowershelfGb300Delta as i32))
            .expect("GB300 Delta powershelf targets should be present");

        assert_eq!(targets.targets.len(), 2);
        assert_eq!(targets.targets[0].target, "");
        assert_eq!(
            targets.targets[0].filename,
            "firmware_objects/fw-1/prod/delta-pmc-firmware.fwpkg"
        );
        assert_eq!(targets.targets[1].target, "");
        assert_eq!(
            targets.targets[1].filename,
            "firmware_objects/fw-1/prod/delta-psu-firmware.tar"
        );
    }

    #[test]
    fn apply_request_uses_only_liteon_entries_for_liteon_powershelf() {
        let firmware = firmware_object_with_lookup(mixed_powershelf_lookup_table());
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "ps-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::PowershelfGb300Liteon as i32),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::PowershelfGb300Liteon as i32))
            .expect("GB300 LiteOn powershelf targets should be present");

        assert_eq!(targets.targets.len(), 2);
        assert_eq!(targets.targets[0].target, "");
        assert_eq!(
            targets.targets[0].filename,
            "firmware_objects/fw-1/prod/liteon-pmc.tar"
        );
        assert_eq!(targets.targets[1].target, "");
        assert_eq!(
            targets.targets[1].filename,
            "firmware_objects/fw-1/prod/liteon-psu.tar"
        );
    }

    #[test]
    fn apply_request_uses_only_delta_entries_for_delta_powershelf() {
        let firmware = firmware_object_with_lookup(mixed_powershelf_lookup_table());
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "ps-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::PowershelfGb300Delta as i32),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::PowershelfGb300Delta as i32))
            .expect("GB300 Delta powershelf targets should be present");

        assert_eq!(targets.targets.len(), 2);
        assert_eq!(targets.targets[0].target, "");
        assert_eq!(
            targets.targets[0].filename,
            "firmware_objects/fw-1/prod/delta-pmc.fwpkg"
        );
        assert_eq!(targets.targets[1].target, "");
        assert_eq!(
            targets.targets[1].filename,
            "firmware_objects/fw-1/prod/delta-psu.tar"
        );
    }

    #[test]
    fn apply_request_rejects_incompatible_powershelf_component_filter() {
        let firmware = firmware_object_with_lookup(mixed_powershelf_lookup_table());
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "ps-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::PowershelfGb300Liteon as i32),
                    ..Default::default()
                }],
            }),
            component_filters: HashMap::from([(
                rm::NodeType::PowershelfGb300Liteon as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["Delta-PMC".to_owned()],
                },
            )]),
            ..Default::default()
        };

        let err = test_service()
            .build_update_firmware_request(&request, &firmware)
            .expect_err("LiteOn powershelf should not accept Delta component filters");
        assert!(err.contains("no matching firmware found in config for Power Shelf"));
    }

    #[test]
    fn apply_request_builds_compute_bmc_with_empty_target() {
        let mut compute_entries = HashMap::new();
        compute_entries.insert(
            "BMC_prod".to_owned(),
            FirmwareLookupEntry {
                filename: "prod/compute-bmc.fwpkg".to_owned(),
                target: String::new(),
                component: "BMC".to_owned(),
                bundle: "compute-bmc-bundle".to_owned(),
                firmware_type: "prod".to_owned(),
                version: Some("1.2.3".to_owned()),
                subcomponents: Vec::new(),
            },
        );
        let lookup = FirmwareLookupTable {
            devices: HashMap::from([("Compute Node".to_owned(), compute_entries)]),
            switch_system_images: HashMap::new(),
        };
        let firmware = firmware_object_with_lookup(lookup);
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "c-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                    ..Default::default()
                }],
            }),
            component_filters: HashMap::from([(
                rm::NodeType::ComputeGb200Nvidia as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["BMC".to_owned()],
                },
            )]),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::ComputeGb200Nvidia as i32))
            .expect("compute targets should be present");

        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].target, "");
        assert_eq!(
            targets.targets[0].filename,
            "firmware_objects/fw-1/prod/compute-bmc.fwpkg"
        );
    }

    #[test]
    fn component_filter_builds_fine_grained_compute_target() {
        let firmware = firmware_object_with_lookup(hmc_lookup_table_from_firmware_manifest());
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "c-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                    ..Default::default()
                }],
            }),
            component_filters: HashMap::from([(
                rm::NodeType::ComputeGb200Nvidia as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["CPU_0".to_owned()],
                },
            )]),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::ComputeGb200Nvidia as i32))
            .expect("compute targets should be present");

        assert_eq!(targets.targets.len(), 1);
        assert_eq!(
            targets.targets[0].target,
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0"
        );
        assert_eq!(
            targets.targets[0].filename,
            "firmware_objects/fw-1/prod/hmc.fwpkg"
        );
    }

    #[test]
    fn unfiltered_compute_apply_keeps_default_targets_only() {
        let firmware = firmware_object_with_lookup(hmc_lookup_table_from_firmware_manifest());
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "c-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::ComputeGb200Nvidia as i32))
            .expect("compute targets should be present");

        assert_eq!(targets.targets.len(), 1);
        assert_eq!(
            targets.targets[0].target,
            "/redfish/v1/Chassis/HGX_Chassis_0"
        );
    }

    #[test]
    fn explicit_empty_component_filter_selects_all_compute_targets() {
        let firmware = firmware_object_with_lookup(hmc_lookup_table_from_firmware_manifest());
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "c-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
                    ..Default::default()
                }],
            }),
            component_filters: HashMap::from([(
                rm::NodeType::ComputeGb200Nvidia as i32,
                rm::FirmwareObjectComponentFilter {
                    components: Vec::new(),
                },
            )]),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::ComputeGb200Nvidia as i32))
            .expect("compute targets should be present");

        assert!(targets.targets.len() > 1);
        assert!(
            targets
                .targets
                .iter()
                .any(|target| target.target == "/redfish/v1/Chassis/HGX_Chassis_0")
        );
        assert!(
            targets.targets.iter().any(|target| target.target
                == "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0")
        );
    }

    #[test]
    fn apply_request_builds_lenovo_hmc_targets_for_all_packages() {
        let firmware = firmware_object_with_lookup(duplicate_hmc_lookup_table());
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "c-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::ComputeGb300Lenovo as i32),
                    ..Default::default()
                }],
            }),
            component_filters: HashMap::from([(
                rm::NodeType::ComputeGb300Lenovo as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["HMC".to_owned()],
                },
            )]),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::ComputeGb300Lenovo as i32))
            .expect("Lenovo compute targets should be present");

        assert_eq!(targets.targets.len(), 2);
        assert_eq!(
            targets.targets[0].target,
            "/redfish/v1/Chassis/HGX_Chassis_0"
        );
        assert_eq!(
            targets.targets[0].filename,
            "firmware_objects/fw-1/prod/hmc.fwpkg"
        );
        assert_eq!(
            targets.targets[1].target,
            "/redfish/v1/Chassis/HGX_Chassis_0"
        );
        assert_eq!(
            targets.targets[1].filename,
            "firmware_objects/fw-1/prod/partner-sbios.fwpkg"
        );
    }

    #[test]
    fn apply_request_builds_supermicro_gb300_targets_in_required_order() {
        let firmware =
            firmware_object_with_lookup(supermicro_gb300_lookup_table_from_firmware_manifest());
        let node_type = NodeType::ComputeGb300Supermicro;
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "c-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::Unspecified as i32),
                    node_descriptor: Some(domain_node_type_to_descriptor(node_type)),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = &update.node_descriptor_firmware_targets[0]
            .firmware_targets
            .as_ref()
            .unwrap()
            .targets;

        assert_eq!(targets.len(), 3);
        assert_eq!(targets[0].target, SUPERMICRO_HGX_TARGET);
        assert!(targets[0].filename.contains("nosbios"));
        assert_eq!(targets[1].target, SUPERMICRO_BIOS_TARGET);
        assert!(targets[1].filename.contains("bios-image"));
        assert_eq!(targets[2].target, "");
        assert!(targets[2].filename.contains("NvOBMC"));
    }

    #[test]
    fn supermicro_bmc_apply_target_carries_version_from_firmware_manifest_lookup() {
        let firmware =
            firmware_object_with_lookup(supermicro_gb300_lookup_table_from_firmware_manifest());
        let node_type = NodeType::ComputeGb300Supermicro;
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "c-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::Unspecified as i32),
                    node_descriptor: Some(domain_node_type_to_descriptor(node_type)),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();

        let versions =
            firmware_target_expected_versions_from_firmware_manifest(&firmware, &update).unwrap();
        let supermicro_versions = versions
            .get(&node_type)
            .expect("Supermicro expected versions should be present");
        let bmc_target = update.node_descriptor_firmware_targets[0]
            .firmware_targets
            .as_ref()
            .unwrap()
            .targets
            .iter()
            .find(|target| target.target.is_empty())
            .expect("BMC target should be present");

        assert_eq!(
            supermicro_versions.get(&bmc_target.filename),
            Some(&"70.02.01.05".to_owned())
        );
        assert_eq!(supermicro_versions.len(), 1);
    }

    #[test]
    fn apply_request_builds_wiwynn_bmc_targets_for_both_firmware_manifest_locations() {
        let firmware =
            firmware_object_with_lookup(duplicate_bmc_lookup_table_from_firmware_manifest());
        let request_for = |node_type: NodeType| {
            let proto_type = domain_node_type_to_proto(node_type);
            let filter = rm::FirmwareObjectComponentFilter {
                components: vec!["BMC".to_owned()],
            };
            let descriptor_only = proto_type == rm::NodeType::Unspecified;
            let component_filters = if descriptor_only {
                HashMap::new()
            } else {
                HashMap::from([(proto_type as i32, filter.clone())])
            };
            let descriptor_component_filters = if descriptor_only {
                vec![rm::NodeDescriptorFirmwareObjectComponentFilter {
                    node_descriptor: Some(domain_node_type_to_descriptor(node_type)),
                    component_filter: Some(filter),
                }]
            } else {
                Vec::new()
            };

            rm::ApplyStoredFirmwareObjectRequest {
                firmware_type: "prod".to_owned(),
                nodes: Some(rm::NodeSet {
                    nodes: vec![rm::NodeInfo {
                        node_id: "c-1".to_owned(),
                        rack_id: "rack-1".to_owned(),
                        r#type: Some(proto_type as i32),
                        node_descriptor: descriptor_only
                            .then(|| domain_node_type_to_descriptor(node_type)),
                        ..Default::default()
                    }],
                }),
                component_filters,
                node_descriptor_component_filters: descriptor_component_filters,
                ..Default::default()
            }
        };

        let wiwynn_update = test_service()
            .build_update_firmware_request(&request_for(NodeType::ComputeGb200Wiwynn), &firmware)
            .unwrap();
        let wiwynn_targets = &wiwynn_update.node_descriptor_firmware_targets[0]
            .firmware_targets
            .as_ref()
            .unwrap()
            .targets;

        assert_eq!(wiwynn_targets.len(), 2);
        assert_eq!(wiwynn_targets[0].target, "");
        assert_eq!(
            wiwynn_targets[0].filename,
            "firmware_objects/fw-1/prod/bmc-primary.fwpkg"
        );
        assert_eq!(wiwynn_targets[1].target, "");
        assert_eq!(
            wiwynn_targets[1].filename,
            "firmware_objects/fw-1/prod/bmc-secondary.fwpkg"
        );

        let nvidia_update = test_service()
            .build_update_firmware_request(&request_for(NodeType::ComputeGb200Nvidia), &firmware)
            .unwrap();
        let nvidia_targets =
            &nvidia_update.firmware_targets[&(rm::NodeType::ComputeGb200Nvidia as i32)].targets;

        assert_eq!(nvidia_targets.len(), 1);
        assert_eq!(
            nvidia_targets[0].filename,
            "firmware_objects/fw-1/prod/bmc-primary.fwpkg"
        );
    }

    #[test]
    fn apply_request_keeps_single_hmc_target_for_non_lenovo_gb300() {
        let firmware = firmware_object_with_lookup(duplicate_hmc_lookup_table());
        let request = rm::ApplyStoredFirmwareObjectRequest {
            firmware_type: "prod".to_owned(),
            nodes: Some(rm::NodeSet {
                nodes: vec![rm::NodeInfo {
                    node_id: "c-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    r#type: Some(rm::NodeType::ComputeGb300Nvidia as i32),
                    ..Default::default()
                }],
            }),
            component_filters: HashMap::from([(
                rm::NodeType::ComputeGb300Nvidia as i32,
                rm::FirmwareObjectComponentFilter {
                    components: vec!["HMC".to_owned()],
                },
            )]),
            ..Default::default()
        };

        let update = test_service()
            .build_update_firmware_request(&request, &firmware)
            .unwrap();
        let targets = update
            .firmware_targets
            .get(&(rm::NodeType::ComputeGb300Nvidia as i32))
            .expect("NVIDIA compute targets should be present");

        assert_eq!(targets.targets.len(), 1);
        assert_eq!(
            targets.targets[0].filename,
            "firmware_objects/fw-1/prod/hmc.fwpkg"
        );
    }

    #[test]
    fn multi_package_policy_is_node_type_and_target_key_scoped() {
        assert!(firmware_target_allows_multiple_packages(
            NodeType::ComputeGb300Lenovo,
            "HMC"
        ));
        assert!(!firmware_target_allows_multiple_packages(
            NodeType::ComputeGb300Lenovo,
            "BMC"
        ));
        assert!(!firmware_target_allows_multiple_packages(
            NodeType::ComputeGb300Nvidia,
            "HMC"
        ));
        assert!(firmware_target_allows_multiple_packages(
            NodeType::ComputeGb200Wiwynn,
            "BMC"
        ));
        assert!(!firmware_target_allows_multiple_packages(
            NodeType::ComputeGb200Wiwynn,
            "HMC"
        ));

        let components = vec![
            (
                "HMC_prod_0001".to_owned(),
                "prod/hmc.fwpkg".to_owned(),
                "/redfish/v1/Chassis/HGX_Chassis_0".to_owned(),
            ),
            (
                "HMC_prod_0002".to_owned(),
                "prod/partner-sbios.fwpkg".to_owned(),
                "/redfish/v1/Chassis/HGX_Chassis_0".to_owned(),
            ),
            (
                "BMC_prod_0001".to_owned(),
                "prod/bmc.fwpkg".to_owned(),
                String::new(),
            ),
            (
                "BMC_prod_0002".to_owned(),
                "prod/bmc-secondary.fwpkg".to_owned(),
                String::new(),
            ),
        ];

        let lenovo = apply_firmware_target_multiplicity_policy(
            NodeType::ComputeGb300Lenovo,
            components.clone(),
        );
        assert_eq!(lenovo.len(), 3);
        assert_eq!(lenovo[0].0, "HMC_prod_0001");
        assert_eq!(lenovo[1].0, "HMC_prod_0002");
        assert_eq!(lenovo[2].0, "BMC_prod_0001");

        let wiwynn = apply_firmware_target_multiplicity_policy(
            NodeType::ComputeGb200Wiwynn,
            components.clone(),
        );
        assert_eq!(wiwynn.len(), 3);
        assert_eq!(wiwynn[0].0, "HMC_prod_0001");
        assert_eq!(wiwynn[1].0, "BMC_prod_0001");
        assert_eq!(wiwynn[2].0, "BMC_prod_0002");

        let nvidia =
            apply_firmware_target_multiplicity_policy(NodeType::ComputeGb300Nvidia, components);
        assert_eq!(nvidia.len(), 2);
        assert_eq!(nvidia[0].0, "HMC_prod_0001");
        assert_eq!(nvidia[1].0, "BMC_prod_0001");
    }

    fn compute_board_sku_with_locations(
        component: &str,
        location: &str,
        location_type: &str,
    ) -> BoardSkuFirmware {
        BoardSkuFirmware {
            sku_id: "test-compute-sku".to_owned(),
            name: "test-compute-board".to_owned(),
            sku_type: "Compute Node".to_owned(),
            firmware_components: vec![FirmwareComponent {
                component: component.to_owned(),
                bundle: Some(format!("{component}-bundle")),
                version: Some("1.2.3".to_owned()),
                component_type: Some("Prod".to_owned()),
                context: String::new(),
                locations: vec![FirmwareLocation {
                    location: location.to_owned(),
                    location_type: location_type.to_owned(),
                    firmware_type: Some("Firmware".to_owned()),
                    sha256: None,
                    context: String::new(),
                }],
                subcomponents: Vec::new(),
            }],
        }
    }

    #[tokio::test]
    async fn download_firmware_artifacts_downloads_http_and_file_in_parallel() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const HTTP_BYTES: &[u8] = b"http firmware payload";

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/compute-bmc.fwpkg"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(HTTP_BYTES))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let local_source = temp.path().join("local-hmc.fwpkg");
        tokio::fs::write(&local_source, b"local firmware payload")
            .await
            .unwrap();

        let parsed = ParsedFirmwareComponents {
            board_skus: vec![
                compute_board_sku_with_locations(
                    "BMC",
                    &format!("{}/compute-bmc.fwpkg", server.uri()),
                    "http",
                ),
                compute_board_sku_with_locations(
                    "HMC",
                    local_source.to_string_lossy().as_ref(),
                    "file",
                ),
            ],
            switch_system_images: Vec::new(),
        };

        let firmware_dir = temp.path().join("firmware");
        let cancel = CancellationToken::new();
        let lookup = download_firmware_artifacts(
            "parallel-fw",
            &parsed,
            None,
            &firmware_dir,
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &cancel,
        )
        .await
        .expect("parallel downloads should succeed");

        let bmc_path = firmware_dir
            .join(FIRMWARE_OBJECT_CACHE_SUBDIR)
            .join("parallel-fw")
            .join("prod")
            .join("compute-bmc.fwpkg");
        let hmc_path = firmware_dir
            .join(FIRMWARE_OBJECT_CACHE_SUBDIR)
            .join("parallel-fw")
            .join("prod")
            .join("local-hmc.fwpkg");
        assert_eq!(tokio::fs::read(&bmc_path).await.unwrap(), HTTP_BYTES);
        assert_eq!(
            tokio::fs::read(&hmc_path).await.unwrap(),
            b"local firmware payload"
        );
        assert!(
            lookup
                .devices
                .get("Compute Node")
                .and_then(|entries| entries.get("BMC_prod_0001"))
                .is_some()
        );
        assert!(
            lookup
                .devices
                .get("Compute Node")
                .and_then(|entries| entries.get("HMC_prod_0001"))
                .is_some()
        );
    }

    #[tokio::test]
    async fn download_firmware_artifacts_ignores_optional_switch_image_failures() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/required.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"required image"))
            .mount(&server)
            .await;

        let parsed = ParsedFirmwareComponents {
            board_skus: Vec::new(),
            switch_system_images: vec![
                SwitchSystemImageArtifact {
                    device_type: "Switch Tray".to_owned(),
                    component: "NVOS".to_owned(),
                    version: "25.02.4440".to_owned(),
                    firmware_type: "prod".to_owned(),
                    package_name: "GB200NVL72_NVOS".to_owned(),
                    location: format!("{}/missing.bin", server.uri()),
                    location_type: "http".to_owned(),
                    required: false,
                    image_filename: "missing.bin".to_owned(),
                    sha256: None,
                },
                SwitchSystemImageArtifact {
                    device_type: "Switch Tray".to_owned(),
                    component: "NVOS".to_owned(),
                    version: "25.02.4440".to_owned(),
                    firmware_type: "prod".to_owned(),
                    package_name: "GB200NVL72_NVOS".to_owned(),
                    location: format!("{}/required.bin", server.uri()),
                    location_type: "http".to_owned(),
                    required: true,
                    image_filename: "required.bin".to_owned(),
                    sha256: None,
                },
            ],
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let cancel = CancellationToken::new();
        download_firmware_artifacts(
            "optional-switch",
            &parsed,
            None,
            temp.path(),
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &cancel,
        )
        .await
        .expect("optional failure should not abort required downloads");
    }

    #[tokio::test]
    async fn download_firmware_artifacts_fails_when_required_download_fails() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/missing.fwpkg"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let parsed = ParsedFirmwareComponents {
            board_skus: vec![compute_board_sku_with_locations(
                "BMC",
                &format!("{}/missing.fwpkg", server.uri()),
                "http",
            )],
            switch_system_images: Vec::new(),
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let cancel = CancellationToken::new();
        let err = download_firmware_artifacts(
            "required-failure",
            &parsed,
            None,
            temp.path(),
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &cancel,
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("required firmware artifact downloads failed"),
            "unexpected error: {err}"
        );
        assert!(
            err.contains("BMC") && err.contains("missing.fwpkg"),
            "error should name the failing component and url, not just a count: {err}"
        );
    }

    #[tokio::test]
    async fn download_firmware_artifacts_summarizes_multiple_required_failures() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        const COMPONENTS: [&str; 7] = ["BMC", "HMC", "PSU", "FPGA", "CPLD", "RETIMER", "NIC"];
        let board_skus = COMPONENTS
            .iter()
            .map(|component| {
                compute_board_sku_with_locations(
                    component,
                    &format!("{}/{component}-missing.fwpkg", server.uri()),
                    "http",
                )
            })
            .collect();

        let parsed = ParsedFirmwareComponents {
            board_skus,
            switch_system_images: Vec::new(),
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let cancel = CancellationToken::new();
        let err = download_firmware_artifacts(
            "many-failures",
            &parsed,
            None,
            temp.path(),
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &cancel,
        )
        .await
        .unwrap_err();

        assert!(
            err.starts_with(&format!(
                "{} required firmware artifact downloads failed:",
                COMPONENTS.len()
            )),
            "unexpected error: {err}"
        );
        assert!(
            err.contains(&format!(
                "...and {} more",
                COMPONENTS.len() - MAX_LISTED_DOWNLOAD_FAILURES
            )),
            "expected truncation marker for failures beyond the cap: {err}"
        );
    }

    #[tokio::test]
    async fn download_firmware_artifacts_cancels_in_flight_download() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/slow.fwpkg"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"slow firmware")
                    .set_delay(Duration::from_secs(2)),
            )
            .mount(&server)
            .await;

        let parsed = ParsedFirmwareComponents {
            board_skus: vec![compute_board_sku_with_locations(
                "BMC",
                &format!("{}/slow.fwpkg", server.uri()),
                "http",
            )],
            switch_system_images: Vec::new(),
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let firmware_dir = temp.path().to_path_buf();
        let download_task = tokio::spawn(async move {
            download_firmware_artifacts(
                "cancel-fw",
                &parsed,
                None,
                &firmware_dir,
                FIRMWARE_OBJECT_CACHE_SUBDIR,
                &cancel_for_task,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();

        let err = download_task.await.unwrap().unwrap_err();
        assert!(
            err.contains("deleted") || err.contains("required firmware artifact downloads failed"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn download_firmware_artifacts_threads_access_token_to_artifactory() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/compute-bmc.fwpkg"))
            .and(header("X-JFrog-Art-Api", "secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"artifactory payload"))
            .mount(&server)
            .await;

        let parsed = ParsedFirmwareComponents {
            board_skus: vec![compute_board_sku_with_locations(
                "BMC",
                &format!("{}/compute-bmc.fwpkg", server.uri()),
                "artifactory",
            )],
            switch_system_images: Vec::new(),
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let cancel = CancellationToken::new();
        download_firmware_artifacts(
            "artifactory-fw",
            &parsed,
            Some("secret-token".to_owned()),
            temp.path(),
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &cancel,
        )
        .await
        .expect("artifactory download through handler should succeed");
    }

    #[tokio::test]
    async fn download_firmware_artifacts_downloads_switch_system_image_to_cache() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvos-amd64-25.02.4440.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"switch image"))
            .mount(&server)
            .await;

        let parsed = ParsedFirmwareComponents {
            board_skus: Vec::new(),
            switch_system_images: vec![SwitchSystemImageArtifact {
                device_type: "Switch Tray".to_owned(),
                component: "NVOS".to_owned(),
                version: "25.02.4440".to_owned(),
                firmware_type: "prod".to_owned(),
                package_name: "GB200NVL72_NVOS".to_owned(),
                location: format!("{}/nvos-amd64-25.02.4440.bin", server.uri()),
                location_type: "http".to_owned(),
                required: true,
                image_filename: "nvos-amd64-25.02.4440.bin".to_owned(),
                sha256: None,
            }],
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let cancel = CancellationToken::new();
        let lookup = download_firmware_artifacts(
            "switch-image",
            &parsed,
            None,
            temp.path(),
            FIRMWARE_OBJECT_CACHE_SUBDIR,
            &cancel,
        )
        .await
        .expect("switch image download should succeed");

        let cached = temp
            .path()
            .join(FIRMWARE_OBJECT_CACHE_SUBDIR)
            .join("switch-image")
            .join("prod")
            .join("nvos-amd64-25.02.4440.bin");
        assert_eq!(tokio::fs::read(&cached).await.unwrap(), b"switch image");
        assert!(
            lookup
                .switch_system_images
                .get("Switch Tray")
                .and_then(|entries| entries.get("NVOS_prod"))
                .is_some()
        );
    }

    #[test]
    fn parse_firmware_component_reads_and_normalizes_sha256() {
        let firmware = serde_json::json!({
            "Component": "BMC",
            "Locations": [{
                "Location": "https://example.test/bmc.fwpkg",
                "LocationType": "HTTPS",
                "Type": "Firmware",
                "Sha256": "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"
            }]
        });
        let parsed = parse_firmware_component(&firmware, "ctx", false);
        assert_eq!(parsed.locations.len(), 1);
        assert_eq!(
            parsed.locations[0].sha256.as_deref(),
            Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
        );
    }

    #[test]
    fn parse_firmware_component_leaves_sha256_absent_when_not_provided() {
        let firmware = serde_json::json!({
            "Component": "BMC",
            "Locations": [{
                "Location": "https://example.test/bmc.fwpkg",
                "LocationType": "HTTPS",
                "Type": "Firmware"
            }]
        });
        let parsed = parse_firmware_component(&firmware, "ctx", false);
        assert_eq!(parsed.locations.len(), 1);
        assert_eq!(parsed.locations[0].sha256, None);
    }

    #[test]
    fn validate_firmware_object_sha256_rejects_malformed_component_digest() {
        let parsed = ParsedFirmwareComponents {
            board_skus: vec![BoardSkuFirmware {
                sku_id: "sku".to_owned(),
                name: "name".to_owned(),
                sku_type: "Compute Node".to_owned(),
                firmware_components: vec![FirmwareComponent {
                    component: "BMC".to_owned(),
                    bundle: None,
                    version: None,
                    component_type: Some("prod".to_owned()),
                    context: String::new(),
                    locations: vec![FirmwareLocation {
                        location: "https://example.test/bmc.fwpkg".to_owned(),
                        location_type: "HTTPS".to_owned(),
                        firmware_type: Some("Firmware".to_owned()),
                        sha256: Some("deadbeef".to_owned()),
                        context: String::new(),
                    }],
                    subcomponents: Vec::new(),
                }],
            }],
            switch_system_images: Vec::new(),
        };
        let err = validate_firmware_object_sha256(&parsed).unwrap_err();
        assert!(err.contains("invalid Sha256"), "unexpected error: {err}");
        assert!(
            err.contains("BMC"),
            "error should identify the component: {err}"
        );
    }

    #[test]
    fn validate_firmware_object_sha256_accepts_valid_and_absent_digests() {
        let parsed = ParsedFirmwareComponents {
            board_skus: Vec::new(),
            switch_system_images: vec![SwitchSystemImageArtifact {
                device_type: "Switch Tray".to_owned(),
                component: "NVOS".to_owned(),
                version: "25.02.4440".to_owned(),
                firmware_type: "prod".to_owned(),
                package_name: "GB200NVL72_NVOS".to_owned(),
                location: "https://example.test/nvos.bin".to_owned(),
                location_type: "http".to_owned(),
                required: true,
                image_filename: "nvos.bin".to_owned(),
                sha256: Some("a".repeat(64)),
            }],
        };
        assert!(validate_firmware_object_sha256(&parsed).is_ok());
    }
}
