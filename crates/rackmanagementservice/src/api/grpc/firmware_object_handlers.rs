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

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use super::server::RackManagerServiceImpl;
use crate::api::grpc::conversions::{
    batch_targets, domain_node_type_to_proto, failed_node_results, flatten_node_info,
    proto_node_type_to_domain, proto_node_type_to_string, timestamp_from_datetime,
};
use crate::api::grpc::firmware_artifact_paths::filename_from_location;
use crate::api::grpc::firmware_handlers::{
    RecoverySource, build_ephemeral_inventory_node, build_ephemeral_node_with_profiles,
    validate_firmware_object_request,
};
use crate::api::grpc::node_type_resolver::{
    domain_node_type_to_descriptor, resolve_component_filter_selectors, resolve_node_info,
};
use crate::api::grpc::switch_image_handlers::{
    SwitchImageRecoveryOptions, create_switch_system_image_node, run_switch_system_image_job,
};
use crate::domain::firmware_manifest::{self as manifest, FirmwareManifest};
use crate::domain::node::{
    ExpectedInventoryPolicy, FirmwareInfo, Node, NodeKind, NodeType, RecoveryBundles,
    RecoveryProfile,
};
use crate::domain::rack::NodeConfig;
use crate::nodes::NodeInstance;
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

mod artifacts;
mod metadata;
mod plan;
mod profile;
mod workflow;

use artifacts::{ArtifactCatalog, Preparation};
use workflow::{FirmwareApply, FirmwareSource};

const FIRMWARE_OBJECT_CACHE_SUBDIR: &str = "firmware_objects";
const EPHEMERAL_FIRMWARE_OBJECT_CACHE_SUBDIR: &str = "firmware_objects_ephemeral";
const COMPUTE_LOOKUP_KEY: &str = "Compute Node";
const SWITCH_LOOKUP_KEY: &str = "Switch Tray";
const POWER_SHELF_LOOKUP_KEY: &str = "Power Shelf";
const FIRMWARE_MANIFEST_COMPARE_CONCURRENCY: usize = 8;
/// The compute OOB recovery flow pushes HMC packages: the regular bundle and
/// the recovery package both come from the manifest's HMC component.
const RECOVERY_BUNDLE_COMPONENT: &str = "HMC";

#[derive(Debug, Clone, PartialEq, Eq)]
struct FirmwareManifestExpectation {
    component: String,
    version: String,
    target: String,
    sku_id: String,
}

type FirmwareComparisonOutcome = (
    usize,
    String,
    std::result::Result<rm::NodeFirmwareManifestComparison, String>,
);

struct PreparedFirmwareComparison {
    index: usize,
    node_id: String,
    node: Arc<NodeInstance>,
    expectations: Vec<FirmwareManifestExpectation>,
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

fn prepare_firmware_comparisons(
    nodes: &[rm::NodeInfo],
    manifest: &FirmwareManifest,
    component_filters: &HashMap<NodeType, &rm::FirmwareObjectComponentFilter>,
    firmware_type: &str,
    profiles: &crate::config::hardware_profiles::HardwareProfileCatalog,
    insecure_bmc: bool,
) -> (
    Vec<FirmwareComparisonOutcome>,
    Vec<PreparedFirmwareComparison>,
) {
    let mut outcomes = Vec::new();
    let mut ready = Vec::new();
    for (index, node_info) in nodes.iter().enumerate() {
        let node_id = node_info.node_id.clone();
        let node_type = match resolve_node_info(node_info) {
            Ok(node_type) => node_type,
            Err(error) => {
                outcomes.push((index, node_id, Err(error.to_string())));
                continue;
            }
        };
        let selection = match selection_for_node_type(component_filters, &[], node_type) {
            Ok(Some(selection)) => selection,
            Ok(None) => {
                outcomes.push((
                    index,
                    node_id,
                    Err(format!(
                        "node type {} is excluded by component filters",
                        node_type.as_str()
                    )),
                ));
                continue;
            }
            Err(error) => {
                outcomes.push((index, node_id, Err(error.to_string())));
                continue;
            }
        };
        let expectations = match plan::expectations_with_profile(
            manifest,
            node_type,
            firmware_type,
            &selection,
            profiles.resolve_request(node_type),
        ) {
            Ok(expectations) => expectations,
            Err(error) => {
                outcomes.push((index, node_id, Err(error.to_string())));
                continue;
            }
        };
        match build_ephemeral_inventory_node(node_info, node_type, profiles, insecure_bmc) {
            Ok(node) => ready.push(PreparedFirmwareComparison {
                index,
                node_id,
                node,
                expectations,
            }),
            Err(error) => outcomes.push((index, node_id, Err(error.message))),
        }
    }
    (outcomes, ready)
}

fn build_firmware_comparison_response(
    object_id: String,
    total_nodes: usize,
    mut outcomes: Vec<FirmwareComparisonOutcome>,
) -> rm::CompareFirmwareObjectResponse {
    outcomes.sort_by_key(|(index, _, _)| *index);
    let mut common = rm::NodeBatchResponse {
        status: rm::ReturnCode::Failure.into(),
        message: String::new(),
        node_results: Vec::with_capacity(total_nodes),
        job_id: String::new(),
        stats: None,
    };
    let mut response = rm::CompareFirmwareObjectResponse {
        response: None,
        object_id,
        matches_manifest: false,
        nodes: Vec::with_capacity(total_nodes),
    };
    let mut successful_nodes = 0u32;
    let mut failed_nodes = 0u32;
    for (_, node_id, outcome) in outcomes {
        match outcome {
            Ok(comparison) => {
                successful_nodes += 1;
                common.node_results.push(rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Success.into(),
                    error_message: String::new(),
                });
                response.nodes.push(comparison);
            }
            Err(error_message) => {
                failed_nodes += 1;
                tracing::error!(node = %node_id, error = %error_message, "firmware manifest comparison failed");
                common.node_results.push(rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message,
                });
            }
        }
    }

    let total_nodes = total_nodes as u32;
    common.stats = Some(rm::NodeOperationStats {
        total_nodes,
        successful_nodes,
        failed_nodes,
    });
    if total_nodes == 0 {
        common.message = "No nodes specified in request".to_owned();
        response.response = Some(common);
        return response;
    }

    let matching_nodes = response
        .nodes
        .iter()
        .filter(|node| node.matches_manifest)
        .count() as u32;
    response.matches_manifest = failed_nodes == 0
        && successful_nodes == total_nodes
        && response.nodes.iter().all(|node| node.matches_manifest);
    if failed_nodes == 0 {
        common.status = rm::ReturnCode::Success.into();
        common.message = format!(
            "Firmware manifest compared for {successful_nodes}/{total_nodes} nodes; {matching_nodes} match"
        );
    } else {
        common.message = format!(
            "Firmware manifest comparison completed with {successful_nodes} successes and {failed_nodes} failures out of {total_nodes} nodes; {matching_nodes} match"
        );
    }
    response.response = Some(common);
    response
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_compare_firmware_object(
        &self,
        req: Request<rm::CompareFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::CompareFirmwareObjectResponse>, Status> {
        let request = req.into_inner();
        let manifest: FirmwareManifest = request
            .config_json
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid config_json: {error}")))?;
        let object_id =
            firmware_object_id_from_manifest(&manifest).map_err(Status::invalid_argument)?;
        let firmware_type = plan::resolve_channel(&manifest, &request.firmware_type)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let component_filters = resolve_component_filter_selectors(
            &request.component_filters,
            &request.node_descriptor_component_filters,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let nodes = request.nodes.map(|nodes| nodes.nodes).unwrap_or_default();
        let total_nodes = nodes.len();
        let (mut outcomes, ready) = prepare_firmware_comparisons(
            &nodes,
            &manifest,
            &component_filters,
            &firmware_type,
            &self.hardware_profiles,
            self.insecure_bmc,
        );
        let fetched = self.fetch_firmware_comparison_outcomes(ready).await;
        outcomes.extend(fetched);
        Ok(Response::new(build_firmware_comparison_response(
            object_id,
            total_nodes,
            outcomes,
        )))
    }

    async fn fetch_firmware_comparison_outcomes(
        &self,
        ready: Vec<PreparedFirmwareComparison>,
    ) -> Vec<FirmwareComparisonOutcome> {
        futures::stream::iter(ready.into_iter().map(|prepared| async move {
            let comparison = async {
                if prepared.node.node_type() != NodeType::SwitchVrnvl72Nvidia {
                    self.initialize_nvue_client(prepared.node.nvue_client(), None)
                        .await
                        .map_err(|error| error.message)?;
                }
                let inventory = prepared
                    .node
                    .get_firmware_inventory()
                    .await
                    .map_err(|error| error.message)?;
                Ok(compare_firmware_object_for_node(
                    prepared.node_id.clone(),
                    prepared.node.node_type(),
                    &prepared.expectations,
                    &inventory,
                ))
            }
            .await;
            (prepared.index, prepared.node_id, comparison)
        }))
        .buffer_unordered(FIRMWARE_MANIFEST_COMPARE_CONCURRENCY)
        .collect()
        .await
    }

    pub(crate) async fn handle_add_firmware_object(
        &self,
        req: Request<rm::AddFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::AddFirmwareObjectResponse>, Status> {
        let r = req.into_inner();
        let manifest: FirmwareManifest = r
            .config_json
            .parse()
            .map_err(|e: manifest::ManifestError| Status::invalid_argument(e.to_string()))?;
        // Preserve the original document (including informational extension fields)
        // for storage, after typed parsing has rejected duplicate known fields.
        let config: Value = serde_json::from_str(&r.config_json)
            .map_err(|e| Status::invalid_argument(format!("invalid config_json: {e}")))?;
        let id = firmware_object_id_from_manifest(&manifest).map_err(Status::invalid_argument)?;

        if r.hardware_type.is_empty() {
            return Err(Status::invalid_argument("hardware_type is required"));
        }

        let catalog = ArtifactCatalog::preparing(&manifest).map_err(Status::invalid_argument)?;
        let parsed_value = Some(
            serde_json::to_value(&catalog).map_err(|error| Status::internal(error.to_string()))?,
        );

        let hw_type = RackHardwareType(r.hardware_type);
        self.backends
            .firmware_objects
            .create(&id, hw_type.clone(), config, parsed_value)
            .await
            .map_err(status_from_rms_error)?;

        // Choose the default atomically. An explicit request forces this bundle
        // to become the default; otherwise it becomes the default only if the
        // hardware type has none yet. set_default_if_none folds the former
        // check-then-act (has_default + set_default) into one serialized step,
        // so two concurrent first-time adds cannot both claim the default.
        //
        // We must use the row these methods return, not the one `create`
        // produced above: the default-selection UPDATE runs after `create`, so
        // the created row still carries `is_default = false` and a pre-update
        // `updated` timestamp. Both methods return the authoritative
        // post-selection row for `id` -- its `is_default` reflects whether this
        // bundle actually ended up the default (set_default_if_none leaves it
        // false when another bundle already owns the slot) -- which is exactly
        // the state the response must report.
        let firmware = if r.set_default {
            self.backends
                .firmware_objects
                .set_default(&id)
                .await
                .map_err(status_from_rms_error)?
        } else {
            self.backends
                .firmware_objects
                .set_default_if_none(&id)
                .await
                .map_err(status_from_rms_error)?
        };

        let cancel = CancellationToken::new();
        self.firmware_download_cancellations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(id.clone(), cancel.clone());
        self.spawn_catalog_preparation(
            firmware.clone(),
            manifest,
            catalog,
            optional_access_token(r.access_token),
            cancel,
        );

        Ok(Response::new(rm::AddFirmwareObjectResponse {
            object: Some(firmware_object_to_proto_with_profiles(
                firmware,
                Some(&self.hardware_profiles),
            )),
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
            object: Some(firmware_object_to_proto_with_profiles(
                firmware,
                Some(&self.hardware_profiles),
            )),
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
            .map(|firmware| {
                firmware_object_to_proto_with_profiles(firmware, Some(&self.hardware_profiles))
            })
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
            object: Some(firmware_object_to_proto_with_profiles(
                firmware,
                Some(&self.hardware_profiles),
            )),
        }))
    }

    #[allow(deprecated)]
    pub(crate) async fn handle_apply_stored_firmware_object(
        &self,
        req: Request<rm::ApplyStoredFirmwareObjectRequest>,
    ) -> std::result::Result<Response<rm::ApplyStoredFirmwareObjectResponse>, Status> {
        let r = req.into_inner();
        let firmware = self
            .resolve_requested_firmware(&r.object_id, &r.hardware_type)
            .await?;
        let object_id = firmware.id.clone();
        if !firmware.available {
            let message = match workflow::stored_catalog(&firmware) {
                Ok(Some(ArtifactCatalog {
                    preparation: Preparation::Failed { message },
                    ..
                })) => format!("firmware object {object_id} preparation failed: {message}"),
                _ => format!("firmware object {object_id} is not available"),
            };
            return Ok(Response::new(rm::ApplyStoredFirmwareObjectResponse {
                response: Some(batch_failure(message)),
                object_id,
                jobs: Vec::new(),
            }));
        }
        let devices = request_devices(r.nodes.as_ref());
        let selections = workflow::selections(
            &devices,
            &r.component_filters,
            &r.node_descriptor_component_filters,
            &r.components,
        );
        let (response, jobs) = self.start_firmware_apply(
            devices,
            FirmwareApply {
                source: FirmwareSource::Stored(firmware),
                selections,
                channel: r.firmware_type,
                force_update: r.force_update,
                access_token: None,
                rack_id: r.rack_id,
            },
        );
        Ok(Response::new(rm::ApplyStoredFirmwareObjectResponse {
            response: Some(response),
            object_id,
            jobs,
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
        let manifest = r
            .config_json
            .parse::<FirmwareManifest>()
            .map_err(|e| e.to_string())
            .and_then(|manifest| {
                firmware_object_id_from_manifest(&manifest)?;
                Ok(manifest)
            });
        let object_id = manifest
            .as_ref()
            .ok()
            .and_then(|manifest| firmware_object_id_from_manifest(manifest).ok())
            .unwrap_or_default();
        let devices = request_devices(r.nodes.as_ref());
        let selections = workflow::selections(
            &devices,
            &r.component_filters,
            &r.node_descriptor_component_filters,
            &[],
        );
        let (response, jobs) = self.start_firmware_apply(
            devices,
            FirmwareApply {
                source: FirmwareSource::Inline(manifest),
                selections,
                channel: r.firmware_type,
                force_update: r.force_update,
                access_token: optional_access_token(r.access_token),
                rack_id: r.rack_id,
            },
        );
        Ok(Response::new(rm::ApplyFirmwareObjectResponse {
            response: Some(response),
            object_id,
            jobs,
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
        let object_id = firmware.id.clone();
        if !firmware.available {
            return Ok(Response::new(rm::ApplyStoredSwitchSystemImageResponse {
                response: Some(batch_failure(format!(
                    "firmware object {object_id} is not available"
                ))),
                object_id,
                image_filename: String::new(),
                jobs: Vec::new(),
            }));
        }
        workflow::require_catalog(&firmware).map_err(Status::failed_precondition)?;
        let manifest = FirmwareManifest::try_from(&firmware.config)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let image_filename = match plan::resolve_channel(&manifest, &r.software_type)
            .and_then(|channel| plan::switch_image(&manifest, &channel))
            .map(|image| image.filename)
            .map_err(|error| error.to_string())
        {
            Ok(filename) => filename,
            Err(error) => {
                return Ok(Response::new(rm::ApplyStoredSwitchSystemImageResponse {
                    response: Some(batch_failure(error)),
                    object_id,
                    image_filename: String::new(),
                    jobs: Vec::new(),
                }));
            }
        };
        let (response, jobs) = self.start_switch_apply(
            request_devices(r.nodes.as_ref()),
            workflow::SwitchApply {
                allow_onie_recovery: r.allow_onie_recovery,
                source: FirmwareSource::Stored(firmware),
                channel: r.software_type,
                access_token: None,
                rack_id: r.rack_id,
            },
        );
        Ok(Response::new(rm::ApplyStoredSwitchSystemImageResponse {
            response: Some(response),
            object_id,
            image_filename,
            jobs,
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
        let manifest = r
            .config_json
            .parse::<FirmwareManifest>()
            .map_err(|e| e.to_string())
            .and_then(|manifest| {
                firmware_object_id_from_manifest(&manifest)?;
                Ok(manifest)
            });
        let object_id = manifest
            .as_ref()
            .ok()
            .and_then(|manifest| firmware_object_id_from_manifest(manifest).ok())
            .unwrap_or_default();
        let (response, jobs) = self.start_switch_apply(
            request_devices(r.nodes.as_ref()),
            workflow::SwitchApply {
                allow_onie_recovery: r.allow_onie_recovery,
                source: FirmwareSource::Inline(manifest),
                channel: r.software_type,
                access_token: optional_access_token(r.access_token),
                rack_id: r.rack_id,
            },
        );
        Ok(Response::new(rm::ApplySwitchSystemImageResponse {
            response: Some(response),
            object_id,
            image_filename: String::new(),
            jobs,
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

    fn precreate_firmware_jobs(
        &self,
        parent_job_id: String,
        devices: Vec<rm::NodeInfo>,
    ) -> PrecreatedFirmwareJobs {
        let total_nodes = devices.len() as u32;

        let (admitted, rejected) = self.job_tracker.create_batch_jobs(
            &parent_job_id,
            JobType::FirmwareUpdate,
            batch_targets(devices),
            |device| {
                let expected_inventory = self
                    .expected_inventory_policy_for_node(device)
                    .map_err(|error| error.message)?;
                let recovery = self
                    .recovery_profile_for_node(device)
                    .map_err(|error| error.message)?;
                Ok((expected_inventory, recovery))
            },
        );

        let failed_nodes = rejected.len() as u32;
        let node_results = failed_node_results(rejected).collect();

        let node_jobs = admitted
            .iter()
            .map(|(device, pending, _)| rm::NodeFirmwareJobInfo {
                node_id: device.node_id.clone(),
                job_id: pending.id().to_string(),
            })
            .collect();

        let precreated_jobs: Vec<_> = admitted
            .into_iter()
            .map(|(device, pending, (expected_inventory, recovery))| {
                let pending = self.trace_firmware_job(pending, resolve_node_info(&device).ok());
                PrecreatedFirmwareJob {
                    device,
                    pending,
                    expected_inventory,
                    recovery,
                }
            })
            .collect();

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
    ) -> PrecreatedSwitchImageJobs {
        let total_nodes = devices.len() as u32;

        let (admitted, rejected) = self.job_tracker.create_batch_jobs(
            &parent_job_id,
            JobType::SwitchSystemImageUpdate,
            batch_targets(devices),
            |_| Ok(()),
        );

        let failed_nodes = rejected.len() as u32;
        let node_results = failed_node_results(rejected).collect();

        let node_jobs = admitted
            .iter()
            .map(|(device, pending, ())| rm::SwitchSystemImageUpdateJobInfo {
                node_id: device.node_id.clone(),
                job_id: pending.id().to_string(),
            })
            .collect();

        let precreated_jobs: Vec<_> = admitted
            .into_iter()
            .map(|(device, pending, ())| PrecreatedSwitchImageJob { device, pending })
            .collect();

        if precreated_jobs.is_empty() {
            self.job_tracker
                .mark_failed_message(&parent_job_id, "No switch system image apply jobs created");
        }

        PrecreatedSwitchImageJobs {
            parent_job_id,
            jobs: precreated_jobs,
            node_jobs,
            node_results,
            total_nodes,
            failed_nodes,
        }
    }

    async fn dispatch_precreated_firmware_jobs(
        &self,
        prepared: workflow::PreparedFirmwareApply,
        jobs: Vec<PrecreatedFirmwareJob>,
        force_update: bool,
    ) {
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

            let Some(targets) = prepared.targets.get(&node_type).cloned() else {
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

            if !flat.has_credentials_for_node_type(node_type) {
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

            let node = match build_ephemeral_node_with_profiles(
                &job.device,
                node_type,
                job.expected_inventory.clone(),
                job.recovery.clone(),
                &self.hardware_profiles,
                self.insecure_bmc,
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

            if let Err(failure) = self.admit_firmware_node_for_job(&job.pending, &node) {
                fail_precreated_child(job.pending, failure, &mut refresh_child_id);

                continue;
            }

            self.spawn_profiled_firmware_update_job(
                job.pending,
                node,
                targets,
                true,
                force_update,
                true,
                self.recovery_dispatch(
                    prepared
                        .recovery
                        .get(&node_type)
                        .cloned()
                        .map_or(RecoverySource::Absent, RecoverySource::Prepared),
                ),
            );
        }
        refresh_parent_after_failures(&self.job_tracker, refresh_child_id);
    }

    fn dispatch_precreated_switch_image_jobs(
        &self,
        jobs: Vec<PrecreatedSwitchImageJob>,
        image_filename: String,
        local_file_path: String,
        allow_onie_recovery: bool,
    ) {
        if image_filename.is_empty() {
            mark_switch_jobs_failed(&self.job_tracker, jobs, "image_filename is required");
            return;
        }
        if !Path::new(&local_file_path).is_file() {
            mark_switch_jobs_failed_with_code(
                &self.job_tracker,
                jobs,
                JobError::FileNotFound,
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

            let recovery = match SwitchImageRecoveryOptions::from_node_info(
                &job.device,
                allow_onie_recovery,
            ) {
                Ok(recovery) => recovery,
                Err(error) => {
                    fail_precreated_child(
                        job.pending,
                        switch_job_failure(error.message),
                        &mut refresh_child_id,
                    );

                    continue;
                }
            };

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

            // The host endpoint is required for NVUE/SSH. Preserve valid BMC
            // data for activation; VR NVL72 capability construction below
            // rejects a missing or unusable BMC endpoint. GB200/GB300 retain
            // their host-only workflow.
            let bmc_endpoint = match flat.optional_bmc_endpoint(self.insecure_bmc) {
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
                recovery: None,
                runtime_config: None,
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

            let switch = Arc::new(tokio::sync::Mutex::new(switch));

            let pending = job.pending;
            let cancel = pending.cancellation_token();

            let service = self.clone();
            let job_tracker = self.job_tracker.clone();
            let image_filename = image_filename.clone();
            let local_file_path = local_file_path.clone();
            let target_build_id = target_build_id.clone();
            let switch_username = user;
            let switch_password = pass;
            let sftp_upload_options = self.sftp_upload_options;
            let readiness = self.switch_readiness_policy(node_type);

            // The switch workflow seals its own job through the tracked handle.
            // The tracked supervisor adds panic/abort handling around it.
            job_tracker
                .spawn_job(pending, move |job| async move {
                    run_switch_system_image_job(
                        &service,
                        job,
                        switch,
                        &image_filename,
                        &local_file_path,
                        &target_build_id,
                        &switch_username,
                        &switch_password,
                        &rack_id,
                        &node_id,
                        recovery,
                        readiness,
                        sftp_upload_options,
                        &cancel,
                    )
                    .await;
                })
                .detach();
        }
        refresh_parent_after_failures(&self.job_tracker, refresh_child_id);
    }

    fn ephemeral_firmware_cache(&self, object_id: &str) -> EphemeralFirmwareCache {
        let object_id = ephemeral_cache_object_id(object_id);
        let dir = firmware_object_cache_dir_for_subdir(
            &self.firmware_dir,
            EPHEMERAL_FIRMWARE_OBJECT_CACHE_SUBDIR,
            &object_id,
        );
        EphemeralFirmwareCache { dir }
    }
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
    recovery: Option<RecoveryProfile>,
}

struct PrecreatedSwitchImageJobs {
    parent_job_id: String,
    jobs: Vec<PrecreatedSwitchImageJob>,
    node_jobs: Vec<rm::SwitchSystemImageUpdateJobInfo>,
    node_results: Vec<rm::NodeOperationResult>,
    total_nodes: u32,
    failed_nodes: u32,
}

struct PrecreatedSwitchImageJob {
    device: rm::NodeInfo,
    pending: RmsJobHandle,
}

struct EphemeralFirmwareCache {
    dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FirmwareTargetMapping<'a> {
    source_component: &'a str,
    target_key: &'a str,
    redfish_target: &'a str,
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

    fn matches_profile_target(
        &self,
        target: &crate::config::hardware_profiles::FirmwareTargetPlan,
    ) -> bool {
        match self.mode {
            FirmwareObjectComponentSelectionMode::DefaultOnly => target.default_apply(),
            FirmwareObjectComponentSelectionMode::All => true,
            FirmwareObjectComponentSelectionMode::Components => self
                .components
                .iter()
                .any(|component| target.matches_component(component)),
        }
    }
}

fn firmware_node_type_lookup_keys() -> [(NodeType, &'static str); 16] {
    [
        (NodeType::ComputeGb200Nvidia, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb200Wiwynn, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb200Dell, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb300Nvidia, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb300Lenovo, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeGb300Supermicro, COMPUTE_LOOKUP_KEY),
        (NodeType::ComputeVrnvl72Nvidia, COMPUTE_LOOKUP_KEY),
        (NodeType::PowershelfGb200Liteon, POWER_SHELF_LOOKUP_KEY),
        (NodeType::PowershelfGb200Delta, POWER_SHELF_LOOKUP_KEY),
        (NodeType::PowershelfGb300Liteon, POWER_SHELF_LOOKUP_KEY),
        (NodeType::PowershelfGb300Delta, POWER_SHELF_LOOKUP_KEY),
        (NodeType::PowershelfVrnvl72Liteon, POWER_SHELF_LOOKUP_KEY),
        (NodeType::PowershelfVrnvl72Delta, POWER_SHELF_LOOKUP_KEY),
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
        validate_firmware_object_component_selection(node_type, global_components)?;
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
    node_type: NodeType,
    filter: &rm::FirmwareObjectComponentFilter,
) -> std::result::Result<FirmwareObjectComponentSelection, String> {
    validate_firmware_object_component_selection(node_type, &filter.components)?;
    Ok(if filter.components.is_empty() {
        FirmwareObjectComponentSelection::all()
    } else {
        FirmwareObjectComponentSelection::components(filter.components.clone())
    })
}

fn validate_firmware_object_component_selection(
    node_type: NodeType,
    components: &[String],
) -> std::result::Result<(), String> {
    if node_type.supports_flint_inband_firmware()
        && components
            .iter()
            .any(|component| component.trim().eq_ignore_ascii_case("BF3_BFB"))
    {
        return Err(
            "BF3_BFB and Arm OS installation are not supported; select BF3_NIC with a Flint-compatible firmware image"
                .to_owned(),
        );
    }
    Ok(())
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

/// Match known manifest names with interchangeable ASCII spaces and hyphens.
/// Preserve every other byte: this is separator compatibility, not typo guessing.
fn firmware_source_component_matches(actual: &str, expected: &str) -> bool {
    let separators = |byte| if byte == b'-' { b' ' } else { byte };
    actual
        .bytes()
        .map(separators)
        .eq(expected.bytes().map(separators))
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
        candidates
            .iter()
            .any(|candidate| firmware_source_component_matches(candidate, &wanted))
    })
}

fn ephemeral_cache_object_id(object_id: &str) -> String {
    format!("{object_id}-{}", uuid::Uuid::new_v4())
}

fn firmware_object_id_from_manifest(
    manifest: &FirmwareManifest,
) -> std::result::Result<String, String> {
    let milestone = manifest.single_milestone().map_err(|e| e.to_string())?;
    let id = format!("{}_{}", manifest.product_name, milestone.name);
    validate_firmware_object_id(&id).map_err(str::to_owned)?;
    Ok(id)
}

fn request_devices(nodes: Option<&rm::NodeSet>) -> Vec<rm::NodeInfo> {
    nodes.map(|nodes| nodes.nodes.clone()).unwrap_or_default()
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
    mark_switch_jobs_failed_with_code(tracker, jobs, JobError::InvalidArgument, message);
}

/// Preserve preparation failure classification in switch-image job status.
fn mark_switch_jobs_failed_with_code(
    tracker: &Arc<crate::orchestrator::job_tracker::JobTracker>,
    jobs: Vec<PrecreatedSwitchImageJob>,
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

fn fail_precreated_child(
    job: RmsJobHandle,
    failure: JobFailure,
    refresh_child_id: &mut Option<JobId>,
) {
    let child_id = job.id().clone();
    job.fail(failure);
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
    // Deliberately node-less, as this job touches no hardware
    let pending = match registry.create_job(
        JobSpec::new(
            "",
            "",
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

/// Recovery bundles for an update that names no firmware object: the
/// latest available object whose manifest carries a recovery package for
/// the node type. Fails closed when the candidates span several rack
/// hardware types. Only the chosen object's files are resolved, so an older
/// object prepared before recovery packages were catalogued (present in its
/// manifest, absent from its cache) cannot block a newer usable one.
pub(crate) async fn resolve_recovery_bundles_from_latest_object(
    store: &dyn FirmwareObjectStore,
    firmware_dir: &Path,
    node_type: NodeType,
    artifact_require_sha256: bool,
) -> crate::utilities::error::Result<RecoveryBundles> {
    let objects = store
        .list(FirmwareObjectSearchFilter {
            only_available: true,
            ..Default::default()
        })
        .await?;
    // The latest available object wins, but only when that pick is
    // unambiguous. Every compute variant selects the same HMC component, and
    // nothing at dispatch names the rack's hardware type. A wrong-variant
    // recovery push can be permanent (ERoT anti-rollback fuses). Candidates
    // that span several hardware types therefore fail closed. An "any"-typed
    // object stays compatible with every type, like the apply-path guard.
    // Known ceiling: this path cannot pin a bundle per node. Per-node
    // pinning arrives with the bundle-selection decision.
    let mut latest = None;
    let mut hardware_types: Vec<RackHardwareType> = Vec::new();
    for object in objects {
        let Some(candidate) = object_recovery_artifacts(&object, node_type)? else {
            continue;
        };
        if object.rack_hardware_type != RackHardwareType::any()
            && !hardware_types.contains(&object.rack_hardware_type)
        {
            hardware_types.push(object.rack_hardware_type.clone());
        }
        if latest.is_none() {
            latest = Some((object, candidate));
        }
    }
    if hardware_types.len() > 1 {
        let names: Vec<String> = hardware_types.iter().map(ToString::to_string).collect();
        return Err(RmsError::failed_precondition(format!(
            "available recovery packages for {node_type} span multiple hardware types ({}): \
             launch the update from a firmware object to pin one",
            names.join(", ")
        )));
    }
    let Some((object, (artifacts, catalog))) = latest else {
        return Err(RmsError::failed_precondition(format!(
            "no available firmware object carries an HMC recovery package for {node_type}"
        )));
    };
    let cache = firmware_object_cache_dir(firmware_dir, &object.id);
    if artifact_require_sha256 {
        for artifact in [&artifacts.regular, &artifacts.recovery] {
            artifact
                .verify_required_http_digest(&cache)
                .await
                .map_err(|error| RmsError::failed_precondition(error.to_string()))?;
        }
    }
    artifacts
        .resolve(&cache, &catalog.ready_artifacts)
        .map_err(|error| {
            RmsError::failed_precondition(format!("firmware object {}: {error}", object.id))
        })
}

/// The object's recovery artifacts by manifest, with its catalog. `None`
/// when the manifest carries no recovery package for the node type, or the
/// object has no artifact catalog and so cannot be applied at all. Says
/// nothing about cached bytes: `RecoveryArtifacts::resolve` does.
fn object_recovery_artifacts(
    object: &FirmwareObject,
    node_type: NodeType,
) -> crate::utilities::error::Result<Option<(plan::RecoveryArtifacts, ArtifactCatalog)>> {
    if workflow::stored_catalog(object)
        .map_err(RmsError::failed_precondition)?
        .is_none()
    {
        return Ok(None);
    }
    let (manifest, catalog) =
        workflow::load_stored(object).map_err(RmsError::failed_precondition)?;
    Ok(plan::recovery_artifacts(&manifest, node_type)
        .map_err(|error| RmsError::failed_precondition(error.to_string()))?
        .map(|artifacts| (artifacts, catalog)))
}

/// Test seam: store `manifest` as a prepared object with every catalog
/// artifact materialized under the service's firmware directory.
#[cfg(test)]
pub(crate) async fn store_prepared_manifest(
    service: &RackManagerServiceImpl,
    id: &str,
    hardware_type: RackHardwareType,
    manifest: &FirmwareManifest,
) -> FirmwareObject {
    let cache = firmware_object_cache_dir(&service.firmware_dir, id);
    let mut catalog = ArtifactCatalog::preparing(manifest).expect("manifest serializes");
    catalog.ready_artifacts = artifacts::materialize(
        &plan::catalog_artifacts(manifest).expect("manifest plans"),
        &cache,
        None,
        false,
        &service.artifact_download_policy,
        &CancellationToken::new(),
    )
    .await
    .expect("local artifacts materialize");
    catalog.preparation = Preparation::Ready;
    let firmware = service
        .backends
        .firmware_objects
        .create(
            id,
            hardware_type,
            serde_json::to_value(manifest).expect("manifest serializes"),
            None,
        )
        .await
        .expect("object stored");
    service
        .backends
        .firmware_objects
        .finish_preparation(
            &firmware.id,
            firmware.created,
            serde_json::to_value(catalog).expect("catalog serializes"),
            true,
        )
        .await
        .expect("preparation published")
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
        "LINKX",
        "BF3_NIC",
        "CX7",
        "CX8",
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

fn firmware_object_to_proto_with_profiles(
    firmware: FirmwareObject,
    profiles: Option<&crate::config::hardware_profiles::HardwareProfileCatalog>,
) -> rm::FirmwareObject {
    // Objects without an artifact catalog cannot be applied; publish no
    // metadata for them rather than projecting an unsupported layout.
    let metadata = firmware
        .parsed_components
        .as_ref()
        .is_some_and(|value| value.get("format_version").is_some())
        .then(|| {
            FirmwareManifest::try_from(&firmware.config)
                .map_err(|e| e.to_string())
                .and_then(|manifest| metadata::from_manifest_with_profiles(&manifest, profiles))
                .inspect_err(|error| {
                    // Keep the catalog retrievable even when today's platform
                    // selector cannot project it, but never return partial metadata
                    // or silently disguise ambiguity as an empty platform list.
                    tracing::warn!(object_id = firmware.id, %error, "failed to project firmware object metadata");
                })
                .ok()
        })
        .flatten();
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

/// Only downloadable .bin images participate in prefetch and apply selection.
/// Metadata projection separately retains advertised names without URLs.
fn downloadable_nvos_location(location: &manifest::Location) -> bool {
    artifacts::has_download_location(location)
        && artifacts::advertised_filename(location)
            .is_some_and(|name| name.to_ascii_lowercase().ends_with(".bin"))
}

/// Select exactly one downloadable image; never impose this rule on prefetch.
fn select_nvos_location<'a>(
    context: &str,
    locations: &'a [manifest::Location],
) -> std::result::Result<&'a manifest::Location, String> {
    let mut candidates = locations
        .iter()
        .filter(|location| downloadable_nvos_location(location));
    let selected = candidates
        .next()
        .ok_or_else(|| format!("{context}.Locations must include an NVOS .bin image location"))?;
    if candidates.next().is_some() {
        return Err(format!(
            "{context}.Locations contains multiple NVOS .bin image candidates"
        ));
    }
    Ok(selected)
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.trim().is_empty())
}

fn optional_access_token(access_token: Option<String>) -> Option<String> {
    access_token.filter(|token| !token.trim().is_empty())
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

fn manifest_component_matches_target(component: &str, target_key: &str) -> bool {
    if normalized_manifest_component(component) != normalized_manifest_component(target_key) {
        return false;
    }

    firmware_component_instances_match(component, target_key)
}

fn firmware_component_instances_match(component: &str, candidate: &str) -> bool {
    match (
        firmware_component_instance_identity(component),
        firmware_component_instance_identity(candidate),
    ) {
        (Some(component_instance), Some(target_instance)) => component_instance == target_instance,
        (Some(_), None) => false,
        _ => true,
    }
}

fn firmware_component_instance_identity(component: &str) -> Option<String> {
    let normalized = normalized_firmware_identity(
        component
            .split(',')
            .next()
            .unwrap_or(component)
            .split(':')
            .next()
            .unwrap_or(component),
    );
    let normalized = normalized
        .strip_prefix("hgxfw")
        .or_else(|| normalized.strip_prefix("hgx"))
        .or_else(|| normalized.strip_prefix("mgxfw"))
        .or_else(|| normalized.strip_prefix("mgx"))
        .or_else(|| normalized.strip_prefix("fw"))
        .unwrap_or(&normalized);
    const INSTANCE_PREFIXES: &[&str] = &[
        "pcieswitchconfig",
        "inforomgpu",
        "erotfpga",
        "erotbmc",
        "erotcpu",
        "hgxbmc",
        "fpga",
        "cpld",
        "bmc",
        "hmc",
        "cpu",
        "gpu",
    ];
    INSTANCE_PREFIXES.iter().find_map(|prefix| {
        let instance = normalized.strip_prefix(prefix)?;
        (!instance.is_empty() && instance.chars().all(|character| character.is_ascii_digit()))
            .then(|| format!("{prefix}{instance}"))
    })
}

fn compare_firmware_object_for_node(
    node_id: String,
    node_type: NodeType,
    expectations: &[FirmwareManifestExpectation],
    inventory: &[FirmwareInfo],
) -> rm::NodeFirmwareManifestComparison {
    let components = expectations
        .iter()
        .map(|expectation| compare_firmware_object_component(node_type, expectation, inventory))
        .collect::<Vec<_>>();
    let matches_manifest = !components.is_empty()
        && components
            .iter()
            .all(|component| component.status == rm::FirmwareComparisonStatus::Matched as i32);
    rm::NodeFirmwareManifestComparison {
        node_id,
        matches_manifest,
        components,
    }
}

fn compare_firmware_object_component(
    node_type: NodeType,
    expectation: &FirmwareManifestExpectation,
    inventory: &[FirmwareInfo],
) -> rm::FirmwareManifestComponentComparison {
    let mut identity_candidates = inventory
        .iter()
        .filter(|installed| firmware_inventory_matches_expectation(installed, expectation))
        .collect::<Vec<_>>();
    identity_candidates.sort_by_key(|installed| (&installed.name, &installed.target));

    let mut comparison = rm::FirmwareManifestComponentComparison {
        component: expectation.component.clone(),
        installed_component: String::new(),
        installed_version: String::new(),
        proposed_version: expectation.version.clone(),
        status: rm::FirmwareComparisonStatus::Unspecified.into(),
        target: expectation.target.clone(),
        reason: String::new(),
        sku_id: expectation.sku_id.clone(),
    };
    if identity_candidates.is_empty() {
        if is_flint_manifest_component(&expectation.component) {
            comparison.status = rm::FirmwareComparisonStatus::Unverified.into();
            comparison.reason =
                "host-side Flint inventory is not included in the node firmware inventory query"
                    .to_owned();
        } else {
            comparison.status = rm::FirmwareComparisonStatus::Missing.into();
            comparison.reason =
                "component was not found in installed firmware inventory".to_owned();
        }
        return comparison;
    }

    let candidates = if expectation.sku_id.trim().is_empty() {
        identity_candidates
    } else {
        let expected_sku = normalized_firmware_identity(&expectation.sku_id);
        let sku_matches = identity_candidates
            .iter()
            .copied()
            .filter(|installed| {
                !installed.sku.trim().is_empty()
                    && normalized_firmware_identity(&installed.sku) == expected_sku
            })
            .collect::<Vec<_>>();
        if !sku_matches.is_empty() {
            sku_matches
        } else if identity_candidates
            .iter()
            .any(|installed| installed.sku.trim().is_empty())
        {
            let sku_unreported = identity_candidates
                .iter()
                .copied()
                .filter(|installed| installed.sku.trim().is_empty())
                .collect::<Vec<_>>();
            set_installed_comparison_fields(&mut comparison, &sku_unreported);
            comparison.status = rm::FirmwareComparisonStatus::Unverified.into();
            comparison.reason = format!(
                "installed firmware inventory did not report the SKU needed to verify {}",
                expectation.sku_id
            );
            return comparison;
        } else {
            comparison.status = rm::FirmwareComparisonStatus::Missing.into();
            comparison.reason = format!(
                "component with SKU {} was not found in installed firmware inventory",
                expectation.sku_id
            );
            return comparison;
        }
    };

    set_installed_comparison_fields(&mut comparison, &candidates);

    if candidates
        .iter()
        .any(|installed| installed.version.trim().is_empty())
    {
        comparison.status = rm::FirmwareComparisonStatus::Unverified.into();
        comparison.reason = "installed firmware inventory did not report a version".to_owned();
    } else if candidates.iter().all(|installed| {
        if node_type == NodeType::SwitchVrnvl72Nvidia
            && normalized_manifest_component(&expectation.component) == "cpld"
        {
            nvfwupd::workflow_api::switch_firmware_versions_match(
                &expectation.component,
                &installed.version,
                &expectation.version,
            )
        } else {
            firmware_versions_match(&installed.version, &expectation.version)
        }
    }) {
        comparison.status = rm::FirmwareComparisonStatus::Matched.into();
    } else {
        comparison.status = rm::FirmwareComparisonStatus::Mismatched.into();
        comparison.reason = "installed version differs from proposed manifest".to_owned();
    }
    comparison
}

fn set_installed_comparison_fields(
    comparison: &mut rm::FirmwareManifestComponentComparison,
    candidates: &[&FirmwareInfo],
) {
    let mut installed_components = candidates
        .iter()
        .map(|installed| installed.name.clone())
        .collect::<Vec<_>>();
    installed_components.sort();
    installed_components.dedup();
    comparison.installed_component = installed_components.join(", ");

    let mut installed_versions = candidates
        .iter()
        .map(|installed| installed.version.trim().to_owned())
        .collect::<Vec<_>>();
    installed_versions.sort();
    installed_versions.dedup();
    comparison.installed_version = installed_versions.join(", ");
}

fn firmware_inventory_matches_expectation(
    installed: &FirmwareInfo,
    expectation: &FirmwareManifestExpectation,
) -> bool {
    if !expectation.target.trim().is_empty() {
        if installed.target.eq_ignore_ascii_case(&expectation.target) {
            return true;
        }
        if !installed.target.trim().is_empty()
            && is_firmware_inventory_resource(&expectation.target)
        {
            return false;
        }
    }

    if normalized_manifest_component(&expectation.component)
        == normalized_inventory_component(&installed.name)
        && firmware_component_instances_match(&expectation.component, &installed.name)
    {
        return true;
    }
    let installed_target = installed.target.rsplit('/').next().unwrap_or_default();
    normalized_manifest_component(&expectation.component)
        == normalized_inventory_component(installed_target)
        && firmware_component_instances_match(&expectation.component, installed_target)
}

fn is_firmware_inventory_resource(target: &str) -> bool {
    target
        .to_ascii_lowercase()
        .contains("/updateservice/firmwareinventory/")
}

fn normalized_manifest_component(component: &str) -> String {
    let component = component
        .split(',')
        .next()
        .unwrap_or(component)
        .split(':')
        .next()
        .unwrap_or(component);
    normalized_component_category(component, false)
}

fn normalized_inventory_component(component: &str) -> String {
    let normalized = normalized_firmware_identity(component);
    let hgx_component = normalized.starts_with("hgx");
    let normalized = normalized
        .strip_prefix("mgxfw")
        .or_else(|| normalized.strip_prefix("mgx"))
        .or_else(|| normalized.strip_prefix("hgxfw"))
        .or_else(|| normalized.strip_prefix("hgx"))
        .unwrap_or(&normalized);
    normalized_component_category(normalized, hgx_component)
}

fn normalized_component_category(component: &str, hgx_component: bool) -> String {
    let normalized = normalized_firmware_identity(component);
    if normalized.contains("inforom") {
        "inforom".to_owned()
    } else if normalized.contains("erot") {
        "erot".to_owned()
    } else if normalized.contains("smr") || normalized.contains("fpga") {
        "fpga".to_owned()
    } else if normalized.contains("cpld") {
        "cpld".to_owned()
    } else if normalized.contains("sbios")
        || normalized.contains("bios")
        || normalized.contains("cpu")
    {
        "sbios".to_owned()
    } else if normalized.contains("hmc") || (hgx_component && normalized.contains("bmc")) {
        "hmc".to_owned()
    } else if normalized.contains("bmc") {
        "bmc".to_owned()
    } else if normalized.starts_with("nvswitchsma") || normalized == "sma" {
        "sma".to_owned()
    } else if normalized.contains("gpu") && !normalized.contains("sma") {
        "gpu".to_owned()
    } else if normalized.contains("pcie") {
        "pcieswitch".to_owned()
    } else if normalized.contains("cx7") {
        "cx7".to_owned()
    } else if normalized.contains("cx8") {
        "cx8".to_owned()
    } else if normalized.contains("bf3") || normalized.contains("bluefield3") {
        "bf3nic".to_owned()
    } else if normalized.contains("pmc") {
        "pmc".to_owned()
    } else if normalized.contains("psu") {
        "psu".to_owned()
    } else {
        normalized
    }
}

fn normalized_firmware_identity(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

pub(super) fn firmware_versions_match(installed: &str, proposed: &str) -> bool {
    installed.trim().eq_ignore_ascii_case(proposed.trim())
}

fn is_flint_manifest_component(component: &str) -> bool {
    matches!(
        normalized_manifest_component(component).as_str(),
        "cx7" | "cx8" | "bf3nic" | "linkx"
    )
}

fn location_is_firmware_payload(location_type: Option<&str>) -> bool {
    matches!(
        location_type,
        Some(location_type)
            if location_type.eq_ignore_ascii_case("Firmware")
                || location_type.eq_ignore_ascii_case("Binary")
    )
}

/// The manifest's `Type` field alone cannot select recovery payloads. Across
/// real exports the recovery fwpkg has appeared correctly typed
/// (1_3_6-GA.json), typed empty (GB200NVL_72x1-1.3.10-Build5_JULY.json),
/// and typed empty while the CoRIM file carried `Firmware-Recovery`
/// (GB200CX8NVL_72x1-1.3.7-build6_GA.json). The `.fwpkg` requirement
/// excludes the mis-typed CoRIM. The type-or-`_recovery`-infix rule
/// covers every mislabeling observed so far.
fn location_is_recovery_payload(location_type: Option<&str>, artifact_name: &str) -> bool {
    let name = artifact_name.to_ascii_lowercase();
    name.ends_with(".fwpkg")
        && (location_type.is_some_and(|ty| ty.eq_ignore_ascii_case("Firmware-Recovery"))
            || name.contains("_recovery"))
}

fn flint_firmware_payload_filename(filename: &str) -> bool {
    let filename = filename.to_ascii_lowercase();
    filename.ends_with(".bin") || filename.ends_with(".pldm")
}

fn firmware_payload_allowed_for_component(component: &str, filename: &str) -> bool {
    if component.eq_ignore_ascii_case("LINKX") {
        return filename.to_ascii_lowercase().ends_with(".bin");
    }
    if component.eq_ignore_ascii_case("BF3_BFB") {
        return false;
    }
    if ["CX7", "CX8", "BF3_NIC"]
        .iter()
        .any(|candidate| component.eq_ignore_ascii_case(candidate))
    {
        return flint_firmware_payload_filename(filename);
    }
    true
}

fn device_type_for_node_type(node_type: NodeType) -> Option<DeviceType> {
    match node_type {
        NodeType::ComputeGb200Nvidia
        | NodeType::ComputeGb200Wiwynn
        | NodeType::ComputeGb200Dell => Some(DeviceType::ComputeGb200Nvidia),
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
        | NodeType::PowershelfGb300Delta
        | NodeType::PowershelfVrnvl72Liteon
        | NodeType::PowershelfVrnvl72Delta => Some(DeviceType::PowerShelf),
    }
}

fn firmware_component_allowed_for_node_type(node_type: NodeType, target_key: &str) -> bool {
    let target_key = target_key_from_component_key(target_key);
    if matches!(target_key.as_str(), "CX7" | "CX8" | "BF3_NIC")
        && !node_type.supports_flint_inband_firmware()
    {
        return false;
    }
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
        // All-PSU updates use UpdateService.HttpPushUri, including on VR.
        // OEM per-PSU action support is a separate NVFWUPD model policy.
        NodeType::PowershelfGb200Liteon
        | NodeType::PowershelfGb300Liteon
        | NodeType::PowershelfVrnvl72Liteon => {
            return matches!(target_key.as_str(), "LiteOnPMC" | "LiteOnPSU");
        }
        NodeType::PowershelfGb200Delta
        | NodeType::PowershelfGb300Delta
        | NodeType::PowershelfVrnvl72Delta => {
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
) -> Vec<FirmwareTargetMapping<'static>> {
    // Dell GB200 always uses its embedded or configured profile. It has no
    // role-keyed legacy catalog interpretation.
    if node_type == NodeType::ComputeGb200Dell {
        return Vec::new();
    }
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

fn get_firmware_component_mappings_for_device_type(
    device_type: &DeviceType,
) -> Vec<FirmwareTargetMapping<'static>> {
    match device_type {
        DeviceType::ComputeVrnvl72Nvidia => vec![
            flint_firmware_mapping("LINKX"),
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
            let mut mappings = vec![
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
            ];
            if *device_type == DeviceType::ComputeGb200Nvidia {
                mappings.extend([
                    flint_firmware_mapping("CX7"),
                    flint_firmware_mapping("CX8"),
                    flint_firmware_mapping("BF3_NIC"),
                ]);
            }
            mappings
        }
        DeviceType::SwitchGb200Nvidia | DeviceType::SwitchGb300Nvidia => vec![
            switch_firmware_mapping("BMC+FPGA+EROT", "BMC", "bmc"),
            switch_firmware_mapping("BMC+FPGA+EROT", "FPGA", "fpga"),
            switch_firmware_mapping("BMC+FPGA+EROT", "EROT", "erot"),
            switch_firmware_mapping("CPLD", "CPLD", "cpld"),
            switch_firmware_mapping("SBIOS+EROT", "BIOS", "bios"),
            switch_firmware_mapping("SBIOS+EROT", "EROT", "erot"),
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
) -> FirmwareTargetMapping<'static> {
    FirmwareTargetMapping {
        source_component: "HMC",
        target_key,
        redfish_target,
        default_apply: false,
    }
}

fn flint_firmware_mapping(component: &'static str) -> FirmwareTargetMapping<'static> {
    FirmwareTargetMapping {
        source_component: component,
        target_key: component,
        redfish_target: component,
        default_apply: false,
    }
}

fn switch_firmware_mapping(
    source_component: &'static str,
    target_key: &'static str,
    redfish_target: &'static str,
) -> FirmwareTargetMapping<'static> {
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
) -> FirmwareTargetMapping<'static> {
    FirmwareTargetMapping {
        source_component,
        target_key,
        redfish_target: "",
        default_apply: true,
    }
}

fn get_firmware_flash_order(node_type: NodeType) -> &'static [&'static str] {
    match node_type {
        NodeType::ComputeGb200Dell => &[], // Order comes exclusively from its hardware profile.
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
            "CX7",
            "CX8",
            "BF3_NIC",
            "",
        ],
        NodeType::PowershelfGb200Liteon
        | NodeType::PowershelfGb200Delta
        | NodeType::PowershelfGb300Liteon
        | NodeType::PowershelfGb300Delta
        // VR Delta reuses the GB Delta order until hardware shows it differs.
        | NodeType::PowershelfVrnvl72Delta => &["DeltaPMC", "LiteOnPMC", "DeltaPSU", "LiteOnPSU"],
        NodeType::PowershelfVrnvl72Liteon => &["LiteOnPMC", "LiteOnPSU"],
    }
}

#[cfg(test)]
mod tests;
