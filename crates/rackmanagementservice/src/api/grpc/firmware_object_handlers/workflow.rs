// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared object preparation and apply lifecycle. Stored objects apply only
//! from a versioned artifact catalog; new execution never constructs a fake DB
//! object.

use std::collections::HashMap;
use std::path::Path;

use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use super::artifacts::{self, ArtifactCatalog, CATALOG_VERSION, Preparation};
use super::plan::{self, FirmwarePlan};
use super::*;
use crate::api::grpc::artifact_download::ArtifactError;
use crate::domain::node::{FirmwareTarget, RecoveryBundles};
use crate::orchestrator::job_lifecycle::JobLifecycleState;
use plan::RecoveryArtifacts;

pub(super) enum FirmwareSource {
    Inline(Result<FirmwareManifest, String>),
    Stored(FirmwareObject),
}

pub(super) struct FirmwareApply {
    pub source: FirmwareSource,
    pub selections: Result<HashMap<NodeType, FirmwareObjectComponentSelection>, String>,
    pub channel: String,
    pub force_update: bool,
    pub access_token: Option<String>,
    pub rack_id: String,
}

pub(super) struct SwitchApply {
    pub allow_onie_recovery: bool,
    pub source: FirmwareSource,
    pub channel: String,
    pub access_token: Option<String>,
    pub rack_id: String,
}

pub(super) struct PreparedFirmwareApply {
    pub(super) channel: String,
    pub(super) targets: HashMap<NodeType, Vec<FirmwareTarget>>,
    /// The two HMC files a node type with a recovery flow recovers from,
    /// materialized with the payloads. Absent when the manifest carries no
    /// recovery package for that type: its jobs fail closed at detection.
    pub(super) recovery: HashMap<NodeType, RecoveryBundles>,
}

struct PreparedSwitchImage {
    channel: String,
    filename: String,
    path: String,
}

pub(super) fn selections(
    nodes: &[rm::NodeInfo],
    filters: &HashMap<i32, rm::FirmwareObjectComponentFilter>,
    descriptors: &[rm::NodeDescriptorFirmwareObjectComponentFilter],
    global: &[String],
) -> Result<HashMap<NodeType, FirmwareObjectComponentSelection>, String> {
    let filters =
        resolve_component_filter_selectors(filters, descriptors).map_err(|e| e.to_string())?;
    let mut selections = HashMap::new();
    for node in nodes {
        let node_type = resolve_node_info(node).map_err(|e| e.to_string())?;
        if let Some(selection) = selection_for_node_type(&filters, global, node_type)? {
            selections.insert(node_type, selection);
        }
    }
    if selections.is_empty() {
        return Err("no node types selected for firmware apply".to_owned());
    }
    Ok(selections)
}

/// The stored object's artifact catalog. `None` marks an object prepared
/// before versioned catalogs existed; RMS no longer applies those.
pub(super) fn stored_catalog(firmware: &FirmwareObject) -> Result<Option<ArtifactCatalog>, String> {
    let Some(value) = firmware.parsed_components.as_ref() else {
        return Ok(None);
    };
    if value.get("format_version").is_none() {
        return Ok(None);
    }
    let catalog: ArtifactCatalog = serde_json::from_value(value.clone())
        .map_err(|e| format!("invalid stored artifact catalog: {e}"))?;
    if catalog.format_version != CATALOG_VERSION {
        return Err(format!(
            "unsupported stored artifact catalog version {}",
            catalog.format_version
        ));
    }
    Ok(Some(catalog))
}

/// Returns the stored catalog or instructions to save, delete, and re-add an
/// object without one.
pub(super) fn require_catalog(firmware: &FirmwareObject) -> Result<ArtifactCatalog, String> {
    stored_catalog(firmware)?.ok_or_else(|| {
        format!(
            "firmware object {} has no artifact catalog; save its manifest and settings, delete the object with DeleteFirmwareObject, then re-add the manifest with AddFirmwareObject",
            firmware.id
        )
    })
}

pub(super) fn load_stored(
    firmware: &FirmwareObject,
) -> Result<(FirmwareManifest, ArtifactCatalog), String> {
    let catalog = require_catalog(firmware)?;
    match &catalog.preparation {
        Preparation::Ready if firmware.available => {}
        Preparation::Failed { message } => {
            return Err(format!("firmware object preparation failed: {message}"));
        }
        _ => return Err("firmware object preparation is not ready".to_owned()),
    }
    let manifest = FirmwareManifest::try_from(&firmware.config).map_err(|e| e.to_string())?;
    if artifacts::manifest_revision(&manifest)? != catalog.revision {
        return Err(
            "stored manifest changed after artifact preparation; re-add this object".to_owned(),
        );
    }
    Ok((manifest, catalog))
}

impl RackManagerServiceImpl {
    pub(super) fn start_switch_apply(
        &self,
        devices: Vec<rm::NodeInfo>,
        request: SwitchApply,
    ) -> (
        rm::NodeBatchResponse,
        Vec<rm::SwitchSystemImageUpdateJobInfo>,
    ) {
        if devices.is_empty() {
            return (
                batch_failure("No devices specified in request".to_owned()),
                Vec::new(),
            );
        }
        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(&request.rack_id, JobType::SwitchSystemImageUpdate)
        else {
            return (
                batch_failure("failed to create parent switch system image apply job".to_owned()),
                Vec::new(),
            );
        };
        let precreated = self.precreate_switch_image_jobs(parent_id, devices);
        let response = accepted_batch_with_stats(
            precreated.parent_job_id,
            precreated.total_nodes,
            precreated.jobs.len() as u32,
            precreated.failed_nodes,
            precreated.node_results,
            "Created switch system image apply jobs. Stage 0 prepares the selected image; use GetSwitchSystemImageJobStatus to track completion.",
        );
        if !precreated.jobs.is_empty() {
            let service = self.clone();
            tokio::spawn(async move {
                let ids = switch_image_job_ids(&precreated.jobs);
                let ephemeral = matches!(request.source, FirmwareSource::Inline(_));
                let cache = match &request.source {
                    FirmwareSource::Inline(_) => {
                        service.ephemeral_firmware_cache("switch-apply").dir
                    }
                    FirmwareSource::Stored(firmware) => {
                        firmware_object_cache_dir(&service.firmware_dir, &firmware.id)
                    }
                };
                mark_switch_jobs_running(
                    &service.job_tracker,
                    &precreated.jobs,
                    "Stage 0: preparing selected switch image",
                );
                let result = service.prepare_switch_apply(&request, &cache).await;
                let channel = match result {
                    Ok(prepared) => {
                        service.dispatch_precreated_switch_image_jobs(
                            precreated.jobs,
                            prepared.filename,
                            prepared.path,
                            request.allow_onie_recovery,
                        );

                        Some(prepared.channel)
                    }
                    Err(error) => {
                        mark_switch_jobs_failed_with_code(
                            &service.job_tracker,
                            precreated.jobs,
                            JobError::from(&error),
                            &error.to_string(),
                        );
                        None
                    }
                };
                if ephemeral {
                    spawn_ephemeral_object_cache_cleanup(
                        service.job_tracker.registry(),
                        ids,
                        cache,
                    );
                } else if let FirmwareSource::Stored(firmware) = request.source
                    && let Some(channel) = channel
                {
                    service.record_completed_apply(firmware, request.rack_id, channel, ids);
                }
            });
        }
        (response, precreated.node_jobs)
    }

    async fn prepare_switch_apply(
        &self,
        request: &SwitchApply,
        cache: &Path,
    ) -> Result<PreparedSwitchImage, ArtifactError> {
        let (manifest, catalog) = match &request.source {
            FirmwareSource::Inline(manifest) => (
                manifest
                    .as_ref()
                    .map_err(|e| ArtifactError::InvalidArgument(e.clone()))?
                    .clone(),
                None,
            ),
            FirmwareSource::Stored(firmware) => {
                let (manifest, catalog) = load_stored(firmware).map_err(ArtifactError::Cache)?;
                (manifest, Some(catalog))
            }
        };
        let channel = plan::resolve_channel(&manifest, &request.channel)?;
        let image = plan::switch_image(&manifest, &channel)?;
        // Validate switch build naming before any transfer.
        infer_target_build_id(&image.filename)
            .map_err(|e| ArtifactError::InvalidArgument(e.message))?;
        let ready = if let Some(catalog) = catalog {
            catalog.ready_artifacts
        } else {
            artifacts::materialize(
                std::slice::from_ref(&image),
                cache,
                request.access_token.as_deref(),
                self.artifact_require_sha256,
                &self.artifact_download_policy,
                &CancellationToken::new(),
            )
            .await?
        };
        if !ready.contains(&image.id) {
            return Err(ArtifactError::Cache(
                "selected switch image was not successfully prepared".to_owned(),
            ));
        }
        let path = image.cache_path(cache).map_err(ArtifactError::Cache)?;
        if !path.is_file() {
            return Err(ArtifactError::Cache(format!(
                "prepared switch image {} is missing",
                path.display()
            )));
        }
        if self.artifact_require_sha256 {
            image.verify_required_http_digest(cache).await?;
        }
        Ok(PreparedSwitchImage {
            channel,
            filename: image.filename,
            path: path.to_string_lossy().into_owned(),
        })
    }

    pub(super) fn start_firmware_apply(
        &self,
        devices: Vec<rm::NodeInfo>,
        request: FirmwareApply,
    ) -> (rm::NodeBatchResponse, Vec<rm::NodeFirmwareJobInfo>) {
        if devices.is_empty() {
            return (
                batch_failure("No devices specified in request".to_owned()),
                Vec::new(),
            );
        }
        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(&request.rack_id, JobType::FirmwareUpdate)
        else {
            return (
                batch_failure("failed to create parent firmware object apply job".to_owned()),
                Vec::new(),
            );
        };
        let precreated = self.precreate_firmware_jobs(parent_id, devices);
        let response = accepted_batch_with_stats(
            precreated.parent_job_id,
            precreated.total_nodes,
            precreated.jobs.len() as u32,
            precreated.failed_nodes,
            precreated.node_results,
            "Created firmware object apply jobs. Stage 0 plans and prepares artifacts; use GetFirmwareJobStatus to track completion.",
        );
        if !precreated.jobs.is_empty() {
            self.spawn_firmware_apply(request, precreated.jobs);
        }
        (response, precreated.node_jobs)
    }

    pub(super) fn spawn_catalog_preparation(
        &self,
        firmware: FirmwareObject,
        manifest: FirmwareManifest,
        mut catalog: ArtifactCatalog,
        access_token: Option<String>,
        cancel: CancellationToken,
    ) {
        let service = self.clone();
        tokio::spawn(async move {
            let cache = firmware_object_cache_dir(&service.firmware_dir, &firmware.id);
            let work = async {
                let artifacts = plan::catalog_artifacts(&manifest)?;
                if artifacts.is_empty() {
                    return Err(ArtifactError::InvalidArgument(
                        "manifest contains no downloadable artifacts".to_owned(),
                    ));
                }
                artifacts::materialize(
                    &artifacts,
                    &cache,
                    access_token.as_deref(),
                    service.artifact_require_sha256,
                    &service.artifact_download_policy,
                    &cancel,
                )
                .await
            };
            let result = std::panic::AssertUnwindSafe(work)
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(ArtifactError::Cache(
                        "artifact preparation panicked".to_owned(),
                    ))
                });
            catalog.preparation = match result {
                Ok(ready) => {
                    catalog.ready_artifacts = ready;
                    Preparation::Ready
                }
                Err(error) => Preparation::Failed {
                    message: error.to_string(),
                },
            };
            // A cancelled/deleted incarnation must never publish into a new one.
            if !cancel.is_cancelled() {
                let available = matches!(catalog.preparation, Preparation::Ready);
                let result = match serde_json::to_value(&catalog) {
                    Ok(value) => service
                        .backends
                        .firmware_objects
                        .finish_preparation(&firmware.id, firmware.created, value, available)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.message),
                    Err(error) => Err(error.to_string()),
                };
                if let Err(error) = result {
                    tracing::error!(object_id = firmware.id, %error, "cannot publish artifact preparation result");
                }
            }
            let mut cancellations = service
                .firmware_download_cancellations
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !cancel.is_cancelled() {
                cancellations.remove(&firmware.id);
            }
        });
    }

    pub(super) fn spawn_firmware_apply(
        &self,
        request: FirmwareApply,
        jobs: Vec<PrecreatedFirmwareJob>,
    ) {
        let service = self.clone();
        tokio::spawn(async move {
            let ids = firmware_job_ids(&jobs);
            let ephemeral = matches!(request.source, FirmwareSource::Inline(_));
            let cache = match &request.source {
                FirmwareSource::Inline(_) => service.ephemeral_firmware_cache("apply").dir,
                FirmwareSource::Stored(firmware) => {
                    firmware_object_cache_dir(&service.firmware_dir, &firmware.id)
                }
            };
            // Register cleanup once, after preparation/dispatch has stopped
            // touching files; jobs keep the cache alive until terminal.
            let channel = service.run_firmware_apply(&request, jobs, &cache).await;
            if ephemeral {
                spawn_ephemeral_object_cache_cleanup(service.job_tracker.registry(), ids, cache);
            } else if let FirmwareSource::Stored(firmware) = request.source
                && let Some(channel) = channel
            {
                service.record_completed_apply(firmware, request.rack_id, channel, ids);
            }
        });
    }

    async fn run_firmware_apply(
        &self,
        request: &FirmwareApply,
        jobs: Vec<PrecreatedFirmwareJob>,
        cache: &Path,
    ) -> Option<String> {
        mark_firmware_jobs_running(
            &self.job_tracker,
            &jobs,
            "Stage 0: planning and preparing firmware artifacts",
        );
        let prepared = self.prepare_firmware_apply(request, cache).await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                mark_firmware_jobs_failed(
                    &self.job_tracker,
                    jobs,
                    JobError::from(&error),
                    &error.to_string(),
                );
                return None;
            }
        };
        let channel = prepared.channel.clone();
        self.dispatch_precreated_firmware_jobs(prepared, jobs, request.force_update)
            .await;
        Some(channel)
    }

    async fn prepare_firmware_apply(
        &self,
        request: &FirmwareApply,
        cache: &Path,
    ) -> Result<PreparedFirmwareApply, ArtifactError> {
        let selections = request
            .selections
            .as_ref()
            .map_err(|e| ArtifactError::InvalidArgument(e.clone()))?;
        let (manifest, catalog) = match &request.source {
            FirmwareSource::Inline(manifest) => (
                manifest
                    .as_ref()
                    .map_err(|e| ArtifactError::InvalidArgument(e.clone()))?
                    .clone(),
                None,
            ),
            FirmwareSource::Stored(firmware) => {
                let (manifest, catalog) = load_stored(firmware).map_err(ArtifactError::Cache)?;
                (manifest, Some(catalog))
            }
        };
        let channel = plan::resolve_channel(&manifest, &request.channel)?;
        let plans: Vec<FirmwarePlan> = selections
            .iter()
            .map(|(node_type, selection)| {
                plan::firmware_plan_with_profile(
                    &manifest,
                    *node_type,
                    &channel,
                    selection,
                    self.hardware_profiles.resolve_request(*node_type),
                )
            })
            .collect::<Result<_, _>>()?;
        // Recovery packages ride along with the payloads: same download, same
        // digest policy, same cache. Resolved here, where the manifest is in
        // hand, so an inline apply recovers from its own manifest and never
        // from another object's release.
        let mut recovery_plan: HashMap<NodeType, RecoveryArtifacts> = HashMap::new();
        for node_type in selections.keys().filter(|t| t.has_recovery_flow()) {
            if let Some(artifacts) = plan::recovery_artifacts(&manifest, *node_type)? {
                recovery_plan.insert(*node_type, artifacts);
            }
        }
        let artifacts: Vec<_> = plans
            .iter()
            .flat_map(|plan| plan.targets.iter().map(|target| target.artifact.clone()))
            .chain(
                recovery_plan
                    .values()
                    .flat_map(|artifacts| [artifacts.regular.clone(), artifacts.recovery.clone()]),
            )
            .collect();
        let ready = if let Some(catalog) = catalog {
            if self.artifact_require_sha256 {
                for artifact in artifacts.iter().filter(|artifact| artifact.required) {
                    artifact.verify_required_http_digest(cache).await?;
                }
            }
            catalog.ready_artifacts
        } else {
            artifacts::materialize(
                &artifacts,
                cache,
                request.access_token.as_deref(),
                self.artifact_require_sha256,
                &self.artifact_download_policy,
                &CancellationToken::new(),
            )
            .await?
        };
        let targets = plans
            .into_iter()
            .map(|plan| {
                let targets = plan.resolve(cache, &ready)?;
                validate_firmware_object_request(plan.node_type, &targets, true)
                    .map_err(|e| ArtifactError::InvalidArgument(e.message))?;
                Ok((plan.node_type, targets))
            })
            .collect::<Result<_, ArtifactError>>()?;
        let recovery = recovery_plan
            .iter()
            .map(|(node_type, artifacts)| Ok((*node_type, artifacts.resolve(cache, &ready)?)))
            .collect::<Result<_, ArtifactError>>()?;
        Ok(PreparedFirmwareApply {
            channel,
            targets,
            recovery,
        })
    }

    /// Acceptance is not an apply. Record only children whose tracked workflow
    /// actually completed; failed/rejected nodes never enter successful history.
    pub(super) fn record_completed_apply(
        &self,
        firmware: FirmwareObject,
        rack_id: String,
        channel: String,
        ids: Vec<String>,
    ) {
        let store = self.backends.firmware_objects.clone();
        let registry = self.job_tracker.registry();
        let timeout = self.workflow_timings.apply_history_timeout;
        tokio::spawn(async move {
            let ids: Vec<_> = ids.into_iter().map(JobId::from).collect();
            let outcomes = registry
                .wait_for_terminal_outcomes(&ids, Some(timeout))
                .await;
            for id in outcomes.missing {
                tracing::warn!(job_id = %id, "missing firmware apply outcome; not recording success");
            }
            if !outcomes.pending.is_empty() {
                tracing::warn!(
                    object_id = firmware.id,
                    pending = outcomes.pending.len(),
                    "apply history observation deadline expired; recording completed nodes only"
                );
            }
            let mut successful: Vec<_> = outcomes
                .terminal
                .into_iter()
                .filter(|snapshot| matches!(snapshot.state, JobLifecycleState::Completed { .. }))
                .map(|snapshot| snapshot.node_id)
                .collect();
            if successful.is_empty() {
                return;
            }
            successful.sort();
            successful.dedup();
            if let Err(error) = store
                .record_apply_for_incarnation(
                    &firmware.id,
                    firmware.created,
                    &rack_id,
                    &channel,
                    firmware.rack_hardware_type,
                    &successful,
                )
                .await
            {
                tracing::warn!(
                    object_id = firmware.id,
                    error = error.message,
                    "failed to record completed firmware apply"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests;
