// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::super::tests::test_service;
use super::*;
use crate::orchestrator::job_lifecycle::JobSpec;
use crate::utilities::error::ErrorCode;
use std::time::Duration;

mod hardware_profiles;

fn compute_selections() -> Result<HashMap<NodeType, FirmwareObjectComponentSelection>, String> {
    Ok(HashMap::from([(
        NodeType::ComputeGb200Nvidia,
        FirmwareObjectComponentSelection::default_only(),
    )]))
}

#[tokio::test]
async fn inline_and_stored_linkx_apply_preserve_rollout_policy() {
    let temp = tempfile::tempdir().unwrap();
    let service = test_service();
    let mut manifest = super::super::tests::linkx_manifest();
    let path = temp.path().join("module.bin");
    tokio::fs::write(&path, b"firmware fixture").await.unwrap();
    let location = &mut manifest.milestones[0].board_skus[0].components.firmware[0].locations[1];
    location.location = Some(path.to_string_lossy().into_owned());
    location.location_type = Some("FILE".to_owned());
    let stored_cache = temp.path().join("stored");
    let stored = store_ready_manifest(&service, &manifest, &stored_cache).await;
    for (source, cache) in [
        (
            FirmwareSource::Inline(Ok(manifest)),
            temp.path().join("inline"),
        ),
        (FirmwareSource::Stored(stored), stored_cache),
    ] {
        let prepared = service
            .prepare_firmware_apply(
                &FirmwareApply {
                    source,
                    selections: Ok(HashMap::from([(
                        NodeType::ComputeVrnvl72Nvidia,
                        FirmwareObjectComponentSelection::components(vec!["LINKX".to_owned()]),
                    )])),
                    channel: "prod".to_owned(),
                    force_update: false,
                    access_token: None,
                    rack_id: "rack".to_owned(),
                },
                &cache,
            )
            .await
            .unwrap();
        let [target] = prepared.targets[&NodeType::ComputeVrnvl72Nvidia].as_slice() else {
            panic!("one LINKX image expected");
        };
        assert_eq!(target.component, "LINKX");
        assert_eq!(target.expected_version.as_deref(), Some("44.40.2035"));
        assert_eq!(
            target.linkx_policy.as_ref().unwrap().current_version,
            "44.40.2019"
        );
        assert_eq!(
            tokio::fs::read(&target.firmware_file).await.unwrap(),
            b"firmware fixture"
        );
    }
}

#[tokio::test]
async fn switch_dispatch_distinguishes_missing_cache_from_invalid_filename() {
    for (filename, expected) in [
        ("", JobError::InvalidArgument),
        ("nvos-amd64-25.02.4440.bin", JobError::FileNotFound),
    ] {
        let service = test_service();
        let parent = service
            .job_tracker
            .create_parent_job("rack", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let pending = service
            .job_tracker
            .create_child_job(&parent, "rack", "switch", JobType::SwitchSystemImageUpdate)
            .unwrap();
        let id = pending.id().clone();
        let temp = tempfile::tempdir().unwrap();

        service.dispatch_precreated_switch_image_jobs(
            vec![PrecreatedSwitchImageJob {
                device: rm::NodeInfo::default(),
                pending,
            }],
            filename.to_owned(),
            temp.path()
                .join("missing.bin")
                .to_string_lossy()
                .into_owned(),
            false,
        );

        let JobLifecycleState::Failed { failure, .. } =
            service.job_tracker.registry().get(&id).unwrap().state
        else {
            panic!("dispatch must fail without contacting hardware");
        };
        assert_eq!(failure.downcast_ref::<JobError>(), Some(&expected));
    }
}

async fn local_manifest(dir: &Path, channel: &str) -> FirmwareManifest {
    let bmc = dir.join("bmc.fwpkg");
    let nvos = dir.join("nvos-amd64-25.02.4440.bin");
    tokio::fs::write(&bmc, b"firmware fixture").await.unwrap();
    tokio::fs::write(&nvos, b"software fixture").await.unwrap();
    serde_json::from_value(serde_json::json!({
        "ProductName": "GB200", "Milestones": [{"Name": "test", "BoardSKUs": [
            {"Type": "Compute Node", "Components": {"Firmware": [{
                "Component": "BMC", "Version": "1.0", "Type": channel,
                "Locations": [{"Location": bmc, "LocationType": "file", "Type": "Firmware"}]
            }]}},
            {"Type": "Switch Tray", "Components": {"Software": [{
                "Component": "NVOS", "Version": "25.02.4440", "Type": channel,
                "Locations": [{"Location": nvos, "LocationType": "file", "Type": "Binary"}]
            }]}}
        ]}]
    }))
    .unwrap()
}

async fn store_ready_manifest(
    service: &RackManagerServiceImpl,
    manifest: &FirmwareManifest,
    cache: &Path,
) -> FirmwareObject {
    let mut catalog = ArtifactCatalog::preparing(manifest).unwrap();
    catalog.ready_artifacts = artifacts::materialize(
        &plan::catalog_artifacts(manifest).unwrap(),
        cache,
        None,
        false,
        &service.artifact_download_policy,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    catalog.preparation = Preparation::Ready;
    let firmware = service
        .backends
        .firmware_objects
        .create(
            "GB200_test",
            RackHardwareType::any(),
            serde_json::to_value(manifest).unwrap(),
            None,
        )
        .await
        .unwrap();
    service
        .backends
        .firmware_objects
        .finish_preparation(
            &firmware.id,
            firmware.created,
            serde_json::to_value(catalog).unwrap(),
            true,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn inline_and_stored_firmware_and_nvos_infer_the_same_channel() {
    for channel in ["Prod", "Dev"] {
        let temp = tempfile::tempdir().unwrap();
        let service = test_service();
        let manifest = local_manifest(temp.path(), channel).await;
        let stored_cache = temp.path().join("stored");
        let firmware = store_ready_manifest(&service, &manifest, &stored_cache).await;
        for (source, cache) in [
            (
                FirmwareSource::Inline(Ok(manifest)),
                temp.path().join("inline"),
            ),
            (FirmwareSource::Stored(firmware), stored_cache),
        ] {
            let apply = FirmwareApply {
                source,
                selections: compute_selections(),
                channel: String::new(),
                force_update: false,
                access_token: None,
                rack_id: "rack-1".to_owned(),
            };
            let prepared = service
                .prepare_firmware_apply(&apply, &cache)
                .await
                .unwrap();
            assert_eq!(prepared.channel, channel.to_ascii_lowercase());
            let targets = &prepared.targets[&NodeType::ComputeGb200Nvidia];
            assert_eq!(targets.len(), 1);
            assert_eq!(
                tokio::fs::read(&targets[0].firmware_file).await.unwrap(),
                b"firmware fixture"
            );

            let switch = SwitchApply {
                allow_onie_recovery: false,
                source: apply.source,
                channel: String::new(),
                access_token: None,
                rack_id: "rack-1".to_owned(),
            };

            let prepared = service.prepare_switch_apply(&switch, &cache).await.unwrap();
            assert_eq!(prepared.channel, channel.to_ascii_lowercase());
            assert_eq!(prepared.filename, "nvos-amd64-25.02.4440.bin");
            assert_eq!(
                tokio::fs::read(&prepared.path).await.unwrap(),
                b"software fixture"
            );
        }
    }
}

#[tokio::test]
async fn inline_and_stored_apply_materialize_every_hmc_and_bmc_location() {
    let temp = tempfile::tempdir().unwrap();
    let service = test_service();
    let mut manifest = local_manifest(temp.path(), "Prod").await;
    manifest.milestones[0].board_skus.truncate(1);
    let mut components = Vec::new();
    let mut expected = Vec::new();
    // Component and file names deliberately differ from the required flash order.
    for component in ["BMC", "HMC"] {
        let mut locations = Vec::new();
        for index in [3, 1, 2] {
            let filename = format!("{component}-{index}.fwpkg");
            let path = temp.path().join(&filename);
            tokio::fs::write(&path, filename.as_bytes()).await.unwrap();
            locations.push(serde_json::json!({
                "Location": path, "LocationType": "FILE", "Type": "Firmware"
            }));
        }
        components.push(
            serde_json::from_value(serde_json::json!({
                "Component": component, "Type": "Prod", "Version": "1.0", "Locations": locations
            }))
            .unwrap(),
        );
    }
    manifest.milestones[0].board_skus[0].components.firmware = components;
    for component in ["HMC", "BMC"] {
        for index in [3, 1, 2] {
            expected.push(format!("{component}-{index}.fwpkg"));
        }
    }
    let stored_cache = temp.path().join("stored");
    let firmware = store_ready_manifest(&service, &manifest, &stored_cache).await;

    for (source, cache) in [
        (
            FirmwareSource::Inline(Ok(manifest)),
            temp.path().join("inline"),
        ),
        (FirmwareSource::Stored(firmware), stored_cache),
    ] {
        let apply = FirmwareApply {
            source,
            selections: Ok(HashMap::from([(
                NodeType::ComputeGb200Wiwynn,
                FirmwareObjectComponentSelection::default_only(),
            )])),
            channel: "prod".to_owned(),
            force_update: false,
            access_token: None,
            rack_id: "rack-1".to_owned(),
        };

        let prepared = service
            .prepare_firmware_apply(&apply, &cache)
            .await
            .unwrap();

        let targets = &prepared.targets[&NodeType::ComputeGb200Wiwynn];
        assert_eq!(targets.len(), expected.len());
        for (target, filename) in targets.iter().zip(&expected) {
            assert_eq!(
                Path::new(&target.firmware_file)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                filename
            );
            assert_eq!(
                tokio::fs::read(&target.firmware_file).await.unwrap(),
                filename.as_bytes()
            );
            assert_eq!(
                target.component,
                if filename.starts_with("HMC") {
                    "HMC"
                } else {
                    "BMC"
                }
            );
            assert_eq!(
                crate::nodes::nvfwupd_adapter::firmware_target_selector(target),
                if filename.starts_with("HMC") {
                    SUPERMICRO_HGX_TARGET
                } else {
                    ""
                }
            );
        }
    }
}

#[tokio::test]
async fn ambiguous_default_fails_before_materializing_any_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let mut manifest = local_manifest(temp.path(), "Prod").await;
    manifest.milestones[0].board_skus[1].components.software[0].channel = Some("Dev".to_owned());
    let cache = temp.path().join("must-not-exist");
    let service = test_service();
    let apply = FirmwareApply {
        source: FirmwareSource::Inline(Ok(manifest)),
        selections: compute_selections(),
        channel: String::new(),
        force_update: false,
        access_token: None,
        rack_id: "rack-1".to_owned(),
    };
    let error = service
        .prepare_firmware_apply(&apply, &cache)
        .await
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("multiple firmware types: dev, prod"),
        "{error}"
    );

    let switch = SwitchApply {
        allow_onie_recovery: false,
        source: apply.source,
        channel: String::new(),
        access_token: None,
        rack_id: "rack-1".to_owned(),
    };

    let error = service
        .prepare_switch_apply(&switch, &cache)
        .await
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("multiple firmware types: dev, prod"),
        "{error}"
    );
    assert!(!cache.exists());
}

/// An available object whose parsed components predate versioned catalogs.
async fn store_uncatalogued_object(service: &RackManagerServiceImpl, id: &str) -> FirmwareObject {
    let store = &service.backends.firmware_objects;
    store
        .create(
            id,
            RackHardwareType::any(),
            Value::Null,
            Some(serde_json::json!({"devices": {"Compute Node": {"HMC": {
                "filename": "prod/hmc.fwpkg", "target": "/redfish/v1/Chassis/HGX_Chassis_0",
                "component": "HMC", "bundle": "GB200", "firmware_type": "prod",
                "version": null, "subcomponents": []
            }}}})),
        )
        .await
        .unwrap();
    store.set_available(id, true).await.unwrap()
}

#[tokio::test]
async fn uncatalogued_stored_objects_require_reimport() {
    let temp = tempfile::tempdir().unwrap();
    let mut service = test_service();
    service.firmware_dir = temp.path().to_path_buf();
    let firmware = store_uncatalogued_object(&service, "uncatalogued").await;
    let cache = firmware_object_cache_dir(&service.firmware_dir, &firmware.id);
    let apply = FirmwareApply {
        source: FirmwareSource::Stored(firmware.clone()),
        selections: compute_selections(),
        channel: String::new(),
        force_update: false,
        access_token: None,
        rack_id: "rack-1".to_owned(),
    };
    let error = service
        .prepare_firmware_apply(&apply, &cache)
        .await
        .err()
        .unwrap()
        .to_string();

    assert!(
        error.contains("delete the object with DeleteFirmwareObject, then re-add the manifest with AddFirmwareObject"),
        "{error}"
    );

    let switch = SwitchApply {
        allow_onie_recovery: false,
        source: FirmwareSource::Stored(firmware.clone()),
        channel: String::new(),
        access_token: None,
        rack_id: "rack-1".to_owned(),
    };
    let error = service
        .prepare_switch_apply(&switch, &cache)
        .await
        .err()
        .unwrap()
        .to_string();

    assert!(
        error.contains("delete the object with DeleteFirmwareObject, then re-add the manifest with AddFirmwareObject"),
        "{error}"
    );

    assert!(
        firmware_object_to_proto_with_profiles(firmware, None)
            .metadata
            .is_none()
    );
}

#[tokio::test]
async fn stored_switch_image_apply_rejects_uncatalogued_object() {
    let service = test_service();
    let firmware = store_uncatalogued_object(&service, "uncatalogued").await;
    let status = service
        .handle_apply_stored_switch_system_image(tonic::Request::new(
            rm::ApplyStoredSwitchSystemImageRequest {
                object_id: firmware.id,
                ..Default::default()
            },
        ))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);

    assert!(
        status.message().contains("delete the object with DeleteFirmwareObject, then re-add the manifest with AddFirmwareObject"),
        "{status}"
    );
}

#[tokio::test]
async fn preparation_failure_codes_reach_the_child_job() {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};
    for (status, expected) in [
        (401, JobError::Unauthenticated),
        (404, JobError::FileNotFound),
        (503, JobError::ServerError),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let service = test_service();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let mut manifest = local_manifest(temp.path(), "Prod").await;
        let location =
            &mut manifest.milestones[0].board_skus[0].components.firmware[0].locations[0];
        location.location = Some(format!("{}/bmc.fwpkg", server.uri()));
        location.location_type = Some("http".to_owned());
        let apply = FirmwareApply {
            source: FirmwareSource::Inline(Ok(manifest)),
            selections: compute_selections(),
            channel: String::new(),
            force_update: false,
            access_token: None,
            rack_id: "rack-1".to_owned(),
        };
        let parent = service
            .job_tracker
            .create_parent_job("rack-1", JobType::FirmwareUpdate)
            .unwrap();
        let pending = service
            .job_tracker
            .create_child_job(&parent, "rack-1", "node-1", JobType::FirmwareUpdate)
            .unwrap();
        let id = pending.id().clone();
        let jobs = vec![PrecreatedFirmwareJob {
            pending,
            device: rm::NodeInfo::default(),
            expected_inventory: None,
            recovery: None,
        }];
        assert!(
            service
                .run_firmware_apply(&apply, jobs, &temp.path().join("cache"))
                .await
                .is_none()
        );
        let JobLifecycleState::Failed { failure, .. } =
            service.job_tracker.registry().get(&id).unwrap().state
        else {
            panic!("preparation should fail the job")
        };
        assert_eq!(failure.downcast_ref::<JobError>(), Some(&expected));
    }
}

#[tokio::test]
async fn validation_checksum_and_cache_failures_are_permanent_and_distinct() {
    for (kind, expected) in [
        ("channel", JobError::InvalidArgument),
        ("checksum", JobError::FailedPrecondition),
        ("cache", JobError::FailedPrecondition),
        ("missing", JobError::FileNotFound),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let service = test_service();
        let mut manifest = local_manifest(temp.path(), "Prod").await;
        let cache = temp.path().join("cache");
        let bmc = &mut manifest.milestones[0].board_skus[0].components.firmware[0];
        match kind {
            "channel" => bmc.channel = None,
            "checksum" => bmc.locations[0].sha256 = Some("0".repeat(64)),
            "cache" => tokio::fs::write(&cache, b"not a directory").await.unwrap(),
            "missing" => {
                bmc.locations[0].location = Some(
                    temp.path()
                        .join("missing.fwpkg")
                        .to_string_lossy()
                        .into_owned(),
                )
            }
            _ => unreachable!(),
        }
        let apply = FirmwareApply {
            source: FirmwareSource::Inline(Ok(manifest)),
            selections: compute_selections(),
            channel: "Prod".to_owned(),
            force_update: false,
            access_token: None,
            rack_id: "rack-1".to_owned(),
        };
        let error = service
            .prepare_firmware_apply(&apply, &cache)
            .await
            .err()
            .unwrap();
        assert_eq!(error.job_error(), expected, "{kind}: {error}");
    }
}

#[tokio::test]
async fn history_deadline_records_success_without_waiting_forever_for_stuck_jobs() {
    for running in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut service = test_service();
        service.workflow_timings.apply_history_timeout = Duration::from_millis(20);
        let manifest = local_manifest(temp.path(), "Prod").await;
        let firmware = store_ready_manifest(&service, &manifest, &temp.path().join("cache")).await;
        let registry = service.job_tracker.registry();
        let completed = registry
            .create_leaf(JobSpec::new(
                "rack-1",
                "completed",
                "Queued",
                tracing::Span::none(),
            ))
            .unwrap();
        let failed = registry
            .create_leaf(JobSpec::new(
                "rack-1",
                "failed",
                "Queued",
                tracing::Span::none(),
            ))
            .unwrap();
        let stuck = registry
            .create_leaf(JobSpec::new(
                "rack-1",
                "stuck",
                "Queued",
                tracing::Span::none(),
            ))
            .unwrap();
        let ids = vec![
            completed.id().to_string(),
            failed.id().to_string(),
            stuck.id().to_string(),
            "missing".to_owned(),
        ];
        completed.complete("done", "");
        failed.fail(job_failure(JobError::ServerError, "failed"));
        if running {
            stuck.progress("Running");
        }
        service.record_completed_apply(
            firmware.clone(),
            "rack-1".to_owned(),
            "prod".to_owned(),
            ids,
        );
        let history = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let history = service
                    .backends
                    .firmware_objects
                    .list_apply_history(Some(&firmware.id), &[])
                    .await
                    .unwrap();
                if !history.is_empty() {
                    break history;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("history collection must end even with a non-terminal child");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].node_ids, ["completed"]);
        assert!(
            !registry.get(stuck.id()).unwrap().state.is_terminal(),
            "history expiry must not cancel hardware work"
        );
    }
}

#[tokio::test]
async fn stored_catalog_version_marks_uncatalogued_and_rejects_future_formats() {
    let temp = tempfile::tempdir().unwrap();
    let service = test_service();
    let manifest = local_manifest(temp.path(), "Prod").await;
    let mut firmware = store_ready_manifest(&service, &manifest, &temp.path().join("cache")).await;
    assert_eq!(
        stored_catalog(&firmware).unwrap().unwrap().format_version,
        CATALOG_VERSION
    );
    firmware.parsed_components.as_mut().unwrap()["format_version"] =
        serde_json::json!(CATALOG_VERSION + 1);
    assert!(
        stored_catalog(&firmware)
            .unwrap_err()
            .contains("unsupported stored artifact catalog version")
    );
    firmware.parsed_components = Some(serde_json::json!({"devices": {}}));
    assert!(stored_catalog(&firmware).unwrap().is_none());
    assert!(
        require_catalog(&firmware)
            .unwrap_err()
            .contains("has no artifact catalog")
    );
}

#[tokio::test]
async fn apply_returns_the_inferred_channel_for_successful_history() {
    let temp = tempfile::tempdir().unwrap();
    let service = test_service();
    let manifest = local_manifest(temp.path(), "Dev").await;
    let cache = temp.path().join("cache");
    let firmware = store_ready_manifest(&service, &manifest, &cache).await;
    let apply = FirmwareApply {
        source: FirmwareSource::Stored(firmware.clone()),
        selections: compute_selections(),
        channel: String::new(),
        force_update: false,
        access_token: None,
        rack_id: "rack-1".to_owned(),
    };
    // No hardware jobs are dispatched; exercise the effective-channel handoff.
    let channel = service
        .run_firmware_apply(&apply, Vec::new(), &cache)
        .await
        .unwrap();
    assert_eq!(channel, "dev");
    let pending = service
        .job_tracker
        .registry()
        .create_leaf(JobSpec::new(
            "rack-1",
            "compute-1",
            "Queued",
            tracing::Span::none(),
        ))
        .unwrap();
    let job_id = pending.id().to_string();
    pending.complete("done", "");
    service.record_completed_apply(firmware.clone(), "rack-1".to_owned(), channel, vec![job_id]);
    let history = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let history = service
                .backends
                .firmware_objects
                .list_apply_history(Some(&firmware.id), &[])
                .await
                .unwrap();
            if !history.is_empty() {
                break history;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].firmware_type, "dev");
    assert_eq!(history[0].node_ids, ["compute-1"]);
}

async fn recovery_manifest(dir: &Path, tag: &str) -> FirmwareManifest {
    let regular = dir.join(format!("{tag}-hmc.fwpkg"));
    let recovery = dir.join(format!("{tag}-hmc_recovery.fwpkg"));
    tokio::fs::write(&regular, b"regular").await.unwrap();
    tokio::fs::write(&recovery, b"recovery").await.unwrap();
    serde_json::from_value(serde_json::json!({
        "ProductName": "GB200", "Milestones": [{"Name": "test", "BoardSKUs": [
            {"Type": "Compute Node", "Components": {"Firmware": [{
                "Component": "HMC", "Version": "1.0", "Type": "Prod",
                "Locations": [
                    {"Location": regular, "LocationType": "file", "Type": "Firmware"},
                    {"Location": recovery, "LocationType": "file", "Type": "Firmware-Recovery"}
                ]
            }]}}
        ]}]
    }))
    .unwrap()
}

fn gb200_all() -> Result<HashMap<NodeType, FirmwareObjectComponentSelection>, String> {
    Ok(HashMap::from([(
        NodeType::ComputeGb200Nvidia,
        FirmwareObjectComponentSelection::all(),
    )]))
}

fn assert_bundles_under(bundles: &RecoveryBundles, cache: &Path, tag: &str) {
    for (path, name) in [
        (&bundles.regular, format!("{tag}-hmc.fwpkg")),
        (&bundles.recovery, format!("{tag}-hmc_recovery.fwpkg")),
    ] {
        let path = Path::new(path);
        assert!(path.starts_with(cache), "{}", path.display());
        assert_eq!(path.file_name().unwrap(), name.as_str());
        assert!(path.is_file(), "{}", path.display());
    }
}

#[tokio::test]
async fn prepared_apply_carries_recovery_bundles_from_its_own_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let mut service = test_service();
    service.firmware_dir = temp.path().to_path_buf();
    let manifest = recovery_manifest(temp.path(), "a").await;
    let hardware = RackHardwareType("gb200-nvl72".to_owned());
    store_prepared_manifest(&service, "obj-a", hardware, &manifest).await;
    let stored = service
        .backends
        .firmware_objects
        .find_by_id("obj-a")
        .await
        .unwrap();
    let stored_cache = firmware_object_cache_dir(&service.firmware_dir, "obj-a");
    let inline_cache = temp.path().join("inline-apply");

    // Stored and inline read the same manifest and land the same two files
    // in their own cache: neither consults the object store at dispatch.
    for (source, cache) in [
        (FirmwareSource::Stored(stored), stored_cache),
        (FirmwareSource::Inline(Ok(manifest)), inline_cache),
    ] {
        let apply = FirmwareApply {
            source,
            selections: gb200_all(),
            channel: String::new(),
            force_update: false,
            access_token: None,
            rack_id: "rack-1".to_owned(),
        };
        let prepared = service
            .prepare_firmware_apply(&apply, &cache)
            .await
            .unwrap();
        let bundles = &prepared.recovery[&NodeType::ComputeGb200Nvidia];
        assert_bundles_under(bundles, &cache, "a");
        // The recovery package never becomes a flash target: every HMC
        // target flashes the regular bundle.
        let targets = &prepared.targets[&NodeType::ComputeGb200Nvidia];
        assert!(!targets.is_empty());
        assert!(
            targets
                .iter()
                .all(|target| target.firmware_file.ends_with("a-hmc.fwpkg")),
            "{targets:?}"
        );
    }
}

#[tokio::test]
async fn manifest_without_a_recovery_package_prepares_no_recovery_bundles() {
    let temp = tempfile::tempdir().unwrap();
    let regular = temp.path().join("hmc.fwpkg");
    tokio::fs::write(&regular, b"regular").await.unwrap();
    let manifest: FirmwareManifest = serde_json::from_value(serde_json::json!({
        "ProductName": "GB200", "Milestones": [{"Name": "test", "BoardSKUs": [
            {"Type": "Compute Node", "Components": {"Firmware": [{
                "Component": "HMC", "Version": "1.0", "Type": "Prod",
                "Locations": [{"Location": regular, "LocationType": "file", "Type": "Firmware"}]
            }]}}
        ]}]
    }))
    .unwrap();
    let service = test_service();
    let apply = FirmwareApply {
        source: FirmwareSource::Inline(Ok(manifest)),
        selections: gb200_all(),
        channel: String::new(),
        force_update: false,
        access_token: None,
        rack_id: "rack-1".to_owned(),
    };
    let prepared = service
        .prepare_firmware_apply(&apply, &temp.path().join("cache"))
        .await
        .unwrap();
    assert!(prepared.recovery.is_empty());
    assert!(!prepared.targets[&NodeType::ComputeGb200Nvidia].is_empty());
}

#[tokio::test]
async fn objectless_resolution_takes_the_latest_object_and_skips_uncached_older_ones() {
    let temp = tempfile::tempdir().unwrap();
    let mut service = test_service();
    service.firmware_dir = temp.path().to_path_buf();
    let hardware = RackHardwareType("gb200-nvl72".to_owned());
    let manifest = recovery_manifest(temp.path(), "old").await;
    store_prepared_manifest(&service, "obj-old", hardware.clone(), &manifest).await;
    // An object prepared before recovery packages were catalogued: the
    // manifest names the package, the cache never received it.
    let old_cache = firmware_object_cache_dir(&service.firmware_dir, "obj-old");
    let mut walker = tokio::fs::read_dir(&old_cache).await.unwrap();
    while let Some(entry) = walker.next_entry().await.unwrap() {
        let file = entry.path().join("old-hmc_recovery.fwpkg");
        if file.is_file() {
            tokio::fs::remove_file(file).await.unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let manifest = recovery_manifest(temp.path(), "new").await;
    store_prepared_manifest(&service, "obj-new", hardware, &manifest).await;
    let store = service.backends.firmware_objects.as_ref();

    let latest = resolve_recovery_bundles_from_latest_object(
        store,
        &service.firmware_dir,
        NodeType::ComputeGb200Nvidia,
        false,
    )
    .await
    .unwrap();
    assert_bundles_under(
        &latest,
        &firmware_object_cache_dir(&service.firmware_dir, "obj-new"),
        "new",
    );
}

#[tokio::test]
async fn objectless_resolution_fails_closed_on_mixed_hardware_types() {
    let temp = tempfile::tempdir().unwrap();
    let mut service = test_service();
    service.firmware_dir = temp.path().to_path_buf();
    for (id, hardware) in [("obj-a", "gb200-nvl72"), ("obj-b", "gb300-nvl72")] {
        let manifest = recovery_manifest(temp.path(), id).await;
        store_prepared_manifest(
            &service,
            id,
            RackHardwareType(hardware.to_owned()),
            &manifest,
        )
        .await;
    }
    let store = service.backends.firmware_objects.as_ref();

    let error = resolve_recovery_bundles_from_latest_object(
        store,
        &service.firmware_dir,
        NodeType::ComputeGb200Nvidia,
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    // An apply launched from either object is pinned to it.
    for id in ["obj-a", "obj-b"] {
        let stored = store.find_by_id(id).await.unwrap();
        let cache = firmware_object_cache_dir(&service.firmware_dir, id);
        let apply = FirmwareApply {
            source: FirmwareSource::Stored(stored),
            selections: gb200_all(),
            channel: String::new(),
            force_update: false,
            access_token: None,
            rack_id: "rack-1".to_owned(),
        };
        let prepared = service
            .prepare_firmware_apply(&apply, &cache)
            .await
            .unwrap();
        assert_bundles_under(
            &prepared.recovery[&NodeType::ComputeGb200Nvidia],
            &cache,
            id,
        );
    }
}

#[tokio::test]
async fn uncatalogued_objects_carry_no_recovery_packages() {
    let temp = tempfile::tempdir().unwrap();
    let mut service = test_service();
    service.firmware_dir = temp.path().to_path_buf();
    store_uncatalogued_object(&service, "uncatalogued").await;
    let store = service.backends.firmware_objects.as_ref();

    let result = resolve_recovery_bundles_from_latest_object(
        store,
        &service.firmware_dir,
        NodeType::ComputeGb200Nvidia,
        false,
    )
    .await;
    // Skipped, not fatal: the scan reports that no object carries a package.
    let error = result.unwrap_err();
    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(
        error
            .message
            .contains("no available firmware object carries"),
        "{}",
        error.message
    );
}
