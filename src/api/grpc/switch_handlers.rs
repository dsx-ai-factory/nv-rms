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
use std::sync::Arc;
use std::time::Duration;

use crate::api::grpc::firmware_handlers::resolve_firmware_file;
use crate::api::grpc::firmware_task_util::terminal_firmware_task_error;
use crate::api::grpc::node_recovery::{attempt_bmc_aux_powercycle_recovery, is_unreachable_err};
use crate::domain::node::{
    FirmwareTarget, FirmwareTaskStatus, FirmwareUpdateOptions, FirmwareUpdateOutcome, Node,
    PowerState,
};
use crate::nodes::switch_gb200_nvidia::config::MIN_FIRMWARE_FILE_SIZE;
use crate::nodes::{NodeInstance, SwitchFirmwareManagement};
use crate::racks::ManagedRack;
use crate::utilities::error::{Result, RmsError};
use librms::protos::rack_manager as rm;

use super::server::{RackManagerServiceImpl, find_rack};

/// Delay between successive polls of a BIOS update task.
const SWITCH_BIOS_UPDATE_POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Delay between successive polls of a firmware update task.
const SWITCH_FIRMWARE_UPDATE_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Timeout for a firmware update task.
const SWITCH_FIRMWARE_UPDATE_POLL_TIMEOUT: Duration = Duration::from_mins(30);

fn find_switch_node(
    rack: &ManagedRack,
    node_id: &str,
) -> std::result::Result<Arc<NodeInstance>, String> {
    let node = rack
        .find_node(node_id)
        .ok_or_else(|| format!("node {node_id} not found"))?;
    if node.as_switch_firmware().is_none() {
        return Err(format!("node {node_id} is not a switch"));
    }
    Ok(node)
}

async fn initialized_switch_firmware<'a>(
    service: &RackManagerServiceImpl,
    node: &'a NodeInstance,
    node_id: &str,
) -> std::result::Result<&'a dyn SwitchFirmwareManagement, String> {
    service
        .initialize_nvue_client(node.nvue_client(), None)
        .await
        .map_err(|error| error.message)?;

    node.as_switch_firmware()
        .ok_or_else(|| format!("node {node_id} is not a switch"))
}

fn switch_fw_component_to_string(component_type: i32) -> String {
    match rm::SwitchFirmwareComponentType::try_from(component_type) {
        Ok(rm::SwitchFirmwareComponentType::Bmc) => "BMC".into(),
        Ok(rm::SwitchFirmwareComponentType::Fpga) => "FPGA".into(),
        Ok(rm::SwitchFirmwareComponentType::Erot) => "EROT".into(),
        Ok(rm::SwitchFirmwareComponentType::Cpld) => "CPLD".into(),
        Ok(rm::SwitchFirmwareComponentType::Bios) => "BIOS".into(),
        Ok(rm::SwitchFirmwareComponentType::Transceiver) => "TRANSCEIVER".into(),
        _ => "UNKNOWN".into(),
    }
}

fn switch_fw_inventory_component_to_string(component_type: i32) -> String {
    match rm::SwitchFirmwareComponentType::try_from(component_type) {
        Ok(rm::SwitchFirmwareComponentType::Cpld) => "CPLD1".into(),
        _ => switch_fw_component_to_string(component_type),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SwitchUpgradeComponent {
    update_target: String,
    result_label: String,
    package_derived: bool,
}

fn switch_upgrade_component(
    component_type: i32,
) -> std::result::Result<SwitchUpgradeComponent, String> {
    match rm::SwitchFirmwareComponentType::try_from(component_type) {
        Ok(rm::SwitchFirmwareComponentType::Unknown) => Ok(SwitchUpgradeComponent {
            update_target: String::new(),
            result_label: "PACKAGE".to_owned(),
            package_derived: true,
        }),
        Ok(rm::SwitchFirmwareComponentType::Unspecified) => {
            Err("component_type is required".to_owned())
        }
        Ok(_) => {
            let component = switch_fw_component_to_string(component_type);
            Ok(SwitchUpgradeComponent {
                update_target: component.clone(),
                result_label: component,
                package_derived: false,
            })
        }
        Err(_) => Err("invalid component type".to_owned()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwitchUpgradeUpdateResult {
    Updated,
    Skipped,
}

#[derive(Debug)]
enum SwitchUpgradeUpdateError {
    Update(RmsError),
    Poll(RmsError),
}

async fn run_switch_upgrade_update<N: Node>(
    node: &N,
    target: &FirmwareTarget,
    force_update: bool,
    poll_timeout: Duration,
    poll_interval: Duration,
) -> std::result::Result<SwitchUpgradeUpdateResult, SwitchUpgradeUpdateError> {
    let outcome = node
        .update_firmware(target, force_update, FirmwareUpdateOptions::default())
        .await
        .map_err(SwitchUpgradeUpdateError::Update)?;

    match outcome {
        FirmwareUpdateOutcome::Started(handle) => {
            tracing::info!(node = %node.id(), task_id = %handle.task_id, "firmware update started");
            poll_node_firmware_task_until_complete(
                node,
                &handle.task_id,
                poll_timeout,
                poll_interval,
            )
            .await
            .map_err(SwitchUpgradeUpdateError::Poll)?;
            Ok(SwitchUpgradeUpdateResult::Updated)
        }
        FirmwareUpdateOutcome::Completed(_) => {
            tracing::info!(node = %node.id(), "firmware update completed");
            Ok(SwitchUpgradeUpdateResult::Updated)
        }
        FirmwareUpdateOutcome::Skipped { .. } => {
            tracing::info!(node = %node.id(), "firmware update skipped");
            Ok(SwitchUpgradeUpdateResult::Skipped)
        }
    }
}

async fn poll_node_firmware_task_until_complete<N: Node>(
    node: &N,
    task_id: &str,
    timeout: Duration,
    interval: Duration,
) -> Result<FirmwareTaskStatus> {
    if task_id.is_empty() {
        return Err(RmsError::internal(
            "switch firmware update returned an empty task id",
        ));
    }

    let deadline = tokio::time::Instant::now() + timeout;
    let mut iteration = 0;
    loop {
        iteration += 1;
        tracing::info!(node = %node.id(), task_id, iteration, interval = ?interval, timeout = ?timeout, "polling switch firmware task");
        match node.poll_firmware_task(task_id).await {
            Ok(status) if status.completed => {
                if let Some(error) =
                    terminal_firmware_task_error("switch firmware task", task_id, &status)
                {
                    return Err(error);
                }
                return Ok(status);
            }
            Ok(_) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(RmsError::internal(format!(
                        "polling switch firmware task {task_id} exceeded timeout of {}s",
                        timeout.as_secs()
                    )));
                }
                tokio::time::sleep(interval).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Returns a poll interval for a switch firmware update task based on the component type.
fn switch_firmware_update_poll_interval(component: &SwitchUpgradeComponent) -> Duration {
    if component.update_target.eq_ignore_ascii_case("bios") {
        SWITCH_BIOS_UPDATE_POLL_INTERVAL
    } else {
        SWITCH_FIRMWARE_UPDATE_POLL_INTERVAL
    }
}

/// Calls `node.activate_firmware()` and, if the switch's host (NVOS) is
/// unreachable, falls back to a BMC aux powercycle via Redfish.
async fn activate_firmware_with_bmc_fallback(
    node: &dyn Node,
) -> crate::utilities::error::Result<()> {
    match node.activate_firmware().await {
        Err(ref e) if is_unreachable_err(e) && node.supports_bmc_aux_powercycle() => {
            tracing::warn!(
                node = %node.id(),
                error = %e.message,
                "switch firmware activation via NVOS unreachable; falling back to BMC aux powercycle"
            );
            attempt_bmc_aux_powercycle_recovery(node).await
        }
        other => other,
    }
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_list_switch_firmware(
        &self,
        req: tonic::Request<rm::ListSwitchFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListSwitchFirmwareResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::ListSwitchFirmwareResponse {
            status: rm::ReturnCode::Failure.into(),
            result_json: String::new(),
            error_message: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.error_message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_switch_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let switch = match initialized_switch_firmware(self, node.as_ref(), &r.node_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };

        if !is_valid_component_type(r.component_type) {
            tracing::error!(rack = %r.rack_id, node = %r.node_id, "invalid component type: UNKNOWN is not allowed");
            resp.error_message = "invalid component type: UNKNOWN is not allowed".into();
            return Ok(tonic::Response::new(resp));
        }

        let component = switch_fw_inventory_component_to_string(r.component_type);

        match switch.list_firmware(&component, false).await {
            Ok(json) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.result_json = json.to_string();
            }
            Err(e) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %e.message, "list_firmware failed");
                resp.error_message = e.message;
            }
        }
        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_push_switch_firmware(
        &self,
        req: tonic::Request<rm::PushSwitchFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::PushSwitchFirmwareResponse>, tonic::Status> {
        let r = req.into_inner();
        let mut resp = rm::PushSwitchFirmwareResponse {
            status: rm::ReturnCode::Failure.into(),
            result_json: String::new(),
            error_message: String::new(),
        };

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.error_message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_switch_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let switch = match initialized_switch_firmware(self, node.as_ref(), &r.node_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };

        if !is_valid_component_type(r.component_type) {
            tracing::error!(rack = %r.rack_id, node = %r.node_id, "invalid component type: UNKNOWN is not allowed");
            resp.error_message = "invalid component type: UNKNOWN is not allowed".into();
            return Ok(tonic::Response::new(resp));
        }
        if !is_valid_filename(&r.filename) {
            tracing::error!(rack = %r.rack_id, node = %r.node_id, filename = %r.filename, "invalid firmware filename");
            resp.error_message = "invalid firmware filename".into();
            return Ok(tonic::Response::new(resp));
        }
        let local_file_path = match resolve_firmware_file(&r.local_file_path, &self.firmware_dir) {
            Ok(path) => path,
            Err(e) => {
                tracing::error!(
                    rack = %r.rack_id,
                    node = %r.node_id,
                    path = %r.local_file_path,
                    error = %e,
                    "invalid firmware file path"
                );
                resp.error_message = format!("invalid firmware file path: {e}");
                return Ok(tonic::Response::new(resp));
            }
        };

        if !file_exists_and_min_size(&local_file_path, MIN_FIRMWARE_FILE_SIZE) {
            tracing::error!(
                rack = %r.rack_id,
                node = %r.node_id,
                path = %local_file_path.display(),
                "firmware file not found or too small"
            );
            resp.error_message = format!(
                "firmware file not found or too small: {}",
                local_file_path.display()
            );
            return Ok(tonic::Response::new(resp));
        }
        let local_file_path = local_file_path.to_string_lossy().into_owned();

        let component = switch_fw_component_to_string(r.component_type);
        match switch
            .push_firmware_file(&local_file_path, &component, &r.filename)
            .await
        {
            Ok(()) => {
                resp.status = rm::ReturnCode::Success.into();
            }
            Err(e) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %e.message, "push_firmware_file failed");
                resp.error_message = e.message;
            }
        }
        Ok(tonic::Response::new(resp))
    }

    pub(crate) async fn handle_list_switch_system_images(
        &self,
        req: tonic::Request<rm::ListSwitchSystemImagesRequest>,
    ) -> std::result::Result<tonic::Response<rm::ListSwitchSystemImagesResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut resp = rm::ListSwitchSystemImagesResponse {
            status: rm::ReturnCode::Failure.into(),
            images_json: String::new(),
            error_message: String::new(),
        };
        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.error_message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_switch_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let switch = match initialized_switch_firmware(self, node.as_ref(), &r.node_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };

        match switch.list_system_images().await {
            Ok(json) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.images_json = json.to_string();
            }
            Err(e) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, error = %e.message, "list_system_images failed");
                resp.error_message = e.message;
            }
        }
        Ok(tonic::Response::new(resp))
    }

    // ── UpgradeSwitchFirmware (multi-stage workflow) ──

    pub(crate) async fn handle_upgrade_switch_firmware(
        &self,
        req: tonic::Request<rm::UpgradeSwitchFirmwareRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpgradeSwitchFirmwareResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut resp = rm::UpgradeSwitchFirmwareResponse {
            status: rm::ReturnCode::Failure.into(),
            failed_stage: rm::UpgradeStage::Unknown.into(),
            error_message: String::new(),
            current_filename: String::new(),
            new_filename: String::new(),
            result_json: String::new(),
        };

        let component = match switch_upgrade_component(r.component_type) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(rack = %r.rack_id, component = r.component_type, error = %e, "invalid switch upgrade component");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let filename = r.filename.clone();

        if r.rack_id.is_empty() || filename.is_empty() {
            tracing::error!(rack_id = %r.rack_id, filename = %filename, "rack_id and filename are required");
            resp.error_message = "rack_id and filename are required".into();
            return Ok(tonic::Response::new(resp));
        }
        let filename = match resolve_firmware_file(&filename, &self.firmware_dir) {
            Ok(path) => path,
            Err(e) => {
                tracing::error!(
                    rack = %r.rack_id,
                    path = %filename,
                    error = %e,
                    "invalid firmware file path"
                );
                resp.error_message = format!("invalid firmware file path: {e}");
                return Ok(tonic::Response::new(resp));
            }
        };
        if !file_exists_and_min_size(&filename, MIN_FIRMWARE_FILE_SIZE) {
            tracing::error!(rack = %r.rack_id, path = %filename.display(), "firmware file not found or too small");
            resp.error_message = format!(
                "firmware file not found or too small: {}",
                filename.display()
            );
            return Ok(tonic::Response::new(resp));
        }
        let filename = filename.to_string_lossy().into_owned();

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(rack_arc) => rack_arc,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.error_message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };

        let node_id = r.node_id.clone();
        let node = match find_switch_node(rack.as_ref(), &node_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(node = %node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let sw = match initialized_switch_firmware(self, node.as_ref(), &node_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(node = %node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };

        let switch_info = node.get_info();
        let nvswitch_host = switch_info.get("host").cloned().unwrap_or_default();

        let file_path = std::path::Path::new(&filename);
        let filename_only = file_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        if !is_valid_filename(&filename_only) {
            tracing::error!(rack = %r.rack_id, node = %node_id, filename = %filename_only, "invalid firmware filename");
            resp.error_message = format!("invalid firmware filename: {filename_only}");
            return Ok(tonic::Response::new(resp));
        }

        let total_start = std::time::Instant::now();
        let mut stage_timings: Vec<(&str, f64)> = Vec::new();

        let mut record_stage = |name: &'static str, start: std::time::Instant| -> f64 {
            let elapsed = start.elapsed().as_secs_f64();
            stage_timings.push((name, elapsed));
            elapsed
        };

        // Step 1: Check current firmware
        resp.failed_stage = rm::UpgradeStage::CheckCurrentVersion.into();
        let stage_start = std::time::Instant::now();
        let current_files = if component.package_derived {
            None
        } else {
            match sw.list_firmware(&component.update_target, true).await {
                Ok(f) => Some(f),
                Err(e) => {
                    tracing::warn!(
                        rack = %r.rack_id,
                        node = %node_id,
                        component = %component.update_target,
                        error = %e.message,
                        "list_firmware failed during upgrade check current version"
                    );
                    None
                }
            }
        };
        record_stage("check_current_version", stage_start);

        // Step 2: Delegate package parse, upload, install, and monitoring to NVFWUPD.
        resp.failed_stage = rm::UpgradeStage::PushFirmware.into();
        let stage_start = std::time::Instant::now();
        let target = FirmwareTarget {
            component: component.update_target.clone(),
            firmware_file: filename.clone(),
        };
        let update_result = match run_switch_upgrade_update(
            node.as_ref(),
            &target,
            // Switch firmware upgrades always force — legacy behavior; no proto field here.
            true,
            SWITCH_FIRMWARE_UPDATE_POLL_TIMEOUT,
            switch_firmware_update_poll_interval(&component),
        )
        .await
        {
            Ok(result) => result,
            Err(SwitchUpgradeUpdateError::Update(e)) => {
                record_stage("push_firmware", stage_start);
                tracing::error!(rack = %r.rack_id, node = %node_id, error = %e.message, "update_firmware failed");
                resp.error_message = e.message;
                resp.result_json = build_upgrade_result_json(
                    &component.result_label,
                    &filename_only,
                    &nvswitch_host,
                    &stage_timings,
                    total_start,
                    Some("push_firmware"),
                );
                return Ok(tonic::Response::new(resp));
            }
            Err(SwitchUpgradeUpdateError::Poll(e)) => {
                record_stage("push_firmware", stage_start);
                resp.failed_stage = rm::UpgradeStage::InstallFirmware.into();
                tracing::error!(rack = %r.rack_id, node = %node_id, error = %e.message, "firmware task polling failed");
                resp.error_message = e.message;
                resp.result_json = build_upgrade_result_json(
                    &component.result_label,
                    &filename_only,
                    &nvswitch_host,
                    &stage_timings,
                    total_start,
                    Some("install_firmware"),
                );
                return Ok(tonic::Response::new(resp));
            }
        };
        record_stage("push_firmware", stage_start);

        if update_result == SwitchUpgradeUpdateResult::Skipped {
            resp.current_filename = current_files.map(|f| f.to_string()).unwrap_or_default();
            resp.new_filename = filename_only.clone();
            resp.status = rm::ReturnCode::Success.into();
            resp.failed_stage = rm::UpgradeStage::Complete.into();
            resp.result_json = build_upgrade_result_json(
                &component.result_label,
                &filename_only,
                &nvswitch_host,
                &stage_timings,
                total_start,
                None,
            );
            return Ok(tonic::Response::new(resp));
        }

        // Step 3: Power cycle
        resp.failed_stage = rm::UpgradeStage::PowerCycle.into();
        let stage_start = std::time::Instant::now();
        if let Err(e) = activate_firmware_with_bmc_fallback(node.as_ref()).await {
            record_stage("power_cycle", stage_start);
            tracing::error!(rack = %r.rack_id, node = %node_id, error = %e.message, "activate_firmware failed");
            resp.error_message = e.message;
            resp.result_json = build_upgrade_result_json(
                &component.result_label,
                &filename_only,
                &nvswitch_host,
                &stage_timings,
                total_start,
                Some("power_cycle"),
            );
            return Ok(tonic::Response::new(resp));
        }
        record_stage("power_cycle", stage_start);

        // Step 4: Wait for health
        resp.failed_stage = rm::UpgradeStage::WaitForHealth.into();
        let stage_start = std::time::Instant::now();
        tokio::time::sleep(Duration::from_secs(30)).await;

        let mut healthy = false;
        for _ in 0..10 {
            if let Ok(PowerState::On) = node.get_power_state().await {
                healthy = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        record_stage("wait_for_health", stage_start);

        if !healthy {
            tracing::error!(rack = %r.rack_id, node = %node_id, "switch did not become healthy within timeout");
            resp.error_message = "switch did not become healthy within timeout".into();
            resp.result_json = build_upgrade_result_json(
                &component.result_label,
                &filename_only,
                &nvswitch_host,
                &stage_timings,
                total_start,
                Some("wait_for_health"),
            );
            return Ok(tonic::Response::new(resp));
        }

        // Step 5: Verify version
        resp.failed_stage = rm::UpgradeStage::VerifyVersion.into();
        let stage_start = std::time::Instant::now();
        if !component.package_derived {
            if let Err(e) = sw.list_firmware(&component.update_target, true).await {
                tracing::warn!(
                    rack = %r.rack_id,
                    node = %node_id,
                    component = %component.update_target,
                    error = %e.message,
                    "list_firmware failed during upgrade verify version"
                );
            }
        }
        record_stage("verify_version", stage_start);

        resp.current_filename = current_files.map(|f| f.to_string()).unwrap_or_default();
        resp.new_filename = filename_only.clone();
        resp.status = rm::ReturnCode::Success.into();
        resp.failed_stage = rm::UpgradeStage::Complete.into();
        resp.result_json = build_upgrade_result_json(
            &component.result_label,
            &filename_only,
            &nvswitch_host,
            &stage_timings,
            total_start,
            None,
        );
        Ok(tonic::Response::new(resp))
    }

    // ── PollSwitchFirmwareJobStatus (blocking poll for switch jobs) ──

    pub(crate) async fn handle_poll_switch_firmware_job_status(
        &self,
        req: tonic::Request<rm::PollSwitchFirmwareJobStatusRequest>,
    ) -> std::result::Result<tonic::Response<rm::PollSwitchFirmwareJobStatusResponse>, tonic::Status>
    {
        let r = req.into_inner();
        let mut resp = rm::PollSwitchFirmwareJobStatusResponse {
            status: rm::ReturnCode::Failure.into(),
            state: String::new(),
            result_json: String::new(),
            error_message: String::new(),
        };

        if r.job_id.is_empty() {
            tracing::error!(rack = %r.rack_id, node = %r.node_id, "job_id is required");
            resp.error_message = "job_id is required".into();
            return Ok(tonic::Response::new(resp));
        }

        let rack = match find_rack(&self.rack_manager, &r.rack_id) {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(rack = %r.rack_id, "rack not found");
                resp.error_message = "rack not found".into();
                return Ok(tonic::Response::new(resp));
            }
        };
        let node = match find_switch_node(rack.as_ref(), &r.node_id) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };
        let sw = match initialized_switch_firmware(self, node.as_ref(), &r.node_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(node = %r.node_id, rack = %r.rack_id, error = %e, "switch node not found");
                resp.error_message = e;
                return Ok(tonic::Response::new(resp));
            }
        };

        let timeout = if r.timeout_seconds > 0 {
            r.timeout_seconds as u64
        } else {
            1800
        };
        let interval = if r.poll_interval_seconds > 0 {
            r.poll_interval_seconds as u64
        } else {
            5
        };

        match sw
            .poll_job_blocking(
                &r.job_id,
                Duration::from_secs(timeout),
                Duration::from_secs(interval),
            )
            .await
        {
            Ok(true) => {
                resp.status = rm::ReturnCode::Success.into();
                resp.state = "action_success".into();
            }
            Ok(false) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, job_id = %r.job_id, "poll_job_blocking reported job failed");
                resp.state = "action_failed".into();
            }
            Err(e) => {
                tracing::error!(rack = %r.rack_id, node = %r.node_id, job_id = %r.job_id, error = %e.message, "poll_job_blocking failed");
                resp.state = "action_failed".into();
                resp.error_message = e.message;
            }
        }
        Ok(tonic::Response::new(resp))
    }
}

// ── Validation helpers ──

fn is_valid_component_type(component_type: i32) -> bool {
    let Ok(component_type) = rm::SwitchFirmwareComponentType::try_from(component_type) else {
        return false;
    };

    !matches!(
        component_type,
        rm::SwitchFirmwareComponentType::Unspecified | rm::SwitchFirmwareComponentType::Unknown
    )
}

fn is_valid_filename(name: &str) -> bool {
    if name.is_empty() || name.starts_with('.') {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

fn file_exists_and_min_size(path: &Path, min_size: u64) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.len() >= min_size)
        .unwrap_or(false)
}

fn build_upgrade_result_json(
    component: &str,
    filename: &str,
    nvswitch_host: &str,
    stage_timings: &[(&str, f64)],
    total_start: std::time::Instant,
    failed_stage_name: Option<&str>,
) -> String {
    let mut stages = serde_json::Map::new();
    for (name, elapsed) in stage_timings {
        let status = if failed_stage_name == Some(*name) {
            "failed"
        } else {
            "completed"
        };
        stages.insert(
            name.to_string(),
            serde_json::json!({
                "status": status,
                "time_taken_seconds": elapsed,
            }),
        );
    }

    serde_json::json!({
        "component": component,
        "firmware_file": filename,
        "nvswitch_host": nvswitch_host,
        "timing_summary": {
            "stages": stages,
            "total_time_seconds": total_start.elapsed().as_secs_f64(),
        }
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::domain::node::{
        FirmwareTaskHandle, FirmwareTaskStatus, FirmwareUpdateSummary, NodeKind, PowerOp,
        PowerTargetType,
    };

    struct TestSwitchNode {
        outcome: Mutex<Option<FirmwareUpdateOutcome>>,
        last_target: Mutex<Option<FirmwareTarget>>,
        task_status: FirmwareTaskStatus,
        update_calls: AtomicUsize,
        start_upload_calls: AtomicUsize,
        poll_calls: AtomicUsize,
    }

    impl TestSwitchNode {
        fn new(outcome: FirmwareUpdateOutcome) -> Self {
            Self {
                outcome: Mutex::new(Some(outcome)),
                last_target: Mutex::new(None),
                task_status: FirmwareTaskStatus {
                    completed: true,
                    percent: 100,
                    state: "Completed".to_owned(),
                    status: "OK".to_owned(),
                    message: "done".to_owned(),
                },
                update_calls: AtomicUsize::new(0),
                start_upload_calls: AtomicUsize::new(0),
                poll_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Node for TestSwitchNode {
        fn id(&self) -> &str {
            "switch-01"
        }

        fn rack_id(&self) -> &str {
            "rack-01"
        }

        fn node_type(&self) -> crate::domain::node::NodeType {
            crate::domain::node::NodeType::SwitchGb200Nvidia
        }

        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        async fn set_power_state(&self, _op: PowerOp, _target: PowerTargetType) -> Result<()> {
            Ok(())
        }

        async fn update_firmware(
            &self,
            target: &FirmwareTarget,
            _force_update: bool,
            _options: FirmwareUpdateOptions,
        ) -> Result<FirmwareUpdateOutcome> {
            self.update_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_target.lock().unwrap() = Some(target.clone());
            Ok(self
                .outcome
                .lock()
                .unwrap()
                .take()
                .expect("update_firmware called more than once"))
        }

        async fn start_firmware_upload(
            &self,
            _target: &FirmwareTarget,
            _force_update: bool,
        ) -> Result<String> {
            self.start_upload_calls.fetch_add(1, Ordering::SeqCst);
            Ok("legacy-task".to_owned())
        }

        async fn poll_firmware_task(&self, _task_id: &str) -> Result<FirmwareTaskStatus> {
            self.poll_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.task_status.clone())
        }
    }

    fn target() -> FirmwareTarget {
        FirmwareTarget {
            component: "CPLD".to_owned(),
            firmware_file: "/tmp/switch.fwpkg".to_owned(),
        }
    }

    #[test]
    fn switch_upgrade_unknown_component_means_package_derived_targets() {
        let component = switch_upgrade_component(rm::SwitchFirmwareComponentType::Unknown as i32)
            .expect("UNKNOWN should be accepted for package-derived switch updates");

        assert_eq!(component.update_target, "");
        assert_eq!(component.result_label, "PACKAGE");
        assert!(component.package_derived);
    }

    #[test]
    fn switch_upgrade_rejects_invalid_component_values() {
        let err = switch_upgrade_component(999).unwrap_err();
        assert_eq!(err, "invalid component type");
    }

    #[tokio::test]
    async fn switch_upgrade_update_allows_empty_target_for_package_derivation() {
        let node = TestSwitchNode::new(FirmwareUpdateOutcome::Completed(FirmwareUpdateSummary {
            message: "completed internally".to_owned(),
            task_ids: Vec::new(),
            details: json!({}),
        }));
        let target = FirmwareTarget {
            component: String::new(),
            firmware_file: "/tmp/switch.fwpkg".to_owned(),
        };

        let result = run_switch_upgrade_update(
            &node,
            &target,
            true,
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .await
        .expect("package-derived target update should succeed");

        assert_eq!(result, SwitchUpgradeUpdateResult::Updated);
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            node.last_target.lock().unwrap().as_ref().unwrap().component,
            ""
        );
    }

    #[tokio::test]
    async fn switch_upgrade_update_uses_nvfwupd_node_update_for_completed_outcome() {
        let node = TestSwitchNode::new(FirmwareUpdateOutcome::Completed(FirmwareUpdateSummary {
            message: "completed internally".to_owned(),
            task_ids: Vec::new(),
            details: json!({}),
        }));

        let result = run_switch_upgrade_update(
            &node,
            &target(),
            true,
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .await
        .expect("completed update should succeed");

        assert_eq!(result, SwitchUpgradeUpdateResult::Updated);
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.start_upload_calls.load(Ordering::SeqCst), 0);
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn switch_upgrade_update_skipped_outcome_does_not_poll() {
        let node = TestSwitchNode::new(FirmwareUpdateOutcome::Skipped {
            reason: "already current".to_owned(),
        });

        let result = run_switch_upgrade_update(
            &node,
            &target(),
            true,
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .await
        .expect("skipped update should succeed");

        assert_eq!(result, SwitchUpgradeUpdateResult::Skipped);
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.start_upload_calls.load(Ordering::SeqCst), 0);
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn switch_upgrade_update_started_outcome_polls_node_task() {
        let node = TestSwitchNode::new(FirmwareUpdateOutcome::Started(FirmwareTaskHandle {
            task_id: "Task-1".to_owned(),
        }));

        let result = run_switch_upgrade_update(
            &node,
            &target(),
            true,
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .await
        .expect("started update should poll and succeed");

        assert_eq!(result, SwitchUpgradeUpdateResult::Updated);
        assert_eq!(node.update_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.start_upload_calls.load(Ordering::SeqCst), 0);
        assert_eq!(node.poll_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn switch_terminal_task_error_ignores_successful_diagnostic_message_text() {
        let status = FirmwareTaskStatus {
            completed: true,
            percent: 100,
            state: "Completed".to_owned(),
            status: "OK".to_owned(),
            message: "Validation failed but update was forced".to_owned(),
        };

        assert!(
            terminal_firmware_task_error(
                "switch firmware task",
                "Task-Completed-With-Diagnostics",
                &status
            )
            .is_none(),
            "completed OK switch task should not fail only because message contains failed"
        );
    }

    #[test]
    fn switch_firmware_update_poll_interval_returns_correct_interval_for_components() {
        let component = SwitchUpgradeComponent {
            update_target: "BIOS".to_owned(),
            result_label: "BIOS".to_owned(),
            package_derived: false,
        };
        assert_eq!(
            switch_firmware_update_poll_interval(&component),
            SWITCH_BIOS_UPDATE_POLL_INTERVAL
        );

        let component = SwitchUpgradeComponent {
            update_target: "FPGA".to_owned(),
            result_label: "FPGA".to_owned(),
            package_derived: false,
        };
        assert_eq!(
            switch_firmware_update_poll_interval(&component),
            SWITCH_FIRMWARE_UPDATE_POLL_INTERVAL
        );
    }

    // ── activate_firmware_with_bmc_fallback ───────────────────────────────────

    use crate::utilities::error::ErrorCode;
    use std::collections::VecDeque;

    use crate::domain::node::PowerState;

    struct ActivationMockNode {
        node_type: crate::domain::node::NodeType,
        /// Errors returned by `activate_firmware`; empty queue → `Ok(())`.
        activation_errors: Mutex<VecDeque<RmsError>>,
        /// Errors returned by `bmc_aux_powercycle`; empty queue → `Ok(())`.
        powercycle_errors: Mutex<VecDeque<RmsError>>,
        /// Power states returned by `get_power_state`; empty queue → `power_state_default`.
        power_states: Mutex<VecDeque<PowerState>>,
        /// Returned by `get_power_state` once the `power_states` queue is exhausted.
        power_state_default: PowerState,
        activation_calls: AtomicUsize,
        powercycle_calls: AtomicUsize,
    }

    impl ActivationMockNode {
        fn switch_ok() -> Self {
            Self {
                node_type: crate::domain::node::NodeType::SwitchGb200Nvidia,
                activation_errors: Mutex::new(VecDeque::new()),
                powercycle_errors: Mutex::new(VecDeque::new()),
                power_states: Mutex::new(VecDeque::new()),
                power_state_default: PowerState::On,
                activation_calls: AtomicUsize::new(0),
                powercycle_calls: AtomicUsize::new(0),
            }
        }

        fn with_activation_error(mut self, code: ErrorCode, msg: &str) -> Self {
            self.activation_errors
                .get_mut()
                .unwrap()
                .push_back(RmsError::new(code, msg));
            self
        }

        fn with_powercycle_error(mut self, code: ErrorCode, msg: &str) -> Self {
            self.powercycle_errors
                .get_mut()
                .unwrap()
                .push_back(RmsError::new(code, msg));
            self
        }
    }

    #[async_trait]
    impl Node for ActivationMockNode {
        fn id(&self) -> &str {
            "activation-mock"
        }
        fn rack_id(&self) -> &str {
            "rack-01"
        }
        fn node_type(&self) -> crate::domain::node::NodeType {
            self.node_type
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        fn supports_bmc_aux_powercycle(&self) -> bool {
            self.node_type.kind() == NodeKind::Switch
        }

        async fn activate_firmware(&self) -> crate::utilities::error::Result<()> {
            self.activation_calls.fetch_add(1, Ordering::SeqCst);
            match self.activation_errors.lock().unwrap().pop_front() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }

        async fn bmc_aux_powercycle(&self) -> crate::utilities::error::Result<()> {
            self.powercycle_calls.fetch_add(1, Ordering::SeqCst);
            match self.powercycle_errors.lock().unwrap().pop_front() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }

        async fn get_power_state(&self) -> crate::utilities::error::Result<PowerState> {
            Ok(self
                .power_states
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(self.power_state_default))
        }
    }

    #[tokio::test]
    async fn activation_bmc_fallback_passes_through_success() {
        let node = ActivationMockNode::switch_ok();
        activate_firmware_with_bmc_fallback(&node).await.unwrap();
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn activation_bmc_fallback_falls_back_when_nvos_unreachable() {
        for code in [
            ErrorCode::ConnectionRefused,
            ErrorCode::Timeout,
            ErrorCode::Unavailable,
        ] {
            let node = ActivationMockNode::switch_ok().with_activation_error(code, "NVOS down");
            activate_firmware_with_bmc_fallback(&node)
                .await
                .unwrap_or_else(|e| panic!("expected Ok for {code:?}, got: {}", e.message));
            assert_eq!(
                node.activation_calls.load(Ordering::SeqCst),
                1,
                "activation called once for {code:?}"
            );
            assert_eq!(
                node.powercycle_calls.load(Ordering::SeqCst),
                1,
                "BMC powercycle used as fallback for {code:?}"
            );
        }
    }

    #[tokio::test]
    async fn activation_bmc_fallback_does_not_fall_back_for_non_unreachable_error() {
        let node =
            ActivationMockNode::switch_ok().with_activation_error(ErrorCode::Internal, "task fail");
        let err = activate_firmware_with_bmc_fallback(&node)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn activation_bmc_fallback_propagates_bmc_error() {
        let node = ActivationMockNode::switch_ok()
            .with_activation_error(ErrorCode::ConnectionRefused, "NVOS down")
            .with_powercycle_error(ErrorCode::FailedPrecondition, "no BMC endpoint");
        let err = activate_firmware_with_bmc_fallback(&node)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::FailedPrecondition);
        assert!(err.message.contains("no BMC endpoint"));
        assert_eq!(node.activation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
    }
}
