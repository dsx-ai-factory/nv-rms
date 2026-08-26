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

//! SDN factory-default reset submission and runtime convergence polling.

use std::time::Duration;

use super::super::system_image::contains_case_insensitive;
use super::super::{SwitchGb200Nvidia, extract_job_id};
use super::app::app_control_plane_state;
use crate::utilities::error::{Result, RmsError};

use nvue_client::DEFAULT_TIMEOUT as NVUE_DEFAULT_TIMEOUT;
use nvue_client::cluster::{
    NMX_CONTROLLER_APP_NAME, NMX_FACTORY_RESET_IN_PROGRESS_REASON, NmxControlPlaneState,
};
use nvue_client::sdn::{
    FACTORY_DEFAULT_ENDPOINT as SDN_FACTORY_DEFAULT_ENDPOINT, SdnFactoryDefaultResetRequest,
};
use serde_json::Value;

// SDN factory-default reset waits on runtime state, which can outlast the
// NVUE action response and should not share the shorter revision-apply limit.
const SDN_FACTORY_RESET_RUNTIME_TIMEOUT: Duration = Duration::from_secs(120);
pub(in crate::nodes::switch_gb200_nvidia) const SDN_FACTORY_RESET_UNCONFIGURED_CONFIRMATION_POLLS: u32 = 3;
#[cfg(test)]
const SDN_FACTORY_RESET_RUNTIME_POLL_DELAY: Duration = Duration::ZERO;
#[cfg(not(test))]
const SDN_FACTORY_RESET_RUNTIME_POLL_DELAY: Duration = Duration::from_secs(5);

impl SwitchGb200Nvidia {
    pub async fn reset_sdn_factory_default(&self) -> Result<String> {
        let payload = serde_json::to_value(SdnFactoryDefaultResetRequest::new()).map_err(|e| {
            RmsError::internal(format!(
                "failed to serialize SDN factory-default reset request: {e}"
            ))
        })?;

        // The submit response returns the NVUE action ID. Keep that ID in all
        // subsequent reset logs so operators can correlate RMS logs with
        // `/nvue_v1/action/<id>` on the switch.
        tracing::info!(
            node = %self.id,
            endpoint = SDN_FACTORY_DEFAULT_ENDPOINT,
            "submitting NVUE SDN factory-default reset action"
        );

        let response = self
            .nvue_client()?
            .post_json(SDN_FACTORY_DEFAULT_ENDPOINT, &payload, NVUE_DEFAULT_TIMEOUT)
            .await?;

        let action_id = extract_job_id(&response).map_err(|e| {
            tracing::warn!(
                node = %self.id,
                endpoint = SDN_FACTORY_DEFAULT_ENDPOINT,
                error = %e.message,
                response = %response,
                "NVUE SDN factory-default reset response missing action ID"
            );

            RmsError::internal(format!(
                "NVUE SDN reset response missing action ID: {}",
                e.message
            ))
        })?;

        tracing::info!(
            node = %self.id,
            action_id = %action_id,
            "NVUE SDN factory-default reset action accepted; waiting for action failure gate"
        );

        // The action endpoint is still useful as a failure gate. Non-primary
        // targets can accept the submit POST and then report action_error with
        // "cluster is not enabled". Action success is not treated as reset
        // completion; the runtime state below remains the completion gate.
        if let Err(e) = self.wait_for_nvue_action_completion(&action_id).await {
            tracing::warn!(
                node = %self.id,
                action_id = %action_id,
                error = %e.message,
                "NVUE SDN factory-default reset action failed"
            );

            if contains_case_insensitive(&e.message, "cluster is not enabled") {
                return Err(RmsError::failed_precondition(format!(
                    "SDN factory-default reset action {action_id} failed because cluster is not enabled; target switch may not be the primary switch"
                )));
            }

            return Err(e);
        }

        tracing::info!(
            node = %self.id,
            action_id = %action_id,
            "NVUE SDN factory-default reset action succeeded; waiting for runtime state"
        );

        // NVUE action success can arrive before the reset actually finishes.
        // Use nmx-controller runtime state as the completion source of truth.
        self.wait_for_sdn_factory_default_runtime_state(&action_id)
            .await?;

        tracing::info!(
            node = %self.id,
            action_id = %action_id,
            "NVUE SDN factory-default reset runtime state completed"
        );

        Ok(action_id)
    }

    async fn wait_for_sdn_factory_default_runtime_state(&self, action_id: &str) -> Result<()> {
        let deadline = std::time::Instant::now() + SDN_FACTORY_RESET_RUNTIME_TIMEOUT;

        let mut poll = 0_u32;
        let mut saw_reset_in_progress = false;
        let mut unconfigured_confirmation_polls = 0_u32;
        let mut last_observed = "nmx-controller state was not observed".to_owned();

        while std::time::Instant::now() < deadline {
            let app_status = match self.get_cluster_apps_status(NMX_CONTROLLER_APP_NAME).await {
                Ok(app_status) => app_status,
                Err(e) => {
                    last_observed = format!("failed to read nmx-controller state: {}", e.message);

                    tracing::warn!(
                        node = %self.id,
                        action_id,
                        poll,
                        error = %e.message,
                        "failed to read SDN factory-default runtime state"
                    );

                    poll += 1;
                    tokio::time::sleep(SDN_FACTORY_RESET_RUNTIME_POLL_DELAY).await;
                    continue;
                }
            };

            let control_plane_state = app_control_plane_state(&app_status).unwrap_or_default();

            let status = app_status
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or_default();

            let reason = app_status
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default();

            let app_missing = app_status.get("installed").and_then(Value::as_bool) == Some(false);

            let runtime_state_available =
                !status.is_empty() || !reason.is_empty() || !control_plane_state.trim().is_empty();

            if app_missing && saw_reset_in_progress {
                tracing::info!(
                    node = %self.id,
                    action_id,
                    poll,
                    response = %app_status,
                    saw_reset_in_progress,
                    "SDN factory-default runtime app is absent after observed reset progress"
                );

                return Ok(());
            }

            if app_missing {
                tracing::warn!(
                    node = %self.id,
                    action_id,
                    poll,
                    response = %app_status,
                    saw_reset_in_progress,
                    "SDN factory-default runtime app is absent without observed reset progress"
                );

                return Err(RmsError::failed_precondition(format!(
                    "nmx-controller runtime app is absent after SDN factory-default reset action {action_id}, but reset progress was not observed; target switch may not be the primary switch"
                )));
            }

            if !runtime_state_available {
                tracing::warn!(
                    node = %self.id,
                    action_id,
                    poll,
                    response = %app_status,
                    "SDN factory-default runtime state is unavailable"
                );

                return Err(RmsError::failed_precondition(format!(
                    "nmx-controller runtime state is unavailable after SDN factory-default reset action {action_id}; target switch may not be the primary switch"
                )));
            }

            // Runtime state is the source of truth. NVUE may decorate the
            // reason text, so any factory-reset-in-progress reason keeps the
            // job running even when the control-plane state is already mixed
            // or unconfigured.
            let reset_in_progress = reason
                .to_ascii_uppercase()
                .contains(NMX_FACTORY_RESET_IN_PROGRESS_REASON);

            if reset_in_progress {
                saw_reset_in_progress = true;
                unconfigured_confirmation_polls = 0;
            }

            let control_plane_unconfigured =
                NmxControlPlaneState::try_from(control_plane_state.trim())
                    == Ok(NmxControlPlaneState::Unconfigured);

            // The action endpoint can finish before runtime state settles.
            // FACTORY RESET IN PROGRESS is a blocking runtime state. If NVUE
            // never exposes that transition, require repeated UNCONFIGURED
            // observations so primary resets do not complete on a stale first
            // read immediately after action submit.
            if control_plane_unconfigured && !reset_in_progress {
                unconfigured_confirmation_polls += 1;

                if saw_reset_in_progress
                    || unconfigured_confirmation_polls
                        >= SDN_FACTORY_RESET_UNCONFIGURED_CONFIRMATION_POLLS
                {
                    tracing::info!(
                        node = %self.id,
                        action_id,
                        poll,
                        status,
                        reason,
                        control_plane_state,
                        reset_in_progress,
                        saw_reset_in_progress,
                        unconfigured_confirmation_polls,
                        "SDN factory-default runtime state reached unconfigured"
                    );

                    return Ok(());
                }
            } else {
                unconfigured_confirmation_polls = 0;
            }

            last_observed = format!(
                "status={status}, reason={reason}, control_plane_state={control_plane_state}"
            );

            tracing::info!(
                node = %self.id,
                action_id,
                poll,
                status,
                reason,
                control_plane_state,
                reset_in_progress,
                saw_reset_in_progress,
                unconfigured_confirmation_polls,
                "waiting for SDN factory-default runtime state"
            );

            poll += 1;
            tokio::time::sleep(SDN_FACTORY_RESET_RUNTIME_POLL_DELAY).await;
        }

        Err(RmsError::timeout(format!(
            "timed out waiting for SDN factory-default runtime state after NVUE action {action_id}: {last_observed}"
        )))
    }
}
