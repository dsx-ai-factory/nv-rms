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

//! Bounded, process-local recovery of a VR LiteOn all-PSU upload.

use super::*;
use tokio::sync::Mutex as AsyncMutex;

// A PMC restart during a firmware update was observed to answer valid
// credentials with HTTP 401 for about 13 minutes before it recovered. Budget
// each outage generously; the fixed update deadline still bounds the total.
const PMC_OFFLINE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
// After a restart the PMC can report idle PSUs before it has re-read them.
// Require the same idle, incomplete state to persist before re-pushing.
const REPUSH_SETTLE_TIME: Duration = Duration::from_secs(300);
// No poll sleeps. Bound the complete read/validation step, and separately the
// re-push upload, as well as individual requests so a slow PMC cannot hold RMS
// inside a poll indefinitely.
#[cfg(not(test))]
const STEP_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(test)]
const STEP_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) struct LiteonAllPsuTask {
    pub(super) update_file: String,
    pub(super) model: String,
    state: AsyncMutex<RecoveryState>,
}

#[derive(Clone, Copy)]
enum Phase {
    PollTask,
    Validate,
}

struct RecoveryState {
    phase: Phase,
    task_id: String,
    deadline: Instant,
    offline_since: Option<Instant>,
    /// First idle observation of incomplete PSUs in the current stable stretch.
    idle_since: Option<Instant>,
    attempts: u32,

    last_task: Value,
    last_validation: Value,
    terminal: Option<Value>,
    /// The task that last kept recovery from re-pushing, for diagnostics.
    blocked_by: Option<String>,
    /// A re-push decided by the step, dispatched afterwards with its own budget.
    pending_push: Option<PendingPush>,
}

struct PendingPush {
    uri: String,
    /// Already recorded as the terminal result before the POST is sent.
    unknown_outcome: (bool, Value),
}

impl LiteonAllPsuTask {
    pub(super) fn new(update_file: &str, model: &str, task_id: &str, started: Instant) -> Self {
        Self {
            update_file: update_file.to_owned(),
            model: model.to_owned(),
            state: AsyncMutex::new(RecoveryState {
                phase: Phase::PollTask,
                task_id: task_id.to_owned(),
                deadline: started + LITEON_PSU_UPDATE_TIMEOUT,
                offline_since: None,
                idle_since: None,
                attempts: 0,

                last_task: json!({}),
                last_validation: json!({}),
                terminal: None,
                blocked_by: None,
                pending_push: None,
            }),
        }
    }
}

/// Whether any PSU reports a different version than in the previous read.
///
/// The first read has no predecessor and is never a change.
fn psu_versions_changed(previous: &Value, current: &Value) -> bool {
    fn versions(validation: &Value) -> std::collections::BTreeMap<String, String> {
        validation
            .get("devices")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|device| {
                let id = device.get("device_id").and_then(Value::as_str)?;
                let version = device.get("version").and_then(Value::as_str)?;
                Some((id.to_owned(), version.trim().to_owned()))
            })
            .collect()
    }
    let before = versions(previous);
    !before.is_empty() && before != versions(current)
}

impl RecoveryState {
    fn expired(&mut self, id: &str) -> Option<(bool, Value)> {
        if Instant::now() >= self.deadline {
            let message = match &self.blocked_by {
                Some(task) => format!(
                    "All-PSU update exceeded its one-hour deadline while PMC task {task} was still active"
                ),
                None => "All-PSU update exceeded its one-hour deadline".to_owned(),
            };
            return Some(self.fail(id, &message));
        }
        if self
            .offline_since
            .is_some_and(|since| since.elapsed() >= PMC_OFFLINE_TIMEOUT)
        {
            return Some(self.fail(
                id,
                &format!(
                    "PMC did not recover within {} minutes",
                    PMC_OFFLINE_TIMEOUT.as_secs() / 60
                ),
            ));
        }
        None
    }

    fn unavailable(&mut self, id: &str, started: Instant) -> (bool, Value) {
        self.offline_since.get_or_insert(started);
        self.idle_since = None;
        self.expired(id).unwrap_or_else(|| (true, self.running(id)))
    }

    fn annotate(&self, original_task_id: &str, mut details: Value) -> Value {
        if let Some(fields) = details.as_object_mut() {
            fields.insert("Id".to_owned(), json!(original_task_id));
            fields.insert("monitored_task_id".to_owned(), json!(self.task_id));
            fields.insert("repush_attempts".to_owned(), json!(self.attempts));
        }
        details
    }

    fn running(&self, original_task_id: &str) -> Value {
        self.annotate(
            original_task_id,
            json!({
                "TaskState": "Running",
                "TaskStatus": "OK",
                "stage": "liteon_psu_all_recovery",
                "PercentComplete": self.last_task.get("PercentComplete").cloned().unwrap_or(json!(0)),
                "Messages": self.last_task.get("Messages").cloned().unwrap_or(json!([])),
                "task": self.last_task,
                "devices": self.last_validation.get("devices").cloned().unwrap_or(json!([])),
                "mismatched": self.last_validation.get("mismatched").cloned().unwrap_or(json!([])),
                "blocked_by": self.blocked_by,
            }),
        )
    }

    fn finish(&mut self, original_task_id: &str, details: Value) -> (bool, Value) {
        let details = self.annotate(original_task_id, details);
        // Keep the terminal result under the original task ID: RMS may refetch
        // it after recovery switched to a different Redfish task ID.
        self.terminal = Some(details.clone());
        (true, details)
    }

    fn fail(&mut self, original_task_id: &str, message: &str) -> (bool, Value) {
        self.finish(
            original_task_id,
            json!({
                "TaskState": "Exception",
                "TaskStatus": "Critical",
                "stage": "liteon_psu_all_recovery",
                "Messages": [{"Message": message, "MessageSeverity": "Critical"}],
                "validation": self.last_validation,
            }),
        )
    }
}

impl PowerShelfRFTarget {
    /// Advance one recovery step, never sleeping or looping over update passes.
    pub(super) async fn poll_liteon_all_psu_recovery(
        &self,
        original_task_id: &str,
        task: &LiteonAllPsuTask,
    ) -> (bool, Value) {
        let Ok(mut state) = task.state.try_lock() else {
            return (
                true,
                json!({
                    "Id": original_task_id,
                    "TaskState": "Running",
                    "TaskStatus": "OK",
                    "stage": "liteon_psu_all_recovery",
                    "Messages": [{"Message": "Another poll is advancing this update"}],
                }),
            );
        };
        if let Some(terminal) = &state.terminal {
            return (true, terminal.clone());
        }
        if let Some(result) = state.expired(original_task_id) {
            return result;
        }
        let started = Instant::now();
        let offline_remaining = state.offline_since.map_or(STEP_TIMEOUT, |since| {
            PMC_OFFLINE_TIMEOUT.saturating_sub(since.elapsed())
        });
        let timeout = STEP_TIMEOUT
            .min(state.deadline.saturating_duration_since(started))
            .min(offline_remaining);
        let result = match tokio::time::timeout(
            timeout,
            self.liteon_recovery_step(original_task_id, task, &mut state),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                if let Some(terminal) = &state.terminal {
                    return (true, terminal.clone());
                }
                return state.unavailable(original_task_id, started);
            }
        };
        match state.pending_push.take() {
            Some(push) => {
                self.liteon_dispatch_repush(original_task_id, task, &mut state, push)
                    .await
            }
            None => result,
        }
    }

    // Transport errors, server unavailability and authentication rejections
    // share one outage budget across task, collection, inventory and
    // UpdateService reads. A restarting PMC answers valid credentials with 401,
    // so that is an outage, not a terminal error. Malformed data never
    // authorizes a write. Only a complete successful step clears the budget.
    async fn liteon_recovery_get(
        &self,
        uri: &str,
        id: &str,
        state: &mut RecoveryState,
        allow_missing: bool,
    ) -> Result<(u16, Value), (bool, Value)> {
        let started = Instant::now();
        let response = match self
            .bmc_access
            .dispatch_get_response(uri, STEP_TIMEOUT)
            .await
        {
            Ok(response) => response,
            Err(_) => return Err(state.unavailable(id, started)),
        };
        let status = response.status().as_u16();
        if status >= 500 || matches!(status, 401 | 403 | 408 | 429) {
            return Err(state.unavailable(id, started));
        }
        if status == 404 && allow_missing {
            return Ok((status, json!({})));
        }
        if status != 200 {
            return Err(state.fail(
                id,
                &format!("Recovery read returned HTTP {status}; no package was re-pushed"),
            ));
        }
        let body = match response.json::<Value>().await {
            Ok(body) if body.is_object() && body.get("error").is_none() => body,
            _ => {
                return Err(state.fail(
                    id,
                    "Recovery read returned malformed data; no package was re-pushed",
                ))
            }
        };
        Ok((status, body))
    }

    /// The task that still prevents a re-push, or `None` when every listed task
    /// has finished. The PMC keeps finished tasks until it restarts, so a
    /// retained `Completed` task must not block recovery. A listing that cannot
    /// be fully read, or a task whose state is unknown, blocks conservatively.
    async fn liteon_recovery_blocking_task(
        &self,
        id: &str,
        state: &mut RecoveryState,
    ) -> Result<Option<String>, (bool, Value)> {
        let (_, tasks) = self
            .liteon_recovery_get("/redfish/v1/TaskService/Tasks", id, state, false)
            .await?;
        let Some(members) = tasks.get("Members").and_then(Value::as_array) else {
            return Err(state.fail(
                id,
                "Task collection has no Members array; no package was re-pushed",
            ));
        };
        let truncated = tasks.get("Members@odata.nextLink").is_some()
            || tasks.get("@odata.nextLink").is_some()
            || tasks
                .get("Members@odata.count")
                .and_then(Value::as_u64)
                .is_some_and(|count| count != members.len() as u64);
        if truncated {
            return Ok(Some("(task list is paginated)".to_owned()));
        }
        for member in members {
            let uri = member.get("@odata.id").and_then(Value::as_str);
            let label = uri
                .and_then(|uri| uri.trim_end_matches('/').rsplit('/').next())
                .unwrap_or("(unidentified)")
                .to_owned();
            let task_state = match member.get("TaskState").and_then(Value::as_str) {
                Some(task_state) => Some(task_state.to_owned()),
                None => match uri {
                    Some(uri) => {
                        let (status, task) = self.liteon_recovery_get(uri, id, state, true).await?;
                        if status == 404 {
                            continue;
                        }
                        task.get("TaskState")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    }
                    None => None,
                },
            };
            let finished = task_state.as_deref().is_some_and(|task_state| {
                matches!(
                    task_state,
                    "Completed" | "Exception" | "Killed" | "Cancelled" | "Canceled" | "Failed"
                )
            });
            if !finished {
                return Ok(Some(label));
            }
        }
        Ok(None)
    }

    async fn liteon_recovery_validation(
        &self,
        id: &str,
        state: &mut RecoveryState,
        target: (u8, u8),
    ) -> Result<Value, (bool, Value)> {
        let (_, collection) = self
            .liteon_recovery_get(LITEON_POWERUNITS_URI, id, state, false)
            .await?;
        let members = collection.get("Members").and_then(Value::as_array);
        let ids: Vec<String> = members
            .into_iter()
            .flatten()
            .filter_map(|member| member.get("@odata.id").and_then(Value::as_str))
            .filter_map(Self::liteon_psu_device_id_from_member_uri)
            .collect();
        if members.is_none_or(|members| members.len() != 6)
            || ids.len() != 6
            || !(1..=6).all(|slot| ids.contains(&slot.to_string()))
            || collection.get("Members@odata.nextLink").is_some()
        {
            return Err(state.fail(
                id,
                "Cannot verify all six PSU inventory entries; no package was re-pushed",
            ));
        }
        let mut devices = Vec::new();
        for device_id in ids {
            let uri = format!(
                "{}/{}",
                LITEON_POWERUNITS_URI,
                Self::liteon_powerdevice_name(&device_id)
            );
            let (_, psu) = self.liteon_recovery_get(&uri, id, state, false).await?;
            devices.push(json!({
                "device_id": device_id,
                "version": psu.get("Version"),
                "state": psu.get("state"),
                "status": psu.get("status"),
                "updateProgress": psu.get("updateProgress"),
                "at_target": Self::liteon_psu_response_ready_at_target_version(&psu, target),
            }));
        }
        Ok(json!({
            "TaskState": "Completed", "TaskStatus": "OK", "PercentComplete": 100,
            "stage": "liteon_psu_all_validation", "devices": devices,
            "expected": Self::liteon_psu_target_version_label(target),
        }))
    }

    async fn liteon_recovery_step(
        &self,
        original_task_id: &str,
        task: &LiteonAllPsuTask,
        state: &mut RecoveryState,
    ) -> (bool, Value) {
        if matches!(state.phase, Phase::PollTask) {
            return self.liteon_poll_task_phase(original_task_id, state).await;
        }
        self.liteon_validate_phase(original_task_id, task, state)
            .await
    }

    /// `PollTask`: follow the tracked Redfish task until it ends or vanishes.
    async fn liteon_poll_task_phase(
        &self,
        original_task_id: &str,
        state: &mut RecoveryState,
    ) -> (bool, Value) {
        let (status, details) = match self
            .liteon_recovery_get(
                &self.get_task_service_uri(&state.task_id),
                original_task_id,
                state,
                true,
            )
            .await
        {
            Ok(response) => response,
            Err(result) => return result,
        };
        if status == 404 {
            match self
                .liteon_recovery_blocking_task(original_task_id, state)
                .await
            {
                Ok(None) => state.blocked_by = None,
                Ok(Some(blocker)) => {
                    state.blocked_by = Some(blocker);
                    state.offline_since = None;
                    state.idle_since = None;
                    return (true, state.running(original_task_id));
                }
                Err(result) => return result,
            }
        } else {
            let Some(task_state) = details.get("TaskState").and_then(Value::as_str) else {
                return state.fail(original_task_id, "Task response has no TaskState");
            };
            if matches!(task_state, "Killed" | "Cancelled" | "Canceled") {
                return state.finish(original_task_id, details);
            }
            let ended = matches!(
                task_state,
                "Completed" | "Exception" | "Failed" | "Interrupted"
            );
            state.last_task = details;
            state.offline_since = None;
            state.idle_since = None;
            if !ended {
                return (true, state.running(original_task_id));
            }
        }
        state.offline_since = None;
        state.phase = Phase::Validate;
        (true, state.running(original_task_id))
    }

    /// `Validate`: read all six PSUs and finish, keep waiting, or re-push.
    async fn liteon_validate_phase(
        &self,
        original_task_id: &str,
        task: &LiteonAllPsuTask,
        state: &mut RecoveryState,
    ) -> (bool, Value) {
        let Some(target_version) = Self::liteon_psu_package_target_version(&task.update_file)
        else {
            return state.fail(
                original_task_id,
                "Cannot determine the PSU package version; automatic re-push is disabled",
            );
        };
        let validation = match self
            .liteon_recovery_validation(original_task_id, state, target_version)
            .await
        {
            Ok(validation) => validation,
            Err(result) => return result,
        };
        // A version that changed since the previous read means a flash just
        // landed, so an idle report may still be stale: restart the settle clock.
        let versions_changed = psu_versions_changed(&state.last_validation, &validation);
        state.last_validation = validation.clone();
        if versions_changed {
            state.idle_since = None;
        }
        let Some(devices) = validation.get("devices").and_then(Value::as_array) else {
            // Enumeration failures have no devices. Never interpret an empty
            // mismatched array as progress or as permission to upload again.
            return state.finish(original_task_id, validation);
        };
        if devices.len() != 6
            || !["1", "2", "3", "4", "5", "6"].iter().all(|id| {
                devices
                    .iter()
                    .any(|device| device.get("device_id").and_then(Value::as_str) == Some(*id))
            })
        {
            return state.fail(
                original_task_id,
                "Cannot verify all six PSU inventory entries; no package was re-pushed",
            );
        }
        if devices.iter().any(|device| {
            Self::liteon_psu_status_is_failure(device.get("state").and_then(Value::as_str))
                || Self::liteon_psu_status_is_failure(device.get("status").and_then(Value::as_str))
        }) {
            return state.fail(
                original_task_id,
                "A PSU reports a failed state or status; no package was re-pushed",
            );
        }
        // A PSU whose fields cannot be read yet is not settled, which is not a
        // failure: wait within the outage budget and never upload meanwhile.
        if devices.iter().any(|device| {
            device.get("error").is_some()
                || device
                    .get("version")
                    .and_then(Value::as_str)
                    .and_then(Self::parse_liteon_psu_version_pair)
                    .is_none()
                || device
                    .get("updateProgress")
                    .and_then(Value::as_u64)
                    .is_none()
                || device
                    .get("state")
                    .and_then(Value::as_str)
                    .is_none_or(|value| value.trim().is_empty())
                || device
                    .get("status")
                    .and_then(Value::as_str)
                    .is_none_or(|value| value.trim().is_empty())
        }) {
            return state.unavailable(original_task_id, Instant::now());
        }
        let idle = devices
            .iter()
            .all(|device| device.get("updateProgress").and_then(Value::as_u64) == Some(0));
        let lagging = devices
            .iter()
            .filter(|device| device.get("at_target").and_then(Value::as_bool) != Some(true))
            .count();
        if idle && lagging == 0 {
            return state.finish(original_task_id, validation);
        }
        if !idle {
            state.offline_since = None;
            state.idle_since = None;
            state.phase = Phase::PollTask;
            return (true, state.running(original_task_id));
        }
        self.liteon_repush_package(original_task_id, task, state)
            .await
    }

    /// Upload the package again once the shelf has been idle, incomplete and
    /// task-free for the settling period. A re-push is never replayed: an
    /// indeterminate terminal result is recorded before the POST is sent.
    async fn liteon_repush_package(
        &self,
        original_task_id: &str,
        task: &LiteonAllPsuTask,
        state: &mut RecoveryState,
    ) -> (bool, Value) {
        let (status, service) = match self
            .liteon_recovery_get("/redfish/v1/UpdateService", original_task_id, state, false)
            .await
        {
            Ok(response) => response,
            Err(result) => return result,
        };
        // Recheck immediately before dispatch. A missing tracked ID is not
        // enough: any other task that has not finished (and any task list that
        // cannot be fully read) prohibits a re-push.
        match self
            .liteon_recovery_blocking_task(original_task_id, state)
            .await
        {
            Ok(None) => state.blocked_by = None,
            Ok(Some(blocker)) => {
                state.blocked_by = Some(blocker);
                state.offline_since = None;
                state.idle_since = None;
                state.phase = Phase::PollTask;
                return (true, state.running(original_task_id));
            }
            Err(result) => return result,
        }
        // The state must stay idle and incomplete for a settling period, with
        // no outage or task in between, before an upload is considered.
        let idle_since = *state.idle_since.get_or_insert_with(Instant::now);
        if idle_since.elapsed() < REPUSH_SETTLE_TIME {
            state.offline_since = None;
            state.phase = Phase::PollTask;
            return (true, state.running(original_task_id));
        }
        if let Some(result) = state.expired(original_task_id) {
            return result;
        }
        let Some(uri) = service
            .get("HttpPushUri")
            .and_then(Value::as_str)
            .filter(|uri| !uri.is_empty() && status == 200)
        else {
            return state.fail(
                original_task_id,
                "UpdateService did not advertise HttpPushUri; no package was re-pushed",
            );
        };
        state.attempts += 1;
        tracing::warn!(
            task_id = %NvUtils::sanitize_log(original_task_id),
            attempt = state.attempts,
            remaining_seconds = state.deadline.saturating_duration_since(Instant::now()).as_secs(),
            "All PMC tasks disappeared and incomplete PSUs are idle; re-pushing all-PSU package"
        );
        // Persist an indeterminate terminal result before dispatch. Cancellation,
        // timeout, or a response without a task ID must never replay this POST.
        let unknown_outcome = state.fail(
            original_task_id,
            "Re-push outcome is unknown; automatic recovery stopped to avoid a duplicate upload",
        );
        // The upload runs after this step, under its own budget, so the reads
        // above cannot consume the time the POST needs.
        state.pending_push = Some(PendingPush {
            uri: uri.to_owned(),
            unknown_outcome,
        });
        (true, state.running(original_task_id))
    }

    /// Send the decided re-push. The step's reads are already finished, so the
    /// POST gets its own full timeout, still capped by the job deadline.
    async fn liteon_dispatch_repush(
        &self,
        original_task_id: &str,
        task: &LiteonAllPsuTask,
        state: &mut RecoveryState,
        push: PendingPush,
    ) -> (bool, Value) {
        let PendingPush {
            uri,
            unknown_outcome,
        } = push;
        let budget = STEP_TIMEOUT.min(state.deadline.saturating_duration_since(Instant::now()));
        let Ok((accepted, response)) = tokio::time::timeout(
            budget,
            self.bmc_access.dispatch_file_upload(
                &uri,
                &task.update_file,
                STEP_TIMEOUT.as_secs(),
                None,
                true,
                None,
            ),
        )
        .await
        else {
            return unknown_outcome;
        };
        if !accepted {
            return unknown_outcome;
        }
        let Some(task_id) = task_id_from_update_response(&response).filter(|id| !id.is_empty())
        else {
            return unknown_outcome;
        };
        state.task_id = task_id;
        state.phase = Phase::PollTask;
        state.offline_since = None;
        state.idle_since = None;
        state.last_task = response;
        state.terminal = None;
        (true, state.running(original_task_id))
    }
}

#[cfg(test)]
mod tests;
