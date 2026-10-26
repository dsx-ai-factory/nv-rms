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

use crate::domain::node::FirmwareTaskStatus;
use crate::utilities::error::RmsError;

pub(crate) fn terminal_firmware_task_error(
    task_label: &str,
    task_id: &str,
    status: &FirmwareTaskStatus,
) -> Option<RmsError> {
    let state = status.state.to_ascii_lowercase();
    let task_status = status.status.to_ascii_lowercase();

    let failed = state == "failed"
        || state == "cancelled"
        || state == "canceled"
        || state == "exception"
        || state == "action_failed"
        || (state == "action_error" && status.percent < 100)
        || task_status == "failed"
        || task_status == "cancelled"
        || task_status == "canceled"
        || task_status == "critical"
        || (task_status == "warning" && warning_message_indicates_failure(&status.message));

    failed.then(|| {
        RmsError::internal(format!(
            "{task_label} {task_id} ended unsuccessfully: state='{}', status='{}', message='{}'",
            status.state, status.status, status.message
        ))
    })
}

fn warning_message_indicates_failure(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "resourceerrorsdetected",
        "detected errors",
        "completed with errors",
        "firmware update had errors",
        "no matching devices",
    ]
    .iter()
    .any(|signal| message.contains(signal))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_task_error_rejects_failed_terminal_states() {
        for status in [
            FirmwareTaskStatus {
                completed: true,
                state: "Failed".to_owned(),
                status: "Critical".to_owned(),
                message: "firmware update failed".to_owned(),
                ..Default::default()
            },
            FirmwareTaskStatus {
                completed: true,
                state: "Cancelled".to_owned(),
                status: "Cancelled".to_owned(),
                message: "cancelled by device".to_owned(),
                ..Default::default()
            },
            FirmwareTaskStatus {
                completed: true,
                state: "Exception".to_owned(),
                status: "Critical".to_owned(),
                message: "Redfish task exception".to_owned(),
                ..Default::default()
            },
            FirmwareTaskStatus {
                completed: true,
                state: "action_failed".to_owned(),
                status: "install failed".to_owned(),
                message: "NVUE action failed".to_owned(),
                ..Default::default()
            },
            FirmwareTaskStatus {
                completed: true,
                percent: 42,
                state: "action_error".to_owned(),
                status: "bad image".to_owned(),
                message: "bad image".to_owned(),
            },
        ] {
            let err = terminal_firmware_task_error("firmware task", "Task-Failed", &status)
                .expect("failed terminal state should return an error");

            assert!(
                err.message.contains("ended unsuccessfully"),
                "unexpected error: {}",
                err.message
            );
        }
    }

    #[test]
    fn terminal_task_error_accepts_action_error_at_100_percent() {
        let status = FirmwareTaskStatus {
            completed: true,
            percent: 100,
            state: "action_error".to_owned(),
            status: "rebooting to install image".to_owned(),
            message: "Installing image after reboot".to_owned(),
        };

        assert!(
            terminal_firmware_task_error("firmware task", "Task-Reboot-Handoff", &status).is_none(),
            "action_error at 100 percent should preserve switch reboot-handoff success"
        );
    }

    #[test]
    fn terminal_task_error_ignores_successful_diagnostic_message_text() {
        let status = FirmwareTaskStatus {
            completed: true,
            percent: 100,
            state: "Completed".to_owned(),
            status: "OK".to_owned(),
            message: "3 components updated; 2 skipped due to failed pre-checks".to_owned(),
        };

        assert!(
            terminal_firmware_task_error("firmware task", "Task-Completed", &status).is_none(),
            "completed OK task should not fail only because message contains failed"
        );
    }

    #[test]
    fn terminal_task_error_rejects_completed_warning_with_structured_failure() {
        let status = FirmwareTaskStatus {
            completed: true,
            percent: 100,
            state: "Completed".to_owned(),
            status: "Warning".to_owned(),
            message: "The resource property Firmware Update Service has detected errors of type 'No Matching Devices'. Resolution: Verify the FW package has devices that are listed in the Redfish FW Inventory".to_owned(),
        };

        let err = terminal_firmware_task_error("firmware task", "0", &status)
            .expect("structured warning should fail the task");

        assert!(
            err.message.contains("No Matching Devices"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn terminal_task_error_allows_benign_completed_warning() {
        let status = FirmwareTaskStatus {
            completed: true,
            percent: 100,
            state: "Completed".to_owned(),
            status: "Warning".to_owned(),
            message: "Completed with advisory warning only".to_owned(),
        };

        assert!(
            terminal_firmware_task_error("firmware task", "Task-Warning", &status).is_none(),
            "benign warning should not fail without a structured failure signal"
        );
    }
}
