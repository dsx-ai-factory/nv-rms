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

//! Types and constants shared by NVUE action endpoints.

use std::collections::BTreeMap;

use crate::uri::path_segment;
use crate::util::non_empty_trimmed;
use crate::{ClientError, JsonNumber, NVUE_V1_SERVER};

use serde::{Deserialize, Serialize};

/// OpenAPI action state: start.
pub const STATE_START: &str = "start";

/// OpenAPI action state: inactive.
pub const STATE_INACTIVE: &str = "inactive";

/// OpenAPI action state: action_success.
pub const STATE_ACTION_SUCCESS: &str = "action_success";

/// OpenAPI action state: action_error.
pub const STATE_ACTION_ERROR: &str = "action_error";

/// Compatibility action state observed from NVUE action polling.
pub const STATE_ACTION_RUNNING: &str = "action_running";

/// Compatibility action state observed from NVUE action polling.
pub const STATE_ACTION_FAILED: &str = "action_failed";

/// Endpoint for `GET /action/{action-job-id}`.
pub fn endpoint(action_job_id: &str) -> String {
    format!("{NVUE_V1_SERVER}/action/{}", path_segment(action_job_id))
}

/// Generic NVUE simple action body.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleAction<P> {
    /// Requested action state.
    pub state: String,

    /// Action parameters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<P>,
}

impl<P> SimpleAction<P> {
    /// Create an action with `state=start`.
    pub fn start(parameters: P) -> Self {
        Self {
            state: STATE_START.to_owned(),
            parameters: Some(parameters),
        }
    }

    /// Create an action with no parameter object.
    pub fn start_without_parameters() -> Self {
        Self {
            state: STATE_START.to_owned(),
            parameters: None,
        }
    }
}

/// Response shapes used by NVUE action-creation endpoints.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ActionIdResponse {
    /// String response body.
    Text(String),

    /// Numeric response body.
    Number(JsonNumber),

    /// Object response body.
    Object(ActionIdFields),
}

impl ActionIdResponse {
    /// Return the action ID if this response carries one.
    pub fn action_id(&self) -> Option<String> {
        match self {
            Self::Text(value) => non_empty_trimmed(value),
            Self::Number(value) => Some(value.as_str().to_owned()),
            Self::Object(fields) => fields.action_id(),
        }
    }
}

/// Object fields that may carry an NVUE action ID.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct ActionIdFields {
    /// Direct action ID field.
    #[serde(
        default,
        alias = "id",
        alias = "Id",
        alias = "action-id",
        alias = "actionId",
        alias = "job_id",
        alias = "JobId"
    )]
    pub action_id: Option<String>,

    /// OData-style URI field.
    #[serde(default, rename = "@odata.id")]
    pub odata_id: Option<String>,

    /// Link URI field.
    #[serde(default)]
    pub href: Option<String>,

    /// Location URI field.
    #[serde(default)]
    pub location: Option<String>,
}

impl ActionIdFields {
    /// Return the best action ID candidate.
    pub fn action_id(&self) -> Option<String> {
        self.action_id
            .as_deref()
            .and_then(non_empty_trimmed)
            .or_else(|| uri_tail(self.odata_id.as_deref()))
            .or_else(|| uri_tail(self.href.as_deref()))
            .or_else(|| uri_tail(self.location.as_deref()))
    }
}

/// Extract an action ID from a raw response body fallback.
pub fn action_id_from_body(body: &str) -> Option<String> {
    let body = body.trim().trim_matches('"');
    if body.is_empty()
        || body.starts_with('{')
        || body.starts_with('[')
        || matches!(body, "null" | "true" | "false")
    {
        None
    } else {
        Some(body.to_owned())
    }
}

/// Extract an NVUE action or job ID from a parsed response body.
pub fn extract_action_id(response: &serde_json::Value) -> Result<String, ClientError> {
    serde_json::from_value::<ActionIdResponse>(response.clone())
        .map_err(|_| ClientError::invalid_response("no action or job ID in NVUE response"))?
        .action_id()
        .ok_or_else(|| ClientError::invalid_response("no action or job ID in NVUE response"))
}

/// Parsed action status from `GET /action/{action-job-id}`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ActionStatus {
    /// Action state.
    #[serde(default)]
    pub state: Option<String>,

    /// Status text.
    #[serde(default)]
    pub status: Option<String>,

    /// Detail text.
    #[serde(default)]
    pub detail: Option<String>,

    /// Action issues.
    #[serde(default)]
    pub issue: Vec<ActionIssue>,

    /// OpenAPI field for action percentage.
    #[serde(default)]
    pub percentage: Option<ProgressValue>,

    /// Compatibility field observed in NVUE responses.
    #[serde(default)]
    pub percent: Option<ProgressValue>,

    /// Redfish-style compatibility field observed in some wrappers.
    #[serde(default, rename = "PercentComplete")]
    pub percent_complete: Option<ProgressValue>,

    /// Optional HTTP status from the action status object.
    #[serde(default, rename = "http-status", alias = "http_status")]
    pub http_status: Option<u16>,
}

/// Terminal classification for an NVUE action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionCompletion {
    /// The action is still running or has no terminal signal yet.
    Running,

    /// The action completed successfully.
    Succeeded,

    /// The action reached a failure state.
    Failed,
}

impl ActionStatus {
    /// Return the raw state, defaulting to the empty string.
    pub fn state(&self) -> &str {
        self.state.as_deref().unwrap_or_default()
    }

    /// Return the lowercase state label used by current NVUE callers.
    pub fn normalized_state(&self) -> String {
        self.state().to_ascii_lowercase()
    }

    /// Return the primary human status text.
    pub fn status_text(&self) -> &str {
        self.status
            .as_deref()
            .or(self.detail.as_deref())
            .unwrap_or_default()
    }

    /// Return detail text.
    pub fn detail_text(&self) -> &str {
        self.detail.as_deref().unwrap_or_default()
    }

    /// Return progress percent, defaulting to zero.
    pub fn percent(&self) -> i32 {
        self.percentage
            .as_ref()
            .or(self.percent.as_ref())
            .or(self.percent_complete.as_ref())
            .and_then(ProgressValue::as_i32)
            .unwrap_or_default()
    }

    /// Return a joined issue message.
    pub fn issue_message(&self) -> String {
        self.issue
            .iter()
            .filter_map(ActionIssue::message)
            .filter(|message| !message.is_empty())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Return true for terminal success states.
    pub fn is_success(&self) -> bool {
        self.completion() == ActionCompletion::Succeeded
    }

    /// Return true when the action should be considered failed.
    pub fn is_failed(&self) -> bool {
        self.completion() == ActionCompletion::Failed
    }

    /// Return the action completion classification.
    pub fn completion(&self) -> ActionCompletion {
        self.completion_with_policy(ActionCompletionPolicy::Strict)
    }

    /// Return completion for switch image install/uninstall compatibility flows.
    pub fn switch_image_completion(&self) -> ActionCompletion {
        self.completion_with_policy(ActionCompletionPolicy::SwitchImage)
    }

    /// Return completion for nvfwupd switch-firmware compatibility flows.
    pub fn switch_firmware_completion(&self) -> ActionCompletion {
        self.completion_with_policy(ActionCompletionPolicy::SwitchFirmware)
    }

    fn completion_with_policy(&self, policy: ActionCompletionPolicy) -> ActionCompletion {
        let state = self.normalized_state_with_policy(policy);
        if matches!(
            state.as_str(),
            "completed" | "complete" | "success" | STATE_ACTION_SUCCESS
        ) {
            return ActionCompletion::Succeeded;
        }
        if state == STATE_ACTION_RUNNING
            && policy.accepts_reboot_success()
            && self.indicates_reboot()
        {
            return ActionCompletion::Succeeded;
        }
        if state == STATE_ACTION_ERROR {
            return self.action_error_completion(policy);
        }
        if state.contains("failed") || (state.contains("error") && state != STATE_ACTION_ERROR) {
            return ActionCompletion::Failed;
        }
        if policy.fails_unexpected_state() && !policy.is_pending_state(&state) {
            return ActionCompletion::Failed;
        }
        ActionCompletion::Running
    }

    /// Return true when status/detail text indicates an expected reboot transition.
    pub fn indicates_reboot(&self) -> bool {
        text_indicates_reboot(self.status_text()) || text_indicates_reboot(self.detail_text())
    }

    /// Convert to a compact status view used by firmware polling code.
    pub fn poll_status(&self) -> ActionPollStatus {
        self.poll_status_with_policy(ActionCompletionPolicy::Strict)
    }

    /// Convert to a compact status view for switch image install/uninstall polling.
    pub fn switch_image_poll_status(&self) -> ActionPollStatus {
        self.poll_status_with_policy(ActionCompletionPolicy::SwitchImage)
    }

    /// Convert to a compact status view for nvfwupd switch-firmware polling.
    pub fn switch_firmware_poll_status(&self) -> ActionPollStatus {
        self.poll_status_with_policy(ActionCompletionPolicy::SwitchFirmware)
    }

    fn poll_status_with_policy(&self, policy: ActionCompletionPolicy) -> ActionPollStatus {
        self.poll_status_with_completion(
            self.normalized_state_with_policy(policy),
            self.completion_with_policy(policy),
        )
    }

    fn poll_status_with_completion(
        &self,
        state: String,
        completion: ActionCompletion,
    ) -> ActionPollStatus {
        let mut status = ActionPollStatus {
            state: state.clone(),
            status: self.status_text().to_owned(),
            percent: self.percent(),
            ..Default::default()
        };

        match completion {
            ActionCompletion::Running => {}
            ActionCompletion::Succeeded => {
                status.completed = true;
                status.percent = 100;

                if state == STATE_ACTION_ERROR || state == STATE_ACTION_RUNNING {
                    status.state = STATE_ACTION_SUCCESS.to_owned();
                }

                if state == STATE_ACTION_ERROR {
                    status.message = self.action_message();
                }
            }
            ActionCompletion::Failed => {
                status.completed = true;
                status.message = self.action_message();

                if status.message.is_empty() {
                    status.message = "firmware job failed".to_owned();
                }
            }
        }

        status
    }

    fn normalized_state_with_policy(&self, policy: ActionCompletionPolicy) -> String {
        self.state_for_policy(policy).to_ascii_lowercase()
    }

    fn state_for_policy(&self, policy: ActionCompletionPolicy) -> &str {
        self.state
            .as_deref()
            .or_else(|| {
                if policy.uses_status_as_state_fallback() {
                    self.status.as_deref()
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    fn action_error_completion(&self, policy: ActionCompletionPolicy) -> ActionCompletion {
        match policy {
            ActionCompletionPolicy::Strict => ActionCompletion::Failed,
            ActionCompletionPolicy::SwitchImage => {
                if self.action_error_indicates_success() {
                    ActionCompletion::Succeeded
                } else {
                    ActionCompletion::Failed
                }
            }
            ActionCompletionPolicy::SwitchFirmware => {
                if self.percent() >= 100 {
                    ActionCompletion::Succeeded
                } else if self.indicates_reboot() {
                    ActionCompletion::Running
                } else {
                    ActionCompletion::Failed
                }
            }
        }
    }

    fn action_error_indicates_success(&self) -> bool {
        self.percent() >= 100
            || self.indicates_reboot()
            || indicates_noop(&self.issue_message())
            || indicates_noop(self.status_text())
            || indicates_noop(self.detail_text())
    }

    fn action_message(&self) -> String {
        let issue_message = self.issue_message();

        if !issue_message.is_empty() {
            return issue_message;
        }

        if !self.detail_text().is_empty() {
            return self.detail_text().to_owned();
        }

        self.status_text().to_owned()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActionCompletionPolicy {
    Strict,
    SwitchImage,
    SwitchFirmware,
}

impl ActionCompletionPolicy {
    fn accepts_reboot_success(self) -> bool {
        self == Self::SwitchImage
    }

    fn uses_status_as_state_fallback(self) -> bool {
        self == Self::SwitchFirmware
    }

    fn fails_unexpected_state(self) -> bool {
        self == Self::SwitchFirmware
    }

    fn is_pending_state(self, state: &str) -> bool {
        matches!(
            state,
            STATE_INACTIVE | "running" | STATE_START | STATE_ACTION_RUNNING
        )
    }
}

/// Compact action status used by switch firmware pollers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActionPollStatus {
    /// Whether the action is terminal.
    pub completed: bool,

    /// Progress percentage.
    pub percent: i32,

    /// Normalized state.
    pub state: String,

    /// Status/detail text.
    pub status: String,

    /// Error or informational message.
    pub message: String,
}

/// Action issue shapes observed in NVUE action responses.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ActionIssue {
    /// Null issue entry.
    Null(()),

    /// Plain issue string.
    Text(String),

    /// Nested issue list.
    List(Vec<ActionIssue>),

    /// Structured issue object.
    Object(BTreeMap<String, ActionIssue>),

    /// Boolean issue entry.
    Bool(bool),

    /// Numeric issue entry.
    Number(JsonNumber),
}

impl ActionIssue {
    fn message(&self) -> Option<String> {
        match self {
            Self::Null(()) => None,
            Self::Text(message) => Some(message.clone()),
            Self::List(issues) => joined_issue_messages(issues),
            Self::Object(issue) => object_issue_message(issue),
            Self::Bool(_) | Self::Number(_) => None,
        }
    }

    fn text_message(&self) -> Option<String> {
        match self {
            Self::Text(message) => Some(message.clone()),
            _ => None,
        }
    }

    fn object_fields(&self) -> Option<&BTreeMap<String, ActionIssue>> {
        match self {
            Self::Object(fields) => Some(fields),
            _ => None,
        }
    }
}

/// Progress fields can appear as integers or strings.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ProgressValue {
    /// Integer percent.
    Integer(i64),

    /// String percent.
    Text(String),
}

impl ProgressValue {
    /// Return progress as an `i32` when it can be parsed.
    pub fn as_i32(&self) -> Option<i32> {
        let value = match self {
            Self::Integer(value) => *value,
            Self::Text(value) => value.trim().trim_end_matches('%').parse().ok()?,
        };

        i32::try_from(value).ok()
    }
}

fn uri_tail(value: Option<&str>) -> Option<String> {
    let value = value?.trim().trim_end_matches('/');

    if value.is_empty() {
        None
    } else {
        value.rsplit('/').next().and_then(non_empty_trimmed)
    }
}

fn text_indicates_reboot(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();

    lower.contains("power cycle")
        || lower.contains("reboot")
        || lower.contains("disconnecting from nvos")
        || lower.contains("offline during reboot")
        || lower.contains("system is offline")
}

fn indicates_noop(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("nothing to uninstall")
}

fn joined_issue_messages(issues: &[ActionIssue]) -> Option<String> {
    let message = issues
        .iter()
        .filter_map(ActionIssue::message)
        .filter(|message| !message.is_empty())
        .collect::<Vec<_>>()
        .join("; ");

    if message.is_empty() {
        None
    } else {
        Some(message)
    }
}

fn object_issue_message(issue: &BTreeMap<String, ActionIssue>) -> Option<String> {
    issue
        .get("message")
        .and_then(ActionIssue::text_message)
        .or_else(|| {
            issue
                .get("data")
                .and_then(ActionIssue::object_fields)
                .and_then(|data| data.get("msg"))
                .and_then(ActionIssue::text_message)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_action_status(input: &str) -> ActionStatus {
        serde_json::from_str(input).expect("action status should deserialize")
    }

    #[test]
    fn endpoint_uses_action_job_id_path() {
        assert_eq!(endpoint("job-1"), "/nvue_v1/action/job-1");
        assert_eq!(endpoint("job/1#next"), "/nvue_v1/action/job%2F1%23next");
    }

    #[test]
    fn action_id_response_reads_common_shapes() {
        let text: ActionIdResponse = serde_json::from_str(r#""job-1""#).unwrap();
        let number: ActionIdResponse = serde_json::from_str("42").unwrap();
        let large_number: ActionIdResponse = serde_json::from_str("18446744073709551615").unwrap();
        let id: ActionIdResponse = serde_json::from_str(r#"{"id":"job-2"}"#).unwrap();
        let location: ActionIdResponse =
            serde_json::from_str(r#"{"location":"/nvue_v1/action/job-3"}"#).unwrap();

        assert_eq!(text.action_id().as_deref(), Some("job-1"));
        assert_eq!(number.action_id().as_deref(), Some("42"));
        assert_eq!(
            large_number.action_id().as_deref(),
            Some("18446744073709551615")
        );

        assert_eq!(id.action_id().as_deref(), Some("job-2"));
        assert_eq!(location.action_id().as_deref(), Some("job-3"));
        assert_eq!(action_id_from_body(r#""job-4""#).as_deref(), Some("job-4"));
        assert_eq!(action_id_from_body("{}"), None);
    }

    #[test]
    fn extract_action_id_accepts_known_shapes_and_rejects_missing_ids() {
        let action = serde_json::json!({"action-id": "job-2"});
        let location = serde_json::json!({"location": "/nvue_v1/action/job-3"});
        let legacy = serde_json::json!({"job_id": "job-4"});

        assert_eq!(extract_action_id(&action).unwrap(), "job-2");
        assert_eq!(extract_action_id(&location).unwrap(), "job-3");
        assert_eq!(extract_action_id(&legacy).unwrap(), "job-4");

        for invalid in [
            serde_json::json!({}),
            serde_json::json!([]),
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(" "),
            serde_json::json!({"id": ""}),
        ] {
            assert!(extract_action_id(&invalid).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn action_id_from_body_rejects_json_literals() {
        let cases = [
            ("null", None),
            ("true", None),
            ("false", None),
            (r#""job-1""#, Some("job-1")),
        ];

        for (body, expected) in cases {
            assert_eq!(action_id_from_body(body).as_deref(), expected);
        }
    }

    #[test]
    fn action_status_normalizes_progress_and_issues_without_generic_success_override() {
        let status = decode_action_status(
            r#"{
                "state": "action_error",
                "percentage": "100%",
                "issue": [
                    {"data": {"msg": "Installing image"}},
                    "Nothing to uninstall",
                    ["Nested issue"],
                    {"data": "ignored compatibility field"},
                    null,
                    false,
                    1.5
                ]
            }"#,
        );

        assert_eq!(status.percent(), 100);
        assert_eq!(
            status.issue_message(),
            "Installing image; Nothing to uninstall; Nested issue"
        );

        assert!(!status.is_success());
        assert!(status.is_failed());
        assert_eq!(status.completion(), ActionCompletion::Failed);
        assert_eq!(
            status.switch_image_completion(),
            ActionCompletion::Succeeded
        );
    }

    #[test]
    fn switch_image_poll_status_preserves_noop_as_success() {
        let status = decode_action_status(
            r#"{
                "state": "action_error",
                "issue": [{"message": "Nothing to uninstall"}]
            }"#,
        )
        .switch_image_poll_status();

        assert_eq!(status.state, STATE_ACTION_SUCCESS);
        assert!(status.completed);
        assert_eq!(status.percent, 100);
    }

    #[test]
    fn switch_image_poll_status_marks_reboot_action_error_as_success() {
        let status = decode_action_status(
            r#"{
                "state": "action_error",
                "detail": "Disconnecting from NVOS during reboot"
            }"#,
        )
        .switch_image_poll_status();

        assert_eq!(status.state, STATE_ACTION_SUCCESS);
        assert!(status.completed);
        assert_eq!(status.percent, 100);
    }

    #[test]
    fn strict_poll_status_marks_action_error_as_failure() {
        let status = decode_action_status(
            r#"{
                "state": "action_error",
                "percent": 100,
                "issue": [{"message": "install failed after transfer"}]
            }"#,
        )
        .poll_status();

        assert_eq!(status.state, STATE_ACTION_ERROR);
        assert!(status.completed);
        assert_eq!(status.message, "install failed after transfer");
    }

    #[test]
    fn poll_status_preserves_status_only_text() {
        let status = decode_action_status(
            r#"{
                "state": "running",
                "status": "installing firmware",
                "percent": 42
            }"#,
        )
        .poll_status();

        assert_eq!(status.status, "installing firmware");
        assert_eq!(status.percent, 42);
        assert_eq!(status.state, "running");
    }

    #[test]
    fn switch_firmware_completion_reads_status_as_state_fallback() {
        let completed = decode_action_status(r#"{"status": "completed"}"#);
        let failed = decode_action_status(
            r#"{
                "status": "action_failed",
                "detail": "install failed"
            }"#,
        );

        assert_eq!(completed.completion(), ActionCompletion::Running);
        assert_eq!(
            completed.switch_firmware_completion(),
            ActionCompletion::Succeeded
        );
        assert_eq!(
            failed.switch_firmware_completion(),
            ActionCompletion::Failed
        );

        let poll = completed.switch_firmware_poll_status();

        assert!(poll.completed);
        assert_eq!(poll.state, "completed");
        assert_eq!(poll.percent, 100);
    }

    #[test]
    fn switch_firmware_completion_accepts_action_error_at_100_percent() {
        let status = decode_action_status(
            r#"{
                "state": "action_error",
                "percent": 100,
                "issue": [{"message": "install failed after transfer"}]
            }"#,
        );

        assert_eq!(status.completion(), ActionCompletion::Failed);
        assert_eq!(
            status.switch_firmware_completion(),
            ActionCompletion::Succeeded
        );

        let poll = status.switch_firmware_poll_status();

        assert!(poll.completed);
        assert_eq!(poll.state, STATE_ACTION_SUCCESS);
        assert_eq!(poll.percent, 100);
    }

    #[test]
    fn switch_firmware_completion_keeps_reboot_action_error_running() {
        let status = decode_action_status(
            r#"{
                "state": "action_error",
                "percent": 20,
                "status": "system is offline during reboot"
            }"#,
        );

        assert_eq!(
            status.switch_firmware_completion(),
            ActionCompletion::Running
        );

        assert!(!status.switch_firmware_poll_status().completed);
    }

    #[test]
    fn switch_firmware_completion_treats_inactive_as_pending() {
        let status = decode_action_status(r#"{"state": "inactive"}"#);

        assert_eq!(
            status.switch_firmware_completion(),
            ActionCompletion::Running
        );

        assert!(!status.switch_firmware_poll_status().completed);
    }

    #[test]
    fn action_error_under_100_without_reboot_is_failed() {
        let status = decode_action_status(
            r#"{
                "state": "action_error",
                "percent": 40,
                "detail": "install failed"
            }"#,
        );

        assert!(status.is_failed());
        assert!(!status.is_success());
        assert_eq!(status.completion(), ActionCompletion::Failed);
    }
}
