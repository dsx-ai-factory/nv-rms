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

//! Types and endpoint helpers for the NVUE `/revision` API area.

use std::collections::BTreeMap;

use crate::uri::{path_segment, query_value};
use crate::util::non_empty_trimmed;
use crate::{JsonNumber, NVUE_V1_SERVER};

use serde::{Deserialize, Serialize};

/// Revision state used to apply a pending revision.
pub const STATE_APPLY: &str = "apply";
/// Revision state used to save an applied revision.
pub const STATE_SAVE: &str = "save";
/// Revision state observed after successful apply.
pub const STATE_APPLIED: &str = "applied";
/// Failed revision state.
pub const STATE_FAILED: &str = "failed";
/// Error revision state.
pub const STATE_ERROR: &str = "error";
/// Invalid revision state.
pub const STATE_INVALID: &str = "invalid";
/// Automatic prompt answer for "Are you sure?".
pub const AUTO_PROMPT_AYS_YES: &str = "ays_yes";
/// Revision ID used by the applied-revision save endpoint.
pub const APPLIED_REVISION_ID: &str = "applied";

/// Full endpoint for `POST /revision`.
pub const REVISION_ENDPOINT: &str = "/nvue_v1/revision";

/// Empty JSON object request body for `POST /revision`.
///
/// Keep this as a braced struct: a unit struct serializes as JSON `null`, while
/// NVUE expects an empty object.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevisionCreate {}

/// Builds `/revision?base_rev={revision-id}` when creating a branch from a base.
pub fn revision_endpoint_with_base(base_revision_id: &str) -> String {
    format!(
        "{REVISION_ENDPOINT}?base_rev={}",
        query_value(base_revision_id)
    )
}

/// Builds the endpoint for `/revision/{revision-id}`.
pub fn revision_endpoint(revision_id: &str) -> String {
    format!("{NVUE_V1_SERVER}/revision/{}", path_segment(revision_id))
}

/// Endpoint used by NVUE for `/revision/applied`.
pub fn applied_revision_endpoint() -> String {
    revision_endpoint(APPLIED_REVISION_ID)
}

/// Request body for applying or saving a revision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevisionUpdate {
    /// Target revision state.
    pub state: String,

    /// Automatic prompt answers.
    #[serde(
        default,
        rename = "auto-prompt",
        skip_serializing_if = "Option::is_none"
    )]
    pub auto_prompt: Option<AutoPrompt>,
}

impl RevisionUpdate {
    /// Create an apply request with `ays_yes`.
    pub fn apply_with_yes_prompt() -> Self {
        Self {
            state: STATE_APPLY.to_owned(),
            auto_prompt: Some(AutoPrompt::ays_yes()),
        }
    }

    /// Create a save request with `ays_yes`.
    pub fn save_with_yes_prompt() -> Self {
        Self {
            state: STATE_SAVE.to_owned(),
            auto_prompt: Some(AutoPrompt::ays_yes()),
        }
    }
}

/// Revision automatic prompt answers.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutoPrompt {
    /// Automatic "Are you sure?" answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ays: Option<String>,

    /// Automatic "Ignore?" answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_fail: Option<String>,

    /// Automatic "Confirm?" answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm: Option<String>,
}

impl AutoPrompt {
    /// Create an `ays_yes` prompt response.
    pub fn ays_yes() -> Self {
        Self {
            ays: Some(AUTO_PROMPT_AYS_YES.to_owned()),
            ignore_fail: None,
            confirm: None,
        }
    }
}

/// Response shapes that can carry a revision ID.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RevisionIdResponse {
    /// String response body.
    Text(String),

    /// Numeric response body.
    Number(JsonNumber),

    /// Mapping keyed by revision ID.
    Revisions(BTreeMap<String, Revision>),

    /// Object with explicit ID fields.
    Fields(RevisionIdFields),
}

impl RevisionIdResponse {
    /// Return the revision ID if present.
    pub fn revision_id(&self) -> Option<String> {
        match self {
            Self::Text(value) => non_empty_trimmed(value),
            Self::Number(value) => Some(value.as_str().to_owned()),
            Self::Fields(fields) => fields.revision_id(),
            Self::Revisions(revisions) => {
                if revisions.len() != 1 {
                    return None;
                }
                revisions
                    .iter()
                    .next()
                    .filter(|(_, revision)| revision.looks_like_revision())
                    .map(|(revision_id, _)| revision_id.clone())
            }
        }
    }
}

/// Explicit revision ID object fields.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RevisionIdFields {
    /// Revision ID.
    #[serde(default, rename = "revision-id")]
    pub revision_id: Option<String>,

    /// Alternate ID field.
    #[serde(default)]
    pub id: Option<String>,
}

impl RevisionIdFields {
    /// Return the revision ID.
    pub fn revision_id(&self) -> Option<String> {
        self.revision_id
            .as_deref()
            .and_then(non_empty_trimmed)
            .or_else(|| self.id.as_deref().and_then(non_empty_trimmed))
    }
}

/// Response shape for `GET /revision/{revision-id}`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RevisionResponse {
    /// Mapping keyed by revision ID.
    Revisions(BTreeMap<String, Revision>),

    /// Flat revision object.
    Revision(Revision),
}

/// Apply-state summary for one NVUE revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionApplyStatus {
    /// Revision reached the applied state.
    Applied,

    /// NVUE reported an idempotent apply with no config diff.
    NoConfigDiff,

    /// Revision reached a terminal failure state.
    Failed {
        /// Terminal revision state.
        state: String,

        /// Compact issue summary, if NVUE provided one.
        issue: String,
    },

    /// Revision is still pending.
    Pending {
        /// Current non-terminal state, if NVUE provided one.
        state: String,
    },
}

impl RevisionResponse {
    /// Return a revision view for `revision_id`.
    pub fn revision(&self, revision_id: &str) -> Option<&Revision> {
        match self {
            Self::Revision(revision) => Some(revision),
            Self::Revisions(revisions) => revisions.get(revision_id),
        }
    }

    /// Return the revision state for `revision_id`.
    pub fn state(&self, revision_id: &str) -> Option<&str> {
        self.revision(revision_id)
            .and_then(|revision| revision.state.as_deref())
    }

    /// Return true when the revision is applied.
    pub fn is_applied(&self, revision_id: &str) -> bool {
        self.state(revision_id) == Some(STATE_APPLIED)
    }

    /// Return true when the revision is failed, errored, or invalid.
    pub fn is_failed(&self, revision_id: &str) -> bool {
        matches!(
            self.state(revision_id),
            Some(STATE_FAILED | STATE_ERROR | STATE_INVALID)
        )
    }

    /// Return true when the revision issue reports no config diff.
    pub fn has_no_config_diff(&self, revision_id: &str) -> bool {
        self.revision(revision_id)
            .and_then(Revision::issue)
            .is_some_and(|issue| issue.contains_text("config apply executed with no config diff"))
    }

    /// Return a compact issue summary.
    pub fn issue_summary(&self, revision_id: &str) -> String {
        self.revision(revision_id)
            .and_then(Revision::issue)
            .map(RevisionIssue::summary)
            .unwrap_or_default()
    }

    /// Return the apply status for `revision_id`.
    ///
    /// `NoConfigDiff` wins over terminal failure states because some NVUE builds
    /// report an idempotent apply as a revision issue on a failed-like response.
    pub fn apply_status(&self, revision_id: &str) -> RevisionApplyStatus {
        let state = self.state(revision_id).unwrap_or_default().to_owned();

        if self.is_applied(revision_id) {
            return RevisionApplyStatus::Applied;
        }

        if self.has_no_config_diff(revision_id) {
            return RevisionApplyStatus::NoConfigDiff;
        }

        if self.is_failed(revision_id) {
            return RevisionApplyStatus::Failed {
                state,
                issue: self.issue_summary(revision_id),
            };
        }

        RevisionApplyStatus::Pending { state }
    }
}

/// Revision object.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Revision {
    /// Revision state.
    #[serde(default)]
    pub state: Option<String>,

    /// Revision message.
    #[serde(default)]
    pub message: Option<String>,

    /// Revision transition state.
    #[serde(default)]
    pub transition: Option<RevisionTransition>,
}

impl Revision {
    fn looks_like_revision(&self) -> bool {
        self.state.is_some() || self.transition.is_some()
    }

    fn issue(&self) -> Option<&RevisionIssue> {
        self.transition.as_ref().and_then(|transition| {
            transition
                .issue
                .as_ref()
                .filter(|issue| !issue.is_empty_object())
        })
    }
}

/// Revision transition object.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RevisionTransition {
    /// Transition issue.
    #[serde(default)]
    pub issue: Option<RevisionIssue>,
}

/// Revision issue value.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RevisionIssue {
    /// Null issue.
    Null(()),

    /// String issue.
    Text(String),

    /// List issue.
    List(Vec<RevisionIssue>),

    /// Object issue.
    Object(BTreeMap<String, RevisionIssue>),

    /// Boolean issue.
    Bool(bool),

    /// Numeric issue.
    Number(JsonNumber),
}

impl RevisionIssue {
    /// Return true when any nested text contains `needle`.
    pub fn contains_text(&self, needle: &str) -> bool {
        match self {
            Self::Text(value) => value.contains(needle),
            Self::List(values) => values.iter().any(|value| value.contains_text(needle)),
            Self::Object(values) => values.values().any(|value| value.contains_text(needle)),
            Self::Null(()) | Self::Bool(_) | Self::Number(_) => false,
        }
    }

    /// Return a compact issue summary.
    pub fn summary(&self) -> String {
        match self {
            Self::Null(()) => String::new(),
            Self::Text(value) => value.clone(),
            Self::List(values) => values
                .iter()
                .map(RevisionIssue::summary)
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
                .join("; "),
            Self::Object(values) => values
                .iter()
                .map(|(key, value)| {
                    let value = value.summary();
                    if value.is_empty() {
                        key.clone()
                    } else {
                        format!("{key}: {value}")
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
            Self::Bool(value) => value.to_string(),
            Self::Number(value) => value.as_str().to_owned(),
        }
    }

    fn is_empty_object(&self) -> bool {
        matches!(self, Self::Object(values) if values.is_empty()) || matches!(self, Self::Null(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_use_nvue_v1_server() {
        assert_eq!(REVISION_ENDPOINT, "/nvue_v1/revision");
        assert_eq!(
            revision_endpoint_with_base("applied"),
            "/nvue_v1/revision?base_rev=applied"
        );
        assert_eq!(revision_endpoint("pending"), "/nvue_v1/revision/pending");
        assert_eq!(applied_revision_endpoint(), "/nvue_v1/revision/applied");
        assert_eq!(
            revision_endpoint_with_base("applied&candidate"),
            "/nvue_v1/revision?base_rev=applied%26candidate"
        );
        assert_eq!(
            revision_endpoint("pending/1"),
            "/nvue_v1/revision/pending%2F1"
        );
    }

    #[test]
    fn create_payload_is_empty_object() {
        let payload = serde_json::to_string(&RevisionCreate::default()).unwrap();

        assert_eq!(payload, "{}");
    }

    #[test]
    fn update_payloads_use_state_and_auto_prompt() {
        let apply = serde_json::to_string(&RevisionUpdate::apply_with_yes_prompt()).unwrap();
        let save = serde_json::to_string(&RevisionUpdate::save_with_yes_prompt()).unwrap();

        assert!(apply.contains(r#""state":"apply""#));
        assert!(apply.contains(r#""auto-prompt":{"ays":"ays_yes"}"#));
        assert!(save.contains(r#""state":"save""#));
    }

    #[test]
    fn revision_id_response_reads_common_shapes() {
        let text: RevisionIdResponse = serde_json::from_str(r#""rev-1""#).unwrap();
        let number: RevisionIdResponse = serde_json::from_str("12").unwrap();
        let field: RevisionIdResponse = serde_json::from_str(r#"{"revision-id":"rev-2"}"#).unwrap();
        let nested: RevisionIdResponse =
            serde_json::from_str(r#"{"rev-3":{"state":"pending"}}"#).unwrap();

        assert_eq!(text.revision_id().as_deref(), Some("rev-1"));
        assert_eq!(number.revision_id().as_deref(), Some("12"));
        assert_eq!(field.revision_id().as_deref(), Some("rev-2"));
        assert_eq!(nested.revision_id().as_deref(), Some("rev-3"));
    }

    #[test]
    fn revision_response_handles_flat_and_nested_shapes() {
        let flat: RevisionResponse = serde_json::from_str(r#"{"state":"applied"}"#).unwrap();
        let invalid: RevisionResponse = serde_json::from_str(r#"{"state":"invalid"}"#).unwrap();
        let pending: RevisionResponse = serde_json::from_str(r#"{"state":"pending"}"#).unwrap();

        let nested: RevisionResponse = serde_json::from_str(
            r#"{
                "rev-1": {
                    "state": "failed",
                    "transition": {
                        "issue": {
                            "msg": "config apply executed with no config diff",
                            "size": 18446744073709551615,
                            "ratio": 1.5
                        }
                    }
                }
            }"#,
        )
        .unwrap();

        assert!(flat.is_applied("ignored"));
        assert!(invalid.is_failed("ignored"));
        assert!(nested.is_failed("rev-1"));
        assert!(nested.has_no_config_diff("rev-1"));
        assert!(
            nested
                .issue_summary("rev-1")
                .contains("config apply executed with no config diff")
        );

        assert_eq!(flat.apply_status("ignored"), RevisionApplyStatus::Applied);

        assert_eq!(
            invalid.apply_status("ignored"),
            RevisionApplyStatus::Failed {
                state: "invalid".to_owned(),
                issue: String::new()
            }
        );

        assert_eq!(
            pending.apply_status("ignored"),
            RevisionApplyStatus::Pending {
                state: "pending".to_owned()
            }
        );

        assert_eq!(
            nested.apply_status("rev-1"),
            RevisionApplyStatus::NoConfigDiff
        );
    }
}
