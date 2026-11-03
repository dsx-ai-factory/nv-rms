// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Configuration provenance for per-node firmware jobs, captured before execution.

use super::{HardwareUpdatePlan, NodeType, RackManagerServiceImpl, RmsJobHandle};

pub(super) fn trace_firmware_job(
    job: RmsJobHandle,
    node_type: Option<NodeType>,
    profile: Option<&HardwareUpdatePlan>,
) -> RmsJobHandle {
    let source = match (node_type, profile) {
        (_, Some(_)) => "profile",
        (Some(_), None) => "code_defaults",
        (None, None) => "unresolved",
    };
    let mut metadata = serde_json::Map::from_iter([
        ("hardware_config_source".into(), serde_json::json!(source)),
        (
            "node_type".into(),
            serde_json::json!(node_type.map(NodeType::as_str)),
        ),
        (
            "rms_build".into(),
            serde_json::json!({
                "git_sha": option_env!("VERGEN_GIT_SHA"),
                "git_describe": option_env!("VERGEN_GIT_DESCRIBE"),
            }),
        ),
    ]);
    if let Some(profile) = profile {
        metadata.extend([
            (
                "hardware_profile".into(),
                serde_json::json!(profile.profile_id()),
            ),
            (
                "hardware_profile_sha256".into(),
                serde_json::json!(profile.source_sha256()),
            ),
            (
                "hardware_profile_schema_version".into(),
                serde_json::json!(profile.schema_version()),
            ),
        ]);
    }
    tracing::info!(
        job_id = %job.id(),
        node_type = node_type.map(NodeType::as_str),
        hardware_config_source = source,
        hardware_profile = profile.map(HardwareUpdatePlan::profile_id),
        hardware_profile_sha256 = profile.map(HardwareUpdatePlan::source_sha256),
        hardware_profile_schema_version = profile.map(HardwareUpdatePlan::schema_version),
        rms_git_sha = option_env!("VERGEN_GIT_SHA"),
        rms_git_describe = option_env!("VERGEN_GIT_DESCRIBE"),
        "firmware job hardware configuration selected"
    );
    job.with_result_metadata(metadata)
}

impl RackManagerServiceImpl {
    pub(crate) fn trace_firmware_job(
        &self,
        job: RmsJobHandle,
        node_type: Option<NodeType>,
    ) -> RmsJobHandle {
        let profile =
            node_type.and_then(|node_type| self.hardware_profiles.resolve_request(node_type));
        trace_firmware_job(job, node_type, profile)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::job_lifecycle::JobState;
    use crate::orchestrator::job_tracker::{JobTracker, JobType};

    #[test]
    fn unresolved_platform_is_not_reported_as_code_defaults() {
        let tracker = JobTracker::new();
        let pending = tracker
            .create_job("rack", "node", JobType::FirmwareUpdate)
            .unwrap();
        let job_id = pending.id().to_string();
        drop(trace_firmware_job(pending, None, None));

        let job = tracker.get_job(&job_id).unwrap();
        assert_eq!(job.state, JobState::Failed);
        let result: serde_json::Value = serde_json::from_str(&job.result_json).unwrap();
        assert_eq!(result["hardware_config_source"], "unresolved");
        assert!(result["node_type"].is_null());
        assert!(result.get("hardware_profile_sha256").is_none());
    }
}
