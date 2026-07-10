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

use super::ids::JobId;

/// Domain-specific policy for tracked job failures.
pub trait JobDomain: Send + Sync + 'static {
    /// Failure recorded when a tracked worker exits without sealing the job.
    fn dropped_failure() -> JobFailure;

    /// Failure recorded when the supervisor observes a task panic.
    fn panic_failure() -> JobFailure;

    /// Failure recorded when the supervisor observes an aborted task.
    fn cancelled_failure() -> JobFailure;

    /// Failure returned when the registry refuses a new job because it is at
    /// its retained-job limit. `current`/`max` describe the occupancy so the
    /// domain can build a useful message.
    fn at_capacity_failure(current: usize, max: usize) -> JobFailure;

    /// Failure returned when a node-exclusive create is refused because
    /// `active_job_id` already targets `(rack_id, node_id)`.
    fn node_busy_failure(rack_id: &str, node_id: &str, active_job_id: &JobId) -> JobFailure;
}

/// Failed-job payload. The typed domain error code is carried inside `error`
/// (as the root cause) so it can be recovered via `error.downcast_ref`.
#[derive(Debug)]
pub struct JobFailure {
    pub error: anyhow::Error,
    pub description: String,
    pub message: String,
    pub result_json: Option<String>,
}

impl JobFailure {
    /// Creates a failed-job payload with the default failed state description.
    pub fn new(error: impl Into<anyhow::Error>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            description: "Failed".to_owned(),
            message: message.into(),
            result_json: None,
        }
    }

    /// Sets the state description used for the failed job.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Sets the result JSON stored with the failed job.
    pub fn with_result_json(mut self, result_json: impl Into<String>) -> Self {
        self.result_json = Some(result_json.into());
        self
    }
}
