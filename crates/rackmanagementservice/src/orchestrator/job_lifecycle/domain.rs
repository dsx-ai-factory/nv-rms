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

use super::ids::JobId;

/// Domain-specific policy for job failures.
pub trait JobDomain: Send + Sync + 'static {
    /// Failure recorded when an owned handle is dropped before reaching a
    /// terminal state.
    fn dropped_failure() -> JobFailure;

    /// Failure returned when the registry refuses a new job because it is at
    /// its retained-job limit. `current`/`max` describe the occupancy so the
    /// domain can build a useful message.
    fn at_capacity_failure(current: usize, max: usize) -> JobFailure;

    /// Failure returned when a node-exclusive create is refused because
    /// `active_job_id` already targets `(rack_id, node_id)`.
    fn node_busy_failure(rack_id: &str, node_id: &str, active_job_id: &JobId) -> JobFailure;

    /// Failure returned when a leaf create is refused because the registry
    /// has begun shutdown and no longer accepts new jobs.
    fn shutting_down_failure() -> JobFailure;

    /// Failure returned when a child cannot be created under the requested
    /// parent because that job is missing, terminal, or itself a child.
    fn parent_unavailable_failure(parent_job_id: &JobId) -> JobFailure;
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
