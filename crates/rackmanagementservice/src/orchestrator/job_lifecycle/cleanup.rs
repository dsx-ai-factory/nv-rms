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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::domain::JobDomain;
use super::ids::JobId;
use super::registry::JobRegistry;

/// Async cleanup owned by a tracked job supervisor.
#[derive(Debug, Clone)]
pub enum CleanupPlan {
    RemoveDir {
        path: PathBuf,
    },
    RemoveDirAfterJobsTerminate {
        child_job_ids: Vec<JobId>,
        path: PathBuf,
        timeout: Option<Duration>,
    },
    /// Test-only plan that panics when the supervisor runs cleanup.
    #[cfg(test)]
    PanicForTest {
        path: PathBuf,
    },
}

impl CleanupPlan {
    /// Removes a directory after the worker exits.
    pub fn remove_dir(path: impl Into<PathBuf>) -> Self {
        Self::RemoveDir { path: path.into() }
    }

    /// Removes a directory after all child jobs terminate.
    pub fn remove_dir_after_jobs_terminate(
        child_job_ids: Vec<JobId>,
        path: impl Into<PathBuf>,
        timeout: Option<Duration>,
    ) -> Self {
        Self::RemoveDirAfterJobsTerminate {
            child_job_ids,
            path: path.into(),
            timeout,
        }
    }

    pub(crate) async fn run<D: JobDomain>(self, registry: Arc<JobRegistry<D>>) -> CleanupReport {
        match self {
            Self::RemoveDir { path } => remove_dir(path).await,
            Self::RemoveDirAfterJobsTerminate {
                child_job_ids,
                path,
                timeout,
            } => {
                let completed = registry
                    .wait_for_terminal_jobs(&child_job_ids, timeout)
                    .await;
                if !completed {
                    tracing::warn!(
                        child_jobs = child_job_ids.len(),
                        "cleanup timed out waiting for child jobs to become terminal"
                    );
                    return CleanupReport::Skipped;
                }
                remove_dir(path).await
            }
            #[cfg(test)]
            Self::PanicForTest { .. } => panic!("simulated cleanup panic"),
        }
    }
}

#[cfg(test)]
impl CleanupPlan {
    pub(crate) fn panic_for_test(path: impl Into<PathBuf>) -> Self {
        Self::PanicForTest { path: path.into() }
    }
}

/// Outcome of a supervisor cleanup attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupReport {
    Skipped,
    Removed,
    NotFound,
    Failed(CleanupFailure),
    /// The cleanup task panicked. The supervisor catches the panic so it never
    /// escalates into a generic Tokio task error; the panic is logged within the
    /// job's tracing span (which carries `job_id`) and the payload message is
    /// captured here. A panic leaves the cleanup incomplete, so the resource may
    /// be leaked.
    Panicked(String),
}

/// Structured details for a failed cleanup attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupFailure {
    pub operation: CleanupOperation,
    pub path: PathBuf,
    pub kind: std::io::ErrorKind,
    pub message: String,
}

/// Cleanup operation that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupOperation {
    RemoveDir,
}

async fn remove_dir(path: PathBuf) -> CleanupReport {
    match tokio::fs::remove_dir_all(&path).await {
        Ok(()) => CleanupReport::Removed,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => CleanupReport::NotFound,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "tracked job cleanup failed to remove directory"
            );
            CleanupReport::Failed(CleanupFailure {
                operation: CleanupOperation::RemoveDir,
                path,
                kind: error.kind(),
                message: error.to_string(),
            })
        }
    }
}
