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

//! Per-stage timing tracker for long-running orchestration flows.
//!
//! Each stage has a stable `name` (e.g. "inspect_state", "stage_image"),
//! a `status` that moves through "queued" -> "running" -> "completed"/"failed",
//! and wall-clock timestamps that clients / dashboards can use to reason
//! about where time is being spent in a multi-minute job.
//!
//! Timing is anchored to a single wall-clock reading taken when the timeline
//! is created; every subsequent timestamp and duration is derived from the
//! monotonic [`Instant`] clock relative to that anchor. This keeps the timeline
//! immune to wall-clock adjustments (NTP steps, leap seconds, manual changes)
//! that would otherwise produce out-of-order or negative durations on a
//! multi-minute job.
//!
//! Serializes via `to_json()` into a JSON object with:
//! ```json
//! {
//!   "started_at_ms": <unix ms when the timeline was created>,
//!   "updated_at_ms": <unix ms at the time of serialization>,
//!   "total_duration_ms": <monotonic ms since creation>,
//!   "current_stage": <name of the first "running" stage, or ""> ,
//!   "stages": [
//!     {
//!       "name": "...", "status": "running|completed|failed",
//!       "message": "...",
//!       "started_at_ms": <int>, "finished_at_ms": <int>, "duration_ms": <int>,
//!       "details": { ... opaque JSON ... }
//!     }
//!   ]
//! }
//! ```

use std::time::Instant;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

/// Possible per-stage statuses. Stored as a small enum so transitions are
/// obvious in logs and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

impl StageStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
struct StageInfo {
    name: String,
    status: StageStatus,
    message: String,
    started_at: Option<Instant>,
    finished_at: Option<Instant>,
    details: Value,
}

impl StageInfo {
    fn new(name: String) -> Self {
        Self {
            name,
            status: StageStatus::Queued,
            message: String::new(),
            started_at: None,
            finished_at: None,
            details: Value::Object(serde_json::Map::new()),
        }
    }
}

/// Ordered, append-only timeline of stages. Transitions a stage to
/// `running` via `start()`, then to `completed` / `failed` via
/// `complete()` / `fail()`.
#[derive(Debug, Clone)]
pub struct StageTimeline {
    stages: Vec<StageInfo>,
    /// Absolute wall-clock time the timeline was created. Used only to project
    /// monotonic offsets onto calendar time for display.
    started_at_wall: DateTime<Utc>,
    /// Monotonic clock reading captured at the same moment as `started_at_wall`.
    /// All stage timing and durations are measured relative to this instant, so
    /// the timeline is immune to wall-clock adjustments after creation.
    started_at_mono: Instant,
}

impl StageTimeline {
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            started_at_wall: Utc::now(),
            started_at_mono: Instant::now(),
        }
    }

    /// Mark `name` as running with the given progress `message`. Creates
    /// the stage if it doesn't exist; preserves the first start instant
    /// if the stage has already been started once.
    pub fn start(&mut self, name: &str, message: &str) {
        let stage = self.find_or_create_mut(name);
        if stage.started_at.is_none() {
            stage.started_at = Some(Instant::now());
        }
        stage.status = StageStatus::Running;
        stage.message = message.to_owned();
    }

    /// Transition the stage to `completed`. `message` and `details` are
    /// stored on the entry; empty strings / empty objects are accepted as
    /// "no update".
    pub fn complete(&mut self, name: &str, message: &str, details: Value) {
        self.finish(name, StageStatus::Completed, message, details);
    }

    /// Transition the stage to `failed`.
    pub fn fail(&mut self, name: &str, message: &str, details: Value) {
        self.finish(name, StageStatus::Failed, message, details);
    }

    /// Serialize the full timeline to JSON for embedding in a result payload.
    pub fn to_json(&self) -> Value {
        let mut current_stage = String::new();
        let mut stages = Vec::with_capacity(self.stages.len());
        for stage in &self.stages {
            if current_stage.is_empty() && stage.status == StageStatus::Running {
                current_stage = stage.name.clone();
            }
            let started_at_ms = stage.started_at.map(|t| self.to_wall_ms(t)).unwrap_or(0);
            let finished_at_ms = stage.finished_at.map(|t| self.to_wall_ms(t)).unwrap_or(0);
            let duration_ms = match (stage.started_at, stage.finished_at) {
                (Some(_), Some(_)) => finished_at_ms - started_at_ms,
                _ => 0,
            };
            stages.push(json!({
                "name": stage.name,
                "status": stage.status.as_str(),
                "message": stage.message,
                "started_at_ms": started_at_ms,
                "finished_at_ms": finished_at_ms,
                "duration_ms": duration_ms,
                "details": stage.details,
            }));
        }

        json!({
            "started_at_ms": self.started_at_wall.timestamp_millis(),
            "updated_at_ms": self.to_wall_ms(Instant::now()),
            "total_duration_ms": self.started_at_mono.elapsed().as_millis() as i64,
            "current_stage": current_stage,
            "stages": stages,
        })
    }

    /// Projects a monotonic instant onto the absolute wall-clock timeline by
    /// adding its offset from the creation anchor to `started_at_wall`. This
    /// yields stable, monotonically ordered display timestamps regardless of
    /// any wall-clock adjustments after the timeline was created.
    fn to_wall_ms(&self, instant: Instant) -> i64 {
        self.started_at_wall.timestamp_millis()
            + instant.duration_since(self.started_at_mono).as_millis() as i64
    }

    fn find_or_create_mut(&mut self, name: &str) -> &mut StageInfo {
        if let Some(idx) = self.stages.iter().position(|s| s.name == name) {
            return &mut self.stages[idx];
        }
        let idx = self.stages.len();
        self.stages.push(StageInfo::new(name.to_owned()));
        &mut self.stages[idx]
    }

    fn finish(&mut self, name: &str, status: StageStatus, message: &str, details: Value) {
        let now = Instant::now();
        let stage = self.find_or_create_mut(name);
        if stage.started_at.is_none() {
            stage.started_at = Some(now);
        }
        stage.finished_at = Some(now);
        stage.status = status;
        if !message.is_empty() {
            stage.message = message.to_owned();
        }
        if !matches!(&details, Value::Object(m) if m.is_empty()) {
            stage.details = details;
        }
    }
}

impl Default for StageTimeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_timeline_has_no_stages_and_zero_current() {
        let t = StageTimeline::new();
        let v = t.to_json();
        assert_eq!(v["stages"].as_array().unwrap().len(), 0);
        assert_eq!(v["current_stage"].as_str().unwrap(), "");
        assert!(v["started_at_ms"].as_i64().unwrap() > 0);
    }

    #[test]
    fn start_then_complete_moves_through_running_to_completed() {
        let mut t = StageTimeline::new();
        t.start("inspect_state", "Inspecting current state");
        let running = t.to_json();
        assert_eq!(running["current_stage"].as_str().unwrap(), "inspect_state");
        let stages = running["stages"].as_array().unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0]["status"].as_str().unwrap(), "running");

        // Sleep briefly so duration is measurable.
        std::thread::sleep(std::time::Duration::from_millis(5));
        t.complete(
            "inspect_state",
            "Inspected",
            serde_json::json!({"nodes": 3}),
        );
        let done = t.to_json();
        assert_eq!(done["current_stage"].as_str().unwrap(), "");
        let s = &done["stages"].as_array().unwrap()[0];
        assert_eq!(s["status"].as_str().unwrap(), "completed");
        assert_eq!(s["message"].as_str().unwrap(), "Inspected");
        assert_eq!(s["details"]["nodes"].as_i64().unwrap(), 3);
        assert!(s["duration_ms"].as_i64().unwrap() >= 5);
        assert!(s["finished_at_ms"].as_i64().unwrap() >= s["started_at_ms"].as_i64().unwrap());
    }

    #[test]
    fn fail_marks_status_failed_and_keeps_details() {
        let mut t = StageTimeline::new();
        t.start("stage_image", "Staging");
        t.fail(
            "stage_image",
            "SFTP failed",
            serde_json::json!({"error": "timeout"}),
        );
        let v = t.to_json();
        let s = &v["stages"].as_array().unwrap()[0];
        assert_eq!(s["status"].as_str().unwrap(), "failed");
        assert_eq!(s["message"].as_str().unwrap(), "SFTP failed");
        assert_eq!(s["details"]["error"].as_str().unwrap(), "timeout");
    }

    #[test]
    fn stages_preserve_insertion_order() {
        let mut t = StageTimeline::new();
        t.start("inspect_state", "");
        t.complete("inspect_state", "", Value::Null);
        t.start("stage_image", "");
        t.complete("stage_image", "", Value::Null);
        t.start("trigger_install", "");
        let v = t.to_json();
        let names: Vec<&str> = v["stages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.get("name").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            vec!["inspect_state", "stage_image", "trigger_install"]
        );
        // The first stage still running (trigger_install) is surfaced as current.
        assert_eq!(v["current_stage"].as_str().unwrap(), "trigger_install");
    }

    #[test]
    fn finish_without_prior_start_still_records_timestamps() {
        let mut t = StageTimeline::new();
        t.complete("direct_complete", "done", Value::Null);
        let v = t.to_json();
        let s = &v["stages"].as_array().unwrap()[0];
        assert_eq!(s["status"].as_str().unwrap(), "completed");
        assert!(s["started_at_ms"].as_i64().unwrap() > 0);
        assert!(s["finished_at_ms"].as_i64().unwrap() > 0);
    }

    #[test]
    fn stage_timestamps_are_derived_from_the_creation_anchor() {
        let mut t = StageTimeline::new();
        let anchor = t.to_json()["started_at_ms"].as_i64().unwrap();

        t.start("inspect_state", "Inspecting");
        std::thread::sleep(std::time::Duration::from_millis(5));
        t.complete("inspect_state", "done", Value::Null);

        let v = t.to_json();
        let s = &v["stages"].as_array().unwrap()[0];
        let started = s["started_at_ms"].as_i64().unwrap();
        let finished = s["finished_at_ms"].as_i64().unwrap();
        let duration = s["duration_ms"].as_i64().unwrap();

        // Display timestamps are projected forward from the single anchor, so
        // they never precede it and stay internally consistent with the
        // monotonic duration.
        assert!(started >= anchor);
        assert!(finished >= started);
        assert_eq!(finished - started, duration);
        assert!(v["updated_at_ms"].as_i64().unwrap() >= finished);
    }

    #[test]
    fn status_as_str_matches_cpp_labels() {
        assert_eq!(StageStatus::Queued.as_str(), "queued");
        assert_eq!(StageStatus::Running.as_str(), "running");
        assert_eq!(StageStatus::Completed.as_str(), "completed");
        assert_eq!(StageStatus::Failed.as_str(), "failed");
    }
}
