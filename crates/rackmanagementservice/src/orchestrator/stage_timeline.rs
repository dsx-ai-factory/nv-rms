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
//! A `StageTimeline` is built once, up front, from the predefined ordered
//! sequence of stages a job will walk through and the `job_id` they all
//! belong to (see [`StageTimeline::with_stages`]), optionally tagged with
//! the node/rack it runs against (see [`StageTimeline::with_node_rack`]),
//! then driven forward with `start_current` / `complete_current` /
//! `fail_current`, which operate on an internal cursor so callers never
//! have to repeat a stage's name (or its description) at every call site.
//! Each of these three transitions also logs a generic `"stage starting"` /
//! `"stage completed"` / `"stage cancelled"`/`"stage failed"` event (with
//! `job_id`, `node`, `rack`, `stage`, and, for failures, `error`), so call
//! sites only need to log stage-specific context beyond that.
//!
//! Each [`Stage`] carries an immutable `name` and `description` (a fixed,
//! imperative summary of what the stage does, e.g. "Stage switch system
//! image on target"), plus a `status` that moves through `queued` ->
//! `running` -> `completed`/`failed`/`skipped`, wall-clock timestamps,
//! opaque `details`, and a `message` that is populated only on failure
//! (the error text). A `Stage` does *not* carry its own `job_id`: every
//! stage in a timeline belongs to the same job, so `job_id` -- like
//! `node`/`rack` -- is stored once on the `StageTimeline` rather than
//! once per stage, and `to_json()` copies it into each serialized stage.
//! Two situations move a stage
//! to `skipped` instead of `completed`: a stage that started but turned
//! out to have nothing to do (see [`StageTimeline::skip_current`]), and a
//! stage never reached at all because the job exited early -- a stage
//! failed, was cancelled, or a precondition check failed before any stage
//! started (see [`StageTimeline::skip_remaining`], which moves every
//! still-`queued` stage to `skipped` and logs it) -- so the timeline never
//! leaves unreached stages looking like they are still pending, or a
//! no-op stage looking like it actually did something.
//!
//! Every `start_stage()` call is meant to be matched by exactly one of
//! `complete_current` / `fail_current` / `skip_current`, in the same order
//! the job actually performs its work. If a caller's control flow ever
//! drifts out of sync with the predefined stage list -- e.g. it calls one
//! of these three transitions more times than there are stages -- the
//! call is a no-op (there is no stage left to record it on) but is logged
//! loudly at `error!` and `debug_assert!`s, so the mismatch fails tests
//! immediately during development rather than silently vanishing from the
//! timeline's own record in production.
//!
//! `fail_job` centralizes the `fail_current` + `skip_remaining` +
//! `job.fail` sequence every job-runner needs when a stage fails or the
//! stage list is exhausted. Job-runner modules define a thin, job-specific
//! `fail_stage` wrapper around it (threading through whatever job state a
//! `result_json` needs beyond the timeline itself), rather than
//! reimplementing that sequence -- two near-identical copies of it once
//! diverged on whether the `cancelled` flag passed to `fail_current`
//! actually reflected the triggering error's outcome.
//!
//! Timing is anchored to a single wall-clock reading taken when the timeline
//! is created; every subsequent timestamp and duration is derived from the
//! monotonic [`Instant`] clock relative to that anchor. This keeps the
//! timeline immune to wall-clock adjustments (NTP steps, leap seconds,
//! manual changes) that would otherwise produce out-of-order or negative
//! durations on a multi-minute job.
//!
//! Serializes via `to_json()` into a JSON object with:
//! ```json
//! {
//!   "started_at_ms": <unix ms when the timeline was created>,
//!   "updated_at_ms": <unix ms at the time of serialization>,
//!   "total_duration_ms": <monotonic ms since creation>,
//!   "current_stage": <name of the first "running" stage, or ""> ,
//!   "stage_index": <cursor position into the predefined stage list>,
//!   "stage_count": <total predefined stages>,
//!   "stages": [
//!     {
//!       "name": "...", "job_id": "...", "status": "queued|running|completed|failed|skipped",
//!       "description": "...", "message": "... (error text; empty on success)",
//!       "started_at_ms": <int>, "finished_at_ms": <int>, "duration_ms": <int>,
//!       "details": { ... opaque JSON ... }
//!     }
//!   ]
//! }
//! ```

use std::time::Instant;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::orchestrator::job_lifecycle::{JobError, JobFailure};
use crate::orchestrator::job_tracker::RmsJobHandle;
use crate::utilities::error::RmsError;

/// Possible per-stage statuses. Stored as a small enum so transitions are
/// obvious in logs and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Skipped,
}

impl StageStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

/// A single stage in a job's predefined [`StageTimeline`].
///
/// `name` and `description` are fixed at construction and never change;
/// `status`, `message`, timestamps, and `details` are updated as the stage
/// moves through its lifecycle. `message` is left empty unless the stage
/// fails, in which case it holds the failure's error text.
///
/// A `Stage` does *not* carry its own `job_id`: every stage within a given
/// [`StageTimeline`] belongs to the same job, so `job_id` is stored once,
/// on the timeline itself (see [`StageTimeline::with_stages`]), the same
/// way `node`/`rack` are (see [`StageTimeline::with_node_rack`]).
#[derive(Debug, Clone)]
pub struct Stage {
    name: String,
    description: String,
    status: StageStatus,
    message: String,
    started_at: Option<Instant>,
    finished_at: Option<Instant>,
    details: Value,
}

impl Stage {
    /// Creates a new, not-yet-started stage. `description` is a fixed,
    /// imperative summary of what the stage does (e.g. "Stage switch system
    /// image on target"), reported as progress when the stage starts and
    /// shown for the stage regardless of outcome. `message` starts empty and
    /// is only ever set by `fail_current`.
    pub fn new(name: String, description: String) -> Self {
        Self {
            name,
            description,
            status: StageStatus::Queued,
            message: String::new(),
            started_at: None,
            finished_at: None,
            details: Value::Object(serde_json::Map::new()),
        }
    }

    /// The stage's stable, unique name (e.g. "inspect_state").
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The stage's fixed, imperative description (e.g. "Stage switch system
    /// image on target").
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Ordered, predefined timeline of stages, driven forward by an internal
/// cursor via `start_current` / `complete_current` / `fail_current`.
#[derive(Debug, Clone)]
pub struct StageTimeline {
    stages: Vec<Stage>,
    /// Index into `stages` of the "current" stage.
    cursor: usize,
    /// The job ID every stage in `stages` belongs to, set via
    /// `with_stages`. Included in every `start_current` / `complete_current`
    /// / `fail_current` log and in each stage's serialized `job_id` in
    /// `to_json()`, so it is stored once here instead of once per `Stage`.
    job_id: String,
    /// Identifiers of the node/rack this timeline's stages run against, set
    /// via `with_node_rack`. Included in every `start_current` /
    /// `complete_current` / `fail_current` log so callers don't have to
    /// repeat them (or look them up from a node handle) at every call site.
    /// Empty when unset.
    node: String,
    rack: String,
    /// Absolute wall-clock time the timeline was created. Used only to project
    /// monotonic offsets onto calendar time for display.
    started_at_wall: DateTime<Utc>,
    /// Monotonic clock reading captured at the same moment as `started_at_wall`.
    /// All stage timing and durations are measured relative to this instant, so
    /// the timeline is immune to wall-clock adjustments after creation.
    started_at_mono: Instant,
}

impl StageTimeline {
    /// Builds an empty timeline with no predefined stages and no job ID.
    pub fn new() -> Self {
        Self::with_stages(String::new(), Vec::new())
    }

    /// Builds a timeline pre-populated with a predefined, ordered sequence of
    /// stages belonging to `job_id`, taking ownership of `stages`. Driving it
    /// forward with `start_current` / `complete_current` / `fail_current`
    /// means callers never have to retype a stage's name, description, or
    /// job ID at every call site.
    ///
    /// Pre-populating every stage up front (rather than only the ones
    /// touched so far) means `to_json()` shows the whole planned pipeline
    /// from the very first poll.
    pub fn with_stages(job_id: impl Into<String>, stages: Vec<Stage>) -> Self {
        Self {
            stages,
            cursor: 0,
            job_id: job_id.into(),
            node: String::new(),
            rack: String::new(),
            started_at_wall: Utc::now(),
            started_at_mono: Instant::now(),
        }
    }

    /// Attaches the node/rack this timeline's stages run against, so
    /// `start_current` / `complete_current` / `fail_current` can include
    /// them in their logs without the caller repeating them (or a node
    /// handle) at every stage transition. Chainable off `with_stages`.
    pub fn with_node_rack(mut self, node: impl Into<String>, rack: impl Into<String>) -> Self {
        self.node = node.into();
        self.rack = rack.into();
        self
    }

    /// The job ID every stage in this timeline belongs to, set via
    /// `with_stages`, or `""` for a timeline built with `new()`.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// The node ID attached via `with_node_rack`, or `""` if never set.
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The rack ID attached via `with_node_rack`, or `""` if never set.
    pub fn rack(&self) -> &str {
        &self.rack
    }

    /// Name of the stage at the cursor, or `None` once the cursor has
    /// advanced past the end of the predefined sequence.
    pub fn current_name(&self) -> Option<&str> {
        self.stages.get(self.cursor).map(|s| s.name.as_str())
    }

    /// Marks the stage at the cursor `Running`, logs its start, and returns
    /// a reference to it. Returns `None` if the cursor has advanced past
    /// the end of the predefined sequence.
    pub fn start_current(&mut self) -> Option<&Stage> {
        let idx = self.cursor;
        let stage = self.stages.get_mut(idx)?;
        if stage.started_at.is_none() {
            stage.started_at = Some(Instant::now());
        }
        stage.status = StageStatus::Running;
        let stage = self.stages.get(idx)?;
        tracing::info!(
            job_id = %self.job_id,
            node = %self.node,
            rack = %self.rack,
            stage = %stage.name,
            description = %stage.description,
            "stage starting ({}/{})", idx + 1, self.stages.len()
        );
        Some(stage)
    }

    /// Like [`Self::start_current`], but returns the stage's owned `(name,
    /// description)` instead of a borrowed `Stage`, so the caller can
    /// keep referring to them across later `complete_current` /
    /// `fail_current` calls that need a fresh `&mut StageTimeline` a
    /// borrowed `Stage` would otherwise conflict with.
    ///
    /// Errs with `"stage list exhausted"` if the cursor has advanced past
    /// the end of the predefined sequence -- i.e. the caller performed more
    /// stage transitions than the timeline's stage list accounted for.
    pub fn start_stage(&mut self) -> Result<(String, String), RmsError> {
        let stage = self
            .start_current()
            .ok_or_else(|| RmsError::internal("stage list exhausted"))?;
        Ok((stage.name().to_owned(), stage.description().to_owned()))
    }

    /// Completes the stage at the cursor, logs its completion, and advances
    /// the cursor. `message` is left untouched (empty, unless a previous
    /// `fail_current` somehow ran on this stage) since it is reserved for
    /// error text. Returns the name of the *next* stage in the sequence, or
    /// `None` if the completed stage was the last one.
    pub fn complete_current(&mut self, details: Value) -> Option<&str> {
        if self.cursor >= self.stages.len() {
            self.warn_cursor_exhausted("complete_current");
            return None;
        }
        self.finish_index(self.cursor, StageStatus::Completed, "", details);
        let stage = &self.stages[self.cursor];
        tracing::info!(
            job_id = %self.job_id,
            node = %self.node,
            rack = %self.rack,
            stage = %stage.name,
            "stage completed ({}/{})", self.cursor + 1, self.stages.len()
        );
        self.cursor += 1;
        self.stages.get(self.cursor).map(|s| s.name.as_str())
    }

    /// Marks the stage at the cursor `Skipped` (rather than `Completed`)
    /// and advances the cursor, exactly like `complete_current` otherwise.
    /// Use this when a stage was started but turned out to have nothing to
    /// do -- e.g. there was no unused partition image to clean up -- so
    /// the timeline reports it as a no-op rather than misleadingly
    /// claiming the stage's work actually ran. Returns the name of the
    /// *next* stage in the sequence, or `None` if the skipped stage was
    /// the last one.
    pub fn skip_current(&mut self, details: Value) -> Option<&str> {
        if self.cursor >= self.stages.len() {
            self.warn_cursor_exhausted("skip_current");
            return None;
        }
        self.finish_index(self.cursor, StageStatus::Skipped, "", details);
        let stage = &self.stages[self.cursor];
        tracing::info!(
            job_id = %self.job_id,
            node = %self.node,
            rack = %self.rack,
            stage = %stage.name,
            "stage skipped ({}/{})", self.cursor + 1, self.stages.len()
        );
        self.cursor += 1;
        self.stages.get(self.cursor).map(|s| s.name.as_str())
    }

    /// Fails the stage at the cursor, recording `message` as its error text
    /// and logging the failure. Terminal: the cursor does not advance.
    ///
    /// `cancelled` only controls the log's level/wording (`warn`+"stage
    /// cancelled" vs `error`+"stage failed") -- the recorded status,
    /// message, and details are the same either way, since a cancelled
    /// stage is still, structurally, a failed one.
    pub fn fail_current(&mut self, cancelled: bool, message: &str, details: Value) {
        if self.cursor < self.stages.len() {
            self.finish_index(self.cursor, StageStatus::Failed, message, details);
            let stage = &self.stages[self.cursor];
            if cancelled {
                tracing::warn!(
                    job_id = %self.job_id,
                    node = %self.node,
                    rack = %self.rack,
                    stage = %stage.name,
                    error = %message,
                    "stage cancelled ({}/{})", self.cursor + 1, self.stages.len()
                );
            } else {
                tracing::error!(
                    job_id = %self.job_id,
                    node = %self.node,
                    rack = %self.rack,
                    stage = %stage.name,
                    error = %message,
                    "stage failed ({}/{})", self.cursor + 1, self.stages.len()
                );
            }
        } else {
            self.warn_cursor_exhausted("fail_current");
        }
    }

    /// Marks every not-yet-started (`Queued`) stage from the cursor onward
    /// as `Skipped` and logs each one. Call this once, right before a job
    /// seals its terminal state early (a stage failed, was cancelled, or a
    /// precondition check failed before any stage even started), so the
    /// timeline doesn't show unreached stages stuck at `queued` forever.
    ///
    /// Stages that already started -- `Running`, `Completed`, or `Failed`
    /// -- are left untouched; `fail_current`/`complete_current` already
    /// recorded their outcome. Idempotent: calling it again (or when there
    /// is nothing left to skip) is a no-op.
    pub fn skip_remaining(&mut self) {
        let total = self.stages.len();
        for idx in self.cursor..total {
            if self.stages[idx].status != StageStatus::Queued {
                continue;
            }
            self.stages[idx].status = StageStatus::Skipped;
            let stage = &self.stages[idx];
            tracing::info!(
                job_id = %self.job_id,
                node = %self.node,
                rack = %self.rack,
                stage = %stage.name,
                "stage skipped ({}/{})", idx + 1, total
            );
        }
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
                "job_id": self.job_id,
                "status": stage.status.as_str(),
                "description": stage.description,
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
            "stage_index": self.cursor,
            "stage_count": self.stages.len(),
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

    /// Called by `complete_current` / `fail_current` / `skip_current` when
    /// the cursor has already advanced past the end of the predefined
    /// stage list. This should never happen -- every `start_stage()` call
    /// is meant to be matched by exactly one of those three -- so hitting
    /// it always means the predefined stage list and the job body's
    /// control flow have drifted out of sync. There is no stage left to
    /// record `transition` on, so without this it would otherwise vanish
    /// silently from the timeline; log it loudly instead, and
    /// `debug_assert!` so the drift fails tests immediately rather than
    /// shipping unnoticed (a no-op in release builds, where the caller
    /// still degrades gracefully).
    fn warn_cursor_exhausted(&self, transition: &str) {
        tracing::error!(
            job_id = %self.job_id(),
            node = %self.node,
            rack = %self.rack,
            cursor = self.cursor,
            stage_count = self.stages.len(),
            "{transition} called past the end of the predefined stage list; \
             this transition cannot be recorded in the timeline"
        );
        debug_assert!(
            false,
            "StageTimeline::{transition} called with cursor {} >= stage_count {}; \
             the predefined stage list and the job body have drifted out of sync",
            self.cursor,
            self.stages.len()
        );
    }

    /// Shared by the `complete_current` / `fail_current` transition paths.
    /// `message` is only written when non-empty, so `complete_current` can
    /// pass through without disturbing the (otherwise error-only) field.
    fn finish_index(&mut self, idx: usize, status: StageStatus, message: &str, details: Value) {
        let now = Instant::now();
        let stage = &mut self.stages[idx];
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

/// Fails the stage at `timeline`'s cursor, marks every stage after it
/// `skipped` (since the job is about to exit early), and seals `job` as
/// failed with a `result_json` built by `build_result_json`. Collapses
/// what would otherwise be a `fail_current` + `skip_remaining` +
/// `build_result_json` + `job.fail` block, repeated at every stage
/// boundary across every job type driven by a `StageTimeline`, into a
/// single call.
///
/// `build_result_json` is passed `outcome` and the now-`skip_remaining`'d
/// `timeline` rather than being handed a finished JSON string, so it can
/// still fold in whatever job-specific state (install job ID, configured
/// services, before/after snapshots, ...) a particular job's result
/// payload needs beyond the timeline itself. Job-runner modules define a
/// thin, job-specific `fail_stage` wrapper around this that threads that
/// state through to their own `build_result_json`, rather than
/// reimplementing this sequence -- see the module docs for why that
/// matters.
///
/// `outcome` should be `"cancelled"` when `error` originated from a
/// cancellation (e.g. `RmsError`'s `ErrorCode::Cancelled`) and `"failed"`
/// otherwise; it is both forwarded to `fail_current` (as a `bool`, to
/// pick `warn!`+"stage cancelled" vs `error!`+"stage failed") and passed
/// through to `build_result_json` as the result's top-level `status`.
pub(crate) fn fail_job(
    timeline: &mut StageTimeline,
    job: RmsJobHandle,
    outcome: &str,
    error: &str,
    details: Value,
    build_result_json: impl FnOnce(&str, &StageTimeline) -> String,
) {
    timeline.fail_current(outcome == "cancelled", error, details);
    timeline.skip_remaining();
    let result_json = build_result_json(outcome, timeline);
    job.fail(JobFailure::new(JobError::Other, error).with_result_json(result_json));
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_JOB_ID: &str = "job-123";

    fn test_stages() -> Vec<Stage> {
        [
            ("inspect_state", "Inspect current state"),
            ("stage_image", "Stage image"),
            ("trigger_install", "Trigger install"),
        ]
        .into_iter()
        .map(|(name, description)| Stage::new(name.to_owned(), description.to_owned()))
        .collect()
    }

    #[test]
    fn new_timeline_has_no_stages_and_zero_current() {
        let t = StageTimeline::new();
        let v = t.to_json();
        assert_eq!(v["stages"].as_array().unwrap().len(), 0);
        assert_eq!(v["current_stage"].as_str().unwrap(), "");
        assert!(v["started_at_ms"].as_i64().unwrap() > 0);
    }

    #[test]
    fn status_as_str_matches_cpp_labels() {
        assert_eq!(StageStatus::Queued.as_str(), "queued");
        assert_eq!(StageStatus::Running.as_str(), "running");
        assert_eq!(StageStatus::Completed.as_str(), "completed");
        assert_eq!(StageStatus::Failed.as_str(), "failed");
        assert_eq!(StageStatus::Skipped.as_str(), "skipped");
    }

    #[test]
    fn with_stages_pre_populates_all_stages_as_queued_with_descriptions_and_job_id() {
        let t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        let v = t.to_json();
        let stages = v["stages"].as_array().unwrap();
        assert_eq!(stages.len(), 3);
        assert_eq!(v["stage_index"].as_u64().unwrap(), 0);
        assert_eq!(v["stage_count"].as_u64().unwrap(), 3);
        for (stage, (name, description)) in stages.iter().zip([
            ("inspect_state", "Inspect current state"),
            ("stage_image", "Stage image"),
            ("trigger_install", "Trigger install"),
        ]) {
            assert_eq!(stage["name"].as_str().unwrap(), name);
            assert_eq!(stage["job_id"].as_str().unwrap(), TEST_JOB_ID);
            assert_eq!(stage["status"].as_str().unwrap(), "queued");
            assert_eq!(stage["description"].as_str().unwrap(), description);
            assert_eq!(stage["message"].as_str().unwrap(), "");
        }
    }

    #[test]
    fn start_stage_returns_owned_name_and_description() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        let (name, description) = t.start_stage().expect("first stage should exist");
        assert_eq!(name, "inspect_state");
        assert_eq!(description, "Inspect current state");
        assert_eq!(
            t.to_json()["stages"].as_array().unwrap()[0]["status"],
            "running"
        );
    }

    #[test]
    fn start_stage_past_the_last_stage_errs_with_stage_list_exhausted() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        for _ in 0..3 {
            t.start_stage().expect("stage should exist");
            t.complete_current(Value::Null);
        }
        let err = t
            .start_stage()
            .expect_err("cursor is past the predefined stages");
        assert_eq!(err.message, "stage list exhausted");
    }

    #[test]
    fn start_current_marks_running_and_returns_the_stage() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        let stage = t.start_current().expect("first stage should exist");
        assert_eq!(stage.name(), "inspect_state");
        assert_eq!(stage.description(), "Inspect current state");

        let v = t.to_json();
        assert_eq!(v["current_stage"].as_str().unwrap(), "inspect_state");
        assert_eq!(v["stages"].as_array().unwrap()[0]["status"], "running");
    }

    #[test]
    fn job_id_is_shared_by_the_timeline_rather_than_each_stage() {
        let t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        assert_eq!(t.job_id(), TEST_JOB_ID);

        // `to_json()` still copies `job_id` into every serialized stage
        // (unchanged wire format), but it is sourced from the timeline's
        // single `job_id`, not stored redundantly per `Stage`.
        let v = t.to_json();
        for stage in v["stages"].as_array().unwrap() {
            assert_eq!(stage["job_id"], TEST_JOB_ID);
        }
    }

    #[test]
    fn job_id_defaults_to_empty_for_an_empty_timeline() {
        let t = StageTimeline::new();
        assert_eq!(t.job_id(), "");
    }

    #[test]
    fn node_and_rack_default_to_empty_when_unset() {
        let t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        assert_eq!(t.node(), "");
        assert_eq!(t.rack(), "");
    }

    #[test]
    fn with_node_rack_attaches_node_and_rack() {
        let t = StageTimeline::with_stages(TEST_JOB_ID, test_stages())
            .with_node_rack("sw-01", "rack-01");
        assert_eq!(t.node(), "sw-01");
        assert_eq!(t.rack(), "rack-01");
    }

    #[test]
    fn complete_current_advances_cursor_leaves_message_empty_and_returns_next_name() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        t.start_current();
        // Sleep briefly so duration is measurable.
        std::thread::sleep(std::time::Duration::from_millis(5));
        let next = t.complete_current(serde_json::json!({"nodes": 3}));
        assert_eq!(next, Some("stage_image"));
        assert_eq!(t.current_name(), Some("stage_image"));

        let v = t.to_json();
        assert_eq!(v["stage_index"].as_u64().unwrap(), 1);
        let inspect = &v["stages"].as_array().unwrap()[0];
        assert_eq!(inspect["status"], "completed");
        assert_eq!(inspect["message"], "");
        assert_eq!(inspect["description"], "Inspect current state");
        assert_eq!(inspect["details"]["nodes"], 3);
        assert!(inspect["duration_ms"].as_i64().unwrap() >= 5);
        assert!(
            inspect["finished_at_ms"].as_i64().unwrap()
                >= inspect["started_at_ms"].as_i64().unwrap()
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "drifted out of sync")]
    fn complete_current_past_the_last_stage_panics_in_debug_builds() {
        // A `complete_current` call past the end of the predefined stage
        // list means the stage list and the job body's control flow have
        // drifted out of sync -- a programming error `debug_assert!`
        // should catch during development/tests rather than let vanish
        // silently from the timeline's record. (Compiled out in release
        // builds, where the caller still degrades gracefully to `None`;
        // `#[cfg(debug_assertions)]` skips this test there too, since
        // `cargo test --release` wouldn't panic either.)
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        for _ in 0..3 {
            t.start_current();
            t.complete_current(Value::Null);
        }
        t.complete_current(Value::Null);
    }

    #[test]
    fn fail_current_records_message_and_is_terminal() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        t.start_current();
        t.fail_current(
            false,
            "SFTP failed",
            serde_json::json!({"error": "timeout"}),
        );

        assert_eq!(t.current_name(), Some("inspect_state"));
        let v = t.to_json();
        assert_eq!(v["stage_index"].as_u64().unwrap(), 0);
        let stage = &v["stages"].as_array().unwrap()[0];
        assert_eq!(stage["status"], "failed");
        assert_eq!(stage["message"], "SFTP failed");
        assert_eq!(stage["details"]["error"], "timeout");
    }

    #[test]
    fn fail_current_records_the_same_failure_regardless_of_cancelled_flag() {
        // `cancelled` only changes the level/wording of the emitted log
        // (warn+"stage cancelled" vs error+"stage failed"); the recorded
        // status, message, and details should be identical either way.
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        t.start_current();
        t.fail_current(
            true,
            "job cancelled before inspect_state",
            serde_json::json!({"cancelled_before_start": true}),
        );

        assert_eq!(t.current_name(), Some("inspect_state"));
        let v = t.to_json();
        let stage = &v["stages"].as_array().unwrap()[0];
        assert_eq!(stage["status"], "failed");
        assert_eq!(stage["message"], "job cancelled before inspect_state");
        assert_eq!(stage["details"]["cancelled_before_start"], true);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "drifted out of sync")]
    fn fail_current_past_the_last_stage_panics_in_debug_builds() {
        // Every call site that reaches `start_stage().expect(...)` /
        // `let-else` "stage list exhausted" handling calls `fail_current`
        // with the cursor already past the end -- exactly this scenario.
        // Before this guard, that call was a silent no-op: no log, no
        // stage marked failed, so the resulting job failure vanished from
        // the timeline's own record even though `job.fail(...)` still
        // recorded it at the `JobHandle` level.
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        for _ in 0..3 {
            t.start_current();
            t.complete_current(Value::Null);
        }
        t.fail_current(false, "stage list exhausted", Value::Null);
    }

    #[test]
    fn skip_current_marks_the_started_stage_skipped_and_advances_the_cursor() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        t.start_current();
        let next = t.skip_current(serde_json::json!({"reason": "nothing to clean up"}));
        assert_eq!(next, Some("stage_image"));
        assert_eq!(t.current_name(), Some("stage_image"));

        let v = t.to_json();
        let inspect = &v["stages"].as_array().unwrap()[0];
        assert_eq!(inspect["status"], "skipped");
        assert_eq!(inspect["message"], "");
        assert_eq!(inspect["details"]["reason"], "nothing to clean up");
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "drifted out of sync")]
    fn skip_current_past_the_last_stage_panics_in_debug_builds() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        for _ in 0..3 {
            t.start_current();
            t.complete_current(Value::Null);
        }
        t.skip_current(Value::Null);
    }

    #[test]
    fn skip_remaining_marks_queued_stages_skipped_after_a_failure() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        t.start_current();
        t.fail_current(false, "SFTP failed", Value::Null);
        t.skip_remaining();

        let v = t.to_json();
        let stages = v["stages"].as_array().unwrap();
        assert_eq!(stages[0]["status"], "failed");
        assert_eq!(stages[1]["status"], "skipped");
        assert_eq!(stages[2]["status"], "skipped");
    }

    #[test]
    fn skip_remaining_marks_every_stage_skipped_before_any_stage_starts() {
        // A precondition check can fail before `start_current` is ever
        // called; the cursor is still 0, so every stage is `Queued`.
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        t.skip_remaining();

        let v = t.to_json();
        for stage in v["stages"].as_array().unwrap() {
            assert_eq!(stage["status"], "skipped");
        }
    }

    #[test]
    fn skip_remaining_leaves_completed_stages_untouched() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        t.start_current();
        t.complete_current(Value::Null);
        t.start_current();
        t.fail_current(false, "stage_image failed", Value::Null);
        t.skip_remaining();

        let v = t.to_json();
        let stages = v["stages"].as_array().unwrap();
        assert_eq!(stages[0]["status"], "completed");
        assert_eq!(stages[1]["status"], "failed");
        assert_eq!(stages[2]["status"], "skipped");
    }

    #[test]
    fn skip_remaining_is_idempotent_and_a_no_op_once_all_stages_finish() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        for _ in 0..3 {
            t.start_current();
            t.complete_current(Value::Null);
        }
        t.skip_remaining();
        t.skip_remaining();

        let v = t.to_json();
        for stage in v["stages"].as_array().unwrap() {
            assert_eq!(stage["status"], "completed");
        }
    }

    #[test]
    fn stage_timestamps_are_derived_from_the_creation_anchor() {
        let mut t = StageTimeline::with_stages(TEST_JOB_ID, test_stages());
        let anchor = t.to_json()["started_at_ms"].as_i64().unwrap();

        t.start_current();
        std::thread::sleep(std::time::Duration::from_millis(5));
        t.complete_current(Value::Null);

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
}
