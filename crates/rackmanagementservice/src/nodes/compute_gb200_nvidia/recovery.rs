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

//! The GB200 compute OOB recovery procedure as two stages of plain data:
//! the plan, the roster narrowed to what needs recovery with a file on
//! each push ([`RecoveryPlan`]), and the device choreography that walks it
//! ([`run`]). The plan is pure, only `run` touches the node. Deliberately
//! absent: the Volatile-DOT CAK install, the Boot AP step, and the ERoT
//! boot-status poll.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::domain::node::{FirmwareInfo, RecoveryBundles, RecoveryProfile};
use crate::utilities::error::{Result, RmsError};

/// How many times the closing confirm re-reads inventory for components
/// that are neither healthy nor in recovery state (still activating,
/// briefly absent), and how long it waits between reads. The procedure
/// document's confirm step names no timeout; its one-minute heading is a
/// human allotment. These are the post-activation version check's numbers,
/// constants like every other settle wait in RMS today, until the settle
/// waits move into `WorkflowTimings` together.
pub(super) const CONFIRM_RETRIES: usize = 5;
#[cfg(not(test))]
const CONFIRM_RETRY_INTERVAL: Duration = Duration::from_secs(60);
#[cfg(test)]
const CONFIRM_RETRY_INTERVAL: Duration = Duration::ZERO;

/// The recovery list: watched components whose inventory reports
/// the recovery signature. Empty means the recovery flow does not apply.
pub(super) fn components_in_recovery_state(
    inventory: &[FirmwareInfo],
    watched: &[String],
) -> Vec<String> {
    inventory
        .iter()
        .filter(|info| {
            watched.contains(&info.name)
                && info.health == "Critical"
                && info.state == "StandbyOffline"
        })
        .map(|info| info.name.clone())
        .collect()
}

/// The flow's cancel check. It runs only where stopping leaves the tray in a
/// known state: before a stage starts and between confirm re-reads. `next`
/// names the stage that did not run.
fn check_cancelled(cancel: &CancellationToken, next: &str) -> Result<()> {
    if cancel.is_cancelled() {
        return Err(RmsError::cancelled(format!(
            "recovery flow cancelled before {next}"
        )));
    }
    Ok(())
}

/// What the procedure needs from the tray. The GB200 node implements it on
/// itself, the tests with a recording mock. Nothing above this module sees it.
pub trait RecoveryDevice {
    /// The node id, for the logs.
    fn name(&self) -> &str;
    /// The firmware inventory as the tray reports it now.
    async fn inventory(&self) -> Result<Vec<FirmwareInfo>>;
    /// Push `file` at the HMC chassis target with the update preconditions
    /// skipped, and return once the BMC task reached a terminal state, or
    /// with `Cancelled` once `cancel` fires while that task is awaited.
    async fn push(&self, file: &str, cancel: &CancellationToken) -> Result<()>;
    /// Force off, aux power cycle, stabilize, then wait until every
    /// `presence` member reports back.
    async fn power_block(&self, presence: &[String]) -> Result<()>;
}

//------------------------------------------------------------------------------

#[derive(serde::Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
struct RawRoster {
    /// Recovered with the regular HMC bundle: one push, then an AC cycle,
    /// one member per round. FPGA1 is reachable only through a working
    /// FPGA0, hence rounds.
    push_then_cycle: Vec<String>,
    /// Recovered with the HMC recovery bundle: one push, no cycle.
    push_only: Vec<String>,
    /// Members that must be back after the AC cycle before the flow goes on.
    presence: Vec<String>,
}

impl RawRoster {
    fn sanitize(self) -> Result<RawRoster> {
        let presence = Self::roster_names(self.presence)?;
        let push_then_cycle = Self::roster_names(self.push_then_cycle)?;
        let push_only = Self::roster_names(self.push_only)?;

        Self::check_all_disjoint(&[&push_then_cycle, &push_only, &presence])?;

        Ok(Self {
            push_then_cycle,
            push_only,
            presence,
        })
    }

    /// One roster list: names trimmed, de-duplicated and sorted, none blank, at
    /// least one left. Trimming matters because an untrimmed name never matches
    /// the inventory, which would silently switch detection off for it. Config
    /// order carries no meaning, so sorting costs nothing and keeps logs stable.
    fn roster_names(raw: Vec<String>) -> Result<Vec<String>> {
        let mut names = std::collections::BTreeSet::new();
        for entry in raw {
            let name = entry.trim();
            if name.is_empty() {
                return Err(RmsError::invalid_argument(
                    "recovery profile contains a blank name",
                ));
            }
            names.insert(name.to_owned());
        }
        if names.is_empty() {
            return Err(RmsError::invalid_argument(
                "recovery profile must not be empty",
            ));
        }
        Ok(names.into_iter().collect())
    }

    /// Every name belongs to exactly one roster list. A component cannot be
    /// both the path back (presence) and something recovered through that
    /// path, nor be recovered by both bundles.
    fn check_all_disjoint(lists: &[&Vec<String>]) -> Result<()> {
        let mut set = std::collections::HashSet::new();

        for l in lists {
            for s in *l {
                if !set.insert(s) {
                    return Err(RmsError::invalid_argument(format!(
                        "recovery profile: {s} is listed twice"
                    )));
                }
            }
        }

        Ok(())
    }
}

//------------------------------------------------------------------------------

/// The GB200 recovery roster: a recovery profile's table parsed and
/// validated by the one node type that has the flow. Field names are the
/// config keys and say what the flow does with each list; the lists hold
/// Redfish FirmwareInventory member names exactly as the tray reports them.
#[derive(Clone, Debug)]
pub struct RecoveryRoster {
    inner: RawRoster,
}

impl RecoveryRoster {
    /// Parse and validate a bound profile. Rules: only the three fields
    /// above exist; every list is non-empty; names are trimmed, de-duplicated
    /// and sorted; `push_then_cycle` and `push_only` are disjoint; `presence`
    /// shares no name with either. A violation is an `invalid_argument` that
    /// fails node registration.
    pub fn parse(profile: &RecoveryProfile) -> Result<Self> {
        let raw: RawRoster = profile
            .parse::<RawRoster>()
            .map_err(|error| RmsError::invalid_argument(format!("recovery profile: {error}")))?
            .sanitize()?;

        Ok(Self { inner: raw })
    }

    /// Components that need to be checked checks for the recovery signature
    pub fn watch_set(&self) -> Vec<String> {
        self.inner
            .push_then_cycle
            .iter()
            .chain(&self.inner.push_only)
            .cloned()
            .collect()
    }
}

//------------------------------------------------------------------------------

/// One push: the file and the names it is meant to bring back.
#[derive(Debug)]
struct Push {
    file: String,
    components: Vec<String>,
}

impl Push {
    fn new(file: String, components: Vec<String>) -> Option<Self> {
        if !components.is_empty() {
            Some(Self { file, components })
        } else {
            None
        }
    }
}

/// What one inventory read says about the components the plan touched.
#[derive(Debug)]
enum RecoveryVerdict {
    /// Every component reads OK and Enabled.
    Healthy,
    /// At least one still shows the recovery signature (Critical,
    /// StandbyOffline): the push did not take, waiting cannot help.
    StillInRecovery(Vec<String>),
    /// None in recovery, but some not yet OK and Enabled (Updating, or
    /// absent right after a push): worth another read.
    InProgress(Vec<String>),
}

//------------------------------------------------------------------------------

/// What the sequence will do, files assigned. `new` returns `None` when
/// nothing needs recovery, so at least one push is `Some` and an empty plan
/// cannot exist.
#[derive(Debug)]
pub struct RecoveryPlan {
    /// Rounds with the normal firmware, when that list has members that
    /// need recovery.
    push_then_cycle: Option<Push>,
    /// The one push with the recovery image, when that list has members
    /// that need recovery.
    push_only: Option<Push>,
    recheck_after_cycle: Vec<String>,
    wait_for_after_cycle: Vec<String>,
}

impl RecoveryPlan {
    /// Narrows the roster's two push lists to the detected names and attaches
    /// a file to each non-empty one: the regular bundle to `push_then_cycle`,
    /// the recovery bundle to `push_only`.
    pub fn new(
        roster: RecoveryRoster,
        bundles: RecoveryBundles,
        in_recovery: &[String],
    ) -> Option<Self> {
        let push_then_cycle = Self::intersect(&roster.inner.push_then_cycle, in_recovery);
        let push_only = Self::intersect(&roster.inner.push_only, in_recovery);

        if push_then_cycle.is_empty() && push_only.is_empty() {
            return None;
        }

        Some(Self {
            push_then_cycle: Push::new(bundles.regular, push_then_cycle),
            push_only: Push::new(bundles.recovery, push_only),
            recheck_after_cycle: roster.inner.push_then_cycle,
            wait_for_after_cycle: roster.inner.presence,
        })
    }

    /// Walk the plan on the node: the rounds first, then the single push, then
    /// the closing confirm over every name the plan touched plus any that
    /// surfaced during the rounds. The confirm reads live inventory, so a stale
    /// list cannot produce a false "recovered".
    pub async fn run(
        &self,
        device: &impl RecoveryDevice,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut to_confirm: Vec<String> = self
            .push_then_cycle
            .iter()
            .chain(&self.push_only)
            .flat_map(|push| push.components.iter().cloned())
            .collect();

        tracing::info!(
            node = %device.name(),
            components = ?to_confirm,
            "components in recovery state; running the OOB recovery flow"
        );

        if let Some(push) = &self.push_then_cycle {
            // One member per round: the cycle after the push activates it, and
            // only then can a member hidden behind it report at all. The re-read
            // after each round finds members still (or newly) in recovery and
            // adds them to the confirm list. Rounds stop at the recheck list's
            // size; a member still in recovery then fails the closing confirm.
            let mut still = push.components.clone();
            let mut rounds = 0;
            while !still.is_empty() && rounds < self.recheck_after_cycle.len() {
                check_cancelled(cancel, "the next push-then-cycle round")?;
                device.push(&push.file, cancel).await?;
                // A pushed image that waits for its cycle is a known state:
                // the next detection finds the member still in recovery
                // and the next run pushes and cycles again.
                check_cancelled(cancel, "the power block")?;
                device.power_block(&self.wait_for_after_cycle).await?;
                rounds += 1;

                // The re-read is network time. A cancel that landed in a
                // failed read is why the flow stops, not the read error. A
                // successful read goes on, and the next safe point stops it.
                let inventory = match device.inventory().await {
                    Ok(inventory) => inventory,
                    Err(error) if cancel.is_cancelled() => {
                        return Err(RmsError::cancelled(format!(
                            "recovery flow cancelled after the power block; inventory read failed: {}",
                            error.message
                        )));
                    }
                    Err(error) => return Err(error),
                };
                still = components_in_recovery_state(&inventory, &self.recheck_after_cycle);
                for name in &still {
                    if !to_confirm.contains(name) {
                        to_confirm.push(name.clone());
                    }
                }
            }
        }

        if let Some(push) = &self.push_only {
            check_cancelled(cancel, "the recovery push")?;
            device.push(&push.file, cancel).await?;
        }

        check_cancelled(cancel, "the closing confirm")?;
        Self::confirm_settled(device, &to_confirm, cancel).await
    }

    fn intersect(list: &[String], other: &[String]) -> Vec<String> {
        other
            .iter()
            .filter(|name| list.contains(name))
            .cloned()
            .collect()
    }

    /// Judge one inventory read for `components`, the names the plan touched.
    /// Pure, so the verdict rules are testable without a node.
    fn confirm(inventory: &[FirmwareInfo], components: &[String]) -> RecoveryVerdict {
        // The recovery signature is one predicate, shared with detection.
        let in_recovery = components_in_recovery_state(inventory, components);
        if !in_recovery.is_empty() {
            return RecoveryVerdict::StillInRecovery(in_recovery);
        }
        let in_progress: Vec<String> = components
            .iter()
            .filter(|name| {
                !inventory.iter().any(|info| {
                    &info.name == *name && info.health == "OK" && info.state == "Enabled"
                })
            })
            .cloned()
            .collect();
        if in_progress.is_empty() {
            RecoveryVerdict::Healthy
        } else {
            RecoveryVerdict::InProgress(in_progress)
        }
    }

    /// The closing confirm: re-read inventory until every touched component
    /// reads OK and Enabled, fail at once when one is still in recovery state,
    /// and fail closed after `CONFIRM_RETRIES` re-reads of components still in
    /// progress.
    async fn confirm_settled(
        device: &impl RecoveryDevice,
        components: &[String],
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut in_progress = Vec::new();
        for read in 0..=CONFIRM_RETRIES {
            if read > 0
                && cancel
                    .run_until_cancelled(tokio::time::sleep(CONFIRM_RETRY_INTERVAL))
                    .await
                    .is_none()
            {
                return Err(RmsError::cancelled(
                    "recovery flow cancelled before the next confirm re-read",
                ));
            }

            let reading = device.inventory().await;

            // The read is network time. A cancel that landed in it is why
            // the job stops, whatever the read returned, so the error says
            // so and carries the reading. One exception: a healthy reading
            // means the recovery finished, and Ok reports that. The job's
            // own guard reports the cancel at the next stage.
            if cancel.is_cancelled() {
                let detail = match &reading {
                    Ok(inventory) => match Self::confirm(inventory, components) {
                        RecoveryVerdict::Healthy => return Ok(()),
                        RecoveryVerdict::StillInRecovery(names)
                        | RecoveryVerdict::InProgress(names) => {
                            format!("components not yet healthy: {names:?}")
                        }
                    },
                    Err(error) => format!("inventory read failed: {}", error.message),
                };
                return Err(RmsError::cancelled(format!(
                    "recovery flow cancelled during the closing confirm; {detail}"
                )));
            }

            let inventory = reading?;
            in_progress = match Self::confirm(&inventory, components) {
                RecoveryVerdict::Healthy => return Ok(()),
                RecoveryVerdict::StillInRecovery(names) => {
                    return Err(RmsError::failed_precondition(format!(
                        "components still not healthy after the recovery flow: {names:?} (still in recovery state)"
                    )));
                }
                RecoveryVerdict::InProgress(names) => names,
            };

            tracing::info!(
                node = %device.name(),
                ?in_progress,
                read,
                re_reads_left = CONFIRM_RETRIES - read,
                "recovery still in progress for some components"
            );
        }
        Err(RmsError::failed_precondition(format!(
            "components still not healthy after the recovery flow: {in_progress:?} (still in progress after {CONFIRM_RETRIES} re-reads)"
        )))
    }
}
