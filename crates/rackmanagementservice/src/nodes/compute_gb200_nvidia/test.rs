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

use super::recovery::*;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio_util::sync::CancellationToken;

use crate::domain::node::{FirmwareInfo, FirmwareType, RecoveryBundles, RecoveryProfile};
use crate::utilities::error::{ErrorCode, Result, RmsError};

struct RecoveryMockNode {
    /// Inventories returned by `inventory`, one per call.
    /// The last entry repeats once the queue is empty.
    inventories: Mutex<VecDeque<Vec<FirmwareInfo>>>,
    /// How many times `inventory` was called, so a test can
    /// pin how many reads the confirm spent.
    inventory_reads: AtomicUsize,
    /// Ordered log of recovery operations. Each `push` records its file.
    /// Each `power_block` records
    /// `POWER_BLOCK`. Tests can then pin the between-pushes ordering
    /// the sequence requires, not just call counts.
    ops: Mutex<Vec<String>>,
    /// Error returned by the next `push` after the
    /// attempt is logged. `None` means `Ok(())`.
    push_error: Mutex<Option<RmsError>>,
    /// Error returned by the next `inventory` after the read is counted.
    inventory_error: Mutex<Option<RmsError>>,
    /// Error returned by the next `power_block` after the
    /// attempt is logged. `None` means `Ok(())`.
    power_block_error: Mutex<Option<RmsError>>,
    /// Presence list received by each `power_block` call, so a
    /// test can pin the wait-for list.
    power_block_presence: Mutex<Vec<Vec<String>>>,
    /// The job token the plan runs under. The mock cancels it from inside
    /// the operation named by `cancel_at`, so a test can pin which safe
    /// point stops the flow next.
    cancel: CancellationToken,
    cancel_at: Option<MockOp>,
}

/// The mock's operations, for `cancel_at`.
#[derive(PartialEq)]
enum MockOp {
    Inventory,
    Push,
    PowerBlock,
}

/// Marker `power_block` records into the mock's `ops` log.
const POWER_BLOCK: &str = "<power-block>";

impl RecoveryMockNode {
    fn with_inventory(inventory: Vec<FirmwareInfo>) -> Self {
        Self::with_inventories([inventory])
    }

    fn with_inventories(inventories: impl IntoIterator<Item = Vec<FirmwareInfo>>) -> Self {
        Self {
            inventories: Mutex::new(inventories.into_iter().collect()),
            inventory_reads: AtomicUsize::new(0),
            ops: Mutex::new(Vec::new()),
            push_error: Mutex::new(None),
            inventory_error: Mutex::new(None),
            power_block_error: Mutex::new(None),
            power_block_presence: Mutex::new(Vec::new()),
            cancel: CancellationToken::new(),
            cancel_at: None,
        }
    }

    /// Cancel the job token from inside every `op`, after it is logged.
    fn cancelling_at(mut self, op: MockOp) -> Self {
        self.cancel_at = Some(op);
        self
    }

    fn cancel_if_at(&self, op: MockOp) {
        if self.cancel_at == Some(op) {
            self.cancel.cancel();
        }
    }

    fn with_push_error(self, code: ErrorCode) -> Self {
        *self.push_error.lock().unwrap() = Some(RmsError::new(code, "push failed"));
        self
    }

    fn with_inventory_error(self, code: ErrorCode) -> Self {
        *self.inventory_error.lock().unwrap() = Some(RmsError::new(code, "read failed"));
        self
    }

    fn with_power_block_error(self, code: ErrorCode) -> Self {
        *self.power_block_error.lock().unwrap() = Some(RmsError::new(code, "power block failed"));
        self
    }
}

impl RecoveryDevice for RecoveryMockNode {
    fn name(&self) -> &str {
        "recovery-mock-node"
    }

    async fn inventory(&self) -> Result<Vec<FirmwareInfo>> {
        self.inventory_reads.fetch_add(1, Ordering::SeqCst);
        self.cancel_if_at(MockOp::Inventory);
        if let Some(error) = self.inventory_error.lock().unwrap().take() {
            return Err(error);
        }
        let mut inventories = self.inventories.lock().unwrap();
        if inventories.len() > 1 {
            Ok(inventories.pop_front().expect("len checked"))
        } else {
            Ok(inventories.front().cloned().unwrap_or_default())
        }
    }

    async fn power_block(&self, presence: &[String]) -> Result<()> {
        self.power_block_presence
            .lock()
            .unwrap()
            .push(presence.to_vec());
        self.ops.lock().unwrap().push(POWER_BLOCK.to_owned());
        self.cancel_if_at(MockOp::PowerBlock);
        match self.power_block_error.lock().unwrap().take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn push(&self, firmware_file: &str, _cancel: &CancellationToken) -> Result<()> {
        self.ops.lock().unwrap().push(firmware_file.to_owned());
        self.cancel_if_at(MockOp::Push);
        match self.push_error.lock().unwrap().take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

pub(super) fn hmc_entry(name: &str, health: &str, state: &str) -> FirmwareInfo {
    FirmwareInfo {
        name: name.to_owned(),
        version: "1.0.0".to_owned(),
        firmware_type: FirmwareType::HMC,
        updateable: true,
        target: String::new(),
        health: health.to_owned(),
        state: state.to_owned(),
        sku: String::new(),
    }
}

fn listed(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

fn bundles() -> RecoveryBundles {
    RecoveryBundles {
        regular: "regular.fwpkg".to_owned(),
        recovery: "recovery.fwpkg".to_owned(),
    }
}

pub(super) fn recovery_profile(value: serde_json::Value) -> RecoveryProfile {
    serde_json::from_value(value).expect("profile literal is a table")
}

/// The one roster table every test here and the node's `from_config`
/// test start from.
pub(super) fn gb200_roster_table() -> serde_json::Value {
    serde_json::json!({
        "presence": ["HGX_FW_BMC_0", "HGX_FW_ERoT_BMC_0"],
        "push_then_cycle": ["HGX_FW_FPGA_0", "HGX_FW_FPGA_1"],
        "push_only": ["HGX_FW_ERoT_CPU_0", "HGX_FW_GPU_0"],
    })
}

/// The roster the plan and sequence tests run against, built through
/// the production parser so its rules hold here too.
fn roster() -> RecoveryRoster {
    RecoveryRoster::parse(&recovery_profile(gb200_roster_table())).expect("roster table is valid")
}

/// The plan the sequence tests drive: the test roster, the detected
/// names, the two test bundle files.
fn plan(in_recovery: &[&str]) -> RecoveryPlan {
    RecoveryPlan::new(roster(), bundles(), &listed(in_recovery))
        .expect("sequence tests start from a non-empty detected list")
}

fn healthy_inventory() -> Vec<FirmwareInfo> {
    ["HGX_FW_FPGA_0", "HGX_FW_ERoT_CPU_0", "HGX_FW_GPU_0"]
        .iter()
        .map(|name| hmc_entry(name, "OK", "Enabled"))
        .collect()
}

#[test]
fn roster_watch_set_is_both_push_lists_trimmed_deduped_and_sorted() {
    let roster = RecoveryRoster::parse(&recovery_profile(serde_json::json!({
        "presence": ["HGX_FW_ERoT_BMC_0", "HGX_FW_BMC_0"],
        "push_then_cycle": [" HGX_FW_FPGA_1 ", "HGX_FW_FPGA_0", "HGX_FW_FPGA_1"],
        "push_only": ["HGX_FW_GPU_0", "HGX_FW_ERoT_CPU_0"],
    })))
    .unwrap();

    // push_then_cycle first, then push_only. Presence members are waited
    // for after the AC cycle, never watched.
    assert_eq!(
        roster.watch_set(),
        [
            "HGX_FW_FPGA_0",
            "HGX_FW_FPGA_1",
            "HGX_FW_ERoT_CPU_0",
            "HGX_FW_GPU_0"
        ]
    );
}

#[tokio::test]
async fn presence_list_reaches_the_power_block_trimmed_deduped_and_sorted() {
    let roster = RecoveryRoster::parse(&recovery_profile(serde_json::json!({
        "presence": [" HGX_FW_ERoT_BMC_0", "HGX_FW_BMC_0", "HGX_FW_BMC_0 "],
        "push_then_cycle": ["HGX_FW_FPGA_0"],
        "push_only": ["HGX_FW_ERoT_CPU_0"],
    })))
    .unwrap();
    let plan = RecoveryPlan::new(roster, bundles(), &listed(&["HGX_FW_FPGA_0"])).unwrap();
    let node = RecoveryMockNode::with_inventory(healthy_inventory());

    plan.run(&node, &node.cancel).await.unwrap();

    assert_eq!(
        *node.power_block_presence.lock().unwrap(),
        vec![listed(&["HGX_FW_BMC_0", "HGX_FW_ERoT_BMC_0"])]
    );
}

#[test]
fn roster_parse_rejects_each_rule_violation() {
    let mut unknown_field = gb200_roster_table();
    unknown_field["sma"] = serde_json::json!(["HGX_FW_GPU_SMA_0"]);
    let mut missing_field = gb200_roster_table();
    missing_field.as_object_mut().unwrap().remove("presence");
    let mut blank_name = gb200_roster_table();
    blank_name["push_only"] = serde_json::json!(["HGX_FW_GPU_0", "  "]);
    let mut empty_list = gb200_roster_table();
    empty_list["push_then_cycle"] = serde_json::json!([]);
    let mut list_overlap = gb200_roster_table();
    list_overlap["push_only"] = serde_json::json!(["HGX_FW_FPGA_1"]);
    let mut presence_overlap = gb200_roster_table();
    presence_overlap["presence"] = serde_json::json!(["HGX_FW_BMC_0", "HGX_FW_GPU_0"]);

    for (case, table) in [
        ("unknown field", unknown_field),
        ("missing field", missing_field),
        ("blank name", blank_name),
        ("empty list", empty_list),
        ("list overlap", list_overlap),
        ("presence overlap", presence_overlap),
    ] {
        let error = RecoveryRoster::parse(&recovery_profile(table)).expect_err(case);
        assert_eq!(error.code, ErrorCode::InvalidArgument, "{case}");
    }
}

#[test]
fn plan_is_none_when_nothing_needs_recovery() {
    assert!(RecoveryPlan::new(roster(), bundles(), &[]).is_none());
}

#[tokio::test]
async fn detected_names_outside_the_roster_are_ignored() {
    // Detection hands the flow the names it saw. Only roster members get a
    // push and a place in the closing confirm, so the unknown name is
    // neither pushed nor waited for.
    let node = RecoveryMockNode::with_inventory(healthy_inventory());

    plan(&["HGX_FW_FPGA_0", "HGX_FW_GPU_9"])
        .run(&node, &node.cancel)
        .await
        .unwrap();

    assert_eq!(
        *node.ops.lock().unwrap(),
        vec!["regular.fwpkg".to_owned(), POWER_BLOCK.to_owned()]
    );
}

#[tokio::test]
async fn full_recovery_runs_both_pushes_with_one_power_block_between() {
    let node = RecoveryMockNode::with_inventory(healthy_inventory());

    plan(&["HGX_FW_FPGA_0", "HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap();

    // The ordering is the invariant, the cycle after the regular push and
    // before the recovery push, so the single ordered log is the assert.
    assert_eq!(
        *node.ops.lock().unwrap(),
        vec![
            "regular.fwpkg".to_owned(),
            POWER_BLOCK.to_owned(),
            "recovery.fwpkg".to_owned(),
        ]
    );
    // The power block must wait for the roster's presence list.
    assert_eq!(
        *node.power_block_presence.lock().unwrap(),
        vec![listed(&["HGX_FW_BMC_0", "HGX_FW_ERoT_BMC_0"])]
    );
}

#[tokio::test]
async fn fpga_only_recovery_skips_the_recovery_push() {
    let node = RecoveryMockNode::with_inventory(healthy_inventory());

    plan(&["HGX_FW_FPGA_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap();

    assert_eq!(
        *node.ops.lock().unwrap(),
        vec!["regular.fwpkg".to_owned(), POWER_BLOCK.to_owned()]
    );
}

#[tokio::test]
async fn hidden_fpga1_gets_its_own_round_after_fpga0_recovers() {
    // FPGA1 reports through FPGA0, so it is invisible at detect time when
    // FPGA0 is broken. The re-read after the first push+cycle round must
    // discover it, run a second round, and cover it in the closing
    // confirm.
    let node = RecoveryMockNode::with_inventories([
        vec![
            hmc_entry("HGX_FW_FPGA_0", "OK", "Enabled"),
            hmc_entry("HGX_FW_FPGA_1", "Critical", "StandbyOffline"),
        ],
        vec![
            hmc_entry("HGX_FW_FPGA_0", "OK", "Enabled"),
            hmc_entry("HGX_FW_FPGA_1", "OK", "Enabled"),
        ],
    ]);

    plan(&["HGX_FW_FPGA_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap();

    assert_eq!(
        *node.ops.lock().unwrap(),
        vec![
            "regular.fwpkg".to_owned(),
            POWER_BLOCK.to_owned(),
            "regular.fwpkg".to_owned(),
            POWER_BLOCK.to_owned(),
        ]
    );
}

#[tokio::test]
async fn fpga_stuck_in_recovery_fails_the_confirm_after_capped_rounds() {
    // A round must make progress. When the FPGA never leaves recovery,
    // the rounds cap stops the loop and the closing confirm fails the
    // sequence instead of cycling the tray forever. The cap is the size
    // of the recheck list, two FPGAs here, so exactly two rounds run for
    // one stuck FPGA.
    let node = RecoveryMockNode::with_inventory(vec![hmc_entry(
        "HGX_FW_FPGA_0",
        "Critical",
        "StandbyOffline",
    )]);

    let error = plan(&["HGX_FW_FPGA_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert_eq!(
        *node.ops.lock().unwrap(),
        vec![
            "regular.fwpkg".to_owned(),
            POWER_BLOCK.to_owned(),
            "regular.fwpkg".to_owned(),
            POWER_BLOCK.to_owned(),
        ]
    );
}

#[tokio::test]
async fn failed_regular_push_stops_the_flow() {
    let node = RecoveryMockNode::with_inventory(Vec::new()).with_push_error(ErrorCode::Internal);

    let error = plan(&["HGX_FW_FPGA_0", "HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Internal, "the push error propagates");
    // The failed attempt is logged. Nothing may run after it: no power
    // block on a node whose FPGA push failed, no recovery push on top.
    assert_eq!(*node.ops.lock().unwrap(), vec!["regular.fwpkg".to_owned()]);
}

#[tokio::test]
async fn failed_power_block_stops_the_flow() {
    let node =
        RecoveryMockNode::with_inventory(Vec::new()).with_power_block_error(ErrorCode::Internal);

    let error = plan(&["HGX_FW_FPGA_0", "HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(
        error.code,
        ErrorCode::Internal,
        "the power block error propagates"
    );
    assert_eq!(
        *node.ops.lock().unwrap(),
        vec!["regular.fwpkg".to_owned(), POWER_BLOCK.to_owned()]
    );
}

#[tokio::test]
async fn recovery_signature_outranks_a_component_still_updating() {
    // One touched component still shows the signature while another is
    // still updating: the signature decides, at once, no re-read.
    let node = RecoveryMockNode::with_inventory(vec![
        hmc_entry("HGX_FW_ERoT_CPU_0", "Critical", "StandbyOffline"),
        hmc_entry("HGX_FW_GPU_0", "Warning", "Updating"),
    ]);

    let error = plan(&["HGX_FW_ERoT_CPU_0", "HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ok_alone_and_absence_count_as_in_progress() {
    // OK without Enabled is not healthy, and a touched component missing
    // from the inventory is in progress, not healthy: the confirm fails
    // closed on a component that vanished, unlike the detect step, where
    // absence means not-in-recovery. Both are re-read, then fail closed.
    let node =
        RecoveryMockNode::with_inventory(vec![hmc_entry("HGX_FW_GPU_0", "OK", "StandbyOffline")]);

    let error = plan(&["HGX_FW_ERoT_CPU_0", "HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert_eq!(
        node.inventory_reads.load(Ordering::SeqCst),
        1 + CONFIRM_RETRIES
    );
}

#[tokio::test]
async fn component_still_updating_after_the_push_is_re_read_until_healthy() {
    let node = RecoveryMockNode::with_inventories([
        vec![hmc_entry("HGX_FW_GPU_0", "Warning", "Updating")],
        vec![hmc_entry("HGX_FW_GPU_0", "Warning", "Updating")],
        vec![hmc_entry("HGX_FW_GPU_0", "OK", "Enabled")],
    ]);

    plan(&["HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap();

    // A push-only recovery: the recovery bundle, no power block.
    assert_eq!(*node.ops.lock().unwrap(), vec!["recovery.fwpkg".to_owned()]);
    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn cancel_during_a_round_stops_before_the_next_round() {
    // FPGA_0 still reads the signature after round one, so round two would
    // run. The token fires inside the re-read, and the next round is the
    // safe point that sees it.
    let node = RecoveryMockNode::with_inventory(vec![hmc_entry(
        "HGX_FW_FPGA_0",
        "Critical",
        "StandbyOffline",
    )])
    .cancelling_at(MockOp::Inventory);

    let error = plan(&["HGX_FW_FPGA_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(
        *node.ops.lock().unwrap(),
        vec!["regular.fwpkg".to_owned(), POWER_BLOCK.to_owned()]
    );
    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_during_a_round_push_stops_before_the_power_block() {
    let node = RecoveryMockNode::with_inventory(healthy_inventory()).cancelling_at(MockOp::Push);

    let error = plan(&["HGX_FW_FPGA_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(*node.ops.lock().unwrap(), vec!["regular.fwpkg".to_owned()]);
}

#[tokio::test]
async fn cancel_during_the_post_cycle_read_outranks_the_read_error() {
    let node = RecoveryMockNode::with_inventory(Vec::new())
        .with_inventory_error(ErrorCode::Unavailable)
        .cancelling_at(MockOp::Inventory);

    let error = plan(&["HGX_FW_FPGA_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(
        *node.ops.lock().unwrap(),
        vec!["regular.fwpkg".to_owned(), POWER_BLOCK.to_owned()]
    );
    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_during_the_power_block_stops_before_the_recovery_push() {
    let node =
        RecoveryMockNode::with_inventory(healthy_inventory()).cancelling_at(MockOp::PowerBlock);

    let error = plan(&["HGX_FW_FPGA_0", "HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Cancelled);
    // The round completes, its re-read runs, the recovery push does not.
    assert_eq!(
        *node.ops.lock().unwrap(),
        vec!["regular.fwpkg".to_owned(), POWER_BLOCK.to_owned()]
    );
    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_during_the_recovery_push_stops_before_the_confirm() {
    let node = RecoveryMockNode::with_inventory(healthy_inventory()).cancelling_at(MockOp::Push);

    let error = plan(&["HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(*node.ops.lock().unwrap(), vec!["recovery.fwpkg".to_owned()]);
    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancel_during_a_confirm_read_outranks_the_still_in_recovery_verdict() {
    // The read that saw the cancel also reads the recovery signature. The
    // job stopped because of the cancel, so the code says Cancelled; the
    // next detection finds the component again.
    let node = RecoveryMockNode::with_inventory(vec![hmc_entry(
        "HGX_FW_GPU_0",
        "Critical",
        "StandbyOffline",
    )])
    .cancelling_at(MockOp::Inventory);

    let error = plan(&["HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_during_a_confirm_read_outranks_the_read_error() {
    let node = RecoveryMockNode::with_inventory(Vec::new())
        .with_inventory_error(ErrorCode::Unavailable)
        .cancelling_at(MockOp::Inventory);

    let error = plan(&["HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Cancelled);
}

#[tokio::test]
async fn cancel_during_a_healthy_confirm_read_still_reports_the_recovery_done() {
    let node =
        RecoveryMockNode::with_inventory(healthy_inventory()).cancelling_at(MockOp::Inventory);

    plan(&["HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap();

    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_between_confirm_re_reads_stops_the_confirm() {
    // The component is still updating, so the confirm would re-read up to
    // CONFIRM_RETRIES more times. The token fires inside the first read.
    let node =
        RecoveryMockNode::with_inventory(vec![hmc_entry("HGX_FW_GPU_0", "Warning", "Updating")])
            .cancelling_at(MockOp::Inventory);

    let error = plan(&["HGX_FW_GPU_0"])
        .run(&node, &node.cancel)
        .await
        .unwrap_err();

    assert_eq!(error.code, ErrorCode::Cancelled);
    assert_eq!(node.inventory_reads.load(Ordering::SeqCst), 1);
}
