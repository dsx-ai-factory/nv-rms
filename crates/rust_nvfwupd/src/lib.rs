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

//! Library surface for NVFWUPD firmware update workflows.

// Phase 1 exposes the existing NVFWUPD modules as a library without folding
// the pre-existing lint backlog into this crate split. Retire these allows as
// the library/async refactor touches the affected modules.
#![allow(
    dead_code,
    unused_assignments,
    unused_imports,
    unused_mut,
    unused_variables,
    clippy::borrowed_box,
    clippy::collapsible_if,
    clippy::collapsible_str_replace,
    clippy::derivable_impls,
    clippy::doc_overindented_list_items,
    clippy::double_ended_iterator_last,
    clippy::for_kv_map,
    clippy::if_same_then_else,
    clippy::iter_kv_map,
    clippy::len_zero,
    clippy::manual_div_ceil,
    clippy::match_like_matches_macro,
    clippy::needless_borrow,
    clippy::needless_borrows_for_generic_args,
    clippy::new_without_default,
    clippy::option_as_ref_deref,
    clippy::option_map_unit_fn,
    clippy::print_literal,
    clippy::print_with_newline,
    clippy::ptr_arg,
    clippy::question_mark,
    clippy::redundant_closure,
    clippy::redundant_pattern_matching,
    clippy::regex_creation_in_loops,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unnecessary_map_or,
    clippy::unnecessary_unwrap,
    clippy::upper_case_acronyms
)]

use std::sync::atomic::{AtomicBool, Ordering};

pub mod bmc_access;
pub mod cli_format;
pub mod cli_schema;
pub mod config_parser;
pub mod config_rftarget;
pub mod dgx_rftarget;
pub mod expected_inventory;
pub mod gb200_rftarget;
pub mod gb200_switch_rftarget;
pub mod gh200_rftarget;
pub mod gh_rftarget;
pub mod hgxb100_rftarget;
pub mod input_params;
pub mod ipmitool_api;
pub mod os_access;
pub mod pldm;
pub mod powershelf_rftarget;
pub mod rf_target;
pub(crate) mod ssh_options;
pub(crate) mod ssh_transport;
pub mod updcommand;
pub mod util;
pub mod utils;
pub mod version;
pub mod workflow;
pub mod workflow_api;

/// Re-exports of concrete target implementations used by CLI and integration callers.
pub mod targets {
    pub use crate::config_rftarget::ConfigRFTarget;
    pub use crate::dgx_rftarget::DGXRFTarget;
    pub use crate::gb200_rftarget::GB200RFTarget;
    pub use crate::gb200_switch_rftarget::GB200SwitchRFTarget;
    pub use crate::gh200_rftarget::GH200RFTarget;
    pub use crate::gh_rftarget::GHRFTarget;
    pub use crate::hgxb100_rftarget::{HGXB100RFTarget, HGXRUBINRFTarget};
    pub use crate::powershelf_rftarget::PowerShelfRFTarget;
}

/// Global flag used by long-running CLI workflows to observe Ctrl-C.
static EXIT_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Return whether a process-level exit has been requested.
pub fn check_exit_requested() -> bool {
    EXIT_REQUESTED.load(Ordering::SeqCst)
}

/// Set or clear the process-level exit request flag.
pub fn set_exit_requested(requested: bool) {
    EXIT_REQUESTED.store(requested, Ordering::SeqCst);
}
