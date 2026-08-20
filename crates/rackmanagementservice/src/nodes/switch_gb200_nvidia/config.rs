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

//! Shared configuration constants for NVIDIA GB200 switch management.
//!
//! Defines ports, limits, timeouts, and retry intervals used by switch
//! workflows and their API handlers.

pub const MIN_FIRMWARE_FILE_SIZE: u64 = 1024;
pub const SFTP_BUFFER_SIZE: usize = crate::transport::ssh::SFTP_UPLOAD_BUFFER_SIZE_BYTES;
pub const DEFAULT_JOB_TIMEOUT_SECONDS: u64 = 1800;
pub const DEFAULT_POLL_INTERVAL_SECONDS: u64 = 5;
/// Maximum time to wait for NMX Controller control-plane convergence.
pub const NMX_CONTROLLER_CONFIGURED_TIMEOUT_SECONDS: u64 = 3 * 60;
pub const DEFAULT_NVUE_PORT: u16 = 443;
pub const DEFAULT_VERIFY_SSL: bool = false;
pub const MAX_RETRY_ATTEMPTS: u32 = 3;
pub const RETRY_WAIT_SECONDS: u64 = 30;
pub const REVISION_POLL_INTERVAL_SECONDS: u64 = 1;
pub const REVISION_APPLY_TIMEOUT_SECONDS: u64 = 60;
pub const GNMI_CONFIG_WAIT_SECONDS: u64 = 15;
pub const GNMI_RETRY_DELAY_1_SECONDS: u64 = 20;
pub const GNMI_RETRY_DELAY_2_SECONDS: u64 = 30;
pub const GRPC_PORT_NMX_CONTROLLER: u16 = 9370;
pub const GRPC_PORT_NMX_TELEMETRY: u16 = 9352;
/// NVOS pauses gRPC while cluster manager actions run; wait before the next one.
pub const CLUSTER_MANAGER_ACTION_SETTLE_SECONDS: u64 = 5;
pub const VALID_COMPONENTS: &[&str] = &["bmc", "fpga", "erot", "cpld", "bios", "transceiver"];
