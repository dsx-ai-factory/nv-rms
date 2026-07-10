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

//! Compatibility re-export for the SSH transport client.
//!
//! New code can import [`crate::transport::ssh`]. Existing call sites keep
//! using `transport::ssh_client` without refactoring.

/// SSH client, endpoint, and SFTP option types retained at the legacy path.
pub use super::ssh::{SftpUploadOptions, SshClient, SshEndpoint};
