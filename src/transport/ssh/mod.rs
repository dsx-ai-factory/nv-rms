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

//! SSH and SFTP transport support for switch workflows.
//!
//! The public surface is intentionally small: callers use
//! [`crate::transport::ssh::SshClient`] while this module keeps command
//! parsing, SFTP setup, password-recovery prompting, log redaction, and
//! error-code mapping private.

mod client;
mod error;
mod exec;
mod logging;
mod password;
mod request;
mod sftp;

#[cfg(test)]
mod tests;

/// SSH client, endpoint settings, and SFTP upload tunables used by switch workflows.
pub use client::{SFTP_UPLOAD_BUFFER_SIZE_BYTES, SftpUploadOptions, SshClient, SshEndpoint};
