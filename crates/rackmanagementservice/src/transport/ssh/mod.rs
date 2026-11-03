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

//! SSH, SCP, and SFTP transport support for switch workflows.
//!
//! The public surface is intentionally small: callers use
//! [`crate::transport::ssh::SshClient`] while this module keeps command
//! parsing, SCP and SFTP setup, password-recovery prompting, log redaction,
//! and error-code mapping private. ONIE recovery stages installers with SCP,
//! while NVOS system-image staging uses SFTP.

mod client;
mod error;
mod exec;
mod logging;
mod password;
mod request;
mod scp;
mod sftp;

#[cfg(test)]
mod tests;

/// SSH client, endpoint settings, and upload tunables used by switch workflows.
pub use client::{SSH_UPLOAD_BUFFER_SIZE_BYTES, SshClient, SshEndpoint, SshUploadOptions};

/// Strip a command from transport error text bound for a client.
pub(crate) use logging::strip_ssh_command_text;
