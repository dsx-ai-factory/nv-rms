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

//! mTLS certificate installation and NVUE configuration for NVIDIA GB200 switches.

mod nvue;
mod workflows;

pub(crate) use self::workflows::{
    StableNmxcTlsConfig, mtls_remote_file_specs, sftp_copy_stage_name,
};

pub use self::workflows::{
    SWITCH_CERTIFICATE_PRIVATE_KEY_PERMISSIONS, SWITCH_CERTIFICATE_PUBLIC_FILE_PERMISSIONS,
    SWITCH_CERTIFICATE_REMOTE_DIR_PERMISSIONS, SwitchMtlsMaterialPaths, SwitchMtlsService,
    cert_ids_for_domain,
};
