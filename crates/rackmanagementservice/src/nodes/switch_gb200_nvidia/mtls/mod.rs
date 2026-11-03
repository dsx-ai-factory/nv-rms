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

//! mTLS certificate installation and NVUE configuration for NVIDIA GB200 switches.

mod nvue;
mod retention;
mod workflows;

pub(crate) use self::retention::{
    CertificateBindings, CertificateInventory, CertificateStore, NVOS_CERTIFICATE_STORE_LIMIT,
    plan_store_reclaim,
};

#[cfg(test)]
pub(crate) use self::retention::{ReportedUsage, StoreInventory};

pub(crate) use self::workflows::{
    GNMI_CLIENT_AUTHENTICATION_MTLS, GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY,
    MtlsSftpCopyOutcome, MtlsUnsetReport, MtlsUnsetStage, MtlsUnsetStatus, StableNmxcTlsConfig,
    mtls_remote_file_specs, sftp_copy_stage_name,
};

#[cfg(test)]
pub(crate) use self::workflows::{MtlsPersistOutcome, MtlsServiceOutcome, MtlsUnsetFailureSite};

pub use self::workflows::{
    SWITCH_CERTIFICATE_PRIVATE_KEY_PERMISSIONS, SWITCH_CERTIFICATE_PUBLIC_FILE_PERMISSIONS,
    SWITCH_CERTIFICATE_REMOTE_DIR_PERMISSIONS, SwitchMtlsMaterialPaths, SwitchMtlsService,
    cert_ids_for_domain,
};
