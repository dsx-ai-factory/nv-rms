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

/// Classification of async RMS workflows tracked by the job lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobType {
    FirmwareUpdate,
    SwitchCertificate,
    SwitchMtlsDisable,
    SwitchSdnFactoryDefaultReset,
    SwitchFactoryDefaultReset,
    SwitchSystemPasswordUpdate,
    SwitchSystemImageUpdate,
    ConfigureScaleUpFabricManagerV2,
}

impl JobType {
    pub const fn span_name(self) -> &'static str {
        match self {
            Self::FirmwareUpdate => "firmware_update",
            Self::SwitchCertificate => "switch_certificate",
            Self::SwitchMtlsDisable => "switch_mtls_disable",
            Self::SwitchSdnFactoryDefaultReset => "switch_sdn_factory_default_reset",
            Self::SwitchFactoryDefaultReset => "switch_factory_default_reset",
            Self::SwitchSystemPasswordUpdate => "switch_system_password_update",
            Self::SwitchSystemImageUpdate => "switch_system_image_update",
            Self::ConfigureScaleUpFabricManagerV2 => "configure_scale_up_fabric_manager_v2",
        }
    }

    /// Singular noun used in batch progress descriptions (e.g. "Batch
    /// {noun} in progress").
    pub(crate) const fn batch_noun(self) -> &'static str {
        match self {
            Self::FirmwareUpdate => "firmware update",
            Self::SwitchCertificate => "switch certificate install",
            Self::SwitchMtlsDisable => "switch mTLS disable",
            Self::SwitchSdnFactoryDefaultReset => "switch SDN factory-default reset",
            Self::SwitchFactoryDefaultReset => "switch factory-default reset",
            Self::SwitchSystemPasswordUpdate => "switch system password update",
            Self::SwitchSystemImageUpdate => "switch system image update",
            Self::ConfigureScaleUpFabricManagerV2 => "scale-up fabric configuration",
        }
    }

    /// Plural noun used in batch completion / failure summaries (e.g. "All N
    /// {noun} completed successfully").
    pub(crate) const fn batch_noun_plural(self) -> &'static str {
        match self {
            Self::FirmwareUpdate => "firmware updates",
            Self::SwitchCertificate => "switch certificate installs",
            Self::SwitchMtlsDisable => "switch mTLS disables",
            Self::SwitchSdnFactoryDefaultReset => "switch SDN factory-default resets",
            Self::SwitchFactoryDefaultReset => "switch factory-default resets",
            Self::SwitchSystemPasswordUpdate => "switch system password updates",
            Self::SwitchSystemImageUpdate => "switch system image updates",
            Self::ConfigureScaleUpFabricManagerV2 => "scale-up fabric configurations",
        }
    }
}
