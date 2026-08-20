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
