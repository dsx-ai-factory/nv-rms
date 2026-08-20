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

mod instance;

pub mod compute_gb200_nvidia;
pub mod compute_gb200_wiwynn;
pub mod compute_gb300_lenovo;
pub mod compute_gb300_nvidia;
pub mod compute_gb300_supermicro;
pub mod compute_vrnvl72_nvidia;
pub mod nvfwupd_adapter;
pub mod powershelf_gb200_delta;
pub mod powershelf_gb200_liteon;
pub mod powershelf_gb300_delta;
pub mod powershelf_gb300_liteon;
pub mod switch_gb200_nvidia;
pub mod switch_gb300_nvidia;

pub(crate) use instance::{SwitchFirmwareManagement, SwitchScaleUpManagement};

pub use instance::NodeInstance;
pub use switch_gb200_nvidia::mtls as switch_mtls;
