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

/// NVIDIA GB300 switch node.
///
/// GB300 switches currently use the GB200 switch implementation. The shared
/// implementation preserves the concrete `SwitchGb300Nvidia` node type from
/// `NodeConfig`, which lets NVFWUPD select the GB300 switch server type.
pub type SwitchGb300Nvidia = crate::nodes::switch_gb200_nvidia::SwitchGb200Nvidia;
