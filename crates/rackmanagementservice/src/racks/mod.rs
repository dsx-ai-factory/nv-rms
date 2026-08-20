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

pub mod nvl_gb;

/// Runtime rack trait object used by RMS inventory.
///
/// The domain `Rack` trait stays generic over its node handle; production RMS
/// binds that handle to the closed `NodeInstance` enum here.
pub type ManagedRack = dyn crate::domain::rack::Rack<Node = crate::nodes::NodeInstance>;
