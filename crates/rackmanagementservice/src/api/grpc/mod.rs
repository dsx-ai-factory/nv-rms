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

pub mod conversions;
pub mod firmware_handlers;
pub mod firmware_object_handlers;
pub mod inventory_handlers;
pub mod node_type_resolver;
pub mod power_handlers;
pub mod scaleupfabricmanager_handlers;
pub mod server;
pub mod switch_certificate_handlers;
pub mod switch_handlers;
pub mod switch_image_handlers;
pub mod switch_security_handlers;

pub(crate) mod artifact_download;
pub(crate) mod firmware_artifact_paths;
pub(crate) mod firmware_task_util;
pub(crate) mod node_recovery;

#[allow(clippy::all, unused_qualifications)]
pub mod proto {
    pub mod switch_client {
        tonic::include_proto!("switch_client");
    }

    pub mod gnmi {
        tonic::include_proto!("gnmi");
    }
}
