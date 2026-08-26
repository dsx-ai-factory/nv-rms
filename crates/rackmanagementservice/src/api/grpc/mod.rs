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
