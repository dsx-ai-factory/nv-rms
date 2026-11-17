/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-Apache-2.0
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// files in this directory except mod.rs are auto-generated and overwritten during the build by build.rs

#[allow(clippy::all)]
#[rustfmt::skip]
pub mod rack_manager {
    include!(concat!(env!("OUT_DIR"), "/rack_manager.rs"));
}

#[allow(clippy::all)]
#[rustfmt::skip]
/// Generated wire models and service bindings for RackManagerV2.
pub mod rack_manager_v2 {
    include!(concat!(env!("OUT_DIR"), "/rack_manager_v2.rs"));
}

#[cfg(feature = "client")]
mod client_generated {
    #[allow(clippy::all)]
    #[rustfmt::skip]
    pub mod rack_manager_client {
        include!(concat!(env!("OUT_DIR"), "/rack_manager_client.rs"));
    }

    #[allow(clippy::all)]
    #[rustfmt::skip]
    pub mod rack_manager_converters {
        include!(concat!(env!("OUT_DIR"), "/rack_manager_converters.rs"));
    }

    #[allow(clippy::all)]
    #[rustfmt::skip]
    pub mod rack_manager_v2_client {
        include!(concat!(env!("OUT_DIR"), "/rack_manager_v2_client.rs"));
    }

    #[allow(clippy::all)]
    #[rustfmt::skip]
    pub mod rack_manager_v2_converters {
        include!(concat!(env!("OUT_DIR"), "/rack_manager_v2_converters.rs"));
    }
}

#[cfg(feature = "client")]
pub use client_generated::{rack_manager_client, rack_manager_converters};

#[cfg(feature = "client")]
/// Generated high-level client wrapper for RackManagerV2.
pub use client_generated::rack_manager_v2_client;

#[cfg(feature = "client")]
/// Generated convenience conversions for RackManagerV2 request types.
pub use client_generated::rack_manager_v2_converters;
