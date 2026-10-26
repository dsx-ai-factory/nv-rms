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
