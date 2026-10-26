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

use std::path::PathBuf;

/// Root directory for bundled Redfish fixture archives.
pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

/// Directory containing static Redfish mockup archives.
pub fn mockup_dir() -> PathBuf {
    fixtures_dir().join("mockup")
}

/// GB200 BMC plus HMC Redfish fixture archive used by RMS compute tests.
pub fn gb200_compute_archive() -> PathBuf {
    mockup_dir()
        .join("nvidia-gb200nvl-baseboard")
        .join("GB200NVL-BMC+HMC.tar.gz")
}

/// GB200 HMC-only Redfish fixture archive.
pub fn gb200_hmc_archive() -> PathBuf {
    mockup_dir()
        .join("nvidia-gb200nvl-baseboard")
        .join("GB200NVL-HMC.tar.gz")
}

/// Power shelf Redfish fixture archive used by RMS powershelf tests.
pub fn powershelf_archive() -> PathBuf {
    mockup_dir().join("nvidia-pmc.tar.gz")
}

/// Convert a fixture path into a lossy owned string for simulator setup.
pub fn path_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}
