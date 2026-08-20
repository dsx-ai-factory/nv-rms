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
