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

use std::path::Path;

/// Client TLS paths for gNMI gRPC mTLS (same on-disk layout as NMX-C/NVUE).
pub type GnmiTlsConfig = crate::libnmxc::NmxcTlsConfig;

pub fn tls_config_from_paths(
    ca_cert_path: &Path,
    client_cert_path: &Path,
    client_key_path: &Path,
    tls_authority: &str,
) -> GnmiTlsConfig {
    GnmiTlsConfig {
        ca_cert_path: Some(ca_cert_path.to_path_buf()),
        client_cert_path: Some(client_cert_path.to_path_buf()),
        client_key_path: Some(client_key_path.to_path_buf()),
        authority: Some(tls_authority.to_owned()),
    }
}
