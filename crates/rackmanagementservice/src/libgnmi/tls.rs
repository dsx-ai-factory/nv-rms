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
