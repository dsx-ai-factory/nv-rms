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

//! Ephemeral collector TLS identities shared by tracing tests.

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};

/// Ephemeral collector identity; tests install its CA only while building a client.
pub struct CollectorTls {
    pub ca_file: tempfile::NamedTempFile,
    pub certificate_pem: String,
    pub private_key_pem: String,
}

impl CollectorTls {
    pub fn new(hostname: &str, expired: bool) -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let mut server_params = CertificateParams::new(vec![hostname.to_string()]).unwrap();
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        if expired {
            server_params.not_before = rcgen::date_time_ymd(2000, 1, 1);
            server_params.not_after = rcgen::date_time_ymd(2001, 1, 1);
        }
        let server_key = KeyPair::generate().unwrap();
        let server_cert = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();
        let ca_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(ca_file.path(), ca.pem()).unwrap();
        Self {
            ca_file,
            certificate_pem: format!("{}{}", server_cert.pem(), ca.pem()),
            private_key_pem: server_key.serialize_pem(),
        }
    }

    pub fn identity(&self) -> tonic::transport::Identity {
        tonic::transport::Identity::from_pem(&self.certificate_pem, &self.private_key_pem)
    }
}
