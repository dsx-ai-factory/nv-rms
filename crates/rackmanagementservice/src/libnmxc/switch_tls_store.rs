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

use std::path::{Component, Path, PathBuf};

use super::NmxcTlsConfig;

const CA_FILE: &str = "ca.pem";
const CLIENT_CERT_FILE: &str = "client.pem";
const CLIENT_KEY_FILE: &str = "client.key";

/// Resolves per-domain mTLS material from a base directory.
///
/// Expected layout (same for switch installation material and RMS client TLS):
///
/// ```text
/// {base_dir}/{domain}/ca.pem
/// {base_dir}/{domain}/client.pem
/// {base_dir}/{domain}/client.key
/// ```
///
/// Under the switch-install root, the legacy `client.pem`/`client.key` names
/// hold the switch server identity. Under the RMS client root, they hold the
/// outbound RMS client identity.
///
/// [`TlsMaterialStore::resolve_for_nvlink_domain`] uses the NVLink domain only
/// for on-disk layout. Its separate DNS name, when provided, sets TLS server
/// name (SNI) authority; otherwise the transport uses its endpoint host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsMaterialStore {
    base_dir: PathBuf,
}

impl TlsMaterialStore {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Resolve material with the legacy behavior that also uses `domain` as
    /// TLS authority. New RMS switch paths should use
    /// [`Self::resolve_for_nvlink_domain`] to keep NVLink and DNS domains separate.
    pub fn resolve(&self, domain: &str) -> Result<NmxcTlsConfig, String> {
        let domain = normalize_domain(domain)?;
        let authority = Some(domain.clone());

        self.resolve_material(domain, authority)
    }

    /// Resolve material by NVLink domain and an independently configured DNS
    /// authority.
    pub fn resolve_for_nvlink_domain(
        &self,
        domain: &str,
        dns_domain: Option<&str>,
    ) -> Result<NmxcTlsConfig, String> {
        let domain = normalize_domain(domain)?;
        let authority = dns_domain.map(normalize_tls_server_name).transpose()?;

        self.resolve_material(domain, authority)
    }

    fn resolve_material(
        &self,
        domain: String,
        authority: Option<String>,
    ) -> Result<NmxcTlsConfig, String> {
        let dir = self.base_dir.join(&domain);
        if dir.as_path() == self.base_dir.as_path() {
            return Err(format!(
                "domain {domain:?} must not resolve to the TLS material root"
            ));
        }
        let ca = dir.join(CA_FILE);
        let client_cert = dir.join(CLIENT_CERT_FILE);
        let client_key = dir.join(CLIENT_KEY_FILE);

        for path in [&ca, &client_cert, &client_key] {
            if !path.is_file() {
                return Err(format!(
                    "TLS material for domain {domain:?} missing file {}",
                    path.display()
                ));
            }
        }

        Ok(NmxcTlsConfig {
            ca_cert_path: Some(ca),
            client_cert_path: Some(client_cert),
            client_key_path: Some(client_key),
            authority,
        })
    }
}

/// Validate an NVLink domain used as one TLS-material directory name.
pub fn normalize_domain(domain: &str) -> Result<String, String> {
    let domain = domain.trim();
    if domain.is_empty() {
        return Err("domain must not be empty".into());
    }

    match Path::new(domain)
        .components()
        .collect::<Vec<_>>()
        .as_slice()
    {
        [Component::Normal(label)] => {
            let label = label
                .to_str()
                .ok_or_else(|| "domain must be valid UTF-8".to_owned())?;
            if label.is_empty() {
                return Err("domain must not be empty".into());
            }
            Ok(label.to_owned())
        }
        [Component::CurDir] | [Component::ParentDir] => {
            Err("domain must not be '.' or '..'".into())
        }
        _ => Err("domain must be a single path component without separators".into()),
    }
}

/// Validate and trim the DNS name used for TLS SNI and certificate verification.
pub fn normalize_tls_server_name(name: &str) -> Result<String, String> {
    let name = name.trim().to_owned();
    rustls::pki_types::DnsName::try_from(name.clone())
        .map(|_| name)
        .map_err(|error| format!("invalid TLS DNS name: {error}"))
}

pub fn validate_domain(domain: &str) -> Result<(), String> {
    normalize_domain(domain).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_material(base: &Path, domain: &str) {
        let dir = base.join(domain);
        std::fs::create_dir_all(&dir).unwrap();
        for name in [CA_FILE, CLIENT_CERT_FILE, CLIENT_KEY_FILE] {
            std::fs::write(dir.join(name), b"placeholder").unwrap();
        }
    }

    #[test]
    fn resolve_finds_material_under_domain_subdirectory() {
        let base = tempfile::tempdir().unwrap();
        write_material(base.path(), "site-wide");

        let store = TlsMaterialStore::new(base.path());
        let tls = store.resolve_for_nvlink_domain("site-wide", None).unwrap();

        assert_eq!(tls.ca_cert_path, Some(base.path().join("site-wide/ca.pem")));

        assert_eq!(tls.authority, None);
    }

    #[test]
    fn resolve_rejects_missing_files() {
        let base = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(base.path().join("fabric-a")).unwrap();

        let store = TlsMaterialStore::new(base.path());
        let err = store
            .resolve_for_nvlink_domain("fabric-a", None)
            .unwrap_err();

        assert!(err.contains("missing file"));
    }

    #[test]
    fn resolve_uses_trimmed_domain_for_paths() {
        let base = tempfile::tempdir().unwrap();
        write_material(base.path(), "site-wide");

        let store = TlsMaterialStore::new(base.path());
        let tls = store
            .resolve_for_nvlink_domain(" site-wide ", None)
            .unwrap();

        assert_eq!(tls.ca_cert_path, Some(base.path().join("site-wide/ca.pem")));

        assert_eq!(tls.authority, None);
    }

    #[test]
    fn validate_domain_rejects_path_traversal() {
        assert!(validate_domain("../etc/passwd").is_err());
        assert!(validate_domain("switch/example.com").is_err());
        assert!(validate_domain("").is_err());
        assert!(validate_domain(".").is_err());
        assert!(validate_domain("..").is_err());
        assert!(validate_domain(" . ").is_err());
        assert!(validate_domain("fabric-a").is_ok());
    }

    #[test]
    fn nvlink_domain_is_not_validated_as_a_dns_name() {
        assert!(validate_domain("fabric..a").is_ok());
        assert!(normalize_tls_server_name("fabric..a").is_err());
    }

    #[test]
    fn resolve_rejects_dot_domain_even_when_base_has_material() {
        let base = tempfile::tempdir().unwrap();
        for name in [CA_FILE, CLIENT_CERT_FILE, CLIENT_KEY_FILE] {
            std::fs::write(base.path().join(name), b"placeholder").unwrap();
        }

        let store = TlsMaterialStore::new(base.path());
        let err = store.resolve_for_nvlink_domain(".", None).unwrap_err();
        assert!(err.contains("domain must not be '.'"));
    }

    #[test]
    fn resolve_preserves_legacy_domain_authority() {
        let base = tempfile::tempdir().unwrap();
        write_material(base.path(), "switch.example.com");

        let tls = TlsMaterialStore::new(base.path())
            .resolve("switch.example.com")
            .unwrap();

        assert_eq!(tls.authority.as_deref(), Some("switch.example.com"));
    }

    #[test]
    fn resolve_normalizes_dns_domain_authority() {
        let base = tempfile::tempdir().unwrap();
        write_material(base.path(), "site-wide");

        let store = TlsMaterialStore::new(base.path());

        for dns_domain in ["switch.example.com", " switch.example.com "] {
            let tls = store
                .resolve_for_nvlink_domain("site-wide", Some(dns_domain))
                .unwrap();

            assert_eq!(tls.ca_cert_path, Some(base.path().join("site-wide/ca.pem")));
            assert_eq!(tls.authority.as_deref(), Some("switch.example.com"));
        }
    }
}
