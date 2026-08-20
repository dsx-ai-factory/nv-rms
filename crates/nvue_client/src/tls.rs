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

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::client::ClientError;

const STABLE_SNAPSHOT_ATTEMPTS: usize = 3;

/// Owned PEM paths and optional TLS authority for NVUE client authentication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientTls {
    ca_cert_path: PathBuf,
    client_cert_path: PathBuf,
    client_key_path: PathBuf,
}

impl ClientTls {
    /// Create client TLS configuration from three PEM paths.
    pub fn new(ca_cert_path: PathBuf, client_cert_path: PathBuf, client_key_path: PathBuf) -> Self {
        Self {
            ca_cert_path,
            client_cert_path,
            client_key_path,
        }
    }

    /// Borrow the configured PEM paths.
    pub fn as_paths(&self) -> ClientTlsPaths<'_> {
        ClientTlsPaths {
            ca_cert_path: &self.ca_cert_path,
            client_cert_path: &self.client_cert_path,
            client_key_path: &self.client_key_path,
        }
    }
}

/// Borrowed PEM paths for NVUE client authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientTlsPaths<'a> {
    /// CA certificate used to verify the NVUE server.
    pub ca_cert_path: &'a Path,

    /// Client certificate presented to the NVUE server.
    pub client_cert_path: &'a Path,

    /// Private key corresponding to the client certificate.
    pub client_key_path: &'a Path,
}

#[derive(Debug, PartialEq, Eq)]
struct RawTlsMaterial {
    ca_cert_pem: Vec<u8>,
    client_cert_pem: Vec<u8>,
    client_key_pem: Vec<u8>,
}

pub(crate) struct TlsSnapshot {
    pub(crate) ca_cert_pem: Vec<u8>,
    pub(crate) identity_pem: Vec<u8>,
    pub(crate) fingerprint: [u8; 32],
}

impl TlsSnapshot {
    fn from_raw(raw: RawTlsMaterial) -> Self {
        let RawTlsMaterial {
            ca_cert_pem,
            client_cert_pem,
            client_key_pem,
        } = raw;

        let mut hasher = Sha256::new();

        for pem in [&ca_cert_pem, &client_cert_pem, &client_key_pem] {
            hasher.update((pem.len() as u64).to_le_bytes());
            hasher.update(pem);
        }

        let fingerprint = hasher.finalize().into();
        let mut identity_pem = client_cert_pem;

        identity_pem.extend_from_slice(&client_key_pem);

        Self {
            ca_cert_pem,
            identity_pem,
            fingerprint,
        }
    }
}

/// Read a coherent generation from PEM files that may rotate independently.
pub(crate) async fn load_stable_snapshot(tls: &ClientTls) -> Result<TlsSnapshot, ClientError> {
    let mut previous = read_material(&tls.as_paths()).await?;

    for _ in 0..STABLE_SNAPSHOT_ATTEMPTS {
        let current = read_material(&tls.as_paths()).await?;

        if current == previous {
            return Ok(TlsSnapshot::from_raw(current));
        }

        previous = current;
    }

    Err(ClientError::UnstableTlsMaterial {
        attempts: STABLE_SNAPSHOT_ATTEMPTS,
    })
}

async fn read_material(paths: &ClientTlsPaths<'_>) -> Result<RawTlsMaterial, ClientError> {
    let ca_cert_pem = read_pem(paths.ca_cert_path, "CA certificate").await?;
    let client_cert_pem = read_pem(paths.client_cert_path, "client certificate").await?;
    let client_key_pem = read_pem(paths.client_key_path, "client key").await?;

    Ok(RawTlsMaterial {
        ca_cert_pem,
        client_cert_pem,
        client_key_pem,
    })
}

async fn read_pem(path: &Path, description: &'static str) -> Result<Vec<u8>, ClientError> {
    tokio::fs::read(path)
        .await
        .map_err(|source| ClientError::ReadTlsMaterial {
            description,
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_fingerprint_distinguishes_pem_boundaries() {
        let first = TlsSnapshot::from_raw(RawTlsMaterial {
            ca_cert_pem: b"a".to_vec(),
            client_cert_pem: b"bc".to_vec(),
            client_key_pem: b"d".to_vec(),
        });

        let second = TlsSnapshot::from_raw(RawTlsMaterial {
            ca_cert_pem: b"ab".to_vec(),
            client_cert_pem: b"c".to_vec(),
            client_key_pem: b"d".to_vec(),
        });

        assert_ne!(first.fingerprint, second.fingerprint);
    }
}
