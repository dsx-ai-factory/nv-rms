// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Artifact identity and materialization, shared by stored and inline objects.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use super::{FirmwareManifest, filename_from_location, manifest, nonempty};
use crate::api::grpc::artifact_download::{
    ArtifactDownload, ArtifactDownloadPolicy, ArtifactError, ArtifactSha256,
    build_artifact_http_client, download_artifact, is_valid_sha256_hex,
};
use crate::api::grpc::firmware_artifact_paths::{artifact_cache_path, redact_location_for_logging};

const DOWNLOAD_CONCURRENCY: usize = 8;
// Bump only for incompatible changes to the persisted catalog/identity contract,
// with an explicit migration or rejection policy for previous versions.
pub(super) const CATALOG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum Preparation {
    Preparing,
    Ready,
    Failed { message: String },
}

/// Only preparation results are persisted. Hardware targets are always resolved
/// from the original manifest for the requested canonical NodeType.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ArtifactCatalog {
    /// RMS storage format, not the input manifest's schema or firmware version.
    /// Objects without this marker, and unsupported versions, are rejected.
    pub(super) format_version: u32,
    pub(super) revision: String,
    pub(super) preparation: Preparation,
    pub(super) ready_artifacts: HashSet<String>,
}

impl ArtifactCatalog {
    pub(super) fn preparing(manifest: &FirmwareManifest) -> Result<Self, String> {
        Ok(Self {
            format_version: CATALOG_VERSION,
            revision: manifest_revision(manifest)?,
            preparation: Preparation::Preparing,
            ready_artifacts: HashSet::new(),
        })
    }
}

pub(super) fn manifest_revision(manifest: &FirmwareManifest) -> Result<String, String> {
    let bytes = serde_json::to_vec(manifest).map_err(|error| error.to_string())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[derive(Clone)]
pub(super) struct Artifact {
    pub(super) id: String,
    pub(super) filename: String,
    source: String,
    transport: String,
    sha256: Option<String>,
    pub(super) required: bool,
    pub(super) context: String,
}

impl Artifact {
    pub(super) fn from_location(
        revision: &str,
        context: String,
        location: &manifest::Location,
    ) -> Result<Self, String> {
        let source = nonempty(location.location.as_deref())
            .ok_or_else(|| format!("{context}.Location is required for an update"))?;
        let transport = nonempty(location.location_type.as_deref())
            .ok_or_else(|| format!("{context}.LocationType is required for an update"))?;
        let filename = advertised_filename(location)
            .ok_or_else(|| format!("{context} must specify a safe FileName or URL basename"))?;
        // Validate both path components before joining; never trust FileName.
        artifact_cache_path(Path::new(""), "artifact", &filename)
            .map_err(|_| format!("{context}.FileName must be a safe filename"))?;
        // Framed fields prevent concatenation collisions. Manifest revision and
        // full hierarchy path distinguish boards, channels and repeated names.
        // This per-reference identity is the persisted v1 contract. A shared
        // byte cache can deduplicate URLs separately; changing these IDs needs
        // a catalog migration/version policy, not a silent hash substitution.
        let identity = serde_json::to_vec(&(revision, &context, source, transport, &filename))
            .map_err(|error| error.to_string())?;
        Ok(Self {
            id: format!("{:x}", Sha256::digest(identity)),
            filename,
            source: source.to_owned(),
            transport: transport.to_owned(),
            sha256: location.sha256.clone(),
            required: location.required.unwrap_or(true),
            context,
        })
    }

    pub(super) fn cache_path(&self, cache: &Path) -> Result<PathBuf, String> {
        artifact_cache_path(cache, &self.id, &self.filename).map_err(|error| error.to_string())
    }

    fn is_http(&self) -> bool {
        matches!(
            self.transport.trim().to_ascii_lowercase().as_str(),
            "http" | "https" | "artifactory" | "jfrog"
        )
    }

    /// When SHA-256 is required, refuse HTTP(S) artifacts with no catalog digest
    /// and re-hash the cached file when a digest is present. Optional NVOS
    /// switch images use this path as well as required firmware payloads.
    pub(super) async fn verify_required_http_digest(
        &self,
        cache: &Path,
    ) -> Result<(), ArtifactError> {
        if !self.is_http() {
            return Ok(());
        }
        let Some(expected) = self.sha256.as_deref() else {
            return Err(ArtifactError::InvalidArgument(format!(
                "firmware artifact {} is missing a required Sha256 digest",
                redact_location_for_logging(&self.source)
            )));
        };
        if !is_valid_sha256_hex(expected) {
            return Err(ArtifactError::InvalidArgument(format!(
                "invalid expected sha256 for {}: expected 64 hex characters",
                self.filename
            )));
        }
        let path = self.cache_path(cache).map_err(ArtifactError::Cache)?;
        let actual = hash_cached_file(&path).await?;
        if !expected.eq_ignore_ascii_case(&actual) {
            return Err(ArtifactError::Cache(format!(
                "sha256 mismatch for {}: expected {expected}, got {actual}",
                path.display()
            )));
        }
        Ok(())
    }
}

async fn hash_cached_file(path: &Path) -> Result<String, ArtifactError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|source| ArtifactError::Io {
            context: format!("failed to open {} for hashing", path.display()),
            source,
        })?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buf)
            .await
            .map_err(|source| ArtifactError::Io {
                context: format!("failed to read {} for hashing", path.display()),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(super) fn advertised_filename(location: &manifest::Location) -> Option<String> {
    nonempty(location.file_name.as_deref())
        .map(str::to_owned)
        .or_else(|| {
            location
                .location
                .as_deref()
                .and_then(|source| filename_from_location(source).ok())
        })
}

/// A null/blank URL is advertised metadata, not a payload candidate. A present
/// URL still requires valid transport and filename metadata in `from_location`.
pub(super) fn has_download_location(location: &manifest::Location) -> bool {
    nonempty(location.location.as_deref()).is_some()
}

/// Deduplicate references before starting I/O and bound the number of in-flight
/// downloads. Only successful artifacts are returned; optional failures cannot
/// accidentally become executable targets.
pub(super) async fn materialize(
    artifacts: &[Artifact],
    cache: &Path,
    access_token: Option<&str>,
    require_sha256: bool,
    policy: &ArtifactDownloadPolicy,
    cancel: &CancellationToken,
) -> Result<HashSet<String>, ArtifactError> {
    let client = build_artifact_http_client(policy.clone())?;
    let unique: BTreeMap<_, _> = artifacts
        .iter()
        .map(|artifact| (artifact.id.clone(), artifact.clone()))
        .collect();
    let mut downloads = futures::stream::iter(unique.into_values().map(|artifact| {
        let client = &client;
        async move {
            let result = async {
                let destination = artifact
                    .cache_path(cache)
                    .map_err(ArtifactError::InvalidArgument)?;
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => Err(ArtifactError::Cancelled),
                    result = download_artifact(ArtifactDownload {
                        location: &artifact.source,
                        location_type: &artifact.transport,
                        component: &artifact.context,
                        bundle: None,
                        access_token,
                        sha256: ArtifactSha256::from_catalog(
                            artifact.sha256.clone(),
                            require_sha256,
                        ),
                    }, &destination, client) => result,
                }
            }
            .await;
            (artifact, result)
        }
    }))
    .buffer_unordered(DOWNLOAD_CONCURRENCY);
    let mut ready = HashSet::new();
    let mut failures = Vec::new();
    while let Some((artifact, result)) = downloads.next().await {
        match result {
            Ok(()) => {
                ready.insert(artifact.id.clone());
            }
            Err(error) if artifact.required => {
                tracing::error!(context = artifact.context, %error, "required artifact unavailable");
                failures.push(error.context(artifact.context));
            }
            Err(error) => {
                tracing::warn!(context = artifact.context, %error, "optional artifact unavailable")
            }
        }
    }
    if cancel.is_cancelled() {
        return Err(ArtifactError::Cancelled);
    }
    if failures.is_empty() {
        Ok(ready)
    } else {
        Err(ArtifactError::Multiple(failures))
    }
}

#[cfg(test)]
mod tests;
