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

//! Firmware artifact download helpers for HTTP file servers, Artifactory, and local files.
//!
//! # Supported protocols
//!
//! ## HTTP / HTTPS file server
//!
//! HTTP and HTTPS use the same download path. There is no separate HTTPS mode — TLS is
//! negotiated automatically when the artifact URL uses the `https://` scheme.
//!
//! - **URL scheme:** `http://` (plain) or `https://` (TLS via `reqwest`/rustls)
//! - **`LocationType`:** `http`, `https`, or `HTTPS`
//! - **Auth:** none (plain GET); `access_token` is ignored
//!
//! ## Artifactory / JFrog
//!
//! - **URL scheme:** `http://`, `https://`
//! - **`LocationType`:** `artifactory`, `jfrog`
//! - **Auth:** API key via `access_token` (`X-JFrog-Art-Api` header), or none if absent/empty
//! - Authenticated downloads require HTTPS, including redirects. Credentials are
//!   never forwarded to another origin. Unauthenticated HTTP remains supported.
//!
//! ## Local file
//!
//! - **URL scheme:** bare filesystem path or `file://`
//! - **`LocationType`:** `file`
//! - **Auth:** none (local read/copy)
//!
//! # Not supported
//!
//! FTP, SFTP, TFTP, HTTP Basic/Digest, Bearer tokens (other than Artifactory's API-key header),
//! and non-local URL schemas such as `s3://`.
//!
//! # Integrity verification (optional SHA-256)
//!
//! These helpers are the download path for the **firmware manifest / firmware-object** flow:
//! RMS itself fetches artifacts declared in a firmware manifest into the staging/cache directory.
//! Because RMS moves the bytes, this is the correct place to verify their integrity.
//!
//! Each downloadable `Location` in the firmware manifest may carry an optional `Sha256` hex digest (see
//! [`super::firmware_object_handlers`]). That digest and the deployment
//! `require_sha256` flag are passed as [`ArtifactSha256`] to
//! [`download_artifact`] and:
//!
//! - **HTTP/Artifactory:** the response body is hashed while streaming to disk (single pass) and
//!   the download fails if the digest does not match — nothing is promoted into the cache.
//! - **Local file:** the copied bytes are hashed and verified before the file is published.
//! - **Cache reuse:** a cached file is re-hashed against the digest, so a matching cache is reused
//!   (and for HTTP the network HEAD is skipped) while a stale/tampered cache is re-downloaded.
//!
//! This catches transmission corruption, tampering on the artifact server, and MITM/content
//! substitution (independent of TLS, which matters when certificate verification is disabled).
//! It is an *integrity* check against a digest the firmware manifest is trusted to declare — not a signature,
//! and not a substitute for the device-side firmware signing enforced at flash time.
//!
//! When no digest is supplied the artifact is not hash-verified unless
//! [`ArtifactSha256`] was built with [`ArtifactSha256::required`]. Present
//! digests are always verified. Host allow-lists and private-target rejection
//! are applied to the initial URL and every redirect (at most 10 hops).
//! Scheme, allow-list, and IP literals are checked without DNS; hostnames are
//! resolved by a policy-aware DNS resolver that returns the checked sockets to
//! the HTTP connector.
//!
//! ## Contrast: bring-your-own-files
//!
//! The direct RPCs (`UpdateFirmware`, `BatchUpdateFirmware`, `PushSwitchFirmware`, direct
//! `ApplySwitchSystemImage`) do **not** flow through this module. There the caller stages files
//! into `--firmware-dir` itself; RMS only validates path safety
//! (`super::firmware_handlers::resolve_firmware_file`) and never fetches or hashes the bytes, so
//! content integrity is entirely the caller's responsibility.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_tempfile::TempFile;
use futures::StreamExt;
use reqwest::Method;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::artifact_download_policy::{ArtifactPolicyResolver, validate_artifact_http_url};
#[cfg(test)]
use super::firmware_artifact_paths::filename_from_location;
use super::firmware_artifact_paths::redact_location_for_logging;

mod error;
pub use super::artifact_download_policy::ArtifactDownloadPolicy;
pub(crate) use error::ArtifactError;

/// Wall clock for one artifact: every attempt, redirect, and retry wait share
/// it, so a retry never makes a download take longer than one attempt could.
const REMOTE_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
/// Attempts per HTTP(S) artifact, the first included. Waits of 2 s and 4 s
/// add 6 s of backoff to failures that return immediately. An unreachable
/// host can spend the 30 s connect timeout on each attempt, about 96 s total.
const ARTIFACT_DOWNLOAD_ATTEMPTS: u32 = 3;
/// Wait before the second attempt. It doubles before each later attempt.
const ARTIFACT_RETRY_BACKOFF: Duration = Duration::from_secs(2);
/// Matches reqwest's default redirect chain limit.
const MAX_ARTIFACT_REDIRECTS: usize = 10;
const FIRMWARE_TEMP_FILE_SUFFIX: &str = ".tmp";
const FIRMWARE_TEMP_FILE_MARKER: &str = ".rms.fw.";

/// HTTP client plus the deployment download policy applied to every hop.
#[derive(Clone, Debug)]
pub(crate) struct ArtifactHttpClient {
    pub(crate) client: reqwest::Client,
    policy: ArtifactDownloadPolicy,
    download_budget: Duration,
    retry_backoff: Duration,
}

#[cfg(test)]
impl ArtifactHttpClient {
    fn from_parts(client: reqwest::Client, policy: ArtifactDownloadPolicy) -> Self {
        Self {
            client,
            policy,
            download_budget: REMOTE_DOWNLOAD_TIMEOUT,
            retry_backoff: Duration::ZERO,
        }
    }
}

/// Build the bounded, non-redirecting client used by `send_artifact_request`.
pub(crate) fn build_artifact_http_client(
    policy: ArtifactDownloadPolicy,
) -> Result<ArtifactHttpClient, ArtifactError> {
    let resolver = Arc::new(ArtifactPolicyResolver::new(policy.clone()));
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(30))
        .timeout(REMOTE_DOWNLOAD_TIMEOUT)
        .dns_resolver(resolver)
        .build()
        .map_err(|e| ArtifactError::Cache(format!("failed to build HTTP client: {e}")))?;
    Ok(ArtifactHttpClient {
        client,
        policy,
        download_budget: REMOTE_DOWNLOAD_TIMEOUT,
        retry_backoff: ARTIFACT_RETRY_BACKOFF,
    })
}

/// Transport/auth profile selected from the firmware manifest `LocationType` and artifact URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactLocationKind {
    HttpFileServer,
    Artifactory,
    File,
}

/// HTTP authentication applied to artifact downloads.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ArtifactHttpAuth {
    None,
    ArtifactoryApiKey(String),
}

/// Resolved download target for one firmware artifact location.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ArtifactDownloadSource {
    Http { url: String, auth: ArtifactHttpAuth },
    File(PathBuf),
}

/// SHA-256 check for one firmware-object artifact download.
///
/// A present catalog digest is always verified. [`Self::required`] only makes
/// a digest mandatory for HTTP(S) sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactSha256 {
    expected: Option<String>,
    required: bool,
}

impl Default for ArtifactSha256 {
    /// No catalog digest and no requirement to supply one.
    fn default() -> Self {
        Self {
            expected: None,
            required: false,
        }
    }
}

impl ArtifactSha256 {
    /// Verifies `digest` when the catalog declared one, but does not require it.
    pub(crate) fn optional(digest: String) -> Self {
        Self {
            expected: Some(digest),
            required: false,
        }
    }

    /// Requires a digest for HTTP(S) artifacts. `None` is the fail-closed case
    /// when the catalog omitted `Sha256`.
    pub(crate) fn required(digest: Option<String>) -> Self {
        Self {
            expected: digest,
            required: true,
        }
    }

    pub(crate) fn from_catalog(digest: Option<String>, require: bool) -> Self {
        if require {
            Self::required(digest)
        } else {
            digest.map(Self::optional).unwrap_or_default()
        }
    }

    pub(crate) fn expected(&self) -> Option<&str> {
        self.expected.as_deref()
    }

    pub(crate) fn missing_required_digest(&self) -> bool {
        self.required && self.expected.is_none()
    }
}

/// Download one firmware artifact into `dest_dir`, using `access_token` when the
/// location type requires authenticated HTTP access.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn download_single_file(
    location: &str,
    location_type: &str,
    component: &str,
    bundle: Option<&str>,
    access_token: Option<&str>,
    expected_sha256: Option<&str>,
    dest_dir: PathBuf,
    http_client: &ArtifactHttpClient,
) -> std::result::Result<(), String> {
    let filename = filename_from_location(location).map_err(|e| e.to_string())?;
    download_artifact(
        ArtifactDownload {
            location,
            location_type,
            component,
            bundle,
            access_token,
            sha256: expected_sha256
                .map(str::to_owned)
                .map(ArtifactSha256::optional)
                .unwrap_or_default(),
        },
        &dest_dir.join(filename),
        http_client,
    )
    .await
    .map_err(|e| e.to_string())
}

pub(crate) struct ArtifactDownload<'a> {
    pub location: &'a str,
    pub location_type: &'a str,
    pub component: &'a str,
    pub bundle: Option<&'a str>,
    pub access_token: Option<&'a str>,
    pub sha256: ArtifactSha256,
}

/// Download to a caller-validated destination. The advertised filename need not
/// match a URL basename (for example, an authenticated `/download` endpoint).
pub(crate) async fn download_artifact(
    request: ArtifactDownload<'_>,
    destination: &Path,
    http_client: &ArtifactHttpClient,
) -> Result<(), ArtifactError> {
    let filename = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ArtifactError::InvalidArgument("artifact destination must have a filename".to_owned())
        })?;
    // Keep this crate-visible downloader safe for callers outside manifest parsing.
    if let Some(expected) = request.sha256.expected()
        && !is_valid_sha256_hex(expected)
    {
        return Err(ArtifactError::InvalidArgument(format!(
            "invalid expected sha256 for {}: expected {SHA256_HEX_LEN} hex characters",
            filename
        )));
    }
    let source = resolve_artifact_download_with_policy(
        request.location,
        request.location_type,
        request.access_token,
        &http_client.policy,
    )
    .map_err(ArtifactError::InvalidArgument)?;
    if request.sha256.missing_required_digest()
        && matches!(source, ArtifactDownloadSource::Http { .. })
    {
        return Err(ArtifactError::InvalidArgument(format!(
            "firmware artifact {} is missing a required Sha256 digest",
            redact_location_for_logging(request.location)
        )));
    }
    let dest_dir = destination.parent().ok_or_else(|| {
        ArtifactError::InvalidArgument("artifact destination must have a parent".to_owned())
    })?;
    tokio::fs::create_dir_all(&dest_dir)
        .await
        .map_err(|e| ArtifactError::Io {
            context: format!("failed to create cache directory {}", dest_dir.display()),
            source: e,
        })?;
    let dest_path = destination.to_path_buf();

    match source {
        ArtifactDownloadSource::Http { url, auth } => {
            download_http_file_with_backoff(
                http_client,
                &url,
                &request,
                &auth,
                &dest_path,
                filename,
            )
            .await
        }
        ArtifactDownloadSource::File(source_path) => {
            copy_local_file_to_cache(&source_path, &request, dest_path, filename).await
        }
    }
}

/// Retry a transient failure of one HTTP(S) artifact download. All attempts
/// and waits share one deadline. Safe to repeat: callers finish every download
/// before any device work starts, and a failed attempt publishes no partial
/// file to the cache.
async fn download_http_file_with_backoff(
    http_client: &ArtifactHttpClient,
    url: &str,
    request: &ArtifactDownload<'_>,
    auth: &ArtifactHttpAuth,
    dest_path: &Path,
    filename: &str,
) -> Result<(), ArtifactError> {
    let deadline = Instant::now() + http_client.download_budget;
    let mut attempt = 1;
    let mut first_failure = None;
    loop {
        let Err(error) = download_http_file(
            http_client,
            url,
            request,
            auth,
            dest_path,
            filename,
            deadline,
        )
        .await
        else {
            return Ok(());
        };

        let delay = http_client.retry_backoff * 2u32.pow(attempt - 1);
        let retry = error.is_transient()
            && attempt < ARTIFACT_DOWNLOAD_ATTEMPTS
            && Instant::now() + delay < deadline;
        if !retry {
            return Err(match first_failure {
                Some(first) => {
                    error.context(format!("after {attempt} attempts (first failure: {first})"))
                }
                None => error,
            });
        }

        first_failure.get_or_insert_with(|| error.to_string());
        tracing::warn!(
            component = %request.component,
            url = %redact_location_for_logging(url),
            attempt,
            max_attempts = ARTIFACT_DOWNLOAD_ATTEMPTS,
            retry_in_secs = delay.as_secs(),
            error = %error,
            "transient artifact download failure; retrying"
        );
        tokio::time::sleep(delay).await;
        attempt += 1;
    }
}

fn artifact_location_kind(
    location_type: &str,
) -> std::result::Result<ArtifactLocationKind, String> {
    match location_type.trim().to_ascii_lowercase().as_str() {
        "artifactory" | "jfrog" => Ok(ArtifactLocationKind::Artifactory),
        "file" => Ok(ArtifactLocationKind::File),
        "http" | "https" => Ok(ArtifactLocationKind::HttpFileServer),
        _ => Err(format!(
            "unsupported firmware artifact LocationType '{location_type}'"
        )),
    }
}

fn validate_location_type_matches_source(
    location_type: &str,
    kind: ArtifactLocationKind,
    source: &ArtifactDownloadSource,
) -> std::result::Result<(), String> {
    match (kind, source) {
        (ArtifactLocationKind::File, ArtifactDownloadSource::File(_))
        | (
            ArtifactLocationKind::Artifactory | ArtifactLocationKind::HttpFileServer,
            ArtifactDownloadSource::Http { .. },
        ) => Ok(()),
        (ArtifactLocationKind::File, ArtifactDownloadSource::Http { .. }) => Err(format!(
            "LocationType '{location_type}' requires a local file path, not an HTTP(S) URL"
        )),
        (
            ArtifactLocationKind::Artifactory | ArtifactLocationKind::HttpFileServer,
            ArtifactDownloadSource::File(_),
        ) => Err(format!(
            "LocationType '{location_type}' requires an HTTP(S) URL, not a local file path"
        )),
    }
}

#[cfg(test)]
fn resolve_artifact_download(
    location: &str,
    location_type: &str,
    access_token: Option<&str>,
) -> std::result::Result<ArtifactDownloadSource, String> {
    resolve_artifact_download_with_policy(
        location,
        location_type,
        access_token,
        &ArtifactDownloadPolicy::default(),
    )
}

fn resolve_artifact_download_with_policy(
    location: &str,
    location_type: &str,
    access_token: Option<&str>,
    _policy: &ArtifactDownloadPolicy,
) -> std::result::Result<ArtifactDownloadSource, String> {
    let access_token = optional_access_token(access_token);
    let kind = artifact_location_kind(location_type)?;

    let source = match reqwest::Url::parse(location) {
        Ok(url) => {
            let scheme = url.scheme();
            if scheme == "http"
                && kind == ArtifactLocationKind::Artifactory
                && access_token.is_some()
            {
                return Err("authenticated Artifactory downloads require HTTPS".to_owned());
            }
            match scheme {
                "http" | "https" => ArtifactDownloadSource::Http {
                    url: location.to_owned(),
                    auth: http_auth_for_location(kind, access_token),
                },
                "file" => url
                    .to_file_path()
                    .map(ArtifactDownloadSource::File)
                    .map_err(|_| {
                        format!(
                            "file artifact location must be a valid file URL: {}",
                            redact_location_for_logging(location)
                        )
                    })?,
                scheme => {
                    return Err(format!(
                        "unsupported firmware artifact location scheme '{scheme}': {}",
                        redact_location_for_logging(location)
                    ));
                }
            }
        }
        Err(_) if kind == ArtifactLocationKind::File || !location.contains("://") => {
            ArtifactDownloadSource::File(PathBuf::from(location))
        }
        Err(_) => {
            return Err("invalid firmware artifact location URL".to_owned());
        }
    };

    validate_location_type_matches_source(location_type, kind, &source)?;
    Ok(source)
}

fn optional_access_token(access_token: Option<&str>) -> Option<&str> {
    access_token.filter(|token| !token.trim().is_empty())
}

fn http_auth_for_location(
    kind: ArtifactLocationKind,
    access_token: Option<&str>,
) -> ArtifactHttpAuth {
    match kind {
        ArtifactLocationKind::Artifactory => access_token
            .map(|token| ArtifactHttpAuth::ArtifactoryApiKey(token.to_owned()))
            .unwrap_or(ArtifactHttpAuth::None),
        ArtifactLocationKind::HttpFileServer | ArtifactLocationKind::File => ArtifactHttpAuth::None,
    }
}

fn apply_artifact_http_auth(
    request: reqwest::RequestBuilder,
    auth: &ArtifactHttpAuth,
) -> reqwest::RequestBuilder {
    match auth {
        ArtifactHttpAuth::None => request,
        ArtifactHttpAuth::ArtifactoryApiKey(token) => request.header("X-JFrog-Art-Api", token),
    }
}

/// Follow at most ten redirects using a client with automatic redirects disabled.
/// Artifactory credentials stay on the original HTTPS origin for GET and HEAD.
/// Keeping the policy here lets authenticated and public requests share one
/// pooled client while selecting credential policy per request. Every hop,
/// and the response body, must finish before `deadline`.
async fn send_artifact_request(
    http_client: &ArtifactHttpClient,
    method: Method,
    url: &str,
    auth: &ArtifactHttpAuth,
    deadline: Instant,
) -> Result<reqwest::Response, ArtifactError> {
    let origin = reqwest::Url::parse(url)
        .map_err(|_| ArtifactError::InvalidArgument("invalid artifact URL".to_owned()))?;
    let mut current = origin.clone();
    for redirects in 0..=MAX_ARTIFACT_REDIRECTS {
        if !matches!(current.scheme(), "http" | "https")
            || (matches!(auth, ArtifactHttpAuth::ArtifactoryApiKey(_))
                && (current.scheme() != "https" || current.origin() != origin.origin()))
        {
            return Err(ArtifactError::RedirectPolicy(
                "authenticated artifact requests and redirects require the original HTTPS origin"
                    .to_owned(),
            ));
        }
        validate_artifact_http_url(&current, &http_client.policy).map_err(|error| {
            if redirects == 0 {
                error
            } else {
                // The caller's URL was already accepted; the server sent a
                // Location the deployment forbids.
                ArtifactError::RedirectPolicy(error.to_string())
            }
        })?;
        let response = apply_artifact_http_auth(
            http_client
                .client
                .request(method.clone(), current.clone())
                .timeout(deadline.saturating_duration_since(Instant::now())),
            auth,
        )
        .send()
        .await
        .map_err(|error| {
            ArtifactError::transport(
                format!(
                    "artifact request failed for {}",
                    redact_location_for_logging(current.as_str())
                ),
                error,
            )
        })?;
        // Only these statuses redirect GET/HEAD; 300 offers choices and 304 is
        // cache validation, not another download destination.
        if !matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
            return Ok(response);
        }
        let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
            return Ok(response);
        };
        if redirects == MAX_ARTIFACT_REDIRECTS {
            return Err(ArtifactError::RedirectPolicy(
                "artifact redirect limit exceeded".to_owned(),
            ));
        }
        current = location
            .to_str()
            .ok()
            .and_then(|location| current.join(location).ok())
            .ok_or_else(|| {
                ArtifactError::RedirectPolicy("invalid artifact redirect location".to_owned())
            })?;
    }
    unreachable!("redirect limit is checked before following the last response")
}

/// Streams the body of `response` into `temp_file`, updating a SHA-256 hasher along the way.
///
/// Returns the total number of bytes written on success.
///
/// On failure returns `(bytes_written_so_far, error_message)` so the caller can include a
/// partial byte count in structured error logs before discarding the temp file. Note that
/// `bytes_written_so_far` may under-report when a [`tokio::io::AsyncWriteExt::write_all`]
/// call fails mid-chunk, since partial chunk writes are not separately tracked.
async fn stream_response_to_temp_file(
    response: reqwest::Response,
    temp_file: &mut TempFile,
    temp_path: &Path,
    url: &str,
    expected_sha256: Option<&str>,
    expected_len: Option<u64>,
) -> Result<u64, (u64, ArtifactError)> {
    let mut total_written: u64 = 0;
    let mut hasher = expected_sha256.is_some().then(Sha256::new);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            (
                total_written,
                ArtifactError::transport("failed to read artifact response body", e),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        temp_file.write_all(&chunk).await.map_err(|e| {
            (
                total_written,
                ArtifactError::Io {
                    context: format!("failed to write {}", temp_path.display()),
                    source: e,
                },
            )
        })?;
        if let Some(hasher) = hasher.as_mut() {
            hasher.update(&chunk);
        }
        total_written += chunk.len() as u64;
    }

    temp_file.flush().await.map_err(|e| {
        (
            total_written,
            ArtifactError::Io {
                context: format!("failed to flush {}", temp_path.display()),
                source: e,
            },
        )
    })?;
    validate_download_size(url, expected_len, total_written).map_err(|e| (total_written, e))?;
    if let (Some(expected), Some(hasher)) = (expected_sha256, hasher) {
        let actual = hex::encode(hasher.finalize());
        if !sha256_matches(expected, &actual) {
            return Err((
                total_written,
                ArtifactError::Integrity(format!(
                    "sha256 mismatch for {}: expected {expected}, got {actual}",
                    redact_location_for_logging(url)
                )),
            ));
        }
    }
    Ok(total_written)
}

async fn download_http_file(
    http_client: &ArtifactHttpClient,
    url: &str,
    request: &ArtifactDownload<'_>,
    auth: &ArtifactHttpAuth,
    dest_path: &Path,
    filename: &str,
    deadline: Instant,
) -> Result<(), ArtifactError> {
    let expected_sha256 = request.sha256.expected();
    let ArtifactDownload {
        location_type,
        component,
        bundle,
        ..
    } = *request;
    if cached_firmware_file_valid(http_client, url, auth, dest_path, expected_sha256, deadline)
        .await?
    {
        return Ok(());
    }

    let safe_url = redact_location_for_logging(url);
    tracing::info!(
        component = %component,
        bundle = ?bundle,
        location_type = %location_type,
        url = %safe_url,
        path = %dest_path.display(),
        "downloading firmware object artifact"
    );
    let download_start = Instant::now();
    let response = send_artifact_request(http_client, Method::GET, url, auth, deadline)
        .await
        .map_err(|e| {
            tracing::debug!(
                component = %component,
                bundle = ?bundle,
                location_type = %location_type,
                url = %safe_url,
                path = %dest_path.display(),
                elapsed_secs = truncate_f64(download_start.elapsed().as_secs_f64(), 2),
                bytes_downloaded = 0u64,
                error = %e,
                "artifact download failed"
            );
            e
        })?;

    if !response.status().is_success() {
        let error = ArtifactError::HttpStatus {
            status: response.status(),
            url: safe_url.clone(),
        };
        tracing::debug!(
            component = %component,
            bundle = ?bundle,
            location_type = %location_type,
            url = %safe_url,
            path = %dest_path.display(),
            elapsed_secs = truncate_f64(download_start.elapsed().as_secs_f64(), 2),
            bytes_downloaded = 0u64,
            error = %error,
            "artifact download failed"
        );
        return Err(error);
    }

    let expected_len = response.content_length();
    if expected_len.is_none() {
        // A well-behaved artifact server should always declare Content-Length;
        // without it the post-stream size validation can only catch an empty
        // body. Log so operators can spot a misbehaving/streaming endpoint. The
        // download itself still fails gracefully on any write error below.
        tracing::warn!(
            component = %component,
            bundle = ?bundle,
            location_type = %location_type,
            url = %safe_url,
            "artifact response has no Content-Length"
        );
    }
    let mut temp_file = firmware_temp_file(dest_path, filename).await?;
    let temp_path = temp_file.file_path().clone();
    let write_result = stream_response_to_temp_file(
        response,
        &mut temp_file,
        &temp_path,
        url,
        expected_sha256,
        expected_len,
    )
    .await;

    let total_written = match write_result {
        Err((bytes_downloaded, error)) => {
            temp_file.drop_async().await;

            // bytes_downloaded may under-report for partial downloads if a write_all() fails.
            // In failure cases, we don't really care about the exact number of bytes written;
            // we'll be discarding the temporarily file anyways. What's more important is the
            // reason it failed.
            tracing::debug!(
                component = %component,
                bundle = ?bundle,
                location_type = %location_type,
                url = %safe_url,
                path = %dest_path.display(),
                elapsed_secs = truncate_f64(download_start.elapsed().as_secs_f64(), 2),
                bytes_downloaded,
                error = %error,
                "artifact download failed"
            );
            return Err(error);
        }
        Ok(total_written) => total_written,
    };

    match temp_file.persist(dest_path).await {
        Ok(_) => {
            tracing::info!(
                component = %component,
                bundle = ?bundle,
                location_type = %location_type,
                url = %safe_url,
                path = %dest_path.display(),
                elapsed_secs = truncate_f64(download_start.elapsed().as_secs_f64(), 2),
                bytes_downloaded = total_written,
                "successfully downloaded artifact"
            );
            Ok(())
        }
        Err(e) => {
            if let Err(cleanup_error) = tokio::fs::remove_file(&e.path).await {
                tracing::warn!(
                    path = %e.path.display(),
                    error = %cleanup_error,
                    "failed to remove temporary firmware artifact after persist failure"
                );
            }
            let error = ArtifactError::Cache(format!(
                "failed to move {} to {}: {}",
                e.path.display(),
                dest_path.display(),
                e.error
            ));
            tracing::debug!(
                component = %component,
                bundle = ?bundle,
                location_type = %location_type,
                url = %safe_url,
                path = %dest_path.display(),
                elapsed_secs = truncate_f64(download_start.elapsed().as_secs_f64(), 2),
                bytes_downloaded = total_written,
                error = %error,
                "artifact download failed"
            );
            Err(error)
        }
    }
}

async fn firmware_temp_file(dest_path: &Path, filename: &str) -> Result<TempFile, ArtifactError> {
    let dest_dir = dest_path.parent().ok_or_else(|| {
        ArtifactError::Cache(format!(
            "cache path has no parent directory: {}",
            dest_path.display()
        ))
    })?;
    TempFile::builder()
        .prefix(firmware_temp_file_prefix(filename))
        .suffix(firmware_temp_file_suffix(std::process::id()))
        .dir(dest_dir)
        .create()
        .await
        .map_err(|e| {
            ArtifactError::Cache(format!(
                "failed to create firmware temp file in {}: {e}",
                dest_dir.display()
            ))
        })
}

fn firmware_temp_file_prefix(filename: &str) -> String {
    format!(".{filename}.")
}

fn firmware_temp_file_suffix(pid: u32) -> String {
    format!("{FIRMWARE_TEMP_FILE_MARKER}{pid}{FIRMWARE_TEMP_FILE_SUFFIX}")
}

#[cfg(test)]
fn firmware_temp_file_name(filename: &str, id: uuid::Uuid, pid: u32) -> String {
    format!(
        "{}{}{}",
        firmware_temp_file_prefix(filename),
        id,
        firmware_temp_file_suffix(pid)
    )
}

fn firmware_temp_file_pid(name: &OsStr) -> Option<u32> {
    let name = name.to_str()?;
    let without_suffix = name.strip_suffix(FIRMWARE_TEMP_FILE_SUFFIX)?;
    let without_dot = without_suffix.strip_prefix('.')?;
    let (artifact_and_id, pid) = without_dot.rsplit_once(FIRMWARE_TEMP_FILE_MARKER)?;
    let (artifact_name, id) = artifact_and_id.rsplit_once('.')?;
    if artifact_name.is_empty() || uuid::Uuid::parse_str(id).is_err() {
        return None;
    }
    pid.parse().ok()
}

#[cfg(test)]
fn is_firmware_temp_file_name(name: &OsStr) -> bool {
    firmware_temp_file_pid(name).is_some()
}

pub(crate) async fn reap_firmware_temp_files(
    firmware_dir: &Path,
) -> std::result::Result<(), String> {
    let mut dirs = vec![firmware_dir.to_path_buf()];
    let mut removed = 0usize;

    while let Some(dir) = dirs.pop() {
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(format!(
                    "failed to read firmware directory {}: {e}",
                    dir.display()
                ));
            }
        };

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| format!("failed to read firmware directory {}: {e}", dir.display()))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| format!("failed to inspect firmware path {}: {e}", path.display()))?;
            if file_type.is_dir() {
                dirs.push(path);
                continue;
            }
            let Some(pid) = firmware_temp_file_pid(&entry.file_name()) else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }

            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    removed += 1;
                    tracing::info!(
                        path = %path.display(),
                        creator_pid = pid,
                        "removed stale firmware temp file"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        creator_pid = pid,
                        error = %e,
                        "failed to remove stale firmware temp file"
                    );
                }
            }
        }
    }

    if removed > 0 {
        tracing::info!(removed, "reaped stale firmware temp files");
    }

    Ok(())
}

async fn copy_local_file_to_cache(
    source_path: &Path,
    request: &ArtifactDownload<'_>,
    dest_path: PathBuf,
    filename: &str,
) -> Result<(), ArtifactError> {
    let expected_sha256 = request.sha256.expected();
    let ArtifactDownload {
        location: source_label,
        location_type,
        component,
        bundle,
        ..
    } = *request;
    let metadata = tokio::fs::metadata(source_path)
        .await
        .map_err(|source| ArtifactError::Io {
            context: format!("failed to stat local artifact {}", source_path.display()),
            source,
        })?;
    if !metadata.is_file() {
        return Err(ArtifactError::InvalidArgument(format!(
            "local artifact is not a file: {}",
            source_path.display(),
        )));
    }
    validate_download_size(source_label, Some(metadata.len()), metadata.len())?;
    if cached_local_file_valid(source_path, &metadata, &dest_path, expected_sha256).await? {
        return Ok(());
    }
    tracing::info!(
        component, bundle, location_type, source = %source_path.display(),
        path = %dest_path.display(), "copying local firmware object artifact"
    );

    // Copy through open handles, not tokio::fs::copy: its detached blocking task
    // could recreate a pathname after cancellation has removed it. TempFile
    // owns cleanup on every error/drop and uses the startup reaper's marker.
    let mut temp_file = firmware_temp_file(&dest_path, filename).await?;
    let mut source_file =
        tokio::fs::File::open(source_path)
            .await
            .map_err(|source| ArtifactError::Io {
                context: format!("failed to open local artifact {}", source_path.display()),
                source,
            })?;
    let mut copied = 0u64;
    let mut hasher = expected_sha256.map(|_| Sha256::new());
    let mut buffer = vec![0u8; SHA256_BUF_SIZE];
    loop {
        let read = source_file
            .read(&mut buffer)
            .await
            .map_err(ArtifactError::from)
            .map_err(|error| {
                error.context(format!(
                    "failed to read local artifact {}",
                    source_path.display()
                ))
            })?;
        if read == 0 {
            break;
        }
        temp_file
            .write_all(&buffer[..read])
            .await
            .map_err(ArtifactError::from)
            .map_err(|error| error.context("failed to write local artifact".to_owned()))?;
        if let Some(hasher) = hasher.as_mut() {
            hasher.update(&buffer[..read]);
        }
        copied += read as u64;
    }
    temp_file
        .flush()
        .await
        .map_err(|source| ArtifactError::Io {
            context: "failed to flush local artifact".to_owned(),
            source,
        })?;
    validate_download_size(source_label, Some(metadata.len()), copied)?;
    if let (Some(expected), Some(hasher)) = (expected_sha256, hasher) {
        let actual = format!("{:x}", hasher.finalize());
        if !sha256_matches(expected, &actual) {
            return Err(ArtifactError::Integrity(format!(
                "sha256 mismatch for {source_label}: expected {expected}, got {actual}"
            )));
        }
    }
    temp_file.persist(&dest_path).await.map(|_| ()).map_err(|error| {
        // persist transfers ownership even on failure.
        if let Err(cleanup) = std::fs::remove_file(&error.path) {
            tracing::warn!(path = %error.path.display(), error = %cleanup, "failed to remove local artifact temp file");
        }
        ArtifactError::Cache(format!("failed to publish local artifact {}: {}", dest_path.display(), error.error))
    })
}
async fn cached_firmware_file_valid(
    http_client: &ArtifactHttpClient,
    url: &str,
    auth: &ArtifactHttpAuth,
    dest_path: &Path,
    expected_sha256: Option<&str>,
    deadline: Instant,
) -> Result<bool, ArtifactError> {
    let metadata = match tokio::fs::metadata(dest_path).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(ArtifactError::Io {
                context: format!("failed to stat {}", dest_path.display()),
                source,
            });
        }
    };
    if !metadata.is_file() {
        return Err(ArtifactError::Cache(format!(
            "cached path is not a file: {}",
            dest_path.display()
        )));
    }
    if metadata.len() == 0 {
        remove_invalid_cached_file(dest_path).await?;
        return Ok(false);
    }

    // When the firmware manifest advertises a digest, hashing the cached file is both the
    // strongest validation and cheaper than a network HEAD round-trip.
    if let Some(expected) = expected_sha256 {
        let actual = file_sha256(dest_path).await?;
        if sha256_matches(expected, &actual) {
            return Ok(true);
        }
        tracing::warn!(
            path = %dest_path.display(),
            expected,
            actual,
            "cached firmware artifact sha256 mismatch; removing"
        );
        remove_invalid_cached_file(dest_path).await?;
        return Ok(false);
    }

    // Allow-list misses and disallowed IP literals are visible without DNS.
    // A hostname that resolves to a disallowed address fails the HEAD below.
    if let Ok(parsed) = reqwest::Url::parse(url)
        && validate_artifact_http_url(&parsed, &http_client.policy).is_err()
    {
        return Ok(reuse_cached_artifact_after_policy_rejection(dest_path, url));
    }

    match remote_content_length(http_client, url, auth, deadline).await {
        Ok(Some(expected)) if expected == metadata.len() => Ok(true),
        Ok(Some(expected)) => {
            tracing::warn!(
                path = %dest_path.display(),
                expected,
                actual = metadata.len(),
                "cached firmware artifact size mismatch; removing"
            );
            remove_invalid_cached_file(dest_path).await?;
            Ok(false)
        }
        Ok(None) => Ok(true),
        Err(e) if e.to_string().contains("disallowed address") => {
            Ok(reuse_cached_artifact_after_policy_rejection(dest_path, url))
        }
        // No size to compare, as with a missing Content-Length: a brief
        // outage must not discard a complete artifact.
        Err(e) if e.is_transient() => {
            tracing::warn!(
                path = %dest_path.display(),
                error = %e,
                "cannot validate cached firmware artifact size; reusing it"
            );
            Ok(true)
        }
        Err(e) => {
            tracing::warn!(
                path = %dest_path.display(),
                error = %e,
                "failed to validate cached firmware artifact size; removing"
            );
            remove_invalid_cached_file(dest_path).await?;
            Ok(false)
        }
    }
}

fn reuse_cached_artifact_after_policy_rejection(dest_path: &Path, url: &str) -> bool {
    tracing::info!(
        path = %dest_path.display(),
        url = %redact_location_for_logging(url),
        "reusing cached firmware artifact; origin is no longer allowed by download policy"
    );
    true
}

async fn cached_local_file_valid(
    source_path: &Path,
    source_metadata: &std::fs::Metadata,
    dest_path: &Path,
    expected_sha256: Option<&str>,
) -> Result<bool, ArtifactError> {
    let metadata = match tokio::fs::metadata(dest_path).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(ArtifactError::Io {
                context: format!("failed to stat {}", dest_path.display()),
                source,
            });
        }
    };
    if !metadata.is_file() {
        return Err(ArtifactError::Cache(format!(
            "cached path is not a file: {}",
            dest_path.display()
        )));
    }

    // A configured digest is authoritative; fall back to a size check otherwise.
    if let Some(expected) = expected_sha256 {
        if metadata.len() > 0 {
            let actual = file_sha256(dest_path).await?;
            if sha256_matches(expected, &actual) {
                return Ok(true);
            }
            tracing::warn!(
                path = %dest_path.display(),
                source = %source_path.display(),
                expected,
                actual,
                "cached local firmware artifact sha256 mismatch; recopying"
            );
        }
        remove_invalid_cached_file(dest_path).await?;
        return Ok(false);
    }

    if metadata.len() == source_metadata.len() && metadata.len() > 0 {
        return Ok(true);
    }

    tracing::warn!(
        path = %dest_path.display(),
        source = %source_path.display(),
        expected = source_metadata.len(),
        actual = metadata.len(),
        "cached local firmware artifact size mismatch; recopying"
    );
    remove_invalid_cached_file(dest_path).await?;
    Ok(false)
}

async fn remote_content_length(
    http_client: &ArtifactHttpClient,
    url: &str,
    auth: &ArtifactHttpAuth,
    deadline: Instant,
) -> Result<Option<u64>, ArtifactError> {
    let response = send_artifact_request(http_client, Method::HEAD, url, auth, deadline).await?;
    if !response.status().is_success() {
        return Err(ArtifactError::HttpStatus {
            status: response.status(),
            url: redact_location_for_logging(url),
        });
    }
    Ok(response.content_length())
}

/// Truncates `value` to `decimals` decimal places without rounding.
///
/// Used to keep floating-point log fields (e.g. `elapsed_secs`) compact and
/// consistent — a 600-second download timeout does not need nanosecond
/// precision in a structured log.
fn truncate_f64(value: f64, decimals: u32) -> f64 {
    let multiplier = 10_f64.powi(decimals as i32);
    (value * multiplier).trunc() / multiplier
}

fn validate_download_size(
    url: &str,
    expected_len: Option<u64>,
    actual_len: u64,
) -> Result<(), ArtifactError> {
    if actual_len == 0 {
        return Err(ArtifactError::Integrity(format!(
            "download returned zero bytes: {}",
            redact_location_for_logging(url)
        )));
    }
    if let Some(expected_len) = expected_len
        && expected_len != actual_len
    {
        return Err(ArtifactError::Integrity(format!(
            "download size mismatch for {}: expected {expected_len} bytes, got {actual_len}",
            redact_location_for_logging(url)
        )));
    }
    Ok(())
}

async fn remove_invalid_cached_file(dest_path: &Path) -> Result<(), ArtifactError> {
    tokio::fs::remove_file(dest_path)
        .await
        .map_err(|source| ArtifactError::Io {
            context: format!(
                "failed to remove invalid cache file {}",
                dest_path.display()
            ),
            source,
        })
}

/// SHA-256 digest length as a lowercase hex string (32 bytes -> 64 hex chars).
const SHA256_HEX_LEN: usize = 64;

/// Buffer size allocated for reading a file's SHA-256 sum.
const SHA256_BUF_SIZE: usize = 64 * 1024; // 64 KiB

/// Returns true when `value` is a well-formed SHA-256 hex digest (64 hex chars).
pub(crate) fn is_valid_sha256_hex(value: &str) -> bool {
    value.len() == SHA256_HEX_LEN && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn sha256_matches(expected: &str, actual: &str) -> bool {
    expected.eq_ignore_ascii_case(actual)
}

/// Streams `path` through SHA-256 and returns the lowercase hex digest.
async fn file_sha256(path: &Path) -> Result<String, ArtifactError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|source| ArtifactError::Io {
            context: format!("failed to open {} for hashing", path.display()),
            source,
        })?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; SHA256_BUF_SIZE];
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

#[cfg(test)]
mod tests {
    use super::*;

    const ARTIFACT_BYTES: &[u8] = b"firmware payload";

    fn test_http_client() -> ArtifactHttpClient {
        test_http_client_with_policy(ArtifactDownloadPolicy::default())
    }

    fn test_http_client_with_policy(policy: ArtifactDownloadPolicy) -> ArtifactHttpClient {
        ArtifactHttpClient {
            retry_backoff: Duration::ZERO,
            ..build_artifact_http_client(policy).expect("artifact HTTP client")
        }
    }

    fn test_deadline() -> Instant {
        Instant::now() + REMOTE_DOWNLOAD_TIMEOUT
    }

    async fn assert_cached_artifact(dest_dir: &Path, filename: &str) {
        let cached = tokio::fs::read(dest_dir.join(filename))
            .await
            .expect("read cached artifact");
        assert_eq!(cached, ARTIFACT_BYTES);
    }

    async fn assert_no_temp_artifacts(dest_dir: &Path) {
        let mut entries = match tokio::fs::read_dir(dest_dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => panic!("read cache dir {}: {e}", dest_dir.display()),
        };
        while let Some(entry) = entries.next_entry().await.expect("read cache entry") {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                !(name.starts_with('.') && name.ends_with(".tmp")),
                "unexpected temp artifact left behind: {}",
                entry.path().display()
            );
        }
    }

    #[test]
    fn firmware_temp_file_name_matches_only_rms_download_temp_files() {
        let id = uuid::Uuid::parse_str("01890f4d-9c0b-7cc3-98c4-dc0c0c07398f").unwrap();
        let name = firmware_temp_file_name("artifact.bin", id, 4242);

        assert_eq!(
            name,
            ".artifact.bin.01890f4d-9c0b-7cc3-98c4-dc0c0c07398f.rms.fw.4242.tmp"
        );
        assert_eq!(firmware_temp_file_pid(OsStr::new(&name)), Some(4242));
        assert!(is_firmware_temp_file_name(OsStr::new(&name)));
        assert!(!is_firmware_temp_file_name(OsStr::new(".artifact.bin.tmp")));
        assert!(!is_firmware_temp_file_name(OsStr::new(
            ".artifact.bin.01890f4d-9c0b-7cc3-98c4-dc0c0c07398f.tmp"
        )));
        assert!(!is_firmware_temp_file_name(OsStr::new(
            "artifact.bin.01890f4d-9c0b-7cc3-98c4-dc0c0c07398f.tmp"
        )));
        assert!(!is_firmware_temp_file_name(OsStr::new(
            ".artifact.bin.not-a-uuid.rms.fw.4242.tmp"
        )));
        assert!(!is_firmware_temp_file_name(OsStr::new(
            ".artifact.bin.01890f4d-9c0b-7cc3-98c4-dc0c0c07398f.rms.fw.not-a-pid.tmp"
        )));
        assert!(!is_firmware_temp_file_name(OsStr::new(
            ".01890f4d-9c0b-7cc3-98c4-dc0c0c07398f.rms.fw.4242.tmp"
        )));
    }

    #[tokio::test]
    async fn firmware_temp_file_is_deleted_when_scope_exits() {
        let temp = tempfile::tempdir().expect("tempdir");
        let dest_path = temp.path().join("artifact.bin");
        let temp_path;

        {
            let temp_file = firmware_temp_file(&dest_path, "artifact.bin")
                .await
                .expect("create firmware temp file");
            temp_path = temp_file.file_path().clone();
            let name = temp_path
                .file_name()
                .expect("temp file name")
                .to_string_lossy();

            assert_eq!(temp_path.parent().unwrap(), temp.path());
            assert!(name.starts_with(".artifact.bin."), "name was {name}");
            assert!(
                name.ends_with(&firmware_temp_file_suffix(std::process::id())),
                "name was {name}"
            );
            assert_eq!(
                firmware_temp_file_pid(OsStr::new(name.as_ref())),
                Some(std::process::id())
            );
            assert!(temp_path.exists(), "temp file should exist while owned");
        }

        assert!(
            !temp_path.exists(),
            "async-tempfile should delete owned temp file when it leaves scope"
        );
    }

    #[tokio::test]
    async fn reap_firmware_temp_files_removes_only_rms_temp_files_recursively() {
        let temp = tempfile::tempdir().expect("tempdir");
        let firmware_dir = temp.path().join("firmware");
        let nested_dir = firmware_dir.join("firmware_objects").join("object-1");
        tokio::fs::create_dir_all(&nested_dir)
            .await
            .expect("create nested firmware cache dir");

        let stale_root = firmware_dir.join(firmware_temp_file_name(
            "root.bin",
            uuid::Uuid::parse_str("01890f4d-9c0b-7cc3-98c4-dc0c0c07398f").unwrap(),
            4242,
        ));
        let stale_nested = nested_dir.join(firmware_temp_file_name(
            "nested.bin",
            uuid::Uuid::parse_str("01890f4d-9c0b-7cc3-98c4-dc0c0c073990").unwrap(),
            4243,
        ));
        let unrelated_hidden_tmp = nested_dir.join(".not-rms.tmp");
        let old_style_hidden_tmp =
            nested_dir.join(".nested.bin.01890f4d-9c0b-7cc3-98c4-dc0c0c073991.tmp");
        let unrelated_uuid_without_prefix =
            nested_dir.join("nested.bin.01890f4d-9c0b-7cc3-98c4-dc0c0c073991.tmp");
        let final_artifact = nested_dir.join("nested.bin");

        for path in [
            &stale_root,
            &stale_nested,
            &unrelated_hidden_tmp,
            &old_style_hidden_tmp,
            &unrelated_uuid_without_prefix,
            &final_artifact,
        ] {
            tokio::fs::write(path, b"artifact")
                .await
                .expect("write test file");
        }

        reap_firmware_temp_files(&firmware_dir)
            .await
            .expect("reap temp files");

        assert!(!stale_root.exists(), "root temp file should be reaped");
        assert!(!stale_nested.exists(), "nested temp file should be reaped");
        assert!(
            unrelated_hidden_tmp.exists(),
            "unrelated hidden tmp should remain"
        );
        assert!(
            old_style_hidden_tmp.exists(),
            "old-style hidden uuid tmp should remain"
        );
        assert!(
            unrelated_uuid_without_prefix.exists(),
            "non-hidden tmp should remain"
        );
        assert!(final_artifact.exists(), "final artifact should remain");
    }

    #[test]
    fn resolve_artifact_download_supports_http_file_server_without_auth() {
        assert_eq!(
            resolve_artifact_download("https://example.test/fw.fwpkg", "http", None).unwrap(),
            ArtifactDownloadSource::Http {
                url: "https://example.test/fw.fwpkg".to_owned(),
                auth: ArtifactHttpAuth::None,
            }
        );
    }

    #[test]
    fn resolve_artifact_download_ignores_access_token_for_http_file_server() {
        assert_eq!(
            resolve_artifact_download(
                "https://example.test/fw.fwpkg",
                "http",
                Some("ignored-token")
            )
            .unwrap(),
            ArtifactDownloadSource::Http {
                url: "https://example.test/fw.fwpkg".to_owned(),
                auth: ArtifactHttpAuth::None,
            }
        );
    }

    #[test]
    fn resolve_artifact_download_supports_artifactory_auth() {
        assert_eq!(
            resolve_artifact_download(
                "https://artifactory.example.test/fw.fwpkg",
                "artifactory",
                Some("secret-token")
            )
            .unwrap(),
            ArtifactDownloadSource::Http {
                url: "https://artifactory.example.test/fw.fwpkg".to_owned(),
                auth: ArtifactHttpAuth::ArtifactoryApiKey("secret-token".to_owned()),
            }
        );
    }

    #[test]
    fn resolve_artifact_download_artifactory_without_token_has_no_auth() {
        assert_eq!(
            resolve_artifact_download(
                "https://artifactory.example.test/fw.fwpkg",
                "artifactory",
                None
            )
            .unwrap(),
            ArtifactDownloadSource::Http {
                url: "https://artifactory.example.test/fw.fwpkg".to_owned(),
                auth: ArtifactHttpAuth::None,
            }
        );
    }

    #[test]
    fn resolve_artifact_download_supports_local_file_paths() {
        let local_path = PathBuf::from("/tmp/fw.fwpkg");
        assert_eq!(
            resolve_artifact_download("/tmp/fw.fwpkg", "file", None).unwrap(),
            ArtifactDownloadSource::File(local_path.clone())
        );

        let file_url = reqwest::Url::from_file_path(&local_path)
            .unwrap()
            .to_string();
        assert_eq!(
            resolve_artifact_download(&file_url, "file", None).unwrap(),
            ArtifactDownloadSource::File(local_path)
        );
    }

    #[tokio::test]
    async fn download_single_file_fetches_artifact_over_http_file_server_without_auth() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            Some("ignored-token"),
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("http download should succeed");

        let requests = server.received_requests().await.expect("wiremock requests");
        assert_eq!(requests.len(), 1);
        assert!(
            !requests[0]
                .headers
                .keys()
                .any(|name| name.as_str().eq_ignore_ascii_case("x-jfrog-art-api")),
            "HTTP file server downloads must not send Artifactory auth header"
        );

        assert_cached_artifact(&dest_dir, "artifact.bin").await;
    }

    #[tokio::test]
    async fn authenticated_http_is_rejected_before_sending_any_request() {
        let server = wiremock::MockServer::start().await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        let error = download_single_file(
            &url,
            "artifactory",
            "BMC",
            None,
            Some("secret-token"),
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .expect_err("credentials must not be sent over plaintext HTTP");
        assert!(error.contains("require HTTPS"));
        assert!(server.received_requests().await.unwrap().is_empty());
        assert!(!dest_dir.exists());
    }

    #[tokio::test]
    async fn apply_artifact_http_auth_omits_artifactory_header_for_http_file_server() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"firmware"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let url = format!("{}/artifact.bin", server.uri());
        apply_artifact_http_auth(client.request(Method::GET, &url), &ArtifactHttpAuth::None)
            .send()
            .await
            .expect("artifact request without auth should succeed");

        let requests = server.received_requests().await.expect("wiremock requests");
        assert_eq!(requests.len(), 1);
        assert!(
            !requests[0]
                .headers
                .keys()
                .any(|name| name.as_str().eq_ignore_ascii_case("x-jfrog-art-api")),
            "HTTP file server downloads must not send Artifactory auth header"
        );
    }

    #[tokio::test]
    async fn apply_artifact_http_auth_sends_artifactory_header_when_configured() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .and(header("X-JFrog-Art-Api", "secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"firmware"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let url = format!("{}/artifact.bin", server.uri());
        apply_artifact_http_auth(
            client.request(Method::GET, &url),
            &ArtifactHttpAuth::ArtifactoryApiKey("secret-token".to_owned()),
        )
        .send()
        .await
        .expect("artifact request with Artifactory auth should succeed");

        let requests = server.received_requests().await.expect("wiremock requests");
        assert_eq!(requests.len(), 1);
    }

    #[test]
    fn validate_download_size_rejects_zero_and_length_mismatch() {
        assert!(validate_download_size("https://example.test/fw.bin", Some(4), 4).is_ok());
        assert!(validate_download_size("https://example.test/fw.bin", None, 4).is_ok());
        assert!(validate_download_size("https://example.test/fw.bin", Some(4), 0).is_err());
        assert!(validate_download_size("https://example.test/fw.bin", Some(4), 3).is_err());
    }

    #[tokio::test]
    async fn download_single_file_copies_local_file_to_cache() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("GBHC01A_01.05.0.fwpkg");
        let dest_dir = temp.path().join("cache");
        tokio::fs::write(&source_path, b"firmware payload")
            .await
            .unwrap();

        let client = test_http_client();
        download_single_file(
            source_path.to_string_lossy().as_ref(),
            "file",
            "HMC",
            Some("GBHC01A"),
            None,
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .unwrap();

        let cached = tokio::fs::read(dest_dir.join("GBHC01A_01.05.0.fwpkg"))
            .await
            .unwrap();
        assert_eq!(cached, b"firmware payload");
    }

    #[test]
    fn resolve_artifact_download_rejects_file_location_type_with_http_url() {
        let err =
            resolve_artifact_download("https://example.test/fw.fwpkg", "file", None).unwrap_err();
        assert!(
            err.contains("LocationType 'file' requires a local file path"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_artifact_download_rejects_http_location_type_with_local_path() {
        let err = resolve_artifact_download("/tmp/fw.fwpkg", "http", None).unwrap_err();
        assert!(
            err.contains("LocationType 'http' requires an HTTP(S) URL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_artifact_download_rejects_artifactory_location_type_with_file_url() {
        let local_path = PathBuf::from("/tmp/fw.fwpkg");
        let file_url = reqwest::Url::from_file_path(&local_path)
            .unwrap()
            .to_string();
        let err = resolve_artifact_download(&file_url, "artifactory", None).unwrap_err();
        assert!(
            err.contains("LocationType 'artifactory' requires an HTTP(S) URL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_artifact_download_rejects_s3_scheme() {
        let err = resolve_artifact_download("s3://bucket/fw.bin", "http", None).unwrap_err();
        assert!(err.contains("unsupported firmware artifact location scheme 's3'"));
    }

    #[test]
    fn resolve_artifact_download_rejects_ftps_location_type() {
        let err = resolve_artifact_download("https://files.example.test/fw.bin", "ftps", None)
            .unwrap_err();
        assert!(
            err.contains("unsupported firmware artifact LocationType 'ftps'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn download_single_file_http_reports_404() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/missing.bin"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let url = format!("{}/missing.bin", server.uri());
        let client = test_http_client();
        let err = download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            temp.path().join("cache"),
            &client,
        )
        .await
        .unwrap_err();
        assert!(err.contains("404"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn download_single_file_http_reports_401() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/protected.bin"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let url = format!("{}/protected.bin", server.uri());
        let client = test_http_client();
        let err = download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            temp.path().join("cache"),
            &client,
        )
        .await
        .unwrap_err();
        assert!(err.contains("401"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn download_single_file_http_rejects_empty_body() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/empty.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes([] as [u8; 0]))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        let url = format!("{}/empty.bin", server.uri());
        let client = test_http_client();
        let err = download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .unwrap_err();
        assert!(err.contains("zero bytes"), "unexpected error: {err}");
        assert_no_temp_artifacts(&dest_dir).await;
    }

    #[tokio::test]
    async fn download_single_file_http_redownloads_after_cache_size_mismatch() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        tokio::fs::write(dest_dir.join("artifact.bin"), b"stale")
            .await
            .unwrap();

        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("redownload after cache mismatch");

        let requests = server.received_requests().await.expect("wiremock requests");
        assert!(
            requests
                .iter()
                .any(|request| request.method.to_string() == "GET"),
            "size mismatch should trigger a fresh GET"
        );
        assert_cached_artifact(&dest_dir, "artifact.bin").await;
    }

    #[tokio::test]
    async fn download_single_file_http_redownloads_after_head_validation_failure() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(405))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        tokio::fs::write(dest_dir.join("artifact.bin"), ARTIFACT_BYTES)
            .await
            .unwrap();

        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("redownload after HEAD failure");

        let requests = server.received_requests().await.expect("wiremock requests");
        assert!(
            requests
                .iter()
                .filter(|request| request.method.to_string() == "GET")
                .count()
                >= 1,
            "HEAD failure should trigger a fresh GET"
        );
    }

    #[tokio::test]
    async fn a_transient_size_check_failure_keeps_the_cached_artifact() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"replacement".as_slice()))
            .expect(0)
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        tokio::fs::write(dest_dir.join("artifact.bin"), ARTIFACT_BYTES)
            .await
            .unwrap();

        let url = format!("{}/artifact.bin", server.uri());
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            dest_dir.clone(),
            &test_http_client(),
        )
        .await
        .expect("the cached artifact is reused");

        assert_cached_artifact(&dest_dir, "artifact.bin").await;
        server.verify().await;
    }

    #[tokio::test]
    async fn a_head_timeout_keeps_the_cached_artifact_without_a_get() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(1)))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"replacement"))
            .expect(0)
            .mount(&server)
            .await;
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("artifact.bin");
        tokio::fs::write(&destination, ARTIFACT_BYTES)
            .await
            .unwrap();
        let client = ArtifactHttpClient {
            download_budget: Duration::from_millis(100),
            ..test_http_client()
        };

        download_single_file(
            &format!("{}/artifact.bin", server.uri()),
            "http",
            "BMC",
            None,
            None,
            None,
            temp.path().to_path_buf(),
            &client,
        )
        .await
        .expect("a HEAD timeout must not discard a complete cached file");

        assert_cached_artifact(temp.path(), "artifact.bin").await;
        server.verify().await;
    }

    #[tokio::test]
    async fn download_single_file_local_file_fails_when_missing() {
        let temp = tempfile::tempdir().unwrap();
        let client = test_http_client();
        let err = download_single_file(
            "/tmp/does-not-exist-rms-artifact.fwpkg",
            "file",
            "BMC",
            None,
            None,
            None,
            temp.path().join("cache"),
            &client,
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("failed to stat local artifact"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn download_single_file_local_file_recopies_after_source_changes() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("local-fw.bin");
        let dest_dir = temp.path().join("cache");
        tokio::fs::write(&source_path, b"v1").await.unwrap();

        let client = test_http_client();
        download_single_file(
            source_path.to_string_lossy().as_ref(),
            "file",
            "BMC",
            None,
            None,
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .unwrap();

        tokio::fs::write(&source_path, b"version-two-longer")
            .await
            .unwrap();
        download_single_file(
            source_path.to_string_lossy().as_ref(),
            "file",
            "BMC",
            None,
            None,
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .unwrap();

        let cached = tokio::fs::read(dest_dir.join("local-fw.bin"))
            .await
            .unwrap();
        assert_eq!(cached, b"version-two-longer");
    }

    fn expected_digest(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    #[test]
    fn is_valid_sha256_hex_accepts_only_64_hex_chars() {
        assert!(is_valid_sha256_hex(&"a".repeat(64)));
        assert!(is_valid_sha256_hex(&"A".repeat(64)));
        assert!(!is_valid_sha256_hex(&"a".repeat(63)));
        assert!(!is_valid_sha256_hex(&"a".repeat(65)));
        assert!(!is_valid_sha256_hex(&"g".repeat(64)));
        assert!(!is_valid_sha256_hex(""));
    }

    #[tokio::test]
    async fn download_single_file_http_verifies_matching_sha256() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        // Upper-cased digest exercises the case-insensitive comparison.
        let digest = expected_digest(ARTIFACT_BYTES).to_ascii_uppercase();
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            Some(&digest),
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("matching sha256 should succeed");
        assert_cached_artifact(&dest_dir, "artifact.bin").await;
    }

    #[tokio::test]
    async fn download_single_file_http_rejects_sha256_mismatch() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        let wrong = "0".repeat(64);
        let err = download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            Some(&wrong),
            dest_dir.clone(),
            &client,
        )
        .await
        .unwrap_err();
        assert!(err.contains("sha256 mismatch"), "unexpected error: {err}");
        assert!(
            !dest_dir.join("artifact.bin").exists(),
            "artifact failing sha256 verification must not be cached"
        );
        assert_no_temp_artifacts(&dest_dir).await;
    }

    #[tokio::test]
    async fn download_single_file_rejects_malformed_expected_sha256() {
        let temp = tempfile::tempdir().expect("tempdir");
        let client = test_http_client();
        let err = download_single_file(
            "https://example.test/artifact.bin",
            "http",
            "BMC",
            None,
            None,
            Some("not-a-valid-digest"),
            temp.path().join("cache"),
            &client,
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("invalid expected sha256"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn download_artifact_requires_sha256_when_configured() {
        let temp = tempfile::tempdir().expect("tempdir");
        let dest = temp.path().join("cache/artifact.bin");
        let client = test_http_client();
        let err = download_artifact(
            ArtifactDownload {
                location: "https://example.test/artifact.bin",
                location_type: "http",
                component: "BMC",
                bundle: None,
                access_token: None,
                sha256: ArtifactSha256::required(None),
            },
            &dest,
            &client,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("missing a required Sha256 digest"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn download_artifact_retries_transient_status_and_read_timeout() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        for first_response in [
            ResponseTemplate::new(503),
            ResponseTemplate::new(200).set_delay(Duration::from_secs(2)),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/artifact.bin"))
                .respond_with(first_response)
                .up_to_n_times(1)
                .with_priority(1)
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/artifact.bin"))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
                .expect(1)
                .mount(&server)
                .await;

            // A read timeout can fire before the shared download deadline,
            // just as an OS-level connection timeout can during a transfer.
            let client = ArtifactHttpClient {
                client: reqwest::Client::builder()
                    .no_proxy()
                    .read_timeout(Duration::from_millis(250))
                    .build()
                    .unwrap(),
                download_budget: Duration::from_secs(5),
                ..test_http_client()
            };
            let temp = tempfile::tempdir().expect("tempdir");
            let dest_dir = temp.path().join("cache");
            let url = format!("{}/artifact.bin", server.uri());
            download_single_file(
                &url,
                "http",
                "BMC",
                None,
                None,
                None,
                dest_dir.clone(),
                &client,
            )
            .await
            .expect("a transient failure with budget left must be retried");

            assert_cached_artifact(&dest_dir, "artifact.bin").await;
            assert_no_temp_artifacts(&dest_dir).await;
            server.verify().await;
        }
    }

    #[tokio::test]
    async fn download_artifact_does_not_retry_a_permanent_status() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(404))
            .up_to_n_times(1)
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .expect(0)
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let url = format!("{}/artifact.bin", server.uri());
        let error = download_artifact(
            ArtifactDownload {
                location: &url,
                location_type: "http",
                component: "BMC",
                bundle: None,
                access_token: None,
                sha256: ArtifactSha256::default(),
            },
            &temp.path().join("cache/artifact.bin"),
            &test_http_client(),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::FileNotFound
        );
        server.verify().await;
    }

    #[tokio::test]
    async fn download_artifact_gives_up_after_the_attempt_limit() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(503))
            .expect(u64::from(ARTIFACT_DOWNLOAD_ATTEMPTS))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let url = format!("{}/artifact.bin", server.uri());
        let error = download_artifact(
            ArtifactDownload {
                location: &url,
                location_type: "http",
                component: "BMC",
                bundle: None,
                access_token: None,
                sha256: ArtifactSha256::default(),
            },
            &temp.path().join("cache/artifact.bin"),
            &test_http_client(),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::ServerError
        );
        server.verify().await;
    }

    /// Serve raw responses on separate connections, including incomplete or
    /// malformed HTTP that a mock server would normally reject or repair.
    async fn spawn_raw_artifact_server(
        responses: Vec<Vec<u8>>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind artifact server");
        let url = format!(
            "http://{}/artifact.bin",
            listener.local_addr().expect("local addr")
        );
        let server = tokio::spawn(async move {
            for response in responses {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut buf).await.expect("read request");
                    assert_ne!(read, 0, "client closed before sending a request");
                    request.extend_from_slice(&buf[..read]);
                }
                stream.write_all(&response).await.expect("write response");
            }
        });
        (url, server)
    }

    fn artifact_response(body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            ARTIFACT_BYTES.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    #[tokio::test]
    async fn incomplete_response_bodies_are_server_errors() {
        for response in [
            artifact_response(&ARTIFACT_BYTES[..ARTIFACT_BYTES.len() / 2]),
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n10\r\nfirmware"
                .to_vec(),
        ] {
            let (url, server) = spawn_raw_artifact_server(vec![response]).await;
            let response = test_http_client().client.get(&url).send().await.unwrap();
            let mut body = response.bytes_stream();
            let cut = loop {
                match body.next().await {
                    Some(Ok(_)) => continue,
                    Some(Err(error)) => break error,
                    None => panic!("an incomplete response body must fail"),
                }
            };

            assert_eq!(
                ArtifactError::transport("body", cut).job_error(),
                crate::orchestrator::job_lifecycle::JobError::ServerError
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_reset_before_the_response_head_is_a_server_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind artifact server");
        let url = format!(
            "http://{}/artifact.bin",
            listener.local_addr().expect("local addr")
        );
        let server = tokio::spawn(async move {
            // Closing a socket that still holds the unread request makes the
            // kernel reset the connection instead of closing it.
            let (stream, _) = listener.accept().await.expect("accept");
            stream.readable().await.expect("request arrives");
            drop(stream);
        });
        let reset = test_http_client()
            .client
            .get(&url)
            .send()
            .await
            .unwrap_err();

        assert_eq!(
            ArtifactError::transport("request", reset).job_error(),
            crate::orchestrator::job_lifecycle::JobError::ServerError
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_retry_after_a_cut_body_caches_only_the_complete_file() {
        let (url, server) = spawn_raw_artifact_server(vec![
            artifact_response(&ARTIFACT_BYTES[..ARTIFACT_BYTES.len() / 2]),
            artifact_response(ARTIFACT_BYTES),
        ])
        .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        let digest = hex::encode(Sha256::digest(ARTIFACT_BYTES));
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            Some(&digest),
            dest_dir.clone(),
            &test_http_client(),
        )
        .await
        .expect("the attempt after the cut body succeeds");

        server.await.expect("both responses were served");
        assert_cached_artifact(&dest_dir, "artifact.bin").await;
        assert_no_temp_artifacts(&dest_dir).await;
    }

    #[tokio::test]
    async fn a_clean_close_before_the_response_head_is_retried() {
        let (url, server) =
            spawn_raw_artifact_server(vec![Vec::new(), artifact_response(ARTIFACT_BYTES)]).await;
        let temp = tempfile::tempdir().unwrap();
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            temp.path().to_path_buf(),
            &test_http_client(),
        )
        .await
        .expect("a server that closes before its response can recover on retry");

        server.await.unwrap();
        assert_cached_artifact(temp.path(), "artifact.bin").await;
        assert_no_temp_artifacts(temp.path()).await;
    }

    #[tokio::test]
    async fn malformed_chunked_framing_is_not_retried() {
        let (url, server) = spawn_raw_artifact_server(vec![
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\ninvalid-size\r\n"
                .to_vec(),
        ])
        .await;
        let temp = tempfile::tempdir().unwrap();
        let error = download_artifact(
            ArtifactDownload {
                location: &url,
                location_type: "http",
                component: "BMC",
                bundle: None,
                access_token: None,
                sha256: ArtifactSha256::default(),
            },
            &temp.path().join("artifact.bin"),
            &test_http_client(),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::ClientError,
            "{error}"
        );
        assert!(!error.is_transient());
        server.await.unwrap();
        assert!(!temp.path().join("artifact.bin").exists());
        assert_no_temp_artifacts(temp.path()).await;
    }

    #[tokio::test]
    async fn retries_share_one_download_budget() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // The first attempt fails 1.5 s into a 2 s budget. The second answers
        // after 1 s more, which only a fresh budget would allow.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503).set_delay(Duration::from_millis(1500)))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(ARTIFACT_BYTES)
                    .set_delay(Duration::from_secs(1)),
            )
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let url = format!("{}/artifact.bin", server.uri());
        let client = ArtifactHttpClient {
            download_budget: Duration::from_secs(2),
            ..test_http_client()
        };
        let error = download_artifact(
            ArtifactDownload {
                location: &url,
                location_type: "http",
                component: "BMC",
                bundle: None,
                access_token: None,
                sha256: ArtifactSha256::default(),
            },
            &temp.path().join("cache/artifact.bin"),
            &client,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::Timeout
        );
        let message = error.to_string();
        assert!(message.contains("after 2 attempts"), "{message}");
        assert!(message.contains("first failure:"), "{message}");
        assert!(message.contains("503 Service Unavailable"), "{message}");
    }

    #[tokio::test]
    async fn no_retry_starts_when_its_backoff_would_outlast_the_budget() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let url = format!("{}/artifact.bin", server.uri());
        let client = ArtifactHttpClient {
            download_budget: Duration::from_secs(1),
            retry_backoff: Duration::from_secs(2),
            ..test_http_client()
        };
        let error = download_artifact(
            ArtifactDownload {
                location: &url,
                location_type: "http",
                component: "BMC",
                bundle: None,
                access_token: None,
                sha256: ArtifactSha256::default(),
            },
            &temp.path().join("cache/artifact.bin"),
            &client,
        )
        .await
        .unwrap_err();

        // The 503 is reported, not a timeout after a wait that could not help.
        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::ServerError
        );
        server.verify().await;
    }

    #[tokio::test]
    async fn download_single_file_local_verifies_and_rejects_sha256() {
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("local-fw.bin");
        tokio::fs::write(&source_path, ARTIFACT_BYTES)
            .await
            .unwrap();
        let client = test_http_client();
        let digest = expected_digest(ARTIFACT_BYTES);

        let dest_dir = temp.path().join("cache");
        download_single_file(
            source_path.to_string_lossy().as_ref(),
            "file",
            "BMC",
            None,
            None,
            Some(&digest),
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("matching sha256 copy should succeed");
        assert_cached_artifact(&dest_dir, "local-fw.bin").await;

        let dest_dir_bad = temp.path().join("cache-bad");
        let wrong = "1".repeat(64);
        let err = download_single_file(
            source_path.to_string_lossy().as_ref(),
            "file",
            "BMC",
            None,
            None,
            Some(&wrong),
            dest_dir_bad.clone(),
            &client,
        )
        .await
        .unwrap_err();
        assert!(err.contains("sha256 mismatch"), "unexpected error: {err}");
        assert!(!dest_dir_bad.join("local-fw.bin").exists());
        assert_no_temp_artifacts(&dest_dir_bad).await;
    }

    #[tokio::test]
    async fn download_single_file_http_skips_download_when_cached_sha256_matches() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"SHOULD NOT BE FETCHED"))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().unwrap();
        let dest_dir = temp.path().join("cache");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        tokio::fs::write(dest_dir.join("artifact.bin"), ARTIFACT_BYTES)
            .await
            .unwrap();

        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        let digest = expected_digest(ARTIFACT_BYTES);
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            Some(&digest),
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("valid cached artifact should be reused");

        let gets = server
            .received_requests()
            .await
            .expect("requests")
            .into_iter()
            .filter(|request| request.method.to_string() == "GET")
            .count();
        assert_eq!(gets, 0, "matching cached sha256 must skip the download");
        assert_cached_artifact(&dest_dir, "artifact.bin").await;
    }

    #[tokio::test]
    async fn download_single_file_http_redownloads_when_cached_sha256_mismatches() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().unwrap();
        let dest_dir = temp.path().join("cache");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        // Same length as ARTIFACT_BYTES but different content: a size check would
        // pass, so this proves the digest (not the size) triggers re-download.
        assert_eq!(b"firmware PAYLOAD".len(), ARTIFACT_BYTES.len());
        tokio::fs::write(dest_dir.join("artifact.bin"), b"firmware PAYLOAD")
            .await
            .unwrap();

        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        let digest = expected_digest(ARTIFACT_BYTES);
        download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            Some(&digest),
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("tampered cache should be replaced");

        let gets = server
            .received_requests()
            .await
            .expect("requests")
            .into_iter()
            .filter(|request| request.method.to_string() == "GET")
            .count();
        assert_eq!(gets, 1, "sha256 mismatch on cached file must trigger a GET");
        assert_cached_artifact(&dest_dir, "artifact.bin").await;
    }

    struct HttpsTestServer {
        url: String,
        cert_pem: String,
        server_task: tokio::task::JoinHandle<()>,
        requests: Arc<std::sync::Mutex<Vec<http::HeaderMap>>>,
    }

    impl HttpsTestServer {
        async fn start(path: &str, body: &'static [u8]) -> Self {
            Self::start_with_redirect(path, body, None).await
        }

        async fn start_with_redirect(
            path: &str,
            body: &'static [u8],
            redirect: Option<String>,
        ) -> Self {
            use axum::Router;
            use axum::body::Body;
            use axum::http::{Request, Response, StatusCode};
            use axum::routing::get;
            use hyper_util::rt::TokioIo;
            use hyper_util::service::TowerToHyperService;
            use rcgen::generate_simple_self_signed;
            use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
            use tokio_rustls::TlsAcceptor;

            let _ = rustls::crypto::ring::default_provider().install_default();
            let cert = generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
                .expect("generate self-signed cert");
            let cert_pem = cert.cert.pem();
            let cert_der = CertificateDer::from(cert.cert.der().to_vec());
            let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

            let mut server_config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert_der], key_der.into())
                .expect("TLS config");
            server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

            let route_path = path.to_owned();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let received = requests.clone();
            let final_received = requests.clone();
            let app = Router::new()
                .route(
                    path,
                    get(move |request: Request<Body>| {
                        let route_path = route_path.clone();
                        received.lock().unwrap().push(request.headers().clone());
                        let redirect = redirect.clone();
                        async move {
                            if let Some(redirect) = redirect {
                                return Response::builder()
                                    .status(StatusCode::FOUND)
                                    .header("location", redirect)
                                    .body(Body::empty())
                                    .unwrap();
                            }
                            if request.uri().path() != route_path {
                                return Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Body::empty())
                                    .unwrap();
                            }
                            match *request.method() {
                                http::Method::HEAD => Response::builder()
                                    .status(StatusCode::OK)
                                    .header("content-length", body.len())
                                    .body(Body::empty())
                                    .unwrap(),
                                http::Method::GET => Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Body::from(body))
                                    .unwrap(),
                                _ => Response::builder()
                                    .status(StatusCode::METHOD_NOT_ALLOWED)
                                    .body(Body::empty())
                                    .unwrap(),
                            }
                        }
                    }),
                )
                .route(
                    "/final.bin",
                    get(move |request: Request<Body>| {
                        final_received
                            .lock()
                            .unwrap()
                            .push(request.headers().clone());
                        async move { Body::from(body) }
                    }),
                );

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind https test server");
            let port = listener.local_addr().expect("local addr").port();
            let url = format!("https://127.0.0.1:{port}{path}");

            let server_task = tokio::spawn(async move {
                let app = app;
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        continue;
                    };
                    let tls = tls_acceptor.clone();
                    let app = app.clone();
                    tokio::spawn(async move {
                        let Ok(tls_stream) = tls.accept(stream).await else {
                            return;
                        };
                        let io = TokioIo::new(tls_stream);
                        let svc = TowerToHyperService::new(app);
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, svc)
                            .await;
                    });
                }
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            Self {
                url,
                cert_pem,
                server_task,
                requests,
            }
        }

        fn trust_client(&self) -> reqwest::Client {
            let cert = reqwest::Certificate::from_pem(self.cert_pem.as_bytes())
                .expect("parse test cert pem");
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .add_root_certificate(cert)
                .build()
                .expect("build https test client")
        }

        fn trust_artifact_client(&self) -> ArtifactHttpClient {
            ArtifactHttpClient::from_parts(self.trust_client(), ArtifactDownloadPolicy::default())
        }

        async fn finish(self) {
            self.server_task.abort();
        }
    }

    #[tokio::test]
    async fn authenticated_https_redirects_work_on_the_same_origin() {
        let server = HttpsTestServer::start_with_redirect(
            "/artifact.bin",
            ARTIFACT_BYTES,
            Some("/final.bin".to_owned()),
        )
        .await;
        let temp = tempfile::tempdir().unwrap();
        download_single_file(
            &server.url,
            "artifactory",
            "BMC",
            None,
            Some("secret-token"),
            None,
            temp.path().to_path_buf(),
            &server.trust_artifact_client(),
        )
        .await
        .unwrap();
        assert_cached_artifact(temp.path(), "artifact.bin").await;
        {
            let requests = server.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(
                requests
                    .iter()
                    .all(|headers| headers["X-JFrog-Art-Api"] == "secret-token")
            );
        }
        server.finish().await;
    }

    #[tokio::test]
    async fn redirect_loop_is_bounded_and_reports_a_policy_failure() {
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(wiremock::matchers::path("/loop"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/loop"))
            .mount(&server)
            .await;
        let error = send_artifact_request(
            &test_http_client(),
            Method::GET,
            &format!("{}/loop", server.uri()),
            &ArtifactHttpAuth::None,
            test_deadline(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ArtifactError::RedirectPolicy(_)));
        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::FailedPrecondition
        );
        assert!(error.to_string().contains("redirect limit"));
        assert_eq!(server.received_requests().await.unwrap().len(), 11);
    }

    #[tokio::test]
    async fn connection_refusal_and_timeouts_redact_urls_and_keep_their_transport_codes() {
        use crate::orchestrator::job_lifecycle::JobError;

        // Inject the connection error so no released port can be reused by another test.
        struct RefusedConnection;

        impl reqwest::dns::Resolve for RefusedConnection {
            fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
                Box::pin(async {
                    Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into())
                })
            }
        }

        let client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(Arc::new(RefusedConnection))
            .build()
            .unwrap();

        let refused = client
            .get("http://artifact.invalid/artifact.bin?token=SECRET&X-Amz-Signature=SIGNATURE")
            .send()
            .await
            .unwrap_err();

        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_delay(Duration::from_secs(1)))
            .mount(&server)
            .await;
        let timeout = test_http_client()
            .client
            .get(format!(
                "{}/artifact.bin?token=SECRET&X-Amz-Signature=SIGNATURE",
                server.uri()
            ))
            .timeout(Duration::from_millis(20))
            .send()
            .await
            .unwrap_err();

        for (source, expected) in [
            (refused, JobError::ServerError),
            (timeout, JobError::Timeout),
        ] {
            let error = ArtifactError::from(source);

            assert_eq!(error.job_error(), expected);
            assert!(error.to_string().contains("artifact transport failed"));

            for text in [error.to_string(), format!("{error:?}")] {
                assert!(!text.contains("SECRET"), "{text}");
                assert!(!text.contains("SIGNATURE"), "{text}");
            }

            let ArtifactError::Transport { source, .. } = error else {
                panic!("reqwest errors must retain a transport source");
            };

            assert!(source.url().is_none());
            assert!(std::error::Error::source(&source).is_some());
        }
    }

    #[tokio::test]
    async fn connect_and_request_timeouts_are_transient() {
        // A lookup that never answers keeps the connector busy until its timeout.
        struct SilentResolver;

        impl reqwest::dns::Resolve for SilentResolver {
            fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
                Box::pin(std::future::pending())
            }
        }

        let connect_timeout = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(Arc::new(SilentResolver))
            .connect_timeout(Duration::from_millis(20))
            .build()
            .unwrap()
            .get("http://artifact.invalid/artifact.bin")
            .send()
            .await
            .unwrap_err();
        assert!(ArtifactError::transport("connect", connect_timeout).is_transient());

        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_delay(Duration::from_secs(1)))
            .mount(&server)
            .await;
        let request_timeout = test_http_client()
            .client
            .get(server.uri())
            .timeout(Duration::from_millis(20))
            .send()
            .await
            .unwrap_err();
        assert!(ArtifactError::transport("download", request_timeout).is_transient());
    }

    #[tokio::test]
    async fn untrusted_tls_is_not_classified_as_a_retryable_connection_failure() {
        let server = HttpsTestServer::start("/artifact.bin", ARTIFACT_BYTES).await;
        let error = send_artifact_request(
            &test_http_client(),
            Method::GET,
            &server.url,
            &ArtifactHttpAuth::ArtifactoryApiKey("secret-token".to_owned()),
            test_deadline(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::ClientError,
            "{error}"
        );
        assert!(server.requests.lock().unwrap().is_empty());
        server.finish().await;
    }

    #[tokio::test]
    async fn authenticated_https_download_preserves_the_api_key() {
        let server = HttpsTestServer::start("/artifact.bin", ARTIFACT_BYTES).await;
        let temp = tempfile::tempdir().unwrap();
        download_single_file(
            &server.url,
            "artifactory",
            "BMC",
            None,
            Some("secret-token"),
            None,
            temp.path().to_path_buf(),
            &server.trust_artifact_client(),
        )
        .await
        .unwrap();
        assert_cached_artifact(temp.path(), "artifact.bin").await;
        assert_eq!(
            server.requests.lock().unwrap()[0]["X-JFrog-Art-Api"],
            "secret-token"
        );
        server.finish().await;
    }

    #[tokio::test]
    async fn authenticated_redirects_never_reach_http_or_another_origin() {
        let target = wiremock::MockServer::start().await;
        for redirect in [
            format!("{}/artifact.bin", target.uri()),
            "https://example.test/elsewhere.bin".to_owned(),
        ] {
            let server = HttpsTestServer::start_with_redirect(
                "/artifact.bin",
                ARTIFACT_BYTES,
                Some(redirect),
            )
            .await;
            for method in [Method::GET, Method::HEAD] {
                let error = send_artifact_request(
                    &server.trust_artifact_client(),
                    method,
                    &server.url,
                    &ArtifactHttpAuth::ArtifactoryApiKey("secret-token".to_owned()),
                    test_deadline(),
                )
                .await
                .unwrap_err();
                assert!(matches!(error, ArtifactError::RedirectPolicy(_)), "{error}");
                assert!(error.to_string().contains("original HTTPS origin"));
            }
            server.finish().await;
        }
        assert!(target.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unauthenticated_artifactory_http_redirects_still_work() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(path("/redirect.bin"))
            .and(query_param("token", "INITIAL"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                "/artifact.bin?token=SECRET&X-Amz-Signature=SIGNATURE",
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .and(query_param("token", "SECRET"))
            .and(query_param("X-Amz-Signature", "SIGNATURE"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .expect(1)
            .mount(&server)
            .await;
        let temp = tempfile::tempdir().unwrap();
        download_single_file(
            &format!("{}/redirect.bin?token=INITIAL", server.uri()),
            "artifactory",
            "BMC",
            None,
            None,
            None,
            temp.path().to_path_buf(),
            &test_http_client(),
        )
        .await
        .unwrap();
        assert_cached_artifact(temp.path(), "redirect.bin").await;
        for request in server.received_requests().await.unwrap() {
            assert!(
                !request
                    .headers
                    .keys()
                    .any(|name| name.as_str().eq_ignore_ascii_case("X-JFrog-Art-Api"))
            );
        }
    }

    #[tokio::test]
    async fn download_http_file_fetches_artifact_over_https() {
        let server = HttpsTestServer::start("/artifact.bin", ARTIFACT_BYTES).await;
        let client = server.trust_client();
        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        let dest_path = dest_dir.join("artifact.bin");

        download_http_file(
            &ArtifactHttpClient::from_parts(client, ArtifactDownloadPolicy::default()),
            &server.url,
            &ArtifactDownload {
                location: &server.url,
                location_type: "https",
                component: "BMC",
                bundle: None,
                access_token: None,
                sha256: ArtifactSha256::default(),
            },
            &ArtifactHttpAuth::None,
            &dest_path,
            "artifact.bin",
            test_deadline(),
        )
        .await
        .expect("https download should succeed");

        assert_cached_artifact(&dest_dir, "artifact.bin").await;
        server.finish().await;
    }

    #[test]
    fn resolve_artifact_download_rejects_unsupported_location_types() {
        for location_type in ["ftp", "ftps", "sftp", "scp", "tftp"] {
            let err =
                resolve_artifact_download("https://example.test/fw.fwpkg", location_type, None)
                    .unwrap_err();
            assert!(
                err.contains("unsupported firmware artifact LocationType"),
                "{location_type}: {err}"
            );
        }
    }

    #[test]
    fn invalid_artifact_locations_do_not_expose_credentials() {
        for (location, kind, expected) in [
            (
                "file://remote/fw.bin?token=SECRET#FRAGMENT",
                "file",
                "file artifact location must be a valid file URL: file://remote/fw.bin",
            ),
            (
                "ftp://USER:PASSWORD@example.test/fw.bin?X-Amz-Signature=SECRET#FRAGMENT",
                "http",
                "unsupported firmware artifact location scheme 'ftp': ftp://example.test/fw.bin",
            ),
            (
                "https://USER:PASSWORD@[invalid/fw.bin?token=SECRET#FRAGMENT",
                "http",
                "invalid firmware artifact location URL",
            ),
        ] {
            let error = resolve_artifact_download(location, kind, None).unwrap_err();

            assert_eq!(error, expected);
        }
    }

    #[tokio::test]
    async fn required_digest_error_redacts_the_artifact_location() {
        let temp = tempfile::tempdir().unwrap();
        let location = "https://USER:PASSWORD@example.test/fw.bin?token=SECRET#FRAGMENT";

        let request = ArtifactDownload {
            location,
            location_type: "https",
            component: "BMC",
            bundle: None,
            access_token: None,
            sha256: ArtifactSha256::required(None),
        };

        let error = download_artifact(request, &temp.path().join("fw.bin"), &test_http_client())
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "firmware artifact https://example.test/fw.bin is missing a required Sha256 digest"
        );

        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::InvalidArgument
        );
    }

    #[tokio::test]
    async fn interrupted_response_body_redacts_the_url_without_publishing_a_partial_file() {
        let (url, server) = spawn_raw_artifact_server(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\npartial"
                .to_vec();
            ARTIFACT_DOWNLOAD_ATTEMPTS as usize
        ])
        .await;
        let url = format!("{url}?token=SECRET&X-Amz-Signature=SIGNATURE");

        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("fw.bin");

        let request = ArtifactDownload {
            location: &url,
            location_type: "http",
            component: "BMC",
            bundle: None,
            access_token: None,
            sha256: ArtifactSha256::default(),
        };

        let error = download_artifact(request, &destination, &test_http_client())
            .await
            .unwrap_err();

        server.await.unwrap();

        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::ServerError,
            "{error}"
        );

        assert!(error.to_string().contains(&format!(
            "after {ARTIFACT_DOWNLOAD_ATTEMPTS} attempts (first failure:"
        )));
        assert!(
            error
                .to_string()
                .contains("failed to read artifact response body")
        );

        for text in [error.to_string(), format!("{error:?}")] {
            assert!(!text.contains("SECRET"), "{text}");
            assert!(!text.contains("SIGNATURE"), "{text}");
        }

        assert!(!destination.exists());
        assert_no_temp_artifacts(temp.path()).await;
    }

    #[tokio::test]
    async fn download_single_file_rejects_host_not_on_allow_list() {
        let temp = tempfile::tempdir().expect("tempdir");
        let client = test_http_client_with_policy(ArtifactDownloadPolicy {
            allowed_hosts: vec!["artifactory.example.com".to_owned()],
            ..ArtifactDownloadPolicy::default()
        });
        let err = download_single_file(
            "https://evil.example/artifact.bin",
            "https",
            "BMC",
            None,
            None,
            None,
            temp.path().to_path_buf(),
            &client,
        )
        .await
        .expect_err("disallowed host");
        assert!(err.contains("allow-list"), "{err}");
    }

    #[tokio::test]
    async fn download_single_file_reuses_cache_when_origin_is_no_longer_allowed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        tokio::fs::write(dest_dir.join("artifact.bin"), ARTIFACT_BYTES)
            .await
            .unwrap();

        let client = test_http_client_with_policy(ArtifactDownloadPolicy {
            allowed_hosts: vec!["artifactory.example.com".to_owned()],
            ..ArtifactDownloadPolicy::default()
        });
        download_single_file(
            "https://evil.example/artifact.bin",
            "https",
            "BMC",
            None,
            None,
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("cached artifact from a now-disallowed host");
        assert_cached_artifact(&dest_dir, "artifact.bin").await;
    }

    #[tokio::test]
    async fn download_single_file_stops_after_max_redirects() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        for hop in 0..=MAX_ARTIFACT_REDIRECTS {
            let current = format!("/r{hop}");
            let next = format!("/r{}", hop + 1);
            Mock::given(method("GET"))
                .and(path(current))
                .respond_with(ResponseTemplate::new(302).insert_header("Location", next.as_str()))
                .mount(&server)
                .await;
        }

        let client = test_http_client();
        let temp = tempfile::tempdir().expect("tempdir");
        let url = format!("{}/r0", server.uri());
        let err = download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            temp.path().to_path_buf(),
            &client,
        )
        .await
        .expect_err("too many redirects");
        assert!(err.contains("redirect limit"), "{err}");
    }

    #[tokio::test]
    async fn download_single_file_rejects_loopback_when_private_targets_disallowed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let client = test_http_client_with_policy(ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        });
        let err = download_single_file(
            "http://127.0.0.1/artifact.bin",
            "http",
            "BMC",
            None,
            None,
            None,
            temp.path().to_path_buf(),
            &client,
        )
        .await
        .expect_err("loopback");
        assert!(err.contains("disallowed address"), "{err}");
    }

    #[tokio::test]
    async fn download_single_file_rejects_localhost_when_private_targets_disallowed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let client = test_http_client_with_policy(ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        });
        let err = download_single_file(
            "http://localhost/artifact.bin",
            "http",
            "BMC",
            None,
            None,
            None,
            temp.path().to_path_buf(),
            &client,
        )
        .await
        .expect_err("localhost");
        assert!(err.contains("disallowed address"), "{err}");
    }

    #[tokio::test]
    async fn download_single_file_reuses_cache_when_hostname_resolves_to_a_disallowed_address() {
        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        tokio::fs::create_dir_all(&dest_dir).await.unwrap();
        tokio::fs::write(dest_dir.join("artifact.bin"), ARTIFACT_BYTES)
            .await
            .unwrap();

        let client = test_http_client_with_policy(ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        });
        download_single_file(
            "http://localhost/artifact.bin",
            "http",
            "BMC",
            None,
            None,
            None,
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("cached artifact whose hostname resolves to a disallowed address");
        assert_cached_artifact(&dest_dir, "artifact.bin").await;
    }

    #[test]
    fn resolve_artifact_download_rejects_unsupported_url_schemes() {
        for (url, scheme) in [
            ("ftp://files.example.test/fw.bin", "ftp"),
            ("sftp://files.example.test/fw.bin", "sftp"),
            ("tftp://files.example.test/fw.bin", "tftp"),
        ] {
            let err = resolve_artifact_download(url, "http", None).unwrap_err();
            assert!(
                err.contains("unsupported firmware artifact location scheme"),
                "{scheme}: {err}"
            );
        }
    }

    #[test]
    fn resolve_artifact_download_accepts_host_not_on_allow_list() {
        let policy = ArtifactDownloadPolicy {
            allowed_hosts: vec!["allowed.example.test".to_owned()],
            allow_private_targets: true,
        };
        let source = resolve_artifact_download_with_policy(
            "https://evil.example.test/fw.bin",
            "http",
            None,
            &policy,
        )
        .expect("host policy is enforced at request time");
        assert!(matches!(
            source,
            ArtifactDownloadSource::Http { url, .. } if url == "https://evil.example.test/fw.bin"
        ));
    }

    #[tokio::test]
    async fn send_artifact_request_rejects_first_url_as_invalid_argument() {
        let client = test_http_client_with_policy(ArtifactDownloadPolicy {
            allowed_hosts: vec!["artifactory.example.com".to_owned()],
            ..ArtifactDownloadPolicy::default()
        });
        let error = send_artifact_request(
            &client,
            Method::GET,
            "https://evil.example/artifact.bin",
            &ArtifactHttpAuth::None,
            test_deadline(),
        )
        .await
        .expect_err("disallowed first URL");
        assert!(
            matches!(error, ArtifactError::InvalidArgument(_)),
            "{error:?}"
        );
        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::InvalidArgument
        );
        assert!(error.to_string().contains("allow-list"), "{error}");
    }

    #[tokio::test]
    async fn download_single_file_rejects_redirect_off_allow_list() {
        use wiremock::matchers::path;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(path("/redirect.bin"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", "https://evil.example.test/fw.bin"),
            )
            .mount(&server)
            .await;

        let origin_host = reqwest::Url::parse(&server.uri())
            .unwrap()
            .host_str()
            .unwrap()
            .to_string();
        let client = test_http_client_with_policy(ArtifactDownloadPolicy {
            allowed_hosts: vec![origin_host],
            allow_private_targets: true,
        });
        let url = format!("{}/redirect.bin", server.uri());
        let error = send_artifact_request(
            &client,
            Method::GET,
            &url,
            &ArtifactHttpAuth::None,
            test_deadline(),
        )
        .await
        .expect_err("redirect off allow-list");
        assert!(
            matches!(error, ArtifactError::RedirectPolicy(_)),
            "{error:?}"
        );
        assert_eq!(
            error.job_error(),
            crate::orchestrator::job_lifecycle::JobError::FailedPrecondition
        );
        assert!(
            error
                .to_string()
                .contains("not on the artifact host allow-list"),
            "{error}"
        );

        let temp = tempfile::tempdir().unwrap();
        let err = download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            None,
            temp.path().to_path_buf(),
            &client,
        )
        .await
        .unwrap_err();
        assert!(err.contains("not on the artifact host allow-list"), "{err}");
    }
}
