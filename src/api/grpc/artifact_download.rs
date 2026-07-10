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

use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Method;

use super::firmware_artifact_paths::filename_from_location;

const REMOTE_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);

pub(crate) fn build_artifact_http_client() -> std::result::Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(REMOTE_DOWNLOAD_TIMEOUT)
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))
}

/// Transport/auth profile selected from SOT `LocationType` and the artifact URL.
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

/// Download one firmware artifact into `dest_dir`, using `access_token` when the
/// location type requires authenticated HTTP access.
pub(crate) async fn download_single_file(
    location: &str,
    location_type: &str,
    component: &str,
    bundle: Option<&str>,
    access_token: Option<&str>,
    dest_dir: PathBuf,
    http_client: &reqwest::Client,
) -> std::result::Result<(), String> {
    let source = resolve_artifact_download(location, location_type, access_token)?;
    let filename = filename_from_location(location).map_err(|e| e.to_string())?;
    tokio::fs::create_dir_all(&dest_dir).await.map_err(|e| {
        format!(
            "failed to create cache directory {}: {e}",
            dest_dir.display()
        )
    })?;
    let dest_path = dest_dir.join(&filename);

    match source {
        ArtifactDownloadSource::Http { url, auth } => {
            download_http_file(
                http_client,
                &url,
                location_type,
                component,
                bundle,
                auth,
                dest_path,
                &filename,
            )
            .await
        }
        ArtifactDownloadSource::File(source_path) => {
            copy_local_file_to_cache(
                &source_path,
                location,
                location_type,
                component,
                bundle,
                dest_path,
                &filename,
            )
            .await
        }
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

fn resolve_artifact_download(
    location: &str,
    location_type: &str,
    access_token: Option<&str>,
) -> std::result::Result<ArtifactDownloadSource, String> {
    let access_token = optional_access_token(access_token);
    let kind = artifact_location_kind(location_type)?;

    let source = match reqwest::Url::parse(location) {
        Ok(url) => {
            let scheme = url.scheme();
            match scheme {
                "http" | "https" => ArtifactDownloadSource::Http {
                    url: location.to_owned(),
                    auth: http_auth_for_location(kind, access_token),
                },
                "file" => url
                    .to_file_path()
                    .map(ArtifactDownloadSource::File)
                    .map_err(|_| {
                        format!("file artifact location must be a valid file URL: {location}")
                    })?,
                scheme => {
                    return Err(format!(
                        "unsupported firmware artifact location scheme '{scheme}': {location}"
                    ));
                }
            }
        }
        Err(_) if kind == ArtifactLocationKind::File || !location.contains("://") => {
            ArtifactDownloadSource::File(PathBuf::from(location))
        }
        Err(_) => {
            return Err(format!(
                "invalid firmware artifact location URL: {location}"
            ));
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

#[allow(clippy::too_many_arguments)]
async fn download_http_file(
    client: &reqwest::Client,
    url: &str,
    location_type: &str,
    component: &str,
    bundle: Option<&str>,
    auth: ArtifactHttpAuth,
    dest_path: PathBuf,
    filename: &str,
) -> std::result::Result<(), String> {
    if cached_firmware_file_valid(client, url, &auth, &dest_path).await? {
        return Ok(());
    }

    tracing::info!(
        component = %component,
        bundle = ?bundle,
        location_type = %location_type,
        url = %url,
        path = %dest_path.display(),
        "downloading firmware object artifact"
    );
    let response = apply_artifact_http_auth(client.request(Method::GET, url), &auth)
        .send()
        .await
        .map_err(|e| format!("failed to download {url}: {e}"))?;

    if !response.status().is_success() {
        return Err(format!(
            "download failed with status {}: {url}",
            response.status()
        ));
    }

    let expected_len = response.content_length();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("failed to read response body: {e}"))?;
    validate_download_size(url, expected_len, bytes.len() as u64)?;

    write_download_bytes(&dest_path, filename, &bytes).await
}

async fn write_download_bytes(
    dest_path: &Path,
    filename: &str,
    bytes: &[u8],
) -> std::result::Result<(), String> {
    let temp_path = dest_path.with_file_name(format!(".{filename}.{}.tmp", uuid::Uuid::new_v4()));
    tokio::fs::write(&temp_path, bytes)
        .await
        .map_err(|e| format!("failed to write {}: {e}", temp_path.display()))?;
    tokio::fs::rename(&temp_path, dest_path).await.map_err(|e| {
        let _ = std::fs::remove_file(&temp_path);
        format!(
            "failed to move {} to {}: {e}",
            temp_path.display(),
            dest_path.display()
        )
    })
}

async fn copy_local_file_to_cache(
    source_path: &Path,
    source_label: &str,
    location_type: &str,
    component: &str,
    bundle: Option<&str>,
    dest_path: PathBuf,
    filename: &str,
) -> std::result::Result<(), String> {
    let metadata = tokio::fs::metadata(source_path).await.map_err(|e| {
        format!(
            "failed to stat local artifact {}: {e}",
            source_path.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "local artifact is not a file: {}",
            source_path.display()
        ));
    }
    validate_download_size(source_label, Some(metadata.len()), metadata.len())?;

    if cached_local_file_valid(source_path, &metadata, &dest_path).await? {
        return Ok(());
    }

    tracing::info!(
        component = %component,
        bundle = ?bundle,
        location_type = %location_type,
        source = %source_path.display(),
        path = %dest_path.display(),
        "copying local firmware object artifact"
    );

    let temp_path = dest_path.with_file_name(format!(".{filename}.{}.tmp", uuid::Uuid::new_v4()));
    let copied = tokio::fs::copy(source_path, &temp_path)
        .await
        .map_err(|e| {
            format!(
                "failed to copy {} to {}: {e}",
                source_path.display(),
                temp_path.display()
            )
        })?;
    validate_download_size(source_label, Some(metadata.len()), copied)?;
    tokio::fs::rename(&temp_path, &dest_path)
        .await
        .map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            format!(
                "failed to move {} to {}: {e}",
                temp_path.display(),
                dest_path.display()
            )
        })?;
    Ok(())
}

async fn cached_firmware_file_valid(
    client: &reqwest::Client,
    url: &str,
    auth: &ArtifactHttpAuth,
    dest_path: &Path,
) -> std::result::Result<bool, String> {
    let metadata = match tokio::fs::metadata(dest_path).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(format!("failed to stat {}: {e}", dest_path.display())),
    };
    if !metadata.is_file() {
        return Err(format!(
            "cached path is not a file: {}",
            dest_path.display()
        ));
    }
    if metadata.len() == 0 {
        remove_invalid_cached_file(dest_path).await?;
        return Ok(false);
    }

    match remote_content_length(client, url, auth).await {
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

async fn cached_local_file_valid(
    source_path: &Path,
    source_metadata: &std::fs::Metadata,
    dest_path: &Path,
) -> std::result::Result<bool, String> {
    let metadata = match tokio::fs::metadata(dest_path).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(format!("failed to stat {}: {e}", dest_path.display())),
    };
    if !metadata.is_file() {
        return Err(format!(
            "cached path is not a file: {}",
            dest_path.display()
        ));
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
    client: &reqwest::Client,
    url: &str,
    auth: &ArtifactHttpAuth,
) -> std::result::Result<Option<u64>, String> {
    let response = apply_artifact_http_auth(client.request(Method::HEAD, url), auth)
        .send()
        .await
        .map_err(|e| format!("failed to validate cached artifact with HEAD {url}: {e}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "cached artifact HEAD validation failed with status {}: {url}",
            response.status()
        ));
    }
    Ok(response.content_length())
}

fn validate_download_size(
    url: &str,
    expected_len: Option<u64>,
    actual_len: u64,
) -> std::result::Result<(), String> {
    if actual_len == 0 {
        return Err(format!("download returned zero bytes: {url}"));
    }
    if let Some(expected_len) = expected_len
        && expected_len != actual_len
    {
        return Err(format!(
            "download size mismatch for {url}: expected {expected_len} bytes, got {actual_len}"
        ));
    }
    Ok(())
}

async fn remove_invalid_cached_file(dest_path: &Path) -> std::result::Result<(), String> {
    tokio::fs::remove_file(dest_path).await.map_err(|e| {
        format!(
            "failed to remove invalid cache file {}: {e}",
            dest_path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const ARTIFACT_BYTES: &[u8] = b"firmware payload";

    fn test_http_client() -> reqwest::Client {
        build_artifact_http_client().expect("artifact HTTP client")
    }

    async fn assert_cached_artifact(dest_dir: &Path, filename: &str) {
        let cached = tokio::fs::read(dest_dir.join(filename))
            .await
            .expect("read cached artifact");
        assert_eq!(cached, ARTIFACT_BYTES);
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
    async fn download_single_file_fetches_artifact_over_artifactory_with_api_key() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.bin"))
            .and(header("X-JFrog-Art-Api", "secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ARTIFACT_BYTES))
            .mount(&server)
            .await;

        let temp = tempfile::tempdir().expect("tempdir");
        let dest_dir = temp.path().join("cache");
        let url = format!("{}/artifact.bin", server.uri());
        let client = test_http_client();
        download_single_file(
            &url,
            "artifactory",
            "BMC",
            None,
            Some("secret-token"),
            dest_dir.clone(),
            &client,
        )
        .await
        .expect("artifactory download should succeed");

        assert_cached_artifact(&dest_dir, "artifact.bin").await;
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
        let url = format!("{}/empty.bin", server.uri());
        let client = test_http_client();
        let err = download_single_file(
            &url,
            "http",
            "BMC",
            None,
            None,
            temp.path().join("cache"),
            &client,
        )
        .await
        .unwrap_err();
        assert!(err.contains("zero bytes"), "unexpected error: {err}");
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
        download_single_file(&url, "http", "BMC", None, None, dest_dir.clone(), &client)
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
            .respond_with(ResponseTemplate::new(500))
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
        download_single_file(&url, "http", "BMC", None, None, dest_dir.clone(), &client)
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
    async fn download_single_file_local_file_fails_when_missing() {
        let temp = tempfile::tempdir().unwrap();
        let client = test_http_client();
        let err = download_single_file(
            "/tmp/does-not-exist-rms-artifact.fwpkg",
            "file",
            "BMC",
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

    struct HttpsTestServer {
        url: String,
        cert_pem: String,
        server_task: tokio::task::JoinHandle<()>,
    }

    impl HttpsTestServer {
        async fn start(path: &str, body: &'static [u8]) -> Self {
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
            let app = Router::new().route(
                path,
                get(move |request: Request<Body>| {
                    let route_path = route_path.clone();
                    async move {
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
            }
        }

        fn trust_client(&self) -> reqwest::Client {
            let cert = reqwest::Certificate::from_pem(self.cert_pem.as_bytes())
                .expect("parse test cert pem");
            reqwest::Client::builder()
                .add_root_certificate(cert)
                .build()
                .expect("build https test client")
        }

        async fn finish(self) {
            self.server_task.abort();
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
            &client,
            &server.url,
            "https",
            "BMC",
            None,
            ArtifactHttpAuth::None,
            dest_path,
            "artifact.bin",
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
}
