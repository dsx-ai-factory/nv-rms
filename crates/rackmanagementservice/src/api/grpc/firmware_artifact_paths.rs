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

/// Describes why a firmware artifact path or location is not safe for cache use.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ArtifactPathError {
    #[error("location must end with a filename: {location}")]
    MissingFilename { location: String },

    #[error("artifact filename must not be empty")]
    EmptyFilename,

    #[error("location filename must be a non-hidden basename: {filename}")]
    NonHiddenBasename { filename: String },

    #[error("location filename contains unsafe characters: {filename}")]
    UnsafeCharacters { filename: String },

    #[error("artifact cache subdirectory is not path-safe: {cache_subdir}")]
    UnsafeCacheSubdir { cache_subdir: String },
}

/// Extracts and validates the filename component from an artifact location.
pub(crate) fn filename_from_location(
    location: &str,
) -> std::result::Result<String, ArtifactPathError> {
    if location.ends_with('/') || location.ends_with('\\') {
        return Err(ArtifactPathError::MissingFilename {
            location: location.to_owned(),
        });
    }

    let filename = match reqwest::Url::parse(location) {
        Ok(url) => url
            .path_segments()
            .and_then(|mut segments| segments.next_back().map(str::to_owned)),
        Err(_) => Path::new(location)
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_owned),
    }
    .filter(|name| !name.is_empty())
    .ok_or_else(|| ArtifactPathError::MissingFilename {
        location: location.to_owned(),
    })?;

    validate_artifact_filename(&filename)?;
    Ok(filename)
}

/// Removes credential-bearing parts (userinfo and query string) from an
/// artifact location before it is written to a log or error message. Firmware
/// manifest locations can be signed URLs (Artifactory/S3-style tokens in the query
/// string, e.g. `?token=...`) or basic-auth URLs (`user:pass@host`); neither
/// should ever be persisted verbatim. Non-URL locations (bare local file
/// paths) are returned unchanged since they aren't credential-bearing.
pub(crate) fn redact_location_for_logging(location: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(location) else {
        // Not a URL `reqwest` can parse (e.g. a bare local path), but it may
        // still carry a `?`/`#` suffix from a non-standard location string;
        // strip that defensively rather than assuming it can't be
        // credential-bearing.
        let end = location.find(['?', '#']).unwrap_or(location.len());
        return location[..end].to_owned();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

/// Validates that a firmware artifact filename is a safe non-hidden basename.
pub(crate) fn validate_artifact_filename(
    filename: &str,
) -> std::result::Result<(), ArtifactPathError> {
    if filename.is_empty() {
        return Err(ArtifactPathError::EmptyFilename);
    }

    if !is_safe_artifact_path_component(filename) {
        return Err(ArtifactPathError::UnsafeCharacters {
            filename: filename.to_owned(),
        });
    }

    if filename.starts_with('.') {
        return Err(ArtifactPathError::NonHiddenBasename {
            filename: filename.to_owned(),
        });
    }

    Ok(())
}

/// Builds a validated firmware artifact path under a cache subdirectory.
pub(crate) fn artifact_cache_path(
    cache_dir: &Path,
    cache_subdir: &str,
    filename: &str,
) -> std::result::Result<PathBuf, ArtifactPathError> {
    validate_artifact_filename(filename)?;
    if !cache_subdir.is_empty()
        && (cache_subdir.starts_with('.') || !is_safe_artifact_path_component(cache_subdir))
    {
        return Err(ArtifactPathError::UnsafeCacheSubdir {
            cache_subdir: cache_subdir.to_owned(),
        });
    }

    Ok(cache_dir.join(cache_subdir).join(filename))
}

fn is_safe_artifact_path_component(value: &str) -> bool {
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_from_location_uses_safe_url_path_basename()
    -> std::result::Result<(), ArtifactPathError> {
        assert_eq!(
            filename_from_location("https://example.test/path/firmware.fwpkg?token=abc")?,
            "firmware.fwpkg"
        );
        assert_eq!(
            filename_from_location("https://example.test/path/nvos.bin#section")?,
            "nvos.bin"
        );
        assert_eq!(filename_from_location("/tmp/local-fw.bin")?, "local-fw.bin");

        Ok(())
    }

    #[test]
    fn filename_from_location_rejects_unsafe_basenames() {
        for location in [
            "https://example.test/path/",
            "https://example.test/path/..",
            "https://example.test/path/.hidden",
            "https://example.test/foo/bar%2F..%2Fevil",
            "https://example.test/path/file name.bin",
            "/tmp/file.bin?token=abc",
            "relative\\path.bin",
        ] {
            assert!(
                filename_from_location(location).is_err(),
                "accepted unsafe location: {location}"
            );
        }
    }

    #[test]
    fn redact_location_for_logging_strips_query_and_userinfo() {
        assert_eq!(
            redact_location_for_logging(
                "https://user:s3cr3t@example.test/path/firmware.fwpkg?token=abc&sig=xyz"
            ),
            "https://example.test/path/firmware.fwpkg"
        );
        assert_eq!(
            redact_location_for_logging("https://example.test/path/nvos.bin#section"),
            "https://example.test/path/nvos.bin"
        );
    }

    #[test]
    fn redact_location_for_logging_leaves_non_url_locations_unchanged() {
        assert_eq!(
            redact_location_for_logging("/tmp/local-fw.bin"),
            "/tmp/local-fw.bin"
        );
    }

    #[test]
    fn redact_location_for_logging_strips_query_and_fragment_from_unparseable_locations() {
        assert_eq!(
            redact_location_for_logging("/tmp/local-fw.bin?token=abc"),
            "/tmp/local-fw.bin"
        );
        assert_eq!(
            redact_location_for_logging("/tmp/local-fw.bin#section"),
            "/tmp/local-fw.bin"
        );
    }

    #[test]
    fn redact_location_for_logging_is_a_noop_without_credentials_or_query() {
        assert_eq!(
            redact_location_for_logging("https://example.test/path/firmware.fwpkg"),
            "https://example.test/path/firmware.fwpkg"
        );
    }

    #[test]
    fn validate_artifact_filename_returns_typed_errors() {
        assert_eq!(
            validate_artifact_filename(""),
            Err(ArtifactPathError::EmptyFilename)
        );
        assert_eq!(
            validate_artifact_filename(".hidden"),
            Err(ArtifactPathError::NonHiddenBasename {
                filename: ".hidden".to_owned()
            })
        );
        assert_eq!(
            validate_artifact_filename("file name.bin"),
            Err(ArtifactPathError::UnsafeCharacters {
                filename: "file name.bin".to_owned()
            })
        );
    }

    #[test]
    fn artifact_cache_path_uses_requested_cache_subdir()
    -> std::result::Result<(), ArtifactPathError> {
        assert_eq!(
            artifact_cache_path(Path::new("/cache"), "dev", "nvos-amd64-25.02.4440.bin")?,
            Path::new("/cache")
                .join("dev")
                .join("nvos-amd64-25.02.4440.bin")
        );

        Ok(())
    }

    #[test]
    fn artifact_cache_path_rejects_unsafe_cache_subdir() {
        assert_eq!(
            artifact_cache_path(Path::new("/cache"), "../dev", "nvos-amd64-25.02.4440.bin"),
            Err(ArtifactPathError::UnsafeCacheSubdir {
                cache_subdir: "../dev".to_owned()
            })
        );
    }
}
