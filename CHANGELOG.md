# Changelog

This file records selected changes in the public RMS repository. Changes after
the latest public tag are listed under Unreleased, with links to their sources.

## Unreleased

Changes since [v0.10.0-rc2](https://github.com/dsx-ai-factory/nv-rms/tree/v0.10.0-rc2).
See the [public commit history](https://github.com/dsx-ai-factory/nv-rms/compare/v0.10.0-rc2...ec04289a8d9271ae1d0ab276aeb543b273a00bf3)
for all changes through 2026-10-14.

### Added

- Documented the Kubernetes deployment model, scaling, and availability limits
  ([#16](https://github.com/dsx-ai-factory/nv-rms/pull/16)).
- Documented firmware artifact sources and the firmware manifest format
  ([#17](https://github.com/dsx-ai-factory/nv-rms/pull/17)).

### Changed

- Updated the public RMS source snapshot
  ([#25](https://github.com/dsx-ai-factory/nv-rms/pull/25)).
- Scoped registry credentials to the `nvcr-publish` environment, removed the
  `ci-publish` commit-message opt-in, and made the CI workflow token read-only
  ([#41](https://github.com/dsx-ai-factory/nv-rms/pull/41),
  [#43](https://github.com/dsx-ai-factory/nv-rms/pull/43),
  [#45](https://github.com/dsx-ai-factory/nv-rms/pull/45)).

### Fixed

- Rebind NMX Controller certificates when the NVUE API certificate or CA binding
  changes, while retaining the connectivity-check fallback
  ([#27](https://github.com/dsx-ai-factory/nv-rms/pull/27)).
- Stabilized parallel tests, including HOME, sanitize-state, and port races
  ([#19](https://github.com/dsx-ai-factory/nv-rms/pull/19),
  [#44](https://github.com/dsx-ai-factory/nv-rms/pull/44)).

## v0.10.0-rc2 - 2026-08-31

Public release-candidate tag at commit
[`d592055`](https://github.com/dsx-ai-factory/nv-rms/commit/d59205531a7d55a56d3b5faf1a69e71db45bc4b6).
The date is the annotated tag date.
