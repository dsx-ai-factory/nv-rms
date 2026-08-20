# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

# Git values injected into the build so the binary reports real version
# metadata. `.git` is excluded from the Docker build context (.dockerignore),
# so vergen-gitcl cannot read it at compile time and falls back to
# VERGEN_IDEMPOTENT_OUTPUT unless these are passed as build-args.
git_sha := `git rev-parse HEAD`
git_describe := `git describe --tags --always --dirty`

# List available recipes.
default:
    @just --list

# Build the release binaries.
build:
    cargo build --release --workspace

# Static checks.
check:
    cargo fmt --all -- --check
    cargo deny check bans sources
    cargo clippy --workspace --all-targets -- -D warnings

# Static checks plus the docs checks (site lint + rustdoc).
check-full: check docs-lint docs-api
    @echo "full check successfully completed"

# Fast unit tests; no PostgreSQL needed.
test:
    cargo test --workspace --lib

# Full release-mode suite; needs PostgreSQL via DATABASE_URL (see CONTRIBUTING.md).
test-full:
    cargo test --workspace --release

# Build a Docker image (<image>:<git-describe>) with embedded git version metadata.
docker-build target="release" image="rms-api" context=".":
    docker build --target {{target}} -t {{image}}:{{git_describe}} \
        --build-arg VERGEN_GIT_SHA="{{git_sha}}" \
        --build-arg VERGEN_GIT_DESCRIBE="{{git_describe}}" \
        "{{context}}"

# Build the builder-stage Docker image (mirrors CI's `--target builder`).
docker-build-builder image="rms-builder" context=".":
    docker build --target builder -t {{image}}:{{git_describe}} \
        --build-arg VERGEN_GIT_SHA="{{git_sha}}" \
        --build-arg VERGEN_GIT_DESCRIBE="{{git_describe}}" \
        "{{context}}"

# Build API docs (rustdoc).
docs-api:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# Docs-site checks; same script CI's lint_docs job runs.
docs-lint:
    ./scripts/docs-lint.sh

# Serve the docs site locally (see fern/README.md for pnpm caveats).
docs-live: docs-lint
    cd fern && fern docs dev

# Generate a shareable docs preview.
docs-preview: docs-lint
    cd fern && fern generate --docs --preview

# Everything docs: site lint + preview and rustdoc.
docs-full: docs-preview docs-api
