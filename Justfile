# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

# Docs-site checks; same script CI's Lint Docs workflow runs.
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
