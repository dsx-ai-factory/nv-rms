#
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

# ── Stage 1: Builder ──
# Rust toolchain + protoc (for tonic build scripts). Compiles the release binary
# and runs tests. Used as the base for CI test/lint/coverage jobs.
#
# Builder dependencies are installed without apt-get: some CI runners (notably
# aarch64) fail apt metadata verification with "invalid signature" errors.

FROM rust:1.96-bookworm AS builder

ARG TARGETARCH
ARG PROTOC_VERSION=28.3

# protoc binary only (prost/tonic do not need libprotobuf-dev).
RUN set -eux; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "${arch}" in \
        amd64) protoc_arch=x86_64 ;; \
        arm64) protoc_arch=aarch_64 ;; \
        *) echo "unsupported architecture: ${arch}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/protoc.zip \
        "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip"; \
    unzip -o /tmp/protoc.zip -d /usr/local bin/protoc 'include/*'; \
    rm /tmp/protoc.zip; \
    protoc --version

WORKDIR /app

# Install the pinned toolchain and its components (see rust-toolchain.toml).
# The base image default is 1.96.1; rust-toolchain.toml pins 1.96.0. Installing
# components before this file is present targets the wrong toolchain and breaks
# `cargo fmt` / `cargo clippy` in CI (including on aarch64 runners).
COPY rust-toolchain.toml .
RUN rustc --version \
    && cargo install cargo-llvm-cov@0.6.21 cargo-deny@0.20.2 --locked

COPY . .

# Container builds use the default linker; lld is installed via apt for local dev
# (see .cargo/config.toml) but apt is avoided in this image (see stage header).
RUN sed -i '/fuse-ld=lld/d' .cargo/config.toml

# vergen-gitcl needs git CLI + a .git dir at compile time, but .git is
# excluded from the build context via .dockerignore.  Inject the values
# as build-time env vars instead; vergen 9.x uses them as overrides.
ARG VERGEN_GIT_SHA
ARG VERGEN_GIT_DESCRIBE

RUN cargo build --release --workspace --locked
# Pre-compile test binaries so the CI run-tests job doesn't need to recompile
RUN cargo test --workspace --release --no-run --locked

# ── Stage 2: Release ──
# Minimal runtime image with the service binary, the NVFWUPD CLI for operator
# diagnostics, and runtime tools used by firmware activation paths.

FROM debian:bookworm-slim AS release

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        ipmitool \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rackmanagementservice /usr/local/bin/
COPY --from=builder /app/target/release/nvfwupd /usr/local/bin/

EXPOSE 8801

# RMS reads all runtime configuration from a TOML file (default
# /etc/rms/config.toml). No configuration is baked into the image; supply it at
# run time via a read-only bind mount (Docker) or a ConfigMap volume (Kubernetes):
#
#   docker run -v "$PWD/config.toml:/etc/rms/config.toml:ro" \
#     -e DATABASE_URL="postgres://..." rms-api:latest
#
# See docker/config.example.toml for a documented template. Override the path
# with `--config <path>` if needed.
ENTRYPOINT ["rackmanagementservice"]
