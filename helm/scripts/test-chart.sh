#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Run Helm chart unit tests, using a local helm-unittest plugin when present
# and falling back to the official Docker image otherwise.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
HELM_UNITTEST_IMAGE="${HELM_UNITTEST_IMAGE:-helmunittest/helm-unittest:4.2.0-1.1.1}"

if helm unittest --help >/dev/null 2>&1; then
  exec helm unittest "$CHART_DIR" "$@"
fi

if command -v docker >/dev/null 2>&1; then
  exec docker run --rm \
    -u "$(id -u):$(id -g)" \
    -e HOME=/tmp \
    -e XDG_CACHE_HOME=/tmp/.cache \
    -v "${CHART_DIR}:/apps" \
    "${HELM_UNITTEST_IMAGE}" \
    . "$@"
fi

cat >&2 <<'EOF'
error: helm-unittest is not installed and Docker is not available.

Install the plugin:
  helm plugin install https://github.com/helm-unittest/helm-unittest

Or run this script on a host with Docker.
EOF
exit 1
