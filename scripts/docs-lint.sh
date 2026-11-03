#!/bin/sh
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Docs-site checks: MDX <img> hygiene, gRPC API reference drift, Fern
# validation, markdown lint.
# Shared by `just docs-lint` and CI; POSIX sh so it also runs in images
# without bash, such as node:alpine.
set -eu
PYTHONDONTWRITEBYTECODE=1
export PYTHONDONTWRITEBYTECODE
cd "$(dirname "$0")/.."

bad=$(find docs fern \( -name '*.md' -o -name '*.mdx' \) \
    -exec grep -HnE '<img[^>]*[^/]>|<img>' {} + 2>/dev/null || true)
if [ -n "$bad" ]; then
    echo "Non-self-closing <img> tags found (MDX requires <img ... />):"
    echo "$bad"
    exit 1
fi

# Fern builds the gRPC API reference from fern/proto with buf, and the API
# reference generator uses buf too. Install it from npm when it isn't on PATH.
BUF_VERSION=1.70.0
if ! command -v buf >/dev/null 2>&1; then
    BUF_DIR=$(mktemp -d)
    trap 'rm -rf "$BUF_DIR"' EXIT
    npm install --silent --no-audit --no-fund --prefix "$BUF_DIR" "@bufbuild/buf@$BUF_VERSION"
    PATH="$BUF_DIR/node_modules/.bin:$PATH"
    export PATH
fi

# Fail when the generated gRPC API reference drifts from the protos, the
# metadata, or the librms revision pinned in Cargo.toml.
python3 ci/test_generate_api_reference.py
python3 ci/generate_api_reference.py check

FERN_VERSION=$(node -p "require('./fern/fern.config.json').version")
FERN="npx -y fern-api@$FERN_VERSION"
(cd fern && $FERN check && $FERN check --broken-links --local --warnings)

npx -y rumdl@0.2.43 check .
