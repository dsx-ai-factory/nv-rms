#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Point this clone at the repo-managed git hooks (.githooks/).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "error: not inside a git repository" >&2
  exit 1
fi

chmod +x .githooks/pre-commit
chmod +x helm/scripts/bump-chart-version.sh
chmod +x helm/scripts/suggest-chart-bump.sh
chmod +x helm/scripts/pre-commit-bump-chart.sh

git config core.hooksPath .githooks
echo "Installed git hooks from .githooks/ (core.hooksPath=.githooks)"
echo "Commits that change chart package inputs must bump helm/Chart.yaml's"
echo "version; the pre-commit hook blocks the commit until you do."
