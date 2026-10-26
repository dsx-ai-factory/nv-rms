#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Pre-commit guard: require a helm/Chart.yaml version bump whenever chart package
# inputs are staged.
#
# The chart version is a plain MAJOR.MINOR.PATCH semver, versioned independently
# of RMS; the developer chooses the bump for each change. This guard mirrors the
# CI check (see .gitlab-ci.yml build-helm-chart-to-ngc) so the mistake is caught
# locally, before pushing, rather than after a failed pipeline.
#
#   SKIP_HELM_CHART_BUMP=1   skip this check for one commit
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

SUGGEST_SCRIPT="${ROOT}/helm/scripts/suggest-chart-bump.sh"
CHART_YAML="helm/Chart.yaml"

info() { echo "helm chart version: $*" >&2; }
fail() { echo "helm chart version: error: $*" >&2; exit 1; }

if [[ "${SKIP_HELM_CHART_BUMP:-}" == "1" ]]; then
  info "check skipped (SKIP_HELM_CHART_BUMP=1)"
  exit 0
fi

staged_chart_package_files() {
  git diff --cached --name-only -- helm/ | grep -E \
    '^(helm/Chart\.yaml|helm/values\.yaml|helm/values\.schema\.json|helm/\.helmignore|helm/templates/|helm/crds/|helm/charts/)' || true
}

changed_files="$(staged_chart_package_files)"
if [[ -z "$changed_files" ]]; then
  exit 0
fi

# A staged change to the version line satisfies the requirement; validate that
# the new value stays a plain X.Y.Z semver so CI does not reject it later.
if git diff --cached -- "$CHART_YAML" | grep -qE '^[-+]version:'; then
  new_version="$(git diff --cached -- "$CHART_YAML" | awk '/^\+version:/{print $2; exit}')"
  if [[ -n "$new_version" && ! "$new_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    fail "chart version '${new_version}' is not a plain X.Y.Z semver (no -rc/-dev suffixes)."
  fi
  exit 0
fi

suggestion="patch"
reason="template/default tweak"
if [[ -x "$SUGGEST_SCRIPT" ]]; then
  IFS=$'\t' read -r suggestion reason < <("$SUGGEST_SCRIPT" || printf 'patch\tunknown')
fi
if [[ "$suggestion" =~ ^(major|minor|patch)$ ]]; then
  cmd_arg="$suggestion"
else
  cmd_arg="{patch|minor|major}"
fi

fail "chart package inputs are staged but helm/Chart.yaml 'version:' was not bumped.
  Staged chart files:
$(printf '%s\n' "$changed_files" | sed 's/^/    /')
  Suggested bump: ${suggestion} (${reason})
  Run:  ./helm/scripts/bump-chart-version.sh ${cmd_arg}
        git add helm/Chart.yaml
  Or set an explicit version with 'bump-chart-version.sh set X.Y.Z',
  or skip this check once with SKIP_HELM_CHART_BUMP=1."
