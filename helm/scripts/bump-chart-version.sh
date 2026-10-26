#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Bump helm/Chart.yaml to a new plain-semver chart version.
#
# The chart version is a plain MAJOR.MINOR.PATCH, versioned independently of the
# RMS application. There are no -rc or -dev pre-release suffixes: developers pick
# the appropriate semver bump for each change.
#
# Usage:
#   bump-chart-version.sh patch          # X.Y.Z -> X.Y.(Z+1)
#   bump-chart-version.sh minor          # X.Y.Z -> X.(Y+1).0
#   bump-chart-version.sh major          # X.Y.Z -> (X+1).0.0
#   bump-chart-version.sh set X.Y.Z      # set an explicit version
#   bump-chart-version.sh show           # print the current version
#
# Bump kind guidance (see helm/README.md "Versioning"):
#   major  breaking values rename/removal, removed template
#   minor  new values / optional resources / new template
#   patch  template bugfix, default tweak, non-breaking change
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_YAML="${CHART_YAML:-${SCRIPT_DIR}/../Chart.yaml}"

die() {
  echo "error: $*" >&2
  exit 1
}

require_chart() {
  [[ -f "$CHART_YAML" ]] || die "Chart.yaml not found at $CHART_YAML"
}

read_version() {
  require_chart
  awk '/^version:/{print $2; exit}' "$CHART_YAML"
}

write_version() {
  local new_version="$1"
  local tmp
  tmp="$(mktemp)"
  awk -v ver="$new_version" '
    /^version:/ { print "version: " ver; next }
    { print }
  ' "$CHART_YAML" >"$tmp"
  mv "$tmp" "$CHART_YAML"
  echo "Chart.yaml version -> $new_version"
}

# Parse a plain X.Y.Z version. Pre-release/build suffixes are rejected: the chart
# version must stay a plain semver.
parse_version() {
  local version="$1"
  if [[ ! "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    die "unsupported chart version: $version (expected plain X.Y.Z with no -rc/-dev suffix)"
  fi
  VER_MAJOR="${BASH_REMATCH[1]}"
  VER_MINOR="${BASH_REMATCH[2]}"
  VER_PATCH="${BASH_REMATCH[3]}"
}

format_core() {
  printf '%s.%s.%s' "$VER_MAJOR" "$VER_MINOR" "$VER_PATCH"
}

bump_core() {
  local kind="$1"
  case "$kind" in
    patch) VER_PATCH=$((VER_PATCH + 1)) ;;
    minor) VER_MINOR=$((VER_MINOR + 1)); VER_PATCH=0 ;;
    major) VER_MAJOR=$((VER_MAJOR + 1)); VER_MINOR=0; VER_PATCH=0 ;;
    *) die "unknown bump kind: $kind (use patch, minor, or major)" ;;
  esac
}

cmd_bump() {
  local kind="$1"
  local current
  current="$(read_version)"
  parse_version "$current"
  bump_core "$kind"
  write_version "$(format_core)"
}

cmd_set() {
  local target="${1:-}"
  [[ -n "$target" ]] || die "usage: $0 set X.Y.Z"
  parse_version "$target"
  write_version "$(format_core)"
}

cmd_show() {
  read_version
}

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    patch | minor | major) cmd_bump "$cmd" ;;
    set) cmd_set "$@" ;;
    show) cmd_show ;;
    -h | --help | help)
      sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
      ;;
    "")
      die "usage: $0 {patch|minor|major|set X.Y.Z|show|help}"
      ;;
    *)
      die "unknown command: $cmd (try $0 help)"
      ;;
  esac
}

main "$@"
