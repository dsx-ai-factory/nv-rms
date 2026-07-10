#!/usr/bin/env bash
# Bump helm/Chart.yaml version for develop, RC, and GA release workflows.
#
# Usage:
#   bump-chart-version.sh dev {patch|minor|major}   # semver bump + -dev.1 from current core
#   bump-chart-version.sh dev next                   # increment -dev.N (same MR / branch)
#   bump-chart-version.sh rc next                   # 0.8.0-rc7 -> 0.8.0-rc8
#   bump-chart-version.sh rc <N> [--base X.Y.Z]     # set 0.8.0-rcN
#   bump-chart-version.sh release [--base X.Y.Z]    # strip to GA X.Y.Z
#   bump-chart-version.sh sync-tag [vX.Y.Z[-rcN]]   # align Chart.yaml with a git tag
#   bump-chart-version.sh show
#
# See helm/README.md "Chart version automation".
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

# Parse X.Y.Z with optional -prerelease (dev.N or rcN).
parse_version() {
  local version="$1"
  if [[ ! "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-(.+))?$ ]]; then
    die "unsupported chart version: $version (expected X.Y.Z[-suffix])"
  fi
  VER_MAJOR="${BASH_REMATCH[1]}"
  VER_MINOR="${BASH_REMATCH[2]}"
  VER_PATCH="${BASH_REMATCH[3]}"
  VER_PRERELEASE="${BASH_REMATCH[5]:-}"
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

dev_suffix_number() {
  local suffix="$1"
  if [[ "$suffix" =~ ^dev\.([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  echo ""
}

cmd_dev() {
  local kind="${1:-}"
  [[ -n "$kind" ]] || die "usage: $0 dev {patch|minor|major|next}"

  local current old_core old_prerelease new_core dev_n
  current="$(read_version)"
  parse_version "$current"
  old_core="$(format_core)"
  old_prerelease="$VER_PRERELEASE"

  if [[ "$kind" == "next" ]]; then
    [[ "$old_prerelease" =~ ^dev\.([0-9]+)$ ]] || die "dev next requires an existing -dev.N version (current: $current)"
    dev_n=$((BASH_REMATCH[1] + 1))
    write_version "${old_core}-dev.${dev_n}"
    return
  fi

  bump_core "$kind"
  new_core="$(format_core)"
  write_version "${new_core}-dev.1"
}

cmd_rc() {
  local arg="" base=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --base)
        base="${2:-}"
        [[ -n "$base" ]] || die "--base requires X.Y.Z"
        shift 2
        ;;
      next)
        [[ -z "$arg" ]] || die "duplicate rc argument"
        arg="next"
        shift
        ;;
      *)
        if [[ "$1" =~ ^[0-9]+$ ]]; then
          [[ -z "$arg" ]] || die "duplicate rc argument"
          arg="$1"
          shift
        elif [[ "$1" =~ ^[0-9] ]]; then
          die "rc number must be digits only: $1"
        else
          die "unknown rc option: $1"
        fi
        ;;
    esac
  done
  [[ -n "$arg" ]] || die "usage: $0 rc next | $0 rc <N> [--base X.Y.Z]"

  local current n
  current="$(read_version)"
  parse_version "$current"
  local saved_major="$VER_MAJOR" saved_minor="$VER_MINOR" saved_patch="$VER_PATCH"
  local saved_prerelease="$VER_PRERELEASE"

  if [[ -z "$base" ]]; then
    base="$(format_core)"
  else
    parse_version "$base"
    base="$(format_core)"
  fi

  VER_MAJOR="$saved_major"
  VER_MINOR="$saved_minor"
  VER_PATCH="$saved_patch"
  VER_PRERELEASE="$saved_prerelease"

  if [[ "$arg" == "next" ]]; then
    if [[ "$VER_PRERELEASE" =~ ^rc([0-9]+)$ ]] && [[ "$(format_core)" == "$base" ]]; then
      n=$((BASH_REMATCH[1] + 1))
    else
      n=1
    fi
  else
    n="$arg"
  fi

  write_version "${base}-rc${n}"
}

cmd_release() {
  local base=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --base)
        base="${2:-}"
        [[ -n "$base" ]] || die "--base requires X.Y.Z"
        shift 2
        ;;
      *)
        die "unknown release option: $1"
        ;;
    esac
  done

  if [[ -z "$base" ]]; then
    local current
    current="$(read_version)"
    parse_version "$current"
    base="$(format_core)"
  else
    parse_version "$base"
    base="$(format_core)"
  fi

  write_version "$base"
}

normalize_tag_version() {
  local tag="$1"
  tag="${tag#v}"
  if [[ ! "$tag" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-rc[0-9]+)?$ ]]; then
    die "tag $1 is not a chart release tag (expected vX.Y.Z or vX.Y.Z-rcN)"
  fi
  echo "$tag"
}

cmd_sync_tag() {
  local tag="${1:-}"
  if [[ -z "$tag" ]]; then
    tag="${CI_COMMIT_TAG:-}"
  fi
  if [[ -z "$tag" ]]; then
    tag="$(git -C "$(dirname "$CHART_YAML")/.." describe --tags --abbrev=0 2>/dev/null || true)"
  fi
  [[ -n "$tag" ]] || die "usage: $0 sync-tag [vX.Y.Z[-rcN]] (or set CI_COMMIT_TAG / run on a tagged commit)"

  write_version "$(normalize_tag_version "$tag")"
}

cmd_show() {
  read_version
}

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    dev) cmd_dev "$@" ;;
    rc) cmd_rc "$@" ;;
    release) cmd_release "$@" ;;
    sync-tag) cmd_sync_tag "$@" ;;
    show) cmd_show ;;
    -h | --help | help)
      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
      ;;
    "")
      die "usage: $0 {dev|rc|release|sync-tag|show|help} ..."
      ;;
    *)
      die "unknown command: $cmd (try $0 help)"
      ;;
  esac
}

main "$@"
