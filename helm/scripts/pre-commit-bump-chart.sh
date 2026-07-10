#!/usr/bin/env bash
# Pre-commit helper: bump helm/Chart.yaml when chart package inputs are staged.
#
# Zero-touch by default:
#   - infers patch|minor|major from the staged diff
#   - on an existing -dev.N version, runs "dev next" instead of re-bumping semver
#   - stages the updated Chart.yaml for the commit
#
# Opt-in / override env vars:
#   SKIP_HELM_CHART_BUMP=1           skip entirely (like SKIP=...)
#   HELM_CHART_BUMP_INTERACTIVE=1    always prompt before bumping
#   HELM_CHART_BUMP_KIND=patch|minor|major|next  force bump kind
#   HELM_CHART_BUMP_AUTO_MAJOR=1     allow major without confirmation
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

BUMP_SCRIPT="${ROOT}/helm/scripts/bump-chart-version.sh"
SUGGEST_SCRIPT="${ROOT}/helm/scripts/suggest-chart-bump.sh"
CHART_YAML="${ROOT}/helm/Chart.yaml"

info() { echo "helm chart version: $*" >&2; }
warn() { echo "helm chart version: warning: $*" >&2; }
fail() { echo "helm chart version: error: $*" >&2; exit 1; }

staged_chart_package_files() {
  git diff --cached --name-only -- helm/ | grep -E \
    '^(helm/Chart\.yaml|helm/values\.yaml|helm/values\.schema\.json|helm/\.helmignore|helm/templates/|helm/crds/|helm/charts/)'
}

if [[ "${SKIP_HELM_CHART_BUMP:-}" == "1" ]]; then
  info "skipped (SKIP_HELM_CHART_BUMP=1)"
  exit 0
fi

if [[ ! -x "$BUMP_SCRIPT" ]]; then
  fail "missing executable ${BUMP_SCRIPT}"
fi

# Version-only Chart.yaml edits (release chore) - nothing to do.
if git diff --cached --name-only -- helm/ | grep -qx 'helm/Chart.yaml'; then
  if git diff --cached -- helm/Chart.yaml | grep -qE '^[-+]version:'; then
    other_helm="$(git diff --cached --name-only -- helm/ | grep -v '^helm/Chart.yaml$' || true)"
    if [[ -z "$other_helm" ]]; then
      info "Chart.yaml version already set for this commit"
      exit 0
    fi
  fi
fi

helm_content_changes="$(staged_chart_package_files || true)"
if [[ -z "$helm_content_changes" ]]; then
  exit 0
fi

if git diff --cached -- helm/Chart.yaml | grep -qE '^[-+]version:'; then
  info "Chart.yaml version already updated with helm changes"
  exit 0
fi

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
if [[ "$branch" == release/* ]]; then
  fail "on ${branch}: do not auto-bump. Set Chart.yaml with:
  ./helm/scripts/bump-chart-version.sh sync-tag vX.Y.Z-rcN
  or commit with SKIP_HELM_CHART_BUMP=1"
fi

current="$("$BUMP_SCRIPT" show)"
kind=""
reason=""

if [[ -n "${HELM_CHART_BUMP_KIND:-}" ]]; then
  kind="${HELM_CHART_BUMP_KIND}"
  reason="HELM_CHART_BUMP_KIND=${kind}"
elif [[ "$current" =~ -dev\.[0-9]+$ ]]; then
  kind="next"
  reason="continuing develop chart line (${current})"
else
  IFS=$'\t' read -r kind reason < <("$SUGGEST_SCRIPT")
fi

choose_kind_interactive() {
  local why="$2"
  local suggested choice
  case "$1" in
    patch | minor | major | next | skip) suggested="$1" ;;
    *) suggested="patch" ;;
  esac
  echo >&2
  echo "Staged Helm chart package changes:" >&2
  git diff --cached --stat -- helm/Chart.yaml helm/values.yaml helm/values.schema.json helm/.helmignore helm/templates/ helm/crds/ helm/charts/ | sed 's/^/  /' >&2
  echo >&2
  echo "Suggested chart bump: ${suggested} (${why})" >&2
  echo "  [P]atch  [m]inor  [M]ajor  [n]ext-dev  [s]kip  [q]uit" >&2
  read -r -p "Chart semver bump [${suggested}]: " choice </dev/tty || choice=""
  choice="${choice:-$suggested}"
  case "$choice" in
    p | P) echo "patch" ;;
    m) echo "minor" ;;
    M) echo "major" ;;
    n | N) echo "next" ;;
    s | S) echo "skip" ;;
    q | Q) fail "commit aborted at chart version prompt" ;;
    *)
      case "${choice,,}" in
        patch) echo "patch" ;;
        minor) echo "minor" ;;
        major) echo "major" ;;
        next) echo "next" ;;
        skip) echo "skip" ;;
        quit) fail "commit aborted at chart version prompt" ;;
        *) echo "$suggested" ;;
      esac
      ;;
  esac
}

if [[ "${HELM_CHART_BUMP_INTERACTIVE:-}" == "1" ]] && [[ -t 0 ]]; then
  kind="$(choose_kind_interactive "$kind" "$reason")"
  if [[ "$kind" == "skip" ]]; then
    warn "skipped — CI will fail unless you bump helm/Chart.yaml manually"
    exit 0
  fi
elif [[ "$kind" == "major" && "${HELM_CHART_BUMP_AUTO_MAJOR:-}" != "1" ]]; then
  if [[ -t 0 ]]; then
    kind="$(choose_kind_interactive "$kind" "$reason")"
    [[ "$kind" != "skip" ]] || { warn "skipped"; exit 0; }
  else
    fail "inferred major bump (${reason}). Re-run with HELM_CHART_BUMP_AUTO_MAJOR=1, HELM_CHART_BUMP_KIND=..., or HELM_CHART_BUMP_INTERACTIVE=1"
  fi
elif [[ "$kind" == "ambiguous" ]]; then
  if [[ -t 0 ]]; then
    warn "${reason}"
    kind="$(choose_kind_interactive "patch" "ambiguous diff")"
    [[ "$kind" != "skip" ]] || { warn "skipped"; exit 0; }
  else
    warn "${reason}; defaulting to patch (set HELM_CHART_BUMP_INTERACTIVE=1 to choose)"
    kind="patch"
  fi
fi

info "bumping (${reason})"
if [[ "$kind" == "next" ]]; then
  "$BUMP_SCRIPT" dev next
else
  "$BUMP_SCRIPT" dev "$kind"
fi

git add "$CHART_YAML"
new="$("$BUMP_SCRIPT" show)"
info "staged helm/Chart.yaml: ${current} -> ${new} (${kind}: ${reason})"
