#!/usr/bin/env bash
# Suggest semver bump kind (patch|minor|major) from staged Helm chart package changes.
# Prints: "<kind>\t<reason>" (tab-separated, one line).
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

chart_package_changes() {
  git diff --cached --name-only -- helm/ | grep -E \
    '^(helm/Chart\.yaml|helm/values\.yaml|helm/values\.schema\.json|helm/\.helmignore|helm/templates/|helm/crds/|helm/charts/)'
}

if chart_package_changes | grep -q .; then
  :
else
  echo -e "patch\tno staged chart package changes"
  exit 0
fi

name_status="$(git diff --cached --name-status -- helm/Chart.yaml helm/values.yaml helm/values.schema.json helm/.helmignore helm/templates/ helm/crds/ helm/charts/ || true)"
values_diff="$(git diff --cached -- helm/values.yaml || true)"
schema_diff="$(git diff --cached -- helm/values.schema.json || true)"

matches_line() {
  [[ -n "$1" ]] && printf '%s\n' "$1" | grep -qE "$2"
}

major_reason=""
minor_reason=""

if matches_line "$name_status" '^D[[:space:]]*helm/templates/'; then
  major_reason="removed helm template(s)"
fi

if matches_line "$values_diff" '^-[[:space:]]*[a-zA-Z0-9_]+:[[:space:]]*$' \
  || matches_line "$values_diff" '^-[[:space:]]*[a-zA-Z0-9_]+:[[:space:]]+[^[:space:]]'; then
  if [[ -z "$major_reason" ]]; then
    major_reason="removed values.yaml key(s)"
  fi
fi

if matches_line "$schema_diff" '^-[[:space:]]*"required"'; then
  major_reason="values.schema.json required field removed"
fi

if matches_line "$schema_diff" '^-[[:space:]]*"[^"]+":[[:space:]]*\{'; then
  if [[ -z "$major_reason" ]]; then
    major_reason="values.schema.json property removed"
  fi
fi

if matches_line "$name_status" '^A[[:space:]]*helm/templates/'; then
  minor_reason="new helm template(s)"
fi

if matches_line "$values_diff" '^\+[[:space:]]*[a-zA-Z0-9_]+:[[:space:]]*$' \
  || matches_line "$values_diff" '^\+[[:space:]]*[a-zA-Z0-9_]+:[[:space:]]+[^[:space:]]'; then
  if [[ -z "$minor_reason" ]]; then
    minor_reason="new values.yaml key(s)"
  fi
fi

if matches_line "$schema_diff" '^\+[[:space:]]*"[^"]+":[[:space:]]*\{'; then
  if [[ -z "$minor_reason" ]]; then
    minor_reason="values.schema.json property added"
  fi
fi

if [[ -n "$major_reason" && -n "$minor_reason" ]]; then
  echo -e "ambiguous\tmajor: ${major_reason}; minor: ${minor_reason}"
  exit 0
fi

if [[ -n "$major_reason" ]]; then
  echo -e "major\t${major_reason}"
  exit 0
fi

if [[ -n "$minor_reason" ]]; then
  echo -e "minor\t${minor_reason}"
  exit 0
fi

echo -e "patch\ttemplate/default tweak"
