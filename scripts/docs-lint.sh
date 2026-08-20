#!/bin/sh
# Docs-site checks: MDX <img> hygiene, Fern validation, markdown lint.
# Shared by `just docs-lint` and CI's lint_docs job; POSIX sh because the CI
# image (node:alpine) has no bash.
set -eu
cd "$(dirname "$0")/.."

bad=$(find docs fern \( -name '*.md' -o -name '*.mdx' \) \
    -exec grep -HnE '<img[^>]*[^/]>|<img>' {} + 2>/dev/null || true)
if [ -n "$bad" ]; then
    echo "Non-self-closing <img> tags found (MDX requires <img ... />):"
    echo "$bad"
    exit 1
fi

FERN="npx -y --package fern-api@5.57.0 fern"
(cd fern && $FERN check && $FERN check --broken-links --local --warnings)

npx -y rumdl@0.2.43 check .
