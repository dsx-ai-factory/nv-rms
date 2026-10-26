# RMS Fern Documentation

This directory contains the [Fern](https://buildwithfern.com) configuration that
renders the Markdown under [`../docs`](../docs) into the Rack Management Service
(RMS) documentation site.

The Markdown in `../docs` is the source of truth for RMS documentation. The
top-level `README.md`, `helm/README.md`, and crate READMEs stay intentionally
thin and link into `../docs`.

## Getting Started

To develop the Fern docs, install the following tools:

- Node.js v22 or later
- [Fern CLI](https://buildwithfern.com/learn/cli-api-reference/cli-reference/overview#install-fern-cli)
- [pnpm](https://pnpm.io/installation)

## Local Development

To start a local development server, run:

```bash
fern docs dev
```

This serves the docs at `http://localhost:3000`.

If you have trouble getting `esbuild` approved to install dependencies, try:

```bash
PNPM_CONFIG_DANGEROUSLY_ALLOW_ALL_BUILDS=true fern docs dev
```

If the command fails with `No such built-in module: node:sqlite`, set this env
var before running `fern docs dev`:

```bash
export NODE_OPTIONS="--experimental-sqlite"
```

## Continuous Integration

Four workflows cover the docs site, each following the reference workflow Fern
publishes under
[Preview changes](https://buildwithfern.com/learn/docs/preview-publish/preview-changes#preview-links)
or
[Publishing your docs](https://buildwithfern.com/learn/docs/preview-publish/publishing-your-docs).

| Workflow | Trigger | Purpose |
| --- | --- | --- |
| [`lint-docs.yaml`](../.github/workflows/lint-docs.yaml) | Push to `main` or `pull-request/<number>` | Runs [`scripts/docs-lint.sh`](../scripts/docs-lint.sh), the same checks as `just docs-lint` |
| [`preview-docs.yaml`](../.github/workflows/preview-docs.yaml) | `pull_request` against `main`, touching `docs/` or `fern/` | Publishes a preview and upserts a pull request comment with the URL plus deep links to every changed page |
| [`cleanup-preview.yaml`](../.github/workflows/cleanup-preview.yaml) | `pull_request` closed, touching `docs/` or `fern/` | Deletes the preview when the pull request closes |
| [`publish-docs.yaml`](../.github/workflows/publish-docs.yaml) | Push to `main`, or manual dispatch | Validates the site, then publishes it live |

Previews are keyed on the head branch name, so every push to a pull request
updates the same URL and `cleanup-preview.yaml` deletes it under that same id when
the pull request closes. Fern previews never expire on their own, which is why the cleanup
workflow exists.

`fern/docs.yml` declares a single instance, so `publish-docs.yaml` publishes
straight to production with no `--instance` flag and no staging site. It can be
re-run by hand from the Actions tab.

Every workflow except `lint-docs.yaml` authenticates to Fern with the
`FERN_TOKEN` repository secret. Because `pull_request` does not expose secrets
to forks, the preview and cleanup workflows skip pull requests opened from a
fork; linting still runs on those through the mirrored
`pull-request/<number>` branch.

## Layout

| Path | Purpose |
| --- | --- |
| `docs.yml` | Site instance, title, theme, navbar configuration, and navigation tree |
| `components/` | Custom React components (footer, badge links). Currently unused. |
