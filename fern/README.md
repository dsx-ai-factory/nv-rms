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

## Layout

| Path | Purpose |
| --- | --- |
| `docs.yml` | Site instance, title, theme, navbar configuration, and navigation tree |
| `components/` | Custom React components (footer, badge links). Currently unused. |
