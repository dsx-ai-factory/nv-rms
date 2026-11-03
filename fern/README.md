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
- [buf](https://buf.build/docs/installation), which Fern uses to build the gRPC API reference from
  `proto/`

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

CI runs [`scripts/docs-lint.sh`](../scripts/docs-lint.sh), the same checks as
`just docs-lint`. Previews and publication follow the reference workflows Fern
publishes under
[Preview changes](https://buildwithfern.com/learn/docs/preview-publish/preview-changes#preview-links)
and
[Publishing your docs](https://buildwithfern.com/learn/docs/preview-publish/publishing-your-docs),
and authenticate to Fern with the `FERN_TOKEN` repository secret.

Previews are keyed on the head branch name, so every push to a pull request
updates the same URL, and they are deleted when the pull request closes because
Fern previews never expire on their own. Pull requests from forks cannot read
the token, so they get no preview. `fern/docs.yml` declares a single instance,
so publication goes straight to production with no `--instance` flag and no
staging site.

## Layout

| Path | Purpose |
| --- | --- |
| `docs.yml` | Site instance, title, theme, navbar configuration, and navigation tree |
| `generators.yml` | Points Fern at `proto/` to render the Protobuf Reference |
| `proto/` | Vendored copy of the librms protos and the `LIBRMS_REV` file that records their source revision and a SHA-256 digest of each proto |
| `components/` | Custom React components (footer, badge links). Currently unused. |

## gRPC API reference

The API Reference tab combines two kinds of content:

- **Protobuf Reference** is rendered by Fern from the protos in `proto/`.
- The overview, getting-started, conventions, and async-jobs pages are handwritten in
  [`../docs/api-reference`](../docs/api-reference). The RPC catalog, the per-category pages, and
  the message type pages in `../docs/api-reference/generated` are generated.

The protos come from the `librms` crate, which RMS pins by revision in `Cargo.toml`. The generator
reads the vendored protos plus [`../docs/api-reference/rpc-metadata.toml`](../docs/api-reference/rpc-metadata.toml),
which records what the protos can't express: whether an RPC is synchronous or asynchronous, which RPC
polls its job, and the target mode.

| Command | Purpose |
| --- | --- |
| `just api-ref-sync` | Copies the protos from the pinned `librms` checkout into `proto/` and updates `LIBRMS_REV` |
| `just api-ref-gen` | Regenerates the pages in `../docs/api-reference/generated` |
| `just api-ref-check` | Runs the generator tests, then fails if the vendored protos differ from the `Cargo.toml` pin or were edited by hand, if the metadata is inconsistent, or if the generated pages are stale |

When you bump the `librms` revision, run `just api-ref-sync` and `just api-ref-gen`. If the protos
added an RPC, the check fails until you add the RPC to `rpc-metadata.toml`. `just docs-lint` runs
`just api-ref-check`.
