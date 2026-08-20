# Building RMS

Make sure you have the [build prerequisites](prerequisites.md) installed first.

## Cloning the RMS repo

```bash
git clone https://github.com/NVIDIA/nv-rms.git
cd nv-rms
```

The `librms` gRPC definitions are fetched automatically by Cargo (pinned by `rev`
in `Cargo.toml`) on the first build - there is no submodule to initialize.

## Building the binaries

The workspace produces two binaries: `rackmanagementservice` (the service) and
`nvfwupd` (the bundled firmware-update CLI, used for operator diagnostics).

### Release build

Recommended, via the [`Justfile`](https://github.com/NVIDIA/nv-rms/blob/main/Justfile):

```bash
just build
```

Or directly with Cargo:

```bash
cargo build --release --workspace
```

Optimized binaries land under `target/release/` and match what the container
image ships:

```text
target/release/rackmanagementservice
target/release/nvfwupd
```

### Debug build

For faster iteration during development:

```bash
cargo build --workspace
```

Unoptimized binaries with debug symbols land under `target/debug/`.

### Code quality checks

```bash
just check
just docs-api
```

## Container images

RMS ships a multi-stage [`Dockerfile`](https://github.com/NVIDIA/nv-rms/blob/main/Dockerfile):

- **Stage 1 - `builder`** (`rust:1.96-bookworm`): the full build environment
  (Rust toolchain, `protoc`, linkers). Produces the workspace release binaries
  and pre-compiled test binaries. This stage is the base for CI lint, test, and
  coverage jobs.
- **Stage 2 - `release`** (`debian:bookworm-slim`): a minimal runtime image
  containing only the `rackmanagementservice` and `nvfwupd` binaries plus
  `ca-certificates` and `ipmitool`. It excludes the Rust toolchain and is the
  artifact for production deployments. It exposes port `8801` and its entrypoint
  is `rackmanagementservice`.

### Build with the Justfile (recommended)

The `Justfile` injects git version metadata so the binary reports a real version
(the image is tagged `rms-api:<git-describe>`):

```bash
# Release image (rms-api:<git-describe>)
just docker-build

# Builder-stage image (rms-builder:<git-describe>)
just docker-build-builder
```

The `.git` directory is excluded from the Docker build context, so `vergen`
cannot read it at compile time. The `Justfile` passes `VERGEN_GIT_SHA` and
`VERGEN_GIT_DESCRIBE` as build args to compensate.

### Build with raw docker

The raw commands work too, but without the `--build-arg VERGEN_*` values the
binary's git version fields show `VERGEN_IDEMPOTENT_OUTPUT`:

```bash
# Minimal release image
docker build --target release -t rms-release .

# Builder-stage image
docker build --target builder -t rms-builder .
```

### Standalone nvfwupd binaries

The `nvfwupd` CLI can be built on its own - convenient for distributing a
diagnostics tool. A plain `cargo build --release -p nvfwupd` inherits the host
sysroot glibc baseline; the `docker/nvfwupd-standalone/` Dockerfiles produce
portable x86-64 and arm64 binaries with a glibc 2.17 baseline. See the
[Development](../reference/development.md#building-standalone-nvfwupd) reference
for the full set of cross-build and verification commands.

## Next steps

- [Testing RMS](testing.md) - run the unit, integration, and persistence suites
- [Running RMS](running.md) - start the binary locally
