# Rack Management Service

Manages data-center racks at scale — power control, inventory, firmware updates,
and switch configuration — exposed via a single gRPC API.

RMS is a stateless, rack-level hardware management service for NVIDIA
infrastructure. A client calls RMS over gRPC; RMS then talks to compute BMCs and
power-shelf controllers over Redfish/HTTPS and to NVSwitch trays over NVUE REST,
SSH/SFTP, and NMX-C gRPC. Long-running firmware and switch work runs as
asynchronous jobs that clients poll to completion.

Built in Rust on **tokio** (async I/O), **tonic** (gRPC), **reqwest**
(Redfish/NVUE), **russh** (SSH/SFTP), **sqlx** (optional Postgres persistence),
and an embedded **nvfwupd** firmware-update workflow library.

## Documentation

The full documentation lives in [`docs/`](docs/) and is rendered with
[Fern](fern/). Start with [`docs/index.md`](docs/index.md), or jump to a section:

- **[Overview](docs/overview.md)** — what RMS is, its capabilities, supported
  platforms, and operating principles (including the stateless model).
- **[Getting Started](docs/getting-started/prerequisites.md)** — prerequisites,
  [building](docs/getting-started/build.md),
  [testing](docs/getting-started/testing.md), and
  [running](docs/getting-started/running.md) RMS as a bare-metal binary.
- **Architecture** — [External View](docs/architecture/external_view.md)
  (northbound/southbound connections) and
  [Internal View](docs/architecture/internal_view.md) (internal components, the
  concurrency model, and error handling).
- **Configuration** — [Configuring RMS](docs/configuration/configuring-rms.md)
  (every `config.toml` key) and
  [Configuration via Helm](docs/configuration/via-helm.md) (the value → key
  mapping).
- **[Deployment](docs/deployment/prerequisites.md)** — deploying on Kubernetes
  with Helm, including cluster prerequisites, certificates, and secrets.
- **[Operations](docs/operations/overview.md)** — the `RackManager` gRPC RPCs
  (by capability) and the async job model.
- **Reference** — [Hardware Compatibility List](docs/reference/hcl.md),
  [Workspace Crates](docs/reference/crates.md),
  [Development](docs/reference/development.md),
  [Adding Hardware Support](docs/reference/new-hardware-support.md), and the
  [Glossary](docs/reference/glossary.md).

## Quick start

```bash
git clone https://github.com/NVIDIA/nv-rms.git
cd nv-rms
just build   # or: cargo build --release --workspace

# Run insecurely for local development (in-memory persistence, plaintext gRPC)
printf '[tls]\ninsecure = true\n' > config.toml
RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice --config config.toml
```

See [Getting Started](docs/getting-started/prerequisites.md) for prerequisites and
the secure (mTLS) path.

## Building the NVFWUPD CLI

The standalone NVFWUPD CLI release artifacts are built from the RMS workspace
with musl targets:

```bash
# Build x86_64 NVFWUPD
make -C crates/rust_nvfwupd nvfwupd-x86

# Build arm64 NVFWUPD
make -C crates/rust_nvfwupd nvfwupd-arm64
```

The equivalent Cargo commands are:

```bash
cargo build --locked --release -p nvfwupd \
  --target x86_64-unknown-linux-musl

cargo zigbuild --locked --release -p nvfwupd \
  --target aarch64-unknown-linux-musl
```

The resulting binaries are written under `target/<target-triple>/release/`.
The arm64 build requires Zig and `cargo-zigbuild` in addition to the Rust target.

## Third-party software

This project will download and install additional third-party open source
software projects. Review the license terms of these open source projects before
use.

See [`THIRD-PARTY-LICENSES`](THIRD-PARTY-LICENSES) for third-party license
information.

## License

Licensed under the NVIDIA Software and Model Evaluation License. See
[`LICENSE`](LICENSE) for the full text.
