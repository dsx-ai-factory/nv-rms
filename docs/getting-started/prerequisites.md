# Prerequisites

This section covers what you need to build and run RMS as a bare-metal binary. To
deploy RMS on Kubernetes instead, see [Deployment](../deployment/prerequisites.md).

## Build prerequisites

For a local (non-Docker) build:

- **Rust** - the Rust version pinned by
  [`rust-toolchain.toml`](https://github.com/NVIDIA/nv-rms/blob/main/rust-toolchain.toml)
  (the crate uses `edition = "2024"`). Install via
  [rustup](https://rustup.rs/).
  > NOTE: On Ubuntu, do **not** use `apt install rustc` -
  the packaged compiler is too old.
- **Protocol buffer compiler** (`protoc`) - required by `tonic-build` to compile
  the `.proto` service definitions at build time. On Debian/Ubuntu, run
  `sudo apt-get install -y protobuf-compiler libprotobuf-dev`. On macOS, run
  `brew install protobuf`.
- **`lld`** (Linux) - the default linker preference in `.cargo/config.toml`.
  Install with `sudo apt-get install -y lld` (or remove the `fuse-ld=lld` line
  for another linker).
- A working SSL/CA bundle (any standard Linux/macOS install is fine; `rustls`
  with `aws-lc-rs` is statically linked).
- **[`just`](https://github.com/casey/just)** (optional, recommended) - a command
  runner used by the [`Justfile`](https://github.com/NVIDIA/nv-rms/blob/main/Justfile)
  build and Docker helpers. Install via `cargo install just` or `brew install just`.

For a **Docker-based build**, only **Docker 24+** is needed - all toolchain
dependencies are installed inside the builder image.

On Debian/Ubuntu, the full system dependency set is:

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    pkg-config \
    protobuf-compiler \
    libprotobuf-dev \
    lld
```

## Runtime prerequisites

When running RMS against real hardware:

- Network reachability from the RMS process to BMC, power-shelf, and switch
  management addresses.
- Network reachability from RMS to Postgres when persistent storage is enabled.
- A server TLS certificate, private key, and CA certificate for mTLS (the
  default; see [Configuring RMS: TLS](../configuration/configuring-rms.md#tls)).
- Switch certificates if using mTLS on switches: both RMS-side client certs/CA and
  switch-side server certs/CA.
- A writable firmware artifact directory for downloaded and staged firmware files.
- Operator-supplied credentials for BMC, power-shelf, and switch endpoints,
  passed in gRPC requests at node-creation time. Do not commit credential files.
- A reachable, supported endpoint for downloading firmware artifacts.
  See [Firmware Sources](../reference/firmware-sources.md#firmware-sources) for the supported
  protocols and endpoint types, including HTTP/HTTPS file servers, Artifactory, and local paths.

## The librms proto definitions

The gRPC service definitions (`rack_manager.proto` and related types) are
provided by the [`librms`](https://github.com/NVIDIA/nv-rms-client) crate, pinned
by `rev` in [`Cargo.toml`](https://github.com/NVIDIA/nv-rms/blob/main/Cargo.toml).
Cargo fetches it automatically during the build - no submodule init step is
required.
