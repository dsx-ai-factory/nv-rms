# Development

This page collects developer-facing reference material for working on RMS itself.
For building and testing, see [Getting Started](../getting-started/build.md); for
adding hardware, see [Adding Support for New Hardware](new-hardware-support.md).

## Project structure

```text
.
├── Cargo.toml                       # workspace manifest
├── Dockerfile                       # stages for CI builds and the release image
├── docker-compose.yml               # local Postgres for the persistence tests
├── Justfile                         # build / docker helpers
└── crates/                          # all workspace crates
    ├── rackmanagementservice/       # core RMS service crate
    │   ├── build.rs                 # tonic-build proto compilation
    │   ├── src/                     # CLI, runtime, domain, transport, and API modules
    │   ├── tests/                   # integration test binaries (see Testing)
    │   └── benches/                 # 100-10k node Criterion benchmarks
    ├── redfish_test_support/        # in-process Redfish simulator + fixtures
    ├── nvue_client/                 # NVUE/NVOS REST client crate
    ├── redfish_client/              # RMS-focused Redfish client crate
    └── rust_nvfwupd/                # embedded nvfwupd firmware-update crate + CLI
```

See [Workspace Crates](crates.md) for the supporting crates and
[Architecture: Internal View](../architecture/internal_view.md) for how the layers fit together.

## Key dependencies

| Crate | Purpose |
| --- | --- |
| `tokio` | Async runtime |
| `tonic` / `prost` | gRPC server + protobuf |
| `reqwest` | Async HTTP client (Redfish, NVUE) |
| `russh` / `russh-sftp` | Async SSH with SCP and SFTP transfer support |
| `nvfwupd` | Embedded firmware-update workflow library and CLI binary |
| `serde` / `serde_json` | JSON serialization |
| `thiserror` | Error type derivation |
| `tracing` / `tracing-subscriber` | Structured logging (logfmt) |
| `clap` | CLI argument parsing (`--config`) |
| `figment` / `toml` | TOML runtime configuration loading |
| `tokio-util` | `CancellationToken` for graceful shutdown |
| `uuid` | Job ID generation |
| `async-trait` | Async methods in trait objects |
| `secrecy` | Zero-on-drop credential storage |
| `sqlx` | Async Postgres driver + compile-time-embedded migrations |
| `chrono` | Timestamps in persistence types |
| `mimalloc` | Global allocator tuned for high async fan-out |

## Adding a persistence domain

The `crates/rackmanagementservice/src/persistence/` module is per-domain by design: each new domain (e.g.
`inventory`) gets its own trait file, two implementation files, and a migration.
The shared infrastructure (pool builder, error type, migrations runner, CI
Postgres service, docker-compose) is not touched.

To add a domain `inventory`:

1. **Domain types + trait** - create `crates/rackmanagementservice/src/persistence/inventory.rs` with the
   entity types and a `Send + Sync` trait whose methods take and return only
   domain types.
1. **Migration** - add
   `crates/rackmanagementservice/src/persistence/postgres/migrations/000N_inventory.sql` with the typed schema.
   Use `sqlx migrate add --source crates/rackmanagementservice/src/persistence/postgres/migrations <name>` to
   generate the timestamped file.
1. **Memory impl** - create `crates/rackmanagementservice/src/persistence/memory/inventory.rs` with a struct
   holding `RwLock`-protected collections, mirroring the Postgres semantics so the
   same behavioral tests pass against either backend.
1. **Postgres impl** - create `crates/rackmanagementservice/src/persistence/postgres/inventory.rs`. Reuse
   `DatabaseError` from `super::error` and the shared `PgPool`. Use
   `sqlx::query_as` (no `!` macro - CI builds don't need a live DB).
1. **Wire into `Backends`** - add a `pub inventory: Arc<dyn InventoryStore>` field
   to the `Backends` struct in `crates/rackmanagementservice/src/persistence/mod.rs`.
1. **Tests** - add scenarios to `crates/rackmanagementservice/tests/persistence.rs` as
   `async fn<S: InventoryStore>(store: &S)` and wrap each with the `memory_test!`
   and `postgres_test!` macros.

> `sqlx-cli` defaults to looking for a top-level `migrations/` directory. RMS's
> live under `crates/rackmanagementservice/src/persistence/postgres/`, so always pass `--source`.

## Adding hardware support

Adding a new node type or rack generation touches API identity, endpoint policy,
rack routing, node construction, firmware policy, and tests. This has its own
detailed guide: [Adding Support for New Hardware](new-hardware-support.md).

## Building standalone nvfwupd

The `nvfwupd` CLI can be built as a standalone, portable binary. A plain build
inherits the host sysroot glibc baseline; the `docker/nvfwupd-standalone/`
Dockerfiles produce binaries with a glibc 2.17 baseline for broad portability.

```bash
# Host-arch build (inherits host glibc baseline)
cargo build --release -p nvfwupd

# Portable x86_64 binary (glibc 2.17 baseline)
docker buildx build --platform linux/amd64 --target artifact \
    --output type=local,dest=target/x86_64-unknown-linux-gnu/release \
    -f docker/nvfwupd-standalone/Dockerfile.x86_64-glibc217 .

# Portable arm64 binary (glibc 2.17 baseline, Zig cross-linking from x86)
docker buildx build --platform linux/amd64 --target artifact \
    --output type=local,dest=target/aarch64-unknown-linux-gnu/release \
    -f docker/nvfwupd-standalone/Dockerfile.arm64-glibc217 .

# Cross-compile arm64 directly from an x86 host (inherits host aarch64 glibc)
rustup target add aarch64-unknown-linux-gnu
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    RUSTFLAGS="-C link-arg=-fuse-ld=bfd" \
    cargo build --release -p nvfwupd --target aarch64-unknown-linux-gnu

# Verify a produced artifact does not require newer glibc symbols
readelf --version-info target/x86_64-unknown-linux-gnu/release/nvfwupd \
    | grep -o 'GLIBC_[0-9]\+\.[0-9]\+' | sort -Vu | tail
```

The release container image also bundles `nvfwupd`; extract it with
`docker cp` from a created container if you need the binary that shipped in a
specific image.

## Building multi-arch release images

The release image can be built for arm64 from an x86 host with Buildx plus
QEMU/binfmt on the Docker daemon:

```bash
docker buildx build --platform linux/arm64 --target release --load -t rms-release:arm64 \
    --build-arg VERGEN_GIT_SHA="$(git rev-parse HEAD)" \
    --build-arg VERGEN_GIT_DESCRIBE="$(git describe --tags --always --dirty)" \
    .
```

## CI/CD

Pull requests run the checks defined in the repository's GitHub Actions
workflows under `.github/workflows/`.

Release consumers should pull a versioned image rather than `latest`.
