# Development

This page collects developer-facing reference material for working on RMS itself.
For building and testing, see [Getting Started](../getting-started/build.md); for
adding hardware, see [Adding Support for New Hardware](new-hardware-support.md).

## Project structure

```text
.
├── Cargo.toml                       # workspace manifest
├── Dockerfile                       # multi-stage build (builder + release)
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
| `russh` / `russh-sftp` | Async SSH + SFTP (switch firmware) |
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

Build only the `nvfwupd` CLI for the host platform with:

```bash
cargo build --locked --release -p nvfwupd
```

The host build writes `target/release/nvfwupd` and uses the host toolchain and
system libraries. On Linux with glibc, its required glibc version depends on
that build environment.

### Linux musl builds

The workspace [Makefile](https://github.com/dsx-ai-factory/nv-rms/blob/main/crates/rust_nvfwupd/Makefile)
provides x86_64 and arm64 musl targets. Install the Rust target for each
architecture that you need:

```bash
rustup target add x86_64-unknown-linux-musl
rustup target add aarch64-unknown-linux-musl
```

Use an x86_64 Linux build environment with a C toolchain for x86_64 musl for
the x86_64 target. The arm64 target uses Zig and `cargo-zigbuild`; install both
and make them available on `PATH` before building.

Run these commands from the workspace root:

```bash
# x86_64 Linux musl
make -C crates/rust_nvfwupd nvfwupd-x86

# arm64 Linux musl, using cargo-zigbuild
make -C crates/rust_nvfwupd nvfwupd-arm64
```

The equivalent Cargo commands are:

```bash
cargo build --locked --release -p nvfwupd \
    --target x86_64-unknown-linux-musl

cargo zigbuild --locked --release -p nvfwupd \
    --target aarch64-unknown-linux-musl
```

The binaries are written to `target/x86_64-unknown-linux-musl/release/nvfwupd`
and `target/aarch64-unknown-linux-musl/release/nvfwupd`, respectively. Check the
produced binary in the intended deployment environment before distribution.

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

GitLab CI runs on every push and merge request, using the multi-stage Dockerfile
to build, test, and package the service.

| Stage | Job | When | What |
| --- | --- | --- | --- |
| build | `build-base` | Every push/MR | Build the Rust project inside Docker |
| test | `fmt-check` | Every push/MR | `cargo fmt --all -- --check` |
| test | `clippy-lint` | Every push/MR | `cargo clippy --workspace --all-targets -- -D warnings` |
| test | `doc-check` | Every push/MR | `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"` |
| test | `deny-check` | Every push/MR | `cargo deny check bans sources` |
| test | `run-tests` | Every push/MR | `cargo test --workspace --release` |
| test | `coverage-report` | `main`; MRs with `ci-coverage` in the commit message | cargo-llvm-cov coverage (Cobertura XML for MR diffs) |
| test | `coverage-rackmanagementservice` | `main`; MRs with `ci-coverage` | package-level RMS coverage summary and threshold |
| test | `coverage-nvfwupd` | `main`; MRs with `ci-coverage` | package-level nvfwupd coverage summary and threshold |
| test | `run-benchmarks` | `main`; MRs with `ci-benchmarks` | `cargo bench` with Criterion HTML artifacts |
| simulation-test | `stratumsim-tests` | Default branch or commit message containing `ci-sim-tests` | Run replacement-grade GB200, GB300, and VR NVL72 ATP through the orchestrator-owned CI driver |
| nico-test | `stratumsim-nico-tests` | Commit message containing `ci-sim-nico-tests` | Run NICo ATP through the orchestrator-owned CI driver |
| build | `build-release-image-mr` | Every MR | Build the minimal release image on native-arch runners |
| build | `build-release-image` | `main`, `release/*`, tags, or `ci-publish` | Build amd64/arm64 release images from x86 via Buildx |
| security | `tag-image-for-nspect-scanning` | `main`, `release/*`, tags, or `ci-publish` | Tag image for NVIDIA security scanning |
| promote | `push-to-registry` | `main`, `release/*`, tags, or `ci-publish` | Push the release image to `nvcr.io/0837451325059433/rms-dev/rms-api` |

Merges to main are gated on `fmt-check`, `clippy-lint`, `doc-check`, `deny-check`,
and `run-tests` all passing. Coverage jobs run on `main` and are opt-in for MRs
via `ci-coverage` in the commit message. Release consumers should pull a versioned image
rather than `latest`.

The job checks out pinned StratumSim and rms-sim-orchestrator sources and builds
their runtime images. It then calls `rms_ci/run.py` from rms-sim-orchestrator.
That driver owns network allocation, combined fixture generation, rack target
rendering, Compose execution, failure logs, and cleanup. RMS CI retains source
authentication, revision pins, and the RMS image build without duplicating
orchestrator topology or lifecycle scripts. The NICo job follows the same
boundary: GitLab checks out authenticated sources and prepares images, then
calls `rms_ci/nico.py`. The orchestrator driver owns NICo runtime volumes,
network adaptation, NICo ATP, diagnostics, and cleanup.
