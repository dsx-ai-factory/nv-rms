# Rack Management Service

Manages data center racks at scale -- power control, inventory, firmware updates, and
switch configuration -- exposed via gRPC.

Built on **tokio** for async I/O, **tonic** for gRPC, and **trait-based polymorphism** for
extensibility across node and rack types.

## Getting Started

### Prerequisites

For a local (non-Docker) build:

- **Rust 1.95+** (the crate uses `edition = "2024"`). Install via [rustup](https://rustup.rs/).
- **Protocol buffer compiler** (`protoc`) -- required by `tonic-build` to compile `.proto`
files at build time.
    - Debian/Ubuntu: `sudo apt-get install -y protobuf-compiler libprotobuf-dev`
    - macOS: `brew install protobuf`
- A working SSL/CA bundle (any standard Linux/macOS install is fine; `rustls` with
`aws-lc-rs` is statically linked).

For a Docker-based build, only **Docker 24+** is needed -- all toolchain dependencies
are installed inside the builder image.

### Cloning

The gRPC service definitions (`rack_manager.proto` and related types) are provided by the
[`librms`](https://github.com/NVIDIA/nv-rms-client) crate, pinned by `rev` in `Cargo.toml`.
Cargo fetches it automatically during the build.

## Design for Scale

The service is designed to manage **10k+ nodes** concurrently. Every I/O operation (Redfish
HTTP to a BMC, SSH/SFTP to a switch, firmware upload, task polling) is async. A firmware
update that polls a BMC every 5 seconds does `tokio::time::sleep(5s).await` between polls,
yielding its thread to serve other work. 10k concurrent firmware jobs run as lightweight
async tasks on a fixed pool of OS threads (typically equal to CPU core count), not 10k
blocked threads.

Key concurrency primitives:

- **Per-node serialization**: Each node holds a `tokio::sync::Mutex` around its transport
client. Operations on different nodes run fully in parallel. Operations on the same node
serialize -- BMCs process one Redfish request at a time, so this matches hardware reality.
- **Rack map**: Protected by `tokio::sync::RwLock`. Reads (find/list) are concurrent.
Writes (add/remove) are exclusive.
- **Job tracking**: `std::sync::RwLock<HashMap>` for concurrent reads of async job status.
- **Batch firmware**: One `tokio::spawn` per node. A 36-node batch = 36 async tasks sharing
a handful of OS threads, not 36 blocked OS threads.
- **Graceful shutdown**: `CancellationToken` propagated to all spawned tasks.

## Architecture

```text
                       ┌─────────────────────────────┐
                       │      API Gateway (api/)     │
                       │                             │
                       │  ┌────────┐   ┌──────────┐  │
                       │  │  gRPC  │   │  REST    │  │
                       │  │(tonic) │   │ (future) │  │
                       │  └───┬────┘   └──────────┘  │
                       └──────┼──────────────────────┘
                              │
                              ▼
                       ┌─────────────────────────────┐
                       │        RackManager          │
                       │   rack CRUD, job dispatch   │
                       └──────┬──────────────┬───────┘
                              │              │
                 ┌────────────▼──┐     ┌─────▼────────┐
                 │  Rack (trait) │     │  JobTracker  │
                 │  node factory │     │  async status│
                 │  + CRUD       │     └──────────────┘
                 └───────┬───────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   ┌────────────┐ ┌────────────┐ ┌──────────────┐
   │  Compute   │ │  Switch    │ │  Powershelf  │
   │  (Redfish) │ │ (NVUE/SSH) │ │  (Redfish)   │
   └─────┬──────┘ └──┬──────┬──┘ └──────┬───────┘
         │           │      │           │
         ▼           ▼      ▼           ▼
      HttpClient  HttpClient SshClient  HttpClient
      (reqwest)   (reqwest)  (russh)    (reqwest)
```

### Layers

1. **API Gateway** (`api/`) -- Protocol-specific entry points. Each sub-protocol (gRPC,
  future REST) lives in its own subfolder. The `RackManager` gRPC service runs on the
   configured port. Gateways are thin translation layers with no business logic.
2. **Orchestrator** (`orchestrator/`) -- `RackManager` owns the rack map and `JobTracker`.
  Short operations (power query) are awaited inline. Long operations (firmware update)
   are spawned as tokio tasks and tracked by job ID. `JobTracker` supports parent/child
   job hierarchies for batch operations, with TTL-based cleanup.
3. **Rack** (`racks/`) -- Trait-based. Each rack type owns a map of `Arc<dyn Node>` and a
  node factory. `NvlGb200Rack` and `NvlGb300Rack` create Compute, Switch, or Powershelf
   nodes based on the typed `NodeType` in config. Adding/removing nodes invalidates the
   power-on order.
4. **Node** (`nodes/`) -- Trait-based. Each node type encapsulates its protocol behind
  the `Node` trait. `Compute` and `Powershelf` use Redfish over HTTPS. `Switch` uses
   NVUE REST + SSH/SFTP. Nodes are pure async I/O -- no knowledge of job tracking or
   scheduling.
5. **Transport** (`transport/`) -- `HttpClient` wraps `reqwest` for async Redfish/NVUE
  HTTP calls (GET, POST JSON, PATCH JSON, multipart upload, raw file upload). `SshClient`
   wraps `russh` for async SSH exec and SFTP file transfers.

6. **NVFWUPD adapter** (`nodes/nvfwupd_adapter.rs`) -- Converts RMS node
   credentials, firmware requests, outcomes, task status, and activation
   requests into the embedded `nvfwupd` workflow API. PLDM firmware package
   parsing and switch CPLD `.vme` extraction now live in the NVFWUPD crate.

### Polymorphism

Rust traits replace class hierarchies for maintainability and extensibility:

| Trait        | Concrete Implementations          |
| ------------ | --------------------------------- |
| `trait Node` | `Compute`, `Switch`, `Powershelf` |
| `trait Rack` | `NvlGb200Rack`, `NvlGb300Rack`    |

Nodes are stored as `Arc<dyn Node>` in the rack's node map. Adding a new node type means
implementing the `Node` trait -- no changes to existing code. Adding a new rack type means
implementing the `Rack` trait with its own node factory.

### Error Handling

All fallible operations return `Result<T, RmsError>`. `RmsError` carries an `ErrorCode`
enum that the gRPC gateway maps to gRPC status codes:

| `ErrorCode`          | gRPC Status           |
| -------------------- | --------------------- |
| `NotFound`           | `NOT_FOUND`           |
| `AlreadyExists`      | `ALREADY_EXISTS`      |
| `InvalidArgument`    | `INVALID_ARGUMENT`    |
| `Timeout`            | `DEADLINE_EXCEEDED`   |
| `FailedPrecondition` | `FAILED_PRECONDITION` |
| `Unavailable`        | `UNAVAILABLE`         |
| `Unimplemented`      | `UNIMPLEMENTED`       |
| `Internal`           | `INTERNAL`            |

### Credentials

Credentials (username/password) are passed directly in gRPC requests at node creation time
and stored on the node using zero-on-drop secure strings (`secrecy` crate). No external
credential provider is used.

## Project Structure

```text
.
├── Cargo.toml                                # crate manifest
├── Cargo.lock
├── build.rs                                  # tonic-build proto compilation
├── Dockerfile                                # multi-stage build (builder + release)
├── docker-compose.yml                        # local Postgres for the persistence tests
├── .gitlab-ci.yml                            # CI pipeline
├── src/
│   ├── main.rs                               # CLI, tokio runtime, TLS, signal handling
│   ├── lib.rs                                # crate root, module re-exports
│   ├── domain/
│   │   ├── mod.rs
│   │   ├── node.rs                           # Node trait + power/firmware types
│   │   └── rack.rs                           # Rack trait, NodeConfig, PowerOnStep
│   ├── nodes/
│   │   ├── mod.rs
│   │   ├── compute_gb200_nvidia.rs           # Redfish BMC node
│   │   ├── compute_gb300_nvidia.rs           # GB300 NVIDIA compute wrapper
│   │   ├── compute_gb300_lenovo.rs           # GB300 Lenovo compute wrapper
│   │   ├── switch_gb200_nvidia.rs            # NVUE + SSH/SFTP switch node
│   │   ├── switch_gb300_nvidia.rs            # GB300 switch wrapper
│   │   ├── powershelf_gb200_liteon.rs        # Redfish power shelf node
│   │   ├── powershelf_gb300_liteon.rs        # GB300 LiteOn power shelf wrapper
│   │   ├── powershelf_gb200_delta.rs         # Shared Delta Redfish + NVFWUPD node
│   │   └── powershelf_gb300_delta.rs         # GB300 wrapper over shared Delta behavior
│   ├── racks/
│   │   ├── mod.rs
│   │   └── nvl_gb.rs                         # GB NVL rack (node factory + CRUD)
│   ├── orchestrator/
│   │   ├── mod.rs
│   │   ├── rack_manager.rs                   # Rack CRUD, operation dispatch
│   │   ├── job_tracker.rs                    # Unified async job lifecycle (firmware + switch OS image)
│   │   ├── job_lifecycle/                    # Generic async job primitives (registry, supervisor, cleanup)
│   │   │   ├── mod.rs
│   │   │   ├── domain.rs
│   │   │   ├── ids.rs
│   │   │   ├── registry.rs
│   │   │   ├── cleanup.rs
│   │   │   └── handle.rs
│   │   └── stage_timeline.rs                 # Per-stage timing for jobs
│   ├── transport/
│   │   ├── mod.rs
│   │   ├── http_client.rs                    # reqwest-based async HTTP (Redfish/NVUE)
│   │   └── ssh_client.rs                     # russh-based async SSH/SFTP
│   ├── utilities/
│   │   ├── mod.rs
│   │   ├── error.rs                          # RmsError, ErrorCode, Result<T> type alias
│   │   └── fwpkg_reader.rs                   # PLDM firmware package parser
│   ├── persistence/                          # swappable storage abstraction
│   │   ├── mod.rs                            # re-exports + Backends aggregator
│   │   ├── firmware_object.rs                  # FirmwareObjectStore trait + domain types
│   │   ├── memory/
│   │   │   ├── mod.rs
│   │   │   └── firmware_object.rs              # MemoryFirmwareObjectStore
│   │   └── postgres/
│   │       ├── mod.rs                        # PgPool builder + run_migrations()
│   │       ├── error.rs                      # DatabaseError + From for RmsError
│   │       ├── firmware_object.rs              # PostgresFirmwareObjectStore (sqlx)
│   │       └── migrations/
│   │           └── 0001_rack_firmware.sql
│   └── api/
│       ├── mod.rs
│       └── grpc/
│           ├── mod.rs
│           ├── server.rs                     # tonic server setup (TLS/mTLS)
│           ├── conversions.rs                # Proto <-> domain type conversions
│           ├── power_handlers.rs             # Power control RPCs
│           ├── inventory_handlers.rs         # Inventory RPCs
│           ├── firmware_handlers.rs          # Firmware RPCs (async, batch, job status)
│           ├── switch_handlers.rs            # Switch firmware + NVUE RPCs
│           ├── switch_image_handlers.rs      # Switch OS image fetch/install RPCs
│           ├── scaleupfabricmanager_handlers.rs  # Scale-up fabric manager RPCs
│           └── proto/
│               ├── switch_client.proto       # Switch client service
│               └── gnmi.proto                # gNMI service (switch telemetry/config)
├── tests/
│   ├── grpc_e2e.rs                           # full gRPC E2E suite (Redfish simulator)
│   ├── grpc_mtls.rs                          # mTLS handshake + RPC tests
│   ├── mockup_server_test.rs                 # Redfish simulator + src integration tests
│   ├── persistence.rs                        # cross-backend persistence tests
├── redfish_test_support/
│   ├── src/                                  # in-process Redfish simulator
│   └── fixtures/mockup/                      # static Redfish JSON device trees
└── benches/
    └── scale.rs                              # 100--10k node Criterion benchmarks
```

## Dependencies

| Crate                            | Purpose                                                  |
| -------------------------------- | -------------------------------------------------------- |
| `tokio`                          | Async runtime                                            |
| `tonic` / `prost`                | gRPC server + protobuf                                   |
| `reqwest`                        | Async HTTP client (Redfish, NVUE)                        |
| `russh` / `russh-sftp`           | Async SSH + SFTP (switch firmware)                       |
| `nvfwupd`                        | Embedded firmware update workflow library and CLI binary |
| `serde` / `serde_json`           | JSON serialization                                       |
| `thiserror`                      | Error type derivation                                    |
| `tracing` / `tracing-subscriber` | Structured logging (logfmt)                              |
| `clap`                           | CLI argument parsing                                     |
| `tokio-util`                     | `CancellationToken` for graceful shutdown                |
| `uuid`                           | Job ID generation                                        |
| `async-trait`                    | Async methods in trait objects                           |
| `secrecy`                        | Zero-on-drop credential storage                          |
| `sqlx`                           | Async Postgres driver + compile-time-embedded migrations |
| `chrono`                         | Timestamps in persistence types                          |

## gRPC API

One gRPC service (`RackManager`) runs on the configured port.

### RackManager Service

- **Power Control**: `SetPowerState`, `BatchSetPowerState`, `GetPowerState`,
`BatchGetPowerState`, `SequenceRackPower`
- **Inventory**: `CreateNodes`, `UpdateNode`, `DeleteNode`, `ListNodeInventory`, `ListRacks`,
`SetRackPowerOnSequence`, `GetRackPowerOnSequence`
- **Device info**: `GetNodeDeviceInfo`, `ListNodeDeviceInfoByNodeType`,
`BatchGetNodeDeviceInfo`
- **Firmware (async)**: `UpdateFirmware`, `BatchUpdateFirmwareByNodeType`,
`BatchUpdateFirmware`, `GetFirmwareJobStatus`
- **Firmware (query)**: `GetNodeFirmwareInventory`, `GetRackFirmwareInventory`
- **Firmware objects**: `AddFirmwareObject`, `GetFirmwareObject`,
`ListFirmwareObjects`, `DeleteFirmwareObject`, `SetDefaultFirmwareObject`,
`ApplyFirmwareObject`, `ApplyStoredFirmwareObject`, `ApplySwitchSystemImage`,
`ApplyStoredSwitchSystemImage`, `GetFirmwareObjectHistory`
- **Switch firmware**: `ListSwitchFirmware`, `PushSwitchFirmware`,
`UpgradeSwitchFirmware`
- **Switch images**: `FetchSwitchSystemImage`, `InstallSwitchSystemImage`,
`ListSwitchSystemImages`, `UpdateSwitchSystemImage`,
`GetSwitchSystemImageJobStatus`
- **Switch fabric**: `ConfigureScaleUpFabricManager`, `GetScaleUpFabricState`,
`BatchGetScaleUpFabricServiceStatus`, `SetScaleUpFabricTelemetryInterfaceState`
- **Utility**: `GetVersion`, `PollSwitchFirmwareJobStatus`

## Postgres for the persistence layer

The service can run against a real Postgres database or a built-in
in-memory store. The decision is made at startup from the `--db-url`
flag (or the `DATABASE_URL` env var). Without one, RMS warns and uses
the in-memory store and data is lost on restart.

This section covers spinning up a local Postgres and resetting it to a
clean state. To then run the tests or the binary against it, see
[Testing and Benchmarks](#testing-and-benchmarks) and
[Running](#running) below.

### Stand it up

The bundled compose file pulls `postgres:16-alpine`, listens on
`localhost:5432`, and uses credentials `postgres / postgres / rms_test`:

```bash
docker compose up -d postgres
```

Connection URL for that container:

```bash
postgres://postgres:postgres@localhost:5432/rms_test
```

### Tear it down

Stop and remove the container while keeping the data volume on disk so
the next `up` resumes with the same data:

```bash
docker compose down
```

To also delete the named volume (full clean slate -- next `up`
initializes a fresh database from scratch):

```bash
docker compose down -v
```

### Reset to a clean state without restarting

If `docker compose down -v` is too heavy (e.g. you don't want to drop
the container), drop and recreate the schema in place. This wipes every
table including `_sqlx_migrations`, so the binary's startup migrations
will re-run on next launch:

```bash
docker compose exec postgres psql -U postgres -d rms_test \
    -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

Note: `cargo test --test persistence` does **not** need any of these
between runs -- `#[sqlx::test]` provisions a fresh ephemeral database
per test and drops it afterwards. Resetting matters only when the
**binary** has been writing to `rms_test` (via `--db-url` /
`DATABASE_URL`) and you want to start over.

### Adding a new migration

`sqlx-cli` defaults to looking for a top-level `migrations/` directory.
Ours lives under `src/persistence/postgres/`, so pass `--source`:

```bash
sqlx migrate add --source src/persistence/postgres/migrations <name>
```

## Setup

The `librms` gRPC definitions are fetched automatically by Cargo (pinned by `rev` in
`Cargo.toml`) on the first build.

## Testing and Benchmarks

### Redfish Simulator

Redfish tests and benchmarks use the `redfish_test_support` workspace crate.
It provides an in-process simulator that serves BMC endpoints over real HTTPS.
The simulator:

- Serves static Redfish JSON from mockup data directories over **TLS**
(self-signed certs generated at startup via `rcgen` + `tokio-rustls`)
- Simulates firmware update tasks with configurable delay and failure rate
- Supports **multi-port binding** -- benchmarks assign one TLS endpoint per simulated
BMC to match production topology (no connection reuse across nodes)
- Starts in-process in milliseconds with zero external dependencies

The fixture data lives in `redfish_test_support/fixtures/mockup/` with two device
trees: NVIDIA GB200 NVL compute (BMC+HMC) and power shelf. All JSON is loaded into
an in-memory `HashMap` at startup.

```text
Test/Benchmark ──gRPC──► RMS ──HTTPS──► RedfishSimulator (axum + rustls)
                                             │
                                             └─ in-memory HashMap of static Redfish JSON
                                             └─ firmware task simulator (delay + failure rate)
                                             └─ /bmc-sim/config admin endpoint
```

### Run everything

The CI `run-tests` job uses release mode (faster integration tests, matches
production codegen) with `DATABASE_URL` set so the persistence Postgres
suite runs against a real database. Bring up the database first per
[Postgres for the persistence layer](#postgres-for-the-persistence-layer),
then:

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/rms_test
cargo test --release
```

This runs unit tests plus all four integration test binaries below.

**Without a database**, skip the Postgres-backed persistence tests by
explicitly listing the suites that don't need one:

```bash
cargo test --release --lib
cargo test --release --test grpc_e2e
cargo test --release --test grpc_mtls
cargo test --release --test mockup_server_test
cargo test --release --test persistence memory::
```

### Unit Tests

```bash
cargo test --lib
```

### Integration Tests

Each scenario is exercised against an in-process backend so tests have no
external dependencies, with the persistence suite additionally running
against a real Postgres when `DATABASE_URL` is set.

| Binary               | Command                                | What it covers                                                                                                                                                                                                                                                                                                                                                                                   |
| -------------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   |
| `grpc_e2e`           | `cargo test --test grpc_e2e`           | Full gRPC E2E: starts RMS plus the Redfish simulator in-process and exercises power, inventory, firmware, and mixed rack scenarios for compute and powershelf nodes.                                                                                                                                                                                                                             |
| `grpc_mtls`          | `cargo test --test grpc_mtls`          | Spawns `GrpcServer` with a client CA and verifies the rustls TLS 1.3 handshake, client-cert verification, ALPN to h2, and a real RPC round-trip.                                                                                                                                                                                                                                                 |
| `mockup_server_test` | `cargo test --test mockup_server_test` | Redfish simulator and src-facing tests for TLS startup, JSON serving, multi-port binding, action validation, firmware task simulation, and RMS node APIs.                                                                                                                                                                                                                                        |
| `persistence`        | `cargo test --test persistence`        | Cross-backend behavioral tests for `FirmwareObjectStore`. The memory backend always runs; the Postgres backend requires `DATABASE_URL` to be set and panics otherwise (`#[sqlx::test]` provisions an isolated database per test and runs the migrations). To run only the memory backend, filter with `memory::`. See [Postgres for the persistence layer](#postgres-for-the-persistence-layer). |

### Benchmarks

Scale benchmarks (Criterion) measure RMS performance at 100--10,000 node scale.
Each benchmark node gets its own mockup BMC on a unique port, forcing independent
TLS sessions -- matching production where each node talks to a different BMC.

| Benchmark                       | What it measures                                                                  |
| ------------------------------- | --------------------------------------------------------------------------------- |
| `add_nodes`                     | gRPC + rack/node registration throughput (no Redfish calls)                       |
| `concurrent_power_state`        | N parallel GetPowerState RPCs, each hitting a unique BMC over HTTPS               |
| `concurrent_firmware_inventory` | N parallel firmware inventory queries (~20 sequential GETs per node)              |
| `parallel_firmware_updates`     | Async firmware job lifecycle: concurrent uploads + task polling + status tracking |

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark
cargo bench --bench scale -- "concurrent_power_state"

# List all benchmarks
cargo bench --bench scale -- --list
```

## Building

```bash
cargo build --release
```

## Running

```bash
# Insecure, in-memory persistence (development) -- default port 8801.
# Logs a warning that data won't survive restarts.
RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice --insecure

# Insecure, with Postgres persistence. Connects to the database, runs
# embedded migrations, then starts serving.
docker compose up -d postgres
RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice \
    --insecure \
    --db-url postgres://postgres:postgres@localhost:5432/rms_test

# Same, using the DATABASE_URL env var instead of the flag.
DATABASE_URL=postgres://postgres:postgres@localhost:5432/rms_test \
    RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice --insecure

# With mTLS (--tls-ca is required whenever TLS is configured)
./target/release/rackmanagementservice \
    --tls-cert /path/to/cert.pem \
    --tls-key /path/to/key.pem \
    --tls-ca /path/to/ca.pem

# Custom port
RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice --port 9000 --insecure

# Custom Postgres pool size (default 20)
RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice \
    --insecure \
    --db-url postgres://postgres:postgres@localhost:5432/rms_test \
    --db-pool-max 50
```

### Runtime Configuration

| Flag | Env | Default | Purpose |
| ---- | --- | ------- | ------- |
| `--port` | -- | `8801` | gRPC server port for RMS API. |
| `--tls-cert` / `--tls-key` | -- | -- | Server certificate + private key; `--tls-ca` required. |
| `--tls-ca` | -- | -- | CA cert for client verification; required with `--tls-cert`/`--tls-key`. |
| `--insecure` | `RMS_ALLOW_INSECURE` | -- | Disable TLS (plaintext); requires env `RMS_ALLOW_INSECURE=1`; dev/testing only. |
| `--switch-cert-root` | `SWITCH_CERT_ROOT` | -- | Directory root where cert material lives for being installed on the switch by the switch certificate RPCs. Cert material should live in a per-domain subdirectory here. Required unless `--insecure-switch` is set. |
| `--client-tls-root` | `CLIENT_TLS_ROOT` | -- | Directory root where client mTLS material lives for secure RMS communication to switch NVUE/NMX-C services. Required unless `--insecure-switch` is set. |
| `--default-switch-domain` | `SWITCH_DEFAULT_DOMAIN` | -- | Default domain for switch TLS material lookup. Acts as a default fallback value for when domain is not specified in a ConfigureSwitchCertificateRequest. Required unless `--insecure-switch` is set. |
| `--dns-domain` | `SWITCH_DNS_DOMAIN` | -- | Optional TLS server-name override. Controls what hostname RMS presents as the TLS server name when making outbound mTLS connections to switches (via NVUE and NMX-C/gRPC). |
| `--insecure-switch` | `RMS_INSECURE_SWITCH` | -- | Opt out of switch mTLS; NVUE uses unverified HTTPS and NMX-C uses HTTP. |
| `--db-url` | `DATABASE_URL` | -- | Postgres connection URL. If unset, uses in-memory store. |
| `--db-pool-max` | -- | `20` | Postgres connection pool size. |
| `--sftp-upload-timeout-seconds` | `RMS_SFTP_UPLOAD_TIMEOUT_SECONDS` | `3600` | Overall wall-clock timeout (seconds) for switch NVOS SFTP image uploads. |
| `--sftp-step-timeout-seconds` | `RMS_SFTP_STEP_TIMEOUT_SECONDS` | `30` | Stall timeout (seconds) for one SFTP step (setup/read/write/flush). Must be <= the upload timeout. |
| `--max-tracked-jobs` | `RMS_MAX_TRACKED_JOBS` | `10000` | Optional; max jobs tracked/retained by the RMS to bound resource usage. |

#### Switch mTLS Configuration

- `--insecure-switch`: When set, `ConfigureSwitchCertificate` does not install
switch certificate material. It instead creates jobs that unset switch service
mTLS mode over SSH for the requested services. This should only be used in development or testing
environments where secure communication from RMS to the switch API is not required and mTLS
has been disabled on the switch services entirely.
- `--switch-cert-root`: This is a directory root where the switch cert material
(`ca.pem`, `client.pem`, `client.key`) for being installed on switches lives. These sets of keys, CAs, and certs
are organized into domains via subdirectories. For example, a single site-wide domain might use `--switch-cert-root /var/run/secrets/switch_server_certs` / `--default-switch-domain site-wide` for the following directory:

  ```console
  /var/run/secrets/switch_server_certs
  └── site-wide
      ├── ca.pem
      ├── client.key
      └── client.pem
  ```

  Note that `client.key`/`client.pem` get bound to the switch's server identity -- the `client` name is
  an artifact of both roots (client and server) sharing the same reusable struct despite the role of `client.pem` being different depending on where it's used.
- `--client-tls-root`: This is a directory root where the RMS switch client cert material (`ca.pem`, `client.pem`, `client.key`) lives. Similar to `--switch-cert-root`, this directory is organized into domains by subdirectories.
However, the current implementation only uses the `--default-switch-domain` value for the RMS outbound client identity,
although support may be added in the future for a domain to be specified by RPC requests. Example using `--client-tls-root /var/run/secrets/switch_client_certs` / `--default-switch-domain site-wide`:

  ```console
  /var/run/secrets/switch_client_certs
  └── site-wide
      ├── ca.pem
      ├── client.key
      └── client.pem
  ```

- `--default-switch-domain`: This is the default (fallback) domain to use for switch/client certs that should exist as a subdirectory
of the switch cert and client TLS root directories. Example: `site-wide`.
- `--dns-domain`: This is the hostname that's presented during the TLS handshake to the switch for the DNS/SNI authority.

### Production deployment

In production, run RMS in its own container with `--db-url` (or
`DATABASE_URL`) pointing at a managed Postgres (RDS, CloudSQL,
Azure Database, etc.) or a self-hosted Postgres `StatefulSet`. The
RMS image bundles the migrations -- on startup the binary connects,
runs any unapplied migrations, and starts serving. No separate
migration job is required for simple rollouts.

The persistence layer's `--db-url` is **independent** of `--insecure`:
you can run with TLS and an in-memory store, or with `--insecure` and
Postgres, or any combination. TLS configures the gRPC server's
transport; `--db-url` configures the persistence layer.

## CI/CD

GitLab CI runs on every push and merge request. The pipeline uses a multi-stage
Dockerfile (`Dockerfile`) to build, test, and package the service.

### Pipeline stages

| Stage    | Job                             | When          | What                                                         |
| -------- | ------------------------------- | ------------- | ------------------------------------------------------------ |
| build    | `build-base`                    | Every push/MR | Build Rust project inside Docker                             |
| test     | `fmt-check`                     | Every push/MR | `cargo fmt -- --check`                                       |
| test     | `clippy-lint`                   | Every push/MR | `cargo clippy -- -D warnings`                                |
| test     | `run-tests`                     | Every push/MR | `cargo test --release`                                       |
| test     | `coverage-report`               | Every push/MR | cargo-llvm-cov coverage (Cobertura XML for MR diffs)         |
| test     | `coverage-rackmanagementservice`| Every push/MR | package-level RMS `cargo llvm-cov` summary and threshold     |
| test     | `coverage-nvfwupd`              | Every push/MR | package-level NVFWUPD `cargo llvm-cov` summary and threshold |
| test     | `run-benchmarks`                | Merge to main | `cargo bench` with Criterion HTML artifacts                  |
| build    | `build-release-image`           | Merge to main | Build minimal release Docker image                           |
| security | `tag-image-for-nspect-scanning` | Merge to main | Tag image for NVIDIA security scanning                       |
| promote  | `push-to-registry`              | Merge to main | Push release image to `nvcr.io/nvidian/dcim/rms-api`         |

Merges to main are gated on `fmt-check`, `clippy-lint`, `run-tests`, and
`coverage-report` all passing.

### Running the pipeline locally

```bash
# Build the Docker image (builder stage)
docker build --target builder -t rms-builder .

# Run tests inside Docker
docker run --rm rms-builder cargo test --workspace --release

# Run clippy inside Docker
docker run --rm rms-builder cargo clippy --workspace -- -D warnings

# Check formatting inside Docker
docker run --rm rms-builder cargo fmt -p rackmanagementservice -- --check
docker run --rm rms-builder cargo fmt -p nvfwupd -- --check

# Build the minimal release image
docker build --target release -t rms-release .

# Run the release image
docker run --rm -p 8801:8801 -e RMS_ALLOW_INSECURE=1 rms-release --insecure

# The release image also includes ipmitool and the nvfwupd CLI for diagnostics.
docker run --rm --entrypoint nvfwupd rms-release --version
```

## Adding a New Persistence Domain

The `src/persistence/` module is per-domain by design: each new domain (e.g.
`inventory`) gets its own trait file, two implementation files, and a
migration. Infrastructure (pool builder, error type, migrations runner, CI
postgres service, docker-compose) is shared and isn't touched.

To add a domain `inventory`:

1. **Domain types + trait** -- create `src/persistence/inventory.rs` with
  the entity types and a `Send + Sync` trait. Method signatures take and
   return only domain types.
2. **Migration** -- add `src/persistence/postgres/migrations/000N_inventory.sql`
  with the typed schema. Use `sqlx migrate add --source src/persistence/postgres/migrations <name>`
   if you want `sqlx-cli` to generate the timestamp.
3. **Memory impl** -- create `src/persistence/memory/inventory.rs` with a
  struct holding `RwLock`-protected collections. Mirror the Postgres
   semantics so the same behavioral tests pass against either.
4. **Postgres impl** -- create `src/persistence/postgres/inventory.rs`.
  Reuse `DatabaseError` from `super::error` and the shared `PgPool`. Use
   `sqlx::query_as` (no `!` macro -- CI builds don't need a live DB).
5. **Wire into `Backends`** -- add a `pub inventory: Arc<dyn InventoryStore>`
  field to the `Backends` struct in `src/persistence/mod.rs`.
6. **Tests** -- add scenarios to `tests/persistence.rs` as
  `async fn<S: InventoryStore>(store: &S)` and wrap each with the
   `memory_test!` and `postgres_test!` macros.

## License

Licensed under the NVIDIA Software and Model Evaluation License. See [`LICENSE`](LICENSE) for the full text.
