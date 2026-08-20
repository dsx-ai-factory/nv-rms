# Testing RMS

RMS has unit tests, four integration test binaries, and Criterion scale
benchmarks. Most tests run against an in-process backend with no external
dependencies; only the Postgres persistence suite needs a real database.

## Prerequisites

### Networking

The integration suites (`grpc_e2e`, `grpc_mtls`, `mockup_server_test`) start an
in-process **Redfish simulator** that serves BMC endpoints over real HTTPS on
loopback, binding one TLS port per simulated BMC. No outbound network access or
real hardware is required, but the tests do bind local TCP ports.

### Postgres (for persistence tests)

The `persistence` suite's Postgres backend requires a reachable Postgres 16 and a
`DATABASE_URL`. The bundled
[`docker-compose.yml`](https://github.com/NVIDIA/nv-rms/blob/main/docker-compose.yml)
pulls `postgres:16-alpine`, listens on `localhost:5432`, and uses credentials
`postgres / postgres / rms_test`:

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/rms_test
```

`cargo test --test persistence` provisions a fresh, isolated ephemeral database
per test (via `#[sqlx::test]`) and drops it afterward, so no manual reset is
needed between runs. To reset the shared `rms_test` database used by the
**binary**, see [Configuring RMS: Postgres](../configuration/configuring-rms.md#postgres).

## Run everything

Matching CI, use release mode with `DATABASE_URL` set so the persistence suite
runs against a real database:

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/rms_test
cargo test --release
```

This runs the unit tests plus all four integration binaries.

## Run without a database

Skip the Postgres-backed persistence tests by listing the suites that don't need
one (the memory backend of the persistence suite always runs):

```bash
cargo test --release --lib
cargo test --release --test grpc_e2e
cargo test --release --test grpc_mtls
cargo test --release --test mockup_server_test
cargo test --release --test persistence memory::
```

## Unit tests

```bash
cargo test --lib
```

## Integration tests

| Binary | Command | What it covers |
| --- | --- | --- |
| `grpc_e2e` | `cargo test --test grpc_e2e` | Full gRPC E2E: starts RMS plus the Redfish simulator in-process and exercises power, inventory, firmware, and mixed-rack scenarios for compute and power-shelf nodes. |
| `grpc_mtls` | `cargo test --test grpc_mtls` | Spawns the gRPC server with a client CA and verifies the rustls TLS 1.3 handshake, client-cert verification, ALPN to h2, and a real RPC round-trip. |
| `mockup_server_test` | `cargo test --test mockup_server_test` | Redfish-simulator and service-facing tests for TLS startup, JSON serving, multi-port binding, action validation, firmware task simulation, and node APIs. |
| `persistence` | `cargo test --test persistence` | Cross-backend behavioral tests for the firmware-object store. The memory backend always runs; the Postgres backend requires `DATABASE_URL` (and panics otherwise). Filter with `memory::` to run only the memory backend. |

### The Redfish simulator

Redfish tests and benchmarks use the `redfish_test_support` workspace crate. It
serves static Redfish JSON from mockup device trees over TLS (self-signed certs
generated at startup via `rcgen` + `tokio-rustls`), simulates firmware update
tasks with a configurable delay and failure rate, and supports multi-port binding
so each simulated BMC gets its own TLS endpoint. It starts in-process in
milliseconds with zero external dependencies. Fixture data lives under
`crates/redfish_test_support/fixtures/mockup/`.

## Benchmarks

Criterion scale benchmarks measure RMS performance from 100 to 10,000 nodes. Each
benchmark node gets its own mockup BMC on a unique port, forcing independent TLS
sessions - matching production, where each node talks to a different BMC.

| Benchmark | What it measures |
| --- | --- |
| `add_nodes` | gRPC + rack/node registration throughput (no Redfish calls). |
| `concurrent_power_state` | N parallel `GetPowerState` RPCs, each hitting a unique BMC over HTTPS. |
| `concurrent_firmware_inventory` | N parallel firmware-inventory queries (~20 sequential GETs per node). |
| `parallel_firmware_updates` | Async firmware job lifecycle: concurrent uploads + task polling + status tracking. |

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark
cargo bench --bench scale -- "concurrent_power_state"

# List all benchmarks
cargo bench --bench scale -- --list
```

## Running tests in Docker

The builder image encapsulates the toolchain, so results match CI without
installing Rust on the host:

```bash
# Build the builder image
docker build --target builder -t rms-builder .

# Unit + integration tests (skipping the Postgres persistence tests)
docker run --rm rms-builder cargo test --workspace --release -- --skip postgres::

# Include the Postgres-backed persistence tests
docker compose up -d postgres
docker run --rm --network host \
    -e DATABASE_URL=postgres://postgres:postgres@localhost:5432/rms_test \
    rms-builder cargo test --workspace --release
```
