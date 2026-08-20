# Overview

Clients invoke RMS API endpoints via gRPC; RMS then talks to compute BMCs and
power-shelf controllers over Redfish/HTTPS, and to NVSwitch trays over NVUE REST,
SSH, SFTP, and NMX-C gRPC. Long-running firmware and switch system-image work is
modeled as asynchronous jobs: the API starts the operation, returns a job ID, and
clients poll job status until the work completes or fails.

RMS is built in Rust on **tokio** (async runtime), **tonic** (gRPC), **reqwest**
(Redfish/NVUE HTTP), **russh** (SSH/SFTP), **sqlx** (optional Postgres
persistence), and an embedded **nvfwupd** firmware-update workflow library.

## Scope

RMS is a **rack operations control plane**, not a full cluster orchestrator.
Its scope is rack hardware operations: power, firmware, switch
operations, and the workflow state needed to coordinate them.

| In scope | Out of scope |
| --- | --- |
| Out-of-band BMC/switch management via Redfish, NVUE, SSH/SFTP, NMX-C | Host OS provisioning or workload scheduling |
| Persisting firmware objects and related metadata when Postgres is configured | Authoritative long-term rack/node inventory as a CMDB |
| Async job tracking for long-running firmware and image workflows | Replacing site-specific source-of-truth systems that own node lists |

Clients of RMS own the authoritative list of nodes to register with RMS for the
duration of an operations session. Additionally, they provide the firmware manifest
at request-time; RMS downloads/fetches the specified files and applies the
appropriate files to the requested nodes based on internal hardware profiles.

## Capabilities

| Area | Capabilities |
| --- | --- |
| Power | Set node power state, query node power state, and run rack-level power-on / power-off / power-cycle sequences. |
| Inventory | Register, update, delete, and list nodes and racks; query device info and firmware inventory per node, node type, or rack. |
| Firmware | Start asynchronous firmware updates (single, batch, and by node type), poll job status, manage persistent firmware objects, and record apply history. |
| Switch operations | List and push switch firmware, manage switch system images, rotate switch passwords, install switch certificates, and configure the scale-up fabric manager (NMX-C) and gNMI telemetry. |
| Observability | Exposes Prometheus `/metrics` on a dedicated port and provides structured tracing/logging. |

For the exact RPCs behind each capability, see [Operations](operations/overview.md).

## Supported Platforms

RMS runs as a Linux service (native binary or container) on **x86-64** and
**arm64**. The release container image is `debian:bookworm-slim`-based; the
binary is built with the Rust toolchain pinned by
[`rust-toolchain.toml`](https://github.com/NVIDIA/nv-rms/blob/main/rust-toolchain.toml).

On the managed side, RMS supports NVIDIA GB200 and GB300 rack generations -
compute trays, NVSwitch trays, and power shelves - over Redfish and NVUE. See
the [Hardware Compatibility List](reference/hcl.md) for the validated node types,
vendors, and management interfaces.

## Operating Principles

### Stateless with respect to inventory

RMS is stateless with respect to rack topology and mutable device attributes.
Because it is stateless, RMS creates **ephemeral node objects** to carry out each
request; no device-level state persists beyond the operation's lifetime. In the
recommended model, an external control-plane or inventory system remains the
source of truth for rack topology, addressing, and credentials, and supplies the
current target list on each request.

### Minimal reliance on persistence

RMS persistence is scoped to **workflow data** - firmware objects, cached artifact
metadata, job tracking, and firmware apply history. It is not intended to make RMS
the long-term system of record for topology or mutable device attributes such as
management IPs.

Persistence is optional and swappable. Without a Postgres connection RMS uses a
built-in in-memory store and logs a warning that data will not survive a restart;
with `[postgres] db_url` (or the `DATABASE_URL` override) set, RMS connects to
Postgres, runs its embedded migrations on startup, and persists workflow state
across restarts. See [Configuring RMS](configuration/configuring-rms.md#postgres) and
[Architecture: Internal View](architecture/internal_view.md#persistence-layer-persistence).

### Per-node serialization at scale

RMS is designed to manage **10k+ nodes** concurrently. Every I/O operation
(Redfish HTTP to a BMC, SSH/SFTP to a switch, firmware upload, task polling) is
async, so thousands of concurrent jobs run as lightweight tasks on a small pool
of OS threads rather than as blocked threads. Operations on **different** nodes
run fully in parallel; operations on the **same** node serialize behind a
per-node lock, matching the reality that a BMC processes one Redfish request at a
time. See [Architecture: Internal View](architecture/internal_view.md#concurrency-and-scale) for the concurrency model.

### Secure by default

The gRPC API requires mTLS (TLS 1.3 via rustls) unless it is explicitly placed in
insecure mode, which additionally requires an independent environment gate
(`RMS_ALLOW_INSECURE=1`) so a stale config alone cannot downgrade the API to
unauthenticated plaintext. Outbound connections to switches use mTLS by default as
well. Because RMS can power-cycle hardware, flash firmware, and change switch
state, production deployments should keep mTLS enabled and restrict access to
trusted services and operators on a controlled management network. See
[Configuring RMS: TLS](configuration/configuring-rms.md#tls) and
[Deployment](deployment/prerequisites.md).

## Where RMS fits

RMS sits below the control-plane and scheduling layers. Higher-level systems
(BMaaS, control planes, ISV orchestration) call the RMS gRPC API to perform
hardware actions; RMS abstracts the underlying Redfish, NVUE, SSH/SFTP, and NMX-C
protocols behind that single boundary. The caller remains responsible for
supplying accurate targets and credentials; RMS is responsible for orchestrating
and tracking the requested operation.
