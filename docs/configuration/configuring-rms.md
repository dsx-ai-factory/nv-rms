# Configuring RMS

RMS reads **all** runtime configuration from a single TOML file, selected with
`--config <path>` (default `/etc/rms/config.toml`). `--config` is the only
command-line flag.

Every key is optional; omitted keys - and omitted sections - fall back to the
defaults documented below, so an empty file is a valid, all-defaults config.
Unknown keys, at the top level or within a section, are **rejected at startup**.
A documented template ships at
[`docker/config.example.toml`](https://github.com/NVIDIA/nv-rms/blob/main/docker/config.example.toml).

Beyond the top-level `port`, configuration is grouped into sections:
`[metrics]`, `[tls]`, `[switches]`, `[postgres]`, `[workflows]`, and `[logging]`.

## Environment overrides

Only two environment inputs are honored, both sourced outside the file so secrets
and safety gates don't live in the config:

| Variable | Effect |
| --- | --- |
| `DATABASE_URL` | When set, supersedes `[postgres] db_url` so the connection string (with its password) can come from a secret store rather than the file. |
| `RMS_ALLOW_INSECURE` | Must equal `1`, **in addition to** `[tls] insecure = true`, before RMS will bind plaintext gRPC. An independent safeguard so a stale config alone cannot downgrade the API to unauthenticated plaintext. |

## Top level

| Key | Default | Purpose |
| --- | --- | --- |
| `port` | `8801` | gRPC server port for the RMS API. |

## `[metrics]`

| Key | Default | Purpose |
| --- | --- | --- |
| `port` | `8802` | Dedicated Prometheus `/metrics` listener port; **must differ** from the top-level `port`. |
| `tls` | `false` | Serve `/metrics` over TLS using the gRPC server certificate (no client auth). Requires `[tls] cert` and `[tls] key`. |

## `[tls]`

The gRPC API requires mTLS unless it is explicitly placed in insecure mode. Set
`cert`, `key`, and `ca` together for mTLS.

| Key | Default | Purpose |
| --- | --- | --- |
| `cert` / `key` | - | Server certificate + private key. `ca` is required alongside them. Supplying one without the other is rejected. |
| `ca` | - | CA cert for client verification; required with `cert`/`key`. Supplying `ca` alone (without cert/key) is rejected - TLS-only, unauthenticated-client mode is not allowed. |
| `insecure` | `false` | Disable TLS on the gRPC listener (plaintext, unauthenticated); dev/testing only. Also requires the `RMS_ALLOW_INSECURE=1` environment gate before RMS will bind plaintext. Metrics plaintext is controlled separately via `[metrics] tls`. |

RMS uses TLS 1.3 via rustls; there is no option to enable older TLS versions.

## `[switches]`

Configures RMS's **outbound** connections to NVSwitch trays and the switch
certificate material it installs. All keys except `insecure_switch` and
`nmx_gateway_id` are required unless `insecure_switch = true`.

| Key | Default | Purpose |
| --- | --- | --- |
| `switch_cert_root` | - | Directory root of cert material installed on switches by the certificate RPCs, organized into per-domain subdirectories. |
| `client_tls_root` | - | Directory root of RMS client mTLS material for secure outbound connections to switch NVUE / NMX-C services, organized into per-domain subdirectories. |
| `default_switch_domain` | - | Default domain for switch TLS material lookup when a request omits `domain`. Required whenever `client_tls_root` is set. |
| `dns_domain` | - | Optional TLS server-name (SNI) authority RMS presents on outbound mTLS connections to switches (NVUE and NMX-C/gRPC). |
| `insecure_switch` | `false` | Disable outbound switch client mTLS. NVUE stays HTTPS but the server certificate is not verified; NMX-C uses plaintext HTTP; secure-only gNMI checks are skipped. Dev/testing only. |
| `nmx_gateway_id` | `rack-manager-grpc-client` | `gateway_id` sent on NMX-C gRPC requests from RMS. Must not be blank/whitespace-only. |

### Switch certificate directory layout

`switch_cert_root` and `client_tls_root` are directory roots whose cert material
(`ca.pem`, `client.pem`, `client.key`) is organized into **domains** via
subdirectories. For a single site-wide domain:

```console
/var/run/secrets/switch_server_certs      # switch_cert_root
└── site-wide                             # default_switch_domain
    ├── ca.pem
    ├── client.key
    └── client.pem
```

- **`switch_cert_root`** holds the material installed **on** the switch. The
  `client.key`/`client.pem` files get bound to the switch's server identity - the
  `client` name is an artifact of both roots sharing one reusable struct despite
  the differing role.
- **`client_tls_root`** holds RMS's **outbound client** identity. The current
  implementation uses only `default_switch_domain` for the client identity;
  per-request domain selection may be added later.
- **`dns_domain`** is the hostname presented during the TLS handshake to the
  switch (the DNS/SNI authority).

When `insecure_switch = true`, `ConfigureSwitchCertificate` installs no switch
certificate material; instead it creates jobs that unset switch-service mTLS mode
over SSH for the requested services. Use only in development/testing where secure
RMS↔switch communication is not required and mTLS has been disabled on the switch
services.

## `[postgres]`

| Key | Default | Purpose |
| --- | --- | --- |
| `db_url` | - | Postgres connection URL. If unset (and no `DATABASE_URL` env override), RMS uses the in-memory store and logs a warning that data is lost on restart. |
| `db_pool_max` | `20` | Postgres connection pool size. Must be greater than 0. |

With a URL set, RMS connects, runs its embedded migrations on startup, then
starts serving - no separate migration step is required.

### Resetting the local Postgres

`cargo test --test persistence` does not need any reset between runs - each test
provisions and drops its own ephemeral database. Resetting matters only when the
**binary** has been writing to a database and you want a clean slate:

```bash
# Full clean slate: drop the container's data volume
docker compose down -v

# Or drop and recreate the schema in place (re-runs startup migrations next launch)
docker compose exec postgres psql -U postgres -d rms_test \
    -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

## `[workflows]`

| Key | Default | Purpose |
| --- | --- | --- |
| `firmware_dir` | `firmware` | Directory where RMS stores and reads firmware artifacts. |
| `sftp_upload_timeout_seconds` | `3600` | Overall wall-clock timeout (seconds) for switch NVOS SFTP image uploads. |
| `sftp_step_timeout_seconds` | `30` | Stall timeout (seconds) for one SFTP step (setup/read/write/flush). Must be ≤ the upload timeout. |
| `max_tracked_jobs` | `10000` | Max async job records the tracker retains, to bound resource usage. |
| `terminal_job_ttl_seconds` | `86400` | Retention period (seconds) for completed and failed job records before eviction. |
| `expected_inventory_profiles` | empty | Map of opaque profile identifiers to NVFWUPD AP names. |

Expected-inventory profiles are deployment controlled. RMS does not interpret
the profile name or derive hardware properties from it:

```toml
[workflows.expected_inventory_profiles]
"gb200-compute-variant-a" = [
    "FW_BMC_0",
    "HGX_FW_GPU_0",
]
"gb200-compute-variant-b" = [
    "FW_BMC_0",
    "HGX_FW_GPU_0",
    "HGX_FW_GPU_1",
]
```

A node selects a profile with
`NodeDescriptor.attributes["inventory_profile"]`. Matching is exact and
case-sensitive after surrounding whitespace is trimmed. Omitting the attribute
keeps the legacy update behavior. An empty or unknown supplied value rejects
the node before RMS creates its firmware job. RMS validates the configured map
at startup; names and AP entries must be non-empty, and duplicate AP names are
removed case-insensitively.

## `[logging]`

| Key | Default | Purpose |
| --- | --- | --- |
| `log_level` | - | Log level / filter directive (e.g. `info` or `info,carbide=debug`). When unset, defaults to `info` plus dependency caps. |

## Persistence and TLS are independent

`[postgres] db_url` is independent of `[tls] insecure`: you can run with TLS and
the in-memory store, or with `insecure = true` and Postgres, or any combination.
`[tls]` configures the gRPC server's transport; `[postgres]` configures the
persistence layer.

## Setting these values via Helm

When deploying on Kubernetes, you don't hand-write `config.toml` - the Helm chart
renders it from `apiServer.*` values. See [Configuration via Helm](via-helm.md)
for the value → key mapping.
