# Configuring RMS

RMS reads runtime settings from a TOML file, selected with `--config <path>`
(default `/etc/rms/config.toml`), plus the environment inputs described below.
`--config` is the only runtime-configuration flag.

Omitted keys and sections use the defaults below. An empty file parses, but
starting RMS with the default secure API requires the `[tls]` credentials.
Unknown keys outside `[tracing]` are reported and ignored for backward
compatibility. Unknown `[tracing]` keys are rejected because a typo could
silently disable or weaken trace export.
A documented template ships at
[`docker/config.example.toml`](https://github.com/dsx-ai-factory/nv-rms/blob/main/docker/config.example.toml).

Beyond the top-level `port`, configuration is grouped into sections:
`[metrics]`, `[tls]`, `[switches]`, `[bmc]`, `[postgres]`, `[workflows]`, `[logging]`,
and `[tracing]`.

## Environment overrides

Only two environment inputs override RMS configuration or gate RMS behavior;
both are sourced outside the file so secrets and safety gates don't live in the
config. Standard TLS-library trust discovery for tracing HTTPS is documented
separately under `[tracing]`.

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
| `cert_reload_interval_seconds` | `300` | How often RMS re-reads `cert` and `key`. A matching renewed pair is presented on new handshakes; an unreadable or mismatched pair leaves the current identity in place. The client CA is not re-read. Must be greater than zero. |

The RMS gRPC API uses TLS 1.3 via rustls; there is no option to enable older TLS versions for that listener. Connections already open keep the certificate from their handshake.

## `[switches]`

Configures RMS's **outbound** connections to NVSwitch trays and the switch
certificate material it installs. RMS can start without these certificate roots;
operations that require missing certificate material fail when invoked.

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
- **`client_tls_root`** holds RMS's **outbound client** identity, selected by the
  request's `domain`, or `default_switch_domain` when omitted.
- **`dns_domain`** is the hostname presented during the TLS handshake to the
  switch (the DNS/SNI authority).

When `insecure_switch = true`, `ConfigureSwitchCertificate` installs no switch
certificate material; instead it creates jobs that unset switch-service mTLS mode
over SSH for the requested services. Insecure scale-up configure unsets NMX-C
manager encryption the same way: it skips restore when encryption is already
disabled. When the cluster is disabled it still issues the restore commands and
treats NVOS's rejection as benign. It fails if the enabled `nmx-controller`
app stays unready for the retry budget. Use only in development/testing where
secure RMS↔switch communication is not required and mTLS has been disabled on
the switch services.

## `[bmc]`

Configures RMS's **outbound** BMC/Redfish HTTPS certificate policy. gRPC
callers cannot change this per request.

| Key | Default | Purpose |
| --- | --- | --- |
| `insecure_bmc` | `true` | Skip BMC server certificate verification. Historical default because production BMC certificates are typically self-signed. Set `false` to verify BMC certificates against the process trust store (and hostname or IP). |

Set `insecure_bmc = false` only after BMC certificates are issued by a CA the
RMS process trusts. Until then, BMC Basic credentials still travel over HTTPS
to an unverified peer, so the management network must remain segmented.

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
| `fabric_config_wait_log_interval` | `30` | Delay in seconds between fabric configuration wait logs. Must be a positive integer and fit the monotonic clock. Does not change device polling or workflow timeouts. |
| `hardware_profiles_dir` | unset | Optional hardware-profile override catalog loaded once at startup, replacing embedded profiles. Empty/partial directories are valid. Changes require a restart; unset uses embedded profiles. |
| `fabric_layout` | unset | Deployment-supplied grouping, link calculations, and controller targets for V2 layout requests. Validated at startup; ordinary V2 requests do not require a catalog. See [Fabric layout catalog](../architecture/fabric-layout-catalog.md). |
| `sftp_upload_timeout_seconds` | `3600` | Overall wall-clock timeout (seconds) for switch NVOS SFTP image uploads. |
| `sftp_step_timeout_seconds` | `30` | Stall timeout (seconds) for one SFTP step (setup/read/write/flush). Must be ≤ the upload timeout. |
| `firmware_task_timeout_seconds` | `10800` | Upper bound (seconds) on polling one firmware task for completion. |
| `firmware_task_poll_interval_seconds` | `30` | Delay (seconds) between polls of an in-flight firmware task. Must be ≤ the firmware task timeout. |
| `firmware_task_transient_error_timeout_seconds` | `900` | How long (seconds) to tolerate consecutive firmware task-status read failures before failing the update. |
| `compute_power_on_timeout_seconds` | `60` | How long (seconds) a compute firmware update waits for the tray to report power `On` after the power-on that opens the flow. |
| `compute_power_on_poll_interval_seconds` | `5` | Delay (seconds) between power-state reads during that wait. Must be ≤ the compute power-on timeout. |
| `onie_recovery_timeout_seconds` | `900` | Non-zero wait after ONIE starts installing until NVOS SSH is ready. |
| `recovered_credential_save_timeout_seconds` | `300` | Non-zero wait for NVUE to save credentials after ONIE recovery. |
| `apply_history_timeout_seconds` | `86400` | Non-zero observation deadline for stored firmware/NVOS apply history. Records completed nodes only on expiry; does not cancel hardware jobs. |
| `max_tracked_jobs` | `10000` | Max async job records the tracker retains, to bound resource usage. |
| `terminal_job_ttl_seconds` | `86400` | Retention period (seconds) for completed and failed job records before eviction. Age is monotonic elapsed time, not wall-clock. |
| `expected_inventory_profiles` | empty | Map of opaque profile identifiers to expected NVFWUPD AP names and physical Flint device counts. |
| `artifact_require_sha256` | `false` | When `true`, HTTP(S) firmware artifacts must include a SHA-256 digest (`AddFirmwareObject`, `ApplyFirmwareObject`, `ApplyStoredFirmwareObject`, and inline `ApplySwitchSystemImage`). Digests that are present are always verified. |
| `artifact_https_hosts` | empty | Hostname or `*.suffix` allow-list for HTTP(S) firmware artifact downloads (`AddFirmwareObject`, `ApplyFirmwareObject`, `ApplyStoredFirmwareObject`, inline `ApplySwitchSystemImage`, and `ApplyStoredSwitchSystemImage`). `*.suffix` matches the suffix and its subdomains. Empty allows any host. See [Artifact download policy](#artifact-download-policy). |
| `artifact_allow_private_targets` | `true` | When `false`, HTTP(S) artifact URLs may not use IANA special-purpose addresses (loopback, link-local, unspecified, multicast, RFC1918/ULA, CGNAT, NAT64, reserved, and documentation ranges). RMS checks literal IPs and DNS results on the initial URL and every redirect, then reuses those validated addresses for the TCP connection. |
| `recovery_profiles` | empty | Per-profile recovery rosters. RMS keeps each one opaque; the node type that binds to it parses it (see below). |

To load hardware profiles from an external directory, set:

```toml
[workflows]
hardware_profiles_dir = "/etc/rms/hardware-profiles/v1"
```

RMS loads and validates all directly contained `.yaml` and `.yml` files once
before accepting requests. Invalid profiles and an unreadable or missing explicitly
configured directory prevent startup. Empty and partial directories are valid:
platforms without an enabled override use their default implementation. Dell GB200
uses its embedded profile for those defaults, including its two activation stages;
other platforms retain their legacy fallback behavior. No external platform
profile is mandatory, including NVIDIA GB200.

The compiled catalog stays fixed for the life of the process: disk edits affect
neither running jobs nor subsequent requests.

Restart RMS to apply profile edits or change the directory path. Neither the
YAML files nor the TOML file are watched. Without this setting, changing
embedded profiles requires a rebuild and restart.
See [hardware-profile startup loading](../../hardware-profiles/README.md#directory-configuration-and-startup-loading)
for deployment guidance.

The three `firmware_task_*` keys bound one shared poll loop, the one RMS runs
after starting a Redfish or NVUE firmware task on a compute tray or a switch
component. Because the same loop serves every component, its timeout has to
cover the slowest of them rather than any single component's expected duration.

Every poll emits one log line, so the interval sets how chatty a running
firmware update is: a 30-minute flash produces roughly 60 lines at the default
30 seconds. Lower it when you want a finer-grained view of a job in progress,
and raise it on deployments where firmware update logs dominate the log budget.
RMS rejects an interval longer than the timeout at startup.

The transient-error window carries a second meaning on Supermicro GB300 BMC
targets. Those updates activate immediately and remove their Redfish task while
RMS is still polling it, so instead of failing, RMS falls back to comparing the
installed version against the firmware manifest. The window is how long RMS
waits before treating the task as gone. Set it far below the flash duration and
verification runs against pre-update firmware, which reports a version mismatch
rather than retrying.

The two `compute_power_on_*` keys bound the power-on that opens a compute tray
firmware update: the wait for the tray to report `PowerState == "On"`. The
default timeout, 60 seconds, is the VR NVL72 bound for this power-on, and GB200
and GB300 use it as their default too. RMS rejects an interval longer than the
timeout at startup. A hardware profile with a `firmware.power_on` block
overrides both keys for jobs that use the profile while it is enabled. The
power-on step itself arrives in a following change. Until it lands, RMS
validates and stores these bounds and nothing reads them.

Expected-inventory profiles are deployment controlled. RMS does not interpret
the profile name or derive hardware properties from it. The name is the
deployment's choice; the content describes the hardware. A profile records which
firmware inventory members a SKU exposes and how many Flint devices it carries,
so it is the deployment's copy of that SKU's topology, not a tuning knob.
Profiles work the same way on every NVFWUPD-backed compute platform: GB200,
GB300, and VR NVL72. The platform decides which AP names a deployment lists, not
how a profile is written or selected. Deployments can use the SKU ID supplied by
NICo as the profile name. The lists below are shortened examples; a real profile
names every component the deployment expects to see:

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
"vrnvl72-compute" = [
    "FW_BMC_0",
    "HGX_FW_BMC_0",
    "HGX_FW_GPU_0",
]
```

The legacy AP-name list remains supported. A structured profile can additionally
declare the expected number of physical host adapters for GB200 in-band updates:

```toml
[workflows.expected_inventory_profiles."sku-gb200"]
ap_names = ["FW_BMC_0", "HGX_FW_GPU_0"]

[workflows.expected_inventory_profiles."sku-gb200".flint_devices]
cx7 = 4
cx8 = 0
bf3_nic = 2
```

Omitting a Flint family disables its count check; setting it to zero requires
that MST enumerate no physical devices of that family.

A node selects a profile with
`NodeDescriptor.attributes["inventory_profile"]`. Matching is exact and
case-sensitive after surrounding whitespace is trimmed. Omitting the attribute
keeps the legacy update behavior. An empty or unknown supplied value rejects
the node before RMS creates its firmware job. RMS validates the configured map at
startup. Profile names must be non-empty; each profile must contain AP names,
Flint counts, or both. AP entries must be non-empty, and duplicate AP names are
removed case-insensitively.

### Artifact download policy

`AddFirmwareObject`, `ApplyFirmwareObject`, `ApplyStoredFirmwareObject`,
inline `ApplySwitchSystemImage`, and `ApplyStoredSwitchSystemImage`
fetch HTTP(S) catalog URLs through one shared downloader. SHA-256 verification
of a present catalog digest is always on. `artifact_require_sha256` defaults to
`false` so catalogs without a digest keep working. Set it to `true` to refuse
unpinned HTTP(S) artifacts (CWE-494), including previously cached objects on
`ApplyStoredFirmwareObject`. The keys below default to the historical open
behavior so existing labs and empty allow-lists keep working. Set them when you
want RMS to refuse unexpected artifact origins (CWE-918).

`artifact_https_hosts` entries are exact hostnames or `*.suffix` patterns.
Entries are parsed with the same host rules as URL authorities: they are
lowercased, Unicode is stored as punycode, a trailing FQDN dot is stripped,
and interior whitespace is rejected at startup. IPv6 literals must include
brackets (`[::1]`). `*.suffix` matches the suffix itself and its subdomains:
`*.jfrog.io` admits `jfrog.io` and `acme.jfrog.io`. Empty allows any host.

On stored apply, a cached file with a catalog digest is reused after a local
hash check. A cached file without a digest sends a HEAD to the catalog URL,
and that HEAD uses the same host and private-target policy. Tightening
`artifact_https_hosts` or `artifact_allow_private_targets` does not delete an
already-cached artifact. A host off the allow-list, or a disallowed IP
literal, is reused with no HEAD or GET. A hostname that resolves to a
disallowed address fails during that HEAD; stored apply still reuses the
local bytes and does not GET. A missing cache cannot be re-fetched from that
host.

| Goal | Set |
| --- | --- |
| Require a catalog SHA-256 digest on HTTP(S) artifacts | `artifact_require_sha256 = true` |
| Allow only approved artifact hosts (including every redirect hop) | `artifact_https_hosts` to hostnames or `*.suffix` patterns (`*.suffix` matches the suffix and its subdomains) |
| Reject loopback, CGNAT, NAT64, reserved, and RFC1918/ULA targets | `artifact_allow_private_targets = false` |

Local `file` locations are not host allow-listed and are not required to include
a digest. Direct firmware RPCs that stage files under `firmware_dir`
(`UpdateFirmware`, `BatchUpdateFirmware`, `PushSwitchFirmware`, path-based
`ApplySwitchSystemImage`) never use this downloader.

```toml
[workflows]
artifact_require_sha256 = true
artifact_https_hosts = ["artifactory.example.com", "*.jfrog.io"]
artifact_allow_private_targets = false
```

Helm equivalents (`apiServer.*`): `artifactRequireSha256`, `artifactHttpsHosts`,
and `artifactAllowPrivateTargets`. See
[Configuration via Helm](via-helm.md).

Recovery profiles describe topology the same way and share the profile key:
a node selects its recovery profile with the same
`NodeDescriptor.attributes["inventory_profile"]` it uses for expected
inventory. RMS itself keeps a recovery profile opaque. At startup it only
checks that each profile is a non-empty name mapping to a table. The node
type that binds to the profile at `add_nodes` parses the fields it
understands and rejects everything else, so a mistyped or misplaced field
fails node registration, before any update job can run. A node without a
profile, or whose profile has no recovery table, gets no recovery detection.
A node type without a recovery flow rejects any recovery profile bound to it.

Today the only node type with a recovery flow is the GB200 NVIDIA compute
tray. Its profile groups the tray's recoverable components by how they are
recovered, using their Redfish `FirmwareInventory` member names:

```toml
[workflows.recovery_profiles."gb200-compute-variant-a"]
# Members that must be back after the AC cycle before the flow continues.
presence = ["HGX_FW_BMC_0", "HGX_FW_ERoT_BMC_0"]
# Recovered with the regular HMC bundle: one push, then an AC cycle, one
# member per round. FPGA1 is reachable only through a working FPGA0.
push_then_cycle = ["HGX_FW_FPGA_0", "HGX_FW_FPGA_1"]
# Recovered with the HMC recovery bundle: one push, no cycle.
push_only = [
    "HGX_FW_ERoT_CPU_0",
    "HGX_FW_ERoT_CPU_1",
    "HGX_FW_GPU_0",
    "HGX_FW_GPU_1",
    "HGX_FW_GPU_2",
    "HGX_FW_GPU_3",
]
```

The keys name what the flow does with each list; the comments tie them to
the bundles. The GB200 node applies these rules: every list is non-empty;
names are trimmed and de-duplicated; `push_then_cycle` and `push_only` share
no name; `presence` shares no name with either, because a component the flow
waits for as its path back cannot also be recovered through that path;
unknown fields are rejected. Detection watches the union of `push_then_cycle`
and `push_only`. Names must match the tray's `FirmwareInventory` member names
exactly, including case; a name that never matches leaves detection silently
off for that component.

A recovery profile is deliberately narrower than the expected-inventory
profile for the same SKU. Components that must be present but that no
recovery bundle can restore belong only in the expected-inventory profile;
the tray BMC (`FW_BMC_0`) is one, as the Redfish endpoint the whole flow talks
through. RMS does not cross-check the two profiles, so keeping such
components out of the recovery lists is the operator's responsibility.

## `[logging]`

RMS writes logfmt to stdout. Set the filter here; `RUST_LOG` is ignored.

| Key | Default | Purpose |
| --- | --- | --- |
| `log_level` | - | Log level / filter directive (e.g. `info` or `info,carbide=debug`). When unset, defaults to `info` plus dependency caps. |
| `enable_timestamps` | `false` | Add native RFC3339 timestamps to log events. Enable when the log collector does not supply timestamps. |

## `[tracing]`

A **trace** groups related work under one trace ID. A **span** records one
operation, with its own span ID, parent, duration, attributes, and outcome.
Use traces to see where RMS spent time, then use the same IDs to find its logs.

Start with [Collector setup](#collector-configuration), then
[verify a read-only request](#follow-a-read-only-request). Use the
[span reference](#an-example-span-and-its-fields) to interpret results and
[developer examples](#developing-traced-operations) to add instrumentation.

### Collector configuration

The flow is **RMS → OTLP Collector receiver → traces pipeline → trace backend**.
RMS pushes completed spans; it does not expose a `/traces` endpoint for scraping.
The Helm chart configures RMS, but does not deploy a Collector or trace backend.

Both `grpc` and `http_protobuf` require **HTTPS with certificate and hostname
verification**, except to a Collector on a loopback IP address, such as a
sidecar in RMS's Pod: `http://127.0.0.1:4317` or `http://[::1]:4318/v1/traces`.
Loopback traffic never leaves RMS's network namespace, which in Kubernetes is
the Pod; with host networking or on a VM, every local process shares it. Use the
IP address, not `localhost`: a name can resolve off the host, so RMS rejects
plaintext to any hostname. For any other plaintext receiver, provision TLS on
the receiver or a compatible TLS-terminating proxy first; changing the URL
scheme alone is insufficient.

Run a sidecar Collector so it outlives RMS. RMS keeps exporting while it drains
work at shutdown, and counts spans it cannot deliver as dropped. On Kubernetes
1.29 or later, a native sidecar (an init container with `restartPolicy: Always`)
stops only after RMS exits, and OpenTelemetry Operator 0.132.0 and later inject
sidecars that way. The Collector's own final flush gets whatever remains of the
Pod's termination grace period.

#### Prepare the Collector

Obtain the receiver's reachable hostname, port, protocol, and issuing CA from the
Collector owner. Supply the Collector with:

- A server certificate chain and private key, mounted at its configured paths.
  The certificate's subject alternative name must match the host in RMS's URL.
- An enabled OTLP receiver and `service.pipelines.traces` pipeline.
- A backend exporter with the destination, trust, credentials, and tenant/routing
  settings required by that backend. These belong on the Collector, not in RMS.

This minimal **validation-only** `collector.yaml` accepts both transports and
prints spans to Collector logs. It does not store traces in a backend or make
them available in Grafana. It was validated with `otelcol-contrib` 0.161.0; other
distributions must include the `otlp` receiver, `batch` processor, and `debug`
exporter.

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
        tls:
          cert_file: /etc/otelcol/tls/server-chain.pem
          key_file: /etc/otelcol/tls/server-key.pem
      http:
        endpoint: 0.0.0.0:4318
        tls:
          cert_file: /etc/otelcol/tls/server-chain.pem
          key_file: /etc/otelcol/tls/server-key.pem
processors:
  batch: {}
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
```

Mount the certificate files read-only. Validate with your distribution's binary
(for example, `otelcol-contrib validate --config=collector.yaml`) before starting it.
For backend storage, configure the site's exporter and reference it in
`service.pipelines.traces.exporters`; do not replace an existing production
pipeline with this debug example. Collector component names and options depend
on the installed distribution/version; follow its
[configuration reference](https://opentelemetry.io/docs/collector/configuration/).

Enable only the receiver ports you need. For a sidecar, bind to `127.0.0.1`
when RMS shares its network namespace, and use that same address in RMS's
`endpoint`: a receiver bound to `127.0.0.1` does not accept `[::1]`. For a
separate Pod, expose the selected port through a Kubernetes Service and allow
RMS egress/Collector ingress.
`0.0.0.0` above is a bind address, not an RMS destination. Keep receiver access
restricted: TLS verifies the Collector, but **RMS currently supplies no client
certificate or authentication headers**. A receiver requiring mTLS, Basic,
Bearer, or API-key authentication is not directly compatible. Backend
credentials do not authenticate RMS to the receiver. See
[Collector security guidance](https://opentelemetry.io/docs/security/config-best-practices/#use-encryption-and-authentication).

#### Point RMS at the receiver

Add this to RMS's `config.toml`, using the actual certificate-matching hostname:

```toml
[tracing]
endpoint = "https://collector.example:4317"
protocol = "grpc"
sampling_ratio = 1.0
```

For HTTP/protobuf, set `protocol = "http_protobuf"` and use the complete URL,
such as `https://collector.example:4318/v1/traces`. This is protobuf over
**HTTPS**; only a loopback Collector may use plaintext, such as
`http://127.0.0.1:4318/v1/traces`. Omit `endpoint` to disable remote export while
preserving local trace/span IDs. Other tracing settings can keep their defaults.

For Helm, merge this fragment into the existing site values:

```yaml
apiServer:
  tracing:
    endpoint: https://collector.example:4317
    protocol: grpc
    samplingRatio: 1.0
```

The chart renders these values into `/etc/rms/config.toml`; its config checksum
triggers a Pod rollout when they change. See [Configuration via Helm](via-helm.md)
for the remaining mappings. Outside Helm, restart RMS after changing its config.

#### Trust a private Collector CA

Both exporters use native trusted roots **inside the RMS process/container**.
A publicly trusted chain needs no extra CA if its root is already installed.
For a private CA, provision its public CA certificate on RMS; the Collector's
private key stays on the Collector. RMS's `[tls]` and Helm `apiServer.tls.*`
configure the inbound RMS API and do not configure Collector trust.

The current chart has no Collector CA mount or generic extra-volume/environment
hooks. One supported deployment approach is a derived image. For RMS's current
Debian-based release image, save this as `Dockerfile.collector-ca` alongside the
PEM-encoded CA certificate `collector-root-ca.crt` (one certificate per `.crt`):

```dockerfile
ARG RMS_IMAGE
FROM ${RMS_IMAGE}
COPY collector-root-ca.crt /usr/local/share/ca-certificates/collector-root-ca.crt
RUN update-ca-certificates
```

Build with your approved RMS base image and a distinct output tag:

```bash
docker build -f Dockerfile.collector-ca \
  --build-arg RMS_IMAGE="$RMS_BASE_IMAGE" -t "$RMS_TRUSTED_IMAGE" .
```

Publish through your normal image workflow, then set `apiServer.image.repository`
and `apiServer.image.tag` to that derived image. This adds the CA to the system
bundle while retaining existing roots. Keep the base image and CA current.

Alternatively, a deployment overlay can mount a PEM trust bundle read-only and
set `SSL_CERT_FILE=/mounted/ca-bundle.pem` on the RMS container. The native loader
also honors `SSL_CERT_DIR`. **Setting either replaces native trust discovery**:
only roots in those supplied locations are loaded. Include every CA needed by
affected connections; these are process-wide settings, not per-Collector options.
Leave both unset when using the derived image's system trust store. Restart RMS
after changing CA material or trust variables; it does not reload exporter trust.

There is no verification bypass. With export configured, RMS rejects
`OTEL_EXPORTER_OTLP[_TRACES]_{HEADERS,COMPRESSION}` variables rather than importing
ambient credentials or unsupported exporter behavior. It ignores
`OTEL_EXPORTER_OTLP[_TRACES]_ENDPOINT` and proxy variables such as `HTTP_PROXY`,
`HTTPS_PROXY`, and `ALL_PROXY`: spans go only to `endpoint`, over a direct
connection. Once configured, use the
[read-only check](#follow-a-read-only-request) below to verify delivery.

### How a trace flows through RMS

1. The request middleware starts a server span, adopts valid caller context,
   and records the recognized RPC name.
1. A handler can create job spans and selected phase spans beneath it. Jobs
   retain their parent context after the request returns.
1. Spans end when their response, job, or phase finishes. With export enabled,
   completed, sampled spans enter a bounded queue and are sent to the Collector
   in OTLP protobuf batches.

RMS creates real trace/span IDs even without an export endpoint; omitting
`endpoint` disables remote export. Stdout log level and trace sampling are
independent.

### Which operations get spans

- **Request (`SERVER`):** covers the handler and response body/trailers. Its
  name is the recognized RPC, such as `rack_manager.RackManager/GetVersion`;
  unknown or noncanonical routes use `_OTHER`.
- **Accepted job (`INTERNAL`):** includes queue time and ends at its first
  completed/failed transition. Keeping a job record for status queries does not
  keep its span open. Child jobs inherit their parent job's context.
- **Phase (`INTERNAL`):** covers a named operation, including its waiting and
  retries. It appears only when the workflow reaches that phase.

The exported job names are:

```text
firmware_update
configure_scale_up_fabric_manager_v2
switch_certificate
switch_mtls_disable
switch_sdn_factory_default_reset
switch_factory_default_reset
switch_system_password_update
switch_system_image_update
cold_reboot_sequence
```

The exported phase names are:

```text
wait_for_uninstall_completion
wait_for_install_completion
wait_for_target_steady_state
switch_system_image_sftp_upload
```

A system-image workflow can produce a request, parent/child jobs, and phases;
its jobs can outlive the request. `GetVersion` produces one RMS request span.
There is no exported span for every function, log message, or polling attempt.
RMS does not inject trace context into outbound Redfish, NVUE, SSH/SFTP, NMX-C,
or gNMI calls, so these spans do not trace work inside those remote systems.

### Caller context and sampling

RMS accepts W3C `traceparent` in gRPC metadata:

```text
traceparent: 00-11111111111111111111111111111111-2222222222222222-01
```

The fields are version, trace ID, caller span ID, and flags. RMS keeps the trace
ID, sets the caller's span ID as `parentSpanId`, and creates its own `spanId`.
Missing, malformed, duplicate, or oversized context starts a new RMS root trace.
RMS accepts one header of at most 512 bytes; it ignores `tracestate` and baggage.

The default `sampling_ratio = 1.0` records every approved RMS span, even if the
caller sends sampled flag `00`. This independent decision is RMS policy; OTel's
default parent-based sampler normally follows the parent. At `0.1`, RMS selects
roughly 10% of uniformly distributed trace IDs, with the same decision for
local descendants—not 10% of spans from every request.

Keep `1.0` for this rollout. The pinned SDK's `TraceIdRatioBased` sampler is
[deprecated by OTel](https://opentelemetry.io/docs/specs/otel/trace/sdk/#traceidratiobased),
and decisions below 100% can differ across SDK versions/languages. Review that
compatibility before reducing the ratio or upgrading the SDK. Sampling 100%
does not guarantee delivery or retention. Separate client/RMS backends can be
searched by the shared trace ID; context propagation does not move stored
spans between backends.

### Follow a read-only request

Complete [Collector setup](#collector-configuration) first. Install `grpcurl`
and point `RMS_PROTO_DIR` at the `proto/` directory from the `nv-rms-client`
revision pinned in RMS's `Cargo.toml`. Set `RMS_API_ADDR` to the RMS API's
certificate-matching hostname and port, and supply its API CA and authorized
client certificate/key paths. These are API credentials, separate from the
Collector's trust configuration.

```bash
TRACE_ID=$(openssl rand -hex 16)
CALLER_SPAN_ID=$(openssl rand -hex 8)

grpcurl \
  -cacert "$RMS_API_CA" -cert "$RMS_CLIENT_CERT" -key "$RMS_CLIENT_KEY" \
  -import-path "$RMS_PROTO_DIR" -proto rack_manager.proto \
  -H "traceparent: 00-${TRACE_ID}-${CALLER_SPAN_ID}-01" \
  -d '{}' "$RMS_API_ADDR" rack_manager.RackManager/GetVersion

printf 'Look up trace: %s\n' "$TRACE_ID"
```

`GetVersion` only reads the server version. After the batch delay (default
5 seconds, plus any Collector/backend delay), search your trace backend for the
printed ID, or inspect Collector debug logs for the validation-only setup. Expect
a `rack_manager.RackManager/GetVersion` server span with a new span ID,
`parentSpanId` equal to `CALLER_SPAN_ID`, RPC status `OK`, and span status `Ok`.
The command supplies a parent identity; it does not export a caller span.

For a Kubernetes deployment at the default `info` log level, find correlated
stdout logs with:

```bash
kubectl -n "$RMS_NAMESPACE" logs deployment/rms-api-server --since=5m \
  | rg "$TRACE_ID"
```

Repeat with a fresh trace ID and flag `00` to check RMS's independent sampling.
Remove the `-H` option to check that RMS starts its own trace; find that ID in
RMS logs. These checks exercise propagation and export without changing racks,
nodes, firmware, or jobs.

### An example span and its fields

This successful `GetVersion` span came from a local, read-only validation run.
It is shown as JSON with empty/default fields visible and attribute order
normalized. RMS sends **binary protobuf**, not OTLP/JSON. In an export request,
this object sits inside `resourceSpans[].scopeSpans[].spans[]`.

```json
{
  "traceId": "11111111111111111111111111111111",
  "spanId": "a622a20a5ce49e14",
  "parentSpanId": "2222222222222222",
  "traceState": "",
  "flags": 769,
  "name": "rack_manager.RackManager/GetVersion",
  "kind": 2,
  "startTimeUnixNano": "1789488305655090000",
  "endTimeUnixNano": "1789488305655488000",
  "attributes": [
    { "key": "rpc.system.name", "value": { "stringValue": "grpc" } },
    {
      "key": "rpc.method",
      "value": { "stringValue": "rack_manager.RackManager/GetVersion" }
    },
    { "key": "rpc.response.status_code", "value": { "stringValue": "OK" } }
  ],
  "droppedAttributesCount": 0,
  "events": [],
  "droppedEventsCount": 0,
  "links": [],
  "droppedLinksCount": 0,
  "status": { "code": 1, "message": "" }
}
```

- **`traceId`:** 16 bytes, displayed as 32 hex characters. Finds the whole trace
  across services and logs.
- **`spanId`:** 8 bytes, displayed as 16 hex characters. Identifies this operation.
- **`parentSpanId`:** the immediate parent's span ID; empty for a root span.
  Backends use it to reconstruct the operation tree.
- **`traceState`:** vendor-specific context; empty because RMS ignores inbound
  `tracestate`.
- **`flags`:** an OTLP bitmask, not status. `769` (`0x301`) means sampled
  (`0x01`), parent remote/local state known (`0x100`), and remote parent (`0x200`).
  The sampled bit reflects RMS's decision.
- **`name`:** a stable operation name for grouping traces. It contains no
  request IDs or arbitrary URLs.
- **`kind`:** role enum. RMS uses `2` (`SERVER`) and `1` (`INTERNAL`). Other
  values are `0` unspecified, `3` client, `4` producer, and `5` consumer.
  A viewer can display labels; OTLP JSON encodes enums as numbers.
- **`startTimeUnixNano` / `endTimeUnixNano`:** UTC-epoch nanoseconds. Here they
  represent `2026-09-15T16:05:05.655090000Z` and
  `2026-09-15T16:05:05.655488000Z`: duration **0.398 ms**. This measures the
  operation, not export latency. JSON uses decimal strings to preserve 64-bit
  precision; viewers can format UTC or local time.
- **`attributes`:** typed key/value facts for filtering. `stringValue` denotes
  a string; the complete export allowlist is below.
- **`status.code`:** `0` = `Unset`, `1` = `Ok`, `2` = `Error`. These are OTel
  values, separate from gRPC status codes.
- **`status.message`:** a concise, safe error description when available;
  otherwise empty. Progress messages belong in logs.
- **`events` / `links`:** annotations and extra span relationships. RMS exports
  neither; ordinary log events remain in stdout. Parent relationships still
  use `parentSpanId`.
- **`droppedAttributesCount`:** attributes lost to SDK recording limits
  (128 per span). Later export-allowlist removals do not increase this count.
- **`droppedEventsCount` / `droppedLinksCount`:** RMS clears these collections
  and counters before export. They do not count lost logs or spans.

See the [OTLP JSON mapping](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding)
for field encodings. Span delivery failures have separate metrics below.

#### Exported attributes and process identity

The complete span-attribute allowlist is:

| Attribute | Use |
| --- | --- |
| `rpc.system.name` | `grpc` on request spans. |
| `rpc.method` | Fully qualified RPC name or `_OTHER`; groups requests. |
| `rpc.response.status_code` | gRPC status name, such as `OK` or `INTERNAL`; useful even when span status is `Unset`. |
| `job_id` | Connects an RMS job span to job-status results and logs. |
| `parent_job_id` | Identifies a child job's RMS parent; omitted when absent. |
| `error.type` | A bounded failure category, without raw error text. |

`job_id` and `parent_job_id` are RMS-specific attributes, separate from OTel span
IDs. Not every span has every attribute. Log-only fields such as `rpc.service`,
`rpc.grpc.status_code`, and `http.response.status_code` are not exported.
String attributes and error descriptions are redacted and limited to 1,024
characters, plus an ellipsis when truncated. Credentials, payloads, and raw
job/response failure details are excluded.

OTLP carries shared identity outside the span:

- **Resource:** `service.name = rackmanagementservice`, `service.version` from
  the binary's Git build description, and a startup UUID as `service.instance.id`.
  SDK fields are `telemetry.sdk.name = opentelemetry`,
  `telemetry.sdk.language = rust`, and `telemetry.sdk.version = 0.32.1`.
- **Instrumentation scope:** `name = rms.spans`, with the same build-description
  version. One scope can contain spans from multiple traces.

Site/cluster enrichment and tenant routing are Collector deployment concerns;
RMS does not infer or add them automatically.

### How RMS assigns status

A span describes its own operation; status does not automatically summarize
parents or children. OTel generally recommends `Unset` for successful
instrumentation but allows application code to assert verified `Ok`. RMS uses:

- **`Unset`:** no final success/error assertion. This is normal for a request
  that only accepts a job, an install/reboot handoff, a parent job, or a completed
  job without a verified postcondition. Early request cancellation can also
  leave it unset. **`Unset` is not proof of business success.**
- **`Ok`:** completed `GetVersion`, successful target-image steady-state checks,
  and leaf jobs verifying the target image, certificate bindings, factory-reset
  readiness, or SDN reset runtime state. It describes the condition checked at
  that time, not every downstream effect.
- **`Error`:** failed jobs, phases returning errors, request execution failures
  (including handler panics that unwind), transport/body failures, and gRPC
  `UNKNOWN`, `DEADLINE_EXCEEDED`,
  `UNIMPLEMENTED`, `INTERNAL`, `UNAVAILABLE`, or `DATA_LOSS`. A recovered retry
  does not fail its phase. Other codes, such as `INVALID_ARGUMENT`, can retain
  `Unset` while recording the RPC status.

RMS asserts `Ok` only at completion: OTel gives it precedence over `Error`.
A gRPC `OK` response can contain a business failure or accepted asynchronous
job, so middleware does not treat every successful response as verified success.
Check RPC status, job results, child spans, and logs together. See the
[status contract](https://opentelemetry.io/docs/specs/otel/trace/api/#set-status)
and [RPC conventions](https://opentelemetry.io/docs/specs/semconv/rpc/rpc-spans/).

#### Error categories

`error.type` is case-sensitive. The six gRPC error names above have empty
status descriptions. Additional request categories describe only observed facts:

| Category | Safe description |
| --- | --- |
| `missing_grpc_status` | `gRPC response ended without a status` |
| `response_body` | `gRPC response body failed`, `gRPC response body panicked`, or `gRPC response body dropped before completion` |
| `http_status` | `gRPC response returned HTTP <numeric status>` |
| `request_execution` | `gRPC service failed before returning a response` or `gRPC service panicked before returning a response` |

Job categories come from `JobError::metrics_label()`:

```text
unspecified, internal, other, invalid_argument, failed_precondition,
client_error, server_error, timeout, invalid_response, unauthenticated,
target_not_found, file_not_found, update_in_progress
```

Phase categories use `ErrorCode` display names:

```text
Internal, NotFound, AlreadyExists, InvalidArgument, Timeout,
FailedPrecondition, Unauthenticated, Unavailable, Cancelled,
Unimplemented, ConnectionRefused, DnsResolutionFailed, Aborted
```

Jobs and phases leave descriptions empty. Unrecognized job errors and abandoned
jobs use `internal`; forced shutdown uses `other`. Jobs that handle cancellation
themselves retain their workflow's category (such as `client_error` or `internal`).
Rejected admission creates no accepted-job span. A later request failure can
replace an earlier category. These are possible values, not a list of failures
observed in testing.

### Logs, delivery, and troubleshooting

Logfmt lines inside traced work carry `trace_id` and `span_id`; an unexported
nested span can use its nearest traced ancestor's IDs. Increasing log verbosity
does not add exported spans. Jobs export at their terminal state, so use logs
for progress while a job remains open.

When a trace is missing or incomplete:

1. Confirm `endpoint` is configured, the Collector has a traces pipeline, and
   TLS hostname/CA verification succeeds. For a loopback endpoint, confirm a
   Collector runs in RMS's network namespace, such as a sidecar in its Pod, and
   listens on that exact address. An unreachable Collector does not stop RMS
   from serving requests; invalid local configuration can stop startup.
1. Wait for the operation to finish and the batch delay (default 5 seconds).
   A successful API response may only mean an asynchronous job was accepted.
1. Check RMS export failure/recovery logs and these Prometheus counters:
   `rms_trace_spans_dropped_total` (queue, export, or shutdown losses),
   `rms_trace_spans_rejected_total` (Collector partial rejection), and
   `rms_trace_export_batches_failed_total` (failed/timed-out/cancelled batches).
1. Check Collector export errors, sampling, routing, and backend retention.
   RMS treats invalid OTLP acknowledgements as export failures. Collector
   acceptance alone does not prove the backend retained the trace.

Delivery is best effort: a bounded queue plus one in-flight batch, bounded
attempt/retry deadlines, and no durable RMS span storage. RMS drains work before
a final flush with its own deadline; a full span queue cannot reject the shutdown
signal. Spans left at that deadline are counted as dropped. Spans lost before
entering a batch do not increment the failed-batch counter.

An unexpected export-worker exit or poisoned queue lock stops export for that
process and emits a safe diagnostic; RMS continues serving requests. Subsequent
spans count as `processor_closed`. Investigate the warning and restart RMS to
restore export. Loss accounting after a panic inside the queue is best effort.

### Developing traced operations

Use existing request middleware and `JobTracker` for their boundaries. JobTracker
captures parent context before background work runs and ends job spans at terminal
transitions. Do not keep a request span open until an asynchronous job finishes.
If assigning a job's parent context fails, RMS warns once per process and lets
the job continue. This warning does not detect missing export-catalog entries.

For a phase, the existing `wait_for_target_steady_state` wrapper in
`api/grpc/switch_image_handlers.rs` demonstrates the pattern (body excerpt):

```rust
use tracing::Instrument;

let span = tracing::info_span!(
    "wait_for_target_steady_state",
    job_id = job_id,
    target_build_id = target_build_id,
);
let result = wait_for_target_steady_state_inner(
    switch,
    target_build_id,
    target_user,
    target_password,
    cancel,
)
.instrument(span.clone())
.await;
record_phase_result(&span, &result, true);
result
```

The span inherits the active job context. `.instrument(...)` enters it whenever
Rust polls the future, preserving context across `.await`; do not hold a
`span.enter()` guard across an await. The clone keeps the span alive so the
wrapper records its result before it closes. `record_phase_result` is the
existing internal helper in `observability/tracing.rs`: it records a bounded
error category on failure, `Ok` only when `verified_success` is true, otherwise
leaves success `Unset`. This wrapper uses `true` because its inner function
verifies the target state. Upload/install handoffs use `false`.

For a new phase, choose a stable boundary and name, then add the reviewed
name to `should_export`. Extend the attribute allowlist only when needed. A new
`info_span!` name is not automatically exported; do not reuse another operation's
approved name to bypass that review. `target_build_id` above is useful in logs
but is filtered from export. Never add passwords or raw request/error payloads.
Test parent IDs, error outcomes, cancellation, and any verified-success claim.

Job span names and their export catalog share the exhaustive `job_spans!` table
in `job_tracker.rs`; add new job types there. RPC catalog tests compare request
names with the pinned `librms` protobuf descriptors and fail if a client-library
update adds an RPC missing from the catalog.

#### RPC convention coverage

OTLP wire compatibility does not mean full RPC semantic-convention coverage:
RMS names unknown routes `_OTHER` (the convention recommends `grpc`), can omit
`rpc.response.status_code` on early execution failure/cancellation, and excludes
conditional `rpc.method_original`, `server.address`, `server.port`, and network
peer fields from its allowlist. Forced shutdown records `CANCELLED` if no status
was recorded. See [RPC naming](https://opentelemetry.io/docs/specs/semconv/rpc/rpc-spans/#name)
and [gRPC attributes](https://opentelemetry.io/docs/specs/semconv/rpc/grpc/#server)
before extending the catalog.

### Tracing configuration reference

All durations below are milliseconds. Queue/batch sizes and durations must be
positive; retries and jitter may be zero. Unknown `[tracing]` keys are rejected.

| Key | Default | Purpose and limits |
| --- | --- | --- |
| `endpoint` | - | HTTPS Collector URL, or `http://` to a loopback IP address (`127.0.0.0/8` or `[::1]`), at most 2048 characters. gRPC has no path; HTTP/protobuf ends in `/v1/traces`. Omit to disable export. |
| `protocol` | `grpc` | `grpc` or `http_protobuf`, both over TLS except to a loopback IP address. |
| `sampling_ratio` | `1.0` | RMS's recording ratio, independent of caller flags; `(0.0, 1.0]`. |
| `max_queue_size` | `2048` | Waiting completed spans; maximum `65536`. |
| `max_export_batch_size` | `512` | Spans per export; maximum `4096` and no larger than the queue. |
| `scheduled_delay_ms` | `5000` | Non-full batch delay; maximum `60000`. |
| `export_attempt_timeout_ms` | `10000` | Per-attempt deadline; maximum `60000` and no longer than the batch deadline. |
| `export_timeout_ms` | `30000` | Total batch deadline including retries; maximum `120000`. |
| `shutdown_timeout_ms` | `5000` | Final flush budget after work drains; maximum `30000`. |
| `max_retries` | `3` | Maximum extra attempts after temporary failure, subject to batch/shutdown deadlines; maximum `10`. |
| `retry_initial_delay_ms` | `100` | Initial backoff; no longer than `retry_max_delay_ms`. |
| `retry_max_delay_ms` | `1600` | Local backoff cap; maximum `60000`. Collector-requested delays may exceed it, within batch/shutdown deadlines. |
| `retry_jitter_ms` | `100` | Maximum added backoff jitter; maximum `60000`. |

The batch deadline includes attempts and backoff; it can stop export before
`max_retries` is reached. With defaults, only two full 10-second timed-out
attempts fit within the 30-second budget; the next attempt or backoff is cut
short and counted as `export_timeout`. Shutdown can end export sooner.

### Where to read the implementation

Paths below are relative to `crates/rackmanagementservice/src/`:

- `logging/setup.rs` and `logging/api_logs.rs`: SDK connection, independent log
  filtering, incoming context, and request lifetime/status.
- `orchestrator/job_tracker.rs` and `orchestrator/job_lifecycle/registry.rs`:
  job names, parent context, terminal status, and span completion.
- `observability/tracing.rs` and `observability/grpc.rs`: resources, sampler,
  exporter, retries, phase-result helper, and span/attribute allowlists.
- `observability/batch.rs`, `observability/span_queue.rs`, and
  `observability/trace_metrics.rs`: queue, deadlines,
  shutdown, and loss counters. `config/mod.rs` validates the settings above.
- `orchestrator/job_lifecycle/error.rs` and `utilities/error.rs`: error categories.

In code, `tracing::Span` becomes SDK `SpanData`, then an OTLP protobuf `Span`.
Special macro fields such as `otel.name`, `otel.kind`, and `otel.status_code`
set span fields; they are not exported as extra attributes. This release pins
OTel API/exporters to `0.32.0`, SDK to `0.32.1`, and `tracing-opentelemetry` to
`0.33.0`. See [SDK SpanData](https://docs.rs/opentelemetry_sdk/0.32.1/opentelemetry_sdk/trace/struct.SpanData.html)
and [OTLP Span](https://docs.rs/opentelemetry-proto/0.32.0/opentelemetry_proto/tonic/trace/v1/struct.Span.html).

#### Maintaining the export adapters

RMS owns its batch processor, gRPC exporter, and HTTP response adapter for the
pinned SDK. They provide Tokio execution, bounded shutdown/loss accounting,
gRPC `RetryInfo` handling, and the OTLP HTTP retry-status set. Re-evaluate them
when upgrading OTel: retire the retry adapters when upstream provides equivalent
behavior or a classifier hook. Re-evaluate the default SDK processor with a Tokio
runtime adapter; retain deadline, overload, and loss-accounting tests before
switching. This is a migration direction, not an implemented replacement.

## Persistence and TLS are independent

`[postgres] db_url` is independent of `[tls] insecure`: you can run with TLS and
the in-memory store, or with `insecure = true` and Postgres, or any combination.
`[tls]` configures the gRPC server's transport; `[postgres]` configures the
persistence layer.

## Setting these values via Helm

When deploying on Kubernetes, you don't hand-write `config.toml` - the Helm chart
renders it from `apiServer.*` values. See [Configuration via Helm](via-helm.md)
for the value → key mapping.
