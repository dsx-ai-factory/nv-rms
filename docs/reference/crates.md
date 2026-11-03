# Workspace Crates

RMS is a Cargo workspace. The core service lives in `crates/rackmanagementservice`
(crate `rackmanagementservice`), and supporting crates provide the protocol clients
and the firmware-update engine. Each is documented as its own crate below.

## `redfish_client`

A thin, RMS-focused wrapper over the workspace `nv-redfish` crate. It exposes
exactly the Redfish operations RMS needs - power state and reset, MNNVLink
topology discovery, multipart firmware upload, and origin-safe JSON GET/POST
for vendor powershelf resources - behind a small, stable surface
and an RMS-shaped error enum, rather than exposing `nv-redfish`'s full schema API
to callers.

- **Artifact:** library (`redfish_client`).
- **Key types:** `RedfishClient` (a cheap-to-clone handle), `RedfishError` /
  `Result`, `NvidiaMnnvlinkTopology` (chassis serial, slot, tray index from the
  Processor OEM block), `ODataLink` / `ResourceCollection` for typed `@odata.id`
  member lists, and passthroughs from `nv-redfish` (`BmcCredentials`,
  `PowerState`, `ResetType`, `DataStream`, `UploadReader`).
- **Key `RedfishClient` methods:** `new`, `computer_system_power_state`,
  `reset_computer_system`, `reset_manager`, `nvidia_mnnvlink_topology`, and the
  `multipart_update_firmware*` family (with `_with_timeout` / `_from_path`
  variants; `DEFAULT_UPLOAD_TIMEOUT` is 600 s), plus `get_resource` (typed body),
  `get_json` (untyped `Value`), and `post_action` for BMC-advertised URIs.
  Absolute and protocol-relative URIs are rejected; remaining paths are resolved
  against the configured BMC origin and credentials are not attached to a
  different host.
- **`RedfishError`** mirrors gRPC-style status categories (`NotFound`, `Timeout`,
  `Unauthenticated`, `FailedPrecondition`, …) so it maps cleanly to
  [`RmsError`](../architecture/internal_view.md#error-handling).
- **Built on:** `nv-redfish` 0.18.1 (features `bmc-http`, `computer-systems`, `managers`,
  `processors`, `update-service`), `reqwest`, `tokio`.
- **RMS power mapping:** Redfish `On` and `Off` map to the corresponding RMS
  states. Other readings, including `Sleeping`, `Hibernating`, and absent values,
  map to RMS `Unknown`.
- **Used by:** re-exported through
  `crates/rackmanagementservice/src/transport/redfish_client.rs` and consumed
  by the compute nodes (MNNVLink topology), the GB200 switch node (power/reset),
  and powershelf LiteOn/Delta power and discovery.

## `nvswitch_cli`

The typed NVOS `nv` CLI layer shared by RMS and the embedded `nvfwupd` crate.
Switch CLI commands were previously assembled as string literals and `format!`
calls at each call site, where a misspelled verb, a transposed path segment,
or a `nv config save` that drifted ahead of its `nv config apply` all produced
code that compiled and silently did the wrong thing on a switch.

This crate makes commands and their ordering into values:

- **Commands.** `NvCommand` (`nv show` / `set` / `unset` / `config` / `action`)
  and `ShellCommand` (`install -d`, `mkdir -p`, `test -f`, `chmod`, `rm -f`)
  render themselves. Arguments are validated types - `NvPath`, `RemotePath`,
  `ClusterApp`, `FileMode` - that reject anything needing quoting or able to
  split a command line. Shell quoting happens at render time, once.
- **Flows.** `CommandFlow` fixes a sequence in a named constructor rather than
  leaving it implicit in whichever function runs the commands.
  `CommandFlow::unset_mtls` is the worked example: per-service unsets, then
  `nv config apply --assume-yes`, then `nv config save`.
- **Failure policy.** NVOS reports "already in the desired state" and "this
  build has no such schema" as ordinary command failures. `BenignFailure`
  names the condition with a `BenignReason` next to the step it belongs to, and
  `FlowStep::with_fallback` covers operations NVOS spells two ways across
  builds.
- **Execution.** `NvSwitchCliHandler` owns per-command timeouts, logging, and
  output checking. Commands are run once; `nv` commands are not uniformly
  idempotent, so whether a retry is safe is left to the caller that knows what
  the command does.
- **Reporting.** `run_flow` returns a `FlowReport` with one `StepReport` per
  declared step, and returns the same report inside a `FlowFailure` when a step
  aborts the flow - so a caller can tell which steps took effect, which were
  tolerated, and which were never attempted. Steps are identified by the
  `StepSubject` tagged onto them, not by their command line: rendered commands
  stay in this crate's tracing output and never reach a caller, keeping them
  out of RMS job results and API error messages.
- **Stopping.** `run_flow_while` consults a caller predicate at step
  boundaries only, never mid-command, so a stopped run still reports each step
  as either fully attempted or never started rather than guessing whether a
  command reached the switch. The report is marked `interrupted`, which is
  what separates a stopped run from one that ended on its own. Racing
  `run_flow` in a `select!` instead would drop the future and lose the report,
  which is exactly the attribution the caller needs.

- **Artifact:** library (`nvswitch_cli`).
- **Key types:** `NvSwitchCliHandler`, `HandlerConfig`, `CommandFlow`,
  `FlowStep`, `BenignFailure` / `BenignOutcome`, `SwitchCommand`
  (`NvCommand` | `ShellCommand`), `MtlsService`, `ManagerEncryption`,
  `InvalidCommand`.
- **Bring your own SSH:** the crate does not open sessions. Callers implement
  `NvSwitchTransport` over an existing session and `CommandFailure` over the
  error type they already propagate, so RMS's `ErrorCode` survives a round trip
  instead of being flattened into an intermediate enum.
- **Built on:** `thiserror`, `tokio` (timers only), `tracing`.
- **Used by:**
  `crates/rackmanagementservice/src/nodes/switch_gb200_nvidia/cli.rs`
  (mTLS unset flows, cluster-app manager encryption, remote TLS staging, SFTP
  upload verification) and `crates/rust_nvfwupd/src/switch_cli.rs` (firmware
  staging and cleanup on switch targets).

## `nvue_client`

An intentionally narrow async HTTP client for the NVUE / NVOS REST API, plus the
focused request/response model types shared by RMS and the embedded `nvfwupd`
crate. It targets a specific NVOS API version and models only the shapes RMS and
`nvfwupd` actually need; the OpenAPI document is deliberately not vendored.

A distinctive feature is **client-TLS rotation**: the client builds a complete
candidate transport, verifies it with a read-only NVUE request, then atomically
swaps it in. A failed or cancelled verification leaves the last-known-good
transport active, and in-flight requests finish on their original generation.

- **Artifact:** library (`nvue_client`).
- **Transport types:** `Client`, `ClientConfig`, `ClientCredentials`,
  `ClientEndpoint`, `ClientError`, `NvueResponse`, `PreparedClientTls`,
  `SharedClient` (`Arc<Client>`), `ClientTls` / `ClientTlsPaths`.
- **Domain modules** (each maps to an NVUE endpoint family): `action`
  (action-job polling), `cluster` (NMX controller cluster/app management),
  `platform` (platform firmware), `revision` (NVUE config apply/save), `sdn`,
  and `system` (system images, power cycle, user password, gNMI server).
- **Built on:** `reqwest`, `rustls`, `secrecy`, `sha2`, `tokio`.
- **Used by:** the GB200 switch node stack
  (`crates/rackmanagementservice/src/nodes/switch_gb200_nvidia/*`) for all NVUE
  transport and cluster, revision, system, and SDN models. The `nvfwupd` crate
  also depends on it for the switch firmware-update path.

## `nvfwupd`

The embedded firmware-update engine (crate directory `crates/rust_nvfwupd`,
package name `nvfwupd`). It performs out-of-band firmware updates on NVIDIA
server, switch, and power-shelf platforms - primarily over Redfish (BMC HTTP),
with additional IPMI, SSH/SFTP, and direct-OS transports. It parses firmware
packages, compares
installed versus packaged versions, drives multipart uploads and update tasks,
polls task/job status, and handles activation, background copy (SPI slot swap),
staged updates, factory reset, and CPLD/flint flows.

- **Artifact:** **both** a library (`nvfwupd`) and a binary (`nvfwupd`). The
  binary is bundled into the RMS release image for operator diagnostics.
- **Firmware package formats:** PLDM `.fwpkg` packages (native Rust parsing of the
  PLDM header, plus an `unpack` mode), tar packages (e.g. power-shelf firmware),
  CPLD `.vme` images (switch targets), and `flint`/Mellanox NIC firmware over SSH.
- **CLI subcommands:** `show_version`, `update_fw`, `activate_fw`,
  `background_copy`, `force_update`, `show_update_progress`,
  `perform_factory_reset`, `show_pkg_content`, `unpack`, `make_upd_targets`,
  `flint_update`, plus `help` / `version`. Global options select the target
  (`-t`), OS target (`-o`), config (`-c`), and verbosity.
- **Core abstraction:** the async `RFTarget` trait, with a concrete target per
  platform (GB200, GB300/VRNVL72 via wrappers, DGX, GH200, HGX, power shelf, switch).

### The workflow API RMS uses

RMS does not build concrete `RFTarget`s. Instead it calls the crate's **workflow
API**, its RMS-facing facade:

- `nvfwupd::workflow` defines the type vocabulary: `TargetConfig`, `ServerType`,
  `FirmwareUpdateRequest`, `UpdateOptions`, `FirmwareUpdateOutcome`, `TaskHandle`,
  `TaskStatus` / `TaskState`, `ActivationRequest` / `ActivationSummary`,
  `FirmwareVersionCheckRequest` / `FirmwareComponent`, and the `NvFwUpdError` enum.
- `nvfwupd::workflow_api` provides the free async functions RMS calls:
  `get_firmware_inventory`, `verify_firmware_versions` /
  `verify_firmware_target_versions`, `update_firmware`, `get_task_status`, and
  `activate_firmware`. A `WorkflowContext` variant can route switch NVUE requests
  through an existing `nvue_client` transport.

RMS's `crates/rackmanagementservice/src/nodes/nvfwupd_adapter.rs` maps between
RMS domain types and these `workflow` types; the per-platform compute and
power-shelf node modules build a `TargetConfig` and call the `workflow_api`
functions directly.

- **Built on:** `clap`, `reqwest`, `russh`/`russh-sftp`, `tar`, `nvue_client`
  (for switch NVUE access), `tokio`, `tracing`.

## `nvdebug_cli`

Runs the nvdebug log collector as an executable RMS finds by configuration
(crate directory `crates/nvdebug_cli`). The CLI is the integration surface
until nvdebug's Rust port ships as a library crate; RMS then compiles it in
and this wrapper is removed. It renders the one invocation RFD 0001
commits to, `collect` with a DUT file, a collector-id list, an output
directory, and the non-interactive flags, plus the same arguments under
`--dry-run` and `--version` as the availability probe. It interprets nothing
nvdebug writes.

- **Artifact:** a library (`nvdebug_cli`). The binary `nvdebug-probe` lives
  in its own package, `crates/nvdebug_probe`, so the library carries none of
  its dependencies. It exercises the API against a real nvdebug and a real
  BMC and is not part of the release image.
- **Types:** `Executable` (an absolute path to a regular file), `Target` (a
  name, a `Bmc`, a baseboard),
  `Bmc` (a `BmcHost` and a Redfish login), `BmcHost` (an IP address or a DNS
  name, without brackets, a port or an IPv6 zone id; nvdebug resolves a name
  itself),
  `CollectorId`, `CollectRequest` (targets, collectors, an absolute output
  directory, a timeout with no default), `Collection` (nvdebug's exit status,
  the output directory, and the start time), `Error` (`Invalid`,
  `Unavailable`, `Timeout`, `Cancelled`, `Failed`: what a caller decides
  on), `Detail` (the action and its object as text, and the error behind it
  as `source()`).
- **Guards:** the BMC password travels in a 0600 file inside a 0700 temporary
  directory, removed when the run ends, never on argv. The directory is
  made and removed on the blocking pool, so a stalled temp mount never holds
  an executor thread. A process killed with SIGKILL leaves its directory
  behind, so `sweep_stale_dut_dirs`, called once at start, removes those
  whose process no longer runs. The probe calls it at its own start. With
  `TMPDIR` on a memory-backed mount, the password never reaches a disk.
  nvdebug leads a process group of its own. Its helpers join it, unless
  one starts a session of its own, as nvdebug 2.2.0 does for some. On
  timeout or cancel the group gets SIGTERM, which nvdebug traps to cancel
  its collectors and kill its helpers, those in sessions of their own
  included. The group then gets up to 10 s to empty before SIGKILL. A helper
  outside the group is out of the crate's reach: it outlives a SIGKILL of
  nvdebug, which comes when nvdebug ignores SIGTERM or a caller drops a run.
  nvdebug gets only part of the caller's environment: `PATH`, `HOME`,
  `TMPDIR`, `LANG`, every `LC_*`, `TZ`, and the trust-store variables
  `SSL_CERT_FILE`, `SSL_CERT_DIR` and `REQUESTS_CA_BUNDLE`. The caller's
  secrets stay out, and so do nvdebug's own `NVDEBUG_*` variables, which
  would override what the crate passes.
  Nothing here checks that the executable is the one configured. A check of
  its content belongs where the configuration is read, and is a later
  change. nvdebug's stdout and stderr are discarded: it writes its own log
  into the run directory, and `--version`, which reads its output, shows a
  broken install. A run claims its output directory before the spawn, with an
  exclusive `flock` on the directory held until the run returns, and is
  refused if another run holds the claim or the directory already holds an
  nvdebug run. The one `nvdebug_logs_*` directory in it after a run is that
  run's. A dry run leaves a run directory too, so a collect after it needs
  a directory of its own. The lock binds every caller of the crate, in any process, and
  nothing else. It lives as long as the caller: a caller killed outright,
  with SIGKILL or a crash, drops it, and nothing stops an nvdebug it leaves
  running. A later run can then share the directory with that nvdebug. A
  separate directory per run is a later change.
- **Exit status:** returned as nvdebug reported it. Exit 0 is not treated as
  success. nvdebug's own status files under the output directory say what
  each collector did, and reading them is a later change.
- **Built on:** `tokio` (process, fs), `tokio-util` (`CancellationToken`),
  `secrecy`, `serde_json`, `tempfile`, `thiserror`, `nix` (signals),
  `rustls-pki-types` (`DnsName`). The probe adds `clap`, `eyre` and
  `tracing-subscriber`.
