# Workspace Crates

RMS is a Cargo workspace. The core service lives in `crates/rackmanagementservice`
(crate `rackmanagementservice`), and supporting crates provide the protocol clients
and the firmware-update engine. Each is documented as its own crate below.

## `redfish_client`

A thin, RMS-focused wrapper over the workspace `nv-redfish` crate. It exposes
exactly the Redfish operations RMS needs - power state and reset, MNNVLink
topology discovery, and multipart firmware upload - behind a small, stable surface
and an RMS-shaped error enum, rather than exposing `nv-redfish`'s full schema API
to callers.

- **Artifact:** library (`redfish_client`).
- **Key types:** `RedfishClient` (a cheap-to-clone handle), `RedfishError` /
  `Result`, `NvidiaMnnvlinkTopology` (chassis serial, slot, tray index from the
  Processor OEM block), and passthroughs from `nv-redfish` (`BmcCredentials`,
  `PowerState`, `ResetType`, `DataStream`, `UploadReader`).
- **Key `RedfishClient` methods:** `new`, `computer_system_power_state`,
  `reset_computer_system`, `reset_manager`, `nvidia_mnnvlink_topology`, and the
  `multipart_update_firmware*` family (with `_with_timeout` / `_from_path`
  variants; `DEFAULT_UPLOAD_TIMEOUT` is 600 s).
- **`RedfishError`** mirrors gRPC-style status categories (`NotFound`, `Timeout`,
  `Unauthenticated`, `FailedPrecondition`, …) so it maps cleanly to
  [`RmsError`](../architecture/internal_view.md#error-handling).
- **Built on:** `nv-redfish` (features `bmc-http`, `computer-systems`, `managers`,
  `processors`, `update-service`), `reqwest`, `tokio`.
- **Used by:** re-exported through `crates/rackmanagementservice/src/transport/redfish_client.rs` and consumed
  by the compute nodes (MNNVLink topology) and the GB200 switch node (power/reset).

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
  (`crates/rackmanagementservice/src/nodes/switch_gb200_nvidia/*`) for all NVUE transport and cluster,
  revision, system, and SDN models. The `nvfwupd` crate also depends on it for the
  switch firmware-update path.

## `nvfwupd`

The embedded firmware-update engine (crate directory `crates/rust_nvfwupd`, package name
`nvfwupd`). It performs out-of-band firmware updates on NVIDIA server, switch, and
power-shelf platforms - primarily over Redfish (BMC HTTP), with additional IPMI,
SSH/SFTP, and direct-OS transports. It parses firmware packages, compares
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

RMS's `crates/rackmanagementservice/src/nodes/nvfwupd_adapter.rs` maps between RMS domain types and these
`workflow` types; the per-platform compute and power-shelf node modules build a
`TargetConfig` and call the `workflow_api` functions directly.

- **Built on:** `clap`, `reqwest`, `russh`/`russh-sftp`, `tar`, `nvue_client`
  (for switch NVUE access), `tokio`, `tracing`.
