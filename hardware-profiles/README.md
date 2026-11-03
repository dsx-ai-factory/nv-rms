# RMS Hardware Update Profiles

Profiles configure firmware selection, upload parameters, update order,
activation, verification, and power-operation paths and timing. RMS includes
built-in profiles and can load a replacement catalog from a directory at startup.

The default profiles describe the recommended settings for supported hardware.
RMS validates configuration structure, consistency, bounds, and paths; this does
not establish hardware compatibility. Operators are responsible for the behavior
of custom settings. Credentials, device addresses, TLS settings, and firmware
files continue to use the existing RMS configuration and APIs.

## Directory configuration and startup loading

Copy the profiles you want to customize into a directory and configure RMS:

```toml
[workflows]
hardware_profiles_dir = "/etc/rms/hardware-profiles/v1"
```

RMS reads only the `.yaml` and `.yml` files directly in that directory.
The directory replaces the built-in profile catalog; it is not an overlay.
An unreadable directory, invalid profile, duplicate profile ID, or duplicate
node-type match prevents startup. Disabled profiles are still validated.

Empty and partial catalogs are valid. A node type without an enabled profile
uses RMS's default behavior. Vendor upload/OEM defaults in that fallback can
still come from the YAML embedded in the binary. Dell GB200 was introduced with
profile-based planning and uses its complete embedded profile whenever its
external override is missing or disabled.

Profiles are loaded once at startup. File edits, additions, and deletions do not
affect either running jobs or subsequent requests until RMS restarts. Deploy
changes and restart RMS after firmware work has finished. Changing the directory
setting also requires a restart. Without `hardware_profiles_dir`, RMS uses
embedded profiles; changing those requires rebuilding and restarting RMS.

From a source checkout, validate a directory and print its profile hashes and
update stages before deployment:

```console
cargo run -p rackmanagementservice --example compile_hardware_profiles -- \
  /etc/rms/hardware-profiles/v1
```

## Using profiles through the APIs

RMS selects the enabled profile whose `matches.node_type` matches the node.
There can be only one profile per node type in a catalog. API requests continue
to identify nodes through their registered identity or supplied node descriptor;
they do not accept a `profile_id` override.

| API | How the profile is used |
| --- | --- |
| `UpdateFirmware` | Uses the registered node's type. Each `firmware_targets[].target` selects a profile `component_key`; `filename` identifies the firmware file. |
| `BatchUpdateFirmwareByNodeType` / `BatchUpdateFirmware` | Selects the profile for each node's type and applies the same component-selection rules as a direct update. |
| `ApplyFirmwareObject` / `ApplyStoredFirmwareObject` | Uses the profile to map manifest artifacts to targets. Component filters select `component_key` values; without a filter, targets with `default_apply: true` are selected. |
| Compute power and reboot APIs | Use the matching profile's configured Redfish resource IDs, power paths, and applicable timeouts. |

For a registered node whose profile contains `component_key: BMC`, an
`UpdateFirmware` request can use:

```json
{
  "rack_id": "rack-01",
  "node_id": "compute-1",
  "firmware_targets": [
    {"target": "BMC", "filename": "bmc.fwpkg"}
  ],
  "activate": true,
  "force_update": false
}
```

The file must already be available in RMS's configured firmware directory.
Component matching ignores ASCII case and surrounding whitespace. Target IDs,
manifest source names, Redfish paths, and empty strings are not alternative
component selectors.

Direct update RPCs use the request's `activate` flag. Firmware-object apply
enables activation. A profile stage activates only when activation is enabled
and the stage sets `activate_after_stage: true`, after at least one update in
that stage was applied. Setting `activate: false` on a direct out-of-band update
suppresses those boundaries.

Poll the returned job IDs with `GetFirmwareJobStatus`. For complete request
formats, component filters, and the separate in-band NIC activation/verification
rules, see [firmware operations](../docs/operations/firmware.md).

Stored objects use the active startup profile when applied. Changes to mappings
or selection rules can require re-importing a legacy stored object's manifest;
RMS reports an error when that is necessary.

## Profile settings

| Field | Meaning |
| --- | --- |
| `schema_version` | Profile format version; currently `1`. |
| `profile_id` | Unique configuration identifier recorded in firmware job results. |
| `matches.node_type` | Existing canonical RMS node type to configure; selects the concrete node implementation. A profile does not define a new node type. |
| `matches.enabled` | Defaults to `true`. Set `false` to leave the profile unselected and use RMS's default behavior for that node type. |
| `driver` | Runtime settings for the implementation selected by `matches.node_type`. |
| `driver.nvfwupd_server_profile` | Selects `compute_gb200`, `compute_gb300`, or `compute_vrnvl72` update/activation behavior. |
| `driver.redfish` | System and manager IDs, device-info timeout, HTTP timeouts, and optional power paths. |
| `driver.timing` | Optional transport, readiness, activation, and recovery timing overrides. |
| `firmware.activation.strategy` | `full_compute_power_cycle`; required when a stage enables activation. |

The `driver` block does not select a separate implementation. Its
`nvfwupd_server_profile` selects NVFWUPD update/activation behavior and can differ
from the node type's default without changing the concrete node implementation.
Redfish and timing settings similarly configure that implementation.

## Firmware targets

Each entry in `firmware.targets` defines a selectable update target:

| Field | Meaning |
| --- | --- |
| `id` | Unique identifier for this target within the profile. |
| `component_key` | Component name used by direct requests and firmware-object component filters. Must be unique across the profile, ignoring ASCII case. |
| `source_components` | Manifest components that supply firmware for this target. |
| `filename` | Optional `suffix` and `contains` filters. Both must match when supplied. Omitted or `{}` accepts any otherwise eligible filename; `case_insensitive` defaults to `false`. |
| `default_apply` | Defaults to `true`. Set `false` to require explicit component selection. A full package can still update that component. |
| `stage` | Positive update-group number for each node. Lower numbers run first (for example, 10 before 20); targets within a group run sequentially in YAML order. |
| `activate_after_stage` | Whether to activate after the selected targets in this group finish, when activation is enabled. All targets in a stage must agree. |

For example, if targets A and B appear in that YAML order with `stage: 10`,
and target C has `stage: 20`, RMS finishes A, then B, then any enabled
activation and verification for stage 10 before starting C. Stage numbers need not be
consecutive. Only selected targets run; stages with no selected targets are
omitted. In-band NIC updates run in a separate phase before these configured
out-of-band stages.

One manifest component can list several firmware payloads in `Locations`.
RMS applies every eligible package in manifest order within each target.
Direct updates preserve request order for packages within each target.
For example, `source_components: [BMC]` selects all eligible packages listed
under that component; the number of source components does not limit the
number of packages.

Choose exactly one destination form for each target:

| Destination | Behavior |
| --- | --- |
| `request_targets` | Explicit Redfish destinations. Manifest apply can upload a package separately to each destination; direct updates require exactly one destination. |
| `driver_default_targets: true` | Let the driver or package determine destinations. |
| `update_parameters.Targets` or `update_parameters.HttpPushUriTargets` | Send one multipart upload containing the complete target array, including an explicit empty array. |

Filename filters select packages; they do not validate contents or compatibility.
Packages may be shared across distinct destinations. Repeated uploads of the
same package to the same destination within a configured target are rejected.

Redfish destinations and power paths must be plain absolute paths within
`/redfish/v1` on the configured BMC. URLs, traversal, escapes, backslashes,
query strings, fragments, and whitespace are rejected.

## Multipart parameters

Each target can supply `update_parameters` and `oem_parameters` as JSON-compatible
objects. Inline JSON and ordinary YAML mappings have identical semantics:

```yaml
update_parameters: {"Targets": ["/redfish/v1/UpdateService/FirmwareInventory/BMC"]}
oem_parameters: {"ImageType": "BMC"}
```

```yaml
update_parameters:
  Targets:
    - /redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0
    - /redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1
```

The second example sends one upload targeting both inventory entries. Configure
only one of `Targets` and `HttpPushUriTargets`, and do not combine either with
`request_targets` or `driver_default_targets`. Other update and OEM parameters
can accompany any destination form.

Both fields require objects; quoted JSON strings and YAML literal blocks are
not decoded again. Nested objects, arrays, strings, numbers, booleans, and null
values are supported. Omitting `oem_parameters` omits the multipart field;
`{}` sends an empty object. RMS validates known parameter shapes and target
paths without restricting vendor parameter names or values.

A request with `force_update: true` sets `ForceUpdate: true`; otherwise the
configured value applies. Configured `ApplyTime` takes precedence over a driver
default. Other configured values are forwarded unchanged.

Set image-type parameters explicitly when changing package formats. For example,
the bundled Lenovo BMC target uses `ImageType: BMC`; change it to `HPM` for an
`.hpm` image. RMS does not infer this value from the filename.

## Firmware polling and verification

| Setting | Behavior |
| --- | --- |
| `firmware.polling.task_timeout` | Polling budget for each device firmware task, excluding upload and activation. |
| `firmware.polling.interval` | Delay between task-status checks. |
| `firmware.polling.transient_error_budget` | Budget for consecutive transient task-poll failures. |
| `firmware.power_on.timeout` | Optional block, absent from the shipped profiles. Wait for the host to report power `On` after the power-on that opens the update. |
| `firmware.power_on.interval` | Delay between host power-state reads during that wait. Must not exceed the timeout. |
| `firmware.verification.enabled` | Enables installed-version checks after profile-stage activation. |
| `firmware.verification.settle_delay` | Wait after activation before checking versions. |
| `firmware.verification.attempts` | Total version-check attempts, including the first. |
| `firmware.verification.retry_interval` | Delay between unsuccessful checks. |

Profile polling settings take precedence over service-wide firmware polling
settings for that job. A `firmware.power_on` block will do the same for the
service-wide `compute_power_on_*` bounds once the power-on step lands in a
following change. Until then the block is validated and stored and nothing
reads it. Without the block, the service-wide bounds apply. These are per-phase
limits, not an end-to-end job deadline.
Disabling profile verification does not disable mandatory in-band NIC checks.

## Compute power paths

`driver.redfish.power` overrides endpoints for existing compute power operations:

| Field | Use when configured |
| --- | --- |
| `system_state_path` | Host power-state GET; also the resource used to discover the host reset action. |
| `system_reset_path` | Host on/off/restart POST for direct power requests and firmware activation. |
| `bmc_reset_path` | Host-BMC reset POST for direct power requests. |
| `hmc_reset_path` | HMC reset POST for direct power requests. |
| `aux_power_cycle_path` | Auxiliary power-cycle POST during firmware activation. |
| `bmc_ready_path` | Readiness GET after auxiliary power cycling. |

Omitted or `null` fields retain resource/action discovery and driver defaults.
Use an override when a system requires a resource or action path that RMS's
existing discovery does not select. An explicit override bypasses the relevant
lookup and never falls back to another endpoint on failure.

No bundled profile currently selects a non-default power endpoint. Host-state
and reset paths are `null`; the explicit readiness path `/redfish/v1` matches
the driver default.

For example, this fragment overrides the host resource and reset action:

```yaml
driver:
  redfish:
    power:
      system_state_path: /redfish/v1/Systems/System_0
      system_reset_path: /redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset
```

With only `system_state_path` set, RMS discovers the reset action from that
resource. Overrides do not add new power operations or change reset payloads,
firmware-upload endpoints, or factory-reset endpoints.

## Compute timing settings

The supplied YAML files document individual timing fields inline.
`driver.redfish` controls shared Redfish connection, request, direct-upload, and
device-info timeouts. NVFWUPD timing is grouped under `driver.timing`:

| Section | Controls |
| --- | --- |
| `transport` | HTTP connection, request, upload, and reachability timeouts. |
| `pre_update` | Background-copy readiness and write-protection inventory checks. |
| `activation` | Power-off delay, AC-cycle attempts/stabilization, and command retries. |
| `recovery` | BMC/host readiness polling, power-on resends, and recovery probes. |
| `inventory_recovery` | Expected-inventory retries after activation, when expected inventory is supplied. |

The background-copy check reads `Oem.Nvidia.BackgroundCopyStatus` on Redfish
chassis resources. It waits for every reported status to reach `Completed`
before uploading firmware. The copy runs on the device; RMS only reads its
status and does not select its source or destination. These settings do not
control artifact downloads into RMS.

`pre_update.background_copy_timeout` sets the overall wait budget (default
`15m`). `pre_update.background_copy_poll_interval` sets the delay between status
checks (default `10s`).

Omitting a timing section or field retains its default. Durations use integer
`ms`, `s`, `m`, or `h` values, up to 24 hours. The `driver.timing` fields and
Redfish connection/request/upload timeouts require whole seconds. Required
timeouts and polling intervals must be positive. Power-off and AC-cycle
stabilization delays, activation command retry intervals, and inventory-recovery
retry intervals may be zero.

Driver `attempts` values include the first request and allow 1–1000;
`retries` count additional requests and allow 0–1000. Background-copy polling
cannot exceed its wait budget; power-on resend intervals cannot be shorter than
power-state polling intervals. Recovery duration can also include HTTP request
time, so attempts multiplied by interval generally is not a wall-clock deadline.

`activation.ac_cycle_attempts` and `activation.ac_cycle_stabilization` apply only
to the `compute_gb200` driver's verified Off/On sequence. Other compute drivers
retain their existing recovery sequence. See the
[activation and recovery design](../docs/architecture/hardware-profile-configuration-layer.md#power-endpoint-overrides)
for that sequence's timing limits. Artifact-download limits remain service
settings outside hardware profiles.

## Configuration traceability

Use `GetFirmwareJobStatus` to inspect a completed per-node job's `result_json`.
The configuration identity is retained on successful, failed, and cancelled jobs:

| Field | Meaning |
| --- | --- |
| `hardware_config_source` | `profile` when an enabled profile matched (for Dell GB200, also its embedded fallback); `code_defaults` otherwise; `unresolved` when the node type could not be resolved. |
| `node_type` | Canonical node type, or `null` when unresolved. |
| `hardware_profile` | Selected profile ID; omitted without a profile. |
| `hardware_profile_sha256` | SHA-256 of the exact YAML bytes loaded at startup; omitted without a profile. |
| `hardware_profile_schema_version` | Selected schema version; omitted without a profile. |
| `rms_build` | Build Git SHA and description, including a dirty marker when available. |

The hash matches `sha256sum <profile.yaml>`. Comments, whitespace, and line
endings affect it. It identifies the source YAML, not all effective settings.
Retain that YAML, service TOML, request inputs, and the RMS release image together.
Build identity can be unavailable in builds without Git metadata.

Batch results record configuration on each child job. Before a job terminates,
the selection is available in logs. This does not add persistent job history or
configuration metadata to direct power/reboot job results.

## Built-in profiles

All seven supported compute-tray types have enabled built-in profiles:

| Node type | Profile |
| --- | --- |
| `compute_gb200_nvidia` | [NVIDIA GB200](v1/compute_gb200_nvidia.yaml) |
| `compute_gb200_wiwynn` | [Wiwynn GB200](v1/compute_gb200_wiwynn.yaml) |
| `compute_gb200_dell` | [Dell GB200 AMI](v1/compute_gb200_dell.yaml) |
| `compute_gb300_nvidia` | [NVIDIA GB300](v1/compute_gb300_nvidia.yaml) |
| `compute_gb300_lenovo` | [Lenovo GB300](v1/compute_gb300_lenovo.yaml) |
| `compute_gb300_supermicro` | [Supermicro GB300](v1/compute_gb300_supermicro.yaml) |
| `compute_vrnvl72_nvidia` | [NVIDIA VR NVL72](v1/compute_vrnvl72_nvidia.yaml) |

Switches and power shelves have no profiles in this catalog. Only NVIDIA GB200
has been exercised on physical hardware with this profile model; the other
profiles have automated parity coverage and still need hardware qualification.
Enabling or changing a profile does not establish that qualification.

Protocol handling, package parsing, cancellation, and preparation/recovery
algorithms remain implemented in Rust. For implementation details, see the
[hardware-profile architecture](../docs/architecture/hardware-profile-configuration-layer.md).
