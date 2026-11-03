# Firmware

See [Operations Overview](overview.md) for the conventions (in-band failures,
registered vs. ephemeral targets) and [the async job model](overview.md#the-async-job-model)
that the asynchronous firmware RPCs below build on.

## Firmware - inventory queries

Synchronous reads of installed firmware.

| RPC | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- |
| `GetNodeFirmwareInventory` | Firmware inventory for one node. | `rack_id`, `node_id` | `firmware_list[]` (name, version, updateable, target, health, type) |
| `BatchGetFirmwareInventory` | Firmware inventory for caller-supplied nodes that do not need to be registered. | `nodes` (`NodeSet` with endpoints and credentials) | Common per-node batch result plus successful `nodes[].firmware_list` entries |
| `CompareFirmwareObject` | Compare a proposed firmware object with installed firmware on caller-supplied nodes without downloading or updating firmware. | `config_json`, `firmware_type`, `nodes`, `component_filters` | Common per-node batch result, object ID, aggregate match flag, and per-component comparisons |
| `GetRackFirmwareInventory` | Firmware inventory for every node in a rack. | `rack_id` | `nodes[]` (each with `firmware_list`) |

`BatchGetFirmwareInventory` is read-only and does not add the supplied nodes to
RMS inventory. The test client accepts the standard direct-operation node CSV:

```console
./rack_manager_client localhost 8801 --insecure \
  batch_get_firmware_inventory nodes.csv
```

Each CSV row supplies the node identity, endpoint, credentials, and node type.
VR NVL72 switch, compute, and power-shelf inventory reads use the BMC endpoint
and its credentials. VR NVL72 switch inventory/comparison requests can omit
`host_endpoint`; RMS does not initialize NVUE for these reads. GB200/GB300 switch
rows continue to use the host/NVUE endpoint. Successful inventories and per-node
failures are both preserved when only part of a batch succeeds.

The VR NVL72 LiteOn PF-1114-1RD exposes PSU inventory at a fixed OEM URI that
Redfish discovery omits. Refer to [Powershelves](#powershelves) for that
inventory and for power-shelf update behavior.

`CompareFirmwareObject` parses and selects components with the same node
type mappings and component filters as firmware-object apply, then compares the
manifest versions with live inventory. It does not persist the manifest,
download artifacts, start jobs, or activate firmware. Version drift is returned
as `FIRMWARE_COMPARISON_STATUS_MISMATCHED` while the RPC and node result remain
successful; connection, credential, node-definition, and inventory-query errors
remain per-node failures. Missing inventory entries and entries without a live
version are reported as `MISSING` and `UNVERIFIED`, respectively. Host-side
Flint components are also `UNVERIFIED` because this read-only RPC currently uses
the node's Redfish/NVUE firmware inventory. A component not exposed by a VR NVL72
switch BMC (for example, NVOS on BMCs that do not report its version) is not
supplemented from NVUE or silently treated as matched.

For nodes, a concrete `NodeInfo.type` takes precedence and the node descriptor
is used as its fallback. Descriptor component filters are similarly resolved to
node types and supplement `component_filters`; an explicit `component_filters`
entry wins when both selectors resolve to the same node type.

```console
./rack_manager_client localhost 8801 --insecure \
  compare_firmware_object nodes.csv manifest.json prod \
  --component-filter 1:BMC
```

The top-level `matches_manifest` flag is true only when every requested node was
queried successfully and every selected component was verified and matched.

## Firmware - asynchronous updates

These start firmware updates and return job IDs. Poll them with
**`GetFirmwareJobStatus`**. A new update is admitted only if the target node is
idle; a second job for the same `(rack_id, node_id)` is rejected as "update in
progress."

| RPC | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- |
| `UpdateFirmware` | Start an async update on one **registered** node: upload each target, poll to completion, optionally activate. | `rack_id`, `node_id`, `firmware_targets[]`, `activate`, `force_update` | `job_id`, `status` |
| `BatchUpdateFirmwareByNodeType` | Apply the same target(s) to all nodes selected by `node_type` or `node_descriptor` in a registered rack. A concrete `node_type` takes precedence. Each node gets a child job under a **parent** job. | `rack_id`, `node_type` or `node_descriptor`, targets, `activate`, `force_update` | `NodeBatchResponse` (`job_id` = parent), `jobs[]` (node → child job) |
| `BatchUpdateFirmware` | Apply firmware to a caller-supplied **ephemeral** node list using a per-node-type target map. | `nodes`, `firmware_targets` (map), `activate`, `force_update` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `GetFirmwareJobStatus` | **Poller** for all firmware-update and firmware-object-apply jobs (leaf or parent). Parent jobs report aggregated child state. | `job_id` | `job_state`, `state_description`, `error_code`, `result_json`, timestamps |

For `BatchUpdateFirmwareByNodeType`, platforms without a `NodeType` enum value
use `node_descriptor` with `node_type` set to `NODE_TYPE_UNSPECIFIED`. These
platforms include Wiwynn and Dell GB200, Supermicro GB300, and VR NVL72 LiteOn
and Delta power shelves. Both selectors support a single `filename` and `target`
or a `firmware_targets` list. A nonempty `firmware_targets` list takes precedence
over `filename` and `target`.

When NVFWUPD reports a failed task with `Critical` status solely because the
component image is identical, RMS marks that target `skipped` if neither the
request nor the target profile enables `ForceUpdate`. RMS confirms the rejection
with a second task status read; if that read fails, the target remains failed.
If the second read is nonterminal or no longer confirms an identical-image-only
rejection, RMS does not skip.
The target's `reason` in `GetFirmwareJobStatus.result_json` contains the device message. If
every target is skipped, the job completes with `status: "already_up_to_date"`
and no activation. When either setting enables force update, the same task
rejection remains a failed job. Other task failures remain failures regardless
of force. Cancellation prevents a skip while preserving other terminal device
failure reasons.
RMS also keeps the job failed when the same task reports another component was
updated or awaits activation, even if its failure message describes an identical image.
Task start and abort events and the generic task completion error do not count
as component failures; an unclassified non-OK message still prevents the skip.

For NVFWUPD-backed updates, a node may select a configured expected hardware
inventory with `NodeDescriptor.attributes["inventory_profile"]`. RMS validates
the profile before admitting a child job and asks NVFWUPD to recheck the AP
inventory before every target update and retry. A mismatch fails with
failed-precondition details listing missing and present APs. Firmware version
verification does not perform this inventory check.

## Firmware Update Behavior

Firmware updates behave differently by platform. This section collects the
platform-specific behavior that applies on top of the RPCs described above.

### BMC HTTP client transport

NVFWUPD BMC and NVUE HTTP clients disable reqwest's default 30-second Linux
`TCP_USER_TIMEOUT` to avoid premature firmware-upload failures. This applies
to all requests through those clients, not just uploads. HTTP deadlines are
unchanged; an unresponsive peer can take longer to fail, up to the request
deadline, while normal kernel TCP failure detection still applies.

### Compute Trays

#### GB200 in-band ConnectX updates

NVIDIA and Wiwynn GB200 compute nodes can update CX7, CX8, and BF3 NIC firmware
through the host OS with Flint. The node registration must provide host SSH
credentials and BMC credentials.

Before device discovery or flashing, RMS makes the host boot requirement
explicit. It powers on a host that is not already on and waits up to six minutes
for SSH and the OS service manager to become ready. If the OS does not boot, RMS
performs one BMC auxiliary AC cycle, powers the host on, and repeats the
six-minute boot wait once. Authentication and host-key failures fail immediately
because a power cycle cannot correct them.

If MST enumeration or a per-device Flint query fails before image staging or
flashing begins, RMS performs one auxiliary AC cycle, boots the host again, and
repeats the complete device discovery once. A second discovery failure ends the
job; RMS never uses this recovery path after a Flint flash has started.

When the node's expected-inventory profile contains `flint_devices` counts, RMS
also compares them with the physical CX7, CX8, and BF3 NICs reported by MST.
Counts are per family and are not assumed to be four. A mismatch uses the same
single pre-flash recovery cycle and must match on the second discovery. RMS
repeats the count check during post-activation version verification.

After a successful flash, RMS follows the GB200 compute activation sequence: it
forces the host off, verifies `PowerState=Off`, performs the auxiliary AC cycle,
waits for stabilization and BMC readiness, powers the host on, and verifies
`PowerState=On`. The bundled profile permits five minutes for each power-state
check (`driver.timing.recovery.power_on_attempts: 60` at `power_on_interval: 5s`).
Both checks use these profile settings; NVFWUPD callers without a timing policy
retain a one-minute window. `driver.timing.activation.ac_cycle_stabilization`
defaults to `3m`, and `ac_cycle_attempts` defaults to `2` total attempts. If BMC
readiness or power-on times out, RMS repeats the AC-cycle and recovery steps
until that attempt budget is exhausted.

For a GB200 compute node with a configured host endpoint, activation is not
complete until the host OS is reachable and ready again. RMS waits up to six
minutes; if the OS does not boot, it performs one additional auxiliary AC cycle,
waits three minutes, powers the host on, and repeats the six-minute OS readiness
check once. Post-activation Flint version verification begins only after this
boot check succeeds.

#### VRNVL72 in-band LinkX transceiver updates

NVIDIA VRNVL72 compute trays support the opt-in `LINKX` component through
`ApplyFirmwareObject` and `ApplyStoredFirmwareObject`. This is **OSFP module
firmware**, not CX9 NIC firmware. Both workflows live in NVFWUPD's `flint` module
and share the Flint/SSH transport. LinkX has its own connector health checks and
immediate module activation, separate from the ConnectX/BlueField NIC flash
sequence. No protobuf fields or new RPCs are required.

The LinkX workflow is available through the NVFWUPD library, not its CLI.
Existing `nvfwupd` CLI commands retain their behavior; LinkX-specific safety
checks do not change the shared NIC workflow.

Hardware profiles continue to configure the OOB targets and their activation
boundaries. `LINKX` remains opt-in and runs after all selected OOB profile stages;
it is not a Redfish profile target and never adds a tray power cycle afterward.

The initial RMS target selects all four physical connectors, C1/C3/C5/C7, using
the VR CX9 wiring layout below. Do not use it on a different MST/interface layout.
The lower-level `nvfwupd::flint::update_linkx_firmware` API also accepts explicit
connector subsets.

| Connector | Programming device suffix | Module query device suffixes | Host interfaces |
| --- | --- | --- | --- |
| C1 | `mt4133_pciconf1` | `mt4133_pciconf0.3`, `mt4133_pciconf1.3` | `eth_r0_p0`–`p3`, `eth_r1_p0`–`p3` |
| C3 | `mt4133_pciconf3` | `mt4133_pciconf2.3`, `mt4133_pciconf3.3` | `eth_r1_p4`–`p7`, `eth_r0_p4`–`p7` |
| C5 | `mt4133_pciconf7` | `mt4133_pciconf6.3`, `mt4133_pciconf7.3` | `eth_r2_p0`–`p3`, `eth_r3_p0`–`p3` |
| C7 | `mt4133_pciconf5` | `mt4133_pciconf4.3`, `mt4133_pciconf5.3` | `eth_r3_p4`–`p7`, `eth_r2_p4`–`p7` |

All device paths have the `/dev/mst/` prefix. The `.3` paths are intentional
module query functions; they must not be removed by NIC VF filtering.

Provide a host endpoint with password-based SSH credentials. SSH uses TOFU with
RMS's persistent `$HOME/.ssh/known_hosts` file; changed host keys fail validation.
The host must have `mst`, `flint`, `mlxlink`, `sha256sum`, and sufficient root/sudo
access installed. Before starting MST, LinkX waits up to six minutes for SSH and
OS readiness, including when an earlier OOB stage has just powered the tray on.
Authentication failures fail immediately; cancellation stops the wait before
tool preparation or flashing. LinkX does not install tools, power on the host,
or recover this workflow with another tray AC cycle. The normal compute node
BMC endpoint is still required for node construction and any OOB stage.

Add a standalone `LINKX` entry at
`Milestones[0].BoardSKUs[].Components.Firmware[]`, under the board whose `Type`
is `Compute Tray`. Place it alongside HMC, BMC, and CX9, not inside CX9's
`SubComponents` or the `Software` list. The versions below are examples,
**not runtime defaults**. Set the approved starting version, target version,
and SHA-256 for your exact rollout image:

```json
{
  "Component": "LINKX",
  "Type": "Prod",
  "Version": "44.40.2035",
  "LinkxRollout": {
    "current_version": "44.40.2019",
    "target_version": "44.40.2035",
    "sha256": "REPLACE_WITH_THE_IMAGE_SHA256_64_HEX_DIGITS"
  },
  "Locations": [{
    "Location": "https://firmware.example.com/linkx.bin",
    "LocationType": "HTTPS",
    "Sha256": "REPLACE_WITH_THE_IMAGE_SHA256_64_HEX_DIGITS",
    "Type": "Binary"
  }]
}
```

`Version` must equal `LinkxRollout.target_version`. `Type` selects the artifact
channel, so `Prod` matches a request with `firmware_type="prod"`. Replace the
example `Location` with the actual approved image URL or supported local path;
RMS does not infer a download source from the component name.
Set both checksum fields to the same image SHA-256: `Locations[].Sha256`
protects the RMS download, while `LinkxRollout.sha256` protects SSH staging.

Select `LINKX` in the VR compute descriptor component filter. It is excluded
from unfiltered default compute updates. Exactly one eligible `.bin` image is
required; metadata/documentation files are ignored and ambiguous images are
rejected. Direct `UpdateFirmware`/batch filename-only requests cannot supply
the rollout policy and are rejected for this component. Set `force_update=false`;
forced reflashes and emergency health-check bypasses are not supported.

NVFWUPD checks the image hash locally and after SSH staging, then uses
`flint -i IMAGE q` to require `Image type: LINKX` and the approved target version.
It preflights every selected connector before flashing any connector. Connectors
already reporting the target on **both** module paths are skipped. Others must
report an approved current A/B firmware slot, an IDLE component update state,
two readable module versions, Ready module state, zero module/datapath firmware
faults, and all eight associated interfaces up.
An inspection command failure or timeout fails preflight before any connector
is flashed, including when readable module versions already match the target.

Connectors flash sequentially in C1/C3/C5/C7 order, using:

```console
flint -d DEVICE --linkx --downstream_device_id_start_index 1 \
  --num_of_downstream_devices 1 --download_transfer --activate -i IMAGE b
```

The command activates the module immediately. NVFWUPD allows up to 20 minutes
for the command and up to five minutes afterward for both module paths to report
the target version, healthy module state, and all eight links up. A failure stops
remaining connectors without retrying the flash. Temporary staging files are
cleaned on exit. Cancellation is observed between phases/connectors, never by
interrupting an active flash. If flashing has started, partial-update evidence
is retained instead of returning a bare cancellation result.
Inspection evidence includes `query_errors` with the command/device/interface
path and sanitized cause. If the recovery deadline expires, observations from
completed queries are retained and `inspection_timed_out` is set. Those values
can include an earlier probe's observations; they do not establish successful
recovery. A timed-out or failed inspection cannot pass the postcheck.

RMS does **not** perform a tray power cycle after LinkX. If a firmware object
explicitly selects OOB components too, RMS runs the OOB stage and its activation
first, then LinkX within the same per-node job. Job results distinguish firmware
version state from link recovery; a module can be at the target version while
the job fails its link-health postcheck. `CompareFirmwareObject` currently marks
LinkX `UNVERIFIED`, because that RPC reads BMC/NVUE inventory, not host module
inventory. The LinkX update itself performs dual-path post-update verification.

#### Dell GB200 AMI sequencing

Use the node descriptor `role=compute`, `vendor=dell`, `product_family=gb200`.
This selects `compute_gb200_dell`, for the AMI host BMC and NVIDIA HMC.
The implementation follows Dell's AMI compute update sequence; iDRAC DUP
updates are a separate protocol and are not handled by this node type.

The embedded [Dell profile](../../hardware-profiles/v1/compute_gb200_dell.yaml)
selects these manifest components and sends one multipart request per package:

| Stage | Component | Manifest source | File selection | UpdateParameters | OemParameters |
| --- | --- | --- | --- | --- | --- |
| 10 | `BMC` | `BMC` | `.ima` | `{"Targets":[]}` | `{"ImageType":"BMC"}` |
| 20 | `HMC` | `HMC` or `HMC_NOSBIOS` | `*nosbios*.fwpkg` | `{"Targets":[]}` | `{"ImageType":"PLDM","Platform":"HGX"}` |
| 20 | `BIOS` | `BIOS` or `SBIOS` | `.fwpkg` | Both `HGX_FW_CPU_0` and `HGX_FW_CPU_1` inventory paths in one `Targets` list | `{"ImageType":"PLDM","Platform":"HGX"}` |

With `activate=true`, RMS uploads all selected BMC packages, performs the full
compute power cycle, waits for readiness, and verifies the installed BMC version.
It then uploads all selected HMC packages followed by SBIOS, performs a second
power cycle, and verifies the package versions. Every eligible manifest location
is preserved in manifest order within its component. An omitted component is not
flashed. A failed upload, failed task, failed activation, version mismatch, or
cancellation stops subsequent stages.

Upload URI discovery uses `UpdateService.MultipartHttpPushUri`; the captured AMI
BMC advertises `/redfish/v1/UpdateService/upload`. The default requests omit
the multipart `ForceUpdate` parameter. For every upload, RMS sets
`UpdateService.HttpPushUriOptions.ForceUpdate` to the requested force value and
verifies it before sending firmware. This enables forced HMC downgrades as well
as host-BMC reflashes. Without force, RMS temporarily disables a previously
enabled setting. An explicit boolean `ForceUpdate` in configured update
parameters takes precedence over the RPC flag; the configured multipart JSON
still passes through unchanged.

RMS retains this setting while the device task runs and restores its original
value after a terminal success or failure, or when preparation/upload fails.
The original value is retained when restoration fails, and RMS retries the
restore before the next upload on that node. After a successful task, a
restoration failure is logged as a warning and does not fail the update, so
activation still runs. After a failed or cancelled task, the restoration failure
is added to the reported error. A second upload on the same node instance waits for the current upload call and
then fails if its device task is still active. If cancellation or lost contact
stops polling before task completion, RMS reports pending cleanup and leaves the
setting unchanged. Poll the task again or retry on the same registered node after
it finishes to complete cleanup. If the BMC no longer returns the task (for
example after it restarts), RMS treats the task as terminal after at least three
consecutive failed status reads during cleanup that span at least 15 minutes.
Quick retries while the BMC reboots therefore keep the setting. RMS then
restores the original value and clears the pending state, so the next update on
that node proceeds. A
task that can be read and is still running is never treated this way. After an
RMS restart or an ephemeral-node job ends with pending cleanup, check the setting
and restore the reported original value manually once the device task is
terminal. RMS cannot restore settings after a process crash.

Before a raw host-BMC upload, RMS enables `Authentication`, `IPMI`, `Network`, and
`REDFISH` under `UpdateService.Oem.AMIUpdateService.PreserveConfiguration`.
AMI links IPMI and network preservation, so RMS enables and verifies both settings
together. Other preservation settings retain their existing values. RMS reads the settings
back before uploading; missing settings, a failed PATCH, or a read-back mismatch
stop the update before any image is sent. Preparation and upload share the node's
operation lock. A BMC image already at the requested version skips both steps.
This prevents an update from discarding the access settings RMS needs for
activation and verification. It does not restore accounts lost during an earlier
update; recover access before retrying with the corrected RMS build.

Raw BMC images require the manifest component's `Version` or the documented
`BmcFwV` filename convention (also used for direct file updates):
`ExampleBmcFwV250729001500.ima` maps to `25.07.29001500`. Conflicting manifest and
filename versions are rejected. RMS compares against
`/redfish/v1/UpdateService/FirmwareInventory/BMC`, or an explicit inventory path
selected by an override profile. Explicit firmware-inventory targets, including
both SBIOS CPU paths, use a supplied manifest `Version` for comparison. Use the
firmware-inventory representation (`216` in the capture), not the computer
system's `BiosVersion` (`2.16`). Package-wide updates and targets without a
manifest version use embedded package versions. An opaque package without either
source of comparable version evidence cannot pass activation verification. Task failures remain
failures; RMS does not reinterpret error messages as success.

The node type always uses its embedded profile when no enabled external override
exists. Operators may change its stages, target paths, selection, and JSON
parameters through the existing profile system, and are responsible for the
resulting hardware behavior. `activate=false` leaves activation to the operator.
NIC/BlueField in-band updates and OOB recovery are not implemented for this type.

Inventory and request handling are tested against sanitized AMI capture data.
A real BMC → HMC/SBIOS update and both activation cycles still require hardware
qualification; the read-only capture contains no firmware-update tasks.

#### Supermicro GB300 sequencing

The default profile for the descriptor-only Supermicro GB300 compute type
selects firmware-object artifacts by filename from the manifest:
the HMC `*nosbios*.fwpkg`, the `BIOS*.bin`, and the host-BMC `*OBMC*.bin`.
RMS applies them in that order. With activation enabled it performs a full
compute power cycle after nosbios and after BIOS; the host-BMC update runs last
and does not add another host AC cycle. Profile-backed requests honor the
configured stages and the request's activation flag; operators are responsible
for changes to these defaults. Calls without an enabled profile retain the
legacy sequencing checks.

The profile declares multipart `Targets` for HMC, BIOS, and BMC. BIOS uses one
upload containing both CPU inventory paths. Lenovo profiles similarly declare
`Targets` and `oem_parameters`; set BMC `ImageType` to `HPM` explicitly for `.hpm`
images. The Lenovo preparation PATCH and readback remain in Rust. See
[multipart parameters](../../hardware-profiles/README.md#multipart-parameters)
for JSON and YAML examples.

### Switches

For GB200, GB300, and VR NVL72 switches, RMS compares installed firmware with
the versions extracted by NVFWUPD from the requested `.fwpkg` payloads before
starting an update. This applies to direct firmware updates and firmware-object
apply. Component filters and explicit targets limit the comparison; a whole
package compares only the components it contains. An EROT target includes every
reported ERoT member, and a CPLD target includes the platform's split CPLD
inventory entries.

For VR NVL72 switches only, inventory comes directly from BMC Redfish to avoid a
potential BMC/NVOS communication issue. BMC `MGX_FW_*`/`FW_*` identities are
matched to the logical package components without merging individual ERoTs.
Combined BMC CPLD versions are compared as complete ordered revision lists.

VR NVL72 switch firmware updates require both `host_endpoint` credentials for the
existing NVUE/SSH upload path and `bmc_endpoint` credentials for inventory and
activation. Firmware activation issues `ComputerSystem.Reset` with `ResetType: PowerCycle`
through the BMC, waits 60 seconds for restart, then makes at most 45 BMC recovery
probes, 15 seconds apart. Recovery requires `System_0` to report `On` and a complete
firmware inventory read; each Redfish request also has a bounded transport timeout.
Post-reset HTTP 401/403 responses are retried within this recovery budget. If the
final probe fails authentication, the job reports that authentication error.
Authentication failures on the reset request itself still fail immediately.
Version verification follows activation. After the versions match, VR NVL72
switch jobs first wait for NVUE to answer at all (up to 45 probes, 15 seconds
apart, 5 seconds each), because the BMC-driven activation and version check can
finish while NVOS still boots; every error except a missing NVUE endpoint is
retried during that wait. GB200 and GB300 skip it, because their version check
reads firmware inventory through NVUE, so NVUE has answered by then. Switch
jobs then poll `GET /nvue_v1/system` until NVOS
reports `status: System is ready` and `health: OK`, on the same attempt count
and interval as the version check. `System is not ready (failed)` fails the job
at once. Any other recognized status or health (`N/A`, `Not OK`, `FATAL`) keeps
polling until the budget runs out, and the job then fails with the reported
status and health. A health word outside the recognized set is a decode error,
and the poll retries it like any other error until the budget runs out.
Cancellation stops further recovery
probes. A failed reset or exhausted recovery is a job failure, not a second reset.
There is no NVUE power-cycle or inventory fallback when BMC details are missing
or BMC access fails. GB200/GB300 switches retain NVUE inventory, comparison, and
activation, including their existing BMC auxiliary power-cycle recovery path.
NVOS system-image installation remains a separate NVUE/SSH workflow.

For split GB200/GB300 switch manifests, `SBIOS+EROT` (package `0006`)
supplies the EROT payload and expected version in preference to `BMC+FPGA+EROT`
(`0004`). The BMC and FPGA still use `0004`. This choice uses manifest component
identity, is independent of entry order and version ordering, and stays within
the selected artifact channel. If no eligible `SBIOS+EROT` payload is supplied
for that channel, EROT falls back to `BMC+FPGA+EROT`. EROT-only applies download
only the selected bundle. Previously cached firmware-object mappings are
normalized when read so their apply and metadata paths follow the same rule.
This normalization replaces only `BMC+FPGA+EROT` fallback entries; other sources,
including VR NVL72 bundles in mixed catalogs, are preserved.
Explicit direct-update targets continue to use the caller-selected payload.

When every selected package component is verified and matches, the job succeeds
with `status: "already_up_to_date"`, all targets marked skipped, and
`activated: false`. RMS does not upload, flash, or power-cycle the switch. The
job's `pre_update_version_check` records the comparison. Any mismatched, missing,
or unverified component prevents this early exit and the normal update proceeds.
Package-parse and inventory-read errors fail the job before flashing.

The preflight completeness requirement does not change post-activation
verification. For whole-package updates, an AP absent from inventory prevents an
early skip but does not introduce a post-activation version mismatch. Reported
version mismatches and missing explicitly selected targets still fail verification.

`force_update: true` bypasses this preflight. Requests containing raw payloads
such as `.vme` or `.bin`, which lack PLDM package metadata, retain their existing
update behavior. NVOS system-image updates have a separate current/next-build
check described in [Switch Management](switch-management.md).

After NVUE accepts a switch power cycle, RMS reads the action status and then
waits for the switch to recover. A rejected request or an explicit action
failure fails activation before that wait. Failed action-status reads are not
themselves an outage. RMS treats that retry window as an outage only when a
read shows the switch stopped answering, or a non-200 answer reports a reboot in
its state or status text. A readable status is not read for that text, since a
reboot it merely announces is not evidence the switch went down. A later
readable status does not retract an outage already observed, because the switch
can stop answering and return before its status becomes readable. That carries
over only once the action reached a terminal state. A status still reporting the
action as running has yet to take the switch down, so recovery has to watch that
outage happen rather than reuse an earlier one. Recovery then succeeds when the
firmware endpoint is reachable, including when it is already back at the first
poll. Otherwise, including a readable non-failed status that no outage preceded
and one that is still running, recovery waits for that endpoint to go
unavailable and return. If that endpoint stays reachable for the whole wait,
the outcome depends on whether the action status ever became readable. A
status that stayed unreadable means the switch answered throughout, so its
action failed: activation fails and RMS does not run a BMC auxiliary AC cycle.
A readable status instead leaves a reboot outstanding and still times out. A
switch that goes unavailable and does not return also times out. GB200 and
GB300 still use the existing auxiliary AC-cycle fallback for those timeouts.

### Powershelves

Power shelves have two firmware component types: PMC and PSU. The rules in this
subsection are specific to the VR NVL72 LiteOn PF-1114-1RD unless a paragraph
names another model.

#### VR NVL72 LiteOn PF-1114-1RD inventory and update routing

The shelf exposes PSU inventory at the fixed OEM URI
`/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits`, including members
`powerdevice1` through `powerdevice6`, even though Redfish discovery omits the
links to this tree. Standard DMTF `PowerSubsystem/PowerSupplies` resources also
exist. NVFWUPD reads the OEM inventory directly.

Direct firmware updates through the OEM per-PSU action do not take effect on
this model. RMS supports both `LiteOnPMC` and `LiteOnPSU` firmware apply through
`UpdateService.HttpPushUri`, with PMC before PSU when both are selected. A PSU
package updates all PSUs, and explicit per-slot OEM requests are rejected.
Failed uploads or all-PSU tasks do not trigger OEM per-PSU fallback on this
model. Task completion is followed by PSU version validation through the
readable OEM inventory.

#### Shelf model detection

Model detection uses the shelf chassis, excluding manager and individual PSU
chassis. A LiteOn PSU update requires exactly one `powershelf*` chassis with a
populated `Model`. Zero or several `powershelf*` chassis, or an empty `Model`,
fail the request before any upload.

A failing chassis read is attempted at most three times in total (two retries),
three seconds apart, before the request fails. The three-second delay applies to
production builds only; tests use a shorter one.

#### All-PSU update deadline and outage budget

For VR LiteOn all-PSU updates, one fixed **one-hour deadline** includes the
initial PSU upload, task polling, PMC outages, validation, and every re-push.
It is not restarted when a new task appears or the PMC returns. This deadline
applies to the PSU stage, not an earlier PMC firmware stage or subsequent
activation. The legacy per-PSU serial monitor of other LiteOn models keeps its
own 1800-second timeout.

If the PMC becomes unavailable, recovery allows **30 minutes per outage**
within that hour. A restarting PMC rejects valid credentials with HTTP 401 for
about 13 minutes before it recovers. Transport failures, timeouts, HTTP 401,
403, 408, and 429, and HTTP 5xx during task, inventory, or UpdateService reads
all consume the same outage budget. A complete successful step clears that
budget; repeated failed reads do not extend it. An exhausted outage budget
fails the update even if time remains in the hour.

#### All-PSU task loss and re-push

When the tracked task disappears, RMS confirms that no other PMC task is still
active. The PMC keeps finished tasks (for example the earlier PMC update) until
it restarts, so a listed `Completed`, `Exception`, `Killed`, or `Cancelled` task
does not block recovery. A task in any other or unreadable state, and a task
list that cannot be read in full, does.

RMS then reads all six PSU versions, health, and update progress:

- If all six are at the target and idle, the update completes without another
  upload.
- If versions remain incomplete but all six PSUs are healthy and idle, RMS waits
  for that state to persist for a **five-minute settling period**. During the
  period there must be no outage, active PSU, active PMC task, or change in any
  PSU version between reads, because a PMC that just restarted can report idle
  PSUs before it re-reads them.
- After the settling period, RMS re-pushes the same package through the
  advertised `HttpPushUri`, rechecking for active tasks immediately before
  dispatch.

There is **no attempt-count or stalled-progress limit** within the hour. Active
PSUs or active PMC tasks prevent a re-push; polling continues within the
original deadline, and a deadline failure names the task that was still active.

A PSU whose version, state, status, or progress cannot be read yet is not
settled: RMS keeps polling and consumes the outage budget, and never re-pushes
meanwhile. The following conditions stop recovery and never authorize a
re-push:

- Malformed or incomplete inventory (not exactly six PSUs).
- A PSU in a failed state.
- Other unexpected HTTP statuses.
- An ambiguous upload result. An uncertain POST result stops recovery to avoid
  duplicating an upload.

No OEM per-slot update is used.

#### All-PSU polling, cancellation, and state

Each status poll advances one step, bounded by 60 seconds and the remaining
job and outage deadlines. A re-push upload runs after that step's reads and gets
its own 60 seconds, still capped by the job deadline. The original task handle
retains the replacement task ID and cumulative re-push count, including terminal
outcomes. RMS retains control of polling and cancellation.

Recovery state is process-local and is not resumed after an RMS restart.
Starting a new update on a shelf discards its retained recovery entries, so a
reused PMC task ID cannot match an earlier update.

#### PMC firmware stage restart

The VR LiteOn PMC firmware stage restarts the PMC to activate its image. The
Redfish task disappears during that restart (HTTP 404 afterwards), and
`UpdateService` rejects valid credentials until the PMC is back.

When the manifest provides the PMC `Version` and task-status requests keep
failing past the transient-error budget, RMS checks the firmware inventory
(`FW_PMC_*`). Recovery gets the same amount of time as the transient-error
budget, but must finish before the original firmware task timeout. RMS completes
the stage only when the installed version matches the manifest version. As with
`CompareFirmwareObject`, the comparison ignores surrounding whitespace and ASCII
case.

RMS starts no inventory read after the recovery budget expires. If time runs out
between reads, the failure reports the last version mismatch, missing PMC entry,
or inventory read error. If a read reaches the deadline, the failure reports an
inventory read timeout, even when a matching version arrives at the deadline.
Recovery with no time left fails without reading inventory. `CancelJob` stops
the read and prevents a matching response from completing the stage.

Only versioned PMC targets on VR LiteOn shelves use this recovery behavior.
Other shelf models and PSU targets do not use it.

#### PMC uploads with OnReset

Power-shelf PMC uploads using `OnReset` retain the configured upload timeout.
This timeout includes streaming the entire package as well as awaiting the HTTP
response; it is not capped at 60 seconds. If an upload response is lost, RMS
still checks for a newly created `OnReset` handoff task before reporting
failure.

Legacy LiteOn CM-14MP-1R/X6 PMCs can stage a package and then crash `bmcweb`
before returning a task. For `OnReset` PMC uploads, NVFWUPD captures a bounded
Redfish manager/inventory baseline. If the upload returns no task and task
reconciliation finds no handoff, it reads inventory again and includes the
before/after manager versions, active-image links, and changed image records
in the failure details. Reads use same-origin links and require complete
inventory membership; unavailable reads remain unknown rather than empty.
This reconciliation uses Redfish only and never replays the upload or resets
the PMC. A manager version of `deadbeef`, an image marked `Enabled`, and even
a newly appearing target version do not prove that activation is ready.
Without an attributed handoff task, the update remains failed with activation
readiness unproven, and later PSU stages do not run. Recovery requires further
readiness evidence or an explicitly requested operator activation procedure.

#### Manifest component names

Powershelf component names accept interchangeable ASCII spaces and hyphens:
`LiteOn PMC` / `LiteOn-PMC`, `LiteOn PSU` / `LiteOn-PSU`, and
`Delta-PMC` / `Delta PMC`, `Delta-PSU` / `Delta PSU`. Capitalization and
other characters remain significant; no typo guessing or vendor substitution
is performed. Prefer one entry per component/channel with its payloads listed
under `Locations`; alternate spellings do not deduplicate repeated payloads.

Vendor-specific powershelf components under compute boards remain supported;
platform/vendor filtering still applies.

The VR NVL72 Delta power shelf (descriptor `role=power_shelf`, `vendor=delta`,
`product_family=vrnvl72`) accepts only the `DeltaPMC` and `DeltaPSU` components
and currently uses the same Delta firmware workflow as GB200 and GB300. Its
manifest target carries no `expected_version`. This path is not yet validated
on VR NVL72 hardware. In `rack_manager_client` target CSVs and
`--component-filter` values, name it `powershelf_vrnvl72_delta`; the client
sends descriptor-keyed selectors.

## Firmware objects

A **firmware object** is a catalog entry built from a firmware manifest JSON: a
software object tree plus RMS-parsed component and artifact metadata, scoped to a
`hardware_type`, optionally marked default, and only `available` once RMS has
downloaded all required artifacts. See the
[sample manifest](https://github.com/dsx-ai-factory/nv-rms/tree/main/sample_firmware_manifest).

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `AddFirmwareObject` | Sync (background download) | Validate + persist a catalog entry from `config_json` and kick off an artifact download. Poll readiness via the object's `available` flag. | `config_json`, `hardware_type`, `access_token?`, `set_default` | `object` |
| `GetFirmwareObject` | Sync | Retrieve one entry by ID. | `id` | `object` |
| `ListFirmwareObjects` | Sync | List entries, optionally filtered. | `only_available?`, `hardware_type?` | `objects[]` |
| `DeleteFirmwareObject` | Sync | Remove an entry, its on-disk cache, and any in-flight download. | `id` | `OperationResponse` |
| `SetDefaultFirmwareObject` | Sync | Mark one object default for its hardware type. | `object_id` | `object` |
| `ApplyStoredFirmwareObject` | Async → `GetFirmwareJobStatus` | Resolve targets from an already-stored, **available** object and start update jobs. Records apply history on success. | `rack_id`, `object_id?`, `hardware_type`, `firmware_type?`, `nodes`, `component_filters`, `force_update` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `ApplyFirmwareObject` | Async → `GetFirmwareJobStatus` | Ingest an **ephemeral** object from inline `config_json`, download, resolve, and start updates - nothing persisted. | `rack_id`, `config_json`, `access_token?`, `hardware_type`, `firmware_type?`, `nodes`, `component_filters`, `force_update` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `ApplyStoredSwitchSystemImage` | Async → `GetSwitchSystemImageJobStatus` | Resolve a switch **system image** (NVOS) from a stored, available object and start switch image-update jobs. Records history. | `rack_id`, `object_id?`, `hardware_type`, `software_type?`, `nodes`, `allow_onie_recovery`; one `nodes.additional_host_endpoints` entry per node when recovery is enabled | `NodeBatchResponse` (parent `job_id`), `image_filename`, `jobs[]` |
| `ApplySwitchSystemImage` | Async → `GetSwitchSystemImageJobStatus` | Ephemeral inline-`config_json` variant of the above. | `rack_id`, `config_json`, `access_token?`, `software_type?`, `hardware_type`, `nodes`, `allow_onie_recovery`; one `nodes.additional_host_endpoints` entry per node when recovery is enabled | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `GetFirmwareObjectHistory` | Sync | List prior apply events recorded by the `ApplyStored*` RPCs. | `object_id?`, `rack_ids?` | `records[]` |

For switch system-image apply RPCs, `host_endpoint` remains the NVOS endpoint.
If omitted, `allow_onie_recovery` is `false`. When it is `true`, each node must
provide exactly one `additional_host_endpoints` entry with the ONIE SSH address
and username/password credentials. Both apply paths carry this endpoint into
the shared switch-image workflow. RMS first probes the normal host endpoint for
NVOS. If NVOS is absent, RMS requires a valid host endpoint MAC and checks it
against ONIE's management MAC, plus the additional endpoint MAC when supplied.
Only a matching ONIE endpoint receives the installer through SCP; RMS then
starts `onie-nos-install` and waits for the normal NVOS SSH endpoint to return
before initializing NVUE.

### Manifest parsing

RMS reads the sanitized `ProductName` → `Milestones` → `BoardSKUs` JSON into
`domain::firmware_manifest::FirmwareManifest`. A firmware object requires exactly
one milestone. Software and firmware use the same typed component structure,
but remain separate lists under their original board. Board names, optional
`SKUID`, filenames, artifact kinds, channels, and subcomponent versions are
preserved without inferring an RMS node type.

- The request's `firmware_type` (or NVOS `software_type`) may be omitted or blank.
  RMS uses the sole component-level `Type` across the manifest's firmware and
  software, ignoring case and surrounding whitespace. A Prod-only manifest
  defaults to Prod; a Dev-only manifest defaults to Dev. Multiple types require
  an explicit request; no types means RMS cannot infer a default. Component
  filters do not change this rule, and `Locations[].Type` is an artifact kind,
  not a channel. Explicit requests still select only the requested type.
  Comparison, inline apply, and stored apply share this rule. Successful stored-apply history
  records the resolved type. The test CLI retains its positional type argument;
  pass `""` in that position to request inference.
- Null download locations and empty versions are valid metadata. They do not
  make an artifact ready to apply. Prefetch skips null/blank URLs and collects
  all downloadable NVOS images without requiring a unique selection. A present
  URL still requires valid transport metadata; malformed downloads are not
  silently skipped. Required payload locations and channels are
  checked by the platform-specific apply planner. An applicable component with
  an absent or blank `Type` fails planning before channel filtering or download;
  it cannot silently disappear from a default apply. Explicitly unselected
  components do not block an apply. The existing VR-NVL72 `Prod` default remains.
- Missing optional lists are empty; present lists must be arrays. Incorrect
  field types and malformed `Sha256` values fail with their JSON field path,
  including in components that are not selected for an update. A checksum must
  be null, absent, or a 64-character hex string; an empty string is invalid.
- Informational extension fields are accepted. Duplicate known JSON fields are
  rejected at ingestion. Top-level legacy `Id`/`BoardSKUs` documents are not accepted.
- NVOS selection uses `FileName`, falling back to the URL basename when absent.
  Applying requires exactly one `.bin` candidate, with a real location, per
  channel. Multiple candidates or multiple switch boards offering the same
  channel fail explicitly; RMS does not rank filenames or choose the first board.
  `FileName` may differ from the URL basename.
- NVOS metadata describes the manifest's advertised images independently of
  apply validation. An image with a null URL remains visible. Each image keeps
  its own `PackageName` and `Required` policy (`true` when unspecified), even
  though explicitly applying it always requires the image bytes. Metadata
  visibility is not evidence of readiness or unambiguous apply selection.

Parsing is separate from payload eligibility, platform-specific target mapping,
download/cache management, and execution. New stored and inline operations use
the shared planner and revision-scoped artifact identities; target mappings are
resolved for each requested node type rather than persisted in a role-keyed
table. RMS does not read objects prepared before versioned artifact catalogs:
applying one fails with instructions to delete and recreate it, and listing it
returns no metadata. They are not automatically migrated.

To recreate an object without an artifact catalog:

1. Use `GetFirmwareObject` to save its `config_json`, `hardware_type`, and
   `is_default` values before deletion.
1. Call `DeleteFirmwareObject` with the object's ID. This removes the stored
   object and its cached artifacts.
1. Call `AddFirmwareObject` with the saved `config_json` and `hardware_type`.
   Set `set_default` to the saved `is_default` value and provide `access_token`
   if the artifact source requires authentication.
1. Poll `GetFirmwareObject` until `available` is `true` before applying it.

Adding the same manifest while the old object exists fails with `ALREADY_EXISTS`.

For every firmware target and node type, RMS accepts all eligible payload
locations in a component. It retains manifest location order within each target,
while preserving the platform's component flash order and activation stages.
Locations are update payloads, not alternative download mirrors. Only list the
files intended for the update; in-band updates still select images by device
family and PSID. Component/channel filters, vendor-specific payload validation,
metadata-only locations, and non-firmware artifact filtering still apply. A
selected component with no eligible payload fails explicitly.
NVOS software-image selection still requires one image.

Recovery packages are the one exception to the all-payload policy. They are
prefetched with the payloads but never enter a flash plan. A recovery package is
a `.fwpkg` location typed `Firmware-Recovery`, or one whose name carries
`_recovery`. The compute OOB recovery flow pushes them. An apply, from a stored
object or an inline manifest, takes them from its own manifest, materialized
with the payloads under the same digest policy. A raw `update_firmware` has no
manifest and takes the latest available object that carries them. Objects
without an artifact catalog are skipped.

The persisted catalog's `format_version` identifies RMS's internal storage
format, not the manifest schema or firmware release. RMS rejects unsupported
catalog versions and objects without this marker. Bump the
version only for incompatible storage/identity changes, with a migration or
re-import policy. Powershelf component names and vendor-specific powershelf
components are described in [Powershelves](#powershelves).

Authenticated Artifactory downloads require HTTPS. Redirects may stay on the
original HTTPS origin only; RMS never forwards the API key to another origin.
Unauthenticated HTTP downloads remain supported. Temporary local copies and
remote downloads are cleaned up on cancellation or failure. Preparation keeps
typed errors: invalid selections, checksum/cache failures, authentication
failures, missing files, and transient transport failures have distinct job
classifications. The legacy firmware-status enum groups some client failures;
`GetJobStatus` exposes the generic error codes.
Redirect-policy refusals are failed preconditions, not invalid client arguments.
Metadata projection rejects ambiguous board selections rather than returning
partial platform metadata. The catalog remains retrievable with absent metadata,
and RMS logs the projection error with the object ID. A valid empty projection
instead returns present metadata with empty component lists.

Stored-apply history waits at most `workflows.apply_history_timeout_seconds`
(default: 86400 seconds) for child outcomes. On expiry, it records only nodes
already observed as completed; failed, missing, and unfinished jobs are excluded.
This observation deadline does not cancel hardware jobs or report them as
successful. Increase it for deployments whose multi-stage updates can exceed
one day. It is independent of the per-task firmware polling timeout.
Terminal outcomes use the shared job-registry waiter. History insertion checks
the object's creation timestamp atomically with the write, so an old apply
cannot insert history after that object has been deleted or replaced. Existing
history still uses the documented ID-based availability semantics.

`component_filters` restrict which components apply to which node type.
CX7, CX8, and BF3 NIC artifacts are opt-in. If one firmware-object apply selects
both Flint and out-of-band components for a GB200 compute node, RMS executes
them as stages in one child job: it flashes and activates all in-band targets
first, then processes and activates the out-of-band targets. A failed or
cancelled in-band stage prevents the out-of-band stage from starting. Direct
firmware-update RPCs continue to reject mixed in-band and out-of-band targets.
