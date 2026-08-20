# Firmware

See [Operations Overview](overview.md) for the conventions (in-band failures,
registered vs. ephemeral targets) and [the async job model](overview.md#the-async-job-model)
that the asynchronous firmware RPCs below build on.

## Firmware - inventory queries

Synchronous reads of installed firmware.

| RPC | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- |
| `GetNodeFirmwareInventory` | Firmware inventory for one node. | `rack_id`, `node_id` | `firmware_list[]` (name, version, updateable, target, health, type) |
| `GetRackFirmwareInventory` | Firmware inventory for every node in a rack. | `rack_id` | `nodes[]` (each with `firmware_list`) |

## Firmware - asynchronous updates

These start firmware updates and return job IDs. Poll them with
**`GetFirmwareJobStatus`**. A new update is admitted only if the target node is
idle; a second job for the same `(rack_id, node_id)` is rejected as "update in
progress."

| RPC | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- |
| `UpdateFirmware` | Start an async update on one **registered** node: upload each target, poll to completion, optionally activate. | `rack_id`, `node_id`, `firmware_targets[]`, `activate`, `force_update` | `job_id`, `status` |
| `BatchUpdateFirmwareByNodeType` | Apply the same target(s) to all nodes of a `node_type` in a registered rack. One child job per node under a **parent** job. | `rack_id`, `node_type`, targets, `activate`, `force_update` | `NodeBatchResponse` (`job_id` = parent), `jobs[]` (node → child job) |
| `BatchUpdateFirmware` | Apply firmware to a caller-supplied **ephemeral** node list using a per-node-type target map. | `nodes`, `firmware_targets` (map), `activate`, `force_update` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `GetFirmwareJobStatus` | **Poller** for all firmware-update and firmware-object-apply jobs (leaf or parent). Parent jobs report aggregated child state. | `job_id` | `job_state`, `state_description`, `error_code`, `result_json`, timestamps |

For NVFWUPD-backed updates, a node may select a configured expected hardware
inventory with `NodeDescriptor.attributes["inventory_profile"]`. RMS validates
the profile before admitting a child job and asks NVFWUPD to recheck the AP
inventory before every target update and retry. A mismatch fails with
failed-precondition details listing missing and present APs. Firmware version
verification does not perform this inventory check.

### Supermicro GB300 sequencing

For the descriptor-only Supermicro GB300 compute type, firmware-object apply
selects artifacts by filename from the manifest rather than by location order:
the HMC `*nosbios*.fwpkg`, the `BIOS*.bin`, and the host-BMC `*OBMC*.bin`.
RMS applies them in that order. With activation enabled it performs a full
compute power cycle after nosbios and after BIOS; the host-BMC update runs last
and does not add another host AC cycle. A combined nosbios-plus-BIOS request is
rejected when activation is disabled because the required intermediate cycle
would be skipped.

## Firmware objects

A **firmware object** is a catalog entry built from a firmware manifest JSON: a
software object tree plus RMS-parsed component and artifact metadata, scoped to a
`hardware_type`, optionally marked default, and only `available` once RMS has
downloaded all referenced artifacts. See the
[sample manifest](https://github.com/NVIDIA/nv-rms/tree/main/sample_firmware_manifest).

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `AddFirmwareObject` | Sync (background download) | Validate + persist a catalog entry from `config_json` and kick off an artifact download. Poll readiness via the object's `available` flag. | `config_json`, `hardware_type`, `access_token?`, `set_default` | `object` |
| `GetFirmwareObject` | Sync | Retrieve one entry by ID. | `id` | `object` |
| `ListFirmwareObjects` | Sync | List entries, optionally filtered. | `only_available?`, `hardware_type?` | `objects[]` |
| `DeleteFirmwareObject` | Sync | Remove an entry, its on-disk cache, and any in-flight download. | `id` | `OperationResponse` |
| `SetDefaultFirmwareObject` | Sync | Mark one object default for its hardware type. | `object_id` | `object` |
| `ApplyStoredFirmwareObject` | Async → `GetFirmwareJobStatus` | Resolve targets from an already-stored, **available** object and start update jobs. Records apply history on success. | `rack_id`, `object_id?`, `hardware_type`, `firmware_type`, `nodes`, `component_filters`, `force_update` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `ApplyFirmwareObject` | Async → `GetFirmwareJobStatus` | Ingest an **ephemeral** object from inline `config_json`, download, resolve, and start updates - nothing persisted. | `rack_id`, `config_json`, `access_token?`, `hardware_type`, `firmware_type`, `nodes`, `component_filters`, `force_update` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `ApplyStoredSwitchSystemImage` | Async → `GetSwitchSystemImageJobStatus` | Resolve a switch **system image** (NVOS) from a stored, available object and start switch image-update jobs. Records history. | `rack_id`, `object_id?`, `hardware_type`, `software_type`, `nodes` | `NodeBatchResponse` (parent `job_id`), `image_filename`, `jobs[]` |
| `ApplySwitchSystemImage` | Async → `GetSwitchSystemImageJobStatus` | Ephemeral inline-`config_json` variant of the above. | `rack_id`, `config_json`, `access_token?`, `software_type`, `hardware_type`, `nodes` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `GetFirmwareObjectHistory` | Sync | List prior apply events recorded by the `ApplyStored*` RPCs. | `object_id?`, `rack_ids?` | `records[]` |

`component_filters` restrict which components apply to which node type.
