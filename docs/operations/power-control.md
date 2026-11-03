# Power Control

See [Operations Overview](overview.md) for the conventions (in-band failures,
registered vs. ephemeral targets) referenced below.

All power RPCs are synchronous except `ExecuteColdReboot`, which returns a job ID.
Registered-node power mutations hold an exclusive per-rack power guard and are
refused while another one is in progress on the same rack.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `SetPowerState` | Sync | Apply one power operation to a single **registered** node. | `rack_id`, `node_id`, `operation` | `status` |
| `BatchSetPowerState` | Sync | Apply one operation to a caller-supplied **ephemeral** node set, independently per node. | `operation`, `nodes` (with per-node endpoints + creds) | `NodeBatchResponse` (`node_results[]`, `stats`) |
| `GetPowerState` | Sync | Read one **registered** node's power state (read path, no guard). | `rack_id`, `node_id` | `status`, `pstate` |
| `BatchGetPowerState` | Sync | Read power state for a caller-supplied **ephemeral** node set. | `nodes` | `NodeBatchResponse` + `node_power_states[]` |
| `ExecuteColdReboot` | Async via `GetJobStatus` | Run the full rack cold reboot sequence on caller-supplied **ephemeral** nodes as one coordinator job. | `nodes` (compute, switch, and power shelf devices of one rack), `domain?`, optional timeouts | `response.job_id` |

Invalid `operation` enums are rejected with `INVALID_ARGUMENT`. A batch's overall
status is `Success` only when zero nodes failed. Batch switch power operations
require a BMC endpoint (Redfish) and BMC credentials.

VR NVL72 switches also require the BMC for registered power operations, including
`PowerCycle`; RMS does not initialize or fall back to NVUE for these operations.
GB200/GB300 switches retain their existing behavior: registered power operations
prefer the BMC when configured, with NVUE power-state reads and `PowerCycle`
available when no BMC is configured.

## Power shelf graceful power off

Some power shelves have no graceful power off in hardware. RMS hides this from
callers: `POWER_OPERATION_OFF` and `POWER_OPERATION_GRACEFUL_SHUTDOWN` work on
every power shelf type, except a LiteOn shelf that advertises no usable power
off action and a Delta VR NVL72 shelf whose `ResetActionInfo` excludes
`ForceOff` (refer to the failure cases below). The generated
[API reference](../api-reference/index.md) describes
`POWER_OPERATION_GRACEFUL_SHUTDOWN` as the Redfish `ResetType`
`GracefulShutdown`, because it comes from the `librms` protobuf definitions. For
power shelves, the behavior on this page applies.

- **LiteOn:** RMS uses `#Chassis.Off` if the chassis advertises it. Otherwise it
  uses `#Chassis.Reset` with `ResetType` `GracefulShutdown`, unless the action's
  `ResetActionInfo` lists allowable values that exclude it. If the shelf offers
  neither, RMS serves the request as `#Chassis.ForceOff`, or `#Chassis.Reset`
  with `ResetType` `ForceOff` when no direct `ForceOff` action exists.
- **Delta GB200 and GB300:** the shelf only has a `TurnOffPSUs` action, so RMS
  serves both operations with it.
- **Delta VR NVL72:** the shelf has no graceful power off, so RMS serves both
  operations as `#Chassis.Reset` with `ResetType` `ForceOff` on the shelf
  chassis. Power on uses `ResetType` `On`. If the action's `ResetActionInfo`
  lists allowable values that exclude the reset type, RMS sends nothing and the
  request fails.

RMS applies the fallback only when the shelf advertises no graceful path. If an
advertised graceful action fails, RMS returns the error and does not retry it as
a hard power cut. When a request falls back, RMS logs a warning with the node
ID, the requested operation, and the action it used. RMS logs that warning
before it sends the action, so the log exists even if the shelf rejects it.
`POWER_OPERATION_FORCE_OFF` always uses the force path directly.

If a LiteOn shelf advertises no usable power off action at all, or a Delta VR
NVL72 shelf's `ResetActionInfo` lists allowable values that exclude `ForceOff`,
RMS sends nothing and the request fails. Failures are in-band, so clients never
receive a gRPC error code for these cases:

- `SetPowerState` returns status `FAILURE` with no message.
- `BatchSetPowerState` reports `FAILURE` for the node. For LiteOn, the per-node
  `error_message` lists the chassis actions the shelf advertises. For Delta VR
  NVL72, it names the refused `ResetType` and the advertised allowable values.
- `ExecuteColdReboot` fails the job, and the failure text includes that same
  message.

Power shelves still reject `POWER_OPERATION_RESET`,
`POWER_OPERATION_GRACEFUL_RESTART`, and `POWER_OPERATION_FORCE_RESTART`.

## Cold reboot

`ExecuteColdReboot` orchestrates a full rack cold reboot on caller-supplied
nodes, with no inventory registration. The request must contain devices from a
single rack, including at least one power shelf, one compute tray, and one
switch tray. RMS rejects a request that fails this check before any power
action. Switch nodes need both a BMC endpoint and a host endpoint.

The sequence gracefully shuts down the compute trays, powers off the power
shelves, waits for the capacitors to discharge, powers the shelves back on,
waits for the tray BMCs and the switch NVOS to come up, and then powers on the
compute nodes. The sequence is fail-fast with no automated rollback.

The RPC returns a coordinator job ID immediately. Poll it with `GetJobStatus`:
while the job runs, `state_description` reports the current runbook step. On
completion or failure, `result_json` holds the per-stage timeline. The optional
timeout fields override the per-stage wait floors, poll maximums, and poll
interval, and each unset field falls back to the server default. For the request
fields and defaults, refer to
[`ExecuteColdReboot`](../api-reference/generated/power-control.md#executecoldreboot).
