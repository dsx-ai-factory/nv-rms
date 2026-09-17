# Power Control

See [Operations Overview](overview.md) for the conventions (in-band failures,
registered vs. ephemeral targets) referenced below.

All power RPCs are synchronous. Registered-node power mutations hold an exclusive
per-rack power guard and are refused while another one is in progress on the
same rack.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `SetPowerState` | Sync | Apply one power operation to a single **registered** node. | `rack_id`, `node_id`, `operation` | `status` |
| `BatchSetPowerState` | Sync | Apply one operation to a caller-supplied **ephemeral** node set, independently per node. | `operation`, `nodes` (with per-node endpoints + creds) | `NodeBatchResponse` (`node_results[]`, `stats`) |
| `GetPowerState` | Sync | Read one **registered** node's power state (read path, no guard). | `rack_id`, `node_id` | `status`, `pstate` |
| `BatchGetPowerState` | Sync | Read power state for a caller-supplied **ephemeral** node set. | `nodes` | `NodeBatchResponse` + `node_power_states[]` |

Invalid `operation` enums are rejected with `INVALID_ARGUMENT`. A batch's overall
status is `Success` only when zero nodes failed. Switch nodes require a BMC
endpoint (Redfish) for power operations.
