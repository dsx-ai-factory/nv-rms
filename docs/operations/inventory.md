# Inventory and Device Info

See [Operations Overview](overview.md) for the conventions (in-band failures,
registered vs. ephemeral targets) referenced below.

## Inventory

Node and rack registration. All synchronous.

| RPC | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- |
| `CreateNodes` | Register a batch of nodes, auto-creating the owning rack. Every node requires a stable **BMC endpoint**; a host endpoint is optional (switches may be BMC-only). | `nodes` (NodeSet) | `OperationResponse`, `stats` |
| `ListNodeInventory` | Enumerate all nodes across all racks. | - | `nodes[]` (id, rack, type, descriptor, addresses) |
| `ListRacks` | List all rack IDs. | - | `rack_ids[]` |
| `DeleteNode` | Remove one node from its rack (holds the rack power guard). | `rack_id`, `node_id` | `OperationResponse` |
| `GetRackPowerOnSequence` | Return a rack's stored power-on order. | `rack_id` | `power_on_order[]`, `is_valid` |
| `SetRackPowerOnSequence` | Store/replace a rack's power-on order (index by list position). | `rack_id`, `power_on_order[]` | `OperationResponse` |
| `UpdateNode` | **Not implemented** - always returns `Failure`. | - | - |

`NodeDescriptor` accepts an optional `inventory_profile` attribute in addition
to the canonical `role`, `vendor`, and `product_family` identity attributes.
The value is an opaque deployment-defined key configured in
`[workflows.expected_inventory_profiles]`. `CreateNodes` rejects empty or
unknown supplied profiles, while omission preserves legacy behavior.
`ListNodeInventory` returns the selected profile.

`CreateNodes` and the sequence/delete RPCs succeed only when zero nodes fail, and
are refused while a rack power sequence is running (a running sequence depends on
current inventory).

## Device info

Tray/chassis location and topology info (chassis serial, slot number, tray
index). All synchronous.

| RPC | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- |
| `GetNodeDeviceInfo` | Device info for one **registered** node. | `rack_id`, `node_id` | `device_info?` (may be absent for node types without it) |
| `ListNodeDeviceInfoByNodeType` | Device info for all registered nodes of a type in a rack. | `rack_id`, `node_type` | `node_device_details[]`, `stats` |
| `BatchGetNodeDeviceInfo` | Device info for a caller-supplied **ephemeral** node set. Delta power-shelf types are rejected. | `nodes` | `node_device_details[]`, `stats` |

Node types that expose no device info return `Success` with an explanatory
message and no `device_info` payload.
