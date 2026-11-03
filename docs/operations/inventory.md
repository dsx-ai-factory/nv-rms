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
| `UpdateNode` | **Not implemented** - always returns `Failure`. | - | - |

`NodeDescriptor` accepts an optional `inventory_profile` attribute in addition
to the canonical `role`, `vendor`, and `product_family` identity attributes.
The value is an opaque deployment-defined key configured in
`[workflows.expected_inventory_profiles]`. Profiles can contain expected
NVFWUPD AP names and physical Flint device counts. `CreateNodes` rejects empty
or unknown supplied profiles, while omission preserves legacy behavior.
`ListNodeInventory` returns the selected profile.

`CreateNodes` succeeds only when no nodes fail. `DeleteNode` is refused while
a registered power operation is running on the rack (the operation resolves its
node from current inventory).

Dell GB200 AMI nodes use `role=compute`, `vendor=dell`, and
`product_family=gb200` in `node_descriptor`. They belong to `NVL_GB200` racks and
round-trip with the canonical descriptor and protobuf `NODE_TYPE_UNSPECIFIED`.
The host BMC inventory entry is `BMC`; `HGX_FW_BMC_0` identifies the separate HMC.

### Client node CSV

Build the bundled client with
`cargo build --release -p rackmanagementservice --features grpc-client --bin rack_manager_client`.
Its node-list CSV accepts a protobuf enum number/name or an RMS stable node name
in the `type` column. Stable names such as `compute_gb200_dell` use the canonical
server mappings to supply the descriptor when no protobuf enum value exists.
This applies to registration and direct node-list operations, including firmware
inventory, comparison, and firmware-object apply. Numeric type-keyed target and
component-filter arguments retain their existing protobuf enum format.

```csv
# node_id,rack_id,ip,port,username,password,mac,type,endpoint_role
dell-gb200-01,dell-lab,192.0.2.4,443,BMC_USER,BMC_PASSWORD,02:00:00:00:00:04,compute_gb200_dell,bmc
```

Use a comment beginning with `#` for the optional header. Supply the actual BMC
address, MAC, and credentials in an uncommitted local CSV. Compute operations
and inventory registration require the BMC MAC field.

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
