# RMS Hardware Compatibility List

This Hardware Compatibility List (HCL) is provided for reference purposes only.
Systems listed here have been unit tested or exercised internally in limited
RMS scenarios. Inclusion in this list does not imply qualification,
certification, or support, and does not represent a commitment to ongoing
compatibility. For specific hardware support inquiries or technical
specifications, please contact the original hardware vendor.

RMS compatibility depends on both the hardware management interface and the node
identity sent by the caller. New clients should send `node_descriptor` attributes
using the matrix below. Legacy `NodeType` values remain supported; when a request
sets a non-`NODE_TYPE_UNSPECIFIED` `NodeType`, that enum value overrides
`node_descriptor` for dispatch.

Last Updated: 2026-08-03

## Supported Hardware

| Hardware | RMS Node Type | Management Interface | Validated Firmware Version | Notes |
| --- | --- | --- | --- | --- |
| GB200 Compute Tray (1RU) | `compute_gb200_nvidia` | Redfish over HTTPS | 1.3.2GA | NVIDIA GB200 compute tray path for inventory, power, and firmware workflows. |
| GB200 Power Shelf, LiteOn | `powershelf_gb200_liteon` | Redfish over HTTPS | | LiteOn GB200 power shelf path for inventory, power, and firmware workflows. |
| GB200 Power Shelf, Delta | `powershelf_gb200_delta` | Redfish over HTTPS | | Delta GB200 power shelf path for inventory, power, and firmware workflows. |
| GB300 Compute Tray, NVIDIA | `compute_gb300_nvidia` | Redfish over HTTPS | | NVIDIA GB300 compute tray path for inventory, power, and firmware workflows. |
| GB300 Power Shelf, LiteOn | `powershelf_gb300_liteon` | Redfish over HTTPS | | LiteOn GB300 power shelf path for inventory, power, and firmware workflows. |
| GB300 Power Shelf, Delta | `powershelf_gb300_delta` | Redfish over HTTPS | | Delta GB300 power shelf path for inventory, power, and firmware workflows. |
| NVSwitch Tray DGX | `switch_gb200_nvidia` | NVUE REST and SSH | 1.3.2GA | NVIDIA GB200 NVSwitch tray path for switch inventory and firmware workflows. |
| GB300 NVSwitch Tray, NVIDIA | `switch_gb300_nvidia` | NVUE REST and SSH | | NVIDIA GB300 NVSwitch tray path for switch inventory and firmware workflows. |

## Hardware Under Development

This list outlines platforms that are under development and have not undergone
full RMS compatibility testing.

| Hardware | RMS Node Type | Management Interface | Development Firmware Version | Notes |
| --- | --- | --- | --- | --- |
| Wiwynn GB200 Compute Tray | `NodeDescriptor` only | Redfish over HTTPS |  | Wiwynn GB200 reuses the reference compute path and accepts both BMC firmware locations supplied by the firmware manifest. |
| Lenovo GB300 Compute Tray | `compute_gb300_lenovo` | Redfish over HTTPS | BMC 3.0.0; BIOS/UEFI 1.0.0GA | Lenovo GB300 compute support is under development and should be validated against the target site release before use. |
| Supermicro GB300 Compute Tray | `NodeDescriptor` only | Redfish over HTTPS | Host BMC 70.02.01.05; BIOS 2.3b | Supermicro GB300 uses the manifest-selected nosbios, BIOS, and host-BMC payloads with required power cycles between nosbios and BIOS. |

## Node Descriptor Translation

RMS resolves a descriptor to the internal node type at the gRPC boundary. The
required identity keys are `role`, `vendor`, and `product_family`. The optional
`inventory_profile` key selects a deployment-defined expected firmware
inventory and does not affect node-type resolution. RMS rejects descriptors
with missing or empty required attributes and rejects unsupported attributes.

`inventory_profile` is an opaque, case-sensitive identifier after trimming.
RMS looks it up in `[workflows.expected_inventory_profiles]`. Missing metadata
preserves legacy firmware behavior; an empty or unknown supplied profile
rejects that node before firmware work starts. `ListNodeInventory` returns the
selected profile with the canonical identity attributes.

Wiwynn GB200 and Supermicro GB300 are descriptor-only and do not add `NodeType`
protobuf enum values. Requests use `NODE_TYPE_UNSPECIFIED` with the descriptors
shown below.

| `role` | `vendor` | `product_family` | Internal node type |
| --- | --- | --- | --- |
| `compute` | `nvidia` | `gb200` | `compute_gb200_nvidia` |
| `compute` | `wiwynn` | `gb200` | `compute_gb200_wiwynn` |
| `compute` | `nvidia` | `gb300` | `compute_gb300_nvidia` |
| `compute` | `lenovo` | `gb300` | `compute_gb300_lenovo` |
| `compute` | `supermicro` | `gb300` | `compute_gb300_supermicro` |
| `compute` | `nvidia` | `vrnvl72` | `compute_vrnvl72_nvidia` |
| `switch` | `nvidia` | `gb200` | `switch_gb200_nvidia` |
| `switch` | `nvidia` | `gb300` | `switch_gb300_nvidia` |
| `switch` | `nvidia` | `vrnvl72` | `switch_vrnvl72_nvidia` |
| `power_shelf` | `liteon` | `gb200` | `powershelf_gb200_liteon` |
| `power_shelf` | `delta` | `gb200` | `powershelf_gb200_delta` |
| `power_shelf` | `liteon` | `gb300` | `powershelf_gb300_liteon` |
| `power_shelf` | `delta` | `gb300` | `powershelf_gb300_delta` |
