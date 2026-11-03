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

Last Updated: 2026-10-16

## NVIDIA-Validated Hardware

Hardware in this section has been tested by NVIDIA engineers against the listed
firmware versions and management interfaces.

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

## Community-Validated Hardware

Hardware in this section has been validated by community contributors. NVIDIA
has not independently tested these platforms. Entries require a linked PR with
evidence of the commands and lifecycle paths tested.

| Hardware | RMS Node Type | Management Interface | Validated Firmware Version | Contributor | Notes |
| --- | --- | --- | --- | --- | --- |

## Hardware Under Development

This list outlines platforms that are under development and have not undergone
full RMS compatibility testing.

| Hardware | RMS Node Type | Management Interface | Development Firmware Version | Notes |
| --- | --- | --- | --- | --- |
| Wiwynn GB200 Compute Tray | `NodeDescriptor` only | Redfish over HTTPS |  | Wiwynn GB200 reuses the reference compute path and accepts all eligible HMC and BMC firmware locations supplied by the firmware manifest. |
| Dell GB200 Compute (AMI host BMC) | `NodeDescriptor` only | Redfish over HTTPS | AMI 25.07.29001500; HMC GB200Nvl-25.06-A (captured inventory) | BMC `.ima`, HMC nosbios `.fwpkg`, and SBIOS `.fwpkg` updates with two activation stages. Validated with captured responses and simulated updates; physical update qualification is pending. |
| Lenovo GB300 Compute Tray | `compute_gb300_lenovo` | Redfish over HTTPS | BMC 3.0.0; BIOS/UEFI 1.0.0GA | Lenovo GB300 compute support is under development and should be validated against the target site release before use. |
| Supermicro GB300 Compute Tray | `NodeDescriptor` only | Redfish over HTTPS | Host BMC 70.02.01.05; BIOS 2.3b | Supermicro GB300 uses the manifest-selected nosbios, BIOS, and host-BMC payloads with required power cycles between nosbios and BIOS. |
| VR NVL72 Power Shelf, LiteOn (PF-1114-1RD) | `NodeDescriptor` only | Redfish over HTTPS |  | Covers inventory, power, PMC firmware updates, and all-PSU firmware updates through `UpdateService.HttpPushUri`. This shelf exposes standard DMTF `PowerSubsystem/PowerSupplies` resources and OEM inventory at `/redfish/v1/Chassis/powershelf/Power/Oem/LiteOn/PowerUnits`, although Redfish discovery omits the OEM links. Direct OEM per-PSU updates do not take effect; explicit per-slot requests and automatic OEM fallback are disabled for this model. The chassis exposes no `#Chassis.Off`, and its `ResetActionInfo` allows only a `ResetType` of `Reset`, so RMS serves graceful power off requests with `#Chassis.ForceOff`. |
| VR NVL72 Power Shelf, Delta | `NodeDescriptor` only | Redfish over HTTPS |  | Identity-only support. RMS reuses the Delta GB200/GB300 Redfish behavior for power state and NVFWUPD firmware workflows (`DeltaPMC` and `DeltaPSU` components). Power control uses `#Chassis.Reset` (`On` / `ForceOff`) on the shelf chassis, as reported by AHE54V2037A shelves on an NVIDIA PMC. Firmware updates on VR NVL72 are not yet verified. |

## Node Descriptor Translation

VR NVL72 switches use BMC Redfish for power, firmware inventory, comparison, and
firmware activation. Firmware uploads still use NVUE/SSH; update requests require
both BMC and host credentials. This does not change GB200/GB300 switch routing.

RMS resolves a descriptor to the internal node type at the gRPC boundary. The
required identity keys are `role`, `vendor`, and `product_family`. The optional
`inventory_profile` key selects a deployment-defined expected firmware
inventory, including optional physical Flint device counts, and does not affect
node-type resolution. RMS rejects descriptors
with missing or empty required attributes and rejects unsupported attributes.

`inventory_profile` is an opaque, case-sensitive identifier after trimming.
RMS looks it up in `[workflows.expected_inventory_profiles]`. Missing metadata
preserves legacy firmware behavior; an empty or unknown supplied profile
rejects that node before firmware work starts. `ListNodeInventory` returns the
selected profile with the canonical identity attributes.

Dell GB200, Wiwynn GB200, Supermicro GB300, and the VR NVL72 LiteOn and Delta
power shelves are descriptor-only and do not add `NodeType` protobuf enum values. Requests use
`NODE_TYPE_UNSPECIFIED` with the descriptors shown below.

| `role` | `vendor` | `product_family` | Internal node type |
| --- | --- | --- | --- |
| `compute` | `nvidia` | `gb200` | `compute_gb200_nvidia` |
| `compute` | `wiwynn` | `gb200` | `compute_gb200_wiwynn` |
| `compute` | `dell` | `gb200` | `compute_gb200_dell` |
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
| `power_shelf` | `liteon` | `vrnvl72` | `powershelf_vrnvl72_liteon` |
| `power_shelf` | `delta` | `vrnvl72` | `powershelf_vrnvl72_delta` |
