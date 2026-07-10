# RMS Hardware Compatibility List

This Hardware Compatibility List (HCL) is provided for reference purposes only.
Systems listed here have been unit tested or exercised internally in limited
RMS scenarios. Inclusion in this list does not imply qualification,
certification, or support, and does not represent a commitment to ongoing
compatibility. For specific hardware support inquiries or technical
specifications, please contact the original hardware vendor.

RMS compatibility depends on both the hardware management interface and the
site configuration used to identify the node type. Use the listed RMS node type
when configuring rack profiles or creating nodes for these platforms.

Last Updated: 2026-06-29

## Supported Hardware

| Hardware | RMS Node Type | Management Interface | Validated Firmware Version | Notes |
| --- | --- | --- | --- | --- |
| GB200 Compute Tray (1RU) | `compute_gb200_nvidia` | Redfish over HTTPS | 1.3.2GA | NVIDIA GB200 compute tray path for inventory, power, and firmware workflows. |
| GB200 Power Shelf, LiteOn | `powershelf_gb200_liteon` | Redfish over HTTPS |  | LiteOn GB200 power shelf path for inventory, power, and firmware workflows. |
| GB200 Power Shelf, Delta | `powershelf_gb200_delta` | Redfish over HTTPS |  | Delta GB200 power shelf path for inventory, power, and firmware workflows. |
| GB300 Compute Tray, NVIDIA | `compute_gb300_nvidia` | Redfish over HTTPS |  | NVIDIA GB300 compute tray path for inventory, power, and firmware workflows. |
| GB300 Power Shelf, LiteOn | `powershelf_gb300_liteon` | Redfish over HTTPS |  | LiteOn GB300 power shelf path for inventory, power, and firmware workflows. |
| GB300 Power Shelf, Delta | `powershelf_gb300_delta` | Redfish over HTTPS |  | Delta GB300 power shelf path for inventory, power, and firmware workflows. |
| NVSwitch Tray DGX | `switch_gb200_nvidia` | NVUE REST and SSH | 1.3.2GA | NVIDIA GB200 NVSwitch tray path for switch inventory and firmware workflows. |
| GB300 NVSwitch Tray, NVIDIA | `switch_gb300_nvidia` | NVUE REST and SSH |  | NVIDIA GB300 NVSwitch tray path for switch inventory and firmware workflows. |

## Hardware Under Development

This list outlines platforms that are under development and have not undergone
full RMS compatibility testing.

| Hardware | RMS Node Type | Management Interface | Development Firmware Version | Notes |
| --- | --- | --- | --- | --- |
| Lenovo GB300 Compute Tray | `compute_gb300_lenovo` | Redfish over HTTPS | BMC 3.0.0; BIOS/UEFI 1.0.0GA | Lenovo GB300 compute support is under development and should be validated against the target site release before use. |
