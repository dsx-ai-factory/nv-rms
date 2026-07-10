# Adding Support for New RMS Hardware

This guide explains how to add or extend hardware support in the Rack Management
Service (RMS) stack when new BMC/server hardware arrives that does not work out
of the box.

For the list of currently supported hardware, see
[RMS Hardware Compatibility List](../hcl.md).

## Overview

RMS models hardware as a closed set of concrete node types. A new platform is
not only a new Redfish or NVUE behavior; it usually affects API identity,
endpoint policy, rack-family routing, concrete node construction, firmware
activation policy, firmware-object mapping, and tests.

The most important rule is to keep existing hardware behavior stable. Add a
new `NodeType` or a narrow wrapper when behavior differs by product, vendor, or
firmware package policy. Do not silently change shared GB200 or GB300 behavior
unless the change is valid for every node type that delegates to that code.

## Implementation Patterns

RMS hardware support generally follows three common patterns:

| Pattern | Example | When to Use |
| --- | --- | --- |
| Explicit hardware identity | `ComputeGb200Nvidia`, `SwitchGb200Nvidia`, `PowershelfGb200Liteon` | Use when a previously generic role needs a stable product/vendor-specific API value. |
| Thin wrapper over an existing implementation | `SwitchGb300Nvidia`, `PowershelfGb300Liteon`, `NvidiaGb300Compute` | Use when the new hardware shares most behavior with an existing node but needs a distinct `NodeType`, product family, or NVFWUPD server type. |
| Wrapper with targeted overrides | `LenovoGb300Compute`, `PowershelfGb300Delta` | Use when most behavior can be reused but the platform needs vendor-specific firmware options, preserve-config behavior, endpoint discovery, power control, or component filtering. |

## Before Coding

Classify the new hardware before making code changes:

1. Is it a new product family, such as a new NVL rack generation?
2. Is it a new node role, or one of the existing roles: compute, powershelf, or switch?
3. Is it a new vendor-specific node type inside an existing product family?
4. Does it use the BMC endpoint, the host endpoint, or both?
5. Does NVFWUPD already have an appropriate `ServerType`, target class, and firmware package behavior?
6. Does firmware-object apply need target remapping, component filtering, or multiple firmware packages for one logical target?
7. Can it safely delegate to an existing implementation, or does it need a new concrete node implementation?

## API and Node Identity

Add or update the public node identity first. RMS uses the protobuf `NodeType`
as the API contract and `src/domain/node.rs` as the domain source of truth.

Expected changes:

- Update the `NodeType` enum in the `nv-rms-client` protobuf/client source, then bump the `src/api/grpc/proto/nv-rms-client` submodule pointer in RMS.
- Add the matching domain variant in `src/domain/node.rs`.
- Add the stable config string in `NodeType::as_str()`.
- Add the role in `NodeType::kind()`.
- Add the rack generation in `NodeType::product_family()`.
- Confirm `NodeType::uses_host_management_endpoint()` is correct. Switches use the host endpoint; compute and powershelf nodes normally use the BMC endpoint.
- Add or reuse the right `NvfwupdServerProfile` in `NodeType::nvfwupd_server_profile()`.
- Extend the `NodeType` metadata tests in `src/domain/node.rs`.

If the hardware belongs to a new rack generation, add a new `ProductFamily`
value and define its rack type and model strings.

## Concrete Node Implementation

Add a concrete node module under `src/nodes/` when the new hardware needs a
distinct runtime implementation. Start with the smallest implementation that
preserves a distinct `NodeType`.

Expected changes:

- Add a module in `src/nodes/mod.rs`.
- Add a concrete node type file, for example `compute_<family>_<vendor>.rs`.
- Implement `from_config()` and reject mismatched `config.node_type`.
- Implement the `Node` trait.
- Return the new `NodeType` from `node_type()`.
- Override `get_info()` so the returned `type` field uses the new stable node type string.
- Validate required endpoints. BMC-backed nodes should reject missing BMC endpoints; switch nodes should validate host endpoint behavior.
- Delegate to an existing implementation only when its behavior is correct for the new hardware.
- Add unit tests for constructor validation, reported node type, endpoint handling, and any vendor-specific behavior.

Thin wrappers are preferred when behavior is intentionally shared. For example,
GB300 switch support can alias the GB200 switch implementation while preserving
the GB300 node type so NVFWUPD and API routing select GB300 behavior where
needed.

Use targeted overrides when only a few operations differ. Lenovo GB300 compute
delegates most behavior to NVIDIA GB300 compute, but overrides firmware update
options for Lenovo BMC and HGX targets and preserves Lenovo BMC update
configuration before BMC updates.

## Runtime Dispatch

Register the new node type in `src/nodes/instance.rs`. This is the bridge
between the domain `NodeType` and the concrete runtime implementation.

Expected changes:

- Add a `NodeInstance` enum variant.
- Add the variant to the `match_node_instance!` macro.
- Add construction in `NodeInstance::from_config()`.
- Add switch-specific trait dispatch if the node is a switch:
  `SwitchFirmwareManagement`, `SwitchPasswordManagement`,
  `SwitchScaleUpManagement`, and device-info behavior.
- Add compute or switch device-info handling in `NodeInstance::get_device_info()` if the new node exposes topology or chassis-location data.

Because `NodeInstance` is intentionally closed and exhaustive, missing entries
usually appear as compile errors. Treat those compile errors as useful checklist
items, not as noise.

## Rack and Product-Family Routing

RMS rejects nodes whose product family does not match the rack type. For a new
node type, verify that rack creation and node admission agree.

Expected changes:

- For an existing rack family, update `NodeType::product_family()`.
- For a new rack family, add a rack wrapper in `src/racks/` and export it from `src/racks/mod.rs`.
- Update `create_rack_by_type()` in `src/orchestrator/rack_manager.rs`.
- Add tests that create the new rack type and reject the new node type in the wrong rack family.

When a later rack generation shares behavior with an existing family, prefer an
inner wrapper that preserves the shared implementation while changing the rack
type and model strings. This keeps the product-family identity distinct without
duplicating rack behavior.

## gRPC Conversion and Endpoint Policy

Update `src/api/grpc/conversions.rs` whenever a new protobuf node type is
introduced.

Expected changes:

- Map protobuf `NodeType` to the domain `NodeType`.
- Map domain `NodeType` back to protobuf.
- Confirm `proto_node_type_to_string()` returns the stable node type name.
- Add tests for both conversion directions.
- Confirm flattened endpoint handling still selects BMC or host endpoint based on the new node type.

Most endpoint behavior should follow `NodeType::uses_host_management_endpoint()`.
If a handler still has explicit node-type matching, update it intentionally and
add a regression test.

## Firmware and NVFWUPD

Firmware support is usually the widest part of a new hardware change. RMS has
two layers to update: the concrete node implementation and the firmware-object
apply path.

Expected changes in RMS:

- Update `src/nodes/nvfwupd_adapter.rs` if the new node needs a new
  `NvfwupdServerProfile` or a different NVFWUPD `ServerType`.
- Implement or delegate node methods for inventory, version checks, update,
  task polling, and activation.
- Update `activation_mode_for_node()` in `src/api/grpc/firmware_handlers.rs`.
- Update `node_supports_post_activation_version_check()` when post-activation
  version verification applies.
- Update firmware-object lookup tables and mappings in
  `src/api/grpc/firmware_object_handlers.rs`.
- Add component policy for vendor-specific firmware targets, such as BMC-only,
  HMC-only, LiteOn PSU/PMC, or Delta PSU/PMC.
- Add multi-package rules when one logical target needs more than one firmware
  package.

Expected changes in NVFWUPD:

- Confirm `rust_nvfwupd/src/workflow.rs` has the correct `ServerType`.
- Add target aliases in `rust_nvfwupd/src/rf_target.rs` if package metadata or
  CLI input needs to resolve to an existing target implementation.
- Add or update target behavior in `rust_nvfwupd/src/workflow_api.rs` when the
  activation, update, or version-check workflow differs.
- Add tests for server-type mapping, package parsing, update request options,
  and activation behavior.

Use vendor-specific firmware options only in the wrapper for that vendor. For
example, Lenovo GB300 sets OEM parameters for BMC and HGX targets without
changing the shared GB300 compute behavior.

## Handler Surfaces to Review

After adding the domain node type and concrete node, search for existing
explicit `NodeType` matches. Use this tree as the quick review map:

```text
rackmanagementservice/
├── src/
│   ├── domain/
│   │   └── node.rs                         # NodeType, NodeKind, ProductFamily, endpoint policy
│   ├── nodes/
│   │   ├── instance.rs                     # NodeInstance construction and trait dispatch
│   │   ├── mod.rs                          # Node module exports
│   │   ├── compute_<family>_<vendor>.rs    # Compute implementations and wrappers
│   │   ├── switch_<family>_<vendor>.rs     # Switch implementations and wrappers
│   │   ├── powershelf_<family>_<vendor>.rs # Powershelf implementations and wrappers
│   │   └── nvfwupd_adapter.rs              # NVFWUPD profile to ServerType mapping
│   ├── racks/
│   │   ├── mod.rs                          # Rack module exports
│   │   └── nvl_gb.rs                       # NVL GB rack wrappers and product-family checks
│   ├── orchestrator/
│   │   └── rack_manager.rs                 # Rack creation and node admission
│   └── api/grpc/
│       ├── conversions.rs                  # Protobuf/domain NodeType mapping and endpoints
│       ├── inventory_handlers.rs           # Inventory and device-info APIs
│       ├── power_handlers.rs               # Power state and power actions
│       ├── firmware_handlers.rs            # Firmware upload, activation, and batch update
│       ├── firmware_object_handlers.rs     # Firmware-object target/component mapping
│       ├── switch_handlers.rs              # Switch management flows
│       ├── switch_image_handlers.rs        # Switch image management
│       ├── switch_security_handlers.rs     # Switch security and credential flows
│       ├── switch_certificate_handlers.rs  # Switch certificate flows
│       ├── scaleupfabricmanager_handlers.rs # Scale-up FM flows
│       ├── node_recovery.rs                # Node recovery flows
│       └── proto/nv-rms-client/            # Protobuf/client NodeType source
├── rust_nvfwupd/
│   └── src/
│       ├── workflow.rs                     # NVFWUPD ServerType definitions
│       ├── workflow_api.rs                 # Update, activation, and version workflows
│       └── rf_target.rs                    # Firmware target aliases and parsing
└── tests/
    ├── client/src/main.rs                  # Admin/test client node-type parsing
    └── grpc_e2e.rs                         # End-to-end API coverage
```

Prefer role-based checks through `NodeKind` when all nodes in a role should
behave the same. Use explicit node-type checks only when behavior really differs
by hardware type or vendor.

## Testing

At minimum, add unit tests for the files touched by the new hardware path:

- `src/domain/node.rs`: string, kind, product family, endpoint policy, and NVFWUPD profile.
- `src/api/grpc/conversions.rs`: protobuf/domain round trips and stable string conversion.
- `src/nodes/<new_node>.rs`: constructor validation, reported type, endpoint requirements, and platform-specific overrides.
- `src/nodes/instance.rs`: construction and dispatch when the new node has special switch or device-info behavior.
- `src/orchestrator/rack_manager.rs` or `src/racks/*`: rack creation and product-family mismatch checks.
- `src/api/grpc/firmware_handlers.rs`: activation mode and post-activation version-check policy.
- `src/api/grpc/firmware_object_handlers.rs`: firmware-object device type, component filtering, multi-package rules, and target mapping.
- `tests/client/src/main.rs`: node-type parsing and role-specific client behavior.
- `tests/grpc_e2e.rs`: one end-to-end API path for create/add/list/power or firmware behavior.

For vendor quirks, use focused HTTP mock tests to verify
preserve-configuration PATCH behavior, readback verification, retry behavior,
and firmware update option selection.

For real hardware validation, run a small progression before broad rollout:

1. Create a rack of the target product family.
2. Add one node with the new `NodeType` and the expected endpoint shape.
3. List inventory and confirm the returned node type.
4. Run a low-risk power read.
5. Run firmware inventory.
6. Verify firmware package versions for a known-good package.
7. Exercise the narrowest safe firmware update or activation flow in a controlled lab.

## Documentation and Release Checklist

Update documentation with the same branch as the hardware support code:

- Add the new platform to `docs/hcl.md` when support is ready.
- Add it under "Hardware Under Development" while support is still being validated.
- Document required rack profile values for NICo integration, especially
  product family and canonical vendor strings.
- Document any firmware package restrictions, component names, or special
  target behavior.
