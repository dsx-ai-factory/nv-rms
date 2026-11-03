# RMS gRPC API reference

The Rack Management Service (RMS) exposes its functionality as a gRPC API. Use this reference to find
every RPC, see its request and response shapes, and learn whether it returns its result directly or
returns a job ID that you poll.

## Services

The RMS API consists of two gRPC services, both defined in the
[`librms`](https://github.com/dsx-ai-factory/nv-rms-client) protobuf package that RMS pins at build
time. RMS serves both on the same port (default `8801`).

| Service | Protobuf package | Proto file | Scope |
| --- | --- | --- | --- |
| `RackManager` | `rack_manager` | `rack_manager.proto` | Power, inventory, firmware, switches, jobs |
| `RackManagerV2` | `rack_manager_v2` | `rack_manager_v2.proto` | Scale-up fabric, system validation |

`RackManagerV2` reuses message types from the `rack_manager` package, such as `NodeSet`. Both services
share one job tracker, so you can poll a job from either service with the `RackManager` job status RPCs.

## How this reference is organized

| Page | What you find there |
| --- | --- |
| [Getting started](getting-started.md) | Connect to RMS, call your first RPC, and follow three common flows |
| [Conventions](conventions.md) | Node identity, registered and caller-supplied targets, and error handling |
| [Async jobs](async-jobs.md) | The job model, execution modes, and which RPC polls which job |
| [RPC catalog](generated/rpc-catalog.md) | Every RPC with its execution mode, polling RPC, and target |
| RPC pages by category | Request, response, and behavior for each RPC (refer to the following list) |
| [Protobuf reference](/rms/api-reference/protobuf-reference) | Interactive reference rendered from the protobuf definitions |

The RPC pages group RPCs by task:

- [Power control](generated/power-control.md)
- [Inventory and device info](generated/inventory.md)
- [Firmware inventory](generated/firmware-inventory.md)
- [Firmware object catalog](generated/firmware-objects.md)
- [Firmware updates](generated/firmware-updates.md)
- [Switch management](generated/switch-management.md)
- [Scale-up fabric](generated/scale-up-fabric.md)
- [Switch TLS and certificates](generated/switch-tls.md)
- [System validation](generated/validation.md)
- [Utility and jobs](generated/utility-jobs.md)

Message and enum definitions are in [`rack_manager` types](generated/types-rack-manager.md) and
[`rack_manager_v2` types](generated/types-rack-manager-v2.md).

## How the reference stays accurate

The protobuf files are the source of truth for request shapes, response shapes, and RPC descriptions.
The RPC pages are generated from them, so field names and descriptions match the API that RMS
serves. The execution mode, polling RPC, and target of each RPC come from a reviewed metadata file
that a CI check compares with the protobuf files.

For the behavior behind each RPC, such as retries, ordering, and per-node limits, refer to the
[Operations](../operations/overview.md) guides.

## Compatibility

The `RackManager` gRPC API is a guaranteed public interface of RMS. Within a major version, existing
clients continue to work and new functionality is added in a backward-compatible way. For the full
policy, refer to [Release and QA process](../release-and-qa-process.md).
