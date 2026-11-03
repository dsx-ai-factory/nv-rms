# RMS gRPC API conventions

This page describes the conventions that apply across the RMS gRPC API: how you connect, how you
identify nodes, how requests and responses are shaped, and how RMS reports failures.

## Transport and authentication

RMS serves gRPC over TLS 1.3 and requires mutual TLS (mTLS) by default. The default port is `8801`.
An operator can enable plaintext for development only, and that mode needs two independent opt-ins.
For the settings, refer to [Configuring RMS](../configuration/configuring-rms.md#tls).

RMS doesn't apply per-RPC authorization. Any client that completes the TLS handshake can call every
RPC, so restrict access to trusted services and operators on a controlled management network.

## Registered and caller-supplied targets

Each RPC acts on nodes in one of three ways. The [RPC catalog](generated/rpc-catalog.md) shows the
mode of each RPC in its **Target** column.

| Target | How you identify nodes | Stored by RMS |
| --- | --- | --- |
| Registered inventory | `rack_id` and `node_id` of a node added with [`CreateNodes`](generated/inventory.md#createnodes) | Yes |
| Caller-supplied nodes | A `NodeSet` or `NodeInfo` in the request that carries endpoints and credentials | No |
| Not node-specific | No node. The RPC acts on the service, the firmware catalog, or a job | Not applicable |

Batch RPCs usually take caller-supplied nodes, so a client that owns the list of nodes can operate
on them without registering them first. For the reasoning behind this model, refer to
[Overview](../overview.md#stateless-with-respect-to-inventory).

## Node identity

A node is described by [`NodeInfo`](generated/types-rack-manager.md#nodeinfo), and a list of nodes by
[`NodeSet`](generated/types-rack-manager.md#nodeset).

- `node_id` identifies the node, and `rack_id` identifies the rack that contains it.
- `bmc_endpoint` and `host_endpoint` are each an
  [`Endpoint`](generated/types-rack-manager.md#endpoint): a network interface, a port, and
  [`Credentials`](generated/types-rack-manager.md#credentials). Credentials are either a username and
  password or a session token. Which endpoints a node needs depends on its type and on the RPC, so
  each RPC page and the [Operations](../operations/overview.md) guides state what is required.
- `type` is a [`NodeType`](generated/types-rack-manager.md#nodetype) that selects the hardware-specific
  implementation. If `type` is unset or `NODE_TYPE_UNSPECIFIED`, RMS uses `node_descriptor`.
  A concrete `type` takes precedence over the descriptor.
- `node_descriptor` carries identity attributes as string pairs. RMS dispatch currently accepts the
  `role`, `vendor`, and `product_family` attributes.

## Request and response shapes

- Fields in messages use the protobuf field names shown in this reference. `grpcurl` accepts these
  names and the lower camel case JSON names.
- Proto3 has no required fields. A field that an RPC needs says so in its description, and a missing
  value takes the protobuf default for its type.
- A field marked `optional` has explicit presence, so RMS can tell an unset field from a default
  value. A field marked `repeated` is a list, and a `map` field is a set of key-value pairs.
- A `oneof` allows at most one of its member fields. `Credentials` is an example.
- Enum fields use the value names shown in the reference. The zero value is `*_UNSPECIFIED` and
  means the caller left the field unset. Don't send it where an RPC needs a value.

## Error handling

RMS reports an outcome in two places: the gRPC status of the call, and a status inside the response.
Check both.

### Status in the response

Most RPCs return a [`ReturnCode`](generated/types-rack-manager.md#returncode) in a `status` field,
either directly or inside an
[`OperationResponse`](generated/types-rack-manager.md#operationresponse) or
[`NodeBatchResponse`](generated/types-rack-manager.md#nodebatchresponse).

| Value | Meaning |
| --- | --- |
| `RETURN_CODE_SUCCESS` | The operation completed. For a batch, every targeted node completed. |
| `RETURN_CODE_FAILURE` | The operation failed. Read the `message` or `error_message` field. |
| `RETURN_CODE_UNSPECIFIED` | The server left the status unset. Treat it as a failure. |

RMS returns validation and business failures this way, inside a successful gRPC response. A gRPC
`OK` status therefore doesn't mean the operation succeeded.

### Batch responses

RPCs that act on several nodes return a `NodeBatchResponse` with these fields:

| Field | Description |
| --- | --- |
| `status` | `RETURN_CODE_SUCCESS` only if every targeted node completed |
| `message` | A human-readable summary |
| `node_results` | One result for each node, with `node_id`, `status`, and `error_message` |
| `stats` | Counts of total, successful, and failed nodes |
| `job_id` | The parent job ID, set by asynchronous batch RPCs |

A batch can partly succeed. For an asynchronous batch, a `FAILURE` status together with successful
entries in `node_results` means RMS refused some nodes and started the rest. A batch fails outright
only when RMS admitted no node. Refer to [Async jobs](async-jobs.md#node-exclusion) for why RMS
refuses a node.

### gRPC errors

RMS rejects some requests with a gRPC error instead of a response status. For example, it returns
`INVALID_ARGUMENT` for an invalid enum value or an empty required identifier, and `NOT_FOUND` when
you poll a job that it doesn't track. Each RPC page lists the gRPC errors that were verified for
that RPC. Treat any non-`OK` gRPC status as a failed call.

## Concurrency

Operations on different nodes run in parallel, and operations on the same node run one at a time.
Registered power changes also hold a per-rack guard, so RMS refuses a second power change on a rack
while one is in progress. For details, refer to
[Operations overview](../operations/overview.md#node-exclusion-and-capacity).

## Compatibility

The `RackManager` service is a guaranteed public interface. Within a major version, existing clients
continue to work, and RMS adds new optional fields and RPCs in a backward-compatible way. Ignore
fields that you don't recognize. For the full policy, refer to
[Release and QA process](../release-and-qa-process.md).
