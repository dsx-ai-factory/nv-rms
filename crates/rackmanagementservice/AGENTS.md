# RMS platform bring-up guidance

This file supplements the repository-root `AGENTS.md`. Follow both files.
Paths beginning with `src/` are relative to this crate. Paths beginning with
`crates/`, `docs/`, or `helm/` are relative to the repository root.

## Architecture boundaries

- Treat `src/domain/node.rs` as the canonical source for RMS node identity and
  routing metadata.
- Keep domain types protocol-neutral. Protobuf and descriptor translation
  belongs in `src/api/grpc/`.
- Keep device I/O in `Node` implementations. Job tracking, scheduling, retry
  policy, and multi-stage workflow orchestration belong in handlers or the
  orchestrator.
- Return `RmsError::unimplemented` for unsupported `Node` operations. Do not
  provide silent no-op behavior.
- Keep credentials in `SecretString`-backed endpoint structures and preserve
  debug-redaction tests.

Registered inventory uses a Rack-to-Node hierarchy. RMS has no separate
registered "single-node system" domain model. Put concrete physical node
support under `src/nodes/`. A one-node deployment can register a one-node rack,
while supported direct RPCs can construct a request-local, ephemeral
`NodeInstance`. Do not add a rackless or empty-rack abstraction without an
explicit architecture requirement.

## Start with a complete type audit

A new enum variant or adapter is not complete platform support. Before editing,
and again before declaring support complete, find every closed-set dispatch:

```bash
rg -n 'NodeType::|DomainNodeType::' \
  crates/rackmanagementservice/src \
  crates/rackmanagementservice/tests
```

Classify every match as implemented, intentionally shared, or explicitly
unsupported for the new type. Do not add a wildcard arm merely to avoid this
audit.

## Platform identity

When adding or changing a platform, vendor, or node role:

1. Add or update its `NodeType` in `src/domain/node.rs`.
1. Update every canonical metadata mapping:
   - `NodeType::as_str`
   - `NodeType::kind`
   - `NodeType::product_family`
   - `NodeType::uses_host_management_endpoint`
   - `NodeType::nvfwupd_server_profile`
1. Add the type to `node_type_metadata_is_canonical`.
1. Add a `ProductFamily` only when the hardware needs a distinct registered
   rack family. Update its stable rack type and display model together.
1. Preserve stable node names as compatibility values. Do not rename or alias
   them without an explicit compatibility plan.

Do not route platforms with ad hoc strings or create a second node-type
registry.

## Descriptor and protobuf handling

Inspect both `src/api/grpc/conversions.rs` and
`src/api/grpc/node_type_resolver.rs`. Add descriptor resolution and canonical
outgoing-descriptor tests.

If the public API needs an exact new enum value, update the authoritative
`librms` protobuf source or version and regenerate bindings before adding the
Rust conversion. Do not invent a crate-local protobuf identity.

If protobuf has no exact platform value, keep the platform descriptor-only:

- map the domain type to protobuf `Unspecified`;
- preserve or reconstruct the canonical descriptor;
- keep descriptor-keyed firmware targets and filters distinct; and
- never collapse descriptor-only platforms into the same map entry.

Treat deployment-defined hardware profile names as opaque configuration.
Validate them once, preserve them through normalization, and do not infer
hardware identity from their spelling. When a platform needs expected-inventory
profiles or catalog changes, inspect `src/config/mod.rs` and add parsing,
validation, and default/configuration coverage.

## Runtime implementation and reuse

For every new `NodeType`:

- export its module from `src/nodes/mod.rs`;
- add the corresponding `NodeInstance` variant;
- update the exhaustive dispatch macro and `NodeInstance::from_config`;
- inspect every capability-specific constructor and dispatch method; and
- update topology, switch, firmware, and recovery routing where applicable.

Prefer a small, identity-preserving wrapper when an existing implementation
has the same transport behavior. The wrapper must report the new `NodeType`
and isolate vendor policy. Do not duplicate a complete implementation only to
create a distinct identity. Constructors must reject incompatible node types.

## Registered rack compatibility

On current `origin/main`, `src/racks/nvl_gb.rs` is the shared registered-rack
implementation for GB200, GB300, and VR NVL72. The filename is historical;
Vera Rubin support is a profile and wrapper in this file, not a separate
`nvl_vr.rs` module. Update this guidance if the module is renamed.

Inspect and update `src/racks/nvl_gb.rs` whenever the registered node-to-rack
compatibility set or rack behavior changes. Update
`src/orchestrator/rack_manager.rs` when a rack family changes. For each affected
family:

- accept only the declared `ProductFamily`;
- preserve rack inventory and serialized power-operation invariants;
- add focused positive creation coverage for each new node type or changed rack
  behavior at the appropriate node or rack layer; and
- preserve negative coverage for incompatible rack and node pairings.

Do not bypass rack validation to make a new type constructible.

## Capability audit

Audit every public operation that may receive the new type:

- descriptor and protobuf conversion;
- inventory and device information;
- power;
- firmware inventory, apply, activation, polling, and verification;
- the type-indexed catalog selection, filtering, target multiplicity, and apply
  mapping in `src/api/grpc/firmware_object_handlers.rs`;
- NVFWUPD profile, endpoint, and topology selection; and
- switch certificate, image, security, factory-reset, and scale-up paths when
  applicable.

Implement each applicable capability or return a deliberate unsupported error
and test it. A shared `NodeKind` or `ProductFamily` does not prove identical
capability behavior.

## Vendor-specific firmware workflows

When implementing or changing a platform-specific firmware workflow whose
package layout, target naming, activation, or flash order differs:

- validate eligible packages and filenames before starting a job;
- reject missing, ambiguous, or conflicting metadata;
- map logical components to device targets explicitly;
- preserve every required package through download and selection;
- define deterministic flash, activation, power-cycle, and verification order;
- propagate manifest expected versions into runtime verification;
- test sequencing, failure, cancellation, and post-flash verification; and
- recover a missing device task only through a narrow platform rule with an
  independently verified post-condition.

Do not guess broadly from filenames, swallow task errors, or add a persistence
migration solely because a `NodeType` was added.

## Tests and documentation

Add the layers that the behavior changes:

- canonical domain metadata tests;
- descriptor normalization and round-trip tests;
- concrete-node constructor acceptance and rejection tests;
- `NodeInstance` construction and capability-dispatch tests;
- rack-family acceptance and rejection tests;
- mocked Redfish or NVUE behavior tests;
- firmware selection, ordering, activation, and recovery tests; and
- gRPC end-to-end coverage when externally visible behavior changes.

Useful focused commands include:

```bash
cargo test -p rackmanagementservice --lib domain::node
cargo test -p rackmanagementservice --lib api::grpc::node_type_resolver
cargo test -p rackmanagementservice --lib nodes::instance
cargo test -p rackmanagementservice --lib racks::nvl_gb
```

Then run the repository-root pre-flight checks required by the parent
`AGENTS.md`.

Reconcile affected configuration and documentation, including as applicable:

- `docs/reference/hcl.md`;
- `docs/operations/firmware.md`;
- `docs/operations/inventory.md`;
- `docs/configuration/configuring-rms.md`; and
- `helm/` values, schema, examples, templates, and tests.
