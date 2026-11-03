# RMS Hardware Profile Configuration Layer

Status: Implemented

Last updated: September 20, 2026

## Summary

RMS supports a versioned hardware-profile layer that moves repeatable hardware
policy into YAML while retaining concrete Rust implementations for each public
hardware type. The profile configures an existing implementation; it does not
replace `NodeType`, `NodeInstance`, the `Node` trait, or protocol code.

Enabled profiles cover NVIDIA/Wiwynn/Dell GB200, NVIDIA/Lenovo/Supermicro GB300,
and NVIDIA VR NVL72 compute trays. They supply Redfish resource identifiers,
the NVFWUPD server profile, firmware target mappings, update order, polling,
activation, and verification policy. RMS still constructs each concrete
platform implementation and delegates device I/O to it and NVFWUPD.

This design gives RMS a controlled way to add or change platform policy without
turning YAML into an unrestricted programming language.

## Decision

Use a code-first, configuration-overlay model:

1. The incoming `NodeType` remains the canonical hardware identity.
1. `NodeInstance::from_config` selects a concrete Rust implementation.
1. An optional, validated `NodeRuntimeConfig` is passed to that implementation.
1. The same compiled hardware profile supplies firmware target and workflow
   policy to the shared job runner.
1. The concrete implementation and registered protocol drivers execute all
   hardware operations.

A new configuration file is sufficient only when the platform already has a
supported RMS identity and its behavior fits the existing typed configuration
and driver vocabulary. A new identity, protocol, or semantic operation still
requires Rust code.

## Motivation

Hardware update behavior was previously distributed across node-type matches,
firmware-object mapping tables, concrete node implementations, and NVFWUPD
defaults. Adding or changing a platform could require coordinated edits in
several places for what was conceptually one policy:

- mapping manifest components to logical and Redfish targets;
- selecting default and optional firmware components;
- optionally selecting packages by filename;
- defining flash order and activation boundaries;
- selecting Redfish resource identifiers and an NVFWUPD server family; and
- configuring polling and post-update verification.

Encoding all behavior in YAML would make firmware and power operations hard to
audit and unsafe to extend. Keeping all policy in Rust would preserve the
existing duplication. The configuration layer separates stable execution
semantics from platform-specific parameters.

## Goals

- Keep one canonical public identity for every supported hardware type.
- Preserve concrete platform implementations and vendor-specific overrides.
- Move repeatable target, ordering, timing, and resource policy into versioned
  files.
- Parse and validate profiles before RMS accepts requests.
- Execute only typed, immutable plans rather than raw YAML values.
- Keep built-in defaults reproducible while allowing an operator to replace the
  catalog at service startup.
- Preserve legacy behavior for node types that have not migrated.
- Make invalid or unsafe configurations fail closed.

## Non-goals

- Defining a general workflow programming language.
- Loading code, scripts, arbitrary HTTP requests, or arbitrary JSON expressions
  from a profile.
- Removing `NodeType` or dynamically creating public hardware identities.
- Moving credentials, IP addresses, certificates, or rack membership into
  hardware profiles.
- Replacing the `Node` trait, concrete nodes, Redfish clients, or NVFWUPD.
- Mutating the profile or runtime settings of a request or job already in flight.
- Claiming config-only support for a platform that requires new protocol or
  recovery semantics.

## Architecture

```text
                         startup
                            |
        built-in YAML or replacement profile directory
                            |
                            v
               parse, validate, and compile
                            |
                            v
         immutable Arc<HardwareProfileCatalog>
                   /                 \
                  /                   \
       runtime parameters          update policy
                |                       |
request ------> NodeConfig          manifest/request
  NodeType         |                       |
     |              v                       v
     +----> NodeInstance::from_config   stage planner
                    |                       |
                    v                       v
          concrete Node implementation <---+--- job executor
                    |
                    v
            Redfish/NVFWUPD/device
```

The catalog is the shared source of profile data for node construction and
firmware planning. `NodeType` selects code. The profile supplies parameters to
that code and policy to the shared executor.

## Responsibility boundaries

| Layer | Responsibilities |
| --- | --- |
| API and domain | Canonical `NodeType`, request normalization, credentials, inventory identity, and rack compatibility |
| Profile loader and compiler | YAML parsing, schema-version checks, safety validation, profile matching, target grouping, and typed plan construction |
| `NodeRuntimeConfig` | Validated settings understood by a concrete implementation, such as Redfish resource IDs and timeouts |
| Concrete `Node` implementation | Device I/O, endpoint ownership, power operations, firmware upload calls, task queries, and vendor-specific overrides |
| Firmware planner | Artifact matching, identity-preserving target resolution, duplicate-upload checks, stage ordering, and resolved activation/verification policy |
| Job executor | Progress, cancellation boundaries, update execution, task polling, activation, verification, and structured job results |
| NVFWUPD and protocol clients | Package handling, HTTP and Redfish mechanics, task-state interpretation, authentication, and TLS behavior |

## Profile sources and lifecycle

RMS supports two mutually exclusive profile sources:

- Built-in profiles under `hardware-profiles/v1` are discovered by the build
  script and embedded into the RMS binary in lexical filename order.
- When `workflows.hardware_profiles_dir` is configured, every `.yaml` and
  `.yml` file directly under that directory replaces the built-in catalog.

The external directory is a complete replacement, not an overlay. RMS reads,
parses, validates, and compiles the chosen catalog before starting the gRPC
server. A file-system error, parse error, validation error, duplicate profile
ID, or duplicate claimed node type prevents startup.

All platform profiles are optional, including NVIDIA GB200. Empty and partial
catalogs are valid. If a platform has no enabled profile in the selected catalog,
RMS uses its concrete implementation's code defaults and code-owned firmware
policy. Missing platform profiles in an external directory do not fall back to
embedded YAML, with one exception: Dell GB200 has no code-owned firmware policy
and always uses its embedded profile when the selected catalog has no enabled
profile for it.

The compiled catalog is immutable and shared as an `Arc<HardwareProfileCatalog>`
for the lifetime of the service. All requests, background jobs, and batch
children use that startup catalog through preparation, execution, activation,
and verification. Node construction receives runtime parameters from the same
catalog; registered nodes do not need to refresh their settings per request.

There is no filesystem watcher or runtime catalog replacement. Editing or
deleting profile files has no effect until RMS restarts. Changing the TOML
directory path also requires a restart; embedded profiles remain build-time assets.

## Compiled data model

Raw YAML is private to the configuration module. It is compiled into these
runtime concepts:

- `HardwareProfileCatalog`: indexes immutable plans by profile ID and canonical
  `NodeType`.
- `HardwareUpdatePlan`: contains identity, runtime configuration,
  polling, verification, ordered stages, and activation metadata.
- `NodeRuntimeConfig`: a typed enum carrying parameters understood by concrete
  node implementations.
- `FirmwareUpdateStage`: an ordered group of targets with one activation
  boundary.
- `FirmwareTargetPlan`: defines component selectors, artifact matching,
  Redfish targeting, and default selection.
- `FirmwareTargetResolution`: selected profile target ID and typed destination,
  carried alongside the logical component and package through execution.
- `ExecutableFirmwareStage`: resolved targets, optional activation action, and
  optional verification policy. Execution does not reinterpret profile policy.
- `RedfishPath`: a validated plain same-origin path shared by firmware and power
  configuration; strings are rendered at adapter boundaries.
- `PollPolicy` and `VerificationPolicy`: bounded timing and retry values.

The current runtime configuration variant is
`RedfishNvfwupdComputeConfig`. It contains:

- the NVFWUPD server profile;
- the Redfish `ComputerSystem` resource ID;
- the BMC `Manager` resource ID;
- the HMC `Manager` resource ID;
- optional direct-power and activation endpoint overrides;
- device-information and shared Redfish transport timeouts; and
- typed NVFWUPD compute transport, precondition, activation, and recovery timing.

Adding a fundamentally different implementation family requires schema support,
a new `NodeRuntimeConfig` variant, validation, and a concrete node implementation.

## Version 1 profile schema

| Section | Purpose |
| --- | --- |
| `schema_version` | Selects the parser contract. Only version `1` is accepted. |
| `profile_id` | Stable profile identifier used for lookup and job results. |
| `matches.node_type` | Exact canonical `NodeType` name, which selects the concrete node implementation. It references, but does not define, hardware identity. |
| `matches.enabled` | Allows the profile to compile without being selected for live requests. |
| `driver` | Provides typed runtime parameters for the implementation selected by `matches.node_type`. |
| `driver.nvfwupd_server_profile` | Selects NVFWUPD update/activation behavior without changing the concrete node implementation; need not match the node type's default. |
| `driver.redfish` | Supplies resource IDs, device-info deadline, and connection/request/direct-upload timeouts. |
| `driver.redfish.power` | Optional host state/reset, BMC/HMC reset, auxiliary-cycle, and recovery-readiness paths; omitted/null fields preserve discovery/defaults. |
| `driver.timing` | Groups NVFWUPD settings into `transport`, `pre_update`, `activation`, `recovery`, and `inventory_recovery`; omitted sections and fields retain defaults. |
| `firmware.polling` | Defines the device-task timeout, query interval, and transient-error budget. |
| `firmware.power_on` | Optional. Bounds the tray power-on that opens an update: wait for `On` and read interval. Will override the `[workflows]` `compute_power_on_*` keys for jobs that use the profile while it is enabled, once the power-on step lands. |
| `firmware.verification` | Enables and bounds post-activation inventory checks. |
| `firmware.targets` | Defines artifact matching, component selection, Redfish targets, and stage membership. |
| `firmware.activation` | Selects a registered activation strategy used at configured stage boundaries. |

Each target contains the following policy:

| Field | Behavior |
| --- | --- |
| `id` | Stable internal target identifier. |
| `component_key` | Canonical component selector exposed to firmware callers. |
| `source_components` | Manifest components that can supply the artifact. |
| `filename` | Optional suffix/substring predicates; all must match. Omitted or empty means no restriction. `case_insensitive` defaults to false. |
| `request_targets` | Explicit same-origin Redfish paths passed to the driver. |
| `driver_default_targets` | Lets the registered driver or package derive targets instead of using explicit paths. |
| `update_parameters` | JSON-compatible multipart UpdateParameters object. `Targets` or `HttpPushUriTargets` declares one upload with all listed paths, including an empty list. |
| `oem_parameters` | Opaque JSON-compatible multipart OemParameters object; omission omits the multipart field. |
| `default_apply` | Selects the target when the caller does not provide component filters. |
| `stage` | Positive integer used to order update groups. |
| `activate_after_stage` | Marks the stage as an activation boundary when the request enables activation. |

Exactly one destination form must be configured: `request_targets`,
`driver_default_targets`, or a `Targets`/`HttpPushUriTargets`
array in `update_parameters`. The two parameter target keys cannot be combined.
Other update and OEM parameters may accompany any destination form.

Parameter objects accept inline JSON or YAML mappings, including nested values.
Strings containing JSON are rejected. The request's `force_update: true` overrides
configured `ForceUpdate`; otherwise the configured value applies. Configured
`ApplyTime` overrides a driver default. Vendor fields remain opaque. See
[multipart parameters](../../hardware-profiles/README.md#multipart-parameters)
for examples and Lenovo image-type selection.

## Startup flow

1. RMS reads ordinary service configuration.
1. RMS selects the built-in catalog or `workflows.hardware_profiles_dir`.
1. Each profile is deserialized with unknown fields denied.
1. The compiler validates the schema and cross-field constraints.
1. Targets are grouped in ascending numeric stage order. Target order within a
   stage remains the YAML document order.
1. Plans are inserted into indexes by profile ID and `NodeType`.
1. The compiled catalog is shared immutably with the gRPC service.
1. RMS starts accepting requests.

Execution never consults raw YAML. Only startup parses the selected catalog;
all profile changes require a service restart.

## Node construction flow

1. A handler resolves the incoming API value or descriptor to a canonical
   `NodeType`.
1. The handler asks the catalog for an enabled profile matching that type.
1. If found, the handler copies the profile's typed runtime configuration into
   `NodeConfig.runtime_config`.
1. `NodeInstance::from_config` matches on `NodeType` and constructs the concrete
   Rust variant.
1. The concrete constructor consumes a compatible runtime configuration or
   uses its code-defined defaults when none is supplied.

Dell GB200 is the exception to step 2. Its firmware planning has no code-owned
policy, so a missing or disabled profile in the selected catalog resolves to the
embedded Dell profile instead of no profile. The request handler resolves every
Dell firmware target against that selected profile. `DellGb200Compute` rejects
unresolved targets rather than choosing a profile itself, so it cannot bypass
an enabled operator override.

For NVIDIA GB200, this always produces
`NodeInstance::ComputeGb200Nvidia(Box<NvidiaGb200Compute>)`. A YAML profile
cannot substitute another implementation or create a generic node instance.

Runtime configuration is currently attached to registered inventory nodes and
to request-local nodes created by firmware, inventory, and power operations.
Other node types continue to use their concrete implementations and legacy
policy unless they have an enabled profile. Dell GB200 always plans from a
profile: the enabled external profile, or the embedded one.

## Firmware planning and execution

### Artifact and target planning

The firmware-object path derives manifest mappings, allowed components, default
selection, and flash order from an enabled profile. The sanitized manifest
remains typed and platform-neutral; mappings are borrowed from the profile for
the requested `NodeType`, never merged into a global component-key map.
Inline apply, stored-object apply, metadata, and inventory comparison use this
same per-platform mapping. Artifact preparation and cache identity remain
independent of the profile, so changing a destination does not duplicate downloads.

Stored objects resolve the active service's startup profile when applied.
Objects prepared before versioned artifact catalogs must be deleted before
their manifests can be re-added. Save the manifest, hardware type, and default
selection first, then follow the migration steps in
[Firmware objects](../operations/firmware.md#firmware-objects).

Direct firmware requests are validated against the same compiled target catalog
during job planning. Manifest selection retains the compiled target reference
for filename matching, then carries its stable ID and typed
destination into the same execution representation as direct requests. It does
not recover target identity by searching transport strings.

Direct requests and firmware-object component filters select a configured target
only by its literal `component_key`. Matching ignores ASCII case and surrounding
whitespace. The target `id` is internal identity, `source_components` selects
manifest artifacts, and Redfish paths specify upload destinations; none supplies
an alternative request name. An empty request component does not select a
profile target, including when the transport uses an empty target list.

When a filename selector is configured, the filename must also satisfy it.
The GB200 profile omits filename selectors and selects by component; the
Supermicro profile retains ASCII case-insensitive selectors to distinguish
packages across stages. `filename.case_insensitive` is opt-in (default false).
These selectors do not validate file contents or hardware compatibility, and
omitting them does not bypass driver or safe file-path checks.
Each request artifact is consumed at most once. Unknown artifacts, explicit
filename-selector mismatches, direct requests with multiple `request_targets`,
and duplicate uploads fail planning before device I/O starts.
Duplicate detection within each configured target uses the resolved destination
and package. Manifest selection can fan out one package to distinct destinations. The logical component remains available to vendor code:
Lenovo BMC preparation uses logical BMC identity, while OEM parameters come
from the profile. `ImageType` defaults to `BMC`; operators set `HPM` explicitly
for `.hpm` images. Multipart target groups remain one upload, and each configured
destination is included in version verification.

RMS requires unique component keys, ignoring ASCII case, across the complete
profile regardless of stage, filename filters, or default selection. Shared
manifest source names and destination paths do not create request ambiguity.
The configuration error identifies the profile, duplicate component key, and all
conflicting target IDs; external-directory errors also identify the YAML file.

### Sequencing

The stage number is declarative ordering data:

1. The compiler sorts stages by ascending `stage` number.
1. The planner visits configured targets in YAML order within each stage.
1. Empty stages are omitted from a particular job.
1. Targets in the selected stage run sequentially.
1. If the API request enables activation and the stage has
   `activate_after_stage: true`, RMS runs the configured activation strategy
   after at least one target in that stage was applied.
1. RMS then performs the stage's resolved verification before continuing to
   the next stage.

The caller's activation flag remains a gate. A profile marks where activation
is allowed or expected; it does not currently force activation when the caller
sets the flag to false. Existing GB200 in-band NIC rules are an exception:
in-band updates still require activation, and manifest requests mixing NIC and
out-of-band targets retain the code-owned NIC-first activation boundary.
Version 1 profiles configure the out-of-band portion, not Flint execution.
Setting `firmware.verification.enabled: false` cannot disable mandatory in-band
NIC verification. A version or expected-device-count failure stops the job before
any later out-of-band stage.

### Execution

For each selected stage, the job runner:

1. Checks for cancellation at the workflow boundary.
1. Calls the concrete node implementation for each target.
1. Uses the profile polling policy when a device task is returned.
1. Records applied or skipped target results.
1. Runs activation at a selected stage boundary.
1. Runs installed-version verification according to the resolved stage policy.
1. Continues to the next stage or fails the job with a phase-specific result.

Per-node firmware jobs capture configuration provenance before asynchronous
execution or artifact preparation. Terminal results retain the configuration
source and RMS build identity on success, failure, cancellation, and worker
abandonment. Profile-backed results also include the profile ID, schema version,
and SHA-256 of the exact startup YAML bytes. Missing or disabled
profiles explicitly report `code_defaults`, except Dell GB200, which reports the
embedded profile it falls back to; an unknown node type reports
`unresolved`. Batch children carry their own identities. Queued/running jobs
log the selection; their existing status payloads are unchanged until terminal.
See [configuration traceability](../../hardware-profiles/README.md#configuration-traceability).

Profile polling overrides service-wide firmware task timing for that job. A
profile `firmware.power_on` block will override the service-wide power-on bounds
the same way once the power-on step lands in a following change. Until then the
block is validated and stored and nothing reads it. Without an enabled profile, the existing legacy
stage, activation, polling, and mapping behavior remains active for every
node type, including NVIDIA GB200. Dell GB200 has no legacy behavior and uses
its embedded profile instead.

## Concrete-code boundary

The following behavior remains in Rust and must not be supplied as arbitrary
profile data:

- `NodeType` identity, API conversions, and rack compatibility;
- credential handling, secret redaction, and TLS policy;
- endpoint ownership and same-origin enforcement;
- HTTP, Redfish, NVUE, SSH, SFTP, and IPMI transports;
- package parsing, multipart construction, and cleanup;
- dynamic Redfish discovery and response deserialization;
- device task-state interpretation;
- job persistence, concurrency, cancellation, and node operation locks;
- power-cycle mechanics;
- complex vendor preparation and recovery algorithms; and
- the concrete node implementations and registered activation strategies; and
- vendor-specific recovery behavior.

`NodeType` selects the concrete node implementation. Profiles configure its
runtime parameters and may choose a registered activation strategy. Unknown
activation strategies reject the profile.

### Power endpoint overrides

The profile compiler validates `driver.redfish.power` into request-local
`ComputePowerPaths`. The concrete compute node uses host-state and reset paths
for direct power requests; its NVFWUPD context carries the host-state/reset,
auxiliary-cycle, and readiness paths into activation and recovery polling.
The NVFWUPD adapter does not receive the direct-only BMC/HMC reset paths.
GB200 pre-update host readiness, post-activation readiness, Flint discovery
recovery, and OOB recovery pushes and power blocks all use the configured
workflow context. OOB recovery rosters remain separately configured through
`workflows.recovery_profiles`; their detection rules and recovery sequence stay
in the concrete node implementation. Public NVFWUPD free functions remain
compatibility wrappers using a default context.

Explicit paths take precedence over discovery and fail without falling back to
another resource. A configured host-state resource also supplies reset-action
discovery if no host reset path is set. Absent overrides preserve the existing
resource-ID lookups, NVFWUPD collection/action discovery, and `/redfish/v1`
readiness probe. Reset payloads, supported operations, ordering, retry conditions,
credentials, and TLS behavior remain code-owned.

GB200 activation retains the verified Off/On sequence. The profile configures
the auxiliary AC-cycle attempt budget and stabilization wait through
`driver.timing.activation.ac_cycle_attempts` and `ac_cycle_stabilization`, with
defaults of two total attempts and three minutes after each cycle. BMC-readiness
and power-on timeouts both use that cycle budget. Profile command retries,
power-state polling/resends, power-off delay, and readiness probes operate within
the recovery algorithm. The bundled profile's 60 power-state checks at five-second
intervals provide a five-minute window for each of Off and On; unconfigured
NVFWUPD callers retain the one-minute window.

See [compute power paths](../../hardware-profiles/README.md#compute-power-paths)
for the six fields and an example. All seven compute profiles retain discovery
by default; configurable paths do not qualify a new platform or recovery sequence.

## Validation and safety

Version 1 rejects profiles that violate any of these implemented rules:

- unknown schema versions, fields, node types, activation strategies,
  or NVFWUPD server profiles;
- empty, malformed, or duplicate profile and target identifiers;
- duplicate component keys across targets, including opt-in targets and disabled
  profiles;
- duplicate profile IDs or duplicate profile claims for one `NodeType`;
- non-compute node types, which version 1 does not support;
- empty target lists or stage number zero;
- inconsistent activation flags within one stage;
- activation boundaries without an activation strategy;
- empty or malformed Redfish resource identifiers;
- power paths or firmware destinations outside `/redfish/v1`,
  with URLs, escapes (including encoded traversal), whitespace, backslashes,
  query/fragment components, duplicate separators, or traversal;
- a target without exactly one destination form: `request_targets`,
  `driver_default_targets`, or an update-parameter target array;
- non-object parameter fields, invalid known parameter types, or conflicting
  `Targets` and `HttpPushUriTargets`;
- empty or path-containing filename predicates, duplicate source
  values, or invalid component selectors;
- zero required timeouts, invalid duration syntax, intervals larger than task
  timeouts, and configured durations longer than 24 hours;
- fractional-second new driver timings, attempt counts outside 1–1000, retry
  counts outside 0–1000, and power-on resend intervals shorter than polling.

The sample YAML documents each timing setting inline. See
[compute timing settings](../../hardware-profiles/README.md#compute-timing-settings)
for units, defaults, retry semantics, and conditional expected-inventory checks.
Task polling is not a whole-job deadline. Most recovery loops add individual
HTTP request time to their polling delays; GB200's verified Off/On loops also
enforce attempts multiplied by interval as an outer wall-clock timeout.
Transport and recovery algorithms remain code-owned even when their timing
parameters are configured.

Profiles contain platform behavior only. Credentials, addresses, certificates,
and artifacts continue to enter through existing RMS configuration and request
paths. Profile request targets are relative, same-origin paths so a profile
cannot redirect credentials to another authority.

## Failure and fallback behavior

| Condition | Result |
| --- | --- |
| Invalid built-in profile | RMS startup fails. |
| Missing or unreadable replacement directory | RMS startup fails. |
| Invalid replacement profile | RMS startup fails. |
| Duplicate profile ID or `NodeType` claim | RMS startup fails. |
| Profile files are edited or removed while RMS is running | No effect on running jobs or new requests; changes are read at the next startup. |
| Disabled profile | It is compiled and inspectable but not selected for requests. Dell GB200 uses its embedded profile instead. |
| No enabled profile for any node type | The concrete implementation's code defaults and code-owned firmware policy are used. Dell GB200 uses its embedded profile. |
| Profile and request node types disagree | Firmware planning fails before device I/O. |
| Unsupported target or package count | Firmware planning fails before device I/O. |
| Update, polling, activation, or verification error | The job fails with the corresponding phase and stops subsequent stages. |

There is no automatic retry with legacy policy after a selected profile fails.
Such a fallback could execute a different hardware sequence and would hide a
configuration defect.

## Current rollout state

| Platform | Concrete implementation | Profile state | Runtime behavior |
| --- | --- | --- | --- |
| NVIDIA GB200 compute | `NvidiaGb200Compute` | Enabled | Runtime parameters, firmware mapping, sequencing, polling, activation, and verification come from the profile. |
| Wiwynn GB200 compute | `WiwynnGb200Compute` | Enabled | Shared GB200 behavior; ordered multi-package BMC and code-owned Flint remain supported. |
| Dell GB200 compute | `DellGb200Compute` | Enabled; always applied | AMI host BMC, HMC, and SBIOS stages come from the profile. No legacy planner exists, so a missing or disabled override uses the embedded profile. |
| NVIDIA GB300 compute | `NvidiaGb300Compute` | Enabled | HMC/BMC and optional AP policy use the GB300 implementation. |
| Lenovo GB300 compute | `LenovoGb300Compute` | Enabled | HMC/BMC only; ordered multi-package HMC, OEM upload and preserve-config remain supported. |
| Supermicro GB300 compute | `SupermicroGb300Compute` | Enabled | Three stages; one BIOS upload targets both CPUs; BMC task-loss recovery remains code-owned. |
| NVIDIA VR NVL72 compute | `NvidiaVrnvl72Compute` | Enabled | Bundled HMC/BMC only; no individual AP targets. |
| Switches and power shelves | Existing concrete types | No profile | Existing code-defined behavior remains unchanged. |

All shipped compute profiles have automated mapping, selection, routing, and
workflow coverage. Only NVIDIA GB200 has physical profile-model validation;
the other platforms still need hardware qualification. Enabling a profile
does not imply that physical validation has occurred.

Supermicro declares one dual-CPU multipart upload in `update_parameters.Targets`.
Its default filename filters, HMC-to-BIOS activation boundary, and BMC stage
without an AC cycle are profile choices. Operators own the correctness of
custom selections and sequencing; Rust does not reimpose these defaults.
Narrow task-loss recovery still requires an independently verified installed
version at the configured destination. Lenovo upload/OEM parameters are also
configured; its preparation PATCH and readback remain in Rust.

Vendor fallback calls without a selected profile read upload/OEM parameter
values from the embedded YAML. Parameter values are not duplicated in vendor
wrappers.

Every eligible manifest payload is retained in order within its target.
Duplicate uploads still fail.

## Adding support for a platform

### Config-only addition

A platform can be added or migrated without production Rust changes when all of
the following are already true:

- its canonical `NodeType` and concrete `NodeInstance` implementation exist;
- its concrete node implementation supports the required transport and package behavior;
- its activation behavior matches a registered activation strategy;
- target selection can be expressed with the existing component and filename
  matchers;
- its sequencing can be expressed as ordered stages; and
- it needs no new preparation, recovery, or task-success semantics.

The change then consists of a profile, fixtures, conformance tests, and
documentation.

### Rust extension

Rust changes are required when the platform needs any of the following:

- a new public identity or API mapping;
- a new endpoint or protocol family;
- new package parsing or request construction;
- a new activation primitive;
- a new task-state interpretation;
- a new safe recovery algorithm; or
- configuration fields with semantics not represented by the versioned schema.

The preferred extension is a small typed implementation or strategy with the
required schema support, rather than platform-specific branches in several handlers.

## Testing strategy

Profile work requires coverage at four levels:

1. Parser and compiler tests reject malformed, unknown, unsafe, duplicate, and
   cross-field-invalid configurations.
1. Planning tests assert artifact matching, target rewriting, default
   selection, preservation of all eligible packages, stage order, and activation boundaries.
1. Concrete-node tests assert that `NodeType` still selects the correct variant
   and that runtime parameters override code defaults without replacing vendor
   behavior.
1. Mocked Redfish and gRPC tests assert the complete update, polling,
   activation, verification, cancellation, and failure flows.

The repository includes a profile compiler for local inspection:

```console
cargo run -p rackmanagementservice --example compile_hardware_profiles -- \
  hardware-profiles/v1
```

Every enabled profile should have parity tests against the behavior it replaces
before the corresponding legacy policy is removed.

## Operations

Operators can select a replacement catalog with:

```toml
[workflows]
hardware_profiles_dir = "/etc/rms/hardware-profiles/v1"
```

Operational requirements:

- manage the directory as release-controlled configuration;
- deploy only the desired overrides; empty and partial catalogs are valid;
- validate the directory with the same RMS version before rollout;
- deploy all profile files before starting RMS; do not edit them during startup;
- finish active firmware work before restarting to apply profile changes;
- restart RMS after changing profiles or the TOML directory path;
- retain the exact profile files with service deployment artifacts; and
- treat profile review with the same rigor as firmware orchestration code.

See [startup loading and restart behavior](../../hardware-profiles/README.md#directory-configuration-and-startup-loading).

## Known limitations and follow-up work

- Recovery algorithms remain code-owned. Their compute timing and retry
  parameters can be configured through `driver.timing`; compute power/recovery
  endpoints can be overridden through `driver.redfish.power`.
- Firmware jobs record exact YAML source hashes and RMS build identity, not a
  hash of all effective settings. Retain service TOML, request inputs, and the
  released RMS image alongside YAML for reproduction. Direct power/reboot jobs
  do not yet carry this firmware provenance metadata.
- The profile directory is an unsigned local trust boundary. Signing and
  provenance enforcement are not implemented.
- The external directory replaces the complete built-in catalog; layered
  overrides and inheritance are not supported.
- The request activation flag can suppress configured activation boundaries.
  Platforms that require activation between stages need an explicit
  precondition rule before their profile is enabled.
- Only the Redfish/NVFWUPD compute driver and full-compute power-cycle strategy
  are represented in the version 1 schema.
- Dry-run plan rendering through the public API and profile conformance tooling
  are not implemented.
- Profiles are loaded only at startup. Hot reload is intentionally unsupported.

## Acceptance criteria

The design is complete for broader migration when:

- every enabled profile reproduces the existing platform's update order,
  activation, verification, cancellation, and failure behavior;
- every firmware job records its selected YAML source identity or explicit code-default fallback;
- the planner rejects ambiguous profiles before startup or before any device
  operation;
- a required activation boundary cannot be accidentally suppressed; and
- adding a platform that fits an existing implementation requires no production Rust
  changes beyond any independently required public `NodeType` registration.

## Related files

- [Hardware profile implementation](../../crates/rackmanagementservice/src/config/hardware_profiles.rs)
- [Concrete NVIDIA GB200 node](../../crates/rackmanagementservice/src/nodes/compute_gb200_nvidia/mod.rs)
- [Concrete node dispatch](../../crates/rackmanagementservice/src/nodes/instance.rs)
- [Firmware job planning and execution](../../crates/rackmanagementservice/src/api/grpc/firmware_handlers.rs)
- [Firmware-object mapping](../../crates/rackmanagementservice/src/api/grpc/firmware_object_handlers.rs)
- [NVIDIA GB200 profile](../../hardware-profiles/v1/compute_gb200_nvidia.yaml)
- [Supermicro GB300 profile](../../hardware-profiles/v1/compute_gb300_supermicro.yaml)
- [Profile authoring guide](../../hardware-profiles/README.md)
