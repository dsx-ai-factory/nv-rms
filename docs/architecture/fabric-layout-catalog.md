# Fabric layout catalog

The catalog selects deployment-supplied link rules using the switch types and
physical topology reported by hardware. Rules describe supported fabric sizes,
coordinate grouping, administrative link targets, and optional controller static
settings. Wiring parameters and controller values are
deployment data rather than constants in RMS source code.

## Configuration

The optional `workflows.fabric_layout` section is loaded from RMS TOML.
Configuration loading rejects malformed definitions and ambiguous profile
selectors. Startup constructs one validated catalog and shares it with the gRPC
service and its clones. Omitting the section installs an empty catalog.

V2 layout configuration selects a catalog profile after discovering switch types
and physical topology. An omitted catalog returns `UNIMPLEMENTED`; unmatched
hardware or unsupported sizes fail before job admission. Hardware discovery,
device writes, and workflow coordination are outside the catalog module.

The following example uses synthetic positions and interface numbers. It is
not a hardware configuration.

```toml
[[workflows.fabric_layout.profiles]]
switch_node_types = ["switch_gb200_nvidia"]
compute_node_types = ["compute_gb200_nvidia"]
physical_topology = "synthetic-physical"
compute_tray_count = 2
switch_tray_count = 2
full_link_states = { "acp1-2" = "up", "acp4-5" = "up", "fnm1" = "up" }

[[workflows.fabric_layout.profiles.fabric_rules]]
compute_tray_count = 1
switch_tray_count = 1
controller_rules = [{ topology_type = "synthetic-topology", static_configs = [] }]

[workflows.fabric_layout.profiles.fabric_rules.grouping.compute]
node = "compute"
coordinate = "tray_index"
divisor = 1
multiplier = 1

[workflows.fabric_layout.profiles.fabric_rules.grouping.switch]
node = "switch"
coordinate = "tray_index"
divisor = 1
multiplier = 1

[[workflows.fabric_layout.profiles.fabric_rules]]
compute_tray_count = 2
switch_tray_count = 2
controller_rules = [{ topology_type = "synthetic-topology", static_configs = [] }]

[workflows.fabric_layout.profiles.link_calculation]
default_state = "down"

[[workflows.fabric_layout.profiles.link_calculation.rules]]
prefix = "acp"
base = 1
offsets = [0]
width = 2
state = "up"

[[workflows.fabric_layout.profiles.link_calculation.rules.terms]]
node = "compute"
coordinate = "tray_index"
divisor = 1
multiplier = 3
```

## Selection and grouping

A profile matches an exact physical topology string and a nonempty set of
observed switch types. Every observed type must be in the profile's switch
selector. Profiles with overlapping switch selectors for the same topology
are rejected. Compute selectors must name compute types; switch selectors must
name switch types.

Profile counts describe physical capacity. Each fabric rule supplies a supported
size within that capacity. Sizes must be unique, and the full-capacity size must
be present. Smaller sizes require grouping; the full-capacity rule omits it.

Grouping transforms each discovered compute or switch position independently.
Equal transformed values form a candidate group. Groups missing either role
are omitted. Missing remap entries and arithmetic errors fail grouping. The
caller must check group sizes, chassis identity, and request membership before
using a candidate.

The example joins compute tray 0 with switch tray 0 and compute tray 1 with
switch tray 1. Slot numbers do not affect these rules.

## Controller settings

Each fabric-size rule may declare `controller_rules`. These rules describe
static settings for the exact, case-sensitive `ScaleUpFabricConfig.topology_type`
supplied by the caller. The profile's `physical_topology` identifies hardware; the controller's
`topology_type` identifies the requested fabric topology. Neither string is parsed
to infer platform settings.

To manage a static setting, replace the first fabric-size rule's empty controller
rule with the following rule:

```toml
[[workflows.fabric_layout.profiles.fabric_rules.controller_rules]]
topology_type = "synthetic-topology"
static_configs = [
  { config_file_name = "fm_config", key = "SYNTHETIC_SETTING", action = { type = "set", value = "0" } }
]
```

Replace the full-capacity fabric-size rule's empty controller rule with the
corresponding restoration rule:

```toml
[[workflows.fabric_layout.profiles.fabric_rules.controller_rules]]
topology_type = "synthetic-topology"
static_configs = [
  { config_file_name = "fm_config", key = "SYNTHETIC_SETTING", action = { type = "remove" } }
]
```

`set` specifies a literal value, including `"0"`. `remove` specifies absence and
does not accept a value. Full-capacity rules may use either action, according to
the deployment's restoration requirements. These examples contain no platform
settings.

The union of file/key pairs across a profile's controller rules defines ownership.
Every controller rule must specify exactly one action for every owned pair. File
names and keys are case-sensitive and passed to the controller unchanged.
Missing actions, duplicate targets, and duplicate topology selectors within a
size are rejected. Every declared topology needs a full-capacity restoration rule.
Different fabric sizes may specify different values or removals.

Catalog validation and controller reconciliation share target-name and literal
validation. Ownership and topology restrictions are checked by the catalog.

File names must be basenames containing ASCII letters, digits, underscores,
periods, or hyphens, without a leading period. Keys contain ASCII letters, digits,
or underscores and cannot start with a digit. The dedicated topology setting
cannot also be a static target. Values and topology strings must be nonempty
unquoted literals without surrounding whitespace, comments, escapes, or control
characters. Unknown fields and actions fail deserialization.

Controller validation errors identify profile, fabric-rule, controller-rule,
and target indices where applicable, plus the invalid field. Indices are
zero-based; errors do not include configured identifiers or values.

Link-only profiles may omit controller rules from every size. Once a profile
declares controller rules, every size must have at least one. An explicit rule
with `static_configs = []` is valid when the profile owns no static keys.
The layout planner rejects a missing topology/size rule rather than inferring
controller settings. Catalog loading and validation do not change any devices.

## Layout planning

The V2 handler's `layout_planner` module accepts discovered node identities and
positions, a profile, requested fabric sizes, and controller configuration. It
returns selected node IDs and primaries, administrative link targets for every
supplied switch, and controller set/remove targets for each fabric. Planning
does not read devices, admit jobs, or change configuration. V2 layout requests
call the planner after discovery and before admitting a coordinated job.

Planning requires all switches in one physical rack, with unique node IDs and
tray positions. The shared rack identity comes from hardware topology-location
fields named `chassis-sn` or `ChassisSerialNumber`, not individual node serial
numbers. Compute inventory may be a subset of physical capacity, provided
the supplied nodes can fill the requested fabrics. Grouping and link calculations
come from the profile, not from platform-specific constants in the planner.
Requests exceeding the supplied compute or switch counts fail before candidate
calculation and placement search.

Candidates are sorted by discovered switch positions, then compute positions.
Selection preserves request order and finds disjoint groups that honor each
primary preference. Candidates containing another fabric's requested primary
are excluded before the search. Overlapping groups can cause selection to
reconsider an earlier group. Without a preference, the lowest-position switch in the selected
group becomes primary. The search checks candidates until it finds a complete
disjoint assignment or exhausts the available combinations. Empty candidate lists
fail before searching. Failed combinations of the next request and occupied nodes
are remembered so assignment permutations do not repeat the same failed search.
There is no fixed attempt limit; search time and cache size depend on candidate overlap.

For layouts below full capacity, unassigned switches receive `down` for every
controlled interface. One full-capacity fabric instead uses `full_link_states`
and its controller restoration rule. Compute inventory may be omitted for that
request; if supplied, every compute tray is required.

Controller targets are selected by exact topology and fabric size. Matching
caller settings are accepted, conflicts with owned values or removals fail,
and unmanaged caller settings are preserved. File names and keys retain their
exact casing. Missing rules, invalid coordinates, overlapping
assignments, and invalid static settings return an error without partial targets.

## Link targets and catalog identity

`full_link_states` defines both controlled interface names and their full-rack
targets. The calculator receives those names, sets rule-emitted interfaces,
and applies `default_state` to the others. For the example, compute tray 0
emits interfaces 1 and 2; compute tray 1 emits interfaces 4 and 5. The caller
chooses when to use calculated targets or full-rack targets.

Keys may be individual names (`acp1`, `fnm1`) or inclusive ranges (`acp1-4`,
`fnm1-2`). Individual names and ranges may appear in the same map. Each value
must be `"up"` or `"down"` and applies to every interface named by its key.

TOML accepts quoted and unquoted interface keys, including ranges:
`acp1-144 = "up"` and `"acp1-144" = "up"` parse identically and both expand to
`acp1` through `acp144`. Examples use quoted interface keys consistently;
quotes are optional.

Ranges use 1 lowercase `acp` or `fnm` prefix followed by decimal indices from
1 through 4294967295, without leading zeros. A range with equal endpoints is valid.
Whitespace in or around keys, reversed
ranges, wildcards, comma-separated lists, and overlapping entries are rejected.
For example, `"acp1-4" = "up"` and `"acp3" = "up"` overlap and fail loading.

When configuration sources are merged through Figment, ranges expand after the
merge. A later source may replace the state of an identical key. A different key
that overlaps an earlier entry fails loading; individual entries do not override
ranges. Every profile requires a nonempty `full_link_states`; RMS does not infer
missing targets from hardware or calculation rules. The 1,024-interface limit
applies to the expanded map. RMS checks each range's size before expanding it.

The loaded profile and its serialized form contain only individual interface
names. Equivalent range and individual-name configurations produce the same
catalog identity, calculated link targets, and full-rack restoration targets.

The catalog identity hashes the typed configuration. TOML comments, whitespace,
and unrelated RMS settings do not affect the identity. Changing a rule or link
target changes the identity, as does a controller setting or action. Omitting
controller rules or supplying empty lists preserves the link-only identity.

The catalog retains limits of 2 MiB of serialized configuration, 64 profiles,
256 total tray positions per profile, 256 fabric rules per profile, and 1,024
controlled interfaces per profile. Controller definitions are limited to 256
rules and 1,024 distinct owned file/key pairs per profile. The coordinate calculator has no work budget
or fixed rule-count limits. Calculation behavior is described in
[Fabric link calculation](fabric-link-calculation.md).
