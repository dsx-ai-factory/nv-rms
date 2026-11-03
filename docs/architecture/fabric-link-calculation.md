# Fabric link calculation

`FabricLinkCalculation` calculates administrative link targets from
deployment-supplied rules and discovered compute and switch positions. The calculator
does not read hardware, select fabric members, or change switch configuration.
V2 layout planning uses these rules from `workflows.fabric_layout` in RMS TOML.
See [Fabric layout catalog](fabric-layout-catalog.md) for the
configuration format and validation boundaries.

Platform wiring parameters are deployment data. The calculator contains no
platform-specific interface numbering or connectivity tables. The example below
uses synthetic interface names and numbers.

## Rule format

`FabricLinkCalculation` supports TOML deserialization through Figment, which RMS
already uses. Call `validate()` after loading rules. `expand()` also validates
the rules before calculation.

```toml
default_state = "down"

[[rules]]
prefix = "port"
base = 1
offsets = [0, 20]
width = 2
state = "up"

terms = [
  { node = "compute", coordinate = "tray_index", divisor = 1, multiplier = 2, remap = { "3" = 3, "4" = 4 } },
  { node = "switch", coordinate = "tray_index", divisor = 1, multiplier = 1 },
]
```

For each selected compute/switch pair, each rule emits numbered interfaces:

```text
prefix + (base + sum(transformed_coordinate * multiplier) + offset + lane)
```

Coordinates are used as supplied; the calculator does not renumber positions.
Each term requires `node` (`compute` or `switch`) and `coordinate`
(`tray_index` or `slot_number`). The term reads that position field, divides by
`divisor`, optionally applies `modulus`, optionally looks up the result in
`remap`, then multiplies by the signed `multiplier`. Division and remainder use
unsigned integer arithmetic. Each offset emits `width`
consecutive interfaces, starting with lane 0.

For compute tray 3 and switch tray 2, the example emits `port9`, `port10`,
`port29`, and `port30`. Slot numbers do not affect this example.

## Inputs and results

The caller supplies the participating positions and a set of controlled interface
names. The result contains every controlled interface
for every supplied switch. Rules set emitted interfaces to their specified state;
all other controlled interfaces receive `default_state`.

The calculator does not infer membership or validate chassis identity. The caller
must supply positions from the same physical chassis and the intended compute and
switch groups. Restoration targets and device readback belong to the caller.
When integrating with a restoration map, derive the interface-name set from that
map's keys; the calculator does not need a second configured interface list.

## Validation

Unknown fields, node kinds, coordinates, and states fail deserialization.
Missing node or coordinate selectors also fail deserialization. The shared
node-kind type includes `powershelf`, which calculator validation rejects.
Rules, offsets, inventory, and controlled interfaces must be nonempty. Widths, divisors, and
moduli must be positive. Remaps must be nonempty, with canonical decimal `u32`
keys. Prefixes contain lowercase ASCII letters.

Expansion uses checked arithmetic and rejects missing remaps, interface numbers
outside the positive `u32` range, uncontrolled interfaces, and duplicate
assignments across rules or compute inputs, even when their target states match.
A failed expansion returns no partial targets and leaves caller inputs unchanged.
Validation and expansion errors identify the rule and field, such as
`rules[2].terms[0].divisor`, without including coordinate values, remap contents,
or interface names. Indices are zero-based.

The calculator has no fixed count limits or work budget. Work and memory usage
depend on the supplied inventory, controlled interfaces, and calculation rules.
Callers are responsible for applying resource policies appropriate to their
execution environment. No device state is changed by calculation or validation.
