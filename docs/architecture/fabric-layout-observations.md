# Fabric layout discovery and verification

The layout observation helpers read physical placement and check configuration
readback. They do not select fabric members or change devices. Layout admission
uses discovered positions for planning; the worker checks saved links and
controller observations before recording configuration completion.

## Discovery

Switch placement comes from NVUE chassis-location data. Compute placement comes
from the compute node's BMC device-information adapter. Caller node IDs identify
results, but do not supply chassis identities, tray indices, or slot numbers.
Missing placement and unsupported switch adapters fail discovery.

Device reads run concurrently and results retain input order. After failure,
panic, or cancellation, operations not yet started are skipped. All operations
may start before a failure is observed. Started operations finish before the
batch returns. Read errors include error categories without raw transport details.

## Verification

Link verification compares the requested administrative states with effective,
applied, and startup configuration. Matching effective defaults alone do not
satisfy an explicit saved target. Unknown or ambiguous states return an error.

Controller observations reuse the inspection RPC's NMX-C sampler and component
status interpretation. Configuration readiness, reported component status, read
errors, and membership checks remain separate observations.

Expected physical identities must be unique within each node kind. Live topology
must contain exactly the expected switch identities. When compute
inventory is supplied, observed GPUs must belong to that inventory. Missing GPU
endpoints do not fail the membership check, allowing control-plane configuration
before GPU software installation. Reported component status remains independent
of that membership decision.

These checks provide configuration and controller evidence. They do not prove
end-to-end communication or bandwidth.
