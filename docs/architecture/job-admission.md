# Scoped job admission

The job registry admits jobs and direct mutations against identity aliases so
callers can coordinate operations that address the same device through different
request identifiers.

## Aliases and reservations

An alias identifies a logical node, a switch tray within a chassis, or a
management endpoint. Endpoint admission normalizes IP spelling, DNS case,
brackets, and trailing dots. Physical switch identities retain the tray index.
IPv4-mapped IPv6 addresses share their IPv4 endpoint alias. Scoped IPv6 aliases
normalize the address and preserve the zone ID verbatim; different zone IDs or
ports identify distinct endpoints. Endpoint aliases do not validate transport
support for a scoped address.

Admission checks every supplied alias before reserving any alias. Job state and
reservations share the registry write lock, preventing partially admitted target
sets. Logical aliases also check the existing active-node index.

A worker can attach additional aliases to an active job. The worker's existing
aliases remain reserved if another owner blocks the additional aliases.
Callers must provide all known aliases; admission does not discover physical
identity or resolve DNS aliases.

## Completion and cancellation

Job reservations are released on terminal transitions. Requesting cancellation
does not release reservations while the job handle remains active. Dropping an
unfinished handle records failure and releases its reservations.

A direct mutation retains its reservation guard for the complete device
operation. Dropping the guard releases every associated alias without retaining
a job record.

Each job domain supplies its own scope-conflict error. The reservation owner ID
may identify a direct mutation rather than a retained job.

`JobHandle::progress` reports whether the registry accepted the update. Missing
or terminal jobs reject progress without changing their recorded state or
timestamps.
