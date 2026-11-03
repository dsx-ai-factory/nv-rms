# Switch mutation admission

RMS reserves switch identities before changing fabric configuration, cluster
state, the gNMI service, or SDN configuration. V2 fabric configuration reserves
every supplied switch through reconciliation and rollback.

V2 admission reads switch identities concurrently and collects results in request
order. All reads finish before RMS reserves the complete set of switch aliases
and starts the configuration job.

Reservations use 3 kinds of identity:

- The caller's rack and node IDs.
- The chassis serial number and tray index read through NVUE.
- Management connection destinations, including supplied BMC and recovery
  endpoints when available.

A TLS hostname is not a physical identity. Endpoint reservations use the actual
connection destination and port. Requests with different node labels still
conflict when a physical identity or management endpoint matches.

Endpoints without an interface or a nonblank IP address contribute no reservation.
Their ports are not validated by admission. Host port 22 maps to NVUE port 443,
matching the connection RMS uses; BMC and supplemental endpoint ports are not remapped.

Direct mutations hold a reservation guard until the handler returns or is dropped.
Cancellation or a deadline releases that guard even if remote device work continues.
Background jobs retain their reservations until the worker finishes, including compensation.
Physical discovery adds another identity when NVUE can report chassis placement.
Missing or unreadable placement retains logical and endpoint reservations;
discovered identity conflicts still block mutation. Without physical identity,
RMS cannot detect different, previously unknown addresses for the same switch.

After reserving the switch, handlers check the shared fabric-operation registry.
An active or unresolved fabric operation blocks the mutation. An admitted
mutation invalidates previous controller-activation evidence for its scope.
Both registries are process-local; separate RMS processes do not coordinate
reservations.

Fabric-layout execution will use the same reservations while preparing shared
links and configuring several controllers. The admission changes do not enable
layout requests. Firmware, password recovery, and factory-reset fallback are
integrated in a separate recovery-admission change.
