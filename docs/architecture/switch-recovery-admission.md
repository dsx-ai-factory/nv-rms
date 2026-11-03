# Switch recovery admission

Switch firmware updates, password updates, and factory resets use the same
process-local reservations as fabric configuration. Admission reserves the switch's
known identities and rejects conflicting owners before submitting device mutations.
Job-owned reservations remain held through recovery
and credential fallback until the worker finishes.

## Firmware and factory reset

Firmware requests reserve known logical, NVUE, BMC, and SSH endpoint identities
before dispatch. The accepted worker reads physical placement and reserves that
identity when NVUE responds. Slow discovery does not delay RPC acceptance or
dispatch of other workers. Unavailable NVOS does not prevent BMC recovery;
discovered ownership conflicts still stop the worker before device writes.

Factory reset reserves registered endpoint identities before submission, including
when the reset must retry with default credentials. Endpoint-only admission cannot
detect a different, previously unknown address for the same physical switch.

## Password updates

Password updates reserve configured NVUE, BMC, and SSH identities before authentication. RMS checks the
current credentials and, for a retry changing the endpoint user, the requested
credentials. RMS adds a physical reservation when the authenticated client can
read chassis identity. Unavailable or incomplete chassis metadata does not block
password updates; conflicts with known or discovered identities do. Both NVUE
password updates and expired-password SSH recovery retain the known reservations.

## Fabric configuration

Active or unresolved fabric ownership blocks recovery mutations. An admitted
mutation invalidates cached controller-activation evidence for the affected
scope. A later fabric reconciliation must obtain fresh evidence rather than
reuse evidence collected before the reset, update, or credential change.
Admission conflicts report `UpdateInProgress`; transport and setup failures retain
their own classifications. An earlier successful admission check may invalidate
activation evidence even if a later check rejects the operation.
