# Fabric operation tracking

`FabricOperationRegistry` records accepted fabric operations and verified
controller activations in memory. A coordinator can share one registry across
service clones.

## Ownership

An operation stores its job ID, scope aliases, rule identity, and controllers
whose activation has not been verified. Admission rejects overlapping scope
and duplicate job IDs.

Callers provide canonical scope keys and use the same spelling for admission,
activation, lookup, and invalidation. Keys are opaque, case-sensitive strings;
the registry sorts and deduplicates them but does not normalize endpoints.
Callers normalize endpoint addresses before constructing keys and preserve the
case of physical and logical identifiers.

A replacement job can take ownership only by naming the previous owner and
using the same scope and rule identity. Pending controllers transfer to the
replacement, and subsequent registry updates from the old job fail.

Job admission reserves devices while workers run. Before releasing failed
ownership, the coordinator drains started work and attempts controller cleanup.
The coordinator records the failed job afterward, releasing job reservations.
The registry does not inspect job state, reserve devices, or stop remote operations.

## Activation evidence

Before changing a controller, the caller marks activation pending. Marking
pending removes any earlier activation proof for that controller.
A controller pending under another operation rejects the whole batch without
changing pending entries or proofs. Repeating a pending mark for the same owner
is allowed.

After checking the device, the caller either records a successful activation
for the exact desired-state digest or clears pending activation after verifying
that the controller is disabled. Successful completion through `finish` requires
no pending controllers. Successful proofs survive operation completion.
Clearing an already-cleared pending entry succeeds for the current owner.

Each proof retains the aliases supplied for its fabric's member switches. The
coordinator supplies every member alias. Operation ownership still covers every
admitted switch, including unassigned switches. After ownership is released, a
mutation invalidates only proofs sharing the mutated switch's aliases.
Invalidation rejects empty lists, blank keys, and active or unresolved overlapping
operations before removing evidence. The caller must hold reservations while
invalidating evidence and changing devices.

## Lifetime and failure handling

Registry updates are serialized by a mutex; the mutex does not cover device I/O.
Lock poisoning returns an internal error instead of using potentially
inconsistent records.

The registry retains at most 256 operations and 1,024 activation proofs. All
operations count toward capacity, including those without pending controllers.
New disjoint operations are rejected at capacity; existing operations are never
evicted. Older activation proofs may be evicted; absence of a proof means unknown,
not successful activation.

After a terminal failure, the coordinator calls `release_failed` even if cleanup
could not verify shutdown. The registry removes that operation and any proofs
for its pending controllers, while preserving verified sibling proofs. Release
does not report successful activation or verified shutdown. The job remains
failed, with cleanup failures reported through job status and logs.

If an interrupted operation remains registered, hold its device reservations and use
`find_overlapping` to obtain its job ID and pending controllers. Confirm the old
worker has stopped. Under the retained owner, verify each pending controller and
call `clear_inactive` for verified shutdown or `record_activation` for verified
activation, then call `finish`. Alternatively, transfer ownership to a new job
with the same scope and rule before verification. Do not clear pending entries
solely because the worker stopped.

Process restart loses all records and proofs. A coordinator must rediscover
device state and verify activation before claiming convergence. The registry
does not persist job history or replace ordinary job status.
