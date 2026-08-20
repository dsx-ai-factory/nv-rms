# Operations Overview

RMS exposes a single gRPC service, **`RackManager`**, whose service definition
comes from the external [`librms`](https://github.com/NVIDIA/nv-rms-client) crate.
The RPCs are documented by capability across the pages in this section:
[Power Control](power-control.md), [Inventory](inventory.md),
[Firmware](firmware.md), [Switch Management](switch-management.md), and
[Utility](utility.md).

This page covers the [conventions](#conventions) shared across all of them and
[the async job model](#the-async-job-model) behind the long-running RPCs.

## Conventions

Two conventions run through the whole API:

- **In-band failures.** Business and validation failures are returned as a
  `Failure` status *inside* a successful gRPC response, not as a gRPC error code.
  RMS returns a real gRPC error (`INVALID_ARGUMENT`, `UNIMPLEMENTED`) only when
  rejecting an invalid enum or an unimplemented method.
- **Registered vs. ephemeral targets.** Some RPCs act on nodes previously
  registered with `CreateNodes` (looked up by `rack_id` / `node_id`). "Batch" RPCs
  act on caller-supplied **ephemeral** nodes whose endpoints and credentials
  arrive in the request and are never persisted - matching RMS's
  [stateless model](../overview.md#stateless-with-respect-to-inventory).

Long-running RPCs return a **job ID** immediately and are polled to completion;
they are marked **async** below. Everything else completes inline (**sync**).

## The async job model

Every asynchronous RPC shares one in-memory **`JobTracker`**. Job IDs are UUIDs.

### Lifecycle states

A job moves `Queued` → `Running` → a terminal `Completed` or `Failed`. Terminal
states are never overwritten. A failed job carries a typed error (e.g.
`ClientError`, `Timeout`, `FileNotFound`, `TargetNotFound`, `Unauthenticated`,
`UpdateInProgress`, `InvalidArgument`) as its root cause.

If a worker task exits without recording a terminal state, an RAII guard seals the
job `Failed` ("job task exited before recording a terminal state"); a panic or
abort is likewise sealed as failed. Worker-recorded terminal state is never
clobbered by these fallbacks.

### Node exclusion and capacity

A new job for a `(rack_id, node_id)` is refused while a non-terminal job already
targets that node (`UpdateInProgress`) - this is the "node busy" rejection surfaced
by the update RPCs. The registry retains at most `max_tracked_jobs` records
(default 10,000); at capacity, new job creation fails until cleanup reopens space.
During shutdown, new jobs are refused so RMS can drain in-flight work.

### Parent / child batches

Every batch RPC creates the **parent** job first, then dynamically attaches one
**child** job per admitted node before starting any child work. The batch
response's `job_id` is the parent's. Any non-terminal top-level job can accept
children; `Completed` and `Failed` jobs are sealed against further attachment.
Each child's `parent_job_id` is fixed when that child is created; only the
parent's `child_job_ids` grows as children are added. A job is reported as a
parent once its `child_job_ids` is non-empty.

Parent state is **aggregated on read** (and on every reaper pass, so a batch
whose children all finish still reaches a terminal state even if never polled):

- All children terminal with no failures → parent `Completed`.
- All children terminal with any failure → parent `Failed`, with a description
  like `Batch complete: {completed}/{total} succeeded, {failed} failed` and a
  `result_json` listing the failed children.
- Otherwise → parent `Running` with a progress description.

`GetJobStatus` returns the parent plus each child; the firmware and switch-image
status RPCs return the single, parent-aggregated job.

### Retention and cleanup

Terminal jobs are retained for `terminal_job_ttl_seconds` (default 24h). A
background reaper sweeps on an interval sized to the TTL (clamped to 1-300s): it
refreshes parent state, evicts expired terminal jobs, and prunes bookkeeping.
Eviction is child-protected - a parent is reaped only after its children - so
polling a parent never races cleanup. Ephemeral apply paths
(`ApplyFirmwareObject`, `ApplySwitchSystemImage`) additionally register a cleanup
plan that deletes their temporary artifact cache once all children reach a
terminal state.
