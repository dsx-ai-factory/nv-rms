# RMS orchestrator

The orchestrator is the single entry point for RMS business logic. It owns two
long-lived concerns:

- **The rack registry** (`RackManager`) — the in-memory map of managed racks.
- **Asynchronous job tracking** (`job_lifecycle` + `job_tracker`) — the machinery
  that runs long operations (firmware updates, switch image/certificate/password
  updates, SDN factory-default resets, scale-up fabric configuration) as
  background jobs that clients start and then poll for status.

This document collects the diagrams and design notes that are too large to keep
inline in the source doc comments.

## RackManager registry locking

The `RackManager` owns the rack registry — a `RwLock<HashMap<String, Arc<ManagedRack>>>`.

### `with_rack_for_node`

`with_rack_for_node(rack_id, rack_type, add)` looks up (or lazily creates) the
rack for `rack_id` and runs the `add` closure against it, atomically under the
registry write lock.

- `add` receives the canonical rack: the already-registered instance when one
  exists, otherwise a freshly built rack.
- A newly built rack is inserted only after `add` succeeds, so a failed `add`
  never leaves an empty rack behind.
- The rack is constructed lazily inside the write lock and only on the vacant
  branch, so no instance is ever built speculatively and discarded on a race.
  Concurrent creators of the same new `rack_id` all converge on the single
  instance built by the winner.

Validation is split from construction so the only fallible step
(`RackKind::parse`) runs before the lock, while `RackKind::build` is infallible
and runs only on the insert path.

#### New rack, `add` succeeds

The rack is built exactly once, then inserted.

```mermaid
sequenceDiagram
    participant C as caller
    participant W as with_rack_for_node
    participant M as registry map (RwLock)

    C->>W: with_rack_for_node(id, ty, add)
    Note over W: RackKind::parse(ty) -> Ok (no lock yet)
    W->>M: acquire write lock
    Note over M: locked
    Note over W: entry(id) => Vacant
    Note over W: rack = kind.build(id) — built once
    W->>W: add(&rack) => Ok(v)
    W->>M: slot.insert(rack) — map now holds {id: rack}
    W->>M: release write lock
    Note over M: free
    W-->>C: Ok(v)
```

#### Existing rack

The registered instance is reused in place — never rebuilt, never re-inserted.

```mermaid
sequenceDiagram
    participant C as caller
    participant W as with_rack_for_node
    participant M as registry map (RwLock)

    C->>W: with_rack_for_node(id, ty, add)
    Note over W: RackKind::parse(ty) -> Ok
    W->>M: acquire write lock
    Note over M: locked
    Note over W: entry(id) => Occupied
    Note over W: ensure_rack_type_matches
    W->>W: add(existing) => Ok(v) — no build, no insert
    W->>M: release write lock
    Note over M: free
    W-->>C: Ok(v)
```

#### New rack, `add` fails

Nothing is persisted: the `VacantEntry` is dropped and the rack is never
inserted.

```mermaid
sequenceDiagram
    participant C as caller
    participant W as with_rack_for_node
    participant M as registry map (RwLock)

    C->>W: with_rack_for_node(id, ty, add)
    W->>M: acquire write lock
    Note over M: locked
    Note over W: entry(id) => Vacant
    Note over W: rack = kind.build(id)
    W->>W: add(&rack) => Err(e)
    Note over W: return Err — VacantEntry dropped, rack not inserted
    W->>M: release write lock
    Note over M: free, unchanged
    W-->>C: Err(e)
```

#### Concurrent creators of the same new rack

The write lock serializes the two threads, so exactly one takes the `Vacant`
arm and the other finds `Occupied` and reuses that instance. Only thread A ever
calls `build`.

```mermaid
sequenceDiagram
    participant A as thread A
    participant B as thread B
    participant M as registry map (RwLock)

    Note over A,B: both call with_rack_for_node(id, ty, ...)
    Note over A: parse(ty) -> Ok
    Note over B: parse(ty) -> Ok

    A->>M: acquire write lock (wins)
    Note over M: LOCKED by A
    B->>M: acquire write lock (blocks)
    Note over B: parked

    Note over A: entry(id) => Vacant
    Note over A: rack = build(id) — the ONLY build
    A->>A: addA(&rack) => Ok
    A->>M: slot.insert(rack) — map now holds {id: rackA}
    A->>M: release write lock
    Note over M: FREE

    M-->>B: lock granted
    Note over M: LOCKED by B
    Note over B: entry(id) => Occupied
    Note over B: ensure_rack_type_matches
    B->>B: addB(&existing) => Ok — reuses rackA
    B->>M: release write lock
    Note over M: FREE

    Note over A,B: both return Ok, both referencing rackA
```

Because `build` runs only inside the `Vacant` arm while the write lock is held,
thread B never constructs a second rack to discard — contrast a
build-before-lock design, where B would build `rackB` and throw it away after
losing the race.

## Job tracking model

Long RMS operations do not block their initiating RPC. A gRPC handler validates
the request, starts a job, and immediately returns a job ID; the client then
polls `GetJobStatus` (or a legacy per-workflow status RPC) until the job reaches
a terminal state. Work runs on a supervised Tokio task, and the job record lives
in an in-memory registry.

The model is split into two layers so the tricky lifecycle mechanics are written
once and reused by every workflow:

- **`job_lifecycle`** — generic, domain-agnostic primitives, parameterized over a
  `JobDomain`. It owns the registry, the RAII guards, the supervisor, and
  cleanup.
- **`job_tracker`** — the RMS-specific facade. `JobTracker` is what handlers call;
  `RmsJobDomain` plugs RMS's neutral error codes (`JobError`) and fallback
  messages into the generic layer, and the tracker adds Prometheus metrics and a
  TTL reaper.

### Layers

```mermaid
flowchart TD
    H["gRPC handlers (firmware, switch image, cert, password, SDN, scale-up)"]
    T["JobTracker (facade + Prometheus metrics + reaper)"]
    D["RmsJobDomain (JobDomain impl: neutral JobError codes)"]
    R["JobRegistry (generic, Arc + short-held RwLock)"]
    ST["Job store + active-node index + parent/child links"]
    J["JobHandle (owned RAII guard)"]
    JH["JobJoinHandle (supervisor task)"]
    CP["CleanupPlan"]

    H --> T
    T --> R
    T -.- D
    R --> ST
    R -->|"create_job"| J
    J -->|"move into spawn_job"| JH
    JH -->|"on worker exit or panic"| CP
```

### Key components

- **`JobRegistry<D>`** — the `Arc`-shared store behind a short-held `RwLock`.
  Guards are never held across `.await`; async work clones what it needs, drops
  the lock, then awaits. Enforces a capacity cap (`MAX_TRACKED_JOBS`, 10,000) and
  a terminal-job TTL.
- **`JobDomain`** — trait supplying the failure constructors the generic layer
  needs (`dropped_failure`, `at_capacity_failure`,
  `node_busy_failure`, `shutting_down_failure`,
  `parent_unavailable_failure`).
  `RmsJobDomain` is the single implementation shared by all RMS workflows.
- **`JobSpec`** — creation input for queued node jobs and running top-level
  jobs (rack/node IDs, initial description, tracing span, optional `JobType`).
- **`JobHandle`** — the owned RAII guard returned for every created job.
  Workflows move it into the task that owns the work. `progress`, `complete`,
  and `fail` transition state; dropping it while still non-terminal marks the
  job failed as abandoned via `dropped_failure`.
- **`JobJoinHandle`** — `#[must_use]` supervisor handle. `wait()` observes the
  final state and cleanup; `detach()` lets fire-and-forget handlers continue while
  the supervisor still runs cleanup.
- **`CleanupPlan` / `CleanupReport`** — post-worker cleanup (e.g. removing a
  temporary artifact directory), run in a supervised, span-instrumented task even
  when the worker panics. Reports one of `Skipped`, `Removed`, `Failed`, or
  `Panicked`.
- **`JobType`** — externally visible workflow classification used for spans and
  metrics: `FirmwareUpdate`, `SwitchCertificate`, `SwitchMtlsDisable`, `SwitchSdnFactoryDefaultReset`,
  `SwitchSystemPasswordUpdate`, `SwitchSystemImageUpdate`,
  `ConfigureScaleUpFabricManagerV2`.
- **`JobError`** — domain-neutral failure classification (internal, timeout,
  client/server error, unauthenticated, target/file not found, ...). `conversions`
  maps it to the protobuf error enums for the status RPCs.

### Job states

Leaf jobs start `Queued`; parent jobs are created already `Running`. `Completed`
and `Failed` are terminal and final for ordinary transitions. Consuming the
handle with `complete` or `fail` prevents its destructor from recording an
abandoned failure.

```mermaid
stateDiagram-v2
    [*] --> Queued: create_job for leaf
    [*] --> Running: create_job for parent
    Queued --> Running: progress() / mark_running
    Queued --> Failed: fail() / drop / mark_failed
    Running --> Completed: complete() / mark_completed
    Running --> Failed: fail() / drop / mark_failed
    Completed --> [*]: reaped after TTL
    Failed --> [*]: reaped after TTL
```

### Job lifecycle: spawn to cleanup

```mermaid
sequenceDiagram
    participant Client as gRPC client
    participant Handler as gRPC handler
    participant Tracker as JobTracker
    participant Reg as JobRegistry
    participant Sup as supervisor task
    participant Worker as worker future

    Handler->>Tracker: create_job(spec)
    Tracker->>Reg: create_job => JobHandle (Queued)
    Tracker-->>Handler: JobHandle (internal return)
    Handler->>Tracker: spawn_job(handle, worker_fn)
    Tracker->>Sup: spawn supervised task
    Handler-->>Client: response containing job_id
    Sup->>Worker: run worker(JobHandle)
    Worker->>Reg: progress("...") => Running
    Worker->>Reg: complete(description, json) => Completed
    Note over Worker: return, panic, or abort without complete/fail<br/>drops JobHandle => Failed (abandoned)
    Sup->>Sup: run CleanupPlan (even on panic)
    Note over Sup: CleanupReport logged in the job span
    Handler->>Tracker: get_job(job_id) (client polls)
    Tracker-->>Handler: JobInfo snapshot
```

### Parent/child jobs and admission control

- **Batch (parent) jobs.** A parent aggregates several child leaf jobs so a batch
  request (e.g. update firmware on many nodes) has one status handle. A child may
  belong to at most one parent; the parent's status is derived from its children.
- **Node-idle admission.** `create_job_if_node_idle` (backed by
  `JobRegistry::create_job`) admits a job only when no other active job targets
  the same `rack_id` / `node_id`, using an active-node index over queued and
  running leaf jobs. This serializes mutating operations per node. Cancellation
  keeps the node busy until the job actually transitions to a terminal state.
- **Capacity and retention.** New jobs are refused with an at-capacity failure
  once the registry is full. Terminal records are retained for the configured TTL
  (so late pollers still see the result) and then removed — by admission-time
  pruning under capacity pressure and by the background reaper started with
  `spawn_reaper`.
- **Shutdown.** After `begin_shutdown`, the registry refuses new jobs with a
  shutting-down failure while letting in-flight work drain.

### Locking, cancellation, and observability

- **Locking.** Registry methods take short synchronous `RwLock` guards and never
  hold them across `.await`. A poisoned lock is recovered rather than propagated.
- **Cancellation.** Each job carries a `CancellationToken`; `cancel` signals it,
  but the node stays busy until the worker records a terminal (cancelled) state.
- **Observability.** Every job runs inside its own tracing span, so worker logs,
  cleanup panics, and leaked resources are all attributable to a `job_id`.
  `JobType` drives per-workflow Prometheus counters and gauges.
