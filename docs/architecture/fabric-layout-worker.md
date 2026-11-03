# Fabric layout worker

The coordinated worker consumes a resolved plan and initialized switch clients.
The caller discovers inventory, validates deployment rules, and reserves every
switch before dispatch. The worker does not select fabric members.

The worker reads effective and saved links, cluster roles, controller settings,
and prior activation evidence before deciding which controllers need changes.
Matching controllers retain their activation evidence and avoid restarts.

Controllers affected by a change stop before shared connectivity changes.
If any link setting differs, the worker writes all requested link settings for
that switch. For example, keeping link 1 Up while changing link 2 to Down still
writes Up for link 1. Before a write that includes Up, the worker confirms that
NMX-C has stopped on a switch not selected as a primary. Current and saved cluster
settings can both be disabled while NMX-C is still stopping.

If shutdown cannot be confirmed, the worker does not apply the requested link
settings or activate controllers. Recovery may still disable links on controllers
that were successfully stopped.

Controller settings are staged while the selected primary is isolated. Every
switch's link target is saved and read back before any fabric is activated.
Independent fabrics then activate concurrently. Each selected primary must report
the requested settings, membership, and configured control plane before the
worker records its activation evidence.

Compute and operational link status are collected separately. Unavailable GPU
endpoints do not prevent a correctly configured control plane from completing.

For a full-rack configuration, RMS also reads the single-fabric status. If that
read fails, the job reports the error and keeps the partial status in its result.
The job fails, but already verified fabrics keep their activation records.
Link-read and controller startup or shutdown errors include the error category,
not raw NVUE response messages.

Cancellation or failure stops new dispatch and drains writes already running.
Verified fabrics remain active. RMS attempts to stop and isolate unverified
controllers, then releases fabric ownership before recording the failed job.
Job reservations remain held until that terminal outcome. Cleanup errors,
timeouts, or panics do not retain ownership; the failed job reports unverified
cleanup so another operation can attempt recovery. Releasing ownership does not
prove that devices stopped or configuration was restored. Pending controllers
have no valid activation proof; verified sibling proofs survive.

RMS restart loses ownership and activation evidence, so the next operation must
verify activation again. No operation state is written to the RMS database.

NVUE startup saves can also persist unrelated applied configuration. The caller
must account for those changes before dispatching the worker.
