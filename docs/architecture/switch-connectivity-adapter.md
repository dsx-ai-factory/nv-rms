# Switch connectivity adapter

The switch adapter provides administrative link configuration and controller
shutdown operations for workflows that coordinate switch connectivity. The
adapter does not select links or define a fabric layout.

## Link configuration

`SwitchScaleUpManagement` exposes link writes and a snapshot of 3 NVUE views:

- Effective configuration includes device defaults.
- Applied configuration contains explicit overrides.
- Startup configuration contains overrides saved for the next restart.

Link writes accept only expanded, numbered ACP and FNM interface IDs. RMS rejects
a batch containing an invalid ID before any device I/O.

Link writes apply only changed overrides and persist the revision. When applied
overrides already match but startup overrides differ, RMS saves the applied
configuration without creating another revision. A no-difference revision also
saves the applied configuration because another writer may have changed the
device after the initial read.

Capability calls serialize access with the switch operation lock. Link writes
do not verify operational link health; the caller checks readback separately.
Unsupported switch implementations return a capability error.

## Controller shutdown

Controller shutdown disables the cluster, verifies that the disabled state is
saved, and waits for the controller to stop. The convergence deadline includes
lock acquisition and device I/O. Node-server routing is preserved.

HTTP 404 establishes controller absence. Otherwise, shutdown requires a stopped
application, no contradictory ready observation, and a disconnected controller.
Read failures or unknown state do not establish shutdown.
