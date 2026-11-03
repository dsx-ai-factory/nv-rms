# Saved link reconciliation

The layout link reconciler compares a desired administrative target with the
switch's effective, applied, and startup configuration. Matching targets return
without a write. Drift is passed to the switch adapter for apply and persistence,
then all 3 views are read again.

A failed initial read prevents writes. A successful adapter result is not enough
to report success: the saved readback must match the requested target. Unrelated
interfaces remain outside the target.

NVUE saves the complete applied configuration to startup, including unrelated
unsaved changes. The caller must account for those changes before reconciliation.

The caller holds switch reservations and coordinates controller shutdown before
changing connectivity. The helper does not select fabric members or start
controllers. Operational link health is independent of administrative state;
enabled links can remain down until compute endpoints become available.
