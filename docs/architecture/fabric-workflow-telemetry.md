# Fabric workflow telemetry

The `metrics::fabric_workflow` module provides timing scopes and progress logs for managed fabric reconciliation. RMS registers the collectors at startup. V2 layout jobs emit workflow and phase measurements; requests without a layout do not emit these measurements.

## Metrics

| Metric | Meaning |
| --- | --- |
| `rms_fabric_reconciliation_duration_seconds` | Elapsed seconds by `stage` and `outcome`. |
| `rms_fabric_reconciliations_active` | Open whole-workflow timing scopes. |

Stages are `workflow`, `inspection`, `preparation`, `activation`, `verification`, `readback`, and `recovery`. Outcomes are `success`, `failure`, `cancelled`, and `interrupted`. Labels do not include job IDs, device IDs, topology names, or configuration values.

The duration histogram records a sample when a timing scope ends. Dropping a scope without an explicit result records `interrupted`. That outcome does not establish whether a remote operation stopped or was rolled back. The active-workflow gauge is decremented when the whole-workflow scope ends, including during unwinding.

Callers keep the whole-workflow scope alive through readback and recovery. Whole-workflow and phase timings overlap and must not be added together. The histogram count reports completed scopes by outcome.

## Logs and cancellation

The `observe` helper logs a stage start, periodic wait messages, and a completion. Set `[workflows].fabric_config_wait_log_interval` to change the wait-log interval. The value is an integer in seconds and defaults to 30. RMS validates the interval at startup, and callers pass the resolved interval from `WorkflowTimings`. The value must be positive and fit the monotonic clock. Changes take effect after RMS restarts.

Wait messages include elapsed time and whether cancellation was requested. The interval affects progress logs only, not device polling, workflow timeouts, or metric recording. The helper keeps polling the same operation; the caller owns cancellation and recovery.

A successful result is recorded as `success`, even if cancellation was requested. An error is recorded as `cancelled` when the token is set, otherwise as `failure`. The `cancelled` outcome does not establish that cancellation caused the error.

Timing scopes retain the tracing subscriber and span so interruption logs keep their job context. Operation results are returned unchanged, and error bodies are not copied into telemetry events.
