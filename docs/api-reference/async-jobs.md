# Async jobs

Some RPCs return before the work finishes. They return a job ID, and you poll a status RPC until the job
reaches a final state. This page explains how to tell which RPCs behave this way, how to poll them, and
what the job states and errors mean.

## Execution modes

The [RPC catalog](generated/rpc-catalog.md) lists an execution mode for every RPC.

| Mode | What the response carries | What you do next |
| --- | --- | --- |
| Synchronous | The full result | Read the response. No job exists. |
| Asynchronous, one job | One `job_id` | Poll the RPC in the **Poll with** column. |
| Asynchronous, parent and child jobs | A `NodeBatchResponse` whose `job_id` is the parent job, plus a result for each node that RMS refused | Poll the parent job. RMS creates one child job for each admitted node. |
| Synchronous, with background work | The full result of the foreground step | Read the response. The RPC page describes the work that continues. |

The response shape alone doesn't tell you the mode, because synchronous batch RPCs also return a
`NodeBatchResponse`. Rely on the catalog, or read the **Execution** row on the RPC page.

## Poll a job

RMS provides four status RPCs. Use the one that the catalog names for your RPC.

| RPC | Use it for | If the job doesn't exist |
| --- | --- | --- |
| [`GetJobStatus`](generated/utility-jobs.md#getjobstatus) | Any job that RMS tracks. Set `include_child_job_states` to also return each child. | gRPC error `NOT_FOUND` |
| [`GetFirmwareJobStatus`](generated/utility-jobs.md#getfirmwarejobstatus) | Firmware update jobs and their parent batch jobs | `RETURN_CODE_FAILURE` in the response |
| [`GetSwitchSystemImageJobStatus`](generated/utility-jobs.md#getswitchsystemimagejobstatus) | Switch system image update jobs | `RETURN_CODE_FAILURE` in the response |
| [`GetConfigureSwitchCertificateJobStatus`](generated/utility-jobs.md#getconfigureswitchcertificatejobstatus) | Switch certificate configuration jobs | `RETURN_CODE_FAILURE` in the response |

`GetJobStatus` returns a list of [`JobStatus`](generated/types-rack-manager.md#jobstatus) messages.
When you query a parent job, the parent comes first in the list. The other status RPCs return the
single job as an aggregate of the parent and its children, and carry their own state and error enums.

### Job states

A job moves through the states of
[`JobExecutionState`](generated/types-rack-manager.md#jobexecutionstate):

1. `JOB_EXECUTION_STATE_QUEUED`: RMS created the job and it hasn't started.
1. `JOB_EXECUTION_STATE_RUNNING`: The job is executing.
1. `JOB_EXECUTION_STATE_COMPLETED` or `JOB_EXECUTION_STATE_FAILED`: The job is final. RMS never
   changes a final state.

Stop polling when you see a final state. Read `state_description` for a human-readable progress
message while the job runs.

### Poll in a loop

This Bash example polls a job with `GetJobStatus` every five seconds until it finishes. It needs
[`jq`](https://jqlang.org/), and it assumes you defined the variables in
[Getting started](getting-started.md#call-an-rpc-with-grpcurl). `grpcurl` prints protobuf JSON with
lower camel case field names, such as `jobStates`, so the `jq` expression reads those names and
accepts the snake case names as a fallback.

```bash
set -o pipefail
job_id="<job_id>"

while true; do
  state=$(grpcurl $RMS_GRPCURL_FLAGS -d "{\"job_id\": \"$job_id\"}" \
    "$RMS_ADDR" rack_manager.RackManager/GetJobStatus \
    | jq -r '(.jobStates // .job_states)[0] | (.executionState // .execution_state)') \
    || { echo "Polling failed" >&2; break; }
  echo "$state"
  case "$state" in
    JOB_EXECUTION_STATE_COMPLETED|JOB_EXECUTION_STATE_FAILED) break ;;
  esac
  sleep 5
done
```

After the loop, call the status RPC once more and read `result_json` and, for a failed job,
`error_code` and `error_message`.

## Read the result

For a job that reaches `JOB_EXECUTION_STATE_COMPLETED`, `result_json` holds a JSON document with
detailed results. Its schema depends on the workflow that created the job, so the RPC page describes
it where it's verified.

For a failed job, `error_message` is always set, and `error_code` is a
[`JobError`](generated/types-rack-manager.md#joberror) value. Common values include:

| `JobError` value | Meaning |
| --- | --- |
| `JOB_ERROR_INVALID_ARGUMENT` | The arguments that started the job aren't valid. |
| `JOB_ERROR_CLIENT_ERROR` | RMS couldn't communicate with the device over HTTP or Redfish. |
| `JOB_ERROR_TIMEOUT` | An operation didn't complete in time. |
| `JOB_ERROR_UNAUTHENTICATED` | The credentials couldn't authenticate. |
| `JOB_ERROR_TARGET_NOT_FOUND` | The target component wasn't found on the node. |
| `JOB_ERROR_FILE_NOT_FOUND` | The firmware file doesn't exist. |
| `JOB_ERROR_UPDATE_IN_PROGRESS` | The node already has an active job. |

The firmware status RPC reports the equivalent
[`FirmwareUpdateError`](generated/types-rack-manager.md#firmwareupdateerror) enum in its `error_code`
field.

## Parent and child jobs

A batch RPC creates a parent job and then one child job for each node that it admits. The `job_id` in
the response identifies the parent. RMS derives the parent state from its children:

- The parent completes when all children complete without a failure.
- The parent fails when all children are final and at least one failed. Its `result_json` lists the
  failed children.
- Otherwise the parent keeps running, and its `state_description` shows progress.

Poll the parent. To see each node, call `GetJobStatus` with `include_child_job_states` set to `true`.
Each child has a `parent_job_id`, and the parent lists its children in `child_job_ids`.

## Node exclusion

RMS allows one active job for each node at a time. It refuses a new job for a node while another job
for that node is still running or queued, and it reports `JOB_ERROR_UPDATE_IN_PROGRESS`. RMS doesn't
queue the request, so retry after the active job ends.

In a batch RPC, a refused node doesn't fail the request. The node appears in `node_results` with the
reason, and RMS still starts every node that it admitted. RMS also treats a node that appears twice in
one request as refused the second time. For the full rules, refer to the
[Operations overview](../operations/overview.md#node-exclusion-and-capacity).

## Job retention

RMS tracks jobs in memory. It keeps a final job for the interval set by `terminal_job_ttl_seconds`,
which is 24 hours by default, and then removes it. Poll to completion and save any result that you
need before that time. A job that RMS no longer tracks looks the same as a job ID that never existed.
For details, refer to the [Operations overview](../operations/overview.md#retention-and-cleanup).
