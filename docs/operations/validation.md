# System Validation

See [Operations Overview](overview.md) for the conventions and the
[async job model](overview.md#the-async-job-model) that this page relies on.

The `RackManagerV2` service provides system validation. It launches an NVSSVT
scan against each Device Under Test (DUT) in a request.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `StartSystemValidation` (`RackManagerV2`) | Async via `GetJobStatus` | Launch one NVSSVT scan job per admitted caller-supplied **ephemeral** node under a parent job. | `nodes`, `config_override?` | `response.job_id` (parent), `jobs[]` (one scan job per node) |

Each node's scan runs as its own job, and a failure to admit or launch one node
doesn't affect the others. NVSSVT can run for a long time, so poll the parent
job with `GetJobStatus` and read each scan job's `result_json` on completion. It
holds the parsed per-DUT status and the summary report.

NVSSVT reaches each DUT over its BMC, which is required, and over its host OS
when you supply a host endpoint. Every node's `bmc_endpoint` must therefore
carry credentials. RMS passes `config_override` through to NVSSVT for every
node's scan without interpreting it.

For the request and response fields, refer to
[`StartSystemValidation`](../api-reference/generated/validation.md#startsystemvalidation-v2).
