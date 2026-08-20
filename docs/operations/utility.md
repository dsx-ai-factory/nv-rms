# Utility

Service-wide RPCs. `GetJobStatus` is the generic poller for the async jobs
described in [the async job model](overview.md#the-async-job-model).

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `GetVersion` | Sync | Return the server version string. | - | `version` |
| `GetJobStatus` | Sync | Generic job-status poller. For a parent job, returns the parent plus each child's status. Polls `UpdateSwitchSystemPassword` and `BatchResetSwitchSdnFactoryDefault` jobs (and any tracked job). | `job_id` | `job_states[]` |
