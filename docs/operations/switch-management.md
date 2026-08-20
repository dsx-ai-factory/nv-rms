# Switch Management

See [Operations Overview](overview.md) for the conventions (in-band failures,
registered vs. ephemeral targets) and [the async job model](overview.md#the-async-job-model)
that the asynchronous switch RPCs below build on.

## Switch firmware and system images

Switch-only RPCs. NVOS system-image installs run as async jobs; the direct
firmware and listing calls are synchronous.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `ListSwitchFirmware` | Sync | Read a switch's firmware inventory for a component. | `rack_id`, `node_id`, `component_type` | `result_json` |
| `PushSwitchFirmware` | Sync | Upload a local firmware file to the switch filesystem (transfer only, no install). Source must resolve within `firmware_dir`. | `rack_id`, `node_id`, `component_type`, `filename`, `local_file_path` | `result_json` |
| `BatchResetSwitchFactoryDefault` | Async → `GetJobStatus` | Submit a destructive full NVOS factory reset per switch through NVUE REST. RMS retries reset submission with `admin/admin` only after configured credentials are rejected, then completes when default-credential SSH connects. RMS does not answer or change the first-login password prompt. Per-node results report job admission only. | `nodes`, `domain?` | `NodeBatchResponse` (parent `job_id`) |
| `ListSwitchSystemImages` | Sync | List NVOS system images on a switch. | `rack_id`, `node_id` | `images_json` |
| `UpdateSwitchSystemImage` | Async → `GetSwitchSystemImageJobStatus` | Batch NVOS image update over an explicit node list: SFTP upload → NVOS install → reboot → steady-state verification, per node under a **parent** job. Early-exits if already up to date. | `nodes`, `image_filename`, `local_file_path` (under `firmware_dir`) | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `GetSwitchSystemImageJobStatus` | Sync | Poll `UpdateSwitchSystemImage` / `ApplyStoredSwitchSystemImage` / `ApplySwitchSystemImage` jobs (leaf or parent). | `job_id` | `state`, `message`, `result_json`, timestamps |

## Scale-up fabric manager

Switch-only RPCs targeting the on-switch NMX controller (`nmx-controller`) over
gRPC and the NVLink switch fabric. Each builds a per-request ephemeral switch from
caller-supplied node info. The `domain` field selects the mTLS cert set (not a DNS
domain).

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `ConfigureScaleUpFabricManager` | Sync | Full single-switch bring-up: enable cluster, enable external gRPC, verify readiness, then issue NMX Hello + `SetStaticConfig(topology)`. Idempotent short-circuit if already configured. | `node`, `topology_type`, `domain?` | `topology_used`, `scale_up_fabric_state_enabled`, `grpc_enabled` |
| `ConfigureScaleUpFabricManagerV2` | Async via `GetJobStatus` | Snapshot switch state; select one primary; disable every submitted non-primary switch; reconcile node IP addresses when required; restore and verify the selected switch's provisioned NMX-C mTLS binding in secure mode, or unset NMX-C mTLS with `insecure_switch`; reconcile topology and ordered extra static config; then require exactly one enabled primary. Failures after mutation restore snapshotted switch state. | `nodes`, `primary_switch_node_id?`, `config`, `domain?` | `job_id` |
| `BatchSetScaleUpFabricState` | Sync | Concurrently enable/disable the fabric cluster across switches. | `nodes`, `enabled` | `NodeBatchResponse` (`node_results`, `stats`) |
| `GetScaleUpFabricState` | Sync | Return cluster state for one switch. | `node` | `state_json` |
| `BatchGetScaleUpFabricServiceStatus` | Sync | Per-node `nmx-controller` cluster-apps status for a switch set (per-node health in the map, not the top-level status). | `nodes` | `service_statuses` (map), `stats` |
| `SetScaleUpFabricTelemetryInterfaceState` | Sync | Enable/disable the switch's gNMI telemetry service. | `node`, `enable` | `result_json` |
| `BatchResetSwitchSdnFactoryDefault` | Async → `GetJobStatus` | Submit one **destructive** SDN factory-reset child job per switch under a parent job. Jobs are intentionally not persisted. | `nodes`, `domain?` | `NodeBatchResponse` (parent `job_id`) |
| `GetScaleUpFabricStatus` | Sync | Read observed topology, static configuration, cluster state, and NMX Controller status across submitted switches without mutation. | `nodes`, `domain?` | `fabric_status` |

For caller compatibility, `ConfigureScaleUpFabricManagerV2` recognizes temporary
`NodeDescriptor.attributes` entries named `fm_config:<key>`. RMS maps entries
from the selected primary switch to the corresponding `fm_config` keys. The job
ignores these attributes on non-primary switches. If
`config.extra_static_configs` already contains the same file and key, the job
fails before switch mutation instead of choosing one value. This prefix is an
unsupported compatibility bridge; callers should use
`config.extra_static_configs` when available. If RMS reconciles an `fm_config`
value that was not confirmed already configured, V2 restarts NMX-C before
waiting for control-plane convergence. Confirmed unchanged values and changes
to other static-config files do not trigger this restart.

## Switch certificates and security

Switch-only. Certificate install and password rotation run as async per-switch
jobs.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `ConfigureSwitchCertificate` | Async → `GetConfigureSwitchCertificateJobStatus` | Per switch, install switch-side mTLS certificate material (resolved **server-side** from configured TLS roots - the operator supplies none) and bind selected services (NVUE, NMX, gNMI) to require mTLS. With `insecure_switch`, instead creates "unset" jobs that move switches back to non-mTLS. | `nodes`, `services[]`, `domain?`, `test_hello` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `BatchDisableSwitchMtls` | Async via `GetJobStatus` | Per switch, disable mTLS for selected NVUE, NMX, or gNMI services through NVOS CLI commands. This operation does not require `insecure_switch` or configured TLS material. | `nodes`, `services[]` | `NodeBatchResponse` (parent `job_id`) |
| `GetConfigureSwitchCertificateJobStatus` | Sync | Poll a `ConfigureSwitchCertificate` job (child or parent). | `job_id` | `state`, `message`, `result_json`, timestamps |
| `UpdateSwitchSystemPassword` | Async → `GetJobStatus` | Rotate an NVOS system-user password on one or more switches. Per-node results report **job admission only**. Passwords are kept task-local and scrubbed from all status and logs. | `username`, `password`, `nodes` | `NodeBatchResponse` (parent `job_id`) |
