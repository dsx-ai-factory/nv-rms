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
| `ConfigureScaleUpFabricManagerV2` | Async via `GetJobStatus` | Snapshot switch state; select one primary; clear both node-IP interfaces and disable every submitted non-primary switch; reconcile primary-interface and optional secondary-interface node IP addresses when required; restore and verify the selected switch's provisioned NMX-C mTLS binding in secure mode, or unset NMX-C mTLS with `insecure_switch`; reconcile topology and ordered extra static config; then require exactly one enabled primary. Failures after mutation restore snapshotted switch state. | `nodes`, `primary_switch_node_id?`, `config`, `domain?` | `job_id` |
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

For `ConfigureScaleUpFabricManagerV2` node-IP reconciliation, the first
`additional_host_endpoints` entry supplies the secondary management address
from `interface.ip_address`. RMS ignores later entries, the selected entry's
port and credentials, and the entire field on other dispatch paths. Secondary
addresses must be unique across nodes and must not overlap primary addresses.
RMS snapshots, reconciles, verifies, and rolls back the secondary addresses
with the primary addresses. Omitting the entry leaves the selected switch's
secondary addresses unchanged. RMS reads both applied address sets for each
enabled non-primary switch into request-local rollback state. Before disabling
an enabled non-primary switch, RMS clears both address sets.
Older servers ignore the additive `NodeInfo` field.

## Switch certificates, credentials, and attestation

Switch-only. Certificate install and password rotation run as async per-switch
jobs. SPDM evidence collection is synchronous within the NICo-owned attestation
job.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `ConfigureSwitchCertificate` | Async → `GetConfigureSwitchCertificateJobStatus` | Per switch, install switch-side mTLS certificate material (resolved **server-side** from configured TLS roots - the operator supplies none) and bind selected services (NVUE, NMX, gNMI) to require mTLS. With `insecure_switch`, instead creates "unset" jobs that move switches back to non-mTLS. | `nodes`, `services[]`, `domain?`, `test_hello` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `BatchDisableSwitchMtls` | Async via `GetJobStatus` | Per switch, disable mTLS for selected NVUE, NMX, or gNMI services through NVOS CLI commands. This operation does not require `insecure_switch` or configured TLS material. | `nodes`, `services[]` | `NodeBatchResponse` (parent `job_id`) |
| `GetConfigureSwitchCertificateJobStatus` | Sync | Poll a `ConfigureSwitchCertificate` job (child or parent). | `job_id` | `state`, `message`, `result_json`, timestamps |
| `UpdateSwitchSystemPassword` | Async → `GetJobStatus` | Rotate an NVOS system-user password on one or more switches. Per-node results report **job admission only**. Passwords are kept task-local and scrubbed from all status and logs. | `username`, `password`, `nodes` | `NodeBatchResponse` (parent `job_id`) |
| `BatchCollectSwitchSpdmAttestationEvidence` | Sync | Collect targets concurrently; per target, discover NVUE SPDM components, generate fresh nonce-bound measurements serially, and fetch each component's signed measurements and certificate chain. RMS collects but does not appraise the evidence, and holds no lock or persisted state across calls. | `targets[]`, 32-byte `nonce`, `domain?` | Batch status and one result per target with per-component status, evidence, and a collection timestamp |

### SPDM attestation evidence

NICo owns the attestation job, generates a cryptographically secure 32-byte
nonce, and correlates each RMS result by `node_id`. RMS rejects duplicate
logical targets and duplicate management addresses in one batch.

For each target, RMS reads
`GET /nvue_v1/system/security/spdm?rev=operational` to discover the components
reported by that switch. For every discovered component, RMS serially submits
`POST /nvue_v1/system/security/spdm/{component-id}` with the `@generate`
action and the hex-encoded nonce, then polls the returned NVUE action ID to
`action_success`.

After a successful generation action, RMS fetches
`GET /nvue_v1/system/security/spdm/{component-id}/measurements` and
`GET /nvue_v1/system/security/spdm/{component-id}/certificates`. The response
contains normalized JSON encodings of those opaque NVUE values. RMS does not
log full measurement blobs or certificate chains.

Targets are collected concurrently; components on one switch are generated
serially. Collection continues after a component fails. A target succeeds only
when every discovered component succeeds. A transport error, malformed action
status, poll timeout, or terminal NVUE failure produces a failed component.

RMS holds no lock or persisted job state. NICo prevents overlapping requests
for the same tray and owns retry, quarantine, storage, required-component, and
appraisal policy. A batch has a 15-minute processing deadline. RMS currently
has no request-level target, component, or aggregate evidence-size limit; each
individual NVUE response is limited to 8 MiB by `nvue_client`. Callers must
therefore bound batch size and configure an appropriate gRPC receive limit.
If the deadline expires after component discovery, RMS preserves completed
evidence and reports timeout failures for the current and unattempted components.

Like every other switch RPC, attestation honors `insecure_switch`; when set,
NVUE server-certificate validation is disabled. RMS returns normalized evidence
without appraisal. NICo passes it to its verifier for nonce, signature,
certificate-chain, required-component, and reference-measurement checks.

The southbound paths and nonce format follow the NVIDIA
[NVOS SPDM command reference](https://docs.nvidia.com/networking/display/nvidianvosusermanualfornvlinkswitchesv25024282/spdm-commands).
