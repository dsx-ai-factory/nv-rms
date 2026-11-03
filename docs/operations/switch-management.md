# Switch Management

See [Operations Overview](overview.md) for the conventions (in-band failures,
registered vs. ephemeral targets) and [the async job model](overview.md#the-async-job-model)
that the asynchronous switch RPCs below build on.

## Switch firmware and system images

Switch-only RPCs. NVOS system-image installs run as async jobs; the direct
firmware and listing calls are synchronous.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `ListSwitchFirmware` | Sync | VR NVL72: read all installed instances of a component from BMC Redfish, without NVUE initialization; requires BMC credentials. GB200/GB300: retain NVUE inventory reads. | `rack_id`, `node_id`, `component_type` | `result_json` (VR NVL72: keyed by BMC inventory URI with Redfish fields; GB200/GB300: existing NVUE shape) |
| `PushSwitchFirmware` | Sync | Upload a local firmware file to the switch filesystem (transfer only, no install). Source must resolve within `firmware_dir`. | `rack_id`, `node_id`, `component_type`, `filename`, `local_file_path` | `result_json` |
| `BatchResetSwitchFactoryDefault` | Async → `GetJobStatus` | Submit a destructive full NVOS factory reset per switch through NVUE REST, with optional SSH submission for connection/setup failures. RMS retries reset submission with `admin/admin` only after configured credentials are rejected, then completes when default-credential SSH connects. RMS does not answer or change the first-login password prompt. Per-node results report job admission only. | `nodes`, `domain?`, `allow_ssh_fallback` (default `false`) | `NodeBatchResponse` (parent `job_id`) |
| `ListSwitchSystemImages` | Sync | List NVOS system images on a switch. | `rack_id`, `node_id` | `images_json` |
| `UpdateSwitchSystemImage` | Async → `GetSwitchSystemImageJobStatus` | Batch NVOS image update over an explicit node list: optional ONIE recovery → secure NVUE bootstrap when recovery installs NVOS → verify/heal ASIC auto-update settings → SFTP upload → NVOS install → reboot → steady-state verification → NVOS readiness (`status` is `System is ready` and `health` is `OK`, polled on the post-activation budget; `System is not ready (failed)` fails at once), per node under a **parent** job. Early-exits if already up to date. Automatic unused-partition cleanup uses a non-force uninstall and skips (does not uninstall) the current, boot-next, and incoming target images. | `nodes`, `image_filename`, `local_file_path` (under `firmware_dir`), `allow_onie_recovery`; one `nodes.additional_host_endpoints` entry per node when recovery is enabled | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `GetSwitchSystemImageJobStatus` | Sync | Poll `UpdateSwitchSystemImage` / `ApplyStoredSwitchSystemImage` / `ApplySwitchSystemImage` jobs (leaf or parent). | `job_id` | `state`, `message`, `result_json`, timestamps |

NVUE HTTP failures handled through the JSON client methods include the HTTP
method, endpoint, status, and a response-body excerpt in the RMS error message.
The excerpt preserves device validation and state details, redacts secret fields,
and is limited to 1024 characters plus an ellipsis when truncated. Empty response
bodies produce the method, endpoint, and status alone.

For full NVOS factory reset, `allow_ssh_fallback` defaults to `false`. NVUE is
always the primary submission path, using normal verified TLS on secure sites.
With an explicit `true`, RMS can submit over SSH when NVUE connection or setup
fails before submission, including DNS, TCP, TLS handshake, and missing or
unusable client TLS material. SSH uses the switch's configured host credentials
and the existing SSH transport; it does not require NVUE TLS material.

SSH reset submission requires the configured switch account to be `admin`.
RMS runs the full NVOS factory-reset command
`nv action reset system factory-default force` directly as that account. The
`force` option suppresses confirmation and initiates a reboot; see the
[NVIDIA NVOS command reference](https://docs.nvidia.com/networking/display/NVIDIANVOSUserManualforInfiniBandSwitchesv25026077/Configuration-Management-Commands).
An explicit SSH command rejection or nonzero exit status fails the job.
A timeout or connection closing after SSH command submission requires completion
polling; it does not cause another reset submission.

Authentication and HTTP/application errors do not authorize SSH fallback. RMS
retains its one-time NVUE retry with `admin/admin` after configured credentials
are rejected. If configured SSH credentials are rejected after an earlier reset,
RMS checks the default login without issuing another reset. A timeout,
disconnect, or unusable response after sending the NVUE
reset can mean it was accepted. RMS polls completion without submitting another
reset, even when `allow_ssh_fallback` is `true`. If completion cannot be confirmed
within the job timeout, the job fails and reports the uncertain outcome.

Both paths use the existing job lifecycle and complete when default-credential
SSH authentication succeeds. With `allow_ssh_fallback: false`, SSH is used only
for readiness checks, never for reset submission. RMS closes readiness
connections without opening a shell, answering the first-login password prompt,
or changing the default password.

Full NVOS factory reset clears imported certificates and service bindings. It
does not generate or provision replacement certificates. After completion, use
`UpdateSwitchSystemPassword` to handle the default first-login password state and
align credentials, then `ConfigureSwitchCertificate` with valid material to
restore verified mTLS. Restore the fabric configuration cleared by the full reset.

For NVOS system-image updates, RMS derives the target build ID from the image
filename using the `nvos-X.Y.Z` convention. It skips the update only when
both the running (`current`) and next-boot (`next`) build IDs match that target.
Matching the running build alone is insufficient because a stale next-boot
partition could restore an older image. A skipped job reports
`already_up_to_date` and performs no ASIC configuration changes, partition
cleanup, upload, install, or reboot.

After ONIE recovery installs the target image, RMS verifies and heals the ASIC
firmware auto-update settings before completing with `status: "recovered"`.
It skips a second image upload, installation, and reboot. An ASIC preflight
failure fails the job.

If omitted, `allow_onie_recovery` is `false`. When it is `true`, each node must
provide exactly one `additional_host_endpoints` entry
with the ONIE SSH address and username/password credentials. Port `0` or `22`
selects SSH port 22; any other explicit port is rejected. The bundled test
client accepts the ONIE endpoint as
`onie_ip,onie_port,onie_username,onie_password` after `node_type` in its short
switch CSV layout, or after `bmc_mac` in the node-list layout. The short layout
requires an explicit `node_type` when these columns are present. RMS initializes
the configured NVUE transport policy. If initialization fails, RMS logs a warning
with the node ID and initialization error, then probes the current transport.
RMS probes REST at `/nvue_v1/system`
with a five-second request timeout. A valid NVUE system
response identifies NVOS as installed, even if it is not yet ready or healthy,
and skips SSH detection. On any REST failure, including a timeout, invalid
response, TLS rejection, or authentication rejection, RMS attempts SSH detection
only when `allow_onie_recovery` is `true`: it checks NVOS first, then ONIE.
When recovery is disabled, the REST error fails the job without an SSH probe.

The initial NVOS SSH probe runs `nv show system version` with a five-second
budget that includes SSH connection time. A successful command identifies NVOS;
its human-readable output is not parsed. If the query fails or times out, RMS
runs `command -v nv` to retain installation detection while `nvued` is starting
or unavailable. A successful binary-presence check also identifies NVOS and
skips ONIE. Only failure of both NVOS checks permits the ONIE probe; if ONIE
cannot also be verified, RMS reports an unknown operating system and fails the
job. After ONIE installation, the boot verification loop continues to use only
the binary-presence check so that it does not depend on NVUE startup.

The ONIE probe requires a valid host endpoint MAC.
RMS compares it with ONIE's reported management MAC
and with the additional endpoint MAC when one is supplied. A missing, invalid,
or mismatched MAC fails the job before image staging. RMS then rejects
undersized installers, copies the image to `/tmp` with SCP, and verifies the
remote size before starting `onie-nos-install`. RMS reports a failed installer
exit promptly and includes
a bounded, redacted installer log tail on failure or timeout while waiting for
the normal NVOS SSH endpoint to return. A fresh installation is normalized
from the expired factory-default `admin/admin` login to the node's configured
NVOS `admin` password. ONIE
recovery rejects a non-`admin` NVOS username before staging the image; the
ONIE endpoint uses its own credentials.
After NVUE initialization, RMS retries temporary transport errors and HTTP
401 responses while saving the recovered credentials within a bounded window.
The `[workflows]` recovery and credential-save timeouts default to 15 and 5
minutes, respectively; operators can increase them for slower switches.

On secure sites, RMS checks that the default domain and both switch and client
TLS material resolve before copying the recovery image; missing material fails
the job without starting ONIE installation. A recovered switch initially
presents its factory NVUE certificate. RMS permits unverified mTLS only when
both the current transport and a verified mTLS retry fail certificate
validation. The bootstrap transport still presents the configured RMS client
certificate, including when the switch server certificate has expired. RMS then
loads the configured switch TLS material, imports and binds the NVUE API
certificate, and verifies the resulting mTLS connection before
normal NVUE initialization and credential persistence. Other transport and
authentication failures do not enable the unverified fallback. Staged
certificate material is removed after import and on failure or cancellation.
With `insecure_switch = true`, RMS skips certificate installation and retains
the configured insecure NVUE behavior.
When NVOS is already installed, the bootstrap stage is skipped and certificates
are not reinstalled.

After NVUE initialization, RMS saves the applied configuration so the password
persists across reboots. NVOS verification, recovery, and certificate bootstrap
run as the tracked image job's initial per-node stages, so the RPC returns job
IDs before those operations finish. The job timeline records whether certificate
bootstrap completed or was skipped and why. When recovery is disabled,
switch-image RPCs ignore `additional_host_endpoints`.

This check is scoped to NVOS. Switch firmware update jobs separately compare
their selected package components; see
[NVSwitch Firmware Version Preflight](firmware.md#switches).

VR NVL72 NVOS updates require both host and BMC endpoints with credentials.
The ONIE recovery setup described above also applies to VR NVL72. Once NVOS
is installed, RMS installs a required image update through NVUE with
`reboot: "no"`, waits for explicit `action_success`, and verifies that the
next-boot build matches the requested image. It then issues one BMC Redfish `ComputerSystem.Reset` with
`ResetType: "PowerCycle"` through the switch's BMC aux-power-cycle path,
instead of asking NVOS to perform the reset. The job completes only after
NVUE reports the target build as both current and next-boot. Installation
errors, failed boot-next checks, or cancellation before activation do not
trigger a reset; a failed reset submission is not retried automatically.
The job timeline records this handoff as `activate_via_bmc`.

This behavior is shared by `UpdateSwitchSystemImage`, `ApplySwitchSystemImage`,
and `ApplyStoredSwitchSystemImage`. GB200/GB300 retain their automatic NVOS
reboot behavior. Upload, ASIC preflight, installation, and post-boot NVOS
verification still require host access; using BMC activation does not bypass
NVOS installation checks or coordinate separate platform-firmware and NVOS jobs.

## Scale-up fabric manager

RPCs targeting the on-switch NMX controller (`nmx-controller`) over
gRPC and the NVLink switch fabric. Each builds a per-request ephemeral switch from
caller-supplied node info. The `domain` field selects the mTLS cert set (not a DNS
domain).

Both the `RackManager` and `RackManagerV2` services define a
`ConfigureScaleUpFabricManager` RPC. The `RackManager` version is synchronous.
The `RackManagerV2` version runs as an asynchronous job, which this page
refers to as V2 where the context is clear.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `ConfigureScaleUpFabricManager` (`RackManager`) | Sync | Full single-switch bring-up: enable cluster, enable external gRPC, verify readiness, then issue NMX Hello + `SetStaticConfig(topology)`. Idempotent short-circuit if already configured. With `insecure_switch`, unsets NMX-C manager encryption after the cluster app reports itself ready; if the cluster is enabled and the app stays unready for the retry budget, configure fails with `cluster app nmx-controller did not become ready for manager configuration` and does not run the restore. | `node`, `topology_type`, `domain?` | `topology_used`, `scale_up_fabric_state_enabled`, `grpc_enabled` |
| `ConfigureScaleUpFabricManager` (`RackManagerV2`) | Async via `GetJobStatus` | Snapshot switch state; select one primary; clear the primary node-IP interface and, when `GET /cluster/node/secondary` exists, the secondary interface, then disable every submitted non-primary switch; reconcile primary-interface and optional secondary-interface node IP addresses when required; restore and verify the selected switch's provisioned NMX-C mTLS binding in secure mode, or with `insecure_switch` unset NMX-C mTLS after primary reconcile (same wait function as V1, later in the job: skip restore when encryption is already disabled; when the cluster is disabled skip the readiness wait and still issue restore (NVOS rejection is tolerated); a ready app is not blocked by `nmxc-conn` down; fail closed if the enabled app stays unready); reconcile topology and ordered extra static config; then require exactly one enabled primary. In secure mode, V2 rebinds NMX-C when its certificate ID, CA ID, or encryption differs from the NVUE API certificate ID, CA ID, or required `mtls` encryption; a matching binding retries Hello connect failures up to five attempts, six seconds apart, and reports the final failure without rebinding. On VR NVL72, RMS probes `GET /cluster/node/secondary` when the request includes secondary addresses or when clearing an enabled non-primary switch. A selected switch with no secondary addresses skips that GET. A 404 skips the secondary snapshot, clear, and verify; the nv-bridge SDN report still runs, because it checks primary-server peers over SSH and does not read `/cluster/node/secondary`; requesting secondary addresses on that image fails the job. A 5xx, authentication failure, or timeout on a probe the job will use fails the job. Failures after mutation restore snapshotted switch state. | `nodes`, `primary_switch_node_id?`, `config`, `domain?` | `job_id` |
| `BatchSetScaleUpFabricState` | Sync | Concurrently enable/disable the fabric cluster across switches. | `nodes`, `enabled` | `NodeBatchResponse` (`node_results`, `stats`) |
| `GetScaleUpFabricState` | Sync | Return cluster state for one switch. | `node` | `state_json` |
| `BatchGetScaleUpFabricServiceStatus` | Sync | Per-node `nmx-controller` cluster-apps status for a switch set (per-node health in the map, not the top-level status). | `nodes` | `service_statuses` (map), `stats` |
| `SetScaleUpFabricTelemetryInterfaceState` | Sync | Enable/disable the switch's gNMI telemetry service. | `node`, `enable` | `result_json` |
| `BatchResetSwitchSdnFactoryDefault` | Async → `GetJobStatus` | Submit one **destructive** SDN factory-reset child job per switch under a parent job. Jobs are intentionally not persisted. | `nodes`, `domain?` | `NodeBatchResponse` (parent `job_id`) |
| `GetScaleUpFabricStatus` | Sync | Read observed topology, static configuration, cluster state, and NMX Controller status across submitted switches without mutation. | `nodes`, `domain?` | `fabric_status` |
| `InspectScaleUpFabrics` | Sync | Read enabled controllers, their compute and switch nodes, and reported component status without changing devices. | `nodes`, `domain?` | `fabrics`, `discovery_errors` |

`RackManagerV2.ConfigureScaleUpFabricManager` accepts an optional `layout` of
fabric sizes and preferred primaries. For layout requests, `nodes` contains every
switch in the physical inventory and the compute nodes available for assignment.
Compute BMCs provide physical placement; no compute host access is required.
RMS selects disjoint groups using discovered positions and deployment rules,
then returns their node IDs and selected primaries in `selected_fabrics`.
The response accepts a job; configuration completes asynchronously.

Layout requests require `config`, positive tray counts, and no top-level
`primary_switch_node_id`. Preferred primaries belong inside each fabric entry.
Layout requests reject descriptor `fm_config:` overrides; supply caller-owned
static settings through `config.extra_static_configs` instead.
Node IDs must be unique and types must resolve. Only GB200 and GB300 switch
layouts are supported. An omitted deployment catalog returns `UNIMPLEMENTED`;
unsupported hardware or placement fails before job admission.

Deployment rules supply complete administrative link and owned controller
settings. All shared link targets are saved and verified before independent
fabric controllers activate concurrently. Unassigned switches have their
controlled interfaces disabled. A single fabric matching the complete physical
capacity restores the profile's full-connectivity targets and controller
settings; compute endpoints can be omitted for that request.

Active jobs reject overlapping layout requests. On failure, RMS drains started
work and attempts controller cleanup before releasing ownership and marking the
job failed. Cleanup errors do not prevent release. Releasing ownership does not
establish successful recovery.
If an interrupted operation remains registered, transferring its ownership
requires the same physical scope and catalog.
Repeated completed requests read current settings and avoid writes and restarts
when saved targets and activation evidence still match. RMS restart loses the
activation evidence. Controller completion does not establish GPU communication
or bandwidth; use `InspectScaleUpFabrics` for current component status.

Without `layout`, RMS retains single-fabric configuration and does not reset
deployment-managed link targets. Every node ID and type is validated before
recognized compute and power-shelf nodes are excluded. Excluded nodes need no
endpoints and receive no reads or writes. At least 1 switch must remain. Use
`GetScaleUpFabricStatus` for topology and static-setting readback.

For requests without `layout`, `RackManagerV2.ConfigureScaleUpFabricManager` recognizes temporary
`NodeDescriptor.attributes` entries named `fm_config:<key>`. RMS maps entries
from the selected primary switch to the corresponding `fm_config` keys. The job
ignores these attributes on non-primary switches. If
`config.extra_static_configs` already contains the same file and key, the job
fails before switch mutation instead of choosing one value. This prefix is an
unsupported compatibility bridge; callers should use
`config.extra_static_configs` when available.

For `RackManager.ConfigureScaleUpFabricManager` and V2 requests without `layout`,
RMS restarts NMX-C after confirming a write to `fm_config`, including
`MNNVL_TOPOLOGY`. Extra entries are applied before the activation restart, so
topology and extra FM changes share one restart. Confirmed unchanged values and
changes confined to other static-config files do not trigger this restart. V1
reports success after the restart action succeeds; V2 also waits for
control-plane convergence.

A failed activation restart produces a V1 failure response or a failed V2 job.
Manager readiness checks attempt to start an explicitly stopped NMX-C, including
NVUE's `reason: stopped by user` response. RMS does not track pending activation
across requests: if NMX-C is already running, a retry may find matching stored
values and skip the restart.

Without `layout`, `RackManagerV2.ConfigureScaleUpFabricManager` reconciles node IPs. The first
`additional_host_endpoints` entry supplies the secondary management address
from `interface.ip_address`. Node-IP reconciliation ignores later entries and
the selected entry's port and credentials. Mutation admission separately validates
the ports of supplied endpoints with addresses and reserves their aliases,
including supplemental endpoints. Secondary addresses must be unique across nodes
and must not overlap primary addresses.
When the secondary collection exists, RMS snapshots, reconciles, verifies, and
rolls back the secondary addresses with the primary addresses. Omitting the
entry leaves the selected switch's secondary addresses unchanged and does not
probe `/cluster/node/secondary` on that switch. Whether
`GET /cluster/node/secondary` exists decides the non-primary read and clear.
When it exists, RMS reads both applied address sets for each enabled
non-primary switch into request-local rollback state, then clears both before
disabling that switch. When that GET returns 404, RMS reads and clears only
the primary set and logs that the secondary interface was skipped.
Older servers ignore the additive `NodeInfo` field.
Layout requests do not perform node-IP reconciliation.

On VR NVL72, the resulting `sdn_fabric_state` determines connection health from
the report's connection state, keepalive state, and chassis identity. The
`activeState` field describes the connection's role and does not affect its
health. Every configured primary-server peer must still appear in the report.

### Live fabric inspection

Use `InspectScaleUpFabrics` to find enabled controllers, their nodes, and reported status.
Use `GetScaleUpFabricStatus` to read topology and static configuration settings.
Inspection does not compare live settings with a desired configuration, start jobs,
change devices, or test communication or bandwidth. Reads can reflect different
points in time.

Supply a `nodes` inventory containing at least one switch. Each switch needs
its host endpoint and credentials. Compute nodes are optional; when supplied,
their BMC endpoints need credentials and MAC addresses. RMS reads chassis serial
numbers and tray indices from the BMCs to match GPUs in the live topology to
caller node IDs. No compute host login is required. Node IDs must be nonempty
and unique; only switch and compute roles are accepted.

RMS reads switch cluster state and physical identity through NVUE. A cluster state
of `enabled` or `start` produces one `fabrics` entry, in switch inventory order.
Disabled controllers produce no entry. An empty fabric list returns `SUCCESS`
only when all requested discovery reads succeeded and every switch was disabled.

For each enabled controller, RMS connects using the request's `domain` and reads:

| NMX-C RPC | Information used |
| --- | --- |
| `GetDomainStateInfo` | Control-plane state and controller status |
| `GetTopologyInfo` | Live device identities, device status, and operational port states |
| `GetSwitchNodeInfoList` | Switch-node status |
| `GetComputeNodeInfoList` | Compute-node status |

RMS matches devices to node IDs by chassis serial number and tray index. Each
fabric's `switch_node_ids` and `compute_node_ids` are sorted and contain no
duplicates. A device that cannot be matched to exactly one inventory node produces
an error; known members are kept. Supply compute inventory to identify GPUs that
appear in the topology. If controllers report shared nodes, RMS keeps both fabric
entries and reports the overlap as an error.

The response's top-level `status` describes whether collection succeeded, not
whether the fabric is healthy. A complete read of an abnormal fabric returns
`SUCCESS`. Read failures, unknown cluster states, and missing or ambiguous node
identities return `FAILURE` with the available results. Errors for a fabric's
members or primary appear in that fabric's `inspection_errors`. Errors for nodes
with no identified fabric appear in `discovery_errors`.
Invalid inventory also returns `FAILURE` with a discovery
error. Malformed `grpc-timeout` metadata returns gRPC `INVALID_ARGUMENT` instead.

Each fabric's `control_plane_state` comes from NMX-C. `CONFIGURED` means NMX-C
reports that state, not that RMS verified desired settings. `component_status`
reports separate values for the controller, computes, switches, and links.
The fabric's `status` is:

- `ABNORMAL` when the control plane failed or any component reports abnormal status.
- `NORMAL` when the controller is configured, every component is normal, and
  collection and node association are complete and unambiguous.
- `UNKNOWN` otherwise, including when GPU endpoints have not been observed.

Unknown protocol values produce unknown status. A known abnormal status takes
precedence over unknown status. Link status excludes administratively down ports
and FNM/HCA ports.

Positive-count node rows without usable physical identities produce inspection
errors; their reported faults still contribute to component status. A device that
lists fewer ports than its reported port count produces a topology interpretation
error and contributes unknown link status. Reported link faults retain precedence.

Collection shares a 60-second deadline. When the caller supplies `grpc-timeout`,
RMS reserves the greater of 10% of that timeout or 250 milliseconds for returning
results and caps collection at the remaining time. Timeouts of 250 milliseconds
or less return gRPC `DEADLINE_EXCEEDED` without device reads. Transport cancellation
can still prevent delivery if the caller's deadline expires.
All switch collections and compute identity reads start concurrently, and results
retain switch inventory order. Within each switch collection, controller reads start once NVUE
reports an enabled cluster, without waiting for that switch's identity read.
Compute discovery also runs independently. RMS matches controller results to
node identities after collection. Cancelling the RPC stops unfinished reads.

Local switch input errors identify missing credentials or invalid endpoints.
Read failures remain redacted in responses.

## Switch certificates, credentials, and attestation

Switch-only. Certificate install and password rotation run as async per-switch
jobs. SPDM evidence collection is synchronous within the NICo-owned attestation
job.

| RPC | Sync/Async | Behavior | Key inputs | Key outputs |
| --- | --- | --- | --- | --- |
| `ConfigureSwitchCertificate` | Async → `GetConfigureSwitchCertificateJobStatus` | Per switch, install switch-side mTLS certificate material (resolved **server-side** from configured TLS roots - the operator supplies none) and bind selected services (NVUE, NMX, gNMI). The job first makes room in the NVOS certificate stores, as described in [Certificate store capacity](#certificate-store-capacity). NVUE and NMX bind the entity certificate and client CA. gNMI always binds the entity certificate on `/system/gnmi-server`. When gNMI is requested, the job records a `discover_nvue_capabilities` stage that determines gNMI client-CA support before bind. The stage details record `gnmi_client_authentication` as `server-certificate-only` or `mtls`. It does not probe NVBridge `/cluster/node/secondary`. That stage issues `GET /system/gnmi-server/mtls`, and bind and verify on the same switch object reuse the answer. A later job probes again. A 404 classifies the client CA as unsupported and the bind commits the entity certificate alone; a 2xx classifies it as supported and the bind stages the entity certificate and client CA in one revision, so a CA failure discards both before apply; 5xx/timeout/auth fail the job. Discovery failures are attributed to that stage, not to `bind_scale-up-fabric-telemetry-interface`. Verification separately requests the client-CA resource only when support was discovered. It records `gnmi_server` and `gnmi_mtls` (`null` when discovery finds the CA resource absent; a later 404 fails verification). The same object records `gnmi_client_authentication` as `server-certificate-only` or `mtls`, and a completed job copies that field to the top level of `result_json` beside `services`. With `insecure_switch`, instead creates "unset" jobs that move switches back to non-mTLS. Binding a scale-up fabric service waits for its cluster app to report itself ready before the first manager update: it fails the job with `cluster app <name> did not become ready for manager configuration` when the app stays unready, and proceeds when readiness cannot be read at all, leaving NVUE to report any resulting failure. | `nodes`, `services[]`, `domain?`, `test_hello` | `NodeBatchResponse` (parent `job_id`), `jobs[]` |
| `BatchDisableSwitchMtls` | Async via `GetJobStatus` | Per switch, disable mTLS for selected NVUE, NMX, or gNMI services through NVOS CLI commands. This operation does not require `insecure_switch` or configured TLS material. The job result reports one status per requested service (`cleared`, `already_disabled`, `unsupported`, `failed`, or `not_attempted`) plus the `config_apply` / `config_save` persist steps; a run that stops partway names the service and stage that stopped it. Cancelling the job (including through graceful shutdown) stops it at the next command boundary and reports the services already changed, so persist steps left `not_attempted` mean the change reached the switch but was not committed. NVOS command text appears only in RMS logs, never in the job result or error message. | `nodes`, `services[]` | `NodeBatchResponse` (parent `job_id`) |
| `GetConfigureSwitchCertificateJobStatus` | Sync | Poll a `ConfigureSwitchCertificate` job (child or parent). | `job_id` | `state`, `message`, `result_json`, timestamps |
| `UpdateSwitchSystemPassword` | Async → `GetJobStatus` | Rotate an NVOS system-user password on one or more switches. Per-node results report **job admission only**. Passwords are kept task-local and scrubbed from all status and logs. | `username`, `password`, `nodes` | `NodeBatchResponse` (parent `job_id`) |
| `BatchCollectSwitchSpdmAttestationEvidence` | Sync | Collect targets concurrently; per target, discover NVUE SPDM components, generate fresh nonce-bound measurements serially, and fetch each component's signed measurements and certificate chain. RMS collects but does not appraise the evidence, and holds no lock or persisted state across calls. | `targets[]`, 32-byte `nonce`, `domain?` | Batch status and one result per target with per-component status, evidence, and a collection timestamp |

Before installing a switch certificate, RMS retries a failed NVUE readiness
check with verified mTLS. If both attempts fail server certificate validation,
it uses unverified mTLS for bootstrap while still presenting its client
certificate. Other transport or authentication failures stop the job. After
installation, RMS requires a verified mTLS connection to the switch. Normal
NVUE initialization does not retain an unverified bootstrap transport.

### Certificate store capacity

Every `ConfigureSwitchCertificate` install imports one CA certificate and one
entity certificate under new timestamped IDs, `rms-<domain>-ca-<timestamp>` and
`rms-<domain>-cert-<timestamp>`, even when the CA material is unchanged. NVOS
documents a limit of 25 imported entity certificates and 25 imported CA
certificates. Each job starts with a `reclaim_certificate_capacity` stage that
makes room for its import before it stages, imports, or binds anything.

RMS keeps superseded entries until a store is full, so earlier material stays
available for services that still use it. When a store has no room for the
import, RMS deletes the oldest entries in that store that are safe to delete,
and only as many as the import needs. Each store's deletions are counted and
chosen from that store alone. An entry is safe to delete only when all of the
following hold:

- Its ID starts with `rms-` and uses only ASCII letters, digits, `-`, and `_`,
  as every ID RMS generates does. Other entries are never deleted, but they
  count toward the 25-entry limit, so a switch with operator-created entries
  reclaims sooner and holds fewer RMS entries.
- It is not material for the running install.
- NVOS does not report a service using it.
- No switch service is bound to it. RMS reads the NVUE API, gNMI server, NMX
  Controller, and NMX Telemetry bindings, including services the request does
  not name. A service omitted from a request keeps whatever entry it uses, even
  the oldest one. A binding read that fails, returns a success status without
  a body, or has an unrecognized shape leaves that service unconfirmed, which
  blocks deletion in that store.

When every store has room, the stage only lists the stores. It reads service
bindings only when a store is full. The stage details record each store's
`entries`, `rms_entries`, and `deletions`, and the `deleted` entries.

The job fails at `reclaim_certificate_capacity`, before it imports anything or
changes any service binding, when one of the following conditions holds. The
job's `error_message` contains the listed text.

| Condition | Message contains |
| --- | --- |
| A store cannot be listed | `failed to list the NVOS <store> store` or `NVOS <store> store listing is not a JSON object` |
| A store is full and a service binding cannot be read or is not recognized | `RMS could not confirm which entries switch services use (<service>: <reason>)` |
| A store is full and too few entries are safe to delete | `NVOS <store> store is full (<n> of 25 entries, <m> created by RMS)` |
| The switch rejects a deletion | `failed to delete unused NVOS <store> entry <id>` |
| A store is still full after deleting | `still lists <n> of 25 entries after RMS deleted unused entries` |

RMS deletes each entry with the NVUE `@delete` action. An entry counts as
deleted only when the action reports `action_success` or the switch returns 404
for it; any other action state, whatever its message, keeps RMS waiting until
the action succeeds, fails, or times out. A successful response that carries no
action ID fails the job, because RMS cannot confirm the deletion.

Both stores are planned before anything is deleted. When either store cannot
make enough room, nothing is deleted from either. A deletion failure can follow
earlier deletions in the same stage, and the failed stage's `deleted` field
lists them. Those entries were unused, so no service loses its binding. A job
cancelled during this stage stops before its next deletion and records which
entries it already deleted the same way.

A failed install can leave entries behind: a CA imported before the entity
import failed, or new material bound to some services but not others. RMS never
deletes an entry a service is bound to, so a partial install keeps every
current binding. A later install deletes the leftover entries once they are
unused and the store needs room.

To recover a switch whose stores cannot be reclaimed, list the stores with
`nv show system security certificate` and `nv show system security
ca-certificate`. Delete entries no service needs on the switch, then retry the
job. Naming more services in the `ConfigureSwitchCertificate` request does not
free space, because the stage runs before any import or binding change.

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
