# Security Policy: Rack Management Service

## Reporting a Vulnerability

If you discover a potential security vulnerability in Rack Management Service (RMS),
please **do not open a public issue, merge request, or discussion**.

Report security issues privately through one of the NVIDIA channels below:

- **NVIDIA Vulnerability Disclosure Program** (preferred):
  [NVIDIA security page](https://www.nvidia.com/en-us/security/)
- **Email**: [NVIDIA PSIRT](mailto:psirt@nvidia.com)
  - NVIDIA encourages use of the
    [public NVIDIA PGP key](https://www.nvidia.com/en-us/security/pgp-key)
- **Repository private reporting**: use the repository Security tab or private
  vulnerability-reporting workflow where enabled.

Please include:

- Product/project name, affected version, release tag, branch, or commit SHA
- Vulnerability type and affected component or RPC
- Step-by-step reproduction instructions
- Proof-of-concept code or request payloads, if available
- Impact assessment, including affected rack, node, BMC, switch, database, or
  firmware-artifact boundary
- Whether the issue was observed with mTLS enabled, `insecure = true`, Helm
  defaults, or a site-specific deployment override

Detailed reports help NVIDIA PSIRT validate severity, coordinate fixes, and
publish security bulletins as appropriate.

## Security Architecture & Context

Rack Management Service is a Rust service for managing data center racks at
scale. It exposes a tonic gRPC API on port 8801 for rack inventory, power
control, firmware update workflows, firmware-object management, switch image
operations, switch password rotation, and scale-up fabric manager operations.

RMS operates as a privileged infrastructure service. Its primary security
responsibility is to protect the rack-management control plane: authenticated
RPC callers can affect power state, install firmware, stage system images,
change switch credentials, query inventory, and persist firmware metadata. A
compromised RMS deployment or unauthenticated RMS listener can become a direct
path to BMC, Redfish, NVUE, SSH/SFTP, NMX-C, PostgreSQL, and firmware-artifact
surfaces.

The audited production scope includes the `rackmanagementservice` service crate
under `crates/rackmanagementservice` and the `rust_nvfwupd` workspace member.
The `redfish_test_support` workspace member is a test-only Redfish simulator used
by integration tests and benchmarks; it is out of scope for deployed RMS binaries
except where simulator behavior could affect production test confidence.

Key components and boundaries:

| Boundary | Interface | Primary code | Security posture |
| --- | --- | --- | --- |
| Client to RMS gRPC | `RackManager` service on `0.0.0.0:<port>` | `crates/rackmanagementservice/src/api/grpc/server.rs`, `crates/rackmanagementservice/src/api/grpc/*_handlers.rs` | Production deployments are expected to use mTLS with the `[tls] cert`, `key`, and `ca` config keys. RMS does not implement application-level users, roles, per-RPC authorization, or in-process rate limiting; an mTLS-authenticated caller is treated as trusted for all RPCs. `[tls] insecure = true` is plaintext and unauthenticated, and is documented for development/testing only; RMS additionally requires the `RMS_ALLOW_INSECURE=1` environment gate (sourced outside the config file) before it will bind plaintext, so a stale config cannot by itself downgrade the API. |
| RMS to BMC / Redfish / NVUE | HTTP(S) through `reqwest` and `rustls` | `crates/rackmanagementservice/src/transport/http_client.rs`, compute and powershelf node implementations, `crates/rackmanagementservice/src/nodes/switch_gb200_nvidia.rs`, `crates/nvue_client/` | Client-supplied endpoint configuration cannot change TLS certificate validation. BMC/Redfish uses HTTPS with Basic auth, but RMS disables BMC server certificate verification because current deployments do not provide BMC certificates that RMS can validate. Switch NVUE uses HTTPS with an RMS client certificate and validates the switch server certificate by default. With `[switches] insecure_switch = true`, NVUE remains HTTPS but uses no client certificate and does not verify the server certificate. These modes depend on trusted management-network controls. |
| RMS to switch OS | SSH/SFTP for command execution and file transfer | `crates/rackmanagementservice/src/transport/ssh_client.rs`, `crates/rust_nvfwupd/src/ssh_transport.rs`, `crates/rackmanagementservice/src/nodes/switch_gb200_nvidia.rs` | SSH host-key verification is disabled on both RMS and `rust_nvfwupd` SSH paths; host identity is delegated to trusted management-network segmentation. |
| RMS to NMX-C | gRPC over HTTPS/mTLS by default, or plaintext HTTP when explicitly opted out | `crates/rackmanagementservice/src/libnmxc/`, `crates/rackmanagementservice/src/api/grpc/scaleupfabricmanager_handlers.rs` | The client requires HTTPS and client certificates by default. `[switches] insecure_switch = true` uses plaintext HTTP without client TLS. |
| Firmware artifacts | Local paths, `file:` URLs, and HTTP(S) URLs from firmware-object JSON | `crates/rackmanagementservice/src/api/grpc/firmware_object_handlers.rs`, `crates/rackmanagementservice/src/api/grpc/firmware_artifact_paths.rs` | RMS validates object IDs, artifact basenames, cache subdirectories, and local firmware paths under the `[workflows] firmware_dir` config key. HTTP(S) artifact downloads can use a caller-supplied JFrog token via `X-JFrog-Art-Api`, but RMS does not enforce an artifact host allow-list or independent signature/hash verification at cache time. |
| Persistence | PostgreSQL or in-memory backend | `crates/rackmanagementservice/src/persistence/` | When `[postgres] db_url` (or the `DATABASE_URL` env override) is set, RMS connects to PostgreSQL with `sqlx` and runs embedded migrations; otherwise it logs a warning and uses in-memory storage. |
| Kubernetes deployment | Helm-managed API server, secrets, and hostPath firmware cache | `helm/templates/api-server-deployment.yaml`, `helm/values.yaml` | The Helm chart defaults to `apiServer.allowInsecure: false` and `apiServer.tls.enabled: true`, mounts API server TLS material and optional switch/NMX-C client certificates from Kubernetes Secrets, and constructs `DATABASE_URL` from database credentials in a Secret or External Secrets Operator workflow. |
| Metrics | Prometheus endpoint on the same service port | `crates/rackmanagementservice/src/api/grpc/server.rs`, `crates/rackmanagementservice/src/metrics/` | `/metrics` is served from the same tonic/axum server and exposes Prometheus metrics plus build metadata such as version and Git SHA. |

RMS intentionally includes several local controls: response-size limits for
Redfish/NVUE JSON responses, canonical firmware path resolution under the
firmware directory, safe artifact filename validation, safe firmware object IDs,
port range checks, switch-target IP validation for ScaleUp Fabric flows,
password redaction in switch password-rotation errors, and `SecretString`
storage for endpoint credentials held in memory.

### Threat Model

1. **Unauthenticated control-plane access through insecure gRPC**:
   If RMS is started with `[tls] insecure = true` **and** the `RMS_ALLOW_INSECURE=1`
   environment gate (or Helm overrides set `apiServer.allowInsecure: true` /
   `apiServer.tls.enabled: false`, which render both), any client that can reach
   port 8801 can invoke power, firmware, inventory, switch image, and switch
   password-rotation RPCs without transport authentication. This can lead to
   service disruption, unauthorized firmware installation, credential changes, or
   rack state manipulation. Requiring the environment gate in addition to the
   config flag is a deliberate defense-in-depth measure: a single stale or
   mis-copied config file cannot on its own downgrade a production API to
   plaintext.

2. **Credential exposure through RPC transport, logs, or operator tooling**:
   Node credentials are passed in gRPC requests and retained in memory for BMC,
   Redfish, NVUE, SSH, and SFTP calls. RMS uses `SecretString` for in-memory
   password storage and redacts some password-rotation error paths, but callers
   must still protect gRPC traffic with mTLS and avoid collecting raw request
   payloads, debug logs, or crash artifacts that contain credentials.

3. **Malicious or tampered firmware artifact staging**:
   Firmware update and firmware-object flows read local files from
   the `[workflows] firmware_dir` directory and can cache artifacts from local paths, `file:` URLs,
   or HTTP(S) locations listed in firmware-object configuration JSON such as
   `AddFirmwareObjectRequest.config_json` and
   `ApplyFirmwareObjectFromJsonRequest.config_json`. HTTP(S) downloads may use
   a caller-supplied JFrog access token in the `X-JFrog-Art-Api` header. RMS
   canonicalizes local firmware paths under the firmware directory, validates
   artifact filenames and firmware object IDs, and compares received size to
   `Content-Length` when present, but it does not enforce an artifact host
   allow-list, artifact approval workflow, cryptographic hash check, or
   signature verification at cache time. Artifact source control, release
   approval, image signing, repository access control, and device-side firmware
   verification remain critical to prevent malicious or wrong-version firmware
   from being applied.

4. **Compromised or spoofed management endpoints**:
   RMS communicates with BMCs and switches over Redfish/NVUE HTTP(S), SSH/SFTP,
   and NMX-C gRPC. BMC/Redfish uses HTTPS with Basic auth, but RMS disables BMC
   server certificate verification because current deployments do not provide
   BMC certificates that RMS can validate. Secure NVUE traffic uses HTTPS with
   Basic auth and validates the switch server certificate. With
   `[switches] insecure_switch = true`, NVUE remains encrypted with HTTPS but does not use a
   client certificate or authenticate the server certificate. An active
   man-in-the-middle can therefore intercept NVUE traffic. NMX-C uses plaintext
   HTTP and has no transport confidentiality in this mode.
   `crates/rackmanagementservice/src/transport/ssh_client.rs`
   and `crates/rust_nvfwupd/src/ssh_transport.rs` accept SSH server host keys without
   verification because switches/BMCs are assumed to be on a trusted management
   network. A malicious endpoint or man-in-the-middle on that network could
   return misleading inventory, job status, or action responses, or could
   capture management credentials.

5. **Switch command and file-transfer abuse**:
   Switch firmware, system image, fabric manager, and password-rotation flows
   use privileged switch host credentials to run NVUE actions and SSH/SFTP
   transfers. RMS constrains filenames, validates target switch IPs for some
   fabric flows, and shell-quotes constructed remote paths, but an authenticated
   RMS client can still request operations that intentionally modify switch
   state. Treat access to these RPCs as equivalent to access to the underlying
   switch administration plane.

6. **Persistence and data-retention failures**:
   If both `[postgres] db_url` and the `DATABASE_URL` override are missing, RMS falls back
   to an in-memory backend and loses firmware-object catalog and apply-history
   state on restart.
   If PostgreSQL credentials or TLS configuration are mismanaged, metadata can
   be unavailable or exposed. Helm deployments should use managed PostgreSQL
   credentials from Kubernetes Secrets or ESO and set `database.sslMode`
   according to the environment's database security requirements.

7. **Metrics and build metadata exposure**:
   `/metrics` exposes service metrics and build labels, including Git SHA and
   version information. In an mTLS-protected deployment this is usually an
   operator observability interface. In an insecure or overly broad network
   exposure it can help attackers fingerprint the service and enumerate active
   RPC methods or status patterns.

8. **Resource exhaustion through firmware downloads and job workflows**:
   Firmware-object download tasks write artifacts under the `[workflows] firmware_dir`
   directory without an RMS-level per-caller or aggregate disk quota.
   `validate_download_size` rejects zero-byte downloads and detects mismatches
   when `Content-Length` is present, but a server that omits `Content-Length`
   can still stream a large response until the request timeout, available
   memory, or backing filesystem stops it. Firmware jobs are serialized per
   rack/node in `crates/rackmanagementservice/src/orchestrator/job_tracker.rs`, and terminal jobs are
   retained for one hour with lazy cleanup. High-volume or long-running
   firmware activity can therefore consume disk, memory, network, and worker
   capacity unless the deployment supplies storage quotas, monitoring, and
   operational concurrency limits.

### Critical Security Assumptions

- **`redfish_test_support` is test-only and out of production scope**:
  the workspace member provides Redfish simulator fixtures for tests and
  benchmarks and is only consumed as a dev-dependency. Findings isolated to
  that crate are not production RMS vulnerabilities unless they affect release
  confidence or the deployed binaries.
- **mTLS is the production authentication boundary**: RMS does not implement
  application-level users, roles, or per-RPC authorization. Production callers
  are assumed to be authenticated and authorized before they can reach the RMS
  gRPC listener.
- **`[tls] insecure = true` is development-only**: plaintext gRPC is assumed to run
  only on local or isolated test networks where all clients are trusted.
- **The management network is trusted and segmented**: BMC, switch, Redfish,
  NVUE, SSH/SFTP, and NMX-C endpoints are assumed to be reachable only from
  trusted infrastructure. SSH host-key verification is not enforced by RMS or
  `rust_nvfwupd`.
- **BMC endpoint identity relies on management-network controls**:
  BMC/Redfish remains HTTPS, but RMS does not validate BMC server certificates
  in current deployments. Client endpoint request fields cannot change this
  behavior; it is an RMS-owned BMC-only policy.
- **Firmware artifacts are approved before RMS applies them**: RMS validates
  paths, filenames, object IDs, and basic transfer size, and can authenticate
  artifact repository downloads with a caller-supplied JFrog token. It assumes
  release, signing, artifact repository, NVFWUPD, BMC, switch, or hardware
  mechanisms establish firmware authenticity and target compatibility because
  RMS does not independently enforce artifact host allow-lists, trusted
  manifests, hashes, or signatures.
- **Resource limits are supplied by deployment infrastructure**: RMS assumes
  Kubernetes, hostPath/storage configuration, monitoring, and operational
  process constrain firmware-cache growth, high-volume artifact downloads, and
  long-running job volume.
- **Kubernetes Secrets and host paths are protected**: TLS keys, database
  credentials, switch certificates, client certificates, image pull secrets, and
  the `[workflows] firmware_dir`/hostPath contents are assumed to be accessible only to the
  RMS workload and authorized operators.
- **PostgreSQL is secured and backed up when persistence matters**: RMS assumes
  the database enforces access control, transport security, durability, and
  backup/restore policy. The in-memory backend is not a durable production
  persistence mode.
- **Container and image supply chain are controlled**: the runtime image,
  bundled `nvfwupd` binary, `ipmitool`, Helm chart, and NGC/GitLab CI artifacts
  are assumed to come from trusted build and promotion pipelines.
- **Downstream devices enforce operation safety**: RMS orchestrates device
  actions, but BMCs, switches, NVUE, NMX-C, Redfish services, and device
  firmware are assumed to enforce their own command semantics, package checks,
  and final safety interlocks.

## Deployment Guidance

- Keep Helm defaults that enable mTLS unless deploying to a local development
  environment. Do not expose an insecure RMS listener on shared, staging, or
  production networks.
- Store API server TLS material, database credentials, switch certificate
  material, and NMX-C client certificates in Kubernetes Secrets or an approved
  external secret manager. Use `[switches] insecure_switch` / `apiServer.insecureSwitch`
  only for lab or bootstrap cases where switch mTLS must be bypassed. In this
  mode, `ConfigureSwitchCertificate` unsets switch service mTLS mode over SSH
  instead of installing certificate material. Do not commit site-specific secret
  values in Helm overrides.
- Restrict network reachability to port 8801 and `/metrics` to trusted clients
  and monitoring systems.
- Restrict the `[workflows] firmware_dir` directory and any hostPath backing it to authorized
  operators and the RMS workload. Treat write access to that directory as a
  firmware-update privilege.
- Configure storage quotas, free-space monitoring, and alerting for the
  firmware cache and any hostPath that backs it. Treat artifact download volume
  and job count as operational capacity controls.
- Treat firmware-object URLs and local artifact paths as trusted operator input.
  Prefer approved internal artifact repositories, restrict who can create
  firmware objects, and enforce any site-specific URL allow-list or artifact
  approval workflow outside RMS if required.
- Use PostgreSQL with TLS and managed credentials for any deployment where
  firmware object state or apply history must survive restarts.
- Treat device endpoint credentials supplied through gRPC as sensitive. Avoid
  logging raw protobuf requests or storing unredacted request captures.

## Out of Scope

The following are normally outside the RMS vulnerability boundary unless they
also bypass an RMS trust boundary:

- Reports that require intentionally running RMS with `insecure = true` on an
  untrusted network contrary to deployment guidance
- Attacks requiring administrator access to Kubernetes Secrets, the RMS hostPath
  firmware directory, or the PostgreSQL administrator account
- Malicious firmware packages that are accepted by the authorized firmware
  release/signing process and by the target device's own verification logic
- Compromise of BMC, switch, NMX-C, PostgreSQL, NGC, or Kubernetes control-plane
  components without an RMS-specific boundary bypass
- Vulnerabilities isolated to `redfish_test_support` test fixtures unless they
  affect production release confidence or the deployed RMS / `rust_nvfwupd`
  binaries
