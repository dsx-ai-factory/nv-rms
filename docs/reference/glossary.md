# Glossary

Terms used throughout the RMS documentation.

<ParamField path="BMC">
  Baseboard Management Controller - the out-of-band controller RMS reaches over Redfish for compute and power-shelf nodes.
</ParamField>

<ParamField path="CPLD .vme">
  Complex Programmable Logic Device firmware image format used for switch CPLD components, extracted by nvfwupd.
</ParamField>

<ParamField path="Firmware manifest">
  The JSON, distributed per firmware milestone, that describes board SKUs, components, versions, and artifact locations. RMS parses it into a firmware object. See the [sample manifest](https://github.com/NVIDIA/nv-rms/tree/main/sample_firmware_manifest).
</ParamField>

<ParamField path="Firmware object">
  Persisted workflow state representing a parsed firmware manifest for a hardware type: a software object tree, resolved components, cached artifact metadata, an optional default marker, and apply history.
</ParamField>

<ParamField path=".fwpkg">
  A PLDM firmware package containing one or more component images.
</ParamField>

<ParamField path="gNMI">
  gRPC Network Management Interface - used for switch telemetry/config connectivity checks.
</ParamField>

<ParamField path="HMC">
  Hardware Management Controller - a management endpoint on some NVIDIA trays, addressed as a firmware target alongside the BMC.
</ParamField>

<ParamField path="In-memory store">
  The default persistence backend when no database URL is configured. Fast and dependency-free, but all workflow state is lost on restart.
</ParamField>

<ParamField path="Insecure mode">
  Plaintext, unauthenticated gRPC. Requires both `[tls] insecure = true` and the `RMS_ALLOW_INSECURE=1` env gate. Dev/test only.
</ParamField>

<ParamField path="Insecure switch">
  `[switches] insecure_switch = true` - RMS drops outbound switch client mTLS (unverified NVUE HTTPS, plaintext NMX-C). Dev/test only.
</ParamField>

<ParamField path="Job">
  An asynchronous unit of long-running work (firmware update, switch system-image install) tracked by ID. Clients poll job status until it reaches a terminal state.
</ParamField>

<ParamField path="JobTracker">
  The orchestrator component that owns job lifecycle: creation, status, retention (`max_tracked_jobs`), and TTL-based cleanup of terminal jobs (`terminal_job_ttl_seconds`).
</ParamField>

<ParamField path="librms">
  The external crate ([`nv-rms-client`](https://github.com/NVIDIA/nv-rms-client)) providing the `rack_manager.proto` gRPC service definitions, pinned by `rev` and fetched by Cargo.
</ParamField>

<ParamField path="MNNVLink topology">
  Multi-node NVLink topology data (chassis serial, slot, tray index) RMS reads from the Redfish Processor OEM block.
</ParamField>

<ParamField path="mTLS">
  Mutual TLS - the default for both the northbound gRPC API and outbound switch connections. Requires a server cert/key plus a client CA.
</ParamField>

<ParamField path="NMX-C">
  The NMX Controller / scale-up fabric manager on NVSwitch trays. RMS configures it and queries its state over gRPC (`nmx_gateway_id` identifies RMS on these requests).
</ParamField>

<ParamField path="Node">
  An ephemeral object representing one managed hardware component (a compute tray, NVSwitch tray, or power shelf). Nodes are created per request and hold their transport client and credentials; RMS stores no device state beyond an operation's lifetime.
</ParamField>

<ParamField path="Node descriptor">
  The `role` / `vendor` / `product_family` attributes a client sends to select a node type at the gRPC boundary (e.g. `compute` / `nvidia` / `gb200`). See [HCL: node descriptor translation](hcl.md#node-descriptor-translation).
</ParamField>

<ParamField path="Node type">
  The concrete hardware identity RMS dispatches on, e.g. `compute_gb200_nvidia`, `switch_gb200_nvidia`, `powershelf_gb200_delta`. See the [HCL](hcl.md).
</ParamField>

<ParamField path="nvfwupd">
  The embedded firmware-update engine (library + CLI). RMS drives it through its workflow API; the CLI also ships in the release image for diagnostics.
</ParamField>

<ParamField path="NVUE / NVOS">
  NVIDIA's switch REST API (NVUE) and operating system (NVOS) on NVSwitch trays. RMS talks to switches over NVUE REST plus SSH/SFTP.
</ParamField>

<ParamField path="Parent/child job">
  A batch operation creates its parent job first, then attaches one child job per admitted node, so callers can track the batch as a whole and per node.
</ParamField>

<ParamField path="Persistence backend">
  The swappable store behind the `FirmwareObjectStore` trait: an in-memory store (default, volatile) or Postgres (durable).
</ParamField>

<ParamField path="PLDM">
  Platform Level Data Model - the firmware-package format (`.fwpkg`) parsed by the embedded nvfwupd crate.
</ParamField>

<ParamField path="Product family">
  The rack generation a node belongs to: GB200, GB300, or VRNVL72. RMS rejects nodes whose product family doesn't match the rack type.
</ParamField>

<ParamField path="Rack">
  A trait-based container that owns a map of nodes and a node factory. RMS ships `NvlGb200Rack`, `NvlGb300Rack`, and `NvlVrnvl72Rack`.
</ParamField>

<ParamField path="Redfish">
  The DMTF standard HTTP/HTTPS management API RMS uses to talk to compute and power-shelf BMCs.
</ParamField>

<ParamField path="RMS">
  Rack Management Service - the stateless, rack-level hardware management service documented here. Exposes a single `RackManager` gRPC API.
</ParamField>

<ParamField path="Scale-up fabric manager">
  The NMX-C-managed NVLink fabric. RMS can query cluster state, enable/disable it on switch targets, and control telemetry interfaces.
</ParamField>
