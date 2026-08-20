# External View

RMS is structured as thin, protocol-specific gateways over an orchestration
layer. A gRPC gateway translates requests into calls on the **orchestrator**,
which owns the rack map and the job tracker. **Rack** implementations create
**node** objects, and node implementations hide the protocol-specific I/O behind
a common trait. Nodes are pure async I/O - they have no knowledge of job tracking
or scheduling.

The service is built on **tokio** for async I/O, **tonic** for gRPC, and
trait-based polymorphism for extensibility across node and rack types.

For how these layers fit together internally, see [Internal View](internal_view.md).

## Northbound and southbound connections

RMS has one **northbound** (incoming client) interface - the `RackManager` gRPC
API - and several **southbound** (outgoing) interfaces it uses to reach hardware.

![RMS northbound and southbound connections](../diagrams/northbound-southbound.svg)

| Direction | Peer | Protocol(s) | Notes |
| --- | --- | --- | --- |
| Northbound | gRPC clients | gRPC over mTLS (TLS 1.3, rustls) | Single `RackManager` service; plaintext only in insecure dev mode. |
| Northbound | Prometheus | HTTP(S) `/metrics` | Independent listener; optional TLS reusing the gRPC server cert. |
| Southbound | Compute tray & power-shelf BMC | Redfish over HTTPS | Power, reset, device info, firmware inventory, multipart firmware upload. |
| Southbound | NVSwitch tray (NVUE/NVOS) | NVUE REST over HTTPS, SSH, SFTP | Switch firmware, system images, passwords, certificates, config. SFTP uploads switch OS images. |
| Southbound | NVSwitch tray (NMX-C) | NMX-C gRPC, gNMI | Scale-up fabric manager state and telemetry-interface control. |
