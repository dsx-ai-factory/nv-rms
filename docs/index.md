# NVIDIA Rack Management Service

The Rack Management Service (RMS) is a stateless, rack-level hardware management
service for NVIDIA data-center infrastructure. It exposes a single gRPC API that
trusted services and operators use to run power control, inventory, firmware
updates, and switch configuration against targets ranging from a full rack down
to an individual component.

RMS translates client intent into hardware actions across multiple management
protocols: it talks to compute BMCs and power-shelf controllers over
Redfish/HTTPS, and to NVSwitch trays over NVUE REST, SSH, SFTP, and NMX-C gRPC.
Long-running work (firmware updates, switch system-image installs) runs as
asynchronous jobs that clients poll to completion.

## Where to Go Next

| | **Run & Operate RMS** | **Integrate with RMS** | **Evaluate RMS** |
| --- | --- | --- | --- |
| **Who** | Operators deploying and running RMS against real racks | Platform engineers building on the RMS gRPC API | Architects evaluating RMS for their stack |
| **Start here** | [Getting Started: Prerequisites](getting-started/prerequisites.md) | [Architecture](architecture/external_view.md) | [Overview](overview.md) |
| **Then** | [Deployment](deployment/prerequisites.md) | [Operations (RPC reference)](operations/overview.md) | [Hardware Compatibility List](reference/hcl.md) |
| **Then** | [Configuration](configuration/configuring-rms.md) | [Development](reference/development.md) | [Glossary](reference/glossary.md) |

## Quick Links

- [Hardware Compatibility List](reference/hcl.md) - Supported racks, trays, and power shelves
- [Configuration Reference](configuration/configuring-rms.md) - Every `config.toml` key and its Helm value
- [Operations](operations/overview.md) - The `RackManager` gRPC RPCs
- [GitHub](https://github.com/NVIDIA/nv-rms)
