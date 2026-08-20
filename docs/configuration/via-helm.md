# Configuration via Helm

This page covers how the Helm chart translates its values into the RMS
`config.toml`. For the configuration keys themselves and their defaults, see
[Configuring RMS](configuring-rms.md).

The [Helm chart](../deployment/kubernetes.md) does not take a raw `config.toml`.
Instead it renders one from `apiServer.*` values into a ConfigMap
(`rms-api-config`) and mounts it read-only at `/etc/rms/config.toml`. The database
connection string is the only runtime override: it is built from Secret-backed
credentials and injected as `DATABASE_URL`, which supersedes the (empty)
`[postgres] db_url` in the ConfigMap so the password never lands in a ConfigMap.
Because RMS reads the file once at startup, the Deployment carries a
`checksum/config` annotation so config changes trigger a rollout.

The rendered `config.toml` groups keys into the same sections as the Rust config
structs. This table maps each Helm value to the `config.toml` key it produces:

| Helm value (`apiServer.*` unless noted) | `config.toml` key | Default |
| --- | --- | --- |
| `port` | `port` | `8801` |
| `allowInsecure` | `[tls] insecure` (+ `RMS_ALLOW_INSECURE=1` env, rendered together) | `false` |
| `tls.enabled` / `tls.existingSecret` / `tls.caEnabled` | `[tls] cert` / `key` / `ca` (mounted from the Secret) | TLS on |
| `serviceMonitor` / metrics TLS wiring | `[metrics] port` / `[metrics] tls` | `8802` / off |
| `switchCertRoot` / `switchCertCertificates` | `[switches] switch_cert_root` | - |
| `clientTlsRoot` / `clientTlsCertificates` | `[switches] client_tls_root` | - |
| `defaultSwitchDomain` | `[switches] default_switch_domain` | - |
| `dnsDomain` | `[switches] dns_domain` | - |
| `insecureSwitch` | `[switches] insecure_switch` | `false` |
| `nmxGatewayId` | `[switches] nmx_gateway_id` | `rack-manager-grpc-client` |
| `database.*` / `rmsPostgres.*` (via Secret → `DATABASE_URL`) | `[postgres] db_url` | in-memory if unset |
| `dbPoolMax` | `[postgres] db_pool_max` | `20` |
| `firmwareMountPath` / `firmwarePersistentVolumeClaim` / `firmwareStoragePath` | `[workflows] firmware_dir` | `firmware` |
| `sftpUploadTimeoutSeconds` | `[workflows] sftp_upload_timeout_seconds` | `3600` |
| `sftpStepTimeoutSeconds` | `[workflows] sftp_step_timeout_seconds` | `30` |
| `maxTrackedJobs` | `[workflows] max_tracked_jobs` | `10000` |
| `terminalJobTtlSeconds` | `[workflows] terminal_job_ttl_seconds` | `86400` |
| `expectedInventoryProfiles` | `[workflows.expected_inventory_profiles]` | empty |
| `logLevel` | `[logging] log_level` | `info` + caps |

See [Deployment](../deployment/kubernetes.md) for install/upgrade workflows and
[`helm/README.md`](https://github.com/NVIDIA/nv-rms/blob/main/helm/README.md) for
the exhaustive values reference.
