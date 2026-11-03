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
| `tls.certReloadIntervalSeconds` | `[tls] cert_reload_interval_seconds` | `300` |
| `serviceMonitor` / metrics TLS wiring | `[metrics] port` / `[metrics] tls` | `8802` / off |
| `switchCertRoot` / `switchCertCertificates` | `[switches] switch_cert_root` | - |
| `clientTlsRoot` / `clientTlsCertificates` | `[switches] client_tls_root` | - |
| `defaultSwitchDomain` | `[switches] default_switch_domain` | - |
| `dnsDomain` | `[switches] dns_domain` | - |
| `insecureSwitch` | `[switches] insecure_switch` | `false` |
| `insecureBmc` | `[bmc] insecure_bmc` | `true` |
| `nmxGatewayId` | `[switches] nmx_gateway_id` | `rack-manager-grpc-client` |
| `database.*` / `rmsPostgres.*` (via Secret → `DATABASE_URL`) | `[postgres] db_url` | in-memory if unset |
| `dbPoolMax` | `[postgres] db_pool_max` | `20` |
| `firmwareMountPath` / `firmwarePersistentVolumeClaim` / `firmwareStoragePath` | `[workflows] firmware_dir` | `firmware` |
| `sftpUploadTimeoutSeconds` | `[workflows] sftp_upload_timeout_seconds` | `3600` |
| `sftpStepTimeoutSeconds` | `[workflows] sftp_step_timeout_seconds` | `30` |
| `firmwareTaskTimeoutSeconds` | `[workflows] firmware_task_timeout_seconds` | `10800` |
| `onieRecoveryTimeoutSeconds` | `[workflows] onie_recovery_timeout_seconds` | `900` |
| `recoveredCredentialSaveTimeoutSeconds` | `[workflows] recovered_credential_save_timeout_seconds` | `300` |
| `firmwareTaskPollIntervalSeconds` | `[workflows] firmware_task_poll_interval_seconds` | `30` |
| `firmwareTaskTransientErrorTimeoutSeconds` | `[workflows] firmware_task_transient_error_timeout_seconds` | `900` |
| `computePowerOnTimeoutSeconds` | `[workflows] compute_power_on_timeout_seconds` | `60` |
| `computePowerOnPollIntervalSeconds` | `[workflows] compute_power_on_poll_interval_seconds` | `5` |
| `maxTrackedJobs` | `[workflows] max_tracked_jobs` | `10000` |
| `terminalJobTtlSeconds` | `[workflows] terminal_job_ttl_seconds` | `86400` |
| `expectedInventoryProfiles` | `[workflows.expected_inventory_profiles]` | empty |
| `artifactRequireSha256` | `[workflows] artifact_require_sha256` | `false` |
| `artifactHttpsHosts` | `[workflows] artifact_https_hosts` | empty |
| `artifactAllowPrivateTargets` | `[workflows] artifact_allow_private_targets` | `true` |
| `logLevel` | `[logging] log_level` | `info` + caps |
| `tracing.endpoint` | `[tracing] endpoint` | unset; no remote export |
| `tracing.protocol` | `[tracing] protocol` | `grpc` |
| `tracing.samplingRatio` | `[tracing] sampling_ratio` | `1.0` |
| `tracing.maxQueueSize` | `[tracing] max_queue_size` | `2048` |
| `tracing.maxExportBatchSize` | `[tracing] max_export_batch_size` | `512` |
| `tracing.scheduledDelayMs` | `[tracing] scheduled_delay_ms` | `5000` |
| `tracing.exportAttemptTimeoutMs` | `[tracing] export_attempt_timeout_ms` | `10000` |
| `tracing.exportTimeoutMs` | `[tracing] export_timeout_ms` | `30000` |
| `tracing.shutdownTimeoutMs` | `[tracing] shutdown_timeout_ms` | `5000` |
| `tracing.maxRetries` | `[tracing] max_retries` | `3` |
| `tracing.retryInitialDelayMs` | `[tracing] retry_initial_delay_ms` | `100` |
| `tracing.retryMaxDelayMs` | `[tracing] retry_max_delay_ms` | `1600` |
| `tracing.retryJitterMs` | `[tracing] retry_jitter_ms` | `100` |
| `terminationGracePeriodSeconds` | Pod shutdown budget for 300-second job drain plus trace flush and margin | `330` |
| `podAnnotations` | Extra RMS Pod annotations, not a `config.toml` key. `checksum/config` stays chart-owned. | `{}` |

The power-on step that reads `computePowerOnTimeoutSeconds` and
`computePowerOnPollIntervalSeconds` arrives in a following change. Until it
lands, RMS validates and stores both values and nothing reads them.

`artifactRequireSha256` and the host-policy keys default to unrestricted
HTTP(S) catalogs. To require a digest or restrict origins, see
[Configuring RMS: Artifact download policy](configuring-rms.md#artifact-download-policy).

See [Deployment](../deployment/kubernetes.md) for install/upgrade workflows and
[`helm/README.md`](https://github.com/dsx-ai-factory/nv-rms/blob/main/helm/README.md) for
the exhaustive values reference.
