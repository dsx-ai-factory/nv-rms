{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/}}
{{/*
Expand the name of the chart.
*/}}
{{- define "rack-manager.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name. When release name equals chart name (no overrides),
use a single name to avoid "rack-manager-rack-manager" and thus secret "rack-manager-rack-manager-postgres-credentials".
*/}}
{{- define "rack-manager.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if and (eq .Release.Name $name) (not .Values.nameOverride) (not .Values.fullnameOverride) }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Namespace for main release (rack-manager)
*/}}
{{- define "rack-manager.namespace" -}}
{{- default .Release.Namespace .Values.namespace }}
{{- end }}

{{/*
True when using standalone in-cluster Postgres (databaseMode == standalone and postgres.enabled).
*/}}
{{- define "rack-manager.useStandalonePostgres" -}}
{{- if and (eq (default "standalone" .Values.databaseMode) "standalone") .Values.postgres.enabled -}}
true
{{- end -}}
{{- end -}}

{{/*
True when using an external PostgreSQL cluster (databaseMode == external).
*/}}
{{- define "rack-manager.useRmsPostgres" -}}
{{- $mode := default "standalone" .Values.databaseMode -}}
{{- if eq $mode "external" -}}
true
{{- end -}}
{{- end -}}

{{/*
True when using in-memory persistence (databaseMode == memory).
No database credentials, ConfigMap, or drop-database hook are created in this mode.
Data is lost on restart — intended for development and testing only.
*/}}
{{- define "rack-manager.useMemoryPersistence" -}}
{{- if eq (default "" .Values.databaseMode) "memory" -}}
true
{{- end -}}
{{- end -}}

{{/*
Credentials secret name for DB: standalone postgres secret, or external database.credentialsSecret.
*/}}
{{- define "rack-manager.databaseCredentialsSecret" -}}
{{- if include "rack-manager.useStandalonePostgres" . }}
{{- $auth := index .Values.postgres "auth" }}
{{- if and $auth $auth.existingSecret }}
{{- $auth.existingSecret }}
{{- else }}
{{- printf "%s-postgres-credentials" (include "rack-manager.fullname" .) }}
{{- end }}
{{- else if include "rack-manager.useRmsPostgres" . }}
{{- required "database.credentialsSecret must be set when databaseMode is \"external\"" .Values.database.credentialsSecret }}
{{- else if .Values.database.credentialsSecret }}
{{- .Values.database.credentialsSecret }}
{{- else }}
{{- fail "database.credentialsSecret must be set when not using standalone in-cluster Postgres" }}
{{- end }}
{{- end }}

{{/*
Effective firmware PVC name: apiServer.firmwarePersistentVolumeClaim when set,
otherwise "<fullname>-firmware" (only used when the chart creates the PVC).
*/}}
{{- define "rack-manager.firmwarePvcName" -}}
{{- .Values.apiServer.firmwarePersistentVolumeClaim | default "" | trim | default (printf "%s-firmware" (include "rack-manager.fullname" .)) -}}
{{- end -}}

{{/*
True when the firmware volume should be backed by a PVC rather than hostPath:
either the chart is creating one (apiServer.firmwarePvc.create) or an existing
claim name was supplied (apiServer.firmwarePersistentVolumeClaim).
*/}}
{{- define "rack-manager.firmwareUsePvc" -}}
{{- $pvc := .Values.apiServer.firmwarePvc | default dict -}}
{{- if or $pvc.create (.Values.apiServer.firmwarePersistentVolumeClaim | default "" | trim) -}}
true
{{- end -}}
{{- end -}}

{{/*
Image pull secrets: global.imagePullSecrets (legacy apiServer.imagePullSecrets accepted).
Use inline: {{ $s := .Values.global.imagePullSecrets | default .Values.apiServer.imagePullSecrets }}
*/}}

{{/*
RMS application image tag: component override, then global.image.tag, then Chart appVersion.
*/}}
{{- define "rack-manager.rmsImageTag" -}}
{{- $componentTag := "" -}}
{{- if .component.image -}}
{{- $componentTag = .component.image.tag | default "" -}}
{{- end -}}
{{- $tag := $componentTag | default .root.Values.global.image.tag -}}
{{- if not $tag -}}
{{- $appVersion := .root.Chart.AppVersion -}}
{{- if and $appVersion (ne $appVersion "0.0.0") -}}
{{- $tag = $appVersion -}}
{{- else -}}
{{- fail "global.image.tag must be set at install/upgrade (e.g. --set global.image.tag=<rms-build-tag>)" -}}
{{- end -}}
{{- end -}}
{{- $tag -}}
{{- end -}}

{{- define "rack-manager.apiServerImageTag" -}}
{{- include "rack-manager.rmsImageTag" (dict "root" . "component" .Values.apiServer) -}}
{{- end -}}

{{/*
Image pull policy: component override, then global.image.pullPolicy.
*/}}
{{- define "rack-manager.rmsImagePullPolicy" -}}
{{- $componentPolicy := "" -}}
{{- if .component.image -}}
{{- $componentPolicy = .component.image.pullPolicy | default "" -}}
{{- end -}}
{{- $componentPolicy | default .root.Values.global.image.pullPolicy | default "IfNotPresent" -}}
{{- end -}}

{{- define "rack-manager.apiServerImagePullPolicy" -}}
{{- include "rack-manager.rmsImagePullPolicy" (dict "root" . "component" .Values.apiServer) -}}
{{- end -}}

{{/*
NVSSVT sidecar image reference. Versioned independently of RMS, so it does not
fall back to global.image.tag the way the RMS components do.
*/}}
{{- define "rack-manager.nvssvtImage" -}}
{{- $repository := required "nvssvt.image.repository must be set when nvssvt.enabled is true" .Values.nvssvt.image.repository -}}
{{- $tag := required "nvssvt.image.tag must be set when nvssvt.enabled is true" .Values.nvssvt.image.tag -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

{{/*
Validate SFTP upload timeout tunables.
Fails helm rendering when the constraint sftpStepTimeoutSeconds <= sftpUploadTimeoutSeconds
is violated, or when either value is zero (the binary rejects both at startup).
*/}}
{{- define "rack-manager.validateSftpOptions" -}}
{{- $upload := .Values.apiServer.sftpUploadTimeoutSeconds | int -}}
{{- $step := .Values.apiServer.sftpStepTimeoutSeconds | int -}}
{{- if le $upload 0 -}}
{{- fail "apiServer.sftpUploadTimeoutSeconds must be greater than 0" -}}
{{- end -}}
{{- if le $step 0 -}}
{{- fail "apiServer.sftpStepTimeoutSeconds must be greater than 0" -}}
{{- end -}}
{{- if gt $step $upload -}}
{{- fail "apiServer.sftpStepTimeoutSeconds must be <= apiServer.sftpUploadTimeoutSeconds" -}}
{{- end -}}
{{- end -}}

{{/*
Validate Grafana dashboard values when grafana.dashboard.enabled is true.
Fails helm rendering when datasourceUid is unset or empty, surfacing the
misconfiguration at template time rather than silently at runtime.
*/}}
{{- define "rack-manager.validateGrafanaDashboard" -}}
{{- if not .Values.grafana.dashboard.datasourceUid -}}
{{- fail "grafana.dashboard.datasourceUid must not be empty when grafana.dashboard.enabled is true" -}}
{{- end -}}
{{- end -}}

{{/*
Validate firmware task poll tunables.
Fails helm rendering when any value is zero or when the constraint
firmwareTaskPollIntervalSeconds <= firmwareTaskTimeoutSeconds is violated
(the binary rejects all of these at startup).
*/}}
{{- define "rack-manager.validateFirmwareTaskOptions" -}}
{{- $timeout := .Values.apiServer.firmwareTaskTimeoutSeconds | int -}}
{{- $interval := .Values.apiServer.firmwareTaskPollIntervalSeconds | int -}}
{{- $transient := .Values.apiServer.firmwareTaskTransientErrorTimeoutSeconds | int -}}
{{- if le $timeout 0 -}}
{{- fail "apiServer.firmwareTaskTimeoutSeconds must be greater than 0" -}}
{{- end -}}
{{- if le $interval 0 -}}
{{- fail "apiServer.firmwareTaskPollIntervalSeconds must be greater than 0" -}}
{{- end -}}
{{- if le $transient 0 -}}
{{- fail "apiServer.firmwareTaskTransientErrorTimeoutSeconds must be greater than 0" -}}
{{- end -}}
{{- if gt $interval $timeout -}}
{{- fail "apiServer.firmwareTaskPollIntervalSeconds must be <= apiServer.firmwareTaskTimeoutSeconds" -}}
{{- end -}}
{{- end -}}

{{/*
Validate the compute power-on tunables.
Fails helm rendering when a value is zero or when
computePowerOnPollIntervalSeconds <= computePowerOnTimeoutSeconds is violated
(the binary rejects both at startup).
*/}}
{{- define "rack-manager.validateComputePowerOnOptions" -}}
{{- $timeout := .Values.apiServer.computePowerOnTimeoutSeconds | int -}}
{{- $interval := .Values.apiServer.computePowerOnPollIntervalSeconds | int -}}
{{- if le $timeout 0 -}}
{{- fail "apiServer.computePowerOnTimeoutSeconds must be greater than 0" -}}
{{- end -}}
{{- if le $interval 0 -}}
{{- fail "apiServer.computePowerOnPollIntervalSeconds must be greater than 0" -}}
{{- end -}}
{{- if gt $interval $timeout -}}
{{- fail "apiServer.computePowerOnPollIntervalSeconds must be <= apiServer.computePowerOnTimeoutSeconds" -}}
{{- end -}}
{{- end -}}

{{/*
Validate bounded tracing settings that depend on more than one value.
*/}}
{{- define "rack-manager.validateTracingOptions" -}}
{{- $tracing := .Values.apiServer.tracing | default dict -}}
{{- $endpoint := dig "endpoint" "" $tracing -}}
{{- $protocol := dig "protocol" "grpc" $tracing -}}
{{- $ratio := dig "samplingRatio" 1.0 $tracing | float64 -}}
{{- $queue := dig "maxQueueSize" 2048 $tracing | int -}}
{{- $batch := dig "maxExportBatchSize" 512 $tracing | int -}}
{{- $attemptTimeout := dig "exportAttemptTimeoutMs" 10000 $tracing | int -}}
{{- $exportTimeout := dig "exportTimeoutMs" 30000 $tracing | int -}}
{{- $retryInitial := dig "retryInitialDelayMs" 100 $tracing | int -}}
{{- $retryMax := dig "retryMaxDelayMs" 1600 $tracing | int -}}
{{- $shutdownTimeout := dig "shutdownTimeoutMs" 5000 $tracing | int -}}
{{- $terminationGrace := include "rack-manager.terminationGracePeriodSeconds" . | int -}}
{{- $requiredGrace := add 305 (div (add $shutdownTimeout 999) 1000) -}}
{{- if or (le $ratio 0.0) (gt $ratio 1.0) -}}
{{- fail "apiServer.tracing.samplingRatio must be in (0.0, 1.0]" -}}
{{- end -}}
{{- if gt $batch $queue -}}
{{- fail "apiServer.tracing.maxExportBatchSize must be <= apiServer.tracing.maxQueueSize" -}}
{{- end -}}
{{- if gt $attemptTimeout $exportTimeout -}}
{{- fail "apiServer.tracing.exportAttemptTimeoutMs must be <= apiServer.tracing.exportTimeoutMs" -}}
{{- end -}}
{{- if gt $retryInitial $retryMax -}}
{{- fail "apiServer.tracing.retryInitialDelayMs must be <= apiServer.tracing.retryMaxDelayMs" -}}
{{- end -}}
{{- if $endpoint -}}
{{- if regexMatch "\\s" $endpoint -}}
{{- fail "apiServer.tracing.endpoint must not contain whitespace" -}}
{{- end -}}
{{- if or (contains "?" $endpoint) (contains "#" $endpoint) -}}
{{- fail "apiServer.tracing.endpoint must not contain a query or fragment" -}}
{{- end -}}
{{- if regexMatch "^[A-Za-z][A-Za-z0-9+.-]*://[^/@]+@" $endpoint -}}
{{- fail "apiServer.tracing.endpoint must not contain user information" -}}
{{- end -}}
{{- $parsed := urlParse $endpoint -}}
{{- /* Plaintext only to a standard loopback IP literal, matching RMS's own check. */ -}}
{{- $loopback := or (regexMatch "^127(\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3}$" $parsed.hostname) (eq $parsed.hostname "::1") -}}
{{- if not (or (eq $parsed.scheme "https") (and (eq $parsed.scheme "http") $loopback)) -}}
{{- fail "apiServer.tracing.endpoint scheme must be https; plaintext http is allowed only for a loopback IP address such as 127.0.0.1 or [::1]" -}}
{{- end -}}
{{- if $parsed.userinfo -}}
{{- fail "apiServer.tracing.endpoint must not contain user information" -}}
{{- end -}}
{{- if not $parsed.hostname -}}
{{- fail "apiServer.tracing.endpoint must include a host" -}}
{{- end -}}
{{- if and (eq $protocol "grpc") $parsed.path (ne $parsed.path "/") -}}
{{- fail "apiServer.tracing.endpoint must not contain a path for grpc" -}}
{{- end -}}
{{- if and (eq $protocol "http_protobuf") (not (hasSuffix "/v1/traces" $parsed.path)) -}}
{{- fail "apiServer.tracing.endpoint must end with /v1/traces for http_protobuf" -}}
{{- end -}}
{{- end -}}
{{- if lt $terminationGrace $requiredGrace -}}
{{- fail (printf "apiServer.terminationGracePeriodSeconds must be at least %d for the RMS job drain, tracing shutdown timeout, and margin" $requiredGrace) -}}
{{- end -}}
{{- end -}}

{{/*
Validate apiServer.strategy: Kubernetes rejects a Deployment that sets
rollingUpdate alongside type: Recreate, or one that can neither surge nor
replace pods (maxSurge 0 with maxUnavailable 0). Included only from the
api-server Deployment template so Helm attributes the render error
deterministically.
*/}}
{{- define "rack-manager.validateStrategyOptions" -}}
{{- $strategy := .Values.apiServer.strategy | default dict -}}
{{- if and $strategy.rollingUpdate (eq ($strategy.type | default "") "Recreate") -}}
{{- fail "apiServer.strategy.rollingUpdate is not allowed when apiServer.strategy.type is Recreate; Kubernetes rejects that Deployment strategy combination" -}}
{{- end -}}
{{- $ru := $strategy.rollingUpdate | default dict -}}
{{- if and (has ($ru.maxSurge | toString) (list "0" "0%")) (has ($ru.maxUnavailable | toString) (list "0" "0%")) -}}
{{- fail "apiServer.strategy.rollingUpdate cannot set both maxSurge and maxUnavailable to 0; Kubernetes rejects that Deployment strategy combination" -}}
{{- end -}}
{{- end -}}

{{/*
Validate the chart-created firmware PVC's ReadWriteOncePod constraints, which
Kubernetes otherwise only rejects (or deadlocks on) after apply: the mode must
be the sole accessModes entry, only one pod may mount the claim, and the
rollout must delete the old pod before creating its replacement (strategy.type
Recreate, or a RollingUpdate with maxSurge 0). Included only from the firmware
PVC template so Helm attributes the render error deterministically.
*/}}
{{- define "rack-manager.validateFirmwarePvcOptions" -}}
{{- $pvc := .Values.apiServer.firmwarePvc | default dict -}}
{{- if and .Values.apiServer.enabled $pvc.create -}}
{{- $accessModes := $pvc.accessModes | default (list "ReadWriteOnce") -}}
{{- if and (has "ReadWriteOncePod" $accessModes) (gt (len $accessModes) 1) -}}
{{- fail "apiServer.firmwarePvc.accessModes cannot combine ReadWriteOncePod with other access modes; Kubernetes requires it to be the only access mode" -}}
{{- end -}}
{{- if has "ReadWriteOncePod" $accessModes -}}
{{- $replicas := .Values.apiServer.replicaCount | int -}}
{{- if gt $replicas 1 -}}
{{- fail (printf "apiServer.firmwarePvc.accessModes ReadWriteOncePod allows only one pod to mount the claim; set apiServer.replicaCount to 1 (got %d)" $replicas) -}}
{{- end -}}
{{- $strategy := .Values.apiServer.strategy | default dict -}}
{{- $deleteFirst := or (eq ($strategy.type | default "") "Recreate") (has (($strategy.rollingUpdate | default dict).maxSurge | toString) (list "0" "0%")) -}}
{{- if not $deleteFirst -}}
{{- fail "apiServer.firmwarePvc.accessModes ReadWriteOncePod requires a delete-before-create rollout; set apiServer.strategy.type: Recreate or apiServer.strategy.rollingUpdate.maxSurge: 0" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "rack-manager.terminationGracePeriodSeconds" -}}
{{- dig "terminationGracePeriodSeconds" 330 .Values.apiServer -}}
{{- end -}}

{{/*
Guard against values that still use the old rmsPostgres.patchForgeCluster key
(renamed to rmsPostgres.patchExternalCluster). values.schema.json does not
constrain rmsPostgres, so Helm silently merges the stale key and would
otherwise resolve patchExternalCluster to its default -- the pre-install
Postgres patch job never runs, with no error. Fail loudly at render time,
mirroring the schema hard-fail for the removed databaseMode "forge" alias.
*/}}
{{- define "rack-manager.validateRmsPostgresKeys" -}}
{{- if hasKey (.Values.rmsPostgres | default dict) "patchForgeCluster" -}}
{{- fail "rmsPostgres.patchForgeCluster has been renamed to rmsPostgres.patchExternalCluster; update your values files before upgrading." -}}
{{- end -}}
{{- end -}}

{{/*
Render the RMS runtime configuration (config.toml body) from apiServer values.
Defined as a named template so both the ConfigMap and the Deployment's
checksum/config annotation share a single source of truth. The database
connection string is intentionally omitted; it is injected via DATABASE_URL.
Configuration beyond the top-level `port` is grouped into TOML sections
([metrics], [tls], [switches], [bmc], [postgres], [workflows], [tracing], [logging])
matching the Rust `RmsConfig` struct.
*/}}
{{- define "rack-manager.apiServerConfigToml" -}}
port = {{ .Values.apiServer.port }}

[metrics]
port = {{ .Values.apiServer.metrics.port }}
tls = {{ and .Values.apiServer.metrics.tls.enabled .Values.apiServer.tls.enabled }}

[tls]
{{- if .Values.apiServer.tls.enabled }}
cert = {{ printf "%s/tls.crt" .Values.apiServer.tls.secretMountPath | quote }}
key = {{ printf "%s/tls.key" .Values.apiServer.tls.secretMountPath | quote }}
{{- if .Values.apiServer.tls.caEnabled }}
ca = {{ printf "%s/ca.crt" .Values.apiServer.tls.secretMountPath | quote }}
{{- end }}
{{- end }}
insecure = {{ .Values.apiServer.allowInsecure }}
cert_reload_interval_seconds = {{ .Values.apiServer.tls.certReloadIntervalSeconds | default 300 }}

[switches]
insecure_switch = {{ .Values.apiServer.insecureSwitch }}
{{- if .Values.apiServer.switchCertCertificates }}
switch_cert_root = {{ required "apiServer.switchCertRoot must be set when switchCertCertificates is configured" .Values.apiServer.switchCertRoot | quote }}
{{- end }}
{{- if .Values.apiServer.clientTlsCertificates }}
client_tls_root = {{ required "apiServer.clientTlsRoot must be set when clientTlsCertificates is configured" .Values.apiServer.clientTlsRoot | quote }}
{{- end }}
{{- if .Values.apiServer.defaultSwitchDomain }}
default_switch_domain = {{ .Values.apiServer.defaultSwitchDomain | quote }}
{{- end }}
{{- if .Values.apiServer.dnsDomain }}
dns_domain = {{ .Values.apiServer.dnsDomain | quote }}
{{- end }}
nmx_gateway_id = {{ .Values.apiServer.nmxGatewayId | quote }}

[bmc]
insecure_bmc = {{ .Values.apiServer.insecureBmc }}

[postgres]
db_pool_max = {{ .Values.apiServer.dbPoolMax }}

[workflows]
firmware_dir = {{ .Values.apiServer.firmwareMountPath | quote }}
max_tracked_jobs = {{ .Values.apiServer.maxTrackedJobs }}
terminal_job_ttl_seconds = {{ .Values.apiServer.terminalJobTtlSeconds }}
sftp_upload_timeout_seconds = {{ .Values.apiServer.sftpUploadTimeoutSeconds }}
sftp_step_timeout_seconds = {{ .Values.apiServer.sftpStepTimeoutSeconds }}
firmware_task_timeout_seconds = {{ .Values.apiServer.firmwareTaskTimeoutSeconds }}
onie_recovery_timeout_seconds = {{ .Values.apiServer.onieRecoveryTimeoutSeconds }}
recovered_credential_save_timeout_seconds = {{ .Values.apiServer.recoveredCredentialSaveTimeoutSeconds }}
firmware_task_poll_interval_seconds = {{ .Values.apiServer.firmwareTaskPollIntervalSeconds }}
firmware_task_transient_error_timeout_seconds = {{ .Values.apiServer.firmwareTaskTransientErrorTimeoutSeconds }}
compute_power_on_timeout_seconds = {{ .Values.apiServer.computePowerOnTimeoutSeconds }}
compute_power_on_poll_interval_seconds = {{ .Values.apiServer.computePowerOnPollIntervalSeconds }}
artifact_require_sha256 = {{ .Values.apiServer.artifactRequireSha256 }}
artifact_https_hosts = {{ default (list) .Values.apiServer.artifactHttpsHosts | toJson }}
artifact_allow_private_targets = {{ .Values.apiServer.artifactAllowPrivateTargets }}
{{- if .Values.apiServer.expectedInventoryProfiles }}

{{- range $profile := keys .Values.apiServer.expectedInventoryProfiles | sortAlpha }}
{{- $expected := index $.Values.apiServer.expectedInventoryProfiles $profile }}

[workflows.expected_inventory_profiles.{{ $profile | quote }}]
{{- if kindIs "slice" $expected }}
ap_names = {{ $expected | toJson }}
{{- else }}
ap_names = {{ default (list) (index $expected "apNames") | toJson }}
{{- $flintDevices := index $expected "flintDevices" }}
{{- if $flintDevices }}

[workflows.expected_inventory_profiles.{{ $profile | quote }}.flint_devices]
{{- if hasKey $flintDevices "cx7" }}
cx7 = {{ index $flintDevices "cx7" }}
{{- end }}
{{- if hasKey $flintDevices "cx8" }}
cx8 = {{ index $flintDevices "cx8" }}
{{- end }}
{{- if hasKey $flintDevices "bf3Nic" }}
bf3_nic = {{ index $flintDevices "bf3Nic" }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{- $tracing := .Values.apiServer.tracing | default dict }}
{{- $traceEndpoint := dig "endpoint" "" $tracing }}
{{- $traceProtocol := dig "protocol" "grpc" $tracing }}
{{- $traceSamplingRatio := dig "samplingRatio" 1.0 $tracing }}
{{- $traceMaxQueueSize := dig "maxQueueSize" 2048 $tracing }}
{{- $traceMaxExportBatchSize := dig "maxExportBatchSize" 512 $tracing }}
{{- $traceScheduledDelayMs := dig "scheduledDelayMs" 5000 $tracing }}
{{- $traceExportAttemptTimeoutMs := dig "exportAttemptTimeoutMs" 10000 $tracing }}
{{- $traceExportTimeoutMs := dig "exportTimeoutMs" 30000 $tracing }}
{{- $traceShutdownTimeoutMs := dig "shutdownTimeoutMs" 5000 $tracing }}
{{- $traceMaxRetries := dig "maxRetries" 3 $tracing }}
{{- $traceRetryInitialDelayMs := dig "retryInitialDelayMs" 100 $tracing }}
{{- $traceRetryMaxDelayMs := dig "retryMaxDelayMs" 1600 $tracing }}
{{- $traceRetryJitterMs := dig "retryJitterMs" 100 $tracing }}

[tracing]
{{- if $traceEndpoint }}
endpoint = {{ $traceEndpoint | quote }}
{{- end }}
protocol = {{ $traceProtocol | quote }}
sampling_ratio = {{ printf "%.17e" ($traceSamplingRatio | float64) }}
max_queue_size = {{ $traceMaxQueueSize }}
max_export_batch_size = {{ $traceMaxExportBatchSize }}
scheduled_delay_ms = {{ $traceScheduledDelayMs }}
export_attempt_timeout_ms = {{ $traceExportAttemptTimeoutMs }}
export_timeout_ms = {{ $traceExportTimeoutMs }}
shutdown_timeout_ms = {{ $traceShutdownTimeoutMs }}
max_retries = {{ $traceMaxRetries }}
retry_initial_delay_ms = {{ $traceRetryInitialDelayMs }}
retry_max_delay_ms = {{ $traceRetryMaxDelayMs }}
retry_jitter_ms = {{ $traceRetryJitterMs }}

[logging]
{{- if .Values.apiServer.logLevel }}
log_level = {{ .Values.apiServer.logLevel | quote }}
{{- end }}
enable_timestamps = {{ .Values.apiServer.enableTimestamps }}
{{- end -}}

{{/*
Default SPIFFE URI SAN shared by every chart-managed certificate.
Empty when certificates.spiffe.uri and certificates.spiffe.trustDomain are both unset.
*/}}
{{- define "rack-manager.spiffeUri" -}}
{{- $spiffe := .Values.certificates.spiffe | default dict -}}
{{- if $spiffe.uri -}}
{{- $spiffe.uri -}}
{{- else if $spiffe.trustDomain -}}
{{- printf "spiffe://%s/%s" $spiffe.trustDomain (trimPrefix "/" ($spiffe.path | default "")) | trimSuffix "/" -}}
{{- end -}}
{{- end -}}

{{/*
True when the chart issues the API server certificate itself.
*/}}
{{- define "rack-manager.apiServerCertificateEnabled" -}}
{{- if and .Values.certificates.enabled .Values.certificates.apiServer.enabled .Values.apiServer.enabled .Values.apiServer.tls.enabled -}}
true
{{- end -}}
{{- end -}}

{{/*
Secret written by the chart-managed API server Certificate.
*/}}
{{- define "rack-manager.apiServerCertificateSecret" -}}
{{- .Values.certificates.apiServer.secretName | default .Values.apiServer.tls.existingSecret | default "rms-api-server-certificate" -}}
{{- end -}}

{{/*
Secret the API server Deployment mounts for its own TLS material: chart-managed
Certificate, then apiServer.tls.existingSecret, then the chart-rendered Secret.
*/}}
{{- define "rack-manager.apiServerTlsSecretName" -}}
{{- if eq (include "rack-manager.apiServerCertificateEnabled" .) "true" -}}
{{- include "rack-manager.apiServerCertificateSecret" . -}}
{{- else if .Values.apiServer.tls.existingSecret -}}
{{- .Values.apiServer.tls.existingSecret -}}
{{- else -}}
{{- printf "%s-api-server-tls" (include "rack-manager.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Render one cert-manager Certificate. Fields resolve narrowest-scope-first:
per-entry override, then the certificates.<group> defaults, then the shared
certificates.* defaults.
Args: root, group, override, secretName, defaultDnsNames, defaultUris.
*/}}
{{- define "rack-manager.certificate" -}}
{{- $root := .root -}}
{{- $shared := $root.Values.certificates -}}
{{- $group := .group | default dict -}}
{{- $override := .override | default dict -}}
{{- /* These two maps merge per key, so a narrower scope replaces only the keys
it names. Any key a scope defines wins, including privateKey.size: 0 (reset to
the algorithm default); only an explicit null is skipped, since it would render
as a null field the API server rejects. */}}
{{- $issuerRef := dict -}}
{{- range $scope := list ($shared.issuerRef | default dict) ($group.issuerRef | default dict) ($override.issuerRef | default dict) -}}
{{- range $key, $value := $scope -}}
{{- if not (kindIs "invalid" $value) -}}
{{- $_ := set $issuerRef $key $value -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $privateKey := dict -}}
{{- range $scope := list ($shared.privateKey | default dict) ($group.privateKey | default dict) ($override.privateKey | default dict) -}}
{{- range $key, $value := $scope -}}
{{- if not (kindIs "invalid" $value) -}}
{{- $_ := set $privateKey $key $value -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $_ := required "certificates.issuerRef.name must be set when certificates.enabled is true" $issuerRef.name -}}
{{- $defaultUris := .defaultUris | default list -}}
{{- $name := $override.name | default .secretName -}}
{{- $duration := $override.duration | default $group.duration | default $shared.duration -}}
{{- $renewBefore := $override.renewBefore | default $group.renewBefore | default $shared.renewBefore -}}
{{- $commonName := $override.commonName | default $group.commonName | default $shared.commonName -}}
{{- $subject := $override.subject | default $group.subject | default $shared.subject -}}
{{- $secretTemplate := $override.secretTemplate | default $group.secretTemplate | default $shared.secretTemplate -}}
{{- $dnsNames := $override.dnsNames | default $group.dnsNames | default $shared.dnsNames | default (.defaultDnsNames | default list) -}}
{{- $ipAddresses := $override.ipAddresses | default $group.ipAddresses | default $shared.ipAddresses | default list -}}
{{- $uris := $override.uris | default $group.uris | default $shared.uris | default $defaultUris -}}
{{- $usages := $override.usages | default $group.usages | default $shared.usages | default list -}}
{{- $outputFormats := $override.additionalOutputFormats | default $group.additionalOutputFormats | default $shared.additionalOutputFormats | default list -}}
{{- if not (or $dnsNames $ipAddresses $uris $commonName) -}}
{{- fail (printf "certificate %q would have no dnsNames, uris, ipAddresses, or commonName; set dnsNames for it (switch server certificates default to apiServer.dnsDomain)" .secretName) -}}
{{- end -}}
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {{ $name }}
  namespace: {{ include "rack-manager.namespace" $root }}
  labels:
    {{- include "rack-manager.labels" $root | nindent 4 }}
    {{- with $shared.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with $shared.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  secretName: {{ .secretName }}
  {{- with $secretTemplate }}
  secretTemplate:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $duration }}
  duration: {{ . }}
  {{- end }}
  {{- with $renewBefore }}
  renewBefore: {{ . }}
  {{- end }}
  {{- with $privateKey }}
  privateKey:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $outputFormats }}
  additionalOutputFormats:
    {{- range . }}
    - type: {{ . }}
    {{- end }}
  {{- end }}
  {{- with $commonName }}
  commonName: {{ . | quote }}
  {{- end }}
  {{- with $subject }}
  subject:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $dnsNames }}
  dnsNames:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $ipAddresses }}
  ipAddresses:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $uris }}
  uris:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with $usages }}
  usages:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  issuerRef:
    {{- toYaml $issuerRef | nindent 4 }}
{{- end -}}

{{/*
Standard labels
*/}}
{{- define "rack-manager.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: {{ include "rack-manager.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}
