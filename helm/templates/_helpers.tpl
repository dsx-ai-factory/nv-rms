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
([metrics], [tls], [switches], [postgres], [workflows], [logging]) matching
the Rust `RmsConfig` struct.
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

[postgres]
db_pool_max = {{ .Values.apiServer.dbPoolMax }}

[workflows]
firmware_dir = {{ .Values.apiServer.firmwareMountPath | quote }}
max_tracked_jobs = {{ .Values.apiServer.maxTrackedJobs }}
terminal_job_ttl_seconds = {{ .Values.apiServer.terminalJobTtlSeconds }}
sftp_upload_timeout_seconds = {{ .Values.apiServer.sftpUploadTimeoutSeconds }}
sftp_step_timeout_seconds = {{ .Values.apiServer.sftpStepTimeoutSeconds }}
{{- if .Values.apiServer.expectedInventoryProfiles }}

[workflows.expected_inventory_profiles]
{{- range $profile := keys .Values.apiServer.expectedInventoryProfiles | sortAlpha }}
{{ $profile | quote }} = {{ index $.Values.apiServer.expectedInventoryProfiles $profile | toJson }}
{{- end }}
{{- end }}

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
