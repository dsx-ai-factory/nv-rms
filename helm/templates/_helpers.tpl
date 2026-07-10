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
databaseMode "forge" is a deprecated alias kept for --reuse-values compatibility
with releases that stored the old value; new installs should use external.
*/}}
{{- define "rack-manager.useRmsPostgres" -}}
{{- $mode := default "standalone" .Values.databaseMode -}}
{{- if or (eq $mode "external") (eq $mode "forge") -}}
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
