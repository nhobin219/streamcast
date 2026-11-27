{{- define "streamcast.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamcast.fullname" -}}
{{- default (printf "%s-%s" .Release.Name (include "streamcast.name" .)) .Values.fullnameOverride | trunc 58 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamcast.selector" -}}
app.kubernetes.io/name: streamcast
streamcast.io/broker: {{ include "streamcast.fullname" . }}
{{- end -}}

{{- define "streamcast.labels" -}}
{{ include "streamcast.selector" . }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/component: {{ include "streamcast.fullname" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{- define "streamcast.serviceAccount" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "streamcast.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "streamcast.image" -}}
{{- printf "%s:%s" .repository .tag -}}
{{- end -}}

{{- define "streamcast.mounts" -}}
- name: data
  mountPath: {{ .Values.persistence.mountPath | quote }}
{{- range .Values.persistence.aliasMounts }}
- name: data
  mountPath: {{ . | quote }}
{{- end }}
- name: run
  mountPath: {{ .Values.maintenance.runDir.path | quote }}
- name: tmp
  mountPath: /tmp
{{- with .Values.extraVolumeMounts }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "streamcast.security" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end -}}

{{/* Inline TOML tables preserve JSON-schema objects and nested policy fields. */}}
{{- define "streamcast.tomlValue" -}}
{{- if kindIs "map" . -}}
{{- $fields := list -}}
{{- range $key, $value := . -}}
{{- $fields = append $fields (printf "%s = %s" ($key | toJson) (include "streamcast.tomlValue" $value)) -}}
{{- end -}}
{{- printf "{%s}" (join ", " $fields) -}}
{{- else if kindIs "slice" . -}}
{{- $items := list -}}
{{- range . -}}
{{- $items = append $items (include "streamcast.tomlValue" .) -}}
{{- end -}}
{{- printf "[%s]" (join ", " $items) -}}
{{- else -}}
{{- toJson . -}}
{{- end -}}
{{- end -}}

{{- define "streamcast.config" -}}
{{- $config := .Values.broker.config -}}
{{- $policy := deepCopy (default dict $config.maintain) -}}
{{- $groups := list -}}
{{- range .Values.maintenance.groups -}}
{{- $groups = append $groups .name -}}
{{- end -}}
{{- $_ := set $policy "external" (dict "run_dir" .Values.maintenance.runDir.path "groups" $groups) -}}
streams = {{ include "streamcast.tomlValue" (default list $config.streams) }}
[server]
host = "0.0.0.0"
port = {{ .Values.broker.port }}
[broker]
{{ range $key, $value := omit $config "storage_profiles" "streams" "maintain" "authorization" -}}
{{ $key | toJson }} = {{ include "streamcast.tomlValue" $value }}
{{ end -}}
{{ if .Values.broker.authorization -}}
authorization = {{ .Values.broker.authorization | toJson }}
{{ end -}}
maintain = {{ include "streamcast.tomlValue" $policy }}
[storage_profiles]
{{ range $key, $value := $config.storage_profiles -}}
{{ $key | toJson }} = {{ include "streamcast.tomlValue" $value }}
{{ end -}}
{{- end -}}

{{- define "streamcast.validate" -}}
{{- if not (has .Values.scaleOwner (list "chart" "external")) -}}
{{- fail "scaleOwner must be chart or external" -}}
{{- end -}}
{{- if ne (toString .Values.replicas) "1" -}}
{{- fail "replicas must be 1: one broker owns the data volume" -}}
{{- end -}}
{{- if and (eq .Values.scaleOwner "external") (not .Values.persistence.existingClaim) -}}
{{- fail "scaleOwner external requires persistence.existingClaim" -}}
{{- end -}}
{{- if not (hasPrefix "/" .Values.maintenance.runDir.path) -}}
{{- fail "maintenance.runDir.path must be absolute" -}}
{{- end -}}
{{- if not .Values.maintenance.groups -}}
{{- fail "maintenance.groups must not be empty" -}}
{{- end -}}
{{- $names := dict -}}
{{- range .Values.maintenance.groups -}}
{{- if or (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" .name)) (gt (len .name) 40) (hasKey $names .name) -}}
{{- fail "maintenance group names must be unique, short lowercase identifiers" -}}
{{- end -}}
{{- $_ := set $names .name true -}}
{{- if and (not $.Values.broker.command) (not (hasKey (default dict $.Values.broker.config.storage_profiles) .name)) -}}
{{- fail "every maintenance group must name a broker.config.storage_profiles entry" -}}
{{- end -}}
{{- end -}}
{{- if not .Values.broker.command -}}
{{- if and .Values.broker.config.registry (not .Values.broker.authorization) -}}
{{- fail "broker.authorization is required with broker.config.registry" -}}
{{- end -}}
{{- if hasKey .Values.broker.config "maintain" -}}
{{- if not (kindIs "map" .Values.broker.config.maintain) -}}
{{- fail "broker.config.maintain must be a maintenance policy table" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
