# insecureSwitch Test Cases

This document describes Helm values overrides and manual test procedures for
`apiServer.insecureSwitch`. Use these files to validate secure switch mTLS
(production default) and insecure switch mode (lab/bootstrap only).

## Overview

`insecureSwitch` controls **outbound** RMS connectivity to switch services
(`nvue_api`, `scale_up_fabric_manager`, and `scale_up_fabric_telemetry_interface`).
It is independent of `apiServer.allowInsecure`, which controls **inbound** gRPC
client authentication to the RMS API server.

Responsibilities of security toggles:

| Setting | Helm value | CLI flag | Scope |
| ------- | ---------- | -------- | ----- |
| Switch mTLS | `apiServer.insecureSwitch` | `--insecure-switch` | RMS → switch outbound services |
| gRPC mTLS | `apiServer.allowInsecure` / `apiServer.tls` | `--insecure` | Client → RMS API |

### Switch services

| Service | Switch-side name | Typical gRPC port | RMS RPC examples |
| ------- | ---------------- | ----------------- | ---------------- |
| `nvue_api` | `nvue-rest-api` | — (HTTPS REST) | NVUE configuration, `ConfigureSwitchCertificate` |
| `scale_up_fabric_manager` | `nmx-controller` | 9370 | `ConfigureScaleUpFabricManager` |
| `scale_up_fabric_telemetry_interface` | `gnmi-server` | 9339 | `SetScaleUpFabricTelemetryInterfaceState`, gNMI Capabilities test |

`scale_up_fabric_telemetry_interface` follows the same secure/insecure transport
rules as `scale_up_fabric_manager`: both are outbound gRPC clients from RMS to
the switch.

### What secure means

When `insecureSwitch` is `false` (production default), RMS uses **mutual TLS**
for switch connectivity. Each service keeps its normal transport with full
client and server certificate verification:

| Service | Secure (`insecureSwitch: false`) |
| ------- | ---------------- |
| **`nvue_api`** | **mTLS over HTTPS** — HTTPS with RMS client certificate and server CA verification |
| **`scale_up_fabric_manager`** | **gRPC over HTTPS** — tonic gRPC over `https://` with client mTLS and server CA verification |
| **`scale_up_fabric_telemetry_interface`** | **gRPC over HTTPS** — same as `scale_up_fabric_manager` (gNMI Capabilities over `https://`) |

Client and switch certificate secrets (`clientTlsCertificates`,
`switchCertCertificates`) and `defaultSwitchDomain` are required.

### What insecure means

`insecureSwitch` does **not** downgrade every switch protocol to plaintext. It
disables **mutual TLS** (RMS client certificates and switch cert provisioning)
while leaving each service on its normal transport:

| Service | Secure (`insecureSwitch: false`) | Insecure (`insecureSwitch: true`) |
| ------- | -------------------------------- | --------------------------------- |
| **`nvue_api`** | mTLS over **HTTPS** | **HTTPS (TLS)** — encrypted, but no RMS client certificate and no server certificate verification |
| **`scale_up_fabric_manager`** | gRPC over **HTTPS** (mTLS) | gRPC over **HTTP** — plaintext, non-TLS endpoint (`http://host:port`) |
| **`scale_up_fabric_telemetry_interface`** | gRPC over **HTTPS** (mTLS) | gRPC over **HTTP** — same pattern as `scale_up_fabric_manager` |

In short: secure mode uses mTLS over HTTPS for `nvue_api` and gRPC over HTTPS
for `scale_up_fabric_manager` and `scale_up_fabric_telemetry_interface`.
Insecure mode keeps `nvue_api` on HTTPS (TLS only) and moves both gRPC services
to HTTP.

### Behavior summary

| Mode | `insecureSwitch` | `nvue_api` | `scale_up_fabric_manager` / `scale_up_fabric_telemetry_interface` | Cert secrets |
| ---- | ---------------- | ---------- | ----------------------------------------------------------------- | ------------ |
| Secure (production) | `false` | mTLS over HTTPS | gRPC over HTTPS (mTLS) | Required |
| Insecure (lab only) | `true` | HTTPS (TLS), no client mTLS, no server verify | gRPC over HTTP (plaintext) | Not required |

When `insecureSwitch` is `true`, `ConfigureSwitchCertificate` does not install
switch certificate material. It runs in unset-mode and removes switch service
mTLS settings over SSH. The RPC response `mode` field is `"insecure-switch"`.

## Test value files

Layer these on top of `helm/values.yaml` with `-f` (later files win).

| File | Purpose |
| ---- | ------- |
| [`switch-mtls-values.yaml`](./examples/overrides/switch-mtls-values.yaml) | Production-style secure switch mTLS |
| [`switch-insecure-values.yaml`](./examples/overrides/switch-insecure-values.yaml) | Lab/bootstrap with switch mTLS disabled |
| [`local-dev-values.yaml`](./examples/overrides/local-dev-values.yaml) | Local k3s: `insecureSwitch: true`, `allowInsecure: true`, in-memory DB |

Replace placeholder Kubernetes secret names with your site PKI before deploying.

---

## Test case 1: `insecureSwitch: false` (secure switch mTLS)

**File:** `helm/examples/overrides/switch-mtls-values.yaml`

### TC1 objective

Verify RMS outbound switch connectivity uses mTLS over HTTPS for `nvue_api` and
gRPC over HTTPS for `scale_up_fabric_manager` and
`scale_up_fabric_telemetry_interface`. The chart must not pass
`--insecure-switch`; client and switch cert material must be mounted and
forwarded to the binary.

### TC1 prerequisites

- Kubernetes secrets with switch and RMS client cert material (`ca.crt`, `tls.crt`, `tls.key`)
- API server TLS secret (`apiServer.tls.existingSecret` or `--set-file` cert/key/ca)
- PostgreSQL reachable (or set `databaseMode: memory` for chart-only validation)
- Switches configured for mTLS (`nvue_api` mTLS over HTTPS; gRPC services gRPC
  over HTTPS)

### TC1 expected chart behavior

- Deployment args omit `--insecure-switch`
- Args include `--client-tls-root`, `--switch-cert-root`, and `--default-switch-domain`
- Volume mounts under `clientTlsRoot` and `switchCertRoot` per domain
- Pod starts only when `clientTlsCertificates` and `defaultSwitchDomain` are configured

### TC1 expected RMS runtime behavior

- Startup validates `client-tls-root` and `default-switch-domain`
- `nvue_api` calls use mTLS over HTTPS (RMS client certificate and server CA verification)
- `scale_up_fabric_manager` and `scale_up_fabric_telemetry_interface` calls use
  gRPC over HTTPS with client mTLS and server certificate verification
- `ConfigureSwitchCertificate` installs switch-side cert material (not unset-mode)
- Startup log does **not** warn about insecure-switch

### TC1 install

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  -f helm/examples/overrides/switch-mtls-values.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

### TC1 verification

**Chart render (no `--insecure-switch`):**

```bash
helm template rack ./helm \
  -f helm/values.yaml \
  -f helm/examples/overrides/switch-mtls-values.yaml \
  --set global.image.tag=test \
  | grep -E 'insecure-switch|client-tls-root|switch-cert-root|default-switch-domain'
```

Expected output includes `client-tls-root`, `switch-cert-root`, and
`default-switch-domain`. Output must **not** include `insecure-switch`.

**Runtime checklist:**

1. Confirm deployment pod reaches `Ready`
2. Invoke switch RPCs (for example `ConfigureSwitchCertificate`,
   `ConfigureScaleUpFabricManager`, `SetScaleUpFabricTelemetryInterfaceState`)
3. Confirm switch-side mTLS is active and RMS presents client certificates
4. Confirm `ConfigureSwitchCertificate` response `mode` is **not** `"insecure-switch"`

---

## Test case 2: `insecureSwitch: true` (lab / bootstrap)

**File:** `helm/examples/overrides/switch-insecure-values.yaml`

For full local k3s (plaintext inbound gRPC, in-memory DB, image pull secret), use
[`local-dev-values.yaml`](./examples/overrides/local-dev-values.yaml) instead.

### TC2 objective

Verify RMS can reach switches without provisioning RMS client mTLS material. The
chart passes `--insecure-switch`. `nvue_api` remains on HTTPS (TLS); gRPC
services (`scale_up_fabric_manager`, `scale_up_fabric_telemetry_interface`) use
gRPC over HTTP.

### TC2 prerequisites

- Trusted management network or lab environment only — **not** for production
- API server TLS secret (`apiServer.tls.existingSecret`) unless using
  `local-dev-values.yaml` with `allowInsecure: true`
- Switches reachable over the management network (`nvue_api` on HTTPS; gRPC
  services on HTTP when mTLS is disabled)
- No `clientTlsCertificates` or `switchCertCertificates` secrets required

### TC2 expected chart behavior

- Deployment args include `--insecure-switch`
- Args omit `--client-tls-root` and `--switch-cert-root` when cert lists are empty
- No client-tls or switch-cert volume mounts
- Pod starts without `clientTlsCertificates` or `defaultSwitchDomain`

### TC2 expected RMS runtime behavior

- Startup skips `client-tls-root`, `switch-cert-root`, and `default-switch-domain` validation
- Startup logs warn that insecure-switch disables switch client mTLS
- `nvue_api` uses **HTTPS (TLS)** with no RMS client certificate and server certificate verification disabled
- `scale_up_fabric_manager` and `scale_up_fabric_telemetry_interface` use
  **gRPC over HTTP** (`http://` target, non-TLS endpoint)
- `ConfigureSwitchCertificate` runs in unset-mode (removes switch mTLS via SSH)
- `ConfigureSwitchCertificate` response `mode` is `"insecure-switch"`

### TC2 install

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  -f helm/examples/overrides/switch-insecure-values.yaml \
  --set databaseMode=memory \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

### TC2 verification

**Chart render (`--insecure-switch` only):**

```bash
helm template rack ./helm \
  -f helm/values.yaml \
  -f helm/examples/overrides/switch-insecure-values.yaml \
  --set databaseMode=memory \
  --set global.image.tag=test \
  | grep -E 'insecure-switch|client-tls-root|switch-cert-root|default-switch-domain'
```

Expected output includes `insecure-switch` only. Output must **not** include
`client-tls-root`, `switch-cert-root`, or `default-switch-domain`.

**Runtime checklist:**

1. Confirm deployment pod reaches `Ready` without client TLS secrets
2. Invoke switch RPCs against lab switches without client cert provisioning
3. Confirm `nvue_api` connectivity over HTTPS (TLS, unverified) and gRPC
   services over HTTP
4. Invoke `ConfigureSwitchCertificate` and confirm response `mode` is
   `"insecure-switch"`

---

## Related configuration

| Value | Description |
| ----- | ----------- |
| `apiServer.clientTlsCertificates` | RMS client mTLS for `nvue_api`, `scale_up_fabric_manager`, and `scale_up_fabric_telemetry_interface`; required when `insecureSwitch` is `false` |
| `apiServer.switchCertCertificates` | Switch-side cert material installed by `ConfigureSwitchCertificate` |
| `apiServer.defaultSwitchDomain` | Default domain when switch RPCs omit `domain` |
| `apiServer.dnsDomain` | Optional DNS suffix for TLS server-name verification |

See also:

- [Configuration](README.md#configuration) in `helm/README.md`
- [SECURITY.md](../SECURITY.md) — trust boundaries and deployment guidance
- [README.md](../README.md) — CLI flags `--insecure-switch` and `RMS_INSECURE_SWITCH`
