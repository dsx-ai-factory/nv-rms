# insecureSwitch Test Cases

This document describes Helm values overrides and manual test procedures for
`apiServer.insecureSwitch`. Use these files to validate secure switch mTLS
(production default) and insecure switch mode (lab/bootstrap only).

## Overview

`insecureSwitch` controls **outbound** RMS client mTLS to switch services. It
changes NVUE and NMX-C differently and also changes certificate-install
behavior. It is independent of `apiServer.allowInsecure`, which controls
**inbound** gRPC client authentication to the RMS API server.

Responsibilities of security toggles:

| Setting | Helm value | config.toml key | Scope |
| ------- | ---------- | --------------- | ----- |
| Switch mTLS | `apiServer.insecureSwitch` | `[switches] insecure_switch` | RMS → switch outbound services |
| gRPC mTLS | `apiServer.allowInsecure` / `apiServer.tls` | `[tls] insecure` | Client → RMS API |

### Switch services

| Service | Switch-side name | Typical gRPC port | RMS RPC examples |
| ------- | ---------------- | ----------------- | ---------------- |
| `nvue_api` | `nvue-rest-api` | — (HTTPS REST) | NVUE configuration, `ConfigureSwitchCertificate` |
| `scale_up_fabric_manager` | `nmx-controller` | 9370 | `ConfigureScaleUpFabricManager` |
| `scale_up_fabric_telemetry_interface` | `gnmi-server` | 9339 | `ConfigureSwitchCertificate` gNMI Capabilities test |

`SetScaleUpFabricTelemetryInterfaceState` enables or disables `gnmi-server`
through NVUE REST. It is not a direct outbound gNMI call. The direct gNMI
Capabilities call exists only as a connectivity check during secure certificate
installation.

### What secure means

When `insecureSwitch` is `false` (production default), RMS uses **mutual TLS**
for switch connectivity. RMS presents client certificates and validates server
certificates for NVUE, NMX-C, and the secure-only gNMI connectivity check:

| Service | Secure (`insecureSwitch: false`) |
| ------- | ---------------- |
| **`nvue_api`** | **mTLS over HTTPS** - HTTPS with an RMS client certificate and server CA verification |
| **`scale_up_fabric_manager`** | **gRPC over HTTPS** — tonic gRPC over `https://` with client mTLS and server CA verification |
| **`scale_up_fabric_telemetry_interface`** | `SetScaleUpFabricTelemetryInterfaceState` uses the NVUE transport; secure `ConfigureSwitchCertificate` can test gNMI Capabilities with mTLS over `https://` |

Client and switch certificate secrets (`clientTlsCertificates`,
`switchCertCertificates`) and `defaultSwitchDomain` are required.

### What insecure means

`insecureSwitch` disables outbound switch client mTLS. It does not force every
switch call to plaintext HTTP. SSH and SFTP remain separate protocols:

| Service | Secure (`insecureSwitch: false`) | Insecure (`insecureSwitch: true`) |
| ------- | -------------------------------- | --------------------------------- |
| **`nvue_api`** | mTLS over **HTTPS** | **HTTPS** without an RMS client certificate or server certificate verification |
| **`scale_up_fabric_manager`** | gRPC over **HTTPS** (mTLS) | gRPC over **HTTP** — plaintext, non-TLS endpoint (`http://host:port`) |
| **`scale_up_fabric_telemetry_interface`** | State changes use secure NVUE; certificate installation can make a direct gNMI Capabilities mTLS call | State changes use unverified NVUE HTTPS; certificate cleanup uses SSH and skips the direct gNMI call |

NVUE traffic remains TLS encrypted in insecure mode, but RMS does not
authenticate the server. An active man-in-the-middle can therefore intercept
that traffic. NMX-C has neither encryption nor server authentication in this
mode.

### Behavior summary

| Mode | `insecureSwitch` | `nvue_api` | NMX-C | Direct gNMI Capabilities | Cert secrets |
| ---- | ---------------- | ---------- | ----- | ------------------------ | ------------ |
| Secure (production) | `false` | mTLS over HTTPS | gRPC over HTTPS (mTLS) | Secure certificate-install flow only | Required |
| Insecure (lab only) | `true` | HTTPS without client mTLS or server verification | gRPC over HTTP (plaintext) | Skipped | Not required |

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
NMX-C, and for the direct gNMI Capabilities check during secure certificate
installation. The rendered `config.toml` must not set `insecure_switch = true`;
client and switch cert material must be mounted and referenced by the config.

### TC1 prerequisites

- Kubernetes secrets with switch and RMS client cert material (`ca.crt`, `tls.crt`, `tls.key`)
- API server TLS secret (`apiServer.tls.existingSecret` or `--set-file` cert/key/ca)
- PostgreSQL reachable (or set `databaseMode: memory` for chart-only validation)
- Switches configured for mTLS (NVUE and NMX-C over HTTPS; secure gNMI
  connectivity check enabled)

### TC1 expected chart behavior

- Rendered `config.toml` sets `insecure_switch = false`
- `config.toml` includes `client_tls_root`, `switch_cert_root`, and `default_switch_domain`
- Volume mounts under `clientTlsRoot` and `switchCertRoot` per domain
- Pod starts only when `clientTlsCertificates` and `defaultSwitchDomain` are configured

### TC1 expected RMS runtime behavior

- Startup validates `client_tls_root` and `default_switch_domain`
- `nvue_api` calls use mTLS over HTTPS with server certificate verification
- `scale_up_fabric_manager` uses gRPC over HTTPS with client mTLS and server
  certificate verification
- `SetScaleUpFabricTelemetryInterfaceState` configures `gnmi-server` through
  the secure NVUE transport
- Secure `ConfigureSwitchCertificate` can run the direct gNMI Capabilities mTLS
  connectivity check
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

**Chart render (`insecure_switch = false`):**

```bash
helm template rack ./helm \
  -f helm/values.yaml \
  -f helm/examples/overrides/switch-mtls-values.yaml \
  --set global.image.tag=test \
  | grep -E 'insecure_switch|client_tls_root|switch_cert_root|default_switch_domain'
```

Expected output (from the rendered `config.toml`) includes `client_tls_root`,
`switch_cert_root`, `default_switch_domain`, and `insecure_switch = false`.

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

Verify RMS can reach switches without provisioning RMS client mTLS material.
The rendered `config.toml` sets `insecure_switch = true`. `nvue_api` remains
HTTPS without server verification, NMX-C uses plaintext HTTP, and no direct gNMI
Capabilities call is made.

### TC2 prerequisites

- Trusted management network or lab environment only — **not** for production
- API server TLS secret (`apiServer.tls.existingSecret`) unless using
  `local-dev-values.yaml` with `allowInsecure: true`
- Switches reachable over the management network with NVUE over HTTPS and NMX-C
  over plaintext HTTP
- No `clientTlsCertificates` or `switchCertCertificates` secrets required

### TC2 expected chart behavior

- Rendered `config.toml` sets `insecure_switch = true`
- `config.toml` omits `client_tls_root` and `switch_cert_root` when cert lists are empty
- No client-tls or switch-cert volume mounts
- Pod starts without `clientTlsCertificates` or `defaultSwitchDomain`

### TC2 expected RMS runtime behavior

- Startup skips `client_tls_root`, `switch_cert_root`, and `default_switch_domain` validation
- Startup logs warn that NVUE uses unverified HTTPS and NMX-C uses plaintext HTTP
- `nvue_api` uses **HTTPS** without an RMS client certificate or server
  certificate verification
- `scale_up_fabric_manager` uses **gRPC over HTTP** (`http://` target,
  non-TLS endpoint)
- `SetScaleUpFabricTelemetryInterfaceState` configures `gnmi-server` through
  the unverified NVUE HTTPS transport
- `ConfigureSwitchCertificate` skips the direct gNMI Capabilities connectivity
  test
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

**Chart render (`insecure_switch = true`):**

```bash
helm template rack ./helm \
  -f helm/values.yaml \
  -f helm/examples/overrides/switch-insecure-values.yaml \
  --set databaseMode=memory \
  --set global.image.tag=test \
  | grep -E 'insecure_switch|client_tls_root|switch_cert_root|default_switch_domain'
```

Expected output (from the rendered `config.toml`) includes `insecure_switch = true`
only. Output must **not** include `client_tls_root`, `switch_cert_root`, or
`default_switch_domain`.

**Runtime checklist:**

1. Confirm deployment pod reaches `Ready` without client TLS secrets
2. Invoke switch RPCs against lab switches without client cert provisioning
3. Confirm `nvue_api` uses unverified HTTPS and NMX-C uses plaintext HTTP
4. Invoke `ConfigureSwitchCertificate` and confirm response `mode` is
   `"insecure-switch"`

---

## Related configuration

| Value | Description |
| ----- | ----------- |
| `apiServer.clientTlsCertificates` | RMS client mTLS for `nvue_api`, `scale_up_fabric_manager`, and the secure gNMI connectivity check; required when `insecureSwitch` is `false` |
| `apiServer.switchCertCertificates` | Switch-side cert material installed by `ConfigureSwitchCertificate` |
| `apiServer.defaultSwitchDomain` | Default domain when switch RPCs omit `domain` |
| `apiServer.dnsDomain` | Optional DNS suffix for TLS server-name verification |

See also:

- [Configuration](README.md#configuration) in `helm/README.md`
- [SECURITY.md](../SECURITY.md) — trust boundaries and deployment guidance
- [README.md](../README.md) — the `insecure_switch` config key
