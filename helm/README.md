# Rack Manager Helm Chart

Helm chart for deploying Rack Management Service (RMS) on Kubernetes: API server
and optional in-cluster PostgreSQL or external PostgreSQL. The API server
applies embedded sqlx database migrations automatically on startup.

## Versioning

This chart uses **two independent version axes**:

| Axis              | Source                        | Example               | Meaning                            |
| ----------------- | ----------------------------- | --------------------- | ---------------------------------- |
| **Chart version** | `Chart.yaml` `version`        | `0.8.0-rc1`           | Semver for template/values changes |
| **App image tag** | `global.image.tag` at install | `v0.8.0-rc4`          | RMS container images to run        |

- Chart version is bumped in `Chart.yaml` whenever `helm/**` changes (enforced in CI).
- CI packages with `helm package --version <chart>` and `--app-version <git-describe>`; the
  artifact is named `rack-manager-<chart-version>.tgz`, not after the app image tag.
- Set `global.image.tag` (or per-component `apiServer.image.tag`)
  at install/upgrade to pick the application build.

Chart version policy:

| Change                         | Bump                                              |
| ------------------------------ | ------------------------------------------------- |
| Breaking values rename/removal | major                                             |
| New values, optional resources | minor                                             |
| Template bugfix, default tweak | patch                                             |
| Pre-release / RC packaging     | add or increment `-rcN` suffix (e.g. `0.8.0-rc1`) |
| GA release                     | drop pre-release suffix (e.g. `0.8.0`)            |

NGC publish (CI): GA git tags (`v1.0.0`) and pre-release git tags (`v0.8.0-rc1`) each
trigger a chart push; chart `version` in `Chart.yaml` should match the intended artifact
(e.g. tag `v0.8.0-rc1` → chart version `0.8.0-rc1` → `rack-manager-0.8.0-rc1.tgz`).
App pre-release builds remain selected via `global.image.tag`, not chart version.

### Chart version automation

Develop-branch chart edits use a **`-dev.N` pre-release suffix** so packaged artifacts are
clearly not RC/GA builds. RC and GA versions align with git tags (`vX.Y.Z-rcN`, `vX.Y.Z`).

| Workflow stage                | Chart version example | How to set                                                          |
| ----------------------------- | --------------------- | ------------------------------------------------------------------- |
| Feature / develop MR          | `0.8.1-dev.1`         | `helm/scripts/bump-chart-version.sh dev patch`                      |
| Another helm commit (same MR) | `0.8.1-dev.2`         | `helm/scripts/bump-chart-version.sh dev next`                       |
| RC cut                        | `0.8.0-rc8`           | `helm/scripts/bump-chart-version.sh rc next` or `rc 8 --base 0.8.0` |
| GA release                    | `0.8.0`               | `helm/scripts/bump-chart-version.sh release --base 0.8.0`           |
| Tagged release (CI / local)   | matches tag           | `helm/scripts/bump-chart-version.sh sync-tag v0.8.0-rc8`            |

Semver bump kind for `dev` follows the [policy table](#versioning) above (`patch` for template
tweaks, `minor` for new optional values, `major` for breaking changes).

#### Zero-touch pre-commit hook (optional)

Install once after clone:

```bash
./scripts/install-githooks.sh
```

On every commit that stages chart package inputs (except a version-only `Chart.yaml` chore),
the hook:

1. **Infers** `patch`, `minor`, or `major` from the staged diff (new values/templates → minor;
   removed values/templates/schema fields → major; otherwise patch).
2. **Auto-increments** `-dev.N` when the chart is already on a develop line (`dev next`).
3. **Stages** the updated `helm/Chart.yaml` into your commit.

Bump details are printed to the terminal only (not appended to the commit message), so
GitLab commit templates and `Signed-off-by` trailers stay intact.

Major bumps require confirmation unless you opt in with `HELM_CHART_BUMP_AUTO_MAJOR=1`.
Set `HELM_CHART_BUMP_INTERACTIVE=1` to always choose the bump at commit time.
Skip once with `SKIP_HELM_CHART_BUMP=1 git commit ...`.

Examples:

```bash
# After changing helm/templates or values.yaml on a feature branch:
./helm/scripts/bump-chart-version.sh dev patch

# Preparing an RC that will be tagged v0.8.0-rc8:
./helm/scripts/bump-chart-version.sh sync-tag v0.8.0-rc8
git add helm/Chart.yaml && git commit -m "chore(helm): chart 0.8.0-rc8 for v0.8.0-rc8"

# GA tag v0.8.0:
./helm/scripts/bump-chart-version.sh sync-tag v0.8.0
```

CI requires a `Chart.yaml` version change only when chart package inputs change:
`helm/Chart.yaml`, `helm/values.yaml`, `helm/values.schema.json`, `helm/.helmignore`,
`helm/templates/**`, `helm/crds/**`, or `helm/charts/**`. Documentation, example,
test, and helper-script changes under `helm/` do not require a chart version bump
by themselves. On tagged pipelines CI also verifies `Chart.yaml` matches the git tag
before publishing to NGC. `-dev.N` charts are built and linted in MR pipelines but
are **not** pushed to NGC (only `vX.Y.Z` and `vX.Y.Z-rcN` tags publish).

## Chart components

| Component          | Description                                                                        |
| ------------------ | ---------------------------------------------------------------------------------- |
| **RMS API Server** | gRPC API for rack management (default port 8801); runs sqlx migrations on startup. |
| **PostgreSQL**     | Optional in-cluster Postgres or external PostgreSQL.                               |

## Prerequisites

- **Kubernetes** cluster (1.24+)
- **Helm** 3.x
- **NGC** account and API key (for pulling the chart and images from NGC)

For the default **external database** mode:

- PostgreSQL cluster reachable at `database.host`; RMS database and user provisioned on that cluster
- DB credentials: External Secrets Operator with a `ClusterSecretStore`, or a pre-created secret (see `database.credentialsSecret`)
- API server TLS: server certificate, key, and client CA for mTLS (via `apiServer.tls.existingSecret` or `--set-file`; see [Configuration](#configuration))

## Install from NGC

Replace `<NGC_TOKEN>` with your [NGC API key](https://ngc.nvidia.com/setup/api-key),
`<CHART_VERSION>` with the chart semver (e.g. `0.8.0-rc1`), and `<APP_TAG>` with the RMS
image tag (e.g. `v0.8.0-rc4`):

```bash
helm pull https://helm.ngc.nvidia.com/nvidian/dcim/charts/rack-manager-<CHART_VERSION>.tgz \
  --username='$oauthtoken' \
  --password='<NGC_TOKEN>'
```

This creates `rack-manager-<CHART_VERSION>.tgz` in the current directory.

> **Note:** `helm pull` is the Helm 3 command to download charts.

Install with a dedicated namespace (recommended):

```bash
helm install rack ./rack-manager-<CHART_VERSION>.tgz \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

With a custom values file:

```bash
helm install rack ./rack-manager-<CHART_VERSION>.tgz \
  -f my-values.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

Verify:

```bash
helm list -n rack-manager
kubectl get pods -n rack-manager -l app.kubernetes.io/name=rack-manager
```

To install from a local checkout of this chart instead:

```bash
cd helm
helm install rack . \
  -f my-values.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

Or from the repository root:

```bash
helm install rack ./helm \
  -f my-values.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

### Manual install on local k3s

Local **k3s** clusters often deploy RMS via ArgoCD (which supplies
`ngc-dcim-imagepullsecret`). For a **manual** `helm install`, create a short
override file locally (do not commit it unless your site standardizes on the
name). Some environments distribute **`imagepullsecret`** via ESO; the chart
default leaves `global.imagePullSecrets` empty.

`helm/examples/overrides/local-dev-values.yaml` (layer with `-f`; do not commit unless
your site standardizes on the name):

```yaml
databaseMode: memory

global:
  imagePullSecrets:
    - name: imagepullsecret

apiServer:
  environmentPath: local-dev
  allowInsecure: true
  insecureSwitch: true
  clientTlsCertificates: []
  switchCertCertificates: []
  defaultSwitchDomain: ""
  tls:
    enabled: false
```

See [INSECURE_SWITCH_TESTING.md](INSECURE_SWITCH_TESTING.md) for `insecureSwitch`
test cases. For insecure switch only, use `helm/examples/overrides/switch-insecure-values.yaml`;
for full local k3s, use [local-dev-values.yaml](./examples/overrides/local-dev-values.yaml).

Install or upgrade:

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  -f helm/examples/overrides/local-dev-values.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

Use a full git-describe NGC tag (e.g. `v0.8.0-rc4-0-g76cf66d`), not a short
release name like `v0.8.0-rc4`. Confirm the secret exists:
`kubectl get secret -n rack-manager imagepullsecret`.

Equivalent one-liner without a file:

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  --set-json 'global.imagePullSecrets=[{"name":"imagepullsecret"}]' \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

## Upgrade

### Application image only (same chart)

Set `NEW_TAG` to the desired RMS image tag (e.g. `export NEW_TAG=v0.1.0-32-g7d6a4ca`):

```bash
helm upgrade rack ./rack-manager-<CHART_VERSION>.tgz -n rack-manager \
  --set global.image.tag=$NEW_TAG
```

Per-component override remains available:

```bash
helm upgrade rack ./rack-manager-<CHART_VERSION>.tgz -n rack-manager \
  --set apiServer.image.tag=$NEW_TAG
```

### Values file

Use a file such as `upgrade-values.yaml`:

```yaml
global:
  image:
    tag: v0.8.0-rc4
```

Or set the tag on the API server only:

```yaml
apiServer:
  image:
    tag: v0.8.8-rc4
```

Then run:

```bash
helm upgrade rack ./rack-manager-<CHART_VERSION>.tgz -f upgrade-values.yaml -n rack-manager
```

If you already use a values file for install, pass it first so other settings are preserved:

```bash
helm upgrade rack ./rack-manager-<CHART_VERSION>.tgz -f my-values.yaml -f upgrade-values.yaml -n rack-manager
```

After upgrade, the API server rolls to the new image and applies any pending sqlx
migrations on startup. If pods do not roll automatically:

```bash
kubectl rollout restart deployment/rms-api-server -n rack-manager
```

### Migrating from databaseMode: forge

Older releases may have `databaseMode: forge` stored in Helm release values. The
chart still accepts `forge` as a deprecated alias for `external`, so upgrades
with `--reuse-values` continue to work. Normalize stored values to `external`
once so future chart versions can drop the alias:

```bash
helm upgrade rack ./rack-manager-<CHART_VERSION>.tgz -n rack-manager \
  --reuse-values --set databaseMode=external
```

This is a one-time step. Later upgrades can use `--reuse-values` as usual.

## Configuration

Key sections in `values.yaml`:

| Section                     | Purpose                                                                       |
| --------------------------- | ----------------------------------------------------------------------------- |
| `global.image`              | Shared RMS API image tag and pull policy.                                     |
| `apiServer`                 | Image, replicas, port (8801), TLS, firmware path.                             |
| `database`                  | Host, port, DB name, `credentialsSecret`, `sslMode` (external DB).            |
| `databaseMode`              | `external` (default), `standalone`, or `memory` (in-memory, dev/test).        |
| `postgres`                  | Standalone Postgres configuration (for `databaseMode: "standalone"`).         |
| `rmsPostgres`               | ExternalSecret (ESO) and optional patch for external PostgreSQL.              |
| `dropDatabaseOnUninstall`   | If `true`, a pre-delete hook drops the release DB on uninstall.               |

By default, `apiServer.allowInsecure` is `false` and `apiServer.tls.enabled` is `true`.
The RMS binary requires mTLS (server cert/key plus client CA) unless `--insecure` is
explicitly passed. A bare install therefore needs TLS material in your values or via
`--set-file`; see the site and insecure-mode examples below.

`apiServer` also supports:

- `switchCertCertificates` / `switchCertRoot` — switch-side cert material
- `clientTlsCertificates` / `clientTlsRoot` — required RMS client mTLS for switch `nvue_api` (mTLS over HTTPS), `scale_up_fabric_manager` (gRPC over HTTPS), and `scale_up_fabric_telemetry_interface` (gRPC over HTTPS, same rules as `scale_up_fabric_manager`) unless `insecureSwitch` is true
- `defaultSwitchDomain` — default when switch RPCs omit domain
- `dnsDomain` — optional DNS domain used for switch, `scale_up_fabric_manager`, and `scale_up_fabric_telemetry_interface` TLS server-name verification, not the NVLink domain
- `insecureSwitch` — passes `--insecure-switch`; disables outbound switch client mTLS. `nvue_api` stays on HTTPS (TLS) without client certs or server verification; `scale_up_fabric_manager` and `scale_up_fabric_telemetry_interface` use gRPC over HTTP.
- `firmwarePersistentVolumeClaim` — existing PVC to mount at `firmwareMountPath` for firmware downloads. Leave empty to use the default `firmwareStoragePath` hostPath.
- `sftpUploadTimeoutSeconds` — overall SFTP upload wall-clock timeout (seconds); passed as `RMS_SFTP_UPLOAD_TIMEOUT_SECONDS`. Default: `3600`.
- `sftpStepTimeoutSeconds` — per-step SFTP stall timeout (seconds); passed as `RMS_SFTP_STEP_TIMEOUT_SECONDS`. Must be <= `sftpUploadTimeoutSeconds`. Default: `30`.

See [INSECURE_SWITCH_TESTING.md](INSECURE_SWITCH_TESTING.md) for values overrides and manual test cases for `insecureSwitch: false` and `true`.

When `insecureSwitch` is true, `ConfigureSwitchCertificate` does not install
switch certificate material. It creates unset-mode jobs that use SSH to remove
switch service mTLS settings for the requested services.

### Site-specific overrides

Keep `helm/values.yaml` as the base and layer site settings with `-f` (later files win).
Create a file such as `my-site-values.yaml` beside your other overrides (do not commit
secrets; use `existingSecret` or ESO in production):

```yaml
database:
  host: my-pg-cluster.postgres.svc.cluster.local
  name: my_rms_db
  credentialsSecret: rms-api-server.rack-manager.my-pg-cluster.credentials
rmsPostgres:
  externalSecret:
    enabled: true
    sourceSecretKey: rms-api-server.rack-manager.my-pg-cluster.credentials.postgresql.acid.zalan.do
  patchForgeCluster:
    enabled: true
    postgresqlName: my-pg-cluster
    databaseName: my_rms_db
apiServer:
  environmentPath: ipp6-gb200-36x1
  allowInsecure: false
  tls:
    enabled: true
    caEnabled: true
    existingSecret: rms-api-server-tls
```

Install or upgrade:

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  -f my-site-values.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

### Firmware PVC

The firmware download filesystem defaults to a node `hostPath` from
`apiServer.firmwareStoragePath`. To use persistent cluster storage, create a
site override with an existing claim name. Keep `apiServer.firmwarePersistentVolumeClaim`
empty in `helm/values.yaml`; set it only in a site values file layered with `-f`.

```yaml
apiServer:
  firmwarePersistentVolumeClaim: rms-firmware-downloads
```

When `apiServer.replicaCount` is greater than 1, use a PVC backed by storage
that supports mounting from every scheduled API server pod, such as
`ReadWriteMany`. `ReadWriteOnce` volumes from common block storage classes can
usually attach to only one node at a time, causing additional replicas on other
nodes to fail with Multi-Attach errors. If you use `ReadWriteOncePod`, keep the
deployment constrained to a single pod/node.

Install or upgrade with the PVC override after the base values:

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  -f helm/examples/overrides/firmware-pvc-values.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

### Insecure Mode

The chart defaults to **mTLS** (`apiServer.allowInsecure: false`, `apiServer.tls.enabled: true`).
For **local development and testing** only, you may opt into plaintext gRPC with no client
certificate verification. This avoids provisioning a PKI on a laptop or k3s cluster when you
are iterating on application logic, running `first_run.sh`-style bootstrap scripts, or
connecting tools that do not yet have client certificates. It is **not** appropriate for
shared, staging, or production clusters: any client that can reach port 8801 can invoke every
RPC (firmware flash, power control, switch password rotation) with no authentication.

Create `values-local-dev.yaml` in your environment repo (not shipped with this chart):

```yaml
apiServer:
  environmentPath: local-dev
  allowInsecure: true
  tls:
    enabled: false
```

When `allowInsecure: true`, the chart sets `RMS_ALLOW_INSECURE=1` in the pod
environment so the binary honors `--insecure`. Without that env var, the process
exits at startup even if the flag is passed.

Install or upgrade with the override layered on the chart base:

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  -f values-local-dev.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

Equivalent one-liner without a file:

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  --set apiServer.allowInsecure=true \
  --set apiServer.tls.enabled=false \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

You can combine this with [`local-dev-values.yaml`](./examples/overrides/local-dev-values.yaml) for
`global.imagePullSecrets`, `insecureSwitch`, and in-memory DB under
[Manual install on local k3s](#manual-install-on-local-k3s).

### In-Memory Mode

Set `databaseMode: memory` to skip all database wiring. No credentials secret,
`rms-database-config` ConfigMap, or drop-database hook are created. The RMS binary
detects the absent `DATABASE_URL` and falls back to its built-in in-memory store
automatically. **Data is lost on restart** — use only for development and testing.

```bash
helm install rack ./rack-manager-<CHART_VERSION>.tgz -n rack-manager --create-namespace \
  --set apiServer.image.repository="<REGISTRY>/rms-release" \
  --set apiServer.image.tag="<APP_TAG>" \
  --set apiServer.allowInsecure=true \
  --set apiServer.tls.enabled=false \
  --set databaseMode=memory
```

Example overrides (shared tag):

```yaml
global:
  image:
    tag: v0.8.0-rc4

# Keep database on uninstall
dropDatabaseOnUninstall: false

apiServer:
  switchCertRoot: /var/run/secrets/switch-cert
  switchCertCertificates:
    - domain: site-wide
      secret: rms-switch-cert-material
  clientTlsRoot: /var/run/secrets/client-tls
  clientTlsCertificates:
    - domain: site-wide
      secret: rms-nmxc-client-certificate
    - domain: ipp6-gb200-36x1.forge
      secret: rms-nmxc-client-certificate-ipp6
  defaultSwitchDomain: site-wide
```

Per-component image tag (overrides `global.image.tag`):

```yaml
apiServer:
  image:
    tag: v0.8.0-rc4
```

## Access the API in-cluster

Port-forward to the API server:

```bash
export NAMESPACE=rack-manager   # or your release namespace
kubectl port-forward -n $NAMESPACE svc/rms-api-server 8801:8801
```

Then connect to `localhost:8801` (or the port you set in `apiServer.port`).

## Uninstall

```bash
helm uninstall rack -n rack-manager
```

If `dropDatabaseOnUninstall: true`, a pre-delete hook runs a job to drop the release database. Set it to `false` in values if you want to keep the database.

## Chart layout

```text
.
├── Chart.yaml
├── scripts/
│   └── test-chart.sh
├── tests/
│   └── api-server-firmware-storage_test.yaml
├── values.yaml
├── values.schema.json
└── templates/
    ├── _helpers.tpl
    ├── namespace.yaml
    ├── api-server-deployment.yaml
    ├── api-server-configmap.yaml
    ├── api-server-tls-secret.yaml
    ├── api-server-servicemonitor.yaml
    ├── grafana-dashboard.yaml
    ├── api-server-servicemonitor.yaml
    ├── grafana-dashboard.yaml
    ├── postgres.yaml
    ├── postgres-credentials-secret.yaml
    ├── rms-postgres-external-secret.yaml
    ├── rms-postgres-patch-job.yaml
    ├── drop-database-job.yaml
    └── NOTES.txt
```

## Chart Tests

Chart unit tests use the `helm-unittest` test format. Run them with:

```bash
bash helm/scripts/test-chart.sh
```

The script uses a local `helm unittest` plugin when installed. If the plugin is
missing and Docker is available, it runs the same tests with the
`helmunittest/helm-unittest` container image.
