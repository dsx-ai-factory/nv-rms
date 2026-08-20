# Rack Manager Helm Chart

Helm chart for deploying Rack Management Service (RMS) on Kubernetes: API server
and optional in-cluster PostgreSQL or external PostgreSQL. The API server
applies embedded sqlx database migrations automatically on startup.

## Versioning

The chart version is a plain **`MAJOR.MINOR.PATCH`** semver that is **versioned
independently of the RMS application**. It carries **no `-rc` or `-dev` suffix**:
every chart change ships under a single, monotonically increasing semver, and the
developer chooses the bump for each change.

Two version axes stay independent:

| Axis              | Source                        | Example  | Meaning                                   |
| ----------------- | ----------------------------- | -------- | ----------------------------------------- |
| **Chart version** | `Chart.yaml` `version`        | `1.0.0`  | Semver for template/values/schema changes |
| **App image tag** | `global.image.tag` at install | `v1.2.3` | RMS container images to run               |

- The chart version describes the **chart** only, not the RMS release. Pick the
  RMS build to run with `global.image.tag` (or per-component
  `apiServer.image.tag`) at install/upgrade; it is unrelated to the chart version.
- CI packages with `helm package --version <chart-version> --app-version <git-describe>`.
  The artifact is named `rack-manager-<chart-version>.tgz`.

### When to bump

Bump `Chart.yaml` `version` on **any** change to chart package inputs:
`helm/Chart.yaml`, `helm/values.yaml`, `helm/values.schema.json`,
`helm/.helmignore`, `helm/templates/**`, `helm/crds/**`, or `helm/charts/**`.
Documentation, examples, tests, and helper scripts under `helm/` do **not**
require a bump by themselves.

| Change                                           | Bump  |
| ------------------------------------------------ | ----- |
| Breaking values rename/removal, removed template | major |
| New values, optional resources, new template     | minor |
| Template bugfix, default tweak, non-breaking     | patch |

Set the version by hand, or use the helper:

```bash
./helm/scripts/bump-chart-version.sh patch   # or minor / major
./helm/scripts/bump-chart-version.sh set 2.0.0
./helm/scripts/bump-chart-version.sh show
git add helm/Chart.yaml
```

### Enforcement

CI (`build-helm-chart-to-ngc`) fails a pipeline when chart package inputs change
but `Chart.yaml` `version` is unchanged, and also rejects any version that is not
a plain `MAJOR.MINOR.PATCH` (no `-rc`/`-dev`). The same check runs locally as a
pre-commit guard once you install the hooks:

```bash
./scripts/install-githooks.sh
```

The guard blocks a commit that stages chart package inputs without a version
bump and suggests the bump kind from the staged diff. Skip it once with
`SKIP_HELM_CHART_BUMP=1 git commit ...`.

### Publishing (CI)

A chart version is published exactly once, and only when the chart itself
changes. `push-helm-chart-to-ngc` runs when `Chart.yaml` changes on the default
branch or on a `release/*` branch, and it still checks NGC for that version
first and skips when it is already there. Whatever plain semver is in
`Chart.yaml` is published as `rack-manager-<chart-version>.tgz`.

RMS release tags do not publish the chart. The chart is versioned independently,
so a tag normally carries no chart change, and GitLab cannot filter on that in
tag pipelines because `rules:changes` always evaluates true there. Land the
version bump on the default branch or a `release/*` branch to ship it.

Published versions are immutable: the push does not force-overwrite. To ship a
chart change, bump `version` in `Chart.yaml` (CI already fails a chart change
that leaves it untouched).

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
`<CHART_VERSION>` with the chart semver (e.g. `1.0.0`), and `<APP_TAG>` with the RMS
image tag (e.g. `v0.8.0-rc4`):

```bash
helm pull https://helm.ngc.nvidia.com/0837451325059433/rms-dev/charts/rack-manager-<CHART_VERSION>.tgz \
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

This is a one-time step. Later upgrades can use `--reuse-values` as usual.

## Configuration

Key sections in `values.yaml`:

| Section                     | Purpose                                                                       |
| --------------------------- | ----------------------------------------------------------------------------- |
| `global.image`              | Shared RMS API image tag and pull policy.                                     |
| `apiServer`                 | Image, replicas, port (8801), TLS, firmware path.                             |
| `certificates`              | Opt-in cert-manager `Certificate` resources for API server and switch mTLS.   |
| `database`                  | Host, port, DB name, `credentialsSecret`, `sslMode` (external DB).            |
| `databaseMode`              | `external` (default), `standalone`, or `memory` (in-memory, dev/test).        |
| `postgres`                  | Standalone Postgres configuration (for `databaseMode: "standalone"`).         |
| `rmsPostgres`               | ExternalSecret (ESO) and optional patch for external PostgreSQL.              |
| `dropDatabaseOnUninstall`   | If `true`, a pre-delete hook drops the release DB on uninstall.               |

All RMS runtime configuration is delivered through a TOML file. The chart renders
it from `apiServer.*` values into a ConfigMap (`rms-api-config`) and mounts it
read-only at `/etc/rms/config.toml`; the binary reads that path by default. The
database connection string is the only runtime override: it is built from the
Secret-backed credentials and injected as the `DATABASE_URL` environment variable,
which supersedes the (empty) `[postgres] db_url` in the ConfigMap so the password
never lands in a ConfigMap. Because RMS reads the file once at startup, the
Deployment carries a `checksum/config` annotation so config changes trigger a
rollout.

Beyond the top-level `port`, the rendered `config.toml` groups keys into sections
matching the Rust config structs: `[metrics]`, `[tls]`, `[switches]`, `[postgres]`,
`[workflows]`, and `[logging]`.

By default, `apiServer.allowInsecure` is `false` and `apiServer.tls.enabled` is `true`.
The RMS binary requires mTLS (server cert/key plus client CA) unless `insecure` is set
(via `apiServer.allowInsecure: true`). A bare install therefore needs TLS material in
your values or via `--set-file`; see the site and insecure-mode examples below.

`apiServer` also supports:

- `switchCertCertificates` / `switchCertRoot` — switch-side cert material (config.toml `[switches] switch_cert_root`)
- `clientTlsCertificates` / `clientTlsRoot` - required RMS client mTLS for switch `nvue_api`, `scale_up_fabric_manager`, and secure certificate-install connectivity checks unless `insecureSwitch` is true (config.toml `[switches] client_tls_root`)
- `defaultSwitchDomain` - default when switch RPCs omit domain (config.toml `[switches] default_switch_domain`)
- `dnsDomain` - optional DNS domain used as the NVUE TLS authority and for switch gRPC server-name verification, not the NVLink domain (config.toml `[switches] dns_domain`)
- `insecureSwitch` - sets config.toml `[switches] insecure_switch = true`; `nvue_api` remains HTTPS without client mTLS or server certificate verification, `scale_up_fabric_manager` uses plaintext HTTP, and secure-only gNMI connectivity checks are skipped.
- `nmxGatewayId` — `gateway_id` sent on NMX-C gRPC requests (config.toml `[switches] nmx_gateway_id`). Default: `rack-manager-grpc-client`.
- `dbPoolMax` — maximum Postgres connection pool size (config.toml `[postgres] db_pool_max`). Must be greater than 0. Default: `20`.
- `maxTrackedJobs` — maximum async job records retained (config.toml `[workflows] max_tracked_jobs`). Default: `10000`.
- `terminalJobTtlSeconds` — retention period for completed and failed job records (config.toml `[workflows] terminal_job_ttl_seconds`). Default: `86400`.
- `expectedInventoryProfiles` — opaque profile identifiers mapped to NVFWUPD AP
  names (config.toml `[workflows.expected_inventory_profiles]`). Nodes select a
  profile with `NodeDescriptor.attributes["inventory_profile"]`. Default: `{}`.
- `logLevel` — optional log level / filter directive replacing `RUST_LOG` (config.toml `[logging] log_level`). Empty uses the default `info` level plus dependency caps.
- `enableTimestamps` — assuming a logging collector is adding its own timestamps, so this is disabled by default to prevent duplicate timestamp fields. If no logging collector is being used, set true to output timestamps natively (config.toml `[logging] enable_timestamps`). Default: `false`.
- `firmwarePersistentVolumeClaim` — existing PVC to mount at `firmwareMountPath` for firmware downloads. Leave empty to use the default `firmwareStoragePath` hostPath.
- `sftpUploadTimeoutSeconds` — overall SFTP upload wall-clock timeout (seconds); config.toml `[workflows] sftp_upload_timeout_seconds`. Default: `3600`.
- `sftpStepTimeoutSeconds` — per-step SFTP stall timeout (seconds); config.toml `[workflows] sftp_step_timeout_seconds`. Must be <= `sftpUploadTimeoutSeconds`. Default: `30`.

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
  patchExternalCluster:
    enabled: true
    postgresqlName: my-pg-cluster
    databaseName: my_rms_db
apiServer:
  environmentPath: myrack
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

### Certificate automation

Set `certificates.enabled: true` and the chart issues the cert-manager
`Certificate` resources itself instead of requiring them to be applied by hand
before install. Requires cert-manager CRDs and a reachable issuer, so it is off
by default.

| Certificate | Group defaults | Consumed by |
| --- | --- | --- |
| API server | `certificates.apiServer` | mounted at `apiServer.tls.secretMountPath` |
| One per `apiServer.switchCertCertificates` entry | `certificates.switchServer` | switch-side material under `switchCertRoot` |
| One per `apiServer.clientTlsCertificates` entry | `certificates.switchClient` | client mTLS under `clientTlsRoot` |

Each `Certificate` is named after, and writes to, the secret its entry already
references. The API server secret resolves as `certificates.apiServer.secretName`
→ `apiServer.tls.existingSecret` → `rms-api-server-certificate`; whichever wins
is what the Deployment mounts, and the inline `apiServer.tls.cert` / `key` / `ca`
Secret is not rendered.

Fields resolve narrowest scope first — a `certificate:` map on an individual
entry, then the group, then shared `certificates.*`: `issuerRef`, `duration`,
`renewBefore`, `privateKey`, `commonName`, `subject`, `secretTemplate`,
`dnsNames`, `ipAddresses`, `uris`, `usages`, `additionalOutputFormats`, and
`enabled`. `issuerRef` and `privateKey` merge per key, so a narrower scope can
change `issuerRef.name` alone and keep the inherited `kind`.

`certificates.issuerRef.name` is required and ships empty; rendering fails until
a site sets it. `certificates.spiffe.trustDomain` also ships empty, and while it
is no URI SAN is issued.

SAN defaults:

- **API server** — the four in-cluster Service names (`rms-api-server`,
  `rms-api-server.{namespace}`, `.svc`, `.svc.cluster.local`).
- **Switch server** — `apiServer.dnsDomain`. RMS uses that value verbatim as the
  TLS authority for outbound switch connections, so the certificate is only
  usable if it carries that name.
- **`spiffe://{trustDomain}/{path}`** — added to the certificates RMS itself
  presents (API server and `switchClient`), not to `switchServer` certificates,
  which are installed on the switches and carry the switch identity.

Rendering fails if a certificate would have no `dnsNames`, `uris`, `ipAddresses`,
or `commonName`, since cert-manager rejects an identity-less certificate.

```yaml
certificates:
  enabled: true
  issuerRef:
    name: site-ca-issuer
  spiffe:
    trustDomain: example.local
apiServer:
  dnsDomain: my-site.example.com
  switchCertCertificates:
    - domain: site-wide
      secret: rms-switch-server-certificate
  clientTlsCertificates:
    - domain: site-wide
      secret: rms-switch-client-certificate
```

See [`examples/overrides/site-certificates-values.yaml`](./examples/overrides/site-certificates-values.yaml)
for a ready-made file.

Issuance is asynchronous: the API server pod stays in `ContainerCreating` until
cert-manager writes the secrets. Allow for that in `--wait` / `--timeout`;
`certificates.annotations` can carry an `argocd.argoproj.io/sync-wave` when the
resources need to be ordered ahead of the Deployment.

#### Rotation and renewal

Enabling `certificates` automates **issuance and renewal only**. cert-manager
rewrites each Secret at `renewBefore`, and the kubelet refreshes the mounted
files, but only one of the three consumers picks the new material up on its own:

| Material | On renewal | To take effect |
| --- | --- | --- |
| API server certificate | Files update; the process keeps using the certificate it read at startup | Restart the pod |
| Switch client mTLS | Picked up automatically, no restart | Nothing |
| Switch server material | Files update; the copy on each switch is untouched | Re-run `ConfigureSwitchCertificate` |

Two consequences follow, and both are load-bearing when choosing `duration`:

- **The API server does not reload its own certificate.** RMS reads the cert,
  key, and client CA once at startup and holds the parsed material for the life
  of the process. When the mounted certificate expires, the pod keeps serving
  the expired one — the Deployment has no probe and no rotation-triggered
  rollout — until something restarts it. Use `kubectl rollout restart
  deployment/rms-api-server -n <namespace>`, a controller such as
  [Reloader](https://github.com/stakater/Reloader), or a scheduled restart.
- **Switches keep the certificate that was pushed to them.** Renewing the
  Secret changes the material inside the RMS pod, not the copy in the switch's
  NVOS store. Only `ConfigureSwitchCertificate` installs it, and each run mints
  fresh timestamped NVOS IDs, so re-pushing never collides with the previously
  installed certificate. Until it is re-run, the switch's copy ages toward the
  expiry it was issued with, and RMS rejects an expired switch certificate on
  its outbound connections.

So `certificates.duration` sets a hard deadline for a redistribution step that
nothing in this chart performs. The 30-day default (`720h0m0s`, renewing at
`360h0m0s`) suits sites that restart the API server and re-push to switches at
least monthly; sites that do not should raise it — `8760h0m0s` for a yearly
cadence — rather than rely on renewal alone. Both are settable per group, so the
API server and the switch-side certificates can run on different clocks.

`certificates.privateKey.rotationPolicy` defaults to `Always`, so each renewal
mints a new private key. The chart sets this explicitly because cert-manager
changed its own default from `Never` to `Always` in v1.18; without it the same
values would rotate keys on one cluster and reuse them on another. Note that key
rotation reaches a consumer only where certificate rotation does — for the API
server and the switches, a reused key and a rotated one are equally stale until
the restart or re-push happens.

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
  allowInsecure: true
  tls:
    enabled: false
```

When `allowInsecure: true`, the rendered `config.toml` sets `[tls] insecure = true` and the
Deployment injects the `RMS_ALLOW_INSECURE=1` environment gate, so the binary serves
plaintext gRPC with no client authentication. RMS requires both signals -- the config
flag and the env gate, which the chart always renders together -- as a defense-in-depth
measure so a stale ConfigMap cannot on its own downgrade the API. Dev/test only;
production MUST leave this `false`.

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
    - domain: myrack.example.com
      secret: rms-nmxc-client-certificate-myrack
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

> **Note — standalone Postgres reinstall after `dropDatabaseOnUninstall`**
>
> When `databaseMode: standalone`, the Postgres StatefulSet uses a PersistentVolumeClaim (PVC)
> that Helm does **not** delete on uninstall. If `dropDatabaseOnUninstall: true`, the
> `drop-database` pre-delete hook drops the release database from the live pod, but the PVC (and
> its already-initialised data directory) survives. On the next `helm install`, the Postgres
> container finds the existing data directory, skips its first-run initialisation, and
> **`POSTGRES_DB` is never consulted** — so the database is not recreated.
>
> The chart handles this automatically via the `wait-for-db` init container in the api-server
> Deployment, which waits for Postgres and idempotently runs `CREATE DATABASE` before the
> api-server starts. If you hit this state on a cluster running an older chart version, create
> the database manually:
>
> ```bash
> kubectl exec -n <namespace> <postgres-pod> -- psql -U <admin-user> -c "CREATE DATABASE <db_name>;"
> ```

## Configuration values reference

`values.yaml` (and its `values.schema.json` schema) is the exhaustive source of
truth for chart values. Key sections:

| Section | Purpose |
| --- | --- |
| `global.image` | Shared RMS API image tag and pull policy. |
| `apiServer` | Image, replicas, port (8801), TLS, switch cert material, firmware path. |
| `certificates` | Opt-in cert-manager `Certificate` resources for API server and switch mTLS. |
| `database` | Host, port, DB name, `credentialsSecret`, `sslMode` (external DB). |
| `databaseMode` | `external` (default), `standalone`, or `memory` (in-memory, dev/test). |
| `postgres` | Standalone Postgres configuration (for `databaseMode: "standalone"`). |
| `rmsPostgres` | ExternalSecret (ESO) and optional patch for external PostgreSQL. |
| `dropDatabaseOnUninstall` | If `true`, a pre-delete hook drops the release DB on uninstall. |

All RMS runtime configuration is delivered through a TOML file that the chart
renders from `apiServer.*` values into the `rms-api-config` ConfigMap, mounted
read-only at `/etc/rms/config.toml`. The database connection string is the only
runtime override: it is built from Secret-backed credentials and injected as
`DATABASE_URL` so the password never lands in a ConfigMap. The Deployment carries
a `checksum/config` annotation so config changes trigger a rollout. See
[Configuration via Helm](../docs/configuration/via-helm.md) for the
full value → `config.toml` key mapping.

`apiServer` values that map to the `config.toml` `[switches]`, `[postgres]`,
`[workflows]`, and `[logging]` sections:

- `switchCertCertificates` / `switchCertRoot` — switch-side cert material
  (`[switches] switch_cert_root`).
- `clientTlsCertificates` / `clientTlsRoot` — RMS client mTLS for switch NVUE,
  scale-up fabric manager, and secure certificate-install checks, unless
  `insecureSwitch` is true (`[switches] client_tls_root`).
- `defaultSwitchDomain` — default when switch RPCs omit domain
  (`[switches] default_switch_domain`).
- `dnsDomain` — optional DNS domain used as the NVUE TLS authority and switch
  gRPC server-name (`[switches] dns_domain`).
- `insecureSwitch` — sets `[switches] insecure_switch = true`; NVUE stays HTTPS
  without client mTLS or server-cert verification, NMX-C uses plaintext HTTP,
  secure-only gNMI checks are skipped.
- `nmxGatewayId` — `gateway_id` on NMX-C gRPC requests
  (`[switches] nmx_gateway_id`; default `rack-manager-grpc-client`).
- `dbPoolMax` — Postgres pool size (`[postgres] db_pool_max`; default `20`, must
  be > 0).
- `maxTrackedJobs` — retained async job records
  (`[workflows] max_tracked_jobs`; default `10000`).
- `terminalJobTtlSeconds` — retention for completed/failed jobs
  (`[workflows] terminal_job_ttl_seconds`; default `86400`).
- `expectedInventoryProfiles` — opaque expected-inventory profile map
  (`[workflows.expected_inventory_profiles]`; default `{}`).
- `logLevel` — log level / filter directive (`[logging] log_level`; empty →
  `info` plus caps).
- `firmwarePersistentVolumeClaim` — existing PVC mounted at
  `firmwareMountPath`; empty uses the `firmwareStoragePath` hostPath.
- `sftpUploadTimeoutSeconds` — overall SFTP upload timeout
  (`[workflows] sftp_upload_timeout_seconds`; default `3600`).
- `sftpStepTimeoutSeconds` — per-step SFTP stall timeout
  (`[workflows] sftp_step_timeout_seconds`; default `30`, must be ≤ upload
  timeout).

The chart defaults to **mTLS** (`apiServer.allowInsecure: false`,
`apiServer.tls.enabled: true`). See
[INSECURE_SWITCH_TESTING.md](INSECURE_SWITCH_TESTING.md) for `insecureSwitch` test
cases and `helm/examples/overrides/` for ready-made override files.

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
    ├── certificates.yaml
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
