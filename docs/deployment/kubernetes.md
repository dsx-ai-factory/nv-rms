# Kubernetes Deployment with Helm

RMS ships a Helm chart under
[`helm/`](https://github.com/dsx-ai-factory/nv-rms/tree/main/helm) that deploys the API
server and, optionally, in-cluster PostgreSQL. The API server applies its embedded
sqlx migrations automatically on startup. Make sure the
[deployment prerequisites](prerequisites.md) - persistence, certificates, and
secrets - are in place first.

## Chart components

| Component | Description |
| --- | --- |
| **RMS API Server** | gRPC API for rack management (default port 8801); runs sqlx migrations on startup. |
| **PostgreSQL** | Optional in-cluster standalone Postgres, or point at an external cluster. |
| **NVSSVT** | Optional system validation launcher, run as a sidecar in the API server pod. See [NVSSVT sidecar](#nvssvt-sidecar). |

All RMS runtime configuration is delivered through a TOML file that the chart
renders from `apiServer.*` values into the `rms-api-config` ConfigMap, mounted
read-only at `/etc/rms/config.toml`. See
[Configuration via Helm](../configuration/via-helm.md)
for the value → `config.toml` key mapping. The default posture is **mTLS on**
(`apiServer.tls.enabled: true`, `apiServer.allowInsecure: false`), so a bare
install needs TLS material.

## Versioning

The chart uses two independent version axes:

| Axis | Source | Example | Meaning |
| --- | --- | --- | --- |
| Chart version | `Chart.yaml` `version` | `1.0.0` | Plain semver for template/values changes |
| App image tag | `global.image.tag` at install | `v0.11.0-rc1-0-g9c01a464` | The RMS container build to run |

Set `global.image.tag` (or per-component `apiServer.image.tag`) at
install/upgrade to pick the application build. See
[`helm/README.md`](https://github.com/dsx-ai-factory/nv-rms/blob/main/helm/README.md) for
the full chart-version policy and automation.

## Install from NGC

Replace `<NGC_TOKEN>` with your [NGC API key](https://ngc.nvidia.com/setup/api-key),
`<CHART_VERSION>` with the chart semver (e.g. `1.0.0`), and `<APP_TAG>` with
the RMS image tag (e.g. `v0.11.0-rc1-0-g9c01a464`):

```bash
helm pull https://helm.ngc.nvidia.com/0837451325059433/rms-dev/charts/rack-manager-<CHART_VERSION>.tgz \
  --username='$oauthtoken' \
  --password='<NGC_TOKEN>'

helm install rack ./rack-manager-<CHART_VERSION>.tgz \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

Verify:

```bash
helm list -n rack-manager
kubectl get pods -n rack-manager -l app.kubernetes.io/name=rack-manager
```

## Install from a local checkout

You can also install directly from the repo's `helm/` directory, layering site
overrides with `-f` (later files win):

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  -f my-site-values.yaml \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

### Site values example

Keep `helm/values.yaml` as the base and layer a site file. This external-DB,
mTLS-on example references certs and secrets created in the
[prerequisites](prerequisites.md):

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
  switchCertRoot: /var/run/secrets/switch-cert
  switchCertCertificates:
    - domain: site-wide
      secret: rms-nmxc-server-certificate
  clientTlsRoot: /var/run/secrets/client-tls
  clientTlsCertificates:
    - domain: site-wide
      secret: rms-nmxc-client-certificate
  defaultSwitchDomain: site-wide
  dnsDomain: myrack.example.com
  tls:
    enabled: true
    caEnabled: true
    existingSecret: rms-api-server-tls
```

When `patchExternalCluster.enabled` is true, the pre-install job adds that
database and a `LOGIN` role that owns it. The role is not `SUPERUSER` and
does not have `CREATEDB`. Migrations only create tables in that database.
The hook does not run again on upgrade, so a cluster already patched with
`superuser` keeps that role until its PostgreSQL user flags are cleared.

## NVSSVT sidecar

`StartSystemValidation` runs NVSSVT scans through the NVSSVT launcher API.
When `nvssvt.enabled` is `true`, the chart runs the launcher as the
`nvssvt-server` container in the API server pod, and RMS reaches it on
`127.0.0.1:8000`. The sidecar is disabled by default. With it disabled, RMS
starts normally, but every node in a `StartSystemValidation` request fails to
launch its scan.

The launcher API has no authentication, so it listens on `127.0.0.1` only and
is not reachable through the pod IP. It keeps the 12 most recent run
directories and deletes older ones when a new scan starts. It never deletes a
run that is still in progress, or a run directory modified within the last
hour, so that RMS can read the run's result.

The NVSSVT image is not part of the RMS image. Build it from the NVSSVT
release bundles and push it to a registry that the cluster can pull from.

### Build the NVSSVT sidecar image

1. Download the NVSSVT release bundle for each architecture that the cluster
   runs from the release PID URLs for your NVSSVT version. Keep the original
   file names:

   | Node architecture | Bundle file name |
   | --- | --- |
   | x86_64 (`linux/amd64`) | `nvssvt-linux-x86_64-<version>.tar.gz` |
   | Arm SBSA (`linux/arm64`) | `nvssvt-linux-sbsa-<version>.tar.gz` |

1. Copy the bundles to `docker/nvssvt/libs/` in the RMS repository:

   ```text
   docker/nvssvt/
   ├── Dockerfile
   ├── nvssvt_server.py
   └── libs/
       ├── nvssvt-linux-sbsa-<version>.tar.gz
       └── nvssvt-linux-x86_64-<version>.tar.gz
   ```

   Git ignores everything in `libs/` except its README.

1. Build the image with `docker/nvssvt` as the build context. The build
   requires BuildKit, which is the default builder in current Docker
   releases. It installs only the bundle that matches the target
   architecture, so you can keep both bundles in `libs/`:

   ```bash
   # Image for the architecture of the build host.
   docker build -t <registry>/nvssvt-server:<version> docker/nvssvt

   # Multi-architecture image (requires both bundles), pushed to the registry.
   docker buildx build --platform linux/amd64,linux/arm64 \
     -t <registry>/nvssvt-server:<version> --push docker/nvssvt
   ```

   If `libs/` holds more than one version for an architecture, select one with
   `--build-arg NVSSVT_VERSION=<version>`. The build fails if the bundle for the
   target architecture is missing.

1. Push the image if you did not build it with `--push`:

   ```bash
   docker push <registry>/nvssvt-server:<version>
   ```

   For a local cluster, such as kind, load the image instead, for example
   `kind load docker-image <registry>/nvssvt-server:<version>`.

### Enable the NVSSVT sidecar

Point the chart at the image in a site values file:

```yaml
nvssvt:
  enabled: true
  image:
    repository: <registry>/nvssvt-server
    tag: <version>
```

If the registry requires authentication, add its pull secret to
`global.imagePullSecrets`. After the upgrade, the API server pod reports
`2/2` containers ready. To confirm that the launcher is running, check its
logs:

```bash
kubectl logs -n rack-manager deployment/rms-api-server -c nvssvt-server
```

## Insecure and in-memory modes (dev/test only)

For local development on a laptop or k3s cluster, you can opt out of mTLS and/or
the database. **Not** appropriate for shared, staging, or production clusters -
any client that can reach port 8801 can invoke every RPC (firmware flash, power
control, switch password rotation) with no authentication.

When `allowInsecure: true`, the chart renders `[tls] insecure = true` **and**
injects the `RMS_ALLOW_INSECURE=1` env gate together (defense in depth). Set
`databaseMode: memory` to skip all database wiring:

```bash
helm upgrade --install rack ./helm \
  -f helm/values.yaml \
  --set apiServer.allowInsecure=true \
  --set apiServer.tls.enabled=false \
  --set databaseMode=memory \
  --set global.image.tag=<APP_TAG> \
  -n rack-manager --create-namespace
```

The repo ships override examples under `helm/examples/overrides/`
(`local-dev-values.yaml`, `switch-insecure-values.yaml`,
`firmware-pvc-values.yaml`) and documents `insecureSwitch` test cases in
[`helm/INSECURE_SWITCH_TESTING.md`](https://github.com/dsx-ai-factory/nv-rms/blob/main/helm/INSECURE_SWITCH_TESTING.md).

## Upgrade

To roll the application image only (same chart):

```bash
helm upgrade rack ./rack-manager-<CHART_VERSION>.tgz -n rack-manager \
  --set global.image.tag=<NEW_TAG>
```

If you install with a values file, pass it first so other settings are preserved
(`-f my-values.yaml -f upgrade-values.yaml`). After upgrade, the API server rolls
to the new image and applies any pending sqlx migrations on startup. If pods don't
roll automatically:

```bash
kubectl rollout restart deployment/rms-api-server -n rack-manager
```

## Access the API in-cluster

```bash
export NAMESPACE=rack-manager
kubectl port-forward -n $NAMESPACE svc/rms-api-server 8801:8801
```

Then connect to `localhost:8801` (or the port set in `apiServer.port`).

## Uninstall

```bash
helm uninstall rack -n rack-manager
```

If `dropDatabaseOnUninstall: true`, a pre-delete hook runs a job to drop the
release database. Set it to `false` to keep the database.

> **Standalone Postgres reinstall caveat.** In `databaseMode: standalone`, the
> Postgres PVC survives uninstall even when `dropDatabaseOnUninstall: true` drops
> the release database from the live pod. On reinstall the container finds the
> existing data directory and skips first-run init, so `POSTGRES_DB` is never
> consulted. The chart's `wait-for-db` init container handles this by idempotently
> running `CREATE DATABASE` before the API server starts. On older charts, create
> it manually:
>
> ```bash
> kubectl exec -n <namespace> <postgres-pod> -- \
>   psql -U <admin-user> -c "CREATE DATABASE <db_name>;"
> ```

## Package the chart

To port the chart elsewhere without the source repo:

```bash
cd nv-rms && helm package helm/
# Successfully packaged chart and saved it to: .../rack-manager-<CHART_VERSION>.tgz
```

## Resulting Kubernetes components

A standalone-Postgres install produces roughly:

```text
pod/postgres-0                       1/1   Running
pod/rms-api-server-<hash>            1/1   Running

service/postgres         ClusterIP   5432/TCP
service/rms-api-server   ClusterIP   8801/TCP

deployment.apps/rms-api-server   1/1
statefulset.apps/postgres        1/1

configmap/rms-api-config          # rendered config.toml
configmap/rms-database-config
secret/rms-api-server-tls         # API server mTLS material
secret/rms-nmxc-client-certificate
secret/rms-nmxc-server-certificate
servicemonitor.monitoring.coreos.com/rms-api-server   # if serviceMonitor enabled
```

An external-DB install omits the `postgres` StatefulSet/Service and instead relies
on the external cluster plus the synced credentials Secret.
