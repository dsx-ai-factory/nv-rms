# Deployment Prerequisites

This section covers what a cluster needs before installing RMS with Helm. For the
install/upgrade workflow itself, see [Kubernetes Deployment with Helm](kubernetes.md).

## Cluster requirements

- **Kubernetes** cluster (1.24+)
- **Helm** 3.x
- **NGC** account and API key - for pulling the chart and images from NGC
- A **container image registry** the cluster can pull from. If it requires
  authentication, provision an `imagePullSecret` (via ESO, ArgoCD, or manually)
  and reference it in `global.imagePullSecrets`.

## Persistence strategy

RMS persists firmware-object state, cached artifact metadata, job tracking, and
apply history (never rack topology). Choose one of three modes via `databaseMode`:

| Mode | `databaseMode` | When to use |
| --- | --- | --- |
| **External Postgres** (default) | `external` | **Recommended for production.** A Postgres cluster provisioned outside the chart (e.g. Zalando Postgres Operator / Patroni, or CloudNativePG) with an RMS database, user, and a Secret synced into the RMS namespace. HA and resilient. |
| **Standalone Postgres** | `standalone` | A Postgres `StatefulSet` deployed from the RMS chart, backed by a PVC. Works out of the box but lacks HA/roles. |
| **In-memory** | `memory` | No database wiring at all; the binary falls back to its in-memory store. **Data is lost on restart** - dev/test only. |

For external and standalone modes, RMS applies its embedded sqlx migrations
automatically on API-server startup - no separate migration job is required.

## Firmware storage

The firmware download filesystem defaults to a node `hostPath`
(`apiServer.firmwareStoragePath`). For durable storage that survives pod
rescheduling, set `apiServer.firmwarePersistentVolumeClaim` to an existing claim
in a **site values file** (keep it empty in the chart base). When
`apiServer.replicaCount > 1`, use a volume that supports mounting from every
scheduled pod (`ReadWriteMany`); `ReadWriteOnce` block volumes attach to only one
node and cause Multi-Attach errors on additional replicas.

## Certificates

An RMS deployment needs a component that issues, rotates, and distributes
certificates from a CA - for example [cert-manager](https://cert-manager.io/).
There are **two distinct categories** of certificate:

1. **Inbound (RMS API server mTLS)** - secures traffic between RMS clients and the
   RMS gRPC API. Clients must present certificates issued by the same CA. The CA
   must issue the **server** certificate with Subject Alternative Names (SANs)
   matching the RMS service URL, bottom-up:
   - `rms-api-server`
   - `rms-api-server.<namespace>`
   - `rms-api-server.<namespace>.svc`
   - `rms-api-server.<namespace>.svc.cluster.local`
1. **Outbound (switch mTLS)** - secures traffic between RMS and switch service
   APIs (NVUE, NMX-C). The switch must serve a cert reflecting its SAN, and RMS
   must hold client certs signed by an authority the switch trusts. See
   [Configuring RMS: `[switches]`](../configuration/configuring-rms.md#switches) for the on-disk
   layout RMS expects.

### Chart-managed certificates

When cert-manager and a site issuer are already present, the Helm chart can issue
all three certificates itself. Set `certificates.enabled: true` and
`certificates.issuerRef.name` — the issuer name is required and has no default,
so the chart fails to render until a site supplies it — and the chart creates
the API server `Certificate` plus one per `switchCertCertificates` and
`clientTlsCertificates` entry, each named after and writing to the secret the
entry already references. The issuer, SPIFFE trust domain, lifetimes, key
algorithm, and SANs are all values, so they can vary per site. See
[Certificate automation](../../helm/README.md#certificate-automation) for the
full override surface. The rest of this section covers issuing them outside the
chart.

Chart-managed certificates are issued and renewed automatically, but only the
switch client material is picked up automatically. The API server reads its
certificate once at startup and needs a pod restart; switches keep the
certificate last installed on them and need `ConfigureSwitchCertificate` re-run.
Plan a redistribution cadence at least as short as `certificates.duration`
before enabling this - see
[Rotation and renewal](../../helm/README.md#rotation-and-renewal).

### cert-manager example

A self-signed root can bootstrap both categories. This issues a CA, an RMS server
cert (referenced by `apiServer.tls.existingSecret`), and switch server/client
certs (referenced by `switchCertCertificates` / `clientTlsCertificates`):

```yaml
# 1. SelfSigned ClusterIssuer (bootstraps the CA cert)
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-cluster-issuer
spec:
  selfSigned: {}
---
# 2. CA Certificate - the shared root of trust for RMS and its clients
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: rms-ca
  namespace: rack-manager
spec:
  isCA: true
  commonName: rms-ca
  secretName: rms-ca
  duration: 87600h   # 10 years
  renewBefore: 720h
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    name: selfsigned-cluster-issuer
    kind: ClusterIssuer
---
# 3. CA Issuer backed by the rms-ca secret above
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: rms-ca-issuer
  namespace: rack-manager
spec:
  ca:
    secretName: rms-ca
---
# 4. RMS server cert - referenced by apiServer.tls.existingSecret
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: rms-api-server-tls
  namespace: rack-manager
spec:
  secretName: rms-api-server-tls
  duration: 8760h    # 1 year
  renewBefore: 720h
  dnsNames:
    - rms-api-server
    - rms-api-server.rack-manager
    - rms-api-server.rack-manager.svc
    - rms-api-server.rack-manager.svc.cluster.local
  issuerRef:
    name: rms-ca-issuer
    kind: Issuer
---
# 5. Switch-side server certs - referenced by switchCertCertificates
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: rms-nmxc-server-certificate
  namespace: rack-manager
spec:
  additionalOutputFormats:
    - type: CombinedPEM
  secretName: rms-nmxc-server-certificate
  duration: 8760h
  renewBefore: 720h
  privateKey:
    algorithm: ECDSA
    size: 384
  dnsNames:
    - myrack.example.com
  issuerRef:
    name: rms-ca-issuer
    kind: Issuer
---
# 6. RMS-side switch client certs - referenced by clientTlsCertificates
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: rms-nmxc-client-certificate
  namespace: rack-manager
spec:
  secretName: rms-nmxc-client-certificate
  duration: 8760h
  renewBefore: 720h
  privateKey:
    algorithm: ECDSA
    size: 384
  subject:
    organizations:
      - rms
  commonName: rms
  usages:
    - client auth
  issuerRef:
    name: rms-ca-issuer
    kind: Issuer
```

### Generating local test certificates without cert-manager

For local testing off-cluster, you can generate a self-signed CA plus server and
client certificates with `openssl`. Use organization-approved tooling in
production. The two material sets RMS needs are the **RMS API server** mTLS
material (`cert`/`key`/`ca`) and the **NVLink switch** mTLS material laid out per
domain (see [Configuring RMS: switch certificate directory layout](../configuration/configuring-rms.md#switch-certificate-directory-layout)).

## Database credentials Secret

For external/standalone Postgres, RMS reads credentials from a Kubernetes Secret
and builds `DATABASE_URL` from them (the password never lands in a ConfigMap). Use
External Secrets Operator with a `ClusterSecretStore`, or pre-create the Secret
and reference it via `database.credentialsSecret`. For a quick standalone-mode
Secret:

```bash
kubectl create secret generic rms-postgres-auth \
  --namespace rack-manager \
  --from-literal=username=postgres \
  --from-literal=password=<password> \
  --from-literal=POSTGRES_PASSWORD=<password>
```

Keep credentials, database URLs, TLS keys, client certificates, and artifact
tokens out of source control - prefer secret managers or Kubernetes Secrets.
