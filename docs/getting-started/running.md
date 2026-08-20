# Running RMS

RMS reads all runtime configuration from a single TOML file selected with
`--config <path>` (default `/etc/rms/config.toml`). For the full set of keys, see
[Configuring RMS](../configuration/configuring-rms.md).

## Run insecurely (development)

The quickest way to get a local instance up is insecure, in-memory mode. This
serves plaintext gRPC on the default port `8801` and logs a warning that data
will not survive a restart.

Plaintext gRPC requires **both** `[tls] insecure = true` in the config **and** the
`RMS_ALLOW_INSECURE=1` environment gate - an independent safeguard so a stale
config alone cannot downgrade the API:

```bash
cat > config.toml <<'EOF'
[tls]
insecure = true
EOF

RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice --config config.toml
```

Scrape the Prometheus metrics endpoint (default `http://localhost:8802/metrics`,
independent of the gRPC port):

```bash
curl http://localhost:8802/metrics
```

## Run insecurely with Postgres persistence

Bring up the bundled database, point `[postgres] db_url` at it, and RMS will
connect, run its embedded migrations, then serve:

```bash
docker compose up -d postgres

cat > config.toml <<'EOF'
[tls]
insecure = true

[postgres]
db_url = "postgres://postgres:postgres@localhost:5432/rms_test"
EOF

RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice --config config.toml
```

You can instead source the connection string from the `DATABASE_URL` override
(leaving `[postgres] db_url` out of the file), which is how production keeps the
password out of the config:

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/rms_test \
    RMS_ALLOW_INSECURE=1 ./target/release/rackmanagementservice --config config.toml
```

## Run with mTLS

Provide the server certificate, private key, and client CA together. No env gate
is needed - this is the default, secure mode:

```bash
cat > config.toml <<'EOF'
[tls]
cert = "/path/to/cert.pem"
key  = "/path/to/key.pem"
ca   = "/path/to/ca.pem"
EOF

./target/release/rackmanagementservice --config config.toml
```

For guidance on generating local test certificates (both the RMS API server mTLS
material and the NVLink switch mTLS material), see
[Deployment: certificates](../deployment/prerequisites.md#certificates).

## Run in a container

The release image reads the same config file. Supply it at run time via a
read-only bind mount and inject the database password out-of-band:

```bash
printf '[tls]\ninsecure = true\n' > config.toml

docker run --rm -p 8801:8801 -p 8802:8802 \
    -e RMS_ALLOW_INSECURE=1 \
    -v "$PWD/config.toml:/etc/rms/config.toml:ro" \
    rms-release
```

The release image also bundles `ipmitool` and the `nvfwupd` CLI for diagnostics:

```bash
docker run --rm --entrypoint nvfwupd rms-release --version
```

For production container and Kubernetes deployment, see
[Deployment](../deployment/prerequisites.md).
