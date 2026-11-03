# NVSSVT release bundles

Place the NVSSVT release bundles for the NVSSVT sidecar image in this
directory before building `docker/nvssvt/Dockerfile`. The bundles are too
large for Git, so everything here except this file is ignored.

| Image architecture | Bundle file name                            |
| ------------------ | ------------------------------------------- |
| `linux/amd64`      | `nvssvt-linux-x86_64-<version>.tar.gz`      |
| `linux/arm64`      | `nvssvt-linux-sbsa-<version>.tar.gz`        |

Download the bundles for your NVSSVT release from its release PID URLs and
keep the original file names. The build matches on these names to select
the bundle for the target architecture.

For example, NVSSVT `2.0.0-QS2` is published at:

- `https://artifactory.nvidia.com/artifactory/sw-dgx-sps-tools-debian-local/nvssvt/release/nvssvt/linux-x86_64/2.0.0-QS2/nvssvt-linux-x86_64-2.0.0-QS2.tar.gz`
- `https://artifactory.nvidia.com/artifactory/sw-dgx-sps-tools-debian-local/nvssvt/release/nvssvt/linux-sbsa/2.0.0-QS2/nvssvt-linux-sbsa-2.0.0-QS2.tar.gz`

Provide the bundle for each architecture you build. When both are present,
an `amd64` build uses the `linux-x86_64` bundle and an `arm64` build uses the
`linux-sbsa` bundle. If this directory holds more than one version for an
architecture, select one with `--build-arg NVSSVT_VERSION=<version>`.

See [Build the NVSSVT sidecar image](../../../docs/deployment/kubernetes.md#build-the-nvssvt-sidecar-image)
for the complete build and deployment steps.
