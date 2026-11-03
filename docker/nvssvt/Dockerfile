#
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Dockerfile for the NVSSVT Launcher API (nvssvt_server.py), run by the RMS
# Helm chart as the `nvssvt-server` sidecar of the API server pod.
#
# Expects one or both NVSSVT release bundles in libs/ next to this file (see
# libs/README.md for where to download them):
#   libs/nvssvt-linux-x86_64-<version>.tar.gz   (used for linux/amd64)
#   libs/nvssvt-linux-sbsa-<version>.tar.gz     (used for linux/arm64)
# The bundle matching the build's target architecture (BuildKit's
# TARGETARCH) is extracted; the other one, if present, is ignored. If libs/
# holds several versions for that architecture, pick one with
# --build-arg NVSSVT_VERSION=<version>.
#
# Build with this directory as the context (BuildKit is required):
#   docker build -t nvssvt-server:2.0.0-QS2 docker/nvssvt
#   docker buildx build --platform linux/arm64 -t nvssvt-server:2.0.0-QS2 docker/nvssvt
# Run:
#   docker run --rm --network host nvssvt-server:2.0.0-QS2
# The API has no authentication, so it listens on 127.0.0.1 only; RMS reaches
# it over the pod's shared loopback. To publish it from a local container for
# testing instead, override the bind address:
#   docker run --rm -p 127.0.0.1:8000:8000 nvssvt-server:2.0.0-QS2 \
#     uvicorn nvssvt_server:app --host 0.0.0.0 --port 8000

FROM python:3.12-slim

ARG TARGETARCH
ARG NVSSVT_VERSION=

RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.34.0 pyyaml==6.0.2

WORKDIR /app

# Bind-mount libs/ rather than COPY it so the bundle for the other
# architecture never lands in an image layer. The bundle's top-level nvssvt/
# directory is stripped, leaving the binary at /app/nvssvt beside its
# config.yaml, dut_config.yaml, controlled_config.yaml,
# nvswitchtray_config.yaml, and config_files/.
RUN --mount=type=bind,source=libs,target=/tmp/nvssvt-libs \
    set -eu; \
    case "${TARGETARCH}" in \
        amd64) nvssvt_platform=linux-x86_64 ;; \
        arm64) nvssvt_platform=linux-sbsa ;; \
        *) echo "Error: unsupported TARGETARCH '${TARGETARCH}' (build with BuildKit for amd64 or arm64)" >&2; exit 1 ;; \
    esac; \
    set -- /tmp/nvssvt-libs/nvssvt-${nvssvt_platform}-${NVSSVT_VERSION:-*}.tar.gz; \
    if [ ! -f "$1" ]; then \
        echo "Error: no libs/nvssvt-${nvssvt_platform}-${NVSSVT_VERSION:-<version>}.tar.gz in the build context; see docker/nvssvt/libs/README.md" >&2; \
        exit 1; \
    fi; \
    if [ "$#" -ne 1 ]; then \
        echo "Error: several NVSSVT ${nvssvt_platform} bundles in libs/; choose one with --build-arg NVSSVT_VERSION=<version>:" >&2; \
        printf '  %s\n' "$@" >&2; \
        exit 1; \
    fi; \
    echo "Installing $(basename "$1") for ${TARGETARCH}"; \
    tar -xzf "$1" -C /app --strip-components=1 --no-same-owner; \
    test -x /app/nvssvt

COPY nvssvt_server.py /app/server/nvssvt_server.py

# The binary resolves config.yaml, dut_config.yaml, controlled_config.yaml,
# nvswitchtray_config.yaml, and config_files/*.yaml relative to the process
# cwd, not to itself. nvssvt_server.py launches it with cwd set to its own
# parent.parent (/app), which is where the bundle was extracted above.
ENV NVSSVT_BIN=/app/nvssvt

WORKDIR /app/server
EXPOSE 8000

CMD ["uvicorn", "nvssvt_server:app", "--host", "127.0.0.1", "--port", "8000"]
