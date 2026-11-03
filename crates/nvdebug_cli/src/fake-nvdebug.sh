#!/bin/sh
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# A stand-in for nvdebug in the crate's tests. It records what the crate
# passed it into the output directory: the arguments, the DUT file's path,
# and a copy of the DUT file. Then it acts on the first collector id: a
# number is its exit code, and HOLD writes "ready" and runs until stopped.

if [ "$1" = --version ]; then
    echo "nvdebug 0.0.0 (fake)"
    exit 0
fi

for arg; do
    case $prev in
        --dut-config) dut=$arg ;;
        --collector-id) ids=$arg ;;
        --output) out=$arg ;;
    esac
    prev=$arg
done
printf '%s\n' "$@" > "$out/argv"
printf '%s\n' "$dut" > "$out/dut_path"
cp "$dut" "$out/dut.json"

case $ids in
    HOLD*)
        echo > "$out/ready"
        # exec, not a loop: a child left by a dead shell turns into a zombie
        # where nothing reaps, as under `cargo test` as PID 1 in a container,
        # and a zombie keeps the process group from emptying.
        exec sleep 600
        ;;
    *) exit "${ids%% *}" ;;
esac
