#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Generate the NVFWUPD third-party source and license disclosure bundle."""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
NVFWUPD_MANIFEST = REPO_ROOT / "crates" / "rust_nvfwupd" / "Cargo.toml"
LICENSES_SOURCE = REPO_ROOT / "THIRD-PARTY-LICENSES"
OUTPUT_DIR = REPO_ROOT / "crates" / "rust_nvfwupd" / "OpenSourceSoftware"
LICENSES_OUTPUT = OUTPUT_DIR / "Licenses-3rd-Party"
SOURCES_OUTPUT = OUTPUT_DIR / "OpenSourceSoftware.tar.gz"
TARGETS = ("x86_64-unknown-linux-musl", "aarch64-unknown-linux-musl")


def cargo_metadata(target: str) -> dict:
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--filter-platform",
        target,
        "--format-version",
        "1",
    ]
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def release_dependency_ids(metadata: dict) -> set[str]:
    packages = {package["id"]: package for package in metadata["packages"]}
    root_ids = [
        package_id
        for package_id, package in packages.items()
        if Path(package["manifest_path"]).resolve() == NVFWUPD_MANIFEST.resolve()
    ]
    if len(root_ids) != 1:
        raise RuntimeError(f"expected one nvfwupd package, found {len(root_ids)}")

    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    pending = root_ids
    selected: set[str] = set()
    while pending:
        package_id = pending.pop()
        if package_id in selected:
            continue
        selected.add(package_id)
        for dependency in nodes[package_id]["deps"]:
            if any(kind["kind"] != "dev" for kind in dependency["dep_kinds"]):
                pending.append(dependency["pkg"])
    return selected


def third_party_packages() -> list[dict]:
    selected_ids: set[str] = set()
    packages_by_id: dict[str, dict] = {}
    for target in TARGETS:
        metadata = cargo_metadata(target)
        packages_by_id.update(
            (package["id"], package) for package in metadata["packages"]
        )
        selected_ids.update(release_dependency_ids(metadata))

    packages = [
        packages_by_id[package_id]
        for package_id in selected_ids
        if packages_by_id[package_id]["source"] is not None
    ]
    packages.sort(key=lambda package: (package["name"], package["version"]))
    return packages


def license_blocks() -> tuple[str, dict[tuple[str, str], str]]:
    text = LICENSES_SOURCE.read_text(encoding="utf-8")
    marker = "[[third_party_libraries]]"
    preamble, *blocks = text.split(marker)
    parsed_blocks: dict[tuple[str, str], str] = {}
    for raw_block in blocks:
        block = marker + raw_block
        entry = tomllib.loads(block)["third_party_libraries"][0]
        key = (entry["package_name"], entry["package_version"])
        if key in parsed_blocks:
            raise RuntimeError(f"duplicate license entry for {key[0]} {key[1]}")
        parsed_blocks[key] = block.strip() + "\n"
    return preamble, parsed_blocks


def write_licenses(packages: list[dict], destination: Path) -> None:
    _, available_blocks = license_blocks()
    keys = [(package["name"], package["version"]) for package in packages]
    missing = [key for key in keys if key not in available_blocks]
    if missing:
        rendered = ", ".join(f"{name} {version}" for name, version in missing)
        raise RuntimeError(f"missing root license entries: {rendered}")

    header = (
        'root_name = "nvfwupd"\n'
        f"release_targets = {json.dumps(list(TARGETS))}\n"
        f"third_party_package_count = {len(keys)}\n\n"
    )
    body = "\n\n".join(available_blocks[key].rstrip() for key in keys) + "\n"
    destination.write_text(header + body, encoding="utf-8", newline="\n")


def copy_package_source(package: dict, destination: Path) -> None:
    source = Path(package["manifest_path"]).resolve().parent
    package_destination = destination / f'{package["name"]}-{package["version"]}'

    def ignore(_directory: str, names: list[str]) -> set[str]:
        # A dependency crate's own lockfile is not used to build NVFWUPD. It can
        # contain a historical development graph and must not be presented as
        # part of the locked NVFWUPD release dependency set.
        return {"Cargo.lock"} if "Cargo.lock" in names else set()

    shutil.copytree(source, package_destination, symlinks=True, ignore=ignore)


def normalized_tar_info(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    info.mtime = 0
    return info


def write_source_archive(packages: list[dict], destination: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="nvfwupd-oss-") as temporary:
        source_root = Path(temporary) / "OpenSourceSoftware"
        source_root.mkdir()
        for package in packages:
            copy_package_source(package, source_root)

        with destination.open("wb") as raw_output:
            with gzip.GzipFile(fileobj=raw_output, mode="wb", mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w") as archive:
                    archive.add(
                        source_root,
                        arcname="OpenSourceSoftware",
                        recursive=True,
                        filter=normalized_tar_info,
                    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="regenerate in memory-backed temporary output and verify checked-in files",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    packages = third_party_packages()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not args.check:
        write_licenses(packages, LICENSES_OUTPUT)
        write_source_archive(packages, SOURCES_OUTPUT)
        print(f"generated NVFWUPD disclosure for {len(packages)} packages")
        return

    with tempfile.TemporaryDirectory(prefix="nvfwupd-oss-check-") as temporary:
        check_dir = Path(temporary)
        generated_licenses = check_dir / "Licenses-3rd-Party"
        generated_sources = check_dir / "OpenSourceSoftware.tar.gz"
        write_licenses(packages, generated_licenses)
        write_source_archive(packages, generated_sources)
        for generated, checked_in in (
            (generated_licenses, LICENSES_OUTPUT),
            (generated_sources, SOURCES_OUTPUT),
        ):
            if generated.read_bytes() != checked_in.read_bytes():
                raise SystemExit(f"outdated generated artifact: {checked_in}")
    print(f"verified NVFWUPD disclosure for {len(packages)} packages")


if __name__ == "__main__":
    main()
