# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for the NVFWUPD open-source disclosure generator."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import generate_nvfwupd_open_source_bundle as generator


class GenerateNvfwupdOpenSourceBundleTests(unittest.TestCase):
    def test_release_dependency_ids_excludes_development_only_edges(self) -> None:
        metadata = {
            "packages": [
                {
                    "id": "nvfwupd",
                    "manifest_path": str(generator.NVFWUPD_MANIFEST),
                },
                {"id": "runtime", "manifest_path": "/runtime/Cargo.toml"},
                {"id": "development", "manifest_path": "/development/Cargo.toml"},
            ],
            "resolve": {
                "nodes": [
                    {
                        "id": "nvfwupd",
                        "deps": [
                            {
                                "pkg": "runtime",
                                "dep_kinds": [{"kind": None, "target": None}],
                            },
                            {
                                "pkg": "development",
                                "dep_kinds": [{"kind": "dev", "target": None}],
                            },
                        ],
                    },
                    {"id": "runtime", "deps": []},
                    {"id": "development", "deps": []},
                ]
            },
        }

        self.assertEqual(
            generator.release_dependency_ids(metadata), {"nvfwupd", "runtime"}
        )

    def test_copy_package_source_omits_package_lockfile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            destination.mkdir()
            (source / "Cargo.toml").write_text("[package]\n", encoding="utf-8")
            (source / "Cargo.lock").write_text("stale graph\n", encoding="utf-8")
            (source / "src").mkdir()
            (source / "src" / "lib.rs").write_text("// source\n", encoding="utf-8")

            generator.copy_package_source(
                {
                    "name": "example",
                    "version": "1.2.3",
                    "manifest_path": str(source / "Cargo.toml"),
                },
                destination,
            )

            copied = destination / "example-1.2.3"
            self.assertTrue((copied / "Cargo.toml").is_file())
            self.assertTrue((copied / "src" / "lib.rs").is_file())
            self.assertFalse((copied / "Cargo.lock").exists())

    def test_third_party_packages_unions_release_targets(self) -> None:
        metadata_by_target = {
            generator.TARGETS[0]: {
                "packages": [
                    {"id": "root", "source": None, "name": "nvfwupd", "version": "1"},
                    {"id": "shared", "source": "registry", "name": "shared", "version": "1"},
                ]
            },
            generator.TARGETS[1]: {
                "packages": [
                    {"id": "root", "source": None, "name": "nvfwupd", "version": "1"},
                    {"id": "arm", "source": "registry", "name": "arm-only", "version": "1"},
                ]
            },
        }

        def selected(metadata: dict) -> set[str]:
            return {package["id"] for package in metadata["packages"]}

        with (
            mock.patch.object(
                generator,
                "cargo_metadata",
                side_effect=lambda target: metadata_by_target[target],
            ),
            mock.patch.object(generator, "release_dependency_ids", side_effect=selected),
        ):
            packages = generator.third_party_packages()

        self.assertEqual(
            [(package["name"], package["version"]) for package in packages],
            [("arm-only", "1"), ("shared", "1")],
        )


if __name__ == "__main__":
    unittest.main()
