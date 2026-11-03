#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validate THIRD-PARTY-LICENSES against Cargo.lock."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import re
import sys
import tomllib


REPO_ROOT = Path(__file__).resolve().parents[1]
CARGO_LOCK = REPO_ROOT / "Cargo.lock"
LICENSES_FILE = REPO_ROOT / "THIRD-PARTY-LICENSES"
MAX_DETAILS = 25
OR_PATTERN = re.compile(r"\bor\b", re.IGNORECASE)


def load_toml(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError:
        fail([f"{path.relative_to(REPO_ROOT)} does not exist"])
    except tomllib.TOMLDecodeError as error:
        fail([f"{path.relative_to(REPO_ROOT)} is not valid TOML: {error}"])


def package_key(package: dict) -> tuple[str, str]:
    return (str(package.get("name", "")).strip(), str(package.get("version", "")).strip())


def entry_key(entry: dict) -> tuple[str, str]:
    return (
        str(entry.get("package_name", "")).strip(),
        str(entry.get("package_version", "")).strip(),
    )


def format_key(key: tuple[str, str]) -> str:
    name, version = key
    return f"{name} {version}"


def third_party_cargo_packages(cargo_lock: dict) -> set[tuple[str, str]]:
    packages = cargo_lock.get("package", [])
    keys = [package_key(package) for package in packages if "source" in package]
    return set(keys)


def license_entries(licenses: dict) -> tuple[set[tuple[str, str]], list[str]]:
    entries = licenses.get("third_party_libraries", [])
    keys = [entry_key(entry) for entry in entries]
    duplicate_keys = sorted(key for key, count in Counter(keys).items() if count > 1)
    errors = [
        f"duplicate THIRD-PARTY-LICENSES entry: {format_key(key)}"
        for key in duplicate_keys
    ]
    return set(keys), errors


def validate_license_entry(entry: dict) -> list[str]:
    key = format_key(entry_key(entry))
    errors: list[str] = []
    license_expression = str(entry.get("license", "")).strip()

    if not license_expression:
        errors.append(f"{key}: missing license field")
    elif license_expression.upper() in {"UNKNOWN", "NOT FOUND"}:
        errors.append(f"{key}: unresolved license value {license_expression!r}")
    elif OR_PATTERN.search(license_expression):
        errors.append(
            f"{key}: license field contains OR; choose MIT when present, "
            "otherwise Apache-2.0 when present"
        )

    embedded_licenses = entry.get("licenses")
    if not embedded_licenses:
        errors.append(f"{key}: missing embedded license text")
        return errors

    for index, embedded_license in enumerate(embedded_licenses, start=1):
        embedded_name = str(embedded_license.get("license", "")).strip()
        embedded_text = str(embedded_license.get("text", "")).strip()
        prefix = f"{key}: embedded license #{index}"

        if not embedded_name:
            errors.append(f"{prefix}: missing license name")
        elif embedded_name.upper() in {"UNKNOWN", "NOT FOUND"}:
            errors.append(f"{prefix}: unresolved license name {embedded_name!r}")

        if not embedded_text:
            errors.append(f"{prefix}: missing license text")
        elif "NOT FOUND" in embedded_text.upper():
            errors.append(f"{prefix}: unresolved license text")

    return errors


def fail(errors: list[str]) -> None:
    print("THIRD-PARTY-LICENSES validation failed:", file=sys.stderr)
    for error in errors[:MAX_DETAILS]:
        print(f"  - {error}", file=sys.stderr)
    remaining = len(errors) - MAX_DETAILS
    if remaining > 0:
        print(f"  ... and {remaining} more issue(s)", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    cargo_lock = load_toml(CARGO_LOCK)
    licenses = load_toml(LICENSES_FILE)

    cargo_keys = third_party_cargo_packages(cargo_lock)
    license_keys, errors = license_entries(licenses)

    missing = sorted(cargo_keys - license_keys)
    extra = sorted(license_keys - cargo_keys)

    errors.extend(
        f"missing THIRD-PARTY-LICENSES entry for {format_key(key)}" for key in missing
    )
    errors.extend(
        f"extra THIRD-PARTY-LICENSES entry not found in Cargo.lock: {format_key(key)}"
        for key in extra
    )

    for entry in licenses.get("third_party_libraries", []):
        errors.extend(validate_license_entry(entry))

    if errors:
        fail(errors)

    print(
        "THIRD-PARTY-LICENSES covers "
        f"{len(cargo_keys)} third-party Cargo.lock package(s)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
