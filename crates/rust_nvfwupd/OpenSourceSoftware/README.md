# NVFWUPD Open-Source Disclosure Bundle

`OpenSourceSoftware.tar.gz` contains the exact third-party crate sources used by
the locked NVFWUPD release graph for the supported x86-64 and ARM64 musl
targets. `Licenses-3rd-Party` contains the corresponding license inventory.

Regenerate both files from the repository root after an NVFWUPD dependency
change:

```console
python3 ci/generate_nvfwupd_open_source_bundle.py
```

Verify that the checked-in files match the locked dependency graph:

```console
python3 ci/generate_nvfwupd_open_source_bundle.py --check
```

The source archive intentionally excludes package-local `Cargo.lock` files.
Those files describe historical development environments for individual
dependency crates; they are not used by Cargo when building NVFWUPD from the
workspace lockfile.
