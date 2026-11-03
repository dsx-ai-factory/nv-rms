/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use vergen_gitcl::{BuildBuilder, Emitter, GitclBuilder, RustcBuilder};

    println!("cargo:rerun-if-changed=src/api/grpc/proto/switch_client.proto");
    println!("cargo:rerun-if-changed=src/api/grpc/proto/gnmi.proto");
    generate_hardware_profile_catalog()?;

    let build = BuildBuilder::default().build_timestamp(true).build()?;
    let git = GitclBuilder::default()
        .describe(true, true, None)
        .sha(true)
        .build()?;
    let rustc = RustcBuilder::default().semver(true).build()?;
    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&git)?
        .add_instructions(&rustc)?
        .emit()?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "src/api/grpc/proto/switch_client.proto",
                "src/api/grpc/proto/gnmi.proto",
            ],
            &["src/api/grpc/proto/"],
        )?;
    Ok(())
}

/// Generate a deterministic embedded catalog from every shipped YAML profile.
///
/// This is intentionally discovery-based: adding a profile file to the
/// versioned directory must not require a matching Rust registration edit.
fn generate_hardware_profile_catalog() -> Result<(), Box<dyn std::error::Error>> {
    use std::fmt::Write as _;
    use std::fs;
    use std::path::PathBuf;

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let profile_dir = manifest_dir.join("../../hardware-profiles/v1");
    println!("cargo:rerun-if-changed={}", profile_dir.display());

    let mut profiles = Vec::new();
    for entry in fs::read_dir(&profile_dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_file()
            && matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("yaml" | "yml")
            )
        {
            profiles.push(path);
        }
    }
    profiles.sort();

    let mut generated =
        String::from("pub const BUILT_IN_HARDWARE_PROFILES: &[(&str, &str)] = &[\n");
    for path in profiles {
        println!("cargo:rerun-if-changed={}", path.display());
        let filename = path
            .file_name()
            .and_then(|filename| filename.to_str())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "hardware profile filename is not valid UTF-8",
                )
            })?;
        writeln!(
            generated,
            "    ({filename:?}, include_str!({path:?})),",
            path = path.to_string_lossy()
        )?;
    }
    generated.push_str("];\n");

    let output = PathBuf::from(std::env::var("OUT_DIR")?).join("hardware_profiles.rs");
    fs::write(output, generated)?;
    Ok(())
}
