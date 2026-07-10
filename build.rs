/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

fn main() -> Result<(), Box<dyn std::error::Error>> {
    use vergen_gitcl::{BuildBuilder, Emitter, GitclBuilder, RustcBuilder};

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
