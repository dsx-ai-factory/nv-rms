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
