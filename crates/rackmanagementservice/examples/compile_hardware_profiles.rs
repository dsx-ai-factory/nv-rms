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

use std::env;
use std::error::Error;

use rackmanagementservice::config::hardware_profiles::HardwareProfileCatalog;

fn main() -> Result<(), Box<dyn Error>> {
    let directory = env::args()
        .nth(1)
        .unwrap_or_else(|| "hardware-profiles/v1".to_owned());
    let catalog = HardwareProfileCatalog::load_directory(&directory)?;

    println!(
        "compiled {} hardware profile(s) from {directory}",
        catalog.len()
    );
    for profile in catalog.profiles() {
        println!(
            "profile={} schema_version={} sha256={} node_type={} enabled={}",
            profile.profile_id(),
            profile.schema_version(),
            profile.source_sha256(),
            profile.node_type().as_str(),
            profile.request_matching_enabled()
        );
        for stage in profile.stages() {
            let targets = stage
                .targets()
                .iter()
                .map(|target| target.id())
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "  stage={} targets={} activate_after={}",
                stage.number(),
                targets,
                stage.activate_after()
            );
        }
    }

    Ok(())
}
