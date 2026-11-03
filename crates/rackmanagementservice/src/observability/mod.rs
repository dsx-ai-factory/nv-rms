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

//! Backend-neutral RMS observability plumbing.

mod batch;
mod grpc;
pub(crate) mod internal_logs;
mod span_queue;
pub(crate) mod trace_metrics;

pub mod tracing;

/// Run one tracing test without other tests' process-wide callsite or environment state.
/// Returns `true` in the parent after verifying that the isolated test passed.
#[cfg(test)]
pub(crate) fn run_test_in_subprocess(test_name: &str) -> bool {
    run_test_in_subprocess_with_env(test_name, &[])
}

/// [`run_test_in_subprocess`] with `env` set for the isolated process's whole lifetime.
#[cfg(test)]
pub(crate) fn run_test_in_subprocess_with_env(test_name: &str, env: &[(&str, &str)]) -> bool {
    const CHILD_ENV: &str = "RMS_ISOLATED_TRACING_TEST";
    if std::env::var(CHILD_ENV).as_deref() == Ok(test_name) {
        return false;
    }

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_ENV, test_name)
        .envs(env.iter().copied())
        .output()
        .expect("run the tracing test in an isolated process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("test result: ok. 1 passed; 0 failed;"),
        "isolated test {test_name} did not pass exactly once:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    true
}
