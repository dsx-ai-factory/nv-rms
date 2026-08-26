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

use crate::utilities::error::RmsError;

/// True when retrying gNMI Capabilities may succeed after a transport connect failure.
pub fn is_capabilities_connect_error(err: &RmsError) -> bool {
    err.message.contains("gNMI Capabilities connect failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;

    #[test]
    fn is_capabilities_connect_error_detects_transport_failures() {
        let err = RmsError::internal(
            "gNMI Capabilities connect failed for https://10.0.0.1:9339 \
             (authority=switch.example.com): Transport error: transport error",
        );
        assert!(is_capabilities_connect_error(&err));

        let rpc_err =
            RmsError::internal("gNMI Capabilities failed for https://10.0.0.1:9339: gRPC status");
        assert!(!is_capabilities_connect_error(&rpc_err));

        let other = RmsError::new(ErrorCode::InvalidArgument, "bad host");
        assert!(!is_capabilities_connect_error(&other));
    }
}
