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
