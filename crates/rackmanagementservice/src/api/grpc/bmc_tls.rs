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

use crate::domain::rack::EndpointConfig;

/// Apply the RMS-owned BMC/Redfish TLS policy to an endpoint.
///
/// Callers cannot change BMC certificate verification per request. The only
/// source of this flag is `[bmc] insecure_bmc` (historical default `true`,
/// because production BMC certificates are typically self-signed).
/// `FlatEndpoint::bmc_endpoint` / `optional_bmc_endpoint` apply this at
/// construction so handlers cannot forget.
pub(crate) fn apply_bmc_redfish_tls_policy(
    mut endpoint: EndpointConfig,
    insecure_bmc: bool,
) -> EndpointConfig {
    endpoint.dangerously_accept_invalid_certs = insecure_bmc;
    endpoint
}

#[cfg(test)]
mod tests {
    use super::apply_bmc_redfish_tls_policy;
    use crate::domain::rack::{Endpoint, EndpointConfig};

    fn endpoint() -> EndpointConfig {
        EndpointConfig::new(Endpoint {
            ip_address: "10.0.0.1".into(),
            mac_address: String::new(),
            port: 443,
            host_name: None,
        })
    }

    #[test]
    fn rms_policy_overwrites_a_client_supplied_verify_flag() {
        let mut supplied = endpoint();
        supplied.dangerously_accept_invalid_certs = false;
        assert!(apply_bmc_redfish_tls_policy(supplied, true).dangerously_accept_invalid_certs);
    }

    #[test]
    fn verify_mode_overwrites_a_client_supplied_insecure_flag() {
        let mut supplied = endpoint();
        supplied.dangerously_accept_invalid_certs = true;
        assert!(!apply_bmc_redfish_tls_policy(supplied, false).dangerously_accept_invalid_certs);
    }
}
