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

use std::net::IpAddr;

/// Format `host:port` for use in HTTP(S) or gRPC URL authorities.
///
/// IPv6 literals are bracketed per RFC 3986 (`[fd00::1]:443`). Hostnames and
/// IPv4 addresses are left unchanged.
pub fn url_authority(host: &str, port: u16) -> String {
    if let Ok(ip) = host.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(v4) => format!("{v4}:{port}"),
            IpAddr::V6(v6) => format!("[{v6}]:{port}"),
        }
    } else {
        format!("{host}:{port}")
    }
}

/// Build an `http://` or `https://` gRPC target URI (tonic / [`crate::libnmxc::Endpoint`]).
pub fn grpc_target_uri(host: &str, port: u16, use_tls: bool) -> String {
    let scheme = if use_tls { "https" } else { "http" };
    format!("{scheme}://{}", url_authority(host, port))
}

/// Build an `http://` or `https://` target URI for HTTPS REST clients.
pub fn http_target_uri(host: &str, port: u16, https: bool) -> String {
    grpc_target_uri(host, port, https)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_authority_formats_ipv6_literal() {
        assert_eq!(url_authority("fd00::1", 443), "[fd00::1]:443");
        assert_eq!(url_authority("10.0.0.1", 443), "10.0.0.1:443");
        assert_eq!(
            url_authority("switch.example.com", 443),
            "switch.example.com:443"
        );
    }

    #[test]
    fn grpc_target_uri_uses_hostname() {
        assert_eq!(
            grpc_target_uri("switch.example.com", 9370, true),
            "https://switch.example.com:9370"
        );
    }

    #[test]
    fn grpc_target_uri_brackets_ipv6_addresses() {
        assert_eq!(
            grpc_target_uri("fd00::1", 9370, false),
            "http://[fd00::1]:9370"
        );
        assert_eq!(
            grpc_target_uri("2001:db8::1", 9339, true),
            "https://[2001:db8::1]:9339"
        );
    }

    #[test]
    fn http_target_uri_matches_grpc_target_uri() {
        assert_eq!(
            http_target_uri("switch.example.com", 443, true),
            grpc_target_uri("switch.example.com", 443, true)
        );
    }

    #[test]
    fn http_target_uri_uses_hostname() {
        assert_eq!(
            http_target_uri("switch.example.com", 9370, true),
            "https://switch.example.com:9370"
        );
    }

    #[test]
    fn http_target_uri_brackets_ipv6_addresses() {
        assert_eq!(
            http_target_uri("fd00::1", 9370, false),
            "http://[fd00::1]:9370"
        );
        assert_eq!(
            http_target_uri("2001:db8::1", 9339, true),
            "https://[2001:db8::1]:9339"
        );
    }
}
