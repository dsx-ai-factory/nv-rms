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

use std::time::Duration;

/// Host the NVSSVT launcher API is reached on by default. NVSSVT runs as a
/// second container in the same pod as RMS, so the two share a network
/// namespace and loopback is the whole story -- no service discovery, no
/// config, nothing to template per deployment.
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// Port the NVSSVT launcher API listens on by default.
pub const DEFAULT_PORT: u16 = 8000;

/// Default bound on a single HTTP round trip (see
/// [`NvssvtConfig::request_timeout`]).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for reaching the NVSSVT launcher HTTP API
/// (`nvssvt_server.py`, run via `uvicorn nvssvt_server:app`).
///
/// [`Default`] targets the in-pod sidecar (`http://127.0.0.1:8000`), which is
/// the deployed topology; the host/port overrides exist for development
/// against an NVSSVT instance running somewhere else.
#[derive(Debug, Clone)]
pub struct NvssvtConfig {
    /// Base URL of the NVSSVT launcher, e.g. `http://127.0.0.1:8000`. Never
    /// carries a trailing slash: [`NvssvtConfig::new`] strips one if given.
    pub base_url: String,
    /// Timeout applied to each individual HTTP call (launch or status poll).
    /// Scans themselves run far longer than this; the timeout only bounds a
    /// single request/response round trip.
    pub request_timeout: Duration,
}

impl Default for NvssvtConfig {
    fn default() -> Self {
        Self::new(DEFAULT_HOST, DEFAULT_PORT)
    }
}

impl NvssvtConfig {
    /// Targets the launcher at `host:port` over plain HTTP. A bare IPv6 host
    /// is bracketed so it forms a valid authority.
    pub fn new(host: &str, port: u16) -> Self {
        let host = host.trim();
        let authority = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host.to_string()
        };
        Self {
            base_url: format!("http://{authority}:{port}"),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Targets `base_url` verbatim, for the rare deployment that fronts
    /// NVSSVT with something other than plain `http://host:port` (a path
    /// prefix, say). Any trailing slash is stripped.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into().trim_end_matches('/').to_string();
        self
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_targets_the_in_pod_sidecar() {
        let config = NvssvtConfig::default();

        assert_eq!(config.base_url, "http://127.0.0.1:8000");
        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
    }

    #[test]
    fn bare_ipv6_hosts_are_bracketed() {
        assert_eq!(NvssvtConfig::new("::1", 8000).base_url, "http://[::1]:8000");
        assert_eq!(
            NvssvtConfig::new("[::1]", 8000).base_url,
            "http://[::1]:8000"
        );
    }

    #[test]
    fn explicit_base_urls_lose_their_trailing_slash() {
        let config = NvssvtConfig::default().with_base_url("http://nvssvt.internal:9000/");

        assert_eq!(config.base_url, "http://nvssvt.internal:9000");
    }
}
