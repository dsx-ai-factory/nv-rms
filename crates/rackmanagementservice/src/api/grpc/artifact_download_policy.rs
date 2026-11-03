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

//! Host policy for firmware artifact HTTP(S) downloads.
//!
//! Defaults stay unrestricted: an empty `artifact_https_hosts` list and
//! `artifact_allow_private_targets = true`. That is a compatibility choice so
//! labs and existing catalogs keep working without a config change. Set a host
//! list or disable private targets to guard against CWE-918. The same opt-in
//! default was used for `artifact_require_sha256` on the sanitized-manifest
//! downloader. Production overlays should set the keys; `helm/values.yaml`
//! stays open for labs.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use super::artifact_download::ArtifactError;
use crate::config::RmsWorkflowConfig;
use crate::utilities::error::{Result, RmsError};
use crate::utilities::special_purpose_ip::{PrivateUse, is_special_purpose_ip};

/// Deployment policy applied to every firmware artifact HTTP(S) download.
///
/// An empty [`Self::allowed_hosts`] list preserves historical behavior: any
/// host is accepted. [`Self::allow_private_targets`] defaults to `true` so
/// lab Artifactory and loopback test servers keep working.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactDownloadPolicy {
    /// Hostname or `*.suffix` patterns (`*.suffix` matches the suffix and its
    /// subdomains). Empty means no allow-list.
    pub allowed_hosts: Vec<String>,
    /// When false, reject loopback, link-local, unspecified, multicast,
    /// private (RFC1918 / ULA), CGNAT, NAT64, and other IANA special-purpose
    /// resolved addresses.
    pub allow_private_targets: bool,
}

impl Default for ArtifactDownloadPolicy {
    fn default() -> Self {
        Self {
            allowed_hosts: Vec::new(),
            allow_private_targets: true,
        }
    }
}

impl ArtifactDownloadPolicy {
    /// Build a policy from `[workflows]` artifact-download keys.
    pub fn from_workflows(workflows: &RmsWorkflowConfig) -> Result<Self> {
        let mut allowed_hosts = Vec::with_capacity(workflows.artifact_https_hosts.len());
        for host in &workflows.artifact_https_hosts {
            allowed_hosts.push(parse_allow_list_entry(host)?);
        }

        Ok(Self {
            allowed_hosts,
            allow_private_targets: workflows.artifact_allow_private_targets,
        })
    }
}

/// Rejects HTTP(S) artifact URLs that violate `policy` without performing DNS.
///
/// This is the synchronous half of the check: scheme, host present, allow-list,
/// and IP-literal classification. Hostname lookup and special-purpose checks on
/// resolved names belong to [`ArtifactPolicyResolver`], which already runs
/// under `spawn_blocking` and pins the connection. hyper-util skips that
/// resolver for literals, so literals must be classified here.
pub(crate) fn validate_artifact_http_url(
    url: &reqwest::Url,
    policy: &ArtifactDownloadPolicy,
) -> std::result::Result<(), ArtifactError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ArtifactError::InvalidArgument(format!(
            "firmware artifact redirect or URL must use http or https, not '{}'",
            url.scheme()
        )));
    }

    // `host_str()` brackets IPv6 (`[::1]`); `host()` is the parsed address.
    let Some(host) = url.host_str().filter(|host| !host.is_empty()) else {
        return Err(ArtifactError::InvalidArgument(format!(
            "firmware artifact URL is missing a host: {}",
            crate::api::grpc::firmware_artifact_paths::redact_location_for_logging(url.as_str())
        )));
    };

    if !host_allowed(host, &policy.allowed_hosts) {
        return Err(ArtifactError::InvalidArgument(format!(
            "firmware artifact host '{host}' is not on the artifact host allow-list"
        )));
    }

    // Seam: hostnames are not resolved here. When private targets are allowed,
    // literals are allowed too. When they are not, classify literals only;
    // hyper-util will not call the policy resolver for them.
    if policy.allow_private_targets {
        return Ok(());
    }
    if let Some(ip) = parse_artifact_ip_literal(host)
        && is_special_purpose_ip(ip, PrivateUse::Disallowed)
    {
        return Err(disallowed_address_error(host));
    }
    Ok(())
}

/// Resolves `host` to connect sockets and enforces private-target policy.
///
/// Used only from [`ArtifactPolicyResolver`]. Restricted-address scanning is
/// skipped when private targets are allowed; the resolved set is returned
/// as-is so the connector can bind those addresses.
pub(crate) fn resolve_validated_artifact_addrs(
    host: &str,
    policy: &ArtifactDownloadPolicy,
) -> std::result::Result<Vec<SocketAddr>, ArtifactError> {
    // Literal IPs skip DNS; hostnames are resolved to the addresses we will pin.
    // `Url::host_str()` yields `[::1]`; `IpAddr` parses `::1`.
    let addrs = if let Some(ip) = parse_artifact_ip_literal(host) {
        vec![SocketAddr::new(ip, 0)]
    } else {
        lookup_artifact_host(host)?
    };

    // Private/loopback/link-local are allowed; do not scan the resolved set.
    if policy.allow_private_targets {
        return Ok(addrs);
    }

    // Fail closed if any resolved address is restricted.
    if addrs
        .iter()
        .any(|addr| is_special_purpose_ip(addr.ip(), PrivateUse::Disallowed))
    {
        return Err(disallowed_address_error(host));
    }

    Ok(addrs)
}

fn parse_artifact_ip_literal(host: &str) -> Option<IpAddr> {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.parse().ok()
}

fn lookup_artifact_host(host: &str) -> std::result::Result<Vec<SocketAddr>, ArtifactError> {
    let addrs = (host, 0)
        .to_socket_addrs()
        .map_err(|error| {
            ArtifactError::InvalidArgument(format!(
                "firmware artifact host '{host}' could not be resolved: {error}"
            ))
        })?
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        return Err(ArtifactError::InvalidArgument(format!(
            "firmware artifact host '{host}' could not be resolved"
        )));
    }
    Ok(addrs)
}

fn disallowed_address_error(host: &str) -> ArtifactError {
    ArtifactError::InvalidArgument(format!(
        "firmware artifact host '{host}' resolves to a disallowed address"
    ))
}

/// DNS resolver that applies [`ArtifactDownloadPolicy`] and returns the checked
/// addresses to reqwest for the original request and every redirect hop.
#[derive(Clone, Debug)]
pub(crate) struct ArtifactPolicyResolver {
    policy: ArtifactDownloadPolicy,
}

impl ArtifactPolicyResolver {
    /// Build a resolver that enforces `policy` at connect time.
    pub(crate) fn new(policy: ArtifactDownloadPolicy) -> Self {
        Self { policy }
    }
}

impl reqwest::dns::Resolve for ArtifactPolicyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().trim_end_matches('.').to_owned();
        let policy = self.policy.clone();
        Box::pin(async move {
            let resolved = tokio::task::spawn_blocking(move || {
                resolve_validated_artifact_addrs(&host, &policy)
            })
            .await
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
                format!("firmware artifact host lookup failed: {error}").into()
            })?
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
                // Wrapped as a connect/DNS error; keep the policy text in Display
                // so `ArtifactError::transport` still reports `disallowed address`.
                std::io::Error::other(error.to_string()).into()
            })?;
            Ok(Box::new(resolved.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

pub(crate) fn host_allowed(host: &str, allowed_hosts: &[String]) -> bool {
    if allowed_hosts.is_empty() {
        return true;
    }

    let host = normalize_host(host);
    allowed_hosts
        .iter()
        .any(|pattern| host_matches(&host, pattern))
}

fn host_matches(host: &str, pattern: &str) -> bool {
    // `*.suffix` matches the suffix itself and its subdomains.
    if let Some(suffix) = pattern.strip_prefix("*.") {
        host == suffix || host.ends_with(&format!(".{suffix}"))
    } else {
        host == pattern
    }
}

/// Parse an `artifact_https_hosts` entry the same way URL hosts are parsed.
///
/// `url::Host::parse` lowercases, applies IDNA, and rejects empty strings and
/// interior whitespace. A trailing FQDN dot is stripped afterwards. `*.suffix`
/// is a DNS label to that parser, so the wildcard rule stays in
/// [`host_matches`]. IPv6 literals must be written with brackets (`[::1]`).
fn parse_allow_list_entry(entry: &str) -> Result<String> {
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return Err(RmsError::invalid_argument(
            "workflows.artifact_https_hosts entries must not be empty",
        ));
    }
    canonical_host(trimmed).ok_or_else(|| {
        RmsError::invalid_argument(format!(
            "workflows.artifact_https_hosts entry '{trimmed}' is not a valid host"
        ))
    })
}

fn canonical_host(host: &str) -> Option<String> {
    match url::Host::parse(host).ok()? {
        url::Host::Domain(domain) => Some(domain.trim_end_matches('.').to_string()),
        url::Host::Ipv4(ip) => Some(ip.to_string()),
        url::Host::Ipv6(ip) => Some(ip.to_string()),
    }
}

fn normalize_host(host: &str) -> String {
    // URL `host_str()` is already lowercased and punycoded; it keeps a trailing
    // FQDN dot. Allow-list entries go through [`parse_allow_list_entry`].
    let host = host.trim();
    canonical_host(host).unwrap_or_else(|| host.trim_end_matches('.').to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(value: &str) -> reqwest::Url {
        reqwest::Url::parse(value).expect("url")
    }

    #[test]
    fn empty_allow_list_accepts_any_public_https_host() {
        let policy = ArtifactDownloadPolicy::default();
        assert!(
            validate_artifact_http_url(&url("https://artifactory.example.com/pkg"), &policy)
                .is_ok()
        );
    }

    #[test]
    fn allow_list_rejects_other_hosts() {
        let policy = ArtifactDownloadPolicy {
            allowed_hosts: vec!["artifactory.example.com".to_owned()],
            ..ArtifactDownloadPolicy::default()
        };
        let err = validate_artifact_http_url(&url("https://evil.example/pkg.bin"), &policy)
            .expect_err("foreign host");
        assert!(matches!(err, ArtifactError::InvalidArgument(_)), "{err:?}");
        let err = err.to_string();
        assert!(err.contains("allow-list"), "{err}");
    }

    #[test]
    fn wildcard_allow_list_matches_suffix() {
        let policy = ArtifactDownloadPolicy {
            allowed_hosts: vec!["*.jfrog.io".to_owned()],
            ..ArtifactDownloadPolicy::default()
        };
        assert!(
            validate_artifact_http_url(&url("https://acme.jfrog.io/artifactory/pkg"), &policy)
                .is_ok()
        );
        assert!(validate_artifact_http_url(&url("https://jfrog.io/pkg"), &policy).is_ok());
    }

    #[test]
    fn loopback_is_rejected_when_private_targets_are_disallowed() {
        let policy = ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        };
        let err = validate_artifact_http_url(&url("http://127.0.0.1/pkg.bin"), &policy)
            .expect_err("loopback")
            .to_string();
        assert!(err.contains("disallowed address"), "{err}");
    }

    #[test]
    fn bracketed_ipv6_literals_are_not_resolved_as_names() {
        let policy = ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        };
        for location in [
            "http://[::1]/pkg.bin",
            "http://[fd00::1]/pkg.bin",
            "http://[2001:db8::1]/x",
        ] {
            let err = validate_artifact_http_url(&url(location), &policy)
                .expect_err(location)
                .to_string();
            assert!(err.contains("disallowed address"), "{location}: {err}");
            assert!(!err.contains("could not be resolved"), "{location}: {err}");
        }
    }

    #[test]
    fn allow_list_matches_bracketed_and_unbracketed_ipv6() {
        let policy = ArtifactDownloadPolicy {
            allowed_hosts: vec!["::1".to_owned()],
            ..ArtifactDownloadPolicy::default()
        };
        assert!(validate_artifact_http_url(&url("http://[::1]/pkg.bin"), &policy).is_ok());
    }

    #[test]
    fn rfc1918_is_rejected_when_private_targets_are_disallowed() {
        let policy = ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        };
        let err = validate_artifact_http_url(&url("http://10.1.2.3/pkg.bin"), &policy)
            .expect_err("private")
            .to_string();
        assert!(err.contains("disallowed address"), "{err}");
    }

    #[test]
    fn validate_does_not_resolve_hostnames() {
        let policy = ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        };
        assert!(
            validate_artifact_http_url(
                &url("http://this-host-does-not-exist.invalid/pkg.bin"),
                &policy
            )
            .is_ok()
        );
        assert!(validate_artifact_http_url(&url("http://localhost/pkg.bin"), &policy).is_ok());
    }

    #[test]
    fn hostname_resolution_rejects_loopback_when_private_targets_are_disallowed() {
        let policy = ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        };
        let err = resolve_validated_artifact_addrs("localhost", &policy)
            .expect_err("localhost")
            .to_string();
        assert!(err.contains("disallowed address"), "{err}");
    }

    #[test]
    fn hostname_resolution_returns_checked_sockets_for_literals() {
        let policy = ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        };
        let addrs = resolve_validated_artifact_addrs("8.8.8.8", &policy).expect("public ip");
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].ip(), "8.8.8.8".parse::<IpAddr>().unwrap());
        assert_eq!(addrs[0].port(), 0);
    }

    #[test]
    fn hostname_resolution_returns_loopback_when_private_targets_are_allowed() {
        let addrs =
            resolve_validated_artifact_addrs("127.0.0.1", &ArtifactDownloadPolicy::default())
                .expect("loopback allowed");
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].ip(), "127.0.0.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn private_target_policy_rejects_cgnat_this_network_reserved_and_nat64() {
        let policy = ArtifactDownloadPolicy {
            allow_private_targets: false,
            ..ArtifactDownloadPolicy::default()
        };
        for host in ["100.64.0.1", "0.0.0.1", "240.0.0.1", "64:ff9b::a00:1"] {
            let err = resolve_validated_artifact_addrs(host, &policy)
                .expect_err(host)
                .to_string();
            assert!(err.contains("disallowed address"), "{host}: {err}");
        }
        let err = resolve_validated_artifact_addrs("::ffff:10.0.0.1", &policy)
            .expect_err("mapped rfc1918")
            .to_string();
        assert!(err.contains("disallowed address"), "{err}");
    }

    #[test]
    fn from_workflows_rejects_blank_allow_list_entries() {
        let workflows = RmsWorkflowConfig {
            artifact_https_hosts: vec![" ".to_owned()],
            ..RmsWorkflowConfig::default()
        };
        let err = ArtifactDownloadPolicy::from_workflows(&workflows).expect_err("blank host");
        assert!(err.message.contains("must not be empty"));
    }

    #[test]
    fn url_host_str_punycodes_and_keeps_a_trailing_dot() {
        assert_eq!(
            url("https://Example.COM./x").host_str(),
            Some("example.com.")
        );
        assert_eq!(
            url("https://ÜNÏCODE.Example.com/x").host_str(),
            Some("xn--ncode-cta3g.example.com")
        );
        assert_eq!(
            url("https://a.jfrog.io:8443/x").host_str(),
            Some("a.jfrog.io")
        );
        assert!(reqwest::Url::parse("https://exa mple.com/x").is_err());
    }

    #[test]
    fn from_workflows_parses_entries_like_url_hosts() {
        let policy = ArtifactDownloadPolicy::from_workflows(&RmsWorkflowConfig {
            artifact_https_hosts: vec![
                "Example.COM.".to_owned(),
                "*.JFrog.io".to_owned(),
                " a.example.com ".to_owned(),
                "[::1]".to_owned(),
                "ÜNÏCODE.Example.com".to_owned(),
            ],
            ..RmsWorkflowConfig::default()
        })
        .expect("valid entries");
        assert_eq!(
            policy.allowed_hosts,
            [
                "example.com",
                "*.jfrog.io",
                "a.example.com",
                "::1",
                "xn--ncode-cta3g.example.com",
            ]
        );
        assert!(validate_artifact_http_url(&url("https://example.com/x"), &policy).is_ok());
        assert!(validate_artifact_http_url(&url("https://Example.COM./x"), &policy).is_ok());
        assert!(validate_artifact_http_url(&url("https://ÜNÏCODE.Example.com/x"), &policy).is_ok());
        assert!(validate_artifact_http_url(&url("https://acme.jfrog.io/x"), &policy).is_ok());
        assert!(validate_artifact_http_url(&url("http://[::1]/x"), &policy).is_ok());
    }

    #[test]
    fn from_workflows_rejects_interior_whitespace_and_unbracketed_ipv6() {
        for entry in ["exa mple.com", "::1"] {
            let workflows = RmsWorkflowConfig {
                artifact_https_hosts: vec![entry.to_owned()],
                ..RmsWorkflowConfig::default()
            };
            let err = ArtifactDownloadPolicy::from_workflows(&workflows).expect_err(entry);
            assert!(
                err.message.contains("not a valid host"),
                "{entry}: {}",
                err.message
            );
        }
    }
}
