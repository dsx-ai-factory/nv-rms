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

use std::path::Path;
use std::time::Duration;

use futures::TryStreamExt;
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::url::http_target_uri;

/// Cap for one BMC Redfish response body before JSON parsing.
///
/// These endpoints return control-plane JSON such as inventory, task status,
/// and action results. Firmware images are uploaded as request bodies, not
/// returned here, so 8 MiB gives ample headroom for expected responses while
/// bounding memory use from a compromised or faulty BMC.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

const DEFAULT_RESPONSE_BODY_CAPACITY: usize = 4 * 1024;

/// Redfish API root. Every BMC endpoint (including device-supplied action
/// targets and `@odata.id` links) lives under this tree, so Redfish clients
/// pass this to [`HttpClient::require_path_prefix`] to fence requests to it as
/// SSRF/confused-deputy defense in depth. Shared here so every Redfish client
/// inherits the same fence rather than redeclaring the literal.
pub const REDFISH_V1_ROOT: &str = "/redfish/v1";

/// Async HTTP client for communicating with BMC / switch REST APIs over HTTP or HTTPS.
///
/// Wraps `reqwest::Client` with Redfish conventions: JSON request/response,
/// error code mapping, basic auth, TLS verify toggle, and firmware upload
/// support. Used by the BMC/powershelf Redfish callers and for plain switch
/// reachability probes; the switch NVUE API is driven by the separate
/// `nvue_client` crate, not this client.
///
/// The underlying reqwest client reuses TCP+TLS connections across calls,
/// so repeated requests to the same BMC skip the TLS handshake.
pub struct HttpClient {
    client: reqwest::Client,
    base_url: String,
    host: String,
    port: u16,
    username: String,
    password: SecretString,
    max_response_bytes: usize,
    /// Optional API tree every request must stay within (e.g. `/redfish/v1`).
    ///
    /// The same-origin guard in [`Self::url`] already blocks a host swap; this
    /// adds a second, protocol-aware fence so a compromised/MITM'd device that
    /// hands back a same-origin but off-tree `target`/`@odata.id` cannot steer a
    /// request (and its Basic-auth) to an unrelated endpoint on that host. Left
    /// `None` for protocol-agnostic uses such as the plain reachability probe.
    required_path_prefix: Option<String>,
}

impl HttpClient {
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
    pub const UPLOAD_TIMEOUT: Duration = Duration::from_secs(600);

    pub fn new(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        dangerously_accept_invalid_certs: bool,
        https: bool,
    ) -> Result<Self> {
        Self::new_with_connect_target(
            host,
            host,
            port,
            username,
            password,
            dangerously_accept_invalid_certs,
            https,
        )
    }

    /// HTTPS/HTTP client that dials `connect_host` while using `tls_host` in the URL and TLS SNI.
    pub fn new_with_connect_target(
        tls_host: &str,
        connect_host: &str,
        port: u16,
        username: &str,
        password: &str,
        dangerously_accept_invalid_certs: bool,
        https: bool,
    ) -> Result<Self> {
        // Gate credential transmission on transport trust. Basic auth attaches the
        // BMC/switch admin password to every request, so refuse to construct a
        // credential-bearing client over plaintext HTTP where any network observer
        // could recover it. Cert-unverified TLS remains permitted (many BMCs ship
        // self-signed certs); that trade-off is opt-in via the insecure flags.
        if !username.is_empty() && !https {
            return Err(RmsError::invalid_argument(format!(
                "refusing to send Basic auth credentials to {tls_host}:{port} over plaintext \
                 HTTP; credentials would be recoverable by any network observer. Use HTTPS."
            )));
        }

        let mut builder = reqwest::Client::builder()
            .danger_accept_invalid_certs(dangerously_accept_invalid_certs)
            .connect_timeout(Duration::from_secs(10));

        if dangerously_accept_invalid_certs {
            // Root stores are unused when certificate verification is disabled,
            // and loading native roots is expensive on some platforms.
            builder = builder.tls_built_in_root_certs(false);
        }

        if tls_host != connect_host
            && let Ok(addr) = format!("{connect_host}:{port}").parse::<std::net::SocketAddr>()
        {
            builder = builder.resolve(tls_host, addr);
        }

        let client = builder
            .build()
            .map_err(|e| RmsError::internal(format!("failed to create HTTP client: {e}")))?;

        Ok(Self {
            client,
            base_url: http_target_uri(tls_host, port, https),
            host: connect_host.to_owned(),
            port,
            username: username.to_owned(),
            password: SecretString::from(password.to_owned()),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            required_path_prefix: None,
        })
    }

    /// Constrain every request to an API subtree (e.g. `/redfish/v1`).
    ///
    /// Callers that only ever talk one protocol should set this so that
    /// device-supplied endpoints (e.g. Redfish action `target`s and
    /// `@odata.id` links) are fenced to that tree in addition to the
    /// mandatory same-origin check. The prefix must itself be an absolute
    /// path; a leading `/` is added if missing and any trailing `/` trimmed so
    /// the boundary match is unambiguous.
    #[must_use]
    pub fn require_path_prefix(mut self, prefix: impl AsRef<str>) -> Self {
        let prefix = prefix.as_ref().trim_end_matches('/');
        let normalized = if prefix.starts_with('/') {
            prefix.to_owned()
        } else {
            format!("/{prefix}")
        };
        self.required_path_prefix = Some(normalized);
        self
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn get(&self, endpoint: &str, timeout: Duration) -> Result<Value> {
        let req = self.client.get(self.url(endpoint)?).timeout(timeout);

        let resp = self
            .apply_auth(req)
            .send()
            .await
            .map_err(|e| map_reqwest_error(&e, &format!("GET {endpoint}")))?;

        parse_response(resp, &format!("GET {endpoint}"), self.max_response_bytes).await
    }

    pub async fn post(&self, endpoint: &str, payload: &Value, timeout: Duration) -> Result<Value> {
        let req = self
            .client
            .post(self.url(endpoint)?)
            .json(payload)
            .timeout(timeout);

        let resp = self
            .apply_auth(req)
            .send()
            .await
            .map_err(|e| map_reqwest_error(&e, &format!("POST {endpoint}")))?;

        parse_response(resp, &format!("POST {endpoint}"), self.max_response_bytes).await
    }

    pub async fn patch(&self, endpoint: &str, payload: &Value, timeout: Duration) -> Result<Value> {
        let req = self
            .client
            .patch(self.url(endpoint)?)
            .json(payload)
            .timeout(timeout);

        let resp = self
            .apply_auth(req)
            .send()
            .await
            .map_err(|e| map_reqwest_error(&e, &format!("PATCH {endpoint}")))?;

        parse_response(resp, &format!("PATCH {endpoint}"), self.max_response_bytes).await
    }

    pub async fn patch_with_if_match(
        &self,
        endpoint: &str,
        payload: &Value,
        if_match: &str,
        timeout: Duration,
    ) -> Result<Value> {
        let req = self
            .client
            .patch(self.url(endpoint)?)
            .header("If-Match", if_match)
            .json(payload)
            .timeout(timeout);

        let resp = self
            .apply_auth(req)
            .send()
            .await
            .map_err(|e| map_reqwest_error(&e, &format!("PATCH {endpoint}")))?;

        parse_response(resp, &format!("PATCH {endpoint}"), self.max_response_bytes).await
    }

    /// Multipart firmware upload (compute nodes).
    ///
    /// Sends a multipart POST with:
    /// - `UpdateParameters`: JSON part with update config
    /// - `UpdateFile`: the firmware binary (streamed from disk)
    pub async fn post_multipart(
        &self,
        endpoint: &str,
        file_path: &str,
        params: &Value,
        timeout: Duration,
    ) -> Result<Value> {
        let path = Path::new(file_path);
        let file_name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();

        let file = tokio::fs::File::open(file_path)
            .await
            .map_err(|e| RmsError::not_found(format!("cannot open file {file_path}: {e}")))?;

        let stream = ReaderStream::new(file);
        let file_body = reqwest::Body::wrap_stream(stream);

        let form = reqwest::multipart::Form::new()
            .part(
                "UpdateParameters",
                reqwest::multipart::Part::text(params.to_string())
                    .mime_str("application/json")
                    .map_err(|e| RmsError::internal(format!("invalid MIME type: {e}")))?,
            )
            .part(
                "UpdateFile",
                reqwest::multipart::Part::stream(file_body)
                    .file_name(file_name)
                    .mime_str("application/octet-stream")
                    .map_err(|e| RmsError::internal(format!("invalid MIME type: {e}")))?,
            );

        let req = self
            .client
            .post(self.url(endpoint)?)
            .multipart(form)
            .timeout(timeout);

        let resp = self
            .apply_auth(req)
            .send()
            .await
            .map_err(|e| map_reqwest_error(&e, &format!("multipart POST {endpoint}")))?;

        parse_response(
            resp,
            &format!("multipart POST {endpoint}"),
            self.max_response_bytes,
        )
        .await
    }

    /// Resolve a request endpoint against the client's `base_url`, refusing any
    /// endpoint that would move the request off the client's own origin.
    ///
    /// Redfish action targets are frequently read verbatim from device
    /// responses (`target`, `@Redfish.ActionInfo`, `@odata.id`). Since
    /// `base_url` is authority-only (`scheme://host:port`, no path), naive
    /// concatenation would let a crafted value such as `//evil.com/x`,
    /// `https://evil.com/x`, or `@evil.com/x` reopen the URL authority and make
    /// reqwest connect to -- and attach the device's Basic-auth credentials to
    /// -- an attacker-chosen host.
    ///
    /// Rather than hand-parse, resolution and the same-origin / absolute-path /
    /// dot-segment / prefix checks are delegated to
    /// [`common::net::resolve_same_origin`]; this method only maps its rejection
    /// to an [`RmsError`] and logs the (host-controlled) detail for forensics.
    fn url(&self, endpoint: &str) -> Result<reqwest::Url> {
        common::net::resolve_same_origin(
            &self.base_url,
            endpoint,
            self.required_path_prefix.as_deref(),
        )
        .map_err(|rejection| match rejection {
            common::net::EndpointRejection::UnparseableBase(error) => {
                // The base URL is derived from the caller-supplied host/port, not
                // from RMS-controlled invariants, so an unparseable base is a
                // bad-input condition rather than an internal fault. Log the cause
                // for diagnostics but keep the (host-derived) detail out of the error.
                tracing::warn!(host = %self.host, error = %error, "client base URL is not parseable");
                RmsError::invalid_argument("client base URL is not a valid absolute URL")
            }
            common::net::EndpointRejection::OffOriginOrMalformed => {
                // Log the offending value (debug-escaped) for forensics, but keep
                // it out of the returned error: a compromised device controls it.
                tracing::warn!(
                    host = %self.host,
                    ?endpoint,
                    "refusing HTTP request to unsafe endpoint; it must resolve to an \
                     absolute path on the client's own origin"
                );

                RmsError::invalid_argument(
                    "HTTP endpoint must resolve to an absolute path on the client's own origin",
                )
            }
        })
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.username.is_empty() {
            req
        } else {
            req.basic_auth(&self.username, Some(self.password.expose_secret()))
        }
    }
}

fn http_error_code(status: u16) -> ErrorCode {
    match status {
        401 => ErrorCode::Unauthenticated,
        404 => ErrorCode::NotFound,
        408 => ErrorCode::Timeout,
        409 => ErrorCode::AlreadyExists,
        503 => ErrorCode::Unavailable,
        _ => ErrorCode::Internal,
    }
}

fn map_reqwest_error(err: &reqwest::Error, context: &str) -> RmsError {
    let code = if err.is_timeout() {
        ErrorCode::Timeout
    } else if err.is_connect() {
        let msg = err.to_string().to_lowercase();
        if msg.contains("dns") || msg.contains("resolve") {
            ErrorCode::DnsResolutionFailed
        } else {
            ErrorCode::ConnectionRefused
        }
    } else if err.is_request() {
        ErrorCode::Unavailable
    } else {
        ErrorCode::Internal
    };

    RmsError::new(code, format!("{context}: {err}"))
}

async fn parse_response(
    resp: reqwest::Response,
    context: &str,
    max_response_bytes: usize,
) -> Result<Value> {
    let status = resp.status().as_u16();

    if status >= 400 {
        let msg = format!("HTTP {context} returned {status}");
        return Err(RmsError::new(http_error_code(status), msg));
    }

    let body = read_limited_response_body(resp, context, max_response_bytes).await?;

    if body.is_empty() || status == 204 {
        return Ok(Value::Object(serde_json::Map::new()));
    }

    serde_json::from_slice(&body)
        .map_err(|e| RmsError::internal(format!("JSON parse error on {context}: {e}")))
}

async fn read_limited_response_body(
    resp: reqwest::Response,
    context: &str,
    max_response_bytes: usize,
) -> Result<Vec<u8>> {
    let content_length = resp.content_length();
    if let Some(content_length) = content_length
        && content_length > max_response_bytes as u64
    {
        return Err(RmsError::internal(format!(
            "response body for {context} exceeded {max_response_bytes} bytes (got {content_length} bytes)"
        )));
    }

    let stream = resp.bytes_stream().map_err(std::io::Error::other);
    let mut reader = StreamReader::new(stream).take((max_response_bytes as u64).saturating_add(1));
    let initial_capacity = content_length
        .map(|content_length| content_length as usize)
        .unwrap_or(DEFAULT_RESPONSE_BODY_CAPACITY)
        .min(max_response_bytes);

    let mut body = Vec::with_capacity(initial_capacity);

    reader.read_to_end(&mut body).await.map_err(|e| {
        RmsError::internal(format!("failed to read response body for {context}: {e}"))
    })?;

    if body.len() > max_response_bytes {
        return Err(RmsError::internal(format!(
            "response body for {context} exceeded {max_response_bytes} bytes (got {} bytes)",
            body.len()
        )));
    }

    Ok(body)
}

#[cfg(test)]
impl HttpClient {
    /// Test-only constructor that accepts a plain HTTP base URL (e.g. wiremock).
    /// Public so it can be used by other test modules.
    pub fn for_test(base_url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.to_owned(),
            host: "test".to_owned(),
            port: 0,
            username: String::new(),
            password: SecretString::from(String::new()),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            required_path_prefix: None,
        }
    }

    pub fn for_test_with_auth(base_url: &str, username: &str, password: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.to_owned(),
            host: "test".to_owned(),
            port: 0,
            username: username.to_owned(),
            password: SecretString::from(password.to_owned()),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            required_path_prefix: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    // ── Unit tests (no server) ──

    #[test]
    fn http_error_code_maps_status_codes() {
        assert_eq!(http_error_code(401), ErrorCode::Unauthenticated);
        assert_eq!(http_error_code(404), ErrorCode::NotFound);
        assert_eq!(http_error_code(408), ErrorCode::Timeout);
        assert_eq!(http_error_code(409), ErrorCode::AlreadyExists);
        assert_eq!(http_error_code(503), ErrorCode::Unavailable);
        assert_eq!(http_error_code(500), ErrorCode::Internal);
        assert_eq!(http_error_code(502), ErrorCode::Internal);
    }

    #[test]
    fn url_construction() {
        let client = HttpClient::new("10.0.0.1", 443, "admin", "pass", false, true).unwrap();
        // The url crate normalizes away the default https port (443).
        assert_eq!(
            client.url("/redfish/v1/Systems/System_0").unwrap().as_str(),
            "https://10.0.0.1/redfish/v1/Systems/System_0"
        );
    }

    #[test]
    fn url_accepts_nested_absolute_paths() {
        let client = HttpClient::new("10.0.0.1", 8443, "admin", "pass", false, true).unwrap();
        // Device-supplied Redfish action targets are absolute and must keep
        // working, including ones carrying `@`/`:` in the path: these resolve
        // to the same origin, so they are not host swaps.
        assert_eq!(
            client
                .url("/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff")
                .unwrap()
                .as_str(),
            "https://10.0.0.1:8443/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
        );
        assert_eq!(
            client.url("/redfish/v1/Managers/BMC@0").unwrap().as_str(),
            "https://10.0.0.1:8443/redfish/v1/Managers/BMC@0"
        );
    }

    #[test]
    fn url_result_stays_on_client_origin() {
        let client = HttpClient::new("10.0.0.5", 8443, "admin", "pass", false, true).unwrap();
        let base = reqwest::Url::parse(&client.base_url).unwrap();
        assert_eq!(client.url("/redfish/v1").unwrap().origin(), base.origin());
    }

    #[test]
    fn url_rejects_percent_encoded_traversal_without_prefix_fence() {
        // A client with no required_path_prefix (the general case a future
        // caller might build) must still reject dot-segment traversal on its
        // own. `Url::join` normalizes percent-encoded dot segments to a path
        // pop, so these carry no literal `..` yet would climb the tree.
        let client = HttpClient::new("10.0.0.1", 8443, "admin", "pass", false, true).unwrap();
        assert!(client.required_path_prefix.is_none());

        for endpoint in [
            "/redfish/v1/../admin",       // literal parent segment
            "/redfish/v1/%2e%2e/admin",   // fully encoded `..`
            "/redfish/v1/%2E%2E/admin",   // encoded, upper-case hex
            "/redfish/v1/.%2e/admin",     // half-encoded `..`
            "/redfish/v1/%2e./admin",     // half-encoded `..`, other half
            "/a/b/%2e%2e/%2e%2e/etc/pwd", // stacked encoded traversal
        ] {
            let err = client
                .url(endpoint)
                .expect_err(&format!("traversal endpoint {endpoint:?} must be rejected"));
            assert_eq!(
                err.code,
                ErrorCode::InvalidArgument,
                "endpoint {endpoint:?}"
            );
        }

        // A `%2e` that is only *part* of a segment (a real filename, not a dot
        // segment) must not be mistaken for traversal.
        assert!(
            client.url("/redfish/v1/firmware%2ebin").is_ok(),
            "an encoded dot inside a longer segment is not traversal"
        );
    }

    #[test]
    fn url_enforces_required_path_prefix() {
        let client = HttpClient::new("10.0.0.1", 8443, "admin", "pass", false, true)
            .unwrap()
            .require_path_prefix("/redfish/v1");

        // Endpoints inside the fenced tree resolve normally...
        assert_eq!(
            client.url("/redfish/v1").unwrap().path(),
            "/redfish/v1",
            "the prefix itself must be allowed"
        );
        assert_eq!(
            client
                .url("/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff")
                .unwrap()
                .path(),
            "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff"
        );

        // ...while same-origin but off-tree endpoints (a compromised device
        // returning a crafted `target`/`@odata.id`) are refused. This includes
        // sibling paths that merely share the prefix as a substring.
        for endpoint in [
            "/",                    // service root probe, wrong client
            "/redfish",             // parent of the fenced tree
            "/redfish/v10/Systems", // sibling: prefix is not a path boundary
            "/redfish/v1beta/x",    // sibling: prefix is not a path boundary
            "/nvue_v1/system",      // different protocol tree
            "/admin/backdoor",      // unrelated same-host endpoint
        ] {
            let err = client
                .url(endpoint)
                .expect_err(&format!("off-tree endpoint {endpoint:?} must be rejected"));
            assert_eq!(
                err.code,
                ErrorCode::InvalidArgument,
                "endpoint {endpoint:?}"
            );
        }
    }

    #[test]
    fn require_path_prefix_normalizes_slashes() {
        // A missing leading slash is added and a trailing slash trimmed so the
        // boundary match is unambiguous regardless of how the caller spells it.
        let client = HttpClient::new("10.0.0.1", 8443, "admin", "pass", false, true)
            .unwrap()
            .require_path_prefix("redfish/v1/");
        assert_eq!(client.required_path_prefix.as_deref(), Some("/redfish/v1"));
        assert!(client.url("/redfish/v1/Chassis").is_ok());
        assert!(client.url("/other").is_err());
    }

    #[test]
    fn url_reports_unparseable_base_as_invalid_argument() {
        let mut client = HttpClient::new("10.0.0.1", 443, "admin", "pass", false, true).unwrap();
        // A base derived from a malformed host is a bad-input condition, not an
        // RMS-internal fault, so it must not be reported as ErrorCode::Internal.
        client.base_url = "not a url".to_owned();
        let err = client.url("/redfish/v1").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn url_rejects_authority_reopening_endpoints() {
        let client = HttpClient::new("10.0.0.1", 443, "admin", "pass", false, true).unwrap();

        // Each of these would otherwise redirect the request (and its Basic-auth)
        // to another host, or is otherwise not a well-formed absolute path. The
        // same-origin check is the authoritative guard; the absolute-path and
        // `..` checks add well-formedness/traversal hygiene.
        for endpoint in [
            "@evil.com/x",           // userinfo-style: not an absolute path
            "evil.com/x",            // relative -> not an absolute path
            "//evil.com/x",          // protocol-relative authority (origin swap)
            "///evil.com/x",         // authority via extra slashes (origin swap)
            "https://evil.com/x",    // absolute URL with scheme (origin swap)
            "http://evil.com/x",     // scheme + host swap
            "\\evil.com/x",          // backslash normalization
            "/\\evil.com/x",         // slash-then-backslash authority trick
            "",                      // empty
            "/redfish/../../secret", // path traversal
        ] {
            let err = client
                .url(endpoint)
                .expect_err(&format!("endpoint {endpoint:?} must be rejected"));
            assert_eq!(
                err.code,
                ErrorCode::InvalidArgument,
                "endpoint {endpoint:?}"
            );
            // The rejected (attacker-controlled) value must not leak into the error.
            assert!(
                !err.message.contains("evil.com"),
                "error must not echo the endpoint: {}",
                err.message
            );
        }
    }

    #[test]
    fn skips_root_certs_for_unverified_client() {
        let client = HttpClient::new("10.0.0.1", 443, "admin", "pass", true, true);
        assert!(client.is_ok());
    }

    #[test]
    fn connect_target_uses_tls_host_in_url() {
        let client = HttpClient::new_with_connect_target(
            "switch.example.com",
            "10.0.0.5",
            443,
            "admin",
            "pass",
            false,
            true,
        )
        .unwrap();
        // The url crate normalizes away the default https port (443).
        assert_eq!(
            client.url("/nvue_v1/system").unwrap().as_str(),
            "https://switch.example.com/nvue_v1/system"
        );
        assert_eq!(client.host(), "10.0.0.5");
    }

    #[test]
    fn url_with_custom_port() {
        let client = HttpClient::new("bmc.local", 8443, "", "", false, true).unwrap();
        assert_eq!(
            client.url("/redfish/v1").unwrap().as_str(),
            "https://bmc.local:8443/redfish/v1"
        );
    }

    #[test]
    fn url_construction_formats_ipv6_literal() {
        let client = HttpClient::new("fd00::1", 443, "admin", "pass", false, true).unwrap();
        // The url crate normalizes away the default https port (443).
        assert_eq!(
            client.url("/nvue_v1/system").unwrap().as_str(),
            "https://[fd00::1]/nvue_v1/system"
        );
    }

    #[test]
    fn host_and_port_accessors() {
        let client = HttpClient::new("10.0.0.5", 443, "user", "pw", false, true).unwrap();
        assert_eq!(client.host(), "10.0.0.5");
        assert_eq!(client.port(), 443);
    }

    #[test]
    fn auth_not_applied_when_username_empty() {
        let client = HttpClient::new("10.0.0.1", 443, "", "", false, true).unwrap();
        assert!(client.username.is_empty());
    }

    #[test]
    fn password_stored_as_secret() {
        let client = HttpClient::new("10.0.0.1", 443, "admin", "hunter2", false, true).unwrap();
        assert_eq!(client.password.expose_secret(), "hunter2");
    }

    #[test]
    fn rejects_basic_auth_over_plaintext_http() {
        let err = match HttpClient::new("10.0.0.1", 80, "s3cret-user", "s3cret-pass", false, false)
        {
            Ok(_) => panic!("plaintext HTTP with credentials must be rejected"),
            Err(e) => e,
        };
        // The error is surfaced to operators and may be logged upstream, so it
        // must never leak the credentials it is refusing to transmit.
        assert!(
            !err.message.contains("s3cret-user"),
            "error text must not include the username: {}",
            err.message
        );
        assert!(
            !err.message.contains("s3cret-pass"),
            "error text must not include the password: {}",
            err.message
        );
    }

    #[test]
    fn allows_empty_credentials_over_plaintext_http() {
        // The switch NVUE reachability probe dials plaintext HTTP with no
        // credentials; that must remain permitted.
        let client = HttpClient::new("10.0.0.1", 80, "", "", false, false);
        assert!(client.is_ok());
    }

    #[test]
    fn allows_credentials_over_unverified_tls() {
        // Many BMCs ship self-signed certs; credentials over cert-unverified TLS
        // are permitted rather than refused (opt-in via the insecure flags).
        let client = HttpClient::new("10.0.0.1", 443, "admin", "pass", true, true);
        assert!(client.is_ok());
    }

    // ── Integration tests (wiremock server) ──

    #[tokio::test]
    async fn get_returns_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Name": "Root Service"})),
            )
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let result = client
            .get("/redfish/v1", HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result["Name"], "Root Service");
    }

    #[tokio::test]
    async fn get_404_returns_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let err = client
            .get("/redfish/v1/missing", HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn get_204_returns_empty_object() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/empty"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let result = client
            .get("/empty", HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap();
        assert!(result.is_object());
        assert!(result.as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_503_returns_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/down"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let err = client
            .get("/down", HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Unavailable);
    }

    #[tokio::test]
    async fn get_invalid_json_returns_internal() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/bad-json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let err = client
            .get("/bad-json", HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("JSON parse error"));
    }

    #[tokio::test]
    async fn get_oversized_response_returns_internal() {
        let server = MockServer::start().await;
        let body = vec![b'a'; 33];

        Mock::given(method("GET"))
            .and(path("/too-large"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;

        let mut client = HttpClient::for_test(&server.uri());
        client.max_response_bytes = 32;

        let result = client.get("/too-large", HttpClient::DEFAULT_TIMEOUT).await;

        let Err(err) = result else {
            panic!("oversized response succeeded");
        };

        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("exceeded"));
        assert!(err.message.contains("32 bytes"));
    }

    #[tokio::test]
    async fn post_sends_json_payload() {
        let server = MockServer::start().await;
        let payload = serde_json::json!({"ResetType": "ForceOff"});

        Mock::given(method("POST"))
            .and(path(
                "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
            ))
            .and(body_json(&payload))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"})),
            )
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let result = client
            .post(
                "/redfish/v1/Systems/System_0/Actions/ComputerSystem.Reset",
                &payload,
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(result["status"], "ok");
    }

    #[tokio::test]
    async fn patch_sends_json_payload() {
        let server = MockServer::start().await;
        let payload = serde_json::json!({"SetPoint": 300});

        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/metrics"))
            .and(body_json(&payload))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"updated": true})),
            )
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let result = client
            .patch("/redfish/v1/metrics", &payload, HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result["updated"], true);
    }

    #[tokio::test]
    async fn patch_with_if_match_sends_header_and_payload() {
        let server = MockServer::start().await;
        let payload = serde_json::json!({"SetPoint": 300});

        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/metrics"))
            .and(header("If-Match", "\"etag-1\""))
            .and(body_json(&payload))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"updated": true})),
            )
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let result = client
            .patch_with_if_match(
                "/redfish/v1/metrics",
                &payload,
                "\"etag-1\"",
                HttpClient::DEFAULT_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(result["updated"], true);
    }

    #[tokio::test]
    async fn error_body_excluded_from_message() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/fail"))
            .respond_with(
                ResponseTemplate::new(500).set_body_string("internal server error details"),
            )
            .mount(&server)
            .await;

        let client = HttpClient::for_test(&server.uri());
        let result = client.get("/fail", HttpClient::DEFAULT_TIMEOUT).await;

        let Err(err) = result else {
            panic!("expected HTTP error");
        };

        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "HTTP GET /fail returned 500");
        assert!(!err.message.contains("internal server error details"));
    }

    #[tokio::test]
    async fn auth_header_sent_when_credentials_provided() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/auth"))
            .and(header("Authorization", "Basic YWRtaW46cGFzcw=="))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"auth": "ok"})),
            )
            .mount(&server)
            .await;

        let client = HttpClient::for_test_with_auth(&server.uri(), "admin", "pass");
        let result = client
            .get("/auth", HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(result["auth"], "ok");
    }

    #[tokio::test]
    async fn post_multipart_sends_file() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"@odata.id": "/tasks/1"})),
            )
            .mount(&server)
            .await;

        let tmp = std::env::temp_dir().join("rms_test_multipart.bin");
        tokio::fs::write(&tmp, b"firmware-binary-content")
            .await
            .unwrap();

        let params = serde_json::json!({"ForceUpdate": true});
        let client = HttpClient::for_test(&server.uri());
        let result = client
            .post_multipart(
                "/upload",
                tmp.to_str().unwrap(),
                &params,
                HttpClient::UPLOAD_TIMEOUT,
            )
            .await
            .unwrap();
        assert_eq!(result["@odata.id"], "/tasks/1");

        tokio::fs::remove_file(&tmp).await.ok();
    }

    #[tokio::test]
    async fn connection_refused_error() {
        let client = HttpClient::for_test("http://127.0.0.1:1");
        let err = client
            .get("/redfish/v1", HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionRefused);
    }

    #[tokio::test]
    async fn request_send_error_maps_to_unavailable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let _accepted = listener.accept().await;
        });

        let client = HttpClient::for_test(&format!("http://{addr}"));
        let err = client
            .get("/redfish/v1", HttpClient::DEFAULT_TIMEOUT)
            .await
            .unwrap_err();

        server.await.unwrap();

        assert_eq!(err.code, ErrorCode::Unavailable);
    }
}
