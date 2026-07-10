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

use std::path::Path;
use std::time::Duration;

use futures::TryStreamExt;
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::url::http_target_uri;

/// Cap for one BMC Redfish/NVUE response body before JSON parsing.
///
/// These endpoints return control-plane JSON such as inventory, task status,
/// and action results. Firmware images are uploaded as request bodies, not
/// returned here, so 8 MiB gives ample headroom for expected responses while
/// bounding memory use from a compromised or faulty BMC.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

const DEFAULT_RESPONSE_BODY_CAPACITY: usize = 4 * 1024;

/// Async HTTP client for communicating with BMC / switch REST APIs over HTTP or HTTPS.
///
/// Wraps `reqwest::Client` with Redfish/NVUE conventions: JSON request/response,
/// error code mapping, basic auth, TLS verify toggle, and firmware upload support.
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
        let mut builder = reqwest::Client::builder()
            .danger_accept_invalid_certs(dangerously_accept_invalid_certs)
            .connect_timeout(Duration::from_secs(10));

        if dangerously_accept_invalid_certs {
            // Root stores are unused when certificate verification is disabled,
            // and loading native roots is expensive on some platforms.
            builder = builder.tls_built_in_root_certs(false);
        }

        if tls_host != connect_host {
            if let Ok(addr) = format!("{connect_host}:{port}").parse::<std::net::SocketAddr>() {
                builder = builder.resolve(tls_host, addr);
            }
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
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn get(&self, endpoint: &str, timeout: Duration) -> Result<Value> {
        let req = self.client.get(self.url(endpoint)).timeout(timeout);

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
            .post(self.url(endpoint))
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
            .patch(self.url(endpoint))
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
            .patch(self.url(endpoint))
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
            .post(self.url(endpoint))
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

    fn url(&self, endpoint: &str) -> String {
        format!("{}{endpoint}", self.base_url)
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
    if let Some(content_length) = content_length {
        if content_length > max_response_bytes as u64 {
            return Err(RmsError::internal(format!(
                "response body for {context} exceeded {max_response_bytes} bytes (got {content_length} bytes)"
            )));
        }
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
        assert_eq!(
            client.url("/redfish/v1/Systems/System_0"),
            "https://10.0.0.1:443/redfish/v1/Systems/System_0"
        );
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
        assert_eq!(
            client.url("/nvue_v1/system"),
            "https://switch.example.com:443/nvue_v1/system"
        );
        assert_eq!(client.host(), "10.0.0.5");
    }

    #[test]
    fn url_with_custom_port() {
        let client = HttpClient::new("bmc.local", 8443, "", "", false, true).unwrap();
        assert_eq!(
            client.url("/redfish/v1"),
            "https://bmc.local:8443/redfish/v1"
        );
    }

    #[test]
    fn url_construction_formats_ipv6_literal() {
        let client = HttpClient::new("fd00::1", 443, "admin", "pass", false, true).unwrap();
        assert_eq!(
            client.url("/nvue_v1/system"),
            "https://[fd00::1]:443/nvue_v1/system"
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
