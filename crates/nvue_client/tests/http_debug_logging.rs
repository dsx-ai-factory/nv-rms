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

//! Verifies the NVUE client emits debug-level request/response logs.
//!
//! This lives in its own integration-test binary on purpose: the debug
//! callsites in `send_request`/`read_response` are traversed by every HTTP
//! test, and tracing caches per-callsite interest globally. Running this as the
//! sole test in its process keeps that cache from being poisoned (cached as
//! "disabled") by unrelated tests before the capturing subscriber is installed.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nvue_client::{Client, ClientConfig, ClientCredentials, ClientEndpoint};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Clone)]
struct BufferWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for BufferWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn http_requests_emit_debug_logs_with_method_path_and_status() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let writer_buf = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(move || BufferWriter {
            buf: writer_buf.clone(),
        })
        .finish();
    // Install globally so the callsite interest resolves to "enabled" for the
    // whole process (this binary only runs this one test).
    tracing::subscriber::set_global_default(subscriber).expect("set global subscriber");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nvue_v1/platform"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .mount(&server)
        .await;

    let config = ClientConfig {
        endpoint: ClientEndpoint::http("127.0.0.1", server.address().port()),
        credentials: ClientCredentials::new("admin", "pass"),
        dangerously_accept_invalid_certs: true,
    };
    let client = Client::new(config).expect("build client");
    client
        .get("/nvue_v1/platform", Duration::from_secs(5))
        .await
        .expect("GET succeeds");

    let logs = String::from_utf8(buf.lock().unwrap().clone()).expect("utf8 logs");
    assert!(
        logs.contains("sending NVUE request"),
        "request log missing: {logs}"
    );
    assert!(
        logs.contains("received NVUE response"),
        "response log missing: {logs}"
    );
    assert!(logs.contains("GET"), "method missing: {logs}");
    assert!(logs.contains("/nvue_v1/platform"), "path missing: {logs}");
    assert!(logs.contains("status=200"), "status missing: {logs}");
}
