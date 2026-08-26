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

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use prometheus::Registry;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

use crate::utilities::error::{Result, RmsError};

use super::prometheus_route;

/// Independent HTTP listener that exposes `GET /metrics`.
pub struct MetricsServer {
    port: u16,
    shutdown_tx: Option<watch::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl MetricsServer {
    #[must_use]
    pub fn new(port: u16) -> Self {
        Self {
            port,
            shutdown_tx: None,
            task: None,
        }
    }

    /// Bind the metrics listener and begin serving in a background task.
    ///
    /// `tls` is `Some` for standard server-authenticated TLS (no client certs).
    pub async fn start(
        &mut self,
        registry: Arc<Registry>,
        tls: Option<Arc<rustls::ServerConfig>>,
    ) -> Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.port)
            .parse()
            .map_err(|e| RmsError::invalid_argument(format!("invalid metrics address: {e}")))?;

        let router = Router::new().route("/metrics", prometheus_route(registry));

        // Bind synchronously so bind failures (e.g. port already in use) are
        // surfaced to the caller instead of being silently logged inside the
        // spawned serving task.
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            RmsError::unavailable(format!("metrics server failed to bind {addr}: {e}"))
        })?;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        self.shutdown_tx = Some(shutdown_tx);

        let task = match tls {
            Some(tls_config) => {
                tracing::info!(port = self.port, mode = "TLS", "metrics server starting");
                let tls_acceptor = TlsAcceptor::from(tls_config);
                tokio::spawn(serve_tls(listener, router, tls_acceptor, shutdown_rx))
            }
            None => {
                tracing::info!(
                    port = self.port,
                    mode = "plaintext",
                    "metrics server starting"
                );
                tokio::spawn(serve_plaintext(listener, router, shutdown_rx))
            }
        };
        self.task = Some(task);

        Ok(())
    }

    /// Signal the serving task to shut down. Returns immediately; call
    /// [`MetricsServer::join`] afterwards to await the background task.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            tracing::info!("metrics server shutdown signal sent");
        }
    }

    /// Await the background serving task. Call [`Self::stop`] first to signal
    /// shutdown, otherwise this waits until the listener exits on its own.
    pub async fn join(&mut self) {
        if let Some(task) = self.task.take()
            && let Err(e) = task.await
        {
            tracing::error!(error = %e, "metrics server task join failed");
        }
    }
}

async fn serve_plaintext(
    listener: tokio::net::TcpListener,
    router: Router,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let server = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!(error = %e, "metrics server error");
        }
    });
    let abort = server.abort_handle();

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "metrics server task failed");
            }
        }
        _ = shutdown_rx.changed() => {
            tracing::info!("shutting down; abandoning in-flight connections");
            abort.abort();
        }
    }
}

async fn serve_tls(
    listener: tokio::net::TcpListener,
    router: Router,
    tls_acceptor: TlsAcceptor,
    mut shutdown_rx: watch::Receiver<()>,
) {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::service::TowerToHyperService;

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let Ok((stream, _)) = accept_result else {
                    continue;
                };
                let tls = tls_acceptor.clone();
                let app = router.clone();
                tokio::task::spawn(async move {
                    let tls_stream = match tls.accept(stream).await {
                        Ok(tls_stream) => tls_stream,
                        Err(e) => {
                            tracing::debug!(error = %e, "metrics TLS connection failed to accept");
                            return;
                        }
                    };
                    let io = TokioIo::new(tls_stream);
                    let svc = TowerToHyperService::new(app);
                    if let Err(e) = Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await
                    {
                        tracing::debug!(error = %e, "metrics TLS connection closed");
                    }
                });
            }
            _ = shutdown_rx.changed() => {
                tracing::info!("shutting down; abandoning in-flight connections");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;
    use std::time::Duration;

    use axum::http::StatusCode;

    use super::*;
    use crate::api::grpc::server::TlsMode;
    use crate::metrics::{MetricsInfo, init};

    fn ensure_registry() -> Arc<Registry> {
        static REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();
        REGISTRY
            .get_or_init(|| {
                init(&MetricsInfo {
                    grpc_port: 8801,
                    firmware_dir: "/tmp/firmware".to_string(),
                    persistence_type: "memory".to_string(),
                    tls_mode: TlsMode::Insecure,
                })
                .expect("metrics init failed")
            })
            .clone()
    }

    #[tokio::test]
    async fn metrics_server_serves_prometheus_on_dedicated_port() {
        let port = portpicker::pick_unused_port().expect("no free port");
        let mut server = MetricsServer::new(port);
        server
            .start(ensure_registry(), None)
            .await
            .expect("start metrics server");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
            .await
            .expect("metrics request");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.text().await.expect("read body");
        assert!(body.contains("rms_api_version"));

        server.stop();
        tokio::time::timeout(Duration::from_secs(1), server.join())
            .await
            .expect("metrics server should exit promptly after shutdown");
    }

    #[tokio::test]
    async fn metrics_server_returns_not_found_for_other_paths() {
        let port = portpicker::pick_unused_port().expect("no free port");
        let mut server = MetricsServer::new(port);
        server
            .start(ensure_registry(), None)
            .await
            .expect("start metrics server");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = reqwest::get(format!("http://127.0.0.1:{port}/healthz"))
            .await
            .expect("healthz request");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        server.stop();
        server.join().await;
    }

    fn test_server_config() -> Arc<rustls::ServerConfig> {
        use rustls::pki_types::PrivateKeyDer;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let generated =
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("generate cert");
        let cert = generated.cert.der().clone();
        let key = PrivateKeyDer::try_from(generated.key_pair.serialize_der()).expect("key der");
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .expect("server config");
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Arc::new(config)
    }

    #[tokio::test]
    async fn metrics_server_serves_over_tls_and_shuts_down() {
        let port = portpicker::pick_unused_port().expect("no free port");
        let mut server = MetricsServer::new(port);
        server
            .start(ensure_registry(), Some(test_server_config()))
            .await
            .expect("start metrics server");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("build tls client");
        let response = client
            .get(format!("https://127.0.0.1:{port}/metrics"))
            .send()
            .await
            .expect("metrics request");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.text().await.expect("read body");
        assert!(body.contains("rms_api_version"));

        server.stop();
        tokio::time::timeout(Duration::from_secs(1), server.join())
            .await
            .expect("metrics TLS server should exit promptly after shutdown");
    }

    #[tokio::test]
    async fn metrics_server_start_surfaces_bind_failure() {
        // Occupy a port, then confirm start() returns an error rather than
        // silently succeeding while the background task fails to bind.
        let occupier = tokio::net::TcpListener::bind("0.0.0.0:0")
            .await
            .expect("bind occupier");
        let port = occupier.local_addr().expect("addr").port();

        let mut server = MetricsServer::new(port);
        let result = server.start(ensure_registry(), None).await;

        assert!(result.is_err(), "expected bind failure to be surfaced");
    }
}
