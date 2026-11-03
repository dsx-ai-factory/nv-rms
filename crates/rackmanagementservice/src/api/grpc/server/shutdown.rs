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

use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tokio_util::task::TaskTracker;
use tonic::transport::server::Connected;

/// Interrupts accepted connections after the graceful drain deadline, including
/// TLS handshakes and clients stalled by HTTP/2 flow control.
pub(super) struct ShutdownIo<T> {
    inner: T,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<T> ShutdownIo<T> {
    pub(super) fn new(inner: T, shutdown: CancellationToken) -> Self {
        Self {
            inner,
            cancelled: Box::pin(shutdown.cancelled_owned()),
        }
    }

    fn check_shutdown(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            Err(io::ErrorKind::ConnectionAborted.into())
        } else {
            Ok(())
        }
    }
}

impl<T: Connected> Connected for ShutdownIo<T> {
    type ConnectInfo = T::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for ShutdownIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check_shutdown(cx)?;
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ShutdownIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check_shutdown(cx)?;
        Pin::new(&mut self.inner).poll_write(cx, buffer)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.check_shutdown(cx)?;
        Pin::new(&mut self.inner).poll_write_vectored(cx, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_shutdown(cx)?;
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_shutdown(cx)?;
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(super) async fn drain_router(
    mut router: JoinHandle<()>,
    requests: &TaskTracker,
    force_shutdown: &CancellationToken,
    graceful_timeout: Duration,
    forced_timeout: Duration,
) {
    requests.close();
    let abort_router = router.abort_handle();
    // Tonic owns connection tasks and Hyper owns request tasks. Waiting for
    // both prevents an ended connection from racing its final span export.
    let drain = async {
        let result = (&mut router).await;
        requests.wait().await;
        result
    };
    tokio::pin!(drain);
    let result = match tokio::time::timeout(graceful_timeout, &mut drain).await {
        Ok(result) => Some(result),
        Err(_) => {
            force_shutdown.cancel();
            tracing::warn!("gRPC drain deadline reached; closing remaining connections");
            tokio::time::timeout(forced_timeout, &mut drain).await.ok()
        }
    };

    match result {
        Some(Ok(())) => tracing::info!("gRPC server and request spans drained"),
        Some(Err(error)) => tracing::warn!(%error, "gRPC server task exited unsuccessfully"),
        None => {
            abort_router.abort();
            tracing::warn!(
                pending_request_handles = requests.len(),
                "gRPC forced drain timed out; some request spans may not have finished"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use http_body::Body as _;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tokio::sync::Notify;
    use tokio_stream::StreamExt as _;
    use tonic::body::Body;
    use tonic::transport::Server;
    use tracing_subscriber::prelude::*;

    use super::*;
    use crate::logging::api_logs::LogLayer;

    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct PendingService {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        handler_dropped: Arc<AtomicBool>,
        body_dropped: Arc<AtomicBool>,
        stall_body: bool,
    }

    impl Default for PendingService {
        fn default() -> Self {
            Self {
                entered: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                handler_dropped: Arc::new(AtomicBool::new(false)),
                body_dropped: Arc::new(AtomicBool::new(false)),
                stall_body: false,
            }
        }
    }

    impl tonic::server::NamedService for PendingService {
        const NAME: &'static str = "rack_manager.RackManager";
    }

    impl tower::Service<http::Request<Body>> for PendingService {
        type Response = http::Response<Body>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: http::Request<Body>) -> Self::Future {
            let service = self.clone();
            Box::pin(async move {
                let _handler = DropMarker(service.handler_dropped);
                service.entered.notify_one();
                service.release.notified().await;
                let body = if service.stall_body {
                    Body::new(StalledBody {
                        chunk: Some(vec![0; 1024 * 1024].into()),
                        _dropped: DropMarker(service.body_dropped),
                    })
                } else {
                    Body::empty()
                };
                Ok(http::Response::builder()
                    .header("grpc-status", "0")
                    .body(body)
                    .unwrap())
            })
        }
    }

    struct StalledBody {
        chunk: Option<hyper::body::Bytes>,
        _dropped: DropMarker,
    }

    impl http_body::Body for StalledBody {
        type Data = hyper::body::Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            match self.chunk.take() {
                Some(chunk) => Poll::Ready(Some(Ok(http_body::Frame::data(chunk)))),
                None => Poll::Pending,
            }
        }
    }

    type ClientTask = JoinHandle<Result<(), hyper::Error>>;
    type ResponseTask = JoinHandle<Result<http::Response<hyper::body::Incoming>, hyper::Error>>;

    async fn serve(
        service: PendingService,
        requests: TaskTracker,
        force_shutdown: CancellationToken,
        graceful_shutdown: CancellationToken,
    ) -> (JoinHandle<()>, ClientTask, ResponseTask) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let io = ShutdownIo::new(server_io, force_shutdown.clone());
        let incoming = tokio_stream::iter([Ok::<_, io::Error>(io)]).chain(tokio_stream::pending());
        let router = tokio::spawn(async move {
            Server::builder()
                .layer(LogLayer::default().with_request_tracker(requests, force_shutdown))
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, graceful_shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let (mut sender, connection) = hyper::client::conn::http2::handshake::<_, _, Body>(
            TokioExecutor::new(),
            TokioIo::new(client_io),
        )
        .await
        .unwrap();
        let client = tokio::spawn(connection);
        let response = tokio::spawn(async move {
            sender
                .send_request(
                    http::Request::builder()
                        .method("POST")
                        .uri("http://test/rack_manager.RackManager/GetVersion")
                        .header("content-type", "application/grpc")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        });
        (router, client, response)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forced_connection_shutdown_drops_pending_handler_and_exports_span() {
        // Other tests can register the shared request callsite without a subscriber.
        // Isolate its process-wide interest cache so this test always creates a span.
        if crate::observability::run_test_in_subprocess(concat!(
            "api::grpc::server::shutdown::tests::",
            "forced_connection_shutdown_drops_pending_handler_and_exports_span"
        )) {
            return;
        }

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let _subscriber = tracing_subscriber::registry()
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("shutdown-test"))
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        crate::observability::tracing::should_export,
                    )),
            )
            .set_default();
        let service = PendingService::default();
        let requests = TaskTracker::new();
        let forced = CancellationToken::new();
        let graceful = CancellationToken::new();
        let (router, client, response) = serve(
            service.clone(),
            requests.clone(),
            forced.clone(),
            graceful.clone(),
        )
        .await;
        service.entered.notified().await;
        graceful.cancel();

        drain_router(
            router,
            &requests,
            &forced,
            Duration::from_millis(20),
            Duration::from_secs(1),
        )
        .await;

        assert!(forced.is_cancelled());
        assert!(service.handler_dropped.load(Ordering::SeqCst));
        assert!(requests.is_empty());
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(
            spans.len(),
            1,
            "pending request must export before provider shutdown"
        );
        assert!(spans[0].attributes.iter().any(|attribute| {
            attribute.key.as_str() == "rpc.response.status_code"
                && attribute.value.as_str() == "CANCELLED"
        }));
        provider.shutdown().unwrap();
        assert!(response.await.unwrap().is_err());
        client.abort();
        let _ = client.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn graceful_shutdown_preserves_completed_request_and_span() {
        // Other tests can register the shared request callsite without a subscriber.
        // Isolate its process-wide interest cache so this test always creates a span.
        if crate::observability::run_test_in_subprocess(concat!(
            "api::grpc::server::shutdown::tests::",
            "graceful_shutdown_preserves_completed_request_and_span"
        )) {
            return;
        }

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let _subscriber = tracing_subscriber::registry()
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("shutdown-test"))
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        crate::observability::tracing::should_export,
                    )),
            )
            .set_default();
        let service = PendingService::default();
        let requests = TaskTracker::new();
        let forced = CancellationToken::new();
        let graceful = CancellationToken::new();
        let (router, client, response) = serve(
            service.clone(),
            requests.clone(),
            forced.clone(),
            graceful.clone(),
        )
        .await;
        service.entered.notified().await;
        graceful.cancel();
        service.release.notify_one();

        drain_router(
            router,
            &requests,
            &forced,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await;

        assert!(!forced.is_cancelled());
        assert!(service.handler_dropped.load(Ordering::SeqCst));
        assert!(requests.is_empty());
        let response = response.await.unwrap().unwrap();
        assert_eq!(response.headers()["grpc-status"], "0");
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert!(spans[0].attributes.iter().any(|attribute| {
            attribute.key.as_str() == "rpc.response.status_code" && attribute.value.as_str() == "OK"
        }));
        provider.shutdown().unwrap();
        client.abort();
        let _ = client.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forced_shutdown_drops_flow_control_stalled_response_body() {
        // Keep the shared request callsite's interest cache isolated, as above.
        if crate::observability::run_test_in_subprocess(concat!(
            "api::grpc::server::shutdown::tests::",
            "forced_shutdown_drops_flow_control_stalled_response_body"
        )) {
            return;
        }

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let _subscriber = tracing_subscriber::registry()
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("shutdown-test"))
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        crate::observability::tracing::should_export,
                    )),
            )
            .set_default();
        let service = PendingService {
            stall_body: true,
            ..PendingService::default()
        };
        let requests = TaskTracker::new();
        let forced = CancellationToken::new();
        let graceful = CancellationToken::new();
        let (router, client, response) = serve(
            service.clone(),
            requests.clone(),
            forced.clone(),
            graceful.clone(),
        )
        .await;
        service.entered.notified().await;
        service.release.notify_one();
        let response = response.await.unwrap().unwrap();
        // Keep the body alive without consuming it, exhausting HTTP/2 credit.
        assert!(!response.body().is_end_stream());
        graceful.cancel();

        drain_router(
            router,
            &requests,
            &forced,
            Duration::from_millis(20),
            Duration::from_secs(1),
        )
        .await;

        assert!(forced.is_cancelled());
        assert!(service.body_dropped.load(Ordering::SeqCst));
        assert!(requests.is_empty());
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(
            spans.len(),
            1,
            "stalled response must export before shutdown"
        );
        assert_eq!(
            spans[0].status,
            opentelemetry::trace::Status::error("gRPC response body dropped before completion")
        );
        assert!(spans[0].attributes.iter().any(|attribute| {
            attribute.key.as_str() == "error.type" && attribute.value.as_str() == "response_body"
        }));
        assert!(spans[0].attributes.iter().any(|attribute| {
            attribute.key.as_str() == "rpc.response.status_code"
                && attribute.value.as_str() == "UNKNOWN"
        }));
        provider.shutdown().unwrap();
        drop(response);
        client.abort();
        let _ = client.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forced_shutdown_interrupts_pending_tls_handshake() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let identity = tonic::transport::Identity::from_pem(
            certificate.cert.pem(),
            certificate.key_pair.serialize_pem(),
        );
        let (mut client, server) = tokio::io::duplex(4096);
        let forced = CancellationToken::new();
        let graceful = CancellationToken::new();
        let incoming =
            tokio_stream::iter([Ok::<_, io::Error>(ShutdownIo::new(server, forced.clone()))])
                .chain(tokio_stream::pending());
        let signal = graceful.clone();
        let router = tokio::spawn(async move {
            Server::builder()
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                .unwrap()
                .add_service(PendingService::default())
                .serve_with_incoming_shutdown(incoming, signal.cancelled_owned())
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        forced.cancel();
        let mut byte = [0];
        let read = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::io::AsyncReadExt::read(&mut client, &mut byte),
        )
        .await
        .expect("pending TLS handshake must close after forced shutdown")
        .unwrap();
        assert_eq!(read, 0);
        graceful.cancel();
        drain_router(
            router,
            &TaskTracker::new(),
            &forced,
            Duration::ZERO,
            Duration::from_secs(1),
        )
        .await;
        drop(client);
    }
}
